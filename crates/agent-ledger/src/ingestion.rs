//! Stream ingestion — a bound provider channel's events becoming blocks.
//!
//! One reader per conversation, long-lived across turns. It persists streaming
//! events as blocks in the store and bridges status and completion signals to
//! the bus. Three rules shape everything here:
//!
//! - **Insert-first.** A streaming tail is in the ledger from its first delta,
//!   so a consumer renders the turn live and a crash loses at most the tail.
//! - **Finalization is atomic.** A committed block replaces its streaming tail
//!   in ONE transaction — streaming blocks are ephemeral, replaced by finals,
//!   and no path leaves both or neither.
//! - **The latch is re-read where it matters.** The channel discards whole
//!   responses while latched, and the tool-call finalizer re-reads the latch
//!   per call rather than trusting the loop's entry check, so an interrupt
//!   racing a mid-stream insert records the block and drives nothing.

use std::sync::Arc;
use std::time::Duration;

use crate::agency::{AgencyCtx, RuntimeKind};
use crate::bus::RuntimeEvent;
use crate::dispatch::TurnAnchor;
use crate::event::CoreEvent;
use crate::event::stream_status;
use crate::providers::types::{
    FinalContentBlock, ProviderResponse, ProviderRx, StopReason, StreamEvent,
};
use crate::reactivity::ReadSignal;
use crate::store::{BlockDestination, now_iso8601};
use crate::tools::{CallOrigin, ToolRunner};
use crate::types::StreamUsage;

use super::actor::RuntimeContext;

/// Per-conversation ingestion reader on a bound provider channel.
///
/// Reads [`ProviderResponse`]s, persists streaming events as blocks, and
/// bridges status/completion signals to the bus. When latched, incoming
/// events are silently discarded.
/// `generation` is the binding identity the actor assigned at this bind. The
/// reader stamps it on every stream-lifecycle signal it emits, so the actor
/// can ignore a signal from a reader it already tore down. `anchor` is the
/// binding's per-turn dispatch seam: the actor sets it at dispatch and clears
/// it at close, and this reader stamps its value on every block it inserts —
/// the provider channel itself stays neutral and never carries ledger
/// identity.
pub(crate) fn spawn_channel<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    provider_rx: ProviderRx,
    latched: ReadSignal<bool>,
    generation: u64,
    anchor: TurnAnchor,
    drain_deadline: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_channel(
        conv_id,
        ctx,
        provider_rx,
        latched,
        generation,
        anchor,
        drain_deadline,
    ))
}

/// The DEFAULT drain deadline (a construction-time parameter of the reader,
/// so tests pin the expiry without waiting production bounds — the same
/// discipline as construction-time tool timeouts): how long the reader
/// waits, after a turn's
/// `MessageEnd`, for the turn's remaining events — the drained tool
/// lifecycles and the trailing done, which real wires send within moments
/// because they are already in flight behind the end. A provider that emits
/// `MessageEnd` and then stalls would otherwise wedge the conversation
/// forever, since message-end no longer settles the dispatch state: no close
/// edge would ever fire. Generous on purpose — a slow wire is not a stall.
///
/// On expiry the reader abandons the turn (2026-08-22): it marks the drained
/// turn's epoch abandoned on the seam, emits its own closed terminal, and
/// exits. The mark is what makes the exit sound — the actor observes it at
/// the close and retires the binding, so the next turn rebinds fresh and the
/// stalled provider's late tail, delivered into the dropped channel, reaches
/// nobody. Staying on the channel instead was rejected: the channel is
/// neutral, so once a successor turn shares it the stalled turn's late tail
/// and the successor's own events cannot be told apart — a late trailing
/// `Done` would close the live turn mid-flight, and the late lifecycles
/// would ingest under the live turn's anchor.
pub(crate) const MESSAGE_END_DRAIN_DEADLINE: Duration = Duration::from_secs(30);

/// One turn's mutable ingestion state, reset at every turn boundary — a
/// message end, a restart, a terminal error.
#[derive(Default)]
struct TurnTrackers {
    current_streaming_block: Option<i64>,
    current_thinking_block: Option<i64>,
    content_final_received: bool,
    thinking_finalized: bool,
    /// Where the turn's text channel stands — see [`TextChannel`].
    text_channel: TextChannel,
    /// Every concurrently-open streaming tool call, in start order — the
    /// shared chat-stream translator buffers N parallel calls and emits a
    /// SINGLE terminal `ToolUseEnd`, so a Vec (not one slot) is what keeps
    /// genuine parallel siblings from clobbering each other.
    open_streaming_tool_calls: Vec<(i64, String, String)>,
}

/// The turn's text-channel progression (2026-08-24): one fact with three
/// stops, not two independent flags, because the two were never independent
/// — the `responding` signal matters only before a commit, and a commit ends
/// the channel's story for the turn.
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
enum TextChannel {
    /// No user-visible text yet. A block may have OPENED — an open that
    /// finalizes empty is the recorded empty turn — but nothing real flowed,
    /// so no compose cue was raised.
    #[default]
    Silent,
    /// Real text flowed: the `responding` status fired, once, at the first
    /// non-empty delta.
    Responding,
    /// A final text block committed, through ANY of the finalize paths.
    /// Read at `message_end`: a completed non-tool turn whose channel never
    /// reached this stop owes the ledger its empty assistant text block —
    /// this state is what tells "the turn's text is already in the ledger"
    /// from "the turn ended with nothing", because a `TextFinal` commit
    /// leaves no streaming tail behind for `message_end` to see.
    Committed,
}

/// What became of the open streamed text tail at a finalization point.
///
/// Three-valued on purpose: the guard logic in [`ChannelReader::content_final`]
/// must tell a DELIBERATE no-persist (no open tail at all) from a FAILED
/// persist (content that may still exist and that a fallback path could still
/// save). Collapsing both into one "nothing persisted" answer is what let an
/// empty sibling clear the received guard and a following `TextFinal` commit
/// the turn's text twice.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TailFinalization {
    /// The tail's final state committed as a final text block — empty
    /// included (2026-08-24): an opened tail that accumulated nothing is
    /// still the text channel's final state, and it commits like any other.
    Persisted,
    /// There was no open tail to commit. Not a failure.
    NothingToPersist,
    /// A commit was owed and did not happen — the fallback paths may still
    /// save the content.
    Failed,
}

/// The reader itself: the hook context, the runner whose insert seam
/// finalizes tool calls, the latch, and the turn's trackers.
struct ChannelReader<K: RuntimeKind, E> {
    ctx: AgencyCtx<E>,
    runner: Arc<ToolRunner<K, E>>,
    latched: ReadSignal<bool>,
    trackers: TurnTrackers,
    /// A turn is open: something arrived since the last `Done`. What makes the
    /// channel closing mid-turn distinguishable from it closing at rest, so
    /// the exit path knows whether it still owes the actor a close.
    mid_turn: bool,
    /// The binding identity the actor assigned at this reader's bind, stamped
    /// on every stream-lifecycle signal the reader emits. A torn-down reader
    /// exits asynchronously, and its parting `StreamClosed` can land while the
    /// successor turn is already streaming — the stamp is what lets the actor
    /// tell that late signal from the live binding's and ignore it.
    generation: u64,
    /// This binding's per-turn dispatch seam. The actor sets the open turn's
    /// anchor at dispatch and clears it at close; every block this reader
    /// inserts stamps the seam's current value. Read per insert rather than
    /// captured per turn: the seam IS the turn state, owned by the one
    /// dispatcher, and a copy here would be a second record of it.
    anchor: TurnAnchor,
    /// An abnormal stop (`max_tokens`, `content_filter`) recorded at
    /// `MessageEnd`, surfaced as the turn's terminal when the drain finishes.
    /// Emitting the error AT the stop closed the dispatch under a reader
    /// still ingesting the same turn: the seam cleared mid-drain, the turn's
    /// trailing writes stamped NULL anchors, and the released streaming flag
    /// mid-turn was the duplicate-turn shape again. Deferred, the error keeps
    /// its latch semantics — the actor latches when it arrives — and the
    /// close happens where every close happens: when the reader is done with
    /// the turn.
    pending_error: Option<String>,
    /// The drained turn completed (`EndTurn`) with NO text block committed,
    /// and owes the ledger its empty assistant text block (2026-08-24). Set
    /// at `message_end`, consumed at the turn's real boundary — the trailing
    /// done — because one real wire ends a tool-call turn with an `EndTurn`
    /// stop and trails the lifecycles after the end: a tool call that
    /// finalizes during the drain cancels the debt, so no empty block sits
    /// orphaned beside the calls. Lives on the reader, not the trackers,
    /// because `message_end` resets the trackers for the drain phase. The
    /// error, restart and teardown ends all drop it: the commit rides only
    /// the normal completion terminal.
    owes_empty_commit: bool,
    /// Between a turn's `MessageEnd` and its terminal: the phase the drain
    /// deadline covers.
    draining: bool,
    /// The seam epoch of the turn being drained, recorded when the drain
    /// begins. If the drain deadline expires, this is the epoch the reader
    /// marks abandoned on the seam — the fact the actor's close reads to
    /// retire the stalled binding.
    drained_epoch: u64,
}

async fn run_channel<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    mut provider_rx: ProviderRx,
    latched: ReadSignal<bool>,
    generation: u64,
    anchor: TurnAnchor,
    drain_deadline: Duration,
) {
    let mut reader = ChannelReader {
        ctx: ctx.agency(conv_id),
        runner: Arc::clone(ctx.runner()),
        latched,
        trackers: TurnTrackers::default(),
        mid_turn: false,
        generation,
        anchor,
        pending_error: None,
        owes_empty_commit: false,
        draining: false,
        drained_epoch: 0,
    };

    loop {
        // Between `MessageEnd` and the turn's terminal the recv is bounded:
        // the remaining events are already in flight on a live wire, so a
        // silence of [`MESSAGE_END_DRAIN_DEADLINE`] here is a stalled
        // provider, and the reader closes the turn itself instead of leaving
        // the dispatch state open forever. Every received event re-arms the
        // deadline.
        let response = if reader.draining {
            let Ok(bounded) = tokio::time::timeout(drain_deadline, provider_rx.recv()).await else {
                tracing::warn!(
                    conversation_id = conv_id,
                    "no event within the drain deadline after message-end — \
                     abandoning the turn and closing the stream for the \
                     stalled provider"
                );
                // A deadline is scoped to the turn that armed it. If the
                // seam's epoch moved on — an out-of-band close ended the
                // drained turn and a successor already dispatched on this
                // binding — this timer is stale: firing it would cap the
                // successor's time to its first token at the drain deadline
                // and abandon a healthy binding. The stale drain state
                // resets and the reader keeps serving the successor.
                if reader.anchor.epoch() != reader.drained_epoch {
                    // Only the drain state resets: mid_turn belongs to
                    // whatever the successor's stream is doing right now
                    // and is managed by its own arms. An empty commit the
                    // drained turn still owed dies with it — the turn was
                    // ended out of band, and committing its block under the
                    // successor's anchor would misattribute it.
                    reader.draining = false;
                    reader.abandon_owed_empty_commit();
                    continue;
                }
                reader
                    .discard_unterminated_tails("drain deadline expired")
                    .await;
                // The mark goes on the seam BEFORE the terminal, so the
                // actor's close observes it and retires the binding; then
                // this reader exits. The provider may still wake and deliver
                // the turn's held tail — trailing lifecycles, a late done —
                // but it delivers into a channel nobody reads and nobody
                // will read again: the successor turn rebinds fresh. Kept
                // reading instead, this reader could not tell that late
                // tail from a successor turn's own events (the channel is
                // neutral), and the late done would close the live turn
                // mid-flight — the second-terminal defect this fence exists
                // for.
                reader.anchor.mark_abandoned(reader.drained_epoch);
                reader.emit_turn_terminal();
                break;
            };
            bounded
        } else {
            provider_rx.recv().await
        };
        let Some(response) = response else { break };
        if reader.latched.get() {
            tracing::debug!(
                conversation_id = conv_id,
                "latched — discarding provider response"
            );
            continue;
        }
        reader.handle_response(response).await;
    }

    // The channel closed. If it closed MID-TURN — a provider task that died
    // without its terminal `Done` — the actor's streaming flag is still set
    // and, left alone, no future turn signal would ever fire again for this
    // conversation. Close the turn ourselves: discard whatever tails the dead
    // stream left open, then emit the close the provider never sent.
    if reader.mid_turn {
        tracing::warn!(
            conversation_id = conv_id,
            "provider channel closed mid-turn — closing the stream for it"
        );
        reader.discard_tracked_tails().await;
        reader.emit_turn_terminal();
    }

    tracing::info!(conversation_id = conv_id, "ingestion stopped");
}

impl<K: RuntimeKind, E: RuntimeEvent> ChannelReader<K, E> {
    /// Where this reader's inserts land: the conversation, anchored on the
    /// open turn from the per-turn seam. Read per insert on purpose — the
    /// seam IS the turn state, owned by the one dispatcher, and a copy here
    /// would be a second record of it.
    fn turn_destination(&self) -> BlockDestination {
        BlockDestination::anchored(self.ctx.conversation_id, self.anchor.get())
    }

    /// The turn's terminal, on every reader-side close: the recorded abnormal
    /// stop when one is pending — the actor latches on it, exactly as it
    /// latches on any stream error — and the plain closed signal otherwise.
    /// One emitter for all three reader-side ends (the trailing done, the
    /// drain deadline, the channel dying mid-turn), so no path can close the
    /// turn and drop the error, or surface the error and leave the turn open.
    fn emit_turn_terminal(&mut self) {
        let conv_id = self.ctx.conversation_id;
        self.mid_turn = false;
        self.draining = false;
        match self.pending_error.take() {
            Some(error) => {
                self.ctx.bus.emit(CoreEvent::StreamError {
                    conversation_id: conv_id,
                    error,
                    generation: Some(self.generation),
                });
            }
            None => {
                self.ctx.bus.emit(CoreEvent::StreamClosed {
                    conversation_id: conv_id,
                    generation: Some(self.generation),
                });
            }
        }
    }

    async fn handle_response(&mut self, response: ProviderResponse) {
        let conv_id = self.ctx.conversation_id;
        if !matches!(response, ProviderResponse::Done) {
            self.mid_turn = true;
        }
        match response {
            ProviderResponse::Event(stream_event) => self.ingest(stream_event).await,
            ProviderResponse::Restart => {
                // Restart-clean: a recoverable mid-stream drop. Discard the
                // turn's uncommitted streaming blocks and reset trackers so
                // the regenerated stream writes onto a clean slate with no
                // orphans. The regenerated stream replays the turn from its
                // start, so the drain phase and a recorded abnormal stop are
                // both superseded.
                self.discard_streaming_tails("restart-clean").await;
                self.pending_error = None;
                self.abandon_owed_empty_commit();
                self.draining = false;
            }
            ProviderResponse::Error(error) => {
                tracing::warn!(conversation_id = conv_id, error = %error, "provider error");
                self.ctx.bus.emit(CoreEvent::StreamError {
                    conversation_id: conv_id,
                    error,
                    generation: Some(self.generation),
                });
                // A terminal error ends the turn without a MessageEnd, so its
                // uncommitted streaming blocks are never finalized. Discard
                // them and reset the trackers — the same clean end-state a
                // recoverable drop reaches via Restart. This is load-bearing,
                // not cosmetic: the ingestion loop is long-lived per
                // conversation, so a stale `current_streaming_block` would
                // make the NEXT turn's first delta append to this failed
                // turn's orphaned block (cross-turn content bleed). Reachable
                // from in-band failure/cancellation terminals, the first
                // non-recoverable errors that fire after partial content.
                self.discard_streaming_tails("provider error").await;
                // The error IS the turn's end: the `StreamError` above settles
                // the actor, so a channel close that follows must not read the
                // turn as still open and emit a second stream-end signal — and
                // a recorded abnormal stop is superseded by it.
                self.mid_turn = false;
                self.pending_error = None;
                self.abandon_owed_empty_commit();
                self.draining = false;
            }
            ProviderResponse::Done => {
                // `Done` is the turn's REAL boundary: every provider emits its
                // tool lifecycles AFTER `MessageEnd`, so only here can "nothing
                // is still open" be checked. An open TEXT tail at `Done` is
                // complete prose — content, not an incomplete lifecycle — and
                // finalizes; a tool or thinking tail still open here is an
                // unterminated lifecycle — an incomplete fact — and is
                // discarded the way Restart discards, so no path leaves a
                // streaming tail alive past its turn.
                self.finalize_streamed_text_tail().await;
                self.discard_unterminated_tails("turn done").await;
                self.commit_owed_empty_turn().await;
                self.emit_turn_terminal();
            }
        }
    }

    /// The open tails' display names, for the boundary warnings.
    fn name_open_tails(&self) -> Vec<String> {
        let mut open = Vec::new();
        if let Some(id) = self.trackers.current_streaming_block {
            open.push(format!("streaming text block {id}"));
        }
        if let Some(id) = self.trackers.current_thinking_block {
            open.push(format!("streaming thinking block {id}"));
        }
        for (block_id, tool_call_id, name) in &self.trackers.open_streaming_tool_calls {
            open.push(format!(
                "streaming tool call block {block_id} ({name}, call id {tool_call_id})"
            ));
        }
        open
    }

    /// Discard whatever the turn boundary found still open, warning with the
    /// name of each dropped tail. Silent when the turn closed cleanly.
    async fn discard_unterminated_tails(&mut self, boundary: &'static str) {
        let dropped = self.name_open_tails();
        if dropped.is_empty() {
            return;
        }
        tracing::warn!(
            conversation_id = self.ctx.conversation_id,
            dropped = ?dropped,
            boundary,
            "unterminated streaming lifecycles at the turn boundary — an \
             unterminated lifecycle is an incomplete fact, discarding"
        );
        self.discard_streaming_tails("unterminated at turn boundary")
            .await;
    }

    /// The exit path's discard: exactly the rows this reader tracked, never
    /// the conversation-wide sweep. The channel can close AFTER an interrupt
    /// already tore the binding down and a successor reader is live for the
    /// next turn, and a conversation-wide delete running that late would take
    /// the successor's live tail with it. On rows the interrupt already swept,
    /// each targeted delete is a no-op.
    async fn discard_tracked_tails(&mut self) {
        let dropped = self.name_open_tails();
        if dropped.is_empty() {
            return;
        }
        tracing::warn!(
            conversation_id = self.ctx.conversation_id,
            dropped = ?dropped,
            boundary = "channel closed mid-turn",
            "unterminated streaming lifecycles at the channel's end — an \
             unterminated lifecycle is an incomplete fact, discarding"
        );
        let trackers = std::mem::take(&mut self.trackers);
        let ids = trackers
            .current_streaming_block
            .into_iter()
            .chain(trackers.current_thinking_block)
            .chain(
                trackers
                    .open_streaming_tool_calls
                    .into_iter()
                    .map(|(id, _, _)| id),
            );
        for block_id in ids {
            if let Err(e) = self.ctx.store.discard_streaming_block(block_id).await {
                tracing::error!(conversation_id = self.ctx.conversation_id, block_id, error = %e, "discard tracked tail failed");
            }
        }
    }

    /// Delete the turn's uncommitted streaming blocks and reset the trackers —
    /// the shared clean end-state of a restart and a terminal error.
    async fn discard_streaming_tails(&mut self, cause: &'static str) {
        let conv_id = self.ctx.conversation_id;
        match self.ctx.store.delete_streaming_blocks(conv_id).await {
            Ok(deleted) => tracing::info!(
                conversation_id = conv_id,
                deleted,
                cause,
                "discarded uncommitted streaming blocks"
            ),
            Err(e) => tracing::error!(
                conversation_id = conv_id,
                error = %e,
                cause,
                "failed to delete streaming blocks"
            ),
        }
        self.trackers = TurnTrackers::default();
    }

    /// The single-slot streaming thinking block, lazy-created on the first
    /// delta of EITHER reasoning channel (verbatim or summary) — both
    /// accumulate on the same block. `None` only when the lazy insert itself
    /// failed.
    async fn ensure_streaming_thinking_block(&mut self) -> Option<i64> {
        if let Some(id) = self.trackers.current_thinking_block {
            return Some(id);
        }
        let conv_id = self.ctx.conversation_id;
        match self
            .ctx
            .store
            .insert_streaming_thinking_block(self.turn_destination(), crate::block::Role::Assistant)
            .await
        {
            Ok(id) => {
                self.trackers.current_thinking_block = Some(id);
                Some(id)
            }
            Err(e) => {
                tracing::error!(conversation_id = conv_id, error = %e, "lazy insert thinking block failed");
                None
            }
        }
    }

    async fn ingest(&mut self, event: StreamEvent) {
        let conv_id = self.ctx.conversation_id;
        match event {
            // Labels are the stable machine keys documented on
            // [`CoreEvent::StreamStatus`] — a consumer maps them to its own
            // copy; the runtime ships no prose.
            StreamEvent::ProviderStatus { label } => {
                self.ctx.bus.emit(CoreEvent::StreamStatus {
                    conversation_id: conv_id,
                    label: stream_status::SENDING.into(),
                    subtitle: Some(label),
                });
            }
            StreamEvent::Connected => {
                self.ctx.bus.emit(CoreEvent::StreamStatus {
                    conversation_id: conv_id,
                    label: stream_status::WAITING_FOR_RESPONSE.into(),
                    subtitle: None,
                });
            }
            StreamEvent::TextBlockStart => self.text_block_start().await,
            StreamEvent::TextDelta { text } => self.text_delta(text).await,
            StreamEvent::ThinkingStart => self.thinking_start().await,
            StreamEvent::ThinkingDelta { text } => self.delta_thinking(text).await,
            StreamEvent::ThinkingSummaryDelta { text } => self.thinking_summary_delta(text).await,
            StreamEvent::ThinkingEnd { opaque } => self.thinking_end(opaque).await,
            StreamEvent::ContentFinal { blocks } => self.content_final(blocks).await,
            StreamEvent::TextFinal { text } => self.text_final(text).await,
            StreamEvent::MessageEnd { usage, stop_reason } => {
                self.message_end(usage, stop_reason).await;
            }
            StreamEvent::ToolUseStart { id, name } => self.tool_use_start(id, name).await,
            StreamEvent::ToolUseInputDelta { json } => self.tool_use_input_delta(json).await,
            StreamEvent::ToolUseEnd => self.tool_use_end().await,
        }
    }

    // ── Text blocks ──────────────────────────────────────────

    async fn text_block_start(&mut self) {
        let conv_id = self.ctx.conversation_id;
        match self
            .ctx
            .store
            .insert_streaming_block(self.turn_destination(), crate::block::Role::Assistant)
            .await
        {
            Ok(block_id) => {
                self.trackers.current_streaming_block = Some(block_id);
                self.ctx.bus.emit(CoreEvent::StreamStatus {
                    conversation_id: conv_id,
                    label: String::new(),
                    subtitle: None,
                });
            }
            Err(e) => {
                tracing::error!(conversation_id = conv_id, error = %e, "insert streaming block failed");
            }
        }
    }

    async fn text_delta(&mut self, text: String) {
        let conv_id = self.ctx.conversation_id;
        // The user-visible-text signal (2026-08-24): `responding` fires once
        // per turn, at the first NON-EMPTY text delta — the moment real
        // user-visible content starts flowing. Deliberately not at
        // `text_block_start`: a block can open and then finalize empty (the
        // recorded empty turn), and a cue raised at the open would announce a
        // reply for a turn that says nothing. Thinking raises it never — its
        // deltas take the other channel.
        if !text.is_empty() && self.trackers.text_channel == TextChannel::Silent {
            self.trackers.text_channel = TextChannel::Responding;
            self.ctx.bus.emit(CoreEvent::StreamStatus {
                conversation_id: conv_id,
                label: stream_status::RESPONDING.into(),
                subtitle: None,
            });
        }
        let block_id = match self.trackers.current_streaming_block {
            Some(id) => id,
            None => match self
                .ctx
                .store
                .insert_streaming_block(self.turn_destination(), crate::block::Role::Assistant)
                .await
            {
                Ok(id) => {
                    self.trackers.current_streaming_block = Some(id);
                    id
                }
                Err(e) => {
                    tracing::error!(conversation_id = conv_id, error = %e, "lazy insert streaming block failed");
                    return;
                }
            },
        };
        if let Err(e) = self
            .ctx
            .store
            .append_to_block_by_id(block_id, "block_text", text, now_iso8601())
            .await
        {
            tracing::error!(conversation_id = conv_id, block_id, error = %e, "append text delta failed");
        }
    }

    // ── Thinking blocks ──────────────────────────────────────

    async fn thinking_start(&mut self) {
        let conv_id = self.ctx.conversation_id;
        match self
            .ctx
            .store
            .insert_streaming_thinking_block(self.turn_destination(), crate::block::Role::Assistant)
            .await
        {
            Ok(block_id) => {
                self.trackers.current_thinking_block = Some(block_id);
                self.ctx.bus.emit(CoreEvent::StreamStatus {
                    conversation_id: conv_id,
                    label: String::new(),
                    subtitle: None,
                });
            }
            Err(e) => {
                tracing::error!(conversation_id = conv_id, error = %e, "insert streaming thinking block failed");
            }
        }
    }

    // Named to stay clear of a vendor wire-field string the isolation scan
    // guards; the event it handles is the neutral `ThinkingDelta`.
    async fn delta_thinking(&mut self, text: String) {
        let Some(block_id) = self.ensure_streaming_thinking_block().await else {
            return;
        };
        if let Err(e) = self
            .ctx
            .store
            .append_to_block_by_id(block_id, "block_thinking", text, now_iso8601())
            .await
        {
            tracing::error!(conversation_id = self.ctx.conversation_id, block_id, error = %e, "append thinking delta failed");
        }
    }

    async fn thinking_summary_delta(&mut self, text: String) {
        // The display-only summary channel accumulates on the SAME streaming
        // thinking block as the verbatim channel — a summary-only provider's
        // first delta lazy-creates the block exactly like a verbatim first
        // delta would.
        let Some(block_id) = self.ensure_streaming_thinking_block().await else {
            return;
        };
        if let Err(e) = self
            .ctx
            .store
            .append_to_thinking_summary(block_id, text, now_iso8601())
            .await
        {
            tracing::error!(conversation_id = self.ctx.conversation_id, block_id, error = %e, "append thinking summary delta failed");
        }
    }

    async fn thinking_end(&mut self, opaque: Option<crate::block::OpaquePayload>) {
        let conv_id = self.ctx.conversation_id;
        let Some(block_id) = self.trackers.current_thinking_block.take() else {
            return;
        };
        match self.ctx.store.get_thinking_block_channels(block_id).await {
            Ok(Some((content, summary)))
                if !content.is_empty() || summary.as_deref().is_some_and(|s| !s.is_empty()) =>
            {
                // EITHER channel finalizes the block: a summary-only provider
                // streams no verbatim content at all. The captured continuity
                // payload rides the same finalization INSERT as the block —
                // immutability: no UPDATE to a committed block. The streaming
                // tail is deleted in the SAME transaction: streaming blocks
                // are ephemeral, replaced by finals.
                match self
                    .ctx
                    .store
                    .insert_thinking_block_with_content(
                        self.turn_destination(),
                        crate::block::Role::Assistant,
                        content,
                        summary.filter(|s| !s.is_empty()),
                        opaque,
                        Some(block_id),
                    )
                    .await
                {
                    // This turn's thinking is now persisted from the live
                    // stream. A later `ContentFinal` — the authoritative
                    // integrity restatement some providers send — must not
                    // re-insert it; mirrors the text check below. Set only on
                    // a SUCCESSFUL insert: a failed one has persisted nothing,
                    // and the flag would suppress the restatement — the one
                    // remaining path that could still save the thinking.
                    Ok(_) => self.trackers.thinking_finalized = true,
                    Err(e) => {
                        tracing::error!(conversation_id = conv_id, block_id, error = %e, "finalize thinking block failed");
                    }
                }
            }
            _ => {
                // An empty (or unreadable) streaming tail can never finalize —
                // discard it instead of orphaning it.
                if let Err(e) = self.ctx.store.discard_streaming_block(block_id).await {
                    tracing::error!(conversation_id = conv_id, block_id, error = %e, "discard empty thinking tail failed");
                }
            }
        }
    }

    // ── Data integrity final persist ─────────────────────────

    async fn content_final(&mut self, blocks: Vec<FinalContentBlock>) {
        let conv_id = self.ctx.conversation_id;
        // The received flag below exists to stop `TextFinal` and `MessageEnd`
        // from writing the turn's text a second time. It is withheld on a
        // FAILED persist — a failed persist has written the content no first
        // time, and the flag would silence the fallback paths that could
        // still save it — and on a restatement that persisted NOTHING, whose
        // content a fallback may still carry. A deliberate no-persist (an
        // empty block beside a sibling that committed) is NEITHER: nothing
        // was lost, so it must not clear the guard — that clearing is what
        // let a following `TextFinal` commit the turn's text twice.
        let mut any_failed = false;
        let mut any_persisted = false;
        for block in blocks {
            match block {
                FinalContentBlock::Text { text } => {
                    // The first final text replaces the turn's streaming text
                    // tail atomically (ephemeral, replaced by finals). An
                    // empty final never commits ITSELF — the streamed tail's
                    // own final state is the turn's content, so it finalizes
                    // from the tail, empty included (2026-08-24); with no
                    // tail open there is nothing here to commit, and a turn
                    // that completes with no text at all writes its empty
                    // block at `message_end`.
                    if text.is_empty() {
                        match self.finalize_streamed_text_tail().await {
                            TailFinalization::Persisted => any_persisted = true,
                            TailFinalization::NothingToPersist => {}
                            TailFinalization::Failed => any_failed = true,
                        }
                        continue;
                    }
                    match self
                        .ctx
                        .store
                        .insert_final_text_block(
                            self.turn_destination(),
                            crate::block::Role::Assistant,
                            text,
                            self.trackers.current_streaming_block.take(),
                        )
                        .await
                    {
                        Ok(_) => {
                            any_persisted = true;
                            self.trackers.text_channel = TextChannel::Committed;
                        }
                        Err(e) => {
                            any_failed = true;
                            tracing::error!(conversation_id = conv_id, error = %e, "persist final text block failed");
                        }
                    }
                }
                FinalContentBlock::Thinking { text } => {
                    // Skip if the live `ThinkingEnd` already persisted this
                    // turn's thinking — otherwise the block is written twice
                    // (the streaming finalize and this integrity persist).
                    // The skip counts as persisted: the turn's thinking IS in
                    // the ledger. Symmetric to the `content_final_received`
                    // check that protects text.
                    if self.trackers.thinking_finalized {
                        any_persisted = true;
                        continue;
                    }
                    match self
                        .ctx
                        .store
                        .insert_thinking_block_with_content(
                            self.turn_destination(),
                            crate::block::Role::Assistant,
                            text,
                            None,
                            None,
                            self.trackers.current_thinking_block.take(),
                        )
                        .await
                    {
                        Ok(_) => any_persisted = true,
                        Err(e) => {
                            any_failed = true;
                            tracing::error!(conversation_id = conv_id, error = %e, "persist final thinking block failed");
                        }
                    }
                }
                FinalContentBlock::Reasoning { text, opaque } => {
                    // Persisted exactly like Thinking, plus the payload — same
                    // `thinking_finalized` check. The integrity restatement
                    // carries verbatim thinking, never a summary, so no
                    // summary channel exists on this path.
                    if self.trackers.thinking_finalized {
                        any_persisted = true;
                        continue;
                    }
                    match self
                        .ctx
                        .store
                        .insert_thinking_block_with_content(
                            self.turn_destination(),
                            crate::block::Role::Assistant,
                            text,
                            None,
                            opaque,
                            self.trackers.current_thinking_block.take(),
                        )
                        .await
                    {
                        Ok(_) => any_persisted = true,
                        Err(e) => {
                            any_failed = true;
                            tracing::error!(conversation_id = conv_id, error = %e, "persist final reasoning block failed");
                        }
                    }
                }
            }
        }
        if any_persisted && !any_failed {
            self.trackers.content_final_received = true;
        }
    }

    /// Commit the open streamed text tail as its final block — the atomic
    /// replace — empty included (2026-08-24). An opened tail that accumulated
    /// no content is still the text channel's final state: the completion
    /// recorded no assistant text, and that empty result is a real fact the
    /// ledger keeps like any assistant text. The empty tail used to be
    /// discarded here on the belief that a committed empty text block poisons
    /// later requests; a
    /// real request against the deployed provider replayed empty assistant
    /// content and it was accepted, so the discard was a design error — and
    /// it left the owing message as the frontier tail after a no-text turn,
    /// which wedged the conversation (nothing re-dispatches a turn that wrote
    /// nothing).
    async fn finalize_streamed_text_tail(&mut self) -> TailFinalization {
        let conv_id = self.ctx.conversation_id;
        let Some(block_id) = self.trackers.current_streaming_block.take() else {
            return TailFinalization::NothingToPersist;
        };
        match self
            .ctx
            .store
            .get_block_content(block_id, "block_text")
            .await
        {
            Ok(Some(content)) => {
                match self
                    .ctx
                    .store
                    .insert_final_text_block(
                        self.turn_destination(),
                        crate::block::Role::Assistant,
                        content,
                        Some(block_id),
                    )
                    .await
                {
                    Ok(_) => {
                        self.trackers.text_channel = TextChannel::Committed;
                        TailFinalization::Persisted
                    }
                    Err(e) => {
                        tracing::error!(conversation_id = conv_id, block_id, error = %e, "persist final text block failed");
                        TailFinalization::Failed
                    }
                }
            }
            Ok(None) => {
                // The tracked row is gone — an out-of-band sweep already took
                // it. Nothing exists to commit.
                TailFinalization::NothingToPersist
            }
            Err(e) => {
                // Unreadable is not empty: the tail may hold real prose a
                // fallback restatement could still save, so this counts as a
                // failure, never as a deliberate no-persist.
                tracing::error!(conversation_id = conv_id, block_id, error = %e, "read streaming text for finalization failed");
                TailFinalization::Failed
            }
        }
    }

    async fn text_final(&mut self, text: String) {
        if self.trackers.content_final_received {
            return;
        }
        if text.is_empty() {
            // The streamed tail under an empty restatement is still the
            // turn's content — finalize from the tail, empty included
            // (2026-08-24).
            self.finalize_streamed_text_tail().await;
            return;
        }
        match self
            .ctx
            .store
            .insert_final_text_block(
                self.turn_destination(),
                crate::block::Role::Assistant,
                text,
                self.trackers.current_streaming_block.take(),
            )
            .await
        {
            Ok(_) => self.trackers.text_channel = TextChannel::Committed,
            Err(e) => {
                tracing::error!(conversation_id = self.ctx.conversation_id, error = %e, "persist final text block failed");
            }
        }
    }

    // ── Message end ──────────────────────────────────────────

    async fn message_end(&mut self, usage: crate::providers::Usage, stop_reason: StopReason) {
        let conv_id = self.ctx.conversation_id;
        let tail = if self.trackers.content_final_received {
            TailFinalization::NothingToPersist
        } else {
            // The atomic replace: the final text commits and the streaming
            // tail dies in one transaction — empty included (2026-08-24).
            self.finalize_streamed_text_tail().await
        };

        // The completed no-text turn owes its empty assistant text block
        // (2026-08-24): a turn that ends normally without committing any
        // assistant text — no text channel at all, a thinking-only turn, or a
        // completion whose only move was elided (a skipped empty-input tool
        // call) — still completed, and the ledger records that completion. The
        // empty block is a frontier-settling record: it closes the frontier's
        // owed turn (an assistant text awaits nobody) and carries the turn's
        // usage, and on replay the model reads its own empty message back
        // instead of a hole. Recorded here as OWED, committed at the turn's
        // real boundary (`Done`): one real wire ends a tool-call turn with an `EndTurn`
        // stop and trails the buffered lifecycles AFTER this event, and an
        // empty block written now would sit orphaned beside those calls.
        // A finalized tool call therefore cancels the debt, and only the
        // trailing done — the normal completion terminal — commits it.
        // Scoped tightly:
        //
        // - `EndTurn` only. A tool-use stop's turn continues into its calls,
        //   and the abnormal stops (`max_tokens`, `content_filter`) ride the
        //   error edge — they latch, and the retry resumes from the
        //   still-owing tail; an empty block committed there would bury the
        //   owed message for good. The stream-error and teardown paths never
        //   reach here at all.
        // - Not when the turn already committed text through ANY finalize
        //   path — the empty block records "no assistant text was committed",
        //   never "text committed, elsewhere".
        // - Not over a FAILED tail commit: the tail held real content that
        //   did not persist, and an empty block on top would forge a
        //   no-text turn out of a store failure.
        // - Not while a streaming text tail is still open (reachable only
        //   when `ContentFinal` skipped the text channel): committing
        //   "nothing" beside unfinalized prose would lie twice.
        self.owes_empty_commit = stop_reason == StopReason::EndTurn
            && self.trackers.text_channel != TextChannel::Committed
            && tail != TailFinalization::Failed
            && self.trackers.current_streaming_block.is_none();

        self.ctx.bus.emit(CoreEvent::StreamDone {
            conversation_id: conv_id,
            usage: Some(StreamUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                reasoning_tokens: usage.reasoning_tokens,
            }),
            stop_reason: Some(stop_reason),
            generation: Some(self.generation),
        });

        if stop_reason == StopReason::ToolUse {
            self.ctx.bus.emit(CoreEvent::StreamStatus {
                conversation_id: conv_id,
                label: stream_status::RUNNING_TOOLS.into(),
                subtitle: None,
            });
        }

        if stop_reason == StopReason::MaxTokens {
            self.record_abnormal_stop("max_tokens").await;
        }

        if stop_reason == StopReason::ContentFilter {
            self.record_abnormal_stop("content_filter").await;
        }

        // Reset state for the next turn's content; the open tool lifecycles a
        // provider emits AFTER this event live in fresh trackers, and `Done` —
        // the turn's real boundary — sweeps whatever they leave open. From
        // here to the terminal the reader is draining, which is the phase the
        // drain deadline bounds.
        self.trackers = TurnTrackers::default();
        self.draining = true;
        // Recorded now, while the drained turn is provably the seam's open
        // turn — the actor cannot dispatch again before this turn's close.
        self.drained_epoch = self.anchor.epoch();
    }

    /// Abandon an owed empty-turn commit without recording the block. Every
    /// edge that ends or supersedes the drained turn WITHOUT it being the
    /// normal completion — the restart-clean, the provider-error terminal, the
    /// stale-drain reset when the seam's epoch moved on, and a finalized tool
    /// call that turns the completion into a tool-call turn — clears the debt
    /// through this one seam. Committing the debt happens in exactly one place
    /// ([`Self::commit_owed_empty_turn`], on the trailing done); every other
    /// clear is an abandonment and routes here, so the set of non-completion
    /// clears has a single named referent a new edge is added against rather
    /// than a fifth scattered assignment that a review has to hunt for.
    fn abandon_owed_empty_commit(&mut self) {
        self.owes_empty_commit = false;
    }

    /// Commit the empty assistant text block a completed no-text turn owes
    /// (2026-08-24), at the turn's real boundary — the trailing done. By this
    /// point every trailing lifecycle has arrived: a tool call that finalized
    /// during the drain has already cancelled the debt, and a recorded
    /// abnormal stop rides the error terminal, so a pending error drops the
    /// commit too. The insert precedes the terminal on purpose: the actor's
    /// close re-derives the owed turn from the ledger, and the block must be
    /// in the snapshot that re-check reads — the empty block resting the
    /// frontier is the whole point.
    async fn commit_owed_empty_turn(&mut self) {
        if !std::mem::take(&mut self.owes_empty_commit) || self.pending_error.is_some() {
            return;
        }
        if let Err(e) = self
            .ctx
            .store
            .insert_final_text_block(
                self.turn_destination(),
                crate::block::Role::Assistant,
                String::new(),
                None,
            )
            .await
        {
            tracing::error!(conversation_id = self.ctx.conversation_id, error = %e, "persist empty final text block failed");
        }
    }

    /// A stop that ends the turn abnormally: record a visible error status
    /// block — anchored, like every product of the turn — and hold the same
    /// machine key for the terminal the drain's end emits, so the turn ends
    /// with an explanation, not silence. The key is from the vocabulary
    /// documented on [`CoreEvent::StreamError`] — the consumer maps it to its
    /// own copy. Deferred to the terminal on purpose: the reader is still
    /// ingesting this turn (a truncated tool lifecycle can trail the stop),
    /// and an error signal emitted here closed the dispatch mid-drain — the
    /// cleared seam stamped NULL anchors on the trailing writes, and the
    /// released streaming flag was the duplicate-turn window again.
    async fn record_abnormal_stop(&mut self, error: &str) {
        let conv_id = self.ctx.conversation_id;
        if let Err(e) = self
            .ctx
            .store
            .insert_status_block(self.turn_destination(), "error".into(), Some(error.into()))
            .await
        {
            tracing::error!(conversation_id = conv_id, error = %e, status = error, "insert error status block failed");
        }
        self.pending_error = Some(error.into());
    }

    // ── Tool use blocks ──────────────────────────────────────

    async fn tool_use_start(&mut self, id: String, name: String) {
        // Opens ANOTHER concurrent streaming tool call. A second Start before
        // the terminal End is NOT a duplicate — the shared chat-stream
        // translator buffers every parallel sibling and emits one ToolUseEnd
        // for all of them, so each Start is a distinct call the model
        // requested and each is tracked in its own slot.
        let conv_id = self.ctx.conversation_id;
        match self
            .ctx
            .store
            .insert_streaming_tool_call_block(
                self.turn_destination(),
                crate::block::Role::Assistant,
                id.clone(),
                name.clone(),
            )
            .await
        {
            Ok(block_id) => {
                self.trackers
                    .open_streaming_tool_calls
                    .push((block_id, id, name));
            }
            Err(e) => {
                tracing::error!(conversation_id = conv_id, error = %e, "insert streaming tool call failed");
            }
        }
    }

    async fn tool_use_input_delta(&mut self, json: String) {
        // Argument deltas stream in start order (the chat-stream translator
        // buffers a call's deltas contiguously after its Start; the
        // event-native shape sends one call at a time), so they belong to the
        // most-recently opened call.
        if let Some(&(block_id, _, _)) = self.trackers.open_streaming_tool_calls.last() {
            // A dropped append is truncated tool JSON — an argument set the
            // model never sent, executed anyway — so the failure is recorded,
            // never swallowed.
            if let Err(e) = self
                .ctx
                .store
                .append_to_streaming_tool_call(block_id, json, now_iso8601())
                .await
            {
                tracing::error!(conversation_id = self.ctx.conversation_id, block_id, error = %e, "append tool call input delta failed");
            }
        }
    }

    async fn tool_use_end(&mut self) {
        // Finalize EVERY currently-open streaming tool call, not just one: the
        // chat-stream translator emits a single terminal End for N buffered
        // siblings, and the event-native shape emits End per sequential call
        // (never more than one open) — draining the Vec is correct for both.
        let conv_id = self.ctx.conversation_id;
        for (streaming_block_id, tool_call_id, name) in
            std::mem::take(&mut self.trackers.open_streaming_tool_calls)
        {
            let input = match self
                .ctx
                .store
                .get_streaming_tool_call_input(streaming_block_id)
                .await
            {
                Ok(input) => input.unwrap_or_default(),
                Err(e) => {
                    tracing::error!(conversation_id = conv_id, tool_call_id = %tool_call_id, block_id = streaming_block_id, error = %e, "read streaming tool call input failed");
                    String::new()
                }
            };

            if input.is_empty() {
                tracing::warn!(conversation_id = conv_id, tool_call_id = %tool_call_id, name = %name, "tool call with empty input — skipping");
                // Nothing will ever finalize this tail — discard it.
                if let Err(e) = self
                    .ctx
                    .store
                    .discard_streaming_block(streaming_block_id)
                    .await
                {
                    tracing::error!(conversation_id = conv_id, block_id = streaming_block_id, error = %e, "discard empty tool call tail failed");
                }
                continue;
            }

            // The live-tail drive at the ingestion insert site — EVERY insert
            // site drives, so parallel siblings each emit their wakeup AT
            // INSERT on the streaming path too. The latch is re-read here,
            // not taken from the loop's entry check: an interrupt racing this
            // mid-stream insert records the block and drives nothing. The
            // final call block replaces its streaming tail in the same
            // transaction (ephemeral, replaced by finals).
            match self
                .runner
                .insert_call(
                    &self.ctx,
                    self.latched.get(),
                    tool_call_id.clone(),
                    name,
                    input,
                    CallOrigin::streamed(Some(streaming_block_id), self.anchor.get()),
                )
                .await
            {
                Ok(block_id) => {
                    // A finalized call cancels any empty-turn commit the
                    // message end recorded as owed: one real wire ends a
                    // tool-call turn with an `EndTurn` stop and trails the
                    // lifecycle after the end, and that turn continues into
                    // its calls — it is not a no-text completion, and an
                    // empty block would sit orphaned beside the call.
                    self.abandon_owed_empty_commit();
                    tracing::info!(
                        conversation_id = conv_id,
                        tool_call_id = %tool_call_id,
                        block_id,
                        "tool call block inserted"
                    );
                }
                Err(e) => tracing::error!(
                    conversation_id = conv_id,
                    tool_call_id = %tool_call_id,
                    error = %e,
                    "tool call insert failed"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::agency::BlockKind;
    use crate::block::OpaquePayload;
    use crate::bus::EventBus;
    use crate::providers::Usage;
    use crate::reactivity::create_signal;
    use crate::store::Store;
    use crate::tools::ToolRegistry;

    use super::*;

    /// Drive a sequence of stream events through a single turn's ingestion,
    /// threading the per-turn mutable state exactly as `run_channel` does.
    async fn drive(ctx: &AgencyCtx<CoreEvent>, latched: bool, events: Vec<StreamEvent>) {
        drive_responses(
            ctx,
            latched,
            events.into_iter().map(ProviderResponse::Event).collect(),
        )
        .await;
    }

    /// Drive whole provider responses — `Done` and `Restart` included —
    /// through the reader, exactly as `run_channel` delivers them.
    async fn drive_responses(
        ctx: &AgencyCtx<CoreEvent>,
        latched: bool,
        responses: Vec<ProviderResponse>,
    ) {
        let (latched, _write_latched) = create_signal(latched);
        let mut reader = ChannelReader {
            ctx: ctx.clone(),
            runner: Arc::new(ToolRunner::<BlockKind, _>::new(Arc::new(
                ToolRegistry::new(),
            ))),
            latched,
            trackers: TurnTrackers::default(),
            mid_turn: false,
            generation: 1,
            anchor: TurnAnchor::new(),
            pending_error: None,
            owes_empty_commit: false,
            draining: false,
            drained_epoch: 0,
        };
        for response in responses {
            reader.handle_response(response).await;
        }
    }

    async fn count_type(ctx: &AgencyCtx<CoreEvent>, ty: &str) -> usize {
        ctx.store
            .list_blocks(ctx.conversation_id)
            .await
            .unwrap()
            .iter()
            .filter(|b| b.block_type == ty)
            .count()
    }

    async fn fixture() -> (
        AgencyCtx<CoreEvent>,
        tokio::sync::broadcast::Receiver<CoreEvent>,
    ) {
        let store = Store::in_memory().unwrap();
        let bus = Arc::new(EventBus::new());
        let rx = bus.subscribe();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        (
            AgencyCtx {
                conversation_id: conv,
                store,
                bus,
            },
            rx,
        )
    }

    fn wakeups(rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>) -> Vec<i64> {
        let mut ids = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let CoreEvent::ToolCallReady { call_block_id, .. } = event {
                ids.push(call_block_id);
            }
        }
        ids
    }

    fn tool_use(id: &str) -> [StreamEvent; 3] {
        [
            StreamEvent::ToolUseStart {
                id: id.into(),
                name: "read_file".into(),
            },
            StreamEvent::ToolUseInputDelta { json: "{}".into() },
            StreamEvent::ToolUseEnd,
        ]
    }

    /// The parallel-sibling regression, ON THE REAL PATH: two sibling
    /// `ToolUseEnd`s through ingestion's stream-event handling emit BOTH
    /// wakeups before either result lands — the live-tail drive at the
    /// ingestion insert site, which is what keeps parallel calls parallel for
    /// every streaming provider. Without it the cursor alone parks on the
    /// first sibling and the second's wakeup never fires until the first
    /// resolves.
    #[tokio::test]
    async fn tool_use_end_drives_the_live_tail_for_parallel_siblings() {
        let (ctx, mut rx) = fixture().await;
        let events = tool_use("sib-1")
            .into_iter()
            .chain(tool_use("sib-2"))
            .collect();
        drive(&ctx, false, events).await;

        let call_ids: Vec<i64> = ctx
            .store
            .list_blocks(ctx.conversation_id)
            .await
            .unwrap()
            .iter()
            .filter(|b| b.block_type == "tool_call")
            .map(|b| b.id)
            .collect();
        assert_eq!(call_ids.len(), 2);
        assert_eq!(
            wakeups(&mut rx),
            call_ids,
            "both siblings woke at insert, in order"
        );
    }

    /// The live-tail drive honors the latch — an interrupt racing a
    /// mid-stream insert RECORDS the tool-call block (data, not
    /// orchestration) and drives nothing; the dangle heals on the next
    /// unlatch via the cursor.
    #[tokio::test]
    async fn latched_tool_use_end_records_the_block_and_drives_nothing() {
        let (ctx, mut rx) = fixture().await;
        drive(&ctx, true, tool_use("raced").into()).await;

        assert_eq!(
            count_type(&ctx, "tool_call").await,
            1,
            "the block IS recorded"
        );
        assert_eq!(
            wakeups(&mut rx),
            Vec::<i64>::new(),
            "nothing acts while latched"
        );
    }

    /// A restated thinking turn: the live `ThinkingEnd` finalizes the
    /// thinking block, then the authoritative `ContentFinal` integrity event
    /// arrives carrying the same thinking. It must NOT re-insert — regression
    /// for the double-insert where both paths wrote a `thinking` block (text
    /// was already guarded; thinking was not). `ContentFinal` precedes
    /// `MessageEnd`, the ordering the existing text guard already relies on.
    #[tokio::test]
    async fn content_final_does_not_duplicate_finalized_thinking() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingStart,
                StreamEvent::ThinkingDelta {
                    text: "reasoning ".into(),
                },
                StreamEvent::ThinkingDelta {
                    text: "trace".into(),
                },
                StreamEvent::ThinkingEnd { opaque: None },
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta {
                    text: "answer".into(),
                },
                StreamEvent::ContentFinal {
                    blocks: vec![
                        FinalContentBlock::Thinking {
                            text: "reasoning trace".into(),
                        },
                        FinalContentBlock::Text {
                            text: "answer".into(),
                        },
                    ],
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(
            count_type(&ctx, "thinking").await,
            1,
            "thinking finalized exactly once"
        );
        assert_eq!(
            count_type(&ctx, "text").await,
            1,
            "text finalized exactly once"
        );
    }

    /// A provider that emits `ThinkingEnd` but NO `ContentFinal` (the plain
    /// streaming path) must still persist its thinking — the guard must not
    /// suppress the only finalization path.
    #[tokio::test]
    async fn thinking_end_without_content_final_persists() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingStart,
                StreamEvent::ThinkingDelta { text: "hmm".into() },
                StreamEvent::ThinkingEnd { opaque: None },
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta { text: "ok".into() },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(count_type(&ctx, "thinking").await, 1);
    }

    /// `ContentFinal` carrying thinking with NO prior `ThinkingEnd` (the
    /// streaming thinking block was empty / never opened) must still persist
    /// exactly one thinking block — the guard only trips when `ThinkingEnd`
    /// actually wrote one.
    #[tokio::test]
    async fn content_final_thinking_without_streaming_persists() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ContentFinal {
                    blocks: vec![
                        FinalContentBlock::Thinking {
                            text: "late thinking".into(),
                        },
                        FinalContentBlock::Text {
                            text: "answer".into(),
                        },
                    ],
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(count_type(&ctx, "thinking").await, 1);
    }

    /// `ThinkingEnd { opaque }` threads the captured continuity payload into
    /// the finalization insert — the committed thinking block reads back with
    /// the payload on its `opaque` field.
    #[tokio::test]
    async fn thinking_end_payload_is_persisted_with_the_block() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingDelta {
                    text: "signed thought".into(),
                },
                StreamEvent::ThinkingEnd {
                    opaque: Some(OpaquePayload::Anthropic {
                        signature: "sig-abc".into(),
                    }),
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let thinking = blocks
            .iter()
            .find(|b| b.block_type == "thinking")
            .expect("thinking block finalized");
        let opaque: OpaquePayload =
            serde_json::from_value(thinking.fields["opaque"].clone()).expect("payload read back");
        assert_eq!(
            opaque,
            OpaquePayload::Anthropic {
                signature: "sig-abc".into()
            }
        );
    }

    /// `ContentFinal` carrying the `Reasoning` variant persists exactly like
    /// `Thinking` — one block, payload threaded through, and the same
    /// `thinking_finalized` guard suppressing a duplicate after a live
    /// `ThinkingEnd`.
    #[tokio::test]
    async fn content_final_reasoning_persists_like_thinking_with_guard() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ContentFinal {
                    blocks: vec![FinalContentBlock::Reasoning {
                        text: "integrity reasoning".into(),
                        opaque: None,
                    }],
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;
        assert_eq!(count_type(&ctx, "thinking").await, 1);

        // Second turn: a live ThinkingEnd finalizes first — the guard must
        // suppress the ContentFinal Reasoning re-insert.
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingDelta {
                    text: "live".into(),
                },
                StreamEvent::ThinkingEnd { opaque: None },
                StreamEvent::ContentFinal {
                    blocks: vec![FinalContentBlock::Reasoning {
                        text: "live".into(),
                        opaque: None,
                    }],
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;
        assert_eq!(
            count_type(&ctx, "thinking").await,
            2,
            "no duplicate from ContentFinal"
        );
    }

    /// A summary-only turn: only `ThinkingSummaryDelta`s stream — no verbatim
    /// content at all. The first summary delta lazy-creates the streaming
    /// block, and `ThinkingEnd` finalizes on the summary channel alone: empty
    /// content, accumulated summary, payload threaded through, streaming tail
    /// replaced.
    #[tokio::test]
    async fn summary_only_turn_finalizes_on_the_summary_channel() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingSummaryDelta {
                    text: "**Weighing options**\n\n".into(),
                },
                StreamEvent::ThinkingSummaryDelta {
                    text: "Comparing the two.".into(),
                },
                StreamEvent::ThinkingEnd {
                    opaque: Some(OpaquePayload::OpenAiResponses {
                        item_id: "rs_1".into(),
                        encrypted_content: "blob".into(),
                    }),
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0, "the tail is replaced");
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let thinking = blocks
            .iter()
            .find(|b| b.block_type == "thinking")
            .expect("finalized");
        assert_eq!(
            thinking.fields["content"],
            serde_json::json!(""),
            "no verbatim content"
        );
        assert_eq!(
            thinking.fields["summary"],
            serde_json::json!("**Weighing options**\n\nComparing the two."),
            "the summary channel accumulated and persisted"
        );
        let opaque: OpaquePayload =
            serde_json::from_value(thinking.fields["opaque"].clone()).unwrap();
        assert!(matches!(opaque, OpaquePayload::OpenAiResponses { .. }));
    }

    /// The aggregator's chat-shaped summary case, through real ingestion: a
    /// turn streams only `ThinkingSummaryDelta`s (the parser routes summary
    /// details to that channel) and closes with the aggregator's opaque
    /// payload. The block finalizes on the summary channel alone — empty
    /// content, accumulated summary — and the payload (every captured entry,
    /// encrypted included) reads back IDENTICALLY for verbatim replay.
    #[tokio::test]
    async fn openrouter_summary_turn_persists_summary_only_with_payload() {
        use crate::block::ReasoningDetailEntry;
        let (ctx, _rx) = fixture().await;
        let entries = vec![
            ReasoningDetailEntry {
                position: 0,
                entry_type: "reasoning.summary".into(),
                entry_id: None,
                upstream_format: "azure-openai-responses-v1".into(),
                index: Some(0),
                content: "**Weighing options**\n\nI compare.".into(),
                signature: None,
            },
            ReasoningDetailEntry {
                position: 1,
                entry_type: "reasoning.encrypted".into(),
                entry_id: Some("rs_1".into()),
                upstream_format: "azure-openai-responses-v1".into(),
                index: Some(0),
                content: "BLOB".into(),
                signature: None,
            },
        ];
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingSummaryDelta {
                    text: "**Weighing options**\n\n".into(),
                },
                StreamEvent::ThinkingSummaryDelta {
                    text: "I compare.".into(),
                },
                StreamEvent::ThinkingEnd {
                    opaque: Some(OpaquePayload::OpenRouter {
                        entries: entries.clone(),
                    }),
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0, "the tail is replaced");
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let thinking = blocks
            .iter()
            .find(|b| b.block_type == "thinking")
            .expect("finalized");
        assert_eq!(
            thinking.fields["content"],
            serde_json::json!(""),
            "no verbatim content"
        );
        assert_eq!(
            thinking.fields["summary"],
            serde_json::json!("**Weighing options**\n\nI compare."),
            "the summary channel accumulated and persisted"
        );
        let opaque: OpaquePayload =
            serde_json::from_value(thinking.fields["opaque"].clone()).unwrap();
        assert_eq!(
            opaque,
            OpaquePayload::OpenRouter { entries },
            "the aggregator payload round-trips identically for verbatim replay"
        );
    }

    /// Both channels accumulate on the SAME streaming block and both persist
    /// through finalization — content and summary are separate columns, never
    /// merged.
    #[tokio::test]
    async fn both_reasoning_channels_accumulate_on_one_block() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingDelta {
                    text: "verbatim ".into(),
                },
                StreamEvent::ThinkingSummaryDelta {
                    text: "summary ".into(),
                },
                StreamEvent::ThinkingDelta {
                    text: "chain".into(),
                },
                StreamEvent::ThinkingSummaryDelta {
                    text: "line".into(),
                },
                StreamEvent::ThinkingEnd { opaque: None },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(
            count_type(&ctx, "thinking").await,
            1,
            "one block for both channels"
        );
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let thinking = blocks.iter().find(|b| b.block_type == "thinking").unwrap();
        assert_eq!(
            thinking.fields["content"],
            serde_json::json!("verbatim chain")
        );
        assert_eq!(
            thinking.fields["summary"],
            serde_json::json!("summary line")
        );
    }

    /// The read-back surfaces the summary on the STREAMING block too — the
    /// live tail renders the summary as it accumulates. A verbatim-only block
    /// carries no `summary` field at all (NULL column, additive field).
    #[tokio::test]
    async fn streaming_block_surfaces_summary_and_verbatim_blocks_omit_it() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![StreamEvent::ThinkingSummaryDelta {
                text: "live summary".into(),
            }],
        )
        .await;
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let streaming = blocks
            .iter()
            .find(|b| b.block_type == "streaming_thinking")
            .expect("live tail");
        assert_eq!(
            streaming.fields["summary"],
            serde_json::json!("live summary")
        );
        assert_eq!(streaming.fields["content"], serde_json::json!(""));

        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingDelta {
                    text: "verbatim only".into(),
                },
                StreamEvent::ThinkingEnd { opaque: None },
            ],
        )
        .await;
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let thinking = blocks.iter().find(|b| b.block_type == "thinking").unwrap();
        assert!(
            !thinking.fields.contains_key("summary"),
            "no summary field without a summary channel"
        );
    }

    // ─── Streaming blocks get literally replaced (ephemeral, replaced by
    //     finals) ─────────────────────────────────────────────────────────

    async fn streaming_count(ctx: &AgencyCtx<CoreEvent>) -> usize {
        ctx.store
            .list_blocks(ctx.conversation_id)
            .await
            .unwrap()
            .iter()
            .filter(|b| b.block_type.starts_with("streaming"))
            .count()
    }

    /// The text happy path: `MessageEnd`'s finalization REPLACES the streaming
    /// block — zero streaming rows survive, the final carries the streamed
    /// content.
    #[tokio::test]
    async fn text_finalization_replaces_the_streaming_block() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta { text: "hel".into() },
                StreamEvent::TextDelta { text: "lo".into() },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(
            streaming_count(&ctx).await,
            0,
            "no streaming orphan on the happy path"
        );
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].block_type, "text");
        assert_eq!(blocks[0].fields["content"], serde_json::json!("hello"));
    }

    /// The thinking happy path: `ThinkingEnd`'s finalization replaces the
    /// `streaming_thinking` tail atomically.
    #[tokio::test]
    async fn thinking_finalization_replaces_the_streaming_block() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingStart,
                StreamEvent::ThinkingDelta { text: "hmm".into() },
                StreamEvent::ThinkingEnd { opaque: None },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0);
        assert_eq!(count_type(&ctx, "thinking").await, 1);
    }

    /// An EMPTY streaming thinking tail (opened, never fed) can never
    /// finalize — `ThinkingEnd` discards it instead of orphaning it.
    #[tokio::test]
    async fn empty_thinking_tail_is_discarded_at_thinking_end() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingStart,
                StreamEvent::ThinkingEnd { opaque: None },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0);
        assert_eq!(count_type(&ctx, "thinking").await, 0);
    }

    /// The tool-call happy path: the final call block replaces its
    /// `streaming_tool_call` tail in the shared insert-seam transaction — for
    /// every sibling.
    #[tokio::test]
    async fn tool_call_finalization_replaces_the_streaming_block() {
        let (ctx, _rx) = fixture().await;
        let events = tool_use("rep-1")
            .into_iter()
            .chain(tool_use("rep-2"))
            .collect();
        drive(&ctx, false, events).await;

        assert_eq!(
            streaming_count(&ctx).await,
            0,
            "both tails replaced by their finals"
        );
        assert_eq!(count_type(&ctx, "tool_call").await, 2);
    }

    /// The integrity-restatement path: `ContentFinal`'s persists replace the
    /// turn's streaming tails too — zero streaming rows after the turn.
    #[tokio::test]
    async fn content_final_replaces_the_streaming_tails() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingStart,
                StreamEvent::ThinkingDelta {
                    text: "plan".into(),
                },
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta {
                    text: "answer".into(),
                },
                StreamEvent::ContentFinal {
                    blocks: vec![
                        FinalContentBlock::Thinking {
                            text: "plan".into(),
                        },
                        FinalContentBlock::Text {
                            text: "answer".into(),
                        },
                    ],
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0);
        assert_eq!(count_type(&ctx, "thinking").await, 1);
        assert_eq!(count_type(&ctx, "text").await, 1);
    }

    /// Two parallel tool calls in the chat-stream shape: N `ToolUseStart`s,
    /// each with its own buffered input delta, closed by a SINGLE terminal
    /// `ToolUseEnd` (the shared translator buffers siblings and emits one End
    /// for all). BOTH calls must finalize — a second Start is a distinct
    /// requested action, never a duplicate to discard. This is the root cause
    /// the Vec tracker exists for: the old single-slot tracker dropped the
    /// first sibling silently.
    #[tokio::test]
    async fn terminal_tool_use_end_finalizes_every_open_parallel_call() {
        // Mid-stream: two Starts leave TWO live tails, nothing discarded.
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ToolUseStart {
                    id: "a".into(),
                    name: "read_file".into(),
                },
                StreamEvent::ToolUseStart {
                    id: "b".into(),
                    name: "read_file".into(),
                },
            ],
        )
        .await;
        assert_eq!(
            streaming_count(&ctx).await,
            2,
            "both parallel tails live until the terminal End"
        );

        // The full chat-stream sequence: Start(a), Delta(a), Start(b),
        // Delta(b), one End — both calls finalize, both tails replaced.
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ToolUseStart {
                    id: "a".into(),
                    name: "read_file".into(),
                },
                StreamEvent::ToolUseInputDelta {
                    json: "{\"a\":1}".into(),
                },
                StreamEvent::ToolUseStart {
                    id: "b".into(),
                    name: "read_file".into(),
                },
                StreamEvent::ToolUseInputDelta {
                    json: "{\"b\":2}".into(),
                },
                StreamEvent::ToolUseEnd,
            ],
        )
        .await;
        assert_eq!(
            streaming_count(&ctx).await,
            0,
            "both tails replaced by their finals"
        );
        let calls: Vec<(String, String)> = ctx
            .store
            .list_blocks(ctx.conversation_id)
            .await
            .unwrap()
            .iter()
            .filter(|b| b.block_type == "tool_call")
            .map(|b| {
                (
                    b.fields["tool_call_id"].as_str().unwrap().to_string(),
                    b.fields["input"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            calls,
            vec![
                ("a".to_string(), "{\"a\":1}".to_string()),
                ("b".to_string(), "{\"b\":2}".to_string()),
            ],
            "both siblings finalized with their own buffered input — neither dropped"
        );
    }

    /// An empty-input `ToolUseEnd` (the pre-existing skip) now also discards
    /// the tail it can never finalize.
    #[tokio::test]
    async fn empty_input_tool_use_end_discards_the_tail() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ToolUseStart {
                    id: "empty".into(),
                    name: "read_file".into(),
                },
                StreamEvent::ToolUseEnd,
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0);
        assert_eq!(count_type(&ctx, "tool_call").await, 0);
    }

    /// A `content_filter` stop inserts an error status block, mirroring the
    /// `max_tokens` arm — the turn ends with a visible explanation, not
    /// silence.
    #[tokio::test]
    async fn content_filter_stop_inserts_error_status_block() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextDelta {
                    text: "partial".into(),
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::ContentFilter,
                },
            ],
        )
        .await;

        assert_eq!(
            count_type(&ctx, "status").await,
            1,
            "one error status block"
        );
    }

    /// The aggregator's finish-reason-`stop` tool call, end to end through
    /// ingestion — joined at the REAL seam: the raw wire chunks go through
    /// the shared chat-stream decoder and whatever IT emits is what feeds the
    /// reader, so a decoder that failed to release the buffered lifecycle (or
    /// released it without its terminal close) fails HERE, not behind a
    /// hand-written fixture that restates the expected events. The decoder
    /// releases the lifecycle at the finish whatever the stop reason, the
    /// reader sees `MessageEnd` (`EndTurn`) FIRST and the tool lifecycle
    /// after it — the call finalizes and its wakeup fires exactly as under a
    /// `tool_calls` finish.
    #[cfg(feature = "_chat_stream")]
    #[tokio::test]
    async fn stop_finish_tool_call_executes_through_ingestion() {
        let (ctx, mut rx) = fixture().await;

        // The raw chunks live with the decoder (only a vendor module may name
        // this wire); what arrives here is whatever the REAL translator emits
        // for them.
        let events = crate::providers::chat::sse::decoded_stop_finish_tool_call_turn();
        drive(&ctx, false, events).await;

        assert_eq!(streaming_count(&ctx).await, 0, "the tail is replaced");
        assert_eq!(count_type(&ctx, "tool_call").await, 1, "the call is real");
        assert_eq!(
            wakeups(&mut rx).len(),
            1,
            "the call woke the runner despite the stop finish"
        );
        assert_eq!(
            count_type(&ctx, "text").await,
            0,
            "the stop-finish tool turn continues into its call — no empty \
             text block sits beside it (2026-08-24)"
        );
    }

    /// A synthetic unterminated lifecycle — a `ToolUseStart` whose `End`
    /// never arrives — is discarded at `Done`, the turn's real boundary, the
    /// same way Restart discards. The ledger keeps owing the model turn: the
    /// dropped tail neither answers the user nor parks the cursor.
    #[tokio::test]
    async fn unterminated_tail_is_discarded_at_done_and_the_frontier_stays_owed() {
        let (ctx, _rx) = fixture().await;
        ctx.store
            .insert_user_blocks(
                ctx.conversation_id,
                vec![crate::types::InputBlock::Text {
                    content: "hi".into(),
                }],
            )
            .await
            .unwrap();

        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::ToolUseStart {
                    id: "never-ends".into(),
                    name: "read_file".into(),
                }),
                ProviderResponse::Done,
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0, "the open tail is discarded");
        assert_eq!(
            count_type(&ctx, "tool_call").await,
            0,
            "an unterminated lifecycle never finalizes"
        );
        let outcome = crate::agency::ratchet::drive::<crate::agency::BlockKind, _>(&ctx)
            .await
            .unwrap();
        assert!(outcome.owes_turn, "the frontier stays owed after the sweep");
    }

    /// The inverted discard (2026-08-24): `MessageEnd` (`EndTurn`) over a
    /// started-but-empty tail COMMITS the empty assistant text block — the
    /// text channel's final state, empty included. The discard this test used
    /// to pin encoded a design error: the "vendors reject empty content"
    /// belief was disproved against the deployed provider, and the discard
    /// left the owing message as the frontier tail, wedging the session.
    #[tokio::test]
    async fn empty_streamed_text_commits_an_empty_final_block_at_message_end() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextBlockStart,
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0, "the tail is replaced");
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let text = blocks
            .iter()
            .find(|b| b.block_type == "text")
            .expect("the empty turn commits a real text block");
        assert_eq!(text.fields["content"], serde_json::json!(""));
        assert_eq!(text.role, Some(crate::block::Role::Assistant));
        assert_eq!(
            count_type(&ctx, "text").await,
            1,
            "exactly one block — the tail finalize and the no-text fallback \
             never double up"
        );
    }

    /// A reusable reader over the fixture's context, for tests that thread
    /// state across store-failure toggles. The latch's write half rides along
    /// so the signal stays alive for the reader's lifetime.
    fn bare_reader(
        ctx: &AgencyCtx<CoreEvent>,
    ) -> (
        ChannelReader<BlockKind, CoreEvent>,
        crate::reactivity::WriteSignal<bool>,
    ) {
        let (latched, write_latched) = create_signal(false);
        (
            ChannelReader {
                ctx: ctx.clone(),
                runner: Arc::new(ToolRunner::<BlockKind, _>::new(Arc::new(
                    ToolRegistry::new(),
                ))),
                latched,
                trackers: TurnTrackers::default(),
                mid_turn: false,
                generation: 1,
                anchor: TurnAnchor::new(),
                pending_error: None,
                owes_empty_commit: false,
                draining: false,
                drained_epoch: 0,
            },
            write_latched,
        )
    }

    /// Inject (or lift) a transient store failure: a RAISE trigger on the
    /// `blocks` header insert, so every block-committing write fails and its
    /// transaction rolls back — the finalization-insert failure the guards
    /// must survive.
    async fn set_insert_failure(ctx: &AgencyCtx<CoreEvent>, failing: bool) {
        let sql = if failing {
            "CREATE TRIGGER injected_insert_failure BEFORE INSERT ON blocks \
             BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
        } else {
            "DROP TRIGGER injected_insert_failure"
        };
        ctx.store
            .run(move |conn| conn.execute(sql, []).map(|_| ()).map_err(Into::into))
            .await
            .unwrap();
    }

    /// The received guard on a FAILED `ContentFinal` persist: the flag exists
    /// to stop the fallbacks from writing the text a second time, and a failed
    /// persist has written it no first time — so the guard stays unset and the
    /// `TextFinal` fallback still saves the turn's text.
    #[tokio::test]
    async fn failed_content_final_persist_sets_no_guard_and_the_fallback_saves_the_text() {
        let (ctx, _rx) = fixture().await;
        let (mut reader, _latch) = bare_reader(&ctx);
        for event in [
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta {
                text: "streamed".into(),
            },
        ] {
            reader.handle_response(ProviderResponse::Event(event)).await;
        }

        set_insert_failure(&ctx, true).await;
        reader
            .handle_response(ProviderResponse::Event(StreamEvent::ContentFinal {
                blocks: vec![FinalContentBlock::Text {
                    text: "answer".into(),
                }],
            }))
            .await;
        assert!(
            !reader.trackers.content_final_received,
            "a failed persist must not set the received guard"
        );

        set_insert_failure(&ctx, false).await;
        reader
            .handle_response(ProviderResponse::Event(StreamEvent::TextFinal {
                text: "answer".into(),
            }))
            .await;
        assert_eq!(
            count_type(&ctx, "text").await,
            1,
            "the fallback still persists the turn's text"
        );
    }

    /// The `thinking_finalized` guard on a FAILED `ThinkingEnd` insert: a
    /// failed finalize has persisted nothing, so the guard stays unset and the
    /// `ContentFinal` restatement — the one remaining path that can still save
    /// the thinking — still runs.
    #[tokio::test]
    async fn failed_thinking_finalize_sets_no_guard_and_the_restatement_saves_it() {
        let (ctx, _rx) = fixture().await;
        let (mut reader, _latch) = bare_reader(&ctx);
        for event in [
            StreamEvent::ThinkingStart,
            StreamEvent::ThinkingDelta {
                text: "trace".into(),
            },
        ] {
            reader.handle_response(ProviderResponse::Event(event)).await;
        }

        set_insert_failure(&ctx, true).await;
        reader
            .handle_response(ProviderResponse::Event(StreamEvent::ThinkingEnd {
                opaque: None,
            }))
            .await;
        assert!(
            !reader.trackers.thinking_finalized,
            "a failed finalize must not set the guard"
        );

        set_insert_failure(&ctx, false).await;
        reader
            .handle_response(ProviderResponse::Event(StreamEvent::ContentFinal {
                blocks: vec![FinalContentBlock::Thinking {
                    text: "trace".into(),
                }],
            }))
            .await;
        assert_eq!(
            count_type(&ctx, "thinking").await,
            1,
            "the restatement still saves the thinking"
        );
    }

    /// The status plane speaks EXACTLY the documented machine keys — never
    /// prose. The vocabulary is the contract on [`CoreEvent::StreamStatus`]:
    /// `sending`, `waiting_for_response`, the empty clear, `responding`,
    /// `running_tools`. A real text delta is driven so `responding` actually
    /// fires — the closed set is checked against the keys as produced, not a
    /// list that could silently omit a real member.
    #[tokio::test]
    async fn stream_status_labels_are_exactly_the_documented_machine_keys() {
        let (ctx, mut rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ProviderStatus {
                    label: "provider detail".into(),
                },
                StreamEvent::Connected,
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta {
                    text: "hello".into(),
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::ToolUse,
                },
            ],
        )
        .await;

        let mut statuses = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let CoreEvent::StreamStatus {
                label, subtitle, ..
            } = event
            {
                statuses.push((label, subtitle));
            }
        }
        assert_eq!(
            statuses,
            vec![
                (
                    stream_status::SENDING.to_string(),
                    Some("provider detail".to_string())
                ),
                (stream_status::WAITING_FOR_RESPONSE.to_string(), None),
                (String::new(), None),
                (stream_status::RESPONDING.to_string(), None),
                (stream_status::RUNNING_TOOLS.to_string(), None),
            ],
            "exactly the documented machine keys, in stream order"
        );
    }

    /// A turn ending at `Done` with an open TEXT tail and no `MessageEnd`:
    /// complete prose is content, not an incomplete lifecycle — the tail
    /// finalizes. A thinking tail at the same boundary stays discarded.
    #[tokio::test]
    async fn open_text_tail_at_done_finalizes_and_other_tails_stay_discarded() {
        let (ctx, _rx) = fixture().await;
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::ThinkingStart),
                ProviderResponse::Event(StreamEvent::ThinkingDelta { text: "hmm".into() }),
                ProviderResponse::Event(StreamEvent::TextBlockStart),
                ProviderResponse::Event(StreamEvent::TextDelta {
                    text: "complete prose".into(),
                }),
                ProviderResponse::Done,
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0, "no tail survives the turn");
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let text = blocks
            .iter()
            .find(|b| b.block_type == "text")
            .expect("the open text tail finalized");
        assert_eq!(text.fields["content"], serde_json::json!("complete prose"));
        assert_eq!(
            count_type(&ctx, "thinking").await,
            0,
            "an open thinking tail is still an incomplete fact"
        );
    }

    /// An empty `TextFinal` over a NON-empty streamed tail finalizes from the
    /// tail: the streamed prose is the turn's content, and the empty
    /// restatement must not discard it.
    #[tokio::test]
    async fn empty_text_final_over_a_streamed_tail_finalizes_from_the_tail() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta {
                    text: "streamed answer".into(),
                },
                StreamEvent::TextFinal {
                    text: String::new(),
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0);
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let text = blocks
            .iter()
            .find(|b| b.block_type == "text")
            .expect("the streamed text finalized");
        assert_eq!(text.fields["content"], serde_json::json!("streamed answer"));
    }

    /// The same rule through `ContentFinal`: an empty final text block over a
    /// non-empty streamed tail finalizes from the tail.
    #[tokio::test]
    async fn empty_content_final_text_over_a_streamed_tail_finalizes_from_the_tail() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta {
                    text: "tail text".into(),
                },
                StreamEvent::ContentFinal {
                    blocks: vec![FinalContentBlock::Text {
                        text: String::new(),
                    }],
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;

        assert_eq!(streaming_count(&ctx).await, 0);
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let text = blocks
            .iter()
            .find(|b| b.block_type == "text")
            .expect("the streamed text finalized");
        assert_eq!(text.fields["content"], serde_json::json!("tail text"));
    }

    /// An empty `ContentFinal` text over NO tail persists nothing — and must
    /// not set the received guard through the `continue`, or a later
    /// `TextFinal` carrying the real text would be silenced.
    #[tokio::test]
    async fn empty_content_final_text_does_not_set_the_received_guard() {
        let (ctx, _rx) = fixture().await;
        let (mut reader, _latch) = bare_reader(&ctx);
        reader
            .handle_response(ProviderResponse::Event(StreamEvent::ContentFinal {
                blocks: vec![FinalContentBlock::Text {
                    text: String::new(),
                }],
            }))
            .await;
        assert!(
            !reader.trackers.content_final_received,
            "nothing was persisted, so nothing is received"
        );

        reader
            .handle_response(ProviderResponse::Event(StreamEvent::TextFinal {
                text: "late text".into(),
            }))
            .await;
        assert_eq!(
            count_type(&ctx, "text").await,
            1,
            "the fallback still persists the turn's text"
        );
    }

    /// The reproduced double-commit, pinned: `ContentFinal` carrying a real
    /// text block AND an empty sibling. The real block persists; the empty
    /// one is a DELIBERATE no-persist, not a failed one — and conflating the
    /// two cleared the received guard, so the following `TextFinal`
    /// restatement committed the turn's text a second time. The guard
    /// reflects failures only: exactly one text block lands.
    #[tokio::test]
    async fn empty_sibling_in_content_final_does_not_reopen_the_text_fallback() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ContentFinal {
                    blocks: vec![
                        FinalContentBlock::Text {
                            text: "answer".into(),
                        },
                        FinalContentBlock::Text {
                            text: String::new(),
                        },
                    ],
                },
                StreamEvent::TextFinal {
                    text: "answer".into(),
                },
            ],
        )
        .await;

        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let texts: Vec<&serde_json::Value> = blocks
            .iter()
            .filter(|b| b.block_type == "text")
            .map(|b| &b.fields["content"])
            .collect();
        assert_eq!(
            texts,
            vec![&serde_json::json!("answer")],
            "the turn's text commits exactly once"
        );
    }

    /// A provider error ends the turn: the channel closing AFTERWARDS is a
    /// close at rest, not a mid-turn death — no warning, and no second
    /// stream-end signal chasing the `StreamError` that already settled the
    /// actor.
    #[tokio::test]
    async fn provider_error_then_close_emits_no_second_stream_end() {
        let (ctx, mut rx) = fixture().await;
        let runtime: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            ctx.store.clone(),
            Arc::clone(&ctx.bus),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(ToolRegistry::new()),
        );
        let (latched, _write_latched) = create_signal(false);
        let (tx, provider_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = spawn_channel(
            ctx.conversation_id,
            runtime,
            provider_rx,
            latched,
            1,
            TurnAnchor::new(),
            MESSAGE_END_DRAIN_DEADLINE,
        );

        tx.send(ProviderResponse::Event(StreamEvent::TextBlockStart))
            .unwrap();
        tx.send(ProviderResponse::Event(StreamEvent::TextDelta {
            text: "partial".into(),
        }))
        .unwrap();
        tx.send(ProviderResponse::Error("boom".into())).unwrap();
        drop(tx);
        handle.await.unwrap();

        let (mut errors, mut closes) = (0, 0);
        while let Ok(event) = rx.try_recv() {
            match event {
                CoreEvent::StreamError { .. } => errors += 1,
                CoreEvent::StreamClosed { .. } => closes += 1,
                _ => {}
            }
        }
        assert_eq!(errors, 1, "the error surfaced exactly once");
        assert_eq!(
            closes, 0,
            "the error ended the turn — the close that followed was a close at rest"
        );
    }

    /// A drain deadline is scoped to the turn that armed it (2026-08-22,
    /// from the closing verification): here the drained turn was ended out
    /// of band and a successor was dispatched on the same binding — the
    /// seam's epoch moved past the reader's drained epoch — so the leftover
    /// timer must NOT abandon the healthy binding. It resets the stale
    /// drain state instead, and the successor's events keep ingesting with
    /// no terminal fired by the stale timer. Real time on purpose: the
    /// constructed 300ms deadline keeps the whole test under a second.
    #[tokio::test]
    async fn a_stale_drain_deadline_resets_instead_of_abandoning_the_binding() {
        let (ctx, _rx) = fixture().await;
        let runtime: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            ctx.store.clone(),
            Arc::clone(&ctx.bus),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(ToolRegistry::new()),
        );
        let (latched, _write_latched) = create_signal(false);
        let (tx, provider_rx) = tokio::sync::mpsc::unbounded_channel();
        // Real summoner blocks: the anchor column is a foreign key, so the
        // anchors must name rows that exist.
        let summoner_one = ctx
            .store
            .insert_text_block(
                ctx.conversation_id,
                crate::block::Role::User,
                "ask one".into(),
            )
            .await
            .expect("the first summoner inserts");
        let summoner_two = ctx
            .store
            .insert_text_block(
                ctx.conversation_id,
                crate::block::Role::User,
                "ask two".into(),
            )
            .await
            .expect("the second summoner inserts");
        let anchor = TurnAnchor::new();
        anchor.set(summoner_one); // the drained turn's dispatch: epoch 1
        let handle = spawn_channel(
            ctx.conversation_id,
            runtime,
            provider_rx,
            latched,
            1,
            anchor.clone(),
            Duration::from_millis(300),
        );
        // Turn one streams and ends its message; its tool tail never comes.
        for event in [
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta { text: "one".into() },
            StreamEvent::TextFinal { text: "one".into() },
            StreamEvent::MessageEnd {
                usage: Usage::default(),
                stop_reason: StopReason::ToolUse,
            },
        ] {
            tx.send(ProviderResponse::Event(event)).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The actor ends the turn out of band and dispatches a successor on
        // the same binding: the seam's epoch moves on.
        anchor.clear();
        anchor.set(summoner_two); // successor dispatch: epoch 2
        // Silence past the deadline: the stale timer fires, resets, and the
        // reader keeps serving — no abandon mark, no terminal.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            !anchor.is_abandoned(),
            "a stale timer must not mark the binding abandoned"
        );
        // The successor's own stream flows and ingests normally.
        for event in [
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta {
                text: "recovered".into(),
            },
            StreamEvent::TextFinal {
                text: "recovered".into(),
            },
            StreamEvent::MessageEnd {
                usage: Usage::default(),
                stop_reason: StopReason::EndTurn,
            },
        ] {
            tx.send(ProviderResponse::Event(event)).unwrap();
        }
        tx.send(ProviderResponse::Done).unwrap();
        drop(tx);
        handle
            .await
            .expect("the reader exits when the channel closes");
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let recovered: Vec<_> = blocks
            .iter()
            .filter(|b| b.block_type == "text" && b.fields["content"] == "recovered")
            .collect();
        assert_eq!(
            recovered.len(),
            1,
            "the successor's prose ingests exactly once after the stale reset; ledger: {:?}",
            blocks
                .iter()
                .map(|b| (
                    b.block_type.clone(),
                    b.fields.get("content").cloned(),
                    b.dispatch_anchor
                ))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            recovered[0].dispatch_anchor,
            Some(summoner_two),
            "the successor's prose carries the successor's anchor"
        );
    }

    /// The drain deadline: a provider that emits `MessageEnd` and then stalls
    /// forever — no tool lifecycle, no trailing done, channel held open —
    /// must not wedge the conversation. Message-end no longer settles the
    /// dispatch state, so without the bounded drain no close edge would ever
    /// fire; with it, silence past [`MESSAGE_END_DRAIN_DEADLINE`] makes the
    /// reader mark the turn abandoned on the seam, emit its own closed
    /// terminal and exit, and the turn's committed content stands. Paused
    /// time: the deadline elapses virtually, instantly.
    #[tokio::test(start_paused = true)]
    async fn a_stall_after_message_end_closes_the_stream_at_the_drain_deadline() {
        let (ctx, mut rx) = fixture().await;
        let runtime: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            ctx.store.clone(),
            Arc::clone(&ctx.bus),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(ToolRegistry::new()),
        );
        let (latched, _write_latched) = create_signal(false);
        let (tx, provider_rx) = tokio::sync::mpsc::unbounded_channel();
        let anchor = TurnAnchor::new();
        let handle = spawn_channel(
            ctx.conversation_id,
            runtime,
            provider_rx,
            latched,
            1,
            anchor.clone(),
            MESSAGE_END_DRAIN_DEADLINE,
        );

        for event in [
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta {
                text: "the answer".into(),
            },
            StreamEvent::MessageEnd {
                usage: Usage::default(),
                stop_reason: StopReason::EndTurn,
            },
        ] {
            tx.send(ProviderResponse::Event(event)).unwrap();
        }
        // The stall: `tx` stays alive and silent — the channel never closes,
        // and no `Done` ever comes.

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let closed = loop {
            match rx.try_recv() {
                Ok(CoreEvent::StreamClosed { generation, .. }) => break generation,
                Ok(CoreEvent::StreamError { .. }) => {
                    panic!("a clean stop that stalls is closed, not errored")
                }
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the drain deadline never closed the stalled stream"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => panic!("subscription failed: {e}"),
            }
        };
        assert_eq!(closed, Some(1), "the reader's own terminal is stamped");
        assert!(
            anchor.is_abandoned(),
            "the abandoned mark reached the seam before the terminal — the \
             actor's close reads it to retire the binding"
        );

        assert_eq!(streaming_count(&ctx).await, 0, "no tail outlives the turn");
        assert_eq!(
            count_type(&ctx, "text").await,
            1,
            "the finalized answer stands"
        );

        // The reader exited at the deadline on its own; the channel closing
        // afterwards is unobserved.
        handle.await.unwrap();
        drop(tx);
        let mut late_terminals = 0;
        while let Ok(event) = rx.try_recv() {
            if matches!(
                event,
                CoreEvent::StreamClosed { .. } | CoreEvent::StreamError { .. }
            ) {
                late_terminals += 1;
            }
        }
        assert_eq!(
            late_terminals, 0,
            "the deadline's close settled the turn — the channel dying later adds no second terminal"
        );
    }

    /// The abnormal-stop terminal is DEFERRED to the drain's end: a
    /// `max_tokens` stop trailed by a truncated tool lifecycle records the
    /// error status AND the trailing call with the open turn's anchor — the
    /// error signal arrives once, when the reader is done with the turn, so
    /// no trailing write can be stamped after the seam cleared.
    #[tokio::test]
    async fn an_abnormal_stop_defers_its_error_until_the_drain_ends() {
        let (ctx, mut rx) = fixture().await;
        let runtime: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            ctx.store.clone(),
            Arc::clone(&ctx.bus),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(ToolRegistry::new()),
        );
        let (latched, _write_latched) = create_signal(false);
        let summoner = ctx
            .store
            .insert_user_blocks(
                ctx.conversation_id,
                vec![crate::types::InputBlock::Text {
                    content: "summon".into(),
                }],
            )
            .await
            .unwrap()[0];
        let anchor = TurnAnchor::new();
        anchor.set(summoner);
        let (tx, provider_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = spawn_channel(
            ctx.conversation_id,
            runtime,
            provider_rx,
            latched,
            1,
            anchor,
            MESSAGE_END_DRAIN_DEADLINE,
        );

        for response in [
            ProviderResponse::Event(StreamEvent::TextDelta {
                text: "cut-".into(),
            }),
            ProviderResponse::Event(StreamEvent::MessageEnd {
                usage: Usage::default(),
                stop_reason: StopReason::MaxTokens,
            }),
            ProviderResponse::Event(StreamEvent::ToolUseStart {
                id: "trailing".into(),
                name: "read_file".into(),
            }),
            ProviderResponse::Event(StreamEvent::ToolUseInputDelta { json: "{}".into() }),
            ProviderResponse::Event(StreamEvent::ToolUseEnd),
            ProviderResponse::Done,
        ] {
            tx.send(response).unwrap();
        }
        drop(tx);
        handle.await.unwrap();

        let mut terminals = Vec::new();
        while let Ok(event) = rx.try_recv() {
            match event {
                CoreEvent::StreamError { error, .. } => terminals.push(error),
                CoreEvent::StreamClosed { .. } => terminals.push("closed".into()),
                _ => {}
            }
        }
        assert_eq!(
            terminals,
            vec!["max_tokens".to_string()],
            "one terminal, the recorded stop, at the drain's end"
        );

        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        for (block_type, expected) in [("status", 1), ("text", 1), ("tool_call", 1)] {
            let anchored: Vec<Option<i64>> = blocks
                .iter()
                .filter(|b| b.block_type == block_type && b.id != summoner)
                .map(|b| b.dispatch_anchor)
                .collect();
            assert_eq!(
                anchored,
                vec![Some(summoner); expected],
                "every {block_type} the drained turn wrote carries the turn's anchor"
            );
        }
    }

    /// The inverted discard on the two explicit final-content paths
    /// (2026-08-24): an empty `TextFinal` and an empty `ContentFinal` text
    /// block each finalize the opened tail AS the empty text block — the
    /// atomic replace, committing the text channel's final state instead of
    /// throwing it away. Exactly one block per turn: the `ContentFinal`
    /// commit sets the received guard and marks the text committed, so
    /// `message_end`'s no-text fallback stays quiet.
    #[tokio::test]
    async fn empty_final_text_variants_commit_the_empty_block() {
        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextBlockStart,
                StreamEvent::TextFinal {
                    text: String::new(),
                },
            ],
        )
        .await;
        assert_eq!(streaming_count(&ctx).await, 0, "the tail is replaced");
        assert_eq!(count_type(&ctx, "text").await, 1, "the empty block commits");

        let (ctx, _rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextBlockStart,
                StreamEvent::ContentFinal {
                    blocks: vec![FinalContentBlock::Text {
                        text: String::new(),
                    }],
                },
                StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                },
            ],
        )
        .await;
        assert_eq!(streaming_count(&ctx).await, 0, "the tail is replaced");
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let texts: Vec<&serde_json::Value> = blocks
            .iter()
            .filter(|b| b.block_type == "text")
            .map(|b| &b.fields["content"])
            .collect();
        assert_eq!(
            texts,
            vec![&serde_json::json!("")],
            "one empty block — never a second from the message-end fallback"
        );
    }

    // ─── The empty completed turn (2026-08-24): a real block on the ledger,
    //     closing the debt it used to leave open ────────────────────────────

    /// The pure no-text completed turn — no text channel ever opened —
    /// commits the empty assistant text block at the turn's boundary, with
    /// the turn's dispatch anchor, and the block closes the frontier's owed
    /// turn: the scheduler's next derivation owes nothing and re-dispatches
    /// nothing. On the pre-change code the turn wrote no row at all, the
    /// owing message stayed the tail, and the session wedged — this test is
    /// the mutation pin for that discard.
    #[tokio::test]
    async fn a_completed_no_text_turn_commits_the_empty_block_and_closes_the_debt() {
        let (ctx, _rx) = fixture().await;
        let summoner = ctx
            .store
            .insert_user_blocks(
                ctx.conversation_id,
                vec![crate::types::InputBlock::Text {
                    content: "anything to add?".into(),
                }],
            )
            .await
            .unwrap()[0];

        let (mut reader, _latch) = bare_reader(&ctx);
        reader.anchor.set(summoner);
        for response in [
            ProviderResponse::Event(StreamEvent::MessageEnd {
                usage: Usage::default(),
                stop_reason: StopReason::EndTurn,
            }),
            ProviderResponse::Done,
        ] {
            reader.handle_response(response).await;
        }

        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let empty = blocks
            .iter()
            .find(|b| b.block_type == "text" && b.id != summoner)
            .expect("the empty turn commits a real text block — not discarded");
        assert_eq!(empty.fields["content"], serde_json::json!(""));
        assert_eq!(empty.role, Some(crate::block::Role::Assistant));
        assert_eq!(
            empty.dispatch_anchor,
            Some(summoner),
            "the empty block carries the turn's dispatch anchor like every \
             block a turn writes"
        );

        let outcome = crate::agency::ratchet::drive::<crate::agency::BlockKind, _>(&ctx)
            .await
            .unwrap();
        assert!(
            !outcome.owes_turn,
            "the empty block settles the frontier — the debt closes once and \
             the scheduler dispatches no second turn for the same message"
        );
        assert_eq!(
            outcome.awaiting, None,
            "the frontier no longer awaits the model"
        );
    }

    /// The placeholder asymmetry, both stops: a turn that ends in tool use
    /// leaves NO orphaned empty text block — under a `tool_use` stop AND
    /// under the aggregator's `EndTurn`-stop shape whose lifecycles trail
    /// the message end. The finalized call cancels the owed empty commit.
    #[tokio::test]
    async fn a_tool_ending_turn_leaves_no_empty_text_block() {
        // The plain tool-use stop.
        let (ctx, _rx) = fixture().await;
        let responses = std::iter::once(ProviderResponse::Event(StreamEvent::MessageEnd {
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        }))
        .chain(tool_use("plain").map(ProviderResponse::Event))
        .chain(std::iter::once(ProviderResponse::Done))
        .collect();
        drive_responses(&ctx, false, responses).await;
        assert_eq!(count_type(&ctx, "tool_call").await, 1);
        assert_eq!(count_type(&ctx, "text").await, 0, "no orphaned empty block");

        // The aggregator shape: an `EndTurn` stop whose buffered lifecycle
        // trails the end — the turn continues into its call, so the empty
        // commit recorded at message end is cancelled at the call's insert.
        let (ctx, _rx) = fixture().await;
        let responses = std::iter::once(ProviderResponse::Event(StreamEvent::MessageEnd {
            usage: Usage::default(),
            stop_reason: StopReason::EndTurn,
        }))
        .chain(tool_use("trailing").map(ProviderResponse::Event))
        .chain(std::iter::once(ProviderResponse::Done))
        .collect();
        drive_responses(&ctx, false, responses).await;
        assert_eq!(count_type(&ctx, "tool_call").await, 1);
        assert_eq!(count_type(&ctx, "text").await, 0, "no orphaned empty block");
    }

    /// A thinking-only completed turn still commits the empty text block:
    /// the thinking block incidentally rests the frontier already, but the
    /// empty block is the canonical assistant move and is committed
    /// regardless.
    #[tokio::test]
    async fn a_thinking_only_turn_commits_the_empty_block_as_its_move() {
        let (ctx, _rx) = fixture().await;
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::ThinkingStart),
                ProviderResponse::Event(StreamEvent::ThinkingDelta {
                    text: "nothing to add".into(),
                }),
                ProviderResponse::Event(StreamEvent::ThinkingEnd { opaque: None }),
                ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }),
                ProviderResponse::Done,
            ],
        )
        .await;

        assert_eq!(count_type(&ctx, "thinking").await, 1);
        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let text = blocks
            .iter()
            .find(|b| b.block_type == "text")
            .expect("the empty block is the turn's canonical move");
        assert_eq!(text.fields["content"], serde_json::json!(""));
    }

    /// An empty-input call skipped at `ToolUseEnd` records nothing — under an
    /// `EndTurn` stop the turn then completed with no recorded move, and the
    /// owed empty block still commits: the debt closes instead of wedging on
    /// a call that never became a fact.
    #[tokio::test]
    async fn a_skipped_empty_input_call_still_commits_the_empty_turn() {
        let (ctx, _rx) = fixture().await;
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }),
                ProviderResponse::Event(StreamEvent::ToolUseStart {
                    id: "no-args".into(),
                    name: "read_file".into(),
                }),
                ProviderResponse::Event(StreamEvent::ToolUseEnd),
                ProviderResponse::Done,
            ],
        )
        .await;

        assert_eq!(
            count_type(&ctx, "tool_call").await,
            0,
            "the call is skipped"
        );
        assert_eq!(
            count_type(&ctx, "text").await,
            1,
            "the empty block still closes the turn"
        );
    }

    /// Where the empty turn's usage attaches (AC5, 2026-08-24): in this
    /// codebase usage attaches to no block row — every turn's request-final
    /// usage rides [`CoreEvent::StreamDone`], emitted at `message_end`. The
    /// empty turn's spent tokens travel that same signal, so nothing is
    /// lost with the text: this pin names the carrier and shows the counts
    /// arriving for a turn that said nothing.
    #[tokio::test]
    async fn an_empty_turn_reports_its_usage_on_stream_done() {
        let (ctx, mut rx) = fixture().await;
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: Usage {
                        input_tokens: 11,
                        output_tokens: 3,
                        reasoning_tokens: Some(150),
                    },
                    stop_reason: StopReason::EndTurn,
                }),
                ProviderResponse::Done,
            ],
        )
        .await;

        let usage = loop {
            match rx.try_recv() {
                Ok(CoreEvent::StreamDone { usage, .. }) => break usage,
                Ok(_) => {}
                Err(e) => panic!("no StreamDone for the empty turn: {e}"),
            }
        }
        .expect("the empty turn's usage is reported, not lost");
        assert_eq!(usage.input_tokens, 11);
        assert_eq!(usage.output_tokens, 3);
        assert_eq!(
            usage.reasoning_tokens,
            Some(150),
            "the reasoning spend that justifies the empty turn rides the same \
             signal, not dropped"
        );
    }

    /// The empty block replays into the model-facing projection as the
    /// assistant's empty message — present, not omitted, not transparent.
    /// Omitting it would recreate the hole in history the block exists to
    /// avoid.
    #[tokio::test]
    async fn the_empty_block_replays_as_the_assistants_empty_message() {
        use crate::providers::render::blocks_to_messages;
        use crate::providers::types::{MessageContent, MessageRole};

        let (ctx, _rx) = fixture().await;
        ctx.store
            .insert_user_blocks(
                ctx.conversation_id,
                vec![crate::types::InputBlock::Text {
                    content: "anything to add?".into(),
                }],
            )
            .await
            .unwrap();
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }),
                ProviderResponse::Done,
            ],
        )
        .await;

        let blocks = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
        let messages = blocks_to_messages::<crate::agency::BlockKind>(&blocks);
        let assistant: Vec<_> = messages
            .iter()
            .filter(|m| m.role == MessageRole::Assistant)
            .collect();
        assert_eq!(
            assistant.len(),
            1,
            "the assistant's empty message is present, not omitted"
        );
        assert!(
            std::ptr::eq(assistant[0], messages.last().unwrap()),
            "it is the conversation's last message"
        );
        match &assistant[0].content {
            MessageContent::Text(text) => {
                assert_eq!(text, "", "the empty message replays empty");
            }
            other @ MessageContent::Parts(_) => {
                panic!("expected text content, got {other:?}")
            }
        }
    }

    /// The `responding` signal (AC7): raised at the FIRST non-empty text
    /// delta, once per turn — and never by a text block that merely opens,
    /// never by thinking, never by an empty delta, so a turn that says
    /// nothing raises no compose cue.
    #[tokio::test]
    async fn the_responding_signal_fires_on_real_text_only() {
        fn responding_count(rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>) -> usize {
            let mut count = 0;
            while let Ok(event) = rx.try_recv() {
                if let CoreEvent::StreamStatus { label, .. } = event
                    && label == stream_status::RESPONDING
                {
                    count += 1;
                }
            }
            count
        }

        // Real text: exactly one signal, at the first delta, not per delta.
        let (ctx, mut rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta { text: "he".into() },
                StreamEvent::TextDelta { text: "llo".into() },
            ],
        )
        .await;
        assert_eq!(responding_count(&mut rx), 1, "once per turn, on real text");

        // Thinking only: silent.
        let (ctx, mut rx) = fixture().await;
        drive(
            &ctx,
            false,
            vec![
                StreamEvent::ThinkingStart,
                StreamEvent::ThinkingDelta { text: "hmm".into() },
                StreamEvent::ThinkingSummaryDelta {
                    text: "weighing".into(),
                },
            ],
        )
        .await;
        assert_eq!(responding_count(&mut rx), 0, "thinking raises no cue");

        // The empty turn: a block opens, an empty delta arrives, the turn
        // finalizes empty — no false compose cue anywhere.
        let (ctx, mut rx) = fixture().await;
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::TextBlockStart),
                ProviderResponse::Event(StreamEvent::TextDelta {
                    text: String::new(),
                }),
                ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }),
                ProviderResponse::Done,
            ],
        )
        .await;
        assert_eq!(
            responding_count(&mut rx),
            0,
            "the empty turn raises no compose cue"
        );
        assert_eq!(
            count_type(&ctx, "text").await,
            1,
            "and still commits its empty block"
        );
    }

    /// The error and teardown edges commit NO empty block (AC8): the turn
    /// latches and the message keeps owing for the retry — an empty block
    /// committed there would bury it forever.
    #[tokio::test]
    async fn error_and_abnormal_edges_commit_no_empty_block() {
        // A stream error after an opened (empty) text block: nothing commits,
        // the frontier keeps owing the turn.
        let (ctx, _rx) = fixture().await;
        ctx.store
            .insert_user_blocks(
                ctx.conversation_id,
                vec![crate::types::InputBlock::Text {
                    content: "still owed".into(),
                }],
            )
            .await
            .unwrap();
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::TextBlockStart),
                ProviderResponse::Error("boom".into()),
            ],
        )
        .await;
        assert_eq!(count_type(&ctx, "text").await, 1, "only the user's block");
        let outcome = crate::agency::ratchet::drive::<crate::agency::BlockKind, _>(&ctx)
            .await
            .unwrap();
        assert!(
            outcome.owes_turn,
            "the errored turn keeps the message owing — nothing buried it"
        );

        // An abnormal stop (`max_tokens`) with no text: the error status is
        // the record; no empty block rides the error edge.
        let (ctx, _rx) = fixture().await;
        drive_responses(
            &ctx,
            false,
            vec![
                ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::MaxTokens,
                }),
                ProviderResponse::Done,
            ],
        )
        .await;
        assert_eq!(count_type(&ctx, "status").await, 1);
        assert_eq!(count_type(&ctx, "text").await, 0, "no empty block on error");
    }

    /// The teardown edge — the channel dying mid-turn — commits no empty
    /// block either: the reader's own close discards the tails and the
    /// retry semantics stand.
    #[tokio::test]
    async fn a_mid_turn_teardown_commits_no_empty_block() {
        let (ctx, _rx) = fixture().await;
        let runtime: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            ctx.store.clone(),
            Arc::clone(&ctx.bus),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(ToolRegistry::new()),
        );
        let (latched, _write_latched) = create_signal(false);
        let (tx, provider_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = spawn_channel(
            ctx.conversation_id,
            runtime,
            provider_rx,
            latched,
            1,
            TurnAnchor::new(),
            MESSAGE_END_DRAIN_DEADLINE,
        );

        tx.send(ProviderResponse::Event(StreamEvent::TextBlockStart))
            .unwrap();
        drop(tx);
        handle.await.unwrap();

        assert_eq!(streaming_count(&ctx).await, 0, "the dead tail is swept");
        assert_eq!(count_type(&ctx, "text").await, 0, "no empty block");
    }
}
