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

use crate::agency::AgencyCtx;
use crate::bus::RuntimeEvent;
use crate::event::CoreEvent;
use crate::providers::types::{
    FinalContentBlock, ProviderResponse, ProviderRx, StopReason, StreamEvent,
};
use crate::reactivity::ReadSignal;
use crate::store::now_iso8601;
use crate::tools::ToolRunner;
use crate::types::StreamUsage;

use super::actor::RuntimeContext;

/// Per-conversation ingestion reader on a bound provider channel.
///
/// Reads [`ProviderResponse`]s, persists streaming events as blocks, and
/// bridges status/completion signals to the bus. When latched, incoming
/// events are silently discarded.
pub(crate) fn spawn_channel<E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    provider_rx: ProviderRx,
    latched: ReadSignal<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_channel(conv_id, ctx, provider_rx, latched))
}

/// One turn's mutable ingestion state, reset at every turn boundary — a
/// message end, a restart, a terminal error.
#[derive(Default)]
struct TurnTrackers {
    current_streaming_block: Option<i64>,
    current_thinking_block: Option<i64>,
    content_final_received: bool,
    thinking_finalized: bool,
    /// Every concurrently-open streaming tool call, in start order — the
    /// shared chat-stream translator buffers N parallel calls and emits a
    /// SINGLE terminal `ToolUseEnd`, so a Vec (not one slot) is what keeps
    /// genuine parallel siblings from clobbering each other.
    open_streaming_tool_calls: Vec<(i64, String, String)>,
}

/// The reader itself: the hook context, the runner whose insert seam
/// finalizes tool calls, the latch, and the turn's trackers.
struct ChannelReader<E> {
    ctx: AgencyCtx<E>,
    runner: Arc<ToolRunner<E>>,
    latched: ReadSignal<bool>,
    trackers: TurnTrackers,
}

async fn run_channel<E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    mut provider_rx: ProviderRx,
    latched: ReadSignal<bool>,
) {
    let mut reader = ChannelReader {
        ctx: ctx.agency(conv_id),
        runner: Arc::clone(ctx.runner()),
        latched,
        trackers: TurnTrackers::default(),
    };

    while let Some(response) = provider_rx.recv().await {
        if reader.latched.get() {
            tracing::debug!(
                conversation_id = conv_id,
                "latched — discarding provider response"
            );
            continue;
        }
        reader.handle_response(response).await;
    }

    tracing::info!(conversation_id = conv_id, "ingestion stopped");
}

impl<E: RuntimeEvent> ChannelReader<E> {
    async fn handle_response(&mut self, response: ProviderResponse) {
        let conv_id = self.ctx.conversation_id;
        match response {
            ProviderResponse::Event(stream_event) => self.ingest(stream_event).await,
            ProviderResponse::Restart => {
                // Restart-clean: a recoverable mid-stream drop. Discard the
                // turn's uncommitted streaming blocks and reset trackers so
                // the regenerated stream writes onto a clean slate with no
                // orphans.
                self.discard_streaming_tails("restart-clean").await;
            }
            ProviderResponse::Error(error) => {
                tracing::warn!(conversation_id = conv_id, error = %error, "provider error");
                self.ctx.bus.emit(CoreEvent::StreamError {
                    conversation_id: conv_id,
                    error,
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
            }
            ProviderResponse::Done => {
                self.ctx.bus.emit(CoreEvent::StreamClosed {
                    conversation_id: conv_id,
                });
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
            .insert_streaming_thinking_block(conv_id, crate::block::Role::Assistant)
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
            StreamEvent::ProviderStatus { label } => {
                self.ctx.bus.emit(CoreEvent::StreamStatus {
                    conversation_id: conv_id,
                    label: "Sending…".into(),
                    subtitle: Some(label),
                });
            }
            StreamEvent::Connected => {
                self.ctx.bus.emit(CoreEvent::StreamStatus {
                    conversation_id: conv_id,
                    label: "Waiting for response…".into(),
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
            .insert_streaming_block(conv_id, crate::block::Role::Assistant)
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
        let block_id = match self.trackers.current_streaming_block {
            Some(id) => id,
            None => match self
                .ctx
                .store
                .insert_streaming_block(conv_id, crate::block::Role::Assistant)
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
        let _ = self
            .ctx
            .store
            .append_to_block_by_id(block_id, "block_text", text, now_iso8601())
            .await;
    }

    // ── Thinking blocks ──────────────────────────────────────

    async fn thinking_start(&mut self) {
        let conv_id = self.ctx.conversation_id;
        match self
            .ctx
            .store
            .insert_streaming_thinking_block(conv_id, crate::block::Role::Assistant)
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
        let _ = self
            .ctx
            .store
            .append_to_block_by_id(block_id, "block_thinking", text, now_iso8601())
            .await;
    }

    async fn thinking_summary_delta(&mut self, text: String) {
        // The display-only summary channel accumulates on the SAME streaming
        // thinking block as the verbatim channel — a summary-only provider's
        // first delta lazy-creates the block exactly like a verbatim first
        // delta would.
        let Some(block_id) = self.ensure_streaming_thinking_block().await else {
            return;
        };
        let _ = self
            .ctx
            .store
            .append_to_thinking_summary(block_id, text, now_iso8601())
            .await;
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
                let _ = self
                    .ctx
                    .store
                    .insert_thinking_block_with_content(
                        conv_id,
                        crate::block::Role::Assistant,
                        content,
                        summary.filter(|s| !s.is_empty()),
                        opaque,
                        Some(block_id),
                    )
                    .await;
                // This turn's thinking is now persisted from the live stream.
                // A later `ContentFinal` — the authoritative integrity
                // restatement some providers send — must not re-insert it;
                // mirrors the text guard below.
                self.trackers.thinking_finalized = true;
            }
            _ => {
                // An empty (or unreadable) streaming tail can never finalize —
                // discard it instead of orphaning it.
                let _ = self.ctx.store.discard_streaming_block(block_id).await;
            }
        }
    }

    // ── Data integrity final persist ─────────────────────────

    async fn content_final(&mut self, blocks: Vec<FinalContentBlock>) {
        let conv_id = self.ctx.conversation_id;
        for block in blocks {
            match block {
                FinalContentBlock::Text { text } => {
                    // The first final text replaces the turn's streaming text
                    // tail atomically (ephemeral, replaced by finals).
                    let _ = self
                        .ctx
                        .store
                        .insert_final_text_block(
                            conv_id,
                            crate::block::Role::Assistant,
                            text,
                            self.trackers.current_streaming_block.take(),
                        )
                        .await;
                }
                FinalContentBlock::Thinking { text } => {
                    // Skip if the live `ThinkingEnd` already persisted this
                    // turn's thinking — otherwise the block is written twice
                    // (the streaming finalize and this integrity persist).
                    // Symmetric to the `content_final_received` guard that
                    // protects text.
                    if !self.trackers.thinking_finalized {
                        let _ = self
                            .ctx
                            .store
                            .insert_thinking_block_with_content(
                                conv_id,
                                crate::block::Role::Assistant,
                                text,
                                None,
                                None,
                                self.trackers.current_thinking_block.take(),
                            )
                            .await;
                    }
                }
                FinalContentBlock::Reasoning { text, opaque } => {
                    // Persisted exactly like Thinking, plus the payload — same
                    // `thinking_finalized` guard. The integrity restatement
                    // carries verbatim thinking, never a summary, so no
                    // summary channel exists on this path.
                    if !self.trackers.thinking_finalized {
                        let _ = self
                            .ctx
                            .store
                            .insert_thinking_block_with_content(
                                conv_id,
                                crate::block::Role::Assistant,
                                text,
                                None,
                                opaque,
                                self.trackers.current_thinking_block.take(),
                            )
                            .await;
                    }
                }
            }
        }
        self.trackers.content_final_received = true;
    }

    async fn text_final(&mut self, text: String) {
        if !self.trackers.content_final_received {
            let _ = self
                .ctx
                .store
                .insert_final_text_block(
                    self.ctx.conversation_id,
                    crate::block::Role::Assistant,
                    text,
                    self.trackers.current_streaming_block.take(),
                )
                .await;
        }
    }

    // ── Message end ──────────────────────────────────────────

    async fn message_end(&mut self, usage: crate::providers::Usage, stop_reason: StopReason) {
        let conv_id = self.ctx.conversation_id;
        if !self.trackers.content_final_received
            && let Some(block_id) = self.trackers.current_streaming_block.take()
            && let Ok(Some(content)) = self
                .ctx
                .store
                .get_block_content(block_id, "block_text")
                .await
        {
            // The atomic replace: the final text lands and the streaming
            // tail dies in one transaction.
            let _ = self
                .ctx
                .store
                .insert_final_text_block(
                    conv_id,
                    crate::block::Role::Assistant,
                    content,
                    Some(block_id),
                )
                .await;
        }

        self.ctx.bus.emit(CoreEvent::StreamDone {
            conversation_id: conv_id,
            usage: Some(StreamUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
            }),
            stop_reason: Some(stop_reason),
        });

        if stop_reason == StopReason::ToolUse {
            self.ctx.bus.emit(CoreEvent::StreamStatus {
                conversation_id: conv_id,
                label: "Running tools…".into(),
                subtitle: None,
            });
        }

        if stop_reason == StopReason::MaxTokens {
            self.end_with_error("Context window limit reached").await;
        }

        if stop_reason == StopReason::ContentFilter {
            self.end_with_error("Response stopped by content filter")
                .await;
        }

        // Reset state for the next turn.
        self.trackers = TurnTrackers::default();
    }

    /// A stop that ends the turn abnormally: record a visible error status
    /// block and surface the same text on the stream-error plane, so the turn
    /// ends with an explanation, not silence.
    async fn end_with_error(&mut self, error: &str) {
        let conv_id = self.ctx.conversation_id;
        let _ = self
            .ctx
            .store
            .insert_status_block(conv_id, "error".into(), Some(error.into()))
            .await;
        self.ctx.bus.emit(CoreEvent::StreamError {
            conversation_id: conv_id,
            error: error.into(),
        });
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
                conv_id,
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
        if let Some((block_id, _, _)) = self.trackers.open_streaming_tool_calls.last() {
            let _ = self
                .ctx
                .store
                .append_to_streaming_tool_call(*block_id, json, now_iso8601())
                .await;
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
            let input = self
                .ctx
                .store
                .get_streaming_tool_call_input(streaming_block_id)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();

            if input.is_empty() {
                tracing::warn!(conversation_id = conv_id, tool_call_id = %tool_call_id, name = %name, "tool call with empty input — skipping");
                // Nothing will ever finalize this tail — discard it.
                let _ = self
                    .ctx
                    .store
                    .discard_streaming_block(streaming_block_id)
                    .await;
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
                    Some(streaming_block_id),
                )
                .await
            {
                Ok(block_id) => tracing::info!(
                    conversation_id = conv_id,
                    tool_call_id = %tool_call_id,
                    block_id,
                    "tool call block inserted"
                ),
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
        let (latched, _write_latched) = create_signal(latched);
        let mut reader = ChannelReader {
            ctx: ctx.clone(),
            runner: Arc::new(ToolRunner::new(Arc::new(ToolRegistry::new()))),
            latched,
            trackers: TurnTrackers::default(),
        };
        for event in events {
            reader.ingest(event).await;
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
}
