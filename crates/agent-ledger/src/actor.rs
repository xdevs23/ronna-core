//! The session actor — the runtime's top: one reactor routing intents to
//! per-conversation actors, one scheduler per conversation driving the
//! ratchet, and one state broadcaster reporting what the drive found.
//!
//! # One scheduler drives a conversation's ratchet
//!
//! The scheduler loop is the SINGLE ratchet driver for its conversation. Two
//! concurrent drivers would double-run `run()` side effects — the recorded
//! double-turn defect: two wakeups for one send, the second tick's awaits
//! running slow enough to reach the decision holding a pre-turn snapshot, and
//! two identical requests going out. So the state broadcaster never drives: it
//! consumes the [`Outcome`](ratchet::Outcome) the scheduler publishes. The
//! actor's own `streaming` flag is the second half of the guard: however many
//! owed-turn signals the scheduler emits, at most one stream request is in
//! flight per conversation.
//!
//! # The latch
//!
//! The latch is the engagement switch, orthogonal to the ledger. A
//! conversation boots latched and nothing drives it — not the ratchet, not the
//! tool pipeline, not the metadata worker — until an explicit intent releases
//! it. A normal stream end never re-latches: the resting state of a finished
//! conversation is unlatched with a ledger that owes nothing, and continuation
//! is the frontier gate's read. Only a stream error latches, because a request
//! we built wrong fails identically on every retry, so it latches and surfaces
//! instead of auto-retrying.
//!
//! # The context split
//!
//! [`RuntimeContext`] is the whole of what a consumer hands the runtime: the
//! store, the bus, the provider registry and the tool registry. The
//! application this module was extracted from threaded a wider service locator
//! through here, bundling these with its own subsystems; the library's context
//! carries exactly what the library owns and nothing that knows what a product
//! is. A consumer keeps its own context and passes this one down.
//!
//! # One ported test that lives elsewhere
//!
//! The source module also carried the push hub and its panicking-sink test;
//! both moved with the bus in slice 1
//! (`bus::tests::a_panicking_sink_is_pruned_and_never_poisons_the_bus`), so
//! this module ports the remaining six of its seven tests and names the
//! seventh here instead of counting it twice.

use std::collections::HashMap;
use std::sync::Arc;

use crate::agency::{AgencyCtx, RuntimeKind, ratchet, redispatch};
use crate::bus::{EventBus, RuntimeEvent};
use crate::dispatch::TurnAnchor;
use crate::event::{AsCoreEvent, CoreEvent};
use crate::providers::types::ReasoningLevel;
use crate::providers::{ModelSelector, ProviderRegistry, ProviderRequest, ProviderTx};
use crate::reactive;
use crate::reactivity::{ReadSignal, WriteSignal, create_signal};
use crate::store::Store;
use crate::tools::{CallOrigin, ToolRegistry, ToolRunner, submit_approval};
use crate::types::{ApprovalChoice, InputBlock};

// ─── The runtime context ─────────────────────────────────────────────────────

/// Everything the runtime owns, bundled once: the store, the bus, the provider
/// registry and the tool registry. A consumer builds one and hands it to
/// [`spawn_reactor`]; nothing else is threaded through the actor.
///
/// This is deliberately NOT a service locator. It carries the four
/// collaborators the runtime itself is made of and no seam for anything else —
/// a consumer with subsystems of its own keeps them in a context of its own
/// and passes this one down. A wider bundle is how the layer this was
/// extracted from ended up knowing what its product did.
///
/// The [`ToolRunner`] is built here, once, from the registry the consumer
/// hands in. It is derived state rather than a fifth collaborator: one runner
/// per context keeps the in-flight guard process-wide, and a consumer that
/// could hand in its own runner could hand in two.
///
/// `K` is the kind the runtime is instantiated over and `E` the consumer's
/// event type; the library's own instantiation names
/// [`BlockKind`](crate::agency::BlockKind) for `K`. The [`RuntimeKind`] bound
/// lives HERE, where `K` is introduced: a kind missing one of the runtime's
/// halves errors at the context a consumer builds, not at a distant seam, and
/// every seam that names this type is compiler-forced to agree with it.
pub struct RuntimeContext<K: RuntimeKind, E> {
    store: Store,
    bus: Arc<EventBus<E>>,
    providers: Arc<ProviderRegistry>,
    runner: Arc<ToolRunner<K, E>>,
}

impl<K: RuntimeKind, E> Clone for RuntimeContext<K, E> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            bus: Arc::clone(&self.bus),
            providers: Arc::clone(&self.providers),
            runner: Arc::clone(&self.runner),
        }
    }
}

impl<K: RuntimeKind, E> RuntimeContext<K, E> {
    /// Bundle the runtime's collaborators. The tool registry is taken as the
    /// registry — the runner over it is built here, so there is exactly one.
    #[must_use]
    pub fn new(
        store: Store,
        bus: Arc<EventBus<E>>,
        providers: Arc<ProviderRegistry>,
        tools: Arc<ToolRegistry<E>>,
    ) -> Self {
        Self {
            store,
            bus,
            providers,
            runner: Arc::new(ToolRunner::new(tools)),
        }
    }

    /// The store handle.
    #[must_use]
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The event bus.
    #[must_use]
    pub fn bus(&self) -> &Arc<EventBus<E>> {
        &self.bus
    }

    /// The provider registry.
    #[must_use]
    pub fn providers(&self) -> &Arc<ProviderRegistry> {
        &self.providers
    }

    /// The one tool runner over the consumer's registry.
    #[must_use]
    pub fn runner(&self) -> &Arc<ToolRunner<K, E>> {
        &self.runner
    }

    /// The per-conversation hook context, derived rather than stored: the
    /// [`AgencyCtx`] is the ONE answer to what a hook may reach, and deriving
    /// it here keeps this bundle from becoming a second one.
    #[must_use]
    pub fn agency(&self, conversation_id: i64) -> AgencyCtx<E> {
        AgencyCtx {
            conversation_id,
            store: self.store.clone(),
            bus: Arc::clone(&self.bus),
        }
    }
}

// ─── Actor framework ─────────────────────────────────────────────────────────

/// Contract for per-conversation actors. Each actor declares what events it
/// handles (via `accepts`) and how to spawn itself (via `spawn`). The reactor
/// creates one instance per conversation and routes events through the filter.
///
/// Mailboxes carry [`CoreEvent`]: the reactor extracts the runtime's view of
/// each bus event once, at the routing site, so no actor ever sees a
/// consumer's own variants.
trait PerConversationActor<K: RuntimeKind, E> {
    fn spawn(id: i64, ctx: RuntimeContext<K, E>) -> tokio::sync::mpsc::UnboundedSender<CoreEvent>;
    fn accepts(event: &CoreEvent) -> bool;
}

/// Declare which [`CoreEvent`] variants an actor handles.
/// Used inside a `PerConversationActor` impl block.
macro_rules! handles {
    ($($event:ident),* $(,)?) => {
        fn accepts(event: &CoreEvent) -> bool {
            matches!(event, $( CoreEvent::$event { .. } )|*)
        }
    };
}

/// A routable actor entry: sender + filter.
struct ActorEntry {
    tx: tokio::sync::mpsc::UnboundedSender<CoreEvent>,
    accepts: fn(&CoreEvent) -> bool,
}

/// Register all per-conversation actor types. Generates `spawn_actor_set`,
/// which the reactor calls to create the full set of actors for a new
/// conversation.
macro_rules! register_actors {
    ($($actor:ident),* $(,)?) => {
        fn spawn_actor_set<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent>(
            id: i64,
            ctx: &RuntimeContext<K, E>,
        ) -> Vec<ActorEntry> {
            vec![$(
                ActorEntry {
                    tx: <$actor<K, E> as PerConversationActor<K, E>>::spawn(id, ctx.clone()),
                    accepts: <$actor<K, E> as PerConversationActor<K, E>>::accepts,
                },
            )*]
        }
    };
}

// ─── ConversationActor ───────────────────────────────────────────────────────

/// The primary per-conversation actor. A thin event handler that mutates
/// data — the reactive scheduler (spawned alongside) watches data changes and
/// drives turns automatically.
///
/// Owns the provider channel (via lazy `bind()`). The scheduler signals
/// `blocks_ready` when the frontier gate owes a model turn; the actor reads
/// the ledger, sends [`ProviderRequest::Stream`], and the ingestion reader
/// handles responses.
struct ConversationActor<K: RuntimeKind, E> {
    id: i64,
    ctx: RuntimeContext<K, E>,
    mailbox: tokio::sync::mpsc::UnboundedReceiver<CoreEvent>,
    write_latched: WriteSignal<bool>,
    read_latched: ReadSignal<bool>,
    blocks_ready: tokio::sync::mpsc::UnboundedReceiver<()>,
    /// The close edges' re-check nudge: a monotonic count the scheduler's
    /// reactive loop tracks, bumped when a close finds an owed-turn signal
    /// was swallowed while the turn was open. The nudge wakes the SCHEDULER,
    /// never the dispatch delivery directly: the scheduler's ratchet drive is
    /// the one place the owed-turn check lives — its parked gate included —
    /// so the close re-runs the whole check instead of a tail-only half of
    /// it, and the drive signals `blocks_ready` exactly when the re-checked
    /// ledger really owes a turn.
    recheck: WriteSignal<u64>,
    provider_tx: Option<ProviderTx>,
    /// The dispatch state (reworked 2026-08-22): opened at delivery, closed on
    /// exactly one of the stream's closed signal, its error signal, or the
    /// interrupt teardown. Message-end does NOT settle it — the closed signal
    /// is emitted after the reader drains the turn's tool lifecycles, so "the
    /// tool round is recorded" is covered by the edge itself. Settling at
    /// message-end was the proven duplicate-turn defect: a message appended
    /// after message-end and before the tool calls were recorded dispatched a
    /// second concurrent model turn.
    streaming: bool,
    /// Whether an owed-turn signal arrived while the dispatch state was open
    /// and was swallowed by the streaming stand-down. The close consumes it:
    /// closing nudges the scheduler to re-drive instead of signalling
    /// blindly, which keeps the ratchet drive the ONLY producer of owed-turn
    /// signals. Signalling blindly at every close was two live defects: a
    /// turn that wrote nothing left the frontier unchanged and the blind
    /// signal redispatched the same turn forever, and a multi-call tool
    /// round could meet the blind signal with a resolved sibling as the
    /// tail — a request carrying a still-dangling call, a shape the drive's
    /// own parked gate can never signal.
    owed_turn_deferred: bool,
    /// The binding identity of the CURRENT provider channel, incremented at
    /// every bind and stamped by that binding's ingestion reader on the
    /// stream-lifecycle signals it emits. A torn-down reader exits
    /// asynchronously, so its parting signal can arrive when the successor
    /// turn is already streaming — [`Self::owns_stream_signal`] is what keeps
    /// that stale signal from clearing the streaming flag on a live turn.
    stream_generation: u64,
    /// The CURRENT binding's per-turn dispatch seam: set at dispatch with the
    /// anchor resolved from the dispatch's own snapshot, cleared at close, and
    /// read by the binding's ingestion reader at every insert. Replaced at
    /// every bind, so a torn-down reader keeps its own, already-cleared slot
    /// and can never stamp a successor turn's identity.
    turn_anchor: TurnAnchor,
    /// How long a reader waits for a turn's remaining events after its
    /// message-end. The production value is the ingestion module's named
    /// constant; tests construct short ones to pin the expiry paths.
    drain_deadline: std::time::Duration,
}

impl<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent> ConversationActor<K, E> {
    async fn run(mut self) {
        tracing::info!(conversation_id = self.id, "conversation actor started");
        loop {
            let mut ready = false;
            tokio::select! {
                event = self.mailbox.recv() => {
                    match event {
                        Some(e) => self.handle(e).await,
                        None => break,
                    }
                }
                Some(()) = self.blocks_ready.recv() => {
                    ready = true;
                }
            }
            if ready {
                self.handle_blocks_ready().await;
            }
        }
        tracing::info!(conversation_id = self.id, "conversation actor stopped");
    }

    async fn handle(&mut self, event: CoreEvent) {
        let label: &'static str = (&event).into();
        tracing::debug!(conversation_id = self.id, event = label, "actor handle");
        match event {
            CoreEvent::DraftPromoted { .. } => self.handle_draft_promoted().await,
            CoreEvent::BlocksAppended { blocks, .. } => self.handle_blocks_appended(blocks).await,
            CoreEvent::ToolCallReceived {
                tool_call_id,
                name,
                input,
                ..
            } => {
                self.handle_tool_call_received(tool_call_id, name, input)
                    .await;
            }
            CoreEvent::ApprovalSubmitted {
                request_block_id,
                decision,
                system_reason,
                user_reason,
                ..
            } => {
                self.handle_approval_submitted(
                    request_block_id,
                    decision,
                    system_reason,
                    user_reason,
                )
                .await;
            }
            CoreEvent::InterruptRequested { .. } => self.handle_interrupt().await,
            CoreEvent::UnlatchRequested { .. } | CoreEvent::UnlatchAll { .. } => {
                self.handle_unlatch();
            }
            // The stream-lifecycle signals carry binding identity, and the
            // gate below is the one place it is enforced: a signal stamped
            // with a generation this actor no longer owns is a torn-down
            // reader's parting word, and acting on it would clear the
            // streaming flag on a live successor turn — the next delivery
            // would then dispatch a second concurrent stream.
            CoreEvent::StreamDone { generation, .. }
            | CoreEvent::StreamError { generation, .. }
            | CoreEvent::StreamClosed { generation, .. }
                if !self.owns_stream_signal(generation) =>
            {
                tracing::debug!(
                    conversation_id = self.id,
                    event = label,
                    ?generation,
                    current_generation = self.stream_generation,
                    "stale stream-lifecycle signal from a torn-down reader — ignored"
                );
            }
            CoreEvent::StreamDone { stop_reason, .. } => self.handle_stream_done(stop_reason),
            CoreEvent::StreamError { .. } => self.handle_stream_error(),
            CoreEvent::StreamClosed { .. } => self.handle_stream_closed(),
            _ => {}
        }
    }

    /// Whether a stream-lifecycle signal speaks for the binding this actor
    /// currently owns. An unstamped signal (`None`) is not scoped to any
    /// binding and always applies.
    ///
    /// The unstamped producers are the actor's own store-failure paths — a
    /// failed draft promotion, a failed user-block insert, a failed provider
    /// bind — which emit `StreamError { generation: None }` to latch the
    /// conversation, possibly while a turn is open. Unstamped by decision
    /// (2026-08-22): a store failure is a fact about this actor, not about
    /// any binding, and a stamped one would be discarded as stale if the
    /// binding were replaced between the emit and its delivery — losing the
    /// latch the error exists to set. So `None` means always-owned, and a
    /// store failure mid-turn closes the dispatch state through the error
    /// edge exactly like a reader-emitted error; the reader that outlives
    /// that close keeps discarding under the latch.
    fn owns_stream_signal(&self, generation: Option<u64>) -> bool {
        generation.is_none_or(|g| g == self.stream_generation)
    }

    // ── Event handlers ──────────────────────────────────────────────────

    async fn handle_draft_promoted(&mut self) {
        let store = &self.ctx.store;
        let bus = &self.ctx.bus;

        match store.promote_draft(self.id).await {
            Ok(ids) => {
                tracing::info!(
                    conversation_id = self.id,
                    block_count = ids.len(),
                    "draft promoted"
                );
            }
            Err(e) => {
                tracing::error!(conversation_id = self.id, error = %e, "draft promotion failed");
                bus.emit(CoreEvent::StreamError {
                    conversation_id: self.id,
                    error: e.to_string(),
                    generation: None,
                });
                return;
            }
        }

        self.write_latched.set(false);
    }

    async fn handle_blocks_appended(&mut self, blocks: Vec<InputBlock>) {
        tracing::info!(
            conversation_id = self.id,
            block_count = blocks.len(),
            "appending user blocks"
        );
        let store = &self.ctx.store;
        let bus = &self.ctx.bus;

        match store.insert_user_blocks(self.id, blocks).await {
            Ok(ids) => {
                tracing::info!(
                    conversation_id = self.id,
                    count = ids.len(),
                    "user blocks inserted"
                );
                for block_id in ids {
                    bus.emit(CoreEvent::BlocksChanged {
                        conversation_id: self.id,
                        block_id,
                    });
                }
            }
            Err(e) => {
                tracing::error!(conversation_id = self.id, error = %e, "block insert failed");
                bus.emit(CoreEvent::StreamError {
                    conversation_id: self.id,
                    error: e.to_string(),
                    generation: None,
                });
                return;
            }
        }

        self.write_latched.set(false);
    }

    async fn handle_tool_call_received(&self, tool_call_id: String, name: String, input: String) {
        let ctx = self.ctx.agency(self.id);
        // The out-of-band path never opened a streamed-input tail, so there is
        // nothing for the final call block to replace.
        match self
            .ctx
            .runner
            .insert_call(
                &ctx,
                self.read_latched.get(),
                tool_call_id.clone(),
                name,
                input,
                CallOrigin::default(),
            )
            .await
        {
            Ok(block_id) => {
                tracing::info!(
                    conversation_id = self.id,
                    tool_call_id,
                    block_id,
                    "tool call block inserted"
                );
            }
            Err(e) => {
                tracing::error!(conversation_id = self.id, tool_call_id, error = %e, "tool call insert failed");
            }
        }
    }

    async fn handle_approval_submitted(
        &self,
        request_block_id: i64,
        decision: ApprovalChoice,
        system_reason: Option<String>,
        user_reason: Option<String>,
    ) {
        let store = &self.ctx.store;
        match submit_approval(
            store,
            self.id,
            request_block_id,
            decision,
            system_reason,
            user_reason,
        )
        .await
        {
            Ok(block_id) => {
                tracing::info!(
                    conversation_id = self.id,
                    request_block_id,
                    block_id,
                    ?decision,
                    "approval decision recorded"
                );
            }
            Err(e) => {
                tracing::warn!(conversation_id = self.id, request_block_id, error = %e, "approval decision rejected");
            }
        }
    }

    async fn handle_interrupt(&mut self) {
        self.write_latched.set(true);
        // The teardown IS the turn's close — the third of the three close
        // edges. The torn-down reader's own terminal is discarded later by
        // generation, so this is the one place the interrupted turn's
        // dispatch state settles. The interrupted turn's anchor is read
        // before the seam clears: the status block below is ABOUT that turn.
        let interrupted_anchor = self.turn_anchor.get();
        self.close_dispatch(false);

        // TEARDOWN, not a second cleanup channel: cancel, then DROP the
        // provider channel and clear the binding. The cancelled turn's reader
        // still tracks this turn's rows — the very rows the sweep below
        // deletes — and a reader kept alive would serve the next turn with
        // those trackers: its deltas would append to deleted ids, nothing
        // would error, nothing would commit, and no assistant block would
        // land. Dropping the channel ends the provider task, the reader
        // drains out through its mid-turn close path (the tail cleanup comes
        // for free there), and the next turn lazily rebinds with a fresh
        // reader and fresh trackers.
        if let Some(tx) = self.provider_tx.take() {
            let _ = tx.send(ProviderRequest::Interrupt);
        }

        let store = &self.ctx.store;
        // The interrupt keeps sweeping the turn's streaming tails itself,
        // immediately before its status append — the IMMEDIATE ledger
        // cleanup. The cancelled turn closes nothing by design, the torn-down
        // reader's own cleanup runs later and touches only what it tracked —
        // without this sweep, the half-written answer stays live-looking in
        // the ledger until then.
        match store.delete_streaming_blocks(self.id).await {
            Ok(deleted) => {
                tracing::info!(
                    conversation_id = self.id,
                    deleted,
                    "interrupt discarded the turn's streaming tails"
                );
            }
            Err(e) => {
                tracing::error!(conversation_id = self.id, error = %e, "interrupt: deleting streaming tails failed");
            }
        }
        // The interrupt's status block takes the actor's current anchor — the
        // interrupted turn is the turn the status is about, and with no turn
        // in flight the status is nobody's product and records NULL.
        if let Err(e) = store
            .insert_status_block(
                crate::store::BlockDestination::anchored(self.id, interrupted_anchor),
                "interrupted".into(),
                None,
            )
            .await
        {
            tracing::error!(conversation_id = self.id, error = %e, "interrupt: status append failed");
        }

        tracing::info!(conversation_id = self.id, "interrupted, latched");
    }

    fn handle_stream_done(&mut self, stop_reason: Option<crate::types::StopReason>) {
        // Message-end no longer settles the dispatch state (2026-08-22): the
        // turn's tool lifecycles arrive AFTER this signal, and settling here
        // was the proven duplicate-turn window. Stop reason stays recorded
        // and surfaced data — it drives no continuation; that derives from
        // the ledger via the frontier gate — and the turn closes on the
        // closed signal, which the reader emits after the tool round drains.
        tracing::info!(conversation_id = self.id, ?stop_reason, "stream done");
    }

    fn handle_stream_error(&mut self) {
        self.close_dispatch(true);
        tracing::info!(conversation_id = self.id, "stream error, latched");
    }

    fn handle_stream_closed(&mut self) {
        self.close_dispatch(false);
        tracing::info!(conversation_id = self.id, "stream closed");
    }

    /// The single close point for the dispatch state, reached on exactly the
    /// three close edges: the stream's closed signal, its error signal, and
    /// the interrupt teardown. The error edge has two producers (2026-08-22):
    /// the binding's reader, whose signals are generation-stamped, and the
    /// actor's own store-failure paths, whose `StreamError` carries no
    /// generation and always applies — a store failure while a turn is open
    /// closes the dispatch state through here and latches, and the reader
    /// that outlives it keeps discarding under the latch (the ownership rule
    /// and the reason the stamp is absent are documented on
    /// [`Self::owns_stream_signal`]).
    ///
    /// Releases the streaming flag, retires the binding when the reader
    /// abandoned the closing turn at its drain deadline (the stalled
    /// provider broke the trailing-done contract, so its channel is never
    /// reused: the next dispatch rebinds fresh and the provider's late tail
    /// dies with the dropped channel instead of reaching a successor turn),
    /// closes the per-turn seam, latches on the error edge (a request we
    /// built wrong fails identically on every retry, so it latches and
    /// surfaces instead of auto-retrying — a normal close never re-latches),
    /// and re-runs the owed-turn check for a signal the open turn swallowed: a message
    /// absorbed while the turn was open woke the scheduler, its drive found
    /// the turn owed, and the delivery stood down on the streaming flag — so
    /// the close nudges the scheduler to re-drive, deterministically. The
    /// nudge goes to the SCHEDULER, not to the dispatch delivery: the drive
    /// re-runs the whole owed-turn check — parked calls included — against
    /// the closed ledger, so a tool round whose siblings are still resolving
    /// parks instead of dispatching a request around a dangling call. With
    /// nothing swallowed the close rests: the frontier was last answered by
    /// a gated drive, and a blind re-check here would redispatch a turn that
    /// wrote nothing forever.
    fn close_dispatch(&mut self, errored: bool) {
        self.streaming = false;
        if self.turn_anchor.is_abandoned() {
            tracing::info!(
                conversation_id = self.id,
                "the reader abandoned this turn at its drain deadline — \
                 retiring the binding"
            );
            self.provider_tx = None;
        }
        self.turn_anchor.clear();
        if errored {
            self.write_latched.set(true);
        }
        if std::mem::take(&mut self.owed_turn_deferred) {
            self.recheck.update(|nudges| *nudges += 1);
        }
    }

    fn handle_unlatch(&mut self) {
        self.write_latched.set(false);
        tracing::info!(conversation_id = self.id, "unlatched");
    }

    // ── Provider binding + stream dispatch ──────────────────────────────

    /// The scheduler's frontier gate fired — the cursor drained to a
    /// model-owed tail. Lazy-bind the provider if needed, then send the
    /// stream request.
    async fn handle_blocks_ready(&mut self) {
        // The latch is re-read at DELIVERY, not trusted from the emitting
        // tick: the scheduler rests while latched, but an owed-turn signal
        // already queued when an interrupt latched would otherwise fire here.
        // The latch covers the whole engagement family, and firing a turn is
        // that family's center — a latched delivery stands down; the owed
        // turn re-signals on the next unlatched tick.
        if self.read_latched.get() {
            tracing::debug!(
                conversation_id = self.id,
                "handle_blocks_ready: latched — standing down"
            );
            return;
        }
        if self.streaming {
            // Swallowed, not dropped: the scheduler found a turn owed while
            // this one was still open — the absorbed-message window. The
            // close consumes the deferral by nudging the scheduler to
            // re-drive, so the absorbed turn fires deterministically without
            // the actor ever producing an owed-turn signal of its own.
            self.owed_turn_deferred = true;
            tracing::debug!(
                conversation_id = self.id,
                "handle_blocks_ready: already streaming — deferred to the close"
            );
            return;
        }

        // No pending-call guard here: a dangling call parks the cursor, so
        // the frontier gate never fires over one — and every signal reaching
        // this point originates at a gated drive; the close edge re-checks
        // through that same drive rather than signalling here. The ledger can
        // still move between a drive and its delivery; the tail re-check
        // below is what stands a signal that went stale down.
        let store = &self.ctx.store;

        let blocks = match store.list_blocks(self.id).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(conversation_id = self.id, error = %e, "handle_blocks_ready: list_blocks failed");
                return;
            }
        };
        // The dispatch-snapshot anchor resolution (2026-08-22): the frontier
        // rule — [`ratchet::frontier_owes_turn`], the same one decision the
        // scheduler's drive signals on — is re-evaluated on the SAME snapshot
        // the model request is built from, never carried by the signal (the
        // slice's decision record: a signal-borne value races the very
        // appends this identity exists to record). A tail that no longer
        // owes a turn stands the dispatch down — which retired the
        // stale-signal window this site carried as a dated deferral
        // (2026-08-21, "known window, kept open on purpose"): a queued
        // owed-turn signal outliving its turn now fires against the answered
        // ledger and dies here instead of dispatching. The tail that DOES
        // owe is the dispatch's anchor source, per the shared rule's
        // inheritance.
        let Some(tail) = blocks.last() else {
            tracing::warn!(
                conversation_id = self.id,
                "handle_blocks_ready: list_blocks returned empty"
            );
            return;
        };
        let Some(anchor) = ratchet::frontier_owes_turn::<K>(tail) else {
            tracing::debug!(
                conversation_id = self.id,
                tail_block = tail.id,
                "handle_blocks_ready: the tail no longer owes a turn — standing down"
            );
            return;
        };

        let conv = match store.find_conversation(self.id).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                tracing::warn!(
                    conversation_id = self.id,
                    "handle_blocks_ready: conversation not found"
                );
                return;
            }
            Err(e) => {
                tracing::warn!(conversation_id = self.id, error = ?e, "handle_blocks_ready: find_conversation failed");
                return;
            }
        };

        let Some(provider_tx) = self.ensure_provider().await else {
            tracing::warn!(
                conversation_id = self.id,
                "handle_blocks_ready: no provider available"
            );
            return;
        };

        let tool_defs = self.ctx.runner.registry().definitions();
        // The stored form is the level's canonical key; a key this build does
        // not know defers to the provider's own default rather than failing
        // the turn.
        let reasoning = conv.reasoning.as_deref().and_then(ReasoningLevel::from_key);

        // The projection fold runs HERE, on the caller's side of the provider
        // boundary: the channel carries neutral messages, never blocks, so no
        // vendor ever parses a kind — a consumer kind's visibility to the
        // model is its own `Projection` impl's answer, made before any vendor
        // is involved.
        let messages = crate::providers::blocks_to_messages::<K>(&blocks);

        // The seam opens BEFORE the send: the reader on the other side of the
        // channel may insert its first block the moment the request lands,
        // and it must already read this turn's anchor. Safe against the close
        // edges for the same reason the streaming flag is: this actor task is
        // the only writer of the seam's turn boundaries.
        self.turn_anchor.set(anchor);

        if let Err(e) = provider_tx.send(ProviderRequest::Stream {
            messages,
            model: ModelSelector::Specific(conv.model.external_id),
            tools: tool_defs,
            reasoning,
        }) {
            tracing::error!(conversation_id = self.id, error = ?e, "handle_blocks_ready: provider channel closed");
            // No turn opened: the seam a failed send left set would leak this
            // identity onto a later binding's first inserts.
            self.turn_anchor.clear();
            return;
        }
        // Set AFTER the dispatch, which is safe HERE and only here because
        // this actor task is the sole writer of `streaming`: the stream-end
        // handlers that clear it run on this same task, so no clear can slip
        // in between the send above and this set. The metadata fulfillment
        // loop has the same shape across TWO tasks and must set its flag
        // before dispatching.
        self.streaming = true;

        tracing::info!(conversation_id = self.id, "sent stream request to provider");
    }

    /// Ensure we have a bound provider channel. Returns the sender, or `None`
    /// if binding failed (error is emitted to the bus).
    async fn ensure_provider(&mut self) -> Option<ProviderTx> {
        if let Some(ref tx) = self.provider_tx {
            if !tx.is_closed() {
                return Some(tx.clone());
            }
            // Channel closed — provider task died. Rebind.
            self.provider_tx = None;
        }

        match self.bind_provider().await {
            Ok(tx) => Some(tx),
            Err(e) => {
                tracing::error!(conversation_id = self.id, error = %e, "provider bind failed");
                let bus = &self.ctx.bus;
                bus.emit(CoreEvent::StreamError {
                    conversation_id: self.id,
                    error: e,
                    generation: None,
                });
                None
            }
        }
    }

    /// Resolve the provider module, call `bind()`, spawn the ingestion
    /// reader, and cache the sender.
    ///
    /// The configuration goes to `bind` exactly as stored. The source of this
    /// module additionally fabricated a scratch working directory into it for
    /// its subprocess provider; that provider stays with its application, and
    /// a provider that needs local state derives it from its own
    /// configuration — the runtime invents no paths.
    async fn bind_provider(&mut self) -> Result<ProviderTx, String> {
        let store = &self.ctx.store;

        let conv = store
            .find_conversation(self.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("conversation not found")?;

        let instance = store
            .find_provider_instance(conv.model.provider_id.clone())
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("provider {} not found", conv.model.provider_id))?;

        let module = self
            .ctx
            .providers
            .get(&instance.provider_type)
            .ok_or_else(|| format!("unknown provider type: {}", instance.provider_type))?;

        let config = module
            .get_config(conv.model.provider_id.clone())
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("provider {} config not found", conv.model.provider_id))?;

        let (provider_tx, provider_rx) =
            module.bind(self.id, conv.model.provider_id.clone(), config);

        // Each bind is a new binding identity: the reader carries it and
        // stamps it on its stream-lifecycle signals, so this actor can tell a
        // torn-down predecessor's late signal from this binding's. The
        // per-turn seam is fresh per binding for the same reason — a
        // torn-down reader keeps its own, already-cleared slot and can never
        // stamp a successor turn's anchor on its late writes.
        self.stream_generation += 1;
        self.turn_anchor = TurnAnchor::new();
        crate::ingestion::spawn_channel(
            self.id,
            self.ctx.clone(),
            provider_rx,
            self.read_latched.clone(),
            self.stream_generation,
            self.turn_anchor.clone(),
            self.drain_deadline,
        );

        self.provider_tx = Some(provider_tx.clone());
        tracing::info!(
            conversation_id = self.id,
            provider_type = instance.provider_type,
            "provider bound"
        );
        Ok(provider_tx)
    }
}

// ─── Reactive scheduler ─────────────────────────────────────────────────────

/// One scheduler tick: rest while latched (the whole ratchet family runs only
/// unlatched), otherwise drive the cursor, signal the actor when the frontier
/// gate owes a turn, and run the redispatch walk — which rides the same tick
/// but is ungated by the model-turn axis, so deferred work resumes even when
/// no turn is owed. Returns the drive's Outcome for the state broadcaster to
/// consume; `None` when nothing was driven.
pub(crate) async fn scheduler_tick<K: RuntimeKind, E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    latched: bool,
    blocks_ready: &tokio::sync::mpsc::UnboundedSender<()>,
) -> Option<ratchet::Outcome> {
    if latched {
        return None;
    }
    let outcome = match ratchet::drive::<K, E>(ctx).await {
        Ok(outcome) => {
            tracing::debug!(
                conversation_id = ctx.conversation_id,
                ?outcome,
                "scheduler tick"
            );
            if outcome.owes_turn {
                let _ = blocks_ready.send(());
            }
            Some(outcome)
        }
        Err(e) => {
            tracing::error!(conversation_id = ctx.conversation_id, error = %e, "scheduler: ratchet drive failed");
            None
        }
    };
    if let Err(e) = redispatch::walk::<K, E>(ctx).await {
        tracing::error!(conversation_id = ctx.conversation_id, error = %e, "scheduler: redispatch walk failed");
    }
    outcome
}

/// Turn scheduler — the SINGLE ratchet driver for its conversation. Two
/// concurrent drivers would double-run `run()` side effects, so the state
/// broadcaster never drives: it consumes the Outcome this loop publishes
/// through `write_outcome`. The actor checks its own `streaming` flag to
/// prevent duplicate requests.
///
/// # The cursor-confirm feedback edge, ruled
///
/// Confirming progress writes the cursor onto the `conversations` row, and
/// that table is one this loop wakes on — so every confirm buys the loop one
/// extra tick. The ruling (2026-08-21, deferred here by the store slice): the
/// scheduler TOLERATES the tick rather than filtering the wake. The extra tick
/// is a no-op — the drive re-reads, confirms nothing new, writes nothing — so
/// the edge feeds back exactly once per confirm and rests, which
/// `cursor_confirm_wakes_once_and_converges_to_rest` pins. Filtering was
/// rejected because the wake path is deliberately kind- and column-blind: a
/// filter would need this loop to know which table columns are "orchestration"
/// and which are "content", a distinction nothing else in the machinery draws,
/// and the first new column would silently fall on the wrong side of it.
async fn run_scheduler<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    read_latched: ReadSignal<bool>,
    recheck: ReadSignal<u64>,
    blocks_ready: tokio::sync::mpsc::UnboundedSender<()>,
    write_outcome: WriteSignal<Option<ratchet::Outcome>>,
) {
    let agency = ctx.agency(conv_id);
    let db_changes = ctx.store.changes.watcher();

    reactive! {
        let latched = read_latched.get();
        // The close edges' re-check rides an ordinary tick (2026-08-22): a
        // turn's close found a swallowed owed-turn signal and asks for one
        // re-derivation. The drive below IS the re-check — the one place the
        // owed-turn decision lives, parked calls included. A bare signal is
        // sound here despite the reactivity module's lost-wakeup warning:
        // this loop subscribes before it works, so a nudge can go unheard
        // only in the instants before a body run that is already committed —
        // and that body's drive re-derives the owed turn from the store,
        // whose writes all precede the nudge. Every wakeup this loop can
        // lose is followed by the very re-check it asked for.
        let _ = recheck.get();
        db_changes.react();

        if let Some(outcome) = scheduler_tick::<K, E>(&agency, latched, &blocks_ready).await {
            write_outcome.set_if_changed(Some(outcome));
        }
    }
}

/// State broadcaster — emits [`CoreEvent::ConversationState`] whenever the
/// latch or the scheduler's published drive Outcome changes. Read-only by
/// design: it never drives the ratchet (the scheduler is the single driver).
/// `work_due` = the frontier owes a turn or the cursor is parked; a latched
/// conversation is not driven, so `work_due` holds the last unlatched
/// derivation — false until the first drive.
async fn run_state_broadcaster<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    read_latched: ReadSignal<bool>,
    read_outcome: ReadSignal<Option<ratchet::Outcome>>,
) {
    let bus = &ctx.bus;

    reactive! {
        let latched = read_latched.get();
        let outcome = read_outcome.get();
        let work_due = outcome.is_some_and(|o| o.owes_turn || o.parked);
        let awaiting = outcome.and_then(|o| o.awaiting);

        bus.emit(CoreEvent::ConversationState {
            conversation_id: conv_id,
            latched,
            work_due,
            awaiting,
        });
    }
}

// ─── The tool pipeline ───────────────────────────────────────────────────────

/// Spawn the reactive loops that drive the tool pipeline for a single
/// conversation: the executor that feeds wakeups to the runner chokepoint,
/// plus whatever per-conversation loops the registered handlers ask for
/// through [`crate::tools::ToolHandler::spawn_reactor`]. Returns handles so
/// the caller can abort on shutdown.
fn spawn_tool_pipeline<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: &RuntimeContext<K, E>,
    latched: &ReadSignal<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    // Subscribed here, before the spawn, so no wakeup can slip between the
    // caller's world starting to move and the loop's own subscription.
    let executor_rx = ctx.bus.subscribe();
    let mut handles = vec![tokio::spawn(run_executor(
        conv_id,
        ctx.clone(),
        latched.clone(),
        executor_rx,
    ))];
    for handler in ctx.runner.registry().handlers() {
        if let Some(h) = handler.spawn_reactor(ctx.agency(conv_id), latched.clone()) {
            handles.push(h);
        }
    }
    handles
}

/// The runner's wakeup loop: consumes every [`CoreEvent::ToolCallReady`] for
/// this conversation off the subscription the caller took BEFORE spawning, and
/// feeds each to the chokepoint. The latch is read at delivery and handed to
/// the runner, which DROPS a latched wakeup rather than deferring it — the
/// latch is a full short-circuit: the parked call re-emits on the next
/// unlatched tick, the same recovery that covers a wakeup lost to lag; the
/// ratchet is the retry loop.
///
/// Each wakeup's body runs in a task of its own, never awaited inline here: a
/// loop that awaits one body serializes every parallel sibling behind it, and
/// parallel calls are promised parallel. The runner's claim set and its
/// conditional resolution writes are what make concurrent bodies safe.
///
/// The bodies join a [`tokio::task::JoinSet`] OWNED by this loop, so aborting
/// the executor aborts the in-flight bodies with it — the pipeline handles
/// returned to the actor really do end the pipeline, bodies included. The set
/// adds NO concurrency bound: every admitted wakeup still spawns immediately;
/// settled bodies are merely reaped so the set holds only in-flight ones.
async fn run_executor<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    latched: ReadSignal<bool>,
    mut rx: tokio::sync::broadcast::Receiver<E>,
) {
    let agency = ctx.agency(conv_id);
    let mut bodies = tokio::task::JoinSet::new();

    loop {
        match rx.recv().await {
            Ok(event) => {
                while bodies.try_join_next().is_some() {}
                if let Some(&CoreEvent::ToolCallReady {
                    conversation_id,
                    call_block_id,
                }) = event.as_core()
                    && conversation_id == conv_id
                {
                    let runner = Arc::clone(&ctx.runner);
                    let agency = agency.clone();
                    let latched_at_delivery = latched.get();
                    bodies.spawn(async move {
                        runner
                            .run_wakeup(&agency, latched_at_delivery, call_block_id)
                            .await;
                    });
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(
                    conversation_id = conv_id,
                    skipped = n,
                    "tool executor lagged — wakeups dropped"
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

// ─── Spawning one conversation's actor set ───────────────────────────────────

impl<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent> PerConversationActor<K, E>
    for ConversationActor<K, E>
{
    fn spawn(id: i64, ctx: RuntimeContext<K, E>) -> tokio::sync::mpsc::UnboundedSender<CoreEvent> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Boot-latched: nothing drives a conversation until an explicit intent
        // (an append, a promotion, an unlatch) releases it, so a process
        // restart cannot fire turns out of a ledger nobody asked to resume.
        let (read_latched, write_latched) = create_signal(true);

        let (blocks_ready_tx, blocks_ready_rx) = tokio::sync::mpsc::unbounded_channel();
        let (read_outcome, write_outcome) = create_signal(None::<ratchet::Outcome>);
        // The close edges' re-check nudge, actor-written and scheduler-read.
        let (read_recheck, write_recheck) = create_signal(0u64);

        let scheduler = tokio::spawn(run_scheduler(
            id,
            ctx.clone(),
            read_latched.clone(),
            read_recheck,
            blocks_ready_tx,
            write_outcome,
        ));
        let tool_handles = spawn_tool_pipeline(id, &ctx, &read_latched);
        let state_broadcaster = tokio::spawn(run_state_broadcaster(
            id,
            ctx.clone(),
            read_latched.clone(),
            read_outcome,
        ));
        // The metadata subsystem rides the SAME latch signal: a latched
        // conversation drives neither ledger.
        let metadata_handles = crate::metadata::spawn(id, ctx.clone(), read_latched.clone());

        let actor = ConversationActor {
            id,
            ctx,
            mailbox: rx,
            write_latched,
            read_latched,
            blocks_ready: blocks_ready_rx,
            recheck: write_recheck,
            provider_tx: None,
            streaming: false,
            owed_turn_deferred: false,
            stream_generation: 0,
            turn_anchor: TurnAnchor::new(),
            drain_deadline: crate::ingestion::MESSAGE_END_DRAIN_DEADLINE,
        };
        tokio::spawn(async move {
            actor.run().await;
            scheduler.abort();
            state_broadcaster.abort();
            for h in metadata_handles.iter().chain(&tool_handles) {
                h.abort();
            }
        });

        tx
    }

    handles![
        DraftPromoted,
        BlocksAppended,
        ToolCallReceived,
        ApprovalSubmitted,
        InterruptRequested,
        UnlatchRequested,
        UnlatchAll,
        StreamDone,
        StreamError,
        StreamClosed,
    ];
}

// ─── Actor registration + Reactor ────────────────────────────────────────────

register_actors![ConversationActor];

/// The central reactive dispatcher — the runtime's top-level entry point.
///
/// Subscribes to the bus and routes events to per-conversation actor sets.
/// Each conversation lazily spawns one instance of every registered actor
/// type; events are filtered through each actor's `accepts` gate, so actors
/// only receive events they declared interest in. Also starts the global
/// watchers that translate row changes into [`CoreEvent::BlocksChanged`] and
/// [`CoreEvent::ConversationsChanged`] for whoever is listening.
///
/// A consumer calls this once, after building its [`RuntimeContext`], and
/// then talks to the runtime exclusively through the bus: an append is a
/// [`CoreEvent::BlocksAppended`], an interrupt is a
/// [`CoreEvent::InterruptRequested`], and so on. There is no second control
/// surface.
pub fn spawn_reactor<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent>(ctx: RuntimeContext<K, E>) {
    let mut rx = ctx.bus.subscribe();
    let watcher_ctx = ctx.clone();

    tokio::spawn(async move {
        let mut routes: HashMap<i64, Vec<ActorEntry>> = HashMap::new();

        loop {
            match rx.recv().await {
                Ok(event) => {
                    let Some(core) = event.as_core() else {
                        continue;
                    };
                    let label: &'static str = core.into();
                    tracing::debug!(event = label, conv = ?core.conversation_id(), "reactor recv");
                    route_event(&ctx, &mut routes, core);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "reactor lagged — events dropped");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::info!("reactor shutting down — bus closed");
                    break;
                }
            }
        }
    });

    spawn_block_watcher(&watcher_ctx);
    spawn_conversations_watcher(&watcher_ctx);
}

/// Conversations watcher — emits [`CoreEvent::ConversationsChanged`] when the
/// `conversations` table mutates. A consumer uses this as a single trigger to
/// refetch its conversation list; no per-row payload needed.
fn spawn_conversations_watcher<K: RuntimeKind, E: RuntimeEvent>(ctx: &RuntimeContext<K, E>) {
    let db_changes = ctx.store.changes.consumer();
    let bus = Arc::clone(&ctx.bus);

    tokio::spawn(async move {
        reactive!(db_changes, change, {
            if change.table == "conversations" {
                bus.emit(CoreEvent::ConversationsChanged {});
            }
        });
    });
}

/// Block watcher — emits [`CoreEvent::BlocksChanged`] when block-related
/// tables change. Runs globally (not per-conversation). The update hook
/// provides the rowid, and the conversation id is resolved from the store.
///
/// The tables it watches are the header, the junction, and the store's own
/// effective content-table list — the one list the change-hook allowlist is
/// also built from, core tables and descriptor tables alike. The watcher keeps
/// no copy of its own, so a consumer kind's table wakes it the moment the
/// store is opened with the descriptor. Every content table is keyed
/// `block_id INTEGER PRIMARY KEY`, which is what lets the change's rowid be
/// read as the block id below.
fn spawn_block_watcher<K: RuntimeKind, E: RuntimeEvent>(ctx: &RuntimeContext<K, E>) {
    let db_changes = ctx.store.changes.consumer();
    let store = ctx.store.clone();
    let bus = Arc::clone(&ctx.bus);

    tokio::spawn(async move {
        reactive!(db_changes, change, {
            if change.table == "blocks"
                || change.table == "conversation_blocks"
                || store.content_tables().iter().any(|t| *t == change.table)
            {
                let block_id = if change.table == "conversation_blocks" {
                    // For junction table changes, the rowid IS the junction
                    // row, but we need the block_id. Query it.
                    let rowid = change.rowid;
                    match store
                        .run(move |conn| {
                            use rusqlite::OptionalExtension;
                            conn.query_row(
                                "SELECT block_id FROM conversation_blocks WHERE id = ?1",
                                [rowid],
                                |row| row.get::<_, i64>(0),
                            )
                            .optional()
                            .map_err(Into::into)
                        })
                        .await
                    {
                        Ok(Some(id)) => id,
                        _ => continue,
                    }
                } else {
                    // The header's rowid is the block id, and a content
                    // table's block_id is its primary key (= rowid).
                    change.rowid
                };

                match store.conversation_for_block(block_id).await {
                    Ok(Some(conversation_id)) => {
                        tracing::debug!(
                            conversation_id,
                            block_id,
                            change_table = %change.table,
                            change_rowid = change.rowid,
                            "block watcher: BlocksChanged"
                        );
                        bus.emit(CoreEvent::BlocksChanged {
                            conversation_id,
                            block_id,
                        });
                    }
                    Ok(None) => {
                        tracing::warn!(block_id, change_table = %change.table, change_rowid = change.rowid, "block watcher: no conversation found for block");
                    }
                    Err(e) => {
                        tracing::warn!(block_id, change_table = %change.table, change_rowid = change.rowid, error = %e, "block watcher: conversation_for_block failed");
                    }
                }
            }
        });
    });
}

/// Route a single event to the appropriate actor(s).
///
/// Conversation-scoped events are dispatched by conversation id. Global
/// events (`conversation_id() == None`) are broadcast to all actor sets.
/// Within each set, events are filtered through each actor's `accepts` gate.
fn route_event<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent>(
    ctx: &RuntimeContext<K, E>,
    routes: &mut HashMap<i64, Vec<ActorEntry>>,
    event: &CoreEvent,
) {
    match event.conversation_id() {
        Some(conv_id) => {
            let actors = routes
                .entry(conv_id)
                .or_insert_with(|| spawn_actor_set(conv_id, ctx));
            for actor in &*actors {
                if (actor.accepts)(event) {
                    let _ = actor.tx.send(event.clone());
                }
            }
        }
        None => {
            for actors in routes.values() {
                for actor in actors {
                    if (actor.accepts)(event) {
                        let _ = actor.tx.send(event.clone());
                    }
                }
            }
        }
    }
}

/// Latch orthogonality — a latched conversation is never driven, and a normal
/// stream end never re-latches — plus the two integration proofs this slice
/// owes: the whole runtime composing over a scripted provider, and the
/// double-turn regression under real interleaving.
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use serde_json::{Value, json};

    use crate::agency::BlockKind;
    use crate::agency::ratchet::oracle::Oracle;
    use crate::block::Block;
    use crate::providers::types::ToolDefinition;
    use crate::providers::{
        BoxFuture, ContentPart, LlmError, Message, MessageContent, ModelInfo, ProviderModule,
        ProviderResponse, ProviderRx, StreamEvent,
    };
    use crate::store::{ProviderInstance, StoreError};
    use crate::tools::{ToolContext, ToolHandler, ToolOutcome};
    use crate::types::StopReason;

    use super::*;

    #[tokio::test]
    async fn latched_conversation_is_never_driven() {
        let mut o = Oracle::new().await;
        o.user_text("hi").await;
        o.call("c1").await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        assert_eq!(
            scheduler_tick::<BlockKind, _>(&o.ctx, true, &tx).await,
            None
        );

        assert!(rx.try_recv().is_err(), "no blocks_ready while latched");
        o.expect_silence(); // ratchet not invoked — no wakeup emitted
        assert_eq!(o.cursor().await, 0, "nothing persisted while latched");
    }

    #[tokio::test]
    async fn unlatched_tick_drives_and_signals_the_owed_turn() {
        let mut o = Oracle::new().await;
        let user = o.user_text("hi").await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outcome = scheduler_tick::<BlockKind, _>(&o.ctx, false, &tx)
            .await
            .unwrap();
        assert!(outcome.owes_turn);
        assert_eq!(outcome.cursor, user);
        assert!(rx.try_recv().is_ok(), "the gate firing signals the actor");
        o.expect_silence();
    }

    #[tokio::test]
    async fn unlatched_tick_parks_without_signalling() {
        let mut o = Oracle::new().await;
        o.user_text("hi").await;
        let call = o.call("c1").await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outcome = scheduler_tick::<BlockKind, _>(&o.ctx, false, &tx)
            .await
            .unwrap();
        assert!(outcome.parked);
        assert!(!outcome.owes_turn);
        assert!(
            rx.try_recv().is_err(),
            "a parked drive never signals a turn"
        );
        o.expect_wakeup(call);
    }

    /// AC6-8's resume half, end to end at the tick seam: a latched tick rests
    /// entirely, and unlatching plus ONE tick is all it takes to drive and
    /// signal the owed turn — no unlatch-dance, no second nudge.
    #[tokio::test]
    async fn unlatch_plus_one_tick_resumes() {
        let mut o = Oracle::new().await;
        let user = o.user_text("hi").await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        assert_eq!(
            scheduler_tick::<BlockKind, _>(&o.ctx, true, &tx).await,
            None
        );
        assert!(rx.try_recv().is_err());
        assert_eq!(o.cursor().await, 0, "the latched tick appended nothing");

        let outcome = scheduler_tick::<BlockKind, _>(&o.ctx, false, &tx)
            .await
            .unwrap();
        assert!(outcome.owes_turn, "one unlatched tick resumes");
        assert_eq!(outcome.cursor, user);
        assert!(rx.try_recv().is_ok());
        o.expect_silence();
    }

    /// The live-tail drive honors the latch — latched, the streamed call
    /// block is RECORDED (data, not orchestration) but nothing acts: no
    /// wakeup, no cursor movement. The dangle heals on the next unlatch.
    #[tokio::test]
    async fn latched_live_tail_records_the_block_and_drives_nothing() {
        let mut o = Oracle::new().await;
        o.user_text("go").await;

        let runner: ToolRunner<BlockKind, CoreEvent> =
            ToolRunner::new(Arc::new(ToolRegistry::new()));
        let block_id = runner
            .insert_call(
                &o.ctx,
                true,
                "raced".into(),
                "read_file".into(),
                "{}".into(),
                CallOrigin::default(),
            )
            .await
            .unwrap();

        assert!(
            o.ledger_ids().await.contains(&block_id),
            "the block IS recorded"
        );
        o.expect_silence();
        assert_eq!(o.cursor().await, 0, "the cursor never moves while latched");
    }

    /// A reader's abandoned mark retires the binding UNCONDITIONALLY
    /// (revised 2026-08-22, after verification proved the epoch guard
    /// wrong): the mark records that the reader exited — a binding-liveness
    /// fact — so even a mark scoped to an earlier epoch, left by a drain
    /// whose turn was ended out of band while a successor had already
    /// bumped the seam, must retire the binding at the next close. The
    /// guarded form discarded exactly that mark and the conversation wedged
    /// on a dead channel.
    #[tokio::test]
    async fn any_abandoned_mark_retires_the_binding_at_the_close() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx, false);
        let (provider_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        actor.provider_tx = Some(provider_tx);

        // The reader marked epoch 1 abandoned; the seam has since moved on.
        actor.turn_anchor.set(1);
        actor.turn_anchor.mark_abandoned(actor.turn_anchor.epoch());
        actor.turn_anchor.clear();
        actor.turn_anchor.set(2);
        assert!(
            actor.turn_anchor.is_abandoned(),
            "the mark is a liveness fact and survives the epoch moving on"
        );

        actor.streaming = true;
        actor.handle_stream_closed();
        assert!(
            actor.provider_tx.is_none(),
            "the close retires the binding on any mark; the epoch is not \
             consulted"
        );
    }

    /// The close edges, reworked 2026-08-22. A normal close — the reader's
    /// closed signal — releases the dispatch state, never re-latches, and
    /// re-runs the owed-turn check for a signal the open turn swallowed, by
    /// nudging the scheduler to re-drive; with nothing swallowed the close
    /// rests, because a blind re-check redispatches a turn that wrote
    /// nothing forever. Message-end settles NOTHING (that settling was the
    /// proven duplicate-turn window); only the error edge latches, because a
    /// request we built wrong fails identically on every retry.
    #[tokio::test]
    async fn the_dispatch_state_closes_on_the_named_edges_only() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let (mut actor, recheck) = bare_actor(conv, ctx, false);

        // Message-end: the dispatch state stays open — the tool round is
        // still being recorded behind it.
        actor.streaming = true;
        actor.handle_stream_done(Some(StopReason::EndTurn));
        assert!(
            actor.streaming,
            "message-end no longer settles the dispatch state"
        );

        // A signal delivered mid-turn is swallowed and deferred to the close.
        actor.handle_blocks_ready().await;
        assert!(
            actor.owed_turn_deferred,
            "a mid-turn delivery defers instead of dropping"
        );

        // The closed edge: released, unlatched, and the swallowed signal
        // becomes exactly one scheduler re-check nudge.
        actor.handle_stream_closed();
        assert!(!actor.streaming, "the closed signal is a close edge");
        assert!(
            !actor.read_latched.get(),
            "a normal close never re-latches — no unlatch-dance needed"
        );
        assert_eq!(
            recheck.get(),
            1,
            "the close re-runs the owed-turn check through the scheduler"
        );
        assert!(!actor.owed_turn_deferred, "the close consumes the deferral");
        assert!(
            actor.blocks_ready.try_recv().is_err(),
            "the actor never produces an owed-turn signal of its own"
        );

        // A close with nothing swallowed rests: no nudge, no signal — the
        // empty turn's unchanged frontier is not re-asked forever.
        actor.streaming = true;
        actor.handle_stream_closed();
        assert_eq!(
            recheck.get(),
            1,
            "an undisturbed close nudges nothing — the empty turn rests"
        );

        // The error edge: released AND latched.
        actor.streaming = true;
        actor.handle_stream_error();
        assert!(!actor.streaming);
        assert!(actor.read_latched.get(), "only the error edge latches");
    }

    /// The cursor-confirm feedback edge, pinned as ruled at [`run_scheduler`]:
    /// a confirm announces a change on a table the scheduler wakes on, so it
    /// buys exactly one extra tick — and that tick is a no-op that announces
    /// nothing further, so the edge converges to rest instead of ringing.
    #[tokio::test]
    async fn cursor_confirm_wakes_once_and_converges_to_rest() {
        let o = Oracle::new().await;
        let user = o.user_text("hi").await;

        let changes = o.ctx.store.changes.consumer();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        // The confirming tick: the drive advances the cursor onto the user
        // block, and persisting it announces a `conversations` change — the
        // wake that costs the extra tick.
        let confirmed = scheduler_tick::<BlockKind, _>(&o.ctx, false, &tx)
            .await
            .unwrap();
        assert_eq!(confirmed.cursor, user);
        assert!(
            changes
                .drain()
                .iter()
                .any(|change| change.table == "conversations"),
            "the confirm itself is the wake"
        );

        // The tick that wake buys: a no-op. Same outcome, no new cursor
        // write, and — the convergence claim — no further `conversations`
        // change announced, so nothing wakes the loop again and it rests.
        // (It re-signals the still-owed turn; the actor's streaming flag is
        // what dedupes that, which the double-turn test below proves.)
        let echo = scheduler_tick::<BlockKind, _>(&o.ctx, false, &tx)
            .await
            .unwrap();
        assert_eq!(echo, confirmed, "the extra tick changes nothing");
        assert!(
            changes
                .drain()
                .iter()
                .all(|change| change.table != "conversations"),
            "one wake per confirm: the no-op tick announces nothing"
        );
    }

    // ─── The scripted provider and the composed runtime ─────────────────

    /// What the scripted provider streams when a turn request arrives.
    #[derive(Clone, Copy)]
    enum Script {
        /// First request answers with one tool call, second with prose. The
        /// composed-runtime shape.
        ToolCallThenText,
        /// Every request answers with one prose turn. The plain anchor shape.
        Prose,
        /// One tool round in the real wire order — optional narration, the
        /// message end with the tool-use stop, THEN the drained tool
        /// lifecycle and the trailing done — and closing prose once a request
        /// carries the answered call. The anchor-shape scripts ride this:
        /// narrated, multi-round, and (with a failing tool) the failed round.
        ToolRound {
            tool: &'static str,
            narration: Option<&'static str>,
        },
        /// [`Script::ToolRound`] with the post-message-end window HELD open:
        /// the message end goes out, then the tool lifecycle waits for the
        /// probe's release — which is what lets a test append a message
        /// inside the window, provably after the message end and before the
        /// tool round is recorded. The duplicate-turn reproduction.
        HoldToolRound { tool: &'static str },
        /// Two sequential tool rounds before the closing prose — the
        /// continuation-round anchor shape: the second round's dispatching
        /// frontier is the first round's RESULT, a turn product, so its call
        /// must inherit the original summoning message's identity.
        TwoToolRounds,
        /// Every request answers with an empty turn — a message end and the
        /// trailing done, no content. The close-rest probe: the frontier is
        /// unchanged at the close, so the close must not keep redispatching
        /// it.
        EmptyTurn,
        /// One turn cut off by the token cap, in the real wire order: partial
        /// prose, the message end with the max-tokens stop, THEN the tool
        /// lifecycle the shared translator still releases, and the trailing
        /// done. Any later request is counted and left open — the abnormal
        /// stop latches, so a second dispatch is the defect under probe.
        MaxTokensToolRound,
        /// One turn emits TWO tool calls before the closing prose — the
        /// multi-call round: while one sibling's result is recorded and the
        /// other still runs, the tail awaits the model but a call still
        /// dangles, and only the drive's parked gate knows. A request built
        /// in that window would carry the dangling call.
        TwoCallsInOneTurn,
        /// Every request is counted and left open — the stream never speaks.
        /// The double-turn probe: any second request is a defect, not a retry.
        CountOnly,
        /// The first turn streams a partial answer and then the provider task
        /// dies, dropping the response channel with no `Done`. Every later
        /// turn (on the rebound channel) answers with prose. The
        /// dead-channel-recovery probe.
        DieMidStreamOnce,
        /// The first turn streams a partial answer and then STALLS — the
        /// channel stays open and no `Done` ever arrives, until the binding
        /// is torn down. Every later turn (on a fresh binding) answers with
        /// prose. The interrupt-teardown probe.
        StallFirstTurn,
        /// The first turn ends its message with prose committed, then stalls
        /// past the reader's drain deadline; the turn's held tail — the tool
        /// lifecycles and the trailing done — waits for the probe's `release`
        /// and goes out only when the test says so, long after the deadline
        /// closed the turn. The second turn streams its prose at once and
        /// holds its own end until the probe's `finish`. The abandoned-turn
        /// reproduction: the late tail is delivered while the successor turn
        /// is provably mid-flight.
        StallPastDeadlineThenLateTail,
        /// VERIFIER VARIANT: like the above, but the stalled provider wakes
        /// with a FULL second round — prose, its own `MessageEnd`, a tool
        /// lifecycle and a trailing `Done` — delivered long after the drain
        /// deadline abandoned turn one, while the successor is mid-flight.
        StallPastDeadlineThenLateFullRound,
        /// Script: turn one ends its message and stalls forever;
        /// turn two is dispatched but the provider says nothing for a long
        /// while; turn three answers normally.
        /// Like `StallFirstTurn`, but with realistic wind-down timing: the
        /// provider task takes [`SLOW_WIND_DOWN`] to exit after the
        /// Interrupt, and the second turn's prose streams at once while its
        /// `MessageEnd` lags by [`SLOW_TURN_END`] — so the successor turn is
        /// still live when the torn-down reader announces its close. Turns
        /// after the second answer immediately. The stale-signal probe.
        SlowTeardown,
    }

    /// How long the `SlowTeardown` provider task keeps winding down after the
    /// Interrupt before it exits and its reader sees the channel close.
    const SLOW_WIND_DOWN: Duration = Duration::from_millis(400);
    /// How long the `SlowTeardown` second turn stays live after its prose:
    /// the gap between its first delta and its `MessageEnd`, long enough to
    /// contain the torn-down reader's late close and the assertions on it.
    const SLOW_TURN_END: Duration = Duration::from_millis(1500);

    /// One `SlowTeardown` turn's answer. Bare deltas, no `TextBlockStart` —
    /// the chat stream decoder's real shape, which is how a reader kept
    /// across an interrupt appends the NEXT turn's deltas to a swept tail.
    fn slow_teardown_turn(
        turn: usize,
        resp_tx: &tokio::sync::mpsc::UnboundedSender<ProviderResponse>,
    ) -> Scripted {
        let message_end = StreamEvent::MessageEnd {
            usage: crate::providers::Usage::default(),
            stop_reason: StopReason::EndTurn,
        };
        match turn {
            // The stall: no `Done` ever comes — torn down by the interrupt.
            1 => Scripted::Events(vec![StreamEvent::TextDelta {
                text: "half-".into(),
            }]),
            // The successor turn stays LIVE past the torn-down reader's late
            // close: its `MessageEnd` arrives from a side task, so the
            // provider task keeps serving — a spurious concurrent request
            // must be counted, not queued behind a sleep. The delayed send is
            // the one answer `Scripted`'s ordered event list cannot express —
            // this arm pins timing, not order — so the end alone rides a task
            // of its own while the delta goes through the harness. The
            // trailing done real wires send rides the same task, after the
            // end.
            2 => {
                let resp_tx = resp_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(SLOW_TURN_END).await;
                    let _ = resp_tx.send(ProviderResponse::Event(message_end));
                    let _ = resp_tx.send(ProviderResponse::Done);
                });
                Scripted::Events(vec![StreamEvent::TextDelta {
                    text: "recovered".into(),
                }])
            }
            _ => Scripted::Turn(vec![
                StreamEvent::TextDelta {
                    text: "clean".into(),
                },
                message_end,
            ]),
        }
    }

    /// One `StallPastDeadlineThenLateTail` turn's answer. The first turn ends
    /// its message with prose committed and holds its tail — the tool
    /// lifecycles and the trailing done — for the `release` latch, delivered
    /// long after the drain deadline abandoned the turn, into a channel the
    /// retired binding no longer reads. The successor turn streams its prose
    /// at once and holds its own end for the `finish` latch, so the test can
    /// deliver the abandoned tail while the successor is provably mid-flight.
    fn stall_then_late_tail_turn(
        turn: usize,
        resp_tx: &tokio::sync::mpsc::UnboundedSender<ProviderResponse>,
        release: &Arc<tokio::sync::Notify>,
        finish: &Arc<tokio::sync::Notify>,
    ) -> Scripted {
        if turn == 1 {
            let resp_tx = resp_tx.clone();
            let release = Arc::clone(release);
            tokio::spawn(async move {
                release.notified().await;
                for event in [
                    StreamEvent::ToolUseStart {
                        id: "call-late".into(),
                        name: "echo".into(),
                    },
                    StreamEvent::ToolUseInputDelta { json: "{}".into() },
                    StreamEvent::ToolUseEnd,
                ] {
                    let _ = resp_tx.send(ProviderResponse::Event(event));
                }
                let _ = resp_tx.send(ProviderResponse::Done);
            });
            return Scripted::Events(vec![
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta {
                    text: "half-".into(),
                },
                StreamEvent::MessageEnd {
                    usage: crate::providers::Usage::default(),
                    stop_reason: StopReason::ToolUse,
                },
            ]);
        }
        let resp_tx = resp_tx.clone();
        let finish = Arc::clone(finish);
        tokio::spawn(async move {
            finish.notified().await;
            let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::MessageEnd {
                usage: crate::providers::Usage::default(),
                stop_reason: StopReason::EndTurn,
            }));
            let _ = resp_tx.send(ProviderResponse::Done);
        });
        Scripted::Events(vec![
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta {
                text: "recovered".into(),
            },
        ])
    }

    /// VERIFIER VARIANT of [`stall_then_late_tail_turn`]: the stalled turn's
    /// late delivery is a WHOLE second lifecycle round — prose, a second
    /// `MessageEnd`, a tool lifecycle and the trailing `Done`.
    fn stall_then_late_full_round_turn(
        turn: usize,
        resp_tx: &tokio::sync::mpsc::UnboundedSender<ProviderResponse>,
        release: &Arc<tokio::sync::Notify>,
        finish: &Arc<tokio::sync::Notify>,
    ) -> Scripted {
        if turn == 1 {
            let resp_tx = resp_tx.clone();
            let release = Arc::clone(release);
            tokio::spawn(async move {
                release.notified().await;
                for event in [
                    StreamEvent::TextBlockStart,
                    StreamEvent::TextDelta {
                        text: "ghost".into(),
                    },
                    StreamEvent::MessageEnd {
                        usage: crate::providers::Usage::default(),
                        stop_reason: StopReason::ToolUse,
                    },
                    StreamEvent::ToolUseStart {
                        id: "call-ghost".into(),
                        name: "echo".into(),
                    },
                    StreamEvent::ToolUseInputDelta { json: "{}".into() },
                    StreamEvent::ToolUseEnd,
                ] {
                    let _ = resp_tx.send(ProviderResponse::Event(event));
                }
                let _ = resp_tx.send(ProviderResponse::Done);
                // And a second full lifecycle behind it, for good measure.
                for event in [
                    StreamEvent::TextBlockStart,
                    StreamEvent::TextDelta {
                        text: "ghost2".into(),
                    },
                    StreamEvent::MessageEnd {
                        usage: crate::providers::Usage::default(),
                        stop_reason: StopReason::EndTurn,
                    },
                ] {
                    let _ = resp_tx.send(ProviderResponse::Event(event));
                }
                let _ = resp_tx.send(ProviderResponse::Done);
            });
            return Scripted::Events(vec![
                StreamEvent::TextBlockStart,
                StreamEvent::TextDelta {
                    text: "half-".into(),
                },
                StreamEvent::MessageEnd {
                    usage: crate::providers::Usage::default(),
                    stop_reason: StopReason::ToolUse,
                },
            ]);
        }
        let resp_tx = resp_tx.clone();
        let finish = Arc::clone(finish);
        tokio::spawn(async move {
            finish.notified().await;
            let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::MessageEnd {
                usage: crate::providers::Usage::default(),
                stop_reason: StopReason::EndTurn,
            }));
            let _ = resp_tx.send(ProviderResponse::Done);
        });
        Scripted::Events(vec![
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta {
                text: "recovered".into(),
            },
        ])
    }

    /// A provider module that answers from a script instead of a wire. It
    /// stands exactly where the runtime's own infrastructure ends: requests
    /// arrive through the real bind seam and responses travel the real
    /// ingestion path.
    struct ScriptedProvider {
        script: Script,
        requests: Arc<AtomicUsize>,
        /// The message shape of every request received, for diagnostics:
        /// a spurious request is only debuggable by what it carried.
        shapes: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
        /// Every turn request's full neutral messages, in arrival order — the
        /// window test asserts WHAT a request carried, not just that one
        /// fired.
        seen: Arc<std::sync::Mutex<Vec<Vec<Message>>>>,
        /// The held-window scripts' latch, shared with the probe: notifying
        /// it lets a held tool round proceed.
        release: Arc<tokio::sync::Notify>,
        /// The second latch, for the scripts that stage TWO holds: notifying
        /// it lets a held turn's end go out. Inert everywhere else.
        finish: Arc<tokio::sync::Notify>,
    }

    /// How many answered calls the request's messages already carry — the
    /// scripted ledger-content cue: zero opens the call round, each recorded
    /// outcome advances the script, and the last round serves the closing
    /// prose.
    fn tool_result_count(messages: &[Message]) -> usize {
        messages
            .iter()
            .map(|m| match &m.content {
                MessageContent::Parts(parts) => parts
                    .iter()
                    .filter(|p| matches!(p, ContentPart::ToolResult { .. }))
                    .count(),
                MessageContent::Text(_) => 0,
            })
            .sum()
    }

    /// One message's compact descriptor for those diagnostics: its role, and
    /// either the text mode or the part tags in order.
    fn message_shape(message: &Message) -> String {
        match &message.content {
            MessageContent::Text(_) => format!("{:?}:text", message.role),
            MessageContent::Parts(parts) => {
                let tags: Vec<&str> = parts
                    .iter()
                    .map(|part| match part {
                        ContentPart::Text { .. } => "text",
                        ContentPart::Reasoning { .. } => "reasoning",
                        ContentPart::ToolUse { .. } => "tool_use",
                        ContentPart::ToolResult { .. } => "tool_result",
                    })
                    .collect();
                format!("{:?}:[{}]", message.role, tags.join(","))
            }
        }
    }

    impl ProviderModule for ScriptedProvider {
        fn type_id(&self) -> &'static str {
            "scripted"
        }
        fn display_name(&self) -> &'static str {
            "Scripted"
        }
        fn description(&self) -> &'static str {
            "answers from a script"
        }
        fn get_config(
            &self,
            _provider_id: String,
        ) -> BoxFuture<'_, Result<Option<Value>, StoreError>> {
            Box::pin(async { Ok(Some(json!({}))) })
        }
        fn save_config(
            &self,
            _provider_id: String,
            _config: Value,
        ) -> BoxFuture<'_, Result<(), StoreError>> {
            Box::pin(async { Ok(()) })
        }
        fn delete_config(&self, _provider_id: String) -> BoxFuture<'_, Result<(), StoreError>> {
            Box::pin(async { Ok(()) })
        }
        fn summary(
            &self,
            _provider_id: String,
        ) -> BoxFuture<'_, Result<Option<String>, StoreError>> {
            Box::pin(async { Ok(None) })
        }
        fn list_models(&self, _config: Value) -> BoxFuture<'_, Result<Vec<ModelInfo>, LlmError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn bind(
            &self,
            _conversation_id: i64,
            _provider_id: String,
            _config: Value,
        ) -> (ProviderTx, ProviderRx) {
            let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel();
            let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
            let script = self.script;
            let requests = Arc::clone(&self.requests);
            let shapes = Arc::clone(&self.shapes);
            let seen = Arc::clone(&self.seen);
            let release = Arc::clone(&self.release);
            let finish = Arc::clone(&self.finish);
            tokio::spawn(async move {
                while let Some(request) = req_rx.recv().await {
                    let ProviderRequest::Stream {
                        messages, tools, ..
                    } = request
                    else {
                        // The Interrupt lands here. A real provider does not
                        // vanish on it instantly — `SlowTeardown` winds down
                        // for a beat first, so the reader's channel-close exit
                        // runs AFTER the successor turn is already streaming.
                        if matches!(script, Script::SlowTeardown) {
                            tokio::time::sleep(SLOW_WIND_DOWN).await;
                        }
                        continue;
                    };
                    // The metadata worker shares this provider and its
                    // derivation request is the one carrying no tool
                    // definitions — answer it with a title and keep it out of
                    // the TURN count, which is what the tests assert on.
                    if tools.is_empty() {
                        let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::TextDelta {
                            text: "A derived title".into(),
                        }));
                        let _ = resp_tx.send(ProviderResponse::Done);
                        continue;
                    }
                    shapes
                        .lock()
                        .unwrap()
                        .push(messages.iter().map(message_shape).collect());
                    seen.lock().unwrap().push(messages.clone());
                    let turn = requests.fetch_add(1, Ordering::SeqCst) + 1;
                    // Scripted by ledger content, not arrival order: a turn
                    // whose messages already carry the answered call gets the
                    // closing prose, the opening turn gets the call.
                    let answered = tool_result_count(&messages);
                    match scripted_turn(script, turn, answered, &resp_tx, &release, &finish) {
                        Scripted::Events(events) => {
                            for event in events {
                                let _ = resp_tx.send(ProviderResponse::Event(event));
                            }
                        }
                        // A completed turn ends with the trailing done real
                        // wires send.
                        Scripted::Turn(events) => {
                            for event in events {
                                let _ = resp_tx.send(ProviderResponse::Event(event));
                            }
                            let _ = resp_tx.send(ProviderResponse::Done);
                        }
                        // The provider task dies mid-turn: the response
                        // channel drops with no `Done`.
                        Scripted::Die(events) => {
                            for event in events {
                                let _ = resp_tx.send(ProviderResponse::Event(event));
                            }
                            return;
                        }
                    }
                }
            });
            (req_tx, resp_rx)
        }
    }

    /// What one scripted turn does after the counters are stamped.
    enum Scripted {
        /// Stream these events, in order, then keep serving with the turn
        /// left OPEN — no trailing done. An empty list is a turn deliberately
        /// left open; the stall scripts and the held window use this arm.
        Events(Vec<StreamEvent>),
        /// Stream these events, then the trailing `Done` real wires send
        /// after every completed turn (2026-08-22): the reworked close edge
        /// settles the dispatch state on the closed signal, so a scripted
        /// turn that ends without its done would leave the state open — a
        /// stall the wire never produces.
        Turn(Vec<StreamEvent>),
        /// Stream these events, in order, then the provider task dies
        /// outright, dropping its channel.
        Die(Vec<StreamEvent>),
    }

    /// The real wire order for one scripted tool round: narration prose
    /// first, the message end with the tool-use stop, THEN the drained tool
    /// lifecycle.
    fn tool_round_events(
        turn: usize,
        tool: &'static str,
        narration: Option<&'static str>,
    ) -> Vec<StreamEvent> {
        let mut events = Vec::new();
        if let Some(narration) = narration {
            events.push(StreamEvent::TextBlockStart);
            events.push(StreamEvent::TextDelta {
                text: narration.into(),
            });
        }
        events.extend([
            StreamEvent::MessageEnd {
                usage: crate::providers::Usage::default(),
                stop_reason: StopReason::ToolUse,
            },
            StreamEvent::ToolUseStart {
                id: format!("call-{turn}"),
                name: tool.into(),
            },
            StreamEvent::ToolUseInputDelta { json: "{}".into() },
            StreamEvent::ToolUseEnd,
        ]);
        events
    }

    /// One completed prose turn's events: block start, one delta, the
    /// message end with the end-turn stop.
    fn prose_turn_events(text: &'static str) -> Vec<StreamEvent> {
        vec![
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta { text: text.into() },
            StreamEvent::MessageEnd {
                usage: crate::providers::Usage::default(),
                stop_reason: StopReason::EndTurn,
            },
        ]
    }

    /// One turn emitting TWO tool calls, in the real wire order: the message
    /// end with the tool-use stop, then BOTH drained tool lifecycles.
    fn two_call_round_events() -> Vec<StreamEvent> {
        let mut events = vec![StreamEvent::MessageEnd {
            usage: crate::providers::Usage::default(),
            stop_reason: StopReason::ToolUse,
        }];
        for id in ["call-a", "call-b"] {
            events.extend([
                StreamEvent::ToolUseStart {
                    id: id.into(),
                    name: "echo".into(),
                },
                StreamEvent::ToolUseInputDelta { json: "{}".into() },
                StreamEvent::ToolUseEnd,
            ]);
        }
        events
    }

    /// One turn cut off by the token cap, in the real wire order: partial
    /// prose, the message end with the max-tokens stop, then the tool
    /// lifecycle the shared translator still releases.
    fn max_tokens_tool_round_events(turn: usize) -> Vec<StreamEvent> {
        vec![
            StreamEvent::TextDelta {
                text: "cut-".into(),
            },
            StreamEvent::MessageEnd {
                usage: crate::providers::Usage::default(),
                stop_reason: StopReason::MaxTokens,
            },
            StreamEvent::ToolUseStart {
                id: format!("call-{turn}"),
                name: "echo".into(),
            },
            StreamEvent::ToolUseInputDelta { json: "{}".into() },
            StreamEvent::ToolUseEnd,
        ]
    }

    /// One scripted turn's answer, decided from the script and what the
    /// request carried — `answered` is how many resolved calls the request's
    /// messages already show. `release` is the held-window scripts' latch and
    /// `finish` the two-hold scripts' second one — unused by every other arm.
    fn scripted_turn(
        script: Script,
        turn: usize,
        answered: usize,
        resp_tx: &tokio::sync::mpsc::UnboundedSender<ProviderResponse>,
        release: &Arc<tokio::sync::Notify>,
        finish: &Arc<tokio::sync::Notify>,
    ) -> Scripted {
        let message_end = StreamEvent::MessageEnd {
            usage: crate::providers::Usage::default(),
            stop_reason: StopReason::EndTurn,
        };
        match (script, answered) {
            // The stream never speaks: `CountOnly` by design on every
            // request — and a two-call round's request showing ONE answered
            // call of the two IS the dangling dispatch under probe: count it
            // and leave the turn open, so the suite's deadline names the
            // stall instead of the script papering over it with an answer.
            (Script::CountOnly, _)
            | (Script::TwoCallsInOneTurn, 1)
            | (Script::MaxTokensToolRound, 1..) => Scripted::Events(Vec::new()),
            (Script::MaxTokensToolRound, 0) => Scripted::Turn(max_tokens_tool_round_events(turn)),
            (Script::SlowTeardown, _) => slow_teardown_turn(turn, resp_tx),
            (Script::StallPastDeadlineThenLateTail, _) => {
                stall_then_late_tail_turn(turn, resp_tx, release, finish)
            }
            (Script::StallPastDeadlineThenLateFullRound, _) => {
                stall_then_late_full_round_turn(turn, resp_tx, release, finish)
            }
            (Script::StallFirstTurn, _) => {
                // Bare deltas, no `TextBlockStart` — the chat stream decoder's
                // real shape: the reader lazy-creates the tail on the first
                // delta, which is exactly how a reader kept across an
                // interrupt appends the NEXT turn's deltas to the swept
                // tail's deleted id.
                if turn == 1 {
                    // The stall: the delta goes out and no `Done` ever comes —
                    // the task keeps listening until its channel drops.
                    return Scripted::Events(vec![StreamEvent::TextDelta {
                        text: "half-".into(),
                    }]);
                }
                Scripted::Turn(vec![
                    StreamEvent::TextDelta {
                        text: "recovered".into(),
                    },
                    message_end,
                ])
            }
            (Script::DieMidStreamOnce, _) => {
                if turn == 1 {
                    return Scripted::Die(vec![
                        StreamEvent::TextBlockStart,
                        StreamEvent::TextDelta {
                            text: "half-".into(),
                        },
                    ]);
                }
                Scripted::Turn(prose_turn_events("recovered"))
            }
            (Script::Prose, _) => Scripted::Turn(prose_turn_events("answered")),
            (Script::ToolCallThenText, 0) => Scripted::Turn(vec![
                StreamEvent::ToolUseStart {
                    id: "call-1".into(),
                    name: "echo".into(),
                },
                StreamEvent::ToolUseInputDelta { json: "{}".into() },
                StreamEvent::ToolUseEnd,
                StreamEvent::MessageEnd {
                    usage: crate::providers::Usage::default(),
                    stop_reason: StopReason::ToolUse,
                },
            ]),
            (Script::ToolRound { tool, narration }, 0) => {
                Scripted::Turn(tool_round_events(turn, tool, narration))
            }
            (Script::TwoToolRounds, 0 | 1) => Scripted::Turn(tool_round_events(turn, "echo", None)),
            (Script::EmptyTurn, _) => Scripted::Turn(vec![message_end]),
            (Script::TwoCallsInOneTurn, 0) => Scripted::Turn(two_call_round_events()),
            (
                Script::ToolCallThenText
                | Script::ToolRound { .. }
                | Script::HoldToolRound { .. }
                | Script::TwoToolRounds
                | Script::TwoCallsInOneTurn,
                2..,
            )
            | (
                Script::ToolCallThenText | Script::ToolRound { .. } | Script::HoldToolRound { .. },
                1,
            ) => Scripted::Turn(prose_turn_events("done")),
            (Script::HoldToolRound { tool }, 0) => {
                // The message end goes out now; the tool lifecycle and the
                // trailing done wait for the probe's release — the held
                // post-message-end window the duplicate-turn defect lived in.
                let resp_tx = resp_tx.clone();
                let release = Arc::clone(release);
                tokio::spawn(async move {
                    release.notified().await;
                    for event in [
                        StreamEvent::ToolUseStart {
                            id: format!("call-{turn}"),
                            name: tool.into(),
                        },
                        StreamEvent::ToolUseInputDelta { json: "{}".into() },
                        StreamEvent::ToolUseEnd,
                    ] {
                        let _ = resp_tx.send(ProviderResponse::Event(event));
                    }
                    let _ = resp_tx.send(ProviderResponse::Done);
                });
                Scripted::Events(vec![StreamEvent::MessageEnd {
                    usage: crate::providers::Usage::default(),
                    stop_reason: StopReason::ToolUse,
                }])
            }
        }
    }

    /// An ungated tool the composed runtime executes for real.
    struct EchoTool;

    impl ToolHandler<CoreEvent> for EchoTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "echo".into(),
                description: "answers with a fixed string".into(),
                parameters: json!({ "type": "object" }),
            }
        }
        fn execute<'a>(
            &'a self,
            _input: &'a str,
            _ctx: ToolContext<'a, CoreEvent>,
        ) -> BoxFuture<'a, ToolOutcome> {
            Box::pin(async { ToolOutcome::Done("echoed".into()) })
        }
    }

    /// A tool that always fails — the failed-round shape's body.
    struct FailingTool;

    impl ToolHandler<CoreEvent> for FailingTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "boom".into(),
                description: "always errors".into(),
                parameters: json!({ "type": "object" }),
            }
        }
        fn execute<'a>(
            &'a self,
            _input: &'a str,
            _ctx: ToolContext<'a, CoreEvent>,
        ) -> BoxFuture<'a, ToolOutcome> {
            Box::pin(async { ToolOutcome::Error("scripted failure".into()) })
        }
    }

    /// The whole runtime wired up over a scripted provider, plus its
    /// conversation id and the provider's request counter.
    struct ComposedProbe {
        requests: Arc<AtomicUsize>,
        shapes: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
        /// Every turn request's full neutral messages, in arrival order.
        seen: Arc<std::sync::Mutex<Vec<Vec<Message>>>>,
        /// The held-window scripts' latch: notify it to let a held tool round
        /// proceed. Inert for every other script.
        release: Arc<tokio::sync::Notify>,
        /// The two-hold scripts' second latch: notify it to let a held turn's
        /// end go out. Inert for every other script.
        finish: Arc<tokio::sync::Notify>,
    }

    /// The scripted context WITHOUT the reactor: for tests that construct or
    /// call actor internals directly rather than routing through the bus.
    async fn scripted_context(
        script: Script,
    ) -> (RuntimeContext<BlockKind, CoreEvent>, i64, ComposedProbe) {
        let store = Store::in_memory().unwrap();
        store
            .save_provider_instance(ProviderInstance {
                id: "scripted-1".into(),
                provider_type: "scripted".into(),
                name: "Scripted".into(),
            })
            .await
            .unwrap();
        let conv = store
            .create_conversation(
                "scripted-1".into(),
                "model-x".into(),
                "Model X".into(),
                "scripted".into(),
            )
            .await
            .unwrap();

        let requests = Arc::new(AtomicUsize::new(0));
        let shapes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());
        let mut providers = ProviderRegistry::new();
        providers.register(Box::new(ScriptedProvider {
            script,
            requests: Arc::clone(&requests),
            shapes: Arc::clone(&shapes),
            seen: Arc::clone(&seen),
            release: Arc::clone(&release),
            finish: Arc::clone(&finish),
        }));
        let mut tools = ToolRegistry::new();
        tools.register("echo", EchoTool);
        tools.register("boom", FailingTool);

        let ctx: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            store,
            Arc::new(EventBus::<CoreEvent>::new()),
            Arc::new(providers),
            Arc::new(tools),
        );
        (
            ctx,
            conv,
            ComposedProbe {
                requests,
                shapes,
                seen,
                release,
                finish,
            },
        )
    }

    async fn composed_runtime(
        script: Script,
    ) -> (RuntimeContext<BlockKind, CoreEvent>, i64, ComposedProbe) {
        let (ctx, conv, probe) = scripted_context(script).await;
        spawn_reactor(ctx.clone());
        (ctx, conv, probe)
    }

    /// Poll the ledger until `accept` says it is the shape awaited, with a
    /// deadline so a stall is a named failure rather than a hung suite.
    async fn await_ledger(
        ctx: &RuntimeContext<BlockKind, CoreEvent>,
        conv: i64,
        what: &str,
        accept: impl Fn(&[Block]) -> bool,
    ) -> Vec<Block> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let blocks = ctx.store.list_blocks(conv).await.unwrap();
            if accept(&blocks) {
                return blocks;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out awaiting {what}; ledger: {:?}",
                blocks
                    .iter()
                    .map(|b| b.block_type.as_str())
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// AC6-6 — the whole runtime composes: a user message appended through
    /// the store wakes the scheduler, a turn fires, the scripted stream is
    /// ingested into blocks with a tool call, the runner admits and executes,
    /// the result wakes the next tick, and a second turn fires — asserted on
    /// the resulting ledger, block by block.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_whole_runtime_composes_over_a_scripted_provider() {
        let (ctx, conv, probe) = composed_runtime(Script::ToolCallThenText).await;

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        let blocks = await_ledger(&ctx, conv, "the two-turn ledger", |blocks| {
            blocks.len() == 5 && blocks.last().is_some_and(|b| b.block_type == "text")
        })
        .await;

        // Block by block: the date marker the append stamped, the user's
        // message, the scripted call, the executed result, the closing prose.
        let shape: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            shape,
            vec!["date_marker", "text", "tool_call", "tool_result", "text"]
        );
        assert_eq!(blocks[1].role, Some(crate::block::Role::User));
        assert_eq!(blocks[1].fields["content"], json!("hi"));
        assert_eq!(blocks[2].fields["name"], json!("echo"));
        assert_eq!(blocks[2].fields["input"], json!("{}"));
        assert_eq!(blocks[3].fields["content"], json!("echoed"));
        assert_eq!(blocks[3].fields["tool_call_id"], json!("call-1"));
        assert_eq!(blocks[4].role, Some(crate::block::Role::Assistant));
        assert_eq!(blocks[4].fields["content"], json!("done"));

        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "exactly two turns fired: one into the call, one out of its result — got {:?}",
            probe.shapes.lock().unwrap()
        );
        // The streaming tails were all replaced by their finals.
        assert!(
            blocks
                .iter()
                .all(|b| !b.block_type.starts_with("streaming"))
        );
    }

    /// AC6-7 — the double-turn regression, exercised with real interleaving:
    /// one send, then a storm of wakeups from two genuinely parallel tasks
    /// while the first turn's awaits are still in flight. Each spammed
    /// `UnlatchRequested` is an event the actor really handles: handling it
    /// writes the latch signal, the write wakes the scheduler (a signal write
    /// wakes subscribers changed or not), and every wake is a full
    /// `ratchet::drive` against a tail that still awaits the model — so each
    /// one re-signals the owed turn at the actor for real. However the ticks
    /// interleave, exactly ONE stream request goes out — the scheduler is the
    /// single driver and the actor's streaming flag holds the door.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_wakeups_never_double_a_turn() {
        let (ctx, conv, probe) = composed_runtime(Script::CountOnly).await;
        let requests = Arc::clone(&probe.requests);

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        // Two parallel spammers race the scheduler's own ticks with extra
        // wakeups for the same send — the two-wakeups-for-one-send shape.
        // The conversation is already unlatched, so each unlatch changes no
        // state; it only forces another genuine scheduler tick.
        let spammers: Vec<_> = (0..2)
            .map(|_| {
                let bus = Arc::clone(&ctx.bus);
                tokio::spawn(async move {
                    for _ in 0..25 {
                        bus.emit(CoreEvent::UnlatchRequested {
                            conversation_id: conv,
                        });
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
            })
            .collect();
        for spammer in spammers {
            spammer.await.unwrap();
        }

        // Wait until the one request has fired, then hold the window open for
        // the duplicate that must never come.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while requests.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the first turn never fired"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "two interleaved wakeup storms, one send, ONE request"
        );
    }

    /// AC6-8's error edge through the composed runtime: a provider stream
    /// error latches the conversation, and a latched conversation appends
    /// nothing however many ticks fire.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stream_error_latches_the_composed_conversation() {
        let (ctx, conv, _probe) = composed_runtime(Script::CountOnly).await;

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        // Wait for the turn to fire and the cursor to confirm the append.
        await_ledger(&ctx, conv, "the appended message", |blocks| {
            blocks.iter().any(|b| b.block_type == "text")
        })
        .await;

        let mut state_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::StreamError {
            conversation_id: conv,
            error: "scripted failure".into(),
            generation: None,
        });

        // The broadcaster reports the latch flipping on.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match state_rx.try_recv() {
                Ok(CoreEvent::ConversationState { latched: true, .. }) => break,
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the error never latched the conversation"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => panic!("state subscription failed: {e}"),
            }
        }

        // Latched: further wakeups append nothing.
        let before = ctx.store.list_blocks(conv).await.unwrap().len();
        for _ in 0..5 {
            ctx.bus.emit(CoreEvent::BlocksChanged {
                conversation_id: conv,
                block_id: 1,
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after = ctx.store.list_blocks(conv).await.unwrap().len();
        assert_eq!(before, after, "a latched conversation appends nothing");
    }

    // ─── The post-review fixes, pinned ───────────────────────────────────

    /// A bare actor over the scripted context, outside the reactor, so the
    /// seams under test are called directly — plus the read half of its
    /// re-check signal, which is where a test observes the nudge count the
    /// scheduler would.
    fn bare_actor(
        conv: i64,
        ctx: RuntimeContext<BlockKind, CoreEvent>,
        latched: bool,
    ) -> (ConversationActor<BlockKind, CoreEvent>, ReadSignal<u64>) {
        let (read_latched, write_latched) = create_signal(latched);
        let (_mail_tx, mailbox) = tokio::sync::mpsc::unbounded_channel();
        let (_blocks_ready_tx, blocks_ready) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (read_recheck, write_recheck) = create_signal(0u64);
        let actor = ConversationActor {
            id: conv,
            ctx,
            mailbox,
            write_latched,
            read_latched,
            blocks_ready,
            recheck: write_recheck,
            provider_tx: None,
            streaming: false,
            owed_turn_deferred: false,
            stream_generation: 0,
            turn_anchor: TurnAnchor::new(),
            drain_deadline: crate::ingestion::MESSAGE_END_DRAIN_DEADLINE,
        };
        (actor, read_recheck)
    }

    /// An interrupt sweeps the turn's streaming tails itself: the cancelled
    /// turn closes nothing and the latched reader discards every cleanup
    /// response, so without the sweep the half-written answer stays
    /// live-looking forever. Afterwards the recorded status caps the frontier:
    /// even a fully unlatched tick owes no turn out of the interrupted ledger.
    #[tokio::test]
    async fn interrupt_sweeps_streaming_tails_and_the_status_caps_the_frontier() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        ctx.store
            .insert_user_blocks(
                conv,
                vec![InputBlock::Text {
                    content: "hi".into(),
                }],
            )
            .await
            .unwrap();
        ctx.store
            .insert_streaming_block(conv, crate::block::Role::Assistant)
            .await
            .unwrap();

        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_interrupt().await;

        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        assert!(
            blocks
                .iter()
                .all(|b| !b.block_type.starts_with("streaming")),
            "no streaming row survives the interrupt"
        );
        assert_eq!(
            blocks.last().map(|b| b.block_type.as_str()),
            Some("status"),
            "the interrupt recorded its status"
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outcome = scheduler_tick::<BlockKind, _>(&ctx.agency(conv), false, &tx)
            .await
            .unwrap();
        assert!(!outcome.owes_turn, "the status caps the frontier");
        assert!(rx.try_recv().is_err(), "no turn signal past an interrupt");
    }

    /// A queued owed-turn signal delivered after an interrupt: the latch is
    /// re-read at delivery, so the delivery stands down instead of firing by
    /// accident — and one unlatched delivery afterwards fires the turn.
    #[tokio::test]
    async fn latched_owed_turn_delivery_stands_down_until_unlatched() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        ctx.store
            .insert_user_blocks(
                conv,
                vec![InputBlock::Text {
                    content: "hi".into(),
                }],
            )
            .await
            .unwrap();

        let (mut actor, _recheck) = bare_actor(conv, ctx, true);
        actor.handle_blocks_ready().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            0,
            "a latched delivery fires nothing"
        );
        assert!(
            actor.provider_tx.is_none(),
            "a latched delivery binds nothing either"
        );

        actor.handle_unlatch();
        actor.handle_blocks_ready().await;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while probe.requests.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the unlatched delivery never fired the turn"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A provider channel that dies mid-stream — closed with no `Done` — no
    /// longer bricks the conversation: the reader closes the stream on the
    /// provider's behalf, the actor's streaming flag clears, and the next
    /// appended message fires the next turn on a rebound channel.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dead_provider_channel_recovers_on_the_next_message() {
        let (ctx, conv, probe) = composed_runtime(Script::DieMidStreamOnce).await;

        let mut closed_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        // The dead channel's reader announces the close the provider never
        // sent.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match closed_rx.try_recv() {
                Ok(CoreEvent::StreamClosed {
                    conversation_id, ..
                }) if conversation_id == conv => {
                    break;
                }
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the dead channel never closed the stream"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => panic!("subscription failed: {e}"),
            }
        }

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "again".into(),
            }],
        });

        let blocks = await_ledger(&ctx, conv, "the recovered turn's answer", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("recovered"))
        })
        .await;
        assert!(
            blocks
                .iter()
                .all(|b| !b.block_type.starts_with("streaming")),
            "the dead turn's tail was discarded"
        );
        // Two or three, and genuinely timing-dependent — never an equality:
        // the dead channel's close re-checks a signal swallowed mid-turn, so
        // the still-owed first message can re-dispatch BEFORE the second
        // append (three requests) or the second append's wake can be the
        // next dispatch (two). The recovery claim is only that dispatching
        // continues on the rebound channel.
        assert!(
            probe.requests.load(Ordering::SeqCst) >= 2,
            "the next turn fired after the channel died"
        );
    }

    /// Two admitted sibling bodies each block until BOTH are running, with a
    /// timeout that turns a deadlock into a recorded error — so this passes
    /// only if the executor spawns per wakeup and the bodies genuinely
    /// overlap. An executor that awaited a body inline would run the first
    /// sibling to its timeout before the second ever started.
    struct RendezvousTool {
        barrier: Arc<tokio::sync::Barrier>,
    }

    impl ToolHandler<CoreEvent> for RendezvousTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "rendezvous".into(),
                description: "blocks until its sibling is also running".into(),
                parameters: json!({ "type": "object" }),
            }
        }
        fn execute<'a>(
            &'a self,
            _input: &'a str,
            _ctx: ToolContext<'a, CoreEvent>,
        ) -> BoxFuture<'a, ToolOutcome> {
            let barrier = Arc::clone(&self.barrier);
            Box::pin(async move {
                match tokio::time::timeout(Duration::from_secs(5), barrier.wait()).await {
                    Ok(_) => ToolOutcome::Done("overlapped".into()),
                    Err(_) => ToolOutcome::Error("serialized: the sibling never arrived".into()),
                }
            })
        }
    }

    #[tokio::test]
    async fn parallel_tool_bodies_genuinely_overlap() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let mut tools = ToolRegistry::new();
        tools.register(
            "rendezvous",
            RendezvousTool {
                barrier: Arc::new(tokio::sync::Barrier::new(2)),
            },
        );
        let ctx: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            store,
            Arc::new(EventBus::<CoreEvent>::new()),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(tools),
        );

        let (latched, _write_latched) = create_signal(false);
        let executor_rx = ctx.bus.subscribe();
        let executor = tokio::spawn(run_executor(conv, ctx.clone(), latched, executor_rx));

        // Recorded latched so nothing acts at insert: the wakeups below are
        // the only triggers, delivered through the executor under test.
        let agency = ctx.agency(conv);
        let first = ctx
            .runner
            .insert_call(
                &agency,
                true,
                "sib-a".into(),
                "rendezvous".into(),
                "{}".into(),
                CallOrigin::default(),
            )
            .await
            .unwrap();
        let second = ctx
            .runner
            .insert_call(
                &agency,
                true,
                "sib-b".into(),
                "rendezvous".into(),
                "{}".into(),
                CallOrigin::default(),
            )
            .await
            .unwrap();
        ctx.bus.emit(CoreEvent::ToolCallReady {
            conversation_id: conv,
            call_block_id: first,
        });
        ctx.bus.emit(CoreEvent::ToolCallReady {
            conversation_id: conv,
            call_block_id: second,
        });

        let blocks = await_ledger(&ctx, conv, "both rendezvous outcomes", |blocks| {
            blocks
                .iter()
                .filter(|b| b.block_type == "tool_result" || b.block_type == "tool_error")
                .count()
                == 2
        })
        .await;
        let outcomes: Vec<(&str, &str)> = blocks
            .iter()
            .filter(|b| b.block_type == "tool_result" || b.block_type == "tool_error")
            .map(|b| {
                (
                    b.block_type.as_str(),
                    b.fields
                        .get("content")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                )
            })
            .collect();
        assert_eq!(
            outcomes,
            vec![("tool_result", "overlapped"), ("tool_result", "overlapped")],
            "both bodies were running at once"
        );
        executor.abort();
    }

    /// The verifier's probe, pinned: stream, interrupt, unlatch, append — the
    /// next turn lands its assistant block, every run. Before the teardown
    /// the interrupt only cancelled: the binding survived, the same reader
    /// served the next turn with trackers naming the swept rows, its deltas
    /// appended to deleted ids, nothing errored, nothing committed, and in
    /// five of eight observed runs the conversation stalled. Eight runs,
    /// deterministically green.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_turn_after_an_interrupt_lands_its_assistant_block() {
        for run in 0..8 {
            let (ctx, conv, _probe) = composed_runtime(Script::StallFirstTurn).await;

            ctx.bus.emit(CoreEvent::BlocksAppended {
                conversation_id: conv,
                blocks: vec![InputBlock::Text {
                    content: "hi".into(),
                }],
            });
            await_ledger(&ctx, conv, "the stalled turn's live tail", |blocks| {
                blocks
                    .iter()
                    .any(|b| b.block_type == "streaming" && b.fields["content"] == json!("half-"))
            })
            .await;

            ctx.bus.emit(CoreEvent::InterruptRequested {
                conversation_id: conv,
            });
            await_ledger(&ctx, conv, "the interrupt's sweep and status", |blocks| {
                blocks.last().is_some_and(|b| b.block_type == "status")
                    && blocks
                        .iter()
                        .all(|b| !b.block_type.starts_with("streaming"))
            })
            .await;

            ctx.bus.emit(CoreEvent::UnlatchRequested {
                conversation_id: conv,
            });
            ctx.bus.emit(CoreEvent::BlocksAppended {
                conversation_id: conv,
                blocks: vec![InputBlock::Text {
                    content: "again".into(),
                }],
            });

            let blocks = await_ledger(
                &ctx,
                conv,
                "the post-interrupt turn's assistant block",
                |blocks| {
                    blocks.iter().any(|b| {
                        b.block_type == "text" && b.fields["content"] == json!("recovered")
                    })
                },
            )
            .await;
            assert!(
                blocks
                    .iter()
                    .all(|b| !b.block_type.starts_with("streaming")),
                "run {run}: no swept tail resurfaces"
            );
        }
    }

    /// Drain the subscription until a `StreamClosed` for this conversation
    /// arrives — in the `SlowTeardown` scripts the only one is the torn-down
    /// reader's parting close, emitted AFTER its exit cleanup ran.
    async fn await_stream_closed(rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>, conv: i64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match rx.try_recv() {
                Ok(CoreEvent::StreamClosed {
                    conversation_id, ..
                }) if conversation_id == conv => return,
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the torn-down reader never announced its close"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => panic!("subscription failed: {e}"),
            }
        }
    }

    /// Drain every queued terminal for this conversation, counting
    /// `(closes, errors)`.
    fn drain_terminals(
        rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>,
        conv: i64,
    ) -> (usize, usize) {
        let (mut closes, mut errors) = (0, 0);
        while let Ok(event) = rx.try_recv() {
            match event {
                CoreEvent::StreamClosed {
                    conversation_id, ..
                } if conversation_id == conv => closes += 1,
                CoreEvent::StreamError {
                    conversation_id, ..
                } if conversation_id == conv => errors += 1,
                _ => {}
            }
        }
        (closes, errors)
    }

    /// The committed assistant prose, in ledger order.
    fn assistant_answers(blocks: &[Block]) -> Vec<String> {
        blocks
            .iter()
            .filter(|b| b.block_type == "text" && b.role == Some(crate::block::Role::Assistant))
            .map(|b| b.fields["content"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    /// The verifier's `SlowTeardown` repro, pinned: the torn-down reader exits
    /// ASYNCHRONOUSLY, and with a realistic provider wind-down its parting
    /// `StreamClosed` lands while the retyped successor turn is already
    /// streaming. Before lifecycle signals carried binding identity, that
    /// late close cleared the streaming flag on the live turn, and the next
    /// delivery dispatched a SECOND concurrent stream — three requests where
    /// two belong, the two billed turns' prose merged into one assistant
    /// block. With the generation stamp the actor ignores the stale signal:
    /// exactly two requests at the danger point, and every answered turn
    /// lands its own, distinct assistant block.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_torn_down_readers_late_close_never_doubles_a_turn() {
        for run in 0..4 {
            let (ctx, conv, probe) = composed_runtime(Script::SlowTeardown).await;
            let mut closed_rx = ctx.bus.subscribe();

            // Turn one, live and stalled.
            ctx.bus.emit(CoreEvent::BlocksAppended {
                conversation_id: conv,
                blocks: vec![InputBlock::Text {
                    content: "hi".into(),
                }],
            });
            await_ledger(&ctx, conv, "the stalled turn's live tail", |blocks| {
                blocks
                    .iter()
                    .any(|b| b.block_type == "streaming" && b.fields["content"] == json!("half-"))
            })
            .await;

            // Interrupt — the provider winds down slowly — and retype
            // immediately: the successor turn streams on a fresh binding.
            ctx.bus.emit(CoreEvent::InterruptRequested {
                conversation_id: conv,
            });
            await_ledger(&ctx, conv, "the interrupt's status", |blocks| {
                blocks.last().is_some_and(|b| b.block_type == "status")
            })
            .await;
            ctx.bus.emit(CoreEvent::UnlatchRequested {
                conversation_id: conv,
            });
            ctx.bus.emit(CoreEvent::BlocksAppended {
                conversation_id: conv,
                blocks: vec![InputBlock::Text {
                    content: "again".into(),
                }],
            });
            await_ledger(&ctx, conv, "the successor turn's live tail", |blocks| {
                blocks.iter().any(|b| {
                    b.block_type == "streaming" && b.fields["content"] == json!("recovered")
                })
            })
            .await;

            // The torn-down reader's late close lands mid-successor-turn.
            await_stream_closed(&mut closed_rx, conv).await;
            tokio::time::sleep(Duration::from_millis(150)).await;

            // Retype during the live turn: in the stale-signal world the
            // cleared flag let this delivery dispatch the concurrent third
            // stream.
            ctx.bus.emit(CoreEvent::BlocksAppended {
                conversation_id: conv,
                blocks: vec![InputBlock::Text {
                    content: "third".into(),
                }],
            });
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                probe.requests.load(Ordering::SeqCst),
                2,
                "run {run}: exactly two requests — a delivery during the live \
                 successor turn stands down"
            );

            // The live turn completes untouched by the stale close...
            await_ledger(&ctx, conv, "the successor turn's answer", |blocks| {
                blocks
                    .iter()
                    .any(|b| b.block_type == "text" && b.fields["content"] == json!("recovered"))
            })
            .await;

            // ...and the next asked-for turn lands as its own block.
            ctx.bus.emit(CoreEvent::BlocksAppended {
                conversation_id: conv,
                blocks: vec![InputBlock::Text {
                    content: "fourth".into(),
                }],
            });
            let blocks = await_ledger(&ctx, conv, "the third turn's answer", |blocks| {
                blocks
                    .iter()
                    .any(|b| b.block_type == "text" && b.fields["content"] == json!("clean"))
            })
            .await;

            assert_eq!(
                assistant_answers(&blocks),
                vec!["recovered".to_string(), "clean".to_string()],
                "run {run}: two distinct assistant blocks, neither merged"
            );
            assert_eq!(
                probe.requests.load(Ordering::SeqCst),
                3,
                "run {run}: the third turn was the only later request"
            );
        }
    }

    /// The exit-path discard's scoping, pinned: it deletes exactly the rows
    /// the DYING reader tracked, never the conversation-wide sweep — the old
    /// reader exits while the successor turn is streaming, and a wide sweep
    /// running that late would eat the successor's live tail. The tail
    /// survives the torn-down reader's cleanup, and the successor's turn
    /// completes out of it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_torn_down_readers_exit_discard_spares_the_successors_live_tail() {
        let (ctx, conv, _probe) = composed_runtime(Script::SlowTeardown).await;
        let mut closed_rx = ctx.bus.subscribe();

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        await_ledger(&ctx, conv, "the stalled turn's live tail", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "streaming" && b.fields["content"] == json!("half-"))
        })
        .await;

        ctx.bus.emit(CoreEvent::InterruptRequested {
            conversation_id: conv,
        });
        await_ledger(&ctx, conv, "the interrupt's status", |blocks| {
            blocks.last().is_some_and(|b| b.block_type == "status")
        })
        .await;
        ctx.bus.emit(CoreEvent::UnlatchRequested {
            conversation_id: conv,
        });
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "again".into(),
            }],
        });
        await_ledger(&ctx, conv, "the successor turn's live tail", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "streaming" && b.fields["content"] == json!("recovered"))
        })
        .await;

        // The old reader is gone — its exit discard has already run by the
        // time its close reaches the bus.
        await_stream_closed(&mut closed_rx, conv).await;
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        assert!(
            blocks
                .iter()
                .any(|b| b.block_type == "streaming" && b.fields["content"] == json!("recovered")),
            "the successor's live tail survives the torn-down reader's cleanup"
        );

        // And the surviving tail is what the turn commits from.
        let blocks = await_ledger(&ctx, conv, "the successor turn's answer", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("recovered"))
        })
        .await;
        assert!(
            blocks
                .iter()
                .all(|b| !b.block_type.starts_with("streaming")),
            "the completed turn replaced its tail"
        );
    }

    /// The pre-spawn subscription, pinned deterministically: on the
    /// current-thread test runtime NOTHING yields between
    /// `spawn_tool_pipeline` returning and the emit below, so the wakeup is
    /// only ever seen if the subscription predates the spawn — a subscription
    /// taken inside the spawned task misses it every time.
    #[tokio::test]
    async fn a_wakeup_emitted_right_after_the_pipeline_spawn_reaches_the_executor() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let mut tools = ToolRegistry::new();
        tools.register("echo", EchoTool);
        let ctx: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            store,
            Arc::new(EventBus::<CoreEvent>::new()),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(tools),
        );

        // Recorded latched so the insert itself drives nothing: the emit
        // below is the one wakeup, and only the pre-spawn subscription can
        // catch it.
        let agency = ctx.agency(conv);
        let call = ctx
            .runner
            .insert_call(
                &agency,
                true,
                "pre".into(),
                "echo".into(),
                "{}".into(),
                CallOrigin::default(),
            )
            .await
            .unwrap();

        let (latched, _write_latched) = create_signal(false);
        let _handles = spawn_tool_pipeline(conv, &ctx, &latched);
        ctx.bus.emit(CoreEvent::ToolCallReady {
            conversation_id: conv,
            call_block_id: call,
        });

        let blocks = await_ledger(&ctx, conv, "the executed result", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_result")
        })
        .await;
        let result = blocks
            .iter()
            .find(|b| b.block_type == "tool_result")
            .unwrap();
        assert_eq!(result.fields["content"], json!("echoed"));
    }

    /// A body that reports when it starts and — through a drop guard — when
    /// it is torn down; on its own it never finishes.
    struct HangingTool {
        started: tokio::sync::mpsc::UnboundedSender<()>,
        torn_down: Arc<std::sync::atomic::AtomicBool>,
    }

    impl ToolHandler<CoreEvent> for HangingTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "hang".into(),
                description: "runs until torn down".into(),
                parameters: json!({ "type": "object" }),
            }
        }
        fn execute<'a>(
            &'a self,
            _input: &'a str,
            _ctx: ToolContext<'a, CoreEvent>,
        ) -> BoxFuture<'a, ToolOutcome> {
            let started = self.started.clone();
            let torn_down = Arc::clone(&self.torn_down);
            Box::pin(async move {
                struct SetOnDrop(Arc<std::sync::atomic::AtomicBool>);
                impl Drop for SetOnDrop {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::SeqCst);
                    }
                }
                let _guard = SetOnDrop(torn_down);
                let _ = started.send(());
                std::future::pending::<()>().await;
                unreachable!("the pending future never resolves")
            })
        }
    }

    // ─── The dispatch identity (slice 8): anchors per proven shape ───────

    /// The one user text block in a ledger — the summoning frontier the
    /// anchor assertions name.
    fn user_block(blocks: &[Block]) -> &Block {
        blocks
            .iter()
            .find(|b| b.block_type == "text" && b.role == Some(crate::block::Role::User))
            .expect("the summoning user message")
    }

    /// Every turn-product block carries the summoning frontier's id and every
    /// non-turn block carries null — the per-shape pin, written once.
    fn assert_anchors(blocks: &[Block], summoner: i64) {
        for block in blocks {
            let is_turn_product = match block.block_type.as_str() {
                "date_marker" => false,
                "text" => block.role == Some(crate::block::Role::Assistant),
                _ => true,
            };
            if block.id == summoner {
                assert_eq!(block.dispatch_anchor, None, "the summoner anchors nothing");
            } else if is_turn_product {
                assert_eq!(
                    block.dispatch_anchor,
                    Some(summoner),
                    "block {} ({}) carries the summoning frontier's id",
                    block.id,
                    block.block_type
                );
            } else {
                assert_eq!(
                    block.dispatch_anchor, None,
                    "block {} ({}) is not a turn's product",
                    block.id, block.block_type
                );
            }
        }
    }

    /// AC8-3, the plain shape: one prose answer, anchored on the message that
    /// summoned it; the message and the date marker carry null.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_plain_shape_anchors_the_answer_on_the_summoning_message() {
        let (ctx, conv, _probe) = composed_runtime(Script::Prose).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the plain answer", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("answered"))
        })
        .await;
        assert_anchors(&blocks, user_block(&blocks).id);
    }

    /// AC8-3, the narrated shape, plus AC8-5's one-step read: narration, the
    /// call, its result and the closing prose all carry the original
    /// summoning message's id — the closing turn was dispatched off the
    /// RESULT and inherited — and the summoning message loads from the call
    /// block through the public API in one step.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_narrated_round_anchors_every_product_and_loads_in_one_step() {
        let (ctx, conv, _probe) = composed_runtime(Script::ToolRound {
            tool: "echo",
            narration: Some("thinking it through"),
        })
        .await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the narrated round's close", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        let shape: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            shape,
            vec![
                "date_marker",
                "text",
                "text",
                "tool_call",
                "tool_result",
                "text"
            ],
            "narration commits BEFORE the call — the real wire order"
        );
        let summoner = user_block(&blocks).id;
        assert_anchors(&blocks, summoner);

        // AC8-5: one step from the call block to the summoning message.
        let call = blocks.iter().find(|b| b.block_type == "tool_call").unwrap();
        let loaded = ctx
            .store
            .find_block(call.dispatch_anchor.expect("the call is anchored"))
            .await
            .unwrap()
            .expect("the summoning message loads");
        assert_eq!(loaded.id, summoner);
        assert_eq!(loaded.fields["content"], json!("hi"));
    }

    /// AC8-3, the multi-round shape with inheritance: the SECOND round's
    /// dispatching frontier is the first round's result — a turn product — so
    /// its call inherits the original summoning message's id, and the
    /// one-step read holds in the continuation round exactly as in round one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_continuation_rounds_call_inherits_the_original_anchor() {
        let (ctx, conv, probe) = composed_runtime(Script::TwoToolRounds).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the two-round close", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            3,
            "two call rounds and the close — got {:?}",
            probe.shapes.lock().unwrap()
        );
        let summoner = user_block(&blocks).id;
        assert_anchors(&blocks, summoner);

        let calls: Vec<&Block> = blocks
            .iter()
            .filter(|b| b.block_type == "tool_call")
            .collect();
        assert_eq!(calls.len(), 2);
        for call in calls {
            let loaded = ctx
                .store
                .find_block(call.dispatch_anchor.expect("anchored"))
                .await
                .unwrap()
                .expect("one step, round one and round two alike");
            assert_eq!(loaded.id, summoner);
        }
    }

    /// AC8-3, the failed round: the tool errors, the error block copies the
    /// call's anchor, and the turn dispatched off the ERROR inherits it too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_rounds_error_and_its_following_turn_stay_anchored() {
        let (ctx, conv, _probe) = composed_runtime(Script::ToolRound {
            tool: "boom",
            narration: None,
        })
        .await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the failed round's close", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        assert!(
            blocks.iter().any(|b| b.block_type == "tool_error"),
            "the round failed"
        );
        assert_anchors(&blocks, user_block(&blocks).id);
    }

    /// AC8-3, the interrupted shape: the live streaming tail carries the
    /// turn's anchor, and the interrupt's status block takes the actor's
    /// current anchor — the interrupted turn is the turn the status is about.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_interrupted_shape_anchors_the_tail_and_the_status() {
        let (ctx, conv, _probe) = composed_runtime(Script::StallFirstTurn).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the stalled turn's live tail", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "streaming" && b.fields["content"] == json!("half-"))
        })
        .await;
        let summoner = user_block(&blocks).id;
        let tail = blocks.iter().find(|b| b.block_type == "streaming").unwrap();
        assert_eq!(
            tail.dispatch_anchor,
            Some(summoner),
            "the live tail is a turn product too"
        );

        ctx.bus.emit(CoreEvent::InterruptRequested {
            conversation_id: conv,
        });
        let blocks = await_ledger(&ctx, conv, "the interrupt's status", |blocks| {
            blocks.last().is_some_and(|b| b.block_type == "status")
        })
        .await;
        let status = blocks.last().unwrap();
        assert_eq!(
            status.dispatch_anchor,
            Some(summoner),
            "the interrupt's status takes the actor's current anchor"
        );
    }

    /// The out-of-band tool path records a NULL anchor — the documented
    /// answer a consumer folds to its floor.
    #[tokio::test]
    async fn an_out_of_band_call_records_a_null_anchor() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let (actor, _recheck) = bare_actor(conv, ctx.clone(), true);
        actor
            .handle_tool_call_received("oob-1".into(), "echo".into(), "{}".into())
            .await;
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        let call = blocks
            .iter()
            .find(|b| b.block_type == "tool_call")
            .expect("the out-of-band call is recorded");
        assert_eq!(call.dispatch_anchor, None);
    }

    /// AC8-4 — the duplicate-turn reproduction, with real interleaving: the
    /// scripted provider HOLDS the post-message-end window open, the test
    /// appends a message inside it, and no second turn dispatches — the
    /// proven defect fired exactly here, off the message-end settling the
    /// dispatch state. Releasing the hold records the tool round and closes
    /// the turn, and the absorbed message's turn then fires exactly once,
    /// with its request containing the absorbed text; exactly one further
    /// answer block lands.
    ///
    /// What this composed shape CANNOT isolate is the close's re-check
    /// itself: the released tool round's inserts and resolution are
    /// change-hook wakes of their own, each an ordinary scheduler tick that
    /// would re-derive the owed turn even if the close nudged nothing — so
    /// a broken re-check would still pass here, masked. The re-check's
    /// direct pin is the nudge counter in
    /// [`the_dispatch_state_closes_on_the_named_edges_only`], where no other
    /// wake exists; this test pins the end-to-end absence of the double
    /// dispatch and the exactly-once answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_append_in_the_held_window_is_dispatched_by_the_close_not_doubled() {
        let (ctx, conv, probe) = composed_runtime(Script::HoldToolRound { tool: "echo" }).await;
        let mut done_rx = ctx.bus.subscribe();

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        // The window opens: the message end has been ingested, the tool round
        // has NOT been recorded — the provider holds it.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match done_rx.try_recv() {
                Ok(CoreEvent::StreamDone {
                    conversation_id, ..
                }) if conversation_id == conv => break,
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the held turn's message end never arrived"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => panic!("subscription failed: {e}"),
            }
        }

        // The append inside the held window.
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "absorbed".into(),
            }],
        });
        await_ledger(&ctx, conv, "the absorbed message", |blocks| {
            blocks
                .iter()
                .any(|b| b.fields.get("content") == Some(&json!("absorbed")))
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "no second turn dispatches inside the held window — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // Release: the tool round records, the trailing done closes the
        // turn, and the close's re-check re-drives the ratchet — which parks
        // on the recorded call until its result lands and then dispatches
        // the absorbed message's turn, deterministically.
        probe.release.notify_one();
        let blocks = await_ledger(&ctx, conv, "the absorbed message's answer", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "exactly one following turn — got {:?}",
            probe.shapes.lock().unwrap()
        );
        assert!(
            probe.seen.lock().unwrap()[1].iter().any(|m| {
                match &m.content {
                    MessageContent::Text(text) => text.contains("absorbed"),
                    MessageContent::Parts(parts) => parts.iter().any(
                        |p| matches!(p, ContentPart::Text { text } if text.contains("absorbed")),
                    ),
                }
            }),
            "the following turn's request contains the absorbed message"
        );
        assert_eq!(
            assistant_answers(&blocks),
            vec!["done".to_string()],
            "exactly one further answer block lands"
        );
        // The absorbed message was recorded inside the window: before the
        // call block in ledger order.
        let absorbed = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("absorbed")))
            .unwrap();
        let call = blocks.iter().find(|b| b.block_type == "tool_call").unwrap();
        let order: Vec<i64> = blocks.iter().map(|b| b.id).collect();
        assert!(
            order.iter().position(|id| *id == absorbed.id)
                < order.iter().position(|id| *id == call.id),
            "the append really landed inside the held window"
        );
    }

    /// The abandoned-turn fence, composed over the verifier's exploit shape
    /// (2026-08-22): a turn ends its message and stalls past the drain
    /// deadline; the reader abandons it — one terminal — and the close
    /// retires the binding. A next message then dispatches the successor
    /// turn on a fresh binding, and only AFTER that does the stalled
    /// provider deliver the abandoned turn's held tail: the trailing tool
    /// lifecycles and the late done. The tail reaches nobody — no second
    /// terminal fires on either turn, the successor keeps its dispatch state
    /// and seam mid-flight, no tool call from the tail is ever recorded, and
    /// every committed product carries its OWN summoner's anchor, never null
    /// and never the other turn's. Without the fence the late done closed
    /// the live turn mid-flight and the late lifecycles ingested under its
    /// anchor. Paused time: the drain deadline elapses virtually.
    #[tokio::test(start_paused = true)]
    async fn an_abandoned_turns_late_tail_reaches_no_successor_turn() {
        let (ctx, conv, probe) = composed_runtime(Script::StallPastDeadlineThenLateTail).await;
        let mut rx = ctx.bus.subscribe();

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        // The first turn's message end commits its prose; the stall follows,
        // and the drain deadline closes the turn — the first terminal.
        await_stream_closed(&mut rx, conv).await;

        // The successor: appended after the abandoned close, dispatched on a
        // fresh binding, streaming and still open.
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "next".into(),
            }],
        });
        await_ledger(
            &ctx,
            conv,
            "the successor turn's streaming tail",
            |blocks| blocks.iter().any(|b| b.block_type.starts_with("streaming")),
        )
        .await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "the successor turn dispatched exactly once — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // The abandoned tail is delivered NOW, mid-flight of the successor.
        probe.release.notify_one();
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }

        // It reached nobody: no terminal beyond the abandoned close, the
        // successor's tail still open, no tool call recorded, no redispatch.
        assert_eq!(
            drain_terminals(&mut rx, conv),
            (0, 0),
            "the late tail fired no terminal on either turn"
        );
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        assert!(
            blocks.iter().any(|b| b.block_type.starts_with("streaming")),
            "the successor turn is still mid-flight — its state was not cleared"
        );
        assert!(
            blocks.iter().all(|b| b.block_type != "tool_call"),
            "no lifecycle from the abandoned tail was recorded"
        );
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "the late done redispatched nothing — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // The successor finishes normally, its products intact.
        probe.finish.notify_one();
        let blocks = await_ledger(&ctx, conv, "the successor's committed answer", |blocks| {
            assistant_answers(blocks) == vec!["half-".to_string(), "recovered".to_string()]
                && blocks
                    .iter()
                    .all(|b| !b.block_type.starts_with("streaming"))
        })
        .await;

        // Each turn's product anchors on its OWN summoning message; every
        // other block carries null.
        let hi = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("hi")))
            .expect("the first summoner")
            .id;
        let next = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("next")))
            .expect("the second summoner")
            .id;
        for block in &blocks {
            let expected = match block.fields.get("content").and_then(|c| c.as_str()) {
                Some("half-") => Some(hi),
                Some("recovered") => Some(next),
                _ => None,
            };
            assert_eq!(
                block.dispatch_anchor, expected,
                "block {} ({}) anchors on its own turn's summoner",
                block.id, block.block_type
            );
        }

        // Exactly one terminal per turn over the whole run: the abandoned
        // close already drained above, the successor's own close now.
        assert_eq!(
            drain_terminals(&mut rx, conv),
            (1, 0),
            "the successor's close is the run's second and last terminal"
        );
    }

    /// VERIFIER VARIANT: the stalled provider wakes with a WHOLE second
    /// round (prose, a second `MessageEnd`, a tool lifecycle, a trailing
    /// `Done`, then another prose+end+done) while the successor turn is
    /// mid-flight. Nothing of it may reach the ledger or the actor.
    #[tokio::test(start_paused = true)]
    async fn a_late_full_round_from_an_abandoned_turn_reaches_no_successor() {
        let (ctx, conv, probe) = composed_runtime(Script::StallPastDeadlineThenLateFullRound).await;
        let mut rx = ctx.bus.subscribe();

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        await_stream_closed(&mut rx, conv).await;

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "next".into(),
            }],
        });
        await_ledger(
            &ctx,
            conv,
            "the successor turn's streaming tail",
            |blocks| blocks.iter().any(|b| b.block_type.starts_with("streaming")),
        )
        .await;
        assert_eq!(probe.requests.load(Ordering::SeqCst), 2);

        probe.release.notify_one();
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            drain_terminals(&mut rx, conv),
            (0, 0),
            "the late full round fired no terminal"
        );
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        assert!(
            blocks.iter().any(|b| b.block_type.starts_with("streaming")),
            "the successor turn is still mid-flight"
        );
        assert!(
            blocks.iter().all(|b| b.block_type != "tool_call"),
            "no lifecycle from the late round was recorded"
        );
        assert!(
            !blocks.iter().any(|b| b
                .fields
                .get("content")
                .and_then(|c| c.as_str())
                .is_some_and(|t| t.contains("ghost"))),
            "no prose from the late round reached the ledger: {:?}",
            blocks
                .iter()
                .map(|b| (b.block_type.clone(), b.fields.get("content").cloned()))
                .collect::<Vec<_>>()
        );
        assert_eq!(probe.requests.load(Ordering::SeqCst), 2, "no redispatch");

        probe.finish.notify_one();
        let blocks = await_ledger(&ctx, conv, "the successor's committed answer", |blocks| {
            assistant_answers(blocks) == vec!["half-".to_string(), "recovered".to_string()]
                && blocks
                    .iter()
                    .all(|b| !b.block_type.starts_with("streaming"))
        })
        .await;
        let hi = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("hi")))
            .unwrap()
            .id;
        let next = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("next")))
            .unwrap()
            .id;
        for block in &blocks {
            let expected = match block.fields.get("content").and_then(|c| c.as_str()) {
                Some("half-") => Some(hi),
                Some("recovered") => Some(next),
                _ => None,
            };
            assert_eq!(
                block.dispatch_anchor, expected,
                "block {} ({}) anchors on its own turn's summoner",
                block.id, block.block_type
            );
        }
        assert_eq!(
            drain_terminals(&mut rx, conv),
            (1, 0),
            "exactly one further terminal: the successor's own close"
        );
    }

    /// The abnormal-stop close, composed — the max-tokens mid-tool-lifecycle
    /// shape: the stop's error terminal is deferred to the reader's drain, so
    /// every product of the cut-off turn (the partial prose, the error
    /// status, the released call and, when its body outruns the latch, the
    /// result) still carries the summoning message's anchor; the turn closes
    /// exactly once, on the deferred error, which latches; and no second
    /// dispatch fires anywhere in the window — closing at the error signal
    /// while the reader was still draining released the streaming flag
    /// mid-turn, the duplicate-turn shape wearing an error stop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_max_tokens_stop_mid_tool_lifecycle_anchors_latches_and_never_doubles() {
        let (ctx, conv, probe) = composed_runtime(Script::MaxTokensToolRound).await;
        let mut state_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        // The drained turn's products are all recorded: the finalized prose,
        // the stop's explanation, the released call.
        let blocks = await_ledger(&ctx, conv, "the cut-off turn's products", |blocks| {
            blocks.iter().any(|b| b.block_type == "status")
                && blocks.iter().any(|b| b.block_type == "tool_call")
                && blocks
                    .iter()
                    .any(|b| b.block_type == "text" && b.fields["content"] == json!("cut-"))
        })
        .await;
        assert_anchors(&blocks, user_block(&blocks).id);

        // Settle, then hold still: a close that released the dispatch state
        // at the error signal would let the tool result's owed turn dispatch
        // here.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        assert_anchors(&blocks, user_block(&blocks).id);
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "the cut-off turn is the only dispatch — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // The deferred error latched: the last broadcast state says so, and
        // nothing after the terminal unlatches.
        let mut last_latched = None;
        while let Ok(event) = state_rx.try_recv() {
            if let CoreEvent::ConversationState { latched, .. } = event {
                last_latched = Some(latched);
            }
        }
        assert_eq!(
            last_latched,
            Some(true),
            "the abnormal stop's terminal latches, exactly like any stream error"
        );
    }

    /// A turn that writes nothing closes to REST. The close re-checks only a
    /// signal the open turn swallowed, and it re-checks through the
    /// scheduler's drive — so the unchanged frontier is re-asked at most
    /// once (for the mid-turn tick the dispatch itself bought) and then the
    /// conversation rests. A close that signalled the dispatch delivery
    /// blindly redispatched the unchanged frontier once per close, forever:
    /// one paid provider request per iteration with no backoff and no cap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_empty_turn_closes_to_rest_instead_of_redispatching_forever() {
        let (ctx, conv, probe) = composed_runtime(Script::EmptyTurn).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        // The turn fires and answers with nothing.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while probe.requests.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the empty turn never dispatched"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Settle, then hold still: under the blind close the counter climbs
        // through this whole window.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let settled = probe.requests.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            settled,
            "the conversation rests after an empty turn"
        );
        // Exactly two, derived: the append's coalesced wakes are one owing
        // tick plus the one extra tick the cursor confirm buys, so exactly
        // two owed-turn signals exist — the first dispatches, the second
        // reaches the actor either after the instant empty turn closed
        // (dispatching directly) or during it (deferred, re-checked at the
        // close, re-signalled once, dispatched). Nothing wakes the scheduler
        // after that, so the re-check chain ends at two instead of looping.
        assert_eq!(
            settled, 2,
            "one dispatch per owed-turn signal, the confirm-bought re-ask included, then rest"
        );
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        assert!(
            blocks
                .iter()
                .all(|b| b.role != Some(crate::block::Role::Assistant)),
            "the empty turn really wrote nothing"
        );
    }

    /// A turn that emits TWO tool calls: while the first sibling's result is
    /// recorded and the second still runs, the tail awaits the model but a
    /// call still dangles — a shape only the drive's parked gate can see.
    /// Because the close re-checks THROUGH that drive, no request is ever
    /// built around the dangling sibling: every dispatched request carries a
    /// result for every call it shows, and the continuation fires exactly
    /// once, off the second result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_two_call_round_never_dispatches_around_a_dangling_sibling() {
        let (ctx, conv, probe) = composed_runtime(Script::TwoCallsInOneTurn).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the two-call round's close", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "the call round and one continuation, nothing between the results — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // Every request is complete: a ToolUse part without its ToolResult
        // in the same request is the dangling-call dispatch.
        for (index, messages) in probe.seen.lock().unwrap().iter().enumerate() {
            let mut uses = Vec::new();
            let mut results = Vec::new();
            for message in messages {
                if let MessageContent::Parts(parts) = &message.content {
                    for part in parts {
                        match part {
                            ContentPart::ToolUse { id, .. } => uses.push(id.clone()),
                            ContentPart::ToolResult { tool_use_id, .. } => {
                                results.push(tool_use_id.clone());
                            }
                            _ => {}
                        }
                    }
                }
            }
            for id in &uses {
                assert!(
                    results.contains(id),
                    "request {index} carries call {id} with no matching result — dispatched around a dangling call"
                );
            }
        }

        // Both calls and both results landed, all anchored on the summoning
        // message.
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b.block_type == "tool_call")
                .count(),
            2
        );
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b.block_type == "tool_result")
                .count(),
            2
        );
        assert_anchors(&blocks, user_block(&blocks).id);
    }

    /// Aborting the executor aborts the in-flight bodies too: they join a set
    /// the loop owns, so the pipeline handles the actor aborts on shutdown
    /// really end the pipeline — bodies included. (The set adds no
    /// concurrency bound; the overlap test above still holds.)
    #[tokio::test]
    async fn aborting_the_executor_aborts_in_flight_tool_bodies() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let torn_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tools = ToolRegistry::new();
        tools.register(
            "hang",
            HangingTool {
                started: started_tx,
                torn_down: Arc::clone(&torn_down),
            },
        );
        let ctx: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
            store,
            Arc::new(EventBus::<CoreEvent>::new()),
            Arc::new(crate::providers::ProviderRegistry::new()),
            Arc::new(tools),
        );

        let (latched, _write_latched) = create_signal(false);
        let executor_rx = ctx.bus.subscribe();
        let executor = tokio::spawn(run_executor(conv, ctx.clone(), latched, executor_rx));

        let agency = ctx.agency(conv);
        let call = ctx
            .runner
            .insert_call(
                &agency,
                true,
                "h1".into(),
                "hang".into(),
                "{}".into(),
                CallOrigin::default(),
            )
            .await
            .unwrap();
        ctx.bus.emit(CoreEvent::ToolCallReady {
            conversation_id: conv,
            call_block_id: call,
        });
        started_rx.recv().await.expect("the body started");

        executor.abort();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !torn_down.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "the in-flight body outlived the aborted executor"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
