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

use crate::agency::{AgencyCtx, RuntimeKind, ToolCall, ratchet, redispatch};
use crate::block::Block;
use crate::bus::{EventBus, RuntimeEvent};
use crate::dispatch::TurnAnchor;
use crate::event::{AsCoreEvent, CoreEvent};
use crate::providers::types::ReasoningLevel;
use crate::providers::{ModelSelector, ProviderRegistry, ProviderRequest, ProviderTx};
use crate::reactive;
use crate::reactivity::{ReadSignal, WriteSignal, create_signal};
use crate::store::Store;
use crate::tools::choice::ResolvedTools;
use crate::tools::{CallOrigin, ToolRegistry, ToolRunner, submit_approval};
use crate::types::{ApprovalChoice, InputBlock, StopReason};

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
    /// Whether the metadata worker derives conversation titles. A behavior
    /// switch, not a collaborator — the context stays the four collaborators
    /// and no service locator. On by default; a consumer that wants no title
    /// traffic at all turns it off at construction, and the runtime then
    /// spawns no metadata worker and dispatches no title request, ever.
    title_derivation: bool,
}

impl<K: RuntimeKind, E> Clone for RuntimeContext<K, E> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            bus: Arc::clone(&self.bus),
            providers: Arc::clone(&self.providers),
            runner: Arc::clone(&self.runner),
            title_derivation: self.title_derivation,
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
            title_derivation: true,
        }
    }

    /// The same context with title derivation switched OFF: the runtime
    /// spawns no metadata worker for any conversation, so no title request
    /// row is ever appended and no title stream is ever dispatched. The
    /// default — untouched contexts — keeps the derivation on.
    #[must_use]
    pub fn without_title_derivation(mut self) -> Self {
        self.title_derivation = false;
        self
    }

    /// The same context with a different CONVERSATION-wide tool-call window on
    /// its runner (2026-08-30) — the shape above, for the numbers that live on
    /// the runner instead of here.
    ///
    /// Test-only by decision, for the GLOBAL numbers alone: they are the
    /// operator's, recorded once as the window's own defaults, and a
    /// consumer-facing knob would be a second place they are decided. The
    /// library's own tests build a small window through here so the window's
    /// behavior is provable without spending sixty calls per assertion. A
    /// window bound to ONE tool is the consumer's own number and has a public
    /// builder of its own ([`Self::with_tool_window`]).
    ///
    /// The write is construction-time and the type says so: this builder
    /// still holds the runner's sole reference, so `Arc::get_mut` hands over
    /// the exclusive borrow the setter needs. A context already shared with a
    /// runtime fails here loudly rather than writing a window half its
    /// readers would never see.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_tool_call_window(
        mut self,
        window: crate::tools::runner::ToolCallWindow,
    ) -> Self {
        Arc::get_mut(&mut self.runner)
            .expect("the tool-call window is set while the context still owns its runner alone")
            .set_window(window);
        self
    }

    /// The same context with ONE TOOL bound to a window of its own
    /// (2026-08-30): exactly `calls` calls of `name` run inside any trailing
    /// `seconds`, and the next one is refused — it resolves with a tool error
    /// the model reads and re-plans against instead of running.
    ///
    /// PUBLIC, and that is the surface decision: which tools exist and how
    /// hard one of them may be leaned on is the consumer's knowledge, never
    /// this library's — nothing here ships a tool name, and a context nobody
    /// calls this on bounds no tool at all. The conversation-wide window's
    /// numbers stay the operator's, with no consumer knob at all.
    ///
    /// Call it once per tool a deployment wants bounded; a second call for the
    /// same name replaces that tool's bound. A bound of ZERO calls is legal:
    /// the count includes the call under admission, so the very first one is
    /// already past it and the tool always refuses. A refused call stays a
    /// recorded call — it counts against this window and the conversation's
    /// alike — and a run of refusals ends the turn on the same consecutive
    /// limit a conversation-wide run does.
    ///
    /// `name` must be a tool this context's registry holds. The registry is
    /// the whole set of names a call can resolve against, and a bound on
    /// anything else — a typo — would compile, pass and silently protect
    /// nothing, so the builder fails loudly instead, the same discipline the
    /// registry's own registration-collision panic carries.
    ///
    /// **Configure every window BEFORE the context is shared.** The write is
    /// construction-time and the type says so: this builder still holds the
    /// runner's sole reference, so `Arc::get_mut` hands over the exclusive
    /// borrow the setter needs, and every reader afterwards takes the bound
    /// without a lock.
    ///
    /// # Panics
    ///
    /// If `seconds` is zero or negative — a window needs a span to trail.
    ///
    /// If `name` names no tool this context's registry holds.
    ///
    /// If the context or its runner is ALREADY SHARED — a clone taken, a
    /// runtime spawned, or a [`runner()`](Self::runner) `Arc` still held. Any
    /// of those means a reader that would never see this write, so the
    /// builder fails loudly here rather than leaving the runtime disagreeing
    /// with itself about a tool's window.
    #[must_use]
    pub fn with_tool_window(mut self, name: impl Into<String>, calls: usize, seconds: i64) -> Self {
        let name = name.into();
        assert!(
            seconds > 0,
            "a tool's window needs a positive span: `{name}` was given {seconds} seconds"
        );
        let runner = Arc::get_mut(&mut self.runner)
            .expect("a tool's window is set while the context still owns its runner alone");
        assert!(
            runner.registry().get(&name).is_some(),
            "a tool's window is bound to a name nothing registered: `{name}`. The registry \
             is the whole set of names a call can resolve against — bind only a tool it holds"
        );
        runner.set_tool_window(
            name,
            crate::tools::runner::ToolWindowBound { calls, seconds },
        );
        self
    }

    /// Whether the metadata worker derives conversation titles.
    #[must_use]
    pub fn title_derivation(&self) -> bool {
        self.title_derivation
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

/// What a conversation's scheduler tells its actor, both read off the SAME
/// drive (2026-09-01).
///
/// The channel carried a bare wakeup while the frontier gate was the only
/// thing a drive could report. The second fact is the conversation's own
/// existence — the drive's cursor read answers it — and it belongs on this
/// channel for the same reason the first does: the scheduler is the single
/// driver, so what it derives travels one way, to the actor that acts on it.
pub(crate) enum SchedulerSignal {
    /// The frontier gate owes a model turn.
    OwesTurn,
    /// The conversation no longer exists. Its actor ends, and the set the
    /// actor owns ends with it.
    ConversationGone,
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
/// [`SchedulerSignal::OwesTurn`] when the frontier gate owes a model turn; the
/// actor reads the ledger, sends [`ProviderRequest::Stream`], and the
/// ingestion reader handles responses. It signals
/// [`SchedulerSignal::ConversationGone`] when the conversation ceased to
/// exist, and this actor's own end is the end of its whole set.
struct ConversationActor<K: RuntimeKind, E> {
    id: i64,
    ctx: RuntimeContext<K, E>,
    mailbox: tokio::sync::mpsc::UnboundedReceiver<CoreEvent>,
    write_latched: WriteSignal<bool>,
    read_latched: ReadSignal<bool>,
    from_scheduler: tokio::sync::mpsc::UnboundedReceiver<SchedulerSignal>,
    /// The close edges' re-check nudge: a monotonic count the scheduler's
    /// reactive loop tracks, bumped when a close finds an owed-turn signal
    /// was swallowed while the turn was open. The nudge wakes the SCHEDULER,
    /// never the dispatch delivery directly: the scheduler's ratchet drive is
    /// the one place the owed-turn check lives — its parked gate included —
    /// so the close re-runs the whole check instead of a tail-only half of
    /// it, and the drive signals [`SchedulerSignal::OwesTurn`] exactly when
    /// the re-checked ledger really owes a turn.
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
    /// turn's anchor, cleared at every stream close, and read by the
    /// binding's ingestion reader at every insert. Replaced at every bind, so
    /// a torn-down reader keeps its own, already-cleared slot and can never
    /// stamp a successor turn's identity.
    turn_anchor: TurnAnchor,
    /// The open TURN's anchor, held by the actor (amended 2026-08-22, after
    /// the consumer's adversarial verification proved the tail-derived
    /// inheritance insufficient): set at the turn's first dispatch and held
    /// until a close that ENDS the turn — the end-turn stop, the error edge,
    /// the interrupt teardown, or the reader's abandoned mark. A tool-use
    /// stop closes the STREAM but leaves this held while a continuation is
    /// genuinely due ([`Self::turn_continuation_due`], refined 2026-08-22,
    /// amended 2026-08-23),
    /// and every continuation dispatch reuses it whatever the tail is — which
    /// is what
    /// keeps a message absorbed between a round's result and the
    /// continuation dispatch from re-anchoring the turn onto itself, the
    /// consumer's proven escalation. The reuse REVALIDATES the hold against
    /// the dispatch's own snapshot first (2026-08-30, the ends-turn stamp): a
    /// turn ended by a stamped resolution owes nothing and therefore signals
    /// nobody, so the one site that reads this cache asks the release rule
    /// there. A fresh turn — nothing held, or a hold the rule no longer
    /// supports — resolves
    /// its anchor from the dispatch snapshot through
    /// [`ratchet::fresh_turn_anchor`] — ledger-first since 2026-08-23, the
    /// verified seventh break, which DEMOTES this field to a consistency
    /// cache over a ledger-derivable fact: a released turn's unanswered
    /// outcome re-attaches the identity at the resuming dispatch even when
    /// a message was absorbed behind it, and a fresh actor over the same
    /// ledger — the restart shape — derives the same turn a live actor was
    /// holding. Distinct from the seam above on purpose:
    /// the seam is a stream-scoped slot the reader consumes and it dies with
    /// the binding, while this identity spans every stream of one turn and
    /// survives a rebind — a continuation recovered on a fresh binding is
    /// still the same turn. A close that ends this identity while the
    /// turn's last outcome is unanswered also writes the end into the
    /// ledger (2026-08-23, [`Self::settle_turn_identity`]): turn closure is
    /// a stored fact, never re-derived from side effects.
    open_turn: Option<i64>,
    /// How many tool outcomes anchored on the open turn ASK FOR a
    /// continuation its dispatches have already answered (2026-08-23, the
    /// verified sixth break's counting arm): recorded at EVERY dispatch that
    /// sets or reuses the identity, from the dispatch's own snapshot — the
    /// same one the request is built from, and through the same fold
    /// ([`ToolCall::outcomes_anchored_in`]) the release rule measures
    /// against. An ends-turn-stamped result is outside that fold on both
    /// sides (2026-08-30): it still rides the request, because the model's
    /// call and its result must stay paired, while nothing is owed for it and
    /// nothing is marked as answering it. The release rule compares the
    /// close's outcome count
    /// against it: an outcome above this mark has not had its continuation
    /// yet, and that continuation is the one thing a tool-use close still
    /// owes. Kept in outcome units, not as a dispatch tally, by decision:
    /// one dispatch can answer several outcomes at once (a multi-call round
    /// resolving before its continuation), and a turn resumed off its
    /// outcome tail — an approval resolution, a restart recovery — has that
    /// outcome answered by the resuming dispatch itself; a per-dispatch
    /// increment undercounts the first and overcounts the second, and each
    /// error re-opens a proven leak shape. Meaningless while `open_turn` is
    /// `None`; rewritten before every read.
    answered_outcomes: usize,
    /// The stop reason the open stream reported at its message-end, consumed
    /// by the close to decide whether the turn's identity survives the
    /// stream: only a tool-use stop leaves the turn open. Taken at every
    /// close, so a stale stop can never speak for a later stream.
    stream_stop: Option<StopReason>,
    /// How long a reader waits for a turn's remaining events after its
    /// message-end. The production value is the ingestion module's named
    /// constant; tests construct short ones to pin the expiry paths.
    drain_deadline: std::time::Duration,
}

/// Which of the three close edges reached
/// [`ConversationActor::close_dispatch`]. Every edge settles the stream; the
/// edge decides what else the close does — the error edge latches, and only
/// [`CloseEdge::Closed`] can leave the turn's held identity open (when the
/// stream stopped for tool use, the reader did not abandon it, and the
/// close's snapshot still owes a continuation).
#[derive(Clone, Copy, PartialEq, Eq)]
enum CloseEdge {
    /// The stream's closed signal: the reader finished the stream, tool
    /// lifecycles drained.
    Closed,
    /// The stream's error signal, from either of its two producers — the
    /// binding's reader or the actor's own store-failure paths (the
    /// ownership rule is documented on
    /// [`ConversationActor::owns_stream_signal`]).
    Errored,
    /// The interrupt teardown: the actor tore the binding down itself.
    Teardown,
}

impl CloseEdge {
    /// The machine key the close-marker status records for a turn this edge
    /// ended — the stored fact of the turn's closure (2026-08-23), naming
    /// the turn's end and the edge that closed it. `None` for the teardown:
    /// the interrupt appends its own anchored status as that end's record,
    /// and a second marker would record one end twice.
    fn turn_end_key(self) -> Option<&'static str> {
        match self {
            CloseEdge::Closed => Some(crate::agency::Status::TURN_ENDED_CLOSED),
            CloseEdge::Errored => Some(crate::agency::Status::TURN_ENDED_ERRORED),
            CloseEdge::Teardown => None,
        }
    }
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
                Some(signal) = self.from_scheduler.recv() => {
                    match signal {
                        SchedulerSignal::OwesTurn => ready = true,
                        // The conversation this actor exists for is gone, so
                        // the actor is over — and the whole set with it, since
                        // this loop's return aborts every loop spawned beside
                        // it. Nothing was told to stop: the scheduler read the
                        // fact off the ledger's own cursor and reported it.
                        SchedulerSignal::ConversationGone => {
                            tracing::info!(
                                conversation_id = self.id,
                                "conversation gone — the actor set ends"
                            );
                            break;
                        }
                    }
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
            CoreEvent::StreamError { .. } => self.handle_stream_error().await,
            CoreEvent::StreamClosed { .. } => self.handle_stream_closed().await,
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
        // edges, and one that ENDS the turn's identity. The torn-down
        // reader's own terminal is discarded later by generation, so this is
        // the one place the interrupted turn's dispatch state settles. The
        // held identity is read before the close ends it: the status block
        // below is ABOUT that turn — and because the identity is actor
        // state, not the stream-scoped seam, an interrupt arriving BETWEEN
        // a tool turn's rounds still names the turn it tore down.
        let interrupted_anchor = self.open_turn;
        self.close_dispatch(CloseEdge::Teardown).await;

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

    fn handle_stream_done(&mut self, stop_reason: Option<StopReason>) {
        // Message-end no longer settles the dispatch state (2026-08-22): the
        // turn's tool lifecycles arrive AFTER this signal, and settling here
        // was the proven duplicate-turn window. The stop reason is recorded
        // for the CLOSE, which consumes it to decide whether the turn's
        // identity survives the stream — it drives no continuation; that
        // derives from the ledger via the frontier gate — and the turn
        // closes on the closed signal, which the reader emits after the tool
        // round drains.
        self.stream_stop = stop_reason;
        tracing::info!(conversation_id = self.id, ?stop_reason, "stream done");
    }

    async fn handle_stream_error(&mut self) {
        self.close_dispatch(CloseEdge::Errored).await;
        tracing::info!(conversation_id = self.id, "stream error, latched");
    }

    async fn handle_stream_closed(&mut self) {
        self.close_dispatch(CloseEdge::Closed).await;
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
    ///
    /// Every close settles the STREAM; only a close that ends the TURN
    /// releases the held identity (amended 2026-08-22). A closed signal
    /// whose stream stopped for tool use leaves the turn open while a
    /// continuation is genuinely due — its rounds reuse the held anchor —
    /// while the end-turn stop, any other or absent stop, the error edge,
    /// the teardown and the abandoned mark all end it:
    ///
    /// - A tool-use close keeps the identity ONLY while
    ///   [`Self::turn_continuation_due`] holds on the close's own snapshot
    ///   (refined 2026-08-22, the verified fifth break; amended 2026-08-23,
    ///   the sixth): this close is the identity's one release site, so a
    ///   tool-use stop whose continuation never comes — a truncated tool
    ///   lifecycle the reader discards records nothing owing — held the
    ///   identity forever, and the next UNRELATED summons inherited it:
    ///   over-declined tool admission on the consumer's side, and an idle
    ///   interrupt stamping a dead turn's summoner. The sixth break proved
    ///   the fifth's two arms leak in both directions — a model-owed tail
    ///   kept a dead identity for someone else's summons, and a parked
    ///   interactive or empty-id call kept one forever — so the rule now
    ///   keeps only for an unanswered outcome or a system-owed call, both
    ///   documented at the rule. Rejected: reusing the held anchor only for
    ///   turn-product frontiers — the absorbed message is exactly a
    ///   non-turn-product frontier, so that re-opens the proven escalation
    ///   the hold exists to close.
    /// - The out-of-band store-failure close ends the identity by decision
    ///   (2026-08-22): it rides the error edge, and the error edge LATCHES —
    ///   whatever the owner repairs and unlatches, the turn that resumes is
    ///   a new decision resolved from the tail, and an identity held across
    ///   the latch would stamp that post-repair turn with a broken turn's
    ///   summons. Rejected: ending only reader-stamped errors — the two
    ///   producers share the edge precisely because they share its meaning.
    /// - The abandoned close ends it even under a tool-use stop: the
    ///   stalled provider's continuation died with the retired binding, so
    ///   an identity kept here would leak onto the next summons' turn.
    ///
    /// A close that ENDS the identity while the close's snapshot holds an
    /// unanswered outcome for that turn also WRITES the turn's end down —
    /// a status block anchored on the turn (2026-08-23, the design review's
    /// verdict after the eighth break: turn closure is a stored fact). The
    /// marker append completes before the deferred owed-turn re-check below
    /// consumes its nudge, so a summons the close's re-check sets in motion
    /// reads a snapshot that already carries the marker — it can never
    /// outrun it. The rule itself lives in [`Self::settle_turn_identity`].
    async fn close_dispatch(&mut self, edge: CloseEdge) {
        self.streaming = false;
        let abandoned = self.turn_anchor.is_abandoned();
        if abandoned {
            tracing::info!(
                conversation_id = self.id,
                "the reader abandoned this turn at its drain deadline — \
                 retiring the binding"
            );
            self.provider_tx = None;
        }
        self.turn_anchor.clear();
        let continuation_stop = edge == CloseEdge::Closed
            && !abandoned
            && self.stream_stop == Some(StopReason::ToolUse);
        self.stream_stop = None;
        if let Some(anchor) = self.open_turn {
            self.settle_turn_identity(anchor, edge, continuation_stop)
                .await;
        }
        if edge == CloseEdge::Errored {
            self.write_latched.set(true);
        }
        if std::mem::take(&mut self.owed_turn_deferred) {
            self.recheck.update(|nudges| *nudges += 1);
        }
    }

    /// Whether the open turn held at `anchor` is genuinely owed a
    /// continuation, decided on `blocks` — the close's own snapshot, read
    /// once by [`Self::settle_turn_identity`] and shared with the marker
    /// decision there. The release rule (2026-08-23, the verified sixth
    /// break, replacing the fifth break's two arms): a tool-use close KEEPS
    /// the identity iff
    ///
    /// - **an outcome is unanswered** — the count of anchored tool outcomes
    ///   that ask for a continuation exceeds the count its dispatches have
    ///   already answered ([`Self::answered_outcomes`]): such an outcome
    ///   summons exactly one continuation, and one above the mark has not
    ///   had its dispatch yet. An ends-turn-stamped result asks for none and
    ///   is excluded by the fold itself (2026-08-30), so an ends-turn round
    ///   leaves the two sides equal and the turn ends; or
    /// - **the system owes an outcome** — an unresolved, NON-interactive
    ///   tool call with a non-empty call id, anchored on this turn, exists
    ///   in the snapshot ([`ToolCall::system_owed_call_anchored_in`]): the
    ///   runner will answer it, and that outcome resumes the turn.
    ///
    /// The fifth break's frontier arm — "the tail owes the model keeps the
    /// identity" — is DELETED: a model-owed tail is someone's summons,
    /// never evidence of this turn's continuation, and it kept a dead
    /// turn's identity for exactly the fresh summons the rule exists to
    /// protect. The unresolved-call arm is narrowed for the same break's
    /// other shape: an interactive call parks on the user and an empty-id
    /// call can never resolve, so either one read as owed pinned the
    /// identity indefinitely. How each proven shape falls out:
    ///
    /// - **The truncated round**: no call recorded, no outcome — both arms
    ///   empty, the close ends the turn, and a message absorbed in the
    ///   round's window summons a turn of its own.
    /// - **The parked interactive call**: excluded from the owed arm, no
    ///   outcome — the close ends the turn. When the approval later
    ///   resolves, the outcome is written with the call's own anchor, the
    ///   tail owes the model, and the dispatch off that tail inherits the
    ///   identity — the turn re-attaches without ever having been held.
    /// - **The close before the result**: the recorded call is unresolved
    ///   and the runner owes it — the owed arm keeps the identity, so a
    ///   message absorbed before the result cannot re-anchor the turn.
    /// - **The result recorded in the stream's own window, with a message
    ///   absorbed behind it**: the call is resolved, but the outcome is
    ///   above the mark — the counting arm keeps the identity, and the
    ///   continuation the close's re-check dispatches still anchors on the
    ///   original summons.
    /// - **The multi-round conversation**: each round's close keeps through
    ///   whichever arm its snapshot shows — call still owed, or outcome not
    ///   yet answered — and each continuation dispatch re-marks the
    ///   answered count from its own snapshot, so the final prose round,
    ///   with every outcome answered and no call owed, ends the turn.
    /// - **The ends-turn round** (2026-08-30): the resolution carries the
    ///   ends-turn stamp, so it never enters the count and no call stays
    ///   owed — both arms empty, and the turn ends on its own stored
    ///   resolution with no marker written beside it. That row is the
    ///   turn's stored end, so the frontier reads THROUGH it exactly as it
    ///   reads through a marker: a message absorbed into the round's window
    ///   still owes behind it and summons its own turn at this close's
    ///   re-check.
    ///
    /// TWO sites ask this rule, and they are the same rule on different
    /// snapshots (2026-08-30): the close asks it to decide whether the
    /// identity survives the stream, and the summons-time reuse in
    /// [`Self::handle_blocks_ready`] asks it to decide whether a hold still
    /// stands before an anchor is resolved from it. One rule, because an
    /// ends-turn tail sends no signal the close can act on, and a second rule
    /// for that case would be a second answer to one question.
    fn turn_continuation_due(&self, blocks: &[Block], anchor: i64) -> bool {
        ToolCall::outcomes_anchored_in(blocks, anchor) > self.answered_outcomes
            || ToolCall::system_owed_call_anchored_in(blocks, anchor)
    }

    /// Settle the held identity at a close, on ONE snapshot — the close's
    /// own: decide whether the turn survives the stream
    /// ([`Self::turn_continuation_due`]), and when it ends with its last
    /// outcome unanswered, write the turn's end DOWN before returning.
    ///
    /// The marker (2026-08-23, the design review's verdict — turn closure
    /// is a stored fact): eight adversarial rounds broke every attempt to
    /// derive "this turn is over" from side effects, each on an edge that
    /// left no side effect behind — the eighth break's four shapes (a
    /// parked interactive call in a later round, a lost later round, the
    /// abandoned close after a result, the error edge after a result) all
    /// end a turn without writing a text or status, stranding the turn's
    /// last outcome as unanswered forever, so it captured the next
    /// unrelated summons. The one place that knows the truth now records
    /// it: when this close ends the identity and
    /// [`ToolCall::unanswered_outcome_anchor`] — the fresh-dispatch walk's
    /// own predicate — still answers this turn on the close's snapshot, the
    /// close appends a status block anchored on the turn, through the same
    /// status write path every other turn record uses. The machine key
    /// names the turn's end and the closing edge. No resolution rule
    /// changes: the walk already honors status markers, so the marker is
    /// read wherever the walk already reads.
    ///
    /// Ordering, the property [`Self::close_dispatch`] states: the append
    /// is awaited here, inside the close, before the close consumes the
    /// deferred owed-turn nudge — and this actor task is the only
    /// dispatcher — so every dispatch that follows this close resolves over
    /// a snapshot that carries the marker. The marker is TRANSPARENT to the
    /// frontier decision (2026-08-23, the verified burial defect — the
    /// owed-turn read skips the ended turn's whole trailing closure run,
    /// [`ratchet::frontier_block`]): a message absorbed into the ended
    /// turn's window still owes behind the marker, and the close's re-check
    /// dispatches its turn — anchored on itself, because this same marker
    /// answers the dead turn's outcome for the inheritance walk. Capping it
    /// instead buried the message forever: the closed edge never latches,
    /// so no re-engagement exists past the close's one re-check. With
    /// nothing owed behind it the marker still rests the frontier — the
    /// dead turn's own outcome, which the marker answers, never summons
    /// through it — and the interrupt's status keeps its cap under the
    /// latch, whose release re-checks there.
    ///
    /// The teardown edge writes no marker here: the interrupt appends its
    /// own status — anchored on the same held identity, read before this
    /// close ends it — as the turn's durable record, and it does so under
    /// the latch, before any dispatch can run. A second marker would record
    /// one end twice.
    ///
    /// The approval-resume stays correct by ordering: the marker answers
    /// only the outcomes before it, and a later approval's outcome lands
    /// after it and inherits as before. A restart mid-round writes no
    /// marker because the tool-use close keeps the identity, so recovery
    /// inheritance is untouched. One residual, over-decline like the rest:
    /// an outcome committed between this snapshot and the marker's append
    /// reads as answered by the marker; the turn then rests until the next
    /// summons instead of resuming.
    ///
    /// An unreadable snapshot KEEPS the identity under a continuation stop,
    /// by decision (2026-08-22): a wrong keep costs one over-declined turn
    /// and heals at that turn's own close, while a wrong end re-opens the
    /// proven escalation for a message-tail continuation. Rejected: ending
    /// on the failed read — it trades a bounded misattribution for an
    /// unbounded authority defect. On an edge that ends the turn
    /// regardless, the failed read ends it WITHOUT a marker — the accepted
    /// unstamped residual documented on the walk.
    async fn settle_turn_identity(
        &mut self,
        anchor: i64,
        edge: CloseEdge,
        continuation_stop: bool,
    ) {
        let blocks = match self.ctx.store.list_blocks(self.id).await {
            Ok(blocks) => blocks,
            Err(e) if continuation_stop => {
                tracing::error!(
                    conversation_id = self.id,
                    error = %e,
                    "settle_turn_identity: list_blocks failed — keeping the held identity"
                );
                return;
            }
            Err(e) => {
                tracing::error!(
                    conversation_id = self.id,
                    error = %e,
                    "settle_turn_identity: list_blocks failed — the turn ends unmarked"
                );
                self.open_turn = None;
                return;
            }
        };
        if continuation_stop && self.turn_continuation_due(&blocks, anchor) {
            return;
        }
        self.open_turn = None;
        let Some(key) = edge.turn_end_key() else {
            return;
        };
        if ToolCall::unanswered_outcome_anchor(&blocks) != Some(anchor) {
            return;
        }
        if let Err(e) = self
            .ctx
            .store
            .insert_status_block(
                crate::store::BlockDestination::anchored(self.id, Some(anchor)),
                key.into(),
                None,
            )
            .await
        {
            tracing::error!(
                conversation_id = self.id,
                anchor,
                error = %e,
                "settle_turn_identity: the turn-end marker append failed — \
                 the turn ends unmarked"
            );
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
        let blocks = match self.ctx.store.list_blocks(self.id).await {
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
        // ledger and dies here instead of dispatching.
        // The tail is read through a dead turn's trailing closure run
        // ([`ratchet::frontier_block`], 2026-08-23, the verified burial
        // defect): the row that stored the turn's end — a turn-end marker,
        // or (2026-08-30) an ends-turn-stamped resolution — and the trailing
        // blocks it answers are transparent here, so an addressed message
        // absorbed anywhere in the dead turn's window — the tool-execution
        // stretch between a call and its result included — still owes behind
        // them and the close's re-check dispatches its turn instead of
        // resting on the closure row forever.
        let Some(tail) = ratchet::frontier_block::<K>(&blocks) else {
            tracing::warn!(
                conversation_id = self.id,
                "handle_blocks_ready: list_blocks returned empty"
            );
            return;
        };
        if !ratchet::frontier_owes_turn::<K>(tail) {
            tracing::debug!(
                conversation_id = self.id,
                tail_block = tail.id,
                "handle_blocks_ready: the tail no longer owes a turn — standing down"
            );
            return;
        }
        // The prompt is the head or the turn is refused, on the same
        // snapshot the request would be built from.
        if !Self::does_the_ledger_open_with_its_head_kind(&blocks) {
            self.refuse_the_turn_over_a_headless_ledger();
            return;
        }

        // The turn's identity (amended 2026-08-22; ledger-first 2026-08-23,
        // the verified seventh break): a dispatch while a turn is open is
        // that turn's continuation and reuses the held anchor whatever the
        // tail is — a message absorbed between a round's result and this
        // dispatch is the tail, and resolving from it re-anchored the turn
        // onto the absorbed message, the consumer's proven escalation. The
        // held identity is a CONSISTENCY CACHE over a ledger-derivable fact
        // (demoted 2026-08-23): a fresh dispatch resolves the same answer
        // from the snapshot itself, through [`ratchet::fresh_turn_anchor`] —
        // the tail's own anchor inherited, a null-anchored tail inheriting
        // the newest UNANSWERED outcome's anchor, a new identity
        // otherwise — which is what keeps a released turn (a parked
        // approval resuming, a restart recovering) anchored on its original
        // summons even when a message was absorbed behind its outcome.
        //
        // The reuse is REVALIDATED against the ledger first (2026-08-30, the
        // ends-turn stamp): a turn ended by a stamped resolution owes no
        // signal — its tail asks for nothing, so the close's release site is
        // never reached and no delivery ever stands down for it. Rather than
        // add a signal whose only job is to refresh this cache, the one site
        // that resolves an anchor FROM the cache asks the release rule again,
        // here, on the snapshot the request is built from: a hold the rule
        // still supports (an unanswered outcome, a call the system owes)
        // survives exactly as before, and a hold it no longer supports is
        // dropped so this summons takes a fresh anchor. Restart-equivalent by
        // construction — a rebooted actor holds nothing and answers from the
        // same rows. The known residual: a summons arriving while the call is
        // still UNRESOLVED attaches to the held turn, as any outstanding
        // system-owed call makes it, and heals the moment the resolution
        // lands.
        let anchor = self
            .open_turn
            .filter(|held| self.turn_continuation_due(&blocks, *held))
            .unwrap_or_else(|| ratchet::fresh_turn_anchor(&blocks, tail));

        // The forced end (2026-08-30), decided BEFORE the dispatch spends: a
        // turn whose trailing tool outcomes are all tool-call window
        // refusals is a turn looping on a spent window, and every further
        // round buys a paid request to be refused again.
        if self.end_turn_if_tool_calls_exhausted(&blocks, anchor).await {
            return;
        }

        let conv = match self.ctx.store.find_conversation(self.id).await {
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

        // What the turn is offered is the CONVERSATION's own recorded answer
        // (2026-09-01): the tools its newest tool choice names, intersected
        // with what this process registered. A ledger carrying no choice is
        // offered the registry, unchanged from before the record existed; an
        // empty choice is offered nothing. The runner resolves a call through
        // the very same function on its own snapshot, so what the model is
        // shown and what a call resolves against can never disagree — and
        // this site still names no kind and branches on no type string.
        let tool_defs = ResolvedTools::of(&blocks, self.ctx.runner.registry()).definitions();
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
            // No stream opened: the seam a failed send left set would leak
            // this identity onto a later binding's first inserts. The HELD
            // identity is untouched on purpose — an open turn stays open
            // across a failed continuation send and the retried dispatch is
            // still its round, while a fresh turn was never opened here.
            self.turn_anchor.clear();
            return;
        }
        // Set AFTER the dispatch, which is safe HERE and only here because
        // this actor task is the sole writer of `streaming`: the stream-end
        // handlers that clear it run on this same task, so no clear can slip
        // in between the send above and this set. The metadata fulfillment
        // loop has the same shape across TWO tasks and must set its flag
        // before dispatching. The dispatched stream opens with no recorded
        // stop, and the turn's identity is held from its first dispatch —
        // a continuation re-holds the same anchor. Every dispatch also
        // answers the outcomes its own snapshot carries that ASK for a
        // continuation (2026-08-23): the mark is what the release rule
        // measures new outcomes against, and recording it from the request's
        // snapshot is what makes a resumed turn's already-carried outcome
        // count as answered. Mark and rule read the ONE fold, so an
        // ends-turn-stamped result — which rides the request but asks for
        // nothing — falls out of both sides at once (2026-08-30).
        self.streaming = true;
        self.stream_stop = None;
        self.answered_outcomes = ToolCall::outcomes_anchored_in(&blocks, anchor);
        self.open_turn = Some(anchor);

        tracing::info!(conversation_id = self.id, "sent stream request to provider");
    }

    /// Does this ledger open with a block whose kind says a conversation
    /// opens with it (2026-09-02)?
    ///
    /// The first row is read through the kind's own answer
    /// ([`Agency::heads_the_ledger`](crate::agency::Agency::heads_the_ledger)),
    /// so this site reads one hook and never learns which kind answered it.
    /// The store holds the same rule at the write — see
    /// [`Store::insert_system_prompt`] for the whole of it — and this is its
    /// second reading, at the one place a wrong shape costs a paid turn. An
    /// empty ledger answers `false`, like any other ledger with no head.
    fn does_the_ledger_open_with_its_head_kind(blocks: &[Block]) -> bool {
        blocks
            .first()
            .is_some_and(|head| K::from_block(head).heads_the_ledger())
    }

    /// Refuse the turn a ledger that does not open with its prompt asks for.
    ///
    /// Such a ledger either holds its instructions where the model is never
    /// told to read them first, or holds none at all, and either way the
    /// request would run a conversation that is quietly not the conversation
    /// it is meant to be — a degraded answer nobody can trace back, the
    /// failure that must never be papered over. So the provider hears nothing:
    /// the refusal is a `StreamError` of this actor's own, unstamped like
    /// every store failure it emits, and the dispatch closes through the error
    /// edge exactly as a failed stream does, latching the conversation instead
    /// of retrying a request that fails identically every time. Repairing a
    /// ledger in that shape is the consumer's, and it repairs it the one way a
    /// ledger changes shape: a fresh conversation whose prompt is its first
    /// block, with the history cloned behind it.
    fn refuse_the_turn_over_a_headless_ledger(&self) {
        tracing::error!(
            conversation_id = self.id,
            "handle_blocks_ready: the ledger's first block is no system prompt — \
             refusing the turn"
        );
        self.ctx.bus.emit(CoreEvent::StreamError {
            conversation_id: self.id,
            error: "the ledger does not open with a system prompt, so no turn is \
                    dispatched for it"
                .to_owned(),
            generation: None,
        });
    }

    /// End the turn that is looping on a spent tool-call window — the
    /// conversation's or a single tool's — if this one is (2026-08-30) — a
    /// WRITE, not a question: the end it decides on is
    /// performed here, in the same call, and `true` is the caller's
    /// instruction to stand its dispatch down.
    ///
    /// Both edges answer `true`, because both mean "do not spend this
    /// dispatch": the marker landed and the turn is over, or the append
    /// FAILED and the turn stays open for the next drive to retry off the
    /// durable fold. Only a run shorter than the limit answers `false`, and
    /// that path writes nothing.
    ///
    /// The rule reads the LEDGER, on the dispatch's own snapshot: when the
    /// turn's trailing tool outcomes are all REFUSALS — every one of them,
    /// whoever refused, each carrying that fact on its own row
    /// ([`Refusal`](crate::agency::Refusal), whose reading is the one home for
    /// what counts) — as many of them as the ONE consecutive limit the runtime
    /// carries ([`ToolCall::trailing_refusal_run`]), the model has stopped making
    /// progress — a history that deep in refusals has gone bad — and every
    /// further round costs a paid request for another refusal. The turn the
    /// rule scopes to is the RESOLVED anchor the caller just decided, held
    /// identity or ledger-derived alike, so a restart that cleared the held
    /// field while the ledger still owes the dead turn's continuation reaches
    /// the same verdict.
    ///
    /// The end, in order: the status block anchored on the turn joins the
    /// ledger, and ONLY after that append succeeds is the held identity
    /// released — a named release edge beside
    /// [`Self::settle_turn_identity`]'s. If the append fails nothing is
    /// released and nothing is dispatched; the next drive re-enters this
    /// check off the durable fold and retries, so there is no retry queue and
    /// no latch. In a quiet system that next drive is the next store change
    /// or member message, and that wait is the stated residual — no dispatch
    /// is spent meanwhile.
    ///
    /// The conversation is NOT latched: this is a decision, not an error, and
    /// the error edge's latch would take the whole conversation down for it.
    /// The marker's key joins the turn-closure family
    /// ([`Status::records_turn_end`](crate::agency::Status)), so the frontier
    /// reads THROUGH it: a member's message landing in the gap between this
    /// check and its append still owes behind the marker and summons its own
    /// turn, which is exactly the burial the tree's own recorded defect
    /// produced from an opaque end marker. With nothing owed behind it the
    /// loop still rests, because the summons bound disowns the ended turn.
    ///
    /// One residual is inherited from the marker's semantics, the same one
    /// the close records for itself: an outcome that commits between this
    /// snapshot and the append reads as answered by the marker and never gets
    /// its continuation. The sibling edge is no cleaner than its predecessor,
    /// said openly.
    async fn end_turn_if_tool_calls_exhausted(&mut self, blocks: &[Block], anchor: i64) -> bool {
        let limit = self.ctx.runner().window().consecutive_limit;
        if ToolCall::trailing_refusal_run(blocks, anchor) < limit {
            return false;
        }
        match self
            .ctx
            .store
            .insert_status_block(
                crate::store::BlockDestination::anchored(self.id, Some(anchor)),
                crate::agency::Status::TOOL_CALLS_EXHAUSTED.into(),
                None,
            )
            .await
        {
            Ok(_) => {
                self.open_turn = None;
                tracing::info!(
                    conversation_id = self.id,
                    anchor,
                    refusals = limit,
                    "a run of refused tool calls ended this turn — standing the \
                     dispatch down"
                );
            }
            Err(e) => {
                tracing::error!(
                    conversation_id = self.id,
                    anchor,
                    error = %e,
                    "the forced end's status append failed — the turn stays open and \
                     the next drive retries"
                );
            }
        }
        true
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
        // stamp a successor turn's anchor on its late writes. The held turn
        // identity is deliberately NOT reset here: it is turn state, not
        // binding state, and a continuation whose channel died mid-turn
        // rebinds fresh while remaining the same turn.
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

/// One scheduler tick: read whether the conversation is still there and then
/// rest while latched (the whole ratchet family runs only unlatched),
/// otherwise drive the cursor, signal the actor when the frontier
/// gate owes a turn, and run the redispatch walk — which rides the same tick
/// but is ungated by the model-turn axis, so deferred work resumes even when
/// no turn is owed. Returns the drive's Outcome for the state broadcaster to
/// consume; `None` when nothing was driven.
///
/// A tick that reads its conversation GONE (2026-09-01) signals the actor and
/// returns at once, redispatch walk included: there is no ledger left to walk,
/// and this tick is the last one the set runs. That read happens LATCHED TOO —
/// the latch decides whether the ratchet runs, never whether the set may
/// outlive its conversation, and a set latched at boot or by a stream failure
/// would otherwise never meet the one fact that ends it.
pub(crate) async fn scheduler_tick<K: RuntimeKind, E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    latched: bool,
    signals: &tokio::sync::mpsc::UnboundedSender<SchedulerSignal>,
) -> Option<ratchet::Outcome> {
    if latched {
        match ratchet::conversation_gone(ctx).await {
            Ok(true) => signal_conversation_gone(ctx, signals),
            Ok(false) => {}
            Err(e) => {
                tracing::error!(conversation_id = ctx.conversation_id, error = %e, "scheduler: reading whether the conversation still exists failed");
            }
        }
        return None;
    }
    let outcome = match ratchet::drive::<K, E>(ctx).await {
        Ok(ratchet::Driven::Ran(outcome)) => {
            tracing::debug!(
                conversation_id = ctx.conversation_id,
                ?outcome,
                "scheduler tick"
            );
            if outcome.owes_turn {
                let _ = signals.send(SchedulerSignal::OwesTurn);
            }
            Some(outcome)
        }
        Ok(ratchet::Driven::ConversationGone) => {
            signal_conversation_gone(ctx, signals);
            return None;
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

/// Tell the actor its conversation is gone — the one place the scheduler says
/// it, for the two reads that can find it: the drive's own cursor read and the
/// latched tick's existence read.
fn signal_conversation_gone<E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    signals: &tokio::sync::mpsc::UnboundedSender<SchedulerSignal>,
) {
    tracing::info!(
        conversation_id = ctx.conversation_id,
        "scheduler: the conversation is gone — ending the actor set"
    );
    let _ = signals.send(SchedulerSignal::ConversationGone);
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
    signals: tokio::sync::mpsc::UnboundedSender<SchedulerSignal>,
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

        if let Some(outcome) = scheduler_tick::<K, E>(&agency, latched, &signals).await {
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

        let (signals_tx, signals_rx) = tokio::sync::mpsc::unbounded_channel();
        let (read_outcome, write_outcome) = create_signal(None::<ratchet::Outcome>);
        // The close edges' re-check nudge, actor-written and scheduler-read.
        let (read_recheck, write_recheck) = create_signal(0u64);

        let scheduler = tokio::spawn(run_scheduler(
            id,
            ctx.clone(),
            read_latched.clone(),
            read_recheck,
            signals_tx,
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
            from_scheduler: signals_rx,
            recheck: write_recheck,
            provider_tx: None,
            streaming: false,
            owed_turn_deferred: false,
            stream_generation: 0,
            turn_anchor: TurnAnchor::new(),
            open_turn: None,
            answered_outcomes: 0,
            stream_stop: None,
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
/// provides the rowid, and the conversation each event names is read from the
/// row that rowid names.
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
            if !(change.table == "blocks"
                || change.table == "conversation_blocks"
                || store.content_tables().iter().any(|t| *t == change.table))
            {
                continue;
            }

            // Which conversations a change concerns is read from the ROW THE
            // CHANGE NAMES (2026-09-01), and the two shapes of row answer
            // differently — which is why the branch is here and not behind a
            // shared lookup.
            let announcements: Vec<(i64, i64)> = if change.table == "conversation_blocks" {
                // A junction row carries both facts itself. Reading the block
                // from it and then asking which conversation holds that block
                // was the mis-attribution: a block shared with a fork answered
                // for whichever join the index reached first, so a fork's own
                // copies were announced to the conversation they were copied
                // FROM.
                match store.joined_block(change.rowid).await {
                    Ok(Some(joined)) => {
                        vec![(joined.conversation_id, joined.block_id)]
                    }
                    // Gone by read time — a delete, or an insert whose
                    // transaction rolled back after the hook already fired.
                    // It attributes nothing and announces nothing.
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(change_rowid = change.rowid, error = %e, "block watcher: reading the junction row failed");
                        continue;
                    }
                }
            } else {
                // The header's rowid is the block id, and a content table's
                // block_id is its primary key (= rowid). This row names no
                // conversation, so the block's joins are looked up — and EVERY
                // one of them is told, because a change to a block is a change
                // for each conversation that reads it.
                let block_id = change.rowid;
                match store.conversations_for_block(block_id).await {
                    Ok(conversations) => conversations
                        .into_iter()
                        .map(|conversation_id| (conversation_id, block_id))
                        .collect(),
                    Err(e) => {
                        tracing::warn!(block_id, change_table = %change.table, change_rowid = change.rowid, error = %e, "block watcher: reading a block's conversations failed");
                        continue;
                    }
                }
            };

            if announcements.is_empty() {
                // A block joined to nothing is a normal transient, not a
                // fault: the header and content rows land before the junction
                // row inside one flow, so their change events can arrive
                // first, and draft blocks live unjoined by design. The
                // junction row's own event carries the notification, so
                // nothing is missed.
                tracing::debug!(block_id = change.rowid, change_table = %change.table, "block watcher: block not joined to a conversation yet");
            }
            for (conversation_id, block_id) in announcements {
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
    // An ended set is forgotten before anything is routed (2026-09-01). A set
    // ends itself when its conversation ceases to exist, and its mailbox
    // closing IS that end — nothing here is told about it. Forgetting matters
    // beyond tidiness: the database reuses a deleted conversation's id, and a
    // fresh conversation that inherits one would otherwise route into the dead
    // set standing in its place.
    routes.retain(|_, actors| !actors.iter().any(|actor| actor.tx.is_closed()));

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
    use crate::agency::results_with_stamps;
    use crate::block::Block;
    use crate::providers::types::ToolDefinition;
    use crate::providers::{
        BoxFuture, ContentPart, LlmError, Message, MessageContent, ModelInfo, ProviderModule,
        ProviderResponse, ProviderRx, StreamEvent,
    };
    use crate::store::{ProviderInstance, StoreError, ToolCallInsert};
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
        actor.handle_stream_closed().await;
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
        actor.handle_stream_closed().await;
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
            actor.from_scheduler.try_recv().is_err(),
            "the actor never produces a scheduler signal of its own"
        );

        // A close with nothing swallowed rests: no nudge, no signal — the
        // empty turn's unchanged frontier is not re-asked forever.
        actor.streaming = true;
        actor.handle_stream_closed().await;
        assert_eq!(
            recheck.get(),
            1,
            "an undisturbed close nudges nothing — the empty turn rests"
        );

        // The error edge: released AND latched.
        actor.streaming = true;
        actor.handle_stream_error().await;
        assert!(!actor.streaming);
        assert!(actor.read_latched.get(), "only the error edge latches");
    }

    /// The turn identity's own close rule (amended 2026-08-22): every close
    /// settles the stream, but only a close that ends the TURN releases the
    /// held anchor. A tool-use stop leaves it held while a continuation is
    /// genuinely due — the continuation rounds are the same turn. The
    /// end-turn stop, a close with no recorded stop, the error edge (which
    /// the out-of-band store-failure close rides, and which ends the
    /// identity by decision: it latches, and the turn a repair resumes is a
    /// new one resolved from the tail), the reader's abandoned mark and the
    /// interrupt teardown all end it.
    #[tokio::test]
    async fn the_turn_identity_survives_only_a_tool_use_close() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        // The turn under close is genuinely owed a continuation: its
        // recorded call is still unresolved in the close's snapshot.
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        ctx.store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-open".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx, false);

        // The tool-use stop: the stream closes, the turn stays open.
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert!(!actor.streaming, "the stream itself closes");
        assert_eq!(
            actor.open_turn,
            Some(summons),
            "a tool-use stop leaves the turn's identity open"
        );

        // The stop is consumed per close: a later close of a stream that
        // never reached its message-end cannot ride the previous stream's
        // tool-use stop, and a stop-less close ends the turn — even while
        // the snapshot still owes the continuation.
        actor.streaming = true;
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "a close with no recorded stop ends the turn"
        );

        // The end-turn stop ends it.
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::EndTurn));
        actor.handle_stream_closed().await;
        assert_eq!(actor.open_turn, None, "the end-turn stop ends the turn");

        // The error edge ends it even under a recorded tool-use stop with
        // the continuation still owed. The actor's own store-failure paths
        // emit this same edge unstamped, so this line is also the
        // out-of-band close's pin.
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_error().await;
        assert_eq!(actor.open_turn, None, "the error edge ends the turn");
        actor.write_latched.set(false);

        // The abandoned mark ends it even under a tool-use stop with the
        // continuation still owed: the stalled provider's continuation died
        // with the retired binding, so a held identity would leak onto the
        // next summons' turn.
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.turn_anchor.set(summons);
        actor.turn_anchor.mark_abandoned(actor.turn_anchor.epoch());
        actor.handle_stream_closed().await;
        assert_eq!(actor.open_turn, None, "the abandoned close ends the turn");

        // The interrupt teardown ends it — pinned against a fresh actor,
        // because the abandoned mark above is a binding-liveness fact and
        // deliberately survives every later close on that seam.
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        // A REAL summons: the teardown's status is anchored on the open
        // turn, and a block id nothing wrote is a broken reference the store
        // now refuses outright (2026-09-01).
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx, false);
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_interrupt().await;
        assert_eq!(actor.open_turn, None, "the teardown ends the turn");
    }

    /// The identity-release rule's END side (2026-08-22, the verified fifth
    /// break): a tool-use close keeps the turn's identity iff a
    /// continuation is genuinely due in the close's snapshot — an
    /// unresolved call bearing THIS turn's anchor, or a frontier owing the
    /// model. A lost tool round — the stop said tool use but the truncated
    /// lifecycle recorded no call — owes nothing, and before the rule its
    /// close held the identity forever: the one release site never ran
    /// again, and the next unrelated summons inherited a dead turn's
    /// summoner.
    #[tokio::test]
    async fn a_tool_use_close_ends_a_turn_owed_no_continuation() {
        // The lost round: the ledger holds the summons and the turn's prose,
        // no call, nothing owing. The close ends the turn.
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        ctx.store
            .insert_text_block(conv, crate::block::Role::Assistant, "hi".into())
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "a tool-use close with nothing owing ends the turn"
        );

        // Another turn's dangling call keeps nothing: the unresolved call
        // must bear THIS turn's anchor, not merely exist.
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let other = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "other".into())
            .await
            .unwrap();
        ctx.store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(other)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-other".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.streaming = true;
        actor.open_turn = Some(other + 1000);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "another turn's unresolved call keeps nothing"
        );
    }

    /// The verified sixth break's shape A at the rule level (2026-08-23),
    /// the unit twin of the end-to-end pin: a lost tool round with a
    /// message absorbed in its window. The truncated lifecycle recorded
    /// nothing owing, so the turn is over — but the absorbed message is
    /// the tail and owes the model, and the deleted frontier arm kept the
    /// dead identity on exactly that: the absorbed message's OWN turn then
    /// inherited it. A model-owed tail is someone's summons, never this
    /// turn's continuation — the close ends the turn, and the dispatch the
    /// close's re-check fires is the absorbed message's own, anchored on
    /// itself.
    #[tokio::test]
    async fn a_lost_rounds_close_ends_the_turn_even_with_a_model_owed_tail() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        ctx.store
            .insert_text_block(conv, crate::block::Role::Assistant, "hi".into())
            .await
            .unwrap();
        let absorbed = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "absorbed".into())
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "a lost round's close ends the turn even though the tail owes the model"
        );
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(absorbed),
            "the absorbed message's turn anchors on itself, never on the dead summons"
        );
        assert_eq!(actor.open_turn, Some(absorbed));
    }

    /// The verified sixth break's shape B at the rule level (2026-08-23):
    /// a call the SYSTEM never owes an outcome for must not hold the
    /// identity. An INTERACTIVE call parks on the user, who may never
    /// answer — counting it as an owed continuation pinned the identity
    /// indefinitely, and someone else's fresh summons inherited it. The
    /// close ends the turn; when the approval later resolves, the outcome
    /// carries the turn's anchor and the tail inheritance re-attaches. An
    /// empty-id call is the same shape one step further: no outcome can
    /// ever match it, so it reads as unresolved forever.
    #[tokio::test]
    async fn a_parked_interactive_call_does_not_hold_the_identity() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        ctx.store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-interactive".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: true,
                },
                None,
            )
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "a parked interactive call ends the identity"
        );

        // The user never answers the approval; someone summons a NEW turn.
        // Its dispatch anchors on itself, never on the parked turn.
        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "the fresh summons' turn never inherits the parked turn's identity"
        );

        // The empty-id call: nothing can ever resolve it, so reading it as
        // an owed continuation pins the identity forever. It ends the turn
        // the same way.
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        ctx.store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: String::new(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "a call no outcome can ever match ends the identity"
        );
    }

    /// The identity-release rule's KEEP side (2026-08-22; amended
    /// 2026-08-23, the sixth break), the [`Script::HoldFirstRoundsDone`]
    /// pin family at the rule level, walking both keep arms and the
    /// release: a call whose result is pending keeps the identity across
    /// the close (the owed arm), an outcome above the answered mark keeps
    /// it for its one continuation (the counting arm) — so a message
    /// absorbed after the result cannot re-anchor the continuation — and
    /// once that continuation has answered the outcome, a tool-use close
    /// ends the turn even over a model-owed tail.
    #[tokio::test]
    async fn a_tool_use_close_keeps_the_turn_only_while_a_continuation_is_due() {
        // The pending result: the turn's own call is unresolved — and the
        // tail is that call, which owes nobody a model turn, so this
        // isolates the unresolved-call arm. The close keeps the identity.
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let call = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-1".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn,
            Some(summons),
            "an unresolved call bearing the turn's anchor keeps the identity"
        );

        // The counting arm (2026-08-23, the sixth break's rule): the result
        // is recorded and a message is absorbed behind it. The call is
        // resolved, so the owed arm is silent — but the outcome is above
        // the answered mark, and the close keeps the identity for the one
        // continuation that outcome still summons.
        ctx.store
            .complete_tool_call_block(conv, "call-1".into(), "ok".into(), call)
            .await
            .unwrap()
            .expect("the call is unresolved");
        ctx.store
            .insert_text_block(conv, crate::block::Role::User, "absorbed".into())
            .await
            .unwrap();
        actor.streaming = true;
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn,
            Some(summons),
            "an outcome above the answered mark keeps the identity"
        );

        // The kept case's point (the HoldFirstRoundsDone family, at the
        // rule level): the continuation's dispatching tail is the absorbed
        // message, and the continuation still anchors on the held summons,
        // never on the absorbed message.
        actor.handle_blocks_ready().await;
        assert!(actor.streaming, "the continuation dispatched");
        assert_eq!(
            actor.open_turn,
            Some(summons),
            "the continuation is the held turn's round"
        );
        assert_eq!(
            actor.turn_anchor.get(),
            Some(summons),
            "the absorbed message never re-anchors the continuation"
        );

        // The continuation answered the outcome — its dispatch re-marked
        // the answered count from its own snapshot — so a tool-use close
        // that recorded nothing more ends the turn, even though the tail
        // still owes the model. The frontier arm is deleted (2026-08-23,
        // the verified sixth break): a model-owed tail is someone's
        // summons, never evidence of this turn's continuation.
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "with every outcome answered and no call owed, the close ends the turn"
        );
    }

    /// The verified seventh break's shape A (2026-08-23), the regression
    /// half: the parked-interactive resume. The sixth fix releases the
    /// identity at the parked call's close by design — the approval's later
    /// outcome re-attaches it at the resuming dispatch. But the fresh
    /// resolution read ONLY the tail: a message absorbed behind the outcome
    /// became the tail, and the resumed continuation anchored on the
    /// absorbed line — the consumer's original escalation, re-opened. The
    /// resolution is ledger-first now ([`ratchet::fresh_turn_anchor`]): the
    /// null-anchored tail inherits the newest UNANSWERED outcome's anchor,
    /// so the continuation anchors the ORIGINAL summons.
    #[tokio::test]
    async fn an_approval_resume_keeps_its_summons_past_an_absorbed_message() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let call = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-appr".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: true,
                },
                None,
            )
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.streaming = true;
        actor.open_turn = Some(summons);
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "the parked interactive close releases the identity"
        );

        // The approval resolves — the outcome carries the call's own
        // anchor — and a message is absorbed behind it before the resuming
        // dispatch fires.
        ctx.store
            .complete_tool_call_block(conv, "call-appr".into(), "approved".into(), call)
            .await
            .unwrap()
            .expect("the call is unresolved");
        ctx.store
            .insert_text_block(conv, crate::block::Role::User, "absorbed".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(summons),
            "the resumed continuation anchors the original summons, never the absorbed line"
        );
        assert_eq!(actor.open_turn, Some(summons));
    }

    /// The verified seventh break's shape B (2026-08-23), the pre-existing
    /// half: the restart shape. A fresh actor over the same ledger holds
    /// nothing — every identity it resolves is ledger-derived — so a round
    /// whose result was recorded while no actor was live, with a message absorbed
    /// behind it, resumed anchored on the absorbed line. The ledger-first
    /// resolution derives the same turn a live actor would have been
    /// holding: the held identity is a consistency cache, and the ledger is
    /// the fact.
    #[tokio::test]
    async fn a_fresh_actor_derives_the_open_turn_from_the_ledger_alone() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let call = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-restart".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        ctx.store
            .complete_tool_call_block(conv, "call-restart".into(), "ok".into(), call)
            .await
            .unwrap()
            .expect("the call is unresolved");
        ctx.store
            .insert_text_block(conv, crate::block::Role::User, "absorbed".into())
            .await
            .unwrap();

        // The restart: a brand-new actor, no held state at all.
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(summons),
            "the recovered continuation anchors the original summons, never the absorbed line"
        );
        assert_eq!(actor.open_turn, Some(summons));
    }

    /// The inheritance's bound, pinned beside the seventh fix (2026-08-23):
    /// a COMPLETED turn's outcome captures nothing. The continuation's text
    /// block carries the turn's anchor behind the outcome, the outcome
    /// reads answered, and the next summons starts an identity of its own.
    #[tokio::test]
    async fn an_answered_outcome_never_captures_the_next_summons() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let call = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-done".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        ctx.store
            .complete_tool_call_block(conv, "call-done".into(), "ok".into(), call)
            .await
            .unwrap()
            .expect("the call is unresolved");
        ctx.store
            .insert_final_text_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                "done".into(),
                None,
            )
            .await
            .unwrap();
        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();

        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "a completed turn's outcome never captures the next summons"
        );
        assert_eq!(actor.open_turn, Some(fresh));
    }

    /// The inheritance's other bound (2026-08-23): a status-marked turn's
    /// outcome does not capture. The interrupt's status block carries the
    /// turn's anchor, which closes the turn as answered for the walk — the
    /// next summons is its own turn.
    #[tokio::test]
    async fn an_interrupted_turns_outcome_never_captures_the_next_summons() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let call = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-cut".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        ctx.store
            .complete_tool_call_block(conv, "call-cut".into(), "ok".into(), call)
            .await
            .unwrap()
            .expect("the call is unresolved");
        ctx.store
            .insert_status_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                "interrupted".into(),
                None,
            )
            .await
            .unwrap();
        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();

        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "a status-marked turn's outcome never captures the next summons"
        );
        assert_eq!(actor.open_turn, Some(fresh));
    }

    // ─── The stored turn closure (2026-08-23, the eighth break) ─────────

    /// One whole recorded tool round — the summons, its call, the call's
    /// result — the floor every eighth-break shape stands on: the result is
    /// the turn's last outcome, and whether anything ever answers it is
    /// exactly what the shapes vary.
    async fn one_recorded_round(
        script: Script,
    ) -> (RuntimeContext<BlockKind, CoreEvent>, i64, i64) {
        let (ctx, conv, _probe) = scripted_context(script).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let call = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-r1".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        ctx.store
            .complete_tool_call_block(conv, "call-r1".into(), "ok".into(), call)
            .await
            .unwrap()
            .expect("the call is unresolved");
        (ctx, conv, summons)
    }

    /// Every status block in the ledger, as `(machine key, anchor)` — what
    /// the close-marker pins read.
    async fn status_markers(
        ctx: &RuntimeContext<BlockKind, CoreEvent>,
        conv: i64,
    ) -> Vec<(String, Option<i64>)> {
        ctx.store
            .list_blocks(conv)
            .await
            .unwrap()
            .iter()
            .filter(|block| block.block_type == "status")
            .map(|block| {
                (
                    block.fields["status"].as_str().unwrap().to_owned(),
                    block.dispatch_anchor,
                )
            })
            .collect()
    }

    /// The eighth break's first shape (2026-08-23): the parked interactive
    /// call in round TWO. Round one recorded a result; round two emitted
    /// only an interactive call, so nothing anchored on the turn follows
    /// that result except a `tool_call` block — an end with no side effect,
    /// which stranded the result as unanswered forever and captured the
    /// next unrelated summons. The close now writes the turn's end down: a
    /// status block anchored on the turn, keyed with the closing edge, and
    /// the fresh summons anchors on itself.
    #[tokio::test]
    async fn a_round_two_interactive_park_writes_the_turns_end() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons), "round two is the same turn");
        assert_eq!(
            actor.answered_outcomes, 1,
            "the round-one outcome is answered by this dispatch"
        );

        ctx.store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-r2".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: true,
                },
                None,
            )
            .await
            .unwrap();
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "the parked interactive close releases the identity"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:closed".to_owned(), Some(summons))],
            "the close wrote the turn's end down, anchored on the turn"
        );

        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "the fresh summons anchors on itself, never on the parked turn"
        );
        assert_eq!(actor.open_turn, Some(fresh));
    }

    /// The eighth break's second shape (2026-08-23): the LOST round two.
    /// Round one recorded a result; round two's stream said tool use but
    /// the truncated lifecycle recorded nothing at all, so the close
    /// releases the identity with the result still unanswered. The close
    /// writes the turn's end down, and the fresh summons anchors on itself.
    #[tokio::test]
    async fn a_lost_round_two_writes_the_turns_end() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons));

        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            actor.open_turn, None,
            "the lost round's close ends the turn"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:closed".to_owned(), Some(summons))]
        );

        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "the fresh summons anchors on itself, never on the lost turn"
        );
    }

    /// The eighth break's third shape (2026-08-23): the ABANDONED close
    /// after a round-one result — the reader hit its drain deadline, the
    /// binding is retired, and the turn ends with its result unanswered.
    /// The abandoned close rides the closed edge, so that is the edge the
    /// marker names.
    #[tokio::test]
    async fn an_abandoned_close_after_a_result_writes_the_turns_end() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons));

        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.turn_anchor.mark_abandoned(actor.turn_anchor.epoch());
        actor.handle_stream_closed().await;
        assert_eq!(actor.open_turn, None, "the abandoned close ends the turn");
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:closed".to_owned(), Some(summons))]
        );

        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "the fresh summons anchors on itself, never on the abandoned turn"
        );
    }

    /// The eighth break's fourth shape (2026-08-23): the ERROR edge after a
    /// round-one result. The actor's own store-failure paths ride this
    /// same edge unstamped, so this is also the recoverable half of that
    /// residual: whenever the store can still serve the close, the turn's
    /// end is recorded, and only a store too broken to write stays
    /// unmarked.
    #[tokio::test]
    async fn an_error_close_after_a_result_writes_the_turns_end() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons));

        actor.handle_stream_error().await;
        assert_eq!(actor.open_turn, None, "the error edge ends the turn");
        assert!(actor.read_latched.get(), "the error edge still latches");
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:errored".to_owned(), Some(summons))]
        );

        actor.write_latched.set(false);
        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "the fresh summons anchors on itself, never on the errored turn"
        );
    }

    /// The two-turns-merged follow-on, resolved (2026-08-23): before the
    /// marker, the parked turn's stranded result captured the next summons
    /// and the ledger read as ONE turn wearing two summonses — and when the
    /// approval finally resolved, its outcome resumed a turn whose identity
    /// the capture had already spent. With the close's marker the shapes
    /// separate: the captured summons gets an identity of its own, answers
    /// as its own turn, and the approval's outcome — which lands AFTER the
    /// marker — still resumes the original turn through the unchanged
    /// inheritance walk. The approval-resume pin, re-proven with the marker
    /// in the ledger.
    #[tokio::test]
    async fn the_captured_summons_shape_resolves_to_two_turns() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        let call2 = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-r2".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: true,
                },
                None,
            )
            .await
            .unwrap();
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;

        // The summons the break used to capture: its own turn now.
        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "the once-captured summons has an identity of its own"
        );

        // That turn answers and ends — anchored on ITS summons.
        ctx.store
            .insert_final_text_block(
                crate::store::BlockDestination::anchored(conv, Some(fresh)),
                crate::block::Role::Assistant,
                "answered".into(),
                None,
            )
            .await
            .unwrap();
        actor.handle_stream_done(Some(StopReason::EndTurn));
        actor.handle_stream_closed().await;
        assert_eq!(actor.open_turn, None);

        // The approval resolves at last: its outcome lands after the
        // marker, so the walk reads it unanswered and the resuming dispatch
        // inherits the ORIGINAL summons — the marker answers only the
        // outcomes before it.
        ctx.store
            .complete_tool_call_block(conv, "call-r2".into(), "approved".into(), call2)
            .await
            .unwrap()
            .expect("the approval call is unresolved");
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(summons),
            "the approval's outcome still resumes the original turn"
        );
        assert_eq!(actor.open_turn, Some(summons));
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:closed".to_owned(), Some(summons))],
            "one end, one marker — the answered fresh turn wrote none"
        );
    }

    /// The ordering the close states (2026-08-23): the marker append
    /// completes before the close consumes the deferred owed-turn nudge,
    /// so a summons the close's re-check sets in motion resolves over a
    /// snapshot that already carries the marker — it cannot outrun it. The
    /// ordering is observable through the anchor: the marker is transparent
    /// to the frontier (the verified burial defect — an opaque marker
    /// buried the absorbed line forever, since the non-latching closed
    /// edge re-checks exactly once), so the re-check dispatches the
    /// absorbed line's turn, and only a snapshot already carrying the
    /// marker anchors that turn on the absorbed line itself — without the
    /// marker, the walk would hand it the dead turn's unanswered outcome.
    #[tokio::test]
    async fn the_turn_end_marker_precedes_the_closes_re_check() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons));

        // A message absorbed while round two is open: the delivery defers
        // to the close.
        let absorbed = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "absorbed".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert!(actor.owed_turn_deferred, "the mid-turn delivery deferred");

        // Round two records nothing and closes: the turn ends, the marker
        // is committed, and only then is the nudge consumed.
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(actor.open_turn, None);
        assert_eq!(recheck.get(), 1, "the close nudged the scheduler once");
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        let marker = blocks.last().unwrap();
        assert_eq!(
            (marker.block_type.as_str(), marker.dispatch_anchor),
            ("status", Some(summons)),
            "the marker is durably committed when the nudge fires"
        );
        assert!(
            marker.id > absorbed,
            "the marker follows the absorbed line in ledger order"
        );

        // The re-check's delivery: the frontier reads through the marker,
        // and the absorbed line summons its own turn — anchored on itself,
        // because the committed marker answers the dead turn's outcome for
        // the inheritance walk. The dead turn itself stays dead.
        actor.handle_blocks_ready().await;
        assert!(
            actor.streaming,
            "the close's re-check dispatches the absorbed line's turn"
        );
        assert_eq!(
            actor.turn_anchor.get(),
            Some(absorbed),
            "the absorbed line anchors on itself — the marker preceded the re-check"
        );
        assert_eq!(actor.open_turn, Some(absorbed));

        // Exactly once: the turn answers and closes, and the frontier
        // rests — no re-dispatch off the marker, and the answered turn
        // writes no second marker.
        ctx.store
            .insert_final_text_block(
                crate::store::BlockDestination::anchored(conv, Some(absorbed)),
                crate::block::Role::Assistant,
                "answered".into(),
                None,
            )
            .await
            .unwrap();
        actor.handle_stream_done(Some(StopReason::EndTurn));
        actor.handle_stream_closed().await;
        assert_eq!(actor.open_turn, None);
        actor.handle_blocks_ready().await;
        assert!(
            !actor.streaming,
            "the absorbed line's turn fires exactly once"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:closed".to_owned(), Some(summons))],
            "one end, one marker"
        );
    }

    /// The transparency's bound (2026-08-23), pinned beside the burial fix:
    /// when nothing owed sits behind the marker — the block there is the
    /// dead turn's own outcome, which the marker answers — the re-check
    /// rests. The frontier reads through the marker to what it buried,
    /// never past it onto the closed turn's products: reaching the outcome
    /// would redispatch the very turn the marker recorded as ended.
    #[tokio::test]
    async fn the_stand_down_rests_when_nothing_owed_sits_behind_the_marker() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons));

        // Round two records nothing and closes: the turn ends over its
        // unanswered result, and the ledger's tail is that result, then
        // the marker.
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:closed".to_owned(), Some(summons))]
        );

        actor.handle_blocks_ready().await;
        assert!(
            !actor.streaming,
            "the dead turn's outcome never summons through its own marker"
        );
        assert_eq!(actor.open_turn, None);
    }

    /// The tool-execution window, end to end on the error edge (2026-08-23,
    /// the verified regression on the transparency's first cut): a message
    /// absorbed between the turn's call and its RESULT sits under the
    /// outcome-plus-marker pair, and a frontier read that skips only the
    /// trailing markers stopped on the outcome, read it as answered by the
    /// marker, and rested — the absorbed line was buried exactly as the
    /// opaque marker buried it, and unlike the closed edge the burial here
    /// survives the latch's release too, because every later delivery reads
    /// the same shape. The dead turn's whole trailing run is transparent,
    /// so the delivery after the unlatch dispatches the absorbed line's
    /// turn — anchored on itself, since the marker answers the dead turn's
    /// outcome for the inheritance walk.
    #[tokio::test]
    async fn an_error_close_dispatches_the_line_absorbed_in_the_tool_window() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let (mut actor, recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons));

        // The round records its call; the line is absorbed while the tool
        // is still executing — BEFORE the result — and the mid-turn
        // delivery defers to the close.
        let call = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call-r1".into(),
                    name: "echo".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let absorbed = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "absorbed".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert!(actor.owed_turn_deferred, "the mid-turn delivery deferred");
        ctx.store
            .complete_tool_call_block(conv, "call-r1".into(), "ok".into(), call)
            .await
            .unwrap()
            .expect("the call is unresolved");

        // The error edge: the turn ends over its unanswered result, the
        // marker is written, and the close consumes the deferral.
        actor.handle_stream_error().await;
        assert_eq!(actor.open_turn, None, "the error edge ends the turn");
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:errored".to_owned(), Some(summons))]
        );
        assert_eq!(recheck.get(), 1, "the close nudged the scheduler once");

        // The repair unlatches; the next delivery reads through the dead
        // turn's whole trailing run and the absorbed line summons its own
        // turn.
        actor.write_latched.set(false);
        actor.handle_blocks_ready().await;
        assert!(
            actor.streaming,
            "the absorbed line's turn dispatches past the outcome-plus-marker pair"
        );
        assert_eq!(
            actor.turn_anchor.get(),
            Some(absorbed),
            "the absorbed line anchors on itself"
        );
        assert_eq!(actor.open_turn, Some(absorbed));
    }

    /// The restart half of the marker's do-no-harm proof (2026-08-23): a
    /// fresh actor over a marked ledger — the restart shape — derives the
    /// same answers a live actor holds. The marked turn stays ended (its
    /// outcome captures nothing), and the next summons starts fresh; the
    /// mid-round restart's inheritance is untouched because a kept close
    /// writes no marker, which
    /// [`a_fresh_actor_derives_the_open_turn_from_the_ledger_alone`] pins.
    #[tokio::test]
    async fn a_restart_over_a_marked_end_starts_the_next_summons_fresh() {
        let (ctx, conv, summons) = one_recorded_round(Script::CountOnly).await;
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(actor.open_turn, Some(summons));
        actor.handle_stream_done(Some(StopReason::ToolUse));
        actor.handle_stream_closed().await;
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("turn_ended:closed".to_owned(), Some(summons))]
        );
        drop(actor);

        // The restart: a brand-new actor over the marked ledger.
        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "fresh".into())
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert_eq!(
            actor.turn_anchor.get(),
            Some(fresh),
            "the restarted actor reads the marked turn as ended"
        );
        assert_eq!(actor.open_turn, Some(fresh));
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
    #[derive(Clone, Copy, Debug)]
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
        /// [`Script::TwoToolRounds`], parameterized: `rounds` sequential tool
        /// rounds before the closing prose, each answering with a call of its
        /// own. A model that never stops asking for tools is what the
        /// tool-call window exists for, and this is that model on the wire —
        /// the only way to drive a RUN of rate-limit refusals, either
        /// window's, through the real loop instead of writing the refusals
        /// into the ledger by hand.
        ManyToolRounds { rounds: usize },
        /// [`Script::TwoToolRounds`] with round ONE's trailing done HELD:
        /// the message end and the whole tool lifecycle go out at once — so
        /// the call records and its result lands while the stream is still
        /// open — and the done waits for the probe's release. What that
        /// stages deterministically is the consumer-proven escalation shape
        /// (amended 2026-08-22): a message appended after round one's result
        /// and released into the close becomes the continuation's
        /// dispatching tail, and the continuation must still anchor on the
        /// original summons.
        HoldFirstRoundsDone,
        /// [`Script::HoldFirstRoundsDone`]'s sibling: round one goes out
        /// WHOLE — message end, tool lifecycle, trailing done — but the tool
        /// itself (`held_echo`) waits for the probe's `finish`, so the
        /// stream CLOSES while the call is still owed its result. The
        /// close's snapshot then holds an unresolved call bearing the
        /// turn's anchor and a tail (the call) that owes nobody a model
        /// turn — the identity-release rule's unresolved-call arm,
        /// isolated. Later rounds answer with the closing prose.
        HoldFirstRoundsResult,
        /// The lost tool round (2026-08-22, the verified fifth break): the
        /// first turn stops for TOOL USE and its tool lifecycle is
        /// truncated — a start with no end, discarded at the trailing done
        /// as an incomplete fact. Nothing owing is recorded, so no
        /// continuation is ever due. Later turns answer with plain prose.
        TruncatedToolLifecycle,
        /// [`Script::TruncatedToolLifecycle`] with the trailing done HELD
        /// for the probe's release: the prose, the message end and the
        /// truncated lifecycle go out at once, so a message can be absorbed
        /// inside the still-open window before the close discards the
        /// lifecycle. The sixth break's shape A: nothing owing is recorded,
        /// yet the tail — the absorbed message — owes the model. Later
        /// turns answer with plain prose.
        TruncatedHeld,
        /// One tool round whose tool ENDS the turn (2026-08-30), in the real
        /// wire order — message end with the tool-use stop, the named tool's
        /// lifecycle, the trailing done — and closing prose for any later
        /// request. The park pin's shape: a turn that ends on a tool round
        /// with nothing after it, which no script said before this slice.
        ///
        /// Parameterized by tool, like [`Script::ToolRound`]: `park` resolves
        /// at once, and `held_park` holds its result past the round's CLOSE,
        /// so the close keeps the identity on its unresolved-call arm and the
        /// stamped resolution lands behind it — the interleaving the
        /// summons-time revalidation exists for.
        ParkRound { tool: &'static str },
        /// One turn emitting the park call AND an ordinary sibling whose
        /// result is held, so the sibling's outcome lands LAST: the park stamp
        /// silences only its own outcome, and the sibling's continuation is
        /// summoned as usual. Later requests answer with the closing prose.
        ParkBesideSibling,
        /// One park round holding BOTH ends of its window open: the message
        /// end and the lifecycle go out at once, the tool (`held_park`) waits
        /// for the probe's `finish`, and the trailing done waits for its
        /// `release`. So a message absorbed into the still-open window lands
        /// BEFORE the stamped resolution, and the close runs after both —
        /// AC8's shape, the one ordering that leaves an addressed message
        /// sitting behind a turn-ending tail with no inbound left to surface
        /// it. Later requests answer with the closing prose.
        ParkRoundOverAnAbsorbedMessage,
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

    /// A turn whose trailing done waits for the probe's release: the given
    /// events go out at once, and only the done is held — the still-open
    /// window the held scripts absorb a message into. Round one of
    /// [`Script::HoldFirstRoundsDone`] rides it with a whole tool round,
    /// [`Script::TruncatedHeld`] with the truncated lifecycle.
    fn held_done_events(
        events: Vec<StreamEvent>,
        resp_tx: &tokio::sync::mpsc::UnboundedSender<ProviderResponse>,
        release: &Arc<tokio::sync::Notify>,
    ) -> Scripted {
        let resp_tx = resp_tx.clone();
        let release = Arc::clone(release);
        tokio::spawn(async move {
            release.notified().await;
            let _ = resp_tx.send(ProviderResponse::Done);
        });
        Scripted::Events(events)
    }

    /// Round one of [`Script::HoldToolRound`]: the message end goes out now;
    /// the tool lifecycle and the trailing done wait for the probe's
    /// release — the held post-message-end window the duplicate-turn defect
    /// lived in.
    fn held_lifecycle_round(
        turn: usize,
        tool: &'static str,
        resp_tx: &tokio::sync::mpsc::UnboundedSender<ProviderResponse>,
        release: &Arc<tokio::sync::Notify>,
    ) -> Scripted {
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
        /// The tool names every request was offered, in arrival order, taken
        /// before anything else is read off the request — the recorded-choice
        /// tests assert WHAT the dispatch offered, and an empty offer is one
        /// of the answers they assert.
        offered: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
        /// The held-window scripts' latch, shared with the probe: notifying
        /// it lets a held tool round proceed.
        release: Arc<tokio::sync::Notify>,
        /// The second latch, for the scripts that stage TWO holds: notifying
        /// it lets a held turn's end go out. Inert everywhere else.
        finish: Arc<tokio::sync::Notify>,
        /// Every definition-free derivation request that reached the wire —
        /// what the title-switch pins count: on, at least one; off, zero.
        title_requests: Arc<AtomicUsize>,
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

    /// Whether any of a request's messages carries `needle` in its text —
    /// how the tests assert WHAT a dispatched request absorbed.
    fn request_carries(messages: &[Message], needle: &str) -> bool {
        messages.iter().any(|m| match &m.content {
            MessageContent::Text(text) => text.contains(needle),
            MessageContent::Parts(parts) => parts
                .iter()
                .any(|p| matches!(p, ContentPart::Text { text } if text.contains(needle))),
        })
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
                        ContentPart::Image { .. } => "image",
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
            let offered = Arc::clone(&self.offered);
            let release = Arc::clone(&self.release);
            let finish = Arc::clone(&self.finish);
            let title_requests = Arc::clone(&self.title_requests);
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
                    // Recorded FIRST, before the title-derivation branch: a
                    // turn whose conversation recorded an empty choice is
                    // offered nothing too, and the tests that assert that need
                    // to see it.
                    offered
                        .lock()
                        .unwrap()
                        .push(tools.iter().map(|tool| tool.name.clone()).collect());
                    // The metadata worker shares this provider and its
                    // derivation request is the one carrying no tool
                    // definitions — answer it with a title and keep it out of
                    // the TURN count, which is what the tests assert on. It
                    // is counted on its own, for the title-switch pins.
                    if tools.is_empty() {
                        title_requests.fetch_add(1, Ordering::SeqCst);
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

    /// The composed-runtime shape's opening turn: one tool call, with the
    /// message end after the whole lifecycle.
    fn call_before_end_events() -> Vec<StreamEvent> {
        vec![
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
        ]
    }

    /// The lost round's events: prose, the message end with the tool-use
    /// stop, then a tool lifecycle cut off before its end — which the
    /// reader discards at the trailing done as an incomplete fact.
    fn truncated_lifecycle_events() -> Vec<StreamEvent> {
        vec![
            StreamEvent::TextBlockStart,
            StreamEvent::TextDelta { text: "hi".into() },
            StreamEvent::MessageEnd {
                usage: crate::providers::Usage::default(),
                stop_reason: StopReason::ToolUse,
            },
            StreamEvent::ToolUseStart {
                id: "call-truncated".into(),
                name: "echo".into(),
            },
            StreamEvent::ToolUseInputDelta { json: "{}".into() },
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

    /// One turn emitting the park call and an ordinary sibling, in the real
    /// wire order: the message end with the tool-use stop, then both drained
    /// lifecycles. The sibling's tool holds its result, so the sibling's
    /// outcome is the one that lands last.
    fn park_and_sibling_round_events() -> Vec<StreamEvent> {
        let mut events = vec![StreamEvent::MessageEnd {
            usage: crate::providers::Usage::default(),
            stop_reason: StopReason::ToolUse,
        }];
        for (id, tool) in [("call-park", "park"), ("call-sibling", "held_echo")] {
            events.extend([
                StreamEvent::ToolUseStart {
                    id: id.into(),
                    name: tool.into(),
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
            (Script::ToolCallThenText, 0) => Scripted::Turn(call_before_end_events()),
            (Script::ToolRound { tool, narration }, 0) => {
                Scripted::Turn(tool_round_events(turn, tool, narration))
            }
            (Script::TwoToolRounds, 0 | 1) | (Script::HoldFirstRoundsDone, 1) => {
                Scripted::Turn(tool_round_events(turn, "echo", None))
            }
            // Keyed on the REQUEST count, not on what the request carried: a
            // refused round answers the model with a tool error, which reads
            // as an answered call, so an `answered`-keyed arm would run out
            // of rounds exactly where the run of refusals begins.
            (Script::ManyToolRounds { rounds }, _) => {
                if turn <= rounds {
                    Scripted::Turn(tool_round_events(turn, "echo", None))
                } else {
                    Scripted::Turn(prose_turn_events("done"))
                }
            }
            (Script::HoldFirstRoundsDone, 0) => {
                held_done_events(tool_round_events(turn, "echo", None), resp_tx, release)
            }
            (Script::HoldFirstRoundsResult, 0) => {
                Scripted::Turn(tool_round_events(turn, "held_echo", None))
            }
            (Script::TruncatedToolLifecycle, _) if turn == 1 => {
                Scripted::Turn(truncated_lifecycle_events())
            }
            (Script::TruncatedHeld, _) if turn == 1 => {
                held_done_events(truncated_lifecycle_events(), resp_tx, release)
            }
            (Script::HoldFirstRoundsResult, 1..)
            | (Script::TruncatedToolLifecycle | Script::TruncatedHeld, _) => {
                Scripted::Turn(prose_turn_events("done"))
            }
            (Script::EmptyTurn, _) => Scripted::Turn(vec![message_end]),
            (Script::TwoCallsInOneTurn, 0) => Scripted::Turn(two_call_round_events()),
            (
                Script::ToolCallThenText
                | Script::ToolRound { .. }
                | Script::HoldToolRound { .. }
                | Script::TwoToolRounds
                | Script::HoldFirstRoundsDone
                | Script::TwoCallsInOneTurn,
                2..,
            )
            | (
                Script::ToolCallThenText | Script::ToolRound { .. } | Script::HoldToolRound { .. },
                1,
            ) => Scripted::Turn(prose_turn_events("done")),
            (Script::HoldToolRound { tool }, 0) => {
                held_lifecycle_round(turn, tool, resp_tx, release)
            }
            (
                Script::ParkRound { .. }
                | Script::ParkBesideSibling
                | Script::ParkRoundOverAnAbsorbedMessage,
                _,
            ) => park_turn(script, turn, answered, resp_tx, release),
        }
    }

    /// Every park shape's whole script (2026-08-30). The opening round calls
    /// the tool that ends the turn, and each shape decides only how much of
    /// that round it holds open. A LATER request answers with the closing
    /// prose — and only a fresh summons or a sibling's outcome can produce
    /// one, so a request keyed on the park round alone is the defect these
    /// pins exist to catch: nothing here answers it.
    fn park_turn(
        script: Script,
        turn: usize,
        answered: usize,
        resp_tx: &tokio::sync::mpsc::UnboundedSender<ProviderResponse>,
        release: &Arc<tokio::sync::Notify>,
    ) -> Scripted {
        if answered > 0 {
            return Scripted::Turn(prose_turn_events("done"));
        }
        match script {
            Script::ParkRound { tool } => Scripted::Turn(tool_round_events(turn, tool, None)),
            Script::ParkBesideSibling => Scripted::Turn(park_and_sibling_round_events()),
            // Both ends of the window held: the tool waits for the probe's
            // finish, the trailing done for its release.
            Script::ParkRoundOverAnAbsorbedMessage => {
                held_done_events(tool_round_events(turn, "held_park", None), resp_tx, release)
            }
            other => unreachable!("park_turn reached from {other:?}"),
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

    /// An ungated tool that answers only at the probe's `finish` — how the
    /// held-result script keeps a recorded call unresolved past its
    /// stream's close.
    struct HeldEchoTool {
        finish: Arc<tokio::sync::Notify>,
    }

    impl ToolHandler<CoreEvent> for HeldEchoTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "held_echo".into(),
                description: "answers with a fixed string when released".into(),
                parameters: json!({ "type": "object" }),
            }
        }
        fn execute<'a>(
            &'a self,
            _input: &'a str,
            _ctx: ToolContext<'a, CoreEvent>,
        ) -> BoxFuture<'a, ToolOutcome> {
            Box::pin(async {
                self.finish.notified().await;
                ToolOutcome::Done("echoed".into())
            })
        }
    }

    /// A tool whose successful call ENDS the turn — the capability slice 18
    /// gave the machinery, played here by a tool that says the model has
    /// nothing left to do.
    struct ParkTool;

    impl ToolHandler<CoreEvent> for ParkTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "park".into(),
                description: "ends the turn".into(),
                parameters: json!({ "type": "object" }),
            }
        }
        fn ends_turn(&self) -> bool {
            true
        }
        fn execute<'a>(
            &'a self,
            _input: &'a str,
            _ctx: ToolContext<'a, CoreEvent>,
        ) -> BoxFuture<'a, ToolOutcome> {
            Box::pin(async { ToolOutcome::Done("nothing to do".into()) })
        }
    }

    /// [`ParkTool`] that answers only at the probe's `finish` — how the
    /// interleaving pin puts the stream's CLOSE before the park result
    /// commits, the exact window the summons-time revalidation exists for.
    struct HeldParkTool {
        finish: Arc<tokio::sync::Notify>,
    }

    impl ToolHandler<CoreEvent> for HeldParkTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "held_park".into(),
                description: "ends the turn when released".into(),
                parameters: json!({ "type": "object" }),
            }
        }
        fn ends_turn(&self) -> bool {
            true
        }
        fn execute<'a>(
            &'a self,
            _input: &'a str,
            _ctx: ToolContext<'a, CoreEvent>,
        ) -> BoxFuture<'a, ToolOutcome> {
            Box::pin(async {
                self.finish.notified().await;
                ToolOutcome::Done("nothing to do".into())
            })
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
        /// The tool names every request was offered, in arrival order.
        offered: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
        /// The held-window scripts' latch: notify it to let a held tool round
        /// proceed. Inert for every other script.
        release: Arc<tokio::sync::Notify>,
        /// The two-hold scripts' second latch: notify it to let a held turn's
        /// end go out. Inert for every other script.
        finish: Arc<tokio::sync::Notify>,
        /// How many definition-free title derivation requests reached the
        /// provider — the title-switch pins' counter.
        title_requests: Arc<AtomicUsize>,
    }

    /// The scripted context WITHOUT the reactor: for tests that construct or
    /// call actor internals directly rather than routing through the bus.
    async fn scripted_context(
        script: Script,
    ) -> (RuntimeContext<BlockKind, CoreEvent>, i64, ComposedProbe) {
        scripted_context_over(Store::in_memory().unwrap(), script).await
    }

    /// The same, over a store the caller opened — the path-backed harness the
    /// restart pins need, since the in-memory store every other test uses
    /// cannot be reopened. Each call creates a conversation of its own, so a
    /// reopen names the conversation it kept from before rather than the one
    /// this returns.
    ///
    /// The conversation opens with a system prompt because the dispatch
    /// refuses a ledger that does not (2026-09-02): every test here that
    /// dispatches a turn needs a conversation a turn can be dispatched for,
    /// and the prompt is that. It is the ledger's first block everywhere, so
    /// a test's own appends start at index one.
    async fn scripted_context_over(
        store: Store,
        script: Script,
    ) -> (RuntimeContext<BlockKind, CoreEvent>, i64, ComposedProbe) {
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
        store
            .insert_system_prompt(conv, "answer the member".into())
            .await
            .unwrap();

        let requests = Arc::new(AtomicUsize::new(0));
        let shapes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let offered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());
        let title_requests = Arc::new(AtomicUsize::new(0));
        let mut providers = ProviderRegistry::new();
        providers.register(Box::new(ScriptedProvider {
            script,
            requests: Arc::clone(&requests),
            shapes: Arc::clone(&shapes),
            seen: Arc::clone(&seen),
            offered: Arc::clone(&offered),
            release: Arc::clone(&release),
            finish: Arc::clone(&finish),
            title_requests: Arc::clone(&title_requests),
        }));
        let mut tools = ToolRegistry::new();
        tools.register("echo", EchoTool);
        tools.register("boom", FailingTool);
        tools.register(
            "held_echo",
            HeldEchoTool {
                finish: Arc::clone(&finish),
            },
        );
        tools.register("park", ParkTool);
        tools.register(
            "held_park",
            HeldParkTool {
                finish: Arc::clone(&finish),
            },
        );

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
                offered,
                release,
                finish,
                title_requests,
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
            blocks.len() == 6 && blocks.last().is_some_and(|b| b.block_type == "text")
        })
        .await;

        // Block by block: the conversation's prompt, the date marker the
        // append stamped, the user's message, the scripted call, the executed
        // result, the closing prose.
        let shape: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            shape,
            vec![
                "system_prompt",
                "date_marker",
                "text",
                "tool_call",
                "tool_result",
                "text"
            ]
        );
        assert_eq!(blocks[2].role, Some(crate::block::Role::User));
        assert_eq!(blocks[2].fields["content"], json!("hi"));
        assert_eq!(blocks[3].fields["name"], json!("echo"));
        assert_eq!(blocks[3].fields["input"], json!("{}"));
        assert_eq!(blocks[4].fields["content"], json!("echoed"));
        assert_eq!(blocks[4].fields["tool_call_id"], json!("call-1"));
        assert_eq!(blocks[5].role, Some(crate::block::Role::Assistant));
        assert_eq!(blocks[5].fields["content"], json!("done"));

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

    /// The title switch, OFF: a context built with
    /// [`RuntimeContext::without_title_derivation`] runs a whole conversation
    /// flow — append, owed turn, streamed answer — and ZERO title requests
    /// reach the provider. The metadata ledger stays empty too: nothing is
    /// parked that could fire a derivation later, because no metadata worker
    /// was spawned at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_context_without_title_derivation_dispatches_no_title_request() {
        let (ctx, conv, probe) = scripted_context(Script::Prose).await;
        let ctx = ctx.without_title_derivation();
        spawn_reactor(ctx.clone());

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        await_ledger(&ctx, conv, "the answered turn", |blocks| {
            blocks
                .last()
                .is_some_and(|b| b.block_type == "text" && b.fields["content"] == json!("answered"))
        })
        .await;

        // The assistant has spoken and the conversation is small — exactly
        // the state the policy watcher inserts its request in. Hold the
        // window open, then pin the silence on every surface.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            probe.title_requests.load(Ordering::SeqCst),
            0,
            "no title request reaches the provider with the switch off"
        );
        assert!(
            !ctx.store.has_metadata(conv, "title_request").await.unwrap(),
            "no request row is ever appended — nothing parked to fire later"
        );
        assert!(
            !ctx.store
                .has_metadata(conv, "title_response")
                .await
                .unwrap()
        );
    }

    /// The title switch's default, ON: the same flow over an untouched
    /// context derives the title as it always has — the switch changes
    /// nothing for existing consumers. The derivation's inner behavior keeps
    /// its own pins in the metadata module; this is the seam-level pair to
    /// the off pin above.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_default_context_keeps_deriving_titles() {
        let (ctx, conv, probe) = composed_runtime(Script::Prose).await;
        assert!(ctx.title_derivation(), "the default is on");

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !ctx
            .store
            .has_metadata(conv, "title_response")
            .await
            .unwrap()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the default context derives the title"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            probe.title_requests.load(Ordering::SeqCst) >= 1,
            "the derivation reached the provider"
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
        let (_signals_tx, from_scheduler) =
            tokio::sync::mpsc::unbounded_channel::<SchedulerSignal>();
        let (read_recheck, write_recheck) = create_signal(0u64);
        let actor = ConversationActor {
            id: conv,
            ctx,
            mailbox,
            write_latched,
            read_latched,
            from_scheduler,
            recheck: write_recheck,
            provider_tx: None,
            streaming: false,
            owed_turn_deferred: false,
            stream_generation: 0,
            turn_anchor: TurnAnchor::new(),
            open_turn: None,
            answered_outcomes: 0,
            stream_stop: None,
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
                "date_marker" | "system_prompt" => false,
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
                "system_prompt",
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

    /// The consumer-proven escalation shape (amended 2026-08-22): a member's
    /// message summons a two-round tool turn, a message is absorbed after
    /// round one's RESULT — it becomes the continuation's dispatching tail —
    /// and the continuation's call still anchors on the ORIGINAL summons.
    /// Under the retired tail-derived inheritance the continuation
    /// re-anchored on the absorbed message and the original summoner fell
    /// out of the turn's identity, which is the escalation the consumer's
    /// admission check proved: a turn summoned by one member re-anchored
    /// onto another's message. The turn's identity is actor state — held
    /// from the first dispatch, kept across the tool-use close — and the
    /// end-turn close then really ends it: a fresh summons after the turn
    /// anchors a turn of its own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_message_absorbed_after_a_rounds_result_never_reanchors_the_turn() {
        let (ctx, conv, probe) = composed_runtime(Script::HoldFirstRoundsDone).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "summons".into(),
            }],
        });

        // Round one runs to its RESULT with the stream still open: the done
        // is held, so the continuation cannot dispatch yet.
        await_ledger(&ctx, conv, "round one's result", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_result")
        })
        .await;

        // The absorbed message: appended after round one's result and before
        // the continuation dispatch — the continuation's dispatching tail.
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
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "no dispatch while round one's stream is open — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // Release the held done: the tool-use close keeps the turn open, and
        // its re-check dispatches the continuation off the absorbed tail.
        probe.release.notify_one();
        let blocks = await_ledger(&ctx, conv, "the turn's closing prose", |blocks| {
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
        assert!(
            request_carries(&probe.seen.lock().unwrap()[1], "absorbed"),
            "the continuation's request absorbed the message"
        );

        // The absorbed message really sits between round one's result and
        // the continuation's call in ledger order.
        let summoner = user_block(&blocks).id;
        let absorbed = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("absorbed")))
            .unwrap();
        let calls: Vec<&Block> = blocks
            .iter()
            .filter(|b| b.block_type == "tool_call")
            .collect();
        let results: Vec<&Block> = blocks
            .iter()
            .filter(|b| b.block_type == "tool_result")
            .collect();
        assert_eq!(calls.len(), 2);
        assert_eq!(results.len(), 2);
        assert!(
            results[0].id < absorbed.id && absorbed.id < calls[1].id,
            "the absorbed message is the continuation's dispatching tail"
        );

        // The pin itself: every product of the turn — the continuation's
        // call included — anchors on the ORIGINAL summons; the absorbed
        // message, a message and not a turn product, anchors nothing.
        assert_eq!(absorbed.dispatch_anchor, None);
        for block in calls.iter().chain(&results) {
            assert_eq!(
                block.dispatch_anchor,
                Some(summoner),
                "block {} ({}) anchors on the original summons",
                block.id,
                block.block_type
            );
        }
        let closing = blocks
            .iter()
            .find(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
            .unwrap();
        assert_eq!(closing.dispatch_anchor, Some(summoner));

        // The end-turn close ended the identity: a fresh summons now
        // anchors a turn of its own, not the finished turn's.
        assert_fresh_summons_starts_its_own_turn(&ctx, conv).await;
    }

    /// The escalation pin's closing claim, run against a conversation whose
    /// tool turn just closed on its end-turn stop: the close really ENDED
    /// the held identity, so a fresh summons resolves from the tail and its
    /// answer anchors on itself — never on the finished turn's summons.
    async fn assert_fresh_summons_starts_its_own_turn(
        ctx: &RuntimeContext<BlockKind, CoreEvent>,
        conv: i64,
    ) {
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "again".into(),
            }],
        });
        let blocks = await_ledger(ctx, conv, "the second summons' answer", |blocks| {
            assistant_answers(blocks) == vec!["done".to_string(), "done".to_string()]
        })
        .await;
        let again = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("again")))
            .unwrap();
        let last_answer = blocks
            .iter()
            .rfind(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
            .unwrap();
        assert_eq!(
            last_answer.dispatch_anchor,
            Some(again.id),
            "a fresh turn resolves from the tail — the held identity is gone"
        );
    }

    /// The verified fifth break, pinned (2026-08-22): a turn stops for TOOL
    /// USE but its lifecycle is truncated and discarded, so nothing owing is
    /// recorded and no continuation ever dispatches — the turn is over in
    /// every observable way, and the close must END its identity. Before the
    /// release rule the close kept it forever: the idle interrupt stamped
    /// the dead turn's summoner, and the next, unrelated summons inherited
    /// it — its answer anchored on the previous summoner instead of on
    /// itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lost_tool_round_ends_the_turn_instead_of_leaking_its_identity() {
        let (ctx, conv, probe) = composed_runtime(Script::TruncatedToolLifecycle).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "first".into(),
            }],
        });
        await_ledger(&ctx, conv, "the truncated turn's prose", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields.get("content") == Some(&json!("hi")))
        })
        .await;

        // The round really is lost: the unterminated lifecycle is discarded,
        // nothing owing is recorded, and nothing dispatches again.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        assert!(
            !blocks.iter().any(|b| b.block_type == "tool_call"),
            "the truncated lifecycle is discarded — ledger {:?}",
            blocks
                .iter()
                .map(|b| b.block_type.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "no continuation dispatches — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // An IDLE interrupt, after that turn ended: with no turn in flight
        // the status is nobody's product and records NULL — never the dead
        // turn's summoner.
        ctx.bus.emit(CoreEvent::InterruptRequested {
            conversation_id: conv,
        });
        let blocks = await_ledger(&ctx, conv, "the idle interrupt's status", |blocks| {
            blocks.last().is_some_and(|b| b.block_type == "status")
        })
        .await;
        assert_eq!(
            blocks.last().unwrap().dispatch_anchor,
            None,
            "an idle interrupt's status anchors nothing"
        );

        // A FRESH summons — which also unlatches — anchors a turn of its
        // own: the answer names ITS summoner, never the lost round's.
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "again".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the second summons' answer", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields.get("content") == Some(&json!("done")))
        })
        .await;
        let first = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("first")))
            .unwrap();
        let again = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("again")))
            .unwrap();
        let answer = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("done")))
            .unwrap();
        assert!(
            answer.id > again.id,
            "the answer follows the second summons"
        );
        assert_ne!(
            answer.dispatch_anchor,
            Some(first.id),
            "LEAK: the second summons' answer anchors on the FIRST summons"
        );
        assert_eq!(
            answer.dispatch_anchor,
            Some(again.id),
            "a fresh turn resolves from the tail"
        );
    }

    /// The verified sixth break's shape A, end to end (2026-08-23): the
    /// lost tool round with a message absorbed in its held window. The
    /// truncated lifecycle records nothing owing, so no continuation is
    /// ever due — but the absorbed message is the close's tail and owes
    /// the model, and the deleted frontier arm kept the dead turn's
    /// identity on exactly that: the absorbed message's own turn — someone
    /// ELSE'S fresh summons — inherited it, and its answer anchored on the
    /// dead summons. The close must end the turn, and the turn its
    /// re-check dispatches must anchor on the absorbed message itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_absorbed_message_after_a_lost_round_anchors_its_own_turn() {
        let (ctx, conv, probe) = composed_runtime(Script::TruncatedHeld).await;
        let mut done_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "first".into(),
            }],
        });

        // The window is provably open: the message end arrived and the
        // trailing done is still held.
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

        // The absorbed message: appended inside the held window, before the
        // close discards the truncated lifecycle.
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

        // Release the done: the close discards the lifecycle, ends the dead
        // turn, and its re-check dispatches the absorbed message's own turn.
        probe.release.notify_one();
        let blocks = await_ledger(&ctx, conv, "the absorbed message's answer", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        assert!(
            !blocks.iter().any(|b| b.block_type == "tool_call"),
            "the truncated lifecycle is discarded"
        );
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "the lost round and the absorbed message's turn — got {:?}",
            probe.shapes.lock().unwrap()
        );
        assert!(
            request_carries(&probe.seen.lock().unwrap()[1], "absorbed"),
            "the second request carries the absorbed message"
        );

        let first = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("first")))
            .unwrap();
        let absorbed = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("absorbed")))
            .unwrap();
        let answer = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("done")))
            .unwrap();
        assert_ne!(
            answer.dispatch_anchor,
            Some(first.id),
            "LEAK: the absorbed message's answer anchors on the dead turn's summons"
        );
        assert_eq!(
            answer.dispatch_anchor,
            Some(absorbed.id),
            "the absorbed message's answer anchors on the absorbed message"
        );
    }

    /// The release rule's kept case, end to end (the
    /// [`Script::HoldFirstRoundsDone`] pin family): round one's stream closes with its call still owed a result
    /// — the unresolved-call arm keeps the identity — so a message absorbed
    /// before the result cannot re-anchor the turn: nothing dispatches
    /// while the call is owed, the continuation the result unlocks absorbs
    /// the message and still anchors on the original summons, and the
    /// end-turn close then really ends the identity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_close_before_the_result_keeps_the_turn_for_its_continuation() {
        let (ctx, conv, probe) = composed_runtime(Script::HoldFirstRoundsResult).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "summons".into(),
            }],
        });

        // Round one goes out whole and its stream closes; the held tool
        // keeps the call unresolved past the close.
        await_ledger(&ctx, conv, "round one's call", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_call")
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The absorbed message: appended after the close, while the call is
        // still owed its result. The drive parks on the unresolved call, so
        // nothing dispatches — deterministically, not by luck.
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
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "no dispatch while the call is owed its result — got {:?}",
            probe.shapes.lock().unwrap()
        );
        assert!(
            !ctx.store
                .list_blocks(conv)
                .await
                .unwrap()
                .iter()
                .any(|b| b.block_type == "tool_result"),
            "the result is still held"
        );

        // Release the tool: the result unlocks the continuation, which
        // absorbs the message and closes the turn with its prose.
        probe.finish.notify_one();
        let blocks = await_ledger(&ctx, conv, "the turn's closing prose", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "one round and its continuation — got {:?}",
            probe.shapes.lock().unwrap()
        );
        assert!(
            request_carries(&probe.seen.lock().unwrap()[1], "absorbed"),
            "the continuation's request absorbed the message"
        );

        // The absorbed message sits between the call and its result in
        // ledger order — absorbed before the result, exactly the window the
        // kept identity covers.
        let call = blocks.iter().find(|b| b.block_type == "tool_call").unwrap();
        let result = blocks
            .iter()
            .find(|b| b.block_type == "tool_result")
            .unwrap();
        let absorbed = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("absorbed")))
            .unwrap();
        assert!(
            call.id < absorbed.id && absorbed.id < result.id,
            "the message is absorbed before the result"
        );

        // Every product of the turn anchors on the original summons; the
        // absorbed message, not a turn product, anchors nothing.
        let summoner = user_block(&blocks).id;
        assert_eq!(absorbed.dispatch_anchor, None);
        let closing = blocks
            .iter()
            .find(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
            .unwrap();
        for block in [call, result, closing] {
            assert_eq!(
                block.dispatch_anchor,
                Some(summoner),
                "block {} ({}) anchors on the original summons",
                block.id,
                block.block_type
            );
        }

        // The end-turn close ended the identity: a fresh summons now
        // anchors a turn of its own.
        assert_fresh_summons_starts_its_own_turn(&ctx, conv).await;
    }

    /// The kept identity's consumer-visible proof, and the pin that the
    /// release rule did not over-rotate: an interrupt arriving AFTER round
    /// one's close but while its result is still owed tears down a turn
    /// that is genuinely open, so its status stamps the held summoner —
    /// a close that wrongly ended the identity here would stamp NULL.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_interrupt_while_the_result_is_owed_stamps_the_held_turns_summoner() {
        let (ctx, conv, probe) = composed_runtime(Script::HoldFirstRoundsResult).await;
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "summons".into(),
            }],
        });
        await_ledger(&ctx, conv, "round one's call", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_call")
        })
        .await;
        // The wait puts round one's close before the interrupt: the
        // identity crossing it is the kept state under probe.
        tokio::time::sleep(Duration::from_millis(200)).await;

        ctx.bus.emit(CoreEvent::InterruptRequested {
            conversation_id: conv,
        });
        let blocks = await_ledger(&ctx, conv, "the interrupt's status", |blocks| {
            blocks.last().is_some_and(|b| b.block_type == "status")
        })
        .await;
        let summoner = user_block(&blocks).id;
        assert_eq!(
            blocks.last().unwrap().dispatch_anchor,
            Some(summoner),
            "the interrupted turn's status names the held summoner"
        );
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "the interrupted round never continued — got {:?}",
            probe.shapes.lock().unwrap()
        );
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
            request_carries(&probe.seen.lock().unwrap()[1], "absorbed"),
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

    /// A turn that says nothing closes its debt with ONE dispatch
    /// (2026-08-24, the inverted discard). The completed no-text turn
    /// commits a real empty assistant text block; that block is the frontier
    /// tail when the coalesced second owed-turn signal re-derives, so the
    /// re-ask finds nothing owed and the conversation rests at exactly one
    /// paid request. This test used to pin the discard's shape — two
    /// dispatches for one message and an assistant that "really wrote
    /// nothing" — which was the misimplementation: the same empty turn
    /// dispatched once per re-derivation, and with no second signal the
    /// owing message wedged unreachable instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_empty_turn_commits_its_block_and_closes_with_one_dispatch() {
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

        // Settle, then hold still: a debt the empty block failed to close
        // would keep the counter climbing through this whole window.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let settled = probe.requests.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            settled,
            "the conversation rests after an empty turn"
        );
        // Exactly one: the empty block settles the frontier, so the extra
        // owed-turn tick the cursor confirm buys re-derives against a tail
        // that owes nothing and dispatches no second turn for the message.
        assert_eq!(
            settled, 1,
            "the empty block closes the debt once — no re-dispatch for the same message"
        );
        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        let empty = blocks
            .iter()
            .find(|b| b.role == Some(crate::block::Role::Assistant) && b.block_type == "text")
            .expect("the empty turn commits a real assistant text block");
        assert_eq!(
            empty.fields["content"],
            json!(""),
            "the block records the empty answer faithfully"
        );
        assert_anchors(&blocks, user_block(&blocks).id);
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

    // ─── The tool-call window's forced end (2026-08-30) ──────────────────

    /// A small window for the pins below: the SHIPPED window with only the
    /// spend shrunk, so a test need not buy sixty calls. The consecutive
    /// limit and the span come from the defaults themselves rather than from
    /// a copy of their numbers — change the operator's limit and these pins
    /// exercise the new one.
    fn small_window() -> crate::tools::runner::ToolCallWindow {
        crate::tools::runner::ToolCallWindow {
            calls: 1,
            ..crate::tools::runner::ToolCallWindow::default()
        }
    }

    /// `count` refused rounds on one turn, written the way the runner writes
    /// them: a call anchored on the turn, resolved through the store surface
    /// the runner itself uses — the one that MARKS the outcome a refusal
    /// (2026-09-01) — with the conversation window's own refusal rendered from
    /// [`small_window`]'s own numbers through that window's template (the
    /// per-tool window has a template of its own), so both the text and the
    /// stored fact here are what the runner would have written. The
    /// call block between two errors is the reason the run is read as an
    /// outcome SUBSEQUENCE — two tool errors are never neighbours in a
    /// ledger.
    async fn refused_rounds(
        ctx: &RuntimeContext<BlockKind, CoreEvent>,
        conv: i64,
        anchor: i64,
        tag: &str,
        count: usize,
    ) {
        let window = small_window();
        for round in 0..count {
            let id = format!("{tag}-{round}");
            let call = ctx
                .store
                .insert_tool_call_block(
                    crate::store::BlockDestination::anchored(conv, Some(anchor)),
                    crate::block::Role::Assistant,
                    ToolCallInsert {
                        tool_call_id: id.clone(),
                        name: "echo".into(),
                        input: "{}".into(),
                        interactive: false,
                    },
                    None,
                )
                .await
                .unwrap();
            ctx.store
                .fail_tool_call_block_marked(
                    conv,
                    id,
                    crate::agency::ToolError::rate_limit_refusal(window.calls, window.seconds),
                    call,
                    crate::agency::Refusal::Refused,
                )
                .await
                .unwrap()
                .expect("the refusal resolves its call");
        }
    }

    /// AC3, through the real loop: a model that keeps calling into a spent
    /// window. Round one runs, every round after it is refused, and after the
    /// fifth refusal the would-be continuation stands down — no provider
    /// request — with the turn's end written down, anchored on the turn and
    /// walk-transparent. Then a fresh summons opens a fresh turn with a fresh
    /// anchor, which on the still-hot window refuses its way to a forced end
    /// of its own: the conversation was never latched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_run_of_window_refusals_ends_the_turn_and_a_fresh_summons_opens_a_new_one() {
        let window = small_window();
        // More rounds than the two forced ends below can ever consume, so the
        // script never runs out before the rule fires.
        let rounds = 4 * window.consecutive_limit;
        let (ctx, conv, probe) = scripted_context(Script::ManyToolRounds { rounds }).await;
        let ctx = ctx.with_tool_call_window(window);
        spawn_reactor(ctx.clone());

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the forced end", |blocks| {
            blocks.iter().any(|b| b.block_type == "status")
        })
        .await;

        let summons = blocks
            .iter()
            .find(|b| b.block_type == "text")
            .expect("the summoning message")
            .id;
        let errors: Vec<&str> = blocks
            .iter()
            .filter(|b| b.block_type == "tool_error")
            .map(|b| b.fields["error"].as_str().unwrap())
            .collect();
        assert_eq!(
            errors.len(),
            window.consecutive_limit,
            "one refusal per round after the window was spent, up to the limit — got {:?}",
            blocks
                .iter()
                .map(|b| b.block_type.as_str())
                .collect::<Vec<_>>()
        );
        assert!(
            errors
                .iter()
                .all(|error| error.starts_with(crate::agency::ToolError::RATE_LIMIT_PREFIX))
        );
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b.block_type == "tool_result")
                .count(),
            window.calls,
            "the window's allowed calls ran for real"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("tool_calls_exhausted".to_owned(), Some(summons))],
            "the forced end wrote the turn's end down, anchored on the turn"
        );
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            window.calls + window.consecutive_limit,
            "one request per round — the allowed calls, then the refusals — and \
             nothing after the last one — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // The conversation lives on: a fresh summons dispatches a turn of its
        // own, anchored on itself, and — the window still being hot — refuses
        // its way to a second forced end.
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "again".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the second forced end", |blocks| {
            blocks.iter().filter(|b| b.block_type == "status").count() == 2
        })
        .await;
        let fresh = blocks
            .iter()
            .filter(|b| b.block_type == "text")
            .nth(1)
            .expect("the second summoning message")
            .id;
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![
                ("tool_calls_exhausted".to_owned(), Some(summons)),
                ("tool_calls_exhausted".to_owned(), Some(fresh)),
            ],
            "the fresh turn carries a fresh anchor, never the ended turn's"
        );
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            window.calls + 2 * window.consecutive_limit,
            "a refused round each until the limit again — the window never \
             recovered, so nothing ran — then the second stand-down — got {:?}",
            probe.shapes.lock().unwrap()
        );
    }

    /// The same forced end at the seam, deterministically: the dispatch
    /// stands down, the marker lands anchored on the turn, the held identity
    /// is released only after that append, and NOTHING latches — then the
    /// next summons dispatches on a fresh anchor.
    #[tokio::test]
    async fn the_forced_end_releases_the_turn_without_latching() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        let ctx = ctx.with_tool_call_window(small_window());
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        refused_rounds(
            &ctx,
            conv,
            summons,
            "spent",
            small_window().consecutive_limit,
        )
        .await;

        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.open_turn = Some(summons);
        actor.handle_blocks_ready().await;

        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            0,
            "the dispatch stood down before it spent"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("tool_calls_exhausted".to_owned(), Some(summons))]
        );
        assert_eq!(actor.open_turn, None, "the append released the identity");
        assert!(!actor.read_latched.get(), "a decision is not an error");
        assert!(!actor.streaming, "nothing was dispatched");

        // A fresh summons behind the marker: read through it, dispatched, and
        // anchored on itself.
        let fresh = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "again".into())
            .await
            .unwrap();
        actor.handle_blocks_ready().await;
        assert!(
            actor.streaming,
            "the ended turn never stopped the conversation"
        );
        assert_eq!(actor.open_turn, Some(fresh), "a fresh turn, a fresh anchor");
        assert_eq!(actor.turn_anchor.get(), Some(fresh));
    }

    /// AC4 — an ordinary tool error resets the run. Four refusals, one
    /// ordinary failure resolved out of band on a call of the turn, one more
    /// refusal: the trailing run is ONE, the turn does not end, and the
    /// dispatch goes out as usual. The run is read as the id-ordered outcome
    /// subsequence, so it reads the same however the window moved meanwhile.
    ///
    /// Written at the seam rather than through a script on purpose: an
    /// out-of-band resolution landing between two refused rounds is not
    /// something a provider script can say, and the check reads the ledger
    /// either way.
    #[tokio::test]
    async fn an_ordinary_error_between_refusals_resets_the_run() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        let ctx = ctx.with_tool_call_window(small_window());
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        // One short of the limit: the run below must be the ordinary error's
        // reset, never a run that was already too short to end the turn.
        refused_rounds(
            &ctx,
            conv,
            summons,
            "spent",
            small_window().consecutive_limit - 1,
        )
        .await;

        let pending = ctx
            .store
            .insert_tool_call_block(
                crate::store::BlockDestination::anchored(conv, Some(summons)),
                crate::block::Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "ordinary".into(),
                    name: "boom".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        ctx.store
            .fail_tool_call_block(conv, "ordinary".into(), "scripted failure".into(), pending)
            .await
            .unwrap()
            .expect("the out-of-band failure resolves the call");
        refused_rounds(&ctx, conv, summons, "after", 1).await;

        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.open_turn = Some(summons);
        actor.handle_blocks_ready().await;

        assert!(
            status_markers(&ctx, conv).await.is_empty(),
            "one refusal behind an ordinary failure is not a run"
        );
        assert_eq!(actor.open_turn, Some(summons), "the turn lives on");
        assert!(actor.streaming, "and its continuation dispatched");
    }

    /// AC5's five-rule half — restart safety. The refusals landed before the
    /// stop, and a rebooted actor holds no turn identity at all; the check
    /// reads the RESOLVED anchor — here derived from the ledger the reopened
    /// store serves — so the forced end still fires. Path-backed, because the
    /// in-memory store cannot be reopened; the first handle is dropped to
    /// release it, and the rebooted runtime is driven by an append.
    #[tokio::test]
    async fn the_forced_end_survives_a_restart_that_cleared_the_held_identity() {
        let dir = crate::store::temp_dir("forced-end-restart");
        let db = dir.join("ledger.db");

        let (conv, summons) = {
            let (ctx, conv, _probe) =
                scripted_context_over(Store::open(&db).unwrap(), Script::CountOnly).await;
            let summons = ctx
                .store
                .insert_text_block(conv, crate::block::Role::User, "summons".into())
                .await
                .unwrap();
            refused_rounds(
                &ctx,
                conv,
                summons,
                "spent",
                small_window().consecutive_limit,
            )
            .await;
            (conv, summons)
        };

        let (ctx, _fresh_conv, probe) =
            scripted_context_over(Store::open(&db).unwrap(), Script::CountOnly).await;
        let ctx = ctx.with_tool_call_window(small_window());
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        assert_eq!(actor.open_turn, None, "a rebooted actor holds no identity");
        actor.handle_blocks_ready().await;

        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            0,
            "the reboot spent nothing"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("tool_calls_exhausted".to_owned(), Some(summons))],
            "the end is anchored on the turn the LEDGER still owed"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // ─── What a turn is offered (2026-09-01) ─────────────────────────────

    /// Dispatch one turn off a fresh summons and answer what the request was
    /// offered, in the order the definitions arrived.
    async fn offered_by_one_dispatch(
        ctx: &RuntimeContext<BlockKind, CoreEvent>,
        conv: i64,
        probe: &ComposedProbe,
    ) -> Vec<String> {
        ctx.store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;
        assert!(actor.streaming, "the turn dispatched");
        // The request crosses a channel into the provider's own task, so the
        // read waits for it to arrive instead of assuming it already has.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let offered = probe.offered.lock().unwrap().clone();
            if let [only] = offered.as_slice() {
                return only.clone();
            }
            assert!(
                offered.len() < 2,
                "exactly one request went out, got {offered:?}"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "timed out awaiting the dispatched request"
            );
            drop(offered);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // ─── The prompt is the head or the turn is refused (2026-09-02) ──────

    /// AC4 — a ledger whose first row is no system prompt buys no request:
    /// the turn fails on the bus, naming the conversation, and the provider
    /// never hears about it. Only a foreign or damaged ledger has that shape,
    /// since the store takes a prompt into an empty conversation and nowhere
    /// else, so the head's junction row is deleted in SQL here.
    #[tokio::test]
    async fn a_ledger_that_does_not_open_with_a_prompt_dispatches_nothing() {
        let (ctx, conv, probe) = scripted_context(Script::Prose).await;
        let head = ctx.store.list_blocks(conv).await.unwrap()[0].id;
        ctx.store
            .run(move |conn| {
                conn.execute(
                    "DELETE FROM conversation_blocks
                     WHERE conversation_id = ?1 AND block_id = ?2",
                    rusqlite::params![conv, head],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        ctx.store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        let mut events = ctx.bus.subscribe();

        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.handle_blocks_ready().await;

        assert!(!actor.streaming, "nothing was dispatched");
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            0,
            "the refusal spent nothing"
        );
        // The event names the conversation in its own typed field, which is
        // where every reader of the bus reads it; the prose says what went
        // wrong and repeats nothing the event already carries.
        let error = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(CoreEvent::StreamError {
                    conversation_id,
                    error,
                    ..
                }) = events.recv().await
                    && conversation_id == conv
                {
                    return error;
                }
            }
        })
        .await
        .expect("the refusal reaches the bus");
        assert!(
            error.contains("does not open with a system prompt"),
            "the error says what the ledger's shape is: {error}"
        );
    }

    /// AC4, the other side: a ledger that opens with its prompt dispatches
    /// exactly as it did before the rule existed.
    #[tokio::test]
    async fn a_ledger_that_opens_with_its_prompt_dispatches_as_before() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        assert_eq!(
            ctx.store.list_blocks(conv).await.unwrap()[0].block_type,
            "system_prompt",
            "the harness conversation opens with its prompt"
        );

        assert_eq!(
            offered_by_one_dispatch(&ctx, conv, &probe).await,
            ctx.runner()
                .registry()
                .names()
                .map(str::to_owned)
                .collect::<Vec<String>>(),
            "one request went out, carrying what a request carries"
        );
    }

    /// AC3 — the dispatch offers exactly the definitions the conversation's
    /// newest recorded choice names, and nothing else the registry holds.
    #[tokio::test]
    async fn a_recorded_choice_offers_exactly_the_tools_it_names() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        assert!(
            ctx.runner().registry().names().count() > 2,
            "the registry holds more than the choice names, or this proves nothing"
        );
        ctx.store
            .append_tool_choice(conv, vec!["echo".into(), "park".into()])
            .await
            .unwrap();

        assert_eq!(
            offered_by_one_dispatch(&ctx, conv, &probe).await,
            vec!["echo".to_owned(), "park".to_owned()]
        );
    }

    /// AC4 — an empty recorded choice offers no definitions at all, with a
    /// full registry loaded the whole time.
    #[tokio::test]
    async fn an_empty_recorded_choice_offers_nothing() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        assert!(ctx.runner().registry().names().count() > 0);
        ctx.store
            .append_tool_choice(conv, Vec::new())
            .await
            .unwrap();

        assert!(
            offered_by_one_dispatch(&ctx, conv, &probe).await.is_empty(),
            "an empty choice is a decision, and the decision is nothing"
        );
    }

    /// AC5 — a conversation that recorded no choice is offered every
    /// registered definition, unchanged from before the record existed. The
    /// record is an exposure decision; a ledger carrying none filters nothing.
    #[tokio::test]
    async fn a_conversation_with_no_recorded_choice_is_offered_the_registry() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        let registered: Vec<String> = ctx.runner().registry().names().map(str::to_owned).collect();

        assert_eq!(
            offered_by_one_dispatch(&ctx, conv, &probe).await,
            registered
        );
    }

    /// AC6 — a recorded name the registry does not hold is offered to nobody:
    /// the intersection is the whole rule, and the state is reachable, because
    /// a restart can register fewer tools than a persisted record names.
    #[tokio::test]
    async fn a_recorded_name_the_registry_lost_is_offered_to_nobody() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        ctx.store
            .append_tool_choice(conv, vec!["echo".into(), "departed".into()])
            .await
            .unwrap();

        assert_eq!(
            offered_by_one_dispatch(&ctx, conv, &probe).await,
            vec!["echo".to_owned()],
            "the name the registry no longer holds resolves to nothing"
        );
    }

    /// AC11 — a conversation with an empty choice cannot resolve anything the
    /// model calls, so each call is refused, and a run of those as long as the
    /// configured consecutive limit forces the turn to end: no request is
    /// spent, the end is written down anchored on the turn, and the held
    /// identity is released.
    #[tokio::test]
    async fn a_run_of_unresolvable_calls_in_a_toolless_conversation_ends_the_turn() {
        let (ctx, conv, probe) = scripted_context(Script::CountOnly).await;
        let limit = ctx.runner().window().consecutive_limit;
        let summons = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "summons".into())
            .await
            .unwrap();
        ctx.store
            .append_tool_choice(conv, Vec::new())
            .await
            .unwrap();

        // The model spends a round on a call each time, through the REAL
        // chokepoint: nothing resolves, so every one is refused.
        let agency = ctx.agency(conv);
        for round in 0..limit {
            let call = ctx
                .runner
                .insert_call(
                    &agency,
                    false,
                    format!("toolless-{round}"),
                    "echo".into(),
                    "{}".into(),
                    CallOrigin::streamed(None, Some(summons)),
                )
                .await
                .unwrap();
            ctx.runner.run_wakeup(&agency, false, call).await;
        }

        let blocks = ctx.store.list_blocks(conv).await.unwrap();
        let refusals: Vec<&Block> = blocks
            .iter()
            .filter(|block| block.block_type == "tool_error")
            .collect();
        assert_eq!(refusals.len(), limit, "one refusal per round");
        assert!(
            refusals.iter().all(|block| {
                block.fields["error"].as_str().unwrap()
                    == "this conversation has no tools, so no tool call can be answered."
                    && block.fields["refusal"].as_bool().unwrap()
            }),
            "each one says the conversation has no tools and records itself a refusal"
        );

        let (mut actor, _recheck) = bare_actor(conv, ctx.clone(), false);
        actor.open_turn = Some(summons);
        actor.handle_blocks_ready().await;

        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            0,
            "the dispatch stood down before it spent"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("tool_calls_exhausted".to_owned(), Some(summons))],
            "the turn's end is written down, anchored on the turn"
        );
        assert_eq!(actor.open_turn, None, "the append released the identity");
        assert!(!actor.streaming, "nothing was dispatched");
    }

    // ─── The per-tool windows (2026-08-30) ───────────────────────────────

    /// AC3 — the five-rule sees PER-TOOL refusals, through the real loop.
    /// `echo` alone is bound, at a single call per minute, through the public
    /// builder a consumer would use. So round one runs, every round after it
    /// is refused by the TOOL's window — with the tool's own text, not the
    /// conversation's — and after the fifth of those the would-be
    /// continuation stands down with no provider request, the turn's end
    /// written down and anchored on the turn. A model looping on one bounded
    /// tool ends its turn exactly as a burst does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_run_of_one_tools_refusals_ends_the_turn() {
        // The consecutive limit comes off the defaults themselves, the
        // `small_window` discipline: change the operator's limit and this pin
        // exercises the new one.
        let defaults = crate::tools::runner::ToolCallWindow::default();
        // More rounds than the forced end can consume, so the script never
        // runs out before the rule fires.
        let rounds = 4 * defaults.consecutive_limit;
        let (ctx, conv, probe) = scripted_context(Script::ManyToolRounds { rounds }).await;
        // The conversation's own window, built UNSPENT for this run: its
        // calls bound is every round the script brings, farther than the
        // forced end ever lets the run reach. The premise is this test's own
        // — nothing is borrowed from the shipped numbers, so nothing here
        // needs an assert about them — and every refusal below names the
        // tool's window in its own text.
        let ctx = ctx
            .with_tool_call_window(crate::tools::runner::ToolCallWindow {
                calls: rounds,
                ..defaults
            })
            .with_tool_window("echo", 1, 60);
        spawn_reactor(ctx.clone());

        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "hi".into(),
            }],
        });
        let blocks = await_ledger(&ctx, conv, "the forced end", |blocks| {
            blocks.iter().any(|b| b.block_type == "status")
        })
        .await;

        let summons = blocks
            .iter()
            .find(|b| b.block_type == "text")
            .expect("the summoning message")
            .id;
        let errors: Vec<&str> = blocks
            .iter()
            .filter(|b| b.block_type == "tool_error")
            .map(|b| b.fields["error"].as_str().unwrap())
            .collect();
        assert_eq!(
            errors.len(),
            defaults.consecutive_limit,
            "one refusal per round after the tool's window was spent, up to the limit — \
             got {:?}",
            blocks
                .iter()
                .map(|b| b.block_type.as_str())
                .collect::<Vec<_>>()
        );
        assert!(
            errors.iter().all(|error| *error
                == crate::agency::ToolError::per_tool_rate_limit_refusal("echo", 1, 60)),
            "every refusal is the TOOL's own, naming the tool and its numbers — got {errors:?}"
        );
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b.block_type == "tool_result")
                .count(),
            1,
            "the tool's one allowed call ran for real"
        );
        assert_eq!(
            status_markers(&ctx, conv).await,
            vec![("tool_calls_exhausted".to_owned(), Some(summons))],
            "one window's refusals or another's, the forced end writes the same \
             turn end, anchored on the turn"
        );
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1 + defaults.consecutive_limit,
            "one request per round — the allowed call, then the refusals — and nothing \
             after the last one — got {:?}",
            probe.shapes.lock().unwrap()
        );
    }

    // ─── The tool that ends the turn (2026-08-30) ────────────────────────

    /// A second summons after a park round: it dispatches, and its answer
    /// anchors on ITSELF — the held identity the park turn left behind was
    /// revalidated against the ledger at the reuse and dropped, so nothing of
    /// the ended turn rides into this one.
    async fn assert_the_next_summons_takes_a_fresh_anchor(
        ctx: &RuntimeContext<BlockKind, CoreEvent>,
        conv: i64,
        parked_turn: i64,
    ) {
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "again".into(),
            }],
        });
        let blocks = await_ledger(ctx, conv, "the second summons' answer", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        let again = blocks
            .iter()
            .find(|b| b.fields.get("content") == Some(&json!("again")))
            .expect("the second summons");
        let answer = blocks
            .iter()
            .rfind(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
            .expect("its answer");
        assert_ne!(
            answer.dispatch_anchor,
            Some(parked_turn),
            "LEAK: the summons after a park round inherited the ended turn's identity"
        );
        assert_eq!(
            answer.dispatch_anchor,
            Some(again.id),
            "the fresh summons anchors its turn on itself"
        );
    }

    /// AC1 — a registered ends-turn tool, called and resolved, ENDS the turn:
    /// the round's resolution is stamped, no continuation is dispatched (the
    /// request counter stays where the opening turn left it), nothing else is
    /// appended — no marker, because the stamped row IS the stored closure —
    /// and the NEXT summons takes a fresh anchor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_park_round_ends_the_turn_and_the_next_summons_starts_fresh() {
        let (ctx, conv, probe) = composed_runtime(Script::ParkRound { tool: "park" }).await;
        let mut closed_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "summons".into(),
            }],
        });

        await_ledger(&ctx, conv, "the park round's resolution", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_result")
        })
        .await;
        // The window a wrongly-summoned continuation fires in is bounded by
        // the round's own close, not by a timer: the resolution has landed
        // and the stream has ended, so the release rule has answered.
        await_stream_closed(&mut closed_rx, conv).await;
        let blocks = ctx.store.list_blocks(conv).await.unwrap();

        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "the park resolution dispatched no continuation — got {:?}",
            probe.shapes.lock().unwrap()
        );
        let shape: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            shape,
            vec![
                "system_prompt",
                "date_marker",
                "text",
                "tool_call",
                "tool_result"
            ],
            "the turn ends on its resolution, with no marker beside it"
        );
        assert_eq!(
            results_with_stamps(&blocks),
            vec![("nothing to do".to_owned(), true)],
            "and that resolution carries the stamp"
        );
        let summons = user_block(&blocks).id;
        assert_anchors(&blocks, summons);

        assert_the_next_summons_takes_a_fresh_anchor(&ctx, conv, summons).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "one request for the park round, one for the fresh summons — got {:?}",
            probe.shapes.lock().unwrap()
        );
    }

    /// AC1's interleaving — the close ran BEFORE the park result committed.
    /// The stream closes with the call still owed, so the release rule keeps
    /// the identity on its unresolved-call arm; the stamped resolution then
    /// lands behind the close, owing nobody, and no signal ever reaches a
    /// release site. The stale hold is dropped where it is READ — at the next
    /// summons' anchor resolution — which is the whole reason the reuse
    /// revalidates.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_park_result_landing_after_the_close_still_ends_the_turn() {
        let (ctx, conv, probe) = composed_runtime(Script::ParkRound { tool: "held_park" }).await;
        let mut closed_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "summons".into(),
            }],
        });

        await_ledger(&ctx, conv, "the park call", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_call")
        })
        .await;
        // The ordering under probe is OBSERVED, not waited out: the stream's
        // own close is awaited, and only then is the held result released, so
        // the identity crossing the close is provably the held one.
        await_stream_closed(&mut closed_rx, conv).await;
        assert!(
            !ctx.store
                .list_blocks(conv)
                .await
                .unwrap()
                .iter()
                .any(|b| b.block_type == "tool_result"),
            "the close really did run first — the result is still held"
        );

        probe.finish.notify_one();
        let blocks = await_ledger(&ctx, conv, "the held park resolution", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_result")
        })
        .await;
        assert_eq!(
            results_with_stamps(&blocks),
            vec![("nothing to do".to_owned(), true)]
        );

        // Whether the resolution behind the close summoned anything is read
        // off the counter at the END: a continuation it wrongly dispatched
        // would sit between these two requests, so the count is the window.
        let summons = user_block(&blocks).id;
        assert_the_next_summons_takes_a_fresh_anchor(&ctx, conv, summons).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "the resolution behind the close summoned nothing — one request \
             for the round, one for the fresh summons — got {:?}",
            probe.shapes.lock().unwrap()
        );
    }

    /// AC4 — a park call beside a sibling silences only ITSELF. The script
    /// controls the ordering: the sibling's tool holds its result, so the
    /// sibling's outcome lands LAST, its turn is still owed a continuation,
    /// and that continuation is summoned — anchored on the original summons,
    /// because the shared fold counts the sibling's outcome while excluding
    /// the park stamp on both sides of the comparison.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_park_call_beside_a_sibling_silences_only_its_own_outcome() {
        let (ctx, conv, probe) = composed_runtime(Script::ParkBesideSibling).await;
        let mut closed_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "summons".into(),
            }],
        });

        await_ledger(&ctx, conv, "the park resolution", |blocks| {
            blocks
                .iter()
                .any(|b| b.fields.get("content") == Some(&json!("nothing to do")))
        })
        .await;
        // Bounded by the round's own close rather than by a timer: the park
        // outcome has landed and the stream has ended, so the release rule
        // has had its say while the sibling is still owed.
        await_stream_closed(&mut closed_rx, conv).await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            1,
            "the park outcome alone summons nothing while its sibling is owed — got {:?}",
            probe.shapes.lock().unwrap()
        );

        // The sibling's outcome lands last, and IT owes the continuation.
        probe.finish.notify_one();
        let blocks = await_ledger(&ctx, conv, "the sibling's continuation", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "the sibling's outcome summoned exactly one continuation — got {:?}",
            probe.shapes.lock().unwrap()
        );
        assert_eq!(
            results_with_stamps(&blocks),
            vec![
                ("nothing to do".to_owned(), true),
                ("echoed".to_owned(), false),
            ],
            "the park result is stamped, the sibling's is not, and the sibling's is last"
        );
        let summons = user_block(&blocks).id;
        assert_anchors(&blocks, summons);
        let closing = blocks
            .iter()
            .rfind(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
            .unwrap();
        assert_eq!(
            closing.dispatch_anchor,
            Some(summons),
            "the continuation is still the original turn's"
        );
    }

    /// AC8 — an addressed message absorbed while an ends-turn round's window
    /// was open is NEVER buried. The script holds both ends of that window
    /// open, so the ledger takes the one order that strands the message: the
    /// call, then the absorbed line, then the stamped resolution on top of
    /// it, then the close. The turn-ending tail asks for nothing, and the
    /// close's own re-check is the last re-engagement a non-latching close
    /// has — so unless the frontier reads THROUGH that tail, the member's
    /// question waits for some unrelated third party to write.
    ///
    /// What the pin claims: after the close, with NO further inbound, the
    /// absorbed line's own turn fires, carries the line, and anchors on
    /// itself. Every ordering it rests on is awaited on a real event — the
    /// call's block, the resolution's block, the stream's own close — and
    /// none of them is slept for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_message_absorbed_into_a_park_round_is_never_buried() {
        let (ctx, conv, probe) = composed_runtime(Script::ParkRoundOverAnAbsorbedMessage).await;
        let mut closed_rx = ctx.bus.subscribe();
        ctx.bus.emit(CoreEvent::BlocksAppended {
            conversation_id: conv,
            blocks: vec![InputBlock::Text {
                content: "summons".into(),
            }],
        });

        // The window is open on both ends: the call is recorded, its result
        // waits on the probe's finish, and the stream's close waits behind
        // that.
        await_ledger(&ctx, conv, "the park call", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_call")
        })
        .await;

        // The member writes INTO that window, so the stamped resolution lands
        // on top of the message rather than under it.
        let absorbed = ctx
            .store
            .insert_text_block(conv, crate::block::Role::User, "absorbed".into())
            .await
            .unwrap();

        probe.finish.notify_one();
        let blocks = await_ledger(&ctx, conv, "the stamped resolution", |blocks| {
            blocks.iter().any(|b| b.block_type == "tool_result")
        })
        .await;
        let line = blocks.iter().position(|b| b.id == absorbed);
        let tail = blocks.iter().position(|b| b.block_type == "tool_result");
        assert!(
            line < tail && line.is_some(),
            "the shape under probe: the absorbed line sits BEHIND the turn-ending tail — got {:?}",
            blocks
                .iter()
                .map(|b| b.block_type.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            results_with_stamps(&blocks),
            vec![("nothing to do".to_owned(), true)],
            "and the tail really is the turn-ending one"
        );

        // The round closes over the buried line, and nothing else arrives.
        probe.release.notify_one();
        await_stream_closed(&mut closed_rx, conv).await;

        let blocks = await_ledger(&ctx, conv, "the absorbed line's own turn", |blocks| {
            blocks
                .iter()
                .any(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
        })
        .await;
        assert_eq!(
            probe.requests.load(Ordering::SeqCst),
            2,
            "the buried line summoned its own turn with no further inbound — got {:?}",
            probe.shapes.lock().unwrap()
        );
        assert!(
            request_carries(&probe.seen.lock().unwrap()[1], "absorbed"),
            "and that turn carries the line it was summoned for"
        );
        let answer = blocks
            .iter()
            .rfind(|b| b.block_type == "text" && b.fields["content"] == json!("done"))
            .expect("the absorbed line's answer");
        assert_eq!(
            answer.dispatch_anchor,
            Some(absorbed),
            "the line's turn anchors on itself, never on the turn that ended over it"
        );
    }

    // ─── Unit 51: an actor set lives exactly as long as its conversation ──

    /// Drain every `BlocksChanged` the watcher emitted, as
    /// `(conversation_id, block_id)` pairs, after giving it a moment to run.
    /// The watcher is the only emitter in these tests — no reactor is
    /// spawned — so what arrives here is exactly what it attributed.
    async fn blocks_changed(
        rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>,
    ) -> Vec<(i64, i64)> {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut seen = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let CoreEvent::BlocksChanged {
                conversation_id,
                block_id,
            } = event
            {
                seen.push((conversation_id, block_id));
            }
        }
        seen
    }

    /// F1/AC3 — the deletion itself ends the set, and nothing commanded it.
    ///
    /// The conversation is deleted while its set sits idle, so the ONLY change
    /// the scheduler can wake on is the deletion. It wakes, its drive reads the
    /// cursor as `None`, and the set ends: the actor's mailbox closes, which is
    /// also the fact the reactor reads to forget the route.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_deletion_itself_ends_the_actor_set() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;

        let tx = <ConversationActor<BlockKind, CoreEvent> as PerConversationActor<
            BlockKind,
            CoreEvent,
        >>::spawn(conv, ctx.clone());
        // Boot-latched sets never drive, so nothing would ever read the
        // cursor. Release it, then let the set settle into rest.
        tx.send(CoreEvent::UnlatchRequested {
            conversation_id: conv,
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !tx.is_closed(),
            "the set of a conversation that exists stays alive"
        );

        ctx.store.delete_conversation(conv).await.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !tx.is_closed() {
            assert!(
                std::time::Instant::now() < deadline,
                "the deleted conversation's actor set never ended"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// F1/AC3 — a LATCHED set learns it too (2026-09-01).
    ///
    /// The latch stops the ratchet; it never grants a set a life past its
    /// conversation. This set is never unlatched — it stays boot-latched, the
    /// same state a stream failure leaves behind — so the only read that can
    /// end it is the existence read a latched tick makes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_latched_set_ends_when_its_conversation_is_deleted() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;

        let tx = <ConversationActor<BlockKind, CoreEvent> as PerConversationActor<
            BlockKind,
            CoreEvent,
        >>::spawn(conv, ctx.clone());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !tx.is_closed(),
            "a latched set of a conversation that exists stays alive"
        );

        ctx.store.delete_conversation(conv).await.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !tx.is_closed() {
            assert!(
                std::time::Instant::now() < deadline,
                "a latched set outlived its conversation"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// F1 — the scheduler reports the death rather than acting on it, and it
    /// reports it off the same drive that does everything else.
    #[tokio::test]
    async fn a_gone_conversation_signals_the_actor_and_drives_nothing() {
        let o = Oracle::new().await;
        o.ctx
            .store
            .delete_conversation(o.ctx.conversation_id)
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        assert_eq!(
            scheduler_tick::<BlockKind, _>(&o.ctx, false, &tx).await,
            None,
            "a gone conversation publishes no drive outcome"
        );
        assert!(
            matches!(rx.try_recv(), Ok(SchedulerSignal::ConversationGone)),
            "the actor is told the conversation is gone"
        );

        // Latched, the same tick reports the same death: the ratchet does not
        // run, the existence read does.
        let (latched_tx, mut latched_rx) = tokio::sync::mpsc::unbounded_channel();
        assert_eq!(
            scheduler_tick::<BlockKind, _>(&o.ctx, true, &latched_tx).await,
            None,
            "a latched tick publishes no drive outcome"
        );
        assert!(
            matches!(latched_rx.try_recv(), Ok(SchedulerSignal::ConversationGone)),
            "a latched set is told the conversation is gone too"
        );
    }

    /// F1 — an ended set is forgotten before anything routes, so a conversation
    /// id the database hands out again cannot land in the dead set standing in
    /// its place.
    #[tokio::test]
    async fn routing_forgets_a_set_whose_actor_ended() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;

        let (dead_tx, dead_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(dead_rx);
        let mut routes: HashMap<i64, Vec<ActorEntry>> = HashMap::new();
        routes.insert(
            conv,
            vec![ActorEntry {
                tx: dead_tx,
                accepts: |_| true,
            }],
        );

        // A global event routes to every live set and spawns none.
        route_event(&ctx, &mut routes, &CoreEvent::UnlatchAll {});
        assert!(
            routes.is_empty(),
            "the ended set is forgotten, not carried forever"
        );

        // The same id arriving again gets a set of its own.
        route_event(
            &ctx,
            &mut routes,
            &CoreEvent::UnlatchRequested {
                conversation_id: conv,
            },
        );
        assert_eq!(routes.len(), 1, "a fresh set is spawned for the id");
        assert!(
            !routes[&conv][0].tx.is_closed(),
            "and it is a live one, not the corpse"
        );
    }

    // ─── Unit 51: a change event names the row's own conversation ─────────

    /// F2/AC4 — a junction change is attributed from its own row, so a fork's
    /// copies announce to the FORK and nothing to the conversation they were
    /// copied from.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_junction_change_names_the_conversation_on_its_own_row() {
        let (ctx, source, _probe) = scripted_context(Script::CountOnly).await;
        let block = ctx
            .store
            .insert_text_block(source, crate::block::Role::User, "hello".into())
            .await
            .unwrap();

        let mut rx = ctx.bus.subscribe();
        spawn_block_watcher(&ctx);

        let fork = ctx
            .store
            .fork_conversation(source, block, crate::store::ModelOverride::default())
            .await
            .unwrap();

        let seen = blocks_changed(&mut rx).await;
        assert!(
            seen.contains(&(fork, block)),
            "the fork's own junction row announces to the fork; saw {seen:?}"
        );
        assert!(
            !seen.iter().any(|(conversation, _)| *conversation == source),
            "and nothing a fork writes is attributed to the source; saw {seen:?}"
        );
    }

    /// F2/AC4 — a block joined to more than one conversation is announced to
    /// EVERY one of them: the row that changed names no conversation, so the
    /// joins are what answer, all of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shared_blocks_change_is_announced_to_every_conversation() {
        let (ctx, source, _probe) = scripted_context(Script::CountOnly).await;
        let block = ctx
            .store
            .insert_streaming_block(source, crate::block::Role::Assistant)
            .await
            .unwrap();
        let fork = ctx
            .store
            .fork_conversation(source, block, crate::store::ModelOverride::default())
            .await
            .unwrap();

        let mut rx = ctx.bus.subscribe();
        spawn_block_watcher(&ctx);

        ctx.store
            .append_to_block_by_id(block, "block_text", "hi".into(), "2026-09-01".into())
            .await
            .unwrap();

        let seen = blocks_changed(&mut rx).await;
        assert!(
            seen.contains(&(source, block)) && seen.contains(&(fork, block)),
            "both conversations reading the block hear about it; saw {seen:?}"
        );
    }

    /// F2/AC4 — a junction row that is gone by the time the watcher reads it
    /// attributes nothing and announces nothing. Deletion is the ordinary way
    /// that happens: every one of the conversation's junction rows fires a
    /// change whose row no longer exists.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_junction_row_gone_by_read_time_announces_nothing() {
        let (ctx, conv, _probe) = scripted_context(Script::CountOnly).await;
        ctx.store
            .insert_text_block(conv, crate::block::Role::User, "hello".into())
            .await
            .unwrap();

        let mut rx = ctx.bus.subscribe();
        spawn_block_watcher(&ctx);

        ctx.store.delete_conversation(conv).await.unwrap();

        assert_eq!(
            blocks_changed(&mut rx).await,
            Vec::new(),
            "a row read after its delete announces nothing"
        );
    }
}
