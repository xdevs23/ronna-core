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

use crate::agency::{AgencyCtx, ratchet, redispatch};
use crate::bus::{EventBus, RuntimeEvent};
use crate::event::{AsCoreEvent, CoreEvent};
use crate::providers::types::ReasoningLevel;
use crate::providers::{ModelSelector, ProviderRegistry, ProviderRequest, ProviderTx};
use crate::reactive;
use crate::reactivity::{ReadSignal, WriteSignal, create_signal};
use crate::store::Store;
use crate::tools::{ToolRegistry, ToolRunner, submit_approval};
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
pub struct RuntimeContext<E> {
    store: Store,
    bus: Arc<EventBus<E>>,
    providers: Arc<ProviderRegistry>,
    runner: Arc<ToolRunner<E>>,
}

impl<E> Clone for RuntimeContext<E> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            bus: Arc::clone(&self.bus),
            providers: Arc::clone(&self.providers),
            runner: Arc::clone(&self.runner),
        }
    }
}

impl<E> RuntimeContext<E> {
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
    pub fn runner(&self) -> &Arc<ToolRunner<E>> {
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
trait PerConversationActor<E> {
    fn spawn(id: i64, ctx: RuntimeContext<E>) -> tokio::sync::mpsc::UnboundedSender<CoreEvent>;
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
        fn spawn_actor_set<E: RuntimeEvent + AsCoreEvent>(
            id: i64,
            ctx: &RuntimeContext<E>,
        ) -> Vec<ActorEntry> {
            vec![$(
                ActorEntry {
                    tx: <$actor<E> as PerConversationActor<E>>::spawn(id, ctx.clone()),
                    accepts: <$actor<E> as PerConversationActor<E>>::accepts,
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
struct ConversationActor<E> {
    id: i64,
    ctx: RuntimeContext<E>,
    mailbox: tokio::sync::mpsc::UnboundedReceiver<CoreEvent>,
    write_latched: WriteSignal<bool>,
    read_latched: ReadSignal<bool>,
    blocks_ready: tokio::sync::mpsc::UnboundedReceiver<()>,
    provider_tx: Option<ProviderTx>,
    streaming: bool,
}

impl<E: RuntimeEvent + AsCoreEvent> ConversationActor<E> {
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
            CoreEvent::StreamDone { stop_reason, .. } => self.handle_stream_done(stop_reason),
            CoreEvent::StreamError { .. } => self.handle_stream_error(),
            CoreEvent::StreamClosed { .. } => self.handle_stream_closed(),
            _ => {}
        }
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
                None,
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
        self.streaming = false;

        if let Some(tx) = &self.provider_tx {
            let _ = tx.send(ProviderRequest::Interrupt);
        }

        let store = &self.ctx.store;
        let _ = store
            .insert_status_block(self.id, "interrupted".into(), None)
            .await;

        tracing::info!(conversation_id = self.id, "interrupted, latched");
    }

    fn handle_stream_done(&mut self, stop_reason: Option<crate::types::StopReason>) {
        // Stop reason is recorded and surfaced data — it drives no
        // continuation; that derives from the ledger via the frontier gate.
        settle_stream_end(&mut self.streaming, &self.write_latched, false);
        tracing::info!(conversation_id = self.id, ?stop_reason, "stream done");
    }

    fn handle_stream_error(&mut self) {
        settle_stream_end(&mut self.streaming, &self.write_latched, true);
        tracing::info!(conversation_id = self.id, "stream error, latched");
    }

    fn handle_stream_closed(&mut self) {
        settle_stream_end(&mut self.streaming, &self.write_latched, false);
        tracing::info!(conversation_id = self.id, "stream closed");
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
        if self.streaming {
            tracing::debug!(
                conversation_id = self.id,
                "handle_blocks_ready: already streaming"
            );
            return;
        }

        // Known window, kept open on purpose (2026-08-21): a queued owed-turn
        // signal can outlive the turn it announced — the scheduler re-signals
        // on every owed tick, and a turn that completes before its queue
        // drains leaves stale signals behind, which fire here against an
        // already-answered ledger. The source carries the same window. The
        // fix — re-reading the frontier at delivery and standing down when
        // the tail no longer awaits the model — is a runtime improvement, and
        // the extraction spec defers those until after Stage 3 so the moved
        // suite keeps meaning something as an equivalence check.
        let Some(provider_tx) = self.ensure_provider().await else {
            tracing::warn!(
                conversation_id = self.id,
                "handle_blocks_ready: no provider available"
            );
            return;
        };

        // No pending-call guard here: a dangling call parks the cursor and the
        // frontier gate never fires — this signal only arrives against a
        // complete ledger.
        let store = &self.ctx.store;

        let blocks = match store.list_blocks(self.id).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(conversation_id = self.id, error = %e, "handle_blocks_ready: list_blocks failed");
                return;
            }
        };
        if blocks.is_empty() {
            tracing::warn!(
                conversation_id = self.id,
                "handle_blocks_ready: list_blocks returned empty"
            );
            return;
        }

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

        let tool_defs = self.ctx.runner.registry().definitions();
        // The stored form is the level's canonical key; a key this build does
        // not know defers to the provider's own default rather than failing
        // the turn.
        let reasoning = conv.reasoning.as_deref().and_then(ReasoningLevel::from_key);

        if let Err(e) = provider_tx.send(ProviderRequest::Stream {
            blocks,
            model: ModelSelector::Specific(conv.model.external_id),
            tools: tool_defs,
            reasoning,
        }) {
            tracing::error!(conversation_id = self.id, error = ?e, "handle_blocks_ready: provider channel closed");
            return;
        }
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

        crate::ingestion::spawn_channel(
            self.id,
            self.ctx.clone(),
            provider_rx,
            self.read_latched.clone(),
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

/// The stream-ending state changes shared by the done/error/closed handlers —
/// the single latch-policy point for stream endings.
///
/// The latch is orthogonal to the ledger: a normal stream end releases the
/// streaming flag and NOTHING else — the resting state of a finished
/// conversation is unlatched + ledger owes nothing; continuation is the
/// frontier gate's read, never a re-latch. Only a stream error latches: a
/// request we built wrong fails identically on every retry, so it latches and
/// surfaces instead of auto-retrying.
fn settle_stream_end(streaming: &mut bool, latch: &WriteSignal<bool>, errored: bool) {
    *streaming = false;
    if errored {
        latch.set(true);
    }
}

/// One scheduler tick: rest while latched (the whole ratchet family runs only
/// unlatched), otherwise drive the cursor, signal the actor when the frontier
/// gate owes a turn, and run the redispatch walk — which rides the same tick
/// but is ungated by the model-turn axis, so deferred work resumes even when
/// no turn is owed. Returns the drive's Outcome for the state broadcaster to
/// consume; `None` when nothing was driven.
pub(crate) async fn scheduler_tick<E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    latched: bool,
    blocks_ready: &tokio::sync::mpsc::UnboundedSender<()>,
) -> Option<ratchet::Outcome> {
    if latched {
        return None;
    }
    let outcome = match ratchet::drive(ctx).await {
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
    if let Err(e) = redispatch::walk(ctx).await {
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
async fn run_scheduler<E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    read_latched: ReadSignal<bool>,
    blocks_ready: tokio::sync::mpsc::UnboundedSender<()>,
    write_outcome: WriteSignal<Option<ratchet::Outcome>>,
) {
    let agency = ctx.agency(conv_id);
    let db_changes = ctx.store.changes.watcher();

    reactive! {
        let latched = read_latched.get();
        db_changes.react();

        if let Some(outcome) = scheduler_tick(&agency, latched, &blocks_ready).await {
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
async fn run_state_broadcaster<E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
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
fn spawn_tool_pipeline<E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: &RuntimeContext<E>,
    latched: &ReadSignal<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = vec![tokio::spawn(run_executor(
        conv_id,
        ctx.clone(),
        latched.clone(),
    ))];
    for handler in ctx.runner.registry().handlers() {
        if let Some(h) = handler.spawn_reactor(ctx.agency(conv_id), latched.clone()) {
            handles.push(h);
        }
    }
    handles
}

/// The runner's wakeup loop: subscribes to the bus and feeds every
/// [`CoreEvent::ToolCallReady`] for this conversation to the chokepoint. The
/// latch is read at delivery and handed to the runner, which DROPS a latched
/// wakeup rather than deferring it — the latch is a full short-circuit: the
/// parked call re-emits on the next unlatched tick, the same recovery that
/// covers a wakeup lost to lag; the ratchet is the retry loop.
async fn run_executor<E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    latched: ReadSignal<bool>,
) {
    let agency = ctx.agency(conv_id);
    let mut rx = ctx.bus.subscribe();

    loop {
        match rx.recv().await {
            Ok(event) => {
                if let Some(&CoreEvent::ToolCallReady {
                    conversation_id,
                    call_block_id,
                }) = event.as_core()
                    && conversation_id == conv_id
                {
                    ctx.runner
                        .run_wakeup(&agency, latched.get(), call_block_id)
                        .await;
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

impl<E: RuntimeEvent + AsCoreEvent> PerConversationActor<E> for ConversationActor<E> {
    fn spawn(id: i64, ctx: RuntimeContext<E>) -> tokio::sync::mpsc::UnboundedSender<CoreEvent> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Boot-latched: nothing drives a conversation until an explicit intent
        // (an append, a promotion, an unlatch) releases it, so a process
        // restart cannot fire turns out of a ledger nobody asked to resume.
        let (read_latched, write_latched) = create_signal(true);

        let (blocks_ready_tx, blocks_ready_rx) = tokio::sync::mpsc::unbounded_channel();
        let (read_outcome, write_outcome) = create_signal(None::<ratchet::Outcome>);

        let scheduler = tokio::spawn(run_scheduler(
            id,
            ctx.clone(),
            read_latched.clone(),
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
            provider_tx: None,
            streaming: false,
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
pub fn spawn_reactor<E: RuntimeEvent + AsCoreEvent>(ctx: RuntimeContext<E>) {
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
fn spawn_conversations_watcher<E: RuntimeEvent>(ctx: &RuntimeContext<E>) {
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
/// The table list below is hardcoded for the same reason the store's
/// change-hook allowlist is, and Stage 3 replaces both with content-table
/// descriptors contributed by the kind itself. Until then it names the tables
/// whose rows a consumer re-renders live — the header, the junction, and the
/// content of the kinds that stream or resolve.
fn spawn_block_watcher<E: RuntimeEvent>(ctx: &RuntimeContext<E>) {
    let db_changes = ctx.store.changes.consumer();
    let store = ctx.store.clone();
    let bus = Arc::clone(&ctx.bus);

    tokio::spawn(async move {
        reactive!(db_changes, change, {
            if change.table == "blocks"
                || change.table == "conversation_blocks"
                || change.table == "block_text"
                || change.table == "block_thinking"
                || change.table == "block_code"
                || change.table == "block_tool_call"
                || change.table == "block_tool_result"
                || change.table == "block_status"
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
fn route_event<E: RuntimeEvent + AsCoreEvent>(
    ctx: &RuntimeContext<E>,
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

    use crate::agency::ratchet::oracle::Oracle;
    use crate::block::Block;
    use crate::providers::types::ToolDefinition;
    use crate::providers::{
        BoxFuture, LlmError, ModelInfo, ProviderModule, ProviderResponse, ProviderRx, StreamEvent,
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
        assert_eq!(scheduler_tick(&o.ctx, true, &tx).await, None);

        assert!(rx.try_recv().is_err(), "no blocks_ready while latched");
        o.expect_silence(); // ratchet not invoked — no wakeup emitted
        assert_eq!(o.cursor().await, 0, "nothing persisted while latched");
    }

    #[tokio::test]
    async fn unlatched_tick_drives_and_signals_the_owed_turn() {
        let mut o = Oracle::new().await;
        let user = o.user_text("hi").await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outcome = scheduler_tick(&o.ctx, false, &tx).await.unwrap();
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
        let outcome = scheduler_tick(&o.ctx, false, &tx).await.unwrap();
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
        assert_eq!(scheduler_tick(&o.ctx, true, &tx).await, None);
        assert!(rx.try_recv().is_err());
        assert_eq!(o.cursor().await, 0, "the latched tick appended nothing");

        let outcome = scheduler_tick(&o.ctx, false, &tx).await.unwrap();
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

        let runner: ToolRunner<CoreEvent> = ToolRunner::new(Arc::new(ToolRegistry::new()));
        let block_id = runner
            .insert_call(
                &o.ctx,
                true,
                "raced".into(),
                "read_file".into(),
                "{}".into(),
                None,
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

    /// A normal stream end (done or closed) never re-latches. The done/closed
    /// handlers delegate here with errored = false, so after a stream done
    /// the latch signal is unchanged.
    #[test]
    fn stream_done_never_relatches() {
        let (latched, write_latched) = create_signal(false);
        let mut streaming = true;
        settle_stream_end(&mut streaming, &write_latched, false);
        assert!(!streaming, "the streaming flag is released");
        assert!(
            !latched.get(),
            "the latch is untouched — no unlatch-dance needed"
        );
    }

    /// A stream error IS a latch mover — a request we built wrong fails
    /// identically on every retry, so it latches and surfaces.
    #[test]
    fn stream_error_latches() {
        let (latched, write_latched) = create_signal(false);
        let mut streaming = true;
        settle_stream_end(&mut streaming, &write_latched, true);
        assert!(!streaming);
        assert!(latched.get());
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
        let confirmed = scheduler_tick(&o.ctx, false, &tx).await.unwrap();
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
        let echo = scheduler_tick(&o.ctx, false, &tx).await.unwrap();
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
        /// Every request is counted and left open — the stream never speaks.
        /// The double-turn probe: any second request is a defect, not a retry.
        CountOnly,
    }

    /// A provider module that answers from a script instead of a wire. It
    /// stands exactly where the runtime's own infrastructure ends: requests
    /// arrive through the real bind seam and responses travel the real
    /// ingestion path.
    struct ScriptedProvider {
        script: Script,
        requests: Arc<AtomicUsize>,
        /// The block-type shape of every request received, for diagnostics:
        /// a spurious request is only debuggable by what it carried.
        shapes: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
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
            tokio::spawn(async move {
                while let Some(request) = req_rx.recv().await {
                    let ProviderRequest::Stream { blocks, tools, .. } = request else {
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
                        .push(blocks.iter().map(|b| b.block_type.clone()).collect());
                    requests.fetch_add(1, Ordering::SeqCst);
                    // Scripted by ledger content, not arrival order: a turn
                    // whose ledger already carries the answered call gets the
                    // closing prose, the opening turn gets the call.
                    let answered = blocks.iter().any(|b| b.block_type == "tool_result");
                    let events: Vec<StreamEvent> = match (script, answered) {
                        (Script::CountOnly, _) => continue,
                        (Script::ToolCallThenText, false) => vec![
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
                        ],
                        (Script::ToolCallThenText, true) => vec![
                            StreamEvent::TextBlockStart,
                            StreamEvent::TextDelta {
                                text: "done".into(),
                            },
                            StreamEvent::MessageEnd {
                                usage: crate::providers::Usage::default(),
                                stop_reason: StopReason::EndTurn,
                            },
                        ],
                    };
                    for event in events {
                        let _ = resp_tx.send(ProviderResponse::Event(event));
                    }
                }
            });
            (req_tx, resp_rx)
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

    /// The whole runtime wired up over a scripted provider, plus its
    /// conversation id and the provider's request counter.
    struct ComposedProbe {
        requests: Arc<AtomicUsize>,
        shapes: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    }

    async fn composed_runtime(script: Script) -> (RuntimeContext<CoreEvent>, i64, ComposedProbe) {
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
        let mut providers = ProviderRegistry::new();
        providers.register(Box::new(ScriptedProvider {
            script,
            requests: Arc::clone(&requests),
            shapes: Arc::clone(&shapes),
        }));
        let mut tools = ToolRegistry::new();
        tools.register("echo", EchoTool);

        let ctx = RuntimeContext::new(
            store,
            Arc::new(EventBus::<CoreEvent>::new()),
            Arc::new(providers),
            Arc::new(tools),
        );
        spawn_reactor(ctx.clone());
        (ctx, conv, ComposedProbe { requests, shapes })
    }

    /// Poll the ledger until `accept` says it is the shape awaited, with a
    /// deadline so a stall is a named failure rather than a hung suite.
    async fn await_ledger(
        ctx: &RuntimeContext<CoreEvent>,
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
}
