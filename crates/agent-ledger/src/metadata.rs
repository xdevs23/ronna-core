//! The metadata worker — derived conversation properties on the SAME agency
//! machinery as conversation blocks: one machinery, two ledgers.
//!
//! Spawned alongside the conversation actor, managed by its lifecycle. Three
//! components run per conversation:
//!
//! - **Policy watcher**: watches conversation blocks, expresses intent by
//!   appending request rows to the metadata ledger. Pure intent — never talks
//!   to a provider.
//! - **Metadata scheduler**: the generic ratchet instantiated over the
//!   metadata ledger ([`ratchet::MetadataLedger`]), behind the conversation's
//!   latch. A request row parks the metadata cursor and re-emits its
//!   fulfillment wakeup per tick — the ratchet IS the retry loop.
//! - **Fulfillment actor**: wakeup-driven consumer of
//!   [`CoreEvent::MetadataRequestReady`]. Re-reads the metadata ledger at
//!   delivery (ledger-keyed idempotency), honors the latch (drop, not defer),
//!   lazy-binds a provider, streams, and writes the response row — whose
//!   insertion settles the parked request on the next tick.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::agency::ratchet::{self, MetadataLedger};
use crate::agency::{AgencyCtx, BlockKind, Projection};
use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::event::{AsCoreEvent, CoreEvent};
use crate::providers::types::{
    ModelSelector, ProviderRequest, ProviderResponse, ProviderRx, ProviderTx, StreamEvent,
};
use crate::reactive;
use crate::reactivity::ReadSignal;
use crate::store::Store;

use super::actor::RuntimeContext;

/// Maximum conversation blocks before title derivation is skipped.
const TITLE_DERIVE_MAX_BLOCKS: usize = 8;

/// Fulfillment backoff: base delay after the first consecutive failure,
/// doubling per failure up to the cap.
const FULFILLMENT_BACKOFF_BASE: Duration = Duration::from_secs(1);
const FULFILLMENT_BACKOFF_CAP: Duration = Duration::from_mins(1);

/// Delivery throttle for failing fulfillment. The ratchet already gives us
/// the retry loop — the parked request re-emits its wakeup every tick — so a
/// deterministic provider failure (a delisted model, a dead endpoint)
/// otherwise re-streams on EVERY tick: the observed storm was dozens of
/// attempts inside a few seconds. This throttle spaces the REAL attempts; a
/// wakeup arriving inside the backoff window is dropped exactly like a
/// latched one, and the parked request re-emits later anyway. In-memory state
/// is correct here: it is throttling, not truth.
pub(crate) struct FulfillmentThrottle {
    state: std::sync::Mutex<ThrottleState>,
}

#[derive(Default)]
struct ThrottleState {
    consecutive_failures: u32,
    not_before: Option<Instant>,
}

impl FulfillmentThrottle {
    pub(crate) fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(ThrottleState::default()),
        }
    }

    /// The delay applied after the Nth consecutive failure (1-based):
    /// base · 2^(N-1), capped.
    fn delay_after(failures: u32) -> Duration {
        FULFILLMENT_BACKOFF_BASE
            .saturating_mul(1u32 << (failures.saturating_sub(1)).min(6))
            .min(FULFILLMENT_BACKOFF_CAP)
    }

    /// Record one failed attempt; returns (consecutive failures, applied
    /// delay).
    pub(crate) fn record_failure(&self) -> (u32, Duration) {
        let mut state = self.state.lock().unwrap();
        state.consecutive_failures += 1;
        let delay = Self::delay_after(state.consecutive_failures);
        state.not_before = Some(Instant::now() + delay);
        (state.consecutive_failures, delay)
    }

    /// A fulfilled request clears the streak and the window.
    pub(crate) fn record_success(&self) {
        *self.state.lock().unwrap() = ThrottleState::default();
    }

    /// Consulted at DELIVERY: true while inside the backoff window.
    pub(crate) fn suppressed(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .not_before
            .is_some_and(|not_before| Instant::now() < not_before)
    }

    /// The attempt number the next dispatch represents (1 = first try).
    pub(crate) fn next_attempt(&self) -> u32 {
        self.state.lock().unwrap().consecutive_failures + 1
    }
}

/// Milliseconds for a log field, saturating rather than truncating.
fn log_millis(delay: Duration) -> u64 {
    u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
}

/// Spawn the per-conversation metadata subsystem. Returns join handles so the
/// conversation actor can abort them on shutdown.
pub(crate) fn spawn<E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    latched: ReadSignal<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    // Subscribed here, before the spawn, so no wakeup can slip between the
    // caller's world starting to move and the loop's own subscription.
    let fulfillment_rx = ctx.bus().subscribe();
    vec![
        tokio::spawn(run_policy_watcher(conv_id, ctx.clone())),
        tokio::spawn(run_metadata_scheduler(
            conv_id,
            ctx.clone(),
            latched.clone(),
        )),
        tokio::spawn(fulfillment_loop(
            conv_id,
            ctx,
            latched,
            fulfillment_rx,
            None,
            Arc::new(FulfillmentThrottle::new()),
        )),
    ]
}

// ─── Policy watcher ─────────────────────────────────────────────────────────

/// Watches conversation blocks, cross-references the metadata table, and
/// expresses intent by inserting request rows. Never talks to a provider.
///
/// Deliberately NOT latch-gated — the insert-vs-act line: appending an intent
/// row is RECORDING data, exactly like the live-tail path records a streamed
/// call block while latched; it executes nothing, emits nothing, and binds no
/// provider. Acting on the row (the ratchet drive, the fulfillment stream) is
/// orchestration and rests behind the latch, so a boot-latched conversation
/// may accrue the request but derivation cannot fire in a boot burst — it
/// heals on the next unlatch via the cursor.
async fn run_policy_watcher<E: RuntimeEvent>(conv_id: i64, ctx: RuntimeContext<E>) {
    let store = ctx.store().clone();
    let db_changes = store.changes.watcher();

    reactive! {
        db_changes.react();
        policy_tick(&store, conv_id).await;
    }
}

/// One policy pass: insert a `title_request` iff the conversation is small
/// enough, the assistant has spoken, and no request/response exists yet — the
/// once-only guard is the ledger itself.
pub(crate) async fn policy_tick(store: &Store, conv_id: i64) {
    let block_count = store.block_count(conv_id).await.unwrap_or(0);

    if block_count > 0
        && block_count <= TITLE_DERIVE_MAX_BLOCKS
        && !store
            .has_metadata(conv_id, "title_request")
            .await
            .unwrap_or(true)
        && !store
            .has_metadata(conv_id, "title_response")
            .await
            .unwrap_or(true)
        && store.has_assistant_blocks(conv_id).await.unwrap_or(false)
    {
        let source_block_id = store
            .list_blocks(conv_id)
            .await
            .ok()
            .and_then(|b| b.first().map(|block| block.id));

        match store
            .insert_metadata(conv_id, "title_request", source_block_id, None)
            .await
        {
            Ok(_) => tracing::info!(conversation_id = conv_id, "title_request inserted"),
            Err(e) => {
                tracing::error!(conversation_id = conv_id, error = %e, "title_request insert failed");
            }
        }
    }
}

// ─── The metadata ratchet ───────────────────────────────────────────────────

/// One metadata scheduler tick: rest while latched (a latched conversation
/// drives NEITHER ledger), otherwise drive the metadata cursor through the
/// same generic drive the conversation scheduler uses, handed
/// [`MetadataLedger`] and nothing else. The metadata frontier never owes a
/// model turn (its kinds await System or nothing), so unlike the conversation
/// tick there is no gate signal to wire.
pub(crate) async fn metadata_tick<E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    latched: bool,
) -> Option<ratchet::Outcome> {
    if latched {
        return None;
    }
    match ratchet::drive_ledger(ctx, &MetadataLedger).await {
        Ok(outcome) => {
            tracing::debug!(
                conversation_id = ctx.conversation_id,
                ?outcome,
                "metadata tick"
            );
            Some(outcome)
        }
        Err(e) => {
            tracing::error!(conversation_id = ctx.conversation_id, error = %e, "metadata scheduler: drive failed");
            None
        }
    }
}

/// Metadata scheduler — the SINGLE driver of the metadata cursor for its
/// conversation, behind the SAME latch signal as the conversation scheduler.
async fn run_metadata_scheduler<E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    read_latched: ReadSignal<bool>,
) {
    let agency = ctx.agency(conv_id);
    let db_changes = ctx.store().changes.watcher();

    reactive! {
        let latched = read_latched.get();
        db_changes.react();

        metadata_tick(&agency, latched).await;
    }
}

// ─── Fulfillment actor ──────────────────────────────────────────────────────

/// The delivery loop: consumes [`CoreEvent::MetadataRequestReady`] off the
/// subscription the caller hands in. Truth is the metadata ledger, the wakeup
/// a prompt to re-derive.
///
/// Tests drive it with a stub provider channel pre-cached in `provider_tx`
/// (the lazy bind then never touches the registry) and an externally
/// observable throttle; production passes `None` and a fresh throttle.
pub(crate) async fn fulfillment_loop<E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    latched: ReadSignal<bool>,
    mut rx: tokio::sync::broadcast::Receiver<E>,
    mut provider_tx: Option<ProviderTx>,
    throttle: Arc<FulfillmentThrottle>,
) {
    let store = ctx.store().clone();
    // Same-process concurrency guard — wakeups re-arrive while a stream is in
    // flight; the ingestion reader clears it when the stream settles. The
    // ledger check below is the correctness guard.
    let streaming = Arc::new(AtomicBool::new(false));

    loop {
        match rx.recv().await {
            Ok(event) => {
                let Some(&CoreEvent::MetadataRequestReady {
                    conversation_id,
                    request_id,
                }) = event.as_core()
                else {
                    continue;
                };
                if conversation_id != conv_id {
                    continue;
                }
                // A wakeup delivered while latched is DROPPED, not deferred —
                // the parked request re-emits on the next unlatched tick; the
                // ratchet is the retry loop.
                if latched.get() {
                    continue;
                }
                // Backoff, same drop-not-defer shape: inside the window the
                // wakeup dies here and the parked request re-emits later.
                if throttle.suppressed() {
                    tracing::debug!(
                        conversation_id = conv_id,
                        request_id,
                        "fulfillment backoff — suppressed retry, wakeup dropped"
                    );
                    continue;
                }
                if streaming.load(Ordering::Relaxed) {
                    continue;
                }

                // Ledger-keyed idempotency at delivery: re-read and act iff
                // the durable state still calls for it — a duplicate or stale
                // wakeup is a no-op.
                let ledger = match store.list_metadata_blocks(conv_id).await {
                    Ok(ledger) => ledger,
                    Err(e) => {
                        tracing::warn!(conversation_id = conv_id, error = %e, "fulfillment: metadata read failed");
                        continue;
                    }
                };
                let request = ledger
                    .iter()
                    .find(|b| b.id == request_id)
                    .map(BlockKind::from_block);
                let Some(BlockKind::MetadataTitleRequest(request)) = request else {
                    continue;
                };
                if request.settled_in(&ledger) {
                    continue;
                }

                let Some(tx) =
                    ensure_provider(conv_id, &ctx, &mut provider_tx, &streaming, &throttle).await
                else {
                    // A binding failure (deleted/misconfigured provider
                    // config) is as deterministic as a delisted model —
                    // without a recorded failure it would re-stream on EVERY
                    // tick, the same unbounded storm the stream-failure arm
                    // already throttles. Open the window here too.
                    let (failures, delay) = throttle.record_failure();
                    tracing::warn!(
                        conversation_id = conv_id,
                        request_id,
                        consecutive_failures = failures,
                        backoff_ms = log_millis(delay),
                        "fulfillment could not bind a provider — backing off"
                    );
                    continue;
                };
                let attempt = throttle.next_attempt();
                if attempt > 1 {
                    tracing::info!(
                        conversation_id = conv_id,
                        request_id,
                        attempt,
                        "fulfillment retry"
                    );
                }
                if request_title_stream(&store, conv_id, &tx).await {
                    streaming.store(true, Ordering::Relaxed);
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(
                    conversation_id = conv_id,
                    skipped = n,
                    "fulfillment actor lagged — wakeups dropped"
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// The title derivation reads prose only: user/assistant blocks that
/// contribute text on the projection axis (their own `llm_text` answer) — no
/// tool calls, no tool results, no reasoning, no system rows. Tool history is
/// token noise here, and tool-shaped messages without declared tools risk
/// provider rejections.
fn title_source_blocks(blocks: Vec<Block>) -> Vec<Block> {
    blocks
        .into_iter()
        .filter(|block| {
            let kind = BlockKind::from_block(block);
            matches!(kind.group_role(), Some(Role::User | Role::Assistant))
                && kind.llm_text().is_some_and(|text| !text.is_empty())
        })
        .collect()
}

/// Build the derivation request off the conversation's prose and send it.
/// Returns whether a stream request went out.
async fn request_title_stream(store: &Store, conv_id: i64, tx: &ProviderTx) -> bool {
    let blocks = match store.list_blocks(conv_id).await {
        Ok(b) => title_source_blocks(b),
        Err(_) => return false,
    };
    if blocks.is_empty() {
        return false;
    }

    // Append a title instruction to the conversation's prose.
    let mut title_blocks = blocks;
    title_blocks.push(Block {
        id: 0,
        role: Some(Role::User),
        block_type: "text".into(),
        created_at: String::new(),
        fields: {
            let mut m = serde_json::Map::new();
            m.insert(
                "content".into(),
                serde_json::Value::String(
                    "Generate a concise title (3-7 words) for this conversation. \
                     Return ONLY the title text, no quotes, no explanation."
                        .into(),
                ),
            );
            m
        },
    });

    tx.send(ProviderRequest::Stream {
        blocks: title_blocks,
        model: ModelSelector::Lightweight,
        tools: vec![],
        // Title derivation uses the lightweight model with no reasoning.
        reasoning: None,
    })
    .is_ok()
}

/// Ingestion reader for the fulfillment actor. Collects text deltas, writes
/// `title_response` metadata, and emits [`CoreEvent::TitleUpdated`] on the
/// bus.
pub(crate) async fn run_fulfillment_ingestion<E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<E>,
    mut provider_rx: ProviderRx,
    streaming: Arc<AtomicBool>,
    throttle: Arc<FulfillmentThrottle>,
) {
    let store = ctx.store();
    let bus = ctx.bus();

    let mut title = String::new();

    while let Some(response) = provider_rx.recv().await {
        match response {
            ProviderResponse::Event(StreamEvent::TextDelta { text }) => {
                title.push_str(&text);
            }
            ProviderResponse::Done => {
                let trimmed = title.trim().trim_matches('"').trim();

                if trimmed.is_empty() {
                    // A stream that produced no title is a failed attempt —
                    // an empty deterministic response would otherwise retry
                    // unthrottled every tick.
                    let (failures, delay) = throttle.record_failure();
                    tracing::warn!(
                        conversation_id = conv_id,
                        consecutive_failures = failures,
                        backoff_ms = log_millis(delay),
                        "title derivation produced no text — backing off"
                    );
                } else {
                    let source_block_id = store
                        .list_blocks(conv_id)
                        .await
                        .ok()
                        .and_then(|b| b.first().map(|block| block.id));

                    let _ = store
                        .insert_metadata(conv_id, "title_response", source_block_id, Some(trimmed))
                        .await;

                    bus.emit(CoreEvent::TitleUpdated {
                        conversation_id: conv_id,
                        title: trimmed.to_owned(),
                    });

                    tracing::info!(conversation_id = conv_id, title = trimmed, "title derived");
                    throttle.record_success();
                }

                title.clear();
                streaming.store(false, Ordering::Relaxed);
            }
            ProviderResponse::Error(e) => {
                // The streaming flag clears and the still-parked request
                // re-emits on the next tick — the ratchet retries, spaced by
                // the throttle at delivery.
                let (failures, delay) = throttle.record_failure();
                tracing::warn!(
                    conversation_id = conv_id,
                    error = %e,
                    consecutive_failures = failures,
                    backoff_ms = log_millis(delay),
                    "title derivation error — backing off"
                );
                title.clear();
                streaming.store(false, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

// ─── Provider binding ───────────────────────────────────────────────────────

async fn ensure_provider<E: RuntimeEvent>(
    conv_id: i64,
    ctx: &RuntimeContext<E>,
    cached_tx: &mut Option<ProviderTx>,
    streaming: &Arc<AtomicBool>,
    throttle: &Arc<FulfillmentThrottle>,
) -> Option<ProviderTx> {
    if let Some(tx) = cached_tx.as_ref()
        && !tx.is_closed()
    {
        return Some(tx.clone());
    }
    *cached_tx = None;

    let store = ctx.store();

    let conv = store.find_conversation(conv_id).await.ok()??;
    let instance = store
        .find_provider_instance(conv.model.provider_id.clone())
        .await
        .ok()??;
    let module = ctx.providers().get(&instance.provider_type)?;
    let config = module
        .get_config(conv.model.provider_id.clone())
        .await
        .ok()??;

    let (tx, rx) = module.bind(conv_id, conv.model.provider_id.clone(), config);

    tokio::spawn(run_fulfillment_ingestion(
        conv_id,
        ctx.clone(),
        rx,
        Arc::clone(streaming),
        Arc::clone(throttle),
    ));

    *cached_tx = Some(tx.clone());
    tracing::info!(conversation_id = conv_id, "metadata provider bound");
    Some(tx)
}

/// The metadata ledger at the delivery seams: duplicate and stale fulfillment
/// wakeups collapse at delivery; the latch rests both the drive and the
/// delivery; the policy watcher's existing semantics are pinned; the backoff
/// throttle spaces failing attempts.
///
/// The three park/settle/cursor tests that drive the metadata ledger through
/// the generic ratchet itself ran ahead with the behavior slice and live in
/// `agency::ratchet_tests` — they are not repeated here.
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde_json::Value;

    use tokio::sync::mpsc::error::TryRecvError;

    use crate::agency::ratchet::oracle::Oracle;
    use crate::providers::ProviderRegistry;
    use crate::reactivity::{WriteSignal, create_signal};
    use crate::store::ToolCallInsert;
    use crate::tools::ToolRegistry;

    use super::*;

    async fn title_request(o: &Oracle) -> i64 {
        o.ctx
            .store
            .insert_metadata(o.ctx.conversation_id, "title_request", None, None)
            .await
            .unwrap()
    }

    async fn title_response(o: &Oracle) -> i64 {
        o.ctx
            .store
            .insert_metadata(
                o.ctx.conversation_id,
                "title_response",
                None,
                Some("A Title"),
            )
            .await
            .unwrap()
    }

    async fn metadata_cursor(o: &Oracle) -> i64 {
        o.ctx
            .store
            .metadata_cursor(o.ctx.conversation_id)
            .await
            .unwrap()
    }

    #[track_caller]
    fn expect_request_wakeup(o: &mut Oracle, request_id: i64) {
        match o
            .rx
            .try_recv()
            .expect("expected a MetadataRequestReady wakeup")
        {
            CoreEvent::MetadataRequestReady {
                conversation_id,
                request_id: id,
            } => {
                assert_eq!(conversation_id, o.ctx.conversation_id);
                assert_eq!(id, request_id);
            }
            other => panic!("expected MetadataRequestReady, got {other:?}"),
        }
    }

    fn drain_bus(o: &mut Oracle) {
        while o.rx.try_recv().is_ok() {}
    }

    /// A [`RuntimeContext`] over the oracle's store and bus with empty
    /// registries — the shape production hands the fulfillment loop, minus
    /// the provider the tests stub at the channel instead.
    fn runtime_ctx(o: &Oracle) -> RuntimeContext<CoreEvent> {
        RuntimeContext::new(
            o.ctx.store.clone(),
            Arc::clone(&o.ctx.bus),
            Arc::new(ProviderRegistry::new()),
            Arc::new(ToolRegistry::new()),
        )
    }

    // ─── The fulfillment actor at delivery ───────────────────────────────

    /// The real delivery loop over a plainly-constructed [`RuntimeContext`],
    /// with a bare channel as the fulfillment stub pre-cached as the
    /// provider — the lazy bind never touches a registry, and the stub's
    /// receiver counts stream requests. The loop's subscription is taken
    /// BEFORE the spawn, so no polling is needed to know it is listening.
    struct FulfillmentRig {
        o: Oracle,
        stub_rx: tokio::sync::mpsc::UnboundedReceiver<ProviderRequest>,
        write_latched: WriteSignal<bool>,
        throttle: Arc<FulfillmentThrottle>,
        actor: tokio::task::JoinHandle<()>,
    }

    impl FulfillmentRig {
        async fn new(latched: bool) -> Self {
            let o = Oracle::new().await;
            let ctx = runtime_ctx(&o);

            let (stub_tx, stub_rx) = tokio::sync::mpsc::unbounded_channel();
            let (read_latched, write_latched) = create_signal(latched);
            let throttle = Arc::new(FulfillmentThrottle::new());
            let rx = o.ctx.bus.subscribe();
            // The stub provider tx is pre-cached, so the loop's lazy bind
            // never touches the empty registry.
            let actor = tokio::spawn(fulfillment_loop(
                o.ctx.conversation_id,
                ctx,
                read_latched,
                rx,
                Some(stub_tx),
                Arc::clone(&throttle),
            ));
            Self {
                o,
                stub_rx,
                write_latched,
                throttle,
                actor,
            }
        }

        fn send_wakeup(&self, request_id: i64) {
            self.o.ctx.bus.emit(CoreEvent::MetadataRequestReady {
                conversation_id: self.o.ctx.conversation_id,
                request_id,
            });
        }

        /// The next stream request's blocks, awaited up to `window` — panics
        /// if none arrives (the dispatch was expected).
        async fn next_stream_blocks(&mut self, window: Duration) -> Vec<Block> {
            let deadline = Instant::now() + window;
            loop {
                match self.stub_rx.try_recv() {
                    Ok(ProviderRequest::Stream { blocks, .. }) => return blocks,
                    Ok(ProviderRequest::Interrupt) => {}
                    Err(_) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    // Empty past the deadline and a disconnected stub alike
                    // mean the dispatch never came.
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                        panic!("expected a stream request within the window")
                    }
                }
            }
        }

        /// Count the stream requests reaching the stub within the window.
        async fn stream_requests(&mut self, window: Duration) -> usize {
            let deadline = Instant::now() + window;
            let mut count = 0;
            loop {
                match self.stub_rx.try_recv() {
                    Ok(ProviderRequest::Stream { .. }) => count += 1,
                    Ok(ProviderRequest::Interrupt) => {}
                    Err(_) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(_) => break,
                }
            }
            count
        }
    }

    impl Drop for FulfillmentRig {
        fn drop(&mut self) {
            self.actor.abort();
        }
    }

    /// At the fulfillment chokepoint: N re-emitted wakeups for one unanswered
    /// request collapse to AT MOST one stream request — the in-flight guard
    /// holds while the response is pending, the ledger check is the
    /// correctness guard.
    #[tokio::test]
    async fn duplicate_wakeups_collapse_to_one_stream_request() {
        let mut rig = FulfillmentRig::new(false).await;
        rig.o.user_text("hello").await; // the derivation streams over the conversation blocks
        let request = title_request(&rig.o).await;

        for _ in 0..5 {
            rig.send_wakeup(request);
        }
        assert_eq!(
            rig.stream_requests(Duration::from_millis(300)).await,
            1,
            "five deliveries, one stream request"
        );
    }

    /// Ledger-keyed idempotency at delivery, independent of the in-flight
    /// flag: a stale wakeup for an already-settled request — and one for an
    /// id the ledger does not know — dispatch nothing.
    #[tokio::test]
    async fn stale_wakeup_for_a_settled_request_is_a_no_op() {
        let mut rig = FulfillmentRig::new(false).await;
        rig.o.user_text("hello").await;
        let request = title_request(&rig.o).await;
        title_response(&rig.o).await; // settled before anything was ever in flight

        rig.send_wakeup(request);
        rig.send_wakeup(request + 999);
        assert_eq!(
            rig.stream_requests(Duration::from_millis(150)).await,
            0,
            "the ledger settles stale wakeups, not the in-flight flag"
        );
    }

    /// The latch end to end on the metadata side: latched, the ratchet drives
    /// nothing and the actor DROPS a delivered wakeup (not defers); on
    /// unlatch the parked request re-emits and heals to exactly one response
    /// flow.
    #[tokio::test]
    async fn latched_metadata_rests_drops_deliveries_and_heals_on_unlatch() {
        let mut rig = FulfillmentRig::new(true).await;
        rig.o.user_text("hello").await;
        let request = title_request(&rig.o).await;

        // The latched ratchet rests entirely: no drive, no wakeup, no cursor.
        for _ in 0..3 {
            assert!(metadata_tick(&rig.o.ctx, true).await.is_none());
        }
        rig.o.expect_silence();
        assert_eq!(metadata_cursor(&rig.o).await, 0);

        // A wakeup delivered while latched is dropped.
        rig.send_wakeup(request);
        assert_eq!(rig.stream_requests(Duration::from_millis(100)).await, 0);
        drain_bus(&mut rig.o);

        // Unlatch: the tick re-emits, the actor dispatches exactly once.
        rig.write_latched.set(false);
        let outcome = metadata_tick(&rig.o.ctx, false).await.unwrap();
        assert!(outcome.parked);
        expect_request_wakeup(&mut rig.o, request);
        assert_eq!(
            rig.stream_requests(Duration::from_millis(300)).await,
            1,
            "exactly one response flow after unlatch"
        );

        // The response lands: the ratchet settles in silence — nothing
        // further ever reaches the actor.
        let response = title_response(&rig.o).await;
        let outcome = metadata_tick(&rig.o.ctx, false).await.unwrap();
        assert!(!outcome.parked);
        assert_eq!(outcome.cursor, response);
        rig.o.expect_silence();
        assert_eq!(rig.stream_requests(Duration::from_millis(100)).await, 0);
    }

    // ─── The policy watcher's semantics, pinned ──────────────────────────

    #[tokio::test]
    async fn policy_inserts_the_request_once_and_only_when_eligible() {
        let o = Oracle::new().await;
        let store = &o.ctx.store;
        let conv = o.ctx.conversation_id;

        // Empty conversation: no intent.
        policy_tick(store, conv).await;
        assert!(!store.has_metadata(conv, "title_request").await.unwrap());

        // The user spoke but the assistant has not: still no intent.
        let first = o.user_text("hi").await;
        policy_tick(store, conv).await;
        assert!(!store.has_metadata(conv, "title_request").await.unwrap());

        // The assistant spoke: exactly one request, anchored on the first
        // block, inserted once no matter how often the policy re-runs.
        o.assistant_text("hello").await;
        policy_tick(store, conv).await;
        policy_tick(store, conv).await;
        let ledger = store.list_metadata_blocks(conv).await.unwrap();
        let requests: Vec<_> = ledger
            .iter()
            .filter(|b| b.block_type == "title_request")
            .collect();
        assert_eq!(requests.len(), 1, "the ledger is the once-only guard");
        assert_eq!(requests[0].fields["source_block_id"], Value::from(first));

        // A response present means the flow is complete — no fresh request
        // (regeneration is a future explicit intent, not this policy's).
        store
            .insert_metadata(conv, "title_response", None, Some("T"))
            .await
            .unwrap();
        policy_tick(store, conv).await;
        let ledger = store.list_metadata_blocks(conv).await.unwrap();
        assert_eq!(
            ledger
                .iter()
                .filter(|b| b.block_type == "title_request")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn policy_skips_conversations_beyond_the_derivation_window() {
        let o = Oracle::new().await;
        for i in 0..5 {
            o.user_text(&format!("m{i}")).await;
            o.assistant_text("r").await;
        }
        policy_tick(&o.ctx.store, o.ctx.conversation_id).await;
        assert!(
            !o.ctx
                .store
                .has_metadata(o.ctx.conversation_id, "title_request")
                .await
                .unwrap(),
            "past the window the conversation keeps its provisional title"
        );
    }

    // ─── The fulfillment throttle (backoff at delivery) ──────────────────

    /// The schedule itself, deterministic: 1s doubling per consecutive
    /// failure to the 60s cap; success resets the streak and the window.
    #[test]
    fn backoff_schedule_doubles_to_the_cap_and_resets_on_success() {
        let throttle = FulfillmentThrottle::new();
        let expected_secs = [1, 2, 4, 8, 16, 32, 60, 60];
        for (i, secs) in expected_secs.into_iter().enumerate() {
            let (failures, delay) = throttle.record_failure();
            assert_eq!(failures as usize, i + 1);
            assert_eq!(
                delay,
                Duration::from_secs(secs),
                "delay after failure {}",
                i + 1
            );
        }
        assert!(throttle.suppressed(), "inside the window after failures");
        assert_eq!(throttle.next_attempt(), 9);

        throttle.record_success();
        assert!(!throttle.suppressed(), "success clears the window");
        assert_eq!(throttle.next_attempt(), 1, "…and the streak");
    }

    /// Backoff consulted at DELIVERY: wakeups landing inside the window are
    /// dropped like latched ones — no stream request goes out no matter how
    /// many rapid ticks re-emit — and a success reopens delivery.
    #[tokio::test]
    async fn wakeups_inside_the_backoff_window_are_dropped() {
        let mut rig = FulfillmentRig::new(false).await;
        rig.o.user_text("hello").await;
        let request = title_request(&rig.o).await;

        rig.throttle.record_failure(); // 1s window opens
        for _ in 0..5 {
            rig.send_wakeup(request);
        }
        assert_eq!(
            rig.stream_requests(Duration::from_millis(150)).await,
            0,
            "every wakeup inside the window is suppressed"
        );

        rig.throttle.record_success();
        rig.send_wakeup(request);
        assert_eq!(
            rig.stream_requests(Duration::from_millis(300)).await,
            1,
            "a cleared throttle dispatches again"
        );
    }

    /// The failure signal reaches the throttle from the REAL ingestion
    /// reader: a provider error opens the window and releases the in-flight
    /// flag.
    #[tokio::test]
    async fn ingestion_error_records_the_failure_and_releases_streaming() {
        let o = Oracle::new().await;
        let ctx = runtime_ctx(&o);

        let throttle = Arc::new(FulfillmentThrottle::new());
        let streaming = Arc::new(AtomicBool::new(true));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reader = tokio::spawn(run_fulfillment_ingestion(
            o.ctx.conversation_id,
            ctx,
            rx,
            Arc::clone(&streaming),
            Arc::clone(&throttle),
        ));

        tx.send(ProviderResponse::Error("404 No endpoints found".into()))
            .unwrap();
        drop(tx);
        reader.await.unwrap();

        assert!(
            throttle.suppressed(),
            "the deterministic failure opened the backoff window"
        );
        assert!(
            !streaming.load(Ordering::Relaxed),
            "the in-flight flag is released for the retry"
        );
    }

    /// The title request carries PROSE only: user/assistant text through the
    /// projection surface — no tool calls, no tool results, no reasoning, no
    /// system rows — plus the trailing instruction. The neutral render of the
    /// dispatched blocks contains zero tool parts.
    #[tokio::test]
    async fn title_request_carries_only_prose() {
        let mut rig = FulfillmentRig::new(false).await;
        let store = rig.o.ctx.store.clone();
        let conv = rig.o.ctx.conversation_id;

        store
            .insert_system_prompt(conv, "be terse".into())
            .await
            .unwrap();
        rig.o.user_text("what is a ratchet?").await;
        store
            .insert_thinking_block_with_content(
                conv,
                Role::Assistant,
                "pondering".into(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        store
            .insert_tool_call_block(
                conv,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "c1".into(),
                    name: "read_file".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        rig.o.result("c1").await;
        rig.o.assistant_text("a one-way advance").await;

        let request = title_request(&rig.o).await;
        rig.send_wakeup(request);

        let blocks = rig.next_stream_blocks(Duration::from_secs(2)).await;
        let types: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            types,
            vec!["text", "text", "text"],
            "user prose, assistant prose, the instruction — nothing else"
        );

        let messages = crate::providers::blocks_to_messages(&blocks);
        assert!(
            messages
                .iter()
                .all(|m| matches!(m.content, crate::providers::MessageContent::Text(_))),
            "no message renders as native parts — no tool shapes on the wire"
        );
    }
}
