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
use std::time::{Duration, Instant};

use crate::agency::ratchet::{self, MetadataLedger};
use crate::agency::{AgencyCtx, BlockKind, FromBlock, RuntimeKind};
use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::event::{AsCoreEvent, CoreEvent};
use crate::providers::types::{
    FinalContentBlock, ModelSelector, ProviderRequest, ProviderResponse, ProviderRx, ProviderTx,
    StreamEvent,
};
use crate::reactive;
use crate::reactivity::ReadSignal;
use crate::store::{BlockDestination, Store};

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

/// The fulfillment side's dispatch state — ONE record under ONE lock: which
/// binding the loop currently dispatches on, and the turns open per opener
/// binding. The conversation actor's `close_dispatch` is the same
/// consolidation on the first ledger; here the state must additionally
/// survive rebinding, because the loop and its ingestion readers are two
/// tasks and a torn-down predecessor reader can drain a whole turn late.
///
/// The generation is what makes the close sound: a turn is opened under the
/// binding that dispatched it, and only that binding's [`FulfillmentSeam`]
/// can close it or read its anchor. A predecessor's late `Done` therefore
/// settles ITS turn — its response still carries its own turn's anchor — and
/// can neither clear the live turn's in-flight state nor stamp the live
/// turn's identity. Two unsynchronized halves (a shared flag beside a
/// per-binding slot) let exactly that interleaving through, which is why the
/// state is one value.
pub(crate) struct FulfillmentTurn {
    state: Arc<std::sync::Mutex<TurnState>>,
}

/// One binding's handle on the shared state: the reader-side seam. Reads and
/// closes ONLY the turn its own generation opened.
#[derive(Clone)]
pub(crate) struct FulfillmentSeam {
    state: Arc<std::sync::Mutex<TurnState>>,
    generation: u64,
}

#[derive(Default)]
struct TurnState {
    /// The binding identity the loop currently dispatches on, bumped at every
    /// bind.
    generation: u64,
    /// The open turns, keyed by the binding that opened each: the live one at
    /// the current generation, plus at most the just-replaced binding's turn
    /// still draining. `bind` prunes older entries, so a reader that died
    /// without its terminal cannot grow the list across rebinds.
    open: Vec<(u64, Option<i64>)>,
}

impl TurnState {
    fn anchor_of(&self, generation: u64) -> Option<i64> {
        self.open
            .iter()
            .find(|(g, _)| *g == generation)
            .and_then(|(_, anchor)| *anchor)
    }

    fn close_generation(&mut self, generation: u64) {
        self.open.retain(|(g, _)| *g != generation);
    }
}

impl FulfillmentTurn {
    fn new() -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(TurnState::default())),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TurnState> {
        // No code runs while the lock is held, so a poisoned lock carries no
        // broken invariant and is simply taken over.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Open the current binding's turn, before the dispatch goes out — the
    /// reader may write the response the moment the request lands, and it
    /// must already read this turn's anchor. `anchor` is `None` only for a
    /// caller that has no request row to name.
    fn open(&self, anchor: Option<i64>) {
        let mut state = self.lock();
        let generation = state.generation;
        state.close_generation(generation);
        state.open.push((generation, anchor));
    }

    /// Close the CURRENT binding's turn — the loop's own settle path for a
    /// dispatch that never went out. The readers close through their seams.
    fn close_unsent(&self) {
        let mut state = self.lock();
        let generation = state.generation;
        state.close_generation(generation);
    }

    /// Whether the current binding has a turn in flight. A predecessor still
    /// draining does not hold this — its channel is dead, and the rebind that
    /// replaced it is the loop's licence to dispatch again.
    fn is_open(&self) -> bool {
        let state = self.lock();
        state.open.iter().any(|(g, _)| *g == state.generation)
    }

    /// The current binding's open-turn anchor, for the tests that pin the
    /// live seam across a predecessor's late close.
    #[cfg(test)]
    fn anchor(&self) -> Option<i64> {
        let state = self.lock();
        state.anchor_of(state.generation)
    }

    /// A new binding: bump the generation and hand back the reader's seam,
    /// scoped to it. Turns older than the binding just replaced are pruned —
    /// their readers are gone; a still-draining predecessor is at most one
    /// bind behind, because a rebind happens only when its channel is already
    /// dead.
    fn bind(&self) -> FulfillmentSeam {
        let mut state = self.lock();
        state.generation += 1;
        let floor = state.generation - 1;
        state.open.retain(|(g, _)| *g >= floor);
        FulfillmentSeam {
            state: Arc::clone(&self.state),
            generation: state.generation,
        }
    }
}

impl FulfillmentSeam {
    fn lock(&self) -> std::sync::MutexGuard<'_, TurnState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The anchor of the turn THIS binding opened, `None` when it has none
    /// open. A predecessor draining late reads its own turn's anchor here,
    /// never the live one's.
    fn anchor(&self) -> Option<i64> {
        self.lock().anchor_of(self.generation)
    }

    /// Settle the turn this binding opened. Only the opener's generation may
    /// close: a stale seam's close removes its own entry — or nothing — and
    /// leaves the live turn open.
    fn close(&self) {
        self.lock().close_generation(self.generation);
    }
}

/// Spawn the per-conversation metadata subsystem. Returns join handles so the
/// conversation actor can abort them on shutdown.
///
/// A context with title derivation switched off spawns NOTHING: the decision
/// lives here, where the subsystem knows itself, so the actor's spawn site
/// stays a plain call. Without the watcher no request row is ever appended,
/// without the scheduler and the fulfillment loop nothing could act on one —
/// zero title traffic by construction, not by a check on the dispatch path.
pub(crate) fn spawn<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    latched: ReadSignal<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    if !ctx.title_derivation() {
        return Vec::new();
    }
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
async fn run_policy_watcher<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
) {
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
pub(crate) async fn metadata_tick<K: RuntimeKind, E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    latched: bool,
) -> Option<ratchet::Outcome> {
    if latched {
        return None;
    }
    match ratchet::drive_ledger::<K, E>(ctx, &MetadataLedger).await {
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
async fn run_metadata_scheduler<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    read_latched: ReadSignal<bool>,
) {
    let agency = ctx.agency(conv_id);
    let db_changes = ctx.store().changes.watcher();

    reactive! {
        let latched = read_latched.get();
        db_changes.react();

        metadata_tick::<K, E>(&agency, latched).await;
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
pub(crate) async fn fulfillment_loop<K: RuntimeKind, E: RuntimeEvent + AsCoreEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    latched: ReadSignal<bool>,
    mut rx: tokio::sync::broadcast::Receiver<E>,
    mut provider_tx: Option<ProviderTx>,
    throttle: Arc<FulfillmentThrottle>,
) {
    let store = ctx.store().clone();
    // The dispatch state, one record: the request row is the frontier owing
    // the derivation (a policy append carries no anchor of its own, so it
    // starts the identity), and the binding's own ingestion reader closes
    // the turn through its seam when the stream settles.
    let turn = FulfillmentTurn::new();

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
                if turn.is_open() {
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
                let Some(request_block) = ledger.iter().find(|b| b.id == request_id) else {
                    continue;
                };
                let BlockKind::MetadataTitleRequest(request) = BlockKind::from_block(request_block)
                else {
                    continue;
                };
                if request.settled_in(&ledger) {
                    continue;
                }
                // The dispatch's anchor: the request row starts the identity
                // (a policy append carries no anchor of its own), and a
                // request that somehow carried one would inherit it — the
                // same rule the conversation dispatch follows. Read off the
                // metadata ledger's own column, because the surfaced block
                // shape deliberately does not carry the metadata id space.
                let request_anchor = store
                    .metadata_dispatch_anchor(request_id)
                    .await
                    .unwrap_or(None)
                    .unwrap_or(request_block.id);

                let Some(tx) =
                    ensure_provider(conv_id, &ctx, &mut provider_tx, &turn, &throttle).await
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
                // The turn opens BEFORE the dispatch and closes again on a
                // failed one. The ingestion reader in the OTHER task closes
                // it when the stream settles, so an open placed after the
                // dispatch races that close: a fast failure could close
                // first, the late open would then read in-flight forever,
                // and no title would ever be derived again for the life of
                // the process. (The conversation actor's streaming flag is
                // set after its dispatch and is safe only because one task
                // owns both writes.)
                turn.open(Some(request_anchor));
                if !request_title_stream::<K>(&store, conv_id, &tx).await {
                    turn.close_unsent();
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
fn title_source_blocks<K: RuntimeKind>(blocks: Vec<Block>) -> Vec<Block> {
    blocks
        .into_iter()
        .filter(|block| {
            let kind = K::from_block(block);
            matches!(kind.group_role(), Some(Role::User | Role::Assistant))
                && kind.llm_text().is_some_and(|text| !text.is_empty())
        })
        .collect()
}

/// Build the derivation request off the conversation's prose and send it.
/// Returns whether a stream request went out.
///
/// The projection fold runs HERE, on the caller's side: the provider channel
/// carries neutral messages, never blocks.
async fn request_title_stream<K: RuntimeKind>(
    store: &Store,
    conv_id: i64,
    tx: &ProviderTx,
) -> bool {
    let blocks = match store.list_blocks(conv_id).await {
        Ok(b) => title_source_blocks::<K>(b),
        Err(_) => return false,
    };
    if blocks.is_empty() {
        return false;
    }
    // The conversation's own configured model rides the selector as the
    // background fallback: a provider with no background model configured
    // derives the title on the model the operator provably chose, never on
    // a slug hardcoded somewhere the operator cannot see. A conversation
    // the store cannot name a model for has nothing safe to dispatch on —
    // the parked request retries like any other failed dispatch.
    let Ok(Some(conversation)) = store.find_conversation(conv_id).await else {
        return false;
    };

    // Append a title instruction to the conversation's prose.
    let mut title_blocks = blocks;
    title_blocks.push(Block {
        id: 0,
        role: Some(Role::User),
        block_type: "text".into(),
        created_at: String::new(),
        dispatch_anchor: None,
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
        messages: crate::providers::blocks_to_messages::<K>(&title_blocks),
        model: ModelSelector::Lightweight {
            main: conversation.model.external_id,
        },
        tools: vec![],
        // Title derivation uses the provider's background model — or the
        // carried main model — with no reasoning.
        reasoning: None,
    })
    .is_ok()
}

/// Ingestion reader for the fulfillment actor. Collects text deltas, writes
/// `title_response` metadata, and emits [`CoreEvent::TitleUpdated`] on the
/// bus.
pub(crate) async fn run_fulfillment_ingestion<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: RuntimeContext<K, E>,
    mut provider_rx: ProviderRx,
    seam: FulfillmentSeam,
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
            // The two final-content shapes are content too: a provider that
            // reports its text through `TextFinal` or the integrity
            // restatement instead of deltas would otherwise derive an empty
            // title, recorded as a failure and retried forever. A final
            // restates the WHOLE turn, so it REPLACES the accumulated
            // partial instead of appending to it.
            ProviderResponse::Event(StreamEvent::TextFinal { text }) => {
                title = text;
            }
            ProviderResponse::Event(StreamEvent::ContentFinal { blocks }) => {
                let restated: String = blocks
                    .iter()
                    .filter_map(|block| match block {
                        FinalContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect();
                if !restated.is_empty() {
                    title = restated;
                }
            }
            // A reconnect regenerates the turn from its start: a kept partial
            // would have the replayed title concatenated onto it.
            ProviderResponse::Restart => title.clear(),
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

                    // The success recording and the TitleUpdated emit are
                    // conditional on the write: a failed persist announced as
                    // a success would broadcast a title the store does not
                    // have, and — unrecorded — would retry unthrottled every
                    // tick. A failed persist is a failed attempt.
                    // The response is the derivation turn's product: it
                    // carries the seam's anchor — the request row's id in its
                    // own ledger.
                    match store
                        .insert_metadata(
                            BlockDestination::anchored(conv_id, seam.anchor()),
                            "title_response",
                            source_block_id,
                            Some(trimmed),
                        )
                        .await
                    {
                        Ok(_) => {
                            bus.emit(CoreEvent::TitleUpdated {
                                conversation_id: conv_id,
                                title: trimmed.to_owned(),
                            });
                            tracing::info!(
                                conversation_id = conv_id,
                                title = trimmed,
                                "title derived"
                            );
                            throttle.record_success();
                        }
                        Err(e) => {
                            let (failures, delay) = throttle.record_failure();
                            tracing::error!(
                                conversation_id = conv_id,
                                error = %e,
                                consecutive_failures = failures,
                                backoff_ms = log_millis(delay),
                                "title_response persist failed — backing off"
                            );
                        }
                    }
                }

                title.clear();
                seam.close();
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
                seam.close();
            }
            // The remaining stream events carry no title text.
            ProviderResponse::Event(_) => {}
        }
    }
}

// ─── Provider binding ───────────────────────────────────────────────────────

async fn ensure_provider<K: RuntimeKind, E: RuntimeEvent>(
    conv_id: i64,
    ctx: &RuntimeContext<K, E>,
    cached_tx: &mut Option<ProviderTx>,
    turn: &FulfillmentTurn,
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

    // Each bind is a new binding identity, scoped into the reader's seam —
    // the same rule the conversation actor's bind follows: a torn-down
    // predecessor reader holds its own generation's seam and can neither
    // borrow a successor turn's anchor for its late writes nor clear the
    // live turn at its close.
    let seam = turn.bind();
    tokio::spawn(run_fulfillment_ingestion(
        conv_id,
        ctx.clone(),
        rx,
        seam,
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
    use crate::providers::types::Message;
    use crate::providers::{BoxFuture, LlmError, ModelInfo, ProviderModule, ProviderRegistry};
    use crate::reactivity::{WriteSignal, create_signal};
    use crate::store::{ProviderInstance, StoreError, ToolCallInsert};
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
    fn runtime_ctx(o: &Oracle) -> RuntimeContext<BlockKind, CoreEvent> {
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

        /// The next stream request's neutral messages, awaited up to `window` —
        /// panics if none arrives (the dispatch was expected).
        async fn next_stream_messages(&mut self, window: Duration) -> Vec<Message> {
            self.next_stream_request(window).await.0
        }

        /// The next stream request's neutral messages and model selector,
        /// awaited up to `window` — panics if none arrives (the dispatch was
        /// expected).
        async fn next_stream_request(&mut self, window: Duration) -> (Vec<Message>, ModelSelector) {
            let deadline = Instant::now() + window;
            loop {
                match self.stub_rx.try_recv() {
                    Ok(ProviderRequest::Stream {
                        messages, model, ..
                    }) => return (messages, model),
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
            assert!(
                metadata_tick::<BlockKind, _>(&rig.o.ctx, true)
                    .await
                    .is_none()
            );
        }
        rig.o.expect_silence();
        assert_eq!(metadata_cursor(&rig.o).await, 0);

        // A wakeup delivered while latched is dropped.
        rig.send_wakeup(request);
        assert_eq!(rig.stream_requests(Duration::from_millis(100)).await, 0);
        drain_bus(&mut rig.o);

        // Unlatch: the tick re-emits, the actor dispatches exactly once.
        rig.write_latched.set(false);
        let outcome = metadata_tick::<BlockKind, _>(&rig.o.ctx, false)
            .await
            .unwrap();
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
        let outcome = metadata_tick::<BlockKind, _>(&rig.o.ctx, false)
            .await
            .unwrap();
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

    /// The in-flight flag rolls back on a failed dispatch, so a later wakeup
    /// still proceeds. Without the rollback the flag would read true forever —
    /// no stream is in flight to ever clear it — and no title would be derived
    /// again for the life of the process.
    #[tokio::test]
    async fn failed_dispatch_rolls_the_in_flight_flag_back() {
        let mut rig = FulfillmentRig::new(false).await;
        let request = title_request(&rig.o).await;

        // No prose in the ledger yet: the dispatch fails AFTER the flag was
        // set, which is exactly the rollback's moment.
        rig.send_wakeup(request);
        assert_eq!(
            rig.stream_requests(Duration::from_millis(150)).await,
            0,
            "there was nothing to stream over"
        );

        // Prose arrives; the SAME loop must still dispatch.
        rig.o.user_text("hello").await;
        rig.send_wakeup(request);
        assert_eq!(
            rig.stream_requests(Duration::from_millis(500)).await,
            1,
            "the rolled-back flag lets the next wakeup through"
        );
    }

    /// A failed `title_response` persist is a FAILED attempt: nothing is
    /// announced (a `TitleUpdated` for a title the store does not have), no
    /// success clears the streak — the failure is recorded and backed off.
    #[tokio::test]
    async fn failed_title_persist_announces_nothing_and_backs_off() {
        let mut o = Oracle::new().await;
        o.ctx
            .store
            .run(|conn| {
                conn.execute(
                    "CREATE TRIGGER injected_metadata_failure BEFORE INSERT ON metadata \
                     BEGIN SELECT RAISE(ABORT, 'injected failure'); END",
                    [],
                )
                .map(|_| ())
                .map_err(Into::into)
            })
            .await
            .unwrap();

        let (throttle, turn) = run_reader(
            &o,
            vec![
                ProviderResponse::Event(StreamEvent::TextDelta {
                    text: "A Title".into(),
                }),
                ProviderResponse::Done,
            ],
        )
        .await;

        assert_eq!(recorded_title(&o).await, None, "the store has no title");
        assert!(
            throttle.suppressed(),
            "the failed persist opened the backoff window, not the success path"
        );
        assert!(!turn.is_open(), "the turn is closed for the retry");
        while let Ok(event) = o.rx.try_recv() {
            assert!(
                !matches!(event, CoreEvent::TitleUpdated { .. }),
                "no title is announced that the store cannot show"
            );
        }
    }

    /// The failure signal reaches the throttle from the REAL ingestion
    /// reader: a provider error opens the window and closes the turn.
    #[tokio::test]
    async fn ingestion_error_records_the_failure_and_releases_streaming() {
        let o = Oracle::new().await;
        let ctx = runtime_ctx(&o);

        let throttle = Arc::new(FulfillmentThrottle::new());
        // In flight with no request row to name: this test never reaches a
        // response write, so only the open-turn state matters.
        let turn = FulfillmentTurn::new();
        let seam = turn.bind();
        turn.open(None);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reader = tokio::spawn(run_fulfillment_ingestion(
            o.ctx.conversation_id,
            ctx,
            rx,
            seam,
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
        assert!(!turn.is_open(), "the turn is closed for the retry");
    }

    /// Spawn the real fulfillment ingestion reader over a bare channel and
    /// wait for it to drain everything sent.
    async fn run_reader(
        o: &Oracle,
        responses: Vec<ProviderResponse>,
    ) -> (Arc<FulfillmentThrottle>, FulfillmentTurn) {
        let ctx = runtime_ctx(o);
        let throttle = Arc::new(FulfillmentThrottle::new());
        // In flight with no request row to name — the anchor-specific test
        // opens the turn on a real request row instead.
        let turn = FulfillmentTurn::new();
        let seam = turn.bind();
        turn.open(None);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reader = tokio::spawn(run_fulfillment_ingestion(
            o.ctx.conversation_id,
            ctx,
            rx,
            seam,
            Arc::clone(&throttle),
        ));
        for response in responses {
            tx.send(response).unwrap();
        }
        drop(tx);
        reader.await.unwrap();
        (throttle, turn)
    }

    async fn recorded_title(o: &Oracle) -> Option<String> {
        o.ctx
            .store
            .list_metadata_blocks(o.ctx.conversation_id)
            .await
            .unwrap()
            .iter()
            .find(|b| b.block_type == "title_response")
            .and_then(|b| b.fields["content"].as_str().map(str::to_string))
    }

    /// The second ledger's turn product carries its OWN ledger's anchor: the
    /// title response names the request row whose owed fulfillment dispatched
    /// the derivation stream — the same rules as the block ledger's products,
    /// and the settle clears the seam.
    #[tokio::test]
    async fn the_title_response_anchors_on_its_request_row() {
        let o = Oracle::new().await;
        let request = title_request(&o).await;

        let ctx = runtime_ctx(&o);
        let throttle = Arc::new(FulfillmentThrottle::new());
        let turn = FulfillmentTurn::new();
        let seam = turn.bind();
        turn.open(Some(request));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reader = tokio::spawn(run_fulfillment_ingestion(
            o.ctx.conversation_id,
            ctx,
            rx,
            seam,
            Arc::clone(&throttle),
        ));
        tx.send(ProviderResponse::Event(StreamEvent::TextDelta {
            text: "A Title".into(),
        }))
        .unwrap();
        tx.send(ProviderResponse::Done).unwrap();
        drop(tx);
        reader.await.unwrap();

        let ledger = o
            .ctx
            .store
            .list_metadata_blocks(o.ctx.conversation_id)
            .await
            .unwrap();
        let response = ledger
            .iter()
            .find(|b| b.block_type == "title_response")
            .expect("the title derived");
        assert_eq!(
            o.ctx
                .store
                .metadata_dispatch_anchor(response.id)
                .await
                .unwrap(),
            Some(request),
            "the response anchors on its request row"
        );
        assert_eq!(
            o.ctx.store.metadata_dispatch_anchor(request).await.unwrap(),
            None,
            "the request starts the identity — a policy append carries none"
        );
        assert_eq!(
            response.dispatch_anchor, None,
            "the surfaced block shape never carries the metadata id space"
        );
        assert_eq!(turn.anchor(), None, "the settle cleared the seam");
        assert!(!turn.is_open(), "the settle closed the turn");
    }

    /// A provider module whose binding is dead on arrival: `bind` drops the
    /// request end immediately, so the cached tx reports closed and the next
    /// [`ensure_provider`] call binds fresh. Each binding's response sender is
    /// captured, which lets the test drive a PREDECESSOR binding's reader
    /// after a rebind has already happened.
    struct DeadBindStub {
        response_ends:
            Arc<std::sync::Mutex<Vec<tokio::sync::mpsc::UnboundedSender<ProviderResponse>>>>,
    }

    impl ProviderModule for DeadBindStub {
        fn type_id(&self) -> &'static str {
            "dead-bind-stub"
        }
        fn display_name(&self) -> &'static str {
            "Dead-bind stub"
        }
        fn description(&self) -> &'static str {
            "drops its request end at bind"
        }
        fn get_config(
            &self,
            _provider_id: String,
        ) -> BoxFuture<'_, Result<Option<Value>, StoreError>> {
            Box::pin(async { Ok(Some(serde_json::json!({}))) })
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
            let (req_tx, _dropped) = tokio::sync::mpsc::unbounded_channel();
            let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
            self.response_ends.lock().unwrap().push(resp_tx);
            (req_tx, resp_rx)
        }
    }

    /// The seam is scoped per BINDING — the dispatch state's own rule: the
    /// bind hands each reader a seam that reads and closes only the turn its
    /// own generation opened. A predecessor reader draining a full turn late
    /// therefore writes its OWN turn's anchor, and its close settles its own
    /// turn — the live turn stays open with its anchor intact.
    #[tokio::test]
    async fn a_rebind_replaces_the_seam_against_late_predecessor_writes() {
        let o = Oracle::new().await;
        let conv = o.ctx.conversation_id;
        // The oracle's conversation names provider "p1"; register it as the
        // dead-bind type so every ensure call binds fresh.
        o.ctx
            .store
            .save_provider_instance(ProviderInstance {
                id: "p1".into(),
                provider_type: "dead-bind-stub".into(),
                name: "Dead-bind stub".into(),
            })
            .await
            .unwrap();
        let response_ends = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut registry = ProviderRegistry::new();
        registry.register(Box::new(DeadBindStub {
            response_ends: Arc::clone(&response_ends),
        }));
        let ctx = RuntimeContext::<BlockKind, CoreEvent>::new(
            o.ctx.store.clone(),
            Arc::clone(&o.ctx.bus),
            Arc::new(registry),
            Arc::new(ToolRegistry::new()),
        );

        let throttle = Arc::new(FulfillmentThrottle::new());
        let mut cached_tx = None;
        let turn = FulfillmentTurn::new();

        // Binding one opens turn one…
        ensure_provider(conv, &ctx, &mut cached_tx, &turn, &throttle)
            .await
            .expect("first bind");
        turn.open(Some(1));
        let predecessor = response_ends.lock().unwrap().first().unwrap().clone();

        // …its channel is already dead, so the next delivery re-binds and
        // turn two opens under the new binding's generation.
        ensure_provider(conv, &ctx, &mut cached_tx, &turn, &throttle)
            .await
            .expect("rebind");
        turn.open(Some(2));

        // The predecessor's reader drains a whole title turn late.
        predecessor
            .send(ProviderResponse::Event(StreamEvent::TextDelta {
                text: "Late Title".into(),
            }))
            .unwrap();
        predecessor.send(ProviderResponse::Done).unwrap();
        drop(predecessor);

        // The late reader has no handle to await, so poll for its write.
        let deadline = Instant::now() + Duration::from_secs(2);
        let response = loop {
            let ledger = o.ctx.store.list_metadata_blocks(conv).await.unwrap();
            if let Some(block) = ledger
                .into_iter()
                .find(|b| b.block_type == "title_response")
            {
                break block;
            }
            assert!(Instant::now() < deadline, "the late reader never wrote");
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        assert_eq!(
            o.ctx
                .store
                .metadata_dispatch_anchor(response.id)
                .await
                .unwrap(),
            Some(1),
            "the late write carries its own turn's anchor, not the successor's"
        );
        assert_eq!(
            turn.anchor(),
            Some(2),
            "the predecessor's close settled its own turn, not the live one"
        );
        assert!(
            turn.is_open(),
            "the live turn's in-flight state survives the predecessor's close \
             — a shared flag was exactly what a late close used to clear"
        );
    }

    /// A reconnect's `Restart` clears the accumulated partial: the
    /// regenerated stream replays the title from its start, so a kept partial
    /// would have the whole replay concatenated onto it.
    #[tokio::test]
    async fn restart_clears_the_partial_title_before_the_replay() {
        let o = Oracle::new().await;
        let (throttle, _turn) = run_reader(
            &o,
            vec![
                ProviderResponse::Event(StreamEvent::TextDelta {
                    text: "A Par".into(),
                }),
                ProviderResponse::Restart,
                ProviderResponse::Event(StreamEvent::TextDelta {
                    text: "A Replayed Title".into(),
                }),
                ProviderResponse::Done,
            ],
        )
        .await;

        assert_eq!(
            recorded_title(&o).await.as_deref(),
            Some("A Replayed Title"),
            "the replay stands alone — no partial concatenated in front"
        );
        assert!(
            !throttle.suppressed(),
            "a clean derivation opens no backoff"
        );
    }

    /// A provider that reports its text through the final-content shapes
    /// instead of deltas still derives a title: `TextFinal` and the text
    /// blocks of `ContentFinal` are content, each replacing whatever partial
    /// accumulated — without them the derivation reads empty and is recorded
    /// as a failure, retried forever.
    #[tokio::test]
    async fn final_content_shapes_derive_the_title() {
        let o = Oracle::new().await;
        run_reader(
            &o,
            vec![
                ProviderResponse::Event(StreamEvent::TextFinal {
                    text: "A Final Title".into(),
                }),
                ProviderResponse::Done,
            ],
        )
        .await;
        assert_eq!(recorded_title(&o).await.as_deref(), Some("A Final Title"));

        let o = Oracle::new().await;
        let (throttle, turn) = run_reader(
            &o,
            vec![
                ProviderResponse::Event(StreamEvent::ContentFinal {
                    blocks: vec![crate::providers::types::FinalContentBlock::Text {
                        text: "A Restated Title".into(),
                    }],
                }),
                ProviderResponse::Done,
            ],
        )
        .await;
        assert_eq!(
            recorded_title(&o).await.as_deref(),
            Some("A Restated Title")
        );
        assert!(!throttle.suppressed(), "no failure was recorded");
        assert!(!turn.is_open(), "the stream settled");
    }

    /// The title request carries PROSE only: user/assistant text through the
    /// projection surface — no tool calls, no tool results, no reasoning, no
    /// system rows — plus the trailing instruction. The dispatched neutral
    /// messages contain zero tool parts.
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

        let messages = rig.next_stream_messages(Duration::from_secs(2)).await;
        let roles: Vec<crate::providers::MessageRole> = messages.iter().map(|m| m.role).collect();
        assert_eq!(
            roles,
            vec![
                crate::providers::MessageRole::User,
                crate::providers::MessageRole::Assistant,
                crate::providers::MessageRole::User,
            ],
            "user prose, assistant prose, the instruction — nothing else"
        );
        assert!(
            messages
                .iter()
                .all(|m| matches!(m.content, crate::providers::MessageContent::Text(_))),
            "no message renders as native parts — no tool shapes on the wire"
        );
    }

    /// The title dispatch names the CONVERSATION'S OWN configured model as
    /// the background fallback: the selector carries it so a provider with no
    /// background model of its own derives the title on the model the
    /// operator chose — never on a slug hardcoded in a provider module.
    #[tokio::test]
    async fn the_title_dispatch_carries_the_conversations_main_model() {
        let mut rig = FulfillmentRig::new(false).await;
        rig.o.user_text("hello").await;
        let request = title_request(&rig.o).await;

        rig.send_wakeup(request);
        let (_, selector) = rig.next_stream_request(Duration::from_secs(2)).await;
        // The oracle's conversation is created on external id "model".
        assert!(
            matches!(&selector, ModelSelector::Lightweight { main } if main == "model"),
            "the selector carries the conversation's configured model as the \
             background fallback; got {selector:?}"
        );
    }
}
