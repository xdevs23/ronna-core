//! The bind loop: one turn, driven end to end, with the resilience every
//! HTTP-based vendor inherits without writing any of it.
//!
//! A vendor module supplies one thing — a function that opens a stream — and
//! gets the rest: a rate-limit retry with the server's own backoff, an idle
//! watchdog, a restart-clean reconnect on a recoverable drop, the one-shot
//! request-shape fallback when a replayed reasoning payload is rejected, and
//! exactly one stream close. Each of those exists because of a failure that
//! reached a user, and each is stated here once rather than per vendor.
//!
//! The close is the part worth reading twice. `Done` means *the provider has
//! nothing more to say*, and a reconnect is not that: a loop that sent `Done`
//! before re-opening would tell its reader the turn had finished, and the
//! reader would commit a half-written turn and then receive a second one.

use std::future::Future;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::block::Block;

use super::http;
use super::types::{
    EventStream, LlmError, ModelSelector, ProviderRequest, ProviderResponse, ReasoningLevel,
    StreamEvent, ToolDefinition,
};

/// The application-level idle window for a streaming connection. If no event
/// arrives within this span the stream is treated as stalled — half-open — and
/// the turn reconnects.
///
/// Generous enough for a legitimate reasoning pause, far tighter than the
/// multi-minute hang a bare half-open socket produces. This is the primary,
/// typed detector; it derives from the transport-level backstop so the two can
/// never drift apart.
pub(crate) const STREAM_IDLE: Duration = http::STREAM_READ_TIMEOUT;

// Bounded backoff, shared by both retry paths: a rate-limited stream open and a
// recoverable mid-stream reconnect.
const MAX_RETRIES: u32 = 5;
const MIN_BACKOFF_SECS: u64 = 2;
const MAX_BACKOFF_SECS: u64 = 300;

/// A successfully opened turn, plus whether the request that opened it actually
/// carried replayed reasoning payloads.
///
/// Only a payload-carrying attempt earns the payload-omitting fallback on a
/// terminal API error. An attempt that carried none and failed anyway would
/// gain nothing from being retried without them, and the retry would cost the
/// user a second wait for the same error.
pub struct OpenedTurn {
    /// The turn's events.
    pub events: EventStream,
    /// Whether a stored continuity payload was replayed into the request.
    pub carried_payloads: bool,
}

/// How a single pumped stream concluded, reported back to the loop so it can
/// decide between finishing, reconnecting, falling back, or tearing down.
enum PumpOutcome {
    /// The stream finished cleanly, and the loop must close the channel.
    Finished,
    /// A terminal error. NOT yet sent to the channel — the cycle decides
    /// whether it earns the payload-omitting fallback before surfacing it.
    Terminal(LlmError),
    /// A recoverable mid-stream drop. No error and no close were sent; the
    /// loop should restart-clean and re-open the same turn.
    Recoverable(LlmError),
    /// The active turn was cancelled. The channel was left untouched — no
    /// close, because a new turn or a teardown follows.
    Cancelled,
}

/// Pump events from a stream into the response channel until the stream ends, a
/// terminal error occurs, the idle watchdog trips, or the turn is cancelled.
///
/// Each event await is wrapped in a timeout, so a half-open connection that
/// stops delivering bytes is detected even when the transport never surfaces an
/// error — every vendor that pumps through here inherits the watchdog with no
/// per-vendor code.
///
/// It never closes the channel itself: the loop owns close semantics, so a
/// reconnect cannot be mistaken for completion.
async fn pump_stream(
    events: &mut EventStream,
    tx: &UnboundedSender<ProviderResponse>,
    cancel: &CancellationToken,
) -> PumpOutcome {
    loop {
        tokio::select! {
            () = cancel.cancelled() => return PumpOutcome::Cancelled,
            next = tokio::time::timeout(STREAM_IDLE, events.next()) => match next {
                // No event within the idle window — a recoverable stall.
                Err(_elapsed) => {
                    warn!(idle_secs = STREAM_IDLE.as_secs(), "stream idle — reconnecting");
                    return PumpOutcome::Recoverable(LlmError::StreamIdle(STREAM_IDLE.as_secs()));
                }
                Ok(Some(Ok(e))) => {
                    if tx.send(ProviderResponse::Event(e)).is_err() {
                        // The receiver is gone; there is nobody left to tell.
                        return PumpOutcome::Cancelled;
                    }
                }
                Ok(Some(Err(e))) => {
                    if e.is_recoverable() {
                        warn!(error = %e, "recoverable mid-stream error — reconnecting");
                        return PumpOutcome::Recoverable(e);
                    }
                    return PumpOutcome::Terminal(e);
                }
                Ok(None) => return PumpOutcome::Finished,
            },
        }
    }
}

/// Run the bind loop for a vendor whose requests never carry replayed reasoning
/// payloads.
///
/// The payload knob is inert and the fallback never fires, because no payload is
/// ever reported. Delegates to [`run_http_bind_loop_with_replay`], so a vendor
/// without an echo mechanism still gets exactly the same retry, watchdog and
/// close behavior as one with it.
pub async fn run_http_bind_loop<F, Fut>(
    req_rx: UnboundedReceiver<ProviderRequest>,
    resp_tx: UnboundedSender<ProviderResponse>,
    open_stream: F,
) where
    F: Fn(Vec<Block>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<EventStream, LlmError>> + Send + 'static,
{
    run_http_bind_loop_with_replay(
        req_rx,
        resp_tx,
        move |blocks, model, tools, reasoning, _include_reasoning_payloads| {
            let events = open_stream(blocks, model, tools, reasoning);
            async move {
                Ok(OpenedTurn {
                    events: events.await?,
                    carried_payloads: false,
                })
            }
        },
    )
    .await;
}

/// Run the bind loop for a vendor that replays stored reasoning payloads.
///
/// Waits for a stream request, calls `open_stream` to create the event stream,
/// then pumps it. An interrupt cancels the active stream. The loop exits when
/// the request channel is dropped.
///
/// `open_stream`'s final argument is the payload knob: `true` on the first
/// attempt, and `false` on the single re-open the cycle allows itself when a
/// payload-carrying attempt dies on a terminal API error. The returned
/// [`OpenedTurn`] reports whether the built request actually carried payloads —
/// no payloads, no fallback.
pub async fn run_http_bind_loop_with_replay<F, Fut>(
    mut req_rx: UnboundedReceiver<ProviderRequest>,
    resp_tx: UnboundedSender<ProviderResponse>,
    open_stream: F,
) where
    F: Fn(Vec<Block>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>, bool) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<OpenedTurn, LlmError>> + Send + 'static,
{
    let open_stream = std::sync::Arc::new(open_stream);
    let mut active: Option<CancellationToken> = None;

    while let Some(req) = req_rx.recv().await {
        match req {
            ProviderRequest::Stream {
                blocks,
                model,
                tools,
                reasoning,
            } => {
                if let Some(token) = active.take() {
                    token.cancel();
                }

                let token = CancellationToken::new();
                active = Some(token.clone());

                // The whole open-pump-reconnect cycle runs in a task of its own
                // so this loop stays responsive to a follow-up request that
                // would cancel it.
                tokio::spawn(run_stream_cycle(
                    open_stream.clone(),
                    resp_tx.clone(),
                    token,
                    TurnRequest {
                        blocks,
                        model,
                        tools,
                        reasoning,
                    },
                ));
            }
            ProviderRequest::Interrupt => {
                if let Some(token) = active.take() {
                    token.cancel();
                }
            }
        }
    }
}

/// One turn's inputs, kept together because every re-open of the SAME turn must
/// use the SAME ones — a reconnect that rebuilt them from anything live would
/// silently change the request mid-turn.
struct TurnRequest {
    blocks: Vec<Block>,
    model: ModelSelector,
    tools: Vec<ToolDefinition>,
    reasoning: Option<ReasoningLevel>,
}

/// Drive one turn end to end: open the stream, retrying rate limits; pump it;
/// and on a recoverable mid-stream drop restart-clean and re-open the SAME
/// turn, bounded by the retry cap and cancellable through the token.
///
/// ```text
///   Opening ──Ok──▶ Pumping ──Finished──▶ Done
///      │ │              │
///      │ │              ├─Recoverable──▶ Reconnecting ──(< MAX)──▶ Opening
///      │ │              │                     └─(= MAX)──▶ Error ─▶ Done
///      │ │              └─Terminal(Api, payloads carried, first time)
///      │ │                     └──▶ fallback: Restart ─▶ Opening (payloads OFF)
///      │ │                └─Terminal(otherwise)──▶ Error ─▶ Done
///      │ └─RateLimited(< MAX)──▶ (backoff) ─▶ Opening
///      │ └─RateLimited(= MAX)/other Err──▶ Error ─▶ Done
///      └─Cancelled (any state) ──▶ (silent: no Done — a new turn/teardown follows)
/// ```
///
/// `Restart` — discard the turn's uncommitted blocks — is emitted at the
/// Pumping-to-Reconnecting edge and at the fallback edge, BEFORE the re-open, so
/// the regenerated stream writes onto a clean slate. A reconnect never closes
/// the stream; only the terminal states do.
///
/// The payload-omitting fallback is a deliberate ONE-SHOT request-shape retry,
/// distinct from a transport reconnect: a terminal API error from an attempt
/// whose build reported replayed reasoning payloads re-opens exactly once with
/// the payloads omitted. Continuity degrades — the reasoning text still renders
/// — and the turn survives, which is the trade being made. An attempt that
/// carried no payloads surfaces the error directly.
async fn run_stream_cycle<F, Fut>(
    open_stream: std::sync::Arc<F>,
    resp_tx: UnboundedSender<ProviderResponse>,
    token: CancellationToken,
    turn: TurnRequest,
) where
    F: Fn(Vec<Block>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>, bool) -> Fut
        + Send
        + Sync,
    Fut: Future<Output = Result<OpenedTurn, LlmError>> + Send,
{
    // Counts recoverable mid-stream reconnects across the whole turn.
    let mut reconnects = 0_u32;
    let mut reconnect_backoff = MIN_BACKOFF_SECS;
    // Payloads ride the request until, at most once, the fallback fires.
    let mut include_payloads = true;
    let mut payload_fallback_spent = false;

    loop {
        // ── Opening, with rate-limit retry ────────────────────────────────
        let Some(opened) = open_with_rate_limit_retry(
            open_stream.as_ref(),
            &resp_tx,
            &token,
            &turn,
            include_payloads,
        )
        .await
        else {
            // Cancelled while waiting out a rate limit: a new turn or a
            // teardown follows, so the channel is left untouched.
            return;
        };

        let OpenedTurn {
            mut events,
            carried_payloads,
        } = match opened {
            Ok(opened) => opened,
            Err(e) => {
                let _ = resp_tx.send(ProviderResponse::Error(e.to_string()));
                let _ = resp_tx.send(ProviderResponse::Done);
                return;
            }
        };

        // ── Pumping ───────────────────────────────────────────────────────
        match pump_stream(&mut events, &resp_tx, &token).await {
            PumpOutcome::Finished => {
                let _ = resp_tx.send(ProviderResponse::Done);
                return;
            }
            PumpOutcome::Cancelled => return,
            PumpOutcome::Terminal(err) => {
                // ── The one-shot payload-omitting fallback ────────────────
                if carried_payloads
                    && !payload_fallback_spent
                    && matches!(err, LlmError::Api { .. })
                {
                    payload_fallback_spent = true;
                    include_payloads = false;
                    warn!(
                        error = %err,
                        "payload-carrying request rejected — retrying once without reasoning payloads"
                    );
                    let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::ProviderStatus {
                        label:
                            "Stored reasoning rejected — retrying without it (continuity degraded)"
                                .into(),
                    }));
                    // Discard the failed attempt's uncommitted blocks before
                    // the re-open, the same as a reconnect does.
                    let _ = resp_tx.send(ProviderResponse::Restart);
                    continue;
                }
                let _ = resp_tx.send(ProviderResponse::Error(err.to_string()));
                let _ = resp_tx.send(ProviderResponse::Done);
                return;
            }
            PumpOutcome::Recoverable(err) => {
                // ── Reconnecting, restart-clean ───────────────────────────
                reconnects += 1;
                if reconnects > MAX_RETRIES {
                    warn!(
                        attempts = MAX_RETRIES,
                        error = %err,
                        "stream drop — giving up after max reconnects"
                    );
                    let _ = resp_tx.send(ProviderResponse::Error(format!(
                        "Connection interrupted after {MAX_RETRIES} attempts: {err}"
                    )));
                    let _ = resp_tx.send(ProviderResponse::Done);
                    return;
                }

                let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::ProviderStatus {
                    label: "Connection lost, reconnecting…".into(),
                }));
                // Discard the turn's uncommitted blocks BEFORE the re-open so
                // the regenerated stream starts clean, with no duplicates.
                let _ = resp_tx.send(ProviderResponse::Restart);

                tokio::select! {
                    () = token.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(reconnect_backoff)) => {}
                }
                reconnect_backoff = (reconnect_backoff * 2).min(MAX_BACKOFF_SECS);
                // Loop back to Opening: the SAME turn, same blocks and tools.
            }
        }
    }
}

/// Open the turn, retrying while the provider says it is rate limited.
///
/// It honours the server's own hint when it gave one and otherwise doubles a
/// bounded backoff, because a rate limit answered by an immediate retry is how
/// a client turns a brief throttle into a longer one.
///
/// `None` means the turn was cancelled mid-wait, and the caller must return
/// without closing the channel.
async fn open_with_rate_limit_retry<F, Fut>(
    open_stream: &F,
    resp_tx: &UnboundedSender<ProviderResponse>,
    token: &CancellationToken,
    turn: &TurnRequest,
    include_payloads: bool,
) -> Option<Result<OpenedTurn, LlmError>>
where
    F: Fn(Vec<Block>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>, bool) -> Fut
        + Send
        + Sync,
    Fut: Future<Output = Result<OpenedTurn, LlmError>> + Send,
{
    let mut backoff = MIN_BACKOFF_SECS;
    let mut attempt = 0;

    loop {
        attempt += 1;
        match open_stream(
            turn.blocks.clone(),
            turn.model.clone(),
            turn.tools.clone(),
            turn.reasoning,
            include_payloads,
        )
        .await
        {
            Err(LlmError::RateLimited { retry_after_secs }) if attempt <= MAX_RETRIES => {
                let wait = retry_after_secs.unwrap_or(backoff);
                let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::ProviderStatus {
                    label: format!("Rate limited, retrying in {wait}s…"),
                }));
                tokio::select! {
                    () = token.cancelled() => return None,
                    () = tokio::time::sleep(Duration::from_secs(wait)) => {}
                }
                backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
            }
            Err(e @ LlmError::RateLimited { .. }) => {
                warn!(
                    attempts = MAX_RETRIES,
                    "rate limited, giving up after max retries"
                );
                return Some(Err(e));
            }
            other => return Some(other),
        }
    }
}

#[cfg(test)]
mod bind_loop_tests;
#[cfg(test)]
mod payload_fallback_tests;
