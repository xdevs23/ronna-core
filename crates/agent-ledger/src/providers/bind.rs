//! The bind loop: one turn, driven end to end, with the resilience every
//! HTTP-based vendor inherits without writing any of it.
//!
//! A vendor module supplies one thing — a function that opens a stream — and
//! gets the rest: a rate-limit retry with the server's own backoff — whether
//! the refusal arrives as the opener's error or as the first item of a stream
//! that opened fine — an idle watchdog, a restart-clean reconnect on a
//! recoverable drop, the one-shot request-shape fallback when a replayed
//! reasoning payload is rejected, and exactly one stream close. Each of those
//! exists because of a failure that reached a user, and each is stated here
//! once rather than per vendor.
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

use super::http;
use super::types::{
    EventStream, LlmError, Message, ModelSelector, ProviderRequest, ProviderResponse,
    ReasoningLevel, StreamEvent, ToolDefinition,
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

// Bounded backoff, shared by every retry path: a rate limit — however it
// arrives — and a recoverable mid-stream reconnect.
const MAX_RETRIES: u32 = 5;
const MIN_BACKOFF_SECS: u64 = 2;
const MAX_BACKOFF_SECS: u64 = 300;

/// The whole time one turn may spend waiting out rate limits, across every
/// attempt. The per-wait cap alone bounds nothing a user can feel: five waits
/// at the server's own hint of five minutes is nearly half an hour of a
/// conversation that looks frozen, so the sum is capped as well as each term.
const MAX_TOTAL_BACKOFF_SECS: u64 = 600;

/// The bounded waiting one turn is allowed while a provider says it is rate
/// limited.
///
/// One budget per turn, shared by both places a rate limit can arrive: as the
/// opener's error, and as the first item of a stream that opened fine. Two
/// budgets would let a provider that refuses both ways cost twice the wait it
/// was allowed.
struct RateLimitBudget {
    attempts: u32,
    backoff_secs: u64,
    spent_secs: u64,
}

impl RateLimitBudget {
    fn new() -> Self {
        Self {
            attempts: 0,
            backoff_secs: MIN_BACKOFF_SECS,
            spent_secs: 0,
        }
    }

    /// How long to wait before trying again, or `None` when the budget is
    /// spent and the rate limit must surface as the turn's error.
    ///
    /// The server's own hint wins when it gave one, because it knows when it
    /// will start answering and a client guessing sooner turns a brief throttle
    /// into a longer one. Both the single wait and the running total are
    /// bounded, so a hint of an hour cannot hold a turn open for one.
    fn next_wait(&mut self, retry_after_secs: Option<u64>) -> Option<Duration> {
        if self.attempts >= MAX_RETRIES {
            return None;
        }
        let wait = retry_after_secs
            .unwrap_or(self.backoff_secs)
            .min(MAX_BACKOFF_SECS);
        if wait > MAX_TOTAL_BACKOFF_SECS.saturating_sub(self.spent_secs) {
            return None;
        }

        self.attempts += 1;
        self.spent_secs += wait;
        self.backoff_secs = (self.backoff_secs * 2).min(MAX_BACKOFF_SECS);
        Some(Duration::from_secs(wait))
    }
}

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
    /// The provider answered the opened stream with a rate limit, before a
    /// single content event. Nothing of the turn has been written down, so the
    /// cycle can wait it out and re-open.
    RateLimited {
        /// The server's own hint, in whole seconds, when it gave one.
        retry_after_secs: Option<u64>,
    },
    /// A recoverable mid-stream drop. No error and no close were sent; the
    /// loop should restart-clean and re-open the same turn.
    Recoverable(LlmError),
    /// The active turn was cancelled. The channel was left untouched — no
    /// close, because a new turn or a teardown follows.
    Cancelled,
}

/// Whether an event is part of the turn's answer, as opposed to a note about
/// the connection carrying it.
///
/// This is what separates a rate limit the cycle may replay from one it must
/// not: once a word of the answer has reached the reader, a silent replay would
/// write the turn down twice.
fn is_content(event: &StreamEvent) -> bool {
    !matches!(
        event,
        StreamEvent::Connected | StreamEvent::ProviderStatus { .. }
    )
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
///
/// A rate limit reaching this far is a stream that opened and then refused —
/// how every vendor here reports a 429, since the opener returns the refusal as
/// the stream's first item rather than as an error at the call site. It is
/// replayable only while no content has flowed; after that it is terminal,
/// because a half-delivered turn that silently re-ran would be written down
/// twice.
async fn pump_stream(
    events: &mut EventStream,
    tx: &UnboundedSender<ProviderResponse>,
    cancel: &CancellationToken,
) -> PumpOutcome {
    let mut content_flowed = false;

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
                    content_flowed |= is_content(&e);
                    if tx.send(ProviderResponse::Event(e)).is_err() {
                        // The receiver is gone; there is nobody left to tell.
                        return PumpOutcome::Cancelled;
                    }
                }
                Ok(Some(Err(e))) => {
                    if let LlmError::RateLimited { retry_after_secs } = &e
                        && !content_flowed
                    {
                        warn!("rate limited before any content — retrying the turn");
                        return PumpOutcome::RateLimited { retry_after_secs: *retry_after_secs };
                    }
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
    F: Fn(Vec<Message>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<EventStream, LlmError>> + Send + 'static,
{
    run_http_bind_loop_with_replay(
        req_rx,
        resp_tx,
        move |messages, model, tools, reasoning, _include_reasoning_payloads| {
            let events = open_stream(messages, model, tools, reasoning);
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
/// the request channel is dropped, and cancels the active turn on its way out —
/// the sender is the binding's owner, so its disappearance ends the turn.
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
    F: Fn(Vec<Message>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>, bool) -> Fut
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
                messages,
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
                        messages,
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

    // The request channel closed. THE BINDING'S CONTRACT: dropping the sender
    // is teardown, immediately, INCLUDING a live turn — no final Done arrives,
    // because the close is suppressed on a cancelled token. A consumer that
    // wants a turn's completion holds the sender until Done; "drop the sender,
    // keep draining the receiver" is not a supported shape, and both in-crate
    // drive helpers hold the sender to the end for exactly that reason. The
    // cancel is EXPLICIT: dropping a token cancels nothing, and a cycle left
    // running would go on reconnecting — for minutes, with backoff — against a
    // receiver nobody may be reading.
    if let Some(token) = active.take() {
        token.cancel();
    }
}

/// One turn's inputs, kept together because every re-open of the SAME turn must
/// use the SAME ones — a reconnect that rebuilt them from anything live would
/// silently change the request mid-turn.
struct TurnRequest {
    messages: Vec<Message>,
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
///      │ │              ├─RateLimited (no content yet, budget left)
///      │ │              │      └──▶ (backoff) ─▶ Opening
///      │ │              ├─RateLimited (budget spent)──▶ Error ─▶ Done
///      │ │              └─Terminal(refusal, payloads carried, first time)
///      │ │                     └──▶ fallback: Restart ─▶ Opening (payloads OFF)
///      │ │                └─Terminal(otherwise)──▶ Error ─▶ Done
///      │ └─RateLimited(budget left)──▶ (backoff) ─▶ Opening
///      │ └─RateLimited(budget spent)/other Err──▶ Error ─▶ Done
///      └─Cancelled (any state) ──▶ (silent: no Done — a new turn/teardown follows)
/// ```
///
/// A rate limit reaches the cycle from either of two places and is answered the
/// same way from both, out of ONE budget: as the opener's error, and — the way
/// every vendor here actually reports one — as the first item of a stream that
/// opened successfully. A 429 that arrives after content has flowed is terminal
/// instead, because replaying a half-delivered turn writes it down twice.
///
/// `Restart` — discard the turn's uncommitted blocks — is emitted at the
/// Pumping-to-Reconnecting edge and at the fallback edge, BEFORE the re-open, so
/// the regenerated stream writes onto a clean slate. A rate-limit retry needs no
/// restart: it is granted only while nothing has been written down. A reconnect
/// never closes the stream; only the terminal states do.
///
/// The payload-omitting fallback is a deliberate ONE-SHOT request-shape retry,
/// distinct from a transport reconnect: a terminal refusal of an attempt whose
/// build reported replayed reasoning payloads re-opens exactly once with the
/// payloads omitted. Continuity degrades — the reasoning text still renders —
/// and the turn survives, which is the trade being made. An attempt that carried
/// no payloads surfaces the error directly.
async fn run_stream_cycle<F, Fut>(
    open_stream: std::sync::Arc<F>,
    resp_tx: UnboundedSender<ProviderResponse>,
    token: CancellationToken,
    turn: TurnRequest,
) where
    F: Fn(Vec<Message>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>, bool) -> Fut
        + Send
        + Sync,
    Fut: Future<Output = Result<OpenedTurn, LlmError>> + Send,
{
    // Counts recoverable mid-stream reconnects across the whole turn.
    let mut reconnects = 0_u32;
    let mut reconnect_backoff = MIN_BACKOFF_SECS;
    // One rate-limit budget for the whole turn, spent from either arrival.
    let mut rate_limit = RateLimitBudget::new();
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
            &mut rate_limit,
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
                close_turn(&resp_tx, &token, Some(e.to_string()));
                return;
            }
        };

        // ── Pumping ───────────────────────────────────────────────────────
        match pump_stream(&mut events, &resp_tx, &token).await {
            PumpOutcome::Finished => {
                close_turn(&resp_tx, &token, None);
                return;
            }
            PumpOutcome::Cancelled => return,
            PumpOutcome::RateLimited { retry_after_secs } => {
                // ── Waiting out a rate limit, with nothing written down ───
                if !wait_out_rate_limit(&resp_tx, &token, &mut rate_limit, retry_after_secs).await {
                    return;
                }
                // Loop back to Opening: the SAME turn, nothing to discard.
            }
            PumpOutcome::Terminal(err) => {
                // ── The one-shot payload-omitting fallback ────────────────
                if carried_payloads
                    && !payload_fallback_spent
                    && matches!(err, LlmError::Api { .. } | LlmError::ProviderFailure(_))
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
                close_turn(&resp_tx, &token, Some(err.to_string()));
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
                    close_turn(
                        &resp_tx,
                        &token,
                        Some(format!(
                            "Connection interrupted after {MAX_RETRIES} attempts: {err}"
                        )),
                    );
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
                // Loop back to Opening: the SAME turn, same messages and tools.
            }
        }
    }
}

/// Wait out a rate limit the opened stream reported, out of the turn's budget.
///
/// `true` means the cycle should re-open the same turn. `false` means it must
/// stop: either the turn was cancelled mid-wait, or the budget is spent and the
/// refusal has already been surfaced as the turn's error.
async fn wait_out_rate_limit(
    resp_tx: &UnboundedSender<ProviderResponse>,
    token: &CancellationToken,
    budget: &mut RateLimitBudget,
    retry_after_secs: Option<u64>,
) -> bool {
    let Some(wait) = budget.next_wait(retry_after_secs) else {
        warn!(
            attempts = MAX_RETRIES,
            "rate limited on the stream, giving up after the retry budget"
        );
        close_turn(
            resp_tx,
            token,
            Some(LlmError::RateLimited { retry_after_secs }.to_string()),
        );
        return false;
    };

    let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::ProviderStatus {
        label: format!("Rate limited, retrying in {}s…", wait.as_secs()),
    }));
    tokio::select! {
        () = token.cancelled() => false,
        () = tokio::time::sleep(wait) => true,
    }
}

/// Close a turn that is really over: the error, when there is one, then exactly
/// one `Done`.
///
/// A CANCELLED turn closes nothing. Cancellation means a newer request on this
/// binding has taken over — or the binding is being torn down — and a `Done`
/// from the superseded turn would tell the reader that the new turn had
/// finished before it produced a word. The check narrows that to the instant
/// between it and the send; without it the window is the whole remaining life
/// of the old cycle, which a reconnect can measure in minutes.
fn close_turn(
    resp_tx: &UnboundedSender<ProviderResponse>,
    token: &CancellationToken,
    error: Option<String>,
) {
    if token.is_cancelled() {
        return;
    }
    if let Some(message) = error {
        let _ = resp_tx.send(ProviderResponse::Error(message));
    }
    let _ = resp_tx.send(ProviderResponse::Done);
}

/// Open the turn, retrying while the provider says it is rate limited.
///
/// This is the arrival a vendor produces when its opener errors outright. Every
/// vendor here instead reports a refusal as the stream's first item, which the
/// pump answers out of the SAME budget — so the shape is covered whichever way
/// a vendor chooses to report it.
///
/// `None` means the turn was cancelled mid-wait, and the caller must return
/// without closing the channel.
async fn open_with_rate_limit_retry<F, Fut>(
    open_stream: &F,
    resp_tx: &UnboundedSender<ProviderResponse>,
    token: &CancellationToken,
    turn: &TurnRequest,
    include_payloads: bool,
    budget: &mut RateLimitBudget,
) -> Option<Result<OpenedTurn, LlmError>>
where
    F: Fn(Vec<Message>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>, bool) -> Fut
        + Send
        + Sync,
    Fut: Future<Output = Result<OpenedTurn, LlmError>> + Send,
{
    loop {
        let opened = open_stream(
            turn.messages.clone(),
            turn.model.clone(),
            turn.tools.clone(),
            turn.reasoning,
            include_payloads,
        )
        .await;

        let Err(LlmError::RateLimited { retry_after_secs }) = opened else {
            return Some(opened);
        };

        let Some(wait) = budget.next_wait(retry_after_secs) else {
            warn!(
                attempts = MAX_RETRIES,
                "rate limited, giving up after the retry budget"
            );
            return Some(Err(LlmError::RateLimited { retry_after_secs }));
        };

        let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::ProviderStatus {
            label: format!("Rate limited, retrying in {}s…", wait.as_secs()),
        }));
        tokio::select! {
            () = token.cancelled() => return None,
            () = tokio::time::sleep(wait) => {}
        }
    }
}

#[cfg(test)]
mod bind_loop_tests;
#[cfg(test)]
mod payload_fallback_tests;
