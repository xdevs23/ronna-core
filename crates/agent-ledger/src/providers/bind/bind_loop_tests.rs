//! Reconnect behavior, pinned at the level a reader cares about: what a
//! consumer of the channel actually observes, and in what order.
//!
//! Time is paused in these tests, so a five-minute backoff resolves instantly
//! and the assertions are about ordering rather than about waiting.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use futures::stream;

use super::*;
use crate::providers::types::{StopReason, Usage};

/// Drive the loop with a single stream request and collect every response until
/// the channel closes.
async fn drive(
    open: impl Fn(TurnRequest) -> EventStream + Send + Sync + 'static,
) -> Vec<ProviderResponse> {
    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();

    let loop_handle = tokio::spawn(run_http_bind_loop(req_rx, resp_tx, move |turn| {
        let events = open(turn);
        async move { Ok::<_, LlmError>(events) }
    }));

    req_tx
        .send(ProviderRequest::Stream {
            messages: vec![],
            model: ModelSelector::Lightweight {
                main: "main-model".into(),
            },
            tools: vec![],
            reasoning: None,
            response_schema: None,
        })
        .unwrap();

    // The sender is held until the turn closes. Dropping it IS a teardown —
    // the loop cancels the active turn on its way out — so an early drop would
    // cancel the very turn these assertions are about.
    let mut out = Vec::new();
    while let Some(resp) = resp_rx.recv().await {
        let done = matches!(resp, ProviderResponse::Done);
        out.push(resp);
        if done {
            break;
        }
    }
    drop(req_tx);
    loop_handle.abort();
    out
}

#[tokio::test]
async fn reconnect_log_keeps_the_http_errors_source() {
    let error = crate::providers::http::client()
        .get("not-an-absolute-url")
        .build()
        .expect_err("a relative URL cannot build a request");
    let mut events: EventStream = Box::pin(stream::iter([Err(LlmError::Http(error))]));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let (outcome, lines) = crate::providers::empty::capture::recorded_future(pump_stream(
        &mut events,
        &tx,
        &CancellationToken::new(),
    ))
    .await;
    assert!(matches!(outcome, PumpOutcome::Recoverable(_)));
    let line = lines
        .iter()
        .find(|line| line.contains("recoverable mid-stream error"))
        .expect("the reconnect is logged");
    assert!(line.contains("source: RelativeUrlWithoutBase"), "{line}");
}

fn is_status(resp: &ProviderResponse, needle: &str) -> bool {
    matches!(resp, ProviderResponse::Event(StreamEvent::ProviderStatus { label }) if label.contains(needle))
}

/// A recoverable mid-stream drop on the first attempt, then a clean stream: the
/// loop emits the reconnecting status and a restart — and NO close — between
/// attempts, then a single close after the successful stream finishes.
#[tokio::test(start_paused = true)]
async fn reconnects_then_completes() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let out = drive(move |_| {
        let n = a.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            // First attempt: connect, then drop mid-stream.
            Box::pin(stream::iter(vec![
                Ok(StreamEvent::Connected),
                Ok(StreamEvent::TextDelta {
                    text: "partial".into(),
                }),
                Err(LlmError::Stream("error decoding response body".into())),
            ])) as EventStream
        } else {
            // Second attempt: a clean, fully-completed stream.
            Box::pin(stream::iter(vec![
                Ok(StreamEvent::Connected),
                Ok(StreamEvent::TextDelta {
                    text: "regenerated".into(),
                }),
                Ok(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }),
            ])) as EventStream
        }
    })
    .await;

    // Exactly one reconnect status and one restart, both BEFORE any close.
    let restart_idx = out
        .iter()
        .position(|r| matches!(r, ProviderResponse::Restart))
        .expect("a restart is emitted");
    let status_idx = out
        .iter()
        .position(|r| is_status(r, "reconnecting"))
        .expect("a reconnecting status is emitted");
    assert!(status_idx < restart_idx, "the status precedes the restart");

    assert!(
        !out.iter()
            .any(|r| matches!(r, ProviderResponse::Error { .. })),
        "no error on eventual success"
    );
    let done_positions: Vec<usize> = out
        .iter()
        .enumerate()
        .filter(|(_, r)| matches!(r, ProviderResponse::Done))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        done_positions,
        vec![out.len() - 1],
        "exactly one close, and it is last"
    );
    assert!(
        restart_idx < done_positions[0],
        "the restart is emitted before the only close"
    );
}

/// A stream that always drops recoverably exhausts the reconnect cap, then
/// terminates with a transport error followed by a single close. The error is
/// retryable: a connection lost past the reconnects can pass on a later resend.
#[tokio::test(start_paused = true)]
async fn exhausts_retries_then_errors() {
    let out = drive(|_| {
        Box::pin(stream::iter(vec![
            Ok(StreamEvent::Connected),
            Err(LlmError::Stream("error decoding response body".into())),
        ])) as EventStream
    })
    .await;

    let restarts = out
        .iter()
        .filter(|r| matches!(r, ProviderResponse::Restart))
        .count();
    assert_eq!(
        u32::try_from(restarts).unwrap(),
        MAX_RETRIES,
        "one restart per reconnect, capped"
    );

    let error_idx = out
        .iter()
        .position(|r| {
            matches!(
                r,
                ProviderResponse::Error { message, retryable: true }
                    if message.contains("Connection interrupted")
            )
        })
        .expect("a retryable terminal connection error is emitted");
    assert!(
        !out[..error_idx]
            .iter()
            .any(|r| matches!(r, ProviderResponse::Done)),
        "no close before the terminal error"
    );

    let done_positions: Vec<usize> = out
        .iter()
        .enumerate()
        .filter(|(_, r)| matches!(r, ProviderResponse::Done))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(done_positions, vec![out.len() - 1]);
    assert_eq!(
        error_idx,
        out.len() - 2,
        "the error immediately precedes the single close"
    );
}

// ─── Rate limits arriving on the stream ──────────────────────────────────────

/// Every vendor here reports a refusal as the stream's FIRST ITEM rather than
/// as an error from the opener, so this is the shape a 429 actually takes. Sent
/// before any content, it costs one wait and the turn then succeeds — the
/// retry the opener path was written for, reached the way real streams reach
/// it.
#[tokio::test(start_paused = true)]
async fn rate_limited_stream_retries_then_succeeds() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let out = drive(move |_| {
        if a.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(stream::iter(vec![
                Ok(StreamEvent::Connected),
                Err(LlmError::RateLimited {
                    retry_after_secs: None,
                }),
            ])) as EventStream
        } else {
            Box::pin(stream::iter(vec![
                Ok(StreamEvent::Connected),
                Ok(StreamEvent::TextDelta {
                    text: "the answer".into(),
                }),
                Ok(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }),
            ])) as EventStream
        }
    })
    .await;

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the refused turn is re-opened exactly once"
    );
    assert!(
        out.iter().any(|r| is_status(r, "Rate limited")),
        "the wait is announced to whoever is watching"
    );
    assert!(
        !out.iter()
            .any(|r| matches!(r, ProviderResponse::Error { .. })),
        "a waited-out rate limit is not an error"
    );
    assert!(
        !out.iter().any(|r| matches!(r, ProviderResponse::Restart)),
        "nothing was written down, so there is nothing to discard"
    );
    assert!(
        out.iter().any(
            |r| matches!(r, ProviderResponse::Event(StreamEvent::TextDelta { text }) if text == "the answer")
        ),
        "the retried turn's answer reaches the reader"
    );
    assert!(matches!(out.last(), Some(ProviderResponse::Done)));
}

/// A rate limit AFTER content has flowed is terminal. The turn is already half
/// delivered, and a silent replay would write it down twice — once as the
/// partial answer the reader already has, once as the regenerated one. The
/// error is retryable: a later resend starts the turn over.
#[tokio::test(start_paused = true)]
async fn rate_limit_after_content_is_terminal() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let out = drive(move |_| {
        a.fetch_add(1, Ordering::SeqCst);
        Box::pin(stream::iter(vec![
            Ok(StreamEvent::Connected),
            Ok(StreamEvent::TextDelta {
                text: "half an answer".into(),
            }),
            Err(LlmError::RateLimited {
                retry_after_secs: Some(1),
            }),
        ])) as EventStream
    })
    .await;

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a half-delivered turn is never replayed"
    );
    let error_idx = out
        .iter()
        .position(|r| {
            matches!(
                r,
                ProviderResponse::Error { message, retryable: true } if message.contains("rate limited")
            )
        })
        .expect("the rate limit surfaces as the turn's retryable error");
    assert_eq!(
        error_idx,
        out.len() - 2,
        "the error immediately precedes the single close"
    );
    assert!(matches!(out.last(), Some(ProviderResponse::Done)));
}

/// A stream that keeps refusing with a rate limit spends the budget of the
/// turn, and the turn then closes with a retryable error: the limit only asks
/// to wait longer than one turn may.
#[tokio::test(start_paused = true)]
async fn spent_rate_limit_budget_closes_with_a_retryable_error() {
    let out = drive(|_| {
        Box::pin(stream::iter(vec![
            Ok(StreamEvent::Connected),
            Err(LlmError::RateLimited {
                retry_after_secs: None,
            }),
        ])) as EventStream
    })
    .await;

    let errors: Vec<_> = out
        .iter()
        .filter_map(|r| match r {
            ProviderResponse::Error { message, retryable } => Some((message.as_str(), *retryable)),
            _ => None,
        })
        .collect();
    assert_eq!(errors, vec![("rate limited", true)]);
    assert_eq!(
        out.iter().filter(|r| is_status(r, "Rate limited")).count(),
        usize::try_from(MAX_RETRIES).unwrap(),
        "every wait of the budget was taken first"
    );
    assert!(matches!(out.last(), Some(ProviderResponse::Done)));
}

/// The server's own hint decides the wait. Guessing sooner is how a client
/// turns a brief throttle into a longer one, so a stated ten minutes is waited
/// out — capped — rather than replaced by the opening backoff of two seconds.
#[tokio::test(start_paused = true)]
async fn retry_after_hint_is_honored() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();

    let started = tokio::time::Instant::now();
    let out = drive(move |_| {
        if a.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(stream::iter(vec![
                Ok(StreamEvent::Connected),
                Err(LlmError::RateLimited {
                    retry_after_secs: Some(120),
                }),
            ])) as EventStream
        } else {
            Box::pin(stream::iter(vec![Ok(StreamEvent::MessageEnd {
                usage: Usage::default(),
                stop_reason: StopReason::EndTurn,
            })])) as EventStream
        }
    })
    .await;
    let waited = started.elapsed();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(
        waited >= Duration::from_mins(2) && waited < Duration::from_secs(121),
        "the server's hint is the wait, not the client's backoff: {waited:?}"
    );
    assert!(
        out.iter().any(|r| is_status(r, "retrying in 120s")),
        "the announced wait is the one actually taken"
    );
    assert!(matches!(out.last(), Some(ProviderResponse::Done)));
}

// ─── Teardown ────────────────────────────────────────────────────────────────

/// Dropping the request sender tears the active turn down.
///
/// The sender is the binding's owner. Without an explicit cancel the cycle
/// survives it: the idle watchdog trips, the turn reconnects, and the whole
/// backoff ladder plays out against a receiver nobody will read again.
#[tokio::test(start_paused = true)]
async fn dropping_the_sender_stops_the_cycle() {
    let opens = Arc::new(AtomicU32::new(0));
    let counted = opens.clone();

    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();

    let loop_handle = tokio::spawn(run_http_bind_loop(req_rx, resp_tx, move |_turn| {
        counted.fetch_add(1, Ordering::SeqCst);
        // A stream that opens and then says nothing: the shape a cycle can sit
        // in for minutes.
        let events =
            Box::pin(stream::iter(vec![Ok(StreamEvent::Connected)]).chain(stream::pending()))
                as EventStream;
        async move { Ok::<_, LlmError>(events) }
    }));

    req_tx
        .send(ProviderRequest::Stream {
            messages: vec![],
            model: ModelSelector::Lightweight {
                main: "main-model".into(),
            },
            tools: vec![],
            reasoning: None,
            response_schema: None,
        })
        .unwrap();

    assert!(matches!(
        resp_rx.recv().await,
        Some(ProviderResponse::Event(StreamEvent::Connected))
    ));
    drop(req_tx);

    // The cycle stops, dropping its half of the response channel with it, and
    // it says nothing on the way out: no close belongs to a turn nobody asked
    // to finish.
    let mut tail = Vec::new();
    while let Some(resp) = resp_rx.recv().await {
        tail.push(resp);
    }
    assert!(
        tail.is_empty(),
        "a torn-down turn emits nothing further, got {tail:?}"
    );
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "the turn is not re-opened after its owner is gone"
    );
    loop_handle.await.expect("the loop exits with the channel");
}

/// A turn superseded by a newer request on the same binding closes NOTHING.
///
/// The first turn is cancelled while its opener is still in flight and then
/// fails; its `Done` would arrive after the second turn had already started,
/// telling the reader that the new turn had finished before it produced a word.
#[tokio::test(start_paused = true)]
async fn a_superseded_turn_sends_no_close() {
    let release_first = Arc::new(tokio::sync::Notify::new());
    let gate = release_first.clone();
    let attempts = Arc::new(AtomicU32::new(0));
    let counted = attempts.clone();

    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();

    let loop_handle = tokio::spawn(run_http_bind_loop_with_replay(
        req_rx,
        resp_tx,
        move |_turn, _include| {
            let first = counted.fetch_add(1, Ordering::SeqCst) == 0;
            let gate = gate.clone();
            async move {
                if first {
                    // Still opening when the newer request arrives, and then
                    // failing — the moment a stale close would be sent.
                    gate.notified().await;
                    return Err(LlmError::Config("the first turn failed".into()));
                }
                Ok(OpenedTurn {
                    events: Box::pin(stream::iter(vec![
                        Ok(StreamEvent::TextDelta {
                            text: "the newer turn".into(),
                        }),
                        Ok(StreamEvent::MessageEnd {
                            usage: Usage::default(),
                            stop_reason: StopReason::EndTurn,
                        }),
                    ])) as EventStream,
                    carried_payloads: false,
                })
            }
        },
    ));

    let request = || ProviderRequest::Stream {
        messages: vec![],
        model: ModelSelector::Lightweight {
            main: "main-model".into(),
        },
        tools: vec![],
        reasoning: None,
        response_schema: None,
    };
    req_tx.send(request()).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    req_tx.send(request()).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // The superseded turn now finishes failing, after its replacement is done.
    release_first.notify_waiters();
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(req_tx);

    let mut out = Vec::new();
    while let Some(resp) = resp_rx.recv().await {
        out.push(resp);
    }

    assert_eq!(attempts.load(Ordering::SeqCst), 2, "both turns were opened");
    assert!(
        !out.iter().any(
            |r| matches!(r, ProviderResponse::Error { message, .. } if message.contains("the first turn failed"))
        ),
        "the superseded turn's error is not surfaced onto the new turn: {out:?}"
    );
    let closes = out
        .iter()
        .filter(|r| matches!(r, ProviderResponse::Done))
        .count();
    assert_eq!(closes, 1, "exactly one close, from the turn that finished");
    assert!(
        matches!(out.last(), Some(ProviderResponse::Done)),
        "and it is last"
    );
    loop_handle.await.expect("the loop exits with the channel");
}

/// The schema is part of the turn's inputs: every re-open of the SAME turn
/// hands the opener the same one, so a reconnect can never silently turn a
/// schema-bound turn into a prose one.
#[tokio::test(start_paused = true)]
async fn every_reopen_of_a_turn_carries_the_same_schema() {
    let schema = serde_json::json!({ "type": "object" });
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();
    let recorded = seen.clone();
    let loop_handle = tokio::spawn(run_http_bind_loop(req_rx, resp_tx, move |turn| {
        let first = recorded.lock().unwrap().is_empty();
        recorded.lock().unwrap().push(turn.response_schema);
        let events = if first {
            vec![
                Ok(StreamEvent::Connected),
                Err(LlmError::Stream("error decoding response body".into())),
            ]
        } else {
            vec![
                Ok(StreamEvent::Connected),
                Ok(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }),
            ]
        };
        async move { Ok::<_, LlmError>(Box::pin(stream::iter(events)) as EventStream) }
    }));
    req_tx
        .send(ProviderRequest::Stream {
            messages: vec![],
            model: ModelSelector::Specific("m".into()),
            tools: vec![],
            reasoning: None,
            response_schema: Some(schema.clone()),
        })
        .unwrap();
    while let Some(resp) = resp_rx.recv().await {
        if matches!(resp, ProviderResponse::Done) {
            break;
        }
    }
    drop(req_tx);
    loop_handle.await.expect("the loop exits with the channel");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![Some(schema.clone()), Some(schema)]
    );
}
