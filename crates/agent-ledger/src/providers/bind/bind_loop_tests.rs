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
    open: impl Fn(
        Vec<Message>,
        ModelSelector,
        Vec<ToolDefinition>,
        Option<ReasoningLevel>,
    ) -> EventStream
    + Send
    + Sync
    + 'static,
) -> Vec<ProviderResponse> {
    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();

    let loop_handle = tokio::spawn(run_http_bind_loop(req_rx, resp_tx, move |b, m, t, r| {
        let events = open(b, m, t, r);
        async move { Ok::<_, LlmError>(events) }
    }));

    req_tx
        .send(ProviderRequest::Stream {
            messages: vec![],
            model: ModelSelector::Lightweight,
            tools: vec![],
            reasoning: None,
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
    let out = drive(move |_, _, _, _| {
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
        !out.iter().any(|r| matches!(r, ProviderResponse::Error(_))),
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
/// terminates with a transport error followed by a single close.
#[tokio::test(start_paused = true)]
async fn exhausts_retries_then_errors() {
    let out = drive(|_, _, _, _| {
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
        .position(
            |r| matches!(r, ProviderResponse::Error(e) if e.contains("Connection interrupted")),
        )
        .expect("a terminal connection error is emitted");
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
    let out = drive(move |_, _, _, _| {
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
        !out.iter().any(|r| matches!(r, ProviderResponse::Error(_))),
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
/// partial answer the reader already has, once as the regenerated one.
#[tokio::test(start_paused = true)]
async fn rate_limit_after_content_is_terminal() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let out = drive(move |_, _, _, _| {
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
        .position(|r| matches!(r, ProviderResponse::Error(e) if e.contains("rate limited")))
        .expect("the rate limit surfaces as the turn's error");
    assert_eq!(
        error_idx,
        out.len() - 2,
        "the error immediately precedes the single close"
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
    let out = drive(move |_, _, _, _| {
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

    let loop_handle = tokio::spawn(run_http_bind_loop(
        req_rx,
        resp_tx,
        move |_b, _m, _t, _r| {
            counted.fetch_add(1, Ordering::SeqCst);
            // A stream that opens and then says nothing: the shape a cycle can sit
            // in for minutes.
            let events =
                Box::pin(stream::iter(vec![Ok(StreamEvent::Connected)]).chain(stream::pending()))
                    as EventStream;
            async move { Ok::<_, LlmError>(events) }
        },
    ));

    req_tx
        .send(ProviderRequest::Stream {
            messages: vec![],
            model: ModelSelector::Lightweight,
            tools: vec![],
            reasoning: None,
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
        move |_b, _m, _t, _r, _include| {
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
        model: ModelSelector::Lightweight,
        tools: vec![],
        reasoning: None,
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
            |r| matches!(r, ProviderResponse::Error(e) if e.contains("the first turn failed"))
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
