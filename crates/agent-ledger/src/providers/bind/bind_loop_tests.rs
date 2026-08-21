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
    open: impl Fn(Vec<Block>, ModelSelector, Vec<ToolDefinition>, Option<ReasoningLevel>) -> EventStream
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
            blocks: vec![],
            model: ModelSelector::Lightweight,
            tools: vec![],
            reasoning: None,
        })
        .unwrap();
    // The loop exits once the cycle task finishes draining requests.
    drop(req_tx);

    let mut out = Vec::new();
    while let Some(resp) = resp_rx.recv().await {
        let done = matches!(resp, ProviderResponse::Done);
        out.push(resp);
        if done {
            break;
        }
    }
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
