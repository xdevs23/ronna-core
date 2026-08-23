//! The payload-omitting fallback: a terminal API error from an attempt that
//! carried replayed reasoning payloads earns exactly one retry with the
//! payloads omitted, and a payload-free attempt surfaces the error directly.
//!
//! "Exactly one" is the whole property. Two would double every user's wait on a
//! provider that is simply refusing the request; zero would turn a rejected
//! continuity payload into a dead conversation the user cannot continue.

use std::sync::{Arc, Mutex};

use futures::stream;

use super::*;
use crate::providers::types::{StopReason, Usage};

/// Drive the replay loop with a single stream request, recording each attempt's
/// payload knob.
async fn drive_replay(
    open: impl Fn(bool) -> Result<OpenedTurn, LlmError> + Send + Sync + 'static,
) -> (Vec<ProviderResponse>, Vec<bool>) {
    let knobs = Arc::new(Mutex::new(Vec::new()));
    let seen = knobs.clone();

    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();

    let loop_handle = tokio::spawn(run_http_bind_loop_with_replay(
        req_rx,
        resp_tx,
        move |_b, _m, _t, _r, include| {
            seen.lock().unwrap().push(include);
            let turn = open(include);
            async move { turn }
        },
    ));

    req_tx
        .send(ProviderRequest::Stream {
            messages: vec![],
            model: ModelSelector::Lightweight {
                main: "main-model".into(),
            },
            tools: vec![],
            reasoning: None,
        })
        .unwrap();

    // Held until the turn closes: dropping the sender cancels the active turn,
    // which is a teardown rather than a way to end the request stream early.
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
    let knobs = knobs.lock().unwrap().clone();
    (out, knobs)
}

fn api_error_stream() -> EventStream {
    Box::pin(stream::iter(vec![
        Ok(StreamEvent::Connected),
        Ok(StreamEvent::TextDelta {
            text: "partial".into(),
        }),
        Err(LlmError::Api {
            status: 400,
            message: "reasoning item invalid".into(),
        }),
    ]))
}

fn clean_stream() -> EventStream {
    Box::pin(stream::iter(vec![
        Ok(StreamEvent::Connected),
        Ok(StreamEvent::TextDelta {
            text: "degraded but alive".into(),
        }),
        Ok(StreamEvent::MessageEnd {
            usage: Usage::default(),
            stop_reason: StopReason::EndTurn,
        }),
    ]))
}

/// A terminal API error on a payload-carrying attempt earns exactly one retry
/// with the knob off, plus the degraded-continuity status and a restart, and the
/// turn then succeeds with no error surfaced.
#[tokio::test(start_paused = true)]
async fn api_error_on_payload_attempt_retries_once_without_payloads() {
    let (out, knobs) = drive_replay(|include| {
        if include {
            Ok(OpenedTurn {
                events: api_error_stream(),
                carried_payloads: true,
            })
        } else {
            Ok(OpenedTurn {
                events: clean_stream(),
                carried_payloads: false,
            })
        }
    })
    .await;

    assert_eq!(
        knobs,
        vec![true, false],
        "exactly one fallback attempt, payloads off"
    );
    assert!(
        out.iter().any(|r| matches!(
            r,
            ProviderResponse::Event(StreamEvent::ProviderStatus { label })
                if label.contains("continuity degraded")
        )),
        "a status line notes the degraded continuity"
    );
    assert!(
        out.iter().any(|r| matches!(r, ProviderResponse::Restart)),
        "the failed attempt's uncommitted blocks are discarded before the re-open"
    );
    assert!(
        !out.iter().any(|r| matches!(r, ProviderResponse::Error(_))),
        "no error on success"
    );
    assert!(matches!(out.last(), Some(ProviderResponse::Done)));
}

/// The fallback attempt failing too surfaces the error: the fallback is strictly
/// one-shot, two attempts total, never a third.
#[tokio::test(start_paused = true)]
async fn second_failure_surfaces_error_after_single_fallback() {
    let (out, knobs) = drive_replay(|include| {
        Ok(OpenedTurn {
            events: api_error_stream(),
            carried_payloads: include,
        })
    })
    .await;

    assert_eq!(knobs, vec![true, false], "one-shot: exactly two attempts");
    let error_idx = out
        .iter()
        .position(
            |r| matches!(r, ProviderResponse::Error(e) if e.contains("reasoning item invalid")),
        )
        .expect("the terminal error surfaces after the failed fallback");
    assert_eq!(
        error_idx,
        out.len() - 2,
        "the error immediately precedes the single close"
    );
    assert!(matches!(out.last(), Some(ProviderResponse::Done)));
}

/// An attempt whose build reported NO payloads gets no fallback — the terminal
/// API error surfaces directly, after a single attempt.
#[tokio::test(start_paused = true)]
async fn no_payload_attempt_surfaces_error_without_retry() {
    let (out, knobs) = drive_replay(|_include| {
        Ok(OpenedTurn {
            events: api_error_stream(),
            carried_payloads: false,
        })
    })
    .await;

    assert_eq!(knobs, vec![true], "no payloads means no fallback attempt");
    assert!(out.iter().any(|r| matches!(r, ProviderResponse::Error(_))));
    assert!(!out.iter().any(|r| matches!(r, ProviderResponse::Restart)));
    assert!(matches!(out.last(), Some(ProviderResponse::Done)));
}
