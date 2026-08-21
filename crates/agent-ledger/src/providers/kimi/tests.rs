//! This vendor's pins: the direct block-to-message build, and the one place its
//! reasoning must land.
//!
//! Six of the source's seven tests for this vendor are here; the seventh is
//! named with its reason in the provider module's header, because it pinned a
//! request builder this layer deliberately does not have.

use super::*;

fn block(id: i64, role: Role, block_type: &str, fields: &[(&str, &str)]) -> Block {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert((*k).into(), Value::String((*v).into()));
    }
    Block {
        id,
        role: Some(role),
        block_type: block_type.into(),
        created_at: String::new(),
        fields: map,
    }
}

/// The exact layout that produced a rejected request: an unfinalized reasoning
/// tail, then an unfinalized call tail, then the committed call — all one
/// assistant turn. The reasoning must ride on the same message as the call, and
/// the tail in between must not split the turn.
#[test]
fn streaming_thinking_survives_as_reasoning_on_tool_call() {
    let blocks = vec![
        block(1, Role::User, "text", &[("content", "hi")]),
        block(
            2,
            Role::Assistant,
            "streaming_thinking",
            &[("content", "let me think")],
        ),
        block(
            3,
            Role::Assistant,
            "streaming_tool_call",
            &[("tool_call_id", "t1"), ("name", "run")],
        ),
        block(
            4,
            Role::Assistant,
            "tool_call",
            &[("tool_call_id", "t1"), ("name", "run"), ("input", "{}")],
        ),
    ];
    let wire = blocks_to_wire_messages(&blocks, true);

    assert_eq!(wire.len(), 2, "user plus a single assistant message");
    let assistant = &wire[1];
    assert_eq!(assistant.role, "assistant");
    assert_eq!(assistant.reasoning_content.as_deref(), Some("let me think"));
    assert!(assistant.tool_calls.as_ref().is_some_and(|t| t.len() == 1));
}

/// The calendar entry is roleless on the row, projects as a system line, and
/// joins the system message so the model learns the date.
#[test]
fn date_marker_joins_the_system_message() {
    let marker = Block {
        id: 2,
        role: None,
        block_type: "date_marker".into(),
        created_at: String::new(),
        fields: {
            let mut m = serde_json::Map::new();
            m.insert("date".into(), Value::String("2026-07-12".into()));
            m
        },
    };
    let blocks = vec![
        block(1, Role::System, "system_prompt", &[("content", "be terse")]),
        marker,
        block(3, Role::User, "text", &[("content", "hi")]),
    ];
    let wire = blocks_to_wire_messages(&blocks, false);

    assert_eq!(wire[0].role, "system");
    assert_eq!(
        wire[0].content.as_deref(),
        Some("be terse\n\nCurrent date: 2026-07-12 (Sunday)"),
        "the projected dated line joins the system content"
    );
}

/// Finalized reasoning is authoritative and wins over any tail left in the same
/// turn — the tail is a partial copy of the same words.
#[test]
fn finalized_thinking_preferred_over_streaming() {
    let blocks = vec![
        block(
            1,
            Role::Assistant,
            "streaming_thinking",
            &[("content", "partial")],
        ),
        block(
            2,
            Role::Assistant,
            "thinking",
            &[("content", "final reasoning")],
        ),
        block(3, Role::Assistant, "text", &[("content", "answer")]),
    ];
    let wire = blocks_to_wire_messages(&blocks, true);

    assert_eq!(wire.len(), 1);
    assert_eq!(
        wire[0].reasoning_content.as_deref(),
        Some("final reasoning")
    );
    assert_eq!(wire[0].content.as_deref(), Some("answer"));
}

/// Reasoning on but none present: the endpoint requires the field, so an empty
/// string goes out rather than the field being omitted.
#[test]
fn empty_reasoning_when_thinking_enabled_and_none_present() {
    let blocks = vec![block(
        1,
        Role::Assistant,
        "tool_call",
        &[("tool_call_id", "t1"), ("name", "run"), ("input", "{}")],
    )];
    let wire = blocks_to_wire_messages(&blocks, true);

    assert_eq!(wire.len(), 1);
    assert_eq!(wire[0].reasoning_content.as_deref(), Some(""));
}

/// Reasoning off and none present: omit the field entirely.
#[test]
fn no_reasoning_field_when_thinking_disabled() {
    let blocks = vec![block(
        1,
        Role::Assistant,
        "text",
        &[("content", "plain answer")],
    )];
    let wire = blocks_to_wire_messages(&blocks, false);

    assert_eq!(wire.len(), 1);
    assert!(wire[0].reasoning_content.is_none());
}

/// A lone reasoning block must never become a standalone assistant message.
/// Reasoning is a rider, not a payload, and an assistant message with neither
/// content nor calls is rejected outright.
#[test]
fn lone_reasoning_is_dropped() {
    let streaming = vec![block(
        1,
        Role::Assistant,
        "streaming_thinking",
        &[("content", "orphan")],
    )];
    assert!(blocks_to_wire_messages(&streaming, false).is_empty());
    assert!(blocks_to_wire_messages(&streaming, true).is_empty());

    let finalized = vec![block(
        1,
        Role::Assistant,
        "thinking",
        &[("content", "orphan")],
    )];
    assert!(blocks_to_wire_messages(&finalized, true).is_empty());
}

/// The response side, decoded by the shared chat-completions decoder.
///
/// These are the shapes this vendor actually sends and its own parser dropped:
/// a chunk carrying two fields kept only the first, a tool-call chunk lost the
/// finish reason riding with it, the counts were a hardcoded default, a
/// transport that ended without the sentinel ended no turn, and no reasoning
/// block was ever closed.
mod stream_shapes {
    use serde_json::json;

    use crate::providers::chat::sse::{SseState, finish_stream, parse_sse_chunk};
    use crate::providers::types::{StopReason, StreamEvent};

    #[allow(clippy::needless_pass_by_value)]
    fn parse(chunk: serde_json::Value, state: &mut SseState) -> Vec<StreamEvent> {
        parse_sse_chunk(&chunk.to_string(), state)
            .into_iter()
            .map(|r| r.expect("the chunk parses cleanly"))
            .collect()
    }

    fn drain(state: &mut SseState) -> Vec<StreamEvent> {
        finish_stream(state)
            .into_iter()
            .map(|r| r.expect("the drain parses cleanly"))
            .collect()
    }

    /// Content and a tool call in ONE chunk: the text is delivered AND the call
    /// is taken up. Returning after the text dropped the call entirely, and the
    /// turn looked like an answer that simply never used its tools.
    #[test]
    fn content_and_tool_call_in_one_chunk_keep_both() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": {
                "content": "let me look",
                "tool_calls": [
                    { "index": 0, "id": "call_1", "function": { "name": "search", "arguments": "{}" } }
                ]
            } }] }),
            &mut state,
        );
        assert!(
            matches!(events.as_slice(), [StreamEvent::TextDelta { text }] if text == "let me look"),
            "the text is delivered live: {events:?}"
        );

        let end = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "tool_calls" }],
                    "usage": { "prompt_tokens": 3, "completion_tokens": 4 } }),
            &mut state,
        );
        assert!(
            matches!(
                end.as_slice(),
                [
                    StreamEvent::MessageEnd { stop_reason: StopReason::ToolUse, .. },
                    StreamEvent::ToolUseStart { id, name },
                    StreamEvent::ToolUseInputDelta { .. },
                    StreamEvent::ToolUseEnd,
                ] if id == "call_1" && name == "search"
            ),
            "the call in that same chunk still completes: {end:?}"
        );
    }

    /// Content and the finish reason in ONE chunk: the text is delivered and
    /// the turn ends. Returning after the text left the turn open forever.
    #[test]
    fn content_and_finish_reason_in_one_chunk_keep_both() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "content": "the answer" }, "finish_reason": "stop" }],
                    "usage": { "prompt_tokens": 11, "completion_tokens": 2 } }),
            &mut state,
        );
        assert!(
            matches!(
                events.as_slice(),
                [
                    StreamEvent::TextDelta { text },
                    StreamEvent::MessageEnd { stop_reason: StopReason::EndTurn, usage },
                ] if text == "the answer" && usage.input_tokens == 11
            ),
            "both fields of the chunk are read: {events:?}"
        );
    }

    /// A tool-call chunk carrying its own finish reason ends the turn too,
    /// rather than the call swallowing the end of the turn.
    #[test]
    fn tool_call_chunk_carrying_a_finish_reason_ends_the_turn() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{
                "delta": { "tool_calls": [
                    { "index": 0, "id": "call_1", "function": { "name": "run", "arguments": "{}" } }
                ] },
                "finish_reason": "tool_calls"
            }], "usage": { "prompt_tokens": 1, "completion_tokens": 1 } }),
            &mut state,
        );
        assert!(
            matches!(
                events.as_slice(),
                [
                    StreamEvent::MessageEnd {
                        stop_reason: StopReason::ToolUse,
                        ..
                    },
                    StreamEvent::ToolUseStart { .. },
                    StreamEvent::ToolUseInputDelta { .. },
                    StreamEvent::ToolUseEnd,
                ]
            ),
            "the finish reason riding with the call is not lost: {events:?}"
        );
    }

    /// The counts are the ones the vendor sent. A hardcoded default reported
    /// every turn on this vendor as free.
    #[test]
    fn the_counts_are_the_ones_the_vendor_sent() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            &mut state,
        );
        let events = parse(
            json!({ "choices": [], "usage": {
                "prompt_tokens": 1200,
                "completion_tokens": 340,
                "completion_tokens_details": { "reasoning_tokens": 90 }
            } }),
            &mut state,
        );
        let [StreamEvent::MessageEnd { usage, .. }] = events.as_slice() else {
            panic!("expected the end of turn, got {events:?}");
        };
        assert_eq!(usage.input_tokens, 1200);
        assert_eq!(usage.output_tokens, 340);
        assert_eq!(usage.reasoning_tokens, Some(90));
    }

    /// A transport that ends without the sentinel still ends the turn. Without
    /// it a completed answer stayed an uncommitted streaming block.
    #[test]
    fn transport_end_without_the_sentinel_still_ends_the_turn() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "content": "done thinking" } }] }),
            &mut state,
        );
        parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            &mut state,
        );

        let drained = drain(&mut state);
        assert!(
            matches!(
                drained.as_slice(),
                [StreamEvent::MessageEnd {
                    stop_reason: StopReason::EndTurn,
                    ..
                }]
            ),
            "the deferred end of turn is released on the drain: {drained:?}"
        );
    }

    /// Reasoning ends when it stops, at the boundary into content. Without that
    /// end the reasoning block dangles: never finalized, never persisted, and
    /// the model appears not to have reasoned at all.
    #[test]
    fn reasoning_ends_when_content_begins() {
        let mut state = SseState::default();
        let thinking = parse(
            json!({ "choices": [{ "delta": { "reasoning_content": "weighing it" } }] }),
            &mut state,
        );
        assert!(
            matches!(thinking.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == "weighing it")
        );

        let boundary = parse(
            json!({ "choices": [{ "delta": { "content": "the answer" } }] }),
            &mut state,
        );
        assert!(
            matches!(
                boundary.as_slice(),
                [
                    StreamEvent::ThinkingEnd { opaque: None },
                    StreamEvent::TextDelta { text },
                ] if text == "the answer"
            ),
            "exactly one end, before the first text: {boundary:?}"
        );
    }

    /// Reasoning still open when the stream stops is closed by the drain, so no
    /// block dangles even on a turn that ended mid-thought.
    #[test]
    fn reasoning_open_at_the_end_is_closed_by_the_drain() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "reasoning_content": "still weighing" } }] }),
            &mut state,
        );

        let drained = drain(&mut state);
        assert!(
            matches!(
                drained.first(),
                Some(StreamEvent::ThinkingEnd { opaque: None })
            ),
            "the open reasoning block is finalized: {drained:?}"
        );
    }
}

/// The rotation: what the SECOND refresh over one binding is handed.
mod token_rotation {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::super::{KimiStore, refresh_and_persist_with};
    use crate::store::{ProviderInstance, Store};

    /// A binding refreshes twice and the second refresh must use the token the
    /// first one rotated in — not the one captured when the binding was
    /// created, which the vendor has already retired.
    ///
    /// Replaying a spent refresh token does not fail the turn that replays it;
    /// it fails the next one, hours later, as a session that expired for no
    /// visible reason.
    #[tokio::test]
    async fn a_second_refresh_uses_the_rotated_token() {
        let store = Store::in_memory().unwrap();
        store
            .save_provider_instance(ProviderInstance {
                id: "p1".into(),
                provider_type: "kimi-for-coding".into(),
                name: "Kimi".into(),
            })
            .await
            .unwrap();
        let kimi = KimiStore::new(store.tx()).await.unwrap();

        // The configuration the binding captured, once, at bind time.
        let bind_config = json!({
            "base_url": null,
            "access_token": "access-0",
            "refresh_token": "refresh-0",
            "expires_at": 0,
        });

        let handed = Arc::new(Mutex::new(Vec::new()));
        for round in 1..=2 {
            let seen = handed.clone();
            let token = refresh_and_persist_with(
                &kimi,
                "p1",
                &bind_config,
                move |_access, refresh, _expires| {
                    seen.lock().unwrap().push(refresh);
                    async move {
                        Ok((
                            format!("access-{round}"),
                            Some(format!("refresh-{round}")),
                            Some(round * 1_000_000),
                        ))
                    }
                },
            )
            .await
            .unwrap();
            assert_eq!(token, format!("access-{round}"));
        }

        let handed = handed.lock().unwrap().clone();
        assert_eq!(
            handed,
            vec![Some("refresh-0".to_string()), Some("refresh-1".to_string())],
            "the second refresh is handed the rotated token, not the bound copy"
        );

        // And what is stored is the whole rotated pair, so the NEXT binding
        // starts from the live credentials rather than a dead access token
        // behind a fresh-looking expiry.
        let stored = kimi.get_config("p1".into()).await.unwrap().unwrap();
        assert_eq!(stored.refresh_token.as_deref(), Some("refresh-2"));
        assert_eq!(stored.access_token.as_deref(), Some("access-2"));
        assert_eq!(stored.expires_at, Some(2_000_000));
    }
}
