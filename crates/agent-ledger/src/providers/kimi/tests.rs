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
