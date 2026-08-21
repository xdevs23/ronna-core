//! The render pass's pins.
//!
//! The goldens pin the EXACT neutral output for each shape a group can take.
//! Byte-identity is the gate: drift here is a regression in what every vendor
//! is told, not a cleanup, and it would reach a model before it reached a
//! reader.

use serde_json::{Value, json};

use super::*;
use crate::agency::BlockKind;
use crate::block::Role;

fn block(id: i64, block_type: &str, fields: serde_json::Map<String, Value>) -> Block {
    Block {
        id,
        role: Some(Role::Assistant),
        block_type: block_type.into(),
        created_at: String::new(),
        fields,
    }
}

fn text_block(id: i64, content: &str) -> Block {
    let mut m = serde_json::Map::new();
    m.insert("content".into(), Value::String(content.into()));
    block(id, "text", m)
}

fn thinking_block(id: i64, content: &str) -> Block {
    let mut m = serde_json::Map::new();
    m.insert("content".into(), Value::String(content.into()));
    block(id, "thinking", m)
}

fn quote_block(id: i64, text: &str) -> Block {
    let mut m = serde_json::Map::new();
    m.insert("start_block_id".into(), Value::Number(10.into()));
    m.insert("start_pos".into(), Value::Number(0.into()));
    m.insert("end_block_id".into(), Value::Number(10.into()));
    m.insert("end_pos".into(), Value::Number(5.into()));
    m.insert("text".into(), Value::String(text.into()));
    block(id, "quote", m)
}

fn tool_call_block(id: i64, call_id: &str, name: &str, input: &str) -> Block {
    let mut m = serde_json::Map::new();
    m.insert("tool_call_id".into(), Value::String(call_id.into()));
    m.insert("name".into(), Value::String(name.into()));
    m.insert("input".into(), Value::String(input.into()));
    block(id, "tool_call", m)
}

fn role_block(id: i64, role: Option<Role>, block_type: &str, pairs: &[(&str, Value)]) -> Block {
    let mut m = serde_json::Map::new();
    for (key, value) in pairs {
        m.insert((*key).to_string(), value.clone());
    }
    Block {
        id,
        role,
        block_type: block_type.into(),
        created_at: String::new(),
        fields: m,
    }
}

fn neutral(blocks: &[Block]) -> Value {
    serde_json::to_value(blocks_to_messages::<BlockKind>(blocks)).unwrap()
}

// ─── The shared markdown vocabulary ──────────────────────────────────────────

#[test]
fn text_passthrough() {
    assert_eq!(render_text("hello world"), "hello world");
}

#[test]
fn quote_wraps_lines() {
    assert_eq!(render_quote("line 1\nline 2"), "> line 1\n> line 2");
}

#[test]
fn quote_empty() {
    assert_eq!(render_quote(""), "");
}

#[test]
fn code_fenced() {
    assert_eq!(
        render_code(Some("rust"), "fn main() {}"),
        "```rust\nfn main() {}\n```"
    );
}

#[test]
fn code_no_language() {
    assert_eq!(render_code(None, "some code"), "```\nsome code\n```");
}

// ─── Text-mode contributions ─────────────────────────────────────────────────

#[test]
fn blocks_to_text_excludes_thinking() {
    let blocks = vec![
        text_block(1, "hello"),
        thinking_block(2, "internal reasoning"),
        text_block(3, "world"),
    ];
    match render_blocks_to_text::<BlockKind>(&blocks) {
        MessageContent::Text(t) => assert_eq!(t, "hello\n\nworld"),
        MessageContent::Parts(_) => panic!("expected text content"),
    }
}

#[test]
fn blocks_to_text_with_quote() {
    let blocks = vec![quote_block(1, "hello"), text_block(2, "I agree")];
    match render_blocks_to_text::<BlockKind>(&blocks) {
        MessageContent::Text(t) => assert_eq!(t, "> hello\n\nI agree"),
        MessageContent::Parts(_) => panic!("expected text content"),
    }
}

/// The producer contract: inside a tool-bearing group a thinking block emits a
/// first-class reasoning part — never a stringified text part — while text and
/// tool blocks keep their existing parts. Reasoning that arrives as text and
/// leaves as text loses the continuity payload riding with it.
#[test]
fn render_group_emits_reasoning_for_thinking() {
    use crate::providers::types::ContentPart;
    let blocks = vec![
        thinking_block(1, "let me think"),
        text_block(2, "the answer"),
        tool_call_block(3, "call_1", "search", "{\"q\":\"x\"}"),
    ];
    let MessageContent::Parts(parts) = render_group::<BlockKind>(&blocks) else {
        panic!("expected parts (the group has tool blocks)");
    };
    assert_eq!(parts.len(), 3);
    match &parts[0] {
        ContentPart::Reasoning { text, opaque } => {
            assert_eq!(text, "let me think");
            assert!(opaque.is_none(), "no payload was stored on this block");
        }
        other => panic!("expected reasoning, got {other:?}"),
    }
    assert!(matches!(&parts[1], ContentPart::Text { text } if text == "the answer"));
    assert!(matches!(&parts[2], ContentPart::ToolUse { name, .. } if name == "search"));
}

/// The pinned asymmetry: a text-only group drops reasoning, because reasoning
/// has no faithful text form and folding it in would present the model's own
/// chain back to it as prose.
#[test]
fn text_only_group_still_drops_thinking() {
    let blocks = vec![thinking_block(1, "hidden"), text_block(2, "shown")];
    match render_group::<BlockKind>(&blocks) {
        MessageContent::Text(t) => assert_eq!(t, "shown"),
        MessageContent::Parts(_) => panic!("expected text content for a tool-less group"),
    }
}

// ─── Projection goldens ──────────────────────────────────────────────────────

#[test]
fn golden_plain_text_group() {
    let blocks = vec![
        role_block(1, Some(Role::User), "text", &[("content", json!("first"))]),
        role_block(2, Some(Role::User), "text", &[("content", json!("second"))]),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "user", "content": "first\n\nsecond" }])
    );
}

#[test]
fn golden_quote_and_text_group() {
    let blocks = vec![
        role_block(
            1,
            Some(Role::User),
            "quote",
            &[("text", json!("two\nlines"))],
        ),
        role_block(
            2,
            Some(Role::User),
            "text",
            &[("content", json!("I agree"))],
        ),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "user", "content": "> two\n> lines\n\nI agree" }])
    );
}

#[test]
fn golden_code_with_and_without_language() {
    let blocks = vec![
        role_block(
            1,
            Some(Role::User),
            "code",
            &[
                ("language", json!("rust")),
                ("content", json!("fn main() {}")),
            ],
        ),
        role_block(2, Some(Role::User), "code", &[("content", json!("plain"))]),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "user", "content": "```rust\nfn main() {}\n```\n\n```\nplain\n```" }])
    );
}

#[test]
fn golden_system_prompt_message() {
    let blocks = vec![role_block(
        1,
        Some(Role::System),
        "system_prompt",
        &[("content", json!("be terse"))],
    )];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "system", "content": "be terse" }])
    );
}

/// The flagship parts-mode shape: interleaved reasoning with its stored
/// continuity payload riding through untouched, several calls, and the
/// result-and-error pair grouping as one tool-to-user message.
#[test]
fn golden_tool_group_with_interleaved_thinking() {
    let opaque = serde_json::to_value(crate::block::OpaquePayload::Anthropic {
        signature: "sig-1".into(),
    })
    .unwrap();
    let blocks = vec![
        role_block(1, Some(Role::User), "text", &[("content", json!("do it"))]),
        role_block(
            2,
            Some(Role::Assistant),
            "thinking",
            &[("content", json!("plan")), ("opaque", opaque)],
        ),
        role_block(
            3,
            Some(Role::Assistant),
            "text",
            &[("content", json!("on it"))],
        ),
        role_block(
            4,
            Some(Role::Assistant),
            "tool_call",
            &[
                ("tool_call_id", json!("c1")),
                ("name", json!("search")),
                ("input", json!("{\"q\":\"x\"}")),
            ],
        ),
        role_block(
            5,
            Some(Role::Assistant),
            "tool_call",
            &[
                ("tool_call_id", json!("c2")),
                ("name", json!("fetch")),
                ("input", json!("{\"u\":\"y\"}")),
            ],
        ),
        role_block(
            6,
            None,
            "tool_result",
            &[("tool_call_id", json!("c1")), ("content", json!("found"))],
        ),
        role_block(
            7,
            None,
            "tool_error",
            &[("tool_call_id", json!("c2")), ("error", json!("boom"))],
        ),
        role_block(
            8,
            Some(Role::Assistant),
            "text",
            &[("content", json!("done"))],
        ),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([
            { "role": "user", "content": "do it" },
            { "role": "assistant", "content": [
                { "type": "reasoning", "text": "plan",
                  "opaque": { "anthropic": { "signature": "sig-1" } } },
                { "type": "text", "text": "on it" },
                { "type": "tool_use", "id": "c1", "name": "search", "input": { "q": "x" } },
                { "type": "tool_use", "id": "c2", "name": "fetch", "input": { "u": "y" } },
            ] },
            { "role": "user", "content": [
                { "type": "tool_result", "tool_use_id": "c1", "content": "found" },
                { "type": "tool_result", "tool_use_id": "c2", "content": "boom" },
            ] },
            { "role": "assistant", "content": "done" },
        ])
    );
}

#[test]
fn golden_text_only_group_drops_thinking() {
    let blocks = vec![
        role_block(
            1,
            Some(Role::Assistant),
            "thinking",
            &[("content", json!("hidden"))],
        ),
        role_block(
            2,
            Some(Role::Assistant),
            "text",
            &[("content", json!("shown"))],
        ),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "assistant", "content": "shown" }])
    );
}

/// The empty-assistant-message hazard: a streaming tool call whose committed
/// counterpart has not landed yet must not leak a message boundary. A provider
/// rejects an assistant message with neither content nor calls outright.
#[test]
fn golden_streaming_excluded_without_boundary_leak() {
    let blocks = vec![
        role_block(1, Some(Role::User), "text", &[("content", json!("hi"))]),
        role_block(2, Some(Role::Assistant), "streaming_tool_call", &[]),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "user", "content": "hi" }])
    );

    let blocks = vec![
        role_block(
            1,
            Some(Role::Assistant),
            "streaming_thinking",
            &[("content", json!("partial"))],
        ),
        role_block(
            2,
            Some(Role::Assistant),
            "text",
            &[("content", json!("final"))],
        ),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "assistant", "content": "final" }])
    );
}

/// The approval blocks are invisible in BOTH modes and contribute no boundary:
/// the neutral output is identical with and without them, in the realistic
/// call-request-decision-result shape (parts mode) and as a group tail (text
/// mode, where a boundary would leave a trailing empty user message).
#[test]
fn golden_approval_blocks_invisible_in_both_modes() {
    let ledger = vec![
        role_block(1, Some(Role::User), "text", &[("content", json!("run it"))]),
        role_block(
            2,
            Some(Role::Assistant),
            "tool_call",
            &[
                ("tool_call_id", json!("c9")),
                ("name", json!("deploy")),
                ("input", json!("{}")),
            ],
        ),
        role_block(
            3,
            Some(Role::User),
            "approval_request",
            &[("for_block_id", json!(2))],
        ),
        role_block(
            4,
            Some(Role::User),
            "approval_decision",
            &[("for_block_id", json!(3)), ("decision", json!("approved"))],
        ),
        role_block(
            5,
            None,
            "tool_result",
            &[
                ("tool_call_id", json!("c9")),
                ("content", json!("deployed")),
            ],
        ),
    ];
    let stripped: Vec<Block> = ledger
        .iter()
        .filter(|b| !b.block_type.starts_with("approval_"))
        .cloned()
        .collect();
    let expected = json!([
        { "role": "user", "content": "run it" },
        { "role": "assistant", "content": [
            { "type": "tool_use", "id": "c9", "name": "deploy", "input": {} },
        ] },
        { "role": "user", "content": [
            { "type": "tool_result", "tool_use_id": "c9", "content": "deployed" },
        ] },
    ]);
    assert_eq!(neutral(&ledger), expected);
    assert_eq!(neutral(&stripped), expected);

    let text_mode = vec![
        role_block(1, Some(Role::User), "text", &[("content", json!("a"))]),
        role_block(
            2,
            Some(Role::User),
            "approval_request",
            &[("for_block_id", json!(1))],
        ),
        role_block(
            3,
            Some(Role::User),
            "approval_decision",
            &[("for_block_id", json!(2)), ("decision", json!("denied"))],
        ),
    ];
    assert_eq!(
        neutral(&text_mode),
        json!([{ "role": "user", "content": "a" }])
    );
}

/// A block type this build does not know contributes no content in either mode
/// but still groups under its stored role — including the lone-unknown empty
/// message, pinned as-is because byte-identity is the gate.
#[test]
fn golden_unknown_type_skipped_but_groups_under_role() {
    let blocks = vec![
        role_block(
            1,
            Some(Role::Assistant),
            "text",
            &[("content", json!("known"))],
        ),
        role_block(2, Some(Role::Assistant), "holographic_block", &[]),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "assistant", "content": "known" }])
    );

    let lone = vec![role_block(1, Some(Role::User), "holographic_block", &[])];
    assert_eq!(neutral(&lone), json!([{ "role": "user", "content": "" }]));
}

/// Missing or malformed fields degrade to empty or verbatim values — never a
/// panic. A ledger row is data, and a reader that panics on data cannot replay.
#[test]
fn golden_malformed_fields_degrade() {
    let bare_call = vec![role_block(1, Some(Role::Assistant), "tool_call", &[])];
    assert_eq!(
        neutral(&bare_call),
        json!([{ "role": "assistant", "content": [
            { "type": "tool_use", "id": "", "name": "", "input": "" },
        ] }])
    );

    let blocks = vec![
        role_block(
            1,
            Some(Role::Assistant),
            "thinking",
            &[("content", json!("t")), ("opaque", json!(42))],
        ),
        role_block(2, Some(Role::Assistant), "quote", &[]),
        role_block(
            3,
            Some(Role::Assistant),
            "tool_call",
            &[
                ("tool_call_id", json!("c1")),
                ("name", json!("n")),
                ("input", json!("not json{")),
            ],
        ),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "assistant", "content": [
            { "type": "reasoning", "text": "t", "opaque": null },
            { "type": "text", "text": "" },
            { "type": "tool_use", "id": "c1", "name": "n", "input": "not json{" },
        ] }])
    );
}

/// The date marker's golden: the EXACT dated system line, grouped as system —
/// and adjacent to a system prompt it joins that message instead of opening a
/// second one.
#[test]
fn golden_date_marker_renders_the_dated_system_line() {
    let blocks = vec![
        role_block(1, None, "date_marker", &[("date", json!("2026-07-12"))]),
        role_block(2, Some(Role::User), "text", &[("content", json!("hi"))]),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([
            { "role": "system", "content": "Current date: 2026-07-12 (Sunday)" },
            { "role": "user", "content": "hi" },
        ])
    );

    let with_prompt = vec![
        role_block(
            1,
            Some(Role::System),
            "system_prompt",
            &[("content", json!("be terse"))],
        ),
        role_block(2, None, "date_marker", &[("date", json!("2026-07-12"))]),
        role_block(3, Some(Role::User), "text", &[("content", json!("hi"))]),
    ];
    assert_eq!(
        neutral(&with_prompt),
        json!([
            { "role": "system", "content": "be terse\n\nCurrent date: 2026-07-12 (Sunday)" },
            { "role": "user", "content": "hi" },
        ])
    );

    // An unparseable stored date degrades to the bare line — never a panic.
    let malformed = vec![role_block(
        1,
        None,
        "date_marker",
        &[("date", json!("not-a-date"))],
    )];
    assert_eq!(
        neutral(&malformed),
        json!([{ "role": "system", "content": "Current date: not-a-date" }])
    );
}

/// Role-carrying records sit inside a same-role run without splitting it and
/// without contributing content. A record that split a run would strand the
/// text on either side of it in two messages.
#[test]
fn golden_records_do_not_split_a_same_role_run() {
    let blocks = vec![
        role_block(1, Some(Role::Assistant), "text", &[("content", json!("a"))]),
        role_block(
            2,
            Some(Role::Assistant),
            "status",
            &[("status", json!("interrupted"))],
        ),
        role_block(3, Some(Role::Assistant), "holographic_block", &[]),
        role_block(4, Some(Role::Assistant), "text", &[("content", json!("b"))]),
    ];
    assert_eq!(
        neutral(&blocks),
        json!([{ "role": "assistant", "content": "a\n\nb" }])
    );
}

/// The transcript export rides the same per-kind text contributions: the
/// system prompt is skipped, reasoning is dropped, labelled sections join with
/// a rule.
#[test]
fn golden_transcript_of_a_mixed_conversation() {
    let blocks = vec![
        role_block(
            1,
            Some(Role::System),
            "system_prompt",
            &[("content", json!("sys"))],
        ),
        role_block(2, Some(Role::User), "text", &[("content", json!("hello"))]),
        role_block(3, Some(Role::User), "quote", &[("text", json!("q1\nq2"))]),
        role_block(
            4,
            Some(Role::Assistant),
            "thinking",
            &[("content", json!("think"))],
        ),
        role_block(
            5,
            Some(Role::Assistant),
            "text",
            &[("content", json!("answer"))],
        ),
        role_block(
            6,
            Some(Role::Assistant),
            "code",
            &[("language", json!("rust")), ("content", json!("x()"))],
        ),
        role_block(
            7,
            Some(Role::Tool),
            "tool_result",
            &[("tool_call_id", json!("c1")), ("content", json!("out"))],
        ),
    ];
    assert_eq!(
        render_conversation::<BlockKind>(&blocks),
        "**You:**\nhello\n\n> q1\n> q2\n\n---\n\n**Assistant:**\nanswer\n\n```rust\nx()\n```\n\n---\n\n**Tool:**\nout"
    );
}

// ─── The layers, composed ────────────────────────────────────────────────────

/// Blocks written through the store come back out as one neutral message list,
/// byte-stable across two runs.
///
/// This is the composition check the three layers below owe each other: the
/// store writes and reads the rows, the behavior layer resolves each row to its
/// kind and states what it contributes, and this pass groups the result. A
/// golden built from hand-made `Block` values proves the last of those three in
/// isolation; only a store-built ledger proves that what persistence hands back
/// is what projection expects to receive.
///
/// Byte-stability across two runs is the part worth asserting: a map that
/// iterated in an unstable order, or a payload field that serialized in a
/// different order each time, would produce a different prompt for an unchanged
/// conversation — and a model's answer would change for no reason anyone could
/// name.
#[tokio::test]
async fn composes_over_a_store_built_ledger_byte_stably() {
    use crate::store::{Store, ToolCallInsert};

    let store = Store::in_memory().unwrap();
    let conversation = store
        .create_conversation("p".into(), "m".into(), "M".into(), "A Vendor".into())
        .await
        .unwrap();

    store
        .insert_system_prompt(conversation, "be terse".into())
        .await
        .unwrap();
    store
        .insert_text_block(conversation, Role::User, "what is x?".into())
        .await
        .unwrap();
    store
        .insert_thinking_block_with_content(
            conversation,
            Role::Assistant,
            "weighing it".into(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    store
        .insert_text_block(conversation, Role::Assistant, "looking".into())
        .await
        .unwrap();
    let call = store
        .insert_tool_call_block(
            conversation,
            Role::Assistant,
            ToolCallInsert {
                tool_call_id: "call_1".into(),
                name: "search".into(),
                input: "{\"q\":\"x\"}".into(),
                interactive: false,
            },
            None,
        )
        .await
        .unwrap();
    store
        .complete_tool_call_block(conversation, "call_1".into(), "found it".into(), call)
        .await
        .unwrap()
        .expect("the call is unresolved");
    store
        .insert_text_block(conversation, Role::Assistant, "x is x".into())
        .await
        .unwrap();

    // Two loads and two renders, compared as the BYTES each render writes.
    // Serializing a `Value` instead would compare a normalized round trip: its
    // maps are sorted on the way in, so a field order that changed between runs
    // — one of the two drifts this assertion exists to catch — would come out
    // identical either way and the check could not fail.
    let render_bytes =
        |blocks: &[Block]| serde_json::to_string(&blocks_to_messages::<BlockKind>(blocks)).unwrap();
    let first_bytes = render_bytes(&store.list_blocks(conversation).await.unwrap());
    let second_bytes = render_bytes(&store.list_blocks(conversation).await.unwrap());
    assert_eq!(
        first_bytes, second_bytes,
        "the same ledger renders to the same bytes"
    );

    assert_eq!(
        serde_json::from_str::<Value>(&first_bytes).unwrap(),
        json!([
            { "role": "system", "content": "be terse" },
            { "role": "user", "content": "what is x?" },
            { "role": "assistant", "content": [
                { "type": "reasoning", "text": "weighing it", "opaque": null },
                { "type": "text", "text": "looking" },
                { "type": "tool_use", "id": "call_1", "name": "search", "input": { "q": "x" } },
            ] },
            { "role": "user", "content": [
                { "type": "tool_result", "tool_use_id": "call_1", "content": "found it" },
            ] },
            { "role": "assistant", "content": "x is x" },
        ]),
        "blocks in, one neutral message list out"
    );
}
