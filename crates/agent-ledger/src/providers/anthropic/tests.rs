//! What this vendor's wire looks like, pinned both ways: events in, and
//! requests out.

use serde_json::json;

use super::wire::{AnthropicSseState, parse_sse_event};
use super::*;
use crate::block::{Block, Role};
use crate::providers::render::blocks_to_messages;
use crate::providers::types::{StopReason, StreamEvent};

/// The reasoning ingest: a block start, its deltas, its signature fragments,
/// and the stop that carries the signature out as the continuity payload.
mod thinking_ingest_fixtures {
    use super::*;

    // The fixtures read better built inline at each call site, so these helpers
    // take the value rather than a reference to one.
    #[allow(clippy::needless_pass_by_value)]
    fn parse(state: &mut AnthropicSseState, event: Value) -> Vec<StreamEvent> {
        parse_sse_event(&event.to_string(), state)
            .into_iter()
            .map(|r| r.expect("the event parses cleanly"))
            .collect()
    }

    fn start_thinking() -> Value {
        json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "thinking", "thinking": "" } })
    }

    fn thinking_delta(text: &str) -> Value {
        json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "thinking_delta", "thinking": text } })
    }

    fn signature_delta(sig: &str) -> Value {
        json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "signature_delta", "signature": sig } })
    }

    fn block_stop() -> Value {
        json!({ "type": "content_block_stop", "index": 0 })
    }

    /// Reasoning deltas stream live — no start event, because the block is
    /// created lazily — signature fragments accumulate silently, and the stop
    /// emits one end carrying the assembled signature.
    #[test]
    fn thinking_stream_accumulates_and_ends_with_signature() {
        let mut state = AnthropicSseState::default();
        assert!(parse(&mut state, start_thinking()).is_empty());
        let d1 = parse(&mut state, thinking_delta("weighing "));
        assert!(
            matches!(d1.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == "weighing ")
        );
        let d2 = parse(&mut state, thinking_delta("options"));
        assert!(
            matches!(d2.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == "options")
        );
        assert!(parse(&mut state, signature_delta("sig-part-1/")).is_empty());
        assert!(parse(&mut state, signature_delta("sig-part-2")).is_empty());

        let end = parse(&mut state, block_stop());
        let [
            StreamEvent::ThinkingEnd {
                opaque: Some(OpaquePayload::Anthropic { signature }),
            },
        ] = end.as_slice()
        else {
            panic!("expected a signature-carrying end, got {end:?}");
        };
        assert_eq!(
            signature, "sig-part-1/sig-part-2",
            "fragments accumulate in order"
        );
    }

    /// A thinking block that never received a signature still finalizes, with
    /// no payload. No signature means no replay is possible, and a fabricated
    /// one would be rejected on the next turn.
    #[test]
    fn thinking_without_signature_ends_with_no_payload() {
        let mut state = AnthropicSseState::default();
        parse(&mut state, start_thinking());
        parse(&mut state, thinking_delta("unsigned thought"));
        let end = parse(&mut state, block_stop());
        assert!(matches!(
            end.as_slice(),
            [StreamEvent::ThinkingEnd { opaque: None }]
        ));
    }

    /// A stream with no reasoning behaves exactly as it did before the
    /// reasoning ingest existed: text streams, a tool lifecycle buffers, and
    /// the turn's end releases the buffered tool events after it.
    #[test]
    fn stream_without_thinking_is_unchanged() {
        let mut state = AnthropicSseState::default();
        let text = parse(
            &mut state,
            json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": "hello" } }),
        );
        assert!(matches!(text.as_slice(), [StreamEvent::TextDelta { text }] if text == "hello"));

        parse(
            &mut state,
            json!({ "type": "content_block_start", "index": 1, "content_block": { "type": "tool_use", "id": "call_1", "name": "search" } }),
        );
        parse(
            &mut state,
            json!({ "type": "content_block_delta", "index": 1, "delta": { "type": "input_json_delta", "partial_json": "{}" } }),
        );
        assert!(
            parse(
                &mut state,
                json!({ "type": "content_block_stop", "index": 1 })
            )
            .is_empty()
        );

        let end = parse(
            &mut state,
            json!({ "type": "message_delta", "delta": { "stop_reason": "tool_use" }, "usage": { "output_tokens": 7 } }),
        );
        assert!(matches!(
            end.as_slice(),
            [
                StreamEvent::MessageEnd {
                    stop_reason: StopReason::ToolUse,
                    ..
                },
                StreamEvent::ToolUseStart { .. },
                StreamEvent::ToolUseInputDelta { .. },
                StreamEvent::ToolUseEnd,
            ]
        ));
    }

    /// A text-only answer ends with no tool event at all. The stop event names
    /// no block kind, so the kind is remembered from the start; ending a tool
    /// call here would hand the reader a close for a call that never opened.
    #[test]
    fn text_block_stop_emits_no_tool_end() {
        let mut state = AnthropicSseState::default();
        parse(
            &mut state,
            json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "text", "text": "" } }),
        );
        parse(
            &mut state,
            json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": "hello" } }),
        );
        assert!(
            parse(
                &mut state,
                json!({ "type": "content_block_stop", "index": 0 })
            )
            .is_empty()
        );

        let end = parse(
            &mut state,
            json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": { "output_tokens": 3 } }),
        );
        assert!(
            matches!(
                end.as_slice(),
                [StreamEvent::MessageEnd {
                    stop_reason: StopReason::EndTurn,
                    ..
                }]
            ),
            "the turn ends with nothing trailing it, got {end:?}"
        );
    }

    /// The input count comes from the start of the message, which is the only
    /// event that carries it. A hardcoded zero would be a fabricated
    /// measurement — a claim that the request cost nothing.
    #[test]
    fn input_tokens_come_from_the_message_start() {
        let mut state = AnthropicSseState::default();
        parse(
            &mut state,
            json!({
                "type": "message_start",
                "message": { "usage": { "input_tokens": 1234, "output_tokens": 1 } }
            }),
        );
        let end = parse(
            &mut state,
            json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": { "output_tokens": 57 } }),
        );
        let [StreamEvent::MessageEnd { usage, .. }] = end.as_slice() else {
            panic!("expected one end of turn, got {end:?}");
        };
        assert_eq!(usage.input_tokens, 1234, "the counted request cost");
        assert_eq!(usage.output_tokens, 57);
        assert_eq!(usage.reasoning_tokens, None, "this vendor states none");
    }

    /// A reasoning turn followed by a tool call: the reasoning end fires live at
    /// the thinking block's own stop, never buffered past the end of the turn,
    /// while the tool lifecycle stays buffered.
    #[test]
    fn thinking_end_fires_live_before_buffered_tool_events() {
        let mut state = AnthropicSseState::default();
        parse(&mut state, start_thinking());
        parse(&mut state, thinking_delta("plan"));
        parse(&mut state, signature_delta("sig"));
        let end = parse(&mut state, block_stop());
        assert!(matches!(
            end.as_slice(),
            [StreamEvent::ThinkingEnd { opaque: Some(_) }]
        ));

        parse(
            &mut state,
            json!({ "type": "content_block_start", "index": 1, "content_block": { "type": "tool_use", "id": "call_1", "name": "search" } }),
        );
        // The tool block's stop buffers its end — the thinking state is already
        // closed, so this cannot emit a second reasoning end.
        let stop = parse(
            &mut state,
            json!({ "type": "content_block_stop", "index": 1 }),
        );
        assert!(stop.is_empty());
    }
}

/// The replay side: a stored payload of THIS vendor's variant renders as the
/// native thinking block, and anything else is omitted.
mod replay_tests {
    use super::*;
    use crate::providers::types::ReasoningDetailEntry;

    fn blocks_with_payload(payload: &OpaquePayload) -> Vec<Block> {
        let mut thinking = serde_json::Map::new();
        thinking.insert("content".into(), Value::String("let me think".into()));
        thinking.insert("opaque".into(), serde_json::to_value(payload).unwrap());
        let mut text = serde_json::Map::new();
        text.insert("content".into(), Value::String("the answer".into()));
        let mut tool_call = serde_json::Map::new();
        for (k, v) in [
            ("tool_call_id", "call_1"),
            ("name", "search"),
            ("input", "{}"),
        ] {
            tool_call.insert(k.into(), Value::String(v.into()));
        }
        vec![
            Block {
                id: 1,
                role: Some(Role::Assistant),
                block_type: "thinking".into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: thinking,
            },
            Block {
                id: 2,
                role: Some(Role::Assistant),
                block_type: "text".into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: text,
            },
            Block {
                id: 3,
                role: Some(Role::Assistant),
                block_type: "tool_call".into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: tool_call,
            },
        ]
    }

    fn wire_for(payload: &OpaquePayload, include: bool) -> (Value, bool) {
        let (_, wire, carried) = convert_messages(
            &blocks_to_messages::<crate::agency::BlockKind>(&blocks_with_payload(payload)),
            include,
        );
        (
            serde_json::to_value(&wire).expect("the wire serializes"),
            carried,
        )
    }

    /// The golden: the native thinking block, signature echoed verbatim.
    #[test]
    fn own_payload_renders_native_thinking_block() {
        let (actual, carried) = wire_for(
            &OpaquePayload::Anthropic {
                signature: "sig-xyz".into(),
            },
            true,
        );
        assert!(carried, "the build reports the replayed payload");
        assert_eq!(
            actual[0]["content"][0],
            json!({ "type": "thinking", "thinking": "let me think", "signature": "sig-xyz" })
        );
        assert_eq!(actual[0]["content"][1]["type"], "text");
    }

    /// The variant gate: a foreign payload is OMITTED — the reasoning renders
    /// as the plain text block it always was, and the foreign blob never
    /// reaches the wire in any shape.
    #[test]
    fn foreign_variant_payload_is_omitted() {
        let payload = OpaquePayload::OpenAiResponses {
            item_id: "rs_1".into(),
            encrypted_content: "FOREIGN-BLOB".into(),
        };
        let (actual, carried) = wire_for(&payload, true);
        assert!(!carried);
        assert_eq!(
            actual[0]["content"][0],
            json!({ "type": "text", "text": "let me think" })
        );
        assert!(!actual.to_string().contains("FOREIGN-BLOB"));

        // The same holds for the other foreign variants, so a vendor added
        // later cannot inherit this one's replay by accident.
        let entries = OpaquePayload::OpenRouter {
            entries: vec![ReasoningDetailEntry {
                position: 0,
                entry_type: "reasoning.text".into(),
                entry_id: None,
                upstream_format: "somewhere-else-v1".into(),
                index: None,
                content: "foreign".into(),
                signature: None,
            }],
        };
        let (actual, carried) = wire_for(&entries, true);
        assert!(!carried);
        assert_eq!(actual[0]["content"][0]["type"], "text");
    }

    /// The knob: payloads suppressed produces the payload-free wire, and the
    /// build reports none — so no fallback loop is possible.
    #[test]
    fn knob_off_renders_plain_text_and_reports_none() {
        let (actual, carried) = wire_for(
            &OpaquePayload::Anthropic {
                signature: "sig".into(),
            },
            false,
        );
        assert!(!carried);
        assert_eq!(
            actual[0]["content"][0],
            json!({ "type": "text", "text": "let me think" })
        );
    }
}

/// The BIND path's golden: the exact bytes a bound turn opens with.
///
/// The other goldens pin the translation from neutral messages onward, which
/// left the request the bind path itself assembles unpinned — and that is where
/// a ceiling of `u32::MAX` rode out on every turn, rejected by the API with a
/// 400 for exceeding the model's own limit.
mod bind_request_golden {
    use super::*;

    fn user_block(id: i64, role: Role, block_type: &str, content: &str) -> Block {
        let mut fields = serde_json::Map::new();
        fields.insert("content".into(), Value::String(content.into()));
        Block {
            id,
            role: Some(role),
            block_type: block_type.into(),
            created_at: String::new(),
            dispatch_anchor: None,
            fields,
        }
    }

    #[test]
    fn a_bound_turn_sends_the_builder_default_ceiling() {
        let blocks = vec![
            user_block(1, Role::System, "system_prompt", "be terse"),
            user_block(2, Role::User, "text", "what is x?"),
        ];
        let tools = vec![ToolDefinition {
            name: "search".into(),
            description: "Search the web".into(),
            parameters: json!({ "type": "object" }),
        }];

        let request = turn_request(
            ModelSelector::Lightweight,
            blocks_to_messages::<crate::agency::BlockKind>(&blocks),
            tools,
            Some(ReasoningLevel::High),
        );
        assert_eq!(
            request.max_tokens, None,
            "the bind path names no ceiling of its own"
        );

        let (body, carried) = AnthropicProvider::build_request_body(&request, true, true);
        assert!(!carried, "no stored payload means nothing replayed");
        assert_eq!(
            serde_json::to_value(&body).expect("the wire serializes"),
            json!({
                "model": LIGHTWEIGHT_MODEL,
                "max_tokens": 32768,
                "system": "be terse",
                "messages": [
                    { "role": "user", "content": [{ "type": "text", "text": "what is x?" }] }
                ],
                "tools": [
                    { "name": "search", "description": "Search the web", "input_schema": { "type": "object" } }
                ],
                "stream": true,
                "thinking": { "type": "adaptive" },
                "effort": "high"
            })
        );
    }
}

/// The neutral-to-wire golden: a reasoning-bearing ledger must serialize to
/// exactly these bytes. The literal is the whole point — it is what proves the
/// central render pass and this vendor's translation still agree.
mod wire_golden {
    use super::*;

    fn block(id: i64, role: Option<Role>, ty: &str, fields: &[(&str, &str)]) -> Block {
        let mut m = serde_json::Map::new();
        for (k, v) in fields {
            m.insert((*k).into(), Value::String((*v).into()));
        }
        Block {
            id,
            role,
            block_type: ty.into(),
            created_at: String::new(),
            dispatch_anchor: None,
            fields: m,
        }
    }

    #[test]
    fn thinking_bearing_blocks_produce_the_expected_wire() {
        let blocks = vec![
            block(
                1,
                Some(Role::Assistant),
                "thinking",
                &[("content", "let me think")],
            ),
            block(
                2,
                Some(Role::Assistant),
                "text",
                &[("content", "the answer")],
            ),
            block(
                3,
                Some(Role::Assistant),
                "tool_call",
                &[
                    ("tool_call_id", "call_1"),
                    ("name", "search"),
                    ("input", "{\"q\":\"x\"}"),
                ],
            ),
            block(
                4,
                Some(Role::Assistant),
                "tool_result",
                &[("tool_call_id", "call_1"), ("content", "result data")],
            ),
        ];

        let (system, wire, carried) = convert_messages(
            &blocks_to_messages::<crate::agency::BlockKind>(&blocks),
            true,
        );
        assert!(system.is_none());
        assert!(!carried, "no stored payload means nothing replayed");

        let actual = serde_json::to_value(&wire).expect("the wire serializes");
        let expected = json!([
            {
                "role": "assistant",
                "content": [
                    { "type": "text", "text": "let me think" },
                    { "type": "text", "text": "the answer" },
                    { "type": "tool_use", "id": "call_1", "name": "search", "input": { "q": "x" } }
                ]
            },
            {
                "role": "user",
                "content": [
                    { "type": "tool_result", "tool_use_id": "call_1", "content": "result data" }
                ]
            }
        ]);
        assert_eq!(actual, expected);
    }

    /// Every system group folds into the one system parameter, JOINED. A
    /// mid-conversation date marker is its own system message, and overwriting
    /// would erase the system prompt the conversation opened with — silently,
    /// and only in conversations that ran across midnight.
    #[test]
    fn later_system_group_joins_the_system_param_instead_of_erasing_it() {
        let blocks = vec![
            block(
                1,
                Some(Role::System),
                "system_prompt",
                &[("content", "be terse")],
            ),
            block(2, None, "date_marker", &[("date", "2026-07-11")]),
            block(3, Some(Role::User), "text", &[("content", "hi")]),
            block(4, Some(Role::Assistant), "text", &[("content", "hello")]),
            block(5, None, "date_marker", &[("date", "2026-07-12")]),
            block(6, Some(Role::User), "text", &[("content", "next day")]),
        ];

        let (system, wire, _) = convert_messages(
            &blocks_to_messages::<crate::agency::BlockKind>(&blocks),
            true,
        );
        assert_eq!(
            system.as_deref(),
            Some(
                "be terse\n\nCurrent date: 2026-07-11 (Saturday)\n\nCurrent date: 2026-07-12 (Sunday)"
            ),
            "the prompt survives and both dated lines ride along"
        );
        assert_eq!(
            wire.len(),
            3,
            "user, assistant, user — no system leakage into the messages"
        );
    }
}
