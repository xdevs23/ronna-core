//! The gateway's pins: the pinned background slug, the capability reading, and
//! the reasoning echo it rebuilds on request messages.

use serde_json::json;

use super::*;
use crate::block::{Block, Role};
use crate::providers::render::blocks_to_messages;
use crate::providers::types::{OpaquePayload, ReasoningDetailEntry};

/// The background slug is pinned so a change to it is a decision someone made,
/// not a detail that drifted. Re-verify any replacement against the gateway's
/// public model list before landing it: a delisted slug fails every background
/// request, deterministically, and looks like an outage.
#[test]
fn lightweight_slug_is_pinned() {
    assert_eq!(OPENROUTER_LIGHTWEIGHT_MODEL, "google/gemini-3.1-flash-lite");
}

mod capability {
    use super::*;

    fn model(reasoning: Value) -> WireModel {
        let mut obj = serde_json::Map::new();
        obj.insert("id".into(), Value::String("a-vendor/a-model".into()));
        if !reasoning.is_null() {
            obj.insert("reasoning".into(), reasoning);
        }
        serde_json::from_value(Value::Object(obj)).expect("the model entry deserializes")
    }

    /// A populated effort list offers exactly those levels, mapped through the
    /// canonical key vocabulary.
    #[test]
    fn supported_efforts_present_maps_each_level() {
        let cap = openrouter_reasoning(&model(json!({
            "supported_efforts": ["low", "medium", "high", "xhigh"]
        })));
        assert_eq!(
            cap.levels,
            vec![
                ReasoningLevel::Low,
                ReasoningLevel::Medium,
                ReasoningLevel::High,
                ReasoningLevel::XHigh
            ]
        );
    }

    /// A descriptor with no effort list falls back to the portable three.
    #[test]
    fn supported_efforts_null_falls_back_to_the_portable_three() {
        let cap = openrouter_reasoning(&model(json!({ "mandatory": true })));
        assert_eq!(
            cap.levels,
            vec![
                ReasoningLevel::Low,
                ReasoningLevel::Medium,
                ReasoningLevel::High
            ]
        );
    }

    /// No descriptor at all means no effort selection, so no control is
    /// offered. Offering one that the model ignores is a lie the user cannot
    /// detect.
    #[test]
    fn reasoning_omitted_yields_empty_capability() {
        let cap = openrouter_reasoning(&model(Value::Null));
        assert!(cap.is_empty());
    }
}

/// The replay: stored entries rebuild onto the assistant request message in
/// position order, gated on the variant, the hazard filter, and the knob.
mod replay {
    use super::*;

    fn entry(position: u32, entry_type: &str, format: &str, content: &str) -> ReasoningDetailEntry {
        ReasoningDetailEntry {
            position,
            entry_type: entry_type.into(),
            entry_id: Some(format!("rd_{position}")),
            upstream_format: format.into(),
            index: Some(position),
            content: content.into(),
            signature: None,
        }
    }

    /// A tool-bearing turn whose reasoning block carries a stored payload,
    /// mirroring exactly what the store's read-back places on the block.
    fn blocks_with_payload(payload: &OpaquePayload) -> Vec<Block> {
        let mut thinking = serde_json::Map::new();
        thinking.insert("content".into(), Value::String("let me think".into()));
        thinking.insert("opaque".into(), serde_json::to_value(payload).unwrap());
        let mut tool_call = serde_json::Map::new();
        for (k, v) in [
            ("tool_call_id", "call_1"),
            ("name", "search"),
            ("input", "{}"),
        ] {
            tool_call.insert(k.into(), Value::String(v.into()));
        }
        let mut text = serde_json::Map::new();
        text.insert("content".into(), Value::String("the answer".into()));
        vec![
            Block {
                id: 1,
                role: Some(Role::Assistant),
                block_type: "thinking".into(),
                created_at: String::new(),
                fields: thinking,
            },
            Block {
                id: 2,
                role: Some(Role::Assistant),
                block_type: "text".into(),
                created_at: String::new(),
                fields: text,
            },
            Block {
                id: 3,
                role: Some(Role::Assistant),
                block_type: "tool_call".into(),
                created_at: String::new(),
                fields: tool_call,
            },
        ]
    }

    fn assistant_message(payload: &OpaquePayload, include: bool) -> (Value, bool) {
        let provider = openrouter_provider("test-key".into(), None);
        let messages =
            blocks_to_messages::<crate::agency::BlockKind>(&blocks_with_payload(payload));
        let (wire, carried) = provider.convert_messages(&messages, include);
        (
            serde_json::to_value(&wire).expect("the wire serializes"),
            carried,
        )
    }

    /// The golden: the rebuilt array preserves entry order and every documented
    /// field, the reasoning text rides the echo rather than being duplicated
    /// into the content, and the build reports the payload.
    #[test]
    fn payload_rebuilds_reasoning_details_in_order() {
        let payload = OpaquePayload::OpenRouter {
            entries: vec![
                ReasoningDetailEntry {
                    position: 0,
                    entry_type: "reasoning.text".into(),
                    entry_id: Some("rd_0".into()),
                    upstream_format: "anthropic-claude-v1".into(),
                    index: Some(0),
                    content: "step one".into(),
                    signature: Some("sig-1".into()),
                },
                entry(1, "reasoning.summary", "openai-responses-v1", "a summary"),
                entry(2, "reasoning.encrypted", "anthropic-claude-v1", "AAAA"),
            ],
        };
        let (actual, carried) = assistant_message(&payload, true);
        assert!(carried, "the build reports the replayed payload");

        let expected = json!([{
            "role": "assistant",
            "content": "the answer",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": { "name": "search", "arguments": "{}" }
            }],
            "reasoning_details": [
                { "type": "reasoning.text", "id": "rd_0", "format": "anthropic-claude-v1",
                  "index": 0, "text": "step one", "signature": "sig-1" },
                { "type": "reasoning.summary", "id": "rd_1", "format": "openai-responses-v1",
                  "index": 1, "summary": "a summary" },
                { "type": "reasoning.encrypted", "id": "rd_2", "format": "anthropic-claude-v1",
                  "index": 2, "data": "AAAA" }
            ]
        }]);
        assert_eq!(actual, expected);
    }

    /// The hazard filter, exactly: one encrypted-entry-plus-format combination
    /// is dropped, a text entry of the SAME format SURVIVES, and the order of
    /// the survivors is intact. Widening this would quietly discard reasoning
    /// that replays fine.
    #[test]
    fn the_hazard_entry_is_filtered_and_its_text_sibling_survives() {
        let payload = OpaquePayload::OpenRouter {
            entries: vec![
                entry(0, "reasoning.text", "google-gemini-v1", "a thought"),
                entry(1, "reasoning.encrypted", "google-gemini-v1", "POISON"),
                entry(2, "reasoning.encrypted", "anthropic-claude-v1", "SAFE"),
            ],
        };
        let (actual, carried) = assistant_message(&payload, true);
        assert!(carried);

        let details = &actual[0]["reasoning_details"];
        assert_eq!(details.as_array().unwrap().len(), 2);
        assert_eq!(
            details[0]["text"], "a thought",
            "the text entry of the same format survives"
        );
        assert_eq!(details[1]["data"], "SAFE");
        assert!(
            !details.to_string().contains("POISON"),
            "the rejected combination is dropped"
        );
    }

    /// A payload whose every entry is filtered replays nothing: the reasoning
    /// text folds back into the content and no payload is reported, so the
    /// fallback cannot fire for a request that carried nothing.
    #[test]
    fn fully_filtered_payload_degrades_to_text_fold() {
        let payload = OpaquePayload::OpenRouter {
            entries: vec![entry(
                0,
                "reasoning.encrypted",
                "google-gemini-v1",
                "POISON",
            )],
        };
        let (actual, carried) = assistant_message(&payload, true);
        assert!(!carried, "nothing replayed means no payload reported");
        assert_eq!(actual[0]["content"], "let me think\nthe answer");
        assert!(actual[0].get("reasoning_details").is_none());
    }

    /// The variant gate: a foreign payload is OMITTED — the text folds, nothing
    /// is adapted, and the foreign blob never reaches the wire.
    #[test]
    fn foreign_variant_payload_is_omitted() {
        let payload = OpaquePayload::Anthropic {
            signature: "FOREIGN-SIG".into(),
        };
        let (actual, carried) = assistant_message(&payload, true);
        assert!(!carried);
        assert_eq!(actual[0]["content"], "let me think\nthe answer");
        assert!(actual[0].get("reasoning_details").is_none());
        assert!(!actual.to_string().contains("FOREIGN-SIG"));
    }

    /// The knob short-circuits the gate to "omit": the same wire as a vendor
    /// with no echo at all.
    #[test]
    fn knob_off_omits_payloads_and_reports_none() {
        let payload = OpaquePayload::OpenRouter {
            entries: vec![entry(0, "reasoning.text", "anthropic-claude-v1", "step")],
        };
        let (actual, carried) = assistant_message(&payload, false);
        assert!(!carried);
        assert_eq!(actual[0]["content"], "let me think\nthe answer");
        assert!(actual[0].get("reasoning_details").is_none());
    }
}

/// The neutral-to-wire golden for this surface: a reasoning-bearing ledger with
/// no stored payload must serialize to exactly these bytes.
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

        let provider = openrouter_provider("test-key".into(), None);
        let (wire, carried) = provider.convert_messages(
            &blocks_to_messages::<crate::agency::BlockKind>(&blocks),
            true,
        );
        assert!(!carried, "no stored payload means nothing replayed");

        let actual = serde_json::to_value(&wire).expect("the wire serializes");
        let expected = json!([
            {
                "role": "assistant",
                "content": "let me think\nthe answer",
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": { "name": "search", "arguments": "{\"q\":\"x\"}" }
                }]
            },
            {
                "role": "tool",
                "content": "result data",
                "tool_call_id": "call_1"
            }
        ]);
        assert_eq!(actual, expected);
    }
}
