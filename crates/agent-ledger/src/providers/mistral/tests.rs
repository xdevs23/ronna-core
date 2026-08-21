//! This vendor's pins: the defensive chunk decoder, the binary effort, and the
//! typed-chunk replay.

use serde_json::json;

use super::*;
use crate::block::{Block, Role};
use crate::providers::chat::sse::parse_sse_chunk;
use crate::providers::render::blocks_to_messages;
use crate::providers::types::ReasoningDetailEntry;

/// The decoder fixtures run through the FULL base parser, so the shared
/// machinery — the probe, the boundary, the deferral — is exercised exactly as
/// it is in a real stream rather than around it.
mod parser {
    use super::*;

    fn state() -> SseState {
        SseState::new(decode_mistral_content, mistral_thinking_end_payload)
    }

    // The fixtures read better built inline at each call site, so these helpers
    // take the value rather than a reference to one.
    #[allow(clippy::needless_pass_by_value)]
    fn parse(chunk: Value, state: &mut SseState) -> Vec<StreamEvent> {
        parse_sse_chunk(&chunk.to_string(), state)
            .into_iter()
            .map(|r| r.expect("the chunk parses cleanly"))
            .collect()
    }

    /// The answer phase: a plain string, decoded exactly like the base.
    #[test]
    fn plain_string_content_is_text_delta() {
        let mut state = state();
        let events = parse(
            json!({ "choices": [{ "delta": { "content": "hello" } }] }),
            &mut state,
        );
        assert!(matches!(events.as_slice(), [StreamEvent::TextDelta { text }] if text == "hello"));
    }

    /// With reasoning off, no chunk ever appears — the whole stream is ordinary
    /// text and the finish carries no reasoning end.
    #[test]
    fn reasoning_off_parses_as_ordinary_text() {
        let mut state = state();
        let events = parse(
            json!({ "choices": [{ "delta": { "content": "direct answer" } }] }),
            &mut state,
        );
        assert!(
            matches!(events.as_slice(), [StreamEvent::TextDelta { text }] if text == "direct answer")
        );

        let finish = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            &mut state,
        );
        assert!(
            finish.is_empty(),
            "no reasoning block to close, and the end of turn is deferred"
        );
    }

    /// A list of reasoning chunks: each entry's nested texts concatenate into
    /// one reasoning delta.
    #[test]
    fn think_chunk_list_concatenates_thinking_texts() {
        let mut state = state();
        let events = parse(
            json!({ "choices": [{ "delta": { "content": [
                { "type": "thinking", "thinking": [
                    { "type": "text", "text": "step one " },
                    { "type": "text", "text": "step two" }
                ] }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [StreamEvent::ThinkingDelta { text }] if text == "step one step two"
        ));
    }

    /// The mixed transition chunk — one list holding the closing reasoning
    /// chunk and the first text chunk — yields exactly one reasoning end, at
    /// the boundary, in order.
    #[test]
    fn mixed_transition_list_emits_single_thinking_end() {
        let mut state = state();
        let events = parse(
            json!({ "choices": [{ "delta": { "content": [
                { "type": "thinking", "thinking": [{ "type": "text", "text": "done pondering" }] },
                { "type": "text", "text": "the answer" }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [
                StreamEvent::ThinkingDelta { text: thinking },
                StreamEvent::ThinkingEnd { opaque: Some(OpaquePayload::Mistral) },
                StreamEvent::TextDelta { text },
            ] if thinking == "done pondering" && text == "the answer"
        ));
    }

    /// Reasoning split across several deltas accumulates, and the single
    /// reasoning end fires at the first text — whether that text arrives in a
    /// later list or as a plain answer-phase string.
    #[test]
    fn thinking_split_across_deltas_ends_once_at_first_text() {
        let mut state = state();
        let first = parse(
            json!({ "choices": [{ "delta": { "content": [
                { "type": "thinking", "thinking": [{ "type": "text", "text": "part one" }] }
            ] } }] }),
            &mut state,
        );
        assert!(
            matches!(first.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == "part one")
        );

        let second = parse(
            json!({ "choices": [{ "delta": { "content": [
                { "type": "thinking", "thinking": [{ "type": "text", "text": " part two" }] }
            ] } }] }),
            &mut state,
        );
        assert!(
            matches!(second.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == " part two")
        );

        // The answer phase reverts to a plain string: one reasoning end, tagged
        // with the unit payload, then text.
        let third = parse(
            json!({ "choices": [{ "delta": { "content": "answer" } }] }),
            &mut state,
        );
        assert!(matches!(
            third.as_slice(),
            [
                StreamEvent::ThinkingEnd { opaque: Some(OpaquePayload::Mistral) },
                StreamEvent::TextDelta { text }
            ] if text == "answer"
        ));

        let fourth = parse(
            json!({ "choices": [{ "delta": { "content": " more" } }] }),
            &mut state,
        );
        assert!(matches!(fourth.as_slice(), [StreamEvent::TextDelta { text }] if text == " more"));
    }

    /// Defensive: unknown entry types are skipped, and a plain-string reasoning
    /// field is taken verbatim. The wire shape here is inferred, so guessing
    /// wrong must cost nothing.
    #[test]
    fn decoder_tolerates_unknown_entries_and_string_thinking() {
        let mut state = state();
        let events = parse(
            json!({ "choices": [{ "delta": { "content": [
                { "type": "future_widget", "data": 42 },
                { "type": "thinking", "thinking": "bare string" }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [StreamEvent::ThinkingDelta { text }] if text == "bare string"
        ));
    }
}

mod request {
    use super::*;

    /// The wire value is only ever one of two words. Sweeping every level means
    /// a level added later cannot silently ship a value the API rejects.
    #[test]
    fn effort_translation_sweeps_all_levels_to_the_binary() {
        for (level, expected) in [
            (ReasoningLevel::Off, "none"),
            (ReasoningLevel::Auto, "none"),
            (ReasoningLevel::Minimal, "none"),
            (ReasoningLevel::Low, "none"),
            (ReasoningLevel::Medium, "high"),
            (ReasoningLevel::High, "high"),
            (ReasoningLevel::XHigh, "high"),
            (ReasoningLevel::Max, "high"),
        ] {
            assert_eq!(
                mistral_reasoning_effort(level).as_deref(),
                Some(expected),
                "{level:?} must map into the wire binary"
            );
        }
    }

    /// End to end: the binary effort flows through the base's translation seam
    /// onto the wire, alongside the usage opt-in.
    #[test]
    fn request_carries_binary_effort_and_usage_opt_in() {
        let provider = mistral_provider("test-key".into(), None);
        let request = CompletionRequest {
            model: "mistral-medium-3-5".into(),
            messages: vec![],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            stream: true,
            reasoning: Some(ReasoningLevel::Max),
        };
        let body = serde_json::to_value(provider.build_request_body(&request, true, true).0)
            .expect("the request serializes");
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(body["stream_options"], json!({ "include_usage": true }));
    }

    /// Exactly the two documented reasoning slugs get the honest pair;
    /// everything else exposes no control at all.
    #[test]
    fn slug_map_grants_capability_to_exactly_two_slugs() {
        for slug in ["mistral-medium-3-5", "mistral-small-latest"] {
            assert_eq!(
                mistral_reasoning_for_slug(slug).levels,
                vec![ReasoningLevel::Off, ReasoningLevel::High]
            );
        }
        for slug in [
            "mistral-large-latest",
            "codestral-latest",
            "open-mistral-nemo",
            "",
        ] {
            assert!(
                mistral_reasoning_for_slug(slug).is_empty(),
                "{slug:?} must have no control"
            );
        }
    }
}

/// The replay: a reasoning block tagged with this vendor's payload rebuilds as
/// the module's typed chunk array on the assistant message.
mod replay {
    use super::*;

    fn blocks_with_payload(payload: &OpaquePayload) -> Vec<Block> {
        let mut thinking = serde_json::Map::new();
        thinking.insert("content".into(), Value::String("pondered deeply".into()));
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

    fn body_for(payload: &OpaquePayload, include: bool) -> (Value, bool) {
        let provider = mistral_provider("test-key".into(), None);
        let request = CompletionRequest {
            model: "mistral-medium-3-5".into(),
            messages: blocks_to_messages::<crate::agency::BlockKind>(&blocks_with_payload(payload)),
            tools: vec![],
            max_tokens: None,
            temperature: None,
            stream: true,
            reasoning: None,
        };
        let (body, carried) = provider.build_request_body(&request, true, include);
        (
            serde_json::to_value(body).expect("the body serializes"),
            carried,
        )
    }

    /// The golden: the assistant content becomes the typed array — the
    /// reasoning chunk rebuilt from the stored text, then the answer as a text
    /// chunk — this vendor's own wire type, owned by this module.
    #[test]
    fn own_payload_rebuilds_typed_chunk_content() {
        let (body, carried) = body_for(&OpaquePayload::Mistral, true);
        assert!(carried, "the build reports the replayed chunk");
        assert_eq!(
            body["messages"][0]["content"],
            json!([
                { "type": "thinking", "thinking": [{ "type": "text", "text": "pondered deeply" }] },
                { "type": "text", "text": "the answer" }
            ])
        );
        assert_eq!(body["messages"][0]["role"], "assistant");
    }

    /// The variant gate: a foreign payload is OMITTED — a plain string fold,
    /// nothing adapted, and no echo sibling either, since this vendor has none.
    #[test]
    fn foreign_variant_payload_is_omitted() {
        let payload = OpaquePayload::OpenRouter {
            entries: vec![ReasoningDetailEntry {
                position: 0,
                entry_type: "reasoning.text".into(),
                entry_id: None,
                upstream_format: "anthropic-claude-v1".into(),
                index: None,
                content: "foreign".into(),
                signature: None,
            }],
        };
        let (body, carried) = body_for(&payload, true);
        assert!(!carried);
        assert_eq!(
            body["messages"][0]["content"],
            "pondered deeply\nthe answer"
        );
        assert!(body["messages"][0].get("reasoning_details").is_none());
    }

    /// The knob: payloads suppressed gives the same plain fold as a vendor with
    /// no echo at all, and reports none.
    #[test]
    fn knob_off_folds_to_plain_text() {
        let (body, carried) = body_for(&OpaquePayload::Mistral, false);
        assert!(!carried);
        assert_eq!(
            body["messages"][0]["content"],
            "pondered deeply\nthe answer"
        );
    }
}
