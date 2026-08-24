//! This vendor's pins: request goldens, parser fixtures, and the reasoning
//! replay.

use serde_json::json;

use super::parser::{ResponsesSseState, parse_responses_event};
use super::*;
use crate::block::{Block, Role};
use crate::providers::render::blocks_to_messages;
use crate::providers::types::{StopReason, StreamEvent, ToolDefinition};

/// The request builder: the mapping from neutral messages into typed items, and
/// the request policy, pinned as exact body JSON.
mod request_goldens {
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

    fn thinking_text_tool_blocks() -> Vec<Block> {
        vec![
            block(
                1,
                Some(Role::System),
                "system_prompt",
                &[("content", "be helpful")],
            ),
            block(2, Some(Role::User), "text", &[("content", "what is x?")]),
            block(
                3,
                Some(Role::Assistant),
                "thinking",
                &[("content", "let me think")],
            ),
            block(
                4,
                Some(Role::Assistant),
                "text",
                &[("content", "the answer")],
            ),
            block(
                5,
                Some(Role::Assistant),
                "tool_call",
                &[
                    ("tool_call_id", "call_1"),
                    ("name", "search"),
                    ("input", "{\"q\":\"x\"}"),
                ],
            ),
            block(
                6,
                Some(Role::Assistant),
                "tool_result",
                &[("tool_call_id", "call_1"), ("content", "result data")],
            ),
        ]
    }

    fn body_for(model: &str, reasoning: Option<ReasoningLevel>) -> Value {
        let request = CompletionRequest {
            model: model.into(),
            messages: blocks_to_messages::<crate::agency::BlockKind>(&thinking_text_tool_blocks()),
            tools: vec![ToolDefinition {
                name: "search".into(),
                description: "Search the web".into(),
                parameters: json!({ "type": "object" }),
            }],
            max_tokens: None,
            temperature: None,
            stream: true,
            reasoning,
        };
        serde_json::to_value(OpenAiResponsesProvider::build_request_body(&request, true, true).0)
            .expect("the body serializes")
    }

    /// A reasoning, text and tool turn against a reasoning-capable model: the
    /// system content lands in the instructions and never as an input item, the
    /// items appear in order under the call identifier, nothing is retained
    /// server-side, and the capability gate admits the reasoning parameters.
    #[test]
    fn reasoning_capable_model_builds_exact_body() {
        let expected = json!({
            "model": "gpt-5",
            "input": [
                { "type": "message", "role": "user", "content": "what is x?" },
                { "type": "message", "role": "assistant", "content": "let me think\nthe answer" },
                { "type": "function_call", "call_id": "call_1", "name": "search", "arguments": "{\"q\":\"x\"}" },
                { "type": "function_call_output", "call_id": "call_1", "output": "result data" }
            ],
            "instructions": "be helpful",
            "tools": [
                { "type": "function", "name": "search", "description": "Search the web", "parameters": { "type": "object" } }
            ],
            "stream": true,
            "store": false,
            "reasoning": { "summary": "auto", "effort": "high" },
            "include": ["reasoning.encrypted_content"]
        });
        assert_eq!(body_for("gpt-5", Some(ReasoningLevel::High)), expected);
    }

    /// A non-reasoning slug: no reasoning object and no include — the API
    /// rejects them there — but nothing is retained server-side either way.
    #[test]
    fn non_reasoning_model_omits_reasoning_but_still_retains_nothing() {
        let expected = json!({
            "model": "gpt-4o",
            "input": [
                { "type": "message", "role": "user", "content": "what is x?" },
                { "type": "message", "role": "assistant", "content": "let me think\nthe answer" },
                { "type": "function_call", "call_id": "call_1", "name": "search", "arguments": "{\"q\":\"x\"}" },
                { "type": "function_call_output", "call_id": "call_1", "output": "result data" }
            ],
            "instructions": "be helpful",
            "tools": [
                { "type": "function", "name": "search", "description": "Search the web", "parameters": { "type": "object" } }
            ],
            "stream": true,
            "store": false
        });
        assert_eq!(body_for("gpt-4o", Some(ReasoningLevel::High)), expected);
    }

    /// A capable model with no selected level still opts into summaries.
    /// Without that opt-in no reasoning text streams at all, and the turn looks
    /// as though the model did not reason.
    #[test]
    fn capable_model_without_level_still_opts_into_summaries() {
        let body = body_for("o3", None);
        assert_eq!(body["reasoning"], json!({ "summary": "auto" }));
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    }

    /// A dated snapshot pin resolves to its family for the capability gate.
    #[test]
    fn dated_snapshot_slug_stays_reasoning_capable() {
        let body = body_for("gpt-5-2026-04-23", Some(ReasoningLevel::Low));
        assert_eq!(
            body["reasoning"],
            json!({ "summary": "auto", "effort": "low" })
        );
    }

    /// A model id carrying multi-byte characters is normalized without
    /// panicking. The cut point for the dated suffix is eleven bytes from the
    /// end, which lands INSIDE a character here — slicing there aborts the
    /// process over an id this library only passes through.
    #[test]
    fn multibyte_model_id_normalizes_without_panicking() {
        // Twelve bytes, four characters: the cut point is not a boundary.
        assert_eq!(normalize_openai_slug("模型模型"), "模型模型");
        // Long enough to reach the check, with the boundary mid-character.
        assert_eq!(
            normalize_openai_slug("gpt-5-модель-preview"),
            "gpt-5-модель-preview"
        );
        // A multi-byte name with a real dated suffix still resolves to its
        // family: the suffix itself is ASCII, so the boundary holds.
        assert_eq!(normalize_openai_slug("gpt-5-模型-2026-04-23"), "gpt-5-模型");
        // And the capability gate reads it as its family does.
        assert!(!openai_reasoning_for_slug("gpt-5-模型-2026-04-23").is_empty());
    }
}

/// Parser fixtures: event sequences in, exact neutral sequences out.
mod parser_fixtures {
    use super::*;

    // The fixtures read better built inline at each call site, so these helpers
    // take the value rather than a reference to one.
    #[allow(clippy::needless_pass_by_value)]
    fn parse(state: &mut ResponsesSseState, event: Value) -> Vec<Result<StreamEvent, LlmError>> {
        parse_responses_event(&event.to_string(), state)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn parse_ok(state: &mut ResponsesSseState, event: Value) -> Vec<StreamEvent> {
        parse(state, event)
            .into_iter()
            .map(|r| r.expect("the event parses cleanly"))
            .collect()
    }

    fn drive(events: Vec<Value>) -> Vec<Result<StreamEvent, LlmError>> {
        let mut state = ResponsesSseState::default();
        events
            .into_iter()
            .flat_map(|e| parse(&mut state, e))
            .collect()
    }

    fn completed(usage: &Value) -> Value {
        json!({ "type": "response.completed", "response": { "status": "completed", "usage": usage } })
    }

    /// A plain text turn: deltas map to text, and the terminal counts —
    /// including the reasoning count — land on the end of turn.
    #[test]
    fn completed_text_turn_maps_usage_and_end_turn() {
        let out = drive(vec![
            json!({ "type": "response.created", "response": { "status": "in_progress" } }),
            json!({ "type": "response.output_item.added", "item": { "type": "message", "id": "msg_1" } }),
            json!({ "type": "response.output_text.delta", "item_id": "msg_1", "delta": "Hello" }),
            json!({ "type": "response.output_text.delta", "item_id": "msg_1", "delta": " world" }),
            json!({ "type": "response.output_text.done", "item_id": "msg_1", "text": "Hello world" }),
            completed(&json!({
                "input_tokens": 12,
                "output_tokens": 40,
                "output_tokens_details": { "reasoning_tokens": 25 },
                "total_tokens": 52
            })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();

        assert!(matches!(&out[0], StreamEvent::TextDelta { text } if text == "Hello"));
        assert!(matches!(&out[1], StreamEvent::TextDelta { text } if text == " world"));
        match &out[2] {
            StreamEvent::MessageEnd { usage, stop_reason } => {
                assert_eq!(*stop_reason, StopReason::EndTurn);
                assert_eq!(usage.input_tokens, 12);
                assert_eq!(usage.output_tokens, 40);
                assert_eq!(usage.reasoning_tokens, Some(25));
            }
            other => panic!("expected the end of turn, got {other:?}"),
        }
        assert_eq!(out.len(), 3);
    }

    /// An incomplete response truncated by the output ceiling, with whatever
    /// counts the terminal carries read defensively.
    #[test]
    fn incomplete_max_output_tokens_maps_to_max_tokens() {
        let out = drive(vec![
            json!({ "type": "response.output_text.delta", "item_id": "msg_1", "delta": "truncat" }),
            json!({ "type": "response.incomplete", "response": {
                "status": "incomplete",
                "incomplete_details": { "reason": "max_output_tokens" },
                "usage": { "input_tokens": 9, "output_tokens": 100 }
            } }),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();
        match &out[1] {
            StreamEvent::MessageEnd { usage, stop_reason } => {
                assert_eq!(*stop_reason, StopReason::MaxTokens);
                assert_eq!(usage.input_tokens, 9);
                assert_eq!(usage.output_tokens, 100);
                assert_eq!(usage.reasoning_tokens, None);
            }
            other => panic!("expected the end of turn, got {other:?}"),
        }
    }

    /// An incomplete response halted by the content filter, with the counts
    /// absent entirely.
    #[test]
    fn incomplete_content_filter_maps_to_content_filter() {
        let out = drive(vec![json!({ "type": "response.incomplete", "response": {
            "status": "incomplete",
            "incomplete_details": { "reason": "content_filter" }
        } })]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();
        match &out[0] {
            StreamEvent::MessageEnd { usage, stop_reason } => {
                assert_eq!(*stop_reason, StopReason::ContentFilter);
                assert_eq!(usage.reasoning_tokens, None);
            }
            other => panic!("expected the end of turn, got {other:?}"),
        }
    }

    /// A failure becomes a terminal error built from the server's own code and
    /// message — non-recoverable, so no reconnect re-runs a judged request, and
    /// carrying no HTTP status, because the request itself was answered.
    #[test]
    fn failed_maps_to_a_terminal_in_stream_verdict() {
        let mut state = ResponsesSseState::default();
        let out = parse(
            &mut state,
            json!({ "type": "response.failed", "response": {
                "status": "failed",
                "error": { "code": "server_error", "message": "The model failed to respond" },
                "usage": null
            } }),
        );
        assert_eq!(out.len(), 1);
        match &out[0] {
            Err(e @ LlmError::ProviderFailure(message)) => {
                assert!(message.contains("server_error"));
                assert!(message.contains("The model failed to respond"));
                assert!(!e.is_recoverable(), "a server verdict is terminal");
                assert!(
                    !e.to_string().contains("200"),
                    "the error states no status it did not receive: {e}"
                );
            }
            other => panic!("expected a terminal in-stream verdict, got {other:?}"),
        }
    }

    /// A call turn: the tool start is keyed by the call identifier, not the
    /// item id; the lifecycle closes on the item's completion with the
    /// authoritative arguments; and the contiguous run is emitted after the end
    /// of turn.
    #[test]
    fn function_call_turn_serializes_lifecycle_after_message_end() {
        let out = drive(vec![
            json!({ "type": "response.output_item.added", "item": {
                "type": "function_call", "id": "fc_1", "call_id": "call_1",
                "name": "get_weather", "arguments": ""
            } }),
            json!({ "type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": "{\"city\":" }),
            json!({ "type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": "\"SF\"}" }),
            json!({ "type": "response.function_call_arguments.done", "item_id": "fc_1", "name": "get_weather", "arguments": "{\"city\":\"SF\"}" }),
            json!({ "type": "response.output_item.done", "item": {
                "type": "function_call", "id": "fc_1", "call_id": "call_1",
                "name": "get_weather", "arguments": "{\"city\":\"SF\"}"
            } }),
            completed(&json!({ "input_tokens": 5, "output_tokens": 9 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();

        assert!(
            matches!(
                &out[0],
                StreamEvent::MessageEnd {
                    stop_reason: StopReason::ToolUse,
                    ..
                }
            ),
            "the end of turn precedes the buffered lifecycle"
        );
        assert!(
            matches!(&out[1], StreamEvent::ToolUseStart { id, name } if id == "call_1" && name == "get_weather")
        );
        assert!(
            matches!(&out[2], StreamEvent::ToolUseInputDelta { json } if json == "{\"city\":\"SF\"}")
        );
        assert!(matches!(&out[3], StreamEvent::ToolUseEnd));
        assert_eq!(out.len(), 4);
    }

    /// A reasoning turn: summary deltas stream live on the display-only channel
    /// — never the verbatim one, so the lossy summary cannot enter block
    /// content — and the item's completion, correlated by id, emits the end
    /// CARRYING its encrypted content. No start event is ever emitted.
    #[test]
    fn reasoning_item_turn_correlates_deltas_with_item_done() {
        let out = drive(vec![
            json!({ "type": "response.output_item.added", "item": { "type": "reasoning", "id": "rs_1" } }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "weighing " }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "options" }),
            json!({ "type": "response.output_item.done", "item": {
                "type": "reasoning", "id": "rs_1",
                "summary": [{ "type": "summary_text", "text": "weighing options" }],
                "status": null,
                "encrypted_content": "gAAAA-encrypted-blob"
            } }),
            json!({ "type": "response.output_text.delta", "item_id": "msg_1", "delta": "answer" }),
            completed(&json!({ "input_tokens": 3, "output_tokens": 7,
                "output_tokens_details": { "reasoning_tokens": 4 } })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();

        assert!(
            matches!(&out[0], StreamEvent::ThinkingSummaryDelta { text } if text == "weighing ")
        );
        assert!(matches!(&out[1], StreamEvent::ThinkingSummaryDelta { text } if text == "options"));
        assert!(matches!(
            &out[2],
            StreamEvent::ThinkingEnd { opaque: Some(OpaquePayload::OpenAiResponses { item_id, encrypted_content }) }
                if item_id == "rs_1" && encrypted_content == "gAAAA-encrypted-blob"
        ));
        assert!(matches!(&out[3], StreamEvent::TextDelta { text } if text == "answer"));
        match &out[4] {
            StreamEvent::MessageEnd { usage, stop_reason } => {
                assert_eq!(*stop_reason, StopReason::EndTurn);
                assert_eq!(usage.reasoning_tokens, Some(4));
            }
            other => panic!("expected the end of turn, got {other:?}"),
        }
        assert!(
            !out.iter().any(|e| matches!(e, StreamEvent::ThinkingStart)),
            "reasoning blocks are created lazily, so no start is emitted"
        );
        assert_eq!(out.len(), 5);
    }

    /// The open-weight reasoning family is the VERBATIM channel and maps to the
    /// reasoning delta, unlike the summary family.
    #[test]
    fn verbatim_reasoning_family_maps_to_thinking_delta() {
        let mut state = ResponsesSseState::default();
        let out = parse_ok(
            &mut state,
            json!({ "type": "response.reasoning_text.delta", "item_id": "rs_1", "delta": "raw chain" }),
        );
        assert!(
            matches!(out.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == "raw chain")
        );
    }

    /// The two families route to two distinct events. The summary is
    /// display-only and must never masquerade as verbatim reasoning.
    #[test]
    fn summary_family_maps_to_the_summary_channel() {
        let mut state = ResponsesSseState::default();
        let out = parse_ok(
            &mut state,
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1",
                    "summary_index": 0, "delta": "lossy summary" }),
        );
        assert!(matches!(
            out.as_slice(),
            [StreamEvent::ThinkingSummaryDelta { text }] if text == "lossy summary"
        ));
    }

    /// Successive summary PARTS stream with no separator, so the boundary is an
    /// index bump: the first delta of a new part gets a blank line prefixed and
    /// deltas within a part are untouched.
    #[test]
    fn summary_part_boundary_joins_with_blank_line() {
        let out = drive(vec![
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1",
                    "summary_index": 0, "delta": "**First**" }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1",
                    "summary_index": 0, "delta": " section." }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1",
                    "summary_index": 1, "delta": "**Second**" }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1",
                    "summary_index": 1, "delta": " section." }),
            json!({ "type": "response.output_item.done", "item": { "type": "reasoning", "id": "rs_1" } }),
            completed(&json!({ "input_tokens": 1, "output_tokens": 2 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();

        let texts: Vec<&str> = out
            .iter()
            .filter_map(|e| match e {
                StreamEvent::ThinkingSummaryDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            texts,
            vec!["**First**", " section.", "\n\n**Second**", " section."],
            "only the new part's first delta carries the join"
        );
    }

    /// The joiner tracks items independently: a second item starting at its own
    /// first part never inherits the first item's index history.
    #[test]
    fn summary_part_joining_is_per_item() {
        let mut state = ResponsesSseState::default();
        let _ = parse_ok(
            &mut state,
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1",
                    "summary_index": 1, "delta": "first item, part 1" }),
        );
        let out = parse(
            &mut state,
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_2",
                    "summary_index": 0, "delta": "second item" }),
        );
        // The first item holds the slot, so the second's delta defers — but the
        // JOIN decision happens before deferral and must not prefix.
        assert!(out.is_empty());
        assert!(matches!(
            state.deferred_reasoning[0].deltas.as_slice(),
            [StreamEvent::ThinkingSummaryDelta { text }] if text == "second item"
        ));
    }

    /// Interleaved argument fragments for two parallel calls: buffering by item
    /// yields one contiguous run per call, in completion order, after the end of
    /// turn — never interleaved.
    #[test]
    fn reordered_parallel_call_deltas_serialize_per_item() {
        let out = drive(vec![
            json!({ "type": "response.output_item.added", "item": {
                "type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "alpha", "arguments": "" } }),
            json!({ "type": "response.output_item.added", "item": {
                "type": "function_call", "id": "fc_2", "call_id": "call_2", "name": "beta", "arguments": "" } }),
            json!({ "type": "response.function_call_arguments.delta", "item_id": "fc_2", "delta": "{\"b\":" }),
            json!({ "type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": "{\"a\":" }),
            json!({ "type": "response.function_call_arguments.delta", "item_id": "fc_2", "delta": "2}" }),
            json!({ "type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": "1}" }),
            json!({ "type": "response.output_item.done", "item": {
                "type": "function_call", "id": "fc_2", "call_id": "call_2", "name": "beta", "arguments": "{\"b\":2}" } }),
            json!({ "type": "response.output_item.done", "item": {
                "type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "alpha", "arguments": "{\"a\":1}" } }),
            completed(&json!({ "input_tokens": 1, "output_tokens": 2 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();

        assert!(matches!(
            &out[0],
            StreamEvent::MessageEnd {
                stop_reason: StopReason::ToolUse,
                ..
            }
        ));
        // Completion order: the second call closed first.
        assert!(
            matches!(&out[1], StreamEvent::ToolUseStart { id, name } if id == "call_2" && name == "beta")
        );
        assert!(matches!(&out[2], StreamEvent::ToolUseInputDelta { json } if json == "{\"b\":2}"));
        assert!(matches!(&out[3], StreamEvent::ToolUseEnd));
        assert!(
            matches!(&out[4], StreamEvent::ToolUseStart { id, name } if id == "call_1" && name == "alpha")
        );
        assert!(matches!(&out[5], StreamEvent::ToolUseInputDelta { json } if json == "{\"a\":1}"));
        assert!(matches!(&out[6], StreamEvent::ToolUseEnd));
        assert_eq!(out.len(), 7);
    }

    /// Two reasoning items whose deltas interleave: the second defers while the
    /// first holds the slot, then flushes as a contiguous run once the slot
    /// frees. One finalized block per item.
    #[test]
    fn interleaved_reasoning_items_emit_contiguous_runs() {
        let out = drive(vec![
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "first " }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_2", "delta": "second " }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "item" }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_2", "delta": "item" }),
            json!({ "type": "response.output_item.done", "item": { "type": "reasoning", "id": "rs_2" } }),
            json!({ "type": "response.output_item.done", "item": { "type": "reasoning", "id": "rs_1" } }),
            completed(&json!({ "input_tokens": 1, "output_tokens": 2 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();

        assert!(matches!(&out[0], StreamEvent::ThinkingSummaryDelta { text } if text == "first "));
        assert!(matches!(&out[1], StreamEvent::ThinkingSummaryDelta { text } if text == "item"));
        assert!(matches!(&out[2], StreamEvent::ThinkingEnd { .. }));
        assert!(matches!(&out[3], StreamEvent::ThinkingSummaryDelta { text } if text == "second "));
        assert!(matches!(&out[4], StreamEvent::ThinkingSummaryDelta { text } if text == "item"));
        assert!(matches!(&out[5], StreamEvent::ThinkingEnd { .. }));
        assert!(matches!(&out[6], StreamEvent::MessageEnd { .. }));
        assert_eq!(out.len(), 7);
    }

    /// The re-order hazard: a delta arrives AFTER its own item's completion. It
    /// must append to that item's run — one contiguous block — rather than
    /// reopening a spurious second reasoning block.
    #[test]
    fn late_reasoning_delta_after_item_done_appends_to_same_item() {
        let out = drive(vec![
            json!({ "type": "response.output_item.done", "item": { "type": "reasoning", "id": "rs_1" } }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "late thought" }),
            completed(&json!({ "input_tokens": 1, "output_tokens": 2 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();
        assert!(
            matches!(&out[0], StreamEvent::ThinkingSummaryDelta { text } if text == "late thought")
        );
        assert!(matches!(&out[1], StreamEvent::ThinkingEnd { .. }));
        assert!(matches!(&out[2], StreamEvent::MessageEnd { .. }));
        assert_eq!(
            out.iter()
                .filter(|e| matches!(e, StreamEvent::ThinkingEnd { .. }))
                .count(),
            1,
            "one end — the late delta did not split off a second block"
        );
    }

    /// A failure after partial text: the partial delta is delivered, then the
    /// terminal error. The parser surfaces the error rather than fabricating an
    /// end of turn over it.
    #[test]
    fn failed_after_partial_text_yields_delta_then_terminal_error() {
        let out = drive(vec![
            json!({ "type": "response.output_text.delta", "item_id": "msg_1", "delta": "partial pre-failure" }),
            json!({ "type": "response.failed", "response": {
                "status": "failed",
                "error": { "code": "server_error", "message": "boom" },
                "usage": null
            } }),
        ]);
        assert!(
            matches!(&out[0], Ok(StreamEvent::TextDelta { text }) if text == "partial pre-failure")
        );
        assert!(
            matches!(&out[1], Err(e @ LlmError::ProviderFailure(_)) if !e.is_recoverable()),
            "terminal, with no end of turn fabricated over the failure"
        );
        assert_eq!(out.len(), 2);
    }

    /// A reasoning item still in flight at the terminal is flushed before the
    /// end of turn: streamed reasoning is never lost.
    #[test]
    fn terminal_flushes_open_reasoning_before_message_end() {
        let out = drive(vec![
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "dangling" }),
            completed(&json!({ "input_tokens": 1, "output_tokens": 2 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();
        assert!(
            matches!(&out[0], StreamEvent::ThinkingSummaryDelta { text } if text == "dangling")
        );
        assert!(matches!(&out[1], StreamEvent::ThinkingEnd { opaque: None }));
        assert!(matches!(&out[2], StreamEvent::MessageEnd { .. }));
    }

    /// An item whose completion carries no encrypted content finalizes with NO
    /// payload — never a fabricated blob, which the next turn would be rejected
    /// for.
    #[test]
    fn reasoning_item_without_encrypted_content_ends_with_no_payload() {
        let out = drive(vec![
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "thought" }),
            json!({ "type": "response.output_item.done", "item": {
                "type": "reasoning", "id": "rs_1",
                "summary": [{ "type": "summary_text", "text": "thought" }],
                "status": null
            } }),
            completed(&json!({ "input_tokens": 1, "output_tokens": 2 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();
        assert!(matches!(&out[1], StreamEvent::ThinkingEnd { opaque: None }));
    }

    /// A deferred item keeps its captured payload through the buffer: the
    /// flushed run's end carries it.
    #[test]
    fn deferred_reasoning_item_keeps_its_payload_through_the_buffer() {
        let out = drive(vec![
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "first" }),
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_2", "delta": "second" }),
            json!({ "type": "response.output_item.done", "item": {
                "type": "reasoning", "id": "rs_2", "encrypted_content": "blob-2" } }),
            json!({ "type": "response.output_item.done", "item": {
                "type": "reasoning", "id": "rs_1", "encrypted_content": "blob-1" } }),
            completed(&json!({ "input_tokens": 1, "output_tokens": 2 })),
        ]);
        let out: Vec<StreamEvent> = out.into_iter().map(Result::unwrap).collect();

        assert!(matches!(
            &out[1],
            StreamEvent::ThinkingEnd { opaque: Some(OpaquePayload::OpenAiResponses { item_id, encrypted_content }) }
                if item_id == "rs_1" && encrypted_content == "blob-1"
        ));
        assert!(matches!(&out[2], StreamEvent::ThinkingSummaryDelta { text } if text == "second"));
        assert!(matches!(
            &out[3],
            StreamEvent::ThinkingEnd { opaque: Some(OpaquePayload::OpenAiResponses { item_id, encrypted_content }) }
                if item_id == "rs_2" && encrypted_content == "blob-2"
        ));
    }

    /// The status is a six-value enum: a non-terminal status riding a
    /// terminal-named event is ignored — never a fabricated end of turn — and a
    /// terminal cancellation is an error.
    #[test]
    fn six_status_enum_handled_exhaustively() {
        let mut state = ResponsesSseState::default();
        for status in ["queued", "in_progress"] {
            let out = parse(
                &mut state,
                json!({ "type": "response.completed", "response": { "status": status } }),
            );
            assert!(out.is_empty(), "{status} on a terminal event is ignored");
        }

        let out = parse(
            &mut state,
            json!({ "type": "response.failed", "response": { "status": "cancelled" } }),
        );
        assert!(
            matches!(&out[0], Err(LlmError::ProviderFailure(message)) if message.contains("cancelled")),
            "a terminal cancellation is an error"
        );
    }
}

/// The replay: a stored payload of THIS vendor's variant renders as a verbatim
/// reasoning item, placed before the item it belongs to.
mod replay {
    use super::*;

    fn blocks_with_payload(payload: &OpaquePayload) -> Vec<Block> {
        let mut thinking = serde_json::Map::new();
        thinking.insert("content".into(), Value::String("weighing options".into()));
        thinking.insert("opaque".into(), serde_json::to_value(payload).unwrap());
        let mut tool_call = serde_json::Map::new();
        for (k, v) in [
            ("tool_call_id", "call_1"),
            ("name", "search"),
            ("input", "{}"),
        ] {
            tool_call.insert(k.into(), Value::String(v.into()));
        }
        let mut tool_result = serde_json::Map::new();
        for (k, v) in [("tool_call_id", "call_1"), ("content", "result data")] {
            tool_result.insert(k.into(), Value::String(v.into()));
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
                block_type: "tool_call".into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: tool_call,
            },
            Block {
                id: 3,
                role: Some(Role::Assistant),
                block_type: "tool_result".into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: tool_result,
            },
        ]
    }

    fn input_for(payload: &OpaquePayload, include: bool) -> (Value, bool) {
        let messages =
            blocks_to_messages::<crate::agency::BlockKind>(&blocks_with_payload(payload));
        let (_, items, carried) = convert_input(&messages, include);
        (
            serde_json::to_value(&items).expect("the items serialize"),
            carried,
        )
    }

    fn own_payload() -> OpaquePayload {
        OpaquePayload::OpenAiResponses {
            item_id: "rs_abc".into(),
            encrypted_content: "gAAAA-blob".into(),
        }
    }

    /// The golden: the verbatim reasoning item, id included, PRECEDES the call
    /// it belongs to. Dropping or reordering it is the documented failure where
    /// a reasoning item arrives without its required following item.
    #[test]
    fn own_payload_replays_verbatim_reasoning_item_before_its_call() {
        let (items, carried) = input_for(&own_payload(), true);
        assert!(carried, "the build reports the replayed payload");

        let expected = json!([
            {
                "type": "reasoning",
                "id": "rs_abc",
                "summary": [{ "type": "summary_text", "text": "weighing options" }],
                "status": null,
                "encrypted_content": "gAAAA-blob"
            },
            { "type": "function_call", "call_id": "call_1", "name": "search", "arguments": "{}" },
            { "type": "function_call_output", "call_id": "call_1", "output": "result data" }
        ]);
        assert_eq!(items, expected);
    }

    /// Two reasoning-and-call pairs in ONE contiguous group must replay in
    /// their exact interleaved order. Regrouping by type strands each reasoning
    /// item from its required following item, and the request is rejected
    /// outright.
    #[test]
    fn multi_pair_turn_preserves_interleaved_item_order() {
        let block = |id: i64, block_type: &str, pairs: &[(&str, &str)]| {
            let mut fields = serde_json::Map::new();
            for (k, v) in pairs {
                fields.insert((*k).into(), Value::String((*v).into()));
            }
            Block {
                id,
                role: Some(Role::Assistant),
                block_type: block_type.into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields,
            }
        };
        let reasoning = |id: i64, item: &str, text: &str, blob: &str| {
            let mut fields = serde_json::Map::new();
            fields.insert("content".into(), Value::String(text.into()));
            fields.insert(
                "opaque".into(),
                serde_json::to_value(OpaquePayload::OpenAiResponses {
                    item_id: item.into(),
                    encrypted_content: blob.into(),
                })
                .unwrap(),
            );
            Block {
                id,
                role: Some(Role::Assistant),
                block_type: "thinking".into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields,
            }
        };
        let blocks = vec![
            reasoning(1, "rs_a", "step one", "blob-a"),
            block(
                2,
                "tool_call",
                &[
                    ("tool_call_id", "call_a"),
                    ("name", "search"),
                    ("input", "{}"),
                ],
            ),
            reasoning(3, "rs_b", "step two", "blob-b"),
            block(
                4,
                "tool_call",
                &[
                    ("tool_call_id", "call_b"),
                    ("name", "fetch"),
                    ("input", "{}"),
                ],
            ),
        ];
        let messages = blocks_to_messages::<crate::agency::BlockKind>(&blocks);
        let (_, items, carried) = convert_input(&messages, true);
        let items = serde_json::to_value(&items).unwrap();
        assert!(carried);

        let expected = json!([
            { "type": "reasoning", "id": "rs_a", "summary": [{ "type": "summary_text", "text": "step one" }], "status": null, "encrypted_content": "blob-a" },
            { "type": "function_call", "call_id": "call_a", "name": "search", "arguments": "{}" },
            { "type": "reasoning", "id": "rs_b", "summary": [{ "type": "summary_text", "text": "step two" }], "status": null, "encrypted_content": "blob-b" },
            { "type": "function_call", "call_id": "call_b", "name": "fetch", "arguments": "{}" }
        ]);
        assert_eq!(items, expected);
    }

    /// The variant gate: a foreign payload is OMITTED — the reasoning text folds
    /// into a plain message, no reasoning item, no adapted blob.
    #[test]
    fn foreign_variant_payload_is_omitted() {
        let payload = OpaquePayload::Anthropic {
            signature: "FOREIGN-SIG".into(),
        };
        let (items, carried) = input_for(&payload, true);
        assert!(!carried);
        assert_eq!(items[0]["type"], "message");
        assert_eq!(items[0]["content"], "weighing options");
        assert!(!items.to_string().contains("FOREIGN-SIG"));
        assert!(!items.to_string().contains("\"reasoning\""));
    }

    /// The knob: payloads suppressed degrades to text only, and reports none.
    #[test]
    fn knob_off_folds_reasoning_to_message_text() {
        let (items, carried) = input_for(&own_payload(), false);
        assert!(!carried);
        assert_eq!(items[0]["type"], "message");
        assert_eq!(items[0]["content"], "weighing options");
        assert!(!items.to_string().contains("gAAAA-blob"));
    }
}

mod registry {
    use super::*;
    use crate::providers::ProviderRegistry;

    /// Type-id continuity: the id resolves to this module even though the wire
    /// surface it speaks has changed since instances were first persisted under
    /// that id. A rename would orphan every one of them.
    #[tokio::test]
    async fn type_id_resolves_to_this_module() {
        let store = crate::store::Store::in_memory().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Box::new(OpenAiModule::new(store.tx()).await));

        let module = registry.get("openai").expect("the type resolves");
        assert_eq!(module.type_id(), "openai");
        assert_eq!(module.display_name(), "OpenAI");
    }
}

/// Empty assistant content, on this vendor's wire: it is ECHOED, in every shape
/// the render pass can produce it in. This API accepts an empty message content
/// — its documented refusal is of a NULL text, which this wire never sends — so
/// a turn that said nothing rides out as the empty message it was, and the model
/// reads its own silence back. The one item that still leaves is the replayed
/// reasoning item no item follows, which the API rejects outright.
mod empty_assistant_content {
    use super::*;

    fn convert(messages: &[Message]) -> Value {
        let (_, items, _) = convert_input(messages, true);
        serde_json::to_value(&items).expect("the items serialize")
    }

    fn text(role: MessageRole, text: &str) -> Message {
        Message {
            role,
            content: MessageContent::Text(text.into()),
        }
    }

    fn parts(role: MessageRole, parts: Vec<ContentPart>) -> Message {
        Message {
            role,
            content: MessageContent::Parts(parts),
        }
    }

    fn tool_use() -> ContentPart {
        ContentPart::ToolUse {
            id: "c1".into(),
            name: "search".into(),
            input: json!({}),
        }
    }

    fn user_hi() -> Message {
        text(MessageRole::User, "hi")
    }

    fn user_hi_item() -> Value {
        json!({ "type": "message", "role": "user", "content": "hi" })
    }

    /// An empty assistant message item, as it rides out, followed by the call.
    fn empty_message_then_call() -> Value {
        json!([
            { "type": "message", "role": "assistant", "content": "" },
            { "type": "function_call", "call_id": "c1", "name": "search", "arguments": "{}" }
        ])
    }

    /// Message level: a silent turn contributes its own message item, empty
    /// content and all.
    #[test]
    fn an_empty_assistant_message_is_echoed() {
        assert_eq!(
            convert(&[user_hi(), text(MessageRole::Assistant, "")]),
            json!([
                user_hi_item(),
                { "type": "message", "role": "assistant", "content": "" }
            ])
        );
    }

    /// Whitespace is content like any other: it goes out verbatim, untrimmed.
    #[test]
    fn a_whitespace_only_assistant_message_is_echoed_verbatim() {
        assert_eq!(
            convert(&[user_hi(), text(MessageRole::Assistant, "   \n ")]),
            json!([
                user_hi_item(),
                { "type": "message", "role": "assistant", "content": "   \n " }
            ])
        );
    }

    /// Part level: the empty text flushes into its own message item at the
    /// position it held, and the call follows it — order intact.
    #[test]
    fn an_empty_text_part_beside_a_tool_call_keeps_both() {
        assert_eq!(
            convert(&[parts(
                MessageRole::Assistant,
                vec![
                    ContentPart::Text {
                        text: String::new()
                    },
                    tool_use()
                ]
            )]),
            empty_message_then_call()
        );
    }

    /// The other empty part shape: a reasoning part with no replayable payload
    /// folds into the message text, and an empty one folds into an empty
    /// message — which is echoed like any other empty text.
    #[test]
    fn a_degraded_empty_reasoning_beside_a_tool_call_is_echoed() {
        assert_eq!(
            convert(&[parts(
                MessageRole::Assistant,
                vec![
                    ContentPart::Reasoning {
                        text: String::new(),
                        opaque: None,
                    },
                    tool_use(),
                ],
            )]),
            empty_message_then_call()
        );
    }

    /// A reasoning item that ENDS its group leaves with the group, whatever
    /// emptied the space behind it. Here nothing was dropped at all: the text
    /// is real, and it flushes into its message BEFORE the replay is pushed,
    /// so the replay ends up last with nothing following it. This API refuses a
    /// reasoning item its produced item does not follow, so sending it would
    /// fail the whole request — the item leaves, and the group keeps its text.
    ///
    /// Pinned deliberately (orchestrator, 2026-08-24): the rule is about the
    /// item being TRAILING, not about the drop, and a reader who assumed the
    /// drop was its only trigger would narrow it and ship the rejected shape.
    #[test]
    fn a_trailing_reasoning_leaves_even_when_the_text_before_it_is_real() {
        assert_eq!(
            convert(&[parts(
                MessageRole::Assistant,
                vec![
                    ContentPart::Text { text: "hi".into() },
                    ContentPart::Reasoning {
                        text: "thought".into(),
                        opaque: Some(OpaquePayload::OpenAiResponses {
                            item_id: "rs_abc".into(),
                            encrypted_content: "gAAAA-blob".into(),
                        }),
                    },
                ],
            )]),
            json!([{ "type": "message", "role": "assistant", "content": "hi" }])
        );
    }

    /// A parts message whose only text is whitespace still carries that text,
    /// so it flushes into a message item and goes out as it stands.
    #[test]
    fn a_parts_message_of_nothing_but_empty_text_is_echoed() {
        assert_eq!(
            convert(&[
                user_hi(),
                parts(
                    MessageRole::Assistant,
                    vec![ContentPart::Text { text: "  ".into() }]
                )
            ]),
            json!([
                user_hi_item(),
                { "type": "message", "role": "assistant", "content": "  " }
            ])
        );
    }

    /// The verbatim reasoning item is not text and is never dropped: this
    /// vendor's own payload replays even when the visible summary is empty,
    /// which is exactly the summary-only case the payload exists for.
    #[test]
    fn an_empty_reasoning_carrying_this_vendors_payload_still_replays() {
        let actual = convert(&[parts(
            MessageRole::Assistant,
            vec![
                ContentPart::Reasoning {
                    text: String::new(),
                    opaque: Some(OpaquePayload::OpenAiResponses {
                        item_id: "rs_abc".into(),
                        encrypted_content: "gAAAA-blob".into(),
                    }),
                },
                tool_use(),
            ],
        )]);
        // The WHOLE input, not just its first item: an empty text item slipping
        // in behind the replay is exactly what this pin exists to catch, and it
        // would sit at an index a spot-check never reads.
        assert_eq!(
            actual,
            json!([
                {
                    "type": "reasoning",
                    "id": "rs_abc",
                    "summary": [{ "type": "summary_text", "text": "" }],
                    "status": null,
                    "encrypted_content": "gAAAA-blob"
                },
                { "type": "function_call", "call_id": "c1", "name": "search", "arguments": "{}" }
            ])
        );
    }

    /// A replayed reasoning item cannot be the last thing a GROUP sends: this
    /// API rejects a reasoning item that no produced item follows ("Item 'rs_…'
    /// of type 'reasoning' was provided without its required following item"),
    /// so a group whose replay is all it has left sends nothing at all. The
    /// next message's item is no rescue — it belongs to another group, and the
    /// pop has already happened by the time it is converted. The build reports
    /// no carried payload for a replay that never went out, so the one-shot
    /// payload retry is not spent on a request that carried none.
    #[test]
    fn a_replayed_reasoning_left_with_nothing_to_follow_it_is_not_sent() {
        let messages = [
            user_hi(),
            parts(
                MessageRole::Assistant,
                vec![ContentPart::Reasoning {
                    text: "let me think".into(),
                    opaque: Some(OpaquePayload::OpenAiResponses {
                        item_id: "rs_1".into(),
                        encrypted_content: "gAAAA-blob".into(),
                    }),
                }],
            ),
            text(MessageRole::User, "still there?"),
        ];

        let (_, items, carried) = convert_input(&messages, true);
        assert!(!carried, "no payload actually rode out");
        assert_eq!(
            serde_json::to_value(&items).expect("the items serialize"),
            json!([
                user_hi_item(),
                { "type": "message", "role": "user", "content": "still there?" }
            ])
        );
    }

    /// The echo is what keeps a replay company: a turn that thought and then
    /// said nothing sends its reasoning item followed by the empty message the
    /// silence became, which is the required following item this API asks for.
    /// Before the echo this shape lost both; it is the pin on why the pop is
    /// now the narrow case rather than the ordinary one.
    #[test]
    fn a_replayed_reasoning_followed_by_the_echoed_silence_rides_out() {
        let messages = [parts(
            MessageRole::Assistant,
            vec![
                ContentPart::Reasoning {
                    text: "let me think".into(),
                    opaque: Some(OpaquePayload::OpenAiResponses {
                        item_id: "rs_1".into(),
                        encrypted_content: "gAAAA-blob".into(),
                    }),
                },
                ContentPart::Text {
                    text: String::new(),
                },
            ],
        )];

        let (_, items, carried) = convert_input(&messages, true);
        assert!(carried, "the replay rode out, so the payload is reported");
        assert_eq!(
            serde_json::to_value(&items).expect("the items serialize"),
            json!([
                {
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": [{ "type": "summary_text", "text": "let me think" }],
                    "status": null,
                    "encrypted_content": "gAAAA-blob"
                },
                { "type": "message", "role": "assistant", "content": "" }
            ])
        );
    }

    /// Nothing else moves: surviving content converts exactly as it did.
    #[test]
    fn non_empty_content_converts_exactly_as_before() {
        assert_eq!(
            convert(&[
                text(MessageRole::Assistant, " hi "),
                parts(
                    MessageRole::Assistant,
                    vec![
                        ContentPart::Text {
                            text: "on it".into()
                        },
                        tool_use()
                    ]
                ),
            ]),
            json!([
                { "type": "message", "role": "assistant", "content": " hi " },
                { "type": "message", "role": "assistant", "content": "on it" },
                { "type": "function_call", "call_id": "c1", "name": "search", "arguments": "{}" },
            ])
        );
    }

    /// The degenerate case: a request left with no input item is refused by
    /// name, before anything is sent, instead of going out with an empty input
    /// for the endpoint to refuse in its own words.
    ///
    /// The shape that reaches it is the trailing-replay pop, the one conversion
    /// on this wire that still takes an item away: a history whose only message
    /// is a turn that thought and produced nothing else has its replay popped
    /// and nothing left behind it. An empty assistant message no longer empties
    /// anything — it is echoed, and an echoed item is an item.
    #[tokio::test]
    async fn a_request_the_reasoning_pop_emptied_is_refused_before_it_is_sent() {
        let request = CompletionRequest {
            model: "gpt-5-test".into(),
            messages: vec![parts(
                MessageRole::Assistant,
                vec![ContentPart::Reasoning {
                    text: "let me think".into(),
                    opaque: Some(OpaquePayload::OpenAiResponses {
                        item_id: "rs_1".into(),
                        encrypted_content: "gAAAA-blob".into(),
                    }),
                }],
            )],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            stream: true,
            reasoning: None,
        };

        let err = OpenAiResponsesProvider::new("test-key".into(), None)
            .open_turn(request, true)
            .await
            .err()
            .expect("a request with nothing to send is refused");
        assert!(matches!(err, LlmError::NoMessage));
    }

    /// Adjacency is untouched, and nothing merges: the call's output still
    /// follows the call, and two adjacent user messages stay two items.
    #[test]
    fn adjacent_messages_are_neither_merged_nor_reordered() {
        assert_eq!(
            convert(&[
                parts(MessageRole::Assistant, vec![tool_use()]),
                parts(
                    MessageRole::User,
                    vec![ContentPart::ToolResult {
                        tool_use_id: "c1".into(),
                        content: "found".into(),
                    }],
                ),
                user_hi(),
            ]),
            json!([
                { "type": "function_call", "call_id": "c1", "name": "search", "arguments": "{}" },
                { "type": "function_call_output", "call_id": "c1", "output": "found" },
                user_hi_item(),
            ])
        );
    }
}
