//! The chat base's own pins: the reasoning ingest and the deferred end of turn.

use reqwest::header::HeaderMap;
use serde_json::{Value, json};

use super::base::*;
use super::sse::{SseState, finish_stream, parse_sse_chunk};
use crate::providers::types::{
    CompletionRequest, OpaquePayload, ReasoningDetailEntry, ReasoningLevel, StopReason, StreamEvent,
};

// The fixtures read better built inline at each call site, so these helpers
// take the value rather than a reference to one.
#[allow(clippy::needless_pass_by_value)]
fn parse(chunk: Value, state: &mut SseState) -> Vec<StreamEvent> {
    parse_sse_chunk(&chunk.to_string(), state)
        .into_iter()
        .map(|r| r.expect("the chunk parses cleanly"))
        .collect()
}

/// A provider on this base with no vendor seams filled, for the request-shape
/// pins. The credential is never sent anywhere: no test here opens a stream.
fn plain_provider() -> ChatProvider {
    ChatProvider::with_headers(
        "test-key".into(),
        None,
        "https://a-chat-endpoint.example/v1",
        HeaderMap::new(),
    )
}

mod reasoning_ingest {
    use super::*;

    /// The flattened reasoning string becomes a reasoning delta.
    #[test]
    fn reasoning_string_becomes_thinking_delta() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "reasoning": "pondering" } }] }),
            &mut state,
        );
        assert!(
            matches!(events.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == "pondering")
        );
        assert!(state.reasoning_open);
    }

    /// The typed array routes each entry's text by its structural type:
    /// verbatim reasoning to one channel, the lossy summary to the other. An
    /// empty entry emits nothing. Both channels open the reasoning block.
    #[test]
    fn reasoning_details_route_text_and_summary_to_distinct_channels() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.text", "text": "step one " },
                { "type": "reasoning.summary", "summary": "step two" },
                { "type": "reasoning.text", "text": "" },
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [
                StreamEvent::ThinkingDelta { text: verbatim },
                StreamEvent::ThinkingSummaryDelta { text: summary },
            ] if verbatim == "step one " && summary == "step two"
        ));
        assert!(
            state.reasoning_open,
            "either channel opens the reasoning block"
        );
    }

    /// Reasoning then content emits exactly one reasoning end, at the boundary,
    /// before the text.
    #[test]
    fn reasoning_to_content_transition_emits_one_thinking_end() {
        let mut state = SseState::default();
        let first = parse(
            json!({ "choices": [{ "delta": { "reasoning": "thinking" } }] }),
            &mut state,
        );
        assert!(matches!(
            first.as_slice(),
            [StreamEvent::ThinkingDelta { .. }]
        ));

        let second = parse(
            json!({ "choices": [{ "delta": { "content": "answer" } }] }),
            &mut state,
        );
        assert!(matches!(
            second.as_slice(),
            [StreamEvent::ThinkingEnd { .. }, StreamEvent::TextDelta { text }] if text == "answer"
        ));
        assert!(!state.reasoning_open);

        // A later content chunk must not emit a second end.
        let third = parse(
            json!({ "choices": [{ "delta": { "content": " more" } }] }),
            &mut state,
        );
        assert!(matches!(third.as_slice(), [StreamEvent::TextDelta { text }] if text == " more"));
    }

    /// Plain content with no preceding reasoning is a bare text delta.
    #[test]
    fn content_without_reasoning_is_plain_text_delta() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "content": "hello" } }] }),
            &mut state,
        );
        assert!(matches!(events.as_slice(), [StreamEvent::TextDelta { text }] if text == "hello"));
    }

    /// A finish after streamed reasoning finalizes the reasoning block
    /// immediately; the end of turn waits for the counts.
    #[test]
    fn finish_after_reasoning_emits_thinking_end_then_deferred_message_end() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "reasoning": "thinking" } }] }),
            &mut state,
        );
        let events = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [StreamEvent::ThinkingEnd { .. }]
        ));

        let done: Vec<StreamEvent> = finish_stream(&mut state)
            .into_iter()
            .map(|r| r.expect("the terminal drain is clean"))
            .collect();
        assert!(matches!(done.as_slice(), [StreamEvent::MessageEnd { .. }]));
    }

    /// The second probe leg: the bare reasoning-content string, the vocabulary
    /// of the vendors that publish no typed array.
    #[test]
    fn reasoning_content_string_becomes_thinking_delta() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "reasoning_content": "mulling" } }] }),
            &mut state,
        );
        assert!(
            matches!(events.as_slice(), [StreamEvent::ThinkingDelta { text }] if text == "mulling")
        );
    }

    /// The first entry of a turn may be metadata only — the content fields are
    /// optional. No event, no open block, and no spurious end on the next
    /// content chunk.
    #[test]
    fn metadata_only_reasoning_details_chunk_is_inert() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.encrypted", "id": "rd_1", "format": "google-gemini-v1" }
            ] } }] }),
            &mut state,
        );
        assert!(events.is_empty());

        let next = parse(
            json!({ "choices": [{ "delta": { "content": "answer" } }] }),
            &mut state,
        );
        assert!(matches!(next.as_slice(), [StreamEvent::TextDelta { text }] if text == "answer"));
    }

    /// A delta with none of the probe fields is inert, which is why the probe
    /// can run unconditionally on every vendor.
    #[test]
    fn delta_without_reasoning_fields_is_inert() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "role": "assistant" } }] }),
            &mut state,
        );
        assert!(events.is_empty());
    }

    /// Every streamed entry — all three types, metadata-only chunks included —
    /// is decomposed in order onto the boundary end's payload, with content
    /// slotted per type and the signature preserved.
    #[test]
    fn reasoning_details_entries_are_captured_onto_thinking_end() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.text", "id": "rd_1", "format": "anthropic-claude-v1",
                  "index": 0, "text": "step one", "signature": "sig-1" },
                { "type": "reasoning.summary", "format": "openai-responses-v1", "summary": "a summary" },
            ] } }] }),
            &mut state,
        );
        parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.encrypted", "id": "rd_3", "format": "google-gemini-v1", "data": "AAAA" }
            ] } }] }),
            &mut state,
        );

        let events = parse(
            json!({ "choices": [{ "delta": { "content": "answer" } }] }),
            &mut state,
        );
        let [
            StreamEvent::ThinkingEnd {
                opaque: Some(OpaquePayload::OpenRouter { entries }),
            },
            StreamEvent::TextDelta { .. },
        ] = events.as_slice()
        else {
            panic!("expected a payload-carrying end, got {events:?}");
        };

        assert_eq!(entries.len(), 3, "all entries captured, encrypted included");
        assert_eq!(
            entries[0],
            ReasoningDetailEntry {
                position: 0,
                entry_type: "reasoning.text".into(),
                entry_id: Some("rd_1".into()),
                upstream_format: "anthropic-claude-v1".into(),
                index: Some(0),
                content: "step one".into(),
                signature: Some("sig-1".into()),
            }
        );
        assert_eq!(entries[1].entry_type, "reasoning.summary");
        assert_eq!(entries[1].content, "a summary");
        assert_eq!(entries[1].entry_id, None);
        assert_eq!(entries[2].entry_type, "reasoning.encrypted");
        assert_eq!(entries[2].content, "AAAA");
        assert_eq!(
            entries[2].position, 2,
            "array order is preserved across chunks"
        );
    }

    /// A stream with only the flat reasoning string closes with NO payload: a
    /// plain chat surface has no echo mechanism, and a fabricated payload would
    /// be rejected on the next turn.
    #[test]
    fn flat_reasoning_string_closes_with_no_payload() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "reasoning": "pondering" } }] }),
            &mut state,
        );
        let events = parse(
            json!({ "choices": [{ "delta": { "content": "answer" } }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [
                StreamEvent::ThinkingEnd { opaque: None },
                StreamEvent::TextDelta { .. }
            ]
        ));
    }

    /// The live shape from a gateway fronting a summary-only model: MANY
    /// summary deltas, one token per chunk. Each routes to the display-only
    /// channel — never the verbatim one — so a summary can NEVER land in block
    /// content. The trailing encrypted entry contributes no display text but is
    /// captured, and the content boundary closes with every entry verbatim.
    #[test]
    fn summary_deltas_stream_the_summary_channel_only() {
        let mut state = SseState::default();

        let first = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.summary", "format": "azure-openai-responses-v1",
                  "index": 0, "summary": "**Weighing options**\n\nI" }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            first.as_slice(),
            [StreamEvent::ThinkingSummaryDelta { text }] if text == "**Weighing options**\n\nI"
        ));
        assert!(
            state.reasoning_open,
            "the summary channel opens the reasoning block"
        );

        let second = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.summary", "format": "azure-openai-responses-v1",
                  "index": 0, "summary": " compare." }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            second.as_slice(),
            [StreamEvent::ThinkingSummaryDelta { text }] if text == " compare."
        ));

        let encrypted = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.encrypted", "id": "rs_1",
                  "format": "azure-openai-responses-v1", "index": 0, "data": "BLOB" }
            ] } }] }),
            &mut state,
        );
        assert!(
            encrypted.is_empty(),
            "encrypted entries carry no display text"
        );

        let content = parse(
            json!({ "choices": [{ "delta": { "content": "answer" } }] }),
            &mut state,
        );
        let [
            StreamEvent::ThinkingEnd {
                opaque: Some(OpaquePayload::OpenRouter { entries }),
            },
            StreamEvent::TextDelta { .. },
        ] = content.as_slice()
        else {
            panic!("expected a payload-carrying end, got {content:?}");
        };
        assert_eq!(entries.len(), 3, "all entries captured, encrypted included");
        assert_eq!(entries[0].entry_type, "reasoning.summary");
        assert_eq!(entries[0].content, "**Weighing options**\n\nI");
        assert_eq!(entries[2].entry_type, "reasoning.encrypted");
        assert_eq!(entries[2].content, "BLOB");
    }

    /// Signature-bearing text entries stay VERBATIM — routed to the reasoning
    /// channel, landing in block content — and the signature rides the captured
    /// payload unchanged.
    #[test]
    fn signed_text_deltas_stay_verbatim() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.text", "format": "anthropic-claude-v1",
                  "index": 0, "text": "let me reason", "signature": "sig-1" }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [StreamEvent::ThinkingDelta { text }] if text == "let me reason"
        ));
    }

    /// A MIXED turn — verbatim and summary entries in the same wire array —
    /// fills BOTH channels in order.
    #[test]
    fn mixed_text_and_summary_details_fill_both_channels() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.text", "index": 0, "text": "verbatim chain" },
                { "type": "reasoning.summary", "index": 0, "summary": "the gist" }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [
                StreamEvent::ThinkingDelta { text: verbatim },
                StreamEvent::ThinkingSummaryDelta { text: summary },
            ] if verbatim == "verbatim chain" && summary == "the gist"
        ));
    }

    /// A summary index bump marks a new section, so the first delta of the new
    /// section gets a blank line prefixed. Deltas within one section join
    /// verbatim.
    #[test]
    fn summary_index_bump_prefixes_a_blank_line() {
        let mut state = SseState::default();

        let sec0 = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.summary", "index": 0, "summary": "**First**" }
            ] } }] }),
            &mut state,
        );
        assert!(
            matches!(
                sec0.as_slice(),
                [StreamEvent::ThinkingSummaryDelta { text }] if text == "**First**"
            ),
            "the opening section is not prefixed"
        );

        let sec0_more = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.summary", "index": 0, "summary": " continues" }
            ] } }] }),
            &mut state,
        );
        assert!(matches!(
            sec0_more.as_slice(),
            [StreamEvent::ThinkingSummaryDelta { text }] if text == " continues"
        ));

        let sec1 = parse(
            json!({ "choices": [{ "delta": { "reasoning_details": [
                { "type": "reasoning.summary", "index": 1, "summary": "**Second**" }
            ] } }] }),
            &mut state,
        );
        assert!(
            matches!(
                sec1.as_slice(),
                [StreamEvent::ThinkingSummaryDelta { text }] if text == "\n\n**Second**"
            ),
            "a new summary section is separated by a blank line"
        );

        // The joiner's prefix is a DISPLAY concern. The captured echo must
        // record each entry's RAW wire text, so the boundary-crossing section's
        // stored content stays free of the prefix — otherwise a replayed turn
        // would carry text the vendor never sent.
        let content = parse(
            json!({ "choices": [{ "delta": { "content": "answer" } }] }),
            &mut state,
        );
        let [
            StreamEvent::ThinkingEnd {
                opaque: Some(OpaquePayload::OpenRouter { entries }),
            },
            StreamEvent::TextDelta { .. },
        ] = content.as_slice()
        else {
            panic!("expected a payload-carrying end, got {content:?}");
        };
        assert_eq!(entries.len(), 3, "all three summary entries captured");
        assert_eq!(entries[0].content, "**First**");
        assert_eq!(entries[1].content, " continues");
        assert_eq!(
            entries[2].content, "**Second**",
            "the boundary-crossing entry's captured content has no joiner prefix"
        );
    }
}

/// The parts branch of the request wire: the role a converted group leaves
/// under, and the typed-chunks form a media-bearing group takes.
mod parts_wire {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;

    use super::*;
    use crate::providers::types::{ContentPart, Message, MessageContent, MessageRole};

    fn convert(messages: &[Message]) -> Value {
        let (wire, _) = plain_provider().convert_messages(messages, true);
        serde_json::to_value(&wire).expect("the wire serializes")
    }

    fn parts(role: MessageRole, parts: Vec<ContentPart>) -> Message {
        Message {
            role,
            content: MessageContent::Parts(parts),
        }
    }

    /// The misattribution bug, pinned on the real path: a parts message leaves
    /// under its MESSAGE's role, never under a hardcoded assistant. A user
    /// group is `user`, an assistant group stays `assistant`, and a tool
    /// result keeps its `tool` routing — three roles varying in one request,
    /// so no single hardcoded role can pass.
    #[test]
    fn parts_messages_leave_under_their_own_role() {
        let actual = convert(&[
            parts(
                MessageRole::User,
                vec![ContentPart::Text {
                    text: "look at this".into(),
                }],
            ),
            parts(
                MessageRole::Assistant,
                vec![
                    ContentPart::Text {
                        text: "noted".into(),
                    },
                    ContentPart::ToolUse {
                        id: "c1".into(),
                        name: "search".into(),
                        input: json!({ "q": "x" }),
                    },
                ],
            ),
            parts(
                MessageRole::User,
                vec![ContentPart::ToolResult {
                    tool_use_id: "c1".into(),
                    content: "found".into(),
                }],
            ),
        ]);

        let expected = json!([
            { "role": "user", "content": "look at this" },
            {
                "role": "assistant",
                "content": "noted",
                "tool_calls": [{
                    "id": "c1",
                    "type": "function",
                    "function": { "name": "search", "arguments": "{\"q\":\"x\"}" }
                }]
            },
            { "role": "tool", "content": "found", "tool_call_id": "c1" },
        ]);
        assert_eq!(actual, expected);
    }

    /// A user message carrying a caption and an image serializes to a `user`
    /// message whose content is the typed chunks — the caption text part and
    /// the `image_url` data URI, in the group's own order — and the URI's MIME
    /// and base64 round-trip the exact bytes.
    #[test]
    fn user_image_rides_the_chunks_content_as_a_data_uri() {
        let bytes: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0xFF];
        let actual = convert(&[parts(
            MessageRole::User,
            vec![
                ContentPart::Text {
                    text: "what is this?".into(),
                },
                ContentPart::Image {
                    mime: "image/png".into(),
                    data: bytes.clone(),
                },
            ],
        )]);

        let encoded = BASE64.encode(&bytes);
        let expected = json!([{
            "role": "user",
            "content": [
                { "type": "text", "text": "what is this?" },
                {
                    "type": "image_url",
                    "image_url": { "url": format!("data:image/png;base64,{encoded}") }
                },
            ],
        }]);
        assert_eq!(actual, expected);

        // The data URI round-trips the bytes, not merely a shape.
        let url = actual[0]["content"][1]["image_url"]["url"]
            .as_str()
            .expect("the URI is a string");
        let (head, b64) = url.split_once(";base64,").expect("a base64 data URI");
        assert_eq!(head, "data:image/png");
        assert_eq!(BASE64.decode(b64).expect("the payload decodes"), bytes);
    }

    /// A caption-less image emits no empty text chunk. A photo sent with no
    /// caption is the common case, and an empty `{"type":"text","text":""}`
    /// part is rejected by some OpenAI-compatible gateways — so the content is
    /// the image chunk alone, not a text chunk carrying the empty string.
    #[test]
    fn a_caption_less_image_emits_only_the_image_chunk() {
        let bytes: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x00, 0xFF];
        let actual = convert(&[parts(
            MessageRole::User,
            vec![
                ContentPart::Text {
                    text: String::new(),
                },
                ContentPart::Image {
                    mime: "image/png".into(),
                    data: bytes.clone(),
                },
            ],
        )]);

        let encoded = BASE64.encode(&bytes);
        let expected = json!([{
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": { "url": format!("data:image/png;base64,{encoded}") }
                },
            ],
        }]);
        assert_eq!(
            actual, expected,
            "the empty caption contributes no chunk; only the image rides"
        );
    }

    /// The shipped media variant set is `Image` alone. Inline audio was
    /// verified silently dropped by the gateway for the model this wire
    /// serves, so no `Audio` variant exists — the match here is exhaustive on
    /// purpose, so a later media variant arrives as a compile error in a pin
    /// that names the decision, a deliberate change rather than an accident.
    #[test]
    fn the_media_variant_set_is_image_only() {
        let tag = |part: &ContentPart| match part {
            ContentPart::Text { .. } => "text",
            ContentPart::Reasoning { .. } => "reasoning",
            ContentPart::ToolUse { .. } => "tool_use",
            ContentPart::ToolResult { .. } => "tool_result",
            ContentPart::Image { .. } => "image",
        };
        assert_eq!(
            tag(&ContentPart::Image {
                mime: "image/png".into(),
                data: vec![],
            }),
            "image"
        );
    }
}

/// The usage opt-in, and the end of turn it defers.
mod usage_tests {
    use super::*;

    fn drain(state: &mut SseState) -> Vec<StreamEvent> {
        finish_stream(state)
            .into_iter()
            .map(|r| r.expect("the terminal drain is clean"))
            .collect()
    }

    fn request(reasoning: Option<ReasoningLevel>) -> CompletionRequest {
        CompletionRequest {
            model: "test-model".into(),
            messages: vec![],
            tools: vec![],
            max_tokens: None,
            temperature: None,
            stream: true,
            reasoning,
        }
    }

    /// Every streaming request opts into the terminal counts; a non-streaming
    /// body carries no streaming options at all.
    #[test]
    fn streaming_request_sends_include_usage() {
        let provider = plain_provider();
        let streaming =
            serde_json::to_value(provider.build_request_body(&request(None), true, true).0)
                .expect("the request serializes");
        assert_eq!(
            streaming["stream_options"],
            json!({ "include_usage": true })
        );

        let blocking =
            serde_json::to_value(provider.build_request_body(&request(None), false, true).0)
                .expect("the request serializes");
        assert!(blocking.get("stream_options").is_none());
    }

    /// The finish defers the end of turn; the empty-choices chunk releases it
    /// carrying real counts. The end-of-stream line afterwards releases nothing
    /// twice.
    #[test]
    fn usage_chunk_releases_deferred_message_end() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "content": "hi" } }] }),
            &mut state,
        );

        let finish = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }], "usage": null }),
            &mut state,
        );
        assert!(
            finish.is_empty(),
            "the end of turn is deferred past the finish"
        );

        let events = parse(
            json!({ "choices": [], "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 34,
                "completion_tokens_details": { "reasoning_tokens": 7 }
            } }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [StreamEvent::MessageEnd { usage, stop_reason: StopReason::EndTurn }]
                if usage.input_tokens == 12
                    && usage.output_tokens == 34
                    && usage.reasoning_tokens == Some(7)
        ));

        assert!(
            drain(&mut state).is_empty(),
            "no second end at the end-of-stream line"
        );
    }

    /// The terminal order is pinned across the deferral: the end of turn, then
    /// the buffered tool events, then the tool close. An absent details object
    /// yields no reasoning count, never a zero.
    #[test]
    fn tool_event_order_preserved_across_deferral() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "tool_calls": [
                { "id": "call_1", "function": { "name": "search", "arguments": "" } }
            ] } }] }),
            &mut state,
        );
        parse(
            json!({ "choices": [{ "delta": { "tool_calls": [
                { "function": { "arguments": "{\"q\":1}" } }
            ] } }] }),
            &mut state,
        );

        let finish = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "tool_calls" }] }),
            &mut state,
        );
        assert!(
            finish.is_empty(),
            "tool events stay buffered until the end of turn releases"
        );

        let events = parse(
            json!({ "choices": [], "usage": { "prompt_tokens": 1, "completion_tokens": 2 } }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [
                StreamEvent::MessageEnd { usage, stop_reason: StopReason::ToolUse },
                StreamEvent::ToolUseStart { id, name },
                StreamEvent::ToolUseInputDelta { json },
                StreamEvent::ToolUseEnd,
            ] if usage.input_tokens == 1
                && usage.reasoning_tokens.is_none()
                && id == "call_1"
                && name == "search"
                && json == "{\"q\":1}"
        ));
    }

    /// A complete buffered call finishing under `stop` — the aggregator shape,
    /// where an endpoint reports tool calls but not the `tool_calls` finish
    /// reason — is still released as a full lifecycle, terminal close included.
    /// The close follows from the drained calls, never from the stop reason:
    /// without it the buffered call reaches the reader as an open lifecycle
    /// that no event ever finalizes, so it never executes.
    #[test]
    fn stop_finish_releases_buffered_tool_calls_with_their_close() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "tool_calls": [
                { "id": "call_1", "function": { "name": "search", "arguments": "{\"q\":1}" } }
            ] } }] }),
            &mut state,
        );
        parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            &mut state,
        );

        let events = parse(
            json!({ "choices": [], "usage": { "prompt_tokens": 1, "completion_tokens": 2 } }),
            &mut state,
        );
        assert!(
            matches!(
                events.as_slice(),
                [
                    StreamEvent::MessageEnd { stop_reason: StopReason::EndTurn, .. },
                    StreamEvent::ToolUseStart { id, name },
                    StreamEvent::ToolUseInputDelta { json },
                    StreamEvent::ToolUseEnd,
                ] if id == "call_1" && name == "search" && json == "{\"q\":1}"
            ),
            "the stop finish releases the complete lifecycle: {events:?}"
        );
    }

    /// Parallel calls: N buffered start-and-arguments pairs are released by a
    /// SINGLE terminal close. That cardinality is what a multi-call finalizer
    /// relies on to tell a sibling call from a duplicate.
    #[test]
    fn parallel_tool_calls_share_one_terminal_end() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "tool_calls": [
                { "index": 0, "id": "call_a", "function": { "name": "search", "arguments": "{\"q\":1}" } }
            ] } }] }),
            &mut state,
        );
        parse(
            json!({ "choices": [{ "delta": { "tool_calls": [
                { "index": 1, "id": "call_b", "function": { "name": "fetch", "arguments": "{\"u\":2}" } }
            ] } }] }),
            &mut state,
        );
        parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "tool_calls" }] }),
            &mut state,
        );

        let events = parse(
            json!({ "choices": [], "usage": { "prompt_tokens": 1, "completion_tokens": 2 } }),
            &mut state,
        );
        assert!(
            matches!(
                events.as_slice(),
                [
                    StreamEvent::MessageEnd { stop_reason: StopReason::ToolUse, .. },
                    StreamEvent::ToolUseStart { id: a_id, .. },
                    StreamEvent::ToolUseInputDelta { .. },
                    StreamEvent::ToolUseStart { id: b_id, .. },
                    StreamEvent::ToolUseInputDelta { .. },
                    StreamEvent::ToolUseEnd,
                ] if a_id == "call_a" && b_id == "call_b"
            ),
            "two starts, one terminal close: {events:?}"
        );
    }

    /// Two calls on a stream that never sends an index stay two calls. The
    /// index fix originally keyed every index-less fragment to the most recent
    /// call, which collapsed two distinct index-less calls into one and
    /// concatenated their argument JSON into garbage — a regression on exactly
    /// the vendor path (the shared decoder's second consumer) whose wire sends
    /// no index at all. Identity without an index comes from the fragment's
    /// shape: id or name opens a call, a bare argument fragment extends one.
    #[test]
    fn index_less_calls_stay_distinct() {
        let mut state = SseState::default();
        for fragment in [
            json!({ "id": "call_a", "function": { "name": "one", "arguments": "" } }),
            json!({ "function": { "arguments": "{\"x\":0}" } }),
            json!({ "id": "call_b", "function": { "name": "two", "arguments": "" } }),
            json!({ "function": { "arguments": "{\"y\":1}" } }),
        ] {
            parse(
                json!({ "choices": [{ "delta": { "tool_calls": [fragment] } }] }),
                &mut state,
            );
        }
        parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "tool_calls" }] }),
            &mut state,
        );
        let events = parse(
            json!({ "choices": [], "usage": { "prompt_tokens": 1, "completion_tokens": 2 } }),
            &mut state,
        );
        let [
            StreamEvent::MessageEnd { .. },
            StreamEvent::ToolUseStart {
                id: a_id,
                name: a_name,
            },
            StreamEvent::ToolUseInputDelta { json: a_args },
            StreamEvent::ToolUseStart {
                id: b_id,
                name: b_name,
            },
            StreamEvent::ToolUseInputDelta { json: b_args },
            StreamEvent::ToolUseEnd,
        ] = events.as_slice()
        else {
            panic!("expected two distinct calls, got {events:?}");
        };
        assert_eq!(
            (a_id.as_str(), a_name.as_str(), a_args.as_str()),
            ("call_a", "one", "{\"x\":0}")
        );
        assert_eq!(
            (b_id.as_str(), b_name.as_str(), b_args.as_str()),
            ("call_b", "two", "{\"y\":1}")
        );
    }

    /// Interleaved fragments belong to the call their INDEX names, not to
    /// whichever call spoke last. The wire streams two calls as index 0's name,
    /// index 1's name, index 0's arguments, index 1's arguments — an
    /// arrival-ordered buffer splices each call's arguments onto the other, and
    /// the model is recorded as having asked for something it never asked for.
    #[test]
    fn interleaved_fragments_stay_with_their_own_call() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "tool_calls": [
                { "index": 0, "id": "call_a", "function": { "name": "search", "arguments": "" } }
            ] } }] }),
            &mut state,
        );
        parse(
            json!({ "choices": [{ "delta": { "tool_calls": [
                { "index": 1, "id": "call_b", "function": { "name": "fetch", "arguments": "" } }
            ] } }] }),
            &mut state,
        );
        // From here the two calls' argument fragments alternate.
        for (index, fragment) in [
            (0, "{\"q\":"),
            (1, "{\"u\":"),
            (0, "\"rust\"}"),
            (1, "\"https://x\"}"),
        ] {
            parse(
                json!({ "choices": [{ "delta": { "tool_calls": [
                    { "index": index, "function": { "arguments": fragment } }
                ] } }] }),
                &mut state,
            );
        }
        parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "tool_calls" }] }),
            &mut state,
        );

        let events = parse(
            json!({ "choices": [], "usage": { "prompt_tokens": 1, "completion_tokens": 2 } }),
            &mut state,
        );
        let [
            StreamEvent::MessageEnd { .. },
            StreamEvent::ToolUseStart {
                id: a_id,
                name: a_name,
            },
            StreamEvent::ToolUseInputDelta { json: a_args },
            StreamEvent::ToolUseStart {
                id: b_id,
                name: b_name,
            },
            StreamEvent::ToolUseInputDelta { json: b_args },
            StreamEvent::ToolUseEnd,
        ] = events.as_slice()
        else {
            panic!("expected two complete calls and one close, got {events:?}");
        };
        assert_eq!((a_id.as_str(), a_name.as_str()), ("call_a", "search"));
        assert_eq!(a_args, "{\"q\":\"rust\"}", "the first call's own arguments");
        assert_eq!((b_id.as_str(), b_name.as_str()), ("call_b", "fetch"));
        assert_eq!(
            b_args, "{\"u\":\"https://x\"}",
            "the second call's own arguments"
        );
    }

    /// A vendor that never sends the counts chunk: the end-of-stream line still
    /// releases the end of turn, with zeroed counts. Never a hang.
    #[test]
    fn done_without_usage_chunk_still_terminates() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "content": "hi" } }] }),
            &mut state,
        );
        let finish = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            &mut state,
        );
        assert!(finish.is_empty());

        let done = drain(&mut state);
        assert!(matches!(
            done.as_slice(),
            [StreamEvent::MessageEnd { usage, stop_reason: StopReason::EndTurn }]
                if usage.input_tokens == 0
                    && usage.output_tokens == 0
                    && usage.reasoning_tokens.is_none()
        ));
    }

    /// A transport drop after a deferred finish: the natural end of the event
    /// stream invokes the same terminal drain, so a completed turn's content is
    /// committed rather than stranded as an uncommitted streaming block.
    #[test]
    fn transport_end_without_done_releases_deferred_message_end() {
        let mut state = SseState::default();
        parse(
            json!({ "choices": [{ "delta": { "content": "hi" } }] }),
            &mut state,
        );
        let finish = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
            &mut state,
        );
        assert!(finish.is_empty(), "deferred past the finish");

        let drained = drain(&mut state);
        assert!(matches!(
            drained.as_slice(),
            [StreamEvent::MessageEnd {
                stop_reason: StopReason::EndTurn,
                ..
            }]
        ));

        // A stream that already released drains to nothing, so the wiring's
        // second call cannot double-emit.
        assert!(
            drain(&mut state).is_empty(),
            "no double end on transport end"
        );
    }

    /// A vendor that carries the counts on the finish chunk itself releases
    /// immediately — no wait for a chunk that will never come.
    #[test]
    fn usage_on_finish_chunk_releases_immediately() {
        let mut state = SseState::default();
        let events = parse(
            json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }], "usage": {
                "prompt_tokens": 5, "completion_tokens": 9
            } }),
            &mut state,
        );
        assert!(matches!(
            events.as_slice(),
            [StreamEvent::MessageEnd { usage, stop_reason: StopReason::EndTurn }]
                if usage.input_tokens == 5 && usage.output_tokens == 9
        ));
        assert!(
            drain(&mut state).is_empty(),
            "no second end at the end-of-stream line"
        );
    }
}
