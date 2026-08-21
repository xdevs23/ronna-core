//! Decoding one chat-completions stream, from the open response down to the
//! neutral events.
//!
//! Every vendor whose responses take this shape reads them here — including one
//! whose REQUESTS are built elsewhere, because the two directions are separate
//! questions and only the request side of that vendor genuinely differs.
//!
//! Two things here are load-bearing and neither is obvious from the wire.
//!
//! The **reasoning boundary**: a streaming reasoning block is finalized only by
//! an explicit end event, never by the arrival of text. Without exactly one end
//! at the reasoning-to-content boundary, the streaming block dangles and is
//! never persisted — the model appears not to have reasoned at all.
//!
//! The **deferred end of turn**: with the usage opt-in, the counts arrive one
//! chunk AFTER the finish reason, so the end of the turn waits for them. A turn
//! that ended at the finish reason would report zero tokens for every request on
//! this surface.

use eventsource_stream::Eventsource;
use futures::stream::{self, StreamExt};
use serde_json::Value;
use tracing::{debug, warn};

use crate::providers::types::{
    EventStream, LlmError, OpaquePayload, ReasoningDetailEntry, StopReason, StreamEvent, Usage,
};

/// The line a chat-completions stream ends on.
const SSE_DONE: &str = "[DONE]";

/// Vendor seam: decode one delta's content field into stream events.
///
/// The default handles the plain-string shape. A vendor whose content is
/// structured plugs its own decoder in, and a decoder owns the
/// reasoning-to-content boundary because only it knows where that boundary is
/// in its own shape.
pub(crate) type ContentDecoder = fn(&Value, &mut SseState) -> Vec<StreamEvent>;

/// Vendor seam: build the continuity payload for the end of a reasoning run.
///
/// The default drains whatever reasoning entries the stream carried. A vendor
/// whose payload is the stored reasoning text itself overrides with its own tag.
pub(crate) type ThinkingEndPayload = fn(&mut SseState) -> Option<OpaquePayload>;

/// One tool call being assembled from its streamed fragments.
///
/// The wire keys a call's fragments by INDEX, and interleaves them: two calls
/// stream as index 0's name, index 1's name, index 0's arguments, index 1's
/// arguments. A buffer that appended in arrival order spliced each call's
/// arguments onto the other one — a request the model never made, made anyway.
struct ToolCallInFlight {
    index: u64,
    id: String,
    name: String,
    arguments: String,
}

/// Cross-chunk decoding state, threaded through the whole stream.
pub(crate) struct SseState {
    /// Calls in flight, keyed by their wire index. They are assembled here and
    /// released as complete lifecycles after the deferred end of turn.
    tool_calls: Vec<ToolCallInFlight>,
    /// A streaming reasoning block is open and not yet finalized.
    pub(crate) reasoning_open: bool,
    /// Every reasoning entry streamed so far: all types, order-preserving,
    /// content fields optional. The payload capture drains them, and this stays
    /// empty for vendors that never send the field.
    reasoning_details: Vec<ReasoningDetailEntry>,
    /// The last summary entry index seen.
    ///
    /// Successive summary parts stream with NO separator on the wire — the
    /// section boundary is visible only as an index bump — so the joiner
    /// prefixes a blank line there. Without it a following bold section header
    /// glues onto the previous part's last line. This surface has a single
    /// reasoning stream per turn, so one index suffices.
    last_summary_index: Option<u64>,
    /// The stop reason captured at the finish, held until the counts arrive.
    pending_stop: Option<StopReason>,
    decoder: ContentDecoder,
    thinking_payload: ThinkingEndPayload,
}

impl Default for SseState {
    fn default() -> Self {
        Self::new(decode_string_content, default_thinking_end_payload)
    }
}

impl SseState {
    /// Fresh state for one stream, with a vendor's two decoding seams.
    pub(crate) fn new(decoder: ContentDecoder, thinking_payload: ThinkingEndPayload) -> Self {
        Self {
            tool_calls: Vec::new(),
            reasoning_open: false,
            reasoning_details: Vec::new(),
            last_summary_index: None,
            pending_stop: None,
            decoder,
            thinking_payload,
        }
    }

    /// Mark a streaming reasoning block open.
    pub(crate) fn open_reasoning(&mut self) {
        self.reasoning_open = true;
    }

    /// Close an open reasoning block, yielding the boundary end exactly once,
    /// carrying the vendor's captured continuity payload. Inert when no
    /// reasoning block is open, which is what makes "exactly once" hold no
    /// matter how many places call it.
    pub(crate) fn close_reasoning(&mut self) -> Option<StreamEvent> {
        if !self.reasoning_open {
            return None;
        }
        self.reasoning_open = false;
        let capture = self.thinking_payload;
        Some(StreamEvent::ThinkingEnd {
            opaque: capture(self),
        })
    }

    /// Capture one streamed reasoning entry in relational form: the position
    /// preserves array order for a verbatim rebuild, and the content field
    /// holds whichever of the three fields this entry type carries.
    fn capture_reasoning_detail(&mut self, entry: &Value) {
        let entry_type = entry["type"].as_str().unwrap_or_default().to_string();
        let content = match entry_type.as_str() {
            "reasoning.summary" => entry["summary"].as_str(),
            "reasoning.encrypted" => entry["data"].as_str(),
            _ => entry["text"].as_str(),
        }
        .unwrap_or_default()
        .to_string();
        self.reasoning_details.push(ReasoningDetailEntry {
            position: u32::try_from(self.reasoning_details.len()).unwrap_or(u32::MAX),
            entry_type,
            entry_id: entry["id"].as_str().map(str::to_string),
            upstream_format: entry["format"].as_str().unwrap_or("unknown").to_string(),
            index: entry["index"].as_u64().and_then(|i| u32::try_from(i).ok()),
            content,
            signature: entry["signature"].as_str().map(str::to_string),
        });
    }
}

/// The default payload capture: drain the streamed reasoning entries.
///
/// `None` when the stream never carried the field — a plain chat-completions
/// endpoint has no echo mechanism at all, and inventing an empty payload would
/// make the next turn claim continuity it does not have.
pub(crate) fn default_thinking_end_payload(state: &mut SseState) -> Option<OpaquePayload> {
    if state.reasoning_details.is_empty() {
        return None;
    }
    Some(OpaquePayload::OpenRouter {
        entries: state.reasoning_details.drain(..).collect(),
    })
}

/// Join successive summary parts: prefix a blank line when the index bumps to a
/// new section. The first delta records its index without a prefix.
fn joined_summary_delta(state: &mut SseState, index: Option<u64>, text: &str) -> String {
    let index = index.unwrap_or(0);
    match state.last_summary_index.replace(index) {
        Some(last) if last != index => format!("\n\n{text}"),
        _ => text.to_string(),
    }
}

/// The reasoning ingest probe, routing each entry's DISPLAY text by its
/// structural type. The two channels are distinct on purpose:
///
/// - a summary entry is the lossy, display-only channel, section-joined on its
///   index;
/// - a text entry is verbatim reasoning and carries a signature;
/// - an encrypted entry has NO display text at all — it is continuity only.
///
/// Probe precedence: the typed array wins outright when present, then the two
/// bare string fields, which are the verbatim-reasoning vocabularies of vendors
/// that publish no array. The probe is inert for a vendor that sends none of
/// them, which is why it can run unconditionally instead of forking the parser
/// per vendor.
///
/// Every entry is CAPTURED unchanged — metadata-only and encrypted ones
/// included — using the SAME type read the routing does, so the closing payload
/// carries the full, byte-identical echo. Routing only chooses which event
/// carries the display text.
fn ingest_reasoning(delta: &Value, state: &mut SseState) -> Vec<StreamEvent> {
    if let Some(details) = delta["reasoning_details"].as_array() {
        let mut events = Vec::new();
        for entry in details {
            match entry["type"].as_str().unwrap_or_default() {
                "reasoning.summary" => {
                    if let Some(text) = entry["summary"].as_str().filter(|t| !t.is_empty()) {
                        let text = joined_summary_delta(state, entry["index"].as_u64(), text);
                        events.push(StreamEvent::ThinkingSummaryDelta { text });
                    }
                }
                "reasoning.encrypted" => {}
                _ => {
                    if let Some(text) = entry["text"].as_str().filter(|t| !t.is_empty()) {
                        events.push(StreamEvent::ThinkingDelta {
                            text: text.to_string(),
                        });
                    }
                }
            }
            state.capture_reasoning_detail(entry);
        }
        return events;
    }
    if let Some(content) = delta["reasoning_content"].as_str() {
        return if content.is_empty() {
            vec![]
        } else {
            vec![StreamEvent::ThinkingDelta {
                text: content.to_string(),
            }]
        };
    }
    match delta["reasoning"].as_str() {
        Some(text) if !text.is_empty() => vec![StreamEvent::ThinkingDelta {
            text: text.to_string(),
        }],
        _ => vec![],
    }
}

/// The default content decoder: a plain string. A reasoning-to-content boundary
/// finalizes the open reasoning block first.
pub(crate) fn decode_string_content(content: &Value, state: &mut SseState) -> Vec<StreamEvent> {
    let Some(text) = content.as_str() else {
        return vec![];
    };
    if text.is_empty() {
        return vec![];
    }
    let mut events = vec![];
    events.extend(state.close_reasoning());
    events.push(StreamEvent::TextDelta {
        text: text.to_string(),
    });
    events
}

/// Fold one fragment into the call it belongs to.
///
/// A fragment carries the identity fields only on the call's first chunk and
/// arguments on the rest, so each field is taken where it appears and never
/// overwritten by a later blank. With an index present, the index is the
/// identity. Without one, the wire gives no key, so identity comes from the
/// fragment's shape: a fragment carrying an id or a name is a call's FIRST
/// chunk and opens a new call, while a bare argument fragment extends the most
/// recent one. Keying every index-less fragment to the most recent call
/// collapsed two distinct index-less calls into one, concatenating their
/// argument JSON into garbage — the same corruption the index key exists to
/// prevent, reintroduced on the path without an index.
fn ingest_tool_call_fragment(state: &mut SseState, fragment: &Value) {
    let carries_identity = fragment["id"].as_str().is_some_and(|s| !s.is_empty())
        || fragment["function"]["name"]
            .as_str()
            .is_some_and(|s| !s.is_empty());
    let index = fragment["index"].as_u64().unwrap_or_else(|| {
        let last = state.tool_calls.last().map_or(0, |call| call.index);
        if carries_identity && !state.tool_calls.is_empty() {
            last + 1
        } else {
            last
        }
    });

    if !state.tool_calls.iter().any(|call| call.index == index) {
        state.tool_calls.push(ToolCallInFlight {
            index,
            id: String::new(),
            name: String::new(),
            arguments: String::new(),
        });
    }
    let call = state
        .tool_calls
        .iter_mut()
        .find(|call| call.index == index)
        .expect("the call for this index exists, having just been ensured");

    if let Some(id) = fragment["id"].as_str().filter(|s| !s.is_empty()) {
        call.id = id.to_string();
    }
    if let Some(name) = fragment["function"]["name"]
        .as_str()
        .filter(|s| !s.is_empty())
    {
        call.name = name.to_string();
    }
    if let Some(args) = fragment["function"]["arguments"].as_str() {
        call.arguments.push_str(args);
    }
}

/// Read a terminal usage object defensively: an absent or null object is no
/// counts at all, absent fields are zero, and reasoning tokens stay optional —
/// absent means absent, never a fabricated zero.
fn parse_usage(value: &Value) -> Option<Usage> {
    let usage = value.as_object()?;
    let count = |field: &str| {
        usage
            .get(field)
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0)
    };
    Some(Usage {
        input_tokens: count("prompt_tokens"),
        output_tokens: count("completion_tokens"),
        reasoning_tokens: usage
            .get("completion_tokens_details")
            .and_then(|details| details["reasoning_tokens"].as_u64())
            .and_then(|n| u32::try_from(n).ok()),
    })
}

/// Drain the calls in flight as complete lifecycles, each one's arguments its
/// own, in the order the wire numbered them, closed by the SINGLE terminal
/// `ToolUseEnd` that covers all of them.
///
/// The close rides on the calls themselves, never on the stop reason: a call
/// buffered here is complete by the time the turn finishes, and an endpoint
/// that finishes with `stop` while carrying complete calls — an aggregator
/// shape — still owes its reader the terminal close, or every buffered call is
/// an orphaned lifecycle that never executes.
fn drain_tool_calls(state: &mut SseState) -> Vec<StreamEvent> {
    let mut calls: Vec<ToolCallInFlight> = state.tool_calls.drain(..).collect();
    calls.sort_by_key(|call| call.index);

    let mut events = Vec::new();
    for call in calls {
        events.push(StreamEvent::ToolUseStart {
            id: call.id,
            name: call.name,
        });
        if !call.arguments.is_empty() {
            events.push(StreamEvent::ToolUseInputDelta {
                json: call.arguments,
            });
        }
    }
    if !events.is_empty() {
        events.push(StreamEvent::ToolUseEnd);
    }
    events
}

/// Release the deferred end of turn with the given counts, preserving the
/// terminal order: the end, then the assembled tool calls with their single
/// terminal close.
///
/// Inert while no finish reason has been captured, so a stray usage chunk
/// cannot conjure an end of turn and a double release is impossible.
fn release_message_end(state: &mut SseState, usage: Usage) -> Vec<Result<StreamEvent, LlmError>> {
    let Some(stop_reason) = state.pending_stop.take() else {
        return vec![];
    };
    let mut events = vec![Ok(StreamEvent::MessageEnd { usage, stop_reason })];
    events.extend(drain_tool_calls(state).into_iter().map(Ok));
    events
}

/// The terminal drain.
///
/// A vendor that never sent counts still gets its end of turn here, with zeroed
/// counts: the stream terminates rather than hanging, which is the trade — a
/// missing number costs a statistic, a missing end costs the whole answer.
///
/// A reasoning block still open at the end closes too: a stream that stopped
/// speaking mid-thought would otherwise leave the block dangling, never
/// finalized and never persisted.
pub(crate) fn finish_stream(state: &mut SseState) -> Vec<Result<StreamEvent, LlmError>> {
    let mut events: Vec<Result<StreamEvent, LlmError>> =
        state.close_reasoning().map(Ok).into_iter().collect();
    events.extend(release_message_end(state, Usage::default()));
    events.extend(drain_tool_calls(state).into_iter().map(Ok));
    events
}

/// Decode one chunk.
pub(crate) fn parse_sse_chunk(
    data: &str,
    state: &mut SseState,
) -> Vec<Result<StreamEvent, LlmError>> {
    let Ok(value) = serde_json::from_str::<Value>(data) else {
        return vec![];
    };

    // With the usage opt-in, the terminal counts arrive AFTER the finish chunk,
    // in a chunk with counts and EMPTY choices. Recognise it and release the
    // deferred end rather than dropping it as unparseable.
    let Some(choice) = value["choices"].get(0) else {
        return match parse_usage(&value["usage"]) {
            Some(usage) => release_message_end(state, usage),
            None => vec![],
        };
    };
    let delta = &choice["delta"];

    let mut events = vec![];

    // Reasoning precedes content. EITHER reasoning channel opens the block:
    // whoever writes the stream down creates it lazily on the first delta of
    // either.
    let reasoning_events = ingest_reasoning(delta, state);
    if !reasoning_events.is_empty() {
        state.open_reasoning();
        events.extend(reasoning_events.into_iter().map(Ok));
    }

    // Content, through the vendor's decoder, which owns the boundary.
    let decode = state.decoder;
    events.extend(decode(&delta["content"], state).into_iter().map(Ok));

    // Tool calls accumulate per index until the end of turn. Their arrival also
    // ends reasoning.
    if let Some(tool_calls) = delta["tool_calls"].as_array() {
        events.extend(state.close_reasoning().map(Ok));
        for tc in tool_calls {
            debug!(tool_call = ?tc, "chat tool call chunk");
            ingest_tool_call_fragment(state, tc);
        }
    }

    // The finish: close reasoning and capture the stop reason, but DEFER the
    // end of turn until the counts arrive. A vendor that carries counts on the
    // finish chunk itself releases immediately.
    if let Some(reason) = choice["finish_reason"].as_str() {
        let stop_reason = match reason {
            "tool_calls" => StopReason::ToolUse,
            "length" => StopReason::MaxTokens,
            _ => StopReason::EndTurn,
        };

        events.extend(state.close_reasoning().map(Ok));
        state.pending_stop = Some(stop_reason);

        if let Some(usage) = parse_usage(&value["usage"]) {
            events.extend(release_message_end(state, usage));
        }
    }

    events
}

/// Test support: the aggregator's finish-reason-`stop` tool-call turn as RAW
/// wire chunks, decoded through the real decoder into neutral events.
///
/// Lives here because the isolation rule allows only a vendor module to name
/// this wire's field names — the joined ingestion test consumes what the
/// translator ACTUALLY emits for this shape, so a decoder that stopped
/// releasing the buffered lifecycle (or dropped its terminal close) fails that
/// test instead of hiding behind a hand-written fixture.
#[cfg(test)]
pub(crate) fn decoded_stop_finish_tool_call_turn() -> Vec<StreamEvent> {
    let chunks = [
        serde_json::json!({ "choices": [{ "delta": { "tool_calls": [
            { "id": "agg-1", "function": { "name": "read_file", "arguments": "{}" } }
        ] } }] }),
        serde_json::json!({ "choices": [{ "delta": {}, "finish_reason": "stop" }] }),
        serde_json::json!({ "choices": [], "usage": {
            "prompt_tokens": 1, "completion_tokens": 2
        } }),
    ];
    let mut state = SseState::default();
    chunks
        .iter()
        .flat_map(|chunk| parse_sse_chunk(&chunk.to_string(), &mut state))
        .map(|event| event.expect("the chunk decodes cleanly"))
        .collect()
}

/// Read one open response as this surface's event stream, decoded into neutral
/// events.
///
/// Every vendor whose responses take this shape reads them here, whatever its
/// requests look like: the end-of-stream line, the terminal drain and the
/// stall-free close are one behavior, and a vendor that reimplemented them got
/// a different subset of them right.
///
/// The transport ending WITHOUT the end-of-stream line drains just the same. A
/// turn that completed and then lost its connection before the sentinel would
/// otherwise never end: no close, no counts, and a streaming block left
/// uncommitted.
pub(crate) fn decode_stream(response: reqwest::Response, state: SseState) -> EventStream {
    let source = Box::pin(response.bytes_stream().eventsource());

    Box::pin(
        stream::unfold(
            (source, state, false),
            |(mut source, mut state, terminated)| async move {
                if terminated {
                    return None;
                }
                let events = match source.next().await {
                    Some(Ok(event)) if event.data == SSE_DONE => finish_stream(&mut state),
                    Some(Ok(event)) => parse_sse_chunk(&event.data, &mut state),
                    Some(Err(e)) => {
                        warn!("SSE stream error: {e}");
                        vec![Err(LlmError::Stream(e.to_string()))]
                    }
                    // The drain is inert if the end-of-stream line already
                    // released the turn, so this cannot double-emit.
                    None => {
                        let drained = finish_stream(&mut state);
                        return (!drained.is_empty()).then_some((drained, (source, state, true)));
                    }
                };
                Some((events, (source, state, terminated)))
            },
        )
        .flat_map(stream::iter),
    )
}
