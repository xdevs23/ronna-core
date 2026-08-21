//! Decoding one Responses stream.
//!
//! This API emits *items* — messages, function calls, reasoning — whose
//! lifecycles may interleave on the wire. Whoever writes the stream down tracks
//! one open block at a time, so this parser SERIALIZES those lifecycles: a tool
//! call's events are buffered per item and emitted as one contiguous run after
//! the turn ends, and a reasoning item streams live only while it holds the
//! single open slot, buffering otherwise. Interleaved runs would be written down
//! as one block containing two models' worth of reasoning.

use serde_json::Value;
use tracing::warn;

use crate::providers::types::{LlmError, OpaquePayload, StopReason, StreamEvent, Usage};

/// Cross-event decoding state.
#[derive(Default)]
pub(super) struct ResponsesSseState {
    /// Calls in flight, keyed by item id, in arrival order.
    calls: Vec<CallInFlight>,
    /// Fully-serialized tool lifecycles, drained after the turn ends.
    tool_events: Vec<StreamEvent>,
    /// True once any call completed, which is what makes the turn a tool turn.
    saw_function_call: bool,
    /// The reasoning item currently streaming live.
    open_reasoning: Option<String>,
    /// Reasoning deltas that arrived for an item other than the open one,
    /// buffered per item until they can emit as a contiguous run.
    pub(super) deferred_reasoning: Vec<DeferredReasoning>,
    /// The last summary index seen per reasoning item.
    ///
    /// Successive summary parts stream with no separator, so the section
    /// boundary is visible only as an index bump, and the joiner prefixes a
    /// blank line there — otherwise a whole-line bold section title glues onto
    /// the previous part's last line.
    summary_positions: Vec<(String, u64)>,
}

struct CallInFlight {
    item_id: String,
    call_id: String,
    name: String,
    /// Arguments accumulated from the streamed fragments.
    accumulated: String,
    /// The full arguments string from the call's own completion event, held as
    /// an integrity check against the accumulated fragments.
    final_args: Option<String>,
}

pub(super) struct DeferredReasoning {
    item_id: String,
    /// Buffered delta events, each already carrying its channel, so deferral
    /// never conflates verbatim reasoning with a summary.
    pub(super) deltas: Vec<StreamEvent>,
    /// The item's completion arrived while another item held the slot; the full
    /// run is flushed once the slot frees.
    done: bool,
    /// The continuity payload captured from the item's completion, attached to
    /// the buffered run's end on flush.
    opaque: Option<OpaquePayload>,
}

/// The response status, matched exhaustively so a new one cannot be silently
/// mis-mapped onto an end of turn that did not happen.
enum ResponseStatus {
    Completed,
    Failed,
    InProgress,
    Cancelled,
    Queued,
    Incomplete,
}

impl ResponseStatus {
    fn parse(status: &str) -> Option<Self> {
        Some(match status {
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "in_progress" => Self::InProgress,
            "cancelled" => Self::Cancelled,
            "queued" => Self::Queued,
            "incomplete" => Self::Incomplete,
            _ => return None,
        })
    }
}

pub(super) fn parse_responses_event(
    data: &str,
    state: &mut ResponsesSseState,
) -> Vec<Result<StreamEvent, LlmError>> {
    let Ok(value) = serde_json::from_str::<Value>(data) else {
        return vec![];
    };

    match value["type"].as_str().unwrap_or_default() {
        "response.output_text.delta" => match value["delta"].as_str() {
            Some(text) if !text.is_empty() => vec![Ok(StreamEvent::TextDelta {
                text: text.to_string(),
            })],
            _ => vec![],
        },

        // The two reasoning delta families are DISTINCT channels: one is the
        // lossy display-only summary, the other is verbatim reasoning.
        // Conflating them put summaries into block content, where they were
        // indistinguishable from the model's own words.
        "response.reasoning_summary_text.delta" => {
            let item_id = value["item_id"].as_str().unwrap_or_default();
            match value["delta"].as_str() {
                Some(text) if !text.is_empty() => {
                    let text =
                        joined_summary_delta(state, item_id, value["summary_index"].as_u64(), text);
                    reasoning_delta(state, item_id, StreamEvent::ThinkingSummaryDelta { text })
                }
                _ => vec![],
            }
        }
        "response.reasoning_text.delta" => {
            let item_id = value["item_id"].as_str().unwrap_or_default();
            match value["delta"].as_str() {
                Some(text) if !text.is_empty() => reasoning_delta(
                    state,
                    item_id,
                    StreamEvent::ThinkingDelta {
                        text: text.to_string(),
                    },
                ),
                _ => vec![],
            }
        }

        "response.output_item.added" => {
            let item = &value["item"];
            if item["type"].as_str() == Some("function_call") {
                // The call identifier is what the later output must reference;
                // the item id is what keys the argument fragments. They are
                // different strings and swapping them breaks the pairing.
                state.calls.push(CallInFlight {
                    item_id: item["id"].as_str().unwrap_or_default().to_string(),
                    call_id: item["call_id"].as_str().unwrap_or_default().to_string(),
                    name: item["name"].as_str().unwrap_or_default().to_string(),
                    accumulated: item["arguments"].as_str().unwrap_or_default().to_string(),
                    final_args: None,
                });
            }
            // A reasoning item's arrival emits no start: the block is created
            // lazily on its first delta.
            vec![]
        }

        "response.function_call_arguments.delta" => {
            let item_id = value["item_id"].as_str().unwrap_or_default();
            if let Some(call) = state.calls.iter_mut().find(|c| c.item_id == item_id) {
                if let Some(delta) = value["delta"].as_str() {
                    call.accumulated.push_str(delta);
                }
            } else {
                warn!(item_id, "argument fragment for an unknown call");
            }
            vec![]
        }

        "response.function_call_arguments.done" => {
            let item_id = value["item_id"].as_str().unwrap_or_default();
            if let Some(call) = state.calls.iter_mut().find(|c| c.item_id == item_id) {
                call.final_args = value["arguments"].as_str().map(str::to_string);
            }
            vec![]
        }

        "response.output_item.done" => {
            let item = &value["item"];
            match item["type"].as_str() {
                Some("function_call") => {
                    finalize_function_call(state, item);
                    vec![]
                }
                // A reasoning item's completion is the ONLY event carrying its
                // encrypted content and id, so it is the one capture point.
                Some("reasoning") => reasoning_done(state, item),
                _ => vec![],
            }
        }

        "response.completed" | "response.incomplete" | "response.failed" => {
            terminal_event(&value, state)
        }

        // Everything else is bookkeeping that carries no information the neutral
        // events need.
        _ => vec![],
    }
}

/// A call's completion is the authoritative lifecycle close: emit the contiguous
/// run into the tool buffer, with the item's full arguments as the payload and
/// the accumulated fragments as a cross-check.
fn finalize_function_call(state: &mut ResponsesSseState, item: &Value) {
    let item_id = item["id"].as_str().unwrap_or_default();
    let call = if let Some(idx) = state.calls.iter().position(|c| c.item_id == item_id) {
        state.calls.remove(idx)
    } else {
        // No arrival was seen: build the lifecycle from the completion alone
        // rather than dropping a call the model actually made.
        warn!(
            item_id,
            "call completed without an arrival — using its own fields"
        );
        CallInFlight {
            item_id: item_id.to_string(),
            call_id: item["call_id"].as_str().unwrap_or_default().to_string(),
            name: item["name"].as_str().unwrap_or_default().to_string(),
            accumulated: String::new(),
            final_args: None,
        }
    };

    let authoritative = item["arguments"]
        .as_str()
        .map(str::to_string)
        .or(call.final_args)
        .unwrap_or_else(|| call.accumulated.clone());
    if !call.accumulated.is_empty() && call.accumulated != authoritative {
        warn!(
            item_id,
            "accumulated argument fragments disagree with the final arguments — using the final"
        );
    }

    state.tool_events.push(StreamEvent::ToolUseStart {
        id: call.call_id,
        name: call.name,
    });
    if !authoritative.is_empty() {
        state.tool_events.push(StreamEvent::ToolUseInputDelta {
            json: authoritative,
        });
    }
    state.tool_events.push(StreamEvent::ToolUseEnd);
    state.saw_function_call = true;
}

/// Join successive summary parts of one reasoning item: the first delta of a
/// part this item has not streamed yet gets a blank line prefixed.
fn joined_summary_delta(
    state: &mut ResponsesSseState,
    item_id: &str,
    summary_index: Option<u64>,
    text: &str,
) -> String {
    let index = summary_index.unwrap_or(0);
    let Some((_, last)) = state
        .summary_positions
        .iter_mut()
        .find(|(id, _)| id == item_id)
    else {
        state.summary_positions.push((item_id.to_string(), index));
        return text.to_string();
    };

    let part_boundary = *last != index;
    *last = index;
    if part_boundary {
        format!("\n\n{text}")
    } else {
        text.to_string()
    }
}

/// Route a reasoning delta — the channel is already baked into the event.
///
/// It streams live while its item holds the single open slot, and defers
/// otherwise, so each item's run stays contiguous.
fn reasoning_delta(
    state: &mut ResponsesSseState,
    item_id: &str,
    event: StreamEvent,
) -> Vec<Result<StreamEvent, LlmError>> {
    match state.open_reasoning.as_deref() {
        Some(open) if open == item_id => vec![Ok(event)],
        Some(_) => {
            defer_reasoning(state, item_id, event);
            vec![]
        }
        None => {
            // A delta that races in AFTER its own item's completion: the item is
            // already recorded closed. Append to its buffered run so it stays
            // one contiguous block rather than reopening the slot and splitting
            // off a spurious second block.
            if state
                .deferred_reasoning
                .iter()
                .any(|d| d.item_id == item_id && d.done)
            {
                defer_reasoning(state, item_id, event);
                return vec![];
            }
            // A fresh item — or one deferred while another held the slot but not
            // yet closed — takes the slot and pulls its buffered deltas along.
            state.open_reasoning = Some(item_id.to_string());
            let mut events: Vec<Result<StreamEvent, LlmError>> =
                take_deferred(state, item_id).into_iter().map(Ok).collect();
            events.push(Ok(event));
            events
        }
    }
}

fn defer_reasoning(state: &mut ResponsesSseState, item_id: &str, event: StreamEvent) {
    if let Some(entry) = state
        .deferred_reasoning
        .iter_mut()
        .find(|d| d.item_id == item_id)
    {
        entry.deltas.push(event);
    } else {
        state.deferred_reasoning.push(DeferredReasoning {
            item_id: item_id.to_string(),
            deltas: vec![event],
            done: false,
            opaque: None,
        });
    }
}

fn take_deferred(state: &mut ResponsesSseState, item_id: &str) -> Vec<StreamEvent> {
    if let Some(idx) = state
        .deferred_reasoning
        .iter()
        .position(|d| d.item_id == item_id)
    {
        state.deferred_reasoning.remove(idx).deltas
    } else {
        vec![]
    }
}

/// A reasoning item's completion ends the block and captures its continuity
/// payload: the server-assigned id plus the encrypted content, when present.
///
/// If the item holds the open slot, close it and flush any deferred items whose
/// lifecycles completed meanwhile. Otherwise mark it closed in the buffer so its
/// run flushes contiguously later — which also lets a delta that races in after
/// its own completion append to the item instead of reopening a new block.
fn reasoning_done(
    state: &mut ResponsesSseState,
    item: &Value,
) -> Vec<Result<StreamEvent, LlmError>> {
    let item_id = item["id"].as_str().unwrap_or_default();
    let opaque = item["encrypted_content"].as_str().map(|encrypted_content| {
        OpaquePayload::OpenAiResponses {
            item_id: item_id.to_string(),
            encrypted_content: encrypted_content.to_string(),
        }
    });

    match state.open_reasoning.as_deref() {
        Some(open) if open == item_id => {
            state.open_reasoning = None;
            let mut events = vec![Ok(StreamEvent::ThinkingEnd { opaque })];
            events.extend(flush_completed_deferred(state));
            events
        }
        _ => {
            match state
                .deferred_reasoning
                .iter_mut()
                .find(|d| d.item_id == item_id)
            {
                Some(entry) => {
                    entry.done = true;
                    entry.opaque = opaque;
                }
                None => state.deferred_reasoning.push(DeferredReasoning {
                    item_id: item_id.to_string(),
                    deltas: vec![],
                    done: true,
                    opaque,
                }),
            }
            vec![]
        }
    }
}

/// Drain deferred items whose completion already arrived, each as a contiguous
/// run carrying its captured payload.
fn flush_completed_deferred(state: &mut ResponsesSseState) -> Vec<Result<StreamEvent, LlmError>> {
    let mut events = vec![];
    while let Some(idx) = state.deferred_reasoning.iter().position(|d| d.done) {
        let entry = state.deferred_reasoning.remove(idx);
        events.extend(entry.deltas.into_iter().map(Ok));
        events.push(Ok(StreamEvent::ThinkingEnd {
            opaque: entry.opaque,
        }));
    }
    events
}

/// Close whatever reasoning is still in flight before the terminal event: the
/// open item ends with no payload, since no completion was seen for it, and
/// every deferred item flushes with whatever payload its completion captured. No
/// streamed reasoning is lost.
fn flush_all_reasoning(state: &mut ResponsesSseState) -> Vec<Result<StreamEvent, LlmError>> {
    let mut events = vec![];
    if state.open_reasoning.take().is_some() {
        events.push(Ok(StreamEvent::ThinkingEnd { opaque: None }));
    }
    for entry in state.deferred_reasoning.drain(..) {
        events.extend(entry.deltas.into_iter().map(Ok));
        events.push(Ok(StreamEvent::ThinkingEnd {
            opaque: entry.opaque,
        }));
    }
    events
}

/// The three terminal events. All share one envelope, and the status inside it
/// is matched across all six of its values.
fn terminal_event(
    value: &Value,
    state: &mut ResponsesSseState,
) -> Vec<Result<StreamEvent, LlmError>> {
    let response = &value["response"];
    let raw_status = response["status"].as_str().unwrap_or_default();
    let Some(status) = ResponseStatus::parse(raw_status) else {
        warn!(status = raw_status, "terminal event with an unknown status");
        return vec![];
    };

    match status {
        ResponseStatus::Completed => {
            let stop_reason = if state.saw_function_call {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            };
            finish(state, response, stop_reason)
        }
        ResponseStatus::Incomplete => {
            let stop_reason = match response["incomplete_details"]["reason"].as_str() {
                Some("max_output_tokens") => StopReason::MaxTokens,
                Some("content_filter") => StopReason::ContentFilter,
                other => {
                    warn!(reason = ?other, "incomplete response with an unknown reason");
                    StopReason::EndTurn
                }
            };
            finish(state, response, stop_reason)
        }
        ResponseStatus::Failed => {
            // A server verdict delivered inside a successful stream. Terminal:
            // reconnecting would re-run a request the server has already judged.
            let code = response["error"]["code"].as_str().unwrap_or("unknown");
            let message = response["error"]["message"]
                .as_str()
                .unwrap_or("response failed");
            vec![Err(LlmError::Api {
                status: 200,
                message: format!("{code}: {message}"),
            })]
        }
        ResponseStatus::Cancelled => vec![Err(LlmError::Api {
            status: 200,
            message: "response cancelled by server".into(),
        })],
        // A non-terminal status on a terminal-named event is a protocol
        // violation. Ignore it rather than fabricating an end of turn that
        // would commit a half-written answer.
        ResponseStatus::InProgress | ResponseStatus::Queued => {
            warn!(
                status = raw_status,
                "terminal event carrying a non-terminal status — ignored"
            );
            vec![]
        }
    }
}

/// The end-of-turn sequence: close any in-flight reasoning, then the end of
/// turn, then the buffered tool lifecycles — whoever writes the stream down
/// resets its trackers at the end of turn, so complete lifecycles must follow
/// it.
fn finish(
    state: &mut ResponsesSseState,
    response: &Value,
    stop_reason: StopReason,
) -> Vec<Result<StreamEvent, LlmError>> {
    let mut events = flush_all_reasoning(state);
    events.push(Ok(StreamEvent::MessageEnd {
        usage: parse_usage(response),
        stop_reason,
    }));
    events.extend(state.tool_events.drain(..).map(Ok));
    events
}

/// Counts from the terminal event, read defensively: they are null on a failure
/// and may be absent or partial on an incomplete response. The reasoning count
/// is present only when reasoning occurred, and absent means absent.
fn parse_usage(response: &Value) -> Usage {
    let usage = &response["usage"];
    let count = |value: &Value| {
        value
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0)
    };
    Usage {
        input_tokens: count(&usage["input_tokens"]),
        output_tokens: count(&usage["output_tokens"]),
        reasoning_tokens: usage["output_tokens_details"]["reasoning_tokens"]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok()),
    }
}
