//! This vendor's wire: the request shapes, the model-listing shapes, and the
//! event parser that turns its server-sent events back into neutral ones.
//!
//! Nothing outside this file knows any of these names.

use eventsource_stream::Eventsource;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::providers::http;
use crate::providers::types::{
    EventStream, LlmError, OpaquePayload, StopReason, StreamEvent, Usage,
};

use super::API_VERSION;

// ─── Request ─────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub(super) struct WireRequest {
    pub(super) model: String,
    pub(super) max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) system: Option<String>,
    pub(super) messages: Vec<WireMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tools: Option<Vec<WireTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) temperature: Option<f32>,
    pub(super) stream: bool,
    /// Adaptive thinking, set when the conversation selects a reasoning level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) thinking: Option<WireThinkingRequest>,
    /// The effort tier for adaptive thinking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) effort: Option<String>,
}

#[derive(Serialize)]
pub(super) struct WireThinkingRequest {
    pub(super) r#type: &'static str,
}

#[derive(Serialize)]
pub(super) struct WireMessage {
    pub(super) role: String,
    pub(super) content: WireContent,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum WireContent {
    Text(String),
    Array(Vec<WireContentBlock>),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum WireContentBlock {
    Text {
        text: String,
    },
    /// The native reasoning echo: the block's thinking text plus the server's
    /// signature — this vendor's continuity payload, replayed verbatim so a
    /// tool-call turn keeps its chain.
    Thinking {
        thinking: String,
        signature: String,
    },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

#[derive(Serialize)]
pub(super) struct WireTool {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) input_schema: Value,
}

// ─── Model listing ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct WireModelsResponse {
    pub(super) data: Vec<WireModel>,
}

#[derive(Deserialize)]
pub(super) struct WireModel {
    pub(super) id: String,
    pub(super) display_name: Option<String>,
    #[serde(default)]
    pub(super) capabilities: Option<WireCapabilities>,
}

/// The subset of a model's self-description this module reads. Every field is
/// optional and every flag defaults to false: a model that says nothing about
/// reasoning is offered without a reasoning control, which is the honest
/// reading of silence.
#[derive(Deserialize)]
pub(super) struct WireCapabilities {
    #[serde(default)]
    pub(super) effort: Option<WireEffort>,
    #[serde(default)]
    pub(super) thinking: Option<WireThinking>,
}

#[derive(Deserialize)]
pub(super) struct WireEffort {
    #[serde(default)]
    pub(super) supported: bool,
    #[serde(default)]
    pub(super) low: WireSupport,
    #[serde(default)]
    pub(super) medium: WireSupport,
    #[serde(default)]
    pub(super) high: WireSupport,
    #[serde(default)]
    pub(super) xhigh: WireSupport,
    #[serde(default)]
    pub(super) max: WireSupport,
}

#[derive(Deserialize)]
pub(super) struct WireThinking {
    #[serde(default)]
    pub(super) supported: bool,
    #[serde(default)]
    pub(super) types: WireThinkingTypes,
}

#[derive(Deserialize, Default)]
pub(super) struct WireThinkingTypes {
    #[serde(default)]
    pub(super) adaptive: WireSupport,
}

#[derive(Deserialize, Default)]
pub(super) struct WireSupport {
    #[serde(default)]
    pub(super) supported: bool,
}

// ─── The stream ──────────────────────────────────────────────────────────────

/// Open the turn lazily: the request goes out when the first event is polled,
/// and the connect result becomes the stream's first item rather than an error
/// at the call site.
///
/// One shape for every outcome is what lets the bind loop treat a refused
/// connection, a rejected request and a mid-stream drop through one code path.
pub(super) fn open_stream(
    client: Client,
    api_key: String,
    url: String,
    body: WireRequest,
) -> EventStream {
    enum Phase {
        Init(Client, String, String, Box<WireRequest>),
        Streaming(EventStream),
    }

    Box::pin(stream::unfold(
        Phase::Init(client, api_key, url, Box::new(body)),
        |phase| async move {
            match phase {
                Phase::Init(client, api_key, url, body) => {
                    let empty = || Box::pin(stream::empty()) as EventStream;

                    let response = match client
                        .post(&url)
                        .header("x-api-key", &api_key)
                        .header("anthropic-version", API_VERSION)
                        .json(&body)
                        .send()
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(error = %e, "anthropic stream failed");
                            return Some((Err(LlmError::Http(e)), Phase::Streaming(empty())));
                        }
                    };

                    if !response.status().is_success() {
                        let status = response.status().as_u16();
                        if status == 429 {
                            return Some((
                                Err(LlmError::RateLimited {
                                    retry_after_secs: http::retry_after(&response),
                                }),
                                Phase::Streaming(empty()),
                            ));
                        }
                        let text = response.text().await.unwrap_or_default();
                        warn!(status, body = %text, "anthropic stream failed");
                        return Some((
                            Err(LlmError::Api {
                                status,
                                message: text,
                            }),
                            Phase::Streaming(empty()),
                        ));
                    }

                    info!("anthropic stream connected");

                    let sse = response
                        .bytes_stream()
                        .eventsource()
                        .scan(AnthropicSseState::default(), |state, result| {
                            let events = match result {
                                Ok(event) => parse_sse_event(&event.data, state),
                                Err(e) => {
                                    warn!("SSE stream error: {e}");
                                    vec![Err(LlmError::Stream(e.to_string()))]
                                }
                            };
                            futures::future::ready(Some(events))
                        })
                        .flat_map(stream::iter);

                    Some((
                        Ok(StreamEvent::Connected),
                        Phase::Streaming(Box::pin(sse) as EventStream),
                    ))
                }
                Phase::Streaming(mut inner) => inner
                    .next()
                    .await
                    .map(|event| (event, Phase::Streaming(inner))),
            }
        },
    ))
}

/// Cross-event decoding state.
///
/// Tool events are buffered until the turn ends, and the open thinking block's
/// signature accumulates across the fragments it streams in. The signature IS
/// the continuity payload, so losing a fragment loses the whole replay.
#[derive(Default)]
pub(super) struct AnthropicSseState {
    tool_buffer: Vec<StreamEvent>,
    /// `Some` while a thinking block is open, holding the accumulated
    /// signature — possibly empty, since not every stream signs.
    thinking: Option<String>,
}

pub(super) fn parse_sse_event(
    data: &str,
    state: &mut AnthropicSseState,
) -> Vec<Result<StreamEvent, LlmError>> {
    let value: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => return vec![Err(LlmError::Json(e))],
    };

    let Some(event_type) = value["type"].as_str() else {
        return vec![];
    };

    match event_type {
        "content_block_delta" => content_block_delta(&value, state),
        "content_block_start" => {
            match value["content_block"]["type"].as_str().unwrap_or("") {
                "tool_use" => {
                    let id = value["content_block"]["id"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    let name = value["content_block"]["name"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    state
                        .tool_buffer
                        .push(StreamEvent::ToolUseStart { id, name });
                }
                "thinking" => state.thinking = Some(String::new()),
                _ => {}
            }
            vec![]
        }
        "content_block_stop" => {
            // Content blocks arrive sequentially, so an open thinking state
            // means this stop closes THE thinking block. Its end fires live —
            // buffering it past the end of the turn would orphan the block,
            // because whoever is writing the stream down finalizes on it.
            if let Some(signature) = state.thinking.take() {
                let opaque =
                    (!signature.is_empty()).then_some(OpaquePayload::Anthropic { signature });
                return vec![Ok(StreamEvent::ThinkingEnd { opaque })];
            }
            state.tool_buffer.push(StreamEvent::ToolUseEnd);
            vec![]
        }
        "message_delta" => {
            let stop_reason = match value["delta"]["stop_reason"].as_str() {
                Some("tool_use") => StopReason::ToolUse,
                Some("max_tokens") => StopReason::MaxTokens,
                _ => StopReason::EndTurn,
            };
            let usage = Usage {
                input_tokens: 0,
                output_tokens: count(&value["usage"]["output_tokens"]),
                reasoning_tokens: None,
            };

            // The end of the turn first, then the buffered tool events.
            let mut events = vec![Ok(StreamEvent::MessageEnd { usage, stop_reason })];
            events.extend(state.tool_buffer.drain(..).map(Ok));
            events
        }
        _ => vec![],
    }
}

fn content_block_delta(
    value: &Value,
    state: &mut AnthropicSseState,
) -> Vec<Result<StreamEvent, LlmError>> {
    let Some(delta_type) = value["delta"]["type"].as_str() else {
        return vec![];
    };
    match delta_type {
        "text_delta" => {
            let text = value["delta"]["text"].as_str().unwrap_or("").to_string();
            vec![Ok(StreamEvent::TextDelta { text })]
        }
        // Reasoning streams live and the block is created lazily on the first
        // delta, so there is no start event. Opening the state here as well
        // covers a delta that arrives without its block start.
        "thinking_delta" => {
            state.thinking.get_or_insert_with(String::new);
            let text = value["delta"]["thinking"]
                .as_str()
                .unwrap_or("")
                .to_string();
            vec![Ok(StreamEvent::ThinkingDelta { text })]
        }
        // The signature arrives in fragments alongside or after the reasoning
        // text; accumulate until the block stops.
        "signature_delta" => {
            let fragment = value["delta"]["signature"].as_str().unwrap_or("");
            state
                .thinking
                .get_or_insert_with(String::new)
                .push_str(fragment);
            vec![]
        }
        "input_json_delta" => {
            let json = value["delta"]["partial_json"]
                .as_str()
                .unwrap_or("")
                .to_string();
            state
                .tool_buffer
                .push(StreamEvent::ToolUseInputDelta { json });
            vec![]
        }
        _ => vec![],
    }
}

/// A token count, read defensively. An absent or oversized value reads as the
/// nearest honest number rather than panicking on a field a vendor changed.
fn count(value: &Value) -> u32 {
    value
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}
