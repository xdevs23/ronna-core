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
    #[serde(skip_serializing_if = "OutputConfig::is_empty")]
    pub(super) output_config: OutputConfig,
    /// Automatic prompt caching: the API puts one breakpoint on the last
    /// cacheable block, so each request reads the prefix the previous one
    /// wrote.
    pub(super) cache_control: WireCacheControl,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum WireCacheControl {
    /// A cache entry with the default lifetime of five minutes.
    Ephemeral,
}

/// The output settings, of which this library sets the effort and the format.
#[derive(Serialize)]
pub(super) struct OutputConfig {
    /// The effort tier for adaptive thinking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) effort: Option<String>,
    /// The response schema, absent for text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) format: Option<WireOutputFormat>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum WireOutputFormat {
    /// The caller's JSON schema document, sent unchanged.
    JsonSchema { schema: Value },
}

impl OutputConfig {
    fn is_empty(&self) -> bool {
        self.effort.is_none() && self.format.is_none()
    }
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
    /// A user-authored image, carried inline as this vendor's base64 source
    /// block.
    Image {
        source: WireImageSource,
    },
}

/// The inline image source: the bytes base64-encoded beside their MIME type.
#[derive(Serialize, Deserialize)]
pub(super) struct WireImageSource {
    /// Always `base64` — the bytes travel with the request rather than by
    /// reference.
    pub(super) r#type: String,
    pub(super) media_type: String,
    pub(super) data: String,
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
    /// Whether the content block currently open is a tool call.
    ///
    /// The stop event names no kind, so the kind is remembered from the start.
    /// Without it every stop that was not a thinking block ended a tool call
    /// that never began, and a text-only answer closed with an unmatched tool
    /// end — an event whoever writes the stream down has no call to attach to.
    tool_open: bool,
    /// The input counts from the start of the message, used when the end of
    /// the turn is built.
    input: InputCounts,
}

/// The input counts this vendor reports: the tokens after the last cache
/// breakpoint, and the tokens written to and read from the cache. Each one is
/// `None` where the event left it out.
#[derive(Deserialize, Default, Clone, Copy)]
struct InputCounts {
    #[serde(rename = "input_tokens")]
    uncached: Option<u32>,
    #[serde(rename = "cache_creation_input_tokens")]
    cache_write: Option<u32>,
    #[serde(rename = "cache_read_input_tokens")]
    cache_read: Option<u32>,
}

impl InputCounts {
    /// The counts in a usage object, all `None` when the event has none.
    fn read(usage: &Value) -> Result<Self, serde_json::Error> {
        Option::<Self>::deserialize(usage).map(Option::unwrap_or_default)
    }

    /// These counts, with each one they leave out taken from `fallback`.
    fn with_fallback(self, fallback: Self) -> Self {
        Self {
            uncached: self.uncached.or(fallback.uncached),
            cache_write: self.cache_write.or(fallback.cache_write),
            cache_read: self.cache_read.or(fallback.cache_read),
        }
    }

    /// The whole prompt, which is the sum of the three counts.
    fn total(self) -> Result<u32, serde_json::Error> {
        [self.uncached, self.cache_write, self.cache_read]
            .into_iter()
            .flatten()
            .try_fold(0, u32::checked_add)
            .ok_or_else(|| serde::de::Error::custom("the input token counts overflow u32"))
    }
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
        "message_start" => match InputCounts::read(&value["message"]["usage"]) {
            Ok(input) => {
                state.input = input;
                vec![]
            }
            Err(e) => vec![Err(LlmError::Json(e))],
        },
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
                    state.tool_open = true;
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
            // Only a block that started as a tool call ends as one. A text
            // block's stop closes nothing: text is finalized by the end of the
            // turn, and a tool end here would name a call that never started.
            if std::mem::take(&mut state.tool_open) {
                state.tool_buffer.push(StreamEvent::ToolUseEnd);
            }
            vec![]
        }
        "message_delta" => {
            let stop_reason = match value["delta"]["stop_reason"].as_str() {
                Some("end_turn") => StopReason::EndTurn,
                Some("tool_use") => StopReason::ToolUse,
                Some("max_tokens") => StopReason::MaxTokens,
                Some("refusal") => StopReason::Refusal,
                Some(other) => {
                    return vec![Err(LlmError::ProviderFailure(format!(
                        "unknown stop reason {other}"
                    )))];
                }
                None => {
                    return vec![Err(LlmError::ProviderFailure(
                        "message delta without a stop reason".into(),
                    ))];
                }
            };
            // The end of the turn restates the input counts on some versions
            // of this wire and omits them on others; the counts from the start
            // of the message are the fallback, never a zero.
            let input = match InputCounts::read(&value["usage"]) {
                Ok(input) => input.with_fallback(state.input),
                Err(e) => return vec![Err(LlmError::Json(e))],
            };
            let input_tokens = match input.total() {
                Ok(total) => total,
                Err(e) => return vec![Err(LlmError::Json(e))],
            };
            let usage = Usage {
                input_tokens,
                output_tokens: count(&value["usage"]["output_tokens"]),
                reasoning_tokens: None,
                cache_read_tokens: input.cache_read,
                cache_write_tokens: input.cache_write,
            };

            // The end of the turn first, then the buffered tool events.
            let mut events = vec![Ok(StreamEvent::MessageEnd { usage, stop_reason })];
            events.extend(state.tool_buffer.drain(..).map(Ok));
            events
        }
        "error" => vec![Err(stream_error(
            WireStreamError::deserialize(&value["error"]).unwrap_or_default(),
        ))],
        _ => vec![],
    }
}

/// The error object of an in-stream error event. A field the event leaves out
/// is absent, and an unreadable object reads as one with neither field.
#[derive(Deserialize, Default)]
struct WireStreamError {
    #[serde(rename = "type")]
    kind: Option<String>,
    message: Option<String>,
}

/// The neutral error for a failure the server reports inside an opened
/// stream.
///
/// An overloaded or failing server is unavailable for now. A rate limit takes
/// the rate-limit path. Every other type, and an error without one, is a
/// verdict on the request and ends the turn.
fn stream_error(error: WireStreamError) -> LlmError {
    let mut detail = error
        .kind
        .clone()
        .unwrap_or_else(|| "error event without a type".into());
    if let Some(message) = error.message {
        detail = format!("{detail}: {message}");
    }
    match error.kind.as_deref() {
        Some("overloaded_error" | "api_error") => LlmError::Unavailable(detail),
        Some("rate_limit_error") => LlmError::RateLimited {
            retry_after_secs: None,
        },
        _ => LlmError::ProviderFailure(detail),
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
        // An argument fragment belongs to a tool block, so it also marks one
        // open: a fragment that arrives without its block start still ends
        // with a matching tool end.
        "input_json_delta" => {
            let json = value["delta"]["partial_json"]
                .as_str()
                .unwrap_or("")
                .to_string();
            state.tool_open = true;
            state
                .tool_buffer
                .push(StreamEvent::ToolUseInputDelta { json });
            vec![]
        }
        _ => vec![],
    }
}

/// A token count where the vendor stated one, read defensively: an absent or
/// oversized value is no count at all rather than a panic on a field a vendor
/// changed.
fn count_opt(value: &Value) -> Option<u32> {
    value.as_u64().and_then(|n| u32::try_from(n).ok())
}

/// A token count, with an unstated one read as zero.
fn count(value: &Value) -> u32 {
    count_opt(value).unwrap_or(0)
}
