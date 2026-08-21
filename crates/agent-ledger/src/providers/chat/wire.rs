//! The chat-completions wire: request shapes, model-listing shapes, and the
//! lazy stream that carries them.

use eventsource_stream::Eventsource;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::providers::http;
use crate::providers::types::{EventStream, LlmError, StreamEvent};

use super::sse::{SseState, finish_stream, parse_sse_chunk};

/// The line a chat-completions stream ends on.
const SSE_DONE: &str = "[DONE]";

#[derive(Serialize)]
pub(crate) struct WireRequest {
    pub(super) model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) max_tokens: Option<u32>,
    pub(super) messages: Vec<WireMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tools: Option<Vec<WireTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) temperature: Option<f32>,
    pub(super) stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) stream_options: Option<WireStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reasoning_effort: Option<String>,
}

/// Streaming opt-ins.
///
/// Requesting the terminal usage chunk is sent on every streaming request. A
/// vendor that ignores it simply never sends the chunk, and the end-of-stream
/// line releases the turn with zeroed counts instead — so asking costs nothing
/// and not asking costs every usage figure on this surface.
#[derive(Serialize)]
pub(super) struct WireStreamOptions {
    pub(super) include_usage: bool,
}

#[derive(Serialize)]
pub(crate) struct WireMessage {
    pub(super) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) content: Option<WireMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tool_calls: Option<Vec<WireToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tool_call_id: Option<String>,
    /// The verbatim reasoning echo: stored entries rebuilt in position order.
    /// Only populated on an endpoint that accepts it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reasoning_details: Option<Vec<WireReasoningDetail>>,
}

/// A message's content, which is legitimately a string or an array on this
/// surface.
///
/// The base only ever constructs the string form. The array is an opaque
/// pass-through a vendor module injects through its own content encoder, so a
/// vendor with a structured content shape does not have to fork the base to get
/// it — and the base does not have to learn what that shape means.
#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum WireMessageContent {
    Text(String),
    /// Constructed only by a vendor's own encoder, so whether anything builds
    /// one depends on which vendors are compiled.
    #[allow(dead_code)]
    Chunks(Vec<Value>),
}

/// One rebuilt reasoning entry: exactly one of the three content fields is
/// populated, per the entry's type, and a signature rides along on the entries
/// that carried one.
#[derive(Serialize)]
pub(super) struct WireReasoningDetail {
    pub(super) r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<String>,
    pub(super) format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) signature: Option<String>,
}

#[derive(Serialize)]
pub(super) struct WireTool {
    pub(super) r#type: String,
    pub(super) function: WireFunction,
}

#[derive(Serialize)]
pub(super) struct WireFunction {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) parameters: Value,
}

#[derive(Serialize, Deserialize)]
pub(super) struct WireToolCall {
    pub(super) id: String,
    pub(super) r#type: String,
    pub(super) function: WireToolCallFunction,
}

#[derive(Serialize, Deserialize)]
pub(super) struct WireToolCallFunction {
    pub(super) name: String,
    pub(super) arguments: String,
}

#[derive(Deserialize)]
pub(super) struct WireModelsResponse {
    pub(super) data: Vec<WireModel>,
}

/// One entry of a model list on this surface.
#[derive(Deserialize)]
pub(crate) struct WireModel {
    pub(crate) id: String,
    pub(super) name: Option<String>,
    /// A per-model reasoning descriptor, where the endpoint publishes one.
    /// Absent on endpoints whose model list carries no capability data, which
    /// is why each vendor supplies its own capability mapper rather than the
    /// base assuming this field exists.
    #[serde(default)]
    pub(crate) reasoning: Option<WireModelReasoning>,
}

/// A model's reasoning descriptor. Its presence means the model reasons;
/// `supported_efforts`, when populated, lists the exact levels it exposes.
#[derive(Deserialize)]
pub(crate) struct WireModelReasoning {
    #[serde(default)]
    pub(crate) supported_efforts: Option<Vec<String>>,
}

/// Send the request and classify the answer.
///
/// A rate limit is separated from every other refusal here, because it is the
/// one the bind loop can wait out — everything else is a fact about the request
/// that waiting will not change.
async fn connect(
    client: &Client,
    endpoint: &str,
    api_key: &str,
    headers: HeaderMap,
    body: &WireRequest,
) -> Result<reqwest::Response, LlmError> {
    let response = client
        .post(endpoint)
        .bearer_auth(api_key)
        .headers(headers)
        .json(body)
        .send()
        .await
        .inspect_err(|e| warn!(error = %e, "chat stream failed"))?;

    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status().as_u16();
    if status == 429 {
        return Err(LlmError::RateLimited {
            retry_after_secs: http::retry_after(&response),
        });
    }
    let text = response.text().await.unwrap_or_default();
    warn!(status, body = %text, "chat stream failed");
    Err(LlmError::Api {
        status,
        message: text,
    })
}

/// Open a turn lazily: the request goes out when the first event is polled, and
/// the connect result becomes the stream's first item.
pub(super) fn open_stream(
    client: Client,
    api_key: String,
    endpoint: String,
    headers: HeaderMap,
    body: WireRequest,
    state: SseState,
) -> EventStream {
    /// The opening state, boxed as a whole: it is far larger than a stream
    /// handle, and every poll after the first would otherwise carry that size.
    struct Init {
        client: Client,
        api_key: String,
        endpoint: String,
        headers: HeaderMap,
        body: WireRequest,
        state: SseState,
    }

    enum Phase {
        Init(Box<Init>),
        Streaming(EventStream),
    }

    Box::pin(stream::unfold(
        Phase::Init(Box::new(Init {
            client,
            api_key,
            endpoint,
            headers,
            body,
            state,
        })),
        |phase| async move {
            match phase {
                Phase::Init(init) => {
                    let Init {
                        client,
                        api_key,
                        endpoint,
                        headers,
                        body,
                        state,
                    } = *init;

                    let response = match connect(&client, &endpoint, &api_key, headers, &body).await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            let empty = Box::pin(stream::empty()) as EventStream;
                            return Some((Err(e), Phase::Streaming(empty)));
                        }
                    };

                    info!("chat stream connected");

                    let source = Box::pin(response.bytes_stream().eventsource());
                    let sse = stream::unfold(
                        (source, state, false),
                        |(mut source, mut state, terminated)| async move {
                            if terminated {
                                return None;
                            }
                            let events = match source.next().await {
                                Some(Ok(event)) if event.data == SSE_DONE => {
                                    finish_stream(&mut state)
                                }
                                Some(Ok(event)) => parse_sse_chunk(&event.data, &mut state),
                                Some(Err(e)) => {
                                    warn!("SSE stream error: {e}");
                                    vec![Err(LlmError::Stream(e.to_string()))]
                                }
                                // The transport ended without the end-of-stream
                                // line: release any deferred end-of-turn so a
                                // completed turn is committed rather than left
                                // stranded as an uncommitted streaming block.
                                // The drain is inert if the line already
                                // released it, so this cannot double-emit.
                                None => {
                                    let drained = finish_stream(&mut state);
                                    return (!drained.is_empty())
                                        .then_some((drained, (source, state, true)));
                                }
                            };
                            Some((events, (source, state, terminated)))
                        },
                    )
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
