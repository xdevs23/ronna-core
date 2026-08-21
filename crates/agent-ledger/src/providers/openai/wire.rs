//! This vendor's wire: the request shapes, the model-listing shapes, and the
//! lazy stream that carries them.

use eventsource_stream::Eventsource;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::providers::http;
use crate::providers::types::{EventStream, LlmError, StreamEvent};

use super::parser::{ResponsesSseState, parse_responses_event};

#[derive(Serialize)]
pub(super) struct ResponsesRequest {
    pub(super) model: String,
    pub(super) input: Vec<InputItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tools: Option<Vec<ResponsesTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) temperature: Option<f32>,
    pub(super) stream: bool,
    /// Always false: responses are never retained on the vendor's side.
    pub(super) store: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reasoning: Option<ReasoningParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) include: Option<Vec<&'static str>>,
}

#[derive(Serialize)]
pub(super) struct ReasoningParam {
    /// The opt-in for visible reasoning summaries. Without it no reasoning text
    /// ever streams, and the turn looks as though the model did not reason.
    pub(super) summary: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) effort: Option<String>,
}

/// One input item.
///
/// The call identifier here is the one that links a call to its output; the
/// item's own id is a different string and using it would break the link.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum InputItem {
    Message {
        role: &'static str,
        content: String,
    },
    FunctionCall {
        call_id: String,
        name: String,
        arguments: String,
    },
    FunctionCallOutput {
        call_id: String,
        output: String,
    },
    /// The replayed reasoning item: the server-assigned id, the visible summary,
    /// the constant-null status, and the encrypted continuity blob — all
    /// verbatim, because this API requires every item since the last user
    /// message to be replayed exactly as it was issued.
    Reasoning {
        id: String,
        summary: Vec<ReasoningSummaryText>,
        status: Option<String>,
        encrypted_content: String,
    },
}

/// One summary entry of a replayed reasoning item.
#[derive(Serialize)]
pub(super) struct ReasoningSummaryText {
    pub(super) r#type: &'static str,
    pub(super) text: String,
}

/// Function tools on this surface are flat — no nesting under a function key.
#[derive(Serialize)]
pub(super) struct ResponsesTool {
    pub(super) r#type: &'static str,
    pub(super) name: String,
    pub(super) description: String,
    pub(super) parameters: Value,
}

#[derive(Deserialize)]
pub(super) struct WireModelsResponse {
    pub(super) data: Vec<WireModel>,
}

#[derive(Deserialize)]
pub(super) struct WireModel {
    pub(super) id: String,
    pub(super) name: Option<String>,
}

/// Open a turn lazily: the request goes out when the first event is polled, and
/// the connect result becomes the stream's first item.
pub(super) fn open_stream(
    client: Client,
    api_key: String,
    endpoint: String,
    body: ResponsesRequest,
) -> EventStream {
    enum Phase {
        Init(Client, String, String, Box<ResponsesRequest>),
        Streaming(EventStream),
    }

    Box::pin(stream::unfold(
        Phase::Init(client, api_key, endpoint, Box::new(body)),
        |phase| async move {
            match phase {
                Phase::Init(client, api_key, endpoint, body) => {
                    let empty = || Box::pin(stream::empty()) as EventStream;

                    let response = match client
                        .post(&endpoint)
                        .bearer_auth(&api_key)
                        .json(&body)
                        .send()
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(error = %e, "responses stream failed");
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
                        warn!(status, body = %text, "responses stream failed");
                        return Some((
                            Err(LlmError::Api {
                                status,
                                message: text,
                            }),
                            Phase::Streaming(empty()),
                        ));
                    }

                    info!("responses stream connected");

                    let sse = response
                        .bytes_stream()
                        .eventsource()
                        .scan(ResponsesSseState::default(), |state, result| {
                            let events = match result {
                                Ok(event) => parse_responses_event(&event.data, state),
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
