//! This vendor's wire, its client-identifying headers, and its event parser.

use std::env;
use std::path::PathBuf;

use eventsource_stream::Eventsource;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::providers::http;
use crate::providers::types::{EventStream, LlmError, StopReason, StreamEvent, Usage};

/// The line this vendor's stream ends on.
const SSE_DONE: &str = "[DONE]";

/// The client version this module identifies itself as. This endpoint is the
/// vendor's own coding surface and expects its own client's identification, so
/// the emulation is the integration rather than an embellishment on it.
const CLIENT_VERSION: &str = "1.41.0";
const CLIENT_USER_AGENT: &str = "KimiCLI/1.41.0";

// ─── Request ─────────────────────────────────────────────────────────────────

/// The reasoning switch on a request: enabled or disabled, by name.
#[derive(Debug, Clone, Serialize)]
pub(super) struct KimiThinking {
    pub(super) r#type: String,
}

#[derive(Serialize)]
pub(super) struct WireRequest {
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
    pub(super) prompt_cache_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) thinking: Option<KimiThinking>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reasoning_effort: Option<String>,
}

#[derive(Serialize)]
pub(super) struct WireMessage {
    pub(super) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) content: Option<String>,
    /// Reasoning rides HERE rather than in the content: this vendor keeps the
    /// two apart, and requires the field on every assistant message once
    /// reasoning is on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tool_calls: Option<Vec<WireToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tool_call_id: Option<String>,
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

#[derive(Deserialize)]
pub(super) struct WireModel {
    pub(super) id: String,
    pub(super) display_name: Option<String>,
    /// Always-on, toggleable, or none. The primary reasoning signal on this
    /// vendor's model list.
    #[serde(default)]
    pub(super) supports_thinking_type: Option<String>,
    /// The fallback when the primary signal is absent: whether the model
    /// reasons at all, treated as toggleable.
    #[serde(default)]
    pub(super) supports_reasoning: Option<bool>,
}

// ─── Endpoint ────────────────────────────────────────────────────────────────

/// Normalize a configured endpoint to exactly one version suffix, so a
/// configuration written either way reaches the same URL.
pub(super) fn normalize_base_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1")
    }
}

// ─── Client identification ───────────────────────────────────────────────────

/// Build the headers this vendor's own client sends.
///
/// Built per request rather than per provider, because assembling them reads the
/// machine and can write a file — side effects that have no business happening
/// when a value is merely constructed.
pub(super) fn build_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    let unknown = || HeaderValue::from_static("unknown");

    headers.insert("User-Agent", HeaderValue::from_static(CLIENT_USER_AGENT));
    headers.insert("X-Msh-Platform", HeaderValue::from_static("kimi_cli"));
    headers.insert("X-Msh-Version", HeaderValue::from_static(CLIENT_VERSION));
    headers.insert(
        "X-Msh-Device-Name",
        HeaderValue::from_str(&ascii_header_value(&hostname())).unwrap_or_else(|_| unknown()),
    );
    headers.insert(
        "X-Msh-Device-Model",
        HeaderValue::from_str(&ascii_header_value(&device_model())).unwrap_or_else(|_| unknown()),
    );
    headers.insert(
        "X-Msh-Device-Id",
        HeaderValue::from_str(&device_id()).unwrap_or_else(|_| unknown()),
    );
    headers.insert(
        "X-Msh-Os-Version",
        HeaderValue::from_str(&ascii_header_value(&os_version())).unwrap_or_else(|_| unknown()),
    );

    headers
}

/// Keep only printable ASCII: a header value is bytes with a narrow legal
/// range, and a machine name is arbitrary text.
fn ascii_header_value(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .filter(|c| matches!(*c as u32, 0x20..=0x7e))
        .collect();
    if sanitized.trim().is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_or_else(|_| "unknown".to_string(), |s| s.trim().to_string())
}

fn kernel_release() -> Option<String> {
    std::process::Command::new("uname")
        .arg("-r")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn device_model() -> String {
    let os = env::consts::OS;
    let arch = env::consts::ARCH;

    if os == "linux" {
        match kernel_release() {
            Some(release) if !arch.is_empty() => format!("Linux {release} {arch}"),
            Some(release) => format!("Linux {release}"),
            None => format!("Linux {arch}"),
        }
    } else {
        format!("{os} {arch}")
    }
}

fn os_version() -> String {
    let os = env::consts::OS;
    if os == "linux" {
        kernel_release().unwrap_or_else(|| format!("{os} unknown"))
    } else {
        os.to_string()
    }
}

/// A stable identifier for this installation, generated once and kept.
///
/// It is persisted outside the ledger on purpose: it identifies the machine
/// rather than a conversation, and a new one on every run would look to the
/// vendor like a new device on every request.
fn device_id() -> String {
    let path = PathBuf::from(env::var("HOME").unwrap_or_default())
        .join(".kimi")
        .join("device_id");

    if let Ok(existing) = std::fs::read_to_string(&path) {
        let trimmed = existing.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    let id = uuid::Uuid::new_v4().to_string().replace('-', "");
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&path, &id);
    id
}

// ─── The stream ──────────────────────────────────────────────────────────────

/// Open a turn lazily: the request goes out when the first event is polled.
pub(super) fn open_stream(
    client: Client,
    access_token: String,
    endpoint: String,
    body: WireRequest,
) -> EventStream {
    enum Phase {
        Init(Client, String, String, Box<WireRequest>),
        Streaming(EventStream),
    }

    Box::pin(stream::unfold(
        Phase::Init(client, access_token, endpoint, Box::new(body)),
        |phase| async move {
            match phase {
                Phase::Init(client, access_token, endpoint, body) => {
                    let empty = || Box::pin(stream::empty()) as EventStream;

                    let response = match client
                        .post(&endpoint)
                        .bearer_auth(&access_token)
                        .headers(build_headers())
                        .json(&body)
                        .send()
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(error = %e, "kimi stream failed");
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
                        warn!(status, body = %text, "kimi stream failed");
                        return Some((
                            Err(LlmError::Api {
                                status,
                                message: text,
                            }),
                            Phase::Streaming(empty()),
                        ));
                    }

                    info!("kimi stream connected");

                    let sse = response
                        .bytes_stream()
                        .eventsource()
                        .scan(Vec::<StreamEvent>::new(), |buffer, result| {
                            let events = match result {
                                Ok(event) if event.data == SSE_DONE => {
                                    buffer.drain(..).map(Ok).collect()
                                }
                                Ok(event) => parse_sse_chunk(&event.data, buffer),
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

/// Decode one chunk. Tool events buffer until the end of the turn, so a
/// complete lifecycle follows it rather than straddling it.
fn parse_sse_chunk(
    data: &str,
    tool_buffer: &mut Vec<StreamEvent>,
) -> Vec<Result<StreamEvent, LlmError>> {
    let Ok(value) = serde_json::from_str::<Value>(data) else {
        return vec![];
    };

    let Some(choice) = value["choices"].get(0) else {
        return vec![];
    };
    let delta = &choice["delta"];

    // Reasoning precedes content in this stream; a single chunk may carry both,
    // in which case both events come out in that order.
    let mut events = vec![];

    if let Some(reasoning) = delta["reasoning_content"].as_str()
        && !reasoning.is_empty()
    {
        events.push(Ok(StreamEvent::ThinkingDelta {
            text: reasoning.to_string(),
        }));
    }

    if let Some(content) = delta["content"].as_str()
        && !content.is_empty()
    {
        events.push(Ok(StreamEvent::TextDelta {
            text: content.to_string(),
        }));
    }

    if !events.is_empty() {
        return events;
    }

    if let Some(tool_calls) = delta["tool_calls"].as_array() {
        for tc in tool_calls {
            if let Some(name) = tc["function"]["name"].as_str() {
                let id = tc["id"].as_str().unwrap_or("").to_string();
                tool_buffer.push(StreamEvent::ToolUseStart {
                    id,
                    name: name.to_string(),
                });
            }
            if let Some(args) = tc["function"]["arguments"].as_str()
                && !args.is_empty()
            {
                tool_buffer.push(StreamEvent::ToolUseInputDelta {
                    json: args.to_string(),
                });
            }
        }
        return vec![];
    }

    if let Some(reason) = choice["finish_reason"].as_str() {
        let stop_reason = match reason {
            "tool_calls" => StopReason::ToolUse,
            "length" => StopReason::MaxTokens,
            _ => StopReason::EndTurn,
        };

        let mut events = vec![Ok(StreamEvent::MessageEnd {
            usage: Usage::default(),
            stop_reason,
        })];
        events.extend(tool_buffer.drain(..).map(Ok));
        if stop_reason == StopReason::ToolUse {
            events.push(Ok(StreamEvent::ToolUseEnd));
        }
        return events;
    }

    vec![]
}
