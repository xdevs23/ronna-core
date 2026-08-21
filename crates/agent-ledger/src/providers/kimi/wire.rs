//! This vendor's wire and its client-identifying headers.
//!
//! Its REQUESTS are built here, because this endpoint rejects the shared base's
//! shape. Its RESPONSES are not: they are chat-completions events like any
//! other on that surface, so they are decoded by the shared decoder rather than
//! by a second parser of this module's own. The second parser is what lost
//! events — a chunk carrying both content and a finish reason kept only the
//! first field, a stream that ended without the sentinel never ended at all,
//! and no reasoning block was ever closed.

use std::env;
use std::path::PathBuf;

use futures::stream::{self, StreamExt};
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::providers::chat::sse::{self, SseState};
use crate::providers::http;
use crate::providers::types::{EventStream, LlmError, StreamEvent};

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

/// The usage opt-in on a streamed request.
#[derive(Debug, Clone, Serialize)]
pub(super) struct KimiStreamOptions {
    pub(super) include_usage: bool,
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
    /// The usage opt-in, mirrored from the shared chat request: without it the
    /// endpoint only volunteers counts, and the decoder can only report what
    /// arrived. This vendor's endpoint speaks the chat-completions dialect, so
    /// the opt-in is the same shape.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) stream_options: Option<KimiStreamOptions>,
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

                    Some((
                        Ok(StreamEvent::Connected),
                        Phase::Streaming(sse::decode_stream(response, SseState::default())),
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
