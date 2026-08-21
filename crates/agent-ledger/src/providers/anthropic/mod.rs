//! The Anthropic provider: the Messages API, its server-sent events, and the
//! native reasoning echo.
//!
//! Everything Anthropic-shaped lives in this module. The blocks arrive already
//! projected into neutral messages by the one central pass, and this module
//! owns exactly the step after that: neutral to wire, and wire back to neutral.

use serde_json::Value;
use tracing::{debug, info, warn};

use crate::store::{StoreError, StoreTx};

use super::bind::{self, OpenedTurn};
use super::http;
use super::http_store::{HttpProviderConfig, HttpProviderStore};
use super::types::{
    CompletionRequest, ContentPart, LlmError, Message, MessageContent, MessageRole, ModelInfo,
    ModelSelector, OpaquePayload, ProviderRx, ProviderTx, ReasoningCapability, ReasoningLevel,
    ToolDefinition,
};
use super::{BoxFuture, ProviderModule};

mod wire;

use wire::{
    WireCapabilities, WireContent, WireContentBlock, WireMessage, WireModel, WireModelsResponse,
    WireRequest, WireThinkingRequest, WireTool,
};

/// The API version header this module speaks. Pinned rather than tracked: a
/// version bump is a wire change, and a wire change belongs in a diff.
const API_VERSION: &str = "2023-06-01";
const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

/// The model this provider falls back to for cheap background work.
const LIGHTWEIGHT_MODEL: &str = "claude-haiku-4-5-20251001";

/// One configured Anthropic endpoint.
pub struct AnthropicProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl AnthropicProvider {
    /// Build a provider for one credential and, optionally, one endpoint.
    #[must_use]
    pub fn new(api_key: String, base_url: Option<String>) -> Self {
        Self {
            client: http::streaming_client(),
            api_key,
            base_url: base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
        }
    }

    /// Build the wire request.
    ///
    /// `include_reasoning_payloads` is the fallback knob; the returned flag
    /// reports whether any stored payload was actually replayed, which is what
    /// decides whether a rejection earns a payload-free retry.
    fn build_request_body(
        request: &CompletionRequest,
        stream: bool,
        include_reasoning_payloads: bool,
    ) -> (WireRequest, bool) {
        let (system, messages, carried_payloads) =
            convert_messages(&request.messages, include_reasoning_payloads);

        let tools: Vec<WireTool> = request
            .tools
            .iter()
            .map(|t| WireTool {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: t.parameters.clone(),
            })
            .collect();

        // Reasoning maps to adaptive thinking plus an optional effort tier. The
        // newer models reject a manual token budget outright, so adaptive is the
        // only path; "the model decides" is adaptive thinking with no effort.
        let (thinking, effort) = match request.reasoning {
            Some(level) => (
                Some(WireThinkingRequest { r#type: "adaptive" }),
                anthropic_effort(level),
            ),
            None => (None, None),
        };

        let body = WireRequest {
            model: request.model.clone(),
            max_tokens: request.max_tokens.unwrap_or(32768),
            system,
            messages,
            tools: if tools.is_empty() { None } else { Some(tools) },
            temperature: request.temperature,
            stream,
            thinking,
            effort,
        };
        (body, carried_payloads)
    }

    /// Fetch and parse the raw model list, keeping the capability fields.
    async fn fetch_wire_models(&self) -> Result<Vec<WireModel>, LlmError> {
        let url = format!("{}/v1/models", self.base_url);
        info!(url, "fetching model list");

        let response = self
            .client
            .get(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            if status == 429 {
                return Err(LlmError::RateLimited {
                    retry_after_secs: http::retry_after(&response),
                });
            }
            let text = response.text().await.unwrap_or_default();
            warn!(status, body = %text, "list_models failed");
            return Err(LlmError::Api {
                status,
                message: text,
            });
        }

        let wire: WireModelsResponse = response.json().await?;
        Ok(wire.data)
    }
}

impl AnthropicProvider {
    /// Every model this endpoint offers, each carrying the reasoning levels it
    /// describes itself as supporting.
    ///
    /// # Errors
    ///
    /// If the request fails or the provider refuses it.
    pub async fn list_models(&self) -> Result<Vec<ModelInfo>, LlmError> {
        let vendor = if self.base_url == DEFAULT_BASE_URL {
            "Anthropic"
        } else {
            "Anthropic (custom)"
        };

        let mut models: Vec<ModelInfo> = self
            .fetch_wire_models()
            .await?
            .into_iter()
            .map(|m| ModelInfo {
                reasoning: reasoning_from_capabilities(m.capabilities.as_ref()),
                name: m.display_name.unwrap_or_else(|| m.id.clone()),
                id: m.id,
                vendor: vendor.to_string(),
            })
            .collect();
        models.sort_by(|a, b| a.name.cmp(&b.name));
        info!(count = models.len(), "fetched models");
        Ok(models)
    }

    /// Open one streaming turn.
    ///
    /// The stream is lazy: the request is not sent until the first event is
    /// polled, so opening a turn that is immediately superseded costs nothing.
    ///
    /// # Errors
    ///
    /// Never at this point — a transport or API failure arrives as the stream's
    /// first item, so the bind loop's retry machinery sees it in the one place
    /// that knows what to do about it.
    ///
    /// Asynchronous although it awaits nothing: this is the shape the bind
    /// loop's opener has, and a vendor that must await before opening — a
    /// credential refresh, say — fits it without every caller changing.
    #[allow(clippy::unused_async)]
    pub async fn open_turn(
        &self,
        request: CompletionRequest,
        include_reasoning_payloads: bool,
    ) -> Result<OpenedTurn, LlmError> {
        info!(
            model = %request.model,
            messages = request.messages.len(),
            "anthropic stream request"
        );
        debug!(body = ?serde_json::to_string(&request.messages), "request messages");

        let (body, carried_payloads) =
            Self::build_request_body(&request, true, include_reasoning_payloads);

        Ok(OpenedTurn {
            events: wire::open_stream(
                self.client.clone(),
                self.api_key.clone(),
                format!("{}/v1/messages", self.base_url),
                body,
            ),
            carried_payloads,
        })
    }
}

/// The neutral request one bound turn is opened with.
///
/// It lives here, named, rather than inline in the bind closure, because what
/// the bind path actually sends is exactly what a golden has to be able to pin.
///
/// `max_tokens` is deliberately `None`: the request builder's own default is
/// this vendor's ceiling, and a caller-supplied number above the model's limit
/// is rejected outright with a 400 — the whole turn, for a field nobody chose.
fn turn_request(
    selector: ModelSelector,
    messages: Vec<Message>,
    tools: Vec<ToolDefinition>,
    reasoning: Option<ReasoningLevel>,
) -> CompletionRequest {
    CompletionRequest {
        model: match selector {
            ModelSelector::Specific(m) => m,
            ModelSelector::Lightweight => LIGHTWEIGHT_MODEL.into(),
        },
        messages,
        tools,
        max_tokens: None,
        temperature: None,
        stream: true,
        reasoning,
    }
}

/// Convert neutral messages into this vendor's messages.
///
/// Free rather than a method: the translation reads nothing about the endpoint
/// it will be sent to, and saying so keeps it obvious that a request's shape
/// does not depend on which instance built it.
fn convert_messages(
    messages: &[Message],
    include_reasoning_payloads: bool,
) -> (Option<String>, Vec<WireMessage>, bool) {
    let mut system: Option<String> = None;
    let mut wire_messages = Vec::new();
    let mut carried_payloads = false;

    for msg in messages {
        let role = match msg.role {
            MessageRole::System => {
                if let MessageContent::Text(text) = &msg.content {
                    // This API has no in-stream system message, so every
                    // system group folds into the one parameter — JOINED,
                    // never overwritten. A later system line, such as a
                    // mid-conversation date marker, must not erase the
                    // system prompt that opened the conversation.
                    system = Some(match system.take() {
                        Some(existing) => format!("{existing}\n\n{text}"),
                        None => text.clone(),
                    });
                }
                continue;
            }
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
        };

        let content = match &msg.content {
            MessageContent::Text(text) => {
                WireContent::Array(vec![WireContentBlock::Text { text: text.clone() }])
            }
            MessageContent::Parts(parts) => WireContent::Array(
                parts
                    .iter()
                    .map(|p| {
                        wire_content_block(p, include_reasoning_payloads, &mut carried_payloads)
                    })
                    .collect(),
            ),
        };

        wire_messages.push(WireMessage {
            role: role.to_string(),
            content,
        });
    }

    (system, wire_messages, carried_payloads)
}

/// Route one neutral part into its wire block.
///
/// The variant gate: reasoning replays natively — as this vendor's own thinking
/// block, signature and all — only for THIS vendor's payload variant, and only
/// while the knob admits payloads. A foreign variant or an absent payload is
/// OMITTED, and its text renders as the plain text block it always was.
///
/// Omitted, never adapted. Another vendor's continuity blob reshaped to look
/// like this one's is a forgery: the signature will not verify, and the whole
/// turn is rejected with an error that names the reasoning rather than the
/// adaptation that caused it.
fn wire_content_block(
    part: &ContentPart,
    include_reasoning_payloads: bool,
    carried_payloads: &mut bool,
) -> WireContentBlock {
    match part {
        ContentPart::Text { text } => WireContentBlock::Text { text: text.clone() },
        ContentPart::Reasoning { text, opaque } => match opaque {
            Some(OpaquePayload::Anthropic { signature }) if include_reasoning_payloads => {
                *carried_payloads = true;
                WireContentBlock::Thinking {
                    thinking: text.clone(),
                    signature: signature.clone(),
                }
            }
            _ => WireContentBlock::Text { text: text.clone() },
        },
        ContentPart::ToolUse { id, name, input } => WireContentBlock::ToolUse {
            id: id.clone(),
            name: name.clone(),
            input: input.clone(),
        },
        ContentPart::ToolResult {
            tool_use_id,
            content,
        } => WireContentBlock::ToolResult {
            tool_use_id: tool_use_id.clone(),
            content: content.clone(),
        },
    }
}

/// The effort tier sent alongside adaptive thinking.
///
/// "The model decides" defers, sending no effort. Off and minimal are never
/// advertised for this vendor and map to no effort defensively — sending an
/// effort value the API does not accept fails the whole turn.
fn anthropic_effort(level: ReasoningLevel) -> Option<String> {
    match level {
        ReasoningLevel::Low => Some("low"),
        ReasoningLevel::Medium => Some("medium"),
        ReasoningLevel::High => Some("high"),
        ReasoningLevel::XHigh => Some("xhigh"),
        ReasoningLevel::Max => Some("max"),
        ReasoningLevel::Auto | ReasoningLevel::Off | ReasoningLevel::Minimal => None,
    }
    .map(str::to_string)
}

/// Map one model's self-described capabilities into canonical reasoning levels.
///
/// The effort tiers come straight from what the model says it accepts, and
/// adaptive thinking surfaces as "the model decides". Reading the model's own
/// description rather than a table here means a newly released model is offered
/// correctly without a release of this library.
fn reasoning_from_capabilities(caps: Option<&WireCapabilities>) -> ReasoningCapability {
    let Some(caps) = caps else {
        return ReasoningCapability::default();
    };

    let mut levels = Vec::new();
    if caps
        .thinking
        .as_ref()
        .is_some_and(|t| t.supported && t.types.adaptive.supported)
    {
        levels.push(ReasoningLevel::Auto);
    }
    if let Some(effort) = caps.effort.as_ref().filter(|e| e.supported) {
        for (supported, level) in [
            (effort.low.supported, ReasoningLevel::Low),
            (effort.medium.supported, ReasoningLevel::Medium),
            (effort.high.supported, ReasoningLevel::High),
            (effort.xhigh.supported, ReasoningLevel::XHigh),
            (effort.max.supported, ReasoningLevel::Max),
        ] {
            if supported {
                levels.push(level);
            }
        }
    }

    ReasoningCapability::new(levels)
}

// ─── The registered module ───────────────────────────────────────────────────

/// The Anthropic provider module, as the registry holds it.
pub struct AnthropicModule {
    store: HttpProviderStore,
}

impl AnthropicModule {
    /// Create the module and its configuration table.
    ///
    /// # Panics
    ///
    /// If the table cannot be created. A provider whose configuration cannot be
    /// stored has no working state to fall back on, and continuing would defer
    /// the same failure to a moment when a user is waiting on an answer.
    pub async fn new(tx: StoreTx) -> Self {
        let store = HttpProviderStore::new(tx, "anthropic", "provider_anthropic")
            .await
            .expect("anthropic domain migration failed");
        Self { store }
    }

    /// Read the credential and endpoint out of an instance's configuration.
    ///
    /// One place, shared by every fallible operation, so "what counts as
    /// configured" has a single answer.
    fn provider_from_config(config: &Value) -> Result<AnthropicProvider, LlmError> {
        let api_key = config["api_key"]
            .as_str()
            .ok_or_else(|| LlmError::MissingKey("anthropic".into()))?
            .to_string();
        let base_url = config["base_url"].as_str().map(String::from);
        Ok(AnthropicProvider::new(api_key, base_url))
    }
}

impl ProviderModule for AnthropicModule {
    fn type_id(&self) -> &'static str {
        "anthropic"
    }

    fn display_name(&self) -> &'static str {
        "Anthropic"
    }

    fn description(&self) -> &'static str {
        "Claude models via the Messages API"
    }

    fn get_config(&self, provider_id: String) -> BoxFuture<'_, Result<Option<Value>, StoreError>> {
        Box::pin(async move {
            let config = self.store.get_config(provider_id).await?;
            config
                .map(|c| serde_json::to_value(c).map_err(|e| StoreError::Other(e.to_string())))
                .transpose()
        })
    }

    fn save_config(
        &self,
        provider_id: String,
        config: Value,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let typed: HttpProviderConfig =
                serde_json::from_value(config).map_err(|e| StoreError::Other(e.to_string()))?;
            self.store.save_config(provider_id, typed).await
        })
    }

    fn delete_config(&self, provider_id: String) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.store.delete_config(provider_id).await })
    }

    fn summary(&self, provider_id: String) -> BoxFuture<'_, Result<Option<String>, StoreError>> {
        Box::pin(async move { self.store.summary(provider_id).await })
    }

    fn bind(
        &self,
        _conversation_id: i64,
        _provider_id: String,
        config: Value,
    ) -> (ProviderTx, ProviderRx) {
        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();

        let api_key = config["api_key"].as_str().unwrap_or("").to_string();
        let base_url = config["base_url"].as_str().map(String::from);

        tokio::spawn(bind::run_http_bind_loop_with_replay(
            req_rx,
            resp_tx,
            move |messages, selector, tools, reasoning, include_reasoning_payloads| {
                let provider = AnthropicProvider::new(api_key.clone(), base_url.clone());
                let request = turn_request(selector, messages, tools, reasoning);
                async move {
                    provider
                        .open_turn(request, include_reasoning_payloads)
                        .await
                }
            },
        ));

        (req_tx, resp_rx)
    }

    fn list_models(&self, config: Value) -> BoxFuture<'_, Result<Vec<ModelInfo>, LlmError>> {
        Box::pin(async move { Self::provider_from_config(&config)?.list_models().await })
    }
}

#[cfg(test)]
mod tests;
