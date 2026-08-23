//! The Kimi provider: this vendor's own coding endpoint, reached with a
//! device-flow authorization rather than a static credential.
//!
//! It speaks a chat-completions-shaped wire, and the split runs between the two
//! directions.
//!
//! Its **requests** do not compose with the shared base, and the reason is one
//! field. This vendor carries reasoning in a sibling field of its own and
//! requires that field on every assistant message once reasoning is enabled —
//! the shared base folds reasoning into the content instead, which this
//! endpoint rejects. So this module keeps a request encoder of its own. It
//! consumes the same neutral messages as every other vendor — blocks never
//! cross the provider boundary — and the reasoning fold into the sibling field
//! happens inside that encoder, where vendor difference belongs.
//!
//! Its **responses** are ordinary chat-completions events, so they are read by
//! the shared decoder. A parser of its own is what silently lost events: the
//! two decoders drifted, and every fix made for the other vendors stopped at
//! the module boundary.

use std::future::Future;

use serde_json::Value;
use tracing::{info, warn};

use crate::store::{StoreError, StoreTx};

use super::bind;
use super::http;
use super::types::{
    ContentPart, EventStream, LlmError, Message, MessageContent, MessageRole, ModelInfo,
    ModelSelector, ProviderRx, ProviderTx, ReasoningCapability, ReasoningLevel, ToolDefinition,
};
use super::{BoxFuture, ProviderModule};

pub mod oauth;
pub mod store;
mod wire;

use oauth::{OAuthError, ensure_fresh_token};
use store::{KimiConfig, KimiStore};
use wire::{
    KimiThinking, WireFunction, WireMessage, WireModel, WireModelsResponse, WireRequest, WireTool,
    WireToolCall, WireToolCallFunction, normalize_base_url,
};

const DEFAULT_BASE_URL: &str = "https://api.kimi.com/coding/v1";

/// The model this provider falls back to for cheap background work.
const LIGHTWEIGHT_MODEL: &str = "kimi-for-coding";

/// What a caller is told when the stored authorization is no longer accepted.
const SESSION_EXPIRED: &str = "The session has expired. Sign in to this provider again.";

/// One configured endpoint on this vendor's surface.
pub(crate) struct KimiProvider {
    client: reqwest::Client,
    access_token: String,
    base_url: String,
    prompt_cache_key: Option<String>,
    thinking: Option<KimiThinking>,
    reasoning_effort: Option<String>,
}

impl KimiProvider {
    /// Build a provider for one access token and, optionally, one endpoint.
    pub(crate) fn new(access_token: String, base_url: Option<String>) -> Self {
        Self {
            client: http::streaming_client(),
            access_token,
            base_url: base_url
                .map_or_else(|| DEFAULT_BASE_URL.to_string(), |u| normalize_base_url(&u)),
            prompt_cache_key: None,
            thinking: None,
            reasoning_effort: None,
        }
    }

    /// Key this provider's prompt cache to one conversation, so two
    /// conversations never share a cached prefix.
    fn with_conversation_id(mut self, conversation_id: i64) -> Self {
        self.prompt_cache_key = Some(conversation_id.to_string());
        self
    }

    /// Set the reasoning switch.
    fn with_thinking(mut self, thinking: Option<KimiThinking>) -> Self {
        self.thinking = thinking;
        self
    }

    /// Set the reasoning effort.
    fn with_reasoning_effort(mut self, effort: Option<String>) -> Self {
        self.reasoning_effort = effort;
        self
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    fn wire_tools(tools: &[ToolDefinition]) -> Option<Vec<WireTool>> {
        if tools.is_empty() {
            return None;
        }
        Some(
            tools
                .iter()
                .map(|t| WireTool {
                    r#type: "function".to_string(),
                    function: WireFunction {
                        name: t.name.clone(),
                        description: t.description.clone(),
                        parameters: t.parameters.clone(),
                    },
                })
                .collect(),
        )
    }

    /// Build a request from neutral messages, folding reasoning into this
    /// vendor's own field.
    fn build_request(
        &self,
        messages: &[Message],
        model: String,
        tools: &[ToolDefinition],
        stream: bool,
    ) -> WireRequest {
        let thinking_enabled = self
            .thinking
            .as_ref()
            .is_some_and(|t| t.r#type == "enabled");

        WireRequest {
            model,
            max_tokens: None,
            messages: messages_to_wire_messages(messages, thinking_enabled),
            tools: Self::wire_tools(tools),
            temperature: None,
            stream,
            // Counts only arrive when asked for on this dialect; a request
            // that never opts in leaves the decoder honestly reporting absent.
            stream_options: stream.then_some(wire::KimiStreamOptions {
                include_usage: true,
            }),
            prompt_cache_key: self.prompt_cache_key.clone(),
            thinking: self.thinking.clone(),
            reasoning_effort: self.reasoning_effort.clone(),
        }
    }

    /// Every model this endpoint offers.
    async fn list_models(&self) -> Result<Vec<ModelInfo>, LlmError> {
        let url = format!("{}/models", self.base_url);
        info!(url, "fetching kimi model list");

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.access_token)
            .headers(wire::build_headers())
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
            warn!(status, body = %text, "kimi list_models failed");
            return Err(LlmError::Api {
                status,
                message: text,
            });
        }

        let wire: WireModelsResponse = response.json().await?;
        let mut models: Vec<ModelInfo> = wire
            .data
            .into_iter()
            .map(|m| ModelInfo {
                reasoning: kimi_reasoning(&m),
                name: m.display_name.clone().unwrap_or_else(|| m.id.clone()),
                id: m.id,
                vendor: "Kimi".to_string(),
            })
            .collect();
        models.sort_by(|a, b| a.name.cmp(&b.name));
        info!(count = models.len(), "fetched kimi models");
        Ok(models)
    }

    /// Open one streaming turn from neutral messages.
    fn stream_turn(
        &self,
        messages: &[Message],
        model: String,
        tools: &[ToolDefinition],
    ) -> EventStream {
        let body = self.build_request(messages, model, tools, true);
        wire::open_stream(
            self.client.clone(),
            self.access_token.clone(),
            self.endpoint(),
            body,
        )
    }
}

/// Encode neutral messages into this vendor's wire messages.
///
/// The vendor difference lives HERE and nowhere earlier: reasoning parts fold
/// into the endpoint's own sibling field — required on every assistant message
/// once reasoning is enabled — and a tool-result part becomes a message of
/// this wire's tool role, because the neutral layer has no tool voice while
/// this wire does. What a block contributes at all is the neutral projection's
/// answer, made before the messages reach any vendor.
fn messages_to_wire_messages(messages: &[Message], thinking_enabled: bool) -> Vec<WireMessage> {
    let mut wire_messages = Vec::new();
    for message in messages {
        match message.role {
            MessageRole::System => push_system(message, &mut wire_messages),
            MessageRole::User => push_user(message, &mut wire_messages),
            MessageRole::Assistant => {
                push_assistant(message, thinking_enabled, &mut wire_messages);
            }
        }
    }
    wire_messages
}

/// A message's plain-string content on this wire: the joined text form as-is,
/// or a parts message's text parts joined the way the text mode joins.
fn text_content(message: &Message) -> String {
    match &message.content {
        MessageContent::Text(text) => text.clone(),
        MessageContent::Parts(parts) => parts
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
    }
}

fn push_system(message: &Message, out: &mut Vec<WireMessage>) {
    let text = text_content(message);
    if !text.is_empty() {
        out.push(WireMessage {
            role: "system".to_string(),
            content: Some(text),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        });
    }
}

/// A user-voiced message. Each tool-result part becomes a message of this
/// wire's tool role, keyed on its call; a result naming no call is skipped
/// with a warning, because an unkeyed tool message is rejected outright. The
/// text contribution, when there is one, follows as a user message.
fn push_user(message: &Message, out: &mut Vec<WireMessage>) {
    if let MessageContent::Parts(parts) = &message.content {
        for part in parts {
            let ContentPart::ToolResult {
                tool_use_id,
                content,
            } = part
            else {
                continue;
            };
            if tool_use_id.is_empty() {
                // The two facts that make the skip actionable: the id exactly
                // as it arrived, and enough of what the part carried to find
                // the result it came from.
                warn!(
                    tool_use_id = %tool_use_id,
                    content_preview = %content.chars().take(120).collect::<String>(),
                    "a tool result names no call, so it is skipped"
                );
                continue;
            }
            out.push(WireMessage {
                role: "tool".to_string(),
                content: Some(content.clone()),
                reasoning_content: None,
                tool_calls: None,
                tool_call_id: Some(tool_use_id.clone()),
            });
        }
    }
    let text = text_content(message);
    if !text.is_empty() {
        out.push(WireMessage {
            role: "user".to_string(),
            content: Some(text),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        });
    }
}

fn push_assistant(message: &Message, thinking_enabled: bool, out: &mut Vec<WireMessage>) {
    let mut text_parts: Vec<&str> = Vec::new();
    let mut reasoning_parts: Vec<&str> = Vec::new();
    let mut tool_calls = Vec::new();

    match &message.content {
        MessageContent::Text(text) => {
            if !text.is_empty() {
                text_parts.push(text);
            }
        }
        MessageContent::Parts(parts) => {
            for part in parts {
                match part {
                    ContentPart::Text { text } => text_parts.push(text),
                    // The sibling field carries the text; this wire has no
                    // continuity payload of its own to replay.
                    ContentPart::Reasoning { text, .. } => reasoning_parts.push(text),
                    ContentPart::ToolUse { id, name, input } => tool_calls.push(WireToolCall {
                        id: id.clone(),
                        r#type: "function".to_string(),
                        function: WireToolCallFunction {
                            name: name.clone(),
                            arguments: serde_json::to_string(input).unwrap_or_default(),
                        },
                    }),
                    ContentPart::ToolResult { .. } => {}
                }
            }
        }
    }

    let reasoning_content = if reasoning_parts.is_empty() {
        // The endpoint requires the field whenever reasoning is on, and there
        // is none for this turn: an empty string goes out rather than the
        // field being omitted.
        thinking_enabled.then(String::new)
    } else {
        Some(reasoning_parts.join("\n\n"))
    };

    // An assistant message is valid only with content or calls. Reasoning is a
    // rider on those, never a payload of its own: a lone reasoning contribution
    // would otherwise become an empty message the endpoint rejects.
    if !text_parts.is_empty() || !tool_calls.is_empty() {
        out.push(WireMessage {
            role: "assistant".to_string(),
            content: if text_parts.is_empty() {
                None
            } else {
                Some(text_parts.join("\n\n"))
            },
            reasoning_content,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            tool_call_id: None,
        });
    }
}

/// Derive reasoning levels from a model's self-described support.
///
/// The thinking-type signal is primary: always-on offers no "off", toggleable
/// does, and none offers nothing. The boolean is the fallback when the signal is
/// absent.
fn kimi_reasoning(model: &WireModel) -> ReasoningCapability {
    use ReasoningLevel::{High, Low, Medium, Off};
    let always_on = || ReasoningCapability::new(vec![Low, Medium, High]);
    let toggleable = || ReasoningCapability::new(vec![Off, Low, Medium, High]);

    match model.supports_thinking_type.as_deref() {
        Some("no") => ReasoningCapability::default(),
        Some("only") => always_on(),
        Some("both") => toggleable(),
        _ if model.supports_reasoning == Some(true) => toggleable(),
        _ => ReasoningCapability::default(),
    }
}

/// Translate a selected level into this vendor's switch-and-effort pair.
///
/// It accepts three efforts, so the smallest collapses to the lowest and the
/// largest two to the highest. "The model decides" sends neither, deferring.
fn kimi_options_for_level(level: ReasoningLevel) -> (Option<KimiThinking>, Option<String>) {
    let enabled = || {
        Some(KimiThinking {
            r#type: "enabled".to_string(),
        })
    };
    match level {
        ReasoningLevel::Off => (
            Some(KimiThinking {
                r#type: "disabled".to_string(),
            }),
            None,
        ),
        ReasoningLevel::Auto => (None, None),
        ReasoningLevel::Minimal | ReasoningLevel::Low => (enabled(), Some("low".to_string())),
        ReasoningLevel::Medium => (enabled(), Some("medium".to_string())),
        ReasoningLevel::High | ReasoningLevel::XHigh | ReasoningLevel::Max => {
            (enabled(), Some("high".to_string()))
        }
    }
}

fn resolve_thinking(config: &Value) -> Option<KimiThinking> {
    let t = config.get("thinking")?.get("type")?.as_str()?;
    (t == "enabled" || t == "disabled").then(|| KimiThinking {
        r#type: t.to_string(),
    })
}

fn resolve_reasoning_effort(config: &Value) -> Option<String> {
    let effort = config.get("reasoning_effort")?.as_str()?.to_lowercase();
    match effort.as_str() {
        "xhigh" | "max" => Some("high".to_string()),
        "off" | "auto" | "low" | "medium" | "high" => Some(effort),
        _ => None,
    }
}

/// Read the switch-and-effort pair out of an instance's configuration.
fn resolve_kimi_options(config: &Value) -> (Option<KimiThinking>, Option<String>) {
    match resolve_reasoning_effort(config).as_deref() {
        Some("auto") => (None, None),
        Some("off") => (
            Some(KimiThinking {
                r#type: "disabled".to_string(),
            }),
            None,
        ),
        Some(effort) => {
            let thinking = resolve_thinking(config).unwrap_or_else(|| KimiThinking {
                r#type: "enabled".to_string(),
            });
            (Some(thinking), Some(effort.to_string()))
        }
        None => (resolve_thinking(config), None),
    }
}

fn config_error(e: &OAuthError) -> LlmError {
    if matches!(e, OAuthError::SessionExpired) {
        LlmError::Config(SESSION_EXPIRED.to_string())
    } else {
        LlmError::Config(e.to_string())
    }
}

/// The Kimi provider module, as the registry holds it.
pub struct KimiModule {
    store: KimiStore,
}

impl KimiModule {
    /// Create the module and its configuration table.
    ///
    /// # Panics
    ///
    /// If the table cannot be created.
    pub async fn new(tx: StoreTx) -> Self {
        let store = KimiStore::new(tx)
            .await
            .expect("kimi domain migration failed");
        Self { store }
    }

    /// Build a provider from an instance's configuration, refreshing the token
    /// if it is due.
    async fn build_provider(&self, config: &Value) -> Result<KimiProvider, LlmError> {
        let (token, _, _) = ensure_fresh_token(
            config["access_token"].as_str().map(String::from),
            config["refresh_token"].as_str().map(String::from),
            config["expires_at"].as_i64(),
        )
        .await
        .map_err(|e| config_error(&e))?;

        let (thinking, reasoning_effort) = resolve_kimi_options(config);
        Ok(
            KimiProvider::new(token, config["base_url"].as_str().map(String::from))
                .with_thinking(thinking)
                .with_reasoning_effort(reasoning_effort),
        )
    }
}

impl ProviderModule for KimiModule {
    fn type_id(&self) -> &'static str {
        "kimi-for-coding"
    }

    fn display_name(&self) -> &'static str {
        "Kimi for Coding"
    }

    fn description(&self) -> &'static str {
        "Kimi models, reached with a device-flow sign-in"
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
            let typed: KimiConfig =
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
        conversation_id: i64,
        provider_id: String,
        config: Value,
    ) -> (ProviderTx, ProviderRx) {
        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
        let (config_thinking, config_effort) = resolve_kimi_options(&config);
        let store = self.store.clone();

        tokio::spawn(bind::run_http_bind_loop(
            req_rx,
            resp_tx,
            move |messages, selector, tools, reasoning| {
                let model = match selector {
                    ModelSelector::Specific(m) => m,
                    ModelSelector::Lightweight { .. } => LIGHTWEIGHT_MODEL.into(),
                };

                // The conversation's selected level overrides the instance's.
                let (thinking, effort) = match reasoning {
                    Some(level) => kimi_options_for_level(level),
                    None => (config_thinking.clone(), config_effort.clone()),
                };
                let provider_id = provider_id.clone();
                let store = store.clone();
                let config = config.clone();

                async move {
                    let token = refresh_and_persist(&store, &provider_id, &config).await?;

                    let provider =
                        KimiProvider::new(token, config["base_url"].as_str().map(String::from))
                            .with_conversation_id(conversation_id)
                            .with_thinking(thinking)
                            .with_reasoning_effort(effort);

                    Ok(provider.stream_turn(&messages, model, &tools))
                }
            },
        ));

        (req_tx, resp_rx)
    }

    fn list_models(&self, config: Value) -> BoxFuture<'_, Result<Vec<ModelInfo>, LlmError>> {
        Box::pin(async move { self.build_provider(&config).await?.list_models().await })
    }

    fn auth_start(&self, _config: Value) -> BoxFuture<'_, Result<Value, LlmError>> {
        Box::pin(async move {
            let auth = oauth::start_device_auth()
                .await
                .map_err(|e| LlmError::Config(e.to_string()))?;
            Ok(serde_json::json!({
                "verification_uri": auth.verification_uri,
                "verification_uri_complete": auth.verification_uri_complete,
                "user_code": auth.user_code,
                "expires_in": auth.expires_in,
                "interval": auth.interval,
                "device_code": auth.device_code,
            }))
        })
    }

    fn auth_poll(&self, config: Value, poll_data: Value) -> BoxFuture<'_, Result<Value, LlmError>> {
        Box::pin(async move {
            let device = oauth::DeviceAuth {
                device_code: poll_data["device_code"].as_str().unwrap_or("").to_string(),
                user_code: poll_data["user_code"].as_str().unwrap_or("").to_string(),
                verification_uri: poll_data["verification_uri"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                verification_uri_complete: poll_data["verification_uri_complete"]
                    .as_str()
                    .map(String::from),
                expires_in: poll_data["expires_in"].as_u64().unwrap_or(600),
                interval: poll_data["interval"].as_u64().unwrap_or(5),
            };

            let tokens = oauth::poll_device_token(&device)
                .await
                .map_err(|e| LlmError::Config(e.to_string()))?;
            let expires_at = chrono::Utc::now().timestamp_millis()
                + i64::try_from(tokens.expires_in).unwrap_or(0) * 1000;

            let mut config: KimiConfig =
                serde_json::from_value(config).map_err(|e| LlmError::Config(e.to_string()))?;
            config.access_token = Some(tokens.access_token);
            config.refresh_token = Some(tokens.refresh_token);
            config.expires_at = Some(expires_at);

            serde_json::to_value(config).map_err(|e| LlmError::Config(e.to_string()))
        })
    }
}

/// Refresh the access token if it is due, and persist whatever came back.
///
/// The persistence is the point: the refresh token ROTATES, so keeping the old
/// one breaks the NEXT refresh — a failure that surfaces hours later as a
/// session that expired for no visible reason. A persistence failure is logged
/// and the turn continues, because the token in hand still works.
async fn refresh_and_persist(
    store: &KimiStore,
    provider_id: &str,
    bind_config: &Value,
) -> Result<String, LlmError> {
    refresh_and_persist_with(store, provider_id, bind_config, ensure_fresh_token).await
}

/// The body of [`refresh_and_persist`], with the refresh itself as a seam.
///
/// Two things are load-bearing here.
///
/// **The credentials are read from the STORE**, not from the copy the binding
/// captured when it was created. That copy is frozen: it still holds the
/// refresh token this binding already spent, so a second refresh over the same
/// binding would replay a dead one — and the session would expire for no
/// visible reason, hours after the turn that actually consumed it. The
/// bind-time copy is the fallback for an instance with no stored row.
///
/// **The new access token is persisted too**, not just the rotated refresh
/// token and the expiry. Storing a fresh expiry beside a stale access token
/// would tell the next turn its dead token is good for another hour.
///
/// The seam exists because the refresh itself is a network call, and what needs
/// pinning is which token the SECOND refresh is handed.
async fn refresh_and_persist_with<F, Fut>(
    store: &KimiStore,
    provider_id: &str,
    bind_config: &Value,
    refresh: F,
) -> Result<String, LlmError>
where
    F: FnOnce(Option<String>, Option<String>, Option<i64>) -> Fut,
    Fut: Future<Output = Result<(String, Option<String>, Option<i64>), OAuthError>>,
{
    let stored = match store.get_config(provider_id.to_string()).await {
        Ok(stored) => stored,
        Err(e) => {
            warn!(error = %e, "could not read the stored authorization, using the bound copy");
            None
        }
    };

    let mut config = stored.unwrap_or_else(|| KimiConfig {
        base_url: bind_config["base_url"].as_str().map(String::from),
        access_token: bind_config["access_token"].as_str().map(String::from),
        refresh_token: bind_config["refresh_token"].as_str().map(String::from),
        expires_at: bind_config["expires_at"].as_i64(),
    });

    let original_refresh = config.refresh_token.clone();
    let original_expires = config.expires_at;

    let (token, new_refresh, new_expires) = refresh(
        config.access_token.clone(),
        original_refresh.clone(),
        original_expires,
    )
    .await
    .map_err(|e| config_error(&e))?;

    if new_refresh != original_refresh || new_expires != original_expires {
        config.access_token = Some(token.clone());
        if let Some(rotated) = new_refresh {
            config.refresh_token = Some(rotated);
        }
        if let Some(expires_at) = new_expires {
            config.expires_at = Some(expires_at);
        }
        if let Err(e) = store.save_config(provider_id.to_string(), config).await {
            warn!(error = %e, "failed to persist the rotated tokens");
        }
    }

    Ok(token)
}

#[cfg(test)]
mod tests;
