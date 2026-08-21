//! The Kimi provider: this vendor's own coding endpoint, reached with a
//! device-flow authorization rather than a static credential.
//!
//! It speaks a chat-completions-shaped wire but does **not** compose with the
//! shared base, and the reason is one field. This vendor carries reasoning in a
//! sibling field of its own and requires that field on every assistant message
//! once reasoning is enabled — the shared base folds reasoning into the content
//! instead, which this endpoint rejects. So this module builds its messages from
//! blocks directly.
//!
//! That direct build is the one place in this library where a module reads block
//! types by name. It is a deliberate, contained exception with a cost: a block
//! kind added elsewhere is invisible to this vendor until it is named here.

use serde_json::Value;
use tracing::{info, warn};

use crate::agency::{BlockKind, Projection};
use crate::block::{Block, Role};
use crate::store::{StoreError, StoreTx};

use super::bind;
use super::http;
use super::types::{
    EventStream, LlmError, ModelInfo, ModelSelector, ProviderRx, ProviderTx, ReasoningCapability,
    ReasoningLevel, ToolDefinition,
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

    /// Build a request straight from blocks, preserving reasoning in this
    /// vendor's own field.
    fn build_request_from_blocks(
        &self,
        blocks: &[Block],
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
            messages: blocks_to_wire_messages(blocks, thinking_enabled),
            tools: Self::wire_tools(tools),
            temperature: None,
            stream,
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

    /// Open one streaming turn from blocks, preserving reasoning.
    fn stream_from_blocks(
        &self,
        blocks: &[Block],
        model: String,
        tools: &[ToolDefinition],
    ) -> EventStream {
        let body = self.build_request_from_blocks(blocks, model, tools, true);
        wire::open_stream(
            self.client.clone(),
            self.access_token.clone(),
            self.endpoint(),
            body,
        )
    }
}

/// Convert blocks straight into this vendor's messages, preserving reasoning in
/// its own field.
///
/// Two hazards this shape exists to avoid, both of which produced rejected
/// requests:
///
/// **Dropped reasoning.** An unfinalized reasoning tail is the ONLY record of
/// reasoning when no finalized sibling exists, so excluding it loses an
/// interrupted turn's reasoning entirely. The priority is finalized reasoning
/// first, the tail as a fallback, then an empty string when reasoning is on,
/// then nothing.
///
/// **A split turn.** The grouping walk steps OVER a boundary-invisible block
/// within a same-role run rather than terminating on it. An unfinalized call
/// tail sitting between a reasoning tail and its committed call would otherwise
/// split one assistant turn into two messages, stranding the reasoning on the
/// wrong one.
fn blocks_to_wire_messages(blocks: &[Block], thinking_enabled: bool) -> Vec<WireMessage> {
    let mut wire_messages = Vec::new();
    let mut i = 0;

    while i < blocks.len() {
        let Some(role) = effective_role(&blocks[i]) else {
            i += 1;
            continue;
        };
        let start = i;
        let mut end = i;
        while i < blocks.len() {
            match effective_role(&blocks[i]) {
                Some(r) if r == role => {
                    i += 1;
                    end = i;
                }
                None => i += 1,
                Some(_) => break,
            }
        }
        let group = &blocks[start..end];

        match role {
            Role::System => push_system(group, &mut wire_messages),
            Role::User => push_user(group, &mut wire_messages),
            Role::Tool => push_tool(group, &mut wire_messages),
            Role::Assistant => push_assistant(group, thinking_enabled, &mut wire_messages),
        }
    }

    wire_messages
}

/// The role a block groups under for THIS vendor.
///
/// It differs from the projection role in one direction only: an unfinalized
/// reasoning tail stays in its assistant group, because it is the fallback
/// source for the reasoning field.
fn effective_role(block: &Block) -> Option<Role> {
    match block.block_type.as_str() {
        // Unfinalized text and call tails carry no persistable content: their
        // content is carried by their committed siblings.
        "streaming" | "streaming_tool_call" => None,
        "streaming_thinking" => Some(Role::Assistant),
        "tool_result" | "tool_error" => Some(Role::Tool),
        // The calendar entry is roleless on the row; on the projection axis it
        // is a system line, so it groups with system content and the model
        // learns the date.
        "date_marker" => Some(Role::System),
        _ => block.role,
    }
}

fn str_field<'a>(block: &'a Block, key: &str) -> &'a str {
    block.fields.get(key).and_then(Value::as_str).unwrap_or("")
}

fn push_system(group: &[Block], out: &mut Vec<WireMessage>) {
    let text: Vec<String> = group
        .iter()
        .filter_map(|b| match b.block_type.as_str() {
            "text" | "system_prompt" => Some(str_field(b, "content").to_string()),
            // The dated line is the block's own projection, never a second copy
            // of the same format string.
            "date_marker" => BlockKind::from_block(b).llm_text(),
            _ => None,
        })
        .collect();
    if !text.is_empty() {
        out.push(WireMessage {
            role: "system".to_string(),
            content: Some(text.join("\n\n")),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        });
    }
}

fn push_user(group: &[Block], out: &mut Vec<WireMessage>) {
    let text: Vec<String> = group
        .iter()
        .filter_map(|b| match b.block_type.as_str() {
            "text" => Some(str_field(b, "content").to_string()),
            "quote" => Some(format!("> {}", str_field(b, "text"))),
            "code" => {
                let lang = b
                    .fields
                    .get("language")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                Some(format!("```{lang}\n{}\n```", str_field(b, "content")))
            }
            _ => None,
        })
        .collect();
    if !text.is_empty() {
        out.push(WireMessage {
            role: "user".to_string(),
            content: Some(text.join("\n\n")),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        });
    }
}

fn push_tool(group: &[Block], out: &mut Vec<WireMessage>) {
    for block in group {
        let content = match block.block_type.as_str() {
            "tool_result" => str_field(block, "content").to_string(),
            "tool_error" => str_field(block, "error").to_string(),
            _ => continue,
        };
        let tool_call_id = str_field(block, "tool_call_id");
        if tool_call_id.is_empty() {
            warn!(
                block_id = block.id,
                block_type = %block.block_type,
                "a tool result names no call, so it is skipped"
            );
            continue;
        }
        out.push(WireMessage {
            role: "tool".to_string(),
            content: Some(content),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: Some(tool_call_id.to_string()),
        });
    }
}

fn push_assistant(group: &[Block], thinking_enabled: bool, out: &mut Vec<WireMessage>) {
    let mut text_parts = Vec::new();
    let mut reasoning_parts = Vec::new();
    let mut streaming_reasoning_parts = Vec::new();
    let mut tool_calls = Vec::new();

    for block in group {
        match block.block_type.as_str() {
            "text" => text_parts.push(str_field(block, "content").to_string()),
            "thinking" => reasoning_parts.push(str_field(block, "content").to_string()),
            "streaming_thinking" => {
                streaming_reasoning_parts.push(str_field(block, "content").to_string());
            }
            "tool_call" => {
                let input = str_field(block, "input");
                let input_value = serde_json::from_str(input)
                    .unwrap_or_else(|_| Value::String(input.to_string()));
                tool_calls.push(WireToolCall {
                    id: str_field(block, "tool_call_id").to_string(),
                    r#type: "function".to_string(),
                    function: WireToolCallFunction {
                        name: str_field(block, "name").to_string(),
                        arguments: serde_json::to_string(&input_value).unwrap_or_default(),
                    },
                });
            }
            _ => {}
        }
    }

    // Finalized reasoning is authoritative. The unfinalized tail is the
    // fallback when a turn was never finalized — an interrupted stream, or a
    // conversation from before finalization existed — so the reasoning is still
    // sent faithfully rather than discarded.
    let reasoning_content = if reasoning_parts.is_empty() {
        if streaming_reasoning_parts.is_empty() {
            // The endpoint requires the field whenever reasoning is on, and
            // there is none for this turn.
            thinking_enabled.then(String::new)
        } else {
            Some(streaming_reasoning_parts.join("\n\n"))
        }
    } else {
        Some(reasoning_parts.join("\n\n"))
    };

    // An assistant message is valid only with content or calls. Reasoning is a
    // rider on those, never a payload of its own: a lone reasoning block would
    // otherwise become an empty message the endpoint rejects.
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
            move |blocks, selector, tools, reasoning| {
                let model = match selector {
                    ModelSelector::Specific(m) => m,
                    ModelSelector::Lightweight => LIGHTWEIGHT_MODEL.into(),
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

                    Ok(provider.stream_from_blocks(&blocks, model, &tools))
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
    config: &Value,
) -> Result<String, LlmError> {
    let original_refresh = config["refresh_token"].as_str().map(String::from);
    let original_expires = config["expires_at"].as_i64();

    let (token, new_refresh, new_expires) = ensure_fresh_token(
        config["access_token"].as_str().map(String::from),
        original_refresh.clone(),
        original_expires,
    )
    .await
    .map_err(|e| config_error(&e))?;

    if new_refresh != original_refresh || new_expires != original_expires {
        let mut updated = config.clone();
        if let Some(ref rt) = new_refresh {
            updated["refresh_token"] = Value::String(rt.clone());
        }
        if let Some(exp) = new_expires {
            updated["expires_at"] = Value::Number(exp.into());
        }
        match serde_json::from_value::<KimiConfig>(updated) {
            Ok(typed) => {
                if let Err(e) = store.save_config(provider_id.to_string(), typed).await {
                    warn!(error = %e, "failed to persist the rotated tokens");
                }
            }
            Err(e) => {
                warn!(error = %e, "the updated config did not parse, so it was not persisted");
            }
        }
    }

    Ok(token)
}

#[cfg(test)]
mod tests;
