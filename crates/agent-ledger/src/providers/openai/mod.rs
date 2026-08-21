//! The `OpenAI` provider, speaking the Responses API.
//!
//! This vendor's own surface is the Responses API; the chat-completions shape
//! lives on in the shared base for the vendors that still speak it. The type id
//! is unchanged from when this module spoke that older surface: the wire
//! changed, the provider identity did not, and every persisted instance names
//! the identity.

use serde_json::Value;
use tracing::{debug, info, warn};

use crate::store::{StoreError, StoreTx};

use super::bind::{self, OpenedTurn};
use super::http;
use super::http_store::{HttpProviderConfig, HttpProviderStore};
use super::render::blocks_to_messages;
use super::types::{
    CompletionRequest, ContentPart, LlmError, Message, MessageContent, MessageRole, ModelInfo,
    ModelSelector, OpaquePayload, ProviderRx, ProviderTx, ReasoningCapability, ReasoningLevel,
};
use super::{BoxFuture, ProviderModule};

mod parser;
mod wire;

use wire::{
    InputItem, ReasoningParam, ReasoningSummaryText, ResponsesRequest, ResponsesTool,
    WireModelsResponse,
};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// The model this provider falls back to for cheap background work.
const LIGHTWEIGHT_MODEL: &str = "gpt-5-nano";

/// One configured endpoint on this vendor's own surface.
pub struct OpenAiResponsesProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl OpenAiResponsesProvider {
    /// Build a provider for one credential and, optionally, one endpoint.
    #[must_use]
    pub fn new(api_key: String, base_url: Option<String>) -> Self {
        Self {
            client: http::streaming_client(),
            api_key,
            base_url: base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
        }
    }

    fn endpoint(&self) -> String {
        format!("{}/responses", self.base_url)
    }

    /// Build the request body. The policy lives here, in one place:
    ///
    /// - **Nothing is retained server-side**, unconditionally and regardless of
    ///   model. State that lives at a vendor is state a fork cannot branch and a
    ///   deletion cannot reach.
    /// - The reasoning parameters go out **only for reasoning-capable models**,
    ///   because this API rejects the reasoning object outright on a model that
    ///   does not reason — and those models stay listed and selectable.
    fn build_request_body(
        request: &CompletionRequest,
        stream: bool,
        include_reasoning_payloads: bool,
    ) -> (ResponsesRequest, bool) {
        let (instructions, input, carried_payloads) =
            convert_input(&request.messages, include_reasoning_payloads);

        let tools: Vec<ResponsesTool> = request
            .tools
            .iter()
            .map(|t| ResponsesTool {
                r#type: "function",
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.parameters.clone(),
            })
            .collect();

        let reasoning_capable = !openai_reasoning_for_slug(&request.model).is_empty();

        let body = ResponsesRequest {
            model: request.model.clone(),
            input,
            instructions,
            tools: if tools.is_empty() { None } else { Some(tools) },
            max_output_tokens: request.max_tokens,
            temperature: request.temperature,
            stream,
            store: false,
            reasoning: reasoning_capable.then(|| ReasoningParam {
                summary: "auto",
                effort: request.reasoning.and_then(openai_reasoning_effort),
            }),
            include: reasoning_capable.then(|| vec!["reasoning.encrypted_content"]),
        };
        (body, carried_payloads)
    }

    /// Open one streaming turn.
    ///
    /// # Errors
    ///
    /// Never at this point — a transport or API failure arrives as the stream's
    /// first item, where the bind loop can classify it.
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
            "responses stream request"
        );
        debug!(body = ?serde_json::to_string(&request.messages), "request messages");

        let (body, carried_payloads) =
            Self::build_request_body(&request, true, include_reasoning_payloads);

        Ok(OpenedTurn {
            events: wire::open_stream(
                self.client.clone(),
                self.api_key.clone(),
                self.endpoint(),
                body,
            ),
            carried_payloads,
        })
    }

    /// Every model this endpoint offers.
    ///
    /// # Errors
    ///
    /// If the request fails or the provider refuses it.
    pub async fn list_models(&self) -> Result<Vec<ModelInfo>, LlmError> {
        let url = format!("{}/models", self.base_url);
        info!(url, "fetching model list");

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.api_key)
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
        let vendor = if self.base_url == DEFAULT_BASE_URL {
            "OpenAI"
        } else {
            "OpenAI (custom)"
        };
        let mut models: Vec<ModelInfo> = wire
            .data
            .into_iter()
            .map(|m| ModelInfo {
                reasoning: openai_reasoning_for_slug(&m.id),
                name: m.name.unwrap_or_else(|| m.id.clone()),
                id: m.id,
                vendor: vendor.to_string(),
            })
            .collect();
        models.sort_by(|a, b| a.name.cmp(&b.name));
        info!(count = models.len(), "fetched models");
        Ok(models)
    }
}

/// Convert neutral messages into this API's instructions string plus its typed
/// input items. System content becomes the top-level instructions, never an
/// input item.
///
/// **Item order is preserved exactly.** A reasoning item must be immediately
/// followed by its own required item, so bucketing by type and concatenating
/// would corrupt a turn holding two reasoning-and-call pairs: each reasoning
/// item would be stranded from the call it belongs to, and the whole request is
/// rejected. Assistant text buffers into one message flushed at its own
/// position, ahead of the next ordering item.
///
/// A reasoning part carrying THIS vendor's payload — gated on that exact variant
/// and on the knob — replays as a reasoning item preserving the server-assigned
/// id, the visible summary, the constant-null status and the encrypted blob
/// verbatim. Anything else folds into the message text: a foreign payload is
/// omitted, never adapted.
fn convert_input(
    messages: &[Message],
    include_reasoning_payloads: bool,
) -> (Option<String>, Vec<InputItem>, bool) {
    let mut instructions: Vec<String> = Vec::new();
    let mut items: Vec<InputItem> = Vec::new();
    let mut carried_payloads = false;

    for msg in messages {
        let role = match msg.role {
            MessageRole::System => {
                match &msg.content {
                    MessageContent::Text(text) => instructions.push(text.clone()),
                    MessageContent::Parts(parts) => {
                        for part in parts {
                            if let ContentPart::Text { text }
                            | ContentPart::Reasoning { text, .. } = part
                            {
                                instructions.push(text.clone());
                            }
                        }
                    }
                }
                continue;
            }
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
        };

        match &msg.content {
            MessageContent::Text(text) => items.push(InputItem::Message {
                role,
                content: text.clone(),
            }),
            MessageContent::Parts(parts) => {
                convert_parts(
                    parts,
                    role,
                    include_reasoning_payloads,
                    &mut items,
                    &mut carried_payloads,
                );
            }
        }
    }

    let instructions = if instructions.is_empty() {
        None
    } else {
        Some(instructions.join("\n\n"))
    };
    (instructions, items, carried_payloads)
}

/// Flush buffered assistant text as one message, at the position it occupies, so
/// a following item keeps its place in the sequence.
fn flush(role: &'static str, pending_text: &mut Vec<String>, items: &mut Vec<InputItem>) {
    if !pending_text.is_empty() {
        items.push(InputItem::Message {
            role,
            content: pending_text.join("\n"),
        });
        pending_text.clear();
    }
}

/// Emit one group's parts at their ORIGINAL positions.
fn convert_parts(
    parts: &[ContentPart],
    role: &'static str,
    include_reasoning_payloads: bool,
    items: &mut Vec<InputItem>,
    carried_payloads: &mut bool,
) {
    let mut pending_text: Vec<String> = Vec::new();

    for part in parts {
        match part {
            ContentPart::Text { text } => pending_text.push(text.clone()),
            ContentPart::Reasoning { text, opaque } => match opaque {
                Some(OpaquePayload::OpenAiResponses {
                    item_id,
                    encrypted_content,
                }) if include_reasoning_payloads => {
                    flush(role, &mut pending_text, items);
                    *carried_payloads = true;
                    items.push(InputItem::Reasoning {
                        id: item_id.clone(),
                        summary: vec![ReasoningSummaryText {
                            r#type: "summary_text",
                            text: text.clone(),
                        }],
                        // Constant null, hardcoded and never stored: it is not a
                        // fact about the conversation.
                        status: None,
                        encrypted_content: encrypted_content.clone(),
                    });
                }
                // Absent, foreign, or suppressed: fold the text, omit the blob.
                _ => pending_text.push(text.clone()),
            },
            ContentPart::ToolUse { id, name, input } => {
                flush(role, &mut pending_text, items);
                items.push(InputItem::FunctionCall {
                    call_id: id.clone(),
                    name: name.clone(),
                    arguments: serde_json::to_string(input).unwrap_or_default(),
                });
            }
            // A tool failure is a result whose output carries the failure text —
            // the render pass already folded it into a result part, as every
            // vendor sees it.
            ContentPart::ToolResult {
                tool_use_id,
                content,
            } => {
                flush(role, &mut pending_text, items);
                items.push(InputItem::FunctionCallOutput {
                    call_id: tool_use_id.clone(),
                    output: content.clone(),
                });
            }
        }
    }

    flush(role, &mut pending_text, items);
}

/// Translate a selected level into this vendor's effort value.
///
/// Off, auto and max have no representation here and are never advertised for
/// this vendor, so they defer to the model's default rather than sending a value
/// the API would reject.
fn openai_reasoning_effort(level: ReasoningLevel) -> Option<String> {
    match level {
        ReasoningLevel::Minimal => Some("minimal"),
        ReasoningLevel::Low => Some("low"),
        ReasoningLevel::Medium => Some("medium"),
        ReasoningLevel::High => Some("high"),
        ReasoningLevel::XHigh => Some("xhigh"),
        ReasoningLevel::Off | ReasoningLevel::Auto | ReasoningLevel::Max => None,
    }
    .map(str::to_string)
}

/// This vendor's model list carries no capability data, so reasoning levels come
/// from a slug map over the documented effort vocabulary.
///
/// The slug is normalized first: a trailing dated snapshot suffix is stripped so
/// a pinned date shares its family's entry, while the identity suffixes stay.
/// The map is also the request policy's capability gate — an empty capability
/// means no reasoning object and no include on the request.
fn openai_reasoning_for_slug(external_id: &str) -> ReasoningCapability {
    use ReasoningLevel::{High, Low, Medium, Minimal, XHigh};
    let slug = normalize_openai_slug(external_id);

    // The newest families add the extra tier on top of the standard ones.
    if slug.starts_with("gpt-5.4") || slug.starts_with("gpt-5.5") {
        return ReasoningCapability::new(vec![Minimal, Low, Medium, High, XHigh]);
    }
    if slug.starts_with("gpt-5") {
        return ReasoningCapability::new(vec![Minimal, Low, Medium, High]);
    }
    // The earlier reasoning models: three tiers, no minimal.
    if slug.starts_with("o1") || slug.starts_with("o3") || slug.starts_with("o4") {
        return ReasoningCapability::new(vec![Low, Medium, High]);
    }
    ReasoningCapability::default()
}

/// Strip a trailing dated snapshot suffix so a dated pin resolves to its family.
fn normalize_openai_slug(external_id: &str) -> &str {
    const DATED_LEN: usize = 11;
    if external_id.len() > DATED_LEN {
        let tail = &external_id[external_id.len() - DATED_LEN..];
        let t = tail.as_bytes();
        if t[0] == b'-'
            && t[1..5].iter().all(u8::is_ascii_digit)
            && t[5] == b'-'
            && t[6..8].iter().all(u8::is_ascii_digit)
            && t[8] == b'-'
            && t[9..11].iter().all(u8::is_ascii_digit)
        {
            return &external_id[..external_id.len() - DATED_LEN];
        }
    }
    external_id
}

/// The `OpenAI` provider module, as the registry holds it.
pub struct OpenAiModule {
    store: HttpProviderStore,
}

impl OpenAiModule {
    /// Create the module and its configuration table.
    ///
    /// The table is the same one this provider used before it moved to the
    /// newer wire surface, so existing instances keep working untouched.
    ///
    /// # Panics
    ///
    /// If the table cannot be created.
    pub async fn new(tx: StoreTx) -> Self {
        let store = HttpProviderStore::new(tx, "openai", "provider_openai")
            .await
            .expect("openai domain migration failed");
        Self { store }
    }

    fn provider_from_config(config: &Value) -> Result<OpenAiResponsesProvider, LlmError> {
        let api_key = config["api_key"]
            .as_str()
            .ok_or_else(|| LlmError::MissingKey("openai".into()))?
            .to_string();
        let base_url = config["base_url"].as_str().map(String::from);
        Ok(OpenAiResponsesProvider::new(api_key, base_url))
    }
}

impl ProviderModule for OpenAiModule {
    fn type_id(&self) -> &'static str {
        "openai"
    }

    fn display_name(&self) -> &'static str {
        "OpenAI"
    }

    fn description(&self) -> &'static str {
        "OpenAI models via the Responses API"
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
            move |blocks, selector, tools, reasoning, include_reasoning_payloads| {
                let model = match selector {
                    ModelSelector::Specific(m) => m,
                    ModelSelector::Lightweight => LIGHTWEIGHT_MODEL.into(),
                };
                let provider = OpenAiResponsesProvider::new(api_key.clone(), base_url.clone());
                let request = CompletionRequest {
                    model,
                    messages: blocks_to_messages(&blocks),
                    tools,
                    max_tokens: None,
                    temperature: None,
                    stream: true,
                    reasoning,
                };
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
