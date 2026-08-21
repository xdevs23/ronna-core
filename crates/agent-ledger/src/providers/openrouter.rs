//! The gateway provider: many vendors' models behind one chat-completions
//! endpoint.
//!
//! It composes with the shared chat base and adds exactly two things of its
//! own: the reasoning echo on assistant request messages, which this endpoint
//! documents as required for keeping a tool-call chain intact, and a capability
//! reading taken from each model's published reasoning descriptor.
//!
//! The ranking headers the source of this code sent — a referring URL and a
//! product title — are not here. They identify an application to the gateway's
//! public leaderboard, which is a consumer's business and not a library's.

use serde_json::Value;

use crate::store::{StoreError, StoreTx};

use super::bind;
use super::chat::{ChatProvider, WireModel, provider_from_config};
use super::http_store::{HttpProviderConfig, HttpProviderStore};
use super::render::blocks_to_messages;
use super::types::{
    CompletionRequest, LlmError, ModelInfo, ModelSelector, ProviderRx, ProviderTx,
    ReasoningCapability, ReasoningLevel,
};
use super::{BoxFuture, ProviderModule};

const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// The slug used for cheap background work, such as deriving a title.
///
/// It must be a CURRENT, live listing. Its predecessor was delisted and then
/// failed every background request with "no endpoints found" — a deterministic
/// failure the retry loop hammered, because a delisted model looks exactly like
/// a temporarily unavailable one. Verify any replacement against the gateway's
/// public model list before landing it.
pub const OPENROUTER_LIGHTWEIGHT_MODEL: &str = "google/gemini-3.1-flash-lite";

/// The chat base configured for this gateway.
fn openrouter_provider(api_key: String, base_url: Option<String>) -> ChatProvider {
    ChatProvider::with_headers(
        api_key,
        base_url,
        OPENROUTER_BASE_URL,
        reqwest::header::HeaderMap::new(),
    )
    .with_details_echo()
}

/// The reasoning a model exposes, read from its published descriptor:
///
/// - no descriptor means no effort selection, so no control is offered;
/// - a populated effort list means offer exactly those levels;
/// - a descriptor with no effort list means the gateway accepts any effort, so
///   offer the portable three.
///
/// Reading the descriptor rather than inferring from a parameter list is what
/// keeps the offered set honest: a model that accepts an effort parameter is not
/// the same as a model that does anything with it.
fn openrouter_reasoning(model: &WireModel) -> ReasoningCapability {
    let Some(reasoning) = &model.reasoning else {
        return ReasoningCapability::default();
    };
    match &reasoning.supported_efforts {
        Some(efforts) => ReasoningCapability::new(
            efforts
                .iter()
                .filter_map(|e| ReasoningLevel::from_key(e))
                .collect(),
        ),
        None => ReasoningCapability::new(vec![
            ReasoningLevel::Low,
            ReasoningLevel::Medium,
            ReasoningLevel::High,
        ]),
    }
}

/// The gateway provider module, as the registry holds it.
pub struct OpenRouterModule {
    store: HttpProviderStore,
}

impl OpenRouterModule {
    /// Create the module and its configuration table.
    ///
    /// # Panics
    ///
    /// If the table cannot be created.
    pub async fn new(tx: StoreTx) -> Self {
        let store = HttpProviderStore::new(tx, "openrouter", "provider_openrouter")
            .await
            .expect("openrouter domain migration failed");
        Self { store }
    }
}

impl ProviderModule for OpenRouterModule {
    fn type_id(&self) -> &'static str {
        "openrouter"
    }

    fn display_name(&self) -> &'static str {
        "OpenRouter"
    }

    fn description(&self) -> &'static str {
        "Many vendors' models through one endpoint"
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
                    ModelSelector::Lightweight => OPENROUTER_LIGHTWEIGHT_MODEL.into(),
                };
                let provider = openrouter_provider(api_key.clone(), base_url.clone());
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
        Box::pin(async move {
            provider_from_config(&config, "openrouter", openrouter_provider)?
                .list_models_with(openrouter_reasoning)
                .await
        })
    }
}

#[cfg(test)]
mod tests;
