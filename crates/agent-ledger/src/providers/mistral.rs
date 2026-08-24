//! The Mistral provider.
//!
//! It speaks the chat-completions wire, so it composes with the shared base and
//! supplies only what differs — all of which is genuinely vendor-specific:
//!
//! - **Ingest.** While reasoning, the content field is a *list* of typed chunks
//!   rather than the plain string, decoded here. The exact wire shape is not
//!   documented, so the decoder is written defensively: unknown entries are
//!   skipped, and a plain-string variant is taken verbatim.
//! - **Effort.** The effort parameter is a hard binary. The API accepts two
//!   values and rejects anything else outright, so the translation collapses
//!   the whole level vocabulary onto those two and offers only the honest pair
//!   in the capability.
//! - **Capability.** The model list carries no reasoning metadata at all, so the
//!   capability comes from a slug map — the one case where a table is the only
//!   truthful answer available.

use reqwest::header::HeaderMap;
use serde::Serialize;
use serde_json::Value;

use crate::store::{StoreError, StoreTx};

use super::bind;
use super::chat::{
    AssistantTextPart, ChatProvider, SseState, WireMessageContent, decode_string_content,
    fold_assistant_content, provider_from_config,
};
use super::http_store::{HttpProviderConfig, HttpProviderStore};
use super::types::{
    CompletionRequest, LlmError, ModelInfo, ModelSelector, OpaquePayload, ProviderRx, ProviderTx,
    ReasoningCapability, ReasoningLevel, StreamEvent,
};
use super::{BoxFuture, ProviderModule};

const MISTRAL_BASE_URL: &str = "https://api.mistral.ai/v1";

/// The model this provider falls back to for cheap background work.
const LIGHTWEIGHT_MODEL: &str = "mistral-small-latest";

/// The chat base configured for this vendor: its endpoint, its binary effort,
/// its typed-chunk decoder, its unit continuity tag, its replay encoder, a fixed
/// vendor label — this vendor's model ids carry no vendor prefix — and its
/// refusal of an empty assistant message.
fn mistral_provider(api_key: String, base_url: Option<String>) -> ChatProvider {
    ChatProvider::with_headers(api_key, base_url, MISTRAL_BASE_URL, HeaderMap::new())
        .effort_translation(mistral_reasoning_effort)
        .content_decoder(decode_mistral_content)
        .thinking_end_payload(mistral_thinking_end_payload)
        .assistant_content_encoder(mistral_assistant_content)
        .vendor_label("Mistral")
        // This endpoint refuses an empty assistant message — it wants content or
        // tool calls — so the echo every other chat endpoint keeps is converted
        // away here, on the vendor's own side.
        .refuses_empty_assistant()
}

/// This vendor has no extra continuity blob: the stored reasoning text itself
/// is the payload, so a closing reasoning run carries just the unit tag. The tag
/// alone gates replay, and the chunk is rebuilt from the block's own content on
/// the next turn.
///
/// It always answers with a payload, and still returns an option because it
/// fills a seam whose other implementations do not.
#[allow(clippy::unnecessary_wraps)]
fn mistral_thinking_end_payload(_state: &mut SseState) -> Option<OpaquePayload> {
    Some(OpaquePayload::Mistral)
}

/// This vendor's typed content chunks. The module owns this shape; the base's
/// pass-through carries it opaquely and never constructs one.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MistralChunk {
    Thinking { thinking: Vec<MistralTextChunk> },
    Text { text: String },
}

#[derive(Serialize)]
struct MistralTextChunk {
    r#type: &'static str,
    text: String,
}

impl MistralTextChunk {
    fn new(text: &str) -> Self {
        Self {
            r#type: "text",
            text: text.to_string(),
        }
    }
}

/// This vendor's content encoder.
///
/// When the group carries reasoning tagged with this vendor's payload, and the
/// knob admits payloads, the assistant content becomes the typed chunk array,
/// echoing the reasoning chunk verbatim. Stripping it degrades the answer
/// measurably, which is why it is rebuilt rather than folded.
///
/// Otherwise it degrades to the base's plain text fold. A foreign payload is
/// omitted — its text becomes a plain text chunk — never adapted into this
/// vendor's shape.
fn mistral_assistant_content(
    parts: &[AssistantTextPart<'_>],
    include_reasoning_payloads: bool,
) -> (Option<WireMessageContent>, bool) {
    let replayable = include_reasoning_payloads
        && parts.iter().any(|p| {
            matches!(
                p,
                AssistantTextPart::Reasoning {
                    opaque: Some(OpaquePayload::Mistral),
                    ..
                }
            )
        });
    if !replayable {
        return fold_assistant_content(parts, include_reasoning_payloads);
    }

    let chunks = parts
        .iter()
        .map(|part| {
            let chunk = match part {
                AssistantTextPart::Reasoning {
                    text,
                    opaque: Some(OpaquePayload::Mistral),
                } => MistralChunk::Thinking {
                    thinking: vec![MistralTextChunk::new(text)],
                },
                AssistantTextPart::Reasoning { text, .. } | AssistantTextPart::Text(text) => {
                    MistralChunk::Text {
                        text: (*text).to_string(),
                    }
                }
            };
            serde_json::to_value(chunk).expect("a chunk of owned strings serializes")
        })
        .collect();
    (Some(WireMessageContent::Chunks(chunks)), true)
}

/// This vendor's effort is a hard binary: the API accepts exactly two values and
/// answers anything else with a validation error naming them.
///
/// Exhaustive over the level vocabulary, so a level added later cannot ship an
/// invalid value here. The maximum maps to the highest available effort. It is
/// sent explicitly whenever a level is selected, because the API's default when
/// the parameter is omitted is not documented.
///
/// It always answers with a value, and still returns an option because it fills
/// a seam whose other implementations defer for some levels.
#[allow(clippy::unnecessary_wraps)]
fn mistral_reasoning_effort(level: ReasoningLevel) -> Option<String> {
    let effort = match level {
        ReasoningLevel::High
        | ReasoningLevel::XHigh
        | ReasoningLevel::Medium
        | ReasoningLevel::Max => "high",
        ReasoningLevel::Off
        | ReasoningLevel::Low
        | ReasoningLevel::Minimal
        | ReasoningLevel::Auto => "none",
    };
    Some(effort.to_string())
}

/// This vendor's model list carries no reasoning metadata, so capability is a
/// slug map.
///
/// The picker gets only the honest pair, each mapping one-to-one onto the wire
/// binary. Offering a middle level that silently became the high one would be a
/// control that does not control anything.
fn mistral_reasoning_for_slug(model_id: &str) -> ReasoningCapability {
    match model_id {
        "mistral-medium-3-5" | "mistral-small-latest" => {
            ReasoningCapability::new(vec![ReasoningLevel::Off, ReasoningLevel::High])
        }
        _ => ReasoningCapability::default(),
    }
}

/// Decode this vendor's content field, defensively:
///
/// - a plain **string** — the answer phase, and the whole stream when reasoning
///   is off — decodes exactly like the base;
/// - a **list** of typed chunks is walked in order: a reasoning entry becomes a
///   reasoning delta, its own nested text chunks concatenated; a text entry
///   becomes a text delta and closes the open reasoning block at the first one.
///   Unknown entry types are tolerated and skipped.
///
/// It never depends on a single mixed transition chunk. Reasoning and text
/// entries may share one list or arrive split across any number of deltas, and
/// the reasoning block closes exactly once either way — the close is idempotent,
/// so the decoder does not have to know which case it is in.
fn decode_mistral_content(content: &Value, state: &mut SseState) -> Vec<StreamEvent> {
    let Some(entries) = content.as_array() else {
        return decode_string_content(content, state);
    };

    let mut events = vec![];
    for entry in entries {
        match entry["type"].as_str() {
            Some("thinking") => {
                let text = think_chunk_text(entry);
                if !text.is_empty() {
                    state.open_reasoning();
                    events.push(StreamEvent::ThinkingDelta { text });
                }
            }
            Some("text") => {
                if let Some(text) = entry["text"].as_str()
                    && !text.is_empty()
                {
                    events.extend(state.close_reasoning());
                    events.push(StreamEvent::TextDelta {
                        text: text.to_string(),
                    });
                }
            }
            _ => {}
        }
    }
    events
}

/// Concatenate a reasoning chunk's nested text entries. A plain-string form is
/// taken verbatim, because the wire shape here is inferred rather than
/// documented and guessing wrong should cost nothing.
fn think_chunk_text(entry: &Value) -> String {
    let thinking = &entry["thinking"];
    if let Some(chunks) = thinking.as_array() {
        return chunks.iter().filter_map(|c| c["text"].as_str()).collect();
    }
    thinking.as_str().unwrap_or_default().to_string()
}

/// The Mistral provider module, as the registry holds it.
pub struct MistralModule {
    store: HttpProviderStore,
}

impl MistralModule {
    /// Create the module and its configuration table.
    ///
    /// # Panics
    ///
    /// If the table cannot be created.
    pub async fn new(tx: StoreTx) -> Self {
        let store = HttpProviderStore::new(tx, "mistral", "provider_mistral")
            .await
            .expect("mistral domain migration failed");
        Self { store }
    }
}

impl ProviderModule for MistralModule {
    fn type_id(&self) -> &'static str {
        "mistral"
    }

    fn display_name(&self) -> &'static str {
        "Mistral"
    }

    fn description(&self) -> &'static str {
        "Mistral models via their own platform"
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
                let model = match selector {
                    ModelSelector::Specific(m) => m,
                    ModelSelector::Lightweight { .. } => LIGHTWEIGHT_MODEL.into(),
                };
                let provider = mistral_provider(api_key.clone(), base_url.clone());
                let request = CompletionRequest {
                    model,
                    messages,
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
            provider_from_config(&config, "mistral", mistral_provider)?
                .list_models_with(|m| mistral_reasoning_for_slug(&m.id))
                .await
        })
    }
}

#[cfg(test)]
mod tests;
