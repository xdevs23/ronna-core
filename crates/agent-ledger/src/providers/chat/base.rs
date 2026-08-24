//! The shared chat-completions request base.
//!
//! Several vendors speak the same chat-completions request and response shape,
//! so that shape is implemented once here and each of them supplies only what
//! genuinely differs. The differences arrive through named seams — an effort
//! translation, a content decoder, a continuity-payload capture, an assistant
//! content encoder, a vendor label — rather than through conditionals keyed on
//! which vendor is calling, because a base that knew its callers would grow one
//! branch per vendor and stop being a base.
//!
//! This module carries no vendor-branded knowledge of its own. A model slug
//! map, a set of ranking headers, an endpoint: those live with the vendor.
//!
//! A vendor whose REQUESTS do not compose with this base still shares the
//! [decoder](super::sse) its responses take, which is why the two live side by
//! side rather than one inside the other.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::header::HeaderMap;
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::providers::bind::OpenedTurn;
use crate::providers::empty;
use crate::providers::http;
use crate::providers::types::{
    CompletionRequest, ContentPart, LlmError, Message, MessageContent, MessageRole, ModelInfo,
    OpaquePayload, ReasoningCapability, ReasoningDetailEntry, ReasoningLevel,
};

use super::sse::{self, ContentDecoder, SseState, ThinkingEndPayload, decode_string_content};
use super::wire::{
    self, WireFunction, WireMessage, WireMessageContent, WireModel, WireModelsResponse,
    WireReasoningDetail, WireRequest, WireStreamOptions, WireTool, WireToolCall,
    WireToolCallFunction,
};

/// Vendor seam: translate a selected level into the wire effort value.
///
/// Each vendor supplies an exhaustive match, so a level added later cannot be
/// silently dropped by a vendor written before it existed.
pub(crate) type EffortTranslation = fn(ReasoningLevel) -> Option<String>;

/// One ordered text-bearing part of a group, handed to a vendor's content
/// encoder: plain text, or reasoning that was not already replayed through a
/// sibling wire field. Named for the assistant because the encoder seam exists
/// for assistant groups — reasoning replay is the one thing a vendor encodes
/// differently — but a media-free user group folds through the same seam's
/// default, under its own role.
///
/// The payload rides along because a vendor's encoder decides for itself
/// whether it can replay one. Which vendors are compiled decides whether any
/// encoder reads it, so it is exempt from the unused check: it is part of the
/// base's contract, not of whichever caller happens to be enabled.
pub(crate) enum AssistantTextPart<'a> {
    Text(&'a str),
    Reasoning {
        text: &'a str,
        #[allow(dead_code)]
        opaque: Option<&'a OpaquePayload>,
    },
}

impl AssistantTextPart<'_> {
    pub(crate) fn text(&self) -> &str {
        match self {
            Self::Text(text) | Self::Reasoning { text, .. } => text,
        }
    }
}

/// One ordered part of a converted group, as [`ChatProvider::convert_parts`]
/// collects it: a text-bearing part bound for a vendor's content encoder, or a
/// media part bound for the structured chunks carrier.
///
/// The split is the point. The text-only carrier above is a vendor seam and
/// stays one; media never reaches an encoder written for text, it switches the
/// whole group's content to typed chunks instead.
enum GroupPart<'a> {
    Text(AssistantTextPart<'a>),
    Image { mime: &'a str, data: &'a [u8] },
}

/// Vendor seam: assemble an assistant group's ordered parts into the wire
/// content, replaying vendor-native payloads.
///
/// Returns the content plus whether a payload was actually replayed. The
/// default folds everything into the joined text string, which is the correct
/// degradation for an absent, foreign, or suppressed payload.
pub(crate) type AssistantContentEncoder =
    for<'a> fn(&[AssistantTextPart<'a>], bool) -> (Option<WireMessageContent>, bool);

/// One configured chat-completions endpoint, plus the seams its vendor filled.
pub(crate) struct ChatProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    extra_headers: HeaderMap,
    effort: EffortTranslation,
    decoder: ContentDecoder,
    thinking_payload: ThinkingEndPayload,
    assistant_content: AssistantContentEncoder,
    /// Whether this endpoint accepts the reasoning echo on assistant request
    /// messages. Most chat surfaces do not speak it at all.
    details_echo: bool,
    /// A fixed vendor name for listed models. `None` derives the vendor from the
    /// model id, for endpoints whose ids carry a vendor prefix.
    vendor: Option<&'static str>,
}

impl ChatProvider {
    /// Build the base for one credential, one endpoint and one set of extra
    /// headers. A vendor layers its remaining specifics on through the builder
    /// methods below.
    pub(crate) fn with_headers(
        api_key: String,
        base_url: Option<String>,
        default_base: &str,
        extra_headers: HeaderMap,
    ) -> Self {
        Self {
            client: http::streaming_client(),
            api_key,
            base_url: base_url.unwrap_or_else(|| default_base.to_string()),
            extra_headers,
            effort: chat_reasoning_effort,
            decoder: decode_string_content,
            thinking_payload: sse::default_thinking_end_payload,
            assistant_content: fold_assistant_content,
            details_echo: false,
            vendor: None,
        }
    }

    // The five seams below are the base's contract. Which of them a given build
    // uses depends on which vendors are compiled, so an unused one is a
    // statement about the feature selection rather than about this code.

    /// Override the reasoning-level translation.
    #[allow(dead_code)]
    pub(crate) fn effort_translation(mut self, translate: EffortTranslation) -> Self {
        self.effort = translate;
        self
    }

    /// Override the content decoder.
    #[allow(dead_code)]
    pub(crate) fn content_decoder(mut self, decoder: ContentDecoder) -> Self {
        self.decoder = decoder;
        self
    }

    /// Override the continuity-payload capture.
    #[allow(dead_code)]
    pub(crate) fn thinking_end_payload(mut self, payload: ThinkingEndPayload) -> Self {
        self.thinking_payload = payload;
        self
    }

    /// Override the assistant content assembly.
    #[allow(dead_code)]
    pub(crate) fn assistant_content_encoder(mut self, encoder: AssistantContentEncoder) -> Self {
        self.assistant_content = encoder;
        self
    }

    /// Accept the reasoning echo on assistant request messages.
    #[allow(dead_code)]
    pub(crate) fn with_details_echo(mut self) -> Self {
        self.details_echo = true;
        self
    }

    /// Fix the vendor name reported on listed models, for endpoints whose model
    /// ids carry no vendor prefix.
    #[allow(dead_code)]
    pub(crate) fn vendor_label(mut self, label: &'static str) -> Self {
        self.vendor = Some(label);
        self
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    /// Build the wire request.
    ///
    /// `include_reasoning_payloads` short-circuits every reasoning replay to
    /// "omit", folding the text instead. The returned flag reports whether a
    /// payload was actually replayed, which is the fallback's trigger.
    pub(crate) fn build_request_body(
        &self,
        request: &CompletionRequest,
        stream: bool,
        include_reasoning_payloads: bool,
    ) -> (WireRequest, bool) {
        let (messages, carried_payloads) =
            self.convert_messages(&request.messages, include_reasoning_payloads);

        let tools: Vec<WireTool> = request
            .tools
            .iter()
            .map(|t| WireTool {
                r#type: "function".to_string(),
                function: WireFunction {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    parameters: t.parameters.clone(),
                },
            })
            .collect();

        let body = WireRequest {
            model: request.model.clone(),
            max_tokens: request.max_tokens,
            messages,
            tools: if tools.is_empty() { None } else { Some(tools) },
            temperature: request.temperature,
            stream,
            stream_options: stream.then_some(WireStreamOptions {
                include_usage: true,
            }),
            reasoning_effort: request.reasoning.and_then(self.effort),
        };
        (body, carried_payloads)
    }

    pub(crate) fn convert_messages(
        &self,
        messages: &[Message],
        include_reasoning_payloads: bool,
    ) -> (Vec<WireMessage>, bool) {
        let mut wire_messages = Vec::new();
        let mut carried_payloads = false;

        for msg in messages {
            match &msg.content {
                MessageContent::Text(text) => {
                    if !empty::keeps_message(msg.role, text) {
                        continue;
                    }
                    wire_messages.push(WireMessage {
                        role: wire_role(msg.role).to_string(),
                        content: Some(WireMessageContent::Text(text.clone())),
                        tool_calls: None,
                        tool_call_id: None,
                        reasoning_details: None,
                    });
                }
                MessageContent::Parts(parts) => {
                    if !empty::keeps_parts(msg.role, parts) {
                        continue;
                    }
                    self.convert_parts(parts, msg.role, include_reasoning_payloads)
                        .append_to(&mut wire_messages, &mut carried_payloads);
                }
            }
        }

        (wire_messages, carried_payloads)
    }

    fn convert_parts(
        &self,
        parts: &[ContentPart],
        role: MessageRole,
        include_reasoning_payloads: bool,
    ) -> ConvertedParts {
        let mut group_parts: Vec<GroupPart> = Vec::new();
        let mut details: Vec<WireReasoningDetail> = Vec::new();
        let mut tool_calls = Vec::new();
        let mut tool_results = Vec::new();
        let mut carried_payloads = false;

        for part in parts {
            match part {
                ContentPart::Text { text } => {
                    collect_text(role, AssistantTextPart::Text(text), &mut group_parts);
                }
                ContentPart::Reasoning { text, opaque } => {
                    // The variant gate: only THIS surface's payload replays
                    // here, only on an endpoint that accepts the echo, and only
                    // while the knob admits payloads. Everything else — a
                    // foreign variant included — is OMITTED, never adapted: the
                    // text folds into the content instead.
                    if include_reasoning_payloads
                        && self.details_echo
                        && let Some(OpaquePayload::OpenRouter { entries }) = opaque
                    {
                        let rebuilt = rebuild_reasoning_details(entries);
                        if !rebuilt.is_empty() {
                            details.extend(rebuilt);
                            carried_payloads = true;
                            continue;
                        }
                    }
                    // Reasoning that reaches here degrades into the content, so
                    // it is collected as the text it has become — and an empty
                    // one is dropped like any other. The replay above is
                    // untouched.
                    collect_text(
                        role,
                        AssistantTextPart::Reasoning {
                            text,
                            opaque: opaque.as_ref(),
                        },
                        &mut group_parts,
                    );
                }
                ContentPart::ToolUse { id, name, input } => {
                    tool_calls.push(WireToolCall {
                        id: id.clone(),
                        r#type: "function".to_string(),
                        function: WireToolCallFunction {
                            name: name.clone(),
                            arguments: serde_json::to_string(input).unwrap_or_default(),
                        },
                    });
                }
                ContentPart::ToolResult {
                    tool_use_id,
                    content,
                } => tool_results.push((tool_use_id.clone(), content.clone())),
                ContentPart::Image { mime, data } => {
                    group_parts.push(GroupPart::Image { mime, data });
                }
            }
        }

        // A group carrying media switches to the typed-chunk content; a group
        // without any goes through the vendor's content encoder exactly as it
        // always did, so the media path cannot disturb a text-only wire.
        let has_media = group_parts
            .iter()
            .any(|part| matches!(part, GroupPart::Image { .. }));
        let (content, chunk_payloads) = if has_media {
            (Some(media_chunks(&group_parts)), false)
        } else {
            let text_parts: Vec<AssistantTextPart> = group_parts
                .into_iter()
                .filter_map(|part| match part {
                    GroupPart::Text(text) => Some(text),
                    GroupPart::Image { .. } => None,
                })
                .collect();
            (self.assistant_content)(&text_parts, include_reasoning_payloads)
        };

        ConvertedParts {
            role,
            content,
            details,
            tool_calls,
            tool_results,
            carried_payloads: carried_payloads || chunk_payloads,
        }
    }

    /// Fetch and parse the raw model list, keeping each entry's reasoning
    /// descriptor.
    async fn fetch_wire_models(&self) -> Result<Vec<WireModel>, LlmError> {
        let url = format!("{}/models", self.base_url);
        info!(url, "fetching model list");

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.api_key)
            .headers(self.extra_headers.clone())
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

    /// List models, deriving each one's reasoning capability through the
    /// caller's mapper.
    ///
    /// The mapper is the caller's because capability lives at the vendor
    /// boundary: one endpoint publishes a descriptor per model, another
    /// publishes nothing and needs a slug map. A base that guessed would be
    /// wrong for one of them.
    pub(crate) async fn list_models_with(
        &self,
        reasoning: impl Fn(&WireModel) -> ReasoningCapability,
    ) -> Result<Vec<ModelInfo>, LlmError> {
        let mut models: Vec<ModelInfo> = self
            .fetch_wire_models()
            .await?
            .into_iter()
            .map(|m| {
                let vendor = self.resolve_vendor(&m.id);
                ModelInfo {
                    reasoning: reasoning(&m),
                    name: m.name.clone().unwrap_or_else(|| m.id.clone()),
                    id: m.id,
                    vendor,
                }
            })
            .collect();
        models.sort_by(|a, b| a.name.cmp(&b.name));
        info!(count = models.len(), "fetched models");
        Ok(models)
    }

    /// The vendor name for a listed model: the fixed label where a vendor set
    /// one, the id's own prefix where ids carry one, and otherwise a neutral
    /// word.
    ///
    /// The fallback is deliberately neutral: this base is shared, and a custom
    /// endpoint pointed at it is not any particular vendor.
    fn resolve_vendor(&self, model_id: &str) -> String {
        if let Some(label) = self.vendor {
            return label.to_string();
        }
        match model_id.split('/').next().filter(|p| !p.is_empty()) {
            Some(prefix) if model_id.contains('/') => {
                let mut chars = prefix.chars();
                chars.next().map_or_else(
                    || prefix.to_string(),
                    |c| c.to_uppercase().to_string() + chars.as_str(),
                )
            }
            _ => "Custom".to_string(),
        }
    }

    /// Open one streaming turn.
    ///
    /// # Errors
    ///
    /// Only when the converted request carries no message at all — the one
    /// refusal made before anything is sent, since a request with an empty
    /// message array is rejected for a reason that names an array position
    /// rather than the content that went missing. A transport or API failure
    /// arrives instead as the stream's first item, where the bind loop's retry
    /// machinery can classify it.
    ///
    /// Asynchronous although it awaits nothing: this is the shape the bind
    /// loop's opener has, and a vendor that must await before opening — a
    /// credential refresh, say — fits it without every caller changing.
    #[allow(clippy::unused_async)]
    pub(crate) async fn open_turn(
        &self,
        request: CompletionRequest,
        include_reasoning_payloads: bool,
    ) -> Result<OpenedTurn, LlmError> {
        info!(
            model = %request.model,
            messages = request.messages.len(),
            "chat stream request"
        );
        debug!(body = ?serde_json::to_string(&request.messages), "request messages");

        let (body, carried_payloads) =
            self.build_request_body(&request, true, include_reasoning_payloads);
        empty::refuse_if_no_message(body.messages.len())?;

        Ok(OpenedTurn {
            events: wire::open_stream(
                self.client.clone(),
                self.api_key.clone(),
                self.endpoint(),
                self.extra_headers.clone(),
                body,
                SseState::new(self.decoder, self.thinking_payload),
            ),
            carried_payloads,
        })
    }
}

/// The wire spelling of a message's role. Content never decides it: a user and
/// an assistant can both carry text, or parts, and the misattribution this
/// mapping replaced sent a user's parts to the model in the assistant's voice.
fn wire_role(role: MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
    }
}

/// Collect one text-bearing part into a group, or drop it when its text is
/// empty.
///
/// The one door into the group's buffer for anything text-shaped: the plain
/// text part comes through here, and so does a reasoning part that degraded
/// into text. A later arm that produces text inherits the drop by using this
/// door rather than by remembering the rule — a guard each arm writes for
/// itself is exactly the shape that left one vendor's parts branch unguarded.
fn collect_text<'a>(role: MessageRole, part: AssistantTextPart<'a>, out: &mut Vec<GroupPart<'a>>) {
    if empty::keeps_text_part(role, part.text()) {
        out.push(GroupPart::Text(part));
    }
}

/// The typed-chunk content for a media-bearing group: every part in the
/// group's own order — text (a media group's caption included) as a text
/// chunk, an image as an `image_url` data URI carrying the bytes
/// base64-encoded.
///
/// The bytes travel encoded rather than by reference on purpose: a platform's
/// file URL is short-lived, authenticated, and names the platform to the
/// model's provider.
fn media_chunks(parts: &[GroupPart<'_>]) -> WireMessageContent {
    let chunks = parts
        .iter()
        .filter_map(|part| match part {
            // An empty caption contributes no chunk: a caption-less image is
            // the common case, and an empty text part (`{"type":"text",
            // "text":""}`) is rejected by some OpenAI-compatible gateways.
            //
            // Only a USER caption can still be one by the time it arrives: an
            // assistant's empty text is gone already, dropped at collection by
            // the shared decision (`collect_text` above). This test stays
            // exact-empty and answers for the voices that decision does not
            // cover — widening it to a trim would change what a user's caption
            // sends, which is a different question with a different cause.
            GroupPart::Text(text) if text.text().is_empty() => None,
            GroupPart::Text(text) => Some(json!({ "type": "text", "text": text.text() })),
            GroupPart::Image { mime, data } => Some(json!({
                "type": "image_url",
                "image_url": { "url": format!("data:{mime};base64,{}", BASE64.encode(data)) },
            })),
        })
        .collect();
    WireMessageContent::Chunks(chunks)
}

/// One group, converted but not yet appended.
struct ConvertedParts {
    /// The voice the group's own message is emitted under, spelled for the wire
    /// at the moment it is pushed. Tool results are not under it: they leave
    /// under the wire's tool role, keyed on the call they answer.
    role: MessageRole,
    content: Option<WireMessageContent>,
    details: Vec<WireReasoningDetail>,
    tool_calls: Vec<WireToolCall>,
    tool_results: Vec<(String, String)>,
    carried_payloads: bool,
}

impl ConvertedParts {
    /// Append the group's message — under the group's real role — and its tool
    /// results, in that order.
    ///
    /// The group's message is emitted only when it carries something: a
    /// message with neither content nor calls is rejected outright by these
    /// endpoints, with an error that names a position in the array rather than
    /// the block that produced it. A group whose only text was empty arrives
    /// here carrying nothing, so this is also where the drop stops being
    /// visible — the log already says what went.
    fn append_to(self, out: &mut Vec<WireMessage>, carried_payloads: &mut bool) {
        *carried_payloads |= self.carried_payloads;

        if self.content.is_some() || !self.tool_calls.is_empty() || !self.details.is_empty() {
            out.push(WireMessage {
                role: wire_role(self.role).to_string(),
                content: self.content,
                tool_calls: if self.tool_calls.is_empty() {
                    None
                } else {
                    Some(self.tool_calls)
                },
                tool_call_id: None,
                reasoning_details: if self.details.is_empty() {
                    None
                } else {
                    Some(self.details)
                },
            });
        }

        for (tool_call_id, content) in self.tool_results {
            out.push(WireMessage {
                role: "tool".to_string(),
                content: Some(WireMessageContent::Text(content)),
                tool_calls: None,
                tool_call_id: Some(tool_call_id),
                reasoning_details: None,
            });
        }
    }
}

/// The default content encoder: fold every part's text into one joined string.
///
/// Reasoning that reaches here has no natively replayable payload, so it renders
/// exactly as it always did.
pub(crate) fn fold_assistant_content(
    parts: &[AssistantTextPart<'_>],
    _include_reasoning_payloads: bool,
) -> (Option<WireMessageContent>, bool) {
    if parts.is_empty() {
        return (None, false);
    }
    let text = parts
        .iter()
        .map(AssistantTextPart::text)
        .collect::<Vec<_>>()
        .join("\n");
    (Some(WireMessageContent::Text(text)), false)
}

/// Rebuild stored entries into the wire echo array, in position order.
///
/// One entry family is dropped: an encrypted entry from one particular upstream
/// format. Replaying that combination is rejected outright by the gateway, so
/// the whole turn dies for the sake of a blob that contributes no text. The
/// predicate is exact — the same upstream format's *text* entries replay fine
/// and are kept — because widening it would quietly discard reasoning that works.
fn rebuild_reasoning_details(entries: &[ReasoningDetailEntry]) -> Vec<WireReasoningDetail> {
    entries
        .iter()
        .filter(|e| {
            !(e.entry_type == "reasoning.encrypted" && e.upstream_format == "google-gemini-v1")
        })
        .map(|e| {
            let mut detail = WireReasoningDetail {
                r#type: e.entry_type.clone(),
                id: e.entry_id.clone(),
                format: e.upstream_format.clone(),
                index: e.index,
                text: None,
                summary: None,
                data: None,
                signature: e.signature.clone(),
            };
            // The stored content holds whichever field the entry type carries;
            // a metadata-only entry rebuilds bare.
            if !e.content.is_empty() {
                match e.entry_type.as_str() {
                    "reasoning.summary" => detail.summary = Some(e.content.clone()),
                    "reasoning.encrypted" => detail.data = Some(e.content.clone()),
                    _ => detail.text = Some(e.content.clone()),
                }
            }
            detail
        })
        .collect()
}

/// The portable effort vocabulary this surface speaks.
///
/// Off, auto and max have no representation here and are never advertised
/// through this base, so they defer to the model's default rather than sending a
/// value the endpoint would reject.
fn chat_reasoning_effort(level: ReasoningLevel) -> Option<String> {
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

/// Build a provider from an instance's configuration through a vendor's own
/// constructor.
///
/// One place where a credential is read, shared by every fallible operation of
/// every vendor on this base. `provider_type` names the provider in the
/// missing-credential error, so the message says which one to go and configure.
pub(crate) fn provider_from_config(
    config: &Value,
    provider_type: &str,
    ctor: fn(String, Option<String>) -> ChatProvider,
) -> Result<ChatProvider, LlmError> {
    let api_key = config["api_key"]
        .as_str()
        .ok_or_else(|| LlmError::MissingKey(provider_type.into()))?
        .to_string();
    let base_url = config["base_url"].as_str().map(String::from);
    Ok(ctor(api_key, base_url))
}
