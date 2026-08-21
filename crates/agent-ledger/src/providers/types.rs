//! The neutral vocabulary the model boundary speaks.
//!
//! Everything here is vendor-free by construction. A request, a message, a
//! stream event and an error mean the same thing whichever vendor produced
//! them, and the translation into a wire shape happens in exactly one place
//! per vendor — inside that vendor's module and nowhere else.
//!
//! The types a block already owns are re-exported rather than restated:
//! [`ContentPart`] and the reasoning-continuity payload arrived with the
//! behavior layer, and a second definition of either would be a second answer
//! to the same question. What lands here instead is the part that is genuinely
//! about the wire — how a neutral part serializes — because serialization is
//! the provider layer's business, not a block kind's.

use std::pin::Pin;

use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::block::Block;

// The behavior layer produces these; the provider layer consumes them. They are
// re-exported at this vantage point so a vendor module has one place to import
// the boundary language from, without the layer beneath it having to know that
// a provider exists.
pub use crate::agency::ContentPart;
pub use crate::block::{OpaquePayload, ReasoningDetailEntry};
pub use crate::types::StopReason;

/// A turn's events as they arrive: an ordered, fallible, borrow-free stream.
///
/// Boxed and pinned because every vendor builds a different concrete stream and
/// the bind loop pumps all of them through one code path.
pub type EventStream = Pin<Box<dyn Stream<Item = Result<StreamEvent, LlmError>> + Send>>;

/// One request for a model turn, in neutral form.
#[derive(Debug, Clone, Serialize)]
pub struct CompletionRequest {
    /// The model's own identifier at its provider.
    pub model: String,
    /// The conversation so far, already grouped into messages.
    pub messages: Vec<Message>,
    /// The tools this turn may call.
    #[serde(default)]
    pub tools: Vec<ToolDefinition>,
    /// A ceiling on the response, or `None` for the provider's own.
    pub max_tokens: Option<u32>,
    /// Sampling temperature, or `None` for the provider's own.
    #[serde(default)]
    pub temperature: Option<f32>,
    /// Whether the response streams.
    #[serde(default)]
    pub stream: bool,
    /// The selected reasoning level, or `None` to defer to the provider's
    /// default. Each vendor's request builder translates it into that vendor's
    /// API parameter.
    #[serde(default)]
    pub reasoning: Option<ReasoningLevel>,
}

/// One message as the model reads it: a voice and its content.
#[derive(Debug, Clone, Serialize)]
pub struct Message {
    /// Who is speaking.
    pub role: MessageRole,
    /// What they said.
    pub content: MessageContent,
}

/// Whose voice a *message* speaks in.
///
/// Deliberately not [`crate::block::Role`]: the ledger's roles include the
/// tool, and no model wire has a tool voice — a tool's output reaches the model
/// as the user's turn. The render pass performs that collapse once, so the two
/// vocabularies never have to be reconciled again further down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// The harness speaking to the model.
    System,
    /// The human, and anything answering on the human's side of the turn.
    User,
    /// The model.
    Assistant,
}

/// A message's content in one of the two shapes a group can take.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// The joined markdown form.
    Text(String),
    /// The native-parts form, used whenever a part has no faithful text form.
    Parts(Vec<ContentPart>),
}

// ─── Neutral parts on the wire ───────────────────────────────────────────────

/// The serialized form of a neutral part.
///
/// A borrowing mirror of [`ContentPart`], carrying the derive. The behavior
/// layer's type stays free of wire concerns: a block kind states what it
/// contributes, and how that contribution is written down is a question only
/// the layer that writes it can answer. Keeping the two apart is also what
/// stops a serde attribute added for one vendor from silently changing what
/// every block kind means.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireContentPart<'a> {
    Text {
        text: &'a str,
    },
    Reasoning {
        text: &'a str,
        opaque: &'a Option<OpaquePayload>,
    },
    ToolUse {
        id: &'a str,
        name: &'a str,
        input: &'a Value,
    },
    ToolResult {
        tool_use_id: &'a str,
        content: &'a str,
    },
}

impl Serialize for ContentPart {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let wire = match self {
            Self::Text { text } => WireContentPart::Text { text },
            Self::Reasoning { text, opaque } => WireContentPart::Reasoning { text, opaque },
            Self::ToolUse { id, name, input } => WireContentPart::ToolUse { id, name, input },
            Self::ToolResult {
                tool_use_id,
                content,
            } => WireContentPart::ToolResult {
                tool_use_id,
                content,
            },
        };
        wire.serialize(serializer)
    }
}

/// A tool as the model is told about it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    /// The registered name the model calls it by.
    pub name: String,
    /// What it does, in the model's words.
    pub description: String,
    /// Its argument schema.
    pub parameters: Value,
}

/// What a turn cost.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Usage {
    /// Tokens the request consumed.
    pub input_tokens: u32,
    /// Tokens the response produced.
    pub output_tokens: u32,
    /// Reasoning tokens spent this turn, when the provider reports them.
    ///
    /// `None` when absent — never a fabricated zero, because a zero read as a
    /// measurement says the model did not reason, which is a different claim
    /// from the provider not having said.
    #[serde(default)]
    pub reasoning_tokens: Option<u32>,
}

/// Everything a provider can say while a turn is in flight.
///
/// This is the whole vocabulary: a vendor module translates its wire into these
/// and nothing downstream ever learns which vendor it was talking to.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    /// The connection is open.
    Connected,
    /// A progress line for whoever is watching the turn.
    ProviderStatus {
        /// The line itself.
        label: String,
    },
    /// A new text block has started.
    TextBlockStart,
    /// More text for the open text block.
    TextDelta {
        /// The fragment.
        text: String,
    },
    /// The complete final text, replacing rather than extending what streamed.
    /// Sent after streaming ends, so a partially-written block is corrected
    /// even though a reader may already have shown it.
    TextFinal {
        /// The whole text.
        text: String,
    },
    /// A new reasoning block has started.
    ThinkingStart,
    /// More reasoning for the open reasoning block.
    ThinkingDelta {
        /// The fragment.
        text: String,
    },
    /// Display-only reasoning summary text — the lossy channel some providers
    /// stream INSTEAD of verbatim reasoning.
    ///
    /// It accumulates on the same streaming block as
    /// [`ThinkingDelta`](Self::ThinkingDelta), in a field of its own, and never
    /// enters projection: replay rides the opaque continuity payload
    /// exclusively, so a summary that reached the model's next turn would be a
    /// paraphrase presented as the model's own chain.
    ThinkingSummaryDelta {
        /// The fragment.
        text: String,
    },
    /// The reasoning block is final.
    ThinkingEnd {
        /// The provider-native continuity payload captured from the stream,
        /// persisted in the same insert that finalizes the block and replayed
        /// verbatim on the next turn. `None` for vendors without one.
        opaque: Option<OpaquePayload>,
    },
    /// The model has asked for a tool.
    ToolUseStart {
        /// The provider's id for this call.
        id: String,
        /// The tool's registered name.
        name: String,
    },
    /// More of the call's arguments.
    ToolUseInputDelta {
        /// The JSON fragment.
        json: String,
    },
    /// The call's arguments are complete.
    ToolUseEnd,
    /// The turn is over.
    MessageEnd {
        /// What it cost.
        usage: Usage,
        /// Why it stopped.
        stop_reason: StopReason,
    },
    /// Final per-block content, for providers that restate a finished turn
    /// structurally rather than as one text blob.
    ContentFinal {
        /// The blocks, in order.
        blocks: Vec<FinalContentBlock>,
    },
}

/// One block of a [`StreamEvent::ContentFinal`] restatement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FinalContentBlock {
    /// Prose.
    Text {
        /// The text.
        text: String,
    },
    /// Reasoning with no continuity payload.
    Thinking {
        /// The reasoning.
        text: String,
    },
    /// Reasoning plus its continuity payload.
    ///
    /// In practice `opaque` is always `None` on this path — only a provider
    /// that restates content emits it, and no such provider echoes reasoning.
    /// The variant exists so the restatement path and the primary
    /// [`StreamEvent::ThinkingEnd`] capture path have the same shape, rather
    /// than one of them being a special case.
    Reasoning {
        /// The reasoning.
        text: String,
        /// Its continuity payload, if any.
        opaque: Option<OpaquePayload>,
    },
}

/// A model as its provider describes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    /// The provider's own identifier.
    pub id: String,
    /// The name a human reads.
    pub name: String,
    /// Who trained it.
    pub vendor: String,
    /// The reasoning levels this model accepts, read from its provider's own
    /// model description during listing.
    ///
    /// Transport-only, never persisted: it is live, version-dependent data, and
    /// a stored copy is wrong the first time the provider changes it.
    #[serde(default, skip_serializing_if = "ReasoningCapability::is_empty")]
    pub reasoning: ReasoningCapability,
}

/// The canonical, closed vocabulary of reasoning-effort levels.
///
/// One source of truth: providers advertise the subset they support and
/// translate each variant into their own API parameter through an exhaustive
/// match, so a newly added level cannot be silently dropped by a vendor that
/// was written before it existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningLevel {
    /// No reasoning.
    Off,
    /// The model decides.
    Auto,
    /// The smallest budget the provider offers.
    Minimal,
    /// A small budget.
    Low,
    /// A middling budget.
    Medium,
    /// A large budget.
    High,
    /// Larger than high, where a provider offers it.
    XHigh,
    /// The largest budget the provider offers.
    Max,
}

impl ReasoningLevel {
    /// The stable key this level is stored and transported under. It matches
    /// the serialized form, because two spellings of one level is how a stored
    /// value stops matching the value that wrote it.
    #[must_use]
    pub fn as_key(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Auto => "auto",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }

    /// Parse a stored key. An unknown key yields `None`, which reads as "defer
    /// to the provider" rather than as any particular level.
    #[must_use]
    pub fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "off" => Self::Off,
            "auto" => Self::Auto,
            "minimal" => Self::Minimal,
            "low" => Self::Low,
            "medium" => Self::Medium,
            "high" => Self::High,
            "xhigh" => Self::XHigh,
            "max" => Self::Max,
            _ => return None,
        })
    }
}

/// What reasoning a model supports, as its provider module reports it.
///
/// Empty `levels` means the model exposes no reasoning control at all, which is
/// a different statement from supporting only `Off`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReasoningCapability {
    /// The levels this model accepts, in the order they should be offered.
    pub levels: Vec<ReasoningLevel>,
    /// The level the provider applies when none is selected, where it says.
    pub default: Option<ReasoningLevel>,
}

impl ReasoningCapability {
    /// Build from an ordered list of supported levels, with no stated default.
    #[must_use]
    pub fn new(levels: Vec<ReasoningLevel>) -> Self {
        Self {
            levels,
            default: None,
        }
    }

    /// Whether the model exposes no reasoning control.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }
}

// ─── The provider channel ────────────────────────────────────────────────────

/// How a caller names the model a turn should run on.
#[derive(Debug, Clone)]
pub enum ModelSelector {
    /// This exact model.
    Specific(String),
    /// Whatever this provider considers its cheap background model. The
    /// provider resolves it, because the answer changes as vendors delist
    /// models and a caller has no way to know.
    Lightweight,
}

/// What a caller asks of a bound provider.
#[derive(Debug)]
pub enum ProviderRequest {
    /// Begin streaming a response for this ledger.
    Stream {
        /// The blocks that make up the conversation so far.
        blocks: Vec<Block>,
        /// Which model.
        model: ModelSelector,
        /// The tools this turn may call.
        tools: Vec<ToolDefinition>,
        /// The selected reasoning level, or `None` to defer.
        reasoning: Option<ReasoningLevel>,
    },
    /// Abandon the active stream.
    Interrupt,
}

/// What a bound provider says back.
#[derive(Debug)]
pub enum ProviderResponse {
    /// One event from the turn.
    Event(StreamEvent),
    /// A recoverable mid-stream drop happened, and the provider is about to
    /// re-open the SAME turn from the last committed block.
    ///
    /// Whoever is writing the stream down must discard the turn's uncommitted
    /// blocks and reset its trackers, so the regenerated stream lands on a
    /// clean slate rather than duplicating what already arrived. This is **not**
    /// a stream close: no [`Done`](Self::Done) accompanies it.
    Restart,
    /// The turn failed.
    Error(String),
    /// The stream is fully closed and the provider has nothing more to say.
    Done,
}

/// The sending half of a bound provider's channel.
pub type ProviderTx = tokio::sync::mpsc::UnboundedSender<ProviderRequest>;

/// The receiving half of a bound provider's channel.
pub type ProviderRx = tokio::sync::mpsc::UnboundedReceiver<ProviderResponse>;

// ─── Errors ──────────────────────────────────────────────────────────────────

/// Everything that can go wrong reaching a model.
#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    /// The transport failed.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// The provider answered with a non-success status.
    #[error("api error {status}: {message}")]
    Api {
        /// The HTTP status it answered with.
        status: u16,
        /// The body it answered with.
        message: String,
    },

    /// The provider is refusing requests for now.
    #[error("rate limited")]
    RateLimited {
        /// The server's own hint, in whole seconds, when it gave one.
        retry_after_secs: Option<u64>,
    },

    /// A payload did not parse.
    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// The stream broke mid-turn.
    #[error("stream error: {0}")]
    Stream(String),

    /// The stream produced no bytes within the idle window, so the connection
    /// went half-open. Recoverable: a reconnect re-opens the same turn.
    #[error("stream idle: no data for {0}s")]
    StreamIdle(u64),

    /// No credential is configured for this provider.
    #[error("missing api key for provider: {0}")]
    MissingKey(String),

    /// The provider instance is misconfigured.
    #[error("config error: {0}")]
    Config(String),
}

impl LlmError {
    /// Whether this is a recoverable mid-stream transport failure that a
    /// reconnect can plausibly fix, as opposed to a terminal error where
    /// retrying would only repeat the same failure.
    ///
    /// Recoverable: dropped and half-open connections, an idle stall, and
    /// transport-level failures — a connect or read timeout, a reset, an early
    /// EOF — that carry no HTTP status. Terminal: a real server response, a
    /// parse failure, a missing credential, a config error.
    ///
    /// A rate limit is deliberately *not* recoverable here. It is handled by
    /// the stream-open retry loop, which honours the server's own backoff hint;
    /// treating it as a mid-stream reconnect would retry it on the wrong
    /// schedule and against the wrong budget.
    #[must_use]
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Stream(_) | Self::StreamIdle(_) => true,
            // An error carrying an HTTP status is a server-side response, not a
            // transport drop — terminal. Anything else is the transport.
            Self::Http(e) => e.status().is_none(),
            Self::Api { .. }
            | Self::RateLimited { .. }
            | Self::Json(_)
            | Self::MissingKey(_)
            | Self::Config(_) => false,
        }
    }
}

#[cfg(test)]
mod recoverable_tests {
    use super::*;

    /// Transport drops and idle stalls are recoverable — a reconnect can fix
    /// them. Everything that represents a server response, bad input, or
    /// missing configuration is terminal.
    #[test]
    fn classifies_recoverable_vs_terminal() {
        // Recoverable: mid-stream transport failures.
        assert!(LlmError::Stream("error decoding response body".into()).is_recoverable());
        assert!(LlmError::StreamIdle(90).is_recoverable());

        // Terminal: a real server response or an unrecoverable local condition.
        assert!(
            !LlmError::Api {
                status: 400,
                message: "bad".into()
            }
            .is_recoverable()
        );
        assert!(
            !LlmError::Api {
                status: 500,
                message: "boom".into()
            }
            .is_recoverable()
        );
        assert!(
            !LlmError::RateLimited {
                retry_after_secs: Some(5)
            }
            .is_recoverable()
        );
        assert!(!LlmError::MissingKey("a-provider".into()).is_recoverable());
        assert!(!LlmError::Config("nope".into()).is_recoverable());

        let json_err = serde_json::from_str::<Value>("{ not json").unwrap_err();
        assert!(!LlmError::Json(json_err).is_recoverable());
    }

    /// A real transport failure (connect refused) carries no HTTP status, so the
    /// transport arm classifies it as recoverable. The status-bearing case — a
    /// server response — cannot be constructed without a live HTTP exchange, as
    /// the client crate exposes no public constructor for one, so it is covered
    /// by the bind-loop tests against the API arm, which is how a
    /// status-bearing response actually surfaces.
    ///
    /// The address is loopback with no listener, so this reaches no network:
    /// the connection is refused by the kernel before a packet leaves.
    #[tokio::test]
    async fn http_transport_error_is_recoverable() {
        let err = reqwest::Client::new()
            .get("http://127.0.0.1:1")
            .send()
            .await
            .unwrap_err();
        assert!(err.status().is_none());
        assert!(LlmError::Http(err).is_recoverable());
    }
}
