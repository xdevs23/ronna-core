//! Conversation-level value types: the composer input, the approval verdict
//! and the sentence a denial records, the ask vocabulary, the stop reason and
//! the usage summary.
//!
//! These are the words every later layer speaks. They carry no behavior beyond
//! intrinsic accessors and the one sentence constructor
//! ([`denial_error_text`]), and they know nothing about any application built
//! on this library.
//!
//! They arrived here from a wire crate that also generated frontend bindings.
//! The binding derive did not come with them: generating bindings is one
//! consumer's concern, and a library that pulls a binding generator into every
//! dependent's tree to serve it is the exact coupling this crate exists to
//! remove. A consumer that wants bindings derives them on its own types.

use serde::{Deserialize, Serialize};

/// Block types a composer can produce.
///
/// The runtime receives these, stores them relationally, and resolves
/// references (a quote to its actual text) when building model messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputBlock {
    /// Literal text the human typed.
    Text {
        /// The text itself.
        content: String,
    },
    /// A span selected out of earlier blocks. Stored as a reference; the text
    /// is resolved on read, never copied at write time.
    Quote {
        /// Block the selection starts in.
        start_block_id: i64,
        /// Character offset the selection starts at.
        start_pos: i64,
        /// Block the selection ends in.
        end_block_id: i64,
        /// Character offset the selection ends at.
        end_pos: i64,
    },
}

/// The two verdicts an approval request can receive. Who denied is
/// structural — `system_reason` vs `user_reason` on the decision block —
/// never encoded in the verdict itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalChoice {
    /// The request may proceed.
    Approved,
    /// The request may not proceed.
    Denied,
}

impl ApprovalChoice {
    /// The stored form of this verdict.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }
}

/// The tool error a denial records on the originating call.
///
/// Built so the model learns WHO denied and why: a system reason means the
/// runtime auto-rejected, a user reason means the human typed it. Who denied is
/// structural — it is the presence of one field or the other, never a flag on
/// the verdict — and this one function is where that structure becomes the
/// sentence the model reads. Every caller composing the text itself would be a
/// second vocabulary for the same fact.
///
/// It lives here, in the leaf both the agency and store layers already import,
/// because the store's denial write is the single construction site and the
/// store never imports the agency layer.
#[must_use]
pub fn denial_error_text(system_reason: Option<&str>, user_reason: Option<&str>) -> String {
    match (system_reason, user_reason) {
        (Some(system), Some(user)) => {
            format!("The system denied this action automatically: {system} The user added: {user}")
        }
        (Some(system), None) => format!("The system denied this action automatically: {system}"),
        (None, Some(user)) => format!("The user denied this action. Reason: {user}"),
        (None, None) => "The user denied this action.".to_string(),
    }
}

/// Who owes a block's next move. "No ask at all" is `Option::None` from the
/// block's own `awaiting` hook — that makes the block invisible to every gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Awaiting {
    /// A model turn is warranted (user text, tool results, harness messages).
    Model,
    /// The runtime owes out-of-band work (an unresolved tool call, a pending
    /// request).
    System,
    /// A human owes a chat reply (an interactive call).
    User,
    /// A human owes an out-of-band action (an approval); parks exactly like
    /// `User`, and a consumer's interface additionally disables its composer.
    /// (Named `OutOfBand`, not `None`: `Some(Awaiting::None)` reads as
    /// `Some(None)`.)
    OutOfBand,
}

/// Why the provider stopped emitting for this turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// The model finished its turn.
    EndTurn,
    /// The model asked for a tool call.
    ToolUse,
    /// The turn hit the output token ceiling.
    MaxTokens,
    /// The provider's content filter halted the response (e.g. an `OpenAI`
    /// Responses `incomplete_details.reason == "content_filter"`).
    ContentFilter,
}

/// Token usage surfaced with a stream's completion.
#[derive(Debug, Clone, Serialize)]
pub struct StreamUsage {
    /// Tokens the request consumed.
    pub input_tokens: u32,
    /// Tokens the response produced.
    pub output_tokens: u32,
    /// Reasoning tokens the turn spent, when the provider reports them. This is
    /// the spend a no-text turn is justified by — the model reasoned and then
    /// recorded no assistant move — so it rides the completion signal like the
    /// other counts rather than being dropped.
    ///
    /// `None` when the provider did not say — never a fabricated zero, which
    /// would read as a measurement that the model did not reason.
    pub reasoning_tokens: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::denial_error_text;

    /// All four authorship shapes of the denial text, asserted THROUGH the
    /// function — who denied is structural, and every caller builds the error
    /// via this one seam.
    #[test]
    fn denial_error_text_covers_all_four_authorship_shapes() {
        assert_eq!(
            denial_error_text(None, None),
            "The user denied this action."
        );
        assert_eq!(
            denial_error_text(None, Some("too risky")),
            "The user denied this action. Reason: too risky"
        );
        assert_eq!(
            denial_error_text(Some("policy"), None),
            "The system denied this action automatically: policy"
        );
        assert_eq!(
            denial_error_text(Some("policy"), Some("agreed")),
            "The system denied this action automatically: policy The user added: agreed"
        );
    }
}
