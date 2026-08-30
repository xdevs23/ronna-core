//! A tool's failure.

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_tool_error};

/// A tool's failure — the model reads why and re-plans, so it asks for a model
/// turn exactly like a result, and projects as a tool-result part carrying the
/// error text.
///
/// Call-to-error matching lives on
/// [`ToolCall::resolved_in`](super::ToolCall::resolved_in), the same predicate
/// a result answers through.
#[derive(Debug, Clone)]
pub struct ToolError {
    /// The call this answers.
    pub tool_call_id: String,
    /// Why it failed.
    pub error: String,
}

impl ToolError {
    /// The stable machine prefix of the tool-call window's refusal
    /// (2026-08-30). The error string's own fixed prefix IS the machine key
    /// here, the way a status row carries its documented key: an error is
    /// already a durable block the model reads, and a column beside it would
    /// be a second place recording what one string already says.
    ///
    /// Read back with a starts-with test, in one place, which is how the
    /// forced end counts a run of refusals. A handler whose own error opened
    /// with these bytes would feed that count — knowingly accepted: it is
    /// vanishingly unlikely, and five consecutive claimed rate limits ending
    /// the turn is defensible behavior even then.
    pub const RATE_LIMIT_PREFIX: &'static str = "tool-call rate limit:";

    /// The window's refusal, rendered for the model: the machine prefix above,
    /// then the one detail template.
    ///
    /// The numbers are INTERPOLATED from the window actually in force —
    /// `calls` per `seconds` — rather than baked into the sentence, so a
    /// deployment or a test running its own window never ships a message that
    /// lies about it. Plain numbers, no unit-word branches: one template, one
    /// decision, one place it reads from.
    #[must_use]
    pub(crate) fn rate_limit_refusal(calls: usize, seconds: i64) -> String {
        let prefix = Self::RATE_LIMIT_PREFIX;
        format!(
            "{prefix} this conversation has spent its {calls} tool calls for the last \
             {seconds} seconds, and this call was not run. Answer with what you already have, \
             or wait before calling tools again."
        )
    }

    /// Whether this error is the tool-call window's own refusal — the read
    /// half of [`RATE_LIMIT_PREFIX`](Self::RATE_LIMIT_PREFIX), and the only
    /// place the prefix is matched.
    pub(crate) fn records_rate_limit_refusal(&self) -> bool {
        self.error.starts_with(Self::RATE_LIMIT_PREFIX)
    }
}

impl super::LeafKind for ToolError {
    const KINDS: &'static [&'static str] = &["tool_error"];

    fn parse(block: &Block) -> Self {
        Self {
            tool_call_id: super::string_field(block, "tool_call_id"),
            error: super::string_field(block, "error"),
        }
    }
}

impl Agency for ToolError {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(Awaiting::Model)
    }
}

impl Projection for ToolError {
    fn group_role(&self) -> Option<Role> {
        Some(Role::Tool)
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::ToolResult {
            tool_use_id: self.tool_call_id.clone(),
            content: self.error.clone(),
        }])
    }

    fn llm_text(&self) -> Option<String> {
        Some(render_tool_error(&self.error))
    }

    fn forces_parts(&self) -> bool {
        true
    }
}
