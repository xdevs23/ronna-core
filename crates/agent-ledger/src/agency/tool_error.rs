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
    /// The stable machine prefix of a tool-call rate-limit refusal
    /// (2026-08-30) — the conversation's own window and a single tool's
    /// window alike, since both mean the same thing to everything that reads
    /// it back. The error string's own fixed prefix IS the machine key
    /// here, the way a status row carries its documented key: an error is
    /// already a durable block the model reads, and a column beside it would
    /// be a second place recording what one string already says.
    ///
    /// ONE prefix for both windows (2026-08-30): a prefix per tool would make
    /// the run the forced end counts read a list of prefixes instead of one
    /// key — two decisions where one stands.
    ///
    /// Read back with a starts-with test, in one place, which is how the
    /// forced end counts a run of refusals. A handler whose own error opened
    /// with these bytes would feed that count — knowingly accepted: it is
    /// vanishingly unlikely, and five consecutive claimed rate limits ending
    /// the turn is defensible behavior even then.
    pub const RATE_LIMIT_PREFIX: &'static str = "tool-call rate limit:";

    /// The CONVERSATION window's refusal, rendered for the model: the machine
    /// prefix above, then this window's own detail template.
    ///
    /// One of two templates, one decision each (2026-08-30): a tool's own
    /// window refuses through
    /// [`per_tool_rate_limit_refusal`](Self::per_tool_rate_limit_refusal),
    /// because the advice tails genuinely differ — a conversation that has
    /// spent its whole allowance cannot reach for another tool, and a model
    /// that has ground one tool flat can. One sentence covering both would
    /// have to hedge on the difference, which is the advice the model acts
    /// on.
    ///
    /// The numbers are INTERPOLATED from the window actually in force —
    /// `calls` per `seconds` — rather than baked into the sentence, so a
    /// deployment or a test running its own window never ships a message that
    /// lies about it. Plain numbers, no unit-word branches: one place each
    /// number reads from.
    #[must_use]
    pub(crate) fn rate_limit_refusal(calls: usize, seconds: i64) -> String {
        let prefix = Self::RATE_LIMIT_PREFIX;
        format!(
            "{prefix} this conversation has spent its {calls} tool calls for the last \
             {seconds} seconds, and this call was not run. Answer with what you already have, \
             or wait before calling tools again."
        )
    }

    /// ONE TOOL's window refusal, rendered for the model (2026-08-30): the
    /// same machine prefix, then this window's own detail template — the
    /// tool's name and ITS configured numbers, interpolated for the same
    /// reason the conversation window's are.
    ///
    /// Plain numbers here too, the same recorded tradeoff the conversation
    /// template makes above: no unit-word branches, one place each number
    /// reads from. At `calls = 1` the sentence therefore reads `its 1 {name}
    /// calls`, plural and all — the grammar bends for that one value instead
    /// of the number's reading splitting into branches.
    ///
    /// The tail is where the two templates part: the conversation is still
    /// free to use a different tool, so the advice says so before it says
    /// wait. The prefix is shared deliberately — a refusal here feeds the
    /// same run the forced end counts, so a model looping on one bounded tool
    /// ends its turn exactly as a burst does.
    #[must_use]
    pub(crate) fn per_tool_rate_limit_refusal(name: &str, calls: usize, seconds: i64) -> String {
        let prefix = Self::RATE_LIMIT_PREFIX;
        format!(
            "{prefix} this conversation has spent its {calls} {name} calls for the last \
             {seconds} seconds, and this call was not run. Answer with what you already have, \
             or use a different tool, or wait before calling this one again."
        )
    }

    /// The refusal a deferring ends-turn tool gets (2026-08-30), pinned byte
    /// for byte like every other sentence the model reads back.
    ///
    /// A tool that ENDS the turn is stamped at the resolution write, where the
    /// runner holds the handler. A deferred outcome resolves later, through
    /// the public out-of-band door, which holds no handler and carries no
    /// stamp — so the end of the turn would be lost and the model summoned
    /// after it. The contract is closed rather than widened: an ends-turn tool
    /// resolves at once or its call is refused, and the model reads why.
    ///
    /// A constant, not a template: there is nothing to interpolate, and one
    /// sentence is the whole decision. Errors never carry the stamp, so this
    /// refusal cannot end a turn either.
    pub(crate) const ENDS_TURN_DEFERRAL_REFUSAL: &'static str = concat!(
        "an ends-turn tool must resolve at once: deferring the end of a turn ",
        "is a contract defect, and this call is refused."
    );

    /// Whether this error is a tool-call rate-limit refusal — either window's,
    /// since both are written with the one prefix. The read half of
    /// [`RATE_LIMIT_PREFIX`](Self::RATE_LIMIT_PREFIX), and the only place the
    /// prefix is matched.
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
