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
    /// The provider's id for the call, echoed back so the model can pair the
    /// two. Never the pairing key: the model may reuse it.
    pub tool_call_id: String,
    /// The ledger row of the call this answers — the call's one identity, read
    /// off the row's own `source_block_id` (2026-09-02). The result kind
    /// carries it on the same terms, stated in full on
    /// [`ToolResult::call_block_id`](super::ToolResult::call_block_id): plain,
    /// because the store refuses a resolution that names no call, and 0 for a
    /// payload built outside the store.
    pub call_block_id: i64,
    /// Why it failed.
    pub error: String,
    /// Whether a standing no declined the call instead of it being attempted —
    /// the stored fact the forced turn end counts.
    pub refusal: Refusal,
}

/// Whether a recorded tool failure is a REFUSAL (2026-09-01).
///
/// A refusal is a call a STANDING no declined before it ran: a spent window, a
/// consumer that offered no tools at all. The model spent a round and got
/// nothing back but the reason, and nothing it can re-plan inside this turn
/// changes the answer — so a run of them is a model looping against something
/// that will keep saying no, which is what the forced turn end exists to stop.
///
/// A no the model can ACT on is not one. A tool's own check refusing this
/// input, a name no registry holds — each hands the model something to
/// correct, the next round can succeed, and each ends the run as an ordinary
/// outcome. Which of the two a failure is belongs to whoever refuses it: this
/// fact is set at the deciding pass and read back off the row, so the answer
/// is never re-derived from the words the model was handed.
///
/// A refused call is still a recorded call and still counts against the
/// tool-call windows; this fact is about what the model was handed, not about
/// what was spent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// A standing no declined the call. It counts toward the trailing refusal
    /// run.
    Refused,
    /// The call was attempted and failed, or was declined in a way the model
    /// can correct within the turn. It ends the run, like any ordinary
    /// outcome.
    Failed,
}

impl Refusal {
    /// The stored form: the row's own column, and the field a parsed block
    /// carries.
    #[must_use]
    pub fn is_refusal(self) -> bool {
        self == Refusal::Refused
    }

    /// Read one back from the stored form.
    #[must_use]
    pub fn from_stored(refusal: bool) -> Self {
        if refusal {
            Refusal::Refused
        } else {
            Refusal::Failed
        }
    }
}

impl ToolError {
    /// The stable opening of a tool-call rate-limit refusal (2026-08-30) — the
    /// conversation's own window and a single tool's window alike, since both
    /// mean the same thing to the model reading them.
    ///
    /// **It is prose, not a key (superseded 2026-09-01).** It WAS the machine
    /// key: the run the forced turn end counts was found by testing these
    /// bytes against the start of a stored error, on the reasoning that an
    /// error is already a durable block and a column beside it would record
    /// twice what one string said. That fell to a second producer. A
    /// consumer's own decline — a call arriving for a turn that offered no
    /// tools — must feed the same count, and there are only two ways to reach
    /// it through a prefix: match the consumer's sentence from in here, which
    /// hardcodes a vocabulary the framework must never know, or have the
    /// consumer open its sentence with these bytes, which ships a message that
    /// lies to the model about what refused it. The fact moved onto the row
    /// instead ([`Refusal`]), where any producer can set it and no reader has
    /// to parse anything. What is left here is one shared opening for two
    /// templates, read by nothing.
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
    /// wait. The opening is shared because the two sentences mean the same
    /// thing to the model reading them; what makes a refusal here feed the
    /// run the forced end counts is the fact its row carries
    /// ([`Refusal`]), the same for both windows.
    #[must_use]
    pub(crate) fn per_tool_rate_limit_refusal(name: &str, calls: usize, seconds: i64) -> String {
        let prefix = Self::RATE_LIMIT_PREFIX;
        format!(
            "{prefix} this conversation has spent its {calls} {name} calls for the last \
             {seconds} seconds, and this call was not run. Answer with what you already have, \
             or use a different tool, or wait before calling this one again."
        )
    }

    /// What the model reads when a call names a tool THIS CONVERSATION does
    /// not have (2026-09-01), where it has others to reach for.
    ///
    /// It lists the conversation's own tools and never the process registry,
    /// and it is one sentence for two situations on purpose: a name nothing
    /// ever registered and a name registered but outside this conversation's
    /// choice read identically, byte for byte. Any difference between the two
    /// answers would disclose the existence of a tool the conversation was
    /// deliberately not given, which is exactly what the recorded choice
    /// exists to stop.
    ///
    /// The names arrive already resolved and already sorted; this renders
    /// them and decides nothing.
    #[must_use]
    pub(crate) fn unresolved_tool(name: &str, tools: &[String]) -> String {
        format!(
            "unknown tool: {name}. This conversation's tools are: {}",
            tools.join(", ")
        )
    }

    /// What the model reads when a call arrives in a conversation that has NO
    /// tools (2026-09-01).
    ///
    /// It names no tool and says nothing about what this process registered:
    /// a conversation whose recorded choice is empty has no tools while the
    /// registry is full, and a sentence about the registry would be a fact
    /// the model has no business reading and, here, not even a true one.
    ///
    /// The empty-registry sentence this replaces asserted exactly that fact.
    /// An empty registry is now just one of the ways a conversation ends up
    /// with nothing, and every one of them reads the same.
    ///
    /// A constant, not a template, for the reason the deferral refusal below
    /// is one: there is nothing to interpolate, and the whole answer is that
    /// nothing here can resolve. Recorded [`Refusal::Refused`] by its caller —
    /// no next round can succeed, so a run of them ends the turn.
    pub(crate) const NO_TOOLS_REFUSAL: &'static str =
        "this conversation has no tools, so no tool call can be answered.";

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

    /// Whether this failure counts toward the trailing refusal run — read off
    /// the row's own fact, never off its text.
    pub(crate) fn records_refusal(&self) -> bool {
        self.refusal.is_refusal()
    }
}

impl super::LeafKind for ToolError {
    const KINDS: &'static [&'static str] = &["tool_error"];

    fn parse(block: &Block) -> Self {
        Self {
            tool_call_id: super::string_field(block, "tool_call_id"),
            call_block_id: super::i64_field(block, "source_block_id"),
            error: super::string_field(block, "error"),
            // Read optionally, the turn-ending stamp's own shape: the column
            // is NOT NULL with a default, so every failure the widening step
            // backfilled reads as an ordinary failure and ends a run exactly
            // as it always did.
            refusal: Refusal::from_stored(
                block
                    .fields
                    .get("refusal")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            ),
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
