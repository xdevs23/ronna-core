//! A tool's answer.

use serde_json::Value;

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_tool_result};

/// A tool's answer. Ordinarily it is information the model has not seen yet,
/// so it asks for a model turn; a resolution STAMPED as ending the turn asks
/// for nothing at all (2026-08-30). Its own work happened elsewhere; running
/// it is trivial either way.
///
/// Call-to-result matching lives on
/// [`ToolCall::resolved_in`](super::ToolCall::resolved_in), which reads the raw
/// ledger — one predicate, so a result can never be counted against a call it
/// does not answer. A stamped result resolves its call exactly like any other:
/// what the stamp changes is the ASK, never the resolution.
///
/// Stored roleless; groups as [`Role::Tool`].
#[derive(Debug, Clone)]
pub struct ToolResult {
    /// The call this answers.
    pub tool_call_id: String,
    /// What the tool returned.
    pub content: String,
    /// Stamped on the block at the resolution write from the handler's own
    /// [`ends_turn`](crate::ToolHandler::ends_turn) — a fact about the tool at
    /// execution time, surfaced by the block loader. Absent means false, so
    /// every resolution written before the stamp existed reads as an ordinary
    /// one.
    ///
    /// This row IS the stored record of the turn's end: no marker is written
    /// beside it, and a restart derives the same closure from the same block.
    /// Being that record, it carries the record's obligations — the frontier
    /// reads THROUGH it exactly as it reads through a turn-end marker, so
    /// nothing absorbed into the ended turn's window stays buried under it.
    pub ends_turn: bool,
}

impl super::LeafKind for ToolResult {
    const KINDS: &'static [&'static str] = &["tool_result"];

    fn parse(block: &Block) -> Self {
        Self {
            tool_call_id: super::string_field(block, "tool_call_id"),
            content: super::string_field(block, "content"),
            ends_turn: block
                .fields
                .get("ends_turn")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

impl Agency for ToolResult {
    /// The stamp answered off the block's own data: an ends-turn resolution
    /// owes nobody a move — the owed-turn rule already reads this hook, at
    /// both of the sites that decide a turn.
    fn awaiting(&self) -> Option<Awaiting> {
        (!self.ends_turn).then_some(Awaiting::Model)
    }

    /// A stamped resolution is READ THROUGH, on the turn-closure marker's own
    /// rule (2026-08-30): it is the stored record of a turn's end, and a
    /// record that ends a turn while asking nothing is exactly the shape the
    /// burial defect lives in — an addressed message absorbed into the ended
    /// turn's window sits BEHIND this row, and an opaque one buried it
    /// forever, because the close that ends a turn here never latches and
    /// re-checks exactly once. Transparency hands the frontier the block
    /// behind the ended turn's trailing run instead, so the message summons
    /// its own turn with no further inbound.
    ///
    /// Answering the same rule the marker answers is why the ends-turn end
    /// needs no marker of its own: one turn end, one stored record, read one
    /// way. The unstamped resolution stays opaque — it owes the model, and
    /// nothing behind it may speak over that.
    fn frontier_transparent(&self) -> bool {
        self.ends_turn
    }
}

/// Every tool result in a ledger snapshot as `(content, stamped)`, in ledger
/// order — the ONE read the pins share for what a resolution recorded
/// (2026-08-30).
///
/// Parsed through this kind, never off the raw fields: a pin that reached into
/// the payload itself could read the stamp a way production never does, and
/// four such reads would drift apart one at a time. The tests that assert the
/// stamp — at the chokepoint, through the actor, and across a reopened
/// store — all come here.
#[cfg(test)]
pub(crate) fn results_with_stamps(ledger: &[Block]) -> Vec<(String, bool)> {
    use super::LeafKind;

    ledger
        .iter()
        .filter(|block| ToolResult::KINDS.contains(&block.block_type.as_str()))
        .map(|block| {
            let result = ToolResult::parse(block);
            (result.content, result.ends_turn)
        })
        .collect()
}

impl Projection for ToolResult {
    fn group_role(&self) -> Option<Role> {
        Some(Role::Tool)
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::ToolResult {
            tool_use_id: self.tool_call_id.clone(),
            content: self.content.clone(),
        }])
    }

    fn llm_text(&self) -> Option<String> {
        Some(render_tool_result(&self.content))
    }

    fn forces_parts(&self) -> bool {
        true
    }
}
