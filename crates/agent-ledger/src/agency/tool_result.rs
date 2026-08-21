//! A tool's answer.

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_tool_result};

/// A tool's answer — information the model has not seen yet, so it asks for a
/// model turn. Its own work happened elsewhere; running it is trivial.
///
/// Call-to-result matching lives on
/// [`ToolCall::resolved_in`](super::ToolCall::resolved_in), which reads the raw
/// ledger — one predicate, so a result can never be counted against a call it
/// does not answer.
///
/// Stored roleless; groups as [`Role::Tool`].
#[derive(Debug, Clone)]
pub struct ToolResult {
    /// The call this answers.
    pub tool_call_id: String,
    /// What the tool returned.
    pub content: String,
}

impl ToolResult {
    pub(super) fn parse(block: &Block) -> Self {
        Self {
            tool_call_id: super::string_field(block, "tool_call_id"),
            content: super::string_field(block, "content"),
        }
    }
}

impl Agency for ToolResult {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(Awaiting::Model)
    }
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
