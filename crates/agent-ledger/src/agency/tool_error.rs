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
    pub(super) fn parse(block: &Block) -> Self {
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
