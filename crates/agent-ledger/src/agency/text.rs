//! Prose.

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_text};

/// A prose block. User-authored text asks for a model turn; an assistant's or
/// a system's text is a finished utterance that awaits nothing.
#[derive(Debug, Clone)]
pub struct Text {
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The prose itself.
    pub content: String,
}

impl Text {
    pub(super) fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            content: super::string_field(block, "content"),
        }
    }
}

impl Agency for Text {
    fn awaiting(&self) -> Option<Awaiting> {
        (self.role == Some(Role::User)).then_some(Awaiting::Model)
    }
}

impl Projection for Text {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::Text {
            text: self.content.clone(),
        }])
    }

    fn llm_text(&self) -> Option<String> {
        Some(render_text(&self.content))
    }
}
