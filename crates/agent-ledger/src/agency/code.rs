//! A code snippet.

use serde_json::Value;

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_code};

/// A code snippet. User-authored code is part of the user's ask and warrants a
/// model turn, like text; an assistant's code is a finished utterance.
/// Projects as fenced markdown in both modes.
#[derive(Debug, Clone)]
pub struct Code {
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The language tag, when the composer supplied one.
    pub language: Option<String>,
    /// The snippet itself.
    pub content: String,
}

impl Code {
    pub(super) fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            language: block
                .fields
                .get("language")
                .and_then(Value::as_str)
                .map(str::to_string),
            content: super::string_field(block, "content"),
        }
    }

    fn fenced(&self) -> String {
        render_code(self.language.as_deref(), &self.content)
    }
}

impl Agency for Code {
    fn awaiting(&self) -> Option<Awaiting> {
        (self.role == Some(Role::User)).then_some(Awaiting::Model)
    }
}

impl Projection for Code {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::Text {
            text: self.fenced(),
        }])
    }

    fn llm_text(&self) -> Option<String> {
        Some(self.fenced())
    }
}
