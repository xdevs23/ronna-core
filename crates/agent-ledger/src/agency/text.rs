//! Prose.

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_text};

/// A prose block. A user's text is a person speaking and the model answers
/// it; every other voice's is a finished utterance that awaits nothing.
#[derive(Debug, Clone)]
pub struct Text {
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The prose itself.
    pub content: String,
}

impl super::LeafKind for Text {
    const KINDS: &'static [&'static str] = &["text"];

    fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            content: super::string_field(block, "content"),
        }
    }
}

impl Agency for Text {
    /// ONE voice asks the model for a turn: a USER's text is a person
    /// speaking, and the model answers it.
    ///
    /// Prose in any other voice STATES something and asks for nothing — an
    /// assistant's is a finished utterance, a tool's an outcome, and a
    /// system's is the harness putting something in front of the model to
    /// read. A compacted thread's digest is exactly that last shape, sitting
    /// in a live serving thread where an ask would dispatch an unasked turn
    /// (2026-08-31: the compaction slice made system-voiced prose await the
    /// model, which put the digest one frontier away from summoning one).
    ///
    /// The harness's ASK is a kind of its own,
    /// [`HarnessMessage`](super::HarnessMessage), written from exactly one
    /// place in this library. Asking is a fact about that kind, never about
    /// a voice this open kind can be written in.
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
