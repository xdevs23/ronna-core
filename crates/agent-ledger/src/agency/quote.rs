//! A quoted span of earlier blocks.

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_quote};

/// A quoted range of earlier blocks. User-authored quotes are part of the
/// user's ask and warrant a model turn, like text.
///
/// Projects as `> `-prefixed markdown in both modes — through the shared
/// formatting function, not a per-mode ladder. A provider with native citation
/// support gets a structured part here one day, and that is a change local to
/// this block.
#[derive(Debug, Clone)]
pub struct Quote {
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The quoted text, resolved from the referenced span at read time.
    pub text: String,
}

impl super::LeafKind for Quote {
    const KINDS: &'static [&'static str] = &["quote"];

    fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            text: super::string_field(block, "text"),
        }
    }
}

impl Agency for Quote {
    fn awaiting(&self) -> Option<Awaiting> {
        (self.role == Some(Role::User)).then_some(Awaiting::Model)
    }
}

impl Projection for Quote {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::Text {
            text: render_quote(&self.text),
        }])
    }

    fn llm_text(&self) -> Option<String> {
        Some(render_quote(&self.text))
    }
}
