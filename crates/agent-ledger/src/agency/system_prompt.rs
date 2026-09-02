//! The conversation's system prompt.

use crate::block::{Block, Role};

use super::Agency;
use super::projection::{Projection, render_text};

/// The conversation's system prompt. Inert agency; projects its content in
/// text mode only.
///
/// In a parts-mode group it contributes nothing, which is what the ladder this
/// trait replaced did — it had no parts arm for a system prompt, and the
/// rendered bytes are pinned to that. In practice a system prompt is always its
/// own tool-less system group, so the mode never arises.
#[derive(Debug, Clone)]
pub struct SystemPrompt {
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The prompt text.
    pub content: String,
}

impl super::LeafKind for SystemPrompt {
    const KINDS: &'static [&'static str] = &["system_prompt"];

    fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            content: super::string_field(block, "content"),
        }
    }
}

impl Agency for SystemPrompt {
    /// The one kind a conversation opens with. The rule is the store's — a
    /// prompt joins a conversation that holds no row yet — and this is the
    /// same rule stated where a reader of the ledger can ask it.
    fn heads_the_ledger(&self) -> bool {
        true
    }
}

impl Projection for SystemPrompt {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    fn llm_text(&self) -> Option<String> {
        Some(render_text(&self.content))
    }
}
