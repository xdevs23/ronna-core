//! A finished turn's reasoning.

use crate::block::{Block, OpaquePayload, Role};

use super::Agency;
use super::projection::{ContentPart, Projection};

/// A finished turn's reasoning. Inert agency — it awaits nothing.
///
/// Its projection is a first-class [`ContentPart::Reasoning`], never
/// stringified: the stored provider continuity payload rides along with it. A
/// payload that fails to deserialize degrades to `None` rather than panicking —
/// a ledger written by a build that knew a vendor this one does not must still
/// read back.
///
/// It contributes NO text-mode string, so a text-only group drops reasoning
/// entirely. That asymmetry is pinned deliberately: a reasoning part flattened
/// into prose reaches the model as the assistant's own words, which is not what
/// it said.
///
/// The block's display-only summary field — the lossy reasoning-summary
/// channel — is deliberately NOT parsed here: summaries never enter projection.
/// The consequence for a summary-only vendor is that the reasoning part's text
/// is empty and replay rides the opaque payload, which is what carries
/// continuity for that vendor anyway.
#[derive(Debug, Clone)]
pub struct Thinking {
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The human-visible reasoning text.
    pub content: String,
    /// The provider-native continuity payload, when one was captured.
    pub opaque: Option<OpaquePayload>,
}

impl super::LeafKind for Thinking {
    const KINDS: &'static [&'static str] = &["thinking"];

    fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            content: super::string_field(block, "content"),
            opaque: block
                .fields
                .get("opaque")
                .and_then(|value| serde_json::from_value(value.clone()).ok()),
        }
    }
}

impl Agency for Thinking {}

impl Projection for Thinking {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::Reasoning {
            text: self.content.clone(),
            opaque: self.opaque.clone(),
        }])
    }
}
