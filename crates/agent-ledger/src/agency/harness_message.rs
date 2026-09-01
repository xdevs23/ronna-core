//! The harness speaking to the model.

use crate::block::{Block, Role};
use crate::types::Awaiting;

use super::Agency;
use super::projection::{ContentPart, Projection, render_text};

/// The harness's own message to the model, appended mid-ledger to have it do
/// something with the history in front of it — and the block whose append
/// SUMMONS that turn.
///
/// A kind of its own, and that is the whole point (2026-08-31, the compaction
/// slice, after the review found one kind carrying two meanings). Prose in the
/// system voice says two different things depending on why it was written: the
/// harness ASKING for a turn, and the harness STATING something the model
/// should read — a compacted thread's digest is exactly the second, sits in a
/// live serving thread, and must never summon anything. Resting both on
/// [`Text`](super::Text) put the ask on an open kind, where any system-voiced
/// prose anyone appends becomes a dispatch; the ask lives here instead, on a
/// kind the library writes from ONE place
/// ([`Store::fork_temporary`](crate::store::Store::fork_temporary)) and no
/// consumer can write at all.
///
/// One fact about the ask, read off this block by the dispatch: it awaits the
/// MODEL, so appending it is what makes the conversation owe a turn.
///
/// What that turn is OFFERED is not this kind's answer (2026-09-01). The
/// conversation the fork builds records an empty
/// [`ToolChoice`](super::ToolChoice) of its own, written by the same door that
/// writes this block, so the turn is offered nothing because the ledger says
/// so — one fact, in the ledger, read by the dispatch and by the runner alike.
#[derive(Debug, Clone)]
pub struct HarnessMessage {
    /// The voice the block was written in — the harness's own, which is the
    /// system voice at the only door that writes one.
    pub role: Option<Role>,
    /// The instructions themselves. The words are the consumer's: this
    /// library has no prompts.
    pub content: String,
}

impl super::LeafKind for HarnessMessage {
    const KINDS: &'static [&'static str] = &["harness_message"];

    fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            content: super::string_field(block, "content"),
        }
    }
}

impl Agency for HarnessMessage {
    /// The ask, unconditional: this kind IS the harness asking, so the answer
    /// does not depend on the voice the row happens to carry. A block of this
    /// kind exists because something wanted a turn.
    fn awaiting(&self) -> Option<Awaiting> {
        Some(Awaiting::Model)
    }
}

impl Projection for HarnessMessage {
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
