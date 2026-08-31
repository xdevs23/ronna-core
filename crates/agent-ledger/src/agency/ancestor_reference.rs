//! Where a thread came from.

use crate::block::Block;

use super::Agency;
use super::projection::Projection;

/// A thread's recorded ancestry: the conversation whose history this one
/// continues.
///
/// A conversation opened to carry a compacted history forward records the
/// conversation it descends from as its first content block, and the id sits
/// in a COLUMN of that block's own row — not in the conversation's
/// `parent_id`, which records a FORK. This thread is not a fork: it opens
/// with a digest of what came before and inherits only the tail of it, so
/// the two facts are different facts and each keeps its own home.
///
/// The referenced conversation may be gone. The column carries no foreign
/// key for exactly that reason: erasure replaces an ancestor with a scrubbed
/// clone and deletes the original, and the record of where a thread came
/// from must survive that rather than cascade with it.
#[derive(Debug, Clone)]
pub struct AncestorReference {
    /// The conversation this thread continues. 0 is the absent sentinel —
    /// no conversation carries that id — so a row whose column did not read
    /// back names nothing rather than naming row zero.
    pub conversation_id: i64,
}

impl super::LeafKind for AncestorReference {
    const KINDS: &'static [&'static str] = &["ancestor_reference"];

    fn parse(block: &Block) -> Self {
        Self {
            conversation_id: super::i64_field(block, "ancestor_conversation_id"),
        }
    }
}

/// Inert: the reference asks nothing, does nothing and is opaque to the
/// frontier. It is a record, and a record's whole behavior is being there.
impl Agency for AncestorReference {}

/// Invisible to the model, in every mode: the reference states an internal
/// id, and an id teaches a model nothing. What the model needs about the
/// history this thread continues is the compaction message appended behind
/// it, in words. The id it carries names no person, no channel and no
/// content — it is a row number in this store.
impl Projection for AncestorReference {}
