//! Which tools a conversation has.

use serde_json::Value;

use crate::block::Block;

use super::projection::Projection;
use super::{Agency, FromBlock};

/// The tools a conversation has, recorded in its own ledger.
///
/// The NEWEST one speaks, and an earlier one is superseded by appending a
/// later one — the ledger's own way of changing a decision, with the history
/// of what was decided when left intact. A conversation whose ledger carries
/// none of these has made no choice at all, which is a different answer from
/// an empty list: no record means every registered tool, an empty record
/// means none.
///
/// What the names mean is decided in one place, [`ToolChoice::newest_in`]'s
/// readers: the recorded names are intersected with the process registry, so
/// a name the registry no longer holds resolves to nothing and is offered to
/// nobody until a later append corrects the record. Nothing here consults a
/// registry — this kind is the RECORD, and the resolution reads it.
#[derive(Debug, Clone)]
pub struct ToolChoice {
    /// The tool names this conversation has, in the order they were recorded.
    /// Empty is a decision: this conversation has no tools.
    pub names: Vec<String>,
}

impl ToolChoice {
    /// The newest recorded choice in a conversation's ledger, or `None` when
    /// it carries none.
    ///
    /// Ledger order is junction order, so the last matching block is the last
    /// one appended. Both readers of the recorded choice — the dispatch that
    /// decides what a turn is offered and the runner that decides what a call
    /// resolves against — come through here, so neither can read a different
    /// record from the other.
    #[must_use]
    pub fn newest_in(ledger: &[Block]) -> Option<Self> {
        ledger
            .iter()
            .rev()
            .find_map(|block| match super::BlockKind::from_block(block) {
                super::BlockKind::ToolChoice(choice) => Some(choice),
                _ => None,
            })
    }
}

impl super::LeafKind for ToolChoice {
    const KINDS: &'static [&'static str] = &["tool_choice"];

    /// The names, read the library's usual way: total, lenient, and falling
    /// back to the empty list, like every other kind's parse.
    ///
    /// The strictness sits at the store's read instead, which is where loaded
    /// blocks come from. The stored column goes through one decoding
    /// (`store::tool_choice::decode_tool_names`), which refuses a column
    /// holding anything but a list of strings instead of handing a shortened
    /// one on — so for a block that came out of a ledger, the empty list means
    /// a record carrying no names and never a name dropped on the way in, and
    /// those two mean opposite things to the resolution. A block assembled in
    /// memory answers to whoever assembled it; this reads what it finds and
    /// claims nothing about where it came from.
    fn parse(block: &Block) -> Self {
        Self {
            names: block
                .fields
                .get("names")
                .and_then(Value::as_array)
                .map(|names| {
                    names
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

impl Agency for ToolChoice {
    /// Transparent to the frontier walk, and that is a requirement of WHERE
    /// this record lands, not a property of what it says.
    ///
    /// The choice is appended at arbitrary points in a conversation's
    /// history — at a channel's first contact, whenever the registered set
    /// differs, at a compacted thread's opening — including behind a message
    /// nobody has answered yet. An opaque tail there would make the record
    /// itself the frontier's answer, and the unanswered message behind it
    /// would be owed a turn by nobody, forever. Read through, the message
    /// behind it still owes.
    ///
    /// This is the second reason a kind answers `true` here, beside the rows
    /// that STORE A TURN'S END: a row that is neither an ask nor an answer,
    /// appended into the middle of a live history.
    fn frontier_transparent(&self) -> bool {
        true
    }
}

/// Invisible to the model, in every mode: the record states which tools this
/// conversation has, and the model learns that from the definitions it is
/// offered, never from prose about them. It awaits nobody for the same
/// reason — a record's whole behavior is being there.
impl Projection for ToolChoice {}
