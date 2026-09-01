//! Which tools a conversation HAS: one resolution, read by both consumers.
//!
//! The dispatch decides what a turn is offered and the runner decides what a
//! call resolves against, and those are the same question asked at two
//! moments. Both come through [`ResolvedTools`], and neither computes anything
//! of its own — which is what makes it impossible for a model to be offered a
//! tool whose call would then not resolve, or to be refused a tool it was just
//! shown.

use crate::agency::ToolChoice;
use crate::block::Block;
use crate::providers::types::ToolDefinition;

use super::{ToolHandler, ToolRegistry};

/// The tools one conversation has: its newest recorded choice intersected with
/// the registry this process loaded.
///
/// Three readings, and the difference between the last two is the whole
/// design:
///
/// - **No recorded choice** — the conversation is every registered tool. The
///   record is an exposure decision, and a ledger carrying none filters
///   nothing. What ENFORCES is elsewhere and runs on every call regardless, so
///   the absence of a record loosens what the model is shown and never what it
///   is allowed.
/// - **A recorded choice** — exactly the names it holds that the registry also
///   holds. A recorded name the registry no longer has resolves to nothing and
///   is offered to nobody; that state is reachable, because a restart can load
///   fewer tools than a persisted record names, and the intersection is the
///   whole rule for it until a later append corrects the record.
/// - **An empty recorded choice** — nothing. This conversation has no tools.
///
/// The names come out in the registry's own sorted order, whatever order they
/// were recorded in, so the schema list a turn is offered never reorders
/// between processes.
///
/// The registry the set was resolved from is held here, and no accessor takes
/// one: a set is a projection OF one registry, and a second registry offered to
/// an accessor would answer for tools this set never intersected.
pub(crate) struct ResolvedTools<'r, E> {
    registry: &'r ToolRegistry<E>,
    names: Vec<String>,
}

impl<'r, E> ResolvedTools<'r, E> {
    /// Resolve a conversation's tools from the ledger snapshot the caller
    /// already holds. Both callers hold one; neither reads a second.
    pub(crate) fn of(ledger: &[Block], registry: &'r ToolRegistry<E>) -> Self {
        let recorded = ToolChoice::newest_in(ledger);
        let names = registry
            .names()
            .filter(|name| {
                recorded
                    .as_ref()
                    .is_none_or(|choice| choice.names.iter().any(|held| held == name))
            })
            .map(str::to_owned)
            .collect();
        Self { registry, names }
    }

    /// The handler a call name resolves to for this conversation — the
    /// runner's question. A name the set does not carry answers `None` even
    /// when the registry holds it, so the handler is reached through the set
    /// and never beside it.
    pub(crate) fn handler(&self, name: &str) -> Option<&dyn ToolHandler<E>> {
        self.registry
            .get(name)
            .filter(|_| self.names.iter().any(|held| held == name))
    }

    /// The tools this conversation has, in sorted order — what an unresolved
    /// name is answered with, and never the process registry.
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// The model-facing definitions of exactly these tools — the dispatch's
    /// question. An empty set offers nothing.
    pub(crate) fn definitions(&self) -> Vec<ToolDefinition> {
        self.names
            .iter()
            .filter_map(|name| self.registry.get(name))
            .map(ToolHandler::definition)
            .collect()
    }
}
