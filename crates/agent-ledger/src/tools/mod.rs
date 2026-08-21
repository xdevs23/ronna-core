//! The tool layer: a registry of handlers, and the runner that is the ONE
//! place a tool body executes.
//!
//! A tool call is a block like any other. What makes this layer different from
//! the rest of the machinery is that a call's next move is not always the
//! system's to make: a gated call has to clear a human first, and an
//! interactive call is answered by the human outright. Both facts are decided
//! here and recorded on the ledger, never carried in memory.
//!
//! # The three seams
//!
//! - **The registry** ([`ToolRegistry`]) maps a recorded name to a handler. It
//!   is the only table of tool names in the runtime: a second one would answer
//!   differently the first time either changed.
//! - **The handler trait** ([`ToolHandler`]) is what a consumer implements. The
//!   library ships no tool of its own — a tool knows what a product does, which
//!   is the one thing this runtime must not.
//! - **The runner** ([`ToolRunner`]) owns admission. It re-reads the ledger on
//!   every wakeup and advances a call from recorded facts only.
//!
//! # Why the runner re-reads instead of remembering
//!
//! The failure this shape exists for looked reasonable at every step. A rework
//! made the cursor re-run every unresolved block on every tick, so a deferred
//! call began re-emitting its wakeup while parked. A check was added to hold a
//! controlled body unless an approval request existed and was approved — and
//! that check reconstructed the decision from the ledger's SHAPE, reading the
//! *absence* of a request as "it proceeded", while the real answer travelled in
//! memory. A call was then refused by its own gate and its body ran anyway: the
//! parked call re-emitted before the refusal's error landed, the check saw no
//! request (a refusal creates none), read that absence as consent, and
//! executed. One call recorded both an error and a success.
//!
//! So: the absence of a block is not a decision, and a decision that travels
//! both as a durable record and as an in-memory value is two decisions waiting
//! to disagree. Everything in [`runner`] and [`admission`] follows from that.
//!
//! # Object-safe, unlike the block kinds
//!
//! [`Agency`](crate::agency::Agency) is statically dispatched because the set of
//! block kinds is closed at compile time and the compiler can check it. A tool
//! set is not: a consumer registers handlers by name at runtime, so this trait
//! is `dyn`-safe and its async methods return a boxed future — the same trade
//! the provider layer makes, for the same reason, through the same
//! [`BoxFuture`] alias rather than a second one.
//!
//! # The tools that are not here
//!
//! The registry ships empty. Two concrete tools live in the application this
//! layer was extracted from — one executing code in an isolated runtime, one
//! reading documentation — and both stay there with the 13 tests that cover
//! them: they depend on subsystems this library has no business naming, and
//! they are that product's capabilities rather than the runtime's. The registry
//! they register into is here; they are not.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::agency::{AgencyCtx, GateDecision};
use crate::providers::BoxFuture;
use crate::providers::types::ToolDefinition;
use crate::reactivity::ReadSignal;

pub mod admission;
pub mod runner;

pub use admission::submit_approval;
pub use runner::ToolRunner;

/// What one invocation of a tool handler produced.
pub enum ToolOutcome {
    /// The body finished and its answer is ready. The runner appends the result
    /// block.
    Done(String),
    /// The body failed. The runner appends the error block, which resolves the
    /// call just as firmly as a result does — the model reads it and re-plans.
    Error(String),
    /// The body handed the work to a backing system that will resolve the call
    /// itself, later, through the conditional resolution writes
    /// ([`Store::complete_tool_call_block`] /
    /// [`Store::fail_tool_call_block`](crate::store::Store::fail_tool_call_block)).
    /// The backing system must keep the call's BLOCK id
    /// ([`ToolContext::block_id`]) — that id keys the write, because a model's
    /// `tool_call_id` can repeat and the block id is the call's one identity.
    ///
    /// The runner appends nothing and does NOT clear its in-flight mark: the
    /// call stays claimed until the resolution lands in the ledger, so a wakeup
    /// arriving in between does not start the work a second time.
    ///
    /// [`Store::complete_tool_call_block`]: crate::store::Store::complete_tool_call_block
    Pending,
}

/// What a tool body is handed: the conversation's blind collaborators, plus
/// which call this is.
///
/// The collaborators arrive as the same [`AgencyCtx`] every block hook gets,
/// rather than as a context of this layer's own. Two context types carrying the
/// store and the bus would be two answers to "what may a hook reach", and the
/// second one drifts.
pub struct ToolContext<'a, E> {
    /// The store handle, the event bus, and the conversation being driven.
    pub agency: &'a AgencyCtx<E>,
    /// The provider's id for this call — echoed onto the result so the model
    /// can pair it, never the resolution key: the model may reuse it.
    pub tool_call_id: &'a str,
    /// The ledger row the call block is — the call's one identity, and the id
    /// a [`Pending`](ToolOutcome::Pending) body's backing system must keep to
    /// resolve the call.
    pub block_id: i64,
}

/// One kind of tool: what the model is told about it, whether it needs
/// clearance, and what it does.
///
/// `E` is the consumer's event type, carried only so a body can reach the bus
/// through its [`ToolContext`].
pub trait ToolHandler<E>: Send + Sync {
    /// The model-facing definition: name, description, argument schema.
    fn definition(&self) -> ToolDefinition;

    /// Whether calls of this tool pass through the admission chokepoint.
    /// Default: ungated.
    ///
    /// An ungated tool gains no admission evaluation and no record of one. That
    /// is deliberate rather than an optimisation: if an ungated call left a
    /// trace of being admitted, "was gated" would stop being answerable from
    /// the ledger.
    fn gated(&self) -> bool {
        false
    }

    /// Whether a call of this tool awaits the HUMAN's reply rather than system
    /// execution. Default: not interactive.
    ///
    /// Read once, at insert, and stamped onto the call block, so the block
    /// answers who owes its next move from its own data on replay — never from
    /// a tool-name match against a registry that may since have changed.
    ///
    /// **Interactive supersedes [`gated`](Self::gated).** The human IS the
    /// admission for an interactive call — they answer it outright, so it
    /// never reaches the runner and its gate would never run. A handler
    /// declaring both is refused at registration in debug builds rather than
    /// silently shipping a gate that nothing consults.
    fn interactive(&self) -> bool {
        false
    }

    /// Vet one invocation, consulted only for a gated call that has no approval
    /// request yet.
    ///
    /// [`Proceed`](GateDecision::Proceed) executes now — a conditionally gated
    /// tool waiving clearance for this particular input.
    /// [`Refuse`](GateDecision::Refuse) resolves the call with a tool error
    /// carrying the reason, and the body never runs.
    /// [`Defer`](GateDecision::Defer) parks: the runner appends the approval
    /// request and the body waits for a human.
    ///
    /// **Side-effect-free by contract.** A check that wrote its own ledger
    /// state would put the same answer on two channels — this return value and
    /// a sibling block — and that duality is exactly what the one-decision rule
    /// forbids.
    ///
    /// A gate that knows the body would refuse must refuse HERE rather than
    /// defer: a durable request a human says yes to must never describe an
    /// effect that was always going to be turned down.
    fn gate<'a>(&'a self, input: &'a str) -> BoxFuture<'a, GateDecision> {
        let _ = input;
        Box::pin(async { GateDecision::Proceed })
    }

    /// Do the work. Reached only through the runner, and only once admission
    /// has recorded its answer.
    fn execute<'a>(&'a self, input: &'a str, ctx: ToolContext<'a, E>)
    -> BoxFuture<'a, ToolOutcome>;

    /// Spawn a per-conversation loop this handler needs in order to resolve
    /// [`ToolOutcome::Pending`] work. Default: no loop.
    ///
    /// The library never calls this: which conversations are live, and when
    /// their loops start and stop, is the session actor's question, not the
    /// registry's. The hook is here because the handler is the only thing that
    /// knows a loop is needed at all.
    fn spawn_reactor(
        &self,
        ctx: AgencyCtx<E>,
        latched: ReadSignal<bool>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let _ = (ctx, latched);
        None
    }
}

/// Handlers by the name a call records.
///
/// Keyed on the recorded name rather than on
/// [`definition().name`](ToolHandler::definition) so that resolution asks the
/// same question the ledger answers: a stored call names a string, and this is
/// what that string means.
///
/// Held in a `BTreeMap` so every iteration order below is the names' sorted
/// order — deterministic across insertion orders, restarts and replicas. The
/// model-facing schema is built from these iterations, and a hash-ordered list
/// would reorder the prompt on every process start, invalidating prompt caches
/// and letting two replicas disagree about one prompt.
pub struct ToolRegistry<E> {
    handlers: BTreeMap<String, Box<dyn ToolHandler<E>>>,
}

impl<E> ToolRegistry<E> {
    /// An empty registry. The library registers nothing into it — every tool
    /// belongs to a consumer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: BTreeMap::new(),
        }
    }

    /// Register one handler under the name calls will record.
    ///
    /// # Panics
    ///
    /// If a handler already answers to `name`. This is the only table of tool
    /// names in the runtime, and a silent overwrite would leave calls already
    /// recorded under the name meaning a different tool than the one that
    /// recorded them — a duplicate registration fails loudly instead, naming
    /// the colliding tool.
    ///
    /// In debug builds, also if the handler declares both
    /// [`gated`](ToolHandler::gated) and [`interactive`](ToolHandler::interactive):
    /// interactive supersedes gated — see [`ToolHandler::interactive`] — so the
    /// gate would never run, and nothing else would ever warn the author.
    pub fn register(&mut self, name: impl Into<String>, handler: impl ToolHandler<E> + 'static) {
        let name = name.into();
        debug_assert!(
            !(handler.gated() && handler.interactive()),
            "tool '{name}' declares both gated() and interactive(): interactive supersedes \
             gated — the human answers an interactive call outright, so its gate never runs"
        );
        match self.handlers.entry(name) {
            Entry::Occupied(entry) => panic!(
                "tool '{}' is already registered: a second handler under one name is refused, \
                 never a silent overwrite",
                entry.key()
            ),
            Entry::Vacant(entry) => {
                entry.insert(Box::new(handler));
            }
        }
    }

    /// Resolve a recorded name, or `None` when nothing answers to it.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&dyn ToolHandler<E>> {
        self.handlers.get(name).map(AsRef::as_ref)
    }

    /// The canonical names in sorted order, for a consumer building an alias
    /// map or a schema list of its own.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }

    /// Every registered handler in name order, for a consumer starting the
    /// loops they ask for through [`ToolHandler::spawn_reactor`].
    pub fn handlers(&self) -> impl Iterator<Item = &dyn ToolHandler<E>> {
        self.handlers.values().map(AsRef::as_ref)
    }

    /// The model-facing definitions of everything registered, in name order —
    /// identical no matter what order registration happened in, so the schema
    /// list never reorders between processes.
    #[must_use]
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.handlers.values().map(|h| h.definition()).collect()
    }
}

impl<E> Default for ToolRegistry<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
