//! Block agency — every block kind answers one small uniform interface:
//! *who owes my next move? do my own work; am I done?*
//!
//! The orchestration machinery drives these hooks blindly and never branches on
//! a block type; behavior lives ON the block kind, in a module of its own. This
//! is the library's first invariant, and this layer is where it either holds or
//! does not: the [`ratchet`] and the [`redispatch`] walk contain no kind name,
//! and the dispatch site below contains no logic.
//!
//! Two axes, two traits. [`Agency`] answers orchestration — who owes the next
//! move, what work this block does, whether it is done. [`Projection`] answers
//! representation — what this block says to the model. They are siblings rather
//! than one wider trait because they evolve independently: the approval blocks
//! are orchestration-loud and model-invisible.

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::block::Block;
use crate::bus::{EventBus, RuntimeEvent};
use crate::store::{Store, StoreError};

mod approval_decision;
mod approval_request;
mod code;
mod date_marker;
mod metadata_title_request;
mod metadata_title_response;
mod projection;
mod quote;
pub mod ratchet;
mod records;
pub mod redispatch;
mod system_prompt;
mod text;
mod thinking;
mod tool_call;
mod tool_error;
mod tool_result;

pub use approval_decision::ApprovalDecision;
pub use approval_request::ApprovalRequest;
pub use code::Code;
pub use date_marker::DateMarker;
pub use metadata_title_request::MetadataTitleRequest;
pub use metadata_title_response::MetadataTitleResponse;
pub use projection::{
    ContentPart, Projection, render_code, render_quote, render_text, render_tool_error,
    render_tool_result,
};
pub use quote::Quote;
pub use records::{Status, Streaming, StreamingThinking, StreamingToolCall, Unknown};
pub use system_prompt::SystemPrompt;
pub use text::Text;
pub use thinking::Thinking;
pub use tool_call::ToolCall;
pub use tool_error::ToolError;
pub use tool_result::ToolResult;

// Who owes a block's next move. Defined in `crate::types`, because it also
// rides conversation state out to a consumer and the vocabulary a consumer
// speaks belongs in one place. Re-exported here so the agency keeps its
// original vantage point: this is the layer that decides the answer.
//
// "No ask at all" is `Option::None` from [`Agency::awaiting`] — that is what
// makes a block invisible to every gate.
pub use crate::types::Awaiting;

/// The outcome of vetting a block before [`Agency::run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateDecision {
    /// Run the block.
    Proceed,
    /// Do not run it: an error block is recorded and the model re-plans.
    Refuse {
        /// What the model is told.
        reason: String,
    },
    /// Do not run it yet: clearance has been parked out of band, and the
    /// deferred body resumes through the redispatch walk.
    Defer,
}

/// The blind collaborators every agency hook may need — the store handle and
/// the event bus, never a domain service.
///
/// The restriction is the point: a block that needed a subsystem handle here
/// would make the ledger's behavior layer depend on whatever that subsystem
/// drags in, and the layer would stop being portable. A block that needs a
/// subsystem emits an event instead; the subsystem that owns the collaborator
/// acts on it.
///
/// `E` is the consumer's event type. The runtime publishes its own
/// [`CoreEvent`](crate::event::CoreEvent) values through it, so a block's
/// wakeup reaches the consumer's bus without the library naming the consumer's
/// events.
#[derive(Clone)]
pub struct AgencyCtx<E> {
    /// The conversation whose ledger is being driven.
    pub conversation_id: i64,
    /// The store handle every hook reads and writes through.
    pub store: Store,
    /// The bus a hook emits its wakeups on.
    pub bus: Arc<EventBus<E>>,
}

/// One trait, implemented by every block kind, all hooks defaulting inert.
///
/// The hooks dispatch statically — nothing is boxed and nothing is `dyn`.
/// That is deliberate: an object-safe form would force every hook to return a
/// boxed future, and the whole reason behavior lives on the kind is that the
/// compiler can check it.
///
/// The async hooks are declared `fn … -> impl Future<Output = …> + Send`
/// rather than native `async fn`, and the `Send` is the point: the machinery
/// awaits these hooks inside spawned tasks, and a native `async fn`'s future
/// has no nameable Send-ness from a bound on the implementor. Implementors
/// keep writing native `async fn` in their impl blocks; the futures those
/// desugar to MUST be `Send` — inherent to a multi-threaded runtime, and the
/// implementor's obligation, checked by the compiler at the impl.
pub trait Agency {
    /// Who owes the next move, by this block's nature. Default: no ask.
    fn awaiting(&self) -> Option<Awaiting> {
        None
    }

    /// Does this block's row outlive the drive that confirmed it? Default:
    /// yes, because block storage is append-only.
    ///
    /// An ephemeral kind answers `false`, and the ratchet then confirms it
    /// WITHOUT persisting the cursor onto it — the cursor keeps the id of the
    /// last durable block instead. The reason is that an ephemeral row is
    /// deleted by the finalization that replaces it: a cursor sitting on one
    /// would be left anchored to a row that no longer exists, and every
    /// finalization would cost a re-drive of the history behind it.
    ///
    /// This is a fact about the ROW's lifetime, not about the work: an
    /// ephemeral block still runs, still reports doneness, and still parks the
    /// drive when it is not done.
    fn durable(&self) -> bool {
        true
    }

    /// Vet this block before [`run`](Self::run).
    ///
    /// [`Refuse`](GateDecision::Refuse) records an error block, skips `run()`
    /// and lets the model re-plan. [`Defer`](GateDecision::Defer) means the
    /// gate has already parked out-of-band clearance and `run()` is skipped;
    /// the deferred body resumes through the redispatch walk.
    ///
    /// Not currently invoked by the ratchet: the active gating seam is the
    /// tool's own gate at the runner chokepoint
    /// ([`tools`](crate::tools)), which owns the tool registry. This hook
    /// exists for blocks whose vetting is answerable from the store and the bus
    /// alone.
    fn gate<E: RuntimeEvent>(
        &self,
        _ctx: &AgencyCtx<E>,
    ) -> impl Future<Output = GateDecision> + Send {
        async { GateDecision::Proceed }
    }

    /// Do this block's OWN work and report doneness.
    ///
    /// `true` means complete and the cursor may advance past this block;
    /// `false` means still owed, so the cursor parks here and re-drives on the
    /// next tick. A block returning `false` MUST be safe to re-run — the cursor
    /// re-drives it inclusively on resume, and a hook that is not idempotent
    /// duplicates its effect every tick it stays parked.
    ///
    /// # Errors
    ///
    /// If the block's own work fails. The drive parks rather than advancing.
    fn run<E: RuntimeEvent>(
        &self,
        _ctx: &AgencyCtx<E>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        async { Ok(true) }
    }

    /// Deferred-work routing: the block id to route the redispatch walk to
    /// next, or `None` to stop.
    ///
    /// Pure routing off this block's own recorded data. Returning `None` once
    /// nothing is left to do IS the walk's idempotency guard — there is no
    /// separate visited-set or run-once flag anywhere in the machinery, so a
    /// hook that keeps routing after the work is done re-runs it forever.
    fn post_gate_id(&self, _ledger: &[Block]) -> Option<i64> {
        None
    }

    /// The deferred work itself, run when the walk unwinds to this block.
    ///
    /// # Errors
    ///
    /// If the deferred work fails.
    fn run_post_gate<E: RuntimeEvent>(
        &self,
        _ctx: &AgencyCtx<E>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async { Ok(()) }
    }
}

/// How a stored row becomes a typed kind — the parse half of the kind
/// contract, beside the behavior half ([`Agency`]) and the representation half
/// ([`Projection`]).
///
/// Implemented by the composing types the machinery is instantiated over:
/// [`BlockKind`] for the library's own kinds, and a consumer's composing enum,
/// whose parse tries its own kinds and delegates anything unrecognised down to
/// the inner implementor — so the library's kinds resolve through the library
/// and only a genuinely unknown type reaches the inert fallback. The
/// implementation is the ONE place per implementor a stored type string is
/// compared to a literal.
pub trait FromBlock {
    /// Resolve a stored row to its typed kind. Total: an unrecognised type
    /// resolves to the implementor's inert fallback, never an error — an old
    /// build reading a newer ledger fails safe instead of misinterpreting it.
    fn from_block(block: &Block) -> Self;
}

/// What the machinery requires of the kind it is instantiated over: behavior,
/// representation, parse, and futures that can cross a task boundary.
///
/// A name for a bound, not a second kind of kind — the same statement
/// [`RuntimeEvent`] makes for the event type. Every generic seam of the
/// runtime takes `K: RuntimeKind`; writing the five bounds out at each of
/// those sites is the same statement made repeatedly, and a statement made
/// repeatedly is one that drifts. The blanket implementation means an
/// implementor of the three traits qualifies with nothing further to write.
pub trait RuntimeKind: Agency + Projection + FromBlock + Send + Sync + 'static {}

impl<K: Agency + Projection + FromBlock + Send + Sync + 'static> RuntimeKind for K {}

/// The typed block layer: one variant per stored block type, each carrying what
/// its agency reads.
///
/// Adding a kind is adding a variant and a module, and the compiler enforces
/// completeness. The enum is closed today, which is why an unregistered type
/// resolves to [`Unknown`] and goes silent; opening it to out-of-library kinds
/// is what the extension mechanism is for, and [`Unknown`] documents where that
/// gap currently lands.
#[derive(Debug, Clone)]
pub enum BlockKind {
    /// Prose.
    Text(Text),
    /// A quoted span of earlier blocks.
    Quote(Quote),
    /// A code snippet.
    Code(Code),
    /// A finished turn's reasoning.
    Thinking(Thinking),
    /// A model's request for tool work.
    ToolCall(ToolCall),
    /// A tool's answer.
    ToolResult(ToolResult),
    /// A tool's failure.
    ToolError(ToolError),
    /// A frontier cap, such as an interrupt.
    Status(Status),
    /// The conversation's system prompt.
    SystemPrompt(SystemPrompt),
    /// An ephemeral text tail.
    Streaming(Streaming),
    /// An ephemeral reasoning tail.
    StreamingThinking(StreamingThinking),
    /// An ephemeral tool-call input tail.
    StreamingToolCall(StreamingToolCall),
    /// The system's ask for a human's clearance.
    ApprovalRequest(ApprovalRequest),
    /// The human's verdict on that ask.
    ApprovalDecision(ApprovalDecision),
    /// The ledger's own calendar entry.
    DateMarker(DateMarker),
    /// The metadata ledger's ask for a derived title.
    MetadataTitleRequest(MetadataTitleRequest),
    /// A settled title derivation.
    MetadataTitleResponse(MetadataTitleResponse),
    /// A block type this build does not know — fully inert, so an old build
    /// reading a newer ledger fails safe instead of misinterpreting it.
    Unknown(Unknown),
}

impl FromBlock for BlockKind {
    /// Resolve a stored row to its typed kind.
    ///
    /// This is the ONE place in the library a stored type string is compared
    /// to a literal. Every other comparison would be a second table of type
    /// names, and two tables of the same names is how a kind ends up behaving
    /// one way in the drive and another way in a query.
    fn from_block(block: &Block) -> Self {
        match block.block_type.as_str() {
            "text" => Self::Text(Text::parse(block)),
            "quote" => Self::Quote(Quote::parse(block)),
            "code" => Self::Code(Code::parse(block)),
            "thinking" => Self::Thinking(Thinking::parse(block)),
            "tool_call" => Self::ToolCall(ToolCall::parse(block)),
            "tool_result" => Self::ToolResult(ToolResult::parse(block)),
            "tool_error" => Self::ToolError(ToolError::parse(block)),
            "status" => Self::Status(Status::parse(block)),
            "system_prompt" => Self::SystemPrompt(SystemPrompt::parse(block)),
            "streaming" => Self::Streaming(Streaming),
            "streaming_thinking" => Self::StreamingThinking(StreamingThinking),
            "streaming_tool_call" => Self::StreamingToolCall(StreamingToolCall),
            "approval_request" => Self::ApprovalRequest(ApprovalRequest::parse(block)),
            "approval_decision" => Self::ApprovalDecision(ApprovalDecision::parse(block)),
            "date_marker" => Self::DateMarker(DateMarker::parse(block)),
            "title_request" => Self::MetadataTitleRequest(MetadataTitleRequest::parse(block)),
            "title_response" => Self::MetadataTitleResponse(MetadataTitleResponse),
            other => {
                tracing::warn!(
                    block_id = block.id,
                    block_type = other,
                    "unknown block type — inert agency"
                );
                Self::Unknown(Unknown::parse(block))
            }
        }
    }
}

/// Pure per-variant delegation — zero logic at the dispatch site, ever.
///
/// Every hook of both traits goes through this one macro, so "the machinery
/// never branches on block kind" is checkable by reading one page: the only
/// `match` on a variant in this layer is here, and it does the same thing in
/// every arm.
macro_rules! dispatch {
    ($self:ident, $kind:ident => $call:expr) => {
        match $self {
            BlockKind::Text($kind) => $call,
            BlockKind::Quote($kind) => $call,
            BlockKind::Code($kind) => $call,
            BlockKind::Thinking($kind) => $call,
            BlockKind::ToolCall($kind) => $call,
            BlockKind::ToolResult($kind) => $call,
            BlockKind::ToolError($kind) => $call,
            BlockKind::Status($kind) => $call,
            BlockKind::SystemPrompt($kind) => $call,
            BlockKind::Streaming($kind) => $call,
            BlockKind::StreamingThinking($kind) => $call,
            BlockKind::StreamingToolCall($kind) => $call,
            BlockKind::ApprovalRequest($kind) => $call,
            BlockKind::ApprovalDecision($kind) => $call,
            BlockKind::DateMarker($kind) => $call,
            BlockKind::MetadataTitleRequest($kind) => $call,
            BlockKind::MetadataTitleResponse($kind) => $call,
            BlockKind::Unknown($kind) => $call,
        }
    };
}

impl Agency for BlockKind {
    fn awaiting(&self) -> Option<Awaiting> {
        dispatch!(self, kind => kind.awaiting())
    }

    fn durable(&self) -> bool {
        dispatch!(self, kind => kind.durable())
    }

    async fn gate<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> GateDecision {
        dispatch!(self, kind => kind.gate(ctx).await)
    }

    async fn run<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<bool, StoreError> {
        dispatch!(self, kind => kind.run(ctx).await)
    }

    fn post_gate_id(&self, ledger: &[Block]) -> Option<i64> {
        dispatch!(self, kind => kind.post_gate_id(ledger))
    }

    async fn run_post_gate<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<(), StoreError> {
        dispatch!(self, kind => kind.run_post_gate(ctx).await)
    }
}

impl Projection for BlockKind {
    fn group_role(&self) -> Option<crate::block::Role> {
        dispatch!(self, kind => kind.group_role())
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        dispatch!(self, kind => kind.llm_parts())
    }

    fn llm_text(&self) -> Option<String> {
        dispatch!(self, kind => kind.llm_text())
    }

    fn forces_parts(&self) -> bool {
        dispatch!(self, kind => kind.forces_parts())
    }
}

/// A string payload field, or the empty string when the payload does not
/// carry it.
///
/// The empty string is a SENTINEL, never a value: it means the field was
/// absent or was not a string, and two blocks that both fell back to it are
/// not two blocks carrying the same id. Every predicate matching on a parsed
/// id therefore rejects the empty string before comparing — the store
/// constrains its own writes `NOT NULL`, but these predicates run over parsed
/// JSON, which may come from anywhere.
fn string_field(block: &Block, key: &str) -> String {
    block
        .fields
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// An integer payload field, or 0 when the payload does not carry it.
///
/// 0 is the same kind of sentinel [`string_field`]'s empty string is: no row
/// has id 0, so a predicate keyed on a parsed row id rejects it rather than
/// letting two absent ids match each other.
fn i64_field(block: &Block, key: &str) -> i64 {
    block
        .fields
        .get(key)
        .and_then(Value::as_i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod ratchet_tests;
#[cfg(test)]
mod tests;
