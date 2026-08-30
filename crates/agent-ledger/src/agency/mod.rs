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

// The composing-enum derive, under the same name as the trait it implements —
// macro and trait live in different namespaces, so `use agent_ledger::Agency`
// brings both, exactly as a consumer expects of a derivable trait.
pub use agent_ledger_derive::Agency;

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

    /// Whether the owed-turn frontier decision reads THROUGH this block
    /// (2026-08-23, the verified burial defect): a transparent tail is
    /// skipped when the frontier's tail is read, and the block behind it
    /// answers instead. Default: opaque — the tail speaks for itself.
    ///
    /// Exactly one shape answers `true`: the status record carrying a
    /// stored turn-closure key ([`Status`]). Such a marker is written
    /// wherever the runtime ends a turn as a stored fact — by a close that
    /// ends a turn over an unanswered outcome, and (2026-08-30) by the
    /// forced end a run of tool-call window refusals triggers between
    /// rounds — and an addressed message absorbed into the dead turn's
    /// window sits BEHIND it in ledger order: an opaque marker buried that
    /// message forever, because neither end latches and the closed edge has
    /// no re-engagement beyond its one re-check. The interrupt's status
    /// stays opaque on purpose: its capping under the latch is that path's
    /// recorded semantics, and the latch's own release re-checks there.
    fn frontier_transparent(&self) -> bool {
        false
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
/// and only a genuinely unknown type reaches the inert fallback. The stored
/// type strings themselves are written in ONE place per kind: the
/// [`LeafKind::KINDS`] const, which the implementation reads.
pub trait FromBlock {
    /// The content-table descriptors of every kind this implementor composes —
    /// what a consumer hands [`StoreConfig`](crate::store::StoreConfig) as its
    /// descriptor set. The derive builds it by concatenating the composed
    /// kinds' [`LeafKind::DESCRIPTORS`] with the inner implementor's, so the
    /// set the store validates and the set the parse chain resolves are the
    /// same declaration. Empty by default: the library's own kinds live in the
    /// library's own tables and contribute nothing here.
    const DESCRIPTORS: &'static [crate::store::ContentDescriptor] = &[];

    /// Every stored type string this implementor resolves to a typed kind —
    /// its claim on the stored-string namespace. [`BlockKind`]'s lists the
    /// library's seventeen; a composing enum's is the union of its leaves'
    /// [`LeafKind::KINDS`] and its delegate's claim, which is what lets the
    /// derive refuse a collision at compile time at every nesting depth: a
    /// leaf whose string is already claimed would silently shadow the earlier
    /// owner in the first-match parse chain, and the shadowed rows would parse
    /// as the wrong kind with no error anywhere.
    ///
    /// Deliberately without a default: an implementor that claimed nothing
    /// would make every disjointness check against it vacuously true, so the
    /// claim is stated or the impl does not compile. Stating it is the most
    /// the compiler can force — the derive is the only path on which the
    /// claim is machine-checked against the parse chain, and a hand-written
    /// implementor vouches for its own honesty: an empty or partial claim
    /// silently weakens every disjointness check a downstream composer runs
    /// against it.
    const CLAIMED_KINDS: &'static [&'static str];

    /// Resolve a stored row to its typed kind. Total: an unrecognised type
    /// resolves to the implementor's inert fallback, never an error — an old
    /// build reading a newer ledger fails safe instead of misinterpreting it.
    fn from_block(block: &Block) -> Self;
}

/// One leaf kind's parse contract: which stored type strings are its, how a
/// row of one of them becomes the typed kind, and which content tables it
/// brings along.
///
/// [`FromBlock`] is the composing half — total, with an inert fallback — and
/// this is the leaf half: partial by design, called only for a row whose type
/// string is in [`KINDS`](Self::KINDS). Every kind, the library's own and a
/// consumer's alike, implements it the same way; a composing enum (or the
/// derive) tries each leaf's `KINDS` and delegates anything unrecognised to
/// its inner [`FromBlock`] implementor. `KINDS` is the ONE place an
/// implementor writes its stored type strings — [`BlockKind::from_block`]
/// reads these consts instead of carrying a second table of names.
pub trait LeafKind: Sized {
    /// The stored type strings this kind parses from.
    const KINDS: &'static [&'static str];

    /// The content-table descriptors this kind contributes. Empty for kinds
    /// stored in the library's own tables; a consumer kind with a table of its
    /// own declares it here, beside the strings that resolve to it.
    const DESCRIPTORS: &'static [crate::store::ContentDescriptor] = &[];

    /// Parse a stored row whose type string is one of [`KINDS`](Self::KINDS).
    fn parse(block: &Block) -> Self;
}

/// The coherence check for one fact declared twice: a descriptor's
/// `ephemeral` flag and its kinds' [`Agency::durable`] answer must be exact
/// negations of each other, and this asserts they are — for every kind every
/// descriptor in the set declares, by parsing a synthetic block of that kind
/// through `K` and comparing both sides.
///
/// The two declarations are one fact about a row's lifetime: the store's side
/// (`ephemeral`) puts the kind into the teardown sweep and the atomic
/// finalization delete, and the kind's side (`durable`) keeps the ratchet's
/// cursor off rows a finalization deletes. Their defaults disagree — a
/// descriptor is durable unless declared ephemeral, and a kind that never
/// overrides `durable()` (the inert fallback included) answers durable — so a
/// consumer that declares only the store's side ships the cursor-anchor
/// regression silently. Run this from the conformance tests over the full
/// descriptor set and the composing kind — derived and hand-written
/// compositions alike: the kind's `durable()` is the ONE source of the
/// row-lifetime fact (the derive delegates it to the leaf like every other
/// hook), and this check is where its agreement with the store's flag is
/// proven.
///
/// # Errors
///
/// The first kind whose two declarations disagree, named with both values.
pub fn check_descriptor_durability<K: Agency + FromBlock>(
    descriptors: &[crate::store::ContentDescriptor],
) -> Result<(), String> {
    for descriptor in descriptors {
        for kind in descriptor.kinds {
            let synthetic = Block {
                id: 0,
                role: None,
                block_type: (*kind).to_owned(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: serde_json::Map::new(),
            };
            let durable = K::from_block(&synthetic).durable();
            if durable == descriptor.ephemeral {
                return Err(format!(
                    "kind '{kind}' declares one row-lifetime fact twice and the two \
                     disagree: descriptor '{table}' says ephemeral = {ephemeral}, but the \
                     kind's durable() answers {durable} (it must answer {expected}) — an \
                     ephemeral row is deleted by the finalization that replaces it, so a \
                     cursor anchored on one dangles",
                    table = descriptor.table,
                    ephemeral = descriptor.ephemeral,
                    expected = !descriptor.ephemeral,
                ));
            }
        }
    }
    Ok(())
}

// ─── The compile-time kind algebra the derive evaluates ──────────────────
//
// Everything below is `const` because the derive's coherence checks run in
// constant evaluation: a collision or a mismatch is a build error, never a
// runtime surprise. `str` equality is spelled out byte by byte for the same
// reason — `==` on `str` is not callable in a const context.

/// Compile-time string equality, byte by byte.
const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Whether a stored type string is in a kind list — const, so the derive's
/// coherence checks can ask it during constant evaluation.
const fn contains_kind(kinds: &[&str], kind: &str) -> bool {
    let mut i = 0;
    while i < kinds.len() {
        if str_eq(kinds[i], kind) {
            return true;
        }
        i += 1;
    }
    false
}

/// Whether two kind lists share no stored type string. The derive asserts
/// this for every leaf against its delegate's [`FromBlock::CLAIMED_KINDS`]
/// and against every sibling leaf's [`LeafKind::KINDS`], so a stored string
/// with two owners — which the first-match parse chain would resolve silently
/// to whichever comes first — cannot compile.
#[must_use]
pub const fn kinds_disjoint(a: &[&str], b: &[&str]) -> bool {
    let mut i = 0;
    while i < a.len() {
        if contains_kind(b, a[i]) {
            return false;
        }
        i += 1;
    }
    true
}

/// Whether every stored type string a descriptor set claims is in `kinds`.
/// The write path keys off a descriptor's `kinds` and the read path keys off
/// the leaf's [`LeafKind::KINDS`]; the derive asserts this over both consts
/// of every leaf, so a descriptor whose kind the parse chain cannot resolve —
/// a row the store would write and the read would hand to the inert fallback —
/// cannot compile.
#[must_use]
pub const fn descriptor_kinds_claimed(
    descriptors: &[crate::store::ContentDescriptor],
    kinds: &[&str],
) -> bool {
    let mut i = 0;
    while i < descriptors.len() {
        let mut j = 0;
        while j < descriptors[i].kinds.len() {
            if !contains_kind(kinds, descriptors[i].kinds[j]) {
                return false;
            }
            j += 1;
        }
        i += 1;
    }
    true
}

/// How many stored type strings a set of kind lists holds in total — the
/// length of what [`concat_kinds`] produces, evaluable in a const context so
/// the derive can size the concatenated claim from the composed kinds' own
/// declarations.
#[must_use]
pub const fn kind_count(sets: &[&[&'static str]]) -> usize {
    let mut total = 0;
    let mut i = 0;
    while i < sets.len() {
        total += sets[i].len();
        i += 1;
    }
    total
}

/// Concatenate kind lists into one array at compile time, in order — a
/// composing enum's [`FromBlock::CLAIMED_KINDS`], built from each composed
/// kind's own declaration so no second list of stored strings exists
/// anywhere. `N` must be [`kind_count`] of the same sets; the derive infers
/// it from the annotated array type, and a mismatch fails the build.
///
/// # Panics
///
/// At compile time, if `N` differs from the sets' total — unreachable through
/// the derive, which computes both from the same sets.
#[must_use]
pub const fn concat_kinds<const N: usize>(sets: &[&[&'static str]]) -> [&'static str; N] {
    let mut out = [""; N];
    let mut at = 0;
    let mut i = 0;
    while i < sets.len() {
        let mut j = 0;
        while j < sets[i].len() {
            out[at] = sets[i][j];
            at += 1;
            j += 1;
        }
        i += 1;
    }
    assert!(
        at == N,
        "concat_kinds was given an N that is not the sets' total"
    );
    out
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
/// completeness. The enum is closed on purpose: out-of-library kinds compose
/// AROUND it — a consumer's enum, derived with the same `Agency` derive,
/// tries its own kinds first and delegates everything else here — so
/// [`Unknown`] is reached only by a genuinely unrecognised type.
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
    /// A frontier cap, such as an interrupt — except the stored turn-closure
    /// markers, which the frontier reads through instead.
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

/// One arm of the core parse chain: does the leaf's [`LeafKind::KINDS`] claim
/// this stored string, and if so, which variant does its parse feed?
///
/// A macro instead of seventeen hand-written if-returns so the shape cannot
/// drift per kind — the same reason the dispatch below is one macro. The type
/// strings themselves live on the leaf kinds as consts; nothing here names
/// one.
macro_rules! try_leaf {
    ($block:ident, $stored:ident, $leaf:ty => $variant:ident) => {
        if <$leaf>::KINDS.contains(&$stored) {
            return Self::$variant(<$leaf>::parse($block));
        }
    };
}

impl FromBlock for BlockKind {
    /// The library's seventeen stored type strings, concatenated at compile
    /// time from the leaf kinds' own `KINDS` consts — the same "one place per
    /// kind" rule the parse chain below reads by, so the claim cannot drift
    /// from what the chain resolves. [`Unknown`] claims nothing: it is the
    /// fallback, not an owner.
    const CLAIMED_KINDS: &'static [&'static str] = {
        const SETS: &[&[&str]] = &[
            Text::KINDS,
            Quote::KINDS,
            Code::KINDS,
            Thinking::KINDS,
            ToolCall::KINDS,
            ToolResult::KINDS,
            ToolError::KINDS,
            Status::KINDS,
            SystemPrompt::KINDS,
            Streaming::KINDS,
            StreamingThinking::KINDS,
            StreamingToolCall::KINDS,
            ApprovalRequest::KINDS,
            ApprovalDecision::KINDS,
            DateMarker::KINDS,
            MetadataTitleRequest::KINDS,
            MetadataTitleResponse::KINDS,
        ];
        const CONCATENATED: [&str; kind_count(SETS)] = concat_kinds(SETS);
        &CONCATENATED
    };

    /// Resolve a stored row to its typed kind.
    ///
    /// The chain reads each leaf kind's [`LeafKind::KINDS`] const, so the one
    /// place a stored type string is written is the leaf kind that owns it —
    /// one place per implementor. A second table of the same names is how a
    /// kind ends up behaving one way in the drive and another way in a query,
    /// which is why this function carries no literal of its own.
    fn from_block(block: &Block) -> Self {
        let stored = block.block_type.as_str();
        try_leaf!(block, stored, Text => Text);
        try_leaf!(block, stored, Quote => Quote);
        try_leaf!(block, stored, Code => Code);
        try_leaf!(block, stored, Thinking => Thinking);
        try_leaf!(block, stored, ToolCall => ToolCall);
        try_leaf!(block, stored, ToolResult => ToolResult);
        try_leaf!(block, stored, ToolError => ToolError);
        try_leaf!(block, stored, Status => Status);
        try_leaf!(block, stored, SystemPrompt => SystemPrompt);
        try_leaf!(block, stored, Streaming => Streaming);
        try_leaf!(block, stored, StreamingThinking => StreamingThinking);
        try_leaf!(block, stored, StreamingToolCall => StreamingToolCall);
        try_leaf!(block, stored, ApprovalRequest => ApprovalRequest);
        try_leaf!(block, stored, ApprovalDecision => ApprovalDecision);
        try_leaf!(block, stored, DateMarker => DateMarker);
        try_leaf!(block, stored, MetadataTitleRequest => MetadataTitleRequest);
        try_leaf!(block, stored, MetadataTitleResponse => MetadataTitleResponse);
        // A kind this library does not know is the NORMAL case here, not a
        // fault: every consumer-defined kind lands in this arm whenever a
        // library scan reads a mixed ledger through the library's own view
        // (the tool-call resolution walks, the redispatch chain). Logging the
        // expected case is noise by definition, and this parse cannot tell a
        // consumer kind from a corrupt one — a place that consults the full
        // consumer registry could, and a warning belongs there if anywhere.
        Self::Unknown(Unknown::parse(block))
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

    fn frontier_transparent(&self) -> bool {
        dispatch!(self, kind => kind.frontier_transparent())
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

/// A string payload field that is genuinely absent when it is absent.
///
/// The counterpart to [`string_field`] for a field a row may legitimately not
/// carry: a nullable column, or a column written before it existed. Its
/// empty-string sentinel would turn "nothing was recorded" into a value that
/// renders as nothing, which is a different claim.
fn optional_string_field(block: &Block, key: &str) -> Option<String> {
    block
        .fields
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
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
