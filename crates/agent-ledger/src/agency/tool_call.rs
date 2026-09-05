//! A model's request for tool work.

use serde_json::Value;

use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::event::CoreEvent;
use crate::store::StoreError;
use crate::types::Awaiting;

use super::projection::{ContentPart, Projection};
use super::{Agency, AgencyCtx, BlockKind, FromBlock, ToolError, ToolResult};

/// A model's request for tool work.
///
/// A regular call owes the SYSTEM its execution: `run()` emits the wakeup and
/// stays not-done until a result or an error naming this call's own row sits
/// in the ledger. Parking the cursor here IS the guard against streaming a fresh turn
/// while calls still dangle — without it a later sibling's result green-lights
/// a turn and the earlier call is never answered.
///
/// An interactive call owes the USER the reply; the system does nothing.
#[derive(Debug, Clone)]
pub struct ToolCall {
    /// The ledger row this call is.
    pub id: i64,
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The provider's id for this call.
    pub tool_call_id: String,
    /// The tool's registered name.
    pub name: String,
    /// The raw input string as stored. Parsed to JSON only at projection time,
    /// so malformed input degrades instead of panicking.
    pub input: String,
    /// Stamped on the block at insert — a fact about the tool at execution
    /// time, surfaced by the block loader. Absent means false.
    pub interactive: bool,
}

impl super::LeafKind for ToolCall {
    const KINDS: &'static [&'static str] = &["tool_call"];

    fn parse(block: &Block) -> Self {
        Self {
            id: block.id,
            role: block.role,
            tool_call_id: super::string_field(block, "tool_call_id"),
            name: super::string_field(block, "name"),
            input: super::string_field(block, "input"),
            interactive: block
                .fields
                .get("interactive")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

impl ToolCall {
    /// Does a result or an error for this call sit in the ledger?
    ///
    /// Keyed on this call's own BLOCK id (2026-09-02) — the call's one
    /// identity, since a model's `tool_call_id` can repeat and two calls of one
    /// round can carry the same echo. This is THE resolution predicate: the
    /// approval kinds' routing and the runner's ledger idempotency all answer
    /// through it, so they cannot drift apart, and it asks exactly what the
    /// store's own conditional resolution write asks.
    ///
    /// The kinds it accepts are read through [`BlockKind`], not off the stored
    /// type string. Comparing the string here would be a second place that
    /// decides what a tool outcome is, and the two would answer differently the
    /// first time either changed.
    #[must_use]
    pub fn resolved_in(&self, ledger: &[Block]) -> bool {
        self.outcome_position_in(ledger).is_some()
    }

    /// WHAT answered this call: the result or the error naming it, or `None`
    /// while nothing does (2026-09-02).
    ///
    /// The public reading of a call's state, for a consumer whose tool defers
    /// its work to a backing system: a result means the work came out, an
    /// error means it did not, and `None` means the call is still pending.
    /// The consumer holds no walk of its own and no comparison of the
    /// provider's echo — this is the one place a call is paired with its
    /// outcome, so the framework and every consumer answer alike.
    ///
    /// It reads the same block [`resolved_in`](Self::resolved_in) does, through
    /// the same pairing read, so a call can never read resolved here and
    /// unresolved there. The outcome handed back is the one that read
    /// classified, never a second classification of the same row.
    #[must_use]
    pub fn outcome_in(&self, ledger: &[Block]) -> Option<CallOutcome> {
        ledger.iter().find_map(|block| self.answered_by(block))
    }

    /// The outcome this ONE block carries for this call, or `None` when the
    /// block answers another call or answers none.
    ///
    /// The whole pairing rule for a single row: [`answered_call`] says which
    /// call a block answers and hands back what kind of answer it is, and this
    /// keeps the answer where the call is this one. Every reading below walks
    /// the ledger through here, so the yes-or-no, the position and the outcome
    /// come from one comparison and one classification.
    ///
    /// Keyed on this call's own ROW id, which no parse can invent — the same
    /// shape [`ApprovalRequest::decision_in`](super::ApprovalRequest::decision_in)
    /// states for its own id-keyed reading. A resolution naming no call is
    /// already out: [`answered_call`] reads its id through the crate's
    /// `id_field` and hands back nothing at all, so an unrelated failure can
    /// never settle a call it does not answer, whatever id the call itself was
    /// built with.
    fn answered_by(&self, block: &Block) -> Option<CallOutcome> {
        answered_call(block).and_then(|(named, outcome)| (named == self.id).then_some(outcome))
    }

    /// WHERE this call's outcome sits — the index in `ledger` of the result or
    /// error naming this call's block, or `None` when nothing answers it.
    ///
    /// This IS [`resolved_in`](Self::resolved_in) and
    /// [`outcome_in`](Self::outcome_in), which ask the same question and keep
    /// the yes-or-no and the row: one rule, one implementation, three readings
    /// of it. The position half exists because a cut through a ledger has to
    /// know how far forward an answer lies, not merely that it lies somewhere
    /// (2026-08-31, the compaction slice) — and a second walk deciding what
    /// answers a call would be a second answer to the question the runner and
    /// the approval chain already ask here.
    ///
    /// It reads the WHOLE ledger, not only the rows behind this call
    /// (2026-09-02). The forward-only walk was what kept a repeated echo from
    /// letting an earlier call's result settle a later call; a block id names
    /// exactly one call, so position carries none of the identity any more and
    /// restricting the search would only hide an outcome a fork's junction
    /// order put in front of its call.
    #[must_use]
    pub(crate) fn outcome_position_in(&self, ledger: &[Block]) -> Option<usize> {
        ledger
            .iter()
            .position(|block| self.answered_by(block).is_some())
    }

    /// The first call BEFORE this one in the ledger that `of_interest` accepts
    /// and no outcome answers (2026-09-02) — the ORDERING question, answered
    /// over the snapshot the caller already holds.
    ///
    /// `of_interest` is what keeps this fold from knowing anything about
    /// tools: the caller says which calls the order binds, and the ledger says
    /// which of them are still owed an outcome. The runner asks it for a call
    /// whose tool runs in order, and a `Some` answer names the call that must
    /// take effect first.
    ///
    /// One pass collects what every outcome in the ledger answers, and the
    /// walk in front of this call reads that set — instead of asking
    /// [`resolved_in`](Self::resolved_in) per candidate, which would re-walk
    /// the ledger once for every earlier call. Both readings pair the same
    /// way, through [`answered_call`].
    ///
    /// Ledger order is the conversation's own order — its junction order, the
    /// order the calls were recorded in — so "before" means what the model
    /// issued first.
    #[must_use]
    pub(crate) fn earlier_unresolved_call(
        ledger: &[Block],
        before: &ToolCall,
        of_interest: &dyn Fn(&ToolCall) -> bool,
    ) -> Option<i64> {
        let own = ledger.iter().position(|block| block.id == before.id)?;
        let answered: std::collections::HashSet<i64> = ledger
            .iter()
            .filter_map(|block| answered_call(block).map(|(named, _)| named))
            .collect();
        ledger[..own]
            .iter()
            .find_map(|block| match BlockKind::from_block(block) {
                BlockKind::ToolCall(call) if !answered.contains(&call.id) && of_interest(&call) => {
                    Some(call.id)
                }
                _ => None,
            })
    }

    /// Does the ledger hold a call anchored on `anchor` whose outcome the
    /// SYSTEM itself still owes?
    ///
    /// One arm of the actor's identity-release rule (2026-08-22; narrowed
    /// 2026-08-23, the verified sixth break): a call the runner will answer
    /// is the proof that the turn's continuation is genuinely due — its
    /// outcome resumes the turn no matter what else the ledger absorbs.
    /// Keyed on the anchor, never on recency — another turn's dangling call
    /// must not keep this turn's identity alive. One call is NOT that proof
    /// and is excluded by the narrowing, because it held an identity open
    /// with no end: an INTERACTIVE call parks on the user, who may never
    /// answer. Its close ends the turn; a later approval writes the outcome
    /// with the call's own anchor, and the tail inheritance re-attaches the
    /// identity at that dispatch.
    ///
    /// A call the provider named with an EMPTY echo was excluded beside it
    /// until the pairing moved onto the block id (2026-09-02): the exclusion
    /// existed because such a call could never be matched by any outcome and
    /// so read as owed forever. It is matched by its own row now, the runner
    /// answers it like any other call, and a system-owed call is exactly what
    /// it is.
    ///
    /// Resolution is answered through [`Self::resolved_in`], THE resolution
    /// predicate, so this cannot drift from what the runner and the
    /// approval kinds consider answered. The call rows are read through
    /// [`BlockKind`] for the same reason the runner reads them there: they
    /// are the library's own kinds, and a composed consumer kind delegates
    /// the core type strings inward to the same parse.
    #[must_use]
    pub(crate) fn system_owed_call_anchored_in(ledger: &[Block], anchor: i64) -> bool {
        ledger.iter().any(|block| {
            block.dispatch_anchor == Some(anchor)
                && matches!(
                    BlockKind::from_block(block),
                    BlockKind::ToolCall(call)
                        if !call.interactive && !call.resolved_in(ledger)
                )
        })
    }

    /// How many tool outcomes anchored on `anchor` ASK FOR A CONTINUATION —
    /// every tool error, and every tool result but one stamped as ending the
    /// turn (2026-08-30).
    ///
    /// The other arm's measure (2026-08-23): an outcome that asks for a
    /// continuation summons exactly one, so a count above what the turn's
    /// dispatches have already answered is a continuation still due. The
    /// kinds are read through [`BlockKind`], like every other outcome
    /// decision, and the anchor keying keeps another turn's outcomes out of
    /// this turn's count.
    ///
    /// The ends-turn exclusion lives HERE, in the fold, and nowhere else
    /// (2026-08-30). Both consumers mean the same thing by this number — the
    /// actor's release rule counts what is still due, and its dispatch mark
    /// records what a request answered — so a filter in one of them would put
    /// two different counts on the two sides of one comparison, and a
    /// sibling's later outcome would be silently dropped in the round after a
    /// turn that ended on a stamp beside a sibling. One fold, one reading, both readers.
    #[must_use]
    pub(crate) fn outcomes_anchored_in(ledger: &[Block], anchor: i64) -> usize {
        ledger
            .iter()
            .filter(|block| {
                block.dispatch_anchor == Some(anchor) && outcome_asks_for_a_continuation(block)
            })
            .count()
    }

    /// How many tool calls this ledger recorded inside the trailing
    /// `seconds` — the tool-call windows' own count (2026-08-30), over every
    /// recorded call when `of_tool` is `None` and over ONE tool's calls when
    /// it names one.
    ///
    /// The name filter is the per-tool window's whole difference from the
    /// conversation's (2026-08-30): it reads the parsed kind's own
    /// [`name`](Self::name), the same parse this fold already runs, so a
    /// per-tool count is ONE walk of the ledger and never a second one beside
    /// it. What the filter never touches is the walk's window edge below,
    /// which ends on ANY call older than the cutoff whatever its name: the
    /// stamps ascend along ledger order for the whole conversation, so the
    /// first call outside the span completes the answer for every name at
    /// once.
    ///
    /// A fold, never a counter: "how much has this conversation spent
    /// lately" is derivable from the rows themselves, so it is not state
    /// anywhere. It therefore survives a restart, which the burn it bounds
    /// also does, and no in-memory copy can disagree with it.
    ///
    /// Read on the stamps as they really are: every call rides
    /// `insert_block`, which names the row's `created_at` from the machine's
    /// local clock with a fixed numeric offset, so the compare parses each
    /// stamp to an INSTANT ([`parse_stamp`](crate::store::parse_stamp)) and
    /// takes "now" from that same clock ([`now_instant`](crate::store::now_instant)).
    /// A lexical compare would be wrong twice over: two offsets straddling a
    /// daylight saving change do not sort as strings, and the window would
    /// silently mean something else wherever the offset is not zero. A stamp
    /// that does not parse is outside the window — the reasoning is recorded
    /// on `parse_stamp`.
    ///
    /// Every recorded call counts: executed, refused by the window itself,
    /// refused by a gate, interactive, out of band. The caller scopes the
    /// ledger — one conversation's blocks — and every call joins exactly one
    /// conversation, so this is the per-conversation count without the fold
    /// naming a conversation at all.
    ///
    /// **Bounded by the window, never by the ledger.** The walk runs BACKWARD
    /// from the newest block and stops at the first call stamped before the
    /// cutoff: the answer is complete there, and the ledger behind it — an
    /// append-only history a consumer keeps without retention — is never
    /// touched. The stop rests on the stamps ascending along ledger order,
    /// which is what the stamp doc above already describes: every call in one
    /// conversation is stamped at insert by the one writer's clock, and a
    /// fork's copies carry their older source stamps ahead of everything
    /// appended after them. A clock stepped BACKWARD is the only way that
    /// assumption breaks, and it breaks toward under-counting — the same
    /// direction the out-of-range fallbacks below take, never toward refusing
    /// a conversation that is spending nothing. A stamp that does not parse
    /// carries no time to compare, so it neither counts nor ends the walk.
    #[must_use]
    pub(crate) fn calls_in_trailing_window(
        ledger: &[Block],
        seconds: i64,
        of_tool: Option<&str>,
    ) -> usize {
        // A `seconds` outside the representable span of a delta degrades to
        // a zero-length window, which counts nothing and therefore refuses
        // nothing. The same direction as the fallback below, and deliberate
        // for the same reason: the window bounds spending, it never becomes
        // the reason a conversation stops working. The numbers arrive as the
        // operator's compiled defaults or as a consumer's
        // `with_tool_window` arguments, and the builder's positive-span
        // assertion is the fence in front of the obvious misconfiguration —
        // what still reaches here past that fence is an absurd span, not a
        // mistaken one, and it degrades for exactly that reason.
        let span = chrono::TimeDelta::try_seconds(seconds).unwrap_or_else(chrono::TimeDelta::zero);
        let Some(cutoff) = crate::store::now_instant().checked_sub_signed(span) else {
            // A clock so far from the epoch that the window cannot be
            // subtracted from it: count nothing rather than refuse
            // everything.
            return 0;
        };
        let mut calls = 0;
        for block in ledger.iter().rev() {
            let BlockKind::ToolCall(call) = BlockKind::from_block(block) else {
                continue;
            };
            match crate::store::parse_stamp(&block.created_at) {
                Some(at) if at >= cutoff => {
                    if of_tool.is_none_or(|wanted| call.name == wanted) {
                        calls += 1;
                    }
                }
                Some(_) => break,
                None => {}
            }
        }
        calls
    }

    /// How many of the turn's LAST tool outcomes are REFUSALS, counted back
    /// from the newest until one is not (2026-08-30).
    ///
    /// The outcome SUBSEQUENCE, never raw block adjacency: every refused
    /// round appends the next round's call block behind the previous error,
    /// so two tool-error rows are never neighbours in the ledger and a
    /// literal run of blocks would never find a run at all. Results and
    /// errors anchored on `anchor` are the subsequence, in ledger order —
    /// which is the ids' own order for a conversation's appends — and a
    /// result, a gate refusal or any other failure ends the run as an
    /// ordinary outcome: only a refusal means the model spent a round and was
    /// handed nothing but the reason.
    ///
    /// EVERY refusal counts, whoever refused (2026-09-01): the conversation's
    /// own window, a single tool's window, and a consumer's own decline alike,
    /// because they mean the same thing — the model is spending rounds on
    /// refusals. A model looping on one bounded tool therefore ends its turn
    /// exactly as a burst does, on the one consecutive limit, and so does one
    /// reaching for a tool a turn never offered.
    ///
    /// Read off the row's own fact ([`Refusal`](super::Refusal)), which is
    /// what makes that sentence true for a producer this crate has never
    /// heard of. It was read off the opening bytes of the rendered error until
    /// the second producer arrived; the supersession is recorded on
    /// [`ToolError::RATE_LIMIT_PREFIX`](super::ToolError::RATE_LIMIT_PREFIX).
    ///
    /// Anchor-keyed like every other turn fold here, which is also what
    /// keeps an out-of-band call — recorded with a NULL anchor — out of the
    /// open turn's run entirely: it can never lengthen one.
    ///
    /// **Bounded by the turn, never by the ledger.** The reverse walk stops
    /// at the anchor's own block: an outcome anchored on this turn was
    /// appended after the block the anchor names, so nothing at or before
    /// that block can belong to the run, and the history in front of the
    /// turn — append-only and kept without retention — is never scanned. Ids
    /// answer "at or before" because they ascend along junction order in
    /// every conversation, the same property the fork's inherited-history
    /// cursor is derived from.
    #[must_use]
    pub(crate) fn trailing_refusal_run(ledger: &[Block], anchor: i64) -> usize {
        ledger
            .iter()
            .rev()
            .take_while(|block| block.id > anchor)
            .filter(|block| block.dispatch_anchor == Some(anchor))
            .filter_map(|block| match BlockKind::from_block(block) {
                BlockKind::ToolError(error) => Some(error.records_refusal()),
                BlockKind::ToolResult(_) => Some(false),
                _ => None,
            })
            .take_while(|refusal| *refusal)
            .count()
    }

    /// The newest tool outcome in the ledger that ASKS FOR A CONTINUATION —
    /// any error, any result but an ends-turn-stamped one — whose turn is
    /// still UNANSWERED, answered as that turn's anchor.
    ///
    /// The fresh-dispatch inheritance's measure (2026-08-23, the verified
    /// seventh break — the resolution is ledger-first): a released turn
    /// resumes off its outcome — a parked interactive call's approval
    /// resolving, a restart recovering a round — and only the ledger knows
    /// it. The tail alone does not: a message absorbed behind the outcome
    /// becomes the tail, and a tail-only fresh resolution anchored the
    /// resumed continuation on the absorbed line — the consumer's original
    /// escalation, re-opened for released turns. So the fresh dispatch asks
    /// the snapshot: walk backward to the newest outcome, and if no
    /// assistant text block and no status block carrying that outcome's
    /// anchor follow it, the outcome's continuation has not happened — the
    /// turn is unanswered, and the dispatch inherits its anchor. A text
    /// block with the anchor is the continuation itself; a status block
    /// with the anchor is the turn's close marker — the interrupt's cap,
    /// the reader's abnormal-stop record, or the close's own turn-end
    /// marker (2026-08-23, turn closure is a stored fact: a close that ends
    /// a held identity over an unanswered outcome writes the end down,
    /// because the eighth break proved every side-effect-free end strands
    /// its outcome here forever); any one means the turn closed and its
    /// outcome captures nothing.
    ///
    /// A newest outcome with a NULL anchor — the out-of-band tool path —
    /// carries no identity to inherit and answers `None`: the consumer's
    /// documented fold for a null anchor is its floor, and reaching past it
    /// to an older outcome would stamp one turn's summoner on another
    /// turn's products.
    ///
    /// The accepted residual, documented with the rule and NARROWED by the
    /// stored closure (2026-08-23): the error-edge close now writes the
    /// turn-end marker whenever the store can still serve it, so only a
    /// turn ended while the store itself cannot write — the unstamped
    /// store-failure shape — leaves no status marker, and its outcome can
    /// over-attach one later summons. The direction is over-decline in the
    /// consumer's authority fold, and it self-heals at that turn's close —
    /// the attached turn's own text or status carries the anchor, and the
    /// outcome reads answered from then on.
    ///
    /// An ends-turn-stamped result is SKIPPED, and the walk looks PAST it
    /// (2026-08-30): the stamp says the turn is over, so no later summons may
    /// have its identity attached to that dead turn — and a sibling's own
    /// unanswered outcome sitting BEHIND an ends-turn tail still anchors the
    /// summons that inherits it. The widened residual is accepted with the
    /// rule: a stamped result no longer shields an OLDER stranded outcome —
    /// the store-failure shape the residual above documents — the way the
    /// newest-outcome cap did, so a summons can attach to an older dead turn
    /// than it could before. It is rarer than the defect the skip prevents,
    /// and the alternative — stopping at the first stamped result — would drop
    /// a sibling's inheritance across a restart, which is the shape the
    /// ledger-first resolution exists for.
    #[must_use]
    pub(crate) fn unanswered_outcome_anchor(ledger: &[Block]) -> Option<i64> {
        let (position, outcome) = ledger
            .iter()
            .enumerate()
            .rev()
            .find(|(_, block)| outcome_asks_for_a_continuation(block))?;
        let anchor = outcome.dispatch_anchor?;
        ledger[position + 1..]
            .iter()
            .all(|block| {
                block.dispatch_anchor != Some(anchor)
                    || !matches!(
                        BlockKind::from_block(block),
                        BlockKind::Text(_) | BlockKind::Status(_)
                    )
            })
            .then_some(anchor)
    }
}

/// What a call's outcome is, for a reader that needs to tell the two apart
/// (2026-09-02): [`ToolCall::outcome_in`] hands one back, and a call with no
/// outcome yet hands back nothing at all.
///
/// The kinds themselves, not a copy of what they say: whatever a resolution
/// records — the text, the turn-ending stamp, the refusal fact — the reader
/// gets, and nothing here has to grow a field when a resolution does.
#[derive(Debug, Clone)]
pub enum CallOutcome {
    /// The call came out with an answer.
    Result(ToolResult),
    /// The call came out with a failure, refused or attempted.
    Error(ToolError),
}

/// What one block answers, and for which call: the ledger row a tool outcome
/// names paired with the outcome itself, or `None` for every block that is not
/// a tool outcome and for one that names no call.
///
/// THE pairing read (2026-09-02). Every question about whether a call is
/// answered — the resolution predicate, the position the cut needs, the public
/// outcome reading, the ordering fold — comes through here, so a call and its
/// outcome are paired in exactly one place: every one of those readings takes
/// its comparison and its result-or-error classification from this one
/// function, and so they cannot drift apart. That includes the nameless
/// payload, whose id the outcome kinds read through the crate's `id_field`: it
/// answers no call here, and therefore answers none in any of them. The kinds
/// are read through [`BlockKind`], like every other outcome decision.
fn answered_call(block: &Block) -> Option<(i64, CallOutcome)> {
    match BlockKind::from_block(block) {
        BlockKind::ToolResult(result) => {
            let named = result.call_block_id?;
            Some((named, CallOutcome::Result(result)))
        }
        BlockKind::ToolError(error) => {
            let named = error.call_block_id?;
            Some((named, CallOutcome::Error(error)))
        }
        _ => None,
    }
}

/// The unresolved call this id names: the id back while the row it names is a
/// tool call still owing its body, and `None` for an id naming no row, a row
/// that is not a call, and a call whose outcome already sits in the ledger.
///
/// The tail both approval kinds' routing ends on (2026-09-03), written once so
/// the two cannot answer differently. Id-keyed and position-aware: the route
/// stays open only while the covered call itself is unresolved, so an unrelated
/// result appended later can never close it and the call's own result closes it
/// for good — that IS what keeps the redispatch walk idempotent, and without it
/// the walk re-executes an already-answered call on every tick. An id read as
/// `None` names nothing and there is no row to look up, which is how a payload
/// naming no call routes nowhere.
///
/// Resolution is answered through [`ToolCall::resolved_in`], THE resolution
/// predicate, and the row is read through [`BlockKind`], like every other
/// decision about what a block is.
pub(super) fn unresolved_call_named(ledger: &[Block], id: Option<i64>) -> Option<i64> {
    let named = id?;
    let call_block = ledger.iter().find(|block| block.id == named)?;
    match BlockKind::from_block(call_block) {
        BlockKind::ToolCall(call) if !call.resolved_in(ledger) => Some(named),
        _ => None,
    }
}

/// Is this block a tool outcome that asks the model for a continuation
/// (2026-08-30)?
///
/// The ONE reading of the turn-ending stamp for the turn folds above: a tool
/// error always asks — the model reads why and re-plans — and a tool result
/// asks unless its own row says the turn ended there. Read through
/// [`BlockKind`], like every other outcome decision, and off the stored stamp,
/// never off a tool's name.
///
/// Written once because both folds must agree by construction: the release
/// rule counts what is due, the fresh dispatch inherits what is unanswered,
/// and an ends-turn result that counted in one but not the other would strand a
/// sibling's continuation.
fn outcome_asks_for_a_continuation(block: &Block) -> bool {
    match BlockKind::from_block(block) {
        BlockKind::ToolResult(result) => !result.ends_turn,
        BlockKind::ToolError(_) => true,
        _ => false,
    }
}

impl Agency for ToolCall {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(if self.interactive {
            Awaiting::User
        } else {
            Awaiting::System
        })
    }

    async fn run<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<bool, StoreError> {
        if self.interactive {
            return Ok(true);
        }
        let ledger = ctx.store.list_blocks(ctx.conversation_id).await?;
        if self.resolved_in(&ledger) {
            return Ok(true);
        }
        ctx.bus.emit(CoreEvent::ToolCallReady {
            conversation_id: ctx.conversation_id,
            call_block_id: self.id,
        });
        Ok(false)
    }

    /// The deferred body IS the block's own `run()` — the ONE execution path:
    /// the walk's unwind re-emits the same wakeup the cursor emits, so the
    /// runner chokepoint stays the only place a tool body executes. A second
    /// execution path here is a second place a call can run twice.
    async fn run_post_gate<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<(), StoreError> {
        self.run(ctx).await.map(|_| ())
    }
}

impl Projection for ToolCall {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    /// A native part only — the model's own request is never echoed back as
    /// text. Non-JSON input rides as a JSON string, verbatim: degrade, never
    /// panic.
    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        let input =
            serde_json::from_str(&self.input).unwrap_or_else(|_| Value::String(self.input.clone()));
        Some(vec![ContentPart::ToolUse {
            id: self.tool_call_id.clone(),
            name: self.name.clone(),
            input,
        }])
    }

    fn forces_parts(&self) -> bool {
        true
    }
}
