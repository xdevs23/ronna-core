//! A model's request for tool work.

use serde_json::Value;

use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::event::CoreEvent;
use crate::store::StoreError;
use crate::types::Awaiting;

use super::projection::{ContentPart, Projection};
use super::{Agency, AgencyCtx, BlockKind, FromBlock};

/// A model's request for tool work.
///
/// A regular call owes the SYSTEM its execution: `run()` emits the wakeup and
/// stays not-done until a result or an error with this call's id follows in the
/// ledger. Parking the cursor here IS the guard against streaming a fresh turn
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
    /// Does a result or an error for this call follow it in the ledger?
    ///
    /// Keyed on the specific call id, never on kind alone — an unrelated result
    /// landing later must never settle this call. This is THE resolution
    /// predicate: the approval kinds' routing and the runner's ledger
    /// idempotency all answer through it, so they cannot drift apart.
    ///
    /// The kinds it accepts are read through [`BlockKind`], not off the stored
    /// type string. Comparing the string here would be a second place that
    /// decides what a tool outcome is, and the two would answer differently the
    /// first time either changed.
    ///
    /// **An absent id matches nothing.** A payload without `tool_call_id`
    /// parses to the empty string, and two such blocks would cross-match on
    /// it — an unrelated failure would silently resolve this call. The store
    /// constrains its own writes `NOT NULL`, but this predicate runs over
    /// parsed JSON rather than over that constraint, so it answers false the
    /// moment there is no id to key on.
    #[must_use]
    pub fn resolved_in(&self, ledger: &[Block]) -> bool {
        if self.tool_call_id.is_empty() {
            return false;
        }
        ledger
            .iter()
            .skip_while(|block| block.id != self.id)
            .skip(1)
            .any(|block| match BlockKind::from_block(block) {
                BlockKind::ToolResult(result) => result.tool_call_id == self.tool_call_id,
                BlockKind::ToolError(error) => error.tool_call_id == self.tool_call_id,
                _ => false,
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
    /// must not keep this turn's identity alive. Two calls are NOT that
    /// proof and are excluded by the narrowing, because each pinned an
    /// identity indefinitely:
    ///
    /// - An INTERACTIVE call parks on the user, who may never answer. Its
    ///   close ends the turn; a later approval writes the outcome with the
    ///   call's own anchor, and the tail inheritance re-attaches the
    ///   identity at that dispatch.
    /// - An EMPTY-ID call can never be resolved at all —
    ///   [`Self::resolved_in`] matches outcomes on the id, and an absent id
    ///   matches nothing — so counting it as owed reads as owed forever.
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
                        if !call.interactive
                            && !call.tool_call_id.is_empty()
                            && !call.resolved_in(ledger)
                )
        })
    }

    /// How many tool outcomes — results and errors — the ledger holds
    /// anchored on `anchor`.
    ///
    /// The other arm's measure (2026-08-23): every outcome a turn's calls
    /// produce summons one continuation, so an outcome count above what the
    /// turn's dispatches have already answered is a continuation still due.
    /// The kinds are read through [`BlockKind`], like every other outcome
    /// decision, and the anchor keying keeps another turn's outcomes out of
    /// this turn's count.
    #[must_use]
    pub(crate) fn outcomes_anchored_in(ledger: &[Block], anchor: i64) -> usize {
        ledger
            .iter()
            .filter(|block| {
                block.dispatch_anchor == Some(anchor)
                    && matches!(
                        BlockKind::from_block(block),
                        BlockKind::ToolResult(_) | BlockKind::ToolError(_)
                    )
            })
            .count()
    }

    /// The newest tool outcome in the ledger — a result or an error — whose
    /// turn is still UNANSWERED, answered as that turn's anchor.
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
    #[must_use]
    pub(crate) fn unanswered_outcome_anchor(ledger: &[Block]) -> Option<i64> {
        let (position, outcome) = ledger.iter().enumerate().rev().find(|(_, block)| {
            matches!(
                BlockKind::from_block(block),
                BlockKind::ToolResult(_) | BlockKind::ToolError(_)
            )
        })?;
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
