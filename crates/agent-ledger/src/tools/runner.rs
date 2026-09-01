//! The runner — the single place a tool body executes, and therefore the
//! single place admission is enforced.
//!
//! Truth is the ledger; a wakeup is only a prompt to re-derive from it. Every
//! trigger — the live-tail drive at insert, the cursor's re-drive of an
//! unresolved call, the redispatch walk unwinding onto an approved one — lands
//! in the same pass, which reads the ledger fresh and answers from what is
//! recorded there. That is what collapses many triggers into exactly one body
//! execution without a queue, a lock or a run-once flag anywhere.
//!
//! Because no emitter path can route around this pass, three properties hold
//! under any interleaving: a refused call can never execute, a deferred call
//! can never execute unapproved, and one call can never carry both a success
//! and an error. ONE restart residual stands against the first of them,
//! recorded on the `claimed` read's own doc below: a pending body that ran
//! before a restart can be refused afterwards, and its late result loses the
//! conditional write — an in-process claim cannot survive the process.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use crate::agency::{
    AgencyCtx, BlockKind, FromBlock, GateDecision, Refusal, RuntimeKind, ToolCall, ToolError,
};
use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::store::{BlockDestination, StoreError, ToolCallInsert};

use super::admission::{ApprovalState, approval_state};
use super::{ToolContext, ToolHandler, ToolOutcome, ToolRegistry};

/// Where a recorded call came from — the second half of the runner's insert
/// seam beside the call's own facts.
///
/// The default is the public out-of-band form: no streamed tail to replace,
/// no dispatch anchor — the anchor is written by the framework's own paths
/// only, and NULL is the documented out-of-band answer. The streamed form is
/// `pub(crate)` by construction: only the streaming reader can name the tail
/// its final call replaces and the anchor its per-turn seam holds.
#[derive(Debug, Default, Clone, Copy)]
pub struct CallOrigin {
    pub(crate) replaces_streaming: Option<i64>,
    pub(crate) dispatch_anchor: Option<i64>,
}

impl CallOrigin {
    /// The streaming reader's finalization: the streamed-input tail this
    /// final call replaces (deleted in the same transaction), and the open
    /// turn's dispatch anchor.
    pub(crate) fn streamed(replaces_streaming: Option<i64>, dispatch_anchor: Option<i64>) -> Self {
        Self {
            replaces_streaming,
            dispatch_anchor,
        }
    }
}

/// The CONVERSATION window's numbers: how many calls a conversation may
/// record in a trailing span, and how long a run of rate-limit refusals —
/// this window's or a single tool's ([`ToolWindowBound`]) — may get before the
/// turn is forced to end (2026-08-30).
///
/// The operator of the deployment this slice was cut for chose them: a
/// runaway turn looped a failing tool for hundreds of rounds, one paid
/// request each, until the provider refused payment. The defaults below ARE
/// that decision; the type exists so a test can run a small window without a
/// second copy of the numbers appearing anywhere.
///
/// Held by the runner, because the runner owns admission and the window is an
/// admission answer. The forced end reads the same field through the
/// context's runner accessor rather than keeping a copy: one home, two
/// readers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ToolCallWindow {
    /// How many calls one conversation may record in the trailing span. The
    /// call under admission is itself recorded, and the refusal fires when
    /// the count EXCEEDS this — so exactly this many calls run inside any
    /// trailing span, and the next one is refused.
    pub calls: usize,
    /// The span, in seconds, the count is taken over.
    pub seconds: i64,
    /// How many of a turn's trailing tool outcomes must all be REFUSALS
    /// before the turn is forced to end between rounds. ONE number for the
    /// whole runtime, and one run for every producer: this window's refusal, a
    /// tool's own [`ToolWindowBound`] refusal and a consumer's own decline all
    /// lengthen it, because each says the model is spending rounds on
    /// refusals. What counts is the outcome row's own fact
    /// ([`Refusal`](crate::agency::Refusal)).
    pub consecutive_limit: usize,
}

impl Default for ToolCallWindow {
    fn default() -> Self {
        Self {
            calls: 60,
            seconds: 60,
            consecutive_limit: 5,
        }
    }
}

/// ONE TOOL's own window: how many calls of that name a conversation may
/// record in a trailing span (2026-08-30).
///
/// The conversation window bounds PACE, and a slow grind is in-rate: a turn
/// that ground one failing tool for hours, well under sixty calls a minute,
/// never tripped it. This bound closes the gap from the other side — a model
/// leaning that hard on one tool is looping, whatever its overall pace.
///
/// The CONSUMER's numbers, unlike [`ToolCallWindow`]'s: which tools exist and
/// how hard one may be leaned on is knowledge only the embedder has, so the
/// map these live in ships EMPTY and this library names no tool anywhere. A
/// tool with no entry meets no per-tool bound at all.
///
/// No consecutive limit of its own: the run that forces a turn to end is one
/// number for the whole runtime
/// ([`ToolCallWindow::consecutive_limit`]), and both windows' refusals feed it
/// the same way every other refusal does — through the fact each outcome row
/// carries ([`Refusal`](crate::agency::Refusal)).
#[derive(Debug, Clone, Copy)]
pub(crate) struct ToolWindowBound {
    /// How many calls of this tool one conversation may record in the
    /// trailing span. The call under admission is itself recorded, and the
    /// refusal fires when the count EXCEEDS this — so exactly this many calls
    /// of the tool run inside any trailing span, and the next one is refused.
    pub calls: usize,
    /// The span, in seconds, the count is taken over.
    pub seconds: i64,
}

/// Is a bound spent — the ONE comparison both windows answer through
/// (2026-08-30).
///
/// The count is the ledger's ([`ToolCall::calls_in_trailing_window`]), and the
/// call under admission is one of the rows it counts: insert-first put it
/// there before any hook ran. So a bound is spent when the count EXCEEDS it —
/// the bound's worth of calls all run, and the first one past it inside any
/// trailing span is refused. `of_tool` is what makes the same fold answer for
/// one tool instead of for the conversation.
fn window_spent(ledger: &[Block], calls: usize, seconds: i64, of_tool: Option<&str>) -> bool {
    ToolCall::calls_in_trailing_window(ledger, seconds, of_tool) > calls
}

/// The admission chokepoint, holding the registry it resolves calls against.
///
/// One per running application. The in-flight set is a SAME-PROCESS duplicate
/// guard and nothing more — a wakeup can re-arrive before a result row commits,
/// and a handler that returned [`Pending`](ToolOutcome::Pending) stays claimed
/// until its backing system writes the resolution. Correctness never rests on
/// it: the conditional resolution writes are what make a second outcome
/// impossible to record, and the body is deliberately at-least-once across a
/// crash — the ledger holds exactly one resolution either way.
pub struct ToolRunner<K: RuntimeKind, E> {
    registry: Arc<ToolRegistry<E>>,
    in_flight: Mutex<HashSet<(i64, i64)>>,
    /// The conversation-wide tool-call window in force. Written EXACTLY ONCE,
    /// at construction: a production build takes [`ToolCallWindow`]'s
    /// defaults — the operator's numbers — and nothing in the crate can write
    /// it after the runner is shared, because the only other writer takes
    /// `&mut self` ([`Self::set_window`], test-only) and the compiler grants
    /// that exclusively while the builder still holds the sole reference. An
    /// immutable field needs no lock, and carrying one would have said the
    /// opposite: that some runtime writer exists.
    window: ToolCallWindow,
    /// The per-tool windows in force, keyed by the name a call records —
    /// EMPTY by default, because this library ships no tool names
    /// (2026-08-30).
    ///
    /// A SECOND plain field beside `window`, not a map inside it: a window
    /// carrying the map would lose its `Copy` and clone a map on every
    /// admission, where this is read by shared reference — a `get` by name.
    /// Written exactly like `window` is, at construction through
    /// [`Self::set_tool_window`], whose `&mut self` is the same compiler proof
    /// that the write lands while the builder still holds the sole reference,
    /// so every reader afterwards takes it without a lock.
    tool_windows: HashMap<String, ToolWindowBound>,
    /// The kind the live-tail drive at the insert seam parses through — a
    /// type-level collaborator the runner never owns. The `fn() -> K` form is
    /// what says so to the compiler: the runner's auto traits and drop check
    /// stay independent of `K`'s.
    _kind: PhantomData<fn() -> K>,
}

/// A held claim, released when dropped. Every exit path releases it — the
/// early stand-downs, the recorded outcomes, and a PANICKING tool body alike —
/// so no path can leak the claim for the life of the process and park the call
/// against every later wakeup. A [`Pending`](ToolOutcome::Pending) body calls
/// [`keep`](Self::keep), the one deliberate exception: the call stays claimed
/// until its backing system resolves it in the ledger.
///
/// Keyed on the call BLOCK id, like everything else about a call's identity:
/// a model's `tool_call_id` can repeat, and two calls sharing one provider id
/// must claim independently.
struct Claim<'r, K: RuntimeKind, E> {
    runner: &'r ToolRunner<K, E>,
    conversation_id: i64,
    call_block_id: i64,
    held: bool,
}

impl<K: RuntimeKind, E> Claim<'_, K, E> {
    /// Keep the claim past this pass: the backing system owns the resolution.
    fn keep(mut self) {
        self.held = false;
    }
}

impl<K: RuntimeKind, E> Drop for Claim<'_, K, E> {
    fn drop(&mut self) {
        if self.held {
            self.runner
                .release(self.conversation_id, self.call_block_id);
        }
    }
}

impl<K: RuntimeKind, E> ToolRunner<K, E> {
    /// A runner over this registry. The `Arc` is shared rather than owned
    /// because a consumer also builds the model's schema list from it.
    #[must_use]
    pub fn new(registry: Arc<ToolRegistry<E>>) -> Self {
        Self {
            registry,
            in_flight: Mutex::new(HashSet::new()),
            window: ToolCallWindow::default(),
            tool_windows: HashMap::new(),
            _kind: PhantomData,
        }
    }

    /// The registry this runner resolves against.
    #[must_use]
    pub fn registry(&self) -> &ToolRegistry<E> {
        &self.registry
    }

    /// The conversation-wide tool-call window in force — read by this runner's
    /// own refusal and, through the context's runner accessor, by the forced
    /// end the actor runs between rounds. One field, both readers.
    pub(crate) fn window(&self) -> ToolCallWindow {
        self.window
    }

    /// Set the conversation-wide window, at construction time.
    ///
    /// Test-only, and that IS the surface decision for the GLOBAL numbers:
    /// they are the operator's, recorded once as [`ToolCallWindow`]'s
    /// defaults, so a production build has nothing that can disagree with
    /// them. The library's own suites need a small window to prove the
    /// behavior without spending sixty calls per assertion, and they reach it
    /// through the context builder that calls this. A per-tool bound is
    /// inherently the CONSUMER's and reaches this runner through the public
    /// context builder (`RuntimeContext::with_tool_window`), which calls the
    /// crate-private [`Self::set_tool_window`] — a different decision about
    /// different numbers, not a hole in this one.
    ///
    /// `&mut self` is the point, not an incidental signature: it is the
    /// compiler's own proof that the write lands while the runner is still
    /// unshared, which is what lets every reader take the field without a
    /// lock.
    #[cfg(test)]
    pub(crate) fn set_window(&mut self, window: ToolCallWindow) {
        self.window = window;
    }

    /// Bind one tool to a window of its own, at construction time
    /// (2026-08-30). A second call for the same name replaces that tool's
    /// bound; every other tool is untouched.
    ///
    /// Crate-private and NOT test-gated, unlike [`Self::set_window`]: these
    /// numbers are the consumer's, and production's one caller is the PUBLIC
    /// builder [`RuntimeContext::with_tool_window`](crate::RuntimeContext::with_tool_window),
    /// which reaches this through the sole reference it still holds. Public
    /// HERE it must not be: the context hands out cloneable `Arc`s of the
    /// runner through its accessor, so a public setter would let any holder
    /// write the map mid-flight — and the map is read without a lock exactly
    /// because nothing can.
    ///
    /// `&mut self` carries the same proof it carries above: the write lands
    /// while the runner is still unshared.
    pub(crate) fn set_tool_window(&mut self, name: String, bound: ToolWindowBound) {
        self.tool_windows.insert(name, bound);
    }

    /// The in-flight set's ONE lock, poison recovery included: a panicking
    /// tool body must not disable the guard for the rest of the process, and
    /// the set carries no invariant a panic could break — it is a set of
    /// claims — so the poison is cleared and the guard taken anyway. Every
    /// site that touches the set comes through here, so the recovery is
    /// decided once.
    fn lock_in_flight(&self) -> std::sync::MutexGuard<'_, HashSet<(i64, i64)>> {
        self.in_flight.lock().unwrap_or_else(|poisoned| {
            self.in_flight.clear_poison();
            poisoned.into_inner()
        })
    }

    /// Claim a call for this process, or `None` when it is already claimed.
    ///
    /// The lock is released before anything is awaited: holding a
    /// synchronous lock across an await point parks every other conversation's
    /// runner behind whichever tool body happens to be slowest.
    fn claim(&self, conversation_id: i64, call_block_id: i64) -> Option<Claim<'_, K, E>> {
        let mut in_flight = self.lock_in_flight();
        in_flight
            .insert((conversation_id, call_block_id))
            .then(|| Claim {
                runner: self,
                conversation_id,
                call_block_id,
                held: true,
            })
    }

    /// Whether this process currently holds a claim on the call — the read
    /// half of [`Self::claim`], peeked by the window's refusal.
    ///
    /// PER-PROCESS, and the residual is stated openly (2026-08-30): a
    /// re-driven pending call whose body ran BEFORE a restart carries no
    /// claim mark afterwards, so a hot window refuses it, the refusal's "this
    /// call was not run" lands false for that one call, and the pre-restart
    /// result loses its conditional write. Rare, restart-shaped, and no worse
    /// than the check-to-append race the forced end records for itself.
    fn claimed(&self, conversation_id: i64, call_block_id: i64) -> bool {
        self.lock_in_flight()
            .contains(&(conversation_id, call_block_id))
    }

    /// Release a claim once the call is resolved in the ledger.
    fn release(&self, conversation_id: i64, call_block_id: i64) {
        self.lock_in_flight()
            .remove(&(conversation_id, call_block_id));
    }
}

impl<K: RuntimeKind, E: RuntimeEvent> ToolRunner<K, E> {
    /// Record a tool call, then drive it once.
    ///
    /// **Insert-first, then act.** The block is in the ledger before any hook
    /// runs on it, so anything the hook records — an approval request, a
    /// refusal — names the call's REAL row id rather than a predicted one.
    ///
    /// This is also the one seam with registry access, which is why the
    /// `interactive` stamp is read here and written onto the block: from then
    /// on the block answers who owes its next move out of its own data, and a
    /// tool that later leaves the registry cannot change the answer for calls
    /// already recorded.
    ///
    /// The drive at insert is what keeps parallel calls parallel. The cursor
    /// alone parks on the first unresolved call, so a sibling inserted behind
    /// it would never emit its wakeup until the first resolved; driving here
    /// emits both immediately, and the runner's ledger idempotency collapses
    /// the two triggers into one execution each.
    ///
    /// While `latched` the block is RECORDED and nothing acts — recording data
    /// is not orchestration. The call dangles until the next unlatched tick,
    /// where the cursor's re-drive heals it.
    ///
    /// `origin` carries where the call came from: the default is the public
    /// out-of-band form, which opened no streamed tail and records no anchor —
    /// the documented NULL a consumer folds to its floor. The streaming
    /// reader's finalization constructs the crate-only streamed form, naming
    /// the tail it replaces and the turn's anchor from its per-turn seam.
    ///
    /// # Errors
    ///
    /// If the insert fails, if the block cannot be read back, or if the block's
    /// own drive fails.
    pub async fn insert_call(
        &self,
        ctx: &AgencyCtx<E>,
        latched: bool,
        tool_call_id: String,
        name: String,
        input: String,
        origin: CallOrigin,
    ) -> Result<i64, StoreError> {
        let interactive = self
            .registry
            .get(&name)
            .is_some_and(<dyn ToolHandler<E>>::interactive);
        let block_id = ctx
            .store
            .insert_tool_call_block(
                BlockDestination::anchored(ctx.conversation_id, origin.dispatch_anchor),
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id,
                    name,
                    input,
                    interactive,
                },
                origin.replaces_streaming,
            )
            .await?;
        if latched {
            return Ok(block_id);
        }
        if let Some(block) = ctx.store.find_block(block_id).await? {
            K::from_block(&block).run(ctx).await?;
        }
        Ok(block_id)
    }

    /// Feed one wakeup to the chokepoint.
    ///
    /// A wakeup delivered while latched is DROPPED, not deferred. The latch is
    /// a full short-circuit rather than a pause button: queueing the wakeup
    /// here would make the runner a second retry mechanism beside the ratchet,
    /// and the two would disagree about what is still owed. The parked call
    /// re-emits on the next unlatched tick, which is the same recovery that
    /// covers a wakeup lost to a lagging subscriber.
    pub async fn run_wakeup(&self, ctx: &AgencyCtx<E>, latched: bool, call_block_id: i64) {
        if latched {
            return;
        }
        self.execute_ready_call(ctx, call_block_id).await;
    }

    /// One pass of admission, from a fresh read of the ledger.
    ///
    /// The order is the design, not an implementation detail:
    ///
    /// 1. **Find the call**, by its BLOCK id — a model's `tool_call_id` can
    ///    repeat, the block id cannot, so a wakeup names exactly one call.
    ///    Absent — a stale wakeup for another ledger — is a silent return.
    /// 2. **Already resolved?** Return. This is what makes a duplicate wakeup
    ///    free, and it comes FIRST so no later step can re-run a completed
    ///    body.
    /// 3. **Is a tool-call window spent** — the conversation's, or this
    ///    tool's own (2026-08-30)? If either is, this call resolves with that
    ///    window's refusal and NOTHING
    ///    else runs. It comes before every other refusal, the unknown tool
    ///    included: a hot window meeting an unknown tool name would otherwise
    ///    loop on the teaching text, a second unbounded shape. What the
    ///    window never touches is a call whose body may already have run —
    ///    one claimed by this process, one carrying a granted human approval
    ///    (step 6's `Approved`) — and an interactive call, whose admission
    ///    belongs to the human; refusing any of those would write "this call
    ///    was not run" over a call that did run, and orphan its real result
    ///    at the conditional write. The window governs FRESH admissions
    ///    alone. The interactive skip is defensive only: an interactive call
    ///    is answered by the human and never reaches this pass at all.
    /// 4. **Resolve the tool.** Unknown resolves the call with an error rather
    ///    than leaving it dangling: a call nobody can run is still a call the
    ///    model is waiting on.
    /// 5. **Ungated?** Straight to the body. No evaluation, no record of one.
    /// 6. **Read the recorded standing** and, only where nothing is recorded
    ///    yet, ask the tool's own gate — which records its answer before
    ///    anything acts on it.
    async fn execute_ready_call(&self, ctx: &AgencyCtx<E>, call_block_id: i64) {
        let ledger = match ctx.store.list_blocks(ctx.conversation_id).await {
            Ok(ledger) => ledger,
            Err(error) => {
                warn!(
                    conversation_id = ctx.conversation_id,
                    %error,
                    "tool runner: reading the ledger failed"
                );
                return;
            }
        };

        // The call row is one of the LIBRARY's own kinds, so it is read
        // through the library's own parse: a composed consumer kind delegates
        // the core type strings inward to the same place.
        let Some(call) = ledger
            .iter()
            .filter(|block| block.id == call_block_id)
            .find_map(|block| match BlockKind::from_block(block) {
                BlockKind::ToolCall(call) => Some(call),
                _ => None,
            })
        else {
            return;
        };
        if call.resolved_in(&ledger) {
            return;
        }

        // The human's standing with this call is folded ONCE on this
        // immutable snapshot and handed to both halves below: the windows'
        // fresh-admission skip and the gate's own admit read the same
        // ledger, and asking it twice would be two folds that exist only to
        // agree with each other.
        let approval = approval_state(&ledger, call.id);

        if self.window_refuses(ctx, &ledger, &call, approval).await {
            return;
        }

        let Some(handler) = self.registry.get(&call.name) else {
            warn!(name = call.name, "tool runner: no handler registered");
            // The refusal carries the fix: the names that WOULD resolve, in
            // the registry's sorted order. If visibility tiers ever land, a
            // HIDDEN tool must produce this exact unknown-tool wording — any
            // difference between the two answers discloses the hidden tool's
            // existence — and the listing must then be built from the EXPOSED
            // set, not the whole registry, or the list itself names every
            // hidden tool.
            let known: Vec<&str> = self.registry.names().collect();
            let unknown = if known.is_empty() {
                format!("unknown tool: {} (no tools are registered)", call.name)
            } else {
                format!(
                    "unknown tool: {}. The registered tools are: {}",
                    call.name,
                    known.join(", ")
                )
            };
            // An ordinary outcome, not a refusal (2026-09-01): the sentence
            // above hands the model the names that WOULD resolve, so the next
            // round can succeed and the model is not looping against a
            // standing no. See [`Refusal`].
            self.resolve_with_error(ctx, &call.tool_call_id, unknown, call.id, Refusal::Failed)
                .await;
            return;
        };

        if handler.gated() && !self.admit(ctx, handler, &call, approval).await {
            return;
        }
        self.run_body(ctx, handler, &call).await;
    }

    /// The tool-call windows' admission answer (2026-08-30): is this
    /// conversation's window spent, is THIS TOOL's own window spent, and is
    /// this a call either may refuse?
    ///
    /// Answers whether the call was refused — `true` means the refusal is
    /// already recorded and the caller must stop, exactly like the gate's
    /// own refusal path.
    ///
    /// ONE pass at the door, for both bounds. The fresh-admission skips run
    /// ONCE and cover the two: a call whose body may already have run — one
    /// claimed by this process, one carrying a granted human approval — and
    /// an interactive call, whose admission belongs to the human, are no more
    /// refusable by a tool's window than by the conversation's. The human's
    /// standing arrives from the caller, folded once on the snapshot this
    /// skip and the gate's own admit below both read.
    ///
    /// The GLOBAL window speaks FIRST and a tool's own second (2026-08-30).
    /// The order is observable only in which text lands, and the
    /// conversation's window is the outer protection: it answers while it
    /// can. A tool with no entry in the map meets no second check at all.
    ///
    /// Both counts are the ledger's ([`ToolCall::calls_in_trailing_window`]),
    /// one walk each, differing only in the name filter — and both are spent
    /// on the one comparison ([`window_spent`]), the call under admission
    /// included. A refused call stays a recorded call and keeps counting
    /// against BOTH windows: a window that forgot its own refusals would
    /// drain while the model spams and hand the whole protection to the
    /// forced end.
    async fn window_refuses(
        &self,
        ctx: &AgencyCtx<E>,
        ledger: &[Block],
        call: &ToolCall,
        approval: ApprovalState,
    ) -> bool {
        if call.interactive || self.claimed(ctx.conversation_id, call.id) {
            return false;
        }
        if approval == ApprovalState::Approved {
            return false;
        }

        let window = self.window();
        if window_spent(ledger, window.calls, window.seconds, None) {
            return self
                .record_window_refusal(
                    ctx,
                    call,
                    "the conversation's tool-call window",
                    window.calls,
                    window.seconds,
                    ToolError::rate_limit_refusal(window.calls, window.seconds),
                )
                .await;
        }

        let Some(bound) = self.tool_windows.get(&call.name) else {
            return false;
        };
        if !window_spent(ledger, bound.calls, bound.seconds, Some(&call.name)) {
            return false;
        }
        self.record_window_refusal(
            ctx,
            call,
            "this tool's own window",
            bound.calls,
            bound.seconds,
            ToolError::per_tool_rate_limit_refusal(&call.name, bound.calls, bound.seconds),
        )
        .await
    }

    /// Record one window's refusal and answer `true`: the spent window's log
    /// line, the rendered refusal and the conditional write that resolves the
    /// call are ONE shape every window refusal takes, so a future change to
    /// how a refusal is recorded is made here once. The conversation's
    /// window and a tool's own differ only in `which` window spent, the
    /// rendered text and the numbers it names.
    async fn record_window_refusal(
        &self,
        ctx: &AgencyCtx<E>,
        call: &ToolCall,
        which: &str,
        calls: usize,
        seconds: i64,
        refusal: String,
    ) -> bool {
        info!(
            name = call.name,
            conversation_id = ctx.conversation_id,
            calls,
            seconds,
            "tool runner: {which} is spent — refusing"
        );
        self.resolve_with_error(ctx, &call.tool_call_id, refusal, call.id, Refusal::Refused)
            .await;
        true
    }

    /// The admission half: act on the human's standing with this call —
    /// handed down folded once on the admission snapshot both halves read —
    /// and consult the tool's own gate only where nothing is recorded yet.
    ///
    /// Answers whether the body may run. Every refusal it makes is RECORDED
    /// before it returns — that is the whole point of the split: the caller
    /// learns a boolean, but the ledger learns the decision, and the ledger is
    /// what the next wakeup reads.
    async fn admit(
        &self,
        ctx: &AgencyCtx<E>,
        handler: &dyn ToolHandler<E>,
        call: &ToolCall,
        approval: ApprovalState,
    ) -> bool {
        match approval {
            ApprovalState::Approved => true,
            // Undecided: the human still owes this. Denied: the verdict already
            // resolved the call with its error, atomically, so there is nothing
            // left to record and never a body to run.
            ApprovalState::Undecided | ApprovalState::Denied => false,
            ApprovalState::Unrequested => match handler.gate(&call.input).await {
                GateDecision::Proceed => true,
                GateDecision::Refuse { reason } => {
                    // The tool refused THIS input and said why, so the model
                    // has something to correct: an ordinary outcome, ending
                    // the trailing run. See [`Refusal`].
                    self.resolve_with_error(
                        ctx,
                        &call.tool_call_id,
                        reason,
                        call.id,
                        Refusal::Failed,
                    )
                    .await;
                    false
                }
                GateDecision::Defer => {
                    // Insert-first holds by construction: the call is already in
                    // the ledger, so the request records its real id. The write
                    // is conditional on no request covering the call yet, so
                    // two wakeups whose reads overlapped still append exactly
                    // one request — the loser's `None` is a stale pass, stood
                    // down without a retry.
                    match ctx
                        .store
                        .insert_approval_request_block(ctx.conversation_id, call.id)
                        .await
                    {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            tracing::debug!(
                                conversation_id = ctx.conversation_id,
                                tool_call_id = call.tool_call_id,
                                "tool runner: a request already covers this call, standing down"
                            );
                        }
                        Err(error) => {
                            warn!(
                                conversation_id = ctx.conversation_id,
                                %error,
                                "tool runner: parking the call for clearance failed"
                            );
                        }
                    }
                    false
                }
            },
        }
    }

    /// The body half, identical for every admitted call: claim it against a
    /// duplicate wakeup, re-read under the claim, run it, and record whatever
    /// it produced.
    ///
    /// This is also the one seam that holds the HANDLER at resolution time,
    /// which is why the turn-ending stamp is read here and written onto the
    /// resolution (2026-08-30): from then on the block answers whether it asks
    /// the model for anything out of its own data, and a tool that later
    /// leaves the registry cannot change the answer for turns already ended.
    /// The insert seam reads [`ToolHandler::interactive`] for the same reason
    /// at the other end of a call's life.
    async fn run_body(&self, ctx: &AgencyCtx<E>, handler: &dyn ToolHandler<E>, call: &ToolCall) {
        let Some(claim) = self.claim(ctx.conversation_id, call.id) else {
            tracing::debug!(
                conversation_id = ctx.conversation_id,
                tool_call_id = call.tool_call_id,
                "tool runner: already in flight, skipping"
            );
            return;
        };

        // The admission snapshot and this claim are separated by awaits, so
        // the snapshot can be stale: a sibling wakeup may have run the body,
        // recorded the result and released between this pass's read and its
        // claim. Re-read UNDER the claim — nothing else in this process can
        // start the body while it is held — and stand down if the call
        // resolved in the meantime. This is what keeps overlapping wakeups at
        // one execution; the conditional writes below are the ledger's own
        // backstop for whatever this process cannot see.
        match ctx.store.list_blocks(ctx.conversation_id).await {
            Ok(ledger) if call.resolved_in(&ledger) => {
                tracing::debug!(
                    conversation_id = ctx.conversation_id,
                    tool_call_id = call.tool_call_id,
                    "tool runner: resolved between snapshot and claim, standing down"
                );
                return;
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    conversation_id = ctx.conversation_id,
                    %error,
                    "tool runner: re-reading the ledger under the claim failed"
                );
                return;
            }
        }

        info!(
            name = call.name,
            conversation_id = ctx.conversation_id,
            "tool runner: executing"
        );
        let outcome = handler
            .execute(
                &call.input,
                ToolContext {
                    agency: ctx,
                    tool_call_id: &call.tool_call_id,
                    block_id: call.id,
                },
            )
            .await;
        let ends_turn = handler.ends_turn();
        match outcome {
            ToolOutcome::Done(result) => {
                match ctx
                    .store
                    .complete_tool_call_block_stamped(
                        ctx.conversation_id,
                        call.tool_call_id.clone(),
                        result,
                        call.id,
                        ends_turn,
                    )
                    .await
                {
                    Ok(Some(_)) => {}
                    // A lost conditional write is a stale pass: the ledger
                    // already carries the call's outcome. Stand down.
                    Ok(None) => {
                        tracing::debug!(
                            conversation_id = ctx.conversation_id,
                            tool_call_id = call.tool_call_id,
                            "tool runner: result already recorded, standing down"
                        );
                    }
                    Err(error) => {
                        warn!(
                            conversation_id = ctx.conversation_id,
                            %error,
                            "tool runner: recording the result failed"
                        );
                    }
                }
            }
            ToolOutcome::Error(error) => {
                self.resolve_with_error(ctx, &call.tool_call_id, error, call.id, Refusal::Failed)
                    .await;
            }
            // The consumer DECLINED the call: same recorded outcome, same
            // re-planning round for the model, and the row carries the fact
            // (2026-09-01) — so a loop of declines runs into the forced end of
            // the turn instead of going on until the model tires of it. The
            // words are the consumer's; the fact is the framework's.
            ToolOutcome::Refused(reason) => {
                self.resolve_with_error(ctx, &call.tool_call_id, reason, call.id, Refusal::Refused)
                    .await;
            }
            // An ends-turn tool that defers is refused, loudly (2026-08-30):
            // the backing system would resolve through the public door, which
            // holds no handler and writes no stamp, so the end of the turn
            // would be lost and the model summoned after it. The contract is
            // closed here rather than widened there, and the error — which
            // never carries the stamp — hands the model its ordinary round.
            ToolOutcome::Pending if ends_turn => {
                warn!(
                    name = call.name,
                    conversation_id = ctx.conversation_id,
                    "tool runner: an ends-turn tool deferred its own resolution — refusing"
                );
                self.resolve_with_error(
                    ctx,
                    &call.tool_call_id,
                    ToolError::ENDS_TURN_DEFERRAL_REFUSAL.to_owned(),
                    call.id,
                    Refusal::Failed,
                )
                .await;
            }
            // The backing system resolves it; the claim stays until it does.
            ToolOutcome::Pending => claim.keep(),
        }
        // Every other path releases here, as `claim` drops — including a
        // panicking body, whose unwind drops it the same way.
    }

    /// Resolve a call with a tool error. Every refusal the runner records goes
    /// through here, so a refusal is always a block the model re-plans against
    /// and never an exception that leaves the call dangling. A lost conditional
    /// write is a stale pass — the call already carries an outcome — and is
    /// stood down, never retried.
    ///
    /// The caller names WHICH KIND of outcome this is (2026-09-01): a
    /// [`Refusal::Refused`] is a decision not to run the call, a
    /// [`Refusal::Failed`] is a run that went wrong. The fact is stored on the
    /// outcome row, so the forced end of a turn counts refusals off the row
    /// instead of reading the words back out of the error text.
    async fn resolve_with_error(
        &self,
        ctx: &AgencyCtx<E>,
        tool_call_id: &str,
        error: String,
        call_block_id: i64,
        refusal: Refusal,
    ) {
        match ctx
            .store
            .fail_tool_call_block_marked(
                ctx.conversation_id,
                tool_call_id.to_string(),
                error,
                call_block_id,
                refusal,
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::debug!(
                    conversation_id = ctx.conversation_id,
                    tool_call_id,
                    "tool runner: an outcome is already recorded, standing down"
                );
            }
            Err(failure) => {
                warn!(
                    conversation_id = ctx.conversation_id,
                    error = %failure,
                    "tool runner: recording the error failed"
                );
            }
        }
    }
}
