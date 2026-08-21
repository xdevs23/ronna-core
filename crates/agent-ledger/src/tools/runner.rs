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
//! and an error.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use crate::agency::{Agency, AgencyCtx, BlockKind, GateDecision, ToolCall};
use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::store::{StoreError, ToolCallInsert};

use super::admission::{ApprovalState, approval_state};
use super::{ToolContext, ToolHandler, ToolOutcome, ToolRegistry};

/// The admission chokepoint, holding the registry it resolves calls against.
///
/// One per running application. The in-flight set is a SAME-PROCESS duplicate
/// guard and nothing more — a wakeup can re-arrive before a result row commits,
/// and a handler that returned [`Pending`](ToolOutcome::Pending) stays claimed
/// until its backing system writes the resolution. Correctness never rests on
/// it: the ledger check above it is what makes a second execution impossible to
/// record, and the body is deliberately at-least-once across a crash, where a
/// second result is the recovery.
pub struct ToolRunner<E> {
    registry: Arc<ToolRegistry<E>>,
    in_flight: Mutex<HashSet<(i64, String)>>,
}

impl<E> ToolRunner<E> {
    /// A runner over this registry. The `Arc` is shared rather than owned
    /// because a consumer also builds the model's schema list from it.
    #[must_use]
    pub fn new(registry: Arc<ToolRegistry<E>>) -> Self {
        Self {
            registry,
            in_flight: Mutex::new(HashSet::new()),
        }
    }

    /// The registry this runner resolves against.
    #[must_use]
    pub fn registry(&self) -> &ToolRegistry<E> {
        &self.registry
    }

    /// Claim a call for this process, or report that it is already claimed.
    ///
    /// The guard is released before anything is awaited: holding a
    /// synchronous lock across an await point parks every other conversation's
    /// runner behind whichever tool body happens to be slowest.
    fn claim(&self, conversation_id: i64, tool_call_id: &str) -> bool {
        let mut in_flight = self.in_flight.lock().unwrap_or_else(|poisoned| {
            // A panicking tool body must not disable the guard for the rest of
            // the process. The set carries no invariant a panic could break —
            // it is a set of claims — so the claim is taken anyway.
            self.in_flight.clear_poison();
            poisoned.into_inner()
        });
        in_flight.insert((conversation_id, tool_call_id.to_string()))
    }

    /// Release a claim once the call is resolved in the ledger.
    fn release(&self, conversation_id: i64, tool_call_id: &str) {
        let mut in_flight = self.in_flight.lock().unwrap_or_else(|poisoned| {
            self.in_flight.clear_poison();
            poisoned.into_inner()
        });
        in_flight.remove(&(conversation_id, tool_call_id.to_string()));
    }
}

impl<E: RuntimeEvent> ToolRunner<E> {
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
    /// `replaces_streaming` is the streamed-input tail this final call replaces,
    /// deleted in the same transaction.
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
        replaces_streaming: Option<i64>,
    ) -> Result<i64, StoreError> {
        let interactive = self
            .registry
            .get(&name)
            .is_some_and(<dyn ToolHandler<E>>::interactive);
        let block_id = ctx
            .store
            .insert_tool_call_block(
                ctx.conversation_id,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id,
                    name,
                    input,
                    interactive,
                },
                replaces_streaming,
            )
            .await?;
        if latched {
            return Ok(block_id);
        }
        if let Some(block) = ctx.store.find_block(block_id).await? {
            BlockKind::from_block(&block).run(ctx).await?;
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
    pub async fn run_wakeup(&self, ctx: &AgencyCtx<E>, latched: bool, tool_call_id: &str) {
        if latched {
            return;
        }
        self.execute_ready_call(ctx, tool_call_id).await;
    }

    /// One pass of admission, from a fresh read of the ledger.
    ///
    /// The order is the design, not an implementation detail:
    ///
    /// 1. **Find the call.** Absent — a stale wakeup for another ledger — is a
    ///    silent return.
    /// 2. **Already resolved?** Return. This is what makes a duplicate wakeup
    ///    free, and it comes FIRST so no later step can re-run a completed
    ///    body.
    /// 3. **Resolve the tool.** Unknown resolves the call with an error rather
    ///    than leaving it dangling: a call nobody can run is still a call the
    ///    model is waiting on.
    /// 4. **Ungated?** Straight to the body. No evaluation, no record of one.
    /// 5. **Read the recorded standing** and, only where nothing is recorded
    ///    yet, ask the tool's own gate — which records its answer before
    ///    anything acts on it.
    async fn execute_ready_call(&self, ctx: &AgencyCtx<E>, tool_call_id: &str) {
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

        let Some(call) = ledger
            .iter()
            .find_map(|block| match BlockKind::from_block(block) {
                BlockKind::ToolCall(call) if call.tool_call_id == tool_call_id => Some(call),
                _ => None,
            })
        else {
            return;
        };
        if call.resolved_in(&ledger) {
            return;
        }

        let Some(handler) = self.registry.get(&call.name) else {
            warn!(name = call.name, "tool runner: no handler registered");
            let unknown = format!("unknown tool: {}", call.name);
            self.resolve_with_error(ctx, &call.tool_call_id, unknown, call.id)
                .await;
            return;
        };

        if handler.gated() && !self.admit(ctx, handler, &ledger, &call).await {
            return;
        }
        self.run_body(ctx, handler, &call).await;
    }

    /// The admission half: read what is recorded about this call, and consult
    /// the tool's own gate only where nothing is recorded yet.
    ///
    /// Answers whether the body may run. Every refusal it makes is RECORDED
    /// before it returns — that is the whole point of the split: the caller
    /// learns a boolean, but the ledger learns the decision, and the ledger is
    /// what the next wakeup reads.
    async fn admit(
        &self,
        ctx: &AgencyCtx<E>,
        handler: &dyn ToolHandler<E>,
        ledger: &[Block],
        call: &ToolCall,
    ) -> bool {
        match approval_state(ledger, call.id) {
            ApprovalState::Approved => true,
            // Undecided: the human still owes this. Denied: the verdict already
            // resolved the call with its error, atomically, so there is nothing
            // left to record and never a body to run.
            ApprovalState::Undecided | ApprovalState::Denied => false,
            ApprovalState::Unrequested => match handler.gate(&call.input).await {
                GateDecision::Proceed => true,
                GateDecision::Refuse { reason } => {
                    self.resolve_with_error(ctx, &call.tool_call_id, reason, call.id)
                        .await;
                    false
                }
                GateDecision::Defer => {
                    // Insert-first holds by construction: the call is already in
                    // the ledger, so the request records its real id. A
                    // re-received wakeup short-circuits above as Undecided, so
                    // this appends exactly one request per call.
                    if let Err(error) = ctx
                        .store
                        .insert_approval_request_block(ctx.conversation_id, call.id)
                        .await
                    {
                        warn!(
                            conversation_id = ctx.conversation_id,
                            %error,
                            "tool runner: parking the call for clearance failed"
                        );
                    }
                    false
                }
            },
        }
    }

    /// The body half, identical for every admitted call: claim it against a
    /// duplicate wakeup, run it, and record whatever it produced.
    async fn run_body(&self, ctx: &AgencyCtx<E>, handler: &dyn ToolHandler<E>, call: &ToolCall) {
        if !self.claim(ctx.conversation_id, &call.tool_call_id) {
            tracing::debug!(
                conversation_id = ctx.conversation_id,
                tool_call_id = call.tool_call_id,
                "tool runner: already in flight, skipping"
            );
            return;
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
        match outcome {
            ToolOutcome::Done(result) => {
                if let Err(error) = ctx
                    .store
                    .complete_tool_call_block(
                        ctx.conversation_id,
                        call.tool_call_id.clone(),
                        result,
                        call.id,
                    )
                    .await
                {
                    warn!(
                        conversation_id = ctx.conversation_id,
                        %error,
                        "tool runner: recording the result failed"
                    );
                }
                self.release(ctx.conversation_id, &call.tool_call_id);
            }
            ToolOutcome::Error(error) => {
                self.resolve_with_error(ctx, &call.tool_call_id, error, call.id)
                    .await;
                self.release(ctx.conversation_id, &call.tool_call_id);
            }
            // The backing system resolves it; the claim stays until it does.
            ToolOutcome::Pending => {}
        }
    }

    /// Resolve a call with a tool error. Every refusal the runner records goes
    /// through here, so a refusal is always a block the model re-plans against
    /// and never an exception that leaves the call dangling.
    async fn resolve_with_error(
        &self,
        ctx: &AgencyCtx<E>,
        tool_call_id: &str,
        error: String,
        call_block_id: i64,
    ) {
        if let Err(failure) = ctx
            .store
            .fail_tool_call_block(
                ctx.conversation_id,
                tool_call_id.to_string(),
                error,
                call_block_id,
            )
            .await
        {
            warn!(
                conversation_id = ctx.conversation_id,
                error = %failure,
                "tool runner: recording the error failed"
            );
        }
    }
}
