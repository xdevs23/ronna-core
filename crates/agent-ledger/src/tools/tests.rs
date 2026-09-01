//! Admission through the REAL tick machinery: the cursor drive and the
//! redispatch walk, many ticks, feeding the real runner chokepoint over a
//! test-only gated tool.
//!
//! Every one of these composes four layers — the store records the call, the
//! ratchet parks on it and re-emits, the runner admits or refuses it, and the
//! frontier reads the result — because that composition is where the failure
//! being guarded against lived. A unit test of the runner alone would have
//! passed throughout the incident that produced this design.
//!
//! Nothing here spawns a loop, sleeps or waits on a clock: a wakeup is handed
//! to the chokepoint by the test itself, so a red result names an assertion
//! rather than a timeout, and the suite stays parallel and fast.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde_json::Value;

use crate::agency::ratchet::oracle::Oracle;
use crate::agency::{
    Agency, AgencyCtx, Awaiting, BlockKind, FromBlock, GateDecision, ratchet, redispatch,
};
use crate::block::Block;
use crate::event::CoreEvent;
use crate::providers::types::ToolDefinition;
use crate::providers::{BoxFuture, blocks_to_messages};
use crate::store::Store;
use crate::tools::CallOrigin;
use crate::types::ApprovalChoice;

use super::{
    ToolContext, ToolHandler, ToolOutcome, ToolRegistry, ToolRunner,
    admission::submit_approval,
    runner::{ToolCallWindow, ToolWindowBound},
};

/// A gated tool that counts its executions and answers whatever gate decision
/// the test built it with.
struct GatedProbe {
    executions: Arc<AtomicUsize>,
    decision: GateDecision,
}

impl ToolHandler<CoreEvent> for GatedProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "gated_probe".into(),
            description: "a test-only gated tool".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn gated(&self) -> bool {
        true
    }

    fn gate<'a>(&'a self, _input: &'a str) -> BoxFuture<'a, GateDecision> {
        Box::pin(async move { self.decision.clone() })
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            ToolOutcome::Done("cleared".into())
        })
    }
}

/// One scheduler tick, in the shape the session actor will own: rest entirely
/// while latched — the whole ratchet family runs only unlatched — otherwise
/// drive the cursor and then run the redispatch walk, which rides the same tick
/// but is ungated by the model-turn axis, so deferred work resumes even when no
/// turn is owed.
async fn tick(ctx: &AgencyCtx<CoreEvent>, latched: bool) -> Option<ratchet::Outcome> {
    if latched {
        return None;
    }
    let outcome = ratchet::drive::<BlockKind, _>(ctx)
        .await
        .expect("the drive reads its ledger")
        .outcome()
        .expect("the conversation exists");
    redispatch::walk::<BlockKind, _>(ctx)
        .await
        .expect("the walk unwinds");
    Some(outcome)
}

/// Every pending wakeup's call block id, in arrival order.
fn drain(rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>) -> Vec<i64> {
    let mut ids = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let CoreEvent::ToolCallReady { call_block_id, .. } = event {
            ids.push(call_block_id);
        }
    }
    ids
}

/// A stored ledger, a runner over a registry holding one gated probe, and the
/// probe's execution counter.
struct GateRig {
    o: Oracle,
    runner: ToolRunner<BlockKind, CoreEvent>,
    executions: Arc<AtomicUsize>,
}

impl GateRig {
    async fn with_gate(decision: GateDecision) -> Self {
        let executions = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(
            "gated_probe",
            GatedProbe {
                executions: Arc::clone(&executions),
                decision,
            },
        );
        Self {
            o: Oracle::new().await,
            runner: ToolRunner::<BlockKind, _>::new(Arc::new(registry)),
            executions,
        }
    }

    async fn new() -> Self {
        Self::with_gate(GateDecision::Defer).await
    }

    fn conv(&self) -> i64 {
        self.o.ctx.conversation_id
    }

    fn store(&self) -> &Store {
        &self.o.ctx.store
    }

    fn executions(&self) -> usize {
        self.executions.load(Ordering::SeqCst)
    }

    async fn tick(&self) -> ratchet::Outcome {
        tick(&self.o.ctx, false)
            .await
            .expect("an unlatched tick drives")
    }

    /// Feed every pending wakeup to the REAL runner chokepoint. Returns how
    /// many wakeups were consumed.
    async fn pump(&mut self) -> usize {
        let mut consumed = 0;
        while let Ok(event) = self.o.rx.try_recv() {
            if let CoreEvent::ToolCallReady {
                conversation_id,
                call_block_id,
            } = event
            {
                assert_eq!(conversation_id, self.conv());
                self.runner
                    .run_wakeup(&self.o.ctx, false, call_block_id)
                    .await;
                consumed += 1;
            }
        }
        consumed
    }

    async fn blocks_of(&self, block_type: &str) -> Vec<Block> {
        self.store()
            .list_blocks(self.conv())
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.block_type == block_type)
            .collect()
    }

    async fn ledger_ids(&self) -> Vec<i64> {
        self.o.ledger_ids().await
    }

    /// Insert a gated call through the live-tail path, unlatched.
    async fn live_call(&self, tool_call_id: &str) -> i64 {
        self.runner
            .insert_call(
                &self.o.ctx,
                false,
                tool_call_id.into(),
                "gated_probe".into(),
                "{}".into(),
                CallOrigin::default(),
            )
            .await
            .unwrap()
    }

    /// User text plus a gated call via the live-tail path, one tick and one
    /// pump — the deferring gate appends exactly one approval request. Returns
    /// the call block id and the request block id.
    async fn deferred_call(&mut self, tool_call_id: &str) -> (i64, i64) {
        self.o.user_text("go").await;
        let call = self.live_call(tool_call_id).await;
        self.tick().await;
        self.pump().await;
        let requests = self.blocks_of("approval_request").await;
        assert_eq!(requests.len(), 1, "the gate deferred exactly one request");
        assert_eq!(requests[0].fields["for_block_id"], Value::from(call));
        (call, requests[0].id)
    }
}

// ─── Cursor × redispatch, exactly once ───────────────────────────────────────

/// The full gated flow: many real ticks emit MANY wakeups — the live-tail
/// drive, the cursor's re-drives and the walk's unwinds, at least two observed —
/// and the runner's ledger idempotency collapses them into EXACTLY one body
/// execution after the approval. Asserting the count of triggers is the point:
/// one execution off one wakeup would prove nothing.
#[tokio::test]
async fn gated_flow_executes_exactly_once_after_approval() {
    let mut rig = GateRig::new().await;
    rig.o.user_text("go").await;
    let call = rig.live_call("cg-approve").await;

    let mut wakeups = 0;
    for _ in 0..3 {
        let outcome = rig.tick().await;
        assert!(outcome.parked, "the gated call parks the cursor");
        assert!(
            !outcome.owes_turn,
            "no turn fires while clearance is pending"
        );
        wakeups += rig.pump().await;
    }
    assert_eq!(rig.executions(), 0, "no execution before the decision");
    let requests = rig.blocks_of("approval_request").await;
    assert_eq!(requests.len(), 1, "re-received wakeups append nothing");
    assert_eq!(requests[0].fields["for_block_id"], Value::from(call));

    submit_approval(
        rig.store(),
        rig.conv(),
        requests[0].id,
        ApprovalChoice::Approved,
        None,
        None,
    )
    .await
    .unwrap();

    for _ in 0..3 {
        rig.tick().await;
        wakeups += rig.pump().await;
    }
    assert!(
        wakeups >= 2,
        "multiple triggers actually fired (got {wakeups})"
    );
    assert_eq!(rig.executions(), 1, "the body ran EXACTLY once");
    assert_eq!(rig.blocks_of("tool_result").await.len(), 1);

    let outcome = rig.tick().await;
    assert!(
        outcome.owes_turn,
        "the drained frontier owes one turn against the result"
    );
    assert!(!outcome.parked);
}

/// The deny path: many ticks, ZERO executions, exactly one tool error carrying
/// who denied and why — and that error tail hands the model its re-plan turn.
#[tokio::test]
async fn denied_call_never_executes_and_carries_the_reason() {
    let mut rig = GateRig::new().await;
    let (_call, request) = rig.deferred_call("cg-deny").await;

    submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Denied,
        None,
        Some("too spicy".into()),
    )
    .await
    .unwrap();

    for _ in 0..4 {
        rig.tick().await;
        rig.pump().await;
    }
    assert_eq!(rig.executions(), 0, "a denied body NEVER runs");
    let errors = rig.blocks_of("tool_error").await;
    assert_eq!(errors.len(), 1, "exactly one tool error resolves the call");
    let text = errors[0].fields["error"].as_str().unwrap();
    assert!(
        text.contains("The user denied this action"),
        "who is named: {text}"
    );
    assert!(text.contains("too spicy"), "the reason is carried: {text}");

    let outcome = rig.tick().await;
    assert!(outcome.owes_turn, "the model re-plans off the error tail");
}

/// Crash AFTER the body ran: the cursor resets to 0, in-flight wakeups are
/// lost, and the walk is stateless by construction — so the replay re-derives
/// everything, never re-executes, and appends nothing.
#[tokio::test]
async fn crash_replay_never_re_executes_a_completed_body() {
    let mut rig = GateRig::new().await;
    let (_call, request) = rig.deferred_call("cg-crash").await;
    submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Approved,
        None,
        None,
    )
    .await
    .unwrap();
    rig.tick().await;
    rig.pump().await;
    assert_eq!(rig.executions(), 1);
    let snapshot = rig.ledger_ids().await;

    rig.store().update_cursor(rig.conv(), 0).await.unwrap();
    while rig.o.rx.try_recv().is_ok() {}

    for _ in 0..3 {
        rig.tick().await;
        rig.pump().await;
    }
    assert_eq!(rig.executions(), 1, "replay never re-runs the body");
    assert_eq!(rig.ledger_ids().await, snapshot, "replay appends nothing");
}

/// Crash BETWEEN the approval and the execution: the pending wakeups die with
/// the process and the cursor resets — the re-drive re-emits and the approved
/// body recovers to exactly one run.
#[tokio::test]
async fn crash_between_approval_and_execution_recovers_to_one_run() {
    let mut rig = GateRig::new().await;
    let (_call, request) = rig.deferred_call("cg-resume").await;
    submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Approved,
        None,
        None,
    )
    .await
    .unwrap();

    rig.store().update_cursor(rig.conv(), 0).await.unwrap();
    while rig.o.rx.try_recv().is_ok() {}

    for _ in 0..3 {
        rig.tick().await;
        rig.pump().await;
    }
    assert_eq!(
        rig.executions(),
        1,
        "the re-emitted wakeup recovers exactly one run"
    );
    assert_eq!(rig.blocks_of("tool_result").await.len(), 1);
}

/// The id-keyed regression trap: an UNRELATED result landing after the decision
/// must not close the approve route. A by-kind "a result followed" match would
/// settle it and strand the approved body forever.
#[tokio::test]
async fn unrelated_result_after_the_decision_does_not_close_the_approve_route() {
    let mut rig = GateRig::new().await;
    let (_call, request) = rig.deferred_call("cg-trap").await;
    submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Approved,
        None,
        None,
    )
    .await
    .unwrap();
    rig.o.call("some-other-call").await;
    rig.o.result("some-other-call").await;

    for _ in 0..3 {
        rig.tick().await;
        rig.pump().await;
    }
    assert_eq!(rig.executions(), 1, "the approved body still ran");
    let results = rig.blocks_of("tool_result").await;
    assert!(
        results
            .iter()
            .any(|b| b.fields["tool_call_id"] == "cg-trap"),
        "the call's OWN result landed"
    );
}

/// Double submit: the second decision write fails cleanly — exactly one
/// decision block, and the losing denial's error never lands.
#[tokio::test]
async fn double_submit_loses_cleanly_with_exactly_one_decision() {
    let mut rig = GateRig::new().await;
    let (_call, request) = rig.deferred_call("cg-race").await;

    submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Approved,
        None,
        None,
    )
    .await
    .unwrap();
    let lost = submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Denied,
        None,
        Some("wait, no".into()),
    )
    .await;
    assert!(lost.is_err(), "the loser gets a clean error");

    let decisions = rig.blocks_of("approval_decision").await;
    assert_eq!(decisions.len(), 1, "exactly one decision block");
    assert_eq!(decisions[0].fields["decision"], Value::from("approved"));
    assert!(
        rig.blocks_of("tool_error").await.is_empty(),
        "the losing denial's error never lands"
    );
}

/// A conditionally gated tool whose gate proceeds runs immediately, with no
/// request in the ledger: no request means not deferred, which means not held.
#[tokio::test]
async fn conditionally_gated_proceed_runs_without_a_request() {
    let mut rig = GateRig::with_gate(GateDecision::Proceed).await;
    rig.o.user_text("go").await;
    rig.live_call("cg-waived").await;
    rig.tick().await;
    rig.pump().await;
    assert_eq!(rig.executions(), 1);
    assert!(
        rig.blocks_of("approval_request").await.is_empty(),
        "a waiving gate defers nothing"
    );
}

/// A refusing gate resolves the call with its reason — the body never runs and
/// nothing waits on a human. This is the end-to-end proof of the refused path:
/// the refusal is recorded, the cursor advances off it, and no approval request
/// is ever parked for an effect that was always going to be turned down.
///
/// One clause of AC5-7 has no referent here and is deliberately not asserted:
/// "the error is recorded atomically with the response". This runtime records no
/// separate gate-answer block, so there is no companion write to be atomic with
/// — the tool error IS the recorded answer, appended in a single insert. The
/// paired-write shape the criterion describes belongs to the generalized
/// permission reference, whose own header says it describes patterns rather than
/// this tree; here the only paired write is a human denial, which lands its
/// decision and its tool error in one transaction (see `submit_approval`) and is
/// pinned by `denied_call_never_executes_and_carries_the_reason`. Resolving the
/// wording is the maintainer's call, not this layer's.
#[tokio::test]
async fn gate_refusal_records_the_reason_and_skips_the_body() {
    let mut rig = GateRig::with_gate(GateDecision::Refuse {
        reason: "not permitted in this session".into(),
    })
    .await;
    rig.o.user_text("go").await;
    let call = rig.live_call("cg-refused").await;
    rig.tick().await;
    rig.pump().await;

    assert_eq!(rig.executions(), 0);
    let errors = rig.blocks_of("tool_error").await;
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].fields["error"],
        Value::from("not permitted in this session")
    );
    assert_eq!(errors[0].fields["tool_call_id"], Value::from("cg-refused"));
    assert!(rig.blocks_of("approval_request").await.is_empty());

    let outcome = rig.tick().await;
    assert!(
        outcome.cursor >= call,
        "the resolved call no longer parks the cursor"
    );
    assert!(!outcome.parked, "the refusal drained the frontier");
}

/// The request and decision blocks never reach the model: the neutral messages
/// are identical with and without them — no content, no message boundary, no
/// empty message leaking that permission was asked or decided.
#[tokio::test]
async fn approval_blocks_never_reach_the_model() {
    let mut rig = GateRig::new().await;
    let (_call, request) = rig.deferred_call("cg-render").await;

    let ledger = rig.store().list_blocks(rig.conv()).await.unwrap();
    assert_model_view_ignores_approvals(&ledger);

    submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Approved,
        None,
        None,
    )
    .await
    .unwrap();
    rig.tick().await;
    rig.pump().await;

    let ledger = rig.store().list_blocks(rig.conv()).await.unwrap();
    assert!(ledger.iter().any(|b| b.block_type == "approval_decision"));
    assert!(ledger.iter().any(|b| b.block_type == "tool_result"));
    assert_model_view_ignores_approvals(&ledger);
}

fn assert_model_view_ignores_approvals(ledger: &[Block]) {
    let stripped: Vec<Block> = ledger
        .iter()
        .filter(|b| {
            !matches!(
                b.block_type.as_str(),
                "approval_request" | "approval_decision"
            )
        })
        .cloned()
        .collect();
    let with = serde_json::to_value(blocks_to_messages::<BlockKind>(ledger)).unwrap();
    let without = serde_json::to_value(blocks_to_messages::<BlockKind>(&stripped)).unwrap();
    assert_eq!(
        with, without,
        "the model's view is identical with or without approval blocks"
    );
}

// ─── Several parked blocks ───────────────────────────────────────────────────

/// The cursor parks on the FIRST unresolved call: the second is not driven BY
/// THE CURSOR while the first dangles, and is driven once it resolves.
#[tokio::test]
async fn cursor_drives_the_second_call_only_after_the_first_resolves() {
    let mut o = Oracle::new().await;
    o.user_text("two").await;
    let first = o.call("d1-first").await;
    let second = o.call("d1-second").await;

    tick(&o.ctx, false).await;
    assert_eq!(drain(&mut o.rx), vec![first]);
    tick(&o.ctx, false).await;
    assert_eq!(
        drain(&mut o.rx),
        vec![first],
        "the second call is never driven by the cursor while the first dangles"
    );

    o.result("d1-first").await;
    tick(&o.ctx, false).await;
    assert_eq!(
        drain(&mut o.rx),
        vec![second],
        "the resolved first lets the cursor advance and drive the second"
    );
}

/// The never-done-first starvation contract, asserted explicitly at ratchet
/// level: a block with no completion path parks the cursor forever and starves
/// everything behind it. Documented, tested, not a surprise.
#[tokio::test]
async fn never_done_first_call_starves_everything_behind_it() {
    let mut o = Oracle::new().await;
    let user = o.user_text("go").await;
    let stuck = o.call("d2-stuck").await;
    o.user_text("hello? anyone?").await;

    for _ in 0..5 {
        let outcome = tick(&o.ctx, false).await.expect("an unlatched tick drives");
        assert!(outcome.parked);
        assert!(
            !outcome.owes_turn,
            "the model-owed tail behind the park never fires"
        );
        assert_eq!(
            outcome.cursor, user,
            "the cursor never passes the stuck call"
        );
        assert_eq!(drain(&mut o.rx), vec![stuck]);
    }
}

/// PARALLELISM: two sibling calls inserted through the live-tail path both emit
/// wakeups BEFORE either resolves and before any tick. This is the test that
/// fails if the drive at insert is ever removed — the cursor alone parks on the
/// first sibling and would never emit the second's wakeup.
#[tokio::test]
async fn live_tail_siblings_emit_wakeups_before_either_resolves() {
    let runner = ToolRunner::<BlockKind, _>::new(Arc::new(ToolRegistry::<CoreEvent>::new()));
    let mut o = Oracle::new().await;
    o.user_text("fan out").await;
    let mut siblings = Vec::new();
    for id in ["d3-sib1", "d3-sib2"] {
        siblings.push(
            runner
                .insert_call(
                    &o.ctx,
                    false,
                    id.into(),
                    "read_file".into(),
                    "{}".into(),
                    CallOrigin::default(),
                )
                .await
                .unwrap(),
        );
    }

    o.expect_wakeup(siblings[0]);
    o.expect_wakeup(siblings[1]);
    o.expect_silence();
}

/// The coverage gap an audit traced by hand: TWO gated calls, both approved in
/// one burst. The cursor — parked on the earlier call — and the redispatch walk
/// — anchored on the later decision — each carry one wakeup, the runner's
/// ledger idempotency keeps each body at exactly one run, and ONE turn fires
/// only after both results land.
#[tokio::test]
async fn two_gated_calls_approved_in_one_burst_execute_once_each_then_one_turn() {
    let mut rig = GateRig::new().await;
    rig.o.user_text("fan out").await;
    let c1 = rig.live_call("burst-1").await;
    let c2 = rig.live_call("burst-2").await;
    rig.tick().await;
    rig.pump().await; // both live-tail wakeups defer — two requests, no bodies
    assert_eq!(rig.executions(), 0);
    let requests = rig.blocks_of("approval_request").await;
    assert_eq!(requests.len(), 2, "one request per call");
    let request_for = |call: i64| {
        requests
            .iter()
            .find(|r| r.fields["for_block_id"] == call)
            .expect("a request covers the call")
            .id
    };

    // One burst: both decisions land back to back, before any tick runs.
    for request in [request_for(c1), request_for(c2)] {
        submit_approval(
            rig.store(),
            rig.conv(),
            request,
            ApprovalChoice::Approved,
            None,
            None,
        )
        .await
        .unwrap();
    }

    for _ in 0..4 {
        let outcome = rig.tick().await;
        if rig.blocks_of("tool_result").await.len() < 2 {
            assert!(!outcome.owes_turn, "no turn fires before both results land");
        }
        rig.pump().await;
    }
    assert_eq!(rig.executions(), 2, "each body ran exactly once");
    let results = rig.blocks_of("tool_result").await;
    assert_eq!(results.len(), 2);
    for id in ["burst-1", "burst-2"] {
        assert_eq!(
            results
                .iter()
                .filter(|b| b.fields["tool_call_id"] == id)
                .count(),
            1,
            "{id} resolved exactly once"
        );
    }

    let outcome = rig.tick().await;
    assert!(outcome.owes_turn, "exactly one turn against both results");
    assert!(!outcome.parked);
}

/// The latch's accepted consequence, end to end: an interrupt races a mid-stream
/// insert and leaves a dangling call. It stays dangling WHILE latched — ticks
/// rest entirely: no drive, no walk, no wakeup, no execution — and after the
/// unlatch, which a user message brings, the ratchet re-drives it, the body
/// executes, the result lands, and exactly one turn fires against the settled
/// ledger.
#[tokio::test]
async fn dangling_call_stays_dangling_while_latched_and_heals_on_unlatch() {
    let mut rig = GateRig::with_gate(GateDecision::Proceed).await;
    rig.o.user_text("go").await;
    rig.runner
        .insert_call(
            &rig.o.ctx,
            true, // the interrupt won the race — recorded, not driven
            "heal".into(),
            "gated_probe".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();
    rig.o.status("interrupted").await;
    rig.o.expect_silence();

    for _ in 0..3 {
        assert!(
            tick(&rig.o.ctx, true).await.is_none(),
            "latched ticks rest entirely"
        );
    }
    rig.o.expect_silence();
    assert_eq!(
        rig.executions(),
        0,
        "the dangle stays dangling while latched"
    );
    assert_eq!(
        rig.o.cursor().await,
        0,
        "the cursor never moves while latched"
    );

    // The unlatch: the user's message lands and ticks resume.
    rig.o.user_text("continue").await;
    let outcome = rig.tick().await;
    assert!(
        outcome.parked,
        "the ratchet parks on the dangle and re-emits"
    );
    assert!(!outcome.owes_turn, "no turn while the call dangles");
    rig.pump().await;
    assert_eq!(
        rig.executions(),
        1,
        "the body executes exactly once after the unlatch"
    );

    let outcome = rig.tick().await;
    assert!(
        outcome.owes_turn && !outcome.parked,
        "one turn fires against the settled ledger"
    );
}

/// The latch at the runner: a wakeup DELIVERED while latched is dropped, not
/// deferred. Nothing replays it — the ratchet's re-emit is the recovery, never
/// a runner-side queue — while a wakeup delivered unlatched executes normally,
/// which is what proves the runner was willing all along and the first one was
/// truly dropped.
#[tokio::test]
async fn a_wakeup_delivered_while_latched_is_dropped_not_deferred() {
    let rig = GateRig::with_gate(GateDecision::Proceed).await;
    rig.o.user_text("go").await;
    let mut calls = Vec::new();
    for id in ["dropped", "after-unlatch"] {
        calls.push(
            rig.runner
                .insert_call(
                    &rig.o.ctx,
                    true, // recorded, not driven: no wakeup of its own
                    id.into(),
                    "gated_probe".into(),
                    "{}".into(),
                    CallOrigin::default(),
                )
                .await
                .unwrap(),
        );
    }

    rig.runner.run_wakeup(&rig.o.ctx, true, calls[0]).await;
    assert_eq!(rig.executions(), 0, "the latched wakeup is dropped");

    rig.runner.run_wakeup(&rig.o.ctx, false, calls[1]).await;
    assert_eq!(rig.executions(), 1, "only the unlatched wakeup ran");

    let results = rig.blocks_of("tool_result").await;
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields["tool_call_id"],
        Value::from("after-unlatch"),
        "the dropped call is still unresolved — the ratchet's re-emit is its recovery"
    );
}

/// A model reusing one `tool_call_id` across turns: two calls under the shared
/// id, resolved in sequence through the real tick machinery. Everything is
/// keyed on the call BLOCK id — the wakeup, the runner's lookup, the
/// conditional writes — so the first call's result never answers for the
/// second, each body runs exactly once, and the ratchet drains past both to
/// exactly one owed turn.
#[tokio::test]
async fn calls_sharing_a_tool_call_id_each_execute_and_the_ratchet_drains() {
    let mut rig = GateRig::with_gate(GateDecision::Proceed).await;
    rig.o.user_text("go").await;
    let first = rig.live_call("dup").await;
    rig.tick().await;
    rig.pump().await;
    assert_eq!(rig.executions(), 1, "the first call's body ran");

    let second = rig.live_call("dup").await;
    for _ in 0..3 {
        rig.tick().await;
        rig.pump().await;
    }
    assert_eq!(
        rig.executions(),
        2,
        "the second call executed despite sharing the provider id"
    );

    let results = rig.blocks_of("tool_result").await;
    assert_eq!(results.len(), 2, "one result per call");
    for call in [first, second] {
        assert!(
            rig.store()
                .complete_tool_call_block(rig.conv(), "dup".into(), "again".into(), call)
                .await
                .unwrap()
                .is_none(),
            "each call carries its own outcome already"
        );
    }

    let outcome = rig.tick().await;
    assert!(outcome.owes_turn, "the drained frontier owes one turn");
    assert!(!outcome.parked, "nothing dangles behind the shared id");
}

// ─── The interactive stamp ───────────────────────────────────────────────────

/// A tool whose call the human answers.
struct InteractiveProbe;

impl ToolHandler<CoreEvent> for InteractiveProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "ask_human".into(),
            description: "a test-only interactive tool".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn interactive(&self) -> bool {
        true
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async {
            ToolOutcome::Error(
                "an interactive call is the human's — the system never executes it".into(),
            )
        })
    }
}

/// `interactive` is stamped at insert, read off the tool definition by the one
/// seam with registry access. The block then answers the human's ask from its
/// own data on replay: no wakeup at insert, no cursor park, no model turn, and
/// the frontier publishes who is owed.
#[tokio::test]
async fn interactive_stamp_lands_at_insert_from_the_registry() {
    let mut registry = ToolRegistry::new();
    registry.register("ask_human", InteractiveProbe);
    let runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
    let mut o = Oracle::new().await;
    o.user_text("go").await;
    let call = runner
        .insert_call(
            &o.ctx,
            false,
            "iv-1".into(),
            "ask_human".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();

    o.expect_silence(); // an interactive call emits no wakeup — the human owes the reply
    let block = o.ctx.store.find_block(call).await.unwrap().unwrap();
    assert_eq!(
        block.fields["interactive"],
        Value::from(true),
        "stamped through the real write path"
    );

    let outcome = o.drive().await;
    assert!(
        !outcome.parked,
        "an interactive call never parks the cursor"
    );
    assert!(!outcome.owes_turn, "and never green-lights a model turn");
    assert_eq!(
        outcome.awaiting,
        Some(Awaiting::User),
        "the frontier publishes the human's ask"
    );
}

/// An interactive tool that also carries a gate implementation — legal, since
/// it does not declare `gated()` — and counts whether that gate is ever asked.
struct InteractiveGateProbe {
    gate_calls: Arc<AtomicUsize>,
}

impl ToolHandler<CoreEvent> for InteractiveGateProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "ask_human_gated".into(),
            description: "a test-only interactive tool with a gate body".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn interactive(&self) -> bool {
        true
    }

    fn gate<'a>(&'a self, _input: &'a str) -> BoxFuture<'a, GateDecision> {
        Box::pin(async move {
            self.gate_calls.fetch_add(1, Ordering::SeqCst);
            GateDecision::Proceed
        })
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async {
            ToolOutcome::Error(
                "an interactive call is the human's — the system never executes it".into(),
            )
        })
    }
}

/// The supersession [`ToolHandler::interactive`] states, pinned: the human IS
/// the admission for an interactive call. It never reaches the runner, so no
/// wakeup fires, no body runs, and no gate is ever consulted — and the frontier
/// hands the ask to the user.
#[tokio::test]
async fn an_interactive_call_is_admitted_by_the_human_never_a_gate() {
    let gate_calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(
        "ask_human_gated",
        InteractiveGateProbe {
            gate_calls: Arc::clone(&gate_calls),
        },
    );
    let runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
    let mut o = Oracle::new().await;
    o.user_text("go").await;
    runner
        .insert_call(
            &o.ctx,
            false,
            "iv-gate".into(),
            "ask_human_gated".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();

    o.expect_silence();
    for _ in 0..3 {
        tick(&o.ctx, false).await;
    }
    assert!(
        drain(&mut o.rx).is_empty(),
        "no wakeup ever fires for an interactive call"
    );
    assert_eq!(
        gate_calls.load(Ordering::SeqCst),
        0,
        "the human is the admission — the gate is never consulted"
    );
    let outcome = o.drive().await;
    assert_eq!(outcome.awaiting, Some(Awaiting::User));
}

// ─── The registry's table discipline ─────────────────────────────────────────

/// A named, inert tool for registry-shape tests.
struct NamedProbe(&'static str);

impl ToolHandler<CoreEvent> for NamedProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.0.into(),
            description: format!("the {} tool", self.0),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Done("ok".into()) })
    }
}

/// The model-facing list is deterministic: two registries built in different
/// insertion orders yield identical `definitions()`, and the order is the
/// names' sorted order — never hash order, which reordered the schema across
/// processes and invalidated prompt caching on every restart.
#[test]
fn definitions_are_identical_across_insertion_orders() {
    let mut first = ToolRegistry::<CoreEvent>::new();
    for name in ["zeta", "alpha", "mid"] {
        first.register(name, NamedProbe(name));
    }
    let mut second = ToolRegistry::<CoreEvent>::new();
    for name in ["mid", "zeta", "alpha"] {
        second.register(name, NamedProbe(name));
    }

    let names: Vec<&str> = first.names().collect();
    assert_eq!(names, ["alpha", "mid", "zeta"], "sorted by name");
    assert_eq!(
        serde_json::to_value(first.definitions()).unwrap(),
        serde_json::to_value(second.definitions()).unwrap(),
        "insertion order never reaches the model"
    );
    assert_eq!(
        first
            .handlers()
            .map(|h| h.definition().name)
            .collect::<Vec<_>>(),
        names,
        "every iteration shares the one order"
    );
}

/// A duplicate registration fails loudly, naming the colliding tool — the
/// registry is the only table of tool names, and a silent overwrite would
/// change what recorded calls mean.
#[test]
#[should_panic(expected = "tool 'dup' is already registered")]
fn a_second_registration_under_one_name_is_refused() {
    let mut registry = ToolRegistry::<CoreEvent>::new();
    registry.register("dup", NamedProbe("dup"));
    registry.register("dup", NamedProbe("dup"));
}

/// A handler declaring both `gated()` and `interactive()` ships a gate nothing
/// will ever consult. Debug builds refuse the registration outright.
struct ContradictoryProbe;

impl ToolHandler<CoreEvent> for ContradictoryProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "contradictory".into(),
            description: "declares gated and interactive at once".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn gated(&self) -> bool {
        true
    }

    fn interactive(&self) -> bool {
        true
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Done("unreachable".into()) })
    }
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "interactive supersedes")]
fn a_gated_interactive_handler_is_refused_at_registration() {
    ToolRegistry::<CoreEvent>::new().register("contradictory", ContradictoryProbe);
}

// ─── What a call resolves against (2026-09-01) ───────────────────────────────

/// A named tool that counts every time its body runs, so a test can assert
/// that a refused call never reached it.
struct CountingProbe(&'static str, Arc<AtomicUsize>);

impl ToolHandler<CoreEvent> for CountingProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.0.into(),
            description: format!("the {} tool", self.0),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            self.1.fetch_add(1, Ordering::SeqCst);
            ToolOutcome::Done("ok".into())
        })
    }
}

/// A runner over the named tools, and an oracle whose conversation already
/// carries a summoning message.
async fn resolution_rig(
    names: &[&'static str],
) -> (Oracle, ToolRunner<BlockKind, CoreEvent>, Arc<AtomicUsize>) {
    let executions = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    for name in names {
        registry.register(*name, CountingProbe(name, Arc::clone(&executions)));
    }
    let o = Oracle::new().await;
    o.user_text("go").await;
    (
        o,
        ToolRunner::<BlockKind, _>::new(Arc::new(registry)),
        executions,
    )
}

/// Record one call under `name` and feed the chokepoint one wakeup.
async fn call_once(o: &Oracle, runner: &ToolRunner<BlockKind, CoreEvent>, name: &str) {
    let call = runner
        .insert_call(
            &o.ctx,
            false,
            format!("call-{name}"),
            name.to_owned(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();
    runner.run_wakeup(&o.ctx, false, call).await;
}

/// Every tool error the conversation carries, as its sentence and whether the
/// row records a refusal.
async fn recorded_failures(o: &Oracle) -> Vec<(String, bool)> {
    o.ctx
        .store
        .list_blocks(o.ctx.conversation_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|block| block.block_type == "tool_error")
        .map(|block| {
            (
                block.fields["error"].as_str().unwrap().to_owned(),
                block.fields["refusal"].as_bool().unwrap(),
            )
        })
        .collect()
}

/// AC7, AC8, AC10 — a name outside the conversation's choice never reaches its
/// handler even though the registry holds it, the sentence names the
/// conversation's own two tools and not the third, and the outcome is an
/// ordinary failure because the next round can succeed.
#[tokio::test]
async fn a_tool_outside_the_choice_is_refused_and_the_sentence_names_the_conversations_own() {
    let (o, runner, executions) = resolution_rig(&["alpha", "mid", "zeta"]).await;
    o.ctx
        .store
        .append_tool_choice(o.ctx.conversation_id, vec!["alpha".into(), "zeta".into()])
        .await
        .unwrap();

    call_once(&o, &runner, "mid").await;

    assert_eq!(
        executions.load(Ordering::SeqCst),
        0,
        "a tool the conversation does not have never reaches its handler"
    );
    let failures = recorded_failures(&o).await;
    assert_eq!(failures.len(), 1, "the call is resolved, not dangling");
    assert_eq!(
        failures[0].0, "unknown tool: mid. This conversation's tools are: alpha, zeta",
        "the sentence names this conversation's tools, never the registry"
    );
    assert!(
        !failures[0].0.contains("mid, "),
        "the third registered tool is not disclosed"
    );
    assert!(
        !failures[0].1,
        "with names to offer, the model can correct itself — an ordinary failure"
    );
}

/// AC8 — the two ways a name fails to resolve read identically, byte for
/// byte: one was never registered anywhere, the other is registered and
/// outside this conversation's choice. Any difference would disclose the
/// existence of the tool the conversation was deliberately not given.
#[tokio::test]
async fn a_name_outside_the_choice_and_a_name_nobody_registered_read_the_same() {
    let (o, runner, _) = resolution_rig(&["alpha", "mid", "zeta"]).await;
    o.ctx
        .store
        .append_tool_choice(o.ctx.conversation_id, vec!["alpha".into(), "zeta".into()])
        .await
        .unwrap();

    call_once(&o, &runner, "mid").await;
    let outside_the_choice = recorded_failures(&o).await[0].0.clone();

    // The same conversation's choice, over a registry that never held `mid`
    // at all.
    let (fresh, runner, _) = resolution_rig(&["alpha", "zeta"]).await;
    fresh
        .ctx
        .store
        .append_tool_choice(
            fresh.ctx.conversation_id,
            vec!["alpha".into(), "zeta".into()],
        )
        .await
        .unwrap();
    call_once(&fresh, &runner, "mid").await;
    let never_registered = recorded_failures(&fresh).await[0].0.clone();

    assert_eq!(
        outside_the_choice, never_registered,
        "registered-but-outside and never-registered read identically"
    );
}

/// AC9 — a conversation whose resolved set is empty, against a registry that
/// HAS tools: the sentence states the conversation has no tools, names none of
/// them, says nothing about the registry, and the row records a refusal
/// because no next round can resolve either.
#[tokio::test]
async fn an_empty_choice_answers_with_no_tools_and_records_a_refusal() {
    let (o, runner, executions) = resolution_rig(&["alpha", "mid", "zeta"]).await;
    o.ctx
        .store
        .append_tool_choice(o.ctx.conversation_id, Vec::new())
        .await
        .unwrap();

    call_once(&o, &runner, "alpha").await;

    assert_eq!(executions.load(Ordering::SeqCst), 0, "nothing ran");
    let failures = recorded_failures(&o).await;
    assert_eq!(failures.len(), 1);
    assert_eq!(
        failures[0].0,
        "this conversation has no tools, so no tool call can be answered."
    );
    for name in ["alpha", "mid", "zeta", "registered"] {
        assert!(
            !failures[0].0.contains(name),
            "the sentence names no tool and asserts nothing about the registry"
        );
    }
    assert!(
        failures[0].1,
        "nothing it calls can resolve, so this is a standing no"
    );
}

/// AC6 — a recorded name the registry does not hold resolves to nothing: it is
/// offered to nobody and resolvable by nobody. The dispatch's half of this
/// criterion is asserted at the dispatch itself; here is the runner's.
#[tokio::test]
async fn a_recorded_name_the_registry_does_not_hold_resolves_to_nothing() {
    let (o, runner, executions) = resolution_rig(&["alpha"]).await;
    o.ctx
        .store
        .append_tool_choice(o.ctx.conversation_id, vec!["alpha".into(), "gone".into()])
        .await
        .unwrap();

    call_once(&o, &runner, "gone").await;

    assert_eq!(executions.load(Ordering::SeqCst), 0);
    let failures = recorded_failures(&o).await;
    assert_eq!(
        failures[0].0, "unknown tool: gone. This conversation's tools are: alpha",
        "the intersection is the whole rule: the recorded name the registry lost is \
         not among this conversation's tools"
    );
}

/// AC5 — a conversation that recorded no choice at all resolves against every
/// registered tool, exactly as every conversation did before the record
/// existed.
#[tokio::test]
async fn a_conversation_with_no_recorded_choice_resolves_against_the_registry() {
    let (o, runner, executions) = resolution_rig(&["alpha", "zeta"]).await;

    call_once(&o, &runner, "zeta").await;

    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "no record filters nothing"
    );
    assert!(recorded_failures(&o).await.is_empty());
}

// ─── The consumer's own admission hook (2026-09-01) ──────────────────────────

/// A tool whose admission answers from the ledger it is handed, recording what
/// it saw so a test can prove the snapshot travelled in instead of being
/// fetched.
struct AdmittingProbe {
    executions: Arc<AtomicUsize>,
    refuse: bool,
    seen: Arc<std::sync::Mutex<Vec<Vec<i64>>>>,
}

impl ToolHandler<CoreEvent> for AdmittingProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "admitted".into(),
            description: "a test-only tool with its own admission".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn admit<'a>(
        &'a self,
        ctx: &'a ToolContext<'a, CoreEvent>,
        ledger: &'a [Block],
    ) -> BoxFuture<'a, crate::tools::Admission> {
        let ids: Vec<i64> = ledger.iter().map(|block| block.id).collect();
        let carries_call = ids.contains(&ctx.block_id);
        Box::pin(async move {
            self.seen.lock().unwrap().push(ids);
            assert!(
                carries_call,
                "the snapshot carries the call under admission"
            );
            if self.refuse {
                crate::tools::Admission::Refuse {
                    reason: "the consumer said no".into(),
                }
            } else {
                crate::tools::Admission::Admit
            }
        })
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            ToolOutcome::Done("ok".into())
        })
    }
}

/// AC15 — the hook's refusal resolves the call with its own sentence, records
/// it `Refusal::Refused`, and the body never runs.
#[tokio::test]
async fn the_consumers_admission_refuses_with_its_sentence_and_the_body_never_runs() {
    let executions = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(
        "admitted",
        AdmittingProbe {
            executions: Arc::clone(&executions),
            refuse: true,
            seen: Arc::clone(&seen),
        },
    );
    let runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
    let o = Oracle::new().await;
    o.user_text("go").await;

    call_once(&o, &runner, "admitted").await;

    assert_eq!(executions.load(Ordering::SeqCst), 0, "the body never ran");
    let failures = recorded_failures(&o).await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, "the consumer said no");
    assert!(
        failures[0].1,
        "the consumer's decline is a refusal, so a run of them ends the turn"
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "the hook was consulted once");
}

/// AC16 — the admission pass loads the conversation's ledger EXACTLY ONCE per
/// call, and the hook is handed that snapshot instead of loading one of its
/// own. The counter is the store's own, so a hook that read the ledger through
/// its context would push the count past one — which is what makes the
/// reading mean anything.
#[tokio::test]
async fn the_admission_pass_loads_the_ledger_once_and_hands_it_to_the_hook() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(
        "admitted",
        AdmittingProbe {
            executions: Arc::new(AtomicUsize::new(0)),
            refuse: true,
            seen: Arc::clone(&seen),
        },
    );
    let runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
    let o = Oracle::new().await;
    o.user_text("go").await;

    let call = runner
        .insert_call(
            &o.ctx,
            true,
            "counted".into(),
            "admitted".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();
    let before = o.ctx.store.ledger_loads();
    runner.run_wakeup(&o.ctx, false, call).await;
    assert_eq!(
        o.ctx.store.ledger_loads() - before,
        1,
        "one pass, one ledger load — the hook was handed the snapshot"
    );

    let ledger = o
        .ctx
        .store
        .list_blocks(o.ctx.conversation_id)
        .await
        .unwrap();
    let ids: Vec<i64> = ledger.iter().map(|block| block.id).collect();
    let saw = seen.lock().unwrap();
    assert_eq!(saw.len(), 1);
    assert!(
        saw[0].iter().all(|id| ids.contains(id)) && saw[0].contains(&call),
        "the hook read the pass's own snapshot of this conversation"
    );
}

// ─── The ledger as the arbiter: the three overlap races ──────────────────────

/// A gated tool whose gate parks its FIRST invocation until the test releases
/// it. The gate sits between a wakeup's ledger read and its claim, so parking
/// there holds one wakeup's STALE snapshot live while another wakeup runs the
/// call to completion — the exact interleaving the overlap races need, made
/// deterministic.
struct ParkedGateProbe {
    executions: Arc<AtomicUsize>,
    decision: GateDecision,
    park_first: AtomicBool,
    gate_entered: Arc<tokio::sync::Notify>,
    gate_release: Arc<tokio::sync::Notify>,
}

impl ToolHandler<CoreEvent> for ParkedGateProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "parked_probe".into(),
            description: "a test-only gated tool with a parkable gate".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn gated(&self) -> bool {
        true
    }

    fn gate<'a>(&'a self, _input: &'a str) -> BoxFuture<'a, GateDecision> {
        Box::pin(async move {
            if self.park_first.swap(false, Ordering::SeqCst) {
                self.gate_entered.notify_one();
                self.gate_release.notified().await;
            }
            self.decision.clone()
        })
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            ToolOutcome::Done("cleared".into())
        })
    }
}

/// Everything two overlapping wakeups need: the shared runner, the oracle, the
/// probe's counters and the gate's two notifies.
struct OverlapRig {
    o: Oracle,
    runner: Arc<ToolRunner<BlockKind, CoreEvent>>,
    executions: Arc<AtomicUsize>,
    gate_entered: Arc<tokio::sync::Notify>,
    gate_release: Arc<tokio::sync::Notify>,
}

impl OverlapRig {
    async fn new(decision: GateDecision) -> Self {
        let executions = Arc::new(AtomicUsize::new(0));
        let gate_entered = Arc::new(tokio::sync::Notify::new());
        let gate_release = Arc::new(tokio::sync::Notify::new());
        let mut registry = ToolRegistry::new();
        registry.register(
            "parked_probe",
            ParkedGateProbe {
                executions: Arc::clone(&executions),
                decision,
                park_first: AtomicBool::new(true),
                gate_entered: Arc::clone(&gate_entered),
                gate_release: Arc::clone(&gate_release),
            },
        );
        let o = Oracle::new().await;
        o.user_text("go").await;
        Self {
            o,
            runner: Arc::new(ToolRunner::<BlockKind, _>::new(Arc::new(registry))),
            executions,
            gate_entered,
            gate_release,
        }
    }

    /// Spawn one wakeup and hold it parked in the gate: its ledger snapshot is
    /// taken, its claim is not, and the test decides when it resumes.
    async fn park_a_wakeup(&self, call_block_id: i64) -> tokio::task::JoinHandle<()> {
        let runner = Arc::clone(&self.runner);
        let ctx = AgencyCtx {
            conversation_id: self.o.ctx.conversation_id,
            store: self.o.ctx.store.clone(),
            bus: Arc::clone(&self.o.ctx.bus),
        };
        let parked =
            tokio::spawn(async move { runner.run_wakeup(&ctx, false, call_block_id).await });
        self.gate_entered.notified().await;
        parked
    }

    async fn blocks_of(&self, block_type: &str) -> Vec<Block> {
        self.o
            .ctx
            .store
            .list_blocks(self.o.ctx.conversation_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.block_type == block_type)
            .collect()
    }
}

/// The double-execution race, deterministic: wakeup B takes its ledger
/// snapshot and parks pre-claim; wakeup A runs the call to completion and
/// releases the claim; B then resumes on the STALE snapshot, passes the
/// resolved check it already made, and claims successfully. The re-read under
/// the claim is what stands it down — one execution, one result.
#[tokio::test]
async fn overlapping_wakeups_execute_the_body_once_and_record_one_result() {
    let rig = OverlapRig::new(GateDecision::Proceed).await;
    let call = rig
        .runner
        .insert_call(
            &rig.o.ctx,
            false,
            "overlap-run".into(),
            "parked_probe".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();

    let stale = rig.park_a_wakeup(call).await;
    rig.runner.run_wakeup(&rig.o.ctx, false, call).await;
    assert_eq!(rig.executions.load(Ordering::SeqCst), 1);

    rig.gate_release.notify_one();
    stale.await.unwrap();
    assert_eq!(
        rig.executions.load(Ordering::SeqCst),
        1,
        "the stale wakeup stood down instead of running the body again"
    );
    assert_eq!(
        rig.blocks_of("tool_result").await.len(),
        1,
        "one call, one result"
    );
    assert!(rig.blocks_of("tool_error").await.is_empty());
}

/// The double-request race, deterministic: wakeup B parks in the gate with a
/// snapshot showing no request; wakeup A defers and appends THE request; B
/// resumes, defers on its stale snapshot, and its insert meets the conditional
/// write — one request, and approving that one request clears the call.
#[tokio::test]
async fn overlapping_wakeups_append_one_approval_request() {
    let rig = OverlapRig::new(GateDecision::Defer).await;
    let call = rig
        .runner
        .insert_call(
            &rig.o.ctx,
            false,
            "overlap-park".into(),
            "parked_probe".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();

    let stale = rig.park_a_wakeup(call).await;
    rig.runner.run_wakeup(&rig.o.ctx, false, call).await;
    assert_eq!(rig.blocks_of("approval_request").await.len(), 1);

    rig.gate_release.notify_one();
    stale.await.unwrap();
    let requests = rig.blocks_of("approval_request").await;
    assert_eq!(
        requests.len(),
        1,
        "the stale wakeup's request write appended nothing"
    );

    // The one request is the one the fold consults: approving it runs the
    // body, so the conversation can never park behind an unconsulted twin.
    submit_approval(
        &rig.o.ctx.store,
        rig.o.ctx.conversation_id,
        requests[0].id,
        ApprovalChoice::Approved,
        None,
        None,
    )
    .await
    .unwrap();
    rig.runner.run_wakeup(&rig.o.ctx, false, call).await;
    assert_eq!(rig.executions.load(Ordering::SeqCst), 1);
    assert_eq!(rig.blocks_of("tool_result").await.len(), 1);
}

/// The denial-after-resolution race, through the real submit path: the call
/// resolves while the request is still open, and the late denial is refused
/// with a conflict — no decision, no second outcome, nothing appended. One
/// call never renders as two `tool_result` parts.
#[tokio::test]
async fn a_denial_after_resolution_is_refused_and_appends_nothing() {
    let mut rig = GateRig::new().await;
    let (call, request) = rig.deferred_call("cg-late-deny").await;
    rig.store()
        .complete_tool_call_block(
            rig.conv(),
            "cg-late-deny".into(),
            "already done".into(),
            call,
        )
        .await
        .unwrap()
        .expect("the out-of-band resolution writes");
    let before = rig.ledger_ids().await;

    let refused = submit_approval(
        rig.store(),
        rig.conv(),
        request,
        ApprovalChoice::Denied,
        None,
        Some("too late".into()),
    )
    .await;
    let error = refused.expect_err("the late denial is refused");
    assert!(
        error.to_string().contains("already resolved"),
        "the conflict is named: {error}"
    );

    assert_eq!(
        rig.ledger_ids().await,
        before,
        "the refused denial appends nothing"
    );
    assert!(rig.blocks_of("approval_decision").await.is_empty());
    assert!(rig.blocks_of("tool_error").await.is_empty());
    assert_eq!(rig.blocks_of("tool_result").await.len(), 1);
}

// ─── The claim across every exit path ────────────────────────────────────────

/// A tool whose body panics on its first run and succeeds on the retry.
struct PanicOnceProbe {
    attempts: Arc<AtomicUsize>,
}

impl ToolHandler<CoreEvent> for PanicOnceProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "panic_once".into(),
            description: "a test-only tool that panics on its first run".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            assert!(
                self.attempts.fetch_add(1, Ordering::SeqCst) != 0,
                "the body panicked"
            );
            ToolOutcome::Done("recovered".into())
        })
    }
}

/// A panicking body releases its claim on the unwind: the claim guard drops,
/// and a later wakeup can claim, retry the body and resolve the call — instead
/// of the claim leaking for the life of the process and parking the call
/// forever.
#[tokio::test]
async fn a_panicking_body_releases_its_claim_for_the_next_wakeup() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(
        "panic_once",
        PanicOnceProbe {
            attempts: Arc::clone(&attempts),
        },
    );
    let runner = Arc::new(ToolRunner::<BlockKind, _>::new(Arc::new(registry)));
    let o = Oracle::new().await;
    o.user_text("go").await;
    let call = runner
        .insert_call(
            &o.ctx,
            false,
            "will-panic".into(),
            "panic_once".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();

    let task_runner = Arc::clone(&runner);
    let ctx = AgencyCtx {
        conversation_id: o.ctx.conversation_id,
        store: o.ctx.store.clone(),
        bus: Arc::clone(&o.ctx.bus),
    };
    let first = tokio::spawn(async move { task_runner.run_wakeup(&ctx, false, call).await }).await;
    assert!(
        first.expect_err("the panic surfaces").is_panic(),
        "the first pass panicked inside the body"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    runner.run_wakeup(&o.ctx, false, call).await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the released claim let the retry reach the body"
    );
    let results: Vec<Block> = o
        .ctx
        .store
        .list_blocks(o.ctx.conversation_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|b| b.block_type == "tool_result")
        .collect();
    assert_eq!(results.len(), 1, "the retry resolved the call");
    assert_eq!(results[0].fields["content"], Value::from("recovered"));
}

// ─── The tool-call window ────────────────────────────────────────────────────

/// An ungated tool that counts every body execution. The window's whole claim
/// is that a refused call never reaches one, so the counter is what proves it.
///
/// The name is carried rather than baked in: the per-tool window's whole claim
/// is that it touches ONE name, which takes a second tool to prove.
struct CountingTool {
    name: &'static str,
    executions: Arc<AtomicUsize>,
}

impl ToolHandler<CoreEvent> for CountingTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name.into(),
            description: "a test-only tool that counts its runs".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            ToolOutcome::Done("ran".into())
        })
    }
}

/// One counting tool under `name`, and the counter proving whether its body
/// ran.
fn register_counter(
    registry: &mut ToolRegistry<CoreEvent>,
    name: &'static str,
) -> Arc<AtomicUsize> {
    let executions = Arc::new(AtomicUsize::new(0));
    registry.register(
        name,
        CountingTool {
            name,
            executions: Arc::clone(&executions),
        },
    );
    executions
}

/// A runner over one counting tool, with the window the test chose.
fn window_runner(window: ToolCallWindow) -> (ToolRunner<BlockKind, CoreEvent>, Arc<AtomicUsize>) {
    let mut registry = ToolRegistry::new();
    let executions = register_counter(&mut registry, "counter");
    let mut runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
    runner.set_window(window);
    (runner, executions)
}

/// Record a call of `name` and feed the chokepoint its one wakeup — the whole
/// admission pass, driven by the test rather than by a loop, so a red result
/// names an assertion instead of a timeout.
async fn drive_named_call(
    runner: &ToolRunner<BlockKind, CoreEvent>,
    ctx: &AgencyCtx<CoreEvent>,
    tool_call_id: &str,
    name: &str,
) -> i64 {
    let call = runner
        .insert_call(
            ctx,
            false,
            tool_call_id.into(),
            name.into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();
    runner.run_wakeup(ctx, false, call).await;
    call
}

/// [`drive_named_call`] for the counting tool the window pins are written
/// against.
async fn drive_call(
    runner: &ToolRunner<BlockKind, CoreEvent>,
    ctx: &AgencyCtx<CoreEvent>,
    tool_call_id: &str,
) -> i64 {
    drive_named_call(runner, ctx, tool_call_id, "counter").await
}

/// Every tool error recorded in the conversation, in ledger order.
async fn error_texts(ctx: &AgencyCtx<CoreEvent>) -> Vec<String> {
    ctx.store
        .list_blocks(ctx.conversation_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|block| block.block_type == "tool_error")
        .map(|block| block.fields["error"].as_str().unwrap().to_owned())
        .collect()
}

/// The window's numbers ARE the operator's decision, recorded once as the
/// defaults, and its refusal reads exactly as specified at those defaults —
/// asserted byte for byte, because the model reads this sentence and acts on
/// it. The forced end reads the row's own fact, never these bytes.
#[test]
fn the_default_window_and_its_refusal_are_pinned() {
    let window = ToolCallWindow::default();
    assert_eq!(window.calls, 60);
    assert_eq!(window.seconds, 60);
    assert_eq!(window.consecutive_limit, 5);

    assert_eq!(
        crate::agency::ToolError::rate_limit_refusal(window.calls, window.seconds),
        "tool-call rate limit: this conversation has spent its 60 tool calls for the last 60 \
         seconds, and this call was not run. Answer with what you already have, or wait \
         before calling tools again."
    );
    // The numbers are interpolated, never baked in: a deployment or a test
    // running its own window never ships a sentence that lies about it.
    assert!(
        crate::agency::ToolError::rate_limit_refusal(3, 30)
            .contains("its 3 tool calls for the last 30 seconds"),
        "the template carries the window actually in force"
    );
    assert!(
        crate::agency::ToolError::rate_limit_refusal(3, 30)
            .starts_with(crate::agency::ToolError::RATE_LIMIT_PREFIX),
        "both window templates open the same way for the model reading them"
    );
}

/// AC1 — the window refuses. With the window spent, the next call resolves
/// with the pinned refusal and its handler never runs; a second conversation
/// on the same runner, at the same moment, runs its call untouched, because
/// the count is a fold over ONE conversation's ledger.
#[tokio::test]
async fn a_spent_window_refuses_the_next_call_and_never_runs_its_body() {
    let (runner, executions) = window_runner(ToolCallWindow {
        calls: 2,
        seconds: 60,
        consecutive_limit: 5,
    });
    let o = Oracle::new().await;
    o.user_text("go").await;

    drive_call(&runner, &o.ctx, "w-1").await;
    drive_call(&runner, &o.ctx, "w-2").await;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "the window's own calls run"
    );
    assert!(
        error_texts(&o.ctx).await.is_empty(),
        "and none of them is refused"
    );

    drive_call(&runner, &o.ctx, "w-3").await;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "the call past the window never reached the body"
    );
    assert_eq!(
        error_texts(&o.ctx).await,
        vec![
            "tool-call rate limit: this conversation has spent its 2 tool calls for the last \
             60 seconds, and this call was not run. Answer with what you already have, or \
             wait before calling tools again."
                .to_owned()
        ],
        "the refusal is recorded as the call's outcome, in the window's own numbers"
    );

    // The same runner, the same instant, a different conversation: untouched.
    let second = o
        .ctx
        .store
        .create_conversation("p1".into(), "model".into(), "model".into(), String::new())
        .await
        .unwrap();
    let other = o.scoped_to(second);
    other.user_text("go").await;
    drive_call(&runner, &other.ctx, "w-other").await;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        3,
        "another conversation's window is its own"
    );
    assert!(
        error_texts(&other.ctx).await.is_empty(),
        "and nothing there is refused"
    );
}

/// AC2 — refusals keep the window spent. A refused call is a recorded call and
/// keeps counting: a window that forgot its own refusals would drain while the
/// model spams and hand the whole protection to the forced end.
#[tokio::test]
async fn refused_calls_keep_counting_against_the_window() {
    let (runner, executions) = window_runner(ToolCallWindow {
        calls: 3,
        seconds: 60,
        consecutive_limit: 5,
    });
    let o = Oracle::new().await;
    o.user_text("go").await;

    for id in ["r-1", "r-2", "r-3"] {
        drive_call(&runner, &o.ctx, id).await;
    }
    for id in ["r-4", "r-5", "r-6", "r-7"] {
        drive_call(&runner, &o.ctx, id).await;
    }

    assert_eq!(
        executions.load(Ordering::SeqCst),
        3,
        "three ran, and the window never recovered while the run continued"
    );
    let errors = error_texts(&o.ctx).await;
    assert_eq!(errors.len(), 4, "every call past the window is refused");
    assert!(
        errors
            .iter()
            .all(|error| error.starts_with(crate::agency::ToolError::RATE_LIMIT_PREFIX))
    );

    let ledger = o
        .ctx
        .store
        .list_blocks(o.ctx.conversation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, None),
        7,
        "the fold counts the refused calls too — executed and refused alike"
    );
}

/// AC6 — an interactive call is COUNTED by the window: it is a recorded call
/// and a paid round's product like any other. It is never runner-refused, and
/// that skip is defensive only — an interactive call is answered by the human
/// outright and never reaches the chokepoint at all, so there is no reachable
/// path to test it through.
#[tokio::test]
async fn the_window_counts_an_interactive_call() {
    let o = Oracle::new().await;
    o.user_text("go").await;
    o.call("plain").await;
    o.ctx
        .store
        .insert_tool_call_block(
            o.ctx.conversation_id,
            crate::block::Role::Assistant,
            crate::store::ToolCallInsert {
                tool_call_id: "asked".into(),
                name: "ask".into(),
                input: "{}".into(),
                interactive: true,
            },
            None,
        )
        .await
        .unwrap();

    let ledger = o
        .ctx
        .store
        .list_blocks(o.ctx.conversation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, None),
        2,
        "the interactive call counts against the window like every recorded call"
    );
}

/// A tool-call row as the ledger holds one, stamped `age` seconds ago in the
/// writer's own form — what a store insert would have written that long back.
fn aged_call(id: i64, age: i64) -> Block {
    named_aged_call(id, age, "counter")
}

/// The same row for whichever tool the test names — what a per-tool count has
/// to tell apart.
fn named_aged_call(id: i64, age: i64, name: &str) -> Block {
    stamped_call(
        id,
        (crate::store::now_instant() - chrono::TimeDelta::seconds(age))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, false),
        name,
    )
}

/// The same row with whatever stamp text the test names.
fn stamped_call(id: i64, created_at: String, name: &str) -> Block {
    Block {
        id,
        role: Some(crate::block::Role::Assistant),
        block_type: "tool_call".into(),
        created_at,
        dispatch_anchor: None,
        fields: serde_json::Map::from_iter([
            ("tool_call_id".to_owned(), Value::String(format!("c-{id}"))),
            ("name".to_owned(), Value::String(name.to_owned())),
            ("input".to_owned(), Value::String("{}".into())),
        ]),
    }
}

/// The count is bounded by the WINDOW, not by the ledger: the walk runs back
/// from the newest row and stops at the first call older than the window, so a
/// history of any depth in front of it costs nothing — and the answer is the
/// same one the whole-ledger fold gave. An unreadable stamp carries no time to
/// compare with, so it neither counts nor ends the walk: the in-window calls
/// behind it still count.
#[test]
fn the_window_count_stops_at_the_first_call_older_than_the_window() {
    let ledger: Vec<Block> = vec![
        aged_call(1, 4000),
        aged_call(2, 3000),
        aged_call(3, 2000),
        aged_call(4, 50),
        // The schema default's form, reachable only by a fixture writing a row
        // without a stamp: unparseable, and no reason to truncate the count.
        stamped_call(5, "2026-08-30 12:00:00".into(), "counter"),
        aged_call(6, 10),
    ];

    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, None),
        2,
        "the two calls inside the minute, across the unreadable stamp between them"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 3600, None),
        4,
        "a wider window reaches further back, and stops at the call outside it"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 0, None),
        0,
        "a window of no length holds nothing"
    );
}

/// AC5's admission half — restart safety. The window is a fold over the
/// ledger, so a process that lost every byte of its memory still refuses:
/// the store is reopened from disk (the in-memory idiom cannot reopen), the
/// first handle is dropped to release it, and the rebooted runtime — a fresh
/// runner with an empty in-flight set — is driven by an append.
#[tokio::test]
async fn a_reopened_ledger_keeps_the_window_spent() {
    let dir = crate::store::temp_dir("tool-call-window-restart");
    let db = dir.join("ledger.db");
    let window = ToolCallWindow {
        calls: 2,
        seconds: 60,
        consecutive_limit: 5,
    };
    let bus = Arc::new(crate::bus::EventBus::<CoreEvent>::new());

    let conv = {
        let store = Store::open(&db).unwrap();
        let conv = store
            .create_conversation("p1".into(), "model".into(), "model".into(), String::new())
            .await
            .unwrap();
        let ctx = AgencyCtx {
            conversation_id: conv,
            store,
            bus: Arc::clone(&bus),
        };
        let (runner, executions) = window_runner(window);
        drive_call(&runner, &ctx, "before-1").await;
        drive_call(&runner, &ctx, "before-2").await;
        assert_eq!(
            executions.load(Ordering::SeqCst),
            2,
            "the window's calls ran"
        );
        conv
    };

    let store = Store::open(&db).unwrap();
    let ctx = AgencyCtx {
        conversation_id: conv,
        store,
        bus,
    };
    let (runner, executions) = window_runner(window);
    drive_call(&runner, &ctx, "after-restart").await;

    assert_eq!(
        executions.load(Ordering::SeqCst),
        0,
        "the rebooted runner refuses off the ledger alone — nothing in memory told it to"
    );
    assert_eq!(
        error_texts(&ctx).await.len(),
        1,
        "and the refusal is recorded as that call's outcome"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

// ─── The per-tool windows ────────────────────────────────────────────────────

/// A runner over TWO counting tools — `counter` and `other` — with the
/// conversation-wide window and whatever per-tool bounds the test named. Two
/// tools, because the per-tool window's whole claim is that it touches one
/// name.
struct ToolWindowRig {
    runner: ToolRunner<BlockKind, CoreEvent>,
    counter: Arc<AtomicUsize>,
    other: Arc<AtomicUsize>,
}

impl ToolWindowRig {
    fn new(window: ToolCallWindow, bounds: &[(&str, usize, i64)]) -> Self {
        let mut registry = ToolRegistry::new();
        let counter = register_counter(&mut registry, "counter");
        let other = register_counter(&mut registry, "other");
        let mut runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
        runner.set_window(window);
        for (name, calls, seconds) in bounds {
            runner.set_tool_window(
                (*name).to_owned(),
                ToolWindowBound {
                    calls: *calls,
                    seconds: *seconds,
                },
            );
        }
        Self {
            runner,
            counter,
            other,
        }
    }

    fn counter_runs(&self) -> usize {
        self.counter.load(Ordering::SeqCst)
    }

    fn other_runs(&self) -> usize {
        self.other.load(Ordering::SeqCst)
    }
}

/// The per-tool refusal a bound of `calls` per `seconds` on `name` produces —
/// rendered from the numbers under test rather than copied, so a test never
/// pins a sentence that lies about its own window. The bytes themselves are
/// pinned out of crate, where the tool name is a consumer's to speak.
fn per_tool_refusal(name: &str, calls: usize, seconds: i64) -> String {
    crate::agency::ToolError::per_tool_rate_limit_refusal(name, calls, seconds)
}

/// The two templates read as two sentences and share one opening, because
/// both refusals mean the same thing to the model. What makes a per-tool
/// refusal feed the same run a conversation-wide one does is the fact its own
/// row carries, asserted where the count is.
#[test]
fn the_two_refusal_templates_differ_and_share_the_one_opening() {
    let conversation = crate::agency::ToolError::rate_limit_refusal(6, 60);
    let per_tool = per_tool_refusal("counter", 6, 60);

    assert!(conversation.starts_with(crate::agency::ToolError::RATE_LIMIT_PREFIX));
    assert!(per_tool.starts_with(crate::agency::ToolError::RATE_LIMIT_PREFIX));
    assert_ne!(
        conversation, per_tool,
        "the advice tails differ: a bounded tool leaves other tools open"
    );
    assert!(
        per_tool.contains("its 6 counter calls for the last 60 seconds"),
        "the tool's name and ITS numbers are interpolated — {per_tool}"
    );
    assert!(
        per_tool.ends_with(
            "Answer with what you already have, or use a different tool, or wait before \
             calling this one again."
        ),
        "the per-tool tail sends the model to another tool — {per_tool}"
    );
}

/// AC1 — the per-tool window refuses. With `counter` bound at two calls and
/// that bound spent, the next fresh admission of `counter` resolves with the
/// per-tool refusal and its handler never runs; a call to a DIFFERENT tool at
/// the same moment runs untouched; the same tool in a second conversation runs
/// untouched, because the count is a fold over ONE conversation's ledger.
#[tokio::test]
async fn a_spent_per_tool_window_refuses_that_tool_alone() {
    let rig = ToolWindowRig::new(ToolCallWindow::default(), &[("counter", 2, 60)]);
    let o = Oracle::new().await;
    o.user_text("go").await;

    drive_call(&rig.runner, &o.ctx, "t-1").await;
    drive_call(&rig.runner, &o.ctx, "t-2").await;
    assert_eq!(rig.counter_runs(), 2, "the tool's own calls run");
    assert!(
        error_texts(&o.ctx).await.is_empty(),
        "and none of them is refused"
    );

    drive_call(&rig.runner, &o.ctx, "t-3").await;
    assert_eq!(
        rig.counter_runs(),
        2,
        "the call past the tool's window never reached the body"
    );
    assert_eq!(
        error_texts(&o.ctx).await,
        vec![per_tool_refusal("counter", 2, 60)],
        "the refusal is recorded as the call's outcome, in that tool's own numbers"
    );

    // The same conversation, the same instant, a tool with no bound.
    drive_named_call(&rig.runner, &o.ctx, "t-other", "other").await;
    assert_eq!(rig.other_runs(), 1, "another tool's calls are its own");
    assert_eq!(
        error_texts(&o.ctx).await.len(),
        1,
        "and nothing new is refused"
    );

    // The same runner, the same instant, a different conversation.
    let second = o
        .ctx
        .store
        .create_conversation("p1".into(), "model".into(), "model".into(), String::new())
        .await
        .unwrap();
    let elsewhere = o.scoped_to(second);
    elsewhere.user_text("go").await;
    drive_call(&rig.runner, &elsewhere.ctx, "t-elsewhere").await;
    assert_eq!(
        rig.counter_runs(),
        3,
        "another conversation's per-tool window is its own"
    );
    assert!(
        error_texts(&elsewhere.ctx).await.is_empty(),
        "and nothing there is refused"
    );
}

/// AC2 — both windows compose, in the pinned order. The conversation's window
/// is the OUTER protection and speaks first: with both spent at the same
/// moment the conversation-wide text lands, never the tool's. And a refused
/// call keeps counting against BOTH folds — it is a recorded call.
#[tokio::test]
async fn the_conversation_window_speaks_before_a_tool_window() {
    let rig = ToolWindowRig::new(
        ToolCallWindow {
            calls: 2,
            seconds: 60,
            consecutive_limit: 5,
        },
        &[("counter", 1, 60)],
    );
    let o = Oracle::new().await;
    o.user_text("go").await;

    // One inside both bounds; one past the tool's bound while the
    // conversation's is cold; one past both.
    drive_call(&rig.runner, &o.ctx, "c-1").await;
    drive_call(&rig.runner, &o.ctx, "c-2").await;
    drive_call(&rig.runner, &o.ctx, "c-3").await;

    assert_eq!(rig.counter_runs(), 1, "only the first call ever ran");
    assert_eq!(
        error_texts(&o.ctx).await,
        vec![
            per_tool_refusal("counter", 1, 60),
            crate::agency::ToolError::rate_limit_refusal(2, 60),
        ],
        "the tool's window refuses while the conversation's is cold, and the \
         conversation's answers first once it is spent"
    );

    let ledger = o
        .ctx
        .store
        .list_blocks(o.ctx.conversation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, None),
        3,
        "every refused call still counts against the conversation's window"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, Some("counter")),
        3,
        "and against the tool's own"
    );
}

/// AC4 — a tool with no entry in the map meets no per-tool check, under any
/// state of the conversation's window: it runs past every bound its neighbour
/// has, and when the conversation's window is spent it is refused by THAT
/// window's text, never by a per-tool one.
#[tokio::test]
async fn an_unbound_tool_meets_no_per_tool_window() {
    let rig = ToolWindowRig::new(
        ToolCallWindow {
            calls: 3,
            seconds: 60,
            consecutive_limit: 5,
        },
        &[("counter", 1, 60)],
    );
    let o = Oracle::new().await;
    o.user_text("go").await;

    for id in ["u-1", "u-2", "u-3"] {
        drive_named_call(&rig.runner, &o.ctx, id, "other").await;
    }
    assert_eq!(
        rig.other_runs(),
        3,
        "the unbound tool ran past its neighbour's bound"
    );
    assert!(
        error_texts(&o.ctx).await.is_empty(),
        "and nothing refused it"
    );

    drive_named_call(&rig.runner, &o.ctx, "u-4", "other").await;
    assert_eq!(rig.other_runs(), 3, "the conversation's window stopped it");
    assert_eq!(
        error_texts(&o.ctx).await,
        vec![crate::agency::ToolError::rate_limit_refusal(3, 60)],
        "refused by the conversation's window, with no per-tool bound in sight"
    );
}

/// The per-tool count is the same fold with a name filter: it counts one
/// tool's rows, it ages out with the window exactly as the whole count does,
/// and the walk's window edge still ends on ANY call older than the span —
/// which is what keeps a filtered count one walk rather than a full scan.
#[test]
fn the_per_tool_count_reads_one_name_and_ages_out() {
    let ledger: Vec<Block> = vec![
        named_aged_call(1, 4000, "counter"),
        named_aged_call(2, 3000, "counter"),
        named_aged_call(3, 40, "counter"),
        named_aged_call(4, 30, "other"),
        named_aged_call(5, 20, "counter"),
    ];

    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, Some("counter")),
        2,
        "the bound tool's calls inside the minute, the other tool's skipped"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, Some("other")),
        1,
        "and each name answers for itself"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 60, None),
        3,
        "while the unfiltered count is still every call in the span"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 3600, Some("counter")),
        3,
        "a wider span reaches the older call, and stops at the one outside it"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 25, Some("counter")),
        1,
        "as the span narrows the tool's window recovers, call by call"
    );
    assert_eq!(
        crate::agency::ToolCall::calls_in_trailing_window(&ledger, 0, Some("counter")),
        0,
        "a window of no length holds nothing"
    );
}

/// AC5's admission half for a tool's own window — restart safety. The per-tool
/// count is a fold over the ledger like every other, so a process that lost
/// every byte of its memory keeps refusing the bound tool off the reopened
/// store alone, while an unbound tool runs there untouched.
#[tokio::test]
async fn a_reopened_ledger_keeps_a_tool_window_spent() {
    let dir = crate::store::temp_dir("per-tool-window-restart");
    let db = dir.join("ledger.db");
    let bounds: &[(&str, usize, i64)] = &[("counter", 2, 60)];
    let bus = Arc::new(crate::bus::EventBus::<CoreEvent>::new());

    let conv = {
        let store = Store::open(&db).unwrap();
        let conv = store
            .create_conversation("p1".into(), "model".into(), "model".into(), String::new())
            .await
            .unwrap();
        let ctx = AgencyCtx {
            conversation_id: conv,
            store,
            bus: Arc::clone(&bus),
        };
        let rig = ToolWindowRig::new(ToolCallWindow::default(), bounds);
        drive_call(&rig.runner, &ctx, "before-1").await;
        drive_call(&rig.runner, &ctx, "before-2").await;
        assert_eq!(rig.counter_runs(), 2, "the tool's window's calls ran");
        conv
    };

    let store = Store::open(&db).unwrap();
    let ctx = AgencyCtx {
        conversation_id: conv,
        store,
        bus,
    };
    let rig = ToolWindowRig::new(ToolCallWindow::default(), bounds);
    drive_call(&rig.runner, &ctx, "after-restart").await;
    assert_eq!(
        rig.counter_runs(),
        0,
        "the rebooted runner refuses off the ledger alone — nothing in memory told it to"
    );
    assert_eq!(
        error_texts(&ctx).await,
        vec![per_tool_refusal("counter", 2, 60)],
        "and the refusal is recorded as that call's outcome"
    );

    drive_named_call(&rig.runner, &ctx, "after-restart-other", "other").await;
    assert_eq!(
        rig.other_runs(),
        1,
        "the unbound tool is unbounded across the restart too"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

// ─── The tool that ends the turn ─────────────────────────────────────────────

/// A tool whose successful call ENDS the turn: the model said it has nothing
/// left to do. Counts its runs, because half of what the pins below claim is
/// that a body did or did not reach one.
struct ParkProbe {
    executions: Arc<AtomicUsize>,
}

impl ToolHandler<CoreEvent> for ParkProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "park".into(),
            description: "a test-only tool that ends the turn".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn ends_turn(&self) -> bool {
        true
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            ToolOutcome::Done("nothing to do".into())
        })
    }
}

/// An ends-turn tool that hands its work to a backing system — the contract
/// defect the runner refuses, since a deferred resolution takes the door that
/// carries no stamp.
struct DeferringParkProbe;

impl ToolHandler<CoreEvent> for DeferringParkProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "deferring_park".into(),
            description: "a test-only ends-turn tool that defers".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn ends_turn(&self) -> bool {
        true
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Pending })
    }
}

/// A handler declaring both `interactive()` and `ends_turn()`: the human
/// answers an interactive call through the door that holds no handler, so the
/// stamp could never be written.
struct InteractiveParkProbe;

impl ToolHandler<CoreEvent> for InteractiveParkProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "ask_and_park".into(),
            description: "a contradiction".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn interactive(&self) -> bool {
        true
    }

    fn ends_turn(&self) -> bool {
        true
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Done("never".into()) })
    }
}

/// A runner over the park tool, an ordinary counting tool and the deferring
/// one, with the window the test chose. Both counters come back: the pins read
/// which body ran.
fn park_runner(
    window: ToolCallWindow,
) -> (
    ToolRunner<BlockKind, CoreEvent>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let mut registry = ToolRegistry::new();
    let counter = register_counter(&mut registry, "counter");
    let park = Arc::new(AtomicUsize::new(0));
    registry.register(
        "park",
        ParkProbe {
            executions: Arc::clone(&park),
        },
    );
    registry.register("deferring_park", DeferringParkProbe);
    let mut runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
    runner.set_window(window);
    (runner, park, counter)
}

/// [`drive_named_call`] with the call recorded as a TURN's product, the way a
/// streamed round records one — which is what the turn folds read.
async fn drive_anchored_call(
    runner: &ToolRunner<BlockKind, CoreEvent>,
    ctx: &AgencyCtx<CoreEvent>,
    tool_call_id: &str,
    name: &str,
    anchor: i64,
) -> i64 {
    let call = runner
        .insert_call(
            ctx,
            false,
            tool_call_id.into(),
            name.into(),
            "{}".into(),
            CallOrigin::streamed(None, Some(anchor)),
        )
        .await
        .unwrap();
    runner.run_wakeup(ctx, false, call).await;
    call
}

/// This conversation's tool results as `(content, stamped)` — the loaded
/// ledger handed to the shared read
/// ([`crate::agency::results_with_stamps`]), which parses the stamp through
/// the kind that owns it.
async fn conversation_results(ctx: &AgencyCtx<CoreEvent>) -> Vec<(String, bool)> {
    let ledger = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
    crate::agency::results_with_stamps(&ledger)
}

/// The parsed ask of the ledger's last block — who the frontier says owes the
/// next move, answered off the stored row alone.
async fn tail_ask(ctx: &AgencyCtx<CoreEvent>) -> Option<Awaiting> {
    let ledger = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
    BlockKind::from_block(ledger.last().expect("a ledger with blocks")).awaiting()
}

/// The capability, end to end at the chokepoint: an ordinary tool's result
/// asks the model for its continuation, and a park tool's resolution — the
/// same door, the same round — is STAMPED and asks for nothing, so the drive
/// finds no turn owed. The stamp rides the handler, never the name: both
/// results are written by one code path that differs only in what the handler
/// answered.
#[tokio::test]
async fn a_park_tools_resolution_is_stamped_and_rests_the_frontier() {
    let (runner, park_runs, counter_runs) = park_runner(ToolCallWindow::default());
    let o = Oracle::new().await;
    o.user_text("go").await;

    drive_named_call(&runner, &o.ctx, "ordinary", "counter").await;
    assert_eq!(counter_runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        conversation_results(&o.ctx).await,
        vec![("ran".to_owned(), false)],
        "an ordinary tool's result carries no stamp"
    );
    assert_eq!(
        tail_ask(&o.ctx).await,
        Some(Awaiting::Model),
        "and it asks the model for its continuation"
    );
    assert!(
        o.drive().await.owes_turn,
        "the drained frontier owes a turn against an ordinary result"
    );

    drive_named_call(&runner, &o.ctx, "parked", "park").await;
    assert_eq!(park_runs.load(Ordering::SeqCst), 1, "the park body ran");
    assert_eq!(
        conversation_results(&o.ctx).await,
        vec![
            ("ran".to_owned(), false),
            ("nothing to do".to_owned(), true)
        ],
        "the park resolution carries the stamp"
    );
    assert_eq!(
        tail_ask(&o.ctx).await,
        None,
        "and asks nobody — the stored row is the turn's end"
    );
    assert!(
        !o.drive().await.owes_turn,
        "so no continuation is owed and none is dispatched"
    );
}

/// The stamp is decided at exactly ONE door. The public resolution write — the
/// out-of-band door a backing system takes — holds no handler and writes an
/// unstamped result even for a call recorded under the park tool's own name:
/// nothing anywhere infers the capability from a name.
#[tokio::test]
async fn the_public_resolution_door_writes_an_unstamped_result() {
    let (runner, park_runs, _counter) = park_runner(ToolCallWindow::default());
    let o = Oracle::new().await;
    o.user_text("go").await;

    // Recorded while latched: the call lands in the ledger and no body runs,
    // so the only writer of its outcome is the door below.
    let call = runner
        .insert_call(
            &o.ctx,
            true,
            "out-of-band".into(),
            "park".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();
    o.ctx
        .store
        .complete_tool_call_block(
            o.ctx.conversation_id,
            "out-of-band".into(),
            "answered elsewhere".into(),
            call,
        )
        .await
        .unwrap()
        .expect("the out-of-band door resolves the call");

    assert_eq!(park_runs.load(Ordering::SeqCst), 0, "no body ever ran");
    assert_eq!(
        conversation_results(&o.ctx).await,
        vec![("answered elsewhere".to_owned(), false)],
        "the door that holds no handler writes no stamp"
    );
    assert_eq!(
        tail_ask(&o.ctx).await,
        Some(Awaiting::Model),
        "so the result asks for its continuation like any other"
    );
}

/// A park tool cannot defer: a `Pending` outcome from an ends-turn handler
/// would resolve later through the unstamped door and silently summon the
/// model after the end of a turn. The runner refuses it instead, with the
/// sentence the model reads — pinned byte for byte — and the refusal is an
/// ERROR, which carries no stamp and hands the model its ordinary round.
#[tokio::test]
async fn a_deferring_ends_turn_tool_is_refused_with_the_pinned_text() {
    let (runner, _park_runs, _counter) = park_runner(ToolCallWindow::default());
    let o = Oracle::new().await;
    o.user_text("go").await;

    drive_named_call(&runner, &o.ctx, "deferred", "deferring_park").await;

    assert_eq!(
        error_texts(&o.ctx).await,
        vec![
            "an ends-turn tool must resolve at once: deferring the end of a turn is a \
             contract defect, and this call is refused."
                .to_owned()
        ],
        "the deferral is refused, in the words the model re-plans against"
    );
    assert!(
        conversation_results(&o.ctx).await.is_empty(),
        "and no result — stamped or not — was written"
    );
    assert_eq!(
        tail_ask(&o.ctx).await,
        Some(Awaiting::Model),
        "an error never ends a turn: the model gets one more round"
    );
}

/// The pairing an interactive ends-turn tool would be is refused at
/// registration, in debug builds, on the gated-plus-interactive precedent: the
/// human resolves an interactive call through the handler-less door, so the
/// stamp could never be written and the capability would ship mute.
///
/// Gated on `debug_assertions` like that precedent's own pin: the check is a
/// debug assertion, so a release build compiles it out and a test demanding
/// its panic there would fail for the reason it exists.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "declares both interactive() and ends_turn()")]
fn an_interactive_ends_turn_handler_is_refused_at_registration() {
    let mut registry = ToolRegistry::<CoreEvent>::new();
    registry.register("ask_and_park", InteractiveParkProbe);
}

/// AC3 — a park call the window refuses does NOT end the turn. The window
/// check runs before the registry lookup, so a park call at a hot window is
/// refused like any call: the body never runs, the outcome is a rate-limit
/// ERROR, the frontier owes the model one more round — in which it ends the
/// turn the ordinary way — and the run the forced end counts sees the refusal
/// exactly as it sees any other. No name-keyed bypass exists to test, and that
/// is the point.
#[tokio::test]
async fn a_park_call_the_window_refuses_does_not_end_the_turn() {
    let (runner, park_runs, _counter) = park_runner(ToolCallWindow {
        calls: 1,
        seconds: 60,
        consecutive_limit: 5,
    });
    let o = Oracle::new().await;
    let summons = o.user_text("go").await;

    drive_anchored_call(&runner, &o.ctx, "p-1", "park", summons).await;
    drive_anchored_call(&runner, &o.ctx, "p-2", "park", summons).await;

    assert_eq!(
        park_runs.load(Ordering::SeqCst),
        1,
        "the call past the window never reached the body"
    );
    assert_eq!(
        error_texts(&o.ctx).await,
        vec![crate::agency::ToolError::rate_limit_refusal(1, 60)],
        "a park call is refused in the window's own words, like any call"
    );
    assert_eq!(
        tail_ask(&o.ctx).await,
        Some(Awaiting::Model),
        "the refusal summons one more round"
    );
    assert!(
        o.drive().await.owes_turn,
        "and the drained frontier owes that turn"
    );

    let ledger = o
        .ctx
        .store
        .list_blocks(o.ctx.conversation_id)
        .await
        .unwrap();
    assert_eq!(
        crate::agency::ToolCall::trailing_refusal_run(&ledger, summons),
        1,
        "the five-rule counts the refused park call like any refusal"
    );
    assert_eq!(
        crate::agency::ToolCall::outcomes_anchored_in(&ledger, summons),
        1,
        "the error asks for a continuation; the stamped result before it does not"
    );
}

/// A consumer tool that DECLINES the call: it read the request, decided
/// against it, and hands back only the reason.
struct DeclineProbe;

impl ToolHandler<CoreEvent> for DeclineProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "decline".into(),
            description: "a test-only declining tool".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Refused("not this one".into()) })
    }
}

/// A consumer tool that TRIED and failed — the same outcome shape, the other
/// fact.
struct BreakProbe;

impl ToolHandler<CoreEvent> for BreakProbe {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "break".into(),
            description: "a test-only failing tool".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Error("the file system said no".into()) })
    }
}

/// F3 — a consumer's decline counts toward the forced turn end and a
/// consumer's failure does not, decided off the stored fact on the outcome row
/// and never off the sentence. The framework matches no consumer vocabulary:
/// the words are the tool's, the fact is [`ToolOutcome::Refused`] versus
/// [`ToolOutcome::Error`].
#[tokio::test]
async fn a_consumers_decline_counts_toward_the_forced_end_and_its_failure_does_not() {
    let mut registry = ToolRegistry::new();
    registry.register("decline", DeclineProbe);
    registry.register("break", BreakProbe);
    let runner = ToolRunner::<BlockKind, _>::new(Arc::new(registry));
    let o = Oracle::new().await;
    let summons = o.user_text("go").await;

    let run = |o: &Oracle| {
        let ctx = o.ctx.clone();
        async move {
            let ledger = ctx.store.list_blocks(ctx.conversation_id).await.unwrap();
            crate::agency::ToolCall::trailing_refusal_run(&ledger, summons)
        }
    };

    drive_anchored_call(&runner, &o.ctx, "d-1", "decline", summons).await;
    assert_eq!(run(&o).await, 1, "the decline is a refusal");

    drive_anchored_call(&runner, &o.ctx, "b-1", "break", summons).await;
    assert_eq!(
        run(&o).await,
        0,
        "a failure is not a refusal, and it breaks the trailing run"
    );

    drive_anchored_call(&runner, &o.ctx, "d-2", "decline", summons).await;
    drive_anchored_call(&runner, &o.ctx, "d-3", "decline", summons).await;
    assert_eq!(run(&o).await, 2, "the run counts from the failure forward");

    assert_eq!(
        error_texts(&o.ctx).await,
        vec![
            "not this one".to_owned(),
            "the file system said no".to_owned(),
            "not this one".to_owned(),
            "not this one".to_owned(),
        ],
        "both outcomes reach the model as its own words, unwrapped"
    );
    assert!(
        conversation_results(&o.ctx).await.is_empty(),
        "neither wrote a result"
    );
    assert_eq!(
        tail_ask(&o.ctx).await,
        Some(Awaiting::Model),
        "and either way the model is owed one more round"
    );
}
