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
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::Value;

use crate::agency::ratchet::oracle::Oracle;
use crate::agency::{AgencyCtx, Awaiting, GateDecision, ratchet, redispatch};
use crate::block::Block;
use crate::event::CoreEvent;
use crate::providers::types::ToolDefinition;
use crate::providers::{BoxFuture, blocks_to_messages};
use crate::store::Store;
use crate::types::ApprovalChoice;

use super::{
    ToolContext, ToolHandler, ToolOutcome, ToolRegistry, ToolRunner, admission::submit_approval,
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
    let outcome = ratchet::drive(ctx)
        .await
        .expect("the drive reads its ledger");
    redispatch::walk(ctx).await.expect("the walk unwinds");
    Some(outcome)
}

/// Every pending wakeup's call id, in arrival order.
fn drain(rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let CoreEvent::ToolCallReady { tool_call_id, .. } = event {
            ids.push(tool_call_id);
        }
    }
    ids
}

/// A stored ledger, a runner over a registry holding one gated probe, and the
/// probe's execution counter.
struct GateRig {
    o: Oracle,
    runner: ToolRunner<CoreEvent>,
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
            runner: ToolRunner::new(Arc::new(registry)),
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
                tool_call_id,
            } = event
            {
                assert_eq!(conversation_id, self.conv());
                self.runner
                    .run_wakeup(&self.o.ctx, false, &tool_call_id)
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
                None,
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
    let with = serde_json::to_value(blocks_to_messages(ledger)).unwrap();
    let without = serde_json::to_value(blocks_to_messages(&stripped)).unwrap();
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
    o.call("d1-first").await;
    o.call("d1-second").await;

    tick(&o.ctx, false).await;
    assert_eq!(drain(&mut o.rx), vec!["d1-first"]);
    tick(&o.ctx, false).await;
    assert_eq!(
        drain(&mut o.rx),
        vec!["d1-first"],
        "the second call is never driven by the cursor while the first dangles"
    );

    o.result("d1-first").await;
    tick(&o.ctx, false).await;
    assert_eq!(
        drain(&mut o.rx),
        vec!["d1-second"],
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
    o.call("d2-stuck").await;
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
        assert_eq!(drain(&mut o.rx), vec!["d2-stuck"]);
    }
}

/// PARALLELISM: two sibling calls inserted through the live-tail path both emit
/// wakeups BEFORE either resolves and before any tick. This is the test that
/// fails if the drive at insert is ever removed — the cursor alone parks on the
/// first sibling and would never emit the second's wakeup.
#[tokio::test]
async fn live_tail_siblings_emit_wakeups_before_either_resolves() {
    let runner = ToolRunner::new(Arc::new(ToolRegistry::<CoreEvent>::new()));
    let mut o = Oracle::new().await;
    o.user_text("fan out").await;
    for id in ["d3-sib1", "d3-sib2"] {
        runner
            .insert_call(
                &o.ctx,
                false,
                id.into(),
                "read_file".into(),
                "{}".into(),
                None,
            )
            .await
            .unwrap();
    }

    o.expect_wakeup("d3-sib1");
    o.expect_wakeup("d3-sib2");
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
            None,
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
    for id in ["dropped", "after-unlatch"] {
        rig.runner
            .insert_call(
                &rig.o.ctx,
                true, // recorded, not driven: no wakeup of its own
                id.into(),
                "gated_probe".into(),
                "{}".into(),
                None,
            )
            .await
            .unwrap();
    }

    rig.runner.run_wakeup(&rig.o.ctx, true, "dropped").await;
    assert_eq!(rig.executions(), 0, "the latched wakeup is dropped");

    rig.runner
        .run_wakeup(&rig.o.ctx, false, "after-unlatch")
        .await;
    assert_eq!(rig.executions(), 1, "only the unlatched wakeup ran");

    let results = rig.blocks_of("tool_result").await;
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields["tool_call_id"],
        Value::from("after-unlatch"),
        "the dropped call is still unresolved — the ratchet's re-emit is its recovery"
    );
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
    let runner = ToolRunner::new(Arc::new(registry));
    let mut o = Oracle::new().await;
    o.user_text("go").await;
    let call = runner
        .insert_call(
            &o.ctx,
            false,
            "iv-1".into(),
            "ask_human".into(),
            "{}".into(),
            None,
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
