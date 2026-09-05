//! Per-kind owed-set equivalence, plus the adversarial id-keying and re-emit
//! cases.
//!
//! Every assertion here answers one question about one kind: who does it say
//! owes its next move, does its own work finish, and does it stay silent when
//! it has nothing to say. The machinery is tested next door, against ledgers.

use tokio::sync::broadcast::error::TryRecvError;

use crate::block::Role;
use crate::event::CoreEvent;
use crate::store::ToolCallInsert;
use crate::types::ApprovalChoice;

use super::*;

/// The kinds that ask nothing, do nothing and say nothing.
///
/// Three role-carrying record types that belong to a consumer rather than to
/// the runtime used to ride this list. What they pinned was generic — a pure
/// record is inert in every direction — and that claim now rests on `status`,
/// whose shape is identical (a stored role and nothing else), and on the
/// unknown fallback, which the run and gate sweeps below exercise through a
/// type string this build has never heard of.
const INERT_TYPES: [&str; 7] = [
    "thinking",
    "system_prompt",
    "status",
    "streaming",
    "streaming_thinking",
    "streaming_tool_call",
    "date_marker",
];

const AUTHORED_TYPES: [&str; 3] = ["text", "quote", "code"];

/// A stored type string this build does not know, used wherever the unknown
/// fallback is the subject.
const UNREGISTERED_TYPE: &str = "holographic_block";

struct Rig {
    ctx: AgencyCtx<CoreEvent>,
    rx: tokio::sync::broadcast::Receiver<CoreEvent>,
}

async fn rig() -> Rig {
    let store = Store::in_memory().unwrap();
    let conversation_id = store
        .create_conversation("p1".into(), "model".into(), "model".into(), String::new())
        .await
        .unwrap();
    let bus = Arc::new(EventBus::new());
    let rx = bus.subscribe();
    Rig {
        ctx: AgencyCtx {
            conversation_id,
            store,
            bus,
        },
        rx,
    }
}

fn bare_block(block_type: &str, role: Option<Role>) -> Block {
    Block {
        id: 1,
        role,
        block_type: block_type.into(),
        created_at: String::new(),
        dispatch_anchor: None,
        fields: serde_json::Map::new(),
    }
}

fn kind(block_type: &str, role: Option<Role>) -> BlockKind {
    BlockKind::from_block(&bare_block(block_type, role))
}

/// The block this id names, out of a ledger that must be holding it: every
/// test here reads a stored row by its id, and a ledger missing it is the
/// test's own setup being wrong, never a case under assertion.
fn block_named(ledger: &[Block], block_id: i64) -> &Block {
    ledger
        .iter()
        .find(|block| block.id == block_id)
        .expect("the ledger holds the block")
}

async fn stored_kind(rig: &Rig, block_id: i64) -> BlockKind {
    let ledger = rig
        .ctx
        .store
        .list_blocks(rig.ctx.conversation_id)
        .await
        .unwrap();
    BlockKind::from_block(block_named(&ledger, block_id))
}

async fn resolve_with_result(rig: &Rig, tool_call_id: &str, call_block_id: i64) {
    rig.ctx
        .store
        .complete_tool_call_block(
            rig.ctx.conversation_id,
            tool_call_id.into(),
            "ok".into(),
            call_block_id,
        )
        .await
        .unwrap()
        .expect("the call is unresolved");
}

async fn insert_call(rig: &Rig, tool_call_id: &str) -> i64 {
    rig.ctx
        .store
        .insert_tool_call_block(
            rig.ctx.conversation_id,
            Role::Assistant,
            ToolCallInsert {
                tool_call_id: tool_call_id.into(),
                name: "read_file".into(),
                input: "{}".into(),
                interactive: false,
            },
            None,
        )
        .await
        .unwrap()
}

#[track_caller]
fn assert_silent(rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>) {
    assert!(
        matches!(rx.try_recv(), Err(TryRecvError::Empty)),
        "expected no bus emission"
    );
}

#[track_caller]
fn assert_wakeup(
    rx: &mut tokio::sync::broadcast::Receiver<CoreEvent>,
    conversation_id: i64,
    call_block_id: i64,
) {
    match rx.try_recv().expect("expected a ToolCallReady wakeup") {
        CoreEvent::ToolCallReady {
            conversation_id: c,
            call_block_id: id,
        } => {
            assert_eq!(c, conversation_id);
            assert_eq!(id, call_block_id);
        }
        other => panic!("expected ToolCallReady, got {other:?}"),
    }
}

// ─── Parsing ─────────────────────────────────────────────────────────────

/// Every type string the library stores resolves to its own variant, and
/// anything else resolves to the inert fallback.
#[test]
fn every_stored_type_parses_to_its_variant() {
    assert!(matches!(kind("text", None), BlockKind::Text(_)));
    assert!(matches!(kind("quote", None), BlockKind::Quote(_)));
    assert!(matches!(kind("code", None), BlockKind::Code(_)));
    assert!(matches!(kind("thinking", None), BlockKind::Thinking(_)));
    assert!(matches!(kind("tool_call", None), BlockKind::ToolCall(_)));
    assert!(matches!(
        kind("tool_result", None),
        BlockKind::ToolResult(_)
    ));
    assert!(matches!(kind("tool_error", None), BlockKind::ToolError(_)));
    assert!(matches!(kind("status", None), BlockKind::Status(_)));
    assert!(matches!(
        kind("system_prompt", None),
        BlockKind::SystemPrompt(_)
    ));
    assert!(matches!(kind("streaming", None), BlockKind::Streaming(_)));
    assert!(matches!(
        kind("streaming_thinking", None),
        BlockKind::StreamingThinking(_)
    ));
    assert!(matches!(
        kind("streaming_tool_call", None),
        BlockKind::StreamingToolCall(_)
    ));
    assert!(matches!(
        kind("approval_request", None),
        BlockKind::ApprovalRequest(_)
    ));
    assert!(matches!(
        kind("approval_decision", None),
        BlockKind::ApprovalDecision(_)
    ));
    assert!(matches!(
        kind("date_marker", None),
        BlockKind::DateMarker(_)
    ));
    assert!(matches!(
        kind("title_request", None),
        BlockKind::MetadataTitleRequest(_)
    ));
    assert!(matches!(
        kind("title_response", None),
        BlockKind::MetadataTitleResponse(_)
    ));
    assert!(matches!(
        kind(UNREGISTERED_TYPE, None),
        BlockKind::Unknown(_)
    ));
}

// ─── awaiting() per kind ─────────────────────────────────────────────────

#[test]
fn user_authored_blocks_await_the_model() {
    for block_type in AUTHORED_TYPES {
        assert_eq!(
            kind(block_type, Some(Role::User)).awaiting(),
            Some(Awaiting::Model)
        );
    }
}

#[test]
fn non_user_authored_blocks_await_nothing() {
    for block_type in AUTHORED_TYPES {
        for role in [
            Some(Role::Assistant),
            Some(Role::System),
            Some(Role::Tool),
            None,
        ] {
            assert_eq!(kind(block_type, role).awaiting(), None);
        }
    }
}

/// The harness's own message asks the model for a turn: it awaits the model,
/// whatever voice the row carries, so the dispatch never has to know which
/// kind it just read. What that turn is OFFERED is no longer this kind's
/// answer — the door that writes this block writes an empty tool choice beside
/// it, and the dispatch reads that record.
#[test]
fn the_harness_message_asks_the_model_for_a_turn() {
    for role in [Some(Role::System), None] {
        assert_eq!(
            kind(HarnessMessage::KINDS[0], role).awaiting(),
            Some(Awaiting::Model),
            "the ask is the KIND's, not the voice's"
        );
    }
}

/// AC1 — the recorded tool choice is inert in every direction: it asks
/// nobody for anything, does its own work by existing, and says nothing to
/// the model. What it is NOT is opaque to the frontier — that half is
/// asserted against a stored ledger next door, where the burial it prevents
/// can actually happen.
#[test]
fn the_tool_choice_is_a_record_and_behaves_like_one() {
    let recorded = kind(ToolChoice::KINDS[0], None);
    assert_eq!(recorded.awaiting(), None, "a record asks nobody");
    assert_eq!(
        recorded.llm_parts(),
        None,
        "a record shows the model nothing"
    );
    assert_eq!(recorded.llm_text(), None);
    assert!(
        recorded.frontier_transparent(),
        "the frontier reads through it, or a message behind one is owed by nobody"
    );
}

/// The harness STATING is not the harness asking (2026-08-31, the review of
/// the compaction slice). A compacted thread's digest is system-voiced prose
/// sitting in a thread that serves a live channel: were the ask a fact about
/// that voice, a thread whose frontier walked back onto its digest would
/// dispatch a turn nobody asked for and speak into the channel. Prose states;
/// the kind next door asks.
#[test]
fn system_voiced_prose_states_and_asks_for_nothing() {
    for block_type in AUTHORED_TYPES {
        assert_eq!(
            kind(block_type, Some(Role::System)).awaiting(),
            None,
            "{block_type} in the harness's voice summons nothing"
        );
    }
}

#[test]
fn inert_kinds_have_no_ask() {
    for block_type in INERT_TYPES {
        assert_eq!(kind(block_type, Some(Role::Assistant)).awaiting(), None);
    }
}

#[test]
fn tool_outcomes_await_the_model() {
    assert_eq!(kind("tool_result", None).awaiting(), Some(Awaiting::Model));
    assert_eq!(kind("tool_error", None).awaiting(), Some(Awaiting::Model));
}

/// The metadata rows: the request awaits the SYSTEM — the runtime owes the
/// derivation — and the response asks nothing. Their `run()` behavior is pinned
/// with the metadata ratchet next door.
#[test]
fn metadata_request_awaits_the_system_and_the_response_awaits_nothing() {
    assert_eq!(
        kind("title_request", None).awaiting(),
        Some(Awaiting::System)
    );
    assert_eq!(kind("title_response", None).awaiting(), None);
}

#[test]
fn regular_call_awaits_the_system_interactive_awaits_the_user() {
    assert_eq!(
        kind("tool_call", Some(Role::Assistant)).awaiting(),
        Some(Awaiting::System)
    );

    let mut block = bare_block("tool_call", Some(Role::Assistant));
    block
        .fields
        .insert("interactive".into(), serde_json::Value::Bool(true));
    assert_eq!(
        BlockKind::from_block(&block).awaiting(),
        Some(Awaiting::User)
    );
}

// ─── run() doneness and silence for the trivially-done kinds ─────────────

#[tokio::test]
async fn inert_and_model_owed_kinds_run_done_and_silent() {
    let mut rig = rig().await;
    for block_type in INERT_TYPES.into_iter().chain(AUTHORED_TYPES).chain([
        "tool_result",
        "tool_error",
        UNREGISTERED_TYPE,
    ]) {
        assert!(
            kind(block_type, Some(Role::User))
                .run(&rig.ctx)
                .await
                .unwrap(),
            "{block_type} must run trivially done"
        );
    }
    assert_silent(&mut rig.rx);
}

#[tokio::test]
async fn every_kind_defaults_gate_and_post_gate_inert() {
    let rig = rig().await;
    for block_type in INERT_TYPES.into_iter().chain(AUTHORED_TYPES).chain([
        "tool_call",
        "tool_result",
        "tool_error",
        UNREGISTERED_TYPE,
    ]) {
        let k = kind(block_type, Some(Role::User));
        assert_eq!(k.gate(&rig.ctx).await, GateDecision::Proceed);
        assert_eq!(k.post_gate_id(&[]), None);
        k.run_post_gate(&rig.ctx).await.unwrap();
    }
}

/// A stored type this build does not know is inert in every direction — the
/// fail-safe an old build reading a newer ledger depends on, and the place an
/// unregistered consumer kind lands today.
#[tokio::test]
async fn unknown_type_is_fully_inert() {
    let mut rig = rig().await;
    let k = kind(UNREGISTERED_TYPE, Some(Role::User));
    assert!(matches!(k, BlockKind::Unknown(_)));
    assert_eq!(k.awaiting(), None);
    assert!(k.run(&rig.ctx).await.unwrap());
    assert_silent(&mut rig.rx);
}

/// Every streaming kind is ephemeral, and only the streaming kinds are.
/// The cursor-anchor defect this pins was fixed for `streaming` and silently
/// still present on the other two until a verifier flipped their `durable()`
/// back and nothing went red — a regression on ANY deleted-row kind ships
/// silently unless all three are pinned together.
#[test]
fn exactly_the_streaming_kinds_are_ephemeral() {
    for stored in ["streaming", "streaming_thinking", "streaming_tool_call"] {
        assert!(
            !kind(stored, Some(Role::Assistant)).durable(),
            "{stored} must be ephemeral: its row is deleted at finalize, so a \
             cursor anchored on it dies with it"
        );
    }
    for stored in ["text", "thinking", "status", "tool_call", UNREGISTERED_TYPE] {
        assert!(
            kind(stored, Some(Role::Assistant)).durable(),
            "{stored} is a durable row and the cursor may anchor on it"
        );
    }
}

/// Exactly one stored kind says a conversation opens with it, over every
/// string this library claims. The dispatch refuses a ledger whose first row
/// answers `false`, so a second kind answering `true` would let a headless
/// ledger buy a turn, and the prompt answering `false` would refuse every
/// conversation there is.
#[test]
fn exactly_the_system_prompt_heads_the_ledger() {
    for stored in BlockKind::CLAIMED_KINDS
        .iter()
        .copied()
        .chain([UNREGISTERED_TYPE])
    {
        assert_eq!(
            kind(stored, None).heads_the_ledger(),
            stored == SystemPrompt::KINDS[0],
            "{stored} answers for itself whether a conversation opens with it"
        );
    }
}

// ─── tool_call run(): park, wakeup, settle ───────────────────────────────

#[tokio::test]
async fn unresolved_tool_call_parks_and_emits_the_wakeup() {
    let mut rig = rig().await;
    let block_id = insert_call(&rig, "call_1").await;
    let call = stored_kind(&rig, block_id).await;

    assert_eq!(call.awaiting(), Some(Awaiting::System));
    assert!(!call.run(&rig.ctx).await.unwrap());
    assert_wakeup(&mut rig.rx, rig.ctx.conversation_id, block_id);
    assert_silent(&mut rig.rx);
}

#[tokio::test]
async fn matching_result_settles_the_call_silently() {
    let mut rig = rig().await;
    let block_id = insert_call(&rig, "call_1").await;
    resolve_with_result(&rig, "call_1", block_id).await;

    let call = stored_kind(&rig, block_id).await;
    assert!(call.run(&rig.ctx).await.unwrap());
    assert_silent(&mut rig.rx);
}

#[tokio::test]
async fn matching_error_settles_the_call_silently() {
    let mut rig = rig().await;
    let block_id = insert_call(&rig, "call_1").await;
    rig.ctx
        .store
        .fail_tool_call_block(
            rig.ctx.conversation_id,
            "call_1".into(),
            "boom".into(),
            block_id,
        )
        .await
        .unwrap()
        .expect("the call is unresolved");

    let call = stored_kind(&rig, block_id).await;
    assert!(call.run(&rig.ctx).await.unwrap());
    assert_silent(&mut rig.rx);
}

#[tokio::test]
async fn unrelated_result_never_settles_a_call() {
    let mut rig = rig().await;
    let block_id = insert_call(&rig, "call_1").await;
    let second = insert_call(&rig, "call_2").await;
    resolve_with_result(&rig, "call_2", second).await;
    let third = insert_call(&rig, "call_3").await;
    rig.ctx
        .store
        .fail_tool_call_block(
            rig.ctx.conversation_id,
            "call_3".into(),
            "boom".into(),
            third,
        )
        .await
        .unwrap()
        .expect("the call is unresolved");

    let call = stored_kind(&rig, block_id).await;
    assert!(!call.run(&rig.ctx).await.unwrap());
    assert_wakeup(&mut rig.rx, rig.ctx.conversation_id, block_id);
}

/// A result already on the ledger BEFORE the call — an earlier call sharing
/// the same provider id, already resolved — must not settle the later call:
/// the result names the earlier call's own row, and the echo the two share
/// decides nothing.
#[tokio::test]
async fn a_result_preceding_the_call_does_not_settle_it() {
    let mut rig = rig().await;
    let earlier = insert_call(&rig, "call_1").await;
    resolve_with_result(&rig, "call_1", earlier).await;
    let block_id = insert_call(&rig, "call_1").await;

    let call = stored_kind(&rig, block_id).await;
    assert!(!call.run(&rig.ctx).await.unwrap());
    assert_wakeup(&mut rig.rx, rig.ctx.conversation_id, block_id);
}

/// AC18 — two calls of one round carrying ONE echo, through the real write
/// path: each is resolved by its own result and by nothing else.
///
/// This is the case the provider's echo cannot answer. Both results carry
/// `dup`, and both sit after both calls, so an echo comparison reads either
/// call as settled by the first result. The block id names one call, so the
/// first call's result leaves the second still owed one, and the second's
/// result answers only the second.
#[tokio::test]
async fn two_calls_sharing_one_echo_resolve_only_through_their_own_result() {
    let rig = rig().await;
    let first = insert_call(&rig, "dup").await;
    let second = insert_call(&rig, "dup").await;
    resolve_with_result(&rig, "dup", first).await;

    let ledger = rig
        .ctx
        .store
        .list_blocks(rig.ctx.conversation_id)
        .await
        .unwrap();
    let call_kind = |id: i64| match BlockKind::from_block(block_named(&ledger, id)) {
        BlockKind::ToolCall(call) => call,
        _ => panic!("expected a tool call"),
    };
    assert!(
        call_kind(first).resolved_in(&ledger),
        "the first call takes its own result"
    );
    assert!(
        !call_kind(second).resolved_in(&ledger),
        "and the shared echo does not settle the second call"
    );

    rig.ctx
        .store
        .fail_tool_call_block(rig.ctx.conversation_id, "dup".into(), "boom".into(), second)
        .await
        .unwrap()
        .expect("the second call is still owed an outcome");
    let ledger = rig
        .ctx
        .store
        .list_blocks(rig.ctx.conversation_id)
        .await
        .unwrap();
    assert!(matches!(
        call_kind(first).outcome_in(&ledger),
        Some(CallOutcome::Result(result)) if result.content == "ok"
    ));
    assert!(matches!(
        call_kind(second).outcome_in(&ledger),
        Some(CallOutcome::Error(error)) if error.error == "boom"
    ));
}

/// AC18 — the outcome reading, which is how a consumer tells a call that came
/// out from one that failed from one still in flight. Three states, one
/// reading, and no echo comparison anywhere in it.
#[tokio::test]
async fn the_outcome_reading_answers_result_error_and_pending() {
    let rig = rig().await;
    let pending = insert_call(&rig, "pending").await;
    let done = insert_call(&rig, "done").await;
    let broken = insert_call(&rig, "broken").await;
    resolve_with_result(&rig, "done", done).await;
    rig.ctx
        .store
        .fail_tool_call_block(
            rig.ctx.conversation_id,
            "broken".into(),
            "it broke".into(),
            broken,
        )
        .await
        .unwrap()
        .expect("the call is unresolved");

    let ledger = rig
        .ctx
        .store
        .list_blocks(rig.ctx.conversation_id)
        .await
        .unwrap();
    let outcome = |id: i64| {
        let BlockKind::ToolCall(call) = BlockKind::from_block(block_named(&ledger, id)) else {
            panic!("expected a tool call");
        };
        call.outcome_in(&ledger)
    };
    assert!(outcome(pending).is_none(), "nothing has answered it yet");
    assert!(matches!(outcome(done), Some(CallOutcome::Result(_))));
    assert!(matches!(outcome(broken), Some(CallOutcome::Error(_))));
}

#[tokio::test]
async fn interactive_call_runs_done_and_stays_silent() {
    let mut rig = rig().await;
    let mut block = bare_block("tool_call", Some(Role::Assistant));
    block.fields.insert(
        "tool_call_id".into(),
        serde_json::Value::String("call_1".into()),
    );
    block
        .fields
        .insert("interactive".into(), serde_json::Value::Bool(true));
    let call = BlockKind::from_block(&block);

    assert_eq!(call.awaiting(), Some(Awaiting::User));
    assert!(call.run(&rig.ctx).await.unwrap());
    assert_silent(&mut rig.rx);
}

/// The interactive stamp survives the store round trip through the REAL write
/// path, and the block loader surfaces it, so a call stamped at insert answers
/// with a user ask and does no system work on replay.
///
/// A hand-set block cannot catch this: it would pass just as well against a
/// loader that silently drops the column, or a write path that never sets it.
#[tokio::test]
async fn interactive_stamp_round_trips_through_the_store() {
    let mut rig = rig().await;
    let block_id = rig
        .ctx
        .store
        .insert_tool_call_block(
            rig.ctx.conversation_id,
            Role::Assistant,
            ToolCallInsert {
                tool_call_id: "call_1".into(),
                name: "ask_human".into(),
                input: "{}".into(),
                interactive: true,
            },
            None,
        )
        .await
        .unwrap();

    let call = stored_kind(&rig, block_id).await;
    assert_eq!(call.awaiting(), Some(Awaiting::User));
    assert!(
        call.run(&rig.ctx).await.unwrap(),
        "an interactive call is inert-true"
    );
    assert_silent(&mut rig.rx);
}

// ─── Approval kinds: their asks and the id-keyed routing traps ───────────

/// Both approval kinds await an out-of-band action — a human owes something
/// that is not a chat reply — and run trivially done, in silence. Out-of-band
/// is as far as this library sees: it has no interface of its own, and what
/// the human acts through is the consumer's business.
#[tokio::test]
async fn approval_blocks_await_an_out_of_band_action_and_run_inert() {
    let mut rig = rig().await;
    for block_type in ["approval_request", "approval_decision"] {
        let k = kind(block_type, Some(Role::User));
        assert_eq!(k.awaiting(), Some(Awaiting::OutOfBand));
        assert!(
            k.run(&rig.ctx).await.unwrap(),
            "{block_type} runs inert-true"
        );
    }
    assert_silent(&mut rig.rx);
}

/// A call, its approval request, and optionally a decision — all read back
/// through the store, so the routing answers from stored fields.
async fn approval_chain(rig: &Rig, decision: Option<ApprovalChoice>) -> (i64, i64, Option<i64>) {
    let call = insert_call(rig, "call_1").await;
    let request = rig
        .ctx
        .store
        .insert_approval_request_block(rig.ctx.conversation_id, call)
        .await
        .unwrap()
        .expect("the first request writes");
    let mut decision_id = None;
    if let Some(choice) = decision {
        decision_id = Some(
            rig.ctx
                .store
                .insert_approval_decision_block(
                    rig.ctx.conversation_id,
                    request,
                    choice,
                    None,
                    None,
                )
                .await
                .unwrap(),
        );
    }
    (call, request, decision_id)
}

async fn post_gate_of(rig: &Rig, block_id: i64) -> Option<i64> {
    let ledger = rig
        .ctx
        .store
        .list_blocks(rig.ctx.conversation_id)
        .await
        .unwrap();
    BlockKind::from_block(block_named(&ledger, block_id)).post_gate_id(&ledger)
}

/// An undecided request routes nowhere — the walk owes nothing until the human
/// speaks.
#[tokio::test]
async fn undecided_request_routes_nowhere() {
    let rig = rig().await;
    let (_call, request, _) = approval_chain(&rig, None).await;
    assert_eq!(post_gate_of(&rig, request).await, None);
}

/// Approved: the decision routes to its request, the request routes to the
/// call — while the call still owes its body.
#[tokio::test]
async fn approved_chain_routes_decision_to_request_to_call() {
    let rig = rig().await;
    let (call, request, decision) = approval_chain(&rig, Some(ApprovalChoice::Approved)).await;
    assert_eq!(post_gate_of(&rig, decision.unwrap()).await, Some(request));
    assert_eq!(post_gate_of(&rig, request).await, Some(call));
}

/// Denied routes nowhere from either block.
#[tokio::test]
async fn denied_chain_routes_nowhere() {
    let rig = rig().await;
    let (_call, request, decision) = approval_chain(&rig, Some(ApprovalChoice::Denied)).await;
    assert_eq!(post_gate_of(&rig, decision.unwrap()).await, None);
    assert_eq!(post_gate_of(&rig, request).await, None);
}

/// The call's OWN result closes the approved route — that IS the walk's
/// idempotency guard.
#[tokio::test]
async fn resolved_call_closes_the_approved_route() {
    let rig = rig().await;
    let (call, request, decision) = approval_chain(&rig, Some(ApprovalChoice::Approved)).await;
    resolve_with_result(&rig, "call_1", call).await;
    assert_eq!(post_gate_of(&rig, decision.unwrap()).await, None);
    assert_eq!(post_gate_of(&rig, request).await, None);
}

/// The by-kind regression trap: an UNRELATED result must not close the approval
/// route — routing is keyed on the specific originating call.
#[tokio::test]
async fn unrelated_result_does_not_close_the_approval_route() {
    let rig = rig().await;
    let (call, request, decision) = approval_chain(&rig, Some(ApprovalChoice::Approved)).await;
    let other = insert_call(&rig, "call_2").await;
    resolve_with_result(&rig, "call_2", other).await;
    assert_eq!(post_gate_of(&rig, decision.unwrap()).await, Some(request));
    assert_eq!(post_gate_of(&rig, request).await, Some(call));
}

/// The call's deferred body IS its own `run()`: `run_post_gate` emits the same
/// wakeup while unresolved and settles silently once the result lands.
#[tokio::test]
async fn tool_call_run_post_gate_is_its_own_run() {
    let mut rig = rig().await;
    let block_id = insert_call(&rig, "call_1").await;
    let call = stored_kind(&rig, block_id).await;

    call.run_post_gate(&rig.ctx).await.unwrap();
    assert_wakeup(&mut rig.rx, rig.ctx.conversation_id, block_id);

    resolve_with_result(&rig, "call_1", block_id).await;
    call.run_post_gate(&rig.ctx).await.unwrap();
    assert_silent(&mut rig.rx);
}

#[tokio::test]
async fn re_driving_an_unresolved_call_re_emits_until_the_result_lands() {
    let mut rig = rig().await;
    let block_id = insert_call(&rig, "call_1").await;
    let call = stored_kind(&rig, block_id).await;

    for _ in 0..3 {
        assert!(!call.run(&rig.ctx).await.unwrap());
    }
    for _ in 0..3 {
        assert_wakeup(&mut rig.rx, rig.ctx.conversation_id, block_id);
    }
    assert_silent(&mut rig.rx);

    resolve_with_result(&rig, "call_1", block_id).await;
    assert!(call.run(&rig.ctx).await.unwrap());
    assert_silent(&mut rig.rx);
}
