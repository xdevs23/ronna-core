//! The machinery, driven over real stored ledgers: the parallel-call stall
//! regression, cursor persistence ordering, frontier gate shapes, fork cursor
//! initialization, and the second ledger on the same generic drive.
//!
//! Every one of these goes through the store — a hand-built vector of blocks
//! would prove the drive against a ledger nothing ever wrote, and the two
//! layers would be free to disagree about ordering, about what a fork inherits
//! and about where a cursor may point.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::event::CoreEvent;
use crate::store::{Continuation, ModelOverride, StoreError};
use crate::types::InputBlock;

use super::AgencyCtx;
use super::ratchet::{self, ConversationLedger, LedgerSource, MetadataLedger, oracle::Oracle};

// ─── The parallel-call stall regression ─────────────────────────────────

/// Three parallel calls; results land staggered LATER-IDS-FIRST.
///
/// This is the flagship regression. Any backward or latest-oriented read of the
/// ledger green-lights a turn off the third call's result while the first still
/// dangles — the model then receives a tool use with no answer, forever. The
/// frontier gate cannot express that state, because it only ever reads the tail
/// once the cursor has drained to it.
#[tokio::test]
async fn staggered_later_results_never_fire_while_the_earliest_dangles() {
    let mut o = Oracle::new().await;
    let user = o.user_text("run three things").await;
    let c1 = o.call("c1").await;
    o.call("c2").await;
    o.call("c3").await;

    // All three dangle: parked ON the first call, cursor confirmed only up to
    // the user block, no turn owed.
    let outcome = o.drive().await;
    assert!(!outcome.owes_turn);
    assert!(outcome.parked);
    assert_eq!(
        outcome.cursor, user,
        "cursor parks at or before the dangling first call"
    );
    o.expect_wakeup(c1);
    o.expect_silence();

    // The third and second land — the tail now awaits the model, but the gate
    // must not fire: the first still dangles and the cursor never reaches the
    // tail. The parked re-drive re-emits the first call's wakeup, because the
    // ratchet IS the retry loop.
    o.result("c3").await;
    o.result("c2").await;
    let outcome = o.drive().await;
    assert!(
        !outcome.owes_turn,
        "a later sibling's result must not green-light the turn"
    );
    assert!(outcome.parked);
    assert_eq!(outcome.cursor, user);
    o.expect_wakeup(c1);
    o.expect_silence();

    // The final result lands, and exactly ONE turn is owed against all three.
    let r1 = o.result("c1").await;
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, r1);
    o.expect_silence();

    // The assistant's reply closes the gate again.
    o.assistant_text("done").await;
    let outcome = o.drive().await;
    assert!(!outcome.owes_turn);
    assert!(!outcome.parked);
}

#[tokio::test]
async fn in_order_resolution_first_result_alone_does_not_fire() {
    let mut o = Oracle::new().await;
    o.user_text("two calls").await;
    let c1 = o.call("c1").await;
    let c2 = o.call("c2").await;

    o.drive().await;
    o.expect_wakeup(c1);

    // The first result lands — the drive confirms the first call and parks on
    // the second. The result itself sits beyond the park, unreachable this
    // drive.
    o.result("c1").await;
    let outcome = o.drive().await;
    assert!(!outcome.owes_turn, "the first result alone must not fire");
    assert!(outcome.parked);
    assert_eq!(outcome.cursor, c1, "cursor confirmed the resolved call");
    o.expect_wakeup(c2);
    o.expect_silence();

    let r2 = o.result("c2").await;
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    assert_eq!(outcome.cursor, r2);
}

#[tokio::test]
async fn single_call_parks_then_fires_on_its_result() {
    let mut o = Oracle::new().await;
    o.user_text("one call").await;
    let c1 = o.call("c1").await;

    let outcome = o.drive().await;
    assert!(!outcome.owes_turn);
    assert!(outcome.parked);
    o.expect_wakeup(c1);

    let r1 = o.result("c1").await;
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, r1);
    o.expect_silence();
}

// ─── Cursor persistence ordering ─────────────────────────────────────────

/// The crash window: a block's `run()` persisted its effect but the cursor did
/// not advance. The inclusive re-drive re-runs idempotent, now-done blocks —
/// nothing duplicates and the cursor catches up.
#[tokio::test]
async fn crash_behind_a_persisted_block_re_drives_without_duplicating() {
    let mut o = Oracle::new().await;
    let user = o.user_text("hi").await;
    let c1 = o.call("c1").await;
    o.drive().await;
    o.expect_wakeup(c1);

    // The result lands out of band; the cursor is still behind it — exactly the
    // crash-window shape.
    let r1 = o.result("c1").await;
    assert_eq!(o.cursor().await, user);
    let before = o.ledger_ids().await;

    let outcome = o.drive().await;
    assert_eq!(o.ledger_ids().await, before, "re-drive appends nothing");
    assert_eq!(outcome.cursor, r1, "cursor caught up to the tail");
    assert!(outcome.owes_turn);
    o.expect_silence();
}

#[tokio::test]
async fn full_ledger_inclusive_redrive_repeated_changes_nothing() {
    let mut o = Oracle::new().await;
    o.user_text("hi").await;
    o.call("c1").await;
    o.result("c1").await;
    let tail = o.assistant_text("done").await;

    let mut outcomes = Vec::new();
    for _ in 0..3 {
        // Reset to 0 each sweep — the whole ledger re-drives inclusively.
        o.ctx
            .store
            .update_cursor(o.ctx.conversation_id, 0)
            .await
            .unwrap();
        let before = o.ledger_ids().await;
        let outcome = o.drive().await;
        assert_eq!(o.ledger_ids().await, before);
        outcomes.push(outcome);
    }
    assert!(outcomes.iter().all(|out| *out == outcomes[0]));
    assert_eq!(outcomes[0].cursor, tail);
    assert!(!outcomes[0].owes_turn, "an assistant tail rests");
    o.expect_silence();
}

/// A conversation written before the cursor existed carries 0 — the drive
/// re-derives the whole ledger from the start and lands on the correct
/// frontier.
#[tokio::test]
async fn legacy_cursor_zero_re_derives_cleanly() {
    let mut o = Oracle::new().await;
    o.user_text("hi").await;
    o.call("c1").await;
    o.result("c1").await;
    o.assistant_text("done").await;
    let user2 = o.user_text("and now?").await;

    assert_eq!(o.cursor().await, 0);
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, user2);
    o.expect_silence();
}

/// A stale anchor — its block gone from the ledger, as a cleaned-up streaming
/// partial would be — re-derives from the start instead of failing.
#[tokio::test]
async fn stale_anchor_re_derives_from_the_start() {
    let mut o = Oracle::new().await;
    let user = o.user_text("hi").await;

    o.ctx
        .store
        .update_cursor(o.ctx.conversation_id, 999_999)
        .await
        .unwrap();
    let outcome = o.drive().await;
    assert_eq!(outcome.cursor, user);
    assert!(outcome.owes_turn);
    o.expect_silence();
}

// ─── The anchor, the ephemeral tail, and what a re-drive costs ───────────

/// The conversation ledger, wrapped so a test can count what the drive writes.
///
/// The count is the whole point: "the re-drive touched only the tail" is not
/// observable from the outcome — the same cursor comes back either way — but
/// it IS observable in how many cursor writes it took to get there, and each
/// of those is a change event the whole reactive stack answers.
struct CountingLedger {
    writes: AtomicUsize,
}

impl CountingLedger {
    fn new() -> Self {
        Self {
            writes: AtomicUsize::new(0),
        }
    }

    fn writes(&self) -> usize {
        self.writes.load(Ordering::Relaxed)
    }
}

impl LedgerSource for CountingLedger {
    async fn list<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Vec<Block>, StoreError> {
        ConversationLedger.list(ctx).await
    }

    async fn cursor<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<i64, StoreError> {
        ConversationLedger.cursor(ctx).await
    }

    async fn persist_cursor<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
        confirmed_id: i64,
    ) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        ConversationLedger.persist_cursor(ctx, confirmed_id).await
    }
}

/// A history long enough that re-driving all of it is unmistakable in the
/// write count.
async fn long_history(o: &Oracle) -> i64 {
    let mut last = 0;
    for turn in 0..20 {
        o.user_text(&format!("ask {turn}")).await;
        last = o.assistant_text(&format!("answer {turn}")).await;
    }
    last
}

/// The cursor never comes to rest on an ephemeral row: a streaming tail is
/// confirmed, but the id that persists is the last DURABLE block.
#[tokio::test]
async fn the_cursor_never_anchors_on_a_streaming_row() {
    let o = Oracle::new().await;
    let durable_tail = long_history(&o).await;
    assert_eq!(o.drive().await.cursor, durable_tail);

    let streaming = o
        .ctx
        .store
        .insert_streaming_block(o.ctx.conversation_id, Role::Assistant)
        .await
        .unwrap();

    let outcome = o.drive().await;
    assert!(!outcome.parked, "an ephemeral tail parks nothing");
    assert_ne!(
        outcome.cursor, streaming,
        "the cursor must not anchor to a row its finalization will delete"
    );
    assert_eq!(outcome.cursor, durable_tail);
    assert_eq!(o.cursor().await, durable_tail);
}

/// One streaming finalize on a long ledger costs the tail, not the history.
///
/// The finalize deletes the streamed row and appends the committed one. The
/// next drive has exactly one new block to confirm, so it writes the cursor
/// exactly once — the whole ledger re-driving would write it once per block.
#[tokio::test]
async fn a_streaming_finalize_costs_one_cursor_write_not_the_ledger_length() {
    let o = Oracle::new().await;
    let durable_tail = long_history(&o).await;
    o.drive().await;

    let streaming = o
        .ctx
        .store
        .insert_streaming_block(o.ctx.conversation_id, Role::Assistant)
        .await
        .unwrap();
    o.drive().await;
    assert_eq!(o.cursor().await, durable_tail);

    let finalized = o
        .ctx
        .store
        .insert_final_text_block(
            o.ctx.conversation_id,
            Role::Assistant,
            "streamed and settled".into(),
            Some(streaming),
        )
        .await
        .unwrap();

    let counting = CountingLedger::new();
    let outcome = ratchet::drive_ledger(&o.ctx, &counting).await.unwrap();
    assert_eq!(outcome.cursor, finalized);
    assert_eq!(
        counting.writes(),
        1,
        "only the newly appended tail was confirmed"
    );
}

/// A cursor left ON a row that no longer exists — what a build without the
/// durability rule persisted onto every streaming tail — anchors on the block
/// that followed it. The re-drive costs the tail after the vanished row, never
/// the whole history.
#[tokio::test]
async fn a_vanished_anchor_re_drives_only_the_tail_after_it() {
    let o = Oracle::new().await;
    long_history(&o).await;
    o.drive().await;

    let streaming = o
        .ctx
        .store
        .insert_streaming_block(o.ctx.conversation_id, Role::Assistant)
        .await
        .unwrap();
    o.ctx
        .store
        .update_cursor(o.ctx.conversation_id, streaming)
        .await
        .unwrap();
    let finalized = o
        .ctx
        .store
        .insert_final_text_block(
            o.ctx.conversation_id,
            Role::Assistant,
            "streamed and settled".into(),
            Some(streaming),
        )
        .await
        .unwrap();

    let counting = CountingLedger::new();
    let outcome = ratchet::drive_ledger(&o.ctx, &counting).await.unwrap();
    assert_eq!(outcome.cursor, finalized, "the drive caught up to the tail");
    assert_eq!(
        counting.writes(),
        1,
        "the vanished anchor cost the tail after it, not the history"
    );
}

// ─── Frontier gate shapes ────────────────────────────────────────────────

#[tokio::test]
async fn user_text_tail_fires() {
    let o = Oracle::new().await;
    let user = o.user_text("hello").await;
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, user);
}

/// The date marker is agency-inert: inserted by the REAL user-append
/// transaction BEFORE the message, the ratchet sails past it in silence — the
/// cursor drains to the user tail and the turn fires exactly as it would
/// without it.
#[tokio::test]
async fn the_ratchet_sails_past_a_date_marker() {
    let mut o = Oracle::new().await;
    let ids = o
        .ctx
        .store
        .insert_user_blocks(
            o.ctx.conversation_id,
            vec![InputBlock::Text {
                content: "hi".into(),
            }],
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
        ledger[0].block_type, "date_marker",
        "the marker precedes the message"
    );

    let outcome = o.drive().await;
    assert!(outcome.owes_turn, "the marker never masks the owed turn");
    assert!(!outcome.parked, "…and never parks the cursor");
    assert_eq!(
        outcome.cursor, ids[0],
        "the cursor drained past the marker to the tail"
    );
    o.expect_silence();
}

#[tokio::test]
async fn assistant_text_tail_rests() {
    let o = Oracle::new().await;
    o.user_text("hello").await;
    let tail = o.assistant_text("hi").await;
    let outcome = o.drive().await;
    assert!(!outcome.owes_turn);
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, tail);
}

#[tokio::test]
async fn interrupted_status_tail_caps_the_frontier() {
    let o = Oracle::new().await;
    o.user_text("hello").await;
    let tail = o.status("interrupted").await;
    let outcome = o.drive().await;
    assert!(!outcome.owes_turn, "no turn fires past a status cap");
    assert!(!outcome.parked, "the cap is cursor-inert — nothing is owed");
    assert_eq!(outcome.cursor, tail);
}

/// The conversation ledger, with an interrupt committed the moment the drive
/// goes back for the tail.
///
/// The drive reads the ledger once to walk it and once more to decide the
/// frontier; this appends the status cap between the two, which is the shape
/// of a real interrupt landing while the hooks run.
struct InterruptingLedger {
    reads: AtomicUsize,
}

impl LedgerSource for InterruptingLedger {
    async fn list<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Vec<Block>, StoreError> {
        if self.reads.fetch_add(1, Ordering::Relaxed) == 1 {
            ctx.store
                .insert_status_block(ctx.conversation_id, "interrupted".into(), None)
                .await?;
        }
        ConversationLedger.list(ctx).await
    }

    async fn cursor<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<i64, StoreError> {
        ConversationLedger.cursor(ctx).await
    }

    async fn persist_cursor<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
        confirmed_id: i64,
    ) -> Result<(), StoreError> {
        ConversationLedger.persist_cursor(ctx, confirmed_id).await
    }
}

/// A cap committed while the drive is in flight caps the frontier.
///
/// Deciding the frontier off the snapshot the drive opened with would fire a
/// turn straight past an interrupt that was already committed — the exact case
/// the status kind exists to prevent. The decision is made on a FRESH read of
/// the tail, so the cap is seen.
#[tokio::test]
async fn a_cap_committed_during_the_drive_caps_the_frontier() {
    let o = Oracle::new().await;
    let user = o.user_text("hello").await;

    let source = InterruptingLedger {
        reads: AtomicUsize::new(0),
    };
    let outcome = ratchet::drive_ledger(&o.ctx, &source).await.unwrap();

    assert!(
        !outcome.owes_turn,
        "no turn fires past a cap the drive had not seen when it started"
    );
    assert_eq!(outcome.awaiting, None, "the frontier reads the fresh tail");
    assert!(!outcome.parked, "parked stays the drive's own answer");
    assert_eq!(outcome.cursor, user, "the cap itself is not yet confirmed");

    let ledger = o.ledger_ids().await;
    assert_eq!(ledger.len(), 2, "the cap really did land mid-drive");

    // The next drive confirms the cap, and it still owes nothing.
    let outcome = o.drive().await;
    assert!(!outcome.owes_turn);
    assert_eq!(outcome.cursor, *ledger.last().unwrap());
}

/// After an interrupt's status cap, a following user message re-opens the
/// gate — the cap parks nothing, it just is not model-owed.
#[tokio::test]
async fn user_message_after_a_status_cap_fires_again() {
    let o = Oracle::new().await;
    o.user_text("hello").await;
    o.status("interrupted").await;
    assert!(!o.drive().await.owes_turn);

    let user = o.user_text("continue").await;
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    assert_eq!(outcome.cursor, user);
}

/// An approval-request tail rests: an out-of-band ask parks the turn axis
/// without parking the cursor. The covered call here is already resolved, so
/// only the tail's own ask is in play.
#[tokio::test]
async fn approval_request_tail_rests() {
    let mut o = Oracle::new().await;
    o.user_text("go").await;
    let call = o.call("c1").await;
    o.result("c1").await;
    let request = o
        .ctx
        .store
        .insert_approval_request_block(o.ctx.conversation_id, call)
        .await
        .unwrap()
        .expect("the first request writes");

    let outcome = o.drive().await;
    assert!(
        !outcome.owes_turn,
        "a tail awaiting a human's out-of-band action never fires a turn"
    );
    assert!(!outcome.parked, "the request is cursor-inert");
    assert_eq!(outcome.cursor, request);
    o.expect_silence();
}

#[tokio::test]
async fn empty_ledger_rests() {
    let o = Oracle::new().await;
    let outcome = o.drive().await;
    assert_eq!(
        outcome,
        ratchet::Outcome {
            cursor: 0,
            owes_turn: false,
            parked: false,
            awaiting: None
        }
    );
}

/// The gate reads the tail ONLY once the cursor drains: a model-owed tail
/// behind a dangling call must not fire until the call resolves.
#[tokio::test]
async fn model_owed_tail_behind_a_dangling_call_waits_for_the_drain() {
    let mut o = Oracle::new().await;
    o.user_text("go").await;
    let c1 = o.call("c1").await;
    // A model-owed tail for an unrelated call: its own resolved pair, past
    // the park.
    o.call("other").await;
    o.result("other").await;

    let outcome = o.drive().await;
    assert!(
        !outcome.owes_turn,
        "the tail is model-owed but the cursor never reached it"
    );
    assert!(outcome.parked);
    o.expect_wakeup(c1);

    let r1 = o.result("c1").await;
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    assert_eq!(outcome.cursor, r1);
    o.expect_silence();
}

/// A mid-ledger model-owed block behind a LATER dangling call does not fire a
/// turn — and does not mask the later call's system work either.
#[tokio::test]
async fn mid_ledger_model_owed_block_behind_later_dangling_call_does_not_fire() {
    let mut o = Oracle::new().await;
    o.user_text("go").await;
    o.call("c1").await;
    let r1 = o.result("c1").await;
    let c2 = o.call("c2").await;

    let outcome = o.drive().await;
    assert!(!outcome.owes_turn);
    assert!(outcome.parked);
    assert_eq!(
        outcome.cursor, r1,
        "cursor confirmed the resolved pair, parked on the second call"
    );
    o.expect_wakeup(c2);
    o.expect_silence();
}

// ─── Forks ───────────────────────────────────────────────────────────────

/// A source with fully-driven history: user, resolved call, reply, user.
async fn driven_source() -> (Oracle, i64) {
    let mut o = Oracle::new().await;
    o.user_text("first").await;
    let c1 = o.call("c1").await;
    o.drive().await;
    o.expect_wakeup(c1);
    o.result("c1").await;
    o.drive().await;
    o.assistant_text("answer").await;
    let anchor = o
        .ctx
        .store
        .insert_user_blocks(
            o.ctx.conversation_id,
            vec![InputBlock::Text {
                content: "again".into(),
            }],
        )
        .await
        .unwrap()[0];
    let outcome = o.drive().await;
    assert!(outcome.owes_turn);
    o.expect_silence();
    (o, anchor)
}

#[tokio::test]
async fn rerun_fork_confirms_inherited_history_and_drives_only_the_tail() {
    let (source, anchor) = driven_source().await;
    let source_ledger = source.ledger_ids().await;
    let source_cursor = source.cursor().await;

    let fork_id = source
        .ctx
        .store
        .fork_continuation(
            source.ctx.conversation_id,
            anchor,
            Continuation::Rerun,
            ModelOverride::default(),
        )
        .await
        .unwrap();
    let mut fork = source.scoped_to(fork_id);

    // A rerun shares everything through the anchor group — the anchor IS the
    // last inherited block.
    assert_eq!(
        fork.cursor().await,
        anchor,
        "fork cursor is the last inherited block id"
    );

    let outcome = fork.drive().await;
    assert!(
        outcome.owes_turn,
        "the shared user tail re-fires on the fork"
    );
    assert_eq!(outcome.cursor, anchor);
    fork.expect_silence(); // the inherited resolved call is never re-driven

    assert_eq!(
        source.ledger_ids().await,
        source_ledger,
        "junction-shared blocks undisturbed"
    );
    assert_eq!(
        source.cursor().await,
        source_cursor,
        "source cursor undisturbed"
    );
}

#[tokio::test]
async fn edit_fork_confirms_inherited_history_and_drives_only_the_edit() {
    let (source, anchor) = driven_source().await;
    let source_ledger = source.ledger_ids().await;
    // The date marker before the anchor group.
    let last_inherited = source_ledger[source_ledger.len() - 2];

    let fork_id = source
        .ctx
        .store
        .fork_continuation(
            source.ctx.conversation_id,
            anchor,
            Continuation::Edit(vec![InputBlock::Text {
                content: "edited".into(),
            }]),
            ModelOverride::default(),
        )
        .await
        .unwrap();
    let mut fork = source.scoped_to(fork_id);

    assert_eq!(
        fork.cursor().await,
        last_inherited,
        "fork cursor is the last inherited block id"
    );

    let outcome = fork.drive().await;
    let fork_tail = *fork.ledger_ids().await.last().unwrap();
    assert!(outcome.owes_turn);
    assert_eq!(
        outcome.cursor, fork_tail,
        "the first drive touched only the edited tail"
    );
    fork.expect_silence();

    assert_eq!(
        source.ledger_ids().await,
        source_ledger,
        "junction-shared blocks undisturbed"
    );
}

#[tokio::test]
async fn new_thread_fork_confirms_nothing_and_derives_its_own_ledger() {
    let (source, anchor) = driven_source().await;
    let source_ledger = source.ledger_ids().await;

    let fork_id = source
        .ctx
        .store
        .fork_continuation(
            source.ctx.conversation_id,
            anchor,
            // The prompt is a parameter here, where the code this was extracted
            // from read a product constant. One is supplied so the fork's
            // ledger has the same shape it had there: a prompt block ahead of
            // the deep-copied group, agency-inert, which the drive sails past.
            Continuation::NewThread {
                system_prompt: Some("a prompt the consumer wrote".into()),
            },
            ModelOverride::default(),
        )
        .await
        .unwrap();
    let mut fork = source.scoped_to(fork_id);

    // Nothing is junction-inherited from the source — the prompt and the
    // deep-copied group are fork-authored and were never driven by anyone, so
    // nothing is confirmed: the cursor starts at 0.
    let fork_ledger = fork.ledger_ids().await;
    assert_eq!(
        fork.cursor().await,
        0,
        "no inherited history — nothing confirmed"
    );

    let outcome = fork.drive().await;
    assert!(outcome.owes_turn);
    assert_eq!(outcome.cursor, *fork_ledger.last().unwrap());
    fork.expect_silence();

    assert_eq!(
        source.ledger_ids().await,
        source_ledger,
        "source blocks undisturbed by the deep copy"
    );
}

/// The fork cursor CAP, on an interrupt-shaped source: a dangling unresolved
/// call, the interrupt's status cap, then a later user group.
///
/// The source confirmed only up to the block BEFORE the dangle, so the fork's
/// cursor must sit there too — never at the inherited tail, which would declare
/// the dangling call confirmed. That is the forbidden skip-ahead: an unpaired
/// tool use on the wire, forever. The fork's first drive re-drives the dangle,
/// and once a result lands it proceeds, while the source stays undisturbed.
#[tokio::test]
async fn fork_cursor_never_passes_what_the_source_confirmed() {
    let mut o = Oracle::new().await;
    let user = o.user_text("run it").await;
    let dangle = o.call("h-dangle").await;
    let outcome = o.drive().await;
    assert!(outcome.parked, "the source parks on the dangling call");
    assert_eq!(outcome.cursor, user);
    o.expect_wakeup(dangle);

    // The interrupt caps the frontier; the user appends and forks. The source
    // is latched in practice and is never driven again.
    o.status("interrupted").await;
    let anchor = o
        .ctx
        .store
        .insert_user_blocks(
            o.ctx.conversation_id,
            vec![InputBlock::Text {
                content: "continue".into(),
            }],
        )
        .await
        .unwrap()[0];
    let source_ledger = o.ledger_ids().await;

    let fork_id = o
        .ctx
        .store
        .fork_continuation(
            o.ctx.conversation_id,
            anchor,
            Continuation::Rerun,
            ModelOverride::default(),
        )
        .await
        .unwrap();
    let mut fork = o.scoped_to(fork_id);

    assert_eq!(
        fork.cursor().await,
        user,
        "capped at the source's confirmed cursor — at or before the dangling call, never the inherited tail"
    );

    // The heal path: the fork's first drive re-drives the dangle and re-emits.
    let outcome = fork.drive().await;
    assert!(outcome.parked);
    assert!(
        !outcome.owes_turn,
        "no turn while the inherited call dangles"
    );
    fork.expect_wakeup(dangle);

    // A result lands in the fork — it proceeds to the settled tail.
    let healed = fork.result("h-dangle").await;
    let outcome = fork.drive().await;
    assert!(
        outcome.owes_turn,
        "exactly one turn against the settled ledger"
    );
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, healed);
    fork.expect_silence();

    // The source is undisturbed: same ledger, same cursor, still dangling.
    assert_eq!(o.ledger_ids().await, source_ledger);
    assert_eq!(o.cursor().await, user);
}

/// Editing the very first group inherits nothing — the cursor stays 0 and the
/// first drive re-derives from the start over the edited blocks only.
#[tokio::test]
async fn edit_fork_at_the_first_group_leaves_cursor_zero() {
    let o = Oracle::new().await;
    let first = o
        .ctx
        .store
        .insert_user_blocks(
            o.ctx.conversation_id,
            vec![InputBlock::Text {
                content: "original".into(),
            }],
        )
        .await
        .unwrap()[0];
    o.assistant_text("reply").await;

    let fork_id = o
        .ctx
        .store
        .fork_continuation(
            o.ctx.conversation_id,
            first,
            Continuation::Edit(vec![InputBlock::Text {
                content: "edited".into(),
            }]),
            ModelOverride::default(),
        )
        .await
        .unwrap();
    let fork = o.scoped_to(fork_id);

    assert_eq!(
        fork.cursor().await,
        0,
        "nothing inherited — nothing confirmed"
    );
    let outcome = fork.drive().await;
    assert!(outcome.owes_turn);
    assert_eq!(outcome.cursor, *fork.ledger_ids().await.last().unwrap());
}

// ─── The second ledger, on the same generic drive ────────────────────────
//
// These drive the metadata ledger through `drive_ledger` itself — the same
// function the conversation ledger goes through, handed a different
// `LedgerSource` and nothing else. If the machinery ever grew a branch for
// either ledger, one of these would have to be rewritten to accommodate it.

async fn title_request(o: &Oracle) -> i64 {
    o.ctx
        .store
        .insert_metadata(o.ctx.conversation_id, "title_request", None, None)
        .await
        .unwrap()
}

async fn title_response(o: &Oracle) -> i64 {
    o.ctx
        .store
        .insert_metadata(
            o.ctx.conversation_id,
            "title_response",
            None,
            Some("A Title"),
        )
        .await
        .unwrap()
}

async fn drive_metadata(o: &Oracle) -> ratchet::Outcome {
    ratchet::drive_ledger(&o.ctx, &MetadataLedger)
        .await
        .unwrap()
}

async fn metadata_cursor(o: &Oracle) -> i64 {
    o.ctx
        .store
        .metadata_cursor(o.ctx.conversation_id)
        .await
        .unwrap()
}

#[track_caller]
fn expect_request_wakeup(o: &mut Oracle, request_id: i64) {
    match o
        .rx
        .try_recv()
        .expect("expected a MetadataRequestReady wakeup")
    {
        CoreEvent::MetadataRequestReady {
            conversation_id,
            request_id: id,
        } => {
            assert_eq!(conversation_id, o.ctx.conversation_id);
            assert_eq!(id, request_id);
        }
        other => panic!("expected MetadataRequestReady, got {other:?}"),
    }
}

#[tokio::test]
async fn title_request_parks_the_metadata_ratchet_until_its_response() {
    let mut o = Oracle::new().await;
    let request = title_request(&o).await;

    // Parked ON the request: the cursor stays before it, and every drive
    // re-emits the fulfillment wakeup — the ratchet IS the retry loop.
    for _ in 0..3 {
        let outcome = drive_metadata(&o).await;
        assert!(outcome.parked);
        assert!(!outcome.owes_turn);
        assert_eq!(outcome.cursor, 0, "the cursor stays before the request");
        expect_request_wakeup(&mut o, request);
        o.expect_silence();
    }
    assert_eq!(metadata_cursor(&o).await, 0);

    // The response lands: the cursor advances past BOTH rows, in silence.
    let response = title_response(&o).await;
    let outcome = drive_metadata(&o).await;
    assert!(!outcome.parked);
    assert!(
        !outcome.owes_turn,
        "the metadata frontier never owes a model turn"
    );
    assert_eq!(outcome.cursor, response);
    assert_eq!(metadata_cursor(&o).await, response);
    o.expect_silence();

    // Settled for good: further drives change nothing and emit nothing.
    let outcome = drive_metadata(&o).await;
    assert_eq!(outcome.cursor, response);
    o.expect_silence();
}

/// Two requests, ONE response: the response settles the first request only,
/// and the second still parks the ratchet.
///
/// The pairing is positional — the schema carries no reference from a response
/// to the request it answers. Settling on kind alone would let this single
/// response settle both, and the second request's derivation would be dropped
/// with nothing left to ask for it again.
#[tokio::test]
async fn one_response_settles_one_request_and_the_next_still_parks() {
    let mut o = Oracle::new().await;
    let first = title_request(&o).await;
    let second = title_request(&o).await;
    title_response(&o).await;

    let outcome = drive_metadata(&o).await;
    assert!(outcome.parked, "the second request is still outstanding");
    assert_eq!(
        outcome.cursor, first,
        "the response settled the first request, and only it"
    );
    expect_request_wakeup(&mut o, second);
    o.expect_silence();

    // Its own response settles it, and the ledger drains.
    let second_response = title_response(&o).await;
    let outcome = drive_metadata(&o).await;
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, second_response);
    o.expect_silence();
}

/// An unrecognized metadata type parses to the same inert fallback a
/// conversation block would — the cursor sails past it.
#[tokio::test]
async fn unknown_metadata_type_is_inert_and_confirmed() {
    let mut o = Oracle::new().await;
    let row = o
        .ctx
        .store
        .insert_metadata(o.ctx.conversation_id, "summary_request", None, None)
        .await
        .unwrap();

    let outcome = drive_metadata(&o).await;
    assert!(!outcome.parked);
    assert_eq!(outcome.cursor, row);
    o.expect_silence();
}

/// One machinery, two ledgers, two cursors — and neither drive touches the
/// other's cursor.
#[tokio::test]
async fn the_two_cursors_never_interact() {
    let mut o = Oracle::new().await;

    // Interleave appends to BOTH ledgers.
    o.user_text("hi").await;
    let request = title_request(&o).await;
    let reply = o.assistant_text("yo").await;

    // Driving conversation blocks moves only the conversation cursor.
    let outcome = o.drive().await;
    assert_eq!(outcome.cursor, reply);
    assert_eq!(
        metadata_cursor(&o).await,
        0,
        "the conversation drive never touches the metadata cursor"
    );
    o.expect_silence();

    // Driving metadata moves only the metadata cursor — here it parks.
    let outcome = drive_metadata(&o).await;
    assert!(outcome.parked);
    expect_request_wakeup(&mut o, request);
    assert_eq!(
        o.cursor().await,
        reply,
        "the metadata drive never touches the conversation cursor"
    );

    // More interleaving: the response lands, then another user message.
    let response = title_response(&o).await;
    let user2 = o.user_text("more").await;

    let outcome = drive_metadata(&o).await;
    assert_eq!(outcome.cursor, response);
    assert_eq!(
        o.cursor().await,
        reply,
        "conversation cursor still untouched"
    );

    let outcome = o.drive().await;
    assert_eq!(outcome.cursor, user2);
    assert_eq!(
        metadata_cursor(&o).await,
        response,
        "metadata cursor still untouched"
    );
    o.expect_silence();
}
