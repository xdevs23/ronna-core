//! Compaction's storage primitives: where a ledger is cut in half, the
//! temporary conversation the first half is summarized in, and the new thread
//! that summary opens.
//!
//! Three operations, and none of them decides anything a consumer decides.
//! The instructions the harness appends, the records the temporary
//! conversation carries, the prompt the new thread opens under and the model
//! it runs on all arrive as parameters; this module owns only the ledger
//! arithmetic and the writes.
//!
//! # The cut
//!
//! [`Store::compaction_cut`] answers where a conversation splits, and it is
//! deterministic: the block at half the ledger BY BLOCK COUNT, resolved to
//! the message group containing it, whose LAST block ends the first half —
//! then extended forward while any tool call inside the first half has its
//! outcome beyond it, each extension landing on the last block of the group
//! holding the answering outcome.
//!
//! Two properties come out of that, and everything downstream rests on them:
//!
//! - **No group is ever split.** The cut always lands on a group's last
//!   block, so the second half always opens at a group start. A group
//!   straddling the half point lands whole in the summarized side.
//! - **No tool lifecycle is ever split.** A call whose outcome sits past the
//!   cut pulls the cut past that outcome, so the second half can never open
//!   on an orphaned result and the first half can never end on a call
//!   nothing answers within it.
//!
//! A conversation the cut leaves with no second half is not compactable: the
//! answer is `None`, and the caller does nothing. Same for a ledger too short
//! to have two halves.
//!
//! # The temporary conversation, and why the order of its appends is the
//! mechanism
//!
//! [`Store::fork_temporary`] forks the first half, records the caller's own
//! blocks on it, and appends the instructions LAST, as a
//! [`HarnessMessage`](crate::agency::HarnessMessage) in the system voice.
//! That order is not tidiness: that kind is the harness ASKING for a turn, so
//! appending it is what summons the turn, and everything the caller wants
//! that turn governed by has to be in the ledger before it lands. The
//! conversation boots latched like every other, so nothing runs until the
//! caller unlatches it.
//!
//! The ask is a KIND, never a voice, and the difference is load-bearing here:
//! the digest [`Store::open_compacted_thread`] writes is system-voiced prose
//! too, in a thread that serves a live channel, and it must never summon
//! anything. So the asking kind is the one with a single writer — this
//! module — and the digest stays an ordinary text block that states what the
//! earlier history held.
//!
//! # The new thread
//!
//! [`Store::open_compacted_thread`] writes, in exactly this order: the
//! thread's system prompt (configuration, if the caller has one), the
//! ancestor-reference block naming where the history came from, the
//! compaction message carrying the captured summary, and then the source's
//! junction rows from past the cut. Two separate appends for the reference
//! and the message, never one fused block — they say different things and
//! the ledger records them as the two facts they are.

use rusqlite::params;

use crate::agency::{BlockKind, FromBlock, HarnessMessage, LeafKind, Text};
use crate::block::{Block, Role};

use super::conversations::{
    confirm_inherited_history, copy_junction_after, insert_conversation,
    insert_system_prompt_block, resolve_model_for_fork, resolve_reasoning_for_fork, role_run,
};
use super::messages::insert_block;
use super::{ModelOverride, Store, StoreError, transact};

/// Where one conversation's ledger splits for a compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerCut {
    /// The LAST block of the first half — the side a compaction summarizes.
    /// Inclusive.
    pub first_half_ends: i64,
    /// The FIRST block of the second half — the side a compaction carries
    /// forward verbatim. Always a group's opening block.
    pub second_half_begins: i64,
}

/// One consumer-kind block a caller has recorded on a conversation the store
/// is building for it — the caller's own policy records, in the caller's own
/// kinds, which this module never inspects.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// The stored type string, claimed by one of the caller's descriptors.
    pub kind: &'static str,
    /// The block's voice, if its content table declares one.
    pub role: Option<Role>,
    /// The declared columns' values.
    pub fields: serde_json::Map<String, serde_json::Value>,
}

/// A temporary conversation, and the harness message whose turn produces its
/// answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporaryConversation {
    /// The conversation itself.
    pub conversation_id: i64,
    /// The instructions block — the LAST block the fork wrote. Everything
    /// past it in this conversation is that turn's product, which is what
    /// lets a caller tell the answer it asked for from the inherited history
    /// it asked about.
    pub instructions_block_id: i64,
}

/// What a temporary conversation is built with.
#[derive(Debug, Clone)]
pub struct TemporaryFork {
    /// Blocks recorded on the temporary conversation AHEAD of the
    /// instructions, so the turn the instructions summon is already governed
    /// by them.
    pub records: Vec<ConsumerRecord>,
    /// The harness's instructions, appended last, in the system voice — the
    /// append that summons the turn. The words are the caller's: this
    /// library has no prompts.
    pub instructions: String,
}

/// What a compacted thread opens with.
#[derive(Debug, Clone)]
pub struct CompactedThread {
    /// The conversation the thread's ancestor-reference block names. Usually
    /// the conversation the second half is copied from, but not necessarily:
    /// a scrubbed lineage points its reference at the scrubbed ancestor
    /// while inheriting from the thread being replaced.
    pub ancestor_conversation_id: i64,
    /// The thread's system prompt, appended ahead of everything else.
    /// `None` opens the thread without one. A prompt is a consumer's own
    /// words, so it arrives here exactly as it does at every other fork
    /// door.
    pub system_prompt: Option<String>,
    /// The compaction message's text — the captured summary of the
    /// summarized half. Appended in the system voice, as ordinary prose that
    /// STATES what the earlier history held: the harness is not the model
    /// recalling it, and it is not asking for anything either.
    pub compaction_message: String,
    /// The model the thread runs on, and its reasoning level.
    pub model: ModelOverride,
}

impl Store {
    /// Where this conversation's ledger splits for a compaction, or `None`
    /// when it does not split at all — a ledger under two blocks, or one
    /// whose cut reaches the end and leaves nothing to carry forward
    /// verbatim.
    ///
    /// The whole rule is on this module's own documentation; it is
    /// deterministic over the ledger, so two readers of one conversation
    /// answer identically.
    ///
    /// # Errors
    ///
    /// If reading the ledger fails or the store's actor has stopped.
    pub async fn compaction_cut(&self, source_id: i64) -> Result<Option<LedgerCut>, StoreError> {
        Ok(ledger_cut(&self.list_blocks(source_id).await?))
    }

    /// Fork the first half of a conversation into a TEMPORARY conversation,
    /// record the caller's own blocks on it, and append the caller's
    /// instructions as the closing harness message — the block whose append
    /// summons the turn that answers them.
    ///
    /// `up_to_block_id` is the inclusive end of the half being forked —
    /// [`LedgerCut::first_half_ends`] on the ordinary path, and the
    /// complement boundary on a regeneration. The fork shares its source's
    /// junction rows: nothing is copied and the source is untouched.
    ///
    /// The conversation comes back LATCHED, like every fresh conversation:
    /// the turn the instructions summon runs when the caller unlatches it,
    /// and not before. Retiring the temporary conversation once its answer
    /// has been read is the caller's, through
    /// [`delete_conversation`](Store::delete_conversation) — the first
    /// half's blocks all live on in the source, and only the two blocks this
    /// call and that turn appended are left for the collector.
    ///
    /// # Errors
    ///
    /// If the source or the block does not exist, if a record names a kind
    /// no descriptor claims, if a write fails, or if the store's actor has
    /// stopped.
    pub async fn fork_temporary(
        &self,
        source_id: i64,
        up_to_block_id: i64,
        fork: TemporaryFork,
    ) -> Result<TemporaryConversation, StoreError> {
        let conversation_id = self
            .fork_conversation(source_id, up_to_block_id, ModelOverride::default())
            .await?;
        for record in fork.records {
            self.append_consumer_block(
                conversation_id,
                record.role,
                record.kind,
                record.fields,
                None,
            )
            .await?;
        }
        // Last, and that is the mechanism: this is the block that owes the
        // turn, so nothing the caller wanted recorded first can land behind
        // it.
        let instructions_block_id = self
            .insert_harness_message(conversation_id, fork.instructions)
            .await?;
        Ok(TemporaryConversation {
            conversation_id,
            instructions_block_id,
        })
    }

    /// Open the thread a compaction hands the channel: the prompt, the
    /// ancestor reference, the compaction message, and the source's junction
    /// rows from past `after_block_id`.
    ///
    /// One transaction, so the scheduler never sees a half-built thread
    /// through the row change hook — and so the inherited tail, which may
    /// itself owe a turn, is never the frontier of a thread that has not got
    /// its digest yet.
    ///
    /// The thread has NO `parent_id`. It is not a fork of anything: it opens
    /// with a summary of a history it does not hold. Where it came from is
    /// the ancestor-reference block's own column, which is the fact the
    /// design asked for and the one that survives the ancestor's deletion.
    ///
    /// # Errors
    ///
    /// If the source or the block does not exist, if a write fails, or if
    /// the store's actor has stopped.
    pub async fn open_compacted_thread(
        &self,
        source_id: i64,
        after_block_id: i64,
        thread: CompactedThread,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            let model_id = resolve_model_for_fork(conn, source_id, &thread.model)?;
            let reasoning = resolve_reasoning_for_fork(conn, source_id, &thread.model)?;

            transact(conn, |tx| {
                let new_id = insert_conversation(tx, None, model_id, reasoning.as_deref())?;
                if let Some(prompt) = &thread.system_prompt {
                    insert_system_prompt_block(tx, new_id, prompt)?;
                }
                // The two appends the design orders, in that order and as two
                // blocks: where the history came from, then what it said.
                insert_ancestor_reference_block(tx, new_id, thread.ancestor_conversation_id)?;
                insert_compaction_message_block(tx, new_id, &thread.compaction_message)?;
                copy_junction_after(tx, source_id, new_id, after_block_id)?;
                // The inherited rows are confirmed exactly as far as the
                // source confirmed them, so the outbound edge is born
                // delivered over history it is inheriting rather than
                // re-announcing it.
                confirm_inherited_history(tx, source_id, new_id)?;
                Ok(new_id)
            })
        })
        .await
    }

    /// Append the harness's own message, in the system voice — the ONE
    /// writer of [`HarnessMessage`](crate::agency::HarnessMessage) anywhere,
    /// and private to this module so it stays the one writer.
    ///
    /// That privacy is the containment the kind exists for: appending this
    /// block IS how a model turn is summoned, so the door that summons a turn
    /// is [`fork_temporary`](Self::fork_temporary) and nothing else. A
    /// consumer says what the turn should do by handing its words in; it
    /// cannot append the ask beside prose of its own.
    async fn insert_harness_message(
        &self,
        conversation_id: i64,
        content: String,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, conversation_id, HarnessMessage::KINDS[0])?;
                // The prose table the text kinds share: what the block SAYS
                // is stored the same way, and what it MEANS is its kind's.
                tx.execute(
                    "INSERT INTO block_text (block_id, role, content) VALUES (?1, ?2, ?3)",
                    params![block_id, Role::System.as_str(), content],
                )?;
                Ok(block_id)
            })
        })
        .await
    }
}

/// Where a loaded ledger splits, the whole rule in one pure function.
fn ledger_cut(blocks: &[Block]) -> Option<LedgerCut> {
    if blocks.len() < 2 {
        return None;
    }
    // Half the ledger BY BLOCK COUNT, resolved to the group containing that
    // block; the first half ends at that group's LAST block, inclusive, so a
    // straddling group is summarized whole.
    let straddling = role_run(blocks, blocks.len() / 2);
    if let Some(cut) = cut_extended_from(blocks, *straddling.end()) {
        return Some(cut);
    }
    // The far side reached the end: one group runs from the half point to
    // the ledger's tail — a long unanswered run of one voice — and taking it
    // whole into the summarized half would leave nothing to carry forward.
    // The NEAR side of the same group is the fallback, and it is the other
    // reading of "take half the ledger" rather than a new rule: the group
    // lands whole in the half that rides across verbatim instead of whole in
    // the half that is summarized. A conversation with more than one group
    // therefore always splits, which is what keeps a ledger that has to be
    // compacted from being one that cannot be.
    cut_extended_from(blocks, straddling.start().checked_sub(1)?)
}

/// The cut ending at `end`, extended forward over every tool lifecycle that
/// would otherwise straddle it, or `None` when the extension leaves no
/// second half.
fn cut_extended_from(blocks: &[Block], mut end: usize) -> Option<LedgerCut> {
    // Every call's answer, located once: the walk below repeats, and asking
    // the ledger again per iteration would re-scan it per call per round.
    let answered: Vec<(usize, usize)> = blocks
        .iter()
        .enumerate()
        .filter_map(|(at, block)| match BlockKind::from_block(block) {
            BlockKind::ToolCall(call) => call.outcome_position_in(blocks).map(|to| (at, to)),
            _ => None,
        })
        .collect();

    // Extend forward, minimally, while a call inside the first half is
    // answered beyond it: to the last block of the group holding the nearest
    // such outcome, repeating until none is left. `end` strictly increases
    // and is bounded by the ledger, so this terminates.
    while let Some(&(_, outcome)) = answered
        .iter()
        .filter(|&&(call, outcome)| call <= end && outcome > end)
        .min_by_key(|&&(_, outcome)| outcome)
    {
        end = *role_run(blocks, outcome).end();
    }

    // The cut reached the end: everything is the summarized half and nothing
    // is left to carry forward verbatim, so this cut is not one.
    if end + 1 >= blocks.len() {
        return None;
    }
    Some(LedgerCut {
        first_half_ends: blocks[end].id,
        second_half_begins: blocks[end + 1].id,
    })
}

/// Append the block that records where a thread came from. Called inside the
/// thread's own transaction, so its header, junction and content rows are
/// already atomic.
fn insert_ancestor_reference_block(
    conn: &rusqlite::Connection,
    conversation_id: i64,
    ancestor_conversation_id: i64,
) -> Result<(), StoreError> {
    let id = insert_block(conn, conversation_id, "ancestor_reference")?;
    conn.execute(
        "INSERT INTO block_ancestor_reference (block_id, ancestor_conversation_id)
         VALUES (?1, ?2)",
        params![id, ancestor_conversation_id],
    )?;
    Ok(())
}

/// Append the compaction message — the captured summary, in the system
/// voice, because the harness is the one stating what the earlier history
/// held. Called inside the thread's own transaction.
///
/// An ordinary `text` block, and deliberately not the harness's asking kind:
/// this one lands in a thread that SERVES a channel, where the frontier reads
/// it like any other block. Prose that states asks nothing; the kind that
/// asks is written into the temporary conversation alone.
fn insert_compaction_message_block(
    conn: &rusqlite::Connection,
    conversation_id: i64,
    message: &str,
) -> Result<(), StoreError> {
    let id = insert_block(conn, conversation_id, Text::KINDS[0])?;
    conn.execute(
        "INSERT INTO block_text (block_id, role, content) VALUES (?1, ?2, ?3)",
        params![id, Role::System.as_str(), message],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// One block of a kind and a voice, its id its position in the fixture.
    fn block(id: i64, role: Option<Role>, kind: &str) -> Block {
        Block {
            id,
            role,
            block_type: kind.into(),
            created_at: String::new(),
            dispatch_anchor: None,
            fields: serde_json::Map::new(),
        }
    }

    /// A tool call under the given provider id, in the assistant's voice.
    fn call(id: i64, tool_call_id: &str) -> Block {
        let mut block = block(id, Some(Role::Assistant), "tool_call");
        block
            .fields
            .insert("tool_call_id".into(), json!(tool_call_id));
        block.fields.insert("name".into(), json!("read_file"));
        block.fields.insert("input".into(), json!("{}"));
        block
    }

    /// The result answering that call — role-less, like every outcome.
    fn result(id: i64, tool_call_id: &str) -> Block {
        let mut block = block(id, None, "tool_result");
        block
            .fields
            .insert("tool_call_id".into(), json!(tool_call_id));
        block.fields.insert("content".into(), json!("ok"));
        block
    }

    fn user(id: i64) -> Block {
        block(id, Some(Role::User), "text")
    }

    fn answer(id: i64) -> Block {
        block(id, Some(Role::Assistant), "text")
    }

    /// A ledger with nothing to split answers `None`: empty, and one block.
    #[test]
    fn a_ledger_too_short_to_have_two_halves_does_not_split() {
        assert_eq!(ledger_cut(&[]), None);
        assert_eq!(ledger_cut(&[user(1)]), None);
    }

    /// The plain shape: alternating voices, each its own group, cut at half
    /// the block count with the second half opening at the next group.
    #[test]
    fn the_cut_lands_at_half_the_block_count() {
        let blocks: Vec<Block> = (1..=8)
            .map(|id| if id % 2 == 1 { user(id) } else { answer(id) })
            .collect();
        assert_eq!(
            ledger_cut(&blocks),
            Some(LedgerCut {
                first_half_ends: 5,
                second_half_begins: 6,
            })
        );
    }

    /// A group straddling the half point lands WHOLE in the summarized half:
    /// the cut moves to that group's last block, never into it.
    #[test]
    fn a_group_straddling_the_half_point_is_summarized_whole() {
        // Six blocks; index 3 (id 4) is the middle, and ids 3, 4 and 5 are
        // one assistant-voiced run.
        let blocks = vec![
            user(1),
            user(2),
            answer(3),
            answer(4),
            answer(5),
            user(6),
            user(7),
        ];
        let cut = ledger_cut(&blocks).expect("the ledger splits");
        assert_eq!(
            cut.first_half_ends, 5,
            "the cut is the straddling group's LAST block"
        );
        assert_eq!(
            cut.second_half_begins, 6,
            "the second half opens at the next group's first block"
        );
    }

    /// A tool lifecycle crossing the half point pulls the cut past its
    /// outcome: the call is never left in the first half with its answer in
    /// the second.
    #[test]
    fn a_call_answered_past_the_cut_pulls_the_cut_past_its_outcome() {
        // Index 4 (id 5) is the middle: the call sits in the summarized
        // half and its result sits one group past it.
        let blocks = vec![
            user(1),
            user(2),
            user(3),
            answer(4),
            call(5, "c1"),
            result(6, "c1"),
            answer(7),
            user(8),
        ];
        let cut = ledger_cut(&blocks).expect("the ledger splits");
        assert_eq!(
            cut.first_half_ends, 6,
            "the cut extends over the answering outcome's group"
        );
        assert_eq!(cut.second_half_begins, 7);
    }

    /// The extension repeats: the group it lands in can itself hold a call
    /// answered further on, and the cut keeps moving until none is left.
    #[test]
    fn the_extension_repeats_until_no_call_is_answered_past_the_cut() {
        let blocks = vec![
            user(1),
            answer(2),
            call(3, "c1"),
            result(4, "c1"),
            call(5, "c2"),
            result(6, "c2"),
            answer(7),
            user(8),
        ];
        // Index 4 (id 5) is the middle: a role-less run holds ids 4 and 6
        // separately, and the second call drags the cut over its own result.
        let cut = ledger_cut(&blocks).expect("the ledger splits");
        assert_eq!(cut.first_half_ends, 6);
        assert_eq!(cut.second_half_begins, 7);
        assert!(
            !blocks[..6].iter().any(|block| {
                matches!(BlockKind::from_block(block), BlockKind::ToolCall(call)
                    if call.outcome_position_in(&blocks).is_some_and(|at| at >= 6))
            }),
            "no call inside the first half is answered outside it"
        );
    }

    /// A call the ledger never answers does not move the cut: there is no
    /// outcome beyond it to reach, and an unanswered call is a shape the
    /// summarized half may legitimately end on.
    #[test]
    fn an_unanswered_call_does_not_extend_the_cut() {
        // Index 3 (id 4) is the middle, inside the assistant-voiced run
        // that holds the dangling call; nothing answers it anywhere, so the
        // cut rests on that run's last block.
        let blocks = vec![
            user(1),
            answer(2),
            call(3, "c1"),
            answer(4),
            user(5),
            user(6),
        ];
        let cut = ledger_cut(&blocks).expect("the ledger splits");
        assert_eq!(cut.first_half_ends, 4);
        assert_eq!(cut.second_half_begins, 5);
    }

    /// An extension that reaches the ledger's end falls back to the NEAR
    /// side of the straddling group: the group rides across verbatim instead
    /// of being summarized, so a ledger with more than one group always
    /// splits.
    #[test]
    fn a_cut_that_reaches_the_end_falls_back_to_the_near_side() {
        let blocks = vec![user(1), answer(2), call(3, "c1"), user(4), result(5, "c1")];
        let cut = ledger_cut(&blocks).expect("the near side splits it");
        assert_eq!(
            cut.first_half_ends, 1,
            "the cut lands before the group whose far side reached the end"
        );
        assert_eq!(
            cut.second_half_begins, 2,
            "that group opens the half carried forward verbatim"
        );
    }

    /// A long unanswered run of one voice at the tail — the shape that makes
    /// the far side reach the end — rides across whole instead of leaving
    /// nothing to compact. Without the near-side fallback the conversation
    /// that most needs compacting is the one that cannot be compacted.
    #[test]
    fn a_long_trailing_run_of_one_voice_rides_across_instead_of_blocking_the_cut() {
        let mut blocks = vec![block(1, Some(Role::System), "system_prompt"), answer(2)];
        for id in 3..=12 {
            blocks.push(user(id));
        }
        let cut = ledger_cut(&blocks).expect("the near side splits it");
        assert_eq!(
            cut.first_half_ends, 2,
            "the trailing run's near side is the cut"
        );
        assert_eq!(cut.second_half_begins, 3);
    }

    /// One group and nothing else does not split: there is no near side to
    /// fall back to, so the answer is that the ledger does not split.
    #[test]
    fn a_ledger_that_is_one_group_does_not_split() {
        let blocks: Vec<Block> = (1..=6).map(user).collect();
        assert_eq!(ledger_cut(&blocks), None);
    }
}
