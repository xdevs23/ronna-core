//! Call records: resolving a tool call with its result or its error, and
//! reading one back by the row id the change hook reported.

use rusqlite::{Connection, OptionalExtension, params};

use crate::agency::Refusal;
use crate::block::ToolCallResult;

use super::messages::{BlockDestination, anchor_of, insert_block};
use super::{Store, StoreError, transact};

/// Whether a result or an error for this call is already recorded in this
/// conversation — the store-side face of the resolution question, keyed on the
/// CALL BLOCK ID through `source_block_id`: a model's `tool_call_id` can
/// repeat, the block id cannot, so a sibling call sharing the provider's id
/// never answers for this one. Read off the local ledger only, so a
/// junction-shared call resolves per conversation.
///
/// This is what the conditional resolution writes below and the denial path in
/// [`Store::insert_approval_decision_block`] all consult — one implementation,
/// so the three cannot drift.
pub(super) fn call_resolution_exists(
    conn: &Connection,
    conversation_id: i64,
    call_block_id: i64,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM block_tool_result btr
             JOIN conversation_blocks cb ON cb.block_id = btr.block_id
             WHERE btr.source_block_id = ?1 AND cb.conversation_id = ?2)
             OR EXISTS(
             SELECT 1 FROM block_tool_error bte
             JOIN conversation_blocks cb ON cb.block_id = bte.block_id
             WHERE bte.source_block_id = ?1 AND cb.conversation_id = ?2)",
        params![call_block_id, conversation_id],
        |row| row.get(0),
    )
}

/// The provider's `tool_call_id` on a recorded call, answering the
/// conversation-scoped question "is this block a tool call of this
/// conversation" along the way: the id when it is one,
/// [`StoreError::NoSuchToolCall`] when it is not. Joined through the junction,
/// so a fork answers off its own ledger.
///
/// The join and the error live here once. [`Store::resolve_tool_call`]
/// needs the id, [`Store::insert_approval_request_block`] needs only that the
/// block is a call and discards the id, and neither carries a copy of the
/// question that could drift from this one.
pub(super) fn provider_id_of_call(
    conn: &Connection,
    conversation_id: i64,
    call_block_id: i64,
) -> Result<String, StoreError> {
    conn.query_row(
        "SELECT btc.tool_call_id FROM block_tool_call btc
         JOIN conversation_blocks cb ON cb.block_id = btc.block_id
         WHERE btc.block_id = ?1 AND cb.conversation_id = ?2",
        params![call_block_id, conversation_id],
        |row| row.get::<_, String>(0),
    )
    .optional()?
    .ok_or(StoreError::NoSuchToolCall {
        block_id: call_block_id,
        conversation_id,
    })
}

/// The same question as a predicate, for a caller that needs only that the
/// block is a call of this conversation and has no use for the echo.
pub(super) fn ensure_call_of_conversation(
    conn: &Connection,
    conversation_id: i64,
    call_block_id: i64,
) -> Result<(), StoreError> {
    provider_id_of_call(conn, conversation_id, call_block_id).map(|_| ())
}

impl Store {
    /// Insert a `tool_result` block for a completed tool call — conditionally,
    /// in its own transaction, the same shape as the decision write: the insert
    /// happens only where no result and no error for this call exist in this
    /// conversation yet, so one call can never carry two outcomes no matter how
    /// many wakeups raced to record one.
    ///
    /// Answers `Some(block_id)` when this write resolved the call and `None`
    /// when a resolution already existed — the caller lost the race on a stale
    /// read and should stand down, never retry.
    ///
    /// `source_block_id` is the block id of the `tool_call` block this answers,
    /// and it is the identity the condition is keyed on: a model's
    /// `tool_call_id` can repeat, the block id cannot, so two calls sharing one
    /// provider id resolve independently.
    ///
    /// **Which door to take.** A backing system settling a
    /// [`Pending`](crate::ToolOutcome::Pending) call takes
    /// [`resolve_tool_call`](Self::resolve_tool_call), the one documented door
    /// for that: it names the call by its block id alone and reads the
    /// provider's echo off the call row, so it cannot be handed a mismatched
    /// pair. This method stays for a caller that already holds a `tool_call_id`
    /// it did not read from this ledger and wants it written as given —
    /// rebuilding a ledger from an external record, say. It fixes the two facts
    /// a resolution outside the runner carries (unstamped, and a failure that
    /// is not a refusal), and `resolve_tool_call` reaches the write through
    /// here, so those facts are decided in one place.
    ///
    /// The result it writes is UNSTAMPED: it asks the model for its
    /// continuation, which is what every out-of-band resolution means. The
    /// turn-ending stamp ([`ToolHandler::ends_turn`](crate::ToolHandler::ends_turn))
    /// is decided where the handler is in hand — the runner's body pass — and
    /// reaches the ledger through the crate-private stamped variant this
    /// method delegates to, the one write implementation underneath both
    /// doors.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn complete_tool_call_block(
        &self,
        conversation_id: i64,
        tool_call_id: String,
        result: String,
        source_block_id: i64,
    ) -> Result<Option<i64>, StoreError> {
        self.complete_tool_call_block_stamped(
            conversation_id,
            tool_call_id,
            result,
            source_block_id,
            false,
        )
        .await
    }

    /// The resolution write itself, with the turn-ending stamp the caller
    /// decided (2026-08-30) — ONE implementation under every door that
    /// completes a call.
    ///
    /// `ends_turn` is [`ToolHandler::ends_turn`](crate::ToolHandler::ends_turn)
    /// as the handler answered it at execution time. A stamped resolution asks
    /// the model for nothing: it IS the stored record of the turn's end, read
    /// back off this row by every fold that walks the ledger, so a restart
    /// derives the same closure from the same block. Recorded at the one
    /// moment it is known, on the row that must answer for it — the
    /// `dispatch_anchor` precedent class.
    ///
    /// Crate-private on purpose: the public doors hold no handler and must
    /// never invent the stamp, and a public parameter would offer every caller
    /// a second place to decide what only the registry knows.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub(crate) async fn complete_tool_call_block_stamped(
        &self,
        conversation_id: i64,
        tool_call_id: String,
        result: String,
        source_block_id: i64,
        ends_turn: bool,
    ) -> Result<Option<i64>, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                if call_resolution_exists(tx, conversation_id, source_block_id)? {
                    return Ok(None);
                }
                // The result copies the call's dispatch anchor at the
                // resolution write — off the durable call row, so a
                // restart-recovered round is correct for free.
                let anchor = anchor_of(tx, source_block_id)?;
                let block_id = insert_block(tx, BlockDestination::anchored(conversation_id, anchor), "tool_result")?;
                tx.execute(
                    "INSERT INTO block_tool_result (block_id, tool_call_id, content, source_block_id, ends_turn) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![block_id, tool_call_id, result, source_block_id, i64::from(ends_turn)],
                )?;
                Ok(Some(block_id))
            })
        })
        .await
    }

    /// Insert a `tool_error` block for a failed tool call — conditional exactly
    /// like [`complete_tool_call_block`](Self::complete_tool_call_block), and
    /// with the same answer: `Some(block_id)` when this write resolved the
    /// call, `None` when a resolution already existed and nothing was written.
    ///
    /// `source_block_id` is the block id of the `tool_call` block this answers,
    /// keyed the same way, and the choice of door is the one
    /// [`complete_tool_call_block`](Self::complete_tool_call_block) states: a
    /// backing system settling a pending call takes
    /// [`resolve_tool_call`](Self::resolve_tool_call), which reaches this write
    /// with the echo read off the call row.
    ///
    /// The failure it writes is NOT a refusal: it records that something was
    /// attempted and went wrong, which is what a failure arriving from outside
    /// the runner means. A refusal — a call declined before it ran — reaches
    /// the ledger through the marked variant this method delegates to, because
    /// only the pass that made the decision knows it was one.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn fail_tool_call_block(
        &self,
        conversation_id: i64,
        tool_call_id: String,
        error: String,
        source_block_id: i64,
    ) -> Result<Option<i64>, StoreError> {
        self.fail_tool_call_block_marked(
            conversation_id,
            tool_call_id,
            error,
            source_block_id,
            Refusal::Failed,
        )
        .await
    }

    /// The failure write itself, with the refusal fact the caller decided
    /// (2026-09-01) — ONE implementation under every door that fails a call,
    /// the resolution path's own shape.
    ///
    /// [`Refusal::Refused`] says the model spent a round and was handed only
    /// the reason, which is what the forced turn end counts a run of. Recorded
    /// on the row at the one moment it is known — the pass that refused —
    /// so every reader answers from the row and no reader parses the sentence
    /// the model reads.
    ///
    /// Crate-private on purpose: a consumer's own decline arrives through
    /// [`ToolOutcome::Refused`](crate::ToolOutcome::Refused), the typed
    /// surface, and the runner sets the fact from it. A public parameter would
    /// be a second place to decide the same thing.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub(crate) async fn fail_tool_call_block_marked(
        &self,
        conversation_id: i64,
        tool_call_id: String,
        error: String,
        source_block_id: i64,
        refusal: Refusal,
    ) -> Result<Option<i64>, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                if call_resolution_exists(tx, conversation_id, source_block_id)? {
                    return Ok(None);
                }
                // Same copy-at-the-resolution-write as the result path.
                let anchor = anchor_of(tx, source_block_id)?;
                let block_id = insert_block(tx, BlockDestination::anchored(conversation_id, anchor), "tool_error")?;
                tx.execute(
                    "INSERT INTO block_tool_error (block_id, tool_call_id, error, source_block_id, refusal) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![block_id, tool_call_id, error, source_block_id, i64::from(refusal.is_refusal())],
                )?;
                Ok(Some(block_id))
            })
        })
        .await
    }

    /// Resolve a pending tool call from outside the runner, naming the call by
    /// its BLOCK id: the identity the ledger keys a resolution on, and the one
    /// a deferring body was handed
    /// ([`ToolContext::block_id`](crate::ToolContext::block_id)). This is the
    /// door a backing system settling a [`Pending`](crate::ToolOutcome::Pending)
    /// call takes, whichever way the work came out.
    ///
    /// The conversation scopes the call, because a fork shares its junction
    /// rows: one call block can hang in several conversations and each resolves
    /// on its own ledger.
    ///
    /// The provider's `tool_call_id` is READ off the call row, never supplied.
    /// It is an echo the model pairs its call with, the row already
    /// carries it, and a caller passing it could pass a different one — the
    /// result would then answer a call the model never made.
    ///
    /// Answers `Some(block_id)` when this write resolved the call and `None`
    /// when an outcome was already recorded: a settled call is left exactly as
    /// it stands and nothing is appended, so a repeated report is a no-op
    /// instead of a second outcome.
    ///
    /// The outcome is the same [`ToolCallResult`] the ledger reads back
    /// ([`lookup_tool_completion`](Self::lookup_tool_completion)): a resolver
    /// supplies exactly the text the model reads, on the arm it came out on,
    /// and there is one vocabulary for a call's outcome instead of two.
    ///
    /// The resolution carries the same two facts the runner's own resolution
    /// carries, and it carries them by DELEGATING to
    /// [`complete_tool_call_block`](Self::complete_tool_call_block) and
    /// [`fail_tool_call_block`](Self::fail_tool_call_block), which fix them:
    ///
    /// - the turn-ending stamp
    ///   ([`ToolHandler::ends_turn`](crate::ToolHandler::ends_turn)), written
    ///   unstamped. That is the handler's own answer for every call this door
    ///   can reach: a handler that ends the turn cannot defer — the runner
    ///   refuses a pending outcome from one — and cannot be interactive, so no
    ///   call settled here belongs to a handler that answers otherwise. The
    ///   stamp is not a caller's to supply; a parameter would be a second place
    ///   to decide what only the registry knows.
    /// - the refusal fact ([`Refusal`]), written [`Failed`](Refusal::Failed).
    ///   A failure arriving from outside the runner records that something was
    ///   attempted and went wrong. A decline before the work ran is a
    ///   [`Refused`](Refusal::Refused) outcome from the body itself, where the
    ///   runner sets the fact.
    ///
    /// # Errors
    ///
    /// [`StoreError::NoSuchToolCall`] if `call_block_id` is no tool call of
    /// this conversation; otherwise whatever the delegated write answers when
    /// it fails or the store's actor has stopped.
    pub async fn resolve_tool_call(
        &self,
        conversation_id: i64,
        call_block_id: i64,
        outcome: ToolCallResult,
    ) -> Result<Option<i64>, StoreError> {
        let tool_call_id = self
            .run(move |conn| provider_id_of_call(conn, conversation_id, call_block_id))
            .await?;
        match outcome {
            ToolCallResult::Success { content } => {
                self.complete_tool_call_block(conversation_id, tool_call_id, content, call_block_id)
                    .await
            }
            ToolCallResult::Error { error } => {
                self.fail_tool_call_block(conversation_id, tool_call_id, error, call_block_id)
                    .await
            }
        }
    }

    /// Look up a completed tool call by its block id — the row id the change
    /// hook reports. Answers with `(conversation_id, tool_call_id, result)` when
    /// the block is a result or an error, and `None` when it is neither.
    ///
    /// # Errors
    ///
    /// If a query fails or the store's actor has stopped.
    pub async fn lookup_tool_completion(
        &self,
        block_id: i64,
    ) -> Result<Option<(i64, String, ToolCallResult)>, StoreError> {
        self.run(move |conn| {
            // A result first, then an error: the two tables are disjoint, so at
            // most one answers.
            let mut stmt = conn.prepare(
                "SELECT cb.conversation_id, btr.tool_call_id, btr.content
                 FROM block_tool_result btr
                 JOIN conversation_blocks cb ON cb.block_id = btr.block_id
                 WHERE btr.block_id = ?1",
            )?;
            if let Some(row) = stmt
                .query_row(params![block_id], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .optional()?
            {
                return Ok(Some((
                    row.0,
                    row.1,
                    ToolCallResult::Success { content: row.2 },
                )));
            }

            let mut stmt = conn.prepare(
                "SELECT cb.conversation_id, bte.tool_call_id, bte.error
                 FROM block_tool_error bte
                 JOIN conversation_blocks cb ON cb.block_id = bte.block_id
                 WHERE bte.block_id = ?1",
            )?;
            if let Some(row) = stmt
                .query_row(params![block_id], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .optional()?
            {
                return Ok(Some((row.0, row.1, ToolCallResult::Error { error: row.2 })));
            }

            Ok(None)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use crate::block::{Role, ToolCallResult};
    use crate::store::{BlockDestination, Store, ToolCallInsert, transact};

    async fn call_block(store: &Store, conv: i64, tool_call_id: &str) -> i64 {
        store
            .insert_tool_call_block(
                conv,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: tool_call_id.into(),
                    name: "read".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap()
    }

    /// The completion lookup answers for both outcomes and stays silent on a
    /// block that is neither — the change hook reports every row id, so "not a
    /// completion" is the common case and must not be an error.
    #[tokio::test]
    async fn lookup_finds_a_result_an_error_and_nothing_else() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let call = call_block(&store, conv, "call_1").await;
        let second = call_block(&store, conv, "call_2").await;

        let result = store
            .complete_tool_call_block(conv, "call_1".into(), "the output".into(), call)
            .await
            .unwrap()
            .expect("the first resolution writes");
        let failure = store
            .fail_tool_call_block(conv, "call_2".into(), "it broke".into(), second)
            .await
            .unwrap()
            .expect("an unresolved call accepts its error");

        match store.lookup_tool_completion(result).await.unwrap() {
            Some((conversation, id, ToolCallResult::Success { content })) => {
                assert_eq!(conversation, conv);
                assert_eq!(id, "call_1");
                assert_eq!(content, "the output");
            }
            other => panic!("expected a success, got {other:?}"),
        }

        match store.lookup_tool_completion(failure).await.unwrap() {
            Some((_, id, ToolCallResult::Error { error })) => {
                assert_eq!(id, "call_2");
                assert_eq!(error, "it broke");
            }
            other => panic!("expected an error, got {other:?}"),
        }

        assert!(
            store.lookup_tool_completion(call).await.unwrap().is_none(),
            "a tool_call block is not a completion"
        );
    }

    /// The conditional resolution write: once a call carries an outcome, a
    /// second result AND a second error both answer `None` and append nothing —
    /// one call, one outcome, whichever writer raced in first.
    #[tokio::test]
    async fn a_resolved_call_refuses_every_further_outcome() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let call = call_block(&store, conv, "call_1").await;

        store
            .complete_tool_call_block(conv, "call_1".into(), "first".into(), call)
            .await
            .unwrap()
            .expect("the first resolution writes");
        let before = store.list_blocks(conv).await.unwrap().len();

        let second_result = store
            .complete_tool_call_block(conv, "call_1".into(), "second".into(), call)
            .await
            .unwrap();
        assert!(second_result.is_none(), "a second result is refused");
        let late_error = store
            .fail_tool_call_block(conv, "call_1".into(), "late failure".into(), call)
            .await
            .unwrap();
        assert!(late_error.is_none(), "an error after the result is refused");

        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(blocks.len(), before, "the losing writes append nothing");
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b.block_type == "tool_result")
                .count(),
            1
        );
        assert!(blocks.iter().all(|b| b.block_type != "tool_error"));
    }

    /// The copy-from-call writer class: a result and an error copy the
    /// dispatch anchor from their source call block AT the resolution write —
    /// off the durable row, so a restart-recovered round is correct with no
    /// in-memory state surviving to the write. An unanchored call's outcomes
    /// stay null.
    #[tokio::test]
    async fn resolutions_copy_the_dispatch_anchor_from_the_call_row() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let summoner = store
            .insert_text_block(conv, Role::User, "summon".into())
            .await
            .unwrap();
        let anchored_call = store
            .insert_tool_call_block(
                BlockDestination::anchored(conv, Some(summoner)),
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "c1".into(),
                    name: "read".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let bare_call = call_block(&store, conv, "c2").await;

        let result = store
            .complete_tool_call_block(conv, "c1".into(), "ok".into(), anchored_call)
            .await
            .unwrap()
            .unwrap();
        let error = store
            .fail_tool_call_block(conv, "c2".into(), "broke".into(), bare_call)
            .await
            .unwrap()
            .unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let anchor_of = |id: i64| blocks.iter().find(|b| b.id == id).unwrap().dispatch_anchor;
        assert_eq!(anchor_of(anchored_call), Some(summoner));
        assert_eq!(
            anchor_of(result),
            Some(summoner),
            "the result copies the call's anchor"
        );
        assert_eq!(
            anchor_of(error),
            None,
            "an unanchored call's outcome stays null"
        );
    }

    /// AC2 — the turn-ending stamp survives the walk and a RESTART: it is
    /// written once at the resolution, read back off the loaded block, and
    /// derived from that stored row alone by a process that never saw the
    /// handler. Path-backed, because the in-memory store cannot be reopened;
    /// the unstamped resolution beside it is what proves the read is the row's
    /// and not a constant.
    #[tokio::test]
    async fn the_ends_turn_stamp_is_stored_and_survives_a_reopen() {
        let dir = crate::store::temp_dir("ends-turn-stamp-restart");
        let db = dir.join("ledger.db");

        let conv = {
            let store = Store::open(&db).unwrap();
            let conv = store
                .create_conversation("p".into(), "m".into(), "m".into(), String::new())
                .await
                .unwrap();
            let ordinary = call_block(&store, conv, "ordinary").await;
            let parked = call_block(&store, conv, "parked").await;
            store
                .complete_tool_call_block_stamped(
                    conv,
                    "ordinary".into(),
                    "the output".into(),
                    ordinary,
                    false,
                )
                .await
                .unwrap()
                .expect("the ordinary resolution writes");
            store
                .complete_tool_call_block_stamped(
                    conv,
                    "parked".into(),
                    "nothing to do".into(),
                    parked,
                    true,
                )
                .await
                .unwrap()
                .expect("the stamped resolution writes");
            conv
        };

        let store = Store::open(&db).unwrap();
        let reopened = store.list_blocks(conv).await.unwrap();
        assert_eq!(
            crate::agency::results_with_stamps(&reopened),
            vec![
                ("the output".to_owned(), false),
                ("nothing to do".to_owned(), true)
            ],
            "each resolution answers for its own turn out of its own row"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The schema's identity invariant, exercised: a model's `tool_call_id` can
    /// repeat, the block id cannot. Two calls sharing one provider id resolve
    /// independently — the first call's result never refuses the second call's
    /// outcome, and each call still refuses its own second outcome.
    #[tokio::test]
    async fn calls_sharing_a_tool_call_id_resolve_independently() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let first = call_block(&store, conv, "dup").await;
        let second = call_block(&store, conv, "dup").await;

        store
            .complete_tool_call_block(conv, "dup".into(), "first outcome".into(), first)
            .await
            .unwrap()
            .expect("the first call resolves");
        store
            .complete_tool_call_block(conv, "dup".into(), "second outcome".into(), second)
            .await
            .unwrap()
            .expect("the second call resolves despite the shared provider id");

        assert!(
            store
                .complete_tool_call_block(conv, "dup".into(), "again".into(), first)
                .await
                .unwrap()
                .is_none(),
            "each call still refuses its own second outcome"
        );
        assert!(
            store
                .fail_tool_call_block(conv, "dup".into(), "late".into(), second)
                .await
                .unwrap()
                .is_none()
        );

        let results: Vec<_> = store
            .list_blocks(conv)
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.block_type == "tool_result")
            .collect();
        assert_eq!(results.len(), 2, "one result per call, two calls");
    }

    /// The out-of-band door (2026-09-02), both outcomes: named by the call's
    /// BLOCK id alone, it records the result or the error, echoes the
    /// provider's id it read off the call row, and carries the two facts the
    /// runner's own resolution carries — the turn-ending stamp, unstamped for
    /// a deferred call, and a failure that is not a refusal.
    #[tokio::test]
    async fn an_out_of_band_resolution_records_a_result_and_an_error() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let settled = call_block(&store, conv, "call_1").await;
        let broken = call_block(&store, conv, "call_2").await;

        let result = store
            .resolve_tool_call(
                conv,
                settled,
                ToolCallResult::Success {
                    content: "posted".into(),
                },
            )
            .await
            .unwrap()
            .expect("a pending call takes its result");
        let error = store
            .resolve_tool_call(
                conv,
                broken,
                ToolCallResult::Error {
                    error: "the send failed".into(),
                },
            )
            .await
            .unwrap()
            .expect("a pending call takes its error");

        match store.lookup_tool_completion(result).await.unwrap() {
            Some((conversation, id, ToolCallResult::Success { content })) => {
                assert_eq!(conversation, conv);
                assert_eq!(id, "call_1", "the echo comes off the call row");
                assert_eq!(content, "posted");
            }
            other => panic!("expected a success, got {other:?}"),
        }
        match store.lookup_tool_completion(error).await.unwrap() {
            Some((_, id, ToolCallResult::Error { error })) => {
                assert_eq!(id, "call_2", "the echo comes off the call row");
                assert_eq!(error, "the send failed");
            }
            other => panic!("expected an error, got {other:?}"),
        }

        let blocks = store.list_blocks(conv).await.unwrap();
        let flag = |id: i64, field: &str| {
            blocks
                .iter()
                .find(|b| b.id == id)
                .unwrap()
                .fields
                .get(field)
                .and_then(serde_json::Value::as_bool)
        };
        assert_eq!(
            flag(result, "ends_turn"),
            Some(false),
            "a deferred call's handler never ends the turn, and the row says so"
        );
        assert_eq!(
            flag(error, "refusal"),
            Some(false),
            "a failure from outside the runner is not a refusal"
        );
    }

    /// A call that already carries an outcome is left as it stands: either arm
    /// of the door answers `None` and appends nothing, so a backing system
    /// reporting the same settlement twice cannot write a second outcome.
    #[tokio::test]
    async fn a_settled_call_takes_no_second_out_of_band_resolution() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let call = call_block(&store, conv, "call_1").await;

        store
            .resolve_tool_call(
                conv,
                call,
                ToolCallResult::Success {
                    content: "first".into(),
                },
            )
            .await
            .unwrap()
            .expect("the first resolution writes");
        let before = store.list_blocks(conv).await.unwrap().len();

        assert!(
            store
                .resolve_tool_call(
                    conv,
                    call,
                    ToolCallResult::Success {
                        content: "second".into()
                    }
                )
                .await
                .unwrap()
                .is_none(),
            "a second result is a no-op"
        );
        assert!(
            store
                .resolve_tool_call(
                    conv,
                    call,
                    ToolCallResult::Error {
                        error: "late".into()
                    }
                )
                .await
                .unwrap()
                .is_none(),
            "a late failure is a no-op"
        );

        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(blocks.len(), before, "the losing reports append nothing");
        let result = blocks
            .iter()
            .find(|b| b.block_type == "tool_result")
            .expect("the one result stands");
        assert_eq!(
            result.fields.get("content").and_then(|v| v.as_str()),
            Some("first"),
            "the recorded outcome is the one that won"
        );
        assert!(blocks.iter().all(|b| b.block_type != "tool_error"));
    }

    /// AC18, the shape that cannot exist: a resolution row naming NO call is
    /// refused by the store itself, on both tables.
    ///
    /// The column has carried the call since it shipped and every writer fills
    /// it, so no stored resolution lacks one — and the schema says so rather
    /// than leaving it to the callers, which is why the kinds carry the id
    /// plainly and no reader branches on a row that answers no call. The write
    /// goes in raw here because no door in this file can produce the shape,
    /// and it goes in the ONE transaction every resolution door writes in: the
    /// `blocks` row and the kind row are one write, so the refusal takes both.
    ///
    /// What "nothing was recorded" is read with is the raw count over `blocks`,
    /// not a ledger reader. A reader joins through the kind tables and would
    /// walk straight past a `blocks` row the refusal left behind — the one
    /// residue this test exists to see.
    #[tokio::test]
    async fn a_resolution_naming_no_call_is_refused_by_the_store() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        call_block(&store, conv, "call_1").await;
        let before = block_rows(&store).await;

        for (kind, table, column) in [
            ("tool_result", "block_tool_result", "content"),
            ("tool_error", "block_tool_error", "error"),
        ] {
            let refused = store
                .run(move |conn| {
                    transact(conn, |tx| {
                        tx.execute("INSERT INTO blocks (block_type) VALUES (?1)", [kind])?;
                        let block_id = tx.last_insert_rowid();
                        tx.execute(
                            &format!(
                                "INSERT INTO {table} (block_id, tool_call_id, {column}, source_block_id)
                                 VALUES (?1, 'call_1', 'orphaned', NULL)"
                            ),
                            params![block_id],
                        )?;
                        Ok(())
                    })
                })
                .await;
            assert!(
                refused.is_err(),
                "a {kind} that names no call is refused, got {refused:?}"
            );
        }

        assert_eq!(
            block_rows(&store).await,
            before,
            "a refused resolution leaves no row anywhere, orphaned blocks included"
        );
    }

    /// Every row of `blocks`, counted raw — no join, no kind table, no
    /// conversation. A block nothing points at counts here and nowhere else.
    async fn block_rows(store: &Store) -> i64 {
        store
            .run(|conn| {
                conn.query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0))
                    .map_err(Into::into)
            })
            .await
            .unwrap()
    }

    /// The junction-shared call, resolved per ledger: a fork inherits the call
    /// block itself, so one pending call hangs in two conversations, and
    /// settling it in one leaves the other still owing an outcome. The
    /// resolution block is the fork's own — the source's ledger never sees it —
    /// and the source then takes its own, different outcome.
    #[tokio::test]
    async fn a_shared_call_resolves_once_per_conversation() {
        let store = Store::in_memory().unwrap();
        let source = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let call = call_block(&store, source, "shared").await;
        let fork = store
            .fork_conversation(source, call, crate::store::ModelOverride::default())
            .await
            .unwrap();
        assert!(
            store
                .list_blocks(fork)
                .await
                .unwrap()
                .iter()
                .any(|b| b.id == call),
            "the fork inherits the very call block, not a copy"
        );

        let in_fork = store
            .resolve_tool_call(
                fork,
                call,
                ToolCallResult::Success {
                    content: "settled in the fork".into(),
                },
            )
            .await
            .unwrap()
            .expect("the fork's ledger owes the outcome");

        assert!(
            store
                .list_blocks(source)
                .await
                .unwrap()
                .iter()
                .all(|b| b.id != in_fork),
            "the fork's resolution is the fork's alone"
        );
        let in_source = store
            .resolve_tool_call(
                source,
                call,
                ToolCallResult::Error {
                    error: "the send failed here".into(),
                },
            )
            .await
            .unwrap()
            .expect("the source still holds the call unresolved and takes its own outcome");

        let source_blocks = store.list_blocks(source).await.unwrap();
        assert!(
            source_blocks
                .iter()
                .any(|b| b.id == in_source && b.block_type == "tool_error"),
            "the source's own outcome stands in its own ledger"
        );
        assert!(
            source_blocks.iter().all(|b| b.block_type != "tool_result"),
            "and it is not the fork's result"
        );
        assert!(
            store
                .list_blocks(fork)
                .await
                .unwrap()
                .iter()
                .all(|b| b.id != in_source),
            "neither resolution crosses into the other ledger"
        );
    }

    /// The door refuses a block that is no call of this conversation — a block
    /// of another kind, a call belonging to another ledger, an id that names
    /// nothing. Each would otherwise write an outcome under an echo the store
    /// could not read. The refusal is the typed
    /// [`NoSuchToolCall`](crate::store::StoreError::NoSuchToolCall) naming both
    /// ids, so a caller reads the case off the variant and never off a
    /// sentence. The absent id is `i64::MAX`, the last id the column can hold:
    /// this ledger holds a handful of rows, so nothing carries it, and the test
    /// assumes nothing about how ids are handed out.
    #[tokio::test]
    async fn a_block_that_is_no_call_here_is_refused() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let elsewhere = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let prose = store
            .insert_text_block(conv, Role::Assistant, "notes".into())
            .await
            .unwrap();
        let foreign = call_block(&store, elsewhere, "call_1").await;

        for (block, what) in [
            (prose, "a text block"),
            (foreign, "a call of another conversation"),
            (i64::MAX, "an id that names nothing"),
        ] {
            let refused = store
                .resolve_tool_call(
                    conv,
                    block,
                    ToolCallResult::Success {
                        content: "done".into(),
                    },
                )
                .await;
            assert!(
                matches!(
                    refused,
                    Err(crate::store::StoreError::NoSuchToolCall {
                        block_id,
                        conversation_id,
                    }) if block_id == block && conversation_id == conv
                ),
                "{what} is refused, got {refused:?}"
            );
        }
        assert!(
            store
                .list_blocks(conv)
                .await
                .unwrap()
                .iter()
                .all(|b| b.block_type != "tool_result"),
            "a refused resolution appends nothing"
        );
    }
}
