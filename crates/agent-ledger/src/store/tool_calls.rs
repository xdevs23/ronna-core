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

/// How a call settled outside the runner, for
/// [`Store::resolve_tool_call`].
///
/// One type with two arms, because a backing system reporting back has one
/// thing to say — which way the work came out — and the call it closes is the
/// same call either way. The arm decides which of the two resolution writes
/// runs; everything else about the write is identical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallResolution {
    /// The work finished. The text is the call's result, as the model reads it.
    Completed(String),
    /// The work was attempted and went wrong. The text is the reason, as the
    /// model reads it.
    Failed(String),
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
    /// provider id resolve independently. A backing system resolving deferred
    /// work calls this with the call block id it kept and the provider's id it
    /// echoes; one holding the block id alone takes
    /// [`resolve_tool_call`](Self::resolve_tool_call), which reads that echo
    /// off the call row and reaches the same write.
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
    /// keyed the same way, and
    /// [`resolve_tool_call`](Self::resolve_tool_call) is its counterpart for a
    /// caller holding the block id alone.
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

    /// Read the provider's id off a recorded call, so a resolution never has
    /// to be handed the echo it must carry. Answers `Other` when the block is
    /// no tool call of this conversation, which is the only way the id can be
    /// missing.
    async fn provider_id_of_call(
        &self,
        conversation_id: i64,
        call_block_id: i64,
    ) -> Result<String, StoreError> {
        self.run(move |conn| {
            conn.query_row(
                "SELECT btc.tool_call_id FROM block_tool_call btc
                 JOIN conversation_blocks cb ON cb.block_id = btc.block_id
                 WHERE btc.block_id = ?1 AND cb.conversation_id = ?2",
                params![call_block_id, conversation_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::Other(format!(
                    "block {call_block_id} is no tool call of conversation {conversation_id}"
                ))
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
    /// The resolution carries the same two facts the runner's own resolution
    /// carries, through those same writes:
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
    /// If `call_block_id` is no tool call of this conversation, if the write
    /// fails, or if the store's actor has stopped.
    pub async fn resolve_tool_call(
        &self,
        conversation_id: i64,
        call_block_id: i64,
        resolution: CallResolution,
    ) -> Result<Option<i64>, StoreError> {
        let tool_call_id = self
            .provider_id_of_call(conversation_id, call_block_id)
            .await?;
        match resolution {
            CallResolution::Completed(result) => {
                self.complete_tool_call_block_stamped(
                    conversation_id,
                    tool_call_id,
                    result,
                    call_block_id,
                    false,
                )
                .await
            }
            CallResolution::Failed(error) => {
                self.fail_tool_call_block_marked(
                    conversation_id,
                    tool_call_id,
                    error,
                    call_block_id,
                    Refusal::Failed,
                )
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
    use crate::block::{Role, ToolCallResult};
    use crate::store::{BlockDestination, CallResolution, Store, ToolCallInsert};

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
            .resolve_tool_call(conv, settled, CallResolution::Completed("posted".into()))
            .await
            .unwrap()
            .expect("a pending call takes its result");
        let error = store
            .resolve_tool_call(
                conv,
                broken,
                CallResolution::Failed("the send failed".into()),
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
            .resolve_tool_call(conv, call, CallResolution::Completed("first".into()))
            .await
            .unwrap()
            .expect("the first resolution writes");
        let before = store.list_blocks(conv).await.unwrap().len();

        assert!(
            store
                .resolve_tool_call(conv, call, CallResolution::Completed("second".into()))
                .await
                .unwrap()
                .is_none(),
            "a second result is a no-op"
        );
        assert!(
            store
                .resolve_tool_call(conv, call, CallResolution::Failed("late".into()))
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

    /// The door refuses a block that is no call of this conversation — a block
    /// of another kind, a call belonging to another ledger, an id that names
    /// nothing. Each would otherwise write an outcome under an echo the store
    /// could not read.
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
            (prose + 10_000, "an id that names nothing"),
        ] {
            let refused = store
                .resolve_tool_call(conv, block, CallResolution::Completed("done".into()))
                .await;
            assert!(
                matches!(refused, Err(crate::store::StoreError::Other(ref reason))
                    if reason.contains("is no tool call")),
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
