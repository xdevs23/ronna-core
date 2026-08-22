//! Call records: resolving a tool call with its result or its error, and
//! reading one back by the row id the change hook reported.

use rusqlite::{Connection, OptionalExtension, params};

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
    /// provider id resolve independently. This is the ONLY door a result takes
    /// into the ledger — a backing system resolving deferred work calls it with
    /// the call block id it kept.
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
                    "INSERT INTO block_tool_result (block_id, tool_call_id, content, source_block_id) VALUES (?1, ?2, ?3, ?4)",
                    params![block_id, tool_call_id, result, source_block_id],
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
    /// keyed the same way, and this is the only door an error takes into the
    /// ledger.
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
        self.run(move |conn| {
            transact(conn, |tx| {
                if call_resolution_exists(tx, conversation_id, source_block_id)? {
                    return Ok(None);
                }
                // Same copy-at-the-resolution-write as the result path.
                let anchor = anchor_of(tx, source_block_id)?;
                let block_id = insert_block(tx, BlockDestination::anchored(conversation_id, anchor), "tool_error")?;
                tx.execute(
                    "INSERT INTO block_tool_error (block_id, tool_call_id, error, source_block_id) VALUES (?1, ?2, ?3, ?4)",
                    params![block_id, tool_call_id, error, source_block_id],
                )?;
                Ok(Some(block_id))
            })
        })
        .await
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
    use crate::store::{BlockDestination, Store, ToolCallInsert};

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
}
