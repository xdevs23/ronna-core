//! Call records: resolving a tool call with its result or its error, and
//! reading one back by the row id the change hook reported.

use rusqlite::{OptionalExtension, params};

use crate::block::ToolCallResult;

use super::messages::insert_block;
use super::{Store, StoreError, transact};

impl Store {
    /// Insert a `tool_result` block for a completed tool call.
    /// `source_block_id` is the block id of the `tool_call` block this answers.
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
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, conversation_id, "tool_result")?;
                tx.execute(
                    "INSERT INTO block_tool_result (block_id, tool_call_id, content, source_block_id) VALUES (?1, ?2, ?3, ?4)",
                    params![block_id, tool_call_id, result, source_block_id],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Insert a `tool_error` block for a failed tool call.
    /// `source_block_id` is the block id of the `tool_call` block this answers.
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
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, conversation_id, "tool_error")?;
                tx.execute(
                    "INSERT INTO block_tool_error (block_id, tool_call_id, error, source_block_id) VALUES (?1, ?2, ?3, ?4)",
                    params![block_id, tool_call_id, error, source_block_id],
                )?;
                Ok(block_id)
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
    use crate::store::{Store, ToolCallInsert};

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
        let call = store
            .insert_tool_call_block(
                conv,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call_1".into(),
                    name: "read".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();

        let result = store
            .complete_tool_call_block(conv, "call_1".into(), "the output".into(), call)
            .await
            .unwrap();
        let failure = store
            .fail_tool_call_block(conv, "call_2".into(), "it broke".into(), call)
            .await
            .unwrap();

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
}
