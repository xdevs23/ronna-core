//! The metadata tables: the second ledger, driven by the same machinery as the
//! block ledger and cursored separately.

use rusqlite::params;

use crate::block::Block;

use super::{Store, StoreError};

impl Store {
    /// A conversation's metadata rows in insertion order, surfaced as the same
    /// [`Block`] shape the behavior layer drives: `id` is the metadata row id,
    /// `block_type` is the row's `meta_type`, and the fields carry its content
    /// and source block.
    ///
    /// The type-string namespace question is settled HERE, at the read: the
    /// `meta_type` values are their own closed set that never collides with
    /// block type strings, so no discriminator is needed and the machinery
    /// stays a plain type-string to behavior map.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn list_metadata_blocks(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<Block>, StoreError> {
        self.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, meta_type, source_block_id, content, created_at
                 FROM metadata WHERE conversation_id = ?1 ORDER BY id",
            )?;
            let rows = stmt.query_map([conversation_id], |row| {
                let mut fields = serde_json::Map::new();
                if let Some(source) = row.get::<_, Option<i64>>(2)? {
                    fields.insert("source_block_id".into(), source.into());
                }
                if let Some(content) = row.get::<_, Option<String>>(3)? {
                    fields.insert("content".into(), content.into());
                }
                Ok(Block {
                    id: row.get(0)?,
                    role: None,
                    block_type: row.get(1)?,
                    created_at: row.get(4)?,
                    fields,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }

    /// How far the ratchet has CONFIRMED driving this conversation's METADATA
    /// ledger — the metadata side's own cursor. One machinery, two ledgers, two
    /// cursors that never interact. 0 means nothing confirmed: the drive
    /// re-derives from the start.
    ///
    /// # Errors
    ///
    /// If the conversation does not exist, or the store's actor has stopped.
    pub async fn metadata_cursor(&self, conversation_id: i64) -> Result<i64, StoreError> {
        self.run(move |conn| {
            conn.query_row(
                "SELECT last_processed_metadata_id FROM conversations WHERE id = ?1",
                [conversation_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .await
    }

    /// Advance the metadata cursor. Same single-field-write contract as the
    /// conversation cursor; metadata rows stay append-only.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn update_metadata_cursor(
        &self,
        conversation_id: i64,
        metadata_id: i64,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute(
                "UPDATE conversations SET last_processed_metadata_id = ?1 WHERE id = ?2",
                params![metadata_id, conversation_id],
            )?;
            Ok(())
        })
        .await
    }

    /// Append a metadata row to a conversation's second ledger.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn insert_metadata(
        &self,
        conversation_id: i64,
        meta_type: &str,
        source_block_id: Option<i64>,
        content: Option<&str>,
    ) -> Result<i64, StoreError> {
        let meta_type = meta_type.to_owned();
        let content = content.map(ToOwned::to_owned);
        self.run(move |conn| {
            conn.execute(
                "INSERT INTO metadata (conversation_id, meta_type, source_block_id, content)
                 VALUES (?1, ?2, ?3, ?4)",
                params![conversation_id, meta_type, source_block_id, content],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .await
    }

    /// Whether a metadata row of the given type exists for a conversation.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn has_metadata(
        &self,
        conversation_id: i64,
        meta_type: &str,
    ) -> Result<bool, StoreError> {
        let meta_type = meta_type.to_owned();
        self.run(move |conn| {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM metadata WHERE conversation_id = ?1 AND meta_type = ?2)",
                params![conversation_id, meta_type],
                |row| row.get(0),
            )?;
            Ok(exists)
        })
        .await
    }

    /// Whether a conversation carries any assistant-role blocks — whether at
    /// least one complete turn has happened. Role lives on the per-type content
    /// tables, not on `blocks`, so this probes every role-carrying table.
    ///
    /// Every one of them: the streaming tables count too. A streamed text tail
    /// and a streamed reasoning tail already answered here through `block_text`
    /// and `block_thinking`, which they share with their finalized blocks;
    /// `block_streaming_tool_call` has a table to itself and was the one
    /// role-carrying table the union left out.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn has_assistant_blocks(&self, conversation_id: i64) -> Result<bool, StoreError> {
        self.run(move |conn| {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM conversation_blocks cb
                    JOIN (
                        SELECT block_id, role FROM block_text
                        UNION ALL SELECT block_id, role FROM block_quote
                        UNION ALL SELECT block_id, role FROM block_code
                        UNION ALL SELECT block_id, role FROM block_thinking
                        UNION ALL SELECT block_id, role FROM block_tool_call
                        UNION ALL SELECT block_id, role FROM block_streaming_tool_call
                    ) r ON r.block_id = cb.block_id
                    WHERE cb.conversation_id = ?1 AND r.role = 'assistant'
                )",
                [conversation_id],
                |row| row.get(0),
            )?;
            Ok(exists)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::block::Role;
    use crate::store::Store;

    /// The second ledger reads back as blocks, in insertion order, and carries
    /// its own cursor — the block cursor stays where it was, because the two
    /// never interact.
    #[tokio::test]
    async fn the_metadata_ledger_reads_as_blocks_and_cursors_on_its_own() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();

        assert!(!store.has_metadata(conv, "title_request").await.unwrap());
        assert!(!store.has_assistant_blocks(conv).await.unwrap());

        let block = store
            .insert_text_block(conv, Role::Assistant, "an answer".into())
            .await
            .unwrap();
        assert!(store.has_assistant_blocks(conv).await.unwrap());

        let request = store
            .insert_metadata(conv, "title_request", Some(block), None)
            .await
            .unwrap();
        let response = store
            .insert_metadata(conv, "title_response", Some(block), Some("A title"))
            .await
            .unwrap();

        let rows = store.list_metadata_blocks(conv).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, request);
        assert_eq!(rows[0].block_type, "title_request");
        assert!(
            !rows[0].fields.contains_key("content"),
            "a NULL content is an absent field, not an empty one"
        );
        assert_eq!(rows[1].id, response);
        assert_eq!(rows[1].fields["content"], "A title");
        assert_eq!(rows[1].fields["source_block_id"], block);

        // The derived title is folded onto the conversation at read time.
        let conversation = store.find_conversation(conv).await.unwrap().unwrap();
        assert_eq!(conversation.title.as_deref(), Some("A title"));

        assert_eq!(store.metadata_cursor(conv).await.unwrap(), 0);
        store.update_metadata_cursor(conv, response).await.unwrap();
        assert_eq!(store.metadata_cursor(conv).await.unwrap(), response);
        assert_eq!(
            store.cursor(conv).await.unwrap(),
            0,
            "the block cursor is untouched by the metadata one"
        );
    }

    /// The assistant probe reads every table that carries a role, including the
    /// streamed tool-call tail — the one role-carrying table it used to miss,
    /// while the streamed text and reasoning tails already counted through the
    /// tables they share with their finalized blocks.
    #[tokio::test]
    async fn a_streamed_tool_call_counts_as_an_assistant_block() {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();

        assert!(!store.has_assistant_blocks(conv).await.unwrap());
        store
            .insert_streaming_tool_call_block(conv, Role::Assistant, "call_1".into(), "edit".into())
            .await
            .unwrap();
        assert!(store.has_assistant_blocks(conv).await.unwrap());
    }
}
