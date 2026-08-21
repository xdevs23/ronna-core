//! Draft persistence: the composer's mutable state, kept apart from blocks.
//!
//! A draft is the one thing in this schema that is edited in place. Blocks are
//! append-only, so the composer's work-in-progress cannot live among them; it
//! lives in its own tables and is promoted into blocks in one transaction.

use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::types::InputBlock;

use super::blocks::resolve_quote_text;
use super::{Store, StoreError, now_iso8601};

/// Block types stored in the draft tables.
///
/// On load, quote blocks include the resolved quoted text, so a consumer's
/// interface can reconstruct the composer without a second round trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DraftBlock {
    /// Literal text the human typed.
    Text {
        /// The text itself.
        content: String,
    },
    /// A span selected out of earlier blocks, with its text already resolved.
    Quote {
        /// Block the selection starts in.
        start_block_id: i64,
        /// Character offset the selection starts at.
        start_pos: i64,
        /// Block the selection ends in.
        end_block_id: i64,
        /// Character offset the selection ends at.
        end_pos: i64,
        /// The selected text, resolved at load time.
        text: String,
    },
}

impl Store {
    /// Replace a conversation's draft with these blocks.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn save_draft(
        &self,
        conversation_id: i64,
        blocks: Vec<InputBlock>,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            let now = now_iso8601();
            let tx = conn.transaction()?;

            tx.execute(
                "INSERT INTO drafts (conversation_id, updated_at)
                 VALUES (?1, ?2)
                 ON CONFLICT(conversation_id) DO UPDATE SET updated_at = excluded.updated_at",
                params![conversation_id, now],
            )?;
            let draft_id: i64 = tx.query_row(
                "SELECT id FROM drafts WHERE conversation_id = ?1",
                [conversation_id],
                |row| row.get(0),
            )?;

            tx.execute("DELETE FROM draft_blocks WHERE draft_id = ?1", [draft_id])?;

            for (pos, block) in blocks.iter().enumerate() {
                let position = i64::try_from(pos).unwrap_or(i64::MAX);
                match block {
                    InputBlock::Text { content } => {
                        tx.execute(
                            "INSERT INTO draft_blocks (draft_id, position, block_type)
                             VALUES (?1, ?2, 'text')",
                            params![draft_id, position],
                        )?;
                        let block_id = tx.last_insert_rowid();
                        tx.execute(
                            "INSERT INTO draft_block_text (block_id, content) VALUES (?1, ?2)",
                            params![block_id, content],
                        )?;
                    }
                    InputBlock::Quote {
                        start_block_id,
                        start_pos,
                        end_block_id,
                        end_pos,
                    } => {
                        tx.execute(
                            "INSERT INTO draft_blocks (draft_id, position, block_type)
                             VALUES (?1, ?2, 'quote')",
                            params![draft_id, position],
                        )?;
                        let block_id = tx.last_insert_rowid();
                        tx.execute(
                            "INSERT INTO draft_block_quote (block_id, start_block_id, start_pos, end_block_id, end_pos)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![block_id, start_block_id, start_pos, end_block_id, end_pos],
                        )?;
                    }
                }
            }

            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// A conversation's draft in composer order, quotes resolved. An empty
    /// vector means there is no draft.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn load_draft(&self, conversation_id: i64) -> Result<Vec<DraftBlock>, StoreError> {
        self.run(move |conn| {
            let draft_id: Option<i64> = conn
                .prepare("SELECT id FROM drafts WHERE conversation_id = ?1")?
                .query_row([conversation_id], |row| row.get(0))
                .optional()?;

            let Some(draft_id) = draft_id else {
                return Ok(Vec::new());
            };

            let mut stmt = conn.prepare(
                "SELECT db.block_type, dbt.content,
                        dbq.start_block_id, dbq.start_pos, dbq.end_block_id, dbq.end_pos
                 FROM draft_blocks db
                 LEFT JOIN draft_block_text dbt ON dbt.block_id = db.id
                 LEFT JOIN draft_block_quote dbq ON dbq.block_id = db.id
                 WHERE db.draft_id = ?1
                 ORDER BY db.position",
            )?;

            let blocks: Vec<DraftBlock> = stmt
                .query_map([draft_id], |row| {
                    let block_type: String = row.get(0)?;
                    match block_type.as_str() {
                        "quote" => Ok(DraftBlock::Quote {
                            start_block_id: row.get(2)?,
                            start_pos: row.get(3)?,
                            end_block_id: row.get(4)?,
                            end_pos: row.get(5)?,
                            text: String::new(),
                        }),
                        _ => Ok(DraftBlock::Text {
                            content: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        }),
                    }
                })?
                .filter_map(Result::ok)
                .collect();

            let blocks = blocks
                .into_iter()
                .map(|b| match b {
                    DraftBlock::Quote {
                        start_block_id,
                        start_pos,
                        end_block_id,
                        end_pos,
                        ..
                    } => {
                        // The draft's own conversation is the quoting one: a
                        // draft quotes what its composer can see.
                        let text = resolve_quote_text(
                            conn,
                            Some(conversation_id),
                            start_block_id,
                            start_pos,
                            end_block_id,
                            end_pos,
                        );
                        DraftBlock::Quote {
                            start_block_id,
                            start_pos,
                            end_block_id,
                            end_pos,
                            text,
                        }
                    }
                    other @ DraftBlock::Text { .. } => other,
                })
                .collect();

            Ok(blocks)
        })
        .await
    }

    /// Turn a conversation's draft into user blocks and delete it, in one
    /// transaction. Returns the new block ids.
    ///
    /// # Errors
    ///
    /// If there is no draft, if the transaction fails, or if the store's actor
    /// has stopped.
    pub async fn promote_draft(&self, conversation_id: i64) -> Result<Vec<i64>, StoreError> {
        self.run(move |conn| {
            let now = now_iso8601();
            let tx = conn.transaction()?;

            let draft_id: i64 = tx
                .prepare("SELECT id FROM drafts WHERE conversation_id = ?1")?
                .query_row([conversation_id], |row| row.get(0))
                .map_err(|_| StoreError::Sqlite(rusqlite::Error::QueryReturnedNoRows))?;

            let draft_blocks: Vec<(i64, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT id, block_type FROM draft_blocks
                     WHERE draft_id = ?1 ORDER BY position",
                )?;
                stmt.query_map([draft_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .filter_map(Result::ok)
                    .collect()
            };

            // The date marker's change detection rides the promote
            // transaction, BEFORE the promoted user blocks.
            super::date_markers::ensure_date_marker(
                &tx,
                conversation_id,
                &super::date_markers::today_local(),
            )?;

            let mut block_ids = Vec::with_capacity(draft_blocks.len());

            for (draft_block_id, block_type) in &draft_blocks {
                tx.execute(
                    "INSERT INTO blocks (block_type, created_at) VALUES (?1, ?2)",
                    params![block_type, now],
                )?;
                let block_id = tx.last_insert_rowid();
                tx.execute(
                    "INSERT INTO conversation_blocks (conversation_id, block_id) VALUES (?1, ?2)",
                    params![conversation_id, block_id],
                )?;

                match block_type.as_str() {
                    "text" => {
                        tx.execute(
                            "INSERT INTO block_text (block_id, role, content)
                             SELECT ?1, 'user', content FROM draft_block_text WHERE block_id = ?2",
                            params![block_id, draft_block_id],
                        )?;
                    }
                    "quote" => {
                        tx.execute(
                            "INSERT INTO block_quote (block_id, role, start_block_id, start_pos, end_block_id, end_pos)
                             SELECT ?1, 'user', start_block_id, start_pos, end_block_id, end_pos
                             FROM draft_block_quote WHERE block_id = ?2",
                            params![block_id, draft_block_id],
                        )?;
                    }
                    _ => {}
                }

                block_ids.push(block_id);
            }

            tx.execute("DELETE FROM drafts WHERE id = ?1", [draft_id])?;

            tx.commit()?;
            Ok(block_ids)
        })
        .await
    }

    /// Discard a conversation's draft.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn delete_draft(&self, conversation_id: i64) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute(
                "DELETE FROM drafts WHERE conversation_id = ?1",
                [conversation_id],
            )?;
            Ok(())
        })
        .await
    }
}
