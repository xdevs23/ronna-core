//! The recorded tool choice: appending one, and reading the newest one back.
//!
//! Two doors and one storage rule between them. The append writes a block like
//! any other — header, junction, content row, one transaction — and the read
//! answers with the NEWEST choice the conversation carries, which is the one
//! that speaks. Superseding is appending; nothing here updates a row.
//!
//! The read is one statement over the junction instead of a fold over a
//! loaded ledger, because two of its callers hold no ledger: the compacted
//! thread's opening, which runs inside its own transaction, and a consumer
//! deciding whether the registered set has changed. The readers that DO hold a
//! ledger — the dispatch and the runner — fold their own snapshot through
//! [`ToolChoice::newest_in`](crate::agency::ToolChoice::newest_in) instead of
//! spending a second read, and both answer from the same rule: the last
//! matching block in junction order.

use rusqlite::{Connection, OptionalExtension, params};

use crate::agency::{LeafKind, ToolChoice};

use super::messages::insert_block;
use super::{Store, StoreError, transact};

impl Store {
    /// Record which tools a conversation has, superseding whatever it recorded
    /// before by being appended after it.
    ///
    /// An empty list is a decision, not an omission: this conversation has no
    /// tools, and both readers of the record answer accordingly. A
    /// conversation that has never been handed one of these has made no
    /// decision at all, which is the different answer
    /// [`newest_tool_choice`](Self::newest_tool_choice) reports as `None`.
    ///
    /// # Errors
    ///
    /// If the write fails or the store's actor has stopped.
    pub async fn append_tool_choice(
        &self,
        conversation_id: i64,
        names: Vec<String>,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                insert_tool_choice_block(tx, conversation_id, &names)
            })
        })
        .await
    }

    /// The newest tool choice this conversation recorded, or `None` when it
    /// recorded none.
    ///
    /// # Errors
    ///
    /// If the read fails or the store's actor has stopped.
    pub async fn newest_tool_choice(
        &self,
        conversation_id: i64,
    ) -> Result<Option<Vec<String>>, StoreError> {
        self.run(move |conn| newest_tool_choice(conn, conversation_id))
            .await
    }
}

/// Append one tool choice block. Called inside a transaction, so its header,
/// junction and content rows land together — the fork doors reach it directly.
///
/// The names are serialized in ONE place, here, and read back in one,
/// [`decode_tool_names`], so no second encoding and no second reading of the
/// stored list exists.
pub(super) fn insert_tool_choice_block(
    conn: &Connection,
    conversation_id: i64,
    names: &[String],
) -> Result<i64, StoreError> {
    let encoded = serde_json::to_string(names)
        .map_err(|error| StoreError::Other(format!("tool names do not serialize: {error}")))?;
    let block_id = insert_block(conn, conversation_id, ToolChoice::KINDS[0])?;
    conn.execute(
        "INSERT INTO block_tool_choice (block_id, names) VALUES (?1, ?2)",
        params![block_id, encoded],
    )?;
    Ok(block_id)
}

/// The newest recorded choice, by junction order — the same "last one
/// appended wins" the ledger fold answers, taken from the other end so a
/// caller that holds no ledger does not have to load one.
pub(super) fn newest_tool_choice(
    conn: &Connection,
    conversation_id: i64,
) -> Result<Option<Vec<String>>, StoreError> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT tc.names
             FROM conversation_blocks cb
             JOIN blocks b ON b.id = cb.block_id
             JOIN block_tool_choice tc ON tc.block_id = b.id
             WHERE cb.conversation_id = ?1 AND b.block_type = ?2
             ORDER BY cb.id DESC LIMIT 1",
            params![conversation_id, ToolChoice::KINDS[0]],
            |row| row.get(0),
        )
        .optional()?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    let names = decode_tool_names(&stored).map_err(|error| {
        StoreError::Other(format!(
            "conversation {conversation_id} records a tool choice whose names do not \
             parse: {error}"
        ))
    })?;
    Ok(Some(names))
}

/// Read the stored column back into the list [`insert_tool_choice_block`]
/// wrote — the ONE decoding of that form, called by both readers of a stored
/// row: the statement above, and the block query's own read.
///
/// The strictness is the point of sharing it. A list of anything but strings
/// is a corrupt row, and it has to be corrupt to BOTH readers: a column
/// holding `[1, 2]` decoded leniently would hand the resolution an empty list,
/// which reads as the opposite decision — this conversation has no tools —
/// while the other reader refused the same row. One decoding, one answer.
pub(super) fn decode_tool_names(stored: &str) -> Result<Vec<String>, serde_json::Error> {
    serde_json::from_str(stored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agency::{Agency, BlockKind, FromBlock};

    async fn conversation(store: &Store) -> i64 {
        store
            .create_conversation("p1".into(), "model".into(), "Model".into(), String::new())
            .await
            .unwrap()
    }

    /// AC1, AC2 — the record round-trips through the ordinary block read with
    /// its names intact, and the newest one is the one that speaks.
    #[tokio::test]
    async fn a_recorded_choice_reads_back_and_the_newest_one_speaks() {
        let store = Store::in_memory().unwrap();
        let conv = conversation(&store).await;

        assert_eq!(
            store.newest_tool_choice(conv).await.unwrap(),
            None,
            "a conversation that recorded nothing has made no choice"
        );

        store
            .append_tool_choice(conv, vec!["read".into(), "write".into()])
            .await
            .unwrap();
        assert_eq!(
            store.newest_tool_choice(conv).await.unwrap(),
            Some(vec!["read".to_owned(), "write".to_owned()])
        );

        let blocks = store.list_blocks(conv).await.unwrap();
        let choice = match BlockKind::from_block(blocks.last().unwrap()) {
            BlockKind::ToolChoice(choice) => choice,
            other => panic!("the block reads back as a tool choice: {other:?}"),
        };
        assert_eq!(choice.names, vec!["read".to_owned(), "write".to_owned()]);

        store
            .append_tool_choice(conv, vec!["write".into()])
            .await
            .unwrap();
        assert_eq!(
            store.newest_tool_choice(conv).await.unwrap(),
            Some(vec!["write".to_owned()]),
            "a later append supersedes an earlier one"
        );
        assert_eq!(
            store
                .list_blocks(conv)
                .await
                .unwrap()
                .iter()
                .filter(|block| block.block_type == "tool_choice")
                .count(),
            2,
            "superseding appends; it never rewrites what was recorded"
        );
    }

    /// AC1, AC4 — the empty choice is a decision the ledger keeps, told apart
    /// from having recorded nothing at all.
    #[tokio::test]
    async fn an_empty_choice_is_recorded_and_is_not_the_absence_of_one() {
        let store = Store::in_memory().unwrap();
        let conv = conversation(&store).await;
        store.append_tool_choice(conv, Vec::new()).await.unwrap();

        assert_eq!(
            store.newest_tool_choice(conv).await.unwrap(),
            Some(Vec::new()),
            "the empty list is what was recorded"
        );
        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(
            ToolChoice::newest_in(&blocks)
                .expect("the ledger carries one")
                .names,
            Vec::<String>::new()
        );
    }

    /// A column holding a list of anything but strings is corrupt to BOTH
    /// readers of a stored row, and to neither of them a conversation that
    /// chose no tools — the answer a lenient decoding would have produced
    /// through the block query while the statement refused the same row.
    #[tokio::test]
    async fn a_column_that_is_not_a_list_of_names_is_refused_by_both_readers() {
        let store = Store::in_memory().unwrap();
        let conv = conversation(&store).await;
        let block_id = store
            .append_tool_choice(conv, vec!["read".into()])
            .await
            .unwrap();

        for corrupt in ["[1, 2]", "{}", "not json"] {
            store
                .run(move |conn| {
                    conn.execute(
                        "UPDATE block_tool_choice SET names = ?1 WHERE block_id = ?2",
                        params![corrupt, block_id],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();

            assert!(
                store.newest_tool_choice(conv).await.is_err(),
                "the statement refuses {corrupt}"
            );
            assert!(
                store.list_blocks(conv).await.is_err(),
                "the block query refuses {corrupt} too, instead of reading it as no tools"
            );
        }
    }

    /// AC1 — the record projects nothing, awaits nobody, and the two readers
    /// of one conversation's ledger answer identically.
    #[tokio::test]
    async fn the_record_asks_nothing_and_shows_the_model_nothing() {
        let store = Store::in_memory().unwrap();
        let conv = conversation(&store).await;
        store
            .append_tool_choice(conv, vec!["read".into()])
            .await
            .unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let kind = BlockKind::from_block(blocks.last().unwrap());
        assert_eq!(kind.awaiting(), None, "the record asks nobody for anything");
        assert!(
            crate::providers::blocks_to_messages::<BlockKind>(&blocks).is_empty(),
            "the record says nothing to the model"
        );

        // The two readers of the recorded choice: the ledger fold both live
        // consumers use, and the one-statement read the fork doors use.
        assert_eq!(
            ToolChoice::newest_in(&blocks).map(|choice| choice.names),
            store.newest_tool_choice(conv).await.unwrap(),
            "the fold and the statement answer the same record"
        );
    }
}
