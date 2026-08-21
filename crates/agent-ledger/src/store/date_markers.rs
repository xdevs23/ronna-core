//! The date-marker insert seam: change detection, not a creation special case.
//!
//! The user-block insert paths call [`ensure_date_marker`] inside their own
//! transaction, BEFORE the user blocks — the marker rides the same atomic
//! append as the message that owes the turn, so the wire never carries a date
//! the ledger cannot replay.

use rusqlite::{Connection, OptionalExtension, params};

use super::StoreError;
use super::messages::insert_block;

/// Today in the machine's local timezone — the marker's stored shape.
pub(super) fn today_local() -> String {
    chrono::Local::now().format("%Y-%m-%d").to_string()
}

/// Compare `today` against the LATEST date marker in the conversation's ledger
/// (junction order); differ — or none, which the first message trips for free —
/// insert a fresh marker and return its id. Same-day appends insert nothing,
/// and a conversation nobody writes to never gets one.
pub(super) fn ensure_date_marker(
    conn: &Connection,
    conversation_id: i64,
    today: &str,
) -> Result<Option<i64>, StoreError> {
    let latest: Option<String> = conn
        .query_row(
            "SELECT bdm.date FROM block_date_marker bdm
             JOIN conversation_blocks cb ON cb.block_id = bdm.block_id
             WHERE cb.conversation_id = ?1
             ORDER BY cb.id DESC LIMIT 1",
            [conversation_id],
            |row| row.get(0),
        )
        .optional()?;

    if latest.as_deref() == Some(today) {
        return Ok(None);
    }

    let block_id = insert_block(conn, conversation_id, "date_marker")?;
    conn.execute(
        "INSERT INTO block_date_marker (block_id, date) VALUES (?1, ?2)",
        params![block_id, today],
    )?;
    Ok(Some(block_id))
}

#[cfg(test)]
mod tests {
    use crate::store::Store;
    use crate::types::InputBlock;

    async fn fixture() -> (Store, i64) {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        (store, conv)
    }

    fn text(content: &str) -> Vec<InputBlock> {
        vec![InputBlock::Text {
            content: content.into(),
        }]
    }

    async fn markers(store: &Store, conv: i64) -> Vec<(usize, String)> {
        store
            .list_blocks(conv)
            .await
            .unwrap()
            .iter()
            .enumerate()
            .filter(|(_, b)| b.block_type == "date_marker")
            .map(|(i, b)| (i, b.fields["date"].as_str().unwrap().to_string()))
            .collect()
    }

    /// The first user message trips "no marker yet is not today" for free: one
    /// marker, positioned BEFORE the user blocks in the ledger.
    #[tokio::test]
    async fn first_message_inserts_the_marker_before_the_user_blocks() {
        let (store, conv) = fixture().await;
        store.insert_user_blocks(conv, text("hello")).await.unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let types: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            types,
            vec!["date_marker", "text"],
            "the marker precedes the message"
        );
        assert_eq!(
            blocks[0].fields["date"].as_str().unwrap(),
            super::today_local(),
            "the marker carries today's local date"
        );
    }

    /// A same-day second message appends no fresh marker — change detection,
    /// not per-message decoration.
    #[tokio::test]
    async fn same_day_second_message_inserts_no_marker() {
        let (store, conv) = fixture().await;
        store.insert_user_blocks(conv, text("one")).await.unwrap();
        store.insert_user_blocks(conv, text("two")).await.unwrap();

        assert_eq!(markers(&store, conv).await.len(), 1);
    }

    /// A conversation spanning midnight gets a fresh marker on the next user
    /// message — driven through the injectable-date seam.
    #[tokio::test]
    async fn changed_date_inserts_a_fresh_marker() {
        let (store, conv) = fixture().await;
        store
            .insert_user_blocks_dated(conv, text("yesterday"), "2026-07-11".into())
            .await
            .unwrap();
        store
            .insert_user_blocks_dated(conv, text("today"), "2026-07-12".into())
            .await
            .unwrap();

        let markers = markers(&store, conv).await;
        assert_eq!(
            markers.len(),
            2,
            "midnight crossed — a fresh marker rides the new message"
        );
        assert_eq!(markers[0].1, "2026-07-11");
        assert_eq!(markers[1].1, "2026-07-12");

        // The fresh marker sits immediately before its message.
        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(
            blocks[markers[1].0 + 1].fields["content"].as_str().unwrap(),
            "today"
        );
    }

    /// The promote path — the other insert seam — rides the same change
    /// detection inside the promote transaction.
    #[tokio::test]
    async fn promote_draft_inserts_the_marker_once() {
        let (store, conv) = fixture().await;
        store.save_draft(conv, text("drafted")).await.unwrap();
        store.promote_draft(conv).await.unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let types: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(types, vec!["date_marker", "text"]);

        store.save_draft(conv, text("again")).await.unwrap();
        store.promote_draft(conv).await.unwrap();
        assert_eq!(
            markers(&store, conv).await.len(),
            1,
            "same-day promote adds no marker"
        );
    }
}
