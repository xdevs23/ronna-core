//! Attachment records and the sparse byte ranges that have been fetched.
//!
//! No file bytes pass through here. This module tracks what an attachment is
//! and which parts of it are already on disk; the caller owns the file itself.

use std::ops::Range;

use rusqlite::{OptionalExtension, params};

use super::{Store, StoreError};

/// Metadata for a persisted attachment.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Attachment {
    /// The caller's own id for it.
    pub id: String,
    /// Where it came from, if it came from anywhere.
    pub url: Option<String>,
    /// Its file name.
    pub filename: String,
    /// Its media type.
    pub mime: String,
    /// Its full size in bytes.
    pub total_size: i64,
    /// When the record was created.
    pub created_at: String,
}

/// A contiguous byte range that has been downloaded. Inclusive at both ends.
#[derive(Debug, Clone)]
pub struct ByteRange {
    /// First byte of the range.
    pub start: i64,
    /// Last byte of the range.
    pub end: i64,
}

impl Store {
    /// Create an attachment record. Does NOT write any file bytes — the caller
    /// creates the sparse file and downloads ranges separately.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn create_attachment(
        &self,
        id: String,
        url: Option<String>,
        filename: String,
        mime: String,
        total_size: i64,
        headers: Vec<(String, String)>,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO attachments (id, url, filename, mime, total_size)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![id, url, filename, mime, total_size],
            )?;
            for (name, value) in &headers {
                tx.execute(
                    "INSERT INTO attachment_headers (attachment_id, name, value)
                     VALUES (?1, ?2, ?3)",
                    params![id, name, value],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Look up an attachment by id.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn find_attachment(&self, id: String) -> Result<Option<Attachment>, StoreError> {
        self.run(move |conn| {
            conn.prepare(
                "SELECT id, url, filename, mime, total_size, created_at
                 FROM attachments WHERE id = ?1",
            )?
            .query_row(params![id], |row| {
                Ok(Attachment {
                    id: row.get(0)?,
                    url: row.get(1)?,
                    filename: row.get(2)?,
                    mime: row.get(3)?,
                    total_size: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// A response header recorded with an attachment.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn get_attachment_header(
        &self,
        attachment_id: String,
        name: String,
    ) -> Result<Option<String>, StoreError> {
        self.run(move |conn| {
            conn.prepare(
                "SELECT value FROM attachment_headers
                 WHERE attachment_id = ?1 AND name = ?2",
            )?
            .query_row(params![attachment_id, name], |row| row.get(0))
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// Every downloaded byte range for an attachment, sorted by start.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn get_attachment_ranges(
        &self,
        attachment_id: String,
    ) -> Result<Vec<ByteRange>, StoreError> {
        self.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT start, end FROM attachment_ranges
                 WHERE attachment_id = ?1 ORDER BY start",
            )?;
            let rows = stmt.query_map(params![attachment_id], |row| {
                Ok(ByteRange {
                    start: row.get(0)?,
                    end: row.get(1)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }

    /// Record a downloaded byte range, merging it with the overlapping and
    /// adjacent ones in a single transaction.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn add_attachment_range(
        &self,
        attachment_id: String,
        start: i64,
        end: i64,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;

            // Find every range that overlaps or abuts [start, end]. Abutting
            // means a gap of zero: [0,100] and [101,200] merge.
            let overlapping: Vec<(i64, i64)> = {
                let mut stmt = tx.prepare(
                    "SELECT start, end FROM attachment_ranges
                     WHERE attachment_id = ?1
                       AND start <= ?3 + 1
                       AND end >= ?2 - 1
                     ORDER BY start",
                )?;
                stmt.query_map(params![attachment_id, start, end], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })?
                .filter_map(Result::ok)
                .collect()
            };

            if overlapping.is_empty() {
                tx.execute(
                    "INSERT INTO attachment_ranges (attachment_id, start, end)
                     VALUES (?1, ?2, ?3)",
                    params![attachment_id, start, end],
                )?;
            } else {
                let merged_start = overlapping
                    .iter()
                    .map(|r| r.0)
                    .min()
                    .unwrap_or(start)
                    .min(start);
                let merged_end = overlapping
                    .iter()
                    .map(|r| r.1)
                    .max()
                    .unwrap_or(end)
                    .max(end);

                tx.execute(
                    "DELETE FROM attachment_ranges
                     WHERE attachment_id = ?1
                       AND start <= ?3 + 1
                       AND end >= ?2 - 1",
                    params![attachment_id, start, end],
                )?;

                tx.execute(
                    "INSERT INTO attachment_ranges (attachment_id, start, end)
                     VALUES (?1, ?2, ?3)",
                    params![attachment_id, merged_start, merged_end],
                )?;
            }

            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Whether a byte range is fully covered by what has been downloaded.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn has_attachment_range(
        &self,
        attachment_id: String,
        start: i64,
        end: i64,
    ) -> Result<bool, StoreError> {
        self.run(move |conn| {
            // A range is covered when a single downloaded range contains it
            // entirely — which is exactly what the merge on insert guarantees
            // for anything contiguous.
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM attachment_ranges
                 WHERE attachment_id = ?1
                   AND start <= ?2
                   AND end >= ?3",
                params![attachment_id, start, end],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        })
        .await
    }

    /// Which sub-ranges within `[start, end)` are not downloaded yet.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn missing_ranges(
        &self,
        attachment_id: String,
        start: i64,
        end: i64,
    ) -> Result<Vec<Range<i64>>, StoreError> {
        let downloaded = self.get_attachment_ranges(attachment_id).await?;
        let mut missing = Vec::new();
        let mut cursor = start;

        for range in &downloaded {
            if range.start > cursor && cursor < end {
                missing.push(cursor..range.start.min(end));
            }
            cursor = cursor.max(range.end + 1);
        }
        if cursor < end {
            missing.push(cursor..end);
        }

        Ok(missing)
    }

    /// Correct an attachment's media type — called when the real response
    /// disagrees with what was reported up front.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn update_attachment_mime(&self, id: String, mime: String) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute(
                "UPDATE attachments SET mime = ?2 WHERE id = ?1",
                params![id, mime],
            )?;
            Ok(())
        })
        .await
    }

    /// Delete an attachment record with its ranges and headers. The caller is
    /// responsible for removing the file from disk.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn delete_attachment(&self, id: String) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute("DELETE FROM attachments WHERE id = ?1", params![id])?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::Store;

    async fn fixture() -> Store {
        let store = Store::in_memory().unwrap();
        store
            .create_attachment(
                "a1".into(),
                Some("https://example.invalid/f.bin".into()),
                "f.bin".into(),
                "application/octet-stream".into(),
                1000,
                vec![("etag".into(), "\"abc\"".into())],
            )
            .await
            .unwrap();
        store
    }

    /// The record and its headers round-trip, and deleting it takes both.
    #[tokio::test]
    async fn an_attachment_round_trips_with_its_headers() {
        let store = fixture().await;

        let found = store.find_attachment("a1".into()).await.unwrap().unwrap();
        assert_eq!(found.filename, "f.bin");
        assert_eq!(found.total_size, 1000);
        assert_eq!(
            store
                .get_attachment_header("a1".into(), "etag".into())
                .await
                .unwrap()
                .as_deref(),
            Some("\"abc\"")
        );

        store
            .update_attachment_mime("a1".into(), "application/pdf".into())
            .await
            .unwrap();
        assert_eq!(
            store
                .find_attachment("a1".into())
                .await
                .unwrap()
                .unwrap()
                .mime,
            "application/pdf"
        );

        store.delete_attachment("a1".into()).await.unwrap();
        assert!(store.find_attachment("a1".into()).await.unwrap().is_none());
        assert!(
            store
                .get_attachment_header("a1".into(), "etag".into())
                .await
                .unwrap()
                .is_none(),
            "the headers cascade with the record"
        );
    }

    /// Ranges merge when they abut — a gap of zero is not a gap — and what is
    /// left over is what the caller still has to fetch.
    #[tokio::test]
    async fn ranges_merge_on_insert_and_the_gaps_are_derived() {
        let store = fixture().await;

        store
            .add_attachment_range("a1".into(), 0, 99)
            .await
            .unwrap();
        store
            .add_attachment_range("a1".into(), 200, 299)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_attachment_ranges("a1".into())
                .await
                .unwrap()
                .len(),
            2,
            "a real gap keeps the ranges apart"
        );

        // Abutting the first range merges rather than adding a third row.
        store
            .add_attachment_range("a1".into(), 100, 149)
            .await
            .unwrap();
        let ranges = store.get_attachment_ranges("a1".into()).await.unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!((ranges[0].start, ranges[0].end), (0, 149));

        assert!(
            store
                .has_attachment_range("a1".into(), 0, 149)
                .await
                .unwrap()
        );
        assert!(
            !store
                .has_attachment_range("a1".into(), 0, 250)
                .await
                .unwrap(),
            "a span crossing the gap is not covered"
        );

        let missing = store.missing_ranges("a1".into(), 0, 400).await.unwrap();
        assert_eq!(missing, vec![150..200, 300..400]);

        // Bridging the gap collapses everything into one range.
        store
            .add_attachment_range("a1".into(), 150, 199)
            .await
            .unwrap();
        let ranges = store.get_attachment_ranges("a1".into()).await.unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!((ranges[0].start, ranges[0].end), (0, 299));
        assert!(
            store
                .missing_ranges("a1".into(), 0, 300)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
