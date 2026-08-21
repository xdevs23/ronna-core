//! Model rows: normalized metadata that survives a provider delisting a model,
//! plus the recency and frequency reads a model picker needs.

use rusqlite::Connection;

use super::{Store, StoreError};
use crate::providers::ReasoningCapability;

/// A model as the picker sees it: the row's identity plus the provider instance
/// it is reached through, and room for the capability a live listing knows.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelEntry {
    /// The model row's id, absent for a model that has never been resolved.
    pub id: Option<i64>,
    /// The provider's own identifier for the model.
    pub external_id: String,
    /// The name a human reads.
    pub display_name: String,
    /// Who trained it.
    pub vendor: String,
    /// The provider instance it is reached through.
    pub provider_id: String,
    /// That instance's name, falling back to its id.
    pub provider_name: String,
    /// The reasoning levels this model accepts.
    ///
    /// Transport-only, and empty on every read in this module: what a model can
    /// do is live, version-dependent data, so no column holds it and a stored
    /// copy would be wrong the first time its provider changed it. The field is
    /// here so a caller holding a fresh listing — where
    /// [`ModelInfo::reasoning`](crate::providers::ModelInfo) carries the same
    /// value — can enrich an entry by `external_id` and pass on this one type.
    /// A second entry type differing by one field is how the enriched and the
    /// bare form start disagreeing.
    #[serde(default, skip_serializing_if = "ReasoningCapability::is_empty")]
    pub reasoning: ReasoningCapability,
}

/// Resolve (upsert) a model row and return its id.
///
/// A free function so it can be called from inside other store closures, where
/// the connection is already borrowed.
pub(super) fn resolve_model(
    conn: &Connection,
    provider_id: &str,
    external_id: &str,
    display_name: &str,
    vendor: &str,
) -> Result<i64, StoreError> {
    conn.execute(
        "INSERT OR IGNORE INTO models (external_id, display_name, vendor, provider_id)
         VALUES (?1, ?2, ?3, ?4)",
        (external_id, display_name, vendor, provider_id),
    )?;
    let id: i64 = conn.query_row(
        "SELECT id FROM models WHERE external_id = ?1 AND provider_id = ?2",
        (external_id, provider_id),
        |row| row.get(0),
    )?;
    Ok(id)
}

impl Store {
    /// Resolve a model row, inserting it if this provider has not offered it
    /// before. Idempotent: the same pair always answers with the same id.
    ///
    /// # Errors
    ///
    /// If the upsert fails or the store's actor has stopped.
    pub async fn resolve_model(
        &self,
        provider_id: String,
        external_id: String,
        display_name: String,
        vendor: String,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            resolve_model(conn, &provider_id, &external_id, &display_name, &vendor)
        })
        .await
    }

    /// The models most recently started a conversation on, newest first.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn recent_models(&self, limit: usize) -> Result<Vec<ModelEntry>, StoreError> {
        self.run(move |conn| {
            let mut stmt = conn.prepare(&format!(
                "{MODEL_ENTRY_SELECT}
                 GROUP BY m.id
                 ORDER BY MAX(c.created_at) DESC
                 LIMIT ?1"
            ))?;
            let rows = stmt
                .query_map(
                    [i64::try_from(limit).unwrap_or(i64::MAX)],
                    row_to_model_entry,
                )?
                .filter_map(Result::ok)
                .collect();
            Ok(rows)
        })
        .await
    }

    /// The models with the most blocks written under them, busiest first,
    /// skipping `exclude_ids` — the recents a caller has already shown.
    ///
    /// Volume is counted in junction rows. The statement this was extracted
    /// from counted rows in a message table that no longer exists anywhere in
    /// the schema, so it could only ever fail at runtime; blocks are what this
    /// architecture has, and the junction is where a conversation's blocks are
    /// counted.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn frequent_models(
        &self,
        limit: usize,
        exclude_ids: Vec<i64>,
    ) -> Result<Vec<ModelEntry>, StoreError> {
        self.run(move |conn| {
            // The exclusion list is built from i64 values the caller already
            // holds as numbers, never from text, so it cannot carry a fragment
            // of SQL with it.
            let excluded = if exclude_ids.is_empty() {
                "SELECT NULL WHERE 0".to_string()
            } else {
                exclude_ids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let sql = format!(
                "{MODEL_ENTRY_SELECT}
                 JOIN conversation_blocks cb ON cb.conversation_id = c.id
                 WHERE m.id NOT IN ({excluded})
                 GROUP BY m.id
                 ORDER BY COUNT(cb.id) DESC
                 LIMIT ?1"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map(
                    [i64::try_from(limit).unwrap_or(i64::MAX)],
                    row_to_model_entry,
                )?
                .filter_map(Result::ok)
                .collect();
            Ok(rows)
        })
        .await
    }
}

/// The model projection both picker reads start from.
const MODEL_ENTRY_SELECT: &str =
    "SELECT m.id, m.external_id, m.display_name, m.vendor, m.provider_id,
            COALESCE(p.name, m.provider_id)
     FROM models m
     JOIN conversations c ON c.model_id = m.id
     LEFT JOIN provider_instances p ON p.id = m.provider_id";

fn row_to_model_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<ModelEntry> {
    Ok(ModelEntry {
        id: Some(row.get(0)?),
        external_id: row.get(1)?,
        display_name: row.get(2)?,
        vendor: row.get(3)?,
        provider_id: row.get(4)?,
        provider_name: row.get(5)?,
        // No column holds capability, so a picker read reports none rather
        // than guessing. A caller enriches from its own listing.
        reasoning: ReasoningCapability::default(),
    })
}

#[cfg(test)]
mod tests {
    use crate::block::Role;
    use crate::providers::{ReasoningCapability, ReasoningLevel};
    use crate::store::Store;

    /// Recency orders by the newest conversation, frequency by how many blocks
    /// were written, and the exclusion list is honoured.
    #[tokio::test]
    async fn recents_order_by_time_and_frequents_by_volume() {
        let store = Store::in_memory().unwrap();

        let quiet = store
            .create_conversation("p".into(), "quiet".into(), "Quiet".into(), String::new())
            .await
            .unwrap();
        let busy = store
            .create_conversation("p".into(), "busy".into(), "Busy".into(), String::new())
            .await
            .unwrap();

        store
            .insert_text_block(quiet, Role::User, "one".into())
            .await
            .unwrap();
        for _ in 0..3 {
            store
                .insert_text_block(busy, Role::User, "more".into())
                .await
                .unwrap();
        }

        let recents = store.recent_models(10).await.unwrap();
        assert_eq!(recents.len(), 2);
        assert!(recents.iter().all(|m| m.provider_name == "p"));

        let frequents = store.frequent_models(10, Vec::new()).await.unwrap();
        assert_eq!(
            frequents.first().map(|m| m.external_id.as_str()),
            Some("busy"),
            "the model with the most blocks leads"
        );

        let busiest_id = frequents[0].id.unwrap();
        let rest = store.frequent_models(10, vec![busiest_id]).await.unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].external_id, "quiet");
    }

    /// The capability field is a transport slot, not a column: a picker read
    /// leaves it empty and it stays off the wire, and a caller that enriches an
    /// entry from a live listing carries the same value on the same type.
    #[tokio::test]
    async fn a_picker_read_carries_no_capability_until_a_caller_enriches_it() {
        let store = Store::in_memory().unwrap();
        let conversation = store
            .create_conversation("p".into(), "m".into(), "M".into(), String::new())
            .await
            .unwrap();
        store
            .insert_text_block(conversation, Role::User, "one".into())
            .await
            .unwrap();

        let mut entry = store.recent_models(10).await.unwrap().remove(0);
        assert!(
            entry.reasoning.is_empty(),
            "no column holds capability, so a read reports none"
        );
        let bare = serde_json::to_value(&entry).unwrap();
        assert!(
            bare.get("reasoning").is_none(),
            "an empty capability stays off the wire entirely"
        );

        entry.reasoning = ReasoningCapability::new(vec![ReasoningLevel::Off, ReasoningLevel::High]);
        let enriched = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            enriched["reasoning"],
            serde_json::json!({ "levels": ["off", "high"], "default": null }),
            "the enriched entry carries the listing's own capability shape"
        );
    }
}
