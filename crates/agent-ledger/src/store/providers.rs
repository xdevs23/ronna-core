//! The provider registry: identity only, no configuration.
//!
//! What a provider instance needs to be reached — endpoints, credentials — is
//! not stored here. This table exists so a model row can name where it came
//! from and keep naming it after the provider is gone.

use rusqlite::OptionalExtension;
use tracing::warn;

use super::{Store, StoreError};

/// A registered provider instance: identity only.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProviderInstance {
    /// The caller's own id for this instance.
    pub id: String,
    /// Which kind of provider it is.
    #[serde(rename = "type")]
    pub provider_type: String,
    /// The name a human reads.
    pub name: String,
}

impl Store {
    /// Every registered provider instance, by name.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn list_provider_instances(&self) -> Result<Vec<ProviderInstance>, StoreError> {
        self.run(|conn| {
            let mut stmt =
                conn.prepare("SELECT id, type, name FROM provider_instances ORDER BY name")?;
            let rows = stmt
                .query_map([], row_to_provider_instance)?
                .filter_map(|r| {
                    r.map_err(|e| warn!(error = %e, "skipping corrupted provider row"))
                        .ok()
                })
                .collect();
            Ok(rows)
        })
        .await
    }

    /// One provider instance by id.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn find_provider_instance(
        &self,
        id: String,
    ) -> Result<Option<ProviderInstance>, StoreError> {
        self.run(move |conn| {
            conn.prepare("SELECT id, type, name FROM provider_instances WHERE id = ?1")?
                .query_row([&id], row_to_provider_instance)
                .optional()
                .map_err(Into::into)
        })
        .await
    }

    /// Insert a provider instance, or update the one already under its id.
    ///
    /// # Errors
    ///
    /// If the upsert fails or the store's actor has stopped.
    pub async fn save_provider_instance(
        &self,
        instance: ProviderInstance,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute(
                "INSERT INTO provider_instances (id, type, name)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET type = excluded.type, name = excluded.name",
                (&instance.id, &instance.provider_type, &instance.name),
            )?;
            Ok(())
        })
        .await
    }

    /// Remove a provider instance. Models that named it keep their rows and
    /// fall back to reporting its id as its name.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn delete_provider_instance(&self, id: String) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute("DELETE FROM provider_instances WHERE id = ?1", [&id])?;
            Ok(())
        })
        .await
    }
}

fn row_to_provider_instance(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProviderInstance> {
    Ok(ProviderInstance {
        id: row.get(0)?,
        provider_type: row.get(1)?,
        name: row.get(2)?,
    })
}
