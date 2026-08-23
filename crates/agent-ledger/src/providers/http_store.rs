//! The config table an HTTP-based provider keeps, and the one shape most of
//! them need.
//!
//! A credential and an optional endpoint is what an HTTP provider's
//! configuration usually amounts to, so it is written once here rather than
//! four times. Each provider type still gets a table of its own — the table
//! name is a constructor argument — because two provider types sharing one
//! table would mean deleting one instance could take another's row with it.
//!
//! The rows live in that provider's own migration domain, on the same single
//! writer as the ledger. A provider does not get a second connection: one
//! writer is what makes "who is writing right now" a question nobody has to
//! answer.

use rusqlite::OptionalExtension;

use crate::store::{StoreError, StoreTx, domain_migrate, domain_run};

use super::mask_secret;

/// The configuration an HTTP-based provider instance holds.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpProviderConfig {
    /// The credential.
    pub api_key: String,
    /// An endpoint override, or `None` for the provider's own default.
    pub base_url: Option<String>,
    /// The model slug for cheap background work — title derivation — or
    /// `None` to run background work on each request's own main model. It
    /// sits on the shared shape so every provider on this store persists it
    /// whole; honoring it is each provider's own bind decision (2026-08-23:
    /// the gateway provider does, because a hardcoded background slug there
    /// crossed vendors and regions the operator never chose).
    #[serde(default)]
    pub lightweight_model: Option<String>,
}

/// One HTTP provider type's configuration table.
#[derive(Clone)]
pub struct HttpProviderStore {
    tx: StoreTx,
    domain: &'static str,
    table: &'static str,
}

impl HttpProviderStore {
    /// Create the table for one provider type and return a handle on it.
    ///
    /// The foreign key onto the instance registry cascades: forgetting a
    /// provider instance forgets its credential in the same statement, rather
    /// than leaving a row nothing points at holding a live secret.
    ///
    /// # Errors
    ///
    /// If the migration fails or the store's actor has stopped.
    pub async fn new(
        tx: StoreTx,
        domain: &'static str,
        table: &'static str,
    ) -> Result<Self, StoreError> {
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                provider_id TEXT PRIMARY KEY REFERENCES provider_instances(id) ON DELETE CASCADE,
                api_key     TEXT NOT NULL,
                base_url    TEXT
            )"
        );
        // v2: the background-work model column, appended instead of folded
        // into the CREATE — installed bases carry the v1 shape, and the
        // domain counter runs each domain's list once, in order, so a fresh
        // database takes both steps and an installed one takes only this.
        let alter_sql = format!("ALTER TABLE {table} ADD COLUMN lightweight_model TEXT");
        // The migration list is `&'static str` because a migration outlives the
        // handle that submitted it — the actor may still be running it after
        // this call returns. There is one of these per provider type per
        // process, so the leak is bounded by the number of provider types.
        let migrations = vec![
            Box::leak(create_sql.into_boxed_str()) as &'static str,
            Box::leak(alter_sql.into_boxed_str()) as &'static str,
        ];
        domain_migrate(&tx, domain, migrations).await?;
        Ok(Self { tx, domain, table })
    }

    /// Read one instance's configuration.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn get_config(
        &self,
        provider_id: String,
    ) -> Result<Option<HttpProviderConfig>, StoreError> {
        let table = self.table;
        domain_run(&self.tx, self.domain, move |conn| {
            conn.prepare(&format!(
                "SELECT api_key, base_url, lightweight_model FROM {table} WHERE provider_id = ?1"
            ))?
            .query_row([&provider_id], |row| {
                Ok(HttpProviderConfig {
                    api_key: row.get(0)?,
                    base_url: row.get(1)?,
                    lightweight_model: row.get(2)?,
                })
            })
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// Write one instance's configuration, inserting or updating.
    ///
    /// # Errors
    ///
    /// If the write fails or the store's actor has stopped.
    pub async fn save_config(
        &self,
        provider_id: String,
        config: HttpProviderConfig,
    ) -> Result<(), StoreError> {
        let table = self.table;
        domain_run(&self.tx, self.domain, move |conn| {
            conn.execute(
                &format!(
                    "INSERT INTO {table} (provider_id, api_key, base_url, lightweight_model)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(provider_id) DO UPDATE SET
                         api_key = excluded.api_key,
                         base_url = excluded.base_url,
                         lightweight_model = excluded.lightweight_model"
                ),
                (
                    &provider_id,
                    &config.api_key,
                    &config.base_url,
                    &config.lightweight_model,
                ),
            )?;
            Ok(())
        })
        .await
    }

    /// Forget one instance's configuration.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn delete_config(&self, provider_id: String) -> Result<(), StoreError> {
        let table = self.table;
        domain_run(&self.tx, self.domain, move |conn| {
            conn.execute(
                &format!("DELETE FROM {table} WHERE provider_id = ?1"),
                [&provider_id],
            )?;
            Ok(())
        })
        .await
    }

    /// A one-line subtitle for an instance: the masked credential, and the
    /// endpoint if one was set.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn summary(&self, provider_id: String) -> Result<Option<String>, StoreError> {
        let Some(config) = self.get_config(provider_id).await? else {
            return Ok(None);
        };

        let mut parts = Vec::new();
        if !config.api_key.is_empty() {
            parts.push(mask_secret(&config.api_key));
        }
        if let Some(ref url) = config.base_url {
            parts.push(url.clone());
        }
        Ok(if parts.is_empty() {
            None
        } else {
            Some(parts.join(" · "))
        })
    }
}
