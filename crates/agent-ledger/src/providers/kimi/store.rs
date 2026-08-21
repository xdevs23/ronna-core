//! This vendor's configuration table.
//!
//! It does not share the credential-and-endpoint table the other HTTP providers
//! use, because it holds no credential: it holds a token pair and an expiry,
//! which rotate on every refresh. A shared table would have to grow three
//! nullable columns that mean nothing to the providers that share it.

use rusqlite::OptionalExtension;

use crate::providers::mask_secret;
use crate::store::{StoreError, StoreTx, domain_migrate, domain_run};

/// The domain this table's migrations advance under.
const DOMAIN: &str = "kimi-oauth";

/// One instance's stored authorization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KimiConfig {
    /// An endpoint override, or `None` for the vendor's own default.
    pub base_url: Option<String>,
    /// The current access token.
    pub access_token: Option<String>,
    /// The refresh token, which the vendor rotates on every use.
    pub refresh_token: Option<String>,
    /// When the access token expires, in milliseconds since the epoch.
    pub expires_at: Option<i64>,
}

/// This vendor's configuration table.
#[derive(Clone)]
pub struct KimiStore {
    tx: StoreTx,
}

impl KimiStore {
    /// Create the table and return a handle on it.
    ///
    /// # Errors
    ///
    /// If the migration fails or the store's actor has stopped.
    pub async fn new(tx: StoreTx) -> Result<Self, StoreError> {
        let sqls = vec![
            "CREATE TABLE IF NOT EXISTS provider_kimi_oauth (
                provider_id   TEXT PRIMARY KEY REFERENCES provider_instances(id) ON DELETE CASCADE,
                base_url      TEXT,
                access_token  TEXT,
                refresh_token TEXT,
                expires_at    INTEGER
            );",
            // The predecessor table held a static credential. Dropping it is
            // the point of this step: leaving it would leave a secret behind in
            // a table nothing reads any more.
            "DROP TABLE IF EXISTS provider_kimi;",
        ];
        domain_migrate(&tx, DOMAIN, sqls).await?;
        Ok(Self { tx })
    }

    /// Read one instance's authorization.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn get_config(&self, provider_id: String) -> Result<Option<KimiConfig>, StoreError> {
        domain_run(&self.tx, DOMAIN, move |conn| {
            conn.prepare(
                "SELECT base_url, access_token, refresh_token, expires_at
                 FROM provider_kimi_oauth WHERE provider_id = ?1",
            )?
            .query_row([&provider_id], |row| {
                Ok(KimiConfig {
                    base_url: row.get(0)?,
                    access_token: row.get(1)?,
                    refresh_token: row.get(2)?,
                    expires_at: row.get(3)?,
                })
            })
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// Write one instance's authorization, inserting or updating.
    ///
    /// # Errors
    ///
    /// If the write fails or the store's actor has stopped.
    pub async fn save_config(
        &self,
        provider_id: String,
        config: KimiConfig,
    ) -> Result<(), StoreError> {
        domain_run(&self.tx, DOMAIN, move |conn| {
            conn.execute(
                "INSERT INTO provider_kimi_oauth
                 (provider_id, base_url, access_token, refresh_token, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(provider_id) DO UPDATE SET
                     base_url = excluded.base_url,
                     access_token = excluded.access_token,
                     refresh_token = excluded.refresh_token,
                     expires_at = excluded.expires_at",
                (
                    &provider_id,
                    &config.base_url,
                    &config.access_token,
                    &config.refresh_token,
                    config.expires_at,
                ),
            )?;
            Ok(())
        })
        .await
    }

    /// Forget one instance's authorization.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn delete_config(&self, provider_id: String) -> Result<(), StoreError> {
        domain_run(&self.tx, DOMAIN, move |conn| {
            conn.execute(
                "DELETE FROM provider_kimi_oauth WHERE provider_id = ?1",
                [&provider_id],
            )?;
            Ok(())
        })
        .await
    }

    /// A one-line subtitle: the masked token, or a note that nobody has signed
    /// in yet.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn summary(&self, provider_id: String) -> Result<Option<String>, StoreError> {
        let Some(config) = self.get_config(provider_id).await? else {
            return Ok(None);
        };

        Ok(Some(
            config
                .access_token
                .as_deref()
                .filter(|t| !t.is_empty())
                .map_or_else(|| "Not authenticated".to_string(), mask_secret),
        ))
    }
}
