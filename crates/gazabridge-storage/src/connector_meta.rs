//! Last detected server facts per connection.
//!
//! Findings and manifests are stored as JSON so this crate does not depend
//! on the connector SDK.

use chrono::{DateTime, Utc};
use gazabridge_domain::ids::ConnectionId;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::{MetadataStore, StorageResult};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorMetadata {
    pub connection_id: ConnectionId,
    pub server_product: String,
    pub server_version: String,
    pub detected_profile: Option<String>,
    /// Effective compatibility status (serialized `CompatibilityStatus`).
    pub assessed_status: String,
    pub findings: serde_json::Value,
    pub manifest: serde_json::Value,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ConnectorMetaRepo<'a> {
    store: &'a MetadataStore,
}

impl MetadataStore {
    pub fn connector_metadata(&self) -> ConnectorMetaRepo<'_> {
        ConnectorMetaRepo { store: self }
    }
}

impl ConnectorMetaRepo<'_> {
    pub async fn upsert(&self, m: &ConnectorMetadata) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO connector_metadata (connection_id, server_product, server_version, detected_profile,
                 assessed_status, findings_json, manifest_json, detected_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(connection_id) DO UPDATE SET
                 server_product = excluded.server_product, server_version = excluded.server_version,
                 detected_profile = excluded.detected_profile, assessed_status = excluded.assessed_status,
                 findings_json = excluded.findings_json, manifest_json = excluded.manifest_json,
                 detected_at = excluded.detected_at",
        )
        .bind(m.connection_id.to_string())
        .bind(&m.server_product)
        .bind(&m.server_version)
        .bind(&m.detected_profile)
        .bind(&m.assessed_status)
        .bind(serde_json::to_string(&m.findings)?)
        .bind(serde_json::to_string(&m.manifest)?)
        .bind(m.detected_at)
        .execute(self.store.pool())
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: ConnectionId) -> StorageResult<Option<ConnectorMetadata>> {
        let row = sqlx::query("SELECT * FROM connector_metadata WHERE connection_id = ?")
            .bind(id.to_string())
            .fetch_optional(self.store.pool())
            .await?;
        row.map(|r| {
            Ok(ConnectorMetadata {
                connection_id: id,
                server_product: r.try_get("server_product")?,
                server_version: r.try_get("server_version")?,
                detected_profile: r.try_get("detected_profile")?,
                assessed_status: r.try_get("assessed_status")?,
                findings: serde_json::from_str(&r.try_get::<String, _>("findings_json")?)?,
                manifest: serde_json::from_str(&r.try_get::<String, _>("manifest_json")?)?,
                detected_at: r.try_get("detected_at")?,
            })
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use gazabridge_domain::connection::{
        ConnectionProfile, ConnectionSettings, SqliteJournalMode, SqliteSettings,
    };

    use super::*;

    #[tokio::test]
    async fn upsert_and_cascade_delete() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let p = ConnectionProfile::new(
            "s",
            "sqlite.sqlite",
            ConnectionSettings::Sqlite(SqliteSettings {
                path: PathBuf::from("/tmp/x.db"),
                read_only: false,
                create_if_missing: false,
                busy_timeout_ms: 1000,
                journal_mode: SqliteJournalMode::Wal,
                foreign_keys: true,
            }),
        );
        store.connections().save(&p).await.unwrap();
        let mut m = ConnectorMetadata {
            connection_id: p.id,
            server_product: "SQLite".into(),
            server_version: "3.46.0".into(),
            detected_profile: Some("sqlite.sqlite".into()),
            assessed_status: "beta".into(),
            findings: serde_json::json!([]),
            manifest: serde_json::json!({"capabilities": []}),
            detected_at: Utc::now(),
        };
        store.connector_metadata().upsert(&m).await.unwrap();
        m.server_version = "3.47.0".into();
        store.connector_metadata().upsert(&m).await.unwrap();
        assert_eq!(
            store
                .connector_metadata()
                .get(p.id)
                .await
                .unwrap()
                .unwrap()
                .server_version,
            "3.47.0"
        );
        store.connections().delete(p.id).await.unwrap();
        assert!(
            store
                .connector_metadata()
                .get(p.id)
                .await
                .unwrap()
                .is_none()
        );
    }
}
