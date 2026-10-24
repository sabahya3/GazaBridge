//! Saved schema snapshots.
//!
//! The object reference and schema are stored as JSON so this crate stays
//! independent of the connector SDK. `object_key` is a stable string used
//! to list the history of one object.

use chrono::{DateTime, Utc};
use gazabridge_domain::ids::ConnectionId;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::{MetadataStore, StorageError, StorageResult, now, parse_id};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub id: Uuid,
    pub connection_id: ConnectionId,
    pub object: serde_json::Value,
    pub object_key: String,
    pub label: Option<String>,
    /// True for inferred (sampled) document structures.
    pub inferred: bool,
    pub schema: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SnapshotRepo<'a> {
    store: &'a MetadataStore,
}

impl MetadataStore {
    pub fn snapshots(&self) -> SnapshotRepo<'_> {
        SnapshotRepo { store: self }
    }
}

fn row_to_snapshot(r: &sqlx::sqlite::SqliteRow) -> StorageResult<SchemaSnapshot> {
    Ok(SchemaSnapshot {
        id: r
            .try_get::<String, _>("id")?
            .parse()
            .map_err(|_| StorageError::Corrupt("invalid snapshot id".into()))?,
        connection_id: parse_id(&r.try_get::<String, _>("connection_id")?, "connection")?,
        object: serde_json::from_str(&r.try_get::<String, _>("object_json")?)?,
        object_key: r.try_get("object_key")?,
        label: r.try_get("label")?,
        inferred: r.try_get("inferred")?,
        schema: serde_json::from_str(&r.try_get::<String, _>("schema_json")?)?,
        created_at: r.try_get("created_at")?,
    })
}

impl SnapshotRepo<'_> {
    pub async fn save(
        &self,
        connection_id: ConnectionId,
        object: &serde_json::Value,
        object_key: &str,
        label: Option<&str>,
        inferred: bool,
        schema: &serde_json::Value,
    ) -> StorageResult<SchemaSnapshot> {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO schema_snapshots (id, connection_id, object_json, object_key, label, inferred, schema_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(connection_id.to_string())
        .bind(serde_json::to_string(object)?)
        .bind(object_key)
        .bind(label)
        .bind(inferred)
        .bind(serde_json::to_string(schema)?)
        .bind(now())
        .execute(self.store.pool())
        .await?;
        self.get(id)
            .await?
            .ok_or(StorageError::NotFound("snapshot"))
    }

    pub async fn get(&self, id: Uuid) -> StorageResult<Option<SchemaSnapshot>> {
        let row = sqlx::query("SELECT * FROM schema_snapshots WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.store.pool())
            .await?;
        row.as_ref().map(row_to_snapshot).transpose()
    }

    /// Snapshots of one object, newest first.
    pub async fn history(
        &self,
        connection_id: ConnectionId,
        object_key: &str,
        limit: u32,
    ) -> StorageResult<Vec<SchemaSnapshot>> {
        let rows = sqlx::query(
            "SELECT * FROM schema_snapshots WHERE connection_id = ? AND object_key = ?
             ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(connection_id.to_string())
        .bind(object_key)
        .bind(i64::from(limit))
        .fetch_all(self.store.pool())
        .await?;
        rows.iter().map(row_to_snapshot).collect()
    }

    pub async fn delete(&self, id: Uuid) -> StorageResult<()> {
        let r = sqlx::query("DELETE FROM schema_snapshots WHERE id = ?")
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        if r.rows_affected() == 0 {
            return Err(StorageError::NotFound("snapshot"));
        }
        Ok(())
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
    async fn snapshots_are_kept_per_object_and_cascade() {
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
        let repo = store.snapshots();
        let obj = serde_json::json!({"name": "orders"});
        let first = repo
            .save(
                p.id,
                &obj,
                "db.orders",
                Some("before"),
                true,
                &serde_json::json!({"v": 1}),
            )
            .await
            .unwrap();
        repo.save(
            p.id,
            &obj,
            "db.orders",
            None,
            true,
            &serde_json::json!({"v": 2}),
        )
        .await
        .unwrap();
        repo.save(
            p.id,
            &serde_json::json!({}),
            "db.other",
            None,
            false,
            &serde_json::json!({}),
        )
        .await
        .unwrap();
        let history = repo.history(p.id, "db.orders", 10).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].schema, serde_json::json!({"v": 2}));
        assert_eq!(
            repo.get(first.id).await.unwrap().unwrap().label.as_deref(),
            Some("before")
        );
        store.connections().delete(p.id).await.unwrap();
        assert!(
            repo.history(p.id, "db.orders", 10)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
