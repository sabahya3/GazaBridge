//! Saved queries and query history.

use chrono::{DateTime, Utc};
use gazabridge_domain::ids::{ConnectionId, SavedQueryId};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use crate::{MetadataStore, StorageError, StorageResult, now, parse_id};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedQuery {
    pub id: SavedQueryId,
    pub name: String,
    pub connection_id: Option<ConnectionId>,
    /// Query language identifier (`postgres_sql`, `mongo_aggregation`, …).
    pub language: String,
    pub text: String,
    pub favorite: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryStatus {
    Ok,
    Error,
    Cancelled,
}

impl HistoryStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub id: i64,
    pub connection_id: Option<ConnectionId>,
    pub language: String,
    pub text: String,
    pub started_at: DateTime<Utc>,
    pub duration_ms: Option<i64>,
    pub row_count: Option<i64>,
    pub affected: Option<i64>,
    pub status: HistoryStatus,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewHistoryEntry {
    pub connection_id: Option<ConnectionId>,
    pub language: String,
    pub text: String,
    pub started_at: DateTime<Utc>,
    pub duration_ms: Option<i64>,
    pub row_count: Option<i64>,
    pub affected: Option<i64>,
    pub status: HistoryStatus,
    /// Must already be redacted.
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryRepo<'a> {
    store: &'a MetadataStore,
}

impl MetadataStore {
    pub fn queries(&self) -> QueryRepo<'_> {
        QueryRepo { store: self }
    }
}

fn row_to_saved(r: &SqliteRow) -> StorageResult<SavedQuery> {
    let conn: Option<String> = r.try_get("connection_id")?;
    Ok(SavedQuery {
        id: parse_id(&r.try_get::<String, _>("id")?, "saved query")?,
        name: r.try_get("name")?,
        connection_id: conn.map(|c| parse_id(&c, "connection")).transpose()?,
        language: r.try_get("language")?,
        text: r.try_get("text")?,
        favorite: r.try_get("favorite")?,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
    })
}

impl QueryRepo<'_> {
    pub async fn save(&self, q: &SavedQuery) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO saved_queries (id, name, connection_id, language, text, favorite, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, connection_id = excluded.connection_id,
                 language = excluded.language, text = excluded.text, favorite = excluded.favorite,
                 updated_at = excluded.updated_at",
        )
        .bind(q.id.to_string())
        .bind(&q.name)
        .bind(q.connection_id.map(|c| c.to_string()))
        .bind(&q.language)
        .bind(&q.text)
        .bind(q.favorite)
        .bind(q.created_at)
        .bind(now())
        .execute(self.store.pool())
        .await?;
        Ok(())
    }

    pub async fn list_saved(
        &self,
        connection_id: Option<ConnectionId>,
    ) -> StorageResult<Vec<SavedQuery>> {
        let rows = sqlx::query(
            "SELECT * FROM saved_queries WHERE (?1 IS NULL OR connection_id = ?1)
             ORDER BY favorite DESC, name COLLATE NOCASE",
        )
        .bind(connection_id.map(|c| c.to_string()))
        .fetch_all(self.store.pool())
        .await?;
        rows.iter().map(row_to_saved).collect()
    }

    pub async fn delete_saved(&self, id: SavedQueryId) -> StorageResult<()> {
        let r = sqlx::query("DELETE FROM saved_queries WHERE id = ?")
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        if r.rows_affected() == 0 {
            return Err(StorageError::NotFound("saved query"));
        }
        Ok(())
    }

    pub async fn record_history(&self, e: NewHistoryEntry) -> StorageResult<i64> {
        let r = sqlx::query(
            "INSERT INTO query_history (connection_id, language, text, started_at, duration_ms, row_count, affected,
                 status, error_message)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(e.connection_id.map(|c| c.to_string()))
        .bind(&e.language)
        .bind(&e.text)
        .bind(e.started_at)
        .bind(e.duration_ms)
        .bind(e.row_count)
        .bind(e.affected)
        .bind(e.status.as_str())
        .bind(&e.error_message)
        .execute(self.store.pool())
        .await?;
        Ok(r.last_insert_rowid())
    }

    pub async fn history(
        &self,
        connection_id: Option<ConnectionId>,
        limit: u32,
    ) -> StorageResult<Vec<HistoryEntry>> {
        let rows = sqlx::query(
            "SELECT * FROM query_history WHERE (?1 IS NULL OR connection_id = ?1)
             ORDER BY started_at DESC, id DESC LIMIT ?2",
        )
        .bind(connection_id.map(|c| c.to_string()))
        .bind(i64::from(limit))
        .fetch_all(self.store.pool())
        .await?;
        rows.iter()
            .map(|r| {
                let conn: Option<String> = r.try_get("connection_id")?;
                let status = match r.try_get::<String, _>("status")?.as_str() {
                    "ok" => HistoryStatus::Ok,
                    "error" => HistoryStatus::Error,
                    "cancelled" => HistoryStatus::Cancelled,
                    _ => return Err(StorageError::Corrupt("unknown history status".into())),
                };
                Ok(HistoryEntry {
                    id: r.try_get("id")?,
                    connection_id: conn.map(|c| parse_id(&c, "connection")).transpose()?,
                    language: r.try_get("language")?,
                    text: r.try_get("text")?,
                    started_at: r.try_get("started_at")?,
                    duration_ms: r.try_get("duration_ms")?,
                    row_count: r.try_get("row_count")?,
                    affected: r.try_get("affected")?,
                    status,
                    error_message: r.try_get("error_message")?,
                })
            })
            .collect()
    }

    /// Keeps only the newest `keep` history entries; returns rows removed.
    pub async fn prune_history(&self, keep: u32) -> StorageResult<u64> {
        let r = sqlx::query(
            "DELETE FROM query_history WHERE id NOT IN (SELECT id FROM query_history ORDER BY started_at DESC, id DESC LIMIT ?)",
        )
        .bind(i64::from(keep))
        .execute(self.store.pool())
        .await?;
        Ok(r.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn saved_queries_and_history() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let repo = store.queries();
        let q = SavedQuery {
            id: SavedQueryId::new(),
            name: "Active users".into(),
            connection_id: None,
            language: "postgres_sql".into(),
            text: "SELECT * FROM users WHERE active LIMIT 100".into(),
            favorite: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        repo.save(&q).await.unwrap();
        assert_eq!(repo.list_saved(None).await.unwrap().len(), 1);
        repo.delete_saved(q.id).await.unwrap();
        assert!(repo.delete_saved(q.id).await.is_err());

        for i in 0..5 {
            repo.record_history(NewHistoryEntry {
                connection_id: None,
                language: "sqlite_sql".into(),
                text: format!("SELECT {i}"),
                started_at: Utc::now(),
                duration_ms: Some(1),
                row_count: Some(1),
                affected: None,
                status: HistoryStatus::Ok,
                error_message: None,
            })
            .await
            .unwrap();
        }
        assert_eq!(repo.prune_history(2).await.unwrap(), 3);
        let h = repo.history(None, 10).await.unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].text, "SELECT 4");
    }
}
