//! Local metadata persistence.
//!
//! One SQLite file holds settings, connection metadata, jobs, runs,
//! checkpoints, saved queries, history and the audit log. It runs in WAL
//! mode with foreign keys enforced and a busy timeout, and every schema
//! change goes through the migrations in `database-migrations/metadata`.

pub mod audit;
pub mod connections;
pub mod connector_meta;
pub mod jobs;
pub mod queries;
pub mod settings;
pub mod snapshots;

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../database-migrations/metadata");

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("metadata database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("metadata migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("stored data is corrupt: {0}")]
    Corrupt(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("{0} not found")]
    NotFound(&'static str),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Handle to the metadata database. Cheap to clone.
#[derive(Debug, Clone)]
pub struct MetadataStore {
    pool: SqlitePool,
    path: Option<PathBuf>,
}

impl MetadataStore {
    /// Opens (creating if needed) the metadata database at `path` and
    /// applies pending migrations.
    pub async fn open(path: &Path) -> StorageResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        let store = Self {
            pool,
            path: Some(path.to_path_buf()),
        };
        store.migrate().await?;
        Ok(store)
    }

    /// An in-memory store for tests and previews.
    pub async fn open_in_memory() -> StorageResult<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")?.foreign_keys(true);
        // A single connection: each in-memory connection is its own database.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let store = Self { pool, path: None };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> StorageResult<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Latest applied migration version.
    pub async fn schema_version(&self) -> StorageResult<i64> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(version), 0) AS v FROM _sqlx_migrations WHERE success = 1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("v")?)
    }

    /// Runs `PRAGMA integrity_check` and foreign-key verification.
    pub async fn check_integrity(&self) -> StorageResult<()> {
        let rows = sqlx::query("PRAGMA integrity_check")
            .fetch_all(&self.pool)
            .await?;
        let messages: Vec<String> = rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>(0).ok())
            .filter(|m| m != "ok")
            .collect();
        if !messages.is_empty() {
            return Err(StorageError::Corrupt(messages.join("; ")));
        }
        let fk = sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&self.pool)
            .await?;
        if !fk.is_empty() {
            return Err(StorageError::Corrupt(format!(
                "{} foreign-key violations",
                fk.len()
            )));
        }
        Ok(())
    }

    /// Writes a consistent copy of the database to `dest` (`VACUUM INTO`).
    /// Refuses to overwrite an existing file.
    pub async fn backup_to(&self, dest: &Path) -> StorageResult<()> {
        if dest.exists() {
            return Err(StorageError::InvalidState(format!(
                "backup destination {} already exists",
                dest.display()
            )));
        }
        let dest_str = dest
            .to_str()
            .ok_or_else(|| StorageError::InvalidState("backup path is not valid UTF-8".into()))?;
        sqlx::query("VACUUM INTO ?")
            .bind(dest_str)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

pub(crate) fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

pub(crate) fn parse_id<T: FromStr>(raw: &str, what: &'static str) -> StorageResult<T> {
    raw.parse()
        .map_err(|_| StorageError::Corrupt(format!("invalid {what} id")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn opens_migrates_and_checks_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("meta.db");
        let store = MetadataStore::open(&path).await.unwrap();
        assert!(store.schema_version().await.unwrap() > 0);
        store.check_integrity().await.unwrap();

        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
        let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(fk, 1);
        store.close().await;

        // Re-opening is idempotent.
        let again = MetadataStore::open(&path).await.unwrap();
        again.check_integrity().await.unwrap();
    }

    #[tokio::test]
    async fn backup_creates_a_copy_and_refuses_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetadataStore::open(&dir.path().join("meta.db"))
            .await
            .unwrap();
        store
            .settings()
            .set("theme", &serde_json::json!("dark"))
            .await
            .unwrap();
        let dest = dir.path().join("backup.db");
        store.backup_to(&dest).await.unwrap();
        assert!(dest.exists());
        assert!(store.backup_to(&dest).await.is_err());

        let copy = MetadataStore::open(&dest).await.unwrap();
        let theme: Option<serde_json::Value> = copy.settings().get("theme").await.unwrap();
        assert_eq!(theme, Some(serde_json::json!("dark")));
    }

    #[tokio::test]
    async fn corrupt_file_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.db");
        std::fs::write(
            &path,
            b"this is not a sqlite database at all, just text padding....",
        )
        .unwrap();
        assert!(MetadataStore::open(&path).await.is_err());
    }
}
