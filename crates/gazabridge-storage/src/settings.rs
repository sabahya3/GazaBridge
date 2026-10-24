//! Application settings and UI preferences (JSON values by key).

use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::SqlitePool;

use crate::{MetadataStore, StorageResult, now};

#[derive(Debug, Clone, Copy)]
enum Table {
    Settings,
    UiPreferences,
}

impl Table {
    fn get_sql(self) -> &'static str {
        match self {
            Self::Settings => "SELECT value_json FROM settings WHERE key = ?",
            Self::UiPreferences => "SELECT value_json FROM ui_preferences WHERE key = ?",
        }
    }

    fn set_sql(self) -> &'static str {
        match self {
            Self::Settings => {
                "INSERT INTO settings (key, value_json, updated_at) VALUES (?, ?, ?)
                 ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at"
            }
            Self::UiPreferences => {
                "INSERT INTO ui_preferences (key, value_json, updated_at) VALUES (?, ?, ?)
                 ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at"
            }
        }
    }

    fn delete_sql(self) -> &'static str {
        match self {
            Self::Settings => "DELETE FROM settings WHERE key = ?",
            Self::UiPreferences => "DELETE FROM ui_preferences WHERE key = ?",
        }
    }
}

/// Typed key/value access to `settings` or `ui_preferences`.
#[derive(Debug, Clone)]
pub struct KeyValueRepo<'a> {
    pool: &'a SqlitePool,
    table: Table,
}

impl MetadataStore {
    pub fn settings(&self) -> KeyValueRepo<'_> {
        KeyValueRepo {
            pool: self.pool(),
            table: Table::Settings,
        }
    }

    pub fn ui_preferences(&self) -> KeyValueRepo<'_> {
        KeyValueRepo {
            pool: self.pool(),
            table: Table::UiPreferences,
        }
    }
}

impl KeyValueRepo<'_> {
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> StorageResult<Option<T>> {
        let raw: Option<String> = sqlx::query_scalar(self.table.get_sql())
            .bind(key)
            .fetch_optional(self.pool)
            .await?;
        raw.map(|r| serde_json::from_str(&r).map_err(Into::into))
            .transpose()
    }

    pub async fn set<T: Serialize + ?Sized>(&self, key: &str, value: &T) -> StorageResult<()> {
        let json = serde_json::to_string(value)?;
        sqlx::query(self.table.set_sql())
            .bind(key)
            .bind(json)
            .bind(now())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> StorageResult<()> {
        sqlx::query(self.table.delete_sql())
            .bind(key)
            .execute(self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::MetadataStore;

    #[tokio::test]
    async fn settings_round_trip() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let s = store.settings();
        assert_eq!(s.get::<u32>("page_size").await.unwrap(), None);
        s.set("page_size", &200u32).await.unwrap();
        s.set("page_size", &500u32).await.unwrap();
        assert_eq!(s.get::<u32>("page_size").await.unwrap(), Some(500));
        s.delete("page_size").await.unwrap();
        assert_eq!(s.get::<u32>("page_size").await.unwrap(), None);

        store
            .ui_preferences()
            .set("layout", &serde_json::json!({"sidebar": 240}))
            .await
            .unwrap();
        assert!(
            store
                .settings()
                .get::<serde_json::Value>("layout")
                .await
                .unwrap()
                .is_none()
        );
    }
}
