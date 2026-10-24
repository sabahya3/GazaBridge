//! Connection profiles, folders, tags and the secret-reference index.

use chrono::{DateTime, Utc};
use gazabridge_domain::compatibility::ConnectionState;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, SecretRef, SshAuth, SshTunnel, TlsSettings,
};
use gazabridge_domain::ids::{ConnectionId, FolderId};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, Sqlite, Transaction};

use crate::{MetadataStore, StorageError, StorageResult, now, parse_id};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Folder {
    pub id: FolderId,
    pub name: String,
    pub parent_id: Option<FolderId>,
    pub position: i64,
}

/// Filters for listing connections. All set fields must match.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionFilter {
    /// Case-insensitive substring of name, profile or tag.
    pub search: Option<String>,
    pub folder_id: Option<FolderId>,
    pub tag: Option<String>,
    pub family: Option<EngineFamily>,
    #[serde(default)]
    pub favorites_only: bool,
}

/// Last known health of a connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionHealth {
    pub state: ConnectionState,
    pub latency_ms: Option<u64>,
    pub checked_at: DateTime<Utc>,
}

/// Secrets to copy when a connection is duplicated: `(from, to)`.
pub type SecretCopyPlan = Vec<(SecretRef, SecretRef)>;

#[derive(Debug, Clone)]
pub struct ConnectionRepo<'a> {
    store: &'a MetadataStore,
}

impl MetadataStore {
    pub fn connections(&self) -> ConnectionRepo<'_> {
        ConnectionRepo { store: self }
    }
}

/// Every secret reference in a profile, keyed by a stable field name.
pub fn secret_fields(profile: &ConnectionProfile) -> Vec<(&'static str, &SecretRef)> {
    let mut out = Vec::new();
    for r in profile.settings.secret_refs() {
        out.push(("password", r));
    }
    if !profile.tls.client_key_passphrase.is_none() {
        out.push((
            "tls.client_key_passphrase",
            &profile.tls.client_key_passphrase,
        ));
    }
    if let Some(ssh) = &profile.ssh_tunnel {
        match &ssh.auth {
            SshAuth::Password { password } if !password.is_none() => {
                out.push(("ssh.password", password))
            }
            SshAuth::PrivateKey { passphrase, .. } if !passphrase.is_none() => {
                out.push(("ssh.key_passphrase", passphrase))
            }
            _ => {}
        }
    }
    out
}

fn locator(r: &SecretRef) -> String {
    match r {
        SecretRef::Keychain { service, account } => format!("{service}/{account}"),
        SecretRef::Session { key } => key.clone(),
        SecretRef::Env { var } => var.clone(),
        SecretRef::Prompt | SecretRef::None => String::new(),
    }
}

/// Rewrites keychain references so a duplicated profile never shares a
/// keychain entry with its original.
fn rekey_secrets(profile: &mut ConnectionProfile) -> SecretCopyPlan {
    let id = profile.id;
    let mut plan = Vec::new();
    let mut rekey = |r: &mut SecretRef, field: &str| {
        if let SecretRef::Keychain { .. } = r {
            let new = SecretRef::keychain_for(id, field);
            plan.push((r.clone(), new.clone()));
            *r = new;
        } else if let SecretRef::Session { .. } = r {
            let new = SecretRef::Session {
                key: format!("{id}:{field}"),
            };
            plan.push((r.clone(), new.clone()));
            *r = new;
        }
    };
    if let Some(p) = profile.settings.password_mut() {
        rekey(p, "password");
    }
    rekey(
        &mut profile.tls.client_key_passphrase,
        "tls.client_key_passphrase",
    );
    if let Some(ssh) = &mut profile.ssh_tunnel {
        match &mut ssh.auth {
            SshAuth::Password { password } => rekey(password, "ssh.password"),
            SshAuth::PrivateKey { passphrase, .. } => rekey(passphrase, "ssh.key_passphrase"),
            SshAuth::Agent => {}
        }
    }
    plan
}

const SELECT_COLUMNS: &str =
    "id, name, compatibility_profile, folder_id, settings_json, tls_json, ssh_json,
     read_only, color, favorite, created_at, updated_at, last_used_at";

impl ConnectionRepo<'_> {
    /// Inserts or updates a profile after validating it.
    pub async fn save(&self, profile: &ConnectionProfile) -> StorageResult<()> {
        profile
            .validate()
            .map_err(|e| StorageError::InvalidState(e.to_string()))?;
        let mut tx = self.store.pool().begin().await?;
        let updated_at = now();
        sqlx::query(
            "INSERT INTO connections (id, name, family, compatibility_profile, folder_id, settings_json, tls_json,
                 ssh_json, read_only, color, favorite, created_at, updated_at, last_used_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                 name = excluded.name, family = excluded.family,
                 compatibility_profile = excluded.compatibility_profile, folder_id = excluded.folder_id,
                 settings_json = excluded.settings_json, tls_json = excluded.tls_json,
                 ssh_json = excluded.ssh_json, read_only = excluded.read_only, color = excluded.color,
                 favorite = excluded.favorite, updated_at = excluded.updated_at",
        )
        .bind(profile.id.to_string())
        .bind(&profile.name)
        .bind(profile.family().as_str())
        .bind(&profile.compatibility_profile)
        .bind(profile.folder_id.map(|f| f.to_string()))
        .bind(serde_json::to_string(&profile.settings)?)
        .bind(serde_json::to_string(&profile.tls)?)
        .bind(profile.ssh_tunnel.as_ref().map(serde_json::to_string).transpose()?)
        .bind(profile.read_only)
        .bind(&profile.color)
        .bind(profile.favorite)
        .bind(profile.created_at)
        .bind(updated_at)
        .bind(profile.last_used_at)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM connection_tags WHERE connection_id = ?")
            .bind(profile.id.to_string())
            .execute(&mut *tx)
            .await?;
        for tag in &profile.tags {
            let tag = tag.trim();
            if tag.is_empty() {
                continue;
            }
            sqlx::query("INSERT OR IGNORE INTO connection_tags (connection_id, tag) VALUES (?, ?)")
                .bind(profile.id.to_string())
                .bind(tag)
                .execute(&mut *tx)
                .await?;
        }
        write_secret_index(&mut tx, profile).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get(&self, id: ConnectionId) -> StorageResult<Option<ConnectionProfile>> {
        let sql = format!("SELECT {SELECT_COLUMNS} FROM connections WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(id.to_string())
            .fetch_optional(self.store.pool())
            .await?;
        match row {
            Some(row) => {
                let mut p = row_to_profile(&row)?;
                p.tags = self.tags_of(id).await?;
                Ok(Some(p))
            }
            None => Ok(None),
        }
    }

    pub async fn require(&self, id: ConnectionId) -> StorageResult<ConnectionProfile> {
        self.get(id)
            .await?
            .ok_or(StorageError::NotFound("connection"))
    }

    /// Finds a connection by exact name (case-insensitive), for the CLI.
    pub async fn find_by_name(&self, name: &str) -> StorageResult<Option<ConnectionProfile>> {
        let id: Option<String> =
            sqlx::query_scalar("SELECT id FROM connections WHERE name = ? COLLATE NOCASE LIMIT 1")
                .bind(name)
                .fetch_optional(self.store.pool())
                .await?;
        match id {
            Some(id) => self.get(parse_id(&id, "connection")?).await,
            None => Ok(None),
        }
    }

    pub async fn list(&self, filter: &ConnectionFilter) -> StorageResult<Vec<ConnectionProfile>> {
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM connections c
             WHERE (?1 IS NULL OR c.folder_id = ?1)
               AND (?2 IS NULL OR c.family = ?2)
               AND (?3 = 0 OR c.favorite = 1)
               AND (?4 IS NULL OR EXISTS (SELECT 1 FROM connection_tags t WHERE t.connection_id = c.id AND t.tag = ?4))
               AND (?5 IS NULL
                    OR instr(lower(c.name), lower(?5)) > 0
                    OR instr(lower(c.compatibility_profile), lower(?5)) > 0
                    OR EXISTS (SELECT 1 FROM connection_tags t WHERE t.connection_id = c.id AND instr(lower(t.tag), lower(?5)) > 0))
             ORDER BY c.favorite DESC, c.name COLLATE NOCASE"
        );
        let search = filter
            .search
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let rows = sqlx::query(&sql)
            .bind(filter.folder_id.map(|f| f.to_string()))
            .bind(filter.family.map(|f| f.as_str()))
            .bind(filter.favorites_only)
            .bind(filter.tag.as_deref())
            .bind(search)
            .fetch_all(self.store.pool())
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let mut p = row_to_profile(&row)?;
            p.tags = self.tags_of(p.id).await?;
            out.push(p);
        }
        Ok(out)
    }

    pub async fn recent(&self, limit: u32) -> StorageResult<Vec<ConnectionProfile>> {
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM connections WHERE last_used_at IS NOT NULL
             ORDER BY last_used_at DESC LIMIT ?"
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .fetch_all(self.store.pool())
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let mut p = row_to_profile(&row)?;
            p.tags = self.tags_of(p.id).await?;
            out.push(p);
        }
        Ok(out)
    }

    async fn tags_of(&self, id: ConnectionId) -> StorageResult<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT tag FROM connection_tags WHERE connection_id = ? ORDER BY tag",
        )
        .bind(id.to_string())
        .fetch_all(self.store.pool())
        .await?)
    }

    pub async fn all_tags(&self) -> StorageResult<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT DISTINCT tag FROM connection_tags ORDER BY tag")
                .fetch_all(self.store.pool())
                .await?,
        )
    }

    /// Deletes a connection and returns the secret references it held so
    /// the caller can remove them from the secret store.
    pub async fn delete(&self, id: ConnectionId) -> StorageResult<Vec<SecretRef>> {
        let profile = self.require(id).await?;
        let refs = secret_fields(&profile)
            .into_iter()
            .map(|(_, r)| r.clone())
            .collect();
        let mut tx = self.store.pool().begin().await?;
        sqlx::query("DELETE FROM secret_refs WHERE owner_kind = 'connection' AND owner_id = ?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM connections WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(refs)
    }

    /// Creates a copy with a new id and name. Secret references are re-keyed
    /// to the new id; the returned plan lists which secrets to copy.
    pub async fn duplicate(
        &self,
        id: ConnectionId,
        new_name: &str,
    ) -> StorageResult<(ConnectionProfile, SecretCopyPlan)> {
        let mut copy = self.require(id).await?;
        let ts = now();
        copy.id = ConnectionId::new();
        copy.name = new_name.to_owned();
        copy.created_at = ts;
        copy.updated_at = ts;
        copy.last_used_at = None;
        let plan = rekey_secrets(&mut copy);
        self.save(&copy).await?;
        Ok((copy, plan))
    }

    pub async fn touch(&self, id: ConnectionId) -> StorageResult<()> {
        sqlx::query("UPDATE connections SET last_used_at = ? WHERE id = ?")
            .bind(now())
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    pub async fn record_health(
        &self,
        id: ConnectionId,
        health: &ConnectionHealth,
    ) -> StorageResult<()> {
        let state = serde_json::to_value(health.state)?;
        sqlx::query("UPDATE connections SET last_state = ?, last_latency_ms = ?, last_checked_at = ? WHERE id = ?")
            .bind(state.as_str().unwrap_or_default().to_owned())
            .bind(health.latency_ms.map(|l| l as i64))
            .bind(health.checked_at)
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    pub async fn health(&self, id: ConnectionId) -> StorageResult<Option<ConnectionHealth>> {
        let row = sqlx::query(
            "SELECT last_state, last_latency_ms, last_checked_at FROM connections WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.store.pool())
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state: Option<String> = row.try_get("last_state")?;
        let checked: Option<DateTime<Utc>> = row.try_get("last_checked_at")?;
        match (state, checked) {
            (Some(state), Some(checked_at)) => Ok(Some(ConnectionHealth {
                state: serde_json::from_value(serde_json::Value::String(state))?,
                latency_ms: row
                    .try_get::<Option<i64>, _>("last_latency_ms")?
                    .map(|l| l.max(0) as u64),
                checked_at,
            })),
            _ => Ok(None),
        }
    }

    /// Indexed secret references for a connection: `(field, store, locator)`.
    pub async fn secret_index(
        &self,
        id: ConnectionId,
    ) -> StorageResult<Vec<(String, String, String)>> {
        let rows = sqlx::query(
            "SELECT field, store, locator FROM secret_refs WHERE owner_kind = 'connection' AND owner_id = ? ORDER BY field",
        )
        .bind(id.to_string())
        .fetch_all(self.store.pool())
        .await?;
        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get("field")?,
                    r.try_get("store")?,
                    r.try_get("locator")?,
                ))
            })
            .collect()
    }

    // ----------------------------------------------------------- folders

    pub async fn create_folder(
        &self,
        name: &str,
        parent_id: Option<FolderId>,
    ) -> StorageResult<Folder> {
        let folder = Folder {
            id: FolderId::new(),
            name: name.trim().to_owned(),
            parent_id,
            position: 0,
        };
        sqlx::query("INSERT INTO folders (id, name, parent_id, position, created_at) VALUES (?, ?, ?, ?, ?)")
            .bind(folder.id.to_string())
            .bind(&folder.name)
            .bind(parent_id.map(|p| p.to_string()))
            .bind(folder.position)
            .bind(now())
            .execute(self.store.pool())
            .await?;
        Ok(folder)
    }

    pub async fn rename_folder(&self, id: FolderId, name: &str) -> StorageResult<()> {
        let r = sqlx::query("UPDATE folders SET name = ? WHERE id = ?")
            .bind(name.trim())
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        if r.rows_affected() == 0 {
            return Err(StorageError::NotFound("folder"));
        }
        Ok(())
    }

    /// Deletes a folder and its sub-folders; contained connections move to
    /// the root (they are never deleted implicitly).
    pub async fn delete_folder(&self, id: FolderId) -> StorageResult<()> {
        sqlx::query("DELETE FROM folders WHERE id = ?")
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    pub async fn folders(&self) -> StorageResult<Vec<Folder>> {
        let rows = sqlx::query("SELECT id, name, parent_id, position FROM folders ORDER BY position, name COLLATE NOCASE")
            .fetch_all(self.store.pool())
            .await?;
        rows.iter()
            .map(|r| {
                let parent: Option<String> = r.try_get("parent_id")?;
                Ok(Folder {
                    id: parse_id(&r.try_get::<String, _>("id")?, "folder")?,
                    name: r.try_get("name")?,
                    parent_id: parent.map(|p| parse_id(&p, "folder")).transpose()?,
                    position: r.try_get("position")?,
                })
            })
            .collect()
    }
}

async fn write_secret_index(
    tx: &mut Transaction<'_, Sqlite>,
    profile: &ConnectionProfile,
) -> StorageResult<()> {
    sqlx::query("DELETE FROM secret_refs WHERE owner_kind = 'connection' AND owner_id = ?")
        .bind(profile.id.to_string())
        .execute(&mut **tx)
        .await?;
    for (field, r) in secret_fields(profile) {
        sqlx::query("INSERT INTO secret_refs (owner_kind, owner_id, field, store, locator) VALUES ('connection', ?, ?, ?, ?)")
            .bind(profile.id.to_string())
            .bind(field)
            .bind(r.kind())
            .bind(locator(r))
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

fn row_to_profile(row: &SqliteRow) -> StorageResult<ConnectionProfile> {
    let settings: ConnectionSettings =
        serde_json::from_str(&row.try_get::<String, _>("settings_json")?)?;
    let tls: TlsSettings = serde_json::from_str(&row.try_get::<String, _>("tls_json")?)?;
    let ssh: Option<String> = row.try_get("ssh_json")?;
    let ssh_tunnel: Option<SshTunnel> = ssh.map(|s| serde_json::from_str(&s)).transpose()?;
    let folder: Option<String> = row.try_get("folder_id")?;
    Ok(ConnectionProfile {
        id: parse_id(&row.try_get::<String, _>("id")?, "connection")?,
        name: row.try_get("name")?,
        compatibility_profile: row.try_get("compatibility_profile")?,
        settings,
        tls,
        ssh_tunnel,
        read_only: row.try_get("read_only")?,
        folder_id: folder.map(|f| parse_id(&f, "folder")).transpose()?,
        tags: Vec::new(),
        color: row.try_get("color")?,
        favorite: row.try_get("favorite")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        last_used_at: row.try_get("last_used_at")?,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use gazabridge_domain::connection::{
        PgSslMode, PostgresSettings, SqliteJournalMode, SqliteSettings,
    };

    use super::*;

    fn pg(name: &str) -> ConnectionProfile {
        let mut p = ConnectionProfile::new(
            name,
            "postgres.postgresql",
            ConnectionSettings::Postgres(PostgresSettings {
                host: "db.example.test".into(),
                port: 5432,
                database: "app".into(),
                username: "app_user".into(),
                password: SecretRef::None,
                ssl_mode: PgSslMode::VerifyFull,
                search_path: None,
                application_name: "GazaBridge".into(),
                connect_timeout_secs: 10,
                statement_timeout_ms: 0,
                pool_size: 4,
            }),
        );
        let id = p.id;
        if let Some(pw) = p.settings.password_mut() {
            *pw = SecretRef::keychain_for(id, "password");
        }
        p.tags = vec!["prod".into(), "billing".into()];
        p
    }

    fn sqlite(name: &str) -> ConnectionProfile {
        ConnectionProfile::new(
            name,
            "sqlite.sqlite",
            ConnectionSettings::Sqlite(SqliteSettings {
                path: PathBuf::from("/tmp/x.db"),
                read_only: true,
                create_if_missing: false,
                busy_timeout_ms: 1000,
                journal_mode: SqliteJournalMode::Wal,
                foreign_keys: true,
            }),
        )
    }

    #[tokio::test]
    async fn save_get_list_delete() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let repo = store.connections();
        let p = pg("Billing DB");
        repo.save(&p).await.unwrap();
        repo.save(&sqlite("Local cache")).await.unwrap();

        let loaded = repo.require(p.id).await.unwrap();
        assert_eq!(loaded.settings, p.settings);
        assert_eq!(loaded.tags, vec!["billing".to_string(), "prod".to_string()]);

        assert_eq!(
            repo.list(&ConnectionFilter::default()).await.unwrap().len(),
            2
        );
        let by_tag = repo
            .list(&ConnectionFilter {
                tag: Some("prod".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_tag.len(), 1);
        let by_search = repo
            .list(&ConnectionFilter {
                search: Some("BILL".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_search.len(), 1);
        let by_family = repo
            .list(&ConnectionFilter {
                family: Some(EngineFamily::Sqlite),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_family[0].name, "Local cache");

        let index = repo.secret_index(p.id).await.unwrap();
        assert_eq!(index.len(), 1);
        assert_eq!(index[0].0, "password");
        assert_eq!(index[0].1, "keychain");

        let refs = repo.delete(p.id).await.unwrap();
        assert_eq!(refs, vec![SecretRef::keychain_for(p.id, "password")]);
        assert!(repo.get(p.id).await.unwrap().is_none());
        assert!(repo.secret_index(p.id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn plaintext_password_is_rejected_by_the_schema() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let p = pg("x");
        let mut settings = serde_json::to_value(&p.settings).unwrap();
        settings["password"] = serde_json::Value::String("hunter2".into());
        let result = sqlx::query(
            "INSERT INTO connections (id, name, family, compatibility_profile, settings_json, tls_json, created_at, updated_at)
             VALUES ('x', 'x', 'postgres', 'postgres.postgresql', ?, '{}', 'now', 'now')",
        )
        .bind(settings.to_string())
        .execute(store.pool())
        .await;
        assert!(result.is_err(), "schema must reject plaintext password");
    }

    #[tokio::test]
    async fn stored_rows_contain_no_secret_values() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let p = pg("y");
        store.connections().save(&p).await.unwrap();
        let raw: String = sqlx::query_scalar("SELECT settings_json || tls_json FROM connections")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!(raw.contains(r#""store":"keychain""#));
    }

    #[tokio::test]
    async fn duplicate_rekeys_secrets() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let repo = store.connections();
        let p = pg("Original");
        repo.save(&p).await.unwrap();
        let (copy, plan) = repo.duplicate(p.id, "Copy").await.unwrap();
        assert_ne!(copy.id, p.id);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].0, SecretRef::keychain_for(p.id, "password"));
        assert_eq!(plan[0].1, SecretRef::keychain_for(copy.id, "password"));
        let mut expected = p.tags.clone();
        expected.sort();
        assert_eq!(copy.tags, expected);
    }

    #[tokio::test]
    async fn folders_and_recent_and_health() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let repo = store.connections();
        let folder = repo.create_folder("Production", None).await.unwrap();
        let mut p = pg("in folder");
        p.folder_id = Some(folder.id);
        repo.save(&p).await.unwrap();
        assert_eq!(
            repo.list(&ConnectionFilter {
                folder_id: Some(folder.id),
                ..Default::default()
            })
            .await
            .unwrap()
            .len(),
            1
        );

        repo.delete_folder(folder.id).await.unwrap();
        assert_eq!(
            repo.require(p.id).await.unwrap().folder_id,
            None,
            "connection survives folder deletion"
        );

        assert!(repo.recent(5).await.unwrap().is_empty());
        repo.touch(p.id).await.unwrap();
        assert_eq!(repo.recent(5).await.unwrap().len(), 1);

        let health = ConnectionHealth {
            state: ConnectionState::Ready,
            latency_ms: Some(12),
            checked_at: Utc::now(),
        };
        repo.record_health(p.id, &health).await.unwrap();
        let h = repo.health(p.id).await.unwrap().unwrap();
        assert_eq!(h.state, ConnectionState::Ready);
        assert_eq!(h.latency_ms, Some(12));
    }

    #[tokio::test]
    async fn invalid_profile_is_not_saved() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let mut p = pg("bad");
        p.name = "  ".into();
        assert!(store.connections().save(&p).await.is_err());
    }
}
