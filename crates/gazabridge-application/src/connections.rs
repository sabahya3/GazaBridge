//! Connection management: create, edit, duplicate, delete, test and open.
//!
//! Secrets go straight from the caller to the secret store; profiles only
//! ever hold [`SecretRef`]s. Every mutation is written to the audit log.

use std::sync::Arc;

use chrono::Utc;
use gazabridge_connectors::ConnectorRegistry;
use gazabridge_domain::compatibility::{CompatibilityStatus, ConnectionState};
use gazabridge_domain::connection::{ConnectionProfile, ConnectionSettings, SecretRef, SshAuth};
use gazabridge_domain::connection_string;
use gazabridge_domain::ids::ConnectionId;
use gazabridge_plugin_sdk::profile::CompatibilityAssessment;
use gazabridge_plugin_sdk::{
    CapabilityManifest, Connection, ErrorKind, ResolvedSecrets, ServerInfo,
};
use gazabridge_secrets::{SecretError, SecretResolver, SecretString};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::audit::NewAuditEvent;
use gazabridge_storage::connections::ConnectionHealth;
use gazabridge_storage::connector_meta::ConnectorMetadata;
use serde::Serialize;
use zeroize::Zeroizing;

use crate::profiled::ProfiledConnection;
use crate::{AppError, AppResult};

/// How the primary password of a connection should be handled.
pub enum PasswordInput {
    /// Leave the stored password reference unchanged.
    Keep,
    /// The connection has no password.
    None,
    /// Store in the OS keychain.
    Remember(Zeroizing<String>),
    /// Keep in memory for this session only.
    SessionOnly(Zeroizing<String>),
    /// Read from an environment variable at connect time.
    FromEnv(String),
    /// Ask every time; nothing is stored.
    AskEachTime,
}

impl std::fmt::Debug for PasswordInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Keep => "Keep",
            Self::None => "None",
            Self::Remember(_) => "Remember(<redacted>)",
            Self::SessionOnly(_) => "SessionOnly(<redacted>)",
            Self::FromEnv(_) => "FromEnv",
            Self::AskEachTime => "AskEachTime",
        })
    }
}

/// Result of testing a connection. Failures are outcomes, not errors, so
/// the UI can show the state and message.
#[derive(Debug, Clone, Serialize)]
pub struct TestOutcome {
    pub state: ConnectionState,
    pub latency_ms: Option<u64>,
    pub server: Option<ServerInfo>,
    /// Profile that best matches the detected server.
    pub detected_profile: Option<String>,
    pub assessment: Option<CompatibilityAssessment>,
    pub manifest: Option<CapabilityManifest>,
    pub error_code: Option<String>,
    pub error: Option<String>,
}

impl TestOutcome {
    fn failed(state: ConnectionState, code: &str, message: String) -> Self {
        Self {
            state,
            latency_ms: None,
            server: None,
            detected_profile: None,
            assessment: None,
            manifest: None,
            error_code: Some(code.to_owned()),
            error: Some(message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionService {
    store: MetadataStore,
    registry: ConnectorRegistry,
    secrets: SecretResolver,
}

fn host_of(settings: &ConnectionSettings) -> Option<String> {
    match settings {
        ConnectionSettings::Postgres(s) => Some(s.host.clone()),
        ConnectionSettings::MySql(s) => Some(s.host.clone()),
        ConnectionSettings::MongoDb(s) => s.hosts.first().map(|h| h.host.clone()).or_else(|| {
            s.uri.as_deref().and_then(|u| {
                let after = u.split_once("://")?.1;
                let hosts = after.rsplit_once('@').map_or(after, |(_, h)| h);
                Some(hosts.split(['/', '?', ',', ':']).next()?.to_owned())
            })
        }),
        ConnectionSettings::Sqlite(_) => None,
    }
}

/// Mutable references to every secret slot of a profile.
fn secret_slots_mut(profile: &mut ConnectionProfile) -> Vec<&mut SecretRef> {
    let mut out = Vec::new();
    if let Some(p) = profile.settings.password_mut() {
        out.push(p);
    }
    out.push(&mut profile.tls.client_key_passphrase);
    if let Some(ssh) = &mut profile.ssh_tunnel {
        match &mut ssh.auth {
            SshAuth::Password { password } => out.push(password),
            SshAuth::PrivateKey { passphrase, .. } => out.push(passphrase),
            SshAuth::Agent => {}
        }
    }
    out
}

fn state_for_status(status: CompatibilityStatus) -> ConnectionState {
    match status {
        CompatibilityStatus::UnsupportedVersion => ConnectionState::UnsupportedVersion,
        CompatibilityStatus::LimitedCompatibility => ConnectionState::LimitedCompatibility,
        _ => ConnectionState::Ready,
    }
}

impl ConnectionService {
    pub fn new(store: MetadataStore, registry: ConnectorRegistry, secrets: SecretResolver) -> Self {
        Self {
            store,
            registry,
            secrets,
        }
    }

    pub fn registry(&self) -> &ConnectorRegistry {
        &self.registry
    }

    /// Redactor that knows every secret resolved so far.
    pub fn redactor(&self) -> &gazabridge_secrets::Redactor {
        self.secrets.redactor()
    }

    // ----------------------------------------------------------- secrets

    async fn put_secret(&self, r: SecretRef, value: SecretString) -> AppResult<()> {
        let resolver = self.secrets.clone();
        tokio::task::spawn_blocking(move || resolver.store(&r, &value))
            .await
            .map_err(|e| AppError::Invalid(format!("secret task failed: {e}")))??;
        Ok(())
    }

    async fn drop_secret(&self, r: SecretRef) -> AppResult<()> {
        let resolver = self.secrets.clone();
        tokio::task::spawn_blocking(move || resolver.delete(&r))
            .await
            .map_err(|e| AppError::Invalid(format!("secret task failed: {e}")))??;
        Ok(())
    }

    async fn get_secret(&self, r: SecretRef) -> AppResult<Option<SecretString>> {
        let resolver = self.secrets.clone();
        Ok(tokio::task::spawn_blocking(move || resolver.resolve(&r))
            .await
            .map_err(|e| AppError::Invalid(format!("secret task failed: {e}")))??)
    }

    /// Resolves every secret a profile references.
    pub async fn resolve_secrets(&self, profile: &ConnectionProfile) -> AppResult<ResolvedSecrets> {
        let mut out = ResolvedSecrets::default();
        if let Some(r) = profile.settings.secret_refs().first() {
            out.password = self.get_secret((*r).clone()).await?;
        }
        if !profile.tls.client_key_passphrase.is_none() {
            out.client_key_passphrase = self
                .get_secret(profile.tls.client_key_passphrase.clone())
                .await?;
        }
        if let Some(ssh) = &profile.ssh_tunnel {
            match &ssh.auth {
                SshAuth::Password { password } => {
                    out.ssh_password = self.get_secret(password.clone()).await?
                }
                SshAuth::PrivateKey { passphrase, .. } if !passphrase.is_none() => {
                    out.ssh_key_passphrase = self.get_secret(passphrase.clone()).await?;
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// Applies a password choice to the profile and returns the secret to
    /// store (if any) under the new reference.
    fn apply_password(
        profile: &mut ConnectionProfile,
        input: PasswordInput,
    ) -> AppResult<Option<(SecretRef, SecretString)>> {
        let id = profile.id;
        let Some(slot) = profile.settings.password_mut() else {
            return match input {
                PasswordInput::Keep | PasswordInput::None => Ok(None),
                _ => Err(AppError::Invalid(
                    "this connection type has no password".into(),
                )),
            };
        };
        Ok(match input {
            PasswordInput::Keep => None,
            PasswordInput::None => {
                *slot = SecretRef::None;
                None
            }
            PasswordInput::AskEachTime => {
                *slot = SecretRef::Prompt;
                None
            }
            PasswordInput::FromEnv(var) => {
                if var.trim().is_empty() {
                    return Err(AppError::Invalid(
                        "environment variable name must not be empty".into(),
                    ));
                }
                *slot = SecretRef::Env { var };
                None
            }
            PasswordInput::Remember(value) => {
                let r = SecretRef::keychain_for(id, "password");
                *slot = r.clone();
                Some((r, value))
            }
            PasswordInput::SessionOnly(value) => {
                let r = SecretRef::Session {
                    key: format!("{id}:password"),
                };
                *slot = r.clone();
                Some((r, value))
            }
        })
    }

    async fn audit(
        &self,
        action: &str,
        profile: &ConnectionProfile,
        summary: String,
    ) -> AppResult<()> {
        self.store
            .audit()
            .record(NewAuditEvent {
                action,
                target_kind: "connection",
                target_id: Some(profile.id.to_string()),
                summary,
                details: serde_json::json!({
                    "engine": profile.family().as_str(),
                    "profile": profile.compatibility_profile,
                    "target": self.secrets.redactor().scrub(&profile.settings.display_target()),
                }),
                correlation_id: None,
            })
            .await?;
        Ok(())
    }

    // ------------------------------------------------------------- CRUD

    pub async fn create(
        &self,
        mut profile: ConnectionProfile,
        password: PasswordInput,
    ) -> AppResult<ConnectionProfile> {
        if self.store.connections().get(profile.id).await?.is_some() {
            return Err(AppError::Invalid(
                "a connection with this id already exists".into(),
            ));
        }
        self.registry.profile(&profile.compatibility_profile)?;
        let secret = Self::apply_password(&mut profile, password)?;
        profile
            .validate()
            .map_err(|e| AppError::Invalid(e.to_string()))?;
        if let Some((r, v)) = secret.clone() {
            self.put_secret(r, v).await?;
        }
        if let Err(e) = self.store.connections().save(&profile).await {
            if let Some((r, _)) = secret {
                let _ = self.drop_secret(r).await;
            }
            return Err(e.into());
        }
        self.audit(
            "connection.create",
            &profile,
            format!("created connection `{}`", profile.name),
        )
        .await?;
        Ok(profile)
    }

    /// Creates a connection from a pasted connection string. The password in
    /// the string (if any) is stored per `remember` and never persisted in
    /// the profile.
    pub async fn create_from_string(
        &self,
        name: &str,
        text: &str,
        remember: bool,
    ) -> AppResult<(ConnectionProfile, Vec<String>)> {
        let parsed =
            connection_string::parse(text).map_err(|e| AppError::Invalid(e.to_string()))?;
        let profile = ConnectionProfile::new(name, parsed.compatibility_profile, parsed.settings);
        let input = match parsed.password {
            Some(pw) if remember => PasswordInput::Remember(pw),
            Some(pw) => PasswordInput::SessionOnly(pw),
            None => PasswordInput::None,
        };
        Ok((self.create(profile, input).await?, parsed.warnings))
    }

    pub async fn update(
        &self,
        mut profile: ConnectionProfile,
        password: PasswordInput,
    ) -> AppResult<ConnectionProfile> {
        let existing = self.store.connections().require(profile.id).await?;
        if existing.family() != profile.family() {
            return Err(AppError::Invalid(
                "the engine of an existing connection cannot change".into(),
            ));
        }
        self.registry.profile(&profile.compatibility_profile)?;
        let old_ref = existing
            .settings
            .secret_refs()
            .first()
            .map(|r| (*r).clone())
            .unwrap_or_default();
        if matches!(password, PasswordInput::Keep)
            && let Some(slot) = profile.settings.password_mut()
        {
            *slot = old_ref.clone();
        }
        let secret = Self::apply_password(&mut profile, password)?;
        profile.created_at = existing.created_at;
        profile
            .validate()
            .map_err(|e| AppError::Invalid(e.to_string()))?;
        if let Some((r, v)) = secret {
            self.put_secret(r, v).await?;
        }
        self.store.connections().save(&profile).await?;
        let new_ref = profile
            .settings
            .secret_refs()
            .first()
            .map(|r| (*r).clone())
            .unwrap_or_default();
        if new_ref != old_ref && !old_ref.is_none() {
            self.drop_secret(old_ref).await?;
        }
        self.audit(
            "connection.update",
            &profile,
            format!("updated connection `{}`", profile.name),
        )
        .await?;
        self.store
            .connections()
            .require(profile.id)
            .await
            .map_err(Into::into)
    }

    /// Duplicates a connection, copying its secrets under new references.
    /// Secrets that cannot be copied fall back to "ask each time".
    pub async fn duplicate(
        &self,
        id: ConnectionId,
        new_name: &str,
    ) -> AppResult<ConnectionProfile> {
        let (mut copy, plan) = self.store.connections().duplicate(id, new_name).await?;
        let mut failed = Vec::new();
        for (from, to) in plan {
            let copied = match self.get_secret(from).await {
                Ok(Some(v)) => self.put_secret(to.clone(), v).await.is_ok(),
                Ok(None) => true,
                Err(_) => false,
            };
            if !copied {
                tracing::warn!(connection = %copy.id, "a secret could not be copied; the copy will ask for it");
                failed.push(to);
            }
        }
        let fallback = !failed.is_empty();
        for slot in secret_slots_mut(&mut copy) {
            if failed.contains(slot) {
                *slot = SecretRef::Prompt;
            }
        }
        if fallback {
            self.store.connections().save(&copy).await?;
        }
        self.audit(
            "connection.duplicate",
            &copy,
            format!("duplicated connection as `{}`", copy.name),
        )
        .await?;
        Ok(copy)
    }

    /// Deletes a connection and every secret it referenced.
    pub async fn delete(&self, id: ConnectionId) -> AppResult<()> {
        let profile = self.store.connections().require(id).await?;
        let refs = self.store.connections().delete(id).await?;
        for r in refs {
            match self.drop_secret(r).await {
                Ok(()) | Err(AppError::Secret(SecretError::NotFound)) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "could not delete a secret of a deleted connection")
                }
            }
        }
        self.audit(
            "connection.delete",
            &profile,
            format!("deleted connection `{}`", profile.name),
        )
        .await
    }

    // --------------------------------------------------------- testing

    async fn connect_with(
        &self,
        profile: &ConnectionProfile,
        secrets: ResolvedSecrets,
    ) -> AppResult<Arc<dyn Connection>> {
        let catalog_profile = self
            .registry
            .profile(&profile.compatibility_profile)?
            .clone();
        let factory = self.registry.factory(profile.family())?.clone();
        let raw = factory.connect(profile, secrets).await?;
        Ok(Arc::new(ProfiledConnection::new(raw, &catalog_profile)))
    }

    async fn probe(
        &self,
        profile: &ConnectionProfile,
        secrets: AppResult<ResolvedSecrets>,
    ) -> TestOutcome {
        let secrets = match secrets {
            Ok(s) => s,
            Err(e) => {
                let state = if matches!(
                    e,
                    AppError::Secret(
                        SecretError::PromptRequired
                            | SecretError::NotFound
                            | SecretError::EnvMissing(_)
                    )
                ) {
                    ConnectionState::ConfigurationRequired
                } else {
                    ConnectionState::ConnectionFailed
                };
                return TestOutcome::failed(
                    state,
                    e.code(),
                    self.secrets.redactor().scrub(&e.to_string()),
                );
            }
        };
        let conn = match self.connect_with(profile, secrets).await {
            Ok(c) => c,
            Err(e) => {
                let state = match &e {
                    AppError::Connector(c) if c.kind() == ErrorKind::InvalidInput => {
                        ConnectionState::ConfigurationRequired
                    }
                    AppError::Invalid(_) | AppError::Registry(_) => {
                        ConnectionState::ConfigurationRequired
                    }
                    _ => ConnectionState::ConnectionFailed,
                };
                return TestOutcome::failed(
                    state,
                    e.code(),
                    self.secrets.redactor().scrub(&e.to_string()),
                );
            }
        };
        let outcome = async {
            let health = conn.health_check().await?;
            let server = conn.server_info().await?;
            let selected = self.registry.profile(&profile.compatibility_profile)?;
            let assessment = selected.assess(&server);
            let detected = self
                .registry
                .catalog()
                .detect(&server, host_of(&profile.settings).as_deref())
                .map(|p| p.id.clone());
            Ok::<_, AppError>(TestOutcome {
                state: state_for_status(assessment.status),
                latency_ms: Some(health.latency.as_millis() as u64),
                server: Some(server),
                detected_profile: detected,
                assessment: Some(assessment),
                manifest: Some(conn.manifest().clone()),
                error_code: None,
                error: None,
            })
        }
        .await;
        conn.close().await;
        outcome.unwrap_or_else(|e| {
            TestOutcome::failed(
                ConnectionState::ConnectionFailed,
                e.code(),
                self.secrets.redactor().scrub(&e.to_string()),
            )
        })
    }

    /// Tests an unsaved profile from the editor. `password` overrides the
    /// stored secret when given.
    pub async fn test_draft(
        &self,
        profile: &ConnectionProfile,
        password: Option<SecretString>,
    ) -> TestOutcome {
        let secrets = match password {
            Some(pw) => Ok(ResolvedSecrets {
                password: Some(pw),
                ..Default::default()
            }),
            None => self.resolve_secrets(profile).await,
        };
        self.probe(profile, secrets).await
    }

    /// Tests a saved connection and records its health and server facts.
    pub async fn test_saved(&self, id: ConnectionId) -> AppResult<TestOutcome> {
        let profile = self.store.connections().require(id).await?;
        let secrets = self.resolve_secrets(&profile).await;
        let outcome = self.probe(&profile, secrets).await;
        self.store
            .connections()
            .record_health(
                id,
                &ConnectionHealth {
                    state: outcome.state,
                    latency_ms: outcome.latency_ms,
                    checked_at: Utc::now(),
                },
            )
            .await?;
        if let (Some(server), Some(assessment), Some(manifest)) =
            (&outcome.server, &outcome.assessment, &outcome.manifest)
        {
            let status = serde_json::to_value(assessment.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            self.store
                .connector_metadata()
                .upsert(&ConnectorMetadata {
                    connection_id: id,
                    server_product: server.product.clone(),
                    server_version: server.version.to_string(),
                    detected_profile: outcome.detected_profile.clone(),
                    assessed_status: status,
                    findings: serde_json::to_value(&assessment.findings).unwrap_or_default(),
                    manifest: serde_json::to_value(manifest).unwrap_or_default(),
                    detected_at: Utc::now(),
                })
                .await?;
        }
        Ok(outcome)
    }

    /// Opens a saved connection with its compatibility profile enforced.
    pub async fn open(&self, id: ConnectionId) -> AppResult<Arc<dyn Connection>> {
        let profile = self.store.connections().require(id).await?;
        let secrets = self.resolve_secrets(&profile).await?;
        let conn = self.connect_with(&profile, secrets).await?;
        self.store.connections().touch(id).await?;
        Ok(conn)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use gazabridge_connectors::ProfileCatalog;
    use gazabridge_domain::connection::{
        PgSslMode, PostgresSettings, SqliteJournalMode, SqliteSettings,
    };
    use gazabridge_secrets::{Redactor, SessionStore};

    use super::*;

    async fn service() -> ConnectionService {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
        registry
            .register(Arc::new(
                gazabridge_connector_sqlite::SqliteConnectorFactory,
            ))
            .unwrap();
        // Session store stands in for the OS keychain in tests.
        let secrets = SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new());
        ConnectionService::new(store, registry, secrets)
    }

    fn sqlite_profile(path: &Path) -> ConnectionProfile {
        ConnectionProfile::new(
            "local",
            "sqlite.sqlite",
            ConnectionSettings::Sqlite(SqliteSettings {
                path: path.to_path_buf(),
                read_only: false,
                create_if_missing: true,
                busy_timeout_ms: 1000,
                journal_mode: SqliteJournalMode::Wal,
                foreign_keys: true,
            }),
        )
    }

    fn pg_profile() -> ConnectionProfile {
        ConnectionProfile::new(
            "pg",
            "postgres.postgresql",
            ConnectionSettings::Postgres(PostgresSettings {
                host: "127.0.0.1".into(),
                port: 1,
                database: "app".into(),
                username: "app".into(),
                password: SecretRef::None,
                ssl_mode: PgSslMode::Disable,
                search_path: None,
                application_name: "GazaBridge".into(),
                connect_timeout_secs: 1,
                statement_timeout_ms: 0,
                pool_size: 1,
            }),
        )
    }

    #[tokio::test]
    async fn sqlite_connection_lifecycle_records_health_and_server() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service().await;
        let p = svc
            .create(
                sqlite_profile(&dir.path().join("a.db")),
                PasswordInput::None,
            )
            .await
            .unwrap();
        let outcome = svc.test_saved(p.id).await.unwrap();
        assert_eq!(outcome.state, ConnectionState::Ready, "{outcome:?}");
        assert_eq!(outcome.server.as_ref().unwrap().product, "SQLite");
        assert_eq!(outcome.detected_profile.as_deref(), Some("sqlite.sqlite"));
        let meta = svc
            .store
            .connector_metadata()
            .get(p.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(meta.assessed_status, "beta");
        assert_eq!(
            svc.store
                .connections()
                .health(p.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            ConnectionState::Ready
        );

        let conn = svc.open(p.id).await.unwrap();
        assert!(conn.health_check().await.is_ok());
        conn.close().await;

        let copy = svc.duplicate(p.id, "copy").await.unwrap();
        assert_ne!(copy.id, p.id);
        svc.delete(p.id).await.unwrap();
        assert!(svc.store.connections().get(p.id).await.unwrap().is_none());
        let audit = svc
            .store
            .audit()
            .list(Some("connection"), None, 10)
            .await
            .unwrap();
        let actions: Vec<_> = audit.iter().map(|a| a.action.as_str()).collect();
        assert!(
            actions.contains(&"connection.create") && actions.contains(&"connection.delete"),
            "{actions:?}"
        );
    }

    #[tokio::test]
    async fn passwords_are_stored_by_reference_and_cleaned_up() {
        let svc = service().await;
        let p = svc
            .create(
                pg_profile(),
                PasswordInput::SessionOnly(Zeroizing::new("s3cret-value".into())),
            )
            .await
            .unwrap();
        let stored = svc.store.connections().require(p.id).await.unwrap();
        let r = stored.settings.secret_refs()[0].clone();
        assert!(matches!(r, SecretRef::Session { .. }));
        assert_eq!(
            svc.resolve_secrets(&stored)
                .await
                .unwrap()
                .password
                .unwrap()
                .as_str(),
            "s3cret-value"
        );

        // Raw metadata never contains the value.
        let raw: String = sqlx_dump(&svc).await;
        assert!(!raw.contains("s3cret-value"));

        // Replacing the password with "ask each time" deletes the old secret.
        let updated = svc
            .update(stored.clone(), PasswordInput::AskEachTime)
            .await
            .unwrap();
        assert_eq!(updated.settings.secret_refs()[0], &SecretRef::Prompt);
        assert!(matches!(
            svc.get_secret(r).await,
            Err(AppError::Secret(SecretError::NotFound))
        ));

        // Testing then reports "configuration required", not a crash.
        let outcome = svc.test_saved(p.id).await.unwrap();
        assert_eq!(outcome.state, ConnectionState::ConfigurationRequired);
        assert_eq!(outcome.error_code.as_deref(), Some("password_required"));
    }

    async fn sqlx_dump(svc: &ConnectionService) -> String {
        let p = svc
            .store
            .connections()
            .list(&Default::default())
            .await
            .unwrap();
        serde_json::to_string(&p).unwrap()
    }

    #[tokio::test]
    async fn unregistered_connector_is_configuration_required() {
        let svc = service().await;
        let p = svc.create(pg_profile(), PasswordInput::None).await.unwrap();
        let outcome = svc.test_saved(p.id).await.unwrap();
        assert_eq!(outcome.state, ConnectionState::ConfigurationRequired);
        assert_eq!(outcome.error_code.as_deref(), Some("connector_unavailable"));
    }

    #[tokio::test]
    async fn connection_strings_never_persist_passwords() {
        let svc = service().await;
        let (p, _) = svc
            .create_from_string(
                "from string",
                "postgresql://app:p%40ss@db.example.test:5432/app?sslmode=require",
                false,
            )
            .await
            .unwrap();
        assert_eq!(p.compatibility_profile, "postgres.postgresql");
        let stored = svc.store.connections().require(p.id).await.unwrap();
        assert!(!serde_json::to_string(&stored).unwrap().contains("p@ss"));
        assert_eq!(
            svc.resolve_secrets(&stored)
                .await
                .unwrap()
                .password
                .unwrap()
                .as_str(),
            "p@ss"
        );
        assert!(
            svc.create_from_string("bad", "nonsense", false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_profiles_and_secret_choices_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let svc = service().await;
        let err = svc
            .create(
                sqlite_profile(&dir.path().join("x.db")),
                PasswordInput::Remember(Zeroizing::new("x".into())),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), "invalid_input");
        let mut p = pg_profile();
        p.compatibility_profile = "postgres.does_not_exist".into();
        assert_eq!(
            svc.create(p, PasswordInput::None).await.unwrap_err().code(),
            "connector_unavailable"
        );
    }

    #[test]
    fn hosts_are_extracted_for_detection() {
        assert_eq!(
            host_of(&pg_profile().settings).as_deref(),
            Some("127.0.0.1")
        );
    }
}
