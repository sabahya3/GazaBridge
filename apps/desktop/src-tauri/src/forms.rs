//! Conversion between connection editor forms and connection profiles.

use std::path::PathBuf;

use gazabridge_application::{PasswordInput, TestOutcome};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, MySqlSettings,
    PostgresSettings, SecretRef, SqliteSettings, TlsSettings,
};
use gazabridge_ipc::{
    ConnectionForm, EngineForm, FindingDto, MongoForm, MySqlForm, PasswordChoice, PasswordMode,
    PostgresForm, SqliteForm, TestResultDto, TlsForm,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct FormError(pub String);

fn parse_enum<T: DeserializeOwned>(field: &str, value: &str) -> Result<T, FormError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| FormError(format!("`{value}` is not a valid {field}")))
}

fn enum_name<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn opt(s: &Option<String>) -> Option<String> {
    s.as_ref()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

fn host_port(entry: &str) -> Result<HostPort, FormError> {
    let entry = entry.trim();
    let (host, port) = match entry.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => (
            h.trim_matches(['[', ']']).to_owned(),
            p.parse()
                .map_err(|_| FormError(format!("invalid port in `{entry}`")))?,
        ),
        _ => (entry.to_owned(), 27017),
    };
    if host.is_empty() {
        return Err(FormError("host must not be empty".into()));
    }
    Ok(HostPort { host, port })
}

fn settings_from(
    engine: &EngineForm,
    password: SecretRef,
) -> Result<ConnectionSettings, FormError> {
    Ok(match engine {
        EngineForm::Postgres(f) => ConnectionSettings::Postgres(PostgresSettings {
            host: f.host.trim().to_owned(),
            port: f.port,
            database: f.database.trim().to_owned(),
            username: f.username.trim().to_owned(),
            password,
            ssl_mode: parse_enum("SSL mode", &f.ssl_mode)?,
            search_path: opt(&f.search_path),
            application_name: f.application_name.clone(),
            connect_timeout_secs: f.connect_timeout_secs,
            statement_timeout_ms: f.statement_timeout_ms,
            pool_size: f.pool_size,
        }),
        EngineForm::MySql(f) => ConnectionSettings::MySql(MySqlSettings {
            host: f.host.trim().to_owned(),
            port: f.port,
            database: opt(&f.database),
            username: f.username.trim().to_owned(),
            password,
            tls_mode: parse_enum("TLS mode", &f.tls_mode)?,
            charset: f.charset.clone(),
            connect_timeout_secs: f.connect_timeout_secs,
            query_timeout_ms: f.query_timeout_ms,
            pool_size: f.pool_size,
        }),
        EngineForm::MongoDb(f) => ConnectionSettings::MongoDb(MongoSettings {
            uri: opt(&f.uri),
            hosts: f
                .hosts
                .iter()
                .filter(|h| !h.trim().is_empty())
                .map(|h| host_port(h))
                .collect::<Result<_, _>>()?,
            replica_set: opt(&f.replica_set),
            auth_source: opt(&f.auth_source),
            username: opt(&f.username),
            password,
            default_database: opt(&f.default_database),
            tls: f.tls,
            read_preference: parse_enum("read preference", &f.read_preference)?,
            direct_connection: f.direct_connection,
            server_selection_timeout_ms: f.server_selection_timeout_ms,
            connect_timeout_ms: f.connect_timeout_ms,
        }),
        EngineForm::Sqlite(f) => ConnectionSettings::Sqlite(SqliteSettings {
            path: PathBuf::from(f.path.trim()),
            read_only: f.read_only,
            create_if_missing: f.create_if_missing,
            busy_timeout_ms: f.busy_timeout_ms,
            journal_mode: parse_enum("journal mode", &f.journal_mode)?,
            foreign_keys: f.foreign_keys,
        }),
    })
}

fn tls_from(t: &TlsForm) -> TlsSettings {
    TlsSettings {
        ca_certificate: opt(&t.ca_certificate).map(PathBuf::from),
        client_certificate: opt(&t.client_certificate).map(PathBuf::from),
        client_key: opt(&t.client_key).map(PathBuf::from),
        client_key_passphrase: SecretRef::None,
        server_name: opt(&t.server_name),
    }
}

/// Builds a profile from a form. `existing` supplies the id, timestamps and
/// secret references of the connection being edited.
pub fn profile_from(
    form: &ConnectionForm,
    existing: Option<&ConnectionProfile>,
) -> Result<ConnectionProfile, FormError> {
    let password = existing
        .and_then(|p| p.settings.secret_refs().first().map(|r| (*r).clone()))
        .unwrap_or_default();
    let settings = settings_from(&form.engine, password)?;
    let mut p = match existing {
        Some(e) => {
            let mut p = e.clone();
            p.name = form.name.trim().to_owned();
            p.compatibility_profile = form.profile_id.clone();
            p.settings = settings;
            p
        }
        None => ConnectionProfile::new(form.name.trim(), form.profile_id.clone(), settings),
    };
    let passphrase = p.tls.client_key_passphrase.clone();
    p.tls = tls_from(&form.tls);
    p.tls.client_key_passphrase = passphrase;
    p.read_only = form.read_only;
    p.favorite = form.favorite;
    p.color = opt(&form.color);
    p.tags = form
        .tags
        .iter()
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
        .collect();
    p.folder_id = match opt(&form.folder_id) {
        Some(f) => Some(
            f.parse()
                .map_err(|_| FormError("invalid folder id".into()))?,
        ),
        None => None,
    };
    Ok(p)
}

pub fn form_from(p: &ConnectionProfile) -> ConnectionForm {
    let engine = match &p.settings {
        ConnectionSettings::Postgres(s) => EngineForm::Postgres(PostgresForm {
            host: s.host.clone(),
            port: s.port,
            database: s.database.clone(),
            username: s.username.clone(),
            ssl_mode: enum_name(&s.ssl_mode),
            search_path: s.search_path.clone(),
            application_name: s.application_name.clone(),
            connect_timeout_secs: s.connect_timeout_secs,
            statement_timeout_ms: s.statement_timeout_ms,
            pool_size: s.pool_size,
        }),
        ConnectionSettings::MySql(s) => EngineForm::MySql(MySqlForm {
            host: s.host.clone(),
            port: s.port,
            database: s.database.clone(),
            username: s.username.clone(),
            tls_mode: enum_name(&s.tls_mode),
            charset: s.charset.clone(),
            connect_timeout_secs: s.connect_timeout_secs,
            query_timeout_ms: s.query_timeout_ms,
            pool_size: s.pool_size,
        }),
        ConnectionSettings::MongoDb(s) => EngineForm::MongoDb(MongoForm {
            uri: s.uri.clone(),
            hosts: s
                .hosts
                .iter()
                .map(|h| format!("{}:{}", h.host, h.port))
                .collect(),
            replica_set: s.replica_set.clone(),
            auth_source: s.auth_source.clone(),
            username: s.username.clone(),
            default_database: s.default_database.clone(),
            tls: s.tls,
            read_preference: enum_name(&s.read_preference),
            direct_connection: s.direct_connection,
            server_selection_timeout_ms: s.server_selection_timeout_ms,
            connect_timeout_ms: s.connect_timeout_ms,
        }),
        ConnectionSettings::Sqlite(s) => EngineForm::Sqlite(SqliteForm {
            path: s.path.display().to_string(),
            read_only: s.read_only,
            create_if_missing: s.create_if_missing,
            busy_timeout_ms: s.busy_timeout_ms,
            journal_mode: enum_name(&s.journal_mode),
            foreign_keys: s.foreign_keys,
        }),
    };
    let path = |o: &Option<PathBuf>| o.as_ref().map(|p| p.display().to_string());
    ConnectionForm {
        id: Some(p.id.to_string()),
        name: p.name.clone(),
        profile_id: p.compatibility_profile.clone(),
        engine,
        tls: TlsForm {
            ca_certificate: path(&p.tls.ca_certificate),
            client_certificate: path(&p.tls.client_certificate),
            client_key: path(&p.tls.client_key),
            server_name: p.tls.server_name.clone(),
        },
        read_only: p.read_only,
        favorite: p.favorite,
        color: p.color.clone(),
        tags: p.tags.clone(),
        folder_id: p.folder_id.map(|f| f.to_string()),
    }
}

pub fn password_mode(p: &ConnectionProfile) -> PasswordMode {
    match p.settings.secret_refs().first() {
        Some(SecretRef::Keychain { .. }) => PasswordMode::Keychain,
        Some(SecretRef::Session { .. }) => PasswordMode::Session,
        Some(SecretRef::Env { var }) => PasswordMode::Env { var: var.clone() },
        Some(SecretRef::Prompt) => PasswordMode::Prompt,
        _ => PasswordMode::None,
    }
}

pub fn password_input(choice: &PasswordChoice) -> PasswordInput {
    match choice {
        PasswordChoice::Keep => PasswordInput::Keep,
        PasswordChoice::None => PasswordInput::None,
        PasswordChoice::Remember { password } => {
            PasswordInput::Remember(Zeroizing::new(password.clone()))
        }
        PasswordChoice::SessionOnly { password } => {
            PasswordInput::SessionOnly(Zeroizing::new(password.clone()))
        }
        PasswordChoice::FromEnv { var } => PasswordInput::FromEnv(var.clone()),
        PasswordChoice::AskEachTime => PasswordInput::AskEachTime,
    }
}

pub fn test_result(o: &TestOutcome) -> TestResultDto {
    TestResultDto {
        state: enum_name(&o.state),
        state_label: o.state.label().to_owned(),
        latency_ms: o.latency_ms,
        server_product: o.server.as_ref().map(|s| s.product.clone()),
        server_version: o.server.as_ref().map(|s| s.version.to_string()),
        detected_profile: o.detected_profile.clone(),
        status: o.assessment.as_ref().map(|a| a.status.label().to_owned()),
        findings: o
            .assessment
            .as_ref()
            .map(|a| {
                a.findings
                    .iter()
                    .map(|f| FindingDto {
                        severity: enum_name(&f.severity),
                        message: f.message.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        error_code: o.error_code.clone(),
        error: o.error.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn form(engine: EngineForm, profile: &str) -> ConnectionForm {
        ConnectionForm {
            id: None,
            name: " Billing ".into(),
            profile_id: profile.into(),
            engine,
            tls: TlsForm {
                ca_certificate: Some("  ".into()),
                ..Default::default()
            },
            read_only: true,
            favorite: false,
            color: Some("#14532D".into()),
            tags: vec!["prod".into(), " ".into()],
            folder_id: None,
        }
    }

    #[test]
    fn every_engine_round_trips_through_the_form() {
        for (family, profile) in [
            ("postgres", "postgres.postgresql"),
            ("mysql", "mysql.mysql"),
            ("mongodb", "mongodb.community"),
            ("sqlite", "sqlite.sqlite"),
        ] {
            let mut engine = EngineForm::default_for(family).unwrap();
            if let EngineForm::Sqlite(s) = &mut engine {
                s.path = "/tmp/x.db".into();
            }
            if let EngineForm::Postgres(p) = &mut engine {
                p.username = "app".into();
            }
            if let EngineForm::MySql(m) = &mut engine {
                m.username = "app".into();
            }
            let f = form(engine, profile);
            let p = profile_from(&f, None).unwrap();
            p.validate().unwrap();
            assert_eq!(p.name, "Billing");
            assert_eq!(p.tags, vec!["prod".to_string()]);
            assert!(p.tls.ca_certificate.is_none(), "blank paths are dropped");
            let back = form_from(&p);
            assert_eq!(back.engine, f.engine, "{family}");
        }
    }

    #[test]
    fn invalid_enumerations_and_hosts_are_reported() {
        let mut engine = EngineForm::default_for("postgres").unwrap();
        if let EngineForm::Postgres(p) = &mut engine {
            p.ssl_mode = "maybe".into();
        }
        assert!(
            profile_from(&form(engine, "postgres.postgresql"), None)
                .unwrap_err()
                .0
                .contains("SSL mode")
        );
        assert!(host_port("db.example:notaport").is_err());
        assert_eq!(host_port("db.example").unwrap().port, 27017);
        assert_eq!(
            host_port("[::1]:27018").unwrap(),
            HostPort {
                host: "::1".into(),
                port: 27018
            }
        );
    }

    #[test]
    fn editing_keeps_identity_and_secret_references() {
        let engine = EngineForm::default_for("postgres").unwrap();
        let mut original =
            profile_from(&form(engine.clone(), "postgres.postgresql"), None).unwrap();
        let id = original.id;
        if let Some(slot) = original.settings.password_mut() {
            *slot = SecretRef::keychain_for(id, "password");
        }
        let edited = profile_from(&form(engine, "postgres.neon"), Some(&original)).unwrap();
        assert_eq!(edited.id, id);
        assert_eq!(edited.created_at, original.created_at);
        assert_eq!(
            edited.settings.secret_refs()[0],
            &SecretRef::keychain_for(id, "password")
        );
        assert_eq!(password_mode(&edited), PasswordMode::Keychain);
    }
}
