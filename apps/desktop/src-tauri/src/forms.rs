//! Conversion between connection editor forms and connection profiles.

use std::path::PathBuf;

use gazabridge_application::{PasswordInput, TestOutcome};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostKeyPolicy, HostPort, MongoSettings, MySqlSettings,
    PostgresSettings, SecretRef, SqliteSettings, SshAuth, SshTunnel, TlsSettings,
};
use gazabridge_ipc::{
    ConnectionForm, EngineForm, FindingDto, MongoForm, MySqlForm, PasswordChoice, PasswordMode,
    PostgresForm, SqliteForm, SshForm, TestResultDto, TlsForm,
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

/// The SSH tunnel of a form. The secret reference is kept from `existing`
/// when the sign-in method is unchanged; the service applies a new choice.
fn ssh_from(f: &SshForm, existing: Option<&SshTunnel>) -> Result<SshTunnel, FormError> {
    let host = f.host.trim();
    if host.is_empty() {
        return Err(FormError("enter the SSH server's host name".into()));
    }
    let username = f.username.trim();
    if username.is_empty() {
        return Err(FormError("enter the SSH user name".into()));
    }
    if f.port == 0 {
        return Err(FormError("the SSH port must be between 1 and 65535".into()));
    }
    let kept = |want_password: bool| match existing.map(|e| &e.auth) {
        Some(SshAuth::Password { password }) if want_password => password.clone(),
        Some(SshAuth::PrivateKey { passphrase, .. }) if !want_password => passphrase.clone(),
        _ => SecretRef::None,
    };
    let auth = match f.auth.as_str() {
        "password" => SshAuth::Password {
            password: kept(true),
        },
        "key" => SshAuth::PrivateKey {
            key_path: opt(&f.key_path)
                .map(PathBuf::from)
                .ok_or_else(|| FormError("choose the SSH private key file".into()))?,
            passphrase: kept(false),
        },
        "agent" => SshAuth::Agent,
        other => return Err(FormError(format!("`{other}` is not an SSH sign-in method"))),
    };
    Ok(SshTunnel {
        host: host.to_owned(),
        port: f.port,
        username: username.to_owned(),
        auth,
        host_key_policy: parse_enum::<HostKeyPolicy>("host key policy", &f.host_key_policy)?,
    })
}

fn ssh_form(t: &SshTunnel) -> SshForm {
    let (auth, key_path) = match &t.auth {
        SshAuth::Password { .. } => ("password", None),
        SshAuth::PrivateKey { key_path, .. } => ("key", Some(key_path.display().to_string())),
        SshAuth::Agent => ("agent", None),
    };
    SshForm {
        host: t.host.clone(),
        port: t.port,
        username: t.username.clone(),
        auth: auth.into(),
        key_path,
        host_key_policy: enum_name(&t.host_key_policy),
    }
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
/// Treats a MongoDB URI typed into the editor the way pasting a connection
/// string does: credentials leave the URI (the password is returned for
/// secure storage, never saved in settings) and the URI decides hosts, TLS
/// and the options it states; empty form fields are filled from it.
fn normalize_mongo(form: &mut MongoForm) -> Result<Option<Zeroizing<String>>, FormError> {
    let Some(uri) = opt(&form.uri) else {
        return Ok(None);
    };
    let parsed =
        gazabridge_domain::connection_string::parse(&uri).map_err(|e| FormError(e.to_string()))?;
    let ConnectionSettings::MongoDb(u) = parsed.settings else {
        return Err(FormError(
            "expected a mongodb:// or mongodb+srv:// URI".into(),
        ));
    };
    let keep = |from_uri: Option<String>, form: &Option<String>| from_uri.or_else(|| opt(form));
    form.uri = u.uri;
    form.hosts.clear();
    form.username = keep(u.username, &form.username);
    form.auth_source = keep(u.auth_source, &form.auth_source);
    form.replica_set = keep(u.replica_set, &form.replica_set);
    form.default_database = keep(u.default_database, &form.default_database);
    // A plain mongodb:// URI means no TLS unless it says tls=true.
    form.tls = u.tls;
    form.direct_connection = form.direct_connection || u.direct_connection;
    Ok(parsed.password)
}

/// Normalises the form and decides which password input applies. A
/// password typed in the password field wins over one inside a URI (the
/// editor's URI box or a MongoDB URI field).
pub fn prepare(
    form: &ConnectionForm,
    choice: &PasswordChoice,
    connection_string: Option<&str>,
) -> Result<(ConnectionForm, PasswordInput), FormError> {
    let mut form = form.clone();
    let mut from_uri = match &mut form.engine {
        EngineForm::MongoDb(m) => normalize_mongo(m)?,
        _ => None,
    };
    if from_uri.is_none()
        && let Some(text) = connection_string.map(str::trim).filter(|t| !t.is_empty())
    {
        from_uri = gazabridge_domain::connection_string::parse(text)
            .map_err(|e| FormError(e.to_string()))?
            .password;
    }
    let typed = matches!(choice,
        PasswordChoice::Remember { password } | PasswordChoice::SessionOnly { password } if !password.is_empty());
    let input = match from_uri {
        Some(pw) if !typed => match choice {
            PasswordChoice::SessionOnly { .. } => PasswordInput::SessionOnly(pw),
            _ => PasswordInput::Remember(pw),
        },
        _ => password_input(choice),
    };
    Ok((form, input))
}

/// Default name for a connection read from a string: `host:port`, or the
/// file name for SQLite.
pub fn suggested_name(settings: &ConnectionSettings) -> String {
    match settings {
        ConnectionSettings::Postgres(s) => format!("{}:{}", s.host, s.port),
        ConnectionSettings::MySql(s) => format!("{}:{}", s.host, s.port),
        ConnectionSettings::MongoDb(s) => match s.hosts.first() {
            Some(h) => format!("{}:{}", h.host, h.port),
            None => s
                .uri
                .as_deref()
                .and_then(|u| u.split_once("://"))
                .map(|(_, rest)| rest.split(['/', '?']).next().unwrap_or(rest).to_owned())
                .unwrap_or_else(|| "MongoDB".into()),
        },
        ConnectionSettings::Sqlite(s) => s
            .path
            .file_name()
            .map_or_else(|| "SQLite".into(), |n| n.to_string_lossy().into_owned()),
    }
}

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
    p.ssh_tunnel = match &form.ssh {
        Some(_) if matches!(p.settings, ConnectionSettings::Sqlite(_)) => {
            return Err(FormError(
                "SQLite files are opened directly; an SSH tunnel does not apply".into(),
            ));
        }
        Some(f) => Some(ssh_from(f, existing.and_then(|e| e.ssh_tunnel.as_ref()))?),
        None => None,
    };
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
        ssh: p.ssh_tunnel.as_ref().map(ssh_form),
        read_only: p.read_only,
        favorite: p.favorite,
        color: p.color.clone(),
        tags: p.tags.clone(),
        folder_id: p.folder_id.map(|f| f.to_string()),
    }
}

fn mode_of(r: Option<&SecretRef>) -> PasswordMode {
    match r {
        Some(SecretRef::Keychain { .. }) => PasswordMode::Keychain,
        Some(SecretRef::Session { .. }) => PasswordMode::Session,
        Some(SecretRef::Env { var }) => PasswordMode::Env { var: var.clone() },
        Some(SecretRef::Prompt) => PasswordMode::Prompt,
        _ => PasswordMode::None,
    }
}

pub fn password_mode(p: &ConnectionProfile) -> PasswordMode {
    mode_of(p.settings.secret_refs().first().copied())
}

/// How the SSH tunnel's password or key passphrase is stored; `None` without
/// a tunnel or with the SSH agent.
pub fn ssh_secret_mode(p: &ConnectionProfile) -> Option<PasswordMode> {
    match &p.ssh_tunnel.as_ref()?.auth {
        SshAuth::Password { password } => Some(mode_of(Some(password))),
        SshAuth::PrivateKey { passphrase, .. } => Some(mode_of(Some(passphrase))),
        SshAuth::Agent => None,
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
            ssh: None,
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

    #[test]
    fn a_uri_typed_in_the_editor_behaves_like_a_pasted_one() {
        let EngineForm::MongoDb(mut m) = EngineForm::default_for("mongodb").unwrap() else {
            unreachable!()
        };
        m.tls = true; // the form default must not override the URI
        m.uri = Some("mongodb://ahmed:secret-pw@localhost:27017/dev".into());
        let f = form(EngineForm::MongoDb(m), "mongodb.community");
        let (prepared, input) = prepare(&f, &PasswordChoice::Keep, None).unwrap();
        let EngineForm::MongoDb(m) = &prepared.engine else {
            unreachable!()
        };
        assert_eq!(m.uri.as_deref(), Some("mongodb://localhost:27017/dev"));
        assert_eq!(m.username.as_deref(), Some("ahmed"));
        assert_eq!(m.default_database.as_deref(), Some("dev"));
        assert!(!m.tls, "plain mongodb:// means no TLS");
        assert!(matches!(input, PasswordInput::Remember(ref p) if p.as_str() == "secret-pw"));
        // The saved profile validates: no password left in the URI.
        profile_from(&prepared, None).unwrap().validate().unwrap();

        // A password typed in the password field wins.
        let typed = PasswordChoice::SessionOnly {
            password: "typed".into(),
        };
        let (_, input) = prepare(&f, &typed, None).unwrap();
        assert!(matches!(input, PasswordInput::SessionOnly(ref p) if p.as_str() == "typed"));

        // tls=true in the URI is honoured.
        let EngineForm::MongoDb(mut m) = EngineForm::default_for("mongodb").unwrap() else {
            unreachable!()
        };
        m.uri = Some("mongodb://h:27017/?tls=true&authSource=admin".into());
        let (prepared, _) = prepare(
            &form(EngineForm::MongoDb(m), "mongodb.community"),
            &PasswordChoice::None,
            None,
        )
        .unwrap();
        let EngineForm::MongoDb(m) = &prepared.engine else {
            unreachable!()
        };
        assert!(m.tls);
        assert_eq!(m.auth_source.as_deref(), Some("admin"));
    }

    #[test]
    fn the_connection_string_box_supplies_only_the_password() {
        let EngineForm::Postgres(p) = EngineForm::default_for("postgres").unwrap() else {
            unreachable!()
        };
        let f = form(EngineForm::Postgres(p), "postgres.postgresql");
        let (_, input) = prepare(
            &f,
            &PasswordChoice::Keep,
            Some("postgres://u:from-uri@h/db"),
        )
        .unwrap();
        assert!(matches!(input, PasswordInput::Remember(ref p) if p.as_str() == "from-uri"));
        assert!(matches!(
            prepare(&f, &PasswordChoice::None, None).unwrap().1,
            PasswordInput::None
        ));
    }

    #[test]
    fn suggested_names_use_host_and_port() {
        let parsed =
            gazabridge_domain::connection_string::parse("mongodb://a:b@localhost:27017/dev")
                .unwrap();
        assert_eq!(suggested_name(&parsed.settings), "localhost:27017");
        let parsed =
            gazabridge_domain::connection_string::parse("mongodb+srv://c0.abcd.mongodb.net/x")
                .unwrap();
        assert_eq!(suggested_name(&parsed.settings), "c0.abcd.mongodb.net");
    }

    #[test]
    fn ssh_tunnels_round_trip_and_keep_their_secret_while_the_method_stays() {
        let engine = EngineForm::default_for("postgres").unwrap();
        let mut f = form(engine, "postgres.postgresql");
        f.ssh = Some(SshForm {
            host: " bastion.example.test ".into(),
            port: 2222,
            username: "deploy".into(),
            auth: "key".into(),
            key_path: Some("/keys/id_ed25519".into()),
            host_key_policy: "accept_new".into(),
        });
        let mut p = profile_from(&f, None).unwrap();
        let t = p.ssh_tunnel.clone().unwrap();
        assert_eq!(t.host, "bastion.example.test");
        assert_eq!(t.host_key_policy, HostKeyPolicy::AcceptNew);
        assert!(
            matches!(&t.auth, SshAuth::PrivateKey { key_path, passphrase }
            if key_path == &PathBuf::from("/keys/id_ed25519") && passphrase.is_none())
        );
        let back = form_from(&p).ssh.unwrap();
        assert_eq!(back.host, "bastion.example.test");
        assert_eq!(back.auth, "key");
        assert_eq!(back.host_key_policy, "accept_new");
        assert_eq!(ssh_secret_mode(&p), Some(PasswordMode::None));

        // A stored passphrase survives an edit with the same method...
        let stored = SecretRef::keychain_for(p.id, "ssh.key_passphrase");
        if let Some(SshAuth::PrivateKey { passphrase, .. }) =
            p.ssh_tunnel.as_mut().map(|t| &mut t.auth)
        {
            *passphrase = stored.clone();
        }
        assert_eq!(ssh_secret_mode(&p), Some(PasswordMode::Keychain));
        let mut edit = form_from(&p);
        edit.ssh.as_mut().unwrap().port = 22;
        let edited = profile_from(&edit, Some(&p)).unwrap();
        assert!(matches!(&edited.ssh_tunnel.as_ref().unwrap().auth,
            SshAuth::PrivateKey { passphrase, .. } if passphrase == &stored));
        // ...and is not carried over to a password.
        edit.ssh.as_mut().unwrap().auth = "password".into();
        let switched = profile_from(&edit, Some(&p)).unwrap();
        assert!(matches!(&switched.ssh_tunnel.as_ref().unwrap().auth,
            SshAuth::Password { password } if password.is_none()));
        // The agent needs no secret; removing the tunnel removes it.
        edit.ssh.as_mut().unwrap().auth = "agent".into();
        assert_eq!(
            ssh_secret_mode(&profile_from(&edit, Some(&p)).unwrap()),
            None
        );
        edit.ssh = None;
        assert!(profile_from(&edit, Some(&p)).unwrap().ssh_tunnel.is_none());
    }

    #[test]
    fn incomplete_ssh_settings_are_reported() {
        let engine = EngineForm::default_for("mysql").unwrap();
        let mut f = form(engine, "mysql.mysql");
        let base = SshForm {
            host: "bastion".into(),
            username: "deploy".into(),
            key_path: Some("/keys/id".into()),
            ..SshForm::new()
        };
        for (broken, needle) in [
            (
                SshForm {
                    host: " ".into(),
                    ..base.clone()
                },
                "host",
            ),
            (
                SshForm {
                    username: String::new(),
                    ..base.clone()
                },
                "user",
            ),
            (
                SshForm {
                    key_path: None,
                    ..base.clone()
                },
                "key file",
            ),
            (
                SshForm {
                    auth: "telnet".into(),
                    ..base.clone()
                },
                "sign-in method",
            ),
            (
                SshForm {
                    host_key_policy: "yolo".into(),
                    ..base.clone()
                },
                "host key policy",
            ),
        ] {
            f.ssh = Some(broken);
            let err = profile_from(&f, None).unwrap_err().to_string();
            assert!(err.contains(needle), "{err}");
        }
        let sqlite = EngineForm::default_for("sqlite").unwrap();
        let mut f = form(sqlite, "sqlite.sqlite");
        f.ssh = Some(base);
        assert!(
            profile_from(&f, None)
                .unwrap_err()
                .to_string()
                .contains("SQLite")
        );
    }
}
