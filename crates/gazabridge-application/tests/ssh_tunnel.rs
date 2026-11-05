//! Connections through SSH tunnels against real servers: PostgreSQL, MySQL
//! and MongoDB sit on a private container network that only an OpenSSH
//! server can reach. Connection tests, saved SSH secrets, host-key checks
//! and copies PostgreSQL → MySQL → MongoDB all go through the tunnels.
//! Run with `cargo test -- --ignored` (needs Docker and ssh-keygen).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gazabridge_application::{
    ConnectionService, CopyItem, ExistingData, ExplorerService, MigrationRequest, MigrationService,
    OnRecordError, PasswordInput, RunView,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::Value;
use gazabridge_domain::compatibility::ConnectionState;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostKeyPolicy, HostPort, MongoSettings, MySqlSettings,
    MySqlTlsMode, PgSslMode, PostgresSettings, ReadPreference, SecretRef, SshAuth, SshTunnel,
};
use gazabridge_domain::ids::{ConnectionId, RunId};
use gazabridge_domain::job::JobStatus;
use gazabridge_mapping::doc_to_table::DocMode;
use gazabridge_mapping::table_to_doc::TableMode;
use gazabridge_migration::validate::Level;
use gazabridge_plugin_sdk::Connection;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

// Test-only values for throwaway containers.
const SSH_PASSWORD: &str = "tunnel-test-pw";
const PG_PASSWORD: &str = "postgres";

fn keygen(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    let status = Command::new("ssh-keygen")
        .args([
            "-q",
            "-t",
            "ed25519",
            "-N",
            "",
            "-C",
            "gazabridge-test",
            "-f",
        ])
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());
    path
}

async fn sshd(network: &str, authorized: &str) -> ContainerAsync<GenericImage> {
    let script = format!(
        "apk add --no-cache openssh-server >/dev/null && ssh-keygen -A && \
         adduser -D tunnel && echo 'tunnel:{SSH_PASSWORD}' | chpasswd && \
         mkdir -p /home/tunnel/.ssh && printf '%s\\n' \"$KEYS\" > /home/tunnel/.ssh/authorized_keys && \
         chown -R tunnel /home/tunnel/.ssh && chmod 700 /home/tunnel/.ssh && \
         chmod 600 /home/tunnel/.ssh/authorized_keys && \
         exec /usr/sbin/sshd -D -e -o AllowTcpForwarding=yes -o PasswordAuthentication=yes"
    );
    GenericImage::new("alpine", "3.20")
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        .with_network(network)
        .with_env_var("KEYS", authorized)
        .with_cmd(["sh", "-c", script.as_str()])
        .start()
        .await
        .expect("sshd")
}

fn tunnel(port: u16, auth: SshAuth, policy: HostKeyPolicy) -> Option<SshTunnel> {
    Some(SshTunnel {
        host: "127.0.0.1".into(),
        port,
        username: "tunnel".into(),
        auth,
        host_key_policy: policy,
    })
}

fn by_password() -> SshAuth {
    SshAuth::Password {
        password: SecretRef::None,
    }
}

async fn run_sql(conn: &Arc<dyn Connection>, language: QueryLanguage, text: &str) -> Vec<Value> {
    conn.execute(
        QueryRequest {
            language,
            text: text.into(),
            params: vec![],
            max_rows: 10_000,
            timeout: None,
            database: None,
            collection: None,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|e| panic!("{text}: {e}"))
    .rows
    .into_iter()
    .flat_map(|r| r.fields.into_iter().map(|f| f.1.value))
    .collect()
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Int(n) => *n,
        Value::UInt(n) => *n as i64,
        Value::Decimal(d) => d.to_plain_string().parse().unwrap(),
        other => panic!("not a count: {other:?}"),
    }
}

async fn finish(m: &MigrationService, run: RunId) -> RunView {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let v = m.view(run).await.unwrap();
        if v.status.is_terminal() || v.status == JobStatus::Paused {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {v:#?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn deep_check(m: &MigrationService, run: RunId) -> Option<bool> {
    m.validate(run, Level::Deep).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        if let Some(v) = m.view(run).await.unwrap().validation
            && !v.running
        {
            return v.passed;
        }
        assert!(Instant::now() < deadline, "validation timed out");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn request(source: ConnectionId, target: ConnectionId, items: Vec<CopyItem>) -> MigrationRequest {
    MigrationRequest {
        name: None,
        source,
        target,
        items,
        existing: ExistingData::Skip,
        copy_indexes: true,
        batch_size: 64,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 2,
        doc_mode: DocMode::Flatten,
        table_mode: TableMode::Reference,
        backup_before_replace: false,
        sync: None,
    }
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn databases_behind_an_ssh_server_are_tested_and_copied_through_tunnels() {
    let dir = tempfile::tempdir().unwrap();
    let key = keygen(dir.path(), "id_tunnel");
    let public = std::fs::read_to_string(key.with_extension("pub")).unwrap();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let tag = format!("{}-{nanos}", std::process::id());
    let network = format!("gb-tun-{tag}");
    let (pg_name, my_name, mo_name) = (
        format!("gb-tpg-{tag}"),
        format!("gb-tmy-{tag}"),
        format!("gb-tmo-{tag}"),
    );
    let (_pg, _my, _mo, ssh) = tokio::join!(
        async {
            Postgres::default()
                .with_tag("16-alpine")
                .with_network(&network)
                .with_container_name(&pg_name)
                .start()
                .await
                .expect("postgres")
        },
        async {
            Mysql::default()
                .with_tag("8.4")
                .with_network(&network)
                .with_container_name(&my_name)
                .start()
                .await
                .expect("mysql")
        },
        async {
            GenericImage::new("mongo", "8.0")
                .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
                .with_network(&network)
                .with_container_name(&mo_name)
                .start()
                .await
                .expect("mongo")
        },
        sshd(&network, public.trim()),
    );
    let ssh_port = ssh.get_host_port_ipv4(22).await.unwrap();

    // Host keys accepted on first use go to the app's own file.
    let known = dir.path().join("known_hosts");
    gazabridge_ssh::set_known_hosts_file(known.clone());

    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(
            gazabridge_connector_postgres::PostgresConnectorFactory,
        ))
        .unwrap();
    registry
        .register(Arc::new(gazabridge_connector_mysql::MySqlConnectorFactory))
        .unwrap();
    registry
        .register(Arc::new(
            gazabridge_connector_mongodb::MongoConnectorFactory,
        ))
        .unwrap();
    let secrets = SessionStore::default();
    let svc = ConnectionService::new(
        store.clone(),
        registry,
        SecretResolver::new(Arc::new(secrets), Redactor::new()),
    );
    let quarantine = tempfile::tempdir().unwrap();
    let m = MigrationService::new(
        store.clone(),
        ExplorerService::new(store.clone(), svc.clone()),
        svc.redactor().clone(),
        quarantine.path().to_path_buf(),
    );

    // ---- PostgreSQL: Strict refuses the unknown server first ---------------
    let mut pg_profile = ConnectionProfile::new(
        "pg via ssh",
        "postgres.postgresql",
        ConnectionSettings::Postgres(PostgresSettings {
            // As the SSH server sees it: a name only the container network knows.
            host: pg_name.clone(),
            port: 5432,
            database: "postgres".into(),
            username: "postgres".into(),
            password: SecretRef::None,
            ssl_mode: PgSslMode::Disable,
            search_path: None,
            application_name: "GazaBridge".into(),
            connect_timeout_secs: 10,
            statement_timeout_ms: 0,
            pool_size: 4,
        }),
    );
    pg_profile.ssh_tunnel = tunnel(ssh_port, by_password(), HostKeyPolicy::Strict);
    let pg = svc
        .create_with_ssh(
            pg_profile,
            PasswordInput::SessionOnly(Zeroizing::new(PG_PASSWORD.into())),
            PasswordInput::SessionOnly(Zeroizing::new(SSH_PASSWORD.into())),
        )
        .await
        .unwrap();
    // The SSH password is held in the secret store, not in the profile.
    assert!(matches!(
        &pg.ssh_tunnel.as_ref().unwrap().auth,
        SshAuth::Password { password: SecretRef::Session { key } } if key.ends_with(":ssh.password")
    ));
    let refused = svc.test_saved(pg.id).await.unwrap();
    assert_eq!(
        refused.state,
        ConnectionState::ConnectionFailed,
        "{refused:?}"
    );
    assert!(
        refused.error.as_deref().unwrap_or("").contains("SHA256:"),
        "{refused:?}"
    );
    assert!(!known.exists(), "Strict records nothing");

    // Trust on first use: recorded, then the connection works.
    let mut edit = pg.clone();
    edit.ssh_tunnel.as_mut().unwrap().host_key_policy = HostKeyPolicy::AcceptNew;
    let pg = svc
        .update_with_ssh(edit, PasswordInput::Keep, PasswordInput::Keep)
        .await
        .unwrap();
    let ok = svc.test_saved(pg.id).await.unwrap();
    assert_eq!(ok.state, ConnectionState::Ready, "{ok:?}");
    assert_eq!(ok.server.as_ref().unwrap().product, "PostgreSQL");
    assert!(
        std::fs::read_to_string(&known)
            .unwrap()
            .contains(&format!("[127.0.0.1]:{ssh_port}"))
    );

    // A wrong SSH password fails as sign-in; the old secret is replaced.
    let wrong = svc
        .update_with_ssh(
            pg.clone(),
            PasswordInput::Keep,
            PasswordInput::SessionOnly(Zeroizing::new("not-it".into())),
        )
        .await
        .unwrap();
    let failed = svc.test_saved(wrong.id).await.unwrap();
    assert_eq!(
        failed.error_code.as_deref(),
        Some("authentication"),
        "{failed:?}"
    );
    let pg = svc
        .update_with_ssh(
            wrong,
            PasswordInput::Keep,
            PasswordInput::SessionOnly(Zeroizing::new(SSH_PASSWORD.into())),
        )
        .await
        .unwrap();
    // A draft test with a typed SSH password, as the editor sends it.
    let draft = svc
        .test_draft_with_ssh(
            &pg,
            Some(Zeroizing::new(PG_PASSWORD.into())),
            Some(Zeroizing::new(SSH_PASSWORD.into())),
        )
        .await;
    assert_eq!(draft.state, ConnectionState::Ready, "{draft:?}");

    // ---- MySQL: password sign-in; MongoDB: a key file ----------------------
    let mut my_profile = ConnectionProfile::new(
        "mysql via ssh",
        "mysql.mysql",
        ConnectionSettings::MySql(MySqlSettings {
            host: my_name.clone(),
            port: 3306,
            database: Some("test".into()),
            username: "root".into(),
            password: SecretRef::None,
            tls_mode: MySqlTlsMode::Disabled,
            charset: "utf8mb4".into(),
            connect_timeout_secs: 20,
            query_timeout_ms: 0,
            pool_size: 4,
        }),
    );
    my_profile.ssh_tunnel = tunnel(ssh_port, by_password(), HostKeyPolicy::Strict);
    let my = svc
        .create_with_ssh(
            my_profile,
            PasswordInput::None,
            PasswordInput::SessionOnly(Zeroizing::new(SSH_PASSWORD.into())),
        )
        .await
        .unwrap();
    let ok = svc.test_saved(my.id).await.unwrap();
    assert_eq!(ok.server.as_ref().unwrap().product, "MySQL", "{ok:?}");

    let mut mo_profile = ConnectionProfile::new(
        "mongo via ssh",
        "mongodb.community",
        ConnectionSettings::MongoDb(MongoSettings {
            uri: None,
            hosts: vec![HostPort {
                host: mo_name.clone(),
                port: 27017,
            }],
            replica_set: None,
            auth_source: None,
            username: None,
            password: SecretRef::None,
            default_database: None,
            tls: false,
            read_preference: ReadPreference::Primary,
            direct_connection: false,
            server_selection_timeout_ms: 10_000,
            connect_timeout_ms: 10_000,
        }),
    );
    mo_profile.ssh_tunnel = tunnel(
        ssh_port,
        SshAuth::PrivateKey {
            key_path: key.clone(),
            passphrase: SecretRef::None,
        },
        HostKeyPolicy::Strict,
    );
    let mo = svc
        .create_with_ssh(mo_profile, PasswordInput::None, PasswordInput::None)
        .await
        .unwrap();
    let ok = svc.test_saved(mo.id).await.unwrap();
    assert_eq!(ok.state, ConnectionState::Ready, "{ok:?}");
    assert_eq!(ok.server.as_ref().unwrap().product, "MongoDB");

    // ---- data: PostgreSQL → MySQL → MongoDB, all through tunnels -----------
    let pgx = svc.open(pg.id).await.unwrap();
    for s in [
        "CREATE TABLE items (id bigint PRIMARY KEY, name text NOT NULL, price numeric(10,2), made timestamptz)",
        "INSERT INTO items SELECT i, 'عنصر ' || i, i * 1.5, timestamptz '2026-01-01 00:00+00' + i * interval '1 hour' FROM generate_series(1, 500) i",
    ] {
        run_sql(&pgx, QueryLanguage::PostgresSql, s).await;
    }
    let to_mysql = request(
        pg.id,
        my.id,
        vec![CopyItem {
            source: ObjectRef::table(
                NamespacePath::schema(Some("postgres".into()), "public"),
                "items",
            ),
            target: ObjectRef::table(NamespacePath::database("viassh"), "items"),
        }],
    );
    let run = m.start(to_mysql, false).await.unwrap();
    let v = finish(&m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.inserted, 500);
    assert_eq!(deep_check(&m, run).await, Some(true));
    let myx = svc.open(my.id).await.unwrap();
    let rows = run_sql(
        &myx,
        QueryLanguage::MySql,
        "SELECT COUNT(*), CAST(SUM(price) AS SIGNED) FROM viassh.items",
    )
    .await;
    assert_eq!(int(&rows[0]), 500);
    assert_eq!(int(&rows[1]), 187_875); // 1.5 × (1 + … + 500), whole

    let to_mongo = request(
        my.id,
        mo.id,
        vec![CopyItem {
            source: ObjectRef::table(NamespacePath::database("viassh"), "items"),
            target: ObjectRef::collection("viassh", "items"),
        }],
    );
    let run = m.start(to_mongo, false).await.unwrap();
    let v = finish(&m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.inserted, 500);
    assert_eq!(deep_check(&m, run).await, Some(true));

    // Closing the connections closes their tunnels.
    pgx.close().await;
    myx.close().await;

    // ---- an SSH server that refuses forwarding to the target ---------------
    let mut nowhere = pg.clone();
    if let ConnectionSettings::Postgres(s) = &mut nowhere.settings {
        s.host = "no-such-host.invalid".into();
    }
    let outcome = svc
        .test_draft_with_ssh(
            &nowhere,
            Some(Zeroizing::new(PG_PASSWORD.into())),
            Some(Zeroizing::new(SSH_PASSWORD.into())),
        )
        .await;
    assert_eq!(outcome.state, ConnectionState::ConnectionFailed);
    assert!(
        outcome
            .error
            .as_deref()
            .unwrap_or("")
            .contains("could not reach"),
        "{outcome:?}"
    );
}
