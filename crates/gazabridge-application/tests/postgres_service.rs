//! End-to-end connection service test against a real PostgreSQL server.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

use std::sync::Arc;

use gazabridge_application::{ConnectionService, PasswordInput};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::compatibility::{CompatibilityStatus, ConnectionState};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, PgSslMode, PostgresSettings, SecretRef,
};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use zeroize::Zeroizing;

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn test_and_open_a_real_postgres_connection() {
    let c = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("container");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(
            gazabridge_connector_postgres::PostgresConnectorFactory,
        ))
        .unwrap();
    let svc = ConnectionService::new(
        store,
        registry,
        SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
    );

    let profile = ConnectionProfile::new(
        "pg16",
        "postgres.postgresql",
        ConnectionSettings::Postgres(PostgresSettings {
            host,
            port,
            database: "postgres".into(),
            username: "postgres".into(),
            password: SecretRef::None,
            ssl_mode: PgSslMode::Disable,
            search_path: None,
            application_name: "GazaBridge".into(),
            connect_timeout_secs: 10,
            statement_timeout_ms: 0,
            pool_size: 2,
        }),
    );

    // Wrong password: a clean failure outcome, not an error.
    let bad = svc
        .create(
            profile.clone(),
            PasswordInput::SessionOnly(Zeroizing::new("wrong".into())),
        )
        .await
        .unwrap();
    let outcome = svc.test_saved(bad.id).await.unwrap();
    assert_eq!(outcome.state, ConnectionState::ConnectionFailed);
    assert_eq!(
        outcome.error_code.as_deref(),
        Some("authentication"),
        "{outcome:?}"
    );

    // Correct password (the image's documented default).
    let fixed = svc
        .update(
            bad.clone(),
            PasswordInput::SessionOnly(Zeroizing::new("postgres".into())),
        )
        .await
        .unwrap();
    let outcome = svc.test_saved(fixed.id).await.unwrap();
    assert_eq!(outcome.state, ConnectionState::Ready, "{outcome:?}");
    let assessment = outcome.assessment.unwrap();
    assert_eq!(assessment.status, CompatibilityStatus::Beta);
    assert!(
        assessment.findings.is_empty(),
        "16 is a tested version: {:?}",
        assessment.findings
    );
    assert_eq!(
        outcome.detected_profile.as_deref(),
        Some("postgres.postgresql")
    );

    let conn = svc.open(fixed.id).await.unwrap();
    assert!(conn.health_check().await.is_ok());
    conn.close().await;
}
