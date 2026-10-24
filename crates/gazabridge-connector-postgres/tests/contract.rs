//! Runs the connector contract suite against real PostgreSQL servers in
//! Docker (Testcontainers). Ignored by default; CI runs it with
//! `GAZABRIDGE_INTEGRATION=1 cargo test -- --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)] // test helpers outside #[test] functions

use std::sync::Arc;

use async_trait::async_trait;
use gazabridge_connector_postgres::{PostgresConnectorFactory, public_schema};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, PgSslMode, PostgresSettings, SecretRef,
};
use gazabridge_plugin_sdk::schema::NamespacePath;
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ConnectorResult, ResolvedSecrets};
use gazabridge_testkit::{ContractHarness, run_contract_suite};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use zeroize::Zeroizing;

struct Harness {
    host: String,
    port: u16,
    _container: ContainerAsync<Postgres>,
}

impl Harness {
    async fn start(tag: &str) -> Self {
        let container = Postgres::default()
            .with_tag(tag)
            .start()
            .await
            .expect("start PostgreSQL container");
        Self {
            host: container.get_host().await.unwrap().to_string(),
            port: container.get_host_port_ipv4(5432).await.unwrap(),
            _container: container,
        }
    }

    fn profile(&self, read_only: bool) -> ConnectionProfile {
        let mut p = ConnectionProfile::new(
            "contract",
            "postgres.postgresql",
            ConnectionSettings::Postgres(PostgresSettings {
                host: self.host.clone(),
                port: self.port,
                database: "postgres".into(),
                username: "postgres".into(),
                password: SecretRef::Session {
                    key: "contract".into(),
                },
                // The container image serves plain TCP only.
                ssl_mode: PgSslMode::Disable,
                search_path: None,
                application_name: "GazaBridge contract".into(),
                connect_timeout_secs: 10,
                statement_timeout_ms: 60_000,
                pool_size: 4,
            }),
        );
        p.read_only = read_only;
        p
    }

    fn secrets() -> ResolvedSecrets {
        // The testcontainers image's documented default credentials.
        ResolvedSecrets {
            password: Some(Zeroizing::new("postgres".into())),
            ..Default::default()
        }
    }
}

#[async_trait]
impl ContractHarness for Harness {
    fn family(&self) -> EngineFamily {
        EngineFamily::Postgres
    }

    fn scratch_namespace(&self) -> NamespacePath {
        public_schema()
    }

    async fn connect(&self) -> ConnectorResult<Arc<dyn Connection>> {
        PostgresConnectorFactory
            .connect(&self.profile(false), Self::secrets())
            .await
    }

    async fn connect_read_only(&self) -> ConnectorResult<Arc<dyn Connection>> {
        PostgresConnectorFactory
            .connect(&self.profile(true), Self::secrets())
            .await
    }
}

async fn run(tag: &str) {
    let harness = Harness::start(tag).await;
    let report = run_contract_suite(&harness).await;
    println!("{report}");
    report.assert_passed();
}

#[tokio::test]
#[ignore = "needs Docker; run with GAZABRIDGE_INTEGRATION=1 cargo test -- --ignored"]
async fn postgres_13_passes_the_connector_contract() {
    run("13-alpine").await;
}

#[tokio::test]
#[ignore = "needs Docker; run with GAZABRIDGE_INTEGRATION=1 cargo test -- --ignored"]
async fn postgres_14_passes_the_connector_contract() {
    run("14-alpine").await;
}

#[tokio::test]
#[ignore = "needs Docker; run with GAZABRIDGE_INTEGRATION=1 cargo test -- --ignored"]
async fn postgres_15_passes_the_connector_contract() {
    run("15-alpine").await;
}

#[tokio::test]
#[ignore = "needs Docker; run with GAZABRIDGE_INTEGRATION=1 cargo test -- --ignored"]
async fn postgres_16_passes_the_connector_contract() {
    run("16-alpine").await;
}

#[tokio::test]
#[ignore = "needs Docker; run with GAZABRIDGE_INTEGRATION=1 cargo test -- --ignored"]
async fn postgres_17_passes_the_connector_contract() {
    run("17-alpine").await;
}
