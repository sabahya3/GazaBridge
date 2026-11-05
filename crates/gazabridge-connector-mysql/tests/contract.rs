//! Runs the connector contract suite against real MySQL and MariaDB servers
//! in Docker (Testcontainers). Ignored by default; run with
//! `cargo test -p gazabridge-connector-mysql -- --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)]

use std::sync::Arc;

use async_trait::async_trait;
use gazabridge_connector_mysql::MySqlConnectorFactory;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, MySqlSettings, MySqlTlsMode, SecretRef,
};
use gazabridge_plugin_sdk::schema::NamespacePath;
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ConnectorResult, ResolvedSecrets};
use gazabridge_testkit::{ContractHarness, run_contract_suite};
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, Image, ImageExt};

pub struct Harness<I: Image> {
    pub host: String,
    pub port: u16,
    pub profile_id: &'static str,
    _container: ContainerAsync<I>,
}

impl<I: Image> std::fmt::Debug for Harness<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Harness")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl<I: Image> Harness<I> {
    pub fn profile(&self, read_only: bool) -> ConnectionProfile {
        let mut p = ConnectionProfile::new(
            "contract",
            self.profile_id,
            ConnectionSettings::MySql(MySqlSettings {
                host: self.host.clone(),
                port: self.port,
                database: Some("test".into()),
                // The modules' images allow root without a password.
                username: "root".into(),
                password: SecretRef::None,
                tls_mode: MySqlTlsMode::Disabled,
                charset: "utf8mb4".into(),
                connect_timeout_secs: 20,
                query_timeout_ms: 0,
                pool_size: 4,
            }),
        );
        p.read_only = read_only;
        p
    }
}

pub async fn mysql(tag: &str) -> Harness<Mysql> {
    let c = Mysql::default()
        .with_tag(tag)
        .start()
        .await
        .expect("start MySQL");
    Harness {
        host: c.get_host().await.unwrap().to_string(),
        port: c.get_host_port_ipv4(3306).await.unwrap(),
        profile_id: "mysql.mysql",
        _container: c,
    }
}

pub async fn mariadb(tag: &str) -> Harness<Mariadb> {
    let c = Mariadb::default()
        .with_tag(tag)
        .start()
        .await
        .expect("start MariaDB");
    Harness {
        host: c.get_host().await.unwrap().to_string(),
        port: c.get_host_port_ipv4(3306).await.unwrap(),
        profile_id: "mysql.mariadb",
        _container: c,
    }
}

#[async_trait]
impl<I: Image + Send + Sync> ContractHarness for Harness<I> {
    fn family(&self) -> EngineFamily {
        EngineFamily::MySql
    }

    fn scratch_namespace(&self) -> NamespacePath {
        NamespacePath::database("test")
    }

    async fn connect(&self) -> ConnectorResult<Arc<dyn Connection>> {
        MySqlConnectorFactory
            .connect(&self.profile(false), ResolvedSecrets::default())
            .await
    }

    async fn connect_read_only(&self) -> ConnectorResult<Arc<dyn Connection>> {
        MySqlConnectorFactory
            .connect(&self.profile(true), ResolvedSecrets::default())
            .await
    }
}

async fn run(h: &dyn ContractHarness) {
    let report = run_contract_suite(h).await;
    println!("{report}");
    report.assert_passed();
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn mysql_8_0_passes_the_connector_contract() {
    run(&mysql("8.0").await).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn mysql_8_4_passes_the_connector_contract() {
    run(&mysql("8.4").await).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn mariadb_10_11_passes_the_connector_contract() {
    run(&mariadb("10.11").await).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn mariadb_11_4_passes_the_connector_contract() {
    run(&mariadb("11.4").await).await;
}
