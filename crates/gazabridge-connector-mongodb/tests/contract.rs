//! Runs the connector contract suite against real single-node MongoDB
//! replica sets (Testcontainers). Ignored by default; run with
//! `cargo test -- --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use gazabridge_connector_mongodb::MongoConnectorFactory;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, HostPort, MongoSettings, ReadPreference,
    SecretRef,
};
use gazabridge_plugin_sdk::schema::NamespacePath;
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ConnectorResult, ResolvedSecrets};
use gazabridge_testkit::{ContractHarness, run_contract_suite};

const DB: &str = "gazabridge_contract";

struct Harness {
    host: String,
    port: u16,
    _rs: common::ReplicaSet,
}

impl Harness {
    async fn start(tag: &str) -> Self {
        let rs = common::start(tag).await;
        Self {
            host: rs.host.clone(),
            port: rs.port,
            _rs: rs,
        }
    }

    fn profile(&self, read_only: bool) -> ConnectionProfile {
        let mut p = ConnectionProfile::new(
            "contract",
            "mongodb.community",
            ConnectionSettings::MongoDb(MongoSettings {
                uri: None,
                hosts: vec![HostPort {
                    host: self.host.clone(),
                    port: self.port,
                }],
                replica_set: None,
                auth_source: None,
                username: None,
                password: SecretRef::None,
                default_database: Some(DB.into()),
                tls: false,
                read_preference: ReadPreference::Primary,
                // The replica set advertises the container's internal host name.
                direct_connection: true,
                server_selection_timeout_ms: 10_000,
                connect_timeout_ms: 10_000,
            }),
        );
        p.read_only = read_only;
        p
    }
}

#[async_trait]
impl ContractHarness for Harness {
    fn family(&self) -> EngineFamily {
        EngineFamily::MongoDb
    }

    fn scratch_namespace(&self) -> NamespacePath {
        NamespacePath::database(DB)
    }

    async fn connect(&self) -> ConnectorResult<Arc<dyn Connection>> {
        MongoConnectorFactory
            .connect(&self.profile(false), ResolvedSecrets::default())
            .await
    }

    async fn connect_read_only(&self) -> ConnectorResult<Arc<dyn Connection>> {
        MongoConnectorFactory
            .connect(&self.profile(true), ResolvedSecrets::default())
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
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_6_passes_the_connector_contract() {
    run("6.0").await;
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_7_passes_the_connector_contract() {
    run("7.0").await;
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_8_passes_the_connector_contract() {
    run("8.0").await;
}
