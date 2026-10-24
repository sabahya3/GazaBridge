//! Runs the connector contract suite against real temporary SQLite files.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] functions

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use gazabridge_connector_sqlite::{SqliteConnectorFactory, main_schema};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, SqliteJournalMode, SqliteSettings,
};
use gazabridge_plugin_sdk::schema::NamespacePath;
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ConnectorResult, ResolvedSecrets};
use gazabridge_testkit::{ContractHarness, run_contract_suite};

struct Harness {
    path: PathBuf,
}

impl Harness {
    fn profile(&self, read_only: bool) -> ConnectionProfile {
        let mut p = ConnectionProfile::new(
            "contract",
            "sqlite.sqlite",
            ConnectionSettings::Sqlite(SqliteSettings {
                path: self.path.clone(),
                read_only,
                create_if_missing: !read_only,
                busy_timeout_ms: 5_000,
                journal_mode: SqliteJournalMode::Wal,
                foreign_keys: true,
            }),
        );
        p.read_only = read_only;
        p
    }
}

#[async_trait]
impl ContractHarness for Harness {
    fn family(&self) -> EngineFamily {
        EngineFamily::Sqlite
    }

    fn scratch_namespace(&self) -> NamespacePath {
        main_schema()
    }

    async fn connect(&self) -> ConnectorResult<Arc<dyn Connection>> {
        SqliteConnectorFactory
            .connect(&self.profile(false), ResolvedSecrets::default())
            .await
    }

    async fn connect_read_only(&self) -> ConnectorResult<Arc<dyn Connection>> {
        SqliteConnectorFactory
            .connect(&self.profile(true), ResolvedSecrets::default())
            .await
    }
}

#[tokio::test]
async fn sqlite_passes_the_connector_contract() {
    let dir = tempfile::tempdir().unwrap();
    let harness = Harness {
        path: dir.path().join("contract.db"),
    };
    let report = run_contract_suite(&harness).await;
    println!("{report}");
    report.assert_passed();
}
