//! MongoDB authentication follows connection-string rules: without
//! `authSource`, users authenticate against the database in the URI path.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

use std::sync::Arc;

use gazabridge_application::ConnectionService;
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::compatibility::ConnectionState;
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use mongodb::bson::doc;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{GenericImage, ImageExt};

// Throwaway credentials for a disposable test container.
const ROOT_PW: &str = "test-root-pw";
const APP_PW: &str = "test-app-pw";

async fn service() -> ConnectionService {
    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(
            gazabridge_connector_mongodb::MongoConnectorFactory,
        ))
        .unwrap();
    ConnectionService::new(
        store,
        registry,
        SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
    )
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn uri_users_authenticate_against_the_path_database() {
    let c = GenericImage::new("mongo", "8.0")
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .with_env_var("MONGO_INITDB_ROOT_USERNAME", "root")
        .with_env_var("MONGO_INITDB_ROOT_PASSWORD", ROOT_PW)
        .start()
        .await
        .expect("start mongod");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(27017).await.unwrap();
    // The init script restarts mongod; retry until the root user works.
    let root = mongodb::Client::with_uri_str(format!(
        "mongodb://root:{ROOT_PW}@{host}:{port}/?authSource=admin"
    ))
    .await
    .unwrap();
    let mut ready = false;
    for _ in 0..100 {
        if root
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .is_ok()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(ready, "mongod with auth did not become ready");
    // A user defined in `dev`, the way `use dev; db.createUser(...)` does it.
    root.database("dev")
        .run_command(doc! { "createUser": "app", "pwd": APP_PW, "roles": [ { "role": "readWrite", "db": "dev" } ] })
        .await
        .unwrap();

    let svc = service().await;
    let cases = [
        // Path database is the auth database (mongosh / Compass behaviour).
        (
            format!("mongodb://app:{APP_PW}@{host}:{port}/dev"),
            ConnectionState::Ready,
        ),
        // Explicit authSource wins.
        (
            format!("mongodb://app:{APP_PW}@{host}:{port}/other?authSource=dev"),
            ConnectionState::Ready,
        ),
        (
            format!("mongodb://root:{ROOT_PW}@{host}:{port}/dev?authSource=admin"),
            ConnectionState::Ready,
        ),
        // No path: admin, where `app` does not exist.
        (
            format!("mongodb://app:{APP_PW}@{host}:{port}/"),
            ConnectionState::ConnectionFailed,
        ),
    ];
    for (i, (uri, expected)) in cases.into_iter().enumerate() {
        let (profile, _) = svc
            .create_from_string(&format!("c{i}"), &uri, false)
            .await
            .unwrap();
        let outcome = svc.test_saved(profile.id).await.unwrap();
        assert_eq!(outcome.state, expected, "case {i}: {outcome:?}");
        if expected == ConnectionState::ConnectionFailed {
            assert_eq!(outcome.error_code.as_deref(), Some("authentication"));
            let hint = outcome.error.unwrap_or_default();
            assert!(
                hint.contains("admin"),
                "the error names the database used: {hint}"
            );
            assert!(!hint.contains(APP_PW));
        }
    }
}
