//! Starts a single-node MongoDB replica set in Docker for tests.
//!
//! The Testcontainers MongoDB module waits for a log line that MongoDB 8.0
//! no longer prints, so the replica set is initiated here and readiness is
//! checked through the `hello` command instead of log text.

#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::time::{Duration, Instant};

use mongodb::bson::doc;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub struct ReplicaSet {
    pub host: String,
    pub port: u16,
    _container: ContainerAsync<GenericImage>,
}

pub async fn start(tag: &str) -> ReplicaSet {
    let container = GenericImage::new("mongo", tag)
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .with_cmd(["--replSet", "rs", "--bind_ip_all"])
        .start()
        .await
        .expect("start mongod");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let client =
        mongodb::Client::with_uri_str(format!("mongodb://{host}:{port}/?directConnection=true"))
            .await
            .unwrap();
    let admin = client.database("admin");
    admin
        .run_command(doc! { "replSetInitiate": { "_id": "rs", "members": [ { "_id": 0, "host": "localhost:27017" } ] } })
        .await
        .expect("replSetInitiate");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let hello = admin.run_command(doc! { "hello": 1 }).await.unwrap();
        if hello.get_bool("isWritablePrimary").unwrap_or(false) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica set did not elect a primary"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ReplicaSet {
        host,
        port,
        _container: container,
    }
}
