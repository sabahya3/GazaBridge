//! Explorer and data editor against real MongoDB and PostgreSQL servers.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;

use gazabridge_application::{
    BrowseRequest, ConnectionService, EditBlocker, ExplorerService, PasswordInput,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::Value;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, PgSslMode, PostgresSettings,
    ReadPreference, SecretRef,
};
use gazabridge_domain::ejson;
use gazabridge_domain::ids::ConnectionId;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectKind, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use mongodb::bson::{Bson, Decimal128, doc, oid::ObjectId};
use serde_json::{Value as J, json};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use zeroize::Zeroizing;

async fn services() -> (MetadataStore, ConnectionService, ExplorerService) {
    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(
            gazabridge_connector_postgres::PostgresConnectorFactory,
        ))
        .unwrap();
    registry
        .register(Arc::new(
            gazabridge_connector_mongodb::MongoConnectorFactory,
        ))
        .unwrap();
    let svc = ConnectionService::new(
        store.clone(),
        registry,
        SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
    );
    let explorer = ExplorerService::new(store.clone(), svc.clone());
    (store, svc, explorer)
}

fn browse(object: &ObjectRef, filter: Option<Filter>, order_by: Vec<SortKey>) -> BrowseRequest {
    BrowseRequest {
        object: object.clone(),
        filter,
        projection: vec![],
        exclude: vec![],
        order_by,
        offset: 0,
        limit: 50,
    }
}

fn edited(original: &str, f: impl FnOnce(&mut serde_json::Map<String, J>)) -> String {
    let mut j: J = serde_json::from_str(original).unwrap();
    f(j.as_object_mut().unwrap());
    j.to_string()
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn explorer_edits_mongodb_documents_without_losing_types() {
    let rs = common::mongo::start("8.0").await;
    let raw = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    let coll = raw
        .database("shop")
        .collection::<mongodb::bson::Document>("items");
    let a = ObjectId::new();
    let b = ObjectId::new();
    coll.insert_many([
        doc! {
            "_id": a, "name": "apple", "n": Bson::Int32(5), "nested": { "k": Bson::Int32(1) },
            "price": Decimal128::from_bytes(gazabridge_test_decimal()), "extra": true,
        },
        doc! { "_id": b, "name": "pear", "n": Bson::Int32(7) },
    ])
    .await
    .unwrap();

    let (store, svc, explorer) = services().await;
    let profile = svc
        .create(
            ConnectionProfile::new(
                "mongo",
                "mongodb.community",
                ConnectionSettings::MongoDb(MongoSettings {
                    uri: None,
                    hosts: vec![HostPort {
                        host: rs.host.clone(),
                        port: rs.port,
                    }],
                    replica_set: None,
                    auth_source: None,
                    username: None,
                    password: SecretRef::None,
                    default_database: Some("shop".into()),
                    tls: false,
                    read_preference: ReadPreference::Primary,
                    direct_connection: true,
                    server_selection_timeout_ms: 10_000,
                    connect_timeout_ms: 10_000,
                }),
            ),
            PasswordInput::None,
        )
        .await
        .unwrap();
    let id = profile.id;

    // Tree.
    let dbs = explorer.list(id, &NamespacePath::root()).await.unwrap();
    assert!(
        dbs.iter()
            .any(|d| d.name == "shop" && d.kind == ObjectKind::Database)
    );
    let colls = explorer
        .list(id, &NamespacePath::database("shop"))
        .await
        .unwrap();
    assert!(
        colls
            .iter()
            .any(|c| c.name == "items" && c.kind == ObjectKind::Collection)
    );

    // Viewer: union of fields, null vs missing.
    let object = ObjectRef::collection("shop", "items");
    let sorted = vec![SortKey {
        field: FieldPath::new("name"),
        direction: SortDirection::Asc,
    }];
    let page = explorer
        .browse(id, browse(&object, None, sorted.clone()))
        .await
        .unwrap();
    assert_eq!(page.key_fields, ["_id"]);
    assert_eq!(page.edit_blocker, None);
    assert_eq!(page.fields[0], "_id");
    assert!(page.fields.contains(&"extra".to_owned()));
    assert_eq!(*page.records[1].get("extra"), Value::Missing);
    let missing = Filter::IsMissing {
        field: FieldPath::new("extra"),
    };
    assert_eq!(
        explorer
            .count(id, &object, Some(&missing), true)
            .await
            .unwrap()
            .unwrap()
            .count,
        1
    );

    // Edit apple: rename, remove `extra`, keep every other type.
    let original = ejson::record_to_ejson_string(&page.records[0]);
    assert!(
        original.contains("$oid") && original.contains("$numberDecimal"),
        "{original}"
    );
    let changed = edited(&original, |m| {
        m.insert("name".into(), json!("green apple"));
        m.remove("extra");
    });
    let plan = explorer
        .plan_update(id, &object, &original, &changed)
        .await
        .unwrap();
    assert!(!plan.conflict);
    assert_eq!(
        plan.changes
            .iter()
            .map(|c| c.field.as_str())
            .collect::<Vec<_>>(),
        ["name", "extra"]
    );
    explorer
        .apply_update(id, &object, &original, &changed)
        .await
        .unwrap();
    let stored = coll.find_one(doc! { "_id": a }).await.unwrap().unwrap();
    assert_eq!(stored.get_str("name").unwrap(), "green apple");
    assert!(stored.get("extra").is_none());
    assert!(
        matches!(stored.get("n"), Some(Bson::Int32(5))),
        "{stored:?}"
    );
    assert!(matches!(
        stored.get_document("nested").unwrap().get("k"),
        Some(Bson::Int32(1))
    ));
    assert!(matches!(stored.get("price"), Some(Bson::Decimal128(_))));

    // Editing an Int32 field keeps it Int32.
    let page = explorer
        .browse(id, browse(&object, None, sorted.clone()))
        .await
        .unwrap();
    let original = ejson::record_to_ejson_string(&page.records[0]);
    let changed = edited(&original, |m| {
        m.insert("n".into(), json!(6));
    });
    explorer
        .apply_update(id, &object, &original, &changed)
        .await
        .unwrap();
    let stored = coll.find_one(doc! { "_id": a }).await.unwrap().unwrap();
    assert!(
        matches!(stored.get("n"), Some(Bson::Int32(6))),
        "{stored:?}"
    );

    // A concurrent change makes the stale edit a conflict.
    let page = explorer
        .browse(id, browse(&object, None, sorted.clone()))
        .await
        .unwrap();
    let pear = ejson::record_to_ejson_string(&page.records[1]);
    coll.update_one(doc! { "_id": b }, doc! { "$set": { "n": 8 } })
        .await
        .unwrap();
    let stale = edited(&pear, |m| {
        m.insert("name".into(), json!("nashi"));
    });
    let err = explorer
        .apply_update(id, &object, &pear, &stale)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "conflict");
    assert_eq!(
        coll.find_one(doc! { "_id": b })
            .await
            .unwrap()
            .unwrap()
            .get_str("name")
            .unwrap(),
        "pear"
    );

    // `_id` cannot change.
    let rekey = edited(&pear, |m| {
        m.insert("_id".into(), json!({ "$oid": ObjectId::new().to_hex() }));
    });
    assert_eq!(
        explorer
            .plan_update(id, &object, &pear, &rekey)
            .await
            .unwrap_err()
            .code(),
        "invalid_input"
    );

    // Insert with typed values.
    explorer
        .insert(id, &object, r#"{"_id": {"$oid": "0102030405060708090a0b0c"}, "name": "fig", "price": {"$numberDecimal": "1.10"}, "at": {"$date": "2026-10-25T00:00:00Z"}}"#)
        .await
        .unwrap();
    let fig = coll
        .find_one(doc! { "name": "fig" })
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(fig.get("_id"), Some(Bson::ObjectId(_))));
    assert!(matches!(fig.get("price"), Some(Bson::Decimal128(_))));
    assert!(matches!(fig.get("at"), Some(Bson::DateTime(_))));

    // Delete pear after reloading it.
    let page = explorer
        .browse(id, browse(&object, None, sorted))
        .await
        .unwrap();
    let pear = page
        .records
        .iter()
        .find(|r| *r.get("name") == Value::String("pear".into()))
        .unwrap();
    explorer
        .delete(id, &object, &ejson::record_to_ejson_string(pear))
        .await
        .unwrap();
    assert!(coll.find_one(doc! { "_id": b }).await.unwrap().is_none());

    let audit = store.audit().list(Some("object"), None, 20).await.unwrap();
    assert_eq!(
        audit.iter().filter(|e| e.action == "data.update").count(),
        2
    );
    assert!(
        audit
            .iter()
            .all(|e| !e.details.to_string().contains("green apple"))
    );
    explorer.disconnect(id).await;
}

/// Decimal128 bytes for `12.50`, built through the connector's codec.
fn gazabridge_test_decimal() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    // coefficient 1250, exponent -2 (biased 6174)
    let coefficient: u128 = 1250;
    let exponent: u128 = 6176 - 2;
    let bits = (exponent << 113) | coefficient;
    bytes.copy_from_slice(&bits.to_le_bytes());
    bytes
}

async fn pg_exec(svc: &ConnectionService, id: ConnectionId, sql: &str) {
    let conn = svc.open(id).await.unwrap();
    conn.execute(
        QueryRequest {
            language: QueryLanguage::PostgresSql,
            text: sql.into(),
            params: vec![],
            max_rows: 10,
            timeout: None,
            database: None,
            collection: None,
        },
        Default::default(),
    )
    .await
    .unwrap();
    conn.close().await;
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn explorer_browses_and_edits_postgres_rows() {
    let c = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("container");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let (_store, svc, explorer) = services().await;
    let profile = svc
        .create(
            ConnectionProfile::new(
                "pg",
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
            ),
            // The image's documented default password.
            PasswordInput::SessionOnly(Zeroizing::new("postgres".into())),
        )
        .await
        .unwrap();
    let id = profile.id;
    pg_exec(
        &svc,
        id,
        "CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL, qty int, doc jsonb, price numeric(10,2), note text);
         CREATE TABLE logbook (line text);
         CREATE VIEW cheap AS SELECT * FROM items WHERE price < 5;",
    )
    .await;

    // Tree: databases → schemas → objects.
    let dbs = explorer.list(id, &NamespacePath::root()).await.unwrap();
    assert!(dbs.iter().any(|d| d.name == "postgres"));
    let schemas = explorer
        .list(id, &NamespacePath::database("postgres"))
        .await
        .unwrap();
    assert!(
        schemas
            .iter()
            .any(|s| s.name == "public" && s.kind == ObjectKind::Schema)
    );
    let public = NamespacePath::schema(Some("postgres".into()), "public");
    let objects = explorer.list(id, &public).await.unwrap();
    assert!(
        objects
            .iter()
            .any(|o| o.name == "items" && o.kind == ObjectKind::Table)
    );
    assert!(
        objects
            .iter()
            .any(|o| o.name == "cheap" && o.kind == ObjectKind::View)
    );

    let items = ObjectRef::table(public.clone(), "items");
    explorer
        .insert(id, &items, r#"{"name": "apple", "qty": 3, "doc": {"tags": ["red", 1]}, "price": {"$numberDecimal": "2.50"}}"#)
        .await
        .unwrap();
    explorer
        .insert(
            id,
            &items,
            r#"{"name": "melon", "qty": 1, "price": {"$numberDecimal": "9.00"}}"#,
        )
        .await
        .unwrap();
    assert_eq!(
        explorer
            .insert(id, &items, r#"{"colour": "red"}"#)
            .await
            .unwrap_err()
            .code(),
        "invalid_input"
    );

    let by_id = vec![SortKey {
        field: FieldPath::new("id"),
        direction: SortDirection::Asc,
    }];
    let page = explorer
        .browse(id, browse(&items, None, by_id.clone()))
        .await
        .unwrap();
    assert_eq!(page.key_fields, ["id"]);
    assert_eq!(page.edit_blocker, None);
    assert_eq!(page.fields, ["id", "name", "qty", "doc", "price", "note"]);
    assert_eq!(*page.records[0].get("note"), Value::Null);

    // Edit apple: qty, JSON column and a null column.
    let original = ejson::record_to_ejson_string(&page.records[0]);
    let changed = edited(&original, |m| {
        m.insert("qty".into(), json!(4));
        m.insert("doc".into(), json!({ "tags": ["green"] }));
        m.insert("note".into(), json!("crisp"));
    });
    let plan = explorer
        .plan_update(id, &items, &original, &changed)
        .await
        .unwrap();
    assert_eq!(plan.changes.len(), 3);
    explorer
        .apply_update(id, &items, &original, &changed)
        .await
        .unwrap();
    let page = explorer
        .browse(id, browse(&items, None, by_id.clone()))
        .await
        .unwrap();
    let apple = &page.records[0];
    assert_eq!(*apple.get("qty"), Value::Int(4));
    assert_eq!(*apple.get("doc"), Value::Json(json!({ "tags": ["green"] })));
    assert_eq!(*apple.get("note"), Value::String("crisp".into()));

    // Columns cannot be removed, keys cannot change.
    let current = ejson::record_to_ejson_string(apple);
    let removed = edited(&current, |m| {
        m.remove("note");
    });
    assert_eq!(
        explorer
            .plan_update(id, &items, &current, &removed)
            .await
            .unwrap_err()
            .code(),
        "invalid_input"
    );

    // Filters and counts.
    let cheap = Filter::Lt {
        field: FieldPath::new("price"),
        value: Value::Int(5),
    };
    assert_eq!(
        explorer
            .count(id, &items, Some(&cheap), true)
            .await
            .unwrap()
            .unwrap()
            .count,
        1
    );

    // A concurrent UPDATE turns the stale edit into a conflict.
    pg_exec(&svc, id, "UPDATE items SET qty = 10 WHERE name = 'apple'").await;
    let stale = edited(&current, |m| {
        m.insert("name".into(), json!("crab apple"));
    });
    assert!(
        explorer
            .plan_update(id, &items, &current, &stale)
            .await
            .unwrap()
            .conflict
    );
    assert_eq!(
        explorer
            .apply_update(id, &items, &current, &stale)
            .await
            .unwrap_err()
            .code(),
        "conflict"
    );

    // Objects that cannot be edited say why.
    let logbook = ObjectRef::table(public.clone(), "logbook");
    let page = explorer
        .browse(id, browse(&logbook, None, vec![]))
        .await
        .unwrap();
    assert_eq!(page.edit_blocker, Some(EditBlocker::NoPrimaryKey));
    let view = ObjectRef {
        namespace: public.clone(),
        name: "cheap".into(),
        kind: ObjectKind::View,
    };
    let page = explorer
        .browse(id, browse(&view, None, vec![]))
        .await
        .unwrap();
    assert_eq!(page.edit_blocker, Some(EditBlocker::NotWritable));
    assert_eq!(page.records.len(), 1);

    // Delete melon.
    let page = explorer
        .browse(id, browse(&items, None, by_id))
        .await
        .unwrap();
    let melon = ejson::record_to_ejson_string(&page.records[1]);
    explorer.delete(id, &items, &melon).await.unwrap();
    assert_eq!(
        explorer
            .count(id, &items, None, true)
            .await
            .unwrap()
            .unwrap()
            .count,
        1
    );
    explorer.disconnect(id).await;
}
