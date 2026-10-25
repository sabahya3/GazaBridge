//! The Explorer's MongoDB query bar against a real server: shell-syntax
//! filters, projection, sort, counts and exports.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;

use gazabridge_application::{
    BrowseRequest, ConnectionService, EditBlocker, ExplorerService, ExportRequest, PasswordInput,
    TransferService,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::Value;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, ReadPreference, SecretRef,
};
use gazabridge_domain::shell_json;
use gazabridge_files::{FileFormat, WriteOptions};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::schema::ObjectRef;
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use mongodb::bson::{Document, doc, oid::ObjectId};
use tokio_util::sync::CancellationToken;

fn native(text: &str) -> Filter {
    Filter::Native {
        query: shell_json::parse(text).unwrap(),
    }
}

fn request(
    object: &ObjectRef,
    filter: Option<Filter>,
    projection: Vec<&str>,
    sort: Vec<SortKey>,
) -> BrowseRequest {
    BrowseRequest {
        object: object.clone(),
        filter,
        projection: projection.into_iter().map(String::from).collect(),
        exclude: vec![],
        order_by: sort,
        offset: 0,
        limit: 50,
    }
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn query_bar_filters_project_sort_count_and_export() {
    let rs = common::mongo::start("8.0").await;
    let raw = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    let anchor = ObjectId::new();
    raw.database("app")
        .collection::<Document>("users")
        .insert_many([
            doc! { "_id": anchor, "name": "Ahmed", "phoneNumber": "966123456789", "age": 30, "at": mongodb::bson::DateTime::from_millis(1_767_225_600_000) },
            doc! { "name": "Sara", "phoneNumber": "966500000000", "age": 25 },
            doc! { "name": "ahmad", "phoneNumber": "970599000000", "age": 41 },
        ])
        .await
        .unwrap();

    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
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
    let transfers = TransferService::new(store, explorer.clone(), svc.redactor().clone());
    let id = svc
        .create(
            ConnectionProfile::new(
                "m",
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
                    default_database: Some("app".into()),
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
        .unwrap()
        .id;
    let users = ObjectRef::collection("app", "users");
    let names = |page: &gazabridge_application::BrowseResult| -> Vec<String> {
        page.records
            .iter()
            .map(|r| match r.get("name") {
                Value::String(s) => s.clone(),
                _ => String::new(),
            })
            .collect()
    };

    // A phone number stored as text matches as text, not as a number.
    let page = explorer
        .browse(
            id,
            request(
                &users,
                Some(native(r#"{phoneNumber: "966123456789"}"#)),
                vec![],
                vec![],
            ),
        )
        .await
        .unwrap();
    assert_eq!(names(&page), ["Ahmed"]);
    let page = explorer
        .browse(
            id,
            request(
                &users,
                Some(native("{phoneNumber: 966123456789}")),
                vec![],
                vec![],
            ),
        )
        .await
        .unwrap();
    assert!(
        page.records.is_empty(),
        "a number never equals text in MongoDB"
    );

    // Operators, regexes, ObjectId and ISODate in shell syntax.
    let page = explorer
        .browse(
            id,
            request(
                &users,
                Some(native("{age: {$gte: 30}, name: /^ah/i}")),
                vec![],
                vec![],
            ),
        )
        .await
        .unwrap();
    assert_eq!(names(&page).len(), 2);
    let page = explorer
        .browse(
            id,
            request(
                &users,
                Some(native(&format!(
                    r#"{{_id: ObjectId("{}"), at: {{$gte: ISODate("2026-01-01")}}}}"#,
                    anchor.to_hex()
                ))),
                vec![],
                vec![],
            ),
        )
        .await
        .unwrap();
    assert_eq!(names(&page), ["Ahmed"]);

    // Sort and Project; projected pages are not editable.
    let by_age = vec![SortKey {
        field: FieldPath::new("age"),
        direction: SortDirection::Desc,
    }];
    let page = explorer
        .browse(id, request(&users, None, vec!["_id", "name"], by_age))
        .await
        .unwrap();
    assert_eq!(names(&page), ["ahmad", "Ahmed", "Sara"]);
    assert_eq!(page.fields, ["_id", "name"]);
    assert_eq!(page.edit_blocker, Some(EditBlocker::Projected));
    let mut hide = request(&users, None, vec![], vec![]);
    hide.exclude = vec!["phoneNumber".into()];
    let page = explorer.browse(id, hide).await.unwrap();
    assert!(!page.fields.contains(&"phoneNumber".to_owned()));

    // Counts follow the query; an estimate covers the whole collection.
    let c = explorer
        .count(id, &users, Some(&native("{age: {$lt: 40}}")), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((c.count, c.exact), (2, true));
    let c = explorer
        .count(id, &users, None, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c.count, 3);

    // Exports apply the query's filter and Project.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("young.jsonl");
    let out = transfers
        .export(
            id,
            ExportRequest {
                object: users.clone(),
                filter: Some(native("{age: {$lt: 40}}")),
                projection: vec!["name".into()],
                exclude: vec![],
                path: path.clone(),
                options: WriteOptions::new(FileFormat::Jsonl),
            },
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(out.file.unwrap().records, 2);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        text.contains("Sara") && !text.contains("phoneNumber") && !text.contains("ahmad"),
        "{text}"
    );

    // Native filters are refused where they cannot apply (destructive guard).
    assert!(!native("{}").is_restrictive());
    assert!(native("{a: 1}").is_restrictive());
}
