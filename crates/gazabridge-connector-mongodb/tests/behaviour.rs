//! MongoDB-specific behaviour against a real replica set (Testcontainers).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;

use futures::StreamExt;
use gazabridge_connector_mongodb::{MongoConnectorFactory, document_to_record};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, ReadPreference, SecretRef,
};
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::connector::InspectOptions;
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::read::{ReadPosition, ReadRequest};
use gazabridge_plugin_sdk::schema::{ObjectRef, SamplingMethod};
use gazabridge_plugin_sdk::write::WriteBatch;
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ErrorKind, ResolvedSecrets};
use mongodb::bson::{Bson, Document, doc};
use tokio_util::sync::CancellationToken;

const DB: &str = "behaviour";

async fn connect(host: &str, port: u16, read_only: bool) -> Arc<dyn Connection> {
    let mut p = ConnectionProfile::new(
        "b",
        "mongodb.community",
        ConnectionSettings::MongoDb(MongoSettings {
            uri: None,
            hosts: vec![HostPort {
                host: host.into(),
                port,
            }],
            replica_set: None,
            auth_source: None,
            username: None,
            password: SecretRef::None,
            default_database: Some(DB.into()),
            tls: false,
            read_preference: ReadPreference::Primary,
            direct_connection: true,
            server_selection_timeout_ms: 10_000,
            connect_timeout_ms: 10_000,
        }),
    );
    p.read_only = read_only;
    MongoConnectorFactory
        .connect(&p, ResolvedSecrets::default())
        .await
        .unwrap()
}

fn coll(name: &str) -> ObjectRef {
    ObjectRef::collection(DB, name)
}

fn query(language: QueryLanguage, collection: &str, text: &str) -> QueryRequest {
    QueryRequest {
        language,
        text: text.into(),
        params: vec![],
        max_rows: 100,
        timeout: None,
        database: Some(DB.into()),
        collection: Some(collection.into()),
    }
}

async fn read_all(
    conn: &Arc<dyn Connection>,
    req: ReadRequest,
) -> (Vec<Record>, Vec<ReadPosition>) {
    let mut s = conn.read(req, CancellationToken::new()).await.unwrap();
    let mut out = Vec::new();
    let mut positions = Vec::new();
    while let Some(b) = s.next().await {
        let b = b.unwrap();
        positions.extend(b.position.clone());
        out.extend(b.records);
    }
    (out, positions)
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_specific_behaviour() {
    let rs = common::start("8.0").await;
    let (host, port) = (rs.host.clone(), rs.port);
    let client =
        mongodb::Client::with_uri_str(format!("mongodb://{host}:{port}/?directConnection=true"))
            .await
            .unwrap();
    let raw = client.database(DB);
    let conn = connect(&host, port, false).await;

    // --- resumable reads across mixed-type _ids (type bracketing) ---
    let mixed: Vec<Document> = vec![
        doc! { "_id": 1, "v": "int" },
        doc! { "_id": 2.5, "v": "double" },
        doc! { "_id": "a", "v": "string" },
        doc! { "_id": "b", "v": "string" },
        doc! { "_id": mongodb::bson::oid::ObjectId::new(), "v": "oid" },
        doc! { "_id": true, "v": "bool" },
        doc! { "_id": mongodb::bson::DateTime::now(), "v": "date" },
    ];
    raw.collection::<Document>("mixed")
        .insert_many(mixed.clone())
        .await
        .unwrap();
    let (all, positions) = read_all(&conn, ReadRequest::new(coll("mixed"), 2)).await;
    assert_eq!(
        all.len(),
        mixed.len(),
        "no document skipped across BSON types"
    );
    // Resume from every position: the remainder is exactly what follows.
    for (i, p) in positions.iter().enumerate() {
        let mut req = ReadRequest::new(coll("mixed"), 2);
        req.resume_after = Some(p.clone());
        let (rest, _) = read_all(&conn, req).await;
        let consumed = (2 * (i + 1)).min(mixed.len());
        assert_eq!(rest.len(), mixed.len() - consumed, "resume after batch {i}");
    }

    // --- custom sort keys with missing and null values are not skipped ---
    let sparse: Vec<Document> = (0..10)
        .map(|i| match i % 3 {
            0 => doc! { "_id": i, "k": i },
            1 => doc! { "_id": i, "k": Bson::Null },
            _ => doc! { "_id": i },
        })
        .collect();
    raw.collection::<Document>("sparse")
        .insert_many(sparse)
        .await
        .unwrap();
    let mut req = ReadRequest::new(coll("sparse"), 1);
    req.order_by = vec![gazabridge_plugin_sdk::filter::SortKey {
        field: gazabridge_plugin_sdk::filter::FieldPath::new("k"),
        direction: gazabridge_plugin_sdk::filter::SortDirection::Asc,
    }];
    let (sorted, _) = read_all(&conn, req).await;
    assert_eq!(
        sorted.len(),
        10,
        "missing/null sort keys must not lose documents"
    );

    // --- exotic BSON types survive a read → write copy unchanged ---
    let exotic = doc! {
        "_id": 1,
        "re": mongodb::bson::Regex { pattern: "^x".into(), options: "i".into() },
        "ts": mongodb::bson::Timestamp { time: 5, increment: 6 },
        "nested": { "n": 3i32, "arr": [1i32, 2i64] },
        "dec": Bson::Decimal128(mongodb::bson::Decimal128::from_bytes(0x3040_0000_0000_0000_0000_0000_0000_0001u128.to_le_bytes())),
    };
    raw.collection::<Document>("src")
        .insert_one(exotic.clone())
        .await
        .unwrap();
    let (records, _) = read_all(&conn, ReadRequest::new(coll("src"), 10)).await;
    conn.write_batch(
        WriteBatch {
            object: coll("dst"),
            mode: WriteMode::Insert,
            records,
            key_fields: vec![],
            atomic: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let copied = raw
        .collection::<Document>("dst")
        .find_one(doc! {})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(copied, exotic, "copy must be byte-for-byte identical");

    // --- decimals beyond decimal128 are stored as exact text ---
    let mut r = Record::new();
    r.insert("_id", Value::Int(1));
    r.insert(
        "amount",
        Value::Decimal("1234567890123456789012345678.0123456789".parse().unwrap()),
    );
    conn.write_batch(
        WriteBatch {
            object: coll("money"),
            mode: WriteMode::Insert,
            records: vec![r],
            key_fields: vec![],
            atomic: true,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let stored = raw
        .collection::<Document>("money")
        .find_one(doc! {})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.get_str("amount").unwrap(),
        "1234567890123456789012345678.0123456789"
    );

    // --- per-document failures from schema validation ---
    raw.run_command(doc! {
        "create": "validated",
        "validator": { "$jsonSchema": { "required": ["name"] } },
    })
    .await
    .unwrap();
    let good = |id: i64| {
        Record::from_pairs([("_id", Value::Int(id)), ("name", Value::String("n".into()))])
    };
    let bad = Record::from_pairs([("_id", Value::Int(3))]);
    let out = conn
        .write_batch(
            WriteBatch {
                object: coll("validated"),
                mode: WriteMode::Insert,
                records: vec![good(1), bad, good(2)],
                key_fields: vec![],
                atomic: false,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(out.inserted, 2);
    assert_eq!(out.failures.len(), 1);
    assert_eq!(
        (out.failures[0].index, out.failures[0].kind),
        (1, ErrorKind::Constraint)
    );

    // --- filters and pipelines ---
    let found = conn
        .execute(
            query(
                QueryLanguage::MongoFilter,
                "sparse",
                r#"{"k": {"$gte": 3}}"#,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(found.rows.len(), 3);
    let grouped = conn
        .execute(
            query(
                QueryLanguage::MongoAggregation,
                "mixed",
                r#"[{"$group": {"_id": "$v", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]"#,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(grouped.rows.len(), 6);
    let plan = conn
        .explain(
            query(QueryLanguage::MongoFilter, "sparse", r#"{"k": 3}"#),
            true,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(plan.analyzed && plan.native.get("executionStats").is_some());

    // --- inference sees mixed and optional fields ---
    let schema = conn
        .inspect(
            &coll("sparse"),
            &InspectOptions {
                sampling: SamplingMethod::FullScan,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let k = schema
        .inferred
        .as_ref()
        .unwrap()
        .fields
        .iter()
        .find(|f| f.path == "k")
        .unwrap();
    assert_eq!((k.present_count, k.null_count), (7, 3));
    assert!(
        schema
            .columns
            .iter()
            .find(|c| c.name == "k")
            .unwrap()
            .nullable
    );

    // --- read-only connections cannot write, even through pipelines ---
    let ro = connect(&host, port, true).await;
    let err = ro
        .execute(
            query(
                QueryLanguage::MongoAggregation,
                "sparse",
                r#"[{"$match": {}}, {"$out": "stolen"}]"#,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ReadOnly);
    assert!(
        raw.list_collection_names()
            .await
            .unwrap()
            .iter()
            .all(|n| n != "stolen")
    );
    assert_eq!(
        ro.execute(
            query(QueryLanguage::MongoFilter, "sparse", "{}"),
            CancellationToken::new()
        )
        .await
        .unwrap()
        .rows
        .len(),
        10
    );

    // --- nested Int32 fidelity through document_to_record (public API) ---
    let rec = document_to_record(&doc! { "a": { "b": [1i32] } });
    assert!(rec.get_canonical("a").unwrap().meta.is_some());
}
