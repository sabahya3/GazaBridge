//! SQLite-specific behaviour beyond the shared contract suite.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] functions

use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use gazabridge_connector_sqlite::{SqliteConnectorFactory, main_schema};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, SqliteJournalMode, SqliteSettings,
};
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::ddl::{ColumnDefinition, SchemaOperation, TableDefinition};
use gazabridge_plugin_sdk::filter::{FieldPath, SortDirection, SortKey};
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::read::ReadRequest;
use gazabridge_plugin_sdk::schema::{ConstraintKind, ObjectKind, ObjectRef};
use gazabridge_plugin_sdk::write::WriteBatch;
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ErrorKind, ResolvedSecrets};
use tokio_util::sync::CancellationToken;

async fn open(path: &Path, read_only: bool) -> Arc<dyn Connection> {
    let mut p = ConnectionProfile::new(
        "t",
        "sqlite.sqlite",
        ConnectionSettings::Sqlite(SqliteSettings {
            path: path.to_path_buf(),
            read_only,
            create_if_missing: !read_only,
            busy_timeout_ms: 5_000,
            journal_mode: SqliteJournalMode::Wal,
            foreign_keys: true,
        }),
    );
    p.read_only = read_only;
    SqliteConnectorFactory
        .connect(&p, ResolvedSecrets::default())
        .await
        .unwrap()
}

fn sql(text: &str) -> QueryRequest {
    QueryRequest {
        language: QueryLanguage::SqliteSql,
        text: text.into(),
        params: vec![],
        max_rows: 1000,
        timeout: None,
        database: None,
        collection: None,
    }
}

fn table(name: &str) -> ObjectRef {
    ObjectRef::table(main_schema(), name)
}

fn col(name: &str, t: CanonicalType, nullable: bool) -> ColumnDefinition {
    ColumnDefinition {
        name: name.into(),
        canonical_type: t,
        native_type: None,
        nullable,
        default_expression: None,
    }
}

async fn create(conn: &Arc<dyn Connection>, name: &str, columns: Vec<ColumnDefinition>) {
    conn.apply_ddl(
        &[SchemaOperation::CreateTable {
            table: TableDefinition {
                object: table(name),
                columns,
                primary_key: vec!["id".into()],
                foreign_keys: vec![],
            },
            if_not_exists: false,
        }],
        CancellationToken::new(),
    )
    .await
    .unwrap();
}

fn batch(name: &str, mode: WriteMode, records: Vec<Record>, atomic: bool) -> WriteBatch {
    WriteBatch {
        object: table(name),
        mode,
        records,
        key_fields: vec!["id".into()],
        atomic,
    }
}

async fn read_all(conn: &Arc<dyn Connection>, req: ReadRequest) -> Vec<Record> {
    let mut s = conn.read(req, CancellationToken::new()).await.unwrap();
    let mut out = Vec::new();
    while let Some(b) = s.next().await {
        out.extend(b.unwrap().records);
    }
    out
}

#[tokio::test]
async fn read_only_is_enforced_by_the_database_too() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ro.db");
    let rw = open(&path, false).await;
    rw.execute(
        sql("CREATE TABLE t (id INTEGER PRIMARY KEY)"),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    rw.close().await;

    let ro = open(&path, true).await;
    let err = ro
        .execute(
            sql("INSERT INTO t (id) VALUES (1)"),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ReadOnly, "{err}");
    let ok = ro
        .execute(sql("SELECT COUNT(*) AS n FROM t"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(ok.rows[0].get("n"), &Value::Int(0));
}

#[tokio::test]
async fn bad_records_are_isolated_and_atomic_batches_roll_back() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(&dir.path().join("w.db"), false).await;
    create(
        &conn,
        "people",
        vec![
            col("id", CanonicalType::I64, false),
            col("name", CanonicalType::TEXT, false),
        ],
    )
    .await;

    let good = |id: i64| {
        Record::from_pairs([
            ("id", Value::Int(id)),
            ("name", Value::String(format!("p{id}"))),
        ])
    };
    let bad = Record::from_pairs([("id", Value::Int(3)), ("name", Value::Null)]);
    let out = conn
        .write_batch(
            batch(
                "people",
                WriteMode::Insert,
                vec![good(1), good(2), bad.clone(), good(4)],
                false,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(out.inserted, 3);
    assert_eq!(out.failures.len(), 1);
    assert_eq!(out.failures[0].index, 2);
    assert_eq!(out.failures[0].kind, ErrorKind::Constraint);

    let err = conn
        .write_batch(
            batch("people", WriteMode::Insert, vec![good(10), bad], true),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Constraint);
    let n = conn
        .count(&table("people"), None, true)
        .await
        .unwrap()
        .count;
    assert_eq!(n, 3, "atomic batch must leave no partial writes");
}

#[tokio::test]
async fn upsert_and_skip_existing_report_accurate_counts() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(&dir.path().join("u.db"), false).await;
    create(
        &conn,
        "kv",
        vec![
            col("id", CanonicalType::I64, false),
            col("v", CanonicalType::TEXT, true),
        ],
    )
    .await;
    let rec = |id: i64, v: &str| {
        Record::from_pairs([("id", Value::Int(id)), ("v", Value::String(v.into()))])
    };

    conn.write_batch(
        batch(
            "kv",
            WriteMode::Insert,
            vec![rec(1, "a"), rec(2, "b")],
            false,
        ),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let up = conn
        .write_batch(
            batch(
                "kv",
                WriteMode::Upsert,
                vec![rec(2, "B"), rec(3, "c")],
                false,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!((up.inserted, up.updated), (1, 1));

    let skip = conn
        .write_batch(
            batch(
                "kv",
                WriteMode::SkipExisting,
                vec![rec(3, "x"), rec(4, "d")],
                false,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!((skip.inserted, skip.skipped), (1, 1));

    let orchestrated = conn
        .write_batch(
            batch("kv", WriteMode::Replace, vec![rec(9, "z")], false),
            CancellationToken::new(),
        )
        .await;
    assert_eq!(orchestrated.unwrap_err().kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn missing_fields_use_column_defaults_while_null_stays_null() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(&dir.path().join("d.db"), false).await;
    let mut status = col("status", CanonicalType::TEXT, true);
    status.default_expression = Some("'new'".into());
    create(
        &conn,
        "orders",
        vec![col("id", CanonicalType::I64, false), status],
    )
    .await;

    let missing = Record::from_pairs([("id", Value::Int(1))]);
    let null = Record::from_pairs([("id", Value::Int(2)), ("status", Value::Null)]);
    conn.write_batch(
        batch("orders", WriteMode::Insert, vec![missing, null], false),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut req = ReadRequest::new(table("orders"), 10);
    req.order_by = vec![SortKey {
        field: FieldPath::new("id"),
        direction: SortDirection::Asc,
    }];
    let rows = read_all(&conn, req).await;
    assert_eq!(rows[0].get("status"), &Value::String("new".into()));
    assert_eq!(rows[1].get("status"), &Value::Null);
}

#[tokio::test]
async fn special_values_round_trip_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(&dir.path().join("s.db"), false).await;
    create(
        &conn,
        "special",
        vec![
            col("id", CanonicalType::I64, false),
            col("f", CanonicalType::F64, true),
            col("u", CanonicalType::UInt { bits: 64 }, true),
            col("big", CanonicalType::BigInt, true),
            col("d", CanonicalType::Duration, true),
            col("j", CanonicalType::Json, true),
        ],
    )
    .await;
    let big: bigdecimal::num_bigint::BigInt = "-123456789012345678901234567890".parse().unwrap();
    let rows = vec![
        Record::from_pairs([
            ("id", Value::Int(1)),
            ("f", Value::Float(f64::NAN)),
            ("u", Value::UInt(u64::MAX)),
            ("big", Value::BigInt(big.clone())),
            (
                "d",
                Value::Duration(
                    gazabridge_domain::value::Duration::from_total_nanos(-1_500_000_000).unwrap(),
                ),
            ),
            (
                "j",
                Value::Json(serde_json::json!({"ar": "غزة", "n": [1, 2.5, null]})),
            ),
        ]),
        Record::from_pairs([
            ("id", Value::Int(2)),
            ("f", Value::Float(f64::NEG_INFINITY)),
        ]),
    ];
    conn.write_batch(
        batch("special", WriteMode::Insert, rows, true),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let got = read_all(&conn, ReadRequest::new(table("special"), 10)).await;
    assert!(matches!(got[0].get("f"), Value::Float(f) if f.is_nan()));
    assert_eq!(got[0].get("u"), &Value::UInt(u64::MAX));
    assert_eq!(got[0].get("big"), &Value::BigInt(big));
    assert_eq!(
        got[0].get("j"),
        &Value::Json(serde_json::json!({"ar": "غزة", "n": [1, 2.5, null]}))
    );
    assert!(matches!(got[0].get("d"), Value::Duration(d) if d.total_nanos() == -1_500_000_000));
    assert_eq!(got[1].get("f"), &Value::Float(f64::NEG_INFINITY));
}

#[tokio::test]
async fn views_stream_but_cannot_resume_and_scripts_run() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(&dir.path().join("v.db"), false).await;
    let script = conn
        .execute(
            sql("CREATE TABLE n (id INTEGER PRIMARY KEY, x INT);
                 INSERT INTO n (x) VALUES (1), (2), (3);
                 CREATE VIEW odd AS SELECT x FROM n WHERE x % 2 = 1;"),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(script.affected, Some(3));

    let view = ObjectRef {
        kind: ObjectKind::View,
        ..table("odd")
    };
    let rows = read_all(&conn, ReadRequest::new(view.clone(), 1)).await;
    assert_eq!(rows.len(), 2);

    let mut stream = conn
        .read(ReadRequest::new(view.clone(), 1), CancellationToken::new())
        .await
        .unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert!(
        first.position.is_none(),
        "views without keys have no resumable position"
    );

    let plan = conn
        .explain(
            sql("SELECT * FROM n WHERE id = 1"),
            false,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(
        plan.text.to_uppercase().contains("SEARCH") || plan.text.to_uppercase().contains("SCAN"),
        "{}",
        plan.text
    );
    assert_eq!(
        conn.explain(sql("SELECT 1"), true, CancellationToken::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
}

#[tokio::test]
async fn inspection_reports_indexes_and_foreign_keys() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(&dir.path().join("i.db"), false).await;
    conn.execute(
        sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE);
             CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE);
             CREATE INDEX child_parent ON child (parent_id);"),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let schema = conn
        .inspect(&table("child"), &Default::default())
        .await
        .unwrap();
    assert_eq!(schema.primary_key, vec!["id".to_string()]);
    assert!(
        schema
            .indexes
            .iter()
            .any(|i| i.name == "child_parent" && i.columns == ["parent_id"])
    );
    let fk = schema
        .constraints
        .iter()
        .find(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }))
        .expect("foreign key");
    assert_eq!(fk.columns, ["parent_id"]);
    let parent = conn
        .inspect(&table("parent"), &Default::default())
        .await
        .unwrap();
    assert!(
        parent
            .constraints
            .iter()
            .any(|c| matches!(c.kind, ConstraintKind::Unique) && c.columns == ["code"])
    );
}
