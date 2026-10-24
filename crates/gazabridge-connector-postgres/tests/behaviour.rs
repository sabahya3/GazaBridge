//! PostgreSQL-specific behaviour against a real server (Testcontainers).
//! Ignored by default; run with `cargo test -- --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bigdecimal::BigDecimal;
use futures::StreamExt;
use gazabridge_connector_postgres::{PostgresConnectorFactory, public_schema};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, PgSslMode, PostgresSettings, SecretRef,
};
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::value::Interval;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::read::ReadRequest;
use gazabridge_plugin_sdk::schema::{ConstraintKind, ObjectRef};
use gazabridge_plugin_sdk::write::WriteBatch;
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ErrorKind, ResolvedSecrets};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

struct Server {
    host: String,
    port: u16,
    _c: ContainerAsync<Postgres>,
}

impl Server {
    async fn start() -> Self {
        let c = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .expect("container");
        Self {
            host: c.get_host().await.unwrap().to_string(),
            port: c.get_host_port_ipv4(5432).await.unwrap(),
            _c: c,
        }
    }

    async fn connect(&self, read_only: bool, statement_timeout_ms: u64) -> Arc<dyn Connection> {
        let mut p = ConnectionProfile::new(
            "b",
            "postgres.postgresql",
            ConnectionSettings::Postgres(PostgresSettings {
                host: self.host.clone(),
                port: self.port,
                database: "postgres".into(),
                username: "postgres".into(),
                password: SecretRef::Session { key: "b".into() },
                ssl_mode: PgSslMode::Disable,
                search_path: None,
                application_name: "GazaBridge behaviour".into(),
                connect_timeout_secs: 10,
                statement_timeout_ms,
                pool_size: 4,
            }),
        );
        p.read_only = read_only;
        let secrets = ResolvedSecrets {
            password: Some(Zeroizing::new("postgres".into())),
            ..Default::default()
        };
        PostgresConnectorFactory.connect(&p, secrets).await.unwrap()
    }
}

fn sql(text: &str) -> QueryRequest {
    QueryRequest {
        language: QueryLanguage::PostgresSql,
        text: text.into(),
        params: vec![],
        max_rows: 1000,
        timeout: None,
        database: None,
        collection: None,
    }
}

fn table(name: &str) -> ObjectRef {
    ObjectRef::table(public_schema(), name)
}

async fn exec(conn: &Arc<dyn Connection>, text: &str) -> gazabridge_plugin_sdk::query::QueryResult {
    conn.execute(sql(text), CancellationToken::new())
        .await
        .unwrap()
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

async fn read_all(conn: &Arc<dyn Connection>, name: &str) -> Vec<Record> {
    let mut req = ReadRequest::new(table(name), 100);
    req.order_by = vec![SortKey {
        field: FieldPath::new("id"),
        direction: SortDirection::Asc,
    }];
    let mut s = conn.read(req, CancellationToken::new()).await.unwrap();
    let mut out = Vec::new();
    while let Some(b) = s.next().await {
        out.extend(b.unwrap().records);
    }
    out
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn postgres_specific_behaviour() {
    let server = Server::start().await;
    let rw = server.connect(false, 0).await;

    // --- types: arrays, enums, jsonb, numeric NaN, interval, inet (extension) ---
    exec(
        &rw,
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
         CREATE TABLE typed (
           id int PRIMARY KEY, tags text[], grid int[][], m mood, doc jsonb, n numeric, iv interval,
           addr inet, big numeric(40,5), ts timestamptz, d date);",
    )
    .await;
    let rec = Record::from_pairs([
        ("id", Value::Int(1)),
        (
            "tags",
            Value::Array(vec![
                Value::String("a \"quoted\" tag".into()),
                Value::Null,
                Value::String("عربي".into()),
            ]),
        ),
        (
            "grid",
            Value::Array(vec![
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
                Value::Array(vec![Value::Int(3), Value::Int(4)]),
            ]),
        ),
        (
            "m",
            Value::Enum {
                type_name: Some("mood".into()),
                label: "happy".into(),
            },
        ),
        (
            "doc",
            Value::Json(serde_json::json!({"k": [1, 2.5, null], "ar": "غزة"})),
        ),
        ("n", Value::Float(f64::NAN)),
        (
            "iv",
            Value::Interval(Interval {
                months: 14,
                days: -3,
                microseconds: 4_500_001,
            }),
        ),
        (
            "addr",
            Value::Extension {
                type_name: "inet".into(),
                text: Some("192.168.0.1/24".into()),
                binary: None,
            },
        ),
        (
            "big",
            Value::Decimal(
                BigDecimal::from_str("12345678901234567890123456789012345.12345").unwrap(),
            ),
        ),
        ("ts", Value::String("2024-03-31T00:30:00+02:00".into())),
        ("d", Value::String("infinity".into())),
    ]);
    let out = rw
        .write_batch(
            batch("typed", WriteMode::Insert, vec![rec], true),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(out.inserted, 1);
    let got = &read_all(&rw, "typed").await[0];
    assert_eq!(
        got.get("tags"),
        &Value::Array(vec![
            Value::String("a \"quoted\" tag".into()),
            Value::Null,
            Value::String("عربي".into())
        ])
    );
    assert_eq!(
        got.get("grid"),
        &Value::Array(vec![
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Array(vec![Value::Int(3), Value::Int(4)])
        ])
    );
    assert_eq!(
        got.get("m"),
        &Value::Enum {
            type_name: Some("mood".into()),
            label: "happy".into()
        }
    );
    assert_eq!(
        got.get("doc"),
        &Value::Json(serde_json::json!({"k": [1, 2.5, null], "ar": "غزة"}))
    );
    assert!(matches!(got.get("n"), Value::Float(f) if f.is_nan()));
    assert_eq!(
        got.get("iv"),
        &Value::Interval(Interval {
            months: 14,
            days: -3,
            microseconds: 4_500_001
        })
    );
    assert_eq!(
        got.get("addr"),
        &Value::Extension {
            type_name: "inet".into(),
            text: Some("192.168.0.1/24".into()),
            binary: None
        }
    );
    assert_eq!(
        got.get("big"),
        &Value::Decimal(BigDecimal::from_str("12345678901234567890123456789012345.12345").unwrap())
    );
    assert_eq!(
        got.get("ts"),
        &Value::UtcDateTime(
            chrono::DateTime::parse_from_rfc3339("2024-03-30T22:30:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc)
        )
    );
    assert!(matches!(got.get("d"), Value::Extension { text: Some(t), .. } if t == "infinity"));

    // Filters on array and enum columns bind correctly.
    let mut req = ReadRequest::new(table("typed"), 10);
    req.filter = Some(Filter::eq("m", Value::String("happy".into())));
    let mut s = rw.read(req, CancellationToken::new()).await.unwrap();
    assert_eq!(s.next().await.unwrap().unwrap().len(), 1);

    let schema = rw
        .inspect(&table("typed"), &Default::default())
        .await
        .unwrap();
    assert!(
        matches!(&schema.column("m").unwrap().canonical_type, gazabridge_domain::CanonicalType::Enum { labels, .. } if labels.len() == 3)
    );

    // --- failure isolation and atomic rollback ---
    exec(
        &rw,
        "CREATE TABLE people (id int PRIMARY KEY, name text NOT NULL, age int CHECK (age >= 0))",
    )
    .await;
    let p = |id: i64, name: Value, age: i64| {
        Record::from_pairs([
            ("id", Value::Int(id)),
            ("name", name),
            ("age", Value::Int(age)),
        ])
    };
    let out = rw
        .write_batch(
            batch(
                "people",
                WriteMode::Insert,
                vec![
                    p(1, Value::String("a".into()), 1),
                    p(2, Value::Null, 2),
                    p(3, Value::String("c".into()), -1),
                    p(4, Value::String("d".into()), 4),
                ],
                false,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(out.inserted, 2);
    let kinds: Vec<_> = out.failures.iter().map(|f| (f.index, f.kind)).collect();
    assert_eq!(
        kinds,
        vec![(1, ErrorKind::Constraint), (2, ErrorKind::Constraint)]
    );
    let err = rw
        .write_batch(
            batch(
                "people",
                WriteMode::Insert,
                vec![p(10, Value::String("x".into()), 1), p(11, Value::Null, 1)],
                true,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Constraint);
    assert_eq!(
        rw.count(&table("people"), None, true).await.unwrap().count,
        2
    );

    // Bad text for a typed column is a per-record conversion failure.
    let out = rw
        .write_batch(
            batch(
                "people",
                WriteMode::Insert,
                vec![Record::from_pairs([
                    ("id", Value::Int(20)),
                    ("name", Value::String("n".into())),
                    ("age", Value::String("old".into())),
                ])],
                false,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(out.failures[0].kind, ErrorKind::Conversion);

    // --- upsert counts ---
    let up = rw
        .write_batch(
            batch(
                "people",
                WriteMode::Upsert,
                vec![
                    p(1, Value::String("A".into()), 9),
                    p(5, Value::String("e".into()), 5),
                ],
                false,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!((up.inserted, up.updated), (1, 1));
    let skip = rw
        .write_batch(
            batch(
                "people",
                WriteMode::SkipExisting,
                vec![
                    p(5, Value::String("z".into()), 0),
                    p(6, Value::String("f".into()), 6),
                ],
                false,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!((skip.inserted, skip.skipped), (1, 1));

    // --- constraints and indexes ---
    exec(&rw, "CREATE TABLE child (id int PRIMARY KEY, person_id int REFERENCES people(id) ON DELETE CASCADE); CREATE INDEX child_p ON child (person_id);").await;
    let child = rw
        .inspect(&table("child"), &Default::default())
        .await
        .unwrap();
    assert!(
        child
            .indexes
            .iter()
            .any(|i| i.name == "child_p" && i.columns == ["person_id"])
    );
    assert!(child.constraints.iter().any(|c| matches!(&c.kind, ConstraintKind::ForeignKey { on_delete: Some(a), .. } if a == "CASCADE")));

    // --- scripts, explain analyze rollback, parameter binding ---
    let script = exec(
        &rw,
        "CREATE TABLE s (x int); INSERT INTO s VALUES (1), (2), (3); CREATE INDEX s_x ON s (x);",
    )
    .await;
    assert_eq!(script.affected, Some(3));
    let plan = rw
        .explain(sql("DELETE FROM s"), true, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        plan.analyzed && plan.text.contains("Delete"),
        "{}",
        plan.text
    );
    assert_eq!(
        exec(&rw, "SELECT count(*) AS n FROM s").await.rows[0].get("n"),
        &Value::Int(3),
        "EXPLAIN ANALYZE must not change data"
    );
    let mut q = sql("SELECT x FROM s WHERE x >= $1 ORDER BY x");
    q.params = vec![Value::Int(2)];
    q.max_rows = 1;
    let r = rw.execute(q, CancellationToken::new()).await.unwrap();
    assert_eq!(r.rows.len(), 1);
    assert!(r.truncated);

    // --- cancellation and timeouts are server-side ---
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        c2.cancel();
    });
    let started = std::time::Instant::now();
    let err = rw
        .execute(sql("SELECT pg_sleep(30)"), cancel)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Cancelled);
    assert!(started.elapsed() < Duration::from_secs(5));
    let mut slow = sql("SELECT pg_sleep(30)");
    slow.timeout = Some(Duration::from_millis(300));
    assert_eq!(
        rw.execute(slow, CancellationToken::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Timeout
    );
    let timed = server.connect(false, 200).await;
    assert_eq!(
        timed
            .execute(sql("SELECT pg_sleep(5)"), CancellationToken::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Timeout
    );
    // The pool still works after cancellation.
    assert_eq!(
        exec(&rw, "SELECT 1 AS one").await.rows[0].get("one"),
        &Value::Int(1)
    );

    // --- read-only connections are enforced by the server ---
    let ro = server.connect(true, 0).await;
    let err = ro
        .execute(sql("INSERT INTO s VALUES (9)"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ReadOnly, "{err}");
    let err = ro
        .execute(
            sql("SET TRANSACTION READ WRITE; INSERT INTO s VALUES (9)"),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err}");
    let err = ro
        .explain(sql("DELETE FROM s"), true, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ReadOnly, "{err}");
    assert_eq!(
        exec(&rw, "SELECT count(*) AS n FROM s").await.rows[0].get("n"),
        &Value::Int(3)
    );

    // --- transactions dropped without commit leave no trace ---
    {
        let mut tx = rw.begin().await.unwrap();
        tx.write_batch(batch(
            "people",
            WriteMode::Insert,
            vec![p(100, Value::String("t".into()), 1)],
            true,
        ))
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let n = rw
        .count(
            &table("people"),
            Some(&Filter::eq("id", Value::Int(100))),
            true,
        )
        .await
        .unwrap()
        .count;
    assert_eq!(n, 0);
}
