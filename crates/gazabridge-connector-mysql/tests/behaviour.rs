//! MySQL- and MariaDB-specific behaviour against real servers
//! (Testcontainers). Ignored by default; run with `cargo test -- --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bigdecimal::BigDecimal;
use futures::StreamExt;
use gazabridge_connector_mysql::MySqlConnectorFactory;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, MySqlSettings, MySqlTlsMode, SecretRef,
};
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::connector::InspectOptions;
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::query::{QueryRequest, QueryResult};
use gazabridge_plugin_sdk::read::ReadRequest;
use gazabridge_plugin_sdk::schema::{ConstraintKind, NamespacePath, ObjectKind, ObjectRef};
use gazabridge_plugin_sdk::write::{UpdateRequest, WriteBatch};
use gazabridge_plugin_sdk::{Connection, ConnectorFactory, ErrorKind, ResolvedSecrets};
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;

async fn connect(host: &str, port: u16, read_only: bool, timeout_ms: u64) -> Arc<dyn Connection> {
    let mut p = ConnectionProfile::new(
        "b",
        "mysql.mysql",
        ConnectionSettings::MySql(MySqlSettings {
            host: host.into(),
            port,
            database: Some("test".into()),
            username: "root".into(),
            password: SecretRef::None,
            tls_mode: MySqlTlsMode::Disabled,
            charset: "utf8mb4".into(),
            connect_timeout_secs: 20,
            query_timeout_ms: timeout_ms,
            pool_size: 4,
        }),
    );
    p.read_only = read_only;
    MySqlConnectorFactory
        .connect(&p, ResolvedSecrets::default())
        .await
        .unwrap()
}

fn sql(text: &str) -> QueryRequest {
    QueryRequest {
        language: QueryLanguage::MySql,
        text: text.into(),
        params: vec![],
        max_rows: 1000,
        timeout: None,
        database: None,
        collection: None,
    }
}

fn table(name: &str) -> ObjectRef {
    ObjectRef::table(NamespacePath::database("test"), name)
}

async fn exec(conn: &Arc<dyn Connection>, text: &str) -> QueryResult {
    conn.execute(sql(text), CancellationToken::new())
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"))
}

fn rec(fields: Vec<(&str, Value)>) -> Record {
    let mut r = Record::new();
    for (k, v) in fields {
        r.insert(k.to_owned(), v);
    }
    r
}

fn batch(name: &str, mode: WriteMode, records: Vec<Record>) -> WriteBatch {
    WriteBatch {
        object: table(name),
        mode,
        records,
        key_fields: vec!["id".into()],
        atomic: false,
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

async fn behaviour(host: &str, port: u16, mariadb: bool) {
    let conn = connect(host, port, false, 0).await;

    // ---- server detection -------------------------------------------------
    let info = conn.server_info().await.unwrap();
    assert_eq!(info.product, if mariadb { "MariaDB" } else { "MySQL" });
    assert!(info.properties.contains_key("sql_mode"));
    let tz = exec(
        &conn,
        "SELECT @@session.time_zone AS tz, @@session.sql_mode AS m",
    )
    .await;
    assert_eq!(tz.rows[0].get("tz"), &Value::String("+00:00".into()));
    let Value::String(mode) = tz.rows[0].get("m") else {
        panic!()
    };
    assert!(
        mode.contains("STRICT_ALL_TABLES") && !mode.contains("NO_BACKSLASH_ESCAPES"),
        "{mode}"
    );

    // ---- native types ------------------------------------------------------
    exec(
        &conn,
        "CREATE TABLE kinds (
            id INT PRIMARY KEY,
            price DECIMAL(12,4),
            doc JSON,
            colour ENUM('red','it''s'),
            note VARCHAR(40) CHARACTER SET utf8mb4,
            seen TIMESTAMP(3) NULL,
            flag TINYINT(1),
            raw VARBINARY(8),
            parent INT,
            CONSTRAINT fk_parent FOREIGN KEY (parent) REFERENCES kinds (id)
        )",
    )
    .await;
    let schema = conn
        .inspect(&table("kinds"), &InspectOptions::default())
        .await
        .unwrap();
    let ty = |n: &str| schema.column(n).unwrap().canonical_type.clone();
    assert_eq!(
        ty("price"),
        CanonicalType::Decimal {
            precision: Some(12),
            scale: Some(4)
        }
    );
    assert_eq!(ty("flag"), CanonicalType::Boolean);
    assert!(
        matches!(ty("colour"), CanonicalType::Enum { ref labels, .. } if labels == &["red", "it's"])
    );
    assert_eq!(
        ty("seen"),
        CanonicalType::UtcDateTime {
            fractional_digits: 3
        }
    );
    if !mariadb {
        // MariaDB stores JSON as LONGTEXT with a validity check.
        assert_eq!(ty("doc"), CanonicalType::Json);
    }
    assert!(
        schema
            .constraints
            .iter()
            .any(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }))
    );
    assert_eq!(schema.primary_key, vec!["id".to_string()]);

    let at = chrono::DateTime::parse_from_rfc3339("2031-05-06T07:08:09.123Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let out = conn
        .write_batch(
            batch(
                "kinds",
                WriteMode::Insert,
                vec![rec(vec![
                    ("id", Value::Int(1)),
                    (
                        "price",
                        Value::Decimal(BigDecimal::from_str("12345678.1234").unwrap()),
                    ),
                    (
                        "doc",
                        Value::Json(serde_json::json!({"a": [1, 2], "ب": "غزة"})),
                    ),
                    ("colour", Value::String("it's".into())),
                    ("note", Value::String("emoji 🚀 and عربي".into())),
                    ("seen", Value::UtcDateTime(at)),
                    ("flag", Value::Bool(true)),
                    ("raw", Value::Binary(vec![0, 255, 1])),
                    ("parent", Value::Null),
                ])],
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(out.inserted, 1, "{out:?}");
    let got = read_all(&conn, "kinds").await;
    let r = &got[0];
    assert_eq!(
        r.get("price"),
        &Value::Decimal(BigDecimal::from_str("12345678.1234").unwrap())
    );
    if !mariadb {
        assert_eq!(
            r.get("doc"),
            &Value::Json(serde_json::json!({"a": [1, 2], "ب": "غزة"}))
        );
    }
    assert!(matches!(r.get("colour"), Value::Enum { label, .. } if label == "it's"));
    assert_eq!(r.get("note"), &Value::String("emoji 🚀 and عربي".into()));
    assert_eq!(r.get("seen"), &Value::UtcDateTime(at));
    assert_eq!(r.get("flag"), &Value::Bool(true));
    assert_eq!(r.get("raw"), &Value::Binary(vec![0, 255, 1]));

    // Strict mode: values that do not fit are record failures, not clipped.
    let out = conn
        .write_batch(
            batch(
                "kinds",
                WriteMode::Insert,
                vec![
                    rec(vec![
                        ("id", Value::Int(2)),
                        ("note", Value::String("x".repeat(41))),
                    ]),
                    rec(vec![("id", Value::Int(3)), ("parent", Value::Int(99))]),
                    rec(vec![
                        ("id", Value::Int(4)),
                        ("note", Value::String("ok".into())),
                    ]),
                ],
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(out.inserted, 1);
    let kinds: Vec<_> = out.failures.iter().map(|f| (f.index, f.kind)).collect();
    assert_eq!(
        kinds,
        vec![(0, ErrorKind::Conversion), (1, ErrorKind::Constraint)],
        "{out:?}"
    );

    // ---- upsert and skip-existing counts -----------------------------------
    exec(
        &conn,
        "CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(20))",
    )
    .await;
    let people = |names: &[(i64, &str)]| {
        names
            .iter()
            .map(|(i, n)| {
                rec(vec![
                    ("id", Value::Int(*i)),
                    ("name", Value::String((*n).into())),
                ])
            })
            .collect::<Vec<_>>()
    };
    let o = conn
        .write_batch(
            batch("people", WriteMode::Insert, people(&[(1, "a"), (2, "b")])),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(o.inserted, 2);
    let o = conn
        .write_batch(
            batch(
                "people",
                WriteMode::Upsert,
                people(&[(1, "a"), (2, "B"), (3, "c")]),
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!((o.inserted, o.updated), (1, 2), "{o:?}");
    let o = conn
        .write_batch(
            batch(
                "people",
                WriteMode::SkipExisting,
                people(&[(3, "zz"), (4, "d")]),
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!((o.inserted, o.skipped), (1, 1), "{o:?}");
    let names: Vec<_> = read_all(&conn, "people")
        .await
        .iter()
        .map(|r| r.get("name").clone())
        .collect();
    assert_eq!(names, ["a", "B", "c", "d"].map(|s| Value::String(s.into())));

    // Updates report matched rows even when the value does not change.
    let u = conn
        .update(UpdateRequest {
            object: table("people"),
            filter: Filter::In {
                field: FieldPath::new("id"),
                values: vec![Value::Int(1), Value::Int(2)],
            },
            set: rec(vec![("name", Value::String("a".into()))]),
            expect_matched: Some(2),
        })
        .await
        .unwrap();
    assert_eq!((u.matched, u.modified), (2, 1));
    let stale = conn
        .update(UpdateRequest {
            object: table("people"),
            filter: Filter::eq("id", Value::Int(1)),
            set: rec(vec![("name", Value::String("x".into()))]),
            expect_matched: Some(2),
        })
        .await
        .unwrap_err();
    assert_eq!(stale.kind(), ErrorKind::Conflict);
    // Case-sensitive filters despite the case-insensitive collation.
    let page = conn
        .read_page(
            gazabridge_plugin_sdk::read::PageRequest {
                object: table("people"),
                projection: vec![],
                exclude: vec![],
                filter: Some(Filter::Contains {
                    field: FieldPath::new("name"),
                    text: "A".into(),
                }),
                order_by: vec![],
                offset: 0,
                limit: 10,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(page.records.is_empty(), "{:?}", page.records);

    // ---- queries -------------------------------------------------------------
    let mut q = sql("SELECT id, name FROM people WHERE id >= ? ORDER BY id");
    q.params = vec![Value::Int(2)];
    q.max_rows = 2;
    let r = conn.execute(q, CancellationToken::new()).await.unwrap();
    assert_eq!(r.rows.len(), 2);
    assert!(r.truncated);
    assert_eq!(r.rows[0].get("id"), &Value::Int(2));
    // Scripts run through the text protocol and report affected rows.
    let r = exec(
        &conn,
        "UPDATE people SET name = 'q' WHERE id = 4; UPDATE people SET name = 'q' WHERE id = 3",
    )
    .await;
    assert_eq!(r.affected, Some(2));
    let r = exec(&conn, "SET @x = 5; SELECT @x + 1 AS y").await;
    assert_eq!(r.rows.len(), 1);

    let plan = conn
        .explain(
            sql("SELECT * FROM people WHERE id = 1"),
            false,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(plan.native.is_object(), "{}", plan.text);
    let analyzed = conn
        .explain(sql("DELETE FROM people"), true, CancellationToken::new())
        .await
        .unwrap();
    assert!(analyzed.analyzed);
    assert_eq!(
        read_all(&conn, "people").await.len(),
        4,
        "EXPLAIN ANALYZE must roll back"
    );

    // ---- cancel and timeouts kill the server-side statement ------------------
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        c2.cancel();
    });
    let started = Instant::now();
    let e = conn
        .execute(sql("SELECT SLEEP(30)"), cancel)
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Cancelled);
    assert!(started.elapsed() < Duration::from_secs(5));
    let mut q = sql("SELECT SLEEP(30)");
    q.timeout = Some(Duration::from_millis(300));
    let e = conn.execute(q, CancellationToken::new()).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Timeout);
    // Nothing is left running.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = exec(
        &conn,
        "SELECT COUNT(*) AS n FROM information_schema.PROCESSLIST WHERE INFO LIKE 'SELECT SLEEP%'",
    )
    .await;
    assert_eq!(
        r.rows[0].get("n"),
        &Value::Int(0),
        "a killed query is still running"
    );
    let n = conn
        .count_within(&table("people"), None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(n.count, 4);

    // ---- read-only connections --------------------------------------------------
    let ro = connect(host, port, true, 0).await;
    assert_eq!(
        exec(&ro, "SELECT COUNT(*) AS n FROM people")
            .await
            .rows
            .len(),
        1
    );
    for stmt in [
        "DELETE FROM people",
        "DROP TABLE people",
        "SELECT 1; DROP TABLE people",
    ] {
        let e = ro
            .execute(sql(stmt), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(e.kind(), ErrorKind::ReadOnly, "{stmt}");
    }

    // ---- listing ---------------------------------------------------------------
    exec(&conn, "CREATE VIEW people_v AS SELECT id FROM people").await;
    let entries = conn.list(&NamespacePath::database("test")).await.unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.name == "people_v" && e.kind == ObjectKind::View)
    );
    let dbs = conn.list(&NamespacePath::root()).await.unwrap();
    assert!(dbs.iter().any(|e| e.name == "test"));
    assert!(!dbs.iter().any(|e| e.name == "mysql"));
    let v = conn
        .inspect(&table("people_v"), &InspectOptions::default())
        .await
        .unwrap();
    assert!(v.definition.is_some());
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn mysql_8_4_specific_behaviour() {
    let c = Mysql::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("container");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    behaviour(&host, port, false).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn mariadb_11_4_specific_behaviour() {
    let c = Mariadb::default()
        .with_tag("11.4")
        .start()
        .await
        .expect("container");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    behaviour(&host, port, true).await;
}
