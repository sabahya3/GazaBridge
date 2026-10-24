//! Batched writes, updates and deletes.
//!
//! Each batch runs inside the caller's transaction. Rows are inserted with
//! multi-row statements inside a savepoint; if a chunk fails because of a
//! record-level problem (duplicate key, constraint, conversion), the chunk is
//! rolled back and retried row by row so that only the offending records are
//! reported and everything else is written.

use gazabridge_domain::Record;
use gazabridge_domain::job::WriteMode;
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, RecordFailure, UpdateRequest, WriteBatch, WriteOutcome,
};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use sqlx::SqliteConnection;

use crate::error::map_sqlx;
use crate::meta::TableMeta;
use crate::sql::{SqlParts, filter_sql, qualified, quote_ident};
use crate::types::{Bind, bind_all, encode};

/// SQLite's default maximum number of host parameters (since 3.32).
pub const MAX_BIND_PARAMETERS: usize = 32_766;
const MAX_ROWS_PER_STATEMENT: usize = 500;

/// Write modes applied directly by this connector. Replace, Merge and
/// Truncate-and-insert are orchestrated by the engine with DDL plus these.
pub const DIRECT_MODES: [WriteMode; 5] = [
    WriteMode::Insert,
    WriteMode::Append,
    WriteMode::Upsert,
    WriteMode::SkipExisting,
    WriteMode::FailOnConflict,
];

async fn exec(conn: &mut SqliteConnection, sql: &str) -> ConnectorResult<()> {
    sqlx::query(sql)
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    Ok(())
}

/// Columns present in a record (Missing fields are omitted so the column
/// default applies).
fn signature(r: &Record) -> Vec<String> {
    r.fields
        .iter()
        .filter(|(_, v)| !matches!(v.value, gazabridge_domain::Value::Missing))
        .map(|(k, _)| k.clone())
        .collect()
}

struct Statement {
    head: String,
    tail: String,
    columns: Vec<String>,
}

fn statement(
    meta: &TableMeta,
    table: &str,
    columns: Vec<String>,
    batch: &WriteBatch,
) -> ConnectorResult<Statement> {
    for c in &columns {
        if meta.column_type(c).is_none() {
            return Err(ConnectorError::invalid_input(format!(
                "unknown column `{c}`"
            )));
        }
    }
    let quoted = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<ConnectorResult<Vec<_>>>()?;
    let head = format!("INSERT INTO {table} ({}) VALUES ", quoted.join(", "));
    let keys = batch
        .key_fields
        .iter()
        .map(|k| quote_ident(k))
        .collect::<ConnectorResult<Vec<_>>>()?;
    let tail = match batch.mode {
        WriteMode::Upsert => {
            let updates: Vec<String> = columns
                .iter()
                .filter(|c| !batch.key_fields.contains(c))
                .map(|c| quote_ident(c).map(|q| format!("{q} = excluded.{q}")))
                .collect::<ConnectorResult<_>>()?;
            if updates.is_empty() {
                format!(" ON CONFLICT ({}) DO NOTHING", keys.join(", "))
            } else {
                format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    keys.join(", "),
                    updates.join(", ")
                )
            }
        }
        WriteMode::SkipExisting => format!(" ON CONFLICT ({}) DO NOTHING", keys.join(", ")),
        _ => String::new(),
    };
    Ok(Statement {
        head,
        tail,
        columns,
    })
}

fn row_binds(meta: &TableMeta, columns: &[String], r: &Record) -> ConnectorResult<Vec<Bind>> {
    columns
        .iter()
        .map(|c| encode(r.get(c), meta.column_type(c)))
        .collect()
}

async fn insert_rows(
    conn: &mut SqliteConnection,
    st: &Statement,
    rows: &[Vec<Bind>],
) -> Result<u64, ConnectorError> {
    let one = format!("({})", vec!["?"; st.columns.len()].join(", "));
    let sql = format!("{}{}{}", st.head, vec![one; rows.len()].join(", "), st.tail);
    let binds: Vec<Bind> = rows.iter().flatten().cloned().collect();
    let r = bind_all(sqlx::query(&sql), &binds)
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    Ok(r.rows_affected())
}

async fn count_existing(
    conn: &mut SqliteConnection,
    meta: &TableMeta,
    table: &str,
    keys: &[String],
    records: &[Record],
) -> ConnectorResult<u64> {
    let cols = keys
        .iter()
        .map(|k| quote_ident(k))
        .collect::<ConnectorResult<Vec<_>>>()?;
    let per_row = format!("({})", vec!["?"; keys.len()].join(", "));
    let mut total = 0u64;
    let rows_per_query = (MAX_BIND_PARAMETERS / keys.len().max(1)).clamp(1, MAX_ROWS_PER_STATEMENT);
    for chunk in records.chunks(rows_per_query) {
        let sql = format!(
            "SELECT COUNT(*) FROM {table} WHERE ({}) IN (VALUES {})",
            cols.join(", "),
            vec![per_row.as_str(); chunk.len()].join(", ")
        );
        let mut binds = Vec::with_capacity(chunk.len() * keys.len());
        for r in chunk {
            for k in keys {
                binds.push(encode(r.get(k), meta.column_type(k))?);
            }
        }
        let n: i64 = bind_all(sqlx::query(&sql), &binds)
            .fetch_one(&mut *conn)
            .await
            .map_err(map_sqlx)
            .and_then(|row| {
                use sqlx::Row;
                row.try_get::<i64, _>(0).map_err(map_sqlx)
            })?;
        total += n.max(0) as u64;
    }
    Ok(total)
}

fn failure(index: usize, e: &ConnectorError) -> RecordFailure {
    RecordFailure {
        index,
        kind: e.kind(),
        message: e.to_string(),
        code: e.code().map(str::to_owned),
    }
}

/// Writes a batch using `conn`, which must already be inside a transaction.
pub async fn write_records(
    conn: &mut SqliteConnection,
    meta: &TableMeta,
    batch: &WriteBatch,
) -> ConnectorResult<WriteOutcome> {
    if !DIRECT_MODES.contains(&batch.mode) {
        return Err(ConnectorError::invalid_input(format!(
            "write mode {:?} is orchestrated by the migration engine, not applied by the connector",
            batch.mode
        )));
    }
    if batch.mode.requires_key() {
        if batch.key_fields.is_empty() {
            return Err(ConnectorError::invalid_input(
                "this write mode needs key fields",
            ));
        }
        for k in &batch.key_fields {
            if meta.column_type(k).is_none() {
                return Err(ConnectorError::invalid_input(format!(
                    "unknown key column `{k}`"
                )));
            }
        }
    }
    let table = qualified(&batch.object)?;
    let mut outcome = WriteOutcome::default();
    let existing = if batch.mode == WriteMode::Upsert {
        count_existing(conn, meta, &table, &batch.key_fields, &batch.records).await?
    } else {
        0
    };

    // Group consecutive records with the same column set.
    let mut start = 0;
    while start < batch.records.len() {
        let sig = signature(&batch.records[start]);
        let mut end = start + 1;
        while end < batch.records.len() && signature(&batch.records[end]) == sig {
            end += 1;
        }
        if sig.is_empty() {
            return Err(ConnectorError::invalid_input(
                "record has no fields to write",
            ));
        }
        let st = statement(meta, &table, sig, batch)?;
        let rows_per_stmt =
            (MAX_BIND_PARAMETERS / st.columns.len()).clamp(1, MAX_ROWS_PER_STATEMENT);
        let mut chunk_start = start;
        while chunk_start < end {
            let chunk_end = (chunk_start + rows_per_stmt).min(end);
            write_chunk(conn, meta, &st, batch, chunk_start, chunk_end, &mut outcome).await?;
            chunk_start = chunk_end;
        }
        start = end;
    }

    match batch.mode {
        WriteMode::Upsert => {
            // `changes()` counts both inserted and updated rows.
            let touched = outcome.inserted;
            outcome.updated = existing.min(touched);
            outcome.inserted = touched - outcome.updated;
        }
        WriteMode::SkipExisting => {
            let attempted = (batch.records.len() - outcome.failures.len()) as u64;
            outcome.skipped = attempted.saturating_sub(outcome.inserted);
        }
        _ => {}
    }
    if batch.atomic
        && let Some(f) = outcome.failures.first()
    {
        return Err(ConnectorError::new(
            f.kind,
            format!(
                "atomic batch rejected: record {} failed: {}",
                f.index, f.message
            ),
        ));
    }
    Ok(outcome)
}

async fn write_chunk(
    conn: &mut SqliteConnection,
    meta: &TableMeta,
    st: &Statement,
    batch: &WriteBatch,
    from: usize,
    to: usize,
    outcome: &mut WriteOutcome,
) -> ConnectorResult<()> {
    // Encode first: conversion failures are per record.
    let mut rows = Vec::with_capacity(to - from);
    let mut indexes = Vec::with_capacity(to - from);
    for i in from..to {
        match row_binds(meta, &st.columns, &batch.records[i]) {
            Ok(b) => {
                rows.push(b);
                indexes.push(i);
            }
            Err(e) if batch.atomic => return Err(e),
            Err(e) => outcome.failures.push(failure(i, &e)),
        }
    }
    if rows.is_empty() {
        return Ok(());
    }
    exec(conn, "SAVEPOINT gb_chunk").await?;
    match insert_rows(conn, st, &rows).await {
        Ok(n) => {
            exec(conn, "RELEASE gb_chunk").await?;
            outcome.inserted += n;
            Ok(())
        }
        Err(e) => {
            exec(conn, "ROLLBACK TO gb_chunk").await?;
            exec(conn, "RELEASE gb_chunk").await?;
            if batch.atomic || !e.kind().is_record_level() {
                return Err(e);
            }
            // Isolate the failing records.
            for (row, &i) in rows.iter().zip(&indexes) {
                exec(conn, "SAVEPOINT gb_row").await?;
                match insert_rows(conn, st, std::slice::from_ref(row)).await {
                    Ok(n) => {
                        exec(conn, "RELEASE gb_row").await?;
                        outcome.inserted += n;
                    }
                    Err(e) if e.kind().is_record_level() => {
                        exec(conn, "ROLLBACK TO gb_row").await?;
                        exec(conn, "RELEASE gb_row").await?;
                        outcome.failures.push(failure(i, &e));
                    }
                    Err(e) => {
                        exec(conn, "ROLLBACK TO gb_row").await?;
                        exec(conn, "RELEASE gb_row").await?;
                        return Err(e);
                    }
                }
            }
            Ok(())
        }
    }
}

fn check_expected(matched: u64, expected: Option<u64>) -> ConnectorResult<()> {
    match expected {
        Some(e) if e != matched => Err(ConnectorError::new(
            ErrorKind::Conflict,
            format!(
                "expected {e} matching record(s) but found {matched}; the data changed or the filter is wrong"
            ),
        )),
        _ => Ok(()),
    }
}

/// Runs an UPDATE inside the caller's transaction. The caller rolls back on error.
pub async fn update(
    conn: &mut SqliteConnection,
    meta: &TableMeta,
    req: &UpdateRequest,
) -> ConnectorResult<MutationOutcome> {
    if !req.filter.is_restrictive() {
        return Err(ConnectorError::invalid_input(
            "UPDATE without a restrictive filter is not allowed",
        ));
    }
    if req.set.is_empty() {
        return Err(ConnectorError::invalid_input(
            "UPDATE needs at least one column",
        ));
    }
    let mut parts = SqlParts::default();
    let mut sets = Vec::with_capacity(req.set.len());
    for (name, v) in &req.set.fields {
        let t = meta
            .column_type(name)
            .ok_or_else(|| ConnectorError::invalid_input(format!("unknown column `{name}`")))?;
        sets.push(format!("{} = ?", quote_ident(name)?));
        parts.binds.push(encode(&v.value, Some(t))?);
    }
    filter_sql(&req.filter, &meta.types(), &mut parts)?;
    let sql = format!(
        "UPDATE {} SET {} WHERE {}",
        qualified(&req.object)?,
        sets.join(", "),
        parts.sql
    );
    let r = bind_all(sqlx::query(&sql), &parts.binds)
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    check_expected(r.rows_affected(), req.expect_matched)?;
    Ok(MutationOutcome {
        matched: r.rows_affected(),
        modified: r.rows_affected(),
    })
}

/// Runs a DELETE inside the caller's transaction. The caller rolls back on error.
pub async fn delete(
    conn: &mut SqliteConnection,
    meta: &TableMeta,
    req: &DeleteRequest,
) -> ConnectorResult<MutationOutcome> {
    if !req.filter.is_restrictive() {
        return Err(ConnectorError::invalid_input(
            "DELETE without a restrictive filter is not allowed",
        ));
    }
    let mut parts = SqlParts::default();
    filter_sql(&req.filter, &meta.types(), &mut parts)?;
    let sql = format!(
        "DELETE FROM {} WHERE {}",
        qualified(&req.object)?,
        parts.sql
    );
    let r = bind_all(sqlx::query(&sql), &parts.binds)
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    check_expected(r.rows_affected(), req.expect_matched)?;
    Ok(MutationOutcome {
        matched: r.rows_affected(),
        modified: r.rows_affected(),
    })
}
