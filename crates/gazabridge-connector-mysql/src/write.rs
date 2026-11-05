//! Batched writes, updates and deletes.
//!
//! Callers run these inside a transaction. Each chunk is inserted inside a
//! savepoint; a record-level failure rolls the chunk back and retries it row
//! by row so only the offending records are reported.
//!
//! Upserts use `ON DUPLICATE KEY UPDATE`, which reacts to *any* unique key
//! of the table, not only `key_fields`. Inserted and updated rows are told
//! apart by counting the keys that already exist (locked with
//! `FOR UPDATE`) before the statement runs.

use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, RecordFailure, UpdateRequest, WriteBatch, WriteOutcome,
};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Row, Value as My};

use crate::error::map_mysql;
use crate::meta::{TableMeta, cell_u64};
use crate::sql::{Params as SqlParams, filter_sql, qualified, quote_ident};
use crate::types::encode;

/// The protocol limit on placeholders per prepared statement.
pub const MAX_BIND_PARAMETERS: usize = 65_535;
const MAX_ROWS_PER_STATEMENT: usize = 1_000;

pub const DIRECT_MODES: [WriteMode; 5] = [
    WriteMode::Insert,
    WriteMode::Append,
    WriteMode::Upsert,
    WriteMode::SkipExisting,
    WriteMode::FailOnConflict,
];

fn signature(r: &Record) -> Vec<String> {
    r.fields
        .iter()
        .filter(|(_, v)| !matches!(v.value, Value::Missing))
        .map(|(k, _)| k.clone())
        .collect()
}

struct Statement {
    table: String,
    head: String,
    tail: String,
    columns: Vec<String>,
    key_positions: Vec<usize>,
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
    let mut key_positions = Vec::new();
    if batch.mode.requires_key() {
        for k in &batch.key_fields {
            let pos = columns.iter().position(|c| c == k).ok_or_else(|| {
                ConnectorError::invalid_input(format!("records lack the key field `{k}`"))
            })?;
            key_positions.push(pos);
        }
    }
    let tail = match batch.mode {
        WriteMode::Upsert => {
            let updates: Vec<String> = columns
                .iter()
                .filter(|c| !batch.key_fields.contains(c))
                .map(|c| quote_ident(c).map(|q| format!("{q} = VALUES({q})")))
                .collect::<ConnectorResult<_>>()?;
            if updates.is_empty() {
                let k = quote_ident(&batch.key_fields[0])?;
                format!(" ON DUPLICATE KEY UPDATE {k} = {k}")
            } else {
                format!(" ON DUPLICATE KEY UPDATE {}", updates.join(", "))
            }
        }
        WriteMode::SkipExisting => {
            // A no-op update reports 0 affected rows, so affected rows equal
            // inserted rows.
            let k = quote_ident(&batch.key_fields[0])?;
            format!(" ON DUPLICATE KEY UPDATE {k} = {k}")
        }
        _ => String::new(),
    };
    Ok(Statement {
        table: table.to_owned(),
        head: format!("INSERT INTO {table} ({}) VALUES ", quoted.join(", ")),
        tail,
        columns,
        key_positions,
    })
}

#[derive(Default)]
struct Counts {
    inserted: u64,
    updated: u64,
}

/// Counts rows of the table whose key matches one of `rows`, locking them.
async fn existing(conn: &mut Conn, st: &Statement, rows: &[Vec<My>]) -> ConnectorResult<u64> {
    let cols = st
        .key_positions
        .iter()
        .map(|&i| quote_ident(&st.columns[i]))
        .collect::<ConnectorResult<Vec<_>>>()?;
    let one = format!("({})", vec!["?"; cols.len()].join(", "));
    let tuples = vec![one; rows.len()].join(", ");
    let sql = format!(
        "SELECT COUNT(*) FROM {} WHERE ({}) IN ({tuples}) FOR UPDATE",
        st.table,
        cols.join(", ")
    );
    let params: Vec<My> = rows
        .iter()
        .flat_map(|r| st.key_positions.iter().map(|&i| r[i].clone()))
        .collect();
    let row: Option<Row> = conn
        .exec_first(sql, Params::Positional(params))
        .await
        .map_err(map_mysql)?;
    Ok(row.and_then(|r| cell_u64(&r, 0)).unwrap_or(0))
}

async fn insert_rows(
    conn: &mut Conn,
    st: &Statement,
    mode: WriteMode,
    rows: &[Vec<My>],
) -> ConnectorResult<Counts> {
    let n = st.columns.len();
    let group = format!("({})", vec!["?"; n].join(", "));
    let sql = format!(
        "{}{}{}",
        st.head,
        vec![group; rows.len()].join(", "),
        st.tail
    );
    let before = if mode == WriteMode::Upsert {
        existing(conn, st, rows).await?
    } else {
        0
    };
    let params: Vec<My> = rows.iter().flatten().cloned().collect();
    conn.exec_drop(sql, Params::Positional(params))
        .await
        .map_err(map_mysql)?;
    let affected = conn.affected_rows();
    Ok(match mode {
        WriteMode::Upsert => {
            let total = rows.len() as u64;
            let updated = before.min(total);
            Counts {
                inserted: total - updated,
                updated,
            }
        }
        _ => Counts {
            inserted: affected,
            updated: 0,
        },
    })
}

fn failure(index: usize, e: &ConnectorError) -> RecordFailure {
    RecordFailure {
        index,
        kind: e.kind(),
        message: e.to_string(),
        code: e.code().map(str::to_owned),
    }
}

async fn exec(conn: &mut Conn, sql: &str) -> ConnectorResult<()> {
    conn.query_drop(sql).await.map_err(map_mysql)
}

/// Writes a batch; `conn` must already be inside a transaction.
pub async fn write_records(
    conn: &mut Conn,
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
        if let Some(k) = batch
            .key_fields
            .iter()
            .find(|k| meta.column_type(k).is_none())
        {
            return Err(ConnectorError::invalid_input(format!(
                "unknown key column `{k}`"
            )));
        }
    }
    let table = qualified(&batch.object)?;
    let mut outcome = WriteOutcome::default();
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
        let per_stmt = (MAX_BIND_PARAMETERS / st.columns.len()).clamp(1, MAX_ROWS_PER_STATEMENT);
        let mut from = start;
        while from < end {
            let to = (from + per_stmt).min(end);
            let mut rows = Vec::with_capacity(to - from);
            let mut indexes = Vec::with_capacity(to - from);
            for i in from..to {
                let r = &batch.records[i];
                match st
                    .columns
                    .iter()
                    .map(|c| encode(r.get(c), meta.column_type(c)))
                    .collect::<ConnectorResult<Vec<_>>>()
                {
                    Ok(p) => {
                        rows.push(p);
                        indexes.push(i);
                    }
                    Err(e) if batch.atomic => return Err(e),
                    Err(e) => outcome.failures.push(failure(i, &e)),
                }
            }
            if !rows.is_empty() {
                write_chunk(conn, &st, batch, &rows, &indexes, &mut outcome).await?;
            }
            from = to;
        }
        start = end;
    }
    if batch.mode == WriteMode::SkipExisting {
        let attempted = (batch.records.len() - outcome.failures.len()) as u64;
        outcome.skipped = attempted.saturating_sub(outcome.inserted);
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
    conn: &mut Conn,
    st: &Statement,
    batch: &WriteBatch,
    rows: &[Vec<My>],
    indexes: &[usize],
    outcome: &mut WriteOutcome,
) -> ConnectorResult<()> {
    exec(conn, "SAVEPOINT gb_chunk").await?;
    match insert_rows(conn, st, batch.mode, rows).await {
        Ok(c) => {
            exec(conn, "RELEASE SAVEPOINT gb_chunk").await?;
            outcome.inserted += c.inserted;
            outcome.updated += c.updated;
            Ok(())
        }
        Err(e) => {
            exec(conn, "ROLLBACK TO SAVEPOINT gb_chunk").await?;
            exec(conn, "RELEASE SAVEPOINT gb_chunk").await?;
            if batch.atomic || !e.kind().is_record_level() {
                return Err(e);
            }
            for (row, &i) in rows.iter().zip(indexes) {
                exec(conn, "SAVEPOINT gb_row").await?;
                match insert_rows(conn, st, batch.mode, std::slice::from_ref(row)).await {
                    Ok(c) => {
                        exec(conn, "RELEASE SAVEPOINT gb_row").await?;
                        outcome.inserted += c.inserted;
                        outcome.updated += c.updated;
                    }
                    Err(e) => {
                        exec(conn, "ROLLBACK TO SAVEPOINT gb_row").await?;
                        exec(conn, "RELEASE SAVEPOINT gb_row").await?;
                        if !e.kind().is_record_level() {
                            return Err(e);
                        }
                        outcome.failures.push(failure(i, &e));
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

async fn matching(
    conn: &mut Conn,
    table: &str,
    cond: &str,
    params: Vec<My>,
) -> ConnectorResult<u64> {
    let row: Option<Row> = conn
        .exec_first(
            format!("SELECT COUNT(*) FROM {table} WHERE {cond} FOR UPDATE"),
            Params::Positional(params),
        )
        .await
        .map_err(map_mysql)?;
    Ok(row.and_then(|r| cell_u64(&r, 0)).unwrap_or(0))
}

/// UPDATE inside the caller's transaction; the caller rolls back on error.
/// MySQL reports changed rows, so matched rows are counted (and locked) first.
pub async fn update(
    conn: &mut Conn,
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
    let types = meta.types();
    let table = qualified(&req.object)?;
    let mut where_params = SqlParams::default();
    let cond = filter_sql(&req.filter, &types, &mut where_params)?;
    let matched = matching(conn, &table, &cond, where_params.values.clone()).await?;
    check_expected(matched, req.expect_matched)?;
    let mut params = SqlParams::default();
    let mut sets = Vec::with_capacity(req.set.len());
    for (name, v) in &req.set.fields {
        let t = types
            .get(name)
            .ok_or_else(|| ConnectorError::invalid_input(format!("unknown column `{name}`")))?;
        let p = params.push(encode(&v.value, Some(t))?);
        sets.push(format!("{} = {p}", quote_ident(name)?));
    }
    params.values.extend(where_params.values);
    let sql = format!("UPDATE {table} SET {} WHERE {cond}", sets.join(", "));
    conn.exec_drop(sql, Params::Positional(params.values))
        .await
        .map_err(map_mysql)?;
    Ok(MutationOutcome {
        matched,
        modified: conn.affected_rows(),
    })
}

/// DELETE inside the caller's transaction; the caller rolls back on error.
pub async fn delete(
    conn: &mut Conn,
    meta: &TableMeta,
    req: &DeleteRequest,
) -> ConnectorResult<MutationOutcome> {
    if !req.filter.is_restrictive() {
        return Err(ConnectorError::invalid_input(
            "DELETE without a restrictive filter is not allowed",
        ));
    }
    let mut params = SqlParams::default();
    let cond = filter_sql(&req.filter, &meta.types(), &mut params)?;
    let sql = format!("DELETE FROM {} WHERE {cond}", qualified(&req.object)?);
    conn.exec_drop(sql, Params::Positional(params.values))
        .await
        .map_err(map_mysql)?;
    let n = conn.affected_rows();
    check_expected(n, req.expect_matched)?;
    Ok(MutationOutcome {
        matched: n,
        modified: n,
    })
}
