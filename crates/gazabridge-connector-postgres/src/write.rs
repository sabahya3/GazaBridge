//! Batched writes, updates and deletes.
//!
//! Callers run these inside a transaction. Each chunk is inserted inside a
//! savepoint; a record-level failure rolls the chunk back and retries it row
//! by row so only the offending records are reported.

use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, RecordFailure, UpdateRequest, WriteBatch, WriteOutcome,
};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use tokio_postgres::Client;

use crate::error::map_pg;
use crate::meta::TableMeta;
use crate::sql::{SqlParts, filter_sql, qualified, quote_ident, shape};
use crate::types::{TextParam, as_params, encode};

/// PostgreSQL's protocol limit on bind parameters per statement.
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
                .map(|c| quote_ident(c).map(|q| format!("{q} = EXCLUDED.{q}")))
                .collect::<ConnectorResult<_>>()?;
            if updates.is_empty() {
                format!(
                    " ON CONFLICT ({}) DO NOTHING RETURNING TRUE",
                    keys.join(", ")
                )
            } else {
                // xmax = 0 identifies freshly inserted rows.
                format!(
                    " ON CONFLICT ({}) DO UPDATE SET {} RETURNING (xmax = 0)",
                    keys.join(", "),
                    updates.join(", ")
                )
            }
        }
        WriteMode::SkipExisting => format!(
            " ON CONFLICT ({}) DO NOTHING RETURNING TRUE",
            keys.join(", ")
        ),
        _ => String::new(),
    };
    Ok(Statement {
        head: format!("INSERT INTO {table} ({}) VALUES ", quoted.join(", ")),
        tail,
        columns,
    })
}

#[derive(Default)]
struct Counts {
    inserted: u64,
    updated: u64,
}

async fn insert_rows(
    client: &Client,
    st: &Statement,
    mode: WriteMode,
    rows: &[Vec<TextParam>],
) -> ConnectorResult<Counts> {
    let n = st.columns.len();
    let groups: Vec<String> = (0..rows.len())
        .map(|r| {
            let marks: Vec<String> = (1..=n).map(|c| format!("${}", r * n + c)).collect();
            format!("({})", marks.join(", "))
        })
        .collect();
    let sql = format!("{}{}{}", st.head, groups.join(", "), st.tail);
    let params: Vec<TextParam> = rows.iter().flatten().cloned().collect();
    match mode {
        WriteMode::Upsert | WriteMode::SkipExisting => {
            let returned = client
                .query(&sql, &as_params(&params))
                .await
                .map_err(map_pg)?;
            let mut c = Counts::default();
            for r in returned {
                if r.get::<_, bool>(0) {
                    c.inserted += 1;
                } else {
                    c.updated += 1;
                }
            }
            Ok(c)
        }
        _ => {
            let inserted = client
                .execute(&sql, &as_params(&params))
                .await
                .map_err(map_pg)?;
            Ok(Counts {
                inserted,
                updated: 0,
            })
        }
    }
}

fn failure(index: usize, e: &ConnectorError) -> RecordFailure {
    RecordFailure {
        index,
        kind: e.kind(),
        message: e.to_string(),
        code: e.code().map(str::to_owned),
    }
}

async fn exec(client: &Client, sql: &str) -> ConnectorResult<()> {
    client.batch_execute(sql).await.map_err(map_pg)
}

/// Writes a batch; `client` must already be inside a transaction.
pub async fn write_records(
    client: &Client,
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
    let types = meta.types();
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
            // Encode first: conversion failures belong to single records.
            let mut rows = Vec::with_capacity(to - from);
            let mut indexes = Vec::with_capacity(to - from);
            for i in from..to {
                let r = &batch.records[i];
                match st
                    .columns
                    .iter()
                    .map(|c| encode(r.get(c), shape(&types, c)))
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
                write_chunk(client, &st, batch, &rows, &indexes, &mut outcome).await?;
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
    client: &Client,
    st: &Statement,
    batch: &WriteBatch,
    rows: &[Vec<TextParam>],
    indexes: &[usize],
    outcome: &mut WriteOutcome,
) -> ConnectorResult<()> {
    exec(client, "SAVEPOINT gb_chunk").await?;
    match insert_rows(client, st, batch.mode, rows).await {
        Ok(c) => {
            exec(client, "RELEASE SAVEPOINT gb_chunk").await?;
            outcome.inserted += c.inserted;
            outcome.updated += c.updated;
            Ok(())
        }
        Err(e) => {
            exec(client, "ROLLBACK TO SAVEPOINT gb_chunk").await?;
            exec(client, "RELEASE SAVEPOINT gb_chunk").await?;
            if batch.atomic || !e.kind().is_record_level() {
                return Err(e);
            }
            for (row, &i) in rows.iter().zip(indexes) {
                exec(client, "SAVEPOINT gb_row").await?;
                match insert_rows(client, st, batch.mode, std::slice::from_ref(row)).await {
                    Ok(c) => {
                        exec(client, "RELEASE SAVEPOINT gb_row").await?;
                        outcome.inserted += c.inserted;
                        outcome.updated += c.updated;
                    }
                    Err(e) => {
                        exec(client, "ROLLBACK TO SAVEPOINT gb_row").await?;
                        exec(client, "RELEASE SAVEPOINT gb_row").await?;
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

/// UPDATE inside the caller's transaction; the caller rolls back on error.
pub async fn update(
    client: &Client,
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
    let mut parts = SqlParts::default();
    let mut sets = Vec::with_capacity(req.set.len());
    for (name, v) in &req.set.fields {
        if !types.contains_key(name) {
            return Err(ConnectorError::invalid_input(format!(
                "unknown column `{name}`"
            )));
        }
        let p = parts.param(encode(&v.value, shape(&types, name))?);
        sets.push(format!("{} = {p}", quote_ident(name)?));
    }
    let cond = filter_sql(&req.filter, &types, &mut parts)?;
    let sql = format!(
        "UPDATE {} SET {} WHERE {cond}",
        qualified(&req.object)?,
        sets.join(", ")
    );
    let n = client
        .execute(&sql, &as_params(&parts.params))
        .await
        .map_err(map_pg)?;
    check_expected(n, req.expect_matched)?;
    Ok(MutationOutcome {
        matched: n,
        modified: n,
    })
}

/// DELETE inside the caller's transaction; the caller rolls back on error.
pub async fn delete(
    client: &Client,
    meta: &TableMeta,
    req: &DeleteRequest,
) -> ConnectorResult<MutationOutcome> {
    if !req.filter.is_restrictive() {
        return Err(ConnectorError::invalid_input(
            "DELETE without a restrictive filter is not allowed",
        ));
    }
    let mut parts = SqlParts::default();
    let cond = filter_sql(&req.filter, &meta.types(), &mut parts)?;
    let sql = format!("DELETE FROM {} WHERE {cond}", qualified(&req.object)?);
    let n = client
        .execute(&sql, &as_params(&parts.params))
        .await
        .map_err(map_pg)?;
    check_expected(n, req.expect_matched)?;
    Ok(MutationOutcome {
        matched: n,
        modified: n,
    })
}
