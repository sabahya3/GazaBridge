//! Streaming reads with keyset pagination and resumable positions.
//!
//! Tables are read in bounded batches ordered by a unique key (the requested
//! order when it is uniform, otherwise the primary key or rowid). Each batch
//! carries a [`ReadPosition`] holding the last key, so a read can resume
//! after a crash without re-reading or skipping records. Views without a key
//! fall back to OFFSET batches and are not resumable.

use futures::stream;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::filter::{SortDirection, SortKey};
use gazabridge_plugin_sdk::read::{
    Page, PageRequest, ReadPosition, ReadRequest, RecordBatch, RecordStream,
};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteRow;
use tokio_util::sync::CancellationToken;

use crate::CONNECTOR_ID;
use crate::error::map_sqlx;
use crate::meta::TableMeta;
use crate::sql::{SqlParts, filter_sql, order_sql, qualified, quote_ident};
use crate::types::{bind_all, decode, encode, raw_value};

const ROWID_ALIAS: &str = "__gazabridge_rowid";

/// A column selected by a read: output name, SQL expression and type.
#[derive(Debug, Clone)]
struct Selected {
    name: String,
    expr: String,
    canonical: CanonicalType,
    /// Selected only to compute positions; not part of the output record.
    hidden: bool,
}

enum Mode {
    Keyset {
        keys: Vec<Selected>,
        direction: SortDirection,
        after: Option<Vec<Value>>,
    },
    Offset {
        order: String,
        offset: u64,
    },
}

struct State {
    pool: SqlitePool,
    from: String,
    selected: Vec<Selected>,
    where_parts: SqlParts,
    mode: Mode,
    batch: usize,
    remaining: Option<u64>,
    cancel: CancellationToken,
    done: bool,
}

fn projection(meta: &TableMeta, wanted: &[String]) -> ConnectorResult<Vec<Selected>> {
    let names: Vec<&str> = if wanted.is_empty() {
        meta.columns.iter().map(|c| c.name.as_str()).collect()
    } else {
        wanted.iter().map(String::as_str).collect()
    };
    names
        .into_iter()
        .map(|n| {
            let canonical = meta
                .column_type(n)
                .ok_or_else(|| ConnectorError::invalid_input(format!("unknown column `{n}`")))?
                .clone();
            Ok(Selected {
                name: n.to_owned(),
                expr: quote_ident(n)?,
                canonical,
                hidden: false,
            })
        })
        .collect()
}

fn decode_row(row: &SqliteRow, selected: &[Selected]) -> ConnectorResult<(Record, Vec<Value>)> {
    let mut record = Record::with_capacity(selected.len());
    let mut hidden = Vec::new();
    for (i, s) in selected.iter().enumerate() {
        let v = decode(raw_value(row, i)?, &s.canonical);
        if s.hidden {
            hidden.push(v);
        } else {
            record.insert(s.name.clone(), v);
        }
    }
    Ok((record, hidden))
}

fn position(keys: &[Value]) -> ConnectorResult<ReadPosition> {
    Ok(ReadPosition {
        connector_id: CONNECTOR_ID.into(),
        token: serde_json::json!({ "after": serde_json::to_value(keys).map_err(|e| ConnectorError::internal(e.to_string()))? }),
    })
}

fn parse_position(p: &ReadPosition, key_count: usize) -> ConnectorResult<Vec<Value>> {
    if p.connector_id != CONNECTOR_ID {
        return Err(ConnectorError::invalid_input(
            "position belongs to a different connector",
        ));
    }
    let after = p
        .token
        .get("after")
        .cloned()
        .ok_or_else(|| ConnectorError::invalid_input("position has no `after` key"))?;
    let values: Vec<Value> = serde_json::from_value(after)
        .map_err(|e| ConnectorError::invalid_input(format!("invalid position: {e}")))?;
    if values.len() != key_count {
        return Err(ConnectorError::invalid_input(
            "position does not match the read's ordering keys",
        ));
    }
    Ok(values)
}

/// Opens a streaming read.
pub fn open(
    pool: SqlitePool,
    meta: &TableMeta,
    req: &ReadRequest,
    cancel: CancellationToken,
) -> ConnectorResult<RecordStream> {
    if req.batch_size == 0 {
        return Err(ConnectorError::invalid_input(
            "batch size must be at least 1",
        ));
    }
    let types = meta.types();
    let mut selected = projection(meta, &req.projection)?;
    let mut where_parts = SqlParts::default();
    if let Some(f) = &req.filter {
        filter_sql(f, &types, &mut where_parts)?;
    }

    // Decide the ordering keys.
    let uniform = req
        .order_by
        .windows(2)
        .all(|w| w[0].direction == w[1].direction);
    let key_names: Option<(Vec<String>, SortDirection)> = if !req.order_by.is_empty() {
        uniform.then(|| {
            (
                req.order_by.iter().map(|k| k.field.0.clone()).collect(),
                req.order_by[0].direction,
            )
        })
    } else if !meta.primary_key.is_empty() {
        Some((meta.primary_key.clone(), SortDirection::Asc))
    } else if meta.has_rowid {
        Some((vec![ROWID_ALIAS.to_owned()], SortDirection::Asc))
    } else {
        None
    };

    let mode = match key_names {
        Some((names, direction)) => {
            let mut keys = Vec::with_capacity(names.len());
            for n in names {
                let (expr, canonical) = if n == ROWID_ALIAS {
                    ("rowid".to_owned(), CanonicalType::I64)
                } else {
                    let t = types.get(&n).ok_or_else(|| {
                        ConnectorError::invalid_input(format!("unknown column `{n}`"))
                    })?;
                    (quote_ident(&n)?, t.clone())
                };
                let key = Selected {
                    name: n.clone(),
                    expr,
                    canonical,
                    hidden: true,
                };
                selected.push(key.clone());
                keys.push(key);
            }
            let after = req
                .resume_after
                .as_ref()
                .map(|p| parse_position(p, keys.len()))
                .transpose()?;
            Mode::Keyset {
                keys,
                direction,
                after,
            }
        }
        None => {
            if req.resume_after.is_some() {
                return Err(ConnectorError::invalid_input(
                    "this read has no unique ordering key and cannot be resumed",
                ));
            }
            Mode::Offset {
                order: order_sql(&req.order_by, &types)?,
                offset: 0,
            }
        }
    };

    let state = State {
        pool,
        from: qualified(&req.object)?,
        selected,
        where_parts,
        mode,
        batch: req.batch_size,
        remaining: req.limit,
        cancel,
        done: false,
    };
    Ok(Box::pin(stream::unfold(state, next_batch)))
}

async fn next_batch(mut st: State) -> Option<(ConnectorResult<RecordBatch>, State)> {
    if st.done {
        return None;
    }
    if st.cancel.is_cancelled() {
        st.done = true;
        return Some((Err(ConnectorError::Cancelled), st));
    }
    let limit = match st.remaining {
        Some(0) => return None,
        Some(r) => r.min(st.batch as u64),
        None => st.batch as u64,
    };
    let result = tokio::select! {
        r = fetch(&st, limit) => r,
        _ = st.cancel.cancelled() => Err(ConnectorError::Cancelled),
    };
    match result {
        Err(e) => {
            st.done = true;
            Some((Err(e), st))
        }
        Ok((records, last_key)) => {
            if records.is_empty() {
                return None;
            }
            let n = records.len() as u64;
            if n < limit {
                st.done = true;
            }
            if let Some(r) = &mut st.remaining {
                *r -= n;
            }
            let pos = match (&mut st.mode, last_key) {
                (Mode::Keyset { after, .. }, Some(k)) => {
                    let p = position(&k);
                    *after = Some(k);
                    match p {
                        Ok(p) => Some(p),
                        Err(e) => return Some((Err(e), st)),
                    }
                }
                (Mode::Offset { offset, .. }, _) => {
                    *offset += n;
                    None
                }
                _ => None,
            };
            Some((Ok(RecordBatch::new(records, pos)), st))
        }
    }
}

async fn fetch(st: &State, limit: u64) -> ConnectorResult<(Vec<Record>, Option<Vec<Value>>)> {
    let select = st
        .selected
        .iter()
        .map(|s| s.expr.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {select} FROM {}", st.from);
    let mut binds = Vec::new();
    let mut conditions = Vec::new();
    if !st.where_parts.sql.is_empty() {
        conditions.push(st.where_parts.sql.clone());
        binds.extend(st.where_parts.binds.iter().cloned());
    }
    let tail = match &st.mode {
        Mode::Keyset {
            keys,
            direction,
            after,
        } => {
            let cols = keys
                .iter()
                .map(|k| k.expr.clone())
                .collect::<Vec<_>>()
                .join(", ");
            let dir = match direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            if let Some(after) = after {
                let op = if *direction == SortDirection::Asc {
                    ">"
                } else {
                    "<"
                };
                let marks = vec!["?"; keys.len()].join(", ");
                conditions.push(format!("({cols}) {op} ({marks})"));
                for (k, v) in keys.iter().zip(after) {
                    binds.push(encode(v, Some(&k.canonical))?);
                }
            }
            let order = keys
                .iter()
                .map(|k| format!("{} {dir}", k.expr))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {order} LIMIT {limit}")
        }
        Mode::Offset { order, offset } => format!("{order} LIMIT {limit} OFFSET {offset}"),
    };
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(&tail);

    let rows = bind_all(sqlx::query(&sql), &binds)
        .fetch_all(&st.pool)
        .await
        .map_err(map_sqlx)?;
    let mut records = Vec::with_capacity(rows.len());
    let mut last = None;
    for row in &rows {
        if st.cancel.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }
        let (record, hidden) = decode_row(row, &st.selected)?;
        records.push(record);
        last = Some(hidden);
    }
    let last_key = match &st.mode {
        Mode::Keyset { .. } => last,
        Mode::Offset { .. } => None,
    };
    Ok((records, last_key))
}

/// Reads one bounded page for interactive viewing.
pub async fn page(pool: &SqlitePool, meta: &TableMeta, req: &PageRequest) -> ConnectorResult<Page> {
    let types = meta.types();
    let selected = projection(meta, &req.projection)?;
    let limit = req.effective_limit();
    let select = selected
        .iter()
        .map(|s| s.expr.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let mut parts = SqlParts::default();
    let mut sql = format!("SELECT {select} FROM {}", qualified(&req.object)?);
    if let Some(f) = &req.filter {
        filter_sql(f, &types, &mut parts)?;
        sql.push_str(" WHERE ");
        sql.push_str(&parts.sql);
    }
    let order: Vec<SortKey> = if req.order_by.is_empty() && meta.has_rowid {
        Vec::new()
    } else {
        req.order_by.clone()
    };
    sql.push_str(&order_sql(&order, &types)?);
    if req.order_by.is_empty() && meta.has_rowid {
        sql.push_str(" ORDER BY rowid");
    }
    // One extra row tells us whether another page exists.
    sql.push_str(&format!(
        " LIMIT {} OFFSET {}",
        u64::from(limit) + 1,
        req.offset
    ));
    let rows = bind_all(sqlx::query(&sql), &parts.binds)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx)?;
    let has_more = rows.len() > limit as usize;
    let mut records = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows.iter().take(limit as usize) {
        records.push(decode_row(row, &selected)?.0);
    }
    let columns = selected
        .iter()
        .filter_map(|s| meta.columns.iter().find(|c| c.name == s.name).cloned())
        .collect();
    Ok(Page {
        columns,
        records,
        offset: req.offset,
        has_more,
    })
}

/// `SELECT COUNT(*)` with an optional filter.
pub async fn count(
    pool: &SqlitePool,
    meta: &TableMeta,
    object: &gazabridge_plugin_sdk::schema::ObjectRef,
    filter: Option<&gazabridge_plugin_sdk::filter::Filter>,
) -> ConnectorResult<u64> {
    let mut sql = format!("SELECT COUNT(*) FROM {}", qualified(object)?);
    let mut parts = SqlParts::default();
    if let Some(f) = filter {
        filter_sql(f, &meta.types(), &mut parts)?;
        sql.push_str(" WHERE ");
        sql.push_str(&parts.sql);
    }
    let row = bind_all(sqlx::query(&sql), &parts.binds)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx)?;
    let v = raw_value(&row, 0)?;
    match v {
        Value::Int(n) if n >= 0 => Ok(n as u64),
        other => Err(ConnectorError::new(
            ErrorKind::Protocol,
            format!("COUNT(*) returned {}", other.kind()),
        )),
    }
}
