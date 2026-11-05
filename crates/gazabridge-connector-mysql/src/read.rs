//! Streaming reads with keyset pagination and resumable positions.
//!
//! Tables with a primary key (or an explicit uniform ordering) are read in
//! bounded batches; each batch reports the last key as a [`ReadPosition`].
//! Objects without a key fall back to OFFSET batches and cannot resume.

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::read::{
    Page, PageRequest, ReadPosition, ReadRequest, RecordBatch, RecordStream,
};
use gazabridge_plugin_sdk::schema::ObjectRef;
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row};
use tokio_util::sync::CancellationToken;

use crate::CONNECTOR_ID;
use crate::connection::{Flavor, Inner};
use crate::error::map_mysql;
use crate::meta::{TableMeta, cell_u64};
use crate::sql::{Params as SqlParams, filter_sql, order_sql, qualified, quote_ident};
use crate::types::{decode, encode, raw_value};

#[derive(Debug, Clone)]
struct Selected {
    name: String,
    canonical: CanonicalType,
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
    inner: Arc<Inner>,
    from: String,
    selected: Vec<Selected>,
    filter: Option<Filter>,
    meta: Arc<TableMeta>,
    mode: Mode,
    batch: usize,
    remaining: Option<u64>,
    cancel: CancellationToken,
    done: bool,
}

fn selected(meta: &TableMeta, name: &str, hidden: bool) -> ConnectorResult<Selected> {
    let t = meta
        .column_type(name)
        .ok_or_else(|| ConnectorError::invalid_input(format!("unknown column `{name}`")))?;
    Ok(Selected {
        name: name.to_owned(),
        canonical: t.clone(),
        hidden,
    })
}

fn projection(meta: &TableMeta, wanted: &[String]) -> ConnectorResult<Vec<Selected>> {
    if wanted.is_empty() {
        meta.columns
            .iter()
            .map(|c| selected(meta, &c.name, false))
            .collect()
    } else {
        wanted.iter().map(|n| selected(meta, n, false)).collect()
    }
}

/// Converts a raw value to the column type, parsing JSON text and bytes.
pub fn decode_typed(raw: Value, t: &CanonicalType) -> Value {
    match (t, raw) {
        (CanonicalType::Json, Value::Binary(b)) => match serde_json::from_slice(&b) {
            Ok(j) => Value::Json(j),
            Err(_) => Value::Binary(b),
        },
        (CanonicalType::Json, Value::String(s)) => match serde_json::from_str(&s) {
            Ok(j) => Value::Json(j),
            Err(_) => Value::String(s),
        },
        (t, raw) => decode(raw, t),
    }
}

fn decode_value(v: mysql_async::Value, t: &CanonicalType) -> Value {
    let binary = matches!(t, CanonicalType::Binary { .. } | CanonicalType::Geometry);
    decode_typed(raw_value(v, binary, *t == CanonicalType::Date), t)
}

fn decode_row(row: Row, selected: &[Selected]) -> (Record, Vec<Value>) {
    let mut record = Record::with_capacity(selected.len());
    let mut hidden = Vec::new();
    for (v, s) in row.unwrap().into_iter().zip(selected) {
        let v = decode_value(v, &s.canonical);
        if s.hidden {
            hidden.push(v);
        } else {
            record.insert(s.name.clone(), v);
        }
    }
    (record, hidden)
}

fn select_list(sel: &[Selected]) -> ConnectorResult<String> {
    Ok(sel
        .iter()
        .map(|s| quote_ident(&s.name))
        .collect::<ConnectorResult<Vec<_>>>()?
        .join(", "))
}

fn position(keys: &[Value]) -> ConnectorResult<ReadPosition> {
    Ok(ReadPosition {
        connector_id: CONNECTOR_ID.into(),
        token: serde_json::json!({
            "after": serde_json::to_value(keys).map_err(|e| ConnectorError::internal(e.to_string()))?
        }),
    })
}

fn parse_position(p: &ReadPosition, n: usize) -> ConnectorResult<Vec<Value>> {
    if p.connector_id != CONNECTOR_ID {
        return Err(ConnectorError::invalid_input(
            "position belongs to a different connector",
        ));
    }
    let values: Vec<Value> = serde_json::from_value(
        p.token
            .get("after")
            .cloned()
            .ok_or_else(|| ConnectorError::invalid_input("position has no `after` key"))?,
    )
    .map_err(|e| ConnectorError::invalid_input(format!("invalid position: {e}")))?;
    if values.len() != n {
        return Err(ConnectorError::invalid_input(
            "position does not match the read's ordering keys",
        ));
    }
    Ok(values)
}

pub fn open(
    inner: Arc<Inner>,
    meta: Arc<TableMeta>,
    req: &ReadRequest,
    cancel: CancellationToken,
) -> ConnectorResult<RecordStream> {
    if req.batch_size == 0 {
        return Err(ConnectorError::invalid_input(
            "batch size must be at least 1",
        ));
    }
    let types = meta.types();
    let mut sel = projection(&meta, &req.projection)?;
    if let Some(f) = &req.filter {
        filter_sql(f, &types, &mut SqlParams::default())?;
    }
    let uniform = req
        .order_by
        .windows(2)
        .all(|w| w[0].direction == w[1].direction);
    let keys: Option<(Vec<String>, SortDirection)> = if !req.order_by.is_empty() {
        uniform.then(|| {
            (
                req.order_by.iter().map(|k| k.field.0.clone()).collect(),
                req.order_by[0].direction,
            )
        })
    } else if !meta.primary_key.is_empty() {
        Some((meta.primary_key.clone(), SortDirection::Asc))
    } else {
        None
    };
    let mode = match keys {
        Some((names, direction)) => {
            let keys = names
                .iter()
                .map(|n| selected(&meta, n, true))
                .collect::<ConnectorResult<Vec<_>>>()?;
            sel.extend(keys.iter().cloned());
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
        inner,
        from: qualified(&req.object)?,
        selected: sel,
        filter: req.filter.clone(),
        meta,
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
    match fetch(&st, limit).await {
        Err(e) => {
            st.done = true;
            Some((Err(e), st))
        }
        Ok((records, last)) => {
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
            let pos = match (&mut st.mode, last) {
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
    let mut params = SqlParams::default();
    let mut conditions = Vec::new();
    if let Some(f) = &st.filter {
        conditions.push(filter_sql(f, &st.meta.types(), &mut params)?);
    }
    let tail = match &st.mode {
        Mode::Keyset {
            keys,
            direction,
            after,
        } => {
            let dir = if *direction == SortDirection::Asc {
                "ASC"
            } else {
                "DESC"
            };
            let cols = keys
                .iter()
                .map(|k| quote_ident(&k.name))
                .collect::<ConnectorResult<Vec<_>>>()?;
            if let Some(after) = after {
                let op = if *direction == SortDirection::Asc {
                    ">"
                } else {
                    "<"
                };
                // Expanded form of (a, b) > (x, y): uses indexes on every
                // server version.
                let mut ors = Vec::new();
                for i in 0..cols.len() {
                    let mut and = Vec::new();
                    for j in 0..i {
                        let p = params.push(encode(&after[j], Some(&keys[j].canonical))?);
                        and.push(format!("{} = {p}", cols[j]));
                    }
                    let p = params.push(encode(&after[i], Some(&keys[i].canonical))?);
                    and.push(format!("{} {op} {p}", cols[i]));
                    ors.push(format!("({})", and.join(" AND ")));
                }
                conditions.push(format!("({})", ors.join(" OR ")));
            }
            let order = cols
                .iter()
                .map(|c| format!("{c} {dir}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {order} LIMIT {limit}")
        }
        Mode::Offset { order, offset } => format!("{order} LIMIT {limit} OFFSET {offset}"),
    };
    let mut sql = format!("SELECT {} FROM {}", select_list(&st.selected)?, st.from);
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(&tail);

    let rows = st
        .inner
        .rows_cancellable(&sql, params.values, &st.cancel)
        .await?;
    let keyset = matches!(st.mode, Mode::Keyset { .. });
    let mut records = Vec::with_capacity(rows.len());
    let mut last = None;
    for row in rows {
        let (record, hidden) = decode_row(row, &st.selected);
        records.push(record);
        last = Some(hidden);
    }
    Ok((records, if keyset { last } else { None }))
}

pub async fn page(
    inner: &Inner,
    meta: &TableMeta,
    req: &PageRequest,
    cancel: &CancellationToken,
) -> ConnectorResult<Page> {
    let types = meta.types();
    let sel = projection(meta, &req.projection)?;
    let limit = req.effective_limit();
    let mut params = SqlParams::default();
    let mut sql = format!(
        "SELECT {} FROM {}",
        select_list(&sel)?,
        qualified(&req.object)?
    );
    if let Some(f) = &req.filter {
        sql.push_str(" WHERE ");
        sql.push_str(&filter_sql(f, &types, &mut params)?);
    }
    let order: Vec<SortKey> = if req.order_by.is_empty() {
        meta.primary_key
            .iter()
            .map(|k| SortKey {
                field: FieldPath::new(k.clone()),
                direction: SortDirection::Asc,
            })
            .collect()
    } else {
        req.order_by.clone()
    };
    sql.push_str(&order_sql(&order, &types)?);
    sql.push_str(&format!(
        " LIMIT {} OFFSET {}",
        u64::from(limit) + 1,
        req.offset
    ));
    let rows = inner.rows_cancellable(&sql, params.values, cancel).await?;
    let has_more = rows.len() > limit as usize;
    let records = rows
        .into_iter()
        .take(limit as usize)
        .map(|r| decode_row(r, &sel).0)
        .collect();
    Ok(Page {
        columns: sel
            .iter()
            .filter_map(|s| meta.columns.iter().find(|c| c.name == s.name).cloned())
            .collect(),
        records,
        offset: req.offset,
        has_more,
    })
}

/// Exact count; with `max_time` the server stops it (MySQL's
/// `MAX_EXECUTION_TIME` hint, MariaDB's `max_statement_time`) and a client
/// timer kills it as a last resort.
pub async fn count(
    inner: &Inner,
    meta: &TableMeta,
    object: &ObjectRef,
    filter: Option<&Filter>,
    max_time: Option<Duration>,
) -> ConnectorResult<u64> {
    let mut params = SqlParams::default();
    let mut tail = format!("FROM {}", qualified(object)?);
    if let Some(f) = filter {
        tail.push_str(" WHERE ");
        tail.push_str(&filter_sql(f, &meta.types(), &mut params)?);
    }
    let sql = match (max_time, inner.flavor) {
        (None, _) => format!("SELECT COUNT(*) {tail}"),
        (Some(t), Flavor::MySql) => format!(
            "SELECT /*+ MAX_EXECUTION_TIME({}) */ COUNT(*) {tail}",
            t.as_millis().clamp(1, u32::MAX as u128)
        ),
        (Some(t), Flavor::MariaDb) => format!(
            "SET STATEMENT max_statement_time = {:.3} FOR SELECT COUNT(*) {tail}",
            t.as_secs_f64().max(0.001)
        ),
    };
    let mut c = inner.conn().await?;
    let id = c.id();
    let cancel = CancellationToken::new();
    let client_limit = max_time.map(|t| t + Duration::from_secs(2));
    let row: Option<Row> = inner
        .guarded(id, &cancel, client_limit, async {
            c.exec_first(sql.as_str(), Params::Positional(params.values))
                .await
                .map_err(|e| {
                    let e = map_mysql(e);
                    // 3024 (MySQL) / 1969 (MariaDB) are server-side timeouts.
                    if e.kind() == ErrorKind::Timeout || e.kind() == ErrorKind::Cancelled {
                        ConnectorError::new(ErrorKind::Timeout, "count took too long")
                    } else {
                        e
                    }
                })
        })
        .await?;
    Ok(row.and_then(|r| cell_u64(&r, 0)).unwrap_or(0))
}
