//! Streaming reads with keyset pagination and resumable positions.
//!
//! Tables with a primary key (or an explicit uniform ordering) are read in
//! bounded batches; each batch reports the last key as a [`ReadPosition`].
//! Objects without a key fall back to OFFSET batches and cannot resume.
//! Batches are separate statements, so a long read observes changes
//! committed between batches; consistent snapshots come with CDC (M8).

use std::sync::Arc;

use futures::stream;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::filter::{Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::read::{
    Page, PageRequest, ReadPosition, ReadRequest, RecordBatch, RecordStream,
};
use gazabridge_plugin_sdk::schema::ObjectRef;
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use tokio_postgres::Row;
use tokio_util::sync::CancellationToken;

use crate::CONNECTOR_ID;
use crate::connection::Inner;
use crate::error::map_pg;
use crate::meta::TableMeta;
use crate::sql::{SqlParts, filter_sql, order_sql, qualified, quote_ident};
use crate::types::{
    Raw, as_params, decode_binary, encode, extension_from_text, needs_text_cast, shape_of,
};

#[derive(Debug, Clone)]
struct Selected {
    name: String,
    expr: String,
    canonical: CanonicalType,
    native: String,
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
    let col = meta
        .columns
        .iter()
        .find(|c| c.name == name)
        .ok_or_else(|| ConnectorError::invalid_input(format!("unknown column `{name}`")))?;
    Ok(Selected {
        name: name.to_owned(),
        expr: meta.select_expr(name)?,
        canonical: col.canonical_type.clone(),
        native: col.native_type.clone(),
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

/// Decodes column `i` of a result row.
pub fn decode_column(row: &Row, i: usize, text_cast: Option<&str>) -> ConnectorResult<Value> {
    let raw: Raw<'_> = row
        .try_get(i)
        .map_err(|e| ConnectorError::from_driver(ErrorKind::Protocol, e.to_string(), None))?;
    Ok(match (raw.0, text_cast) {
        (None, _) => Value::Null,
        (Some(bytes), Some(type_name)) => match std::str::from_utf8(bytes) {
            Ok(text) => extension_from_text(type_name, text),
            Err(_) => decode_binary(row.columns()[i].type_(), bytes),
        },
        (Some(bytes), None) => decode_binary(row.columns()[i].type_(), bytes),
    })
}

fn decode_row(row: &Row, selected: &[Selected]) -> ConnectorResult<(Record, Vec<Value>)> {
    let mut record = Record::with_capacity(selected.len());
    let mut hidden = Vec::new();
    for (i, s) in selected.iter().enumerate() {
        let cast = needs_text_cast(&s.canonical).then_some(s.native.as_str());
        let v = decode_column(row, i, cast)?;
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
        // Validate early so errors surface when opening the stream.
        filter_sql(f, &types, &mut SqlParts::default())?;
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
            if keys.iter().any(|k| needs_text_cast(&k.canonical)) {
                return Err(ConnectorError::invalid_input(
                    "ordering by columns of extension types is not supported for streaming reads",
                ));
            }
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
    let mut parts = SqlParts::default();
    let mut conditions = Vec::new();
    if let Some(f) = &st.filter {
        conditions.push(filter_sql(f, &st.meta.types(), &mut parts)?);
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
                let marks = keys
                    .iter()
                    .zip(after)
                    .map(|(k, v)| Ok(parts.param(encode(v, shape_of(&k.canonical))?)))
                    .collect::<ConnectorResult<Vec<_>>>()?;
                let op = if *direction == SortDirection::Asc {
                    ">"
                } else {
                    "<"
                };
                conditions.push(format!("({}) {op} ({})", cols.join(", "), marks.join(", ")));
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
    let select = st
        .selected
        .iter()
        .map(|s| s.expr.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {select} FROM {}", st.from);
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(&tail);

    let rows = st
        .inner
        .query_cancellable(&sql, &parts.params, &st.cancel)
        .await?;
    let mut records = Vec::with_capacity(rows.len());
    let mut last = None;
    for row in &rows {
        let (record, hidden) = decode_row(row, &st.selected)?;
        records.push(record);
        last = Some(hidden);
    }
    Ok((
        records,
        if matches!(st.mode, Mode::Keyset { .. }) {
            last
        } else {
            None
        },
    ))
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
    let mut parts = SqlParts::default();
    let select = sel
        .iter()
        .map(|s| s.expr.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {select} FROM {}", qualified(&req.object)?);
    if let Some(f) = &req.filter {
        sql.push_str(" WHERE ");
        sql.push_str(&filter_sql(f, &types, &mut parts)?);
    }
    let order: Vec<SortKey> = if req.order_by.is_empty() {
        meta.primary_key
            .iter()
            .map(|k| SortKey {
                field: gazabridge_plugin_sdk::filter::FieldPath::new(k.clone()),
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
    let rows = inner.query_cancellable(&sql, &parts.params, cancel).await?;
    let has_more = rows.len() > limit as usize;
    let mut records = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows.iter().take(limit as usize) {
        records.push(decode_row(row, &sel)?.0);
    }
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

pub async fn count(
    inner: &Inner,
    meta: &TableMeta,
    object: &ObjectRef,
    filter: Option<&Filter>,
) -> ConnectorResult<u64> {
    let mut parts = SqlParts::default();
    let mut sql = format!("SELECT count(*) FROM {}", qualified(object)?);
    if let Some(f) = filter {
        sql.push_str(" WHERE ");
        sql.push_str(&filter_sql(f, &meta.types(), &mut parts)?);
    }
    let client = inner.client().await?;
    let row = client
        .query_one(&sql, &as_params(&parts.params))
        .await
        .map_err(map_pg)?;
    let n: i64 = row.get(0);
    Ok(n.max(0) as u64)
}
