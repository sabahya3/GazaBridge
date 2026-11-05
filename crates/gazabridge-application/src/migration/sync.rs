//! Continuous sync: after the copy, source changes keep flowing into the
//! target until the run is paused or stopped.
//!
//! * The starting point is recorded **before** the copy (a change stream
//!   token, or each object's current maximum of the timestamp field), so a
//!   change made while the copy runs is applied afterwards; applying is
//!   idempotent (upserts and deletes by key), so applying it twice is
//!   harmless.
//! * Change streams (MongoDB replica sets) carry inserts, updates and
//!   deletes; each batch is coalesced per key (the last change wins) and
//!   checkpointed after it is written, so a pause, a restart or a dropped
//!   connection resumes exactly where it stopped.
//! * Polling re-reads records whose timestamp field is at or after the last
//!   value seen (minus a small window for dates, against late commits); it
//!   sees inserts and updates, not deletes.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::StreamExt;
use gazabridge_domain::compatibility::SyncMethod;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{Record, Value};
use gazabridge_mapping::doc_to_table;
use gazabridge_migration::{Identity, TargetSpec, Transform};
use gazabridge_plugin_sdk::Capability;
use gazabridge_plugin_sdk::cdc::{ChangeFeedRequest, ChangeOperation};
use gazabridge_plugin_sdk::connector::{Connection, InspectOptions};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::read::{ReadPosition, ReadRequest};
use gazabridge_plugin_sdk::schema::{ObjectRef, SamplingMethod};
use gazabridge_plugin_sdk::write::{DeleteRequest, WriteBatch};
use gazabridge_storage::jobs::{EventLevel, RunRecord};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

use super::{
    ExistingData, MigrationPlan, MigrationRequest, MigrationService, Saved, StreamEnd, StreamPhase,
    StreamView, SyncKind, checkpoint, set_stream, stream_key,
};
use crate::convert::ItemMapping;
use crate::{AppError, AppResult};

/// Checkpoint name of the change stream position.
pub(super) const CHANGES: &str = "#changes";
/// Changes gathered into one write.
const BATCH: usize = 500;
/// Re-read window for date cursors, against transactions committed late.
const OVERLAP: chrono::TimeDelta = chrono::TimeDelta::seconds(5);
const POLL_CONNECTOR: &str = "gazabridge.poll";

fn poll_key(item: &super::CopyItem) -> String {
    format!("#poll:{}", stream_key(item))
}

fn followed(mapping: &ItemMapping) -> bool {
    !matches!(
        mapping,
        ItemMapping::View { .. } | ItemMapping::Embedded { .. }
    )
}

fn cursor_position(v: Option<&Value>) -> ReadPosition {
    ReadPosition {
        connector_id: POLL_CONNECTOR.into(),
        token: serde_json::json!({ "cursor": v.map(gazabridge_mapping::ext_json) }),
    }
}

fn cursor_of(p: &ReadPosition) -> Option<Value> {
    p.token
        .get("cursor")
        .filter(|c| !c.is_null())
        .map(gazabridge_mapping::from_ext_json)
}

/// Identity of a record for coalescing changes.
fn key_text(v: &Value) -> String {
    serde_json::to_string(&gazabridge_migration::validate::normalize(v)).unwrap_or_default()
}

/// One item's pending changes: the last change per key wins.
#[derive(Default)]
struct Pending {
    order: Vec<String>,
    by_key: BTreeMap<String, Option<Record>>,
    keys: BTreeMap<String, Value>,
}

impl Pending {
    fn push(&mut self, key: Value, after: Option<Record>) {
        let k = key_text(&key);
        if !self.by_key.contains_key(&k) {
            self.order.push(k.clone());
        }
        self.keys.insert(k.clone(), key);
        self.by_key.insert(k, after);
    }

    fn split(self) -> (Vec<Record>, Vec<Value>) {
        let mut ups = Vec::new();
        let mut dels = Vec::new();
        for k in self.order {
            match self.by_key.get(&k).cloned().flatten() {
                Some(doc) => ups.push(doc),
                None => {
                    if let Some(key) = self.keys.get(&k) {
                        dels.push(key.clone());
                    }
                }
            }
        }
        (ups, dels)
    }
}

impl MigrationService {
    /// Checks the sync settings against the connections and objects.
    pub(super) async fn check_sync(
        &self,
        req: &MigrationRequest,
        mappings: &[ItemMapping],
        plan: &mut MigrationPlan,
    ) -> AppResult<()> {
        let Some(sync) = &req.sync else { return Ok(()) };
        let source = self.explorer.session(req.source).await?;
        let family = source.manifest().family;
        if mappings.iter().any(
            |m| matches!(m, ItemMapping::TableToDoc { mapping, .. } if !mapping.embeds.is_empty()),
        ) {
            plan.blockers.push(
                "embedded child tables are not kept in sync; choose one collection per table"
                    .into(),
            );
        }
        match sync.method {
            SyncKind::ChangeStream => {
                if family != EngineFamily::MongoDb {
                    plan.blockers.push("change streams are a MongoDB feature; choose timestamp polling for this source".into());
                } else if !source.manifest().supports(Capability::ChangeDataCapture) {
                    plan.blockers.push(
                        "this MongoDB server is standalone, and change streams need a replica set. Choose timestamp polling (a field like updatedAt), or run the server as a single-node replica set (`--replSet rs0`, then `rs.initiate()`).".into(),
                    );
                }
                let dbs: std::collections::BTreeSet<_> = req
                    .items
                    .iter()
                    .map(|i| i.source.namespace.database.clone())
                    .collect();
                if dbs.len() > 1 {
                    plan.blockers
                        .push("a sync follows collections of one source database".into());
                }
            }
            SyncKind::Polling => {
                let Some(field) = sync
                    .cursor_field
                    .as_deref()
                    .map(str::trim)
                    .filter(|f| !f.is_empty())
                else {
                    plan.blockers.push("choose the field that records when a record changed (for example updatedAt or updated_at)".into());
                    return Ok(());
                };
                plan.warnings.push(format!(
                    "polling every {} s on `{field}` copies new and changed records; deleted records are not removed from the target",
                    sync.interval_secs.max(1)
                ));
                let options = InspectOptions {
                    include_indexes: false,
                    include_constraints: false,
                    sample_size: 200,
                    sampling: SamplingMethod::Random,
                };
                for (item, mapping) in req.items.iter().zip(mappings) {
                    if !followed(mapping) {
                        continue;
                    }
                    let Ok(schema) = source.inspect(&item.source, &options).await else {
                        continue;
                    };
                    let seen = schema.columns.iter().any(|c| c.name == field)
                        || schema
                            .inferred
                            .as_ref()
                            .is_some_and(|i| i.fields.iter().any(|f| f.path == field));
                    if !seen && family != EngineFamily::MongoDb {
                        plan.blockers
                            .push(format!("`{}` has no column `{field}`", item.source));
                    } else if !seen {
                        plan.warnings.push(format!(
                            "`{field}` was not found in a sample of `{}`; documents without it are not followed",
                            item.source
                        ));
                    }
                }
            }
        }
        plan.warnings.push(
            "after the copy, the run keeps applying source changes (the source wins) until you pause or stop it".into(),
        );
        Ok(())
    }

    /// Records where following starts, before anything is copied.
    pub(super) async fn before_copy(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
        mappings: &[ItemMapping],
        saved: &std::collections::HashMap<String, (Saved, bool)>,
    ) -> Result<(), String> {
        let Some(sync) = &req.sync else { return Ok(()) };
        if run.dry_run || !saved.is_empty() {
            return Ok(());
        }
        let source = self
            .explorer
            .session(req.source)
            .await
            .map_err(|e| self.redactor.scrub(&e.to_string()))?;
        let save = |key: String, position: ReadPosition| async move {
            checkpoint(
                &self.store,
                run.id,
                &run.config_fingerprint,
                &key,
                &Saved {
                    position: Some(position),
                    stats: Default::default(),
                    indexes_created: 0,
                    quarantine: None,
                },
                false,
            )
            .await
            .map_err(|e| e.to_string())
        };
        match sync.method {
            SyncKind::ChangeStream => {
                let objects: Vec<ObjectRef> = req
                    .items
                    .iter()
                    .zip(mappings)
                    .filter(|(_, m)| followed(m))
                    .map(|(i, _)| i.source.clone())
                    .collect();
                let pos = source.current_position(&objects).await.map_err(|e| {
                    self.redactor
                        .scrub(&format!("cannot start following changes: {e}"))
                })?;
                save(CHANGES.into(), pos).await?;
            }
            SyncKind::Polling => {
                let field = sync.cursor_field.clone().unwrap_or_default();
                for (item, mapping) in req.items.iter().zip(mappings) {
                    if !followed(mapping) {
                        continue;
                    }
                    let mut r = ReadRequest::new(item.source.clone(), 1);
                    r.order_by = vec![SortKey {
                        field: FieldPath::new(field.clone()),
                        direction: SortDirection::Desc,
                    }];
                    r.limit = Some(1);
                    let mut top = None;
                    if let Ok(mut s) = source.read(r, CancellationToken::new()).await
                        && let Some(Ok(b)) = s.next().await
                    {
                        top = b
                            .records
                            .first()
                            .map(|rec| rec.get(&field).clone())
                            .filter(|v| !matches!(v, Value::Null | Value::Missing));
                    }
                    save(poll_key(item), cursor_position(top.as_ref())).await?;
                }
            }
        }
        self.event(run.id, EventLevel::Info, "sync_start", None, "The starting point of the sync is recorded; changes made during the copy are applied after it")
            .await;
        Ok(())
    }

    /// Follows source changes until stopped; returns how it ended.
    pub(super) async fn follow(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
        mappings: &[ItemMapping],
        streams: &Arc<Mutex<Vec<StreamView>>>,
        stop: &CancellationToken,
    ) -> StreamEnd {
        let Some(sync) = &req.sync else {
            return StreamEnd::Done { warnings: false };
        };
        let (source, target) = match (
            self.explorer.session(req.source).await,
            self.explorer.session(req.target).await,
        ) {
            (Ok(s), Ok(t)) => (s, t),
            (Err(e), _) | (_, Err(e)) => {
                return StreamEnd::Failed(self.redactor.scrub(&e.to_string()));
            }
        };
        for (i, m) in mappings.iter().enumerate() {
            if followed(m) {
                set_stream(streams, i, |v| v.phase = StreamPhase::Following);
            }
        }
        self.event(
            run.id,
            EventLevel::Info,
            "following",
            None,
            "The copy is done; following changes",
        )
        .await;
        let end = match sync.method {
            SyncKind::ChangeStream => {
                self.follow_stream(run, req, mappings, streams, stop, source, target)
                    .await
            }
            SyncKind::Polling => {
                self.follow_poll(run, req, mappings, streams, stop, source, target)
                    .await
            }
        };
        let phase = match &end {
            StreamEnd::Failed(_) => StreamPhase::Failed,
            _ => StreamPhase::Stopped,
        };
        for (i, m) in mappings.iter().enumerate() {
            if followed(m) {
                set_stream(streams, i, |v| v.phase = phase);
            }
        }
        end
    }

    async fn saved_position(&self, run: &RunRecord, key: &str) -> Option<ReadPosition> {
        self.store
            .jobs()
            .checkpoints(run.id)
            .await
            .ok()?
            .into_iter()
            .find(|c| c.stream == key)
            .and_then(|c| serde_json::from_value::<Saved>(c.position?).ok())
            .and_then(|s| s.position)
    }

    async fn save_position(
        &self,
        run: &RunRecord,
        key: &str,
        position: ReadPosition,
    ) -> AppResult<()> {
        checkpoint(
            &self.store,
            run.id,
            &run.config_fingerprint,
            key,
            &Saved {
                position: Some(position),
                stats: Default::default(),
                indexes_created: 0,
                quarantine: None,
            },
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // the whole follow loop's context
    async fn follow_stream(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
        mappings: &[ItemMapping],
        streams: &Arc<Mutex<Vec<StreamView>>>,
        stop: &CancellationToken,
        source: Arc<dyn Connection>,
        target: Arc<dyn Connection>,
    ) -> StreamEnd {
        let items: Vec<usize> = (0..req.items.len())
            .filter(|i| followed(&mappings[*i]))
            .collect();
        let objects: Vec<ObjectRef> = items.iter().map(|i| req.items[*i].source.clone()).collect();
        let by_name: BTreeMap<String, usize> = items
            .iter()
            .map(|i| (req.items[*i].source.name.clone(), *i))
            .collect();
        let mut position = self.saved_position(run, CHANGES).await;
        let retry = gazabridge_migration::RetryPolicy::default();
        let mut attempt = 0u32;
        'reopen: loop {
            if stop.is_cancelled() {
                return StreamEnd::Stopped;
            }
            let feed = source
                .change_feed(
                    ChangeFeedRequest {
                        objects: objects.clone(),
                        method: SyncMethod::ChangeStream,
                        resume_after: position.clone(),
                        cursor_field: None,
                        poll_interval_ms: 0,
                    },
                    stop.clone(),
                )
                .await;
            let mut feed = match feed {
                Ok(f) => f,
                Err(e) if e.is_retryable() && attempt + 1 < retry.max_attempts => {
                    attempt += 1;
                    tokio::select! {
                        () = tokio::time::sleep(retry.delay(attempt)) => continue 'reopen,
                        () = stop.cancelled() => return StreamEnd::Stopped,
                    }
                }
                Err(e) => return self.sync_failed(run, &e.to_string()).await,
            };
            loop {
                let first = tokio::select! {
                    () = stop.cancelled() => return StreamEnd::Stopped,
                    ev = feed.next() => ev,
                };
                let mut failure = None;
                let mut batch = Vec::new();
                match first {
                    None => continue 'reopen,
                    Some(Ok(ev)) => batch.push(ev),
                    Some(Err(e)) => failure = Some(e),
                }
                while failure.is_none() && batch.len() < BATCH {
                    match tokio::time::timeout(Duration::from_millis(200), feed.next()).await {
                        Ok(Some(Ok(ev))) => batch.push(ev),
                        Ok(Some(Err(e))) => failure = Some(e),
                        _ => break,
                    }
                }
                if !batch.is_empty() {
                    attempt = 0;
                    let mut pending: BTreeMap<usize, Pending> = BTreeMap::new();
                    let mut latest: BTreeMap<usize, chrono::DateTime<chrono::Utc>> =
                        BTreeMap::new();
                    for ev in &batch {
                        let Some(&i) = by_name.get(&ev.object.name) else {
                            continue;
                        };
                        let key = ev.key.get("_id").clone();
                        let after = if ev.operation == ChangeOperation::Delete {
                            None
                        } else {
                            ev.after.clone()
                        };
                        pending.entry(i).or_default().push(key, after);
                        latest.insert(i, ev.source_time.unwrap_or_else(chrono::Utc::now));
                    }
                    for (i, p) in pending {
                        let (ups, dels) = p.split();
                        match self
                            .apply(
                                &req.items[i],
                                &mappings[i],
                                source.clone(),
                                target.as_ref(),
                                ups,
                                &dels,
                            )
                            .await
                        {
                            Ok((n, failed)) => {
                                let at = latest.get(&i).copied();
                                set_stream(streams, i, |v| {
                                    v.changes += n + dels.len() as u64;
                                    v.deletes += dels.len() as u64;
                                    v.stats.failed += failed;
                                    v.last_change = at;
                                });
                            }
                            Err(e) => return self.sync_failed(run, &e.to_string()).await,
                        }
                    }
                    position = batch.last().map(|e| e.position.clone());
                    if let Some(p) = &position
                        && let Err(e) = self.save_position(run, CHANGES, p.clone()).await
                    {
                        return self.sync_failed(run, &e.to_string()).await;
                    }
                }
                if let Some(e) = failure {
                    if matches!(e, gazabridge_plugin_sdk::ConnectorError::Cancelled) {
                        return StreamEnd::Stopped;
                    }
                    if e.is_retryable() && attempt + 1 < retry.max_attempts {
                        attempt += 1;
                        tokio::select! {
                            () = tokio::time::sleep(retry.delay(attempt)) => continue 'reopen,
                            () = stop.cancelled() => return StreamEnd::Stopped,
                        }
                    }
                    return self.sync_failed(run, &e.to_string()).await;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)] // the whole follow loop's context
    async fn follow_poll(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
        mappings: &[ItemMapping],
        streams: &Arc<Mutex<Vec<StreamView>>>,
        stop: &CancellationToken,
        source: Arc<dyn Connection>,
        target: Arc<dyn Connection>,
    ) -> StreamEnd {
        let Some(sync) = &req.sync else {
            return StreamEnd::Stopped;
        };
        let field = sync.cursor_field.clone().unwrap_or_default();
        let interval = Duration::from_secs(sync.interval_secs.max(1));
        loop {
            for (i, item) in req.items.iter().enumerate() {
                if !followed(&mappings[i]) {
                    continue;
                }
                if stop.is_cancelled() {
                    return StreamEnd::Stopped;
                }
                let key = poll_key(item);
                let last = self
                    .saved_position(run, &key)
                    .await
                    .as_ref()
                    .and_then(cursor_of);
                let from = match &last {
                    Some(Value::UtcDateTime(t)) => Some(Value::UtcDateTime(*t - OVERLAP)),
                    Some(Value::LocalDateTime(t)) => Some(Value::LocalDateTime(*t - OVERLAP)),
                    other => other.clone(),
                };
                let source_keys = mappings[i].source_keys();
                let mut r = ReadRequest::new(item.source.clone(), 1_000);
                r.filter = from.map(|v| Filter::Ge {
                    field: FieldPath::new(field.clone()),
                    value: v,
                });
                r.order_by = std::iter::once(field.clone())
                    .chain(source_keys.iter().filter(|k| **k != field).cloned())
                    .map(|f| SortKey {
                        field: FieldPath::new(f),
                        direction: SortDirection::Asc,
                    })
                    .collect();
                let mut stream = match source.read(r, stop.clone()).await {
                    Ok(s) => s,
                    Err(e) if e.is_retryable() => continue,
                    Err(e) => return self.sync_failed(run, &e.to_string()).await,
                };
                let mut newest = last.clone();
                while let Some(batch) = stream.next().await {
                    let records = match batch {
                        Ok(b) => b.records,
                        Err(gazabridge_plugin_sdk::ConnectorError::Cancelled) => {
                            return StreamEnd::Stopped;
                        }
                        Err(e) if e.is_retryable() => break,
                        Err(e) => return self.sync_failed(run, &e.to_string()).await,
                    };
                    // Records at the boundary were applied in the previous round.
                    let fresh = records
                        .iter()
                        .filter(|r| {
                            last.as_ref()
                                .is_none_or(|l| key_text(r.get(&field)) != key_text(l))
                        })
                        .count() as u64;
                    if let Some(v) = records
                        .last()
                        .map(|r| r.get(&field).clone())
                        .filter(|v| !matches!(v, Value::Null | Value::Missing))
                    {
                        newest = Some(v);
                    }
                    match self
                        .apply(
                            item,
                            &mappings[i],
                            source.clone(),
                            target.as_ref(),
                            records,
                            &[],
                        )
                        .await
                    {
                        Ok((_, failed)) => set_stream(streams, i, |v| {
                            v.changes += fresh;
                            v.stats.failed += failed;
                            if fresh > 0 {
                                v.last_change = Some(chrono::Utc::now());
                            }
                        }),
                        Err(e) => return self.sync_failed(run, &e.to_string()).await,
                    }
                }
                if newest != last
                    && let Err(e) = self
                        .save_position(run, &key, cursor_position(newest.as_ref()))
                        .await
                {
                    return self.sync_failed(run, &e.to_string()).await;
                }
            }
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                () = stop.cancelled() => return StreamEnd::Stopped,
            }
        }
    }

    async fn sync_failed(&self, run: &RunRecord, message: &str) -> StreamEnd {
        let m = self.redactor.scrub(&format!("the sync stopped: {message}"));
        self.event(run.id, EventLevel::Error, "sync_failed", None, &m)
            .await;
        StreamEnd::Failed(m)
    }

    /// Writes changed records (as upserts) and removes deleted ones.
    /// Returns the rows written and the records the target rejected.
    async fn apply(
        &self,
        item: &super::CopyItem,
        mapping: &ItemMapping,
        source: Arc<dyn Connection>,
        target: &dyn Connection,
        upserts: Vec<Record>,
        deletes: &[Value],
    ) -> AppResult<(u64, u64)> {
        let transform: Arc<dyn Transform> = match mapping.transform(ExistingData::Update, source) {
            Some(t) => t,
            None => Arc::new(Identity(TargetSpec {
                object: item.target.clone(),
                key_fields: vec!["_id".into()],
                owner: None,
                replace: false,
            })),
        };
        let targets = transform.targets();
        let mut written = 0;
        let mut failed = 0;
        if !upserts.is_empty() {
            let out = transform.apply(&upserts).await?;
            failed += out.rejected.len() as u64;
            for (t, rows) in out.rows.into_iter().enumerate() {
                let Some(spec) = targets.get(t) else { continue };
                if spec.replace
                    && let Some(owner) = &spec.owner
                    && !out.owners.is_empty()
                {
                    self.with_retry(|| {
                        target.delete(DeleteRequest {
                            object: spec.object.clone(),
                            filter: Filter::In {
                                field: FieldPath::new(owner.clone()),
                                values: out.owners.iter().map(|(_, v)| v.clone()).collect(),
                            },
                            expect_matched: None,
                        })
                    })
                    .await?;
                }
                if rows.is_empty() {
                    continue;
                }
                let records: Vec<Record> = rows.into_iter().map(|(_, r)| r).collect();
                let outcome = self
                    .with_retry(|| {
                        target.write_batch(
                            WriteBatch {
                                object: spec.object.clone(),
                                mode: WriteMode::Upsert,
                                records: records.clone(),
                                key_fields: spec.key_fields.clone(),
                                atomic: false,
                            },
                            CancellationToken::new(),
                        )
                    })
                    .await?;
                if t == 0 {
                    written += outcome.inserted + outcome.updated;
                }
                failed += outcome.failures.len() as u64;
            }
        }
        if !deletes.is_empty() {
            match mapping {
                ItemMapping::DocToTable { mapping, .. } => {
                    let keys: Vec<Value> = deletes
                        .iter()
                        .filter_map(|k| doc_to_table::key_value(mapping, k))
                        .collect();
                    // Children first: they reference the root rows.
                    for t in mapping.tables.iter().skip(1).rev() {
                        self.with_retry(|| {
                            target.delete(DeleteRequest {
                                object: t.target.clone(),
                                filter: Filter::In {
                                    field: FieldPath::new(doc_to_table::PARENT),
                                    values: keys.clone(),
                                },
                                expect_matched: None,
                            })
                        })
                        .await?;
                    }
                    self.with_retry(|| {
                        target.delete(DeleteRequest {
                            object: mapping.tables[0].target.clone(),
                            filter: Filter::In {
                                field: FieldPath::new(doc_to_table::ID),
                                values: keys.clone(),
                            },
                            expect_matched: None,
                        })
                    })
                    .await?;
                }
                ItemMapping::Copy | ItemMapping::Collection { .. } => {
                    self.with_retry(|| {
                        target.delete(DeleteRequest {
                            object: item.target.clone(),
                            filter: Filter::In {
                                field: FieldPath::new("_id"),
                                values: deletes.to_vec(),
                            },
                            expect_matched: None,
                        })
                    })
                    .await?;
                }
                _ => {}
            }
        }
        Ok((written, failed))
    }

    async fn with_retry<T, F, Fut>(&self, f: F) -> AppResult<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, gazabridge_plugin_sdk::ConnectorError>>,
    {
        let retry = gazabridge_migration::RetryPolicy::default();
        let mut attempt = 0;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_retryable() && attempt + 1 < retry.max_attempts => {
                    attempt += 1;
                    tokio::time::sleep(retry.delay(attempt)).await;
                }
                Err(e) => return Err(AppError::Connector(e)),
            }
        }
    }
}
