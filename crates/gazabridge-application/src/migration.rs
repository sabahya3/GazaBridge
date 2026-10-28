//! Copying and converting between connections: MongoDB → MongoDB copies,
//! MongoDB → PostgreSQL and PostgreSQL → MongoDB conversions.
//!
//! A copy is a job with runs. Each collection or table is a stream copied by the
//! migration engine; progress is checkpointed in the metadata store after
//! every durable write, so a run can be paused, resumed, cancelled, and
//! continued after the app was closed or crashed, without losing or
//! duplicating documents. Indexes are recreated from the source's full
//! specification once a collection's data is in place.
//!
//! Nothing is written before [`MigrationService::plan`] has passed: the
//! plan reads counts and index definitions only.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::ids::{ConnectionId, RunId};
use gazabridge_domain::job::{JobKind, JobStatus, WriteMode};
use gazabridge_mapping::doc_to_table::{DocMode, TablePreview};
use gazabridge_mapping::table_to_doc::TableMode;
use gazabridge_migration::validate::{Comparison, Level, StreamValidation, validate_stream};
use gazabridge_migration::{
    Checkpoints, EngineOptions, Identity, RecordErrors, Resume, RetryPolicy, StreamSpec,
    StreamStats, TargetSpec, Transform, convert_stream, copy_stream,
};
use gazabridge_plugin_sdk::connector::{Connection, InspectOptions};
use gazabridge_plugin_sdk::ddl::{IndexDefinition, SchemaOperation};
use gazabridge_plugin_sdk::read::ReadPosition;
use gazabridge_plugin_sdk::schema::{ObjectKind, ObjectRef};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::audit::NewAuditEvent;
use gazabridge_storage::jobs::{Checkpoint, EventLevel, NewJob, NewRun, RunRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::convert::{self, Direction, ItemMapping};
use crate::explorer::ExplorerService;
use crate::{AppError, AppResult};

/// Rejection messages kept per stream in a run's report.
const MAX_REJECTIONS: usize = 20;

/// What to do when the target collection already holds documents.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExistingData {
    /// Keep target documents; copy only documents whose `_id` is new.
    #[default]
    Skip,
    /// Replace target documents that have the same `_id`.
    Update,
    /// Stop at the first document whose `_id` already exists.
    Fail,
    /// Drop the target collection first. Destructive; needs confirmation.
    Replace,
}

impl ExistingData {
    fn write_mode(self) -> WriteMode {
        match self {
            Self::Skip => WriteMode::SkipExisting,
            Self::Update => WriteMode::Upsert,
            Self::Fail | Self::Replace => WriteMode::Insert,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnRecordError {
    #[default]
    Stop,
    /// Write rejected documents to a file and continue.
    Quarantine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyItem {
    pub source: ObjectRef,
    pub target: ObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRequest {
    #[serde(default)]
    pub name: Option<String>,
    pub source: ConnectionId,
    pub target: ConnectionId,
    pub items: Vec<CopyItem>,
    #[serde(default)]
    pub existing: ExistingData,
    #[serde(default = "yes")]
    pub copy_indexes: bool,
    #[serde(default = "default_batch")]
    pub batch_size: usize,
    #[serde(default)]
    pub on_record_error: OnRecordError,
    #[serde(default)]
    pub max_records_per_sec: Option<u64>,
    /// Collections copied at the same time.
    #[serde(default = "one")]
    pub parallel: usize,
    /// MongoDB → PostgreSQL: how documents become rows.
    #[serde(default)]
    pub doc_mode: DocMode,
    /// PostgreSQL → MongoDB: how rows become documents.
    #[serde(default)]
    pub table_mode: TableMode,
}

/// What a run stores: the request and the mapping decided for each item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RunDefinition {
    request: MigrationRequest,
    mappings: Vec<ItemMapping>,
}

impl RunDefinition {
    fn parse(v: &serde_json::Value) -> AppResult<Self> {
        if let Ok(d) = serde_json::from_value::<Self>(v.clone()) {
            return Ok(d);
        }
        // Runs recorded before conversions existed hold the request alone.
        let request: MigrationRequest = serde_json::from_value(v.clone())
            .map_err(|e| AppError::Invalid(format!("unreadable copy definition: {e}")))?;
        let mappings = vec![ItemMapping::Copy; request.items.len()];
        Ok(Self { request, mappings })
    }
}

fn yes() -> bool {
    true
}
fn default_batch() -> usize {
    1_000
}
fn one() -> usize {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanItem {
    pub source: ObjectRef,
    pub target: ObjectRef,
    /// `copy`, `doc_to_table`, `table_to_doc` or `embedded`.
    pub kind: String,
    /// Estimated documents in the source.
    pub source_count: Option<u64>,
    pub target_exists: bool,
    pub target_count: Option<u64>,
    /// Indexes that will be created (the `_id` index always exists).
    pub indexes: Vec<String>,
    /// What will run on the target, as shell statements or SQL.
    pub statements: Vec<String>,
    pub warnings: Vec<String>,
    /// Conversions to tables: the planned tables with sample rows.
    #[serde(default)]
    pub tables: Vec<TablePreview>,
    /// Sample source documents (to tables) or resulting documents (from tables).
    #[serde(default)]
    pub documents: Vec<String>,
    /// Rows embedded in this collection's documents instead of their own.
    #[serde(default)]
    pub embedded_into: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub items: Vec<PlanItem>,
    /// Reasons the copy cannot start.
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    /// Target data will be dropped; starting needs explicit confirmation.
    pub destructive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamPhase {
    Pending,
    Preparing,
    Copying,
    Indexing,
    Done,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamView {
    pub source: ObjectRef,
    pub target: ObjectRef,
    pub phase: StreamPhase,
    pub total: Option<u64>,
    pub stats: StreamStats,
    pub indexes_created: u32,
    pub quarantine: Option<PathBuf>,
    pub rejections: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEventView {
    pub at: DateTime<Utc>,
    pub level: String,
    pub stream: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunView {
    pub run_id: RunId,
    pub name: String,
    pub status: JobStatus,
    pub source: ConnectionId,
    pub target: ConnectionId,
    pub existing: ExistingData,
    pub streams: Vec<StreamView>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub events: Vec<RunEventView>,
    /// Read and converted everything, wrote nothing.
    pub dry_run: bool,
    /// The latest validation of this run (live while it runs).
    pub validation: Option<ValidationReport>,
}

/// Source and target compared after a copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReport {
    pub level: Level,
    pub running: bool,
    /// `None` while running.
    pub passed: Option<bool>,
    pub streams: Vec<StreamValidation>,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopReason {
    Pause,
    Cancel,
}

struct Handle {
    streams: Arc<Mutex<Vec<StreamView>>>,
    stop: CancellationToken,
    reason: Arc<Mutex<Option<StopReason>>>,
}

#[derive(Clone)]
pub struct MigrationService {
    store: MetadataStore,
    explorer: ExplorerService,
    redactor: gazabridge_secrets::Redactor,
    quarantine_dir: PathBuf,
    active: Arc<Mutex<HashMap<RunId, Handle>>>,
    validations: Arc<Mutex<HashMap<RunId, ValidationReport>>>,
}

impl std::fmt::Debug for MigrationService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationService").finish_non_exhaustive()
    }
}

/// Checkpoint position plus the stream's totals, saved together.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Saved {
    position: Option<ReadPosition>,
    stats: StreamStats,
    #[serde(default)]
    indexes_created: u32,
    #[serde(default)]
    quarantine: Option<PathBuf>,
}

struct StoreCheckpoints {
    store: MetadataStore,
    run_id: RunId,
    fingerprint: String,
    quarantine: Option<PathBuf>,
}

#[async_trait]
impl Checkpoints for StoreCheckpoints {
    async fn save(
        &self,
        stream: &str,
        position: Option<&ReadPosition>,
        stats: &StreamStats,
    ) -> Result<(), String> {
        let saved = Saved {
            position: position.cloned(),
            stats: stats.clone(),
            indexes_created: 0,
            quarantine: self.quarantine.clone(),
        };
        checkpoint(
            &self.store,
            self.run_id,
            &self.fingerprint,
            stream,
            &saved,
            false,
        )
        .await
        .map_err(|e| e.to_string())
    }
}

async fn checkpoint(
    store: &MetadataStore,
    run_id: RunId,
    fingerprint: &str,
    stream: &str,
    saved: &Saved,
    completed: bool,
) -> AppResult<()> {
    store
        .jobs()
        .save_checkpoint(&Checkpoint {
            run_id,
            stream: stream.to_owned(),
            position: Some(
                serde_json::to_value(saved).map_err(|e| AppError::Invalid(e.to_string()))?,
            ),
            records_committed: i64::try_from(saved.stats.processed()).unwrap_or(i64::MAX),
            bytes_committed: i64::try_from(saved.stats.bytes).unwrap_or(i64::MAX),
            completed,
            config_fingerprint: fingerprint.to_owned(),
            updated_at: Utc::now(),
        })
        .await?;
    Ok(())
}

fn stream_key(item: &CopyItem) -> String {
    item.target.to_string()
}

fn family(conn: &dyn Connection) -> EngineFamily {
    conn.manifest().family
}

fn view_for(item: &CopyItem) -> StreamView {
    StreamView {
        source: item.source.clone(),
        target: item.target.clone(),
        phase: StreamPhase::Pending,
        total: None,
        stats: StreamStats::default(),
        indexes_created: 0,
        quarantine: None,
        rejections: Vec::new(),
        error: None,
    }
}

fn set_stream(streams: &Mutex<Vec<StreamView>>, i: usize, f: impl FnOnce(&mut StreamView)) {
    if let Ok(mut s) = streams.lock()
        && let Some(v) = s.get_mut(i)
    {
        f(v);
    }
}

enum StreamEnd {
    Done { warnings: bool },
    Stopped,
    Failed(String),
}

impl MigrationService {
    pub fn new(
        store: MetadataStore,
        explorer: ExplorerService,
        redactor: gazabridge_secrets::Redactor,
        quarantine_dir: PathBuf,
    ) -> Self {
        Self {
            store,
            explorer,
            redactor,
            quarantine_dir,
            active: Arc::default(),
            validations: Arc::default(),
        }
    }

    /// Source, target and options; changing any of them invalidates
    /// checkpoints (a resume would otherwise continue somewhere else).
    async fn fingerprint(&self, req: &MigrationRequest) -> AppResult<String> {
        let source = self.store.connections().require(req.source).await?;
        let target = self.store.connections().require(req.target).await?;
        let mut h = Sha256::new();
        for part in [
            serde_json::to_vec(req),
            serde_json::to_vec(&source.settings),
            serde_json::to_vec(&target.settings),
        ] {
            h.update(part.map_err(|e| AppError::Invalid(e.to_string()))?);
        }
        Ok(hex::encode(h.finalize()))
    }

    /// Checks the request and describes what a copy would do. Reads only.
    pub async fn plan(&self, req: &MigrationRequest) -> AppResult<MigrationPlan> {
        Ok(self.prepare(req).await?.0)
    }

    /// The plan and, for each item, the mapping a run will use.
    async fn prepare(
        &self,
        req: &MigrationRequest,
    ) -> AppResult<(MigrationPlan, Vec<ItemMapping>)> {
        let mut plan = MigrationPlan {
            items: Vec::new(),
            blockers: Vec::new(),
            warnings: Vec::new(),
            destructive: false,
        };
        if req.items.is_empty() {
            plan.blockers
                .push("choose at least one collection to copy".into());
            return Ok((plan, Vec::new()));
        }
        let source = self.explorer.session(req.source).await?;
        let target = self.explorer.session(req.target).await?;
        let (sf, tf) = (family(source.as_ref()), family(target.as_ref()));
        let Some(direction) = convert::direction(sf, tf) else {
            plan.blockers.push(format!(
                "{sf:?} → {tf:?} is not supported yet; supported: MongoDB → MongoDB, MongoDB → PostgreSQL, PostgreSQL → MongoDB"
            ));
            return Ok((plan, Vec::new()));
        };
        if target.is_read_only() {
            plan.blockers
                .push("the target connection is read-only; edit it to allow writes".into());
        }
        let mut targets = BTreeSet::new();
        for item in &req.items {
            if !targets.insert(item.target.to_string()) {
                plan.blockers
                    .push(format!("`{}` is the target of two sources", item.target));
            }
        }
        if direction != Direction::Copy {
            let planned = match direction {
                Direction::DocToTable => {
                    convert::plan_doc_to_table(req, source.as_ref(), target.as_ref()).await?
                }
                _ => convert::plan_table_to_doc(req, &source, target.as_ref()).await?,
            };
            plan.items = planned.items;
            plan.blockers.extend(planned.blockers);
            plan.destructive = planned.destructive;
            return Ok((plan, planned.mappings));
        }
        let mut mappings = Vec::new();
        let mut seen = BTreeSet::new();
        let mut target_names: HashMap<Option<String>, BTreeSet<String>> = HashMap::new();
        for item in &req.items {
            let mut p = PlanItem {
                source: item.source.clone(),
                target: item.target.clone(),
                kind: "copy".into(),
                tables: Vec::new(),
                documents: Vec::new(),
                embedded_into: None,
                source_count: None,
                target_exists: false,
                target_count: None,
                indexes: Vec::new(),
                statements: Vec::new(),
                warnings: Vec::new(),
            };
            seen.insert(item.target.to_string());
            if req.source == req.target && item.source == item.target {
                plan.blockers.push(format!(
                    "`{}` would be copied onto itself; choose another target database or connection",
                    item.source
                ));
            }
            if item.source.kind == ObjectKind::View {
                p.warnings.push(
                    "a view is copied as a regular collection holding its current results".into(),
                );
            }
            p.source_count = source
                .count(&item.source, None, false)
                .await
                .ok()
                .map(|c| c.count);
            let names = match target_names.entry(item.target.namespace.database.clone()) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => e.insert(
                    target
                        .list(&item.target.namespace)
                        .await
                        .map(|l| l.into_iter().map(|n| n.name).collect())
                        .unwrap_or_default(),
                ),
            };
            p.target_exists = names.contains(&item.target.name);
            if p.target_exists {
                p.target_count = target
                    .count(&item.target, None, false)
                    .await
                    .ok()
                    .map(|c| c.count);
            }
            let has_data = p.target_count.unwrap_or(0) > 0;
            let mut ops = Vec::new();
            match req.existing {
                ExistingData::Replace if p.target_exists => {
                    plan.destructive = true;
                    p.warnings.push(format!(
                        "the target collection and its {} document(s) will be dropped first",
                        p.target_count.map_or("~?".into(), |c| c.to_string())
                    ));
                    ops.push(SchemaOperation::DropObject {
                        object: item.target.clone(),
                        if_exists: true,
                    });
                }
                ExistingData::Fail if has_data => p.warnings.push(
                    "the target already has documents; the copy stops at the first _id that exists"
                        .into(),
                ),
                ExistingData::Skip if has_data => p.warnings.push(
                    "the target already has documents; those with the same _id are kept as they are"
                        .into(),
                ),
                ExistingData::Update if has_data => p.warnings.push(
                    "the target already has documents; those with the same _id are replaced".into(),
                ),
                _ => {}
            }
            ops.push(SchemaOperation::CreateCollection {
                object: item.target.clone(),
                if_not_exists: true,
            });
            if req.copy_indexes {
                for ix in self
                    .indexes_to_copy(
                        source.as_ref(),
                        target.as_ref(),
                        item,
                        req.existing,
                        p.target_exists,
                    )
                    .await?
                {
                    p.indexes.push(ix.name.clone());
                    ops.push(SchemaOperation::CreateIndex {
                        object: item.target.clone(),
                        index: ix,
                        if_not_exists: true,
                    });
                }
            }
            match target.plan_ddl(&ops) {
                Ok(d) => p.statements = d.statements,
                Err(e) => plan.blockers.push(format!("{}: {e}", item.target)),
            }
            plan.items.push(p);
            mappings.push(ItemMapping::Copy);
        }
        Ok((plan, mappings))
    }

    /// Source indexes (except `_id`) the target does not already have.
    async fn indexes_to_copy(
        &self,
        source: &dyn Connection,
        target: &dyn Connection,
        item: &CopyItem,
        existing: ExistingData,
        target_exists: bool,
    ) -> AppResult<Vec<IndexDefinition>> {
        let options = InspectOptions {
            include_indexes: true,
            include_constraints: false,
            sample_size: 1,
            ..InspectOptions::default()
        };
        let src = source.inspect(&item.source, &options).await?;
        let present: BTreeSet<String> = if target_exists && existing != ExistingData::Replace {
            target
                .inspect(&item.target, &options)
                .await
                .map(|s| s.indexes.into_iter().map(|i| i.name).collect())
                .unwrap_or_default()
        } else {
            BTreeSet::new()
        };
        Ok(src
            .indexes
            .into_iter()
            .filter(|i| !i.primary && !present.contains(&i.name))
            .map(|i| IndexDefinition {
                name: i.name,
                columns: i.columns,
                unique: i.unique,
                native: i.native,
            })
            .collect())
    }

    /// Plans, records the job and run, and starts copying in the background.
    pub async fn start(
        &self,
        req: MigrationRequest,
        confirm_destructive: bool,
    ) -> AppResult<RunId> {
        self.launch(req, confirm_destructive, false).await
    }

    /// Reads and converts everything without writing: what would be
    /// inserted, kept, replaced, or rejected and why. Nothing is created or
    /// dropped either.
    pub async fn dry_run(&self, req: MigrationRequest) -> AppResult<RunId> {
        self.launch(req, true, true).await
    }

    async fn launch(
        &self,
        req: MigrationRequest,
        confirm_destructive: bool,
        dry_run: bool,
    ) -> AppResult<RunId> {
        let (plan, mappings) = self.prepare(&req).await?;
        if let Some(b) = plan.blockers.first() {
            return Err(AppError::Invalid(b.clone()));
        }
        if plan.destructive && !confirm_destructive {
            return Err(AppError::ConfirmationRequired("drop_target".into()));
        }
        let fingerprint = self.fingerprint(&req).await?;
        let definition = serde_json::to_value(RunDefinition {
            request: req.clone(),
            mappings: mappings.clone(),
        })
        .map_err(|e| AppError::Invalid(e.to_string()))?;
        let name = req
            .name
            .clone()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| {
                let db = |o: &ObjectRef| o.namespace.database.clone().unwrap_or_default();
                format!(
                    "{} {} → {} · {}",
                    if dry_run { "Dry run" } else { "Copy" },
                    db(&req.items[0].source),
                    db(&req.items[0].target),
                    Utc::now().format("%Y-%m-%d %H:%M:%S")
                )
            });
        let jobs = self.store.jobs();
        let job = jobs
            .create(NewJob {
                name,
                kind: JobKind::Migration,
                definition: definition.clone(),
                source_connection_id: Some(req.source),
                target_connection_id: Some(req.target),
                mapping: None,
                transformation: None,
            })
            .await?;
        jobs.transition(job.id, JobStatus::Validating).await?;
        jobs.transition(job.id, JobStatus::Ready).await?;
        let run = jobs
            .create_run(NewRun {
                job_id: job.id,
                dry_run,
                definition_snapshot: definition,
                config_fingerprint: fingerprint,
                mapping_version: None,
                transformation_version: None,
                batch_settings: serde_json::json!({ "batch_size": req.batch_size }),
                concurrency: i64::try_from(req.parallel.clamp(1, 4)).unwrap_or(1),
                retry_policy: serde_json::to_value(RetryPolicy::default()).unwrap_or_default(),
                app_version: env!("CARGO_PKG_VERSION").into(),
            })
            .await?;
        jobs.transition_run(run.id, JobStatus::Running).await?;
        self.audit(
            if dry_run { "migration.dry_run" } else { "migration.start" },
            &run,
            format!(
                "Started {} {} collection(s)",
                if dry_run { "a dry run of" } else { "copying" },
                req.items.len()
            ),
            serde_json::json!({
                "items": req.items.iter().map(|i| format!("{} → {}", i.source, i.target)).collect::<Vec<_>>(),
                "existing": req.existing, "destructive": plan.destructive,
            }),
        )
        .await?;
        let started = if dry_run {
            "Dry run started: everything is read and converted, nothing is written"
        } else {
            "Copy started"
        };
        self.event(run.id, EventLevel::Info, "started", None, started)
            .await;
        let run_id = run.id;
        self.spawn(run, req, mappings, HashMap::new());
        Ok(run_id)
    }

    /// Continues a paused, failed or interrupted run from its checkpoints.
    pub async fn resume(&self, run_id: RunId) -> AppResult<()> {
        if self.is_active(run_id) {
            return Err(AppError::Invalid("this copy is already running".into()));
        }
        let jobs = self.store.jobs();
        let run = jobs.require_run(run_id).await?;
        if run.dry_run {
            return Err(AppError::Invalid(
                "a dry run is not resumed; start a new one".into(),
            ));
        }
        let RunDefinition {
            request: req,
            mappings,
        } = RunDefinition::parse(&run.definition_snapshot)?;
        jobs.verify_resumable(run_id, &self.fingerprint(&req).await?)
            .await?;
        match run.status {
            JobStatus::Paused => {
                jobs.transition_run(run_id, JobStatus::Resuming).await?;
            }
            JobStatus::Failed => {}
            other => {
                return Err(AppError::Invalid(format!(
                    "a {other} copy cannot be resumed"
                )));
            }
        }
        let run = jobs.transition_run(run_id, JobStatus::Running).await?;
        let saved: HashMap<String, (Saved, bool)> = jobs
            .checkpoints(run_id)
            .await?
            .into_iter()
            .filter_map(|c| {
                let s: Saved = serde_json::from_value(c.position?).ok()?;
                Some((c.stream, (s, c.completed)))
            })
            .collect();
        self.event(
            run_id,
            EventLevel::Info,
            "resumed",
            None,
            "Copy resumed from its last checkpoint",
        )
        .await;
        self.spawn(run, req, mappings, saved);
        Ok(())
    }

    /// Stops after the current batch; the run can be resumed later.
    pub async fn pause(&self, run_id: RunId) -> AppResult<()> {
        self.stop(run_id, StopReason::Pause).await
    }

    /// Stops a running copy, or ends a paused or failed one for good.
    pub async fn cancel(&self, run_id: RunId) -> AppResult<()> {
        if self.is_active(run_id) {
            return self.stop(run_id, StopReason::Cancel).await;
        }
        let jobs = self.store.jobs();
        let run = jobs.require_run(run_id).await?;
        if run.status == JobStatus::Paused {
            jobs.transition_run(run_id, JobStatus::Cancelling).await?;
            jobs.transition_run(run_id, JobStatus::Cancelled).await?;
            self.event(
                run_id,
                EventLevel::Info,
                "cancelled",
                None,
                "Copy cancelled",
            )
            .await;
            Ok(())
        } else {
            Err(AppError::Invalid(format!(
                "a {} copy cannot be cancelled",
                run.status
            )))
        }
    }

    async fn stop(&self, run_id: RunId, reason: StopReason) -> AppResult<()> {
        let next = match reason {
            StopReason::Pause => JobStatus::Pausing,
            StopReason::Cancel => JobStatus::Cancelling,
        };
        self.store.jobs().transition_run(run_id, next).await?;
        let active = self
            .active
            .lock()
            .map_err(|_| AppError::Invalid("state lock poisoned".into()))?;
        let h = active
            .get(&run_id)
            .ok_or_else(|| AppError::Invalid("this copy is not running".into()))?;
        if let Ok(mut r) = h.reason.lock() {
            *r = Some(reason);
        }
        h.stop.cancel();
        Ok(())
    }

    fn is_active(&self, run_id: RunId) -> bool {
        self.active.lock().is_ok_and(|a| a.contains_key(&run_id))
    }

    /// Runs the app left active when it closed become paused, so they can be
    /// resumed from their checkpoints. Call once at startup.
    pub async fn recover_interrupted(&self) -> AppResult<usize> {
        let jobs = self.store.jobs();
        let mut n = 0;
        for run in jobs.interrupted_runs().await? {
            if self.is_active(run.id) {
                continue;
            }
            let path: &[JobStatus] = match run.status {
                JobStatus::Running => &[JobStatus::Pausing, JobStatus::Paused],
                JobStatus::Pausing => &[JobStatus::Paused],
                JobStatus::Resuming => &[JobStatus::Paused],
                JobStatus::Cancelling => &[JobStatus::Cancelled],
                JobStatus::Validating => &[JobStatus::Failed],
                _ => &[],
            };
            for s in path {
                jobs.transition_run(run.id, *s).await?;
            }
            self.event(
                run.id,
                EventLevel::Warning,
                "interrupted",
                None,
                "The app closed while this copy was running; resume it to continue from the last checkpoint",
            )
            .await;
            n += 1;
        }
        Ok(n)
    }

    fn spawn(
        &self,
        run: RunRecord,
        req: MigrationRequest,
        mappings: Vec<ItemMapping>,
        saved: HashMap<String, (Saved, bool)>,
    ) {
        let streams = Arc::new(Mutex::new(
            req.items
                .iter()
                .map(|item| {
                    let mut v = view_for(item);
                    if let Some((s, done)) = saved.get(&stream_key(item)) {
                        v.stats = s.stats.clone();
                        v.indexes_created = s.indexes_created;
                        v.quarantine = s.quarantine.clone();
                        if *done {
                            v.phase = StreamPhase::Done;
                        }
                    }
                    v
                })
                .collect::<Vec<_>>(),
        ));
        let stop = CancellationToken::new();
        let reason = Arc::new(Mutex::new(None));
        if let Ok(mut a) = self.active.lock() {
            a.insert(
                run.id,
                Handle {
                    streams: streams.clone(),
                    stop: stop.clone(),
                    reason: reason.clone(),
                },
            );
        }
        let svc = self.clone();
        tokio::spawn(async move {
            let ends = svc
                .run_streams(&run, &req, &mappings, &saved, &streams, &stop)
                .await;
            svc.finish(&run, &req, ends, &streams, &reason).await;
            if let Ok(mut a) = svc.active.lock() {
                a.remove(&run.id);
            }
        });
    }

    async fn run_streams(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
        mappings: &[ItemMapping],
        saved: &HashMap<String, (Saved, bool)>,
        streams: &Arc<Mutex<Vec<StreamView>>>,
        stop: &CancellationToken,
    ) -> Vec<StreamEnd> {
        let (source, target) = match (
            self.explorer.session(req.source).await,
            self.explorer.session(req.target).await,
        ) {
            (Ok(s), Ok(t)) => (s, t),
            (Err(e), _) | (_, Err(e)) => {
                return vec![StreamEnd::Failed(self.redactor.scrub(&e.to_string()))];
            }
        };
        futures::stream::iter(0..req.items.len())
            .map(|i| {
                let item = &req.items[i];
                let mapping = mappings.get(i).cloned().unwrap_or(ItemMapping::Copy);
                let (source, target) = (source.clone(), target.clone());
                async move {
                    let end = self
                        .run_stream(
                            run,
                            req,
                            i,
                            item,
                            &mapping,
                            saved.get(&stream_key(item)),
                            source,
                            target,
                            streams,
                            stop,
                        )
                        .await;
                    if matches!(end, StreamEnd::Failed(_)) {
                        // One failure stops the others at their next batch.
                        stop.cancel();
                    }
                    end
                }
            })
            .buffered(req.parallel.clamp(1, 4))
            .collect()
            .await
    }

    #[allow(clippy::too_many_arguments)] // one stream's whole context
    async fn run_stream(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
        i: usize,
        item: &CopyItem,
        mapping: &ItemMapping,
        saved: Option<&(Saved, bool)>,
        source: Arc<dyn Connection>,
        target: Arc<dyn Connection>,
        streams: &Arc<Mutex<Vec<StreamView>>>,
        stop: &CancellationToken,
    ) -> StreamEnd {
        let key = stream_key(item);
        if saved.is_some_and(|(_, done)| *done) {
            return StreamEnd::Done { warnings: false };
        }
        if let ItemMapping::Embedded { into } = mapping {
            set_stream(streams, i, |v| v.phase = StreamPhase::Done);
            self.event(
                run.id,
                EventLevel::Info,
                "embedded",
                Some(&key),
                &format!("{} is embedded in the documents of {into}", item.source),
            )
            .await;
            return StreamEnd::Done { warnings: false };
        }
        if stop.is_cancelled() {
            set_stream(streams, i, |v| v.phase = StreamPhase::Stopped);
            return StreamEnd::Stopped;
        }
        let fail = |message: String| {
            let message = self.redactor.scrub(&message);
            set_stream(streams, i, |v| {
                v.phase = StreamPhase::Failed;
                v.error = Some(message.clone());
            });
            message
        };
        let total = convert::quick_count(source.as_ref(), &item.source).await;
        set_stream(streams, i, |v| {
            v.total = total;
            v.phase = StreamPhase::Preparing;
        });
        // Target preparation happens once, before the first checkpoint.
        if saved.is_none() && !run.dry_run {
            let ops = mapping.prepare_ops(item, req.existing);
            if let Err(e) = target.apply_ddl(&ops, CancellationToken::new()).await {
                let m = fail(format!("preparing {}: {e}", item.target));
                self.event(run.id, EventLevel::Error, "prepare_failed", Some(&key), &m)
                    .await;
                return StreamEnd::Failed(m);
            }
        }
        // Indexes are read before copying so a later failure still reports them.
        let indexes = if run.dry_run {
            Vec::new()
        } else if *mapping != ItemMapping::Copy {
            if req.copy_indexes {
                mapping.index_ops(item)
            } else {
                Vec::new()
            }
        } else if req.copy_indexes {
            match self
                .indexes_to_copy(
                    source.as_ref(),
                    target.as_ref(),
                    item,
                    ExistingData::Skip,
                    true,
                )
                .await
            {
                Ok(ix) => ix
                    .into_iter()
                    .map(|index| SchemaOperation::CreateIndex {
                        object: item.target.clone(),
                        index,
                        if_not_exists: true,
                    })
                    .collect(),
                Err(e) => {
                    let m = fail(format!("reading the indexes of {}: {e}", item.source));
                    return StreamEnd::Failed(m);
                }
            }
        } else {
            Vec::new()
        };
        set_stream(streams, i, |v| v.phase = StreamPhase::Copying);
        let quarantine_dir = self.quarantine_dir.join(run.id.to_string());
        let options = EngineOptions {
            batch_size: req.batch_size.clamp(1, 50_000),
            queue_depth: 4,
            retry: RetryPolicy::default(),
            on_record_error: match req.on_record_error {
                OnRecordError::Stop => RecordErrors::Fail,
                OnRecordError::Quarantine => RecordErrors::Quarantine {
                    dir: quarantine_dir,
                },
            },
            max_records_per_sec: req.max_records_per_sec,
            dry_run: run.dry_run,
            assume_empty_targets: run.dry_run && req.existing == ExistingData::Replace,
        };
        let spec = StreamSpec {
            key: key.clone(),
            source: item.source.clone(),
            target: item.target.clone(),
            filter: None,
            mode: req.existing.write_mode(),
            key_fields: vec!["_id".into()],
            source_keys: mapping.source_keys(),
        };
        let checkpoints = StoreCheckpoints {
            store: self.store.clone(),
            run_id: run.id,
            fingerprint: run.config_fingerprint.clone(),
            quarantine: saved.and_then(|(s, _)| s.quarantine.clone()),
        };
        let resume = saved.map(|(s, _)| Resume {
            position: s.position.clone(),
            stats: s.stats.clone(),
        });
        let progress_streams = streams.clone();
        let on_progress = move |s: &StreamStats| {
            set_stream(&progress_streams, i, |v| v.stats = s.clone());
        };
        let result = match mapping.transform(req.existing, source.clone()) {
            None => {
                copy_stream(
                    source.clone(),
                    target.clone(),
                    &spec,
                    &options,
                    resume,
                    stop.clone(),
                    &checkpoints,
                    &on_progress,
                )
                .await
            }
            Some(transform) => {
                convert_stream(
                    source.clone(),
                    target.clone(),
                    &spec,
                    &options,
                    resume,
                    stop.clone(),
                    &checkpoints,
                    &on_progress,
                    transform,
                )
                .await
            }
        };
        let outcome = match result {
            Ok(o) => o,
            Err(f) => {
                set_stream(streams, i, |v| {
                    v.stats = f.outcome.stats.clone();
                    v.quarantine = f.outcome.quarantine.clone();
                    v.rejections = f.outcome.rejections.clone();
                });
                let m = fail(f.error.to_string());
                self.event(run.id, EventLevel::Error, "stream_failed", Some(&key), &m)
                    .await;
                return StreamEnd::Failed(m);
            }
        };
        let quarantine = outcome
            .quarantine
            .clone()
            .or_else(|| saved.and_then(|(s, _)| s.quarantine.clone()));
        set_stream(streams, i, |v| {
            v.stats = outcome.stats.clone();
            v.quarantine = quarantine.clone();
            v.rejections
                .extend(outcome.rejections.iter().take(MAX_REJECTIONS).cloned());
        });
        let mut warnings = false;
        if outcome.stats.failed > 0 {
            warnings = true;
            let where_ = quarantine
                .as_ref()
                .map_or(String::new(), |p| format!("; saved in {}", p.display()));
            self.event(
                run.id,
                EventLevel::Warning,
                "rejected",
                Some(&key),
                &format!(
                    "{} document(s) were rejected by the target{where_}",
                    outcome.stats.failed
                ),
            )
            .await;
        }
        if outcome.stopped {
            set_stream(streams, i, |v| v.phase = StreamPhase::Stopped);
            return StreamEnd::Stopped;
        }
        set_stream(streams, i, |v| v.phase = StreamPhase::Indexing);
        let mut created = 0u32;
        for op in indexes {
            let name = match &op {
                SchemaOperation::CreateIndex { index, .. } => index.name.clone(),
                _ => String::new(),
            };
            match target
                .apply_ddl(std::slice::from_ref(&op), CancellationToken::new())
                .await
            {
                Ok(()) => created += 1,
                Err(e) => {
                    warnings = true;
                    let m = self
                        .redactor
                        .scrub(&format!("index `{name}` was not created: {e}"));
                    self.event(run.id, EventLevel::Warning, "index_failed", Some(&key), &m)
                        .await;
                }
            }
        }
        let done = Saved {
            position: outcome.position.clone(),
            stats: outcome.stats.clone(),
            indexes_created: created,
            quarantine: quarantine.clone(),
        };
        if !run.dry_run
            && let Err(e) = checkpoint(
                &self.store,
                run.id,
                &run.config_fingerprint,
                &key,
                &done,
                true,
            )
            .await
        {
            let m = fail(format!("saving the final checkpoint: {e}"));
            return StreamEnd::Failed(m);
        }
        set_stream(streams, i, |v| {
            v.indexes_created = created;
            v.phase = StreamPhase::Done;
        });
        self.event(
            run.id,
            EventLevel::Info,
            "stream_done",
            Some(&key),
            &format!(
                "{}{} → {}: {} inserted, {} updated, {} kept, {} rejected, {created} index(es)",
                if run.dry_run { "Would write: " } else { "" },
                item.source,
                item.target,
                outcome.stats.inserted,
                outcome.stats.updated,
                outcome.stats.skipped,
                outcome.stats.failed
            ),
        )
        .await;
        StreamEnd::Done { warnings }
    }

    async fn finish(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
        ends: Vec<StreamEnd>,
        streams: &Mutex<Vec<StreamView>>,
        reason: &Mutex<Option<StopReason>>,
    ) {
        let jobs = self.store.jobs();
        let reason = reason.lock().ok().and_then(|r| *r);
        let failure = ends.iter().find_map(|e| match e {
            StreamEnd::Failed(m) => Some(m.clone()),
            _ => None,
        });
        let warnings = ends
            .iter()
            .any(|e| matches!(e, StreamEnd::Done { warnings: true }));
        let current = jobs.require_run(run.id).await.map(|r| r.status);
        let path: Vec<JobStatus> = match (&failure, reason, current) {
            (Some(_), _, Ok(JobStatus::Running | JobStatus::Pausing | JobStatus::Cancelling)) => {
                vec![JobStatus::Failed]
            }
            (None, Some(StopReason::Pause), _) => vec![JobStatus::Paused],
            (None, Some(StopReason::Cancel), _) => vec![JobStatus::Cancelled],
            (None, None, _) if warnings => vec![JobStatus::CompletedWithWarnings],
            (None, None, _) => vec![JobStatus::Completed],
            _ => vec![JobStatus::Failed],
        };
        if let Some(m) = &failure {
            let _ = jobs.set_run_error_summary(run.id, m).await;
        }
        let snapshot = streams.lock().ok().map(|s| s.clone());
        if let Some(s) = snapshot {
            let _ = jobs
                .update_run_stats(run.id, &serde_json::json!({ "streams": s }))
                .await;
        }
        for status in path {
            if let Err(e) = jobs.transition_run(run.id, status).await {
                tracing::error!(run = %run.id, error = %e, "could not record the copy's final state");
            }
        }
        let status = jobs.require_run(run.id).await.map(|r| r.status);
        if run.dry_run && matches!(status, Ok(JobStatus::Completed)) {
            let _ = jobs.mark_dry_run_passed(run.job_id).await;
        }
        let summary = match &status {
            Ok(s) => format!(
                "{} of {} collection(s) ended: {s}",
                if run.dry_run { "Dry run" } else { "Copy" },
                req.items.len()
            ),
            Err(_) => "Copy ended".into(),
        };
        self.event(
            run.id,
            if failure.is_some() {
                EventLevel::Error
            } else {
                EventLevel::Info
            },
            "finished",
            None,
            &summary,
        )
        .await;
        let _ = self
            .audit(
                "migration.finish",
                run,
                summary,
                serde_json::json!({ "status": status.as_ref().ok(), "error": failure }),
            )
            .await;
        // Counts are cheap: every finished copy is checked at least this far.
        if !run.dry_run
            && matches!(
                status,
                Ok(JobStatus::Completed | JobStatus::CompletedWithWarnings)
            )
        {
            self.run_validation(run, Level::Basic).await;
        }
    }

    /// Compares a run's sources with its targets in the background; follow
    /// it with [`Self::view`].
    pub async fn validate(&self, run_id: RunId, level: Level) -> AppResult<()> {
        if self.is_active(run_id) {
            return Err(AppError::Invalid("wait until the copy has stopped".into()));
        }
        let run = self.store.jobs().require_run(run_id).await?;
        if run.dry_run {
            return Err(AppError::Invalid(
                "a dry run wrote nothing to validate".into(),
            ));
        }
        // Registered before returning, so the next view already shows it.
        {
            let mut v = self
                .validations
                .lock()
                .map_err(|_| AppError::Invalid("state lock poisoned".into()))?;
            if v.contains_key(&run_id) {
                return Err(AppError::Invalid("a validation is already running".into()));
            }
            v.insert(run_id, Self::running_report(level));
        }
        let svc = self.clone();
        tokio::spawn(async move {
            svc.run_validation(&run, level).await;
        });
        Ok(())
    }

    fn running_report(level: Level) -> ValidationReport {
        ValidationReport {
            level,
            running: true,
            passed: None,
            streams: Vec::new(),
            error: None,
            started_at: Utc::now(),
            ended_at: None,
        }
    }

    async fn run_validation(&self, run: &RunRecord, level: Level) -> ValidationReport {
        let mut report = Self::running_report(level);
        if let Ok(mut v) = self.validations.lock() {
            v.entry(run.id).or_insert_with(|| report.clone());
        }
        let result = self.compare(run, level).await;
        report = self
            .validations
            .lock()
            .ok()
            .and_then(|mut v| v.remove(&run.id))
            .unwrap_or(report);
        report.running = false;
        report.ended_at = Some(Utc::now());
        match result {
            Ok(streams) => {
                report.passed = Some(streams.iter().all(|s| s.passed));
                report.streams = streams;
            }
            Err(e) => {
                report.passed = Some(false);
                report.error = Some(self.redactor.scrub(&e.to_string()));
            }
        }
        if let Ok(json) = serde_json::to_value(&report) {
            let _ = self.store.jobs().set_run_validation(run.id, &json).await;
        }
        let level_name = serde_json::to_value(level)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        let (lvl, message) = match (&report.error, report.passed) {
            (Some(e), _) => (
                EventLevel::Error,
                format!("Validation ({level_name}) failed: {e}"),
            ),
            (None, Some(true)) => (
                EventLevel::Info,
                format!("Validation ({level_name}) passed"),
            ),
            _ => (
                EventLevel::Warning,
                format!("Validation ({level_name}) found differences"),
            ),
        };
        self.event(run.id, lvl, "validated", None, &message).await;
        report
    }

    async fn compare(&self, run: &RunRecord, level: Level) -> AppResult<Vec<StreamValidation>> {
        let def = RunDefinition::parse(&run.definition_snapshot)?;
        let req = &def.request;
        let source = self.explorer.session(req.source).await?;
        let target = self.explorer.session(req.target).await?;
        let mut out = Vec::new();
        for (i, item) in req.items.iter().enumerate() {
            let mapping = def.mappings.get(i).cloned().unwrap_or(ItemMapping::Copy);
            if matches!(mapping, ItemMapping::Embedded { .. }) {
                continue;
            }
            let (transform, how): (Arc<dyn Transform>, Comparison) =
                match mapping.transform(req.existing, source.clone()) {
                    Some(t) => (t, Comparison::Normalized),
                    None => (
                        Arc::new(Identity(TargetSpec {
                            object: item.target.clone(),
                            key_fields: vec!["_id".into()],
                            owner: None,
                            replace: false,
                        })),
                        Comparison::Exact,
                    ),
                };
            let live = self.validations.clone();
            let (run_id, slot) = (run.id, out.len());
            let on_progress = move |sv: &StreamValidation| {
                if let Ok(mut m) = live.lock()
                    && let Some(r) = m.get_mut(&run_id)
                {
                    if r.streams.len() <= slot {
                        r.streams.push(sv.clone());
                    } else {
                        r.streams[slot] = sv.clone();
                    }
                }
            };
            let v = validate_stream(
                source.clone(),
                target.clone(),
                &item.source,
                transform,
                level,
                how,
                req.batch_size.clamp(1, 5_000),
                CancellationToken::new(),
                &on_progress,
            )
            .await?;
            out.push(v);
        }
        Ok(out)
    }

    /// Live progress of a running copy, or the recorded state of an old one.
    pub async fn view(&self, run_id: RunId) -> AppResult<RunView> {
        let jobs = self.store.jobs();
        let run = jobs.require_run(run_id).await?;
        let job = jobs.require(run.job_id).await?;
        let req = RunDefinition::parse(&run.definition_snapshot)?.request;
        let live = self.active.lock().ok().and_then(|a| {
            a.get(&run_id)
                .and_then(|h| h.streams.lock().ok().map(|s| s.clone()))
        });
        let streams = match live {
            Some(s) => s,
            None => self.recorded_streams(&run, &req).await?,
        };
        let events = jobs
            .events(run_id, 500)
            .await?
            .into_iter()
            .rev()
            .take(50)
            .map(|e| RunEventView {
                at: e.at,
                level: serde_json::to_value(e.level)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default(),
                stream: e.stream,
                message: e.message,
            })
            .collect();
        Ok(RunView {
            run_id,
            name: job.name,
            status: run.status,
            source: req.source,
            target: req.target,
            existing: req.existing,
            streams,
            created_at: run.created_at,
            started_at: run.started_at,
            ended_at: run.ended_at,
            error: run.error_summary,
            events,
            dry_run: run.dry_run,
            validation: self
                .validations
                .lock()
                .ok()
                .and_then(|v| v.get(&run_id).cloned())
                .or_else(|| {
                    run.validation
                        .as_ref()
                        .and_then(|j| serde_json::from_value(j.clone()).ok())
                }),
        })
    }

    async fn recorded_streams(
        &self,
        run: &RunRecord,
        req: &MigrationRequest,
    ) -> AppResult<Vec<StreamView>> {
        // Final stats when the run ended normally; checkpoints otherwise.
        if let Some(s) = run.stats.get("streams")
            && let Ok(v) = serde_json::from_value::<Vec<StreamView>>(s.clone())
            && v.len() == req.items.len()
        {
            return Ok(v);
        }
        let cps: HashMap<String, (Saved, bool)> = self
            .store
            .jobs()
            .checkpoints(run.id)
            .await?
            .into_iter()
            .filter_map(|c| {
                Some((
                    c.stream,
                    (serde_json::from_value(c.position?).ok()?, c.completed),
                ))
            })
            .collect();
        Ok(req
            .items
            .iter()
            .map(|item| {
                let mut v = view_for(item);
                if let Some((s, done)) = cps.get(&stream_key(item)) {
                    v.stats = s.stats.clone();
                    v.indexes_created = s.indexes_created;
                    v.quarantine = s.quarantine.clone();
                    v.phase = if *done {
                        StreamPhase::Done
                    } else {
                        StreamPhase::Stopped
                    };
                }
                v
            })
            .collect())
    }

    /// Most recent copies, newest first.
    pub async fn recent(&self, limit: u32) -> AppResult<Vec<RunView>> {
        let jobs = self.store.jobs();
        let mut runs = Vec::new();
        for job in jobs.list(false).await? {
            if job.kind != JobKind::Migration {
                continue;
            }
            runs.extend(jobs.runs_for(job.id, 5).await?);
        }
        runs.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        let mut out = Vec::new();
        for run in runs.into_iter().take(limit as usize) {
            if let Ok(mut v) = self.view(run.id).await {
                v.events.clear();
                out.push(v);
            }
        }
        Ok(out)
    }

    async fn event(
        &self,
        run_id: RunId,
        level: EventLevel,
        code: &str,
        stream: Option<&str>,
        message: &str,
    ) {
        let message = self.redactor.scrub(message);
        if let Err(e) = self
            .store
            .jobs()
            .add_event(run_id, level, code, stream, &message)
            .await
        {
            tracing::warn!(run = %run_id, error = %e, "could not record a copy event");
        }
    }

    async fn audit(
        &self,
        action: &str,
        run: &RunRecord,
        summary: String,
        details: serde_json::Value,
    ) -> AppResult<()> {
        self.store
            .audit()
            .record(NewAuditEvent {
                action,
                target_kind: "job_run",
                target_id: Some(run.id.to_string()),
                summary,
                details,
                correlation_id: None,
            })
            .await?;
        Ok(())
    }
}
