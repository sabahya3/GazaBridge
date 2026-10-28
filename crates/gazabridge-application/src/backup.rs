//! Backups and restores.
//!
//! A backup is a folder: one file per collection or table plus a
//! `manifest.json` written last, so a folder without a manifest is an
//! unfinished backup and is never offered for restore. The manifest holds
//! each file's SHA-256 (checked before anything is restored), record
//! counts, and what is needed to recreate the objects: MongoDB index
//! specifications, PostgreSQL column definitions, keys and indexes.
//!
//! * GazaBridge backups need no external tool: MongoDB collections are kept
//!   as gzip BSON (exact), PostgreSQL tables as gzip typed JSON lines.
//! * Native backups run `mongodump`/`pg_dump` (see [`crate::tools`]) and keep
//!   their archive.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::ids::ConnectionId;
use gazabridge_domain::job::WriteMode;
use gazabridge_files::{
    Compression, FileFormat, JsonFlavor, MalformedPolicy, ReadOptions, WriteOptions,
};
use gazabridge_plugin_sdk::connector::{Connection, InspectOptions};
use gazabridge_plugin_sdk::ddl::{
    ColumnDefinition, ForeignKeyDefinition, IndexDefinition, SchemaOperation, TableDefinition,
};
use gazabridge_plugin_sdk::schema::{ConstraintKind, ObjectKind, ObjectRef, SamplingMethod};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::audit::NewAuditEvent;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::connections::ConnectionService;
use crate::explorer::ExplorerService;
use crate::migration::ExistingData;
use crate::tools::{self, ToolKind};
use crate::transfer::{ExportRequest, ImportRequest, TransferService};
use crate::{AppError, AppResult};

pub const MANIFEST: &str = "manifest.json";
const FORMAT: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupMethod {
    /// Files written by GazaBridge; no external tool needed.
    #[default]
    GazaBridge,
    /// `mongodump` or `pg_dump`.
    Native,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupObject {
    pub object: ObjectRef,
    /// File name inside the backup folder (GazaBridge backups).
    pub file: Option<String>,
    pub records: u64,
    pub bytes: u64,
    pub sha256: Option<String>,
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    /// PostgreSQL: what recreates the table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<TableDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolArchive {
    pub tool: String,
    pub version: Option<String>,
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
    /// The command as run, without secrets.
    pub command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub app_version: String,
    pub created_at: DateTime<Utc>,
    pub method: BackupMethod,
    pub label: String,
    pub family: EngineFamily,
    /// Connection name and redacted address at backup time.
    pub connection: String,
    pub target: String,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub objects: Vec<BackupObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive: Option<ToolArchive>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl Manifest {
    pub fn records(&self) -> u64 {
        self.objects.iter().map(|o| o.records).sum()
    }

    pub fn bytes(&self) -> u64 {
        self.archive
            .as_ref()
            .map_or_else(|| self.objects.iter().map(|o| o.bytes).sum(), |a| a.bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupRequest {
    pub connection: ConnectionId,
    pub objects: Vec<ObjectRef>,
    #[serde(default)]
    pub method: BackupMethod,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreRequest {
    /// Backup folder.
    pub backup: PathBuf,
    pub connection: ConnectionId,
    /// New database (MongoDB) or schema (PostgreSQL); empty keeps the original.
    #[serde(default)]
    pub rename_to: Option<String>,
    /// Restore only these objects (as named in the backup); empty: all.
    #[serde(default)]
    pub only: Vec<ObjectRef>,
    #[serde(default)]
    pub existing: ExistingData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreItem {
    pub from: ObjectRef,
    pub to: ObjectRef,
    pub records: u64,
    pub target_exists: bool,
    pub target_count: Option<u64>,
    /// The file's checksum matches the manifest.
    pub verified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestorePlan {
    pub items: Vec<RestoreItem>,
    pub statements: Vec<String>,
    /// Native restores: the command, without secrets.
    pub command: Option<String>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub destructive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpPhase {
    Running,
    Done,
    Failed,
    Cancelled,
}

/// Progress of a backup or restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupProgress {
    pub phase: OpPhase,
    pub step: String,
    pub objects_done: usize,
    pub objects_total: usize,
    pub records: u64,
    /// Lines the native tool printed (last ones).
    pub log: Vec<String>,
    pub error: Option<String>,
    /// The backup folder (backups) when done.
    pub path: Option<PathBuf>,
}

impl BackupProgress {
    fn running(total: usize) -> Self {
        Self {
            phase: OpPhase::Running,
            step: String::new(),
            objects_done: 0,
            objects_total: total,
            records: 0,
            log: Vec::new(),
            error: None,
            path: None,
        }
    }
}

struct Op {
    progress: BackupProgress,
    cancel: CancellationToken,
}

#[derive(Clone)]
pub struct BackupService {
    store: MetadataStore,
    connections: ConnectionService,
    explorer: ExplorerService,
    transfers: TransferService,
    root: PathBuf,
    ops: Arc<Mutex<HashMap<String, Op>>>,
}

impl std::fmt::Debug for BackupService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupService")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

fn io(path: &Path) -> impl Fn(std::io::Error) -> AppError + '_ {
    move |e| AppError::Invalid(format!("{}: {e}", path.display()))
}

fn safe_name(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    out.trim_matches('.').to_owned()
}

fn family(conn: &dyn Connection) -> EngineFamily {
    conn.manifest().family
}

/// SHA-256 of a file, computed off the async runtime.
pub async fn file_sha256(path: PathBuf) -> AppResult<String> {
    tokio::task::spawn_blocking(move || -> AppResult<String> {
        use std::io::Read;
        let mut f = std::fs::File::open(&path).map_err(io(&path))?;
        let mut h = Sha256::new();
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = f.read(&mut buf).map_err(io(&path))?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
    })
    .await
    .map_err(|e| AppError::Invalid(format!("checksum task failed: {e}")))?
}

/// Tables ordered so every table comes after the tables it references.
fn by_dependencies(objects: Vec<BackupObject>) -> Vec<BackupObject> {
    let mut left = objects;
    let mut out: Vec<BackupObject> = Vec::with_capacity(left.len());
    while !left.is_empty() {
        let placed: BTreeSet<String> = out.iter().map(|o| o.object.name.clone()).collect();
        let names: BTreeSet<String> = left.iter().map(|o| o.object.name.clone()).collect();
        let ready = left.iter().position(|o| {
            o.table.as_ref().is_none_or(|t| {
                t.foreign_keys.iter().all(|fk| {
                    fk.referenced.name == o.object.name
                        || placed.contains(&fk.referenced.name)
                        || !names.contains(&fk.referenced.name)
                })
            })
        });
        // A reference cycle: keep the remaining order.
        let i = ready.unwrap_or(0);
        out.push(left.remove(i));
    }
    out
}

fn renamed(o: &ObjectRef, family: EngineFamily, to: Option<&str>) -> ObjectRef {
    let mut r = o.clone();
    if let Some(to) = to.filter(|t| !t.trim().is_empty()) {
        match family {
            EngineFamily::Postgres => r.namespace.schema = Some(to.trim().to_owned()),
            _ => r.namespace.database = Some(to.trim().to_owned()),
        }
    }
    r
}

impl BackupService {
    pub fn new(
        store: MetadataStore,
        connections: ConnectionService,
        explorer: ExplorerService,
        transfers: TransferService,
        root: PathBuf,
    ) -> Self {
        Self {
            store,
            connections,
            explorer,
            transfers,
            root,
            ops: Arc::default(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    // ------------------------------------------------------------ listing

    /// Finished backups (folders with a manifest), newest first.
    pub async fn list(&self) -> AppResult<Vec<(PathBuf, Manifest)>> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            let Ok(entries) = std::fs::read_dir(&root) else {
                return out;
            };
            for e in entries.flatten() {
                let dir = e.path();
                let Ok(text) = std::fs::read_to_string(dir.join(MANIFEST)) else {
                    continue;
                };
                if let Ok(m) = serde_json::from_str::<Manifest>(&text) {
                    out.push((dir, m));
                }
            }
            out.sort_by_key(|b| std::cmp::Reverse(b.1.created_at));
            out
        })
        .await
        .map_err(|e| AppError::Invalid(format!("listing backups failed: {e}")))
    }

    pub async fn manifest(&self, dir: &Path) -> AppResult<Manifest> {
        let text = tokio::fs::read_to_string(dir.join(MANIFEST))
            .await
            .map_err(|_| {
                AppError::Invalid(format!("{} is not a finished backup", dir.display()))
            })?;
        serde_json::from_str(&text)
            .map_err(|e| AppError::Invalid(format!("unreadable manifest: {e}")))
    }

    /// Deletes a backup folder. Only folders inside the backup root that
    /// hold a manifest are deleted.
    pub async fn delete(&self, dir: &Path) -> AppResult<()> {
        let dir = dir.canonicalize().map_err(io(dir))?;
        let root = self.root.canonicalize().map_err(io(&self.root))?;
        if !dir.starts_with(&root) || dir == root {
            return Err(AppError::Invalid(
                "only backups made by GazaBridge can be deleted here".into(),
            ));
        }
        self.manifest(&dir).await?;
        tokio::fs::remove_dir_all(&dir).await.map_err(io(&dir))?;
        self.audit(
            "backup.delete",
            "Deleted a backup",
            serde_json::json!({ "path": dir }),
        )
        .await
    }

    // ------------------------------------------------------------ backups

    /// Backs up `objects` and returns the new backup folder. Used directly
    /// (e.g. before a migration drops a target) and by [`Self::start_backup`].
    pub async fn backup(
        &self,
        req: &BackupRequest,
        cancel: CancellationToken,
        on_progress: &(dyn Fn(&BackupProgress) + Send + Sync),
    ) -> AppResult<PathBuf> {
        if req.objects.is_empty() {
            return Err(AppError::Invalid(
                "choose at least one collection or table".into(),
            ));
        }
        let profile = self.store.connections().require(req.connection).await?;
        let conn = self.explorer.session(req.connection).await?;
        let fam = family(conn.as_ref());
        if !matches!(fam, EngineFamily::MongoDb | EngineFamily::Postgres) {
            return Err(AppError::Invalid(
                "backups support MongoDB and PostgreSQL".into(),
            ));
        }
        let first = &req.objects[0];
        let label = req
            .label
            .clone()
            .filter(|l| !l.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "{} {}",
                    profile.name,
                    first
                        .namespace
                        .database
                        .clone()
                        .or(first.namespace.schema.clone())
                        .unwrap_or_default()
                )
            });
        let stamp = Utc::now();
        tokio::fs::create_dir_all(&self.root)
            .await
            .map_err(io(&self.root))?;
        // A folder of its own, never shared: cleaning up a failed backup
        // must not touch another one.
        let base = format!("{}-{}", stamp.format("%Y%m%d-%H%M%S"), safe_name(&label));
        let mut dir = self.root.join(&base);
        let mut n = 2;
        loop {
            match tokio::fs::create_dir(&dir).await {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && n < 1_000 => {
                    dir = self.root.join(format!("{base}-{n}"));
                    n += 1;
                }
                Err(e) => return Err(io(&dir)(e)),
            }
        }
        let result = self
            .backup_into(
                req,
                &dir,
                &label,
                stamp,
                fam,
                &profile.name,
                conn,
                cancel,
                on_progress,
            )
            .await;
        match result {
            Ok(()) => {
                self.audit(
                    "backup.create",
                    &format!(
                        "Backed up {} object(s) of {}",
                        req.objects.len(),
                        profile.name
                    ),
                    serde_json::json!({ "path": dir, "method": req.method }),
                )
                .await?;
                Ok(dir)
            }
            Err(e) => {
                // No manifest means unfinished: remove the partial folder.
                let _ = tokio::fs::remove_dir_all(&dir).await;
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)] // one backup's context
    async fn backup_into(
        &self,
        req: &BackupRequest,
        dir: &Path,
        label: &str,
        stamp: DateTime<Utc>,
        fam: EngineFamily,
        connection_name: &str,
        conn: Arc<dyn Connection>,
        cancel: CancellationToken,
        on_progress: &(dyn Fn(&BackupProgress) + Send + Sync),
    ) -> AppResult<()> {
        let profile = self.store.connections().require(req.connection).await?;
        let mut manifest = Manifest {
            format: FORMAT,
            app_version: env!("CARGO_PKG_VERSION").into(),
            created_at: stamp,
            method: req.method,
            label: label.to_owned(),
            family: fam,
            connection: connection_name.to_owned(),
            target: self
                .connections
                .redactor()
                .scrub(&profile.settings.display_target()),
            database: req.objects[0].namespace.database.clone(),
            schema: req.objects[0].namespace.schema.clone(),
            objects: Vec::new(),
            archive: None,
            warnings: Vec::new(),
        };
        let mut progress = BackupProgress::running(req.objects.len());
        let inspect = InspectOptions {
            include_indexes: true,
            include_constraints: true,
            sample_size: 1,
            sampling: SamplingMethod::First,
        };
        // Definitions first: they are needed whatever the method.
        for o in &req.objects {
            let schema = conn.inspect(o, &inspect).await?;
            let mut item = BackupObject {
                object: o.clone(),
                file: None,
                records: 0,
                bytes: 0,
                sha256: None,
                indexes: schema
                    .indexes
                    .iter()
                    .filter(|i| !i.primary)
                    .map(|i| IndexDefinition {
                        name: i.name.clone(),
                        columns: i.columns.clone(),
                        unique: i.unique,
                        native: i.native.clone(),
                    })
                    .collect(),
                table: None,
            };
            if fam == EngineFamily::Postgres {
                let mut dropped_defaults = false;
                item.table = Some(TableDefinition {
                    object: o.clone(),
                    columns: schema
                        .columns
                        .iter()
                        .map(|c| {
                            // Sequences are not part of the backup.
                            let default = c.default_expression.clone().filter(|d| {
                                let keep = !d.contains("nextval(");
                                dropped_defaults |= !keep;
                                keep
                            });
                            ColumnDefinition {
                                name: c.name.clone(),
                                canonical_type: c.canonical_type.clone(),
                                native_type: Some(c.native_type.clone()),
                                nullable: c.nullable,
                                default_expression: default,
                            }
                        })
                        .collect(),
                    primary_key: schema.primary_key.clone(),
                    foreign_keys: schema
                        .constraints
                        .iter()
                        .filter_map(|c| match &c.kind {
                            ConstraintKind::ForeignKey {
                                referenced,
                                referenced_columns,
                                ..
                            } => Some(ForeignKeyDefinition {
                                name: c.name.clone(),
                                columns: c.columns.clone(),
                                referenced: referenced.clone(),
                                referenced_columns: referenced_columns.clone(),
                            }),
                            _ => None,
                        })
                        .collect(),
                });
                if dropped_defaults {
                    manifest.warnings.push(format!(
                        "`{}`: sequence defaults are not recreated; stored values are restored as they are",
                        o.name
                    ));
                }
            }
            manifest.objects.push(item);
        }
        match req.method {
            BackupMethod::GazaBridge => {
                for (i, item) in manifest.objects.iter_mut().enumerate() {
                    if cancel.is_cancelled() {
                        return Err(AppError::Connector(
                            gazabridge_plugin_sdk::ConnectorError::Cancelled,
                        ));
                    }
                    progress.step = item.object.to_string();
                    on_progress(&progress);
                    let (format, flavor, ext) = match fam {
                        EngineFamily::MongoDb => (FileFormat::Bson, JsonFlavor::Typed, "bson.gz"),
                        _ => (FileFormat::Jsonl, JsonFlavor::Typed, "jsonl.gz"),
                    };
                    let file =
                        format!("{:03}-{}.{ext}", i + 1, safe_name(&item.object.to_string()));
                    let mut options = WriteOptions::new(format);
                    options.json_flavor = flavor;
                    options.compression = Compression::Gzip;
                    let summary = self
                        .transfers
                        .export(
                            req.connection,
                            ExportRequest {
                                object: item.object.clone(),
                                filter: None,
                                projection: vec![],
                                exclude: vec![],
                                order_by: vec![],
                                skip: 0,
                                limit: None,
                                path: dir.join(&file),
                                options,
                            },
                            cancel.clone(),
                            |_| {},
                        )
                        .await?;
                    let written = summary
                        .file
                        .ok_or_else(|| AppError::Invalid("the export wrote no file".into()))?;
                    item.file = Some(file);
                    item.records = written.records;
                    item.bytes = written.bytes;
                    item.sha256 = Some(written.sha256);
                    progress.objects_done = i + 1;
                    progress.records += written.records;
                    on_progress(&progress);
                }
            }
            BackupMethod::Native => {
                let kind = match fam {
                    EngineFamily::MongoDb => ToolKind::MongoDump,
                    _ => ToolKind::PgDump,
                };
                let info = tools::detect(kind).await;
                let path = info.path.clone().ok_or_else(|| {
                    AppError::Invalid(format!(
                        "{} was not found. {}",
                        kind.program(),
                        kind.install_hint()
                    ))
                })?;
                let secrets = self.connections.resolve_secrets(&profile).await?;
                let file = match fam {
                    EngineFamily::MongoDb => "archive.gz",
                    _ => "dump.pgcustom",
                };
                let all: Vec<String> = match fam {
                    EngineFamily::MongoDb => conn
                        .list(&req.objects[0].namespace)
                        .await
                        .map(|l| {
                            l.into_iter()
                                .filter(|e| e.kind == ObjectKind::Collection)
                                .map(|e| e.name)
                                .collect()
                        })
                        .unwrap_or_default(),
                    _ => Vec::new(),
                };
                let inv = tools::dump(
                    kind,
                    &path,
                    &profile,
                    &secrets,
                    &req.objects,
                    &all,
                    &dir.join(file),
                )?;
                progress.step = inv.preview.clone();
                on_progress(&progress);
                let log = Mutex::new(Vec::<String>::new());
                let outcome = tools::run(inv, cancel.clone(), &|line: &str| {
                    if let Ok(mut l) = log.lock() {
                        l.push(line.to_owned());
                        let len = l.len();
                        if len > 200 {
                            l.drain(..len - 200);
                        }
                    }
                })
                .await?;
                progress.log = log.lock().map(|l| l.clone()).unwrap_or_default();
                if !outcome.success {
                    return Err(AppError::Invalid(format!(
                        "{} failed (exit {}): {}",
                        kind.program(),
                        outcome.code.map_or("signal".into(), |c| c.to_string()),
                        progress
                            .log
                            .iter()
                            .rev()
                            .take(5)
                            .rev()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(" | ")
                    )));
                }
                let archive = dir.join(file);
                let bytes = tokio::fs::metadata(&archive)
                    .await
                    .map_err(io(&archive))?
                    .len();
                manifest.archive = Some(ToolArchive {
                    tool: kind.program().into(),
                    version: info.version,
                    file: file.into(),
                    bytes,
                    sha256: file_sha256(archive).await?,
                    command: outcome.preview,
                });
                for item in &mut manifest.objects {
                    item.records = conn
                        .count(&item.object, None, false)
                        .await
                        .map(|c| c.count)
                        .unwrap_or(0);
                }
                progress.objects_done = manifest.objects.len();
                on_progress(&progress);
            }
        }
        // The manifest goes last: its presence marks a finished backup.
        let tmp = dir.join(format!("{MANIFEST}.partial"));
        let json =
            serde_json::to_vec_pretty(&manifest).map_err(|e| AppError::Invalid(e.to_string()))?;
        tokio::fs::write(&tmp, json).await.map_err(io(&tmp))?;
        tokio::fs::rename(&tmp, dir.join(MANIFEST))
            .await
            .map_err(io(dir))?;
        Ok(())
    }

    // ----------------------------------------------------------- restores

    /// Checks a backup against a target: checksums, object names, existing
    /// data, and the statements or command that would run. Writes nothing.
    pub async fn plan_restore(&self, req: &RestoreRequest) -> AppResult<(RestorePlan, Manifest)> {
        let manifest = self.manifest(&req.backup).await?;
        let conn = self.explorer.session(req.connection).await?;
        let mut plan = RestorePlan {
            items: Vec::new(),
            statements: Vec::new(),
            command: None,
            blockers: Vec::new(),
            warnings: manifest.warnings.clone(),
            destructive: false,
        };
        if family(conn.as_ref()) != manifest.family {
            plan.blockers.push(format!(
                "this backup is from {:?}; restore it into a {:?} connection",
                manifest.family, manifest.family
            ));
            return Ok((plan, manifest));
        }
        if conn.is_read_only() {
            plan.blockers
                .push("the target connection is read-only; edit it to allow writes".into());
        }
        let rename = req.rename_to.as_deref();
        let wanted: Vec<&BackupObject> = manifest
            .objects
            .iter()
            .filter(|o| req.only.is_empty() || req.only.contains(&o.object))
            .collect();
        if wanted.is_empty() {
            plan.blockers.push("nothing selected to restore".into());
        }
        if let Some(a) = &manifest.archive {
            let ok = file_sha256(req.backup.join(&a.file)).await.ok().as_deref()
                == Some(a.sha256.as_str());
            if !ok {
                plan.blockers.push(format!(
                    "`{}` does not match its checksum: the backup is damaged or was changed",
                    a.file
                ));
            }
        }
        let mut existing_names: HashMap<String, BTreeSet<String>> = HashMap::new();
        for o in &wanted {
            let to = renamed(&o.object, manifest.family, rename);
            let key = format!("{:?}", to.namespace);
            if !existing_names.contains_key(&key) {
                let names = conn
                    .list(&to.namespace)
                    .await
                    .map(|l| l.into_iter().map(|e| e.name).collect())
                    .unwrap_or_default();
                existing_names.insert(key.clone(), names);
            }
            let exists = existing_names[&key].contains(&to.name);
            let verified = match (&o.file, &o.sha256) {
                (Some(f), Some(sum)) => {
                    file_sha256(req.backup.join(f)).await.ok().as_deref() == Some(sum.as_str())
                }
                _ => manifest.archive.is_some(),
            };
            if !verified {
                plan.blockers.push(format!(
                    "the file for `{}` does not match its checksum",
                    o.object
                ));
            }
            let target_count = if exists {
                conn.count(&to, None, false).await.ok().map(|c| c.count)
            } else {
                None
            };
            if exists && req.existing == ExistingData::Replace {
                plan.destructive = true;
                plan.warnings
                    .push(format!("`{to}` and its data will be dropped first"));
            } else if exists && target_count.unwrap_or(0) > 0 {
                plan.warnings.push(format!(
                    "`{to}` already has data; {}",
                    match req.existing {
                        ExistingData::Skip => "records with the same key are kept",
                        ExistingData::Update => "records with the same key are replaced",
                        _ => "the restore stops at the first key that exists",
                    }
                ));
            }
            plan.items.push(RestoreItem {
                from: o.object.clone(),
                to,
                records: o.records,
                target_exists: exists,
                target_count,
                verified,
            });
        }
        match manifest.method {
            BackupMethod::GazaBridge => {
                let ops = self.restore_ops(&manifest, &wanted, req);
                match conn.plan_ddl(&ops) {
                    Ok(d) => plan.statements = d.statements,
                    Err(e) => plan.blockers.push(e.to_string()),
                }
            }
            BackupMethod::Native => {
                let kind = match manifest.family {
                    EngineFamily::MongoDb => ToolKind::MongoRestore,
                    _ => ToolKind::PgRestore,
                };
                if req.existing == ExistingData::Update {
                    plan.blockers.push(format!(
                        "{} cannot replace single records; choose keep, stop, or delete first",
                        kind.program()
                    ));
                }
                match tools::detect(kind).await.path {
                    None => plan.blockers.push(format!(
                        "{} was not found. {}",
                        kind.program(),
                        kind.install_hint()
                    )),
                    Some(path) => {
                        let profile = self.store.connections().require(req.connection).await?;
                        let inv = tools::restore(
                            kind,
                            &path,
                            &profile,
                            &Default::default(),
                            &manifest,
                            &req.backup,
                            rename,
                            &req.only,
                            req.existing == ExistingData::Replace,
                        )?;
                        plan.command = Some(inv.preview.clone());
                    }
                }
            }
        }
        Ok((plan, manifest))
    }

    fn restore_ops(
        &self,
        manifest: &Manifest,
        wanted: &[&BackupObject],
        req: &RestoreRequest,
    ) -> Vec<SchemaOperation> {
        let rename = req.rename_to.as_deref();
        let ordered = by_dependencies(wanted.iter().map(|o| (*o).clone()).collect());
        let mut ops = Vec::new();
        if req.existing == ExistingData::Replace {
            for o in ordered.iter().rev() {
                ops.push(SchemaOperation::DropObject {
                    object: renamed(&o.object, manifest.family, rename),
                    if_exists: true,
                });
            }
        }
        if manifest.family == EngineFamily::Postgres
            && let Some(schema) = rename.filter(|s| !s.trim().is_empty() && *s != "public")
            && let Some(first) = ordered.first()
        {
            let mut ns = first.object.namespace.clone();
            ns.schema = Some(schema.trim().to_owned());
            ops.push(SchemaOperation::CreateNamespace {
                namespace: ns,
                if_not_exists: true,
            });
        }
        for o in &ordered {
            let to = renamed(&o.object, manifest.family, rename);
            match &o.table {
                Some(t) => {
                    let mut t = t.clone();
                    t.object = to.clone();
                    // References inside the backup follow the rename.
                    for fk in &mut t.foreign_keys {
                        if fk.referenced.namespace.schema == o.object.namespace.schema {
                            fk.referenced = renamed(&fk.referenced, manifest.family, rename);
                        }
                    }
                    ops.push(SchemaOperation::CreateTable {
                        table: t,
                        if_not_exists: true,
                    });
                }
                None => ops.push(SchemaOperation::CreateCollection {
                    object: to,
                    if_not_exists: true,
                    options: None,
                }),
            }
        }
        ops
    }

    /// Restores a backup (after checking it again) and returns the restored
    /// record count.
    pub async fn restore(
        &self,
        req: &RestoreRequest,
        confirm_destructive: bool,
        cancel: CancellationToken,
        on_progress: &(dyn Fn(&BackupProgress) + Send + Sync),
    ) -> AppResult<u64> {
        let (plan, manifest) = self.plan_restore(req).await?;
        if let Some(b) = plan.blockers.first() {
            return Err(AppError::Invalid(b.clone()));
        }
        if plan.destructive && !confirm_destructive {
            return Err(AppError::ConfirmationRequired("drop_target".into()));
        }
        let wanted: Vec<&BackupObject> = manifest
            .objects
            .iter()
            .filter(|o| req.only.is_empty() || req.only.contains(&o.object))
            .collect();
        let mut progress = BackupProgress::running(wanted.len());
        let restored = match manifest.method {
            BackupMethod::GazaBridge => {
                self.restore_files(req, &manifest, &wanted, cancel, &mut progress, on_progress)
                    .await?
            }
            BackupMethod::Native => {
                self.restore_native(req, &manifest, cancel, &mut progress, on_progress)
                    .await?
            }
        };
        self.audit(
            "backup.restore",
            &format!("Restored {} object(s) from {}", wanted.len(), manifest.label),
            serde_json::json!({ "path": req.backup, "existing": req.existing, "records": restored }),
        )
        .await?;
        Ok(restored)
    }

    async fn restore_files(
        &self,
        req: &RestoreRequest,
        manifest: &Manifest,
        wanted: &[&BackupObject],
        cancel: CancellationToken,
        progress: &mut BackupProgress,
        on_progress: &(dyn Fn(&BackupProgress) + Send + Sync),
    ) -> AppResult<u64> {
        let conn = self.explorer.session(req.connection).await?;
        let rename = req.rename_to.as_deref();
        progress.step = "preparing".into();
        on_progress(progress);
        let ops = self.restore_ops(manifest, wanted, req);
        conn.apply_ddl(&ops, CancellationToken::new()).await?;
        let mut restored = 0;
        let ordered = by_dependencies(wanted.iter().map(|o| (*o).clone()).collect());
        for (i, o) in ordered.iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(AppError::Connector(
                    gazabridge_plugin_sdk::ConnectorError::Cancelled,
                ));
            }
            let to = renamed(&o.object, manifest.family, rename);
            progress.step = to.to_string();
            on_progress(progress);
            let key_fields = match &o.table {
                Some(t) => t.primary_key.clone(),
                None => vec!["_id".into()],
            };
            let mode = match (req.existing, key_fields.is_empty()) {
                (_, true) => WriteMode::Insert,
                (ExistingData::Skip, _) => WriteMode::SkipExisting,
                (ExistingData::Update, _) => WriteMode::Upsert,
                _ => WriteMode::Insert,
            };
            let format = match manifest.family {
                EngineFamily::MongoDb => FileFormat::Bson,
                _ => FileFormat::Jsonl,
            };
            let Some(file) = &o.file else { continue };
            let summary = self
                .transfers
                .import(
                    req.connection,
                    ImportRequest {
                        object: to.clone(),
                        path: req.backup.join(file),
                        options: ReadOptions {
                            format,
                            json_flavor: JsonFlavor::Typed,
                            compression: Compression::Gzip,
                            malformed: MalformedPolicy::Fail,
                        },
                        mode,
                        key_fields,
                        batch_size: 1_000,
                        stop_on_error: true,
                    },
                    cancel.clone(),
                    |_| {},
                )
                .await?;
            if summary.failed > 0 {
                return Err(AppError::Invalid(format!(
                    "`{to}`: {} record(s) were rejected: {}",
                    summary.failed,
                    summary.messages.first().cloned().unwrap_or_default()
                )));
            }
            restored += summary.inserted + summary.updated + summary.skipped;
            for ix in &o.indexes {
                let op = SchemaOperation::CreateIndex {
                    object: to.clone(),
                    index: ix.clone(),
                    if_not_exists: true,
                };
                if let Err(e) = conn
                    .apply_ddl(std::slice::from_ref(&op), CancellationToken::new())
                    .await
                {
                    progress
                        .log
                        .push(format!("index `{}` on {to} was not created: {e}", ix.name));
                }
            }
            progress.objects_done = i + 1;
            progress.records = restored;
            on_progress(progress);
        }
        Ok(restored)
    }

    async fn restore_native(
        &self,
        req: &RestoreRequest,
        manifest: &Manifest,
        cancel: CancellationToken,
        progress: &mut BackupProgress,
        on_progress: &(dyn Fn(&BackupProgress) + Send + Sync),
    ) -> AppResult<u64> {
        let kind = match manifest.family {
            EngineFamily::MongoDb => ToolKind::MongoRestore,
            _ => ToolKind::PgRestore,
        };
        let path = tools::detect(kind).await.path.ok_or_else(|| {
            AppError::Invalid(format!(
                "{} was not found. {}",
                kind.program(),
                kind.install_hint()
            ))
        })?;
        let profile = self.store.connections().require(req.connection).await?;
        let secrets = self.connections.resolve_secrets(&profile).await?;
        let inv = tools::restore(
            kind,
            &path,
            &profile,
            &secrets,
            manifest,
            &req.backup,
            req.rename_to.as_deref(),
            &req.only,
            req.existing == ExistingData::Replace,
        )?;
        progress.step = inv.preview.clone();
        on_progress(progress);
        let log = Mutex::new(Vec::<String>::new());
        let outcome = tools::run(inv, cancel, &|line: &str| {
            if let Ok(mut l) = log.lock() {
                l.push(line.to_owned());
            }
        })
        .await?;
        progress.log = log
            .lock()
            .map(|l| l.iter().rev().take(200).rev().cloned().collect())
            .unwrap_or_default();
        if !outcome.success {
            return Err(AppError::Invalid(format!(
                "{} failed (exit {}): {}",
                kind.program(),
                outcome.code.map_or("signal".into(), |c| c.to_string()),
                progress
                    .log
                    .iter()
                    .rev()
                    .take(5)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" | ")
            )));
        }
        progress.objects_done = progress.objects_total;
        on_progress(progress);
        Ok(manifest.records())
    }

    // ------------------------------------------------- background operations

    fn begin(&self, op_id: &str, total: usize) -> AppResult<CancellationToken> {
        let mut ops = self
            .ops
            .lock()
            .map_err(|_| AppError::Invalid("state lock poisoned".into()))?;
        if ops
            .get(op_id)
            .is_some_and(|o| o.progress.phase == OpPhase::Running)
        {
            return Err(AppError::Invalid(format!(
                "operation `{op_id}` is already running"
            )));
        }
        let cancel = CancellationToken::new();
        ops.insert(
            op_id.to_owned(),
            Op {
                progress: BackupProgress::running(total),
                cancel: cancel.clone(),
            },
        );
        Ok(cancel)
    }

    fn set(ops: &Mutex<HashMap<String, Op>>, op_id: &str, p: &BackupProgress) {
        if let Ok(mut ops) = ops.lock()
            && let Some(op) = ops.get_mut(op_id)
        {
            op.progress = p.clone();
        }
    }

    fn end(&self, op_id: &str, result: Result<Option<PathBuf>, AppError>) {
        let redactor = self.connections.redactor().clone();
        if let Ok(mut ops) = self.ops.lock()
            && let Some(op) = ops.get_mut(op_id)
        {
            let p = &mut op.progress;
            match result {
                Ok(path) => {
                    p.phase = OpPhase::Done;
                    p.path = path;
                }
                Err(e) => {
                    p.phase = if e.code() == "cancelled" {
                        OpPhase::Cancelled
                    } else {
                        OpPhase::Failed
                    };
                    p.error = Some(redactor.scrub(&e.to_string()));
                }
            }
        }
    }

    pub fn start_backup(&self, op_id: &str, req: BackupRequest) -> AppResult<()> {
        let cancel = self.begin(op_id, req.objects.len())?;
        let (svc, op) = (self.clone(), op_id.to_owned());
        tokio::spawn(async move {
            let ops = svc.ops.clone();
            let id = op.clone();
            let r = svc
                .backup(&req, cancel, &move |p| Self::set(&ops, &id, p))
                .await;
            svc.end(&op, r.map(Some));
        });
        Ok(())
    }

    pub fn start_restore(&self, op_id: &str, req: RestoreRequest, confirm: bool) -> AppResult<()> {
        let cancel = self.begin(op_id, req.only.len())?;
        let (svc, op) = (self.clone(), op_id.to_owned());
        tokio::spawn(async move {
            let ops = svc.ops.clone();
            let id = op.clone();
            let r = svc
                .restore(&req, confirm, cancel, &move |p| Self::set(&ops, &id, p))
                .await;
            svc.end(&op, r.map(|_| None));
        });
        Ok(())
    }

    pub fn status(&self, op_id: &str) -> Option<BackupProgress> {
        self.ops.lock().ok()?.get(op_id).map(|o| o.progress.clone())
    }

    pub fn cancel(&self, op_id: &str) -> bool {
        self.ops
            .lock()
            .ok()
            .and_then(|o| o.get(op_id).map(|op| op.cancel.cancel()))
            .is_some()
    }

    async fn audit(
        &self,
        action: &str,
        summary: &str,
        details: serde_json::Value,
    ) -> AppResult<()> {
        self.store
            .audit()
            .record(NewAuditEvent {
                action,
                target_kind: "backup",
                target_id: None,
                summary: summary.to_owned(),
                details,
                correlation_id: None,
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use gazabridge_domain::CanonicalType;
    use gazabridge_plugin_sdk::schema::NamespacePath;

    use super::*;

    fn table(name: &str, refs: &[&str]) -> BackupObject {
        let o = ObjectRef::table(NamespacePath::schema(None, "public"), name);
        BackupObject {
            object: o.clone(),
            file: None,
            records: 0,
            bytes: 0,
            sha256: None,
            indexes: vec![],
            table: Some(TableDefinition {
                object: o,
                columns: vec![ColumnDefinition {
                    name: "id".into(),
                    canonical_type: CanonicalType::I64,
                    native_type: None,
                    nullable: false,
                    default_expression: None,
                }],
                primary_key: vec!["id".into()],
                foreign_keys: refs
                    .iter()
                    .map(|r| ForeignKeyDefinition {
                        name: None,
                        columns: vec!["x".into()],
                        referenced: ObjectRef::table(NamespacePath::schema(None, "public"), *r),
                        referenced_columns: vec!["id".into()],
                    })
                    .collect(),
            }),
        }
    }

    #[test]
    fn tables_restore_after_the_tables_they_reference() {
        let ordered = by_dependencies(vec![
            table("lines", &["orders", "items"]),
            table("orders", &["customers"]),
            table("customers", &[]),
            table("items", &["items"]),
        ]);
        let names: Vec<&str> = ordered.iter().map(|o| o.object.name.as_str()).collect();
        assert_eq!(names, ["customers", "orders", "items", "lines"]);
    }

    #[test]
    fn names_are_safe_and_renames_follow_the_engine() {
        assert_eq!(safe_name("shop.orders/../x"), "shop.orders_.._x");
        let o = ObjectRef::collection("shop", "orders");
        assert_eq!(
            renamed(&o, EngineFamily::MongoDb, Some("restored"))
                .namespace
                .database
                .as_deref(),
            Some("restored")
        );
        let t = ObjectRef::table(NamespacePath::schema(Some("db".into()), "public"), "t");
        assert_eq!(
            renamed(&t, EngineFamily::Postgres, Some("old"))
                .namespace
                .schema
                .as_deref(),
            Some("old")
        );
        assert_eq!(renamed(&t, EngineFamily::Postgres, Some("  ")), t);
    }
}
