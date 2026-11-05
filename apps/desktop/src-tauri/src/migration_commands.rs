//! Copying collections between connections.

use gazabridge_application::ValidationReport;
use gazabridge_application::{
    CopyItem, ExistingData, MigrationPlan, MigrationRequest, OnRecordError, RunView, StreamView,
    SyncKind, SyncSettings,
};
use gazabridge_domain::ids::RunId;
use gazabridge_ipc::{
    CommandError, CopyItemDto, DifferenceDto, MigrationArgs, MigrationPlanDto, MonitorDto,
    ObjectAddr, PlanItemDto, ReportArgs, RunArgs, RunDto, RunEventDto, RunMonitorDto, StreamDto,
    StreamMonitorDto, StreamValidationDto, SyncArgs, TablePreviewDto, TargetCheckDto, ValidateArgs,
    ValidationDto,
};
use gazabridge_mapping::doc_to_table::DocMode;
use gazabridge_mapping::table_to_doc::TableMode;
use gazabridge_migration::validate::Level;
use gazabridge_plugin_sdk::schema::ObjectRef;
use tauri::State;

use crate::connection_commands::{app_error, parse_id};
use crate::explorer_commands::object_ref;
use crate::state::AppState;

fn invalid(message: impl Into<String>) -> CommandError {
    CommandError {
        connection_id: None,
        code: "invalid_input".into(),
        message: message.into(),
    }
}

fn as_str<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn addr(o: &ObjectRef) -> ObjectAddr {
    ObjectAddr {
        database: o.namespace.database.clone(),
        schema: o.namespace.schema.clone(),
        name: o.name.clone(),
        kind: as_str(&o.kind),
    }
}

fn run_id(s: &str) -> Result<RunId, CommandError> {
    s.parse().map_err(|_| invalid("invalid run id"))
}

pub fn request(args: &MigrationArgs) -> Result<MigrationRequest, CommandError> {
    let existing = match args.existing.as_str() {
        "" | "skip" => ExistingData::Skip,
        "update" => ExistingData::Update,
        "fail" => ExistingData::Fail,
        "replace" => ExistingData::Replace,
        other => {
            return Err(invalid(format!(
                "unknown choice for existing data `{other}`"
            )));
        }
    };
    let doc_mode = match args.doc_mode.as_str() {
        "" | "flatten" => DocMode::Flatten,
        "normalize" => DocMode::Normalize,
        "jsonb" => DocMode::Jsonb,
        "hybrid" => DocMode::Hybrid,
        other => return Err(invalid(format!("unknown document mapping `{other}`"))),
    };
    let table_mode = match args.table_mode.as_str() {
        "" | "reference" => TableMode::Reference,
        "embed" => TableMode::Embed,
        other => return Err(invalid(format!("unknown table mapping `{other}`"))),
    };
    let on_record_error = match args.on_record_error.as_str() {
        "" | "stop" => OnRecordError::Stop,
        "quarantine" => OnRecordError::Quarantine,
        other => {
            return Err(invalid(format!(
                "unknown choice for rejected documents `{other}`"
            )));
        }
    };
    Ok(MigrationRequest {
        name: args.name.clone().filter(|n| !n.trim().is_empty()),
        source: parse_id(&args.source_connection_id)?,
        target: parse_id(&args.target_connection_id)?,
        items: args
            .items
            .iter()
            .map(|i: &CopyItemDto| {
                Ok(CopyItem {
                    source: object_ref(&i.source)?,
                    target: object_ref(&i.target)?,
                })
            })
            .collect::<Result<_, CommandError>>()?,
        existing,
        copy_indexes: args.copy_indexes,
        batch_size: if args.batch_size == 0 {
            1_000
        } else {
            args.batch_size as usize
        },
        on_record_error,
        max_records_per_sec: args.max_records_per_sec.filter(|r| *r > 0),
        parallel: (args.parallel as usize).clamp(1, 4),
        doc_mode,
        table_mode,
        backup_before_replace: args.backup_before_replace.unwrap_or(true),
        sync: match &args.sync {
            None => None,
            Some(s) => Some(SyncSettings {
                method: match s.method.as_str() {
                    "" | "change_stream" => SyncKind::ChangeStream,
                    "polling" => SyncKind::Polling,
                    other => return Err(invalid(format!("unknown sync method `{other}`"))),
                },
                cursor_field: s
                    .cursor_field
                    .clone()
                    .map(|f| f.trim().to_owned())
                    .filter(|f| !f.is_empty()),
                interval_secs: s.interval_secs.clamp(1, 3_600),
                initial_copy: s.initial_copy,
            }),
        },
    })
}

pub fn plan_dto(p: MigrationPlan) -> MigrationPlanDto {
    MigrationPlanDto {
        items: p
            .items
            .into_iter()
            .map(|i| PlanItemDto {
                source: addr(&i.source),
                target: addr(&i.target),
                kind: i.kind,
                tables: i
                    .tables
                    .into_iter()
                    .map(|t| TablePreviewDto {
                        table: addr(&t.table),
                        columns: t.columns,
                        types: t.types,
                        sources: t.sources,
                        rows: t.rows,
                    })
                    .collect(),
                documents: i.documents,
                embedded_into: i.embedded_into.as_ref().map(addr),
                source_count: i.source_count,
                target_exists: i.target_exists,
                target_count: i.target_count,
                indexes: i.indexes,
                statements: i.statements,
                warnings: i.warnings,
            })
            .collect(),
        blockers: p.blockers,
        warnings: p.warnings,
        destructive: p.destructive,
    }
}

fn stream_dto(s: StreamView) -> StreamDto {
    StreamDto {
        source: addr(&s.source),
        target: addr(&s.target),
        phase: as_str(&s.phase),
        total: s.total,
        read: s.stats.read,
        inserted: s.stats.inserted,
        updated: s.stats.updated,
        skipped: s.stats.skipped,
        failed: s.stats.failed,
        bytes: s.stats.bytes,
        retries: s.stats.retries,
        indexes_created: s.indexes_created,
        quarantine: s.quarantine.map(|p| p.display().to_string()),
        rejections: s.rejections,
        error: s.error,
        backup: s.backup.map(|p| p.display().to_string()),
        changes: s.changes,
        deletes: s.deletes,
        last_change: s.last_change.map(|t| t.to_rfc3339()),
    }
}

fn validation_dto(v: ValidationReport) -> ValidationDto {
    ValidationDto {
        level: as_str(&v.level),
        running: v.running,
        passed: v.passed,
        error: v.error,
        streams: v
            .streams
            .into_iter()
            .map(|s| StreamValidationDto {
                source: addr(&s.source),
                source_count: s.source_count.map(|c| c.count),
                target_count: s.target_count.map(|c| c.count),
                counts_exact: s.source_count.is_some_and(|c| c.exact)
                    && s.target_count.is_some_and(|c| c.exact),
                checked: s.checked,
                passed: s.passed,
                notes: s.notes,
                targets: s
                    .targets
                    .into_iter()
                    .map(|t| TargetCheckDto {
                        target: addr(&t.target),
                        expected: t.expected,
                        matched: t.matched,
                        missing: t.missing,
                        extra: t.extra,
                        different: t.different,
                        chunks: t.chunks,
                        mismatched_chunks: t.mismatched_chunks,
                        expected_checksum: t.expected_checksum,
                        actual_checksum: t.actual_checksum,
                    })
                    .collect(),
                differences: s
                    .differences
                    .into_iter()
                    .map(|d| DifferenceDto {
                        target: d.target,
                        key: d.key,
                        kind: as_str(&d.kind),
                        detail: d.detail,
                    })
                    .collect(),
            })
            .collect(),
    }
}

pub fn run_dto(v: RunView) -> RunDto {
    RunDto {
        run_id: v.run_id.to_string(),
        name: v.name,
        dry_run: v.dry_run,
        validation: v.validation.map(validation_dto),
        sync: v.sync.map(|s| SyncArgs {
            method: as_str(&s.method),
            cursor_field: s.cursor_field,
            interval_secs: s.interval_secs,
            initial_copy: s.initial_copy,
        }),
        status: v.status.as_str().to_owned(),
        source_connection_id: v.source.to_string(),
        target_connection_id: v.target.to_string(),
        existing: as_str(&v.existing),
        streams: v.streams.into_iter().map(stream_dto).collect(),
        created_at: v.created_at.to_rfc3339(),
        started_at: v.started_at.map(|t| t.to_rfc3339()),
        ended_at: v.ended_at.map(|t| t.to_rfc3339()),
        error: v.error,
        events: v
            .events
            .into_iter()
            .map(|e| RunEventDto {
                at: e.at.to_rfc3339(),
                level: e.level,
                stream: e.stream,
                message: e.message,
            })
            .collect(),
    }
}

pub async fn plan_impl(
    state: &AppState,
    args: MigrationArgs,
) -> Result<MigrationPlanDto, CommandError> {
    let req = request(&args)?;
    state
        .migrations
        .plan(&req)
        .await
        .map(plan_dto)
        .map_err(|e| app_error(state, e))
}

#[tauri::command]
pub async fn migration_plan(
    state: State<'_, AppState>,
    args: MigrationArgs,
) -> Result<MigrationPlanDto, CommandError> {
    plan_impl(&state, args).await
}

#[tauri::command]
pub async fn migration_start(
    state: State<'_, AppState>,
    args: MigrationArgs,
) -> Result<String, CommandError> {
    let req = request(&args)?;
    state
        .migrations
        .start(req, args.confirm_destructive)
        .await
        .map(|id| id.to_string())
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_dry_run(
    state: State<'_, AppState>,
    args: MigrationArgs,
) -> Result<String, CommandError> {
    let req = request(&args)?;
    state
        .migrations
        .dry_run(req)
        .await
        .map(|id| id.to_string())
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_validate(
    state: State<'_, AppState>,
    args: ValidateArgs,
) -> Result<(), CommandError> {
    let level = match args.level.as_str() {
        "basic" => Level::Basic,
        "standard" | "" => Level::Standard,
        "deep" => Level::Deep,
        other => return Err(invalid(format!("unknown validation level `{other}`"))),
    };
    state
        .migrations
        .validate(run_id(&args.run_id)?, level)
        .await
        .map_err(|e| app_error(&state, e))
}

pub fn monitor_dto(m: gazabridge_application::monitor::MonitorSnapshot) -> MonitorDto {
    MonitorDto {
        at: m.at.to_rfc3339(),
        memory_bytes: m.process.as_ref().map(|p| p.memory_bytes),
        cpu_percent: m.process.as_ref().map(|p| p.cpu_percent),
        runs: m
            .runs
            .into_iter()
            .map(|r| RunMonitorDto {
                run_id: r.run_id.to_string(),
                name: r.name,
                status: as_str(&r.status),
                dry_run: r.dry_run,
                started_at: r.started_at.map(|t| t.to_rfc3339()),
                processed: r.processed,
                total: r.total,
                records_per_sec: r.records_per_sec,
                bytes_per_sec: r.bytes_per_sec,
                eta_secs: r.eta_secs,
                streams: r
                    .streams
                    .into_iter()
                    .map(|s| StreamMonitorDto {
                        source: addr(&s.source),
                        target: addr(&s.target),
                        phase: as_str(&s.phase),
                        processed: s.processed,
                        total: s.total,
                        failed: s.failed,
                        retries: s.retries,
                        bytes: s.bytes,
                        records_per_sec: s.records_per_sec,
                        bytes_per_sec: s.bytes_per_sec,
                        eta_secs: s.eta_secs,
                        queue_len: s.queue_len,
                        queue_capacity: s.queue_capacity,
                        read_ms: s.read_ms,
                        write_ms: s.write_ms,
                        slow_side: s.slow_side.map(|side| as_str(&side)),
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[tauri::command]
pub async fn monitor_snapshot(state: State<'_, AppState>) -> Result<MonitorDto, CommandError> {
    state
        .migrations
        .monitor()
        .await
        .map(monitor_dto)
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_report(
    state: State<'_, AppState>,
    args: ReportArgs,
) -> Result<(), CommandError> {
    use gazabridge_application::report::ReportFormat;
    let format = match args.format.as_str() {
        "json" => ReportFormat::Json,
        "html" => ReportFormat::Html,
        "markdown" | "md" => ReportFormat::Markdown,
        "csv" => ReportFormat::Csv,
        other => return Err(invalid(format!("unknown report format `{other}`"))),
    };
    state
        .migrations
        .export_report(
            run_id(&args.run_id)?,
            std::path::Path::new(&args.path),
            format,
        )
        .await
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_status(
    state: State<'_, AppState>,
    args: RunArgs,
) -> Result<RunDto, CommandError> {
    state
        .migrations
        .view(run_id(&args.run_id)?)
        .await
        .map(run_dto)
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_pause(
    state: State<'_, AppState>,
    args: RunArgs,
) -> Result<(), CommandError> {
    state
        .migrations
        .pause(run_id(&args.run_id)?)
        .await
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_resume(
    state: State<'_, AppState>,
    args: RunArgs,
) -> Result<(), CommandError> {
    state
        .migrations
        .resume(run_id(&args.run_id)?)
        .await
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_cancel(
    state: State<'_, AppState>,
    args: RunArgs,
) -> Result<(), CommandError> {
    state
        .migrations
        .cancel(run_id(&args.run_id)?)
        .await
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn migration_recent(
    state: State<'_, AppState>,
    limit: Option<u32>,
) -> Result<Vec<RunDto>, CommandError> {
    state
        .migrations
        .recent(limit.unwrap_or(20).clamp(1, 500))
        .await
        .map(|v| v.into_iter().map(run_dto).collect())
        .map_err(|e| app_error(&state, e))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use gazabridge_ipc::{ConnectionForm, EngineForm, PasswordChoice, SaveConnectionArgs, TlsForm};

    use super::*;
    use crate::connection_commands::save_connection_impl;

    fn args() -> MigrationArgs {
        MigrationArgs {
            source_connection_id: gazabridge_domain::ids::ConnectionId::new().to_string(),
            target_connection_id: gazabridge_domain::ids::ConnectionId::new().to_string(),
            items: vec![CopyItemDto {
                source: ObjectAddr {
                    database: Some("shop".into()),
                    schema: None,
                    name: "orders".into(),
                    kind: "collection".into(),
                },
                target: ObjectAddr {
                    database: Some("copy".into()),
                    schema: None,
                    name: "orders".into(),
                    kind: "collection".into(),
                },
            }],
            existing: String::new(),
            copy_indexes: true,
            batch_size: 0,
            on_record_error: String::new(),
            max_records_per_sec: Some(0),
            parallel: 9,
            name: Some("  ".into()),
            confirm_destructive: false,
            doc_mode: String::new(),
            table_mode: String::new(),
            backup_before_replace: None,
            sync: None,
        }
    }

    #[test]
    fn arguments_get_safe_defaults_and_bad_choices_are_refused() {
        let r = request(&args()).unwrap();
        assert_eq!(r.existing, ExistingData::Skip);
        assert_eq!(r.on_record_error, OnRecordError::Stop);
        assert_eq!(
            (r.batch_size, r.parallel, r.max_records_per_sec),
            (1_000, 4, None)
        );
        assert_eq!(r.name, None);
        assert_eq!(addr(&r.items[0].target).display(), "copy.orders");
        let mut a = args();
        a.existing = "truncate".into();
        assert!(request(&a).is_err());
        let mut a = args();
        a.doc_mode = "normalize".into();
        a.table_mode = "embed".into();
        let r = request(&a).unwrap();
        assert_eq!(
            (r.doc_mode, r.table_mode),
            (DocMode::Normalize, TableMode::Embed)
        );
        a.doc_mode = "csv".into();
        assert!(request(&a).is_err());
        let mut a = args();
        a.items[0].source.kind = "rocket".into();
        assert!(request(&a).is_err());
    }

    #[tokio::test]
    async fn engine_pairs_without_a_conversion_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
            .await
            .unwrap();
        let EngineForm::Sqlite(mut sq) = EngineForm::default_for("sqlite").unwrap() else {
            unreachable!()
        };
        sq.path = dir.path().join("a.sqlite").display().to_string();
        sq.create_if_missing = true;
        let id = save_connection_impl(
            &state,
            SaveConnectionArgs {
                form: ConnectionForm {
                    id: None,
                    name: "a".into(),
                    profile_id: "sqlite.sqlite".into(),
                    engine: EngineForm::Sqlite(sq),
                    tls: TlsForm::default(),
                    ssh: None,
                    read_only: false,
                    favorite: false,
                    color: None,
                    tags: vec![],
                    folder_id: None,
                },
                password: PasswordChoice::None,
                connection_string: None,
                ssh_secret: PasswordChoice::Keep,
            },
        )
        .await
        .unwrap()
        .form
        .id
        .unwrap();
        let mut a = args();
        a.source_connection_id = id.clone();
        a.target_connection_id = id;
        a.items[0].source = ObjectAddr {
            database: None,
            schema: Some("main".into()),
            name: "a".into(),
            kind: "table".into(),
        };
        let plan = plan_impl(&state, a).await.unwrap();
        assert!(
            plan.blockers.iter().any(|b| b.contains("not supported")),
            "{:?}",
            plan.blockers
        );
    }
}
