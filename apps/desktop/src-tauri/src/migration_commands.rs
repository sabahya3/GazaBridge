//! Copying collections between connections.

use gazabridge_application::{
    CopyItem, ExistingData, MigrationPlan, MigrationRequest, OnRecordError, RunView, StreamView,
};
use gazabridge_domain::ids::RunId;
use gazabridge_ipc::{
    CommandError, CopyItemDto, MigrationArgs, MigrationPlanDto, ObjectAddr, PlanItemDto, RunArgs,
    RunDto, RunEventDto, StreamDto, TablePreviewDto,
};
use gazabridge_mapping::doc_to_table::DocMode;
use gazabridge_mapping::table_to_doc::TableMode;
use gazabridge_plugin_sdk::schema::ObjectRef;
use tauri::State;

use crate::connection_commands::{app_error, parse_id};
use crate::explorer_commands::object_ref;
use crate::state::AppState;

fn invalid(message: impl Into<String>) -> CommandError {
    CommandError {
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
    }
}

pub fn run_dto(v: RunView) -> RunDto {
    RunDto {
        run_id: v.run_id.to_string(),
        name: v.name,
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
pub async fn migration_recent(state: State<'_, AppState>) -> Result<Vec<RunDto>, CommandError> {
    state
        .migrations
        .recent(20)
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
                    read_only: false,
                    favorite: false,
                    color: None,
                    tags: vec![],
                    folder_id: None,
                },
                password: PasswordChoice::None,
                connection_string: None,
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
