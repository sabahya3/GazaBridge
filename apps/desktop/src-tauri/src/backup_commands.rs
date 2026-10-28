//! Backups and restores.

use std::path::PathBuf;

use gazabridge_application::ExistingData;
use gazabridge_application::backup::{
    BackupMethod, BackupProgress, BackupRequest, Manifest, RestorePlan, RestoreRequest,
};
use gazabridge_application::tools::{self, ToolKind};
use gazabridge_ipc::{
    BackupArgs, BackupDto, BackupObjectDto, BackupStatusDto, CommandError, ObjectAddr, OpArgs,
    PathArgs, RestoreArgs, RestoreItemDto, RestorePlanDto, ToolDto,
};
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

fn existing(s: &str) -> Result<ExistingData, CommandError> {
    Ok(match s {
        "" | "skip" => ExistingData::Skip,
        "update" => ExistingData::Update,
        "fail" => ExistingData::Fail,
        "replace" => ExistingData::Replace,
        other => {
            return Err(invalid(format!(
                "unknown choice for existing data `{other}`"
            )));
        }
    })
}

pub fn backup_dto(path: &std::path::Path, m: &Manifest) -> BackupDto {
    BackupDto {
        path: path.display().to_string(),
        label: m.label.clone(),
        created_at: m.created_at.to_rfc3339(),
        method: as_str(&m.method),
        family: match m.family {
            gazabridge_domain::connection::EngineFamily::Postgres => "postgres".into(),
            gazabridge_domain::connection::EngineFamily::MongoDb => "mongodb".into(),
            other => format!("{other:?}").to_lowercase(),
        },
        connection: m.connection.clone(),
        target: m.target.clone(),
        database: m.database.clone(),
        schema: m.schema.clone(),
        objects: m
            .objects
            .iter()
            .map(|o| BackupObjectDto {
                object: addr(&o.object),
                records: o.records,
                bytes: o.bytes,
            })
            .collect(),
        records: m.records(),
        bytes: m.bytes(),
        tool: m.archive.as_ref().map(|a| {
            format!("{} {}", a.tool, a.version.clone().unwrap_or_default())
                .trim()
                .to_owned()
        }),
        command: m.archive.as_ref().map(|a| a.command.clone()),
        warnings: m.warnings.clone(),
    }
}

fn status_dto(p: BackupProgress) -> BackupStatusDto {
    BackupStatusDto {
        phase: as_str(&p.phase),
        step: p.step,
        objects_done: p.objects_done,
        objects_total: p.objects_total,
        records: p.records,
        log: p.log,
        error: p.error,
        path: p.path.map(|p| p.display().to_string()),
    }
}

pub fn restore_request(args: &RestoreArgs) -> Result<RestoreRequest, CommandError> {
    Ok(RestoreRequest {
        backup: PathBuf::from(&args.backup),
        connection: parse_id(&args.connection_id)?,
        rename_to: args.rename_to.clone().filter(|r| !r.trim().is_empty()),
        only: args.only.iter().map(object_ref).collect::<Result<_, _>>()?,
        existing: existing(&args.existing)?,
    })
}

fn plan_dto(p: RestorePlan) -> RestorePlanDto {
    RestorePlanDto {
        items: p
            .items
            .into_iter()
            .map(|i| RestoreItemDto {
                from: addr(&i.from),
                to: addr(&i.to),
                records: i.records,
                target_exists: i.target_exists,
                target_count: i.target_count,
                verified: i.verified,
            })
            .collect(),
        statements: p.statements,
        command: p.command,
        blockers: p.blockers,
        warnings: p.warnings,
        destructive: p.destructive,
    }
}

#[tauri::command]
pub async fn backup_tools() -> Result<Vec<ToolDto>, CommandError> {
    let mut out = Vec::new();
    for kind in ToolKind::ALL {
        let t = tools::detect(kind).await;
        out.push(ToolDto {
            name: kind.program().into(),
            found: t.path.is_some(),
            path: t.path.map(|p| p.display().to_string()),
            version: t.version,
            install_hint: t.install_hint,
        });
    }
    Ok(out)
}

#[tauri::command]
pub async fn backup_list(state: State<'_, AppState>) -> Result<Vec<BackupDto>, CommandError> {
    state
        .backups
        .list()
        .await
        .map(|l| l.iter().map(|(p, m)| backup_dto(p, m)).collect())
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn backup_start(
    state: State<'_, AppState>,
    args: BackupArgs,
) -> Result<(), CommandError> {
    let req = BackupRequest {
        connection: parse_id(&args.connection_id)?,
        objects: args
            .objects
            .iter()
            .map(object_ref)
            .collect::<Result<_, _>>()?,
        method: match args.method.as_str() {
            "" | "gazabridge" => BackupMethod::GazaBridge,
            "native" => BackupMethod::Native,
            other => return Err(invalid(format!("unknown backup method `{other}`"))),
        },
        label: args.label.clone(),
    };
    state
        .backups
        .start_backup(&args.op_id, req)
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn backup_status(
    state: State<'_, AppState>,
    args: OpArgs,
) -> Result<BackupStatusDto, CommandError> {
    state
        .backups
        .status(&args.op_id)
        .map(status_dto)
        .ok_or_else(|| CommandError {
            code: "not_found".into(),
            message: format!("no operation `{}`", args.op_id),
        })
}

#[tauri::command]
pub async fn backup_cancel(state: State<'_, AppState>, args: OpArgs) -> Result<bool, CommandError> {
    Ok(state.backups.cancel(&args.op_id))
}

#[tauri::command]
pub async fn backup_delete(state: State<'_, AppState>, args: PathArgs) -> Result<(), CommandError> {
    state
        .backups
        .delete(std::path::Path::new(&args.path))
        .await
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn restore_plan(
    state: State<'_, AppState>,
    args: RestoreArgs,
) -> Result<RestorePlanDto, CommandError> {
    let req = restore_request(&args)?;
    state
        .backups
        .plan_restore(&req)
        .await
        .map(|(p, _)| plan_dto(p))
        .map_err(|e| app_error(&state, e))
}

#[tauri::command]
pub async fn restore_start(
    state: State<'_, AppState>,
    args: RestoreArgs,
) -> Result<(), CommandError> {
    let req = restore_request(&args)?;
    state
        .backups
        .start_restore(&args.op_id, req, args.confirm_destructive)
        .map_err(|e| app_error(&state, e))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn restore_arguments_are_checked() {
        let mut args = RestoreArgs {
            op_id: "o".into(),
            backup: "/x/b".into(),
            connection_id: gazabridge_domain::ids::ConnectionId::new().to_string(),
            rename_to: Some("  ".into()),
            only: vec![],
            existing: String::new(),
            confirm_destructive: false,
        };
        let r = restore_request(&args).unwrap();
        assert_eq!((r.rename_to, r.existing), (None, ExistingData::Skip));
        args.existing = "truncate".into();
        assert!(restore_request(&args).is_err());
    }
}
