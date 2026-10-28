//! Tauri commands. Every error crossing the IPC boundary is redacted.

use gazabridge_domain::connection::EngineFamily;
use gazabridge_ipc::{
    AppInfo, CommandError, ConnectionSummary, LogFilter, LogLine, Preferences, ProfileSummary,
};
use gazabridge_observability::LogLevel;
use gazabridge_observability::reader::{self, LogQuery};
use gazabridge_plugin_sdk::Capability;
use gazabridge_storage::connections::ConnectionFilter;
use tauri::State;

use crate::state::AppState;

const PREFERENCES_KEY: &str = "preferences";

fn fail(state: &AppState, code: &str, e: impl std::fmt::Display) -> CommandError {
    let message = state.secrets.redactor().scrub(&e.to_string());
    tracing::warn!(code, %message, "command failed");
    CommandError {
        connection_id: None,
        code: code.to_owned(),
        message,
    }
}

#[tauri::command]
pub async fn app_info(state: State<'_, AppState>) -> Result<AppInfo, CommandError> {
    let schema = state
        .store
        .schema_version()
        .await
        .map_err(|e| fail(&state, "storage", e))?;
    Ok(AppInfo {
        name: "GazaBridge".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        connector_api: gazabridge_plugin_sdk::API_VERSION.to_string(),
        metadata_schema_version: schema,
        data_directory: state.data_dir.display().to_string(),
        log_directory: state.log_dir.display().to_string(),
        telemetry_enabled: false,
    })
}

#[tauri::command]
pub async fn get_preferences(state: State<'_, AppState>) -> Result<Preferences, CommandError> {
    let prefs = state
        .store
        .ui_preferences()
        .get::<Preferences>(PREFERENCES_KEY)
        .await
        .map_err(|e| fail(&state, "storage", e))?;
    Ok(prefs.unwrap_or_default())
}

#[tauri::command]
pub async fn set_preferences(
    state: State<'_, AppState>,
    preferences: Preferences,
) -> Result<(), CommandError> {
    if !(1..=10_000).contains(&preferences.default_row_limit) {
        return Err(CommandError {
            connection_id: None,
            code: "invalid_input".into(),
            message: "default row limit must be between 1 and 10000".into(),
        });
    }
    state
        .store
        .ui_preferences()
        .set(PREFERENCES_KEY, &preferences)
        .await
        .map_err(|e| fail(&state, "storage", e))
}

#[tauri::command]
pub async fn list_connections(
    state: State<'_, AppState>,
    search: Option<String>,
) -> Result<Vec<ConnectionSummary>, CommandError> {
    let repo = state.store.connections();
    let profiles = repo
        .list(&ConnectionFilter {
            search,
            ..Default::default()
        })
        .await
        .map_err(|e| fail(&state, "storage", e))?;
    let mut out = Vec::with_capacity(profiles.len());
    for p in profiles {
        let health = repo
            .health(p.id)
            .await
            .map_err(|e| fail(&state, "storage", e))?;
        let profile_name = state
            .registry
            .catalog()
            .get(&p.compatibility_profile)
            .map(|c| c.display_name.clone())
            .unwrap_or_else(|| p.compatibility_profile.clone());
        out.push(ConnectionSummary {
            id: p.id.to_string(),
            name: p.name.clone(),
            family: p.family().as_str().into(),
            profile_id: p.compatibility_profile.clone(),
            profile_name,
            target: state.secrets.redactor().scrub(&p.settings.display_target()),
            read_only: p.read_only,
            favorite: p.favorite,
            tags: p.tags.clone(),
            color: p.color.clone(),
            state: health.as_ref().map(|h| h.state.label().to_owned()),
            latency_ms: health.and_then(|h| h.latency_ms),
        });
    }
    Ok(out)
}

#[tauri::command]
pub async fn list_profiles(
    state: State<'_, AppState>,
) -> Result<Vec<ProfileSummary>, CommandError> {
    let registered: Vec<EngineFamily> = state.registry.registered_families().collect();
    Ok(state
        .registry
        .catalog()
        .all()
        .map(|p| {
            let available = registered.contains(&p.family);
            let cdc = available
                && state
                    .registry
                    .planned_manifest(&p.id, false)
                    .is_ok_and(|m| m.supports(Capability::ChangeDataCapture));
            ProfileSummary {
                id: p.id.clone(),
                family: p.family.as_str().into(),
                family_name: p.family.display_name().into(),
                display_name: p.display_name.clone(),
                status: p.status.label().into(),
                tested_versions: p.tested_versions.iter().map(|v| v.to_string()).collect(),
                known_limitations: p.known_limitations.clone(),
                unsupported_operations: p.unsupported_operations.clone(),
                auth_methods: p
                    .auth_methods
                    .iter()
                    .map(|a| {
                        serde_json::to_value(a)
                            .ok()
                            .and_then(|v| v.as_str().map(str::to_owned))
                            .unwrap_or_default()
                    })
                    .collect(),
                documentation_url: p.documentation_url.clone(),
                connector_available: available,
                cdc_available: cdc,
            }
        })
        .collect())
}

#[tauri::command]
pub async fn query_logs(
    state: State<'_, AppState>,
    filter: LogFilter,
) -> Result<Vec<LogLine>, CommandError> {
    let query = LogQuery {
        max_verbosity: filter.max_level.as_deref().and_then(LogLevel::parse),
        search: filter.search.filter(|s| !s.trim().is_empty()),
        run_id: filter.run_id,
        limit: filter.limit.clamp(1, 5_000) as usize,
        ..Default::default()
    };
    let dir = state.log_dir.clone();
    let entries = tokio::task::spawn_blocking(move || reader::query(&dir, &query))
        .await
        .map_err(|e| fail(&state, "internal", e))?
        .map_err(|e| fail(&state, "io", e))?;
    Ok(entries
        .into_iter()
        .map(|e| LogLine {
            timestamp: e.timestamp.map(|t| t.to_rfc3339()),
            level: e.level.as_str().into(),
            target: e.target,
            message: e.message,
            run_id: e.run_id,
            job_id: e.job_id,
        })
        .collect())
}
