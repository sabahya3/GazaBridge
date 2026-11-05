//! Query Studio commands.

use std::time::Duration;

use gazabridge_application::QueryInput;
use gazabridge_application::explorer::field_union;
use gazabridge_application::query::language_name;
use gazabridge_domain::ejson;
use gazabridge_domain::ids::{ConnectionId, SavedQueryId};
use gazabridge_ipc::{
    CancelArgs, CommandError, DeleteSavedArgs, ExplainArgs, ExplainDto, HistoryArgs, HistoryDto,
    ListSavedArgs, QueryContextArgs, QueryContextDto, QueryResultDto, RunQueryArgs, SaveQueryArgs,
    SavedQueryDto,
};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_storage::queries::SavedQuery;
use tauri::State;

use crate::connection_commands::{app_error, parse_id};
use crate::explorer_commands::table_dto;
use crate::state::AppState;

fn invalid(message: impl Into<String>) -> CommandError {
    CommandError {
        connection_id: None,
        code: "invalid_input".into(),
        message: message.into(),
    }
}

fn parse_language(name: &str) -> Result<QueryLanguage, CommandError> {
    serde_json::from_value(serde_json::Value::String(name.to_owned()))
        .map_err(|_| invalid(format!("unknown query language `{name}`")))
}

fn opt_id(id: Option<&str>) -> Result<Option<ConnectionId>, CommandError> {
    id.filter(|s| !s.is_empty()).map(parse_id).transpose()
}

fn input(args: &RunQueryArgs) -> Result<QueryInput, CommandError> {
    let params =
        args.params
            .iter()
            .enumerate()
            .map(
                |(i, text)| match serde_json::from_str::<serde_json::Value>(text) {
                    Ok(j) => ejson::from_ejson(&j)
                        .map_err(|e| invalid(format!("parameter ${}: {e}", i + 1))),
                    // Not JSON: the text itself.
                    Err(_) => Ok(gazabridge_domain::Value::String(text.clone())),
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
    let clean = |s: &Option<String>| {
        s.as_ref()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    Ok(QueryInput {
        language: parse_language(&args.language)?,
        text: args.text.clone(),
        params,
        database: clean(&args.database),
        collection: clean(&args.collection),
        max_rows: args.max_rows,
        timeout: args
            .timeout_secs
            .filter(|s| *s > 0)
            .map(|s| Duration::from_secs(u64::from(s))),
        confirmed: args.confirmed,
    })
}

fn saved_dto(q: SavedQuery) -> SavedQueryDto {
    SavedQueryDto {
        id: q.id.to_string(),
        name: q.name,
        connection_id: q.connection_id.map(|c| c.to_string()),
        language: q.language,
        text: q.text,
        favorite: q.favorite,
        updated_at: q.updated_at.to_rfc3339(),
    }
}

pub async fn context_impl(
    state: &AppState,
    args: QueryContextArgs,
) -> Result<QueryContextDto, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let c = state
        .queries
        .context(id)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(QueryContextDto {
        languages: c
            .languages
            .into_iter()
            .map(|l| language_name(l).to_owned())
            .collect(),
        explain: c.explain,
        explain_analyze: c.explain_analyze,
        read_only: c.read_only,
    })
}

pub async fn run_impl(
    state: &AppState,
    args: RunQueryArgs,
) -> Result<QueryResultDto, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let input = input(&args)?;
    let r = state
        .queries
        .run(id, &args.run_id, input)
        .await
        .map_err(|e| app_error(state, e))?;
    let fields: Vec<String> = if r.columns.is_empty() {
        field_union(&r.rows)
    } else {
        r.columns.iter().map(|c| c.name.clone()).collect()
    };
    let (columns, rows) = table_dto(&fields, &r.columns, &[], &r.rows);
    Ok(QueryResultDto {
        columns,
        rows,
        affected: r.affected,
        truncated: r.truncated,
        elapsed_ms: u64::try_from(r.elapsed.as_millis()).unwrap_or(u64::MAX),
        notices: r.notices,
    })
}

pub async fn explain_impl(state: &AppState, args: ExplainArgs) -> Result<ExplainDto, CommandError> {
    let id = parse_id(&args.query.connection_id)?;
    let input = input(&args.query)?;
    let plan = state
        .queries
        .explain(id, &args.query.run_id, input, args.analyze)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(ExplainDto {
        text: plan.text,
        native: serde_json::to_string_pretty(&plan.native).unwrap_or_default(),
        analyzed: plan.analyzed,
    })
}

pub async fn history_impl(
    state: &AppState,
    args: HistoryArgs,
) -> Result<Vec<HistoryDto>, CommandError> {
    let id = opt_id(args.connection_id.as_deref())?;
    let entries = state
        .queries
        .history(id, args.limit)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(entries
        .into_iter()
        .map(|h| HistoryDto {
            id: h.id,
            connection_id: h.connection_id.map(|c| c.to_string()),
            language: h.language,
            text: h.text,
            started_at: h.started_at.to_rfc3339(),
            duration_ms: h.duration_ms,
            row_count: h.row_count,
            affected: h.affected,
            status: serde_json::to_value(h.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default(),
            error_message: h.error_message,
        })
        .collect())
}

pub async fn list_saved_impl(
    state: &AppState,
    args: ListSavedArgs,
) -> Result<Vec<SavedQueryDto>, CommandError> {
    let id = opt_id(args.connection_id.as_deref())?;
    let list = state
        .queries
        .saved(id)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(list.into_iter().map(saved_dto).collect())
}

pub async fn save_impl(
    state: &AppState,
    args: SaveQueryArgs,
) -> Result<SavedQueryDto, CommandError> {
    let id = args
        .id
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<SavedQueryId>()
                .map_err(|_| invalid("invalid saved query id"))
        })
        .transpose()?;
    let q = state
        .queries
        .save(
            id,
            &args.name,
            opt_id(args.connection_id.as_deref())?,
            parse_language(&args.language)?,
            &args.text,
            args.favorite,
        )
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(saved_dto(q))
}

pub async fn delete_saved_impl(
    state: &AppState,
    args: DeleteSavedArgs,
) -> Result<(), CommandError> {
    let id = args
        .id
        .parse::<SavedQueryId>()
        .map_err(|_| invalid("invalid saved query id"))?;
    state
        .queries
        .delete_saved(id)
        .await
        .map_err(|e| app_error(state, e))
}

#[tauri::command]
pub async fn query_context(
    state: State<'_, AppState>,
    args: QueryContextArgs,
) -> Result<QueryContextDto, CommandError> {
    context_impl(&state, args).await
}

#[tauri::command]
pub async fn query_run(
    state: State<'_, AppState>,
    args: RunQueryArgs,
) -> Result<QueryResultDto, CommandError> {
    run_impl(&state, args).await
}

#[tauri::command]
pub async fn query_explain(
    state: State<'_, AppState>,
    args: ExplainArgs,
) -> Result<ExplainDto, CommandError> {
    explain_impl(&state, args).await
}

#[tauri::command]
pub async fn query_cancel(
    state: State<'_, AppState>,
    args: CancelArgs,
) -> Result<bool, CommandError> {
    Ok(state.queries.cancel(&args.run_id).await)
}

#[tauri::command]
pub async fn query_history(
    state: State<'_, AppState>,
    args: HistoryArgs,
) -> Result<Vec<HistoryDto>, CommandError> {
    history_impl(&state, args).await
}

#[tauri::command]
pub async fn query_list_saved(
    state: State<'_, AppState>,
    args: ListSavedArgs,
) -> Result<Vec<SavedQueryDto>, CommandError> {
    list_saved_impl(&state, args).await
}

#[tauri::command]
pub async fn query_save(
    state: State<'_, AppState>,
    args: SaveQueryArgs,
) -> Result<SavedQueryDto, CommandError> {
    save_impl(&state, args).await
}

#[tauri::command]
pub async fn query_delete_saved(
    state: State<'_, AppState>,
    args: DeleteSavedArgs,
) -> Result<(), CommandError> {
    delete_saved_impl(&state, args).await
}

#[cfg(test)]
mod tests {
    use gazabridge_ipc::{
        ConnectionForm, EngineForm, PasswordChoice, SaveConnectionArgs, TlsForm, risks_from_message,
    };

    use super::*;
    use crate::connection_commands::save_connection_impl;

    fn run(cid: &str, text: &str) -> RunQueryArgs {
        RunQueryArgs {
            connection_id: cid.into(),
            run_id: "r1".into(),
            language: "sqlite_sql".into(),
            text: text.into(),
            params: vec![],
            database: None,
            collection: None,
            max_rows: 2,
            timeout_secs: None,
            confirmed: false,
        }
    }

    #[tokio::test]
    async fn query_studio_end_to_end_on_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
            .await
            .unwrap();
        let EngineForm::Sqlite(mut s) = EngineForm::default_for("sqlite").unwrap() else {
            unreachable!()
        };
        s.path = dir.path().join("q.db").display().to_string();
        s.create_if_missing = true;
        let form = ConnectionForm {
            id: None,
            name: "q".into(),
            profile_id: "sqlite.sqlite".into(),
            engine: EngineForm::Sqlite(s),
            tls: TlsForm::default(),
            ssh: None,
            read_only: false,
            favorite: false,
            color: None,
            tags: vec![],
            folder_id: None,
        };
        let saved = save_connection_impl(
            &state,
            SaveConnectionArgs {
                form,
                password: PasswordChoice::None,
                connection_string: None,
                ssh_secret: PasswordChoice::Keep,
            },
        )
        .await
        .unwrap();
        let cid = saved.form.id.unwrap();

        let ctx = context_impl(
            &state,
            QueryContextArgs {
                connection_id: cid.clone(),
            },
        )
        .await
        .unwrap();
        assert_eq!(ctx.languages, ["sqlite_sql"]);

        run_impl(
            &state,
            run(&cid, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"),
        )
        .await
        .unwrap();
        let mut insert = run(&cid, "INSERT INTO t (name) VALUES (?), (?), (?)");
        insert.params = vec!["\"a\"".into(), "b".into(), "\"c\"".into()];
        assert_eq!(run_impl(&state, insert).await.unwrap().affected, Some(3));

        let r = run_impl(&state, run(&cid, "SELECT id, name FROM t ORDER BY id"))
            .await
            .unwrap();
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );
        assert_eq!(r.rows.len(), 2);
        assert!(r.truncated);
        assert_eq!(r.rows[1].cells[1].text, "b");

        // Destructive statements need confirmation.
        let err = run_impl(&state, run(&cid, "DELETE FROM t"))
            .await
            .unwrap_err();
        assert_eq!(err.code, gazabridge_ipc::CONFIRMATION_REQUIRED);
        assert_eq!(risks_from_message(&err.message), ["delete_without_where"]);
        let mut confirmed = run(&cid, "DELETE FROM t");
        confirmed.confirmed = true;
        assert_eq!(run_impl(&state, confirmed).await.unwrap().affected, Some(3));

        // Errors are recorded in history; nothing to cancel afterwards.
        assert!(run_impl(&state, run(&cid, "SELEC nonsense")).await.is_err());
        let history = history_impl(
            &state,
            HistoryArgs {
                connection_id: Some(cid.clone()),
                limit: 50,
            },
        )
        .await
        .unwrap();
        assert_eq!(history[0].status, "error");
        assert!(history[0].error_message.is_some());
        assert_eq!(history.len(), 5, "the refused DELETE never ran");
        assert!(!state.queries.cancel("r1").await);

        // Saved queries.
        let q = save_impl(
            &state,
            SaveQueryArgs {
                id: None,
                name: "All".into(),
                connection_id: Some(cid.clone()),
                language: "sqlite_sql".into(),
                text: "SELECT * FROM t".into(),
                favorite: true,
            },
        )
        .await
        .unwrap();
        let renamed = save_impl(
            &state,
            SaveQueryArgs {
                id: Some(q.id.clone()),
                name: "Everything".into(),
                connection_id: Some(cid.clone()),
                language: "sqlite_sql".into(),
                text: q.text.clone(),
                favorite: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(renamed.id, q.id);
        let list = list_saved_impl(
            &state,
            ListSavedArgs {
                connection_id: Some(cid.clone()),
            },
        )
        .await
        .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "Everything");
        delete_saved_impl(&state, DeleteSavedArgs { id: q.id })
            .await
            .unwrap();
        assert!(
            list_saved_impl(
                &state,
                ListSavedArgs {
                    connection_id: None
                }
            )
            .await
            .unwrap()
            .is_empty()
        );

        assert_eq!(
            run_impl(&state, run(&cid, "  ")).await.unwrap_err().code,
            "invalid_input"
        );
        let mut bad = run(&cid, "SELECT 1");
        bad.language = "cobol".into();
        assert_eq!(
            run_impl(&state, bad).await.unwrap_err().code,
            "invalid_input"
        );
    }
}
