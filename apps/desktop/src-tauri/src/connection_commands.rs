//! Connection management commands.
//!
//! Each command is a thin Tauri wrapper around an `*_impl` function that
//! takes `&AppState`, so the logic is testable without a WebView.

use gazabridge_application::AppError;
use gazabridge_domain::ids::ConnectionId;
use gazabridge_ipc::{
    CommandError, ConnectionDetails, FromStringArgs, SaveConnectionArgs, TestResultDto,
};
use tauri::State;

use crate::forms::{self, FormError};
use crate::state::AppState;

pub(crate) fn app_error(state: &AppState, e: AppError) -> CommandError {
    CommandError {
        code: e.code().to_owned(),
        message: state.secrets.redactor().scrub(&e.to_string()),
    }
}

fn form_error(e: FormError) -> CommandError {
    CommandError {
        code: "invalid_input".into(),
        message: e.0,
    }
}

pub(crate) fn parse_id(id: &str) -> Result<ConnectionId, CommandError> {
    id.parse().map_err(|_| CommandError {
        code: "invalid_input".into(),
        message: "invalid connection id".into(),
    })
}

pub async fn get_connection_impl(
    state: &AppState,
    id: &str,
) -> Result<ConnectionDetails, CommandError> {
    let id = parse_id(id)?;
    let profile = state
        .store
        .connections()
        .require(id)
        .await
        .map_err(|e| app_error(state, e.into()))?;
    Ok(ConnectionDetails {
        form: forms::form_from(&profile),
        password_mode: forms::password_mode(&profile),
    })
}

pub async fn save_connection_impl(
    state: &AppState,
    args: SaveConnectionArgs,
) -> Result<ConnectionDetails, CommandError> {
    let password = forms::password_input(&args.password);
    let saved = match &args.form.id {
        Some(id) => {
            let existing = state
                .store
                .connections()
                .require(parse_id(id)?)
                .await
                .map_err(|e| app_error(state, e.into()))?;
            let profile = forms::profile_from(&args.form, Some(&existing)).map_err(form_error)?;
            // Browsing sessions must not keep using the old settings.
            state.explorer.disconnect(existing.id).await;
            state.connections.update(profile, password).await
        }
        None => {
            let profile = forms::profile_from(&args.form, None).map_err(form_error)?;
            state.connections.create(profile, password).await
        }
    }
    .map_err(|e| app_error(state, e))?;
    Ok(ConnectionDetails {
        form: forms::form_from(&saved),
        password_mode: forms::password_mode(&saved),
    })
}

pub async fn create_from_string_impl(
    state: &AppState,
    args: FromStringArgs,
) -> Result<ConnectionDetails, CommandError> {
    let (profile, _warnings) = state
        .connections
        .create_from_string(&args.name, &args.text, args.remember)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(ConnectionDetails {
        form: forms::form_from(&profile),
        password_mode: forms::password_mode(&profile),
    })
}

pub async fn delete_connection_impl(state: &AppState, id: &str) -> Result<(), CommandError> {
    state
        .connections
        .delete(parse_id(id)?)
        .await
        .map_err(|e| app_error(state, e))
}

pub async fn duplicate_connection_impl(
    state: &AppState,
    id: &str,
    name: &str,
) -> Result<ConnectionDetails, CommandError> {
    let copy = state
        .connections
        .duplicate(parse_id(id)?, name)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(ConnectionDetails {
        form: forms::form_from(&copy),
        password_mode: forms::password_mode(&copy),
    })
}

pub async fn test_connection_impl(
    state: &AppState,
    id: &str,
) -> Result<TestResultDto, CommandError> {
    let outcome = state
        .connections
        .test_saved(parse_id(id)?)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(forms::test_result(&outcome))
}

/// Tests an unsaved form. A password typed in the editor is used directly;
/// otherwise the stored secret of the connection being edited is used.
pub async fn test_draft_impl(
    state: &AppState,
    args: SaveConnectionArgs,
) -> Result<TestResultDto, CommandError> {
    let existing = match &args.form.id {
        Some(id) => Some(
            state
                .store
                .connections()
                .require(parse_id(id)?)
                .await
                .map_err(|e| app_error(state, e.into()))?,
        ),
        None => None,
    };
    let profile = forms::profile_from(&args.form, existing.as_ref()).map_err(form_error)?;
    let typed = match &args.password {
        gazabridge_ipc::PasswordChoice::Remember { password }
        | gazabridge_ipc::PasswordChoice::SessionOnly { password } => {
            Some(zeroize::Zeroizing::new(password.clone()))
        }
        _ => None,
    };
    let outcome = state.connections.test_draft(&profile, typed).await;
    Ok(forms::test_result(&outcome))
}

#[tauri::command]
pub async fn get_connection(
    state: State<'_, AppState>,
    id: String,
) -> Result<ConnectionDetails, CommandError> {
    get_connection_impl(&state, &id).await
}

#[tauri::command]
pub async fn save_connection(
    state: State<'_, AppState>,
    args: SaveConnectionArgs,
) -> Result<ConnectionDetails, CommandError> {
    save_connection_impl(&state, args).await
}

#[tauri::command]
pub async fn create_connection_from_string(
    state: State<'_, AppState>,
    args: FromStringArgs,
) -> Result<ConnectionDetails, CommandError> {
    create_from_string_impl(&state, args).await
}

#[tauri::command]
pub async fn delete_connection(state: State<'_, AppState>, id: String) -> Result<(), CommandError> {
    delete_connection_impl(&state, &id).await
}

#[tauri::command]
pub async fn duplicate_connection(
    state: State<'_, AppState>,
    id: String,
    name: String,
) -> Result<ConnectionDetails, CommandError> {
    duplicate_connection_impl(&state, &id, &name).await
}

#[tauri::command]
pub async fn test_connection(
    state: State<'_, AppState>,
    id: String,
) -> Result<TestResultDto, CommandError> {
    test_connection_impl(&state, &id).await
}

#[tauri::command]
pub async fn test_connection_draft(
    state: State<'_, AppState>,
    args: SaveConnectionArgs,
) -> Result<TestResultDto, CommandError> {
    test_draft_impl(&state, args).await
}

#[cfg(test)]
mod tests {
    use gazabridge_ipc::{ConnectionForm, EngineForm, PasswordChoice, TlsForm};

    use super::*;

    fn sqlite_form(path: &std::path::Path) -> ConnectionForm {
        let EngineForm::Sqlite(mut s) = EngineForm::default_for("sqlite").unwrap() else {
            unreachable!()
        };
        s.path = path.display().to_string();
        s.create_if_missing = true;
        ConnectionForm {
            id: None,
            name: "local".into(),
            profile_id: "sqlite.sqlite".into(),
            engine: EngineForm::Sqlite(s),
            tls: TlsForm::default(),
            read_only: false,
            favorite: true,
            color: None,
            tags: vec!["dev".into()],
            folder_id: None,
        }
    }

    #[tokio::test]
    async fn connection_commands_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
            .await
            .unwrap();

        // Draft test before saving.
        let form = sqlite_form(&dir.path().join("app.db"));
        let draft = test_draft_impl(
            &state,
            SaveConnectionArgs {
                form: form.clone(),
                password: PasswordChoice::None,
            },
        )
        .await
        .unwrap();
        assert_eq!(draft.state, "ready", "{draft:?}");
        assert_eq!(draft.server_product.as_deref(), Some("SQLite"));

        // Save, read back, edit.
        let saved = save_connection_impl(
            &state,
            SaveConnectionArgs {
                form,
                password: PasswordChoice::None,
            },
        )
        .await
        .unwrap();
        let id = saved.form.id.clone().unwrap();
        let mut edited = get_connection_impl(&state, &id).await.unwrap().form;
        edited.name = "renamed".into();
        let after = save_connection_impl(
            &state,
            SaveConnectionArgs {
                form: edited,
                password: PasswordChoice::Keep,
            },
        )
        .await
        .unwrap();
        assert_eq!(after.form.name, "renamed");
        assert_eq!(after.form.id.as_deref(), Some(id.as_str()));

        // Saved test records health.
        let tested = test_connection_impl(&state, &id).await.unwrap();
        assert_eq!(tested.state_label, "Ready");

        // Duplicate and delete.
        let copy = duplicate_connection_impl(&state, &id, "copy")
            .await
            .unwrap();
        assert_ne!(copy.form.id, after.form.id);
        delete_connection_impl(&state, &id).await.unwrap();
        assert_eq!(
            get_connection_impl(&state, &id).await.unwrap_err().code,
            "not_found"
        );
    }

    #[tokio::test]
    async fn bad_input_returns_stable_error_codes() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
            .await
            .unwrap();
        assert_eq!(
            get_connection_impl(&state, "nope").await.unwrap_err().code,
            "invalid_input"
        );
        let mut form = sqlite_form(&dir.path().join("x.db"));
        if let EngineForm::Sqlite(s) = &mut form.engine {
            s.journal_mode = "sideways".into();
        }
        let err = save_connection_impl(
            &state,
            SaveConnectionArgs {
                form,
                password: PasswordChoice::None,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, "invalid_input");
        let err = create_from_string_impl(
            &state,
            FromStringArgs {
                name: "x".into(),
                text: "postgres://u:topsecret@h:notaport/db".into(),
                remember: false,
            },
        )
        .await
        .unwrap_err();
        assert!(!err.message.contains("topsecret"), "{}", err.message);
    }
}
