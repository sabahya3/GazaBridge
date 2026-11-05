//! Connection management commands.
//!
//! Each command is a thin Tauri wrapper around an `*_impl` function that
//! takes `&AppState`, so the logic is testable without a WebView.

use gazabridge_application::{AppError, PasswordInput};
use gazabridge_domain::ids::ConnectionId;
use gazabridge_ipc::{
    CommandError, ConnectionDetails, FromStringArgs, ParseStringArgs, ParsedStringDto,
    SaveConnectionArgs, TestResultDto,
};
use tauri::State;

use crate::forms::{self, FormError};
use crate::state::AppState;

pub(crate) fn app_error(state: &AppState, e: AppError) -> CommandError {
    CommandError {
        connection_id: e.connection_id(),
        code: e.code().to_owned(),
        message: state.secrets.redactor().scrub(&e.to_string()),
    }
}

fn form_error(e: FormError) -> CommandError {
    CommandError {
        connection_id: None,
        code: "invalid_input".into(),
        message: e.0,
    }
}

pub(crate) fn parse_id(id: &str) -> Result<ConnectionId, CommandError> {
    id.parse().map_err(|_| CommandError {
        connection_id: None,
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
        ssh_secret_mode: forms::ssh_secret_mode(&profile),
    })
}

pub async fn save_connection_impl(
    state: &AppState,
    args: SaveConnectionArgs,
) -> Result<ConnectionDetails, CommandError> {
    let (form, password) = forms::prepare(
        &args.form,
        &args.password,
        args.connection_string.as_deref(),
    )
    .map_err(form_error)?;
    let ssh = forms::password_input(&args.ssh_secret);
    let saved = match &form.id {
        Some(id) => {
            let existing = state
                .store
                .connections()
                .require(parse_id(id)?)
                .await
                .map_err(|e| app_error(state, e.into()))?;
            let profile = forms::profile_from(&form, Some(&existing)).map_err(form_error)?;
            // Browsing sessions must not keep using the old settings.
            state.explorer.disconnect(existing.id).await;
            state
                .connections
                .update_with_ssh(profile, password, ssh)
                .await
        }
        None => {
            let profile = forms::profile_from(&form, None).map_err(form_error)?;
            state
                .connections
                .create_with_ssh(profile, password, ssh)
                .await
        }
    }
    .map_err(|e| app_error(state, e))?;
    Ok(ConnectionDetails {
        form: forms::form_from(&saved),
        password_mode: forms::password_mode(&saved),
        ssh_secret_mode: forms::ssh_secret_mode(&saved),
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
        ssh_secret_mode: forms::ssh_secret_mode(&profile),
    })
}

/// Reads a connection string into an editor form without saving anything.
/// The password stays out of the result; the editor keeps the text and
/// sends it again on save so the password goes to secure storage.
pub fn parse_string_impl(text: &str) -> Result<ParsedStringDto, CommandError> {
    let parsed =
        gazabridge_domain::connection_string::parse(text.trim()).map_err(|e| CommandError {
            connection_id: None,
            code: "invalid_input".into(),
            message: e.to_string(),
        })?;
    let suggested_name = forms::suggested_name(&parsed.settings);
    let profile = gazabridge_domain::connection::ConnectionProfile::new(
        suggested_name.clone(),
        parsed.compatibility_profile,
        parsed.settings,
    );
    let mut form = forms::form_from(&profile);
    form.id = None;
    Ok(ParsedStringDto {
        form,
        has_password: parsed.password.is_some(),
        suggested_name,
        warnings: parsed.warnings,
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
        ssh_secret_mode: forms::ssh_secret_mode(&copy),
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
    let (form, input) = forms::prepare(
        &args.form,
        &args.password,
        args.connection_string.as_deref(),
    )
    .map_err(form_error)?;
    let profile = forms::profile_from(&form, existing.as_ref()).map_err(form_error)?;
    let typed = match input {
        PasswordInput::Remember(p) | PasswordInput::SessionOnly(p) => Some(p),
        _ => None,
    };
    let typed_ssh = match forms::password_input(&args.ssh_secret) {
        PasswordInput::Remember(p) | PasswordInput::SessionOnly(p) => Some(p),
        _ => None,
    };
    let outcome = state
        .connections
        .test_draft_with_ssh(&profile, typed, typed_ssh)
        .await;
    Ok(forms::test_result(&outcome))
}

#[tauri::command]
pub async fn parse_connection_string(
    args: ParseStringArgs,
) -> Result<ParsedStringDto, CommandError> {
    parse_string_impl(&args.text)
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
            ssh: None,
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
                connection_string: None,
                ssh_secret: PasswordChoice::Keep,
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
                connection_string: None,
                ssh_secret: PasswordChoice::Keep,
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
                connection_string: None,
                ssh_secret: PasswordChoice::Keep,
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
                connection_string: None,
                ssh_secret: PasswordChoice::Keep,
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

/// Uses a password for a connection this session; `remember` saves it in
/// the keychain again. Returns whether it was saved.
#[tauri::command]
pub async fn unlock_connection(
    state: tauri::State<'_, AppState>,
    args: gazabridge_ipc::UnlockArgs,
) -> Result<bool, CommandError> {
    let id = parse_id(&args.connection_id)?;
    state
        .connections
        .unlock(id, zeroize::Zeroizing::new(args.password), args.remember)
        .await
        .map_err(|e| app_error(&state, e))
}
