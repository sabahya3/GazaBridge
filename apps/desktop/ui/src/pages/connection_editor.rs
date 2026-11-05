//! Connection editor: structured fields per engine, password handling,
//! draft testing and saving.

use gazabridge_ipc::{
    ConnectionDetails, ConnectionForm, EngineForm, MongoForm, MySqlForm, ParseStringArgs,
    ParsedStringDto, PasswordChoice, PasswordMode, PickPathArgs, PostgresForm, ProfileSummary,
    SaveConnectionArgs, SqliteForm, SshForm, TestResultDto, TlsForm, commands,
};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use crate::app::use_ui;
use crate::ipc::{NoArgs, call};

/// Value of the "New connection…" entry in connection lists.
pub const NEW_CONNECTION: &str = "__new_connection__";

/// "New connection…" entry for the end of a connection `<select>`.
pub fn new_connection_option() -> impl IntoView {
    let ui = use_ui();
    view! { <option value=NEW_CONNECTION>{format!("+ {}", ui.text("conn.new_here"))}</option> }
}

/// The connection editor in a dialog, for pages that pick a connection.
/// `on_created` receives the saved connection's id.
#[component]
pub fn NewConnectionDialog(open: RwSignal<bool>, on_created: Callback<String>) -> impl IntoView {
    let ui = use_ui();
    let profiles = RwSignal::new(None::<Result<Vec<ProfileSummary>, String>>);
    Effect::new(move |_| {
        if open.get() && profiles.get_untracked().is_none() {
            spawn_local(async move {
                profiles.set(Some(
                    call::<_, Vec<ProfileSummary>>(commands::LIST_PROFILES, &NoArgs {})
                        .await
                        .map_err(|e| e.message),
                ));
            });
        }
    });
    let close = Callback::new(move |()| open.set(false));
    let saved = Callback::new(move |id: String| {
        open.set(false);
        on_created.run(id);
    });
    move || {
        open.get().then(|| view! {
            <div class="modal-backdrop">
                <div class="modal conn-dialog" role="dialog" aria-modal="true" aria-labelledby="editor-title"
                    on:keydown=move |e| if e.key() == "Escape" { open.set(false) }>
                    {move || match profiles.get() {
                        None => view! { <p class="muted">{ui.tr("common.loading")}</p> }.into_any(),
                        Some(Err(e)) => view! {
                            <p class="error-detail">{e}</p>
                            <button type="button" class="btn btn-secondary" on:click=move |_| open.set(false)>{ui.tr("common.cancel")}</button>
                        }.into_any(),
                        Some(Ok(p)) => view! { <ConnectionEditor initial=None profiles=p on_done=close on_saved=saved /> }.into_any(),
                    }}
                </div>
            </div>
        })
    }
}

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

/// Read/write helpers for the engine-specific part of the form.
macro_rules! engine_access {
    ($get:ident, $set:ident, $variant:ident, $ty:ty) => {
        fn $get<T: Default>(form: RwSignal<ConnectionForm>, f: impl Fn(&$ty) -> T) -> T {
            form.with(|c| match &c.engine {
                EngineForm::$variant(e) => f(e),
                _ => T::default(),
            })
        }
        fn $set(form: RwSignal<ConnectionForm>, f: impl FnOnce(&mut $ty)) {
            form.update(|c| {
                if let EngineForm::$variant(e) = &mut c.engine {
                    f(e);
                }
            });
        }
    };
}

engine_access!(get_pg, set_pg, Postgres, PostgresForm);
engine_access!(get_my, set_my, MySql, MySqlForm);
engine_access!(get_mongo, set_mongo, MongoDb, MongoForm);
engine_access!(get_lite, set_lite, Sqlite, SqliteForm);

fn text_field(
    label: impl Fn() -> String + Send + Sync + Copy + 'static,
    value: impl Fn() -> String + Send + Sync + 'static,
    on_change: impl Fn(String) + 'static,
    input_type: &'static str,
) -> impl IntoView {
    view! {
        <label class="field">
            <span>{label}</span>
            <input class="input" type=input_type prop:value=value on:input=move |e| on_change(event_target_value(&e)) />
        </label>
    }
}

fn number_field(
    label: impl Fn() -> String + Send + Sync + Copy + 'static,
    value: impl Fn() -> u64 + Send + Sync + 'static,
    on_change: impl Fn(u64) + 'static,
) -> impl IntoView {
    view! {
        <label class="field">
            <span>{label}</span>
            <input
                class="input"
                type="number"
                min="0"
                prop:value=move || value().to_string()
                on:change=move |e| {
                    if let Ok(n) = event_target_value(&e).trim().parse::<u64>() {
                        on_change(n);
                    }
                }
            />
        </label>
    }
}

fn check_field(
    label: impl Fn() -> String + Send + Sync + Copy + 'static,
    value: impl Fn() -> bool + Send + Sync + 'static,
    on_change: impl Fn(bool) + 'static,
) -> impl IntoView {
    view! {
        <label class="field-check">
            <input type="checkbox" prop:checked=value on:change=move |e| on_change(event_target_checked(&e)) />
            <span>{label}</span>
        </label>
    }
}

fn select_field(
    label: impl Fn() -> String + Send + Sync + Copy + 'static,
    options: Vec<(&'static str, &'static str)>,
    value: impl Fn() -> String + Send + Sync + Clone + 'static,
    on_change: impl Fn(String) + 'static,
) -> impl IntoView {
    view! {
        <label class="field">
            <span>{label}</span>
            <select class="input" on:change=move |e| on_change(event_target_value(&e))>
                {options
                    .into_iter()
                    .map(|(v, text)| {
                        let value = value.clone();
                        view! { <option value=v selected=move || value() == v>{text}</option> }
                    })
                    .collect_view()}
            </select>
        </label>
    }
}

fn opt_string(s: String) -> Option<String> {
    let t = s.trim().to_owned();
    (!t.is_empty()).then_some(t)
}

fn postgres_fields(form: RwSignal<ConnectionForm>) -> impl IntoView {
    let ui = use_ui();
    view! {
        {text_field(ui.tr("editor.host"), move || get_pg(form, |p| p.host.clone()), move |v| set_pg(form, |p| p.host = v), "text")}
        {number_field(ui.tr("editor.port"), move || get_pg(form, |p| u64::from(p.port)), move |v| set_pg(form, |p| p.port = v.min(65_535) as u16))}
        {text_field(ui.tr("editor.database"), move || get_pg(form, |p| p.database.clone()), move |v| set_pg(form, |p| p.database = v), "text")}
        {text_field(ui.tr("editor.username"), move || get_pg(form, |p| p.username.clone()), move |v| set_pg(form, |p| p.username = v), "text")}
        {select_field(
            ui.tr("editor.ssl_mode"),
            vec![("verify-full", "verify-full"), ("verify-ca", "verify-ca"), ("require", "require"), ("prefer", "prefer"), ("disable", "disable")],
            move || get_pg(form, |p| p.ssl_mode.clone()),
            move |v| set_pg(form, |p| p.ssl_mode = v),
        )}
        {text_field(ui.tr("editor.search_path"), move || get_pg(form, |p| p.search_path.clone().unwrap_or_default()), move |v| set_pg(form, |p| p.search_path = opt_string(v)), "text")}
        {number_field(ui.tr("editor.statement_timeout"), move || get_pg(form, |p| p.statement_timeout_ms), move |v| set_pg(form, |p| p.statement_timeout_ms = v))}
        {number_field(ui.tr("editor.connect_timeout"), move || get_pg(form, |p| u64::from(p.connect_timeout_secs)), move |v| set_pg(form, |p| p.connect_timeout_secs = v.min(3600) as u32))}
        {number_field(ui.tr("editor.pool_size"), move || get_pg(form, |p| u64::from(p.pool_size)), move |v| set_pg(form, |p| p.pool_size = v.clamp(1, 1000) as u32))}
    }
}

fn mysql_fields(form: RwSignal<ConnectionForm>) -> impl IntoView {
    let ui = use_ui();
    view! {
        {text_field(ui.tr("editor.host"), move || get_my(form, |p| p.host.clone()), move |v| set_my(form, |p| p.host = v), "text")}
        {number_field(ui.tr("editor.port"), move || get_my(form, |p| u64::from(p.port)), move |v| set_my(form, |p| p.port = v.min(65_535) as u16))}
        {text_field(ui.tr("editor.database"), move || get_my(form, |p| p.database.clone().unwrap_or_default()), move |v| set_my(form, |p| p.database = opt_string(v)), "text")}
        {text_field(ui.tr("editor.username"), move || get_my(form, |p| p.username.clone()), move |v| set_my(form, |p| p.username = v), "text")}
        {select_field(
            ui.tr("editor.tls_mode"),
            vec![("verify-identity", "VERIFY_IDENTITY"), ("verify-ca", "VERIFY_CA"), ("required", "REQUIRED"), ("preferred", "PREFERRED"), ("disabled", "DISABLED")],
            move || get_my(form, |p| p.tls_mode.clone()),
            move |v| set_my(form, |p| p.tls_mode = v),
        )}
        {text_field(ui.tr("editor.charset"), move || get_my(form, |p| p.charset.clone()), move |v| set_my(form, |p| p.charset = v), "text")}
        {number_field(ui.tr("editor.pool_size"), move || get_my(form, |p| u64::from(p.pool_size)), move |v| set_my(form, |p| p.pool_size = v.clamp(1, 1000) as u32))}
    }
}

fn mongo_fields(form: RwSignal<ConnectionForm>) -> impl IntoView {
    let ui = use_ui();
    view! {
        {move || get_mongo(form, |p| p.uri.clone()).map(|uri| view! {
            <div class="field">
                <span>{ui.tr("editor.uri")}</span>
                <code class="mono small uri-readonly"><bdi>{uri}</bdi></code>
                <span class="muted small">{ui.tr("editor.uri_help")}</span>
                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| set_mongo(form, |p| p.uri = None)>{ui.tr("editor.use_hosts")}</button>
            </div>
        })}
        {text_field(
            ui.tr("editor.hosts"),
            move || get_mongo(form, |p| p.hosts.join(", ")),
            move |v| set_mongo(form, |p| p.hosts = v.split(',').map(|h| h.trim().to_owned()).filter(|h| !h.is_empty()).collect()),
            "text",
        )}
        {text_field(ui.tr("editor.username"), move || get_mongo(form, |p| p.username.clone().unwrap_or_default()), move |v| set_mongo(form, |p| p.username = opt_string(v)), "text")}
        {text_field(ui.tr("editor.auth_source"), move || get_mongo(form, |p| p.auth_source.clone().unwrap_or_default()), move |v| set_mongo(form, |p| p.auth_source = opt_string(v)), "text")}
        {text_field(ui.tr("editor.default_database"), move || get_mongo(form, |p| p.default_database.clone().unwrap_or_default()), move |v| set_mongo(form, |p| p.default_database = opt_string(v)), "text")}
        {text_field(ui.tr("editor.replica_set"), move || get_mongo(form, |p| p.replica_set.clone().unwrap_or_default()), move |v| set_mongo(form, |p| p.replica_set = opt_string(v)), "text")}
        {select_field(
            ui.tr("editor.read_preference"),
            vec![("primary", "primary"), ("primary_preferred", "primaryPreferred"), ("secondary", "secondary"), ("secondary_preferred", "secondaryPreferred"), ("nearest", "nearest")],
            move || get_mongo(form, |p| p.read_preference.clone()),
            move |v| set_mongo(form, |p| p.read_preference = v),
        )}
        {check_field(ui.tr("editor.tls"), move || get_mongo(form, |p| p.tls), move |v| set_mongo(form, |p| p.tls = v))}
        {check_field(ui.tr("editor.direct_connection"), move || get_mongo(form, |p| p.direct_connection), move |v| set_mongo(form, |p| p.direct_connection = v))}
    }
}

fn sqlite_fields(form: RwSignal<ConnectionForm>) -> impl IntoView {
    let ui = use_ui();
    view! {
        {text_field(ui.tr("editor.file_path"), move || get_lite(form, |p| p.path.clone()), move |v| set_lite(form, |p| p.path = v), "text")}
        {check_field(ui.tr("editor.create_if_missing"), move || get_lite(form, |p| p.create_if_missing), move |v| set_lite(form, |p| p.create_if_missing = v))}
        {select_field(
            ui.tr("editor.journal_mode"),
            vec![("wal", "WAL"), ("delete", "DELETE"), ("truncate", "TRUNCATE"), ("persist", "PERSIST"), ("memory", "MEMORY"), ("off", "OFF")],
            move || get_lite(form, |p| p.journal_mode.clone()),
            move |v| set_lite(form, |p| p.journal_mode = v),
        )}
        {check_field(ui.tr("editor.foreign_keys"), move || get_lite(form, |p| p.foreign_keys), move |v| set_lite(form, |p| p.foreign_keys = v))}
        {number_field(ui.tr("editor.busy_timeout"), move || get_lite(form, |p| p.busy_timeout_ms), move |v| set_lite(form, |p| p.busy_timeout_ms = v))}
    }
}

fn get_ssh<T: Default>(form: RwSignal<ConnectionForm>, f: impl Fn(&SshForm) -> T) -> T {
    form.with(|c| c.ssh.as_ref().map(f).unwrap_or_default())
}

fn set_ssh(form: RwSignal<ConnectionForm>, f: impl FnOnce(&mut SshForm)) {
    form.update(|c| {
        if let Some(s) = c.ssh.as_mut() {
            f(s);
        }
    });
}

/// The SSH tunnel's secret choice. An empty password or passphrase means
/// there is none (a key without a passphrase).
fn ssh_choice(kind: &str, value: String, env: String) -> PasswordChoice {
    match kind {
        "remember" | "session" if value.is_empty() => PasswordChoice::None,
        "remember" => PasswordChoice::Remember { password: value },
        "session" => PasswordChoice::SessionOnly { password: value },
        "env" => PasswordChoice::FromEnv { var: env },
        "none" => PasswordChoice::None,
        _ => PasswordChoice::Keep,
    }
}

/// SSH tunnel settings: server, user, sign-in method and host key policy.
fn ssh_fields(
    form: RwSignal<ConnectionForm>,
    kind: RwSignal<String>,
    value: RwSignal<String>,
    env: RwSignal<String>,
    existing: Option<PasswordMode>,
) -> impl IntoView {
    let ui = use_ui();
    let has_existing = existing.is_some();
    let browse = move |_| {
        let pick = PickPathArgs {
            save: false,
            default_name: None,
            extensions: Vec::new(),
        };
        spawn_local(async move {
            if let Ok(Some(path)) =
                call::<_, Option<String>>(commands::PICK_FILE_PATH, &Wrap { args: &pick }).await
            {
                set_ssh(form, |s| s.key_path = Some(path));
            }
        });
    };
    view! {
        {text_field(ui.tr("editor.ssh_host"), move || get_ssh(form, |s| s.host.clone()), move |v| set_ssh(form, |s| s.host = v), "text")}
        {number_field(ui.tr("editor.port"), move || get_ssh(form, |s| u64::from(s.port)), move |v| set_ssh(form, |s| s.port = u16::try_from(v).unwrap_or(22)))}
        {text_field(ui.tr("editor.ssh_user"), move || get_ssh(form, |s| s.username.clone()), move |v| set_ssh(form, |s| s.username = v), "text")}
        <label class="field">
            <span>{ui.tr("editor.ssh_auth")}</span>
            <select class="input" prop:value=move || get_ssh(form, |s| s.auth.clone()) on:change=move |e| set_ssh(form, |s| s.auth = event_target_value(&e))>
                <option value="key">{ui.tr("editor.ssh_auth_key")}</option>
                <option value="agent">{ui.tr("editor.ssh_auth_agent")}</option>
                <option value="password">{ui.tr("editor.ssh_auth_password")}</option>
            </select>
        </label>
        <Show when=move || get_ssh(form, |s| s.auth == "key")>
            <div class="field-row">
                {text_field(ui.tr("editor.ssh_key_path"), move || get_ssh(form, |s| s.key_path.clone().unwrap_or_default()), move |v| set_ssh(form, |s| s.key_path = opt_string(v)), "text")}
                <button type="button" class="btn btn-secondary" on:click=browse>{ui.tr("editor.choose_file")}</button>
            </div>
            <p class="muted small">{ui.tr("editor.ssh_key_note")}</p>
        </Show>
        <Show when=move || get_ssh(form, |s| s.auth != "agent")>
            {existing.clone().map(|m| view! {
                <p class="muted small">{ui.tr("editor.current_storage")}": "{ui.text(mode_key(&m))}</p>
            })}
            <label class="field">
                <span>{move || if get_ssh(form, |s| s.auth == "password") { ui.text("editor.ssh_password") } else { ui.text("editor.ssh_passphrase") }}</span>
                <select class="input" prop:value=move || kind.get() on:change=move |e| kind.set(event_target_value(&e))>
                    {has_existing.then(|| view! { <option value="keep">{ui.tr("editor.pw_keep")}</option> })}
                    <option value="remember">{ui.tr("editor.pw_remember")}</option>
                    <option value="session">{ui.tr("editor.pw_session")}</option>
                    <option value="env">{ui.tr("editor.pw_env")}</option>
                    <option value="none">{ui.tr("editor.pw_none")}</option>
                </select>
            </label>
            <Show when=move || matches!(kind.get().as_str(), "remember" | "session")>
                <label class="field">
                    <span class="sr-only">{move || if get_ssh(form, |s| s.auth == "password") { ui.text("editor.ssh_password") } else { ui.text("editor.ssh_passphrase") }}</span>
                    <input class="input" type="password" autocomplete="off"
                        placeholder=move || if get_ssh(form, |s| s.auth == "key") { ui.text("editor.ssh_passphrase_empty") } else { String::new() }
                        prop:value=move || value.get() on:input=move |e| value.set(event_target_value(&e)) />
                </label>
            </Show>
            <Show when=move || kind.get() == "env">
                {text_field(ui.tr("editor.env_var"), move || env.get(), move |v| env.set(v), "text")}
            </Show>
        </Show>
        <label class="field">
            <span>{ui.tr("editor.ssh_host_key")}</span>
            <select class="input" prop:value=move || get_ssh(form, |s| s.host_key_policy.clone()) on:change=move |e| set_ssh(form, |s| s.host_key_policy = event_target_value(&e))>
                <option value="strict">{ui.tr("editor.ssh_strict")}</option>
                <option value="accept_new">{ui.tr("editor.ssh_accept_new")}</option>
            </select>
        </label>
        <p class="muted small">{move || if get_ssh(form, |s| s.host_key_policy == "strict") { ui.text("editor.ssh_strict_note") } else { ui.text("editor.ssh_accept_new_note") }}</p>
    }
}

fn mode_key(mode: &PasswordMode) -> &'static str {
    match mode {
        PasswordMode::None => "editor.pw_none",
        PasswordMode::Keychain => "editor.stored_keychain",
        PasswordMode::Session => "editor.pw_session",
        PasswordMode::Env { .. } => "editor.pw_env",
        PasswordMode::Prompt => "editor.pw_ask",
    }
}

const COLORS: [(&str, &str); 7] = [
    ("", "editor.color_none"),
    ("#2e7d32", "editor.color_green"),
    ("#1565c0", "editor.color_blue"),
    ("#c62828", "editor.color_red"),
    ("#ef6c00", "editor.color_orange"),
    ("#6a1b9a", "editor.color_purple"),
    ("#00838f", "editor.color_teal"),
];

fn family_label(family: &str) -> &'static str {
    match family {
        "postgres" => "PostgreSQL",
        "mysql" => "MySQL",
        "mongodb" => "MongoDB",
        _ => "SQLite",
    }
}

fn default_profile(family: &str) -> &'static str {
    match family {
        "postgres" => "postgres.postgresql",
        "mysql" => "mysql.mysql",
        "mongodb" => "mongodb.community",
        _ => "sqlite.sqlite",
    }
}

/// Editor for a new (`initial = None`) or existing connection.
#[component]
pub fn ConnectionEditor(
    initial: Option<ConnectionDetails>,
    profiles: Vec<ProfileSummary>,
    on_done: Callback<()>,
    /// Set when the editor opens from another page: called with the new
    /// connection's id, and "Save and connect" is not offered.
    #[prop(optional)]
    on_saved: Option<Callback<String>>,
) -> impl IntoView {
    let ui = use_ui();
    let is_new = initial.is_none();
    let existing_mode = initial.as_ref().map(|d| d.password_mode.clone());
    let existing_ssh_mode = initial.as_ref().and_then(|d| d.ssh_secret_mode.clone());
    let ssh_kind = RwSignal::new(if existing_ssh_mode.is_some() {
        "keep".to_owned()
    } else {
        "remember".to_owned()
    });
    let existing_ssh = StoredValue::new(existing_ssh_mode.clone());
    let ssh_value = RwSignal::new(String::new());
    let ssh_env = RwSignal::new(match &existing_ssh_mode {
        Some(PasswordMode::Env { var }) => var.clone(),
        _ => String::new(),
    });
    let start = initial.map(|d| d.form).unwrap_or_else(|| ConnectionForm {
        id: None,
        name: String::new(),
        profile_id: "postgres.postgresql".into(),
        engine: EngineForm::default_for("postgres").unwrap_or(EngineForm::Sqlite(SqliteForm {
            path: String::new(),
            read_only: false,
            create_if_missing: false,
            busy_timeout_ms: 5_000,
            journal_mode: "wal".into(),
            foreign_keys: true,
        })),
        tls: TlsForm::default(),
        ssh: None,
        read_only: false,
        favorite: false,
        color: None,
        tags: Vec::new(),
        folder_id: None,
    });
    let form = RwSignal::new(start);
    // "keep" leaves an existing secret untouched.
    let pw_kind = RwSignal::new(if is_new {
        "remember".to_owned()
    } else {
        "keep".to_owned()
    });
    let pw_value = RwSignal::new(String::new());
    let env_var = RwSignal::new(match &existing_mode {
        Some(PasswordMode::Env { var }) => var.clone(),
        _ => String::new(),
    });
    let test = RwSignal::new(None::<Result<TestResultDto, String>>);
    let busy = RwSignal::new(false);
    let testing = RwSignal::new(false);
    let error = RwSignal::new(None::<String>);
    // The connection string box: parsed into the form; its password is sent
    // again on test/save so the backend can store it securely.
    let uri_text = RwSignal::new(String::new());
    let uri_note = RwSignal::new(None::<Result<(String, bool), String>>);

    let families: Vec<&'static str> = ["postgres", "mysql", "mongodb", "sqlite"]
        .into_iter()
        .filter(|f| {
            profiles
                .iter()
                .any(|p| p.family == *f && p.connector_available)
        })
        .collect();
    let profiles = StoredValue::new(profiles);

    let choice = move || -> PasswordChoice {
        let value = pw_value.get_untracked();
        match pw_kind.get_untracked().as_str() {
            "none" => PasswordChoice::None,
            "remember" => PasswordChoice::Remember { password: value },
            "session" => PasswordChoice::SessionOnly { password: value },
            "env" => PasswordChoice::FromEnv {
                var: env_var.get_untracked(),
            },
            "ask" => PasswordChoice::AskEachTime,
            _ => PasswordChoice::Keep,
        }
    };

    let ssh_secret = move || {
        ssh_choice(
            &ssh_kind.get_untracked(),
            ssh_value.get_untracked(),
            ssh_env.get_untracked(),
        )
    };
    // SQLite files are opened directly, so a tunnel left from another
    // engine is not sent.
    let form_to_send = move || {
        let mut f = form.get_untracked();
        if f.engine.family() == "sqlite" {
            f.ssh = None;
        }
        f
    };

    let connection_string = move || {
        let t = uri_text.get_untracked();
        (!t.trim().is_empty()).then_some(t)
    };

    let parse_uri = move || {
        let text = uri_text.get_untracked();
        if text.trim().is_empty() {
            uri_note.set(None);
            return;
        }
        spawn_local(async move {
            let args = ParseStringArgs { text };
            match call::<_, ParsedStringDto>(
                commands::PARSE_CONNECTION_STRING,
                &Wrap { args: &args },
            )
            .await
            {
                Ok(p) => {
                    let family = p.form.engine.family().to_owned();
                    form.update(|f| {
                        f.engine = p.form.engine;
                        f.profile_id = p.form.profile_id;
                        f.tls = p.form.tls;
                        if f.name.trim().is_empty() {
                            f.name = p.suggested_name;
                        }
                    });
                    uri_note.set(Some(Ok((family, p.has_password))));
                }
                Err(e) => uri_note.set(Some(Err(e.message))),
            }
        });
    };

    let run_test = move |_| {
        busy.set(true);
        testing.set(true);
        test.set(None);
        let args = SaveConnectionArgs {
            form: form_to_send(),
            password: choice(),
            connection_string: connection_string(),
            ssh_secret: ssh_secret(),
        };
        spawn_local(async move {
            let r =
                call::<_, TestResultDto>(commands::TEST_CONNECTION_DRAFT, &Wrap { args: &args })
                    .await;
            test.set(Some(r.map_err(|e| e.message)));
            busy.set(false);
            testing.set(false);
        });
    };

    let save = move |open_after: bool| {
        busy.set(true);
        error.set(None);
        let args = SaveConnectionArgs {
            form: form_to_send(),
            password: choice(),
            connection_string: connection_string(),
            ssh_secret: ssh_secret(),
        };
        spawn_local(async move {
            match call::<_, ConnectionDetails>(commands::SAVE_CONNECTION, &Wrap { args: &args })
                .await
            {
                Ok(saved) => {
                    if let (Some(cb), Some(id)) = (on_saved, saved.form.id.clone()) {
                        cb.run(id);
                    }
                    if open_after {
                        ui.open_connection.set(saved.form.id.clone());
                        ui.section.set(crate::nav::Section::Explorer);
                    }
                    on_done.run(());
                }
                Err(e) => error.set(Some(e.message)),
            }
            busy.set(false);
        });
    };

    let family = move || form.with(|f| f.engine.family());

    view! {
        <section class="card editor" aria-labelledby="editor-title">
            <h2 id="editor-title">{if is_new { ui.tr("editor.new_title") } else { ui.tr("editor.edit_title") }}</h2>

            <label class="field field-wide">
                <span>{ui.tr("editor.connection_string")}</span>
                <textarea
                    class="input mono uri-input"
                    rows="3"
                    spellcheck="false"
                    autocomplete="off"
                    placeholder="mongodb://user:password@localhost:27017/mydb"
                    prop:value=move || uri_text.get()
                    on:input=move |e| uri_text.set(event_target_value(&e))
                    on:change=move |_| parse_uri()
                    on:paste=move |_| set_timeout(parse_uri, std::time::Duration::from_millis(0))
                ></textarea>
                {move || match uri_note.get() {
                    None => view! { <span class="muted small">{if is_new { ui.tr("editor.connection_string_help") } else { ui.tr("editor.connection_string_replace") }}</span> }.into_any(),
                    Some(Ok((family, has_password))) => view! {
                        <span class="notice-ok small">
                            {format!("{} {}", ui.text("editor.detected"), family_label(&family))}
                            {has_password.then(|| format!(" · {}", ui.text("editor.password_moved")))}
                        </span>
                    }.into_any(),
                    Some(Err(e)) => view! { <span class="error-detail small">{e}</span> }.into_any(),
                }}
            </label>

            <div class="field-row">
                {text_field(ui.tr("editor.name"), move || form.with(|f| f.name.clone()), move |v| form.update(|f| f.name = v), "text")}
                <label class="field">
                    <span>{ui.tr("editor.color")}</span>
                    <select class="input" on:change=move |e| { let v = event_target_value(&e); form.update(|f| f.color = (!v.is_empty()).then_some(v)); }>
                        {COLORS.iter().map(|(value, key)| {
                            let value = *value;
                            view! { <option value=value selected=move || form.with(|f| f.color.as_deref().unwrap_or("") == value)>{ui.tr(key)}</option> }
                        }).collect_view()}
                    </select>
                </label>
            </div>
            {check_field(ui.tr("connections.favorite"), move || form.with(|f| f.favorite), move |v| form.update(|f| f.favorite = v))}

            <details class="advanced">
            <summary>{ui.tr("editor.advanced")}</summary>

            <label class="field">
                <span>{ui.tr("editor.engine")}</span>
                <select
                    class="input"
                    disabled=!is_new
                    on:change=move |e| {
                        let fam = event_target_value(&e);
                        if let Some(engine) = EngineForm::default_for(&fam) {
                            form.update(|f| {
                                f.engine = engine;
                                f.profile_id = default_profile(&fam).into();
                            });
                        }
                    }
                >
                    {families
                        .into_iter()
                        .map(|f| view! { <option value=f selected=move || family() == f>{f}</option> })
                        .collect_view()}
                </select>
            </label>

            <label class="field">
                <span>{ui.tr("editor.profile")}</span>
                <select class="input" on:change=move |e| { let v = event_target_value(&e); form.update(|f| f.profile_id = v); }>
                    {move || {
                        let fam = family();
                        profiles
                            .get_value()
                            .into_iter()
                            .filter(|p| p.family == fam)
                            .map(|p| {
                                let id = p.id.clone();
                                let selected = form.with(|f| f.profile_id == id);
                                view! { <option value=p.id.clone() selected=selected>{format!("{} — {}", p.display_name, p.status)}</option> }
                            })
                            .collect_view()
                    }}
                </select>
            </label>

            <fieldset class="field-group">
                <legend>{ui.tr("editor.connection")}</legend>
                {move || match family() {
                    "postgres" => postgres_fields(form).into_any(),
                    "mysql" => mysql_fields(form).into_any(),
                    "mongodb" => mongo_fields(form).into_any(),
                    _ => sqlite_fields(form).into_any(),
                }}
            </fieldset>

            <Show when=move || family() != "sqlite">
                <fieldset class="field-group">
                    <legend>{ui.tr("editor.password")}</legend>
                    {existing_mode.clone().map(|m| view! {
                        <p class="muted small">{ui.tr("editor.current_storage")}": "{ui.text(mode_key(&m))}</p>
                    })}
                    <select class="input" on:change=move |e| pw_kind.set(event_target_value(&e))>
                        {(!is_new).then(|| view! { <option value="keep" selected=move || pw_kind.get() == "keep">{ui.tr("editor.pw_keep")}</option> })}
                        <option value="remember" selected=move || pw_kind.get() == "remember">{ui.tr("editor.pw_remember")}</option>
                        <option value="session" selected=move || pw_kind.get() == "session">{ui.tr("editor.pw_session")}</option>
                        <option value="ask" selected=move || pw_kind.get() == "ask">{ui.tr("editor.pw_ask")}</option>
                        <option value="env" selected=move || pw_kind.get() == "env">{ui.tr("editor.pw_env")}</option>
                        <option value="none" selected=move || pw_kind.get() == "none">{ui.tr("editor.pw_none")}</option>
                    </select>
                    <Show when=move || matches!(pw_kind.get().as_str(), "remember" | "session")>
                        <label class="field">
                            <span>{ui.tr("editor.password")}</span>
                            <input
                                class="input"
                                type="password"
                                autocomplete="off"
                                prop:value=move || pw_value.get()
                                on:input=move |e| pw_value.set(event_target_value(&e))
                            />
                        </label>
                    </Show>
                    <Show when=move || pw_kind.get() == "env">
                        {text_field(ui.tr("editor.env_var"), move || env_var.get(), move |v| env_var.set(v), "text")}
                    </Show>
                </fieldset>

                <details class="field-group">
                    <summary>{ui.tr("editor.tls_files")}</summary>
                    {text_field(ui.tr("editor.ca_certificate"), move || form.with(|f| f.tls.ca_certificate.clone().unwrap_or_default()), move |v| form.update(|f| f.tls.ca_certificate = opt_string(v)), "text")}
                    {text_field(ui.tr("editor.client_certificate"), move || form.with(|f| f.tls.client_certificate.clone().unwrap_or_default()), move |v| form.update(|f| f.tls.client_certificate = opt_string(v)), "text")}
                    {text_field(ui.tr("editor.client_key"), move || form.with(|f| f.tls.client_key.clone().unwrap_or_default()), move |v| form.update(|f| f.tls.client_key = opt_string(v)), "text")}
                </details>

                <details class="field-group ssh-group" open=form.with_untracked(|f| f.ssh.is_some())>
                    <summary>{ui.tr("editor.ssh")}</summary>
                    <p class="muted small">{ui.tr("editor.ssh_intro")}</p>
                    {check_field(
                        ui.tr("editor.ssh_use"),
                        move || form.with(|f| f.ssh.is_some()),
                        move |on| form.update(|f| f.ssh = on.then(|| f.ssh.clone().unwrap_or_default())),
                    )}
                    <Show when=move || form.with(|f| f.ssh.is_some())>
                        {ssh_fields(form, ssh_kind, ssh_value, ssh_env, existing_ssh.get_value())}
                    </Show>
                </details>
            </Show>

            <fieldset class="field-group">
                <legend>{ui.tr("editor.organisation")}</legend>
                {check_field(ui.tr("connections.read_only"), move || form.with(|f| f.read_only), move |v| form.update(|f| f.read_only = v))}
                {text_field(
                    ui.tr("editor.tags"),
                    move || form.with(|f| f.tags.join(", ")),
                    move |v| form.update(|f| f.tags = v.split(',').map(|t| t.trim().to_owned()).filter(|t| !t.is_empty()).collect()),
                    "text",
                )}
            </fieldset>

            </details>

            <div class="toolbar editor-actions">
                <button type="button" class="btn btn-secondary" on:click=move |_| on_done.run(())>{ui.tr("common.cancel")}</button>
                <span class="spacer"></span>
                <button type="button" class="btn btn-secondary" disabled=move || busy.get() aria-busy=move || testing.get().to_string() on:click=run_test>
                    {move || if testing.get() { ui.text("editor.testing") } else { ui.text("editor.test") }}
                </button>
                <button type="button" class="btn btn-secondary" disabled=move || busy.get() on:click=move |_| save(false)>{ui.tr("editor.save")}</button>
                {on_saved.is_none().then(|| view! {
                    <button type="button" class="btn" disabled=move || busy.get() on:click=move |_| save(true)>{ui.tr("editor.save_connect")}</button>
                })}
            </div>

            <div role="status" aria-live="polite">
                {move || error.get().map(|e| view! { <p class="error-detail">{e}</p> })}
                {move || test.get().map(|r| match r {
                    Ok(t) => view! { <TestResultView result=t /> }.into_any(),
                    Err(e) => view! { <p class="error-detail">{e}</p> }.into_any(),
                })}
            </div>
        </section>
    }
}

/// Renders the outcome of a connection test.
#[component]
pub fn TestResultView(result: TestResultDto) -> impl IntoView {
    let ui = use_ui();
    let ok = result.state == "ready";
    let class = if ok {
        "test-result ok"
    } else {
        "test-result fail"
    };
    let summary = match (
        &result.server_product,
        &result.server_version,
        result.latency_ms,
    ) {
        (Some(p), Some(v), Some(ms)) => format!("{p} {v} · {ms} ms"),
        _ => String::new(),
    };
    view! {
        <div class=class>
            <strong>{result.state_label.clone()}</strong>
            <span>{summary}</span>
            {result.status.clone().map(|s| view! { <span class="badge">{s}</span> })}
            {result.detected_profile.clone().map(|d| view! { <span class="muted small">{ui.tr("editor.detected")}": "{d}</span> })}
            {result.error.clone().map(|e| {
                // Plain-language summary first; the driver's text on demand.
                let (summary, details) = match e.split_once("\n\nDetails: ") {
                    Some((s, d)) => (s.to_owned(), Some(d.to_owned())),
                    None => (e, None),
                };
                view! {
                    <p class="error-summary">{summary}</p>
                    {details.map(|d| view! {
                        <details><summary class="small">{ui.tr("connections.error_details")}</summary><p class="error-detail">{d}</p></details>
                    })}
                }
            })}
            <ul>
                {result.findings.iter().map(|f| view! { <li class=format!("finding finding-{}", f.severity)>{f.message.clone()}</li> }).collect_view()}
            </ul>
        </div>
    }
}
