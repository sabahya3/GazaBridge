//! Application shell: header, navigation, command palette and page routing.

use gazabridge_ipc::{Preferences, Theme, commands};
use leptos::ev;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::i18n::t;
use crate::ipc::{NoArgs, call};
use crate::nav::Section;
use crate::pages::{
    BackupsPage, ConnectionsPage, ExplorerPage, FilesPage, HomePage, LogsPage, MigrationsPage,
    MonitorPage, QueryStudioPage, ReportsPage, SettingsPage, SyncJobsPage, TransferDirection,
};

/// Shared UI state, provided as context.
#[derive(Clone, Copy)]
pub struct Ui {
    pub prefs: RwSignal<Preferences>,
    pub section: RwSignal<Section>,
    /// Connection the Explorer should open next ("Save & Connect").
    pub open_connection: RwSignal<Option<String>>,
}

impl Ui {
    pub fn tr(self, key: &'static str) -> impl Fn() -> String + Copy + 'static {
        let prefs = self.prefs;
        move || t(prefs.with(|p| p.locale), key)
    }

    pub fn text(&self, key: &str) -> String {
        t(self.prefs.with_untracked(|p| p.locale), key)
    }
}

pub fn use_ui() -> Ui {
    use_context::<Ui>().unwrap_or_else(|| Ui {
        prefs: RwSignal::new(Preferences::default()),
        section: RwSignal::new(Section::Home),
        open_connection: RwSignal::new(None),
    })
}

/// Reflects preferences on the `<html>` element: theme, contrast, language
/// and text direction.
fn apply_document_preferences(p: &Preferences) {
    let Some(root) = document().document_element() else {
        return;
    };
    let theme = match p.theme {
        Theme::System => "system",
        Theme::Light => "light",
        Theme::Dark => "dark",
    };
    let _ = root.set_attribute("data-theme", theme);
    let _ = root.set_attribute(
        "data-contrast",
        if p.high_contrast { "high" } else { "normal" },
    );
    let _ = root.set_attribute("lang", p.locale.code());
    let _ = root.set_attribute("dir", if p.locale.is_rtl() { "rtl" } else { "ltr" });
}

#[component]
pub fn App() -> impl IntoView {
    let ui = Ui {
        prefs: RwSignal::new(Preferences::default()),
        section: RwSignal::new(Section::Home),
        open_connection: RwSignal::new(None),
    };
    provide_context(ui);

    spawn_local(async move {
        if let Ok(p) = call::<_, Preferences>(commands::GET_PREFERENCES, &NoArgs {}).await {
            ui.prefs.set(p);
        }
    });

    Effect::new(move |_| ui.prefs.with(apply_document_preferences));

    let palette_open = RwSignal::new(false);
    let _keys = window_event_listener(ev::keydown, move |e| {
        let modifier = e.ctrl_key() || e.meta_key();
        if modifier && e.key().eq_ignore_ascii_case("k") {
            e.prevent_default();
            palette_open.update(|o| *o = !*o);
        } else if e.key() == "Escape" {
            palette_open.set(false);
        }
    });

    view! {
        <div class="shell" class:nav-collapsed=move || ui.prefs.with(|p| p.sidebar_collapsed)>
            <a class="skip-link" href="#main">{ui.tr("nav.primary")}</a>
            <Header palette_open=palette_open />
            <Sidebar />
            <main id="main" class="content" tabindex="-1">
                {move || match ui.section.get() {
                    Section::Connections => view! { <ConnectionsPage /> }.into_any(),
                    Section::Explorer => view! { <ExplorerPage /> }.into_any(),
                    Section::QueryStudio => view! { <QueryStudioPage /> }.into_any(),
                    Section::Migrations => view! { <MigrationsPage /> }.into_any(),
                    Section::Backups => view! { <BackupsPage /> }.into_any(),
                    Section::SyncJobs => view! { <SyncJobsPage /> }.into_any(),
                    Section::Monitor => view! { <MonitorPage /> }.into_any(),
                    Section::Imports => view! { <FilesPage direction=TransferDirection::Import /> }.into_any(),
                    Section::Exports => view! { <FilesPage direction=TransferDirection::Export /> }.into_any(),
                    Section::Reports => view! { <ReportsPage /> }.into_any(),
                    Section::Logs => view! { <LogsPage /> }.into_any(),
                    Section::Settings => view! { <SettingsPage /> }.into_any(),
                    _ => view! { <HomePage /> }.into_any(),
                }}
            </main>
            <Show when=move || palette_open.get()>
                <CommandPalette open=palette_open />
            </Show>
            <PasswordDialog />
        </div>
    }
}

#[derive(serde::Serialize)]
struct UnlockWrap<'a> {
    args: &'a gazabridge_ipc::UnlockArgs,
}

/// Asks for a connection's password when a command needs it, then lets the
/// waiting commands retry.
#[component]
fn PasswordDialog() -> impl IntoView {
    let ui = use_ui();
    let ask = RwSignal::new(None::<(String, String)>);
    let waiters = StoredValue::new(Vec::<futures_channel::oneshot::Sender<bool>>::new());
    let password = RwSignal::new(String::new());
    let remember = RwSignal::new(true);
    let error = RwSignal::new(None::<String>);
    let busy = RwSignal::new(false);
    crate::ipc::set_password_prompt(Box::new(move |id, message, tx| {
        waiters.update_value(|w| w.push(tx));
        if ask.get_untracked().is_none() {
            password.set(String::new());
            error.set(None);
            ask.set(Some((id, message)));
        }
    }));
    let finish = move |ok: bool| {
        waiters.update_value(|w| {
            for tx in w.drain(..) {
                let _ = tx.send(ok);
            }
        });
        password.set(String::new());
        ask.set(None);
    };
    let unlock = move || {
        let Some((id, _)) = ask.get_untracked() else {
            return;
        };
        let args = gazabridge_ipc::UnlockArgs {
            connection_id: id,
            password: password.get_untracked(),
            remember: remember.get_untracked(),
        };
        busy.set(true);
        spawn_local(async move {
            match call::<_, bool>(commands::UNLOCK_CONNECTION, &UnlockWrap { args: &args }).await {
                Ok(_) => finish(true),
                Err(e) => error.set(Some(e.message)),
            }
            busy.set(false);
        });
    };
    move || {
        ask.get().map(|(_, message)| {
            view! {
                <div class="modal-backdrop">
                    <div class="modal pw-dialog" role="dialog" aria-modal="true" aria-labelledby="pw-title">
                        <h3 id="pw-title">{ui.tr("pw.title")}</h3>
                        <p class="small">{message}</p>
                        <form on:submit=move |e| { e.prevent_default(); unlock(); }>
                            <label class="field">
                                <span>{ui.tr("pw.password")}</span>
                                <input class="input" type="password" autocomplete="current-password" autofocus
                                    prop:value=move || password.get() on:input=move |e| password.set(event_target_value(&e)) />
                            </label>
                            <label class="field-check">
                                <input type="checkbox" prop:checked=move || remember.get() on:change=move |e| remember.set(event_target_checked(&e)) />
                                <span>{ui.tr("pw.remember")}</span>
                            </label>
                            {move || error.get().map(|e| view! { <p class="error-detail">{e}</p> })}
                            <div class="toolbar modal-footer">
                                <button type="submit" class="btn" disabled=move || busy.get() || password.get().is_empty()>{ui.tr("pw.unlock")}</button>
                                <button type="button" class="btn btn-secondary" on:click=move |_| finish(false)>{ui.tr("common.cancel")}</button>
                            </div>
                        </form>
                    </div>
                </div>
            }
        })
    }
}

/// Collapses or expands the main navigation and remembers the choice.
fn toggle_sidebar(ui: Ui) {
    #[derive(serde::Serialize)]
    struct Args {
        preferences: Preferences,
    }
    ui.prefs
        .update(|p| p.sidebar_collapsed = !p.sidebar_collapsed);
    let preferences = ui.prefs.get_untracked();
    spawn_local(async move {
        let _ = call::<_, ()>(commands::SET_PREFERENCES, &Args { preferences }).await;
    });
}

#[component]
fn Header(palette_open: RwSignal<bool>) -> impl IntoView {
    let ui = use_ui();
    view! {
        <header class="header">
            <div class="brand">
                <button type="button" class="icon-btn" aria-label=ui.tr("nav.toggle") title=ui.tr("nav.toggle")
                    aria-expanded=move || (!ui.prefs.with(|p| p.sidebar_collapsed)).to_string()
                    on:click=move |_| toggle_sidebar(ui)>
                    {crate::icons::icon(crate::icons::Icon::PanelLeft)}
                </button>
                <img class="brand-mark" src="assets/logo-mark.svg" alt="" width="28" height="28" />
                <div class="brand-text">
                    <span class="brand-name">{ui.tr("app.name")}</span>
                    <span class="brand-tagline">{ui.tr("app.tagline")}</span>
                </div>
            </div>
            <button
                class="palette-trigger"
                type="button"
                aria-haspopup="dialog"
                aria-keyshortcuts="Control+K Meta+K"
                on:click=move |_| palette_open.set(true)
            >
                <span>{ui.tr("palette.open")}</span>
                <kbd>{ui.tr("palette.hint")}</kbd>
            </button>
        </header>
    }
}

#[component]
fn Sidebar() -> impl IntoView {
    let ui = use_ui();
    let item = move |s: Section| {
        let available = s.is_available();
        let label = ui.tr(s.label_key());
        let unavailable = ui.tr("nav.not_available");
        view! {
            <li>
                <button
                    type="button"
                    class="nav-item"
                    class:active=move || ui.section.get() == s
                    aria-current=move || (ui.section.get() == s).then_some("page")
                    disabled=!available
                    title=move || if available { label() } else { format!("{} — {}", label(), unavailable()) }
                    on:click=move |_| ui.section.set(s)
                >
                    <span class="nav-icon">{crate::icons::icon(s.icon())}</span>
                    <span class="nav-label">{label}</span>
                    {(!available).then(|| view! { <span class="sr-only">{unavailable}</span> })}
                </button>
            </li>
        }
    };
    view! {
        <nav class="sidebar" aria-label=ui.tr("nav.primary")>
            <ul>
                {Section::ALL.into_iter().filter(Section::is_available).map(item).collect_view()}
            </ul>
            <details class="nav-soon">
                <summary>{ui.tr("nav.coming_soon")}</summary>
                <ul>
                    {Section::ALL.into_iter().filter(|s| !s.is_available()).map(item).collect_view()}
                </ul>
            </details>
        </nav>
    }
}

#[component]
fn CommandPalette(open: RwSignal<bool>) -> impl IntoView {
    let ui = use_ui();
    let query = RwSignal::new(String::new());
    let selected = RwSignal::new(0usize);

    let matches = move || {
        let q = query.get().to_lowercase();
        Section::ALL
            .into_iter()
            .filter(Section::is_available)
            .filter(|s| q.is_empty() || ui.text(s.label_key()).to_lowercase().contains(&q))
            .collect::<Vec<_>>()
    };

    let go = move |s: Section| {
        ui.section.set(s);
        open.set(false);
    };

    let on_key = move |e: ev::KeyboardEvent| {
        let items = matches();
        match e.key().as_str() {
            "ArrowDown" => {
                e.prevent_default();
                if !items.is_empty() {
                    selected.update(|i| *i = (*i + 1) % items.len());
                }
            }
            "ArrowUp" => {
                e.prevent_default();
                if !items.is_empty() {
                    selected.update(|i| *i = (*i + items.len() - 1) % items.len());
                }
            }
            "Enter" => {
                if let Some(s) = items.get(selected.get_untracked()) {
                    go(*s);
                }
            }
            _ => {}
        }
    };

    view! {
        <div class="palette-backdrop" on:click=move |_| open.set(false)>
            <div
                class="palette"
                role="dialog"
                aria-modal="true"
                aria-label=ui.tr("palette.open")
                on:click=|e| e.stop_propagation()
            >
                <input
                    class="palette-input"
                    type="text"
                    autofocus
                    placeholder=ui.tr("palette.placeholder")
                    aria-label=ui.tr("palette.placeholder")
                    prop:value=move || query.get()
                    on:input=move |e| {
                        query.set(event_target_value(&e));
                        selected.set(0);
                    }
                    on:keydown=on_key
                />
                <ul class="palette-list" role="listbox">
                    {move || {
                        let items = matches();
                        if items.is_empty() {
                            return view! { <li class="palette-empty">{ui.tr("palette.no_results")}</li> }.into_any();
                        }
                        items
                            .into_iter()
                            .enumerate()
                            .map(|(i, s)| {
                                view! {
                                    <li
                                        role="option"
                                        class="palette-item"
                                        class:selected=move || selected.get() == i
                                        aria-selected=move || (selected.get() == i).to_string()
                                        on:click=move |_| go(s)
                                    >
                                        <span class="nav-icon">{crate::icons::icon(s.icon())}</span>
                                        {ui.tr(s.label_key())}
                                    </li>
                                }
                            })
                            .collect_view()
                            .into_any()
                    }}
                </ul>
            </div>
        </div>
    }
}
