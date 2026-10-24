//! Application shell: header, navigation, command palette and page routing.

use gazabridge_ipc::{Preferences, Theme, commands};
use leptos::ev;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::i18n::t;
use crate::ipc::{NoArgs, call};
use crate::nav::Section;
use crate::pages::{ConnectionsPage, HomePage, LogsPage, SettingsPage};

/// Shared UI state, provided as context.
#[derive(Clone, Copy)]
pub struct Ui {
    pub prefs: RwSignal<Preferences>,
    pub section: RwSignal<Section>,
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
        <div class="shell">
            <a class="skip-link" href="#main">{ui.tr("nav.primary")}</a>
            <Header palette_open=palette_open />
            <Sidebar />
            <main id="main" class="content" tabindex="-1">
                {move || match ui.section.get() {
                    Section::Connections => view! { <ConnectionsPage /> }.into_any(),
                    Section::Logs => view! { <LogsPage /> }.into_any(),
                    Section::Settings => view! { <SettingsPage /> }.into_any(),
                    _ => view! { <HomePage /> }.into_any(),
                }}
            </main>
            <Show when=move || palette_open.get()>
                <CommandPalette open=palette_open />
            </Show>
        </div>
    }
}

#[component]
fn Header(palette_open: RwSignal<bool>) -> impl IntoView {
    let ui = use_ui();
    view! {
        <header class="header">
            <div class="brand">
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
    view! {
        <nav class="sidebar" aria-label=ui.tr("nav.primary")>
            <ul>
                {Section::ALL
                    .into_iter()
                    .map(|s| {
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
                                    <span class="nav-icon" aria-hidden="true">{s.icon()}</span>
                                    <span class="nav-label">{label}</span>
                                    {(!available).then(|| view! {
                                        <span class="sr-only">{unavailable}</span>
                                    })}
                                </button>
                            </li>
                        }
                    })
                    .collect_view()}
            </ul>
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
                                        <span aria-hidden="true">{s.icon()}</span>
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
