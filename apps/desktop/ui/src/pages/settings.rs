use gazabridge_ipc::{Locale, Preferences, Theme, commands};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use crate::app::use_ui;
use crate::ipc::call;

#[derive(Serialize)]
struct Args {
    preferences: Preferences,
}

#[derive(Clone, PartialEq)]
enum SaveState {
    Idle,
    Saved,
    Failed(String),
}

#[component]
pub fn SettingsPage() -> impl IntoView {
    let ui = use_ui();
    let save_state = RwSignal::new(SaveState::Idle);

    let save = move |update: Box<dyn FnOnce(&mut Preferences)>| {
        let mut next = ui.prefs.get_untracked();
        update(&mut next);
        let previous = ui.prefs.get_untracked();
        ui.prefs.set(next.clone());
        spawn_local(async move {
            match call::<_, ()>(commands::SET_PREFERENCES, &Args { preferences: next }).await {
                Ok(()) => save_state.set(SaveState::Saved),
                Err(e) => {
                    ui.prefs.set(previous);
                    save_state.set(SaveState::Failed(e.message));
                }
            }
        });
    };

    view! {
        <section class="page" aria-labelledby="settings-title">
            <h1 id="settings-title">{ui.tr("settings.title")}</h1>
            <div role="status" aria-live="polite" class="save-status">
                {move || match save_state.get() {
                    SaveState::Idle => String::new(),
                    SaveState::Saved => ui.text("settings.saved"),
                    SaveState::Failed(e) => format!("{}: {e}", ui.text("settings.save_failed")),
                }}
            </div>

            <fieldset class="card">
                <legend>{ui.tr("settings.appearance")}</legend>
                <label class="field">
                    <span>{ui.tr("settings.theme")}</span>
                    <select class="input" on:change=move |e| {
                        let theme = match event_target_value(&e).as_str() {
                            "light" => Theme::Light,
                            "dark" => Theme::Dark,
                            _ => Theme::System,
                        };
                        save(Box::new(move |p| p.theme = theme));
                    }>
                        <option value="system" selected=move || ui.prefs.with(|p| p.theme == Theme::System)>{ui.tr("settings.theme_system")}</option>
                        <option value="light" selected=move || ui.prefs.with(|p| p.theme == Theme::Light)>{ui.tr("settings.theme_light")}</option>
                        <option value="dark" selected=move || ui.prefs.with(|p| p.theme == Theme::Dark)>{ui.tr("settings.theme_dark")}</option>
                    </select>
                </label>
                <label class="field-check">
                    <input
                        type="checkbox"
                        prop:checked=move || ui.prefs.with(|p| p.high_contrast)
                        on:change=move |e| {
                            let on = event_target_checked(&e);
                            save(Box::new(move |p| p.high_contrast = on));
                        }
                    />
                    <span>{ui.tr("settings.high_contrast")}</span>
                </label>
                <label class="field">
                    <span>{ui.tr("settings.language")}</span>
                    <select class="input" on:change=move |e| {
                        let locale = if event_target_value(&e) == "ar" { Locale::Ar } else { Locale::En };
                        save(Box::new(move |p| p.locale = locale));
                    }>
                        <option value="en" lang="en" selected=move || ui.prefs.with(|p| p.locale == Locale::En)>"English"</option>
                        <option value="ar" lang="ar" dir="rtl" selected=move || ui.prefs.with(|p| p.locale == Locale::Ar)>"العربية"</option>
                    </select>
                </label>
            </fieldset>

            <fieldset class="card">
                <legend>{ui.tr("settings.data")}</legend>
                <label class="field">
                    <span>{ui.tr("settings.row_limit")}</span>
                    <input
                        class="input"
                        type="number"
                        min="1"
                        max="10000"
                        prop:value=move || ui.prefs.with(|p| p.default_row_limit.to_string())
                        on:change=move |e| {
                            if let Ok(n) = event_target_value(&e).parse::<u32>()
                                && (1..=10_000).contains(&n)
                            {
                                save(Box::new(move |p| p.default_row_limit = n));
                            }
                        }
                    />
                    <small class="muted">{ui.tr("settings.row_limit_help")}</small>
                </label>
            </fieldset>

            <fieldset class="card">
                <legend>{ui.tr("settings.privacy")}</legend>
                <p>{ui.tr("settings.telemetry_body")}</p>
            </fieldset>
        </section>
    }
}
