use gazabridge_ipc::{AppInfo, commands};
use leptos::prelude::*;
use leptos::task::spawn_local;

use super::Load;
use crate::app::use_ui;
use crate::ipc::{NoArgs, call};
use crate::nav::Section;

#[component]
pub fn HomePage() -> impl IntoView {
    let ui = use_ui();
    let info = RwSignal::new(Load::<AppInfo>::Loading);
    spawn_local(async move {
        info.set(
            match call::<_, AppInfo>(commands::APP_INFO, &NoArgs {}).await {
                Ok(i) => Load::Ready(i),
                Err(e) => Load::Failed(e.message),
            },
        );
    });

    let module = move |s: Section| {
        let available = s.is_available();
        view! {
            <li class="module" class:module-planned=!available>
                <span class="nav-icon">{crate::icons::icon(s.icon())}</span>
                <span>{ui.tr(s.label_key())}</span>
                <span class="badge" class:badge-ok=available class:badge-muted=!available>
                    {if available { ui.tr("home.available") } else { ui.tr("home.planned") }}
                </span>
            </li>
        }
    };

    view! {
        <section class="page" aria-labelledby="home-title">
            <h1 id="home-title">{ui.tr("home.welcome")}</h1>
            <p class="lead">{ui.tr("home.intro")}</p>

            <div class="card notice">
                <h2>{ui.tr("home.safety_title")}</h2>
                <p>{ui.tr("home.safety_body")}</p>
            </div>

            <h2>{ui.tr("home.modules")}</h2>
            <ul class="module-grid">
                {Section::ALL.into_iter().filter(|s| *s != Section::Home).map(module).collect_view()}
            </ul>

            <div class="card">
                {move || match info.get() {
                    Load::Loading => view! { <p>{ui.tr("common.loading")}</p> }.into_any(),
                    Load::Failed(e) => view! { <p class="error-detail">{e}</p> }.into_any(),
                    Load::Ready(i) => view! {
                        <dl class="facts">
                            <dt>{ui.tr("home.version")}</dt><dd>{i.version}</dd>
                            <dt>{ui.tr("home.connector_api")}</dt><dd>{i.connector_api}</dd>
                            <dt>{ui.tr("home.schema_version")}</dt><dd>{i.metadata_schema_version}</dd>
                            <dt>{ui.tr("home.data_dir")}</dt><dd class="mono">{i.data_directory}</dd>
                            <dt>{ui.tr("home.telemetry")}</dt><dd>{ui.tr("home.telemetry_off")}</dd>
                        </dl>
                    }.into_any(),
                }}
            </div>
        </section>
    }
}
