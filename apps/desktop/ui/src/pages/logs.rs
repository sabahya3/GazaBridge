use gazabridge_ipc::{LogFilter, LogLine, commands};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::{ErrorBox, Load};
use crate::app::use_ui;
use crate::ipc::call;

#[derive(Serialize)]
struct Args {
    filter: LogFilter,
}

#[component]
pub fn LogsPage() -> impl IntoView {
    let ui = use_ui();
    let level = RwSignal::new(String::new());
    let search = RwSignal::new(String::new());
    let lines = RwSignal::new(Load::<Vec<LogLine>>::Loading);

    let load = move || {
        lines.set(Load::Loading);
        let args = Args {
            filter: LogFilter {
                max_level: Some(level.get_untracked()).filter(|l| !l.is_empty()),
                search: Some(search.get_untracked()).filter(|s| !s.trim().is_empty()),
                run_id: None,
                limit: 1_000,
            },
        };
        spawn_local(async move {
            lines.set(match call(commands::QUERY_LOGS, &args).await {
                Ok(v) => Load::Ready(v),
                Err(e) => Load::Failed(e.message),
            });
        });
    };
    load();

    view! {
        <section class="page" aria-labelledby="logs-title">
            <h1 id="logs-title">{ui.tr("logs.title")}</h1>
            <p class="muted">{ui.tr("logs.redaction_note")}</p>
            <div class="toolbar">
                <label class="field-inline">
                    <span>{ui.tr("logs.level")}</span>
                    <select class="input" on:change=move |e| { level.set(event_target_value(&e)); load(); }>
                        <option value="">{ui.tr("logs.all_levels")}</option>
                        <option value="error">"error"</option>
                        <option value="warn">"warn"</option>
                        <option value="info">"info"</option>
                        <option value="debug">"debug"</option>
                    </select>
                </label>
                <input
                    type="search"
                    class="input"
                    placeholder=ui.tr("logs.search")
                    aria-label=ui.tr("logs.search")
                    prop:value=move || search.get()
                    on:change=move |e| { search.set(event_target_value(&e)); load(); }
                />
                <button type="button" class="btn" on:click=move |_| load()>{ui.tr("common.refresh")}</button>
            </div>
            {move || match lines.get() {
                Load::Loading => view! { <p>{ui.tr("common.loading")}</p> }.into_any(),
                Load::Failed(e) => view! { <ErrorBox message=e retry=load /> }.into_any(),
                Load::Ready(v) if v.is_empty() => view! { <p class="empty-state">{ui.tr("logs.empty")}</p> }.into_any(),
                Load::Ready(v) => view! {
                    <div class="table-wrap">
                        <table class="log-table">
                            <thead>
                                <tr>
                                    <th scope="col">{ui.tr("logs.time")}</th>
                                    <th scope="col">{ui.tr("logs.level")}</th>
                                    <th scope="col">{ui.tr("logs.source")}</th>
                                    <th scope="col">{ui.tr("logs.message")}</th>
                                </tr>
                            </thead>
                            <tbody>
                                {v.into_iter().rev().map(|l| {
                                    let class = format!("lvl lvl-{}", l.level);
                                    view! {
                                        <tr>
                                            <td class="mono">{l.timestamp.unwrap_or_default()}</td>
                                            <td><span class=class>{l.level}</span></td>
                                            <td class="mono">{l.target}</td>
                                            <td class="log-msg">{l.message}</td>
                                        </tr>
                                    }
                                }).collect_view()}
                            </tbody>
                        </table>
                    </div>
                }.into_any(),
            }}
        </section>
    }
}
