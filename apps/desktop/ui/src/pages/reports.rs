//! Reports page: every recent copy, conversion and sync with its outcome,
//! and its report saved as JSON, HTML, Markdown or CSV.

use gazabridge_ipc::{ConnectionSummary, PickPathArgs, ReportArgs, RunDto, commands};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::migrations::{num, status_class, status_key};
use super::{ErrorBox, Load};
use crate::app::use_ui;
use crate::icons::{Icon, icon};
use crate::ipc::call;

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

#[derive(Serialize)]
struct RecentArgs {
    limit: u32,
}

#[derive(Serialize)]
struct ListArgs {
    search: Option<String>,
}

fn short_time(rfc3339: &str) -> String {
    rfc3339.get(..16).unwrap_or(rfc3339).replace('T', " ")
}

/// Seconds between two RFC 3339 times, as `1:02:05` / `2:05`.
fn took(start: &str, end: &str) -> Option<String> {
    let secs = |t: &str| js_sys::Date::parse(t) / 1000.0;
    let d = secs(end) - secs(start);
    if !d.is_finite() || d < 0.0 {
        return None;
    }
    let d = d.round() as u64;
    let (h, m, s) = (d / 3600, d / 60 % 60, d % 60);
    Some(if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    })
}

/// `shop → copy` → `shop-copy`; `report` when nothing usable is left.
fn file_stem(name: &str) -> String {
    let parts: Vec<&str> = name
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|p| !p.is_empty())
        .collect();
    if parts.is_empty() {
        "report".into()
    } else {
        parts.join("-")
    }
}

fn extension(format: &str) -> &str {
    if format == "markdown" { "md" } else { format }
}

/// Which status filter a run falls under.
fn bucket(status: &str) -> &'static str {
    match status {
        "completed" => "ok",
        "completed_with_warnings" => "warn",
        "failed" | "cancelled" => "failed",
        _ => "active",
    }
}

#[component]
pub fn ReportsPage() -> impl IntoView {
    let ui = use_ui();
    let runs = RwSignal::new(Load::<Vec<RunDto>>::Loading);
    let names = RwSignal::new(Vec::<ConnectionSummary>::new());
    let format = RwSignal::new("html".to_owned());
    let filter = RwSignal::new("all".to_owned());
    let search = RwSignal::new(String::new());
    let message = RwSignal::new(None::<Result<String, String>>);

    let load = move || {
        runs.set(Load::Loading);
        spawn_local(async move {
            runs.set(
                match call::<_, Vec<RunDto>>(commands::MIGRATION_RECENT, &RecentArgs { limit: 200 })
                    .await
                {
                    Ok(v) => Load::Ready(v),
                    Err(e) => Load::Failed(e.message),
                },
            );
        });
    };
    load();
    spawn_local(async move {
        if let Ok(l) = call::<_, Vec<ConnectionSummary>>(
            commands::LIST_CONNECTIONS,
            &ListArgs { search: None },
        )
        .await
        {
            names.set(l);
        }
    });
    let conn_name = move |id: &str| {
        names.with(|l| {
            l.iter()
                .find(|c| c.id == id)
                .map_or_else(|| ui.text("rep.deleted_connection"), |c| c.name.clone())
        })
    };

    let save = move |run: RunDto| {
        message.set(None);
        let fmt = format.get_untracked();
        let ext = extension(&fmt).to_owned();
        let stem = file_stem(&run.name);
        let pick = PickPathArgs {
            save: true,
            default_name: Some(format!(
                "{}-{}.{ext}",
                stem,
                run.created_at.get(..10).unwrap_or("report")
            )),
            extensions: vec![ext],
        };
        spawn_local(async move {
            let path =
                match call::<_, Option<String>>(commands::PICK_FILE_PATH, &Wrap { args: &pick })
                    .await
                {
                    Ok(Some(p)) => p,
                    Ok(None) => return,
                    Err(e) => return message.set(Some(Err(e.message))),
                };
            let args = ReportArgs {
                run_id: run.run_id,
                format: fmt,
                path: path.clone(),
            };
            message.set(Some(
                match call::<_, ()>(commands::MIGRATION_REPORT, &Wrap { args: &args }).await {
                    Ok(()) => Ok(format!("{} {path}", ui.text("mig.report_saved"))),
                    Err(e) => Err(e.message),
                },
            ));
        });
    };

    let visible = move || match runs.get() {
        Load::Ready(list) => {
            let (f, q) = (filter.get(), search.get().to_lowercase());
            list.into_iter()
                .filter(|r| f == "all" || bucket(&r.status) == f)
                .filter(|r| {
                    q.is_empty()
                        || r.name.to_lowercase().contains(&q)
                        || r.streams
                            .iter()
                            .any(|s| s.source.display().to_lowercase().contains(&q))
                })
                .collect::<Vec<_>>()
        }
        _ => Vec::new(),
    };

    let row = move |r: RunDto| {
        let records: u64 = r
            .streams
            .iter()
            .map(|s| s.inserted + s.updated + s.skipped)
            .sum();
        let rejected: u64 = r.streams.iter().map(|s| s.failed).sum();
        let duration = match (&r.started_at, &r.ended_at) {
            (Some(a), Some(b)) => took(a, b),
            _ => None,
        };
        let check = r.validation.as_ref().and_then(|v| v.passed);
        let active = r.is_active();
        let route = format!(
            "{} → {}",
            conn_name(&r.source_connection_id),
            conn_name(&r.target_connection_id)
        );
        let run = r.clone();
        view! {
            <tr>
                <td>
                    <strong><bdi>{r.name.clone()}</bdi></strong>
                    {r.dry_run.then(|| view! { " " <span class="badge">{ui.tr("rep.dry_run")}</span> })}
                    <div class="muted small"><bdi>{route}</bdi></div>
                </td>
                <td><span class=status_class(&r.status)>{ui.tr(status_key(&r.status))}</span></td>
                <td class="mono small">{short_time(&r.created_at)}</td>
                <td class="mono small">{duration.unwrap_or_else(|| "—".into())}</td>
                <td class="num">
                    {num(records)}
                    {(rejected > 0).then(|| view! { <div class="finding-warning small">{format!("{} {}", num(rejected), ui.text("mig.n_rejected"))}</div> })}
                </td>
                <td>{match check {
                    Some(true) => view! { <span class="status status-ok">"✓ "{ui.tr("rep.check_passed")}</span> }.into_any(),
                    Some(false) => view! { <span class="status status-error">"✗ "{ui.tr("rep.check_failed")}</span> }.into_any(),
                    None => view! { <span class="muted small">{ui.tr("rep.not_checked")}</span> }.into_any(),
                }}</td>
                <td>
                    <button type="button" class="btn btn-secondary" disabled=active
                        title=move || if active { ui.text("rep.wait_active") } else { String::new() }
                        on:click=move |_| save(run.clone())>
                        {ui.tr("mig.save_report")}
                    </button>
                </td>
            </tr>
        }
    };

    view! {
        <section class="page page-wide reports" aria-labelledby="rep-title">
            <h1 id="rep-title">{icon(Icon::Chart)}" "{ui.tr("nav.reports")}</h1>
            <p class="muted">{ui.tr("rep.intro")}</p>
            <div class="toolbar rep-toolbar">
                <input class="input" type="search" aria-label=ui.text("common.search") placeholder=ui.text("rep.search")
                    prop:value=move || search.get() on:input=move |e| search.set(event_target_value(&e)) />
                <select class="input" aria-label=ui.text("rep.show") prop:value=move || filter.get() on:change=move |e| filter.set(event_target_value(&e))>
                    <option value="all">{ui.tr("rep.all")}</option>
                    <option value="ok">{ui.tr("mig.st_completed")}</option>
                    <option value="warn">{ui.tr("mig.st_warnings")}</option>
                    <option value="failed">{ui.tr("rep.failed_or_cancelled")}</option>
                    <option value="active">{ui.tr("rep.unfinished")}</option>
                </select>
                <span class="spacer"></span>
                <label class="inline-field">
                    <span>{ui.tr("mig.report")}</span>
                    <select class="input" prop:value=move || format.get() on:change=move |e| format.set(event_target_value(&e))>
                        <option value="html">"HTML"</option>
                        <option value="markdown">"Markdown"</option>
                        <option value="json">"JSON"</option>
                        <option value="csv">"CSV"</option>
                    </select>
                </label>
                <button type="button" class="btn btn-secondary" on:click=move |_| load()>{ui.tr("common.refresh")}</button>
            </div>
            <div role="status" aria-live="polite">
                {move || message.get().map(|m| match m {
                    Ok(t) => view! { <p class="status status-ok">{t}</p> }.into_any(),
                    Err(t) => view! { <p class="error-detail">{t}</p> }.into_any(),
                })}
            </div>
            {move || match runs.get() {
                Load::Loading => view! { <p class="muted">{ui.tr("common.loading")}</p> }.into_any(),
                Load::Failed(e) => view! { <ErrorBox message=e retry=load /> }.into_any(),
                Load::Ready(all) if all.is_empty() => view! { <p class="empty-state">{ui.tr("mig.history_empty")}</p> }.into_any(),
                Load::Ready(_) => view! {
                    <div class="grid-wrap">
                        <table class="grid rep-table">
                            <thead><tr>
                                <th>{ui.tr("rep.copy")}</th>
                                <th>{ui.tr("rep.status")}</th>
                                <th>{ui.tr("rep.started")}</th>
                                <th>{ui.tr("rep.took")}</th>
                                <th class="num">{ui.tr("rep.records")}</th>
                                <th>{ui.tr("rep.check")}</th>
                                <th><span class="sr-only">{ui.tr("mig.report")}</span></th>
                            </tr></thead>
                            <tbody>{move || visible().into_iter().map(row).collect_view()}</tbody>
                        </table>
                    </div>
                }.into_any(),
            }}
        </section>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_fall_into_filters_and_formats_get_extensions() {
        assert_eq!(bucket("completed"), "ok");
        assert_eq!(bucket("completed_with_warnings"), "warn");
        assert_eq!(bucket("cancelled"), "failed");
        assert_eq!(bucket("paused"), "active");
        assert_eq!(extension("markdown"), "md");
        assert_eq!(extension("html"), "html");
        assert_eq!(file_stem("shop → copy"), "shop-copy");
        assert_eq!(file_stem("نسخ الطلبات"), "نسخ-الطلبات");
        assert_eq!(file_stem(" / "), "report");
    }
}
