//! Sync Jobs page: every copy that keeps its target in sync, with the
//! changes it applied, and pause, resume or stop. New syncs are set up on
//! the Migrations page ("Keep in sync").

use gazabridge_ipc::{ConnectionSummary, MonitorDto, RunArgs, RunDto, commands, events};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::migrations::{num, status_class, status_key};
use super::{ErrorBox, Load};
use crate::app::use_ui;
use crate::icons::{Icon, icon};
use crate::ipc::{Subscription, call, listen};
use crate::nav::Section;

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

/// `2026-11-05T20:10:33.5+00:00` → `2026-11-05 20:10:33`.
fn when(rfc3339: &str) -> String {
    rfc3339.get(..19).unwrap_or(rfc3339).replace('T', " ")
}

/// Running and paused syncs first, newest first within each group.
fn order(runs: &mut [RunDto]) {
    runs.sort_by_key(|r| {
        (
            !(r.is_active() || r.status == "paused"),
            std::cmp::Reverse(r.created_at.clone()),
        )
    });
}

#[component]
pub fn SyncJobsPage() -> impl IntoView {
    let ui = use_ui();
    let runs = RwSignal::new(Load::<Vec<RunDto>>::Loading);
    let names = RwSignal::new(Vec::<ConnectionSummary>::new());
    let message = RwSignal::new(None::<String>);
    let subscription = StoredValue::new_local(None::<Subscription>);

    let load = move || {
        spawn_local(async move {
            match call::<_, Vec<RunDto>>(commands::MIGRATION_RECENT, &RecentArgs { limit: 200 })
                .await
            {
                Ok(list) => {
                    let mut syncs: Vec<RunDto> = list
                        .into_iter()
                        .filter(|r| r.sync.is_some() && !r.dry_run)
                        .collect();
                    order(&mut syncs);
                    runs.set(Load::Ready(syncs));
                }
                Err(e) if !matches!(runs.get_untracked(), Load::Ready(_)) => {
                    runs.set(Load::Failed(e.message));
                }
                Err(_) => {}
            }
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
        // The backend announces applied changes, pauses and stops.
        if let Ok(sub) = listen(events::MONITOR, move |_: MonitorDto| load()).await {
            let _ = subscription.try_set_value(Some(sub));
        }
    });
    on_cleanup(move || {
        let _ = subscription.try_update_value(Option::take);
    });

    let conn_name = move |id: &str| {
        names.with(|l| {
            l.iter()
                .find(|c| c.id == id)
                .map_or_else(|| ui.text("rep.deleted_connection"), |c| c.name.clone())
        })
    };

    let act = move |command: &'static str, run_id: String| {
        message.set(None);
        spawn_local(async move {
            match call::<_, ()>(
                command,
                &Wrap {
                    args: &RunArgs { run_id },
                },
            )
            .await
            {
                Ok(()) => load(),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let card = move |r: RunDto| {
        let following = r.is_active() && r.streams.iter().any(|s| s.phase == "following");
        let changes: u64 = r.streams.iter().map(|s| s.changes).sum();
        let deletes: u64 = r.streams.iter().map(|s| s.deletes).sum();
        let failed: u64 = r.streams.iter().map(|s| s.failed).sum();
        let last = r.streams.iter().filter_map(|s| s.last_change.clone()).max();
        let method = r.sync.as_ref().map(|s| {
            if s.method == "polling" {
                format!(
                    "{} · {} {} · {} {}s",
                    ui.text("mig.sync_poll"),
                    ui.text("mig.sync_field"),
                    s.cursor_field.clone().unwrap_or_default(),
                    ui.text("sync.every"),
                    s.interval_secs
                )
            } else {
                ui.text("mig.sync_stream")
            }
        });
        let route = format!(
            "{} → {}",
            conn_name(&r.source_connection_id),
            conn_name(&r.target_connection_id)
        );
        let (id_pause, id_resume, id_stop) = (r.run_id.clone(), r.run_id.clone(), r.run_id.clone());
        let active = r.is_active();
        let resumable = r.can_resume();
        let stoppable = active || r.status == "paused";
        let objects: Vec<String> = r
            .streams
            .iter()
            .map(|s| format!("{} → {}", s.source.display(), s.target.display()))
            .collect();
        view! {
            <article class="card sync-job">
                <header class="mig-stream-head">
                    <h2 class="mon-run-name"><bdi>{r.name.clone()}</bdi></h2>
                    {if following {
                        view! { <span class="status status-info">{ui.tr("mig.ph_following")}</span> }.into_any()
                    } else {
                        view! { <span class=status_class(&r.status)>{ui.tr(status_key(&r.status))}</span> }.into_any()
                    }}
                </header>
                <p class="muted small"><bdi>{route}</bdi>{method.map(|m| format!(" · {m}"))}</p>
                <ul class="sync-objects mono small">{objects.into_iter().map(|o| view! { <li><bdi>{o}</bdi></li> }).collect_view()}</ul>
                <dl class="mon-stats">
                    <div><dt>{ui.tr("sync.changes")}</dt><dd>{num(changes)}</dd></div>
                    <div><dt>{ui.tr("sync.deletes")}</dt><dd>{num(deletes)}</dd></div>
                    <div><dt>{ui.tr("sync.last")}</dt><dd class="small">{last.map_or_else(|| "—".into(), |t| when(&t))}</dd></div>
                    {(failed > 0).then(|| view! { <div><dt>{ui.tr("sync.rejected")}</dt><dd class="finding-warning">{num(failed)}</dd></div> })}
                </dl>
                {r.error.clone().map(|e| view! { <p class="error-detail small">{e}</p> })}
                <div class="toolbar">
                    {active.then(|| view! {
                        <button type="button" class="btn btn-secondary" on:click=move |_| act(commands::MIGRATION_PAUSE, id_pause.clone())>{ui.tr("mig.pause")}</button>
                    })}
                    {resumable.then(|| view! {
                        <button type="button" class="btn" on:click=move |_| act(commands::MIGRATION_RESUME, id_resume.clone())>{ui.tr("mig.resume")}</button>
                    })}
                    {stoppable.then(|| view! {
                        <button type="button" class="btn btn-danger" on:click=move |_| act(commands::MIGRATION_CANCEL, id_stop.clone())>{ui.tr("sync.stop")}</button>
                    })}
                </div>
            </article>
        }
    };

    view! {
        <section class="page page-wide sync-jobs" aria-labelledby="sync-title">
            <h1 id="sync-title">{icon(Icon::Repeat)}" "{ui.tr("nav.sync_jobs")}</h1>
            <p class="muted">{ui.tr("sync.intro")}</p>
            <div class="toolbar">
                <button type="button" class="btn" on:click=move |_| ui.section.set(Section::Migrations)>{ui.tr("sync.new")}</button>
            </div>
            <div role="status" aria-live="polite">
                {move || message.get().map(|m| view! { <p class="error-detail">{m}</p> })}
            </div>
            {move || match runs.get() {
                Load::Loading => view! { <p class="muted">{ui.tr("common.loading")}</p> }.into_any(),
                Load::Failed(e) => view! { <ErrorBox message=e retry=load /> }.into_any(),
                Load::Ready(list) if list.is_empty() => view! { <p class="empty-state">{ui.tr("sync.empty")}</p> }.into_any(),
                Load::Ready(list) => view! { <div class="mon-runs">{list.into_iter().map(card).collect_view()}</div> }.into_any(),
            }}
        </section>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: &str, status: &str, created: &str) -> RunDto {
        RunDto {
            run_id: id.into(),
            name: id.into(),
            status: status.into(),
            source_connection_id: String::new(),
            target_connection_id: String::new(),
            existing: "skip".into(),
            streams: Vec::new(),
            created_at: created.into(),
            started_at: None,
            ended_at: None,
            error: None,
            events: Vec::new(),
            dry_run: false,
            validation: None,
            sync: None,
        }
    }

    #[test]
    fn live_syncs_come_first() {
        let mut runs = vec![
            run("old-done", "cancelled", "2026-11-01"),
            run("paused", "paused", "2026-11-02"),
            run("new-done", "completed", "2026-11-05"),
            run("running", "running", "2026-11-03"),
        ];
        order(&mut runs);
        let ids: Vec<&str> = runs.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, ["running", "paused", "new-done", "old-done"]);
        assert_eq!(when("2026-11-05T20:10:33.5+00:00"), "2026-11-05 20:10:33");
    }
}
