//! Backups: make them, see them, check and restore them.

use std::collections::BTreeSet;
use std::time::Duration;

use gazabridge_ipc::{
    BackupArgs, BackupDto, BackupStatusDto, ConnectionSummary, ExplorerListArgs, NodeDto,
    ObjectAddr, OpArgs, PathArgs, RestoreArgs, RestorePlanDto, ToolDto, commands,
};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::Load;
use super::connection_editor::{NEW_CONNECTION, NewConnectionDialog, new_connection_option};
use crate::app::use_ui;
use crate::icons::{Icon, icon};
use crate::ipc::{NoArgs, call};

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

#[derive(Serialize)]
struct ListArgs {
    search: Option<String>,
}

fn op_id() -> String {
    format!("bk-{}", js_sys::Date::now() as u64)
}

fn num(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

pub(super) fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

fn short_time(rfc3339: &str) -> String {
    rfc3339.get(..16).unwrap_or(rfc3339).replace('T', " ")
}

/// Follows a background operation until it ends.
fn follow(op: String, status: RwSignal<Option<BackupStatusDto>>, done: impl Fn() + Copy + 'static) {
    let handle = StoredValue::new(None::<IntervalHandle>);
    let tick = move || {
        let op = op.clone();
        spawn_local(async move {
            if let Ok(s) = call::<_, BackupStatusDto>(
                commands::BACKUP_STATUS,
                &Wrap {
                    args: &OpArgs { op_id: op },
                },
            )
            .await
            {
                let finished = s.phase != "running";
                status.set(Some(s));
                if finished {
                    if let Some(h) = handle.get_value() {
                        h.clear();
                    }
                    done();
                }
            }
        });
    };
    if let Ok(h) = set_interval_with_handle(tick, Duration::from_millis(500)) {
        handle.set_value(Some(h));
    }
}

#[component]
fn Progress(status: RwSignal<Option<BackupStatusDto>>) -> impl IntoView {
    let ui = use_ui();
    move || {
        status.get().map(|s| {
            let pct = if s.objects_total > 0 { s.objects_done as f64 * 100.0 / s.objects_total as f64 } else { 0.0 };
            match s.phase.as_str() {
                "running" => view! {
                    <div class="bk-progress">
                        <div class="progress" role="progressbar" aria-valuenow=format!("{pct:.0}")><span style=format!("inline-size: {pct:.1}%")></span></div>
                        <p class="muted small mono"><bdi>{s.step.clone()}</bdi></p>
                        <p class="muted small">{format!("{} / {} · {} {}", s.objects_done, s.objects_total, num(s.records), ui.text("bk.records"))}</p>
                    </div>
                }.into_any(),
                "done" => view! {
                    <div class="test-result ok">
                        <strong>{ui.tr("bk.done")}</strong>
                        {s.path.clone().map(|p| view! { <p class="small">{ui.tr("bk.saved_in")}" "<code class="mono"><bdi>{p}</bdi></code></p> })}
                        {(s.records > 0).then(|| view! { <p class="small">{format!("{} {}", num(s.records), ui.text("bk.records"))}</p> })}
                    </div>
                }.into_any(),
                _ => view! {
                    <div class="test-result fail">
                        <strong>{if s.phase == "cancelled" { ui.tr("bk.cancelled") } else { ui.tr("bk.failed") }}</strong>
                        {s.error.clone().map(|e| view! { <p class="error-detail">{e}</p> })}
                        {(!s.log.is_empty()).then(|| view! {
                            <details class="small"><summary>{ui.tr("bk.tool_output")}</summary>
                                <pre class="mono code-block">{s.log.join("\n")}</pre>
                            </details>
                        })}
                    </div>
                }.into_any(),
            }
        })
    }
}

#[component]
pub fn BackupsPage() -> impl IntoView {
    let ui = use_ui();
    let connections = RwSignal::new(Load::<Vec<ConnectionSummary>>::Loading);
    let tools = RwSignal::new(Vec::<ToolDto>::new());
    let backups = RwSignal::new(Load::<Vec<BackupDto>>::Loading);
    let message = RwSignal::new(None::<String>);

    // New backup
    let conn_id = RwSignal::new(String::new());
    let dbs = RwSignal::new(Vec::<String>::new());
    let db = RwSignal::new(String::new());
    let schemas = RwSignal::new(Vec::<String>::new());
    let schema = RwSignal::new(String::new());
    let objects = RwSignal::new(Vec::<NodeDto>::new());
    let selected = RwSignal::new(BTreeSet::<String>::new());
    let method = RwSignal::new("gazabridge".to_owned());
    let label = RwSignal::new(String::new());
    let backup_status = RwSignal::new(None::<BackupStatusDto>);
    let backup_op = StoredValue::new(String::new());

    // Restore
    let restoring = RwSignal::new(None::<BackupDto>);
    let r_conn = RwSignal::new(String::new());
    let r_rename = RwSignal::new(String::new());
    let r_existing = RwSignal::new("skip".to_owned());
    let r_only = RwSignal::new(BTreeSet::<String>::new());
    let r_plan = RwSignal::new(None::<RestorePlanDto>);
    let r_confirm = RwSignal::new(String::new());
    let r_status = RwSignal::new(None::<BackupStatusDto>);
    let confirm_delete = RwSignal::new(None::<BackupDto>);

    let summary = move |id: &str| {
        connections.with(|c| match c {
            Load::Ready(l) => l.iter().find(|x| x.id == id).cloned(),
            _ => None,
        })
    };
    let family = move |id: &str| summary(id).map(|c| c.family).unwrap_or_default();
    let pg = move || family(&conn_id.get()) == "postgres";
    let own_db = move |id: &str| {
        summary(id)
            .and_then(|c| {
                Some(
                    c.target
                        .rsplit_once('/')?
                        .1
                        .split('?')
                        .next()
                        .unwrap_or_default()
                        .to_owned(),
                )
            })
            .filter(|d| !d.is_empty() && !d.contains(':'))
    };
    let tool_found = move |name: &str| tools.with(|t| t.iter().any(|x| x.name == name && x.found));

    let load_backups = move || {
        spawn_local(async move {
            match call::<_, Vec<BackupDto>>(commands::BACKUP_LIST, &NoArgs {}).await {
                Ok(l) => backups.set(Load::Ready(l)),
                Err(e) => backups.set(Load::Failed(e.message)),
            }
        });
    };
    let load_connections = move |then: Option<Box<dyn FnOnce()>>| {
        spawn_local(async move {
            match call::<_, Vec<ConnectionSummary>>(
                commands::LIST_CONNECTIONS,
                &ListArgs { search: None },
            )
            .await
            {
                Ok(l) => connections.set(Load::Ready(l)),
                Err(e) => connections.set(Load::Failed(e.message)),
            }
            if let Some(f) = then {
                f();
            }
        });
    };
    load_connections(None);
    spawn_local(async move {
        if let Ok(t) = call::<_, Vec<ToolDto>>(commands::BACKUP_TOOLS, &NoArgs {}).await {
            tools.set(t);
        }
    });
    load_backups();

    let list = move |args: ExplorerListArgs, into: Box<dyn Fn(Vec<NodeDto>)>| {
        spawn_local(async move {
            match call::<_, Vec<NodeDto>>(commands::EXPLORER_LIST, &Wrap { args: &args }).await {
                Ok(n) => into(n),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let load_objects = move || {
        selected.set(BTreeSet::new());
        objects.set(Vec::new());
        let (id, d, s) = (
            conn_id.get_untracked(),
            db.get_untracked(),
            schema.get_untracked(),
        );
        let is_pg = family(&id) == "postgres";
        if id.is_empty() || d.is_empty() || (is_pg && s.is_empty()) {
            return;
        }
        list(
            ExplorerListArgs {
                connection_id: id,
                database: Some(d),
                schema: is_pg.then_some(s),
            },
            Box::new(move |n| {
                objects.set(
                    n.into_iter()
                        .filter(|x| x.readable && matches!(x.kind.as_str(), "collection" | "table"))
                        .collect(),
                )
            }),
        );
    };
    let pick_conn = move |id: String| {
        conn_id.set(id.clone());
        db.set(String::new());
        schema.set(String::new());
        dbs.set(Vec::new());
        schemas.set(Vec::new());
        objects.set(Vec::new());
        if id.is_empty() {
            return;
        }
        // MySQL backups use the GazaBridge format only.
        if family(&id) == "mysql" {
            method.set("gazabridge".into());
        }
        let is_pg = family(&id) == "postgres";
        list(
            ExplorerListArgs {
                connection_id: id.clone(),
                database: None,
                schema: None,
            },
            Box::new(move |n| {
                dbs.set(
                    n.into_iter()
                        .filter(|x| x.kind == "database")
                        .map(|x| x.name)
                        .filter(|x| !matches!(x.as_str(), "admin" | "local" | "config"))
                        .collect(),
                )
            }),
        );
        if is_pg && let Some(d) = own_db(&id) {
            db.set(d.clone());
            list(
                ExplorerListArgs {
                    connection_id: id,
                    database: Some(d),
                    schema: None,
                },
                Box::new(move |n| {
                    let names: Vec<String> = n.into_iter().map(|x| x.name).collect();
                    if names.iter().any(|x| x == "public") {
                        schema.set("public".into());
                    }
                    schemas.set(names);
                    load_objects();
                }),
            );
        }
    };

    let start_backup = move |_| {
        message.set(None);
        let (d, s, is_pg) = (db.get_untracked(), schema.get_untracked(), pg());
        let op = op_id();
        backup_op.set_value(op.clone());
        let kinds: Vec<(String, String)> = objects
            .with_untracked(|o| o.iter().map(|n| (n.name.clone(), n.kind.clone())).collect());
        let args = BackupArgs {
            op_id: op.clone(),
            connection_id: conn_id.get_untracked(),
            objects: selected
                .get_untracked()
                .into_iter()
                .map(|name| ObjectAddr {
                    database: Some(d.clone()),
                    schema: is_pg.then(|| s.clone()),
                    kind: kinds
                        .iter()
                        .find(|(n, _)| *n == name)
                        .map_or("collection".into(), |(_, k)| k.clone()),
                    name,
                })
                .collect(),
            method: method.get_untracked(),
            label: Some(label.get_untracked()).filter(|l| !l.trim().is_empty()),
        };
        spawn_local(async move {
            match call::<_, ()>(commands::BACKUP_START, &Wrap { args: &args }).await {
                Ok(()) => {
                    backup_status.set(None);
                    follow(op, backup_status, load_backups);
                }
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let cancel = move |op: String| {
        spawn_local(async move {
            let _ = call::<_, bool>(
                commands::BACKUP_CANCEL,
                &Wrap {
                    args: &OpArgs { op_id: op },
                },
            )
            .await;
        });
    };

    let restore_args = move |confirm: bool, op: String| -> Option<RestoreArgs> {
        let b = restoring.get_untracked()?;
        let only = r_only.get_untracked();
        Some(RestoreArgs {
            op_id: op,
            backup: b.path.clone(),
            connection_id: r_conn.get_untracked(),
            rename_to: Some(r_rename.get_untracked()).filter(|r| !r.trim().is_empty()),
            only: b
                .objects
                .iter()
                .filter(|o| only.contains(&o.object.display()))
                .map(|o| o.object.clone())
                .collect(),
            existing: r_existing.get_untracked(),
            confirm_destructive: confirm,
        })
    };
    Effect::new(move |_| {
        let _ = (
            r_conn.get(),
            r_rename.get(),
            r_existing.get(),
            r_only.get(),
            restoring.get(),
        );
        r_plan.set(None);
        r_confirm.set(String::new());
    });
    let check_restore = move |_| {
        message.set(None);
        let Some(args) = restore_args(false, String::new()) else {
            return;
        };
        spawn_local(async move {
            match call::<_, RestorePlanDto>(commands::RESTORE_PLAN, &Wrap { args: &args }).await {
                Ok(p) => r_plan.set(Some(p)),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let start_restore = move |_| {
        message.set(None);
        let op = op_id();
        let Some(args) = restore_args(true, op.clone()) else {
            return;
        };
        spawn_local(async move {
            match call::<_, ()>(commands::RESTORE_START, &Wrap { args: &args }).await {
                Ok(()) => {
                    r_status.set(None);
                    r_plan.set(None);
                    follow(op, r_status, || {});
                }
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let confirm_word = move || {
        restoring
            .get()
            .map(|b| {
                let r = r_rename.get();
                if r.trim().is_empty() {
                    b.schema
                        .clone()
                        .filter(|_| b.family == "postgres")
                        .or(b.database.clone())
                        .unwrap_or_default()
                } else {
                    r.trim().to_owned()
                }
            })
            .unwrap_or_default()
    };
    let can_restore = move || {
        r_plan.get().is_some_and(|p| {
            p.blockers.is_empty() && (!p.destructive || r_confirm.get().trim() == confirm_word())
        }) && !r_status.get().is_some_and(|s| s.phase == "running")
    };
    let delete = move |b: BackupDto| {
        spawn_local(async move {
            match call::<_, ()>(
                commands::BACKUP_DELETE,
                &Wrap {
                    args: &PathArgs {
                        path: b.path.clone(),
                    },
                },
            )
            .await
            {
                Ok(()) => {
                    confirm_delete.set(None);
                    if restoring.get_untracked().is_some_and(|r| r.path == b.path) {
                        restoring.set(None);
                    }
                    load_backups();
                }
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let conn_options = move |current: RwSignal<String>, only_family: Option<String>| {
        view! {
            <option value="" selected=move || current.get().is_empty()>{ui.tr("mig.choose_connection")}</option>
            {move || match connections.get() {
                Load::Ready(l) => l
                    .into_iter()
                    .filter(|c| matches!(c.family.as_str(), "mongodb" | "postgres" | "mysql"))
                    .filter(|c| only_family.as_ref().is_none_or(|f| *f == c.family))
                    .map(|c| {
                        let id = c.id.clone();
                        let sel = id.clone();
                        let engine = match c.family.as_str() {
                            "postgres" => "PostgreSQL",
                            "mysql" => "MySQL",
                            _ => "MongoDB",
                        };
                        view! { <option value=id selected=move || current.get() == sel>{format!("{} · {engine}", c.name)}</option> }
                    })
                    .collect_view()
                    .into_any(),
                _ => ().into_any(),
            }}
            {new_connection_option()}
        }
    };

    let existing_option = move |value: &'static str, title: &'static str| {
        view! {
            <label class="scope-option" class:danger=value == "replace">
                <input type="radio" name="bk-existing" prop:checked=move || r_existing.get() == value on:change=move |_| r_existing.set(value.to_owned()) />
                <span class="scope-text"><strong>{ui.tr(title)}</strong></span>
            </label>
        }
    };

    // A connection made from this page: `Some(true)` for the backup side,
    // `Some(false)` for the restore target.
    let new_conn_for = RwSignal::new(None::<bool>);
    let new_conn_open = RwSignal::new(false);
    Effect::new(move |_| {
        if new_conn_for.get().is_some() {
            new_conn_open.set(true);
        }
    });
    Effect::new(move |_| {
        if !new_conn_open.get() {
            new_conn_for.set(None);
        }
    });
    let new_conn_created = Callback::new(move |id: String| {
        let backup_side = new_conn_for.get_untracked().unwrap_or(true);
        load_connections(Some(Box::new(move || {
            if backup_side {
                pick_conn(id)
            } else {
                r_conn.set(id)
            }
        })));
    });

    view! {
            <section class="page page-wide backups" aria-labelledby="bk-title">
            <NewConnectionDialog open=new_conn_open on_created=new_conn_created />
                <h1 id="bk-title">{ui.tr("bk.title")}</h1>
                <p class="muted">{ui.tr("bk.intro")}</p>
                <div role="status" aria-live="polite">
                    {move || message.get().map(|m| view! { <p class="error-detail">{m}</p> })}
                </div>

                <div class="mig-grid bk-grid">
                    <section class="card mig-side" aria-labelledby="bk-new">
                        <h2 id="bk-new">{icon(Icon::Archive)}" "{ui.tr("bk.new")}</h2>
                        <label class="field">
                            <span>{ui.tr("mig.connection")}</span>
                            <select class="input" prop:value=move || conn_id.get() on:change=move |e| {
                                let v = event_target_value(&e);
                                if v == NEW_CONNECTION {
                                    conn_id.set(conn_id.get_untracked());
                                    new_conn_for.set(Some(true));
                                } else {
                                    pick_conn(v);
                                }
                            }>{conn_options(conn_id, None)}</select>
                        </label>
                        <Show when=move || !pg()>
                            <label class="field">
                                <span>{ui.tr("mig.database")}</span>
                                <select class="input" disabled=move || conn_id.get().is_empty() on:change=move |e| { db.set(event_target_value(&e)); load_objects(); }>
                                    <option value="" selected=move || db.get().is_empty()>{ui.tr("mig.choose_database")}</option>
                                    {move || dbs.get().into_iter().map(|d| { let (s, v) = (d.clone(), d.clone()); view! { <option value=v selected=move || db.get() == s>{d}</option> } }).collect_view()}
                                </select>
                            </label>
                        </Show>
                        <Show when=pg>
                            <label class="field">
                                <span>{move || format!("{} · {}", ui.text("mig.schema"), db.get())}</span>
                                <select class="input" on:change=move |e| { schema.set(event_target_value(&e)); load_objects(); }>
                                    <option value="" selected=move || schema.get().is_empty()>{ui.tr("mig.choose_schema")}</option>
                                    {move || schemas.get().into_iter().map(|d| { let (s, v) = (d.clone(), d.clone()); view! { <option value=v selected=move || schema.get() == s>{d}</option> } }).collect_view()}
                                </select>
                            </label>
                        </Show>
                        <div class="toolbar tight">
                            <span class="muted small">{move || format!("{} {}", selected.get().len(), ui.text("mig.selected"))}</span>
                            <button type="button" class="btn btn-secondary btn-sm" disabled=move || objects.get().is_empty()
                                on:click=move |_| {
                                    let names: Vec<String> = objects.get().into_iter().map(|n| n.name).collect();
                                    selected.update(|s| if names.iter().all(|n| s.contains(n)) { s.clear() } else { s.extend(names) });
                                }>{ui.tr("mig.select_all")}</button>
                        </div>
                        <ul class="mig-list">
                            {move || objects.get().into_iter().map(|n| {
                                let (a, b) = (n.name.clone(), n.name.clone());
                                view! {
                                    <li><label class="field-check">
                                        <input type="checkbox" prop:checked=move || selected.get().contains(&a)
                                            on:change=move |e| { let on = event_target_checked(&e); selected.update(|s| { if on { s.insert(b.clone()); } else { s.remove(&b); } }); } />
                                        <span class="mig-coll">{icon(Icon::for_kind(&n.kind))}<bdi>{n.name.clone()}</bdi></span>
                                        {n.estimated_count.map(|c| view! { <span class="muted small">{format!("~{}", num(c))}</span> })}
                                    </label></li>
                                }
                            }).collect_view()}
                        </ul>
                        <fieldset class="field-group scope">
                            <legend>{ui.tr("bk.method")}</legend>
                            <label class="scope-option">
                                <input type="radio" name="bk-method" prop:checked=move || method.get() == "gazabridge" on:change=move |_| method.set("gazabridge".into()) />
                                <span class="scope-text"><strong>{ui.tr("bk.m_gazabridge")}</strong><span class="muted small">{ui.tr("bk.m_gazabridge_help")}</span></span>
                            </label>
    <Show when=move || family(&conn_id.get()) != "mysql">
                                                    <label class="scope-option" class:disabled=move || !tool_found(if pg() { "pg_dump" } else { "mongodump" })>
                                <input type="radio" name="bk-method" disabled=move || !tool_found(if pg() { "pg_dump" } else { "mongodump" })
                                    prop:checked=move || method.get() == "native" on:change=move |_| method.set("native".into()) />
                                <span class="scope-text">
                                    <strong>{move || if pg() { "pg_dump" } else { "mongodump" }}</strong>
                                    <span class="muted small">{ui.tr("bk.m_native_help")}</span>
                                </span>
                            </label>
                            </Show>
                        </fieldset>
                        <label class="field">
                            <span>{ui.tr("bk.label")}</span>
                            <input class="input" type="text" prop:value=move || label.get() on:input=move |e| label.set(event_target_value(&e)) />
                        </label>
                        <div class="toolbar">
                            <button type="button" class="btn" disabled=move || selected.get().is_empty() || backup_status.get().is_some_and(|s| s.phase == "running") on:click=start_backup>
                                {icon(Icon::Download)}" "{ui.tr("bk.start")}
                            </button>
                            <Show when=move || backup_status.get().is_some_and(|s| s.phase == "running")>
                                <button type="button" class="btn btn-danger" on:click=move |_| cancel(backup_op.get_value())>{ui.tr("query.cancel")}</button>
                            </Show>
                        </div>
                        <Progress status=backup_status />
                    </section>

                    <div class="mig-arrow" aria-hidden="true"></div>

                    <section class="card mig-side" aria-labelledby="bk-tools">
                        <h2 id="bk-tools">{icon(Icon::Settings)}" "{ui.tr("bk.tools")}</h2>
                        <p class="muted small">{ui.tr("bk.tools_help")}</p>
                        <ul class="bk-tools">
                            {move || tools.get().into_iter().map(|t| view! {
                                <li>
                                    <span class=if t.found { "status status-ok" } else { "status status-muted" }>{if t.found { "✓" } else { "—" }}</span>
                                    " "<strong class="mono">{t.name.clone()}</strong>
                                    {if t.found {
                                        view! { <span class="muted small">" "{t.version.clone().unwrap_or_default().trim_start_matches(t.name.as_str()).trim_start_matches(" version:").trim().to_owned()}</span> }.into_any()
                                    } else {
                                        view! { <div class="muted small">{t.install_hint.clone()}</div> }.into_any()
                                    }}
                                </li>
                            }).collect_view()}
                        </ul>
                    </section>
                </div>

                <section class="bk-list" aria-labelledby="bk-list-title">
                    <h2 id="bk-list-title">{ui.tr("bk.list")}</h2>
                    {move || match backups.get() {
                        Load::Loading => view! { <p class="muted">{ui.tr("common.loading")}</p> }.into_any(),
                        Load::Failed(e) => view! { <p class="error-detail">{e}</p> }.into_any(),
                        Load::Ready(l) if l.is_empty() => view! { <p class="muted small">{ui.tr("bk.empty")}</p> }.into_any(),
                        Load::Ready(l) => view! {
                            <div class="grid-wrap">
                                <table class="grid">
                                    <thead><tr>
                                        <th>{ui.tr("bk.col_label")}</th>
                                        <th>{ui.tr("bk.col_from")}</th>
                                        <th>{ui.tr("bk.col_objects")}</th>
                                        <th>{ui.tr("bk.col_size")}</th>
                                        <th>{ui.tr("mig.col_started")}</th>
                                        <th></th>
                                    </tr></thead>
                                    <tbody>
                                        {l.into_iter().map(|b| {
                                            let (b1, b2) = (b.clone(), b.clone());
                                            view! {
                                                <tr>
                                                    <td><bdi>{b.label.clone()}</bdi>
                                                        <div class="muted small">{b.tool.clone().unwrap_or_else(|| "GazaBridge".into())}</div></td>
                                                    <td class="small"><bdi>{b.connection.clone()}</bdi><div class="muted mono"><bdi>{b.schema.clone().filter(|_| b.family == "postgres").or(b.database.clone()).unwrap_or_default()}</bdi></div></td>
                                                    <td class="small">{format!("{} · {} {}", b.objects.len(), num(b.records), ui.text("bk.records"))}</td>
                                                    <td class="small">{bytes(b.bytes)}</td>
                                                    <td class="small">{short_time(&b.created_at)}</td>
                                                    <td class="bk-actions">
                                                        <button type="button" class="btn btn-secondary btn-sm" on:click=move |_| {
                                                            r_only.set(b1.objects.iter().map(|o| o.object.display()).collect());
                                                            r_rename.set(String::new());
                                                            r_conn.set(String::new());
                                                            r_status.set(None);
                                                            restoring.set(Some(b1.clone()));
                                                        }>{ui.tr("bk.restore")}</button>
                                                        <button type="button" class="btn btn-secondary btn-sm" aria-label=ui.tr("bk.delete") title=ui.tr("bk.delete") on:click=move |_| confirm_delete.set(Some(b2.clone()))>{icon(Icon::Trash)}</button>
                                                    </td>
                                                </tr>
                                            }
                                        }).collect_view()}
                                    </tbody>
                                </table>
                            </div>
                        }.into_any(),
                    }}
                    {move || confirm_delete.get().map(|b| {
                        let b2 = b.clone();
                        view! {
                            <div class="test-result fail" role="alertdialog">
                                <p>{format!("{} “{}”", ui.text("bk.delete_question"), b.label)}</p>
                                <p class="muted small mono"><bdi>{b.path.clone()}</bdi></p>
                                <div class="toolbar">
                                    <button type="button" class="btn btn-danger" on:click=move |_| delete(b2.clone())>{ui.tr("bk.delete")}</button>
                                    <button type="button" class="btn btn-secondary" on:click=move |_| confirm_delete.set(None)>{ui.tr("common.cancel")}</button>
                                </div>
                            </div>
                        }
                    })}
                </section>

                {move || restoring.get().map(|b| {
                    let fam = b.family.clone();
                    let is_pg = fam == "postgres";
                    let original = if is_pg { b.schema.clone() } else { b.database.clone() }.unwrap_or_default();
                    view! {
                        <section class="card bk-restore" aria-labelledby="bk-restore-title">
                            <h2 id="bk-restore-title">{icon(Icon::Upload)}" "{ui.tr("bk.restore_title")}" — "<bdi>{b.label.clone()}</bdi></h2>
                            {(!b.warnings.is_empty()).then(|| view! { <ul class="small">{b.warnings.iter().map(|w| view! { <li class="finding-warning">{w.clone()}</li> }).collect_view()}</ul> })}
                            <div class="bk-restore-grid">
                                <div>
                                    <label class="field">
                                        <span>{ui.tr("bk.restore_into")}</span>
                                        <select class="input" prop:value=move || r_conn.get() on:change=move |e| {
                                            let v = event_target_value(&e);
                                            if v == NEW_CONNECTION {
                                                r_conn.set(r_conn.get_untracked());
                                                new_conn_for.set(Some(false));
                                            } else {
                                                r_conn.set(v);
                                            }
                                        }>{conn_options(r_conn, Some(fam.clone()))}</select>
                                    </label>
                                    <label class="field">
                                        <span>{if is_pg { ui.tr("bk.rename_schema") } else { ui.tr("bk.rename_db") }}</span>
                                        <input class="input mono" type="text" placeholder=original.clone() prop:value=move || r_rename.get() on:input=move |e| r_rename.set(event_target_value(&e)) />
                                    </label>
                                    <ul class="mig-list">
                                        {b.objects.iter().map(|o| {
                                            let name = o.object.display();
                                            let (a, c) = (name.clone(), name.clone());
                                            view! {
                                                <li><label class="field-check">
                                                    <input type="checkbox" prop:checked=move || r_only.get().contains(&a)
                                                        on:change=move |e| { let on = event_target_checked(&e); r_only.update(|s| { if on { s.insert(c.clone()); } else { s.remove(&c); } }); } />
                                                    <span class="mig-coll mono"><bdi>{name.clone()}</bdi></span>
                                                    <span class="muted small">{num(o.records)}</span>
                                                </label></li>
                                            }
                                        }).collect_view()}
                                    </ul>
                                </div>
                                <fieldset class="field-group scope">
                                    <legend>{ui.tr("mig.existing_title")}</legend>
                                    {existing_option("skip", "mig.ex_skip")}
                                    {existing_option("update", "mig.ex_update")}
                                    {existing_option("fail", "mig.ex_fail")}
                                    {existing_option("replace", "mig.ex_replace")}
                                </fieldset>
                            </div>
                            <div class="toolbar">
                                <button type="button" class="btn btn-secondary" disabled=move || r_conn.get().is_empty() || r_only.get().is_empty() on:click=check_restore>{icon(Icon::Eye)}" "{ui.tr("bk.check")}</button>
                                <span class="muted small">{ui.tr("bk.check_help")}</span>
                            </div>
                            {move || r_plan.get().map(|p| view! {
                                <div class="bk-plan">
                                    {(!p.blockers.is_empty()).then(|| view! { <div class="test-result fail" role="alert"><ul>{p.blockers.iter().map(|x| view! { <li>{x.clone()}</li> }).collect_view()}</ul></div> })}
                                    <div class="grid-wrap">
                                        <table class="grid">
                                            <thead><tr><th>{ui.tr("bk.col_restore")}</th><th>{ui.tr("mig.col_docs")}</th><th>{ui.tr("mig.col_target_now")}</th><th>{ui.tr("bk.col_verified")}</th></tr></thead>
                                            <tbody>{p.items.iter().map(|i| view! {
                                                <tr>
                                                    <td class="mono"><bdi>{i.from.display()}</bdi>" → "<bdi>{i.to.display()}</bdi></td>
                                                    <td>{num(i.records)}</td>
                                                    <td>{if i.target_exists { i.target_count.map_or("?".into(), num) } else { ui.text("mig.new_collection") }}</td>
                                                    <td>{if i.verified { "✓ SHA-256" } else { "✗" }}</td>
                                                </tr>
                                            }).collect_view()}</tbody>
                                        </table>
                                    </div>
                                    {p.warnings.iter().map(|w| view! { <p class="small finding-warning">{w.clone()}</p> }).collect_view()}
                                    {p.command.clone().map(|c| view! { <details class="small" open=true><summary>{ui.tr("bk.command")}</summary><pre class="mono code-block">{c}</pre></details> })}
                                    {(!p.statements.is_empty()).then(|| view! { <details class="small"><summary>{ui.tr("mig.statements")}</summary><pre class="mono code-block">{p.statements.join("\n")}</pre></details> })}
                                    {p.destructive.then(|| view! {
                                        <div class="test-result fail">
                                            <p><strong>{ui.tr("mig.destructive_title")}</strong></p>
                                            <label class="field">
                                                <span>{move || format!("{} {}", ui.text("bk.type_to_confirm"), confirm_word())}</span>
                                                <input class="input mono" type="text" autocomplete="off" prop:value=move || r_confirm.get() on:input=move |e| r_confirm.set(event_target_value(&e)) />
                                            </label>
                                        </div>
                                    })}
                                    <div class="toolbar">
                                        <button type="button" class="btn" class:btn-danger=p.destructive disabled=move || !can_restore() on:click=start_restore>{icon(Icon::Upload)}" "{ui.tr("bk.restore_start")}</button>
                                    </div>
                                </div>
                            })}
                            <Progress status=r_status />
                            <div class="toolbar">
                                <button type="button" class="btn btn-secondary" disabled=move || r_status.get().is_some_and(|s| s.phase == "running") on:click=move |_| restoring.set(None)>{ui.tr("transfer.close")}</button>
                            </div>
                        </section>
                    }
                })}
            </section>
        }
}
