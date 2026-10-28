//! Copying MongoDB collections from one connection to another: choose,
//! review the plan, then follow, pause, resume or cancel the copy.

use std::collections::BTreeSet;
use std::time::Duration;

use gazabridge_ipc::{
    ConnectionSummary, CopyItemDto, ExplorerListArgs, MigrationArgs, MigrationPlanDto, NodeDto,
    ObjectAddr, RunArgs, RunDto, StreamDto, commands,
};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::Load;
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

/// `12345` → `12,345`.
fn num(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn status_class(status: &str) -> &'static str {
    match status {
        "completed" => "status status-ok",
        "completed_with_warnings" | "paused" | "pausing" => "status status-warn",
        "failed" | "cancelled" => "status status-error",
        _ => "status status-info",
    }
}

fn status_key(status: &str) -> &'static str {
    match status {
        "running" => "mig.st_running",
        "pausing" => "mig.st_pausing",
        "paused" => "mig.st_paused",
        "resuming" => "mig.st_resuming",
        "cancelling" => "mig.st_cancelling",
        "cancelled" => "mig.st_cancelled",
        "failed" => "mig.st_failed",
        "completed_with_warnings" => "mig.st_warnings",
        "completed" => "mig.st_completed",
        _ => "mig.st_other",
    }
}

fn phase_key(phase: &str) -> &'static str {
    match phase {
        "pending" => "mig.ph_pending",
        "preparing" => "mig.ph_preparing",
        "copying" => "mig.ph_copying",
        "indexing" => "mig.ph_indexing",
        "done" => "mig.ph_done",
        "stopped" => "mig.ph_stopped",
        _ => "mig.ph_failed",
    }
}

fn short_time(rfc3339: &str) -> String {
    // `2026-10-28T17:49:50.123+00:00` → `2026-10-28 17:49`
    rfc3339.get(..16).unwrap_or(rfc3339).replace('T', " ")
}

#[component]
pub fn MigrationsPage() -> impl IntoView {
    let ui = use_ui();
    let connections = RwSignal::new(Load::<Vec<ConnectionSummary>>::Loading);
    let source_id = RwSignal::new(String::new());
    let target_id = RwSignal::new(String::new());
    let source_dbs = RwSignal::new(Vec::<String>::new());
    let target_dbs = RwSignal::new(Vec::<String>::new());
    let source_db = RwSignal::new(String::new());
    let target_db = RwSignal::new(String::new());
    // PostgreSQL: tables live in schemas.
    let source_schemas = RwSignal::new(Vec::<String>::new());
    let source_schema = RwSignal::new(String::new());
    let target_schema = RwSignal::new("public".to_owned());
    let doc_mode = RwSignal::new("flatten".to_owned());
    let table_mode = RwSignal::new("reference".to_owned());
    let collections = RwSignal::new(Load::<Vec<NodeDto>>::Ready(Vec::new()));
    let selected = RwSignal::new(BTreeSet::<String>::new());
    let search = RwSignal::new(String::new());
    let existing = RwSignal::new("skip".to_owned());
    let copy_indexes = RwSignal::new(true);
    let on_error = RwSignal::new("stop".to_owned());
    let batch = RwSignal::new(1_000u32);
    let parallel = RwSignal::new(2u32);
    let rate = RwSignal::new(String::new());
    let plan = RwSignal::new(None::<MigrationPlanDto>);
    let planning = RwSignal::new(false);
    let confirm_text = RwSignal::new(String::new());
    let message = RwSignal::new(None::<String>);
    let run = RwSignal::new(None::<RunDto>);
    let history = RwSignal::new(Vec::<RunDto>::new());
    let poll_handle = StoredValue::new(None::<IntervalHandle>);

    let usable = move || match connections.get() {
        Load::Ready(list) => list
            .into_iter()
            .filter(|c| matches!(c.family.as_str(), "mongodb" | "postgres"))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    let summary = move |id: &str| {
        connections.with(|c| match c {
            Load::Ready(list) => list.iter().find(|x| x.id == id).cloned(),
            _ => None,
        })
    };
    let family = move |id: RwSignal<String>| {
        let id = id.get();
        summary(&id).map(|c| c.family).unwrap_or_default()
    };
    let is_pg = move |id: RwSignal<String>| family(id) == "postgres";
    // A PostgreSQL connection browses its own database (`…/app?…`).
    let own_db = move |id: &str| {
        summary(id)
            .and_then(|c| {
                let path = c.target.rsplit_once('/')?.1.to_owned();
                Some(path.split('?').next().unwrap_or_default().to_owned())
            })
            .filter(|d| !d.is_empty() && !d.contains(':'))
    };
    let direction = move || match (family(source_id).as_str(), family(target_id).as_str()) {
        ("mongodb", "postgres") => "doc_to_table",
        ("postgres", "mongodb") => "table_to_doc",
        ("", _) | (_, "") => "",
        _ => "copy",
    };
    let conn_name = move |id: &str| {
        connections
            .with(|c| match c {
                Load::Ready(list) => list.iter().find(|x| x.id == id).map(|x| x.name.clone()),
                _ => None,
            })
            .unwrap_or_else(|| ui.text("mig.unknown_connection"))
    };

    let load_history = move || {
        spawn_local(async move {
            if let Ok(list) = call::<_, Vec<RunDto>>(commands::MIGRATION_RECENT, &NoArgs {}).await {
                history.set(list);
            }
        });
    };

    let list_dbs = move |id: String, into: RwSignal<Vec<String>>| {
        into.set(Vec::new());
        if id.is_empty() {
            return;
        }
        let args = ExplorerListArgs {
            connection_id: id,
            database: None,
            schema: None,
        };
        spawn_local(async move {
            match call::<_, Vec<NodeDto>>(commands::EXPLORER_LIST, &Wrap { args: &args }).await {
                Ok(nodes) => into.set(
                    nodes
                        .into_iter()
                        .filter(|n| n.kind == "database")
                        .map(|n| n.name)
                        .filter(|n| !matches!(n.as_str(), "admin" | "local" | "config"))
                        .collect(),
                ),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let list_collections = move || {
        let (id, db) = (source_id.get_untracked(), source_db.get_untracked());
        let schema = source_schema.get_untracked();
        let pg = is_pg(source_id);
        selected.set(BTreeSet::new());
        plan.set(None);
        if id.is_empty() || db.is_empty() || (pg && schema.is_empty()) {
            collections.set(Load::Ready(Vec::new()));
            return;
        }
        collections.set(Load::Loading);
        let args = ExplorerListArgs {
            connection_id: id,
            database: Some(db),
            schema: pg.then_some(schema),
        };
        spawn_local(async move {
            match call::<_, Vec<NodeDto>>(commands::EXPLORER_LIST, &Wrap { args: &args }).await {
                Ok(nodes) => collections.set(Load::Ready(
                    nodes
                        .into_iter()
                        .filter(|n| n.readable && !n.name.starts_with("system."))
                        .filter(|n| !matches!(n.kind.as_str(), "sequence" | "index"))
                        .collect(),
                )),
                Err(e) => collections.set(Load::Failed(e.message)),
            }
        });
    };

    let list_schemas = move || {
        source_schemas.set(Vec::new());
        source_schema.set(String::new());
        let (id, db) = (source_id.get_untracked(), source_db.get_untracked());
        if id.is_empty() || db.is_empty() {
            return;
        }
        let args = ExplorerListArgs {
            connection_id: id,
            database: Some(db),
            schema: None,
        };
        spawn_local(async move {
            match call::<_, Vec<NodeDto>>(commands::EXPLORER_LIST, &Wrap { args: &args }).await {
                Ok(nodes) => {
                    let names: Vec<String> = nodes.into_iter().map(|n| n.name).collect();
                    if names.iter().any(|n| n == "public") {
                        source_schema.set("public".into());
                    }
                    source_schemas.set(names);
                    list_collections();
                }
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    spawn_local(async move {
        match call::<_, Vec<ConnectionSummary>>(
            commands::LIST_CONNECTIONS,
            &ListArgs { search: None },
        )
        .await
        {
            Ok(list) => connections.set(Load::Ready(list)),
            Err(e) => connections.set(Load::Failed(e.message)),
        }
    });
    load_history();

    let args = move |confirm: bool| -> MigrationArgs {
        let (sdb, tdb) = (source_db.get_untracked(), target_db.get_untracked());
        let (source_pg, target_pg) = (is_pg(source_id), is_pg(target_id));
        let tdb = if target_pg {
            own_db(&target_id.get_untracked()).unwrap_or_default()
        } else if tdb.trim().is_empty() {
            sdb.clone()
        } else {
            tdb.trim().to_owned()
        };
        let sschema = source_pg.then(|| source_schema.get_untracked());
        let tschema = target_pg.then(|| {
            let t = target_schema.get_untracked();
            if t.trim().is_empty() {
                "public".to_owned()
            } else {
                t.trim().to_owned()
            }
        });
        let kinds: Vec<(String, String)> = collections.with_untracked(|c| match c {
            Load::Ready(list) => list
                .iter()
                .map(|n| (n.name.clone(), n.kind.clone()))
                .collect(),
            _ => Vec::new(),
        });
        let kind_of = |name: &str| {
            kinds
                .iter()
                .find(|(n, _)| n == name)
                .map_or("collection".to_owned(), |(_, k)| k.clone())
        };
        MigrationArgs {
            source_connection_id: source_id.get_untracked(),
            target_connection_id: target_id.get_untracked(),
            items: selected
                .get_untracked()
                .into_iter()
                .map(|name| CopyItemDto {
                    source: ObjectAddr {
                        database: Some(sdb.clone()),
                        schema: sschema.clone(),
                        kind: kind_of(&name),
                        name: name.clone(),
                    },
                    target: ObjectAddr {
                        database: Some(tdb.clone()),
                        schema: tschema.clone(),
                        name,
                        kind: if target_pg { "table" } else { "collection" }.into(),
                    },
                })
                .collect(),
            existing: existing.get_untracked(),
            copy_indexes: copy_indexes.get_untracked(),
            batch_size: batch.get_untracked(),
            on_record_error: on_error.get_untracked(),
            max_records_per_sec: rate.get_untracked().trim().parse().ok(),
            parallel: parallel.get_untracked(),
            name: None,
            confirm_destructive: confirm,
            doc_mode: doc_mode.get_untracked(),
            table_mode: table_mode.get_untracked(),
        }
    };

    // Any change after a review invalidates it.
    Effect::new(move |_| {
        let _ = (
            target_id.get(),
            target_db.get(),
            selected.get(),
            existing.get(),
            copy_indexes.get(),
            (doc_mode.get(), table_mode.get(), target_schema.get()),
        );
        plan.set(None);
        confirm_text.set(String::new());
    });

    let review = move |_| {
        message.set(None);
        planning.set(true);
        let a = args(false);
        spawn_local(async move {
            match call::<_, MigrationPlanDto>(commands::MIGRATION_PLAN, &Wrap { args: &a }).await {
                Ok(p) => plan.set(Some(p)),
                Err(e) => message.set(Some(e.message)),
            }
            planning.set(false);
        });
    };

    let stop_polling = move || {
        if let Some(h) = poll_handle.get_value() {
            h.clear();
        }
        poll_handle.set_value(None);
    };
    let follow = move |run_id: String| {
        stop_polling();
        let tick = move || {
            let args = RunArgs {
                run_id: run_id.clone(),
            };
            spawn_local(async move {
                match call::<_, RunDto>(commands::MIGRATION_STATUS, &Wrap { args: &args }).await {
                    Ok(r) => {
                        let settled = !r.is_active();
                        run.set(Some(r));
                        if settled {
                            if let Some(h) = poll_handle.get_value() {
                                h.clear();
                            }
                            poll_handle.set_value(None);
                            load_history();
                        }
                    }
                    Err(e) => message.set(Some(e.message)),
                }
            });
        };
        tick();
        if let Ok(h) = set_interval_with_handle(tick, Duration::from_millis(700)) {
            poll_handle.set_value(Some(h));
        }
    };
    on_cleanup(move || {
        if let Some(h) = poll_handle.get_value() {
            h.clear();
        }
    });

    let start = move |_| {
        message.set(None);
        let a = args(true);
        spawn_local(async move {
            match call::<_, String>(commands::MIGRATION_START, &Wrap { args: &a }).await {
                Ok(id) => {
                    plan.set(None);
                    follow(id);
                    load_history();
                }
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let act = move |command: &'static str, run_id: String| {
        message.set(None);
        let args = RunArgs {
            run_id: run_id.clone(),
        };
        spawn_local(async move {
            match call::<_, ()>(command, &Wrap { args: &args }).await {
                Ok(()) => follow(run_id),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let confirm_needed = move || plan.get().is_some_and(|p| p.destructive);
    let confirm_word = move || {
        if is_pg(target_id) {
            let t = target_schema.get();
            return if t.trim().is_empty() {
                "public".to_owned()
            } else {
                t.trim().to_owned()
            };
        }
        let t = target_db.get();
        if t.trim().is_empty() {
            source_db.get()
        } else {
            t.trim().to_owned()
        }
    };
    let can_start = move || {
        plan.get().is_some_and(|p| p.blockers.is_empty())
            && (!confirm_needed() || confirm_text.get().trim() == confirm_word())
            && !run.get().is_some_and(|r| r.is_active())
    };
    let can_review = move || {
        !source_id.get().is_empty()
            && !target_id.get().is_empty()
            && !source_db.get().is_empty()
            && !selected.get().is_empty()
            && !planning.get()
    };

    let visible = move || {
        let q = search.get().to_lowercase();
        match collections.get() {
            Load::Ready(list) => list
                .into_iter()
                .filter(|n| q.is_empty() || n.name.to_lowercase().contains(&q))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        }
    };

    let conn_options = move |current: RwSignal<String>| {
        view! {
            <option value="" selected=move || current.get().is_empty()>{ui.tr("mig.choose_connection")}</option>
            {move || usable().into_iter().map(|c| {
                let id = c.id.clone();
                let sel = id.clone();
                let engine = if c.family == "postgres" { "PostgreSQL" } else { "MongoDB" };
                let label = if c.read_only {
                    format!("{} · {engine} · {}", c.name, ui.text("mig.read_only"))
                } else {
                    format!("{} · {engine}", c.name)
                };
                view! { <option value=id selected=move || current.get() == sel>{label}</option> }
            }).collect_view()}
        }
    };

    let mode_option = move |signal: RwSignal<String>,
                            group: &'static str,
                            value: &'static str,
                            title: &'static str,
                            help: &'static str| {
        view! {
            <label class="scope-option">
                <input type="radio" name=group prop:checked=move || signal.get() == value on:change=move |_| signal.set(value.to_owned()) />
                <span class="scope-text">
                    <strong>{ui.tr(title)}</strong>
                    <span class="muted small">{ui.tr(help)}</span>
                </span>
            </label>
        }
    };

    let existing_option = move |value: &'static str, title: &'static str, help: &'static str| {
        view! {
            <label class="scope-option" class:danger=value == "replace">
                <input type="radio" name="mig-existing" prop:checked=move || existing.get() == value on:change=move |_| existing.set(value.to_owned()) />
                <span class="scope-text">
                    <strong>{ui.tr(title)}</strong>
                    <span class="muted small">{ui.tr(help)}</span>
                </span>
            </label>
        }
    };

    let stream_row = move |s: StreamDto| {
        let done = s.read;
        let pct = s
            .total
            .filter(|t| *t > 0)
            .map(|t| ((done as f64 / t as f64) * 100.0).min(100.0));
        let width = pct.unwrap_or(if s.phase == "done" { 100.0 } else { 0.0 });
        let total = s.total.map_or_else(|| "?".into(), num);
        view! {
            <li class="mig-stream" class:failed=s.phase == "failed">
                <div class="mig-stream-head">
                    <span class="mono"><bdi>{s.source.display()}</bdi>" → "<bdi>{s.target.display()}</bdi></span>
                    <span class="muted small">{ui.tr(phase_key(&s.phase))}</span>
                </div>
                <div class="progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow=format!("{width:.0}")>
                    <span style=format!("inline-size: {width:.1}%")></span>
                </div>
                <div class="muted small mig-numbers">
                    <span>{format!("{} / {total}", num(done))}</span>
                    <span>{format!("{} {}", num(s.inserted), ui.text("mig.n_inserted"))}</span>
                    {(s.updated > 0).then(|| view! { <span>{format!("{} {}", num(s.updated), ui.text("mig.n_updated"))}</span> })}
                    {(s.skipped > 0).then(|| view! { <span>{format!("{} {}", num(s.skipped), ui.text("mig.n_kept"))}</span> })}
                    {(s.failed > 0).then(|| view! { <span class="finding-warning">{format!("{} {}", num(s.failed), ui.text("mig.n_rejected"))}</span> })}
                    {(s.indexes_created > 0).then(|| view! { <span>{format!("{} {}", s.indexes_created, ui.text("mig.n_indexes"))}</span> })}
                    {(s.retries > 0).then(|| view! { <span>{format!("{} {}", s.retries, ui.text("mig.n_retries"))}</span> })}
                </div>
                {s.error.clone().map(|e| view! { <p class="error-detail small">{e}</p> })}
                {s.quarantine.clone().map(|q| view! { <p class="small">{ui.tr("mig.quarantine_at")}" "<code class="mono"><bdi>{q}</bdi></code></p> })}
                {(!s.rejections.is_empty()).then(|| view! {
                    <details class="small"><summary>{ui.tr("mig.rejections")}</summary>
                        <ul class="mono">{s.rejections.iter().map(|r| view! { <li>{r.clone()}</li> }).collect_view()}</ul>
                    </details>
                })}
            </li>
        }
    };

    view! {
        <section class="page page-wide migrations" aria-labelledby="mig-title">
            <h1 id="mig-title">{ui.tr("mig.title")}</h1>
            <p class="muted">{ui.tr("mig.intro")}</p>
            <div role="status" aria-live="polite">
                {move || message.get().map(|m| view! { <p class="error-detail">{m}</p> })}
                {move || matches!(connections.get(), Load::Failed(_)).then(|| view! { <p class="error-detail">{ui.tr("mig.no_connections")}</p> })}
            </div>

            <div class="mig-grid">
                <section class="card mig-side" aria-labelledby="mig-src">
                    <h2 id="mig-src">{icon(Icon::Database)}" "{ui.tr("mig.source")}</h2>
                    <label class="field">
                        <span>{ui.tr("mig.connection")}</span>
                        <select class="input" on:change=move |e| {
                            let v = event_target_value(&e);
                            source_id.set(v.clone());
                            source_db.set(String::new());
                            source_schema.set(String::new());
                            list_dbs(v.clone(), source_dbs);
                            if is_pg(source_id) && let Some(db) = own_db(&v) {
                                source_db.set(db);
                                list_schemas();
                            } else {
                                list_collections();
                            }
                        }>{conn_options(source_id)}</select>
                    </label>
                    <label class="field">
                        <span>{ui.tr("mig.database")}</span>
                        <select class="input" disabled=move || source_id.get().is_empty() on:change=move |e| {
                            source_db.set(event_target_value(&e));
                            if is_pg(source_id) { list_schemas(); } else { list_collections(); }
                        }>
                            <option value="" selected=move || source_db.get().is_empty()>{ui.tr("mig.choose_database")}</option>
                            {move || source_dbs.get().into_iter().map(|d| {
                                let (sel, value) = (d.clone(), d.clone());
                                view! { <option value=value selected=move || source_db.get() == sel>{d}</option> }
                            }).collect_view()}
                        </select>
                    </label>
                    <Show when=move || is_pg(source_id)>
                        <label class="field">
                            <span>{ui.tr("mig.schema")}</span>
                            <select class="input" disabled=move || source_schemas.get().is_empty() on:change=move |e| {
                                source_schema.set(event_target_value(&e));
                                list_collections();
                            }>
                                <option value="" selected=move || source_schema.get().is_empty()>{ui.tr("mig.choose_schema")}</option>
                                {move || source_schemas.get().into_iter().map(|d| {
                                    let (sel, value) = (d.clone(), d.clone());
                                    view! { <option value=value selected=move || source_schema.get() == sel>{d}</option> }
                                }).collect_view()}
                            </select>
                        </label>
                    </Show>
                    <div class="mig-collections">
                        <div class="toolbar tight">
                            <input class="input" type="search" placeholder=ui.tr("mig.filter") aria-label=ui.tr("mig.filter")
                                prop:value=move || search.get() on:input=move |e| search.set(event_target_value(&e)) />
                            <button type="button" class="btn btn-secondary btn-sm" disabled=move || visible().is_empty()
                                on:click=move |_| {
                                    let names: Vec<String> = visible().into_iter().map(|n| n.name).collect();
                                    selected.update(|s| {
                                        if names.iter().all(|n| s.contains(n)) {
                                            for n in &names { s.remove(n); }
                                        } else {
                                            s.extend(names);
                                        }
                                    });
                                }>{ui.tr("mig.select_all")}</button>
                        </div>
                        <p class="muted small">{move || format!("{} {}", selected.get().len(), ui.text("mig.selected"))}</p>
                        <ul class="mig-list">
                            {move || match collections.get() {
                                Load::Loading => view! { <li class="muted">{ui.tr("common.loading")}</li> }.into_any(),
                                Load::Failed(e) => view! { <li class="error-detail">{e}</li> }.into_any(),
                                Load::Ready(_) if source_db.get().is_empty() || (is_pg(source_id) && source_schema.get().is_empty()) => view! { <li class="muted small">{ui.tr("mig.pick_db_first")}</li> }.into_any(),
                                Load::Ready(_) => visible().into_iter().map(|n| {
                                    let name = n.name.clone();
                                    let (a, b) = (name.clone(), name.clone());
                                    view! {
                                        <li>
                                            <label class="field-check">
                                                <input type="checkbox" prop:checked=move || selected.get().contains(&a)
                                                    on:change=move |e| {
                                                        let on = event_target_checked(&e);
                                                        selected.update(|s| { if on { s.insert(b.clone()); } else { s.remove(&b); } });
                                                    } />
                                                <span class="mig-coll">{icon(Icon::for_kind(&n.kind))}<bdi>{name}</bdi></span>
                                                {n.estimated_count.map(|c| view! { <span class="muted small">{format!("~{}", num(c))}</span> })}
                                            </label>
                                        </li>
                                    }
                                }).collect_view().into_any(),
                            }}
                        </ul>
                    </div>
                </section>

                <div class="mig-arrow" aria-hidden="true">{icon(Icon::ArrowLeftRight)}</div>

                <section class="card mig-side" aria-labelledby="mig-dst">
                    <h2 id="mig-dst">{icon(Icon::Database)}" "{ui.tr("mig.target")}</h2>
                    <label class="field">
                        <span>{ui.tr("mig.connection")}</span>
                        <select class="input" on:change=move |e| {
                            let v = event_target_value(&e);
                            target_id.set(v.clone());
                            list_dbs(v, target_dbs);
                        }>{conn_options(target_id)}</select>
                    </label>
                    <Show when=move || !is_pg(target_id) fallback=move || view! {
                        <label class="field">
                            <span>{move || format!("{} · {}", ui.text("mig.schema"), own_db(&target_id.get()).unwrap_or_default())}</span>
                            <input class="input mono" type="text" placeholder="public"
                                prop:value=move || target_schema.get() on:input=move |e| target_schema.set(event_target_value(&e)) />
                            <span class="muted small">{ui.tr("mig.target_schema_help")}</span>
                        </label>
                    }>
                        <label class="field">
                            <span>{ui.tr("mig.database")}</span>
                            <input class="input mono" type="text" list="mig-target-dbs" placeholder=move || source_db.get()
                                prop:value=move || target_db.get() on:input=move |e| target_db.set(event_target_value(&e)) />
                            <datalist id="mig-target-dbs">
                                {move || target_dbs.get().into_iter().map(|d| view! { <option value=d></option> }).collect_view()}
                            </datalist>
                            <span class="muted small">{ui.tr("mig.target_db_help")}</span>
                        </label>
                    </Show>
                    <Show when=move || direction() == "doc_to_table">
                        <fieldset class="field-group scope">
                            <legend>{ui.tr("mig.doc_mode_title")}</legend>
                            {mode_option(doc_mode, "mig-mode-doc", "flatten", "mig.dm_flatten", "mig.dm_flatten_help")}
                            {mode_option(doc_mode, "mig-mode-doc", "normalize", "mig.dm_normalize", "mig.dm_normalize_help")}
                            {mode_option(doc_mode, "mig-mode-doc", "jsonb", "mig.dm_jsonb", "mig.dm_jsonb_help")}
                            {mode_option(doc_mode, "mig-mode-doc", "hybrid", "mig.dm_hybrid", "mig.dm_hybrid_help")}
                        </fieldset>
                    </Show>
                    <Show when=move || direction() == "table_to_doc">
                        <fieldset class="field-group scope">
                            <legend>{ui.tr("mig.table_mode_title")}</legend>
                            {mode_option(table_mode, "mig-mode-table", "reference", "mig.tm_reference", "mig.tm_reference_help")}
                            {mode_option(table_mode, "mig-mode-table", "embed", "mig.tm_embed", "mig.tm_embed_help")}
                        </fieldset>
                    </Show>
                    <fieldset class="field-group scope">
                        <legend>{ui.tr("mig.existing_title")}</legend>
                        {existing_option("skip", "mig.ex_skip", "mig.ex_skip_help")}
                        {existing_option("update", "mig.ex_update", "mig.ex_update_help")}
                        {existing_option("fail", "mig.ex_fail", "mig.ex_fail_help")}
                        {existing_option("replace", "mig.ex_replace", "mig.ex_replace_help")}
                    </fieldset>
                    <label class="field-check">
                        <input type="checkbox" prop:checked=move || copy_indexes.get() on:change=move |e| copy_indexes.set(event_target_checked(&e)) />
                        <span>{ui.tr("mig.copy_indexes")}</span>
                    </label>
                    <label class="field">
                        <span>{ui.tr("mig.on_error")}</span>
                        <select class="input" on:change=move |e| on_error.set(event_target_value(&e))>
                            <option value="stop" selected=move || on_error.get() == "stop">{ui.tr("mig.err_stop")}</option>
                            <option value="quarantine" selected=move || on_error.get() == "quarantine">{ui.tr("mig.err_quarantine")}</option>
                        </select>
                    </label>
                    <details class="mig-advanced">
                        <summary>{ui.tr("mig.advanced")}</summary>
                        <div class="toolbar">
                            <label class="field-inline">
                                <span>{ui.tr("mig.batch")}</span>
                                <input class="input" type="number" min="1" max="50000" size="6" prop:value=move || batch.get().to_string()
                                    on:input=move |e| if let Ok(v) = event_target_value(&e).parse() { batch.set(v) } />
                            </label>
                            <label class="field-inline">
                                <span>{ui.tr("mig.parallel")}</span>
                                <select class="input" on:change=move |e| if let Ok(v) = event_target_value(&e).parse() { parallel.set(v) }>
                                    {[1u32, 2, 3, 4].into_iter().map(|n| view! { <option value=n.to_string() selected=move || parallel.get() == n>{n}</option> }).collect_view()}
                                </select>
                            </label>
                            <label class="field-inline">
                                <span>{ui.tr("mig.rate")}</span>
                                <input class="input" type="number" min="0" size="7" placeholder=ui.tr("mig.rate_none")
                                    prop:value=move || rate.get() on:input=move |e| rate.set(event_target_value(&e)) />
                            </label>
                        </div>
                        <p class="muted small">{ui.tr("mig.advanced_help")}</p>
                    </details>
                </section>
            </div>

            <div class="toolbar">
                <button type="button" class="btn" disabled=move || !can_review() on:click=review>
                    {icon(Icon::Eye)}" "{move || if planning.get() { ui.text("mig.reviewing") } else { ui.text("mig.review") }}
                </button>
                <span class="muted small">{ui.tr("mig.review_help")}</span>
            </div>

            {move || plan.get().map(|p| view! {
                <section class="card mig-plan" aria-labelledby="mig-plan-title">
                    <h2 id="mig-plan-title">{ui.tr("mig.plan_title")}</h2>
                    {(!p.blockers.is_empty()).then(|| view! {
                        <div class="test-result fail" role="alert">
                            <ul>{p.blockers.iter().map(|b| view! { <li>{b.clone()}</li> }).collect_view()}</ul>
                        </div>
                    })}
                    <div class="grid-wrap">
                        <table class="grid">
                            <thead><tr>
                                <th>{ui.tr("mig.col_copy")}</th>
                                <th>{ui.tr("mig.col_docs")}</th>
                                <th>{ui.tr("mig.col_target_now")}</th>
                                <th>{ui.tr("mig.col_indexes")}</th>
                                <th>{ui.tr("mig.col_notes")}</th>
                            </tr></thead>
                            <tbody>
                                {p.items.iter().map(|i| view! {
                                    <tr>
                                        <td class="mono"><bdi>{i.source.display()}</bdi>" → "<bdi>{i.target.display()}</bdi></td>
                                        <td>{i.source_count.map_or_else(|| "?".into(), |c| format!("~{}", num(c)))}</td>
                                        <td>{if i.target_exists { i.target_count.map_or_else(|| "?".into(), |c| format!("~{}", num(c))) } else { ui.text("mig.new_collection") }}</td>
                                        <td class="small">{if i.indexes.is_empty() { "—".to_owned() } else { i.indexes.join(", ") }}</td>
                                        <td class="small">{i.warnings.iter().map(|w| view! { <div class="finding-warning">{w.clone()}</div> }).collect_view()}</td>
                                    </tr>
                                }).collect_view()}
                            </tbody>
                        </table>
                    </div>
                    {p.items.iter().filter(|i| !i.tables.is_empty() || !i.documents.is_empty()).map(|i| {
                        let title = format!("{} → {}", i.source.display(), i.target.display());
                        let to_tables = i.kind == "doc_to_table";
                        view! {
                            <details class="mig-preview" open=true>
                                <summary><span class="mono"><bdi>{title}</bdi></span>" · "{ui.tr("mig.preview")}</summary>
                                {i.tables.iter().map(|t| view! {
                                    <h3 class="mono small"><bdi>{t.table.display()}</bdi></h3>
                                    <div class="grid-wrap">
                                        <table class="grid">
                                            <thead>
                                                <tr>{t.columns.iter().zip(&t.sources).map(|(c, src)| view! { <th title=src.clone()><bdi>{c.clone()}</bdi></th> }).collect_view()}</tr>
                                                <tr class="muted small">{t.types.iter().map(|ty| view! { <th class="mono">{ty.clone()}</th> }).collect_view()}</tr>
                                            </thead>
                                            <tbody>
                                                {t.rows.iter().map(|r| view! {
                                                    <tr>{r.iter().map(|v| view! { <td class="cell mono" title=v.clone()><bdi>{v.clone()}</bdi></td> }).collect_view()}</tr>
                                                }).collect_view()}
                                            </tbody>
                                        </table>
                                    </div>
                                }).collect_view()}
                                {i.documents.first().cloned().map(|d| view! {
                                    <details class="small">
                                        <summary>{if to_tables { ui.tr("mig.sample_source") } else { ui.tr("mig.sample_result") }}</summary>
                                        <pre class="mono code-block">{d}</pre>
                                    </details>
                                })}
                            </details>
                        }
                    }).collect_view()}
                    <details class="small">
                        <summary>{ui.tr("mig.statements")}</summary>
                        <pre class="mono code-block">{p.items.iter().flat_map(|i| i.statements.clone()).collect::<Vec<_>>().join("\n")}</pre>
                    </details>
                    {p.destructive.then(|| view! {
                        <div class="test-result fail">
                            <p><strong>{ui.tr("mig.destructive_title")}</strong></p>
                            <label class="field">
                                <span>{move || format!("{} {}", ui.text("mig.type_to_confirm"), confirm_word())}</span>
                                <input class="input mono" type="text" autocomplete="off" prop:value=move || confirm_text.get()
                                    on:input=move |e| confirm_text.set(event_target_value(&e)) />
                            </label>
                        </div>
                    })}
                    <div class="toolbar">
                        <button type="button" class="btn" class:btn-danger=p.destructive disabled=move || !can_start() on:click=start>
                            {icon(Icon::Play)}" "{ui.tr("mig.start")}
                        </button>
                        <span class="muted small">{move || format!("{} → {}", conn_name(&source_id.get()), conn_name(&target_id.get()))}</span>
                    </div>
                </section>
            })}

            {move || run.get().map(|r| {
                let (id1, id2, id3) = (r.run_id.clone(), r.run_id.clone(), r.run_id.clone());
                let active = r.is_active();
                let resumable = r.can_resume();
                let paused = r.status == "paused";
                view! {
                    <section class="card mig-run" aria-labelledby="mig-run-title">
                        <div class="mig-run-head">
                            <h2 id="mig-run-title"><bdi>{r.name.clone()}</bdi></h2>
                            <span class=status_class(&r.status)>{ui.tr(status_key(&r.status))}</span>
                        </div>
                        <p class="muted small">{format!("{} → {}", conn_name(&r.source_connection_id), conn_name(&r.target_connection_id))}</p>
                        {r.error.clone().map(|e| view! { <p class="error-detail">{e}</p> })}
                        <ul class="mig-streams">{r.streams.clone().into_iter().map(stream_row).collect_view()}</ul>
                        <div class="toolbar">
                            {(r.status == "running").then(|| view! {
                                <button type="button" class="btn btn-secondary" on:click=move |_| act(commands::MIGRATION_PAUSE, id1.clone())>{ui.tr("mig.pause")}</button>
                            })}
                            {resumable.then(|| view! {
                                <button type="button" class="btn" on:click=move |_| act(commands::MIGRATION_RESUME, id2.clone())>{icon(Icon::Play)}" "{ui.tr("mig.resume")}</button>
                            })}
                            {(active || paused).then(|| view! {
                                <button type="button" class="btn btn-danger" on:click=move |_| act(commands::MIGRATION_CANCEL, id3.clone())>{ui.tr("mig.cancel")}</button>
                            })}
                            {(!active).then(|| view! {
                                <button type="button" class="btn btn-secondary" on:click=move |_| run.set(None)>{ui.tr("transfer.close")}</button>
                            })}
                        </div>
                        {resumable.then(|| view! { <p class="muted small">{ui.tr("mig.resume_help")}</p> })}
                        {(!r.events.is_empty()).then(|| view! {
                            <details class="small" open=!active>
                                <summary>{ui.tr("mig.events")}</summary>
                                <ul class="mig-events">{r.events.iter().map(|e| view! {
                                    <li class=format!("ev-{}", e.level)><span class="mono muted">{short_time(&e.at)}</span>" "{e.message.clone()}</li>
                                }).collect_view()}</ul>
                            </details>
                        })}
                    </section>
                }
            })}

            <section class="mig-history" aria-labelledby="mig-hist-title">
                <h2 id="mig-hist-title">{ui.tr("mig.history")}</h2>
                {move || if history.get().is_empty() {
                    view! { <p class="muted small">{ui.tr("mig.history_empty")}</p> }.into_any()
                } else {
                    view! {
                        <div class="grid-wrap">
                            <table class="grid">
                                <thead><tr>
                                    <th>{ui.tr("mig.col_name")}</th>
                                    <th>{ui.tr("mig.col_status")}</th>
                                    <th>{ui.tr("mig.col_docs")}</th>
                                    <th>{ui.tr("mig.col_started")}</th>
                                    <th></th>
                                </tr></thead>
                                <tbody>
                                    {history.get().into_iter().map(|h| {
                                        let id = h.run_id.clone();
                                        let copied: u64 = h.streams.iter().map(StreamDto::processed).sum();
                                        view! {
                                            <tr>
                                                <td><bdi>{h.name.clone()}</bdi><div class="muted small">{format!("{} {}", h.streams.len(), ui.text("mig.collections"))}</div></td>
                                                <td><span class=status_class(&h.status)>{ui.tr(status_key(&h.status))}</span></td>
                                                <td>{num(copied)}</td>
                                                <td class="small">{short_time(&h.created_at)}</td>
                                                <td><button type="button" class="btn btn-secondary btn-sm" on:click=move |_| follow(id.clone())>{ui.tr("mig.open")}</button></td>
                                            </tr>
                                        }
                                    }).collect_view()}
                                </tbody>
                            </table>
                        </div>
                    }.into_any()
                }}
            </section>
        </section>
    }
}
