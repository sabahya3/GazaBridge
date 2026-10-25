//! Query Studio: tabs of SQL or MongoDB queries with parameters, cancel,
//! explain, history and saved queries.

use std::collections::HashMap;

use gazabridge_ipc::{
    CONFIRMATION_REQUIRED, CancelArgs, ConnectionSummary, DeleteSavedArgs, ExplainArgs, ExplainDto,
    ExplorerListArgs, HistoryArgs, HistoryDto, ListSavedArgs, NodeDto, QueryContextArgs,
    QueryContextDto, QueryResultDto, RunQueryArgs, SaveQueryArgs, SavedQueryDto, commands,
    risks_from_message,
};
use leptos::ev;
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::Load;
use crate::app::use_ui;
use crate::ipc::call;

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

#[derive(Serialize)]
struct ListArgs {
    search: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct Tab {
    id: u32,
    connection_id: Option<String>,
    language: String,
    database: String,
    collection: String,
    text: String,
    /// One typed-JSON parameter per line.
    params: String,
    max_rows: u32,
    timeout_secs: u32,
    saved_id: Option<String>,
    saved_name: String,
}

impl Tab {
    fn new(id: u32) -> Self {
        Self {
            id,
            connection_id: None,
            language: String::new(),
            database: String::new(),
            collection: String::new(),
            text: String::new(),
            params: String::new(),
            max_rows: 1000,
            timeout_secs: 0,
            saved_id: None,
            saved_name: String::new(),
        }
    }

    fn title(&self, untitled: &str) -> String {
        if !self.saved_name.is_empty() {
            return self.saved_name.clone();
        }
        let first = self
            .text
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .unwrap_or("");
        if first.is_empty() {
            untitled.to_owned()
        } else {
            first.chars().take(24).collect()
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Running(String),
    Rows(QueryResultDto),
    Plan(ExplainDto),
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Action {
    Run,
    Explain,
    ExplainAnalyze,
}

fn is_mongo(language: &str) -> bool {
    language.starts_with("mongo_")
}

fn language_key(language: &str) -> &'static str {
    match language {
        "postgres_sql" => "query.lang_postgres",
        "sqlite_sql" => "query.lang_sqlite",
        "my_sql" => "query.lang_mysql",
        "mongo_filter" => "query.lang_mongo_filter",
        "mongo_aggregation" => "query.lang_mongo_aggregation",
        _ => "query.lang_unknown",
    }
}

fn risk_key(risk: &str) -> &'static str {
    match risk {
        "drop" => "query.risk_drop",
        "truncate" => "query.risk_truncate",
        "delete_without_where" => "query.risk_delete",
        "update_without_where" => "query.risk_update",
        "overwrites_collection" => "query.risk_out",
        _ => "query.risk_unknown",
    }
}

fn placeholder_key(language: &str) -> &'static str {
    match language {
        "mongo_filter" => "query.placeholder_filter",
        "mongo_aggregation" => "query.placeholder_pipeline",
        _ => "query.placeholder_sql",
    }
}

#[component]
pub fn QueryStudioPage() -> impl IntoView {
    let ui = use_ui();
    let connections = RwSignal::new(Load::<Vec<ConnectionSummary>>::Loading);
    let tabs = RwSignal::new(vec![Tab::new(1)]);
    let active = RwSignal::new(1u32);
    let next_id = StoredValue::new(2u32);
    let run_counter = StoredValue::new(0u64);
    let outcomes = RwSignal::new(HashMap::<u32, Outcome>::new());
    let contexts = RwSignal::new(HashMap::<String, Load<QueryContextDto>>::new());
    let databases = RwSignal::new(Vec::<String>::new());
    let collections = RwSignal::new(Vec::<String>::new());
    let side = RwSignal::new("history");
    let history = RwSignal::new(Load::<Vec<HistoryDto>>::Loading);
    let saved = RwSignal::new(Load::<Vec<SavedQueryDto>>::Loading);
    let confirm = RwSignal::new(None::<(Vec<String>, Action)>);
    let save_open = RwSignal::new(false);
    let save_name = RwSignal::new(String::new());
    let save_favorite = RwSignal::new(false);
    let result_json = RwSignal::new(false);
    let message = RwSignal::new(None::<String>);

    spawn_local(async move {
        connections.set(
            match call(commands::LIST_CONNECTIONS, &ListArgs { search: None }).await {
                Ok(v) => Load::Ready(v),
                Err(e) => Load::Failed(e.message),
            },
        );
    });

    let current = move || {
        tabs.with(|ts| ts.iter().find(|t| t.id == active.get()).cloned())
            .unwrap_or_else(|| Tab::new(0))
    };
    let current_untracked = move || {
        tabs.with_untracked(|ts| ts.iter().find(|t| t.id == active.get_untracked()).cloned())
            .unwrap_or_else(|| Tab::new(0))
    };
    let update = move |f: &dyn Fn(&mut Tab)| {
        let id = active.get_untracked();
        tabs.update(|ts| {
            if let Some(t) = ts.iter_mut().find(|t| t.id == id) {
                f(t);
            }
        });
    };

    let load_history = move || {
        let conn = current_untracked().connection_id;
        spawn_local(async move {
            let args = HistoryArgs {
                connection_id: conn,
                limit: 100,
            };
            history.set(
                match call(commands::QUERY_HISTORY, &Wrap { args: &args }).await {
                    Ok(v) => Load::Ready(v),
                    Err(e) => Load::Failed(e.message),
                },
            );
        });
    };
    let load_saved = move || {
        spawn_local(async move {
            let args = ListSavedArgs {
                connection_id: None,
            };
            saved.set(
                match call(commands::QUERY_LIST_SAVED, &Wrap { args: &args }).await {
                    Ok(v) => Load::Ready(v),
                    Err(e) => Load::Failed(e.message),
                },
            );
        });
    };
    load_history();
    load_saved();

    let load_collections = move |conn: String, db: String| {
        collections.set(Vec::new());
        if db.trim().is_empty() {
            return;
        }
        spawn_local(async move {
            let args = ExplorerListArgs {
                connection_id: conn,
                database: Some(db),
                schema: None,
            };
            if let Ok(nodes) =
                call::<_, Vec<NodeDto>>(commands::EXPLORER_LIST, &Wrap { args: &args }).await
            {
                collections.set(
                    nodes
                        .into_iter()
                        .filter(|n| n.readable)
                        .map(|n| n.name)
                        .collect(),
                );
            }
        });
    };

    // Loads what a connection can run and, for MongoDB, its databases.
    let use_connection = move |conn: String| {
        let known = contexts.with_untracked(|c| matches!(c.get(&conn), Some(Load::Ready(_))));
        let apply = move |ctx: &QueryContextDto| {
            let langs = ctx.languages.clone();
            update(&|t| {
                if !langs.contains(&t.language) {
                    t.language = langs.first().cloned().unwrap_or_default();
                }
            });
        };
        if known {
            if let Some(Load::Ready(ctx)) = contexts.with_untracked(|c| c.get(&conn).cloned()) {
                apply(&ctx);
            }
        } else {
            contexts.update(|c| {
                c.insert(conn.clone(), Load::Loading);
            });
            let conn2 = conn.clone();
            spawn_local(async move {
                let args = QueryContextArgs {
                    connection_id: conn2.clone(),
                };
                let r = call::<_, QueryContextDto>(commands::QUERY_CONTEXT, &Wrap { args: &args })
                    .await;
                match r {
                    Ok(ctx) => {
                        apply(&ctx);
                        contexts.update(|c| {
                            c.insert(conn2, Load::Ready(ctx));
                        });
                    }
                    Err(e) => contexts.update(|c| {
                        c.insert(conn2, Load::Failed(e.message));
                    }),
                }
            });
        }
        databases.set(Vec::new());
        let conn3 = conn.clone();
        spawn_local(async move {
            let args = ExplorerListArgs {
                connection_id: conn3,
                database: None,
                schema: None,
            };
            if let Ok(nodes) =
                call::<_, Vec<NodeDto>>(commands::EXPLORER_LIST, &Wrap { args: &args }).await
            {
                databases.set(
                    nodes
                        .into_iter()
                        .filter(|n| n.kind == "database")
                        .map(|n| n.name)
                        .collect(),
                );
            }
        });
        let db = current_untracked().database;
        load_collections(conn, db);
        load_history();
    };

    let context = move || {
        current()
            .connection_id
            .and_then(|c| contexts.with(|m| m.get(&c).cloned()))
            .and_then(|l| match l {
                Load::Ready(c) => Some(c),
                _ => None,
            })
    };

    let start = move |action: Action, confirmed: bool| {
        let tab = current_untracked();
        let Some(conn) = tab.connection_id.clone() else {
            message.set(Some(ui.text("query.pick_connection")));
            return;
        };
        message.set(None);
        let n = run_counter.get_value() + 1;
        run_counter.set_value(n);
        let run_id = format!("tab{}-{n}", tab.id);
        let args = RunQueryArgs {
            connection_id: conn,
            run_id: run_id.clone(),
            language: tab.language.clone(),
            text: tab.text.clone(),
            params: tab
                .params
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect(),
            database: Some(tab.database.clone()),
            collection: Some(tab.collection.clone()),
            max_rows: tab.max_rows,
            timeout_secs: Some(tab.timeout_secs),
            confirmed,
        };
        let tab_id = tab.id;
        outcomes.update(|o| {
            o.insert(tab_id, Outcome::Running(run_id));
        });
        spawn_local(async move {
            let result = match action {
                Action::Run => {
                    call::<_, QueryResultDto>(commands::QUERY_RUN, &Wrap { args: &args })
                        .await
                        .map(Outcome::Rows)
                }
                Action::Explain | Action::ExplainAnalyze => {
                    let ex = ExplainArgs {
                        query: args,
                        analyze: action == Action::ExplainAnalyze,
                    };
                    call::<_, ExplainDto>(commands::QUERY_EXPLAIN, &Wrap { args: &ex })
                        .await
                        .map(Outcome::Plan)
                }
            };
            let outcome = match result {
                Ok(o) => o,
                Err(e) if e.code == CONFIRMATION_REQUIRED => {
                    confirm.set(Some((risks_from_message(&e.message), action)));
                    outcomes.update(|o| {
                        o.remove(&tab_id);
                    });
                    return;
                }
                Err(e) if e.code == "cancelled" => Outcome::Failed(ui.text("query.cancelled")),
                Err(e) => Outcome::Failed(e.message),
            };
            outcomes.update(|o| {
                o.insert(tab_id, outcome);
            });
            if action == Action::Run {
                load_history();
            }
        });
    };
    let cancel = move |_| {
        let id = active.get_untracked();
        if let Some(Outcome::Running(run_id)) = outcomes.with_untracked(|o| o.get(&id).cloned()) {
            spawn_local(async move {
                let _ = call::<_, bool>(
                    commands::QUERY_CANCEL,
                    &Wrap {
                        args: &CancelArgs { run_id },
                    },
                )
                .await;
            });
        }
    };
    let running = move || {
        matches!(
            outcomes.with(|o| o.get(&active.get()).cloned()),
            Some(Outcome::Running(_))
        )
    };

    let new_tab = move |_| {
        let id = next_id.get_value();
        next_id.set_value(id + 1);
        let conn = current_untracked().connection_id;
        let lang = current_untracked().language;
        tabs.update(|ts| {
            let mut t = Tab::new(id);
            t.connection_id = conn;
            t.language = lang;
            ts.push(t);
        });
        active.set(id);
    };
    let close_tab = move |id: u32| {
        tabs.update(|ts| {
            ts.retain(|t| t.id != id);
            if ts.is_empty() {
                let nid = next_id.get_value();
                next_id.set_value(nid + 1);
                ts.push(Tab::new(nid));
            }
        });
        outcomes.update(|o| {
            o.remove(&id);
        });
        if active.get_untracked() == id {
            let first = tabs.with_untracked(|ts| ts[0].id);
            active.set(first);
        }
    };
    let open_text = move |conn: Option<String>,
                          language: String,
                          text: String,
                          saved: Option<(String, String)>| {
        update(&|t| {
            if conn.is_some() {
                t.connection_id = conn.clone();
            }
            t.language = language.clone();
            t.text = text.clone();
            if let Some((id, name)) = &saved {
                t.saved_id = Some(id.clone());
                t.saved_name = name.clone();
            }
        });
        if let Some(c) = conn {
            use_connection(c);
        }
    };
    let do_save = move |_| {
        let tab = current_untracked();
        let args = SaveQueryArgs {
            id: tab.saved_id.clone(),
            name: save_name.get_untracked(),
            connection_id: tab.connection_id.clone(),
            language: tab.language.clone(),
            text: tab.text.clone(),
            favorite: save_favorite.get_untracked(),
        };
        spawn_local(async move {
            match call::<_, SavedQueryDto>(commands::QUERY_SAVE, &Wrap { args: &args }).await {
                Ok(q) => {
                    update(&|t| {
                        t.saved_id = Some(q.id.clone());
                        t.saved_name = q.name.clone();
                    });
                    save_open.set(false);
                    load_saved();
                }
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let delete_saved = move |id: String| {
        spawn_local(async move {
            match call::<_, ()>(
                commands::QUERY_DELETE_SAVED,
                &Wrap {
                    args: &DeleteSavedArgs { id },
                },
            )
            .await
            {
                Ok(()) => load_saved(),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let on_editor_key = move |e: ev::KeyboardEvent| {
        if (e.ctrl_key() || e.meta_key()) && e.key() == "Enter" {
            e.prevent_default();
            if !running() {
                start(Action::Run, false);
            }
        }
    };

    view! {
        <section class="page page-wide" aria-labelledby="qs-title">
            <h1 id="qs-title">{ui.tr("nav.query_studio")}</h1>
            <div class="tabs" role="tablist" aria-label=ui.tr("query.tabs")>
                {move || tabs.get().into_iter().map(|t| {
                    let id = t.id;
                    let title = t.title(&ui.text("query.untitled"));
                    view! {
                        <div class="tab" class:active=move || active.get() == id>
                            <button type="button" role="tab" aria-selected=move || (active.get() == id).to_string() on:click=move |_| active.set(id)>{title}</button>
                            <button type="button" class="tab-close" aria-label=ui.tr("query.close_tab") on:click=move |_| close_tab(id)>{crate::icons::icon(crate::icons::Icon::X)}</button>
                        </div>
                    }
                }).collect_view()}
                <button type="button" class="tab-new" aria-label=ui.tr("query.new_tab") on:click=new_tab>"+"</button>
            </div>

            <div class="studio">
                <div class="studio-main card">
                    <div class="toolbar">
                        <label class="field-inline">
                            <span>{ui.tr("explorer.connection")}</span>
                            <select class="input" on:change=move |e| {
                                let v = event_target_value(&e);
                                let conn = (!v.is_empty()).then_some(v);
                                update(&|t| { t.connection_id = conn.clone(); t.database.clear(); t.collection.clear(); });
                                if let Some(c) = conn { use_connection(c); }
                            }>
                                <option value="" selected=move || current().connection_id.is_none()>{ui.tr("explorer.choose")}</option>
                                {move || match connections.get() {
                                    Load::Ready(list) => list.into_iter().map(|c| {
                                        let id = c.id.clone();
                                        view! { <option value=c.id.clone() selected=move || current().connection_id.as_deref() == Some(id.as_str())>{c.name.clone()}</option> }
                                    }).collect_view().into_any(),
                                    _ => ().into_any(),
                                }}
                            </select>
                        </label>
                        <label class="field-inline">
                            <span>{ui.tr("query.language")}</span>
                            <select class="input" on:change=move |e| { let v = event_target_value(&e); update(&|t| t.language = v.clone()); }>
                                {move || context().map(|c| c.languages).unwrap_or_default().into_iter().map(|l| {
                                    let l2 = l.clone();
                                    view! { <option value=l.clone() selected=move || current().language == l2>{ui.tr(language_key(&l))}</option> }
                                }).collect_view()}
                            </select>
                        </label>
                        {move || context().filter(|c| c.read_only).map(|_| view! { <span class="badge">{ui.tr("connections.read_only")}</span> })}
                    </div>

                    <Show when=move || is_mongo(&current().language)>
                        <div class="toolbar">
                            <datalist id="qs-databases">{move || databases.get().into_iter().map(|d| view! { <option value=d></option> }).collect_view()}</datalist>
                            <datalist id="qs-collections">{move || collections.get().into_iter().map(|c| view! { <option value=c></option> }).collect_view()}</datalist>
                            <label class="field-inline">
                                <span>{ui.tr("editor.database")}</span>
                                <input class="input" list="qs-databases" prop:value=move || current().database
                                    on:change=move |e| {
                                        let v = event_target_value(&e);
                                        update(&|t| t.database = v.clone());
                                        if let Some(c) = current_untracked().connection_id { load_collections(c, v); }
                                    } />
                            </label>
                            <label class="field-inline">
                                <span>{ui.tr("query.collection")}</span>
                                <input class="input" list="qs-collections" prop:value=move || current().collection
                                    on:change=move |e| { let v = event_target_value(&e); update(&|t| t.collection = v.clone()); } />
                            </label>
                        </div>
                    </Show>

                    <textarea class="input mono query-input" rows="12" spellcheck="false" aria-label=ui.tr("query.text")
                        placeholder=move || ui.text(placeholder_key(&current().language))
                        prop:value=move || current().text
                        on:input=move |e| { let v = event_target_value(&e); update(&|t| t.text = v.clone()); }
                        on:keydown=on_editor_key></textarea>

                    <details class="filter-box">
                        <summary>{ui.tr("query.options")}</summary>
                        <Show when=move || !is_mongo(&current().language)>
                            <label class="field">
                                <span>{ui.tr("query.params")}</span>
                                <textarea class="input mono" rows="3" spellcheck="false"
                                    prop:value=move || current().params
                                    on:input=move |e| { let v = event_target_value(&e); update(&|t| t.params = v.clone()); }></textarea>
                                <span class="muted small">{ui.tr("query.params_help")}</span>
                            </label>
                        </Show>
                        <div class="toolbar">
                            <label class="field-inline">
                                <span>{ui.tr("query.max_rows")}</span>
                                <input class="input" type="number" min="1" max="50000" prop:value=move || current().max_rows.to_string()
                                    on:change=move |e| { if let Ok(n) = event_target_value(&e).parse::<u32>() { update(&|t| t.max_rows = n.clamp(1, 50_000)); } } />
                            </label>
                            <label class="field-inline">
                                <span>{ui.tr("query.timeout")}</span>
                                <input class="input" type="number" min="0" prop:value=move || current().timeout_secs.to_string()
                                    on:change=move |e| { if let Ok(n) = event_target_value(&e).parse::<u32>() { update(&|t| t.timeout_secs = n); } } />
                            </label>
                        </div>
                    </details>

                    <div class="toolbar">
                        <button type="button" class="btn" disabled=running on:click=move |_| start(Action::Run, false) aria-keyshortcuts="Control+Enter Meta+Enter">
                            {crate::icons::icon(crate::icons::Icon::Play)}{ui.tr("query.run")}
                        </button>
                        <Show when=running>
                            <button type="button" class="btn btn-danger" on:click=cancel>{ui.tr("query.cancel")}</button>
                        </Show>
                        <Show when=move || context().is_some_and(|c| c.explain)>
                            <button type="button" class="btn btn-secondary" disabled=running on:click=move |_| start(Action::Explain, false)>{ui.tr("query.explain")}</button>
                        </Show>
                        <Show when=move || context().is_some_and(|c| c.explain_analyze)>
                            <button type="button" class="btn btn-secondary" disabled=running title=ui.tr("query.explain_analyze_help")
                                on:click=move |_| start(Action::ExplainAnalyze, false)>{ui.tr("query.explain_analyze")}</button>
                        </Show>
                        <button type="button" class="btn btn-secondary" on:click=move |_| {
                            let t = current_untracked();
                            save_name.set(if t.saved_name.is_empty() { t.title(&ui.text("query.untitled")) } else { t.saved_name });
                            save_open.set(true);
                        }>{ui.tr("query.save")}</button>
                        <span class="muted small">{ui.tr("query.shortcut")}</span>
                    </div>

                    <Show when=move || save_open.get()>
                        <div class="toolbar save-row">
                            <input class="input" type="text" aria-label=ui.tr("editor.name") prop:value=move || save_name.get()
                                on:input=move |e| save_name.set(event_target_value(&e)) />
                            <label class="field-check">
                                <input type="checkbox" prop:checked=move || save_favorite.get() on:change=move |e| save_favorite.set(event_target_checked(&e)) />
                                <span>{ui.tr("connections.favorite")}</span>
                            </label>
                            <button type="button" class="btn btn-small" on:click=do_save>{ui.tr("editor.save")}</button>
                            <button type="button" class="btn btn-small btn-secondary" on:click=move |_| save_open.set(false)>{ui.tr("common.cancel")}</button>
                        </div>
                    </Show>

                    <div role="status" aria-live="polite">
                        {move || message.get().map(|m| view! { <p class="error-detail">{m}</p> })}
                    </div>

                    // Results
                    {move || match outcomes.with(|o| o.get(&active.get()).cloned()) {
                        None => view! { <p class="muted">{ui.tr("query.no_results")}</p> }.into_any(),
                        Some(Outcome::Running(_)) => view! { <p aria-busy="true">{ui.tr("query.running")}</p> }.into_any(),
                        Some(Outcome::Failed(e)) => view! { <div class="error-box" role="alert"><p class="error-detail">{e}</p></div> }.into_any(),
                        Some(Outcome::Plan(p)) => view! {
                            <section class="plan-view">
                                <h3>{if p.analyzed { ui.tr("query.plan_analyzed") } else { ui.tr("query.plan") }}</h3>
                                <pre class="mono plan-text">{p.text}</pre>
                                <details><summary>{ui.tr("query.plan_native")}</summary><pre class="mono plan-text">{p.native}</pre></details>
                            </section>
                        }.into_any(),
                        Some(Outcome::Rows(r)) => {
                            let summary = match (r.columns.is_empty(), r.affected) {
                                (true, Some(n)) => format!("{}: {n} · {} ms", ui.text("query.affected_label"), r.elapsed_ms),
                                _ => format!("{}: {} · {} ms", ui.text("query.rows_label"), r.rows.len(), r.elapsed_ms),
                            };
                            view! {
                                <section class="results">
                                    <div class="toolbar">
                                        <span class="muted small">{summary}</span>
                                        {r.truncated.then(|| view! { <span class="badge">{ui.tr("query.truncated")}</span> })}
                                        <div class="segmented" role="group" aria-label=ui.tr("explorer.view_mode")>
                                            <button type="button" class:active=move || !result_json.get() on:click=move |_| result_json.set(false)>{ui.tr("explorer.view_table")}</button>
                                            <button type="button" class:active=move || result_json.get() on:click=move |_| result_json.set(true)>{ui.tr("explorer.view_json")}</button>
                                        </div>
                                    </div>
                                    {r.notices.iter().map(|n| view! { <p class="muted small">{n.clone()}</p> }).collect_view()}
                                    {(!r.columns.is_empty() || !r.rows.is_empty()).then(|| {
                                        let r = r.clone();
                                        move || if result_json.get() {
                                            view! {
                                                <div class="json-list">
                                                    {r.rows.iter().map(|row| view! { <article class="json-record"><pre class="mono">{row.json.clone()}</pre></article> }).collect_view()}
                                                </div>
                                            }.into_any()
                                        } else {
                                            view! {
                                                <div class="grid-wrap">
                                                    <table class="grid">
                                                        <thead><tr>
                                                            {r.columns.iter().map(|c| {
                                                                let title = if c.native_type.is_empty() { c.name.clone() } else { format!("{} ({})", c.name, c.native_type) };
                                                                view! { <th scope="col" title=title><bdi>{c.name.clone()}</bdi></th> }
                                                            }).collect_view()}
                                                        </tr></thead>
                                                        <tbody>
                                                            {r.rows.iter().map(|row| view! {
                                                                <tr>
                                                                    {row.cells.iter().map(|c| match c.kind.as_str() {
                                                                        "missing" => view! { <td class="cell-missing" title=ui.tr("explorer.missing_hint")>"—"</td> }.into_any(),
                                                                        "null" => view! { <td class="cell-null">"null"</td> }.into_any(),
                                                                        kind => view! { <td class=format!("cell cell-{kind}") title=c.text.clone()>{c.text.clone()}</td> }.into_any(),
                                                                    }).collect_view()}
                                                                </tr>
                                                            }).collect_view()}
                                                        </tbody>
                                                    </table>
                                                </div>
                                            }.into_any()
                                        }
                                    })}
                                </section>
                            }.into_any()
                        }
                    }}
                </div>

                <aside class="studio-side card" aria-label=ui.tr("query.library")>
                    <div class="segmented" role="group">
                        <button type="button" class:active=move || side.get() == "history" on:click=move |_| { side.set("history"); load_history(); }>{ui.tr("query.history")}</button>
                        <button type="button" class:active=move || side.get() == "saved" on:click=move |_| { side.set("saved"); load_saved(); }>{ui.tr("query.saved")}</button>
                    </div>
                    {move || if side.get() == "history" {
                        match history.get() {
                            Load::Loading => view! { <p class="muted small">{ui.tr("common.loading")}</p> }.into_any(),
                            Load::Failed(e) => view! { <p class="error-detail">{e}</p> }.into_any(),
                            Load::Ready(list) if list.is_empty() => view! { <p class="muted small">{ui.tr("query.history_empty")}</p> }.into_any(),
                            Load::Ready(list) => view! {
                                <ul class="library">
                                    {list.into_iter().map(|h| {
                                        let (conn, lang, text) = (h.connection_id.clone(), h.language.clone(), h.text.clone());
                                        let meta = format!(
                                            "{} · {} · {}",
                                            h.started_at.get(..16).unwrap_or(&h.started_at).replace('T', " "),
                                            h.status,
                                            h.duration_ms.map(|d| format!("{d} ms")).unwrap_or_default(),
                                        );
                                        view! {
                                            <li class=format!("library-item status-{}", h.status)>
                                                <button type="button" on:click=move |_| open_text(conn.clone(), lang.clone(), text.clone(), None)>
                                                    <code class="library-text">{h.text.chars().take(160).collect::<String>()}</code>
                                                    <span class="muted small">{meta}</span>
                                                    {h.error_message.map(|m| view! { <span class="error-detail small">{m}</span> })}
                                                </button>
                                            </li>
                                        }
                                    }).collect_view()}
                                </ul>
                            }.into_any(),
                        }
                    } else {
                        match saved.get() {
                            Load::Loading => view! { <p class="muted small">{ui.tr("common.loading")}</p> }.into_any(),
                            Load::Failed(e) => view! { <p class="error-detail">{e}</p> }.into_any(),
                            Load::Ready(list) if list.is_empty() => view! { <p class="muted small">{ui.tr("query.saved_empty")}</p> }.into_any(),
                            Load::Ready(list) => view! {
                                <ul class="library">
                                    {list.into_iter().map(|q| {
                                        let (conn, lang, text, id, name) = (q.connection_id.clone(), q.language.clone(), q.text.clone(), q.id.clone(), q.name.clone());
                                        let del = q.id.clone();
                                        view! {
                                            <li class="library-item">
                                                <button type="button" on:click=move |_| open_text(conn.clone(), lang.clone(), text.clone(), Some((id.clone(), name.clone())))>
                                                    <strong>{q.favorite.then_some("★ ")}{q.name.clone()}</strong>
                                                    <span class="muted small">{ui.tr(language_key(&q.language))}</span>
                                                </button>
                                                <button type="button" class="icon-btn danger" aria-label=ui.tr("connections.delete") title=ui.tr("connections.delete") on:click=move |_| delete_saved(del.clone())>{crate::icons::icon(crate::icons::Icon::Trash)}</button>
                                            </li>
                                        }
                                    }).collect_view()}
                                </ul>
                            }.into_any(),
                        }
                    }}
                </aside>
            </div>

            <Show when=move || confirm.get().is_some()>
                <div class="palette-backdrop">
                    <div class="palette confirm" role="alertdialog" aria-modal="true" aria-labelledby="qs-confirm-title">
                        <h2 id="qs-confirm-title">{ui.tr("query.confirm_title")}</h2>
                        <ul>
                            {move || confirm.get().map(|(risks, _)| risks.into_iter().map(|r| view! { <li>{ui.tr(risk_key(&r))}</li> }).collect_view())}
                        </ul>
                        <p class="muted small">{ui.tr("query.confirm_body")}</p>
                        <div class="toolbar">
                            <button type="button" class="btn btn-danger" on:click=move |_| {
                                if let Some((_, action)) = confirm.get_untracked() {
                                    confirm.set(None);
                                    start(action, true);
                                }
                            }>{ui.tr("query.confirm_run")}</button>
                            <button type="button" class="btn btn-secondary" on:click=move |_| confirm.set(None)>{ui.tr("common.cancel")}</button>
                        </div>
                    </div>
                </div>
            </Show>
        </section>
    }
}
