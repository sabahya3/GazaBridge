//! Imports and Exports pages: choose a connection and an object, then
//! export it to a file or import a file into it, without opening the
//! Explorer first. The transfer itself is the Explorer's panel.

use gazabridge_ipc::{
    BrowseArgs, BrowsePageDto, ConnectionSummary, ExplorerListArgs, NodeDto, ObjectAddr, commands,
};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::connection_editor::{NEW_CONNECTION, NewConnectionDialog, new_connection_option};
use super::transfer::{Direction, TransferPanel};
use crate::app::use_ui;
use crate::icons::{Icon, icon};
use crate::ipc::call;

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

#[derive(Serialize)]
struct ListArgs {
    search: Option<String>,
}

/// Value of the "New collection…" entry (imports into MongoDB).
const NEW_COLLECTION: &str = "__new_collection__";

/// Objects a transfer can use: views only for exports.
fn usable(n: &NodeDto, direction: Direction) -> bool {
    n.readable
        && !n.name.starts_with("system.")
        && match n.kind.as_str() {
            "collection" | "table" => true,
            "view" | "materialized_view" => direction == Direction::Export,
            _ => false,
        }
}

fn engine(family: &str) -> &'static str {
    match family {
        "postgres" => "PostgreSQL",
        "mysql" => "MySQL",
        "mongodb" => "MongoDB",
        _ => "SQLite",
    }
}

#[component]
pub fn FilesPage(direction: Direction) -> impl IntoView {
    let ui = use_ui();
    let export = direction == Direction::Export;
    let connections = RwSignal::new(Vec::<ConnectionSummary>::new());
    let conn_id = RwSignal::new(String::new());
    let dbs = RwSignal::new(Vec::<String>::new());
    let db = RwSignal::new(String::new());
    let schemas = RwSignal::new(Vec::<String>::new());
    let schema = RwSignal::new(String::new());
    let objects = RwSignal::new(Vec::<NodeDto>::new());
    let object = RwSignal::new(String::new());
    let new_name = RwSignal::new(String::new());
    // The object the panel works on, with its key fields.
    let chosen = RwSignal::new(None::<(ObjectAddr, Vec<String>)>);
    let message = RwSignal::new(None::<String>);
    let done = RwSignal::new(None::<String>);
    let new_conn_open = RwSignal::new(false);

    let family = move || {
        connections.with(|l| {
            l.iter()
                .find(|c| c.id == conn_id.get())
                .map(|c| c.family.clone())
                .unwrap_or_default()
        })
    };

    let load_connections = move |then: Option<Box<dyn FnOnce()>>| {
        spawn_local(async move {
            match call::<_, Vec<ConnectionSummary>>(
                commands::LIST_CONNECTIONS,
                &ListArgs { search: None },
            )
            .await
            {
                Ok(l) => connections.set(l),
                Err(e) => message.set(Some(e.message)),
            }
            if let Some(f) = then {
                f();
            }
        });
    };
    load_connections(None);

    // Lists one level of the connection's tree.
    let list = move |database: Option<String>,
                     schema_name: Option<String>,
                     then: Box<dyn FnOnce(Vec<NodeDto>)>| {
        let args = ExplorerListArgs {
            connection_id: conn_id.get_untracked(),
            database,
            schema: schema_name,
        };
        spawn_local(async move {
            match call::<_, Vec<NodeDto>>(commands::EXPLORER_LIST, &Wrap { args: &args }).await {
                Ok(nodes) => then(nodes),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let reset_object = move || {
        object.set(String::new());
        new_name.set(String::new());
        chosen.set(None);
        done.set(None);
    };
    let show_objects = move |nodes: Vec<NodeDto>| {
        objects.set(nodes.into_iter().filter(|n| usable(n, direction)).collect());
    };
    let pick_schema = move |s: String| {
        schema.set(s.clone());
        objects.set(Vec::new());
        reset_object();
        if !s.is_empty() {
            list(Some(db.get_untracked()), Some(s), Box::new(show_objects));
        }
    };
    let pick_db = move |d: String| {
        db.set(d.clone());
        schemas.set(Vec::new());
        schema.set(String::new());
        objects.set(Vec::new());
        reset_object();
        if d.is_empty() {
            return;
        }
        list(
            Some(d),
            None,
            Box::new(move |nodes| {
                let names: Vec<String> = nodes
                    .iter()
                    .filter(|n| n.kind == "schema")
                    .map(|n| n.name.clone())
                    .collect();
                if names.is_empty() {
                    show_objects(nodes);
                } else {
                    let first = names
                        .iter()
                        .find(|n| *n == "public")
                        .cloned()
                        .unwrap_or_default();
                    schemas.set(names);
                    pick_schema(first);
                }
            }),
        );
    };
    let pick_conn = move |id: String| {
        conn_id.set(id.clone());
        message.set(None);
        dbs.set(Vec::new());
        db.set(String::new());
        schemas.set(Vec::new());
        schema.set(String::new());
        objects.set(Vec::new());
        reset_object();
        if id.is_empty() {
            return;
        }
        list(
            None,
            None,
            Box::new(move |nodes| {
                let names: Vec<String> = nodes
                    .iter()
                    .filter(|n| n.kind == "database")
                    .map(|n| n.name.clone())
                    .filter(|n| !matches!(n.as_str(), "admin" | "local" | "config"))
                    .collect();
                if names.is_empty() {
                    // SQLite: the objects sit at the top.
                    show_objects(nodes);
                } else {
                    dbs.set(names);
                }
            }),
        );
    };
    let new_conn_created = Callback::new(move |id: String| {
        load_connections(Some(Box::new(move || pick_conn(id))));
    });

    // Opens the panel for `name` (`kind` known when it already exists).
    let open_panel = move |name: String, kind: String| {
        let addr = ObjectAddr {
            database: Some(db.get_untracked()).filter(|d| !d.is_empty()),
            schema: Some(schema.get_untracked()).filter(|s| !s.is_empty()),
            name,
            kind: kind.clone(),
        };
        done.set(None);
        if kind == "collection" {
            chosen.set(Some((addr, vec!["_id".to_owned()])));
            return;
        }
        let args = BrowseArgs {
            connection_id: conn_id.get_untracked(),
            object: addr.clone(),
            filter: None,
            sort: None,
            query: None,
            offset: 0,
            limit: 1,
        };
        spawn_local(async move {
            let keys =
                match call::<_, BrowsePageDto>(commands::EXPLORER_BROWSE, &Wrap { args: &args })
                    .await
                {
                    Ok(p) => p.key_fields,
                    Err(_) => Vec::new(),
                };
            chosen.set(Some((addr, keys)));
        });
    };
    let pick_object = move |name: String| {
        reset_object();
        object.set(name.clone());
        if name.is_empty() || name == NEW_COLLECTION {
            return;
        }
        let kind = objects.with_untracked(|o| {
            o.iter()
                .find(|n| n.name == name)
                .map(|n| n.kind.clone())
                .unwrap_or_default()
        });
        open_panel(name, kind);
    };

    let title = if export { "nav.exports" } else { "nav.imports" };
    let intro = if export {
        "files.export_intro"
    } else {
        "files.import_intro"
    };

    view! {
        <section class="page page-wide files" aria-labelledby="files-title">
            <NewConnectionDialog open=new_conn_open on_created=new_conn_created />
            <h1 id="files-title">{icon(if export { Icon::Upload } else { Icon::Download })}" "{ui.tr(title)}</h1>
            <p class="muted">{ui.tr(intro)}</p>
            <div role="status" aria-live="polite">
                {move || message.get().map(|m| view! { <p class="error-detail">{m}</p> })}
                {move || done.get().map(|m| view! { <p class="status status-ok">{m}</p> })}
            </div>
            <div class="files-grid">
                <section class="card files-side" aria-label=ui.text(if export { "files.from" } else { "files.into" })>
                    <h2>{ui.tr(if export { "files.from" } else { "files.into" })}</h2>
                    <label class="field">
                        <span>{ui.tr("mig.connection")}</span>
                        <select class="input" prop:value=move || conn_id.get() on:change=move |e| {
                            let v = event_target_value(&e);
                            if v == NEW_CONNECTION {
                                conn_id.set(conn_id.get_untracked());
                                new_conn_open.set(true);
                            } else {
                                pick_conn(v);
                            }
                        }>
                            <option value="">{ui.tr("mig.choose_connection")}</option>
                            {move || connections.get().into_iter()
                                .filter(|c| export || !c.read_only)
                                .map(|c| {
                                    let label = format!("{} · {}", c.name, engine(&c.family));
                                    view! { <option value=c.id.clone()>{label}</option> }
                                })
                                .collect_view()}
                            {new_connection_option()}
                        </select>
                    </label>
                    {move || (!dbs.get().is_empty()).then(|| view! {
                        <label class="field">
                            <span>{ui.tr("mig.database")}</span>
                            <select class="input" prop:value=move || db.get() on:change=move |e| pick_db(event_target_value(&e))>
                                <option value="">{ui.tr("mig.choose_database")}</option>
                                {dbs.get().into_iter().map(|d| view! { <option value=d.clone()>{d.clone()}</option> }).collect_view()}
                            </select>
                        </label>
                    })}
                    {move || (!schemas.get().is_empty()).then(|| view! {
                        <label class="field">
                            <span>{ui.tr("mig.schema")}</span>
                            <select class="input" prop:value=move || schema.get() on:change=move |e| pick_schema(event_target_value(&e))>
                                {schemas.get().into_iter().map(|s| view! { <option value=s.clone()>{s.clone()}</option> }).collect_view()}
                            </select>
                        </label>
                    })}
                    {move || {
                        let ready = !conn_id.get().is_empty() && (dbs.get().is_empty() || !db.get().is_empty());
                        let mongo_import = !export && family() == "mongodb" && !db.get().is_empty();
                        ready.then(|| view! {
                            <label class="field">
                                <span>{ui.tr("files.object")}</span>
                                <select class="input" prop:value=move || object.get() on:change=move |e| pick_object(event_target_value(&e))>
                                    <option value="">{ui.tr("files.choose_object")}</option>
                                    {objects.get().into_iter().map(|n| {
                                        let label = match n.estimated_count {
                                            Some(c) => format!("{} ({} · {})", n.name, n.kind, super::migrations::num(c)),
                                            None => format!("{} ({})", n.name, n.kind),
                                        };
                                        view! { <option value=n.name.clone()>{label}</option> }
                                    }).collect_view()}
                                    {mongo_import.then(|| view! {
                                        <option value=NEW_COLLECTION>{format!("+ {}", ui.text("files.new_collection"))}</option>
                                    })}
                                </select>
                            </label>
                        })
                    }}
                    {move || (object.get() == NEW_COLLECTION).then(|| view! {
                        <div class="toolbar">
                            <input class="input" type="text" aria-label=ui.text("files.new_collection")
                                placeholder=ui.text("files.new_collection_name")
                                prop:value=move || new_name.get()
                                on:input=move |e| new_name.set(event_target_value(&e)) />
                            <button type="button" class="btn"
                                disabled=move || new_name.get().trim().is_empty() || objects.get().iter().any(|n| n.name == new_name.get().trim())
                                on:click=move |_| open_panel(new_name.get_untracked().trim().to_owned(), "collection".into())>
                                {ui.tr("files.use_name")}
                            </button>
                        </div>
                    })}
                </section>
                <div class="files-main">
                    {move || match chosen.get() {
                        None => view! {
                            <p class="empty-state">{ui.tr(if export { "files.pick_export" } else { "files.pick_import" })}</p>
                        }.into_any(),
                        Some((addr, keys)) => view! {
                            <div class="modal files-panel">
                                <TransferPanel
                                    connection_id=conn_id.get_untracked()
                                    object=addr
                                    direction=direction
                                    filter=Signal::derive(|| None)
                                    query=Signal::derive(|| None)
                                    skip=Signal::derive(|| 0)
                                    limit=Signal::derive(|| None)
                                    matching=Signal::derive(|| None)
                                    key_fields=keys
                                    on_close=Callback::new(move |()| reset_object())
                                    on_imported=Callback::new(move |()| {
                                        done.set(Some(ui.text("files.imported")));
                                        // A new collection now exists: list it.
                                        if !db.get_untracked().is_empty() {
                                            let s = Some(schema.get_untracked()).filter(|s| !s.is_empty());
                                            list(Some(db.get_untracked()), s, Box::new(show_objects));
                                        }
                                    })
                                />
                            </div>
                        }.into_any(),
                    }}
                </div>
            </div>
        </section>
    }
}
