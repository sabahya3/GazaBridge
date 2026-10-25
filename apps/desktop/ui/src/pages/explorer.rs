//! Explorer: namespace tree, data viewer (table / JSON / tree) and the
//! record editor with change preview and confirmed deletes.

use gazabridge_ipc::{
    BrowseArgs, BrowsePageDto, ConditionDto, ConnectionSummary, CountArgs, CountDto,
    DeleteRecordArgs, EditArgs, EditPlanDto, ExplorerListArgs, FilterDto, FilterOp, InsertArgs,
    MutationDto, NodeDto, ObjectAddr, SortDto, commands,
};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::transfer::{Direction, TransferPanel};
use super::{ErrorBox, Load};
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

const PAGE_SIZES: [u32; 4] = [25, 50, 100, 500];

#[derive(Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    Table,
    Json,
    Tree,
}

#[derive(Clone, PartialEq)]
enum Editor {
    Closed,
    Insert,
    Edit { original: String },
}

fn blocker_key(code: &str) -> &'static str {
    match code {
        "read_only_connection" => "explorer.blocked_read_only",
        "no_primary_key" => "explorer.blocked_no_key",
        "not_writable" => "explorer.blocked_not_writable",
        _ => "explorer.blocked_unsupported",
    }
}

fn op_key(op: FilterOp) -> &'static str {
    match op {
        FilterOp::Eq => "explorer.op_eq",
        FilterOp::Ne => "explorer.op_ne",
        FilterOp::Lt => "explorer.op_lt",
        FilterOp::Le => "explorer.op_le",
        FilterOp::Gt => "explorer.op_gt",
        FilterOp::Ge => "explorer.op_ge",
        FilterOp::Contains => "explorer.op_contains",
        FilterOp::StartsWith => "explorer.op_starts_with",
        FilterOp::IsNull => "explorer.op_is_null",
        FilterOp::IsNotNull => "explorer.op_is_not_null",
        FilterOp::IsMissing => "explorer.op_is_missing",
    }
}

#[component]
pub fn ExplorerPage() -> impl IntoView {
    let ui = use_ui();
    let connections = RwSignal::new(Load::<Vec<ConnectionSummary>>::Loading);
    let connection = RwSignal::new(ui.open_connection.get_untracked());
    ui.open_connection.set(None);
    let object = RwSignal::new(None::<ObjectAddr>);

    let load = move || {
        connections.set(Load::Loading);
        spawn_local(async move {
            connections.set(
                match call(commands::LIST_CONNECTIONS, &ListArgs { search: None }).await {
                    Ok(v) => Load::Ready(v),
                    Err(e) => Load::Failed(e.message),
                },
            );
        });
    };
    load();

    let disconnect = move |_| {
        if let Some(id) = connection.get_untracked() {
            #[derive(Serialize)]
            struct Args {
                connection_id: String,
            }
            spawn_local(async move {
                let _ =
                    call::<_, ()>(commands::EXPLORER_DISCONNECT, &Args { connection_id: id }).await;
            });
        }
        connection.set(None);
        object.set(None);
    };

    view! {
        <section class="page page-wide" aria-labelledby="explorer-title">
            <h1 id="explorer-title">{ui.tr("nav.explorer")}</h1>
            {move || match connections.get() {
                Load::Loading => view! { <p>{ui.tr("common.loading")}</p> }.into_any(),
                Load::Failed(e) => view! { <ErrorBox message=e retry=load /> }.into_any(),
                Load::Ready(list) if list.is_empty() => view! {
                    <div class="empty-state">
                        <h2>{ui.tr("connections.empty_title")}</h2>
                        <p>{ui.tr("explorer.no_connections")}</p>
                    </div>
                }.into_any(),
                Load::Ready(list) => view! {
                    <div class="toolbar">
                        <label class="field-inline">
                            <span>{ui.tr("explorer.connection")}</span>
                            <select class="input" on:change=move |e| {
                                let v = event_target_value(&e);
                                object.set(None);
                                connection.set((!v.is_empty()).then_some(v));
                            }>
                                <option value="" selected=move || connection.get().is_none()>{ui.tr("explorer.choose")}</option>
                                {list.into_iter().map(|c| {
                                    let id = c.id.clone();
                                    view! {
                                        <option value=c.id.clone() selected=move || connection.get().as_deref() == Some(id.as_str())>
                                            {format!("{} — {}", c.name, c.target)}
                                        </option>
                                    }
                                }).collect_view()}
                            </select>
                        </label>
                        <Show when=move || connection.get().is_some()>
                            <button type="button" class="btn btn-small btn-secondary" on:click=disconnect>{ui.tr("explorer.disconnect")}</button>
                        </Show>
                    </div>
                }.into_any(),
            }}
            {move || connection.get().map(|cid| {
                let cid_tree = cid.clone();
                let on_open = Callback::new(move |o: ObjectAddr| object.set(Some(o)));
                view! {
                    <div class="explorer">
                        <nav class="tree card" aria-label=ui.tr("explorer.tree")>
                            <TreeLevel connection_id=cid_tree database=None schema=None on_open=on_open />
                        </nav>
                        <div class="viewer">
                            {move || match object.get() {
                                Some(o) => view! { <Viewer connection_id=cid.clone() object=o /> }.into_any(),
                                None => view! { <p class="muted">{ui.tr("explorer.pick_object")}</p> }.into_any(),
                            }}
                        </div>
                    </div>
                }
            })}
        </section>
    }
}

/// One level of the namespace tree; children load when expanded.
#[component]
fn TreeLevel(
    connection_id: String,
    database: Option<String>,
    schema: Option<String>,
    on_open: Callback<ObjectAddr>,
) -> impl IntoView {
    let ui = use_ui();
    let nodes = RwSignal::new(Load::<Vec<NodeDto>>::Loading);
    let args = ExplorerListArgs {
        connection_id: connection_id.clone(),
        database: database.clone(),
        schema: schema.clone(),
    };
    let load = move || {
        let args = args.clone();
        nodes.set(Load::Loading);
        spawn_local(async move {
            nodes.set(
                match call(commands::EXPLORER_LIST, &Wrap { args: &args }).await {
                    Ok(v) => Load::Ready(v),
                    Err(e) => Load::Failed(e.message),
                },
            );
        });
    };
    load();

    view! {
        {move || match nodes.get() {
            Load::Loading => view! { <p class="muted small">{ui.tr("common.loading")}</p> }.into_any(),
            Load::Failed(e) => view! { <p class="error-detail">{e}</p> }.into_any(),
            Load::Ready(list) if list.is_empty() => view! { <p class="muted small">{ui.tr("explorer.empty_level")}</p> }.into_any(),
            Load::Ready(list) => view! {
                <ul class="tree-level" role="group">
                    {list.into_iter().map(|n| {
                        let expanded = RwSignal::new(false);
                        let (child_db, child_schema) = match n.kind.as_str() {
                            "database" => (Some(n.name.clone()), None),
                            _ => (database.clone(), Some(n.name.clone())),
                        };
                        let addr = ObjectAddr { database: database.clone(), schema: schema.clone(), name: n.name.clone(), kind: n.kind.clone() };
                        let cid = connection_id.clone();
                        let count = n.estimated_count.map(|c| format!(" ({c})")).unwrap_or_default();
                        let label = format!("{}{count}", n.name);
                        view! {
                            <li class="tree-node">
                                {if n.expandable {
                                    view! {
                                        <button type="button" class="tree-toggle" aria-expanded=move || expanded.get().to_string()
                                            on:click=move |_| expanded.update(|e| *e = !*e)>
                                            <span aria-hidden="true">{move || if expanded.get() { "▾" } else { "▸" }}</span>
                                            <span class="tree-kind">{n.kind.clone()}</span>
                                            {label.clone()}
                                        </button>
                                    }.into_any()
                                } else if n.readable {
                                    view! {
                                        <button type="button" class="tree-leaf" on:click=move |_| on_open.run(addr.clone())>
                                            <span class="tree-kind">{n.kind.clone()}</span>
                                            {label.clone()}
                                        </button>
                                    }.into_any()
                                } else {
                                    view! { <span class="tree-leaf muted"><span class="tree-kind">{n.kind.clone()}</span>{label.clone()}</span> }.into_any()
                                }}
                                {move || expanded.get().then(|| view! {
                                    <TreeLevel connection_id=cid.clone() database=child_db.clone() schema=child_schema.clone() on_open=on_open />
                                }.into_any())}
                            </li>
                        }
                    }).collect_view()}
                </ul>
            }.into_any(),
        }}
    }
}

/// Renders typed JSON as a collapsible tree.
fn json_tree(value: &serde_json::Value) -> AnyView {
    use serde_json::Value as J;
    match value {
        J::Object(map) if map.len() == 1 && map.keys().next().is_some_and(|k| k.starts_with('$')) => {
            view! { <code class="json-typed">{value.to_string()}</code> }.into_any()
        }
        J::Object(map) => view! {
            <ul class="json-tree">
                {map.iter().map(|(k, v)| view! {
                    <li>
                        {match v {
                            J::Object(_) | J::Array(_) => view! {
                                <details open>
                                    <summary><span class="json-key">{k.clone()}</span></summary>
                                    {json_tree(v)}
                                </details>
                            }.into_any(),
                            _ => view! { <span><span class="json-key">{k.clone()}</span>": "{json_tree(v)}</span> }.into_any(),
                        }}
                    </li>
                }).collect_view()}
            </ul>
        }.into_any(),
        J::Array(items) => view! {
            <ol class="json-tree" start="0">
                {items.iter().map(|v| view! { <li>{json_tree(v)}</li> }).collect_view()}
            </ol>
        }.into_any(),
        J::Null => view! { <span class="cell-null">"null"</span> }.into_any(),
        J::String(s) => view! { <span class="json-string">{format!("{s:?}")}</span> }.into_any(),
        other => view! { <span class="json-scalar">{other.to_string()}</span> }.into_any(),
    }
}

#[component]
fn Viewer(connection_id: String, object: ObjectAddr) -> impl IntoView {
    let ui = use_ui();
    let page = RwSignal::new(Load::<BrowsePageDto>::Loading);
    let conditions = RwSignal::new(Vec::<ConditionDto>::new());
    let match_any = RwSignal::new(false);
    let applied = RwSignal::new(None::<FilterDto>);
    let sort = RwSignal::new(None::<SortDto>);
    let offset = RwSignal::new(0u64);
    let limit = RwSignal::new(50u32);
    let mode = RwSignal::new(ViewMode::Table);
    let count = RwSignal::new(None::<CountDto>);
    let message = RwSignal::new(None::<(bool, String)>);
    let editor = RwSignal::new(Editor::Closed);
    let transfer = RwSignal::new(None::<Direction>);
    let editor_text = RwSignal::new(String::new());
    let plan = RwSignal::new(None::<EditPlanDto>);
    let busy = RwSignal::new(false);
    let confirm_delete = RwSignal::new(None::<String>);
    let cid = StoredValue::new(connection_id);
    let obj = StoredValue::new(object.clone());

    let load = move || {
        page.set(Load::Loading);
        let args = BrowseArgs {
            connection_id: cid.get_value(),
            object: obj.get_value(),
            filter: applied.get_untracked(),
            sort: sort.get_untracked(),
            offset: offset.get_untracked(),
            limit: limit.get_untracked(),
        };
        spawn_local(async move {
            page.set(
                match call(commands::EXPLORER_BROWSE, &Wrap { args: &args }).await {
                    Ok(v) => Load::Ready(v),
                    Err(e) => Load::Failed(e.message),
                },
            );
        });
    };
    load();

    let apply_filter = move || {
        let conds = conditions.get_untracked();
        applied.set((!conds.is_empty()).then(|| FilterDto {
            any: match_any.get_untracked(),
            conditions: conds,
        }));
        offset.set(0);
        count.set(None);
        load();
    };
    let run_count = move |_| {
        let args = CountArgs {
            connection_id: cid.get_value(),
            object: obj.get_value(),
            filter: applied.get_untracked(),
        };
        spawn_local(async move {
            match call::<_, CountDto>(commands::EXPLORER_COUNT, &Wrap { args: &args }).await {
                Ok(c) => count.set(Some(c)),
                Err(e) => message.set(Some((false, e.message))),
            }
        });
    };
    let toggle_sort = move |field: String| {
        sort.update(|s| {
            *s = match s.take() {
                Some(cur) if cur.field == field && !cur.descending => Some(SortDto {
                    field,
                    descending: true,
                }),
                Some(cur) if cur.field == field => None,
                _ => Some(SortDto {
                    field,
                    descending: false,
                }),
            }
        });
        offset.set(0);
        load();
    };
    let close_editor = move || {
        editor.set(Editor::Closed);
        plan.set(None);
    };
    let open_insert = move |_| {
        let template = match page.get_untracked() {
            Load::Ready(p) if p.columns.iter().any(|c| !c.native_type.is_empty()) => {
                let fields: Vec<String> = p
                    .columns
                    .iter()
                    .filter(|c| !c.key)
                    .map(|c| format!("  {}: null", serde_json::Value::String(c.name.clone())))
                    .collect();
                format!("{{\n{}\n}}", fields.join(",\n"))
            }
            _ => "{\n  \n}".to_owned(),
        };
        editor_text.set(template);
        plan.set(None);
        message.set(None);
        editor.set(Editor::Insert);
    };
    let open_edit = move |json: String| {
        editor_text.set(json.clone());
        plan.set(None);
        message.set(None);
        editor.set(Editor::Edit { original: json });
    };
    let preview = move |_| {
        let Editor::Edit { original } = editor.get_untracked() else {
            return;
        };
        let args = EditArgs {
            connection_id: cid.get_value(),
            object: obj.get_value(),
            original,
            edited: editor_text.get_untracked(),
        };
        busy.set(true);
        spawn_local(async move {
            match call::<_, EditPlanDto>(commands::EXPLORER_PLAN_UPDATE, &Wrap { args: &args })
                .await
            {
                Ok(p) => {
                    plan.set(Some(p));
                    message.set(None);
                }
                Err(e) => message.set(Some((false, e.message))),
            }
            busy.set(false);
        });
    };
    let save = move |_| {
        let Editor::Edit { original } = editor.get_untracked() else {
            return;
        };
        let args = EditArgs {
            connection_id: cid.get_value(),
            object: obj.get_value(),
            original,
            edited: editor_text.get_untracked(),
        };
        busy.set(true);
        spawn_local(async move {
            match call::<_, MutationDto>(commands::EXPLORER_APPLY_UPDATE, &Wrap { args: &args })
                .await
            {
                Ok(_) => {
                    close_editor();
                    message.set(Some((true, ui.text("explorer.saved"))));
                    load();
                }
                Err(e) => {
                    plan.set(None);
                    message.set(Some((false, e.message)));
                }
            }
            busy.set(false);
        });
    };
    let insert = move |_| {
        let args = InsertArgs {
            connection_id: cid.get_value(),
            object: obj.get_value(),
            json: editor_text.get_untracked(),
        };
        busy.set(true);
        spawn_local(async move {
            match call::<_, ()>(commands::EXPLORER_INSERT, &Wrap { args: &args }).await {
                Ok(()) => {
                    close_editor();
                    message.set(Some((true, ui.text("explorer.inserted"))));
                    count.set(None);
                    load();
                }
                Err(e) => message.set(Some((false, e.message))),
            }
            busy.set(false);
        });
    };
    let do_delete = move || {
        let Some(original) = confirm_delete.get_untracked() else {
            return;
        };
        confirm_delete.set(None);
        let args = DeleteRecordArgs {
            connection_id: cid.get_value(),
            object: obj.get_value(),
            original,
        };
        spawn_local(async move {
            match call::<_, MutationDto>(commands::EXPLORER_DELETE, &Wrap { args: &args }).await {
                Ok(_) => {
                    message.set(Some((true, ui.text("explorer.deleted"))));
                    count.set(None);
                    load();
                }
                Err(e) => message.set(Some((false, e.message))),
            }
        });
    };

    let editable = move || matches!(page.get(), Load::Ready(ref p) if p.edit_blocker.is_none());
    // Imports need a writable object, not a key.
    let importable = move || matches!(page.get(), Load::Ready(ref p) if p.edit_blocker.is_none() || p.edit_blocker.as_deref() == Some("no_primary_key"));
    let key_fields = move || match page.get_untracked() {
        Load::Ready(p) => p.key_fields,
        _ => Vec::new(),
    };
    let field_names = move || match page.get() {
        Load::Ready(p) => p.columns.into_iter().map(|c| c.name).collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    view! {
        <section class="card viewer-card" aria-labelledby="viewer-title">
            <header class="viewer-head">
                <h2 id="viewer-title"><span class="tree-kind">{object.kind.clone()}</span><bdi>{object.display()}</bdi></h2>
                <div class="toolbar">
                    <div class="segmented" role="group" aria-label=ui.tr("explorer.view_mode")>
                        <button type="button" class:active=move || mode.get() == ViewMode::Table aria-pressed=move || (mode.get() == ViewMode::Table).to_string() on:click=move |_| mode.set(ViewMode::Table)>{ui.tr("explorer.view_table")}</button>
                        <button type="button" class:active=move || mode.get() == ViewMode::Json aria-pressed=move || (mode.get() == ViewMode::Json).to_string() on:click=move |_| mode.set(ViewMode::Json)>{ui.tr("explorer.view_json")}</button>
                        <button type="button" class:active=move || mode.get() == ViewMode::Tree aria-pressed=move || (mode.get() == ViewMode::Tree).to_string() on:click=move |_| mode.set(ViewMode::Tree)>{ui.tr("explorer.view_tree")}</button>
                    </div>
                    <button type="button" class="btn btn-small btn-secondary" on:click=move |_| load()>{ui.tr("common.refresh")}</button>
                    <button type="button" class="btn btn-small btn-secondary" on:click=run_count>{ui.tr("explorer.count")}</button>
                    {move || count.get().map(|c| view! { <span class="muted small">{format!("{}: {}", ui.text("explorer.count_label"), c.count)}</span> })}
                    <button type="button" class="btn btn-small btn-secondary" on:click=move |_| transfer.set(Some(Direction::Export))>{ui.tr("transfer.export")}</button>
                    <Show when=importable>
                        <button type="button" class="btn btn-small btn-secondary" on:click=move |_| transfer.set(Some(Direction::Import))>{ui.tr("transfer.import")}</button>
                    </Show>
                    <Show when=editable>
                        <button type="button" class="btn btn-small" on:click=open_insert>{ui.tr("explorer.insert")}</button>
                    </Show>
                </div>
            </header>

            // Filter builder
            <details class="filter-box">
                <summary>
                    {ui.tr("explorer.filter")}
                    {move || applied.get().map(|f| view! { <span class="badge">{f.conditions.len()}</span> })}
                </summary>
                <datalist id="explorer-fields">
                    {move || field_names().into_iter().map(|f| view! { <option value=f></option> }).collect_view()}
                </datalist>
                <p class="muted small">{ui.tr("explorer.value_help")}</p>
                {move || conditions.get().into_iter().enumerate().map(|(i, c)| {
                    let takes_value = c.op.takes_value();
                    view! {
                        <div class="filter-row">
                            <input class="input" type="text" list="explorer-fields" aria-label=ui.tr("explorer.field")
                                placeholder=ui.tr("explorer.field") prop:value=c.field.clone()
                                on:change=move |e| conditions.update(|cs| cs[i].field = event_target_value(&e)) />
                            <select class="input" aria-label=ui.tr("explorer.operator")
                                on:change=move |e| if let Some(op) = FilterOp::parse(&event_target_value(&e)) { conditions.update(|cs| cs[i].op = op) }>
                                {FilterOp::ALL.into_iter().map(|op| view! {
                                    <option value=op.as_str() selected=op == c.op>{ui.tr(op_key(op))}</option>
                                }).collect_view()}
                            </select>
                            {takes_value.then(|| view! {
                                <input class="input mono" type="text" aria-label=ui.tr("explorer.value") placeholder=ui.tr("explorer.value")
                                    prop:value=c.value.clone()
                                    on:change=move |e| conditions.update(|cs| cs[i].value = event_target_value(&e)) />
                            })}
                            <button type="button" class="btn btn-small btn-secondary" aria-label=ui.tr("explorer.remove_condition")
                                on:click=move |_| conditions.update(|cs| { cs.remove(i); })>"×"</button>
                        </div>
                    }
                }).collect_view()}
                <div class="toolbar">
                    <button type="button" class="btn btn-small btn-secondary" on:click=move |_| conditions.update(|cs| cs.push(ConditionDto { field: String::new(), op: FilterOp::Eq, value: String::new() }))>{ui.tr("explorer.add_condition")}</button>
                    <label class="field-check">
                        <input type="checkbox" prop:checked=move || match_any.get() on:change=move |e| match_any.set(event_target_checked(&e)) />
                        <span>{ui.tr("explorer.match_any")}</span>
                    </label>
                    <button type="button" class="btn btn-small" on:click=move |_| apply_filter()>{ui.tr("explorer.apply")}</button>
                    <button type="button" class="btn btn-small btn-secondary" on:click=move |_| { conditions.set(Vec::new()); apply_filter(); }>{ui.tr("explorer.clear")}</button>
                </div>
            </details>

            <div role="status" aria-live="polite">
                {move || message.get().map(|(ok, m)| view! { <p class=if ok { "notice notice-ok" } else { "error-detail" }>{m}</p> })}
                {move || match page.get() {
                    Load::Ready(p) => p.edit_blocker.map(|b| view! { <p class="muted small">{ui.tr(blocker_key(&b))}</p> }),
                    _ => None,
                }}
            </div>

            // Export / import
            {move || transfer.get().map(|direction| view! {
                <TransferPanel
                    connection_id=cid.get_value()
                    object=obj.get_value()
                    direction=direction
                    filter=Signal::derive(move || applied.get())
                    key_fields=key_fields()
                    on_close=Callback::new(move |_| transfer.set(None))
                    on_imported=Callback::new(move |_| { count.set(None); load(); })
                />
            })}

            // Editor
            {move || (editor.get() != Editor::Closed).then(|| {
                let is_insert = editor.get_untracked() == Editor::Insert;
                view! {
                    <section class="editor record-editor" aria-labelledby="record-editor-title">
                        <h3 id="record-editor-title">{if is_insert { ui.tr("explorer.insert_title") } else { ui.tr("explorer.edit_title") }}</h3>
                        <p class="muted small">{ui.tr("explorer.editor_help")}</p>
                        <textarea class="input mono json-input" rows="14" spellcheck="false" aria-labelledby="record-editor-title"
                            prop:value=move || editor_text.get()
                            on:input=move |e| { editor_text.set(event_target_value(&e)); plan.set(None); }></textarea>
                        {move || plan.get().map(|p| view! {
                            <div class="plan">
                                {p.conflict.then(|| view! { <p class="error-detail">{ui.tr("explorer.conflict")}</p> })}
                                {if p.changes.is_empty() {
                                    view! { <p class="muted">{ui.tr("explorer.no_changes")}</p> }.into_any()
                                } else {
                                    view! {
                                        <table class="grid">
                                            <thead><tr>
                                                <th scope="col">{ui.tr("explorer.field")}</th>
                                                <th scope="col">{ui.tr("explorer.before")}</th>
                                                <th scope="col">{ui.tr("explorer.after")}</th>
                                            </tr></thead>
                                            <tbody>
                                                {p.changes.into_iter().map(|c| view! {
                                                    <tr>
                                                        <th scope="row" class="mono">{c.field}</th>
                                                        <td class="mono">{c.before.unwrap_or_else(|| ui.text("explorer.absent"))}</td>
                                                        <td class="mono">{c.after.unwrap_or_else(|| ui.text("explorer.removed"))}</td>
                                                    </tr>
                                                }).collect_view()}
                                            </tbody>
                                        </table>
                                    }.into_any()
                                }}
                            </div>
                        })}
                        <div class="toolbar">
                            {if is_insert {
                                view! { <button type="button" class="btn" disabled=move || busy.get() on:click=insert>{ui.tr("explorer.insert")}</button> }.into_any()
                            } else {
                                view! {
                                    <button type="button" class="btn btn-secondary" disabled=move || busy.get() on:click=preview>{ui.tr("explorer.preview")}</button>
                                    <button type="button" class="btn"
                                        disabled=move || busy.get() || !plan.get().is_some_and(|p| !p.conflict && !p.changes.is_empty())
                                        on:click=save>{ui.tr("explorer.save_changes")}</button>
                                }.into_any()
                            }}
                            <button type="button" class="btn btn-secondary" on:click=move |_| close_editor()>{ui.tr("common.cancel")}</button>
                        </div>
                    </section>
                }
            })}

            // Data
            {move || match page.get() {
                Load::Loading => view! { <p>{ui.tr("common.loading")}</p> }.into_any(),
                Load::Failed(e) => view! { <ErrorBox message=e retry=load /> }.into_any(),
                Load::Ready(p) if p.rows.is_empty() => view! { <p class="muted">{ui.tr("explorer.no_records")}</p> }.into_any(),
                Load::Ready(p) => {
                    let can_edit = p.edit_blocker.is_none();
                    match mode.get() {
                        ViewMode::Table => view! {
                            <div class="grid-wrap">
                                <table class="grid">
                                    <thead><tr>
                                        {p.columns.iter().map(|c| {
                                            let name = c.name.clone();
                                            let name_sort = c.name.clone();
                                            let title = if c.native_type.is_empty() { c.name.clone() } else { format!("{} ({})", c.name, c.native_type) };
                                            let arrow = move || match sort.get() {
                                                Some(s) if s.field == name => if s.descending { " ▼" } else { " ▲" },
                                                _ => "",
                                            };
                                            view! {
                                                <th scope="col" title=title>
                                                    <button type="button" class="sort-btn" on:click=move |_| toggle_sort(name_sort.clone())>
                                                        {c.key.then_some("🔑 ")}<bdi>{c.name.clone()}</bdi>{arrow}
                                                    </button>
                                                </th>
                                            }
                                        }).collect_view()}
                                        <th scope="col"><span class="sr-only">{ui.tr("explorer.actions")}</span></th>
                                    </tr></thead>
                                    <tbody>
                                        {p.rows.into_iter().map(|r| {
                                            let (json_e, json_d) = (r.json.clone(), r.json.clone());
                                            view! {
                                                <tr>
                                                    {r.cells.into_iter().map(|c| match c.kind.as_str() {
                                                        "missing" => view! { <td class="cell-missing" title=ui.tr("explorer.missing_hint")>"—"</td> }.into_any(),
                                                        "null" => view! { <td class="cell-null">"null"</td> }.into_any(),
                                                        kind => {
                                                            let title = c.text.clone();
                                                            view! { <td class=format!("cell cell-{kind}") title=title>{c.text}</td> }.into_any()
                                                        }
                                                    }).collect_view()}
                                                    <td class="row-actions">
                                                        {can_edit.then(|| view! {
                                                            <button type="button" class="btn btn-small btn-secondary" on:click=move |_| open_edit(json_e.clone())>{ui.tr("connections.edit")}</button>
                                                            <button type="button" class="btn btn-small btn-danger" on:click=move |_| confirm_delete.set(Some(json_d.clone()))>{ui.tr("connections.delete")}</button>
                                                        })}
                                                    </td>
                                                </tr>
                                            }
                                        }).collect_view()}
                                    </tbody>
                                </table>
                            </div>
                        }.into_any(),
                        ViewMode::Json => view! {
                            <div class="json-list">
                                {p.rows.into_iter().map(|r| {
                                    let (json_e, json_d) = (r.json.clone(), r.json.clone());
                                    view! {
                                        <article class="json-record">
                                            <pre class="mono">{r.json}</pre>
                                            {can_edit.then(|| view! {
                                                <div class="toolbar">
                                                    <button type="button" class="btn btn-small btn-secondary" on:click=move |_| open_edit(json_e.clone())>{ui.tr("connections.edit")}</button>
                                                    <button type="button" class="btn btn-small btn-danger" on:click=move |_| confirm_delete.set(Some(json_d.clone()))>{ui.tr("connections.delete")}</button>
                                                </div>
                                            })}
                                        </article>
                                    }
                                }).collect_view()}
                            </div>
                        }.into_any(),
                        ViewMode::Tree => view! {
                            <div class="json-list">
                                {p.rows.into_iter().map(|r| {
                                    let parsed: serde_json::Value = serde_json::from_str(&r.json).unwrap_or_default();
                                    view! { <article class="json-record">{json_tree(&parsed)}</article> }
                                }).collect_view()}
                            </div>
                        }.into_any(),
                    }
                }
            }}

            // Paging
            <div class="toolbar pager">
                <button type="button" class="btn btn-small btn-secondary"
                    disabled=move || offset.get() == 0
                    on:click=move |_| { offset.update(|o| *o = o.saturating_sub(u64::from(limit.get_untracked()))); load(); }>{ui.tr("explorer.previous")}</button>
                <span class="muted small">
                    {move || match page.get() {
                        Load::Ready(p) if !p.rows.is_empty() => format!("{}–{}", p.offset + 1, p.offset + p.rows.len() as u64),
                        _ => String::new(),
                    }}
                </span>
                <button type="button" class="btn btn-small btn-secondary"
                    disabled=move || !matches!(page.get(), Load::Ready(ref p) if p.has_more)
                    on:click=move |_| { offset.update(|o| *o += u64::from(limit.get_untracked())); load(); }>{ui.tr("explorer.next")}</button>
                <label class="field-inline">
                    <span>{ui.tr("explorer.page_size")}</span>
                    <select class="input" on:change=move |e| {
                        if let Ok(n) = event_target_value(&e).parse() { limit.set(n); offset.set(0); load(); }
                    }>
                        {PAGE_SIZES.into_iter().map(|n| view! { <option value=n.to_string() selected=move || limit.get() == n>{n}</option> }).collect_view()}
                    </select>
                </label>
            </div>

            <Show when=move || confirm_delete.get().is_some()>
                <div class="palette-backdrop">
                    <div class="palette confirm" role="alertdialog" aria-modal="true" aria-labelledby="delete-record-title">
                        <h2 id="delete-record-title">{ui.tr("explorer.delete_title")}</h2>
                        <pre class="mono confirm-preview">{move || confirm_delete.get().unwrap_or_default()}</pre>
                        <p class="muted small">{ui.tr("explorer.delete_body")}</p>
                        <div class="toolbar">
                            <button type="button" class="btn btn-danger" on:click=move |_| do_delete()>{ui.tr("connections.delete")}</button>
                            <button type="button" class="btn btn-secondary" on:click=move |_| confirm_delete.set(None)>{ui.tr("common.cancel")}</button>
                        </div>
                    </div>
                </div>
            </Show>
        </section>
    }
}
