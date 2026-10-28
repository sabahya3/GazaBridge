//! Export to and import from files, for the object shown in the Explorer.

use std::time::Duration;

use gazabridge_ipc::{
    CountArgs, CountDto, ExportArgs, FilterDto, ImportArgs, NativeQueryDto, ObjectAddr, OpArgs,
    PickPathArgs, TransferStatusDto, commands, detect_format,
};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use crate::app::use_ui;
use crate::ipc::call;

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Export,
    Import,
}

fn op_id() -> String {
    format!("op-{}", js_sys::Date::now() as u64)
}

fn human_bytes(n: u64) -> String {
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

fn file_name(object: &ObjectAddr, format: &str, gzip: bool) -> String {
    let base: String = object
        .name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{base}.{format}{}", if gzip { ".gz" } else { "" })
}

#[component]
pub fn TransferPanel(
    connection_id: String,
    object: ObjectAddr,
    direction: Direction,
    /// The filter applied in the viewer (exports can use it).
    filter: Signal<Option<FilterDto>>,
    /// The query bar's applied query (filter, projection and sort).
    query: Signal<Option<NativeQueryDto>>,
    /// The query bar's Skip and Limit.
    skip: Signal<u64>,
    limit: Signal<Option<u64>>,
    /// How many records the viewer's current filter or query matches.
    matching: Signal<Option<CountDto>>,
    key_fields: Vec<String>,
    on_close: Callback<()>,
    /// Called after a successful import.
    on_imported: Callback<()>,
) -> impl IntoView {
    let ui = use_ui();
    let is_collection = object.kind == "collection";
    let format = RwSignal::new("json".to_owned());
    let flavor = RwSignal::new(
        if is_collection {
            "mongo_relaxed"
        } else {
            "typed"
        }
        .to_owned(),
    );
    let gzip = RwSignal::new(false);
    let pretty = RwSignal::new(false);
    let has_query = move || {
        filter.get().is_some() || query.get().is_some() || skip.get() > 0 || limit.get().is_some()
    };
    let use_filter = RwSignal::new(has_query());
    // Size of the whole object, for the "everything" choice.
    let total = RwSignal::new(None::<CountDto>);
    let path = RwSignal::new(String::new());
    let csv_columns = RwSignal::new(String::new());
    let csv_delimiter = RwSignal::new(",".to_owned());
    let csv_null = RwSignal::new(String::new());
    let csv_ignore = RwSignal::new(false);
    let mode = RwSignal::new("insert".to_owned());
    let keys = RwSignal::new(key_fields.join(", "));
    let malformed = RwSignal::new("fail".to_owned());
    let stop_on_error = RwSignal::new(true);
    let status = RwSignal::new(None::<TransferStatusDto>);
    let message = RwSignal::new(None::<String>);
    let confirm_replace = RwSignal::new(false);
    let current_op = StoredValue::new(String::new());
    let cid = StoredValue::new(connection_id);
    let obj = StoredValue::new(object.clone());

    let running = move || status.get().is_some_and(|s| s.phase == "running");
    if direction == Direction::Export {
        let args = CountArgs {
            connection_id: cid.get_value(),
            object: obj.get_value(),
            filter: None,
            query: None,
            exact: false,
            max_time_ms: None,
        };
        spawn_local(async move {
            if let Ok(c) =
                call::<_, Option<CountDto>>(commands::EXPLORER_COUNT, &Wrap { args: &args }).await
            {
                total.set(c);
            }
        });
    }
    let count_text = move |c: Option<CountDto>| match c {
        Some(c) if c.exact => format!("{} {}", c.count, ui.text("transfer.records")),
        Some(c) => format!("~{} {}", c.count, ui.text("transfer.records")),
        None => String::new(),
    };
    let query_text = move || {
        let mut parts = Vec::new();
        if let Some(q) = query.get() {
            if !q.filter.trim().is_empty() {
                parts.push(format!(
                    "{}: {}",
                    ui.text("explorer.q_filter"),
                    q.filter.trim()
                ));
            }
            for (key, text) in [
                ("explorer.q_project", &q.project),
                ("explorer.q_sort", &q.sort),
            ] {
                let t = text.trim();
                if !t.is_empty() && t != "{}" {
                    parts.push(format!("{}: {t}", ui.text(key)));
                }
            }
        }
        if skip.get() > 0 {
            parts.push(format!("{}: {}", ui.text("explorer.q_skip"), skip.get()));
        }
        if let Some(l) = limit.get() {
            parts.push(format!("{}: {l}", ui.text("explorer.q_limit"),));
        }
        if let Some(f) = filter.get() {
            let joiner = if f.any { " OR " } else { " AND " };
            parts.push(
                f.conditions
                    .iter()
                    .map(|c| {
                        format!("{} {} {}", c.field, c.op.as_str(), c.value)
                            .trim()
                            .to_owned()
                    })
                    .collect::<Vec<_>>()
                    .join(joiner),
            );
        }
        parts.join("  ·  ")
    };
    // What the query exports: its matches, after Skip, at most Limit.
    let query_count = move || {
        let (skip, limit) = (skip.get(), limit.get());
        match matching.get() {
            Some(mut c) => {
                c.count = c.count.saturating_sub(skip).min(limit.unwrap_or(u64::MAX));
                Some(c)
            }
            None => limit.map(|l| CountDto {
                count: l,
                exact: false,
            }),
        }
    };

    let poll = move |op: String| {
        let handle = StoredValue::new(None::<IntervalHandle>);
        let tick = move || {
            let op = op.clone();
            spawn_local(async move {
                match call::<_, TransferStatusDto>(
                    commands::TRANSFER_STATUS,
                    &Wrap {
                        args: &OpArgs { op_id: op.clone() },
                    },
                )
                .await
                {
                    Ok(s) => {
                        let finished = s.phase != "running";
                        if finished {
                            if let Some(h) = handle.get_value() {
                                h.clear();
                            }
                            if s.phase == "done" && direction == Direction::Import {
                                on_imported.run(());
                            }
                            let _ = call::<_, ()>(
                                commands::TRANSFER_FORGET,
                                &Wrap {
                                    args: &OpArgs { op_id: op },
                                },
                            )
                            .await;
                        }
                        status.set(Some(s));
                    }
                    Err(e) => {
                        if let Some(h) = handle.get_value() {
                            h.clear();
                        }
                        message.set(Some(e.message));
                    }
                }
            });
        };
        if let Ok(h) = set_interval_with_handle(tick, Duration::from_millis(400)) {
            handle.set_value(Some(h));
        }
    };

    let choose = move |_| {
        let save = direction == Direction::Export;
        let args = PickPathArgs {
            save,
            default_name: save.then(|| {
                file_name(
                    &obj.get_value(),
                    &format.get_untracked(),
                    gzip.get_untracked(),
                )
            }),
            extensions: if save {
                vec![format.get_untracked()]
            } else {
                ["json", "jsonl", "ndjson", "bson", "gz"]
                    .map(String::from)
                    .to_vec()
            },
        };
        spawn_local(async move {
            match call::<_, Option<String>>(commands::PICK_FILE_PATH, &Wrap { args: &args }).await {
                Ok(Some(p)) => {
                    if !save && let Some((f, gz)) = detect_format(&p) {
                        format.set(f.to_owned());
                        gzip.set(gz);
                    }
                    path.set(p);
                }
                Ok(None) => {}
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let start = move |overwrite: bool| {
        message.set(None);
        confirm_replace.set(false);
        let op = op_id();
        current_op.set_value(op.clone());
        let split = |s: String| {
            s.split(',')
                .map(|x| x.trim().to_owned())
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>()
        };
        spawn_local(async move {
            let result = match direction {
                Direction::Export => {
                    let args = ExportArgs {
                        connection_id: cid.get_value(),
                        op_id: op.clone(),
                        object: obj.get_value(),
                        filter: if use_filter.get_untracked() {
                            filter.get_untracked()
                        } else {
                            None
                        },
                        query: if use_filter.get_untracked() {
                            query.get_untracked()
                        } else {
                            None
                        },
                        skip: if use_filter.get_untracked() {
                            skip.get_untracked()
                        } else {
                            0
                        },
                        limit: if use_filter.get_untracked() {
                            limit.get_untracked()
                        } else {
                            None
                        },
                        path: path.get_untracked(),
                        format: format.get_untracked(),
                        json_flavor: flavor.get_untracked(),
                        gzip: gzip.get_untracked(),
                        pretty: pretty.get_untracked(),
                        overwrite,
                        csv_columns: split(csv_columns.get_untracked()),
                        csv_delimiter: csv_delimiter.get_untracked(),
                        csv_null_text: csv_null.get_untracked(),
                        csv_ignore_extra: csv_ignore.get_untracked(),
                    };
                    call::<_, ()>(commands::TRANSFER_EXPORT, &Wrap { args: &args }).await
                }
                Direction::Import => {
                    let p = path.get_untracked();
                    let ext = if format.get_untracked() == "bson" {
                        "bson"
                    } else {
                        "jsonl"
                    };
                    let args = ImportArgs {
                        connection_id: cid.get_value(),
                        op_id: op.clone(),
                        object: obj.get_value(),
                        quarantine_path: (malformed.get_untracked() == "quarantine")
                            .then(|| format!("{p}.rejected-{}.{ext}", js_sys::Date::now() as u64)),
                        path: p,
                        format: format.get_untracked(),
                        json_flavor: flavor.get_untracked(),
                        gzip: gzip.get_untracked(),
                        malformed: malformed.get_untracked(),
                        mode: mode.get_untracked(),
                        key_fields: split(keys.get_untracked()),
                        batch_size: 1000,
                        stop_on_error: stop_on_error.get_untracked(),
                    };
                    call::<_, ()>(commands::TRANSFER_IMPORT, &Wrap { args: &args }).await
                }
            };
            match result {
                Ok(()) => {
                    status.set(Some(TransferStatusDto {
                        phase: "running".into(),
                        records: 0,
                        summary: Default::default(),
                        error: None,
                        error_code: None,
                    }));
                    poll(op);
                }
                Err(e) if e.code == "file_exists" => confirm_replace.set(true),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let cancel = move |_| {
        let op = current_op.get_value();
        spawn_local(async move {
            let _ = call::<_, bool>(
                commands::TRANSFER_CANCEL,
                &Wrap {
                    args: &OpArgs { op_id: op },
                },
            )
            .await;
        });
    };

    let is_json = move || matches!(format.get().as_str(), "json" | "jsonl");
    let title = if direction == Direction::Export {
        ui.tr("transfer.export_title")
    } else {
        ui.tr("transfer.import_title")
    };

    view! {
        <section class="transfer" aria-labelledby="transfer-title">
            <h3 id="transfer-title">{title}" — "<bdi>{object.display()}</bdi></h3>

            {(direction == Direction::Export).then(|| view! {
                <fieldset class="field-group scope">
                    <legend>{ui.tr("transfer.what")}</legend>
                    <label class="scope-option">
                        <input type="radio" name="export-scope" prop:checked=move || !use_filter.get() on:change=move |_| use_filter.set(false) />
                        <span class="scope-text">
                            <strong>{if is_collection { ui.tr("transfer.all_collection") } else { ui.tr("transfer.all_table") }}</strong>
                            <span class="muted small">{move || count_text(total.get())}</span>
                        </span>
                    </label>
                    <label class="scope-option" class:disabled=move || !has_query()>
                        <input type="radio" name="export-scope" disabled=move || !has_query() prop:checked=move || use_filter.get() && has_query() on:change=move |_| use_filter.set(true) />
                        <span class="scope-text">
                            <strong>{ui.tr("transfer.current_query")}</strong>
                            {move || if has_query() {
                                view! {
                                    <code class="mono small scope-query"><bdi>{query_text()}</bdi></code>
                                    <span class="muted small">{count_text(query_count())}</span>
                                }.into_any()
                            } else {
                                view! { <span class="muted small">{ui.tr("transfer.no_query")}</span> }.into_any()
                            }}
                        </span>
                    </label>
                    <p class="muted small">{ui.tr("transfer.sort_note")}</p>
                </fieldset>
            })}

            <div class="toolbar">
                <label class="field-inline">
                    <span>{ui.tr("transfer.format")}</span>
                    <select class="input" on:change=move |e| {
                        let f = event_target_value(&e);
                        if f == "xlsx" {
                            gzip.set(false);
                        }
                        format.set(f);
                    }>
                        <option value="json" selected=move || format.get() == "json">"JSON"</option>
                        <option value="jsonl" selected=move || format.get() == "jsonl">{ui.tr("transfer.jsonl")}</option>
                        {(direction == Direction::Export).then(|| view! {
                            <option value="csv" selected=move || format.get() == "csv">"CSV"</option>
                            <option value="xlsx" selected=move || format.get() == "xlsx">{ui.tr("transfer.xlsx")}</option>
                        })}
                        <option value="bson" selected=move || format.get() == "bson">{ui.tr("transfer.bson")}</option>
                    </select>
                </label>
                <Show when=is_json>
                    <label class="field-inline">
                        <span>{ui.tr("transfer.flavor")}</span>
                        <select class="input" on:change=move |e| flavor.set(event_target_value(&e))>
                            <option value="typed" selected=move || flavor.get() == "typed">{ui.tr("transfer.flavor_typed")}</option>
                            <option value="mongo_relaxed" selected=move || flavor.get() == "mongo_relaxed">{ui.tr("transfer.flavor_relaxed")}</option>
                            <option value="mongo_canonical" selected=move || flavor.get() == "mongo_canonical">{ui.tr("transfer.flavor_canonical")}</option>
                        </select>
                    </label>
                </Show>
                <Show when=move || format.get() != "xlsx">
                    <label class="field-check">
                        <input type="checkbox" prop:checked=move || gzip.get() on:change=move |e| gzip.set(event_target_checked(&e)) />
                        <span>"gzip"</span>
                    </label>
                </Show>
            </div>
            <Show when=is_json>
                <p class="muted small">{ui.tr("transfer.flavor_help")}</p>
            </Show>

            <div class="toolbar path-row">
                <input class="input mono" type="text" aria-label=ui.tr("transfer.path") placeholder=ui.tr("transfer.path")
                    prop:value=move || path.get() on:input=move |e| {
                        let v = event_target_value(&e);
                        if direction == Direction::Import && let Some((f, gz)) = detect_format(&v) {
                            format.set(f.to_owned());
                            gzip.set(gz);
                        }
                        path.set(v);
                    } />
                <button type="button" class="btn btn-secondary" on:click=choose>{ui.tr("transfer.choose")}</button>
            </div>

            {match direction {
                Direction::Export => view! {
                    <div class="toolbar">
                        <Show when=move || format.get() == "json">
                            <label class="field-check">
                                <input type="checkbox" prop:checked=move || pretty.get() on:change=move |e| pretty.set(event_target_checked(&e)) />
                                <span>{ui.tr("transfer.pretty")}</span>
                            </label>
                        </Show>
                    </div>
                    <Show when=move || matches!(format.get().as_str(), "csv" | "xlsx")>
                        <fieldset class="field-group">
                            <legend>{ui.tr("transfer.columns_title")}</legend>
                            <label class="field">
                                <span>{ui.tr("transfer.csv_columns")}</span>
                                <input class="input mono" type="text" prop:value=move || csv_columns.get() on:input=move |e| csv_columns.set(event_target_value(&e)) />
                                <span class="muted small">{ui.tr("transfer.csv_columns_help")}</span>
                            </label>
                            <div class="toolbar">
                                <Show when=move || format.get() == "csv">
                                <label class="field-inline">
                                    <span>{ui.tr("transfer.csv_delimiter")}</span>
                                    <select class="input" on:change=move |e| csv_delimiter.set(event_target_value(&e))>
                                        <option value=",">","</option>
                                        <option value=";">";"</option>
                                        <option value="tab">"Tab"</option>
                                        <option value="|">"|"</option>
                                    </select>
                                </label>
                                </Show>
                                <label class="field-inline">
                                    <span>{ui.tr("transfer.csv_null")}</span>
                                    <input class="input mono" type="text" size="6" prop:value=move || csv_null.get() on:input=move |e| csv_null.set(event_target_value(&e)) />
                                </label>
                            </div>
                            <label class="field-check">
                                <input type="checkbox" prop:checked=move || csv_ignore.get() on:change=move |e| csv_ignore.set(event_target_checked(&e)) />
                                <span>{ui.tr("transfer.csv_ignore")}</span>
                            </label>
                            <p class="muted small">{move || if format.get() == "xlsx" { ui.tr("transfer.xlsx_loss") } else { ui.tr("transfer.csv_loss") }}</p>
                        </fieldset>
                    </Show>
                }.into_any(),
                Direction::Import => view! {
                    <div class="toolbar">
                        <label class="field-inline">
                            <span>{ui.tr("transfer.mode")}</span>
                            <select class="input" on:change=move |e| mode.set(event_target_value(&e))>
                                <option value="insert">{ui.tr("transfer.mode_insert")}</option>
                                <option value="upsert">{ui.tr("transfer.mode_upsert")}</option>
                            </select>
                        </label>
                        <Show when=move || mode.get() == "upsert">
                            <label class="field-inline">
                                <span>{ui.tr("transfer.keys")}</span>
                                <input class="input mono" type="text" prop:value=move || keys.get() on:input=move |e| keys.set(event_target_value(&e)) />
                            </label>
                        </Show>
                    </div>
                    <div class="toolbar">
                        <label class="field-inline">
                            <span>{ui.tr("transfer.malformed")}</span>
                            <select class="input" on:change=move |e| malformed.set(event_target_value(&e))>
                                <option value="fail">{ui.tr("transfer.malformed_fail")}</option>
                                <option value="skip">{ui.tr("transfer.malformed_skip")}</option>
                                <option value="quarantine">{ui.tr("transfer.malformed_quarantine")}</option>
                            </select>
                        </label>
                        <label class="field-check">
                            <input type="checkbox" prop:checked=move || stop_on_error.get() on:change=move |e| stop_on_error.set(event_target_checked(&e)) />
                            <span>{ui.tr("transfer.stop_on_error")}</span>
                        </label>
                    </div>
                }.into_any(),
            }}

            <div role="status" aria-live="polite">
                {move || message.get().map(|m| view! { <p class="error-detail">{m}</p> })}
                {move || status.get().map(|s| {
                    let sum = s.summary.clone();
                    match s.phase.as_str() {
                        "running" => view! { <p aria-busy="true">{format!("{} {}", ui.text("transfer.progress"), s.records)}</p> }.into_any(),
                        "done" => view! {
                            <div class="test-result ok">
                                <strong>{ui.tr("transfer.done")}</strong>
                                <ul>
                                    {match &sum.file {
                                        Some(f) => view! {
                                            <li>{format!("{}: {}", ui.text("query.rows_label"), f.records)}</li>
                                            <li>{format!("{}: {}", ui.text("transfer.size"), human_bytes(f.bytes))}</li>
                                            <li class="mono small">{format!("SHA-256: {}", f.sha256)}</li>
                                            <li class="mono small"><bdi>{f.path.clone()}</bdi></li>
                                            {(!f.ignored_fields.is_empty()).then(|| view! { <li>{format!("{}: {}", ui.text("transfer.ignored"), f.ignored_fields.join(", "))}</li> })}
                                        }.into_any(),
                                        None => view! {
                                            <li>{format!("{}: {}", ui.text("transfer.read"), sum.records_read)}</li>
                                            <li>{format!("{}: {} · {}: {}", ui.text("transfer.inserted"), sum.inserted, ui.text("transfer.updated"), sum.updated)}</li>
                                            {(sum.failed > 0).then(|| view! { <li class="finding-error">{format!("{}: {}", ui.text("transfer.failed"), sum.failed)}</li> })}
                                            {(sum.malformed > 0).then(|| view! { <li class="finding-warning">{format!("{}: {}", ui.text("transfer.malformed_count"), sum.malformed)}</li> })}
                                            {sum.quarantine_path.clone().map(|q| view! { <li class="mono small"><bdi>{q}</bdi></li> })}
                                        }.into_any(),
                                    }}
                                </ul>
                                {(!sum.messages.is_empty()).then(|| view! {
                                    <details><summary>{ui.tr("transfer.messages")}</summary>
                                        <ul class="mono small">{sum.messages.iter().map(|m| view! { <li>{m.clone()}</li> }).collect_view()}</ul>
                                    </details>
                                })}
                            </div>
                        }.into_any(),
                        "cancelled" => view! { <p class="muted">{ui.tr("transfer.cancelled")}</p> }.into_any(),
                        _ => view! {
                            <div class="test-result fail">
                                <strong>{ui.tr("transfer.failed_title")}</strong>
                                <p class="error-detail">{s.error.clone().unwrap_or_default()}</p>
                                {(!sum.messages.is_empty()).then(|| view! {
                                    <ul class="mono small">{sum.messages.iter().map(|m| view! { <li>{m.clone()}</li> }).collect_view()}</ul>
                                })}
                            </div>
                        }.into_any(),
                    }
                })}
            </div>

            <Show when=move || confirm_replace.get()>
                <div class="test-result fail">
                    <p>{ui.tr("transfer.replace_question")}</p>
                    <div class="toolbar">
                        <button type="button" class="btn btn-danger" on:click=move |_| start(true)>{ui.tr("transfer.replace")}</button>
                        <button type="button" class="btn btn-secondary" on:click=move |_| confirm_replace.set(false)>{ui.tr("common.cancel")}</button>
                    </div>
                </div>
            </Show>

            <div class="toolbar modal-footer">
                <button type="button" class="btn" disabled=move || running() || path.get().trim().is_empty() on:click=move |_| start(false)>
                    {if direction == Direction::Export { ui.tr("transfer.start_export") } else { ui.tr("transfer.start_import") }}
                </button>
                <Show when=running>
                    <button type="button" class="btn btn-danger" on:click=cancel>{ui.tr("query.cancel")}</button>
                </Show>
                <button type="button" class="btn btn-secondary" disabled=running on:click=move |_| on_close.run(())>{ui.tr("transfer.close")}</button>
            </div>
        </section>
    }
}
