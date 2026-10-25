//! Aggregation pipeline editor for a collection: stages with previews, or
//! the whole pipeline as text (mongosh syntax), kept in sync.

use gazabridge_ipc::{
    CONFIRMATION_REQUIRED, ObjectAddr, QueryResultDto, RunQueryArgs, commands, risks_from_message,
};
use gazabridge_query::builder::to_shell;
use gazabridge_query::shell;
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;
use serde_json::Value as J;

use crate::app::use_ui;
use crate::icons::{Icon, icon};
use crate::ipc::call;

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

#[derive(Debug, Clone, PartialEq)]
struct Stage {
    op: String,
    body: String,
    enabled: bool,
}

/// Stage operators offered, with a starting body for each.
const STAGES: [(&str, &str); 18] = [
    ("$match", "{ field: \"value\" }"),
    ("$project", "{ field: 1 }"),
    ("$addFields", "{ newField: \"$field\" }"),
    ("$set", "{ field: \"value\" }"),
    ("$unset", "\"field\""),
    ("$group", "{ _id: \"$field\", count: { $sum: 1 } }"),
    ("$sort", "{ field: -1 }"),
    ("$limit", "10"),
    ("$skip", "10"),
    ("$unwind", "\"$field\""),
    (
        "$lookup",
        "{ from: \"otherCollection\", localField: \"field\", foreignField: \"_id\", as: \"joined\" }",
    ),
    ("$count", "\"total\""),
    ("$sample", "{ size: 10 }"),
    ("$replaceRoot", "{ newRoot: \"$field\" }"),
    ("$facet", "{ byStatus: [{ $sortByCount: \"$status\" }] }"),
    (
        "$bucket",
        "{ groupBy: \"$field\", boundaries: [0, 10, 100], default: \"other\" }",
    ),
    ("$out", "\"targetCollection\""),
    ("$merge", "{ into: \"targetCollection\" }"),
];

const PREVIEW_ROWS: u32 = 20;

fn new_stage() -> Stage {
    Stage {
        op: STAGES[0].0.into(),
        body: STAGES[0].1.into(),
        enabled: true,
    }
}

/// The pipeline (enabled stages up to `upto`) as strict JSON.
fn pipeline_json(stages: &[Stage], upto: Option<usize>) -> Result<String, String> {
    let mut out = Vec::new();
    for (i, s) in stages.iter().enumerate() {
        if upto.is_some_and(|u| i > u) {
            break;
        }
        if !s.enabled {
            continue;
        }
        let body = shell::parse(&s.body).map_err(|e| format!("{} {}: {e}", i + 1, s.op))?;
        let mut m = serde_json::Map::new();
        m.insert(s.op.clone(), body);
        out.push(J::Object(m));
    }
    serde_json::to_string(&out).map_err(|e| e.to_string())
}

fn pipeline_text(stages: &[Stage]) -> Result<String, String> {
    let mut parts = Vec::new();
    for (i, s) in stages.iter().filter(|s| s.enabled).enumerate() {
        let body = shell::parse(&s.body).map_err(|e| format!("{} {}: {e}", i + 1, s.op))?;
        parts.push(format!("  {{ {}: {} }}", s.op, to_shell(&body)));
    }
    Ok(format!("[\n{}\n]", parts.join(",\n")))
}

fn stages_from_text(text: &str) -> Result<Vec<Stage>, String> {
    let J::Array(items) = shell::parse(text).map_err(|e| e.to_string())? else {
        return Err("a pipeline is a list of stages: [ { $match: { … } }, … ]".into());
    };
    items
        .into_iter()
        .enumerate()
        .map(|(i, item)| match item {
            J::Object(m) if m.len() == 1 => {
                let (op, body) = m.into_iter().next().unwrap_or_default();
                Ok(Stage {
                    op,
                    body: to_shell(&body),
                    enabled: true,
                })
            }
            _ => Err(format!("stage {} must have exactly one operator", i + 1)),
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Stages,
    Text,
}

#[component]
pub fn AggregationView(connection_id: String, object: ObjectAddr) -> impl IntoView {
    let ui = use_ui();
    let stages = RwSignal::new(vec![new_stage()]);
    let mode = RwSignal::new(Mode::Stages);
    let text = RwSignal::new(String::new());
    let result = RwSignal::new(None::<Result<(Option<usize>, QueryResultDto), String>>);
    let running = RwSignal::new(false);
    let confirm = RwSignal::new(None::<(Vec<String>, Option<usize>)>);
    let error = RwSignal::new(None::<String>);
    let cid = StoredValue::new(connection_id);
    let obj = StoredValue::new(object);

    let switch = move |to: Mode| {
        error.set(None);
        let r = match (mode.get_untracked(), to) {
            (Mode::Stages, Mode::Text) => {
                pipeline_text(&stages.get_untracked()).map(|t| text.set(t))
            }
            (Mode::Text, Mode::Stages) => {
                stages_from_text(&text.get_untracked()).map(|s| stages.set(s))
            }
            _ => Ok(()),
        };
        match r {
            Ok(()) => mode.set(to),
            Err(e) => error.set(Some(e)),
        }
    };

    // Runs the pipeline (up to a stage for previews).
    let run = move |upto: Option<usize>, confirmed: bool| {
        error.set(None);
        if mode.get_untracked() == Mode::Text {
            match stages_from_text(&text.get_untracked()) {
                Ok(s) => stages.set(s),
                Err(e) => {
                    error.set(Some(e));
                    return;
                }
            }
        }
        let pipeline = match pipeline_json(&stages.get_untracked(), upto) {
            Ok(p) => p,
            Err(e) => {
                error.set(Some(e));
                return;
            }
        };
        let o = obj.get_value();
        let args = RunQueryArgs {
            connection_id: cid.get_value(),
            run_id: format!("agg-{}", js_sys::Date::now() as u64),
            language: "mongo_aggregation".into(),
            text: pipeline,
            params: Vec::new(),
            database: o.database.clone(),
            collection: Some(o.name.clone()),
            max_rows: PREVIEW_ROWS,
            timeout_secs: Some(30),
            confirmed,
        };
        running.set(true);
        spawn_local(async move {
            match call::<_, QueryResultDto>(commands::QUERY_RUN, &Wrap { args: &args }).await {
                Ok(r) => result.set(Some(Ok((upto, r)))),
                Err(e) if e.code == CONFIRMATION_REQUIRED => {
                    confirm.set(Some((risks_from_message(&e.message), upto)))
                }
                Err(e) => result.set(Some(Err(e.message))),
            }
            running.set(false);
        });
    };

    view! {
        <section class="aggregation" aria-label=ui.tr("agg.title")>
            <div class="qb-top">
                <div class="segmented" role="group" aria-label=ui.tr("qb.mode")>
                    <button type="button" class:active=move || mode.get() == Mode::Stages on:click=move |_| switch(Mode::Stages)>{icon(Icon::Layers)}<span>{ui.tr("agg.stages")}</span></button>
                    <button type="button" class:active=move || mode.get() == Mode::Text on:click=move |_| switch(Mode::Text)>{icon(Icon::Code)}<span>{ui.tr("qb.text")}</span></button>
                </div>
                <span class="muted small">{ui.tr("agg.help")}</span>
                <span class="spacer"></span>
                <button type="button" class="btn btn-small" disabled=move || running.get() on:click=move |_| run(None, false)>{icon(Icon::Play)}{ui.tr("agg.run")}</button>
            </div>

            {move || match mode.get() {
                Mode::Text => view! {
                    <textarea class="input mono agg-text" rows="12" spellcheck="false" aria-label=ui.tr("agg.title")
                        placeholder="[\n  { $match: { status: \"open\" } },\n  { $group: { _id: \"$city\", n: { $sum: 1 } } }\n]"
                        prop:value=move || text.get() on:input=move |e| text.set(event_target_value(&e))></textarea>
                }.into_any(),
                Mode::Stages => view! {
                    <ol class="agg-stages">
                        {move || stages.get().into_iter().enumerate().map(|(i, st)| {
                            let op = st.op.clone();
                            let enabled = st.enabled;
                            let count = stages.with(Vec::len);
                            view! {
                                <li class="agg-stage" class:off=!enabled>
                                    <div class="agg-stage-head">
                                        <span class="agg-num">{i + 1}</span>
                                        <select class="input agg-op" aria-label=ui.tr("agg.operator") on:change=move |e| {
                                            let v = event_target_value(&e);
                                            let template = STAGES.iter().find(|(o, _)| *o == v).map(|(_, t)| t.to_string()).unwrap_or_default();
                                            stages.update(|s| { s[i].op = v; s[i].body = template; });
                                        }>
                                            {STAGES.iter().map(|(o, _)| {
                                                let o = *o;
                                                view! { <option value=o selected=op == o>{o}</option> }
                                            }).collect_view()}
                                        </select>
                                        <label class="field-check small">
                                            <input type="checkbox" prop:checked=enabled on:change=move |e| { let v = event_target_checked(&e); stages.update(|s| s[i].enabled = v); } />
                                            <span>{ui.tr("agg.enabled")}</span>
                                        </label>
                                        <span class="spacer"></span>
                                        <button type="button" class="btn btn-small btn-secondary" disabled=move || running.get() on:click=move |_| run(Some(i), false)>{icon(Icon::Eye)}{ui.tr("agg.preview")}</button>
                                        <button type="button" class="icon-btn" disabled=i == 0 aria-label=ui.tr("qb.move_up") title=ui.tr("qb.move_up") on:click=move |_| stages.update(|s| s.swap(i, i - 1))>{icon(Icon::ArrowUp)}</button>
                                        <button type="button" class="icon-btn danger" disabled=count == 1 aria-label=ui.tr("agg.remove") title=ui.tr("agg.remove") on:click=move |_| stages.update(|s| { s.remove(i); })>{icon(Icon::Trash)}</button>
                                    </div>
                                    <textarea class="input mono agg-body" rows="3" spellcheck="false" aria-label=format!("{} {}", i + 1, st.op)
                                        prop:value=st.body.clone() on:change=move |e| { let v = event_target_value(&e); stages.update(|s| s[i].body = v); }></textarea>
                                </li>
                            }
                        }).collect_view()}
                    </ol>
                    <button type="button" class="btn btn-small btn-secondary" on:click=move |_| stages.update(|s| s.push(new_stage()))>{icon(Icon::Plus)}{ui.tr("agg.add_stage")}</button>
                }.into_any(),
            }}

            {move || error.get().map(|e| view! { <p class="error-detail small">{e}</p> })}
            {move || running.get().then(|| view! { <p class="muted" aria-busy="true">{ui.tr("query.running")}</p> })}
            {move || result.get().map(|r| match r {
                Err(e) => view! { <div class="error-box" role="alert"><p class="error-detail">{e}</p></div> }.into_any(),
                Ok((upto, r)) => {
                    let title = match upto {
                        Some(i) => format!("{} {}", ui.text("agg.after_stage"), i + 1),
                        None => ui.text("agg.output"),
                    };
                    let summary = format!(
                        "{} · {}{} · {} ms",
                        title,
                        r.rows.len(),
                        if r.truncated { "+" } else { "" },
                        r.elapsed_ms
                    );
                    view! {
                        <section class="agg-output">
                            <p class="muted small">{summary}{r.truncated.then(|| format!(" · {}", ui.text("agg.preview_note")))}</p>
                            <div class="json-list">
                                {r.rows.into_iter().map(|row| view! { <article class="json-record"><pre class="mono">{row.json}</pre></article> }).collect_view()}
                            </div>
                        </section>
                    }.into_any()
                }
            })}

            <Show when=move || confirm.get().is_some()>
                <div class="palette-backdrop">
                    <div class="palette confirm" role="alertdialog" aria-modal="true" aria-labelledby="agg-confirm-title">
                        <h2 id="agg-confirm-title">{ui.tr("query.confirm_title")}</h2>
                        <ul>{move || confirm.get().map(|(risks, _)| risks.into_iter().map(|r| view! { <li>{r}</li> }).collect_view())}</ul>
                        <p class="muted small">{ui.tr("query.confirm_body")}</p>
                        <div class="toolbar">
                            <button type="button" class="btn btn-danger" on:click=move |_| {
                                if let Some((_, upto)) = confirm.get_untracked() {
                                    confirm.set(None);
                                    run(upto, true);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stages_and_text_round_trip() {
        let stages = vec![
            Stage {
                op: "$match".into(),
                body: "{status: 'open'}".into(),
                enabled: true,
            },
            Stage {
                op: "$limit".into(),
                body: "5".into(),
                enabled: false,
            },
            Stage {
                op: "$group".into(),
                body: "{_id: '$city', n: {$sum: 1}}".into(),
                enabled: true,
            },
        ];
        let json = pipeline_json(&stages, None).unwrap();
        assert_eq!(
            json,
            r#"[{"$match":{"status":"open"}},{"$group":{"_id":"$city","n":{"$sum":1}}}]"#
        );
        assert_eq!(
            pipeline_json(&stages, Some(0)).unwrap(),
            r#"[{"$match":{"status":"open"}}]"#
        );
        let text = pipeline_text(&stages).unwrap();
        let back = stages_from_text(&text).unwrap();
        assert_eq!(back.len(), 2, "disabled stages are left out of the text");
        assert_eq!(pipeline_json(&back, None).unwrap(), json);
        assert!(stages_from_text("{}").is_err());
        assert!(stages_from_text("[{a: 1, b: 2}]").is_err());
    }
}
