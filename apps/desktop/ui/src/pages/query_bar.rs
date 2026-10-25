//! MongoDB query bar: a visual builder (AND/OR groups, operators, typed
//! values, sort keys, field selection) and a text editor (mongosh syntax),
//! kept in sync.

use gazabridge_ipc::NativeQueryDto;
use gazabridge_query::builder::{
    self, Condition, Group, Node, Op, Projection, SortField, ValueType, from_filter,
    projection_from_json, projection_to_json, sort_from_json, sort_to_json, to_filter, to_shell,
};
use gazabridge_query::shell;
use leptos::prelude::*;

use crate::app::use_ui;
use crate::icons::{Icon, icon};

/// What the bar applies: the query and Compass-style skip and limit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AppliedQuery {
    pub query: Option<NativeQueryDto>,
    pub skip: u64,
    pub limit: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Builder,
    Text,
}

fn op_key(op: Op) -> &'static str {
    match op {
        Op::Eq => "qb.op_eq",
        Op::Ne => "qb.op_ne",
        Op::Gt => "qb.op_gt",
        Op::Gte => "qb.op_gte",
        Op::Lt => "qb.op_lt",
        Op::Lte => "qb.op_lte",
        Op::In => "qb.op_in",
        Op::Nin => "qb.op_nin",
        Op::Between => "qb.op_between",
        Op::Regex => "qb.op_regex",
        Op::Exists => "qb.op_exists",
        Op::Missing => "qb.op_missing",
        Op::IsNull => "qb.op_is_null",
        Op::NotNull => "qb.op_not_null",
        Op::Type => "qb.op_type",
        Op::Size => "qb.op_size",
        Op::ElemMatch => "qb.op_elem_match",
    }
}

fn type_key(t: ValueType) -> &'static str {
    match t {
        ValueType::Auto => "qb.type_auto",
        ValueType::String => "qb.type_string",
        ValueType::Number => "qb.type_number",
        ValueType::Bool => "qb.type_bool",
        ValueType::ObjectId => "qb.type_object_id",
        ValueType::Date => "qb.type_date",
        ValueType::Null => "qb.type_null",
    }
}

/// Mutable access to the group at `path` inside `root`.
fn group_at<'a>(root: &'a mut Group, path: &[usize]) -> Option<&'a mut Group> {
    let mut g = root;
    for i in path {
        g = match g.children.get_mut(*i) {
            Some(Node::Group(inner)) => inner,
            _ => return None,
        };
    }
    Some(g)
}

fn new_condition() -> Node {
    Node::Condition(Condition::default())
}

#[derive(Clone, Copy)]
struct Ctx {
    root: RwSignal<Group>,
    fields: Signal<Vec<(String, Option<ValueType>)>>,
}

impl Ctx {
    fn update(self, path: &[usize], f: impl FnOnce(&mut Group)) {
        self.root.update(|r| {
            if let Some(g) = group_at(r, path) {
                f(g);
            }
        });
    }

    fn detected(self, field: &str) -> Option<ValueType> {
        self.fields
            .with_untracked(|fs| fs.iter().find(|(f, _)| f == field).and_then(|(_, t)| *t))
    }
}

#[component]
fn ConditionRow(ctx: Ctx, path: Vec<usize>, index: usize, cond: Condition) -> impl IntoView {
    let ui = use_ui();
    let p = StoredValue::new(path);
    let edit = move |f: Box<dyn FnOnce(&mut Condition)>| {
        ctx.update(&p.get_value(), move |g| {
            if let Some(Node::Condition(c)) = g.children.get_mut(index) {
                f(c);
            }
        });
    };
    let op = cond.op;
    let field = cond.field.clone();
    let detected = ctx.detected(&cond.field);
    let auto_label = match detected {
        Some(t) => format!("{} ({})", ui.text("qb.type_auto"), ui.text(type_key(t))),
        None => ui.text("qb.type_auto"),
    };
    view! {
        <div class="qb-cond">
            <input class="input qb-cond-field mono" type="text" list="qb-fields" placeholder=ui.tr("explorer.field") aria-label=ui.tr("explorer.field")
                prop:value=field on:change=move |e| { let v = event_target_value(&e); edit(Box::new(move |c| c.field = v)); } />
            <select class="input qb-cond-op" aria-label=ui.tr("explorer.operator")
                on:change=move |e| { if let Some(o) = Op::parse(&event_target_value(&e)) { edit(Box::new(move |c| c.op = o)); } }>
                {Op::ALL.into_iter().map(|o| view! { <option value=o.as_str() selected=o == op>{ui.tr(op_key(o))}</option> }).collect_view()}
            </select>
            {(op.arity() >= 1).then(|| {
                let placeholder = match op {
                    Op::In | Op::Nin => ui.text("qb.ph_list"),
                    Op::Type => "string, int, objectId…".to_owned(),
                    Op::ElemMatch => "{ qty: { $gt: 2 } }".to_owned(),
                    Op::Regex => "^abc".to_owned(),
                    _ => ui.text("explorer.value"),
                };
                view! {
                    <input class="input qb-cond-value mono" type="text" placeholder=placeholder aria-label=ui.tr("explorer.value")
                        prop:value=cond.value.clone() on:change=move |e| { let v = event_target_value(&e); edit(Box::new(move |c| c.value = v)); } />
                }
            })}
            {(op.arity() == 2).then(|| view! {
                <span class="muted small">{ui.tr("qb.and")}</span>
                <input class="input qb-cond-value mono" type="text" aria-label=ui.tr("explorer.value")
                    prop:value=cond.value2.clone() on:change=move |e| { let v = event_target_value(&e); edit(Box::new(move |c| c.value2 = v)); } />
            })}
            {op.typed().then(|| {
                let current = cond.value_type;
                view! {
                    <select class="input qb-cond-type" aria-label=ui.tr("qb.type")
                        on:change=move |e| { let t = ValueType::parse(&event_target_value(&e)); edit(Box::new(move |c| c.value_type = t)); }>
                        {ValueType::ALL.into_iter().map(|t| {
                            let label = if t == ValueType::Auto { auto_label.clone() } else { ui.text(type_key(t)) };
                            view! { <option value=t.as_str() selected=t == current>{label}</option> }
                        }).collect_view()}
                    </select>
                }
            })}
            {(op == Op::Regex).then(|| {
                let ci = cond.case_insensitive;
                view! {
                    <label class="field-check small">
                        <input type="checkbox" prop:checked=ci on:change=move |e| { let v = event_target_checked(&e); edit(Box::new(move |c| c.case_insensitive = v)); } />
                        <span>{ui.tr("qb.ignore_case")}</span>
                    </label>
                }
            })}
            <button type="button" class="icon-btn danger" aria-label=ui.tr("explorer.remove_condition") title=ui.tr("explorer.remove_condition")
                on:click=move |_| ctx.update(&p.get_value(), move |g| { g.children.remove(index); })>{icon(Icon::X)}</button>
        </div>
    }
}

#[component]
fn GroupEditor(ctx: Ctx, path: Vec<usize>) -> impl IntoView {
    let ui = use_ui();
    let p = StoredValue::new(path.clone());
    let is_root = path.is_empty();
    let group = move || {
        ctx.root.with(|r| {
            let mut r = r.clone();
            group_at(&mut r, &p.get_value())
                .cloned()
                .unwrap_or_default()
        })
    };
    view! {
        <div class="qb-group" class:qb-root=is_root>
            <div class="qb-group-head">
                <div class="segmented qb-andor" role="group" aria-label=ui.tr("qb.match")>
                    <button type="button" class:active=move || !group().any on:click=move |_| ctx.update(&p.get_value(), |g| g.any = false)>{ui.tr("qb.all")}</button>
                    <button type="button" class:active=move || group().any on:click=move |_| ctx.update(&p.get_value(), |g| g.any = true)>{ui.tr("qb.any")}</button>
                </div>
                <span class="muted small">{move || if group().any { ui.text("qb.any_help") } else { ui.text("qb.all_help") }}</span>
                <span class="spacer"></span>
                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| ctx.update(&p.get_value(), |g| g.children.push(new_condition()))>
                    {icon(Icon::Plus)}{ui.tr("qb.add_condition")}
                </button>
                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| ctx.update(&p.get_value(), |g| g.children.push(Node::Group(Group { any: true, children: vec![new_condition()] })))>
                    {icon(Icon::Plus)}{ui.tr("qb.add_group")}
                </button>
                {(!is_root).then(|| view! {
                    <button type="button" class="icon-btn danger" aria-label=ui.tr("qb.remove_group") title=ui.tr("qb.remove_group")
                        on:click=move |_| {
                            let path = p.get_value();
                            if let Some((last, parent)) = path.split_last() {
                                let last = *last;
                                ctx.update(parent, move |g| { g.children.remove(last); });
                            }
                        }>{icon(Icon::X)}</button>
                })}
            </div>
            {move || {
                let g = group();
                if g.children.is_empty() {
                    return view! { <p class="muted small qb-empty">{ui.tr("qb.no_conditions")}</p> }.into_any();
                }
                g.children.into_iter().enumerate().map(|(i, child)| match child {
                    Node::Condition(c) => view! { <ConditionRow ctx=ctx path=p.get_value() index=i cond=c /> }.into_any(),
                    Node::Group(_) => {
                        let mut child_path = p.get_value();
                        child_path.push(i);
                        view! { <GroupEditor ctx=ctx path=child_path /> }.into_any()
                    }
                }).collect_view().into_any()
            }}
        </div>
    }
}

#[component]
pub fn QueryBar(
    /// Known fields and their detected types (from the loaded records).
    fields: Signal<Vec<(String, Option<ValueType>)>>,
    on_apply: Callback<AppliedQuery>,
) -> impl IntoView {
    let ui = use_ui();
    let mode = RwSignal::new(Mode::Builder);
    let root = RwSignal::new(Group::default());
    let sort = RwSignal::new(Vec::<SortField>::new());
    let projection = RwSignal::new(Projection::default());
    let filter_text = RwSignal::new(String::new());
    let project_text = RwSignal::new(String::new());
    let sort_text = RwSignal::new(String::new());
    let skip = RwSignal::new(0u64);
    let limit = RwSignal::new(0u64);
    let options = RwSignal::new(false);
    let error = RwSignal::new(None::<String>);
    let ctx = Ctx { root, fields };

    // Builder → text (exact, always possible).
    let builder_to_text = move || -> Result<(), String> {
        let detected = move |f: &str| ctx.detected(f);
        let filter = to_filter(&root.get_untracked(), &detected).map_err(|e| e.to_string())?;
        filter_text.set(if filter.as_object().is_some_and(|o| o.is_empty()) {
            String::new()
        } else {
            to_shell(&filter)
        });
        let p = projection_to_json(&projection.get_untracked());
        project_text.set(if p.as_object().is_some_and(|o| o.is_empty()) {
            String::new()
        } else {
            to_shell(&p)
        });
        let s = sort_to_json(&sort.get_untracked());
        sort_text.set(if s.as_object().is_some_and(|o| o.is_empty()) {
            String::new()
        } else {
            to_shell(&s)
        });
        Ok(())
    };
    // Text → builder, when every part has a builder form.
    let text_to_builder = move || -> Result<(), String> {
        let parse = |t: String| shell::parse(&t).map_err(|e| e.to_string());
        let f = parse(filter_text.get_untracked())?;
        let g = from_filter(&f).ok_or_else(|| ui.text("qb.too_complex"))?;
        let p = projection_from_json(&parse(project_text.get_untracked())?)
            .ok_or_else(|| ui.text("qb.too_complex"))?;
        let s = sort_from_json(&parse(sort_text.get_untracked())?)
            .ok_or_else(|| ui.text("qb.too_complex"))?;
        root.set(g);
        projection.set(p);
        sort.set(s);
        Ok(())
    };
    let switch = move |to: Mode| {
        error.set(None);
        let r = match (mode.get_untracked(), to) {
            (Mode::Builder, Mode::Text) => builder_to_text(),
            (Mode::Text, Mode::Builder) => text_to_builder(),
            _ => Ok(()),
        };
        match r {
            Ok(()) => mode.set(to),
            Err(e) => error.set(Some(e)),
        }
    };
    let find = move || {
        error.set(None);
        if mode.get_untracked() == Mode::Builder
            && let Err(e) = builder_to_text()
        {
            error.set(Some(e));
            return;
        }
        let q = NativeQueryDto {
            filter: filter_text.get_untracked(),
            project: project_text.get_untracked(),
            sort: sort_text.get_untracked(),
        };
        on_apply.run(AppliedQuery {
            query: (!q.is_empty()).then_some(q),
            skip: skip.get_untracked(),
            limit: limit.get_untracked(),
        });
    };
    let reset = move || {
        root.set(Group::default());
        sort.set(Vec::new());
        projection.set(Projection::default());
        filter_text.set(String::new());
        project_text.set(String::new());
        sort_text.set(String::new());
        skip.set(0);
        limit.set(0);
        find();
    };
    let on_key = move |e: leptos::ev::KeyboardEvent| {
        if e.key() == "Enter" {
            e.prevent_default();
            find();
        }
    };
    let summary = move || {
        let n = root.with(count_conditions);
        let s = sort.with(Vec::len);
        let p = projection.with(|p| p.fields.len());
        let mut parts = Vec::new();
        if n > 0 {
            parts.push(format!("{n} {}", ui.text("qb.conditions")));
        }
        if s > 0 {
            parts.push(format!("{s} {}", ui.text("qb.sort_keys")));
        }
        if p > 0 {
            parts.push(format!("{p} {}", ui.text("qb.fields")));
        }
        parts.join(" · ")
    };

    view! {
        <div class="query-bar">
            <datalist id="qb-fields">
                {move || fields.get().into_iter().map(|(f, _)| view! { <option value=f></option> }).collect_view()}
            </datalist>
            <div class="qb-top">
                <div class="segmented" role="group" aria-label=ui.tr("qb.mode")>
                    <button type="button" class:active=move || mode.get() == Mode::Builder on:click=move |_| switch(Mode::Builder)>{icon(Icon::ListTree)}<span>{ui.tr("qb.builder")}</span></button>
                    <button type="button" class:active=move || mode.get() == Mode::Text on:click=move |_| switch(Mode::Text)>{icon(Icon::Code)}<span>{ui.tr("qb.text")}</span></button>
                </div>
                <span class="muted small">{summary}</span>
                <span class="spacer"></span>
                <button type="button" class="btn btn-small btn-secondary" aria-expanded=move || options.get().to_string()
                    on:click=move |_| options.update(|o| *o = !*o)>
                    {move || icon(if options.get() { Icon::ChevronDown } else { Icon::ChevronRight })}{ui.tr("explorer.q_options")}
                </button>
                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| reset()>{ui.tr("explorer.q_reset")}</button>
                <button type="button" class="btn btn-small" on:click=move |_| find()>{icon(Icon::Search)}{ui.tr("explorer.q_find")}</button>
            </div>

            {move || match mode.get() {
                Mode::Builder => view! { <GroupEditor ctx=ctx path=Vec::new() /> }.into_any(),
                Mode::Text => view! {
                    <label class="qb-field qb-main">
                        <span class="qb-label">{icon(Icon::Filter)}{ui.tr("explorer.q_filter")}</span>
                        <input class="qb-input mono" type="text" spellcheck="false" placeholder="{ field: \"value\" }"
                            prop:value=move || filter_text.get() on:input=move |e| filter_text.set(event_target_value(&e)) on:keydown=on_key />
                    </label>
                }.into_any(),
            }}

            <Show when=move || options.get()>
                {move || match mode.get() {
                    Mode::Builder => view! {
                        <div class="qb-section">
                            <div class="qb-section-head">
                                <strong class="small">{ui.tr("explorer.q_sort")}</strong>
                                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| sort.update(|s| s.push(SortField::default()))>{icon(Icon::Plus)}{ui.tr("qb.add_sort")}</button>
                            </div>
                            {move || sort.get().into_iter().enumerate().map(|(i, k)| {
                                let desc = k.descending;
                                view! {
                                    <div class="qb-cond">
                                        <span class="muted small qb-order">{i + 1}</span>
                                        <input class="input qb-cond-field mono" type="text" list="qb-fields" prop:value=k.field.clone() aria-label=ui.tr("explorer.field")
                                            on:change=move |e| { let v = event_target_value(&e); sort.update(|s| s[i].field = v); } />
                                        <div class="segmented" role="group">
                                            <button type="button" class:active=!desc on:click=move |_| sort.update(|s| s[i].descending = false)>{ui.tr("qb.ascending")}</button>
                                            <button type="button" class:active=desc on:click=move |_| sort.update(|s| s[i].descending = true)>{ui.tr("qb.descending")}</button>
                                        </div>
                                        <button type="button" class="icon-btn" disabled=i == 0 aria-label=ui.tr("qb.move_up") title=ui.tr("qb.move_up")
                                            on:click=move |_| sort.update(|s| s.swap(i, i - 1))>{icon(Icon::ArrowUp)}</button>
                                        <button type="button" class="icon-btn danger" aria-label=ui.tr("explorer.remove_condition")
                                            on:click=move |_| sort.update(|s| { s.remove(i); })>{icon(Icon::X)}</button>
                                    </div>
                                }
                            }).collect_view()}
                        </div>
                        <div class="qb-section">
                            <div class="qb-section-head">
                                <strong class="small">{ui.tr("explorer.q_project")}</strong>
                                <div class="segmented" role="group">
                                    <button type="button" class:active=move || !projection.get().exclude on:click=move |_| projection.update(|p| p.exclude = false)>{ui.tr("qb.show_only")}</button>
                                    <button type="button" class:active=move || projection.get().exclude on:click=move |_| projection.update(|p| p.exclude = true)>{ui.tr("qb.hide")}</button>
                                </div>
                                <span class="muted small">{ui.tr("qb.project_help")}</span>
                            </div>
                            <div class="qb-chips">
                                {move || {
                                    let chosen = projection.get().fields;
                                    let mut names: Vec<String> = fields.get().into_iter().map(|(f, _)| f).collect();
                                    for c in &chosen {
                                        if !names.contains(c) {
                                            names.push(c.clone());
                                        }
                                    }
                                    names.into_iter().map(|f| {
                                        let on = chosen.contains(&f);
                                        let name = f.clone();
                                        view! {
                                            <button type="button" class="chip" class:on=on aria-pressed=on.to_string()
                                                on:click=move |_| {
                                                    let n = name.clone();
                                                    projection.update(move |p| if let Some(i) = p.fields.iter().position(|x| *x == n) { p.fields.remove(i); } else { p.fields.push(n); });
                                                }>
                                                {on.then(|| icon(Icon::Check))}<bdi>{f}</bdi>
                                            </button>
                                        }
                                    }).collect_view()
                                }}
                                <input class="input chip-input mono" type="text" list="qb-fields" placeholder=ui.tr("qb.other_field")
                                    on:change=move |e| {
                                        let v = event_target_value(&e).trim().to_owned();
                                        if !v.is_empty() { projection.update(|p| if !p.fields.contains(&v) { p.fields.push(v.clone()); }); }
                                    } />
                            </div>
                        </div>
                    }.into_any(),
                    Mode::Text => view! {
                        <div class="qb-row qb-options">
                            <label class="qb-field">
                                <span class="qb-label">{ui.tr("explorer.q_project")}</span>
                                <input class="qb-input mono" type="text" spellcheck="false" placeholder="{ field: 1 }"
                                    prop:value=move || project_text.get() on:input=move |e| project_text.set(event_target_value(&e)) on:keydown=on_key />
                            </label>
                            <label class="qb-field">
                                <span class="qb-label">{ui.tr("explorer.q_sort")}</span>
                                <input class="qb-input mono" type="text" spellcheck="false" placeholder="{ field: -1 }"
                                    prop:value=move || sort_text.get() on:input=move |e| sort_text.set(event_target_value(&e)) on:keydown=on_key />
                            </label>
                        </div>
                    }.into_any(),
                }}
                <div class="qb-row">
                    <label class="qb-field qb-num">
                        <span class="qb-label">{ui.tr("explorer.q_skip")}</span>
                        <input class="qb-input" type="number" min="0" placeholder="0"
                            prop:value=move || { let v = skip.get(); if v == 0 { String::new() } else { v.to_string() } }
                            on:input=move |e| skip.set(event_target_value(&e).parse().unwrap_or(0)) on:keydown=on_key />
                    </label>
                    <label class="qb-field qb-num">
                        <span class="qb-label">{ui.tr("explorer.q_limit")}</span>
                        <input class="qb-input" type="number" min="0" placeholder="0"
                            prop:value=move || { let v = limit.get(); if v == 0 { String::new() } else { v.to_string() } }
                            on:input=move |e| limit.set(event_target_value(&e).parse().unwrap_or(0)) on:keydown=on_key />
                    </label>
                </div>
            </Show>

            {move || error.get().map(|e| view! { <p class="error-detail small">{e}</p> })}
        </div>
    }
}

fn count_conditions(g: &Group) -> usize {
    g.children
        .iter()
        .map(|c| match c {
            Node::Condition(c) if !c.field.trim().is_empty() => 1,
            Node::Condition(_) => 0,
            Node::Group(g) => count_conditions(g),
        })
        .sum()
}

/// Field names and their single detected type in the loaded records.
pub fn detected_fields(p: &gazabridge_ipc::BrowsePageDto) -> Vec<(String, Option<ValueType>)> {
    p.columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let mut kinds: Vec<&str> = p
                .rows
                .iter()
                .filter_map(|r| r.cells.get(i))
                .map(|c| c.kind.as_str())
                .filter(|k| !matches!(*k, "null" | "missing"))
                .collect();
            kinds.sort_unstable();
            kinds.dedup();
            let t = match kinds.as_slice() {
                [one] => builder::ValueType::from_kind(one),
                _ => None,
            };
            (c.name.clone(), t)
        })
        .collect()
}
