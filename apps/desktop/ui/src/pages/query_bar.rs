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

/// Which text box a problem belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Filter,
    Project,
    Sort,
    Builder,
}

/// A problem shown under the bar, with an optional corrected text.
#[derive(Debug, Clone, PartialEq)]
struct Problem {
    part: Part,
    message: String,
    fix: Option<String>,
}

/// Searchable picker: chosen fields as chips, the rest in a short list.
#[component]
fn FieldPicker(
    fields: Signal<Vec<(String, Option<ValueType>)>>,
    chosen: RwSignal<Vec<String>>,
) -> impl IntoView {
    let ui = use_ui();
    let search = RwSignal::new(String::new());
    let toggle = move |name: String| {
        chosen.update(move |c| {
            if let Some(i) = c.iter().position(|x| *x == name) {
                c.remove(i);
            } else {
                c.push(name);
            }
        })
    };
    view! {
        <div class="picker">
            <div class="picker-chosen">
                {move || chosen.get().into_iter().map(|f| {
                    let name = f.clone();
                    view! {
                        <span class="chip on">
                            <bdi>{f}</bdi>
                            <button type="button" class="chip-x" aria-label=ui.tr("qb.remove_field") on:click=move |_| toggle(name.clone())>{icon(Icon::X)}</button>
                        </span>
                    }
                }).collect_view()}
                <details class="picker-pop">
                    <summary class="btn btn-small btn-secondary">{icon(Icon::Plus)}{ui.tr("qb.add_field")}</summary>
                    <div class="picker-panel">
                        <label class="tree-search picker-search">
                            {icon(Icon::Search)}
                            <input type="search" placeholder=ui.tr("qb.find_field") aria-label=ui.tr("qb.find_field")
                                prop:value=move || search.get() on:input=move |e| search.set(event_target_value(&e))
                                on:keydown=move |e: leptos::ev::KeyboardEvent| {
                                    // Enter adds a field that is not in the list.
                                    if e.key() == "Enter" {
                                        e.prevent_default();
                                        let v = search.get_untracked().trim().to_owned();
                                        if !v.is_empty() && !chosen.get_untracked().contains(&v) {
                                            toggle(v);
                                            search.set(String::new());
                                        }
                                    }
                                } />
                        </label>
                        <ul class="picker-list" role="listbox" aria-multiselectable="true">
                            {move || {
                                let q = search.get().to_lowercase();
                                let picked = chosen.get();
                                let items: Vec<String> = fields.get().into_iter().map(|(f, _)| f).filter(|f| q.is_empty() || f.to_lowercase().contains(&q)).collect();
                                if items.is_empty() {
                                    return view! { <li class="muted small picker-none">{ui.tr("qb.no_field_match")}</li> }.into_any();
                                }
                                items.into_iter().map(|f| {
                                    let on = picked.contains(&f);
                                    let name = f.clone();
                                    view! {
                                        <li>
                                            <label class="picker-item" role="option" aria-selected=on.to_string()>
                                                <input type="checkbox" prop:checked=on on:change=move |_| toggle(name.clone()) />
                                                <bdi>{f}</bdi>
                                            </label>
                                        </li>
                                    }
                                }).collect_view().into_any()
                            }}
                        </ul>
                    </div>
                </details>
                <Show when=move || !chosen.with(Vec::is_empty)>
                    <button type="button" class="btn btn-small btn-secondary" on:click=move |_| chosen.set(Vec::new())>{ui.tr("qb.clear_fields")}</button>
                </Show>
            </div>
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
    let project_fields = RwSignal::new(Vec::<String>::new());
    let project_hide = RwSignal::new(false);
    let filter_text = RwSignal::new(String::new());
    let project_text = RwSignal::new(String::new());
    let sort_text = RwSignal::new(String::new());
    let skip = RwSignal::new(0u64);
    let limit = RwSignal::new(0u64);
    let options = RwSignal::new(false);
    let problem = RwSignal::new(None::<Problem>);
    let ctx = Ctx { root, fields };
    let projection = move || Projection {
        exclude: project_hide.get_untracked(),
        fields: project_fields.get_untracked(),
    };

    // Builder → text (exact, always possible when values are valid).
    let builder_to_text = move || -> Result<(), Problem> {
        let detected = move |f: &str| ctx.detected(f);
        let filter = to_filter(&root.get_untracked(), &detected).map_err(|e| Problem {
            part: Part::Builder,
            message: e.to_string(),
            fix: None,
        })?;
        let empty = |j: &serde_json::Value| j.as_object().is_some_and(|o| o.is_empty());
        filter_text.set(if empty(&filter) {
            String::new()
        } else {
            to_shell(&filter)
        });
        let p = projection_to_json(&projection());
        project_text.set(if empty(&p) {
            String::new()
        } else {
            to_shell(&p)
        });
        let srt = sort_to_json(&sort.get_untracked());
        sort_text.set(if empty(&srt) {
            String::new()
        } else {
            to_shell(&srt)
        });
        Ok(())
    };
    // Checks the three text boxes without sending anything.
    let check_text =
        move || -> Result<(serde_json::Value, serde_json::Value, serde_json::Value), Problem> {
            let parse = |part: Part, text: String| {
                shell::parse(&text).map_err(|e| Problem {
                    part,
                    message: e.message.clone(),
                    fix: e.suggestion.as_ref().map(to_shell),
                })
            };
            let f = parse(Part::Filter, filter_text.get_untracked())?;
            if !f.is_object() {
                return Err(Problem {
                    part: Part::Filter,
                    message: ui.text("qb.err_filter_doc"),
                    fix: None,
                });
            }
            let p = parse(Part::Project, project_text.get_untracked())?;
            if projection_from_json(&p).is_none() {
                return Err(Problem {
                    part: Part::Project,
                    message: ui.text("qb.err_project"),
                    fix: None,
                });
            }
            let srt = parse(Part::Sort, sort_text.get_untracked())?;
            if sort_from_json(&srt).is_none() {
                return Err(Problem {
                    part: Part::Sort,
                    message: ui.text("qb.err_sort"),
                    fix: None,
                });
            }
            Ok((f, p, srt))
        };
    // Text → builder, when every part has a builder form.
    let text_to_builder = move || -> Result<(), Problem> {
        let (f, p, srt) = check_text()?;
        let too_complex = || Problem {
            part: Part::Filter,
            message: ui.text("qb.too_complex"),
            fix: None,
        };
        let g = from_filter(&f).ok_or_else(too_complex)?;
        let pr = projection_from_json(&p).ok_or_else(too_complex)?;
        let ss = sort_from_json(&srt).ok_or_else(too_complex)?;
        root.set(g);
        project_hide.set(pr.exclude);
        project_fields.set(pr.fields);
        sort.set(ss);
        Ok(())
    };
    let switch = move |to: Mode| {
        problem.set(None);
        let r = match (mode.get_untracked(), to) {
            (Mode::Builder, Mode::Text) => builder_to_text(),
            (Mode::Text, Mode::Builder) => text_to_builder(),
            _ => Ok(()),
        };
        match r {
            Ok(()) => mode.set(to),
            Err(e) => problem.set(Some(e)),
        }
    };
    let find = move || {
        problem.set(None);
        let r = if mode.get_untracked() == Mode::Builder {
            builder_to_text()
        } else {
            Ok(())
        };
        if let Err(e) = r.and_then(|()| check_text().map(|_| ())) {
            problem.set(Some(e));
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
        project_fields.set(Vec::new());
        project_hide.set(false);
        filter_text.set(String::new());
        project_text.set(String::new());
        sort_text.set(String::new());
        skip.set(0);
        limit.set(0);
        find();
    };
    let use_fix = move |pr: Problem| {
        if let Some(fix) = pr.fix {
            match pr.part {
                Part::Filter => filter_text.set(fix),
                Part::Project => project_text.set(fix),
                Part::Sort => sort_text.set(fix),
                Part::Builder => {}
            }
            find();
        }
    };
    let on_key = move |e: leptos::ev::KeyboardEvent| {
        if e.key() == "Enter" {
            e.prevent_default();
            find();
        }
    };
    let plural = move |n: usize, one: &'static str, many: &'static str| {
        format!("{n} {}", ui.text(if n == 1 { one } else { many }))
    };
    let summary = move || {
        let mut parts = Vec::new();
        let n = root.with(count_conditions);
        if n > 0 {
            parts.push(plural(n, "qb.condition", "qb.conditions"));
        }
        let s = sort.with(|s| s.iter().filter(|k| !k.field.trim().is_empty()).count());
        if s > 0 {
            parts.push(plural(s, "qb.sort_key", "qb.sort_keys"));
        }
        let p = project_fields.with(Vec::len);
        if p > 0 {
            parts.push(plural(p, "qb.field_one", "qb.fields"));
        }
        if parts.is_empty() {
            ui.text("qb.everything")
        } else {
            parts.join(" · ")
        }
    };
    let problem_view = move |part: Part| {
        move || {
            problem.get().filter(|p| p.part == part).map(|p| {
            let fix_text = p.fix.clone();
            view! {
                <div class="qb-problem" role="alert">
                    <span>{capitalize(&p.message)}</span>
                    {fix_text.map(|t| {
                        let pr = p.clone();
                        view! {
                            <span class="qb-fix">
                                <span class="muted">{ui.tr("qb.did_you_mean")}</span>
                                <code class="mono"><bdi>{t}</bdi></code>
                                <button type="button" class="btn btn-small" on:click=move |_| use_fix(pr.clone())>{ui.tr("qb.use_it")}</button>
                            </span>
                        }
                    })}
                </div>
            }
        })
        }
    };
    let text_box = move |part: Part,
                         label: &'static str,
                         placeholder: &'static str,
                         value: RwSignal<String>| {
        view! {
            <div class="qb-text-part">
                <label class="qb-field" class:invalid=move || problem.with(|p| p.as_ref().is_some_and(|p| p.part == part))>
                    <span class="qb-label">{ui.tr(label)}</span>
                    <input class="qb-input mono" type="text" spellcheck="false" placeholder=placeholder
                        prop:value=move || value.get() on:input=move |e| { value.set(event_target_value(&e)); problem.set(None); } on:keydown=on_key />
                </label>
                {problem_view(part)}
            </div>
        }
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
                <span class="qb-summary">{summary}</span>
                <span class="spacer"></span>
                <button type="button" class="btn btn-small btn-secondary" aria-expanded=move || options.get().to_string()
                    on:click=move |_| options.update(|o| *o = !*o)>
                    {move || icon(if options.get() { Icon::ChevronDown } else { Icon::ChevronRight })}{ui.tr("qb.more_options")}
                </button>
                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| reset()>{ui.tr("explorer.q_reset")}</button>
                <button type="button" class="btn btn-small" on:click=move |_| find()>{icon(Icon::Search)}{ui.tr("explorer.q_find")}</button>
            </div>

            {move || match mode.get() {
                Mode::Builder => view! {
                    <GroupEditor ctx=ctx path=Vec::new() />
                    {problem_view(Part::Builder)}
                }.into_any(),
                Mode::Text => text_box(Part::Filter, "explorer.q_filter", "{ field: \"value\" }", filter_text).into_any(),
            }}

            <Show when=move || options.get()>
                <div class="qb-options-grid">
                    <section class="qb-section">
                        <header class="qb-section-head">
                            <strong>{ui.tr("explorer.q_sort")}</strong>
                            <span class="muted small">{ui.tr("qb.sort_help")}</span>
                        </header>
                        {move || match mode.get() {
                            Mode::Builder => view! {
                                {move || sort.get().into_iter().enumerate().map(|(i, k)| {
                                    let desc = k.descending;
                                    view! {
                                        <div class="qb-sort-row">
                                            <span class="qb-order">{i + 1}</span>
                                            <input class="input mono qb-sort-field" type="text" list="qb-fields" prop:value=k.field.clone() aria-label=ui.tr("explorer.field")
                                                placeholder=ui.tr("explorer.field")
                                                on:change=move |e| { let v = event_target_value(&e); sort.update(|s| s[i].field = v); } />
                                            <div class="segmented" role="group">
                                                <button type="button" class:active=!desc title=ui.tr("qb.ascending") on:click=move |_| sort.update(|s| s[i].descending = false)>"A → Z"</button>
                                                <button type="button" class:active=desc title=ui.tr("qb.descending") on:click=move |_| sort.update(|s| s[i].descending = true)>"Z → A"</button>
                                            </div>
                                            <button type="button" class="icon-btn" disabled=i == 0 aria-label=ui.tr("qb.move_up") title=ui.tr("qb.move_up")
                                                on:click=move |_| sort.update(|s| s.swap(i, i - 1))>{icon(Icon::ArrowUp)}</button>
                                            <button type="button" class="icon-btn danger" aria-label=ui.tr("qb.remove_sort") title=ui.tr("qb.remove_sort")
                                                on:click=move |_| sort.update(|s| { s.remove(i); })>{icon(Icon::X)}</button>
                                        </div>
                                    }
                                }).collect_view()}
                                <button type="button" class="btn btn-small btn-secondary qb-add" on:click=move |_| sort.update(|s| s.push(SortField::default()))>{icon(Icon::Plus)}{ui.tr("qb.add_sort")}</button>
                            }.into_any(),
                            Mode::Text => text_box(Part::Sort, "explorer.q_sort", "{ field: -1 }", sort_text).into_any(),
                        }}
                    </section>
                    <section class="qb-section">
                        <header class="qb-section-head">
                            <strong>{ui.tr("qb.fields_title")}</strong>
                            {move || (mode.get() == Mode::Builder).then(|| view! {
                                <div class="segmented" role="group">
                                    <button type="button" class:active=move || !project_hide.get() on:click=move |_| project_hide.set(false)>{ui.tr("qb.show_only")}</button>
                                    <button type="button" class:active=move || project_hide.get() on:click=move |_| project_hide.set(true)>{ui.tr("qb.hide")}</button>
                                </div>
                            })}
                        </header>
                        {move || match mode.get() {
                            Mode::Builder => view! {
                                <FieldPicker fields=fields chosen=project_fields />
                                <p class="muted small">{move || if project_fields.with(Vec::is_empty) { ui.text("qb.all_fields") } else if project_hide.get() { ui.text("qb.hiding") } else { ui.text("qb.showing_only") }}</p>
                            }.into_any(),
                            Mode::Text => text_box(Part::Project, "explorer.q_project", "{ field: 1 }", project_text).into_any(),
                        }}
                    </section>
                    <section class="qb-section qb-paging">
                        <header class="qb-section-head"><strong>{ui.tr("qb.paging")}</strong></header>
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
                        <p class="muted small">{ui.tr("qb.paging_help")}</p>
                    </section>
                </div>
            </Show>
        </div>
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    c.next()
        .map(|f| f.to_uppercase().chain(c).collect())
        .unwrap_or_default()
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
