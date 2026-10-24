use gazabridge_ipc::{
    ConnectionDetails, ConnectionSummary, FromStringArgs, ProfileSummary, TestResultDto, commands,
};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::connection_editor::{ConnectionEditor, TestResultView};
use super::{ErrorBox, Load};
use crate::app::use_ui;
use crate::i18n::status_key;
use crate::ipc::{NoArgs, call};

#[derive(Serialize)]
struct ListArgs {
    search: Option<String>,
}

#[derive(Serialize)]
struct IdArgs {
    id: String,
}

#[derive(Serialize)]
struct DuplicateArgs {
    id: String,
    name: String,
}

#[derive(Serialize)]
struct Wrap<'a, T> {
    args: &'a T,
}

fn status_class(label: &str) -> &'static str {
    match label {
        "Production Ready" => "status status-ok",
        "Beta" => "status status-info",
        "Limited Compatibility" | "Unsupported Version" => "status status-warn",
        _ => "status status-muted",
    }
}

#[derive(Clone)]
enum Mode {
    List,
    Edit(Option<Box<ConnectionDetails>>),
    Paste,
}

#[component]
pub fn ConnectionsPage() -> impl IntoView {
    let ui = use_ui();
    let search = RwSignal::new(String::new());
    let connections = RwSignal::new(Load::<Vec<ConnectionSummary>>::Loading);
    let profiles = RwSignal::new(Load::<Vec<ProfileSummary>>::Loading);
    let mode = RwSignal::new(Mode::List);
    let message = RwSignal::new(None::<String>);
    let last_test = RwSignal::new(None::<(String, TestResultDto)>);
    let confirm_delete = RwSignal::new(None::<(String, String)>);
    let paste_name = RwSignal::new(String::new());
    let paste_text = RwSignal::new(String::new());
    let paste_remember = RwSignal::new(true);

    let load_connections = move || {
        connections.set(Load::Loading);
        let q = search.get_untracked();
        spawn_local(async move {
            let args = ListArgs {
                search: (!q.trim().is_empty()).then_some(q),
            };
            connections.set(match call(commands::LIST_CONNECTIONS, &args).await {
                Ok(v) => Load::Ready(v),
                Err(e) => Load::Failed(e.message),
            });
        });
    };
    let load_profiles = move || {
        profiles.set(Load::Loading);
        spawn_local(async move {
            profiles.set(match call(commands::LIST_PROFILES, &NoArgs {}).await {
                Ok(v) => Load::Ready(v),
                Err(e) => Load::Failed(e.message),
            });
        });
    };
    load_connections();
    load_profiles();

    let back_to_list = Callback::new(move |_| {
        mode.set(Mode::List);
        load_connections();
    });

    let edit = move |id: String| {
        spawn_local(async move {
            match call::<_, ConnectionDetails>(commands::GET_CONNECTION, &IdArgs { id }).await {
                Ok(d) => mode.set(Mode::Edit(Some(Box::new(d)))),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let test = move |id: String| {
        message.set(None);
        spawn_local(async move {
            match call::<_, TestResultDto>(commands::TEST_CONNECTION, &IdArgs { id: id.clone() })
                .await
            {
                Ok(r) => last_test.set(Some((id, r))),
                Err(e) => message.set(Some(e.message)),
            }
            load_connections();
        });
    };
    let duplicate = move |id: String, name: String| {
        let copy_name = format!("{name} ({})", ui.text("connections.copy_suffix"));
        spawn_local(async move {
            match call::<_, ConnectionDetails>(
                commands::DUPLICATE_CONNECTION,
                &DuplicateArgs {
                    id,
                    name: copy_name,
                },
            )
            .await
            {
                Ok(_) => load_connections(),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let do_delete = move || {
        let Some((id, _)) = confirm_delete.get_untracked() else {
            return;
        };
        confirm_delete.set(None);
        spawn_local(async move {
            match call::<_, ()>(commands::DELETE_CONNECTION, &IdArgs { id }).await {
                Ok(()) => load_connections(),
                Err(e) => message.set(Some(e.message)),
            }
        });
    };
    let create_from_string = move |_| {
        let args = FromStringArgs {
            name: paste_name.get_untracked(),
            text: paste_text.get_untracked(),
            remember: paste_remember.get_untracked(),
        };
        spawn_local(async move {
            match call::<_, ConnectionDetails>(
                commands::CREATE_CONNECTION_FROM_STRING,
                &Wrap { args: &args },
            )
            .await
            {
                Ok(d) => {
                    paste_text.set(String::new());
                    paste_name.set(String::new());
                    mode.set(Mode::Edit(Some(Box::new(d))));
                }
                Err(e) => message.set(Some(e.message)),
            }
        });
    };

    let profile_list = move || match profiles.get() {
        Load::Ready(p) => p,
        _ => Vec::new(),
    };

    view! {
        <section class="page" aria-labelledby="conn-title">
            <h1 id="conn-title">{ui.tr("connections.title")}</h1>
            <div role="status" aria-live="polite">
                {move || message.get().map(|m| view! { <p class="error-detail">{m}</p> })}
            </div>

            {move || match mode.get() {
                Mode::Edit(initial) => view! {
                    <ConnectionEditor initial=initial.map(|d| *d) profiles=profile_list() on_done=back_to_list />
                }.into_any(),
                Mode::Paste => view! {
                    <section class="card editor">
                        <h2>{ui.tr("connections.paste_title")}</h2>
                        <p class="muted small">{ui.tr("connections.paste_help")}</p>
                        <label class="field">
                            <span>{ui.tr("editor.name")}</span>
                            <input class="input" type="text" prop:value=move || paste_name.get() on:input=move |e| paste_name.set(event_target_value(&e)) />
                        </label>
                        <label class="field">
                            <span>{ui.tr("connections.connection_string")}</span>
                            <input class="input mono" type="password" autocomplete="off" prop:value=move || paste_text.get() on:input=move |e| paste_text.set(event_target_value(&e)) />
                        </label>
                        <label class="field-check">
                            <input type="checkbox" prop:checked=move || paste_remember.get() on:change=move |e| paste_remember.set(event_target_checked(&e)) />
                            <span>{ui.tr("editor.pw_remember")}</span>
                        </label>
                        <div class="toolbar">
                            <button type="button" class="btn" on:click=create_from_string>{ui.tr("connections.create")}</button>
                            <button type="button" class="btn btn-secondary" on:click=move |_| mode.set(Mode::List)>{ui.tr("common.cancel")}</button>
                        </div>
                    </section>
                }.into_any(),
                Mode::List => view! {
                    <div class="toolbar">
                        <input
                            type="search"
                            class="input"
                            placeholder=ui.tr("connections.search")
                            aria-label=ui.tr("connections.search")
                            prop:value=move || search.get()
                            on:input=move |e| {
                                search.set(event_target_value(&e));
                                load_connections();
                            }
                        />
                        <button type="button" class="btn" on:click=move |_| mode.set(Mode::Edit(None))>{ui.tr("connections.new")}</button>
                        <button type="button" class="btn btn-secondary" on:click=move |_| mode.set(Mode::Paste)>{ui.tr("connections.paste")}</button>
                    </div>
                    {move || match connections.get() {
                        Load::Loading => view! { <p>{ui.tr("common.loading")}</p> }.into_any(),
                        Load::Failed(e) => view! { <ErrorBox message=e retry=load_connections /> }.into_any(),
                        Load::Ready(list) if list.is_empty() => view! {
                            <div class="empty-state">
                                <h2>{ui.tr("connections.empty_title")}</h2>
                                <p>{ui.tr("connections.empty_body")}</p>
                            </div>
                        }.into_any(),
                        Load::Ready(list) => view! {
                            <ul class="conn-list">
                                {list.into_iter().map(|c| {
                                    let (id_t, id_e, id_d, id_x) = (c.id.clone(), c.id.clone(), c.id.clone(), c.id.clone());
                                    let (name_d, name_x) = (c.name.clone(), c.name.clone());
                                    let id_r = c.id.clone();
                                    view! {
                                        <li class="card conn">
                                            <span class="conn-swatch" style:background-color=c.color.clone().unwrap_or_default()></span>
                                            <div class="conn-main">
                                                <strong>{c.name.clone()}</strong>
                                                <span class="mono muted">{c.target.clone()}</span>
                                                <span class="muted">{c.profile_name.clone()}</span>
                                            </div>
                                            <div class="conn-flags">
                                                {c.read_only.then(|| view! { <span class="badge">{ui.tr("connections.read_only")}</span> })}
                                                {c.favorite.then(|| view! { <span class="badge">{ui.tr("connections.favorite")}</span> })}
                                                {c.tags.iter().map(|t| view! { <span class="tag">{t.clone()}</span> }).collect_view()}
                                                <span class="muted">
                                                    {match (c.state.clone(), c.latency_ms) {
                                                        (Some(s), Some(ms)) => format!("{s} · {ms} ms"),
                                                        (Some(s), None) => s,
                                                        _ => ui.text("connections.never_checked"),
                                                    }}
                                                </span>
                                            </div>
                                            <div class="conn-actions">
                                                <button type="button" class="btn btn-small" on:click=move |_| test(id_t.clone())>{ui.tr("editor.test")}</button>
                                                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| edit(id_e.clone())>{ui.tr("connections.edit")}</button>
                                                <button type="button" class="btn btn-small btn-secondary" on:click=move |_| duplicate(id_d.clone(), name_d.clone())>{ui.tr("connections.duplicate")}</button>
                                                <button type="button" class="btn btn-small btn-danger" on:click=move |_| confirm_delete.set(Some((id_x.clone(), name_x.clone())))>{ui.tr("connections.delete")}</button>
                                            </div>
                                            {move || last_test.get().filter(|(tid, _)| *tid == id_r).map(|(_, r)| view! { <TestResultView result=r /> })}
                                        </li>
                                    }
                                }).collect_view()}
                            </ul>
                        }.into_any(),
                    }}
                }.into_any(),
            }}

            <Show when=move || confirm_delete.get().is_some()>
                <div class="palette-backdrop">
                    <div class="palette confirm" role="alertdialog" aria-modal="true" aria-labelledby="confirm-title">
                        <h2 id="confirm-title">{ui.tr("connections.delete_title")}</h2>
                        <p>
                            {move || confirm_delete.get().map(|(_, name)| name).unwrap_or_default()}
                        </p>
                        <p class="muted small">{ui.tr("connections.delete_body")}</p>
                        <div class="toolbar">
                            <button type="button" class="btn btn-danger" on:click=move |_| do_delete()>{ui.tr("connections.delete")}</button>
                            <button type="button" class="btn btn-secondary" on:click=move |_| confirm_delete.set(None)>{ui.tr("common.cancel")}</button>
                        </div>
                    </div>
                </div>
            </Show>

            <h2>{ui.tr("connections.catalogue")}</h2>
            <p class="muted">{ui.tr("connections.catalogue_intro")}</p>
            {move || match profiles.get() {
                Load::Loading => view! { <p>{ui.tr("common.loading")}</p> }.into_any(),
                Load::Failed(e) => view! { <ErrorBox message=e retry=load_profiles /> }.into_any(),
                Load::Ready(list) => {
                    let mut families: Vec<(String, Vec<ProfileSummary>)> = Vec::new();
                    for p in list {
                        match families.iter_mut().find(|(f, _)| *f == p.family_name) {
                            Some((_, v)) => v.push(p),
                            None => families.push((p.family_name.clone(), vec![p])),
                        }
                    }
                    view! {
                        <div class="catalogue">
                            {families.into_iter().map(|(family, items)| view! {
                                <section class="catalogue-family">
                                    <h3>{family}</h3>
                                    {items.into_iter().map(|p| view! { <ProfileCard profile=p /> }).collect_view()}
                                </section>
                            }).collect_view()}
                        </div>
                    }.into_any()
                }
            }}
        </section>
    }
}

#[component]
fn ProfileCard(profile: ProfileSummary) -> impl IntoView {
    let ui = use_ui();
    let p = profile;
    let status_label = ui.text(status_key(&p.status));
    let tested = if p.tested_versions.is_empty() {
        ui.text("connections.none_recorded")
    } else {
        p.tested_versions.join(", ")
    };
    view! {
        <details class="card profile">
            <summary>
                <span class="profile-name">{p.display_name.clone()}</span>
                <span class=status_class(&p.status)>{status_label}</span>
                {(!p.connector_available).then(|| view! {
                    <span class="badge badge-muted">{ui.tr("connections.connector_missing")}</span>
                })}
            </summary>
            <dl class="facts">
                <dt>{ui.tr("connections.tested_versions")}</dt><dd>{tested}</dd>
                <dt>{ui.tr("connections.auth")}</dt><dd>{p.auth_methods.join(", ")}</dd>
                <dt>{ui.tr("connections.cdc")}</dt>
                <dd>{if p.cdc_available { ui.tr("common.yes")() } else { ui.tr("common.no")() }}</dd>
                <dt>{ui.tr("connections.docs")}</dt><dd class="mono">{p.documentation_url.clone()}</dd>
            </dl>
            {(!p.known_limitations.is_empty()).then(|| view! {
                <h4>{ui.tr("connections.limitations")}</h4>
                <ul>{p.known_limitations.iter().map(|l| view! { <li>{l.clone()}</li> }).collect_view()}</ul>
            })}
            {(!p.unsupported_operations.is_empty()).then(|| view! {
                <h4>{ui.tr("connections.unsupported")}</h4>
                <ul>{p.unsupported_operations.iter().map(|l| view! { <li>{l.clone()}</li> }).collect_view()}</ul>
            })}
        </details>
    }
}
