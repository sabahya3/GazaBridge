use gazabridge_ipc::{ConnectionSummary, ProfileSummary, commands};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Serialize;

use super::{ErrorBox, Load};
use crate::app::use_ui;
use crate::i18n::status_key;
use crate::ipc::{NoArgs, call};

#[derive(Serialize)]
struct ListArgs {
    search: Option<String>,
}

fn status_class(label: &str) -> &'static str {
    match label {
        "Production Ready" => "status status-ok",
        "Beta" => "status status-info",
        "Limited Compatibility" | "Unsupported Version" => "status status-warn",
        _ => "status status-muted",
    }
}

#[component]
pub fn ConnectionsPage() -> impl IntoView {
    let ui = use_ui();
    let search = RwSignal::new(String::new());
    let connections = RwSignal::new(Load::<Vec<ConnectionSummary>>::Loading);
    let profiles = RwSignal::new(Load::<Vec<ProfileSummary>>::Loading);

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

    view! {
        <section class="page" aria-labelledby="conn-title">
            <h1 id="conn-title">{ui.tr("connections.title")}</h1>
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
                        {list.into_iter().map(|c| view! {
                            <li class="card conn">
                                <span class="conn-swatch" style:background-color=c.color.clone().unwrap_or_default()></span>
                                <div class="conn-main">
                                    <strong>{c.name}</strong>
                                    <span class="mono muted">{c.target}</span>
                                    <span class="muted">{c.profile_name}</span>
                                </div>
                                <div class="conn-flags">
                                    {c.read_only.then(|| view! { <span class="badge">{ui.tr("connections.read_only")}</span> })}
                                    {c.favorite.then(|| view! { <span class="badge">{ui.tr("connections.favorite")}</span> })}
                                    {c.tags.into_iter().map(|t| view! { <span class="tag">{t}</span> }).collect_view()}
                                    <span class="muted">
                                        {match (c.state, c.latency_ms) {
                                            (Some(s), Some(ms)) => format!("{s} · {ms} ms"),
                                            (Some(s), None) => s,
                                            _ => ui.text("connections.never_checked"),
                                        }}
                                    </span>
                                </div>
                            </li>
                        }).collect_view()}
                    </ul>
                }.into_any(),
            }}

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
