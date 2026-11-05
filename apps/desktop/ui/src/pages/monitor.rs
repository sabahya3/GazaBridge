//! Live view of running copies: records and bytes per second, time left,
//! queue fill and time per batch on each side, and the app's own memory
//! and CPU. The backend pushes a snapshot whenever a copy moves on.

use gazabridge_ipc::{MonitorDto, RunMonitorDto, StreamMonitorDto, commands, events};
use leptos::prelude::*;
use leptos::task::spawn_local;

use super::Load;
use super::backups::bytes;
use super::migrations::{num, phase_key, status_class, status_key};
use crate::app::use_ui;
use crate::ipc::{NoArgs, Subscription, call, listen};

/// `3725` → `1:02:05`, `65` → `1:05`.
fn duration(secs: u64) -> String {
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    }
}

fn rate(per_sec: Option<f64>) -> String {
    per_sec.map_or_else(|| "—".into(), |r| num(r.round() as u64))
}

fn byte_rate(per_sec: Option<f64>) -> String {
    per_sec.map_or_else(|| "—".into(), |r| format!("{}/s", bytes(r.round() as u64)))
}

/// `12.34` → `12 ms`, `0.42` → `0.4 ms`, `2500.0` → `2.5 s`.
fn millis(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.1} s", ms / 1000.0)
    } else if ms >= 10.0 {
        format!("{ms:.0} ms")
    } else {
        format!("{ms:.1} ms")
    }
}

fn percent(done: u64, total: Option<u64>) -> Option<f64> {
    total
        .filter(|t| *t > 0)
        .map(|t| (done as f64 * 100.0 / t as f64).min(100.0))
}

#[component]
pub fn MonitorPage() -> impl IntoView {
    let ui = use_ui();
    let snap = RwSignal::new(Load::<MonitorDto>::Loading);
    let subscription = StoredValue::new_local(None::<Subscription>);

    let refresh = move || {
        spawn_local(async move {
            match call::<_, MonitorDto>(commands::MONITOR_SNAPSHOT, &NoArgs {}).await {
                Ok(s) => snap.set(Load::Ready(s)),
                // Keep the last good figures on a passing hiccup.
                Err(e) if !matches!(snap.get_untracked(), Load::Ready(_)) => {
                    snap.set(Load::Failed(e.message))
                }
                Err(_) => {}
            }
        });
    };
    refresh();
    spawn_local(async move {
        let pushed = listen(events::MONITOR, move |s: MonitorDto| {
            snap.set(Load::Ready(s))
        })
        .await;
        if let Ok(sub) = pushed {
            // Dropped (and unsubscribed) at once if the page already closed.
            let _ = subscription.try_set_value(Some(sub));
        }
    });
    on_cleanup(move || {
        let _ = subscription.try_update_value(Option::take);
    });

    let stream_row = move |s: StreamMonitorDto| {
        let pct = percent(s.processed, s.total);
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
                    <span>{format!("{} / {total}", num(s.processed))}</span>
                    {s.records_per_sec.map(|r| view! { <span>{format!("{} {}", rate(Some(r)), ui.text("mon.per_sec"))}</span> })}
                    {s.bytes_per_sec.map(|b| view! { <span>{byte_rate(Some(b))}</span> })}
                    {s.eta_secs.map(|e| view! { <span>{format!("{} {}", ui.text("mon.eta"), duration(e))}</span> })}
                    {(s.failed > 0).then(|| view! { <span class="finding-warning">{format!("{} {}", num(s.failed), ui.text("mig.n_rejected"))}</span> })}
                    {(s.retries > 0).then(|| view! { <span>{format!("{} {}", s.retries, ui.text("mig.n_retries"))}</span> })}
                </div>
                {(s.queue_capacity > 0 && (s.read_ms.is_some() || s.write_ms.is_some())).then(|| view! {
                    <div class="muted small mig-numbers mon-pipe">
                        <span>{format!("{} {}/{}", ui.text("mon.queue"), s.queue_len, s.queue_capacity)}</span>
                        {s.read_ms.map(|ms| view! { <span>{format!("{} {}", ui.text("mon.read_ms"), millis(ms))}</span> })}
                        {s.write_ms.map(|ms| view! { <span>{format!("{} {}", ui.text("mon.write_ms"), millis(ms))}</span> })}
                        {s.slow_side.as_deref().map(|side| view! {
                            <span class="mon-slow">{ui.tr(match side { "source" => "mon.slow_source", "limit" => "mon.slow_limit", _ => "mon.slow_target" })}</span>
                        })}
                    </div>
                })}
            </li>
        }
    };

    let run_card = move |r: RunMonitorDto| {
        let pct = percent(r.processed, r.total);
        view! {
            <article class="card mon-run">
                <header class="mig-stream-head">
                    <h2 class="mon-run-name"><bdi>{r.name.clone()}</bdi></h2>
                    <span class=status_class(&r.status)>{ui.tr(status_key(&r.status))}</span>
                </header>
                {r.dry_run.then(|| view! { <p class="small mig-dry-note">{ui.tr("mig.dry_run_note")}</p> })}
                <dl class="mon-stats">
                    <div><dt>{ui.tr("mon.records")}</dt><dd>{format!("{} / {}", num(r.processed), r.total.map_or_else(|| "?".into(), num))}</dd></div>
                    <div><dt>{ui.tr("mon.rate")}</dt><dd>{rate(r.records_per_sec)}</dd></div>
                    <div><dt>{ui.tr("mon.throughput")}</dt><dd>{byte_rate(r.bytes_per_sec)}</dd></div>
                    <div><dt>{ui.tr("mon.eta")}</dt><dd>{r.eta_secs.map_or_else(|| "—".into(), duration)}</dd></div>
                    <div><dt>{ui.tr("mon.done")}</dt><dd>{pct.map_or_else(|| "—".into(), |p| format!("{p:.0}%"))}</dd></div>
                </dl>
                <ul class="mig-streams">{r.streams.into_iter().map(stream_row).collect_view()}</ul>
            </article>
        }
    };

    view! {
        <section class="page" aria-labelledby="mon-title">
            <h1 id="mon-title">{ui.tr("nav.monitor")}</h1>
            <p class="muted">{ui.tr("mon.intro")}</p>
            {move || match snap.get() {
                Load::Loading => view! { <p class="muted">{ui.tr("common.loading")}</p> }.into_any(),
                Load::Failed(e) => view! { <super::ErrorBox message=e retry=refresh /> }.into_any(),
                Load::Ready(s) => view! {
                    <dl class="mon-stats card" aria-label=ui.text("mon.app")>
                        <div><dt>{ui.tr("mon.memory")}</dt><dd>{s.memory_bytes.map_or_else(|| "—".into(), bytes)}</dd></div>
                        <div><dt>{ui.tr("mon.cpu")}</dt><dd>{s.cpu_percent.map_or_else(|| "—".into(), |c| format!("{c:.0}%"))}</dd></div>
                        <div><dt>{ui.tr("mon.active")}</dt><dd>{s.runs.len()}</dd></div>
                    </dl>
                    {if s.runs.is_empty() {
                        view! { <p class="empty-state">{ui.tr("mon.empty")}</p> }.into_any()
                    } else {
                        view! { <div class="mon-runs">{s.runs.into_iter().map(run_card).collect_view()}</div> }.into_any()
                    }}
                }.into_any(),
            }}
        </section>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_and_percentages_read_naturally() {
        assert_eq!(duration(65), "1:05");
        assert_eq!(duration(3725), "1:02:05");
        assert_eq!(duration(0), "0:00");
        assert_eq!(percent(5, Some(10)), Some(50.0));
        assert_eq!(percent(5, Some(0)), None);
        assert_eq!(percent(20, Some(10)), Some(100.0));
        assert_eq!(rate(None), "—");
        assert_eq!(millis(12.34), "12 ms");
        assert_eq!(millis(0.42), "0.4 ms");
        assert_eq!(millis(2500.0), "2.5 s");
        assert_eq!(rate(Some(1234.4)), "1,234");
    }
}
