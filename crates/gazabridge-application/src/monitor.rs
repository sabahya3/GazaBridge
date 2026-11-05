//! Live figures for the Monitor page (roadmap 6.7): per running copy and
//! stream, records and bytes per second over the last few seconds and the
//! time left; for the app itself, memory and CPU.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use gazabridge_domain::ids::RunId;
use gazabridge_domain::job::JobStatus;
use gazabridge_plugin_sdk::schema::ObjectRef;
use serde::{Deserialize, Serialize};

use crate::migration::{RunView, StreamPhase};

/// Rates are measured over this window.
const WINDOW: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessStats {
    /// Resident memory of the app.
    pub memory_bytes: u64,
    /// Share of one CPU core since the previous sample (can exceed 100).
    pub cpu_percent: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamMonitor {
    pub source: ObjectRef,
    pub target: ObjectRef,
    pub phase: StreamPhase,
    /// Records the target has acknowledged (written, skipped or rejected).
    pub processed: u64,
    pub total: Option<u64>,
    pub failed: u64,
    pub retries: u64,
    pub bytes: u64,
    pub records_per_sec: Option<f64>,
    pub bytes_per_sec: Option<f64>,
    pub eta_secs: Option<u64>,
    /// Batches waiting between reader and writer, out of the queue's room.
    pub queue_len: u32,
    pub queue_capacity: u32,
    /// Average time per batch on each side, in milliseconds.
    pub read_ms: Option<f64>,
    pub write_ms: Option<f64>,
    /// Which side holds the copy back, judged from the queue.
    pub slow_side: Option<SlowSide>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlowSide {
    /// The queue is empty: the writer waits for the source.
    Source,
    /// The queue is full: the reader waits for the target.
    Target,
    /// The copy's rate limit holds it back.
    Limit,
}

/// Judges the slow side from the queue after the writer took a batch.
pub fn slow_side(queue_len: u32, capacity: u32) -> Option<SlowSide> {
    if capacity < 2 {
        return None;
    }
    if queue_len == 0 {
        Some(SlowSide::Source)
    } else if queue_len + 1 >= capacity {
        Some(SlowSide::Target)
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunMonitor {
    pub run_id: RunId,
    pub name: String,
    pub status: JobStatus,
    pub dry_run: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub processed: u64,
    pub total: Option<u64>,
    pub records_per_sec: Option<f64>,
    pub bytes_per_sec: Option<f64>,
    pub eta_secs: Option<u64>,
    pub streams: Vec<StreamMonitor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorSnapshot {
    pub at: DateTime<Utc>,
    pub process: Option<ProcessStats>,
    pub runs: Vec<RunMonitor>,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    at: Instant,
    processed: u64,
    bytes: u64,
}

/// Recent samples per stream, keyed by run and stream index.
#[derive(Debug, Default)]
pub struct Rates {
    samples: Mutex<HashMap<(RunId, usize), VecDeque<Sample>>>,
}

/// Records and bytes per second between the oldest and newest sample.
fn rate(samples: &VecDeque<Sample>) -> Option<(f64, f64)> {
    let (first, last) = (samples.front()?, samples.back()?);
    let secs = last.at.checked_duration_since(first.at)?.as_secs_f64();
    if secs < 0.5 {
        return None;
    }
    Some((
        last.processed.saturating_sub(first.processed) as f64 / secs,
        last.bytes.saturating_sub(first.bytes) as f64 / secs,
    ))
}

/// Seconds left at `per_sec`, when the total is known and progress is made.
pub fn eta(processed: u64, total: Option<u64>, per_sec: Option<f64>) -> Option<u64> {
    let (total, per_sec) = (total?, per_sec?);
    if per_sec <= 0.0 {
        return None;
    }
    Some((total.saturating_sub(processed) as f64 / per_sec).ceil() as u64)
}

impl Rates {
    /// Adds a sample and returns the stream's rates (`None` until two
    /// samples half a second apart exist).
    fn record(
        &self,
        key: (RunId, usize),
        now: Instant,
        processed: u64,
        bytes: u64,
    ) -> Option<(f64, f64)> {
        let mut all = self.samples.lock().ok()?;
        let s = all.entry(key).or_default();
        s.push_back(Sample {
            at: now,
            processed,
            bytes,
        });
        while s.len() > 2 && s.front().is_some_and(|f| now.duration_since(f.at) > WINDOW) {
            s.pop_front();
        }
        rate(s)
    }

    /// Forgets runs that are no longer active.
    fn retain(&self, active: &[RunId]) {
        if let Ok(mut all) = self.samples.lock() {
            all.retain(|(r, _), _| active.contains(r));
        }
    }

    pub fn snapshot(&self, views: Vec<RunView>, now: Instant) -> Vec<RunMonitor> {
        self.retain(&views.iter().map(|v| v.run_id).collect::<Vec<_>>());
        views
            .into_iter()
            .map(|v| {
                let streams: Vec<StreamMonitor> = v
                    .streams
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        let processed = s.stats.processed();
                        let moving = matches!(
                            s.phase,
                            StreamPhase::Copying | StreamPhase::Indexing | StreamPhase::Preparing
                        );
                        let r = self.record((v.run_id, i), now, processed, s.stats.bytes);
                        let (rps, bps) = match (moving, r) {
                            (true, Some((a, b))) => (Some(a), Some(b)),
                            _ => (None, None),
                        };
                        let g = s.stats.gauges;
                        let ms = |us: u64| (moving && us > 0).then(|| us as f64 / 1000.0);
                        StreamMonitor {
                            queue_len: g.queue_len,
                            queue_capacity: g.queue_capacity,
                            read_ms: ms(g.read_us),
                            write_ms: ms(g.write_us),
                            slow_side: if moving && rps.is_some() {
                                if g.throttled {
                                    Some(SlowSide::Limit)
                                } else {
                                    slow_side(g.queue_len, g.queue_capacity)
                                }
                            } else {
                                None
                            },
                            source: s.source.clone(),
                            target: s.target.clone(),
                            phase: s.phase,
                            processed,
                            total: s.total,
                            failed: s.stats.failed,
                            retries: s.stats.retries,
                            bytes: s.stats.bytes,
                            records_per_sec: rps,
                            bytes_per_sec: bps,
                            eta_secs: if moving {
                                eta(processed, s.total, rps)
                            } else {
                                None
                            },
                        }
                    })
                    .collect();
                let processed = streams.iter().map(|s| s.processed).sum();
                let total = streams.iter().map(|s| s.total).sum::<Option<u64>>();
                let sum = |f: fn(&StreamMonitor) -> Option<f64>| {
                    let vals: Vec<f64> = streams.iter().filter_map(f).collect();
                    (!vals.is_empty()).then(|| vals.iter().sum())
                };
                let records_per_sec = sum(|s| s.records_per_sec);
                RunMonitor {
                    run_id: v.run_id,
                    name: v.name,
                    status: v.status,
                    dry_run: v.dry_run,
                    started_at: v.started_at,
                    processed,
                    total,
                    records_per_sec,
                    bytes_per_sec: sum(|s| s.bytes_per_sec),
                    // The slowest stream decides when the run ends.
                    eta_secs: streams.iter().filter_map(|s| s.eta_secs).max(),
                    streams,
                }
            })
            .collect()
    }
}

/// The app's own memory and CPU.
pub struct ProcessProbe {
    system: Mutex<sysinfo::System>,
    pid: Option<sysinfo::Pid>,
}

impl std::fmt::Debug for ProcessProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessProbe").finish_non_exhaustive()
    }
}

impl Default for ProcessProbe {
    fn default() -> Self {
        Self {
            system: Mutex::new(sysinfo::System::new()),
            pid: sysinfo::get_current_pid().ok(),
        }
    }
}

impl ProcessProbe {
    pub fn sample(&self) -> Option<ProcessStats> {
        let pid = self.pid?;
        let mut sys = self.system.lock().ok()?;
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            sysinfo::ProcessRefreshKind::nothing()
                .with_memory()
                .with_cpu(),
        );
        let p = sys.process(pid)?;
        Some(ProcessStats {
            memory_bytes: p.memory(),
            cpu_percent: p.cpu_usage(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rates_follow_the_window_and_eta_follows_the_rate() {
        let rates = Rates::default();
        let run = RunId::new();
        let t0 = Instant::now();
        assert_eq!(rates.record((run, 0), t0, 0, 0), None);
        assert_eq!(
            rates.record((run, 0), t0 + Duration::from_millis(200), 10, 100),
            None,
            "too close to measure"
        );
        let r = rates
            .record((run, 0), t0 + Duration::from_secs(2), 1_000, 50_000)
            .unwrap();
        assert!((r.0 - 500.0).abs() < 1e-9 && (r.1 - 25_000.0).abs() < 1e-9);
        // Old samples leave the window: the rate is the recent one.
        rates.record((run, 0), t0 + Duration::from_secs(20), 1_000, 50_000);
        let r = rates
            .record((run, 0), t0 + Duration::from_secs(22), 1_200, 50_000)
            .unwrap();
        assert!(r.0 < 500.0, "{r:?}");

        assert_eq!(eta(1_000, Some(3_000), Some(500.0)), Some(4));
        assert_eq!(eta(1_000, None, Some(500.0)), None);
        assert_eq!(eta(1_000, Some(3_000), Some(0.0)), None);
        assert_eq!(eta(5_000, Some(3_000), Some(10.0)), Some(0));

        assert_eq!(slow_side(0, 4), Some(SlowSide::Source));
        assert_eq!(slow_side(3, 4), Some(SlowSide::Target));
        assert_eq!(slow_side(1, 4), None);
        assert_eq!(slow_side(0, 1), None);

        rates.retain(&[]);
        assert!(rates.samples.lock().unwrap().is_empty());
    }

    #[test]
    fn the_process_reports_its_memory() {
        let p = ProcessProbe::default();
        let s = p.sample().unwrap();
        assert!(s.memory_bytes > 1_000_000, "{s:?}");
    }
}
