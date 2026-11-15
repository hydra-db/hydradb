//! Bounded, process-wide diagnostics. Samples existing gauges and counts
//! finished work over a window without walking graphs or issuing store reads.
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use hydradb::{cgroup_directory, DurationHistogramSnapshot, MemoryDiagnosticSnapshot, RemoteStorageProfile};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{valid_control_bearer, AdminState};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Options {
    seconds: Option<u64>,
}

impl Options {
    pub(super) fn duration(&self) -> Result<Duration, (StatusCode, &'static str)> {
        let seconds = self.seconds.unwrap_or(10);
        if !(1..=30).contains(&seconds) {
            return Err((StatusCode::BAD_REQUEST, "seconds must be between 1 and 30"));
        }
        Ok(Duration::from_secs(seconds))
    }
}

struct Snapshot {
    gauges: Value,
    counters: BTreeMap<String, u64>,
    stages: BTreeMap<&'static str, MemoryDiagnosticSnapshot>,
    client: BTreeMap<&'static str, DurationHistogramSnapshot>,
}

impl Snapshot {
    fn read(state: &AdminState) -> Self {
        let rt = tokio::runtime::Handle::current().metrics();
        let cache = state.routed_node.slatedb_cache_metrics();
        let observe = &cache.observe;
        let mut counters = os_counters(Path::new("/proc/self/io"), cgroup_directory().as_deref());
        for i in 0..rt.num_workers() {
            counters.insert(
                format!("tokio.worker.{i}.busy_us"),
                rt.worker_total_busy_duration(i).as_micros() as u64,
            );
            counters.insert(format!("tokio.worker.{i}.parks"), rt.worker_park_count(i));
        }
        Self {
            gauges: json!({
                "memory": state.memory_sampler.resource_snapshot(),
                "tokio": {"workers": rt.num_workers(), "alive_tasks": rt.num_alive_tasks(), "global_queue_depth": rt.global_queue_depth()},
                "slatedb_cache": {"capacity_bytes": cache.capacity_bytes, "resident_bytes": cache.resident_bytes, "entries": cache.entries,
                    "observation": (observe.generation != 0).then(|| json!({"generation":observe.generation,
                        "hits":observe.hits.iter().sum::<u64>(), "reloads":observe.reload_executions,
                        "reload_bytes":observe.reload_success_bytes, "coalesced_waiters":observe.coalesced_waiters.iter().sum::<u64>()}))},
            }),
            counters,
            stages: hydradb::memory_diagnostic_snapshot().into_iter().map(|s| (s.stage, s)).collect(),
            client: state
                .query
                .metrics()
                .histogram_fields()
                .map(|(name, h)| (name, h.clone()))
                .collect(),
        }
    }
}

fn os_counters(io_path: &Path, cgroup: Option<&Path>) -> BTreeMap<String, u64> {
    let mut counters = BTreeMap::new();
    if let Ok(io) = std::fs::read_to_string(io_path) {
        for line in io.lines() {
            if let Some((name, value)) = line.split_once(": ") {
                if matches!(name, "read_bytes" | "write_bytes" | "cancelled_write_bytes") {
                    if let Ok(bytes) = value.parse() {
                        counters.insert(format!("process.disk.{name}"), bytes);
                    }
                }
            }
        }
    }
    if let Some(dir) = cgroup {
        for resource in ["cpu", "io", "memory"] {
            if let Ok(text) = std::fs::read_to_string(dir.join(format!("{resource}.pressure"))) {
                for line in text.lines() {
                    let mut fields = line.split_whitespace();
                    if let Some(kind @ ("some" | "full")) = fields.next() {
                        if let Some(us) = fields.find_map(|f| f.strip_prefix("total=")?.parse::<u64>().ok()) {
                            counters.insert(format!("cgroup.{resource}.pressure_{kind}_us"), us);
                        }
                    }
                }
            }
        }
    }
    counters
}

fn latency(hist: &DurationHistogramSnapshot) -> Value {
    json!({"count": hist.count(), "sum_us": hist.sum_us, "bucket_counts": hist.bucket_counts})
}

fn interval_latency(end: &DurationHistogramSnapshot, start: &DurationHistogramSnapshot) -> Value {
    latency(&DurationHistogramSnapshot {
        sum_us: end.sum_us.saturating_sub(start.sum_us),
        bucket_counts: std::array::from_fn(|i| end.bucket_counts[i].saturating_sub(start.bucket_counts[i])),
    })
}

pub(super) async fn handle(State(state): State<AdminState>, headers: HeaderMap, Query(options): Query<Options>) -> Response {
    if !valid_control_bearer(&headers, &state.control_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let duration = match options.duration() {
        Ok(d) => d,
        Err(e) => return e.into_response(),
    };
    let Some(profile) = RemoteStorageProfile::try_start() else {
        return (StatusCode::CONFLICT, "a resource profile is already in progress").into_response();
    };
    let since = Instant::now();
    let initial_state = state.clone();
    let Ok(start) = tokio::task::spawn_blocking(move || Snapshot::read(&initial_state)).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    tokio::time::sleep(duration).await;
    let Ok(end) = tokio::task::spawn_blocking(move || Snapshot::read(&state)).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    let (remote, histograms) = profile.finish();
    let elapsed_seconds = since.elapsed().as_secs_f64();
    let timings: BTreeMap<_, _> = histograms.into_iter().map(|(name, h)| (name, latency(&h))).collect();
    let counters: BTreeMap<_, _> = end
        .counters
        .iter()
        .filter_map(|(key, end)| Some((key, end.checked_sub(*start.counters.get(key)?)?)))
        .collect();
    let stages: BTreeMap<_, _> = end
        .stages
        .iter()
        .filter_map(|(name, s)| {
            let before = start.stages.get(name)?;
            Some((
                *name,
                json!({
                    "entered": s.entered.saturating_sub(before.entered), "exited": s.exited.saturating_sub(before.exited),
                    "active_start": before.items, "active_end": s.items,
                    "estimated_bytes_start": before.estimated_bytes, "estimated_bytes_end": s.estimated_bytes,
                    "duration":interval_latency(&s.duration, &before.duration),
                }),
            ))
        })
        .collect();
    let client: BTreeMap<_, _> = end
        .client
        .iter()
        .filter_map(|(name, h)| Some((name, interval_latency(h, start.client.get(name)?))))
        .collect();
    super::json_response(
        StatusCode::OK,
        json!({
            "elapsed_seconds": elapsed_seconds,
            "start": start.gauges,
            "end": end.gauges,
            "counter_deltas": counters,
            "remote_store": {
                "gets": remote.gets,
                "ranged_gets": remote.ranged_gets,
                "get_bytes": remote.get_bytes,
                "puts": remote.puts,
                "put_bytes": remote.put_bytes,
                "timings": timings,
            },
            "stages": stages,
            "client_latency": client,
            "latency_bucket_upper_bounds_us": hydradb::DURATION_BUCKET_BOUNDS_US,
        }),
    )
}
