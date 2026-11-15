//! One-second samples retain high-water marks since admin sampler startup.
//! Peaks do not reset on scrape (multiple scrapers must observe the same state).
//! Scheduling delays and subsecond spikes are possible; this is not a profiler.
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hydradb::cgroup_directory;
use tokio::task::JoinHandle;

use super::{append_process_memory_metrics, process_memory_metrics, ProcessMemoryMetrics};

const PROCESS_SAMPLING_SUPPORTED: bool = cfg!(all(target_os = "linux", target_env = "gnu"));
const CGROUP_SAMPLING_SUPPORTED: bool = cfg!(target_os = "linux");

#[derive(Clone, Debug, Default)]
struct SourceStatus {
    samples: u64,
    failures: u64,
    last_success: f64,
    available: bool,
}

impl SourceStatus {
    fn observe(&mut self, attempted: bool, succeeded: bool, now: f64) {
        self.available = succeeded;
        if !attempted {
            return;
        }
        if succeeded {
            self.samples += 1;
            self.last_success = now;
        } else {
            self.failures += 1;
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Sample {
    timestamp: f64,
    samples: u64,
    failures: u64,
    rss_peak: Option<u64>,
    live_peak: Option<u64>,
    free_peak: Option<u64>,
    open_file_descriptors_peak: Option<u64>,
    cgroup_peak: Option<u64>,
    process: Option<ProcessMemoryMetrics>,
    cgroup: Option<CgroupMemory>,
    process_status: SourceStatus,
    cgroup_status: SourceStatus,
    runtime: RuntimeGauges,
}

/// Thread, scheduler and CPU-throttling gauges, sampled with the memory
/// snapshot and exported by `crate::otel_metrics::node_runtime`.
///
/// Every field is optional and an absent one is not exported: there is no
/// `/proc` on macOS, cgroup v1 has no `cpu.stat` in the unified layout, and a
/// cgroup v2 without the cpu controller enabled reports usage but no
/// throttling. A zero would claim a measurement nobody took.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeGauges {
    /// The `Threads:` row of `/proc/self/status`.
    pub(crate) threads: Option<u64>,
    pub(crate) cgroup_cpu: Option<CgroupCpu>,
    pub(crate) tokio: Option<TokioRuntime>,
}

/// Cumulative CPU accounting from cgroup v2 `cpu.stat`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub(crate) struct CgroupCpu {
    pub(crate) usage_usec: Option<u64>,
    /// Enforcement periods in which the cgroup was throttled.
    pub(crate) nr_throttled: Option<u64>,
    pub(crate) throttled_usec: Option<u64>,
}

/// The tokio runtime's stable metrics, read from the handle the sampler runs on.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TokioRuntime {
    pub(crate) workers: u64,
    pub(crate) alive_tasks: u64,
    pub(crate) global_queue_depth: u64,
}

impl TokioRuntime {
    /// `None` outside a runtime. Three loads from the scheduler's own counters,
    /// cheap enough to read on a worker.
    fn current() -> Option<Self> {
        let metrics = tokio::runtime::Handle::try_current().ok()?.metrics();
        Some(Self {
            workers: metrics.num_workers() as u64,
            alive_tasks: metrics.num_alive_tasks() as u64,
            global_queue_depth: metrics.global_queue_depth() as u64,
        })
    }
}

impl RuntimeGauges {
    /// A fresh reading, for an exporter that does not hold the sampler. The
    /// file reads run on the blocking pool, like the sampler's own.
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    pub(crate) async fn read() -> Self {
        let tokio = TokioRuntime::current();
        let (threads, cgroup_cpu) = run_blocking(|| (read_threads(), cgroup_directory().as_deref().and_then(read_cgroup_cpu)))
            .await
            .unwrap_or_default();
        Self {
            threads,
            cgroup_cpu,
            tokio,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
struct CgroupMemory {
    current: u64,
    limit: Option<u64>,
    kernel_peak: Option<u64>,
    anon: Option<u64>,
    file: Option<u64>,
    kernel: Option<u64>,
    events: Vec<(&'static str, u64)>,
}

fn update_peak(peak: &mut Option<u64>, value: u64) {
    *peak = Some(peak.unwrap_or_default().max(value));
}

impl Sample {
    fn observe(
        &mut self,
        process: Option<ProcessMemoryMetrics>,
        cgroup: Option<CgroupMemory>,
        now: f64,
        process_attempted: bool,
        cgroup_attempted: bool,
    ) {
        self.process_status.observe(process_attempted, process.is_some(), now);
        self.cgroup_status.observe(cgroup_attempted, cgroup.is_some(), now);
        if (process_attempted || cgroup_attempted) && process.is_none() && cgroup.is_none() {
            self.failures += 1;
            // Failed reads must not publish stale current values.
            self.process = None;
            self.cgroup = None;
            return;
        }
        if process.is_some() || cgroup.is_some() {
            self.timestamp = now;
            self.samples += 1;
        }
        if let Some(memory) = &process {
            update_peak(&mut self.rss_peak, memory.resident_bytes);
            update_peak(&mut self.live_peak, memory.allocator_live_bytes);
            update_peak(&mut self.free_peak, memory.allocator_arena_free_bytes);
            if let Some(open) = memory.open_file_descriptors {
                update_peak(&mut self.open_file_descriptors_peak, open);
            }
        }
        if let Some(memory) = &cgroup {
            update_peak(&mut self.cgroup_peak, memory.current);
        }
        self.process = process;
        self.cgroup = cgroup;
    }

    /// Replaces rather than merges, so a source that stops answering stops
    /// being exported instead of repeating its last value.
    fn observe_runtime(&mut self, runtime: RuntimeGauges) {
        self.runtime = runtime;
    }

    fn render(&self, output: &mut String) {
        crate::otel_metrics::node_runtime::append(output, &self.runtime);
        fn gauge(output: &mut String, name: &str, value: impl std::fmt::Display) {
            output.push_str(&format!("# TYPE {name} gauge\n{name} {value}\n"));
        }
        gauge(output, "graph_memory_sampler_last_success_timestamp_seconds", self.timestamp);
        for (name, value) in [
            ("graph_memory_sampler_samples_total", self.samples),
            ("graph_memory_sampler_failures_total", self.failures),
        ] {
            output.push_str(&format!("# TYPE {name} counter\n{name} {value}\n"));
        }
        output.push_str(concat!(
            "# TYPE graph_memory_sampler_source_samples_total counter\n",
            "# TYPE graph_memory_sampler_source_failures_total counter\n",
            "# TYPE graph_memory_sampler_source_last_success_timestamp_seconds gauge\n",
            "# TYPE graph_memory_sampler_source_available gauge\n",
        ));
        for (source, status) in [("process", &self.process_status), ("cgroup", &self.cgroup_status)] {
            output.push_str(&format!(
                "graph_memory_sampler_source_samples_total{{source=\"{source}\"}} {}\n\
                 graph_memory_sampler_source_failures_total{{source=\"{source}\"}} {}\n\
                 graph_memory_sampler_source_last_success_timestamp_seconds{{source=\"{source}\"}} {}\n\
                 graph_memory_sampler_source_available{{source=\"{source}\"}} {}\n",
                status.samples,
                status.failures,
                status.last_success,
                u8::from(status.available),
            ));
        }
        if let Some(memory) = self.process {
            append_process_memory_metrics(output, memory);
        }
        for (name, value) in [
            ("graph_process_resident_memory_sampled_peak_bytes", self.rss_peak),
            ("graph_process_allocator_live_sampled_peak_bytes", self.live_peak),
            ("graph_process_allocator_arena_free_sampled_peak_bytes", self.free_peak),
            ("graph_process_open_file_descriptors_sampled_peak", self.open_file_descriptors_peak),
            ("graph_cgroup_memory_sampled_peak_bytes", self.cgroup_peak),
        ] {
            if let Some(value) = value {
                gauge(output, name, value);
            }
        }
        if let Some(memory) = &self.cgroup {
            output.push_str("# TYPE graph_cgroup_memory_bytes gauge\n");
            for (kind, value) in [
                ("current", Some(memory.current)),
                ("anon", memory.anon),
                ("file", memory.file),
                ("kernel", memory.kernel),
            ] {
                if let Some(value) = value {
                    output.push_str(&format!("graph_cgroup_memory_bytes{{kind=\"{kind}\"}} {value}\n"));
                }
            }
            if let Some(value) = memory.limit {
                gauge(output, "graph_cgroup_memory_limit_bytes", value);
            }
            if let Some(value) = memory.kernel_peak {
                gauge(output, "graph_cgroup_memory_peak_bytes", value);
            }
            output.push_str("# TYPE graph_cgroup_memory_events_total counter\n");
            for (event, value) in &memory.events {
                output.push_str(&format!("graph_cgroup_memory_events_total{{event=\"{event}\"}} {value}\n"));
            }
        }
    }
}

pub(super) struct MemorySampler {
    sample: Arc<Mutex<Sample>>,
    task: JoinHandle<()>,
}

impl MemorySampler {
    pub(super) fn start() -> Arc<Self> {
        let sample = Arc::new(Mutex::new(Sample::default()));
        let state = sample.clone();
        let task = tokio::spawn(async move {
            let mut cgroup = None;
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let previous_cgroup = cgroup.clone();
                let tokio = TokioRuntime::current();
                match run_blocking(move || collect_sample(previous_cgroup)).await {
                    Ok(collected) => {
                        cgroup = collected.cgroup_directory;
                        let mut sample = state.lock().unwrap_or_else(|e| e.into_inner());
                        sample.observe(
                            collected.process,
                            collected.cgroup,
                            collected.now,
                            PROCESS_SAMPLING_SUPPORTED,
                            CGROUP_SAMPLING_SUPPORTED,
                        );
                        sample.observe_runtime(RuntimeGauges {
                            threads: collected.threads,
                            cgroup_cpu: collected.cgroup_cpu,
                            tokio,
                        });
                    }
                    Err(_) => {
                        cgroup = None;
                        let mut sample = state.lock().unwrap_or_else(|e| e.into_inner());
                        sample.observe(None, None, 0.0, PROCESS_SAMPLING_SUPPORTED, CGROUP_SAMPLING_SUPPORTED);
                        sample.observe_runtime(RuntimeGauges {
                            tokio,
                            ..RuntimeGauges::default()
                        });
                    }
                }
            }
        });
        Arc::new(Self { sample, task })
    }

    pub(super) fn render(&self, output: &mut String) {
        self.sample.lock().unwrap_or_else(|e| e.into_inner()).clone().render(output);
    }

    pub(super) fn resource_snapshot(&self) -> serde_json::Value {
        let s = self.sample.lock().unwrap_or_else(|e| e.into_inner()).clone();
        serde_json::json!({
            "sample_unix_seconds": s.timestamp,
            "process": s.process.filter(|_| s.process_status.available),
            "threads": s.runtime.threads,
            "cgroup_memory": s.cgroup.filter(|_| s.cgroup_status.available),
            "cgroup_cpu": s.runtime.cgroup_cpu,
        })
    }
}

async fn run_blocking<F, T>(operation: F) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation).await
}

struct CollectedSample {
    process: Option<ProcessMemoryMetrics>,
    cgroup: Option<CgroupMemory>,
    cgroup_directory: Option<std::path::PathBuf>,
    now: f64,
    threads: Option<u64>,
    cgroup_cpu: Option<CgroupCpu>,
}

fn collect_sample(mut directory: Option<std::path::PathBuf>) -> CollectedSample {
    // Keep allocator and procfs/cgroupfs calls off runtime workers. No heap
    // walking, task scan or trim occurs.
    let process = process_memory_metrics();
    let threads = read_threads();
    if CGROUP_SAMPLING_SUPPORTED && directory.is_none() {
        directory = cgroup_directory();
    }
    let cgroup = directory.as_deref().and_then(read_cgroup);
    let cgroup_cpu = directory.as_deref().and_then(read_cgroup_cpu);
    if CGROUP_SAMPLING_SUPPORTED && cgroup.is_none() {
        // Re-resolve next pass after either discovery or reading failed.
        directory = None;
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64();
    CollectedSample {
        process,
        cgroup,
        cgroup_directory: directory,
        now,
        threads,
        cgroup_cpu,
    }
}

impl Drop for MemorySampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn field(input: &str, key: &str) -> Option<u64> {
    input.lines().find_map(|line| {
        let mut words = line.split_ascii_whitespace();
        (words.next()? == key).then(|| words.next()?.parse().ok()).flatten()
    })
}

fn read_cgroup(dir: &std::path::Path) -> Option<CgroupMemory> {
    let current = std::fs::read_to_string(dir.join("memory.current")).ok()?.trim().parse().ok()?;
    let optional_number = |file| std::fs::read_to_string(dir.join(file)).ok()?.trim().parse::<u64>().ok();
    let stat = std::fs::read_to_string(dir.join("memory.stat")).unwrap_or_default();
    let events = std::fs::read_to_string(dir.join("memory.events")).unwrap_or_default();
    Some(CgroupMemory {
        current,
        limit: optional_number("memory.max"), // "max" means unlimited, not zero.
        kernel_peak: optional_number("memory.peak"),
        anon: field(&stat, "anon"),
        file: field(&stat, "file"),
        kernel: field(&stat, "kernel"),
        events: ["low", "high", "max", "oom", "oom_kill", "oom_group_kill"]
            .into_iter()
            .filter_map(|name| field(&events, name).map(|value| (name, value)))
            .collect(),
    })
}

fn read_cgroup_cpu(dir: &std::path::Path) -> Option<CgroupCpu> {
    parse_cgroup_cpu(&std::fs::read_to_string(dir.join("cpu.stat")).ok()?)
}

/// `None` when the file carries none of the three rows, so an unreadable or
/// foreign file is absent rather than a set of zeroes.
fn parse_cgroup_cpu(stat: &str) -> Option<CgroupCpu> {
    let cpu = CgroupCpu {
        usage_usec: field(stat, "usage_usec"),
        nr_throttled: field(stat, "nr_throttled"),
        throttled_usec: field(stat, "throttled_usec"),
    };
    (cpu != CgroupCpu::default()).then_some(cpu)
}

/// `None` wherever `/proc` does not exist, which is every non-Linux target.
fn read_threads() -> Option<u64> {
    parse_threads(&std::fs::read_to_string("/proc/self/status").ok()?)
}

fn parse_threads(status: &str) -> Option<u64> {
    status.lines().find_map(|line| line.strip_prefix("Threads:")?.trim().parse().ok())
}
