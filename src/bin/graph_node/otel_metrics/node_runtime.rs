//! Thread, tokio scheduler and cgroup CPU gauges. One table drives both
//! exports, so Prometheus and OTLP cannot disagree about which series exist.
//!
//! `/metrics` renders the memory sampler's latest one-second reading; the OTLP
//! collector takes a fresh reading each interval, because it does not hold the
//! sampler and the reading costs two small file reads. A value the platform
//! cannot provide is absent from both, never zero.

use crate::admin::memory_sampler::RuntimeGauges;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    /// A point-in-time value.
    Gauge,
    /// A cumulative count of events.
    Counter,
    /// A cumulative sum of microseconds.
    MicrosecondCounter,
}

struct Row {
    prometheus: &'static str,
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    otlp: &'static str,
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    description: &'static str,
    /// UCUM annotation for a gauge; counters take theirs from [`Kind`].
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    gauge_unit: &'static str,
    kind: Kind,
    value: fn(&RuntimeGauges) -> Option<u64>,
}

const ROWS: [Row; 7] = [
    Row {
        prometheus: "graph_process_threads",
        otlp: "hydradb.process.threads",
        description: "OS threads in the process, from /proc/self/status",
        gauge_unit: "{thread}",
        kind: Kind::Gauge,
        value: |g| g.threads,
    },
    Row {
        prometheus: "graph_runtime_tokio_workers",
        otlp: "hydradb.runtime.tokio.workers",
        description: "Worker threads in the tokio runtime",
        gauge_unit: "{thread}",
        kind: Kind::Gauge,
        value: |g| g.tokio.map(|t| t.workers),
    },
    Row {
        prometheus: "graph_runtime_tokio_alive_tasks",
        otlp: "hydradb.runtime.tokio.alive_tasks",
        description: "Tasks spawned on the tokio runtime that have not completed",
        gauge_unit: "{task}",
        kind: Kind::Gauge,
        value: |g| g.tokio.map(|t| t.alive_tasks),
    },
    Row {
        prometheus: "graph_runtime_tokio_global_queue_depth",
        otlp: "hydradb.runtime.tokio.global_queue_depth",
        description: "Tasks waiting in the tokio runtime's global injection queue",
        gauge_unit: "{task}",
        kind: Kind::Gauge,
        value: |g| g.tokio.map(|t| t.global_queue_depth),
    },
    Row {
        prometheus: "graph_cgroup_cpu_usage_microseconds_total",
        otlp: "hydradb.cgroup.cpu.usage",
        description: "CPU time consumed by the container's cgroup, from cpu.stat usage_usec",
        gauge_unit: "",
        kind: Kind::MicrosecondCounter,
        value: |g| g.cgroup_cpu.and_then(|c| c.usage_usec),
    },
    Row {
        prometheus: "graph_cgroup_cpu_throttled_periods_total",
        otlp: "hydradb.cgroup.cpu.throttled_periods",
        description: "CFS enforcement periods in which the cgroup was throttled, from cpu.stat nr_throttled",
        gauge_unit: "",
        kind: Kind::Counter,
        value: |g| g.cgroup_cpu.and_then(|c| c.nr_throttled),
    },
    Row {
        prometheus: "graph_cgroup_cpu_throttled_microseconds_total",
        otlp: "hydradb.cgroup.cpu.throttled",
        description: "Time the cgroup spent throttled, from cpu.stat throttled_usec",
        gauge_unit: "",
        kind: Kind::MicrosecondCounter,
        value: |g| g.cgroup_cpu.and_then(|c| c.throttled_usec),
    },
];

/// Render every row `gauges` has a value for.
pub(crate) fn append(output: &mut String, gauges: &RuntimeGauges) {
    use std::fmt::Write as _;
    for row in &ROWS {
        let Some(value) = (row.value)(gauges) else {
            continue;
        };
        let kind = match row.kind {
            Kind::Gauge => "gauge",
            Kind::Counter | Kind::MicrosecondCounter => "counter",
        };
        writeln!(output, "# TYPE {} {kind}", row.prometheus).unwrap();
        writeln!(output, "{} {value}", row.prometheus).unwrap();
    }
}

#[cfg(feature = "otlp")]
#[derive(Debug)]
enum Instrument {
    Gauge(hydradb_telemetry::meter::ObservableGauge),
    Counter(hydradb_telemetry::meter::ObservableCounter),
}

#[cfg(feature = "otlp")]
#[derive(Debug)]
pub(super) struct Instruments {
    #[allow(clippy::type_complexity)]
    rows: Vec<(Instrument, fn(&RuntimeGauges) -> Option<u64>)>,
}

#[cfg(feature = "otlp")]
impl Instruments {
    pub(super) fn register(providers: &hydradb_telemetry::otlp::Providers) -> Self {
        use hydradb_telemetry::meter::{CounterSpec, CounterUnit, ObservableCounter, ObservableGauge};
        let meter = providers.meter(super::METER_NAME);
        let rows = ROWS
            .iter()
            .map(|row| {
                let counter = |unit| {
                    Instrument::Counter(ObservableCounter::register(
                        &meter,
                        CounterSpec {
                            name: row.otlp,
                            description: row.description,
                            unit,
                        },
                    ))
                };
                let instrument = match row.kind {
                    Kind::Gauge => Instrument::Gauge(ObservableGauge::register(&meter, row.otlp, row.description, row.gauge_unit)),
                    Kind::Counter => counter(CounterUnit::Count),
                    Kind::MicrosecondCounter => counter(CounterUnit::Microseconds),
                };
                (instrument, row.value)
            })
            .collect();
        Self { rows }
    }

    pub(super) async fn record(&self) {
        self.record_reading(&RuntimeGauges::read().await);
    }

    /// A row the platform cannot provide reports no series rather than a zero.
    /// A gauge whose reading goes missing after a good one is forgotten, since
    /// its last value would claim a measurement nobody took.
    pub(super) fn record_reading(&self, gauges: &RuntimeGauges) {
        for (instrument, value) in &self.rows {
            let result = match (instrument, value(gauges)) {
                (Instrument::Gauge(gauge), Some(value)) => gauge.record(&[], value as f64),
                (Instrument::Gauge(gauge), None) => gauge.forget(&[]),
                (Instrument::Counter(counter), Some(value)) => counter.record(&[], value),
                // Frozen, not forgotten: see `ObservableCounter`, a gap reads
                // as a reset and spikes `rate()`.
                (Instrument::Counter(_), None) => Ok(()),
            };
            if let Err(error) = result {
                tracing::warn!(%error, "node runtime metric was not published");
            }
        }
    }
}
