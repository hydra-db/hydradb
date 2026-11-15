//! Fixed-stage memory attribution export. Uses the same snapshots as Prometheus.
#[cfg(feature = "otlp")]
#[derive(Debug)]
pub(super) struct Instruments {
    items: hydradb_telemetry::meter::ObservableGauge,
    bytes: hydradb_telemetry::meter::ObservableGauge,
    oldest: hydradb_telemetry::meter::ObservableGauge,
    oldest_tracked: hydradb_telemetry::meter::ObservableGauge,
    overflow_items: hydradb_telemetry::meter::ObservableGauge,
    overflow_cohort: hydradb_telemetry::meter::ObservableGauge,
    entered: hydradb_telemetry::meter::ObservableCounter,
    exited: hydradb_telemetry::meter::ObservableCounter,
    duration: hydradb_telemetry::meter::ObservableHistogram,
}
#[cfg(feature = "otlp")]
impl Instruments {
    pub(super) fn register(providers: &hydradb_telemetry::otlp::Providers) -> Result<Self, hydradb_telemetry::meter::HistogramError> {
        use hydradb_telemetry::meter::{
            CounterSpec, CounterUnit, HistogramSpec, HistogramUnit, ObservableCounter, ObservableGauge, ObservableHistogram,
        };
        let meter = providers.meter(super::METER_NAME);
        Ok(Self {
            items: ObservableGauge::register(&meter, "hydradb.memory.diagnostic.items", "Current objects or operations in a fixed diagnostic stage", "{item}"),
            bytes: ObservableGauge::register(&meter, "hydradb.memory.diagnostic.estimated_bytes", "Estimated owned request bytes; shard views overlap client execution; storage byte sizes unknown", "By"),
            oldest: ObservableGauge::register(&meter, "hydradb.memory.diagnostic.oldest_seconds", "Compatibility age combining retained detail and overflow cohort; inspect explicit gauges for precision", "s"),
            oldest_tracked: ObservableGauge::register(&meter, "hydradb.memory.diagnostic.oldest_tracked_seconds", "Age of oldest item in the bounded retained timestamp set", "s"),
            overflow_items: ObservableGauge::register(&meter, "hydradb.memory.diagnostic.tracking_overflow_items", "Current stage items beyond the bounded retained timestamp set", "{item}"),
            overflow_cohort: ObservableGauge::register(&meter, "hydradb.memory.diagnostic.overflow_cohort_seconds", "Age since the current timestamp-tracking overflow cohort began; may conservatively exceed the oldest remaining overflow item", "s"),
            entered: ObservableCounter::register(&meter, CounterSpec { name: "hydradb.memory.diagnostic.entered", description: "Cumulative stage entries", unit: CounterUnit::Count }),
            exited: ObservableCounter::register(&meter, CounterSpec { name: "hydradb.memory.diagnostic.exited", description: "Cumulative stage exits including cancellation and errors", unit: CounterUnit::Count }),
            duration: ObservableHistogram::register(&meter, HistogramSpec { name: "hydradb.memory.diagnostic.duration", description: "Stage residency on exit including cancellation and errors", unit: HistogramUnit::Seconds }, &hydradb::DURATION_BUCKET_BOUNDS_US)?,
        })
    }
    pub(super) fn record(&self) {
        use hydradb_telemetry::semconv::L_MEMORY_STAGE;
        for snapshot in hydradb::memory_diagnostic_snapshot() {
            let labels = [(L_MEMORY_STAGE, snapshot.stage)];
            // Label is closed and never contains duplicates or reserved le.
            let results = [
                self.items.record(&labels, snapshot.items as f64),
                self.bytes.record(&labels, snapshot.estimated_bytes as f64),
                self.oldest.record(&labels, snapshot.oldest_seconds),
                self.oldest_tracked.record(&labels, snapshot.oldest_tracked_seconds),
                self.overflow_items.record(&labels, snapshot.tracking_overflow_items as f64),
                self.overflow_cohort.record(&labels, snapshot.overflow_cohort_seconds),
                self.entered.record(&labels, snapshot.entered),
                self.exited.record(&labels, snapshot.exited),
            ];
            for result in results {
                if let Err(error) = result {
                    tracing::warn!(%error, "memory diagnostic metric was not published");
                }
            }
            if let Err(error) = self
                .duration
                .record_snapshot(&labels, &snapshot.duration.bucket_counts, snapshot.duration.sum_us)
            {
                tracing::warn!(%error, "memory diagnostic duration was not published");
            }
        }
    }
}

/// Direct Prometheus export is independent of the OTLP collector and tracing.
pub(crate) fn append(output: &mut String) {
    use std::fmt::Write as _;
    for (suffix, kind) in [
        ("items", "gauge"),
        ("estimated_bytes", "gauge"),
        ("oldest_seconds", "gauge"),
        ("oldest_tracked_seconds", "gauge"),
        ("tracking_overflow_items", "gauge"),
        ("overflow_cohort_seconds", "gauge"),
        ("entered_total", "counter"),
        ("exited_total", "counter"),
        ("duration_seconds", "histogram"),
    ] {
        writeln!(output, "# TYPE graph_memory_diagnostic_{suffix} {kind}").unwrap();
    }
    for sample in hydradb::memory_diagnostic_snapshot() {
        let stage = sample.stage;
        writeln!(output, "graph_memory_diagnostic_items{{stage=\"{stage}\"}} {}", sample.items).unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_estimated_bytes{{stage=\"{stage}\"}} {}",
            sample.estimated_bytes
        )
        .unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_oldest_seconds{{stage=\"{stage}\"}} {}",
            sample.oldest_seconds
        )
        .unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_oldest_tracked_seconds{{stage=\"{stage}\"}} {}",
            sample.oldest_tracked_seconds
        )
        .unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_tracking_overflow_items{{stage=\"{stage}\"}} {}",
            sample.tracking_overflow_items
        )
        .unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_overflow_cohort_seconds{{stage=\"{stage}\"}} {}",
            sample.overflow_cohort_seconds
        )
        .unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_entered_total{{stage=\"{stage}\"}} {}",
            sample.entered
        )
        .unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_exited_total{{stage=\"{stage}\"}} {}",
            sample.exited
        )
        .unwrap();
        for (bound, count) in sample.duration.cumulative() {
            let le = bound.map_or_else(|| "+Inf".to_string(), |b| (b as f64 / 1_000_000.0).to_string());
            writeln!(
                output,
                "graph_memory_diagnostic_duration_seconds_bucket{{stage=\"{stage}\",le=\"{le}\"}} {count}"
            )
            .unwrap();
        }
        writeln!(
            output,
            "graph_memory_diagnostic_duration_seconds_sum{{stage=\"{stage}\"}} {}",
            sample.duration.sum_us as f64 / 1_000_000.0
        )
        .unwrap();
        writeln!(
            output,
            "graph_memory_diagnostic_duration_seconds_count{{stage=\"{stage}\"}} {}",
            sample.duration.count()
        )
        .unwrap();
    }
}
