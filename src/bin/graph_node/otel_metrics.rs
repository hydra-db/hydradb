//! The OTel half of the duration-histogram export, and the name table it is
//! driven by.
//!
//! # Two exports, one enumeration
//!
//! The kernel enumerates its histograms by **Rust identifier** — `read_latency`,
//! `query_rows_latency` — and knows nothing about either exposition vocabulary
//! (`slatedb_graph_kernel::ClientQueryMetricsSnapshot::histogram_fields` and
//! friends). The two vocabularies live here and in [`crate::admin`]: this module
//! holds the OTel names, `admin.rs` holds the Prometheus names, and
//! [`tests::every_histogram_field_reaches_both_exports`] fails the build if a
//! field the kernel enumerates is missing from either. That is the property
//! §1.6 of `docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md` asks
//! for: adding a histogram cannot silently reach one export and not the other.
//!
//! # Names
//!
//! §1.9: `db.*` where a semantic convention genuinely exists, `turbolay.*`
//! otherwise. Of the five histograms exactly one quantity has a semconv name —
//! client query duration is `db.client.operation.duration`, stable, and fixed in
//! **seconds** while the kernel measures in microseconds. The conversion is one
//! call at the export boundary ([`ExportUnit`]) and a bound table in seconds
//! never travels back towards the kernel.
//!
//! `db.namespace` is **deliberately omitted** from the `db.client.*` family even
//! though semconv marks it required-if-applicable. It is applicable — the
//! namespace is the `scope` — and `scope` is the unbounded tenant root the
//! metric-label registry exists to keep off metrics. A conformance checker will
//! flag its absence; that flag is the design, not a bug.
//!
//! ## Read and write share one instrument name
//!
//! §1.10 splits client latency into a read and a write distribution, and §1.9
//! names both `db.client.operation.duration`. Semconv separates them with
//! `db.operation.name` rather than with two metric names, and that key now
//! exists as `turbolay_telemetry::semconv::L_DB_OPERATION_NAME`, so both rows
//! carry the semconv name and are told apart by a label.
//!
//! It shipped for one commit as `db.client.operation.duration.read` / `….write`
//! because the label did not exist yet, and the reason that was not simply
//! "record both under one name and move on" is worth keeping: two series under
//! one instrument name with no distinguishing label do not error, do not warn
//! and do not duplicate — `ObservableHistogram` keys its series map by the label
//! set, so the second `record_snapshot` of each interval would *overwrite* the
//! first. Silent collapse, plausible numbers, re-conflating exactly what the
//! split exists to separate.
//!
//! That is why [`NodeHistograms::register`] registers one instrument per
//! distinct *name* rather than one per row, and why
//! [`otel_instrument_groups`] — which is what it iterates — is asserted on by a
//! test that runs without the `otlp` feature. A row added with a duplicate name
//! and no operation label is a test failure, not a silently merged series.
//!
//! `db.namespace` is still deliberately absent, and that is not incidental to
//! the above: the two attributes semconv marks required on this metric are
//! `db.system.name` (present, one value) and `db.namespace` (absent, because
//! its only value is the unbounded `turbolay.scope`).
//!
//! # Wiring
//!
//! [`NodeHistograms::register`] takes a `turbolay_telemetry::otlp::Providers`,
//! reached through `TelemetryGuard::providers()`. [`MetricCollection::start`] is
//! what `graph-node.rs` calls: it registers the instruments once and owns the
//! interval task that snapshots the kernel and publishes into them. Without the
//! `otlp` feature, or with no OTLP endpoint configured, it is inert and starts
//! no task at all.
//!
//! `record_transport` remains unfed: `graph-node` instantiates neither
//! `TcpQueryServer` nor `TcpQueryCellClient`, so it holds no
//! `QueryTransportMetricsSnapshot` to record. The name table and the rendering
//! cover `rpc_latency` and `serve_latency` regardless, because the field lives
//! in the kernel and the export must not be the thing that decides which
//! binary is allowed to have it.
//!
//! # Why a counter family and not a histogram data point
//!
//! `opentelemetry` 0.32 has no observable histogram and `opentelemetry_sdk`
//! 0.32 has no `MetricProducer`, so a histogram computed in the kernel and read
//! from a cached snapshot cannot reach OTLP as a histogram data point. It
//! reaches it as a family of observable counters keyed by `le`, which is what a
//! Prometheus histogram already is. `turbolay_telemetry::meter` owns that
//! rendering, and owning it in one place is what stops the two exports
//! disagreeing about where a bucket ends. See that module for the full
//! argument.

// Only the recording half names a snapshot, and that half is behind `otlp`.
#[cfg(feature = "otlp")]
use slatedb_graph_kernel::DurationHistogramSnapshot;

/// The unit an exported histogram's bounds and sum are **rendered** in.
///
/// The kernel measures in microseconds and only ever measures in microseconds.
/// This is a boundary conversion, and it exists because
/// `db.client.operation.duration` is a stable semantic convention fixed in
/// seconds — a series claiming that name in microseconds is worse than a series
/// with a Turbolay name.
///
/// It is deliberately shared by both exports. The names may diverge; the *unit*
/// must not, or `histogram_quantile` over the Prometheus family and the same
/// query over the OTLP family answer differently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExportUnit {
    /// Export as the kernel measures. UCUM `us`.
    Microseconds,
    /// Divide by 1e6 at the boundary. UCUM `s`.
    Seconds,
}

impl ExportUnit {
    /// Render one finite bucket bound as an `le` label value.
    ///
    /// Character-for-character what `turbolay_telemetry::meter::HistogramUnit`
    /// does, and it has to be: two renderings of the same bound that differ by
    /// a trailing zero are two series to every backend downstream. `{}` on
    /// `f64` is the shortest representation that round-trips, so 100 µs renders
    /// as `0.0001` and 30 s as `30`.
    pub fn render_bound(self, bound_us: u64) -> String {
        match self {
            Self::Microseconds => bound_us.to_string(),
            Self::Seconds => (bound_us as f64 / 1_000_000.0).to_string(),
        }
    }

    /// Render a microsecond sum in the export unit.
    pub fn render_sum(self, sum_us: u64) -> String {
        match self {
            Self::Microseconds => sum_us.to_string(),
            Self::Seconds => (sum_us as f64 / 1_000_000.0).to_string(),
        }
    }

    /// The same choice, spelled the way the meter spells it.
    #[cfg(feature = "otlp")]
    fn meter_unit(self) -> turbolay_telemetry::meter::HistogramUnit {
        use turbolay_telemetry::meter::HistogramUnit;
        match self {
            Self::Microseconds => HistogramUnit::Microseconds,
            Self::Seconds => HistogramUnit::Seconds,
        }
    }
}

/// One row of the OTel name table.
#[derive(Clone, Copy, Debug)]
pub struct OtelHistogram {
    /// The kernel's Rust identifier — the key both name tables are keyed by.
    pub field: &'static str,
    /// OTel metric name stem. The meter derives `{name}.bucket`, `{name}.sum`
    /// and `{name}.count` from it.
    ///
    /// **Not unique.** Two rows sharing a name is the semconv shape for one
    /// metric measured over two populations; what makes them two series rather
    /// than one is [`OtelHistogram::operation`], and a shared name with no
    /// operation is a build failure over in
    /// [`tests::rows_sharing_a_name_are_told_apart_by_the_operation_label`].
    pub name: &'static str,
    /// Instrument description.
    pub description: &'static str,
    /// Bound and sum unit.
    pub unit: ExportUnit,
    /// The `db.operation.name` value this row is recorded under, if the metric
    /// is one semconv splits by operation rather than by name.
    ///
    /// `None` for every `turbolay.*` metric: those are named for what they
    /// measure, so a second dimension would be a distinction the name already
    /// makes.
    pub operation: Option<&'static str>,
}

/// The OTel name table. One row per histogram the kernel enumerates.
///
/// Adding a histogram to a snapshot type and not adding it here fails
/// [`tests::every_histogram_field_reaches_both_exports`].
pub const OTEL_HISTOGRAMS: &[OtelHistogram] = &[
    OtelHistogram {
        field: "read_latency",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(turbolay_telemetry::semconv::DB_OPERATION_READ),
    },
    OtelHistogram {
        field: "write_latency",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(turbolay_telemetry::semconv::DB_OPERATION_WRITE),
    },
    OtelHistogram {
        field: "query_rows_latency",
        name: "turbolay.query.rows.duration",
        description: "Shard row-query execution",
        unit: ExportUnit::Microseconds,
        operation: None,
    },
    OtelHistogram {
        field: "rpc_latency",
        name: "turbolay.query.transport.rpc.duration",
        description: "Query-transport client RPC round-trip",
        unit: ExportUnit::Microseconds,
        operation: None,
    },
    OtelHistogram {
        field: "serve_latency",
        name: "turbolay.query.transport.serve.duration",
        description: "Query-transport server executor time",
        unit: ExportUnit::Microseconds,
        operation: None,
    },
];

/// The OTel name and unit for a kernel field identifier.
pub fn otel_histogram(field: &str) -> Option<&'static OtelHistogram> {
    OTEL_HISTOGRAMS.iter().find(|export| export.field == field)
}

/// [`OTEL_HISTOGRAMS`] grouped by instrument name, in first-appearance order.
///
/// **One group is one registered instrument.** This exists as its own function,
/// outside the `otlp` cfg, because the alternative — registering per row —
/// registers the same instrument name twice, and the SDK's response to that is
/// a duplicate-instrument conflict rather than a second series. Deriving the
/// registration from a grouping a plain unit test can inspect is what puts that
/// under `just ci`, where an `otlp`-gated test would only be compiled.
pub fn otel_instrument_groups() -> Vec<(&'static str, Vec<&'static OtelHistogram>)> {
    let mut groups: Vec<(&'static str, Vec<&'static OtelHistogram>)> = Vec::new();
    for export in OTEL_HISTOGRAMS {
        match groups.iter_mut().find(|(name, _)| *name == export.name) {
            Some((_, rows)) => rows.push(export),
            None => groups.push((export.name, vec![export])),
        }
    }
    groups
}

/// The instrumentation scope every instrument registered here belongs to.
#[cfg(feature = "otlp")]
const METER_NAME: &str = "turbolay.graph_node";

/// Every registered histogram family, keyed by the kernel's field identifier.
///
/// Registration builds the instruments once; the `record_*` methods publish the
/// latest cached snapshot into them. The OTel callbacks run on the periodic
/// reader's own OS thread and only ever read, which is why nothing here is
/// `async` and why a failure is returned rather than panicked.
///
/// Several fields can map to the **same** `ObservableHistogram` — that is what
/// `read_latency` and `write_latency` sharing `db.client.operation.duration`
/// means — so the values are `Arc`s and the row's `operation` is what keeps
/// their series apart inside it.
#[cfg(feature = "otlp")]
#[derive(Debug)]
pub struct NodeHistograms {
    registered: std::collections::HashMap<
        &'static str,
        (
            std::sync::Arc<turbolay_telemetry::meter::ObservableHistogram>,
            Option<&'static str>,
        ),
    >,
}

#[cfg(feature = "otlp")]
impl NodeHistograms {
    /// Register one instrument per distinct name in [`OTEL_HISTOGRAMS`] against
    /// the process's meter.
    ///
    /// Per *name*, not per row: two rows sharing a name are two series of one
    /// instrument, and registering that name twice is a duplicate-instrument
    /// conflict. The grouping is [`otel_instrument_groups`].
    ///
    /// The ladder comes from the kernel
    /// ([`slatedb_graph_kernel::DURATION_BUCKET_BOUNDS_US`]) rather than being
    /// restated here, so the Prometheus rendering in [`crate::admin`] and this
    /// one cannot disagree about where a bucket ends.
    pub fn register(
        providers: &turbolay_telemetry::otlp::Providers,
    ) -> Result<Self, turbolay_telemetry::meter::HistogramError> {
        use turbolay_telemetry::meter::{HistogramSpec, ObservableHistogram};

        let meter = providers.meter(METER_NAME);
        let mut registered = std::collections::HashMap::with_capacity(OTEL_HISTOGRAMS.len());
        for (name, rows) in otel_instrument_groups() {
            // Every row in a group shares the instrument, so its unit and
            // description have to be the group's, not the row's. A group whose
            // rows disagree about either is caught by
            // `rows_sharing_a_name_are_told_apart_by_the_operation_label`.
            let first = rows.first().expect("a group has at least one row");
            let histogram = std::sync::Arc::new(ObservableHistogram::register(
                &meter,
                HistogramSpec {
                    name,
                    description: first.description,
                    unit: first.unit.meter_unit(),
                },
                &slatedb_graph_kernel::DURATION_BUCKET_BOUNDS_US,
            )?);
            for row in rows {
                registered.insert(
                    row.field,
                    (std::sync::Arc::clone(&histogram), row.operation),
                );
            }
        }
        Ok(Self { registered })
    }

    /// Publish one field's snapshot.
    ///
    /// A field with no registered instrument is a hole in [`OTEL_HISTOGRAMS`],
    /// which `every_histogram_field_reaches_both_exports` makes impossible to
    /// ship. It is skipped rather than raised because losing one series is a
    /// better outcome on a metrics thread than losing the interval.
    fn record(
        &self,
        field: &'static str,
        labels: &[(turbolay_telemetry::semconv::MetricLabel, &str)],
        snapshot: &DurationHistogramSnapshot,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        let Some((histogram, operation)) = self.registered.get(field) else {
            debug_assert!(false, "{field} is enumerated but not in OTEL_HISTOGRAMS");
            return Ok(());
        };
        // Appended here rather than passed by every caller: which rows share an
        // instrument is a property of the name table, and a caller that had to
        // remember the operation label is a caller that can forget it — and
        // forgetting it merges two populations without a word.
        let mut labels = labels.to_vec();
        if let Some(operation) = operation {
            labels.push((turbolay_telemetry::semconv::L_DB_OPERATION_NAME, *operation));
        }
        histogram.record_snapshot(&labels, &snapshot.bucket_counts, snapshot.sum_us)
    }

    /// The process-global client query histograms.
    ///
    /// Carries `db.system.name` because that is the attribute a vendor's
    /// database view keys on, and putting `db.*` names on the wire while
    /// omitting it would pay the cost of §1.9's vocabulary split and collect
    /// none of the benefit. `db.operation.name` is added by [`Self::record`]
    /// from the name table, which is what makes `read_latency` and
    /// `write_latency` two series of the one semconv instrument.
    ///
    /// It carries no `scope`, and therefore no `db.namespace`: that is §1.4's
    /// whole point, and the label registry makes it a type error rather than a
    /// review comment.
    pub fn record_client(
        &self,
        snapshot: &slatedb_graph_kernel::ClientQueryMetricsSnapshot,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        use turbolay_telemetry::semconv::{DB_SYSTEM_NEO4J, L_DB_SYSTEM_NAME};

        for (field, histogram) in snapshot.histogram_fields() {
            self.record(field, &[(L_DB_SYSTEM_NAME, DB_SYSTEM_NEO4J)], histogram)?;
        }
        Ok(())
    }

    /// One shard's operational histograms, labelled by `cell_id` alone.
    ///
    /// Never `cell_id × edge_type`: an 18-bucket family times 96 is 1,728
    /// series per instrument per node, which is where §1.3's cardinality
    /// arithmetic stops being affordable. And never `scope`, which `/metrics`
    /// does carry — that divergence is deliberate and is the reason both
    /// exports exist.
    pub fn record_shard(
        &self,
        metrics: &slatedb_graph_kernel::ScopedGraphShardRuntimeMetrics,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        use turbolay_telemetry::semconv::L_CELL_ID;

        for (field, histogram) in metrics.shard.operational.histogram_fields() {
            self.record(
                field,
                &[(L_CELL_ID, metrics.shard.cell_id.as_str())],
                histogram,
            )?;
        }
        Ok(())
    }

    /// The query-transport histograms.
    ///
    /// Unlabelled: only one of `rpc_latency` and `serve_latency` is ever
    /// non-empty on a given instance, so the instrument name already says which
    /// side of the wire it was measured on.
    pub fn record_transport(
        &self,
        snapshot: &slatedb_graph_kernel::QueryTransportMetricsSnapshot,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        for (field, histogram) in snapshot.histogram_fields() {
            self.record(field, &[], histogram)?;
        }
        Ok(())
    }
}

/// The interval task that feeds the registered instruments, or nothing at all.
///
/// This type exists in both feature configurations so `graph-node.rs` needs no
/// `cfg` of its own. Without `otlp` — or with `otlp` on and no OTLP endpoint
/// configured — [`MetricCollection::start`] registers nothing, spawns nothing
/// and costs nothing, which is the ordinary build.
///
/// # Why a task rather than a callback
///
/// The OTel callback type is `Fn`, not `async fn`
/// (`opentelemetry-0.32.0/src/metrics/instruments/mod.rs:264`), and it runs on
/// the `PeriodicReader`'s own OS thread. The only source of shard metrics is
/// `ScopedRoutedGraphCluster::local_shard_runtime_metrics`, which is `async` and
/// takes cache mutexes. Neither `.await` nor `block_on` is available inside the
/// callback, so collection has to happen somewhere else and publish into a
/// cache the callback reads. That somewhere is here.
///
/// # Why the same interval as the reader
///
/// `TelemetryConfig::metric_export_interval` is read once in `main` and used for
/// both the `PeriodicReader` and this task. They are two halves of one clock: if
/// this task is slower, every export repeats a stale snapshot; if it is faster,
/// the work of the extra rounds is discarded unread. The env var
/// `OTEL_METRIC_EXPORT_INTERVAL` is not read here — the SDK would parse it a
/// second time and the two parses are not identical (the config rejects a zero
/// the SDK silently ignores, which matters precisely because a zero the SDK
/// ignores is still a zero this loop would honour).
pub struct MetricCollection {
    #[cfg(feature = "otlp")]
    running: Option<(
        tokio::sync::watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    )>,
}

impl MetricCollection {
    /// Register the instruments and start the interval task.
    ///
    /// Every failure degrades to "no OTel metrics" rather than to a failed
    /// boot. A node that will not start because a histogram would not register
    /// is strictly worse than a node with one export instead of two, and
    /// `/metrics` is unaffected either way.
    #[cfg(feature = "otlp")]
    pub fn start(
        telemetry: &turbolay_telemetry::TelemetryGuard,
        interval: std::time::Duration,
        query: slatedb_graph_kernel::ClientQueryService,
        node: std::sync::Arc<slatedb_graph_kernel::ScopedRoutedGraphCluster>,
    ) -> Self {
        let Some(providers) = telemetry.providers() else {
            // No endpoint: there is no metrics pipeline to feed, so taking the
            // cache locks once a minute would buy nothing at all.
            return Self { running: None };
        };
        let histograms = match NodeHistograms::register(providers) {
            Ok(histograms) => std::sync::Arc::new(histograms),
            Err(error) => {
                tracing::warn!(error = %error, "OTel histograms did not register; no metrics will be exported");
                return Self { running: None };
            }
        };

        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(collect_forever(histograms, query, node, interval, stop_rx));
        tracing::info!(
            interval_ms = interval.as_millis() as u64,
            "OTel metric collection started"
        );
        Self {
            running: Some((stop_tx, task)),
        }
    }

    /// Without the `otlp` feature there is no meter to feed.
    #[cfg(not(feature = "otlp"))]
    pub fn start(
        _telemetry: &turbolay_telemetry::TelemetryGuard,
        _interval: std::time::Duration,
        _query: slatedb_graph_kernel::ClientQueryService,
        _node: std::sync::Arc<slatedb_graph_kernel::ScopedRoutedGraphCluster>,
    ) -> Self {
        Self {}
    }

    /// Stop the task and wait for it.
    ///
    /// Awaited rather than aborted, and awaited *before* `run_node` unwraps the
    /// cluster `Arc`: the task holds a clone of it and of the query service, so
    /// a collection still in flight is a `try_unwrap` failure and a node that
    /// reports "still has active runtime references" on a clean shutdown.
    pub async fn stop(self) {
        #[cfg(feature = "otlp")]
        if let Some((stop_tx, task)) = self.running {
            let _ = stop_tx.send(true);
            if let Err(error) = task.await {
                tracing::warn!(error = %error, "OTel metric collection task failed");
            }
        }
    }
}

impl std::fmt::Debug for MetricCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricCollection").finish_non_exhaustive()
    }
}

/// One pass: publish the client histograms, then every shard's.
///
/// The order is deliberate. `ClientQueryService::metrics()` is a set of relaxed
/// `AtomicU64` loads — synchronous, lock-free, unconditionally cheap — while the
/// shard snapshot takes twelve cache mutexes per cell. Recording the cheap half
/// first means a shard that cannot be sampled costs only the shard series.
#[cfg(feature = "otlp")]
async fn collect_once(
    histograms: &NodeHistograms,
    query: &slatedb_graph_kernel::ClientQueryService,
    node: &slatedb_graph_kernel::ScopedRoutedGraphCluster,
    shard_budget: std::time::Duration,
) {
    if let Err(error) = histograms.record_client(&query.metrics()) {
        tracing::warn!(error = %error, "client query histograms were not published");
    }

    // `local_shard_runtime_metrics` is unbounded, and two of the things it waits
    // on are not this task's to fix: the scoped-cluster mutex is held across a
    // shard open elsewhere in `engine/cluster.rs`, and each cell then takes
    // twelve read-path cache mutexes (disjointly, since `08e78df` — this task is
    // what exercises that fix every interval, and before it the collector could
    // park on the seventh lock while holding the first six).
    //
    // So the wait is bounded. Cancelling the future at an await point drops the
    // guards it holds and nothing else; the cost of giving up is that the
    // previous interval's series stay published, which for a cumulative counter
    // is the *correct* degradation — a gap would read as a reset and produce a
    // spurious `rate()` spike on a node whose only fault was being busy.
    match tokio::time::timeout(shard_budget, node.local_shard_runtime_metrics()).await {
        Ok(shards) => {
            for shard in &shards {
                if let Err(error) = histograms.record_shard(shard) {
                    tracing::warn!(
                        cell_id = %shard.shard.cell_id,
                        error = %error,
                        "shard histograms were not published"
                    );
                }
            }
        }
        Err(_) => tracing::warn!(
            budget_ms = shard_budget.as_millis() as u64,
            "shard metric collection exceeded its budget; last interval's series stay published"
        ),
    }
}

/// The interval loop.
///
/// Both waits select on the stop signal, so a shutdown never has to wait out a
/// slow collection or a full interval — without that, a 60s interval makes a
/// clean shutdown up to 60s slower for a task nobody is waiting on the results
/// of.
#[cfg(feature = "otlp")]
async fn collect_forever(
    histograms: std::sync::Arc<NodeHistograms>,
    query: slatedb_graph_kernel::ClientQueryService,
    node: std::sync::Arc<slatedb_graph_kernel::ScopedRoutedGraphCluster>,
    interval: std::time::Duration,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    // Half the interval, so a collection that runs long can never still be
    // running when the next one is due. The pair is then self-limiting: at worst
    // this task spends half its time collecting and the export sees the previous
    // snapshot, instead of queueing collections behind a wedged shard.
    let shard_budget = interval / 2;
    loop {
        tokio::select! {
            biased;
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    return;
                }
            }
            () = collect_once(&histograms, &query, &node, shard_budget) => {}
        }
        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    return;
                }
            }
            () = tokio::time::sleep(interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use slatedb_graph_kernel::{
        ClientQueryMetricsSnapshot, GraphOperationalMetricsSnapshot, QueryTransportMetricsSnapshot,
        DURATION_BUCKET_BOUNDS_US, DURATION_BUCKET_COUNT,
    };

    use super::*;
    use crate::admin::{prometheus_histogram, PROMETHEUS_HISTOGRAMS};

    /// Every histogram field the kernel enumerates, from every snapshot type
    /// that has one. This is the enumeration both exports are derived from, so
    /// it is also the only list this test may consult.
    fn enumerated_fields() -> Vec<&'static str> {
        let client = ClientQueryMetricsSnapshot::default();
        let operational = GraphOperationalMetricsSnapshot::default();
        let transport = QueryTransportMetricsSnapshot::default();
        client
            .histogram_fields()
            .map(|(field, _)| field)
            .chain(operational.histogram_fields().map(|(field, _)| field))
            .chain(transport.histogram_fields().map(|(field, _)| field))
            .collect()
    }

    /// §1.6: "the two must not disagree about names". A histogram that reaches
    /// one export and not the other is the failure mode that section is most
    /// worried about producing in a year, and this is the cheap test it asks
    /// for — extended to the reverse direction, because a name table row for a
    /// field nobody records is dead weight that reads like a missing recording
    /// site.
    #[test]
    fn every_histogram_field_reaches_both_exports() {
        let enumerated = enumerated_fields();
        assert!(
            !enumerated.is_empty(),
            "the kernel enumerates no histograms at all"
        );

        for field in &enumerated {
            assert!(
                otel_histogram(field).is_some(),
                "{field} is recorded by the kernel but has no OTel name in OTEL_HISTOGRAMS"
            );
            assert!(
                prometheus_histogram(field).is_some(),
                "{field} is recorded by the kernel but has no Prometheus name in PROMETHEUS_HISTOGRAMS"
            );
        }

        for export in OTEL_HISTOGRAMS {
            assert!(
                enumerated.contains(&export.field),
                "OTEL_HISTOGRAMS names {}, which no snapshot enumerates",
                export.field
            );
        }
        for export in PROMETHEUS_HISTOGRAMS {
            assert!(
                enumerated.contains(&export.field),
                "PROMETHEUS_HISTOGRAMS names {}, which no snapshot enumerates",
                export.field
            );
        }
    }

    /// The names may diverge — `/metrics` keeps `graph_*` and OTLP takes
    /// `db.*`/`turbolay.*` — but a bucket bound rendered two different ways is
    /// a single measurement that answers two different questions.
    #[test]
    fn the_two_exports_agree_about_the_unit() {
        for field in enumerated_fields() {
            let otel = otel_histogram(field).expect("named for OTel");
            let prometheus = prometheus_histogram(field).expect("named for Prometheus");
            assert_eq!(
                otel.unit, prometheus.unit,
                "{field} is exported in two different units"
            );
        }
    }

    /// A Prometheus name in seconds whose unit says microseconds — or the
    /// reverse — is a series every dashboard reads off by a factor of a
    /// million, and nothing downstream can detect it.
    #[test]
    fn prometheus_names_carry_their_unit_suffix() {
        for export in PROMETHEUS_HISTOGRAMS {
            let expected = match export.unit {
                ExportUnit::Seconds => "_seconds",
                ExportUnit::Microseconds => "_microseconds",
            };
            assert!(
                export.name.ends_with(expected),
                "{} is exported in {:?} but is not named {expected}",
                export.name,
                export.unit
            );
        }
    }

    /// Two fields sharing an instrument name with nothing to tell them apart is
    /// two populations collapsing into one series — silently, because
    /// `ObservableHistogram` keys its series by the label set and the second
    /// record of each interval simply overwrites the first.
    ///
    /// So the uniqueness this asserts is over the *series identity*, not over
    /// the name: `(name, operation)`. `db.client.operation.duration` is
    /// deliberately two rows; both taking the bare name with `operation: None`
    /// is the failure.
    #[test]
    fn no_two_fields_share_an_exported_series_identity() {
        let otel: Vec<(&str, Option<&str>)> = OTEL_HISTOGRAMS
            .iter()
            .map(|export| (export.name, export.operation))
            .collect();
        let prometheus: Vec<(&str, Option<&str>)> = PROMETHEUS_HISTOGRAMS
            .iter()
            .map(|export| (export.name, None))
            .collect();
        for table in [otel, prometheus] {
            let mut sorted = table.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                table.len(),
                "two fields share a series identity: {table:?}"
            );
        }
    }

    /// The registration invariant, checked where a test can actually run it.
    ///
    /// `NodeHistograms::register` is behind the `otlp` feature, which no `just`
    /// recipe *runs* — `check-all-features` compiles it and nothing executes it.
    /// So the property that keeps it correct is asserted against
    /// [`otel_instrument_groups`], which is the thing it iterates and is not
    /// gated: one group is one instrument, and a group with more than one row is
    /// exactly the case where every row must carry a distinct
    /// `db.operation.name`.
    #[test]
    fn rows_sharing_a_name_are_told_apart_by_the_operation_label() {
        let groups = otel_instrument_groups();
        assert_eq!(
            groups.len(),
            4,
            "read and write share one instrument; the three turbolay.* metrics do not: {groups:#?}"
        );

        for (name, rows) in &groups {
            // A shared instrument has one description and one unit, because
            // those are properties of the instrument and not of the row.
            let first = rows.first().expect("a group has at least one row");
            for row in rows {
                assert_eq!(row.unit, first.unit, "{name} is registered in two units");
                assert_eq!(
                    row.description, first.description,
                    "{name} is registered with two descriptions"
                );
            }

            if rows.len() == 1 {
                continue;
            }
            let mut operations: Vec<&str> = rows
                .iter()
                .map(|row| {
                    row.operation.unwrap_or_else(|| {
                        panic!("{} shares the name {name} but carries no operation label, so its series would merge", row.field)
                    })
                })
                .collect();
            let before = operations.len();
            operations.sort_unstable();
            operations.dedup();
            assert_eq!(
                before,
                operations.len(),
                "{name} has two rows claiming the same operation"
            );
        }

        let client = groups
            .iter()
            .find(|(name, _)| *name == "db.client.operation.duration")
            .expect("the one metric with a stable semantic convention");
        let mut fields: Vec<&str> = client.1.iter().map(|row| row.field).collect();
        fields.sort_unstable();
        assert_eq!(fields, vec!["read_latency", "write_latency"]);
    }

    /// The semconv name is the *whole* name. A suffix would make it a Turbolay
    /// metric wearing a semconv prefix, matching no vendor's database view and
    /// none of the queries that name is worth having for.
    #[test]
    fn the_client_metric_keeps_the_bare_semconv_name() {
        for export in OTEL_HISTOGRAMS {
            if export.field == "read_latency" || export.field == "write_latency" {
                assert_eq!(export.name, "db.client.operation.duration");
            }
        }
    }

    /// The deliberate non-conformance, asserted rather than described: semconv
    /// marks `db.namespace` required-if-applicable on `db.client.*` and it *is*
    /// applicable — the namespace is `turbolay.scope`, the unbounded tenant root
    /// the label registry exists to keep off metrics. Its absence is the design,
    /// and a future row adding it back should fail here rather than in a bill.
    #[test]
    fn the_client_family_carries_no_namespace() {
        assert!(
            !turbolay_telemetry::semconv::METRIC_LABELS
                .iter()
                .any(|label| label.key() == "db.namespace"),
            "db.namespace became a metric label"
        );
        assert!(turbolay_telemetry::semconv::SPAN_ONLY_KEYS
            .contains(&turbolay_telemetry::semconv::SCOPE));
    }

    /// `le` is the join between the two exports. The seconds rendering is the
    /// one that can go wrong — `0.000100000` and `0.0001` are two series.
    #[test]
    fn bounds_render_identically_to_the_meters_rendering() {
        let microseconds: Vec<String> = DURATION_BUCKET_BOUNDS_US
            .iter()
            .map(|bound| ExportUnit::Microseconds.render_bound(*bound))
            .collect();
        assert_eq!(microseconds.first().map(String::as_str), Some("100"));
        assert_eq!(microseconds.last().map(String::as_str), Some("30000000"));

        let seconds: Vec<String> = DURATION_BUCKET_BOUNDS_US
            .iter()
            .map(|bound| ExportUnit::Seconds.render_bound(*bound))
            .collect();
        assert_eq!(seconds.first().map(String::as_str), Some("0.0001"));
        assert_eq!(seconds.last().map(String::as_str), Some("30"));
        assert_eq!(ExportUnit::Seconds.render_sum(2_500_000), "2.5");
        assert_eq!(ExportUnit::Microseconds.render_sum(2_500_000), "2500000");
    }

    /// The ladder is exported once from the kernel precisely so nothing here
    /// restates it. If that ever stops holding, every `le` in both exports is
    /// off by one bucket.
    #[test]
    fn the_ladder_comes_from_the_kernel() {
        assert_eq!(DURATION_BUCKET_BOUNDS_US.len() + 1, DURATION_BUCKET_COUNT);
    }
}
