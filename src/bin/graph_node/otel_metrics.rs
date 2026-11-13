//! The OTel half of the metrics export, and the name tables both halves are
//! driven by.
//!
//! # Two exports, one enumeration
//!
//! The kernel enumerates its metrics by **Rust identifier** — `read_latency`,
//! `query_rows_latency`, `write_attempts` — and knows nothing about either
//! exposition vocabulary
//! (`hydradb::ClientQueryMetricsSnapshot::histogram_fields`,
//! `::counter_fields`, `::class_counter_fields` and their siblings). The two
//! vocabularies live here and in [`crate::admin`]: this module holds the OTel
//! names, `admin.rs` holds the Prometheus names, and
//! [`tests::every_histogram_field_reaches_both_exports`] and
//! [`tests::every_counter_field_reaches_both_exports`] fail the build if a field
//! the kernel enumerates is missing from either. That is the property §1.6 of
//! the OTel metrics and alerting design asks for:
//! adding a metric cannot silently reach one export and not the other.
//!
//! # One counter snapshot reaches the meter; the other two reach `/metrics` only
//!
//! All 69 scalar kernel counters are exported through `/metrics`. Sixty-eight
//! are also registered as OTel instruments:
//! `GraphOperationalMetricsSnapshot`'s scalars less the one that restates a
//! histogram's sum.
//! [`METERED_COUNTER_SOURCES`] is that decision as a value rather than a
//! paragraph — one variant long — and [`otel_counter_instruments`] is what
//! [`NodeCounters::register`] iterates. `ClientQueryMetricsSnapshot` and
//! `GraphCacheMetricsSnapshot` are named there and deliberately not wired; each
//! needs a variant in that list and a `record_*` method beside
//! [`NodeCounters::record_shard_totals`], and nothing else.
//!
//! §1.4 is what makes the asymmetry legitimate rather than an oversight — the
//! two exports are allowed to differ in *dimensionality*, and they already do:
//! `/metrics` carries `hydradb.scope` on three counter families and OTLP
//! carries it nowhere.
//!
//! # Names
//!
//! §1.9: `db.*` where a semantic convention genuinely exists, `hydradb.*`
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
//! exists as `hydradb_telemetry::semconv::L_DB_OPERATION_NAME`, so both rows
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
//! its only value is the unbounded `hydradb.scope`).
//!
//! # Wiring
//!
//! [`NodeHistograms::register`] and [`NodeCounters::register`] take a
//! `hydradb_telemetry::otlp::Providers`, reached through
//! `TelemetryGuard::providers()`. That accessor is the only way in on purpose:
//! `opentelemetry::global::meter` on a provider that was never installed
//! globally returns a **no-op** meter that accepts every instrument, reports
//! nothing, and fails no test — silence indistinguishable from a working
//! pipeline, which is the failure `Providers` exists to make unreachable. It is
//! not even reachable from here, and that is worth more than the discipline:
//! `opentelemetry` is not a dependency of the root package, only of
//! `crates/telemetry`, so naming `global::meter` in this file is an unresolved
//! crate rather than a silent series.
//!
//! [`MetricCollection::start`] is what `graph-node.rs` calls: it registers the
//! instruments once and owns the interval task that snapshots the kernel and
//! publishes into them. Without the `otlp` feature, or with no OTLP endpoint
//! configured, it is inert and starts no task at all.
//!
//! Two of the three pieces the wiring rests on are deliberately outside the
//! `otlp` cfg, because no `just` recipe *executes* this binary's `otlp` code —
//! `check-all-features` compiles it and stops there. [`otel_instrument_groups`]
//! is the histograms' registration invariant and [`shard_counter_totals`] is the
//! counters' arithmetic, and both are plain functions a default-feature test
//! reaches. What is left behind the gate is a loop over what they return.
//!
//! `record_transport` remains unfed, and that is a *structural* fact rather than
//! an unfinished table. [`FieldSource`] is the type that says so,
//! [`TRANSPORT_ONLY_COUNTERS`] is the pinned list, and
//! [`tests::the_transport_counters_are_pinned_as_sourceless`] plus
//! [`tests::only_the_transport_histograms_declare_no_graph_node_source`] are what
//! make it check rather than merely claim. See [`FieldSource::TransportOnly`] for
//! the evidence, and [`CounterSource::field_source`] for the trip-wire that fires
//! the day a source appears.
//!
//! # Why a counter family and not a histogram data point
//!
//! `opentelemetry` 0.32 has no observable histogram and `opentelemetry_sdk`
//! 0.32 has no `MetricProducer`, so a histogram computed in the kernel and read
//! from a cached snapshot cannot reach OTLP as a histogram data point. It
//! reaches it as a family of observable counters keyed by `le`, which is what a
//! Prometheus histogram already is. `hydradb_telemetry::meter` owns that
//! rendering, and owning it in one place is what stops the two exports
//! disagreeing about where a bucket ends. See that module for the full
//! argument.

#[path = "otel_metrics/memory_diagnostics.rs"]
pub(crate) mod memory_diagnostics;
#[path = "otel_metrics/node_runtime.rs"]
pub(crate) mod node_runtime;
#[path = "otel_metrics/planner_statistics.rs"]
pub(crate) mod planner_statistics;
#[path = "otel_metrics/remote_object_store.rs"]
pub(crate) mod remote_object_store;
#[path = "otel_metrics/storage_caches.rs"]
pub(crate) mod storage_caches;

// Only the recording half names a snapshot, and that half is behind `otlp`.
#[cfg(feature = "otlp")]
use hydradb::DurationHistogramSnapshot;

/// The unit an exported histogram's bounds and sum are **rendered** in.
///
/// The kernel measures in microseconds and only ever measures in microseconds.
/// This is a boundary conversion, and it exists because
/// `db.client.operation.duration` is a stable semantic convention fixed in
/// seconds — a series claiming that name in microseconds is worse than a series
/// with a HydraDB name.
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
    /// Character-for-character what `hydradb_telemetry::meter::HistogramUnit`
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
    fn meter_unit(self) -> hydradb_telemetry::meter::HistogramUnit {
        use hydradb_telemetry::meter::HistogramUnit;
        match self {
            Self::Microseconds => HistogramUnit::Microseconds,
            Self::Seconds => HistogramUnit::Seconds,
        }
    }
}

/// Whether `graph-node` holds a live source for a metric field, or whether only
/// some other process does.
///
/// # Why the type exists
///
/// The kernel enumerates four metrics snapshots. `graph-node` obtains three of
/// them and cannot obtain the fourth, and before this type said so the fact lived
/// in three doc comments that a reader had to trust. It is now declared per row
/// and checked against the kernel's own enumeration, so a name table with more
/// rows than there are live sources reads as a deliberate shape rather than as an
/// abandoned migration.
///
/// # The evidence, as of `1d66650`
///
/// It is **not** a feature gap. `graph-node` requires `server-runtime`
/// (`Cargo.toml:117`), which implies `query-service-discovery` and therefore
/// `query-transport` (`Cargo.toml:89-98`), so `TcpQueryServer` and
/// `TcpQueryCellClient` are compiled into the binary. It is an architectural
/// fact: nothing constructs one.
///
/// - `graph-node`'s query fan-in is Bolt (`ClientBoltServer::bind`,
///   `src/bin/graph-node.rs:247`) and HTTP (`ClientHttpServer::bind`, `:260`),
///   both over one `ClientQueryService` whose cell client is
///   `Arc<ScopedRoutedGraphCluster>` — in-process (`:194-196`).
/// - Cross-node work is routed at the **Bolt protocol** layer, by handing the
///   client a routing table (`ObjectStoreBoltRoutingTableProvider`, `:236-241`),
///   so there is no server-to-server query hop for a transport client to make.
/// - `QueryTransportMetrics` is owned only by `TcpQueryCellClient`
///   (`src/query/coordination.rs:1757`) and by `TcpQueryServer` and its runtime
///   (`:1874`, `:1881`, `:1937`). The only non-test construction of either in the
///   tree is `MultiCellQueryCoordinator::from_service_directory`
///   (`src/query/coordination.rs:2799`), which no binary calls — it is a library
///   facility for an embedder that brings its own fan-out.
///
/// So the transport snapshot has no `graph-node` source under any feature set or
/// configuration, and inventing a transport server to have something to enumerate
/// would be building production code to satisfy a name table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldSource {
    /// `graph-node` holds a live snapshot of this field at runtime, so both
    /// exports carry real series for it.
    GraphNode,
    /// Only a process that constructs a `TcpQueryServer` or a
    /// `TcpQueryCellClient` holds this field's source. See the type's docs for
    /// why `graph-node` is not such a process.
    ///
    /// A row marked this way is named and rendered — the field lives in the
    /// kernel and an export must not be the thing that decides which binary is
    /// allowed to have it — but on this binary it renders zeroes from a
    /// default-constructed snapshot in a test and nothing at all in production.
    TransportOnly,
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
    /// [`tests::rows_sharing_a_name_are_told_apart_by_the_operation_and_engine_labels`].
    pub name: &'static str,
    /// Instrument description.
    pub description: &'static str,
    /// Bound and sum unit.
    pub unit: ExportUnit,
    /// The `db.operation.name` value this row is recorded under, if the metric
    /// is one semconv splits by operation rather than by name.
    ///
    /// `None` for every `hydradb.*` metric: those are named for what they
    /// measure, so a second dimension would be a distinction the name already
    /// makes.
    pub operation: Option<&'static str>,
    /// Whether this binary has anything to record into the instrument.
    ///
    /// Declared per row rather than inferred, and cross-checked against the
    /// kernel's enumeration by
    /// [`tests::only_the_transport_histograms_declare_no_graph_node_source`].
    /// This column is the answer to "why does the 28-row table have 26 live
    /// rows".
    pub source: FieldSource,
}

/// The OTel name table. One row per histogram the kernel enumerates.
///
/// Adding a histogram to a snapshot type and not adding it here fails
/// [`tests::every_histogram_field_reaches_both_exports`].
///
/// Twenty-eight rows, twenty-six of them with a `graph-node` source. That is not a table
/// half-finished: see [`FieldSource`], and
/// [`tests::only_the_transport_histograms_declare_no_graph_node_source`] for the
/// assertion that keeps the twenty-six-of-twenty-eight accounting honest.
pub const OTEL_HISTOGRAMS: &[OtelHistogram] = &[
    // Six rows, one instrument: the operation label splits reads from writes
    // and `hydradb.cypher_engine` splits the three engine modes, which the kernel
    // keeps apart at the point of recording. See
    // `crate::admin::cypher_engine_label`.
    OtelHistogram {
        field: "read_latency_legacy",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(hydradb_telemetry::semconv::DB_OPERATION_READ),
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "read_latency_experimental",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(hydradb_telemetry::semconv::DB_OPERATION_READ),
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "read_latency_datafusion",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(hydradb_telemetry::semconv::DB_OPERATION_READ),
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "write_latency_legacy",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(hydradb_telemetry::semconv::DB_OPERATION_WRITE),
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "write_latency_experimental",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(hydradb_telemetry::semconv::DB_OPERATION_WRITE),
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "write_latency_datafusion",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(hydradb_telemetry::semconv::DB_OPERATION_WRITE),
        source: FieldSource::GraphNode,
    },
    // `hydradb.*`, not `db.*`: there is no semantic convention for a
    // causal-consistency wait, and the one stable database histogram
    // (`db.client.operation.duration`) is already claimed above by the two
    // rows that genuinely mean it. Change 4 of
    // the cell-affine read routing design.
    OtelHistogram {
        field: "bookmark_wait_latency",
        name: "hydradb.client.bookmark_wait.duration",
        description: "Causal-consistency bookmark wait before a read is admitted",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "query_rows_latency",
        name: "hydradb.query.rows.duration",
        description: "Shard row-query execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "query_property_fetch_latency",
        name: "hydradb.query.property_fetch.duration",
        description: "Stored-property reads issued while materialising row-query results",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "create_relationships_batch_latency",
        name: "hydradb.write.create_relationships_batch.duration",
        description: "End-to-end create-relationships-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "delete_relationship_mutations_batch_latency",
        name: "hydradb.write.delete_relationship_mutations_batch.duration",
        description: "End-to-end delete-relationship-mutations-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "delete_vertices_and_isolated_candidates_batch_latency",
        name: "hydradb.write.delete_vertices_and_isolated_candidates_batch.duration",
        description: "End-to-end delete-vertices-and-isolated-candidates-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "detach_delete_vertices_batch_latency",
        name: "hydradb.write.detach_delete_vertices_batch.duration",
        description: "End-to-end detach-delete-vertices-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "merge_relationships_batch_latency",
        name: "hydradb.write.merge_relationships_batch.duration",
        description: "End-to-end merge-relationships-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "merge_vertex_metadata_batch_latency",
        name: "hydradb.write.merge_vertex_metadata_batch.duration",
        description: "End-to-end merge-vertex-metadata-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "reserve_edge_delete_noops_batch_latency",
        name: "hydradb.write.reserve_edge_delete_noops_batch.duration",
        description: "End-to-end reserve-edge-delete-noops-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "set_edge_metadata_latency",
        name: "hydradb.write.set_edge_metadata.duration",
        description: "End-to-end set-edge-metadata execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "set_edge_metadata_batch_latency",
        name: "hydradb.write.set_edge_metadata_batch.duration",
        description: "End-to-end set-edge-metadata-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "write_edge_latency",
        name: "hydradb.write.write_edge.duration",
        description: "End-to-end write-edge execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "write_edge_mutations_batch_latency",
        name: "hydradb.write.write_edge_mutations_batch.duration",
        description: "End-to-end write-edge-mutations-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "create_relationship_latency",
        name: "hydradb.write.create_relationship.duration",
        description: "End-to-end create-relationship execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "import_vertex_metadata_batch_latency",
        name: "hydradb.write.import_vertex_metadata_batch.duration",
        description: "End-to-end import-vertex-metadata-batch execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    // `hydradb.query.*`, not `hydradb.shard.query.*`: the shard prefix is
    // already taken by the `...access.<source>.duration.sum` counters, and a
    // histogram sharing their stem would collide with them once a backend
    // translates the counter's `.sum` into a Prometheus suffix.
    OtelHistogram {
        field: "query_experimental_access_statistics_latency",
        name: "hydradb.query.experimental.access.statistics.duration",
        description: "Experimental row-query execution whose access path statistics chose",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "query_experimental_access_probe_latency",
        name: "hydradb.query.experimental.access.probe.duration",
        description: "Experimental row-query execution whose access path the runtime probe chose",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "query_experimental_access_guard_latency",
        name: "hydradb.query.experimental.access.guard.duration",
        description: "Experimental row-query execution in which a statistics-chosen access declined at its guard",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "query_experimental_access_default_latency",
        name: "hydradb.query.experimental.access.default.duration",
        description: "Experimental row-query execution whose access path neither statistics nor the probe chose",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "rpc_latency",
        name: "hydradb.query.transport.rpc.duration",
        description: "Query-transport client RPC round-trip",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::TransportOnly,
    },
    OtelHistogram {
        field: "serve_latency",
        name: "hydradb.query.transport.serve.duration",
        description: "Query-transport server executor time",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::TransportOnly,
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

/// Which snapshot type a counter row belongs to.
///
/// Every counter table is keyed by `(source, field)` rather than by `field`
/// alone, because the kernel's identifiers are **not** unique across the three
/// snapshots: `backpressure_waits` is a field of both
/// `ClientQueryMetricsSnapshot` and `GraphOperationalMetricsSnapshot`, and they
/// count different events at different layers. A flat table keyed by the
/// identifier would silently give one of them the other's name.
///
/// There is deliberately **no variant for `QueryTransportMetricsSnapshot`**,
/// whose twenty-three counters this binary cannot obtain — see [`FieldSource`]
/// for the evidence and [`TRANSPORT_ONLY_COUNTERS`] for the pinned list. Its two
/// *histograms* are named in both tables because `graph-node` renders them from a
/// snapshot handed in by a test, and a name table that decided which binary may
/// have a field would be the wrong thing -- but a counter with no value to render
/// and no name to render it under is better absent than invented.
///
/// **This enum is the trip-wire.** Both counter name tables are keyed by
/// `(source, field)` and every renderer in [`crate::admin`] takes a
/// `CounterSource`, so a transport counter cannot reach either export without a
/// variant here. Adding one does not compile until
/// [`CounterSource::field_source`]'s exhaustive match classifies it, and
/// classifying it as [`FieldSource::TransportOnly`] fails
/// [`tests::every_counter_source_has_a_graph_node_snapshot`] until the tables and
/// a renderer exist. That is what stops the series staying quietly missing on the
/// day a source appears.
///
/// `Hash` because [`NodeCounters`] keys its registered instruments by
/// `(source, field)`; see that type for why the pair and not the field alone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CounterSource {
    /// `ClientQueryMetricsSnapshot`. One per node, not per shard.
    Client,
    /// `GraphOperationalMetricsSnapshot`. One per shard.
    Shard,
    /// `GraphCacheMetricsSnapshot`. One per shard.
    ShardCache,
}

impl CounterSource {
    /// Every variant, so the tests that must be total over them can be.
    ///
    /// Kept in step with the enum by
    /// [`tests::every_counter_source_has_a_graph_node_snapshot`], which asserts
    /// the length alongside the classification — the one place a variant added
    /// without being listed here would otherwise slip through.
    pub const ALL: &'static [Self] = &[Self::Client, Self::Shard, Self::ShardCache];

    /// Where `graph-node` obtains a snapshot of this source, or that it cannot.
    ///
    /// The match is exhaustive on purpose. A variant added to
    /// [`CounterSource`] stops this compiling until its author decides which
    /// answer is true, and the accompanying comment is where the runtime
    /// call site goes.
    pub const fn field_source(self) -> FieldSource {
        match self {
            // `ClientQueryService::metrics()`, held by `graph-node.rs:194` and
            // read by `admin::metrics` at `admin.rs:648`.
            Self::Client => FieldSource::GraphNode,
            // `ScopedRoutedGraphCluster::local_shard_runtime_metrics()`, read by
            // `admin::metrics` at `admin.rs:668`. Both snapshots arrive on the
            // same `GraphShardRuntimeMetrics`.
            Self::Shard | Self::ShardCache => FieldSource::GraphNode,
        }
    }
}

/// Every counter on `QueryTransportMetricsSnapshot`, which `graph-node` has no
/// source for.
///
/// Written out rather than derived so the *absence* is a thing in the source
/// tree: a reader who wonders why sixty-five counters are exported when the kernel
/// declares eighty-eight finds the missing twenty-three named here, with
/// [`FieldSource`] above explaining why. Pinned against the kernel's own
/// enumeration by [`tests::the_transport_counters_are_pinned_as_sourceless`], so a
/// counter added to that snapshot fails the build rather than joining a silent
/// gap.
///
/// Note the three identifiers that also name a *different* measurement one layer
/// up — `auth_failures`, `cancellations` and `backpressure_waits` are also fields
/// of `ClientQueryMetricsSnapshot` (and `backpressure_waits` of
/// `GraphOperationalMetricsSnapshot` besides), and those rows are exported. That
/// collision is
/// why both counter tables are keyed by `(source, field)` and never by `field`
/// alone, and the pin test asserts it rather than leaving it to be rediscovered.
pub const TRANSPORT_ONLY_COUNTERS: &[&str] = &[
    "requests_started",
    "requests_completed",
    "requests_failed",
    "auth_failures",
    "namespace_access_denials",
    "namespace_quota_waits",
    "cancellations",
    "cancelled_rejections",
    "slow_queries",
    "backpressure_waits",
    "client_retries",
    "bytes_sent",
    "bytes_received",
    "remote_latency_us",
    "connections_accepted",
    "connections_active",
    "connections_rejected",
    "connections_created",
    "connections_reused",
    "client_connection_waits",
    "handshake_failures",
    "idle_timeouts",
    "forced_shutdowns",
];

/// How one counter reaches OTLP -- or why it does not reach it on its own.
///
/// The dimensionality here is deliberately **not** the Prometheus one. §1.4 of
/// the metrics plan says the two exports have different cost functions and
/// should not be forced to the same shape: `/metrics` is scraped by a Prometheus
/// already sized for the tenant count it sees and carries `hydradb.scope` on
/// three counter families for historical reasons, while OTLP ships to a vendor
/// billing per series. No variant here carries a scope, and there is no way to
/// spell one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtelCounterExport {
    /// One process-global sum, no attributes beyond the resource.
    Global(&'static str),
    /// One sum per `hydradb.cell_id`, summed over every scope open on the node.
    PerCell(&'static str),
    /// Not an instrument of its own. The counter is a restatement of the `.sum`
    /// of the named histogram instruments, which are already exported, so a
    /// second instrument would be a second name for one number.
    Derived(&'static [&'static str]),
}

impl OtelCounterExport {
    /// The instrument name, or `None` for a derived counter.
    pub fn name(self) -> Option<&'static str> {
        match self {
            Self::Global(name) | Self::PerCell(name) => Some(name),
            Self::Derived(_) => None,
        }
    }
}

/// What an exported counter counts, which is what fixes its unit.
///
/// Derived from the kernel's field identifier rather than declared per row, and
/// the derivation is the `_us` suffix: every cumulative microsecond total the
/// kernel keeps is named for it — `gc_duration_us`, `bulk_import_commit_us`,
/// `graph_compute_queue_us` — and nothing else is. A column would be seventy-six
/// values to distinguish two cases, and it would be free to drift from the names
/// the *other* export already derives from the same convention: every one of
/// these rows is spelled `_microseconds` in [`crate::admin::PROMETHEUS_COUNTERS`].
/// [`tests::the_microsecond_counters_are_named_for_their_unit`] holds the
/// derivation and both name tables together in all three directions, which is
/// what a column could not do.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterQuantity {
    /// A count of events. UCUM `1`.
    Events,
    /// A cumulative sum of microseconds — the counter an operator divides by the
    /// matching event count to get a mean. UCUM `us`.
    Microseconds,
}

impl CounterQuantity {
    /// The quantity a kernel counter field measures.
    pub fn of_field(field: &str) -> Self {
        if field.ends_with("_us") {
            Self::Microseconds
        } else {
            Self::Events
        }
    }

    /// The same choice, spelled the way the meter spells it.
    ///
    /// There is no `Seconds` to map onto and that is deliberate on the meter's
    /// side, not an omission here: scaling a counter at this boundary would make
    /// it disagree by a factor of a million with the `_microseconds` series
    /// `/metrics` renders from the very same field, undetectably.
    #[cfg(feature = "otlp")]
    fn meter_unit(self) -> hydradb_telemetry::meter::CounterUnit {
        use hydradb_telemetry::meter::CounterUnit;
        match self {
            Self::Events => CounterUnit::Count,
            Self::Microseconds => CounterUnit::Microseconds,
        }
    }
}

/// One row of the OTel counter name table.
///
/// There is no `description` field, and its absence is a decision rather than an
/// omission. Registration needs one — [`NodeCounters::register`] passes
/// [`OtelCounter::field`] — and the kernel identifier is the better string than a
/// sentence would be: seventy-six generated sentences would each restate the
/// metric name in longer words, whereas the identifier is the one thing the name
/// does *not* carry, and it is the key `/metrics`, the kernel's enumeration and
/// both name tables are joined on. An operator who finds
/// `hydradb.shard.compute.queue.duration.sum` on a dashboard and wants the code
/// gets `graph_compute_queue_us` for free.
#[derive(Clone, Copy, Debug)]
pub struct OtelCounter {
    pub source: CounterSource,
    /// The kernel's Rust identifier -- the key both name tables are keyed by.
    pub field: &'static str,
    pub export: OtelCounterExport,
}

/// The OTel counter name table. One row per counter the kernel enumerates.
///
/// **Not every row is a registered instrument.** [`METERED_COUNTER_SOURCES`] says
/// which are, [`otel_counter_instruments`] applies it, and the two sources it
/// leaves out reach `/metrics` alone for now. A row is in this table whether or
/// not it is on the meter, because that is what makes
/// [`tests::every_counter_field_reaches_both_exports`] total: a counter added to
/// the kernel today cannot reach one export and quietly miss the other on the day
/// the remaining instruments land.
///
/// `hydradb.*` throughout. §1.9 says `db.*` where a semantic convention
/// genuinely exists, and there is no semconv counter for any of these -- the one
/// stable database metric, `db.client.operation.duration`, is a histogram and is
/// already claimed by [`OTEL_HISTOGRAMS`].
pub const OTEL_COUNTERS: &[OtelCounter] = &[
    // `ClientQueryMetricsSnapshot`, in declaration order.
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_started",
        export: OtelCounterExport::Global("hydradb.client.queries.started"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_completed",
        export: OtelCounterExport::Global("hydradb.client.queries.completed"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_failed",
        export: OtelCounterExport::Global("hydradb.client.queries.failed"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "rows_returned",
        export: OtelCounterExport::Global("hydradb.client.rows.returned"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "auth_failures",
        export: OtelCounterExport::Global("hydradb.client.auth.failures"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "scope_denials",
        export: OtelCounterExport::Global("hydradb.client.scope.denials"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "cancellations",
        export: OtelCounterExport::Global("hydradb.client.queries.cancelled"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "backpressure_waits",
        export: OtelCounterExport::Global("hydradb.client.backpressure.waits"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "prepare_requests",
        export: OtelCounterExport::Global("hydradb.client.prepare.requests"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "prepare_duration_us",
        export: OtelCounterExport::Global("hydradb.client.prepare.duration.sum"),
    },
    // The bookmark-wait family. `Global` for the same reason its Prometheus
    // twin is: the wait is a property of the node the read landed on, and a
    // per-cell split would divide the one ratio worth alerting on.
    OtelCounter {
        source: CounterSource::Client,
        field: "bookmark_waits",
        export: OtelCounterExport::Global("hydradb.client.bookmark_wait.requests"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_polled",
        export: OtelCounterExport::Global("hydradb.client.bookmark_wait.polled"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_declined",
        export: OtelCounterExport::Global("hydradb.client.bookmark_wait.declined"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_on_cell_writer",
        export: OtelCounterExport::Global("hydradb.client.bookmark_wait.on_cell_writer"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_off_cell_writer",
        export: OtelCounterExport::Global("hydradb.client.bookmark_wait.off_cell_writer"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "admission_wait_us",
        export: OtelCounterExport::Global("hydradb.client.admission_wait.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "serialize_duration_us",
        export: OtelCounterExport::Global("hydradb.client.serialize.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "serialized_rows",
        export: OtelCounterExport::Global("hydradb.client.serialized_rows"),
    },
    // Derived: the kernel builds it from `read_latency.sum_us +
    // write_latency.sum_us`, and both instruments already publish a `.sum`.
    OtelCounter {
        source: CounterSource::Client,
        field: "execution_duration_us",
        export: OtelCounterExport::Derived(&["db.client.operation.duration"]),
    },
    // `GraphOperationalMetricsSnapshot`, in declaration order.
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_attempts",
        export: OtelCounterExport::PerCell("hydradb.shard.write.attempts"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_commits",
        export: OtelCounterExport::PerCell("hydradb.shard.write.commits"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_retries",
        export: OtelCounterExport::PerCell("hydradb.shard.write.retries"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_batches_profiled",
        export: OtelCounterExport::PerCell("hydradb.shard.bulk_import.batches_profiled"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_preflight_us",
        export: OtelCounterExport::PerCell("hydradb.shard.bulk_import.preflight.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_batch_build_us",
        export: OtelCounterExport::PerCell("hydradb.shard.bulk_import.batch_build.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_counter_read_us",
        export: OtelCounterExport::PerCell("hydradb.shard.bulk_import.counter_read.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_commit_us",
        export: OtelCounterExport::PerCell("hydradb.shard.bulk_import.commit.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_batches_profiled",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.batches_profiled"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_endpoint_check_us",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.endpoint_check.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_scan_us",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.identity_scan.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_pointer_hits",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.identity_pointer.hits"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_pointer_misses",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.identity_pointer.misses"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_record_read_us",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.record_read.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_structural_check_us",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.structural_check.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_segment_scans",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.segment_scans"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_segment_neighbors",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.segment_neighbors"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_counter_read_us",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.counter_read.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_commit_us",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.commit.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "relationship_import_idempotency_replays",
        export: OtelCounterExport::PerCell("hydradb.shard.relationship_import.idempotency_replays"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_nochange_exits",
        export: OtelCounterExport::PerCell("hydradb.shard.merge_vertex_metadata.nochange_exits"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_all_replays",
        export: OtelCounterExport::PerCell("hydradb.shard.delete_vertex_batch.all_replays"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_batches_profiled",
        export: OtelCounterExport::PerCell("hydradb.shard.merge_vertex_metadata.batches_profiled"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_batch_items",
        export: OtelCounterExport::PerCell("hydradb.shard.merge_vertex_metadata.batch_items"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_read_us",
        export: OtelCounterExport::PerCell("hydradb.shard.merge_vertex_metadata.read.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_txn_us",
        export: OtelCounterExport::PerCell("hydradb.shard.merge_vertex_metadata.txn.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_batches_profiled",
        export: OtelCounterExport::PerCell("hydradb.shard.delete_vertex_batch.batches_profiled"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_items",
        export: OtelCounterExport::PerCell("hydradb.shard.delete_vertex_batch.batch_items"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_read_us",
        export: OtelCounterExport::PerCell("hydradb.shard.delete_vertex_batch.read.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_txn_us",
        export: OtelCounterExport::PerCell("hydradb.shard.delete_vertex_batch.txn.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_batches_profiled",
        export: OtelCounterExport::PerCell("hydradb.shard.reserve_edge_delete_noops.batches_profiled"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_batch_items",
        export: OtelCounterExport::PerCell("hydradb.shard.reserve_edge_delete_noops.batch_items"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_read_us",
        export: OtelCounterExport::PerCell("hydradb.shard.reserve_edge_delete_noops.read.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_txn_us",
        export: OtelCounterExport::PerCell("hydradb.shard.reserve_edge_delete_noops.txn.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_builds_started",
        export: OtelCounterExport::PerCell("hydradb.shard.artifact.builds.started"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_builds_completed",
        export: OtelCounterExport::PerCell("hydradb.shard.artifact.builds.completed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_build_duration_us",
        export: OtelCounterExport::PerCell("hydradb.shard.artifact.build.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_publish_batches",
        export: OtelCounterExport::PerCell("hydradb.shard.artifact.publish.batches"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_records_published",
        export: OtelCounterExport::PerCell("hydradb.shard.artifact.records.published"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_publish_duration_us",
        export: OtelCounterExport::PerCell("hydradb.shard.artifact.publish.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_jobs_started",
        export: OtelCounterExport::PerCell("hydradb.shard.gc.jobs.started"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_jobs_completed",
        export: OtelCounterExport::PerCell("hydradb.shard.gc.jobs.completed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_keys_deleted",
        export: OtelCounterExport::PerCell("hydradb.shard.gc.keys.deleted"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_duration_us",
        export: OtelCounterExport::PerCell("hydradb.shard.gc.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "verifier_runs",
        export: OtelCounterExport::PerCell("hydradb.shard.verifier.runs"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "verifier_failures",
        export: OtelCounterExport::PerCell("hydradb.shard.verifier.failures"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "verifier_duration_us",
        export: OtelCounterExport::PerCell("hydradb.shard.verifier.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_started",
        export: OtelCounterExport::PerCell("hydradb.shard.query.rows.started"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_completed",
        export: OtelCounterExport::PerCell("hydradb.shard.query.rows.completed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_failed",
        export: OtelCounterExport::PerCell("hydradb.shard.query.rows.failed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_returned",
        export: OtelCounterExport::PerCell("hydradb.shard.query.rows.returned"),
    },
    // Derived: the kernel sets it from `query_rows_latency.sum_us`.
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_duration_us",
        export: OtelCounterExport::Derived(&["hydradb.query.rows.duration"]),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_property_seek_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.property_seek.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_relationship_expand_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.relationship_expand.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_ordered_property_scan_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.ordered_property_scan.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_parse_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.parse.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_lower_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.lower.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_bind_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.bind.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_snapshot_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.snapshot.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_statistics_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.statistics.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_plan_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.plan.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_execute_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.execute.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_storage_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.storage.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_storage_calls",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.storage.calls"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_result_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.result.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_sampled_plans",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.sampled_plans"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_statistics_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.statistics.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_statistics_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.statistics.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_probe_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.probe.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_probe_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.probe.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_guard_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.guard.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_guard_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.guard.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_default_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.default.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_default_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.default.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_guard_stops",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.guard_stops"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_planned_without_statistics",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.planned_without_statistics"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_reloads",
        export: OtelCounterExport::PerCell("hydradb.shard.planner_statistics.reloads"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_load_failures",
        export: OtelCounterExport::PerCell("hydradb.shard.planner_statistics.load_failures"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_corrupt_ignored",
        export: OtelCounterExport::PerCell("hydradb.shard.planner_statistics.corrupt_ignored"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_hints_sent",
        export: OtelCounterExport::PerCell("hydradb.shard.planner_statistics.hints_sent"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_rows_estimated",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.rows_estimated"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_rows_actual",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.rows_actual"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_underestimates",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.underestimates"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_overestimates",
        export: OtelCounterExport::PerCell("hydradb.shard.query.experimental.access.overestimates"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_route_legacy_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.route.legacy.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_route_experimental_requests",
        export: OtelCounterExport::PerCell("hydradb.shard.query.route.experimental.requests"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_route_native_path_fallbacks",
        export: OtelCounterExport::PerCell("hydradb.shard.query.route.native_path.fallbacks"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_route_mutation_fallbacks",
        export: OtelCounterExport::PerCell("hydradb.shard.query.route.mutation.fallbacks"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_property_fetches",
        export: OtelCounterExport::PerCell("hydradb.shard.query.property_fetches"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_artifact_lookup_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.artifact_lookup.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_cache_us",
        export: OtelCounterExport::PerCell("hydradb.shard.query.graphblas_cache.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_artifact_snapshots",
        export: OtelCounterExport::PerCell("hydradb.shard.query.graphblas.artifact_snapshots"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_rebuilt_snapshots",
        export: OtelCounterExport::PerCell("hydradb.shard.query.graphblas.rebuilt_snapshots"),
    },
    // The plan-shape family. `hydradb.shard.query.plans` is the denominator the
    // other four are read against; one plan can contribute to more than one of
    // them, so they do not sum to it.
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_plans_total",
        export: OtelCounterExport::PerCell("hydradb.shard.query.plans"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_label_scan",
        export: OtelCounterExport::PerCell("hydradb.shard.query.plans.label_scan"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_property_index",
        export: OtelCounterExport::PerCell("hydradb.shard.query.plans.property_index"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_full_scan",
        export: OtelCounterExport::PerCell("hydradb.shard.query.plans.full_scan"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_equality_pushdown",
        export: OtelCounterExport::PerCell("hydradb.shard.query.plans.equality_pushdown"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rust_sparse_fallbacks",
        export: OtelCounterExport::PerCell("hydradb.shard.query.rust_sparse_fallbacks"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "graph_compute_tasks",
        export: OtelCounterExport::PerCell("hydradb.shard.compute.tasks"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "graph_compute_queue_us",
        export: OtelCounterExport::PerCell("hydradb.shard.compute.queue.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "graph_compute_duration_us",
        export: OtelCounterExport::PerCell("hydradb.shard.compute.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gds_assemblies",
        export: OtelCounterExport::PerCell("hydradb.shard.gds.assemblies"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "backpressure_waits",
        export: OtelCounterExport::PerCell("hydradb.shard.backpressure.waits"),
    },
    // The graph write permit: acquisitions, queue wait and hold, the
    // last two as `.duration.sum` like the compute pair above.
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_permit_acquisitions",
        export: OtelCounterExport::PerCell("hydradb.shard.write_permit.acquisitions"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_permit_wait_us",
        export: OtelCounterExport::PerCell("hydradb.shard.write_permit.wait.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_permit_held_us",
        export: OtelCounterExport::PerCell("hydradb.shard.write_permit.held.duration.sum"),
    },
    // `GraphCacheMetricsSnapshot`, in declaration order.
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_artifact_hits",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.matrix_artifact.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_artifact_misses",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.matrix_artifact.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_adjacency_hits",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.matrix_adjacency.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_adjacency_misses",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.matrix_adjacency.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "graphblas_hits",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.graphblas.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "graphblas_misses",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.graphblas.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "parsed_row_query_hits",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.parsed_row_query.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "parsed_row_query_misses",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.parsed_row_query.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_rows_hits",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.relationship_rows.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_rows_misses",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.relationship_rows.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_property_rows_hits",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.relationship_property_rows.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_property_rows_misses",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.relationship_property_rows.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "insertions",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.insertions"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "evictions",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.evictions"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "pinned_insertions",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.pinned_insertions"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "tenant_quota_rejections",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.tenant_quota_rejections"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "hydration_started",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.hydration.started"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "hydration_waited",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.hydration.waited"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "hydration_completed",
        export: OtelCounterExport::PerCell("hydradb.shard.cache.hydration.completed"),
    },
];

/// The OTel names for the counters dimensioned by `error.class`.
///
/// A separate table because the series carry an extra attribute the scalar rows
/// do not, and folding a `by_class` flag into [`OtelCounter`] would put a
/// boolean on sixty-five rows to distinguish two.
///
/// The name is *not* the scalar's. `hydradb.client.queries.failed` with an
/// `error.class` attribute would be the tidier modelling, but the scalar is also
/// exported and the two would then be one instrument reporting a total and a
/// breakdown under the same name -- the kind of double count a dashboard sums
/// without noticing.
pub const OTEL_CLASS_COUNTERS: &[OtelCounter] = &[
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_class",
        export: OtelCounterExport::Global("hydradb.client.queries.failed.by_class"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_failed_by_class",
        export: OtelCounterExport::PerCell("hydradb.shard.query.rows.failed.by_class"),
    },
];

/// The OTel names for the counters dimensioned by stage, error class and
/// failure reason. Named, like every client row, so the two exports cannot
/// disagree about the family once `CounterSource::Client` is recorded.
pub const OTEL_FAILURE_COUNTERS: &[OtelCounter] = &[
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_reason_legacy",
        export: OtelCounterExport::Global("hydradb.client.queries.failed.by_reason"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_reason_experimental",
        export: OtelCounterExport::Global("hydradb.client.queries.failed.by_reason"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_reason_datafusion",
        export: OtelCounterExport::Global("hydradb.client.queries.failed.by_reason"),
    },
];

/// The OTel row for a `(source, field)` pair, over every counter table.
pub fn otel_counter(source: CounterSource, field: &str) -> Option<&'static OtelCounter> {
    OTEL_COUNTERS
        .iter()
        .chain(OTEL_CLASS_COUNTERS)
        .chain(OTEL_FAILURE_COUNTERS)
        .find(|export| export.source == source && export.field == field)
}

/// Which counter snapshots this binary registers instruments for and feeds.
///
/// One variant, and the whole of the remaining wiring is this list. Adding
/// [`CounterSource::Client`] registers eleven more instruments and needs a
/// `record_client_counters` beside [`NodeCounters::record_shard_totals`] fed from
/// `ClientQueryService::metrics()`, which `collect_once` already holds; adding
/// [`CounterSource::ShardCache`] registers nineteen and must read
/// `node.graph_cache_metrics()` so scope closure cannot reduce a counter. Neither is a
/// design question any more, which is exactly why the stopping point is a value
/// here rather than a sentence somewhere.
///
/// `/metrics` is the decided consumer of these numbers, so the list is short by
/// intent rather than by inattention: the point of the shard family being on the
/// meter is that the pattern exists and is proven end to end, not that every
/// counter is on the wire.
pub const METERED_COUNTER_SOURCES: &[CounterSource] = &[CounterSource::Shard];

/// The [`OTEL_COUNTERS`] rows that become registered instruments.
///
/// **One row is one instrument**, which is the opposite of
/// [`otel_instrument_groups`] and is why no grouping happens here: counter names
/// are unique across both tables, and
/// [`tests::no_two_metrics_share_an_exported_name`] is what keeps that true. Two
/// counter rows sharing a name would be the same silent collapse the histograms'
/// grouping exists to prevent, with nothing to tell the series apart.
///
/// Three filters, all of them load-bearing:
///
/// - [`METERED_COUNTER_SOURCES`], so a source with no recording path registers no
///   instrument. An instrument nothing records into exports nothing at all
///   (`crates/telemetry/tests/meter_export.rs::an_unrecorded_counter_exports_nothing`),
///   so registering the other two sources now would be harmless on the wire and
///   dishonest in the code — a callback the collection task never reaches.
/// - [`OtelCounterExport::name`], which is `None` exactly for the derived rows.
///   Those restate the `.sum` of a histogram family that is already exported, so a
///   second instrument would be a second name for one number.
/// - [`OTEL_CLASS_COUNTERS`] is not consulted. A per-class instrument is
///   `cell_id × error.class`, ten series per cell rather than one, and choosing
///   that multiplier for a vendor billing per series is a §1.4 dimensionality
///   decision this round did not take.
///   [`tests::only_the_shard_scalars_are_registered_this_round`] pins the absence
///   so it stays a decision.
pub fn otel_counter_instruments() -> Vec<&'static OtelCounter> {
    OTEL_COUNTERS
        .iter()
        .filter(|row| METERED_COUNTER_SOURCES.contains(&row.source))
        .filter(|row| row.export.name().is_some())
        .collect()
}

/// Every cell's operational counters, flattened into the rows the recording
/// loop publishes.
///
/// [`OtelCounterExport::PerCell`] reads "one sum per `hydradb.cell_id`", and
/// the sum is the runtime's
/// (`ScopedRoutedGraphCluster::cell_operational_counter_totals`): every scope
/// open on the node plus the final values of every scope that has closed.
/// Summing only the open scopes, as this once did, makes the series fall when a
/// scope closes, and a cumulative OTLP counter that falls is read downstream as
/// a reset — the whole remaining sum is then counted again as new increase.
/// Publishing per scope instead would not duplicate and would not
/// warn: the series identity is the label set, so the last scope recorded in an
/// interval would simply *replace* the others. The same totals feed
/// `admin::append_per_cell_counters`.
///
/// Outside the `otlp` cfg on purpose. This is the half of
/// [`NodeCounters::record_shard_totals`] that a name table cannot check and that no
/// `just` recipe would otherwise execute, so it lives where
/// [`tests::two_scopes_on_one_cell_sum_rather_than_overwrite`] reaches it under
/// default features, and the gated half is reduced to a loop over what it returns.
///
/// Ordered by `cell_id`, then by the kernel's declaration order within a cell.
/// Nothing downstream depends on either — a series is addressed by its labels —
/// but a deterministic order is what lets a test assert on the whole return value
/// instead of searching it.
pub fn shard_counter_totals(
    totals: &std::collections::BTreeMap<String, hydradb::CellOperationalCounterTotals>,
) -> Vec<(&str, &[(&'static str, u64)])> {
    totals
        .iter()
        .map(|(cell_id, cell)| (cell_id.as_str(), cell.counters.as_slice()))
        .collect()
}

/// The instrumentation scope every instrument registered here belongs to.
#[cfg(feature = "otlp")]
const METER_NAME: &str = "hydradb.graph_node";

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
    registered:
        std::collections::HashMap<&'static str, (std::sync::Arc<hydradb_telemetry::meter::ObservableHistogram>, Option<&'static str>)>,
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
    /// ([`hydradb::DURATION_BUCKET_BOUNDS_US`]) rather than being
    /// restated here, so the Prometheus rendering in [`crate::admin`] and this
    /// one cannot disagree about where a bucket ends.
    pub fn register(providers: &hydradb_telemetry::otlp::Providers) -> Result<Self, hydradb_telemetry::meter::HistogramError> {
        use hydradb_telemetry::meter::{HistogramSpec, ObservableHistogram};

        let meter = providers.meter(METER_NAME);
        let mut registered = std::collections::HashMap::with_capacity(OTEL_HISTOGRAMS.len());
        for (name, rows) in otel_instrument_groups() {
            // Every row in a group shares the instrument, so its unit and
            // description have to be the group's, not the row's. A group whose
            // rows disagree about either is caught by
            // `rows_sharing_a_name_are_told_apart_by_the_operation_and_engine_labels`.
            let first = rows.first().expect("a group has at least one row");
            let histogram = std::sync::Arc::new(ObservableHistogram::register(
                &meter,
                HistogramSpec {
                    name,
                    description: first.description,
                    unit: first.unit.meter_unit(),
                },
                &hydradb::DURATION_BUCKET_BOUNDS_US,
            )?);
            for row in rows {
                registered.insert(row.field, (std::sync::Arc::clone(&histogram), row.operation));
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
        labels: &[(hydradb_telemetry::semconv::MetricLabel, &str)],
        snapshot: &DurationHistogramSnapshot,
    ) -> Result<(), hydradb_telemetry::meter::HistogramError> {
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
            labels.push((hydradb_telemetry::semconv::L_DB_OPERATION_NAME, *operation));
        }
        histogram.record_snapshot(&labels, &snapshot.bucket_counts, snapshot.sum_us)
    }

    /// The process-global client query histograms.
    ///
    /// Carries `db.system.name` because that is the attribute a vendor's
    /// database view keys on, and putting `db.*` names on the wire while
    /// omitting it would pay the cost of §1.9's vocabulary split and collect
    /// none of the benefit. `db.operation.name` is added by [`Self::record`]
    /// from the name table, which is what makes the read and write fields two
    /// series of the one semconv instrument.
    ///
    /// It carries no `scope`, and therefore no `db.namespace`: that is §1.4's
    /// whole point, and the label registry makes it a type error rather than a
    /// review comment.
    ///
    /// `hydradb.cypher_engine` is the engine that ran the statements in the
    /// field being recorded, and comes from the field rather than from the
    /// service: a node the kill switch has flipped holds multiple populations,
    /// and labelling them by the node's current engine would move a whole
    /// history between engines at the next collection. What it buys is the
    /// ability to overlay engine modes on one latency panel and read the gap
    /// without a join on the instance.
    pub fn record_client(&self, snapshot: &hydradb::ClientQueryMetricsSnapshot) -> Result<(), hydradb_telemetry::meter::HistogramError> {
        use hydradb_telemetry::semconv::{DB_SYSTEM_NEO4J, L_CYPHER_ENGINE, L_DB_SYSTEM_NAME};

        let system = (L_DB_SYSTEM_NAME, DB_SYSTEM_NEO4J);
        for (field, histogram) in snapshot.histogram_fields() {
            // Same split as `/metrics`: the execution families carry the
            // engine, the bookmark-wait family does not.
            match crate::admin::cypher_engine_label(field) {
                Some(engine) => self.record(field, &[system, (L_CYPHER_ENGINE, engine)], histogram)?,
                None => self.record(field, &[system], histogram)?,
            }
        }
        Ok(())
    }

    /// One shard's operational histograms, labelled by `cell_id` alone.
    ///
    /// Never `cell_id × edge_type`: a 21-bucket family times 96 is 2,016
    /// series per instrument per node, which is where §1.3's cardinality
    /// arithmetic stops being affordable. And never `scope`, which `/metrics`
    /// does carry — that divergence is deliberate and is the reason both
    /// exports exist.
    pub fn record_shard(&self, metrics: &hydradb::ScopedGraphShardRuntimeMetrics) -> Result<(), hydradb_telemetry::meter::HistogramError> {
        use hydradb_telemetry::semconv::L_CELL_ID;

        for (field, histogram) in metrics.shard.operational.histogram_fields() {
            self.record(field, &[(L_CELL_ID, metrics.shard.cell_id.as_str())], histogram)?;
        }
        Ok(())
    }

    /// The query-transport histograms.
    ///
    /// Unlabelled: only one of `rpc_latency` and `serve_latency` is ever
    /// non-empty on a given instance, so the instrument name already says which
    /// side of the wire it was measured on.
    ///
    /// **No production caller, by construction** — both rows are
    /// [`FieldSource::TransportOnly`]. It exists so the two instruments have a
    /// recording path the day a process that *does* hold a transport snapshot
    /// registers them, and so `collect_once` does not have to grow a branch for a
    /// snapshot it will never see.
    pub fn record_transport(
        &self,
        snapshot: &hydradb::QueryTransportMetricsSnapshot,
    ) -> Result<(), hydradb_telemetry::meter::HistogramError> {
        for (field, histogram) in snapshot.histogram_fields() {
            self.record(field, &[], histogram)?;
        }
        Ok(())
    }
}

/// Every registered counter instrument, keyed by `(source, field)`.
///
/// Keyed by the pair and never by the field alone, for the reason
/// [`CounterSource`] gives: `backpressure_waits` is a field of two different
/// snapshots counting two different events, so a map keyed by the identifier would
/// hand one of them the other's instrument. Only one source is registered today
/// and the collision is therefore latent rather than live — which is precisely
/// when a key is cheap to get right.
///
/// The values are owned rather than `Arc`ed, and that is the difference from
/// [`NodeHistograms`]: two histogram rows share `db.client.operation.duration` and
/// are told apart by a label, whereas one counter row is one instrument name.
#[cfg(feature = "otlp")]
#[derive(Debug)]
pub struct NodeCounters {
    registered: std::collections::HashMap<(CounterSource, &'static str), hydradb_telemetry::meter::ObservableCounter>,
}

#[cfg(feature = "otlp")]
impl NodeCounters {
    /// Register one observable counter per [`otel_counter_instruments`] row
    /// against the process's meter.
    ///
    /// Infallible, and the signature is honest rather than symmetrical with
    /// [`NodeHistograms::register`]: a counter has no ladder to validate, and
    /// `ObservableCounter::register` returns no `Result` because the SDK validates
    /// instrument names internally and silently drops what it rejects. A name
    /// surviving is therefore an export-level claim, not a return value, which is
    /// what `otlp_export_tests::the_shard_counters_reach_an_otlp_collector_summed_by_cell`
    /// is for.
    ///
    /// `providers` is the meter from `TelemetryGuard::providers()` and there is no
    /// other way in — see the module note: `global::meter` would register every
    /// instrument against a no-op meter and report nothing, and the root package's
    /// manifest is what makes it unreachable rather than merely discouraged.
    pub fn register(providers: &hydradb_telemetry::otlp::Providers) -> Self {
        use hydradb_telemetry::meter::{CounterSpec, ObservableCounter};

        let meter = providers.meter(METER_NAME);
        let instruments = otel_counter_instruments();
        let mut registered = std::collections::HashMap::with_capacity(instruments.len());
        for row in instruments {
            let Some(name) = row.export.name() else {
                debug_assert!(false, "otel_counter_instruments filters the derived rows");
                continue;
            };
            registered.insert(
                (row.source, row.field),
                ObservableCounter::register(
                    &meter,
                    CounterSpec {
                        name,
                        description: row.field,
                        unit: CounterQuantity::of_field(row.field).meter_unit(),
                    },
                ),
            );
        }
        Self { registered }
    }

    /// Publish one counter's cumulative total for one series.
    ///
    /// A field with no registered instrument is skipped, and the `debug_assert`
    /// is what separates the two ways that happens: a *derived* row has no
    /// instrument by design, and anything else is a hole between the kernel's
    /// enumeration and [`otel_counter_instruments`]. Skipped rather than raised
    /// for [`NodeHistograms::record`]'s reason — losing one series is a better
    /// outcome on a metrics thread than losing the interval.
    fn record(
        &self,
        source: CounterSource,
        field: &'static str,
        labels: &[(hydradb_telemetry::semconv::MetricLabel, &str)],
        value: u64,
    ) -> Result<(), hydradb_telemetry::meter::CounterError> {
        let Some(counter) = self.registered.get(&(source, field)) else {
            debug_assert!(
                matches!(
                    otel_counter(source, field).map(|row| row.export),
                    Some(OtelCounterExport::Derived(_))
                ),
                "{source:?}::{field} is enumerated and not derived, but no instrument was registered"
            );
            return Ok(());
        };
        counter.record(labels, value)
    }

    /// Every cell's operational counters, labelled by `cell_id` alone.
    ///
    /// Takes the runtime's per-cell totals rather than shard snapshots, and that
    /// is [`shard_counter_totals`]'s doing rather than a convenience: a per-cell
    /// series is a sum over every scope that has hosted that cell, closed ones
    /// included, and a method that saw only open shards could not compute it.
    /// `NodeHistograms::record_shard` is per shard because its own signature
    /// predates the question.
    ///
    /// Never `scope`, which `/metrics` does carry on three families — that
    /// divergence is §1.4 and is the reason both exports exist. Never
    /// `cell_id × edge_type`, which is where §1.3's cardinality arithmetic stops
    /// being affordable. Neither is expressible here anyway: the label type's
    /// constructor is private to `hydradb_telemetry::semconv` and `scope` is not
    /// in the registry at all.
    pub fn record_shard_totals(
        &self,
        totals: &std::collections::BTreeMap<String, hydradb::CellOperationalCounterTotals>,
    ) -> Result<(), hydradb_telemetry::meter::CounterError> {
        use hydradb_telemetry::semconv::L_CELL_ID;

        for (cell_id, fields) in shard_counter_totals(totals) {
            for &(field, value) in fields {
                self.record(CounterSource::Shard, field, &[(L_CELL_ID, cell_id)], value)?;
            }
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
    running: Option<(tokio::sync::watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
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
        telemetry: &hydradb_telemetry::TelemetryGuard,
        interval: std::time::Duration,
        query: hydradb::ClientQueryService,
        node: std::sync::Arc<hydradb::ScopedRoutedGraphCluster>,
    ) -> Self {
        let Some(providers) = telemetry.providers() else {
            // No endpoint: there is no metrics pipeline to feed, so taking the
            // cache locks once a minute would buy nothing at all.
            return Self { running: None };
        };
        let histograms = match NodeHistograms::register(providers) {
            Ok(histograms) => histograms,
            Err(error) => {
                tracing::warn!(error = %error, "OTel histograms did not register; no metrics will be exported");
                return Self { running: None };
            }
        };
        let memory = match memory_diagnostics::Instruments::register(providers) {
            Ok(memory) => memory,
            Err(error) => {
                tracing::warn!(%error, "memory diagnostics did not register; no metrics will be exported");
                return Self { running: None };
            }
        };
        let instruments = std::sync::Arc::new(NodeInstruments {
            memory,
            histograms,
            counters: NodeCounters::register(providers),
            planner_statistics: planner_statistics::Gauges::register(providers),
            remote_object_store: remote_object_store::Instruments::register(providers),
            node_runtime: node_runtime::Instruments::register(providers),
            storage_caches: storage_caches::Instruments::register(providers),
        });

        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(collect_forever(instruments, query, node, interval, stop_rx));
        tracing::info!(
            counters = otel_counter_instruments().len(),
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
        _telemetry: &hydradb_telemetry::TelemetryGuard,
        _interval: std::time::Duration,
        _query: hydradb::ClientQueryService,
        _node: std::sync::Arc<hydradb::ScopedRoutedGraphCluster>,
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

/// Everything registered against the meter, held by the collection task.
///
/// One `Arc` for both kinds rather than one each. They are registered together,
/// fed from the same two snapshots on the same interval, and dropped together;
/// two handles would only be two chances to feed one of them and not the other.
#[cfg(feature = "otlp")]
#[derive(Debug)]
struct NodeInstruments {
    memory: memory_diagnostics::Instruments,
    histograms: NodeHistograms,
    counters: NodeCounters,
    planner_statistics: planner_statistics::Gauges,
    remote_object_store: remote_object_store::Instruments,
    node_runtime: node_runtime::Instruments,
    storage_caches: storage_caches::Instruments,
}

/// One pass: publish the client histograms, then every shard's, then the shard
/// counters.
///
/// The order is deliberate. `ClientQueryService::metrics()` is a set of relaxed
/// `AtomicU64` loads — synchronous, lock-free, unconditionally cheap — while the
/// shard snapshot takes twelve cache mutexes per cell. Recording the cheap half
/// first means a shard that cannot be sampled costs only the shard series.
///
/// The counters come last and read the runtime's per-cell totals, because a
/// per-cell counter is a sum over every scope that has hosted that cell —
/// closed ones included — and the histograms' per-shard loop has no place to
/// put it. See [`shard_counter_totals`].
#[cfg(feature = "otlp")]
async fn collect_once(
    instruments: &NodeInstruments,
    query: &hydradb::ClientQueryService,
    node: &hydradb::ScopedRoutedGraphCluster,
    shard_budget: std::time::Duration,
) {
    let NodeInstruments {
        histograms,
        counters,
        memory,
        planner_statistics,
        remote_object_store,
        node_runtime,
        storage_caches,
    } = instruments;
    memory.record();
    remote_object_store.record();
    node_runtime.record().await;
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
            planner_statistics.record(&shards);
            storage_caches.record(&shards);
        }
        Err(_) => tracing::warn!(
            budget_ms = shard_budget.as_millis() as u64,
            "shard metric collection exceeded its budget; last interval's series stay published"
        ),
    }
    // Outside the budget too: the shared disk cache's counters are its own
    // recorder's, and reading them waits on no shard.
    storage_caches.record_disk(node.shared_disk_cache_metrics());
    // Outside the budget: the runtime's totals are atomics read under one
    // mutex, include closed scopes, and never wait on a shard.
    if let Err(error) = counters.record_shard_totals(&node.cell_operational_counter_totals()) {
        tracing::warn!(error = %error, "shard counters were not published");
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
    instruments: std::sync::Arc<NodeInstruments>,
    query: hydradb::ClientQueryService,
    node: std::sync::Arc<hydradb::ScopedRoutedGraphCluster>,
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
            () = collect_once(&instruments, &query, &node, shard_budget) => {}
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
