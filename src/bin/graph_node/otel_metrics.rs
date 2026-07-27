//! The OTel half of the metrics export, and the name tables both halves are
//! driven by.
//!
//! # Two exports, one enumeration
//!
//! The kernel enumerates its metrics by **Rust identifier** — `read_latency`,
//! `query_rows_latency`, `write_attempts` — and knows nothing about either
//! exposition vocabulary
//! (`slatedb_graph_kernel::ClientQueryMetricsSnapshot::histogram_fields`,
//! `::counter_fields`, `::class_counter_fields` and their siblings). The two
//! vocabularies live here and in [`crate::admin`]: this module holds the OTel
//! names, `admin.rs` holds the Prometheus names, and
//! [`tests::every_histogram_field_reaches_both_exports`] and
//! [`tests::every_counter_field_reaches_both_exports`] fail the build if a field
//! the kernel enumerates is missing from either. That is the property §1.6 of
//! `docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md` asks for:
//! adding a metric cannot silently reach one export and not the other.
//!
//! # One counter snapshot reaches the meter; the other two reach `/metrics` only
//!
//! All 65 kernel counters are exported through `/metrics`. Thirty-four are also
//! registered as OTel instruments: `GraphOperationalMetricsSnapshot`'s
//! thirty-five scalars, less the one that restates a histogram's sum.
//! [`METERED_COUNTER_SOURCES`] is that decision as a value rather than a
//! paragraph — one variant long — and [`otel_counter_instruments`] is what
//! [`NodeCounters::register`] iterates. `ClientQueryMetricsSnapshot` and
//! `GraphCacheMetricsSnapshot` are named there and deliberately not wired; each
//! needs a variant in that list and a `record_*` method beside
//! [`NodeCounters::record_shard_totals`], and nothing else.
//!
//! §1.4 is what makes the asymmetry legitimate rather than an oversight — the
//! two exports are allowed to differ in *dimensionality*, and they already do:
//! `/metrics` carries `turbolay.scope` on three counter families and OTLP
//! carries it nowhere.
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
//! [`NodeHistograms::register`] and [`NodeCounters::register`] take a
//! `turbolay_telemetry::otlp::Providers`, reached through
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
//! Prometheus histogram already is. `turbolay_telemetry::meter` owns that
//! rendering, and owning it in one place is what stops the two exports
//! disagreeing about where a bucket ends. See that module for the full
//! argument.

// Only the recording half names a snapshot, and that half is behind `otlp`.
#[cfg(feature = "otlp")]
use slatedb_graph_kernel::DurationHistogramSnapshot;

/// The export proof, in its own file because a stand-in collector is a hundred
/// lines that have nothing to say about metric names.
///
/// Gated on `otlp` as well as on `test`, because everything it touches is: there
/// is no [`NodeCounters`] to register without the feature. That also means no
/// `just` recipe runs it — `check-all-features` compiles it and stops — which is
/// why the properties that can be checked without a meter are checked in
/// [`tests`] instead, and this file is confined to the one claim that genuinely
/// needs a socket.
///
/// The `#[path]` is not decoration. This module is itself declared with a `#[path]`
/// in `graph-node.rs`, and a child of such a module resolves against the
/// *directory of that path* — `src/bin/graph_node/` — rather than against
/// `…/otel_metrics/`. Without the attribute rustc looks for
/// `graph_node/otlp_export_tests.rs`, which would put a child of this module beside
/// its parent.
#[cfg(all(test, feature = "otlp"))]
#[path = "otel_metrics/otlp_export_tests.rs"]
mod otlp_export_tests;

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
    /// Whether this binary has anything to record into the instrument.
    ///
    /// Declared per row rather than inferred, and cross-checked against the
    /// kernel's enumeration by
    /// [`tests::only_the_transport_histograms_declare_no_graph_node_source`].
    /// This column is the answer to "why does a five-row table have three live
    /// families".
    pub source: FieldSource,
}

/// The OTel name table. One row per histogram the kernel enumerates.
///
/// Adding a histogram to a snapshot type and not adding it here fails
/// [`tests::every_histogram_field_reaches_both_exports`].
///
/// Five rows, three of them with a `graph-node` source. That is not a table
/// half-finished: see [`FieldSource`], and
/// [`tests::only_the_transport_histograms_declare_no_graph_node_source`] for the
/// assertion that keeps the three-of-five accounting honest.
pub const OTEL_HISTOGRAMS: &[OtelHistogram] = &[
    OtelHistogram {
        field: "read_latency",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(turbolay_telemetry::semconv::DB_OPERATION_READ),
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "write_latency",
        name: "db.client.operation.duration",
        description: "End-to-end client operation execution",
        unit: ExportUnit::Seconds,
        operation: Some(turbolay_telemetry::semconv::DB_OPERATION_WRITE),
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "query_rows_latency",
        name: "turbolay.query.rows.duration",
        description: "Shard row-query execution",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::GraphNode,
    },
    OtelHistogram {
        field: "rpc_latency",
        name: "turbolay.query.transport.rpc.duration",
        description: "Query-transport client RPC round-trip",
        unit: ExportUnit::Microseconds,
        operation: None,
        source: FieldSource::TransportOnly,
    },
    OtelHistogram {
        field: "serve_latency",
        name: "turbolay.query.transport.serve.duration",
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
/// already sized for the tenant count it sees and carries `turbolay.scope` on
/// three counter families for historical reasons, while OTLP ships to a vendor
/// billing per series. No variant here carries a scope, and there is no way to
/// spell one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtelCounterExport {
    /// One process-global sum, no attributes beyond the resource.
    Global(&'static str),
    /// One sum per `turbolay.cell_id`, summed over every scope open on the node.
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
/// `graph_compute_queue_us` — and nothing else is. A column would be sixty-seven
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
    fn meter_unit(self) -> turbolay_telemetry::meter::CounterUnit {
        use turbolay_telemetry::meter::CounterUnit;
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
/// sentence would be: sixty-seven generated sentences would each restate the
/// metric name in longer words, whereas the identifier is the one thing the name
/// does *not* carry, and it is the key `/metrics`, the kernel's enumeration and
/// both name tables are joined on. An operator who finds
/// `turbolay.shard.compute.queue.duration.sum` on a dashboard and wants the code
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
/// `turbolay.*` throughout. §1.9 says `db.*` where a semantic convention
/// genuinely exists, and there is no semconv counter for any of these -- the one
/// stable database metric, `db.client.operation.duration`, is a histogram and is
/// already claimed by [`OTEL_HISTOGRAMS`].
pub const OTEL_COUNTERS: &[OtelCounter] = &[
    // `ClientQueryMetricsSnapshot`, in declaration order.
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_started",
        export: OtelCounterExport::Global("turbolay.client.queries.started"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_completed",
        export: OtelCounterExport::Global("turbolay.client.queries.completed"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_failed",
        export: OtelCounterExport::Global("turbolay.client.queries.failed"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "rows_returned",
        export: OtelCounterExport::Global("turbolay.client.rows.returned"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "auth_failures",
        export: OtelCounterExport::Global("turbolay.client.auth.failures"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "scope_denials",
        export: OtelCounterExport::Global("turbolay.client.scope.denials"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "cancellations",
        export: OtelCounterExport::Global("turbolay.client.queries.cancelled"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "backpressure_waits",
        export: OtelCounterExport::Global("turbolay.client.backpressure.waits"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "prepare_requests",
        export: OtelCounterExport::Global("turbolay.client.prepare.requests"),
    },
    OtelCounter {
        source: CounterSource::Client,
        field: "prepare_duration_us",
        export: OtelCounterExport::Global("turbolay.client.prepare.duration.sum"),
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
        export: OtelCounterExport::PerCell("turbolay.shard.write.attempts"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_commits",
        export: OtelCounterExport::PerCell("turbolay.shard.write.commits"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "write_retries",
        export: OtelCounterExport::PerCell("turbolay.shard.write.retries"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_batches_profiled",
        export: OtelCounterExport::PerCell("turbolay.shard.bulk_import.batches_profiled"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_preflight_us",
        export: OtelCounterExport::PerCell("turbolay.shard.bulk_import.preflight.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_batch_build_us",
        export: OtelCounterExport::PerCell("turbolay.shard.bulk_import.batch_build.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_counter_read_us",
        export: OtelCounterExport::PerCell("turbolay.shard.bulk_import.counter_read.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "bulk_import_commit_us",
        export: OtelCounterExport::PerCell("turbolay.shard.bulk_import.commit.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_builds_started",
        export: OtelCounterExport::PerCell("turbolay.shard.artifact.builds.started"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_builds_completed",
        export: OtelCounterExport::PerCell("turbolay.shard.artifact.builds.completed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_build_duration_us",
        export: OtelCounterExport::PerCell("turbolay.shard.artifact.build.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_publish_batches",
        export: OtelCounterExport::PerCell("turbolay.shard.artifact.publish.batches"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_records_published",
        export: OtelCounterExport::PerCell("turbolay.shard.artifact.records.published"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "artifact_publish_duration_us",
        export: OtelCounterExport::PerCell("turbolay.shard.artifact.publish.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_jobs_started",
        export: OtelCounterExport::PerCell("turbolay.shard.gc.jobs.started"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_jobs_completed",
        export: OtelCounterExport::PerCell("turbolay.shard.gc.jobs.completed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_keys_deleted",
        export: OtelCounterExport::PerCell("turbolay.shard.gc.keys.deleted"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "gc_duration_us",
        export: OtelCounterExport::PerCell("turbolay.shard.gc.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "verifier_runs",
        export: OtelCounterExport::PerCell("turbolay.shard.verifier.runs"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "verifier_failures",
        export: OtelCounterExport::PerCell("turbolay.shard.verifier.failures"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "verifier_duration_us",
        export: OtelCounterExport::PerCell("turbolay.shard.verifier.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_started",
        export: OtelCounterExport::PerCell("turbolay.shard.query.rows.started"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_completed",
        export: OtelCounterExport::PerCell("turbolay.shard.query.rows.completed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_failed",
        export: OtelCounterExport::PerCell("turbolay.shard.query.rows.failed"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_returned",
        export: OtelCounterExport::PerCell("turbolay.shard.query.rows.returned"),
    },
    // Derived: the kernel sets it from `query_rows_latency.sum_us`.
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_duration_us",
        export: OtelCounterExport::Derived(&["turbolay.query.rows.duration"]),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_artifact_lookup_us",
        export: OtelCounterExport::PerCell("turbolay.shard.query.artifact_lookup.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_cache_us",
        export: OtelCounterExport::PerCell("turbolay.shard.query.graphblas_cache.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_artifact_snapshots",
        export: OtelCounterExport::PerCell("turbolay.shard.query.graphblas.artifact_snapshots"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_rebuilt_snapshots",
        export: OtelCounterExport::PerCell("turbolay.shard.query.graphblas.rebuilt_snapshots"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rust_sparse_fallbacks",
        export: OtelCounterExport::PerCell("turbolay.shard.query.rust_sparse_fallbacks"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "graph_compute_tasks",
        export: OtelCounterExport::PerCell("turbolay.shard.compute.tasks"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "graph_compute_queue_us",
        export: OtelCounterExport::PerCell("turbolay.shard.compute.queue.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "graph_compute_duration_us",
        export: OtelCounterExport::PerCell("turbolay.shard.compute.duration.sum"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "backpressure_waits",
        export: OtelCounterExport::PerCell("turbolay.shard.backpressure.waits"),
    },
    // `GraphCacheMetricsSnapshot`, in declaration order.
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_artifact_hits",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.matrix_artifact.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_artifact_misses",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.matrix_artifact.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_adjacency_hits",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.matrix_adjacency.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "matrix_adjacency_misses",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.matrix_adjacency.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "graphblas_hits",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.graphblas.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "graphblas_misses",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.graphblas.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "parsed_row_query_hits",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.parsed_row_query.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "parsed_row_query_misses",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.parsed_row_query.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_rows_hits",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.relationship_rows.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_rows_misses",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.relationship_rows.misses"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_property_rows_hits",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.relationship_property_rows.hits"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "relationship_property_rows_misses",
        export: OtelCounterExport::PerCell(
            "turbolay.shard.cache.relationship_property_rows.misses",
        ),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "insertions",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.insertions"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "evictions",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.evictions"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "pinned_insertions",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.pinned_insertions"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "tenant_quota_rejections",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.tenant_quota_rejections"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "hydration_started",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.hydration.started"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "hydration_waited",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.hydration.waited"),
    },
    OtelCounter {
        source: CounterSource::ShardCache,
        field: "hydration_completed",
        export: OtelCounterExport::PerCell("turbolay.shard.cache.hydration.completed"),
    },
];

/// The OTel names for the counters dimensioned by `error.class`.
///
/// A separate table because the series carry an extra attribute the scalar rows
/// do not, and folding a `by_class` flag into [`OtelCounter`] would put a
/// boolean on sixty-five rows to distinguish two.
///
/// The name is *not* the scalar's. `turbolay.client.queries.failed` with an
/// `error.class` attribute would be the tidier modelling, but the scalar is also
/// exported and the two would then be one instrument reporting a total and a
/// breakdown under the same name -- the kind of double count a dashboard sums
/// without noticing.
pub const OTEL_CLASS_COUNTERS: &[OtelCounter] = &[
    OtelCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_class",
        export: OtelCounterExport::Global("turbolay.client.queries.failed.by_class"),
    },
    OtelCounter {
        source: CounterSource::Shard,
        field: "query_rows_failed_by_class",
        export: OtelCounterExport::PerCell("turbolay.shard.query.rows.failed.by_class"),
    },
];

/// The OTel row for a `(source, field)` pair, over both counter tables.
pub fn otel_counter(source: CounterSource, field: &str) -> Option<&'static OtelCounter> {
    OTEL_COUNTERS
        .iter()
        .chain(OTEL_CLASS_COUNTERS)
        .find(|export| export.source == source && export.field == field)
}

/// Which counter snapshots this binary registers instruments for and feeds.
///
/// One variant, and the whole of the remaining wiring is this list. Adding
/// [`CounterSource::Client`] registers eleven more instruments and needs a
/// `record_client_counters` beside [`NodeCounters::record_shard_totals`] fed from
/// `ClientQueryService::metrics()`, which `collect_once` already holds; adding
/// [`CounterSource::ShardCache`] registers nineteen and needs the same over
/// `metrics.shard.cache`, off the slice `collect_once` already has. Neither is a
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

/// Every shard's operational counters, summed by `cell_id`.
///
/// [`OtelCounterExport::PerCell`] reads "one sum per `turbolay.cell_id`, summed
/// over every scope open on the node", and this is where the summing happens.
/// Publishing per scope instead would not duplicate and would not warn: the series
/// identity is the label set, so the last scope recorded in an interval would
/// simply *replace* the others and the counter would report one tenant's traffic
/// as the cell's. The same arithmetic on the `/metrics` side is
/// `admin::append_per_cell_counters`, and it is there for a sharper version of the
/// same reason — two identical series in one scrape is an outright rejection
/// rather than a plausible undercount.
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
    shards: &[slatedb_graph_kernel::ScopedGraphShardRuntimeMetrics],
) -> Vec<(&str, Vec<(&'static str, u64)>)> {
    let mut totals: std::collections::BTreeMap<&str, Vec<(&'static str, u64)>> =
        std::collections::BTreeMap::new();
    for shard in shards {
        let cell = totals.entry(shard.shard.cell_id.as_str()).or_default();
        for (field, value) in shard.shard.operational.counter_fields() {
            match cell.iter_mut().find(|(name, _)| *name == field) {
                // Saturating for the reason `admin::accumulate` gives: these are
                // `u64` counters only a decades-long uptime could overflow, and a
                // wrapped total on a metrics series is a phantom incident.
                Some(slot) => slot.1 = slot.1.saturating_add(value),
                None => cell.push((field, value)),
            }
        }
    }
    totals.into_iter().collect()
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
    ///
    /// **No production caller, by construction** — both rows are
    /// [`FieldSource::TransportOnly`]. It exists so the two instruments have a
    /// recording path the day a process that *does* hold a transport snapshot
    /// registers them, and so `collect_once` does not have to grow a branch for a
    /// snapshot it will never see.
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
    registered: std::collections::HashMap<
        (CounterSource, &'static str),
        turbolay_telemetry::meter::ObservableCounter,
    >,
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
    pub fn register(providers: &turbolay_telemetry::otlp::Providers) -> Self {
        use turbolay_telemetry::meter::{CounterSpec, ObservableCounter};

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
        labels: &[(turbolay_telemetry::semconv::MetricLabel, &str)],
        value: u64,
    ) -> Result<(), turbolay_telemetry::meter::CounterError> {
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

    /// Every shard's operational counters, labelled by `cell_id` alone.
    ///
    /// Takes the whole slice rather than one shard at a time, and that is
    /// [`shard_counter_totals`]'s doing rather than a convenience: a per-cell
    /// series is a sum over the scopes on that cell, and a method that saw one
    /// scope could not compute it. `NodeHistograms::record_shard` is per shard
    /// because its own signature predates the question.
    ///
    /// Never `scope`, which `/metrics` does carry on three families — that
    /// divergence is §1.4 and is the reason both exports exist. Never
    /// `cell_id × edge_type`, which is where §1.3's cardinality arithmetic stops
    /// being affordable. Neither is expressible here anyway: the label type's
    /// constructor is private to `turbolay_telemetry::semconv` and `scope` is not
    /// in the registry at all.
    pub fn record_shard_totals(
        &self,
        shards: &[slatedb_graph_kernel::ScopedGraphShardRuntimeMetrics],
    ) -> Result<(), turbolay_telemetry::meter::CounterError> {
        use turbolay_telemetry::semconv::L_CELL_ID;

        for (cell_id, fields) in shard_counter_totals(shards) {
            for (field, value) in fields {
                self.record(CounterSource::Shard, field, &[(L_CELL_ID, cell_id)], value)?;
            }
        }
        Ok(())
    }

    /// Every series each registered instrument is currently reporting.
    ///
    /// The published totals, read back through the same accessor the SDK's
    /// callback uses. It exists because the export proof asserts on a protobuf
    /// frame, where an instrument name is a literal substring and a value is a
    /// varint: the socket is what proves the series *left the process*, and this is
    /// what proves the number in it was the sum and not one scope's share.
    #[cfg(test)]
    fn observations(
        &self,
        source: CounterSource,
        field: &'static str,
    ) -> Vec<turbolay_telemetry::meter::Observation<u64>> {
        self.registered
            .get(&(source, field))
            .map(turbolay_telemetry::meter::ObservableCounter::observations)
            .unwrap_or_default()
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
            Ok(histograms) => histograms,
            Err(error) => {
                tracing::warn!(error = %error, "OTel histograms did not register; no metrics will be exported");
                return Self { running: None };
            }
        };
        let instruments = std::sync::Arc::new(NodeInstruments {
            histograms,
            counters: NodeCounters::register(providers),
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

/// Everything registered against the meter, held by the collection task.
///
/// One `Arc` for both kinds rather than one each. They are registered together,
/// fed from the same two snapshots on the same interval, and dropped together;
/// two handles would only be two chances to feed one of them and not the other.
#[cfg(feature = "otlp")]
#[derive(Debug)]
struct NodeInstruments {
    histograms: NodeHistograms,
    counters: NodeCounters,
}

/// One pass: publish the client histograms, then every shard's, then the shard
/// counters.
///
/// The order is deliberate. `ClientQueryService::metrics()` is a set of relaxed
/// `AtomicU64` loads — synchronous, lock-free, unconditionally cheap — while the
/// shard snapshot takes twelve cache mutexes per cell. Recording the cheap half
/// first means a shard that cannot be sampled costs only the shard series.
///
/// The counters come last and take the whole slice at once, because a per-cell
/// counter is a sum over the scopes on that cell and the histograms' per-shard
/// loop has no place to put it. See [`shard_counter_totals`].
#[cfg(feature = "otlp")]
async fn collect_once(
    instruments: &NodeInstruments,
    query: &slatedb_graph_kernel::ClientQueryService,
    node: &slatedb_graph_kernel::ScopedRoutedGraphCluster,
    shard_budget: std::time::Duration,
) {
    let NodeInstruments {
        histograms,
        counters,
    } = instruments;
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
            if let Err(error) = counters.record_shard_totals(&shards) {
                tracing::warn!(error = %error, "shard counters were not published");
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
    instruments: std::sync::Arc<NodeInstruments>,
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

#[cfg(test)]
mod tests {
    use slatedb_graph_kernel::{
        ClientQueryMetricsSnapshot, GraphCacheMetricsSnapshot, GraphId,
        GraphOperationalMetricsSnapshot, GraphScope, GraphShardRuntimeMetrics, NamespaceId,
        QueryTransportMetricsSnapshot, ScopedGraphShardRuntimeMetrics, DURATION_BUCKET_BOUNDS_US,
        DURATION_BUCKET_COUNT,
    };

    use super::*;
    use crate::admin::{
        prometheus_counter, prometheus_histogram, PrometheusCounterExport,
        PROMETHEUS_CLASS_COUNTERS, PROMETHEUS_COUNTERS, PROMETHEUS_HISTOGRAMS,
    };

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

    /// Every counter field the kernel enumerates, from every snapshot type this
    /// binary can obtain, tagged with the snapshot it came from.
    ///
    /// Scalar counters and per-class counters in one list, because a name table
    /// row is a name table row and the two tables are searched together. The
    /// class rows arrive flattened — one per field per class — and are collapsed
    /// with a consecutive `dedup`, which is enough because
    /// `class_counter_fields` groups by field.
    ///
    /// `QueryTransportMetricsSnapshot` is absent on purpose; see
    /// [`CounterSource`].
    fn enumerated_counter_fields() -> Vec<(CounterSource, &'static str)> {
        let client = ClientQueryMetricsSnapshot::default();
        let operational = GraphOperationalMetricsSnapshot::default();
        let cache = GraphCacheMetricsSnapshot::default();
        let mut fields: Vec<(CounterSource, &'static str)> = Vec::new();
        fields.extend(
            client
                .counter_fields()
                .map(|(field, _)| (CounterSource::Client, field)),
        );
        fields.extend(
            client
                .class_counter_fields()
                .map(|(field, _, _)| (CounterSource::Client, field)),
        );
        fields.extend(
            operational
                .counter_fields()
                .map(|(field, _)| (CounterSource::Shard, field)),
        );
        fields.extend(
            operational
                .class_counter_fields()
                .map(|(field, _, _)| (CounterSource::Shard, field)),
        );
        fields.extend(
            cache
                .counter_fields()
                .map(|(field, _)| (CounterSource::ShardCache, field)),
        );
        fields.dedup();
        fields
    }

    /// §1.6, for counters. The same property
    /// [`every_histogram_field_reaches_both_exports`] holds for the five
    /// duration histograms, over the sixty-five counters that are the actual
    /// gap: `/metrics` exported eight of them before M2.
    ///
    /// The reverse direction matters more here than it did for histograms. A
    /// name table with sixty-five rows can grow a row for a field that was
    /// renamed or deleted and nothing else will ever notice, and a dead row
    /// reads exactly like a live counter whose recording site is missing.
    #[test]
    fn every_counter_field_reaches_both_exports() {
        let enumerated = enumerated_counter_fields();
        assert_eq!(
            enumerated.len(),
            67,
            "35 operational + 19 cache + 11 client counters, plus the two \
             error-class breakdowns: {enumerated:#?}"
        );

        for (source, field) in &enumerated {
            assert!(
                otel_counter(*source, field).is_some(),
                "{source:?}::{field} is recorded by the kernel but has no OTel name"
            );
            assert!(
                prometheus_counter(*source, field).is_some(),
                "{source:?}::{field} is recorded by the kernel but has no Prometheus name"
            );
        }

        for export in OTEL_COUNTERS.iter().chain(OTEL_CLASS_COUNTERS) {
            assert!(
                enumerated.contains(&(export.source, export.field)),
                "OTEL_COUNTERS names {:?}::{}, which no snapshot enumerates",
                export.source,
                export.field
            );
        }
        for export in PROMETHEUS_COUNTERS.iter().chain(PROMETHEUS_CLASS_COUNTERS) {
            assert!(
                enumerated.contains(&(export.source, export.field)),
                "PROMETHEUS_COUNTERS names {:?}::{}, which no snapshot enumerates",
                export.source,
                export.field
            );
        }
    }

    /// The `PROMETHEUS_HISTOGRAMS`-names-five-when-three-have-sources
    /// reconciliation, as an assertion rather than a paragraph.
    ///
    /// Three properties in one test because they are one claim:
    ///
    /// 1. Exactly the fields `QueryTransportMetricsSnapshot` enumerates are
    ///    declared [`FieldSource::TransportOnly`], in **both** tables. Flip one
    ///    row either way and this fails.
    /// 2. The two tables agree per field, so a future round cannot decide the
    ///    transport is reachable for `/metrics` and not for the meter.
    /// 3. The count of `graph-node`-sourced rows equals the number of histograms
    ///    the two snapshots this binary *does* hold enumerate — derived from the
    ///    kernel, not written as `3`, so it stays true when H3 adds a family.
    #[test]
    fn only_the_transport_histograms_declare_no_graph_node_source() {
        let transport: Vec<&str> = QueryTransportMetricsSnapshot::default()
            .histogram_fields()
            .map(|(field, _)| field)
            .collect();
        assert_eq!(
            transport,
            vec!["rpc_latency", "serve_latency"],
            "the transport snapshot's histograms changed; reclassify the rows"
        );

        let expected = |field: &str| {
            if transport.contains(&field) {
                FieldSource::TransportOnly
            } else {
                FieldSource::GraphNode
            }
        };
        for export in OTEL_HISTOGRAMS {
            assert_eq!(
                export.source,
                expected(export.field),
                "OTEL_HISTOGRAMS misclassifies {}",
                export.field
            );
        }
        for export in PROMETHEUS_HISTOGRAMS {
            assert_eq!(
                export.source,
                expected(export.field),
                "PROMETHEUS_HISTOGRAMS misclassifies {}",
                export.field
            );
            let otel = otel_histogram(export.field).expect("named for OTel");
            assert_eq!(
                export.source, otel.source,
                "{} has a source in one export and not the other",
                export.field
            );
        }

        let node_sourced = ClientQueryMetricsSnapshot::default()
            .histogram_fields()
            .count()
            + GraphOperationalMetricsSnapshot::default()
                .histogram_fields()
                .count();
        for table in [
            PROMETHEUS_HISTOGRAMS
                .iter()
                .filter(|row| row.source == FieldSource::GraphNode)
                .count(),
            OTEL_HISTOGRAMS
                .iter()
                .filter(|row| row.source == FieldSource::GraphNode)
                .count(),
        ] {
            assert_eq!(
                table, node_sourced,
                "a name table claims a live family this binary has no snapshot for"
            );
        }
    }

    /// The twenty-three counters that reach neither export, pinned.
    ///
    /// This is the test that makes the absence structural. Three directions:
    ///
    /// - The kernel's own enumeration equals [`TRANSPORT_ONLY_COUNTERS`], so a
    ///   counter added to `QueryTransportMetricsSnapshot` fails here instead of
    ///   joining a silent gap — and a counter *removed* from it leaves a dead
    ///   entry that also fails.
    /// - Every counter the kernel declares is accounted for: the sixty-seven rows
    ///   `every_counter_field_reaches_both_exports` covers plus these
    ///   twenty-three is the whole of it, so "sixty-five exported" is a complete
    ///   statement rather than a count of what somebody got round to.
    /// - Where a transport identifier *does* have a name-table row, a `graph-node`
    ///   snapshot must also enumerate it. Otherwise the row would be exporting a
    ///   transport number under a source that cannot produce one, which is the
    ///   exact mistake a table keyed by `field` alone would invite —
    ///   `auth_failures`, `cancellations` and `backpressure_waits` all take this
    ///   branch (the last of them twice, since the shard has one too), so it is
    ///   not vacuous.
    #[test]
    fn the_transport_counters_are_pinned_as_sourceless() {
        let enumerated: Vec<&str> = QueryTransportMetricsSnapshot::default()
            .counter_fields()
            .map(|(field, _)| field)
            .collect();
        assert_eq!(
            enumerated, TRANSPORT_ONLY_COUNTERS,
            "the transport snapshot's counters changed; TRANSPORT_ONLY_COUNTERS is the pin"
        );

        let exported = enumerated_counter_fields();
        assert_eq!(
            exported.len() + TRANSPORT_ONLY_COUNTERS.len(),
            67 + 23,
            "every counter the kernel declares is either exported or pinned here"
        );

        let mut collided: Vec<(CounterSource, &str)> = Vec::new();
        for field in TRANSPORT_ONLY_COUNTERS {
            for source in CounterSource::ALL {
                if otel_counter(*source, field).is_none() {
                    continue;
                }
                collided.push((*source, field));
                assert!(
                    exported.contains(&(*source, *field)),
                    "{source:?}::{field} has a name table row, but only the transport \
                     snapshot enumerates that identifier"
                );
            }
        }
        assert_eq!(
            collided,
            vec![
                (CounterSource::Client, "auth_failures"),
                (CounterSource::Client, "cancellations"),
                (CounterSource::Client, "backpressure_waits"),
                (CounterSource::Shard, "backpressure_waits"),
            ],
            "the identifiers a transport counter shares with an exported one changed"
        );
    }

    /// The trip-wire for the day a transport source appears.
    ///
    /// Vacuous today, and deliberately so: every [`CounterSource`] variant is
    /// [`FieldSource::GraphNode`] and there is nothing else to say. What makes it
    /// worth its lines is the *pair* — [`CounterSource::field_source`]'s
    /// exhaustive match means a new variant does not compile until it is
    /// classified, and a variant classified [`FieldSource::TransportOnly`] fails
    /// here. So a future round that gives `graph-node` a transport snapshot gets a
    /// red test naming exactly what it still owes, instead of twenty-three series
    /// that stay missing because nobody remembered the tables.
    #[test]
    fn every_counter_source_has_a_graph_node_snapshot() {
        assert_eq!(
            CounterSource::ALL.len(),
            3,
            "CounterSource::ALL is out of step with the enum"
        );
        for source in CounterSource::ALL {
            assert_eq!(
                source.field_source(),
                FieldSource::GraphNode,
                "{source:?} has no graph-node snapshot, so its counters need name \
                 table rows and a renderer in admin.rs before this can pass"
            );
        }
    }

    /// A counter that restates a histogram's sum must be marked derived on
    /// **both** sides or on neither.
    ///
    /// The two exports are allowed to disagree about dimensionality (§1.4) and
    /// about names (§1.9). They are not allowed to disagree about whether a
    /// number is exported at all, because that is the one difference an operator
    /// reading two dashboards would read as data.
    #[test]
    fn the_two_exports_agree_about_which_counters_are_derived() {
        for (source, field) in enumerated_counter_fields() {
            let otel = otel_counter(source, field).expect("named for OTel");
            let prometheus = prometheus_counter(source, field).expect("named for Prometheus");
            assert_eq!(
                matches!(otel.export, OtelCounterExport::Derived(_)),
                matches!(prometheus.export, PrometheusCounterExport::Derived(_)),
                "{source:?}::{field} is derived in one export and a series in the other"
            );
        }
    }

    /// A derived counter has to point at a histogram family that is really
    /// exported, or "it is already on the wire" is an unchecked claim.
    #[test]
    fn derived_counters_point_at_exported_histogram_families() {
        for export in OTEL_COUNTERS {
            let OtelCounterExport::Derived(families) = export.export else {
                continue;
            };
            assert!(
                !families.is_empty(),
                "{} is derived from nothing",
                export.field
            );
            for family in families {
                assert!(
                    OTEL_HISTOGRAMS.iter().any(|row| row.name == *family),
                    "{} is derived from {family}, which no OTel histogram is named",
                    export.field
                );
            }
        }
        for export in PROMETHEUS_COUNTERS {
            let PrometheusCounterExport::Derived(families) = export.export else {
                continue;
            };
            assert!(
                !families.is_empty(),
                "{} is derived from nothing",
                export.field
            );
            for family in families {
                assert!(
                    PROMETHEUS_HISTOGRAMS.iter().any(|row| row.name == *family),
                    "{} is derived from {family}, which no Prometheus histogram is named",
                    export.field
                );
            }
        }
    }

    /// One name, one metric — across the counter tables *and* the histogram
    /// tables, because the collision that actually bites is between the two.
    ///
    /// `query_rows_duration_us` is the kernel's name for
    /// `query_rows_latency.sum_us`, and the obvious Prometheus name for it is
    /// `graph_query_rows_duration_microseconds` — which is already the histogram
    /// family's stem. Two `# TYPE` lines for one name, one `counter` and one
    /// `histogram`, is a rejected scrape rather than an extra series, so this is
    /// the test that made [`PrometheusCounterExport::Derived`] exist.
    #[test]
    fn no_two_metrics_share_an_exported_name() {
        let mut prometheus: Vec<&str> = PROMETHEUS_COUNTERS
            .iter()
            .chain(PROMETHEUS_CLASS_COUNTERS)
            .filter_map(|export| export.export.name())
            .chain(PROMETHEUS_HISTOGRAMS.iter().map(|export| export.name))
            .collect();
        let before = prometheus.len();
        prometheus.sort_unstable();
        prometheus.dedup();
        assert_eq!(
            before,
            prometheus.len(),
            "two Prometheus metrics share a name"
        );

        // `OTEL_HISTOGRAMS` names are deliberately not unique — read and write
        // share `db.client.operation.duration` and are told apart by
        // `db.operation.name` — so they are deduplicated before being joined
        // rather than counted twice.
        let mut histograms: Vec<&str> = OTEL_HISTOGRAMS.iter().map(|export| export.name).collect();
        histograms.sort_unstable();
        histograms.dedup();
        let mut otel: Vec<&str> = OTEL_COUNTERS
            .iter()
            .chain(OTEL_CLASS_COUNTERS)
            .filter_map(|export| export.export.name())
            .chain(histograms)
            .collect();
        let before = otel.len();
        otel.sort_unstable();
        otel.dedup();
        assert_eq!(before, otel.len(), "two OTel metrics share a name");
    }

    /// `turbolay.scope` is unbounded per tenant, and §1.4's decision was to
    /// leave the families that already carry it and let nothing new join them.
    ///
    /// "Nothing new" is three specific families, named here. On the OTel side
    /// there is nothing to assert because there is nothing to say: no
    /// [`OtelCounterExport`] variant carries a scope.
    #[test]
    fn only_the_three_pre_existing_families_are_dimensioned_by_scope() {
        let scoped: Vec<&str> = PROMETHEUS_COUNTERS
            .iter()
            .chain(PROMETHEUS_CLASS_COUNTERS)
            .filter(|export| matches!(export.export, PrometheusCounterExport::ScopePerCell(_)))
            .map(|export| export.field)
            .collect();
        assert_eq!(
            scoped,
            vec![
                "query_graphblas_artifact_snapshots",
                "query_graphblas_rebuilt_snapshots",
                "query_rust_sparse_fallbacks",
            ],
            "a new counter family took a scope label"
        );
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

    /// Two shards on one `cell_id` under different tenants, plus a third cell.
    ///
    /// The shape `local_shard_runtime_metrics` returns on a node hosting two
    /// tenants that both have work on the same cell — the case that makes the
    /// summing in [`shard_counter_totals`] necessary rather than tidy.
    fn two_tenants_on_one_cell_and_one_alone() -> Vec<ScopedGraphShardRuntimeMetrics> {
        [
            ("alpha", "cell-a", 3u64, 700u64),
            ("beta", "cell-a", 4, 90),
            ("alpha", "cell-b", 5, 1),
        ]
        .into_iter()
        .map(
            |(tenant, cell_id, write_attempts, gc_duration_us)| ScopedGraphShardRuntimeMetrics {
                scope: GraphScope::tenant(
                    NamespaceId::new(tenant).expect("a valid namespace id"),
                    GraphId::default(),
                ),
                shard: GraphShardRuntimeMetrics {
                    cell_id: cell_id.to_string(),
                    operational: GraphOperationalMetricsSnapshot {
                        write_attempts,
                        gc_duration_us,
                        ..Default::default()
                    },
                    cache: GraphCacheMetricsSnapshot::default(),
                    cache_entries: Default::default(),
                    cache_resident_bytes: Default::default(),
                },
            },
        )
        .collect()
    }

    /// The counters `NodeCounters` registers are exactly
    /// `GraphOperationalMetricsSnapshot`'s scalars that are not derived — total
    /// over the kernel's own enumeration, in both directions.
    ///
    /// This is what stops the wiring being advisory. Before it, a shard counter
    /// added to the kernel would get a name from
    /// [`every_counter_field_reaches_both_exports`], reach `/metrics`, and reach
    /// the meter only if somebody remembered — and a missing OTLP series looks
    /// exactly like a counter at rest. The `iff` is the whole assertion: a
    /// registered derived row would double-count a histogram's `.sum` under a
    /// second name, and an unregistered plain row is a silent hole.
    #[test]
    fn every_shard_counter_is_registered_as_an_instrument() {
        let registered: Vec<(CounterSource, &str)> = otel_counter_instruments()
            .iter()
            .map(|row| (row.source, row.field))
            .collect();

        let enumerated: Vec<&'static str> = GraphOperationalMetricsSnapshot::default()
            .counter_fields()
            .map(|(field, _)| field)
            .collect();
        assert_eq!(
            enumerated.len(),
            35,
            "the operational snapshot's scalar counters changed: {enumerated:#?}"
        );

        let mut derived = 0;
        for field in &enumerated {
            let row = otel_counter(CounterSource::Shard, field).expect("named for OTel");
            let is_derived = matches!(row.export, OtelCounterExport::Derived(_));
            derived += usize::from(is_derived);
            assert_eq!(
                registered.contains(&(CounterSource::Shard, *field)),
                !is_derived,
                "{field} is registered as an instrument iff it is not derived"
            );
        }
        assert_eq!(
            derived, 1,
            "query_rows_duration_us is the one shard counter that restates a histogram sum"
        );
        assert_eq!(
            registered.len(),
            enumerated.len() - derived,
            "otel_counter_instruments names something the operational snapshot does not \
             enumerate: {registered:#?}"
        );
    }

    /// One snapshot is on the meter and two are not, and the stopping point is
    /// pinned rather than described.
    ///
    /// Three absences, each of which would otherwise be indistinguishable from an
    /// oversight: the two unwired sources, named so that a reader knows the gap is
    /// bounded and knows what closing it costs; and the error-class breakdowns,
    /// which are absent because `cell_id × error.class` is ten series per cell
    /// instead of one and that multiplier is a §1.4 decision, not a wiring detail.
    #[test]
    fn only_the_shard_scalars_are_registered_this_round() {
        assert_eq!(METERED_COUNTER_SOURCES, &[CounterSource::Shard]);
        let unwired: Vec<CounterSource> = CounterSource::ALL
            .iter()
            .copied()
            .filter(|source| !METERED_COUNTER_SOURCES.contains(source))
            .collect();
        assert_eq!(
            unwired,
            vec![CounterSource::Client, CounterSource::ShardCache],
            "the set of counter snapshots reaching /metrics alone changed"
        );

        let instruments = otel_counter_instruments();
        for row in OTEL_CLASS_COUNTERS {
            assert!(
                !instruments
                    .iter()
                    .any(|instrument| instrument.source == row.source
                        && instrument.field == row.field),
                "{} is registered, so it now costs cell_id × error.class series",
                row.field
            );
        }
    }

    /// A counter's unit is derived from its field name, so the derivation and both
    /// name tables have to agree — in all three directions, or the derivation is
    /// just a guess that happens to be right today.
    ///
    /// A microsecond total exported as a count is not a wrong number an operator
    /// can see; it is a series whose scale has to be guessed, and guessed against
    /// the `_microseconds` series `/metrics` renders from the same field.
    #[test]
    fn the_microsecond_counters_are_named_for_their_unit() {
        let mut microseconds = 0;
        for (source, field) in enumerated_counter_fields() {
            let quantity = CounterQuantity::of_field(field);
            let otel = otel_counter(source, field).expect("named for OTel");
            let prometheus = prometheus_counter(source, field).expect("named for Prometheus");
            // Derived rows have no name in either table, so there is nothing to
            // cross-check them against — the field name is the only evidence and
            // this test would be circular. Both of them are `_us`.
            let Some(otel_name) = otel.export.name() else {
                assert_eq!(quantity, CounterQuantity::Microseconds);
                continue;
            };
            let prometheus_name = prometheus.export.name().expect("agreed to be a series");

            microseconds += usize::from(quantity == CounterQuantity::Microseconds);
            assert_eq!(
                quantity == CounterQuantity::Microseconds,
                otel_name.ends_with(".duration.sum"),
                "{source:?}::{field} is a {quantity:?} counter named {otel_name}"
            );
            assert_eq!(
                quantity == CounterQuantity::Microseconds,
                prometheus_name.ends_with("_microseconds"),
                "{source:?}::{field} is a {quantity:?} counter named {prometheus_name}"
            );
        }
        assert!(
            microseconds > 0,
            "no counter is a microsecond total, so the derivation is untested"
        );
    }

    /// Two tenants with work on one cell produce **one** series per counter,
    /// carrying the sum.
    ///
    /// The failure this rules out is silent in a way the `/metrics` one is not.
    /// `admin::tests::per_cell_counters_are_summed_across_scopes` guards a
    /// duplicate series in a scrape, which Prometheus rejects outright; here the
    /// series identity is the label set, so publishing per scope would make the
    /// last tenant recorded *overwrite* the others and the cell would report one
    /// tenant's traffic as its own — a plausible number, permanently low, with
    /// nothing anywhere to say so.
    #[test]
    fn two_scopes_on_one_cell_sum_rather_than_overwrite() {
        let shards = two_tenants_on_one_cell_and_one_alone();
        let totals = shard_counter_totals(&shards);
        let cells: Vec<&str> = totals.iter().map(|(cell_id, _)| *cell_id).collect();
        assert_eq!(
            cells,
            vec!["cell-a", "cell-b"],
            "one entry per cell, whatever the tenant count"
        );

        let value = |cell: &str, field: &str| {
            totals
                .iter()
                .find(|(cell_id, _)| *cell_id == cell)
                .and_then(|(_, fields)| fields.iter().find(|(name, _)| *name == field))
                .map(|(_, value)| *value)
                .unwrap_or_else(|| panic!("no {field} for {cell} in {totals:#?}"))
        };
        assert_eq!(value("cell-a", "write_attempts"), 7, "3 + 4, not 4");
        assert_eq!(value("cell-a", "gc_duration_us"), 790, "700 + 90");
        assert_eq!(value("cell-b", "write_attempts"), 5);

        // Every enumerated field is present for every cell, including the ones
        // that are zero: the recording loop drives off this, so a field dropped
        // here is a series that silently stops rather than one that reads zero.
        let enumerated = GraphOperationalMetricsSnapshot::default()
            .counter_fields()
            .count();
        for (cell_id, fields) in &totals {
            assert_eq!(fields.len(), enumerated, "{cell_id} lost a counter field");
        }
    }

    /// No shard, no series — not a row of zeroes.
    ///
    /// A node that has opened no scope yet must publish nothing, because an
    /// observable counter reporting a zero for a label set that does not exist is
    /// how a dashboard grows rows for cells that were never hosted here.
    #[test]
    fn a_node_with_no_shards_publishes_no_counter_series() {
        assert!(shard_counter_totals(&[]).is_empty());
    }
}
