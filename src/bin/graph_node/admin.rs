#[cfg(any(target_os = "linux", target_os = "macos"))]
#[path = "admin/cpu_profile.rs"]
mod cpu_profile;
#[path = "admin/memory_sampler.rs"]
pub(crate) mod memory_sampler;
#[path = "admin/resource_profile.rs"]
mod resource_profile;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use hydradb::{
    CellOperationalCounterTotals, ClientQueryService, CypherEngineMode, CypherEngineSelection, DurationHistogramSnapshot,
    ExperimentalOperatorMetricsSnapshot, GraphCacheMetricsSnapshot, GraphOperationalMetricsSnapshot, MutationAtomicityPolicy,
    MutationAtomicitySelection, Result, ScopedGraphShardRuntimeMetrics, ScopedRoutedGraphCluster,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::otel_metrics::{CounterSource, ExportUnit, FieldSource};
use crate::readiness::NodeReadiness;

/// One row of the Prometheus name table.
///
/// The counterpart of `crate::otel_metrics::OtelHistogram`, kept separate
/// deliberately: two independent tables over one enumeration is what lets
/// `every_histogram_field_reaches_both_exports` mean something. One shared
/// table would make the test vacuous.
pub struct PrometheusHistogram {
    /// The kernel's Rust identifier — the key both name tables are keyed by.
    pub field: &'static str,
    /// Series name stem. `{name}_bucket`, `{name}_sum` and `{name}_count` are
    /// derived from it, which is the shape `histogram_quantile` expects.
    pub name: &'static str,
    /// Bound and sum unit. Must match the OTel row's, and a test says so.
    pub unit: ExportUnit,
    /// Whether this binary has anything to render into the family.
    ///
    /// Must match the OTel row's, and
    /// `crate::otel_metrics::tests::only_the_transport_histograms_declare_no_graph_node_source`
    /// says so. Twenty-six of the twenty-eight rows below are
    /// [`FieldSource::GraphNode`]; the other two are named and rendered but have
    /// no source in this process, which is a property of the *binary* and not of
    /// the endpoint — see [`FieldSource`].
    pub source: FieldSource,
    /// Whether this family is rendered with the `scope` label.
    ///
    /// `scope` is the unbounded tenant root, so a family that carries it costs
    /// one series per bucket *per tenant*: twenty-one buckets across four
    /// thousand scopes is a cardinality bill nothing new should be signing.
    /// `query_rows_latency` carries it for history — `8d7e939` matched it to
    /// the counters it sits beside — and
    /// `tests::only_the_pre_existing_families_carry_a_scope_label` pins the
    /// closed list. Anything added after that point renders per `cell_id`
    /// alone, which is what `false` here buys.
    pub carries_scope: bool,
}

/// The Prometheus name table. One row per histogram the kernel enumerates.
///
/// `graph_*` throughout, because that is what every existing series on this
/// endpoint is called and a scraper's relabelling rules key off the prefix. The
/// OTel vocabulary (`db.*`/`hydradb.*`) is a separate decision living in a
/// separate table; see `crate::otel_metrics`.
///
/// The unit suffix is part of the name on purpose. Prometheus convention wants
/// seconds, and one of these families is in seconds because semconv fixes it
/// there — the rest stay in microseconds so the two exports report the same
/// numbers, and `_microseconds` says so where `_us` would invite a guess.
/// The two execution families each have one row per engine. They are three
/// series of one family, told apart by [`CYPHER_ENGINE_LABEL`] and not by
/// their name, so a dashboard sees the family it always saw; what the split
/// buys is that the label comes from the field rather than from the node at
/// scrape time. See [`cypher_engine_label`].
pub const PROMETHEUS_HISTOGRAMS: &[PrometheusHistogram] = &[
    PrometheusHistogram {
        field: "read_latency_legacy",
        name: "graph_client_operation_read_duration_seconds",
        unit: ExportUnit::Seconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "read_latency_experimental",
        name: "graph_client_operation_read_duration_seconds",
        unit: ExportUnit::Seconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "read_latency_datafusion",
        name: "graph_client_operation_read_duration_seconds",
        unit: ExportUnit::Seconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "write_latency_legacy",
        name: "graph_client_operation_write_duration_seconds",
        unit: ExportUnit::Seconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "write_latency_experimental",
        name: "graph_client_operation_write_duration_seconds",
        unit: ExportUnit::Seconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "write_latency_datafusion",
        name: "graph_client_operation_write_duration_seconds",
        unit: ExportUnit::Seconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    // Read-your-writes latency, and the instrument change 4 of
    // the cell-affine read routing design exists to add. In
    // microseconds rather than seconds because it is a HydraDB metric and not a
    // semconv one — only `db.client.operation.duration` is fixed in seconds —
    // and because the two counters it sits beside are counts of the same waits,
    // so keeping the family in the kernel's own unit means no boundary
    // conversion stands between the histogram and its numerators.
    PrometheusHistogram {
        field: "bookmark_wait_latency",
        name: "graph_client_bookmark_wait_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "query_rows_latency",
        name: "graph_query_rows_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: true,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "query_property_fetch_latency",
        name: "graph_query_property_fetch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "create_relationships_batch_latency",
        name: "graph_create_relationships_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "delete_relationship_mutations_batch_latency",
        name: "graph_delete_relationship_mutations_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "delete_vertices_and_isolated_candidates_batch_latency",
        name: "graph_delete_vertices_and_isolated_candidates_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "detach_delete_vertices_batch_latency",
        name: "graph_detach_delete_vertices_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "merge_relationships_batch_latency",
        name: "graph_merge_relationships_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "merge_vertex_metadata_batch_latency",
        name: "graph_merge_vertex_metadata_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "reserve_edge_delete_noops_batch_latency",
        name: "graph_reserve_edge_delete_noops_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    // Write entry points that commit outside the durability pipeline, so
    // their volume and latency can be compared with the pipelined families.
    PrometheusHistogram {
        field: "set_edge_metadata_latency",
        name: "graph_set_edge_metadata_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "set_edge_metadata_batch_latency",
        name: "graph_set_edge_metadata_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "write_edge_latency",
        name: "graph_write_edge_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "write_edge_mutations_batch_latency",
        name: "graph_write_edge_mutations_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "create_relationship_latency",
        name: "graph_create_relationship_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "import_vertex_metadata_batch_latency",
        name: "graph_import_vertex_metadata_batch_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    // Whole-request latency of experimental queries split by what chose the
    // access path, so `histogram_quantile` reads p95 per source. Keyed by
    // `cell_id` alone, like the write-batch families: the source is the
    // field, never a label value derived from the query.
    PrometheusHistogram {
        field: "query_experimental_access_statistics_latency",
        name: "graph_query_experimental_access_statistics_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "query_experimental_access_probe_latency",
        name: "graph_query_experimental_access_probe_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "query_experimental_access_guard_latency",
        name: "graph_query_experimental_access_guard_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "query_experimental_access_default_latency",
        name: "graph_query_experimental_access_default_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "rpc_latency",
        name: "graph_query_transport_rpc_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::TransportOnly,
    },
    PrometheusHistogram {
        field: "serve_latency",
        name: "graph_query_transport_serve_duration_microseconds",
        unit: ExportUnit::Microseconds,
        carries_scope: false,
        source: FieldSource::TransportOnly,
    },
];

/// The Prometheus name and unit for a kernel field identifier.
pub fn prometheus_histogram(field: &str) -> Option<&'static PrometheusHistogram> {
    PROMETHEUS_HISTOGRAMS.iter().find(|export| export.field == field)
}

/// How one counter is rendered on `/metrics`.
///
/// The variant carries the series name, so a counter that is rendered by no
/// series of its own cannot be given one by accident: there is nowhere to put
/// the string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrometheusCounterExport {
    /// One unlabelled process-global series.
    Global(&'static str),
    /// One series per `cell_id`, **summed over every scope open on the node**.
    ///
    /// Summed rather than split because the alternative is a `scope` label, and
    /// §1.4 of the metrics plan is explicit that `scope` is the unbounded tenant
    /// root: a node holding *T* scopes would multiply every one of these
    /// families by *T*. Emitting one series per cell and not per (scope, cell)
    /// is also not optional — two scopes hosting the same `cell_id` would
    /// otherwise render the same series name and label set twice with different
    /// values in one scrape, which Prometheus rejects outright.
    ///
    /// The sum is over every scope that has *ever* been open on the node, not
    /// only the ones open now: a closed scope's final values stay in the total
    /// (`ScopedRoutedGraphCluster::cell_operational_counter_totals`). A sum over
    /// open scopes alone falls when one closes, and `rate()`/`increase()` do
    /// not undercount across a fall — they read it as a reset to zero and
    /// count the whole remaining sum again as new increase. In practice
    /// that turned scope evictions into phantom spikes of thousands
    /// of `graph_write_commits` a minute with no client writes at all.
    PerCell(&'static str),
    /// One series per `{scope, cell_id}`.
    ///
    /// Three families only, and no new row may use it. They predate the label
    /// decision, they are scraped today, and §1.4 chose to leave them rather
    /// than break every dashboard built on them — see the module note on
    /// `append_node_metrics`.
    ScopePerCell(&'static str),
    /// Rendered by no series of its own.
    ///
    /// The counter is a restatement of the `_sum` of the named histogram
    /// families, which this endpoint already emits. For
    /// `query_rows_duration_us` that is not merely redundant but impossible to
    /// name well: the obvious name is the histogram family's own stem, and a
    /// `# TYPE … counter` on a name already declared `histogram` is a scrape
    /// error rather than an extra series.
    Derived(&'static [&'static str]),
}

impl PrometheusCounterExport {
    /// The series name, or `None` for a derived counter.
    pub fn name(self) -> Option<&'static str> {
        match self {
            Self::Global(name) | Self::PerCell(name) | Self::ScopePerCell(name) => Some(name),
            Self::Derived(_) => None,
        }
    }
}

/// One row of the Prometheus counter name table.
pub struct PrometheusCounter {
    pub source: CounterSource,
    /// The kernel's Rust identifier — the key both name tables are keyed by.
    pub field: &'static str,
    pub export: PrometheusCounterExport,
}

/// The Prometheus counter name table. One row per counter the kernel
/// enumerates, across all three snapshot types.
///
/// `graph_*` throughout, and no `_total` suffix anywhere: neither is idiomatic
/// Prometheus, and both are what the eight series this endpoint already serves
/// look like. Consistency with the endpoint beats consistency with the
/// convention, because a scraper's relabelling rules are written against the
/// endpoint.
///
/// The `_us` counters are cumulative microsecond sums and are named
/// `_microseconds`, matching the unit suffix [`PROMETHEUS_HISTOGRAMS`] uses and
/// for the same reason: a series whose unit has to be guessed is a series read
/// off by a factor of a million.
///
/// Rows appear in each snapshot's declaration order, which is also the order
/// `counter_fields()` yields — the rendering drives off the enumeration and only
/// consults this table for the name, so the order here is documentation rather
/// than behaviour.
pub const PROMETHEUS_COUNTERS: &[PrometheusCounter] = &[
    // `ClientQueryMetricsSnapshot`. Five of these are the pre-existing series
    // and keep their exact names: `graph_query_started`, `_completed`,
    // `_failed`, `graph_query_auth_failures`, `graph_query_scope_denials`. The
    // six new ones take `graph_client_*`, which is the prefix the client
    // histogram families already use, rather than extending a `graph_query_*`
    // vocabulary that the shard's own `query_rows_*` counters would collide
    // with — `rows_returned` here and `query_rows_returned` there are different
    // measurements one hop apart.
    PrometheusCounter {
        source: CounterSource::Client,
        field: "queries_started",
        export: PrometheusCounterExport::Global("graph_query_started"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "queries_completed",
        export: PrometheusCounterExport::Global("graph_query_completed"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "queries_failed",
        export: PrometheusCounterExport::Global("graph_query_failed"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "rows_returned",
        export: PrometheusCounterExport::Global("graph_client_rows_returned"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "auth_failures",
        export: PrometheusCounterExport::Global("graph_query_auth_failures"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "scope_denials",
        export: PrometheusCounterExport::Global("graph_query_scope_denials"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "cancellations",
        export: PrometheusCounterExport::Global("graph_client_cancellations"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "backpressure_waits",
        export: PrometheusCounterExport::Global("graph_client_backpressure_waits"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "prepare_requests",
        export: PrometheusCounterExport::Global("graph_client_prepare_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "prepare_duration_us",
        export: PrometheusCounterExport::Global("graph_client_prepare_duration_microseconds"),
    },
    // The bookmark-wait family: one denominator and four fractions of it, all
    // `Global` because the wait is a property of the *node* the read landed on
    // and not of the cell it asked for — the whole point of
    // the cell-affine read routing design is that two nodes
    // serving the same cell answer this differently. A `cell_id` label would
    // therefore split the one series an operator wants to read.
    //
    // `graph_client_bookmark_waits_polled / graph_client_bookmark_waits` is the
    // alert: ~0 in steady state once `GRAPH_READ_ROUTING=owner`, non-zero only
    // around a writer handoff. `..._off_cell_writer / graph_client_bookmark_waits`
    // is "reads are landing on non-owners", which was an inference before this
    // and is now a query.
    PrometheusCounter {
        source: CounterSource::Client,
        field: "bookmark_waits",
        export: PrometheusCounterExport::Global("graph_client_bookmark_waits"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_polled",
        export: PrometheusCounterExport::Global("graph_client_bookmark_waits_polled"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_declined",
        export: PrometheusCounterExport::Global("graph_client_bookmark_waits_declined"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_on_cell_writer",
        export: PrometheusCounterExport::Global("graph_client_bookmark_waits_on_cell_writer"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "bookmark_waits_off_cell_writer",
        export: PrometheusCounterExport::Global("graph_client_bookmark_waits_off_cell_writer"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "admission_wait_us",
        export: PrometheusCounterExport::Global("graph_client_admission_wait_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "serialize_duration_us",
        export: PrometheusCounterExport::Global("graph_client_serialize_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "serialized_rows",
        export: PrometheusCounterExport::Global("graph_client_serialized_rows"),
    },
    // Derived: the kernel builds it from `read_latency.sum_us +
    // write_latency.sum_us`, and both families already publish a `_sum`.
    PrometheusCounter {
        source: CounterSource::Client,
        field: "execution_duration_us",
        export: PrometheusCounterExport::Derived(&[
            "graph_client_operation_read_duration_seconds",
            "graph_client_operation_write_duration_seconds",
        ]),
    },
    // `GraphOperationalMetricsSnapshot`.
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "write_attempts",
        export: PrometheusCounterExport::PerCell("graph_write_attempts"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "write_commits",
        export: PrometheusCounterExport::PerCell("graph_write_commits"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "write_retries",
        export: PrometheusCounterExport::PerCell("graph_write_retries"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "bulk_import_batches_profiled",
        export: PrometheusCounterExport::PerCell("graph_bulk_import_batches_profiled"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "bulk_import_preflight_us",
        export: PrometheusCounterExport::PerCell("graph_bulk_import_preflight_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "bulk_import_batch_build_us",
        export: PrometheusCounterExport::PerCell("graph_bulk_import_batch_build_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "bulk_import_counter_read_us",
        export: PrometheusCounterExport::PerCell("graph_bulk_import_counter_read_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "bulk_import_commit_us",
        export: PrometheusCounterExport::PerCell("graph_bulk_import_commit_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_batches_profiled",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_batches_profiled"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_endpoint_check_us",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_endpoint_check_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_scan_us",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_identity_scan_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_pointer_hits",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_identity_pointer_hits"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_pointer_misses",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_identity_pointer_misses"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_record_read_us",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_record_read_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_structural_check_us",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_structural_check_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_segment_scans",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_segment_scans"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_segment_neighbors",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_segment_neighbors"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_counter_read_us",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_counter_read_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_commit_us",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_commit_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_idempotency_replays",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_idempotency_replays"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_nochange_exits",
        export: PrometheusCounterExport::PerCell("graph_merge_vertex_metadata_nochange_exits"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_all_replays",
        export: PrometheusCounterExport::PerCell("graph_delete_vertex_batch_all_replays"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_batches_profiled",
        export: PrometheusCounterExport::PerCell("graph_merge_vertex_metadata_batches_profiled"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_batch_items",
        export: PrometheusCounterExport::PerCell("graph_merge_vertex_metadata_batch_items"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_read_us",
        export: PrometheusCounterExport::PerCell("graph_merge_vertex_metadata_read_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "merge_vertex_metadata_txn_us",
        export: PrometheusCounterExport::PerCell("graph_merge_vertex_metadata_txn_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_batches_profiled",
        export: PrometheusCounterExport::PerCell("graph_delete_vertex_batch_batches_profiled"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_items",
        export: PrometheusCounterExport::PerCell("graph_delete_vertex_batch_items"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_read_us",
        export: PrometheusCounterExport::PerCell("graph_delete_vertex_batch_read_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "delete_vertex_batch_txn_us",
        export: PrometheusCounterExport::PerCell("graph_delete_vertex_batch_txn_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_batches_profiled",
        export: PrometheusCounterExport::PerCell("graph_reserve_edge_delete_noops_batches_profiled"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_batch_items",
        export: PrometheusCounterExport::PerCell("graph_reserve_edge_delete_noops_batch_items"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_read_us",
        export: PrometheusCounterExport::PerCell("graph_reserve_edge_delete_noops_read_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_txn_us",
        export: PrometheusCounterExport::PerCell("graph_reserve_edge_delete_noops_txn_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "artifact_builds_started",
        export: PrometheusCounterExport::PerCell("graph_artifact_builds_started"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "artifact_builds_completed",
        export: PrometheusCounterExport::PerCell("graph_artifact_builds_completed"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "artifact_build_duration_us",
        export: PrometheusCounterExport::PerCell("graph_artifact_build_duration_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "artifact_publish_batches",
        export: PrometheusCounterExport::PerCell("graph_artifact_publish_batches"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "artifact_records_published",
        export: PrometheusCounterExport::PerCell("graph_artifact_records_published"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "artifact_publish_duration_us",
        export: PrometheusCounterExport::PerCell("graph_artifact_publish_duration_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "gc_jobs_started",
        export: PrometheusCounterExport::PerCell("graph_gc_jobs_started"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "gc_jobs_completed",
        export: PrometheusCounterExport::PerCell("graph_gc_jobs_completed"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "gc_keys_deleted",
        export: PrometheusCounterExport::PerCell("graph_gc_keys_deleted"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "gc_duration_us",
        export: PrometheusCounterExport::PerCell("graph_gc_duration_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "verifier_runs",
        export: PrometheusCounterExport::PerCell("graph_verifier_runs"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "verifier_failures",
        export: PrometheusCounterExport::PerCell("graph_verifier_failures"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "verifier_duration_us",
        export: PrometheusCounterExport::PerCell("graph_verifier_duration_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_rows_started",
        export: PrometheusCounterExport::PerCell("graph_query_rows_started"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_rows_completed",
        export: PrometheusCounterExport::PerCell("graph_query_rows_completed"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_rows_failed",
        export: PrometheusCounterExport::PerCell("graph_query_rows_failed"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_rows_returned",
        export: PrometheusCounterExport::PerCell("graph_query_rows_returned"),
    },
    // Derived, and the row that forced the variant to exist: the kernel sets it
    // from `query_rows_latency.sum_us`, and the histogram family is already
    // named `graph_query_rows_duration_microseconds`.
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_rows_duration_us",
        export: PrometheusCounterExport::Derived(&["graph_query_rows_duration_microseconds"]),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_property_seek_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_property_seek_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_relationship_expand_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_relationship_expand_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_ordered_property_scan_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_ordered_property_scan_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_parse_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_parse_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_lower_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_lower_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_bind_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_bind_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_snapshot_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_snapshot_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_statistics_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_statistics_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_plan_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_plan_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_execute_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_execute_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_storage_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_storage_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_storage_calls",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_storage_calls"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_result_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_result_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_sampled_plans",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_sampled_plans"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_statistics_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_statistics_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_statistics_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_statistics_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_probe_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_probe_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_probe_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_probe_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_guard_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_guard_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_guard_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_guard_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_default_requests",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_default_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_default_us",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_default_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_guard_stops",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_guard_stops"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_planned_without_statistics",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_planned_without_statistics"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_reloads",
        export: PrometheusCounterExport::PerCell("graph_planner_statistics_reloads"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_load_failures",
        export: PrometheusCounterExport::PerCell("graph_planner_statistics_load_failures"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_corrupt_ignored",
        export: PrometheusCounterExport::PerCell("graph_planner_statistics_corrupt_ignored"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "planner_statistics_hints_sent",
        export: PrometheusCounterExport::PerCell("graph_planner_statistics_hints_sent"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_rows_estimated",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_rows_estimated"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_rows_actual",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_rows_actual"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_underestimates",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_underestimates"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_access_overestimates",
        export: PrometheusCounterExport::PerCell("graph_query_experimental_access_overestimates"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_route_legacy_requests",
        export: PrometheusCounterExport::PerCell("graph_query_route_legacy_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_route_experimental_requests",
        export: PrometheusCounterExport::PerCell("graph_query_route_experimental_requests"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_route_native_path_fallbacks",
        export: PrometheusCounterExport::PerCell("graph_query_route_native_path_fallbacks"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_route_mutation_fallbacks",
        export: PrometheusCounterExport::PerCell("graph_query_route_mutation_fallbacks"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_property_fetches",
        export: PrometheusCounterExport::PerCell("graph_query_property_fetches"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_artifact_lookup_us",
        export: PrometheusCounterExport::PerCell("graph_query_artifact_lookup_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_cache_us",
        export: PrometheusCounterExport::PerCell("graph_query_graphblas_cache_microseconds"),
    },
    // The three pre-existing shard series. `ScopePerCell` and nothing else.
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_artifact_snapshots",
        export: PrometheusCounterExport::ScopePerCell("graph_query_graphblas_artifact_snapshots"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_graphblas_rebuilt_snapshots",
        export: PrometheusCounterExport::ScopePerCell("graph_query_graphblas_rebuilt_snapshots"),
    },
    // One row per plan shape, all `PerCell` because a bad plan belongs to the
    // tenant whose data shape produced it. `graph_query_plans_total` is the
    // denominator: the other four are fractions of it, and none of them sums
    // with the others, since one plan can both seek and scan.
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_plans_total",
        export: PrometheusCounterExport::PerCell("graph_query_plans_total"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_label_scan",
        export: PrometheusCounterExport::PerCell("graph_query_plans_with_label_scan"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_property_index",
        export: PrometheusCounterExport::PerCell("graph_query_plans_with_property_index"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_full_scan",
        export: PrometheusCounterExport::PerCell("graph_query_plans_with_full_scan"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_plans_with_equality_pushdown",
        export: PrometheusCounterExport::PerCell("graph_query_plans_with_equality_pushdown"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_rust_sparse_fallbacks",
        export: PrometheusCounterExport::ScopePerCell("graph_query_rust_sparse_fallbacks"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "graph_compute_tasks",
        export: PrometheusCounterExport::PerCell("graph_compute_tasks"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "graph_compute_queue_us",
        export: PrometheusCounterExport::PerCell("graph_compute_queue_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "graph_compute_duration_us",
        export: PrometheusCounterExport::PerCell("graph_compute_duration_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "gds_assemblies",
        export: PrometheusCounterExport::PerCell("graph_gds_assemblies"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "backpressure_waits",
        export: PrometheusCounterExport::PerCell("graph_backpressure_waits"),
    },
    // The graph write permit. `PerCell` because the permit *is*
    // per cell: `max_concurrent_graph_writes` is one per shard, so
    // `rate(graph_write_permit_held_microseconds[5m]) / 1e6` is the fraction
    // of wall time the cell's single writer slot was occupied. A `scope`
    // label is ruled out by the same §1.4 cardinality decision as every
    // other row here.
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "write_permit_acquisitions",
        export: PrometheusCounterExport::PerCell("graph_write_permit_acquisitions"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "write_permit_wait_us",
        export: PrometheusCounterExport::PerCell("graph_write_permit_wait_microseconds"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "write_permit_held_us",
        export: PrometheusCounterExport::PerCell("graph_write_permit_held_microseconds"),
    },
    // `GraphCacheMetricsSnapshot`. The nineteen counters that reached nothing
    // at all before M2, under the `graph_cache_*` prefix the two existing cache
    // gauges already use.
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "matrix_artifact_hits",
        export: PrometheusCounterExport::PerCell("graph_cache_matrix_artifact_hits"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "matrix_artifact_misses",
        export: PrometheusCounterExport::PerCell("graph_cache_matrix_artifact_misses"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "matrix_adjacency_hits",
        export: PrometheusCounterExport::PerCell("graph_cache_matrix_adjacency_hits"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "matrix_adjacency_misses",
        export: PrometheusCounterExport::PerCell("graph_cache_matrix_adjacency_misses"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "graphblas_hits",
        export: PrometheusCounterExport::PerCell("graph_cache_graphblas_hits"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "graphblas_misses",
        export: PrometheusCounterExport::PerCell("graph_cache_graphblas_misses"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "parsed_row_query_hits",
        export: PrometheusCounterExport::PerCell("graph_cache_parsed_row_query_hits"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "parsed_row_query_misses",
        export: PrometheusCounterExport::PerCell("graph_cache_parsed_row_query_misses"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "relationship_rows_hits",
        export: PrometheusCounterExport::PerCell("graph_cache_relationship_rows_hits"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "relationship_rows_misses",
        export: PrometheusCounterExport::PerCell("graph_cache_relationship_rows_misses"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "relationship_property_rows_hits",
        export: PrometheusCounterExport::PerCell("graph_cache_relationship_property_rows_hits"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "relationship_property_rows_misses",
        export: PrometheusCounterExport::PerCell("graph_cache_relationship_property_rows_misses"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "insertions",
        export: PrometheusCounterExport::PerCell("graph_cache_insertions"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "evictions",
        export: PrometheusCounterExport::PerCell("graph_cache_evictions"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "pinned_insertions",
        export: PrometheusCounterExport::PerCell("graph_cache_pinned_insertions"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "tenant_quota_rejections",
        export: PrometheusCounterExport::PerCell("graph_cache_tenant_quota_rejections"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "hydration_started",
        export: PrometheusCounterExport::PerCell("graph_cache_hydration_started"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "hydration_waited",
        export: PrometheusCounterExport::PerCell("graph_cache_hydration_waited"),
    },
    PrometheusCounter {
        source: CounterSource::ShardCache,
        field: "hydration_completed",
        export: PrometheusCounterExport::PerCell("graph_cache_hydration_completed"),
    },
];

/// The Prometheus names for the counters dimensioned by `error.class`.
///
/// Named as an explicit breakdown of the scalar they sum to —
/// `graph_query_failed_by_class` against `graph_query_failed` — so that the
/// relationship an operator has to trust is stated in the name. It is true by
/// construction: `record_query_rows_failure` increments both.
pub const PROMETHEUS_CLASS_COUNTERS: &[PrometheusCounter] = &[
    PrometheusCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_class",
        export: PrometheusCounterExport::Global("graph_query_failed_by_class"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_rows_failed_by_class",
        export: PrometheusCounterExport::PerCell("graph_query_rows_failed_by_class"),
    },
];

/// The Prometheus names for the query-failure counters, dimensioned by stage
/// and failure reason.
///
/// Named as a breakdown by *reason* beside `graph_query_failed_by_class`, and
/// not as a breakdown of `graph_query_failed`: this family also counts the
/// prepare step, which that scalar never sees, so the two do not sum to each
/// other and a name implying they do would be a trap.
pub const PROMETHEUS_FAILURE_COUNTERS: &[PrometheusCounter] = &[
    PrometheusCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_reason_legacy",
        export: PrometheusCounterExport::Global("graph_query_failed_by_reason"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_reason_experimental",
        export: PrometheusCounterExport::Global("graph_query_failed_by_reason"),
    },
    PrometheusCounter {
        source: CounterSource::Client,
        field: "queries_failed_by_reason_datafusion",
        export: PrometheusCounterExport::Global("graph_query_failed_by_reason"),
    },
];

/// The Prometheus row for a `(source, field)` pair, over every counter table.
///
/// Keyed by the pair and not by the identifier: `backpressure_waits` is a field
/// of two different snapshots.
pub fn prometheus_counter(source: CounterSource, field: &str) -> Option<&'static PrometheusCounter> {
    PROMETHEUS_COUNTERS
        .iter()
        .chain(PROMETHEUS_CLASS_COUNTERS)
        .chain(PROMETHEUS_FAILURE_COUNTERS)
        .find(|export| export.source == source && export.field == field)
}

/// The label an error-class-dimensioned series carries.
///
/// `error_class`, **not** the registry's `error.class`. A Prometheus label name
/// must match `[a-zA-Z_][a-zA-Z0-9_]*`; a dot is a parse error, not a stylistic
/// choice. So the one attribute that is a metric label in both vocabularies is
/// spelled differently in each, and this constant is where that is written down
/// rather than discovered from a rejected scrape.
const ERROR_CLASS_LABEL: &str = "error_class";

/// The labels a failure-reason series carries beside the engine. Both are
/// closed vocabularies: `QueryFailureStage::as_str` and
/// `QueryFailureReason::as_str`. There is no `error_class`: the family counts
/// only the `query` class.
const FAILURE_STAGE_LABEL: &str = "stage";
const FAILURE_REASON_LABEL: &str = "reason";

/// The label the client latency histograms carry for the Cypher engine that
/// ran the statements in them.
///
/// `cypher_engine` here, `hydradb.cypher_engine` on the OTLP side: same
/// convention as `cell_id`, the Prometheus name being the registry key without
/// its namespace. The value is from a fixed engine-mode set, so it costs at most
/// three series per family per node and buys a cross-engine
/// overlay that needs no join on `instance`.
const CYPHER_ENGINE_LABEL: &str = "cypher_engine";

/// The engine a client histogram field holds the statements of, or `None` for
/// a field that carries no [`CYPHER_ENGINE_LABEL`] at all.
///
/// The value is a property of the **field**, not of the node: a node that has
/// served multiple engines — which the kill switch allows — has multiple populations,
/// and reading the label off the service at scrape time would hand all of them to
/// whichever engine is effective when Prometheus happens to ask. Shared with
/// the OTLP export through [`hydradb::ClientQueryMetricsSnapshot`]'s field
/// names so the two exports cannot label a family differently.
pub fn cypher_engine_label(field: &str) -> Option<&'static str> {
    match field {
        "read_latency_legacy" | "write_latency_legacy" | "queries_failed_by_reason_legacy" => {
            Some(hydradb::CypherEngineMode::Legacy.as_str())
        }
        "read_latency_experimental" | "write_latency_experimental" | "queries_failed_by_reason_experimental" => {
            Some(hydradb::CypherEngineMode::Experimental.as_str())
        }
        "read_latency_datafusion" | "write_latency_datafusion" | "queries_failed_by_reason_datafusion" => Some("datafusion"),
        _ => None,
    }
}

/// Whether a client histogram field carries [`CYPHER_ENGINE_LABEL`]: the
/// execution families, and only those. Bookmark wait is time spent before a
/// query is admitted, which no engine choice can move.
pub fn carries_cypher_engine(field: &str) -> bool {
    cypher_engine_label(field).is_some()
}

#[derive(Clone)]
struct AdminState {
    ready: NodeReadiness,
    query: ClientQueryService,
    routed_node: Arc<ScopedRoutedGraphCluster>,
    memory_sampler: Arc<memory_sampler::MemorySampler>,
    /// The node's graph auth token. Controls and scope diagnostics check it;
    /// the probes and `/metrics` stay unauthenticated.
    control_token: Arc<str>,
    /// Buddy WAL's series, when the node runs it (`GRAPH_BUDDY_WAL`). Off,
    /// `/metrics` carries no `graph_buddy_wal_*` family at all.
    buddy_wal: Option<Arc<hydradb_buddy_wal::BuddyWalMetrics>>,
}

pub struct AdminServer {
    local_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl AdminServer {
    pub async fn bind_scoped(
        addr: SocketAddr,
        ready: NodeReadiness,
        query: ClientQueryService,
        node: Arc<ScopedRoutedGraphCluster>,
        control_token: String,
        buddy_wal: Option<Arc<hydradb_buddy_wal::BuddyWalMetrics>>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(admin_io_error)?;
        let local_addr = listener.local_addr().map_err(admin_io_error)?;
        let state = AdminState {
            ready,
            query,
            routed_node: node,
            memory_sampler: memory_sampler::MemorySampler::start(),
            control_token: Arc::from(control_token),
            buddy_wal,
        };
        Self::serve(listener, local_addr, state)
    }

    fn serve(listener: TcpListener, local_addr: SocketAddr, state: AdminState) -> Result<Self> {
        let router = Router::new()
            .route("/livez", get(live))
            .route("/readyz", get(readiness))
            .route("/metrics", get(metrics))
            .route(
                "/debug/resource-profile",
                get(resource_profile::handle).head(|| async { StatusCode::METHOD_NOT_ALLOWED }),
            )
            .route("/debug/slatedb-cache", get(slatedb_cache_diagnostics))
            .route(CYPHER_ENGINE_ROUTE, get(cypher_engine_state).put(set_cypher_engine_override))
            .route(
                CYPHER_MUTATION_ATOMICITY_ROUTE,
                get(mutation_atomicity_state).put(set_mutation_atomicity_override),
            );
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let router = router.route(
            "/debug/pprof/profile",
            get(cpu_profile::handle).head(|| async { StatusCode::METHOD_NOT_ALLOWED }),
        );
        let router = router.with_state(state);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    while stop_rx.changed().await.is_ok() {
                        if *stop_rx.borrow() {
                            return;
                        }
                    }
                })
                .await
                .map_err(admin_io_error)
        });
        Ok(Self { local_addr, stop_tx, task })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        self.task.await.map_err(|err| hydradb::GraphError::CorruptValue {
            key: "runtime/admin".to_string(),
            reason: err.to_string(),
        })?
    }
}

async fn live() -> StatusCode {
    StatusCode::OK
}

/// The Cypher engine kill switch (Workstream 13).
///
/// `GET` reports `{configured, override, effective, experimental_compiled,
/// datafusion_compiled, mutation_atomicity}`.
/// `PUT` with `Authorization: Bearer <graph auth token>` and a body of
/// `{"override": "legacy" | "experimental" | "datafusion" | null}` sets or clears the
/// override for this node, with no restart, and answers with the new state.
/// `null` returns to `GRAPH_CYPHER_ENGINE`; so does a restart, because the
/// override is deliberately not persisted.
///
/// Statements prepared or executing when it flips keep the engine they were
/// admitted with. A binary built without an engine's feature will not accept
/// that engine as an override.
pub const CYPHER_ENGINE_ROUTE: &str = "/v1/cypher-engine";
/// Runtime mutation atomicity policy control route.
pub const CYPHER_MUTATION_ATOMICITY_ROUTE: &str = "/v1/cypher-mutation-atomicity";

fn cypher_engine_document(selection: CypherEngineSelection, mutation_atomicity: MutationAtomicitySelection) -> serde_json::Value {
    serde_json::json!({
        "configured": selection.configured.as_str(),
        "override": selection.override_mode.map(CypherEngineMode::as_str),
        "effective": selection.effective().as_str(),
        "experimental_compiled": cfg!(feature = "experimental-cypher-engine"),
        "datafusion_compiled": cfg!(feature = "datafusion-cypher-engine"),
        "mutation_atomicity": mutation_atomicity_document(mutation_atomicity),
    })
}

fn mutation_atomicity_document(selection: MutationAtomicitySelection) -> serde_json::Value {
    serde_json::json!({
        "configured": selection.configured.as_str(),
        "override": selection.override_policy.map(MutationAtomicityPolicy::as_str),
        "effective": selection.effective().as_str(),
    })
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [("content-type", "application/json"), ("cache-control", "no-store")],
        body.to_string(),
    )
        .into_response()
}

async fn slatedb_cache_diagnostics(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if !valid_control_bearer(&headers, &state.control_token) {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"error": "a bearer graph auth token is required"}),
        );
    }
    json_response(StatusCode::OK, cache_observe_document(&state.routed_node.slatedb_cache_metrics()))
}

fn cache_observe_document(cache: &hydradb::SlateDbCacheMetricsSnapshot) -> serde_json::Value {
    let observe = &cache.observe;
    let scopes: Vec<_> = observe
        .scopes
        .iter()
        .map(|scope| {
            serde_json::json!({
                    "path": scope.path,
            "pending_admissions": scope.pending_admissions,
                    "evictions": scope.evictions,
                    "evicted_bytes": scope.evicted_bytes,
                    "misses_after_eviction": scope.misses_after_eviction,
                    "reload_executions": scope.reload_executions,
                    "reload_bytes": scope.reload_bytes,
                    "reload_failures": scope.reload_failures,
                    "reload_duration_us": scope.reload_duration_us,
                    "resident_tracked": scope.resident_tracked,
                    "resident_bytes": scope.resident_bytes,
                    "ghost_records": scope.ghost_records,
                    "metadata_bytes_estimate": scope.metadata_bytes,
                    "resident_dropped": scope.resident_dropped,
                    "ghost_dropped": scope.ghost_dropped,
                    "ghost_expired": scope.ghost_expired,
                })
        })
        .collect();
    serde_json::json!({
        "enabled": observe.generation != 0,
        "generation": observe.generation,
        "cache": {
            "capacity_bytes": cache.capacity_bytes,
            "resident_bytes": cache.resident_bytes,
            "entries": cache.entries,
        },
        "limits": {
            "max_named_scopes": hydradb::MAX_OBSERVED_SCOPES,
            "max_scope_path_bytes": hydradb::MAX_SCOPE_PATH_BYTES,
            "max_metadata_bytes": observe.max_metadata_bytes,
            "max_resident_records_per_shard": observe.max_resident_records,
            "max_ghost_records_per_shard": observe.max_ghost_records,
            "ghost_retention_secs": observe.ghost_retention_secs,
        },
        "coverage": {
            "metadata_bytes_estimate": observe.metadata_bytes,
            "fixed_metadata_bytes_estimate": observe.fixed_metadata_bytes,
            "envelope_metadata_bytes_estimate": observe.envelope_metadata_bytes,
            "scope_overflow": observe.scope_overflow,
            "ordering_gaps": observe.ordering_gaps,
            "pending_admissions": observe.pending_admissions,
            "resident_dropped": observe.resident_overflow,
            "ghost_dropped": observe.ghost_dropped,
            "ghost_expired": observe.ghost_expired,
        },
        "scopes": scopes,
    })
}

async fn cypher_engine_state(State(state): State<AdminState>) -> Response {
    json_response(
        StatusCode::OK,
        cypher_engine_document(state.query.cypher_engine_selection(), state.query.mutation_atomicity_selection()),
    )
}

async fn set_cypher_engine_override(State(state): State<AdminState>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    if !valid_control_bearer(&headers, &state.control_token) {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"error": "a bearer graph auth token is required"}),
        );
    }
    let requested = match parse_engine_override(&body) {
        Ok(requested) => requested,
        Err(reason) => {
            let status = if reason.contains("requires the datafusion-cypher-engine Cargo feature") {
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::BAD_REQUEST
            };
            return json_response(status, serde_json::json!({"error": reason}));
        }
    };
    match state.query.set_cypher_engine_override(requested) {
        Ok((previous, selection)) => {
            tracing::warn!(
                cypher_engine.configured = selection.configured.as_str(),
                cypher_engine.previous_override = previous.map_or("none", CypherEngineMode::as_str),
                cypher_engine.override = requested.map_or("none", CypherEngineMode::as_str),
                cypher_engine.effective = selection.effective().as_str(),
                "cypher engine override changed"
            );
            json_response(
                StatusCode::OK,
                cypher_engine_document(selection, state.query.mutation_atomicity_selection()),
            )
        }
        Err(error) => json_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "error": error.to_string(),
                "state": cypher_engine_document(state.query.cypher_engine_selection(), state.query.mutation_atomicity_selection()),
            }),
        ),
    }
}

async fn mutation_atomicity_state(State(state): State<AdminState>) -> Response {
    json_response(
        StatusCode::OK,
        mutation_atomicity_document(state.query.mutation_atomicity_selection()),
    )
}

async fn set_mutation_atomicity_override(State(state): State<AdminState>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    if !valid_control_bearer(&headers, &state.control_token) {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"error": "a bearer graph auth token is required"}),
        );
    }
    let requested = match parse_mutation_atomicity_override(&body) {
        Ok(requested) => requested,
        Err(reason) => return json_response(StatusCode::BAD_REQUEST, serde_json::json!({"error": reason})),
    };
    let (previous, selection) = state.query.set_mutation_atomicity_override(requested);
    tracing::warn!(
        cypher_mutation_atomicity.configured = selection.configured.as_str(),
        cypher_mutation_atomicity.previous_override = previous.map_or("none", MutationAtomicityPolicy::as_str),
        cypher_mutation_atomicity.override = requested.map_or("none", MutationAtomicityPolicy::as_str),
        cypher_mutation_atomicity.effective = selection.effective().as_str(),
        "cypher mutation atomicity override changed"
    );
    json_response(StatusCode::OK, mutation_atomicity_document(selection))
}

/// `{"override": ...}` with the key required, so an empty or mistyped body
/// cannot silently clear an override an operator set on purpose.
fn parse_engine_override(body: &[u8]) -> std::result::Result<Option<CypherEngineMode>, String> {
    let document: serde_json::Value = serde_json::from_slice(body).map_err(|error| format!("body must be a JSON object: {error}"))?;
    let Some(value) = document.as_object().and_then(|object| object.get("override")) else {
        return Err(r#"body must be {"override": "legacy" | "experimental" | "datafusion" | null}"#.to_string());
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(mode) if mode == "legacy" => Ok(Some(CypherEngineMode::Legacy)),
        serde_json::Value::String(mode) if mode == "experimental" => Ok(Some(CypherEngineMode::Experimental)),
        serde_json::Value::String(mode) if mode == "datafusion" => {
            #[cfg(feature = "datafusion-cypher-engine")]
            {
                Ok(Some(CypherEngineMode::DataFusion))
            }
            #[cfg(not(feature = "datafusion-cypher-engine"))]
            {
                Err("DataFusion engine override requires the datafusion-cypher-engine Cargo feature".to_string())
            }
        }
        other => Err(format!(
            r#"override must be "legacy", "experimental", "datafusion" or null, not {other}"#
        )),
    }
}

fn parse_mutation_atomicity_override(body: &[u8]) -> std::result::Result<Option<MutationAtomicityPolicy>, String> {
    let document: serde_json::Value = serde_json::from_slice(body).map_err(|error| format!("body must be a JSON object: {error}"))?;
    let Some(value) = document.as_object().and_then(|object| object.get("override")) else {
        return Err(r#"body must be {"override": "statement" | "legacy_chunked" | null}"#.to_string());
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(policy) if policy == "statement" => Ok(Some(MutationAtomicityPolicy::Statement)),
        serde_json::Value::String(policy) if policy == "legacy_chunked" => Ok(Some(MutationAtomicityPolicy::LegacyChunked)),
        other => Err(format!(r#"override must be "statement", "legacy_chunked" or null, not {other}"#)),
    }
}

pub(crate) fn valid_control_bearer(headers: &HeaderMap, expected: &str) -> bool {
    use subtle::ConstantTimeEq as _;
    let Some((scheme, supplied)) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
    else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer")
        && !supplied.is_empty()
        && !expected.is_empty()
        && bool::from(supplied.as_bytes().ct_eq(expected.as_bytes()))
}

/// 200 exactly when the heartbeat publisher would publish.
///
/// That includes decision 7: a node whose heartbeat LIST has been failing past
/// the grace window has shed its view of the fleet, refuses every promotion, and
/// reports itself unready here as well — one signal, not two that can disagree.
/// `/livez` is unaffected, so k8s takes a shed node out of Service endpoints
/// without restarting it, which is what lets it recover when the store does.
async fn readiness(State(state): State<AdminState>) -> StatusCode {
    if state.ready.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics(State(state): State<AdminState>) -> Response {
    // First in the body, so `curl -s :9090/metrics | head -2` names the build
    // without a grep. Every other series below describes what this process is
    // doing; this one says which process it is.
    let mut output = hydradb_telemetry::build_info::prometheus_gauge();
    output.push_str(&format!(
        "# TYPE graph_runtime_ready gauge\ngraph_runtime_ready {}\n",
        u8::from(state.ready.is_ready()),
    ));
    output.push_str(&cypher_engine_gauge(&state.query));
    output.push_str(&mutation_atomicity_gauge(&state.query));
    let query = state.query.metrics();
    // The five pre-existing client series come out of this loop, under their
    // original names and in their original relative order, because the loop is
    // driven by `counter_fields()` and the kernel declares them in that order.
    // The six that were never exported are interleaved where the kernel
    // declares them; a scrape does not care about order and a diff shows them
    // as insertions.
    append_global_counters(&mut output, CounterSource::Client, query.counter_fields());
    append_global_class_counters(&mut output, CounterSource::Client, query.class_counter_fields());
    // Carries the engine label for the same reason the execution histograms
    // do, and takes it from the same place: the field. The
    // per-engine failure comparison is the point of the family, and a node
    // the kill switch has flipped can hold multiple populations.
    append_global_failure_counters(&mut output, CounterSource::Client, query.failure_counter_fields());
    // Additive, and deliberately after the counters rather than interleaved
    // with them: every series above keeps the exact name, labels and value it
    // had before the histograms existed.
    append_histogram_types(&mut output, query.histogram_fields());
    // The engine label goes on the execution families and nothing else, and
    // its value comes from the field: the per-engine histograms are separate
    // series of one family, and a node that was flipped holds every population.
    // Bookmark wait is time spent before a query is admitted, which no engine
    // choice can move, and its family is pinned label-free by
    // `the_bookmark_wait_family_renders`.
    let (engine_fields, plain_fields): (Vec<_>, Vec<_>) = query.histogram_fields().partition(|(field, _)| carries_cypher_engine(field));
    for (field, histogram) in engine_fields {
        let Some(engine) = cypher_engine_label(field) else {
            continue;
        };
        append_histograms(&mut output, std::iter::once((field, histogram)), &[(CYPHER_ENGINE_LABEL, engine)]);
    }
    append_histograms(&mut output, plain_fields.into_iter(), &[]);
    append_bounded_node_metrics(
        &mut output,
        state.routed_node.local_shard_runtime_metrics(),
        std::time::Duration::from_secs(1),
    )
    .await;
    append_per_cell_counters(&mut output, &state.routed_node.cell_operational_counter_totals());
    append_cache_counters(&mut output, &state.routed_node.graph_cache_metrics());
    prometheus::TextEncoder::new()
        .encode_utf8(&state.routed_node.writer_lease_directory().collect_metrics(), &mut output)
        .expect("writer lease collectors emit valid metric families");
    append_slatedb_cache_metrics(&mut output, state.routed_node.slatedb_cache_metrics());
    // The node's one disk cache, read from its own counters: not per shard,
    // and not inside the shard budget above.
    crate::otel_metrics::storage_caches::append(&mut output, state.routed_node.shared_disk_cache_metrics());
    crate::otel_metrics::memory_diagnostics::append(&mut output);
    crate::otel_metrics::remote_object_store::append(&mut output);
    state.memory_sampler.render(&mut output);
    if let Some(buddy_wal) = &state.buddy_wal {
        output.push_str(&buddy_wal.render());
    }
    (
        [
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        output,
    )
        .into_response()
}

/// One info-style series naming the configured and effective engine, so an
/// active kill-switch override is visible fleet-wide as
/// `configured != effective`. Two closed three-value labels: nine series at most.
fn cypher_engine_gauge(query: &ClientQueryService) -> String {
    let selection = query.cypher_engine_selection();
    format!(
        "# TYPE graph_cypher_engine gauge\ngraph_cypher_engine{{configured=\"{}\",effective=\"{}\"}} 1\n",
        selection.configured.as_str(),
        selection.effective().as_str(),
    )
}

/// One bounded gauge reporting the startup and effective mutation policy.
/// Both labels come from two-value enums, so this family has at most four
/// series per node regardless of statement volume.
fn mutation_atomicity_gauge(query: &ClientQueryService) -> String {
    let selection = query.mutation_atomicity_selection();
    format!(
        "# TYPE graph_cypher_mutation_atomicity gauge\ngraph_cypher_mutation_atomicity{{configured=\"{}\",effective=\"{}\"}} 1\n",
        selection.configured.as_str(),
        selection.effective().as_str(),
    )
}

/// A slow scope open must not hide process-wide OOM diagnostics. Missing shard
/// samples are omitted rather than reset to zero; this gauge marks that gap.
async fn append_bounded_node_metrics<F>(output: &mut String, collect: F, budget: std::time::Duration)
where
    F: std::future::Future<Output = Vec<ScopedGraphShardRuntimeMetrics>>,
{
    let result = tokio::time::timeout(budget, collect).await;
    output.push_str("# TYPE graph_runtime_shard_metrics_collection_success gauge\n");
    output.push_str(&format!(
        "graph_runtime_shard_metrics_collection_success {}\n",
        u8::from(result.is_ok())
    ));
    if let Ok(shards) = result {
        append_node_metrics(output, &shards);
    }
}

fn append_slatedb_cache_metrics(output: &mut String, cache: hydradb::SlateDbCacheMetricsSnapshot) {
    output.push_str("# TYPE graph_slatedb_cache_capacity_bytes gauge\n");
    output.push_str("# TYPE graph_slatedb_cache_resident_bytes gauge\n");
    output.push_str("# TYPE graph_slatedb_cache_entries gauge\n");
    output.push_str(&format!("graph_slatedb_cache_capacity_bytes {}\n", cache.capacity_bytes));
    output.push_str(&format!("graph_slatedb_cache_resident_bytes {}\n", cache.resident_bytes));
    output.push_str(&format!("graph_slatedb_cache_entries {}\n", cache.entries));
    append_cache_observe_metrics(output, &cache.observe);
}

/// Bounded observer series (ENG-3466). Kind and reason label sets are
/// closed enums, so series count is fixed regardless of traffic. When
/// observation is disabled only the `observe_enabled` gauge is emitted;
/// enabling it later adds the bounded families below, never per-key series.
fn append_cache_observe_metrics(output: &mut String, observe: &hydradb::CacheObserveSnapshot) {
    let enabled = observe.generation != 0;
    output.push_str("# TYPE graph_slatedb_cache_observe_enabled gauge\n");
    output.push_str(&format!("graph_slatedb_cache_observe_enabled {}\n", u8::from(enabled)));
    if !enabled {
        return;
    }
    use hydradb::{EntryKind, RemovalReason};
    let kinds: Vec<EntryKind> = (0..hydradb::ENTRY_KIND_COUNT)
        .map(|i| match i {
            0 => EntryKind::Unknown,
            1 => EntryKind::DataBlock,
            2 => EntryKind::Index,
            3 => EntryKind::Filter,
            _ => EntryKind::Stats,
        })
        .collect();
    let kind_series: [(&str, &Vec<u64>); 12] = [
        ("graph_slatedb_cache_fetches", &observe.fetches),
        ("graph_slatedb_cache_gets", &observe.gets),
        ("graph_slatedb_cache_hits", &observe.hits),
        ("graph_slatedb_cache_tracked_hits", &observe.tracked_hits),
        ("graph_slatedb_cache_misses_after_eviction", &observe.misses_after_eviction),
        ("graph_slatedb_cache_misses_unknown", &observe.misses_unknown),
        ("graph_slatedb_cache_admissions", &observe.admissions),
        ("graph_slatedb_cache_loader_executions", &observe.loader_executions),
        ("graph_slatedb_cache_loader_success_bytes", &observe.loader_success_bytes),
        ("graph_slatedb_cache_loader_failures", &observe.loader_failures),
        ("graph_slatedb_cache_coalesced_waiters", &observe.coalesced_waiters),
        (
            "graph_slatedb_cache_fetch_served_from_resident",
            &observe.fetch_served_from_resident,
        ),
    ];
    for (name, values) in kind_series {
        output.push_str(&format!("# TYPE {name} counter\n"));
        for (kind, value) in kinds.iter().zip(values.iter()) {
            output.push_str(&format!("{name}{{entry_kind=\"{}\"}} {value}\n", kind.as_str()));
        }
    }
    let reasons = [
        RemovalReason::CapacityEviction,
        RemovalReason::Replacement,
        RemovalReason::ExplicitRemove,
        RemovalReason::Clear,
    ];
    output.push_str("# TYPE graph_slatedb_cache_removals counter\n");
    for (reason, value) in reasons.iter().zip(observe.removals_by_reason.iter()) {
        output.push_str(&format!("graph_slatedb_cache_removals{{reason=\"{}\"}} {value}\n", reason.as_str()));
    }
    output.push_str("# TYPE graph_slatedb_cache_removed_bytes counter\n");
    for (reason, value) in reasons.iter().zip(&observe.removal_bytes_by_reason) {
        output.push_str(&format!(
            "graph_slatedb_cache_removed_bytes{{reason=\"{}\"}} {value}\n",
            reason.as_str()
        ));
    }
    for (name, values) in [
        ("graph_slatedb_cache_misses_after_removal", &observe.misses_by_removal_reason),
        ("graph_slatedb_cache_returns_after_removal", &observe.returns_by_removal_reason),
    ] {
        output.push_str(&format!("# TYPE {name} counter\n"));
        for (reason, value) in reasons.iter().zip(values) {
            output.push_str(&format!("{name}{{reason=\"{}\"}} {value}\n", reason.as_str()));
        }
    }
    let scalars: [(&str, u64); 15] = [
        ("graph_slatedb_cache_removals_untracked", observe.removals_untracked),
        ("graph_slatedb_cache_episodes_opened", observe.episodes_opened),
        ("graph_slatedb_cache_episodes_closed_returned", observe.episodes_closed_returned),
        ("graph_slatedb_cache_episodes_closed_expired", observe.episodes_closed_expired),
        ("graph_slatedb_cache_episodes_closed_dropped", observe.episodes_closed_dropped),
        ("graph_slatedb_cache_reinsertion_intervals", observe.reinsertion_intervals),
        (
            "graph_slatedb_cache_reinsertion_interval_ms_avg",
            observe.reinsertion_interval_ms_avg,
        ),
        (
            "graph_slatedb_cache_reinsertion_interval_ms_max",
            observe.reinsertion_interval_ms_max,
        ),
        ("graph_slatedb_cache_zero_hit_evictions", observe.zero_hit_evictions),
        (
            "graph_slatedb_cache_readmissions_while_resident",
            observe.readmissions_while_resident,
        ),
        ("graph_slatedb_cache_ghost_dropped", observe.ghost_dropped),
        ("graph_slatedb_cache_ghost_expired", observe.ghost_expired),
        ("graph_slatedb_cache_resident_overflow", observe.resident_overflow),
        ("graph_slatedb_cache_untracked_hits", observe.untracked_hits),
        ("graph_slatedb_cache_observe_resident_tracked", observe.resident_tracked),
    ];
    for (name, value) in scalars {
        let metric_type = match name {
            "graph_slatedb_cache_observe_resident_tracked"
            | "graph_slatedb_cache_reinsertion_interval_ms_avg"
            | "graph_slatedb_cache_reinsertion_interval_ms_max" => "gauge",
            _ => "counter",
        };
        output.push_str(&format!("# TYPE {name} {metric_type}\n{name} {value}\n"));
    }
    output.push_str("# TYPE graph_slatedb_cache_observe_ghost_records gauge\n");
    output.push_str(&format!("graph_slatedb_cache_observe_ghost_records {}\n", observe.ghost_records));
    output.push_str("# TYPE graph_slatedb_cache_observe_drops_observed gauge\n");
    output.push_str(&format!("graph_slatedb_cache_observe_drops_observed {}\n", observe.drops_observed));
    for (name, value, metric_type) in [
        ("graph_slatedb_cache_reload_executions", observe.reload_executions, "counter"),
        ("graph_slatedb_cache_reload_success_bytes", observe.reload_success_bytes, "counter"),
        ("graph_slatedb_cache_reload_failures", observe.reload_failures, "counter"),
        ("graph_slatedb_cache_loader_cancelled", observe.loader_cancelled, "counter"),
        ("graph_slatedb_cache_ordering_gaps", observe.ordering_gaps, "counter"),
        ("graph_slatedb_cache_pending_admissions", observe.pending_admissions, "gauge"),
        ("graph_slatedb_cache_scope_overflow", observe.scope_overflow, "counter"),
        (
            "graph_slatedb_cache_observe_metadata_bytes_estimate",
            observe.metadata_bytes,
            "gauge",
        ),
        (
            "graph_slatedb_cache_observe_fixed_metadata_bytes_estimate",
            observe.fixed_metadata_bytes,
            "gauge",
        ),
        (
            "graph_slatedb_cache_observe_envelope_metadata_bytes_estimate",
            observe.envelope_metadata_bytes,
            "gauge",
        ),
        (
            "graph_slatedb_cache_observe_metadata_budget_bytes",
            observe.max_metadata_bytes,
            "gauge",
        ),
    ] {
        output.push_str(&format!("# TYPE {name} {metric_type}\n{name} {value}\n"));
    }
    for (name, histogram) in [
        ("graph_slatedb_cache_eviction_to_miss_microseconds", &observe.eviction_to_miss),
        ("graph_slatedb_cache_reload_duration_microseconds", &observe.reload_duration),
        (
            "graph_slatedb_cache_reload_failed_duration_microseconds",
            &observe.reload_failed_duration,
        ),
    ] {
        append_cache_observe_histogram(output, name, histogram);
    }
}

fn append_cache_observe_histogram(output: &mut String, name: &str, histogram: &DurationHistogramSnapshot) {
    output.push_str(&format!("# TYPE {name} histogram\n"));
    for (bound, count) in histogram.cumulative() {
        let le = bound.map_or_else(|| LE_INFINITY.to_string(), |value| value.to_string());
        output.push_str(&format!("{name}_bucket{{le=\"{le}\"}} {count}\n"));
    }
    output.push_str(&format!("{name}_sum {}\n{name}_count {}\n", histogram.sum_us, histogram.count()));
}

/// A point-in-time process-memory snapshot for the Linux graph-node process.
///
/// `allocator_arena_bytes` is glibc's non-mmapped heap, including its in-use
/// and free blocks. Mmap-backed allocations are live while they exist but are
/// not part of `uordblks`, so `allocator_mmap_bytes` stays separate and
/// `allocator_live_bytes` adds the two in-use domains. The retained-memory
/// diagnosis is `allocator_arena_free_bytes / allocator_arena_bytes`; mmap
/// memory must never dilute that ratio.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
struct ProcessMemoryMetrics {
    resident_bytes: u64,
    allocator_arena_bytes: u64,
    allocator_arena_in_use_bytes: u64,
    allocator_arena_free_bytes: u64,
    allocator_mmap_bytes: u64,
    allocator_live_bytes: u64,
    open_file_descriptors: Option<u64>,
    open_file_descriptor_soft_limit: Option<u64>,
}

fn append_process_memory_metrics(output: &mut String, memory: ProcessMemoryMetrics) {
    output.push_str(concat!(
        "# TYPE graph_process_resident_memory_bytes gauge\n",
        "# TYPE graph_process_allocator_arena_bytes gauge\n",
        "# TYPE graph_process_allocator_arena_in_use_bytes gauge\n",
        "# TYPE graph_process_allocator_arena_free_bytes gauge\n",
        "# TYPE graph_process_allocator_mmap_bytes gauge\n",
        "# TYPE graph_process_allocator_live_bytes gauge\n",
    ));
    output.push_str(&format!("graph_process_resident_memory_bytes {}\n", memory.resident_bytes));
    output.push_str(&format!("graph_process_allocator_arena_bytes {}\n", memory.allocator_arena_bytes));
    output.push_str(&format!(
        "graph_process_allocator_arena_in_use_bytes {}\n",
        memory.allocator_arena_in_use_bytes
    ));
    output.push_str(&format!(
        "graph_process_allocator_arena_free_bytes {}\n",
        memory.allocator_arena_free_bytes
    ));
    output.push_str(&format!("graph_process_allocator_mmap_bytes {}\n", memory.allocator_mmap_bytes));
    output.push_str(&format!("graph_process_allocator_live_bytes {}\n", memory.allocator_live_bytes));
    if let Some(open) = memory.open_file_descriptors {
        output.push_str("# TYPE graph_process_open_file_descriptors gauge\n");
        output.push_str(&format!("graph_process_open_file_descriptors {open}\n"));
    }
    if let Some(limit) = memory.open_file_descriptor_soft_limit {
        output.push_str("# TYPE graph_process_open_file_descriptor_soft_limit gauge\n");
        output.push_str(&format!("graph_process_open_file_descriptor_soft_limit {limit}\n"));
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn process_memory_metrics() -> Option<ProcessMemoryMetrics> {
    let resident_bytes = parse_linux_rss_bytes(&std::fs::read_to_string("/proc/self/status").ok()?)?;
    // Reading this directory briefly owns one descriptor which appears in its
    // own listing. Subtract it so the gauge describes the process before the
    // sample. Individual entry errors still count as occupied descriptor slots.
    let open_file_descriptors = std::fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.count().saturating_sub(1) as u64);
    let open_file_descriptor_soft_limit = process_file_descriptor_soft_limit();
    // `mallinfo2` is a glibc snapshot function. It reads allocator metadata but
    // does not allocate, so the admin scrape cannot itself inflate the value it
    // reports. It is intentionally unavailable on musl and non-Linux targets.
    let allocator = unsafe { libc::mallinfo2() };
    let arena_in_use_bytes = allocator.uordblks as u64;
    let mmap_bytes = allocator.hblkhd as u64;
    Some(ProcessMemoryMetrics {
        resident_bytes,
        allocator_arena_bytes: allocator.arena as u64,
        allocator_arena_in_use_bytes: arena_in_use_bytes,
        allocator_arena_free_bytes: allocator.fordblks as u64,
        allocator_mmap_bytes: mmap_bytes,
        allocator_live_bytes: arena_in_use_bytes.saturating_add(mmap_bytes),
        open_file_descriptors,
        open_file_descriptor_soft_limit,
    })
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn process_file_descriptor_soft_limit() -> Option<u64> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
        return None;
    }
    let limit = unsafe { limit.assume_init() };
    (limit.rlim_cur != libc::RLIM_INFINITY).then_some(limit.rlim_cur)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn process_memory_metrics() -> Option<ProcessMemoryMetrics> {
    None
}

/// Parses the `VmRSS` row from Linux `/proc/<pid>/status`, whose `kB` unit is
/// 1,024 bytes despite its spelling. Keep this independent from procfs I/O so
/// malformed and missing input has a small, deterministic unit-test surface.
fn parse_linux_rss_bytes(status: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?.split_ascii_whitespace().next()?;
        value.parse::<u64>().ok()?.checked_mul(1024)
    })
}

/// Highest write-permit utilization (in use / capacity) across the scopes
/// hosted on each cell, skipping shards with no permit capacity.
fn max_scope_write_permit_utilization(shard_metrics: &[ScopedGraphShardRuntimeMetrics]) -> BTreeMap<&str, f64> {
    let mut max_scope_utilization = BTreeMap::<&str, f64>::new();
    for metrics in shard_metrics {
        let shard = &metrics.shard;
        if shard.write_permit_capacity == 0 {
            continue;
        }
        let utilization = shard.write_permits_in_use as f64 / shard.write_permit_capacity as f64;
        max_scope_utilization
            .entry(shard.cell_id.as_str())
            .and_modify(|current| *current = current.max(utilization))
            .or_insert(utilization);
    }
    max_scope_utilization
}

/// The per-shard half of the endpoint.
///
/// # Two dimensionalities, on purpose
///
/// Three counter families here are labelled `{scope, cell_id}` and everything
/// else added since is labelled `{cell_id}` alone. That is not drift. `scope` is
/// the unbounded tenant root, and §1.4 of the metrics plan chose to leave the
/// families that already carry it — they are scraped today and every dashboard
/// and recording rule built on them is downstream of those exact strings — while
/// letting nothing new join them. The remaining shard counters are therefore
/// summed per cell, which is also the only way they *can* be emitted: two
/// scopes hosting the same `cell_id` would otherwise render one series name and
/// label set twice in a single scrape. They are rendered by
/// `append_per_cell_counters` from the runtime's cumulative totals, not here,
/// so a scope closing cannot lower them.
fn append_node_metrics(output: &mut String, shard_metrics: &[ScopedGraphShardRuntimeMetrics]) {
    // Declared from the enumeration rather than written out, so a counter that
    // joins `GraphOperationalMetricsSnapshot` cannot be declared here and
    // rendered nowhere, or the reverse. A default snapshot is the cheapest way
    // to ask the kernel "which counters do you have" without restating the
    // answer.
    append_counter_types(
        output,
        CounterSource::Shard,
        GraphOperationalMetricsSnapshot::default().counter_fields(),
        |export| matches!(export, PrometheusCounterExport::ScopePerCell(_)),
    );
    output.push_str(concat!(
        "# TYPE graph_cache_entries gauge\n",
        "# TYPE graph_cache_resident_bytes gauge\n",
        "# TYPE graph_storage_l0_sst_count gauge\n",
        "# TYPE graph_storage_sorted_run_count gauge\n",
        "# TYPE graph_storage_sst_count gauge\n",
        "# TYPE graph_storage_segment_max_l0_sst_count gauge\n",
        "# TYPE graph_storage_immutable_memtable_flushes counter\n",
        "# TYPE graph_storage_get_requests counter\n",
        "# TYPE graph_storage_scan_requests counter\n",
        "# TYPE graph_storage_total_mem_size_bytes gauge\n",
        "# TYPE graph_storage_sst_filter_checks counter\n",
        "# TYPE graph_storage_backpressure_writes counter\n",
        "# TYPE graph_storage_l0_write_stalls counter\n",
        "# TYPE graph_storage_compaction_bytes counter\n",
        "# TYPE graph_storage_running_compactions gauge\n",
        "# TYPE graph_storage_last_compaction_timestamp_sec gauge\n",
        "# TYPE graph_storage_wal_buffer_flushes counter\n",
        "# TYPE graph_storage_wal_buffer_flush_requests counter\n",
        "# TYPE graph_storage_write_bytes counter\n",
        "# TYPE graph_storage_block_cache_accesses counter\n",
        "# TYPE graph_storage_object_store_requests counter\n",
    ));
    // From the same enumeration the series below come from, so a family whose
    // shards are all absent still declares itself.
    append_histogram_types(output, GraphOperationalMetricsSnapshot::default().histogram_fields());
    // A point-in-time gauge derived from each live scope's semaphore state.
    // Fold scopes sharing a cell with `max`, not `sum`, and never label by
    // scope: this is the maximum single-scope occupancy with bounded
    // per-cell cardinality. Consumers average the available scrape samples and
    // separately require adequate sample coverage; the average alone is not a
    // continuous-time utilization measurement when scrapes are sparse.
    output.push_str("# TYPE graph_write_permit_max_scope_utilization_ratio gauge\n");
    for (cell_id, utilization) in max_scope_write_permit_utilization(shard_metrics) {
        output.push_str(&format!(
            "graph_write_permit_max_scope_utilization_ratio{{cell_id=\"{cell_id}\"}} {utilization}\n"
        ));
    }
    // Histograms that do not carry `scope` are keyed by `cell_id` alone, and a
    // node may hold several scopes on one cell -- every scope on a query node
    // uses the same `GRAPH_CELL_ID`, so this is the normal case rather than a
    // corner. Rendering them inside the per-scope loop would emit the same
    // series name and label set once per scope in a single scrape, which
    // Prometheus rejects outright. They are summed here and rendered once,
    // exactly as `append_per_cell_counters` does for the counters that made
    // this same choice.
    let mut per_cell_histograms: BTreeMap<&str, BTreeMap<&'static str, DurationHistogramSnapshot>> = BTreeMap::new();
    for metrics in shard_metrics {
        let scope = metrics.scope.to_string();
        let metrics = &metrics.shard;
        let scoped = render_labels(&[("scope", &scope), ("cell_id", &metrics.cell_id)], None);
        for (field, value) in metrics.operational.counter_fields() {
            let Some(export) = prometheus_counter(CounterSource::Shard, field) else {
                debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_COUNTERS");
                continue;
            };
            if let PrometheusCounterExport::ScopePerCell(name) = export.export {
                output.push_str(&format!("{name}{scoped} {value}\n"));
            }
        }
        for (cache, entries) in [
            ("matrix_artifacts", metrics.cache_entries.matrix_artifacts),
            ("matrix_adjacencies", metrics.cache_entries.matrix_adjacencies),
            ("graphblas_matrices", metrics.cache_entries.graphblas_matrices),
            ("parsed_row_queries", metrics.cache_entries.parsed_row_queries),
            ("relationship_rows", metrics.cache_entries.relationship_row_sets),
            ("relationship_property_rows", metrics.cache_entries.relationship_property_row_sets),
        ] {
            output.push_str(&format!(
                "graph_cache_entries{{scope=\"{}\",cell_id=\"{}\",cache=\"{cache}\"}} {entries}\n",
                scope, metrics.cell_id
            ));
        }
        for (cache, bytes) in [
            ("matrix_adjacencies", metrics.cache_resident_bytes.matrix_adjacencies),
            ("graphblas_matrices", metrics.cache_resident_bytes.graphblas_matrices),
            ("relationship_rows", metrics.cache_resident_bytes.relationship_rows),
            ("source_relationship_rows", metrics.cache_resident_bytes.source_relationship_rows),
            (
                "relationship_property_rows",
                metrics.cache_resident_bytes.relationship_property_rows,
            ),
            ("gds_projected_graphs", metrics.cache_resident_bytes.gds_projected_graphs),
        ] {
            output.push_str(&format!(
                "graph_cache_resident_bytes{{scope=\"{}\",cell_id=\"{}\",cache=\"{cache}\"}} {bytes}\n",
                scope, metrics.cell_id
            ));
        }
        // SlateDB storage-engine metrics for this scope's LSM. Rendered by hand
        // like the cache gauges above rather than through the counter
        // enumeration, because they originate in SlateDB, not in
        // `GraphOperationalMetricsSnapshot` -- keeping them out of that
        // enumeration is what lets the export-completeness invariant stay a
        // statement about the kernel's own counters. `l0_sst_count` is the one
        // to watch: it is the number of L0 SSTs a range scan must consult, so
        // it is the multiplier behind relationship-`id` prefix-scan cost.
        let storage = &metrics.storage;
        for (name, value) in [
            ("graph_storage_l0_sst_count", storage.l0_sst_count),
            ("graph_storage_sorted_run_count", storage.sorted_run_count),
            ("graph_storage_sst_count", storage.sst_count),
            ("graph_storage_segment_max_l0_sst_count", storage.segment_max_l0_sst_count),
            ("graph_storage_immutable_memtable_flushes", storage.immutable_memtable_flushes),
            ("graph_storage_get_requests", storage.get_requests),
            ("graph_storage_scan_requests", storage.scan_requests),
            ("graph_storage_total_mem_size_bytes", storage.total_mem_size_bytes),
            ("graph_storage_backpressure_writes", storage.backpressure_writes),
            ("graph_storage_l0_write_stalls", storage.l0_write_stalls),
            ("graph_storage_compaction_bytes", storage.compaction_bytes),
            ("graph_storage_running_compactions", storage.running_compactions),
            ("graph_storage_last_compaction_timestamp_sec", storage.last_compaction_timestamp_sec),
            ("graph_storage_wal_buffer_flushes", storage.wal_buffer_flushes),
            ("graph_storage_wal_buffer_flush_requests", storage.wal_buffer_flush_requests),
        ] {
            output.push_str(&format!("{name}{{scope=\"{}\",cell_id=\"{}\"}} {value}\n", scope, metrics.cell_id));
        }
        // Filter outcomes are the direct measurement of file fan-in: for the
        // legacy whole-key-only policy `kind="prefix"` records no negatives;
        // incident-prefix filters on rewritten SSTs can now skip those files.
        // Positives/scan_requests is the number of SSTs each prefix scan
        // opens, while `kind="point"` negatives are the files a point lookup
        // skipped for free. A point-lookup index moves work from the first
        // bucket to the second; these six series are its before/after.
        for (kind, outcome, value) in [
            ("point", "positive", storage.sst_filter_point_positives),
            ("point", "negative", storage.sst_filter_point_negatives),
            ("point", "false_positive", storage.sst_filter_point_false_positives),
            ("prefix", "positive", storage.sst_filter_prefix_positives),
            ("prefix", "negative", storage.sst_filter_prefix_negatives),
            ("prefix", "false_positive", storage.sst_filter_prefix_false_positives),
        ] {
            output.push_str(&format!(
                "graph_storage_sst_filter_checks{{scope=\"{}\",cell_id=\"{}\",kind=\"{kind}\",outcome=\"{outcome}\"}} {value}\n",
                scope, metrics.cell_id
            ));
        }
        // write_amp = (wal_flush + l0_flush + compaction_bytes) / memtable —
        // the stages are labelled so the ratio is computable in PromQL.
        for (stage, value) in [
            ("memtable", storage.memtable_write_bytes),
            ("wal_flush", storage.wal_flush_bytes),
            ("l0_flush", storage.l0_flush_bytes),
        ] {
            output.push_str(&format!(
                "graph_storage_write_bytes{{scope=\"{}\",cell_id=\"{}\",stage=\"{stage}\"}} {value}\n",
                scope, metrics.cell_id
            ));
        }
        for (entry_kind, result, value) in [
            ("data_block", "hit", storage.block_cache_data_hits),
            ("data_block", "miss", storage.block_cache_data_misses),
            ("filter", "hit", storage.block_cache_filter_hits),
            ("filter", "miss", storage.block_cache_filter_misses),
        ] {
            output.push_str(&format!(
                "graph_storage_block_cache_accesses{{scope=\"{}\",cell_id=\"{}\",entry_kind=\"{entry_kind}\",result=\"{result}\"}} {value}\n",
                scope, metrics.cell_id
            ));
        }
        // Per SlateDB component, one label set for the name: the compactor's
        // and the garbage collector's requests are not foreground reads, and
        // `sum without(component)` gives the store's total.
        for (op, by_component) in [
            ("get", storage.object_store_get_requests_by_component),
            ("put", storage.object_store_put_requests_by_component),
        ] {
            for (component, value) in by_component.by_label() {
                output.push_str(&format!(
                    "graph_storage_object_store_requests{{scope=\"{}\",cell_id=\"{}\",op=\"{op}\",component=\"{component}\"}} {value}\n",
                    scope, metrics.cell_id
                ));
            }
        }
        // `scope` and `cell_id`, and nothing else. Never `edge_type`: a
        // 21-bucket family times 96 cell×type pairs is 2,016 series per
        // instrument per node, which is where the cardinality budget stops
        // being affordable. `scope` is here and absent from the OTel export by
        // design -- that divergence is why both exports exist.
        append_histograms(
            output,
            metrics
                .operational
                .histogram_fields()
                .filter(|(field, _)| histogram_carries_scope(field)),
            &[("scope", &scope), ("cell_id", &metrics.cell_id)],
        );
        for (field, snapshot) in metrics.operational.histogram_fields() {
            if histogram_carries_scope(field) {
                continue;
            }
            merge_histogram(
                per_cell_histograms
                    .entry(metrics.cell_id.as_str())
                    .or_default()
                    .entry(field)
                    .or_default(),
                snapshot,
            );
        }
    }
    for (cell_id, histograms) in &per_cell_histograms {
        append_histograms(
            output,
            histograms.iter().map(|(field, snapshot)| (*field, snapshot)),
            &[("cell_id", cell_id)],
        );
    }
    append_experimental_operator_families(output, shard_metrics);
    crate::otel_metrics::planner_statistics::append(output, shard_metrics);
}

/// The experimental engine's per-operator families: one Prometheus family per
/// [`ExperimentalOperatorMetricsSnapshot`] counter, labelled `cell_id` and
/// `operator`, summed across scopes sharing a cell.
///
/// `operator` is `GraphPhysicalPlan::operator_name`, a closed vocabulary of
/// under twenty values, and only operators that have run render a series, so a
/// cell costs at most one series per operator kind per family. Prometheus
/// only for now: the OTLP meter's structured-counter path does not exist yet.
pub const EXPERIMENTAL_OPERATOR_FAMILIES: &[(&str, &str)] = &[
    ("requests", "graph_query_experimental_operator_requests"),
    ("invocations", "graph_query_experimental_operator_invocations"),
    ("rows_in", "graph_query_experimental_operator_rows_in"),
    ("rows_out", "graph_query_experimental_operator_rows_out"),
    ("self_us", "graph_query_experimental_operator_self_microseconds"),
    ("storage_us", "graph_query_experimental_operator_storage_microseconds"),
    ("storage_requests", "graph_query_experimental_operator_storage_requests"),
    ("storage_bytes", "graph_query_experimental_operator_storage_bytes"),
    ("hydrated_vertices", "graph_query_experimental_operator_hydrated_vertices"),
    ("scanned_relationships", "graph_query_experimental_operator_scanned_relationships"),
    ("peak_retained_rows", "graph_query_experimental_operator_peak_retained_rows"),
    ("estimate_misses", "graph_query_experimental_operator_estimate_misses"),
];

fn append_experimental_operator_families(output: &mut String, shard_metrics: &[ScopedGraphShardRuntimeMetrics]) {
    let mut per_cell: BTreeMap<&str, BTreeMap<&'static str, ExperimentalOperatorMetricsSnapshot>> = BTreeMap::new();
    for metrics in shard_metrics {
        for operator in &metrics.shard.operational.experimental_operators {
            per_cell
                .entry(metrics.shard.cell_id.as_str())
                .or_default()
                .entry(operator.operator)
                .or_insert_with(|| ExperimentalOperatorMetricsSnapshot {
                    operator: operator.operator,
                    ..ExperimentalOperatorMetricsSnapshot::default()
                })
                .accumulate(operator);
        }
    }
    for (index, (field, name)) in EXPERIMENTAL_OPERATOR_FAMILIES.iter().enumerate() {
        // Declared even with no series, so a scrape names every family.
        output.push_str(&format!("# TYPE {name} counter\n"));
        for (cell_id, operators) in &per_cell {
            for operator in operators.values() {
                let (counter, value) = operator.counter_fields()[index];
                debug_assert_eq!(counter, *field, "family table out of order");
                output.push_str(&format!(
                    "{name}{{cell_id=\"{cell_id}\",operator=\"{}\"}} {value}\n",
                    operator.operator
                ));
            }
        }
    }
}

/// Whether a histogram family is rendered per `{scope, cell_id}` or per
/// `cell_id` alone. Unknown fields are treated as scoped so the pre-existing
/// families keep their labels if this table and the enumeration ever disagree —
/// `crate::otel_metrics::tests::every_histogram_field_reaches_both_exports`
/// catches the disagreement itself.
fn histogram_carries_scope(field: &str) -> bool {
    prometheus_histogram(field).is_none_or(|export| export.carries_scope)
}

/// Add one shard's observations into a running per-cell total.
///
/// Bucket-wise, because a histogram sums the way its buckets do: the merged
/// `count()` is then the sum of the merged buckets by the same construction
/// that makes it true of a single shard's, and `_count` still equals the
/// `+Inf` cumulative bucket after merging.
fn merge_histogram(total: &mut DurationHistogramSnapshot, shard: &DurationHistogramSnapshot) {
    for (slot, count) in total.bucket_counts.iter_mut().zip(shard.bucket_counts) {
        *slot = slot.saturating_add(count);
    }
    total.sum_us = total.sum_us.saturating_add(shard.sum_us);
}

/// Every shard counter that is not one of the three `{scope, cell_id}`
/// survivors, per `cell_id`, rendered family by family.
///
/// From the runtime's cumulative totals rather than from the shard snapshots
/// `append_node_metrics` renders, so a scope closing never lowers a series —
/// see [`PrometheusCounterExport::PerCell`]. That also puts these outside the
/// bounded shard collection: the totals are atomics read under one mutex, so a
/// slow scope open cannot make a counter family vanish for a scrape either.
///
/// Family-major rather than shard-major: all of a family's cells are known at
/// once and can be emitted under a single `# TYPE` line, which is what the
/// exposition format actually asks for.
fn append_per_cell_counters(output: &mut String, totals: &BTreeMap<String, CellOperationalCounterTotals>) {
    let operational = totals
        .iter()
        .map(|(cell_id, cell)| (cell_id.as_str(), cell.counters.clone()))
        .collect();
    let classes = totals
        .iter()
        .map(|(cell_id, cell)| (cell_id.as_str(), cell.classes.clone()))
        .collect();
    append_per_cell_family(
        output,
        CounterSource::Shard,
        GraphOperationalMetricsSnapshot::default().counter_fields(),
        &operational,
    );
    append_per_cell_class_family(
        output,
        CounterSource::Shard,
        GraphOperationalMetricsSnapshot::default().class_counter_fields(),
        &classes,
    );
}

fn append_cache_counters(output: &mut String, metrics: &BTreeMap<String, GraphCacheMetricsSnapshot>) {
    let totals = metrics
        .iter()
        .map(|(cell, snapshot)| (cell.as_str(), snapshot.counter_fields().collect()))
        .collect();
    append_per_cell_family(
        output,
        CounterSource::ShardCache,
        GraphCacheMetricsSnapshot::default().counter_fields(),
        &totals,
    );
}

/// Declare a `# TYPE … counter` line for every field a snapshot enumerates
/// whose export shape `wanted` accepts.
///
/// Driven by the enumeration and filtered by shape, so a family declares itself
/// exactly once however many shards render it and whichever loop does.
fn append_counter_types(
    output: &mut String,
    source: CounterSource,
    fields: impl Iterator<Item = (&'static str, u64)>,
    wanted: impl Fn(PrometheusCounterExport) -> bool,
) {
    for (field, _) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_COUNTERS");
            continue;
        };
        if !wanted(export.export) {
            continue;
        }
        let Some(name) = export.export.name() else {
            continue;
        };
        output.push_str(&format!("# TYPE {name} counter\n"));
    }
}

/// Render every [`PrometheusCounterExport::PerCell`] family a snapshot
/// enumerates, one series per cell.
///
/// A cell that reports no value for an enumerated field renders `0` rather than
/// being skipped: the field is enumerated by the type, so its absence would mean
/// the accumulation lost it, and a missing series reads to a scraper as a shard
/// that stopped rather than a counter at rest.
fn append_per_cell_family(
    output: &mut String,
    source: CounterSource,
    fields: impl Iterator<Item = (&'static str, u64)>,
    totals: &BTreeMap<&str, Vec<(&'static str, u64)>>,
) {
    for (field, _) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_COUNTERS");
            continue;
        };
        let PrometheusCounterExport::PerCell(name) = export.export else {
            continue;
        };
        output.push_str(&format!("# TYPE {name} counter\n"));
        for (cell_id, cell) in totals {
            let value = cell.iter().find(|(name, _)| *name == field).map_or(0, |(_, value)| *value);
            output.push_str(&format!("{name}{{cell_id=\"{cell_id}\"}} {value}\n"));
        }
    }
}

/// [`append_per_cell_family`] for the error-class breakdowns.
///
/// The enumeration is already flattened into one row per field per class, so the
/// `# TYPE` line is emitted when the field changes rather than once per row.
fn append_per_cell_class_family(
    output: &mut String,
    source: CounterSource,
    fields: impl Iterator<Item = (&'static str, &'static str, u64)>,
    totals: &BTreeMap<&str, Vec<(&'static str, &'static str, u64)>>,
) {
    let mut declared: Option<&'static str> = None;
    for (field, class, _) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_CLASS_COUNTERS");
            continue;
        };
        let PrometheusCounterExport::PerCell(name) = export.export else {
            continue;
        };
        if declared != Some(field) {
            output.push_str(&format!("# TYPE {name} counter\n"));
            declared = Some(field);
        }
        for (cell_id, cell) in totals {
            let value = cell
                .iter()
                .find(|(name, existing, _)| *name == field && *existing == class)
                .map_or(0, |(_, _, value)| *value);
            output.push_str(&format!(
                "{name}{{cell_id=\"{cell_id}\",{ERROR_CLASS_LABEL}=\"{class}\"}} {value}\n"
            ));
        }
    }
}

/// Render every [`PrometheusCounterExport::Global`] counter a snapshot
/// enumerates, unlabelled.
fn append_global_counters(output: &mut String, source: CounterSource, fields: impl Iterator<Item = (&'static str, u64)>) {
    for (field, value) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_COUNTERS");
            continue;
        };
        let PrometheusCounterExport::Global(name) = export.export else {
            continue;
        };
        output.push_str(&format!("# TYPE {name} counter\n{name} {value}\n"));
    }
}

/// [`append_global_counters`] for the error-class breakdowns.
fn append_global_class_counters(
    output: &mut String,
    source: CounterSource,
    fields: impl Iterator<Item = (&'static str, &'static str, u64)>,
) {
    let mut declared: Option<&'static str> = None;
    for (field, class, value) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_CLASS_COUNTERS");
            continue;
        };
        let PrometheusCounterExport::Global(name) = export.export else {
            continue;
        };
        if declared != Some(field) {
            output.push_str(&format!("# TYPE {name} counter\n"));
            declared = Some(field);
        }
        output.push_str(&format!("{name}{{{ERROR_CLASS_LABEL}=\"{class}\"}} {value}\n"));
    }
}

fn append_global_failure_counters(
    output: &mut String,
    source: CounterSource,
    fields: impl Iterator<Item = (&'static str, &'static str, &'static str, u64)>,
) {
    // Declared by name, not by field: engine modes are series of one family,
    // and a repeated `# TYPE` is a rejected scrape.
    let mut declared: Option<&'static str> = None;
    for (field, stage, reason, value) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_FAILURE_COUNTERS");
            continue;
        };
        let PrometheusCounterExport::Global(name) = export.export else {
            continue;
        };
        let Some(cypher_engine) = cypher_engine_label(field) else {
            debug_assert!(false, "{field} is a failure counter with no engine");
            continue;
        };
        if declared != Some(name) {
            output.push_str(&format!("# TYPE {name} counter\n"));
            declared = Some(name);
        }
        output.push_str(&format!(
            "{name}{{{CYPHER_ENGINE_LABEL}=\"{cypher_engine}\",{FAILURE_STAGE_LABEL}=\"{stage}\",\
             {FAILURE_REASON_LABEL}=\"{reason}\"}} {value}\n"
        ));
    }
}

/// Declare a `# TYPE … histogram` line for every family a snapshot enumerates.
///
/// Split from the series rendering because a per-shard family declares its type
/// once and renders its series once per shard, and a repeated `# TYPE` line is
/// a scrape error rather than a cosmetic one.
fn append_histogram_types<'a>(output: &mut String, fields: impl Iterator<Item = (&'static str, &'a DurationHistogramSnapshot)>) {
    // Two fields can share a family name — the per-engine execution
    // histograms do — and a repeated `# TYPE` for one name makes the whole
    // scrape unparseable, so a name is declared once however many series
    // carry it.
    let mut declared: Vec<&'static str> = Vec::new();
    for (field, _) in fields {
        let Some(export) = prometheus_histogram(field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_HISTOGRAMS");
            continue;
        };
        if declared.contains(&export.name) {
            continue;
        }
        declared.push(export.name);
        output.push_str(&format!("# TYPE {} histogram\n", export.name));
    }
}

/// Render every histogram a snapshot enumerates, under `labels`.
///
/// The enumeration is the kernel's `histogram_fields()`; the names come from
/// [`PROMETHEUS_HISTOGRAMS`]. A field the kernel records and this table does not
/// name is a build failure over in
/// `crate::otel_metrics::tests::every_histogram_field_reaches_both_exports`, so
/// the miss below cannot reach a release — it is skipped rather than raised
/// because dropping one family is a better outcome on a scrape than a 500.
fn append_histograms<'a>(
    output: &mut String,
    fields: impl Iterator<Item = (&'static str, &'a DurationHistogramSnapshot)>,
    labels: &[(&str, &str)],
) {
    for (field, snapshot) in fields {
        let Some(export) = prometheus_histogram(field) else {
            debug_assert!(false, "{field} is enumerated but absent from PROMETHEUS_HISTOGRAMS");
            continue;
        };
        let owned: Vec<(&str, &str)>;
        let labels: &[(&str, &str)] = if export.carries_scope {
            labels
        } else {
            owned = labels.iter().copied().filter(|(key, _)| *key != "scope").collect();
            &owned
        };
        // Cumulative, because that is what `le` means. The kernel counts per
        // bucket -- one `fetch_add` -- and `DurationHistogramSnapshot` owns the
        // accumulation, so this rendering and the meter's cannot drift.
        for (bound, cumulative) in snapshot.cumulative() {
            let le = bound.map_or_else(|| LE_INFINITY.to_string(), |bound| export.unit.render_bound(bound));
            output.push_str(&format!(
                "{}_bucket{} {cumulative}\n",
                export.name,
                render_labels(labels, Some(("le", &le)))
            ));
        }
        let labelled = render_labels(labels, None);
        output.push_str(&format!(
            "{}_sum{labelled} {}\n",
            export.name,
            export.unit.render_sum(snapshot.sum_us)
        ));
        // Derived from the buckets, not carried alongside them, so `_count` and
        // `le="+Inf"` are equal by construction.
        output.push_str(&format!("{}_count{labelled} {}\n", export.name, snapshot.count()));
    }
}

/// The `le` value of the overflow bucket.
const LE_INFINITY: &str = "+Inf";

/// `{a="1",b="2"}`, or the empty string when there is nothing to render.
///
/// Prometheus accepts `metric{}` but no existing series here writes it, and a
/// bare name is what a diff of the unlabelled families should show.
fn render_labels(labels: &[(&str, &str)], extra: Option<(&str, &str)>) -> String {
    let mut rendered = String::new();
    for (key, value) in labels.iter().copied().chain(extra) {
        rendered.push_str(if rendered.is_empty() { "{" } else { "," });
        rendered.push_str(&format!("{key}=\"{value}\""));
    }
    if !rendered.is_empty() {
        rendered.push('}');
    }
    rendered
}

fn admin_io_error(error: std::io::Error) -> hydradb::GraphError {
    hydradb::GraphError::CorruptValue {
        key: "runtime/admin".to_string(),
        reason: error.to_string(),
    }
}

// weave: run 'weave explain src/bin/graph_node/admin.rs' for per-hunk detail, 'weave check' to verify your resolution
