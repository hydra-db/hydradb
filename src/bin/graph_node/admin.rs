#[cfg(test)]
#[path = "admin/cypher_engine_tests.rs"]
mod cypher_engine_tests;
#[path = "admin/memory_sampler.rs"]
mod memory_sampler;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use hydradb::{
    ClientQueryService, CypherEngineMode, CypherEngineSelection, DurationHistogramSnapshot,
    ExperimentalOperatorMetricsSnapshot, GraphCacheMetricsSnapshot,
    GraphOperationalMetricsSnapshot, Result, ScopedGraphShardRuntimeMetrics,
    ScopedRoutedGraphCluster,
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
    /// says so. Three of the five rows below are
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
/// The two execution families each have one row per engine. They are two
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
    // Read-your-writes latency, and the instrument change 4 of
    // `docs/plans/2026-08-21-cell-affine-read-routing.md` exists to add. In
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
    PROMETHEUS_HISTOGRAMS
        .iter()
        .find(|export| export.field == field)
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
    /// The cost is one real property: a scope closing makes the sum *fall*,
    /// which `rate()` reads as a counter reset and undercounts across, where a
    /// per-scope series would simply have gone stale. Undercounting a rate at a
    /// scope eviction is the cheaper of the two failures.
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
    // `docs/plans/2026-08-21-cell-affine-read-routing.md` is that two nodes
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
        export: PrometheusCounterExport::PerCell(
            "graph_relationship_import_endpoint_check_microseconds",
        ),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_scan_us",
        export: PrometheusCounterExport::PerCell(
            "graph_relationship_import_identity_scan_microseconds",
        ),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_pointer_hits",
        export: PrometheusCounterExport::PerCell("graph_relationship_import_identity_pointer_hits"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_identity_pointer_misses",
        export: PrometheusCounterExport::PerCell(
            "graph_relationship_import_identity_pointer_misses",
        ),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_record_read_us",
        export: PrometheusCounterExport::PerCell(
            "graph_relationship_import_record_read_microseconds",
        ),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "relationship_import_structural_check_us",
        export: PrometheusCounterExport::PerCell(
            "graph_relationship_import_structural_check_microseconds",
        ),
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
        export: PrometheusCounterExport::PerCell(
            "graph_relationship_import_counter_read_microseconds",
        ),
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
        export: PrometheusCounterExport::PerCell(
            "graph_reserve_edge_delete_noops_batches_profiled",
        ),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_batch_items",
        export: PrometheusCounterExport::PerCell("graph_reserve_edge_delete_noops_batch_items"),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_read_us",
        export: PrometheusCounterExport::PerCell(
            "graph_reserve_edge_delete_noops_read_microseconds",
        ),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "reserve_edge_delete_noops_txn_us",
        export: PrometheusCounterExport::PerCell(
            "graph_reserve_edge_delete_noops_txn_microseconds",
        ),
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
        export: PrometheusCounterExport::PerCell(
            "graph_query_experimental_relationship_expand_requests",
        ),
    },
    PrometheusCounter {
        source: CounterSource::Shard,
        field: "query_experimental_ordered_property_scan_requests",
        export: PrometheusCounterExport::PerCell(
            "graph_query_experimental_ordered_property_scan_requests",
        ),
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
        export: PrometheusCounterExport::PerCell(
            "graph_query_experimental_statistics_microseconds",
        ),
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
        field: "backpressure_waits",
        export: PrometheusCounterExport::PerCell("graph_backpressure_waits"),
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
];

/// The Prometheus row for a `(source, field)` pair, over every counter table.
///
/// Keyed by the pair and not by the identifier: `backpressure_waits` is a field
/// of two different snapshots.
pub fn prometheus_counter(
    source: CounterSource,
    field: &str,
) -> Option<&'static PrometheusCounter> {
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
/// its namespace. The value is `legacy` or `experimental`, so it costs at most
/// two series per family per node and buys a legacy-versus-experimental
/// overlay that needs no join on `instance`.
const CYPHER_ENGINE_LABEL: &str = "cypher_engine";

/// The engine a client histogram field holds the statements of, or `None` for
/// a field that carries no [`CYPHER_ENGINE_LABEL`] at all.
///
/// The value is a property of the **field**, not of the node: a node that has
/// served both engines — which the kill switch allows — has two populations,
/// and reading the label off the service at scrape time would hand both to
/// whichever engine is effective when Prometheus happens to ask. Shared with
/// the OTLP export through [`hydradb::ClientQueryMetricsSnapshot`]'s field
/// names so the two exports cannot label a family differently.
pub fn cypher_engine_label(field: &str) -> Option<&'static str> {
    match field {
        "read_latency_legacy" | "write_latency_legacy" | "queries_failed_by_reason_legacy" => {
            Some(hydradb::CypherEngineMode::Legacy.as_str())
        }
        "read_latency_experimental"
        | "write_latency_experimental"
        | "queries_failed_by_reason_experimental" => {
            Some(hydradb::CypherEngineMode::Experimental.as_str())
        }
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
    /// The node's graph auth token. Only the mutating control routes check
    /// it; the probes and `/metrics` stay unauthenticated as before.
    control_token: Arc<str>,
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
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(admin_io_error)?;
        let local_addr = listener.local_addr().map_err(admin_io_error)?;
        let state = AdminState {
            ready,
            query,
            routed_node: node,
            memory_sampler: memory_sampler::MemorySampler::start(),
            control_token: Arc::from(control_token),
        };
        Self::serve(listener, local_addr, state)
    }

    fn serve(listener: TcpListener, local_addr: SocketAddr, state: AdminState) -> Result<Self> {
        let router = Router::new()
            .route("/livez", get(live))
            .route("/healthz", get(live))
            .route("/readyz", get(readiness))
            .route("/metrics", get(metrics))
            .route(
                CYPHER_ENGINE_ROUTE,
                get(cypher_engine_state).put(set_cypher_engine_override),
            )
            .with_state(state);
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
        Ok(Self {
            local_addr,
            stop_tx,
            task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        self.task
            .await
            .map_err(|err| hydradb::GraphError::CorruptValue {
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
/// `GET` reports `{configured, override, effective, experimental_compiled}`.
/// `PUT` with `Authorization: Bearer <graph auth token>` and a body of
/// `{"override": "legacy" | "experimental" | null}` sets or clears the
/// override for this node, with no restart, and answers with the new state.
/// `null` returns to `GRAPH_CYPHER_ENGINE`; so does a restart, because the
/// override is deliberately not persisted.
///
/// Statements prepared or executing when it flips keep the engine they were
/// admitted with. A binary built without `experimental-cypher-engine` answers
/// `422` to `"experimental"` and leaves the state unchanged.
pub const CYPHER_ENGINE_ROUTE: &str = "/v1/cypher-engine";

fn cypher_engine_document(selection: CypherEngineSelection) -> serde_json::Value {
    serde_json::json!({
        "configured": selection.configured.as_str(),
        "override": selection.override_mode.map(CypherEngineMode::as_str),
        "effective": selection.effective().as_str(),
        "experimental_compiled": cfg!(feature = "experimental-cypher-engine"),
    })
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [
            ("content-type", "application/json"),
            ("cache-control", "no-store"),
        ],
        body.to_string(),
    )
        .into_response()
}

async fn cypher_engine_state(State(state): State<AdminState>) -> Response {
    json_response(
        StatusCode::OK,
        cypher_engine_document(state.query.cypher_engine_selection()),
    )
}

async fn set_cypher_engine_override(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if !valid_control_bearer(&headers, &state.control_token) {
        return json_response(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"error": "a bearer graph auth token is required"}),
        );
    }
    let requested = match parse_engine_override(&body) {
        Ok(requested) => requested,
        Err(reason) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error": reason}),
            )
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
            json_response(StatusCode::OK, cypher_engine_document(selection))
        }
        Err(error) => json_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "error": error.to_string(),
                "state": cypher_engine_document(state.query.cypher_engine_selection()),
            }),
        ),
    }
}

/// `{"override": ...}` with the key required, so an empty or mistyped body
/// cannot silently clear an override an operator set on purpose.
fn parse_engine_override(body: &[u8]) -> std::result::Result<Option<CypherEngineMode>, String> {
    let document: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| format!("body must be a JSON object: {error}"))?;
    let Some(value) = document
        .as_object()
        .and_then(|object| object.get("override"))
    else {
        return Err(r#"body must be {"override": "legacy" | "experimental" | null}"#.to_string());
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(mode) if mode == "legacy" => Ok(Some(CypherEngineMode::Legacy)),
        serde_json::Value::String(mode) if mode == "experimental" => {
            Ok(Some(CypherEngineMode::Experimental))
        }
        other => Err(format!(
            r#"override must be "legacy", "experimental" or null, not {other}"#
        )),
    }
}

fn valid_control_bearer(headers: &HeaderMap, expected: &str) -> bool {
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
    let query = state.query.metrics();
    // The five pre-existing client series come out of this loop, under their
    // original names and in their original relative order, because the loop is
    // driven by `counter_fields()` and the kernel declares them in that order.
    // The six that were never exported are interleaved where the kernel
    // declares them; a scrape does not care about order and a diff shows them
    // as insertions.
    append_global_counters(&mut output, CounterSource::Client, query.counter_fields());
    append_global_class_counters(
        &mut output,
        CounterSource::Client,
        query.class_counter_fields(),
    );
    // Carries the engine label for the same reason the execution histograms
    // do, and takes it from the same place: the field. The
    // legacy-versus-experimental failure comparison is the point of the
    // family, and a node the kill switch has flipped holds both populations.
    append_global_failure_counters(
        &mut output,
        CounterSource::Client,
        query.failure_counter_fields(),
    );
    // Additive, and deliberately after the counters rather than interleaved
    // with them: every series above keeps the exact name, labels and value it
    // had before the histograms existed.
    append_histogram_types(&mut output, query.histogram_fields());
    // The engine label goes on the execution families and nothing else, and
    // its value comes from the field: the legacy and experimental histograms
    // are two series of one family, and a node that was flipped holds both.
    // Bookmark wait is time spent before a query is admitted, which no engine
    // choice can move, and its family is pinned label-free by
    // `the_bookmark_wait_family_renders`.
    let (engine_fields, plain_fields): (Vec<_>, Vec<_>) = query
        .histogram_fields()
        .partition(|(field, _)| carries_cypher_engine(field));
    for (field, histogram) in engine_fields {
        let Some(engine) = cypher_engine_label(field) else {
            continue;
        };
        append_histograms(
            &mut output,
            std::iter::once((field, histogram)),
            &[(CYPHER_ENGINE_LABEL, engine)],
        );
    }
    append_histograms(&mut output, plain_fields.into_iter(), &[]);
    append_bounded_node_metrics(
        &mut output,
        state.routed_node.local_shard_runtime_metrics(),
        std::time::Duration::from_secs(1),
    )
    .await;
    append_slatedb_cache_metrics(&mut output, state.routed_node.slatedb_cache_metrics());
    crate::otel_metrics::memory_diagnostics::append(&mut output);
    state.memory_sampler.render(&mut output);
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
/// `configured != effective`. Two closed two-value labels: four series at most.
fn cypher_engine_gauge(query: &ClientQueryService) -> String {
    let selection = query.cypher_engine_selection();
    format!(
        "# TYPE graph_cypher_engine gauge\ngraph_cypher_engine{{configured=\"{}\",effective=\"{}\"}} 1\n",
        selection.configured.as_str(),
        selection.effective().as_str(),
    )
}

/// A slow scope open must not hide process-wide OOM diagnostics. Missing shard
/// samples are omitted rather than reset to zero; this gauge marks that gap.
async fn append_bounded_node_metrics<F>(
    output: &mut String,
    collect: F,
    budget: std::time::Duration,
) where
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
    output.push_str(&format!(
        "graph_slatedb_cache_capacity_bytes {}\n",
        cache.capacity_bytes
    ));
    output.push_str(&format!(
        "graph_slatedb_cache_resident_bytes {}\n",
        cache.resident_bytes
    ));
    output.push_str(&format!("graph_slatedb_cache_entries {}\n", cache.entries));
}

/// A point-in-time process-memory snapshot for the Linux graph-node process.
///
/// `allocator_arena_bytes` is glibc's non-mmapped heap, including its in-use
/// and free blocks. Mmap-backed allocations are live while they exist but are
/// not part of `uordblks`, so `allocator_mmap_bytes` stays separate and
/// `allocator_live_bytes` adds the two in-use domains. The retained-memory
/// diagnosis is `allocator_arena_free_bytes / allocator_arena_bytes`; mmap
/// memory must never dilute that ratio.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    output.push_str(&format!(
        "graph_process_resident_memory_bytes {}\n",
        memory.resident_bytes
    ));
    output.push_str(&format!(
        "graph_process_allocator_arena_bytes {}\n",
        memory.allocator_arena_bytes
    ));
    output.push_str(&format!(
        "graph_process_allocator_arena_in_use_bytes {}\n",
        memory.allocator_arena_in_use_bytes
    ));
    output.push_str(&format!(
        "graph_process_allocator_arena_free_bytes {}\n",
        memory.allocator_arena_free_bytes
    ));
    output.push_str(&format!(
        "graph_process_allocator_mmap_bytes {}\n",
        memory.allocator_mmap_bytes
    ));
    output.push_str(&format!(
        "graph_process_allocator_live_bytes {}\n",
        memory.allocator_live_bytes
    ));
    if let Some(open) = memory.open_file_descriptors {
        output.push_str("# TYPE graph_process_open_file_descriptors gauge\n");
        output.push_str(&format!("graph_process_open_file_descriptors {open}\n"));
    }
    if let Some(limit) = memory.open_file_descriptor_soft_limit {
        output.push_str("# TYPE graph_process_open_file_descriptor_soft_limit gauge\n");
        output.push_str(&format!(
            "graph_process_open_file_descriptor_soft_limit {limit}\n"
        ));
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn process_memory_metrics() -> Option<ProcessMemoryMetrics> {
    let resident_bytes =
        parse_linux_rss_bytes(&std::fs::read_to_string("/proc/self/status").ok()?)?;
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
        let value = line
            .strip_prefix("VmRSS:")?
            .split_ascii_whitespace()
            .next()?;
        value.parse::<u64>().ok()?.checked_mul(1024)
    })
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
/// summed across the scopes open on this node, which is also the only way they
/// *can* be emitted: two scopes hosting the same `cell_id` would otherwise
/// render one series name and label set twice in a single scrape.
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
        "# TYPE graph_storage_write_bytes counter\n",
        "# TYPE graph_storage_block_cache_accesses counter\n",
        "# TYPE graph_storage_object_store_requests counter\n",
    ));
    // From the same enumeration the series below come from, so a family whose
    // shards are all absent still declares itself.
    append_histogram_types(
        output,
        GraphOperationalMetricsSnapshot::default().histogram_fields(),
    );
    // Histograms that do not carry `scope` are keyed by `cell_id` alone, and a
    // node may hold several scopes on one cell -- every scope on a query node
    // uses the same `GRAPH_CELL_ID`, so this is the normal case rather than a
    // corner. Rendering them inside the per-scope loop would emit the same
    // series name and label set once per scope in a single scrape, which
    // Prometheus rejects outright. They are summed here and rendered once,
    // exactly as `append_per_cell_counters` does for the counters that made
    // this same choice.
    let mut per_cell_histograms: BTreeMap<&str, BTreeMap<&'static str, DurationHistogramSnapshot>> =
        BTreeMap::new();
    for metrics in shard_metrics {
        let scope = metrics.scope.to_string();
        let metrics = &metrics.shard;
        let scoped = render_labels(&[("scope", &scope), ("cell_id", &metrics.cell_id)], None);
        for (field, value) in metrics.operational.counter_fields() {
            let Some(export) = prometheus_counter(CounterSource::Shard, field) else {
                debug_assert!(
                    false,
                    "{field} is enumerated but absent from PROMETHEUS_COUNTERS"
                );
                continue;
            };
            if let PrometheusCounterExport::ScopePerCell(name) = export.export {
                output.push_str(&format!("{name}{scoped} {value}\n"));
            }
        }
        for (cache, entries) in [
            ("matrix_artifacts", metrics.cache_entries.matrix_artifacts),
            (
                "matrix_adjacencies",
                metrics.cache_entries.matrix_adjacencies,
            ),
            (
                "graphblas_matrices",
                metrics.cache_entries.graphblas_matrices,
            ),
            (
                "parsed_row_queries",
                metrics.cache_entries.parsed_row_queries,
            ),
            (
                "relationship_rows",
                metrics.cache_entries.relationship_row_sets,
            ),
            (
                "relationship_property_rows",
                metrics.cache_entries.relationship_property_row_sets,
            ),
        ] {
            output.push_str(&format!(
                "graph_cache_entries{{scope=\"{}\",cell_id=\"{}\",cache=\"{cache}\"}} {entries}\n",
                scope, metrics.cell_id
            ));
        }
        for (cache, bytes) in [
            (
                "matrix_adjacencies",
                metrics.cache_resident_bytes.matrix_adjacencies,
            ),
            (
                "graphblas_matrices",
                metrics.cache_resident_bytes.graphblas_matrices,
            ),
            (
                "relationship_rows",
                metrics.cache_resident_bytes.relationship_rows,
            ),
            (
                "source_relationship_rows",
                metrics.cache_resident_bytes.source_relationship_rows,
            ),
            (
                "relationship_property_rows",
                metrics.cache_resident_bytes.relationship_property_rows,
            ),
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
            (
                "graph_storage_segment_max_l0_sst_count",
                storage.segment_max_l0_sst_count,
            ),
            (
                "graph_storage_immutable_memtable_flushes",
                storage.immutable_memtable_flushes,
            ),
            ("graph_storage_get_requests", storage.get_requests),
            ("graph_storage_scan_requests", storage.scan_requests),
            (
                "graph_storage_total_mem_size_bytes",
                storage.total_mem_size_bytes,
            ),
            (
                "graph_storage_backpressure_writes",
                storage.backpressure_writes,
            ),
            ("graph_storage_l0_write_stalls", storage.l0_write_stalls),
            ("graph_storage_compaction_bytes", storage.compaction_bytes),
            (
                "graph_storage_running_compactions",
                storage.running_compactions,
            ),
            (
                "graph_storage_last_compaction_timestamp_sec",
                storage.last_compaction_timestamp_sec,
            ),
        ] {
            output.push_str(&format!(
                "{name}{{scope=\"{}\",cell_id=\"{}\"}} {value}\n",
                scope, metrics.cell_id
            ));
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
            (
                "point",
                "false_positive",
                storage.sst_filter_point_false_positives,
            ),
            ("prefix", "positive", storage.sst_filter_prefix_positives),
            ("prefix", "negative", storage.sst_filter_prefix_negatives),
            (
                "prefix",
                "false_positive",
                storage.sst_filter_prefix_false_positives,
            ),
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
        for (op, value) in [
            ("get", storage.object_store_get_requests),
            ("put", storage.object_store_put_requests),
        ] {
            output.push_str(&format!(
                "graph_storage_object_store_requests{{scope=\"{}\",cell_id=\"{}\",op=\"{op}\"}} {value}\n",
                scope, metrics.cell_id
            ));
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
            histograms
                .iter()
                .map(|(field, snapshot)| (*field, snapshot)),
            &[("cell_id", cell_id)],
        );
    }
    append_per_cell_counters(output, shard_metrics);
    append_experimental_operator_families(output, shard_metrics);
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
    (
        "invocations",
        "graph_query_experimental_operator_invocations",
    ),
    ("rows_in", "graph_query_experimental_operator_rows_in"),
    ("rows_out", "graph_query_experimental_operator_rows_out"),
    (
        "self_us",
        "graph_query_experimental_operator_self_microseconds",
    ),
    (
        "storage_us",
        "graph_query_experimental_operator_storage_microseconds",
    ),
    (
        "storage_requests",
        "graph_query_experimental_operator_storage_requests",
    ),
    (
        "storage_bytes",
        "graph_query_experimental_operator_storage_bytes",
    ),
    (
        "hydrated_vertices",
        "graph_query_experimental_operator_hydrated_vertices",
    ),
    (
        "scanned_relationships",
        "graph_query_experimental_operator_scanned_relationships",
    ),
    (
        "peak_retained_rows",
        "graph_query_experimental_operator_peak_retained_rows",
    ),
    (
        "estimate_misses",
        "graph_query_experimental_operator_estimate_misses",
    ),
];

fn append_experimental_operator_families(
    output: &mut String,
    shard_metrics: &[ScopedGraphShardRuntimeMetrics],
) {
    let mut per_cell: BTreeMap<&str, BTreeMap<&'static str, ExperimentalOperatorMetricsSnapshot>> =
        BTreeMap::new();
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

/// Per-cell counter totals, summed over every scope open on this node.
///
/// Three maps rather than one map of a three-field row, because each is handed
/// to its renderer alongside the enumeration it belongs to and no renderer ever
/// wants two of them.
#[derive(Default)]
struct CellTotals<'a> {
    /// `GraphOperationalMetricsSnapshot::counter_fields`, per cell.
    operational: BTreeMap<&'a str, Vec<(&'static str, u64)>>,
    /// `GraphCacheMetricsSnapshot::counter_fields`, per cell.
    cache: BTreeMap<&'a str, Vec<(&'static str, u64)>>,
    /// `GraphOperationalMetricsSnapshot::class_counter_fields`, per cell.
    classes: BTreeMap<&'a str, Vec<(&'static str, &'static str, u64)>>,
}

/// Every shard counter that is not one of the three `{scope, cell_id}`
/// survivors, summed by `cell_id` and rendered family by family.
///
/// Family-major rather than shard-major, which is the opposite of the loop
/// above: these series are aggregated, so all of a family's cells are known at
/// once and can be emitted under a single `# TYPE` line. That is what the
/// exposition format actually asks for, and the shard-major block above only
/// departs from it because its output has to stay byte-identical.
fn append_per_cell_counters(output: &mut String, shard_metrics: &[ScopedGraphShardRuntimeMetrics]) {
    let mut totals = CellTotals::default();
    for metrics in shard_metrics {
        let cell_id = metrics.shard.cell_id.as_str();
        accumulate(
            totals.operational.entry(cell_id).or_default(),
            metrics.shard.operational.counter_fields(),
        );
        accumulate(
            totals.cache.entry(cell_id).or_default(),
            metrics.shard.cache.counter_fields(),
        );
        accumulate_classes(
            totals.classes.entry(cell_id).or_default(),
            metrics.shard.operational.class_counter_fields(),
        );
    }

    append_per_cell_family(
        output,
        CounterSource::Shard,
        GraphOperationalMetricsSnapshot::default().counter_fields(),
        &totals.operational,
    );
    append_per_cell_family(
        output,
        CounterSource::ShardCache,
        GraphCacheMetricsSnapshot::default().counter_fields(),
        &totals.cache,
    );
    append_per_cell_class_family(
        output,
        CounterSource::Shard,
        GraphOperationalMetricsSnapshot::default().class_counter_fields(),
        &totals.classes,
    );
}

/// Add one snapshot's counters into a running per-cell total.
///
/// Matched by field name rather than by position. The enumeration is in
/// declaration order and two snapshots of one type cannot disagree about it, so
/// indexing would work — but a linear match over this bounded field set costs nothing
/// and cannot silently add `write_commits` into `write_attempts` if that ever
/// stops being true.
fn accumulate(
    totals: &mut Vec<(&'static str, u64)>,
    fields: impl Iterator<Item = (&'static str, u64)>,
) {
    for (field, value) in fields {
        match totals.iter_mut().find(|(name, _)| *name == field) {
            // Saturating: the operands are `u64` counters that only a
            // decades-long uptime could overflow, and a wrapped total on a
            // metrics endpoint is a phantom incident.
            Some(slot) => slot.1 = slot.1.saturating_add(value),
            None => totals.push((field, value)),
        }
    }
}

/// [`accumulate`] for the `(field, class, count)` rows.
fn accumulate_classes(
    totals: &mut Vec<(&'static str, &'static str, u64)>,
    fields: impl Iterator<Item = (&'static str, &'static str, u64)>,
) {
    for (field, class, value) in fields {
        match totals
            .iter_mut()
            .find(|(name, existing, _)| *name == field && *existing == class)
        {
            Some(slot) => slot.2 = slot.2.saturating_add(value),
            None => totals.push((field, class, value)),
        }
    }
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
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_COUNTERS"
            );
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
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_COUNTERS"
            );
            continue;
        };
        let PrometheusCounterExport::PerCell(name) = export.export else {
            continue;
        };
        output.push_str(&format!("# TYPE {name} counter\n"));
        for (cell_id, cell) in totals {
            let value = cell
                .iter()
                .find(|(name, _)| *name == field)
                .map_or(0, |(_, value)| *value);
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
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_CLASS_COUNTERS"
            );
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
fn append_global_counters(
    output: &mut String,
    source: CounterSource,
    fields: impl Iterator<Item = (&'static str, u64)>,
) {
    for (field, value) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_COUNTERS"
            );
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
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_CLASS_COUNTERS"
            );
            continue;
        };
        let PrometheusCounterExport::Global(name) = export.export else {
            continue;
        };
        if declared != Some(field) {
            output.push_str(&format!("# TYPE {name} counter\n"));
            declared = Some(field);
        }
        output.push_str(&format!(
            "{name}{{{ERROR_CLASS_LABEL}=\"{class}\"}} {value}\n"
        ));
    }
}

fn append_global_failure_counters(
    output: &mut String,
    source: CounterSource,
    fields: impl Iterator<Item = (&'static str, &'static str, &'static str, u64)>,
) {
    // Declared by name, not by field: the two engines are two series of one
    // family, and a repeated `# TYPE` is a rejected scrape.
    let mut declared: Option<&'static str> = None;
    for (field, stage, reason, value) in fields {
        let Some(export) = prometheus_counter(source, field) else {
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_FAILURE_COUNTERS"
            );
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
fn append_histogram_types<'a>(
    output: &mut String,
    fields: impl Iterator<Item = (&'static str, &'a DurationHistogramSnapshot)>,
) {
    // Two fields can share a family name — the per-engine execution
    // histograms do — and a repeated `# TYPE` for one name makes the whole
    // scrape unparseable, so a name is declared once however many series
    // carry it.
    let mut declared: Vec<&'static str> = Vec::new();
    for (field, _) in fields {
        let Some(export) = prometheus_histogram(field) else {
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_HISTOGRAMS"
            );
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
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_HISTOGRAMS"
            );
            continue;
        };
        let owned: Vec<(&str, &str)>;
        let labels: &[(&str, &str)] = if export.carries_scope {
            labels
        } else {
            owned = labels
                .iter()
                .copied()
                .filter(|(key, _)| *key != "scope")
                .collect();
            &owned
        };
        // Cumulative, because that is what `le` means. The kernel counts per
        // bucket -- one `fetch_add` -- and `DurationHistogramSnapshot` owns the
        // accumulation, so this rendering and the meter's cannot drift.
        for (bound, cumulative) in snapshot.cumulative() {
            let le = bound.map_or_else(
                || LE_INFINITY.to_string(),
                |bound| export.unit.render_bound(bound),
            );
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
        output.push_str(&format!(
            "{}_count{labelled} {}\n",
            export.name,
            snapshot.count()
        ));
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

#[cfg(test)]
mod tests {
    use hydradb::{
        ClientQueryServiceConfig, GraphCacheMetricsSnapshot, GraphId, GraphMemoryConfig,
        GraphOpenOptions, GraphOperationalMetricsSnapshot, GraphScope, GraphShardRuntimeMetrics,
        NamespaceId, NamespacePath, ObjectStoreNodeDirectory, PlacementConfig, PlacementView,
        QueryCellClient, QueryTransportMetricsSnapshot, ScopedGraphShardRuntimeMetrics,
    };
    use slatedb::object_store::memory::InMemory;

    use super::*;

    /// Two shards with contrived-but-distinguishable values, so every series in
    /// `append_node_metrics` renders something a diff can see move.
    fn shard_metrics() -> Vec<ScopedGraphShardRuntimeMetrics> {
        let mut first = GraphOperationalMetricsSnapshot {
            query_graphblas_artifact_snapshots: 11,
            query_graphblas_rebuilt_snapshots: 22,
            query_rust_sparse_fallbacks: 33,
            query_experimental_property_seek_requests: 77,
            query_experimental_relationship_expand_requests: 88,
            query_experimental_ordered_property_scan_requests: 99,
            ..Default::default()
        };
        first.query_rows_latency.bucket_counts[0] = 4;
        first.query_rows_latency.bucket_counts[3] = 2;
        first.query_rows_latency.bucket_counts[17] = 1;
        first.query_rows_latency.sum_us = 31_000_909;
        first.query_rows_duration_us = first.query_rows_latency.sum_us;

        let second = GraphOperationalMetricsSnapshot {
            query_graphblas_artifact_snapshots: 44,
            query_graphblas_rebuilt_snapshots: 55,
            query_rust_sparse_fallbacks: 66,
            ..Default::default()
        };

        [("cell-a", first), ("cell-b", second)]
            .into_iter()
            .map(|(cell_id, operational)| ScopedGraphShardRuntimeMetrics {
                scope: GraphScope::default(),
                shard: GraphShardRuntimeMetrics {
                    cell_id: cell_id.to_string(),
                    operational,
                    cache: GraphCacheMetricsSnapshot::default(),
                    cache_entries: Default::default(),
                    cache_resident_bytes: Default::default(),
                    storage: Default::default(),
                },
            })
            .collect()
    }

    /// A query service over a routed cluster that never opens a scope. Enough
    /// to drive the handler; the counters it reports are all zero, which is
    /// what a freshly booted node reports too.
    fn query_service() -> ClientQueryService {
        let directory =
            ObjectStoreNodeDirectory::new(["cell-a".to_string()], ["graph-node-0".to_string()])
                .expect("a one-cell directory");
        let placement = PlacementView::new(
            "graph-node-0",
            ["graph-node-0".to_string()],
            PlacementConfig::default(),
        )
        .expect("a fleet of one");
        let node = Arc::new(
            ScopedRoutedGraphCluster::new(
                "graph/data",
                NamespacePath::default(),
                GraphId::default(),
                "graph-node-0",
                directory,
                placement,
                Arc::new(InMemory::new()),
                GraphOpenOptions::default(),
                GraphMemoryConfig::default(),
                4,
            )
            .expect("a routed cluster"),
        );
        ClientQueryService::new(
            node as Arc<dyn QueryCellClient>,
            ClientQueryServiceConfig::default(),
        )
        .expect("a query service")
    }

    /// The whole `/metrics` document, rendered through the real handler.
    ///
    /// Shard series are appended separately because a cluster that has opened
    /// no scope reports no shards, and the labelled half of the endpoint is
    /// exactly the half a rename would break.
    async fn rendered_metrics() -> String {
        let directory =
            ObjectStoreNodeDirectory::new(["cell-a".to_string()], ["graph-node-0".to_string()])
                .expect("a one-cell directory");
        let placement = PlacementView::new(
            "graph-node-0",
            ["graph-node-0".to_string()],
            PlacementConfig::default(),
        )
        .expect("a fleet of one");
        let routed_node = Arc::new(
            ScopedRoutedGraphCluster::new(
                "graph/data",
                NamespacePath::default(),
                GraphId::default(),
                "graph-node-0",
                directory,
                placement.clone(),
                Arc::new(InMemory::new()),
                GraphOpenOptions::default(),
                GraphMemoryConfig::default(),
                4,
            )
            .expect("a routed cluster"),
        );
        let ready = NodeReadiness::new(placement);
        ready.mark_ready();
        let state = AdminState {
            ready,
            query: query_service(),
            routed_node,
            memory_sampler: memory_sampler::MemorySampler::start(),
            control_token: Arc::from("unused-by-the-metrics-handler"),
        };

        let response = metrics(State(state)).await;
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("a complete body");
        let mut document = String::from_utf8(body.to_vec()).expect("utf-8");
        append_node_metrics(&mut document, &shard_metrics());
        document
    }

    /// Writes `/metrics` to `$HYDRADB_METRICS_CAPTURE` when it is set, so the
    /// endpoint can be diffed across a change to it. Asserts nothing on its
    /// own: the assertion is the diff.
    #[tokio::test]
    async fn capture_metrics_document() {
        let document = rendered_metrics().await;
        if let Ok(path) = std::env::var("HYDRADB_METRICS_CAPTURE") {
            std::fs::write(path, &document).expect("capture is writable");
        }
        assert!(document.contains("graph_runtime_ready 1\n"));
    }

    /// The build-info series opens the document, so `curl … | head -2` names
    /// the build. Asserted on the rendered endpoint rather than on
    /// `prometheus_gauge` alone because what can regress here is the wiring —
    /// a future edit that appends the gauge instead of seeding `output` with
    /// it would still pass a test of the telemetry crate.
    #[tokio::test]
    async fn metrics_open_with_the_build_info_series() {
        let document = rendered_metrics().await;
        let mut lines = document.lines();
        assert_eq!(lines.next(), Some("# TYPE graph_build_info gauge"));
        let series = lines.next().expect("a build_info series");
        assert!(series.starts_with("graph_build_info{"), "{series}");
        assert!(series.ends_with("} 1"), "{series}");
        assert!(series.contains("commit=\""), "{series}");
        // The series the endpoint used to open with is still present and
        // unchanged; this gauge was added ahead of it, not in place of it.
        assert!(document.contains("graph_runtime_ready 1\n"));
    }

    #[test]
    fn slatedb_cache_metrics_report_the_actual_ram_budget() {
        let mut output = String::new();
        append_slatedb_cache_metrics(
            &mut output,
            hydradb::SlateDbCacheMetricsSnapshot {
                capacity_bytes: 671_088_640,
                resident_bytes: 12_345,
                entries: 7,
            },
        );
        assert!(output.contains("graph_slatedb_cache_capacity_bytes 671088640\n"));
        assert!(output.contains("graph_slatedb_cache_resident_bytes 12345\n"));
        assert!(output.contains("graph_slatedb_cache_entries 7\n"));
    }

    #[tokio::test]
    async fn blocked_shard_collection_is_cancelled_without_publishing_false_zeroes() {
        let mut output = String::from("graph_runtime_ready 1\n");
        append_bounded_node_metrics(
            &mut output,
            std::future::pending(),
            std::time::Duration::from_millis(1),
        )
        .await;
        crate::otel_metrics::memory_diagnostics::append(&mut output);
        assert!(output.contains("graph_runtime_shard_metrics_collection_success 0\n"));
        assert!(output.contains("graph_memory_diagnostic_items{"));
        assert!(!output.contains("graph_storage_total_mem_size_bytes"));
        let mut completed = String::new();
        append_bounded_node_metrics(
            &mut completed,
            std::future::ready(shard_metrics()),
            std::time::Duration::from_secs(1),
        )
        .await;
        assert!(completed.contains("graph_runtime_shard_metrics_collection_success 1\n"));
        assert!(completed.contains("graph_storage_total_mem_size_bytes"));
    }

    #[test]
    fn process_memory_metrics_render_rss_and_allocator_accounting() {
        let mut output = String::new();
        append_process_memory_metrics(
            &mut output,
            ProcessMemoryMetrics {
                resident_bytes: 13_521_796 * 1024,
                allocator_arena_bytes: 11_396_000_000,
                allocator_arena_in_use_bytes: 1_234_000_000,
                allocator_arena_free_bytes: 10_162_000_000,
                allocator_mmap_bytes: 4_000_000_000,
                allocator_live_bytes: 5_234_000_000,
                open_file_descriptors: Some(377),
                open_file_descriptor_soft_limit: Some(65_536),
            },
        );

        for line in [
            "# TYPE graph_process_resident_memory_bytes gauge\n",
            "# TYPE graph_process_allocator_arena_bytes gauge\n",
            "# TYPE graph_process_allocator_arena_in_use_bytes gauge\n",
            "# TYPE graph_process_allocator_arena_free_bytes gauge\n",
            "# TYPE graph_process_allocator_mmap_bytes gauge\n",
            "# TYPE graph_process_allocator_live_bytes gauge\n",
            "graph_process_resident_memory_bytes 13846319104\n",
            "graph_process_allocator_arena_bytes 11396000000\n",
            "graph_process_allocator_arena_in_use_bytes 1234000000\n",
            "graph_process_allocator_arena_free_bytes 10162000000\n",
            "graph_process_allocator_mmap_bytes 4000000000\n",
            "graph_process_allocator_live_bytes 5234000000\n",
            "graph_process_open_file_descriptors 377\n",
            "graph_process_open_file_descriptor_soft_limit 65536\n",
        ] {
            assert!(output.contains(line), "{line:?} did not render");
        }
    }

    #[test]
    fn linux_rss_parser_requires_a_valid_vmrss_row() {
        assert_eq!(
            parse_linux_rss_bytes("Name:\tgraph-node\nVmRSS:\t13521796 kB\n"),
            Some(13_521_796 * 1024)
        );
        assert_eq!(parse_linux_rss_bytes("VmSize:\t13521796 kB\n"), None);
        assert_eq!(parse_linux_rss_bytes("VmRSS:\tnot-a-number kB\n"), None);
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn linux_process_memory_snapshot_reads_rss_and_glibc_allocator() {
        let memory = process_memory_metrics().expect("Linux procfs and glibc are available");
        assert!(memory.resident_bytes > 0);
        assert!(memory.allocator_arena_bytes >= memory.allocator_arena_free_bytes);
        assert!(memory.allocator_live_bytes >= memory.allocator_mmap_bytes);
        assert!(memory.open_file_descriptors.is_some_and(|open| open > 0));
        assert!(memory
            .open_file_descriptor_soft_limit
            .is_some_and(|limit| limit > 0));
    }

    /// The histogram family is **additive**. Every series this endpoint served
    /// before it existed keeps its exact name, its exact labels and its exact
    /// value -- a scraper's recording rules and every dashboard built on them
    /// are downstream of these strings, and renaming one is a silent outage in
    /// a dashboard rather than a loud one here.
    #[test]
    fn experimental_operator_families_render_per_cell_and_operator() {
        let operator = |requests| ExperimentalOperatorMetricsSnapshot {
            operator: "ExpandExec",
            requests,
            invocations: 3,
            hydrated_vertices: 40,
            ..Default::default()
        };
        let mut metrics = shard_metrics();
        metrics[0].shard.operational.experimental_operators = vec![operator(2)];
        // A second scope on the same cell sums into the same series.
        let mut same_cell = metrics[0].clone();
        same_cell.scope = GraphScope::new(
            NamespacePath::new([NamespaceId::new("other").unwrap()]).unwrap(),
            GraphId::default(),
        );
        same_cell.shard.operational.experimental_operators = vec![operator(5)];
        metrics.push(same_cell);
        let mut output = String::new();
        append_experimental_operator_families(&mut output, &metrics);
        for (index, (field, _)) in EXPERIMENTAL_OPERATOR_FAMILIES.iter().enumerate() {
            assert_eq!(
                ExperimentalOperatorMetricsSnapshot::default().counter_fields()[index].0,
                *field
            );
        }
        assert!(
            output.contains("# TYPE graph_query_experimental_operator_estimate_misses counter\n")
        );
        let cell = &metrics[0].shard.cell_id;
        assert!(output.contains(&format!(
            "graph_query_experimental_operator_requests{{cell_id=\"{cell}\",operator=\"ExpandExec\"}} 7\n"
        )));
        assert!(output.contains(&format!(
            "graph_query_experimental_operator_hydrated_vertices{{cell_id=\"{cell}\",operator=\"ExpandExec\"}} 80\n"
        )));
        assert!(!output.contains("scope="));
    }

    #[tokio::test]
    async fn the_pre_existing_series_are_untouched() {
        let document = rendered_metrics().await;
        for line in [
            "# TYPE graph_runtime_ready gauge\n",
            "graph_runtime_ready 1\n",
            "# TYPE graph_query_started counter\ngraph_query_started 0\n",
            "# TYPE graph_query_completed counter\ngraph_query_completed 0\n",
            "# TYPE graph_query_failed counter\ngraph_query_failed 0\n",
            "# TYPE graph_query_auth_failures counter\ngraph_query_auth_failures 0\n",
            "# TYPE graph_query_scope_denials counter\ngraph_query_scope_denials 0\n",
            "graph_query_graphblas_artifact_snapshots{scope=\"default/graphs/default\",cell_id=\"cell-a\"} 11\n",
            "graph_query_graphblas_rebuilt_snapshots{scope=\"default/graphs/default\",cell_id=\"cell-a\"} 22\n",
            "graph_query_rust_sparse_fallbacks{scope=\"default/graphs/default\",cell_id=\"cell-b\"} 66\n",
            "graph_cache_entries{scope=\"default/graphs/default\",cell_id=\"cell-a\",cache=\"graphblas_matrices\"} 0\n",
            "graph_cache_resident_bytes{scope=\"default/graphs/default\",cell_id=\"cell-b\",cache=\"relationship_rows\"} 0\n",
        ] {
            assert!(document.contains(line), "{line:?} no longer appears");
        }
    }

    #[tokio::test]
    async fn experimental_operator_requests_render_per_cell() {
        let document = rendered_metrics().await;
        for line in [
            "# TYPE graph_query_experimental_property_seek_requests counter\n",
            "# TYPE graph_query_experimental_relationship_expand_requests counter\n",
            "# TYPE graph_query_experimental_ordered_property_scan_requests counter\n",
            "graph_query_experimental_property_seek_requests{cell_id=\"cell-a\"} 77\n",
            "graph_query_experimental_relationship_expand_requests{cell_id=\"cell-a\"} 88\n",
            "graph_query_experimental_ordered_property_scan_requests{cell_id=\"cell-a\"} 99\n",
            "graph_query_experimental_property_seek_requests{cell_id=\"cell-b\"} 0\n",
            "graph_query_experimental_relationship_expand_requests{cell_id=\"cell-b\"} 0\n",
            "graph_query_experimental_ordered_property_scan_requests{cell_id=\"cell-b\"} 0\n",
        ] {
            assert!(document.contains(line), "{line:?} did not render");
        }
    }

    /// The SlateDB storage-engine families declare their `# TYPE` and render one
    /// series per (scope, cell_id), the same shape as the cache gauges. The
    /// fixture opens no real store, so the values are the zero snapshot -- this
    /// asserts the wiring renders, not the counts.
    #[tokio::test]
    async fn the_storage_families_render_per_scope() {
        let document = rendered_metrics().await;
        for line in [
            "# TYPE graph_storage_l0_sst_count gauge\n",
            "# TYPE graph_storage_scan_requests counter\n",
            "# TYPE graph_storage_get_requests counter\n",
            "# TYPE graph_storage_sst_filter_checks counter\n",
            "# TYPE graph_storage_compaction_bytes counter\n",
            "# TYPE graph_storage_write_bytes counter\n",
            "# TYPE graph_storage_block_cache_accesses counter\n",
            "# TYPE graph_storage_object_store_requests counter\n",
            "graph_storage_l0_sst_count{scope=\"default/graphs/default\",cell_id=\"cell-a\"} 0\n",
            "graph_storage_scan_requests{scope=\"default/graphs/default\",cell_id=\"cell-a\"} 0\n",
            "graph_storage_backpressure_writes{scope=\"default/graphs/default\",cell_id=\"cell-a\"} 0\n",
            "graph_storage_sst_filter_checks{scope=\"default/graphs/default\",cell_id=\"cell-a\",kind=\"prefix\",outcome=\"positive\"} 0\n",
            "graph_storage_write_bytes{scope=\"default/graphs/default\",cell_id=\"cell-a\",stage=\"memtable\"} 0\n",
            "graph_storage_block_cache_accesses{scope=\"default/graphs/default\",cell_id=\"cell-a\",entry_kind=\"filter\",result=\"miss\"} 0\n",
            "graph_storage_object_store_requests{scope=\"default/graphs/default\",cell_id=\"cell-a\",op=\"get\"} 0\n",
        ] {
            assert!(document.contains(line), "{line:?} did not render");
        }
    }

    /// `_count` is derived by summing the buckets, so it and `le="+Inf"` are
    /// equal by construction rather than by two `fetch_add`s staying in step.
    /// The rendering must not lose that.
    #[tokio::test]
    async fn the_count_line_equals_the_overflow_bucket() {
        let document = rendered_metrics().await;
        let labels = "{scope=\"default/graphs/default\",cell_id=\"cell-a\"";
        assert!(document.contains(&format!(
            "graph_query_rows_duration_microseconds_bucket{labels},le=\"+Inf\"}} 7\n"
        )));
        assert!(document.contains(&format!(
            "graph_query_rows_duration_microseconds_count{labels}}} 7\n"
        )));
        assert!(document.contains(&format!(
            "graph_query_rows_duration_microseconds_sum{labels}}} 31000909\n"
        )));
    }

    /// The seconds family is the one that can be wrong by a factor of a
    /// million, and the `le` bounds are the join between the two exports: an
    /// `0.000100000` here against a `0.0001` in OTLP is two series that look
    /// like one.
    #[tokio::test]
    async fn the_client_family_renders_seconds() {
        let document = rendered_metrics().await;
        assert!(
            document.contains("# TYPE graph_client_operation_read_duration_seconds histogram\n")
        );
        assert!(document.contains(
            "graph_client_operation_read_duration_seconds_bucket{cypher_engine=\"legacy\",le=\"0.0001\"} 0\n"
        ));
        assert!(document.contains(
            "graph_client_operation_write_duration_seconds_bucket{cypher_engine=\"legacy\",le=\"30\"} 0\n"
        ));
        assert!(document.contains(
            "graph_client_operation_write_duration_seconds_count{cypher_engine=\"legacy\"} 0\n"
        ));
    }

    /// The engine label is the one dimension the client family carries, and
    /// both values render on every node, whatever it is configured or
    /// overridden to: the histograms are per engine, so a node the kill switch
    /// has flipped holds two populations and neither may inherit the other's
    /// observations. An engine that has run nothing renders at zero rather
    /// than going absent, so a panel keeps its series across a flip.
    #[tokio::test]
    async fn the_client_family_carries_the_engine_that_ran_each_population() {
        let document = rendered_metrics().await;
        // One `# TYPE` per family however many engines carry it: a repeated
        // one is a rejected scrape.
        for family in [
            "graph_client_operation_read_duration_seconds",
            "graph_client_operation_write_duration_seconds",
        ] {
            assert_eq!(
                document
                    .lines()
                    .filter(|line| *line == format!("# TYPE {family} histogram"))
                    .count(),
                1,
                "{family} declared its type twice"
            );
        }
        for engine in ["legacy", "experimental"] {
            assert_eq!(
                document
                    .lines()
                    .filter(|line| line.starts_with("graph_client_operation_")
                        && line.contains(&format!("cypher_engine=\"{engine}\"")))
                    .count(),
                // Two families, each one `_bucket` per ladder bucket (`+Inf`
                // included) plus `_sum` and `_count`.
                2 * (hydradb::DURATION_BUCKET_COUNT + 2),
                "{engine} is missing from the client families"
            );
        }
        assert!(
            hydradb_telemetry::semconv::METRIC_LABELS
                .iter()
                .any(|label| label.key() == format!("hydradb.{CYPHER_ENGINE_LABEL}")),
            "the Prometheus label and the registry key have drifted",
        );
        // And only the execution families: a labelled bookmark-wait series
        // would split the one ratio that family exists for.
        assert!(
            !document
                .lines()
                .any(|line| line.starts_with("graph_client_bookmark_wait")
                    && line.contains(CYPHER_ENGINE_LABEL)),
            "the bookmark-wait family took the engine label"
        );
    }

    /// The failure-reason family: every valid label combination renders at
    /// zero, so a `rate()` over a bucket that has never failed is a flat line
    /// rather than an absent series, and every series names the engine.
    #[tokio::test]
    async fn the_failure_reason_family_renders_every_bucket_at_zero() {
        let document = rendered_metrics().await;
        assert!(document.contains("# TYPE graph_query_failed_by_reason counter\n"));
        let series: Vec<&str> = document
            .lines()
            .filter(|line| line.starts_with("graph_query_failed_by_reason{"))
            .collect();
        // 2 engines x 2 stages x 14 reasons; only the query class, so no
        // error_class. Both engines render on every node: the counters are per
        // engine, so a flip moves nothing already counted, and an engine that
        // has failed nothing is a zero rather than an absent series.
        assert_eq!(series.len(), 2 * 2 * hydradb::QueryFailureReason::COUNT);
        for engine in ["legacy", "experimental"] {
            assert_eq!(
                series
                    .iter()
                    .filter(|line| line.contains(&format!("cypher_engine=\"{engine}\"")))
                    .count(),
                2 * hydradb::QueryFailureReason::COUNT,
                "{engine} is missing from the failure-reason family"
            );
        }
        assert_eq!(
            document
                .lines()
                .filter(|line| *line == "# TYPE graph_query_failed_by_reason counter")
                .count(),
            1,
            "the failure-reason family declared its type twice"
        );
        assert!(!series.iter().any(|line| line.contains("error_class")));
        for line in [
            "graph_query_failed_by_reason{cypher_engine=\"legacy\",stage=\"prepare\",\
             reason=\"unsupported_where\"} 0",
            "graph_query_failed_by_reason{cypher_engine=\"legacy\",stage=\"execute\",\
             reason=\"invalid_request\"} 0",
            "graph_query_failed_by_reason{cypher_engine=\"experimental\",stage=\"execute\",\
             reason=\"invalid_request\"} 0",
        ] {
            assert!(series.contains(&line), "{line:?} did not render");
        }
    }

    /// The bookmark-wait family, end to end through the real handler.
    ///
    /// The name table is checked for *consistency* by
    /// `crate::otel_metrics::tests::every_histogram_field_reaches_both_exports`,
    /// which proves a field is named in both exports and says nothing about
    /// whether a byte of it reaches a scraper. This is the other half: the
    /// series an operator would paste into a dashboard, spelled exactly as
    /// `/metrics` renders it.
    ///
    /// Five counters and a histogram, because the ratio is the point.
    /// `graph_client_bookmark_waits_polled / graph_client_bookmark_waits` is
    /// the alert change 4 of
    /// `docs/plans/2026-08-21-cell-affine-read-routing.md` asks for, and a
    /// numerator whose denominator failed to render is not alertable — so both
    /// halves are asserted, and asserted as complete `# TYPE` + series pairs
    /// rather than as substrings that a prefix of some other family could
    /// satisfy.
    #[tokio::test]
    async fn the_bookmark_wait_family_renders() {
        let document = rendered_metrics().await;
        for line in [
            "# TYPE graph_client_bookmark_waits counter\ngraph_client_bookmark_waits 0\n",
            "# TYPE graph_client_bookmark_waits_polled counter\ngraph_client_bookmark_waits_polled 0\n",
            "# TYPE graph_client_bookmark_waits_declined counter\ngraph_client_bookmark_waits_declined 0\n",
            "# TYPE graph_client_bookmark_waits_on_cell_writer counter\ngraph_client_bookmark_waits_on_cell_writer 0\n",
            "# TYPE graph_client_bookmark_waits_off_cell_writer counter\ngraph_client_bookmark_waits_off_cell_writer 0\n",
            "# TYPE graph_client_bookmark_wait_duration_microseconds histogram\n",
            // Microseconds, not seconds: a `le="0.0001"` here would be the
            // same measurement read off by a factor of a million, and the
            // unit is the one thing the two exports may not disagree about.
            "graph_client_bookmark_wait_duration_microseconds_bucket{le=\"100\"} 0\n",
            "graph_client_bookmark_wait_duration_microseconds_bucket{le=\"30000000\"} 0\n",
            "graph_client_bookmark_wait_duration_microseconds_bucket{le=\"+Inf\"} 0\n",
            "graph_client_bookmark_wait_duration_microseconds_sum 0\n",
            "graph_client_bookmark_wait_duration_microseconds_count 0\n",
        ] {
            assert!(document.contains(line), "{line:?} did not render");
        }
        // Process-global, like every other client series: a `cell_id` or
        // `scope` label would split the one ratio the family exists for.
        assert!(
            !document.contains("graph_client_bookmark_waits{"),
            "the bookmark-wait counters took a label"
        );
    }

    /// The transport families render from the same enumeration and the same
    /// name table as everything else. They are asserted here rather than
    /// through `/metrics` because both rows are
    /// `crate::otel_metrics::FieldSource::TransportOnly` -- this binary holds no
    /// `QueryTransportMetricsSnapshot`, for the reasons that type documents.
    ///
    /// So this test *is* the only exercise those two families get, which is why
    /// it renders them from a hand-built snapshot rather than skipping them: a
    /// name table row nothing ever renders is a row whose `le` arithmetic and
    /// unit suffix nobody has checked.
    #[test]
    fn the_transport_families_render_from_the_same_enumeration() {
        let mut snapshot = QueryTransportMetricsSnapshot::default();
        snapshot.serve_latency.bucket_counts[11] = 3;
        snapshot.serve_latency.bucket_counts[17] = 2;
        snapshot.serve_latency.sum_us = 91_500_000;
        snapshot.remote_latency_us = snapshot.serve_latency.sum_us;

        let mut output = String::new();
        append_histogram_types(&mut output, snapshot.histogram_fields());
        append_histograms(&mut output, snapshot.histogram_fields(), &[]);

        assert!(
            output.contains("# TYPE graph_query_transport_rpc_duration_microseconds histogram\n")
        );
        assert!(
            output.contains("# TYPE graph_query_transport_serve_duration_microseconds histogram\n")
        );
        // 500ms is a pinned bound because it is `slow_query_log_threshold`:
        // the mass above it and the `slow_queries` counter are the same event,
        // so the cumulative count *at* the bound is what reconciles them.
        assert!(output.contains(
            "graph_query_transport_serve_duration_microseconds_bucket{le=\"500000\"} 3\n"
        ));
        assert!(output
            .contains("graph_query_transport_serve_duration_microseconds_bucket{le=\"+Inf\"} 5\n"));
        assert!(output.contains("graph_query_transport_serve_duration_microseconds_count 5\n"));
        assert!(output.contains("graph_query_transport_rpc_duration_microseconds_count 0\n"));
    }

    /// Two tenants on one node share a `cell_id`, and the counters that carry
    /// no `scope` must therefore be **summed**, not emitted twice.
    ///
    /// This is the failure mode the shape decision exists to avoid, and it is a
    /// hard one: `graph_write_attempts{cell_id="cell-a"}` appearing twice with
    /// two values in a single scrape is not a wrong number, it is a rejected
    /// scrape — Prometheus drops the whole response.
    #[test]
    fn per_cell_counters_are_summed_across_scopes() {
        let shards: Vec<ScopedGraphShardRuntimeMetrics> = [("alpha", 3u64), ("beta", 4u64)]
            .into_iter()
            .map(|(tenant, writes)| ScopedGraphShardRuntimeMetrics {
                scope: GraphScope::tenant(
                    NamespaceId::new(tenant).expect("a valid namespace id"),
                    GraphId::default(),
                ),
                shard: GraphShardRuntimeMetrics {
                    cell_id: "cell-a".to_string(),
                    operational: GraphOperationalMetricsSnapshot {
                        write_attempts: writes,
                        ..Default::default()
                    },
                    cache: GraphCacheMetricsSnapshot {
                        matrix_artifact_hits: writes * 10,
                        ..Default::default()
                    },
                    cache_entries: Default::default(),
                    cache_resident_bytes: Default::default(),
                    storage: Default::default(),
                },
            })
            .collect();

        let mut output = String::new();
        append_per_cell_counters(&mut output, &shards);

        assert_eq!(
            output.matches("graph_write_attempts{").count(),
            1,
            "one series per cell, whatever the tenant count: {output}"
        );
        assert!(output.contains("graph_write_attempts{cell_id=\"cell-a\"} 7\n"));
        assert!(output.contains("graph_cache_matrix_artifact_hits{cell_id=\"cell-a\"} 70\n"));
    }

    /// A histogram that does not carry `scope` must be **summed** across the
    /// scopes sharing its cell, not rendered once per scope.
    ///
    /// This is the counters' `PerCell` hazard applied to a family with twenty
    /// series instead of one: every scope on a query node runs the same
    /// `GRAPH_CELL_ID`, so two open scopes would otherwise emit
    /// `..._bucket{cell_id="cell-a",le="100"}` twice in a single scrape with
    /// different values, which Prometheus rejects outright. The failure is a
    /// rejected scrape of the *whole endpoint*, not a wrong number on one
    /// family, which is why it is worth a test of its own rather than trusting
    /// the label list.
    #[test]
    fn a_scopeless_histogram_is_summed_across_scopes_sharing_a_cell() {
        // Deliberately different observations per scope: a merge that took one
        // shard and dropped the other would still emit one series, and only the
        // arithmetic tells the two apart.
        let shards: Vec<ScopedGraphShardRuntimeMetrics> = [("alpha", 100u64), ("beta", 2_500u64)]
            .into_iter()
            .map(|(tenant, micros)| {
                let mut fetches = DurationHistogramSnapshot::default();
                fetches.bucket_counts[bucket_of(micros)] = 3;
                fetches.sum_us = micros * 3;
                ScopedGraphShardRuntimeMetrics {
                    scope: GraphScope::tenant(
                        NamespaceId::new(tenant).expect("a valid namespace id"),
                        GraphId::default(),
                    ),
                    shard: GraphShardRuntimeMetrics {
                        cell_id: "cell-a".to_string(),
                        operational: GraphOperationalMetricsSnapshot {
                            query_property_fetch_latency: fetches,
                            ..Default::default()
                        },
                        cache: Default::default(),
                        cache_entries: Default::default(),
                        cache_resident_bytes: Default::default(),
                        storage: Default::default(),
                    },
                }
            })
            .collect();

        let mut output = String::new();
        append_node_metrics(&mut output, &shards);

        for suffix in ["_sum", "_count"] {
            assert_eq!(
                output
                    .matches(&format!(
                        "graph_query_property_fetch_duration_microseconds{suffix}{{"
                    ))
                    .count(),
                1,
                "one series per cell however many scopes are open: {output}"
            );
        }
        assert_eq!(
            output
                .matches("graph_query_property_fetch_duration_microseconds_bucket{cell_id=\"cell-a\",le=\"100\"}")
                .count(),
            1,
            "a duplicated bucket line is a rejected scrape: {output}"
        );
        // 3 + 3 observations, 300 + 7500 microseconds: both shards, added.
        assert!(output.contains(
            "graph_query_property_fetch_duration_microseconds_count{cell_id=\"cell-a\"} 6\n"
        ));
        assert!(output.contains(
            "graph_query_property_fetch_duration_microseconds_sum{cell_id=\"cell-a\"} 7800\n"
        ));
        // The scoped family beside it is untouched: one series per scope still.
        assert_eq!(
            output
                .matches("graph_query_rows_duration_microseconds_count{scope=")
                .count(),
            2,
            "a family that carries scope keeps one series per scope: {output}"
        );
    }

    /// The bucket a duration lands in, derived from the exported bounds rather
    /// than hardcoded, so the test does not have to be rewritten when the
    /// ladder changes.
    fn bucket_of(micros: u64) -> usize {
        hydradb::DURATION_BUCKET_BOUNDS_US.partition_point(|bound| *bound < micros)
    }

    /// `scope` is unbounded per tenant, so the families that carry it are a
    /// fixed list of six and nothing may join them.
    ///
    /// Six, not the five §1.4 counted: `8d7e939` gave the shard row-query
    /// histogram the same `{scope, cell_id}` labels as the counters it sits
    /// beside, which was the consistent choice at the time and is the sixth
    /// family here. Every other series on this endpoint is either process-global
    /// or keyed by `cell_id` alone.
    #[tokio::test]
    async fn only_the_pre_existing_families_carry_a_scope_label() {
        const SCOPED: &[&str] = &[
            "graph_query_graphblas_artifact_snapshots",
            "graph_query_graphblas_rebuilt_snapshots",
            "graph_query_rust_sparse_fallbacks",
            "graph_cache_entries",
            "graph_cache_resident_bytes",
            "graph_query_rows_duration_microseconds",
            // SlateDB storage-engine gauges/counters, one series per (scope,
            // cell_id) like the cache families above. Bounded the same way:
            // scope count is the number of graphs open on the node, cell_id is
            // one value per node.
            "graph_storage_l0_sst_count",
            "graph_storage_segment_max_l0_sst_count",
            "graph_storage_immutable_memtable_flushes",
            "graph_storage_get_requests",
            "graph_storage_scan_requests",
            "graph_storage_total_mem_size_bytes",
            // The proof set for the prefix-scan work: filter outcomes carry
            // fixed {kind, outcome} pairs (2×3), write bytes a fixed 3-value
            // {stage}, cache accesses a fixed {entry_kind, result} (2×2),
            // object-store requests a fixed 2-value {op}. All sub-labels are
            // enumerated in the render loops, so per-scope cardinality stays a
            // constant 27 series.
            "graph_storage_sst_filter_checks",
            "graph_storage_backpressure_writes",
            "graph_storage_l0_write_stalls",
            "graph_storage_compaction_bytes",
            "graph_storage_running_compactions",
            "graph_storage_last_compaction_timestamp_sec",
            "graph_storage_write_bytes",
            "graph_storage_block_cache_accesses",
            "graph_storage_object_store_requests",
        ];
        for line in rendered_metrics().await.lines() {
            if !line.contains("scope=\"") {
                continue;
            }
            let name = line.split(['{', ' ']).next().unwrap_or_default();
            assert!(
                SCOPED.iter().any(|family| name.starts_with(family)),
                "{name} is a new series carrying an unbounded scope label"
            );
        }
    }

    #[tokio::test]
    async fn admin_server_serves_livez_and_healthz_routes() {
        let directory =
            ObjectStoreNodeDirectory::new(["cell-a".to_string()], ["graph-node-0".to_string()])
                .expect("a one-cell directory");
        let placement = PlacementView::new(
            "graph-node-0",
            ["graph-node-0".to_string()],
            PlacementConfig::default(),
        )
        .expect("a fleet of one");
        let routed_node = Arc::new(
            ScopedRoutedGraphCluster::new(
                "graph/data",
                NamespacePath::default(),
                GraphId::default(),
                "graph-node-0",
                directory,
                placement.clone(),
                Arc::new(InMemory::new()),
                GraphOpenOptions::default(),
                GraphMemoryConfig::default(),
                4,
            )
            .expect("a routed cluster"),
        );
        let ready = NodeReadiness::new(placement);
        ready.mark_ready();

        let server = AdminServer::bind_scoped(
            "127.0.0.1:0".parse().unwrap(),
            ready,
            query_service(),
            routed_node,
        )
        .await
        .expect("admin server binds");

        let client = reqwest::Client::new();
        let livez = client
            .get(format!("http://{}/livez", server.local_addr()))
            .send()
            .await
            .unwrap();
        assert_eq!(livez.status(), reqwest::StatusCode::OK);

        let healthz = client
            .get(format!("http://{}/healthz", server.local_addr()))
            .send()
            .await
            .unwrap();
        assert_eq!(healthz.status(), reqwest::StatusCode::OK);

        let readyz = client
            .get(format!("http://{}/readyz", server.local_addr()))
            .send()
            .await
            .unwrap();
        assert_eq!(readyz.status(), reqwest::StatusCode::OK);

        server.stop().await.unwrap();
    }
}
// weave: run 'weave explain src/bin/graph_node/admin.rs' for per-hunk detail, 'weave check' to verify your resolution
