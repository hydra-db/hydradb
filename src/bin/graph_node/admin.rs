use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use slatedb_graph_kernel::{
    ClientQueryService, DurationHistogramSnapshot, GraphCacheMetricsSnapshot,
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
pub const PROMETHEUS_HISTOGRAMS: &[PrometheusHistogram] = &[
    PrometheusHistogram {
        field: "read_latency",
        name: "graph_client_operation_read_duration_seconds",
        unit: ExportUnit::Seconds,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "write_latency",
        name: "graph_client_operation_write_duration_seconds",
        unit: ExportUnit::Seconds,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "query_rows_latency",
        name: "graph_query_rows_duration_microseconds",
        unit: ExportUnit::Microseconds,
        source: FieldSource::GraphNode,
    },
    PrometheusHistogram {
        field: "rpc_latency",
        name: "graph_query_transport_rpc_duration_microseconds",
        unit: ExportUnit::Microseconds,
        source: FieldSource::TransportOnly,
    },
    PrometheusHistogram {
        field: "serve_latency",
        name: "graph_query_transport_serve_duration_microseconds",
        unit: ExportUnit::Microseconds,
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

/// The Prometheus row for a `(source, field)` pair, over both counter tables.
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

#[derive(Clone)]
struct AdminState {
    ready: NodeReadiness,
    query: ClientQueryService,
    routed_node: Arc<ScopedRoutedGraphCluster>,
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
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(admin_io_error)?;
        let local_addr = listener.local_addr().map_err(admin_io_error)?;
        let state = AdminState {
            ready,
            query,
            routed_node: node,
        };
        Self::serve(listener, local_addr, state)
    }

    fn serve(listener: TcpListener, local_addr: SocketAddr, state: AdminState) -> Result<Self> {
        let router = Router::new()
            .route("/livez", get(live))
            .route("/readyz", get(readiness))
            .route("/metrics", get(metrics))
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
            .map_err(|err| slatedb_graph_kernel::GraphError::CorruptValue {
                key: "runtime/admin".to_string(),
                reason: err.to_string(),
            })?
    }
}

async fn live() -> StatusCode {
    StatusCode::OK
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
    let mut output = format!(
        "# TYPE graph_runtime_ready gauge\ngraph_runtime_ready {}\n",
        u8::from(state.ready.is_ready()),
    );
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
    // Additive, and deliberately after the counters rather than interleaved
    // with them: every series above keeps the exact name, labels and value it
    // had before the histograms existed.
    append_histogram_types(&mut output, query.histogram_fields());
    append_histograms(&mut output, query.histogram_fields(), &[]);
    append_node_metrics(
        &mut output,
        &state.routed_node.local_shard_runtime_metrics().await,
    );
    (
        [
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        output,
    )
        .into_response()
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
        "# TYPE graph_cache_resident_bytes gauge\n"
    ));
    // From the same enumeration the series below come from, so a family whose
    // shards are all absent still declares itself.
    append_histogram_types(
        output,
        GraphOperationalMetricsSnapshot::default().histogram_fields(),
    );
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
            (
                "native_path_results",
                metrics.cache_entries.native_path_results,
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
            // The gauge the operator actually reads. Adding the field to
            // `GraphCacheResidentBytes` without adding it here would fix the
            // struct and leave the symptom — a resident-bytes series that is
            // correct for five caches and blind to the sixth.
            (
                "native_path_results",
                metrics.cache_resident_bytes.native_path_results,
            ),
        ] {
            output.push_str(&format!(
                "graph_cache_resident_bytes{{scope=\"{}\",cell_id=\"{}\",cache=\"{cache}\"}} {bytes}\n",
                scope, metrics.cell_id
            ));
        }
        // `scope` and `cell_id`, and nothing else. Never `edge_type`: an
        // 18-bucket family times 96 cell×type pairs is 1,728 series per
        // instrument per node, which is where the cardinality budget stops
        // being affordable. `scope` is here and absent from the OTel export by
        // design -- that divergence is why both exports exist.
        append_histograms(
            output,
            metrics.operational.histogram_fields(),
            &[("scope", &scope), ("cell_id", &metrics.cell_id)],
        );
    }
    append_per_cell_counters(output, shard_metrics);
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
/// indexing would work — but a linear match over thirty-five rows costs nothing
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

/// Declare a `# TYPE … histogram` line for every family a snapshot enumerates.
///
/// Split from the series rendering because a per-shard family declares its type
/// once and renders its series once per shard, and a repeated `# TYPE` line is
/// a scrape error rather than a cosmetic one.
fn append_histogram_types<'a>(
    output: &mut String,
    fields: impl Iterator<Item = (&'static str, &'a DurationHistogramSnapshot)>,
) {
    for (field, _) in fields {
        let Some(export) = prometheus_histogram(field) else {
            debug_assert!(
                false,
                "{field} is enumerated but absent from PROMETHEUS_HISTOGRAMS"
            );
            continue;
        };
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

fn admin_io_error(error: std::io::Error) -> slatedb_graph_kernel::GraphError {
    slatedb_graph_kernel::GraphError::CorruptValue {
        key: "runtime/admin".to_string(),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;
    use slatedb_graph_kernel::{
        ClientQueryServiceConfig, GraphCacheMetricsSnapshot, GraphId, GraphMemoryConfig,
        GraphOpenOptions, GraphOperationalMetricsSnapshot, GraphScope, GraphShardRuntimeMetrics,
        NamespaceId, NamespacePath, ObjectStoreNodeDirectory, PlacementConfig, PlacementView,
        QueryCellClient, QueryTransportMetricsSnapshot, ScopedGraphShardRuntimeMetrics,
    };

    use super::*;

    /// Two shards with contrived-but-distinguishable values, so every series in
    /// `append_node_metrics` renders something a diff can see move.
    fn shard_metrics() -> Vec<ScopedGraphShardRuntimeMetrics> {
        let mut first = GraphOperationalMetricsSnapshot {
            query_graphblas_artifact_snapshots: 11,
            query_graphblas_rebuilt_snapshots: 22,
            query_rust_sparse_fallbacks: 33,
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

    /// The histogram family is **additive**. Every series this endpoint served
    /// before it existed keeps its exact name, its exact labels and its exact
    /// value -- a scraper's recording rules and every dashboard built on them
    /// are downstream of these strings, and renaming one is a silent outage in
    /// a dashboard rather than a loud one here.
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
        assert!(document
            .contains("graph_client_operation_read_duration_seconds_bucket{le=\"0.0001\"} 0\n"));
        assert!(document
            .contains("graph_client_operation_write_duration_seconds_bucket{le=\"30\"} 0\n"));
        assert!(document.contains("graph_client_operation_write_duration_seconds_count 0\n"));
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
}
