use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use slatedb_graph_kernel::{
    ClientQueryService, DurationHistogramSnapshot, Result, ScopedRoutedGraphCluster,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::otel_metrics::ExportUnit;
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
}

/// The Prometheus name table. One row per histogram the kernel enumerates.
///
/// `graph_*` throughout, because that is what every existing series on this
/// endpoint is called and a scraper's relabelling rules key off the prefix. The
/// OTel vocabulary (`db.*`/`turbolay.*`) is a separate decision living in a
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
    },
    PrometheusHistogram {
        field: "write_latency",
        name: "graph_client_operation_write_duration_seconds",
        unit: ExportUnit::Seconds,
    },
    PrometheusHistogram {
        field: "query_rows_latency",
        name: "graph_query_rows_duration_microseconds",
        unit: ExportUnit::Microseconds,
    },
    PrometheusHistogram {
        field: "rpc_latency",
        name: "graph_query_transport_rpc_duration_microseconds",
        unit: ExportUnit::Microseconds,
    },
    PrometheusHistogram {
        field: "serve_latency",
        name: "graph_query_transport_serve_duration_microseconds",
        unit: ExportUnit::Microseconds,
    },
];

/// The Prometheus name and unit for a kernel field identifier.
pub fn prometheus_histogram(field: &str) -> Option<&'static PrometheusHistogram> {
    PROMETHEUS_HISTOGRAMS
        .iter()
        .find(|export| export.field == field)
}

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
    output.push_str(&format!(
        concat!(
            "# TYPE graph_query_started counter\n",
            "graph_query_started {}\n",
            "# TYPE graph_query_completed counter\n",
            "graph_query_completed {}\n",
            "# TYPE graph_query_failed counter\n",
            "graph_query_failed {}\n",
            "# TYPE graph_query_auth_failures counter\n",
            "graph_query_auth_failures {}\n",
            "# TYPE graph_query_scope_denials counter\n",
            "graph_query_scope_denials {}\n"
        ),
        query.queries_started,
        query.queries_completed,
        query.queries_failed,
        query.auth_failures,
        query.scope_denials,
    ));
    // Additive, and deliberately after the counters rather than interleaved
    // with them: every series above keeps the exact name, labels and value it
    // had before the histograms existed.
    append_histogram_types(&mut output, query.histogram_fields());
    append_histograms(&mut output, query.histogram_fields(), &[]);
    append_node_metrics(
        &mut output,
        state.routed_node.local_shard_runtime_metrics().await,
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

fn append_node_metrics(
    output: &mut String,
    shard_metrics: Vec<slatedb_graph_kernel::ScopedGraphShardRuntimeMetrics>,
) {
    output.push_str(concat!(
        "# TYPE graph_query_graphblas_artifact_snapshots counter\n",
        "# TYPE graph_query_graphblas_rebuilt_snapshots counter\n",
        "# TYPE graph_query_rust_sparse_fallbacks counter\n",
        "# TYPE graph_cache_entries gauge\n",
        "# TYPE graph_cache_resident_bytes gauge\n"
    ));
    // From the same enumeration the series below come from, so a family whose
    // shards are all absent still declares itself. A default snapshot is the
    // cheapest way to ask the kernel "which histograms do you have" without
    // restating the answer here.
    append_histogram_types(
        output,
        slatedb_graph_kernel::GraphOperationalMetricsSnapshot::default().histogram_fields(),
    );
    for metrics in shard_metrics {
        let scope = metrics.scope.to_string();
        let metrics = metrics.shard;
        output.push_str(&format!(
            concat!(
                "graph_query_graphblas_artifact_snapshots{{scope=\"{}\",cell_id=\"{}\"}} {}\n",
                "graph_query_graphblas_rebuilt_snapshots{{scope=\"{}\",cell_id=\"{}\"}} {}\n",
                "graph_query_rust_sparse_fallbacks{{scope=\"{}\",cell_id=\"{}\"}} {}\n"
            ),
            scope,
            metrics.cell_id,
            metrics.operational.query_graphblas_artifact_snapshots,
            scope,
            metrics.cell_id,
            metrics.operational.query_graphblas_rebuilt_snapshots,
            scope,
            metrics.cell_id,
            metrics.operational.query_rust_sparse_fallbacks,
        ));
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
        NamespacePath, ObjectStoreNodeDirectory, PlacementConfig, PlacementView, QueryCellClient,
        QueryTransportMetricsSnapshot, ScopedGraphShardRuntimeMetrics,
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
        append_node_metrics(&mut document, shard_metrics());
        document
    }

    /// Writes `/metrics` to `$TURBOLAY_METRICS_CAPTURE` when it is set, so the
    /// endpoint can be diffed across a change to it. Asserts nothing on its
    /// own: the assertion is the diff.
    #[tokio::test]
    async fn capture_metrics_document() {
        let document = rendered_metrics().await;
        if let Ok(path) = std::env::var("TURBOLAY_METRICS_CAPTURE") {
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
    /// through `/metrics` because this binary instantiates no
    /// `TcpQueryServer` and no `TcpQueryCellClient`, so it holds no
    /// `QueryTransportMetricsSnapshot` to feed them -- see the module note in
    /// `otel_metrics.rs`.
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
}
