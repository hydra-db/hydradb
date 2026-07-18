use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use slatedb_graph_kernel::{ClientQueryService, Result, RoutedGraphCluster};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone)]
struct AdminState {
    ready: Arc<AtomicBool>,
    query: ClientQueryService,
    routed_node: Arc<RoutedGraphCluster>,
}

pub struct AdminServer {
    local_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl AdminServer {
    pub async fn bind_routed(
        addr: SocketAddr,
        ready: Arc<AtomicBool>,
        query: ClientQueryService,
        node: Arc<RoutedGraphCluster>,
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

async fn readiness(State(state): State<AdminState>) -> StatusCode {
    if state.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics(State(state): State<AdminState>) -> Response {
    let mut output = format!(
        "# TYPE graph_runtime_ready gauge\ngraph_runtime_ready {}\n",
        u8::from(state.ready.load(Ordering::Acquire)),
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
    shard_metrics: Vec<slatedb_graph_kernel::GraphShardRuntimeMetrics>,
) {
    output.push_str(concat!(
        "# TYPE graph_query_graphblas_artifact_snapshots counter\n",
        "# TYPE graph_query_graphblas_rebuilt_snapshots counter\n",
        "# TYPE graph_query_rust_sparse_fallbacks counter\n",
        "# TYPE graph_cache_entries gauge\n",
        "# TYPE graph_cache_resident_bytes gauge\n"
    ));
    for metrics in shard_metrics {
        output.push_str(&format!(
            concat!(
                "graph_query_graphblas_artifact_snapshots{{cell_id=\"{}\"}} {}\n",
                "graph_query_graphblas_rebuilt_snapshots{{cell_id=\"{}\"}} {}\n",
                "graph_query_rust_sparse_fallbacks{{cell_id=\"{}\"}} {}\n"
            ),
            metrics.cell_id,
            metrics.operational.query_graphblas_artifact_snapshots,
            metrics.cell_id,
            metrics.operational.query_graphblas_rebuilt_snapshots,
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
                "graph_cache_entries{{cell_id=\"{}\",cache=\"{cache}\"}} {entries}\n",
                metrics.cell_id
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
                "graph_cache_resident_bytes{{cell_id=\"{}\",cache=\"{cache}\"}} {bytes}\n",
                metrics.cell_id
            ));
        }
    }
}

fn admin_io_error(error: std::io::Error) -> slatedb_graph_kernel::GraphError {
    slatedb_graph_kernel::GraphError::CorruptValue {
        key: "runtime/admin".to_string(),
        reason: error.to_string(),
    }
}
