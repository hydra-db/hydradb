use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures::StreamExt;
use slatedb::object_store::path::Path;
use slatedb_graph_kernel::{
    object_store_from_env, GraphCluster, GraphId, GraphScope, NamespaceId, NamespacePath,
    ObjectStoreGraphScopeDirectory,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct IndexerMetrics {
    ready: AtomicBool,
    cycles: AtomicU64,
    successful_cycles: AtomicU64,
    failed_cycles: AtomicU64,
    open_failures: AtomicU64,
    generations_published: AtomicU64,
    generation_failures: AtomicU64,
    generations_deleted: AtomicU64,
    last_success_ms: AtomicU64,
}

struct IndexerAdminServer {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<std::io::Result<()>>,
}

#[tokio::main]
async fn main() -> RuntimeResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let data_path = env_value("GRAPH_DATA_PATH", "graph/data");
    let root_scope = graph_scope()?;
    let cells = env_value("GRAPH_CELLS", "cell-0")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if cells.is_empty() {
        return Err("GRAPH_CELLS must contain at least one cell".into());
    }
    let interval =
        Duration::from_millis(env_value("GRAPH_INDEXER_INTERVAL_MS", "5000").parse::<u64>()?);
    if interval.is_zero() {
        return Err("GRAPH_INDEXER_INTERVAL_MS must be greater than zero".into());
    }
    let retain_previous = env_value("GRAPH_INDEXER_RETAIN_PREVIOUS", "1").parse::<usize>()?;
    let admin_addr = env_value("GRAPH_INDEXER_ADMIN_ADDR", "0.0.0.0:9091").parse::<SocketAddr>()?;

    let metrics = Arc::new(IndexerMetrics::default());
    let admin = IndexerAdminServer::bind(admin_addr, Arc::clone(&metrics)).await?;
    // An empty graph has no SlateDB manifest yet. The indexer is healthy and
    // ready to observe that namespace even though there is nothing to build.
    metrics.ready.store(true, Ordering::Release);
    let object_store = object_store_from_env(None)?;
    let scope_directory = ObjectStoreGraphScopeDirectory::new(
        data_path.clone(),
        root_scope.namespace.clone(),
        root_scope.graph_id.clone(),
        Arc::clone(&object_store),
    );
    let mut shutdown = Box::pin(shutdown_signal());
    tracing::info!(scope = %root_scope, ?cells, "graph indexer started");

    loop {
        metrics.cycles.fetch_add(1, Ordering::Relaxed);
        match run_registered_scopes_cycle(
            &data_path,
            &scope_directory,
            &cells,
            Arc::clone(&object_store),
            retain_previous,
            &metrics,
        )
        .await
        {
            Ok(()) => {
                metrics.successful_cycles.fetch_add(1, Ordering::Relaxed);
                metrics
                    .last_success_ms
                    .store(unix_time_ms(), Ordering::Relaxed);
                metrics.ready.store(true, Ordering::Release);
            }
            Err(error) => {
                metrics.failed_cycles.fetch_add(1, Ordering::Relaxed);
                metrics.ready.store(false, Ordering::Release);
                tracing::warn!(error = %error, "graph index cycle failed; retrying");
            }
        }

        tokio::select! {
            result = &mut shutdown => {
                result?;
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
    metrics.ready.store(false, Ordering::Release);
    admin.stop().await?;
    tracing::info!(scope = %root_scope, "graph indexer stopped");
    Ok(())
}

async fn run_registered_scopes_cycle(
    data_path: &str,
    scope_directory: &ObjectStoreGraphScopeDirectory,
    cells: &[String],
    object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    retain_previous: usize,
    metrics: &IndexerMetrics,
) -> RuntimeResult<()> {
    let scopes = scope_directory.list().await?;
    let mut failures = Vec::new();
    for scope in scopes {
        if !scope_has_data(data_path, &scope, cells, &object_store).await? {
            continue;
        }
        let cluster = match GraphCluster::open_cells_scoped(
            data_path.to_string(),
            scope.clone(),
            cells.to_vec(),
            Arc::clone(&object_store),
        )
        .await
        {
            Ok(cluster) => cluster,
            Err(error) => {
                metrics.open_failures.fetch_add(1, Ordering::Relaxed);
                failures.push(format!("open scope {scope}: {error}"));
                continue;
            }
        };
        if let Err(error) = run_index_cycle(&cluster, cells, retain_previous, metrics).await {
            failures.push(format!("index scope {scope}: {error}"));
        }
        if let Err(error) = cluster.close().await {
            failures.push(format!("close scope {scope}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(std::io::Error::other(failures.join("; ")).into())
    }
}

async fn scope_has_data(
    data_path: &str,
    scope: &GraphScope,
    cells: &[String],
    object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
) -> RuntimeResult<bool> {
    let scope_path = scope.scoped_store_path(data_path);
    for cell_id in cells {
        let prefix = Path::from(format!("{scope_path}/{cell_id}"));
        if object_store
            .list(Some(&prefix))
            .next()
            .await
            .transpose()?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn run_index_cycle(
    cluster: &GraphCluster,
    cells: &[String],
    retain_previous: usize,
    metrics: &IndexerMetrics,
) -> RuntimeResult<()> {
    let mut failures = Vec::new();
    for cell_id in cells {
        let Some(shard) = cluster.shard(cell_id) else {
            failures.push(format!("missing configured cell {cell_id}"));
            continue;
        };
        if let Err(error) = shard.refresh_storage_sequence(cell_id).await {
            failures.push(format!("refresh {cell_id}: {error}"));
            continue;
        }
        let dirty = match shard.dirty_graph_index_edge_types(cell_id).await {
            Ok(dirty) => dirty,
            Err(error) => {
                failures.push(format!("discover dirty edge types for {cell_id}: {error}"));
                continue;
            }
        };
        for (edge_type, dirty_sequence) in dirty {
            let current = match shard.current_graph_index(cell_id, &edge_type).await {
                Ok(current) => current,
                Err(error) => {
                    failures.push(format!("read index {cell_id}/{edge_type}: {error}"));
                    continue;
                }
            };
            if current
                .as_ref()
                .is_some_and(|generation| generation.base_sequence >= dirty_sequence)
            {
                continue;
            }
            match shard.build_graph_index(cell_id, &edge_type).await {
                Ok(generation) => {
                    metrics
                        .generations_published
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::info!(
                        cell_id,
                        edge_type,
                        base_sequence = generation.base_sequence,
                        edge_count = generation.edge_count,
                        generation = generation.generation,
                        "graph index generation published"
                    );
                    match shard
                        .gc_graph_index_generations(cell_id, &edge_type, retain_previous)
                        .await
                    {
                        Ok(deleted) => {
                            metrics
                                .generations_deleted
                                .fetch_add(deleted, Ordering::Relaxed);
                        }
                        Err(error) => tracing::warn!(
                            cell_id,
                            edge_type,
                            error = %error,
                            "graph index generation cleanup failed"
                        ),
                    }
                }
                Err(error) => {
                    metrics.generation_failures.fetch_add(1, Ordering::Relaxed);
                    failures.push(format!("build index {cell_id}/{edge_type}: {error}"));
                }
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(std::io::Error::other(failures.join("; ")).into())
    }
}

impl IndexerAdminServer {
    async fn bind(addr: SocketAddr, metrics: Arc<IndexerMetrics>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let router = Router::new()
            .route("/livez", get(|| async { StatusCode::OK }))
            .route("/readyz", get(indexer_readiness))
            .route("/metrics", get(indexer_metrics))
            .with_state(metrics);
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
        });
        Ok(Self { stop_tx, task })
    }

    async fn stop(self) -> RuntimeResult<()> {
        let _ = self.stop_tx.send(true);
        self.task.await??;
        Ok(())
    }
}

async fn indexer_readiness(State(metrics): State<Arc<IndexerMetrics>>) -> StatusCode {
    if metrics.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn indexer_metrics(State(metrics): State<Arc<IndexerMetrics>>) -> Response {
    let output = format!(
        concat!(
            "# TYPE graph_indexer_ready gauge\n",
            "graph_indexer_ready {}\n",
            "# TYPE graph_indexer_cycles counter\n",
            "graph_indexer_cycles {}\n",
            "# TYPE graph_indexer_successful_cycles counter\n",
            "graph_indexer_successful_cycles {}\n",
            "# TYPE graph_indexer_failed_cycles counter\n",
            "graph_indexer_failed_cycles {}\n",
            "# TYPE graph_indexer_open_failures counter\n",
            "graph_indexer_open_failures {}\n",
            "# TYPE graph_indexer_generations_published counter\n",
            "graph_indexer_generations_published {}\n",
            "# TYPE graph_indexer_generation_failures counter\n",
            "graph_indexer_generation_failures {}\n",
            "# TYPE graph_indexer_generations_deleted counter\n",
            "graph_indexer_generations_deleted {}\n",
            "# TYPE graph_indexer_last_success_ms gauge\n",
            "graph_indexer_last_success_ms {}\n",
        ),
        u8::from(metrics.ready.load(Ordering::Acquire)),
        metrics.cycles.load(Ordering::Relaxed),
        metrics.successful_cycles.load(Ordering::Relaxed),
        metrics.failed_cycles.load(Ordering::Relaxed),
        metrics.open_failures.load(Ordering::Relaxed),
        metrics.generations_published.load(Ordering::Relaxed),
        metrics.generation_failures.load(Ordering::Relaxed),
        metrics.generations_deleted.load(Ordering::Relaxed),
        metrics.last_success_ms.load(Ordering::Relaxed),
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

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn graph_scope() -> RuntimeResult<GraphScope> {
    let namespace = NamespacePath::new(
        env_value("GRAPH_NAMESPACE", "default")
            .split('/')
            .map(|segment| NamespaceId::new(segment.to_string()))
            .collect::<slatedb_graph_kernel::Result<Vec<_>>>()?,
    )?;
    Ok(GraphScope::new(
        namespace,
        GraphId::new(env_value("GRAPH_ID", "default"))?,
    ))
}

fn env_value(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn shutdown_signal() -> RuntimeResult<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;
    use slatedb_graph_kernel::EdgeMutation;

    use super::*;

    #[tokio::test]
    async fn indexer_discovers_registered_scopes_and_ignores_empty_ones() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let scope = GraphScope::new(
            root.child(NamespaceId::new("dGVuYW50LWE").unwrap())
                .unwrap()
                .child(NamespaceId::new("Y29sbGVjdGlvbi1h").unwrap())
                .unwrap(),
            graph_id.clone(),
        );
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            root,
            graph_id,
            Arc::clone(&object_store),
        );
        directory.register(&scope).await.unwrap();
        let metrics = IndexerMetrics::default();

        run_registered_scopes_cycle(
            "graph/data",
            &directory,
            &["cell-0".to_string()],
            Arc::clone(&object_store),
            1,
            &metrics,
        )
        .await
        .unwrap();
        assert_eq!(metrics.open_failures.load(Ordering::Relaxed), 0);

        let writer = GraphCluster::open_cells_standalone_writers_scoped(
            "graph/data",
            scope.clone(),
            ["cell-0"],
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        writer
            .shard("cell-0")
            .unwrap()
            .write_edge(EdgeMutation {
                cell_id: "cell-0".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "indexer-scope-write".to_string(),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        run_registered_scopes_cycle(
            "graph/data",
            &directory,
            &["cell-0".to_string()],
            Arc::clone(&object_store),
            1,
            &metrics,
        )
        .await
        .unwrap();
        assert_eq!(metrics.generations_published.load(Ordering::Relaxed), 1);

        let reader = GraphCluster::open_cells_scoped("graph/data", scope, ["cell-0"], object_store)
            .await
            .unwrap();
        assert!(reader
            .shard("cell-0")
            .unwrap()
            .current_graph_index("cell-0", "FOLLOWS")
            .await
            .unwrap()
            .is_some());
        reader.close().await.unwrap();
    }
}
