#[allow(dead_code)]
#[path = "graph_node/admin.rs"]
mod admin;
#[allow(dead_code)]
#[path = "graph_node/config.rs"]
mod config;
#[allow(dead_code)]
#[path = "graph_node/tls.rs"]
mod tls;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use config::RuntimeConfig;
use slatedb::object_store::{path::Path, ObjectStore};
use slatedb_graph_kernel::{
    object_store_from_env, BoltServerConfig, ClientBoltServer, ClientHttpServer,
    ClientQueryService, ClientQueryServiceConfig, ClientQueryTarget,
    HierarchicalClientDatabaseResolver, HttpQueryServerConfig, ObjectStoreBoltRoutingTableProvider,
    ObjectStoreNodeDirectory, QueryTransportAction, QueryTransportScopeGrant,
    ScopedRoutedGraphCluster, StaticQueryTransportScopeAuthorizer,
};
use tracing_subscriber::EnvFilter;
use turbolay_placement::heartbeat::{delete_heartbeat, put_heartbeat, validate_node_id, Heartbeat};

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> RuntimeResult<()> {
    init_tracing();
    let config = RuntimeConfig::from_env()?;
    tracing::info!(scope = %config.scope, "starting graph node");
    run_node(config).await
}

async fn run_node(config: RuntimeConfig) -> RuntimeResult<()> {
    let started_at = Utc::now();
    // Rejected here rather than at the first PUT: an id the object layer cannot
    // name is a node that runs, serves, and never appears in anyone's live set —
    // a failure with no symptom other than a warning every heartbeat interval.
    validate_node_id(&config.node_id)?;
    std::fs::create_dir_all(&config.data_cache_dir)?;
    let object_store = object_store_from_env(None)?;
    let open_options = config.graph_open_options();
    let memory_config = config.graph_memory_config();
    let directory = ObjectStoreNodeDirectory::new(
        config.cells.iter().cloned(),
        config.bolt_node_addresses.keys().cloned(),
    )?;
    let node = Arc::new(ScopedRoutedGraphCluster::new(
        config.data_path.clone(),
        config.scope.namespace.clone(),
        config.scope.graph_id.clone(),
        config.node_id.clone(),
        directory.clone(),
        Arc::clone(&object_store),
        open_options,
        memory_config,
        config.max_open_scopes,
    )?);
    let (index_discovery_stop, index_discovery_task) = start_index_discovery(
        Arc::clone(&node),
        config.cells.clone(),
        config.index_discovery_interval,
    );

    let token = config.read_auth_token()?;
    let authorizer = StaticQueryTransportScopeAuthorizer::new().with_bearer_grant(
        token.clone(),
        QueryTransportScopeGrant::graph_namespace(
            config.scope.namespace.clone(),
            config.scope.graph_id.clone(),
            true,
            [
                QueryTransportAction::Read,
                QueryTransportAction::Write,
                QueryTransportAction::Cancel,
                QueryTransportAction::Admin,
            ],
        ),
    )?;
    let service = ClientQueryService::new(
        Arc::clone(&node) as Arc<dyn slatedb_graph_kernel::QueryCellClient>,
        ClientQueryServiceConfig::default()
            .with_required_bearer_token(token)
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_concurrent_queries(config.max_concurrent_queries)
            .with_max_query_runtime_ms(config.max_query_runtime_ms)
            .with_server_cursor_limits(
                config.max_server_cursors,
                config.max_cursor_buffer_bytes,
                config.cursor_ttl.as_millis().try_into()?,
            ),
    )?;
    let target = ClientQueryTarget::new(config.scope.clone(), config.cell_id.clone())?;
    let resolver = Arc::new(HierarchicalClientDatabaseResolver::new(
        config.database.clone(),
        target,
    )?);
    let tls_reloader = if config.allow_plaintext {
        None
    } else {
        Some(tls::FileTlsReloader::start(
            config
                .tls_certificate
                .as_ref()
                .expect("validated TLS certificate"),
            config
                .tls_private_key
                .as_ref()
                .expect("validated TLS private key"),
            Duration::from_secs(1),
        )?)
    };
    let tls_provider = tls_reloader.as_ref().map(tls::FileTlsReloader::provider);

    let mut bolt_config = BoltServerConfig::new(resolver)
        .with_default_database(config.database.clone())
        .with_max_connections(config.max_bolt_connections)
        .with_graceful_shutdown_timeout(config.graceful_shutdown_timeout);
    let routing = ObjectStoreBoltRoutingTableProvider::new(config.bolt_node_addresses.clone(), 30)?
        .with_readiness_port(config.admin_addr.port())?;
    bolt_config = bolt_config.with_routing_table_provider(Arc::new(routing));
    if let Some(provider) = &tls_provider {
        bolt_config = bolt_config.with_tls_provider(Arc::clone(provider));
    } else {
        bolt_config = bolt_config.insecure_allow_plaintext();
    }
    let bolt = ClientBoltServer::bind(config.bolt_addr, service.clone(), bolt_config).await?;

    let mut http_config = HttpQueryServerConfig::default()
        .with_graceful_shutdown_timeout(config.graceful_shutdown_timeout)
        .with_default_page_size(config.default_page_size);
    if config.scope.is_default() {
        http_config = http_config.allow_default_namespace();
    }
    if let Some(provider) = tls_provider {
        http_config = http_config.with_tls_provider(provider);
    } else {
        http_config = http_config.insecure_allow_plaintext();
    }
    let http = ClientHttpServer::bind(config.http_addr, service.clone(), http_config).await?;

    let ready = Arc::new(AtomicBool::new(false));
    let admin = admin::AdminServer::bind_scoped(
        config.admin_addr,
        Arc::clone(&ready),
        service.clone(),
        Arc::clone(&node),
    )
    .await?;
    ready.store(true, Ordering::Release);
    let (heartbeat_stop, heartbeat_task) = start_heartbeat_publisher(
        Arc::clone(&object_store),
        Path::from(config.data_path.as_str()),
        config.node_id.clone(),
        config.cells.clone(),
        Arc::clone(&ready),
        started_at,
        config.heartbeat_interval,
    );
    tracing::info!(
        node_id = %config.node_id,
        bolt_addr = %bolt.local_addr(),
        http_addr = %http.local_addr(),
        admin_addr = %admin.local_addr(),
        "graph node listeners started"
    );

    tokio::select! {
        result = shutdown_signal() => result?,
    }
    ready.store(false, Ordering::Release);
    // Before anything drains. The publisher's last act is to DELETE this node's
    // heartbeat, and awaiting it here is what makes a graceful restart hand the
    // node's cells over immediately instead of costing peers a full
    // `heartbeat_timeout` to notice — decision 4.
    let _ = heartbeat_stop.send(true);
    // Bounded, because a hung object-store request must not hold the drain open.
    // Giving up on the withdrawal costs peers one `heartbeat_timeout` to notice —
    // exactly the hard-crash bound decision 4 already accepts — whereas waiting
    // forever would cost the whole shutdown.
    match tokio::time::timeout(config.heartbeat_interval, heartbeat_task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(error = %error, "heartbeat publisher task failed"),
        Err(_) => tracing::warn!(
            node_id = %config.node_id,
            "timed out withdrawing the heartbeat; peers will age this node out"
        ),
    }
    admin.stop().await?;
    http.stop().await?;
    bolt.stop().await?;
    if let Some(tls_reloader) = tls_reloader {
        tls_reloader.stop().await;
    }
    let _ = index_discovery_stop.send(true);
    index_discovery_task.await??;
    drop(service);
    let node =
        Arc::try_unwrap(node).map_err(|_| "graph node still has active runtime references")?;
    node.close().await?;
    tracing::info!(node_id = %config.node_id, "graph node stopped");
    Ok(())
}

/// Publish this node's heartbeat while it is ready, withdraw it when it is not.
///
/// ```text
/// every heartbeat_interval:   ready?  -> PUT    <base>/_graph_nodes/v1/<id>
///                             not?    -> DELETE <base>/_graph_nodes/v1/<id>
/// SIGTERM:                            -> DELETE <base>/_graph_nodes/v1/<id>, then drain
/// ```
///
/// Decision 4 of `docs/plans/2026-07-25-rendezvous-placement.md`. **Presence of
/// the object is the readiness signal**, not merely a liveness one: it replaces
/// the per-caller `/readyz` fan-out that routing used to run, so a node that is
/// up but unready must leave *no* object behind. That is why the unready branch
/// DELETEs instead of skipping the PUT — a stale object would keep advertising a
/// node that is not serving until it aged out. This is the one place the loop
/// diverges from sleet's publisher (`../sleet/src/daemon.rs:213-310`), which
/// publishes unconditionally.
///
/// Readiness is the same `AtomicBool` the admin server's `/readyz` reads, so the
/// two answers cannot drift apart.
///
/// **Every store failure is a `warn!` and nothing more.** A node whose PUTs fail
/// ages out of every peer's live set, which is the correct outcome and needs no
/// help; a node that exited because it could not write a heartbeat would turn a
/// transient object-store blip into an outage.
fn start_heartbeat_publisher(
    object_store: Arc<dyn ObjectStore>,
    base_path: Path,
    node_id: String,
    cells: Vec<String>,
    ready: Arc<AtomicBool>,
    started_at: DateTime<Utc>,
    interval: Duration,
) -> (
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            publish_heartbeat(
                object_store.as_ref(),
                &base_path,
                &node_id,
                &cells,
                started_at,
                ready.load(Ordering::Acquire),
            )
            .await;
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(interval) => {}
            }
        }
        // The withdrawal that makes a graceful restart instant. It runs on the
        // way out even if the loop above just published, because `ready` is
        // cleared and the stop signal sent in the same breath and the ordering
        // between them is not worth relying on; `delete_heartbeat` is idempotent.
        withdraw_heartbeat(object_store.as_ref(), &base_path, &node_id).await;
    });
    (stop_tx, task)
}

async fn publish_heartbeat(
    store: &dyn ObjectStore,
    base_path: &Path,
    node_id: &str,
    cells: &[String],
    started_at: DateTime<Utc>,
    ready: bool,
) {
    if !ready {
        withdraw_heartbeat(store, base_path, node_id).await;
        return;
    }
    // The publisher is the one place that legitimately reads a clock — decision
    // 10 keeps every function below it pure, so the two timestamps are passed
    // inward rather than read there. Both are this node's own clock and are
    // observability only: liveness is the object's `LastModified`.
    let body = Heartbeat::new(
        node_id,
        env!("CARGO_PKG_VERSION"),
        started_at,
        Utc::now(),
        cells.to_vec(),
    );
    if let Err(error) = put_heartbeat(store, base_path, &body).await {
        tracing::warn!(node_id, error = %error, "failed to publish heartbeat");
    }
}

async fn withdraw_heartbeat(store: &dyn ObjectStore, base_path: &Path, node_id: &str) {
    if let Err(error) = delete_heartbeat(store, base_path, node_id).await {
        tracing::warn!(node_id, error = %error, "failed to withdraw heartbeat");
    }
}

fn start_index_discovery(
    node: Arc<ScopedRoutedGraphCluster>,
    cells: Vec<String>,
    interval: Duration,
) -> (
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<RuntimeResult<()>>,
) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            for cluster in node.loaded_clusters().await {
                for cell_id in &cells {
                    let shard = cluster.shard(cell_id)?;
                    match shard.dirty_graph_index_edge_types(cell_id).await {
                        Ok(edge_types) => {
                            for (edge_type, _) in edge_types {
                                if let Err(error) =
                                    shard.discover_graph_index(cell_id, &edge_type).await
                                {
                                    tracing::warn!(
                                        scope = %cluster.scope(),
                                        cell_id,
                                        edge_type,
                                        error = %error,
                                        "failed to discover graph index generation"
                                    );
                                }
                            }
                        }
                        Err(error) => tracing::warn!(
                            scope = %cluster.scope(),
                            cell_id,
                            error = %error,
                            "failed to discover dirty graph index edge types"
                        ),
                    }
                }
            }
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(interval.max(Duration::from_millis(100))) => {}
            }
        }
    });
    (stop_tx, task)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .init();
}

async fn shutdown_signal() -> RuntimeResult<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::{memory::InMemory, ObjectStoreExt};

    use super::*;

    /// Long enough to cover many publisher ticks, short enough that a genuine
    /// hang fails the test instead of the suite.
    const SETTLE: Duration = Duration::from_secs(5);
    const TICK: Duration = Duration::from_millis(5);

    async fn settles_to(store: &Arc<dyn ObjectStore>, path: &Path, present: bool) -> bool {
        let deadline = std::time::Instant::now() + SETTLE;
        while std::time::Instant::now() < deadline {
            if store.head(path).await.is_ok() == present {
                return true;
            }
            tokio::time::sleep(TICK).await;
        }
        false
    }

    /// Decision 4: the object's presence *is* the readiness signal, so going
    /// unready has to remove it. A publisher that merely skipped the PUT would
    /// keep advertising an unready node for a whole `heartbeat_timeout`, and
    /// nothing downstream could tell the difference.
    #[tokio::test]
    async fn heartbeat_tracks_readiness_in_both_directions() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let base = Path::from("graph/data");
        let path = Path::from("graph/data/_graph_nodes/v1/graph-node-0");
        let ready = Arc::new(AtomicBool::new(true));
        let (stop, task) = start_heartbeat_publisher(
            Arc::clone(&store),
            base,
            "graph-node-0".to_string(),
            vec!["cell-0".to_string()],
            Arc::clone(&ready),
            Utc::now(),
            TICK,
        );

        assert!(settles_to(&store, &path, true).await, "never published");
        ready.store(false, Ordering::Release);
        assert!(
            settles_to(&store, &path, false).await,
            "stayed published while unready"
        );
        ready.store(true, Ordering::Release);
        assert!(
            settles_to(&store, &path, true).await,
            "did not republish on becoming ready again"
        );

        let _ = stop.send(true);
        task.await.unwrap();
    }

    /// The SIGTERM withdrawal, and the reason `run_node` awaits the task before
    /// draining: peers take the node's cells over the moment this DELETE lands
    /// rather than `heartbeat_timeout` later.
    #[tokio::test]
    async fn shutdown_withdraws_the_heartbeat_of_a_ready_node() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let base = Path::from("graph/data");
        let path = Path::from("graph/data/_graph_nodes/v1/graph-node-0");
        let ready = Arc::new(AtomicBool::new(true));
        // A publisher interval far longer than the test: the withdrawal must
        // come from the shutdown path, not from a tick that happened to land.
        let (stop, task) = start_heartbeat_publisher(
            Arc::clone(&store),
            base,
            "graph-node-0".to_string(),
            vec!["cell-0".to_string()],
            ready,
            Utc::now(),
            Duration::from_secs(600),
        );

        assert!(settles_to(&store, &path, true).await, "never published");
        let _ = stop.send(true);
        task.await.unwrap();
        assert!(
            store.head(&path).await.is_err(),
            "heartbeat outlived the process"
        );
    }
}
