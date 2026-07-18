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

use config::RuntimeConfig;
use slatedb_graph_kernel::{
    object_store_from_env, BoltServerConfig, ClientBoltServer, ClientHttpServer,
    ClientQueryService, ClientQueryServiceConfig, ClientQueryTarget, HttpQueryServerConfig,
    ObjectStoreBoltRoutingTableProvider, ObjectStoreNodeDirectory, QueryTransportAction,
    QueryTransportScopeGrant, RoutedGraphCluster, StaticClientDatabaseResolver,
    StaticQueryTransportScopeAuthorizer,
};
use tracing_subscriber::EnvFilter;

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> RuntimeResult<()> {
    init_tracing();
    let config = RuntimeConfig::from_env()?;
    tracing::info!(scope = %config.scope, "starting graph node");
    run_node(config).await
}

async fn run_node(config: RuntimeConfig) -> RuntimeResult<()> {
    std::fs::create_dir_all(&config.data_cache_dir)?;
    let object_store = object_store_from_env(None)?;
    let open_options = config.graph_open_options();
    let memory_config = config.graph_memory_config();
    let directory = ObjectStoreNodeDirectory::new(
        config.cells.iter().cloned(),
        config.bolt_node_addresses.keys().cloned(),
    )?;
    let node = Arc::new(
        RoutedGraphCluster::open_promotable_scoped_with_memory_options(
            config.data_path.clone(),
            config.scope.clone(),
            config.node_id.clone(),
            directory.clone(),
            object_store,
            open_options,
            memory_config,
        )
        .await?,
    );
    let (index_discovery_stop, index_discovery_task) = start_index_discovery(
        Arc::clone(&node),
        config.cells.clone(),
        config.index_discovery_interval,
    );

    let token = config.read_auth_token()?;
    let authorizer = StaticQueryTransportScopeAuthorizer::new().with_bearer_grant(
        token.clone(),
        QueryTransportScopeGrant::graph(
            config.scope.clone(),
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
    let resolver = Arc::new(StaticClientDatabaseResolver::single(
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
    let routing = ObjectStoreBoltRoutingTableProvider::new(config.bolt_node_addresses.clone(), 30)?;
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
    let admin = admin::AdminServer::bind_routed(
        config.admin_addr,
        Arc::clone(&ready),
        service.clone(),
        Arc::clone(&node),
    )
    .await?;
    ready.store(true, Ordering::Release);
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

fn start_index_discovery(
    node: Arc<RoutedGraphCluster>,
    cells: Vec<String>,
    interval: Duration,
) -> (
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<RuntimeResult<()>>,
) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            for cell_id in &cells {
                let shard = node.shard(cell_id)?;
                match shard.dirty_graph_index_edge_types(cell_id).await {
                    Ok(edge_types) => {
                        for (edge_type, _) in edge_types {
                            if let Err(error) =
                                shard.discover_graph_index(cell_id, &edge_type).await
                            {
                                tracing::warn!(
                                    cell_id,
                                    edge_type,
                                    error = %error,
                                    "failed to discover graph index generation"
                                );
                            }
                        }
                    }
                    Err(error) => tracing::warn!(
                        cell_id,
                        error = %error,
                        "failed to discover dirty graph index edge types"
                    ),
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
