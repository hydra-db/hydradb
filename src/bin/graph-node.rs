#[allow(dead_code)]
#[path = "graph_node/admin.rs"]
mod admin;
#[allow(dead_code)]
#[path = "graph_node/config.rs"]
mod config;
#[path = "graph_node/kubernetes.rs"]
mod kubernetes;
#[allow(dead_code)]
#[path = "graph_node/tls.rs"]
mod tls;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use config::RuntimeConfig;
use slatedb_graph_kernel::{
    object_store_from_env, BoltServerConfig, ClientBoltServer, ClientHttpServer,
    ClientQueryService, ClientQueryServiceConfig, ClientQueryTarget,
    ControllerBoltRoutingTableProvider, GraphControlClient, GraphControlRpcClient,
    GraphControlRpcClientConfig, GraphError, GraphNode, GraphNodeHealthState,
    GraphNodeRuntimeConfig, HttpQueryServerConfig, QueryTransportAction, QueryTransportScopeGrant,
    StaticClientDatabaseResolver, StaticQueryTransportScopeAuthorizer,
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
    let role_publisher =
        kubernetes::KubernetesPodRolePublisher::from_env("graph.usecortex.io/serving")?;
    role_publisher.publish(false).await?;
    std::fs::create_dir_all(&config.data_cache_dir)?;
    let object_store = object_store_from_env(None)?;
    let internal_tls = if config.internal_allow_plaintext {
        None
    } else {
        Some(tls::FileMutualTlsClientReloader::start(
            config
                .internal_tls_certificate
                .as_deref()
                .expect("validated internal certificate"),
            config
                .internal_tls_private_key
                .as_deref()
                .expect("validated internal private key"),
            config
                .internal_tls_ca
                .as_deref()
                .expect("validated internal CA"),
            Duration::from_secs(1),
        )?)
    };
    let control_config = if let Some(reloader) = &internal_tls {
        GraphControlRpcClientConfig::new(
            config.control_rpc_server_name.clone(),
            reloader.provider(),
        )
    } else {
        GraphControlRpcClientConfig::insecure_allow_plaintext()
    };
    let control = Arc::new(GraphControlRpcClient::new(
        config.control_rpc_endpoint,
        config.scope.clone(),
        control_config,
    )?);
    let open_options = config.graph_open_options();
    let memory_config = config.graph_memory_config();
    wait_for_initial_placement(&config, control.as_ref()).await?;
    let node = Arc::new(
        GraphNode::open_managed_with_memory_config(
            config.data_path.clone(),
            config.node_id.clone(),
            Arc::clone(&control),
            object_store,
            GraphNodeRuntimeConfig::new(
                config.lease_ttl,
                config.lease_renew_interval,
                config.shard_refresh_interval,
            )
            .with_options(open_options),
            memory_config,
        )
        .await?,
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
            .with_max_query_runtime_ms(config.max_query_runtime_ms),
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
    let routing = ControllerBoltRoutingTableProvider::new(
        Arc::clone(&control),
        config.bolt_node_addresses.clone(),
        config.heartbeat_ttl,
        30,
    )?;
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
    let (serving_stop, serving_task) = start_serving_monitor(
        Arc::clone(&node),
        config.cell_id.clone(),
        role_publisher.clone(),
    );
    let admin = admin::AdminServer::bind(
        config.admin_addr,
        Arc::clone(&ready),
        Arc::clone(&control),
        Some(service.clone()),
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
    let _ = serving_stop.send(true);
    serving_task.await??;
    role_publisher.publish(false).await?;
    node.set_health_state(GraphNodeHealthState::Draining)
        .await?;
    admin.stop().await?;
    http.stop().await?;
    bolt.stop().await?;
    if let Some(tls_reloader) = tls_reloader {
        tls_reloader.stop().await;
    }
    drop(service);
    let node =
        Arc::try_unwrap(node).map_err(|_| "graph node still has active runtime references")?;
    node.close().await?;
    if let Some(internal_tls) = internal_tls {
        internal_tls.stop().await;
    }
    tracing::info!(node_id = %config.node_id, "graph node stopped");
    Ok(())
}

fn start_serving_monitor(
    node: Arc<slatedb_graph_kernel::ManagedGraphNode>,
    cell_id: String,
    role_publisher: kubernetes::KubernetesPodRolePublisher,
) -> (
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<RuntimeResult<()>>,
) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        let mut published = false;
        loop {
            let owns_cell = node
                .local_cells()
                .await
                .map(|cells| cells.iter().any(|candidate| candidate == &cell_id))
                .unwrap_or(false);
            if owns_cell != published {
                match role_publisher.publish(owns_cell).await {
                    Ok(()) => published = owns_cell,
                    Err(error) => {
                        tracing::warn!(error = %error, owns_cell, "failed to publish graph-node serving role");
                    }
                }
            }
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    });
    (stop_tx, task)
}

async fn wait_for_initial_placement(
    config: &RuntimeConfig,
    control: &dyn GraphControlClient,
) -> RuntimeResult<()> {
    const MAX_ATTEMPTS: usize = 120;
    for attempt in 1..=MAX_ATTEMPTS {
        control
            .publish_node_heartbeat(&config.node_id, GraphNodeHealthState::Active)
            .await?;
        match control.load_placement().await {
            Ok(placement) if placement.cells().next().is_some() => return Ok(()),
            Ok(_) => {}
            Err(GraphError::CorruptValue { key, .. }) if key == "placement" => {}
            Err(error) => return Err(error.into()),
        }
        tracing::info!(attempt, node_id = %config.node_id, "waiting for initial shard placement");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "controller did not publish initial placement for node {} within 60 seconds",
        config.node_id
    )
    .into())
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
