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

use config::ControllerRuntimeConfig;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    object_store_from_env, GraphCacheConfig, GraphClusterControllerConfig, GraphControlPlane,
    GraphControlRpcServer, GraphControlRpcServerConfig, GraphRuntimeLease,
};
use tracing_subscriber::EnvFilter;

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> RuntimeResult<()> {
    init_tracing();
    let config = ControllerRuntimeConfig::from_env()?;
    std::fs::create_dir_all(&config.control_cache_dir)?;
    let ready = Arc::new(AtomicBool::new(false));
    let admin =
        admin::AdminServer::bind_without_control(config.admin_addr, Arc::clone(&ready)).await?;
    let object_store = object_store_from_env(None)?;
    let role_publisher =
        kubernetes::KubernetesPodRolePublisher::from_env("graph.usecortex.io/control-active")?;
    role_publisher.publish(false).await?;
    ready.store(true, Ordering::Release);
    let mut runtime_lease = tokio::select! {
        lease = acquire_runtime_lease(&config, Arc::clone(&object_store)) => lease,
        result = shutdown_signal() => {
            result?;
            admin.stop().await?;
            return Ok(());
        }
    };
    let control = open_control_plane(&config, object_store).await?;
    let controller_config = GraphClusterControllerConfig::new(
        config.cells.clone(),
        config.heartbeat_ttl,
        config.lease_ttl,
    )?
    .with_rebalance_mode(config.rebalance_mode)
    .with_existing_cell_discovery(true);
    let controller = Arc::clone(&control)
        .start_cluster_controller(controller_config, config.controller_interval)?;

    let internal_tls = if config.internal_allow_plaintext {
        None
    } else {
        Some(tls::FileMutualTlsServerReloader::start(
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
    let rpc_config = if let Some(reloader) = &internal_tls {
        GraphControlRpcServerConfig::new(reloader.provider())
    } else {
        GraphControlRpcServerConfig::insecure_allow_plaintext()
    };
    let rpc =
        GraphControlRpcServer::bind(config.control_rpc_addr, Arc::clone(&control), rpc_config)
            .await?;
    role_publisher.publish(true).await?;
    tracing::info!(
        scope = %config.scope,
        rpc_addr = %rpc.local_addr(),
        admin_addr = %admin.local_addr(),
        "graph controller became active"
    );

    let stop_result = tokio::select! {
        result = shutdown_signal() => result,
        result = runtime_lease.wait_until_lost() => result.map_err(Into::into),
    };
    ready.store(false, Ordering::Release);
    role_publisher.publish(false).await?;
    admin.stop().await?;
    rpc.stop().await?;
    if let Some(internal_tls) = internal_tls {
        internal_tls.stop().await;
    }
    controller.stop().await?;
    control.close().await?;
    let lease_stop_result = runtime_lease.stop().await;
    stop_result?;
    lease_stop_result?;
    Ok(())
}

async fn acquire_runtime_lease(
    config: &ControllerRuntimeConfig,
    object_store: Arc<dyn ObjectStore>,
) -> GraphRuntimeLease {
    loop {
        match GraphRuntimeLease::acquire(
            &config.control_path,
            &config.scope,
            Arc::clone(&object_store),
            config.runtime_lease_ttl,
            config.runtime_lease_renew_interval,
        )
        .await
        {
            Ok(lease) => return lease,
            Err(error) => {
                tracing::info!(error = %error, "another controller is active; waiting for runtime lease");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn open_control_plane(
    config: &ControllerRuntimeConfig,
    object_store: Arc<dyn ObjectStore>,
) -> RuntimeResult<Arc<GraphControlPlane>> {
    let cache = GraphCacheConfig::disk_cache_without_preload(
        &config.control_cache_dir,
        config.control_cache_bytes,
    );
    let control = GraphControlPlane::open_scoped_with_cache(
        config.control_path.clone(),
        object_store,
        config.scope.clone(),
        cache,
    )
    .await?;
    Ok(Arc::new(control))
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
