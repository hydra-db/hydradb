use std::sync::Arc;
use std::time::Duration;

use slatedb_graph_kernel::{
    object_store_from_env, ArtifactDirection, BoltServerConfig, ClientBoltServer,
    ClientQueryService, ClientQueryServiceConfig, ClientQueryTarget, GraphBackpressurePolicy,
    GraphCacheConfig, GraphCachePolicy, GraphControlPlane, GraphIndexPolicy, GraphLimits,
    GraphOpenOptions, GraphRetentionPolicy, GraphScope, QueryTransportAction,
    QueryTransportScopeGrant, RoutedGraphCluster, ShardPlacement, StaticClientDatabaseResolver,
    StaticQueryTransportScopeAuthorizer,
};

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "bolt-bench";
const TOKEN: &str = "s3-bolt-benchmark-secret-32-chars";
const HOPS: &[u8] = &[1, 3, 5, 10];

#[tokio::main]
async fn main() -> BenchResult<()> {
    let fanout = env_u64("GRAPH_BENCH_FANOUT", 100)?;
    let max_hop = *HOPS.iter().max().expect("benchmark hops are nonempty");
    let prefix = required_env("GRAPH_BENCH_PREFIX")?;
    let cache_dir = required_env("GRAPH_DATA_CACHE_DIR")?;
    let ready_file = required_env("GRAPH_BENCH_READY_FILE")?;
    let stop_file = required_env("GRAPH_BENCH_STOP_FILE")?;
    let seed = env_bool("GRAPH_BENCH_SEED", false);
    let addr = required_env("GRAPH_BOLT_ADDR")?.parse()?;
    let object_store = object_store_from_env(None)?;
    let control = Arc::new(
        GraphControlPlane::open(format!("{prefix}/control"), Arc::clone(&object_store)).await?,
    );
    control
        .publish_placement(&ShardPlacement::fixed([(CELL_ID, "benchmark-node")])?)
        .await?;
    let cluster = Arc::new(
        RoutedGraphCluster::open_owned_with_control_and_options(
            format!("{prefix}/data"),
            "benchmark-node",
            control.as_ref(),
            object_store,
            Duration::from_secs(300),
            graph_options(fanout, max_hop, cache_dir.into()),
        )
        .await?,
    );
    let lease_renewer = cluster.start_lease_renewer(
        Arc::clone(&control),
        Duration::from_secs(300),
        Duration::from_secs(60),
    )?;
    if seed {
        seed_graph(&cluster, fanout, max_hop).await?;
    }

    let scope = GraphScope::default();
    let authorizer = StaticQueryTransportScopeAuthorizer::new().with_bearer_grant(
        TOKEN,
        QueryTransportScopeGrant::graph(
            scope.clone(),
            [
                QueryTransportAction::Read,
                QueryTransportAction::Write,
                QueryTransportAction::Cancel,
            ],
        ),
    )?;
    let service = ClientQueryService::new(
        cluster.clone(),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token(TOKEN)
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_concurrent_queries(256)
            .with_max_query_runtime_ms(180_000),
    )?;
    let resolver =
        StaticClientDatabaseResolver::single("default", ClientQueryTarget::new(scope, CELL_ID)?)?;
    let server = ClientBoltServer::bind(
        addr,
        service,
        BoltServerConfig::new(Arc::new(resolver))
            .with_max_connections(256)
            .with_prefetch_rows(64)
            .insecure_allow_plaintext(),
    )
    .await?;
    std::fs::write(&ready_file, format!("{}\n", server.local_addr()))?;
    eprintln!(
        "benchmark-server-ready fanout={fanout} seed={seed} addr={} prefix={prefix}",
        server.local_addr()
    );

    while !std::path::Path::new(&stop_file).exists() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = std::fs::remove_file(&ready_file);
    server.stop().await?;
    lease_renewer.stop().await?;
    cluster.close().await?;
    control.close().await?;
    Ok(())
}

async fn seed_graph(cluster: &RoutedGraphCluster, fanout: u64, max_hop: u8) -> BenchResult<()> {
    let shard = cluster.shard(CELL_ID)?;
    for hop in HOPS {
        let edge_type = edge_type(*hop);
        shard
            .bulk_append_edges_trusted_chunked(
                CELL_ID,
                &edge_type,
                layered_edges(fanout, max_hop),
                &format!("s3-bolt-seed-{fanout}-{hop}"),
                10_000,
            )
            .await?;
    }

    let epoch = shard.current_epoch(CELL_ID).await?;
    for hop in HOPS {
        let edge_type = edge_type(*hop);
        shard
            .build_matrix_tiles(CELL_ID, &edge_type, epoch, 4_096)
            .await?;
        shard
            .build_supernode_groups_for_directions(
                CELL_ID,
                &edge_type,
                epoch,
                10,
                512,
                &[ArtifactDirection::Out],
            )
            .await?;
        shard
            .refresh_edge_type_query_stats(CELL_ID, &edge_type)
            .await?;
        eprintln!("benchmark-seeded fanout={fanout} hop={hop} edge_type={edge_type}");
    }
    Ok(())
}

fn graph_options(fanout: u64, max_hop: u8, cache_dir: std::path::PathBuf) -> GraphOpenOptions {
    let edges = fanout.saturating_mul(u64::from(max_hop));
    GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: usize::try_from(edges.saturating_add(10_000))
                .unwrap_or(usize::MAX),
            max_artifact_source_epochs: u64::MAX,
            max_traversal_hops: max_hop,
            max_artifact_build_edges: edges.saturating_add(1),
            max_query_result_vertices: usize::try_from(fanout.saturating_add(1_024))
                .unwrap_or(usize::MAX),
            max_query_intermediate_rows: usize::try_from(edges.saturating_add(1_024))
                .unwrap_or(usize::MAX),
            max_query_index_candidates: usize::try_from(edges.saturating_add(1_024))
                .unwrap_or(usize::MAX),
            max_query_scan_edges: edges.saturating_mul(2).max(1),
            max_query_runtime_ms: Some(180_000),
        },
        cache: GraphCacheConfig::disk_cache_without_preload(cache_dir, 2 * 1024 * 1024 * 1024),
        cache_policy: GraphCachePolicy {
            max_matrix_adjacencies: 0,
            max_graphblas_matrices: HOPS.len(),
            max_reachability_results: 0,
            max_entries_per_cell: None,
            pin_matrix_min_edges: 0,
            max_concurrent_hydrations: HOPS.len(),
            ..Default::default()
        },
        backpressure_policy: GraphBackpressurePolicy {
            max_concurrent_graph_writes: 1,
            ..Default::default()
        },
        index_policy: GraphIndexPolicy::Full,
        retention_policy: GraphRetentionPolicy {
            read_lease_ttl_ms: 60_000,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn layered_edges(fanout: u64, max_hop: u8) -> impl Iterator<Item = (u64, u64)> {
    (0..fanout).flat_map(move |index| {
        (1..=max_hop).scan(1, move |src, hop| {
            let dst = (u64::from(hop) * 1_000_000) + index + 1;
            let edge = (*src, dst);
            *src = dst;
            Some(edge)
        })
    })
}

fn edge_type(hop: u8) -> String {
    format!("BENCH_H{hop}")
}

fn required_env(name: &str) -> BenchResult<String> {
    std::env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn env_u64(name: &str, default: u64) -> BenchResult<u64> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Some(true),
            "0" | "false" | "no" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}
