use std::sync::Arc;
use std::time::Duration;

use slatedb_graph_kernel::{
    object_store_from_env, BoltServerConfig, ClientBoltServer, ClientQueryService,
    ClientQueryServiceConfig, ClientQueryTarget, GraphBackpressurePolicy, GraphCacheConfig,
    GraphCachePolicy, GraphIndexPolicy, GraphLimits, GraphOpenOptions, GraphScope,
    ObjectStoreNodeDirectory, QueryTransportAction, QueryTransportScopeGrant, RoutedGraphCluster,
    StaticClientDatabaseResolver, StaticQueryTransportScopeAuthorizer, StorageSequence,
};

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "bolt-bench";
const EDGE_TYPE: &str = "BENCH";
const TOKEN: &str = "s3-bolt-benchmark-secret-32-chars";
const HOPS: &[u8] = &[1, 3, 5, 10];

#[tokio::main]
async fn main() -> BenchResult<()> {
    if !cfg!(feature = "graphblas") {
        return Err("s3_bolt_benchmark_server requires the graphblas feature".into());
    }
    let fanout = env_u64("GRAPH_BENCH_FANOUT", 100)?;
    let max_hop = *HOPS.iter().max().expect("benchmark hops are nonempty");
    let prefix = required_env("GRAPH_BENCH_PREFIX")?;
    let cache_dir = required_env("GRAPH_DATA_CACHE_DIR")?;
    let ready_file = required_env("GRAPH_BENCH_READY_FILE")?;
    let stop_file = required_env("GRAPH_BENCH_STOP_FILE")?;
    let metrics_file = required_env("GRAPH_BENCH_METRICS_FILE")?;
    let expected_graphblas_tasks = env_u64("GRAPH_BENCH_EXPECTED_GRAPHBLAS_TASKS", 0)?;
    let seed = env_bool("GRAPH_BENCH_SEED", false);
    let addr = required_env("GRAPH_BOLT_ADDR")?.parse()?;
    let object_store = object_store_from_env(None)?;
    let cluster = Arc::new(
        RoutedGraphCluster::open_promotable_with_options(
            format!("{prefix}/data"),
            "benchmark-node",
            ObjectStoreNodeDirectory::new([CELL_ID], ["benchmark-node"])?,
            object_store,
            graph_options(fanout, max_hop, cache_dir.into()),
        )
        .await?,
    );
    if seed {
        seed_graph(&cluster, fanout, max_hop).await?;
    }
    let read_epoch = cluster.shard(CELL_ID)?.current_epoch(CELL_ID).await?;
    verify_graphblas_artifacts(&cluster, read_epoch).await?;
    let metrics_before = cluster.shard(CELL_ID)?.graph_operational_metrics();

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
    let service_metrics_before = service.metrics();
    let service_metrics = service.clone();
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
    let service_metrics_after = service_metrics.metrics();
    let shard = cluster.shard(CELL_ID)?;
    let metrics_after = shard.graph_operational_metrics();
    let cache_resident = shard.graph_cache_resident_bytes().await;
    let (process_rss_kib, process_peak_rss_kib) = process_memory_kib();
    let graph_compute_tasks = metrics_after
        .graph_compute_tasks
        .saturating_sub(metrics_before.graph_compute_tasks);
    let report = serde_json::json!({
        "graphblas_compiled": cfg!(feature = "graphblas"),
        "verified_matrix_artifacts": 1,
        "read_epoch": read_epoch,
        "expected_graphblas_tasks": expected_graphblas_tasks,
        "graph_compute_tasks": graph_compute_tasks,
        "graph_compute_queue_us": metrics_after
            .graph_compute_queue_us
            .saturating_sub(metrics_before.graph_compute_queue_us),
        "graph_compute_duration_us": metrics_after
            .graph_compute_duration_us
            .saturating_sub(metrics_before.graph_compute_duration_us),
        "query_artifact_lookup_us": metrics_after
            .query_artifact_lookup_us
            .saturating_sub(metrics_before.query_artifact_lookup_us),
        "query_graphblas_cache_us": metrics_after
            .query_graphblas_cache_us
            .saturating_sub(metrics_before.query_graphblas_cache_us),
        "query_rows_completed": metrics_after
            .query_rows_completed
            .saturating_sub(metrics_before.query_rows_completed),
        "query_rows_failed": metrics_after
            .query_rows_failed
            .saturating_sub(metrics_before.query_rows_failed),
        "query_rows_duration_us": metrics_after
            .query_rows_duration_us
            .saturating_sub(metrics_before.query_rows_duration_us),
        "client_prepare_requests": service_metrics_after
            .prepare_requests
            .saturating_sub(service_metrics_before.prepare_requests),
        "client_prepare_duration_us": service_metrics_after
            .prepare_duration_us
            .saturating_sub(service_metrics_before.prepare_duration_us),
        "client_queries_started": service_metrics_after
            .queries_started
            .saturating_sub(service_metrics_before.queries_started),
        "client_execution_duration_us": service_metrics_after
            .execution_duration_us
            .saturating_sub(service_metrics_before.execution_duration_us),
        "cache_resident_bytes": {
            "total": cache_resident.total(),
            "matrix_adjacencies": cache_resident.matrix_adjacencies,
            "graphblas_matrices": cache_resident.graphblas_matrices,
            "relationship_rows": cache_resident.relationship_rows,
            "source_relationship_rows": cache_resident.source_relationship_rows,
            "relationship_property_rows": cache_resident.relationship_property_rows,
        },
        "process_rss_kib": process_rss_kib,
        "process_peak_rss_kib": process_peak_rss_kib,
    });
    std::fs::write(&metrics_file, serde_json::to_vec_pretty(&report)?)?;
    cluster.close().await?;
    if graph_compute_tasks < expected_graphblas_tasks {
        return Err(format!(
            "GraphBLAS verification failed: observed {graph_compute_tasks} compute tasks, expected at least {expected_graphblas_tasks}"
        )
        .into());
    }
    Ok(())
}

async fn seed_graph(cluster: &RoutedGraphCluster, fanout: u64, max_hop: u8) -> BenchResult<()> {
    let shard = cluster.shard(CELL_ID)?;
    shard
        .bulk_append_edges_trusted_chunked(
            CELL_ID,
            EDGE_TYPE,
            layered_edges(fanout, max_hop),
            &format!("s3-bolt-seed-{fanout}-{max_hop}"),
            slatedb_graph_kernel::DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
        )
        .await?;

    let epoch = shard.current_epoch(CELL_ID).await?;
    shard
        .build_adjacency_image(CELL_ID, EDGE_TYPE, epoch, 4_096)
        .await?;
    shard
        .refresh_edge_type_query_stats(CELL_ID, EDGE_TYPE)
        .await?;
    eprintln!("benchmark-seeded fanout={fanout} max_hop={max_hop} edge_type={EDGE_TYPE}");
    Ok(())
}

async fn verify_graphblas_artifacts(
    cluster: &RoutedGraphCluster,
    read_epoch: StorageSequence,
) -> BenchResult<()> {
    let shard = cluster.shard(CELL_ID)?;
    let artifact = shard
        .latest_matrix_artifact(CELL_ID, EDGE_TYPE, read_epoch)
        .await?
        .ok_or_else(|| {
            format!("missing adjacency image for edge type {EDGE_TYPE} at epoch {read_epoch}")
        })?;
    if artifact.base_epoch != read_epoch {
        return Err(format!(
            "stale adjacency image for edge type {EDGE_TYPE}: base epoch {}, read epoch {read_epoch}",
            artifact.base_epoch
        )
        .into());
    }
    eprintln!(
        "benchmark-artifact-verified edge_type={EDGE_TYPE} epoch={read_epoch} edges={}",
        artifact.edge_count
    );
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
            max_graphblas_matrices: 1,
            max_entries_per_cell: None,
            pin_matrix_min_edges: 0,
            max_concurrent_hydrations: 1,
            ..Default::default()
        },
        backpressure_policy: GraphBackpressurePolicy {
            max_concurrent_graph_writes: 1,
            ..Default::default()
        },
        index_policy: GraphIndexPolicy::Full,
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

fn process_memory_kib() -> (Option<u64>, Option<u64>) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    (
        proc_status_kib(&status, "VmRSS:"),
        proc_status_kib(&status, "VmHWM:"),
    )
}

fn proc_status_kib(status: &str, field: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}
