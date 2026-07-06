use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, ArtifactDirection, GraphCacheConfig,
    GraphCachePolicy, GraphLimits, GraphOpenOptions, GraphShard, QueryContext, RowQueryPlan,
};

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";
const DEFAULT_FANOUTS: &[u64] = &[50, 100, 1_000, 5_000, 10_000];
const DEFAULT_HOPS: &[u8] = &[1, 5, 10, 15, 20];

struct BenchEnv {
    shard_path: String,
    object_store: Arc<dyn ObjectStore>,
    cache_root: PathBuf,
    cache_bytes: usize,
    fanout: u64,
    max_hop: u8,
}

struct RunConfig {
    hot_iters: u32,
    concurrency: usize,
    concurrent_iters: u32,
    page_size: usize,
}

struct PrintRecord<'a> {
    kind: &'a str,
    object_backend: &'a str,
    fanout: u64,
    hops: u8,
    edges: u64,
    query_shape: &'a str,
    page_size: usize,
    concurrency: usize,
    build_elapsed: Duration,
    result: QueryBenchResult,
    optimizer_plan: &'a str,
}

#[tokio::main]
async fn main() -> BenchResult<()> {
    let fanouts = parse_u64_list("PHASE2_QUERY_BENCH_FANOUTS", DEFAULT_FANOUTS);
    let hops = parse_u8_list("PHASE2_QUERY_BENCH_HOPS", DEFAULT_HOPS);
    let max_hop = env_u8(
        "PHASE2_QUERY_BENCH_DATA_HOPS",
        hops.iter().copied().max().unwrap_or(20),
    );
    let hot_iters = env_u32("PHASE2_QUERY_BENCH_HOT_ITERS", 9).max(1);
    let concurrency = env_usize("PHASE2_QUERY_BENCH_CONCURRENCY", 8).max(1);
    let concurrent_iters = env_u32("PHASE2_QUERY_BENCH_CONCURRENT_ITERS", 16).max(1);
    let page_size = env_usize("PHASE2_QUERY_BENCH_PAGE_SIZE", 64).max(1);
    let tile_size = env_u64("PHASE2_QUERY_BENCH_MATRIX_TILE", 4_096);
    let cache_bytes = env_usize(
        "PHASE2_QUERY_BENCH_DISK_CACHE_BYTES",
        8 * 1024 * 1024 * 1024,
    );
    let bulk_chunk_size = env_usize("PHASE2_QUERY_BENCH_BULK_CHUNK_SIZE", 100_000);

    let bench_root = TempBenchRoot::new()?;
    let cache_root = bench_root.path().join("slatedb-cache");
    fs::create_dir_all(&cache_root)?;
    let (object_store, object_backend, object_label) =
        if let Ok(env_file) = std::env::var("PHASE2_QUERY_BENCH_OBJECT_ENV") {
            (
                object_store_from_env(Some(env_file.clone()))?,
                "env",
                format!("env:{env_file}"),
            )
        } else {
            let object_root = bench_root.path().join("object-store");
            fs::create_dir_all(&object_root)?;
            (
                local_object_store(&object_root)?,
                "local",
                format!("local:{}", object_root.display()),
            )
        };
    let run_id = format!("phase2-query-bench-{}", std::process::id());

    eprintln!(
        "phase2 query benchmark: fanouts={fanouts:?} hops={hops:?} max_hop={max_hop} hot_iters={hot_iters} concurrency={concurrency} concurrent_iters={concurrent_iters} page_size={page_size} object_store={} cache_root={}",
        object_label,
        cache_root.display()
    );
    println!(
        "kind,object_backend,fanout,hops,edges,query_shape,page_size,build_ms,cold_us,warm_us,hot_p50_us,hot_p95_us,hot_p99_us,hot_mean_us,hot_qps,concurrency,concurrent_queries,concurrent_p50_us,concurrent_p95_us,concurrent_p99_us,concurrent_mean_us,concurrent_qps,rows,concurrent_rows,has_next,cold_cache_hydrations,warm_cache_hits,warm_cache_misses,optimizer_plan"
    );
    io::stdout().flush()?;

    for fanout in fanouts {
        let shard_path = format!("{run_id}/fanout-{fanout}");
        let env = BenchEnv {
            shard_path,
            object_store: Arc::clone(&object_store),
            cache_root: cache_root.clone(),
            cache_bytes,
            fanout,
            max_hop,
        };
        let run_config = RunConfig {
            hot_iters,
            concurrency,
            concurrent_iters,
            page_size,
        };
        let writer = GraphShard::open_standalone_writer_with_options(
            env.shard_path.clone(),
            Arc::clone(&env.object_store),
            graph_options(None, cache_bytes, fanout, max_hop),
        )
        .await?;

        let build_started = Instant::now();
        writer
            .bulk_import_edges_chunked(
                CELL_ID,
                EDGE_TYPE,
                layered_edges(fanout, max_hop),
                &format!("query-fanout-{fanout}"),
                bulk_chunk_size,
            )
            .await?;
        let base_epoch = writer.current_epoch(CELL_ID).await?;
        writer
            .build_matrix_tiles(CELL_ID, EDGE_TYPE, base_epoch, tile_size)
            .await?;
        writer
            .build_supernode_groups_for_directions(
                CELL_ID,
                EDGE_TYPE,
                base_epoch,
                10,
                512,
                &[ArtifactDirection::Out],
            )
            .await?;
        writer
            .refresh_edge_type_query_stats(CELL_ID, EDGE_TYPE)
            .await?;
        let build_elapsed = build_started.elapsed();
        writer.close().await?;
        eprintln!(
            "fanout={fanout} stage=build edges={} elapsed_ms={}",
            layered_edge_count(fanout, max_hop),
            millis(build_elapsed)
        );

        let page_query =
            format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}]->(v) RETURN v.id ORDER BY v.id");
        let page_plan = explain_plan(&env, &page_query, "supernode-page-plan").await?;
        let page = bench_page_workload(&env, &run_config, &page_query).await?;
        print_result(PrintRecord {
            kind: "supernode_page",
            object_backend,
            fanout,
            hops: 1,
            edges: layered_edge_count(fanout, max_hop),
            query_shape: "cypher_page",
            page_size,
            concurrency,
            build_elapsed,
            result: page,
            optimizer_plan: &page_plan,
        })?;

        for &hop in &hops {
            let query = format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN v.id");
            let plan = explain_plan(&env, &query, &format!("multi-hop-plan-{hop}")).await?;
            let result = bench_rows_workload(&env, &run_config, hop, &query).await?;
            print_result(PrintRecord {
                kind: "multi_hop_rows",
                object_backend,
                fanout,
                hops: hop,
                edges: layered_edge_count(fanout, max_hop),
                query_shape: "cypher_rows",
                page_size: 0,
                concurrency,
                build_elapsed,
                result,
                optimizer_plan: &plan,
            })?;

            let count_query = format!(
                "MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN count(*) AS total"
            );
            let count_plan =
                explain_plan(&env, &count_query, &format!("multi-hop-count-plan-{hop}")).await?;
            let count_result = bench_rows_workload(&env, &run_config, hop, &count_query).await?;
            print_result(PrintRecord {
                kind: "multi_hop_count",
                object_backend,
                fanout,
                hops: hop,
                edges: layered_edge_count(fanout, max_hop),
                query_shape: "cypher_count",
                page_size: 0,
                concurrency,
                build_elapsed,
                result: count_result,
                optimizer_plan: &count_plan,
            })?;

            let page_query = format!(
                "MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN v.id ORDER BY v.id"
            );
            let page_plan =
                explain_plan(&env, &page_query, &format!("multi-hop-page-plan-{hop}")).await?;
            let page_result = bench_page_workload(&env, &run_config, &page_query).await?;
            print_result(PrintRecord {
                kind: "multi_hop_page",
                object_backend,
                fanout,
                hops: hop,
                edges: layered_edge_count(fanout, max_hop),
                query_shape: "cypher_page",
                page_size,
                concurrency,
                build_elapsed,
                result: page_result,
                optimizer_plan: &page_plan,
            })?;
        }
    }

    Ok(())
}

async fn explain_plan(env: &BenchEnv, query: &str, name: &str) -> BenchResult<String> {
    let cache_dir = env.cache_root.join(format!("fanout-{}-{name}", env.fanout));
    reset_dir(&cache_dir)?;
    let reader = GraphShard::open_with_options(
        env.shard_path.clone(),
        Arc::clone(&env.object_store),
        graph_options(Some(&cache_dir), env.cache_bytes, env.fanout, env.max_hop),
    )
    .await?;
    let plan = reader
        .explain_opencypher_rows(QueryContext::new(CELL_ID, format!("explain-{name}")), query)
        .await?;
    reader.close().await?;
    Ok(plan_signature(&plan))
}

async fn bench_rows_workload(
    env: &BenchEnv,
    run_config: &RunConfig,
    hop: u8,
    query: &str,
) -> BenchResult<QueryBenchResult> {
    let cache_dir = env
        .cache_root
        .join(format!("fanout-{}-hop-{hop}-rows", env.fanout));
    reset_dir(&cache_dir)?;
    let cold_reader = GraphShard::open_with_options(
        env.shard_path.clone(),
        Arc::clone(&env.object_store),
        graph_options(Some(&cache_dir), env.cache_bytes, env.fanout, env.max_hop),
    )
    .await?;
    let cold_started = Instant::now();
    let cold_rows = cold_reader
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, format!("cold-rows-{}-{hop}", env.fanout)),
            query,
        )
        .await?;
    let cold_elapsed = cold_started.elapsed();
    let cold_metrics = cold_reader.graph_cache_metrics();
    cold_reader.close().await?;

    let warm_reader = Arc::new(
        GraphShard::open_with_options(
            env.shard_path.clone(),
            Arc::clone(&env.object_store),
            graph_options(Some(&cache_dir), env.cache_bytes, env.fanout, env.max_hop),
        )
        .await?,
    );
    let warm_started = Instant::now();
    let warm_rows = warm_reader
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, format!("warm-rows-{}-{hop}", env.fanout)),
            query,
        )
        .await?;
    let warm_elapsed = warm_started.elapsed();
    assert_eq!(cold_rows.rows.len(), warm_rows.rows.len());

    let mut hot_latencies = Vec::with_capacity(run_config.hot_iters as usize);
    let hot_started = Instant::now();
    let mut rows = warm_rows.rows.len();
    for iter in 0..run_config.hot_iters {
        let started = Instant::now();
        let result = warm_reader
            .execute_cypher_rows(
                QueryContext::new(CELL_ID, format!("hot-rows-{}-{hop}-{iter}", env.fanout)),
                query,
            )
            .await?;
        rows = result.rows.len();
        hot_latencies.push(started.elapsed());
    }
    let hot_elapsed = hot_started.elapsed();
    let hot_stats = LatencyStats::from_durations(&hot_latencies);

    let concurrent = bench_concurrent_rows(
        Arc::clone(&warm_reader),
        env.fanout,
        hop,
        query,
        run_config.concurrency,
        run_config.concurrent_iters,
    )
    .await?;
    let warm_metrics = warm_reader.graph_cache_metrics();
    warm_reader.close().await?;

    Ok(QueryBenchResult {
        cold_us: micros(cold_elapsed),
        warm_us: micros(warm_elapsed),
        hot: hot_stats,
        hot_qps: f64::from(run_config.hot_iters) / hot_elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        concurrent,
        rows,
        has_next: false,
        cold_cache_hydrations: cold_metrics.hydration_started,
        warm_cache_hits: cache_hits(&warm_metrics),
        warm_cache_misses: cache_misses(&warm_metrics),
    })
}

async fn bench_page_workload(
    env: &BenchEnv,
    run_config: &RunConfig,
    query: &str,
) -> BenchResult<QueryBenchResult> {
    let cache_dir = env.cache_root.join(format!("fanout-{}-page", env.fanout));
    reset_dir(&cache_dir)?;
    let cold_reader = GraphShard::open_with_options(
        env.shard_path.clone(),
        Arc::clone(&env.object_store),
        graph_options(Some(&cache_dir), env.cache_bytes, env.fanout, env.max_hop),
    )
    .await?;
    let cold_started = Instant::now();
    let cold_page = cold_reader
        .execute_cypher_rows_page(
            QueryContext::new(CELL_ID, format!("cold-page-{}", env.fanout)),
            query,
            None,
            run_config.page_size,
        )
        .await?;
    let cold_elapsed = cold_started.elapsed();
    let cold_metrics = cold_reader.graph_cache_metrics();
    cold_reader.close().await?;

    let warm_reader = Arc::new(
        GraphShard::open_with_options(
            env.shard_path.clone(),
            Arc::clone(&env.object_store),
            graph_options(Some(&cache_dir), env.cache_bytes, env.fanout, env.max_hop),
        )
        .await?,
    );
    let warm_started = Instant::now();
    let warm_page = warm_reader
        .execute_cypher_rows_page(
            QueryContext::new(CELL_ID, format!("warm-page-{}", env.fanout)),
            query,
            None,
            run_config.page_size,
        )
        .await?;
    let warm_elapsed = warm_started.elapsed();
    assert_eq!(cold_page.rows.len(), warm_page.rows.len());

    let mut hot_latencies = Vec::with_capacity(run_config.hot_iters as usize);
    let hot_started = Instant::now();
    let mut rows = warm_page.rows.len();
    let mut has_next = warm_page.next_cursor.is_some();
    for iter in 0..run_config.hot_iters {
        let started = Instant::now();
        let page = warm_reader
            .execute_cypher_rows_page(
                QueryContext::new(CELL_ID, format!("hot-page-{}-{iter}", env.fanout)),
                query,
                None,
                run_config.page_size,
            )
            .await?;
        rows = page.rows.len();
        has_next = page.next_cursor.is_some();
        hot_latencies.push(started.elapsed());
    }
    let hot_elapsed = hot_started.elapsed();
    let hot_stats = LatencyStats::from_durations(&hot_latencies);

    let concurrent = bench_concurrent_pages(
        Arc::clone(&warm_reader),
        env.fanout,
        query,
        run_config.page_size,
        run_config.concurrency,
        run_config.concurrent_iters,
    )
    .await?;
    let warm_metrics = warm_reader.graph_cache_metrics();
    warm_reader.close().await?;

    Ok(QueryBenchResult {
        cold_us: micros(cold_elapsed),
        warm_us: micros(warm_elapsed),
        hot: hot_stats,
        hot_qps: f64::from(run_config.hot_iters) / hot_elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        concurrent,
        rows,
        has_next,
        cold_cache_hydrations: cold_metrics.hydration_started,
        warm_cache_hits: cache_hits(&warm_metrics),
        warm_cache_misses: cache_misses(&warm_metrics),
    })
}

async fn bench_concurrent_rows(
    shard: Arc<GraphShard>,
    fanout: u64,
    hop: u8,
    query: &str,
    concurrency: usize,
    iters_per_worker: u32,
) -> BenchResult<ConcurrentStats> {
    let total_started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let shard = Arc::clone(&shard);
        let query = query.to_string();
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(iters_per_worker as usize);
            let mut rows = 0_usize;
            for iter in 0..iters_per_worker {
                let started = Instant::now();
                let result = shard
                    .execute_cypher_rows(
                        QueryContext::new(
                            CELL_ID,
                            format!("concurrent-rows-{fanout}-{hop}-{worker}-{iter}"),
                        ),
                        &query,
                    )
                    .await?;
                rows = result.rows.len();
                latencies.push(started.elapsed());
            }
            Ok::<_, slatedb_graph_kernel::GraphError>((latencies, rows))
        }));
    }
    let mut latencies = Vec::new();
    let mut rows = 0_usize;
    for task in tasks {
        let (mut task_latencies, task_rows) = task.await??;
        rows = task_rows;
        latencies.append(&mut task_latencies);
    }
    let elapsed = total_started.elapsed();
    Ok(ConcurrentStats {
        stats: LatencyStats::from_durations(&latencies),
        qps: (latencies.len() as f64) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        queries: latencies.len() as u64,
        rows,
    })
}

async fn bench_concurrent_pages(
    shard: Arc<GraphShard>,
    fanout: u64,
    query: &str,
    page_size: usize,
    concurrency: usize,
    iters_per_worker: u32,
) -> BenchResult<ConcurrentStats> {
    let total_started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let shard = Arc::clone(&shard);
        let query = query.to_string();
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(iters_per_worker as usize);
            let mut rows = 0_usize;
            for iter in 0..iters_per_worker {
                let started = Instant::now();
                let page = shard
                    .execute_cypher_rows_page(
                        QueryContext::new(
                            CELL_ID,
                            format!("concurrent-page-{fanout}-{worker}-{iter}"),
                        ),
                        &query,
                        None,
                        page_size,
                    )
                    .await?;
                rows = page.rows.len();
                latencies.push(started.elapsed());
            }
            Ok::<_, slatedb_graph_kernel::GraphError>((latencies, rows))
        }));
    }
    let mut latencies = Vec::new();
    let mut rows = 0_usize;
    for task in tasks {
        let (mut task_latencies, task_rows) = task.await??;
        rows = task_rows;
        latencies.append(&mut task_latencies);
    }
    let elapsed = total_started.elapsed();
    Ok(ConcurrentStats {
        stats: LatencyStats::from_durations(&latencies),
        qps: (latencies.len() as f64) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        queries: latencies.len() as u64,
        rows,
    })
}

fn graph_options(
    cache_dir: Option<&Path>,
    cache_bytes: usize,
    fanout: u64,
    max_hop: u8,
) -> GraphOpenOptions {
    let edges = layered_edge_count(fanout, max_hop);
    let query_rows = edges.saturating_add(fanout).saturating_add(1_024);
    GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: usize::try_from(edges).unwrap_or(usize::MAX).max(1),
            max_artifact_source_epochs: u64::MAX,
            max_traversal_hops: max_hop,
            max_artifact_build_edges: edges.saturating_add(1),
            max_query_result_vertices: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_intermediate_rows: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_index_candidates: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_scan_edges: edges.saturating_mul(u64::from(max_hop).max(1)).max(1),
            max_query_runtime_ms: Some(env_u64("PHASE2_QUERY_BENCH_QUERY_TIMEOUT_MS", 120_000)),
        },
        cache: cache_dir
            .map(|path| GraphCacheConfig::disk_cache_without_preload(path, cache_bytes))
            .unwrap_or_default(),
        cache_policy: GraphCachePolicy {
            max_matrix_adjacencies: 8,
            max_graphblas_matrices: 8,
            max_posting_chunks: 262_144,
            max_entries_per_cell: None,
            pin_matrix_min_edges: 50_000,
            pin_supernode_min_degree: 10_000,
            prefetch_supernode_chunks: 2,
            max_concurrent_hydrations: 32,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn print_result(record: PrintRecord<'_>) -> BenchResult<()> {
    let result = record.result;
    println!(
        "{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.2},{},{},{:.3},{:.3},{:.3},{:.3},{:.2},{},{},{},{},{},{},{}",
        record.kind,
        record.object_backend,
        record.fanout,
        record.hops,
        record.edges,
        record.query_shape,
        record.page_size,
        millis(record.build_elapsed),
        result.cold_us,
        result.warm_us,
        result.hot.p50_us,
        result.hot.p95_us,
        result.hot.p99_us,
        result.hot.mean_us,
        result.hot_qps,
        record.concurrency,
        result.concurrent.queries,
        result.concurrent.stats.p50_us,
        result.concurrent.stats.p95_us,
        result.concurrent.stats.p99_us,
        result.concurrent.stats.mean_us,
        result.concurrent.qps,
        result.rows,
        result.concurrent.rows,
        result.has_next,
        result.cold_cache_hydrations,
        result.warm_cache_hits,
        result.warm_cache_misses,
        csv_field(record.optimizer_plan)
    );
    io::stdout().flush()?;
    Ok(())
}

fn plan_signature(plan: &RowQueryPlan) -> String {
    let mut parts = Vec::new();
    for group in &plan.groups {
        for pattern in &group.patterns {
            parts.push(format!(
                "{:?}:{:?}:{:?}",
                pattern.access, pattern.optimizer_passes, group.optimizer_passes
            ));
        }
    }
    for arm in &plan.union_arms {
        parts.push(plan_signature(arm));
    }
    parts.join("+")
}

fn csv_field(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn cache_hits(metrics: &slatedb_graph_kernel::GraphCacheMetricsSnapshot) -> u64 {
    metrics.matrix_artifact_hits
        + metrics.matrix_adjacency_hits
        + metrics.graphblas_hits
        + metrics.parsed_row_query_hits
        + metrics.reachability_result_hits
        + metrics.supernode_group_hits
        + metrics.posting_chunk_hits
        + metrics.materialized_supernode_hits
}

fn cache_misses(metrics: &slatedb_graph_kernel::GraphCacheMetricsSnapshot) -> u64 {
    metrics.matrix_artifact_misses
        + metrics.matrix_adjacency_misses
        + metrics.graphblas_misses
        + metrics.parsed_row_query_misses
        + metrics.reachability_result_misses
        + metrics.supernode_group_misses
        + metrics.posting_chunk_misses
        + metrics.materialized_supernode_misses
}

fn layered_edges(fanout: u64, max_hop: u8) -> impl Iterator<Item = (u64, u64)> {
    (0..fanout).flat_map(move |index| {
        (1..=max_hop).scan(1, move |src, hop| {
            let dst = layer_vertex(hop, index);
            let edge = (*src, dst);
            *src = dst;
            Some(edge)
        })
    })
}

fn layered_edge_count(fanout: u64, max_hop: u8) -> u64 {
    fanout.saturating_mul(u64::from(max_hop))
}

fn layer_vertex(hop: u8, index: u64) -> u64 {
    (u64::from(hop) * 1_000_000) + index + 1
}

fn reset_dir(path: &Path) -> BenchResult<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn parse_u64_list(name: &str, default: &[u64]) -> Vec<u64> {
    let Ok(value) = std::env::var(name) else {
        return default.to_vec();
    };
    let parsed: Vec<_> = value
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .filter(|value| *value > 0)
        .collect();
    if parsed.is_empty() {
        default.to_vec()
    } else {
        parsed
    }
}

fn parse_u8_list(name: &str, default: &[u8]) -> Vec<u8> {
    let Ok(value) = std::env::var(name) else {
        return default.to_vec();
    };
    let parsed: Vec<_> = value
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .filter(|value| *value > 0)
        .collect();
    if parsed.is_empty() {
        default.to_vec()
    } else {
        parsed
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn micros(duration: Duration) -> u128 {
    duration.as_micros()
}

fn millis(duration: Duration) -> u128 {
    duration.as_millis()
}

#[derive(Default)]
struct QueryBenchResult {
    cold_us: u128,
    warm_us: u128,
    hot: LatencyStats,
    hot_qps: f64,
    concurrent: ConcurrentStats,
    rows: usize,
    has_next: bool,
    cold_cache_hydrations: u64,
    warm_cache_hits: u64,
    warm_cache_misses: u64,
}

#[derive(Default)]
struct ConcurrentStats {
    stats: LatencyStats,
    qps: f64,
    queries: u64,
    rows: usize,
}

#[derive(Default)]
struct LatencyStats {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    mean_us: f64,
}

impl LatencyStats {
    fn from_durations(durations: &[Duration]) -> Self {
        if durations.is_empty() {
            return Self::default();
        }
        let mut values: Vec<_> = durations
            .iter()
            .map(|duration| duration.as_nanos())
            .collect();
        values.sort_unstable();
        let total: u128 = values.iter().sum();
        Self {
            p50_us: nanos_to_micros(percentile(&values, 50)),
            p95_us: nanos_to_micros(percentile(&values, 95)),
            p99_us: nanos_to_micros(percentile(&values, 99)),
            mean_us: nanos_to_micros(total) / (values.len() as f64),
        }
    }
}

fn percentile(values: &[u128], percentile: u32) -> u128 {
    let len = values.len();
    let index = ((len.saturating_sub(1) as u128) * u128::from(percentile)).div_ceil(100) as usize;
    values[index.min(len - 1)]
}

fn nanos_to_micros(nanos: u128) -> f64 {
    (nanos as f64) / 1_000.0
}

struct TempBenchRoot {
    path: PathBuf,
    keep: bool,
}

impl TempBenchRoot {
    fn new() -> BenchResult<Self> {
        let keep = std::env::var("PHASE2_QUERY_BENCH_KEEP")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"));
        let path = std::env::temp_dir().join(format!(
            "phase2-query-bench-{}-{}",
            std::process::id(),
            current_millis()
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path, keep })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBenchRoot {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn current_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}
