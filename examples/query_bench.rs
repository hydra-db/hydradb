use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, GraphCacheConfig, GraphCachePolicy,
    GraphIndexPolicy, GraphLimits, GraphMemoryConfig, GraphOpenOptions, GraphShard,
    GraphStorageMemoryConfig, QueryContext, RowQueryPlan,
};

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";
const DEFAULT_FANOUTS: &[u64] = &[50, 100, 1_000, 5_000, 10_000];
const DEFAULT_HOPS: &[u8] = &[1, 5, 10, 15, 20];
const CACHE_DIR_MARKER: &str = ".slatedb-graph-query-bench-cache";

struct BenchEnv {
    shard_path: String,
    object_store: Arc<dyn ObjectStore>,
    cache_root: PathBuf,
    cache_bytes: usize,
    fanout: u64,
    max_hop: u8,
    index_policy: GraphIndexPolicy,
}

struct RunConfig {
    cold_iters: u32,
    hot_iters: u32,
    concurrency: usize,
    concurrent_iters: u32,
    page_size: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchMode {
    Full,
    BuildOnly,
    QueryOnly,
}

impl BenchMode {
    fn builds(self) -> bool {
        matches!(self, Self::Full | Self::BuildOnly)
    }

    fn queries(self) -> bool {
        matches!(self, Self::Full | Self::QueryOnly)
    }
}

#[derive(Clone, Copy, Debug)]
struct WorkloadFilter {
    one_hop_page: bool,
    rows: bool,
    count: bool,
    page: bool,
}

impl WorkloadFilter {
    fn all() -> Self {
        Self {
            one_hop_page: true,
            rows: true,
            count: true,
            page: true,
        }
    }
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
    build_rss_bytes: u64,
    result: QueryBenchResult,
    optimizer_plan: &'a str,
}

fn main() -> BenchResult<()> {
    match string_env("GRAPH_QUERY_BENCH_RUNTIME", "multi-thread")
        .to_ascii_lowercase()
        .as_str()
    {
        "current-thread" | "current_thread" | "single-thread" | "single_thread" => {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async_main())
        }
        "multi-thread" | "multi_thread" | "threaded" => {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Ok(worker_threads) = std::env::var("GRAPH_QUERY_BENCH_RUNTIME_WORKERS") {
                if worker_threads.trim().is_empty() {
                    return builder.enable_all().build()?.block_on(async_main());
                }
                builder.worker_threads(worker_threads.parse::<usize>()?.max(1));
            }
            builder.enable_all().build()?.block_on(async_main())
        }
        other => Err(format!("unsupported GRAPH_QUERY_BENCH_RUNTIME={other}").into()),
    }
}

async fn async_main() -> BenchResult<()> {
    let fanouts = parse_u64_list("GRAPH_QUERY_BENCH_FANOUTS", DEFAULT_FANOUTS);
    let hops = parse_u8_list("GRAPH_QUERY_BENCH_HOPS", DEFAULT_HOPS);
    let max_hop = env_u8(
        "GRAPH_QUERY_BENCH_DATA_HOPS",
        hops.iter().copied().max().unwrap_or(20),
    );
    let hot_iters = env_u32("GRAPH_QUERY_BENCH_HOT_ITERS", 9).max(1);
    let concurrency = env_usize("GRAPH_QUERY_BENCH_CONCURRENCY", 8).max(1);
    let concurrent_iters = env_u32("GRAPH_QUERY_BENCH_CONCURRENT_ITERS", 16).max(1);
    let page_size = env_usize("GRAPH_QUERY_BENCH_PAGE_SIZE", 64).max(1);
    let tile_size = env_u64("GRAPH_QUERY_BENCH_MATRIX_TILE", 4_096);
    let cache_bytes = env_usize("GRAPH_QUERY_BENCH_DISK_CACHE_BYTES", 8 * 1024 * 1024 * 1024);
    let bulk_chunk_size = env_usize("GRAPH_QUERY_BENCH_BULK_CHUNK_SIZE", 10_000);
    let cold_iters = env_u32("GRAPH_QUERY_BENCH_COLD_ITERS", 5).max(1);
    let bench_mode = parse_bench_mode(&string_env("GRAPH_QUERY_BENCH_MODE", "full"))?;
    let workload_filter = parse_workload_filter(&string_env("GRAPH_QUERY_BENCH_WORKLOADS", "all"))?;
    let index_policy = parse_index_policy(&string_env(
        "GRAPH_QUERY_BENCH_INDEX_POLICY",
        "outbound-only",
    ))?;

    let bench_root = TempBenchRoot::new()?;
    let cache_root = bench_root.path().join("slatedb-cache");
    fs::create_dir_all(&cache_root)?;
    let (object_store, object_backend, object_label) =
        if let Ok(env_file) = std::env::var("GRAPH_QUERY_BENCH_OBJECT_ENV") {
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
    let run_id = string_env(
        "GRAPH_QUERY_BENCH_RUN_ID",
        &format!("query-bench-{}", std::process::id()),
    );

    eprintln!(
        "query benchmark: mode={bench_mode:?} run_id={run_id} fanouts={fanouts:?} hops={hops:?} max_hop={max_hop} cold_iters={cold_iters} hot_iters={hot_iters} concurrency={concurrency} concurrent_iters={concurrent_iters} page_size={page_size} index_policy={index_policy:?} object_store={} cache_root={}",
        object_label,
        cache_root.display()
    );
    println!(
        "kind,object_backend,fanout,hops,edges,query_shape,page_size,build_ms,build_rss_mib,cold_samples,cold_open_query_p50_us,cold_open_query_p95_us,cold_open_query_p99_us,cold_open_query_mean_us,cold_query_p50_us,cold_query_p95_us,cold_query_p99_us,cold_query_mean_us,cold_peak_rss_mib,warm_us,warm_rss_mib,hot_p50_us,hot_p95_us,hot_p99_us,hot_mean_us,hot_qps,hot_peak_rss_mib,concurrency,concurrent_queries,concurrent_p50_us,concurrent_p95_us,concurrent_p99_us,concurrent_mean_us,concurrent_qps,concurrent_peak_rss_mib,rows,concurrent_rows,has_next,cold_cache_hydrations,warm_cache_hits,warm_cache_misses,optimizer_plan"
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
            index_policy,
        };
        let run_config = RunConfig {
            cold_iters,
            hot_iters,
            concurrency,
            concurrent_iters,
            page_size,
        };
        let (build_elapsed, build_rss_bytes) = if bench_mode.builds() {
            let writer = GraphShard::open_standalone_writer_with_memory_options(
                env.shard_path.clone(),
                Arc::clone(&env.object_store),
                graph_options(None, cache_bytes, fanout, max_hop, index_policy),
                graph_memory_config(),
            )
            .await?;
            log_stage_rss(fanout, "writer-open");

            let build_started = Instant::now();
            writer
                .bulk_append_edges_trusted_chunked(
                    CELL_ID,
                    EDGE_TYPE,
                    layered_edges(fanout, max_hop),
                    &format!("query-fanout-{fanout}"),
                    bulk_chunk_size,
                )
                .await?;
            log_stage_rss(fanout, "bulk-import");
            let base_epoch = writer.current_epoch(CELL_ID).await?;
            writer
                .build_matrix_tiles(CELL_ID, EDGE_TYPE, base_epoch, tile_size)
                .await?;
            log_stage_rss(fanout, "matrix-build");
            writer
                .refresh_edge_type_query_stats(CELL_ID, EDGE_TYPE)
                .await?;
            log_stage_rss(fanout, "stats-refresh");
            let build_elapsed = build_started.elapsed();
            trim_process_memory_for_profile();
            let build_rss_bytes = current_rss_bytes();
            writer.close().await?;
            trim_process_memory_for_profile();
            log_stage_rss(fanout, "writer-close");
            eprintln!(
                "fanout={fanout} stage=build edges={} elapsed_ms={} rss_mib={:.1}",
                layered_edge_count(fanout, max_hop),
                millis(build_elapsed),
                bytes_to_mib(build_rss_bytes)
            );
            (build_elapsed, build_rss_bytes)
        } else {
            let rss = current_rss_bytes();
            eprintln!(
                "fanout={fanout} stage=query-only-open edges={} rss_mib={:.1}",
                layered_edge_count(fanout, max_hop),
                bytes_to_mib(rss)
            );
            (Duration::ZERO, rss)
        };

        if !bench_mode.queries() {
            continue;
        }

        let page_query =
            format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}]->(v) RETURN v.id ORDER BY v.id");
        if workload_filter.one_hop_page {
            let page = bench_page_workload(&env, &run_config, "one-hop-page", &page_query).await?;
            let page_plan = explain_plan(&env, &page_query, "one-hop-page-plan").await?;
            print_result(PrintRecord {
                kind: "one_hop_page",
                object_backend,
                fanout,
                hops: 1,
                edges: layered_edge_count(fanout, max_hop),
                query_shape: "cypher_page",
                page_size,
                concurrency,
                build_elapsed,
                build_rss_bytes,
                result: page,
                optimizer_plan: &page_plan,
            })?;
        }

        for &hop in &hops {
            if workload_filter.rows {
                let query = format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN v.id");
                let result = bench_rows_workload(&env, &run_config, hop, &query).await?;
                let plan = explain_plan(&env, &query, &format!("multi-hop-plan-{hop}")).await?;
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
                    build_rss_bytes,
                    result,
                    optimizer_plan: &plan,
                })?;
            }

            if workload_filter.count {
                let count_query = format!(
                    "MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN count(*) AS total"
                );
                let count_result =
                    bench_rows_workload(&env, &run_config, hop, &count_query).await?;
                let count_plan =
                    explain_plan(&env, &count_query, &format!("multi-hop-count-plan-{hop}"))
                        .await?;
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
                    build_rss_bytes,
                    result: count_result,
                    optimizer_plan: &count_plan,
                })?;
            }

            if workload_filter.page {
                let page_query = format!(
                    "MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN v.id ORDER BY v.id"
                );
                let page_result =
                    bench_page_workload(&env, &run_config, &format!("hop-{hop}-page"), &page_query)
                        .await?;
                let page_plan =
                    explain_plan(&env, &page_query, &format!("multi-hop-page-plan-{hop}")).await?;
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
                    build_rss_bytes,
                    result: page_result,
                    optimizer_plan: &page_plan,
                })?;
            }
        }
    }

    Ok(())
}

async fn explain_plan(env: &BenchEnv, query: &str, name: &str) -> BenchResult<String> {
    let cache_dir = env.cache_root.join(format!("fanout-{}-{name}", env.fanout));
    reset_dir(&cache_dir)?;
    let reader = GraphShard::open_standalone_writer_with_memory_options(
        env.shard_path.clone(),
        Arc::clone(&env.object_store),
        graph_options(
            Some(&cache_dir),
            env.cache_bytes,
            env.fanout,
            env.max_hop,
            env.index_policy,
        ),
        graph_memory_config(),
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
    let cold = bench_cold_rows(env, run_config, hop, query).await?;

    let cache_dir = env
        .cache_root
        .join(format!("fanout-{}-hop-{hop}-rows-warm", env.fanout));
    reset_dir(&cache_dir)?;
    let seed_reader = GraphShard::open_standalone_writer_with_memory_options(
        env.shard_path.clone(),
        Arc::clone(&env.object_store),
        graph_options(
            Some(&cache_dir),
            env.cache_bytes,
            env.fanout,
            env.max_hop,
            env.index_policy,
        ),
        graph_memory_config(),
    )
    .await?;
    let seed_rows = seed_reader
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, format!("warm-seed-rows-{}-{hop}", env.fanout)),
            query,
        )
        .await?;
    assert_eq!(cold.rows, seed_rows.rows.len());
    seed_reader.close().await?;

    let warm_reader = Arc::new(
        GraphShard::open_standalone_writer_with_memory_options(
            env.shard_path.clone(),
            Arc::clone(&env.object_store),
            graph_options(
                Some(&cache_dir),
                env.cache_bytes,
                env.fanout,
                env.max_hop,
                env.index_policy,
            ),
            graph_memory_config(),
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
    let warm_rss_bytes = current_rss_bytes();
    assert_eq!(cold.rows, warm_rows.rows.len());

    let mut hot_latencies = Vec::with_capacity(run_config.hot_iters as usize);
    let hot_started = Instant::now();
    let mut rows = warm_rows.rows.len();
    let mut hot_peak_rss_bytes = warm_rss_bytes;
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
        hot_peak_rss_bytes = hot_peak_rss_bytes.max(current_rss_bytes());
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
        cold_samples: cold.samples,
        cold_open_query: cold.open_query,
        cold_query: cold.query,
        cold_peak_rss_bytes: cold.peak_rss_bytes,
        warm_us: micros(warm_elapsed),
        warm_rss_bytes,
        hot: hot_stats,
        hot_qps: f64::from(run_config.hot_iters) / hot_elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        hot_peak_rss_bytes,
        concurrent,
        rows,
        has_next: false,
        cold_cache_hydrations: cold.cache_hydrations,
        warm_cache_hits: cache_hits(&warm_metrics),
        warm_cache_misses: cache_misses(&warm_metrics),
    })
}

async fn bench_page_workload(
    env: &BenchEnv,
    run_config: &RunConfig,
    workload_name: &str,
    query: &str,
) -> BenchResult<QueryBenchResult> {
    let cold = bench_cold_page(env, run_config, workload_name, query).await?;

    let cache_dir = env
        .cache_root
        .join(format!("fanout-{}-{workload_name}-warm", env.fanout));
    reset_dir(&cache_dir)?;
    let seed_reader = GraphShard::open_standalone_writer_with_memory_options(
        env.shard_path.clone(),
        Arc::clone(&env.object_store),
        graph_options(
            Some(&cache_dir),
            env.cache_bytes,
            env.fanout,
            env.max_hop,
            env.index_policy,
        ),
        graph_memory_config(),
    )
    .await?;
    let seed_page = seed_reader
        .execute_cypher_rows_page(
            QueryContext::new(
                CELL_ID,
                format!("warm-seed-page-{}-{workload_name}", env.fanout),
            ),
            query,
            None,
            run_config.page_size,
        )
        .await?;
    assert_eq!(cold.rows, seed_page.rows.len());
    assert_eq!(cold.has_next, seed_page.next_cursor.is_some());
    seed_reader.close().await?;

    let warm_reader = Arc::new(
        GraphShard::open_standalone_writer_with_memory_options(
            env.shard_path.clone(),
            Arc::clone(&env.object_store),
            graph_options(
                Some(&cache_dir),
                env.cache_bytes,
                env.fanout,
                env.max_hop,
                env.index_policy,
            ),
            graph_memory_config(),
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
    let warm_rss_bytes = current_rss_bytes();
    assert_eq!(cold.rows, warm_page.rows.len());
    assert_eq!(cold.has_next, warm_page.next_cursor.is_some());

    let mut hot_latencies = Vec::with_capacity(run_config.hot_iters as usize);
    let hot_started = Instant::now();
    let mut rows = warm_page.rows.len();
    let mut has_next = warm_page.next_cursor.is_some();
    let mut hot_peak_rss_bytes = warm_rss_bytes;
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
        hot_peak_rss_bytes = hot_peak_rss_bytes.max(current_rss_bytes());
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
        cold_samples: cold.samples,
        cold_open_query: cold.open_query,
        cold_query: cold.query,
        cold_peak_rss_bytes: cold.peak_rss_bytes,
        warm_us: micros(warm_elapsed),
        warm_rss_bytes,
        hot: hot_stats,
        hot_qps: f64::from(run_config.hot_iters) / hot_elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        hot_peak_rss_bytes,
        concurrent,
        rows,
        has_next,
        cold_cache_hydrations: cold.cache_hydrations,
        warm_cache_hits: cache_hits(&warm_metrics),
        warm_cache_misses: cache_misses(&warm_metrics),
    })
}

async fn bench_cold_rows(
    env: &BenchEnv,
    run_config: &RunConfig,
    hop: u8,
    query: &str,
) -> BenchResult<ColdBenchResult> {
    let mut open_query_latencies = Vec::with_capacity(run_config.cold_iters as usize);
    let mut query_latencies = Vec::with_capacity(run_config.cold_iters as usize);
    let mut rows = None;
    let mut cache_hydrations = 0_u64;
    let mut peak_rss_bytes = current_rss_bytes();

    for sample in 0..run_config.cold_iters {
        let cache_dir = env.cache_root.join(format!(
            "fanout-{}-hop-{hop}-rows-cold-{sample}",
            env.fanout
        ));
        reset_dir(&cache_dir)?;

        let open_started = Instant::now();
        let reader = GraphShard::open_standalone_writer_with_memory_options(
            env.shard_path.clone(),
            Arc::clone(&env.object_store),
            graph_options(
                Some(&cache_dir),
                env.cache_bytes,
                env.fanout,
                env.max_hop,
                env.index_policy,
            ),
            graph_memory_config(),
        )
        .await?;

        let query_started = Instant::now();
        let result = reader
            .execute_cypher_rows(
                QueryContext::new(CELL_ID, format!("cold-rows-{}-{hop}-{sample}", env.fanout)),
                query,
            )
            .await?;
        query_latencies.push(query_started.elapsed());
        open_query_latencies.push(open_started.elapsed());

        if let Some(expected) = rows {
            assert_eq!(expected, result.rows.len());
        } else {
            rows = Some(result.rows.len());
        }

        let metrics = reader.graph_cache_metrics();
        cache_hydrations = cache_hydrations.saturating_add(metrics.hydration_started);
        peak_rss_bytes = peak_rss_bytes.max(current_rss_bytes());
        reader.close().await?;
    }

    Ok(ColdBenchResult {
        samples: u64::from(run_config.cold_iters),
        open_query: LatencyStats::from_durations(&open_query_latencies),
        query: LatencyStats::from_durations(&query_latencies),
        rows: rows.unwrap_or_default(),
        has_next: false,
        cache_hydrations,
        peak_rss_bytes,
    })
}

async fn bench_cold_page(
    env: &BenchEnv,
    run_config: &RunConfig,
    workload_name: &str,
    query: &str,
) -> BenchResult<ColdBenchResult> {
    let mut open_query_latencies = Vec::with_capacity(run_config.cold_iters as usize);
    let mut query_latencies = Vec::with_capacity(run_config.cold_iters as usize);
    let mut rows = None;
    let mut has_next = None;
    let mut cache_hydrations = 0_u64;
    let mut peak_rss_bytes = current_rss_bytes();

    for sample in 0..run_config.cold_iters {
        let cache_dir = env.cache_root.join(format!(
            "fanout-{}-{workload_name}-cold-{sample}",
            env.fanout
        ));
        reset_dir(&cache_dir)?;

        let open_started = Instant::now();
        let reader = GraphShard::open_standalone_writer_with_memory_options(
            env.shard_path.clone(),
            Arc::clone(&env.object_store),
            graph_options(
                Some(&cache_dir),
                env.cache_bytes,
                env.fanout,
                env.max_hop,
                env.index_policy,
            ),
            graph_memory_config(),
        )
        .await?;

        let query_started = Instant::now();
        let page = reader
            .execute_cypher_rows_page(
                QueryContext::new(
                    CELL_ID,
                    format!("cold-page-{}-{workload_name}-{sample}", env.fanout),
                ),
                query,
                None,
                run_config.page_size,
            )
            .await?;
        query_latencies.push(query_started.elapsed());
        open_query_latencies.push(open_started.elapsed());

        if let Some(expected) = rows {
            assert_eq!(expected, page.rows.len());
        } else {
            rows = Some(page.rows.len());
        }
        if let Some(expected) = has_next {
            assert_eq!(expected, page.next_cursor.is_some());
        } else {
            has_next = Some(page.next_cursor.is_some());
        }

        let metrics = reader.graph_cache_metrics();
        cache_hydrations = cache_hydrations.saturating_add(metrics.hydration_started);
        peak_rss_bytes = peak_rss_bytes.max(current_rss_bytes());
        reader.close().await?;
    }

    Ok(ColdBenchResult {
        samples: u64::from(run_config.cold_iters),
        open_query: LatencyStats::from_durations(&open_query_latencies),
        query: LatencyStats::from_durations(&query_latencies),
        rows: rows.unwrap_or_default(),
        has_next: has_next.unwrap_or(false),
        cache_hydrations,
        peak_rss_bytes,
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
    let peak_rss_bytes = current_rss_bytes();
    Ok(ConcurrentStats {
        stats: LatencyStats::from_durations(&latencies),
        qps: (latencies.len() as f64) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        queries: latencies.len() as u64,
        rows,
        peak_rss_bytes,
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
    let peak_rss_bytes = current_rss_bytes();
    Ok(ConcurrentStats {
        stats: LatencyStats::from_durations(&latencies),
        qps: (latencies.len() as f64) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        queries: latencies.len() as u64,
        rows,
        peak_rss_bytes,
    })
}

fn graph_options(
    cache_dir: Option<&Path>,
    cache_bytes: usize,
    fanout: u64,
    max_hop: u8,
    index_policy: GraphIndexPolicy,
) -> GraphOpenOptions {
    let edges = layered_edge_count(fanout, max_hop);
    let query_rows = edges.saturating_add(fanout).saturating_add(1_024);
    let max_matrix_adjacencies = env_usize("GRAPH_QUERY_BENCH_MAX_MATRIX_ADJACENCIES", 0);
    let max_graphblas_matrices = env_usize("GRAPH_QUERY_BENCH_MAX_GRAPHBLAS_MATRICES", 1);
    {
        let mut options = GraphOpenOptions::default();
        options.limits = GraphLimits {
            max_bulk_import_edges: usize::try_from(edges).unwrap_or(usize::MAX).max(1),
            max_artifact_source_epochs: u64::MAX,
            max_traversal_hops: max_hop,
            max_artifact_build_edges: edges.saturating_add(1),
            max_query_result_vertices: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_intermediate_rows: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_index_candidates: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_scan_edges: edges.saturating_mul(u64::from(max_hop).max(1)).max(1),
            max_query_runtime_ms: Some(env_u64("GRAPH_QUERY_BENCH_QUERY_TIMEOUT_MS", 120_000)),
            ..GraphLimits::default()
        };
        options.cache = cache_dir
            .filter(|_| cache_bytes > 0)
            .map(|path| GraphCacheConfig::disk_cache_without_preload(path, cache_bytes))
            .unwrap_or_else(GraphCacheConfig::disabled);
        options.cache_policy = {
            let mut cache_policy = GraphCachePolicy::default();
            cache_policy.max_matrix_adjacencies = max_matrix_adjacencies;
            cache_policy.max_graphblas_matrices = max_graphblas_matrices;
            cache_policy.max_entries_per_cell = None;
            cache_policy.pin_matrix_min_edges = 50_000;
            cache_policy.max_concurrent_hydrations = 32;
            cache_policy
        };
        options.index_policy = index_policy;
        options
    }
}

fn graph_memory_config() -> GraphMemoryConfig {
    GraphMemoryConfig {
        storage: GraphStorageMemoryConfig {
            l0_sst_size_bytes: env_usize(
                "GRAPH_QUERY_BENCH_L0_SST_BYTES",
                GraphStorageMemoryConfig::default().l0_sst_size_bytes,
            ),
            max_unflushed_bytes: env_usize(
                "GRAPH_QUERY_BENCH_MAX_UNFLUSHED_BYTES",
                GraphStorageMemoryConfig::default().max_unflushed_bytes,
            ),
            ..GraphStorageMemoryConfig::default()
        },
        ..GraphMemoryConfig::default()
    }
}

fn print_result(record: PrintRecord<'_>) -> BenchResult<()> {
    let result = record.result;
    println!(
        "{},{},{},{},{},{},{},{},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.2},{:.3},{},{},{:.3},{:.3},{:.3},{:.3},{:.2},{:.3},{},{},{},{},{},{},{}",
        record.kind,
        record.object_backend,
        record.fanout,
        record.hops,
        record.edges,
        record.query_shape,
        record.page_size,
        millis(record.build_elapsed),
        bytes_to_mib(record.build_rss_bytes),
        result.cold_samples,
        result.cold_open_query.p50_us,
        result.cold_open_query.p95_us,
        result.cold_open_query.p99_us,
        result.cold_open_query.mean_us,
        result.cold_query.p50_us,
        result.cold_query.p95_us,
        result.cold_query.p99_us,
        result.cold_query.mean_us,
        bytes_to_mib(result.cold_peak_rss_bytes),
        result.warm_us,
        bytes_to_mib(result.warm_rss_bytes),
        result.hot.p50_us,
        result.hot.p95_us,
        result.hot.p99_us,
        result.hot.mean_us,
        result.hot_qps,
        bytes_to_mib(result.hot_peak_rss_bytes),
        record.concurrency,
        result.concurrent.queries,
        result.concurrent.stats.p50_us,
        result.concurrent.stats.p95_us,
        result.concurrent.stats.p99_us,
        result.concurrent.stats.mean_us,
        result.concurrent.qps,
        bytes_to_mib(result.concurrent.peak_rss_bytes),
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
}

fn cache_misses(metrics: &slatedb_graph_kernel::GraphCacheMetricsSnapshot) -> u64 {
    metrics.matrix_artifact_misses
        + metrics.matrix_adjacency_misses
        + metrics.graphblas_misses
        + metrics.parsed_row_query_misses
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
        let marker = path.join(CACHE_DIR_MARKER);
        let entries = fs::read_dir(path)?.collect::<std::result::Result<Vec<_>, _>>()?;
        let has_non_marker_entries = entries
            .iter()
            .any(|entry| entry.file_name() != CACHE_DIR_MARKER);
        if has_non_marker_entries && !marker.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "{} already exists and is not a marked benchmark cache directory",
                    path.display()
                ),
            )
            .into());
        }
        for entry in entries {
            if entry.file_name() == CACHE_DIR_MARKER {
                continue;
            }
            let entry_path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                fs::remove_dir_all(&entry_path)?;
            } else {
                fs::remove_file(&entry_path)?;
            }
        }
    } else {
        fs::create_dir_all(path)?;
    }
    fs::write(
        path.join(CACHE_DIR_MARKER),
        b"slatedb graph query benchmark cache\n",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_dir_refuses_unmarked_non_empty_directory() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("important.txt"), "keep me").unwrap();
        let err = reset_dir(temp.path()).unwrap_err();
        assert_eq!(
            err.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::AlreadyExists)
        );
        assert!(temp.path().join("important.txt").exists());
    }

    #[test]
    fn reset_dir_cleans_marked_cache_directory() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join(CACHE_DIR_MARKER), "marker").unwrap();
        fs::write(temp.path().join("cache.bin"), "stale").unwrap();
        reset_dir(temp.path()).unwrap();
        assert!(temp.path().join(CACHE_DIR_MARKER).exists());
        assert!(!temp.path().join("cache.bin").exists());
    }
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

fn string_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_index_policy(value: &str) -> BenchResult<GraphIndexPolicy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "full" => Ok(GraphIndexPolicy::Full),
        "outbound" | "outbound-only" | "outboundonly" => Ok(GraphIndexPolicy::OutboundOnly),
        other => Err(format!("unsupported GRAPH_QUERY_BENCH_INDEX_POLICY={other}").into()),
    }
}

fn parse_bench_mode(value: &str) -> BenchResult<BenchMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "full" => Ok(BenchMode::Full),
        "build" | "build-only" | "buildonly" => Ok(BenchMode::BuildOnly),
        "query" | "query-only" | "queryonly" | "read" | "read-only" | "readonly" => {
            Ok(BenchMode::QueryOnly)
        }
        other => Err(format!("unsupported GRAPH_QUERY_BENCH_MODE={other}").into()),
    }
}

fn parse_workload_filter(value: &str) -> BenchResult<WorkloadFilter> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("all") {
        return Ok(WorkloadFilter::all());
    }
    let mut filter = WorkloadFilter {
        one_hop_page: false,
        rows: false,
        count: false,
        page: false,
    };
    for item in value
        .split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
    {
        match item.to_ascii_lowercase().as_str() {
            "one-hop" | "one-hop-page" | "one_hop_page" => filter.one_hop_page = true,
            "rows" | "multi-hop-rows" | "multi_hop_rows" => filter.rows = true,
            "count" | "multi-hop-count" | "multi_hop_count" => filter.count = true,
            "page" | "multi-hop-page" | "multi_hop_page" => filter.page = true,
            other => {
                return Err(format!("unsupported GRAPH_QUERY_BENCH_WORKLOADS item={other}").into())
            }
        }
    }
    if !(filter.one_hop_page || filter.rows || filter.count || filter.page) {
        return Err("GRAPH_QUERY_BENCH_WORKLOADS selected no workloads".into());
    }
    Ok(filter)
}

fn micros(duration: Duration) -> u128 {
    duration.as_micros()
}

fn millis(duration: Duration) -> u128 {
    duration.as_millis()
}

fn log_stage_rss(fanout: u64, stage: &str) {
    eprintln!(
        "fanout={fanout} stage={stage} rss_mib={:.1}",
        bytes_to_mib(current_rss_bytes())
    );
}

fn current_rss_bytes() -> u64 {
    platform_current_rss_bytes().unwrap_or(0)
}

fn trim_process_memory_for_profile() {
    if !std::env::var("GRAPH_TRIM_MEMORY_AFTER_HYDRATION").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }) {
        return;
    }
    platform_trim_process_memory();
}

fn bytes_to_mib(bytes: u64) -> f64 {
    (bytes as f64) / (1024.0 * 1024.0)
}

#[cfg(target_os = "linux")]
fn platform_current_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        return kb.checked_mul(1024);
    }
    None
}

#[cfg(target_os = "linux")]
fn platform_trim_process_memory() {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    unsafe {
        let _ = malloc_trim(0);
    }
}

#[cfg(not(target_os = "linux"))]
fn platform_trim_process_memory() {}

#[cfg(windows)]
fn platform_current_rss_bytes() -> Option<u64> {
    use std::ffi::c_void;

    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
    }

    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut c_void,
            counters: *mut ProcessMemoryCounters,
            size: u32,
        ) -> i32;
    }

    let mut counters = ProcessMemoryCounters {
        cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
        page_fault_count: 0,
        peak_working_set_size: 0,
        working_set_size: 0,
        quota_peak_paged_pool_usage: 0,
        quota_paged_pool_usage: 0,
        quota_peak_non_paged_pool_usage: 0,
        quota_non_paged_pool_usage: 0,
        pagefile_usage: 0,
        peak_pagefile_usage: 0,
    };
    let ok = unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<ProcessMemoryCounters>() as u32,
        )
    };
    if ok == 0 {
        None
    } else {
        Some(counters.working_set_size as u64)
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
fn platform_current_rss_bytes() -> Option<u64> {
    None
}

#[derive(Default)]
struct QueryBenchResult {
    cold_samples: u64,
    cold_open_query: LatencyStats,
    cold_query: LatencyStats,
    cold_peak_rss_bytes: u64,
    warm_us: u128,
    warm_rss_bytes: u64,
    hot: LatencyStats,
    hot_qps: f64,
    hot_peak_rss_bytes: u64,
    concurrent: ConcurrentStats,
    rows: usize,
    has_next: bool,
    cold_cache_hydrations: u64,
    warm_cache_hits: u64,
    warm_cache_misses: u64,
}

#[derive(Default)]
struct ColdBenchResult {
    samples: u64,
    open_query: LatencyStats,
    query: LatencyStats,
    rows: usize,
    has_next: bool,
    cache_hydrations: u64,
    peak_rss_bytes: u64,
}

#[derive(Default)]
struct ConcurrentStats {
    stats: LatencyStats,
    qps: f64,
    queries: u64,
    rows: usize,
    peak_rss_bytes: u64,
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
        let keep = std::env::var("GRAPH_QUERY_BENCH_KEEP")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"));
        let (path, keep) = if let Ok(path) = std::env::var("GRAPH_QUERY_BENCH_ROOT") {
            (PathBuf::from(path), true)
        } else {
            (
                std::env::temp_dir().join(format!(
                    "query-bench-{}-{}",
                    std::process::id(),
                    current_millis()
                )),
                keep,
            )
        };
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
