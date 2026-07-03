use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, ArtifactDirection, EdgeMutation, GraphCacheConfig, GraphCachePolicy,
    GraphLimits, GraphOpenOptions, GraphShard, SparseKernelBackend,
};

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";
const WRITE_SAMPLE_EDGE_TYPE: &str = "USER_BLOCKED_USER";
const DEFAULT_FANOUTS: &[u64] = &[50, 100, 1_000, 10_000, 50_000, 100_000];
const DEFAULT_HOPS: &[u8] = &[1, 3, 5, 10, 12];

#[tokio::main]
async fn main() -> BenchResult<()> {
    let fanouts = parse_u64_list("PHASE0_BENCH_FANOUTS", DEFAULT_FANOUTS);
    let hops = parse_u8_list("PHASE0_BENCH_HOPS", DEFAULT_HOPS);
    let max_hop = env_u8(
        "PHASE0_BENCH_DATA_HOPS",
        hops.iter().copied().max().unwrap_or(12),
    );
    let hot_iters = env_u32("PHASE0_BENCH_HOT_ITERS", 7).max(1);
    let write_samples = env_u64("PHASE0_BENCH_WRITE_SAMPLES", 32);
    let matrix_kernel = selected_matrix_kernel();
    let tile_size = env_u64("PHASE0_BENCH_MATRIX_TILE", 4_096);
    let cache_bytes = env_usize("PHASE0_BENCH_DISK_CACHE_BYTES", 8 * 1024 * 1024 * 1024);
    let bulk_chunk_size = env_usize("PHASE0_BENCH_BULK_CHUNK_SIZE", 100_000);

    let bench_root = TempBenchRoot::new()?;
    let object_root = bench_root.path().join("object-store");
    let cache_root = bench_root.path().join("slatedb-cache");
    fs::create_dir_all(&object_root)?;
    fs::create_dir_all(&cache_root)?;
    let object_store = local_object_store(&object_root)?;
    let run_id = format!("phase0-path-bench-{}", std::process::id());

    eprintln!(
        "phase0 path benchmark: fanouts={fanouts:?} hops={hops:?} max_hop={max_hop} kernel={matrix_kernel:?} hot_iters={hot_iters} write_samples={write_samples} object_root={} cache_root={}",
        object_root.display(),
        cache_root.display()
    );
    println!(
        "kind,fanout,hops,edges,kernel,write_bulk_ms,write_edges_per_s,write_us_per_edge,write_sample_count,write_p50_us,write_p95_us,write_p99_us,build_ms,supernode_degree_cold_us,supernode_exists_cold_us,supernode_page_cold_us,cold_us,warm_us,hot_p50_us,hot_p95_us,hot_p99_us,hot_mean_us,hot_qps,hot_result_vertices_per_s,hot_edge_visits_per_s,result_vertices,edge_visits,delta_records,cold_cache_hydrations,warm_cache_hits,warm_cache_misses"
    );
    io::stdout().flush()?;

    for fanout in fanouts {
        let shard_path = format!("{run_id}/fanout-{fanout}");
        let writer_options = writer_options(fanout, max_hop, write_samples);
        let writer = GraphShard::open_standalone_writer_with_options(
            shard_path.clone(),
            Arc::clone(&object_store),
            writer_options,
        )
        .await?;

        let bulk_started = Instant::now();
        writer
            .bulk_import_edges_chunked(
                CELL_ID,
                EDGE_TYPE,
                layered_edges(fanout, max_hop),
                &format!("fanout-{fanout}"),
                bulk_chunk_size,
            )
            .await?;
        let bulk_elapsed = bulk_started.elapsed();
        eprintln!(
            "fanout={fanout} stage=bulk_import edges={} elapsed_ms={}",
            layered_edge_count(fanout, max_hop),
            millis(bulk_elapsed)
        );

        let write_latencies = sample_write_latencies(&writer, fanout, write_samples).await?;
        let write_stats = LatencyStats::from_durations(&write_latencies);
        eprintln!(
            "fanout={fanout} stage=sample_writes samples={} p50_us={}",
            write_latencies.len(),
            write_stats.p50_us
        );

        let base_epoch = writer.current_epoch(CELL_ID).await?;
        let build_started = Instant::now();
        let artifact = writer
            .build_matrix_tiles(CELL_ID, EDGE_TYPE, base_epoch, tile_size)
            .await?;
        let build_elapsed = build_started.elapsed();
        eprintln!(
            "fanout={fanout} stage=matrix_build edges={} elapsed_ms={}",
            artifact.edge_count,
            millis(build_elapsed)
        );
        let supernode_build_started = Instant::now();
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
        eprintln!(
            "fanout={fanout} stage=supernode_build elapsed_ms={}",
            millis(supernode_build_started.elapsed())
        );
        writer.close().await?;

        let supernode = bench_supernode(
            &shard_path,
            Arc::clone(&object_store),
            &cache_root,
            fanout,
            cache_bytes,
        )
        .await?;
        eprintln!(
            "fanout={fanout} stage=supernode_cold degree_us={} exists_us={} page_us={}",
            supernode.degree_us, supernode.exists_us, supernode.page_us
        );

        for &hop in &hops {
            let cache_dir = cache_root.join(format!("fanout-{fanout}-hop-{hop}"));
            reset_dir(&cache_dir)?;
            let read_options = reader_options(&cache_dir, cache_bytes, hop);

            let cold_reader = GraphShard::open_with_options(
                shard_path.clone(),
                Arc::clone(&object_store),
                read_options.clone(),
            )
            .await?;
            let cold_started = Instant::now();
            let cold = cold_reader
                .matrix_reachable_with_kernel(
                    CELL_ID,
                    EDGE_TYPE,
                    &[1],
                    hop,
                    base_epoch,
                    matrix_kernel,
                )
                .await?;
            let cold_elapsed = cold_started.elapsed();
            let cold_metrics = cold_reader.graph_cache_metrics();
            cold_reader.close().await?;
            eprintln!(
                "fanout={fanout} hop={hop} stage=cold_read elapsed_us={}",
                micros(cold_elapsed)
            );

            let warm_reader = GraphShard::open_with_options(
                shard_path.clone(),
                Arc::clone(&object_store),
                read_options,
            )
            .await?;
            let warm_started = Instant::now();
            let warm = warm_reader
                .matrix_reachable_with_kernel(
                    CELL_ID,
                    EDGE_TYPE,
                    &[1],
                    hop,
                    base_epoch,
                    matrix_kernel,
                )
                .await?;
            let warm_elapsed = warm_started.elapsed();
            assert_eq!(cold.vertices, warm.vertices);
            eprintln!(
                "fanout={fanout} hop={hop} stage=warm_read elapsed_us={}",
                micros(warm_elapsed)
            );

            let mut hot_latencies = Vec::with_capacity(hot_iters as usize);
            let hot_started = Instant::now();
            let mut hot_result = warm;
            for _ in 0..hot_iters {
                let started = Instant::now();
                hot_result = warm_reader
                    .matrix_reachable_with_kernel(
                        CELL_ID,
                        EDGE_TYPE,
                        &[1],
                        hop,
                        base_epoch,
                        matrix_kernel,
                    )
                    .await?;
                hot_latencies.push(started.elapsed());
            }
            let hot_total = hot_started.elapsed();
            assert_eq!(cold.vertices, hot_result.vertices);
            let hot_stats = LatencyStats::from_durations(&hot_latencies);
            let warm_metrics = warm_reader.graph_cache_metrics();
            warm_reader.close().await?;
            eprintln!(
                "fanout={fanout} hop={hop} stage=hot_read p50_us={} qps={:.2}",
                hot_stats.p50_us,
                f64::from(hot_iters) / hot_total.as_secs_f64().max(f64::MIN_POSITIVE)
            );

            let hot_qps = f64::from(hot_iters) / hot_total.as_secs_f64().max(f64::MIN_POSITIVE);
            let result_vertices = hot_result.vertices.len() as u64;
            let result_vertices_per_s = (result_vertices as f64) * hot_qps;
            let edge_visits_per_s = (hot_result.edge_visits as f64) * hot_qps;
            let write_bulk_ms = millis(bulk_elapsed);
            let write_edges_per_s =
                (artifact.edge_count as f64) / bulk_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
            let write_us_per_edge =
                (micros(bulk_elapsed) as f64) / (artifact.edge_count as f64).max(1.0);
            let build_ms = millis(build_elapsed);
            let cold_us = micros(cold_elapsed);
            let warm_us = micros(warm_elapsed);
            let cold_cache_hydrations = cold_metrics.hydration_started;
            let warm_cache_hits = warm_metrics.matrix_artifact_hits
                + warm_metrics.matrix_adjacency_hits
                + warm_metrics.graphblas_hits
                + warm_metrics.supernode_group_hits
                + warm_metrics.posting_chunk_hits;
            let warm_cache_misses = warm_metrics.matrix_artifact_misses
                + warm_metrics.matrix_adjacency_misses
                + warm_metrics.graphblas_misses
                + warm_metrics.supernode_group_misses
                + warm_metrics.posting_chunk_misses;
            let kernel = hot_result.sparse_kernel;
            let edges = artifact.edge_count;
            let write_sample_count = write_latencies.len();
            let write_p50_us = write_stats.p50_us;
            let write_p95_us = write_stats.p95_us;
            let write_p99_us = write_stats.p99_us;
            let hot_p50_us = hot_stats.p50_us;
            let hot_p95_us = hot_stats.p95_us;
            let hot_p99_us = hot_stats.p99_us;
            let hot_mean_us = hot_stats.mean_us;
            let edge_visits = hot_result.edge_visits;
            let delta_records = hot_result.delta_records_applied;

            println!(
                "traversal,{fanout},{hop},{edges},{kernel:?},{write_bulk_ms},{write_edges_per_s:.2},{write_us_per_edge:.2},{write_sample_count},{write_p50_us},{write_p95_us},{write_p99_us},{build_ms},{},{},{},{cold_us},{warm_us},{hot_p50_us},{hot_p95_us},{hot_p99_us},{hot_mean_us:.2},{hot_qps:.2},{result_vertices_per_s:.2},{edge_visits_per_s:.2},{result_vertices},{edge_visits},{delta_records},{cold_cache_hydrations},{warm_cache_hits},{warm_cache_misses}",
                supernode.degree_us,
                supernode.exists_us,
                supernode.page_us,
            );
            io::stdout().flush()?;
        }
    }

    Ok(())
}

async fn sample_write_latencies(
    shard: &GraphShard,
    fanout: u64,
    sample_count: u64,
) -> BenchResult<Vec<Duration>> {
    let mut latencies = Vec::with_capacity(sample_count as usize);
    for index in 0..sample_count {
        let started = Instant::now();
        shard
            .write_edge(EdgeMutation {
                cell_id: CELL_ID.to_string(),
                edge_type: WRITE_SAMPLE_EDGE_TYPE.to_string(),
                src: 9_000_000_000 + fanout,
                dst: 9_100_000_000 + index,
                idempotency_key: format!("sample-write-{fanout}-{index}"),
            })
            .await?;
        latencies.push(started.elapsed());
    }
    Ok(latencies)
}

async fn bench_supernode(
    shard_path: &str,
    object_store: Arc<dyn ObjectStore>,
    cache_root: &Path,
    fanout: u64,
    cache_bytes: usize,
) -> BenchResult<SupernodeBench> {
    let cache_dir = cache_root.join(format!("fanout-{fanout}-supernode"));
    reset_dir(&cache_dir)?;
    let started = Instant::now();
    let reader = GraphShard::open_with_options(
        shard_path.to_string(),
        object_store,
        reader_options(&cache_dir, cache_bytes, 1),
    )
    .await?;
    eprintln!(
        "fanout={fanout} stage=supernode_reader_open elapsed_ms={}",
        millis(started.elapsed())
    );
    let started = Instant::now();
    let read_epoch = reader.current_epoch(CELL_ID).await?;
    eprintln!(
        "fanout={fanout} stage=supernode_epoch elapsed_ms={}",
        millis(started.elapsed())
    );

    let started = Instant::now();
    let degree = reader
        .supernode_degree(CELL_ID, EDGE_TYPE, 1, read_epoch)
        .await?;
    let degree_elapsed = started.elapsed();
    eprintln!(
        "fanout={fanout} stage=supernode_degree elapsed_us={}",
        micros(degree_elapsed)
    );
    assert_eq!(degree, fanout);

    let started = Instant::now();
    let exists = reader
        .supernode_edge_exists(
            CELL_ID,
            EDGE_TYPE,
            1,
            layer_vertex(1, fanout - 1),
            read_epoch,
        )
        .await?;
    let exists_elapsed = started.elapsed();
    eprintln!(
        "fanout={fanout} stage=supernode_exists elapsed_us={}",
        micros(exists_elapsed)
    );
    assert!(exists);

    let started = Instant::now();
    let page = reader
        .supernode_page(CELL_ID, EDGE_TYPE, ArtifactDirection::Out, 1, read_epoch, 0)
        .await?
        .expect("root supernode page should exist");
    let page_elapsed = started.elapsed();
    eprintln!(
        "fanout={fanout} stage=supernode_page elapsed_us={}",
        micros(page_elapsed)
    );
    assert!(!page.vertices.is_empty());
    reader.close().await?;

    Ok(SupernodeBench {
        degree_us: micros(degree_elapsed),
        exists_us: micros(exists_elapsed),
        page_us: micros(page_elapsed),
    })
}

fn writer_options(fanout: u64, max_hop: u8, write_samples: u64) -> GraphOpenOptions {
    GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: layered_edge_count(fanout, max_hop)
                .saturating_add(write_samples)
                .saturating_add(1) as usize,
            max_artifact_source_epochs: u64::MAX,
            max_traversal_hops: max_hop,
        },
        cache_policy: GraphCachePolicy {
            max_matrix_adjacencies: 4,
            max_graphblas_matrices: 4,
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

fn reader_options(cache_dir: &Path, cache_bytes: usize, max_hop: u8) -> GraphOpenOptions {
    GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: 1,
            max_artifact_source_epochs: u64::MAX,
            max_traversal_hops: max_hop,
        },
        cache: GraphCacheConfig::disk_cache(cache_dir, cache_bytes),
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
    }
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

fn selected_matrix_kernel() -> SparseKernelBackend {
    match std::env::var("PHASE0_MATRIX_KERNEL") {
        Ok(value) if value.eq_ignore_ascii_case("graphblas") => {
            SparseKernelBackend::SuiteSparseGraphBlas
        }
        Ok(value) if value.eq_ignore_ascii_case("rust") => SparseKernelBackend::RustSparse,
        _ if cfg!(feature = "graphblas") => SparseKernelBackend::SuiteSparseGraphBlas,
        _ => SparseKernelBackend::RustSparse,
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

fn micros(duration: Duration) -> u128 {
    duration.as_micros()
}

fn millis(duration: Duration) -> u128 {
    duration.as_millis()
}

struct SupernodeBench {
    degree_us: u128,
    exists_us: u128,
    page_us: u128,
}

#[derive(Default)]
struct LatencyStats {
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    mean_us: f64,
}

impl LatencyStats {
    fn from_durations(durations: &[Duration]) -> Self {
        if durations.is_empty() {
            return Self::default();
        }
        let mut values: Vec<_> = durations
            .iter()
            .map(|duration| duration.as_micros())
            .collect();
        values.sort_unstable();
        let total: u128 = values.iter().sum();
        Self {
            p50_us: percentile(&values, 50),
            p95_us: percentile(&values, 95),
            p99_us: percentile(&values, 99),
            mean_us: (total as f64) / (values.len() as f64),
        }
    }
}

fn percentile(values: &[u128], percentile: u32) -> u128 {
    let len = values.len();
    let index = ((len.saturating_sub(1) as u128) * u128::from(percentile)).div_ceil(100) as usize;
    values[index.min(len - 1)]
}

struct TempBenchRoot {
    path: PathBuf,
    keep: bool,
}

impl TempBenchRoot {
    fn new() -> BenchResult<Self> {
        let keep = std::env::var("PHASE0_BENCH_KEEP")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"));
        let path = std::env::temp_dir().join(format!(
            "phase0-path-bench-{}-{}",
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
