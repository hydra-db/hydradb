use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, EdgeIngestOptions, EdgeMutation, GraphCacheConfig,
    GraphDurabilityConfig, GraphIndexPolicy, GraphLimits, GraphOpenOptions,
    GraphOperationalMetricsSnapshot, GraphShard, DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
};

type ProfileResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";

#[tokio::main]
async fn main() -> ProfileResult<()> {
    let mode = string_env("GRAPH_WRITE_PROFILE_MODE", "ingest");
    let batch_size = env_usize("GRAPH_WRITE_PROFILE_BATCH_SIZE", 1_024);
    let batches = env_usize("GRAPH_WRITE_PROFILE_BATCHES", 128);
    let warmup_batches = env_usize_allow_zero("GRAPH_WRITE_PROFILE_WARMUP_BATCHES", 8);
    let seed_degree = env_u64("GRAPH_WRITE_PROFILE_SEED_DEGREE", 0);
    let seed_chunk = env_usize("GRAPH_WRITE_PROFILE_SEED_CHUNK", 50_000);
    let src = env_u64("GRAPH_WRITE_PROFILE_SRC", 1);
    let cache_bytes = env_usize("GRAPH_WRITE_PROFILE_DISK_CACHE_BYTES", 1024 * 1024 * 1024);
    let wal_flush_interval_ms = env_u64("GRAPH_WRITE_PROFILE_WAL_FLUSH_INTERVAL_MS", 1);
    let await_durable_writes = env_bool("GRAPH_WRITE_PROFILE_AWAIT_DURABLE", true);
    let trusted_chunk_size = env_usize(
        "GRAPH_WRITE_PROFILE_TRUSTED_CHUNK_SIZE",
        DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
    );
    let index_policy = parse_index_policy(&string_env("GRAPH_WRITE_PROFILE_INDEX_POLICY", "full"))?;
    let keep_data = env_bool("GRAPH_WRITE_PROFILE_KEEP_DATA", false);

    let root = TempProfileRoot::new()?;
    let cache_dir = root.path().join("slatedb-cache");
    fs::create_dir_all(&cache_dir)?;
    let (object_store, object_label) = object_store(&root)?;
    let total_mutations = (batch_size as u64)
        .saturating_mul((batches + warmup_batches) as u64)
        .saturating_add(seed_degree);
    let options = writer_options(
        &cache_dir,
        cache_bytes,
        total_mutations.max(batch_size as u64),
        wal_flush_interval_ms,
        await_durable_writes,
        index_policy,
    );
    let default_shard_path = format!("graph-write-profile-{}", std::process::id());
    let shard_path = string_env("GRAPH_WRITE_PROFILE_DB_PATH", &default_shard_path);
    let shard = GraphShard::open_standalone_writer_with_options(
        shard_path.clone(),
        Arc::clone(&object_store),
        options,
    )
    .await?;

    if seed_degree > 0 {
        let started = Instant::now();
        shard
            .bulk_import_edges_chunked(
                CELL_ID,
                EDGE_TYPE,
                (0..seed_degree).map(|index| (src, 1_000_000 + index)),
                "write-profile-seed",
                seed_chunk,
            )
            .await?;
        eprintln!(
            "graph write profile seed degree={} elapsed_ms={}",
            seed_degree,
            started.elapsed().as_millis()
        );
    }
    if mode == "delete" {
        let started = Instant::now();
        seed_delete_profile_edges(&shard, src, batch_size, warmup_batches, batches).await?;
        eprintln!(
            "graph write profile delete seed edges={} elapsed_ms={}",
            batch_size.saturating_mul(warmup_batches.saturating_add(batches)),
            started.elapsed().as_millis()
        );
    }

    eprintln!(
        "graph write profile mode={mode} object_store={object_label} db_path={shard_path} batch_size={batch_size} batches={batches} warmup_batches={warmup_batches} seed_degree={seed_degree} wal_flush_interval_ms={wal_flush_interval_ms} await_durable={await_durable_writes} index_policy={index_policy:?} trusted_chunk_size={trusted_chunk_size}"
    );
    let (stats, metrics_before, metrics_after) = if mode == "log-drain" {
        let metrics_before = shard.graph_operational_metrics();
        let stats = run_log_drain_profile(&shard, src, batch_size, warmup_batches, batches).await?;
        let metrics_after = shard.graph_operational_metrics();
        (stats, metrics_before, metrics_after)
    } else {
        run_warmup(
            &shard,
            &mode,
            src,
            batch_size,
            warmup_batches,
            trusted_chunk_size,
        )
        .await?;
        let metrics_before = shard.graph_operational_metrics();
        let stats = run_measured(
            &shard,
            &mode,
            src,
            batch_size,
            warmup_batches,
            batches,
            trusted_chunk_size,
        )
        .await?;
        let metrics_after = shard.graph_operational_metrics();
        (stats, metrics_before, metrics_after)
    };
    let breakdown =
        WriteProfileBreakdown::from_metrics(&metrics_before, &metrics_after, stats.total_edges);
    let epoch = shard.current_epoch(CELL_ID).await?;
    shard.close().await?;
    if keep_data {
        root.keep();
    }

    println!(
        "mode,object_store,wal_flush_interval_ms,await_durable,index_policy,batch_size,batches,trusted_chunk_size,total_edges,total_ms,edges_per_s,p50_us_per_edge,p95_us_per_edge,p99_us_per_edge,mean_us_per_edge,bulk_profiled_batches,bulk_preflight_us_per_edge,bulk_batch_build_us_per_edge,bulk_counter_read_us_per_edge,bulk_commit_us_per_edge,bulk_commit_pct,epoch"
    );
    println!(
        "{mode},{object_label},{wal_flush_interval_ms},{await_durable_writes},{index_policy:?},{batch_size},{batches},{trusted_chunk_size},{},{:.3},{:.2},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.1},{epoch}",
        stats.total_edges,
        millis_f64(stats.total_elapsed),
        stats.edges_per_s(),
        stats.latency_per_edge.p50_us,
        stats.latency_per_edge.p95_us,
        stats.latency_per_edge.p99_us,
        stats.latency_per_edge.mean_us,
        breakdown.profiled_batches,
        breakdown.preflight_us_per_edge,
        breakdown.batch_build_us_per_edge,
        breakdown.counter_read_us_per_edge,
        breakdown.commit_us_per_edge,
        breakdown.commit_pct,
    );
    Ok(())
}

async fn run_log_drain_profile(
    shard: &GraphShard,
    src: u64,
    batch_size: usize,
    warmup_batches: usize,
    batches: usize,
) -> ProfileResult<WriteProfileStats> {
    for batch in 0..warmup_batches {
        run_batch(
            shard,
            "log",
            src,
            batch_size,
            batch,
            "warmup",
            DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
        )
        .await?;
    }
    if warmup_batches > 0 {
        shard
            .materialize_edge_mutation_log(CELL_ID, warmup_batches)
            .await?;
    }

    let total_started = Instant::now();
    let mut total_edges = 0_u64;
    for offset in 0..batches {
        total_edges = total_edges.saturating_add(
            run_batch(
                shard,
                "log",
                src,
                batch_size,
                warmup_batches + offset,
                "measure",
                DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
            )
            .await?,
        );
    }
    let materialized = shard
        .materialize_edge_mutation_log(CELL_ID, batches)
        .await?;
    let elapsed = total_started.elapsed();
    let visible_edges = materialized.mutations.max(total_edges);
    Ok(WriteProfileStats {
        total_edges: visible_edges,
        total_elapsed: elapsed,
        latency_per_edge: LatencyStats::from_durations(&[duration_div(
            elapsed,
            visible_edges.max(1),
        )]),
    })
}

async fn run_warmup(
    shard: &GraphShard,
    mode: &str,
    src: u64,
    batch_size: usize,
    warmup_batches: usize,
    trusted_chunk_size: usize,
) -> ProfileResult<()> {
    for batch in 0..warmup_batches {
        run_batch(
            shard,
            mode,
            src,
            batch_size,
            batch,
            "warmup",
            trusted_chunk_size,
        )
        .await?;
    }
    Ok(())
}

async fn seed_delete_profile_edges(
    shard: &GraphShard,
    src: u64,
    batch_size: usize,
    warmup_batches: usize,
    batches: usize,
) -> ProfileResult<()> {
    for batch in 0..warmup_batches {
        let base = write_base("warmup", batch);
        shard
            .bulk_import_edges(
                CELL_ID,
                EDGE_TYPE,
                (0..batch_size).map(|index| (src, base + index as u64)),
                &format!("write-profile-delete-seed-warmup-{batch}"),
            )
            .await?;
    }
    for batch in 0..batches {
        let base = write_base("measure", warmup_batches + batch);
        shard
            .bulk_import_edges(
                CELL_ID,
                EDGE_TYPE,
                (0..batch_size).map(|index| (src, base + index as u64)),
                &format!("write-profile-delete-seed-measure-{batch}"),
            )
            .await?;
    }
    Ok(())
}

async fn run_measured(
    shard: &GraphShard,
    mode: &str,
    src: u64,
    batch_size: usize,
    first_batch: usize,
    batches: usize,
    trusted_chunk_size: usize,
) -> ProfileResult<WriteProfileStats> {
    let mut per_edge = Vec::with_capacity(batches);
    let total_started = Instant::now();
    let mut total_edges = 0_u64;
    for offset in 0..batches {
        let started = Instant::now();
        let inserted = run_batch(
            shard,
            mode,
            src,
            batch_size,
            first_batch + offset,
            "measure",
            trusted_chunk_size,
        )
        .await?;
        let elapsed = started.elapsed();
        total_edges = total_edges.saturating_add(inserted);
        per_edge.push(duration_div(elapsed, inserted.max(1)));
    }
    Ok(WriteProfileStats {
        total_edges,
        total_elapsed: total_started.elapsed(),
        latency_per_edge: LatencyStats::from_durations(&per_edge),
    })
}

async fn run_batch(
    shard: &GraphShard,
    mode: &str,
    src: u64,
    batch_size: usize,
    batch: usize,
    phase: &str,
    trusted_chunk_size: usize,
) -> ProfileResult<u64> {
    let base = write_base(phase, batch);
    match mode {
        "strict" => {
            for index in 0..batch_size {
                let dst = base + index as u64;
                shard
                    .write_edge(EdgeMutation {
                        cell_id: CELL_ID.to_string(),
                        edge_type: EDGE_TYPE.to_string(),
                        src,
                        dst,
                        idempotency_key: format!("write-profile-strict-{phase}-{batch}-{index}"),
                    })
                    .await?;
            }
            Ok(batch_size as u64)
        }
        "delete" => {
            let mut deleted = 0_u64;
            for index in 0..batch_size {
                let dst = base + index as u64;
                let result = shard
                    .delete_edge(EdgeMutation {
                        cell_id: CELL_ID.to_string(),
                        edge_type: EDGE_TYPE.to_string(),
                        src,
                        dst,
                        idempotency_key: format!("write-profile-delete-{phase}-{batch}-{index}"),
                    })
                    .await?;
                if result.deleted {
                    deleted = deleted.saturating_add(1);
                }
            }
            Ok(deleted)
        }
        "bulk" => {
            let result = shard
                .bulk_import_edges(
                    CELL_ID,
                    EDGE_TYPE,
                    (0..batch_size).map(|index| (src, base + index as u64)),
                    &format!("write-profile-bulk-{phase}-{batch}"),
                )
                .await?;
            Ok(result.inserted)
        }
        "bulk-trusted" => {
            let result = shard
                .bulk_append_edges_trusted_bounded(
                    CELL_ID,
                    EDGE_TYPE,
                    (0..batch_size).map(|index| (src, base + index as u64)),
                    &format!("write-profile-bulk-trusted-{phase}-{batch}"),
                    trusted_chunk_size,
                )
                .await?;
            Ok(result.inserted)
        }
        "segment-trusted" => {
            let result = shard
                .bulk_append_supernode_segment_trusted(
                    CELL_ID,
                    EDGE_TYPE,
                    src,
                    (0..batch_size).map(|index| base + index as u64),
                    &format!("write-profile-segment-trusted-{phase}-{batch}"),
                )
                .await?;
            Ok(result.inserted)
        }
        "ingest" => {
            let result = shard
                .ingest_edge_mutations(
                    CELL_ID,
                    (0..batch_size).map(|index| {
                        let dst = base + index as u64;
                        EdgeMutation {
                            cell_id: CELL_ID.to_string(),
                            edge_type: EDGE_TYPE.to_string(),
                            src,
                            dst,
                            idempotency_key: format!(
                                "write-profile-ingest-{phase}-{batch}-{index}"
                            ),
                        }
                    }),
                    EdgeIngestOptions { batch_size },
                )
                .await?;
            Ok(result.inserted)
        }
        "log" => {
            let result = shard
                .append_edge_mutation_log(
                    CELL_ID,
                    &format!("write-profile-log-{phase}-{batch}"),
                    (0..batch_size).map(|index| {
                        let dst = base + index as u64;
                        EdgeMutation {
                            cell_id: CELL_ID.to_string(),
                            edge_type: EDGE_TYPE.to_string(),
                            src,
                            dst,
                            idempotency_key: format!("write-profile-log-{phase}-{batch}-{index}"),
                        }
                    }),
                )
                .await?;
            Ok(result.mutations)
        }
        "log-materialize" => {
            let result = shard
                .append_edge_mutation_log(
                    CELL_ID,
                    &format!("write-profile-log-materialize-{phase}-{batch}"),
                    (0..batch_size).map(|index| {
                        let dst = base + index as u64;
                        EdgeMutation {
                            cell_id: CELL_ID.to_string(),
                            edge_type: EDGE_TYPE.to_string(),
                            src,
                            dst,
                            idempotency_key: format!(
                                "write-profile-log-materialize-{phase}-{batch}-{index}"
                            ),
                        }
                    }),
                )
                .await?;
            let materialized = shard.materialize_edge_mutation_log(CELL_ID, 1).await?;
            Ok(materialized.mutations.max(result.mutations))
        }
        other => Err(format!("unsupported GRAPH_WRITE_PROFILE_MODE={other}").into()),
    }
}

fn writer_options(
    cache_dir: &Path,
    cache_bytes: usize,
    max_edges: u64,
    wal_flush_interval_ms: u64,
    await_durable_writes: bool,
    index_policy: GraphIndexPolicy,
) -> GraphOpenOptions {
    GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: usize::try_from(max_edges).unwrap_or(usize::MAX),
            max_artifact_source_epochs: u64::MAX,
            max_artifact_build_edges: u64::MAX,
            ..Default::default()
        },
        cache: GraphCacheConfig::disk_cache_without_preload(cache_dir, cache_bytes),
        durability: GraphDurabilityConfig::low_latency_durable(wal_flush_interval_ms)
            .with_await_durable_writes(await_durable_writes),
        index_policy,
        ..Default::default()
    }
}

fn parse_index_policy(value: &str) -> ProfileResult<GraphIndexPolicy> {
    match value.to_ascii_lowercase().as_str() {
        "full" => Ok(GraphIndexPolicy::Full),
        "outbound" | "outbound-only" | "outboundonly" => Ok(GraphIndexPolicy::OutboundOnly),
        other => Err(format!("unsupported GRAPH_WRITE_PROFILE_INDEX_POLICY={other}").into()),
    }
}

fn object_store(root: &TempProfileRoot) -> ProfileResult<(Arc<dyn ObjectStore>, String)> {
    if let Ok(env_file) = std::env::var("GRAPH_WRITE_PROFILE_OBJECT_ENV") {
        return Ok((
            object_store_from_env(Some(env_file.clone()))?,
            format!("env:{env_file}"),
        ));
    }
    let object_root = root.path().join("object-store");
    fs::create_dir_all(&object_root)?;
    Ok((
        local_object_store(&object_root)?,
        format!("local:{}", object_root.display()),
    ))
}

fn write_base(phase: &str, batch: usize) -> u64 {
    let phase_offset = match phase {
        "warmup" => 7_000_000_000_u64,
        _ => 8_000_000_000_u64,
    };
    phase_offset + (batch as u64 * 10_000_000)
}

fn duration_div(duration: Duration, divisor: u64) -> Duration {
    Duration::from_nanos((duration.as_nanos() / u128::from(divisor)) as u64)
}

fn millis_f64(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_usize_allow_zero(name: &str, default: usize) -> usize {
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

fn string_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) if value.eq_ignore_ascii_case("1") || value.eq_ignore_ascii_case("true") => true,
        Ok(value) if value.eq_ignore_ascii_case("0") || value.eq_ignore_ascii_case("false") => {
            false
        }
        _ => default,
    }
}

struct WriteProfileStats {
    total_edges: u64,
    total_elapsed: Duration,
    latency_per_edge: LatencyStats,
}

impl WriteProfileStats {
    fn edges_per_s(&self) -> f64 {
        (self.total_edges as f64) / self.total_elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    }
}

#[derive(Default)]
struct WriteProfileBreakdown {
    profiled_batches: u64,
    preflight_us_per_edge: f64,
    batch_build_us_per_edge: f64,
    counter_read_us_per_edge: f64,
    commit_us_per_edge: f64,
    commit_pct: f64,
}

impl WriteProfileBreakdown {
    fn from_metrics(
        before: &GraphOperationalMetricsSnapshot,
        after: &GraphOperationalMetricsSnapshot,
        total_edges: u64,
    ) -> Self {
        let preflight_us = after
            .bulk_import_preflight_us
            .saturating_sub(before.bulk_import_preflight_us);
        let batch_build_us = after
            .bulk_import_batch_build_us
            .saturating_sub(before.bulk_import_batch_build_us);
        let counter_read_us = after
            .bulk_import_counter_read_us
            .saturating_sub(before.bulk_import_counter_read_us);
        let commit_us = after
            .bulk_import_commit_us
            .saturating_sub(before.bulk_import_commit_us);
        let total_profiled_us = preflight_us
            .saturating_add(batch_build_us)
            .saturating_add(counter_read_us)
            .saturating_add(commit_us);
        let edges = total_edges.max(1) as f64;
        Self {
            profiled_batches: after
                .bulk_import_batches_profiled
                .saturating_sub(before.bulk_import_batches_profiled),
            preflight_us_per_edge: preflight_us as f64 / edges,
            batch_build_us_per_edge: batch_build_us as f64 / edges,
            counter_read_us_per_edge: counter_read_us as f64 / edges,
            commit_us_per_edge: commit_us as f64 / edges,
            commit_pct: if total_profiled_us == 0 {
                0.0
            } else {
                commit_us as f64 * 100.0 / total_profiled_us as f64
            },
        }
    }
}

#[derive(Default)]
struct LatencyStats {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    mean_us: f64,
}

impl LatencyStats {
    fn from_durations(values: &[Duration]) -> Self {
        if values.is_empty() {
            return Self::default();
        }
        let mut micros: Vec<_> = values
            .iter()
            .map(|duration| duration.as_secs_f64() * 1_000_000.0)
            .collect();
        micros.sort_by(f64::total_cmp);
        let sum: f64 = micros.iter().sum();
        Self {
            p50_us: percentile(&micros, 50),
            p95_us: percentile(&micros, 95),
            p99_us: percentile(&micros, 99),
            mean_us: sum / micros.len() as f64,
        }
    }
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    let len = sorted.len();
    let rank = ((len - 1) as f64 * percentile as f64 / 100.0).ceil() as usize;
    sorted[rank.min(len - 1)]
}

struct TempProfileRoot {
    path: PathBuf,
    keep: bool,
}

impl TempProfileRoot {
    fn new() -> ProfileResult<Self> {
        let path = std::env::temp_dir().join(format!(
            "graph-write-profile-{}-{}",
            std::process::id(),
            chrono_like_millis()
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path, keep: false })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn keep(mut self) {
        self.keep = true;
    }
}

impl Drop for TempProfileRoot {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn chrono_like_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}
