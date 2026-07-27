use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    object_store_from_env, GraphCacheConfig, GraphError, GraphLimits, GraphOpenOptions, GraphShard,
    QueryContext, Result,
};

const CACHE_DIR_MARKER: &str = ".slatedb-graph-query-bench-cache";

#[tokio::main]
async fn main() -> Result<()> {
    let config = BenchConfig::from_args(std::env::args().skip(1).collect())?;
    let object_store = object_store_from_env(config.env_file.clone())?;
    refresh_requested_stats(&config, Arc::clone(&object_store)).await?;
    println!(
        "mode,cache,iterations,rows,open_p50_us,open_p95_us,query_p50_us,query_p95_us,query_p99_us,query_mean_us,qps"
    );

    if config.cold_iters > 0 {
        let cold = bench_cold_no_cache(&config, Arc::clone(&object_store)).await?;
        print_record("cold", "none", &cold);
    }

    if config.warm_iters > 0 {
        let Some(cache_dir) = &config.cache_dir else {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorQueryBench",
                feature: "--warm-iters requires --cache-dir".to_string(),
            });
        };
        reset_dir(cache_dir)?;
        let _warmup = bench_single_open_query(
            &config,
            Arc::clone(&object_store),
            graph_options(Some(cache_dir), config.cache_bytes, config.query_timeout_ms),
            "warm-disk-seed",
        )
        .await?;
        let warm = bench_warm_disk(&config, Arc::clone(&object_store), cache_dir).await?;
        print_record("warm", "disk", &warm);
    }

    if config.hot_iters > 0 {
        let hot = bench_hot_memory(&config, object_store).await?;
        print_record(
            "hot",
            if config.cache_dir.is_some() {
                "disk+memory"
            } else {
                "memory"
            },
            &hot,
        );
    }
    Ok(())
}

async fn bench_cold_no_cache(
    config: &BenchConfig,
    object_store: Arc<dyn ObjectStore>,
) -> Result<BenchRecord> {
    let mut record = BenchRecord::default();
    for iter in 0..config.cold_iters {
        let sample = bench_single_open_query(
            config,
            Arc::clone(&object_store),
            graph_options(None, config.cache_bytes, config.query_timeout_ms),
            &format!("cold-{iter}"),
        )
        .await?;
        record.push(sample);
    }
    Ok(record)
}

async fn bench_warm_disk(
    config: &BenchConfig,
    object_store: Arc<dyn ObjectStore>,
    cache_dir: &str,
) -> Result<BenchRecord> {
    let mut record = BenchRecord::default();
    for iter in 0..config.warm_iters {
        let sample = bench_single_open_query(
            config,
            Arc::clone(&object_store),
            graph_options(Some(cache_dir), config.cache_bytes, config.query_timeout_ms),
            &format!("warm-disk-{iter}"),
        )
        .await?;
        record.push(sample);
    }
    Ok(record)
}

async fn bench_hot_memory(
    config: &BenchConfig,
    object_store: Arc<dyn ObjectStore>,
) -> Result<BenchRecord> {
    let options = graph_options(
        config.cache_dir.as_deref(),
        config.cache_bytes,
        config.query_timeout_ms,
    );
    let shard =
        GraphShard::open_with_options(config.db_path.clone(), object_store, options).await?;
    let mut record = BenchRecord::default();
    let _warmup = query_once(&shard, config, "hot-memory-seed").await?;
    for iter in 0..config.hot_iters {
        let query = query_once(&shard, config, &format!("hot-memory-{iter}")).await?;
        record.push(BenchSample {
            open_elapsed: Duration::ZERO,
            query_elapsed: query.elapsed,
            rows: query.rows,
        });
    }
    shard.close().await?;
    Ok(record)
}

async fn bench_single_open_query(
    config: &BenchConfig,
    object_store: Arc<dyn ObjectStore>,
    options: GraphOpenOptions,
    idempotency_key: &str,
) -> Result<BenchSample> {
    let open_started = Instant::now();
    let shard =
        GraphShard::open_with_options(config.db_path.clone(), object_store, options).await?;
    let open_elapsed = open_started.elapsed();
    let query = query_once(&shard, config, idempotency_key).await?;
    shard.close().await?;
    Ok(BenchSample {
        open_elapsed,
        query_elapsed: query.elapsed,
        rows: query.rows,
    })
}

async fn query_once(
    shard: &GraphShard,
    config: &BenchConfig,
    idempotency_key: &str,
) -> Result<QuerySample> {
    let started = Instant::now();
    let rows = shard
        .execute_cypher_rows(
            QueryContext::new(&config.cell_id, idempotency_key),
            &config.query,
        )
        .await?;
    Ok(QuerySample {
        elapsed: started.elapsed(),
        rows: rows.rows.len(),
    })
}

async fn refresh_requested_stats(
    config: &BenchConfig,
    object_store: Arc<dyn ObjectStore>,
) -> Result<()> {
    if config.refresh_edge_stats.is_empty() {
        return Ok(());
    }
    let shard = GraphShard::open_standalone_writer_with_options(
        config.db_path.clone(),
        object_store,
        graph_options(
            config.cache_dir.as_deref(),
            config.cache_bytes,
            config.query_timeout_ms,
        ),
    )
    .await?;
    for edge_type in &config.refresh_edge_stats {
        shard
            .refresh_edge_type_query_stats(&config.cell_id, edge_type)
            .await?;
    }
    shard.close().await
}

fn graph_options(
    cache_dir: Option<&str>,
    cache_bytes: usize,
    query_timeout_ms: Option<u64>,
) -> GraphOpenOptions {
    let cache = cache_dir
        .map(|cache_dir| GraphCacheConfig::disk_cache(cache_dir, cache_bytes))
        .unwrap_or_default();
    {
        let mut options = GraphOpenOptions::default();
        options.cache = cache;
        options.limits = GraphLimits {
            max_query_runtime_ms: query_timeout_ms,
            ..GraphLimits::default()
        };
        options
    }
}

fn reset_dir(path: &str) -> Result<()> {
    let path = Path::new(path);
    if path.exists() {
        if !path.is_dir() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorQueryBench",
                feature: format!("--cache-dir {} is not a directory", path.display()),
            });
        }
        let marker = path.join(CACHE_DIR_MARKER);
        let entries = fs::read_dir(path)
            .map_err(|err| GraphError::CorruptValue {
                key: path.display().to_string(),
                reason: format!("failed to inspect cache directory: {err}"),
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| GraphError::CorruptValue {
                key: path.display().to_string(),
                reason: format!("failed to inspect cache directory entry: {err}"),
            })?;
        let has_non_marker_entries = entries
            .iter()
            .any(|entry| entry.file_name() != CACHE_DIR_MARKER);
        if has_non_marker_entries && !marker.is_file() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorQueryBench",
                feature: format!(
                    "--cache-dir {} already exists and is not a benchmark cache; choose an empty directory or a directory created by this benchmark",
                    path.display()
                ),
            });
        }
        for entry in entries {
            if entry.file_name() == CACHE_DIR_MARKER {
                continue;
            }
            let entry_path = entry.path();
            let file_type = entry.file_type().map_err(|err| GraphError::CorruptValue {
                key: entry_path.display().to_string(),
                reason: format!("failed to inspect cache entry: {err}"),
            })?;
            if file_type.is_dir() {
                fs::remove_dir_all(&entry_path).map_err(|err| GraphError::CorruptValue {
                    key: entry_path.display().to_string(),
                    reason: format!("failed to clear cache directory entry: {err}"),
                })?;
            } else {
                fs::remove_file(&entry_path).map_err(|err| GraphError::CorruptValue {
                    key: entry_path.display().to_string(),
                    reason: format!("failed to clear cache file entry: {err}"),
                })?;
            }
        }
    } else {
        fs::create_dir_all(path).map_err(|err| GraphError::CorruptValue {
            key: path.display().to_string(),
            reason: format!("failed to create cache directory: {err}"),
        })?;
    }
    fs::write(
        path.join(CACHE_DIR_MARKER),
        b"slatedb graph query benchmark cache\n",
    )
    .map_err(|err| GraphError::CorruptValue {
        key: path.display().to_string(),
        reason: format!("failed to write cache directory marker: {err}"),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_dir_refuses_unmarked_non_empty_directory() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("important.txt"), "keep me").unwrap();
        let err = reset_dir(temp.path().to_str().unwrap()).unwrap_err();
        assert!(matches!(err, GraphError::UnsupportedQuery { .. }));
        assert!(temp.path().join("important.txt").exists());
    }

    #[test]
    fn reset_dir_cleans_marked_cache_directory() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join(CACHE_DIR_MARKER), "marker").unwrap();
        fs::write(temp.path().join("cache.bin"), "stale").unwrap();
        reset_dir(temp.path().to_str().unwrap()).unwrap();
        assert!(temp.path().join(CACHE_DIR_MARKER).exists());
        assert!(!temp.path().join("cache.bin").exists());
    }

    #[test]
    fn reset_dir_allows_empty_directory() {
        let temp = tempfile::tempdir().unwrap();
        reset_dir(temp.path().to_str().unwrap()).unwrap();
        assert!(temp.path().join(CACHE_DIR_MARKER).exists());
    }
}

#[derive(Clone, Debug)]
struct BenchConfig {
    env_file: Option<String>,
    db_path: String,
    cell_id: String,
    query: String,
    cold_iters: usize,
    warm_iters: usize,
    hot_iters: usize,
    cache_dir: Option<String>,
    cache_bytes: usize,
    query_timeout_ms: Option<u64>,
    refresh_edge_stats: Vec<String>,
}

impl BenchConfig {
    fn from_args(args: Vec<String>) -> Result<Self> {
        let mut parser = ArgParser::new(args);
        if parser.flag("--help") || parser.flag("-h") {
            print_usage();
            std::process::exit(0);
        }
        let config = Self {
            env_file: parser.optional("--env-file")?,
            db_path: parser.required("--db-path")?,
            cell_id: parser.required("--cell-id")?,
            query: parser.required("--query")?,
            cold_iters: parser.optional_usize("--cold-iters")?.unwrap_or(5),
            warm_iters: parser.optional_usize("--warm-iters")?.unwrap_or(5),
            hot_iters: parser.optional_usize("--hot-iters")?.unwrap_or(30),
            cache_dir: parser.optional("--cache-dir")?,
            cache_bytes: parser
                .optional_usize("--cache-bytes")?
                .unwrap_or(1024 * 1024 * 1024),
            query_timeout_ms: parser.optional_u64("--query-timeout-ms")?.or(Some(180_000)),
            refresh_edge_stats: parser
                .optional("--refresh-edge-stats")?
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToString::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        };
        parser.finish()?;
        Ok(config)
    }
}

struct ArgParser {
    args: Vec<String>,
}

impl ArgParser {
    fn new(args: Vec<String>) -> Self {
        Self { args }
    }

    fn required(&mut self, name: &str) -> Result<String> {
        self.optional(name)?
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "FalkorQueryBench",
                feature: format!("missing required argument {name}"),
            })
    }

    fn optional(&mut self, name: &str) -> Result<Option<String>> {
        let Some(idx) = self.args.iter().position(|arg| arg == name) else {
            return Ok(None);
        };
        self.args.remove(idx);
        if idx >= self.args.len() || self.args[idx].starts_with('-') {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorQueryBench",
                feature: format!("{name} requires a value"),
            });
        }
        let value = self.args.remove(idx);
        if value.trim().is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorQueryBench",
                feature: format!("{name} cannot be empty"),
            });
        }
        Ok(Some(value))
    }

    fn optional_usize(&mut self, name: &str) -> Result<Option<usize>> {
        self.optional(name)?
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|err| GraphError::UnsupportedQuery {
                        dialect: "FalkorQueryBench",
                        feature: format!("{name} must be a positive integer: {err}"),
                    })
            })
            .transpose()
    }

    fn optional_u64(&mut self, name: &str) -> Result<Option<u64>> {
        self.optional(name)?
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|err| GraphError::UnsupportedQuery {
                        dialect: "FalkorQueryBench",
                        feature: format!("{name} must be a positive integer: {err}"),
                    })
            })
            .transpose()
    }

    fn flag(&mut self, name: &str) -> bool {
        match self.args.iter().position(|arg| arg == name) {
            Some(idx) => {
                self.args.remove(idx);
                true
            }
            None => false,
        }
    }

    fn finish(self) -> Result<()> {
        if self.args.is_empty() {
            Ok(())
        } else {
            Err(GraphError::UnsupportedQuery {
                dialect: "FalkorQueryBench",
                feature: format!("unknown arguments: {}", self.args.join(" ")),
            })
        }
    }
}

#[derive(Clone, Debug)]
struct QuerySample {
    elapsed: Duration,
    rows: usize,
}

#[derive(Clone, Debug)]
struct BenchSample {
    open_elapsed: Duration,
    query_elapsed: Duration,
    rows: usize,
}

#[derive(Default, Debug)]
struct BenchRecord {
    open_elapsed: Vec<Duration>,
    query_elapsed: Vec<Duration>,
    rows: Option<usize>,
}

impl BenchRecord {
    fn push(&mut self, sample: BenchSample) {
        self.open_elapsed.push(sample.open_elapsed);
        self.query_elapsed.push(sample.query_elapsed);
        self.rows = Some(sample.rows);
    }
}

fn print_record(mode: &str, cache: &str, record: &BenchRecord) {
    let total_query_us = record
        .query_elapsed
        .iter()
        .map(|duration| duration.as_micros())
        .sum::<u128>();
    let qps = if total_query_us == 0 {
        0.0
    } else {
        record.query_elapsed.len() as f64 / (total_query_us as f64 / 1_000_000.0)
    };
    println!(
        "{mode},{cache},{},{},{},{},{},{},{},{},{qps:.2}",
        record.query_elapsed.len(),
        record.rows.unwrap_or(0),
        percentile_us(&record.open_elapsed, 0.50),
        percentile_us(&record.open_elapsed, 0.95),
        percentile_us(&record.query_elapsed, 0.50),
        percentile_us(&record.query_elapsed, 0.95),
        percentile_us(&record.query_elapsed, 0.99),
        mean_us(&record.query_elapsed),
    );
}

fn percentile_us(values: &[Duration], percentile: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut values = values
        .iter()
        .map(|duration| duration.as_micros())
        .collect::<Vec<_>>();
    values.sort_unstable();
    let idx = ((values.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn mean_us(values: &[Duration]) -> u128 {
    if values.is_empty() {
        return 0;
    }
    values
        .iter()
        .map(|duration| duration.as_micros())
        .sum::<u128>()
        / values.len() as u128
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --features opencypher --example falkor_query_bench -- \\
         --db-path <graph-db-path> --cell-id <cell> --query <cypher> \\
         [--cache-dir target/s3-cache] [--refresh-edge-stats RELATES,HAS_CHUNK] \\
         [--query-timeout-ms 180000] [--cold-iters 5] [--warm-iters 5] [--hot-iters 30]\n\
         Set any iteration count to 0 to skip that cache phase."
    );
}
