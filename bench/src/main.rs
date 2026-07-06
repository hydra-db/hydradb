//! `turbolay-bench` CLI — LDBC-shaped synthetic benchmark harness for
//! turbolay (RFC 0017 Phase 3), pattern-matched from the NamiDB reference
//! harness (`graphdb-experiments/crates/namidb-bench`).
//!
//! Three subcommands:
//!
//! - `generate` — writes a synthetic LDBC-shaped dataset to a directory.
//! - `run` — loads the dataset (or regenerates it inline) into a turbolay
//!   `Writer`, bench-runs each selected query, prints `BenchOutput` JSON.
//! - `verify` — loads the dataset into an in-memory `Writer` and dumps every
//!   selected query's DISTINCT, un-truncated row set as JSON
//!   (`queries::execute_distinct`), for diffing against
//!   `bench/py/verify_diff.py` — which runs the same Cypher shapes
//!   (`bench/py/falkordb_runner.py::cypher_for`) against FalkorDB with
//!   `RETURN DISTINCT ...` and no `LIMIT`, sorts identically, and diffs row
//!   sets directly. This *is* a row-for-row comparison, not just an
//!   inspectable dump — see `queries.rs`'s module doc on why `RETURN
//!   DISTINCT` (rather than raw per-path Cypher rows) is the correct
//!   apples-to-apples comparison against turbolay's per-hop node dedup.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use common::storage::config::{AwsObjectStoreConfig, LocalObjectStoreConfig, SlateDbStorageConfig};
use common::{ObjectStoreConfig, StorageConfig};
use turbolay::write::Writer;

mod dataset;
mod loader;
mod queries;
mod runner;

use dataset::{DatasetConfig, DatasetSizes};
use queries::{Query, Schema};
use runner::{BenchOutput, QueryResult, SizesReport};

#[derive(Debug, Parser)]
#[command(
    version,
    author,
    about = "LDBC-shaped synthetic bench for turbolay (RFC 0017 Phase 3)."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

/// Storage backend to load/query against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Backend {
    /// `object_store`'s in-memory backend — pure engine cost, no I/O.
    Memory,
    /// SlateDB over a local-filesystem object store (`--local-path`).
    Local,
    /// SlateDB over an S3-compatible object store (`--region`/`--bucket`, or
    /// `AWS_REGION`/`AWS_BUCKET`).
    #[value(name = "s3")]
    S3,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Generate the synthetic CSV dataset to a directory.
    Generate {
        #[arg(short, long, default_value = "0.1")]
        scale: f64,
        #[arg(short = 'S', long, default_value_t = 42)]
        seed: u64,
        #[arg(short, long)]
        out: PathBuf,
        /// Number of hub persons to inject (indices 0..N get extra KNOWS edges).
        #[arg(long, default_value_t = 0)]
        hub_count: usize,
        /// Target total degree for each hub person (extra KNOWS edges added).
        #[arg(long, default_value_t = 0)]
        hub_degree: usize,
    },
    /// Load (or reuse) the dataset into the chosen backend, time each
    /// selected query × parameter, print `BenchOutput` JSON.
    Run {
        #[arg(short, long, default_value = "0.1")]
        scale: f64,
        #[arg(short = 'S', long, default_value_t = 42)]
        seed: u64,
        /// Skip generation; load from this directory (generates a fresh one
        /// there if it's empty).
        #[arg(long)]
        dataset_dir: Option<PathBuf>,
        /// Warm-run sample count (per query, per parameter).
        #[arg(long, default_value_t = 50)]
        warm_runs: usize,
        /// How many distinct Person ids to use as the anchor per query.
        #[arg(long, default_value_t = 3)]
        param_count: usize,
        /// Restrict to specific queries; omit = all six.
        #[arg(long, value_enum)]
        only: Vec<Query>,
        /// Storage backend. Default: in-memory (pure engine cost).
        #[arg(long, value_enum, default_value = "memory")]
        backend: Backend,
        /// Local-filesystem object store root, for `--backend local`.
        /// Defaults to a fresh temp dir.
        #[arg(long)]
        local_path: Option<PathBuf>,
        /// AWS region, for `--backend s3` (or `AWS_REGION` env).
        #[arg(long)]
        region: Option<String>,
        /// S3 bucket, for `--backend s3` (or `AWS_BUCKET` env).
        #[arg(long)]
        bucket: Option<String>,
        /// Use the N highest-degree Person nodes as params instead of
        /// evenly-spaced indices. Requires `--dataset-dir` (reads the edge
        /// CSVs for degree computation). Overrides `--param-count`.
        #[arg(long)]
        top_degree: Option<usize>,
        /// Inject hub persons during dataset generation (ignored when
        /// `--dataset-dir` already has data). See `generate --hub-count`.
        #[arg(long, default_value_t = 0)]
        hub_count: usize,
        /// Target total degree for each hub person. See `generate --hub-degree`.
        #[arg(long, default_value_t = 0)]
        hub_degree: usize,
        /// Group up to this many logical node/edge records into one
        /// `Writer::ingest_batch` physical commit. `0` or `1` = the legacy
        /// per-record path (one durable commit per record). Batched ingest
        /// is what makes real-S3-backed loads at LDBC scale-10-ish tractable
        /// (per-record would be one network round trip per element).
        #[arg(long, default_value_t = 1000)]
        batch_size: usize,
        /// KNOWS-prefix hop depth(s) to sweep. Repeatable
        /// (`--hops 1 --hops 2 ...`). Each selected query is run once per hop
        /// value: every query walks `hops` outgoing KNOWS hops from the anchor
        /// to build a person frontier, then applies its tail (messages /
        /// likers / replies). Omit = each query's natural depth
        /// (ic02=1/ic09=2/ic07=0/ic08=0).
        #[arg(long)]
        hops: Vec<usize>,
        /// Anchor on these Person *indices* (0-based, as emitted by the
        /// generator's `person_id`). Repeatable. Overrides `--param-count`
        /// and `--top-degree`. Use with the hub indices (`0..hub_count`) so
        /// the supernode-degree sweep actually anchors on the high-degree
        /// hubs.
        #[arg(long)]
        anchor_index: Vec<usize>,
    },
    /// Load the dataset into an in-memory `Writer` and dump every selected
    /// query's DISTINCT, un-truncated row set as JSON (`{query, param, rows:
    /// [[col, ...], ...]}`) — the correctness oracle. Diff this straight
    /// across against `bench/py/verify_diff.py`, which runs the same Cypher
    /// with `RETURN DISTINCT ...` and no `LIMIT` against FalkorDB. (For the
    /// timed, `LIMIT 20`-truncated row shape instead, see `Cmd::Run`.)
    Verify {
        #[arg(short, long, default_value = "0.1")]
        scale: f64,
        #[arg(short = 'S', long, default_value_t = 42)]
        seed: u64,
        #[arg(long)]
        dataset_dir: Option<PathBuf>,
        /// How many distinct Person ids to use as the anchor per query.
        #[arg(long, default_value_t = 1)]
        param_count: usize,
        /// Restrict to specific queries; omit = all six.
        #[arg(long, value_enum)]
        only: Vec<Query>,
        /// See `Run`'s `--batch-size`.
        #[arg(long, default_value_t = 1000)]
        batch_size: usize,
        /// KNOWS-prefix hop depth(s) to dump. Repeatable. Omit = each query's
        /// natural depth. See `Run`'s `--hops`.
        #[arg(long)]
        hops: Vec<usize>,
        /// Anchor on these Person indices. Repeatable. Overrides
        /// `--param-count`. See `Run`'s `--anchor-index`.
        #[arg(long)]
        anchor_index: Vec<usize>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Generate {
            scale,
            seed,
            out,
            hub_count,
            hub_degree,
        } => {
            let sizes = dataset::generate(
                &out,
                &DatasetConfig {
                    scale,
                    seed,
                    hub_count,
                    hub_degree,
                },
            )?;
            eprintln!(
                "generated dataset @ scale={scale} seed={seed} hub_count={hub_count} \
                 hub_degree={hub_degree} into {}: {sizes:?}",
                out.display()
            );
        }
        Cmd::Run {
            scale,
            seed,
            dataset_dir,
            warm_runs,
            param_count,
            only,
            backend,
            local_path,
            region,
            bucket,
            top_degree,
            hub_count,
            hub_degree,
            batch_size,
            hops,
            anchor_index,
        } => {
            let (dataset_dir, sizes) =
                resolve_dataset(dataset_dir, scale, seed, hub_count, hub_degree)?;

            let backend_label_str =
                backend_label(backend, local_path.as_deref(), bucket.as_deref());
            let mut writer = open_writer(backend, local_path, region, bucket).await?;
            loader::load_into_writer(&mut writer, &dataset_dir, batch_size).await?;
            let schema = Schema::resolve(&writer)?;

            let params: Vec<String> = if !anchor_index.is_empty() {
                // Explicit anchor indices win — the supernode sweep anchors on
                // the hub persons (0..hub_count) so degree bites the traversal.
                anchor_index.iter().map(|i| make_person_id_hex(*i)).collect()
            } else if let Some(top_n) = top_degree {
                let deg_list = dataset::person_degrees_from_csvs(&dataset_dir)
                    .context("compute person degrees for --top-degree")?;
                deg_list.into_iter().take(top_n).map(|(id, _)| id).collect()
            } else {
                (0..param_count)
                    .map(|i| make_person_id_hex(i * (sizes.persons / param_count.max(1)).max(1)))
                    .collect()
            };

            let queries: Vec<Query> = if only.is_empty() {
                vec![
                    Query::Ic02,
                    Query::Ic07,
                    Query::Ic08,
                    Query::Ic09,
                    Query::Ic3h,
                    Query::Ic4h,
                ]
            } else {
                only
            };

            // Hop-sweep axis: each `Some(h)` runs every query at that KNOWS
            // prefix depth; empty = one pass at each query's natural depth.
            let hop_values: Vec<Option<usize>> = if hops.is_empty() {
                vec![None]
            } else {
                hops.iter().map(|h| Some(*h)).collect()
            };

            let mut results: Vec<QueryResult> = Vec::new();
            for q in &queries {
                for h in &hop_values {
                    for p in &params {
                        let r = runner::run_query(
                            &writer,
                            &schema,
                            &backend_label_str,
                            *q,
                            p,
                            warm_runs,
                            *h,
                        )
                        .await?;
                        eprintln!(
                            " {} hops={} param={} rows={} cold={}us warm_p50={}us",
                            r.query,
                            r.hops,
                            &r.param[..8.min(r.param.len())],
                            r.rows,
                            r.cold_us,
                            r.warm_p50_us
                        );
                        results.push(r);
                    }
                }
            }

            let out = BenchOutput {
                scale,
                seed,
                backend: backend_label_str,
                hub_degree,
                dataset_sizes: SizesReport::from(&sizes),
                results,
            };
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
        Cmd::Verify {
            scale,
            seed,
            dataset_dir,
            param_count,
            only,
            batch_size,
            hops,
            anchor_index,
        } => {
            // Hub params must match the Run sweep: reuse the dataset dir's hub
            // generation via resolve_dataset (hub_count/degree come from the dir
            // if it already exists, else default). We only need the CSVs here.
            let (dataset_dir, sizes) = resolve_dataset(dataset_dir, scale, seed, 0, 0)?;

            let mut writer = Writer::in_memory().await?;
            loader::load_into_writer(&mut writer, &dataset_dir, batch_size).await?;
            let schema = Schema::resolve(&writer)?;

            let params: Vec<String> = if !anchor_index.is_empty() {
                anchor_index.iter().map(|i| make_person_id_hex(*i)).collect()
            } else {
                (0..param_count)
                    .map(|i| make_person_id_hex(i * (sizes.persons / param_count.max(1)).max(1)))
                    .collect()
            };
            let queries: Vec<Query> = if only.is_empty() {
                vec![
                    Query::Ic02,
                    Query::Ic07,
                    Query::Ic08,
                    Query::Ic09,
                    Query::Ic3h,
                    Query::Ic4h,
                ]
            } else {
                only
            };
            let hop_values: Vec<Option<usize>> = if hops.is_empty() {
                vec![None]
            } else {
                hops.iter().map(|h| Some(*h)).collect()
            };

            let mut dumps = Vec::new();
            for q in &queries {
                for h in &hop_values {
                    for p in &params {
                        let rows =
                            queries::execute_distinct_with_hops(&writer, &schema, *q, p, *h).await?;
                        let effective_hops = h.unwrap_or(0);
                        dumps.push(serde_json::json!({
                            "query": q.name(),
                            "hops": effective_hops,
                            "param": p,
                            "rows": rows,
                        }));
                    }
                }
            }
            println!("{}", serde_json::to_string_pretty(&dumps)?);
        }
    }
    Ok(())
}

/// Resolves or generates the dataset directory, mirroring the NamiDB
/// reference harness's protocol: reuse `dataset_dir` if it already has
/// `persons.csv`, otherwise generate fresh (into a temp dir if
/// `dataset_dir` was never given).
fn resolve_dataset(
    dataset_dir: Option<PathBuf>,
    scale: f64,
    seed: u64,
    hub_count: usize,
    hub_degree: usize,
) -> Result<(PathBuf, DatasetSizes)> {
    let dir = dataset_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("turbolay-bench-{}", uuid::Uuid::now_v7().simple()))
    });
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let sizes = if dir.join("persons.csv").exists() {
        DatasetSizes::from_scale(scale)
    } else {
        dataset::generate(
            &dir,
            &DatasetConfig {
                scale,
                seed,
                hub_count,
                hub_degree,
            },
        )?
    };
    Ok((dir, sizes))
}

async fn open_writer(
    backend: Backend,
    local_path: Option<PathBuf>,
    region: Option<String>,
    bucket: Option<String>,
) -> Result<Writer> {
    match backend {
        Backend::Memory => Ok(Writer::in_memory().await?),
        Backend::Local => {
            let path = local_path.unwrap_or_else(|| {
                std::env::temp_dir().join(format!(
                    "turbolay-bench-local-{}",
                    uuid::Uuid::now_v7().simple()
                ))
            });
            std::fs::create_dir_all(&path)
                .with_context(|| format!("create local object-store dir {}", path.display()))?;
            let config = StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "turbolay-bench".to_string(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: path.to_string_lossy().into_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            });
            Ok(Writer::open(&config).await?)
        }
        Backend::S3 => {
            let region = region
                .or_else(|| std::env::var("AWS_REGION").ok())
                .context("--region or AWS_REGION is required for --backend s3")?;
            let bucket = bucket
                .or_else(|| std::env::var("AWS_BUCKET").ok())
                .context("--bucket or AWS_BUCKET is required for --backend s3")?;
            let config = StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "turbolay-bench".to_string(),
                object_store: ObjectStoreConfig::Aws(AwsObjectStoreConfig { region, bucket }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            });
            Ok(Writer::open(&config).await?)
        }
    }
}

fn backend_label(backend: Backend, local_path: Option<&Path>, bucket: Option<&str>) -> String {
    match backend {
        Backend::Memory => "memory://bench".to_string(),
        Backend::Local => format!(
            "local://{}",
            local_path
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<temp>".to_string())
        ),
        Backend::S3 => format!("s3://{}", bucket.unwrap_or("<env>")),
    }
}

/// Builds a stable 32-hex-char Person id from an index — the exact scheme
/// `dataset.rs::person_id`/`encode_id` uses (prefix byte `b'P'` + big-endian
/// `u128` index in the remaining 15 bytes), so `--param-count`/`--top-degree`
/// params always resolve to real dataset rows. Copied verbatim from the
/// NamiDB reference harness's own `make_person_id_hex`.
fn make_person_id_hex(i: usize) -> String {
    let mut bytes = [0u8; 16];
    bytes[0] = b'P';
    let i_bytes = (i as u128).to_be_bytes();
    bytes[1..].copy_from_slice(&i_bytes[1..]);
    let mut s = String::with_capacity(32);
    for b in bytes {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{:02x}", b));
    }
    s
}
