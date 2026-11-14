//! Off/on overhead measurement for the ENG-3466 cache observer.
//!
//! Workload: a fixed dataset of `keys` keys (each `value_bytes` long) is
//! written and flushed once, then read through a `DbReader` backed by a
//! small `SharedSlateDbCache` (capacity_bytes << dataset) in two modes:
//! observer disabled and observer enabled with default bounds. Each mode
//! runs `rounds` times; per-op latencies are collected and summarized as
//! count, mean, p50, p95, p99, max, plus throughput. The cache is cold at
//! the start of each round (fresh cache + fresh reader), so every round
//! measures admission + eviction behavior from empty.
//!
//! Access pattern per op index i: 80% hot (keys 0..hot_keys, cycling),
//! 20% one-pass scan (keys hot_keys..keys, cycling). This mixes resident
//! reuse with unique-block pressure.
//!
//! Usage: cargo run --locked --release -p hydradb-graph-slate --example cache_observe_bench
//!   [--keys N] [--value-bytes N] [--cache-bytes N] [--ops N] [--rounds N] [--hot-keys N]

use std::sync::Arc;
use std::time::Instant;

use hydradb_graph_slate::{next_db_cache_id, ObserveConfig, SharedSlateDbCache, SharedSlateDbCacheConfig};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbReader, DbReaderMode};

struct Args {
    keys: usize,
    value_bytes: usize,
    cache_bytes: usize,
    ops: usize,
    rounds: usize,
    hot_keys: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        keys: 4096,
        value_bytes: 1024,
        cache_bytes: 512 * 1024,
        ops: 60_000,
        rounds: 5,
        hot_keys: 128,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it.next().expect("missing value for flag");
        let parsed: usize = value.parse().expect("invalid number");
        match flag.as_str() {
            "--keys" => args.keys = parsed,
            "--value-bytes" => args.value_bytes = parsed,
            "--cache-bytes" => args.cache_bytes = parsed,
            "--ops" => args.ops = parsed,
            "--rounds" => args.rounds = parsed,
            "--hot-keys" => args.hot_keys = parsed,
            other => panic!("unknown flag: {other}"),
        }
    }
    args
}

struct RoundStats {
    ops: usize,
    total_secs: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
}

fn percentiles(mut samples: Vec<std::time::Duration>) -> RoundStats {
    let ops = samples.len();
    samples.sort_unstable();
    let quantile = |q: f64| -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        let idx = ((ops as f64 - 1.0) * q).round() as usize;
        samples[idx].as_secs_f64() * 1e6
    };
    RoundStats {
        ops,
        total_secs: samples.iter().map(|d| d.as_secs_f64()).sum(),
        p50_us: quantile(0.5),
        p95_us: quantile(0.95),
        p99_us: quantile(0.99),
        max_us: samples.last().map_or(0.0, |d| d.as_secs_f64() * 1e6),
    }
}

async fn write_dataset(store: Arc<dyn ObjectStore>, args: &Args) {
    let db = Db::builder("cache-observe-bench", Arc::clone(&store))
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    for id in 0..args.keys {
        db.put(format!("key-{id:06}"), vec![7u8; args.value_bytes]).await.unwrap();
    }
    db.flush().await.unwrap();
    db.close().await.unwrap();
}

async fn run_round(store: Arc<dyn ObjectStore>, args: &Args, observe: bool) -> (RoundStats, String) {
    let cache = if observe {
        Arc::new(SharedSlateDbCache::with_config(SharedSlateDbCacheConfig::with_observe(
            args.cache_bytes,
            ObserveConfig::default(),
        )))
    } else {
        Arc::new(SharedSlateDbCache::with_config(SharedSlateDbCacheConfig::new(args.cache_bytes)))
    };
    let reader = DbReader::builder("cache-observe-bench", store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_db_cache(cache.clone(), next_db_cache_id())
        .build()
        .await
        .unwrap();
    let mut samples = Vec::with_capacity(args.ops);
    let mut scan_cursor = args.hot_keys;
    let started = Instant::now();
    for i in 0..args.ops {
        let id = if i % 5 != 4 {
            i % args.hot_keys
        } else {
            let id = scan_cursor;
            scan_cursor = args.hot_keys + (scan_cursor - args.hot_keys + 1) % (args.keys - args.hot_keys);
            id
        };
        let t0 = Instant::now();
        let value = reader.get(format!("key-{id:06}")).await.unwrap().unwrap();
        assert_eq!(value.len(), args.value_bytes);
        samples.push(t0.elapsed());
    }
    let wall = started.elapsed();
    reader.close().await.unwrap();
    drop(reader);
    let _ = wall;
    let snap = cache.snapshot();
    (percentiles(samples), format!("{:?}", snap.observe))
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    write_dataset(Arc::clone(&store), &args).await;

    println!(
        "workload keys={} value_bytes={} cache_bytes={} ops={} rounds={} hot_keys={}",
        args.keys, args.value_bytes, args.cache_bytes, args.ops, args.rounds, args.hot_keys
    );
    println!("dataset_bytes_approx={}", args.keys * args.value_bytes);

    for observe in [false, true] {
        let mode = if observe { "observe=on" } else { "observe=off" };
        let mut totals = Vec::new();
        let mut ops_per_sec = Vec::new();
        for round in 0..args.rounds {
            let (stats, observe_summary) = run_round(Arc::clone(&store), &args, observe).await;
            let ops_s = stats.ops as f64 / stats.total_secs;
            totals.push(stats.total_secs);
            ops_per_sec.push(ops_s);
            println!(
                "{mode} round={round} ops={} total_s={:.3} ops_s={:.0} p50_us={:.1} p95_us={:.1} p99_us={:.1} max_us={:.1}",
                stats.ops, stats.total_secs, ops_s, stats.p50_us, stats.p95_us, stats.p99_us, stats.max_us
            );
            if observe && round == 0 {
                println!("{mode} snapshot={observe_summary}");
            }
        }
        totals.sort_by(|a, b| a.total_cmp(b));
        ops_per_sec.sort_by(|a, b| a.total_cmp(b));
        println!(
            "{mode} median_total_s={:.3} median_ops_s={:.0}",
            totals[totals.len() / 2],
            ops_per_sec[ops_per_sec.len() / 2]
        );
    }
}
