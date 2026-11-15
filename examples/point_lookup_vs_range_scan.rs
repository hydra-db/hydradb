//! Measures the read cost of two candidate index key shapes,
//! on the same SlateDB instance the graph runs on.
//!
//! Today every property index is sorted and answered by a prefix scan:
//!
//!   cell/{cell}/vprop_idx/{property}/{value}/{vertex_id}    -> vertex id
//!
//! The proposed point-lookup form moves the unknown out of the key and into
//! the value, so an exact match is one `get`:
//!
//!   cell/{cell}/vprop_lookup/{property}/{value}             -> [vertex ids]
//!
//! The difference is not cosmetic. SlateDB's per-SST bloom filters are keyed
//! on whole keys, so `get` skips SSTs that cannot hold the key. `scan_prefix`
//! only gets the same skipping when a prefix extractor is configured, and
//! `open_graph_db` (src/core/config.rs) configures none — so a prefix scan
//! opens and merges an iterator across every SST whose range overlaps the
//! prefix, however few keys actually match.
//!
//! This harness writes both key families over the same values, flushes so the
//! reads hit SSTs rather than the memtable, and times an exact-match lookup
//! under each shape.
//!
//! Usage:
//!   cargo run --release --example point_lookup_vs_range_scan \
//!     [VALUES [IDS_PER_VALUE [ITERS]]]
//!
//! Defaults: 13008 distinct values, 1 vertex per value (the unique-id case,
//! like `app_external_id`), 200 iterations. Raise IDS_PER_VALUE to model a
//! lower-cardinality property, where the sorted form scans a posting list and
//! the lookup form still costs one `get`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, WriteBatch};

const CELL: &str = "bench-cell";
const PROPERTY: &str = "app_external_id";
fn arg(index: usize, default: usize) -> usize {
    std::env::args().nth(index).and_then(|value| value.parse().ok()).unwrap_or(default)
}

fn sorted_key(value: &str, vertex_id: u64) -> String {
    format!("cell/{CELL}/vprop_idx/{PROPERTY}/{value}/{vertex_id:020}")
}

fn sorted_prefix(value: &str) -> String {
    format!("cell/{CELL}/vprop_idx/{PROPERTY}/{value}/")
}

fn lookup_key(value: &str) -> String {
    format!("cell/{CELL}/vprop_lookup/{PROPERTY}/{value}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let values = arg(1, 13_008);
    let ids_per_value = arg(2, 1).max(1);
    let iters = arg(3, 200);
    // Keys per L0 SST: the harness flushes after every chunk, so this controls
    // how many SSTs a read has to consider — the variable the bloom-filter
    // argument actually turns on. SlateDB only builds a filter for SSTs with at
    // least `min_filter_keys` (1,000) entries, so values below that measure the
    // no-filter case.
    let keys_per_sst = arg(4, 5_000).max(1);
    // "interleaved" writes both key families in the same batch, which is what
    // a real vertex write does — every SST then spans both families and its
    // min/max key range prunes nothing. "separated" writes each family in its
    // own pass, giving both shapes tight per-SST ranges. The two answer
    // different questions; run both before trusting either.
    let separated = std::env::args().nth(5).as_deref() == Some("separated");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let run_id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
    let db = Db::builder(format!("bench/point-lookup-{run_id}"), store).build().await?;

    println!(
        "writing {values} values x {ids_per_value} id(s) in both key shapes, \
         {keys_per_sst} keys per SST, families {} ...",
        if separated { "separated" } else { "interleaved" }
    );
    let started = Instant::now();
    let mut batch = WriteBatch::new();
    let mut buffered = 0_usize;
    let mut ssts = 0_usize;
    let passes: &[&[&str]] = if separated {
        &[&["sorted"], &["lookup"]]
    } else {
        &[&["sorted", "lookup"]]
    };
    for families in passes {
        for index in 0..values {
            let value = format!("ext-{index:08}");
            let mut packed = Vec::with_capacity(ids_per_value * 8);
            for slot in 0..ids_per_value {
                let vertex_id = (index * ids_per_value + slot) as u64;
                if families.contains(&"sorted") {
                    batch.put(sorted_key(&value, vertex_id).as_bytes(), vertex_id.to_be_bytes());
                    buffered += 1;
                }
                packed.extend_from_slice(&vertex_id.to_be_bytes());
            }
            if families.contains(&"lookup") {
                batch.put(lookup_key(&value).as_bytes(), packed.as_slice());
                buffered += 1;
            }
            if buffered >= keys_per_sst {
                db.write(std::mem::replace(&mut batch, WriteBatch::new())).await?;
                // Force the memtable to L0 per chunk: a read served from memory,
                // or from one giant SST, measures nothing about filters.
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await?;
                ssts += 1;
                buffered = 0;
            }
        }
    }
    if buffered > 0 {
        db.write(batch).await?;
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await?;
        ssts += 1;
    }
    println!("wrote in {} ms across {ssts} flushed SST(s)\n", started.elapsed().as_millis());

    // Probe values spread across the keyspace so neither shape benefits from
    // one hot block being resident for every iteration.
    let probes: Vec<String> = (0..iters)
        .map(|iteration| format!("ext-{:08}", (iteration * values / iters.max(1)) % values))
        .collect();

    // Untimed warm pass so both shapes see the same cache state.
    for probe in &probes {
        db.get(lookup_key(probe).as_bytes()).await?;
        let mut iter = db.scan_prefix(sorted_prefix(probe).as_bytes(), ..).await?;
        while iter.next().await?.is_some() {}
    }

    let mut scan_samples = Vec::with_capacity(iters);
    let mut get_samples = Vec::with_capacity(iters);
    for probe in &probes {
        let started = Instant::now();
        let mut iter = db.scan_prefix(sorted_prefix(probe).as_bytes(), ..).await?;
        let mut found = 0_usize;
        while iter.next().await?.is_some() {
            found += 1;
        }
        scan_samples.push(started.elapsed());
        assert_eq!(found, ids_per_value, "sorted scan lost rows for {probe}");

        let started = Instant::now();
        let value = db.get(lookup_key(probe).as_bytes()).await?;
        get_samples.push(started.elapsed());
        assert_eq!(
            value.map(|bytes| bytes.len()),
            Some(ids_per_value * 8),
            "lookup get lost rows for {probe}"
        );
    }

    println!(
        "{:<28} {:>10} {:>10} {:>10} {:>10}",
        "read shape", "min_us", "p50_us", "mean_us", "p99_us"
    );
    let scan = report("vprop_idx prefix scan", &mut scan_samples);
    let get = report("vprop_lookup point get", &mut get_samples);
    println!(
        "\nprefix scan / point get at p50: {:.1}x  ({values} values, {ids_per_value} id(s) each)",
        scan / get.max(f64::MIN_POSITIVE)
    );

    db.close().await?;
    Ok(())
}

/// Prints one row and returns the p50 in microseconds.
fn report(name: &str, samples: &mut [Duration]) -> f64 {
    samples.sort();
    let median = samples[samples.len() / 2];
    let mean = samples.iter().sum::<Duration>() / samples.len() as u32;
    let p99 = samples[(samples.len() * 99 / 100).min(samples.len() - 1)];
    println!(
        "{:<28} {:>10.2} {:>10.2} {:>10.2} {:>10.2}",
        name,
        us(samples[0]),
        us(median),
        us(mean),
        us(p99)
    );
    us(median)
}

fn us(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}
