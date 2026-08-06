//! Probe: does the WAL tail walk actually fetch concurrently?
//!
//! Wraps an in-memory store in `object_store`'s `ThrottledStore` with a fixed
//! per-GET delay, manufactures a trickled span, and times one incremental
//! build. With N files at D delay: a serial walk costs ~N x D x
//! round-trips-per-file; a C-way concurrent walk divides that by C. Run twice
//! with `GRAPH_WAL_TAIL_FETCH_CONCURRENCY=1` and `=16` to compare.
//!
//! Usage: cargo run --release --example tail_concurrency_probe [TRICKLE [DELAY_MS]]

use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::throttle::{ThrottleConfig, ThrottledStore};
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{GraphIndexBuildPath, GraphLimits, GraphShard, Result};

const CELL: &str = "bench-cell";
const EDGE_TYPE: &str = "FOLLOWS";

fn arg(index: usize, default: u64) -> u64 {
    std::env::args()
        .nth(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    let trickle = arg(1, 100);
    let delay_ms = arg(2, 25);

    let config = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(delay_ms),
        ..ThrottleConfig::default()
    };
    let store: Arc<dyn ObjectStore> =
        Arc::new(ThrottledStore::new(InMemory::new(), config)) as Arc<dyn ObjectStore>;

    let limits = GraphLimits {
        max_wal_tail_files: 1_000_000,
        ..GraphLimits::default()
    };
    let shard =
        GraphShard::open_standalone_writer_with_limits("probe/tail-concurrency", store, limits)
            .await?;

    shard
        .write_edges_batch(
            CELL,
            EDGE_TYPE,
            (0..1_000).map(|i| (i / 8, 1_000 + i)),
            "seed",
        )
        .await?;
    shard.build_graph_index(CELL, EDGE_TYPE).await?;

    for index in 0..trickle {
        shard
            .write_edges_batch(
                CELL,
                EDGE_TYPE,
                [(10_000 + index, 20_000 + index)],
                &format!("trickle-{index}"),
            )
            .await?;
    }

    let concurrency = std::env::var("GRAPH_WAL_TAIL_FETCH_CONCURRENCY")
        .unwrap_or_else(|_| "16 (default)".to_string());
    let started = Instant::now();
    let (generation, path) = shard.build_graph_index_auto(CELL, EDGE_TYPE).await?;
    let elapsed = started.elapsed().as_millis();
    let described = match path {
        GraphIndexBuildPath::Incremental { delta_edges } => {
            format!("incremental, {delta_edges} delta edges")
        }
        other => format!("{other:?}"),
    };
    println!(
        "concurrency {concurrency}: walk+build {elapsed} ms over ~{} files at {delay_ms} ms/GET [{described}]",
        generation.last_wal_id,
    );
    shard.close().await?;
    Ok(())
}
