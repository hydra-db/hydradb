//! Before/after benchmark for incremental graph index builds.
//!
//! Seeds a graph, publishes a full baseline generation, then runs write
//! cycles. Each cycle applies a small delta (adds + a deletion), builds the
//! index through `build_graph_index_auto` (the incremental path the indexer
//! binary uses under `GRAPH_INDEXER_BUILD_MODE=incremental`), and then runs
//! the old full rebuild for comparison at the same durable sequence.
//!
//! Two numbers per cycle, matching the Prometheus counters the indexer
//! exports: `delta_edges` (work the incremental build did — the
//! `graph_indexer_incremental_delta_edges` series) and the full build's
//! `edge_count` (work the old path did every cycle — the
//! `graph_indexer_full_build_edges` series). Wall times sit beside them.
//!
//! The store is in-memory, which *understates* the win: in production the
//! full scan pays object-store latency per read, while the incremental path
//! reads one previous CSC payload plus the WAL tail.
//!
//! Usage: cargo run --release --example incremental_index_bench [SEED [CYCLES [DELTA [ENV_FILE]]]]
//! Defaults: 200_000 seed edges, 10 cycles, 100 delta edges per cycle,
//! in-memory store. Pass an env file (`AWS_ENDPOINT`, `AWS_BUCKET`, ... — the
//! same format `scripts/minio_smoke.sh` generates) as the fourth argument to
//! run against a real object store instead.

use std::sync::Arc;
use std::time::Instant;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    object_store_from_env, EdgeMutation, GraphIndexBuildPath, GraphShard, Result,
};

const CELL: &str = "bench-cell";
const EDGE_TYPE: &str = "FOLLOWS";
/// Average out-degree of seeded vertices, so the graph has realistic fan-out
/// rather than one edge per source.
const OUT_DEGREE: u64 = 8;

fn arg(index: usize, default: u64) -> u64 {
    std::env::args()
        .nth(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    let seed_edges = arg(1, 200_000);
    let cycles = arg(2, 10);
    let delta_per_cycle = arg(3, 100);

    let store: Arc<dyn ObjectStore> = match std::env::args().nth(4) {
        Some(env_file) => {
            println!("using object store from {env_file}");
            object_store_from_env(Some(env_file))?
        }
        None => Arc::new(InMemory::new()),
    };
    // Unique path per run: the store may be a persistent bucket, and a reused
    // path replays the seed's idempotency keys into a conflict.
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_millis();
    let shard = GraphShard::open_standalone_writer(
        format!("bench/incremental-index-{run_id}").as_str(),
        store,
    )
    .await?;

    println!("seeding {seed_edges} edges (out-degree ~{OUT_DEGREE})...");
    let started = Instant::now();
    shard
        .write_edges_batch_chunked(
            CELL,
            EDGE_TYPE,
            (0..seed_edges).map(|index| (index / OUT_DEGREE, seed_edges + index)),
            "bench-seed",
            10_000,
        )
        .await?;
    println!("seeded in {} ms", started.elapsed().as_millis());

    let started = Instant::now();
    let baseline = shard.build_graph_index(CELL, EDGE_TYPE).await?;
    println!(
        "baseline full build: {} edges in {} ms\n",
        baseline.edge_count,
        started.elapsed().as_millis()
    );

    println!(
        "{:>5} {:>12} {:>12} {:>10} {:>10} {:>9}",
        "cycle", "delta_edges", "full_edges", "incr_ms", "full_ms", "speedup"
    );

    let mut next_vertex = 2 * seed_edges;
    let mut total_incremental_ms = 0_u128;
    let mut total_full_ms = 0_u128;
    let mut total_delta_edges = 0_u64;
    let mut total_full_edges = 0_u64;

    for cycle in 0..cycles {
        // The delta: mostly adds from fresh vertices, plus one deletion of a
        // seeded edge so removals (and the empty-source normalization) are
        // exercised every cycle.
        shard
            .write_edges_batch(
                CELL,
                EDGE_TYPE,
                (0..delta_per_cycle.saturating_sub(1)).map(|index| {
                    (
                        next_vertex + index / OUT_DEGREE,
                        next_vertex + 1_000 + index,
                    )
                }),
                &format!("bench-delta-{cycle}"),
            )
            .await?;
        shard
            .delete_edge(EdgeMutation {
                cell_id: CELL.to_string(),
                edge_type: EDGE_TYPE.to_string(),
                src: cycle / OUT_DEGREE,
                dst: seed_edges + cycle,
                idempotency_key: format!("bench-delete-{cycle}"),
            })
            .await?;
        next_vertex += 2_000 + delta_per_cycle;

        // The new path, exactly as the indexer binary drives it.
        let started = Instant::now();
        let (_generation, path) = shard.build_graph_index_auto(CELL, EDGE_TYPE).await?;
        let incremental_ms = started.elapsed().as_millis();

        let delta_edges = match path {
            GraphIndexBuildPath::Incremental { delta_edges } => delta_edges,
            other => {
                println!("cycle {cycle}: expected an incremental build, got {other:?}");
                continue;
            }
        };

        // The old path, timed at the same durable sequence. Publishing is a
        // no-op (the incremental generation is already current), but the scan
        // and encode — the work being replaced — run in full.
        let started = Instant::now();
        let full = shard.build_graph_index(CELL, EDGE_TYPE).await?;
        let full_ms = started.elapsed().as_millis();

        total_incremental_ms += incremental_ms;
        total_full_ms += full_ms;
        total_delta_edges += delta_edges;
        total_full_edges += full.edge_count;

        println!(
            "{cycle:>5} {delta_edges:>12} {:>12} {incremental_ms:>10} {full_ms:>10} {:>8.1}x",
            full.edge_count,
            full_ms as f64 / incremental_ms.max(1) as f64,
        );
    }

    println!(
        "\ntotals over {cycles} cycles: incremental {total_incremental_ms} ms vs full {total_full_ms} ms ({:.1}x)",
        total_full_ms as f64 / total_incremental_ms.max(1) as f64,
    );
    println!(
        "edges touched: incremental applied {total_delta_edges} delta edges; the old path scanned {total_full_edges}  ({:.0}x fewer)",
        total_full_edges as f64 / total_delta_edges.max(1) as f64,
    );
    println!(
        "these are the `graph_indexer_incremental_delta_edges` vs `graph_indexer_full_build_edges` Prometheus series."
    );

    shard.close().await?;
    Ok(())
}
