//! Staging replay: the indexer's operating loop, not a one-shot build.
//!
//! The 2026-08-05 staging regression was not "one big build was slow" — it
//! was a *loop* degrading cycle over cycle: 8 edge types sharing one scope
//! database, an indexer building every dirty type every cycle, per-type
//! deltas of ~15 edges, and WAL spans of hundreds-to-thousands of time-cut
//! files accumulating between cycles from write activity the quiet types had
//! no part in. The WAL-tail path priced each build at one round trip per
//! span file, so every quiet type paid for the hot type's traffic, every
//! cycle, and slower cycles grew the next span (the feedback loop).
//!
//! This harness recreates that loop faithfully: seed 8 edge types, then run
//! N indexer cycles; each cycle first commits `COMMITS` single-write
//! transactions — mostly to one hot edge type, with each quiet type
//! receiving exactly `QUIET_DELTA` edges spread across the window — then
//! builds every edge type via `build_graph_index_auto` (the production
//! path), then runs the xlog GC exactly as the indexer's cleanup step does.
//! Per build it reports wall time, the path taken, the delta applied, and
//! the WAL span the old walk would have paid for; per cycle it re-verifies
//! one quiet type against a full rebuild at the same sequence.
//!
//! Usage: cargo run --release --example staging_replay_bench \
//!            [SEED [CYCLES [COMMITS [QUIET_DELTA [ENV_FILE]]]]]
//! Defaults: 100_000 seed edges/type, 4 cycles, 320 commits/cycle,
//! 15 quiet edges/type/cycle, in-memory store. Pass an env file
//! (`CLOUD_PROVIDER=aws` format) to run against a real object store.

use std::sync::Arc;
use std::time::Instant;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    object_store_from_env, GraphIndexBuildPath, GraphLimits, GraphShard, Result,
};

const CELL: &str = "staging-cell";
const EDGE_TYPES: [&str; 8] = [
    "FOLLOWS",
    "REPLIES",
    "LIKES",
    "AUTHORED",
    "MENTIONS",
    "SUBSCRIBES",
    "MODERATES",
    "LINKS",
];
const OUT_DEGREE: u64 = 8;

fn arg(index: usize, default: u64) -> u64 {
    std::env::args()
        .nth(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn describe(path: &GraphIndexBuildPath) -> String {
    match path {
        GraphIndexBuildPath::Incremental { delta_edges } => {
            format!("incremental, {delta_edges} delta edges")
        }
        GraphIndexBuildPath::Full { edges } => format!("FULL, {edges} edges scanned"),
        GraphIndexBuildPath::Current => "current, no work".to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let seed_edges = arg(1, 100_000);
    let cycles = arg(2, 4);
    let commits = arg(3, 320);
    let quiet_delta = arg(4, 15);

    let store: Arc<dyn ObjectStore> = match std::env::args().nth(5) {
        Some(env_file) => {
            println!("using object store from {env_file}");
            object_store_from_env(Some(env_file))?
        }
        None => Arc::new(InMemory::new()),
    };
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_millis();
    let limits = GraphLimits {
        max_artifact_build_edges: 50_000_000,
        ..GraphLimits::default()
    };
    let shard = GraphShard::open_standalone_writer_with_limits(
        format!("bench/staging-replay-{run_id}").as_str(),
        store,
        limits,
    )
    .await?;

    let hot = EDGE_TYPES[0];
    let quiet = &EDGE_TYPES[1..];
    println!(
        "staging replay: {} types x {seed_edges} seed edges, {cycles} cycles, \
         {commits} commits/cycle ({} hot on {hot}, {quiet_delta} per quiet type)",
        EDGE_TYPES.len(),
        commits - quiet.len() as u64 * quiet_delta,
    );

    let started = Instant::now();
    for edge_type in EDGE_TYPES {
        shard
            .write_edges_batch_chunked(
                CELL,
                edge_type,
                (0..seed_edges).map(|i| (i / OUT_DEGREE, seed_edges + i)),
                &format!("replay-seed-{edge_type}"),
                10_000,
            )
            .await?;
    }
    println!("seeded in {} ms", started.elapsed().as_millis());

    // Track each type's previous generation to report the WAL span its build
    // faced — the quantity the old walk paid one GET per file for.
    let mut previous: std::collections::BTreeMap<&str, u64> = Default::default();
    let mut hot_counter = 0_u64;
    let mut quiet_counters = vec![0_u64; quiet.len()];

    for cycle in 0..cycles {
        // Background traffic: quiet writes spread evenly through the hot
        // stream, exactly one durable commit each, so every commit can cut
        // its own WAL file — staging's shape.
        let quiet_total = quiet.len() as u64 * quiet_delta;
        let stride = (commits / quiet_total.max(1)).max(1);
        let trickle_started = Instant::now();
        let mut quiet_written = 0_u64;
        for index in 0..commits {
            let quiet_turn = index % stride == 0 && quiet_written < quiet_total;
            if quiet_turn {
                let type_index = (quiet_written % quiet.len() as u64) as usize;
                let counter = quiet_counters[type_index];
                shard
                    .write_edges_batch(
                        CELL,
                        quiet[type_index],
                        [(20 * seed_edges + counter, 21 * seed_edges + counter)],
                        &format!("replay-quiet-{cycle}-{quiet_written}"),
                    )
                    .await?;
                quiet_counters[type_index] += 1;
                quiet_written += 1;
            } else {
                shard
                    .write_edges_batch(
                        CELL,
                        hot,
                        [(10 * seed_edges + hot_counter, 11 * seed_edges + hot_counter)],
                        &format!("replay-hot-{cycle}-{hot_counter}"),
                    )
                    .await?;
                hot_counter += 1;
            }
        }
        println!(
            "\n== cycle {cycle}: {commits} commits ({quiet_written} quiet) in {} ms ==",
            trickle_started.elapsed().as_millis()
        );

        // The indexer cycle: build every edge type, hot first (as the
        // dirty-type iteration would), then GC — the cleanup step's shape.
        let cycle_started = Instant::now();
        for edge_type in EDGE_TYPES {
            let build_started = Instant::now();
            let (generation, path) = shard.build_graph_index_auto(CELL, edge_type).await?;
            let elapsed_ms = build_started.elapsed().as_millis();
            let span = previous
                .get(edge_type)
                .map(|last| generation.last_wal_id.saturating_sub(*last))
                .unwrap_or(0);
            previous.insert(edge_type, generation.last_wal_id);
            println!(
                "  {edge_type:<11} {elapsed_ms:>7} ms  span {span:>5} wal files  [{}]",
                describe(&path)
            );
        }
        // Equivalence probe: one quiet type per cycle, full rebuild at the
        // same durable sequence must agree with what the incremental just
        // published.
        let probe = quiet[cycle as usize % quiet.len()];
        let inc = shard
            .current_graph_index(CELL, probe)
            .await?
            .expect("probe type was just built");
        let full_started = Instant::now();
        let full = shard.build_graph_index(CELL, probe).await?;
        println!(
            "  verify {probe}: full rebuild {} ms, generations {} (incremental {}, full {})",
            full_started.elapsed().as_millis(),
            if full.generation == inc.generation {
                "AGREE"
            } else {
                "DISAGREE"
            },
            &inc.generation[..12.min(inc.generation.len())],
            &full.generation[..12.min(full.generation.len())],
        );
        let mut reclaimed = 0_u64;
        for edge_type in EDGE_TYPES {
            reclaimed += shard.gc_topology_changelog(CELL, edge_type).await?;
        }
        println!(
            "  cycle total {} ms, xlog GC reclaimed {reclaimed} entries",
            cycle_started.elapsed().as_millis()
        );
    }

    println!(
        "\nold-path comparator: each quiet build above would have walked its span \
         serially (~50 ms/file measured on staging; a 300-file span ≈ 15 s per type, \
         every cycle, per each of the 7 quiet types)"
    );
    shard.close().await?;
    Ok(())
}
