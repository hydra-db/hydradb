//! Reproduces the staging WAL-tail regression and measures the fix.
//!
//! The staging failure mode (`interactive/incremental-build-cost.html`): a
//! tenant trickles small writes, every ~commit becomes its own WAL file, and
//! the next incremental index build pays one object-store round trip per file
//! — cost proportional to *write activity*, not to the delta or the graph. A
//! bulk-seeded bench never sees this, so this harness manufactures it: after
//! seeding and a baseline generation, it commits `TRICKLE` single-edge writes
//! per edge type, then times the builds that must walk that span.
//!
//! Two edge types share the database, as in production, so the second
//! incremental build measures the per-shard parsed-file cache: its span was
//! already downloaded by the first build's walk.
//!
//! A/B against the serial walk this replaced:
//! `GRAPH_WAL_TAIL_FETCH_CONCURRENCY=1` reproduces old behavior (minus the
//! cache, which the second edge type isolates anyway).
//!
//! Usage: cargo run --release --example wal_tail_trickle_bench \
//!            [SEED [TRICKLE [ENV_FILE]]]
//! Defaults: 100_000 seed edges per type, 500 trickle commits per type,
//! in-memory store. The env file is the same `CLOUD_PROVIDER=aws` format the
//! other benches take; pass one pointing at real S3 to measure real latency.
//!
//! Measure-only mode: `BENCH_PATH=<existing store path> BENCH_TYPE=<edge type>`
//! skips seeding and trickling and times one build over whatever un-walked
//! span the path already holds — for measuring the same trickled span from
//! multiple processes (each process resolves its fetch concurrency once).

use std::sync::Arc;
use std::time::Instant;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    object_store_from_env, GraphIndexBuildPath, GraphLimits, GraphShard, Result,
};

const CELL: &str = "bench-cell";
const EDGE_TYPES: [&str; 2] = ["FOLLOWS", "REPLIES"];
const OUT_DEGREE: u64 = 8;

fn arg(index: usize, default: u64) -> u64 {
    std::env::args()
        .nth(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    let seed_edges = arg(1, 100_000);
    let trickle = arg(2, 500);

    let store: Arc<dyn ObjectStore> = match std::env::args().nth(3) {
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
    // The gate under test must not fire here — the point is to measure the
    // walk itself, so the cap is lifted well past any span this run creates.
    let limits = GraphLimits {
        max_wal_tail_files: 1_000_000,
        max_artifact_build_edges: 50_000_000,
        ..GraphLimits::default()
    };

    if let Ok(path) = std::env::var("BENCH_PATH") {
        let edge_type = std::env::var("BENCH_TYPE").unwrap_or_else(|_| EDGE_TYPES[0].to_string());
        let started = Instant::now();
        let shard =
            GraphShard::open_standalone_writer_with_limits(path.as_str(), store, limits).await?;
        // The open replays every WAL file past the last checkpoint — over a
        // freshly trickled span that is itself a serial walk, so it is timed
        // and reported apart from the build being measured.
        println!("opened in {} ms", started.elapsed().as_millis());
        flush();
        let previous = shard
            .current_graph_index(CELL, &edge_type)
            .await?
            .expect("measure-only mode needs a published generation to build from");
        println!(
            "previous generation {} at wal id {}",
            previous.generation, previous.last_wal_id
        );
        flush();
        let started = Instant::now();
        let (generation, path_taken) = shard.build_graph_index_auto(CELL, &edge_type).await?;
        let elapsed_ms = started.elapsed().as_millis();
        let span = generation.last_wal_id.saturating_sub(previous.last_wal_id);
        println!(
            "measure-only {edge_type}: {elapsed_ms} ms over {span} wal files [{}] \
             ({:.1} ms/file)",
            describe(&path_taken),
            elapsed_ms as f64 / span.max(1) as f64,
        );
        flush();
        let started = Instant::now();
        let full = shard.build_graph_index(CELL, &edge_type).await?;
        println!(
            "full rebuild at same seq: {} ms [{} edges]",
            started.elapsed().as_millis(),
            full.edge_count
        );
        flush();
        shard.close().await?;
        return Ok(());
    }

    let shard = GraphShard::open_standalone_writer_with_limits(
        format!("bench/wal-tail-trickle-{run_id}").as_str(),
        store,
        limits,
    )
    .await?;

    let concurrency = std::env::var("GRAPH_WAL_TAIL_FETCH_CONCURRENCY")
        .unwrap_or_else(|_| "16 (default)".to_string());
    println!(
        "seed {seed_edges} edges x {} types, trickle {trickle} commits/type, \
         tail fetch concurrency {concurrency}",
        EDGE_TYPES.len()
    );

    let started = Instant::now();
    for edge_type in EDGE_TYPES {
        shard
            .write_edges_batch_chunked(
                CELL,
                edge_type,
                (0..seed_edges).map(|index| (index / OUT_DEGREE, seed_edges + index)),
                &format!("trickle-seed-{edge_type}"),
                10_000,
            )
            .await?;
    }
    println!("seeded in {} ms", started.elapsed().as_millis());

    let mut baselines = Vec::new();
    for edge_type in EDGE_TYPES {
        let started = Instant::now();
        let generation = shard.build_graph_index(CELL, edge_type).await?;
        println!(
            "baseline full build {edge_type}: {} edges in {} ms (last_wal_id {})",
            generation.edge_count,
            started.elapsed().as_millis(),
            generation.last_wal_id,
        );
        baselines.push(generation);
    }

    // The trickle. One committed write per call, alternating edge types —
    // each durable commit flushes its own WAL file(s), which is exactly the
    // shape a small-batch ingestion pipeline leaves behind.
    println!("\ntrickling {trickle} single-edge commits per type...");
    let started = Instant::now();
    for index in 0..trickle {
        for (offset, edge_type) in EDGE_TYPES.iter().enumerate() {
            shard
                .write_edges_batch(
                    CELL,
                    edge_type,
                    [(
                        2 * seed_edges + index / OUT_DEGREE + offset as u64 * seed_edges,
                        3 * seed_edges + index + offset as u64 * seed_edges,
                    )],
                    &format!("trickle-{edge_type}-{index}"),
                )
                .await?;
        }
    }
    println!("trickled in {} ms", started.elapsed().as_millis());

    // First incremental build: a cold walk over the whole trickled span.
    let started = Instant::now();
    let (cold_generation, cold_path) = shard.build_graph_index_auto(CELL, EDGE_TYPES[0]).await?;
    let cold_ms = started.elapsed().as_millis();
    let cold_span = cold_generation
        .last_wal_id
        .saturating_sub(baselines[0].last_wal_id);

    // Second incremental build: same span, already parsed and cached by the
    // first walk — the cross-edge-type sharing the fix adds.
    let started = Instant::now();
    let (warm_generation, warm_path) = shard.build_graph_index_auto(CELL, EDGE_TYPES[1]).await?;
    let warm_ms = started.elapsed().as_millis();
    let warm_span = warm_generation
        .last_wal_id
        .saturating_sub(baselines[1].last_wal_id);

    // The old path, timed at the same durable sequence.
    let started = Instant::now();
    let full = shard.build_graph_index(CELL, EDGE_TYPES[0]).await?;
    let full_ms = started.elapsed().as_millis();

    println!("\nwal span walked: {cold_span} files (cold), {warm_span} files (warm)");
    println!(
        "incremental {} (cold walk):  {cold_ms} ms  [{}]",
        EDGE_TYPES[0],
        describe(&cold_path)
    );
    println!(
        "incremental {} (warm cache): {warm_ms} ms  [{}]",
        EDGE_TYPES[1],
        describe(&warm_path)
    );
    println!(
        "full rebuild at same seq:        {full_ms} ms  [{} edges]",
        full.edge_count
    );
    println!(
        "\ncold walk per file: {:.1} ms; warm/cold: {:.2}; incremental(cold)/full: {:.2}",
        cold_ms as f64 / cold_span.max(1) as f64,
        warm_ms as f64 / cold_ms.max(1) as f64,
        cold_ms as f64 / full_ms.max(1) as f64,
    );

    shard.close().await?;
    Ok(())
}

/// Stdout is block-buffered when piped; a killed process loses whatever sat
/// in the buffer, so each measurement line is flushed the moment it exists.
fn flush() {
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
}

fn describe(path: &GraphIndexBuildPath) -> String {
    match path {
        GraphIndexBuildPath::Incremental { delta_edges } => {
            format!("incremental, {delta_edges} delta edges")
        }
        GraphIndexBuildPath::Full { edges } => format!("FULL FALLBACK, {edges} edges"),
        GraphIndexBuildPath::Current => "already current".to_string(),
    }
}
