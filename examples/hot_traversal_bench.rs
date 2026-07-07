use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{ArtifactDirection, GraphShard, Result, SparseKernelBackend};

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";
const DEFAULT_FANOUTS: &[u64] = &[10, 50, 100, 500, 1_000, 5_000, 10_000];
const DEFAULT_MAX_HOPS: u8 = 5;

#[tokio::main]
async fn main() -> Result<()> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let matrix_kernel = selected_matrix_kernel();
    let bench_iters = bench_iters();
    let max_hops = max_hops();
    let fanouts = fanouts();
    let reopen_before_warmup = reopen_before_warmup();

    println!(
        "graph hot traversal benchmark: fanouts={:?} max_hops={max_hops} requested_matrix_kernel={matrix_kernel:?} best_of_iters={bench_iters} reopen_before_warmup={reopen_before_warmup}",
        fanouts,
    );
    println!(
        "fanout,hops,matrix_kernel,posting_us,matrix_us,posting_edge_visits,matrix_edge_visits,posting_deltas,matrix_deltas,result_vertices,matrix_wins"
    );

    for &fanout in &fanouts {
        let shard_path = format!("graph-bench/fanout-{fanout}");
        let shard =
            GraphShard::open_standalone_writer(shard_path.clone(), Arc::clone(&object_store))
                .await?;
        load_layered_supernode(&shard, fanout, max_hops).await?;

        let base_epoch = shard.current_epoch(CELL_ID).await?;
        let build_started = Instant::now();
        shard
            .build_posting_chunks(CELL_ID, EDGE_TYPE, base_epoch, 256)
            .await?;
        let artifact = shard
            .build_matrix_tiles(CELL_ID, EDGE_TYPE, base_epoch, 4_096)
            .await?;
        let supernodes = shard
            .build_supernode_groups(CELL_ID, EDGE_TYPE, base_epoch, 10, 256)
            .await?;
        let build_elapsed = build_started.elapsed();

        eprintln!(
            "dataset fanout={fanout} epoch={base_epoch} edges={} matrix_tiles={} transpose_tiles={} supernodes={} build_ms={}",
            artifact.edge_count,
            artifact.out_tiles,
            artifact.transpose_tiles,
            supernodes.len(),
            build_elapsed.as_millis()
        );

        let shard = if reopen_before_warmup {
            shard.close().await?;
            GraphShard::open(shard_path, Arc::clone(&object_store)).await?
        } else {
            shard
        };

        let warmup_started = Instant::now();
        shard
            .matrix_reachable_with_kernel(
                CELL_ID,
                EDGE_TYPE,
                &[1],
                max_hops,
                base_epoch,
                matrix_kernel,
            )
            .await?;
        eprintln!(
            "matrix_warmup fanout={fanout} kernel={matrix_kernel:?} warmup_us={}",
            micros(warmup_started.elapsed())
        );

        for hops in 1..=max_hops {
            let mut best_posting = None;
            let mut best_posting_elapsed = Duration::MAX;
            for _ in 0..bench_iters {
                let started = Instant::now();
                let posting = shard
                    .posting_reachable(CELL_ID, EDGE_TYPE, &[1], hops, base_epoch)
                    .await?;
                let elapsed = started.elapsed();
                if elapsed < best_posting_elapsed {
                    best_posting_elapsed = elapsed;
                    best_posting = Some(posting);
                }
            }
            let posting = best_posting.expect("benchmark should run at least once");

            let mut best_matrix = None;
            let mut best_matrix_elapsed = Duration::MAX;
            for _ in 0..bench_iters {
                let started = Instant::now();
                let matrix = shard
                    .matrix_reachable_with_kernel(
                        CELL_ID,
                        EDGE_TYPE,
                        &[1],
                        hops,
                        base_epoch,
                        matrix_kernel,
                    )
                    .await?;
                let elapsed = started.elapsed();
                if elapsed < best_matrix_elapsed {
                    best_matrix_elapsed = elapsed;
                    best_matrix = Some(matrix);
                }
            }
            let matrix = best_matrix.expect("benchmark should run at least once");

            assert_eq!(posting.vertices, matrix.vertices);
            println!(
                "{fanout},{hops},{:?},{},{},{},{},{},{},{},{}",
                matrix.sparse_kernel,
                micros(best_posting_elapsed),
                micros(best_matrix_elapsed),
                posting.edge_visits,
                matrix.edge_visits,
                posting.delta_records_applied,
                matrix.delta_records_applied,
                matrix.vertices.len(),
                best_matrix_elapsed < best_posting_elapsed
            );
        }

        let degree_started = Instant::now();
        let degree = shard
            .supernode_degree(CELL_ID, EDGE_TYPE, 1, base_epoch)
            .await?;
        let degree_elapsed = degree_started.elapsed();

        let exists_started = Instant::now();
        let exists_last = shard
            .supernode_edge_exists(
                CELL_ID,
                EDGE_TYPE,
                1,
                layer_vertex(1, fanout - 1),
                base_epoch,
            )
            .await?;
        let exists_elapsed = exists_started.elapsed();

        let page_started = Instant::now();
        let first_page = shard
            .supernode_page(CELL_ID, EDGE_TYPE, ArtifactDirection::Out, 1, base_epoch, 0)
            .await?
            .expect("root supernode page should exist");
        let page_elapsed = page_started.elapsed();

        eprintln!(
            "supernode fanout={fanout} degree={degree} degree_us={} exists_last={} exists_us={} first_page={} page_us={} has_next={}",
            micros(degree_elapsed),
            exists_last,
            micros(exists_elapsed),
            first_page.vertices.len(),
            micros(page_elapsed),
            first_page.has_next
        );

        shard.close().await?;
    }

    Ok(())
}

async fn load_layered_supernode(shard: &GraphShard, fanout: u64, max_hops: u8) -> Result<()> {
    let mut edges = Vec::with_capacity((fanout as usize).saturating_mul(usize::from(max_hops)));
    for index in 0..fanout {
        let mut src = 1;
        for hop in 1..=max_hops {
            let dst = layer_vertex(hop, index);
            edges.push((src, dst));
            src = dst;
        }
    }
    shard
        .bulk_import_edges(CELL_ID, EDGE_TYPE, edges, &format!("fanout-{fanout}"))
        .await?;
    Ok(())
}

fn layer_vertex(hop: u8, index: u64) -> u64 {
    (u64::from(hop) * 1_000_000) + index + 1
}

fn micros(duration: Duration) -> u128 {
    duration.as_micros()
}

fn bench_iters() -> u32 {
    std::env::var("GRAPH_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|iters| *iters > 0)
        .unwrap_or(3)
}

fn max_hops() -> u8 {
    std::env::var("GRAPH_BENCH_MAX_HOPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|hops| *hops > 0)
        .unwrap_or(DEFAULT_MAX_HOPS)
}

fn fanouts() -> Vec<u64> {
    let Ok(value) = std::env::var("GRAPH_BENCH_FANOUTS") else {
        return DEFAULT_FANOUTS.to_vec();
    };
    let parsed: Vec<_> = value
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .filter(|fanout| *fanout > 0)
        .collect();
    if parsed.is_empty() {
        DEFAULT_FANOUTS.to_vec()
    } else {
        parsed
    }
}

fn reopen_before_warmup() -> bool {
    std::env::var("GRAPH_BENCH_REOPEN_BEFORE_WARMUP").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn selected_matrix_kernel() -> SparseKernelBackend {
    match std::env::var("GRAPH_MATRIX_KERNEL").or_else(|_| std::env::var("GRAPH_MATRIX_KERNEL")) {
        Ok(value) if value.eq_ignore_ascii_case("graphblas") => {
            SparseKernelBackend::SuiteSparseGraphBlas
        }
        Ok(value) if value.eq_ignore_ascii_case("rust") => SparseKernelBackend::RustSparse,
        _ if cfg!(feature = "graphblas") => SparseKernelBackend::SuiteSparseGraphBlas,
        _ => SparseKernelBackend::RustSparse,
    }
}
