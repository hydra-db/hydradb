use std::sync::Arc;
use std::time::Instant;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{EdgeMutation, GraphShard, Result};

const CELL: &str = "batch-bench";
const EDGE_TYPE: &str = "FOLLOWS";

fn mutations(prefix: &str, count: usize) -> Vec<EdgeMutation> {
    (0..count)
        .map(|index| EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: index as u64,
            dst: index as u64 + 1_000_000,
            idempotency_key: format!("{prefix}-{index:020}"),
        })
        .collect()
}

async fn open(path: &str) -> Result<GraphShard> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    GraphShard::open_standalone_writer(path, store).await
}

fn print_result(operation: &str, batch_us: u128, singles_us: u128, count: usize) {
    let speedup = singles_us as f64 / batch_us.max(1) as f64;
    println!(
        "{operation}: items={count} batch_us={batch_us} repeated_single_us={singles_us} speedup={speedup:.2}x"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let count = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|err| slatedb_graph_kernel::GraphError::CorruptValue {
            key: "batch_benchmark_count".to_string(),
            reason: err.to_string(),
        })?
        .unwrap_or(1_000);
    if count == 0 {
        return Err(slatedb_graph_kernel::GraphError::CorruptValue {
            key: "batch_benchmark_count".to_string(),
            reason: "count must be greater than zero".to_string(),
        });
    }

    let batch_write = open("batch-benchmark/write-batch").await?;
    let started = Instant::now();
    batch_write
        .write_edge_mutations_batch(CELL, mutations("batch-write", count))
        .await?;
    let batch_write_us = started.elapsed().as_micros();

    let single_write = open("batch-benchmark/write-single").await?;
    let started = Instant::now();
    for mutation in mutations("single-write", count) {
        single_write.write_edge(mutation).await?;
    }
    let single_write_us = started.elapsed().as_micros();
    print_result("write", batch_write_us, single_write_us, count);

    let sources: Vec<_> = (0..count).map(|index| index as u64).collect();
    let started = Instant::now();
    let batch_rows = batch_write
        .out_neighbors_batch(CELL, EDGE_TYPE, sources.iter().copied())
        .await?;
    let batch_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let mut single_rows = Vec::with_capacity(count);
    for source in &sources {
        single_rows.push(batch_write.out_neighbors(CELL, EDGE_TYPE, *source).await?);
    }
    let single_read_us = started.elapsed().as_micros();
    assert_eq!(
        batch_rows
            .iter()
            .map(|entry| entry.neighbors.clone())
            .collect::<Vec<_>>(),
        single_rows
    );
    print_result("read", batch_read_us, single_read_us, count);

    let batch_delete = open("batch-benchmark/delete-batch").await?;
    batch_delete
        .write_edge_mutations_batch(CELL, mutations("batch-delete-seed", count))
        .await?;
    let started = Instant::now();
    batch_delete
        .delete_edges_batch(
            CELL,
            EDGE_TYPE,
            (0..count).map(|index| (index as u64, index as u64 + 1_000_000)),
            "batch-delete",
        )
        .await?;
    let batch_delete_us = started.elapsed().as_micros();

    let single_delete = open("batch-benchmark/delete-single").await?;
    single_delete
        .write_edge_mutations_batch(CELL, mutations("single-delete-seed", count))
        .await?;
    let started = Instant::now();
    for mutation in mutations("single-delete", count) {
        single_delete.delete_edge(mutation).await?;
    }
    let single_delete_us = started.elapsed().as_micros();
    print_result("delete", batch_delete_us, single_delete_us, count);

    batch_write.close().await?;
    single_write.close().await?;
    batch_delete.close().await?;
    single_delete.close().await?;
    Ok(())
}
