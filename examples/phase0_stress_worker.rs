use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, EdgeMutation, GraphShard, Result,
    SparseKernelBackend,
};
use std::time::Duration;

const DEFAULT_CELL_ID: &str = "reddit-home";
const DEFAULT_EDGE_TYPE: &str = "USER_FOLLOWS_USER";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: phase0_stress_worker <writer|deleter|artifact|reader> <env-file|local:PATH|-> <db-path> [node-id] [ops] [start]"
        );
        std::process::exit(2);
    }

    let mode = args[1].as_str();
    let store_arg = &args[2];
    let db_path = &args[3];
    let node_id = args.get(4).map(String::as_str).unwrap_or("node-a");
    let ops = parse_arg(&args, 5, 1_000);
    let start = parse_arg(&args, 6, 1);
    let op_delay = parse_env_u64("PHASE0_OP_DELAY_MICROS").map(Duration::from_micros);
    let cell_id = std::env::var("PHASE0_CELL_ID").unwrap_or_else(|_| DEFAULT_CELL_ID.to_string());
    let edge_type =
        std::env::var("PHASE0_EDGE_TYPE").unwrap_or_else(|_| DEFAULT_EDGE_TYPE.to_string());

    let object_store = load_object_store(store_arg)?;
    let shard = match mode {
        "writer" | "deleter" | "artifact" => {
            GraphShard::open_standalone_writer(db_path.to_string(), object_store).await?
        }
        "reader" => GraphShard::open(db_path.to_string(), object_store).await?,
        _ => {
            eprintln!("unknown mode {mode}");
            std::process::exit(2);
        }
    };

    match mode {
        "writer" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            for offset in 0..ops {
                let dst = start + offset;
                shard
                    .write_edge(EdgeMutation {
                        cell_id: cell_id.clone(),
                        edge_type: edge_type.clone(),
                        src,
                        dst,
                        idempotency_key: format!("{node_id}-write-{src}-{dst}"),
                    })
                    .await?;
                sleep_between_ops(op_delay);
            }
            println!(
                "phase0 worker writer node={node_id} cell={cell_id} src={src} ops={ops} epoch={}",
                shard.current_epoch(&cell_id).await?
            );
        }
        "deleter" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            for offset in 0..ops {
                let dst = start + offset;
                shard
                    .delete_edge(EdgeMutation {
                        cell_id: cell_id.clone(),
                        edge_type: edge_type.clone(),
                        src,
                        dst,
                        idempotency_key: format!("{node_id}-delete-{src}-{dst}"),
                    })
                    .await?;
                sleep_between_ops(op_delay);
            }
            println!(
                "phase0 worker deleter node={node_id} cell={cell_id} src={src} ops={ops} epoch={}",
                shard.current_epoch(&cell_id).await?
            );
        }
        "artifact" => {
            let epoch = shard.current_epoch(&cell_id).await?;
            if epoch > 0 {
                let rollup = shard
                    .rollup_artifacts(
                        &cell_id,
                        &edge_type,
                        epoch,
                        env_usize("PHASE0_POSTING_CHUNK", 256),
                        parse_env_u64("PHASE0_MATRIX_TILE").unwrap_or(4_096),
                        parse_env_u64("PHASE0_SUPERNODE_THRESHOLD").unwrap_or(10),
                        env_usize("PHASE0_SUPERNODE_CHUNK", 256),
                    )
                    .await?;
                println!(
                    "phase0 worker artifact node={node_id} cell={cell_id} epoch={} posting_chunks={} matrix_edges={} supernodes={}",
                    rollup.base_epoch,
                    rollup.posting_chunks,
                    rollup.matrix_edge_count,
                    rollup.supernode_groups
                );
            } else {
                println!("phase0 worker artifact node={node_id} cell={cell_id} epoch=0 skipped");
            }
        }
        "reader" => {
            let epoch = shard.current_epoch(&cell_id).await?;
            if epoch > 0 {
                let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
                let hops = parse_env_u8("PHASE0_HOPS").unwrap_or(1);
                let posting = shard
                    .posting_reachable(&cell_id, &edge_type, &[src], hops, epoch)
                    .await?;
                let matrix = shard
                    .matrix_reachable_with_kernel(
                        &cell_id,
                        &edge_type,
                        &[src],
                        hops,
                        epoch,
                        selected_matrix_kernel(),
                    )
                    .await?;
                assert_eq!(posting.vertices, matrix.vertices);
                println!(
                    "phase0 worker reader node={node_id} cell={cell_id} epoch={epoch} hops={hops} vertices={} deltas={}",
                    matrix.vertices.len(),
                    matrix.delta_records_applied
                );
            } else {
                println!("phase0 worker reader node={node_id} cell={cell_id} epoch=0 skipped");
            }
        }
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }

    shard.close().await?;
    Ok(())
}

fn load_object_store(
    value: &str,
) -> Result<std::sync::Arc<dyn slatedb::object_store::ObjectStore>> {
    if let Some(path) = value.strip_prefix("local:") {
        local_object_store(path)
    } else if value == "-" {
        object_store_from_env(None)
    } else {
        object_store_from_env(Some(value.to_string()))
    }
}

fn selected_matrix_kernel() -> SparseKernelBackend {
    match std::env::var("PHASE0_MATRIX_KERNEL") {
        Ok(value) if value.eq_ignore_ascii_case("graphblas") => {
            SparseKernelBackend::SuiteSparseGraphBlas
        }
        Ok(value) if value.eq_ignore_ascii_case("rust") => SparseKernelBackend::RustSparse,
        _ if cfg!(feature = "graphblas") => SparseKernelBackend::SuiteSparseGraphBlas,
        _ => SparseKernelBackend::RustSparse,
    }
}

fn parse_arg(args: &[String], index: usize, default: u64) -> u64 {
    args.get(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn parse_env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

fn parse_env_u8(name: &str) -> Option<u8> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn sleep_between_ops(delay: Option<Duration>) {
    if let Some(delay) = delay {
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
    }
}
