use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, EdgeIngestOptions, EdgeMutation, GraphError,
    GraphIndexPolicy, GraphOpenOptions, GraphShard, Result, SparseKernelBackend,
};
use std::time::Duration;

const DEFAULT_CELL_ID: &str = "reddit-home";
const DEFAULT_EDGE_TYPE: &str = "USER_FOLLOWS_USER";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: phase0_stress_worker <writer|batch|chunked-batch|deleter|segment|segment-delete|segment-compact|segment-gc|matrix|supernode|rollup|artifact|delta-gc|reader|verify|digest> <env-file|local:PATH|-> <db-path> [node-id] [ops] [start]"
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
    let open_options = graph_open_options();
    let shard = match mode {
        "writer" | "batch" | "chunked-batch" | "deleter" | "segment" | "segment-delete"
        | "segment-compact" | "segment-gc" | "matrix" | "supernode" | "rollup" | "artifact"
        | "delta-gc" => {
            GraphShard::open_standalone_writer_with_options(
                db_path.to_string(),
                object_store,
                open_options,
            )
            .await?
        }
        "reader" | "verify" | "digest" => {
            GraphShard::open_with_options(db_path.to_string(), object_store, open_options).await?
        }
        _ => {
            eprintln!("unknown mode {mode}");
            std::process::exit(2);
        }
    };

    match mode {
        "writer" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            let writer_batch = env_usize("PHASE0_WRITER_BATCH", 1_024);
            if writer_batch == 1 {
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
                    "phase0 worker writer node={node_id} cell={cell_id} src={src} ops={ops} batch=1 epoch={}",
                    shard.current_epoch(&cell_id).await?
                );
            } else {
                let mut offset = 0_u64;
                let mut inserted = 0_u64;
                let mut already_existed = 0_u64;
                let mut batches = 0_u64;
                let mut end_epoch = shard.current_epoch(&cell_id).await?;
                while offset < ops {
                    let chunk_end = (offset + writer_batch as u64).min(ops);
                    let mutations = (offset..chunk_end).map(|chunk_offset| {
                        let dst = start + chunk_offset;
                        EdgeMutation {
                            cell_id: cell_id.clone(),
                            edge_type: edge_type.clone(),
                            src,
                            dst,
                            idempotency_key: format!("{node_id}-write-{src}-{dst}"),
                        }
                    });
                    let result = shard
                        .ingest_edge_mutations(
                            &cell_id,
                            mutations,
                            EdgeIngestOptions {
                                batch_size: writer_batch,
                            },
                        )
                        .await?;
                    inserted = inserted.saturating_add(result.inserted);
                    already_existed = already_existed.saturating_add(result.already_existed);
                    batches = batches.saturating_add(result.batches);
                    end_epoch = result.end_epoch;
                    offset = chunk_end;
                    sleep_between_ops(op_delay);
                }
                println!(
                    "phase0 worker writer node={node_id} cell={cell_id} src={src} ops={ops} batch={writer_batch} batches={batches} inserted={inserted} existed={already_existed} epoch={end_epoch}"
                );
            }
        }
        "batch" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            let chunk_size = env_usize("PHASE0_BULK_CHUNK", 10_000) as u64;
            let mut inserted = 0_u64;
            let mut already_existed = 0_u64;
            let mut end_epoch = shard.current_epoch(&cell_id).await?;
            let mut chunk_start = 0_u64;
            while chunk_start < ops {
                let chunk_end = (chunk_start + chunk_size).min(ops);
                let edges = (chunk_start..chunk_end).map(|offset| (src, start + offset));
                let result = shard
                    .bulk_import_edges(
                        &cell_id,
                        &edge_type,
                        edges,
                        &format!("{node_id}-batch-{src}-{start}-{ops}-{chunk_start}-{chunk_end}"),
                    )
                    .await?;
                inserted = inserted.saturating_add(result.inserted);
                already_existed = already_existed.saturating_add(result.already_existed);
                end_epoch = result.end_epoch;
                chunk_start = chunk_end;
                sleep_between_ops(op_delay);
            }
            println!(
                "phase0 worker batch node={node_id} cell={cell_id} src={src} ops={ops} inserted={inserted} existed={already_existed} epoch={end_epoch}"
            );
        }
        "chunked-batch" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            let edges = (0..ops).map(|offset| (src, start + offset));
            let result = shard
                .bulk_import_edges_chunked(
                    &cell_id,
                    &edge_type,
                    edges,
                    &format!("{node_id}-batch-{src}-{start}-{ops}"),
                    env_usize("PHASE0_BULK_CHUNK", 10_000),
                )
                .await?;
            println!(
                "phase0 worker chunked-batch node={node_id} cell={cell_id} src={src} ops={ops} inserted={} existed={} epoch={}",
                result.inserted,
                result.already_existed,
                result.end_epoch
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
        "segment" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            let chunk_size = env_usize("PHASE0_SEGMENT_CHUNK", 1_024) as u64;
            let mut inserted = 0_u64;
            let mut already_existed = 0_u64;
            let mut end_epoch = shard.current_epoch(&cell_id).await?;
            let mut chunk_start = 0_u64;
            while chunk_start < ops {
                let chunk_end = (chunk_start + chunk_size).min(ops);
                let dsts = (chunk_start..chunk_end).map(|offset| start + offset);
                let result = shard
                    .bulk_append_supernode_segment_trusted(
                        &cell_id,
                        &edge_type,
                        src,
                        dsts,
                        &format!("{node_id}-segment-{src}-{start}-{ops}-{chunk_start}-{chunk_end}"),
                    )
                    .await?;
                inserted = inserted.saturating_add(result.inserted);
                already_existed = already_existed.saturating_add(result.already_existed);
                end_epoch = result.end_epoch;
                chunk_start = chunk_end;
                sleep_between_ops(op_delay);
            }
            println!(
                "phase0 worker segment node={node_id} cell={cell_id} src={src} ops={ops} inserted={inserted} existed={already_existed} epoch={end_epoch}"
            );
        }
        "segment-delete" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            let stride = parse_env_u64("PHASE0_SEGMENT_DELETE_STRIDE")
                .unwrap_or(2)
                .max(1);
            let mut deletes = 0_u64;
            for offset in (0..ops).filter(|offset| offset % stride == 0) {
                let dst = start + offset;
                shard
                    .delete_edge(EdgeMutation {
                        cell_id: cell_id.clone(),
                        edge_type: edge_type.clone(),
                        src,
                        dst,
                        idempotency_key: format!("{node_id}-segment-delete-{src}-{dst}"),
                    })
                    .await?;
                deletes = deletes.saturating_add(1);
                sleep_between_ops(op_delay);
            }
            println!(
                "phase0 worker segment-delete node={node_id} cell={cell_id} src={src} deletes={deletes} epoch={}",
                shard.current_epoch(&cell_id).await?
            );
        }
        "segment-compact" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            let epoch = shard.current_epoch(&cell_id).await?;
            if epoch > 0 {
                shard
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
                let compact = shard
                    .compact_supernode_segments(
                        &cell_id,
                        &edge_type,
                        src,
                        epoch,
                        &format!("{node_id}-segment-compact-{src}-{epoch}"),
                    )
                    .await?;
                println!(
                    "phase0 worker segment-compact node={node_id} cell={cell_id} src={src} epoch={} segments={} segment_deletes={} tombstone_deletes={} input_edges={} output_edges={}",
                    compact.compacted_through_epoch,
                    compact.source_segments,
                    compact.deleted_segment_keys,
                    compact.deleted_tombstone_keys,
                    compact.input_edges,
                    compact.output_edges
                );
            } else {
                println!(
                    "phase0 worker segment-compact node={node_id} cell={cell_id} epoch=0 skipped"
                );
            }
        }
        "segment-gc" => {
            let src = parse_env_u64("PHASE0_SRC_ID").unwrap_or(start);
            let epoch = shard.current_epoch(&cell_id).await?;
            if epoch > 0 {
                shard
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
                let compact = shard
                    .compact_supernode_segments(
                        &cell_id,
                        &edge_type,
                        src,
                        epoch,
                        &format!("{node_id}-segment-gc-compact-{src}-{epoch}"),
                    )
                    .await?;
                let delta_gc = shard
                    .delete_deltas_through_rollup(&cell_id, &edge_type, epoch)
                    .await?;
                println!(
                    "phase0 worker segment-gc node={node_id} cell={cell_id} src={src} epoch={} segments_deleted={} tombstones_deleted={} deltas_deleted={} deltas_retained={}",
                    compact.compacted_through_epoch,
                    compact.deleted_segment_keys,
                    compact.deleted_tombstone_keys,
                    delta_gc.deleted_delta_keys,
                    delta_gc.retained_delta_keys
                );
            } else {
                println!("phase0 worker segment-gc node={node_id} cell={cell_id} epoch=0 skipped");
            }
        }
        "matrix" => {
            let epoch = shard.current_epoch(&cell_id).await?;
            if epoch > 0 {
                let artifact = shard
                    .build_matrix_tiles(
                        &cell_id,
                        &edge_type,
                        epoch,
                        parse_env_u64("PHASE0_MATRIX_TILE").unwrap_or(4_096),
                    )
                    .await?;
                println!(
                    "phase0 worker matrix node={node_id} cell={cell_id} epoch={} edges={} out_tiles={} transpose_tiles={}",
                    artifact.base_epoch,
                    artifact.edge_count,
                    artifact.out_tiles,
                    artifact.transpose_tiles
                );
            } else {
                println!("phase0 worker matrix node={node_id} cell={cell_id} epoch=0 skipped");
            }
        }
        "supernode" => {
            let epoch = shard.current_epoch(&cell_id).await?;
            if epoch > 0 {
                let groups = shard
                    .build_supernode_groups(
                        &cell_id,
                        &edge_type,
                        epoch,
                        parse_env_u64("PHASE0_SUPERNODE_THRESHOLD").unwrap_or(10),
                        env_usize("PHASE0_SUPERNODE_CHUNK", 256),
                    )
                    .await?;
                println!(
                    "phase0 worker supernode node={node_id} cell={cell_id} epoch={epoch} groups={}",
                    groups.len()
                );
            } else {
                println!("phase0 worker supernode node={node_id} cell={cell_id} epoch=0 skipped");
            }
        }
        "artifact" | "rollup" => {
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
        "delta-gc" => {
            let epoch = shard.current_epoch(&cell_id).await?;
            if epoch > 0 {
                shard
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
                let gc = shard
                    .delete_deltas_through_rollup(&cell_id, &edge_type, epoch)
                    .await?;
                println!(
                    "phase0 worker delta-gc node={node_id} cell={cell_id} compacted={} deleted={} retained={}",
                    gc.compacted_through_epoch,
                    gc.deleted_delta_keys,
                    gc.retained_delta_keys
                );
            } else {
                println!("phase0 worker delta-gc node={node_id} cell={cell_id} epoch=0 skipped");
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
        "verify" => {
            let report = shard
                .verify_current_graph(
                    &cell_id,
                    &edge_type,
                    parse_env_u8("PHASE0_VERIFY_HOPS").unwrap_or(3),
                    env_usize("PHASE0_VERIFY_ROOTS", 8),
                )
                .await?;
            println!(
                "phase0 worker verify node={node_id} cell={cell_id} epoch={} edges={} checksum={} canonical={} out_index={} in_index={} matrix_edges={} posting_chunks={} supernodes={} traversal_checks={} mismatches={}",
                report.read_epoch,
                report.digest.live_edges,
                report.digest.edge_checksum,
                report.canonical_edges,
                report.out_index_edges,
                report.in_index_edges,
                report.matrix_edges_checked,
                report.posting_chunks_checked,
                report.supernode_groups_checked,
                report.traversal_roots_checked,
                report.mismatch_count
            );
            if !report.is_clean() {
                return Err(GraphError::CorruptValue {
                    key: format!("verify/{cell_id}/{edge_type}"),
                    reason: format!("{:?}", report.mismatch_samples),
                });
            }
        }
        "digest" => {
            let epoch = shard.current_epoch(&cell_id).await?;
            let digest = shard
                .export_live_graph_digest(&cell_id, &edge_type, epoch)
                .await?;
            println!(
                "phase0 worker digest node={node_id} cell={cell_id} epoch={} edges={} edge_checksum={} out_degree_checksum={} in_degree_checksum={}",
                digest.read_epoch,
                digest.live_edges,
                digest.edge_checksum,
                digest.out_degree_checksum,
                digest.in_degree_checksum
            );
        }
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }

    sleep_between_ops(parse_env_u64("PHASE0_EXIT_DELAY_MICROS").map(Duration::from_micros));
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

fn graph_open_options() -> GraphOpenOptions {
    let index_policy = match std::env::var("PHASE0_INDEX_POLICY") {
        Ok(value) if value.eq_ignore_ascii_case("outbound-only") => GraphIndexPolicy::OutboundOnly,
        _ => GraphIndexPolicy::default(),
    };
    GraphOpenOptions {
        index_policy,
        ..Default::default()
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
