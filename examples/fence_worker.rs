use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, EdgeMutation, GraphControlPlane, GraphError,
    GraphIndexPolicy, GraphOpenOptions, GraphShard, Result, RoutedGraphCluster, ShardLease,
    ShardPlacement,
};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_CELL_ID: &str = "reddit-home";
const DEFAULT_EDGE_TYPE: &str = "USER_FOLLOWS_USER";
const NODE_A: &str = "node-a";
const NODE_B: &str = "node-b";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!(
            "usage: fence_worker <init|takeover|stale-probe|reader> <env-file|local:PATH|-> <base-path> <control-path>"
        );
        std::process::exit(2);
    }

    let mode = args[1].as_str();
    let store_arg = &args[2];
    let base_path = &args[3];
    let control_path = &args[4];
    let cell_id = std::env::var("GRAPH_CELL_ID").unwrap_or_else(|_| DEFAULT_CELL_ID.to_string());
    let edge_type =
        std::env::var("GRAPH_EDGE_TYPE").unwrap_or_else(|_| DEFAULT_EDGE_TYPE.to_string());
    let lease_ttl = Duration::from_millis(env_u64("GRAPH_LEASE_TTL_MS", 5_000));
    let object_store = load_object_store(store_arg)?;

    match mode {
        "init" => {
            let control =
                GraphControlPlane::open(control_path.to_string(), Arc::clone(&object_store))
                    .await?;
            let placement = ShardPlacement::fixed([(cell_id.clone(), NODE_A.to_string())])?;
            control.publish_placement(&placement).await?;
            let cluster = RoutedGraphCluster::open_owned_with_control_and_options(
                base_path.to_string(),
                NODE_A,
                &control,
                Arc::clone(&object_store),
                lease_ttl,
                graph_options(),
            )
            .await?;
            let lease = cluster
                .lease(&cell_id)
                .ok_or_else(|| GraphError::WriteRequiresLease {
                    operation: "fence_init",
                    cell_id: cell_id.clone(),
                })?;

            for dst in 10..18 {
                cluster
                    .write_edge(EdgeMutation {
                        cell_id: cell_id.clone(),
                        edge_type: edge_type.clone(),
                        src: 100,
                        dst,
                        idempotency_key: format!("init-{dst}"),
                    })
                    .await?;
            }

            let shard = cluster.shard(&cell_id)?;
            shard
                .bulk_append_supernode_segment_trusted(
                    &cell_id,
                    &edge_type,
                    200,
                    [210, 211, 212],
                    "init-segment-200",
                )
                .await?;
            shard
                .delete_edge(EdgeMutation {
                    cell_id: cell_id.clone(),
                    edge_type: edge_type.clone(),
                    src: 200,
                    dst: 211,
                    idempotency_key: "init-segment-delete-211".to_string(),
                })
                .await?;
            let epoch = shard.current_epoch(&cell_id).await?;
            shard
                .rollup_artifacts(
                    &cell_id,
                    &edge_type,
                    epoch,
                    env_usize("GRAPH_POSTING_CHUNK", 4),
                    env_u64("GRAPH_MATRIX_TILE", 4),
                    env_u64("GRAPH_SUPERNODE_THRESHOLD", 2),
                    env_usize("GRAPH_SUPERNODE_CHUNK", 4),
                )
                .await?;
            cluster.close().await?;
            control.close().await?;
            println!(
                "graph fence init cell={cell_id} owner={} token={} epoch={epoch}",
                lease.owner_node_id, lease.lease_token
            );
        }
        "takeover" => {
            let control =
                GraphControlPlane::open(control_path.to_string(), Arc::clone(&object_store))
                    .await?;
            let failed_over = control
                .failover_expired_cell(&cell_id, NODE_B, lease_ttl)
                .await?;
            let cluster = RoutedGraphCluster::open_owned_with_control_and_options(
                base_path.to_string(),
                NODE_B,
                &control,
                Arc::clone(&object_store),
                lease_ttl,
                graph_options(),
            )
            .await?;
            let active = cluster
                .lease(&cell_id)
                .ok_or_else(|| GraphError::WriteRequiresLease {
                    operation: "fence_takeover",
                    cell_id: cell_id.clone(),
                })?;
            cluster
                .write_edge(EdgeMutation {
                    cell_id: cell_id.clone(),
                    edge_type: edge_type.clone(),
                    src: 100,
                    dst: 99,
                    idempotency_key: "takeover-99".to_string(),
                })
                .await?;
            let shard = cluster.shard(&cell_id)?;
            shard
                .bulk_append_supernode_segment_trusted(
                    &cell_id,
                    &edge_type,
                    200,
                    [299],
                    "takeover-segment-299",
                )
                .await?;
            let epoch = shard.current_epoch(&cell_id).await?;
            shard
                .rollup_artifacts(
                    &cell_id,
                    &edge_type,
                    epoch,
                    env_usize("GRAPH_POSTING_CHUNK", 4),
                    env_u64("GRAPH_MATRIX_TILE", 4),
                    env_u64("GRAPH_SUPERNODE_THRESHOLD", 2),
                    env_usize("GRAPH_SUPERNODE_CHUNK", 4),
                )
                .await?;
            shard
                .compact_supernode_segments(
                    &cell_id,
                    &edge_type,
                    200,
                    epoch,
                    "takeover-compact-200",
                )
                .await?;
            cluster.close().await?;
            control.close().await?;
            println!(
                "graph fence takeover cell={cell_id} failover_token={} active_token={} epoch={epoch}",
                failed_over.lease_token, active.lease_token
            );
        }
        "stale-probe" => {
            let stale_token = env_u64("GRAPH_STALE_LEASE_TOKEN", 1);
            let future_expiry = now_millis()
                .checked_add(env_u64("GRAPH_STALE_LOCAL_EXPIRY_MS", 60_000))
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "graph/stale_expiry".to_string(),
                    reason: "stale local expiry overflow".to_string(),
                })?;
            let stale_lease = ShardLease {
                cell_id: cell_id.clone(),
                owner_node_id: NODE_A.to_string(),
                lease_token: stale_token,
                expires_at_ms: future_expiry,
            };
            let shard = GraphShard::open_chaos_leased_writer_with_options(
                format!("{base_path}/{cell_id}"),
                Arc::clone(&object_store),
                graph_options(),
                NODE_A,
                stale_lease,
            )
            .await?;
            let epoch = shard.current_epoch(&cell_id).await?;

            expect_stale(
                "write_edge",
                stale_token,
                shard.write_edge(EdgeMutation {
                    cell_id: cell_id.clone(),
                    edge_type: edge_type.clone(),
                    src: 100,
                    dst: 777,
                    idempotency_key: "stale-write-777".to_string(),
                }),
            )
            .await?;
            expect_stale(
                "delete_edge",
                stale_token,
                shard.delete_edge(EdgeMutation {
                    cell_id: cell_id.clone(),
                    edge_type: edge_type.clone(),
                    src: 100,
                    dst: 10,
                    idempotency_key: "stale-delete-10".to_string(),
                }),
            )
            .await?;
            expect_stale(
                "bulk_import_edges",
                stale_token,
                shard.bulk_import_edges(
                    &cell_id,
                    &edge_type,
                    [(100, 778), (100, 779)],
                    "stale-bulk",
                ),
            )
            .await?;
            expect_stale(
                "bulk_append_supernode_segment_trusted",
                stale_token,
                shard.bulk_append_supernode_segment_trusted(
                    &cell_id,
                    &edge_type,
                    200,
                    [888],
                    "stale-segment-888",
                ),
            )
            .await?;
            expect_stale(
                "compact_supernode_segments",
                stale_token,
                shard.compact_supernode_segments(&cell_id, &edge_type, 200, epoch, "stale-compact"),
            )
            .await?;
            expect_stale(
                "build_posting_chunks",
                stale_token,
                shard.build_posting_chunks(&cell_id, &edge_type, epoch, 4),
            )
            .await?;
            expect_stale(
                "build_matrix_tiles",
                stale_token,
                shard.build_matrix_tiles(&cell_id, &edge_type, epoch, 4),
            )
            .await?;
            expect_stale(
                "build_supernode_groups",
                stale_token,
                shard.build_supernode_groups(&cell_id, &edge_type, epoch, 2, 4),
            )
            .await?;
            expect_stale(
                "rollup_artifacts",
                stale_token,
                shard.rollup_artifacts(&cell_id, &edge_type, epoch, 4, 4, 2, 4),
            )
            .await?;
            expect_stale(
                "delete_graph_artifacts_before",
                stale_token,
                shard.delete_graph_artifacts_before(&cell_id, &edge_type, epoch.saturating_add(1)),
            )
            .await?;
            expect_stale(
                "delete_deltas_through_rollup",
                stale_token,
                shard.delete_deltas_through_rollup(&cell_id, &edge_type, epoch),
            )
            .await?;

            if shard
                .edge_exists_at(&cell_id, &edge_type, 100, 777, epoch.saturating_add(10))
                .await?
                || shard
                    .edge_exists_at(&cell_id, &edge_type, 200, 888, epoch.saturating_add(10))
                    .await?
            {
                return Err(GraphError::CorruptValue {
                    key: "graph/stale_probe".to_string(),
                    reason: "stale write became visible".to_string(),
                });
            }
            shard.close().await?;
            println!(
                "graph fence stale-probe cell={cell_id} stale_token={stale_token} epoch={epoch} rejected_all=true"
            );
        }
        "reader" => {
            let shard = GraphShard::open(format!("{base_path}/{cell_id}"), object_store).await?;
            let epoch = shard.current_epoch(&cell_id).await?;
            let stale_visible = shard
                .edge_exists_at(&cell_id, &edge_type, 100, 777, epoch)
                .await?;
            let takeover_visible = shard
                .edge_exists_at(&cell_id, &edge_type, 100, 99, epoch)
                .await?;
            let segment_visible = shard
                .edge_exists_at(&cell_id, &edge_type, 200, 299, epoch)
                .await?;
            let segment_deleted_visible = shard
                .edge_exists_at(&cell_id, &edge_type, 200, 211, epoch)
                .await?;
            if stale_visible || !takeover_visible || !segment_visible || segment_deleted_visible {
                return Err(GraphError::CorruptValue {
                    key: "graph/fence_reader".to_string(),
                    reason: format!(
                        "unexpected visibility stale_visible={stale_visible} takeover_visible={takeover_visible} segment_visible={segment_visible} segment_deleted_visible={segment_deleted_visible}"
                    ),
                });
            }
            shard.close().await?;
            println!(
                "graph fence reader cell={cell_id} epoch={epoch} stale_visible={stale_visible} takeover_visible={takeover_visible}"
            );
        }
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }

    Ok(())
}

async fn expect_stale<T, F>(operation: &'static str, stale_token: u64, future: F) -> Result<()>
where
    F: Future<Output = Result<T>>,
{
    match future.await {
        Err(GraphError::StaleShardLease {
            cell_id,
            node_id,
            lease_token,
        }) if node_id == NODE_A && lease_token == stale_token => {
            println!(
                "graph fence stale-rejected operation={operation} cell={cell_id} node={node_id} token={lease_token}"
            );
            Ok(())
        }
        Err(err) => Err(GraphError::CorruptValue {
            key: format!("graph/fence/{operation}"),
            reason: format!("expected stale lease error, got {err}"),
        }),
        Ok(_) => Err(GraphError::CorruptValue {
            key: format!("graph/fence/{operation}"),
            reason: "stale operation unexpectedly succeeded".to_string(),
        }),
    }
}

fn load_object_store(value: &str) -> Result<Arc<dyn ObjectStore>> {
    if let Some(path) = value.strip_prefix("local:") {
        local_object_store(path)
    } else if value == "-" {
        object_store_from_env(None)
    } else {
        object_store_from_env(Some(value.to_string()))
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn graph_options() -> GraphOpenOptions {
    GraphOpenOptions {
        index_policy: GraphIndexPolicy::OutboundOnly,
        ..Default::default()
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
