use slatedb_graph_kernel::{
    object_store_from_env, EdgeMutation, GraphShard, Result, SparseKernelBackend,
};

#[tokio::main]
async fn main() -> Result<()> {
    let env_file = std::env::args().nth(1);
    let db_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| format!("graph-object-store-smoke-{}", std::process::id()));
    let cell_id = "reddit-home";
    let edge_type = "USER_FOLLOWS_USER";

    {
        let object_store = object_store_from_env(env_file.clone())?;
        let shard = GraphShard::open_standalone_writer(db_path.clone(), object_store).await?;
        for dst in 10..16 {
            shard
                .write_edge(EdgeMutation {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    src: 100,
                    dst,
                    idempotency_key: format!("follow-{dst}"),
                })
                .await?;
        }

        let base_epoch = shard.current_epoch(cell_id).await?;
        shard
            .build_matrix_tiles(cell_id, edge_type, base_epoch, 4)
            .await?;

        shard
            .write_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: 100,
                dst: 16,
                idempotency_key: "follow-16".to_string(),
            })
            .await?;
        shard
            .delete_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: 100,
                dst: 11,
                idempotency_key: "unfollow-11".to_string(),
            })
            .await?;

        let read_epoch = shard.current_epoch(cell_id).await?;
        let matrix = shard
            .matrix_reachable_with_kernel(
                cell_id,
                edge_type,
                &[100],
                1,
                read_epoch,
                selected_matrix_kernel(),
            )
            .await?;
        assert_eq!(matrix.vertices, vec![10, 12, 13, 14, 15, 16]);
        shard.close().await?;
    }

    let object_store = object_store_from_env(env_file)?;
    let reopened = GraphShard::open(db_path, object_store).await?;
    let read_epoch = reopened.current_epoch(cell_id).await?;
    let snapshot = reopened.snapshot_at(cell_id, read_epoch).await?;
    assert!(!snapshot.edge_exists(edge_type, 100, 11).await?);
    assert!(snapshot.edge_exists(edge_type, 100, 16).await?);
    assert_eq!(
        snapshot.out_neighbors(edge_type, 100).await?,
        vec![10, 12, 13, 14, 15, 16]
    );
    reopened.close().await?;

    println!("graph object-store smoke passed at epoch {read_epoch}");
    Ok(())
}

fn selected_matrix_kernel() -> SparseKernelBackend {
    match std::env::var("GRAPH_MATRIX_KERNEL") {
        Ok(value) if value.eq_ignore_ascii_case("graphblas") => SparseKernelBackend::SuiteSparse,
        Ok(value) if value.eq_ignore_ascii_case("compact") => SparseKernelBackend::CompactCsc,
        Ok(value) if value.eq_ignore_ascii_case("rust") => SparseKernelBackend::Adjacency,
        _ => SparseKernelBackend::SuiteSparse,
    }
}
