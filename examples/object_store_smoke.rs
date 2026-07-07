use slatedb_graph_kernel::{
    object_store_from_env, ArtifactDirection, EdgeMutation, GraphShard, Result, SparseKernelBackend,
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
            .build_posting_chunks(cell_id, edge_type, base_epoch, 2)
            .await?;
        shard
            .build_matrix_tiles(cell_id, edge_type, base_epoch, 4)
            .await?;
        shard
            .build_supernode_groups(cell_id, edge_type, base_epoch, 4, 2)
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
    assert!(
        !reopened
            .supernode_edge_exists(cell_id, edge_type, 100, 11, read_epoch)
            .await?
    );
    assert!(
        reopened
            .supernode_edge_exists(cell_id, edge_type, 100, 16, read_epoch)
            .await?
    );
    let page = reopened
        .supernode_page(
            cell_id,
            edge_type,
            ArtifactDirection::Out,
            100,
            read_epoch,
            0,
        )
        .await?
        .expect("supernode page should exist after reopen");
    assert_eq!(page.vertices, vec![10, 12]);
    reopened.close().await?;

    println!("graph object-store smoke passed at epoch {read_epoch}");
    Ok(())
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
