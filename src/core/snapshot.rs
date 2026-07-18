use std::sync::Arc;

use crate::{
    decode_u64, keys, remote_read_options, validate_component, GraphError, GraphShard,
    MatrixTraversalResult, Result, SparseKernelBackend, StorageSequence, TopologySequence,
    VertexId,
};

pub struct GraphSnapshot<'a> {
    pub(crate) shard: &'a GraphShard,
    pub(crate) cell_id: String,
    pub(crate) read_epoch: TopologySequence,
    pub(crate) storage_snapshot: Option<Arc<slatedb::DbSnapshot>>,
}

pub struct OwnedGraphSnapshot {
    shard: Arc<GraphShard>,
    cell_id: String,
    read_epoch: TopologySequence,
    storage_snapshot: Option<Arc<slatedb::DbSnapshot>>,
}

impl GraphShard {
    pub async fn owned_snapshot(self: &Arc<Self>, cell_id: &str) -> Result<OwnedGraphSnapshot> {
        validate_component("cell_id", cell_id)?;
        let storage_snapshot = self.db.snapshot().await?;
        let read_epoch = if let Some(snapshot) = storage_snapshot.as_ref() {
            let key = keys::last_epoch(cell_id);
            match snapshot
                .get_with_options(key.as_bytes(), &remote_read_options())
                .await?
            {
                Some(value) => decode_u64(&key, &value)?,
                None => 0,
            }
        } else {
            // DbReader pins a checkpoint/manifest even though this SlateDB
            // revision does not expose a DbSnapshot handle for it.
            self.current_epoch(cell_id).await?
        };
        Ok(OwnedGraphSnapshot {
            shard: Arc::clone(self),
            cell_id: cell_id.to_string(),
            read_epoch,
            storage_snapshot,
        })
    }

    pub async fn owned_snapshot_at(
        self: &Arc<Self>,
        cell_id: &str,
        read_epoch: TopologySequence,
    ) -> Result<OwnedGraphSnapshot> {
        validate_component("cell_id", cell_id)?;
        let storage_snapshot = self.db.snapshot().await?;
        let snapshot_epoch = if let Some(snapshot) = storage_snapshot.as_ref() {
            let drop_marker = keys::cell_drop_marker(cell_id);
            let pending_drop_marker = keys::cell_drop_pending_marker(cell_id);
            if snapshot
                .get_with_options(drop_marker.as_bytes(), &remote_read_options())
                .await?
                .is_some()
                || snapshot
                    .get_with_options(pending_drop_marker.as_bytes(), &remote_read_options())
                    .await?
                    .is_some()
            {
                return Err(GraphError::CellDropped {
                    operation: "owned_snapshot_at",
                    cell_id: cell_id.to_string(),
                });
            }
            let key = keys::last_epoch(cell_id);
            match snapshot
                .get_with_options(key.as_bytes(), &remote_read_options())
                .await?
            {
                Some(value) => decode_u64(&key, &value)?,
                None => 0,
            }
        } else {
            // DbReader pins a checkpoint/manifest even though this SlateDB
            // revision does not expose a DbSnapshot handle for it.
            self.current_epoch(cell_id).await?
        };
        if read_epoch > snapshot_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: cell_id.to_string(),
                read_epoch,
                current_epoch: snapshot_epoch,
            });
        }
        if read_epoch != snapshot_epoch {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphSnapshot",
                feature: "historical graph epochs are not SlateDB snapshots".to_string(),
            });
        }
        Ok(OwnedGraphSnapshot {
            shard: Arc::clone(self),
            cell_id: cell_id.to_string(),
            read_epoch,
            storage_snapshot,
        })
    }
}

impl OwnedGraphSnapshot {
    pub fn cell_id(&self) -> &str {
        &self.cell_id
    }

    pub fn read_epoch(&self) -> TopologySequence {
        self.read_epoch
    }

    /// Returns the SlateDB sequence pinned by this snapshot. Reader-only shards
    /// pin their checkpoint through `DbReader`, which does not expose a
    /// `DbSnapshot` sequence in the SlateDB revision used here.
    pub fn storage_sequence(&self) -> Option<StorageSequence> {
        self.storage_snapshot
            .as_ref()
            .map(|snapshot| snapshot.seq())
    }

    pub async fn edge_exists(&self, edge_type: &str, src: VertexId, dst: VertexId) -> Result<bool> {
        let snapshot = self.as_borrowed();
        snapshot.edge_exists(edge_type, src, dst).await
    }

    pub async fn out_neighbors(&self, edge_type: &str, src: VertexId) -> Result<Vec<VertexId>> {
        let snapshot = self.as_borrowed();
        snapshot.out_neighbors(edge_type, src).await
    }

    pub async fn out_degree(&self, edge_type: &str, src: VertexId) -> Result<u64> {
        let snapshot = self.as_borrowed();
        snapshot.out_degree(edge_type, src).await
    }

    pub async fn matrix_reachable(
        &self,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
    ) -> Result<MatrixTraversalResult> {
        let snapshot = self.as_borrowed();
        snapshot.matrix_reachable(edge_type, starts, hops).await
    }

    pub async fn matrix_reachable_with_kernel(
        &self,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<MatrixTraversalResult> {
        let snapshot = self.as_borrowed();
        snapshot
            .matrix_reachable_with_kernel(edge_type, starts, hops, sparse_kernel)
            .await
    }

    fn as_borrowed(&self) -> GraphSnapshot<'_> {
        GraphSnapshot {
            shard: self.shard.as_ref(),
            cell_id: self.cell_id.clone(),
            read_epoch: self.read_epoch,
            storage_snapshot: self.storage_snapshot.clone(),
        }
    }
}

impl<'a> GraphSnapshot<'a> {
    pub fn cell_id(&self) -> &str {
        &self.cell_id
    }

    pub fn read_epoch(&self) -> TopologySequence {
        self.read_epoch
    }

    /// Returns the SlateDB sequence pinned by this snapshot. Reader-only shards
    /// pin their checkpoint through `DbReader`, which does not expose a
    /// `DbSnapshot` sequence in the SlateDB revision used here.
    pub fn storage_sequence(&self) -> Option<StorageSequence> {
        self.storage_snapshot
            .as_ref()
            .map(|snapshot| snapshot.seq())
    }

    pub async fn edge_exists(&self, edge_type: &str, src: VertexId, dst: VertexId) -> Result<bool> {
        if let Some(snapshot) = self.storage_snapshot.as_deref() {
            return self
                .shard
                .edge_exists_in_storage_snapshot(
                    snapshot,
                    &self.cell_id,
                    edge_type,
                    src,
                    dst,
                    self.read_epoch,
                )
                .await;
        }
        self.shard
            .edge_exists_at(&self.cell_id, edge_type, src, dst, self.read_epoch)
            .await
    }

    pub async fn out_neighbors(&self, edge_type: &str, src: VertexId) -> Result<Vec<VertexId>> {
        if let Some(snapshot) = self.storage_snapshot.as_deref() {
            return self
                .shard
                .out_neighbors_in_storage_snapshot(
                    snapshot,
                    &self.cell_id,
                    edge_type,
                    src,
                    self.read_epoch,
                )
                .await;
        }
        self.shard
            .out_neighbors_at(&self.cell_id, edge_type, src, self.read_epoch)
            .await
    }

    pub async fn out_degree(&self, edge_type: &str, src: VertexId) -> Result<u64> {
        if let Some(snapshot) = self.storage_snapshot.as_deref() {
            return Ok(self
                .shard
                .out_neighbors_in_storage_snapshot(
                    snapshot,
                    &self.cell_id,
                    edge_type,
                    src,
                    self.read_epoch,
                )
                .await?
                .len() as u64);
        }
        self.shard
            .out_degree_at(&self.cell_id, edge_type, src, self.read_epoch)
            .await
    }

    pub async fn matrix_reachable(
        &self,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
    ) -> Result<MatrixTraversalResult> {
        let traversal =
            self.shard
                .matrix_reachable(&self.cell_id, edge_type, starts, hops, self.read_epoch);
        if let Some(snapshot) = self.storage_snapshot.as_ref() {
            return crate::GraphStore::scope_snapshot(Arc::clone(snapshot), traversal).await;
        }
        traversal.await
    }

    pub async fn matrix_reachable_with_kernel(
        &self,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<MatrixTraversalResult> {
        let traversal = self.shard.matrix_reachable_with_kernel(
            &self.cell_id,
            edge_type,
            starts,
            hops,
            self.read_epoch,
            sparse_kernel,
        );
        if let Some(snapshot) = self.storage_snapshot.as_ref() {
            return crate::GraphStore::scope_snapshot(Arc::clone(snapshot), traversal).await;
        }
        traversal.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::{memory::InMemory, ObjectStore};

    const CELL: &str = "snapshot-epoch-filter";
    const EDGE_TYPE: &str = "FOLLOWS";

    fn mutation(src: VertexId, dst: VertexId, idempotency_key: &str) -> crate::EdgeMutation {
        crate::EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src,
            dst,
            idempotency_key: idempotency_key.to_string(),
        }
    }

    #[tokio::test]
    async fn direct_records_after_read_epoch_are_hidden_from_borrowed_and_owned_snapshots() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = Arc::new(
            GraphShard::open_standalone_writer("graph/snapshot-epoch-filter", object_store)
                .await
                .unwrap(),
        );
        shard.write_edge(mutation(1, 2, "epoch-1")).await.unwrap();
        let read_epoch = shard.current_epoch(CELL).await.unwrap();
        shard.write_edge(mutation(1, 3, "epoch-2")).await.unwrap();

        // This represents a writer advancing from N to N+1 between learning N
        // and reading direct records from the storage snapshot.
        let storage_snapshot = shard.db.snapshot().await.unwrap().unwrap();
        let borrowed = GraphSnapshot {
            shard: shard.as_ref(),
            cell_id: CELL.to_string(),
            read_epoch,
            storage_snapshot: Some(Arc::clone(&storage_snapshot)),
        };
        let owned = OwnedGraphSnapshot {
            shard: Arc::clone(&shard),
            cell_id: CELL.to_string(),
            read_epoch,
            storage_snapshot: Some(storage_snapshot),
        };

        assert!(borrowed.edge_exists(EDGE_TYPE, 1, 2).await.unwrap());
        assert!(!borrowed.edge_exists(EDGE_TYPE, 1, 3).await.unwrap());
        assert_eq!(borrowed.out_neighbors(EDGE_TYPE, 1).await.unwrap(), vec![2]);
        assert_eq!(borrowed.out_degree(EDGE_TYPE, 1).await.unwrap(), 1);
        assert!(owned.edge_exists(EDGE_TYPE, 1, 2).await.unwrap());
        assert!(!owned.edge_exists(EDGE_TYPE, 1, 3).await.unwrap());
        assert_eq!(owned.out_neighbors(EDGE_TYPE, 1).await.unwrap(), vec![2]);
        assert_eq!(owned.out_degree(EDGE_TYPE, 1).await.unwrap(), 1);

        drop(borrowed);
        drop(owned);
        shard.close().await.unwrap();
    }
}
