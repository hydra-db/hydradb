use std::sync::Arc;

use crate::{
    GraphShard, GraphStorageSnapshot, MatrixTraversalResult, Result, SparseKernelBackend,
    StorageSequence, VertexId,
};
pub struct GraphSnapshot<'a> {
    pub(crate) shard: &'a GraphShard,
    pub(crate) cell_id: String,
    pub(crate) read_epoch: StorageSequence,
    pub(crate) storage_snapshot: Arc<GraphStorageSnapshot>,
}

impl<'a> GraphSnapshot<'a> {
    pub fn cell_id(&self) -> &str {
        &self.cell_id
    }

    pub fn read_epoch(&self) -> StorageSequence {
        self.read_epoch
    }

    /// Returns the SlateDB sequence pinned by this snapshot.
    pub fn storage_sequence(&self) -> Option<StorageSequence> {
        Some(self.storage_snapshot.seq())
    }

    pub async fn edge_exists(&self, edge_type: &str, src: VertexId, dst: VertexId) -> Result<bool> {
        self.shard
            .edge_exists_in_storage_snapshot(
                self.storage_snapshot.as_ref(),
                &self.cell_id,
                edge_type,
                src,
                dst,
                self.read_epoch,
            )
            .await
    }

    pub async fn out_neighbors(&self, edge_type: &str, src: VertexId) -> Result<Vec<VertexId>> {
        self.shard
            .out_neighbors_in_storage_snapshot(
                self.storage_snapshot.as_ref(),
                &self.cell_id,
                edge_type,
                src,
                self.read_epoch,
            )
            .await
    }

    pub async fn in_neighbors(&self, edge_type: &str, dst: VertexId) -> Result<Vec<VertexId>> {
        self.shard
            .in_neighbors_in_storage_snapshot(
                self.storage_snapshot.as_ref(),
                &self.cell_id,
                edge_type,
                dst,
            )
            .await
    }

    pub async fn out_degree(&self, edge_type: &str, src: VertexId) -> Result<u64> {
        self.shard
            .out_degree_in_storage_snapshot(
                self.storage_snapshot.as_ref(),
                &self.cell_id,
                edge_type,
                src,
            )
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
        crate::GraphStore::scope_snapshot(Arc::clone(&self.storage_snapshot), traversal).await
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
        crate::GraphStore::scope_snapshot(Arc::clone(&self.storage_snapshot), traversal).await
    }
}
