use std::sync::Arc;

use crate::{
    GraphShard, MatrixTraversalResult, Result, SparseKernelBackend, StorageSequence,
    TopologySequence, VertexId,
};
pub struct GraphSnapshot<'a> {
    pub(crate) shard: &'a GraphShard,
    pub(crate) cell_id: String,
    pub(crate) read_epoch: TopologySequence,
    pub(crate) storage_snapshot: Option<Arc<slatedb::DbSnapshot>>,
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
