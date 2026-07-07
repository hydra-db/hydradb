use crate::{
    ArtifactDirection, GraphEpoch, GraphShard, MatrixTraversalResult, Result, SparseKernelBackend,
    SupernodePage, VertexId,
};
pub struct GraphSnapshot<'a> {
    pub(crate) shard: &'a GraphShard,
    pub(crate) cell_id: String,
    pub(crate) read_epoch: GraphEpoch,
}

impl<'a> GraphSnapshot<'a> {
    pub fn cell_id(&self) -> &str {
        &self.cell_id
    }

    pub fn read_epoch(&self) -> GraphEpoch {
        self.read_epoch
    }

    pub async fn edge_exists(&self, edge_type: &str, src: VertexId, dst: VertexId) -> Result<bool> {
        self.shard
            .edge_exists_at(&self.cell_id, edge_type, src, dst, self.read_epoch)
            .await
    }

    pub async fn out_neighbors(&self, edge_type: &str, src: VertexId) -> Result<Vec<VertexId>> {
        self.shard
            .out_neighbors_at(&self.cell_id, edge_type, src, self.read_epoch)
            .await
    }

    pub async fn out_degree(&self, edge_type: &str, src: VertexId) -> Result<u64> {
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
        self.shard
            .matrix_reachable(&self.cell_id, edge_type, starts, hops, self.read_epoch)
            .await
    }

    pub async fn matrix_reachable_with_kernel(
        &self,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<MatrixTraversalResult> {
        self.shard
            .matrix_reachable_with_kernel(
                &self.cell_id,
                edge_type,
                starts,
                hops,
                self.read_epoch,
                sparse_kernel,
            )
            .await
    }

    pub async fn supernode_degree(&self, edge_type: &str, vertex_id: VertexId) -> Result<u64> {
        self.shard
            .supernode_degree(&self.cell_id, edge_type, vertex_id, self.read_epoch)
            .await
    }

    pub async fn supernode_page(
        &self,
        edge_type: &str,
        direction: ArtifactDirection,
        vertex_id: VertexId,
        page_id: u64,
    ) -> Result<Option<SupernodePage>> {
        self.shard
            .supernode_page(
                &self.cell_id,
                edge_type,
                direction,
                vertex_id,
                self.read_epoch,
                page_id,
            )
            .await
    }

    pub async fn supernode_edge_exists(
        &self,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
    ) -> Result<bool> {
        self.shard
            .supernode_edge_exists(&self.cell_id, edge_type, src, dst, self.read_epoch)
            .await
    }

    pub async fn supernode_intersection(
        &self,
        edge_type: &str,
        vertex_id: VertexId,
        candidates: &[VertexId],
    ) -> Result<Vec<VertexId>> {
        self.shard
            .supernode_intersection(
                &self.cell_id,
                edge_type,
                vertex_id,
                candidates,
                self.read_epoch,
            )
            .await
    }
}
