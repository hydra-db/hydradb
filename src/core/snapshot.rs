use std::{collections::BTreeMap, sync::Arc};

use crate::{
    codec::{decode_relationship_record, decode_vertex_metadata},
    keys, GraphError, GraphShard, GraphStorageSnapshot, MatrixTraversalResult, RelationshipId, RelationshipRecord, Result,
    SparseKernelBackend, StorageSequence, VertexId, VertexMetadata,
};

/// A committed, read-only inventory of canonical graph entities.
///
/// This API supports conformance side-effect accounting. It does not expose
/// derived indexes, caches, or uncommitted writer state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphInspection {
    pub vertices: BTreeMap<VertexId, VertexMetadata>,
    pub relationships: BTreeMap<RelationshipId, RelationshipRecord>,
}
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

    /// Every stored key under `prefix` at this snapshot, in key order, with
    /// its value as stored: for tools that compare or hash a whole store
    /// family by family. The scan is remote and keeps its blocks out of the
    /// cache, as any full-range scan does.
    pub async fn scan_storage_prefix(&self, prefix: &[u8]) -> Result<slatedb::DbIterator> {
        #[cfg(feature = "opencypher")]
        let options = crate::codec::remote_bulk_scan_options();
        #[cfg(not(feature = "opencypher"))]
        let options = crate::codec::remote_scan_options();
        Ok(self.storage_snapshot.scan_prefix_with_options(prefix, .., &options).await?)
    }

    /// Inspect all canonical vertices and relationships in this committed snapshot.
    pub async fn inspect_graph(&self) -> Result<GraphInspection> {
        let mut inspection = GraphInspection::default();

        let vertex_prefix = keys::vertex_prefix(&self.cell_id);
        let mut vertices = self
            .storage_snapshot
            .scan_prefix_with_options(vertex_prefix.as_bytes(), .., &slatedb::config::ScanOptions::default())
            .await?;
        while let Some(entry) = vertices.next().await? {
            let key = std::str::from_utf8(&entry.key).map_err(|error| GraphError::CorruptValue {
                key: String::from_utf8_lossy(&entry.key).into_owned(),
                reason: format!("vertex key is not UTF-8: {error}"),
            })?;
            let vertex_id = key
                .strip_prefix(&vertex_prefix)
                .and_then(|suffix| suffix.parse::<VertexId>().ok())
                .ok_or_else(|| GraphError::CorruptValue {
                    key: key.to_string(),
                    reason: "vertex key has an invalid id suffix".to_string(),
                })?;
            inspection.vertices.insert(vertex_id, decode_vertex_metadata(key, &entry.value)?);
        }

        let relationship_prefix = keys::relationship_cell_prefix(&self.cell_id);
        let mut relationships = self
            .storage_snapshot
            .scan_prefix_with_options(relationship_prefix.as_bytes(), .., &slatedb::config::ScanOptions::default())
            .await?;
        while let Some(entry) = relationships.next().await? {
            let key = std::str::from_utf8(&entry.key).map_err(|error| GraphError::CorruptValue {
                key: String::from_utf8_lossy(&entry.key).into_owned(),
                reason: format!("relationship key is not UTF-8: {error}"),
            })?;
            let record = decode_relationship_record(key, &entry.value)?;
            inspection.relationships.insert(record.relationship_id, record);
        }
        Ok(inspection)
    }

    pub async fn edge_exists(&self, edge_type: &str, src: VertexId, dst: VertexId) -> Result<bool> {
        self.shard
            .edge_exists_in_storage_snapshot(self.storage_snapshot.as_ref(), &self.cell_id, edge_type, src, dst, self.read_epoch)
            .await
    }

    pub async fn out_neighbors(&self, edge_type: &str, src: VertexId) -> Result<Vec<VertexId>> {
        self.shard
            .out_neighbors_in_storage_snapshot(self.storage_snapshot.as_ref(), &self.cell_id, edge_type, src, self.read_epoch)
            .await
    }

    pub async fn in_neighbors(&self, edge_type: &str, dst: VertexId) -> Result<Vec<VertexId>> {
        self.shard
            .in_neighbors_in_storage_snapshot(self.storage_snapshot.as_ref(), &self.cell_id, edge_type, dst)
            .await
    }

    pub async fn out_degree(&self, edge_type: &str, src: VertexId) -> Result<u64> {
        self.shard
            .out_degree_in_storage_snapshot(self.storage_snapshot.as_ref(), &self.cell_id, edge_type, src)
            .await
    }

    pub async fn matrix_reachable(&self, edge_type: &str, starts: &[VertexId], hops: u8) -> Result<MatrixTraversalResult> {
        let traversal = self.shard.matrix_reachable(&self.cell_id, edge_type, starts, hops, self.read_epoch);
        crate::SlateGraphStore::scope_snapshot(Arc::clone(&self.storage_snapshot), traversal).await
    }

    pub async fn matrix_reachable_with_kernel(
        &self,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<MatrixTraversalResult> {
        let traversal = self
            .shard
            .matrix_reachable_with_kernel(&self.cell_id, edge_type, starts, hops, self.read_epoch, sparse_kernel);
        crate::SlateGraphStore::scope_snapshot(Arc::clone(&self.storage_snapshot), traversal).await
    }
}
