use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use slatedb::bytes::Bytes;
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions};
use slatedb::object_store::{local::LocalFileSystem, ObjectStore};
use slatedb::{Db, DbTransaction, ErrorKind, IsolationLevel, WriteBatch};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::sparse_kernel::{
    compile_graphblas_csc, compile_graphblas_matrix, default_matrix_kernel,
    expand as expand_sparse, expand_compiled_graphblas, graphblas_csc_from_adjacency,
    CompiledGraphBlasMatrix, GraphBlasCsc, SparseKernelBackend,
};
use crate::{
    decode_edge_record, decode_u64, ensure_limit, open_graph_db, parse_u64, validate_component,
    DeltaKind, DeltaRecord, EdgeRecord, GraphCacheConfig, GraphCacheKind, GraphEpoch, GraphError,
    GraphOpenOptions, GraphShard, MatrixAdjacency, MatrixCacheKey, PostingChunkCacheKey, Result,
    SupernodeCacheKey, VertexId,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ArtifactDirection {
    Out,
    In,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostingChunk {
    pub cell_id: String,
    pub edge_type: String,
    pub direction: ArtifactDirection,
    pub owner: VertexId,
    pub base_epoch: GraphEpoch,
    pub chunk_id: u64,
    pub vertices: Vec<VertexId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixArtifact {
    pub cell_id: String,
    pub edge_type: String,
    pub base_epoch: GraphEpoch,
    pub tile_size: u64,
    pub out_tiles: u64,
    pub transpose_tiles: u64,
    pub edge_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraversalBackend {
    PostingExpansion,
    MatrixOverlay,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixTraversalResult {
    pub backend: TraversalBackend,
    pub vertices: Vec<VertexId>,
    pub hops: u8,
    pub base_epoch: GraphEpoch,
    pub edge_visits: u64,
    pub delta_records_applied: u64,
    pub sparse_kernel: SparseKernelBackend,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkResult {
    pub posting: MatrixTraversalResult,
    pub matrix: MatrixTraversalResult,
    pub matrix_wins: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupernodeGroup {
    pub cell_id: String,
    pub edge_type: String,
    pub direction: ArtifactDirection,
    pub vertex_id: VertexId,
    pub base_epoch: GraphEpoch,
    pub degree: u64,
    pub chunk_count: u64,
    pub page_size: u64,
    pub chunk_bounds: Vec<SupernodeChunkBound>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupernodeChunkBound {
    pub chunk_id: u64,
    pub first: VertexId,
    pub last: VertexId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupernodePage {
    pub vertex_id: VertexId,
    pub edge_type: String,
    pub direction: ArtifactDirection,
    pub page_id: u64,
    pub vertices: Vec<VertexId>,
    pub has_next: bool,
}

pub struct Phase0Cluster {
    shards: BTreeMap<String, GraphShard>,
}

pub struct GraphControlPlane {
    db: Db,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPlacement {
    owners: BTreeMap<String, String>,
}

pub struct RoutedPhase0Cluster {
    local_node_id: String,
    placement: ShardPlacement,
    shards: BTreeMap<String, GraphShard>,
    leases: Arc<RwLock<BTreeMap<String, ShardLease>>>,
}

pub struct GraphNode {
    cluster: RoutedPhase0Cluster,
    lease_renewer: LeaseRenewalHandle,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardLease {
    pub cell_id: String,
    pub owner_node_id: String,
    pub lease_token: u64,
    pub expires_at_ms: u64,
}

pub struct LeaseRenewalHandle {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRollup {
    pub cell_id: String,
    pub edge_type: String,
    pub base_epoch: GraphEpoch,
    pub posting_chunks: u64,
    pub matrix_edge_count: u64,
    pub supernode_groups: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactGcResult {
    pub deleted_keys: u64,
    pub retained_keys: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeltaGcResult {
    pub deleted_delta_keys: u64,
    pub retained_delta_keys: u64,
    pub compacted_through_epoch: GraphEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatrixTile {
    cell_id: String,
    edge_type: String,
    base_epoch: GraphEpoch,
    tile_size: u64,
    direction: ArtifactDirection,
    tile_row: u64,
    tile_col: u64,
    rows: BTreeMap<VertexId, Vec<VertexId>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SupernodeDeltaOverlay {
    plus: BTreeSet<VertexId>,
    minus: BTreeSet<VertexId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphBlasCscManifest {
    cell_id: String,
    edge_type: String,
    base_epoch: GraphEpoch,
    chunk_size: u64,
    vertices_len: u64,
    pointers_len: u64,
    indices_len: u64,
    vertex_chunks: u64,
    pointer_chunks: u64,
    index_chunks: u64,
    checksum: u64,
}

impl GraphShard {
    pub async fn build_posting_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        chunk_size: usize,
    ) -> Result<Vec<PostingChunk>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "build_posting_chunks")?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "posting_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }
        ensure_limit(
            "build_posting_chunks",
            base_epoch,
            self.limits.max_artifact_source_epochs,
        )?;

        let edges = self.edges_at(cell_id, edge_type, base_epoch).await?;
        let mut by_out: BTreeMap<VertexId, Vec<VertexId>> = BTreeMap::new();
        let mut by_in: BTreeMap<VertexId, Vec<VertexId>> = BTreeMap::new();
        for edge in edges {
            by_out.entry(edge.src).or_default().push(edge.dst);
            by_in.entry(edge.dst).or_default().push(edge.src);
        }

        let mut chunks = Vec::new();
        append_posting_chunks(
            cell_id,
            edge_type,
            base_epoch,
            ArtifactDirection::Out,
            chunk_size,
            by_out,
            &mut chunks,
        );
        append_posting_chunks(
            cell_id,
            edge_type,
            base_epoch,
            ArtifactDirection::In,
            chunk_size,
            by_in,
            &mut chunks,
        );

        let mut batch = WriteBatch::new();
        let mut pending_writes = 0_usize;
        for chunk in &chunks {
            put_artifact_record(
                self,
                &mut batch,
                &mut pending_writes,
                posting_key(chunk),
                encode_posting_chunk(chunk),
            )
            .await?;
        }
        flush_artifact_put_batch(self, &mut batch, &mut pending_writes).await?;
        if !chunks.is_empty() {
            let mut cache = self.posting_chunk_cache.lock().await;
            for chunk in &chunks {
                cache.insert(
                    PostingChunkCacheKey::from_chunk(chunk),
                    chunk.clone(),
                    chunk.cell_id.clone(),
                    false,
                    &self.cache_metrics,
                );
            }
        }
        Ok(chunks)
    }

    pub async fn posting_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        owner: VertexId,
        base_epoch: GraphEpoch,
    ) -> Result<Vec<PostingChunk>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = posting_prefix(cell_id, edge_type, direction, owner, base_epoch);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut chunks = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            chunks.push(decode_posting_chunk(&key, &kv.value)?);
        }
        chunks.sort_by_key(|chunk| chunk.chunk_id);
        Ok(chunks)
    }

    pub async fn build_matrix_tiles(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        tile_size: u64,
    ) -> Result<MatrixArtifact> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "build_matrix_tiles")?;
        if tile_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "matrix_tile_size".to_string(),
                reason: "tile size must be greater than zero".to_string(),
            });
        }
        ensure_limit(
            "build_matrix_tiles",
            base_epoch,
            self.limits.max_artifact_source_epochs,
        )?;

        let edges = self.edges_at(cell_id, edge_type, base_epoch).await?;
        let out_tiles = matrix_tiles_from_edges(
            cell_id,
            edge_type,
            base_epoch,
            tile_size,
            ArtifactDirection::Out,
            &edges,
        );
        let transpose_tiles = matrix_tiles_from_edges(
            cell_id,
            edge_type,
            base_epoch,
            tile_size,
            ArtifactDirection::In,
            &edges,
        );
        let adjacency = Arc::new(adjacency_from_edges(&edges));
        let graphblas_csc = graphblas_csc_from_adjacency(adjacency.as_ref())?;
        let artifact = MatrixArtifact {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            base_epoch,
            tile_size,
            out_tiles: out_tiles.len() as u64,
            transpose_tiles: transpose_tiles.len() as u64,
            edge_count: edges.len() as u64,
        };

        let mut data_batch = WriteBatch::new();
        let mut pending_writes = 0_usize;
        for tile in out_tiles.iter().chain(transpose_tiles.iter()) {
            put_artifact_record(
                self,
                &mut data_batch,
                &mut pending_writes,
                matrix_tile_key(tile),
                encode_matrix_tile(tile),
            )
            .await?;
        }
        let graphblas_manifest = append_graphblas_csc_chunks(
            self,
            &mut data_batch,
            &mut pending_writes,
            cell_id,
            edge_type,
            base_epoch,
            &graphblas_csc,
        )
        .await?;
        flush_artifact_put_batch(self, &mut data_batch, &mut pending_writes).await?;

        let mut manifest_batch = WriteBatch::new();
        manifest_batch.put(
            matrix_manifest_key(cell_id, edge_type, base_epoch),
            encode_matrix_artifact(&artifact),
        );
        manifest_batch.put(
            graphblas_csc_key(cell_id, edge_type, base_epoch),
            encode_graphblas_csc_manifest(&graphblas_manifest),
        );
        self.write_strict(manifest_batch).await?;
        let cache_key = MatrixCacheKey::new(cell_id, edge_type, base_epoch);
        let pinned = self.cache_policy.pin_matrix_artifact(&artifact);
        let tenant = cell_id.to_string();
        self.matrix_artifact_cache.lock().await.insert(
            cache_key.clone(),
            artifact.clone(),
            tenant.clone(),
            pinned,
            &self.cache_metrics,
        );
        self.matrix_cache.lock().await.insert(
            cache_key.clone(),
            Arc::clone(&adjacency),
            tenant.clone(),
            pinned,
            &self.cache_metrics,
        );

        #[cfg(feature = "graphblas")]
        {
            let started = Instant::now();
            let compiled = Arc::new(compile_graphblas_csc(&graphblas_csc)?);
            phase0_matrix_profile(
                phase0_matrix_profile_enabled(),
                "build_matrix_tiles_precompile_graphblas",
                started.elapsed(),
                edges.len() as u64,
            );
            if let Some(prewarm_start) = adjacency
                .iter()
                .filter(|(_, dsts)| !dsts.is_empty())
                .min_by_key(|(_, dsts)| dsts.len())
                .map(|(src, _)| *src)
            {
                let started = Instant::now();
                let prewarm =
                    expand_compiled_graphblas(&compiled, adjacency.as_ref(), &[prewarm_start], 1)?;
                phase0_matrix_profile(
                    phase0_matrix_profile_enabled(),
                    "build_matrix_tiles_prewarm_graphblas",
                    started.elapsed(),
                    prewarm.vertices.len() as u64,
                );
            }
            self.graphblas_cache.lock().await.insert(
                cache_key,
                compiled,
                tenant,
                pinned,
                &self.cache_metrics,
            );
        }
        Ok(artifact)
    }

    pub async fn latest_matrix_artifact(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Option<MatrixArtifact>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if let Some(cached) = self.matrix_artifact_cache.lock().await.get_latest_by(
            |key, _| {
                key.cell_id == cell_id && key.edge_type == edge_type && key.base_epoch <= read_epoch
            },
            |key, _| key.base_epoch,
        ) {
            self.cache_metrics
                .record_hit(GraphCacheKind::MatrixArtifact);
            return Ok(Some(cached));
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::MatrixArtifact);
        let _permit = self
            .acquire_hydration_permit("latest_matrix_artifact")
            .await?;
        let prefix = matrix_manifest_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest = None;
        let mut decoded = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let artifact = decode_matrix_artifact(&key, &kv.value)?;
            decoded.push(artifact.clone());
            if artifact.base_epoch <= read_epoch
                && latest
                    .as_ref()
                    .is_none_or(|current: &MatrixArtifact| artifact.base_epoch > current.base_epoch)
            {
                latest = Some(artifact);
            }
        }
        self.record_hydration_complete();
        if !decoded.is_empty() {
            let mut cache = self.matrix_artifact_cache.lock().await;
            for artifact in decoded {
                let pinned = self.cache_policy.pin_matrix_artifact(&artifact);
                cache.insert(
                    MatrixCacheKey::new(
                        &artifact.cell_id,
                        &artifact.edge_type,
                        artifact.base_epoch,
                    ),
                    artifact,
                    cell_id.to_string(),
                    pinned,
                    &self.cache_metrics,
                );
            }
        }
        Ok(latest)
    }

    pub async fn matrix_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<MatrixTraversalResult> {
        self.matrix_reachable_with_kernel(
            cell_id,
            edge_type,
            starts,
            hops,
            read_epoch,
            default_matrix_kernel(),
        )
        .await
    }

    pub async fn matrix_reachable_with_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<MatrixTraversalResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        ensure_limit(
            "matrix_reachable",
            u64::from(hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        if hops == 1 {
            if let Some(result) = self
                .supernode_one_hop_reachable(cell_id, edge_type, starts, read_epoch, sparse_kernel)
                .await?
            {
                return Ok(result);
            }
        }
        let profile = phase0_matrix_profile_enabled();
        let total_started = Instant::now();

        let started = Instant::now();
        let artifact = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .await?;
        phase0_matrix_profile(
            profile,
            "latest_matrix_artifact",
            started.elapsed(),
            artifact.as_ref().map_or(0, |artifact| artifact.base_epoch),
        );

        let base_epoch = artifact.as_ref().map_or(0, |artifact| artifact.base_epoch);
        let started = Instant::now();
        let deltas = if read_epoch <= base_epoch {
            Vec::new()
        } else {
            self.deltas_between(cell_id, edge_type, base_epoch, read_epoch)
                .await?
        };
        phase0_matrix_profile(
            profile,
            "deltas_since",
            started.elapsed(),
            deltas.len() as u64,
        );

        if sparse_kernel == SparseKernelBackend::SuiteSparseGraphBlas
            && deltas.is_empty()
            && artifact.is_some()
        {
            let started = Instant::now();
            let compiled = self
                .cached_graphblas_matrix(cell_id, edge_type, base_epoch)
                .await?;
            phase0_matrix_profile(profile, "cached_graphblas_matrix", started.elapsed(), 0);

            let started = Instant::now();
            let empty_adjacency = BTreeMap::new();
            let traversal = expand_compiled_graphblas(&compiled, &empty_adjacency, starts, hops)?;
            phase0_matrix_profile(
                profile,
                "expand_compiled_graphblas",
                started.elapsed(),
                traversal.vertices.len() as u64,
            );
            phase0_matrix_profile(
                profile,
                "matrix_reachable_total",
                total_started.elapsed(),
                0,
            );
            return Ok(MatrixTraversalResult {
                backend: TraversalBackend::MatrixOverlay,
                vertices: traversal.vertices,
                hops,
                base_epoch,
                edge_visits: traversal.edge_visits,
                delta_records_applied: 0,
                sparse_kernel: traversal.backend,
            });
        }

        let started = Instant::now();
        let base_adjacency = if let Some(artifact) = artifact.as_ref() {
            self.cached_matrix_adjacency(cell_id, edge_type, artifact.base_epoch)
                .await?
        } else {
            Arc::new(BTreeMap::new())
        };
        phase0_matrix_profile(
            profile,
            "cached_matrix_adjacency",
            started.elapsed(),
            base_adjacency.len() as u64,
        );

        let mut adjacency = base_adjacency.as_ref().clone();
        let applied = apply_delta_overlay(&mut adjacency, deltas, base_epoch, read_epoch);
        let traversal = expand_sparse(&adjacency, starts, hops, sparse_kernel)?;
        phase0_matrix_profile(
            profile,
            "matrix_reachable_total",
            total_started.elapsed(),
            0,
        );
        Ok(MatrixTraversalResult {
            backend: TraversalBackend::MatrixOverlay,
            vertices: traversal.vertices,
            hops,
            base_epoch,
            edge_visits: traversal.edge_visits,
            delta_records_applied: applied,
            sparse_kernel: traversal.backend,
        })
    }

    pub async fn posting_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<MatrixTraversalResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        ensure_limit(
            "posting_reachable",
            u64::from(hops),
            u64::from(self.limits.max_traversal_hops),
        )?;
        let deltas = self
            .deltas_between(cell_id, edge_type, 0, read_epoch)
            .await?;
        let mut adjacency = BTreeMap::new();
        let applied = apply_delta_overlay(&mut adjacency, deltas, 0, read_epoch);
        let traversal = expand_sparse(&adjacency, starts, hops, SparseKernelBackend::RustSparse)?;
        Ok(MatrixTraversalResult {
            backend: TraversalBackend::PostingExpansion,
            vertices: traversal.vertices,
            hops,
            base_epoch: 0,
            edge_visits: traversal.edge_visits,
            delta_records_applied: applied,
            sparse_kernel: traversal.backend,
        })
    }

    pub async fn benchmark_hot_hops(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<BenchmarkResult> {
        let posting = self
            .posting_reachable(cell_id, edge_type, starts, hops, read_epoch)
            .await?;
        let matrix = self
            .matrix_reachable(cell_id, edge_type, starts, hops, read_epoch)
            .await?;
        Ok(BenchmarkResult {
            matrix_wins: matrix.vertices == posting.vertices
                && (matrix.edge_visits < posting.edge_visits
                    || matrix.delta_records_applied < posting.delta_records_applied),
            posting,
            matrix,
        })
    }

    pub async fn build_supernode_groups(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
    ) -> Result<Vec<SupernodeGroup>> {
        self.build_supernode_groups_for_directions(
            cell_id,
            edge_type,
            base_epoch,
            degree_threshold,
            chunk_size,
            &[ArtifactDirection::Out, ArtifactDirection::In],
        )
        .await
    }

    pub async fn build_supernode_groups_for_directions(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
        directions: &[ArtifactDirection],
    ) -> Result<Vec<SupernodeGroup>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "build_supernode_groups_for_directions")?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "supernode_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }

        ensure_limit(
            "build_supernode_groups",
            base_epoch,
            self.limits.max_artifact_source_epochs,
        )?;

        if base_epoch == self.current_epoch(cell_id).await? {
            return self
                .build_current_supernode_groups(
                    cell_id,
                    edge_type,
                    base_epoch,
                    degree_threshold,
                    chunk_size,
                    directions,
                )
                .await;
        }

        let edges = self.edges_at(cell_id, edge_type, base_epoch).await?;
        let mut by_out: BTreeMap<VertexId, Vec<VertexId>> = BTreeMap::new();
        let mut by_in: BTreeMap<VertexId, Vec<VertexId>> = BTreeMap::new();
        for edge in edges {
            by_out.entry(edge.src).or_default().push(edge.dst);
            by_in.entry(edge.dst).or_default().push(edge.src);
        }

        let mut chunks = Vec::new();
        let mut groups = Vec::new();
        if directions.contains(&ArtifactDirection::Out) {
            append_supernode_chunks(
                cell_id,
                edge_type,
                base_epoch,
                ArtifactDirection::Out,
                degree_threshold,
                chunk_size,
                by_out,
                &mut chunks,
                &mut groups,
            );
        }
        if directions.contains(&ArtifactDirection::In) {
            append_supernode_chunks(
                cell_id,
                edge_type,
                base_epoch,
                ArtifactDirection::In,
                degree_threshold,
                chunk_size,
                by_in,
                &mut chunks,
                &mut groups,
            );
        }
        self.persist_supernode_artifacts(&chunks, &groups).await?;
        Ok(groups)
    }

    async fn build_current_supernode_groups(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
        directions: &[ArtifactDirection],
    ) -> Result<Vec<SupernodeGroup>> {
        let mut chunks = Vec::new();
        let mut groups = Vec::new();
        for direction in directions {
            self.append_current_supernode_groups(
                cell_id,
                edge_type,
                base_epoch,
                *direction,
                degree_threshold,
                chunk_size,
                &mut chunks,
                &mut groups,
            )
            .await?;
        }
        self.persist_supernode_artifacts(&chunks, &groups).await?;
        Ok(groups)
    }

    async fn append_current_supernode_groups(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        direction: ArtifactDirection,
        degree_threshold: u64,
        chunk_size: usize,
        chunks: &mut Vec<PostingChunk>,
        groups: &mut Vec<SupernodeGroup>,
    ) -> Result<()> {
        for owner in self
            .current_supernode_owners(cell_id, edge_type, direction, degree_threshold)
            .await?
        {
            let first_chunk_id = chunks.len() as u64;
            let mut chunk_bounds = Vec::new();
            let mut vertices = Vec::with_capacity(chunk_size);
            let prefix = match direction {
                ArtifactDirection::Out => crate::keys::out_prefix(cell_id, edge_type, owner),
                ArtifactDirection::In => crate::keys::in_prefix(cell_id, edge_type, owner),
            };
            let mut iter = self.scan_remote_prefix(&prefix).await?;
            let mut last_vertex = None;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let edge = decode_edge_record(&key, &kv.value)?;
                if edge.epoch > base_epoch {
                    continue;
                }
                let vertex = match direction {
                    ArtifactDirection::Out => edge.dst,
                    ArtifactDirection::In => edge.src,
                };
                if last_vertex == Some(vertex) {
                    continue;
                }
                last_vertex = Some(vertex);
                vertices.push(vertex);
                if vertices.len() == chunk_size {
                    append_current_supernode_chunk(
                        cell_id,
                        edge_type,
                        base_epoch,
                        direction,
                        owner,
                        chunks,
                        &mut chunk_bounds,
                        &mut vertices,
                    );
                }
            }
            if !vertices.is_empty() {
                append_current_supernode_chunk(
                    cell_id,
                    edge_type,
                    base_epoch,
                    direction,
                    owner,
                    chunks,
                    &mut chunk_bounds,
                    &mut vertices,
                );
            }
            let chunk_count = (chunks.len() as u64).saturating_sub(first_chunk_id);
            if chunk_count == 0 {
                continue;
            }
            let degree = chunks
                .iter()
                .skip(first_chunk_id as usize)
                .map(|chunk| chunk.vertices.len() as u64)
                .sum::<u64>();
            groups.push(SupernodeGroup {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                direction,
                vertex_id: owner,
                base_epoch,
                degree,
                chunk_count,
                page_size: chunk_size as u64,
                chunk_bounds,
            });
        }
        Ok(())
    }

    async fn current_supernode_owners(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        degree_threshold: u64,
    ) -> Result<Vec<VertexId>> {
        let prefix = match direction {
            ArtifactDirection::Out => crate::keys::degree_out_prefix(cell_id, edge_type),
            ArtifactDirection::In => crate::keys::degree_in_prefix(cell_id, edge_type),
        };
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut owners = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let degree = decode_u64(&key, &kv.value)?;
            if degree >= degree_threshold {
                owners.push(parse_last_key_component(&key, "supernode_owner")?);
            }
        }
        Ok(owners)
    }

    pub async fn rollup_artifacts(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        posting_chunk_size: usize,
        matrix_tile_size: u64,
        supernode_degree_threshold: u64,
        supernode_chunk_size: usize,
    ) -> Result<GraphRollup> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "rollup_artifacts")?;
        let posting_chunks = self
            .build_posting_chunks(cell_id, edge_type, base_epoch, posting_chunk_size)
            .await?;
        let matrix = self
            .build_matrix_tiles(cell_id, edge_type, base_epoch, matrix_tile_size)
            .await?;
        let supernode_groups = self
            .persist_supernode_groups_from_chunks(
                cell_id,
                edge_type,
                base_epoch,
                supernode_degree_threshold,
                supernode_chunk_size,
                &posting_chunks,
            )
            .await?;

        let rollup = GraphRollup {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            base_epoch,
            posting_chunks: posting_chunks.len() as u64,
            matrix_edge_count: matrix.edge_count,
            supernode_groups: supernode_groups.len() as u64,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            rollup_key(cell_id, edge_type, base_epoch),
            encode_graph_rollup(&rollup),
        );
        self.write_strict(batch).await?;
        Ok(rollup)
    }

    pub async fn latest_rollup(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Option<GraphRollup>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = rollup_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest = None;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let rollup = decode_graph_rollup(&key, &kv.value)?;
            if rollup.base_epoch <= read_epoch
                && latest
                    .as_ref()
                    .is_none_or(|current: &GraphRollup| rollup.base_epoch > current.base_epoch)
            {
                latest = Some(rollup);
            }
        }
        Ok(latest)
    }

    pub async fn delete_graph_artifacts_before(
        &self,
        cell_id: &str,
        edge_type: &str,
        keep_epoch: GraphEpoch,
    ) -> Result<ArtifactGcResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "delete_graph_artifacts_before")?;
        let mut result = ArtifactGcResult::default();
        let mut batch = WriteBatch::new();
        let mut pending_deletes = 0_usize;

        for prefix in graph_artifact_gc_prefixes(cell_id, edge_type) {
            let mut iter = self.scan_remote_prefix(&prefix).await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let Some(base_epoch) = graph_artifact_epoch_from_key(&key)? else {
                    result.retained_keys += 1;
                    continue;
                };
                if base_epoch < keep_epoch {
                    batch.delete(key.as_bytes());
                    result.deleted_keys += 1;
                    pending_deletes += 1;
                    if pending_deletes >= GRAPH_ARTIFACT_GC_BATCH_KEYS {
                        flush_artifact_gc_batch(self, &mut batch, &mut pending_deletes).await?;
                    }
                } else {
                    result.retained_keys += 1;
                }
            }
        }

        flush_artifact_gc_batch(self, &mut batch, &mut pending_deletes).await?;
        self.matrix_artifact_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.matrix_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.graphblas_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.supernode_group_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        self.posting_chunk_cache.lock().await.retain(|key, _| {
            key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch >= keep_epoch
        });
        Ok(result)
    }

    async fn persist_supernode_groups_from_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        degree_threshold: u64,
        chunk_size: usize,
        chunks: &[PostingChunk],
    ) -> Result<Vec<SupernodeGroup>> {
        let mut groups = Vec::new();
        let mut batch = WriteBatch::new();
        let mut pending_writes = 0_usize;
        for direction in [ArtifactDirection::Out, ArtifactDirection::In] {
            let mut owners: BTreeMap<VertexId, Vec<&PostingChunk>> = BTreeMap::new();
            for chunk in chunks.iter().filter(|chunk| chunk.direction == direction) {
                owners.entry(chunk.owner).or_default().push(chunk);
            }
            for (owner, owner_chunks) in owners {
                let degree: u64 = owner_chunks
                    .iter()
                    .map(|chunk| chunk.vertices.len() as u64)
                    .sum();
                if degree < degree_threshold {
                    continue;
                }
                let group = SupernodeGroup {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    direction,
                    vertex_id: owner,
                    base_epoch,
                    degree,
                    chunk_count: owner_chunks.len() as u64,
                    page_size: chunk_size as u64,
                    chunk_bounds: supernode_chunk_bounds(&owner_chunks),
                };
                put_artifact_record(
                    self,
                    &mut batch,
                    &mut pending_writes,
                    supernode_group_key(&group),
                    encode_supernode_group(&group),
                )
                .await?;
                groups.push(group);
            }
        }
        flush_artifact_put_batch(self, &mut batch, &mut pending_writes).await?;
        if !groups.is_empty() {
            let mut cache = self.supernode_group_cache.lock().await;
            for group in &groups {
                cache.insert(
                    SupernodeCacheKey::new(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                    ),
                    group.clone(),
                    group.cell_id.clone(),
                    self.cache_policy.pin_supernode_group(group),
                    &self.cache_metrics,
                );
            }
        }
        Ok(groups)
    }

    async fn persist_supernode_artifacts(
        &self,
        chunks: &[PostingChunk],
        groups: &[SupernodeGroup],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        let mut pending_writes = 0_usize;
        for chunk in chunks {
            put_artifact_record(
                self,
                &mut batch,
                &mut pending_writes,
                posting_key(chunk),
                encode_posting_chunk(chunk),
            )
            .await?;
        }
        for group in groups {
            put_artifact_record(
                self,
                &mut batch,
                &mut pending_writes,
                supernode_group_key(group),
                encode_supernode_group(group),
            )
            .await?;
        }
        flush_artifact_put_batch(self, &mut batch, &mut pending_writes).await?;
        if !chunks.is_empty() {
            let mut cache = self.posting_chunk_cache.lock().await;
            for chunk in chunks {
                let pinned = groups.iter().any(|group| {
                    group.cell_id == chunk.cell_id
                        && group.edge_type == chunk.edge_type
                        && group.direction == chunk.direction
                        && group.vertex_id == chunk.owner
                        && group.base_epoch == chunk.base_epoch
                        && self.cache_policy.pin_supernode_group(group)
                });
                cache.insert(
                    PostingChunkCacheKey::from_chunk(chunk),
                    chunk.clone(),
                    chunk.cell_id.clone(),
                    pinned,
                    &self.cache_metrics,
                );
            }
        }
        if !groups.is_empty() {
            let mut cache = self.supernode_group_cache.lock().await;
            for group in groups {
                cache.insert(
                    SupernodeCacheKey::new(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                    ),
                    group.clone(),
                    group.cell_id.clone(),
                    self.cache_policy.pin_supernode_group(group),
                    &self.cache_metrics,
                );
            }
        }
        Ok(())
    }

    pub async fn supernode_group(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Option<SupernodeGroup>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if let Some(cached) = self.supernode_group_cache.lock().await.get_latest_by(
            |key, _| {
                key.cell_id == cell_id
                    && key.edge_type == edge_type
                    && key.direction == direction
                    && key.vertex_id == vertex_id
                    && key.base_epoch <= read_epoch
            },
            |key, _| key.base_epoch,
        ) {
            self.cache_metrics
                .record_hit(GraphCacheKind::SupernodeGroup);
            self.prefetch_supernode_chunks(&cached).await;
            return Ok(Some(cached));
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::SupernodeGroup);
        let _permit = self.acquire_hydration_permit("supernode_group").await?;
        let prefix = supernode_group_prefix(cell_id, edge_type, direction, vertex_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut latest = None;
        let mut decoded = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let group = decode_supernode_group(&key, &kv.value)?;
            decoded.push(group.clone());
            if group.base_epoch <= read_epoch
                && latest
                    .as_ref()
                    .is_none_or(|current: &SupernodeGroup| group.base_epoch > current.base_epoch)
            {
                latest = Some(group);
            }
        }
        self.record_hydration_complete();
        if !decoded.is_empty() {
            let mut cache = self.supernode_group_cache.lock().await;
            for group in decoded {
                cache.insert(
                    SupernodeCacheKey::new(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                    ),
                    group.clone(),
                    group.cell_id.clone(),
                    self.cache_policy.pin_supernode_group(&group),
                    &self.cache_metrics,
                );
            }
        }
        if let Some(group) = latest.as_ref() {
            self.prefetch_supernode_chunks(group).await;
        }
        Ok(latest)
    }

    async fn supernode_one_hop_reachable(
        &self,
        cell_id: &str,
        edge_type: &str,
        starts: &[VertexId],
        read_epoch: GraphEpoch,
        sparse_kernel: SparseKernelBackend,
    ) -> Result<Option<MatrixTraversalResult>> {
        let profile = phase0_matrix_profile_enabled();
        let total_started = Instant::now();
        if starts.is_empty() {
            return Ok(Some(MatrixTraversalResult {
                backend: TraversalBackend::MatrixOverlay,
                vertices: Vec::new(),
                hops: 1,
                base_epoch: read_epoch,
                edge_visits: 0,
                delta_records_applied: 0,
                sparse_kernel,
            }));
        }

        let mut result = BTreeSet::new();
        let mut base_epoch = read_epoch;
        let mut edge_visits = 0_u64;
        let mut delta_records_applied = 0_u64;
        for start in starts {
            let started = Instant::now();
            let Some(group) = self
                .supernode_group(
                    cell_id,
                    edge_type,
                    ArtifactDirection::Out,
                    *start,
                    read_epoch,
                )
                .await?
            else {
                phase0_matrix_profile(
                    profile,
                    "supernode_one_hop_group_miss",
                    started.elapsed(),
                    *start,
                );
                return Ok(None);
            };
            phase0_matrix_profile(
                profile,
                "supernode_one_hop_group_hit",
                started.elapsed(),
                group.chunk_count,
            );
            base_epoch = base_epoch.min(group.base_epoch);
            let started = Instant::now();
            let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
            phase0_matrix_profile(
                profile,
                "supernode_one_hop_overlay",
                started.elapsed(),
                overlay.plus.len() as u64 + overlay.minus.len() as u64,
            );
            delta_records_applied = delta_records_applied
                .saturating_add(overlay.plus.len() as u64)
                .saturating_add(overlay.minus.len() as u64);
            let started = Instant::now();
            let degree = self.supernode_degree_with_overlay(&group, &overlay).await?;
            phase0_matrix_profile(
                profile,
                "supernode_one_hop_degree",
                started.elapsed(),
                degree,
            );
            edge_visits = edge_visits.saturating_add(degree);
            let limit = usize::try_from(degree).unwrap_or(usize::MAX);
            let started = Instant::now();
            for vertex in self
                .merged_supernode_vertices(&group, &overlay, 0, limit)
                .await?
            {
                result.insert(vertex);
            }
            phase0_matrix_profile(
                profile,
                "supernode_one_hop_materialize",
                started.elapsed(),
                result.len() as u64,
            );
        }

        phase0_matrix_profile(
            profile,
            "supernode_one_hop_total",
            total_started.elapsed(),
            result.len() as u64,
        );
        Ok(Some(MatrixTraversalResult {
            backend: TraversalBackend::MatrixOverlay,
            vertices: result.into_iter().collect(),
            hops: 1,
            base_epoch,
            edge_visits,
            delta_records_applied,
            sparse_kernel,
        }))
    }

    pub async fn supernode_degree(
        &self,
        cell_id: &str,
        edge_type: &str,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<u64> {
        if let Some(group) = self
            .supernode_group(
                cell_id,
                edge_type,
                ArtifactDirection::Out,
                vertex_id,
                read_epoch,
            )
            .await?
        {
            let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
            return self.supernode_degree_with_overlay(&group, &overlay).await;
        }
        self.out_degree_at(cell_id, edge_type, vertex_id, read_epoch)
            .await
    }

    pub async fn supernode_page(
        &self,
        cell_id: &str,
        edge_type: &str,
        direction: ArtifactDirection,
        vertex_id: VertexId,
        read_epoch: GraphEpoch,
        page_id: u64,
    ) -> Result<Option<SupernodePage>> {
        let Some(group) = self
            .supernode_group(cell_id, edge_type, direction, vertex_id, read_epoch)
            .await?
        else {
            return Ok(None);
        };
        if group.page_size == 0 {
            return Err(GraphError::CorruptValue {
                key: supernode_group_key(&group),
                reason: "supernode page size must be greater than zero".to_string(),
            });
        }
        let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
        let page_size =
            usize::try_from(group.page_size).map_err(|err| GraphError::CorruptValue {
                key: supernode_group_key(&group),
                reason: format!("invalid supernode page size: {err}"),
            })?;
        let skip =
            page_id
                .checked_mul(group.page_size)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: supernode_group_key(&group),
                    reason: "supernode page offset overflow".to_string(),
                })?;
        let mut vertices = self
            .merged_supernode_vertices(&group, &overlay, skip, page_size.saturating_add(1))
            .await?;
        if vertices.is_empty() {
            return Ok(None);
        }
        let has_next = vertices.len() > page_size;
        vertices.truncate(page_size);
        Ok(Some(SupernodePage {
            vertex_id,
            edge_type: edge_type.to_string(),
            direction,
            page_id,
            has_next,
            vertices,
        }))
    }

    pub async fn supernode_edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<bool> {
        if let Some(group) = self
            .supernode_group(cell_id, edge_type, ArtifactDirection::Out, src, read_epoch)
            .await?
        {
            let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
            if overlay.plus.contains(&dst) {
                return Ok(true);
            }
            if overlay.minus.contains(&dst) {
                return Ok(false);
            }
            return self.supernode_base_contains(&group, dst).await;
        }
        self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch)
            .await
    }

    pub async fn supernode_intersection(
        &self,
        cell_id: &str,
        edge_type: &str,
        vertex_id: VertexId,
        candidates: &[VertexId],
        read_epoch: GraphEpoch,
    ) -> Result<Vec<VertexId>> {
        let mut result = BTreeSet::new();
        if let Some(group) = self
            .supernode_group(
                cell_id,
                edge_type,
                ArtifactDirection::Out,
                vertex_id,
                read_epoch,
            )
            .await?
        {
            let overlay = self.supernode_delta_overlay(&group, read_epoch).await?;
            for candidate in candidates.iter().copied().collect::<BTreeSet<_>>() {
                if overlay.plus.contains(&candidate)
                    || (!overlay.minus.contains(&candidate)
                        && self.supernode_base_contains(&group, candidate).await?)
                {
                    result.insert(candidate);
                }
            }
            return Ok(result.into_iter().collect());
        }
        let candidate_set: HashSet<VertexId> = candidates.iter().copied().collect();
        for dst in self
            .out_neighbors_at(cell_id, edge_type, vertex_id, read_epoch)
            .await?
        {
            if candidate_set.contains(&dst) {
                result.insert(dst);
            }
        }
        Ok(result.into_iter().collect())
    }

    async fn supernode_delta_overlay(
        &self,
        group: &SupernodeGroup,
        read_epoch: GraphEpoch,
    ) -> Result<SupernodeDeltaOverlay> {
        if read_epoch <= group.base_epoch {
            return Ok(SupernodeDeltaOverlay::default());
        }
        let mut overlay = SupernodeDeltaOverlay::default();
        for delta in self
            .deltas_between(
                &group.cell_id,
                &group.edge_type,
                group.base_epoch,
                read_epoch,
            )
            .await?
        {
            let (owner, neighbor) = match group.direction {
                ArtifactDirection::Out => (delta.edge.src, delta.edge.dst),
                ArtifactDirection::In => (delta.edge.dst, delta.edge.src),
            };
            if owner != group.vertex_id {
                continue;
            }
            match delta.kind {
                DeltaKind::Plus => {
                    overlay.minus.remove(&neighbor);
                    overlay.plus.insert(neighbor);
                }
                DeltaKind::Minus => {
                    overlay.plus.remove(&neighbor);
                    overlay.minus.insert(neighbor);
                }
            }
        }
        Ok(overlay)
    }

    async fn supernode_degree_with_overlay(
        &self,
        group: &SupernodeGroup,
        overlay: &SupernodeDeltaOverlay,
    ) -> Result<u64> {
        let mut degree = i128::from(group.degree);
        for vertex in &overlay.minus {
            if self.supernode_base_contains(group, *vertex).await? {
                degree -= 1;
            }
        }
        for vertex in &overlay.plus {
            if !self.supernode_base_contains(group, *vertex).await? {
                degree += 1;
            }
        }
        Ok(degree.max(0) as u64)
    }

    async fn supernode_base_contains(
        &self,
        group: &SupernodeGroup,
        vertex: VertexId,
    ) -> Result<bool> {
        if let Some(bound) = supernode_bound_for_vertex(group, vertex) {
            let Some(chunk) = self.posting_chunk(group, bound.chunk_id).await? else {
                return Err(GraphError::CorruptValue {
                    key: posting_chunk_key(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                        bound.chunk_id,
                    ),
                    reason: "missing supernode posting chunk".to_string(),
                });
            };
            return Ok(chunk.vertices.binary_search(&vertex).is_ok());
        }

        if !group.chunk_bounds.is_empty() {
            return Ok(false);
        }

        for chunk_id in 0..group.chunk_count {
            let Some(chunk) = self.posting_chunk(group, chunk_id).await? else {
                return Err(GraphError::CorruptValue {
                    key: posting_chunk_key(
                        &group.cell_id,
                        &group.edge_type,
                        group.direction,
                        group.vertex_id,
                        group.base_epoch,
                        chunk_id,
                    ),
                    reason: "missing supernode posting chunk".to_string(),
                });
            };
            let Some(first) = chunk.vertices.first() else {
                continue;
            };
            if vertex < *first {
                return Ok(false);
            }
            let Some(last) = chunk.vertices.last() else {
                continue;
            };
            if vertex <= *last {
                return Ok(chunk.vertices.binary_search(&vertex).is_ok());
            }
        }
        Ok(false)
    }

    async fn merged_supernode_vertices(
        &self,
        group: &SupernodeGroup,
        overlay: &SupernodeDeltaOverlay,
        skip: u64,
        limit: usize,
    ) -> Result<Vec<VertexId>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let plus: Vec<_> = overlay.plus.iter().copied().collect();
        let mut plus_idx = 0_usize;
        let mut chunk_id = 0_u64;
        let mut chunk_vertices = Vec::new();
        let mut chunk_idx = 0_usize;
        let mut pending_base = None;
        let mut skipped = 0_u64;
        let mut output = Vec::new();

        loop {
            while pending_base.is_none()
                && (chunk_idx < chunk_vertices.len() || chunk_id < group.chunk_count)
            {
                if chunk_idx >= chunk_vertices.len() {
                    let Some(chunk) = self.posting_chunk(group, chunk_id).await? else {
                        return Err(GraphError::CorruptValue {
                            key: posting_chunk_key(
                                &group.cell_id,
                                &group.edge_type,
                                group.direction,
                                group.vertex_id,
                                group.base_epoch,
                                chunk_id,
                            ),
                            reason: "missing supernode posting chunk".to_string(),
                        });
                    };
                    chunk_vertices = chunk.vertices;
                    chunk_idx = 0;
                    chunk_id += 1;
                }
                pending_base = next_live_base_vertex(&chunk_vertices, &mut chunk_idx, overlay);
            }

            let next_plus = plus.get(plus_idx).copied();
            let Some(next) = merge_next_vertex(pending_base, next_plus) else {
                break;
            };
            if next_plus == Some(next) {
                plus_idx += 1;
            }
            if pending_base == Some(next) {
                pending_base = None;
            }

            if skipped < skip {
                skipped += 1;
                continue;
            }
            output.push(next);
            if output.len() == limit {
                break;
            }
        }

        Ok(output)
    }

    async fn posting_chunk(
        &self,
        group: &SupernodeGroup,
        chunk_id: u64,
    ) -> Result<Option<PostingChunk>> {
        let cache_key = PostingChunkCacheKey::new(group, chunk_id);
        if let Some(cached) = self.posting_chunk_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::PostingChunk);
            return Ok(Some(cached));
        }

        self.cache_metrics.record_miss(GraphCacheKind::PostingChunk);
        let _permit = self.acquire_hydration_permit("posting_chunk").await?;
        let key = posting_chunk_key(
            &group.cell_id,
            &group.edge_type,
            group.direction,
            group.vertex_id,
            group.base_epoch,
            chunk_id,
        );
        let decoded = self
            .read_remote(&key)
            .await?
            .map(|value| decode_posting_chunk(&key, &value))
            .transpose()?;
        self.record_hydration_complete();
        if let Some(chunk) = decoded.clone() {
            self.posting_chunk_cache.lock().await.insert(
                cache_key,
                chunk,
                group.cell_id.clone(),
                self.cache_policy.pin_supernode_group(group),
                &self.cache_metrics,
            );
        }
        Ok(decoded)
    }

    async fn prefetch_supernode_chunks(&self, group: &SupernodeGroup) {
        let chunk_count = self
            .cache_policy
            .prefetch_supernode_chunks
            .min(group.chunk_count);
        if chunk_count == 0 {
            self.cache_metrics
                .prefetch_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        for chunk_id in 0..chunk_count {
            self.cache_metrics
                .prefetch_requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self.posting_chunk(group, chunk_id).await.is_err() {
                self.cache_metrics
                    .prefetch_skipped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    pub(crate) async fn cached_matrix_adjacency(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
    ) -> Result<Arc<MatrixAdjacency>> {
        let cache_key = MatrixCacheKey::new(cell_id, edge_type, base_epoch);
        if let Some(cached) = self.matrix_cache.lock().await.get(&cache_key) {
            self.cache_metrics
                .record_hit(GraphCacheKind::MatrixAdjacency);
            return Ok(cached);
        }

        self.cache_metrics
            .record_miss(GraphCacheKind::MatrixAdjacency);
        let _permit = self
            .acquire_hydration_permit("cached_matrix_adjacency")
            .await?;
        let adjacency = Arc::new(
            self.load_matrix_adjacency(cell_id, edge_type, base_epoch)
                .await?,
        );
        self.record_hydration_complete();
        let mut cache = self.matrix_cache.lock().await;
        Ok(cache
            .insert(
                cache_key,
                Arc::clone(&adjacency),
                cell_id.to_string(),
                adjacency_edge_count(adjacency.as_ref()) >= self.cache_policy.pin_matrix_min_edges,
                &self.cache_metrics,
            )
            .unwrap_or(adjacency))
    }

    async fn cached_graphblas_matrix(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
    ) -> Result<Arc<CompiledGraphBlasMatrix>> {
        let cache_key = MatrixCacheKey::new(cell_id, edge_type, base_epoch);
        if let Some(cached) = self.graphblas_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::GraphBlas);
            phase0_matrix_profile(
                phase0_matrix_profile_enabled(),
                "graphblas_cache_hit",
                Duration::ZERO,
                base_epoch,
            );
            return Ok(cached);
        }

        self.cache_metrics.record_miss(GraphCacheKind::GraphBlas);
        let started = Instant::now();
        let (compiled, compile_units) =
            if let Some(csc) = self.graphblas_csc(cell_id, edge_type, base_epoch).await? {
                let edge_count = csc.indices.len() as u64;
                (Arc::new(compile_graphblas_csc(&csc)?), edge_count)
            } else {
                let adjacency = self
                    .cached_matrix_adjacency(cell_id, edge_type, base_epoch)
                    .await?;
                let edge_count = adjacency.values().map(|dsts| dsts.len() as u64).sum();
                (
                    Arc::new(compile_graphblas_matrix(adjacency.as_ref())?),
                    edge_count,
                )
            };
        phase0_matrix_profile(
            phase0_matrix_profile_enabled(),
            "compile_graphblas_matrix",
            started.elapsed(),
            compile_units,
        );
        let mut cache = self.graphblas_cache.lock().await;
        Ok(cache
            .insert(
                cache_key,
                Arc::clone(&compiled),
                cell_id.to_string(),
                compile_units >= self.cache_policy.pin_matrix_min_edges,
                &self.cache_metrics,
            )
            .unwrap_or(compiled))
    }

    async fn graphblas_csc(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
    ) -> Result<Option<GraphBlasCsc>> {
        let key = graphblas_csc_key(cell_id, edge_type, base_epoch);
        let _permit = self.acquire_hydration_permit("graphblas_csc").await?;
        let Some(value) = self.read_remote(&key).await? else {
            self.record_hydration_complete();
            return Ok(None);
        };
        if value.starts_with(GRAPHBLAS_CSC_MAGIC) {
            let csc = decode_graphblas_csc(&key, &value, cell_id, edge_type, base_epoch)?;
            self.record_hydration_complete();
            return Ok(Some(csc));
        }

        let manifest = decode_graphblas_csc_manifest(&key, &value)?;
        if manifest.cell_id != cell_id
            || manifest.edge_type != edge_type
            || manifest.base_epoch != base_epoch
        {
            return corrupt(&key, "GraphBLAS CSC manifest identity does not match key");
        }

        let vertices = self
            .load_graphblas_csc_chunks(
                cell_id,
                edge_type,
                base_epoch,
                "vertices",
                manifest.vertex_chunks,
                manifest.vertices_len,
            )
            .await?;
        let pointers = self
            .load_graphblas_csc_chunks(
                cell_id,
                edge_type,
                base_epoch,
                "pointers",
                manifest.pointer_chunks,
                manifest.pointers_len,
            )
            .await?;
        let indices = self
            .load_graphblas_csc_chunks(
                cell_id,
                edge_type,
                base_epoch,
                "indices",
                manifest.index_chunks,
                manifest.indices_len,
            )
            .await?;
        let csc = GraphBlasCsc {
            vertices,
            pointers,
            indices,
        };
        if graphblas_csc_checksum(&csc) != manifest.checksum {
            return corrupt(&key, "GraphBLAS CSC checksum mismatch");
        }
        validate_graphblas_csc_artifact(&key, &csc)?;
        self.record_hydration_complete();
        Ok(Some(csc))
    }

    async fn load_graphblas_csc_chunks(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
        field: &'static str,
        chunk_count: u64,
        expected_len: u64,
    ) -> Result<Vec<u64>> {
        let mut values = Vec::new();
        for chunk_id in 0..chunk_count {
            let key = graphblas_csc_chunk_key(cell_id, edge_type, base_epoch, field, chunk_id);
            let Some(value) = self.read_remote(&key).await? else {
                return corrupt(&key, "missing GraphBLAS CSC chunk");
            };
            values.extend(decode_graphblas_csc_chunk(&key, &value, field, chunk_id)?);
        }
        if values.len() as u64 != expected_len {
            return corrupt(
                &graphblas_csc_key(cell_id, edge_type, base_epoch),
                format!(
                    "GraphBLAS CSC {field} length {} does not match manifest {expected_len}",
                    values.len()
                ),
            );
        }
        Ok(values)
    }

    async fn load_matrix_adjacency(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
    ) -> Result<MatrixAdjacency> {
        let mut adjacency: MatrixAdjacency = BTreeMap::new();
        let prefix = matrix_tile_prefix(cell_id, edge_type, base_epoch, ArtifactDirection::Out);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let tile = decode_matrix_tile(&key, &kv.value)?;
            for (src, dsts) in tile.rows {
                adjacency.entry(src).or_default().extend(dsts);
            }
        }
        Ok(adjacency)
    }
}

impl GraphControlPlane {
    pub async fn open(
        path: impl Into<slatedb::object_store::path::Path>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_with_cache(path, object_store, GraphCacheConfig::default()).await
    }

    pub async fn open_with_cache(
        path: impl Into<slatedb::object_store::path::Path>,
        object_store: Arc<dyn ObjectStore>,
        cache: GraphCacheConfig,
    ) -> Result<Self> {
        Ok(Self {
            db: open_graph_db(path, object_store, &cache).await?,
        })
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub async fn publish_placement(&self, placement: &ShardPlacement) -> Result<()> {
        let mut batch = WriteBatch::new();
        for (cell_id, node_id) in &placement.owners {
            validate_component("cell_id", cell_id)?;
            validate_component("node_id", node_id)?;
            batch.put(
                control_placement_key(cell_id),
                encode_control_placement(cell_id, node_id),
            );
        }
        self.write_strict(batch).await
    }

    pub async fn rebalance_rendezvous(
        &self,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        node_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<ShardPlacement> {
        let placement = ShardPlacement::rendezvous(cell_ids, node_ids)?;
        self.publish_placement(&placement).await?;
        Ok(placement)
    }

    pub async fn load_placement(&self) -> Result<ShardPlacement> {
        let mut iter = self
            .db
            .scan_prefix_with_options(
                CONTROL_PLACEMENT_PREFIX.as_bytes(),
                ..,
                &control_scan_options(),
            )
            .await?;
        let mut assignments = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            assignments.push(decode_control_placement(&key, &kv.value)?);
        }
        ShardPlacement::fixed(assignments)
    }

    pub async fn acquire_lease(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease> {
        self.acquire_lease_at(cell_id, node_id, ttl, now_millis())
            .await
    }

    pub async fn renew_lease(&self, lease: &ShardLease, ttl: Duration) -> Result<ShardLease> {
        self.renew_lease_at(lease, ttl, now_millis()).await
    }

    async fn acquire_lease_at(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<ShardLease> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", node_id)?;
        let ttl_ms = lease_ttl_ms(ttl)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .acquire_lease_txn(cell_id, node_id, ttl_ms, now_ms)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("control transaction retry loop always returns on final attempt")
    }

    async fn acquire_lease_txn(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ShardLease> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let placement_key = control_placement_key(cell_id);
        let owner = read_control_txn(&txn, &placement_key)
            .await?
            .map(|value| decode_control_placement(&placement_key, &value))
            .transpose()?
            .map(|(_, owner)| owner)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })?;
        if owner != node_id {
            return Err(GraphError::ShardNotOwned {
                cell_id: cell_id.to_string(),
                owner_node_id: owner,
                local_node_id: node_id.to_string(),
            });
        }

        let lease_key = control_lease_key(cell_id);
        if let Some(value) = read_control_txn(&txn, &lease_key).await? {
            let current = decode_shard_lease(&lease_key, &value)?;
            if current.owner_node_id != node_id && current.expires_at_ms > now_ms {
                return Err(GraphError::ShardLeaseHeld {
                    cell_id: cell_id.to_string(),
                    owner_node_id: current.owner_node_id,
                    expires_at_ms: current.expires_at_ms,
                });
            }
        }

        let token_key = control_lease_token_key(cell_id);
        let token = read_control_counter_txn(&txn, &token_key)
            .await?
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: token_key.clone(),
                reason: "lease token overflow".to_string(),
            })?;
        let lease = ShardLease {
            cell_id: cell_id.to_string(),
            owner_node_id: node_id.to_string(),
            lease_token: token,
            expires_at_ms: now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: lease_key.clone(),
                    reason: "lease expiry overflow".to_string(),
                })?,
        };
        txn.put(token_key.as_bytes(), encode_u64_be(token))?;
        txn.put(lease_key.as_bytes(), encode_shard_lease(&lease))?;
        commit_control_txn(txn).await?;
        Ok(lease)
    }

    async fn renew_lease_at(
        &self,
        lease: &ShardLease,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<ShardLease> {
        let ttl_ms = lease_ttl_ms(ttl)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self.renew_lease_txn(lease, ttl_ms, now_ms).await {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("control transaction retry loop always returns on final attempt")
    }

    async fn renew_lease_txn(
        &self,
        lease: &ShardLease,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ShardLease> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let lease_key = control_lease_key(&lease.cell_id);
        let Some(value) = read_control_txn(&txn, &lease_key).await? else {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        };
        let current = decode_shard_lease(&lease_key, &value)?;
        if current.owner_node_id != lease.owner_node_id
            || current.lease_token != lease.lease_token
            || current.expires_at_ms <= now_ms
        {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }

        let renewed = ShardLease {
            expires_at_ms: now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: lease_key.clone(),
                    reason: "lease expiry overflow".to_string(),
                })?,
            ..lease.clone()
        };
        txn.put(lease_key.as_bytes(), encode_shard_lease(&renewed))?;
        commit_control_txn(txn).await?;
        Ok(renewed)
    }

    pub async fn current_lease(&self, cell_id: &str) -> Result<Option<ShardLease>> {
        validate_component("cell_id", cell_id)?;
        let key = control_lease_key(cell_id);
        self.db
            .get_with_options(key.as_bytes(), &control_read_options())
            .await?
            .map(|value| decode_shard_lease(&key, &value))
            .transpose()
    }

    pub async fn failover_expired_cell(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease> {
        self.failover_expired_cell_at(cell_id, new_node_id, ttl, now_millis())
            .await
    }

    async fn failover_expired_cell_at(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<ShardLease> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", new_node_id)?;
        let ttl_ms = lease_ttl_ms(ttl)?;
        for attempt in 0..GRAPH_CONTROL_TXN_MAX_RETRIES {
            match self
                .failover_expired_cell_txn(cell_id, new_node_id, ttl_ms, now_ms)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_CONTROL_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("control transaction retry loop always returns on final attempt")
    }

    async fn failover_expired_cell_txn(
        &self,
        cell_id: &str,
        new_node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ShardLease> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let lease_key = control_lease_key(cell_id);
        if let Some(value) = read_control_txn(&txn, &lease_key).await? {
            let current = decode_shard_lease(&lease_key, &value)?;
            if current.owner_node_id != new_node_id && current.expires_at_ms > now_ms {
                return Err(GraphError::ShardLeaseHeld {
                    cell_id: cell_id.to_string(),
                    owner_node_id: current.owner_node_id,
                    expires_at_ms: current.expires_at_ms,
                });
            }
        }

        let token_key = control_lease_token_key(cell_id);
        let token = read_control_counter_txn(&txn, &token_key)
            .await?
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: token_key.clone(),
                reason: "lease token overflow".to_string(),
            })?;
        let lease = ShardLease {
            cell_id: cell_id.to_string(),
            owner_node_id: new_node_id.to_string(),
            lease_token: token,
            expires_at_ms: now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: lease_key.clone(),
                    reason: "lease expiry overflow".to_string(),
                })?,
        };
        txn.put(
            control_placement_key(cell_id).as_bytes(),
            encode_control_placement(cell_id, new_node_id),
        )?;
        txn.put(token_key.as_bytes(), encode_u64_be(token))?;
        txn.put(lease_key.as_bytes(), encode_shard_lease(&lease))?;
        commit_control_txn(txn).await?;
        Ok(lease)
    }

    async fn write_strict(&self, batch: WriteBatch) -> Result<()> {
        let mut options = WriteOptions::default();
        options.await_durable = true;
        self.db.write_with_options(batch, &options).await?;
        Ok(())
    }
}

impl LeaseRenewalHandle {
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        match self.task.await {
            Ok(result) => result,
            Err(err) => Err(GraphError::CorruptValue {
                key: "control/lease_renewer".to_string(),
                reason: format!("lease renewer task failed: {err}"),
            }),
        }
    }
}

impl GraphNode {
    pub async fn open(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: Arc<GraphControlPlane>,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
        lease_renew_interval: Duration,
    ) -> Result<Self> {
        Self::open_with_options(
            base_path,
            local_node_id,
            control,
            object_store,
            lease_ttl,
            lease_renew_interval,
            GraphOpenOptions::default(),
        )
        .await
    }

    pub async fn open_with_options(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: Arc<GraphControlPlane>,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
        lease_renew_interval: Duration,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let cluster = RoutedPhase0Cluster::open_owned_with_control_and_options(
            base_path,
            local_node_id,
            control.as_ref(),
            object_store,
            lease_ttl,
            options,
        )
        .await?;
        let lease_renewer =
            cluster.start_lease_renewer(control, lease_ttl, lease_renew_interval)?;
        Ok(Self {
            cluster,
            lease_renewer,
        })
    }

    pub fn cluster(&self) -> &RoutedPhase0Cluster {
        &self.cluster
    }

    pub async fn close(self) -> Result<()> {
        self.lease_renewer.stop().await?;
        self.cluster.close().await
    }
}

impl Phase0Cluster {
    pub async fn open_cells(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let mut shards = BTreeMap::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open(path, Arc::clone(&object_store)).await?;
            shards.insert(cell_id, shard);
        }
        Ok(Self { shards })
    }

    pub async fn open_cells_standalone_writers(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let mut shards = BTreeMap::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open_standalone_writer(path, Arc::clone(&object_store)).await?;
            shards.insert(cell_id, shard);
        }
        Ok(Self { shards })
    }

    pub fn shard(&self, cell_id: &str) -> Option<&GraphShard> {
        self.shards.get(cell_id)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub async fn close(&self) -> Result<()> {
        for shard in self.shards.values() {
            shard.close().await?;
        }
        Ok(())
    }
}

impl ShardPlacement {
    pub fn fixed(
        assignments: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self> {
        let mut owners = BTreeMap::new();
        for (cell_id, node_id) in assignments {
            let cell_id = cell_id.into();
            let node_id = node_id.into();
            validate_component("cell_id", &cell_id)?;
            validate_component("node_id", &node_id)?;
            if owners.insert(cell_id.clone(), node_id).is_some() {
                return Err(GraphError::CorruptValue {
                    key: format!("placement/{cell_id}"),
                    reason: "duplicate cell placement".to_string(),
                });
            }
        }
        if owners.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "placement".to_string(),
                reason: "at least one cell placement is required".to_string(),
            });
        }
        Ok(Self { owners })
    }

    pub fn rendezvous(
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        node_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let mut nodes = Vec::new();
        for node_id in node_ids {
            let node_id = node_id.into();
            validate_component("node_id", &node_id)?;
            nodes.push(node_id);
        }
        nodes.sort();
        nodes.dedup();
        if nodes.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "placement".to_string(),
                reason: "at least one owner node is required".to_string(),
            });
        }

        let mut owners = BTreeMap::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            let owner = nodes
                .iter()
                .max_by_key(|node_id| rendezvous_score(&cell_id, node_id))
                .expect("nodes is not empty")
                .clone();
            if owners.insert(cell_id.clone(), owner).is_some() {
                return Err(GraphError::CorruptValue {
                    key: format!("placement/{cell_id}"),
                    reason: "duplicate cell placement".to_string(),
                });
            }
        }
        if owners.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "placement".to_string(),
                reason: "at least one cell placement is required".to_string(),
            });
        }
        Ok(Self { owners })
    }

    pub fn owner(&self, cell_id: &str) -> Result<&str> {
        validate_component("cell_id", cell_id)?;
        self.owners
            .get(cell_id)
            .map(String::as_str)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
    }

    pub fn ensure_local_owner(&self, local_node_id: &str, cell_id: &str) -> Result<()> {
        validate_component("node_id", local_node_id)?;
        let owner = self.owner(cell_id)?;
        if owner == local_node_id {
            Ok(())
        } else {
            Err(GraphError::ShardNotOwned {
                cell_id: cell_id.to_string(),
                owner_node_id: owner.to_string(),
                local_node_id: local_node_id.to_string(),
            })
        }
    }

    pub fn cells_for_node(&self, node_id: &str) -> Result<Vec<String>> {
        validate_component("node_id", node_id)?;
        Ok(self
            .owners
            .iter()
            .filter_map(|(cell_id, owner)| (owner == node_id).then_some(cell_id.clone()))
            .collect())
    }

    pub fn cells(&self) -> impl Iterator<Item = &str> {
        self.owners.keys().map(String::as_str)
    }
}

impl RoutedPhase0Cluster {
    pub async fn open_owned(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        placement: ShardPlacement,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let local_node_id = local_node_id.into();
        validate_component("node_id", &local_node_id)?;

        let mut shards = BTreeMap::new();
        for cell_id in placement.cells_for_node(&local_node_id)? {
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open(path, Arc::clone(&object_store)).await?;
            shards.insert(cell_id, shard);
        }

        Ok(Self {
            local_node_id,
            placement,
            shards,
            leases: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    pub async fn open_owned_with_control(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: &GraphControlPlane,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
    ) -> Result<Self> {
        Self::open_owned_with_control_and_options(
            base_path,
            local_node_id,
            control,
            object_store,
            lease_ttl,
            GraphOpenOptions::default(),
        )
        .await
    }

    pub async fn open_owned_with_control_and_options(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        control: &GraphControlPlane,
        object_store: Arc<dyn ObjectStore>,
        lease_ttl: Duration,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let local_node_id = local_node_id.into();
        let placement = control.load_placement().await?;
        validate_component("node_id", &local_node_id)?;
        let local_cells = placement.cells_for_node(&local_node_id)?;
        let leases = Arc::new(RwLock::new(BTreeMap::new()));
        for cell_id in &local_cells {
            let lease = control
                .acquire_lease(cell_id, &local_node_id, lease_ttl)
                .await?;
            leases
                .write()
                .map_err(lock_error)?
                .insert(cell_id.clone(), lease);
        }

        let mut shards = BTreeMap::new();
        for cell_id in local_cells {
            let path = format!("{base_path}/{cell_id}");
            let shard = GraphShard::open_leased_writer(
                path,
                Arc::clone(&object_store),
                options.clone(),
                local_node_id.clone(),
                Arc::clone(&leases),
            )
            .await?;
            shards.insert(cell_id, shard);
        }

        Ok(Self {
            local_node_id,
            placement,
            shards,
            leases,
        })
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn placement(&self) -> &ShardPlacement {
        &self.placement
    }

    pub fn local_cells(&self) -> Vec<&str> {
        self.shards.keys().map(String::as_str).collect()
    }

    pub fn lease(&self, cell_id: &str) -> Option<ShardLease> {
        self.leases
            .read()
            .ok()
            .and_then(|leases| leases.get(cell_id).cloned())
    }

    pub async fn renew_leases(
        &mut self,
        control: &GraphControlPlane,
        lease_ttl: Duration,
    ) -> Result<()> {
        let leases: Vec<_> = self
            .leases
            .read()
            .map_err(lock_error)?
            .values()
            .cloned()
            .collect();
        for lease in leases {
            let renewed = control.renew_lease(&lease, lease_ttl).await?;
            self.leases
                .write()
                .map_err(lock_error)?
                .insert(renewed.cell_id.clone(), renewed);
        }
        Ok(())
    }

    pub fn start_lease_renewer(
        &self,
        control: Arc<GraphControlPlane>,
        lease_ttl: Duration,
        interval: Duration,
    ) -> Result<LeaseRenewalHandle> {
        if interval.is_zero() {
            return Err(GraphError::CorruptValue {
                key: "control/lease_renew_interval".to_string(),
                reason: "lease renewal interval must be greater than zero".to_string(),
            });
        }
        let leases = Arc::clone(&self.leases);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(interval) => {
                        let current: Vec<_> = leases
                            .read()
                            .map_err(lock_error)?
                            .values()
                            .cloned()
                            .collect();
                        for lease in current {
                            let renewed = control.renew_lease(&lease, lease_ttl).await?;
                            leases
                                .write()
                                .map_err(lock_error)?
                                .insert(renewed.cell_id.clone(), renewed);
                        }
                    }
                }
            }
            Ok(())
        });
        Ok(LeaseRenewalHandle { stop_tx, task })
    }

    pub fn shard(&self, cell_id: &str) -> Result<&GraphShard> {
        self.placement
            .ensure_local_owner(&self.local_node_id, cell_id)?;
        self.shards
            .get(cell_id)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
    }

    fn ensure_active_write_lease(&self, cell_id: &str) -> Result<()> {
        if let Some(lease) = self
            .leases
            .read()
            .map_err(lock_error)?
            .get(cell_id)
            .cloned()
        {
            if lease.owner_node_id == self.local_node_id && lease.expires_at_ms > now_millis() {
                return Ok(());
            }
            return Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: self.local_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }
        Err(GraphError::WriteRequiresLease {
            operation: "routed_write",
            cell_id: cell_id.to_string(),
        })
    }

    pub async fn write_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::CommitResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_active_write_lease(&mutation.cell_id)?;
        shard.write_edge(mutation).await
    }

    pub async fn delete_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::DeleteResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_active_write_lease(&mutation.cell_id)?;
        shard.delete_edge(mutation).await
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_active_write_lease(cell_id)?;
        shard
            .bulk_import_edges(cell_id, edge_type, edges, idempotency_key)
            .await
    }

    pub async fn write_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_active_write_lease(cell_id)?;
        shard
            .write_edges_batch(cell_id, edge_type, edges, idempotency_key)
            .await
    }

    pub async fn write_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_active_write_lease(cell_id)?;
        shard
            .write_edges_batch_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn close(&self) -> Result<()> {
        for shard in self.shards.values() {
            shard.close().await?;
        }
        Ok(())
    }
}

pub fn local_object_store(path: impl AsRef<std::path::Path>) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(
        path.as_ref().to_path_buf(),
    )?) as Arc<dyn ObjectStore>)
}

pub fn object_store_from_env(env_file: Option<String>) -> Result<Arc<dyn ObjectStore>> {
    Ok(slatedb::admin::load_object_store_from_env(env_file)?)
}

const GRAPH_CONTROL_TXN_MAX_RETRIES: usize = 32;
const CONTROL_PLACEMENT_PREFIX: &str = "control/placement/";

fn control_placement_key(cell_id: &str) -> String {
    format!("{CONTROL_PLACEMENT_PREFIX}{cell_id}")
}

fn control_lease_key(cell_id: &str) -> String {
    format!("control/lease/{cell_id}")
}

fn control_lease_token_key(cell_id: &str) -> String {
    format!("control/lease_token/{cell_id}")
}

fn encode_control_placement(cell_id: &str, owner_node_id: &str) -> Vec<u8> {
    format!("placement1\t{cell_id}\t{owner_node_id}\n").into_bytes()
}

fn decode_control_placement(key: &str, value: &[u8]) -> Result<(String, String)> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 3 || parts[0] != "placement1" {
        return corrupt(key, "expected placement1 record with 3 fields");
    }
    validate_component("cell_id", parts[1])?;
    validate_component("node_id", parts[2])?;
    Ok((parts[1].to_string(), parts[2].to_string()))
}

fn encode_shard_lease(lease: &ShardLease) -> Vec<u8> {
    format!(
        "lease1\t{}\t{}\t{}\t{}\n",
        lease.cell_id, lease.owner_node_id, lease.lease_token, lease.expires_at_ms
    )
    .into_bytes()
}

fn decode_shard_lease(key: &str, value: &[u8]) -> Result<ShardLease> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "lease1" {
        return corrupt(key, "expected lease1 record with 5 fields");
    }
    validate_component("cell_id", parts[1])?;
    validate_component("node_id", parts[2])?;
    Ok(ShardLease {
        cell_id: parts[1].to_string(),
        owner_node_id: parts[2].to_string(),
        lease_token: parse_u64(key, parts[3], "lease_token")?,
        expires_at_ms: parse_u64(key, parts[4], "expires_at_ms")?,
    })
}

async fn read_control_txn(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    txn.mark_read([key.as_bytes()])?;
    Ok(txn
        .get_with_options(key.as_bytes(), &control_read_options())
        .await?)
}

async fn read_control_counter_txn(txn: &DbTransaction, key: &str) -> Result<u64> {
    match read_control_txn(txn, key).await? {
        Some(value) => decode_u64_be(key, &value),
        None => Ok(0),
    }
}

async fn commit_control_txn(txn: DbTransaction) -> Result<()> {
    let mut options = WriteOptions::default();
    options.await_durable = true;
    txn.commit_with_options(&options).await?;
    Ok(())
}

fn control_read_options() -> ReadOptions {
    let mut options = ReadOptions::default();
    options.durability_filter = DurabilityLevel::Remote;
    options
}

fn control_scan_options() -> ScanOptions {
    ScanOptions::default()
        .with_durability_filter(DurabilityLevel::Remote)
        .with_cache_blocks(false)
}

fn encode_u64_be(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_u64_be(key: &str, value: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("expected 8 bytes, got {}", value.len()),
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn lease_ttl_ms(ttl: Duration) -> Result<u64> {
    u64::try_from(ttl.as_millis()).map_err(|err| GraphError::CorruptValue {
        key: "control/lease_ttl".to_string(),
        reason: format!("lease ttl is too large: {err}"),
    })
}

fn now_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> GraphError {
    GraphError::CorruptValue {
        key: "control/lease_lock".to_string(),
        reason: "lease state lock poisoned".to_string(),
    }
}

fn append_posting_chunks(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
    chunk_size: usize,
    mut adjacency: BTreeMap<VertexId, Vec<VertexId>>,
    chunks: &mut Vec<PostingChunk>,
) {
    for (owner, vertices) in adjacency.iter_mut() {
        vertices.sort_unstable();
        vertices.dedup();
        for (chunk_id, chunk) in vertices.chunks(chunk_size).enumerate() {
            chunks.push(PostingChunk {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                direction,
                owner: *owner,
                base_epoch,
                chunk_id: chunk_id as u64,
                vertices: chunk.to_vec(),
            });
        }
    }
}

fn append_current_supernode_chunk(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
    owner: VertexId,
    chunks: &mut Vec<PostingChunk>,
    chunk_bounds: &mut Vec<SupernodeChunkBound>,
    vertices: &mut Vec<VertexId>,
) {
    let chunk_id = chunk_bounds.len() as u64;
    if let (Some(first), Some(last)) = (vertices.first().copied(), vertices.last().copied()) {
        chunk_bounds.push(SupernodeChunkBound {
            chunk_id,
            first,
            last,
        });
    }
    chunks.push(PostingChunk {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        direction,
        owner,
        base_epoch,
        chunk_id,
        vertices: std::mem::take(vertices),
    });
}

fn append_supernode_chunks(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
    degree_threshold: u64,
    chunk_size: usize,
    mut adjacency: BTreeMap<VertexId, Vec<VertexId>>,
    chunks: &mut Vec<PostingChunk>,
    groups: &mut Vec<SupernodeGroup>,
) {
    for (owner, vertices) in adjacency.iter_mut() {
        vertices.sort_unstable();
        vertices.dedup();
        let degree = vertices.len() as u64;
        if degree < degree_threshold {
            continue;
        }
        let first_chunk_id = chunks.len() as u64;
        let mut chunk_bounds = Vec::new();
        for (offset, chunk) in vertices.chunks(chunk_size).enumerate() {
            let chunk_id = offset as u64;
            if let (Some(first), Some(last)) = (chunk.first(), chunk.last()) {
                chunk_bounds.push(SupernodeChunkBound {
                    chunk_id,
                    first: *first,
                    last: *last,
                });
            }
            chunks.push(PostingChunk {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                direction,
                owner: *owner,
                base_epoch,
                chunk_id,
                vertices: chunk.to_vec(),
            });
        }
        groups.push(SupernodeGroup {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            direction,
            vertex_id: *owner,
            base_epoch,
            degree,
            chunk_count: (chunks.len() as u64).saturating_sub(first_chunk_id),
            page_size: chunk_size as u64,
            chunk_bounds,
        });
    }
}

fn supernode_chunk_bounds(chunks: &[&PostingChunk]) -> Vec<SupernodeChunkBound> {
    let mut bounds: Vec<_> = chunks
        .iter()
        .filter_map(|chunk| {
            let first = chunk.vertices.first().copied()?;
            let last = chunk.vertices.last().copied()?;
            Some(SupernodeChunkBound {
                chunk_id: chunk.chunk_id,
                first,
                last,
            })
        })
        .collect();
    bounds.sort_by_key(|bound| (bound.first, bound.last, bound.chunk_id));
    bounds
}

fn supernode_bound_for_vertex(
    group: &SupernodeGroup,
    vertex: VertexId,
) -> Option<&SupernodeChunkBound> {
    let idx = group
        .chunk_bounds
        .partition_point(|bound| bound.last < vertex);
    let bound = group.chunk_bounds.get(idx)?;
    (bound.first <= vertex && vertex <= bound.last).then_some(bound)
}

fn rendezvous_score(cell_id: &str, node_id: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    cell_id.hash(&mut hasher);
    node_id.hash(&mut hasher);
    hasher.finish()
}

fn matrix_tiles_from_edges(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    tile_size: u64,
    direction: ArtifactDirection,
    edges: &[EdgeRecord],
) -> Vec<MatrixTile> {
    let mut tiles: BTreeMap<(u64, u64), BTreeMap<VertexId, Vec<VertexId>>> = BTreeMap::new();
    for edge in edges {
        let (row, col) = match direction {
            ArtifactDirection::Out => (edge.src, edge.dst),
            ArtifactDirection::In => (edge.dst, edge.src),
        };
        let key = (row / tile_size, col / tile_size);
        tiles
            .entry(key)
            .or_default()
            .entry(row)
            .or_default()
            .push(col);
    }
    tiles
        .into_iter()
        .map(|((tile_row, tile_col), mut rows)| {
            for values in rows.values_mut() {
                values.sort_unstable();
                values.dedup();
            }
            MatrixTile {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                base_epoch,
                tile_size,
                direction,
                tile_row,
                tile_col,
                rows,
            }
        })
        .collect()
}

fn adjacency_from_edges(edges: &[EdgeRecord]) -> MatrixAdjacency {
    let mut adjacency = MatrixAdjacency::new();
    for edge in edges {
        adjacency.entry(edge.src).or_default().insert(edge.dst);
    }
    adjacency
}

fn adjacency_edge_count(adjacency: &MatrixAdjacency) -> u64 {
    adjacency.values().map(|dsts| dsts.len() as u64).sum()
}

fn apply_delta_overlay(
    adjacency: &mut BTreeMap<VertexId, BTreeSet<VertexId>>,
    deltas: Vec<DeltaRecord>,
    base_epoch: GraphEpoch,
    read_epoch: GraphEpoch,
) -> u64 {
    let mut applied = 0_u64;
    for delta in deltas {
        if delta.edge.epoch <= base_epoch || delta.edge.epoch > read_epoch {
            continue;
        }
        applied += 1;
        match delta.kind {
            DeltaKind::Plus => {
                adjacency
                    .entry(delta.edge.src)
                    .or_default()
                    .insert(delta.edge.dst);
            }
            DeltaKind::Minus => {
                if let Some(row) = adjacency.get_mut(&delta.edge.src) {
                    row.remove(&delta.edge.dst);
                }
            }
        }
    }
    applied
}

fn next_live_base_vertex(
    vertices: &[VertexId],
    index: &mut usize,
    overlay: &SupernodeDeltaOverlay,
) -> Option<VertexId> {
    while let Some(vertex) = vertices.get(*index).copied() {
        *index += 1;
        if !overlay.minus.contains(&vertex) {
            return Some(vertex);
        }
    }
    None
}

fn merge_next_vertex(base: Option<VertexId>, plus: Option<VertexId>) -> Option<VertexId> {
    match (base, plus) {
        (Some(base), Some(plus)) => Some(base.min(plus)),
        (Some(base), None) => Some(base),
        (None, Some(plus)) => Some(plus),
        (None, None) => None,
    }
}

fn direction_str(direction: ArtifactDirection) -> &'static str {
    match direction {
        ArtifactDirection::Out => "out",
        ArtifactDirection::In => "in",
    }
}

fn parse_direction(value: &str) -> Result<ArtifactDirection> {
    match value {
        "out" => Ok(ArtifactDirection::Out),
        "in" => Ok(ArtifactDirection::In),
        other => Err(GraphError::CorruptValue {
            key: "direction".to_string(),
            reason: format!("invalid artifact direction {other}"),
        }),
    }
}

fn parse_last_key_component(key: &str, field: &str) -> Result<u64> {
    let Some(value) = key.rsplit('/').next() else {
        return corrupt(key, "missing key component");
    };
    parse_u64(key, value, field)
}

fn posting_key(chunk: &PostingChunk) -> String {
    posting_chunk_key(
        &chunk.cell_id,
        &chunk.edge_type,
        chunk.direction,
        chunk.owner,
        chunk.base_epoch,
        chunk.chunk_id,
    )
}

fn posting_chunk_key(
    cell_id: &str,
    edge_type: &str,
    direction: ArtifactDirection,
    owner: VertexId,
    base_epoch: GraphEpoch,
    chunk_id: u64,
) -> String {
    format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/{}/{owner:020}/{base_epoch:020}/{chunk_id:020}",
        direction_str(direction)
    )
}

fn posting_prefix(
    cell_id: &str,
    edge_type: &str,
    direction: ArtifactDirection,
    owner: VertexId,
    base_epoch: GraphEpoch,
) -> String {
    format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/{}/{owner:020}/{base_epoch:020}/",
        direction_str(direction)
    )
}

fn encode_posting_chunk(chunk: &PostingChunk) -> Vec<u8> {
    format!(
        "posting1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        chunk.cell_id,
        chunk.edge_type,
        direction_str(chunk.direction),
        chunk.owner,
        chunk.base_epoch,
        chunk.chunk_id,
        encode_vertices(&chunk.vertices)
    )
    .into_bytes()
}

fn decode_posting_chunk(key: &str, value: &[u8]) -> Result<PostingChunk> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "posting1" {
        return corrupt(key, "expected posting1 record with 8 fields");
    }
    Ok(PostingChunk {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        direction: parse_direction(parts[3])?,
        owner: parse_u64(key, parts[4], "owner")?,
        base_epoch: parse_u64(key, parts[5], "base_epoch")?,
        chunk_id: parse_u64(key, parts[6], "chunk_id")?,
        vertices: decode_vertices(key, parts[7])?,
    })
}

fn matrix_manifest_key(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> String {
    format!("cell/{cell_id}/artifact/matrix_manifest/{edge_type}/{base_epoch:020}")
}

fn matrix_manifest_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/matrix_manifest/{edge_type}/")
}

fn matrix_tile_key(tile: &MatrixTile) -> String {
    format!(
        "cell/{}/artifact/matrix/{}/{:020}/{}/{:020}/{:020}",
        tile.cell_id,
        tile.edge_type,
        tile.base_epoch,
        direction_str(tile.direction),
        tile.tile_row,
        tile.tile_col
    )
}

fn matrix_tile_prefix(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
) -> String {
    format!(
        "cell/{cell_id}/artifact/matrix/{edge_type}/{base_epoch:020}/{}/",
        direction_str(direction)
    )
}

fn graphblas_csc_key(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc/{edge_type}/{base_epoch:020}")
}

fn graphblas_csc_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc/{edge_type}/")
}

fn graphblas_csc_chunk_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/")
}

fn graphblas_csc_chunk_key(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    field: &str,
    chunk_id: u64,
) -> String {
    format!(
        "cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{base_epoch:020}/{field}/{chunk_id:020}"
    )
}

fn rollup_key(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> String {
    format!("cell/{cell_id}/rollup/{edge_type}/{base_epoch:020}")
}

fn rollup_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/rollup/{edge_type}/")
}

fn graph_artifact_gc_prefixes(cell_id: &str, edge_type: &str) -> Vec<String> {
    vec![
        format!("cell/{cell_id}/artifact/posting/{edge_type}/"),
        matrix_manifest_prefix(cell_id, edge_type),
        format!("cell/{cell_id}/artifact/matrix/{edge_type}/"),
        graphblas_csc_prefix(cell_id, edge_type),
        graphblas_csc_chunk_prefix(cell_id, edge_type),
        format!("cell/{cell_id}/artifact/supernode/{edge_type}/"),
        rollup_prefix(cell_id, edge_type),
    ]
}

fn graph_artifact_epoch_from_key(key: &str) -> Result<Option<GraphEpoch>> {
    let parts: Vec<_> = key.split('/').collect();
    let epoch = match parts.as_slice() {
        ["cell", _, "artifact", "posting", _, _, _, base_epoch, ..] => Some(*base_epoch),
        ["cell", _, "artifact", "matrix_manifest", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "matrix", _, base_epoch, ..] => Some(*base_epoch),
        ["cell", _, "artifact", "graphblas_csc", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "graphblas_csc_chunk", _, base_epoch, ..] => Some(*base_epoch),
        ["cell", _, "artifact", "supernode", _, _, _, base_epoch] => Some(*base_epoch),
        ["cell", _, "rollup", _, base_epoch] => Some(*base_epoch),
        _ => None,
    };
    epoch
        .map(|value| parse_u64(key, value, "base_epoch"))
        .transpose()
}

const GRAPH_ARTIFACT_WRITE_BATCH_KEYS: usize = 512;
const GRAPH_ARTIFACT_GC_BATCH_KEYS: usize = 512;

async fn put_artifact_record(
    shard: &GraphShard,
    batch: &mut WriteBatch,
    pending_writes: &mut usize,
    key: String,
    value: Vec<u8>,
) -> Result<()> {
    batch.put(key, value);
    *pending_writes += 1;
    if *pending_writes >= GRAPH_ARTIFACT_WRITE_BATCH_KEYS {
        flush_artifact_put_batch(shard, batch, pending_writes).await?;
    }
    Ok(())
}

async fn flush_artifact_put_batch(
    shard: &GraphShard,
    batch: &mut WriteBatch,
    pending_writes: &mut usize,
) -> Result<()> {
    if *pending_writes == 0 {
        return Ok(());
    }
    let batch_to_write = std::mem::replace(batch, WriteBatch::new());
    shard.write_strict(batch_to_write).await?;
    *pending_writes = 0;
    Ok(())
}

async fn flush_artifact_gc_batch(
    shard: &GraphShard,
    batch: &mut WriteBatch,
    pending_deletes: &mut usize,
) -> Result<()> {
    if *pending_deletes == 0 {
        return Ok(());
    }
    let batch_to_write = std::mem::replace(batch, WriteBatch::new());
    shard.write_strict(batch_to_write).await?;
    *pending_deletes = 0;
    Ok(())
}

fn encode_matrix_artifact(artifact: &MatrixArtifact) -> Vec<u8> {
    format!(
        "matrix_manifest1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        artifact.cell_id,
        artifact.edge_type,
        artifact.base_epoch,
        artifact.tile_size,
        artifact.out_tiles,
        artifact.transpose_tiles,
        artifact.edge_count
    )
    .into_bytes()
}

fn decode_matrix_artifact(key: &str, value: &[u8]) -> Result<MatrixArtifact> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "matrix_manifest1" {
        return corrupt(key, "expected matrix_manifest1 record with 8 fields");
    }
    Ok(MatrixArtifact {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        tile_size: parse_u64(key, parts[4], "tile_size")?,
        out_tiles: parse_u64(key, parts[5], "out_tiles")?,
        transpose_tiles: parse_u64(key, parts[6], "transpose_tiles")?,
        edge_count: parse_u64(key, parts[7], "edge_count")?,
    })
}

fn encode_graph_rollup(rollup: &GraphRollup) -> Vec<u8> {
    format!(
        "graph_rollup1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        rollup.cell_id,
        rollup.edge_type,
        rollup.base_epoch,
        rollup.posting_chunks,
        rollup.matrix_edge_count,
        rollup.supernode_groups
    )
    .into_bytes()
}

fn decode_graph_rollup(key: &str, value: &[u8]) -> Result<GraphRollup> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "graph_rollup1" {
        return corrupt(key, "expected graph_rollup1 record with 7 fields");
    }
    Ok(GraphRollup {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        posting_chunks: parse_u64(key, parts[4], "posting_chunks")?,
        matrix_edge_count: parse_u64(key, parts[5], "matrix_edge_count")?,
        supernode_groups: parse_u64(key, parts[6], "supernode_groups")?,
    })
}

const GRAPHBLAS_CSC_MAGIC: &[u8] = b"graphblas_csc1\n";
const GRAPHBLAS_CSC_MANIFEST_MAGIC: &str = "graphblas_csc_manifest2";
const GRAPHBLAS_CSC_CHUNK_MAGIC: &[u8] = b"graphblas_csc_chunk1\n";
const GRAPHBLAS_CSC_CHUNK_U64S: usize = 64 * 1024;

async fn append_graphblas_csc_chunks(
    shard: &GraphShard,
    batch: &mut WriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    csc: &GraphBlasCsc,
) -> Result<GraphBlasCscManifest> {
    let vertex_chunks = append_graphblas_csc_field_chunks(
        shard,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "vertices",
        &csc.vertices,
    )
    .await?;
    let pointer_chunks = append_graphblas_csc_field_chunks(
        shard,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "pointers",
        &csc.pointers,
    )
    .await?;
    let index_chunks = append_graphblas_csc_field_chunks(
        shard,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "indices",
        &csc.indices,
    )
    .await?;
    Ok(GraphBlasCscManifest {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        base_epoch,
        chunk_size: GRAPHBLAS_CSC_CHUNK_U64S as u64,
        vertices_len: csc.vertices.len() as u64,
        pointers_len: csc.pointers.len() as u64,
        indices_len: csc.indices.len() as u64,
        vertex_chunks,
        pointer_chunks,
        index_chunks,
        checksum: graphblas_csc_checksum(csc),
    })
}

async fn append_graphblas_csc_field_chunks(
    shard: &GraphShard,
    batch: &mut WriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    field: &'static str,
    values: &[u64],
) -> Result<u64> {
    let mut chunks = 0_u64;
    for (chunk_id, chunk) in values.chunks(GRAPHBLAS_CSC_CHUNK_U64S).enumerate() {
        put_artifact_record(
            shard,
            batch,
            pending_writes,
            graphblas_csc_chunk_key(cell_id, edge_type, base_epoch, field, chunk_id as u64),
            encode_graphblas_csc_chunk(field, chunk_id as u64, chunk),
        )
        .await?;
        chunks += 1;
    }
    Ok(chunks)
}

fn encode_graphblas_csc_manifest(manifest: &GraphBlasCscManifest) -> Vec<u8> {
    format!(
        "{GRAPHBLAS_CSC_MANIFEST_MAGIC}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        manifest.cell_id,
        manifest.edge_type,
        manifest.base_epoch,
        manifest.chunk_size,
        manifest.vertices_len,
        manifest.pointers_len,
        manifest.indices_len,
        manifest.vertex_chunks,
        manifest.pointer_chunks,
        manifest.index_chunks,
        manifest.checksum
    )
    .into_bytes()
}

fn decode_graphblas_csc_manifest(key: &str, value: &[u8]) -> Result<GraphBlasCscManifest> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 12 || parts[0] != GRAPHBLAS_CSC_MANIFEST_MAGIC {
        return corrupt(
            key,
            "expected graphblas_csc_manifest2 record with 12 fields",
        );
    }
    let manifest = GraphBlasCscManifest {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        chunk_size: parse_u64(key, parts[4], "chunk_size")?,
        vertices_len: parse_u64(key, parts[5], "vertices_len")?,
        pointers_len: parse_u64(key, parts[6], "pointers_len")?,
        indices_len: parse_u64(key, parts[7], "indices_len")?,
        vertex_chunks: parse_u64(key, parts[8], "vertex_chunks")?,
        pointer_chunks: parse_u64(key, parts[9], "pointer_chunks")?,
        index_chunks: parse_u64(key, parts[10], "index_chunks")?,
        checksum: parse_u64(key, parts[11], "checksum")?,
    };
    if manifest.chunk_size != GRAPHBLAS_CSC_CHUNK_U64S as u64 {
        return corrupt(key, "unsupported GraphBLAS CSC chunk size");
    }
    if expected_chunk_count(manifest.vertices_len) != manifest.vertex_chunks
        || expected_chunk_count(manifest.pointers_len) != manifest.pointer_chunks
        || expected_chunk_count(manifest.indices_len) != manifest.index_chunks
    {
        return corrupt(key, "GraphBLAS CSC manifest chunk count mismatch");
    }
    Ok(manifest)
}

fn expected_chunk_count(len: u64) -> u64 {
    if len == 0 {
        0
    } else {
        len.div_ceil(GRAPHBLAS_CSC_CHUNK_U64S as u64)
    }
}

fn decode_graphblas_csc(
    key: &str,
    value: &[u8],
    expected_cell_id: &str,
    expected_edge_type: &str,
    expected_base_epoch: GraphEpoch,
) -> Result<GraphBlasCsc> {
    if !value.starts_with(GRAPHBLAS_CSC_MAGIC) {
        return corrupt(key, "expected graphblas_csc1 binary artifact");
    }
    let mut cursor = GRAPHBLAS_CSC_MAGIC.len();
    let cell_id = decode_binary_string(key, value, &mut cursor, "cell_id")?;
    let edge_type = decode_binary_string(key, value, &mut cursor, "edge_type")?;
    let base_epoch = decode_binary_u64(key, value, &mut cursor, "base_epoch")?;
    if cell_id != expected_cell_id
        || edge_type != expected_edge_type
        || base_epoch != expected_base_epoch
    {
        return corrupt(key, "GraphBLAS CSC identity does not match key");
    }
    let vertices = decode_binary_u64s(key, value, &mut cursor, "vertices")?;
    let pointers = decode_binary_u64s(key, value, &mut cursor, "pointers")?;
    let indices = decode_binary_u64s(key, value, &mut cursor, "indices")?;
    if cursor != value.len() {
        return corrupt(key, "trailing bytes in graphblas CSC artifact");
    }
    let csc = GraphBlasCsc {
        vertices,
        pointers,
        indices,
    };
    validate_graphblas_csc_artifact(key, &csc)?;
    Ok(csc)
}

fn encode_graphblas_csc_chunk(field: &str, chunk_id: u64, values: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(GRAPHBLAS_CSC_CHUNK_MAGIC);
    encode_binary_string(&mut out, field);
    encode_binary_u64(&mut out, chunk_id);
    encode_binary_u64s(&mut out, values);
    out
}

fn decode_graphblas_csc_chunk(
    key: &str,
    value: &[u8],
    expected_field: &str,
    expected_chunk_id: u64,
) -> Result<Vec<u64>> {
    if !value.starts_with(GRAPHBLAS_CSC_CHUNK_MAGIC) {
        return corrupt(key, "expected graphblas_csc_chunk1 binary artifact");
    }
    let mut cursor = GRAPHBLAS_CSC_CHUNK_MAGIC.len();
    let field = decode_binary_string(key, value, &mut cursor, "field")?;
    let chunk_id = decode_binary_u64(key, value, &mut cursor, "chunk_id")?;
    if field != expected_field || chunk_id != expected_chunk_id {
        return corrupt(key, "GraphBLAS CSC chunk identity does not match key");
    }
    let values = decode_binary_u64s(key, value, &mut cursor, "values")?;
    if cursor != value.len() {
        return corrupt(key, "trailing bytes in GraphBLAS CSC chunk");
    }
    Ok(values)
}

fn validate_graphblas_csc_artifact(key: &str, csc: &GraphBlasCsc) -> Result<()> {
    if csc.pointers.len() != csc.vertices.len() + 1 {
        return corrupt(key, "CSC pointer count does not match vertex count");
    }
    if csc.pointers.first().copied() != Some(0) {
        return corrupt(key, "CSC first pointer must be zero");
    }
    for window in csc.pointers.windows(2) {
        if window[0] > window[1] {
            return corrupt(key, "CSC pointers must be monotonic");
        }
    }
    if csc.pointers.last().copied().unwrap_or(0) as usize != csc.indices.len() {
        return corrupt(key, "CSC edge count does not match index count");
    }
    if let Some(index) = csc
        .indices
        .iter()
        .copied()
        .find(|index| *index >= csc.vertices.len() as u64)
    {
        return corrupt(key, format!("CSC index {index} exceeds vertex count"));
    }
    for window in csc.vertices.windows(2) {
        if window[0] >= window[1] {
            return corrupt(key, "CSC vertices must be sorted and unique");
        }
    }
    Ok(())
}

fn graphblas_csc_checksum(csc: &GraphBlasCsc) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    checksum_u64(&mut hash, 0x01);
    for value in &csc.vertices {
        checksum_u64(&mut hash, *value);
    }
    checksum_u64(&mut hash, 0x02);
    for value in &csc.pointers {
        checksum_u64(&mut hash, *value);
    }
    checksum_u64(&mut hash, 0x03);
    for value in &csc.indices {
        checksum_u64(&mut hash, *value);
    }
    hash
}

fn checksum_u64(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn encode_matrix_tile(tile: &MatrixTile) -> Vec<u8> {
    let rows = tile
        .rows
        .iter()
        .map(|(row, cols)| format!("{row}:{}", encode_vertices(cols)))
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "matrix_tile1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        tile.cell_id,
        tile.edge_type,
        tile.base_epoch,
        tile.tile_size,
        direction_str(tile.direction),
        tile.tile_row,
        tile.tile_col,
        rows
    )
    .into_bytes()
}

fn decode_matrix_tile(key: &str, value: &[u8]) -> Result<MatrixTile> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 9 || parts[0] != "matrix_tile1" {
        return corrupt(key, "expected matrix_tile1 record with 9 fields");
    }
    let mut rows = BTreeMap::new();
    if !parts[8].is_empty() {
        for row in parts[8].split(';') {
            let Some((src, dsts)) = row.split_once(':') else {
                return corrupt(key, "invalid matrix row encoding");
            };
            rows.insert(
                parse_u64(key, src, "matrix_row")?,
                decode_vertices(key, dsts)?,
            );
        }
    }
    Ok(MatrixTile {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        tile_size: parse_u64(key, parts[4], "tile_size")?,
        direction: parse_direction(parts[5])?,
        tile_row: parse_u64(key, parts[6], "tile_row")?,
        tile_col: parse_u64(key, parts[7], "tile_col")?,
        rows,
    })
}

fn supernode_group_key(group: &SupernodeGroup) -> String {
    format!(
        "cell/{}/artifact/supernode/{}/{}/{:020}/{:020}",
        group.cell_id,
        group.edge_type,
        direction_str(group.direction),
        group.vertex_id,
        group.base_epoch
    )
}

fn supernode_group_prefix(
    cell_id: &str,
    edge_type: &str,
    direction: ArtifactDirection,
    vertex_id: VertexId,
) -> String {
    format!(
        "cell/{cell_id}/artifact/supernode/{edge_type}/{}/{vertex_id:020}/",
        direction_str(direction)
    )
}

fn encode_supernode_group(group: &SupernodeGroup) -> Vec<u8> {
    format!(
        "supernode3\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        group.cell_id,
        group.edge_type,
        direction_str(group.direction),
        group.vertex_id,
        group.base_epoch,
        group.degree,
        group.chunk_count,
        group.page_size,
        encode_supernode_chunk_bounds(&group.chunk_bounds)
    )
    .into_bytes()
}

fn decode_supernode_group(key: &str, value: &[u8]) -> Result<SupernodeGroup> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts[0] == "supernode1" {
        if parts.len() != 8 {
            return corrupt(key, "expected supernode1 record with 8 fields");
        }
        return Ok(SupernodeGroup {
            cell_id: parts[1].to_string(),
            edge_type: parts[2].to_string(),
            direction: parse_direction(parts[3])?,
            vertex_id: parse_u64(key, parts[4], "vertex_id")?,
            base_epoch: parse_u64(key, parts[5], "base_epoch")?,
            degree: parse_u64(key, parts[6], "degree")?,
            chunk_count: parse_u64(key, parts[7], "chunk_count")?,
            page_size: 0,
            chunk_bounds: Vec::new(),
        });
    }
    if parts[0] == "supernode2" {
        if parts.len() != 9 {
            return corrupt(key, "expected supernode2 record with 9 fields");
        }
        return Ok(SupernodeGroup {
            cell_id: parts[1].to_string(),
            edge_type: parts[2].to_string(),
            direction: parse_direction(parts[3])?,
            vertex_id: parse_u64(key, parts[4], "vertex_id")?,
            base_epoch: parse_u64(key, parts[5], "base_epoch")?,
            degree: parse_u64(key, parts[6], "degree")?,
            chunk_count: parse_u64(key, parts[7], "chunk_count")?,
            page_size: parse_u64(key, parts[8], "page_size")?,
            chunk_bounds: Vec::new(),
        });
    }
    if parts.len() != 10 || parts[0] != "supernode3" {
        return corrupt(key, "expected supernode3 record with 10 fields");
    }
    Ok(SupernodeGroup {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        direction: parse_direction(parts[3])?,
        vertex_id: parse_u64(key, parts[4], "vertex_id")?,
        base_epoch: parse_u64(key, parts[5], "base_epoch")?,
        degree: parse_u64(key, parts[6], "degree")?,
        chunk_count: parse_u64(key, parts[7], "chunk_count")?,
        page_size: parse_u64(key, parts[8], "page_size")?,
        chunk_bounds: decode_supernode_chunk_bounds(key, parts[9])?,
    })
}

fn encode_supernode_chunk_bounds(bounds: &[SupernodeChunkBound]) -> String {
    bounds
        .iter()
        .map(|bound| format!("{}:{}:{}", bound.chunk_id, bound.first, bound.last))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_supernode_chunk_bounds(key: &str, value: &str) -> Result<Vec<SupernodeChunkBound>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut bounds = Vec::new();
    for item in value.split(',') {
        let parts: Vec<_> = item.split(':').collect();
        if parts.len() != 3 {
            return corrupt(key, "invalid supernode chunk bound encoding");
        }
        bounds.push(SupernodeChunkBound {
            chunk_id: parse_u64(key, parts[0], "chunk_id")?,
            first: parse_u64(key, parts[1], "chunk_first")?,
            last: parse_u64(key, parts[2], "chunk_last")?,
        });
    }
    bounds.sort_by_key(|bound| (bound.first, bound.last, bound.chunk_id));
    Ok(bounds)
}

fn encode_vertices(vertices: &[VertexId]) -> String {
    vertices
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_vertices(key: &str, value: &str) -> Result<Vec<VertexId>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| parse_u64(key, part, "vertex"))
        .collect()
}

fn encode_binary_string(out: &mut Vec<u8>, value: &str) {
    encode_binary_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn encode_binary_u64s(out: &mut Vec<u8>, values: &[u64]) {
    encode_binary_u64(out, values.len() as u64);
    for value in values {
        encode_binary_u64(out, *value);
    }
}

fn encode_binary_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_binary_string(
    key: &str,
    value: &[u8],
    cursor: &mut usize,
    field: &str,
) -> Result<String> {
    let len = decode_binary_len(key, value, cursor, field)?;
    let bytes = take_binary(key, value, cursor, len, field)?;
    String::from_utf8(bytes.to_vec()).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("invalid UTF-8 in {field}: {err}"),
    })
}

fn decode_binary_u64s(
    key: &str,
    value: &[u8],
    cursor: &mut usize,
    field: &str,
) -> Result<Vec<u64>> {
    let len = decode_binary_len(key, value, cursor, field)?;
    let bytes = take_binary(
        key,
        value,
        cursor,
        len.checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("binary array {field} is too large"),
            })?,
        field,
    )?;
    Ok(bytes
        .chunks_exact(std::mem::size_of::<u64>())
        .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("chunk length is exactly 8")))
        .collect())
}

fn decode_binary_len(key: &str, value: &[u8], cursor: &mut usize, field: &str) -> Result<usize> {
    let raw = decode_binary_u64(key, value, cursor, field)?;
    usize::try_from(raw).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("binary length for {field} does not fit usize"),
    })
}

fn decode_binary_u64(key: &str, value: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
    let bytes = take_binary(key, value, cursor, std::mem::size_of::<u64>(), field)?;
    Ok(u64::from_le_bytes(
        bytes.try_into().expect("u64 field length is exactly 8"),
    ))
}

fn take_binary<'a>(
    key: &str,
    value: &'a [u8],
    cursor: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("binary cursor overflow while reading {field}"),
        })?;
    if end > value.len() {
        return corrupt(key, format!("truncated binary field {field}"));
    }
    let out = &value[*cursor..end];
    *cursor = end;
    Ok(out)
}

fn text_value<'a>(key: &str, value: &'a [u8]) -> Result<&'a str> {
    std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })
}

fn corrupt<T>(key: &str, reason: impl Into<String>) -> Result<T> {
    Err(GraphError::CorruptValue {
        key: key.to_string(),
        reason: reason.into(),
    })
}

fn phase0_matrix_profile_enabled() -> bool {
    std::env::var("PHASE0_PROFILE_MATRIX").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn phase0_matrix_profile(enabled: bool, stage: &str, elapsed: Duration, units: u64) {
    if enabled {
        eprintln!(
            "matrix_profile stage={stage} elapsed_us={} units={units}",
            elapsed.as_micros()
        );
    }
}
