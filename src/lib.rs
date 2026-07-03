use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use slatedb::bytes::Bytes;
use slatedb::config::{
    DurabilityLevel, PreloadLevel, ReadOptions, ScanOptions, Settings, WriteOptions,
};
use slatedb::object_store::{path::Path, ObjectStore};
use slatedb::{Db, DbTransaction, ErrorKind, IsolationLevel, WriteBatch};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

mod algebra;
pub mod opencypher;
mod phase0;
mod placement;
mod sparse_kernel;

pub use algebra::{QueryContext, QueryOutput, QueryStatement};
pub use opencypher::{
    parse_cypher, parse_opencypher, CypherFrontend, DefaultCypherFrontend, LibCypherParserFrontend,
};
pub use phase0::{
    local_object_store, object_store_from_env, ArtifactDirection, ArtifactGcResult,
    BenchmarkResult, DeltaGcResult, GraphControlPlane, GraphNode, GraphRollup, LeaseRenewalHandle,
    MatrixArtifact, MatrixTraversalResult, Phase0Cluster, PostingChunk, RoutedPhase0Cluster,
    ShardLease, ShardPlacement, SupernodeGroup, SupernodePage, TraversalBackend,
};
pub use placement::{
    compare_locality_layouts, locality_cell_id, locality_cell_prefix, locality_cell_prefix_len,
    LocalityCellExtractor, LocalityLayoutExperiment, StorageLayout,
};
pub use sparse_kernel::SparseKernelBackend;

pub type VertexId = u64;
pub type GraphEpoch = u64;
pub(crate) type MatrixAdjacency = BTreeMap<VertexId, BTreeSet<VertexId>>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct MatrixCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) base_epoch: GraphEpoch,
}

impl MatrixCacheKey {
    pub(crate) fn new(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            base_epoch,
        }
    }
}

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("slatedb error: {0}")]
    Slate(#[from] slatedb::Error),
    #[error("object store error: {0}")]
    ObjectStore(#[from] slatedb::object_store::Error),
    #[error("invalid {component} key component: {value}")]
    InvalidKeyComponent {
        component: &'static str,
        value: String,
    },
    #[error("corrupt value at {key}: {reason}")]
    CorruptValue { key: String, reason: String },
    #[error("idempotency key conflict for {operation} request key {idempotency_key}")]
    IdempotencyConflict {
        operation: &'static str,
        idempotency_key: String,
    },
    #[error(
        "snapshot epoch {read_epoch} is ahead of current epoch {current_epoch} for cell {cell_id}"
    )]
    SnapshotAhead {
        cell_id: String,
        read_epoch: GraphEpoch,
        current_epoch: GraphEpoch,
    },
    #[error("no shard placement exists for cell {cell_id}")]
    UnknownShard { cell_id: String },
    #[error("cell {cell_id} is owned by node {owner_node_id}, not local node {local_node_id}")]
    ShardNotOwned {
        cell_id: String,
        owner_node_id: String,
        local_node_id: String,
    },
    #[error("cell {cell_id} is currently leased by node {owner_node_id} until {expires_at_ms}")]
    ShardLeaseHeld {
        cell_id: String,
        owner_node_id: String,
        expires_at_ms: u64,
    },
    #[error("node {node_id} does not hold current lease token {lease_token} for cell {cell_id}")]
    StaleShardLease {
        cell_id: String,
        node_id: String,
        lease_token: u64,
    },
    #[error("{operation} requires an active graph control-plane lease for cell {cell_id}")]
    WriteRequiresLease {
        operation: &'static str,
        cell_id: String,
    },
    #[error(
        "snapshot epoch {read_epoch} for cell {cell_id} edge {edge_type} is below compacted watermark {min_epoch}"
    )]
    SnapshotExpired {
        cell_id: String,
        edge_type: String,
        read_epoch: GraphEpoch,
        min_epoch: GraphEpoch,
    },
    #[error("{operation} rejected by admission control: actual {actual} exceeds limit {limit}")]
    AdmissionRejected {
        operation: &'static str,
        actual: u64,
        limit: u64,
    },
    #[error("sparse kernel {backend} failed: {reason}")]
    SparseKernel {
        backend: &'static str,
        reason: String,
    },
    #[error("{dialect} parse error: {reason}")]
    QueryParse {
        dialect: &'static str,
        reason: String,
    },
    #[error("{dialect} query is not supported yet: {feature}")]
    UnsupportedQuery {
        dialect: &'static str,
        feature: String,
    },
}

pub type Result<T> = std::result::Result<T, GraphError>;

const GRAPH_TXN_MAX_RETRIES: usize = 32;
const GRAPH_DELTA_GC_BATCH_KEYS: usize = 512;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphLimits {
    pub max_bulk_import_edges: usize,
    pub max_artifact_source_epochs: GraphEpoch,
    pub max_traversal_hops: u8,
}

impl Default for GraphLimits {
    fn default() -> Self {
        Self {
            max_bulk_import_edges: 1_000_000,
            max_artifact_source_epochs: 10_000_000,
            max_traversal_hops: 16,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphCacheConfig {
    pub object_store_cache_dir: Option<PathBuf>,
    pub object_store_cache_bytes: Option<usize>,
    pub object_store_cache_puts: bool,
    pub preload_sst_on_startup: bool,
}

impl GraphCacheConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn disk_cache(cache_dir: impl Into<PathBuf>, max_cache_size_bytes: usize) -> Self {
        Self {
            object_store_cache_dir: Some(cache_dir.into()),
            object_store_cache_bytes: Some(max_cache_size_bytes),
            object_store_cache_puts: true,
            preload_sst_on_startup: true,
        }
    }

    fn apply_to_settings(&self, settings: &mut Settings) {
        if let Some(cache_dir) = &self.object_store_cache_dir {
            settings.object_store_cache_options.root_folder = Some(cache_dir.clone());
        }
        if let Some(max_cache_size_bytes) = self.object_store_cache_bytes {
            settings.object_store_cache_options.max_cache_size_bytes = Some(max_cache_size_bytes);
        }
        settings.object_store_cache_options.cache_puts = self.object_store_cache_puts;
        if self.preload_sst_on_startup {
            settings
                .object_store_cache_options
                .preload_disk_cache_on_startup = Some(PreloadLevel::AllSst);
        }
    }
}

impl Default for GraphCacheConfig {
    fn default() -> Self {
        Self {
            object_store_cache_dir: None,
            object_store_cache_bytes: None,
            object_store_cache_puts: false,
            preload_sst_on_startup: false,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphOpenOptions {
    pub limits: GraphLimits,
    pub cache: GraphCacheConfig,
    pub cache_policy: GraphCachePolicy,
}

pub(crate) async fn open_graph_db(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
) -> Result<Db> {
    let mut settings = Settings::default();
    cache.apply_to_settings(&mut settings);
    Ok(Db::builder(path, object_store)
        .with_settings(settings)
        .build()
        .await?)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphCachePolicy {
    pub max_matrix_artifacts: usize,
    pub max_matrix_adjacencies: usize,
    pub max_graphblas_matrices: usize,
    pub max_supernode_groups: usize,
    pub max_posting_chunks: usize,
    pub max_entries_per_cell: Option<usize>,
    pub pin_matrix_min_edges: u64,
    pub pin_supernode_min_degree: u64,
    pub prefetch_supernode_chunks: u64,
    pub max_concurrent_hydrations: usize,
}

impl Default for GraphCachePolicy {
    fn default() -> Self {
        Self {
            max_matrix_artifacts: 1_024,
            max_matrix_adjacencies: 128,
            max_graphblas_matrices: 64,
            max_supernode_groups: 4_096,
            max_posting_chunks: 16_384,
            max_entries_per_cell: Some(8_192),
            pin_matrix_min_edges: 1_000_000,
            pin_supernode_min_degree: 10_000,
            prefetch_supernode_chunks: 1,
            max_concurrent_hydrations: 16,
        }
    }
}

impl GraphCachePolicy {
    fn hydration_permits(&self) -> usize {
        self.max_concurrent_hydrations.max(1)
    }

    fn pin_matrix_artifact(&self, artifact: &phase0::MatrixArtifact) -> bool {
        artifact.edge_count >= self.pin_matrix_min_edges
    }

    fn pin_supernode_group(&self, group: &phase0::SupernodeGroup) -> bool {
        group.degree >= self.pin_supernode_min_degree
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphCacheKind {
    MatrixArtifact,
    MatrixAdjacency,
    GraphBlas,
    SupernodeGroup,
    PostingChunk,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheMetricsSnapshot {
    pub matrix_artifact_hits: u64,
    pub matrix_artifact_misses: u64,
    pub matrix_adjacency_hits: u64,
    pub matrix_adjacency_misses: u64,
    pub graphblas_hits: u64,
    pub graphblas_misses: u64,
    pub supernode_group_hits: u64,
    pub supernode_group_misses: u64,
    pub posting_chunk_hits: u64,
    pub posting_chunk_misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub pinned_insertions: u64,
    pub tenant_quota_rejections: u64,
    pub hydration_started: u64,
    pub hydration_waited: u64,
    pub hydration_completed: u64,
    pub prefetch_requests: u64,
    pub prefetch_skipped: u64,
}

#[derive(Default)]
pub(crate) struct GraphCacheMetrics {
    matrix_artifact_hits: AtomicU64,
    matrix_artifact_misses: AtomicU64,
    matrix_adjacency_hits: AtomicU64,
    matrix_adjacency_misses: AtomicU64,
    graphblas_hits: AtomicU64,
    graphblas_misses: AtomicU64,
    supernode_group_hits: AtomicU64,
    supernode_group_misses: AtomicU64,
    posting_chunk_hits: AtomicU64,
    posting_chunk_misses: AtomicU64,
    insertions: AtomicU64,
    evictions: AtomicU64,
    pinned_insertions: AtomicU64,
    tenant_quota_rejections: AtomicU64,
    hydration_started: AtomicU64,
    hydration_waited: AtomicU64,
    hydration_completed: AtomicU64,
    prefetch_requests: AtomicU64,
    prefetch_skipped: AtomicU64,
}

impl GraphCacheMetrics {
    fn record_hit(&self, kind: GraphCacheKind) {
        self.counter(kind, true).fetch_add(1, Ordering::Relaxed);
    }

    fn record_miss(&self, kind: GraphCacheKind) {
        self.counter(kind, false).fetch_add(1, Ordering::Relaxed);
    }

    fn counter(&self, kind: GraphCacheKind, hit: bool) -> &AtomicU64 {
        match (kind, hit) {
            (GraphCacheKind::MatrixArtifact, true) => &self.matrix_artifact_hits,
            (GraphCacheKind::MatrixArtifact, false) => &self.matrix_artifact_misses,
            (GraphCacheKind::MatrixAdjacency, true) => &self.matrix_adjacency_hits,
            (GraphCacheKind::MatrixAdjacency, false) => &self.matrix_adjacency_misses,
            (GraphCacheKind::GraphBlas, true) => &self.graphblas_hits,
            (GraphCacheKind::GraphBlas, false) => &self.graphblas_misses,
            (GraphCacheKind::SupernodeGroup, true) => &self.supernode_group_hits,
            (GraphCacheKind::SupernodeGroup, false) => &self.supernode_group_misses,
            (GraphCacheKind::PostingChunk, true) => &self.posting_chunk_hits,
            (GraphCacheKind::PostingChunk, false) => &self.posting_chunk_misses,
        }
    }

    fn snapshot(&self) -> GraphCacheMetricsSnapshot {
        GraphCacheMetricsSnapshot {
            matrix_artifact_hits: self.matrix_artifact_hits.load(Ordering::Relaxed),
            matrix_artifact_misses: self.matrix_artifact_misses.load(Ordering::Relaxed),
            matrix_adjacency_hits: self.matrix_adjacency_hits.load(Ordering::Relaxed),
            matrix_adjacency_misses: self.matrix_adjacency_misses.load(Ordering::Relaxed),
            graphblas_hits: self.graphblas_hits.load(Ordering::Relaxed),
            graphblas_misses: self.graphblas_misses.load(Ordering::Relaxed),
            supernode_group_hits: self.supernode_group_hits.load(Ordering::Relaxed),
            supernode_group_misses: self.supernode_group_misses.load(Ordering::Relaxed),
            posting_chunk_hits: self.posting_chunk_hits.load(Ordering::Relaxed),
            posting_chunk_misses: self.posting_chunk_misses.load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            pinned_insertions: self.pinned_insertions.load(Ordering::Relaxed),
            tenant_quota_rejections: self.tenant_quota_rejections.load(Ordering::Relaxed),
            hydration_started: self.hydration_started.load(Ordering::Relaxed),
            hydration_waited: self.hydration_waited.load(Ordering::Relaxed),
            hydration_completed: self.hydration_completed.load(Ordering::Relaxed),
            prefetch_requests: self.prefetch_requests.load(Ordering::Relaxed),
            prefetch_skipped: self.prefetch_skipped.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphRepairReport {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: GraphEpoch,
    pub live_edges: u64,
    pub delta_records: u64,
    pub degree_mismatches: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutation {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeRecord {
    pub cell_id: String,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub epoch: GraphEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitResult {
    pub epoch: GraphEpoch,
    pub already_existed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteResult {
    pub epoch: GraphEpoch,
    pub deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkImportResult {
    pub start_epoch: GraphEpoch,
    pub end_epoch: GraphEpoch,
    pub inserted: u64,
    pub already_existed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaKind {
    Plus,
    Minus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaRecord {
    pub kind: DeltaKind,
    pub edge: EdgeRecord,
}

pub struct GraphShard {
    db: Db,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) cache_metrics: Arc<GraphCacheMetrics>,
    hydration_gate: Arc<Semaphore>,
    write_authority: GraphWriteAuthority,
    writer_gate: Mutex<()>,
    matrix_artifact_cache: Mutex<BoundedGraphCache<MatrixCacheKey, phase0::MatrixArtifact>>,
    matrix_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<MatrixAdjacency>>>,
    graphblas_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, Arc<sparse_kernel::CompiledGraphBlasMatrix>>>,
    supernode_group_cache: Mutex<BoundedGraphCache<SupernodeCacheKey, phase0::SupernodeGroup>>,
    posting_chunk_cache: Mutex<BoundedGraphCache<PostingChunkCacheKey, phase0::PostingChunk>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheEntryCounts {
    pub matrix_artifacts: usize,
    pub matrix_adjacencies: usize,
    pub graphblas_matrices: usize,
    pub supernode_groups: usize,
    pub posting_chunks: usize,
}

#[derive(Clone)]
pub(crate) enum GraphWriteAuthority {
    ReadOnly,
    Standalone,
    Leased {
        local_node_id: String,
        leases: Arc<RwLock<BTreeMap<String, phase0::ShardLease>>>,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SupernodeCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) direction: phase0::ArtifactDirection,
    pub(crate) vertex_id: VertexId,
    pub(crate) base_epoch: GraphEpoch,
}

impl SupernodeCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        direction: phase0::ArtifactDirection,
        vertex_id: VertexId,
        base_epoch: GraphEpoch,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            direction,
            vertex_id,
            base_epoch,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct PostingChunkCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) direction: phase0::ArtifactDirection,
    pub(crate) vertex_id: VertexId,
    pub(crate) base_epoch: GraphEpoch,
    pub(crate) chunk_id: u64,
}

impl PostingChunkCacheKey {
    pub(crate) fn new(group: &phase0::SupernodeGroup, chunk_id: u64) -> Self {
        Self {
            cell_id: group.cell_id.clone(),
            edge_type: group.edge_type.clone(),
            direction: group.direction,
            vertex_id: group.vertex_id,
            base_epoch: group.base_epoch,
            chunk_id,
        }
    }

    pub(crate) fn from_chunk(chunk: &phase0::PostingChunk) -> Self {
        Self {
            cell_id: chunk.cell_id.clone(),
            edge_type: chunk.edge_type.clone(),
            direction: chunk.direction,
            vertex_id: chunk.owner,
            base_epoch: chunk.base_epoch,
            chunk_id: chunk.chunk_id,
        }
    }
}

struct CacheEntry<V> {
    value: V,
    tenant: String,
    pinned: bool,
    last_access: u64,
}

pub(crate) struct BoundedGraphCache<K, V> {
    max_entries: usize,
    max_entries_per_tenant: Option<usize>,
    clock: u64,
    entries: BTreeMap<K, CacheEntry<V>>,
    tenant_entries: BTreeMap<String, usize>,
}

impl<K, V> BoundedGraphCache<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    fn new(max_entries: usize, max_entries_per_tenant: Option<usize>) -> Self {
        Self {
            max_entries,
            max_entries_per_tenant,
            clock: 0,
            entries: BTreeMap::new(),
            tenant_entries: BTreeMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn get(&mut self, key: &K) -> Option<V> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.last_access = self.clock;
        Some(entry.value.clone())
    }

    fn get_latest_by(
        &mut self,
        mut predicate: impl FnMut(&K, &V) -> bool,
        mut score: impl FnMut(&K, &V) -> GraphEpoch,
    ) -> Option<V> {
        let key = self
            .entries
            .iter()
            .filter(|(key, entry)| predicate(key, &entry.value))
            .max_by_key(|(key, entry)| score(key, &entry.value))
            .map(|(key, _)| key.clone())?;
        self.get(&key)
    }

    fn insert(
        &mut self,
        key: K,
        value: V,
        tenant: impl Into<String>,
        pinned: bool,
        metrics: &GraphCacheMetrics,
    ) -> Option<V> {
        if self.max_entries == 0 {
            metrics
                .tenant_quota_rejections
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }

        self.clock = self.clock.saturating_add(1);
        let tenant = tenant.into();
        let previous = self.entries.insert(
            key.clone(),
            CacheEntry {
                value,
                tenant: tenant.clone(),
                pinned,
                last_access: self.clock,
            },
        );
        if let Some(previous) = previous {
            self.decrement_tenant(&previous.tenant);
        }
        *self.tenant_entries.entry(tenant.clone()).or_default() += 1;
        metrics.insertions.fetch_add(1, Ordering::Relaxed);
        if pinned {
            metrics.pinned_insertions.fetch_add(1, Ordering::Relaxed);
        }

        self.enforce_tenant_quota(&tenant, metrics);
        self.enforce_total_limit(metrics);
        self.get(&key)
    }

    fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        let removed: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| (!keep(key, &entry.value)).then_some(key.clone()))
            .collect();
        for key in removed {
            self.remove(&key);
        }
    }

    fn enforce_tenant_quota(&mut self, tenant: &str, metrics: &GraphCacheMetrics) {
        let Some(limit) = self.max_entries_per_tenant else {
            return;
        };
        while self.tenant_entries.get(tenant).copied().unwrap_or(0) > limit {
            if self.evict_one(Some(tenant), false, metrics).is_none()
                && self.evict_one(Some(tenant), true, metrics).is_none()
            {
                metrics
                    .tenant_quota_rejections
                    .fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    fn enforce_total_limit(&mut self, metrics: &GraphCacheMetrics) {
        while self.entries.len() > self.max_entries {
            if self.evict_one(None, false, metrics).is_none()
                && self.evict_one(None, true, metrics).is_none()
            {
                break;
            }
        }
    }

    fn evict_one(
        &mut self,
        tenant: Option<&str>,
        allow_pinned: bool,
        metrics: &GraphCacheMetrics,
    ) -> Option<()> {
        let key = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                tenant.is_none_or(|tenant| tenant == entry.tenant)
                    && (allow_pinned || !entry.pinned)
            })
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())?;
        self.remove(&key);
        metrics.evictions.fetch_add(1, Ordering::Relaxed);
        Some(())
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
        self.decrement_tenant(&entry.tenant);
        Some(entry.value)
    }

    fn decrement_tenant(&mut self, tenant: &str) {
        if let Some(count) = self.tenant_entries.get_mut(tenant) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.tenant_entries.remove(tenant);
            }
        }
    }
}

pub struct GraphSnapshot<'a> {
    shard: &'a GraphShard,
    cell_id: String,
    read_epoch: GraphEpoch,
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

impl GraphShard {
    pub async fn open(path: impl Into<Path>, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        Self::open_with_options(path, object_store, GraphOpenOptions::default()).await
    }

    pub async fn open_with_limits(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        limits: GraphLimits,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            object_store,
            GraphOpenOptions {
                limits,
                cache: GraphCacheConfig::default(),
                cache_policy: GraphCachePolicy::default(),
            },
        )
        .await
    }

    pub async fn open_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_internal(path, object_store, options, GraphWriteAuthority::ReadOnly).await
    }

    pub async fn open_standalone_writer(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_standalone_writer_with_options(path, object_store, GraphOpenOptions::default())
            .await
    }

    pub async fn open_standalone_writer_with_limits(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        limits: GraphLimits,
    ) -> Result<Self> {
        Self::open_standalone_writer_with_options(
            path,
            object_store,
            GraphOpenOptions {
                limits,
                cache: GraphCacheConfig::default(),
                cache_policy: GraphCachePolicy::default(),
            },
        )
        .await
    }

    pub async fn open_standalone_writer_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_internal(path, object_store, options, GraphWriteAuthority::Standalone).await
    }

    pub(crate) async fn open_leased_writer(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        local_node_id: String,
        leases: Arc<RwLock<BTreeMap<String, phase0::ShardLease>>>,
    ) -> Result<Self> {
        Self::open_internal(
            path,
            object_store,
            options,
            GraphWriteAuthority::Leased {
                local_node_id,
                leases,
            },
        )
        .await
    }

    async fn open_internal(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        write_authority: GraphWriteAuthority,
    ) -> Result<Self> {
        let db = open_graph_db(path, object_store, &options.cache).await?;
        let cache_policy = options.cache_policy;
        let tenant_quota = cache_policy.max_entries_per_cell;
        let cache_metrics = Arc::new(GraphCacheMetrics::default());
        let hydration_gate = Arc::new(Semaphore::new(cache_policy.hydration_permits()));
        Ok(Self {
            db,
            limits: options.limits,
            cache_policy: cache_policy.clone(),
            cache_metrics,
            hydration_gate,
            write_authority,
            writer_gate: Mutex::new(()),
            matrix_artifact_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_matrix_artifacts,
                tenant_quota,
            )),
            matrix_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_matrix_adjacencies,
                tenant_quota,
            )),
            graphblas_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_graphblas_matrices,
                tenant_quota,
            )),
            supernode_group_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_supernode_groups,
                tenant_quota,
            )),
            posting_chunk_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_posting_chunks,
                tenant_quota,
            )),
        })
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub fn graph_cache_metrics(&self) -> GraphCacheMetricsSnapshot {
        self.cache_metrics.snapshot()
    }

    pub async fn graph_cache_entry_counts(&self) -> GraphCacheEntryCounts {
        GraphCacheEntryCounts {
            matrix_artifacts: self.matrix_artifact_cache.lock().await.len(),
            matrix_adjacencies: self.matrix_cache.lock().await.len(),
            graphblas_matrices: self.graphblas_cache.lock().await.len(),
            supernode_groups: self.supernode_group_cache.lock().await.len(),
            posting_chunks: self.posting_chunk_cache.lock().await.len(),
        }
    }

    pub(crate) async fn acquire_hydration_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.cache_metrics
            .hydration_started
            .fetch_add(1, Ordering::Relaxed);
        if self.hydration_gate.available_permits() == 0 {
            self.cache_metrics
                .hydration_waited
                .fetch_add(1, Ordering::Relaxed);
        }
        self.hydration_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: format!("cache/hydration/{operation}"),
                reason: format!("hydration gate closed: {err}"),
            })
    }

    pub(crate) fn record_hydration_complete(&self) {
        self.cache_metrics
            .hydration_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn ensure_write_authority(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        match &self.write_authority {
            GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresLease {
                operation,
                cell_id: cell_id.to_string(),
            }),
            GraphWriteAuthority::Standalone => Ok(()),
            GraphWriteAuthority::Leased {
                local_node_id,
                leases,
            } => {
                let Some(lease) = leases.read().map_err(lock_error)?.get(cell_id).cloned() else {
                    return Err(GraphError::WriteRequiresLease {
                        operation,
                        cell_id: cell_id.to_string(),
                    });
                };
                if lease.owner_node_id == *local_node_id && lease.expires_at_ms > graph_now_millis()
                {
                    Ok(())
                } else {
                    Err(GraphError::StaleShardLease {
                        cell_id: cell_id.to_string(),
                        node_id: local_node_id.clone(),
                        lease_token: lease.lease_token,
                    })
                }
            }
        }
    }

    pub async fn snapshot(&self, cell_id: &str) -> Result<GraphSnapshot<'_>> {
        validate_component("cell_id", cell_id)?;
        let read_epoch = self.current_epoch(cell_id).await?;
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
        })
    }

    pub async fn snapshot_at(
        &self,
        cell_id: &str,
        read_epoch: GraphEpoch,
    ) -> Result<GraphSnapshot<'_>> {
        validate_component("cell_id", cell_id)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if read_epoch > current_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: cell_id.to_string(),
                read_epoch,
                current_epoch,
            });
        }
        Ok(GraphSnapshot {
            shard: self,
            cell_id: cell_id.to_string(),
            read_epoch,
        })
    }

    pub async fn write_edge(&self, mutation: EdgeMutation) -> Result<CommitResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge")?;

        let _writer = self.writer_gate.lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.write_edge_txn(&mutation).await {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn write_edge_txn(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "create", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_commit_idempotency(&idem_key, &mutation, &value);
        }

        let canonical_key = keys::edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );

        if let Some(value) = read_txn_remote(&txn, &canonical_key).await? {
            let record = decode_edge_record(&canonical_key, &value)?;
            let result = CommitResult {
                epoch: record.epoch,
                already_existed: true,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_commit_idempotency(mutation, &result),
            )?;
            commit_txn_strict(txn).await?;
            return Ok(result);
        }

        let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
        let record = EdgeRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
            epoch,
        };
        let result = CommitResult {
            epoch,
            already_existed: false,
        };

        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
        let out_degree = read_counter_txn(&txn, &out_degree_key).await? + 1;
        let in_degree = read_counter_txn(&txn, &in_degree_key).await? + 1;

        txn.put(
            keys::last_epoch(&mutation.cell_id).as_bytes(),
            encode_u64(epoch),
        )?;
        txn.put(canonical_key.as_bytes(), encode_edge_record(&record))?;
        txn.put(
            keys::out_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            encode_edge_record(&record),
        )?;
        txn.put(
            keys::in_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.dst,
                mutation.src,
            )
            .as_bytes(),
            encode_edge_record(&record),
        )?;
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        txn.put(
            keys::outbox(
                &mutation.cell_id,
                epoch,
                DeltaKind::Plus,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record.clone(),
            }),
        )?;
        txn.put(
            keys::delta_plus(
                &mutation.cell_id,
                &mutation.edge_type,
                epoch,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            encode_edge_record(&record),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_commit_idempotency(mutation, &result),
        )?;

        commit_txn_strict(txn).await?;
        Ok(result)
    }

    pub async fn delete_edge(&self, mutation: EdgeMutation) -> Result<DeleteResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_edge")?;

        let _writer = self.writer_gate.lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.delete_edge_txn(&mutation).await {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn delete_edge_txn(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "delete", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_delete_idempotency(&idem_key, &mutation, &value);
        }

        let canonical_key = keys::edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );

        let Some(existing) = read_txn_remote(&txn, &canonical_key).await? else {
            let result = DeleteResult {
                epoch: read_counter_txn(&txn, &keys::last_epoch(&mutation.cell_id)).await?,
                deleted: false,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
            commit_txn_strict(txn).await?;
            return Ok(result);
        };

        decode_edge_record(&canonical_key, &existing)?;
        let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
        let record = EdgeRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
            epoch,
        };
        let result = DeleteResult {
            epoch,
            deleted: true,
        };

        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
        let out_degree = read_counter_txn(&txn, &out_degree_key)
            .await?
            .saturating_sub(1);
        let in_degree = read_counter_txn(&txn, &in_degree_key)
            .await?
            .saturating_sub(1);

        txn.put(
            keys::last_epoch(&mutation.cell_id).as_bytes(),
            encode_u64(epoch),
        )?;
        txn.delete(canonical_key.as_bytes())?;
        txn.delete(
            keys::out_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
        )?;
        txn.delete(
            keys::in_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.dst,
                mutation.src,
            )
            .as_bytes(),
        )?;
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        txn.put(
            keys::outbox(
                &mutation.cell_id,
                epoch,
                DeltaKind::Minus,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Minus,
                edge: record.clone(),
            }),
        )?;
        txn.put(
            keys::delta_minus(
                &mutation.cell_id,
                &mutation.edge_type,
                epoch,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            encode_edge_record(&record),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_delete_idempotency(mutation, &result),
        )?;

        commit_txn_strict(txn).await?;
        Ok(result)
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "bulk_import_edges")?;

        let mut edges: Vec<_> = edges.into_iter().collect();
        ensure_limit(
            "bulk_import_edges",
            edges.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        edges.sort_unstable();
        edges.dedup();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _writer = self.writer_gate.lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_import_edges_txn(cell_id, edge_type, &edges, idempotency_key, fingerprint)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub async fn write_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges(cell_id, edge_type, edges, idempotency_key)
            .await
    }

    pub async fn write_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn bulk_import_edges_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "bulk_import_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }

        let mut edges: Vec<_> = edges.into_iter().collect();
        edges.sort_unstable_by_key(|(src, dst)| (bulk_import_chunk_order(*src, *dst), *src, *dst));
        edges.dedup();

        let mut start_epoch = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;
        let mut chunk = Vec::with_capacity(chunk_size);
        let mut chunk_id = 0_u64;
        for edge in edges {
            chunk.push(edge);
            if chunk.len() == chunk_size {
                let result = self
                    .bulk_import_edges(
                        cell_id,
                        edge_type,
                        std::mem::take(&mut chunk),
                        &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                    )
                    .await?;
                start_epoch.get_or_insert(result.start_epoch);
                end_epoch = result.end_epoch;
                inserted = inserted.saturating_add(result.inserted);
                already_existed = already_existed.saturating_add(result.already_existed);
                chunk_id = chunk_id.saturating_add(1);
                chunk = Vec::with_capacity(chunk_size);
            }
        }
        if !chunk.is_empty() {
            let result = self
                .bulk_import_edges(
                    cell_id,
                    edge_type,
                    chunk,
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                )
                .await?;
            start_epoch.get_or_insert(result.start_epoch);
            end_epoch = result.end_epoch;
            inserted = inserted.saturating_add(result.inserted);
            already_existed = already_existed.saturating_add(result.already_existed);
        }

        Ok(BulkImportResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            inserted,
            already_existed,
        })
    }

    async fn bulk_import_edges_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: &[(VertexId, VertexId)],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<BulkImportResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let idem_key = keys::idempotency(cell_id, "bulk-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let fresh_cell = current_epoch == 0;
        let mut already_existed = 0_u64;
        let mut inserted_edges = Vec::new();
        for (src, dst) in edges.iter().copied() {
            if !fresh_cell
                && read_txn_remote(&txn, &keys::edge(cell_id, edge_type, src, dst))
                    .await?
                    .is_some()
            {
                already_existed += 1;
                continue;
            }
            inserted_edges.push((src, dst));
        }

        let inserted =
            u64::try_from(inserted_edges.len()).map_err(|err| GraphError::CorruptValue {
                key: "bulk_import".to_string(),
                reason: format!("too many edges in one import: {err}"),
            })?;
        let end_epoch =
            current_epoch
                .checked_add(inserted)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "bulk_import".to_string(),
                    reason: "epoch overflow during bulk import".to_string(),
                })?;
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch + 1
        };
        let result = BulkImportResult {
            start_epoch,
            end_epoch,
            inserted,
            already_existed,
        };

        let mut out_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        let mut in_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        for (offset, (src, dst)) in inserted_edges.into_iter().enumerate() {
            let epoch = current_epoch + 1 + offset as u64;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                epoch,
            };
            txn.put(
                keys::edge(cell_id, edge_type, src, dst).as_bytes(),
                encode_edge_record(&record),
            )?;
            txn.put(
                keys::out_edge(cell_id, edge_type, src, dst).as_bytes(),
                encode_edge_record(&record),
            )?;
            txn.put(
                keys::in_edge(cell_id, edge_type, dst, src).as_bytes(),
                encode_edge_record(&record),
            )?;
            txn.put(
                keys::outbox(cell_id, epoch, DeltaKind::Plus, edge_type, src, dst).as_bytes(),
                encode_delta_record(&DeltaRecord {
                    kind: DeltaKind::Plus,
                    edge: record.clone(),
                }),
            )?;
            txn.put(
                keys::delta_plus(cell_id, edge_type, epoch, src, dst).as_bytes(),
                encode_edge_record(&record),
            )?;
            *out_increments.entry(src).or_insert(0) += 1;
            *in_increments.entry(dst).or_insert(0) += 1;
        }

        for (src, increment) in out_increments {
            let key = keys::degree_out(cell_id, edge_type, src);
            let base = if fresh_cell {
                0
            } else {
                read_counter_txn(&txn, &key).await?
            };
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        for (dst, increment) in in_increments {
            let key = keys::degree_in(cell_id, edge_type, dst);
            let base = if fresh_cell {
                0
            } else {
                read_counter_txn(&txn, &key).await?
            };
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if inserted > 0 {
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(end_epoch))?;
        }
        txn.put(
            keys::mutation_batch(cell_id, result.start_epoch, idempotency_key).as_bytes(),
            encode_mutation_batch_log(edge_type, idempotency_key, fingerprint, &result),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;

        commit_txn_strict(txn).await?;
        Ok(result)
    }

    pub async fn execute_cypher(&self, context: QueryContext, query: &str) -> Result<QueryOutput> {
        self.execute_opencypher(context, query).await
    }

    pub async fn execute_opencypher(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryOutput> {
        let statement = parse_opencypher(query)?;
        self.execute_query_statement(context, statement).await
    }

    pub async fn execute_query_statement(
        &self,
        context: QueryContext,
        statement: QueryStatement,
    ) -> Result<QueryOutput> {
        validate_component("cell_id", &context.cell_id)?;
        match statement {
            QueryStatement::CreateEdge {
                edge_type,
                src,
                dst,
            } => {
                validate_component("idempotency_key", &context.idempotency_key)?;
                let result = self
                    .write_edge(EdgeMutation {
                        cell_id: context.cell_id,
                        edge_type,
                        src,
                        dst,
                        idempotency_key: context.idempotency_key,
                    })
                    .await?;
                Ok(QueryOutput::Write(result))
            }
            QueryStatement::MatchOut {
                edge_type,
                src,
                return_count,
            } => {
                if return_count {
                    let count = self.out_degree(&context.cell_id, &edge_type, src).await?;
                    Ok(QueryOutput::Count(count))
                } else {
                    let vertices = self
                        .out_neighbors(&context.cell_id, &edge_type, src)
                        .await?;
                    Ok(QueryOutput::Vertices(vertices))
                }
            }
            QueryStatement::MatchOutFiltered {
                edge_type,
                src,
                dst,
                return_count,
            } => {
                let exists = self
                    .edge_exists(&context.cell_id, &edge_type, src, dst)
                    .await?;
                if return_count {
                    Ok(QueryOutput::Count(u64::from(exists)))
                } else if exists {
                    Ok(QueryOutput::Vertices(vec![dst]))
                } else {
                    Ok(QueryOutput::Vertices(Vec::new()))
                }
            }
            QueryStatement::MatchEdge {
                edge_type,
                src,
                dst,
                return_count,
            } => {
                let exists = self
                    .edge_exists(&context.cell_id, &edge_type, src, dst)
                    .await?;
                if return_count {
                    Ok(QueryOutput::Count(u64::from(exists)))
                } else {
                    Ok(QueryOutput::Bool(exists))
                }
            }
            QueryStatement::MatchReachable {
                edge_type,
                src,
                min_hops,
                max_hops,
                return_count,
            } => {
                let read_epoch = self.current_epoch(&context.cell_id).await?;
                let mut vertices = self
                    .matrix_reachable(&context.cell_id, &edge_type, &[src], max_hops, read_epoch)
                    .await?
                    .vertices;
                if min_hops > 1 {
                    let shorter: BTreeSet<_> = self
                        .matrix_reachable(
                            &context.cell_id,
                            &edge_type,
                            &[src],
                            min_hops - 1,
                            read_epoch,
                        )
                        .await?
                        .vertices
                        .into_iter()
                        .collect();
                    vertices.retain(|vertex| !shorter.contains(vertex));
                }
                if return_count {
                    Ok(QueryOutput::Count(vertices.len() as u64))
                } else {
                    Ok(QueryOutput::Vertices(vertices))
                }
            }
        }
    }

    pub async fn edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
    ) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::edge(cell_id, edge_type, src, dst);
        Ok(self.read_remote(&key).await?.is_some())
    }

    pub async fn out_neighbors(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::out_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut neighbors = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.push(record.dst);
        }
        neighbors.sort_unstable();
        Ok(neighbors)
    }

    pub async fn in_neighbors(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut neighbors = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.push(record.src);
        }
        neighbors.sort_unstable();
        Ok(neighbors)
    }

    pub async fn out_degree(&self, cell_id: &str, edge_type: &str, src: VertexId) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.read_counter(&keys::degree_out(cell_id, edge_type, src))
            .await
    }

    pub async fn outbox_since(
        &self,
        cell_id: &str,
        after_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        validate_component("cell_id", cell_id)?;
        let prefix = keys::outbox_prefix(cell_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_delta_record(&key, &kv.value)?;
            if record.edge.epoch > after_epoch {
                records.push(record);
            }
        }
        sort_deltas(&mut records);
        Ok(records)
    }

    pub async fn deltas_since(
        &self,
        cell_id: &str,
        edge_type: &str,
        after_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        self.deltas_between(cell_id, edge_type, after_epoch, GraphEpoch::MAX)
            .await
    }

    pub async fn deltas_between(
        &self,
        cell_id: &str,
        edge_type: &str,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if after_epoch >= read_epoch {
            return Ok(Vec::new());
        }
        let watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
        if after_epoch < watermark {
            return Err(GraphError::SnapshotExpired {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                read_epoch: after_epoch,
                min_epoch: watermark,
            });
        }

        let mut records = Vec::new();
        self.scan_delta_prefix(
            &keys::delta_plus_prefix(cell_id, edge_type),
            DeltaKind::Plus,
            after_epoch,
            read_epoch,
            &mut records,
        )
        .await?;
        self.scan_delta_prefix(
            &keys::delta_minus_prefix(cell_id, edge_type),
            DeltaKind::Minus,
            after_epoch,
            read_epoch,
            &mut records,
        )
        .await?;

        sort_deltas(&mut records);
        Ok(records)
    }

    pub async fn current_epoch(&self, cell_id: &str) -> Result<GraphEpoch> {
        validate_component("cell_id", cell_id)?;
        self.read_counter(&keys::last_epoch(cell_id)).await
    }

    pub async fn edges_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<EdgeRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let mut edges = std::collections::BTreeMap::new();
        let base_epoch = if let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .await?
        {
            let adjacency = self
                .cached_matrix_adjacency(cell_id, edge_type, artifact.base_epoch)
                .await?;
            for (src, dsts) in adjacency.iter() {
                for dst in dsts {
                    edges.insert(
                        (*src, *dst),
                        EdgeRecord {
                            cell_id: cell_id.to_string(),
                            edge_type: edge_type.to_string(),
                            src: *src,
                            dst: *dst,
                            epoch: artifact.base_epoch,
                        },
                    );
                }
            }
            artifact.base_epoch
        } else {
            0
        };
        for delta in self
            .deltas_between(cell_id, edge_type, base_epoch, read_epoch)
            .await?
        {
            let key = (delta.edge.src, delta.edge.dst);
            match delta.kind {
                DeltaKind::Plus => {
                    edges.insert(key, delta.edge);
                }
                DeltaKind::Minus => {
                    edges.remove(&key);
                }
            }
        }
        Ok(edges.into_values().collect())
    }

    pub async fn validate_cell_edge_type(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphRepairReport> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let read_epoch = self.current_epoch(cell_id).await?;
        let edges = self.edges_at(cell_id, edge_type, read_epoch).await?;
        let deltas = self
            .deltas_between(cell_id, edge_type, 0, read_epoch)
            .await?;
        let mut out_counts = BTreeMap::<VertexId, u64>::new();
        let mut in_counts = BTreeMap::<VertexId, u64>::new();
        for edge in &edges {
            *out_counts.entry(edge.src).or_default() += 1;
            *in_counts.entry(edge.dst).or_default() += 1;
        }
        let mut degree_mismatches = Vec::new();
        for (src, expected) in out_counts {
            let actual = self.out_degree(cell_id, edge_type, src).await?;
            if actual != expected {
                degree_mismatches.push(format!("out:{src}:expected={expected}:actual={actual}"));
            }
        }
        for (dst, expected) in in_counts {
            let actual = self
                .read_counter(&keys::degree_in(cell_id, edge_type, dst))
                .await?;
            if actual != expected {
                degree_mismatches.push(format!("in:{dst}:expected={expected}:actual={actual}"));
            }
        }
        Ok(GraphRepairReport {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            read_epoch,
            live_edges: edges.len() as u64,
            delta_records: deltas.len() as u64,
            degree_mismatches,
        })
    }

    pub async fn delete_deltas_through_rollup(
        &self,
        cell_id: &str,
        edge_type: &str,
        compact_through_epoch: GraphEpoch,
    ) -> Result<DeltaGcResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "delete_deltas_through_rollup")?;
        let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, compact_through_epoch)
            .await?
        else {
            return Err(GraphError::CorruptValue {
                key: keys::delta_gc_watermark(cell_id, edge_type),
                reason: "cannot compact deltas without a matrix rollup artifact".to_string(),
            });
        };
        if artifact.base_epoch != compact_through_epoch {
            return Err(GraphError::CorruptValue {
                key: keys::delta_gc_watermark(cell_id, edge_type),
                reason: format!(
                    "latest matrix artifact is at epoch {}, expected {compact_through_epoch}",
                    artifact.base_epoch
                ),
            });
        }

        let mut watermark_batch = WriteBatch::new();
        watermark_batch.put(
            keys::delta_gc_watermark(cell_id, edge_type),
            encode_u64(compact_through_epoch),
        );
        self.write_strict(watermark_batch).await?;

        let mut result = DeltaGcResult {
            compacted_through_epoch: compact_through_epoch,
            ..DeltaGcResult::default()
        };
        self.delete_delta_prefix_through(
            &keys::delta_plus_prefix(cell_id, edge_type),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_delta_prefix_through(
            &keys::delta_minus_prefix(cell_id, edge_type),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        Ok(result)
    }

    async fn delta_gc_watermark(&self, cell_id: &str, edge_type: &str) -> Result<GraphEpoch> {
        self.read_counter(&keys::delta_gc_watermark(cell_id, edge_type))
            .await
    }

    async fn delete_delta_prefix_through(
        &self,
        prefix: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self.scan_remote_prefix(prefix).await?;
        let mut batch = WriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge = decode_edge_record(&key, &kv.value)?;
            if edge.epoch <= compact_through_epoch {
                batch.delete(key.as_bytes());
                result.deleted_delta_keys += 1;
                pending_deletes += 1;
                if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                    self.flush_delta_gc_batch(&mut batch, &mut pending_deletes)
                        .await?;
                }
            } else {
                result.retained_delta_keys += 1;
            }
        }
        self.flush_delta_gc_batch(&mut batch, &mut pending_deletes)
            .await
    }

    async fn flush_delta_gc_batch(
        &self,
        batch: &mut WriteBatch,
        pending_deletes: &mut usize,
    ) -> Result<()> {
        if *pending_deletes == 0 {
            return Ok(());
        }
        let batch_to_write = std::mem::replace(batch, WriteBatch::new());
        self.write_strict(batch_to_write).await?;
        *pending_deletes = 0;
        Ok(())
    }

    pub async fn edge_exists_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<bool> {
        Ok(self
            .edges_at(cell_id, edge_type, read_epoch)
            .await?
            .into_iter()
            .any(|edge| edge.src == src && edge.dst == dst))
    }

    pub async fn out_neighbors_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<VertexId>> {
        let mut neighbors: Vec<_> = self
            .edges_at(cell_id, edge_type, read_epoch)
            .await?
            .into_iter()
            .filter_map(|edge| (edge.src == src).then_some(edge.dst))
            .collect();
        neighbors.sort_unstable();
        Ok(neighbors)
    }

    pub async fn out_degree_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<u64> {
        Ok(self
            .out_neighbors_at(cell_id, edge_type, src, read_epoch)
            .await?
            .len() as u64)
    }

    async fn read_counter(&self, key: &str) -> Result<u64> {
        match self.read_remote(key).await? {
            Some(value) => decode_u64(key, &value),
            None => Ok(0),
        }
    }

    pub(crate) async fn read_remote(&self, key: &str) -> Result<Option<Bytes>> {
        let value = self
            .db
            .get_with_options(key.as_bytes(), &remote_read_options())
            .await?;
        Ok(value)
    }

    pub(crate) async fn scan_remote_prefix(&self, prefix: &str) -> Result<slatedb::DbIterator> {
        let iter = self
            .db
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        Ok(iter)
    }

    pub(crate) async fn scan_remote_prefix_from(
        &self,
        prefix: &str,
        start_suffix: &str,
    ) -> Result<slatedb::DbIterator> {
        let iter = self
            .db
            .scan_prefix_with_options(
                prefix.as_bytes(),
                start_suffix.as_bytes().to_vec()..,
                &remote_scan_options(),
            )
            .await?;
        Ok(iter)
    }

    async fn scan_delta_prefix(
        &self,
        prefix: &str,
        kind: DeltaKind,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
        records: &mut Vec<DeltaRecord>,
    ) -> Result<()> {
        if after_epoch == GraphEpoch::MAX {
            return Ok(());
        }
        let start_suffix = format!("{:020}", after_epoch + 1);
        let mut iter = self.scan_remote_prefix_from(prefix, &start_suffix).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge = decode_edge_record(&key, &kv.value)?;
            if edge.epoch > read_epoch {
                break;
            }
            if edge.epoch > after_epoch {
                records.push(DeltaRecord { kind, edge });
            }
        }
        Ok(())
    }

    pub(crate) async fn write_strict(&self, batch: WriteBatch) -> Result<()> {
        let mut options = WriteOptions::default();
        options.await_durable = true;
        self.db.write_with_options(batch, &options).await?;
        Ok(())
    }
}

async fn read_txn_remote(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    txn.mark_read([key.as_bytes()])?;
    Ok(txn
        .get_with_options(key.as_bytes(), &remote_read_options())
        .await?)
}

async fn read_counter_txn(txn: &DbTransaction, key: &str) -> Result<u64> {
    match read_txn_remote(txn, key).await? {
        Some(value) => decode_u64(key, &value),
        None => Ok(0),
    }
}

async fn next_epoch_txn(txn: &DbTransaction, cell_id: &str) -> Result<GraphEpoch> {
    let current = read_counter_txn(txn, &keys::last_epoch(cell_id)).await?;
    current
        .checked_add(1)
        .ok_or_else(|| GraphError::CorruptValue {
            key: keys::last_epoch(cell_id),
            reason: "epoch overflow".to_string(),
        })
}

async fn commit_txn_strict(txn: DbTransaction) -> Result<()> {
    let mut options = WriteOptions::default();
    options.await_durable = true;
    txn.commit_with_options(&options).await?;
    Ok(())
}

fn remote_read_options() -> ReadOptions {
    let mut options = ReadOptions::default();
    options.durability_filter = DurabilityLevel::Remote;
    options
}

fn remote_scan_options() -> ScanOptions {
    ScanOptions::default()
        .with_durability_filter(DurabilityLevel::Remote)
        .with_cache_blocks(false)
}

fn validate_component(component: &'static str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(GraphError::InvalidKeyComponent {
            component,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn encode_u64(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_u64(key: &str, value: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("expected 8 bytes, got {}", value.len()),
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn encode_edge_record(record: &EdgeRecord) -> Vec<u8> {
    format!(
        "edge1\t{}\t{}\t{}\t{}\t{}\n",
        record.epoch, record.cell_id, record.edge_type, record.src, record.dst
    )
    .into_bytes()
}

fn decode_edge_record(key: &str, value: &[u8]) -> Result<EdgeRecord> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 6 || parts[0] != "edge1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge1 record with 6 fields".to_string(),
        });
    }
    Ok(EdgeRecord {
        epoch: parse_u64(key, parts[1], "epoch")?,
        cell_id: parts[2].to_string(),
        edge_type: parts[3].to_string(),
        src: parse_u64(key, parts[4], "src")?,
        dst: parse_u64(key, parts[5], "dst")?,
    })
}

fn encode_commit_idempotency(mutation: &EdgeMutation, result: &CommitResult) -> Vec<u8> {
    format!(
        "commit2\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.epoch,
        u8::from(result.already_existed),
        mutation.cell_id,
        mutation.edge_type,
        mutation.src,
        mutation.dst
    )
    .into_bytes()
}

fn encode_delete_idempotency(mutation: &EdgeMutation, result: &DeleteResult) -> Vec<u8> {
    format!(
        "delete2\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.epoch,
        u8::from(result.deleted),
        mutation.cell_id,
        mutation.edge_type,
        mutation.src,
        mutation.dst
    )
    .into_bytes()
}

fn encode_bulk_import_idempotency(
    idempotency_key: &str,
    fingerprint: u64,
    result: &BulkImportResult,
) -> Vec<u8> {
    format!(
        "bulk_import1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.start_epoch,
        result.end_epoch,
        result.inserted,
        result.already_existed,
        fingerprint,
        idempotency_key
    )
    .into_bytes()
}

fn encode_mutation_batch_log(
    edge_type: &str,
    idempotency_key: &str,
    fingerprint: u64,
    result: &BulkImportResult,
) -> Vec<u8> {
    format!(
        "mutation_batch1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        edge_type,
        result.start_epoch,
        result.end_epoch,
        result.inserted,
        result.already_existed,
        fingerprint,
        idempotency_key
    )
    .into_bytes()
}

fn decode_bulk_import_idempotency(
    key: &str,
    idempotency_key: &str,
    fingerprint: u64,
    value: &[u8],
) -> Result<BulkImportResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "bulk_import1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected bulk_import1 record with 7 fields".to_string(),
        });
    }
    if parts[6] != idempotency_key || parse_u64(key, parts[5], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "bulk-import",
            idempotency_key: idempotency_key.to_string(),
        });
    }
    Ok(BulkImportResult {
        start_epoch: parse_u64(key, parts[1], "start_epoch")?,
        end_epoch: parse_u64(key, parts[2], "end_epoch")?,
        inserted: parse_u64(key, parts[3], "inserted")?,
        already_existed: parse_u64(key, parts[4], "already_existed")?,
    })
}

fn decode_delete_result(key: &str, value: &[u8]) -> Result<DeleteResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 3 || parts[0] != "delete1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected delete1 record with 3 fields".to_string(),
        });
    }
    let deleted = match parts[2] {
        "0" => false,
        "1" => true,
        other => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid deleted flag {other}"),
            });
        }
    };
    Ok(DeleteResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        deleted,
    })
}

fn decode_delete_idempotency(
    key: &str,
    mutation: &EdgeMutation,
    value: &[u8],
) -> Result<DeleteResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.first() == Some(&"delete1") {
        return decode_delete_result(key, value);
    }
    if parts.len() != 7 || parts[0] != "delete2" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected delete2 record with 7 fields".to_string(),
        });
    }
    ensure_idempotent_edge(key, "delete", mutation, &parts[3..7])?;
    let deleted = decode_bool_flag(key, parts[2], "deleted")?;
    Ok(DeleteResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        deleted,
    })
}

fn decode_commit_result(key: &str, value: &[u8]) -> Result<CommitResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 3 || parts[0] != "commit1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected commit1 record with 3 fields".to_string(),
        });
    }
    let existed = match parts[2] {
        "0" => false,
        "1" => true,
        other => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid already_existed flag {other}"),
            });
        }
    };
    Ok(CommitResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        already_existed: existed,
    })
}

fn decode_commit_idempotency(
    key: &str,
    mutation: &EdgeMutation,
    value: &[u8],
) -> Result<CommitResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.first() == Some(&"commit1") {
        return decode_commit_result(key, value);
    }
    if parts.len() != 7 || parts[0] != "commit2" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected commit2 record with 7 fields".to_string(),
        });
    }
    ensure_idempotent_edge(key, "create", mutation, &parts[3..7])?;
    let already_existed = decode_bool_flag(key, parts[2], "already_existed")?;
    Ok(CommitResult {
        epoch: parse_u64(key, parts[1], "epoch")?,
        already_existed,
    })
}

fn ensure_idempotent_edge(
    key: &str,
    operation: &'static str,
    mutation: &EdgeMutation,
    fields: &[&str],
) -> Result<()> {
    let [cell_id, edge_type, src, dst] = fields else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected idempotency edge identity".to_string(),
        });
    };
    if *cell_id != mutation.cell_id
        || *edge_type != mutation.edge_type
        || parse_u64(key, src, "src")? != mutation.src
        || parse_u64(key, dst, "dst")? != mutation.dst
    {
        return Err(GraphError::IdempotencyConflict {
            operation,
            idempotency_key: mutation.idempotency_key.clone(),
        });
    }
    Ok(())
}

fn decode_bool_flag(key: &str, value: &str, field: &str) -> Result<bool> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        other => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid {field} flag {other}"),
        }),
    }
}

fn bulk_import_fingerprint(cell_id: &str, edge_type: &str, edges: &[(VertexId, VertexId)]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    fn update(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    update(&mut hash, cell_id.as_bytes());
    update(&mut hash, b"\0");
    update(&mut hash, edge_type.as_bytes());
    update(&mut hash, b"\0");
    for (src, dst) in edges {
        update(&mut hash, &src.to_be_bytes());
        update(&mut hash, &dst.to_be_bytes());
    }
    hash
}

fn bulk_import_chunk_order(src: VertexId, dst: VertexId) -> u64 {
    let mut value = src ^ dst.rotate_left(32) ^ 0x9e37_79b9_7f4a_7c15;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> GraphError {
    GraphError::CorruptValue {
        key: "graph/write_authority_lock".to_string(),
        reason: "write authority lock poisoned".to_string(),
    }
}

fn graph_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn encode_delta_record(record: &DeltaRecord) -> Vec<u8> {
    let kind = match record.kind {
        DeltaKind::Plus => "+",
        DeltaKind::Minus => "-",
    };
    format!(
        "delta1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        kind,
        record.edge.epoch,
        record.edge.cell_id,
        record.edge.edge_type,
        record.edge.src,
        record.edge.dst
    )
    .into_bytes()
}

fn decode_delta_record(key: &str, value: &[u8]) -> Result<DeltaRecord> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "delta1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected delta1 record with 7 fields".to_string(),
        });
    }
    let kind = match parts[1] {
        "+" => DeltaKind::Plus,
        "-" => DeltaKind::Minus,
        other => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid delta kind {other}"),
            });
        }
    };
    Ok(DeltaRecord {
        kind,
        edge: EdgeRecord {
            epoch: parse_u64(key, parts[2], "epoch")?,
            cell_id: parts[3].to_string(),
            edge_type: parts[4].to_string(),
            src: parse_u64(key, parts[5], "src")?,
            dst: parse_u64(key, parts[6], "dst")?,
        },
    })
}

fn sort_deltas(records: &mut [DeltaRecord]) {
    records.sort_by_key(|delta| {
        (
            delta.edge.epoch,
            match delta.kind {
                DeltaKind::Plus => 0_u8,
                DeltaKind::Minus => 1_u8,
            },
            delta.edge.src,
            delta.edge.dst,
        )
    });
}

fn parse_u64(key: &str, value: &str, field: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|err| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid {field}: {err}"),
        })
}

fn ensure_limit(operation: &'static str, actual: u64, limit: u64) -> Result<()> {
    if actual > limit {
        Err(GraphError::AdmissionRejected {
            operation,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

mod keys {
    use super::{GraphEpoch, VertexId};

    pub fn last_epoch(cell_id: &str) -> String {
        format!("cell/{cell_id}/meta/last_epoch")
    }

    pub fn idempotency(cell_id: &str, operation: &str, idempotency_key: &str) -> String {
        format!("cell/{cell_id}/idem/{operation}/{idempotency_key}")
    }

    pub fn edge(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
        format!("cell/{cell_id}/edge/{edge_type}/{src:020}/{dst:020}")
    }

    pub fn out_edge(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
        format!("cell/{cell_id}/e/out/{edge_type}/{src:020}/{dst:020}")
    }

    pub fn in_edge(cell_id: &str, edge_type: &str, dst: VertexId, src: VertexId) -> String {
        format!("cell/{cell_id}/e/in/{edge_type}/{dst:020}/{src:020}")
    }

    pub fn out_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
        format!("cell/{cell_id}/e/out/{edge_type}/{src:020}/")
    }

    pub fn in_prefix(cell_id: &str, edge_type: &str, dst: VertexId) -> String {
        format!("cell/{cell_id}/e/in/{edge_type}/{dst:020}/")
    }

    pub fn degree_out_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/cnt/out/{edge_type}/")
    }

    pub fn degree_in_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/cnt/in/{edge_type}/")
    }

    pub fn degree_out(cell_id: &str, edge_type: &str, src: VertexId) -> String {
        format!("cell/{cell_id}/cnt/out/{edge_type}/{src:020}")
    }

    pub fn degree_in(cell_id: &str, edge_type: &str, dst: VertexId) -> String {
        format!("cell/{cell_id}/cnt/in/{edge_type}/{dst:020}")
    }

    pub fn outbox(
        cell_id: &str,
        epoch: GraphEpoch,
        kind: super::DeltaKind,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
    ) -> String {
        let kind = match kind {
            super::DeltaKind::Plus => "plus",
            super::DeltaKind::Minus => "minus",
        };
        format!("cell/{cell_id}/outbox/{epoch:020}/{kind}/{edge_type}/{src:020}/{dst:020}")
    }

    pub fn outbox_prefix(cell_id: &str) -> String {
        format!("cell/{cell_id}/outbox/")
    }

    pub fn mutation_batch(cell_id: &str, start_epoch: GraphEpoch, idempotency_key: &str) -> String {
        format!("cell/{cell_id}/mutation_batch/{start_epoch:020}/{idempotency_key}")
    }

    pub fn delta_plus(
        cell_id: &str,
        edge_type: &str,
        epoch: GraphEpoch,
        src: VertexId,
        dst: VertexId,
    ) -> String {
        format!("cell/{cell_id}/delta/plus/{edge_type}/{epoch:020}/{src:020}/{dst:020}")
    }

    pub fn delta_plus_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/delta/plus/{edge_type}/")
    }

    pub fn delta_minus_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/delta/minus/{edge_type}/")
    }

    pub fn delta_minus(
        cell_id: &str,
        edge_type: &str,
        epoch: GraphEpoch,
        src: VertexId,
        dst: VertexId,
    ) -> String {
        format!("cell/{cell_id}/delta/minus/{edge_type}/{epoch:020}/{src:020}/{dst:020}")
    }

    pub fn delta_gc_watermark(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/meta/delta_gc/{edge_type}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::bytes::Bytes;
    use slatedb::object_store::local::LocalFileSystem;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{PrefixExtractor, PrefixTarget};

    async fn open_test_shard(path: &str, object_store: Arc<dyn ObjectStore>) -> GraphShard {
        GraphShard::open_standalone_writer(path, object_store)
            .await
            .unwrap()
    }

    fn mutation(src: VertexId, dst: VertexId, idempotency_key: &str) -> EdgeMutation {
        EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
            src,
            dst,
            idempotency_key: idempotency_key.to_string(),
        }
    }

    #[tokio::test]
    async fn raw_graph_shard_open_is_read_only() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open("graph/read-only-open", object_store)
            .await
            .unwrap();
        let err = shard
            .write_edge(mutation(1, 2, "read-only"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::WriteRequiresLease {
                operation: "write_edge",
                cell_id
            } if cell_id == "reddit-home"
        ));
    }

    #[tokio::test]
    async fn graph_open_options_wire_slatedb_disk_cache() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache_dir = tempfile::tempdir().unwrap();
        let options = GraphOpenOptions {
            cache: GraphCacheConfig::disk_cache(cache_dir.path(), 64 * 1024 * 1024),
            ..Default::default()
        };
        {
            let shard = GraphShard::open_standalone_writer_with_options(
                "graph/cache-config",
                Arc::clone(&object_store),
                options.clone(),
            )
            .await
            .unwrap();
            shard.write_edge(mutation(1, 2, "cache-1")).await.unwrap();
            shard.close().await.unwrap();
        }

        let reader = GraphShard::open_with_options("graph/cache-config", object_store, options)
            .await
            .unwrap();
        assert!(reader
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
            .await
            .unwrap());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn graph_cache_policy_bounds_entries_and_reports_hits_misses() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = GraphOpenOptions {
            cache_policy: GraphCachePolicy {
                max_matrix_adjacencies: 1,
                max_entries_per_cell: Some(8),
                ..Default::default()
            },
            ..Default::default()
        };
        let path = "graph/cache-policy";
        {
            let writer = GraphShard::open_standalone_writer_with_options(
                path,
                Arc::clone(&object_store),
                options.clone(),
            )
            .await
            .unwrap();
            writer.write_edge(mutation(1, 2, "cache-a")).await.unwrap();
            writer
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "OTHER_EDGE".to_string(),
                    src: 10,
                    dst: 20,
                    idempotency_key: "cache-b".to_string(),
                })
                .await
                .unwrap();
            writer
                .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2, 2)
                .await
                .unwrap();
            writer
                .build_matrix_tiles("reddit-home", "OTHER_EDGE", 2, 2)
                .await
                .unwrap();
            assert_eq!(writer.matrix_cache.lock().await.len(), 1);
            assert!(writer.graph_cache_metrics().evictions >= 1);
            writer.close().await.unwrap();
        }

        let reader = GraphShard::open_with_options(path, object_store, options)
            .await
            .unwrap();
        for _ in 0..2 {
            reader
                .matrix_reachable_with_kernel(
                    "reddit-home",
                    "USER_SUBSCRIBED_TO_SUBREDDIT",
                    &[1],
                    1,
                    2,
                    SparseKernelBackend::RustSparse,
                )
                .await
                .unwrap();
        }
        let metrics = reader.graph_cache_metrics();
        assert!(metrics.matrix_artifact_misses >= 1);
        assert!(metrics.matrix_artifact_hits >= 1);
        assert!(metrics.matrix_adjacency_misses >= 1);
        assert!(metrics.matrix_adjacency_hits >= 1);
        assert!(metrics.hydration_started >= 2);
        assert!(metrics.hydration_completed >= 2);
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn supernode_lookup_prefetches_and_caches_posting_chunks() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "graph/supernode-prefetch";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        {
            let writer = open_test_shard(path, Arc::clone(&object_store)).await;
            for dst in 10..18 {
                writer
                    .write_edge(EdgeMutation {
                        cell_id: "reddit-home".to_string(),
                        edge_type: edge_type.to_string(),
                        src: 1,
                        dst,
                        idempotency_key: format!("prefetch-{dst}"),
                    })
                    .await
                    .unwrap();
            }
            let base_epoch = writer.current_epoch("reddit-home").await.unwrap();
            writer
                .build_supernode_groups("reddit-home", edge_type, base_epoch, 4, 2)
                .await
                .unwrap();
            writer.close().await.unwrap();
        }

        let reader = GraphShard::open_with_options(
            path,
            object_store,
            GraphOpenOptions {
                cache_policy: GraphCachePolicy {
                    prefetch_supernode_chunks: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let read_epoch = reader.current_epoch("reddit-home").await.unwrap();
        let page = reader
            .supernode_page(
                "reddit-home",
                edge_type,
                ArtifactDirection::Out,
                1,
                read_epoch,
                0,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(page.vertices, vec![10, 11]);
        assert!(reader.posting_chunk_cache.lock().await.len() >= 1);
        let metrics = reader.graph_cache_metrics();
        assert!(metrics.supernode_group_misses >= 1);
        assert!(metrics.prefetch_requests >= 1);
        assert!(metrics.posting_chunk_misses >= 1);
        assert!(metrics.posting_chunk_hits >= 1);
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn write_edge_commits_canonical_records_and_outbox() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/write-edge", object_store).await;

        let first = shard.write_edge(mutation(1, 2, "req-1")).await.unwrap();
        assert_eq!(
            first,
            CommitResult {
                epoch: 1,
                already_existed: false
            }
        );
        assert!(shard
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
            .await
            .unwrap());
        assert_eq!(
            shard
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            vec![2]
        );
        assert_eq!(
            shard
                .in_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2)
                .await
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            1
        );

        let retry = shard.write_edge(mutation(1, 2, "req-1")).await.unwrap();
        assert_eq!(retry, first);

        let outbox = shard.outbox_since("reddit-home", 0).await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].kind, DeltaKind::Plus);
        assert_eq!(outbox[0].edge.src, 1);
        assert_eq!(outbox[0].edge.dst, 2);
    }

    #[tokio::test]
    async fn duplicate_edge_with_new_request_does_not_increment_degree() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/duplicate-edge", object_store).await;

        let first = shard.write_edge(mutation(7, 8, "req-1")).await.unwrap();
        let duplicate = shard.write_edge(mutation(7, 8, "req-2")).await.unwrap();

        assert_eq!(first.epoch, duplicate.epoch);
        assert!(!first.already_existed);
        assert!(duplicate.already_existed);
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
                .await
                .unwrap(),
            1
        );
        assert_eq!(shard.outbox_since("reddit-home", 0).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn concurrent_writes_allocate_unique_epochs_through_slate_transactions() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = Arc::new(open_test_shard("graph/concurrent-unique", object_store).await);

        let mut handles = Vec::new();
        for idx in 0..16_u64 {
            let shard = Arc::clone(&shard);
            handles.push(tokio::spawn(async move {
                shard
                    .write_edge(EdgeMutation {
                        cell_id: "reddit-home".to_string(),
                        edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                        src: 1,
                        dst: 1_000 + idx,
                        idempotency_key: format!("concurrent-{idx}"),
                    })
                    .await
            }));
        }

        let mut epochs = Vec::new();
        for handle in handles {
            epochs.push(handle.await.unwrap().unwrap().epoch);
        }
        epochs.sort_unstable();
        assert_eq!(epochs, (1..=16).collect::<Vec<_>>());
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            16
        );
    }

    #[tokio::test]
    async fn concurrent_duplicate_edge_writes_converge_to_one_record() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = Arc::new(open_test_shard("graph/concurrent-duplicate", object_store).await);

        let mut handles = Vec::new();
        for idx in 0..12_u64 {
            let shard = Arc::clone(&shard);
            handles.push(tokio::spawn(async move {
                shard
                    .write_edge(EdgeMutation {
                        cell_id: "reddit-home".to_string(),
                        edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                        src: 7,
                        dst: 8,
                        idempotency_key: format!("same-edge-{idx}"),
                    })
                    .await
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap().unwrap());
        }
        assert_eq!(
            results
                .iter()
                .filter(|result| !result.already_existed)
                .count(),
            1
        );
        assert!(results.iter().all(|result| result.epoch == 1));
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
                .await
                .unwrap(),
            1
        );
        assert_eq!(shard.outbox_since("reddit-home", 0).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn bulk_import_edges_writes_normal_indexes_deltas_and_idempotency() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/bulk-import", object_store).await;

        let result = shard
            .bulk_import_edges(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 2), (1, 3), (1, 2)],
                "bulk-1",
            )
            .await
            .unwrap();
        let retry = shard
            .bulk_import_edges(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 3), (1, 2)],
                "bulk-1",
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BulkImportResult {
                start_epoch: 1,
                end_epoch: 2,
                inserted: 2,
                already_existed: 0
            }
        );
        assert_eq!(retry, result);
        assert_eq!(
            shard
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            shard
                .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
                .await
                .unwrap()
                .iter()
                .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
                .collect::<Vec<_>>(),
            vec![(DeltaKind::Plus, 1, 2, 1), (DeltaKind::Plus, 1, 3, 2)]
        );

        let conflict = shard
            .bulk_import_edges(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 4)],
                "bulk-1",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            GraphError::IdempotencyConflict {
                operation: "bulk-import",
                ref idempotency_key
            } if idempotency_key == "bulk-1"
        ));

        let second = shard
            .bulk_import_edges(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 2), (2, 4)],
                "bulk-2",
            )
            .await
            .unwrap();
        assert_eq!(
            second,
            BulkImportResult {
                start_epoch: 3,
                end_epoch: 3,
                inserted: 1,
                already_existed: 1
            }
        );
        assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn chunked_bulk_import_respects_batch_limits_and_keeps_idempotency() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_limits(
            "graph/bulk-import-chunked",
            object_store,
            GraphLimits {
                max_bulk_import_edges: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let too_large = shard
            .bulk_import_edges(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 2), (1, 3), (1, 4)],
                "bulk-too-large",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            too_large,
            GraphError::AdmissionRejected {
                operation: "bulk_import_edges",
                actual: 3,
                limit: 2
            }
        ));

        let result = shard
            .bulk_import_edges_chunked(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 2), (1, 3), (1, 4), (1, 5), (1, 6)],
                "bulk-chunked",
                2,
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            BulkImportResult {
                start_epoch: 1,
                end_epoch: 5,
                inserted: 5,
                already_existed: 0
            }
        );

        let retry = shard
            .bulk_import_edges_chunked(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 6), (1, 5), (1, 4), (1, 3), (1, 2)],
                "bulk-chunked",
                2,
            )
            .await
            .unwrap();
        assert_eq!(retry, result);
        assert_eq!(
            shard
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            vec![2, 3, 4, 5, 6]
        );
    }

    #[tokio::test]
    async fn write_edges_batch_uses_one_batch_idempotency_and_logs_batch_boundary() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/write-edges-batch", object_store).await;

        let result = shard
            .write_edges_batch(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(7, 10), (7, 11), (7, 10), (8, 12)],
                "batch-create-1",
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            BulkImportResult {
                start_epoch: 1,
                end_epoch: 3,
                inserted: 3,
                already_existed: 0
            }
        );
        assert_eq!(
            shard
                .write_edges_batch(
                    "reddit-home",
                    "USER_SUBSCRIBED_TO_SUBREDDIT",
                    [(8, 12), (7, 11), (7, 10)],
                    "batch-create-1",
                )
                .await
                .unwrap(),
            result
        );

        let mut iter = shard
            .scan_remote_prefix("cell/reddit-home/mutation_batch/")
            .await
            .unwrap();
        let mut logs = Vec::new();
        while let Some(kv) = iter.next().await.unwrap() {
            logs.push((
                String::from_utf8_lossy(&kv.key).into_owned(),
                String::from_utf8_lossy(&kv.value).into_owned(),
            ));
        }
        assert_eq!(logs.len(), 1);
        assert!(logs[0].0.ends_with("/batch-create-1"));
        assert!(logs[0]
            .1
            .starts_with("mutation_batch1\tUSER_SUBSCRIBED_TO_SUBREDDIT\t1\t3\t3\t0\t"));
    }

    #[test]
    fn bulk_import_chunk_order_spreads_layered_supernode_edges() {
        let mut edges: Vec<_> = (0..1_000_u64)
            .flat_map(|index| {
                (1..=12_u64).scan(1_u64, move |src, hop| {
                    let dst = hop * 1_000_000 + index + 1;
                    let edge = (*src, dst);
                    *src = dst;
                    Some(edge)
                })
            })
            .collect();
        edges.sort_unstable_by_key(|(src, dst)| (bulk_import_chunk_order(*src, *dst), *src, *dst));

        let root_edges_in_first_chunk = edges
            .iter()
            .take(1_000)
            .filter(|(src, _)| *src == 1)
            .count();
        assert!(
            root_edges_in_first_chunk < 250,
            "deterministic chunk order concentrated {root_edges_in_first_chunk} root edges"
        );
    }

    #[tokio::test]
    async fn idempotency_keys_are_bound_to_the_original_edge() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/idempotency-conflict", object_store).await;

        shard
            .write_edge(mutation(7, 8, "create-conflict"))
            .await
            .unwrap();
        let create_err = shard
            .write_edge(mutation(7, 9, "create-conflict"))
            .await
            .unwrap_err();
        assert!(matches!(
            create_err,
            GraphError::IdempotencyConflict {
                operation: "create",
                ref idempotency_key
            } if idempotency_key == "create-conflict"
        ));

        shard
            .delete_edge(mutation(7, 8, "delete-conflict"))
            .await
            .unwrap();
        let delete_err = shard
            .delete_edge(mutation(7, 9, "delete-conflict"))
            .await
            .unwrap_err();
        assert!(matches!(
            delete_err,
            GraphError::IdempotencyConflict {
                operation: "delete",
                ref idempotency_key
            } if idempotency_key == "delete-conflict"
        ));
    }

    #[tokio::test]
    async fn delete_edge_publishes_delta_minus_and_snapshot_reads_stay_correct() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/delete-edge", object_store).await;

        let first = shard.write_edge(mutation(1, 2, "create-1")).await.unwrap();
        let second = shard.write_edge(mutation(1, 3, "create-2")).await.unwrap();
        let delete = shard.delete_edge(mutation(1, 2, "delete-1")).await.unwrap();
        let retry = shard.delete_edge(mutation(1, 2, "delete-1")).await.unwrap();
        let absent = shard
            .delete_edge(mutation(1, 99, "delete-absent"))
            .await
            .unwrap();

        assert_eq!(first.epoch, 1);
        assert_eq!(second.epoch, 2);
        assert_eq!(
            delete,
            DeleteResult {
                epoch: 3,
                deleted: true
            }
        );
        assert_eq!(retry, delete);
        assert_eq!(
            absent,
            DeleteResult {
                epoch: 3,
                deleted: false
            }
        );

        assert!(!shard
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
            .await
            .unwrap());
        assert_eq!(
            shard
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            vec![3]
        );
        assert_eq!(
            shard
                .out_neighbors_at("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
                .await
                .unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            shard
                .out_neighbors_at("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 3)
                .await
                .unwrap(),
            vec![3]
        );

        let deltas = shard
            .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
            .await
            .unwrap();
        assert_eq!(
            deltas.iter().map(|delta| delta.kind).collect::<Vec<_>>(),
            vec![DeltaKind::Plus, DeltaKind::Plus, DeltaKind::Minus]
        );
        assert_eq!(
            shard
                .outbox_since("reddit-home", 0)
                .await
                .unwrap()
                .iter()
                .map(|delta| delta.kind)
                .collect::<Vec<_>>(),
            vec![DeltaKind::Plus, DeltaKind::Plus, DeltaKind::Minus]
        );
    }

    #[tokio::test]
    async fn snapshot_api_pins_epoch_across_deletes_and_artifact_rebuilds() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/snapshot-api", object_store).await;
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        shard.write_edge(mutation(1, 2, "create-1")).await.unwrap();
        let epoch_one = shard.snapshot("reddit-home").await.unwrap();

        shard.write_edge(mutation(1, 3, "create-2")).await.unwrap();
        let epoch_two = shard.snapshot("reddit-home").await.unwrap();
        shard
            .build_matrix_tiles("reddit-home", edge_type, epoch_two.read_epoch(), 2)
            .await
            .unwrap();
        shard
            .build_supernode_groups("reddit-home", edge_type, epoch_two.read_epoch(), 2, 2)
            .await
            .unwrap();

        shard.delete_edge(mutation(1, 2, "delete-1")).await.unwrap();
        let latest = shard.snapshot("reddit-home").await.unwrap();

        assert_eq!(epoch_one.read_epoch(), 1);
        assert_eq!(
            epoch_one.out_neighbors(edge_type, 1).await.unwrap(),
            vec![2]
        );
        assert!(epoch_one.edge_exists(edge_type, 1, 2).await.unwrap());
        assert_eq!(
            epoch_one
                .matrix_reachable(edge_type, &[1], 1)
                .await
                .unwrap()
                .vertices,
            vec![2]
        );

        assert_eq!(
            epoch_two.out_neighbors(edge_type, 1).await.unwrap(),
            vec![2, 3]
        );
        assert_eq!(epoch_two.supernode_degree(edge_type, 1).await.unwrap(), 2);
        assert!(epoch_two
            .supernode_edge_exists(edge_type, 1, 2)
            .await
            .unwrap());

        assert_eq!(latest.read_epoch(), 3);
        assert_eq!(latest.out_neighbors(edge_type, 1).await.unwrap(), vec![3]);
        assert!(!latest.edge_exists(edge_type, 1, 2).await.unwrap());
        assert_eq!(latest.supernode_degree(edge_type, 1).await.unwrap(), 1);
        assert_eq!(
            latest
                .matrix_reachable(edge_type, &[1], 1)
                .await
                .unwrap()
                .vertices,
            vec![3]
        );
    }

    #[tokio::test]
    async fn snapshot_at_rejects_future_epochs() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/snapshot-future", object_store).await;

        shard
            .write_edge(mutation(10, 20, "create-1"))
            .await
            .unwrap();
        let err = match shard.snapshot_at("reddit-home", 2).await {
            Ok(_) => panic!("future snapshot should be rejected"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            GraphError::SnapshotAhead {
                ref cell_id,
                read_epoch: 2,
                current_epoch: 1,
            } if cell_id == "reddit-home"
        ));
    }

    #[tokio::test]
    async fn reopened_reader_sees_data_from_object_store() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "graph/reopen";

        {
            let shard = open_test_shard(path, Arc::clone(&object_store)).await;
            shard.write_edge(mutation(100, 200, "req-1")).await.unwrap();
            shard.close().await.unwrap();
        }

        let reopened = open_test_shard(path, object_store).await;
        assert!(reopened
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 100, 200)
            .await
            .unwrap());
        assert_eq!(
            reopened
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 100)
                .await
                .unwrap(),
            vec![200]
        );
        assert_eq!(
            reopened
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 100)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            reopened.outbox_since("reddit-home", 0).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn local_object_store_reopen_reads_from_remote_ground_truth() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = "graph/local-empty-cache";

        {
            let object_store: Arc<dyn ObjectStore> =
                Arc::new(LocalFileSystem::new_with_prefix(tempdir.path()).unwrap());
            let shard = open_test_shard(path, object_store).await;
            shard.write_edge(mutation(500, 600, "req-1")).await.unwrap();
            shard.close().await.unwrap();
        }

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tempdir.path()).unwrap());
        let reopened = open_test_shard(path, object_store).await;
        assert!(reopened
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 500, 600)
            .await
            .unwrap());
        assert_eq!(
            reopened
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 500)
                .await
                .unwrap(),
            vec![600]
        );
    }

    #[tokio::test]
    async fn second_writer_open_fences_first_writer_instance() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "graph/writer-fence";

        let first = open_test_shard(path, Arc::clone(&object_store)).await;
        first.write_edge(mutation(1, 2, "first-1")).await.unwrap();

        let second = open_test_shard(path, object_store).await;
        second.write_edge(mutation(1, 3, "second-1")).await.unwrap();

        let err = first
            .write_edge(mutation(1, 4, "first-fenced"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::Slate(ref slate_err) if matches!(slate_err.kind(), ErrorKind::Closed(_))
        ));
        second.close().await.unwrap();
    }

    #[tokio::test]
    async fn phase0_cluster_runs_multiple_local_shards_on_one_object_store() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = Phase0Cluster::open_cells_standalone_writers(
            "phase0-cluster",
            ["cell-a".to_string(), "cell-b".to_string()],
            object_store,
        )
        .await
        .unwrap();
        assert_eq!(cluster.shard_count(), 2);

        for (cell_id, src, dst) in [("cell-a", 1, 10), ("cell-b", 1, 20)] {
            cluster
                .shard(cell_id)
                .unwrap()
                .write_edge(EdgeMutation {
                    cell_id: cell_id.to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src,
                    dst,
                    idempotency_key: format!("req-{cell_id}"),
                })
                .await
                .unwrap();
        }

        assert_eq!(
            cluster
                .shard("cell-a")
                .unwrap()
                .out_neighbors("cell-a", "FOLLOWS", 1)
                .await
                .unwrap(),
            vec![10]
        );
        assert_eq!(
            cluster
                .shard("cell-b")
                .unwrap()
                .out_neighbors("cell-b", "FOLLOWS", 1)
                .await
                .unwrap(),
            vec![20]
        );
        cluster.close().await.unwrap();
    }

    #[tokio::test]
    async fn routed_cluster_rejects_writes_for_non_owned_cells() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let placement =
            ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-search", "node-b")])
                .unwrap();
        let cluster = RoutedPhase0Cluster::open_owned(
            "phase0-routed-cluster",
            "node-a",
            placement,
            object_store,
        )
        .await
        .unwrap();
        assert_eq!(cluster.local_cells(), vec!["reddit-home"]);

        let unleased = cluster
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "owned-write".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            unleased,
            GraphError::WriteRequiresLease {
                operation: "routed_write",
                ref cell_id
            } if cell_id == "reddit-home"
        ));

        let err = cluster
            .write_edge(EdgeMutation {
                cell_id: "reddit-search".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "wrong-owner".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::ShardNotOwned {
                ref cell_id,
                ref owner_node_id,
                ref local_node_id
            } if cell_id == "reddit-search"
                && owner_node_id == "node-b"
                && local_node_id == "node-a"
        ));
        cluster.close().await.unwrap();
    }

    #[tokio::test]
    async fn control_plane_persists_placement_and_enforces_active_leases() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = GraphControlPlane::open("graph-control/leases", Arc::clone(&object_store))
            .await
            .unwrap();
        let placement =
            ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-search", "node-b")])
                .unwrap();
        control.publish_placement(&placement).await.unwrap();

        let mut cluster = RoutedPhase0Cluster::open_owned_with_control(
            "phase0-control-cluster",
            "node-a",
            &control,
            Arc::clone(&object_store),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
        let first_token = cluster.lease("reddit-home").unwrap().lease_token;
        cluster
            .renew_leases(&control, std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            cluster.lease("reddit-home").unwrap().lease_token,
            first_token
        );
        cluster
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "leased-write".to_string(),
            })
            .await
            .unwrap();

        let failover = ShardPlacement::fixed([("reddit-home", "node-b")]).unwrap();
        control.publish_placement(&failover).await.unwrap();
        let held = control
            .acquire_lease("reddit-home", "node-b", std::time::Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(matches!(
            held,
            GraphError::ShardLeaseHeld {
                ref cell_id,
                ref owner_node_id,
                ..
            } if cell_id == "reddit-home" && owner_node_id == "node-a"
        ));
        cluster.close().await.unwrap();
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn lease_renewer_extends_owned_shard_leases_in_background() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = Arc::new(
            GraphControlPlane::open("graph-control/renewer", Arc::clone(&object_store))
                .await
                .unwrap(),
        );
        let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
        control.publish_placement(&placement).await.unwrap();
        let cluster = RoutedPhase0Cluster::open_owned_with_control(
            "phase0-renewer-cluster",
            "node-a",
            &control,
            object_store,
            std::time::Duration::from_millis(250),
        )
        .await
        .unwrap();
        let first_expiry = cluster.lease("reddit-home").unwrap().expires_at_ms;
        let handle = cluster
            .start_lease_renewer(
                Arc::clone(&control),
                std::time::Duration::from_millis(250),
                std::time::Duration::from_millis(25),
            )
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(750);
        let mut renewed_expiry = first_expiry;
        while std::time::Instant::now() < deadline {
            renewed_expiry = cluster.lease("reddit-home").unwrap().expires_at_ms;
            if renewed_expiry > first_expiry {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(renewed_expiry > first_expiry);
        handle.stop().await.unwrap();
        cluster.close().await.unwrap();
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn graph_node_starts_lease_renewal_automatically() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = Arc::new(
            GraphControlPlane::open("graph-control/node-renewer", Arc::clone(&object_store))
                .await
                .unwrap(),
        );
        control
            .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
            .await
            .unwrap();

        let node = GraphNode::open(
            "phase0-graph-node",
            "node-a",
            Arc::clone(&control),
            object_store,
            std::time::Duration::from_millis(250),
            std::time::Duration::from_millis(25),
        )
        .await
        .unwrap();
        let first_expiry = node.cluster().lease("reddit-home").unwrap().expires_at_ms;
        node.cluster()
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "node-write".to_string(),
            })
            .await
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(750);
        let mut renewed_expiry = first_expiry;
        while std::time::Instant::now() < deadline {
            renewed_expiry = node.cluster().lease("reddit-home").unwrap().expires_at_ms;
            if renewed_expiry > first_expiry {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(renewed_expiry > first_expiry);
        node.close().await.unwrap();
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn control_plane_can_fail_over_after_lease_expiry() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = GraphControlPlane::open("graph-control/failover", object_store)
            .await
            .unwrap();
        let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
        control.publish_placement(&placement).await.unwrap();
        control
            .acquire_lease("reddit-home", "node-a", std::time::Duration::from_millis(5))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        let lease = control
            .failover_expired_cell("reddit-home", "node-b", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(lease.owner_node_id, "node-b");
        assert_eq!(
            control
                .load_placement()
                .await
                .unwrap()
                .owner("reddit-home")
                .unwrap(),
            "node-b"
        );
        control.close().await.unwrap();
    }

    #[tokio::test]
    async fn graph_limits_reject_unbounded_bulk_artifact_and_traversal_work() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_limits(
            "graph/limits",
            object_store,
            GraphLimits {
                max_bulk_import_edges: 2,
                max_artifact_source_epochs: 2,
                max_traversal_hops: 1,
            },
        )
        .await
        .unwrap();
        let bulk_err = shard
            .bulk_import_edges(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 2), (1, 3), (1, 4)],
                "too-large",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            bulk_err,
            GraphError::AdmissionRejected {
                operation: "bulk_import_edges",
                actual: 3,
                limit: 2
            }
        ));

        shard.write_edge(mutation(1, 2, "limit-1")).await.unwrap();
        shard.write_edge(mutation(2, 3, "limit-2")).await.unwrap();
        shard
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2, 2)
            .await
            .unwrap();
        shard.write_edge(mutation(3, 4, "limit-3")).await.unwrap();
        let artifact_err = shard
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 3, 2)
            .await
            .unwrap_err();
        assert!(matches!(
            artifact_err,
            GraphError::AdmissionRejected {
                operation: "build_matrix_tiles",
                actual: 3,
                limit: 2
            }
        ));

        let traversal_err = shard
            .matrix_reachable("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", &[1], 2, 2)
            .await
            .unwrap_err();
        assert!(matches!(
            traversal_err,
            GraphError::AdmissionRejected {
                operation: "matrix_reachable",
                actual: 2,
                limit: 1
            }
        ));
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn repair_report_validates_degrees_and_delta_counts() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/repair-report", object_store).await;
        shard.write_edge(mutation(1, 2, "repair-1")).await.unwrap();
        shard.write_edge(mutation(1, 3, "repair-2")).await.unwrap();
        let report = shard
            .validate_cell_edge_type("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT")
            .await
            .unwrap();
        assert_eq!(report.live_edges, 2);
        assert_eq!(report.delta_records, 2);
        assert!(report.degree_mismatches.is_empty());
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn rollup_artifact_gc_keeps_latest_artifacts_and_retains_snapshot_deltas() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "graph/rollup-artifact-gc";
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        {
            let shard = open_test_shard(path, Arc::clone(&object_store)).await;
            shard
                .write_edge(mutation(1, 2, "rollup-base-1"))
                .await
                .unwrap();
            let epoch_one = shard.current_epoch(cell_id).await.unwrap();
            let first = shard
                .rollup_artifacts(cell_id, edge_type, epoch_one, 2, 2, 1, 2)
                .await
                .unwrap();
            assert_eq!(first.base_epoch, epoch_one);

            shard
                .write_edge(mutation(1, 3, "rollup-base-2"))
                .await
                .unwrap();
            let epoch_two = shard.current_epoch(cell_id).await.unwrap();
            let second = shard
                .rollup_artifacts(cell_id, edge_type, epoch_two, 2, 2, 1, 2)
                .await
                .unwrap();
            assert_eq!(second.base_epoch, epoch_two);

            let gc = shard
                .delete_graph_artifacts_before(cell_id, edge_type, epoch_two)
                .await
                .unwrap();
            assert!(gc.deleted_keys > 0);
            assert!(gc.retained_keys > 0);
            shard.close().await.unwrap();
        }

        let reopened = open_test_shard(path, object_store).await;
        assert!(reopened
            .latest_matrix_artifact(cell_id, edge_type, 1)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            reopened
                .latest_rollup(cell_id, edge_type, 2)
                .await
                .unwrap()
                .unwrap()
                .base_epoch,
            2
        );
        assert_eq!(
            reopened
                .out_neighbors_at(cell_id, edge_type, 1, 1)
                .await
                .unwrap(),
            vec![2]
        );
        assert_eq!(
            reopened
                .matrix_reachable(cell_id, edge_type, &[1], 1, 2)
                .await
                .unwrap()
                .vertices,
            vec![2, 3]
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn delta_gc_requires_rollup_and_preserves_reads_after_watermark() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/delta-gc-rollup", object_store).await;
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        shard.write_edge(mutation(1, 2, "gc-base-1")).await.unwrap();
        shard.write_edge(mutation(1, 3, "gc-base-2")).await.unwrap();
        let base_epoch = shard.current_epoch(cell_id).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        shard
            .write_edge(mutation(1, 4, "gc-after-rollup"))
            .await
            .unwrap();
        let read_epoch = shard.current_epoch(cell_id).await.unwrap();

        let gc = shard
            .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
            .await
            .unwrap();
        assert_eq!(gc.compacted_through_epoch, base_epoch);
        assert!(gc.deleted_delta_keys >= 2);
        assert_eq!(
            shard
                .out_neighbors_at(cell_id, edge_type, 1, read_epoch)
                .await
                .unwrap(),
            vec![2, 3, 4]
        );

        let old_snapshot = shard
            .out_neighbors_at(cell_id, edge_type, 1, base_epoch - 1)
            .await
            .unwrap_err();
        assert!(matches!(
            old_snapshot,
            GraphError::SnapshotExpired {
                ref cell_id,
                ref edge_type,
                min_epoch,
                ..
            } if cell_id == "reddit-home"
                && edge_type == "USER_SUBSCRIBED_TO_SUBREDDIT"
                && min_epoch == base_epoch
        ));
        let raw_deltas = shard.deltas_since(cell_id, edge_type, 0).await.unwrap_err();
        assert!(matches!(raw_deltas, GraphError::SnapshotExpired { .. }));
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_rollup_artifact_builds_publish_one_coherent_epoch() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = Arc::new(open_test_shard("graph/concurrent-rollup", object_store).await);
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        for idx in 0..64_u64 {
            shard
                .write_edge(mutation(
                    1,
                    10_000 + idx,
                    &format!("rollup-concurrent-{idx}"),
                ))
                .await
                .unwrap();
        }
        let base_epoch = shard.current_epoch(cell_id).await.unwrap();

        let first = {
            let shard = Arc::clone(&shard);
            tokio::spawn(async move {
                shard
                    .rollup_artifacts(cell_id, edge_type, base_epoch, 8, 16, 16, 8)
                    .await
            })
        };
        let second = {
            let shard = Arc::clone(&shard);
            tokio::spawn(async move {
                shard
                    .rollup_artifacts(cell_id, edge_type, base_epoch, 8, 16, 16, 8)
                    .await
            })
        };

        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        assert_eq!(first, second);
        assert_eq!(
            shard
                .latest_rollup(cell_id, edge_type, base_epoch)
                .await
                .unwrap(),
            Some(first)
        );
        assert_eq!(
            shard
                .matrix_reachable(cell_id, edge_type, &[1], 1, base_epoch)
                .await
                .unwrap()
                .vertices
                .len(),
            64
        );
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn phase0_cluster_reopens_many_shards_from_local_object_store() {
        let tempdir = tempfile::tempdir().unwrap();
        let cells = ["cell-a", "cell-b", "cell-c", "cell-d"];
        let edge_type = "FOLLOWS";

        {
            let object_store = local_object_store(tempdir.path()).unwrap();
            let cluster = Phase0Cluster::open_cells_standalone_writers(
                "phase0-local-cluster",
                cells,
                object_store,
            )
            .await
            .unwrap();
            for (idx, cell_id) in cells.iter().enumerate() {
                let shard = cluster.shard(cell_id).unwrap();
                let src = 10 + idx as u64;
                for step in 0..4 {
                    shard
                        .write_edge(EdgeMutation {
                            cell_id: (*cell_id).to_string(),
                            edge_type: edge_type.to_string(),
                            src: src + step,
                            dst: src + step + 1,
                            idempotency_key: format!("{cell_id}-chain-{step}"),
                        })
                        .await
                        .unwrap();
                }
                for dst in 100..106 {
                    shard
                        .write_edge(EdgeMutation {
                            cell_id: (*cell_id).to_string(),
                            edge_type: edge_type.to_string(),
                            src: 1000 + idx as u64,
                            dst,
                            idempotency_key: format!("{cell_id}-super-{dst}"),
                        })
                        .await
                        .unwrap();
                }
                let base_epoch = shard.current_epoch(cell_id).await.unwrap();
                shard
                    .build_posting_chunks(cell_id, edge_type, base_epoch, 2)
                    .await
                    .unwrap();
                shard
                    .build_matrix_tiles(cell_id, edge_type, base_epoch, 4)
                    .await
                    .unwrap();
                shard
                    .build_supernode_groups(cell_id, edge_type, base_epoch, 4, 2)
                    .await
                    .unwrap();
            }
            cluster.close().await.unwrap();
        }

        let object_store = local_object_store(tempdir.path()).unwrap();
        let reopened = Phase0Cluster::open_cells("phase0-local-cluster", cells, object_store)
            .await
            .unwrap();
        assert_eq!(reopened.shard_count(), cells.len());
        for (idx, cell_id) in cells.iter().enumerate() {
            let shard = reopened.shard(cell_id).unwrap();
            let read_epoch = shard.current_epoch(cell_id).await.unwrap();
            let src = 10 + idx as u64;
            let traversal = shard
                .matrix_reachable(cell_id, edge_type, &[src], 4, read_epoch)
                .await
                .unwrap();
            assert_eq!(traversal.vertices, vec![src + 1, src + 2, src + 3, src + 4]);
            assert_eq!(
                shard
                    .supernode_degree(cell_id, edge_type, 1000 + idx as u64, read_epoch)
                    .await
                    .unwrap(),
                6
            );
            assert!(shard
                .supernode_edge_exists(cell_id, edge_type, 1000 + idx as u64, 105, read_epoch)
                .await
                .unwrap());
        }
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn env_object_store_loader_supports_phase0_harness() {
        let tempdir = tempfile::tempdir().unwrap();
        let env_path = tempdir.path().join("memory.env");
        std::fs::write(&env_path, "CLOUD_PROVIDER=memory\n").unwrap();

        let object_store =
            object_store_from_env(Some(env_path.to_string_lossy().into_owned())).unwrap();
        let shard = open_test_shard("graph/env-loader", object_store).await;
        shard.write_edge(mutation(700, 701, "req-1")).await.unwrap();
        assert!(shard
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 700, 701)
            .await
            .unwrap());
    }

    #[test]
    fn locality_cell_extractor_covers_phase0_keyspace() {
        let keys = vec![
            keys::last_epoch("reddit-home"),
            keys::idempotency("reddit-home", "create", "req-1"),
            keys::edge("reddit-home", "USER_FOLLOWS_USER", 1, 2),
            keys::out_edge("reddit-home", "USER_FOLLOWS_USER", 1, 2),
            keys::in_edge("reddit-home", "USER_FOLLOWS_USER", 2, 1),
            keys::degree_out("reddit-home", "USER_FOLLOWS_USER", 1),
            keys::degree_in("reddit-home", "USER_FOLLOWS_USER", 2),
            keys::outbox("reddit-home", 1, DeltaKind::Plus, "USER_FOLLOWS_USER", 1, 2),
            keys::delta_plus("reddit-home", "USER_FOLLOWS_USER", 1, 1, 2),
            keys::delta_minus("reddit-home", "USER_FOLLOWS_USER", 2, 1, 2),
            "cell/reddit-home/artifact/posting/USER_FOLLOWS_USER/out/00000000000000000001/00000000000000000002/00000000000000000000".to_string(),
            "cell/reddit-home/artifact/matrix_manifest/USER_FOLLOWS_USER/00000000000000000002".to_string(),
            "cell/reddit-home/artifact/matrix/USER_FOLLOWS_USER/00000000000000000002/out/00000000000000000000/00000000000000000000".to_string(),
            "cell/reddit-home/artifact/supernode/USER_FOLLOWS_USER/out/00000000000000000001/00000000000000000002".to_string(),
            keys::edge("subreddit-programming", "POSTED_IN", 10, 20),
        ];

        let experiment = compare_locality_layouts(keys.iter().map(String::as_str));
        assert!(experiment.segment_extractor_safe());
        assert_eq!(experiment.total_keys, keys.len());
        assert_eq!(experiment.cells["reddit-home"], 14);
        assert_eq!(experiment.cells["subreddit-programming"], 1);
        assert_eq!(
            experiment.recommended_phase0_layout,
            StorageLayout::OneDbPerLocalityCell
        );

        let extractor = LocalityCellExtractor::new();
        let expected = locality_cell_prefix("reddit-home");
        let edge_key = keys::out_edge("reddit-home", "USER_FOLLOWS_USER", 1, 2);
        assert_eq!(
            extractor.prefix(edge_key.as_bytes()),
            Some(expected.as_ref())
        );
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Point(Bytes::from(edge_key))),
            Some(expected.len())
        );
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Prefix(Bytes::from_static(
                b"cell/reddit-home/e/out/"
            ))),
            Some(expected.len())
        );
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Prefix(Bytes::from_static(b"cell/reddit"))),
            None
        );
    }

    #[tokio::test]
    async fn cypher_create_and_match_use_storage_kernel() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/cypher-create-match", object_store).await;

        let write = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "cypher-req-1"),
                "CREATE (u:User {id: 10})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s:Subreddit {id: 20})",
            )
            .await
            .unwrap();
        assert_eq!(
            write,
            QueryOutput::Write(CommitResult {
                epoch: 1,
                already_existed: false
            })
        );

        let neighbors = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "read-req"),
                "MATCH (u {id: 10})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s) RETURN s.id",
            )
            .await
            .unwrap();
        assert_eq!(neighbors, QueryOutput::Vertices(vec![20]));

        let count = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "read-req"),
                "MATCH (u {id: 10})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s {id: 20}) RETURN count(*)",
            )
            .await
            .unwrap();
        assert_eq!(count, QueryOutput::Count(1));
    }

    #[tokio::test]
    async fn cypher_where_and_variable_hops_use_storage_kernel() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/cypher-where-varhop", object_store).await;

        for (idx, (src, dst)) in [(1, 2), (2, 3), (3, 4), (1, 9)].into_iter().enumerate() {
            shard
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src,
                    dst,
                    idempotency_key: format!("cypher-hop-{idx}"),
                })
                .await
                .unwrap();
        }

        let filtered = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "read-req"),
                "MATCH (u {id: 1})-[:FOLLOWS]->(v) WHERE v.id = 9 RETURN v.id",
            )
            .await
            .unwrap();
        assert_eq!(filtered, QueryOutput::Vertices(vec![9]));

        let reachable = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "read-req"),
                "MATCH (u {id: 1})-[:FOLLOWS*2..3]->(v) RETURN v.id",
            )
            .await
            .unwrap();
        assert_eq!(reachable, QueryOutput::Vertices(vec![3, 4]));
    }

    #[tokio::test]
    async fn edge_writes_publish_delta_plus_records_for_builders() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/delta-plus", object_store).await;

        shard.write_edge(mutation(1, 10, "req-1")).await.unwrap();
        shard.write_edge(mutation(1, 11, "req-2")).await.unwrap();

        let deltas = shard
            .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
            .await
            .unwrap();
        assert_eq!(
            deltas,
            vec![
                DeltaRecord {
                    kind: DeltaKind::Plus,
                    edge: EdgeRecord {
                        cell_id: "reddit-home".to_string(),
                        edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                        src: 1,
                        dst: 10,
                        epoch: 1
                    }
                },
                DeltaRecord {
                    kind: DeltaKind::Plus,
                    edge: EdgeRecord {
                        cell_id: "reddit-home".to_string(),
                        edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                        src: 1,
                        dst: 11,
                        epoch: 2
                    }
                }
            ]
        );

        let after_first = shard
            .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].edge.dst, 11);
    }

    #[tokio::test]
    async fn reopened_reader_sees_delta_plus_records() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "graph/reopen-deltas";

        {
            let shard = open_test_shard(path, Arc::clone(&object_store)).await;
            shard.write_edge(mutation(42, 84, "req-1")).await.unwrap();
            shard.close().await.unwrap();
        }

        let reopened = open_test_shard(path, object_store).await;
        let deltas = reopened
            .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
            .await
            .unwrap();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].kind, DeltaKind::Plus);
        assert_eq!(deltas[0].edge.src, 42);
        assert_eq!(deltas[0].edge.dst, 84);
    }

    #[tokio::test]
    async fn posting_and_matrix_artifacts_apply_delta_overlay_for_hot_hops() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/matrix-overlay", object_store).await;

        for (idx, (src, dst)) in [(1, 2), (1, 3), (2, 4), (3, 4), (4, 5)]
            .into_iter()
            .enumerate()
        {
            shard
                .write_edge(mutation(src, dst, &format!("base-{idx}")))
                .await
                .unwrap();
        }
        let base_epoch = shard.current_epoch("reddit-home").await.unwrap();

        let chunks = shard
            .build_posting_chunks("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
            .await
            .unwrap();
        assert!(chunks.iter().any(|chunk| {
            chunk.direction == ArtifactDirection::Out
                && chunk.owner == 1
                && chunk.vertices == vec![2, 3]
        }));

        let artifact = shard
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
            .await
            .unwrap();
        assert_eq!(artifact.edge_count, 5);
        assert!(artifact.out_tiles > 0);
        assert!(artifact.transpose_tiles > 0);

        shard
            .write_edge(mutation(4, 6, "delta-plus"))
            .await
            .unwrap();
        shard
            .delete_edge(mutation(3, 4, "delta-minus"))
            .await
            .unwrap();
        let read_epoch = shard.current_epoch("reddit-home").await.unwrap();

        let posting = shard
            .posting_reachable(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1],
                3,
                read_epoch,
            )
            .await
            .unwrap();
        let matrix = shard
            .matrix_reachable(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1],
                3,
                read_epoch,
            )
            .await
            .unwrap();
        assert_eq!(matrix.base_epoch, base_epoch);
        assert_eq!(matrix.delta_records_applied, 2);
        assert_eq!(matrix.vertices, vec![2, 3, 4, 5, 6]);
        assert_eq!(posting.vertices, matrix.vertices);

        let bench = shard
            .benchmark_hot_hops(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1],
                3,
                read_epoch,
            )
            .await
            .unwrap();
        assert!(bench.matrix_wins);
        assert_eq!(bench.matrix.vertices, bench.posting.vertices);
        assert!(bench.matrix.delta_records_applied < bench.posting.delta_records_applied);
    }

    #[cfg(feature = "graphblas")]
    #[tokio::test]
    async fn graphblas_matrix_kernel_matches_rust_kernel_after_delta_overlay() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/matrix-graphblas", object_store).await;

        for (idx, (src, dst)) in [
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 4),
            (4, 5),
            (42, 100),
            (42, 101),
            (42, 102),
        ]
        .into_iter()
        .enumerate()
        {
            shard
                .write_edge(mutation(src, dst, &format!("graphblas-base-{idx}")))
                .await
                .unwrap();
        }
        let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
        shard
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
            .await
            .unwrap();

        shard
            .write_edge(mutation(4, 6, "graphblas-delta-plus"))
            .await
            .unwrap();
        shard
            .delete_edge(mutation(3, 4, "graphblas-delta-minus"))
            .await
            .unwrap();
        let read_epoch = shard.current_epoch("reddit-home").await.unwrap();

        let rust = shard
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1, 42],
                3,
                read_epoch,
                SparseKernelBackend::RustSparse,
            )
            .await
            .unwrap();
        let graphblas = shard
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1, 42],
                3,
                read_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();

        assert_eq!(
            graphblas.sparse_kernel,
            SparseKernelBackend::SuiteSparseGraphBlas
        );
        assert_eq!(graphblas.vertices, rust.vertices);
        assert_eq!(graphblas.edge_visits, rust.edge_visits);
        assert_eq!(graphblas.delta_records_applied, rust.delta_records_applied);
    }

    #[cfg(feature = "graphblas")]
    #[tokio::test]
    async fn graphblas_matrix_kernel_reuses_compiled_base_matrix_cache() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/matrix-graphblas-cache", object_store).await;

        for (idx, (src, dst)) in [
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 4),
            (4, 5),
            (42, 100),
            (42, 101),
            (42, 102),
        ]
        .into_iter()
        .enumerate()
        {
            shard
                .write_edge(mutation(src, dst, &format!("graphblas-cache-base-{idx}")))
                .await
                .unwrap();
        }
        let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
        shard
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
            .await
            .unwrap();

        assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
        let first = shard
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1, 42],
                3,
                base_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();
        assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
        let second = shard
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1, 42],
                3,
                base_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();
        assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
        assert_eq!(second.vertices, first.vertices);
        assert_eq!(second.delta_records_applied, 0);

        shard
            .write_edge(mutation(4, 6, "graphblas-cache-delta-plus"))
            .await
            .unwrap();
        let read_epoch = shard.current_epoch("reddit-home").await.unwrap();
        let with_delta = shard
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1, 42],
                3,
                read_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();
        assert_eq!(with_delta.delta_records_applied, 1);
        assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
    }

    #[cfg(feature = "graphblas")]
    #[tokio::test]
    async fn graphblas_empty_cache_reader_uses_persisted_csc_artifact() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = open_test_shard(
            "graph/matrix-graphblas-persisted-csc",
            Arc::clone(&object_store),
        )
        .await;

        for (idx, (src, dst)) in [
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 4),
            (4, 5),
            (4, 6),
            (42, 100),
            (42, 101),
            (101, 102),
        ]
        .into_iter()
        .enumerate()
        {
            writer
                .write_edge(mutation(src, dst, &format!("graphblas-csc-base-{idx}")))
                .await
                .unwrap();
        }
        let base_epoch = writer.current_epoch("reddit-home").await.unwrap();
        writer
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
            .await
            .unwrap();
        let expected = writer
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1, 42],
                3,
                base_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();
        writer.close().await.unwrap();

        let reader = open_test_shard("graph/matrix-graphblas-persisted-csc", object_store).await;
        assert_eq!(reader.graphblas_cache.lock().await.len(), 0);
        assert_eq!(reader.matrix_cache.lock().await.len(), 0);

        let actual = reader
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1, 42],
                3,
                base_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();
        assert_eq!(actual.vertices, expected.vertices);
        assert_eq!(actual.edge_visits, expected.edge_visits);
        assert_eq!(actual.delta_records_applied, 0);
        assert_eq!(reader.graphblas_cache.lock().await.len(), 1);
        assert_eq!(reader.matrix_cache.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn supernode_groups_count_exists_intersect_and_page_without_full_scan() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/supernode", object_store).await;

        for dst in 10..16 {
            shard
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "USER_FOLLOWS_USER".to_string(),
                    src: 100,
                    dst,
                    idempotency_key: format!("follow-{dst}"),
                })
                .await
                .unwrap();
        }
        let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
        let groups = shard
            .build_supernode_groups("reddit-home", "USER_FOLLOWS_USER", base_epoch, 4, 2)
            .await
            .unwrap();
        let group = groups
            .iter()
            .find(|group| group.direction == ArtifactDirection::Out && group.vertex_id == 100)
            .unwrap();
        assert_eq!(group.degree, 6);
        assert_eq!(group.chunk_count, 3);
        assert_eq!(group.page_size, 2);
        assert_eq!(
            group
                .chunk_bounds
                .iter()
                .map(|bound| (bound.chunk_id, bound.first, bound.last))
                .collect::<Vec<_>>(),
            vec![(0, 10, 11), (1, 12, 13), (2, 14, 15)]
        );

        assert_eq!(
            shard
                .supernode_degree("reddit-home", "USER_FOLLOWS_USER", 100, base_epoch)
                .await
                .unwrap(),
            6
        );
        assert!(shard
            .supernode_edge_exists("reddit-home", "USER_FOLLOWS_USER", 100, 14, base_epoch)
            .await
            .unwrap());
        let one_hop = shard
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_FOLLOWS_USER",
                &[100],
                1,
                base_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();
        assert_eq!(one_hop.vertices, vec![10, 11, 12, 13, 14, 15]);
        assert_eq!(one_hop.edge_visits, 6);
        assert_eq!(
            shard
                .supernode_intersection(
                    "reddit-home",
                    "USER_FOLLOWS_USER",
                    100,
                    &[11, 14, 99],
                    base_epoch,
                )
                .await
                .unwrap(),
            vec![11, 14]
        );

        let page = shard
            .supernode_page(
                "reddit-home",
                "USER_FOLLOWS_USER",
                ArtifactDirection::Out,
                100,
                base_epoch,
                0,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(page.vertices, vec![10, 11]);
        assert!(page.has_next);

        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "USER_FOLLOWS_USER".to_string(),
                src: 100,
                dst: 16,
                idempotency_key: "follow-16".to_string(),
            })
            .await
            .unwrap();
        shard
            .delete_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "USER_FOLLOWS_USER".to_string(),
                src: 100,
                dst: 11,
                idempotency_key: "unfollow-11".to_string(),
            })
            .await
            .unwrap();
        let read_epoch = shard.current_epoch("reddit-home").await.unwrap();

        assert_eq!(
            shard
                .supernode_degree("reddit-home", "USER_FOLLOWS_USER", 100, read_epoch)
                .await
                .unwrap(),
            6
        );
        assert!(!shard
            .supernode_edge_exists("reddit-home", "USER_FOLLOWS_USER", 100, 11, read_epoch)
            .await
            .unwrap());
        assert!(shard
            .supernode_edge_exists("reddit-home", "USER_FOLLOWS_USER", 100, 16, read_epoch)
            .await
            .unwrap());
        assert_eq!(
            shard
                .supernode_intersection(
                    "reddit-home",
                    "USER_FOLLOWS_USER",
                    100,
                    &[11, 14, 16],
                    read_epoch,
                )
                .await
                .unwrap(),
            vec![14, 16]
        );

        let current_page_0 = shard
            .supernode_page(
                "reddit-home",
                "USER_FOLLOWS_USER",
                ArtifactDirection::Out,
                100,
                read_epoch,
                0,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current_page_0.vertices, vec![10, 12]);
        assert!(current_page_0.has_next);

        let current_page_2 = shard
            .supernode_page(
                "reddit-home",
                "USER_FOLLOWS_USER",
                ArtifactDirection::Out,
                100,
                read_epoch,
                2,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current_page_2.vertices, vec![15, 16]);
        assert!(!current_page_2.has_next);
    }
}
