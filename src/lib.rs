use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use slatedb::bytes::Bytes;
use slatedb::config::{
    DurabilityLevel, PreloadLevel, ReadOptions, ScanOptions, Settings, WriteOptions,
};
use slatedb::object_store::{path::Path, ObjectStore};
use slatedb::{Db, DbTransaction, ErrorKind, IsolationLevel, WriteBatch};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

mod algebra;
#[cfg(feature = "opencypher")]
pub mod opencypher;
mod phase0;
mod placement;
mod sparse_kernel;

pub use algebra::{QueryContext, QueryOutput, QueryStatement};
#[cfg(feature = "opencypher")]
pub use opencypher::{
    parse_cypher, parse_opencypher, CypherFrontend, DefaultCypherFrontend, LibCypherParserFrontend,
};
pub use phase0::{
    local_object_store, object_store_from_env, ArtifactDirection, ArtifactGcResult,
    BenchmarkResult, DeltaGcResult, GraphControlMetricsSnapshot, GraphControlPlane, GraphNode,
    GraphRollup, LeaseRenewalHandle, MatrixArtifact, MatrixTraversalResult, Phase0Cluster,
    PostingChunk, RoutedPhase0Cluster, ShardLease, ShardPlacement, SupernodeGroup, SupernodePage,
    TraversalBackend,
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
    #[error(
        "{operation} snapshot epoch {read_epoch} for cell {cell_id} edge {edge_type} changed while building; current epoch is {current_epoch}"
    )]
    SnapshotChanged {
        operation: &'static str,
        cell_id: String,
        edge_type: String,
        read_epoch: GraphEpoch,
        current_epoch: GraphEpoch,
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
    #[error("{operation} would violate retention for cell {cell_id}: requested epoch {requested_epoch}, safe epoch {safe_epoch}")]
    RetentionViolation {
        operation: &'static str,
        cell_id: String,
        requested_epoch: GraphEpoch,
        safe_epoch: GraphEpoch,
    },
}

pub type Result<T> = std::result::Result<T, GraphError>;

const GRAPH_TXN_MAX_RETRIES: usize = 32;
const GRAPH_DELTA_GC_BATCH_KEYS: usize = 512;
const GRAPH_WRITE_LANES: usize = 64;
pub const DEFAULT_TRUSTED_APPEND_CHUNK_EDGES: usize = 32_768;
// Release profiling showed larger materialization transactions regress from SlateDB
// write-batch and conflict-tracking overhead; keep async drains in the same
// microbatch range as foreground indexed writes.
const GRAPH_MUTATION_LOG_MATERIALIZE_TXN_EDGES: usize = 512;
const GRAPH_STORE_FORMAT_KEY: &str = "graph/meta/format_version";
const GRAPH_STORE_FORMAT_VERSION: u64 = 1;
static GRAPH_READ_LEASE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphLimits {
    pub max_bulk_import_edges: usize,
    pub max_artifact_source_epochs: GraphEpoch,
    pub max_traversal_hops: u8,
    pub max_artifact_build_edges: u64,
}

impl Default for GraphLimits {
    fn default() -> Self {
        Self {
            max_bulk_import_edges: 1_000_000,
            max_artifact_source_epochs: 10_000_000,
            max_traversal_hops: 16,
            max_artifact_build_edges: 10_000_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRetentionPolicy {
    pub min_retained_epochs: GraphEpoch,
    pub read_lease_ttl_ms: u64,
    pub max_read_leases_to_scan: u64,
}

impl Default for GraphRetentionPolicy {
    fn default() -> Self {
        Self {
            min_retained_epochs: 0,
            read_lease_ttl_ms: 60_000,
            max_read_leases_to_scan: 10_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphBackpressurePolicy {
    pub max_concurrent_graph_writes: usize,
    pub max_concurrent_artifact_builds: usize,
    pub max_concurrent_gc_jobs: usize,
}

impl Default for GraphBackpressurePolicy {
    fn default() -> Self {
        Self {
            max_concurrent_graph_writes: 1,
            max_concurrent_artifact_builds: 1,
            max_concurrent_gc_jobs: 1,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
        Self::disk_cache_with_preload(cache_dir, max_cache_size_bytes, true)
    }

    pub fn disk_cache_without_preload(
        cache_dir: impl Into<PathBuf>,
        max_cache_size_bytes: usize,
    ) -> Self {
        Self::disk_cache_with_preload(cache_dir, max_cache_size_bytes, false)
    }

    pub fn disk_cache_with_preload(
        cache_dir: impl Into<PathBuf>,
        max_cache_size_bytes: usize,
        preload_sst_on_startup: bool,
    ) -> Self {
        Self {
            object_store_cache_dir: Some(cache_dir.into()),
            object_store_cache_bytes: Some(max_cache_size_bytes),
            object_store_cache_puts: true,
            preload_sst_on_startup,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphOpenOptions {
    pub limits: GraphLimits,
    pub cache: GraphCacheConfig,
    pub durability: GraphDurabilityConfig,
    pub cache_policy: GraphCachePolicy,
    pub retention_policy: GraphRetentionPolicy,
    pub backpressure_policy: GraphBackpressurePolicy,
    pub index_policy: GraphIndexPolicy,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GraphIndexPolicy {
    #[default]
    Full,
    OutboundOnly,
}

impl GraphIndexPolicy {
    pub fn write_reverse_index(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphDurabilityConfig {
    pub wal_flush_interval_ms: Option<u64>,
    pub await_durable_writes: bool,
}

impl Default for GraphDurabilityConfig {
    fn default() -> Self {
        Self {
            wal_flush_interval_ms: Some(1),
            await_durable_writes: true,
        }
    }
}

impl GraphDurabilityConfig {
    pub fn slatedb_default() -> Self {
        Self {
            wal_flush_interval_ms: Some(100),
            await_durable_writes: true,
        }
    }

    pub fn low_latency_durable(flush_interval_ms: u64) -> Self {
        Self {
            wal_flush_interval_ms: Some(flush_interval_ms.max(1)),
            await_durable_writes: true,
        }
    }

    pub fn with_await_durable_writes(mut self, await_durable_writes: bool) -> Self {
        self.await_durable_writes = await_durable_writes;
        self
    }

    fn apply_to_settings(&self, settings: &mut Settings) {
        settings.flush_interval = self.wal_flush_interval_ms.map(Duration::from_millis);
    }
}

pub(crate) async fn open_graph_db(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
    durability: &GraphDurabilityConfig,
) -> Result<Db> {
    let mut settings = Settings::default();
    cache.apply_to_settings(&mut settings);
    durability.apply_to_settings(&mut settings);
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
    pub max_materialized_supernodes: usize,
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
            max_materialized_supernodes: 512,
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
    MaterializedSupernode,
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
    pub materialized_supernode_hits: u64,
    pub materialized_supernode_misses: u64,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphOperationalMetricsSnapshot {
    pub write_attempts: u64,
    pub write_commits: u64,
    pub write_retries: u64,
    pub stale_write_rejects: u64,
    pub retention_rejects: u64,
    pub bulk_import_batches_profiled: u64,
    pub bulk_import_preflight_us: u64,
    pub bulk_import_batch_build_us: u64,
    pub bulk_import_counter_read_us: u64,
    pub bulk_import_commit_us: u64,
    pub artifact_builds_started: u64,
    pub artifact_builds_completed: u64,
    pub artifact_build_duration_us: u64,
    pub artifact_publish_batches: u64,
    pub artifact_records_published: u64,
    pub artifact_publish_duration_us: u64,
    pub gc_jobs_started: u64,
    pub gc_jobs_completed: u64,
    pub gc_keys_deleted: u64,
    pub gc_duration_us: u64,
    pub read_leases_created: u64,
    pub verifier_runs: u64,
    pub verifier_failures: u64,
    pub verifier_duration_us: u64,
    pub backpressure_waits: u64,
}

#[derive(Default)]
pub(crate) struct GraphOperationalMetrics {
    write_attempts: AtomicU64,
    write_commits: AtomicU64,
    write_retries: AtomicU64,
    stale_write_rejects: AtomicU64,
    retention_rejects: AtomicU64,
    bulk_import_batches_profiled: AtomicU64,
    bulk_import_preflight_us: AtomicU64,
    bulk_import_batch_build_us: AtomicU64,
    bulk_import_counter_read_us: AtomicU64,
    bulk_import_commit_us: AtomicU64,
    artifact_builds_started: AtomicU64,
    artifact_builds_completed: AtomicU64,
    artifact_build_duration_us: AtomicU64,
    artifact_publish_batches: AtomicU64,
    artifact_records_published: AtomicU64,
    artifact_publish_duration_us: AtomicU64,
    gc_jobs_started: AtomicU64,
    gc_jobs_completed: AtomicU64,
    gc_keys_deleted: AtomicU64,
    gc_duration_us: AtomicU64,
    read_leases_created: AtomicU64,
    verifier_runs: AtomicU64,
    verifier_failures: AtomicU64,
    verifier_duration_us: AtomicU64,
    backpressure_waits: AtomicU64,
}

impl GraphOperationalMetrics {
    fn snapshot(&self) -> GraphOperationalMetricsSnapshot {
        GraphOperationalMetricsSnapshot {
            write_attempts: self.write_attempts.load(Ordering::Relaxed),
            write_commits: self.write_commits.load(Ordering::Relaxed),
            write_retries: self.write_retries.load(Ordering::Relaxed),
            stale_write_rejects: self.stale_write_rejects.load(Ordering::Relaxed),
            retention_rejects: self.retention_rejects.load(Ordering::Relaxed),
            bulk_import_batches_profiled: self.bulk_import_batches_profiled.load(Ordering::Relaxed),
            bulk_import_preflight_us: self.bulk_import_preflight_us.load(Ordering::Relaxed),
            bulk_import_batch_build_us: self.bulk_import_batch_build_us.load(Ordering::Relaxed),
            bulk_import_counter_read_us: self.bulk_import_counter_read_us.load(Ordering::Relaxed),
            bulk_import_commit_us: self.bulk_import_commit_us.load(Ordering::Relaxed),
            artifact_builds_started: self.artifact_builds_started.load(Ordering::Relaxed),
            artifact_builds_completed: self.artifact_builds_completed.load(Ordering::Relaxed),
            artifact_build_duration_us: self.artifact_build_duration_us.load(Ordering::Relaxed),
            artifact_publish_batches: self.artifact_publish_batches.load(Ordering::Relaxed),
            artifact_records_published: self.artifact_records_published.load(Ordering::Relaxed),
            artifact_publish_duration_us: self.artifact_publish_duration_us.load(Ordering::Relaxed),
            gc_jobs_started: self.gc_jobs_started.load(Ordering::Relaxed),
            gc_jobs_completed: self.gc_jobs_completed.load(Ordering::Relaxed),
            gc_keys_deleted: self.gc_keys_deleted.load(Ordering::Relaxed),
            gc_duration_us: self.gc_duration_us.load(Ordering::Relaxed),
            read_leases_created: self.read_leases_created.load(Ordering::Relaxed),
            verifier_runs: self.verifier_runs.load(Ordering::Relaxed),
            verifier_failures: self.verifier_failures.load(Ordering::Relaxed),
            verifier_duration_us: self.verifier_duration_us.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
        }
    }
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
    materialized_supernode_hits: AtomicU64,
    materialized_supernode_misses: AtomicU64,
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
            (GraphCacheKind::MaterializedSupernode, true) => &self.materialized_supernode_hits,
            (GraphCacheKind::MaterializedSupernode, false) => &self.materialized_supernode_misses,
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
            materialized_supernode_hits: self.materialized_supernode_hits.load(Ordering::Relaxed),
            materialized_supernode_misses: self
                .materialized_supernode_misses
                .load(Ordering::Relaxed),
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphExportDigest {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: GraphEpoch,
    pub live_edges: u64,
    pub edge_checksum: u64,
    pub out_degree_checksum: u64,
    pub in_degree_checksum: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCorrectnessReport {
    pub cell_id: String,
    pub edge_type: String,
    pub read_epoch: GraphEpoch,
    pub delta_gc_watermark: GraphEpoch,
    pub digest: GraphExportDigest,
    pub canonical_edges: u64,
    pub out_index_edges: u64,
    pub in_index_edges: u64,
    pub degree_counters: u64,
    pub posting_chunks_checked: u64,
    pub matrix_edges_checked: u64,
    pub supernode_groups_checked: u64,
    pub traversal_roots_checked: u64,
    pub mismatch_count: u64,
    pub mismatch_samples: Vec<String>,
}

impl GraphCorrectnessReport {
    pub fn is_clean(&self) -> bool {
        self.mismatch_count == 0
    }
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
pub(crate) struct OutEdgeSegment {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) start_epoch: GraphEpoch,
    pub(crate) end_epoch: GraphEpoch,
    pub(crate) edges: Vec<(GraphEpoch, VertexId)>,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SegmentCompactionResult {
    pub compacted_through_epoch: GraphEpoch,
    pub source_segments: u64,
    pub deleted_segment_keys: u64,
    pub deleted_tombstone_keys: u64,
    pub input_edges: u64,
    pub output_edges: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkImportResult {
    pub start_epoch: GraphEpoch,
    pub end_epoch: GraphEpoch,
    pub inserted: u64,
    pub already_existed: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BulkImportOptions {
    pub duplicate_policy: BulkImportDuplicatePolicy,
    pub delta_log_policy: BulkImportDeltaLogPolicy,
}

impl BulkImportOptions {
    pub fn trusted_append() -> Self {
        Self {
            duplicate_policy: BulkImportDuplicatePolicy::TrustNoExisting,
            delta_log_policy: BulkImportDeltaLogPolicy::Batch,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BulkImportDuplicatePolicy {
    #[default]
    CheckExisting,
    TrustNoExisting,
}

impl BulkImportDuplicatePolicy {
    fn check_existing(self) -> bool {
        matches!(self, Self::CheckExisting)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BulkImportDeltaLogPolicy {
    #[default]
    PerEdge,
    Batch,
}

impl BulkImportDeltaLogPolicy {
    fn write_per_edge(self) -> bool {
        matches!(self, Self::PerEdge)
    }

    fn write_batch(self) -> bool {
        matches!(self, Self::Batch)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutationBatchResult {
    pub start_epoch: GraphEpoch,
    pub end_epoch: GraphEpoch,
    pub inserted: u64,
    pub already_existed: u64,
    pub results: Vec<CommitResult>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeIngestOptions {
    pub batch_size: usize,
}

impl Default for EdgeIngestOptions {
    fn default() -> Self {
        Self { batch_size: 1_024 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeIngestResult {
    pub start_epoch: GraphEpoch,
    pub end_epoch: GraphEpoch,
    pub inserted: u64,
    pub already_existed: u64,
    pub batches: u64,
    pub mutations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutationLogAppendResult {
    pub log_epoch: GraphEpoch,
    pub mutations: u64,
    pub already_appended: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EdgeMutationLogMaterializeResult {
    pub scanned_batches: u64,
    pub materialized_batches: u64,
    pub mutations: u64,
    pub inserted: u64,
    pub already_existed: u64,
    pub last_log_epoch: GraphEpoch,
    pub materialized_log_epoch: GraphEpoch,
    pub current_epoch: GraphEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EdgeMutationLogBatch {
    cell_id: String,
    batch_id: String,
    fingerprint: u64,
    mutations: Vec<EdgeMutation>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct OutboxDeltaBatch {
    cell_id: String,
    edge_type: String,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
    edges: Vec<(VertexId, VertexId)>,
}

pub struct GraphShard {
    db: Db,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) retention_policy: GraphRetentionPolicy,
    pub(crate) cache_metrics: Arc<GraphCacheMetrics>,
    pub(crate) operation_metrics: Arc<GraphOperationalMetrics>,
    hydration_gate: Arc<Semaphore>,
    graph_write_gate: Arc<Semaphore>,
    artifact_build_gate: Arc<Semaphore>,
    gc_gate: Arc<Semaphore>,
    index_policy: GraphIndexPolicy,
    await_durable_writes: bool,
    write_authority: GraphWriteAuthority,
    writer_lanes: Vec<Mutex<()>>,
    matrix_artifact_cache: Mutex<BoundedGraphCache<MatrixCacheKey, phase0::MatrixArtifact>>,
    matrix_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<MatrixAdjacency>>>,
    graphblas_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, Arc<sparse_kernel::CompiledGraphBlasMatrix>>>,
    supernode_group_cache: Mutex<BoundedGraphCache<SupernodeCacheKey, phase0::SupernodeGroup>>,
    posting_chunk_cache: Mutex<BoundedGraphCache<PostingChunkCacheKey, phase0::PostingChunk>>,
    materialized_supernode_cache: Mutex<BoundedGraphCache<SupernodeCacheKey, Arc<Vec<VertexId>>>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheEntryCounts {
    pub matrix_artifacts: usize,
    pub matrix_adjacencies: usize,
    pub graphblas_matrices: usize,
    pub supernode_groups: usize,
    pub posting_chunks: usize,
    pub materialized_supernodes: usize,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphWriteFence {
    cell_id: String,
    owner_node_id: String,
    lease_token: u64,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphReadLease {
    cell_id: String,
    lease_id: String,
    read_epoch: GraphEpoch,
    expires_at_ms: u64,
}

impl From<&phase0::ShardLease> for GraphWriteFence {
    fn from(lease: &phase0::ShardLease) -> Self {
        Self {
            cell_id: lease.cell_id.clone(),
            owner_node_id: lease.owner_node_id.clone(),
            lease_token: lease.lease_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum GraphWriteOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GraphWriteBatch {
    ops: Vec<GraphWriteOp>,
}

impl GraphWriteBatch {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.ops.push(GraphWriteOp::Put(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        ));
    }

    pub(crate) fn delete<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        self.ops
            .push(GraphWriteOp::Delete(Bytes::copy_from_slice(key.as_ref())));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.ops.len()
    }
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
                durability: GraphDurabilityConfig::default(),
                cache_policy: GraphCachePolicy::default(),
                retention_policy: GraphRetentionPolicy::default(),
                backpressure_policy: GraphBackpressurePolicy::default(),
                index_policy: GraphIndexPolicy::default(),
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
                durability: GraphDurabilityConfig::default(),
                cache_policy: GraphCachePolicy::default(),
                retention_policy: GraphRetentionPolicy::default(),
                backpressure_policy: GraphBackpressurePolicy::default(),
                index_policy: GraphIndexPolicy::default(),
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

    #[cfg(feature = "chaos-harness")]
    pub async fn open_chaos_leased_writer_with_options(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        local_node_id: impl Into<String>,
        lease: phase0::ShardLease,
    ) -> Result<Self> {
        let local_node_id = local_node_id.into();
        validate_component("node_id", &local_node_id)?;
        if lease.owner_node_id != local_node_id {
            return Err(GraphError::StaleShardLease {
                cell_id: lease.cell_id.clone(),
                node_id: local_node_id,
                lease_token: lease.lease_token,
            });
        }
        let leases = Arc::new(RwLock::new(BTreeMap::from([(
            lease.cell_id.clone(),
            lease,
        )])));
        Self::open_leased_writer(path, object_store, options, local_node_id, leases).await
    }

    #[cfg(feature = "chaos-harness")]
    pub async fn open_chaos_leased_writer(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        local_node_id: impl Into<String>,
        lease: phase0::ShardLease,
    ) -> Result<Self> {
        Self::open_chaos_leased_writer_with_options(
            path,
            object_store,
            GraphOpenOptions::default(),
            local_node_id,
            lease,
        )
        .await
    }

    async fn open_internal(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        write_authority: GraphWriteAuthority,
    ) -> Result<Self> {
        let db = open_graph_db(path, object_store, &options.cache, &options.durability).await?;
        ensure_store_format(&db, &write_authority).await?;
        let cache_policy = options.cache_policy;
        let backpressure_policy = options.backpressure_policy;
        let tenant_quota = cache_policy.max_entries_per_cell;
        let cache_metrics = Arc::new(GraphCacheMetrics::default());
        let operation_metrics = Arc::new(GraphOperationalMetrics::default());
        let hydration_gate = Arc::new(Semaphore::new(cache_policy.hydration_permits()));
        let graph_write_gate = Arc::new(Semaphore::new(
            backpressure_policy.max_concurrent_graph_writes.max(1),
        ));
        let artifact_build_gate = Arc::new(Semaphore::new(
            backpressure_policy.max_concurrent_artifact_builds.max(1),
        ));
        let gc_gate = Arc::new(Semaphore::new(
            backpressure_policy.max_concurrent_gc_jobs.max(1),
        ));
        Ok(Self {
            db,
            limits: options.limits,
            cache_policy: cache_policy.clone(),
            retention_policy: options.retention_policy,
            cache_metrics,
            operation_metrics,
            hydration_gate,
            graph_write_gate,
            artifact_build_gate,
            gc_gate,
            index_policy: options.index_policy,
            await_durable_writes: options.durability.await_durable_writes,
            write_authority,
            writer_lanes: (0..GRAPH_WRITE_LANES).map(|_| Mutex::new(())).collect(),
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
            materialized_supernode_cache: Mutex::new(BoundedGraphCache::new(
                cache_policy.max_materialized_supernodes,
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

    pub fn graph_operational_metrics(&self) -> GraphOperationalMetricsSnapshot {
        self.operation_metrics.snapshot()
    }

    pub fn graph_index_policy(&self) -> GraphIndexPolicy {
        self.index_policy
    }

    pub(crate) fn writes_reverse_index(&self) -> bool {
        self.index_policy.write_reverse_index()
    }

    fn writer_lane(&self, cell_id: &str) -> &Mutex<()> {
        &self.writer_lanes[writer_lane_index(cell_id)]
    }

    pub async fn graph_cache_entry_counts(&self) -> GraphCacheEntryCounts {
        GraphCacheEntryCounts {
            matrix_artifacts: self.matrix_artifact_cache.lock().await.len(),
            matrix_adjacencies: self.matrix_cache.lock().await.len(),
            graphblas_matrices: self.graphblas_cache.lock().await.len(),
            supernode_groups: self.supernode_group_cache.lock().await.len(),
            posting_chunks: self.posting_chunk_cache.lock().await.len(),
            materialized_supernodes: self.materialized_supernode_cache.lock().await.len(),
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

    pub(crate) async fn acquire_graph_write_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.operation_metrics
            .write_attempts
            .fetch_add(1, Ordering::Relaxed);
        self.acquire_operation_permit(operation, &self.graph_write_gate)
            .await
    }

    pub(crate) async fn acquire_artifact_build_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.operation_metrics
            .artifact_builds_started
            .fetch_add(1, Ordering::Relaxed);
        self.acquire_operation_permit(operation, &self.artifact_build_gate)
            .await
    }

    pub(crate) async fn acquire_gc_permit(
        &self,
        operation: &'static str,
    ) -> Result<OwnedSemaphorePermit> {
        self.operation_metrics
            .gc_jobs_started
            .fetch_add(1, Ordering::Relaxed);
        self.acquire_operation_permit(operation, &self.gc_gate)
            .await
    }

    async fn acquire_operation_permit(
        &self,
        operation: &'static str,
        gate: &Arc<Semaphore>,
    ) -> Result<OwnedSemaphorePermit> {
        if gate.available_permits() == 0 {
            self.operation_metrics
                .backpressure_waits
                .fetch_add(1, Ordering::Relaxed);
        }
        gate.clone()
            .acquire_owned()
            .await
            .map_err(|err| GraphError::CorruptValue {
                key: format!("backpressure/{operation}"),
                reason: format!("operation gate closed: {err}"),
            })
    }

    pub(crate) fn ensure_write_authority(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        self.active_write_lease(cell_id, operation).map(|_| ())
    }

    pub(crate) fn active_write_lease(
        &self,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<Option<phase0::ShardLease>> {
        match &self.write_authority {
            GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresLease {
                operation,
                cell_id: cell_id.to_string(),
            }),
            GraphWriteAuthority::Standalone => Ok(None),
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
                    Ok(Some(lease))
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

    pub(crate) async fn install_write_fence(
        &self,
        cell_id: &str,
        lease: &phase0::ShardLease,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", &lease.owner_node_id)?;
        if lease.cell_id != cell_id {
            return Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }
        let Some(active) = self.active_write_lease(cell_id, "install_write_fence")? else {
            return Ok(());
        };
        if active.owner_node_id != lease.owner_node_id || active.lease_token != lease.lease_token {
            return Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: lease.owner_node_id.clone(),
                lease_token: lease.lease_token,
            });
        }

        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.install_write_fence_txn(cell_id, lease).await {
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

    async fn install_write_fence_txn(
        &self,
        cell_id: &str,
        lease: &phase0::ShardLease,
    ) -> Result<()> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let key = keys::write_fence(cell_id);
        if let Some(value) = read_txn_remote(&txn, &key).await? {
            let current = decode_write_fence(&key, &value)?;
            if current.lease_token > lease.lease_token
                || (current.lease_token == lease.lease_token
                    && current.owner_node_id != lease.owner_node_id)
            {
                return Err(GraphError::StaleShardLease {
                    cell_id: cell_id.to_string(),
                    node_id: lease.owner_node_id.clone(),
                    lease_token: lease.lease_token,
                });
            }
        }
        txn.put(
            key.as_bytes(),
            encode_write_fence(&GraphWriteFence::from(lease)),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await
    }

    async fn validate_write_fence_txn(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        operation: &'static str,
    ) -> Result<()> {
        let Some(lease) = self.active_write_lease(cell_id, operation)? else {
            return Ok(());
        };
        let key = keys::write_fence(cell_id);
        let Some(value) = read_txn_remote(txn, &key).await? else {
            return Err(GraphError::WriteRequiresLease {
                operation,
                cell_id: cell_id.to_string(),
            });
        };
        let fence = decode_write_fence(&key, &value)?;
        if fence.cell_id == cell_id
            && fence.owner_node_id == lease.owner_node_id
            && fence.lease_token == lease.lease_token
        {
            Ok(())
        } else {
            Err(GraphError::StaleShardLease {
                cell_id: cell_id.to_string(),
                node_id: lease.owner_node_id,
                lease_token: lease.lease_token,
            })
        }
    }

    async fn publish_read_lease(&self, cell_id: &str, read_epoch: GraphEpoch) -> Result<()> {
        if self.retention_policy.read_lease_ttl_ms == 0 {
            return Ok(());
        }
        let now_ms = graph_now_millis();
        let lease_id = format!(
            "{now_ms:020}-{:020}",
            GRAPH_READ_LEASE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let lease = GraphReadLease {
            cell_id: cell_id.to_string(),
            lease_id: lease_id.clone(),
            read_epoch,
            expires_at_ms: now_ms.saturating_add(self.retention_policy.read_lease_ttl_ms),
        };
        let mut batch = WriteBatch::new();
        batch.put(
            keys::read_lease(cell_id, &lease_id).as_bytes(),
            encode_read_lease(&lease),
        );
        let options = WriteOptions {
            await_durable: true,
            ..Default::default()
        };
        self.db.write_with_options(batch, &options).await?;
        self.operation_metrics
            .read_leases_created
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn min_active_read_epoch(&self, cell_id: &str) -> Result<Option<GraphEpoch>> {
        if self.retention_policy.read_lease_ttl_ms == 0 {
            return Ok(None);
        }
        let now_ms = graph_now_millis();
        let prefix = keys::read_lease_prefix(cell_id);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut scanned = 0_u64;
        let mut min_epoch = None;
        let mut expired_batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            scanned = scanned.saturating_add(1);
            if scanned > self.retention_policy.max_read_leases_to_scan {
                self.operation_metrics
                    .retention_rejects
                    .fetch_add(1, Ordering::Relaxed);
                return Err(GraphError::AdmissionRejected {
                    operation: "read_lease_scan",
                    actual: scanned,
                    limit: self.retention_policy.max_read_leases_to_scan,
                });
            }
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let lease = decode_read_lease(&key, &kv.value)?;
            if lease.cell_id != cell_id {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "read lease cell id does not match key prefix".to_string(),
                });
            }
            if lease.expires_at_ms <= now_ms {
                expired_batch.delete(key.as_bytes());
                pending_deletes += 1;
                if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                    self.flush_read_lease_gc_batch(
                        cell_id,
                        &mut expired_batch,
                        &mut pending_deletes,
                    )
                    .await?;
                }
            } else {
                min_epoch = Some(min_epoch.map_or(lease.read_epoch, |epoch: GraphEpoch| {
                    epoch.min(lease.read_epoch)
                }));
            }
        }
        self.flush_read_lease_gc_batch(cell_id, &mut expired_batch, &mut pending_deletes)
            .await?;
        Ok(min_epoch)
    }

    async fn flush_read_lease_gc_batch(
        &self,
        cell_id: &str,
        batch: &mut GraphWriteBatch,
        pending_deletes: &mut usize,
    ) -> Result<()> {
        if *pending_deletes == 0 {
            return Ok(());
        }
        let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
        self.write_graph_batch_strict(cell_id, "prune_read_leases", batch_to_write)
            .await?;
        *pending_deletes = 0;
        Ok(())
    }

    async fn delta_gc_safe_epoch(&self, cell_id: &str, edge_type: &str) -> Result<GraphEpoch> {
        let current_epoch = self.current_epoch(cell_id).await?;
        let retained_safe_epoch =
            current_epoch.saturating_sub(self.retention_policy.min_retained_epochs);
        if self.min_active_read_epoch(cell_id).await?.is_some() {
            let watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
            Ok(retained_safe_epoch.min(watermark))
        } else {
            Ok(retained_safe_epoch)
        }
    }

    async fn artifact_gc_safe_keep_epoch(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphEpoch> {
        if self.min_active_read_epoch(cell_id).await?.is_some() {
            return Ok(1);
        }
        if self.retention_policy.min_retained_epochs == 0 {
            return Ok(GraphEpoch::MAX);
        }
        let current_epoch = self.current_epoch(cell_id).await?;
        let oldest_retained_epoch =
            current_epoch.saturating_sub(self.retention_policy.min_retained_epochs);
        if oldest_retained_epoch == 0 {
            return Ok(1);
        }
        Ok(self
            .latest_matrix_artifact(cell_id, edge_type, oldest_retained_epoch)
            .await?
            .map_or(1, |artifact| artifact.base_epoch))
    }

    pub(crate) fn record_retention_reject(
        &self,
        operation: &'static str,
        cell_id: &str,
        requested_epoch: GraphEpoch,
        safe_epoch: GraphEpoch,
    ) -> GraphError {
        self.operation_metrics
            .retention_rejects
            .fetch_add(1, Ordering::Relaxed);
        GraphError::RetentionViolation {
            operation,
            cell_id: cell_id.to_string(),
            requested_epoch,
            safe_epoch,
        }
    }

    pub(crate) fn record_artifact_build_completed(&self, duration: std::time::Duration) {
        self.operation_metrics
            .artifact_builds_completed
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .artifact_build_duration_us
            .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
    }

    pub(crate) fn record_gc_completed(&self, deleted_keys: u64, duration: std::time::Duration) {
        self.operation_metrics
            .gc_jobs_completed
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .gc_keys_deleted
            .fetch_add(deleted_keys, Ordering::Relaxed);
        self.operation_metrics
            .gc_duration_us
            .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
    }

    pub(crate) fn record_verifier_completed(
        &self,
        mismatch_count: u64,
        duration: std::time::Duration,
    ) {
        self.operation_metrics
            .verifier_runs
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .verifier_duration_us
            .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
        if mismatch_count > 0 {
            self.operation_metrics
                .verifier_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_graph_batch_commit(
        &self,
        operation: &'static str,
        record_count: usize,
        duration: std::time::Duration,
    ) {
        if matches!(
            operation,
            "build_posting_chunks"
                | "build_matrix_tiles"
                | "build_supernode_groups"
                | "rollup_artifacts"
        ) {
            self.operation_metrics
                .artifact_publish_batches
                .fetch_add(1, Ordering::Relaxed);
            self.operation_metrics
                .artifact_records_published
                .fetch_add(record_count as u64, Ordering::Relaxed);
            self.operation_metrics
                .artifact_publish_duration_us
                .fetch_add(duration_micros_u64(duration), Ordering::Relaxed);
        }
    }

    fn record_bulk_import_profile(
        &self,
        preflight: std::time::Duration,
        batch_build: std::time::Duration,
        counter_read: std::time::Duration,
        commit: std::time::Duration,
    ) {
        self.operation_metrics
            .bulk_import_batches_profiled
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_preflight_us
            .fetch_add(duration_micros_u64(preflight), Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_batch_build_us
            .fetch_add(duration_micros_u64(batch_build), Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_counter_read_us
            .fetch_add(duration_micros_u64(counter_read), Ordering::Relaxed);
        self.operation_metrics
            .bulk_import_commit_us
            .fetch_add(duration_micros_u64(commit), Ordering::Relaxed);
    }

    pub async fn snapshot(&self, cell_id: &str) -> Result<GraphSnapshot<'_>> {
        validate_component("cell_id", cell_id)?;
        let read_epoch = self.current_epoch(cell_id).await?;
        self.publish_read_lease(cell_id, read_epoch).await?;
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
        self.publish_read_lease(cell_id, read_epoch).await?;
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

        let _permit = self.acquire_graph_write_permit("write_edge").await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.write_edge_txn(&mutation).await {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn write_edge_txn(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "write_edge")
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "create", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_commit_idempotency(&idem_key, mutation, &value);
        }

        let edge_key = keys::out_edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );

        if let Some(value) = read_txn_remote(&txn, &edge_key).await? {
            let record = decode_edge_record(&edge_key, &value)?;
            let result = CommitResult {
                epoch: record.epoch,
                already_existed: true,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_commit_idempotency(mutation, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
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
        let edge_value = encode_edge_record(&record);
        let delta_value = encode_delta_record(&DeltaRecord {
            kind: DeltaKind::Plus,
            edge: record.clone(),
        });
        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let out_degree = read_counter_txn(&txn, &out_degree_key).await? + 1;
        let in_degree = if self.writes_reverse_index() {
            let in_degree_key =
                keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key).await? + 1;
            Some((in_degree_key, in_degree))
        } else {
            None
        };

        txn.put(
            keys::last_epoch(&mutation.cell_id).as_bytes(),
            encode_u64(epoch),
        )?;
        txn.put(
            keys::out_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            &edge_value,
        )?;
        if self.writes_reverse_index() {
            txn.put(
                keys::in_edge(
                    &mutation.cell_id,
                    &mutation.edge_type,
                    mutation.dst,
                    mutation.src,
                )
                .as_bytes(),
                &edge_value,
            )?;
        }
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
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
            &delta_value,
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_commit_idempotency(mutation, &result),
        )?;

        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn delete_edge(&self, mutation: EdgeMutation) -> Result<DeleteResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_edge")?;

        let _permit = self.acquire_graph_write_permit("delete_edge").await?;
        let _writer = self.writer_lane(&mutation.cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self.delete_edge_txn(&mutation).await {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn delete_edge_txn(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "delete_edge")
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "delete", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_delete_idempotency(&idem_key, mutation, &value);
        }

        let canonical_key = keys::edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let edge_key = keys::out_edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );

        let Some(existing) = read_txn_remote(&txn, &edge_key).await? else {
            let current_epoch =
                read_counter_txn(&txn, &keys::last_epoch(&mutation.cell_id)).await?;
            let segment_edge = if self.writes_reverse_index() {
                None
            } else {
                self.out_segment_edge_record_at(
                    &mutation.cell_id,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                    current_epoch,
                )
                .await?
            };
            let Some(segment_edge) = segment_edge else {
                let result = DeleteResult {
                    epoch: current_epoch,
                    deleted: false,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_delete_idempotency(mutation, &result),
                )?;
                commit_txn_strict(txn, self.await_durable_writes).await?;
                return Ok(result);
            };
            let tombstone_key = keys::out_segment_tombstone(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            );
            if let Some(value) = read_txn_remote(&txn, &tombstone_key).await? {
                let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
                if !segment_edge_visible(segment_edge.epoch, Some(tombstone_epoch)) {
                    let result = DeleteResult {
                        epoch: current_epoch,
                        deleted: false,
                    };
                    txn.put(
                        idem_key.as_bytes(),
                        encode_delete_idempotency(mutation, &result),
                    )?;
                    commit_txn_strict(txn, self.await_durable_writes).await?;
                    return Ok(result);
                }
            }
            let epoch = current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_epoch(&mutation.cell_id),
                    reason: "epoch overflow".to_string(),
                })?;
            let result = DeleteResult {
                epoch,
                deleted: true,
            };
            let record = EdgeRecord {
                cell_id: mutation.cell_id.clone(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
                epoch,
            };
            let delta_value = encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Minus,
                edge: record,
            });
            let out_degree_key =
                keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
            let out_degree = read_counter_txn(&txn, &out_degree_key)
                .await?
                .saturating_sub(1);

            txn.put(
                keys::last_epoch(&mutation.cell_id).as_bytes(),
                encode_u64(epoch),
            )?;
            txn.put(tombstone_key.as_bytes(), encode_u64(epoch))?;
            txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
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
                &delta_value,
            )?;
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        };

        decode_edge_record(&edge_key, &existing)?;
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
        let delta_value = encode_delta_record(&DeltaRecord {
            kind: DeltaKind::Minus,
            edge: record.clone(),
        });

        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let out_degree = read_counter_txn(&txn, &out_degree_key)
            .await?
            .saturating_sub(1);
        let in_degree = if self.writes_reverse_index() {
            let in_degree_key =
                keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key)
                .await?
                .saturating_sub(1);
            Some((in_degree_key, in_degree))
        } else {
            None
        };

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
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
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
            &delta_value,
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_delete_idempotency(mutation, &result),
        )?;

        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            BulkImportOptions::default(),
        )
        .await
    }

    pub async fn bulk_append_edges_trusted(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_append_edges_trusted_bounded(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
        )
        .await
    }

    pub async fn bulk_append_edges_trusted_bounded(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        max_edges_per_commit: usize,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if max_edges_per_commit == 0 {
            return Err(GraphError::CorruptValue {
                key: "trusted_append_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }
        let edges: Vec<_> = edges.into_iter().collect();
        if edges.len() > max_edges_per_commit {
            return self
                .bulk_import_edges_chunked_with_options(
                    cell_id,
                    edge_type,
                    edges,
                    idempotency_key,
                    max_edges_per_commit,
                    BulkImportOptions::trusted_append(),
                )
                .await;
        }
        self.bulk_import_edges_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            BulkImportOptions::trusted_append(),
        )
        .await
    }

    pub async fn bulk_append_supernode_segment_trusted(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: impl IntoIterator<Item = VertexId>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if self.writes_reverse_index() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphWrite",
                feature: "segment trusted append requires outbound-only index policy".to_string(),
            });
        }
        self.ensure_write_authority(cell_id, "bulk_append_supernode_segment_trusted")?;

        let mut dsts: Vec<_> = dsts.into_iter().collect();
        ensure_limit(
            "bulk_append_supernode_segment_trusted",
            dsts.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        dsts.sort_unstable();
        dsts.dedup();
        let edges: Vec<_> = dsts.iter().copied().map(|dst| (src, dst)).collect();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _permit = self
            .acquire_graph_write_permit("bulk_append_supernode_segment_trusted")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_append_supernode_segment_trusted_txn(
                    cell_id,
                    edge_type,
                    src,
                    &dsts,
                    idempotency_key,
                    fingerprint,
                )
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub async fn bulk_import_edges_with_options(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        options: BulkImportOptions,
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

        let _permit = self.acquire_graph_write_permit("bulk_import_edges").await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_import_edges_txn(
                    cell_id,
                    edge_type,
                    &edges,
                    idempotency_key,
                    fingerprint,
                    options,
                )
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub async fn write_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeMutationBatchResult> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "write_edge_mutations_batch")?;

        let mutations: Vec<_> = mutations.into_iter().collect();
        if mutations.is_empty() {
            let epoch = self.current_epoch(cell_id).await?;
            return Ok(EdgeMutationBatchResult {
                start_epoch: epoch,
                end_epoch: epoch,
                inserted: 0,
                already_existed: 0,
                results: Vec::new(),
            });
        }
        ensure_limit(
            "write_edge_mutations_batch",
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        for mutation in &mutations {
            validate_component("cell_id", &mutation.cell_id)?;
            validate_component("edge_type", &mutation.edge_type)?;
            validate_component("idempotency_key", &mutation.idempotency_key)?;
            if mutation.cell_id != cell_id {
                return Err(GraphError::CorruptValue {
                    key: format!("cell/{cell_id}/write_edge_mutations_batch"),
                    reason: format!(
                        "batch contains mutation for different cell {}",
                        mutation.cell_id
                    ),
                });
            }
        }

        let _permit = self
            .acquire_graph_write_permit("write_edge_mutations_batch")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_mutations_batch_txn(
                    cell_id,
                    &mutations,
                    "write_edge_mutations_batch",
                    None,
                )
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    pub async fn ingest_edge_mutations(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
        options: EdgeIngestOptions,
    ) -> Result<EdgeIngestResult> {
        validate_component("cell_id", cell_id)?;
        if options.batch_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "edge_ingest_batch_size".to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        if self.limits.max_bulk_import_edges == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "ingest_edge_mutations",
                actual: 1,
                limit: 0,
            });
        }

        let batch_size = options.batch_size.min(self.limits.max_bulk_import_edges);
        let mut chunk = Vec::with_capacity(batch_size);
        let mut start_epoch = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;
        let mut batches = 0_u64;
        let mut mutations_seen = 0_u64;

        for mutation in mutations {
            mutations_seen = mutations_seen.saturating_add(1);
            chunk.push(mutation);
            if chunk.len() == batch_size {
                let result = self
                    .write_edge_mutations_batch(cell_id, std::mem::take(&mut chunk))
                    .await?;
                merge_ingest_batch(
                    &result,
                    &mut start_epoch,
                    &mut end_epoch,
                    &mut inserted,
                    &mut already_existed,
                    &mut batches,
                );
                chunk = Vec::with_capacity(batch_size);
            }
        }
        if !chunk.is_empty() {
            let result = self.write_edge_mutations_batch(cell_id, chunk).await?;
            merge_ingest_batch(
                &result,
                &mut start_epoch,
                &mut end_epoch,
                &mut inserted,
                &mut already_existed,
                &mut batches,
            );
        }

        Ok(EdgeIngestResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            inserted,
            already_existed,
            batches,
            mutations: mutations_seen,
        })
    }

    pub async fn append_edge_mutation_log(
        &self,
        cell_id: &str,
        batch_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeMutationLogAppendResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("batch_id", batch_id)?;
        self.ensure_write_authority(cell_id, "append_edge_mutation_log")?;

        let mutations: Vec<_> = mutations.into_iter().collect();
        if mutations.is_empty() {
            return Ok(EdgeMutationLogAppendResult {
                log_epoch: self
                    .read_counter(&keys::mutation_log_epoch(cell_id))
                    .await?,
                mutations: 0,
                already_appended: false,
            });
        }
        ensure_limit(
            "append_edge_mutation_log",
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        validate_edge_mutations_for_cell(cell_id, &mutations, "append_edge_mutation_log")?;
        let fingerprint = edge_mutation_log_fingerprint(cell_id, batch_id, &mutations);

        let _permit = self
            .acquire_graph_write_permit("append_edge_mutation_log")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .append_edge_mutation_log_txn(cell_id, batch_id, &mutations, fingerprint)
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn append_edge_mutation_log_txn(
        &self,
        cell_id: &str,
        batch_id: &str,
        mutations: &[EdgeMutation],
        fingerprint: u64,
    ) -> Result<EdgeMutationLogAppendResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "append_edge_mutation_log")
            .await?;
        let idem_key = keys::idempotency(cell_id, "mutation-log", batch_id);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_mutation_log_append_idempotency(
                &idem_key,
                batch_id,
                fingerprint,
                &value,
            );
        }

        let current_log_epoch = read_counter_txn(&txn, &keys::mutation_log_epoch(cell_id)).await?;
        let log_epoch =
            current_log_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::mutation_log_epoch(cell_id),
                    reason: "mutation log epoch overflow".to_string(),
                })?;
        let result = EdgeMutationLogAppendResult {
            log_epoch,
            mutations: mutations.len() as u64,
            already_appended: false,
        };
        let batch = EdgeMutationLogBatch {
            cell_id: cell_id.to_string(),
            batch_id: batch_id.to_string(),
            fingerprint,
            mutations: mutations.to_vec(),
        };
        txn.put(
            keys::mutation_log_entry(cell_id, log_epoch, batch_id).as_bytes(),
            encode_edge_mutation_log_batch(&batch),
        )?;
        txn.put(
            keys::mutation_log_epoch(cell_id).as_bytes(),
            encode_u64(log_epoch),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_mutation_log_append_idempotency(batch_id, fingerprint, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn materialize_edge_mutation_log(
        &self,
        cell_id: &str,
        max_batches: usize,
    ) -> Result<EdgeMutationLogMaterializeResult> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "materialize_edge_mutation_log")?;

        let mut result = EdgeMutationLogMaterializeResult {
            materialized_log_epoch: self
                .read_counter(&keys::mutation_log_materialized_epoch(cell_id))
                .await?,
            current_epoch: self.current_epoch(cell_id).await?,
            ..Default::default()
        };
        if max_batches == 0 {
            result.last_log_epoch = self
                .read_counter(&keys::mutation_log_epoch(cell_id))
                .await?;
            return Ok(result);
        }

        while result.materialized_batches < max_batches as u64 {
            let start_suffix = result
                .materialized_log_epoch
                .checked_add(1)
                .map(|epoch| format!("{epoch:020}/"))
                .unwrap_or_else(|| format!("{:020}/", GraphEpoch::MAX));
            let mut iter = self
                .scan_remote_prefix_from(&keys::mutation_log_prefix(cell_id), &start_suffix)
                .await?;
            let mut pending = Vec::new();
            let mut pending_mutations = 0_usize;
            while result.materialized_batches < max_batches as u64 {
                let Some(kv) = iter.next().await? else {
                    break;
                };
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let log_epoch = parse_mutation_log_epoch(&key)?;
                if log_epoch <= result.materialized_log_epoch {
                    continue;
                }
                let batch = decode_edge_mutation_log_batch(&key, &kv.value)?;
                if batch.cell_id != cell_id {
                    return Err(GraphError::CorruptValue {
                        key,
                        reason: format!(
                            "mutation log batch belongs to cell {}, expected {cell_id}",
                            batch.cell_id
                        ),
                    });
                }
                validate_edge_mutations_for_cell(
                    cell_id,
                    &batch.mutations,
                    "materialize_edge_mutation_log",
                )?;
                let materialize_edge_limit = self
                    .limits
                    .max_bulk_import_edges
                    .min(GRAPH_MUTATION_LOG_MATERIALIZE_TXN_EDGES);
                if !pending.is_empty()
                    && pending_mutations.saturating_add(batch.mutations.len())
                        > materialize_edge_limit
                {
                    break;
                }
                pending_mutations = pending_mutations.saturating_add(batch.mutations.len());
                result.scanned_batches = result.scanned_batches.saturating_add(1);
                result.materialized_batches = result.materialized_batches.saturating_add(1);
                result.mutations = result
                    .mutations
                    .saturating_add(batch.mutations.len() as u64);
                result.materialized_log_epoch = log_epoch;
                pending.push((log_epoch, batch.mutations));
            }
            if pending.is_empty() {
                break;
            }
            let last_log_epoch = pending
                .last()
                .map(|(log_epoch, _)| *log_epoch)
                .unwrap_or(result.materialized_log_epoch);
            let batch_result = self
                .materialize_edge_mutation_log_batches(cell_id, last_log_epoch, pending)
                .await?;
            result.inserted = result.inserted.saturating_add(batch_result.inserted);
            result.already_existed = result
                .already_existed
                .saturating_add(batch_result.already_existed);
            result.current_epoch = batch_result.end_epoch;
        }
        result.last_log_epoch = self
            .read_counter(&keys::mutation_log_epoch(cell_id))
            .await?;
        result.current_epoch = self.current_epoch(cell_id).await?;
        Ok(result)
    }

    async fn materialize_edge_mutation_log_batches(
        &self,
        cell_id: &str,
        last_log_epoch: GraphEpoch,
        batches: Vec<(GraphEpoch, Vec<EdgeMutation>)>,
    ) -> Result<EdgeMutationBatchResult> {
        let mutation_count = batches
            .iter()
            .map(|(_, mutations)| mutations.len())
            .sum::<usize>();
        ensure_limit(
            "materialize_edge_mutation_log",
            mutation_count as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let mut mutations = Vec::with_capacity(mutation_count);
        for (_, batch_mutations) in batches {
            mutations.extend(batch_mutations);
        }
        let _permit = self
            .acquire_graph_write_permit("materialize_edge_mutation_log")
            .await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_mutations_batch_txn(
                    cell_id,
                    &mutations,
                    "materialize_edge_mutation_log",
                    Some(last_log_epoch),
                )
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn write_edge_mutations_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
        materialized_log_epoch: Option<GraphEpoch>,
    ) -> Result<EdgeMutationBatchResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await?;

        let mut idempotency_keys = BTreeSet::new();
        for mutation in mutations {
            if !idempotency_keys.insert(mutation.idempotency_key.clone()) {
                return Err(GraphError::IdempotencyConflict {
                    operation: "create",
                    idempotency_key: mutation.idempotency_key.clone(),
                });
            }
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut known_edges = BTreeMap::<(String, VertexId, VertexId), GraphEpoch>::new();
        let mut out_increments = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_increments = BTreeMap::<(String, VertexId), u64>::new();
        let write_reverse_index = self.writes_reverse_index();
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;

        for mutation in mutations {
            let idem_key = keys::idempotency(cell_id, "create", &mutation.idempotency_key);
            if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
                let result = decode_commit_idempotency(&idem_key, mutation, &value)?;
                if result.already_existed {
                    already_existed = already_existed.saturating_add(1);
                } else {
                    inserted = inserted.saturating_add(1);
                }
                results.push(result);
                continue;
            }

            let identity = (mutation.edge_type.clone(), mutation.src, mutation.dst);
            if let Some(epoch) = known_edges.get(&identity).copied() {
                let result = CommitResult {
                    epoch,
                    already_existed: true,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, &result),
                )?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            let edge_key = keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            if let Some(value) = read_txn_remote(&txn, &edge_key).await? {
                let record = decode_edge_record(&edge_key, &value)?;
                let result = CommitResult {
                    epoch: record.epoch,
                    already_existed: true,
                };
                known_edges.insert(identity, record.epoch);
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, &result),
                )?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            next_epoch = next_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: operation.to_string(),
                    reason: "epoch overflow during edge mutation batch".to_string(),
                })?;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
                epoch: next_epoch,
            };
            let result = CommitResult {
                epoch: next_epoch,
                already_existed: false,
            };
            let edge_value = encode_edge_record(&record);
            let delta_value = encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record.clone(),
            });
            txn.put(
                keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
                &edge_value,
            )?;
            if write_reverse_index {
                txn.put(
                    keys::in_edge(cell_id, &mutation.edge_type, mutation.dst, mutation.src)
                        .as_bytes(),
                    &edge_value,
                )?;
            }
            txn.put(
                keys::outbox(
                    cell_id,
                    next_epoch,
                    DeltaKind::Plus,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                )
                .as_bytes(),
                &delta_value,
            )?;
            txn.put(
                idem_key.as_bytes(),
                encode_commit_idempotency(mutation, &result),
            )?;
            known_edges.insert(identity, next_epoch);
            *out_increments
                .entry((mutation.edge_type.clone(), mutation.src))
                .or_insert(0) += 1;
            if write_reverse_index {
                *in_increments
                    .entry((mutation.edge_type.clone(), mutation.dst))
                    .or_insert(0) += 1;
            }
            inserted = inserted.saturating_add(1);
            results.push(result);
        }

        for ((edge_type, src), increment) in out_increments {
            let key = keys::degree_out(cell_id, &edge_type, src);
            let base = read_counter_txn(&txn, &key).await?;
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for ((edge_type, dst), increment) in in_increments {
                let key = keys::degree_in(cell_id, &edge_type, dst);
                let base = read_counter_txn(&txn, &key).await?;
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        if next_epoch > current_epoch {
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(next_epoch))?;
        }
        if let Some(log_epoch) = materialized_log_epoch {
            txn.put(
                keys::mutation_log_materialized_epoch(cell_id).as_bytes(),
                encode_u64(log_epoch),
            )?;
        }

        let inserted_start_epoch = results
            .iter()
            .filter(|result| !result.already_existed)
            .map(|result| result.epoch)
            .min();
        let inserted_end_epoch = results
            .iter()
            .filter(|result| !result.already_existed)
            .map(|result| result.epoch)
            .max();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(EdgeMutationBatchResult {
            start_epoch: inserted_start_epoch.unwrap_or(current_epoch),
            end_epoch: inserted_end_epoch.unwrap_or(next_epoch),
            inserted,
            already_existed,
            results,
        })
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
        self.bulk_import_edges_chunked_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            chunk_size,
            BulkImportOptions::default(),
        )
        .await
    }

    pub async fn bulk_append_edges_trusted_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            chunk_size,
            BulkImportOptions::trusted_append(),
        )
        .await
    }

    pub async fn bulk_import_edges_chunked_with_options(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
        options: BulkImportOptions,
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
                    .bulk_import_edges_with_options(
                        cell_id,
                        edge_type,
                        std::mem::take(&mut chunk),
                        &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                        options,
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
                .bulk_import_edges_with_options(
                    cell_id,
                    edge_type,
                    chunk,
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                    options,
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

    async fn bulk_append_supernode_segment_trusted_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: &[VertexId],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<BulkImportResult> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_append_supernode_segment_trusted")
            .await?;
        let idem_key = keys::idempotency(cell_id, "segment-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }
        let fingerprint_key = segment_import_fingerprint_key(cell_id, edge_type, src, fingerprint);
        if let Some(value) = read_txn_remote(&txn, &fingerprint_key).await? {
            return decode_bulk_import_fingerprint_idempotency(
                &fingerprint_key,
                fingerprint,
                &value,
            );
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let existing =
            out_neighbors_for_src_txn(&txn, cell_id, edge_type, src, current_epoch).await?;
        let inserted_dsts: Vec<_> = dsts
            .iter()
            .copied()
            .filter(|dst| !existing.contains(dst))
            .collect();
        let already_existed = u64::try_from(dsts.len().saturating_sub(inserted_dsts.len()))
            .map_err(|err| GraphError::CorruptValue {
                key: "segment_import".to_string(),
                reason: format!("too many existing edges in one segment import: {err}"),
            })?;
        let inserted =
            u64::try_from(inserted_dsts.len()).map_err(|err| GraphError::CorruptValue {
                key: "segment_import".to_string(),
                reason: format!("too many edges in one segment import: {err}"),
            })?;
        let end_epoch =
            current_epoch
                .checked_add(inserted)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "segment_import".to_string(),
                    reason: "epoch overflow during segment import".to_string(),
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

        if inserted > 0 {
            txn.put(
                keys::out_segment(
                    cell_id,
                    edge_type,
                    src,
                    end_epoch,
                    start_epoch,
                    idempotency_key,
                )
                .as_bytes(),
                encode_out_edge_segment(&inserted_dsts),
            )?;
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(end_epoch))?;
            let degree_key = keys::degree_out(cell_id, edge_type, src);
            let base = if current_epoch == 0 {
                0
            } else {
                read_counter_txn(&txn, &degree_key).await?
            };
            txn.put(degree_key.as_bytes(), encode_u64(base + inserted))?;
            txn.put(
                keys::outbox_batch(
                    cell_id,
                    end_epoch,
                    start_epoch,
                    DeltaKind::Plus,
                    edge_type,
                    idempotency_key,
                )
                .as_bytes(),
                encode_outbox_delta_batch_same_src(
                    cell_id,
                    edge_type,
                    DeltaKind::Plus,
                    start_epoch,
                    end_epoch,
                    src,
                    &inserted_dsts,
                ),
            )?;
        }
        txn.put(
            keys::mutation_batch(cell_id, result.start_epoch, idempotency_key).as_bytes(),
            encode_mutation_batch_log(edge_type, idempotency_key, fingerprint, &result),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;
        txn.put(
            fingerprint_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;

        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    async fn bulk_import_edges_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: &[(VertexId, VertexId)],
        idempotency_key: &str,
        fingerprint: u64,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        let preflight_started = std::time::Instant::now();
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_import_edges")
            .await?;
        let idem_key = keys::idempotency(cell_id, "bulk-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }

        let current_epoch = read_counter_txn(&txn, &keys::last_epoch(cell_id)).await?;
        let fresh_cell = current_epoch == 0;
        let mut already_existed = 0_u64;
        let mut inserted_edges = Vec::new();
        for (src, dst) in edges.iter().copied() {
            if options.duplicate_policy.check_existing()
                && !fresh_cell
                && read_txn_remote(&txn, &keys::out_edge(cell_id, edge_type, src, dst))
                    .await?
                    .is_some()
            {
                already_existed += 1;
                continue;
            }
            inserted_edges.push((src, dst));
        }
        let preflight_elapsed = preflight_started.elapsed();

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

        let write_reverse_index = self.writes_reverse_index();
        let mut out_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        let mut in_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        let batch_build_started = std::time::Instant::now();
        for (offset, (src, dst)) in inserted_edges.iter().copied().enumerate() {
            let epoch = current_epoch + 1 + offset as u64;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                epoch,
            };
            let edge_value = encode_edge_record(&record);
            let delta_value = encode_delta_record(&DeltaRecord {
                kind: DeltaKind::Plus,
                edge: record.clone(),
            });
            txn.put(
                keys::out_edge(cell_id, edge_type, src, dst).as_bytes(),
                &edge_value,
            )?;
            if write_reverse_index {
                txn.put(
                    keys::in_edge(cell_id, edge_type, dst, src).as_bytes(),
                    &edge_value,
                )?;
            }
            if options.delta_log_policy.write_per_edge() {
                txn.put(
                    keys::outbox(cell_id, epoch, DeltaKind::Plus, edge_type, src, dst).as_bytes(),
                    &delta_value,
                )?;
            }
            *out_increments.entry(src).or_insert(0) += 1;
            if write_reverse_index {
                *in_increments.entry(dst).or_insert(0) += 1;
            }
        }
        let batch_build_elapsed = batch_build_started.elapsed();

        let counter_read_started = std::time::Instant::now();
        for (src, increment) in out_increments {
            let key = keys::degree_out(cell_id, edge_type, src);
            let base = if fresh_cell {
                0
            } else {
                read_counter_txn(&txn, &key).await?
            };
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for (dst, increment) in in_increments {
                let key = keys::degree_in(cell_id, edge_type, dst);
                let base = if fresh_cell {
                    0
                } else {
                    read_counter_txn(&txn, &key).await?
                };
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        let counter_read_elapsed = counter_read_started.elapsed();
        if inserted > 0 {
            txn.put(keys::last_epoch(cell_id).as_bytes(), encode_u64(end_epoch))?;
            if options.delta_log_policy.write_batch() {
                txn.put(
                    keys::outbox_batch(
                        cell_id,
                        end_epoch,
                        start_epoch,
                        DeltaKind::Plus,
                        edge_type,
                        idempotency_key,
                    )
                    .as_bytes(),
                    encode_outbox_delta_batch(
                        cell_id,
                        edge_type,
                        DeltaKind::Plus,
                        start_epoch,
                        end_epoch,
                        &inserted_edges,
                    ),
                )?;
            }
        }
        txn.put(
            keys::mutation_batch(cell_id, result.start_epoch, idempotency_key).as_bytes(),
            encode_mutation_batch_log(edge_type, idempotency_key, fingerprint, &result),
        )?;
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;

        let commit_started = std::time::Instant::now();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        let commit_elapsed = commit_started.elapsed();
        self.record_bulk_import_profile(
            preflight_elapsed,
            batch_build_elapsed,
            counter_read_elapsed,
            commit_elapsed,
        );
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
        #[cfg(feature = "opencypher")]
        {
            let statement = parse_opencypher(query)?;
            self.execute_query_statement(context, statement).await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
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
                let vertices = self
                    .reachable_vertices_in_hop_range_at(
                        &context.cell_id,
                        &edge_type,
                        src,
                        min_hops,
                        max_hops,
                        read_epoch,
                    )
                    .await?
                    .0;
                if return_count {
                    Ok(QueryOutput::Count(vertices.len() as u64))
                } else {
                    Ok(QueryOutput::Vertices(vertices))
                }
            }
        }
    }

    async fn reachable_vertices_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        read_epoch: GraphEpoch,
    ) -> Result<(Vec<VertexId>, u64)> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if min_hops > max_hops {
            return Err(GraphError::UnsupportedQuery {
                dialect: "OpenCypher",
                feature: "invalid variable-length hop range".to_string(),
            });
        }
        ensure_limit(
            "cypher_match_reachable",
            u64::from(max_hops),
            u64::from(self.limits.max_traversal_hops),
        )?;

        let mut adjacency = BTreeMap::<VertexId, BTreeSet<VertexId>>::new();
        for edge in self.edges_at(cell_id, edge_type, read_epoch).await? {
            adjacency.entry(edge.src).or_default().insert(edge.dst);
        }

        let mut result = BTreeSet::new();
        if min_hops == 0 {
            result.insert(src);
        }
        if max_hops == 0 {
            return Ok((result.into_iter().collect(), 0));
        }

        let mut frontier = BTreeSet::from([src]);
        let mut edge_visits = 0_u64;
        for depth in 1..=max_hops {
            let mut next = BTreeSet::new();
            for vertex in &frontier {
                if let Some(neighbors) = adjacency.get(vertex) {
                    edge_visits = edge_visits.saturating_add(neighbors.len() as u64);
                    next.extend(neighbors.iter().copied());
                }
            }
            if depth >= min_hops {
                result.extend(next.iter().copied());
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Ok((result.into_iter().collect(), edge_visits))
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
        let key = keys::out_edge(cell_id, edge_type, src, dst);
        if self.read_remote(&key).await?.is_some() {
            return Ok(true);
        }
        let read_epoch = self.current_epoch(cell_id).await?;
        Ok(self
            .out_segment_edge_record_at(cell_id, edge_type, src, dst, read_epoch)
            .await?
            .is_some())
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
        let read_epoch = self.current_epoch(cell_id).await?;
        let tombstones = self
            .scan_out_segment_tombstones_for_src_at(cell_id, edge_type, src, read_epoch)
            .await?;
        neighbors.extend(
            self.scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch)
                .await?
                .into_iter()
                .filter(|edge| segment_edge_visible(edge.epoch, tombstones.get(&edge.dst).copied()))
                .map(|edge| edge.dst),
        );
        neighbors.sort_unstable();
        neighbors.dedup();
        Ok(neighbors)
    }

    async fn out_segment_edge_record_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Option<EdgeRecord>> {
        let tombstone_epoch = self
            .out_segment_tombstone_epoch_at(cell_id, edge_type, src, dst, read_epoch)
            .await?;
        let mut latest = None;
        for edge in self
            .scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch)
            .await?
        {
            if edge.dst == dst && segment_edge_visible(edge.epoch, tombstone_epoch) {
                latest = Some(edge);
            }
        }
        Ok(latest)
    }

    async fn scan_out_segments_for_src_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<EdgeRecord>> {
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut edges = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > read_epoch {
                break;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                if epoch > read_epoch {
                    break;
                }
                edges.push(EdgeRecord {
                    cell_id: segment.cell_id.clone(),
                    edge_type: segment.edge_type.clone(),
                    src: segment.src,
                    dst,
                    epoch,
                });
            }
        }
        Ok(edges)
    }

    async fn out_segment_tombstone_epoch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<Option<GraphEpoch>> {
        let key = keys::out_segment_tombstone(cell_id, edge_type, src, dst);
        let Some(value) = self.read_remote(&key).await? else {
            return Ok(None);
        };
        let epoch = decode_u64(&key, &value)?;
        Ok((epoch <= read_epoch).then_some(epoch))
    }

    async fn scan_out_segment_tombstones_for_src_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: GraphEpoch,
    ) -> Result<BTreeMap<VertexId, GraphEpoch>> {
        let prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, key_src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert(dst, epoch);
            }
        }
        Ok(tombstones)
    }

    async fn out_segment_tombstones_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<BTreeMap<(VertexId, VertexId), GraphEpoch>> {
        let prefix = keys::out_segment_tombstone_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert((src, dst), epoch);
            }
        }
        Ok(tombstones)
    }

    pub(crate) async fn out_segment_edge_pairs_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<BTreeSet<(VertexId, VertexId)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::out_segment_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let tombstones = self
            .out_segment_tombstones_at(cell_id, edge_type, read_epoch)
            .await?;
        let mut pairs = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > read_epoch {
                continue;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                if epoch > read_epoch {
                    break;
                }
                let tombstone_epoch = tombstones.get(&(segment.src, dst)).copied();
                if segment_edge_visible(epoch, tombstone_epoch) {
                    pairs.insert((segment.src, dst));
                }
            }
        }
        Ok(pairs)
    }

    pub async fn in_neighbors(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if !self.writes_reverse_index() {
            let read_epoch = self.current_epoch(cell_id).await?;
            let mut neighbors: Vec<_> = self
                .edges_at(cell_id, edge_type, read_epoch)
                .await?
                .into_iter()
                .filter_map(|edge| (edge.dst == dst).then_some(edge.src))
                .collect();
            neighbors.sort_unstable();
            return Ok(neighbors);
        }
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
        records.extend(
            self.scan_outbox_delta_batches_between(cell_id, None, after_epoch, GraphEpoch::MAX)
                .await?,
        );
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

        let mut records = self
            .scan_outbox_deltas_between(cell_id, edge_type, after_epoch, read_epoch)
            .await?;
        records.extend(
            self.scan_outbox_delta_batches_between(
                cell_id,
                Some(edge_type),
                after_epoch,
                read_epoch,
            )
            .await?,
        );

        let final_watermark = self.delta_gc_watermark(cell_id, edge_type).await?;
        if after_epoch < final_watermark {
            return Err(GraphError::SnapshotExpired {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                read_epoch: after_epoch,
                min_epoch: final_watermark,
            });
        }

        sort_deltas(&mut records);
        Ok(records)
    }

    async fn scan_outbox_delta_batches_between(
        &self,
        cell_id: &str,
        edge_type: Option<&str>,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        let start_suffix = after_epoch
            .checked_add(1)
            .map(|epoch| format!("{epoch:020}/"))
            .unwrap_or_else(|| format!("{:020}/", GraphEpoch::MAX));
        let mut iter = self
            .scan_remote_prefix_from(&keys::outbox_batch_prefix(cell_id), &start_suffix)
            .await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let batch = decode_outbox_delta_batch(&key, &kv.value)?;
            if batch.start_epoch > read_epoch {
                break;
            }
            if let Some(edge_type) = edge_type {
                if batch.edge_type != edge_type {
                    continue;
                }
            }
            for (offset, (src, dst)) in batch.edges.iter().copied().enumerate() {
                let epoch = batch.start_epoch + offset as u64;
                if epoch <= after_epoch {
                    continue;
                }
                if epoch > read_epoch {
                    break;
                }
                records.push(DeltaRecord {
                    kind: batch.kind,
                    edge: EdgeRecord {
                        cell_id: batch.cell_id.clone(),
                        edge_type: batch.edge_type.clone(),
                        src,
                        dst,
                        epoch,
                    },
                });
            }
        }
        sort_deltas(&mut records);
        Ok(records)
    }

    async fn scan_outbox_deltas_between(
        &self,
        cell_id: &str,
        edge_type: &str,
        after_epoch: GraphEpoch,
        read_epoch: GraphEpoch,
    ) -> Result<Vec<DeltaRecord>> {
        let start_suffix = after_epoch
            .checked_add(1)
            .map(|epoch| format!("{epoch:020}/"))
            .unwrap_or_else(|| format!("{:020}/", GraphEpoch::MAX));
        let mut iter = self
            .scan_remote_prefix_from(&keys::outbox_prefix(cell_id), &start_suffix)
            .await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_delta_record(&key, &kv.value)?;
            if record.edge.epoch > read_epoch {
                break;
            }
            if record.edge.edge_type == edge_type && record.edge.epoch > after_epoch {
                records.push(record);
            }
        }
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
        if self.writes_reverse_index() {
            for (dst, expected) in in_counts {
                let actual = self
                    .read_counter(&keys::degree_in(cell_id, edge_type, dst))
                    .await?;
                if actual != expected {
                    degree_mismatches.push(format!("in:{dst}:expected={expected}:actual={actual}"));
                }
            }
        } else {
            let mut iter = self
                .scan_remote_prefix(&keys::degree_in_prefix(cell_id, edge_type))
                .await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key);
                degree_mismatches.push(format!("in:{key}:unexpected-under-outbound-only"));
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
        let _permit = self
            .acquire_gc_permit("delete_deltas_through_rollup")
            .await?;
        let started = std::time::Instant::now();
        let safe_epoch = self.delta_gc_safe_epoch(cell_id, edge_type).await?;
        if compact_through_epoch > safe_epoch {
            return Err(self.record_retention_reject(
                "delete_deltas_through_rollup",
                cell_id,
                compact_through_epoch,
                safe_epoch,
            ));
        }
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

        let mut watermark_batch = GraphWriteBatch::new();
        watermark_batch.put(
            keys::delta_gc_watermark(cell_id, edge_type),
            encode_u64(compact_through_epoch),
        );
        self.write_graph_batch_strict(cell_id, "delete_deltas_through_rollup", watermark_batch)
            .await?;

        let mut result = DeltaGcResult {
            compacted_through_epoch: compact_through_epoch,
            ..DeltaGcResult::default()
        };
        self.delete_outbox_deltas_through(cell_id, edge_type, compact_through_epoch, &mut result)
            .await?;
        self.delete_outbox_delta_batches_through(
            cell_id,
            edge_type,
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_delta_prefix_through(
            cell_id,
            &keys::delta_plus_prefix(cell_id, edge_type),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_delta_prefix_through(
            cell_id,
            &keys::delta_minus_prefix(cell_id, edge_type),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_owner_delta_prefix_through(
            cell_id,
            &keys::owner_delta_kind_prefix(cell_id, edge_type, DeltaKind::Plus),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        self.delete_owner_delta_prefix_through(
            cell_id,
            &keys::owner_delta_kind_prefix(cell_id, edge_type, DeltaKind::Minus),
            compact_through_epoch,
            &mut result,
        )
        .await?;
        tracing::info!(
            target: "slatedb_graph_kernel",
            cell_id,
            edge_type,
            compact_through_epoch,
            deleted_delta_keys = result.deleted_delta_keys,
            retained_delta_keys = result.retained_delta_keys,
            "deleted graph deltas through rollup"
        );
        self.record_gc_completed(result.deleted_delta_keys, started.elapsed());
        Ok(result)
    }

    pub(crate) async fn delta_gc_watermark(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<GraphEpoch> {
        self.read_counter(&keys::delta_gc_watermark(cell_id, edge_type))
            .await
    }

    async fn delete_delta_prefix_through(
        &self,
        cell_id: &str,
        prefix: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self.scan_remote_prefix(prefix).await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge = decode_edge_record(&key, &kv.value)?;
            if edge.epoch <= compact_through_epoch {
                batch.delete(key.as_bytes());
                result.deleted_delta_keys += 1;
                pending_deletes += 1;
                if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                    self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                        .await?;
                }
            } else {
                result.retained_delta_keys += 1;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn delete_outbox_deltas_through(
        &self,
        cell_id: &str,
        edge_type: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::outbox_prefix(cell_id))
            .await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let delta = decode_delta_record(&key, &kv.value)?;
            if delta.edge.epoch > compact_through_epoch {
                break;
            }
            if delta.edge.edge_type != edge_type {
                continue;
            }
            batch.delete(key.as_bytes());
            result.deleted_delta_keys += 1;
            pending_deletes += 1;
            if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                    .await?;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn delete_outbox_delta_batches_through(
        &self,
        cell_id: &str,
        edge_type: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self
            .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
            .await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let delta_batch = decode_outbox_delta_batch(&key, &kv.value)?;
            if delta_batch.end_epoch > compact_through_epoch {
                break;
            }
            if delta_batch.edge_type != edge_type {
                continue;
            }
            batch.delete(key.as_bytes());
            result.deleted_delta_keys += 1;
            pending_deletes += 1;
            if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                    .await?;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn delete_owner_delta_prefix_through(
        &self,
        cell_id: &str,
        prefix: &str,
        compact_through_epoch: GraphEpoch,
        result: &mut DeltaGcResult,
    ) -> Result<()> {
        let mut iter = self.scan_remote_prefix(prefix).await?;
        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let delta = decode_delta_record(&key, &kv.value)?;
            if delta.edge.epoch <= compact_through_epoch {
                batch.delete(key.as_bytes());
                result.deleted_delta_keys += 1;
                pending_deletes += 1;
                if pending_deletes >= GRAPH_DELTA_GC_BATCH_KEYS {
                    self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
                        .await?;
                }
            } else {
                result.retained_delta_keys += 1;
            }
        }
        self.flush_delta_gc_batch(cell_id, &mut batch, &mut pending_deletes)
            .await
    }

    async fn flush_delta_gc_batch(
        &self,
        cell_id: &str,
        batch: &mut GraphWriteBatch,
        pending_deletes: &mut usize,
    ) -> Result<()> {
        if *pending_deletes == 0 {
            return Ok(());
        }
        let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
        self.write_graph_batch_strict(cell_id, "delete_deltas_through_rollup", batch_to_write)
            .await?;
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

    pub async fn compact_supernode_segments(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        compacted_through_epoch: GraphEpoch,
        idempotency_key: &str,
    ) -> Result<SegmentCompactionResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "compact_supernode_segments")?;

        let started = std::time::Instant::now();
        let _permit = self.acquire_gc_permit("compact_supernode_segments").await?;
        let _writer = self.writer_lane(cell_id).lock().await;
        let idempotency_operation = segment_compaction_idempotency_operation(edge_type, src);
        let idem_key = keys::idempotency(cell_id, &idempotency_operation, idempotency_key);
        if let Some(value) = self.read_remote(&idem_key).await? {
            return decode_segment_compaction_idempotency(
                &idem_key,
                idempotency_key,
                compacted_through_epoch,
                &value,
            );
        }

        let current_epoch = self.current_epoch(cell_id).await?;
        if compacted_through_epoch > current_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: cell_id.to_string(),
                read_epoch: compacted_through_epoch,
                current_epoch,
            });
        }
        let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, compacted_through_epoch)
            .await?
        else {
            return Err(GraphError::CorruptValue {
                key: keys::out_segment_src_prefix(cell_id, edge_type, src),
                reason: "cannot compact segments without a matrix rollup artifact".to_string(),
            });
        };
        if artifact.base_epoch != compacted_through_epoch {
            return Err(GraphError::CorruptValue {
                key: keys::out_segment_src_prefix(cell_id, edge_type, src),
                reason: format!(
                    "latest matrix artifact is at epoch {}, expected {compacted_through_epoch}",
                    artifact.base_epoch
                ),
            });
        }
        let current_degree = self.out_neighbors(cell_id, edge_type, src).await?.len() as u64;

        let mut segment_iter = self
            .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, src))
            .await?;
        let mut source_segments = Vec::new();
        let mut input_edges = 0_u64;
        while let Some(kv) = segment_iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > compacted_through_epoch {
                break;
            }
            if segment.end_epoch > compacted_through_epoch {
                continue;
            }
            input_edges = input_edges.saturating_add(segment.edges.len() as u64);
            source_segments.push((key, segment));
        }

        let mut tombstone_iter = self
            .scan_remote_prefix(&keys::out_segment_tombstone_src_prefix(
                cell_id, edge_type, src,
            ))
            .await?;
        let mut tombstones = BTreeMap::<VertexId, (GraphEpoch, String)>::new();
        while let Some(kv) = tombstone_iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, key_src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= current_epoch {
                tombstones.insert(dst, (epoch, key));
            }
        }

        let mut live = BTreeMap::<VertexId, GraphEpoch>::new();
        for (_, segment) in &source_segments {
            for (epoch, dst) in &segment.edges {
                if *epoch > compacted_through_epoch {
                    continue;
                }
                let tombstone_epoch = tombstones
                    .get(dst)
                    .map(|(epoch, _)| *epoch)
                    .filter(|epoch| *epoch <= compacted_through_epoch);
                if segment_edge_visible(*epoch, tombstone_epoch) {
                    live.entry(*dst)
                        .and_modify(|existing| *existing = (*existing).max(*epoch))
                        .or_insert(*epoch);
                }
            }
        }
        let mut compacted_edges: Vec<_> =
            live.into_iter().map(|(dst, epoch)| (epoch, dst)).collect();
        compacted_edges.sort_unstable();

        let mut batch = GraphWriteBatch::new();
        for (key, _) in &source_segments {
            batch.delete(key.as_bytes());
        }
        let mut deleted_tombstone_keys = 0_u64;
        for (_, (epoch, key)) in tombstones {
            if epoch <= compacted_through_epoch {
                batch.delete(key.as_bytes());
                deleted_tombstone_keys = deleted_tombstone_keys.saturating_add(1);
            }
        }
        if let (Some((start_epoch, _)), Some((end_epoch, _))) =
            (compacted_edges.first(), compacted_edges.last())
        {
            batch.put(
                keys::out_segment(
                    cell_id,
                    edge_type,
                    src,
                    *end_epoch,
                    *start_epoch,
                    &format!("compact-{idempotency_key}"),
                ),
                encode_out_edge_segment_records(&compacted_edges),
            );
        }
        batch.put(
            keys::degree_out(cell_id, edge_type, src),
            encode_u64(current_degree),
        );
        let result = SegmentCompactionResult {
            compacted_through_epoch,
            source_segments: source_segments.len() as u64,
            deleted_segment_keys: source_segments.len() as u64,
            deleted_tombstone_keys,
            input_edges,
            output_edges: compacted_edges.len() as u64,
        };
        batch.put(
            idem_key.as_bytes(),
            encode_segment_compaction_idempotency(idempotency_key, &result),
        );
        self.write_graph_batch_strict(cell_id, "compact_supernode_segments", batch)
            .await?;
        self.record_gc_completed(
            result
                .deleted_segment_keys
                .saturating_add(result.deleted_tombstone_keys),
            started.elapsed(),
        );
        Ok(result)
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

    #[cfg(test)]
    pub(crate) async fn write_strict_for_test(&self, batch: WriteBatch) -> Result<()> {
        let options = WriteOptions {
            await_durable: true,
            ..Default::default()
        };
        self.db.write_with_options(batch, &options).await?;
        Ok(())
    }

    pub(crate) async fn write_graph_batch_strict(
        &self,
        cell_id: &str,
        operation: &'static str,
        batch: GraphWriteBatch,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        if batch.is_empty() {
            return Ok(());
        }
        let record_count = batch.len();
        let started = std::time::Instant::now();
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_graph_batch_txn(cell_id, operation, batch.clone())
                .await
            {
                Err(GraphError::Slate(err))
                    if err.kind() == ErrorKind::Transaction
                        && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Err(err @ GraphError::StaleShardLease { .. }) => {
                    self.operation_metrics
                        .stale_write_rejects
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }
                Ok(()) => {
                    self.record_graph_batch_commit(operation, record_count, started.elapsed());
                    return Ok(());
                }
                result => return result,
            }
        }
        unreachable!("transaction retry loop always returns on final attempt")
    }

    async fn write_graph_batch_txn(
        &self,
        cell_id: &str,
        operation: &'static str,
        batch: GraphWriteBatch,
    ) -> Result<()> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .await?;
        for op in batch.ops {
            match op {
                GraphWriteOp::Put(key, value) => txn.put(key.as_ref(), value.as_ref())?,
                GraphWriteOp::Delete(key) => txn.delete(key.as_ref())?,
            }
        }
        commit_txn_strict(txn, self.await_durable_writes).await
    }
}

async fn ensure_store_format(db: &Db, write_authority: &GraphWriteAuthority) -> Result<()> {
    let current = db
        .get_with_options(GRAPH_STORE_FORMAT_KEY.as_bytes(), &remote_read_options())
        .await?;
    let Some(value) = current else {
        if !matches!(write_authority, GraphWriteAuthority::ReadOnly) {
            tracing::info!(
                target: "slatedb_graph_kernel",
                version = GRAPH_STORE_FORMAT_VERSION,
                "initializing graph store format version"
            );
            let mut batch = WriteBatch::new();
            batch.put(
                GRAPH_STORE_FORMAT_KEY.as_bytes(),
                encode_u64(GRAPH_STORE_FORMAT_VERSION),
            );
            let options = WriteOptions {
                await_durable: true,
                ..Default::default()
            };
            db.write_with_options(batch, &options).await?;
        }
        return Ok(());
    };

    let version = decode_u64(GRAPH_STORE_FORMAT_KEY, &value)?;
    if version != GRAPH_STORE_FORMAT_VERSION {
        tracing::error!(
            target: "slatedb_graph_kernel",
            version,
            expected = GRAPH_STORE_FORMAT_VERSION,
            "unsupported graph store format version"
        );
        return Err(GraphError::CorruptValue {
            key: GRAPH_STORE_FORMAT_KEY.to_string(),
            reason: format!(
                "unsupported graph store format {version}; expected {GRAPH_STORE_FORMAT_VERSION}"
            ),
        });
    }
    Ok(())
}

async fn read_txn_remote(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    txn.mark_read([key.as_bytes()])?;
    Ok(txn
        .get_with_options(key.as_bytes(), &remote_read_options())
        .await?)
}

async fn out_neighbors_for_src_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    read_epoch: GraphEpoch,
) -> Result<BTreeSet<VertexId>> {
    let mut neighbors = BTreeSet::new();

    {
        let prefix = keys::out_prefix(cell_id, edge_type, src);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            if record.epoch <= read_epoch {
                neighbors.insert(record.dst);
            }
        }
    }

    let mut tombstones = BTreeMap::<VertexId, GraphEpoch>::new();
    {
        let prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, key_src, dst) =
                parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert(dst, epoch);
            }
        }
    }

    {
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.start_epoch > read_epoch {
                break;
            }
            for (epoch, dst) in segment.edges.iter().copied() {
                if epoch > read_epoch {
                    break;
                }
                if segment_edge_visible(epoch, tombstones.get(&dst).copied()) {
                    neighbors.insert(dst);
                }
            }
        }
    }

    Ok(neighbors)
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

async fn commit_txn_strict(txn: DbTransaction, await_durable: bool) -> Result<()> {
    let options = WriteOptions {
        await_durable,
        ..Default::default()
    };
    txn.commit_with_options(&options).await?;
    Ok(())
}

fn remote_read_options() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    }
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

fn validate_edge_mutations_for_cell(
    cell_id: &str,
    mutations: &[EdgeMutation],
    operation: &'static str,
) -> Result<()> {
    let mut idempotency_keys = BTreeSet::new();
    for mutation in mutations {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        if mutation.cell_id != cell_id {
            return Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/{operation}"),
                reason: format!(
                    "batch contains mutation for different cell {}",
                    mutation.cell_id
                ),
            });
        }
        if !idempotency_keys.insert(mutation.idempotency_key.clone()) {
            return Err(GraphError::IdempotencyConflict {
                operation: "create",
                idempotency_key: mutation.idempotency_key.clone(),
            });
        }
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
    encode_edge_epoch(record.epoch)
}

fn encode_edge_epoch(epoch: GraphEpoch) -> Vec<u8> {
    let mut value = Vec::with_capacity(b"edge3".len() + 8);
    value.extend_from_slice(b"edge3");
    value.extend_from_slice(&epoch.to_be_bytes());
    value
}

fn decode_edge_record(key: &str, value: &[u8]) -> Result<EdgeRecord> {
    if let Some(epoch) = value.strip_prefix(b"edge3") {
        let mut record = parse_edge_record_key(key)?;
        record.epoch = decode_u64(key, epoch)?;
        return Ok(record);
    }
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() == 2 && parts[0] == "edge2" {
        let mut record = parse_edge_record_key(key)?;
        record.epoch = parse_u64(key, parts[1], "epoch")?;
        return Ok(record);
    }
    if parts.len() == 6 && parts[0] == "edge1" {
        return Ok(EdgeRecord {
            epoch: parse_u64(key, parts[1], "epoch")?,
            cell_id: parts[2].to_string(),
            edge_type: parts[3].to_string(),
            src: parse_u64(key, parts[4], "src")?,
            dst: parse_u64(key, parts[5], "dst")?,
        });
    }
    Err(GraphError::CorruptValue {
        key: key.to_string(),
        reason: "expected edge3, edge2, or edge1 record".to_string(),
    })
}

fn encode_out_edge_segment(dsts: &[VertexId]) -> Vec<u8> {
    let mut value = Vec::with_capacity(b"out_segment1\n".len() + 8 + dsts.len() * 8);
    value.extend_from_slice(b"out_segment1\n");
    value.extend_from_slice(&(dsts.len() as u64).to_be_bytes());
    for dst in dsts {
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

fn encode_out_edge_segment_records(edges: &[(GraphEpoch, VertexId)]) -> Vec<u8> {
    let mut value = Vec::with_capacity(b"out_segment2\n".len() + 8 + edges.len() * 16);
    value.extend_from_slice(b"out_segment2\n");
    value.extend_from_slice(&(edges.len() as u64).to_be_bytes());
    for (epoch, dst) in edges {
        value.extend_from_slice(&epoch.to_be_bytes());
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

fn encode_segment_compaction_idempotency(
    idempotency_key: &str,
    result: &SegmentCompactionResult,
) -> Vec<u8> {
    format!(
        "segment_compact1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        result.compacted_through_epoch,
        result.source_segments,
        result.deleted_segment_keys,
        result.deleted_tombstone_keys,
        result.input_edges,
        result.output_edges,
        idempotency_key
    )
    .into_bytes()
}

fn decode_segment_compaction_idempotency(
    key: &str,
    idempotency_key: &str,
    compacted_through_epoch: GraphEpoch,
    value: &[u8],
) -> Result<SegmentCompactionResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "segment_compact1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected segment_compact1 record with 8 fields".to_string(),
        });
    }
    if parts[7] != idempotency_key {
        return Err(GraphError::IdempotencyConflict {
            operation: "segment-compact",
            idempotency_key: idempotency_key.to_string(),
        });
    }
    let recorded_epoch = parse_u64(key, parts[1], "compacted_through_epoch")?;
    if recorded_epoch != compacted_through_epoch {
        return Err(GraphError::IdempotencyConflict {
            operation: "segment-compact",
            idempotency_key: idempotency_key.to_string(),
        });
    }
    Ok(SegmentCompactionResult {
        compacted_through_epoch: recorded_epoch,
        source_segments: parse_u64(key, parts[2], "source_segments")?,
        deleted_segment_keys: parse_u64(key, parts[3], "deleted_segment_keys")?,
        deleted_tombstone_keys: parse_u64(key, parts[4], "deleted_tombstone_keys")?,
        input_edges: parse_u64(key, parts[5], "input_edges")?,
        output_edges: parse_u64(key, parts[6], "output_edges")?,
    })
}

fn decode_out_edge_segment(key: &str, value: &[u8]) -> Result<OutEdgeSegment> {
    let (cell_id, edge_type, src, end_epoch, start_epoch) = parse_out_edge_segment_key(key)?;
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "out edge segment start epoch is greater than end epoch".to_string(),
        });
    }
    if let Some(body) = value.strip_prefix(b"out_segment2\n") {
        return decode_out_edge_segment_v2(
            key,
            body,
            cell_id,
            edge_type,
            src,
            start_epoch,
            end_epoch,
        );
    }
    let Some(body) = value.strip_prefix(b"out_segment1\n") else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected out_segment2 or out_segment1 record".to_string(),
        });
    };
    if body.len() < 8 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected out segment count, got {} bytes", body.len()),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid out segment count bytes".to_string(),
        })?);
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "out segment epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("out segment count {expected} is too large"),
    })?;
    let expected_bytes = expected_count
        .checked_mul(8)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("out segment count {expected} is too large"),
        })?;
    let dst_bytes = &body[8..];
    if dst_bytes.len() != expected_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_bytes} out segment dst bytes, got {}",
                dst_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    for (offset, chunk) in dst_bytes.chunks_exact(8).enumerate() {
        let dst = u64::from_be_bytes(chunk.try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid out segment dst bytes".to_string(),
        })?);
        edges.push((start_epoch + offset as u64, dst));
    }
    Ok(OutEdgeSegment {
        cell_id,
        edge_type,
        src,
        start_epoch,
        end_epoch,
        edges,
    })
}

fn decode_out_edge_segment_v2(
    key: &str,
    body: &[u8],
    cell_id: String,
    edge_type: String,
    src: VertexId,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
) -> Result<OutEdgeSegment> {
    if body.len() < 8 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected out_segment2 count, got {} bytes", body.len()),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid out_segment2 count bytes".to_string(),
        })?);
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("out_segment2 count {expected} is too large"),
    })?;
    let expected_bytes =
        expected_count
            .checked_mul(16)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("out_segment2 count {expected} is too large"),
            })?;
    let edge_bytes = &body[8..];
    if edge_bytes.len() != expected_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_bytes} out_segment2 edge bytes, got {}",
                edge_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    let mut previous_epoch = None;
    for chunk in edge_bytes.chunks_exact(16) {
        let epoch =
            u64::from_be_bytes(
                chunk[..8]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid out_segment2 epoch bytes".to_string(),
                    })?,
            );
        let dst =
            u64::from_be_bytes(
                chunk[8..16]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid out_segment2 dst bytes".to_string(),
                    })?,
            );
        if epoch < start_epoch || epoch > end_epoch {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!(
                    "out_segment2 edge epoch {epoch} outside key range {start_epoch}..={end_epoch}"
                ),
            });
        }
        if previous_epoch.is_some_and(|previous| epoch <= previous) {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "out_segment2 epochs must be strictly increasing".to_string(),
            });
        }
        previous_epoch = Some(epoch);
        edges.push((epoch, dst));
    }
    Ok(OutEdgeSegment {
        cell_id,
        edge_type,
        src,
        start_epoch,
        end_epoch,
        edges,
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

fn encode_edge_mutation_log_batch(batch: &EdgeMutationLogBatch) -> Vec<u8> {
    let mut value = format!(
        "edge_mutation_log1\t{}\t{}\t{}\t{}\n",
        batch.cell_id,
        batch.batch_id,
        batch.fingerprint,
        batch.mutations.len()
    );
    for mutation in &batch.mutations {
        value.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            mutation.edge_type, mutation.src, mutation.dst, mutation.idempotency_key
        ));
    }
    value.into_bytes()
}

fn decode_edge_mutation_log_batch(key: &str, value: &[u8]) -> Result<EdgeMutationLogBatch> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.trim_end_matches('\n').lines();
    let Some(header) = lines.next() else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "empty edge mutation log batch".to_string(),
        });
    };
    let parts: Vec<&str> = header.split('\t').collect();
    if parts.len() != 5 || parts[0] != "edge_mutation_log1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge_mutation_log1 header with 5 fields".to_string(),
        });
    }
    let cell_id = parts[1].to_string();
    let batch_id = parts[2].to_string();
    let fingerprint = parse_u64(key, parts[3], "fingerprint")?;
    let expected = parse_u64(key, parts[4], "mutation_count")?;
    validate_component("cell_id", &cell_id)?;
    validate_component("batch_id", &batch_id)?;

    let mut mutations = Vec::new();
    for line in lines {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 4 {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "expected mutation row with 4 fields".to_string(),
            });
        }
        let edge_type = parts[0].to_string();
        let idempotency_key = parts[3].to_string();
        validate_component("edge_type", &edge_type)?;
        validate_component("idempotency_key", &idempotency_key)?;
        mutations.push(EdgeMutation {
            cell_id: cell_id.clone(),
            edge_type,
            src: parse_u64(key, parts[1], "src")?,
            dst: parse_u64(key, parts[2], "dst")?,
            idempotency_key,
        });
    }
    if mutations.len() as u64 != expected {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected} mutation rows, decoded {}",
                mutations.len()
            ),
        });
    }
    let batch = EdgeMutationLogBatch {
        cell_id,
        batch_id,
        fingerprint,
        mutations,
    };
    let actual = edge_mutation_log_fingerprint(&batch.cell_id, &batch.batch_id, &batch.mutations);
    if actual != fingerprint {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "mutation log fingerprint mismatch expected {fingerprint} got {actual}"
            ),
        });
    }
    Ok(batch)
}

fn encode_mutation_log_append_idempotency(
    batch_id: &str,
    fingerprint: u64,
    result: &EdgeMutationLogAppendResult,
) -> Vec<u8> {
    format!(
        "mutation_log_append1\t{}\t{}\t{}\t{}\n",
        result.log_epoch, result.mutations, fingerprint, batch_id
    )
    .into_bytes()
}

fn decode_mutation_log_append_idempotency(
    key: &str,
    batch_id: &str,
    fingerprint: u64,
    value: &[u8],
) -> Result<EdgeMutationLogAppendResult> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "mutation_log_append1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected mutation_log_append1 record with 5 fields".to_string(),
        });
    }
    if parts[4] != batch_id || parse_u64(key, parts[3], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "mutation-log",
            idempotency_key: batch_id.to_string(),
        });
    }
    Ok(EdgeMutationLogAppendResult {
        log_epoch: parse_u64(key, parts[1], "log_epoch")?,
        mutations: parse_u64(key, parts[2], "mutations")?,
        already_appended: true,
    })
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

fn decode_bulk_import_fingerprint_idempotency(
    key: &str,
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
    if parse_u64(key, parts[5], "fingerprint")? != fingerprint {
        return Err(GraphError::IdempotencyConflict {
            operation: "bulk-import-fingerprint",
            idempotency_key: format!("{fingerprint:020}"),
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

fn edge_mutation_log_fingerprint(cell_id: &str, batch_id: &str, mutations: &[EdgeMutation]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    fn update(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    update(&mut hash, cell_id.as_bytes());
    update(&mut hash, b"\0");
    update(&mut hash, batch_id.as_bytes());
    update(&mut hash, b"\0");
    for mutation in mutations {
        update(&mut hash, mutation.edge_type.as_bytes());
        update(&mut hash, b"\0");
        update(&mut hash, &mutation.src.to_be_bytes());
        update(&mut hash, &mutation.dst.to_be_bytes());
        update(&mut hash, b"\0");
        update(&mut hash, mutation.idempotency_key.as_bytes());
        update(&mut hash, b"\0");
    }
    hash
}

fn parse_mutation_log_epoch(key: &str) -> Result<GraphEpoch> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() < 5 || parts[0] != "cell" || parts[2] != "mutation_log" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected cell/{cell_id}/mutation_log/{epoch}/{batch_id}".to_string(),
        });
    }
    parse_u64(key, parts[3], "mutation_log_epoch")
}

fn bulk_import_chunk_order(src: VertexId, dst: VertexId) -> u64 {
    let mut value = src ^ dst.rotate_left(32) ^ 0x9e37_79b9_7f4a_7c15;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn segment_compaction_idempotency_operation(edge_type: &str, src: VertexId) -> String {
    format!("segment-compact-{edge_type}-{src:020}")
}

fn segment_import_fingerprint_key(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    fingerprint: u64,
) -> String {
    keys::idempotency(
        cell_id,
        &format!("segment-import-fp-{edge_type}-{src:020}"),
        &format!("{fingerprint:020}"),
    )
}

fn writer_lane_index(cell_id: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in cell_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % GRAPH_WRITE_LANES
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

fn duration_micros_u64(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn merge_ingest_batch(
    batch: &EdgeMutationBatchResult,
    start_epoch: &mut Option<GraphEpoch>,
    end_epoch: &mut GraphEpoch,
    inserted: &mut u64,
    already_existed: &mut u64,
    batches: &mut u64,
) {
    if batch.inserted > 0 {
        *start_epoch =
            Some(start_epoch.map_or(batch.start_epoch, |epoch| epoch.min(batch.start_epoch)));
    }
    *end_epoch = (*end_epoch).max(batch.end_epoch);
    *inserted = inserted.saturating_add(batch.inserted);
    *already_existed = already_existed.saturating_add(batch.already_existed);
    *batches = batches.saturating_add(1);
}

fn encode_delta_record(record: &DeltaRecord) -> Vec<u8> {
    let _ = record;
    b"delta2\n".to_vec()
}

fn encode_outbox_delta_batch(
    cell_id: &str,
    edge_type: &str,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
    edges: &[(VertexId, VertexId)],
) -> Vec<u8> {
    if let Some((src, _)) = edges.first() {
        if edges.iter().all(|(candidate, _)| candidate == src) {
            let dsts: Vec<_> = edges.iter().map(|(_, dst)| *dst).collect();
            return encode_outbox_delta_batch_same_src(
                cell_id,
                edge_type,
                kind,
                start_epoch,
                end_epoch,
                *src,
                &dsts,
            );
        }
    }
    let mut value = Vec::with_capacity(b"outbox_batch2\n".len() + 8 + edges.len() * 16);
    value.extend_from_slice(b"outbox_batch2\n");
    value.extend_from_slice(&(edges.len() as u64).to_be_bytes());
    for (src, dst) in edges {
        value.extend_from_slice(&src.to_be_bytes());
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

fn encode_outbox_delta_batch_same_src(
    cell_id: &str,
    edge_type: &str,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
    src: VertexId,
    dsts: &[VertexId],
) -> Vec<u8> {
    let _ = (cell_id, edge_type, kind, start_epoch, end_epoch);
    let mut value = Vec::with_capacity(b"outbox_batch3\n".len() + 16 + dsts.len() * 8);
    value.extend_from_slice(b"outbox_batch3\n");
    value.extend_from_slice(&(dsts.len() as u64).to_be_bytes());
    value.extend_from_slice(&src.to_be_bytes());
    for dst in dsts {
        value.extend_from_slice(&dst.to_be_bytes());
    }
    value
}

fn decode_outbox_delta_batch(key: &str, value: &[u8]) -> Result<OutboxDeltaBatch> {
    let (key_cell_id, key_end_epoch, key_start_epoch, key_kind, key_edge_type) =
        parse_outbox_batch_key(key)?;
    if let Some(body) = value.strip_prefix(b"outbox_batch3\n") {
        return decode_outbox_delta_batch_v3(
            key,
            body,
            key_cell_id,
            key_edge_type,
            key_kind,
            key_start_epoch,
            key_end_epoch,
        );
    }
    if let Some(body) = value.strip_prefix(b"outbox_batch2\n") {
        return decode_outbox_delta_batch_v2(
            key,
            body,
            key_cell_id,
            key_edge_type,
            key_kind,
            key_start_epoch,
            key_end_epoch,
        );
    }
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.trim_end_matches('\n').lines();
    let Some(header) = lines.next() else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "empty outbox delta batch".to_string(),
        });
    };
    let parts: Vec<&str> = header.split('\t').collect();
    if parts.len() != 7 || parts[0] != "outbox_batch1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected outbox_batch1 header with 7 fields".to_string(),
        });
    }
    validate_component("cell_id", parts[1])?;
    validate_component("edge_type", parts[2])?;
    let cell_id = parts[1].to_string();
    let edge_type = parts[2].to_string();
    let start_epoch = parse_u64(key, parts[3], "start_epoch")?;
    let end_epoch = parse_u64(key, parts[4], "end_epoch")?;
    let kind = parse_delta_kind(key, parts[5])?;
    let expected = parse_u64(key, parts[6], "edge_count")?;
    if cell_id != key_cell_id
        || edge_type != key_edge_type
        || start_epoch != key_start_epoch
        || end_epoch != key_end_epoch
        || kind != key_kind
    {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch header does not match key identity".to_string(),
        });
    }
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch start epoch is greater than end epoch".to_string(),
        });
    }
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "outbox batch epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let mut edges = Vec::new();
    for line in lines {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 2 {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "expected outbox batch row with 2 fields".to_string(),
            });
        }
        edges.push((
            parse_u64(key, parts[0], "src")?,
            parse_u64(key, parts[1], "dst")?,
        ));
    }
    if edges.len() as u64 != expected {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected} outbox batch rows, decoded {}",
                edges.len()
            ),
        });
    }
    Ok(OutboxDeltaBatch {
        cell_id,
        edge_type,
        kind,
        start_epoch,
        end_epoch,
        edges,
    })
}

fn decode_outbox_delta_batch_v2(
    key: &str,
    body: &[u8],
    cell_id: String,
    edge_type: String,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
) -> Result<OutboxDeltaBatch> {
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch start epoch is greater than end epoch".to_string(),
        });
    }
    if body.len() < 8 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected outbox_batch2 count, got {} bytes", body.len()),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid outbox_batch2 count bytes".to_string(),
        })?);
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "outbox batch epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let edge_bytes = &body[8..];
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("outbox_batch2 count {expected} is too large"),
    })?;
    let expected_bytes =
        expected_count
            .checked_mul(16)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("outbox_batch2 count {expected} is too large"),
            })?;
    if edge_bytes.len() != expected_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_bytes} outbox_batch2 edge bytes, got {}",
                edge_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    for chunk in edge_bytes.chunks_exact(16) {
        let src =
            u64::from_be_bytes(
                chunk[..8]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid outbox_batch2 src bytes".to_string(),
                    })?,
            );
        let dst =
            u64::from_be_bytes(
                chunk[8..16]
                    .try_into()
                    .map_err(|_| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "invalid outbox_batch2 dst bytes".to_string(),
                    })?,
            );
        edges.push((src, dst));
    }
    Ok(OutboxDeltaBatch {
        cell_id,
        edge_type,
        kind,
        start_epoch,
        end_epoch,
        edges,
    })
}

fn decode_outbox_delta_batch_v3(
    key: &str,
    body: &[u8],
    cell_id: String,
    edge_type: String,
    kind: DeltaKind,
    start_epoch: GraphEpoch,
    end_epoch: GraphEpoch,
) -> Result<OutboxDeltaBatch> {
    if start_epoch > end_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "outbox batch start epoch is greater than end epoch".to_string(),
        });
    }
    if body.len() < 16 {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected outbox_batch3 count and src, got {} bytes",
                body.len()
            ),
        });
    }
    let expected =
        u64::from_be_bytes(body[..8].try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid outbox_batch3 count bytes".to_string(),
        })?);
    let src = u64::from_be_bytes(
        body[8..16]
            .try_into()
            .map_err(|_| GraphError::CorruptValue {
                key: key.to_string(),
                reason: "invalid outbox_batch3 src bytes".to_string(),
            })?,
    );
    let expected_from_epoch = end_epoch.saturating_sub(start_epoch).saturating_add(1);
    if expected != expected_from_epoch {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "outbox batch epoch range implies {expected_from_epoch} edges, header says {expected}"
            ),
        });
    }
    let expected_count = usize::try_from(expected).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("outbox_batch3 count {expected} is too large"),
    })?;
    let expected_dst_bytes =
        expected_count
            .checked_mul(8)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("outbox_batch3 count {expected} is too large"),
            })?;
    let dst_bytes = &body[16..];
    if dst_bytes.len() != expected_dst_bytes {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!(
                "expected {expected_dst_bytes} outbox_batch3 dst bytes, got {}",
                dst_bytes.len()
            ),
        });
    }
    let mut edges = Vec::with_capacity(expected_count);
    for chunk in dst_bytes.chunks_exact(8) {
        let dst = u64::from_be_bytes(chunk.try_into().map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: "invalid outbox_batch3 dst bytes".to_string(),
        })?);
        edges.push((src, dst));
    }
    Ok(OutboxDeltaBatch {
        cell_id,
        edge_type,
        kind,
        start_epoch,
        end_epoch,
        edges,
    })
}

fn parse_outbox_batch_key(
    key: &str,
) -> Result<(String, GraphEpoch, GraphEpoch, DeltaKind, String)> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "outbox_batch", end_epoch, start_epoch, kind, edge_type, batch_id] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            validate_component("batch_id", batch_id)?;
            Ok((
                (*cell_id).to_string(),
                parse_u64(key, end_epoch, "end_epoch")?,
                parse_u64(key, start_epoch, "start_epoch")?,
                parse_delta_kind(key, kind)?,
                (*edge_type).to_string(),
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason:
                "expected cell/{cell_id}/outbox_batch/{end_epoch}/{start_epoch}/{kind}/{edge_type}/{batch_id}"
                    .to_string(),
        }),
    }
}

fn decode_delta_record(key: &str, value: &[u8]) -> Result<DeltaRecord> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() == 1 && parts[0] == "delta2" {
        return parse_delta_record_key(key);
    }
    if parts.len() == 7 && parts[0] == "delta1" {
        let kind = parse_delta_kind(key, parts[1])?;
        return Ok(DeltaRecord {
            kind,
            edge: EdgeRecord {
                epoch: parse_u64(key, parts[2], "epoch")?,
                cell_id: parts[3].to_string(),
                edge_type: parts[4].to_string(),
                src: parse_u64(key, parts[5], "src")?,
                dst: parse_u64(key, parts[6], "dst")?,
            },
        });
    }
    Err(GraphError::CorruptValue {
        key: key.to_string(),
        reason: "expected delta2 or delta1 record".to_string(),
    })
}

fn parse_edge_record_key(key: &str) -> Result<EdgeRecord> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "edge", edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                epoch: 0,
            })
        }
        ["cell", cell_id, "e", "out", edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                epoch: 0,
            })
        }
        ["cell", cell_id, "e", "in", edge_type, dst, src] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                epoch: 0,
            })
        }
        ["cell", cell_id, "delta", kind, edge_type, epoch, src, dst]
            if matches!(*kind, "plus" | "minus") =>
        {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(EdgeRecord {
                cell_id: (*cell_id).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                epoch: parse_u64(key, epoch, "epoch")?,
            })
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "cannot infer edge record identity from key".to_string(),
        }),
    }
}

fn parse_out_edge_segment_key(
    key: &str,
) -> Result<(String, String, VertexId, GraphEpoch, GraphEpoch)> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        [
            "cell",
            cell_id,
            "seg",
            "out",
            edge_type,
            src,
            end_epoch,
            start_epoch,
            segment_id,
        ] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            validate_component("segment_id", segment_id)?;
            Ok((
                (*cell_id).to_string(),
                (*edge_type).to_string(),
                parse_u64(key, src, "src")?,
                parse_u64(key, end_epoch, "end_epoch")?,
                parse_u64(key, start_epoch, "start_epoch")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason:
                "expected cell/{cell_id}/seg/out/{edge_type}/{src}/{end_epoch}/{start_epoch}/{segment_id}"
                    .to_string(),
        }),
    }
}

fn parse_out_edge_segment_tombstone_key(key: &str) -> Result<(String, String, VertexId, VertexId)> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "seg", "tomb", "out", edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok((
                (*cell_id).to_string(),
                (*edge_type).to_string(),
                parse_u64(key, src, "src")?,
                parse_u64(key, dst, "dst")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected cell/{cell_id}/seg/tomb/out/{edge_type}/{src}/{dst}".to_string(),
        }),
    }
}

fn segment_edge_visible(edge_epoch: GraphEpoch, tombstone_epoch: Option<GraphEpoch>) -> bool {
    match tombstone_epoch {
        Some(epoch) => edge_epoch > epoch,
        None => true,
    }
}

fn parse_delta_record_key(key: &str) -> Result<DeltaRecord> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "outbox", epoch, kind, edge_type, src, dst] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            Ok(DeltaRecord {
                kind: parse_delta_kind(key, kind)?,
                edge: EdgeRecord {
                    cell_id: (*cell_id).to_string(),
                    edge_type: (*edge_type).to_string(),
                    src: parse_u64(key, src, "src")?,
                    dst: parse_u64(key, dst, "dst")?,
                    epoch: parse_u64(key, epoch, "epoch")?,
                },
            })
        }
        ["cell", cell_id, "delta_owner", kind, edge_type, direction, owner, epoch, neighbor] => {
            validate_component("cell_id", cell_id)?;
            validate_component("edge_type", edge_type)?;
            let owner = parse_u64(key, owner, "owner")?;
            let neighbor = parse_u64(key, neighbor, "neighbor")?;
            let (src, dst) = match *direction {
                "out" => (owner, neighbor),
                "in" => (neighbor, owner),
                other => {
                    return Err(GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: format!("invalid delta owner direction {other}"),
                    });
                }
            };
            Ok(DeltaRecord {
                kind: parse_delta_kind(key, kind)?,
                edge: EdgeRecord {
                    cell_id: (*cell_id).to_string(),
                    edge_type: (*edge_type).to_string(),
                    src,
                    dst,
                    epoch: parse_u64(key, epoch, "epoch")?,
                },
            })
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "cannot infer delta record identity from key".to_string(),
        }),
    }
}

fn parse_delta_kind(key: &str, value: &str) -> Result<DeltaKind> {
    match value {
        "plus" | "+" => Ok(DeltaKind::Plus),
        "minus" | "-" => Ok(DeltaKind::Minus),
        other => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid delta kind {other}"),
        }),
    }
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

fn encode_write_fence(fence: &GraphWriteFence) -> Vec<u8> {
    format!(
        "write_fence1\t{}\t{}\t{}\t{}\n",
        fence.cell_id, fence.owner_node_id, fence.lease_token, fence.expires_at_ms
    )
    .into_bytes()
}

fn decode_write_fence(key: &str, value: &[u8]) -> Result<GraphWriteFence> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "write_fence1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected write_fence1 record with 5 fields".to_string(),
        });
    }
    validate_component("cell_id", parts[1])?;
    validate_component("node_id", parts[2])?;
    Ok(GraphWriteFence {
        cell_id: parts[1].to_string(),
        owner_node_id: parts[2].to_string(),
        lease_token: parse_u64(key, parts[3], "lease_token")?,
        expires_at_ms: parse_u64(key, parts[4], "expires_at_ms")?,
    })
}

fn encode_read_lease(lease: &GraphReadLease) -> Vec<u8> {
    format!(
        "read_lease1\t{}\t{}\t{}\t{}\n",
        lease.cell_id, lease.lease_id, lease.read_epoch, lease.expires_at_ms
    )
    .into_bytes()
}

fn decode_read_lease(key: &str, value: &[u8]) -> Result<GraphReadLease> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "read_lease1" {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected read_lease1 record with 5 fields".to_string(),
        });
    }
    validate_component("cell_id", parts[1])?;
    validate_component("read_lease_id", parts[2])?;
    Ok(GraphReadLease {
        cell_id: parts[1].to_string(),
        lease_id: parts[2].to_string(),
        read_epoch: parse_u64(key, parts[3], "read_epoch")?,
        expires_at_ms: parse_u64(key, parts[4], "expires_at_ms")?,
    })
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

    pub fn write_fence(cell_id: &str) -> String {
        format!("cell/{cell_id}/meta/write_fence")
    }

    pub fn read_lease_prefix(cell_id: &str) -> String {
        format!("cell/{cell_id}/read_lease/")
    }

    pub fn read_lease(cell_id: &str, lease_id: &str) -> String {
        format!("{}{}", read_lease_prefix(cell_id), lease_id)
    }

    pub fn last_epoch(cell_id: &str) -> String {
        format!("cell/{cell_id}/meta/last_epoch")
    }

    pub fn mutation_log_epoch(cell_id: &str) -> String {
        format!("cell/{cell_id}/meta/mutation_log_epoch")
    }

    pub fn mutation_log_materialized_epoch(cell_id: &str) -> String {
        format!("cell/{cell_id}/meta/mutation_log_materialized_epoch")
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

    pub fn out_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/e/out/{edge_type}/")
    }

    pub fn in_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/e/in/{edge_type}/")
    }

    pub fn out_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
        format!("cell/{cell_id}/e/out/{edge_type}/{src:020}/")
    }

    pub fn out_segment(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        end_epoch: GraphEpoch,
        start_epoch: GraphEpoch,
        segment_id: &str,
    ) -> String {
        format!(
            "cell/{cell_id}/seg/out/{edge_type}/{src:020}/{end_epoch:020}/{start_epoch:020}/{segment_id}"
        )
    }

    pub fn out_segment_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/seg/out/{edge_type}/")
    }

    pub fn out_segment_src_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
        format!("cell/{cell_id}/seg/out/{edge_type}/{src:020}/")
    }

    pub fn out_segment_tombstone(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
    ) -> String {
        format!("cell/{cell_id}/seg/tomb/out/{edge_type}/{src:020}/{dst:020}")
    }

    pub fn out_segment_tombstone_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
        format!("cell/{cell_id}/seg/tomb/out/{edge_type}/")
    }

    pub fn out_segment_tombstone_src_prefix(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
    ) -> String {
        format!("cell/{cell_id}/seg/tomb/out/{edge_type}/{src:020}/")
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

    pub fn outbox_batch(
        cell_id: &str,
        end_epoch: GraphEpoch,
        start_epoch: GraphEpoch,
        kind: super::DeltaKind,
        edge_type: &str,
        batch_id: &str,
    ) -> String {
        let kind = match kind {
            super::DeltaKind::Plus => "plus",
            super::DeltaKind::Minus => "minus",
        };
        format!(
            "cell/{cell_id}/outbox_batch/{end_epoch:020}/{start_epoch:020}/{kind}/{edge_type}/{batch_id}"
        )
    }

    pub fn outbox_batch_prefix(cell_id: &str) -> String {
        format!("cell/{cell_id}/outbox_batch/")
    }

    pub fn mutation_batch(cell_id: &str, start_epoch: GraphEpoch, idempotency_key: &str) -> String {
        format!("cell/{cell_id}/mutation_batch/{start_epoch:020}/{idempotency_key}")
    }

    pub fn mutation_log_prefix(cell_id: &str) -> String {
        format!("cell/{cell_id}/mutation_log/")
    }

    pub fn mutation_log_entry(cell_id: &str, log_epoch: GraphEpoch, batch_id: &str) -> String {
        format!("{}{log_epoch:020}/{batch_id}", mutation_log_prefix(cell_id))
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn delta_minus(
        cell_id: &str,
        edge_type: &str,
        epoch: GraphEpoch,
        src: VertexId,
        dst: VertexId,
    ) -> String {
        format!("cell/{cell_id}/delta/minus/{edge_type}/{epoch:020}/{src:020}/{dst:020}")
    }

    pub fn owner_delta_prefix(
        cell_id: &str,
        kind: super::DeltaKind,
        edge_type: &str,
        direction: &str,
        owner: VertexId,
    ) -> String {
        let kind = match kind {
            super::DeltaKind::Plus => "plus",
            super::DeltaKind::Minus => "minus",
        };
        format!("cell/{cell_id}/delta_owner/{kind}/{edge_type}/{direction}/{owner:020}/")
    }

    pub fn owner_delta_kind_prefix(
        cell_id: &str,
        edge_type: &str,
        kind: super::DeltaKind,
    ) -> String {
        let kind = match kind {
            super::DeltaKind::Plus => "plus",
            super::DeltaKind::Minus => "minus",
        };
        format!("cell/{cell_id}/delta_owner/{kind}/{edge_type}/")
    }

    #[cfg(test)]
    pub fn owner_delta(
        cell_id: &str,
        kind: super::DeltaKind,
        edge_type: &str,
        direction: &str,
        owner: VertexId,
        epoch: GraphEpoch,
        neighbor: VertexId,
    ) -> String {
        format!(
            "{}{epoch:020}/{neighbor:020}",
            owner_delta_prefix(cell_id, kind, edge_type, direction, owner)
        )
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

    async fn segment_append_txn_retry_for_test(
        shard: Arc<GraphShard>,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: Vec<VertexId>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        let edges: Vec<_> = dsts.iter().copied().map(|dst| (src, dst)).collect();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match shard
                .bulk_append_supernode_segment_trusted_txn(
                    cell_id,
                    edge_type,
                    src,
                    &dsts,
                    idempotency_key,
                    fingerprint,
                )
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

    async fn bulk_import_txn_retry_for_test(
        shard: Arc<GraphShard>,
        cell_id: &str,
        edge_type: &str,
        edges: Vec<(VertexId, VertexId)>,
        idempotency_key: &str,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match shard
                .bulk_import_edges_txn(
                    cell_id,
                    edge_type,
                    &edges,
                    idempotency_key,
                    fingerprint,
                    options,
                )
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

    fn assert_stale_node_a(err: GraphError) {
        assert!(matches!(
            err,
            GraphError::StaleShardLease {
                ref cell_id,
                ref node_id,
                lease_token: 1
            } if cell_id == "reddit-home" && node_id == "node-a"
        ));
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

    #[test]
    fn compact_v2_values_decode_alongside_legacy_v1_values() {
        let edge_key = keys::out_edge("reddit-home", "USER_FOLLOWS_USER", 1, 2);
        let v1_edge = b"edge1\t7\treddit-home\tUSER_FOLLOWS_USER\t1\t2\n";
        let decoded_v1 = decode_edge_record(&edge_key, v1_edge).unwrap();
        assert_eq!(decoded_v1.epoch, 7);
        assert_eq!(decoded_v1.src, 1);
        assert_eq!(decoded_v1.dst, 2);

        let v2_edge = decode_edge_record(&edge_key, b"edge2\t8\n").unwrap();
        assert_eq!(v2_edge.epoch, 8);
        assert_eq!(v2_edge.edge_type, "USER_FOLLOWS_USER");
        assert_eq!(v2_edge.src, 1);
        assert_eq!(v2_edge.dst, 2);

        let v3_edge = decode_edge_record(&edge_key, &encode_edge_epoch(9)).unwrap();
        assert_eq!(v3_edge.epoch, 9);
        assert_eq!(v3_edge.edge_type, "USER_FOLLOWS_USER");
        assert_eq!(v3_edge.src, 1);
        assert_eq!(v3_edge.dst, 2);

        let outbox_key = keys::outbox("reddit-home", 9, DeltaKind::Plus, "USER_FOLLOWS_USER", 1, 2);
        let delta_v2 = decode_delta_record(&outbox_key, b"delta2\n").unwrap();
        assert_eq!(delta_v2.kind, DeltaKind::Plus);
        assert_eq!(delta_v2.edge.epoch, 9);
        assert_eq!(delta_v2.edge.src, 1);
        assert_eq!(delta_v2.edge.dst, 2);

        let outbox_batch_key = keys::outbox_batch(
            "reddit-home",
            11,
            10,
            DeltaKind::Plus,
            "USER_FOLLOWS_USER",
            "b1",
        );
        let outbox_batch_v2 = encode_outbox_delta_batch(
            "reddit-home",
            "USER_FOLLOWS_USER",
            DeltaKind::Plus,
            10,
            11,
            &[(1, 2), (3, 4)],
        );
        assert!(outbox_batch_v2.starts_with(b"outbox_batch2\n"));
        let decoded_batch_v2 =
            decode_outbox_delta_batch(&outbox_batch_key, &outbox_batch_v2).unwrap();
        assert_eq!(decoded_batch_v2.edges, vec![(1, 2), (3, 4)]);
        assert_eq!(decoded_batch_v2.start_epoch, 10);
        assert_eq!(decoded_batch_v2.end_epoch, 11);

        let outbox_batch_v3 = encode_outbox_delta_batch(
            "reddit-home",
            "USER_FOLLOWS_USER",
            DeltaKind::Plus,
            10,
            11,
            &[(9, 2), (9, 4)],
        );
        assert!(outbox_batch_v3.starts_with(b"outbox_batch3\n"));
        assert!(outbox_batch_v3.len() < outbox_batch_v2.len());
        let decoded_batch_v3 =
            decode_outbox_delta_batch(&outbox_batch_key, &outbox_batch_v3).unwrap();
        assert_eq!(decoded_batch_v3.edges, vec![(9, 2), (9, 4)]);
        assert_eq!(decoded_batch_v3.start_epoch, 10);
        assert_eq!(decoded_batch_v3.end_epoch, 11);

        let outbox_batch_v1 =
            b"outbox_batch1\treddit-home\tUSER_FOLLOWS_USER\t10\t11\tplus\t2\n1\t2\n3\t4\n";
        let decoded_batch_v1 =
            decode_outbox_delta_batch(&outbox_batch_key, outbox_batch_v1).unwrap();
        assert_eq!(decoded_batch_v1.edges, decoded_batch_v2.edges);

        let owner_key = keys::owner_delta(
            "reddit-home",
            DeltaKind::Minus,
            "USER_FOLLOWS_USER",
            "in",
            2,
            10,
            1,
        );
        let owner_delta = decode_delta_record(&owner_key, b"delta2\n").unwrap();
        assert_eq!(owner_delta.kind, DeltaKind::Minus);
        assert_eq!(owner_delta.edge.epoch, 10);
        assert_eq!(owner_delta.edge.src, 1);
        assert_eq!(owner_delta.edge.dst, 2);

        let legacy_delta = b"delta1\t+\t11\treddit-home\tUSER_FOLLOWS_USER\t3\t4\n";
        let decoded_legacy = decode_delta_record(&outbox_key, legacy_delta).unwrap();
        assert_eq!(decoded_legacy.kind, DeltaKind::Plus);
        assert_eq!(decoded_legacy.edge.epoch, 11);
        assert_eq!(decoded_legacy.edge.src, 3);
        assert_eq!(decoded_legacy.edge.dst, 4);
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
    async fn bulk_import_transactions_retry_without_epoch_overlap() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard =
            Arc::new(open_test_shard("graph/bulk-import-transaction-race", object_store).await);
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let left = {
            let shard = Arc::clone(&shard);
            tokio::spawn(async move {
                bulk_import_txn_retry_for_test(
                    shard,
                    cell_id,
                    edge_type,
                    vec![(1, 2), (1, 3)],
                    "bulk-race-a",
                    BulkImportOptions::default(),
                )
                .await
            })
        };
        let right = {
            let shard = Arc::clone(&shard);
            tokio::spawn(async move {
                bulk_import_txn_retry_for_test(
                    shard,
                    cell_id,
                    edge_type,
                    vec![(1, 4), (1, 5)],
                    "bulk-race-b",
                    BulkImportOptions::default(),
                )
                .await
            })
        };

        let mut ranges = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
        ranges.sort_by_key(|result| result.start_epoch);
        assert_eq!(
            ranges,
            vec![
                BulkImportResult {
                    start_epoch: 1,
                    end_epoch: 2,
                    inserted: 2,
                    already_existed: 0,
                },
                BulkImportResult {
                    start_epoch: 3,
                    end_epoch: 4,
                    inserted: 2,
                    already_existed: 0,
                },
            ]
        );
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 4);
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
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
    async fn trusted_bulk_append_uses_batch_delta_log_and_survives_rollup_gc() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/trusted-bulk-append", object_store).await;
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let result = shard
            .bulk_append_edges_trusted(
                cell_id,
                edge_type,
                [(1, 2), (1, 3), (2, 4), (1, 2)],
                "trusted-bulk-1",
            )
            .await
            .unwrap();
        let retry = shard
            .bulk_append_edges_trusted(
                cell_id,
                edge_type,
                [(1, 2), (1, 3), (2, 4)],
                "trusted-bulk-1",
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BulkImportResult {
                start_epoch: 1,
                end_epoch: 3,
                inserted: 3,
                already_existed: 0,
            }
        );
        assert_eq!(retry, result);

        let mut per_edge_outbox = shard
            .scan_remote_prefix(&keys::outbox_prefix(cell_id))
            .await
            .unwrap();
        assert!(per_edge_outbox.next().await.unwrap().is_none());

        let mut batch_outbox = shard
            .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
            .await
            .unwrap();
        assert!(batch_outbox.next().await.unwrap().is_some());
        assert!(batch_outbox.next().await.unwrap().is_none());

        assert_eq!(
            shard
                .deltas_since(cell_id, edge_type, 0)
                .await
                .unwrap()
                .iter()
                .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
                .collect::<Vec<_>>(),
            vec![
                (DeltaKind::Plus, 1, 2, 1),
                (DeltaKind::Plus, 1, 3, 2),
                (DeltaKind::Plus, 2, 4, 3),
            ]
        );
        assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 3);
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            shard.edges_at(cell_id, edge_type, 2).await.unwrap(),
            vec![
                EdgeRecord {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    src: 1,
                    dst: 2,
                    epoch: 1,
                },
                EdgeRecord {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    src: 1,
                    dst: 3,
                    epoch: 2,
                },
            ]
        );

        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);

        shard
            .rollup_artifacts(cell_id, edge_type, result.end_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        let gc = shard
            .delete_deltas_through_rollup(cell_id, edge_type, result.end_epoch)
            .await
            .unwrap();
        assert_eq!(gc.deleted_delta_keys, 1);
        assert!(matches!(
            shard.deltas_since(cell_id, edge_type, 0).await.unwrap_err(),
            GraphError::SnapshotExpired { min_epoch: 3, .. }
        ));
        assert!(shard.outbox_since(cell_id, 0).await.unwrap().is_empty());
        let mut batch_outbox = shard
            .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
            .await
            .unwrap();
        assert!(batch_outbox.next().await.unwrap().is_none());

        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    }

    #[tokio::test]
    async fn outbound_only_index_policy_skips_reverse_rows_with_read_fallback() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/outbound-only-index",
            object_store,
            GraphOpenOptions {
                index_policy: GraphIndexPolicy::OutboundOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let result = shard
            .bulk_import_edges(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(1, 2), (1, 3), (4, 2)],
                "bulk-outbound-only",
            )
            .await
            .unwrap();

        assert_eq!(result.inserted, 3);
        assert_eq!(shard.graph_index_policy(), GraphIndexPolicy::OutboundOnly);
        assert_eq!(
            shard
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            shard
                .in_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2)
                .await
                .unwrap(),
            vec![1, 4]
        );
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
                .await
                .unwrap(),
            2
        );

        let mut reverse_edges = shard
            .scan_remote_prefix(&keys::in_edge_type_prefix(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
            ))
            .await
            .unwrap();
        assert!(reverse_edges.next().await.unwrap().is_none());

        let mut reverse_degrees = shard
            .scan_remote_prefix(&keys::degree_in_prefix(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
            ))
            .await
            .unwrap();
        assert!(reverse_degrees.next().await.unwrap().is_none());

        let report = shard
            .verify_current_graph("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
        assert_eq!(report.in_index_edges, 0);
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
    async fn trusted_chunked_bulk_append_uses_bounded_batch_delta_logs() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_limits(
            "graph/trusted-bulk-import-chunked",
            object_store,
            GraphLimits {
                max_bulk_import_edges: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let zero_chunk = shard
            .bulk_append_edges_trusted_bounded(
                cell_id,
                edge_type,
                [(1, 2)],
                "trusted-zero-chunk",
                0,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            zero_chunk,
            GraphError::CorruptValue { ref key, .. } if key == "trusted_append_chunk_size"
        ));

        let result = shard
            .bulk_append_edges_trusted_bounded(
                cell_id,
                edge_type,
                [(1, 2), (1, 3), (1, 4), (1, 5), (1, 6)],
                "trusted-chunked",
                2,
            )
            .await
            .unwrap();
        let retry = shard
            .bulk_append_edges_trusted_chunked(
                cell_id,
                edge_type,
                [(1, 6), (1, 5), (1, 4), (1, 3), (1, 2)],
                "trusted-chunked",
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
        assert_eq!(retry, result);
        assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 5);
        assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 5);

        let mut per_edge_outbox = shard
            .scan_remote_prefix(&keys::outbox_prefix(cell_id))
            .await
            .unwrap();
        assert!(per_edge_outbox.next().await.unwrap().is_none());

        let mut batch_outbox = shard
            .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
            .await
            .unwrap();
        let mut batch_records = 0;
        while batch_outbox.next().await.unwrap().is_some() {
            batch_records += 1;
        }
        assert_eq!(batch_records, 3);
    }

    #[tokio::test]
    async fn trusted_supernode_segment_append_skips_canonical_rows_and_survives_rollup_gc() {
        let full_index_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let full_index = open_test_shard("graph/segment-full-index-reject", full_index_store).await;
        let rejected = full_index
            .bulk_append_supernode_segment_trusted(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                1,
                [2],
                "segment-reject",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            rejected,
            GraphError::UnsupportedQuery {
                dialect: "GraphWrite",
                ..
            }
        ));

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/trusted-supernode-segment",
            object_store,
            GraphOpenOptions {
                index_policy: GraphIndexPolicy::OutboundOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let result = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [4, 2, 3, 2],
                "trusted-segment-1",
            )
            .await
            .unwrap();
        let retry = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [2, 3, 4],
                "trusted-segment-1",
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
        assert_eq!(retry, result);
        assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 3);
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
        assert!(shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 3, 4]
        );
        assert_eq!(
            shard.in_neighbors(cell_id, edge_type, 2).await.unwrap(),
            vec![1]
        );
        assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 3);
        assert_eq!(
            shard
                .deltas_since(cell_id, edge_type, 0)
                .await
                .unwrap()
                .iter()
                .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
                .collect::<Vec<_>>(),
            vec![
                (DeltaKind::Plus, 1, 2, 1),
                (DeltaKind::Plus, 1, 3, 2),
                (DeltaKind::Plus, 1, 4, 3),
            ]
        );

        let mut canonical = shard
            .scan_remote_prefix(&keys::out_edge_type_prefix(cell_id, edge_type))
            .await
            .unwrap();
        assert!(canonical.next().await.unwrap().is_none());
        let mut segments = shard
            .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
            .await
            .unwrap();
        assert!(segments.next().await.unwrap().is_some());
        assert!(segments.next().await.unwrap().is_none());

        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
        assert_eq!(report.canonical_edges, 0);
        assert_eq!(report.out_index_edges, 3);

        let delete = shard
            .delete_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: 1,
                dst: 3,
                idempotency_key: "trusted-segment-delete-1".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            delete,
            DeleteResult {
                epoch: 4,
                deleted: true
            }
        );
        let delete_retry = shard
            .delete_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: 1,
                dst: 3,
                idempotency_key: "trusted-segment-delete-1".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(delete_retry, delete);
        assert!(!shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 4]
        );
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 2);
        assert_eq!(
            shard
                .out_neighbors_at(cell_id, edge_type, 1, result.end_epoch)
                .await
                .unwrap(),
            vec![2, 3, 4]
        );
        assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 4);
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
        assert_eq!(report.canonical_edges, 0);
        assert_eq!(report.out_index_edges, 2);

        shard
            .rollup_artifacts(cell_id, edge_type, delete.epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        let gc = shard
            .delete_deltas_through_rollup(cell_id, edge_type, delete.epoch)
            .await
            .unwrap();
        assert_eq!(gc.deleted_delta_keys, 2);
        assert!(shard.outbox_since(cell_id, 0).await.unwrap().is_empty());
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 4]
        );

        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    }

    #[tokio::test]
    async fn segment_compaction_merges_segments_and_gcs_tombstones_after_rollup() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/segment-compaction",
            object_store,
            GraphOpenOptions {
                index_policy: GraphIndexPolicy::OutboundOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let first = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [2, 3],
                "compact-segment-1",
            )
            .await
            .unwrap();
        let second = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [4, 5],
                "compact-segment-2",
            )
            .await
            .unwrap();
        assert_eq!(first.end_epoch, 2);
        assert_eq!(second.end_epoch, 4);

        let delete = shard
            .delete_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: 1,
                dst: 3,
                idempotency_key: "compact-delete-3".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(delete.epoch, 5);
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 4, 5]
        );

        shard
            .rollup_artifacts(cell_id, edge_type, delete.epoch, 2, 2, 1, 2)
            .await
            .unwrap();

        let mut segments = shard
            .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
            .await
            .unwrap();
        let mut segment_count = 0;
        while segments.next().await.unwrap().is_some() {
            segment_count += 1;
        }
        assert_eq!(segment_count, 2);
        let mut tombstones = shard
            .scan_remote_prefix(&keys::out_segment_tombstone_src_prefix(
                cell_id, edge_type, 1,
            ))
            .await
            .unwrap();
        assert!(tombstones.next().await.unwrap().is_some());
        assert!(tombstones.next().await.unwrap().is_none());

        let compact = shard
            .compact_supernode_segments(cell_id, edge_type, 1, delete.epoch, "compact-1")
            .await
            .unwrap();
        assert_eq!(
            compact,
            SegmentCompactionResult {
                compacted_through_epoch: 5,
                source_segments: 2,
                deleted_segment_keys: 2,
                deleted_tombstone_keys: 1,
                input_edges: 4,
                output_edges: 3,
            }
        );
        let retry = shard
            .compact_supernode_segments(cell_id, edge_type, 1, delete.epoch, "compact-1")
            .await
            .unwrap();
        assert_eq!(retry, compact);

        let mut segments = shard
            .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
            .await
            .unwrap();
        let mut compacted_values = Vec::new();
        while let Some(kv) = segments.next().await.unwrap() {
            compacted_values.push(kv.value.to_vec());
        }
        assert_eq!(compacted_values.len(), 1);
        assert!(compacted_values[0].starts_with(b"out_segment2\n"));
        let mut tombstones = shard
            .scan_remote_prefix(&keys::out_segment_tombstone_src_prefix(
                cell_id, edge_type, 1,
            ))
            .await
            .unwrap();
        assert!(tombstones.next().await.unwrap().is_none());

        assert!(!shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 4, 5]
        );
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);

        shard
            .delete_deltas_through_rollup(cell_id, edge_type, delete.epoch)
            .await
            .unwrap();
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    }

    #[tokio::test]
    async fn trusted_segment_append_replay_with_new_job_id_does_not_double_count_degree() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/segment-replay-fingerprint",
            object_store,
            GraphOpenOptions {
                index_policy: GraphIndexPolicy::OutboundOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let first = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [2, 3, 4],
                "segment-job-a",
            )
            .await
            .unwrap();
        let replay = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [2, 3, 4],
                "segment-job-b",
            )
            .await
            .unwrap();

        assert_eq!(replay, first);
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 3, 4]
        );
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
        let mut segments = shard
            .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
            .await
            .unwrap();
        let mut segment_count = 0;
        while segments.next().await.unwrap().is_some() {
            segment_count += 1;
        }
        assert_eq!(segment_count, 1);
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    }

    #[tokio::test]
    async fn trusted_segment_append_filters_partial_overlap_without_degree_drift() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/segment-partial-overlap",
            object_store,
            GraphOpenOptions {
                index_policy: GraphIndexPolicy::OutboundOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let first = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [2, 3, 4],
                "segment-overlap-a",
            )
            .await
            .unwrap();
        let second = shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [3, 4, 5],
                "segment-overlap-b",
            )
            .await
            .unwrap();

        assert_eq!(
            first,
            BulkImportResult {
                start_epoch: 1,
                end_epoch: 3,
                inserted: 3,
                already_existed: 0,
            }
        );
        assert_eq!(
            second,
            BulkImportResult {
                start_epoch: 4,
                end_epoch: 4,
                inserted: 1,
                already_existed: 2,
            }
        );
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 4);
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    }

    #[tokio::test]
    async fn segment_append_transactions_retry_without_epoch_overlap() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        };
        let shard = Arc::new(
            GraphShard::open_standalone_writer_with_options(
                "graph/segment-transaction-race",
                object_store,
                options,
            )
            .await
            .unwrap(),
        );
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        let left = {
            let shard = Arc::clone(&shard);
            tokio::spawn(async move {
                segment_append_txn_retry_for_test(
                    shard,
                    cell_id,
                    edge_type,
                    1,
                    vec![2, 3],
                    "segment-race-a",
                )
                .await
            })
        };
        let right = {
            let shard = Arc::clone(&shard);
            tokio::spawn(async move {
                segment_append_txn_retry_for_test(
                    shard,
                    cell_id,
                    edge_type,
                    1,
                    vec![4, 5],
                    "segment-race-b",
                )
                .await
            })
        };

        let mut ranges = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
        ranges.sort_by_key(|result| result.start_epoch);
        assert_eq!(
            ranges,
            vec![
                BulkImportResult {
                    start_epoch: 1,
                    end_epoch: 2,
                    inserted: 2,
                    already_existed: 0,
                },
                BulkImportResult {
                    start_epoch: 3,
                    end_epoch: 4,
                    inserted: 2,
                    already_existed: 0,
                },
            ]
        );
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 4);
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    }

    #[tokio::test]
    async fn segment_compaction_preserves_segments_after_compacted_epoch() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/segment-compaction-boundary",
            object_store,
            GraphOpenOptions {
                index_policy: GraphIndexPolicy::OutboundOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

        shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [2, 3],
                "boundary-segment-old",
            )
            .await
            .unwrap();
        let rollup_epoch = shard.current_epoch(cell_id).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, rollup_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        shard
            .bulk_append_supernode_segment_trusted(
                cell_id,
                edge_type,
                1,
                [4, 5],
                "boundary-segment-new",
            )
            .await
            .unwrap();

        let compact = shard
            .compact_supernode_segments(cell_id, edge_type, 1, rollup_epoch, "boundary-compact")
            .await
            .unwrap();
        assert_eq!(
            compact,
            SegmentCompactionResult {
                compacted_through_epoch: rollup_epoch,
                source_segments: 1,
                deleted_segment_keys: 1,
                deleted_tombstone_keys: 0,
                input_edges: 2,
                output_edges: 2,
            }
        );
        assert_eq!(
            shard
                .out_neighbors_at(cell_id, edge_type, 1, rollup_epoch)
                .await
                .unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 4);

        let mut segments = shard
            .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
            .await
            .unwrap();
        let mut segment_count = 0;
        while segments.next().await.unwrap().is_some() {
            segment_count += 1;
        }
        assert_eq!(segment_count, 2);
        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    }

    #[test]
    fn writer_lanes_partition_different_cells() {
        assert_ne!(writer_lane_index("cell-a"), writer_lane_index("cell-b"));
        assert_ne!(
            writer_lane_index("reddit-home"),
            writer_lane_index("other-cell")
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

    #[tokio::test]
    async fn write_edge_mutations_batch_keeps_per_edge_idempotency_and_indexes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/write-edge-mutations-batch", object_store).await;

        let result = shard
            .write_edge_mutations_batch(
                "reddit-home",
                [
                    mutation(7, 10, "edge-batch-1"),
                    mutation(7, 11, "edge-batch-2"),
                    mutation(7, 10, "edge-batch-3"),
                ],
            )
            .await
            .unwrap();

        assert_eq!(result.start_epoch, 1);
        assert_eq!(result.end_epoch, 2);
        assert_eq!(result.inserted, 2);
        assert_eq!(result.already_existed, 1);
        assert_eq!(
            result.results,
            vec![
                CommitResult {
                    epoch: 1,
                    already_existed: false
                },
                CommitResult {
                    epoch: 2,
                    already_existed: false
                },
                CommitResult {
                    epoch: 1,
                    already_existed: true
                }
            ]
        );
        assert_eq!(
            shard
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
                .await
                .unwrap(),
            vec![10, 11]
        );
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
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
            vec![(DeltaKind::Plus, 7, 10, 1), (DeltaKind::Plus, 7, 11, 2)]
        );

        let retry = shard
            .write_edge_mutations_batch(
                "reddit-home",
                [
                    mutation(7, 10, "edge-batch-1"),
                    mutation(7, 11, "edge-batch-2"),
                    mutation(7, 10, "edge-batch-3"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(retry, result);
        assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn write_edge_mutations_batch_rejects_idempotency_reuse_for_different_edge() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard =
            open_test_shard("graph/write-edge-mutations-batch-conflict", object_store).await;

        shard
            .write_edge_mutations_batch(
                "reddit-home",
                [
                    mutation(7, 10, "edge-batch-conflict"),
                    mutation(7, 11, "edge-batch-ok"),
                ],
            )
            .await
            .unwrap();

        let conflict = shard
            .write_edge_mutations_batch("reddit-home", [mutation(7, 12, "edge-batch-conflict")])
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            GraphError::IdempotencyConflict {
                operation: "create",
                ref idempotency_key
            } if idempotency_key == "edge-batch-conflict"
        ));

        let duplicate_in_batch = shard
            .write_edge_mutations_batch(
                "reddit-home",
                [
                    mutation(8, 10, "edge-batch-duplicate"),
                    mutation(8, 11, "edge-batch-duplicate"),
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(
            duplicate_in_batch,
            GraphError::IdempotencyConflict {
                operation: "create",
                ref idempotency_key
            } if idempotency_key == "edge-batch-duplicate"
        ));
    }

    #[tokio::test]
    async fn ingest_edge_mutations_chunks_and_replays_idempotently() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/ingest-edge-mutations-chunked",
            object_store,
            GraphOpenOptions {
                limits: GraphLimits {
                    max_bulk_import_edges: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let mutations =
            (0..5).map(|index| mutation(9, 100 + index, &format!("edge-ingest-chunked-{index}")));
        let result = shard
            .ingest_edge_mutations(
                "reddit-home",
                mutations,
                EdgeIngestOptions { batch_size: 10 },
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            EdgeIngestResult {
                start_epoch: 1,
                end_epoch: 5,
                inserted: 5,
                already_existed: 0,
                batches: 3,
                mutations: 5,
            }
        );
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 9)
                .await
                .unwrap(),
            5
        );

        let replay = shard
            .ingest_edge_mutations(
                "reddit-home",
                (0..5)
                    .map(|index| mutation(9, 100 + index, &format!("edge-ingest-chunked-{index}"))),
                EdgeIngestOptions { batch_size: 10 },
            )
            .await
            .unwrap();
        assert_eq!(replay, result);
        assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 5);

        let duplicate = shard
            .ingest_edge_mutations(
                "reddit-home",
                [mutation(9, 100, "edge-ingest-existing-edge")],
                EdgeIngestOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(duplicate.inserted, 0);
        assert_eq!(duplicate.already_existed, 1);
        assert_eq!(duplicate.end_epoch, 5);
    }

    #[tokio::test]
    async fn ingest_edge_mutations_rejects_zero_batch_size() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/ingest-edge-mutations-zero-batch", object_store).await;

        let err = shard
            .ingest_edge_mutations(
                "reddit-home",
                [mutation(9, 10, "edge-ingest-zero-batch")],
                EdgeIngestOptions { batch_size: 0 },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::CorruptValue { ref key, .. } if key == "edge_ingest_batch_size"
        ));
        assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn mutation_log_append_is_durable_and_replayed_after_reopen() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "graph/mutation-log-reopen";
        {
            let writer = open_test_shard(path, Arc::clone(&object_store)).await;
            let result = writer
                .append_edge_mutation_log(
                    "reddit-home",
                    "log-batch-1",
                    [
                        mutation(20, 30, "log-edge-1"),
                        mutation(20, 31, "log-edge-2"),
                    ],
                )
                .await
                .unwrap();
            assert_eq!(
                result,
                EdgeMutationLogAppendResult {
                    log_epoch: 1,
                    mutations: 2,
                    already_appended: false
                }
            );
            assert_eq!(writer.current_epoch("reddit-home").await.unwrap(), 0);
            assert!(!writer
                .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 20, 30)
                .await
                .unwrap());
            writer.close().await.unwrap();
        }

        let reopened = open_test_shard(path, Arc::clone(&object_store)).await;
        let materialized = reopened
            .materialize_edge_mutation_log("reddit-home", 16)
            .await
            .unwrap();
        assert_eq!(materialized.scanned_batches, 1);
        assert_eq!(materialized.materialized_batches, 1);
        assert_eq!(materialized.mutations, 2);
        assert_eq!(materialized.inserted, 2);
        assert_eq!(materialized.materialized_log_epoch, 1);
        assert_eq!(materialized.current_epoch, 2);
        assert_eq!(
            reopened
                .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 20)
                .await
                .unwrap(),
            vec![30, 31]
        );
    }

    #[tokio::test]
    async fn mutation_log_append_is_batch_idempotent_and_detects_conflict() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/mutation-log-idempotency", object_store).await;

        let first = shard
            .append_edge_mutation_log(
                "reddit-home",
                "log-batch-idem",
                [
                    mutation(30, 40, "log-idem-1"),
                    mutation(30, 41, "log-idem-2"),
                ],
            )
            .await
            .unwrap();
        let retry = shard
            .append_edge_mutation_log(
                "reddit-home",
                "log-batch-idem",
                [
                    mutation(30, 40, "log-idem-1"),
                    mutation(30, 41, "log-idem-2"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            retry,
            EdgeMutationLogAppendResult {
                already_appended: true,
                ..first
            }
        );

        let conflict = shard
            .append_edge_mutation_log(
                "reddit-home",
                "log-batch-idem",
                [mutation(30, 42, "log-idem-different")],
            )
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            GraphError::IdempotencyConflict {
                operation: "mutation-log",
                ref idempotency_key
            } if idempotency_key == "log-batch-idem"
        ));

        let mut iter = shard
            .scan_remote_prefix("cell/reddit-home/mutation_log/")
            .await
            .unwrap();
        let mut logs = 0;
        while iter.next().await.unwrap().is_some() {
            logs += 1;
        }
        assert_eq!(logs, 1);
    }

    #[tokio::test]
    async fn mutation_log_materializer_replay_is_idempotent_if_watermark_is_lost() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/mutation-log-replay-idempotent", object_store).await;

        shard
            .append_edge_mutation_log(
                "reddit-home",
                "log-batch-watermark",
                [
                    mutation(40, 50, "log-watermark-1"),
                    mutation(40, 51, "log-watermark-2"),
                ],
            )
            .await
            .unwrap();
        let first = shard
            .materialize_edge_mutation_log("reddit-home", 16)
            .await
            .unwrap();
        assert_eq!(first.inserted, 2);
        assert_eq!(first.current_epoch, 2);

        let mut batch = WriteBatch::new();
        batch.put(
            keys::mutation_log_materialized_epoch("reddit-home"),
            encode_u64(0),
        );
        shard.write_strict_for_test(batch).await.unwrap();

        let replay = shard
            .materialize_edge_mutation_log("reddit-home", 16)
            .await
            .unwrap();
        assert_eq!(replay.materialized_batches, 1);
        assert_eq!(replay.current_epoch, 2);
        assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 2);
        assert_eq!(
            shard
                .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 40)
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn mutation_log_materializer_uses_bounded_microdrains() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/mutation-log-bounded-drain", object_store).await;

        for batch in 0..2_u64 {
            let mutations = (0..400_u64)
                .map(|index| {
                    mutation(
                        50 + batch,
                        10_000 + (batch * 1_000) + index,
                        &format!("log-bounded-{batch}-{index}"),
                    )
                })
                .collect::<Vec<_>>();
            shard
                .append_edge_mutation_log(
                    "reddit-home",
                    &format!("log-bounded-batch-{batch}"),
                    mutations,
                )
                .await
                .unwrap();
        }

        let first = shard
            .materialize_edge_mutation_log("reddit-home", 2)
            .await
            .unwrap();
        assert_eq!(first.scanned_batches, 2);
        assert_eq!(first.materialized_batches, 2);
        assert_eq!(first.mutations, 800);
        assert_eq!(first.materialized_log_epoch, 2);
        assert_eq!(first.last_log_epoch, 2);
        assert_eq!(first.current_epoch, 800);

        let second = shard
            .materialize_edge_mutation_log("reddit-home", 2)
            .await
            .unwrap();
        assert_eq!(second.scanned_batches, 0);
        assert_eq!(second.materialized_batches, 0);
        assert_eq!(second.mutations, 0);
        assert_eq!(second.materialized_log_epoch, 2);
        assert_eq!(second.current_epoch, 800);
        assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 800);
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
    async fn leased_writer_requires_installed_data_write_fence() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let lease = ShardLease {
            cell_id: "reddit-home".to_string(),
            owner_node_id: "node-a".to_string(),
            lease_token: 1,
            expires_at_ms: graph_now_millis() + 60_000,
        };
        let leases = Arc::new(RwLock::new(BTreeMap::from([(
            lease.cell_id.clone(),
            lease.clone(),
        )])));
        let shard = GraphShard::open_leased_writer(
            "graph/leased-fence-required",
            Arc::clone(&object_store),
            GraphOpenOptions::default(),
            "node-a".to_string(),
            Arc::clone(&leases),
        )
        .await
        .unwrap();

        let err = shard
            .write_edge(mutation(1, 2, "missing-data-fence"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::WriteRequiresLease {
                operation: "write_edge",
                ref cell_id
            } if cell_id == "reddit-home"
        ));

        shard
            .install_write_fence("reddit-home", &lease)
            .await
            .unwrap();
        shard
            .write_edge(mutation(1, 2, "after-data-fence"))
            .await
            .unwrap();
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn newer_data_write_fence_rejects_all_stale_write_classes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        let lease = ShardLease {
            cell_id: cell_id.to_string(),
            owner_node_id: "node-a".to_string(),
            lease_token: 1,
            expires_at_ms: graph_now_millis() + 60_000,
        };
        let leases = Arc::new(RwLock::new(BTreeMap::from([(
            lease.cell_id.clone(),
            lease.clone(),
        )])));
        let shard = GraphShard::open_leased_writer(
            "graph/stale-data-fence",
            Arc::clone(&object_store),
            GraphOpenOptions::default(),
            "node-a".to_string(),
            Arc::clone(&leases),
        )
        .await
        .unwrap();
        shard.install_write_fence(cell_id, &lease).await.unwrap();
        shard.write_edge(mutation(1, 2, "base-1")).await.unwrap();
        let epoch_two = shard.write_edge(mutation(1, 3, "base-2")).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, epoch_two.epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        let epoch_three = shard.write_edge(mutation(1, 4, "base-3")).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, epoch_three.epoch, 2, 2, 1, 2)
            .await
            .unwrap();

        let newer = ShardLease {
            cell_id: cell_id.to_string(),
            owner_node_id: "node-b".to_string(),
            lease_token: 2,
            expires_at_ms: graph_now_millis() + 60_000,
        };
        let mut batch = WriteBatch::new();
        batch.put(
            keys::write_fence(cell_id),
            encode_write_fence(&GraphWriteFence::from(&newer)),
        );
        shard.write_strict_for_test(batch).await.unwrap();

        assert_stale_node_a(
            shard
                .write_edge(mutation(1, 5, "stale-edge"))
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .delete_edge(mutation(1, 2, "stale-delete"))
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .bulk_import_edges(cell_id, edge_type, [(2, 20), (2, 21)], "stale-bulk")
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .build_posting_chunks(cell_id, edge_type, epoch_three.epoch, 2)
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .build_matrix_tiles(cell_id, edge_type, epoch_three.epoch, 2)
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .build_supernode_groups(cell_id, edge_type, epoch_three.epoch, 1, 2)
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .rollup_artifacts(cell_id, edge_type, epoch_three.epoch, 2, 2, 1, 2)
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .delete_graph_artifacts_before(cell_id, edge_type, epoch_three.epoch)
                .await
                .unwrap_err(),
        );
        assert_stale_node_a(
            shard
                .delete_deltas_through_rollup(cell_id, edge_type, epoch_three.epoch)
                .await
                .unwrap_err(),
        );

        assert!(!shard.edge_exists(cell_id, edge_type, 1, 5).await.unwrap());
        assert_eq!(
            shard.delta_gc_watermark(cell_id, edge_type).await.unwrap(),
            0
        );
        shard.close().await.unwrap();
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
            std::time::Duration::from_secs(2),
        )
        .await
        .unwrap();
        let first_expiry = cluster.lease("reddit-home").unwrap().expires_at_ms;
        let handle = cluster
            .start_lease_renewer(
                Arc::clone(&control),
                std::time::Duration::from_secs(2),
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
    async fn control_plane_metrics_count_lease_renewal_failures() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = GraphControlPlane::open("graph-control/renew-metrics", object_store)
            .await
            .unwrap();
        control
            .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
            .await
            .unwrap();
        let lease = control
            .acquire_lease("reddit-home", "node-a", std::time::Duration::from_millis(5))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        let err = control
            .renew_lease(&lease, std::time::Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::StaleShardLease {
                ref cell_id,
                ref node_id,
                lease_token: 1
            } if cell_id == "reddit-home" && node_id == "node-a"
        ));

        let metrics = control.graph_control_metrics();
        assert_eq!(metrics.lease_acquire_attempts, 1);
        assert_eq!(metrics.lease_acquire_successes, 1);
        assert_eq!(metrics.lease_renew_attempts, 1);
        assert_eq!(metrics.lease_renew_successes, 0);
        assert_eq!(metrics.lease_renew_failures, 1);
        assert_eq!(metrics.lease_renew_lost, 1);
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
            std::time::Duration::from_secs(2),
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
                ..Default::default()
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
    async fn read_leases_block_delta_and_artifact_gc_until_ttl() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/read-lease-retention",
            object_store,
            GraphOpenOptions {
                retention_policy: GraphRetentionPolicy {
                    read_lease_ttl_ms: 60_000,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        shard.write_edge(mutation(1, 2, "lease-1")).await.unwrap();
        shard.write_edge(mutation(2, 3, "lease-2")).await.unwrap();
        let base_epoch = shard.current_epoch(cell_id).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        let _snapshot = shard.snapshot(cell_id).await.unwrap();

        let delta_err = shard
            .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
            .await
            .unwrap_err();
        assert!(matches!(
            delta_err,
            GraphError::RetentionViolation {
                operation: "delete_deltas_through_rollup",
                ref cell_id,
                requested_epoch,
                safe_epoch: 0,
            } if cell_id == "reddit-home" && requested_epoch == base_epoch
        ));

        let artifact_err = shard
            .delete_graph_artifacts_before(cell_id, edge_type, base_epoch + 1)
            .await
            .unwrap_err();
        assert!(matches!(
            artifact_err,
            GraphError::RetentionViolation {
                operation: "delete_graph_artifacts_before",
                ref cell_id,
                requested_epoch,
                safe_epoch: 1,
            } if cell_id == "reddit-home" && requested_epoch == base_epoch + 1
        ));

        let metrics = shard.graph_operational_metrics();
        assert!(metrics.read_leases_created >= 1);
        assert!(metrics.retention_rejects >= 2);
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn expired_read_leases_are_pruned_and_gc_can_continue() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/read-lease-expiry",
            object_store,
            GraphOpenOptions {
                retention_policy: GraphRetentionPolicy {
                    read_lease_ttl_ms: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        shard.write_edge(mutation(1, 2, "expiry-1")).await.unwrap();
        shard.write_edge(mutation(2, 3, "expiry-2")).await.unwrap();
        let base_epoch = shard.current_epoch(cell_id).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        let _snapshot = shard.snapshot(cell_id).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let delta_gc = shard
            .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
            .await
            .unwrap();
        assert!(delta_gc.deleted_delta_keys > 0);
        assert_eq!(
            shard.delta_gc_watermark(cell_id, edge_type).await.unwrap(),
            base_epoch
        );
        let metrics = shard.graph_operational_metrics();
        assert!(metrics.gc_jobs_completed >= 1);
        assert!(metrics.gc_keys_deleted >= delta_gc.deleted_delta_keys);
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn min_retained_epochs_blocks_delta_gc() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/min-retained-epochs",
            object_store,
            GraphOpenOptions {
                retention_policy: GraphRetentionPolicy {
                    min_retained_epochs: 10,
                    read_lease_ttl_ms: 0,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        shard
            .write_edge(mutation(1, 2, "retained-1"))
            .await
            .unwrap();
        shard
            .write_edge(mutation(2, 3, "retained-2"))
            .await
            .unwrap();
        let base_epoch = shard.current_epoch(cell_id).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        let err = shard
            .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::RetentionViolation {
                operation: "delete_deltas_through_rollup",
                requested_epoch: 2,
                safe_epoch: 0,
                ..
            }
        ));
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn artifact_build_edge_limit_rejects_loaded_builds() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/artifact-edge-limit",
            object_store,
            GraphOpenOptions {
                limits: GraphLimits {
                    max_artifact_build_edges: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        shard
            .write_edge(mutation(1, 2, "edge-limit-1"))
            .await
            .unwrap();
        shard
            .write_edge(mutation(2, 3, "edge-limit-2"))
            .await
            .unwrap();
        shard
            .write_edge(mutation(3, 4, "edge-limit-3"))
            .await
            .unwrap();
        let err = shard
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 3, 2)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::AdmissionRejected {
                operation: "build_matrix_tiles_edges",
                actual: 3,
                limit: 2
            }
        ));
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn operational_metrics_track_writes_artifacts_gc_and_verifier() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/operational-metrics",
            object_store,
            GraphOpenOptions {
                retention_policy: GraphRetentionPolicy {
                    read_lease_ttl_ms: 0,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        shard.write_edge(mutation(1, 2, "metrics-1")).await.unwrap();
        shard.write_edge(mutation(2, 3, "metrics-2")).await.unwrap();
        let base_epoch = shard.current_epoch(cell_id).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        shard
            .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
            .await
            .unwrap();
        let report = shard
            .verify_current_graph(cell_id, edge_type, 2, 8)
            .await
            .unwrap();
        assert_eq!(report.mismatch_count, 0);

        let metrics = shard.graph_operational_metrics();
        assert!(metrics.write_attempts >= 2);
        assert!(metrics.write_commits >= 2);
        assert!(metrics.artifact_builds_started >= 2);
        assert!(metrics.artifact_builds_completed >= 2);
        assert!(metrics.artifact_build_duration_us > 0);
        assert!(metrics.artifact_publish_batches > 0);
        assert!(metrics.artifact_records_published > 0);
        assert!(metrics.artifact_publish_duration_us > 0);
        assert!(metrics.gc_jobs_started >= 1);
        assert!(metrics.gc_jobs_completed >= 1);
        assert!(metrics.gc_keys_deleted > 0);
        assert!(metrics.gc_duration_us > 0);
        assert!(metrics.verifier_runs >= 1);
        assert_eq!(metrics.verifier_failures, 0);
        assert!(metrics.verifier_duration_us > 0);
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
    async fn current_graph_verifier_survives_rollup_and_delta_gc() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        let shard = open_test_shard("graph/current-verifier-gc", object_store).await;
        shard.write_edge(mutation(1, 2, "verify-1")).await.unwrap();
        shard.write_edge(mutation(1, 3, "verify-2")).await.unwrap();
        let base_epoch = shard.current_epoch(cell_id).await.unwrap();
        shard
            .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
            .await
            .unwrap();
        shard
            .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
            .await
            .unwrap();
        shard.write_edge(mutation(1, 4, "verify-3")).await.unwrap();
        shard
            .delete_edge(mutation(1, 2, "verify-delete-1"))
            .await
            .unwrap();

        let report = shard
            .verify_current_graph(cell_id, edge_type, 3, 8)
            .await
            .unwrap();
        assert!(report.is_clean(), "{:?}", report.mismatch_samples);
        assert_eq!(report.digest.live_edges, 2);
        assert!(report.delta_gc_watermark >= base_epoch);
        assert!(report.matrix_edges_checked >= 2);
        assert!(report.traversal_roots_checked > 0);
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn current_graph_verifier_detects_index_corruption() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cell_id = "reddit-home";
        let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
        let shard = open_test_shard("graph/current-verifier-corrupt", object_store).await;
        shard
            .write_edge(mutation(1, 2, "verify-corrupt"))
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        batch.delete(keys::out_edge(cell_id, edge_type, 1, 2).as_bytes());
        shard.write_strict_for_test(batch).await.unwrap();

        let report = shard
            .verify_current_graph(cell_id, edge_type, 1, 4)
            .await
            .unwrap();
        assert!(!report.is_clean());
        assert!(
            report
                .mismatch_samples
                .iter()
                .any(|sample| sample.contains("out_index:missing")),
            "{:?}",
            report.mismatch_samples
        );
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
        assert_eq!(gc.deleted_delta_keys, 2);
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

    #[cfg(feature = "opencypher")]
    #[tokio::test]
    async fn cypher_create_and_match_use_storage_kernel() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/cypher-create-match", object_store).await;

        let write = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "cypher-req-1"),
                "CREATE (u {id: 10})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s {id: 20})",
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

    #[cfg(feature = "opencypher")]
    #[tokio::test]
    async fn cypher_where_and_variable_hops_use_storage_kernel() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = open_test_shard("graph/cypher-where-varhop", object_store).await;

        for (idx, (src, dst)) in [(1, 2), (2, 3), (3, 4), (1, 3), (1, 9)]
            .into_iter()
            .enumerate()
        {
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

        let exact_two_hop = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "read-req"),
                "MATCH (u {id: 1})-[:FOLLOWS*2..2]->(v) RETURN v.id",
            )
            .await
            .unwrap();
        assert_eq!(exact_two_hop, QueryOutput::Vertices(vec![3, 4]));

        for (idx, (src, dst)) in [(1, 2), (2, 3), (1, 3)].into_iter().enumerate() {
            shard
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "MASKED_BY_SHORTER_PATH".to_string(),
                    src,
                    dst,
                    idempotency_key: format!("cypher-shortest-mask-{idx}"),
                })
                .await
                .unwrap();
        }
        let shortest_mask_regression = shard
            .execute_cypher(
                QueryContext::new("reddit-home", "read-req"),
                "MATCH (u {id: 1})-[:MASKED_BY_SHORTER_PATH*2..2]->(v) RETURN v.id",
            )
            .await
            .unwrap();
        assert_eq!(shortest_mask_regression, QueryOutput::Vertices(vec![3]));
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
                .graph_cache_entry_counts()
                .await
                .materialized_supernodes,
            1
        );
        assert!(shard.graph_cache_metrics().materialized_supernode_misses >= 1);

        let one_hop_cached = shard
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
        assert_eq!(one_hop_cached.vertices, one_hop.vertices);
        assert!(shard.graph_cache_metrics().materialized_supernode_hits >= 1);
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
