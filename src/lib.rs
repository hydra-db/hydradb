use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use slatedb::bytes::Bytes;
use slatedb::config::{
    DurabilityLevel, PreloadLevel, ReadOptions, ScanOptions, Settings, WriteOptions,
};
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
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
    BenchmarkResult, DeltaGcResult, GraphControlEdgeWatermark, GraphControlIdempotencyRecord,
    GraphControlMetricsSnapshot, GraphControlPlane, GraphControlRepairReport,
    GraphControlWatermark, GraphNode, GraphRollup, GraphShardCatalogEntry, LeaseRenewalHandle,
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
    #[error("cell write conflict for {operation} on {cell_id}")]
    CellWriteConflict {
        operation: &'static str,
        cell_id: String,
    },
    #[error("{operation} requires await_durable_writes=true: {reason}")]
    UnsafeDurabilityConfig {
        operation: &'static str,
        reason: String,
    },
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
        "control metadata conflict at {key}: expected generation {expected_generation:?}, actual generation {actual_generation:?}"
    )]
    ControlMetadataConflict {
        key: String,
        expected_generation: Option<u64>,
        actual_generation: Option<u64>,
    },
    #[error(
        "control watermark regression for {field} on cell {cell_id}: requested {requested_epoch}, current {current_epoch}"
    )]
    ControlWatermarkRegression {
        cell_id: String,
        field: &'static str,
        requested_epoch: GraphEpoch,
        current_epoch: GraphEpoch,
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
const GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS: usize = 256;
const GRAPH_CELL_WRITE_LOCK_BACKOFF_MS: u64 = 2;
const GRAPH_CELL_WRITE_LOCK_TTL_MS: u64 = 5 * 60 * 1000;
pub const DEFAULT_TRUSTED_APPEND_CHUNK_EDGES: usize = 32_768;
// Release profiling showed larger materialization transactions regress from SlateDB
// write-batch and conflict-tracking overhead; keep async drains in the same
// microbatch range as foreground indexed writes.
const GRAPH_MUTATION_LOG_MATERIALIZE_TXN_EDGES: usize = 512;
const GRAPH_STORE_FORMAT_KEY: &str = "graph/meta/format_version";
const GRAPH_STORE_FORMAT_VERSION: u64 = 1;
static GRAPH_READ_LEASE_COUNTER: AtomicU64 = AtomicU64::new(1);
static GRAPH_CELL_WRITE_LOCK_COUNTER: AtomicU64 = AtomicU64::new(1);

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

    pub fn checked_batch_append() -> Self {
        Self {
            duplicate_policy: BulkImportDuplicatePolicy::CheckExisting,
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
    object_store: Arc<dyn ObjectStore>,
    store_path: Path,
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

struct CellWriteLock {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    owner_token: String,
}

impl CellWriteLock {
    async fn release(self) -> Result<()> {
        let current = match self.object_store.get(&self.path).await {
            Ok(current) => current,
            Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let version = UpdateVersion {
            e_tag: current.meta.e_tag.clone(),
            version: current.meta.version.clone(),
        };
        let value = current.bytes().await?;
        let record = decode_cell_write_lock_record(self.path.as_ref(), &value)?;
        if record.owner_token != self.owner_token {
            return Ok(());
        }
        let payload = encode_cell_write_lock_record(
            &record.cell_id,
            &record.operation,
            &self.owner_token,
            record.created_ms,
            0,
            CellWriteLockState::Released,
        );
        match self
            .object_store
            .put_opts(&self.path, payload.into(), PutMode::Update(version).into())
            .await
        {
            Ok(_) | Err(slatedb::object_store::Error::Precondition { .. }) => Ok(()),
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
            Err(slatedb::object_store::Error::NotImplemented { .. })
            | Err(slatedb::object_store::Error::NotSupported { .. }) => {
                match self.object_store.delete(&self.path).await {
                    Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
                    Err(err) => Err(err.into()),
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CellWriteLockState {
    Active,
    Released,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CellWriteLockRecord {
    cell_id: String,
    operation: String,
    owner_token: String,
    created_ms: u64,
    expires_at_ms: u64,
    state: CellWriteLockState,
}

impl CellWriteLockRecord {
    fn is_expired(&self, now_ms: u64) -> bool {
        self.state == CellWriteLockState::Released || self.expires_at_ms <= now_ms
    }
}

fn encode_cell_write_lock_record(
    cell_id: &str,
    operation: &str,
    owner_token: &str,
    created_ms: u64,
    expires_at_ms: u64,
    state: CellWriteLockState,
) -> Bytes {
    let state = match state {
        CellWriteLockState::Active => "active",
        CellWriteLockState::Released => "released",
    };
    Bytes::from(format!(
        "graph-cell-write-lock-v2\ncell={cell_id}\noperation={operation}\nowner_token={owner_token}\ncreated_ms={created_ms}\nexpires_at_ms={expires_at_ms}\nstate={state}\n"
    ))
}

fn decode_cell_write_lock_record(key: &str, value: &[u8]) -> Result<CellWriteLockRecord> {
    let text = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })?;
    let mut lines = text.trim_end_matches('\n').lines();
    let Some(header) = lines.next() else {
        return Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "empty cell write lock record".to_string(),
        });
    };
    let mut fields = BTreeMap::new();
    for line in lines {
        let Some((name, value)) = line.split_once('=') else {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("invalid cell write lock field {line}"),
            });
        };
        fields.insert(name, value);
    }
    let field = |name: &'static str| -> Result<&str> {
        fields
            .get(name)
            .copied()
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("missing cell write lock field {name}"),
            })
    };
    let cell_id = field("cell")?;
    validate_component("cell_id", cell_id)?;
    let operation = field("operation")?;
    validate_component("operation", operation)?;

    match header {
        "graph-cell-write-lock-v1" => {
            let created_ms = parse_u64(key, field("created_ms")?, "created_ms")?;
            Ok(CellWriteLockRecord {
                cell_id: cell_id.to_string(),
                operation: operation.to_string(),
                owner_token: String::new(),
                created_ms,
                expires_at_ms: created_ms.saturating_add(GRAPH_CELL_WRITE_LOCK_TTL_MS),
                state: CellWriteLockState::Active,
            })
        }
        "graph-cell-write-lock-v2" => {
            let owner_token = field("owner_token")?;
            let created_ms = parse_u64(key, field("created_ms")?, "created_ms")?;
            let expires_at_ms = parse_u64(key, field("expires_at_ms")?, "expires_at_ms")?;
            let state = match field("state")? {
                "active" => CellWriteLockState::Active,
                "released" => CellWriteLockState::Released,
                other => {
                    return Err(GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: format!("invalid cell write lock state {other}"),
                    });
                }
            };
            Ok(CellWriteLockRecord {
                cell_id: cell_id.to_string(),
                operation: operation.to_string(),
                owner_token: owner_token.to_string(),
                created_ms,
                expires_at_ms,
                state,
            })
        }
        other => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("unsupported cell write lock record {other}"),
        }),
    }
}

async fn release_cell_write_lock<T>(lock: CellWriteLock, result: Result<T>) -> Result<T> {
    match (result, lock.release().await) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), _) => Err(err),
    }
}

fn is_retryable_write_conflict(err: &GraphError) -> bool {
    matches!(err, GraphError::Slate(err) if err.kind() == ErrorKind::Transaction)
        || matches!(err, GraphError::CellWriteConflict { .. })
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

mod codec;
pub(crate) use codec::*;

mod keys;
mod shard;

#[cfg(test)]
mod tests;
