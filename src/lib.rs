use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use slatedb::bytes::Bytes;
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions};
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
#[cfg(test)]
use slatedb::ErrorKind;
use slatedb::{Db, DbTransaction, IsolationLevel, WriteBatch};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

mod core;
mod engine;
mod placement;
mod query;
mod sparse_kernel;

pub(crate) use core::cache::{BoundedGraphCache, PostingChunkCacheKey, SupernodeCacheKey};
#[cfg(feature = "opencypher")]
pub(crate) use core::cache::{
    RelationshipPropertyRowsCacheKey, RelationshipRowsCacheEntry, RelationshipRowsCacheKey,
    RelationshipRowsCacheValue, SourceRelationshipRowsCacheKey,
};
pub(crate) use core::config::open_graph_db;
pub use core::config::{
    GraphBackpressurePolicy, GraphCacheConfig, GraphDurabilityConfig, GraphIndexPolicy,
    GraphLimits, GraphOpenOptions, GraphRetentionPolicy, DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
};
pub use core::error::{GraphError, Result};
pub use core::metrics::{
    GraphCacheKind, GraphCacheMetricsSnapshot, GraphCachePolicy, GraphOperationalMetricsSnapshot,
};
pub(crate) use core::metrics::{GraphCacheMetrics, GraphOperationalMetrics};
pub use core::model::{
    BulkImportDeltaLogPolicy, BulkImportDuplicatePolicy, BulkImportOptions, BulkImportResult,
    CommitResult, DeleteResult, DeltaKind, DeltaRecord, EdgeDeleteBatchResult, EdgeIngestOptions,
    EdgeIngestResult, EdgeMetadata, EdgeMutation, EdgeMutationBatchResult,
    EdgeMutationLogAppendResult, EdgeMutationLogMaterializeResult, EdgeRecord, GraphCellDropResult,
    GraphCorrectnessReport, GraphExportDigest, GraphRepairReport, QueryFloat,
    RelationshipCreateResult, RelationshipId, RelationshipImportResult, RelationshipMutation,
    RelationshipRecord, SegmentCompactionResult, VertexDeleteResult, VertexMetadata,
    VertexPropertyValue,
};
pub(crate) use core::model::{EdgeMutationLogBatch, OutEdgeSegment, OutboxDeltaBatch};
pub use core::snapshot::GraphSnapshot;
#[cfg(feature = "opencypher")]
pub use core::state::QueryStatsRefreshHandle;
pub(crate) use core::state::{
    decode_cell_write_lock_record, encode_cell_write_lock_record, is_retryable_write_conflict,
    release_cell_write_lock, CellWriteLock, CellWriteLockState, GraphReadLease,
    GraphWriteAuthority, GraphWriteFence, GraphWriteOp,
};
pub use core::state::{GraphCacheEntryCounts, GraphShard};
pub(crate) use core::write_batch::GraphWriteBatch;
pub use engine::{
    local_object_store, object_store_from_env, ArtifactDirection, ArtifactGcResult,
    BenchmarkResult, DeltaGcResult, GraphCluster, GraphClusterControllerConfig,
    GraphClusterControllerHandle, GraphClusterControllerReport, GraphClusterRebalanceMode,
    GraphControlCellDropReport, GraphControlEdgeWatermark, GraphControlIdempotencyRecord,
    GraphControlMetricsSnapshot, GraphControlPlane, GraphControlRepairReport,
    GraphControlWatermark, GraphNode, GraphNodeHealthState, GraphNodeHeartbeat,
    GraphNodeMaintenanceMetricsSnapshot, GraphNodeRuntimeConfig, GraphPendingFailover, GraphRollup,
    GraphShardCatalogEntry, GraphShardReassignment, GraphShardRefreshReport, LeaseRenewalHandle,
    ManagedGraphNode, MatrixArtifact, MatrixTraversalResult, NodeHeartbeatHandle, PostingChunk,
    RoutedGraphCluster, ShardLease, ShardPlacement, ShardRefreshHandle, SupernodeGroup,
    SupernodePage, TraversalBackend,
};
pub use placement::{
    compare_locality_layouts, locality_cell_id, locality_cell_prefix, locality_cell_prefix_len,
    LocalityCellExtractor, LocalityLayoutExperiment, StorageLayout,
};
pub use query::algebra::{
    LogicalQueryPlan, PhysicalQueryPlan, QueryCancellationToken, QueryCardinalityStatsKind,
    QueryCardinalityStatsRefresh, QueryColumn, QueryContext, QueryCursorToken, QueryMutationResult,
    QueryOutput, QueryPlan, QueryPlanner, QueryResultPage, QueryResultSet, QueryRow,
    QueryStatement, QueryStatsHistogramRefresh, QueryStatsRecord, QueryStatsRefreshKind,
    QueryStatsRefreshResult, QueryStatsRefreshSpec, QueryValue, QueryWindow, RowQueryAccess,
    RowQueryOptimizerPass, RowQueryPlan, RowQueryPlanGroup, RowQueryPlanPattern,
};
#[cfg(feature = "query-service-discovery")]
pub use query::coordination::{
    ConsulQueryServiceDiscovery, EtcdQueryServiceDiscovery, KubernetesQueryServiceDiscovery,
};
#[cfg(feature = "opencypher")]
pub use query::coordination::{
    DistributedQueryCoordinator, DistributedQueryJoin, DistributedQueryLeg, DistributedQueryMerge,
    DistributedQueryPageRequest, DistributedQueryPlan, DistributedQueryPlanResult, QueryCellClient,
};
#[cfg(feature = "query-transport")]
pub use query::coordination::{
    QueryServiceDirectory, QueryServiceDiscovery, QueryServiceEndpoint, QueryTransportAuthPolicy,
    QueryTransportClientConfig, QueryTransportConnectionIdentity, QueryTransportMetricsSnapshot,
    QueryTransportSecret, QueryTransportServerConfig, StaticQueryServiceDiscovery,
    TcpQueryCellClient, TcpQueryRowStream, TcpQueryServer,
};
#[cfg(feature = "query-transport-tls")]
pub use query::coordination::{
    QueryTransportTlsClientConfigProvider, QueryTransportTlsServerConfigProvider,
    ReloadableQueryTransportTlsClientConfigProvider,
    ReloadableQueryTransportTlsServerConfigProvider, StaticQueryTransportTlsClientConfigProvider,
    StaticQueryTransportTlsServerConfigProvider,
};
#[cfg(feature = "opencypher")]
pub use query::corpus::{
    parse_opencypher_tck_corpus, parse_opencypher_tck_corpus_dir, CypherTckCase,
    CypherTckCompatibilityReport, CypherTckCorpus,
};
#[cfg(feature = "opencypher")]
pub use query::opencypher::{
    parse_cypher, parse_cypher_with_parameters, parse_cypher_with_window, parse_opencypher,
    parse_opencypher_mutation_query_with_parameters, parse_opencypher_row_query,
    parse_opencypher_row_query_with_parameters, parse_opencypher_with_parameters,
    parse_opencypher_with_window, CypherFrontend, DefaultCypherFrontend, LibCypherParserFrontend,
    ParsedMutationQuery, ParsedQuery, ParsedRowQuery, RowAggregateFunction, RowComparisonOp,
    RowEdgePattern, RowExpression, RowMatchGroup, RowMutationAction, RowNodePattern, RowPattern,
    RowPredicate, RowProjection, RowSort, RowSortExpression,
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

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ParsedRowQueryCacheKey {
    query: String,
}

#[cfg(feature = "opencypher")]
impl ParsedRowQueryCacheKey {
    pub(crate) fn new(query: &str) -> Self {
        Self {
            query: query.to_string(),
        }
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ReachabilityCacheKey {
    cell_id: String,
    edge_type: String,
    src: VertexId,
    min_hops: u8,
    max_hops: u8,
    read_epoch: GraphEpoch,
    window: Option<ReachabilityCacheWindow>,
}

#[cfg(feature = "opencypher")]
impl ReachabilityCacheKey {
    pub(crate) fn new(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: GraphEpoch,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            min_hops: hop_range.0,
            max_hops: hop_range.1,
            read_epoch,
            window: None,
        }
    }

    pub(crate) fn new_window(
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: GraphEpoch,
        window: QueryWindow,
        ascending: bool,
    ) -> Self {
        Self {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            min_hops: hop_range.0,
            max_hops: hop_range.1,
            read_epoch,
            window: Some(ReachabilityCacheWindow {
                skip: window.skip,
                limit: window.limit,
                ascending,
            }),
        }
    }

    pub(crate) fn cell_id(&self) -> &str {
        &self.cell_id
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReachabilityCacheWindow {
    skip: u64,
    limit: Option<usize>,
    ascending: bool,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReachabilityCacheValue {
    vertices: Option<Arc<Vec<VertexId>>>,
    count: u64,
    edge_visits: u64,
}

#[cfg(feature = "opencypher")]
impl ReachabilityCacheValue {
    pub(crate) fn from_vertices(vertices: Vec<VertexId>, edge_visits: u64) -> Self {
        let count = vertices.len() as u64;
        Self {
            vertices: Some(Arc::new(vertices)),
            count,
            edge_visits,
        }
    }

    pub(crate) fn count_only(count: u64, edge_visits: u64) -> Self {
        Self {
            vertices: None,
            count,
            edge_visits,
        }
    }

    pub(crate) fn vertices(&self) -> Option<Arc<Vec<VertexId>>> {
        self.vertices.clone()
    }

    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    pub(crate) fn edge_visits(&self) -> u64 {
        self.edge_visits
    }
}

pub(crate) const GRAPH_TXN_MAX_RETRIES: usize = 32;
pub(crate) const GRAPH_DELTA_GC_BATCH_KEYS: usize = 512;
pub(crate) const GRAPH_WRITE_LANES: usize = 64;
pub(crate) const GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS: usize = 256;
pub(crate) const GRAPH_CELL_WRITE_LOCK_BACKOFF_MS: u64 = 2;
pub(crate) const GRAPH_CELL_WRITE_LOCK_TTL_MS: u64 = 5 * 60 * 1000;
// Release profiling showed larger materialization transactions regress from SlateDB
// write-batch and conflict-tracking overhead; keep async drains in the same
// microbatch range as foreground indexed writes.
pub(crate) const GRAPH_MUTATION_LOG_MATERIALIZE_TXN_EDGES: usize = 512;
pub(crate) const GRAPH_STORE_FORMAT_KEY: &str = "graph/meta/format_version";
pub(crate) const GRAPH_STORE_FORMAT_VERSION: u64 = 1;
static GRAPH_READ_LEASE_COUNTER: AtomicU64 = AtomicU64::new(1);
static GRAPH_CELL_WRITE_LOCK_COUNTER: AtomicU64 = AtomicU64::new(1);

mod codec;
pub(crate) use codec::*;

mod keys;
mod shard;

#[cfg(test)]
mod tests;
