use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use slatedb::bytes::Bytes;
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions};
use slatedb::object_store::{path::Path, ObjectStore};
#[cfg(test)]
use slatedb::object_store::{ObjectStoreExt, PutMode};
#[cfg(test)]
use slatedb::ErrorKind;
use slatedb::{DbTransaction, IsolationLevel, WriteBatch};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

#[cfg(feature = "client-api")]
mod client;
mod core;
mod engine;
mod placement;
mod query;
mod sparse_kernel;

#[cfg(feature = "bolt-server")]
pub use client::bolt::{
    BoltRoutingServer, BoltRoutingTable, BoltRoutingTableProvider, BoltServerConfig,
    BoltServerHandle, ClientBoltServer, RendezvousBoltRoutingTableProvider,
};
#[cfg(feature = "http-api")]
pub use client::http::{ClientHttpServer, HttpQueryServerConfig, HttpQueryServerHandle};
#[cfg(feature = "client-api")]
pub use client::service::{
    ClientBookmark, ClientDatabaseResolver, ClientQueryCredentials, ClientQueryMetricsSnapshot,
    ClientQueryPage, ClientQueryRequest, ClientQueryResult, ClientQueryService,
    ClientQueryServiceConfig, ClientQuerySession, ClientQueryTarget, StaticClientDatabaseResolver,
};
pub(crate) use core::cache::BoundedGraphCache;
#[cfg(feature = "opencypher")]
pub(crate) use core::cache::{
    source_relationship_rows_resident_bytes, RelationshipPropertyRowsCacheKey,
    RelationshipRowsCacheEntry, RelationshipRowsCacheKey, RelationshipRowsCacheValue,
    SourceRelationshipRowsCacheKey,
};
pub(crate) use core::config::{open_graph_db, open_graph_reader};
pub use core::config::{
    GraphBackpressurePolicy, GraphCacheConfig, GraphDurabilityConfig, GraphIndexPolicy,
    GraphLimits, GraphMemoryConfig, GraphOpenOptions, GraphStorageMemoryConfig,
    DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
};
pub use core::error::{GraphError, Result};
pub use core::metrics::{
    GraphCacheKind, GraphCacheMetricsSnapshot, GraphCachePolicy, GraphOperationalMetricsSnapshot,
};
pub(crate) use core::metrics::{GraphCacheMetrics, GraphOperationalMetrics};
pub use core::model::{
    BulkImportDeltaLogPolicy, BulkImportDuplicatePolicy, BulkImportOptions, BulkImportResult,
    CommitResult, DeleteResult, DeltaKind, DeltaRecord, EdgeDeleteBatchResult,
    EdgeExistenceBatchEntry, EdgeIngestOptions, EdgeIngestResult, EdgeMetadata, EdgeMutation,
    EdgeMutationBatchResult, EdgeMutationLogAppendResult, EdgeMutationLogMaterializeResult,
    EdgeRecord, GraphCellDropResult, GraphCorrectnessReport, GraphExportDigest, GraphRepairReport,
    NeighborBatchEntry, QueryFloat, RelationshipCreateResult, RelationshipId,
    RelationshipImportResult, RelationshipMutation, RelationshipRecord, SegmentCompactionResult,
    VertexDeleteResult, VertexMetadata, VertexPropertyValue,
};
pub(crate) use core::model::{EdgeMutationLogBatch, OutEdgeSegment, OutboxDeltaBatch};
pub use core::namespace::{
    GraphId, GraphScope, NamespaceId, NamespacePath, DEFAULT_GRAPH_ID, DEFAULT_NAMESPACE_ID,
    MAX_NAMESPACE_DEPTH,
};
pub use core::snapshot::{GraphSnapshot, OwnedGraphSnapshot};
#[cfg(feature = "opencypher")]
pub use core::state::QueryStatsRefreshHandle;
pub(crate) use core::state::{
    acquire_distributed_write_lock, is_retryable_write_conflict, release_cell_write_lock,
    CellWriteLock, GraphStore, GraphWriteAuthority, GraphWriteOp,
};
#[cfg(test)]
pub(crate) use core::state::{
    decode_cell_write_lock_record, encode_cell_write_lock_record, CellWriteLockState,
};
pub use core::state::{GraphCacheEntryCounts, GraphCacheResidentBytes, GraphShard};
pub(crate) use core::write_batch::{GraphWriteBatch, GraphWriteGuard};
pub use engine::{
    local_object_store, object_store_from_env, ArtifactGcResult, BenchmarkResult, DeltaGcResult,
    GraphCluster, GraphNodeMaintenanceMetricsSnapshot, GraphShardRuntimeMetrics, MatrixArtifact,
    MatrixArtifactRefreshHandle, MatrixArtifactRefreshPolicy, MatrixArtifactRefreshReport,
    MatrixTraversalResult, RoutedGraphCluster, ShardPlacement, TraversalBackend,
};
pub use placement::{
    compare_locality_layouts, locality_cell_id, locality_cell_prefix, locality_cell_prefix_len,
    LocalityCellExtractor, LocalityLayoutExperiment, StorageLayout,
};
pub use query::algebra::{
    LogicalQueryPlan, PhysicalQueryPlan, QueryBatchEdge, QueryBatchOperation,
    QueryBatchRelationship, QueryBatchRelationshipMerge, QueryBatchVertex, QueryCancellationToken,
    QueryCardinalityStatsKind, QueryCardinalityStatsRefresh, QueryColumn, QueryContext,
    QueryCursorToken, QueryMutationResult, QueryOutput, QueryParameterValue, QueryPlan,
    QueryPlanner, QueryResultPage, QueryResultSet, QueryRow, QueryStatement,
    QueryStatsHistogramRefresh, QueryStatsRecord, QueryStatsRefreshKind, QueryStatsRefreshResult,
    QueryStatsRefreshSpec, QueryValue, QueryWindow, RowQueryAccess, RowQueryOptimizerPass,
    RowQueryPlan, RowQueryPlanGroup, RowQueryPlanPattern,
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
    QueryServiceDirectory, QueryServiceDiscovery, QueryServiceEndpoint, QueryTransportAction,
    QueryTransportAuthPolicy, QueryTransportCancellationPrincipal, QueryTransportClientConfig,
    QueryTransportConnectionIdentity, QueryTransportMetricsSnapshot, QueryTransportNamespaceQuotas,
    QueryTransportPrincipal, QueryTransportScopeAuthorizer, QueryTransportScopeGrant,
    QueryTransportSecret, QueryTransportServerConfig, StaticQueryServiceDiscovery,
    StaticQueryTransportScopeAuthorizer, TcpQueryCellClient, TcpQueryRowStream, TcpQueryServer,
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
    parse_opencypher_mutation_query_with_parameters, parse_opencypher_row_query,
    parse_opencypher_row_query_with_parameters, ParsedMutationQuery, ParsedRowQuery,
    RowAggregateFunction, RowComparisonOp, RowEdgePattern, RowExpression, RowMatchGroup,
    RowMutationAction, RowNodePattern, RowPattern, RowPredicate, RowProjection, RowSort,
    RowSortExpression,
};
pub use sparse_kernel::SparseKernelBackend;

pub type VertexId = u64;
/// SlateDB's sequence number for a committed storage snapshot.
pub type StorageSequence = u64;

/// Monotonic cursor for topology changes consumed by asynchronous matrix builds.
/// This is not a second storage MVCC system; canonical record visibility belongs
/// to SlateDB snapshots.
pub type TopologySequence = u64;

pub(crate) type MatrixAdjacency = BTreeMap<VertexId, BTreeSet<VertexId>>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct MatrixCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) base_epoch: TopologySequence,
}

impl MatrixCacheKey {
    pub(crate) fn new(cell_id: &str, edge_type: &str, base_epoch: TopologySequence) -> Self {
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
static GRAPH_CELL_WRITE_LOCK_COUNTER: AtomicU64 = AtomicU64::new(1);

mod codec;
pub(crate) use codec::*;

mod keys;
mod shard;

#[cfg(test)]
mod namespace_tests;
#[cfg(test)]
mod tests;
