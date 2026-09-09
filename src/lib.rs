use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use slatedb::bytes::Bytes;
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions};
use slatedb::object_store::{path::Path, ObjectStore};
#[cfg(test)]
use slatedb::ErrorKind;
#[cfg(test)]
use slatedb::WriteBatch;
use slatedb::{DbTransaction, IsolationLevel};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

#[cfg(feature = "client-api")]
mod client;
mod core;
mod engine;
// Storage locality layout: which SlateDB prefix a key belongs to. Named
// `locality` and not `placement` because the kernel now has a real placement
// module (`engine::placement`, which decides who owns a cell's *writer*), and
// two modules called `placement` -- one of which is not about placement at all
// -- is the same confusion decision 12 of the rendezvous plan avoided by
// renaming. Private, so this costs nothing outside the crate: every item below
// is re-exported under its own unchanged name.
mod locality;
mod query;
mod sparse_kernel;

#[cfg(feature = "bolt-server")]
pub use client::bolt::{
    BoltRoutingServer, BoltRoutingTable, BoltRoutingTableProvider, BoltServerConfig,
    BoltServerHandle, ClientBoltServer, ObjectStoreBoltRoutingTableProvider,
};
#[cfg(feature = "http-api")]
pub use client::http::{ClientHttpServer, HttpQueryServerConfig, HttpQueryServerHandle};
#[cfg(feature = "client-api")]
pub use client::service::{
    ClientBookmark, ClientDatabaseResolver, ClientQueryCredentials, ClientQueryMetricsSnapshot,
    ClientQueryPage, ClientQueryRequest, ClientQueryResult, ClientQueryService,
    ClientQueryServiceConfig, ClientQuerySession, ClientQueryTarget, ClientReadConsistency,
    HierarchicalClientDatabaseResolver, StaticClientDatabaseResolver,
};
pub(crate) use core::cache::BoundedGraphCache;
#[cfg(feature = "opencypher")]
pub(crate) use core::cache::{
    source_relationship_rows_resident_bytes, NativePathResultCacheKey, NativePathResultCacheValue,
    RelationshipPropertyRowsCacheKey, RelationshipRowsCacheEntry, RelationshipRowsCacheKey,
    RelationshipRowsCacheValue, SourceRelationshipRowsCacheKey,
};
pub(crate) use core::config::{open_graph_db, open_graph_reader};
pub use core::config::{
    GraphBackpressurePolicy, GraphCacheConfig, GraphDurabilityConfig, GraphIndexPolicy,
    GraphLimits, GraphMemoryConfig, GraphOpenOptions, GraphStorageMemoryConfig,
    DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
};
pub use core::error::{GraphError, Result};
// Widen this as H1 converts the remaining client duration counters. It is no
// longer feature-gated: `GraphOperationalMetrics::query_rows_latency` is a
// default-features field, so the type is constructed on every build.
pub(crate) use core::histogram::AtomicDurationHistogram;
pub use core::histogram::{
    DurationHistogramSnapshot, DURATION_BUCKET_BOUNDS_US, DURATION_BUCKET_COUNT,
};
pub use core::metrics::{
    GraphCacheKind, GraphCacheMetricsSnapshot, GraphCachePolicy, GraphOperationalMetricsSnapshot,
};
pub(crate) use core::metrics::{GraphCacheMetrics, GraphOperationalMetrics};
pub(crate) use core::model::OutEdgeSegment;
pub use core::model::{
    BulkImportDuplicatePolicy, BulkImportOptions, BulkImportResult, CommitResult, DeleteResult,
    EdgeDeleteBatchResult, EdgeExistenceBatchEntry, EdgeIngestOptions, EdgeIngestResult,
    EdgeMetadata, EdgeMutation, EdgeMutationBatchResult, EdgeRecord, GraphCellDropResult,
    GraphCorrectnessReport, GraphExportDigest, GraphRepairReport, NeighborBatchEntry, QueryFloat,
    RelationshipCreateResult, RelationshipId, RelationshipImportResult, RelationshipMutation,
    RelationshipRecord, SegmentCompactionResult, VertexDeleteResult, VertexMetadata,
    VertexPropertyValue,
};
pub use core::namespace::{
    GraphId, GraphScope, NamespaceId, NamespacePath, DEFAULT_GRAPH_ID, DEFAULT_NAMESPACE_ID,
    MAX_NAMESPACE_DEPTH,
};
pub use core::snapshot::GraphSnapshot;
#[cfg(feature = "opencypher")]
pub use core::state::QueryStatsRefreshHandle;
pub(crate) use core::state::{
    finish_local_write, is_retryable_write_conflict, process_writer_registry, GraphStorageSnapshot,
    GraphStore, GraphWriteAuthority, GraphWriteOp, LocalWriteGuard,
};
pub use core::state::{
    GraphCacheEntryCounts, GraphCacheResidentBytes, GraphShard, ProcessWriterRegistry,
};
pub use core::trace_context::{install_trace_context_bridge, TraceContextBridge};
pub(crate) use core::write_batch::{GraphWriteBatch, GraphWriteGuard};
#[cfg(feature = "query-transport")]
pub use engine::ScopedRoutedGraphCluster;
pub use engine::{
    local_object_store, object_store_from_env, probe_store_writable, ArtifactGcResult,
    BenchmarkResult, CellOwnership, GraphCluster, GraphIndexBuildPath, GraphIndexGeneration,
    GraphShardRuntimeMetrics, MatrixArtifact, MatrixTraversalResult, ObjectStoreGraphScopeDirectory,
    ObjectStoreNodeDirectory, ObjectStoreWriterLeaseDirectory, PlacementConfig,
    PlacementRefreshHandle, PlacementView, RoutedGraphCluster, ScopedGraphShardRuntimeMetrics,
    TraversalBackend, WriterLeaseOwner, WriterLeaseRenewalFailure,
};
pub use locality::{
    compare_locality_layouts, locality_cell_id, locality_cell_prefix, locality_cell_prefix_len,
    LocalityCellExtractor, LocalityLayoutExperiment, StorageLayout,
};
pub use query::algebra::{
    LogicalQueryPlan, PhysicalQueryPlan, QueryBatchEdge, QueryBatchMergePolicy,
    QueryBatchOperation, QueryBatchRelationship, QueryBatchRelationshipMerge, QueryBatchVertex,
    QueryCancellationToken, QueryCardinalityStatsKind, QueryCardinalityStatsRefresh, QueryColumn,
    QueryContext, QueryCursorToken, QueryMutationResult, QueryOutput, QueryParameterValue,
    QueryPath, QueryPathNode, QueryPathRelationship, QueryPlan, QueryPlanner, QueryResultPage,
    QueryResultSet, QueryRow, QueryStatement, QueryStatsHistogramRefresh, QueryStatsRecord,
    QueryStatsRefreshKind, QueryStatsRefreshResult, QueryStatsRefreshSpec, QueryValue, QueryWindow,
    RowQueryAccess, RowQueryOptimizerPass, RowQueryPlan, RowQueryPlanGroup, RowQueryPlanPattern,
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

pub(crate) type MatrixAdjacency = BTreeMap<VertexId, BTreeSet<VertexId>>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct MatrixCacheKey {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) base_epoch: StorageSequence,
}

impl MatrixCacheKey {
    pub(crate) fn new(cell_id: &str, edge_type: &str, base_epoch: StorageSequence) -> Self {
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
pub(crate) const GRAPH_MAINTENANCE_BATCH_KEYS: usize = 512;
pub(crate) const GRAPH_WRITE_LANES: usize = 64;

mod codec;
pub(crate) use codec::*;

mod keys;
mod shard;

#[cfg(test)]
mod namespace_tests;
#[cfg(test)]
mod tests;
