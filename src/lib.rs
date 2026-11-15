use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::object_store::{path::Path, ObjectStore};
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
mod offline_image;
mod query;
mod sparse_kernel;

#[cfg(feature = "bolt-server")]
pub use client::bolt::{
    BoltReadRouting, BoltRoutingServer, BoltRoutingTable, BoltRoutingTableProvider, BoltServerConfig, BoltServerHandle, ClientBoltServer,
    ObjectStoreBoltRoutingTableProvider,
};
#[cfg(feature = "http-api")]
pub use client::http::{ClientHttpServer, HttpQueryServerConfig, HttpQueryServerHandle};
#[cfg(feature = "client-api")]
pub use client::service::{
    ClientBookmark, ClientDatabaseResolver, ClientQueryCredentials, ClientQueryMetricsSnapshot, ClientQueryPage, ClientQueryRequest,
    ClientQueryResult, ClientQueryService, ClientQueryServiceConfig, ClientQuerySession, ClientQueryTarget, ClientReadConsistency,
    CypherEngineSelection, FirstQueryOutcomes, HierarchicalClientDatabaseResolver, MutationAtomicitySelection,
    StaticClientDatabaseResolver,
};
pub use core::available_memory::cgroup_directory;
pub(crate) use core::cache::BoundedGraphCache;
#[cfg(feature = "opencypher")]
pub(crate) use core::cache::{
    source_relationship_rows_resident_bytes, GdsProjectedGraph, GdsProjectedGraphCacheKey, NativePathResultCacheKey,
    NativePathResultCacheValue, RelationshipPropertyRowsCacheKey, RelationshipRowsCacheEntry, RelationshipRowsCacheValue,
    SourceRelationshipRowsCacheKey, MAX_GDS_PROJECTED_GRAPHS,
};
pub use core::cache_observe::{
    CacheObserveSnapshot, EntryKind, RemovalReason, ScopeObserveSnapshot, ENTRY_KIND_COUNT, MAX_OBSERVED_SCOPES, MAX_SCOPE_PATH_BYTES,
    REMOVAL_REASON_COUNT,
};
pub use core::config::{
    GraphBackpressurePolicy, GraphCacheConfig, GraphDurabilityConfig, GraphIndexPolicy, GraphLimits, GraphMemoryConfig, GraphOpenOptions,
    GraphReaderMode, GraphStorageMemoryConfig, PlannerStatisticsConfig, DEFAULT_TRUSTED_APPEND_CHUNK_EDGES, GRAPH_SST_COMPRESSION_CODEC,
    GRAPH_SST_TARGET_BYTES,
};
pub use core::db_cache::SlateDbCacheMetricsSnapshot;
#[cfg(feature = "query-transport")]
pub(crate) use core::db_cache::{process_slate_db_cache_with_config, SharedSlateDbCache};
pub use core::error::{GqlStatusReport, GraphError, QueryFailureReason, Result};
pub use core::metrics::{QueryFailureCountsSnapshot, QueryFailureStage};
#[cfg(feature = "query-telemetry")]
pub use hydradb_query_telemetry::QueryFailureReporter;
// Widen this as H1 converts the remaining client duration counters. It is no
// longer feature-gated: `GraphOperationalMetrics::query_rows_latency` is a
// default-features field, so the type is constructed on every build.
pub use core::create_only::{probe_create_only_puts, CreateOnlyProbeError, WriterGate};
#[cfg(feature = "query-transport")]
pub(crate) use core::histogram::AtomicDurationHistogram;
pub use core::histogram::{DurationHistogramSnapshot, DURATION_BUCKET_BOUNDS_US, DURATION_BUCKET_COUNT};
pub use core::memory_diagnostics::{memory_diagnostic_snapshot, MemoryDiagnosticSnapshot};
pub use core::metrics::{
    CellOperationalCounterTotals, ExperimentalOperatorMetricsSnapshot, GraphCacheKind, GraphCacheMetricsSnapshot, GraphCachePolicy,
    GraphOperationalMetricsSnapshot, PlannerStatisticsLoadedSnapshot,
};
pub(crate) use core::metrics::{
    CumulativeOperationalCounters, CumulativeOperationalRegistration, GraphCacheMetrics, GraphOperationalMetrics, RelationshipImportProfile,
};
#[cfg(feature = "opencypher")]
pub(crate) use core::model::RelationshipDeleteBatchResult;
pub use core::model::{
    stored_relationship_id, BulkImportDuplicatePolicy, BulkImportOptions, BulkImportResult, CommitResult, DeleteResult,
    EdgeDeleteBatchResult, EdgeExistenceBatchEntry, EdgeIngestOptions, EdgeIngestResult, EdgeMetadata, EdgeMutation,
    EdgeMutationBatchResult, EdgeRecord, GraphCellDropResult, GraphCorrectnessReport, GraphExportDigest, GraphRepairReport,
    NeighborBatchEntry, QueryFloat, RelationshipCreateResult, RelationshipId, RelationshipImportResult, RelationshipMutation,
    RelationshipRecord, SegmentCompactionResult, VertexCreateResult, VertexDeleteResult, VertexMetadata, VertexPropertyInteger,
    VertexPropertyList, VertexPropertyValue, GENERATED_RELATIONSHIP_ID_FLOOR,
};
pub use core::namespace::{
    encode_scope_namespace_id, graph_scope_for_database, DatabaseName, GraphId, GraphScope, NamespaceId, NamespacePath, DEFAULT_GRAPH_ID,
    DEFAULT_NAMESPACE_ID, MAX_NAMESPACE_DEPTH,
};
pub use core::snapshot::{GraphInspection, GraphSnapshot};
pub(crate) use core::state::{
    finish_local_write, is_retryable_write_conflict, process_writer_registry, GraphStorageSnapshot, GraphWriteAuthority, GraphWriteOp,
    LocalWriteGuard,
};
pub use core::state::{
    BookmarkWait, GraphCacheEntryCounts, GraphCacheResidentBytes, GraphShard, GraphStorageMetricsSnapshot, NodeWriters,
    ProcessWriterRegistry, SlateGraphStore, WalFeed,
};
pub use core::trace_context::{install_trace_context_bridge, TraceContextBridge};
pub(crate) use core::write_batch::{GraphWriteBatch, GraphWriteGuard};
#[cfg(feature = "query-transport")]
pub use engine::ScopedRoutedGraphCluster;
pub use engine::{
    decode_planner_statistics, encode_planner_statistics, local_object_store, object_store_from_env, remote_object_store_snapshot,
    ArtifactGcResult, BenchmarkResult, CellOwnership, GraphCluster, GraphIndexBuildPath, GraphIndexGeneration, GraphScopeChange,
    GraphShardRuntimeMetrics, MatrixArtifact, MatrixTraversalResult, ObjectStoreGraphScopeDirectory, ObjectStoreNodeDirectory,
    ObjectStoreWriterLeaseDirectory, PlacementConfig, PlacementRefreshHandle, PlacementView, PlannerStatistics,
    PlannerStatisticsBuildLimits, PlannerStatisticsPublish, PlannerStatisticsPublisher, PlannerStatisticsRequest, PropertyIndexStatistics,
    PropertySample, RemoteObjectStoreSnapshot, RemoteStorageProfile, RoutedGraphCluster, ScopedGraphShardRuntimeMetrics, SharedDiskCache,
    SharedDiskCacheMetrics, TagRange, TraversalBackend, WriterLeaseOwner, WriterLeaseRenewalFailure, PLANNER_STATISTICS_FORMAT_VERSION,
    PLANNER_STATISTICS_TOP_VALUES,
};
pub use hydradb_graph_plan::{
    explain_physical_plan, lower_cypher_ast, plan_physical, BoundValue, GraphLogicalPlan, GraphPhysicalPlan, GraphPlanError,
    GraphPlanResult, LogicalBinaryOperator, LogicalExpression, LogicalProjection, PhysicalBinaryOperator, PhysicalExpression,
    PhysicalPlanningContext, PhysicalProjection, ScalarValue, Symbol, ValueOrigin,
};
pub use hydradb_graph_slate::prefix_filter::graph_filter_policies;
pub use locality::{
    compare_locality_layouts, locality_cell_id, locality_cell_prefix, locality_cell_prefix_len, LocalityCellExtractor,
    LocalityLayoutExperiment, StorageLayout,
};
pub use offline_image::{OfflineGraphImageEncoder, OfflineImageRow};
pub use query::algebra::{
    CypherEngineMode, LogicalQueryPlan, MutationAtomicityPolicy, PhysicalQueryPlan, QueryBatchEdge, QueryBatchIsolatedVertex,
    QueryBatchMergePolicy, QueryBatchOperation, QueryBatchRelationship, QueryBatchRelationshipMerge, QueryBatchVertex,
    QueryCancellationToken, QueryColumn, QueryContext, QueryCursorToken, QueryMutationResult, QueryOutput, QueryParameterValue, QueryPath,
    QueryPathNode, QueryPathRelationship, QueryPlan, QueryPlanner, QueryResultPage, QueryResultSet, QueryRow, QueryStatement, QueryValue,
    QueryWindow, RowQueryAccess, RowQueryOptimizerPass, RowQueryPlan, RowQueryPlanGroup, RowQueryPlanPattern,
};
#[cfg(feature = "query-service-discovery")]
pub use query::coordination::{ConsulQueryServiceDiscovery, EtcdQueryServiceDiscovery, KubernetesQueryServiceDiscovery};
#[cfg(feature = "opencypher")]
pub use query::coordination::{
    DistributedQueryCoordinator, DistributedQueryJoin, DistributedQueryLeg, DistributedQueryMerge, DistributedQueryPageRequest,
    DistributedQueryPlan, DistributedQueryPlanResult, PreparedCypherEffect, PreparedCypherQuery, QueryCellClient,
};
#[cfg(feature = "query-transport")]
pub use query::coordination::{
    QueryServiceDirectory, QueryServiceDiscovery, QueryServiceEndpoint, QueryTransportAction, QueryTransportAuthPolicy,
    QueryTransportCancellationPrincipal, QueryTransportClientConfig, QueryTransportConnectionIdentity, QueryTransportMetricsSnapshot,
    QueryTransportNamespaceQuotas, QueryTransportPrincipal, QueryTransportScopeAuthorizer, QueryTransportScopeGrant, QueryTransportSecret,
    QueryTransportServerConfig, StaticQueryServiceDiscovery, StaticQueryTransportScopeAuthorizer, TcpQueryCellClient, TcpQueryRowStream,
    TcpQueryServer,
};
#[cfg(feature = "query-transport-tls")]
pub use query::coordination::{
    QueryTransportTlsClientConfigProvider, QueryTransportTlsServerConfigProvider, ReloadableQueryTransportTlsClientConfigProvider,
    ReloadableQueryTransportTlsServerConfigProvider, StaticQueryTransportTlsClientConfigProvider,
    StaticQueryTransportTlsServerConfigProvider,
};
#[cfg(feature = "opencypher")]
pub use query::corpus::{
    parse_opencypher_tck_corpus, parse_opencypher_tck_corpus_dir, CypherTckCase, CypherTckCompatibilityReport, CypherTckCorpus,
    CypherTckOutcome,
};
#[cfg(feature = "experimental-cypher-engine")]
pub use query::experimental_cypher::validate_experimental_cypher_preparation;
#[cfg(feature = "opencypher")]
pub use query::opencypher::{
    parse_opencypher_mutation_query_with_list_parameters, parse_opencypher_mutation_query_with_parameters, parse_opencypher_row_query,
    parse_opencypher_row_query_with_list_parameters, parse_opencypher_row_query_with_parameters, prepare_opencypher_query_with_parameters,
    validate_opencypher_syntax, EdgeDirection, ListParameters, MutationReturn, ParsedMutationQuery, ParsedRowQuery,
    PreparedOpenCypherQuery, RowAggregateFunction, RowComparisonOp, RowEdgePattern, RowExpression, RowMatchGroup, RowMutationAction,
    RowNodePattern, RowPattern, RowPredicate, RowProjection, RowSort, RowSortExpression,
};
#[cfg(feature = "datafusion-cypher-engine")]
pub use shard::datafusion_cypher::{new_slate_graph_provider, SlateGraphReadView};
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
        Self { query: query.to_string() }
    }
}

pub(crate) const GRAPH_TXN_MAX_RETRIES: usize = 32;
pub(crate) const GRAPH_MAINTENANCE_BATCH_KEYS: usize = 512;
pub(crate) const GRAPH_WRITE_LANES: usize = 64;

mod codec;
pub(crate) use codec::*;

mod keys;
mod shard;
