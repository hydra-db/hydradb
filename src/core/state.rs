use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use slatedb::bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
use slatedb::ErrorKind;
#[cfg(feature = "opencypher")]
use slatedb::IsolationLevel;
use slatedb::{Db, DbReader};
#[cfg(feature = "opencypher")]
use tokio::sync::watch;
use tokio::sync::{Mutex, Semaphore};
#[cfg(feature = "opencypher")]
use tokio::task::JoinHandle;

#[cfg(feature = "opencypher")]
use crate::query::opencypher::ParsedRowQuery;
use crate::{
    engine, graph_now_millis, new_cell_write_lock_owner_token, parse_u64, sparse_kernel,
    validate_component, BoundedGraphCache, GraphCacheMetrics, GraphCachePolicy, GraphEpoch,
    GraphError, GraphIndexPolicy, GraphLimits, GraphOperationalMetrics, GraphRetentionPolicy,
    MatrixAdjacency, MatrixCacheKey, PostingChunkCacheKey, Result, SupernodeCacheKey, VertexId,
    GRAPH_CELL_WRITE_LOCK_BACKOFF_MS, GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS,
};
#[cfg(feature = "opencypher")]
use crate::{
    ParsedRowQueryCacheKey, ReachabilityCacheKey, ReachabilityCacheValue,
    RelationshipPropertyRowsCacheKey, RelationshipRowsCacheKey, RelationshipRowsCacheValue,
    SourceRelationshipRowsCacheKey,
};

pub struct GraphShard {
    pub(crate) db: GraphStore,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) store_path: Path,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) retention_policy: GraphRetentionPolicy,
    #[cfg(feature = "opencypher")]
    pub(crate) query_read_leases: Arc<QueryReadLeaseManager>,
    pub(crate) cache_metrics: Arc<GraphCacheMetrics>,
    pub(crate) operation_metrics: Arc<GraphOperationalMetrics>,
    pub(crate) hydration_gate: Arc<Semaphore>,
    pub(crate) graph_write_gate: Arc<Semaphore>,
    pub(crate) artifact_build_gate: Arc<Semaphore>,
    pub(crate) gc_gate: Arc<Semaphore>,
    pub(crate) index_policy: GraphIndexPolicy,
    pub(crate) await_durable_writes: bool,
    pub(crate) write_authority: GraphWriteAuthority,
    pub(crate) writer_lanes: Vec<Mutex<()>>,
    pub(crate) matrix_artifact_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, engine::MatrixArtifact>>,
    pub(crate) matrix_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<MatrixAdjacency>>>,
    pub(crate) graphblas_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, Arc<sparse_kernel::CompiledGraphBlasMatrix>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) parsed_row_query_cache:
        Mutex<BoundedGraphCache<ParsedRowQueryCacheKey, ParsedRowQuery>>,
    #[cfg(feature = "opencypher")]
    pub(crate) reachability_cache:
        Mutex<BoundedGraphCache<ReachabilityCacheKey, ReachabilityCacheValue>>,
    #[cfg(feature = "opencypher")]
    pub(crate) relationship_rows_cache:
        Mutex<BoundedGraphCache<RelationshipRowsCacheKey, RelationshipRowsCacheValue>>,
    #[cfg(feature = "opencypher")]
    pub(crate) source_relationship_rows_cache:
        Mutex<BoundedGraphCache<SourceRelationshipRowsCacheKey, Arc<Vec<VertexId>>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) relationship_property_rows_cache:
        Mutex<BoundedGraphCache<RelationshipPropertyRowsCacheKey, RelationshipRowsCacheValue>>,
    pub(crate) supernode_group_cache:
        Mutex<BoundedGraphCache<SupernodeCacheKey, engine::SupernodeGroup>>,
    pub(crate) posting_chunk_cache:
        Mutex<BoundedGraphCache<PostingChunkCacheKey, engine::PostingChunk>>,
    pub(crate) materialized_supernode_cache:
        Mutex<BoundedGraphCache<SupernodeCacheKey, Arc<Vec<VertexId>>>>,
}

#[derive(Clone)]
pub(crate) enum GraphStore {
    Writer(Db),
    Reader(Arc<DbReader>),
}

impl GraphStore {
    pub(crate) fn writer(&self) -> Result<&Db> {
        match self {
            Self::Writer(db) => Ok(db),
            Self::Reader(_) => Err(GraphError::ReadOnlyShardStorage),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn writer_clone(&self) -> Result<Db> {
        self.writer().cloned()
    }

    pub(crate) async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Writer(db) => db.get_with_options(key, options).await,
            Self::Reader(reader) => reader.get_with_options(key, options).await,
        }
    }

    pub(crate) async fn scan_prefix_with_options(
        &self,
        prefix: &[u8],
        start_suffix: Option<Vec<u8>>,
        options: &ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error> {
        match (self, start_suffix) {
            (Self::Writer(db), Some(start)) => {
                db.scan_prefix_with_options(prefix, start.., options).await
            }
            (Self::Writer(db), None) => db.scan_prefix_with_options(prefix, .., options).await,
            (Self::Reader(reader), Some(start)) => {
                reader
                    .scan_prefix_with_options(prefix, start.., options)
                    .await
            }
            (Self::Reader(reader), None) => {
                reader.scan_prefix_with_options(prefix, .., options).await
            }
        }
    }

    pub(crate) async fn close(&self) -> std::result::Result<(), slatedb::Error> {
        match self {
            Self::Writer(db) => db.close().await,
            Self::Reader(reader) => reader.close().await,
        }
    }
}

#[cfg(feature = "opencypher")]
pub struct QueryStatsRefreshHandle {
    pub(crate) stop_tx: Option<watch::Sender<bool>>,
    pub(crate) handle: Option<JoinHandle<Result<()>>>,
}

#[cfg(feature = "opencypher")]
impl QueryStatsRefreshHandle {
    pub fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    pub async fn stop(mut self) -> Result<()> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(true);
        }
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        match handle.await {
            Ok(result) => result,
            Err(err) => Err(GraphError::CorruptValue {
                key: "query/stats_refresh_job".to_string(),
                reason: format!("query stats refresh task failed: {err}"),
            }),
        }
    }
}

#[cfg(feature = "opencypher")]
impl Drop for QueryStatsRefreshHandle {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheEntryCounts {
    pub matrix_artifacts: usize,
    pub matrix_adjacencies: usize,
    pub graphblas_matrices: usize,
    #[cfg(feature = "opencypher")]
    pub parsed_row_queries: usize,
    #[cfg(feature = "opencypher")]
    pub reachability_results: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_row_sets: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_property_row_sets: usize,
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
        leases: Arc<RwLock<BTreeMap<String, engine::ShardLease>>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphWriteFence {
    pub(crate) cell_id: String,
    pub(crate) owner_node_id: String,
    pub(crate) lease_token: u64,
    pub(crate) expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphReadLease {
    pub(crate) cell_id: String,
    pub(crate) lease_id: String,
    pub(crate) read_epoch: GraphEpoch,
    pub(crate) expires_at_ms: u64,
}

#[cfg(feature = "opencypher")]
#[derive(Default)]
struct QueryReadLeaseCellState {
    active_epochs: BTreeMap<GraphEpoch, u64>,
    persisted_epoch: Option<GraphEpoch>,
    persisted_expires_at_ms: u64,
}

#[cfg(feature = "opencypher")]
pub(crate) struct QueryReadLeaseManager {
    db: Option<Db>,
    lease_id: String,
    ttl_ms: u64,
    cells: Mutex<BTreeMap<String, QueryReadLeaseCellState>>,
    metrics: Arc<GraphOperationalMetrics>,
}

#[cfg(feature = "opencypher")]
impl QueryReadLeaseManager {
    pub(crate) fn new(
        db: Option<Db>,
        ttl_ms: u64,
        metrics: Arc<GraphOperationalMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            lease_id: format!(
                "query-{:020}",
                crate::GRAPH_READ_LEASE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ),
            ttl_ms,
            cells: Mutex::new(BTreeMap::new()),
            metrics,
        })
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        cell_id: &str,
        read_epoch: GraphEpoch,
        max_runtime_ms: Option<u64>,
    ) -> Result<Arc<QueryReadLeaseRegistration>> {
        if self.ttl_ms == 0 {
            return Ok(Arc::new(QueryReadLeaseRegistration {
                manager: None,
                cell_id: cell_id.to_string(),
                read_epoch,
            }));
        }

        let Some(db) = &self.db else {
            return Err(GraphError::ReadLeaseCoordinatorUnavailable {
                cell_id: cell_id.to_string(),
            });
        };

        let now_ms = graph_now_millis();
        let hold_ms = self
            .ttl_ms
            .max(max_runtime_ms.unwrap_or(0).saturating_add(5_000));
        let required_expiry = now_ms.saturating_add(hold_ms);
        let published_expiry = required_expiry.saturating_add(self.ttl_ms);
        let mut cells = self.cells.lock().await;
        let state = cells.entry(cell_id.to_string()).or_default();
        *state.active_epochs.entry(read_epoch).or_default() += 1;
        let min_epoch = state
            .active_epochs
            .keys()
            .next()
            .copied()
            .expect("the acquired epoch is active");
        let lease_covers_query = state
            .persisted_epoch
            .is_some_and(|persisted| persisted <= min_epoch)
            && state.persisted_expires_at_ms >= required_expiry;
        if !lease_covers_query {
            let lease = GraphReadLease {
                cell_id: cell_id.to_string(),
                lease_id: self.lease_id.clone(),
                read_epoch: min_epoch,
                expires_at_ms: published_expiry,
            };
            let publish = async {
                let txn = db.begin(IsolationLevel::SerializableSnapshot).await?;
                let drop_marker = crate::keys::cell_drop_marker(cell_id);
                let pending_drop_marker = crate::keys::cell_drop_pending_marker(cell_id);
                if crate::read_txn_remote(&txn, &drop_marker).await?.is_some()
                    || crate::read_txn_remote(&txn, &pending_drop_marker)
                        .await?
                        .is_some()
                {
                    return Err(GraphError::CellDropped {
                        operation: "pin_query_read",
                        cell_id: cell_id.to_string(),
                    });
                }
                txn.put(
                    crate::keys::read_lease(cell_id, &self.lease_id).as_bytes(),
                    crate::encode_read_lease(&lease),
                )?;
                crate::commit_txn_strict(txn, true).await
            }
            .await;
            if let Err(err) = publish {
                decrement_active_read_epoch(state, read_epoch);
                return Err(err);
            }
            state.persisted_epoch = Some(min_epoch);
            state.persisted_expires_at_ms = published_expiry;
            self.metrics
                .read_leases_created
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        drop(cells);

        Ok(Arc::new(QueryReadLeaseRegistration {
            manager: Some(Arc::clone(self)),
            cell_id: cell_id.to_string(),
            read_epoch,
        }))
    }

    async fn release(&self, cell_id: &str, read_epoch: GraphEpoch) {
        let mut cells = self.cells.lock().await;
        if let Some(state) = cells.get_mut(cell_id) {
            decrement_active_read_epoch(state, read_epoch);
        }
    }
}

#[cfg(feature = "opencypher")]
fn decrement_active_read_epoch(state: &mut QueryReadLeaseCellState, read_epoch: GraphEpoch) {
    let Some(count) = state.active_epochs.get_mut(&read_epoch) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        state.active_epochs.remove(&read_epoch);
    }
}

#[cfg(feature = "opencypher")]
pub(crate) struct QueryReadLeaseRegistration {
    manager: Option<Arc<QueryReadLeaseManager>>,
    cell_id: String,
    read_epoch: GraphEpoch,
}

#[cfg(feature = "opencypher")]
impl std::fmt::Debug for QueryReadLeaseRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryReadLeaseRegistration")
            .field("cell_id", &self.cell_id)
            .field("read_epoch", &self.read_epoch)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "opencypher")]
impl PartialEq for QueryReadLeaseRegistration {
    fn eq(&self, other: &Self) -> bool {
        self.cell_id == other.cell_id
            && self.read_epoch == other.read_epoch
            && match (&self.manager, &other.manager) {
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            }
    }
}

#[cfg(feature = "opencypher")]
impl Eq for QueryReadLeaseRegistration {}

#[cfg(feature = "opencypher")]
impl Drop for QueryReadLeaseRegistration {
    fn drop(&mut self) {
        let Some(manager) = self.manager.take() else {
            return;
        };
        let cell_id = self.cell_id.clone();
        let read_epoch = self.read_epoch;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                manager.release(&cell_id, read_epoch).await;
            });
        }
    }
}

impl From<&engine::ShardLease> for GraphWriteFence {
    fn from(lease: &engine::ShardLease) -> Self {
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

pub(crate) struct CellWriteLock {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) owner_token: String,
    pub(crate) ttl_ms: u64,
}

impl CellWriteLock {
    pub(crate) async fn renew(&self) -> Result<()> {
        let current = self.object_store.get(&self.path).await?;
        let version = UpdateVersion {
            e_tag: current.meta.e_tag.clone(),
            version: current.meta.version.clone(),
        };
        let value = current.bytes().await?;
        let record = decode_cell_write_lock_record(self.path.as_ref(), &value)?;
        if record.owner_token != self.owner_token || record.state != CellWriteLockState::Active {
            return Err(GraphError::CellWriteConflict {
                operation: "renew_cell_write_lock",
                cell_id: record.cell_id,
            });
        }
        let now_ms = graph_now_millis();
        let payload = encode_cell_write_lock_record(
            &record.cell_id,
            &record.operation,
            &self.owner_token,
            record.created_ms,
            now_ms.saturating_add(self.ttl_ms),
            CellWriteLockState::Active,
        );
        match self
            .object_store
            .put_opts(&self.path, payload.into(), PutMode::Update(version).into())
            .await
        {
            Ok(_) => Ok(()),
            Err(slatedb::object_store::Error::Precondition { .. })
            | Err(slatedb::object_store::Error::NotFound { .. }) => {
                Err(GraphError::CellWriteConflict {
                    operation: "renew_cell_write_lock",
                    cell_id: record.cell_id,
                })
            }
            Err(slatedb::object_store::Error::NotImplemented { .. })
            | Err(slatedb::object_store::Error::NotSupported { .. }) => {
                // Some local object-store backends do not support conditional update. We already
                // verified that this owner still holds an active lock; those backends also cannot
                // safely CAS-reclaim the lock, so accepting the renewal preserves exclusivity.
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn release(self) -> Result<()> {
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

pub(crate) async fn acquire_distributed_write_lock(
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    scope_id: &str,
    operation: &'static str,
    ttl_ms: u64,
) -> Result<CellWriteLock> {
    validate_component("lock_scope", scope_id)?;
    if ttl_ms == 0 {
        return Err(GraphError::CorruptValue {
            key: path.to_string(),
            reason: "distributed write lock TTL must be greater than zero".to_string(),
        });
    }
    let owner_token = new_cell_write_lock_owner_token();
    for attempt in 0..GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS {
        let now_ms = graph_now_millis();
        let payload = encode_cell_write_lock_record(
            scope_id,
            operation,
            &owner_token,
            now_ms,
            now_ms.saturating_add(ttl_ms),
            CellWriteLockState::Active,
        );
        match object_store
            .put_opts(&path, payload.clone().into(), PutMode::Create.into())
            .await
        {
            Ok(_) => {
                return Ok(CellWriteLock {
                    object_store,
                    path,
                    owner_token,
                    ttl_ms,
                });
            }
            Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
                if let Some(lock) = try_reclaim_distributed_write_lock(
                    Arc::clone(&object_store),
                    &path,
                    scope_id,
                    operation,
                    &owner_token,
                    ttl_ms,
                )
                .await?
                {
                    return Ok(lock);
                }
                if attempt + 1 < GRAPH_CELL_WRITE_LOCK_MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        GRAPH_CELL_WRITE_LOCK_BACKOFF_MS,
                    ))
                    .await;
                    continue;
                }
                return Err(GraphError::CellWriteConflict {
                    operation,
                    cell_id: scope_id.to_string(),
                });
            }
            Err(err) => return Err(err.into()),
        }
    }
    Err(GraphError::CellWriteConflict {
        operation,
        cell_id: scope_id.to_string(),
    })
}

async fn try_reclaim_distributed_write_lock(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
    scope_id: &str,
    operation: &'static str,
    owner_token: &str,
    ttl_ms: u64,
) -> Result<Option<CellWriteLock>> {
    let current = match object_store.get(path).await {
        Ok(current) => current,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let version = UpdateVersion {
        e_tag: current.meta.e_tag.clone(),
        version: current.meta.version.clone(),
    };
    let value = current.bytes().await?;
    let record = decode_cell_write_lock_record(path.as_ref(), &value)?;
    if record.cell_id != scope_id {
        return Err(GraphError::CorruptValue {
            key: path.to_string(),
            reason: format!(
                "distributed write lock belongs to scope {}, expected {scope_id}",
                record.cell_id
            ),
        });
    }
    let now_ms = graph_now_millis();
    if !record.is_expired(now_ms) {
        return Ok(None);
    }
    let payload = encode_cell_write_lock_record(
        scope_id,
        operation,
        owner_token,
        now_ms,
        now_ms.saturating_add(ttl_ms),
        CellWriteLockState::Active,
    );
    match object_store
        .put_opts(path, payload.into(), PutMode::Update(version).into())
        .await
    {
        Ok(_) => Ok(Some(CellWriteLock {
            object_store,
            path: path.clone(),
            owner_token: owner_token.to_string(),
            ttl_ms,
        })),
        Err(slatedb::object_store::Error::Precondition { .. })
        | Err(slatedb::object_store::Error::NotFound { .. })
        | Err(slatedb::object_store::Error::NotImplemented { .. })
        | Err(slatedb::object_store::Error::NotSupported { .. }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CellWriteLockState {
    Active,
    Released,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CellWriteLockRecord {
    pub(crate) cell_id: String,
    pub(crate) operation: String,
    pub(crate) owner_token: String,
    pub(crate) created_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) state: CellWriteLockState,
}

impl CellWriteLockRecord {
    pub(crate) fn is_expired(&self, now_ms: u64) -> bool {
        self.state == CellWriteLockState::Released || self.expires_at_ms <= now_ms
    }
}

pub(crate) fn encode_cell_write_lock_record(
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
        "graph-cell-write-lock-v1\ncell={cell_id}\noperation={operation}\nowner_token={owner_token}\ncreated_ms={created_ms}\nexpires_at_ms={expires_at_ms}\nstate={state}\n"
    ))
}

pub(crate) fn decode_cell_write_lock_record(
    key: &str,
    value: &[u8],
) -> Result<CellWriteLockRecord> {
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

pub(crate) async fn release_cell_write_lock<T>(
    lock: CellWriteLock,
    result: Result<T>,
) -> Result<T> {
    match (result, lock.release().await) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), _) => Err(err),
    }
}

pub(crate) fn is_retryable_write_conflict(err: &GraphError) -> bool {
    matches!(err, GraphError::Slate(err) if err.kind() == ErrorKind::Transaction)
        || matches!(err, GraphError::CellWriteConflict { .. })
}
