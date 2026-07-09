use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use slatedb::bytes::Bytes;
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
use slatedb::Db;
use slatedb::ErrorKind;
#[cfg(feature = "opencypher")]
use tokio::sync::watch;
use tokio::sync::{Mutex, Semaphore};
#[cfg(feature = "opencypher")]
use tokio::task::JoinHandle;

#[cfg(feature = "opencypher")]
use crate::query::opencypher::ParsedRowQuery;
use crate::{
    engine, graph_now_millis, parse_u64, sparse_kernel, validate_component, BoundedGraphCache,
    GraphCacheMetrics, GraphCachePolicy, GraphEpoch, GraphError, GraphIndexPolicy, GraphLimits,
    GraphOperationalMetrics, GraphRetentionPolicy, MatrixAdjacency, MatrixCacheKey,
    PostingChunkCacheKey, Result, SupernodeCacheKey, VertexId, GRAPH_CELL_WRITE_LOCK_TTL_MS,
};
#[cfg(feature = "opencypher")]
use crate::{
    ParsedRowQueryCacheKey, ReachabilityCacheKey, ReachabilityCacheValue,
    RelationshipPropertyRowsCacheKey, RelationshipRowsCacheKey, RelationshipRowsCacheValue,
    SourceRelationshipRowsCacheKey,
};

pub struct GraphShard {
    pub(crate) db: Db,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) store_path: Path,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) retention_policy: GraphRetentionPolicy,
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
            now_ms.saturating_add(GRAPH_CELL_WRITE_LOCK_TTL_MS),
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
        "graph-cell-write-lock-v2\ncell={cell_id}\noperation={operation}\nowner_token={owner_token}\ncreated_ms={created_ms}\nexpires_at_ms={expires_at_ms}\nstate={state}\n"
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
