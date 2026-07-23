use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};

use slatedb::bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::object_store::{memory::InMemory, path::Path, ObjectStore};
use slatedb::ErrorKind;
use slatedb::{Db, DbReader, DbReaderSnapshot, DbSnapshot};
#[cfg(feature = "opencypher")]
use tokio::sync::watch;
use tokio::sync::{Mutex, OnceCell, OwnedMutexGuard, RwLock as AsyncRwLock, Semaphore};
#[cfg(feature = "opencypher")]
use tokio::task::JoinHandle;

#[cfg(feature = "opencypher")]
use crate::query::opencypher::ParsedRowQuery;
use crate::{
    engine, graph_now_millis, open_graph_db, open_graph_reader, sparse_kernel, BoundedGraphCache,
    GraphCacheConfig, GraphCacheMetrics, GraphCachePolicy, GraphDurabilityConfig, GraphError,
    GraphIndexPolicy, GraphLimits, GraphOperationalMetrics, GraphStorageMemoryConfig,
    MatrixAdjacency, MatrixCacheKey, Result,
};

tokio::task_local! {
    static ACTIVE_STORAGE_SNAPSHOT: Arc<GraphStorageSnapshot>;
}
#[cfg(feature = "opencypher")]
use crate::{
    ParsedRowQueryCacheKey, RelationshipPropertyRowsCacheKey, RelationshipRowsCacheKey,
    RelationshipRowsCacheValue, SourceRelationshipRowsCacheKey, VertexId,
};

pub struct GraphShard {
    pub(crate) db: GraphStore,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) cache_metrics: Arc<GraphCacheMetrics>,
    pub(crate) operation_metrics: Arc<GraphOperationalMetrics>,
    pub(crate) hydration_gate: Arc<Semaphore>,
    #[cfg(feature = "graphblas")]
    pub(crate) matrix_compilation_gate: Arc<Semaphore>,
    pub(crate) graph_write_gate: Arc<Semaphore>,
    pub(crate) artifact_build_gate: Arc<Semaphore>,
    pub(crate) gc_gate: Arc<Semaphore>,
    pub(crate) index_policy: GraphIndexPolicy,
    pub(crate) await_durable_writes: bool,
    pub(crate) write_authority: GraphWriteAuthority,
    pub(crate) local_write_guard: Arc<Mutex<()>>,
    pub(crate) local_artifact_guard: Arc<Mutex<()>>,
    pub(crate) writer_lanes: Vec<Mutex<()>>,
    pub(crate) matrix_artifact_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, engine::MatrixArtifact>>,
    pub(crate) graph_index_generations:
        Mutex<std::collections::BTreeMap<MatrixCacheKey, engine::GraphIndexGeneration>>,
    pub(crate) matrix_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<MatrixAdjacency>>>,
    pub(crate) graphblas_cache:
        Mutex<BoundedGraphCache<MatrixCacheKey, Arc<sparse_kernel::CompiledGraphBlasMatrix>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) parsed_row_query_cache:
        Mutex<BoundedGraphCache<ParsedRowQueryCacheKey, ParsedRowQuery>>,
    #[cfg(feature = "opencypher")]
    pub(crate) relationship_rows_cache:
        Mutex<BoundedGraphCache<RelationshipRowsCacheKey, RelationshipRowsCacheValue>>,
    #[cfg(feature = "opencypher")]
    pub(crate) source_relationship_rows_cache:
        Mutex<BoundedGraphCache<SourceRelationshipRowsCacheKey, Arc<Vec<VertexId>>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) relationship_property_rows_cache:
        Mutex<BoundedGraphCache<RelationshipPropertyRowsCacheKey, RelationshipRowsCacheValue>>,
}

#[derive(Clone)]
pub(crate) struct GraphStore {
    inner: Arc<GraphStoreInner>,
}

struct GraphStoreInner {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    cache: GraphCacheConfig,
    storage_memory: GraphStorageMemoryConfig,
    durability: GraphDurabilityConfig,
    writer: StdRwLock<Option<Db>>,
    reader: AsyncRwLock<Option<Arc<DbReader>>>,
    missing_reader_probe_ms: AtomicU64,
    writer_open_gate: Mutex<()>,
    reader_open_gate: Mutex<()>,
}

const MISSING_READER_RECHECK_MS: u64 = 10_000;
static EMPTY_GRAPH_STORE: OnceCell<Db> = OnceCell::const_new();

pub(crate) enum GraphStorageSnapshot {
    Writer(Arc<DbSnapshot>),
    Reader(Arc<DbReaderSnapshot>),
    Empty(Arc<DbSnapshot>),
}

impl GraphStorageSnapshot {
    pub(crate) fn seq(&self) -> u64 {
        match self {
            Self::Writer(snapshot) | Self::Empty(snapshot) => snapshot.seq(),
            Self::Reader(snapshot) => snapshot.seq(),
        }
    }

    pub(crate) fn last_wal_id(&self) -> Option<u64> {
        match self {
            Self::Writer(_) => None,
            Self::Reader(snapshot) => Some(snapshot.last_wal_id()),
            Self::Empty(_) => Some(0),
        }
    }

    pub(crate) async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Writer(snapshot) | Self::Empty(snapshot) => {
                snapshot.get_with_options(key, options).await
            }
            Self::Reader(snapshot) => snapshot.get_with_options(key, options).await,
        }
    }

    pub(crate) async fn scan_prefix_with_options<T>(
        &self,
        prefix: &[u8],
        subrange: T,
        options: &ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error>
    where
        T: slatedb::ByteRangeBounds + Send,
    {
        match self {
            Self::Writer(snapshot) | Self::Empty(snapshot) => {
                snapshot
                    .scan_prefix_with_options(prefix, subrange, options)
                    .await
            }
            Self::Reader(snapshot) => {
                snapshot
                    .scan_prefix_with_options(prefix, subrange, options)
                    .await
            }
        }
    }
}

impl GraphStore {
    pub(crate) fn lazy(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        cache: GraphCacheConfig,
        storage_memory: GraphStorageMemoryConfig,
        durability: GraphDurabilityConfig,
    ) -> Self {
        Self {
            inner: Arc::new(GraphStoreInner {
                path,
                object_store,
                cache,
                storage_memory,
                durability,
                writer: StdRwLock::new(None),
                reader: AsyncRwLock::new(None),
                missing_reader_probe_ms: AtomicU64::new(0),
                writer_open_gate: Mutex::new(()),
                reader_open_gate: Mutex::new(()),
            }),
        }
    }

    fn open_writer(&self) -> Option<Db> {
        self.inner
            .writer
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn store_path(&self) -> &Path {
        &self.inner.path
    }

    pub(crate) fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.inner.object_store
    }

    pub(crate) fn writer(&self) -> Result<Db> {
        self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)
    }

    pub(crate) async fn refresh_writer_fence(&self) -> Result<()> {
        let _open_guard = self.inner.writer_open_gate.lock().await;
        let writer = self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)?;
        match writer.refresh_manifest().await {
            Ok(()) => Ok(()),
            Err(err) if matches!(err.kind(), ErrorKind::Closed(slatedb::CloseReason::Fenced)) => {
                *self
                    .inner
                    .writer
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                Err(err.into())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn promote_writer(&self) -> Result<bool> {
        if self.open_writer().is_some() {
            return Ok(false);
        }
        let _open_guard = self.inner.writer_open_gate.lock().await;
        if self.open_writer().is_some() {
            return Ok(false);
        }
        let writer = open_graph_db(
            self.inner.path.clone(),
            Arc::clone(&self.inner.object_store),
            &self.inner.cache,
            &self.inner.storage_memory,
            &self.inner.durability,
        )
        .await?;
        *self
            .inner
            .writer
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(writer);
        Ok(true)
    }

    async fn empty_store() -> Result<Db> {
        EMPTY_GRAPH_STORE
            .get_or_try_init(|| async {
                Db::builder(Path::from("empty"), Arc::new(InMemory::new()))
                    .build()
                    .await
                    .map_err(GraphError::from)
            })
            .await
            .cloned()
    }

    pub(crate) async fn open_reader(&self) -> Result<Option<Arc<DbReader>>> {
        self.open_reader_inner(false).await
    }

    async fn open_reader_inner(&self, force_missing_probe: bool) -> Result<Option<Arc<DbReader>>> {
        if let Some(reader) = self.inner.reader.read().await.as_ref().cloned() {
            return Ok(Some(reader));
        }
        let last_probe = self.inner.missing_reader_probe_ms.load(Ordering::Relaxed);
        if !force_missing_probe
            && last_probe != 0
            && graph_now_millis().saturating_sub(last_probe) < MISSING_READER_RECHECK_MS
        {
            return Ok(None);
        }
        let _open_guard = self.inner.reader_open_gate.lock().await;
        if let Some(reader) = self.inner.reader.read().await.as_ref().cloned() {
            return Ok(Some(reader));
        }
        match open_graph_reader(
            self.inner.path.clone(),
            Arc::clone(&self.inner.object_store),
            &self.inner.cache,
        )
        .await
        {
            Ok(reader) => {
                let reader = Arc::new(reader);
                *self.inner.reader.write().await = Some(Arc::clone(&reader));
                self.inner
                    .missing_reader_probe_ms
                    .store(0, Ordering::Relaxed);
                Ok(Some(reader))
            }
            Err(GraphError::Slate(error)) if matches!(error.kind(), ErrorKind::DatabaseMissing) => {
                self.inner
                    .missing_reader_probe_ms
                    .store(graph_now_millis().max(1), Ordering::Relaxed);
                Self::empty_store().await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>> {
        if let Ok(snapshot) = ACTIVE_STORAGE_SNAPSHOT.try_with(Arc::clone) {
            return Ok(snapshot.get_with_options(key, options).await?);
        }
        if let Some(writer) = self.open_writer() {
            return Ok(writer.get_with_options(key, options).await?);
        }
        if let Some(reader) = self.open_reader().await? {
            return Ok(reader.get_with_options(key, options).await?);
        }
        Ok(Self::empty_store()
            .await?
            .get_with_options(key, options)
            .await?)
    }

    pub(crate) async fn scan_prefix_with_options(
        &self,
        prefix: &[u8],
        start_suffix: Option<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<slatedb::DbIterator> {
        if let Ok(snapshot) = ACTIVE_STORAGE_SNAPSHOT.try_with(Arc::clone) {
            return Ok(match start_suffix {
                Some(start) => {
                    snapshot
                        .scan_prefix_with_options(prefix, start.., options)
                        .await?
                }
                None => {
                    snapshot
                        .scan_prefix_with_options(prefix, .., options)
                        .await?
                }
            });
        }
        if let Some(writer) = self.open_writer() {
            return Ok(match start_suffix {
                Some(start) => {
                    writer
                        .scan_prefix_with_options(prefix, start.., options)
                        .await?
                }
                None => writer.scan_prefix_with_options(prefix, .., options).await?,
            });
        }
        if let Some(reader) = self.open_reader().await? {
            return Ok(match start_suffix {
                Some(start) => {
                    reader
                        .scan_prefix_with_options(prefix, start.., options)
                        .await?
                }
                None => reader.scan_prefix_with_options(prefix, .., options).await?,
            });
        }
        let empty = Self::empty_store().await?;
        Ok(match start_suffix {
            Some(start) => {
                empty
                    .scan_prefix_with_options(prefix, start.., options)
                    .await?
            }
            None => empty.scan_prefix_with_options(prefix, .., options).await?,
        })
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let reader = self.inner.reader.read().await.as_ref().cloned();
        if let Some(reader) = reader {
            reader.close().await?;
        }
        if let Some(writer) = self.open_writer() {
            writer.close().await?;
        }
        Ok(())
    }

    pub(crate) async fn snapshot(&self) -> Result<Arc<GraphStorageSnapshot>> {
        if let Ok(snapshot) = ACTIVE_STORAGE_SNAPSHOT.try_with(Arc::clone) {
            return Ok(snapshot);
        }
        if let Some(writer) = self.open_writer() {
            return writer
                .snapshot()
                .await
                .map(|snapshot| Arc::new(GraphStorageSnapshot::Writer(snapshot)))
                .map_err(Into::into);
        }
        if let Some(reader) = self.open_reader().await? {
            return reader
                .snapshot()
                .await
                .map(|snapshot| Arc::new(GraphStorageSnapshot::Reader(snapshot)))
                .map_err(Into::into);
        }
        Self::empty_store()
            .await?
            .snapshot()
            .await
            .map(|snapshot| Arc::new(GraphStorageSnapshot::Empty(snapshot)))
            .map_err(Into::into)
    }

    pub(crate) async fn reader_snapshot(&self) -> Result<Arc<GraphStorageSnapshot>> {
        if let Some(reader) = self.open_reader().await? {
            return reader
                .snapshot()
                .await
                .map(|snapshot| Arc::new(GraphStorageSnapshot::Reader(snapshot)))
                .map_err(Into::into);
        }
        Self::empty_store()
            .await?
            .snapshot()
            .await
            .map(|snapshot| Arc::new(GraphStorageSnapshot::Empty(snapshot)))
            .map_err(Into::into)
    }

    pub(crate) async fn durable_sequence(&self) -> Result<u64> {
        if let Some(writer) = self.open_writer() {
            return Ok(writer.status().durable_seq);
        }
        Ok(self
            .open_reader()
            .await?
            .map(|reader| reader.status().durable_seq)
            .unwrap_or(0))
    }

    pub(crate) async fn refresh_durable_reader(&self) -> Result<u64> {
        let Some(reader) = self.open_reader_inner(true).await? else {
            return Ok(0);
        };
        reader.refresh().await?;
        Ok(reader.status().durable_seq)
    }

    #[cfg(feature = "graphblas")]
    pub(crate) async fn last_durable_wal_id(&self) -> Result<u64> {
        let snapshot = self.snapshot().await?;
        if let Some(last_wal_id) = snapshot.last_wal_id() {
            return Ok(last_wal_id);
        }
        Ok(self.writer()?.last_flushed_wal_id())
    }

    #[cfg(feature = "graphblas")]
    pub(crate) fn wal_reader(&self) -> slatedb::WalReader {
        slatedb::WalReader::new(
            self.inner.path.clone(),
            Arc::clone(&self.inner.object_store),
        )
    }

    pub(crate) async fn scope_snapshot<F>(
        snapshot: Arc<GraphStorageSnapshot>,
        future: F,
    ) -> F::Output
    where
        F: std::future::Future,
    {
        ACTIVE_STORAGE_SNAPSHOT.scope(snapshot, future).await
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
    pub relationship_row_sets: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_property_row_sets: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheResidentBytes {
    pub matrix_adjacencies: usize,
    pub graphblas_matrices: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_rows: usize,
    #[cfg(feature = "opencypher")]
    pub source_relationship_rows: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_property_rows: usize,
}

impl GraphCacheResidentBytes {
    pub fn total(&self) -> usize {
        self.matrix_adjacencies
            .saturating_add(self.graphblas_matrices)
            .saturating_add({
                #[cfg(feature = "opencypher")]
                {
                    self.relationship_rows
                        .saturating_add(self.source_relationship_rows)
                        .saturating_add(self.relationship_property_rows)
                }
                #[cfg(not(feature = "opencypher"))]
                {
                    0
                }
            })
    }
}

#[derive(Clone)]
pub(crate) enum GraphWriteAuthority {
    ReadOnly,
    Promotable,
    Writer,
}

#[derive(Clone, Debug)]
pub(crate) enum GraphWriteOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
}

static LOCAL_WRITE_GUARD_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(crate) struct LocalWriteGuard {
    pub(crate) token: String,
    _guard: OwnedMutexGuard<()>,
}

impl LocalWriteGuard {
    pub(crate) fn new(guard: OwnedMutexGuard<()>) -> Self {
        let counter = LOCAL_WRITE_GUARD_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            token: format!("{}-{}-{counter}", graph_now_millis(), std::process::id()),
            _guard: guard,
        }
    }

    pub(crate) async fn renew(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn release(self) -> Result<()> {
        Ok(())
    }
}

pub(crate) async fn finish_local_write<T>(guard: LocalWriteGuard, result: Result<T>) -> Result<T> {
    guard.release().await?;
    result
}

pub(crate) fn is_retryable_write_conflict(err: &GraphError) -> bool {
    matches!(err, GraphError::Slate(err) if matches!(err.kind(), ErrorKind::Transaction | ErrorKind::Invalid))
}
