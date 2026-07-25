use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

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
    // A writer loss is acknowledged only by a reader refresh from the same or a later generation.
    reader_refresh_generation: AtomicU64,
    reader_refreshed_generation: AtomicU64,
    writer_open_gate: Mutex<()>,
    reader_open_gate: Mutex<()>,
    reader_refresh_gate: Mutex<()>,
    // How long a fenced writer waits before re-promoting: exactly one heartbeat
    // interval, so the rival has published a fresh view and stood down.
    heartbeat_interval: Duration,
}

static EMPTY_GRAPH_STORE: OnceCell<Db> = OnceCell::const_new();

/// The heartbeat interval a fenced writer waits out, until the node config
/// carries the configured value here. Decision 5 of the rendezvous placement
/// plan fixes the default at 5s and validates `interval < timeout` at startup.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// The floor of the retry ladder, and the value a fence resets it to.
const FENCE_BACKOFF_FLOOR: Duration = Duration::from_secs(1);

/// The ceiling of the retry ladder. Doubling is applied *before* the wait, so
/// the first plain failure sleeps 2s and the sixth reaches this.
const MAX_FENCE_BACKOFF: Duration = Duration::from_secs(60);

/// How many times a single `refresh_writer_fence` call may re-attempt before it
/// gives the failure back to the caller. The write that triggered the refresh is
/// blocked for the whole budget, so it is small: one fence wait plus two rungs
/// of the ladder, ~11s worst case, is long enough for view skew to converge and
/// short enough that a genuinely broken store still surfaces to the client.
const WRITER_REFRESH_ATTEMPTS: u32 = 4;

/// What one attempt at refreshing the writer produced. A fence is kept distinct
/// from a plain failure because the two earn different waits: view skew
/// converges on the heartbeat clock, a failing store on the exponential ladder.
enum FenceAttempt {
    Refreshed,
    Fenced(GraphError),
    Failed(GraphError),
}

/// The retry loop itself, generic over the attempt body so the backoff policy is
/// unit-testable without a real SlateDB writer. Ported from sleet's
/// `supervise_with` (`../sleet/src/daemon.rs:436-485`), which is the proven
/// version of exactly this policy.
///
/// Returns the last error once `max_attempts` is spent; there is no sleep after
/// the final attempt, since nobody is waiting on it.
async fn retry_with_fence_backoff<F, Fut>(
    mut attempt: F,
    heartbeat_interval: Duration,
    max_attempts: u32,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = FenceAttempt>,
{
    let mut backoff = FENCE_BACKOFF_FLOOR;
    let mut remaining = max_attempts.max(1);
    loop {
        let (delay, err) = match attempt().await {
            FenceAttempt::Refreshed => return Ok(()),
            FenceAttempt::Fenced(err) => {
                // A fence is view skew, not a failure: it says another node
                // believes it owns this cell, and one heartbeat interval is what
                // that belief takes to be re-derived. Riding the ladder here
                // would make a node that is merely converging look dead.
                backoff = FENCE_BACKOFF_FLOOR;
                (heartbeat_interval, err)
            }
            FenceAttempt::Failed(err) => {
                backoff = (backoff * 2).min(MAX_FENCE_BACKOFF);
                (backoff, err)
            }
        };
        remaining -= 1;
        if remaining == 0 {
            return Err(err);
        }
        tokio::time::sleep(delay).await;
    }
}

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
                reader_refresh_generation: AtomicU64::new(0),
                reader_refreshed_generation: AtomicU64::new(0),
                writer_open_gate: Mutex::new(()),
                reader_open_gate: Mutex::new(()),
                reader_refresh_gate: Mutex::new(()),
                heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
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

    fn readable_writer(&self) -> Option<Db> {
        let writer = self.open_writer()?;
        if writer.status().close_reason.is_none() {
            return Some(writer);
        }
        self.clear_closed_writer();
        None
    }

    fn clear_closed_writer(&self) -> bool {
        let mut writer = self
            .inner
            .writer
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if writer
            .as_ref()
            .is_some_and(|current| current.status().close_reason.is_some())
        {
            *writer = None;
            self.inner
                .reader_refresh_generation
                .fetch_add(1, Ordering::AcqRel);
            return true;
        }
        false
    }

    fn recover_closed_writer_error(&self, error: &slatedb::Error) -> bool {
        if !matches!(error.kind(), ErrorKind::Closed(_)) {
            return false;
        }
        self.clear_closed_writer();
        true
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

    /// Refresh the writer's manifest, which is where a fence surfaces: another
    /// node has taken the epoch while this one still holds an open handle.
    ///
    /// Dropping the fenced handle and returning is not enough — the next write
    /// simply reopens the writer, re-fences the rival, and the two trade the
    /// epoch forever. So the fence path drops the handle, waits exactly one
    /// heartbeat interval (long enough for the rival to refresh its view and
    /// stand down), re-promotes, and resets the backoff ladder to its floor: a
    /// fence is view skew, not a failure, and a node that is merely converging
    /// must not look dead. The retry is unconditional — ownership is enforced by
    /// the caller, not here, so this node may re-fence the winner once more
    /// before the two views agree. All three rules are sleet's
    /// (`../sleet/src/daemon.rs:458-472`), matched deliberately.
    pub(crate) async fn refresh_writer_fence(&self) -> Result<()> {
        let _open_guard = self.inner.writer_open_gate.lock().await;
        // No writer at all is a read-only shard, not a fenced one: waiting
        // promotes nothing, so fail before spending any of the budget.
        self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)?;
        retry_with_fence_backoff(
            move || async move {
                let writer = match self.open_writer() {
                    Some(writer) => writer,
                    // A previous attempt's fence cleared the handle; re-promote
                    // under the gate we already hold.
                    None => match self.install_writer().await {
                        Ok(writer) => writer,
                        Err(err) => return FenceAttempt::Failed(err),
                    },
                };
                match writer.refresh_manifest().await {
                    Ok(()) => FenceAttempt::Refreshed,
                    Err(err)
                        if matches!(
                            err.kind(),
                            ErrorKind::Closed(slatedb::CloseReason::Fenced)
                        ) =>
                    {
                        self.clear_closed_writer();
                        FenceAttempt::Fenced(err.into())
                    }
                    Err(err) => FenceAttempt::Failed(err.into()),
                }
            },
            self.inner.heartbeat_interval,
            WRITER_REFRESH_ATTEMPTS,
        )
        .await
    }

    pub(crate) async fn promote_writer(&self) -> Result<bool> {
        if self.open_writer().is_some() {
            return Ok(false);
        }
        let _open_guard = self.inner.writer_open_gate.lock().await;
        if self.open_writer().is_some() {
            return Ok(false);
        }
        self.install_writer().await?;
        Ok(true)
    }

    /// Open a fresh writer and install it. The caller must already hold
    /// `writer_open_gate`; both the first promotion and a re-promotion after a
    /// fence funnel through here, so the fence path cannot deadlock against
    /// `promote_writer` taking the same gate a second time.
    async fn install_writer(&self) -> Result<Db> {
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
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(writer.clone());
        Ok(writer)
    }

    async fn empty_store() -> Result<Db> {
        EMPTY_GRAPH_STORE
            .get_or_try_init(|| {
                Box::pin(async {
                    Db::builder(Path::from("empty"), Arc::new(InMemory::new()))
                        .build()
                        .await
                        .map_err(GraphError::from)
                })
            })
            .await
            .cloned()
    }

    pub(crate) async fn open_reader(&self) -> Result<Option<Arc<DbReader>>> {
        if let Some(reader) = self.inner.reader.read().await.as_ref().cloned() {
            return Ok(Some(reader));
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
                Ok(Some(reader))
            }
            Err(GraphError::Slate(error)) if matches!(error.kind(), ErrorKind::DatabaseMissing) => {
                Self::empty_store().await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn readable_reader(&self) -> Result<Option<Arc<DbReader>>> {
        let Some(reader) = self.open_reader().await? else {
            return Ok(None);
        };
        if self.inner.reader_refresh_generation.load(Ordering::Acquire)
            == self
                .inner
                .reader_refreshed_generation
                .load(Ordering::Acquire)
        {
            return Ok(Some(reader));
        }

        let _refresh_guard = self.inner.reader_refresh_gate.lock().await;
        loop {
            let required_generation = self.inner.reader_refresh_generation.load(Ordering::Acquire);
            if required_generation
                == self
                    .inner
                    .reader_refreshed_generation
                    .load(Ordering::Acquire)
            {
                break;
            }
            reader.refresh().await?;
            self.inner
                .reader_refreshed_generation
                .store(required_generation, Ordering::Release);
        }
        Ok(Some(reader))
    }

    pub(crate) async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>> {
        if let Ok(snapshot) = ACTIVE_STORAGE_SNAPSHOT.try_with(Arc::clone) {
            return Ok(snapshot.get_with_options(key, options).await?);
        }
        if let Some(writer) = self.readable_writer() {
            match writer.get_with_options(key, options).await {
                Ok(value) => return Ok(value),
                Err(error) if self.recover_closed_writer_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(reader) = self.readable_reader().await? {
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
        if let Some(writer) = self.readable_writer() {
            let writer_start_suffix = start_suffix.clone();
            let result = match writer_start_suffix {
                Some(start) => {
                    writer
                        .scan_prefix_with_options(prefix, start.., options)
                        .await
                }
                None => writer.scan_prefix_with_options(prefix, .., options).await,
            };
            match result {
                Ok(iter) => return Ok(iter),
                Err(error) if self.recover_closed_writer_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(reader) = self.readable_reader().await? {
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
        if let Some(writer) = self.readable_writer() {
            match writer.snapshot().await {
                Ok(snapshot) => return Ok(Arc::new(GraphStorageSnapshot::Writer(snapshot))),
                Err(error) if self.recover_closed_writer_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(reader) = self.readable_reader().await? {
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
        if let Some(reader) = self.readable_reader().await? {
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
        if let Some(writer) = self.readable_writer() {
            return Ok(writer.status().durable_seq);
        }
        Ok(self
            .readable_reader()
            .await?
            .map(|reader| reader.status().durable_seq)
            .unwrap_or(0))
    }

    pub(crate) async fn refresh_durable_reader(&self) -> Result<u64> {
        let Some(reader) = self.open_reader().await? else {
            return Ok(0);
        };
        let _refresh_guard = self.inner.reader_refresh_gate.lock().await;
        loop {
            let required_generation = self.inner.reader_refresh_generation.load(Ordering::Acquire);
            reader.refresh().await?;
            self.inner
                .reader_refreshed_generation
                .store(required_generation, Ordering::Release);
            if self.inner.reader_refresh_generation.load(Ordering::Acquire) == required_generation {
                break;
            }
        }
        Ok(reader.status().durable_seq)
    }

    pub(crate) async fn last_durable_wal_id(&self) -> Result<u64> {
        let snapshot = self.snapshot().await?;
        if let Some(last_wal_id) = snapshot.last_wal_id() {
            return Ok(last_wal_id);
        }
        Ok(self.writer()?.last_flushed_wal_id())
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use tokio::time::Instant;

    /// Any error the loop can carry; the policy never inspects it beyond the
    /// fenced/failed split the caller already made.
    fn failure() -> GraphError {
        GraphError::ReadOnlyShardStorage
    }

    /// Decision 6's three rules in one scenario, on paused virtual time. This is
    /// the scheduling clock only — nothing here reads object-store liveness.
    ///
    /// A fence waits one heartbeat interval and *resets* the ladder, so the
    /// plain failure after it sleeps 2s (the floor, doubled before use) rather
    /// than the 10s it would have reached had the fence advanced the ladder; the
    /// next failure sleeps 4s. Every attempt is entered straight from the sleep,
    /// with no ownership re-check in between: the loop's only contact with the
    /// outside world is calling the body, and it calls it a fourth time after
    /// the fence without being told this node still owns the cell.
    #[tokio::test(start_paused = true)]
    async fn a_fence_waits_one_heartbeat_interval_and_resets_the_ladder_to_its_floor() {
        let started = Instant::now();
        let entered = RefCell::new(Vec::new());

        let result = retry_with_fence_backoff(
            || async {
                let attempt = {
                    let mut entered = entered.borrow_mut();
                    entered.push(started.elapsed());
                    entered.len()
                };
                match attempt {
                    1 => FenceAttempt::Fenced(failure()),
                    2 | 3 => FenceAttempt::Failed(failure()),
                    _ => FenceAttempt::Refreshed,
                }
            },
            Duration::from_secs(5),
            WRITER_REFRESH_ATTEMPTS,
        )
        .await;

        assert!(result.is_ok(), "the fourth attempt refreshed");
        assert_eq!(
            entered.into_inner(),
            vec![
                Duration::ZERO,
                Duration::from_secs(5), // one heartbeat interval after the fence
                Duration::from_secs(7), // +2s: the ladder restarted at its floor
                Duration::from_secs(11), // +4s: and then doubled
            ]
        );
    }

    /// The ladder is sleet's: doubled before use, so the first failure sleeps 2s,
    /// and capped, so a store that stays broken is retried every minute rather
    /// than at an ever-receding deadline.
    #[tokio::test(start_paused = true)]
    async fn the_failure_ladder_doubles_before_each_wait_and_stops_at_the_maximum() {
        let started = Instant::now();
        let entered = RefCell::new(Vec::new());

        let _ = retry_with_fence_backoff(
            || async {
                entered.borrow_mut().push(started.elapsed());
                FenceAttempt::Failed(failure())
            },
            Duration::from_secs(5),
            8,
        )
        .await;

        let waits: Vec<Duration> = entered
            .into_inner()
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect();
        assert_eq!(
            waits,
            vec![2, 4, 8, 16, 32, 60, 60]
                .into_iter()
                .map(Duration::from_secs)
                .collect::<Vec<_>>()
        );
    }

    /// The budget is what keeps a blocked write from waiting forever. It is
    /// spent on attempts, not sleeps: the last failure is handed back at once.
    #[tokio::test(start_paused = true)]
    async fn spending_the_attempt_budget_returns_the_last_failure_without_a_trailing_wait() {
        let started = Instant::now();
        let attempts = Cell::new(0u32);

        let err = retry_with_fence_backoff(
            || async {
                attempts.set(attempts.get() + 1);
                FenceAttempt::Failed(failure())
            },
            Duration::from_secs(5),
            3,
        )
        .await
        .expect_err("every attempt failed");

        assert!(matches!(err, GraphError::ReadOnlyShardStorage));
        assert_eq!(attempts.get(), 3);
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(6),
            "2s + 4s, no more"
        );
    }

    /// A budget of one still makes one attempt: `max_attempts` counts attempts,
    /// not retries, and zero would silently skip the refresh a write depends on.
    #[tokio::test(start_paused = true)]
    async fn a_refresh_that_succeeds_first_time_never_sleeps() {
        let started = Instant::now();
        let attempts = Cell::new(0u32);

        retry_with_fence_backoff(
            || async {
                attempts.set(attempts.get() + 1);
                FenceAttempt::Refreshed
            },
            Duration::from_secs(5),
            WRITER_REFRESH_ATTEMPTS,
        )
        .await
        .expect("refreshed");

        assert_eq!(attempts.get(), 1);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}
