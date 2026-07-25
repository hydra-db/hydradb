use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, RwLock as StdRwLock};
use std::time::{Duration, Instant};

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
use turbolay_placement::cell_writer;

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
    // Armed by a fence or a failed refresh, consulted by the promotion gate.
    writer_reopen_gate: StdMutex<WriterReopenGate>,
}

static EMPTY_GRAPH_STORE: OnceCell<Db> = OnceCell::const_new();

/// The floor of the retry ladder, and the value a fence resets it to.
const FENCE_BACKOFF_FLOOR: Duration = Duration::from_secs(1);

/// The ceiling of the retry ladder. Doubling is applied *before* the wait, so
/// the first plain failure sleeps 2s and the sixth reaches this.
const MAX_FENCE_BACKOFF: Duration = Duration::from_secs(60);

/// How long a shard must wait before it may re-open its writer, and why.
///
/// # Why this is a gate and not a retry loop
///
/// Decision 6 of the rendezvous plan takes its three rules from sleet's
/// `supervise_with` (`../sleet/src/daemon.rs:436-485`), and the third of them is
/// *retry unconditionally, without re-checking ownership*. That rule is safe in
/// sleet and **unsafe here**, for a reason that is entirely about where the code
/// sits rather than about what it does.
///
/// Sleet's retry lives in a daemon supervisor. Nothing is waiting on it, and its
/// reconcile loop cancels the task out of band the moment ownership moves, so a
/// blind retry can never outlive the node's claim to the work. This sits on the
/// **write path**, where there is no out-of-band canceller: a loop that
/// re-promotes on its own authority would walk straight past
/// `ensure_local_writer`, which is the single place ownership is checked, and a
/// fenced non-owner would take the epoch back anyway. That defeats touch point
/// (b) — the one branch this whole plan exists to add.
///
/// So the wait is recorded here and enforced at the promotion gate, which
/// re-derives ownership first. Rules 1 and 2 survive exactly; rule 3 inverts,
/// deliberately.
///
/// # Why it takes `now`
///
/// Same reason as decision 10: a state machine that reads its own clock has to
/// be tested by sleeping. Every method takes the instant, so the tests are plain
/// `#[test]`s over hand-built `Instant`s and the whole policy is checked in
/// microseconds.
#[derive(Debug)]
struct WriterReopenGate {
    /// The earliest instant a writer may be re-opened, or `None` if now.
    not_before: Option<Instant>,
    /// The exponential ladder, advanced by plain failures and reset by a fence.
    backoff: Duration,
}

impl Default for WriterReopenGate {
    fn default() -> Self {
        Self {
            not_before: None,
            backoff: FENCE_BACKOFF_FLOOR,
        }
    }
}

impl WriterReopenGate {
    /// A fence: wait exactly one heartbeat interval, and reset the ladder.
    ///
    /// The interval is sized to give *the rival* time to refresh its view and
    /// stand down, so it is the bare interval rather than anything derived from
    /// the ladder. And the reset is what keeps a converging node from looking
    /// dead: without it, repeated fences ride the ladder to [`MAX_FENCE_BACKOFF`]
    /// even though nothing has actually failed.
    fn note_fence(&mut self, now: Instant, heartbeat_interval: Duration) {
        self.backoff = FENCE_BACKOFF_FLOOR;
        self.not_before = Some(now + heartbeat_interval);
    }

    /// A plain failure: advance the ladder and wait that long.
    ///
    /// Doubling is applied *before* use, so the first failure waits 2s.
    fn note_failure(&mut self, now: Instant) {
        self.backoff = (self.backoff * 2).min(MAX_FENCE_BACKOFF);
        self.not_before = Some(now + self.backoff);
    }

    /// A writer opened, or refreshed cleanly. Clear everything.
    fn note_success(&mut self) {
        self.backoff = FENCE_BACKOFF_FLOOR;
        self.not_before = None;
    }

    /// How much longer the caller must wait, or `None` if it may proceed.
    fn remaining(&self, now: Instant) -> Option<Duration> {
        self.not_before
            .filter(|not_before| *not_before > now)
            .map(|not_before| not_before - now)
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
        heartbeat_interval: Duration,
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
                heartbeat_interval,
                writer_reopen_gate: StdMutex::new(WriterReopenGate::default()),
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

    /// This shard's store path split into the base it shares with its sibling
    /// cells and its own cell id, or `None` if the path has no base to split
    /// off.
    ///
    /// Both cluster types lay shards out as `<base>/<cell_id>`
    /// (`engine/cluster.rs`), so this is where the advisory cell-writer record
    /// goes: beside the cell it describes, derived from where the shard
    /// actually is rather than from a base passed down a second time and free
    /// to disagree.
    ///
    /// `None` means a single-segment path — an embedder that opened a store at
    /// the bucket root — which has no `<base>` and therefore no place to put the
    /// record. The record is advisory, so the callers warn and carry on.
    pub(crate) fn cell_location(&self) -> Option<(Path, String)> {
        let mut parts: Vec<_> = self.inner.path.parts().collect();
        let cell = parts.pop()?;
        if parts.is_empty() {
            return None;
        }
        Some((Path::from_iter(parts), cell.as_ref().to_string()))
    }

    pub(crate) fn writer(&self) -> Result<Db> {
        self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)
    }

    /// The SlateDB writer epoch this shard currently holds, or `None` if it
    /// holds no writer.
    ///
    /// Straight off the open handle's manifest snapshot —
    /// `DbStatus.current_manifest` and `VersionedManifest::writer_epoch()` — and
    /// so it costs no I/O. **This is the authority.** The advisory record under
    /// `_cell_writers/v1/` records what this returned at the moment of a
    /// promotion; if the two ever disagree, this one is right.
    pub(crate) fn writer_epoch(&self) -> Option<u64> {
        self.open_writer()
            .map(|db| db.status().current_manifest.writer_epoch())
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
    /// Refresh the writer's manifest, recording a re-open delay if it is fenced.
    ///
    /// **This never promotes.** It drops the fenced handle, arms
    /// [`WriterReopenGate`] and hands the error back; the next write re-derives
    /// ownership through `ensure_local_writer` and is held off by
    /// [`GraphStore::writer_reopen_delay`] until the wait is spent. Promoting
    /// from in here would bypass the ownership check entirely — see the note on
    /// [`WriterReopenGate`].
    pub(crate) async fn refresh_writer_fence(&self) -> Result<()> {
        let _open_guard = self.inner.writer_open_gate.lock().await;
        // No writer at all is a read-only shard, not a fenced one: waiting
        // promotes nothing, so there is no delay to arm.
        let writer = self.open_writer().ok_or(GraphError::ReadOnlyShardStorage)?;
        match writer.refresh_manifest().await {
            Ok(()) => {
                self.reopen_gate().note_success();
                Ok(())
            }
            Err(err) if matches!(err.kind(), ErrorKind::Closed(slatedb::CloseReason::Fenced)) => {
                // Before the handle goes: the epoch this node thought it held.
                // Half of the log line below is meaningless without it.
                let lost_epoch = self.writer_epoch();
                self.clear_closed_writer();
                self.reopen_gate()
                    .note_fence(Instant::now(), self.inner.heartbeat_interval);
                self.log_fence_attribution(lost_epoch).await;
                Err(err.into())
            }
            Err(err) => {
                self.reopen_gate().note_failure(Instant::now());
                Err(err.into())
            }
        }
    }

    /// Name whoever last successfully promoted, so a fence reads in one line.
    ///
    /// This is the incident in `cell-writer-fencing-pingpong`: three nodes
    /// traded one cell's epoch, and every log line said a write had been fenced
    /// without ever saying by whom, because SlateDB's writer epoch carries no
    /// identity. The advisory record under `_cell_writers/v1/` is where that
    /// identity is written down, and this is the first of its two read paths
    /// (decision 3 of `docs/plans/2026-07-25-rendezvous-placement.md`).
    ///
    /// # Three things this deliberately is not
    ///
    /// It is **not on the write path**: it runs only in the fence arm, which is
    /// an error path that has already lost its writer and is about to return an
    /// error. A GET here costs a request per fence, not per write.
    ///
    /// It **decides nothing**. The record is read, formatted and dropped.
    /// Nothing downstream branches on it — promotion is rendezvous' call, made
    /// in `ensure_local_writer` with no I/O at all, and consulting the record to
    /// decide would be option 3b, which decision 3 rejects.
    ///
    /// It is **not authoritative**. The record says who last *promoted*, which
    /// stops being who *holds* the writer at the moment of the fence this
    /// function is logging. The name is reported as attribution, and every field
    /// is qualified accordingly — if it disagrees with the manifest, the
    /// manifest is right.
    async fn log_fence_attribution(&self, lost_epoch: Option<u64>) {
        let Some((base, cell_id)) = self.cell_location() else {
            tracing::warn!(
                path = %self.inner.path,
                lost_epoch,
                "writer fenced; no base path to read the advisory cell-writer record from"
            );
            return;
        };
        match cell_writer::read_cell_writer(self.inner.object_store.as_ref(), &base, &cell_id).await
        {
            Ok(Some(record)) => tracing::warn!(
                cell_id,
                lost_epoch,
                last_promoted_by = %record.node_id,
                last_promoted_epoch = record.epoch,
                last_promoted_at = %record.at,
                "writer fenced; the advisory record names the last node to promote"
            ),
            Ok(None) => tracing::warn!(
                cell_id,
                lost_epoch,
                "writer fenced; no advisory cell-writer record exists to name who took it"
            ),
            Err(error) => tracing::warn!(
                cell_id,
                lost_epoch,
                %error,
                "writer fenced; the advisory cell-writer record could not be read"
            ),
        }
    }

    fn reopen_gate(&self) -> StdMutexGuard<'_, WriterReopenGate> {
        self.inner
            .writer_reopen_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// How long before this shard may re-open its writer, or `None` if now.
    ///
    /// The promotion gate consults this *after* it has satisfied itself that
    /// this node should own the cell at all. Ownership first, then pacing: a
    /// node that is not the owner must be refused outright rather than merely
    /// asked to wait.
    pub(crate) fn writer_reopen_delay(&self) -> Option<Duration> {
        self.reopen_gate().remaining(Instant::now())
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
        self.reopen_gate().note_success();
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

    /// A synthetic origin. The gate only ever compares instants and subtracts
    /// them, so any origin works and none of these tests touch a real clock.
    fn origin() -> Instant {
        Instant::now()
    }

    /// Decision 6's rules 1 and 2, which are the two that survive the write-path
    /// context intact.
    ///
    /// A fence waits exactly one heartbeat interval — not the ladder, and not
    /// the larger of the two — because the interval is sized to let the *rival*
    /// re-derive its view and stand down. And it resets the ladder to its floor,
    /// so the plain failure that follows waits 2s (the floor, doubled before
    /// use) rather than the rung it would have reached had the fence advanced
    /// it.
    #[test]
    fn a_fence_waits_one_heartbeat_interval_and_resets_the_ladder_to_its_floor() {
        let now = origin();
        let mut gate = WriterReopenGate::default();

        gate.note_fence(now, Duration::from_secs(5));
        assert_eq!(gate.remaining(now), Some(Duration::from_secs(5)));
        assert_eq!(
            gate.remaining(now + Duration::from_secs(5)),
            None,
            "the wait is over the instant it elapses"
        );

        gate.note_failure(now + Duration::from_secs(5));
        assert_eq!(
            gate.remaining(now + Duration::from_secs(5)),
            Some(Duration::from_secs(2)),
            "the fence reset the ladder, so this is the floor doubled, not a higher rung"
        );
    }

    /// Repeated fences never ride the ladder. Without the reset, a node that is
    /// merely converging on a new view would back off to the 60s cap and look
    /// dead, which is the failure decision 6 rule 1 exists to prevent.
    #[test]
    fn fencing_repeatedly_never_advances_the_ladder() {
        let mut now = origin();
        let mut gate = WriterReopenGate::default();

        for _ in 0..10 {
            gate.note_fence(now, Duration::from_secs(5));
            assert_eq!(gate.remaining(now), Some(Duration::from_secs(5)));
            now += Duration::from_secs(5);
        }
    }

    /// The ladder is sleet's: doubled *before* use, so the first plain failure
    /// waits 2s, and capped, so a store that stays broken is retried every
    /// minute rather than at an ever-receding deadline.
    #[test]
    fn the_failure_ladder_doubles_before_each_wait_and_stops_at_the_maximum() {
        let mut now = origin();
        let mut gate = WriterReopenGate::default();
        let mut waits = Vec::new();

        for _ in 0..8 {
            gate.note_failure(now);
            let wait = gate.remaining(now).expect("a failure always arms the gate");
            waits.push(wait);
            now += wait;
        }

        assert_eq!(
            waits,
            vec![2, 4, 8, 16, 32, 60, 60, 60]
                .into_iter()
                .map(Duration::from_secs)
                .collect::<Vec<_>>()
        );
    }

    /// A clean refresh or a fresh writer clears both halves of the gate, so the
    /// next fence starts from the floor rather than from wherever the last
    /// incident left the ladder.
    #[test]
    fn success_clears_the_wait_and_the_ladder_together() {
        let now = origin();
        let mut gate = WriterReopenGate::default();

        gate.note_failure(now);
        gate.note_failure(now);
        assert!(gate.remaining(now).is_some());

        gate.note_success();
        assert_eq!(gate.remaining(now), None);

        gate.note_failure(now);
        assert_eq!(
            gate.remaining(now),
            Some(Duration::from_secs(2)),
            "back to the floor, not to the rung the earlier failures reached"
        );
    }

    /// An un-armed gate never delays. A shard that has never been fenced must
    /// promote on demand, which is the behaviour every non-contended cell has.
    #[test]
    fn a_gate_that_was_never_armed_lets_the_caller_straight_through() {
        assert_eq!(WriterReopenGate::default().remaining(origin()), None);
    }
}
