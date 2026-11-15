use crate::core::config::collect_storage_metrics;
use crate::core::memory_diagnostics::{MemoryDiagnosticGuard, MemoryStage, TrackedMemory};
use crate::{
    graph_now_millis, open_graph_db, open_graph_reader, GraphCacheConfig, GraphDurabilityConfig, GraphError, GraphIndexPolicy,
    GraphMemoryConfig, GraphOpenOptions, GraphOperationalMetrics, GraphStorageMemoryConfig, Result, SharedSlateDbCache,
};
use hydradb_placement::cell_writer;
use slatedb::bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::db_cache::DbCache;
use slatedb::object_store::{memory::InMemory, path::Path, ObjectStore};
use slatedb::{Db, DbReader, DbReaderSnapshot, DbSnapshot, ErrorKind};
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, OnceLock, RwLock as StdRwLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OnceCell, OwnedMutexGuard, RwLock as AsyncRwLock, Semaphore};
use tracing::Instrument as _;
tokio::task_local! {
    static ACTIVE_STORAGE_SNAPSHOT: Arc<GraphStorageSnapshot>;
    static SNAPSHOT_READABLE_CELLS: std::cell::RefCell<SnapshotReadableCells>;
}

#[derive(Default)]
struct SnapshotReadableCells {
    cells: std::collections::BTreeSet<String>,
    bytes: usize,
}

impl SnapshotReadableCells {
    fn insert(&mut self, cell: &str) {
        if self.cells.len() >= 128 || self.bytes.saturating_add(cell.len()) > 16 * 1024 {
            return;
        }
        if self.cells.insert(cell.to_owned()) {
            self.bytes += cell.len();
        }
    }
}

#[cfg(feature = "experimental-cypher-engine")]
tokio::task_local! {
    static READ_ACCOUNTING: Arc<ReadAccounting>;
}

/// SlateDB reads issued on this task while a scope is installed, for
/// attributing storage work to the query operator that caused it.
///
/// Point reads are counted with the bytes of the key and value they returned.
/// Range scans are counted as requests only: their bytes arrive later, through
/// iterators the callers drive, and are not measured here.
#[cfg(feature = "experimental-cypher-engine")]
#[derive(Debug, Default)]
pub struct ReadAccounting {
    point_reads: AtomicU64,
    range_scans: AtomicU64,
    point_read_bytes: AtomicU64,
}

#[cfg(feature = "experimental-cypher-engine")]
impl ReadAccounting {
    pub async fn scope<F: std::future::Future>(self: &Arc<Self>, future: F) -> F::Output {
        READ_ACCOUNTING.scope(Arc::clone(self), future).await
    }

    /// Add this scope's reads to the scope enclosing the caller, if any.
    /// A call measured in a scope of its own (so overlapping calls on the same
    /// task cannot see each other's reads) still counts toward the request and
    /// any call it is nested in.
    pub fn absorb_into_enclosing(&self) {
        let _ = READ_ACCOUNTING.try_with(|enclosing| {
            enclosing
                .point_reads
                .fetch_add(self.point_reads.load(Ordering::Relaxed), Ordering::Relaxed);
            enclosing
                .range_scans
                .fetch_add(self.range_scans.load(Ordering::Relaxed), Ordering::Relaxed);
            enclosing
                .point_read_bytes
                .fetch_add(self.point_read_bytes.load(Ordering::Relaxed), Ordering::Relaxed);
        });
    }

    /// `(requests, point_read_bytes)` so far.
    pub fn totals(&self) -> (u64, u64) {
        (
            self.point_reads.load(Ordering::Relaxed) + self.range_scans.load(Ordering::Relaxed),
            self.point_read_bytes.load(Ordering::Relaxed),
        )
    }
}

fn account_point_read(_key: &[u8], _value: Option<&Bytes>) {
    #[cfg(feature = "experimental-cypher-engine")]
    let _ = READ_ACCOUNTING.try_with(|accounting| {
        accounting.point_reads.fetch_add(1, Ordering::Relaxed);
        accounting
            .point_read_bytes
            .fetch_add((_key.len() + _value.map_or(0, Bytes::len)) as u64, Ordering::Relaxed);
    });
}

fn account_range_scan() {
    #[cfg(feature = "experimental-cypher-engine")]
    let _ = READ_ACCOUNTING.try_with(|accounting| {
        accounting.range_scans.fetch_add(1, Ordering::Relaxed);
    });
}

pub struct SlateMutationState {
    pub graph_write_gate: Arc<Semaphore>,
    /// Permits `graph_write_gate` was created with; a semaphore cannot report it.
    pub graph_write_permit_capacity: usize,
    pub write_pipeline_gate: Arc<Semaphore>,
    pub index_policy: GraphIndexPolicy,
    pub await_durable_writes: bool,
    pub write_authority: GraphWriteAuthority,
    pub local_write_guard: Arc<Mutex<()>>,
    pub operation_metrics: Arc<GraphOperationalMetrics>,
    pub xlog_floor_ensured: Arc<StdRwLock<std::collections::HashSet<String>>>,
}

#[derive(Clone)]
pub struct SlateGraphStore {
    inner: Arc<SlateGraphStoreInner>,
    mutation: Arc<SlateMutationState>,
}

/// How long a listing that found no manifest newer than the writer's lets the
/// strong-read check rely on a single successor read. Must stay well below the
/// manifest garbage collector's minimum age (SlateDB's default, 300 s; Turbolay
/// sets no override); see `SlateGraphStore::check_writer_not_superseded`.
const MANIFEST_LIST_CONFIRM_INTERVAL: Duration = Duration::from_secs(60);

struct SlateGraphStoreInner {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    cache: GraphCacheConfig,
    slatedb_cache: Arc<SharedSlateDbCache>,
    reader_mode: crate::GraphReaderMode,
    reader_manifest_poll_interval: Duration,
    wal_feed: Option<WalFeed>,
    storage_memory: GraphStorageMemoryConfig,
    durability: GraphDurabilityConfig,
    runtime_handle: tokio::runtime::Handle,
    writer_state: Arc<ProcessWriterState>,
    writer_owner_active: AtomicBool,
    reader: AsyncRwLock<Option<Arc<TrackedMemory<DbReader>>>>,
    // A writer loss is acknowledged only by a reader refresh from the same or a later generation.
    reader_refresh_generation: AtomicU64,
    reader_refreshed_generation: AtomicU64,
    // Explicit requests are covered only by a refresh begun after their registration.
    reader_refresh_requested: AtomicU64,
    reader_refresh_completed: AtomicU64,
    reader_open_gate: Mutex<()>,
    reader_refresh_gate: Mutex<()>,
    // Strong reads on the cell's writer: a request is covered only by a fence
    // check begun after its registration, the same rule as the reader refresh.
    writer_check_requested: AtomicU64,
    writer_check_completed: AtomicU64,
    writer_check_gate: Mutex<()>,
    // Strong-read checks that found a manifest newer than the writer's and
    // fell back to a full writer fence refresh. Steady state is zero.
    writer_check_fallbacks: AtomicU64,
    // Read-only manifest access for strong-read checks, built once.
    manifest_admin: std::sync::OnceLock<slatedb::admin::Admin>,
    // When a listing last found no manifest newer than the writer's; see
    // `check_writer_not_superseded`.
    manifest_listed_at: StdMutex<Option<Instant>>,
    // The highest frontier a strong read was given from this node's writer. A
    // strong read that falls back to the reader never serves one behind it.
    strong_frontier_floor: AtomicU64,
    retiring: AtomicBool,
    // SlateDB metrics recorders for the currently-open writer and reader `Db`s,
    // captured when each is opened so `storage_metrics` can snapshot L0 counts
    // and request tallies for `/metrics`. Replaced on every re-open (a fence
    // recovery opens a fresh writer with a fresh recorder); `None` until first
    // open. Never held across an `.await`.
    writer_metrics_recorder: StdMutex<Option<Arc<DefaultMetricsRecorder>>>,
    reader_metrics_recorder: StdMutex<Option<Arc<DefaultMetricsRecorder>>>,
}

struct ProcessWriterState {
    writer: StdRwLock<Option<TrackedMemory<Db>>>,
    // The registry's install signal; `None` for standalone opens, which no
    // registry tracks.
    installs: Option<Arc<tokio::sync::watch::Sender<()>>>,
    // Every store sharing this writer state, so a peer can ask this node to
    // confirm the writer by path (buddy WAL) through whichever store is still
    // open. Weak, so the registry never keeps a store alive.
    stores: StdMutex<Vec<buddy_wal::WeakStore>>,
    open_gate: Mutex<()>,
    owners: StdMutex<usize>,
    closing: AtomicBool,
    heartbeat_interval: Duration,
    reopen_gate: StdMutex<WriterReopenGate>,
}

struct ProcessWriterClosingGuard<'a>(&'a AtomicBool);

impl Drop for ProcessWriterClosingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ProcessWriterKey {
    path: String,
    node_id: String,
}

/// Process-local writer ownership shared by routed runtimes over one physical store.
///
/// Reuse one registry only when every participating runtime addresses the same
/// physical object store. Keeping the identity explicit avoids guessing backend
/// identity from an [`ObjectStore`] wrapper's allocation or display text.
pub struct ProcessWriterRegistry {
    states: StdMutex<BTreeMap<ProcessWriterKey, Weak<ProcessWriterState>>>,
    // Bumped after every writer install, so the buddy WAL feed learns of new
    // writers without polling; see `buddy_wal`.
    installs: Arc<tokio::sync::watch::Sender<()>>,
}

impl ProcessWriterRegistry {
    pub fn new() -> Self {
        Self {
            states: StdMutex::new(BTreeMap::new()),
            installs: Arc::new(tokio::sync::watch::Sender::new(())),
        }
    }
}

#[path = "state/buddy_wal.rs"]
mod buddy_wal;
pub use buddy_wal::{NodeWriters, WalFeed};

impl Default for ProcessWriterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

static PROCESS_WRITER_REGISTRIES: OnceLock<StdMutex<BTreeMap<usize, Weak<ProcessWriterRegistry>>>> = OnceLock::new();
static PROCESS_WRITER_CLEANUP_RUNTIME: OnceLock<tokio::runtime::Handle> = OnceLock::new();

fn process_writer_cleanup_runtime() -> &'static tokio::runtime::Handle {
    PROCESS_WRITER_CLEANUP_RUNTIME.get_or_init(|| {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("hydradb-writer-cleanup".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build process writer cleanup runtime");
                sender
                    .send(runtime.handle().clone())
                    .expect("publish process writer cleanup runtime");
                runtime.block_on(std::future::pending::<()>());
            })
            .expect("spawn process writer cleanup thread");
        receiver.recv().expect("receive process writer cleanup runtime")
    })
}

pub fn process_writer_registry(object_store: &Arc<dyn ObjectStore>) -> Arc<ProcessWriterRegistry> {
    let key = Arc::as_ptr(object_store) as *const () as usize;
    let registries = PROCESS_WRITER_REGISTRIES.get_or_init(|| StdMutex::new(BTreeMap::new()));
    let mut registries = registries.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    registries.retain(|_, registry| registry.strong_count() > 0);
    registries.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
        let registry = Arc::new(ProcessWriterRegistry::new());
        registries.insert(key, Arc::downgrade(&registry));
        registry
    })
}

/// Return the one writer state a graph-node may own for this physical database.
///
/// Standalone shard opens deliberately remain independent: callers use those
/// to model separate processes and SlateDB must still fence competing writers.
/// Routed opens supply their node ID, so duplicate scope runtimes inside one
/// graph-node converge on one handle without sharing across simulated nodes.
fn process_writer_state(
    path: &Path,
    heartbeat_interval: Duration,
    process_writer: Option<(&str, &Arc<ProcessWriterRegistry>)>,
) -> Result<Arc<ProcessWriterState>> {
    let new_state = |installs| {
        Arc::new(ProcessWriterState {
            writer: StdRwLock::new(None),
            installs,
            stores: StdMutex::new(Vec::new()),
            open_gate: Mutex::new(()),
            owners: StdMutex::new(0),
            closing: AtomicBool::new(false),
            heartbeat_interval,
            reopen_gate: StdMutex::new(WriterReopenGate::default()),
        })
    };
    let Some((node_id, registry)) = process_writer else {
        let state = new_state(None);
        state.add_owner();
        return Ok(state);
    };
    let key = ProcessWriterKey {
        path: path.to_string(),
        node_id: node_id.to_string(),
    };
    let mut states = registry.states.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    states.retain(|_, state| state.strong_count() > 0);
    let state = states.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
        let state = new_state(Some(Arc::clone(&registry.installs)));
        states.insert(key, Arc::downgrade(&state));
        state
    });
    if state.heartbeat_interval != heartbeat_interval {
        return Err(GraphError::RoutedWriterConfigMismatch {
            path: path.to_string(),
            node_id: node_id.to_string(),
            existing: state.heartbeat_interval,
            requested: heartbeat_interval,
        });
    }
    state.add_owner();
    // Keep selection and owner acquisition atomic. The weak entry remains
    // upgradeable while a final-owner close is in flight because that close
    // holds an Arc to the state. A concurrent opener therefore joins the same
    // closing state and the same open gate instead of creating a second writer.
    drop(states);
    Ok(state)
}

async fn finish_process_writer_owner_release(state: Arc<ProcessWriterState>) -> Result<()> {
    state.close_final_owner_writer().await
}

async fn abandon_process_writer_after_runtime_shutdown(state: Arc<ProcessWriterState>) {
    state.abandon_final_owner_writer().await;
}

fn schedule_process_writer_owner_release(
    runtime_handle: &tokio::runtime::Handle,
    state: Arc<ProcessWriterState>,
) -> tokio::sync::oneshot::Receiver<Result<()>> {
    let release_state = Arc::clone(&state);
    let release_task = runtime_handle.spawn(async move { finish_process_writer_owner_release(release_state).await });
    let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
    process_writer_cleanup_runtime().spawn(async move {
        let result = match release_task.await {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "writer cleanup runtime stopped; abandoning its inactive writer handle"
                );
                abandon_process_writer_after_runtime_shutdown(state).await;
                Ok(())
            }
        };
        let _ = result_sender.send(result);
    });
    result_receiver
}

static EMPTY_GRAPH_STORE: OnceCell<Db> = OnceCell::const_new();

fn is_active_reader_snapshot_error(error: &slatedb::Error) -> bool {
    matches!(error.kind(), ErrorKind::Invalid)
        && error
            .to_string()
            .contains("cannot close database reader while snapshots are active")
}

async fn close_reader_after_snapshots(reader: Arc<TrackedMemory<DbReader>>, _diagnostic: MemoryDiagnosticGuard) {
    let mut delay = Duration::from_millis(10);
    loop {
        tokio::time::sleep(delay).await;
        match reader.close().await {
            Ok(()) => return,
            Err(error) if is_active_reader_snapshot_error(&error) => {
                delay = (delay * 2).min(Duration::from_secs(1));
            }
            Err(error) => {
                tracing::warn!(%error, "failed to retire SlateDB reader after snapshots drained");
                return;
            }
        }
    }
}

/// The floor of the retry ladder, and the value a fence resets it to.
const FENCE_BACKOFF_FLOOR: Duration = Duration::from_secs(1);

/// The ceiling of the retry ladder. Doubling is applied *before* the wait, so
/// the first plain failure sleeps 2s and the sixth reaches this.
const MAX_FENCE_BACKOFF: Duration = Duration::from_secs(60);

/// How long a shard must wait before it may re-open its writer, and why.
///
/// # Why this is a gate and not a retry loop
///
/// Decision 6 of the rendezvous plan adapts its three rules from a proven
/// daemon-supervisor implementation, and the third of them is *retry
/// unconditionally, without re-checking ownership*. That rule is safe in a
/// supervisor and **unsafe here**, for a reason that is entirely about where the
/// code sits rather than about what it does.
///
/// A supervisor's retry lives in a daemon. Nothing is waiting on it, and its
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

impl ProcessWriterState {
    fn owners(&self) -> StdMutexGuard<'_, usize> {
        self.owners.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn add_owner(&self) {
        *self.owners() += 1;
    }

    fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Make `writer` this state's writer and tell the registry's listeners.
    fn install(&self, writer: TrackedMemory<Db>) {
        *self.writer.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(writer);
        if let Some(installs) = &self.installs {
            installs.send_replace(());
        }
    }

    fn begin_release(&self) -> bool {
        let mut owners = self.owners();
        *owners = owners.checked_sub(1).expect("process writer owner count underflow");
        if *owners > 0 {
            return false;
        }
        // Set this while owner registration is still excluded. A new owner
        // either joins before the decrement (making it non-final), or joins
        // after this flag is visible and waits for the old handle to close.
        self.closing.store(true, Ordering::Release);
        true
    }

    async fn close_final_owner_writer(&self) -> Result<()> {
        let _diagnostic = MemoryDiagnosticGuard::new(MemoryStage::StorageWriterClose, 0);
        let _open_guard = self.open_gate.lock().await;
        let writer = self.writer.write().unwrap_or_else(|poisoned| poisoned.into_inner()).take();
        let result = match writer {
            Some(writer) => writer.close().await.map_err(GraphError::from),
            None => Ok(()),
        };
        self.closing.store(false, Ordering::Release);
        result
    }

    async fn abandon_final_owner_writer(&self) {
        let _open_guard = self.open_gate.lock().await;
        self.writer.write().unwrap_or_else(|poisoned| poisoned.into_inner()).take();
        self.closing.store(false, Ordering::Release);
    }

    async fn retire_writer(&self) -> Result<bool> {
        let _diagnostic = MemoryDiagnosticGuard::new(MemoryStage::StorageWriterClose, 0);
        let _open_guard = self.open_gate.lock().await;
        self.closing.store(true, Ordering::Release);
        let _closing_guard = ProcessWriterClosingGuard(&self.closing);
        let writer = self.writer.write().unwrap_or_else(|poisoned| poisoned.into_inner()).take();
        let result = match writer {
            Some(writer) => writer.close().await.map(|_| true).map_err(GraphError::from),
            None => Ok(false),
        };
        result
    }
}

impl Drop for SlateGraphStoreInner {
    fn drop(&mut self) {
        if !self.writer_owner_active.swap(false, Ordering::AcqRel) {
            return;
        }
        let writer_state = Arc::clone(&self.writer_state);
        if !writer_state.begin_release() {
            return;
        }
        drop(schedule_process_writer_owner_release(&self.runtime_handle, writer_state));
    }
}

pub enum GraphStorageSnapshot {
    Writer(TrackedMemory<Arc<DbSnapshot>>),
    Reader(TrackedMemory<Arc<DbReaderSnapshot>>),
    Empty(TrackedMemory<Arc<DbSnapshot>>),
}

impl GraphStorageSnapshot {
    pub fn seq(&self) -> u64 {
        match self {
            Self::Writer(snapshot) | Self::Empty(snapshot) => snapshot.seq(),
            Self::Reader(snapshot) => snapshot.seq(),
        }
    }

    pub fn last_wal_id(&self) -> Option<u64> {
        match self {
            Self::Writer(..) => None,
            Self::Reader(snapshot) => Some(snapshot.last_wal_id()),
            Self::Empty(..) => Some(0),
        }
    }

    pub async fn get_with_options(&self, key: &[u8], options: &ReadOptions) -> std::result::Result<Option<Bytes>, slatedb::Error> {
        // Accounted here rather than in `SlateGraphStore`, so the query paths that
        // hold a snapshot and read it directly are counted too.
        let value = match self {
            Self::Writer(snapshot) | Self::Empty(snapshot) => snapshot.get_with_options(key, options).await,
            Self::Reader(snapshot) => snapshot.get_with_options(key, options).await,
        }?;
        account_point_read(key, value.as_ref());
        Ok(value)
    }

    pub async fn scan_prefix_with_options<T>(
        &self,
        prefix: &[u8],
        subrange: T,
        options: &ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error>
    where
        T: slatedb::ByteRangeBounds + Send,
    {
        account_range_scan();
        match self {
            Self::Writer(snapshot) | Self::Empty(snapshot) => snapshot.scan_prefix_with_options(prefix, subrange, options).await,
            Self::Reader(snapshot) => snapshot.scan_prefix_with_options(prefix, subrange, options).await,
        }
    }
}

impl SlateGraphStore {
    pub async fn open(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        write_authority: GraphWriteAuthority,
    ) -> Result<Self> {
        if !options.durability.await_durable_writes && !matches!(&write_authority, GraphWriteAuthority::ReadOnly) {
            return Err(GraphError::UnsafeDurabilityConfig {
                operation: "open_write_authoritative_shard",
                reason: "graph writes rely on remotely durable SlateDB sequences, degree counters, and idempotency records".to_string(),
            });
        }
        let store = Self::lazy_with_authority(
            path.into(),
            object_store,
            &options,
            &GraphMemoryConfig::default(),
            None,
            write_authority.clone(),
        )?;
        match write_authority {
            GraphWriteAuthority::ReadOnly => {
                store.open_reader().await?;
            }
            GraphWriteAuthority::Promotable => {}
            GraphWriteAuthority::Writer => {
                store.promote_writer().await?;
            }
        }
        Ok(store)
    }

    pub fn mutation_state(&self) -> &SlateMutationState {
        &self.mutation
    }

    pub fn has_block_cache(&self) -> bool {
        self.inner.cache.slatedb_cache_bytes > 0
    }

    pub fn lazy(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        options: &GraphOpenOptions,
        memory: &GraphMemoryConfig,
        process_writer: Option<(&str, Arc<ProcessWriterRegistry>)>,
    ) -> Result<Self> {
        Self::lazy_with_authority(path, object_store, options, memory, process_writer, GraphWriteAuthority::ReadOnly)
    }

    pub fn lazy_with_authority(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        options: &GraphOpenOptions,
        memory: &GraphMemoryConfig,
        process_writer: Option<(&str, Arc<ProcessWriterRegistry>)>,
        write_authority: GraphWriteAuthority,
    ) -> Result<Self> {
        let runtime_handle = tokio::runtime::Handle::current();
        let writer_state = process_writer_state(
            &path,
            options.fence_backoff_interval,
            process_writer.as_ref().map(|(node_id, registry)| (*node_id, registry)),
        )?;
        let slatedb_cache = crate::process_slate_db_cache_with_config(&object_store, options.cache.slatedb_cache_config())?;
        let slatedb_cache = slatedb_cache.scoped(path.as_ref());
        let graph_write_permit_capacity = options.backpressure_policy.max_concurrent_graph_writes.max(1);
        Ok(Self {
            mutation: Arc::new(SlateMutationState {
                graph_write_gate: Arc::new(Semaphore::new(graph_write_permit_capacity)),
                graph_write_permit_capacity,
                write_pipeline_gate: Arc::new(Semaphore::new(crate::write_pipeline::MAX_PENDING_DURABLE_WRITES)),
                index_policy: options.index_policy,
                await_durable_writes: options.durability.await_durable_writes,
                write_authority,
                local_write_guard: Arc::new(Mutex::new(())),
                operation_metrics: Arc::new(GraphOperationalMetrics::default()),
                xlog_floor_ensured: Arc::new(StdRwLock::new(std::collections::HashSet::new())),
            }),
            inner: Arc::new(SlateGraphStoreInner {
                path,
                object_store,
                cache: options.cache.clone(),
                slatedb_cache,
                reader_mode: options.reader_mode,
                reader_manifest_poll_interval: options.reader_manifest_poll_interval,
                wal_feed: options.wal_feed.clone(),
                storage_memory: memory.storage.clone(),
                durability: options.durability.clone(),
                runtime_handle,
                writer_state,
                writer_owner_active: AtomicBool::new(true),
                reader: AsyncRwLock::new(None),
                reader_refresh_generation: AtomicU64::new(0),
                reader_refreshed_generation: AtomicU64::new(0),
                reader_refresh_requested: AtomicU64::new(0),
                reader_refresh_completed: AtomicU64::new(0),
                reader_open_gate: Mutex::new(()),
                reader_refresh_gate: Mutex::new(()),
                writer_check_requested: AtomicU64::new(0),
                writer_check_completed: AtomicU64::new(0),
                writer_check_gate: Mutex::new(()),
                writer_check_fallbacks: AtomicU64::new(0),
                manifest_admin: std::sync::OnceLock::new(),
                manifest_listed_at: StdMutex::new(None),
                strong_frontier_floor: AtomicU64::new(0),
                retiring: AtomicBool::new(false),
                writer_metrics_recorder: StdMutex::new(None),
                reader_metrics_recorder: StdMutex::new(None),
            }),
        }
        .sharing_writer_state())
    }

    /// Snapshot the SlateDB storage-engine metrics for whichever of this store's
    /// writer/reader handles are open. Cheap: two mutex reads plus in-memory
    /// counter loads, no object-store I/O. Returns zeroes before either handle
    /// has been opened.
    pub fn storage_metrics(&self) -> GraphStorageMetricsSnapshot {
        let writer = self
            .inner
            .writer_metrics_recorder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let reader = self
            .inner
            .reader_metrics_recorder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        collect_storage_metrics(writer.as_deref(), reader.as_deref())
    }

    fn open_writer(&self) -> Option<TrackedMemory<Db>> {
        if self.inner.retiring.load(Ordering::Acquire) || self.inner.writer_state.is_closing() {
            return None;
        }
        self.inner
            .writer_state
            .writer
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn readable_writer(&self) -> Option<TrackedMemory<Db>> {
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
            .writer_state
            .writer
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let close_reason = writer.as_ref().and_then(|current| current.status().close_reason);
        if let Some(close_reason) = close_reason {
            *writer = None;
            self.inner.reader_refresh_generation.fetch_add(1, Ordering::AcqRel);
            if close_reason == slatedb::CloseReason::Fenced {
                self.reopen_gate()
                    .note_fence(Instant::now(), self.inner.writer_state.heartbeat_interval);
            }
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

    pub fn store_path(&self) -> &Path {
        &self.inner.path
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
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
    pub fn cell_location(&self) -> Option<(Path, String)> {
        let mut parts: Vec<_> = self.inner.path.parts().collect();
        let cell = parts.pop()?;
        if parts.is_empty() {
            return None;
        }
        Some((Path::from_iter(parts), cell.as_ref().to_string()))
    }

    pub fn writer(&self) -> Result<TrackedMemory<Db>> {
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
    pub fn writer_epoch(&self) -> Option<u64> {
        self.open_writer().map(|db| db.status().current_manifest.writer_epoch())
    }

    /// Refresh the writer's manifest, recording a re-open delay if it is fenced.
    ///
    /// A fence surfaces here: another node has taken the epoch while this one
    /// still holds an open handle.
    ///
    /// **This never promotes.** It drops the fenced handle, arms
    /// [`WriterReopenGate`] and hands the error back; the next write re-derives
    /// ownership through `ensure_local_writer` and is held off by
    /// [`SlateGraphStore::writer_reopen_delay`] until the wait is spent. Promoting
    /// from in here would bypass the ownership check entirely — see the note on
    /// [`WriterReopenGate`].
    ///
    /// # The span
    ///
    /// `writer.fence_refresh` is where the writer ping-pong becomes one backend
    /// query. The three attribution fields are declared empty and filled in by
    /// [`Self::log_fence_attribution`] only on the fence arm, so a healthy
    /// refresh carries no extra cost and a fence carries the identity of
    /// whoever took the epoch. Grouping these spans by `hydradb.cell_id` and
    /// counting distinct `hydradb.writer.last_promoted_by` over five minutes is
    /// the incident, stated as a query.
    ///
    /// **Emission only.** Every branch, every gate and every wait below is
    /// exactly as it was; this function deliberately matches the fenced-handle
    /// re-open delay of the supervision implementation it was adapted from, and
    /// the timing is not this plan's to touch.
    pub async fn refresh_writer_fence(&self) -> Result<()> {
        let span = tracing::info_span!(
            "writer.fence_refresh",
            // `Path::filename` is the cell directory and costs no allocation;
            // `cell_location` is only paid for on the fence arm.
            hydradb.cell_id = self.inner.path.filename().unwrap_or_default(),
            hydradb.writer.epoch = tracing::field::Empty,
            hydradb.writer.last_promoted_by = tracing::field::Empty,
            hydradb.writer.last_promoted_epoch = tracing::field::Empty,
            hydradb.writer.last_promoted_at = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        self.refresh_writer_fence_traced().instrument(span).await
    }

    /// The body of [`Self::refresh_writer_fence`], running inside its span.
    async fn refresh_writer_fence_traced(&self) -> Result<()> {
        let _open_guard = self.inner.writer_state.open_gate.lock().await;
        // No writer at all is a read-only shard, not a fenced one: waiting
        // promotes nothing, so there is no delay to arm.
        let writer = self.open_writer().ok_or(GraphError::ReadOnlyShardStorage).inspect_err(|error| {
            tracing::Span::current().record("error.class", error.class());
        })?;
        match writer.refresh_manifest().await {
            Ok(()) => {
                self.reopen_gate().note_success();
                Ok(())
            }
            Err(err) if matches!(err.kind(), ErrorKind::Closed(slatedb::CloseReason::Fenced)) => {
                // Before the handle goes: the epoch this node thought it held.
                // Half of the log line below is meaningless without it.
                let lost_epoch = self.writer_epoch();
                if let Some(epoch) = lost_epoch {
                    tracing::Span::current().record("hydradb.writer.epoch", epoch);
                }
                self.clear_closed_writer();
                self.log_fence_attribution(lost_epoch).await;
                let error: GraphError = err.into();
                // `fencing` is expected, not alarming — the class is here so a
                // dashboard can chart the rate, and the warn that already exists
                // in `log_fence_attribution` stays the only line emitted.
                tracing::Span::current().record("error.class", error.class());
                Err(error)
            }
            // Not a fence, and not a re-open: the handle is still open and
            // still this node's. There is nothing for the gate to pace — a
            // delay armed here would only be spent by some later promotion that
            // this failure says nothing about. The ladder belongs to failed
            // *opens*, which is where `promote_writer` arms it.
            Err(err) => {
                let error: GraphError = err.into();
                tracing::Span::current().record("error.class", error.class());
                Err(error)
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
    /// (decision 3 of the rendezvous placement design).
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
    ///
    /// # Why the three fields are also span attributes
    ///
    /// As a warn line these were a genuinely good diagnostic with nothing to
    /// correlate them to — one free-floating message per fence, joinable to the
    /// rest of the incident only by timestamp. Promoted onto the enclosing
    /// `writer.fence_refresh` span they answer the ping-pong question directly:
    /// group by `hydradb.cell_id`, count distinct
    /// `hydradb.writer.last_promoted_by` over a window, and a count above one
    /// *is* the duel. The warn stays exactly as it was, for whoever is tailing a
    /// pod rather than querying a backend.
    async fn log_fence_attribution(&self, lost_epoch: Option<u64>) {
        let Some((base, cell_id)) = self.cell_location() else {
            tracing::warn!(
                path = %self.inner.path,
                lost_epoch,
                "writer fenced; no base path to read the advisory cell-writer record from"
            );
            return;
        };
        match cell_writer::read_cell_writer(self.inner.object_store.as_ref(), &base, &cell_id).await {
            Ok(Some(record)) => {
                let span = tracing::Span::current();
                span.record("hydradb.writer.last_promoted_by", tracing::field::display(&record.node_id));
                span.record("hydradb.writer.last_promoted_epoch", record.epoch);
                span.record("hydradb.writer.last_promoted_at", tracing::field::display(&record.at));
                tracing::warn!(
                    cell_id,
                    lost_epoch,
                    last_promoted_by = %record.node_id,
                    last_promoted_epoch = record.epoch,
                    last_promoted_at = %record.at,
                    "writer fenced; the advisory record names the last node to promote"
                );
            }
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
            .writer_state
            .reopen_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// How long before this shard may re-open its writer, or `None` if now.
    ///
    /// The promotion gate consults this *after* it has satisfied itself that
    /// this node should own the cell at all. Ownership first, then pacing: a
    /// node that is not the owner must be refused outright rather than merely
    /// asked to wait.
    pub fn writer_reopen_delay(&self) -> Option<Duration> {
        self.reopen_gate().remaining(Instant::now())
    }

    pub async fn promote_writer(&self) -> Result<bool> {
        if self.inner.retiring.load(Ordering::Acquire) {
            return Err(GraphError::ReadOnlyShardStorage);
        }
        if self.open_writer().is_some() {
            return Ok(false);
        }
        self.spawn_writer_promotion().await.map_err(|error| GraphError::CorruptValue {
            key: format!("{}/writer-promotion", self.inner.path),
            reason: format!("writer promotion task failed: {error}"),
        })?
    }

    /// Close only the routed writer while keeping this shard available for reads.
    ///
    /// Lease loss must withdraw write authority immediately, but evicting the
    /// whole scoped shard would also discard its reader and caches. Promotion
    /// and retirement share `open_gate`, so a request cannot install a new
    /// writer while the old lease holder is still closing its handle.
    pub async fn retire_writer(&self) -> Result<bool> {
        let retired = self.inner.writer_state.retire_writer().await?;
        if retired {
            self.inner.reader_refresh_generation.fetch_add(1, Ordering::AcqRel);
        }
        Ok(retired)
    }

    /// Start promotion outside the requesting task's cancellation lifetime.
    ///
    /// Opening SlateDB starts writer background tasks before `Db::build` returns.
    /// If a timed-out Bolt request drops that future, releasing `open_gate` lets a
    /// later request open a second writer while the first open is still alive.
    /// The detached task owns the complete open-and-install sequence, so dropping
    /// its join handle cannot strand a half-open writer.
    fn spawn_writer_promotion(&self) -> tokio::task::JoinHandle<Result<bool>> {
        let store = self.clone();
        tokio::spawn(async move { store.promote_writer_owned().await }.instrument(tracing::Span::current()))
    }

    async fn promote_writer_owned(&self) -> Result<bool> {
        if self.inner.retiring.load(Ordering::Acquire) {
            return Err(GraphError::ReadOnlyShardStorage);
        }
        if self.open_writer().is_some() {
            return Ok(false);
        }
        let _open_guard = self.inner.writer_state.open_gate.lock().await;
        if self.inner.retiring.load(Ordering::Acquire) {
            return Err(GraphError::ReadOnlyShardStorage);
        }
        if self.open_writer().is_some() {
            return Ok(false);
        }
        // A failed open is the ladder's case, and the only one: this is where a
        // writer re-open actually happens, so it is the one place a wait can be
        // armed that the next attempt will really spend. Without it a store that
        // refuses every open is retried once per write, which is the hot loop
        // the exponential ladder exists to damp.
        match self.install_writer().await {
            Ok(_) => Ok(true),
            Err(err) => {
                self.reopen_gate().note_failure(Instant::now());
                Err(err)
            }
        }
    }

    /// Open a fresh writer and install it. The caller must already hold
    /// `writer_open_gate`; both the first promotion and a re-promotion after a
    /// fence funnel through here, so the fence path cannot deadlock against
    /// `promote_writer` taking the same gate a second time.
    async fn install_writer(&self) -> Result<TrackedMemory<Db>> {
        let (writer, recorder) = open_graph_db(
            self.inner.path.clone(),
            Arc::clone(&self.inner.object_store),
            &self.inner.cache,
            &self.inner.storage_memory,
            &self.inner.durability,
            Arc::clone(&self.inner.slatedb_cache) as Arc<dyn DbCache>,
        )
        .await?;
        let writer = TrackedMemory::new(writer, MemoryStage::StorageWriter);
        *self
            .inner
            .writer_metrics_recorder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(recorder);
        self.inner.writer_state.install(writer.clone());
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

    pub async fn open_reader(&self) -> Result<Option<Arc<TrackedMemory<DbReader>>>> {
        if self.inner.retiring.load(Ordering::Acquire) {
            return Err(GraphError::ReadOnlyShardStorage);
        }
        if let Some(reader) = self.inner.reader.read().await.as_ref().cloned() {
            return Ok(Some(reader));
        }
        let _open_guard = self.inner.reader_open_gate.lock().await;
        if self.inner.retiring.load(Ordering::Acquire) {
            return Err(GraphError::ReadOnlyShardStorage);
        }
        if let Some(reader) = self.inner.reader.read().await.as_ref().cloned() {
            return Ok(Some(reader));
        }
        match open_graph_reader(
            self.inner.path.clone(),
            Arc::clone(&self.inner.object_store),
            &self.inner.cache,
            self.inner.reader_mode,
            self.inner.reader_manifest_poll_interval,
            Arc::clone(&self.inner.slatedb_cache) as Arc<dyn DbCache>,
        )
        .await
        {
            Ok((reader, recorder)) => {
                *self
                    .inner
                    .reader_metrics_recorder
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(recorder);
                let reader = Arc::new(TrackedMemory::new(reader, MemoryStage::StorageReader));
                *self.inner.reader.write().await = Some(Arc::clone(&reader));
                self.follow_wal_feed(&reader);
                Ok(Some(reader))
            }
            Err(GraphError::Slate(error)) if matches!(error.kind(), ErrorKind::DatabaseMissing) => {
                Self::empty_store().await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn readable_reader(&self) -> Result<Option<Arc<TrackedMemory<DbReader>>>> {
        let Some(reader) = self.open_reader().await? else {
            return Ok(None);
        };
        if self.inner.reader_refresh_generation.load(Ordering::Acquire) == self.inner.reader_refreshed_generation.load(Ordering::Acquire) {
            return Ok(Some(reader));
        }

        let _refresh_guard = self.inner.reader_refresh_gate.lock().await;
        loop {
            let required_generation = self.inner.reader_refresh_generation.load(Ordering::Acquire);
            if required_generation == self.inner.reader_refreshed_generation.load(Ordering::Acquire) {
                break;
            }
            let _diagnostic = MemoryDiagnosticGuard::new(MemoryStage::StorageReaderRefresh, 0);
            reader.refresh().await?;
            self.inner.reader_refreshed_generation.store(required_generation, Ordering::Release);
        }
        Ok(Some(reader))
    }

    pub async fn get_with_options(&self, key: &[u8], options: &ReadOptions) -> Result<Option<Bytes>> {
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
        Ok(Self::empty_store().await?.get_with_options(key, options).await?)
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: &[u8],
        start_suffix: Option<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<slatedb::DbIterator> {
        self.scan_prefix_range_with_options(prefix, start_suffix, None, options).await
    }

    /// [`Self::scan_prefix_with_options`] with an exclusive end as well as an
    /// inclusive start, both relative to `prefix`.
    ///
    /// The end is what lets a descending walk resume where its last page
    /// stopped: SlateDB starts a descending scan at the top of its range, so
    /// without an end every page would re-read the index from its highest key.
    pub async fn scan_prefix_range_with_options(
        &self,
        prefix: &[u8],
        start_suffix: Option<Vec<u8>>,
        end_suffix_exclusive: Option<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<slatedb::DbIterator> {
        let range = (
            start_suffix.map_or(Bound::Unbounded, Bound::Included),
            end_suffix_exclusive.map_or(Bound::Unbounded, Bound::Excluded),
        );
        if let Ok(snapshot) = ACTIVE_STORAGE_SNAPSHOT.try_with(Arc::clone) {
            return Ok(snapshot.scan_prefix_with_options(prefix, range, options).await?);
        }
        if let Some(writer) = self.readable_writer() {
            match writer.scan_prefix_with_options(prefix, range.clone(), options).await {
                Ok(iter) => return Ok(iter),
                Err(error) if self.recover_closed_writer_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(reader) = self.readable_reader().await? {
            return Ok(reader.scan_prefix_with_options(prefix, range, options).await?);
        }
        Ok(Self::empty_store().await?.scan_prefix_with_options(prefix, range, options).await?)
    }

    pub async fn close(&self) -> Result<()> {
        self.inner.retiring.store(true, Ordering::Release);
        let _reader_guard = self.inner.reader_open_gate.lock().await;

        let reader = self.inner.reader.write().await.take();

        let close_writer = async {
            if self.inner.writer_owner_active.swap(false, Ordering::AcqRel) {
                if self.inner.writer_state.begin_release() {
                    schedule_process_writer_owner_release(&self.inner.runtime_handle, Arc::clone(&self.inner.writer_state))
                        .await
                        .unwrap_or_else(|_| {
                            Err(GraphError::CorruptValue {
                                key: format!("process-writer/{}", self.inner.path),
                                reason: "writer cleanup supervisor stopped unexpectedly".to_string(),
                            })
                        })
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            }
        };

        let close_reader = async {
            if let Some(reader) = reader {
                let diagnostic = MemoryDiagnosticGuard::new(MemoryStage::StorageReaderClose, 0);
                match reader.close().await {
                    Ok(()) => {}
                    Err(error) if is_active_reader_snapshot_error(&error) => {
                        tokio::spawn(close_reader_after_snapshots(reader, diagnostic));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        };
        // Checkpoint cleanup and the final writer drain are independent. Wait
        // for both (including on error) before releasing replacement capacity.
        let (writer_result, reader_result) = tokio::join!(
            close_writer.instrument(tracing::info_span!("storage.writer_close")),
            close_reader.instrument(tracing::info_span!("storage.reader_close")),
        );
        reader_result.and(writer_result)
    }

    pub async fn snapshot(&self) -> Result<Arc<GraphStorageSnapshot>> {
        if let Ok(snapshot) = ACTIVE_STORAGE_SNAPSHOT.try_with(Arc::clone) {
            return Ok(snapshot);
        }
        if let Some(writer) = self.readable_writer() {
            match writer
                .durable_snapshot()
                .instrument(tracing::info_span!("storage.writer_snapshot"))
                .await
            {
                Ok(snapshot) => {
                    return Ok(Arc::new(GraphStorageSnapshot::Writer(TrackedMemory::new(
                        snapshot,
                        MemoryStage::StorageSnapshot,
                    ))))
                }
                Err(error) if self.recover_closed_writer_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(reader) = self.readable_reader().await? {
            return reader
                .snapshot()
                .instrument(tracing::info_span!("storage.reader_snapshot"))
                .await
                .map(|snapshot| {
                    Arc::new(GraphStorageSnapshot::Reader(TrackedMemory::new(
                        snapshot,
                        MemoryStage::StorageSnapshot,
                    )))
                })
                .map_err(Into::into);
        }
        Self::empty_store()
            .await?
            .snapshot()
            .map(|snapshot| {
                Arc::new(GraphStorageSnapshot::Empty(TrackedMemory::new(
                    snapshot,
                    MemoryStage::StorageSnapshot,
                )))
            })
            .map_err(Into::into)
    }

    /// A snapshot of the writer as of now, whatever snapshot the task has
    /// pinned: for a writer that must read state its own earlier commits
    /// protect, after the reads a request was answering from were pinned.
    /// Reads from it follow the usual durability filter, so inside a write
    /// pipeline they see the writer's committed writes.
    pub async fn writer_snapshot(&self) -> Result<Arc<GraphStorageSnapshot>> {
        let snapshot = self.writer()?.snapshot()?;
        Ok(Arc::new(GraphStorageSnapshot::Writer(TrackedMemory::new(
            snapshot,
            MemoryStage::StorageSnapshot,
        ))))
    }

    pub async fn reader_snapshot(&self) -> Result<Arc<GraphStorageSnapshot>> {
        if let Some(reader) = self.readable_reader().await? {
            return reader
                .snapshot()
                .instrument(tracing::info_span!("storage.reader_snapshot"))
                .await
                .map(|snapshot| {
                    Arc::new(GraphStorageSnapshot::Reader(TrackedMemory::new(
                        snapshot,
                        MemoryStage::StorageSnapshot,
                    )))
                })
                .map_err(Into::into);
        }
        Self::empty_store()
            .await?
            .snapshot()
            .map(|snapshot| {
                Arc::new(GraphStorageSnapshot::Empty(TrackedMemory::new(
                    snapshot,
                    MemoryStage::StorageSnapshot,
                )))
            })
            .map_err(Into::into)
    }

    pub async fn durable_sequence(&self) -> Result<u64> {
        Ok(self.durable_sequence_observed().await?.0)
    }

    /// [`Self::durable_sequence`], plus **which branch answered it**.
    ///
    /// The branch is the single most diagnostic fact on the causal-read path
    /// and nothing recorded it before
    /// the cell-affine read routing design. On the cell's
    /// writer the answer is the writer's own commit status — no I/O, and at or
    /// past any bookmark minted from a commit here by construction, so
    /// `wait_for_storage_sequence`'s fast exit always fires. Anywhere else it
    /// comes from a `DbReader` that only advances when its manifest is
    /// refreshed from the object store, so the fast exit *cannot* fire for a
    /// bookmark newer than the last refresh and the 10ms poll loop is the only
    /// way out. That is the whole of the 17-38s `graph_client_prepare_duration`
    /// seen on the read path.
    ///
    /// Returning the flag rather than counting it here keeps the fact where it
    /// can be joined to a request: the shard has no idea which client asked, and
    /// the client had no way to find out. Nothing is allocated and nothing is
    /// locked — the bool falls out of the branch that was taken anyway.
    pub async fn durable_sequence_observed(&self) -> Result<(u64, bool)> {
        if let Some(writer) = self.readable_writer() {
            return Ok((writer.status().durable_seq, true));
        }
        Ok((
            self.readable_reader().await?.map(|reader| reader.status().durable_seq).unwrap_or(0),
            false,
        ))
    }

    pub async fn refresh_durable_reader(&self) -> Result<u64> {
        let Some(reader) = self.open_reader().await? else {
            return Ok(0);
        };
        let request = self.inner.reader_refresh_requested.fetch_add(1, Ordering::AcqRel) + 1;
        let _refresh_guard = self.inner.reader_refresh_gate.lock().await;
        if self.inner.reader_refresh_completed.load(Ordering::Acquire) >= request
            && self.inner.reader_refreshed_generation.load(Ordering::Acquire)
                == self.inner.reader_refresh_generation.load(Ordering::Acquire)
        {
            return Ok(reader.status().durable_seq);
        }
        loop {
            let covered = self.inner.reader_refresh_requested.load(Ordering::Acquire);
            let required_generation = self.inner.reader_refresh_generation.load(Ordering::Acquire);
            let _diagnostic = MemoryDiagnosticGuard::new(MemoryStage::StorageReaderRefresh, 0);
            reader.refresh().await?;
            self.inner.reader_refreshed_generation.store(required_generation, Ordering::Release);
            self.inner.reader_refresh_completed.store(covered, Ordering::Release);
            if self.inner.reader_refresh_generation.load(Ordering::Acquire) == required_generation {
                break;
            }
        }
        Ok(reader.status().durable_seq)
    }

    /// The frontier a `strong` read must observe, and the storage it reads.
    ///
    /// On the cell's writer the frontier is the writer's own durable sequence:
    /// writes are acknowledged only once durable, so everything this writer
    /// acknowledged is in it. What the writer cannot know locally is whether a
    /// newer writer has opened elsewhere and acknowledged writes of its own. A
    /// newer writer bumps the manifest's writer epoch before it can write, so a
    /// manifest check that begins after the read arrived and still finds this
    /// writer current proves nothing was acknowledged elsewhere before the read.
    /// That check is one manifest GET, shared by every strong read that
    /// registered before it began, instead of a `DbReader` refresh per read that
    /// downloads the manifest and replays new WAL behind `reader_refresh_gate`.
    ///
    /// Off the writer, with a peer's buddy WAL feed, the owner is asked to run
    /// that same check and the reader waits for its answer
    /// (`strong_frontier_from_owner`). Otherwise, or when that cannot answer,
    /// or when the check finds this writer fenced or gone, it is
    /// [`Self::refresh_durable_reader`] as before. `strong_snapshot` then reads
    /// from the same place: a writer that closes in between bumps
    /// `reader_refresh_generation`, and `readable_reader` refreshes before the
    /// reader serves anything.
    pub async fn refresh_strong_frontier(&self) -> Result<u64> {
        if self.readable_writer().is_some() {
            match self.confirm_writer_current().await {
                Ok(Some(hydradb_buddy_wal::Confirmed { durable_seq: sequence, .. })) => {
                    self.inner.strong_frontier_floor.fetch_max(sequence, Ordering::AcqRel);
                    return Ok(sequence);
                }
                Ok(None) => {}
                // A fenced writer was cleared by the check; anything else that
                // left a live writer behind is a real error.
                Err(error) if self.readable_writer().is_some() => return Err(error),
                Err(_) => {}
            }
        } else if let Some(sequence) = self.strong_frontier_from_owner().await? {
            // Off the writer, with a peer's feed: the owner confirmed its
            // writer and this reader has applied what it had acknowledged.
            // The floor keeps `strong_snapshot` from serving below it.
            self.inner.strong_frontier_floor.fetch_max(sequence, Ordering::AcqRel);
            return Ok(sequence);
        }
        self.refresh_durable_reader().await
    }

    /// Confirms, with one shared manifest check begun after this call
    /// registered, that this node's writer is still the cell's writer, and
    /// returns its durable sequence. `None` when no live writer remains.
    ///
    /// The epoch and sequence come from one read of one writer's status, so
    /// they always describe the same writer, even if the writer changed
    /// while the check ran.
    async fn confirm_writer_current(&self) -> Result<Option<hydradb_buddy_wal::Confirmed>> {
        let request = self.inner.writer_check_requested.fetch_add(1, Ordering::AcqRel) + 1;
        let _check_guard = self.inner.writer_check_gate.lock().await;
        if self.inner.writer_check_completed.load(Ordering::Acquire) < request {
            let covered = self.inner.writer_check_requested.load(Ordering::Acquire);
            let Some(writer) = self.readable_writer() else {
                return Ok(None);
            };
            if !self.check_writer_not_superseded(&writer).await? {
                // Superseded: this read goes to the reader. Not marked
                // completed, so later reads check again for themselves.
                return Ok(None);
            }
            self.inner.writer_check_completed.store(covered, Ordering::Release);
        }
        // A writer opened here after the check fenced every older one when it
        // opened, so it is current too.
        Ok(self.readable_writer().map(|writer| {
            let status = writer.status();
            hydradb_buddy_wal::Confirmed {
                writer_epoch: status.current_manifest.writer_epoch(),
                durable_seq: status.durable_seq,
            }
        }))
    }

    /// Proves no newer manifest than the writer's own exists, without going
    /// through the writer.
    ///
    /// `Db::refresh_manifest` (what [`Self::refresh_writer_fence`] calls) is a
    /// command to the writer's manifest-writer task, the same task that
    /// records memtable flushes, and it merges the result under the database
    /// state lock every read and transaction takes. Run for every strong read
    /// on a busy owner (54 a second on 2026-11-06) it starved flushes and held
    /// that lock: write retries went from 3% to 12% and transactions began to
    /// exhaust their retries on contention.
    ///
    /// A newer writer must write a manifest carrying its higher epoch before
    /// it can acknowledge anything, so it is enough to know that no manifest
    /// newer than the writer's current one exists. Only when one does (a
    /// compaction, or a new writer) does the check fall back to the full fence
    /// refresh, which lets the writer catch up and reports a fence if there is
    /// one.
    ///
    /// Manifest ids are consecutive, so the cheap proof is that the manifest
    /// after the writer's (`n + 1`) does not exist: one read of one object.
    /// That alone is not sound once garbage collection is involved, which may
    /// delete `n + 1` while keeping a later `n + 2`. But it never deletes a
    /// manifest younger than its minimum age: SlateDB's default of 300 s,
    /// which Turbolay does not override. So the check lists the manifests
    /// after the writer's at least every [`MANIFEST_LIST_CONFIRM_INTERVAL`]
    /// (60 s): a listing that finds none proves nothing newer existed then,
    /// and everything created since is younger than 60 s, so it is all still
    /// there and `n + 1` exists whenever anything newer does. Between
    /// listings, the single read suffices.
    ///
    /// A newer manifest is judged by its own writer epoch, not by asking the
    /// writer: the writer finds its latest manifest by probing `n + 1` too, so
    /// it would miss the same gap. A higher epoch means the writer has been
    /// superseded, and the read must not be served from it (`Ok(false)`); a
    /// newer manifest at the writer's own epoch (a compaction) only means the
    /// writer is behind, and the full fence refresh catches it up.
    async fn check_writer_not_superseded(&self, writer: &TrackedMemory<Db>) -> Result<bool> {
        let status = writer.status();
        let current = status.current_manifest.id();
        let epoch = status.current_manifest.writer_epoch();
        drop(status);
        let admin = self
            .inner
            .manifest_admin
            .get_or_init(|| slatedb::admin::Admin::builder(self.inner.path.clone(), Arc::clone(&self.inner.object_store)).build());
        let listed_recently = self
            .inner
            .manifest_listed_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some_and(|at| at.elapsed() < MANIFEST_LIST_CONFIRM_INTERVAL);
        let newer = if listed_recently {
            admin
                .read_manifest(Some(current.saturating_add(1)))
                .await?
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            // Stamped before the listing, so the window never outlives it.
            let started = Instant::now();
            let newer = admin.list_manifests(current.saturating_add(1)..).await?;
            if newer.is_empty() {
                *self
                    .inner
                    .manifest_listed_at
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(started);
            }
            newer
        };
        if newer.is_empty() {
            return Ok(true);
        }
        self.inner.writer_check_fallbacks.fetch_add(1, Ordering::Relaxed);
        if newer.iter().any(|manifest| manifest.writer_epoch() > epoch) {
            // Let the writer find its fence so it retires promptly; its own
            // probe can miss a collected successor, so the read goes to the
            // reader whatever this concludes.
            let _ = self.refresh_writer_fence().await;
            return Ok(false);
        }
        self.refresh_writer_fence().await?;
        Ok(true)
    }

    /// The snapshot a `strong` read executes against, after
    /// [`Self::refresh_strong_frontier`]: the writer's durable snapshot when
    /// this node holds a live writer, otherwise the refreshed reader's.
    ///
    /// The reader fallback does not trust the generation counter alone.
    /// Retirement hides the writer before it finishes closing it and only then
    /// bumps the generation, so a read that confirmed the writer's frontier and
    /// lost the writer in that window would find an unrefreshed reader. The
    /// reader is refreshed whenever it is behind any frontier this node's
    /// writer has handed to a strong read; everything up to that frontier is
    /// durable, so one refresh reaches it.
    pub async fn strong_snapshot(&self) -> Result<Arc<GraphStorageSnapshot>> {
        // A writer behind this node's reader is not the cell's current writer:
        // something newer committed past it. That happens when a read began
        // with no local writer, refreshed the reader, and a writer opened here
        // meanwhile and was fenced before it noticed. The reader, refreshed
        // for this read, is the one to serve it.
        let reader_ahead_of = |writer_durable: u64| async move {
            match self.inner.reader.read().await.as_ref() {
                Some(reader) => reader.status().durable_seq > writer_durable,
                None => false,
            }
        };
        if let Some(writer) = self.readable_writer() {
            if reader_ahead_of(writer.status().durable_seq).await {
                return self.reader_snapshot().await;
            }
            match writer
                .durable_snapshot()
                .instrument(tracing::info_span!("storage.writer_snapshot"))
                .await
            {
                Ok(snapshot) => {
                    return Ok(Arc::new(GraphStorageSnapshot::Writer(TrackedMemory::new(
                        snapshot,
                        MemoryStage::StorageSnapshot,
                    ))))
                }
                Err(error) if self.recover_closed_writer_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let floor = self.inner.strong_frontier_floor.load(Ordering::Acquire);
        if floor > 0 {
            if let Some(reader) = self.open_reader().await? {
                if reader.status().durable_seq < floor {
                    self.refresh_durable_reader().await?;
                }
            }
        }
        self.reader_snapshot().await
    }

    pub async fn last_durable_wal_id(&self) -> Result<u64> {
        let snapshot = self.snapshot().await?;
        if let Some(last_wal_id) = snapshot.last_wal_id() {
            return Ok(last_wal_id);
        }
        Ok(self.writer()?.last_flushed_wal_id())
    }

    pub fn wal_reader(&self) -> slatedb::WalReader {
        slatedb::WalReader::new(self.inner.path.clone(), Arc::clone(&self.inner.object_store))
    }

    pub async fn scope_snapshot<F>(snapshot: Arc<GraphStorageSnapshot>, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        // The query adapter reinstalls its pinned snapshot in each storage
        // callback, sometimes once per expanded edge. Resetting the readable
        // cells there repeats two marker GETs against the same immutable view.
        // Reuse only the exact snapshot object: sequence numbers can coincide
        // across different graph stores, and a newer snapshot must recheck drops.
        let already_scoped = ACTIVE_STORAGE_SNAPSHOT
            .try_with(|active| Arc::ptr_eq(active, &snapshot))
            .unwrap_or(false);
        if already_scoped {
            return future.await;
        }
        let future = SNAPSHOT_READABLE_CELLS.scope(std::cell::RefCell::new(SnapshotReadableCells::default()), future);
        ACTIVE_STORAGE_SNAPSHOT.scope(snapshot, future).await
    }

    pub fn snapshot_cell_is_readable(cell: &str) -> bool {
        SNAPSHOT_READABLE_CELLS
            .try_with(|state| state.borrow().cells.contains(cell))
            .unwrap_or(false)
    }

    pub fn remember_snapshot_readable_cell(cell: &str) {
        let _ = SNAPSHOT_READABLE_CELLS.try_with(|state| state.borrow_mut().insert(cell));
    }
}

/// SlateDB storage-engine counters and gauges, snapshotted from the recorder
/// each `Db`/`DbReader` was opened with. These describe the LSM the write and
/// read paths sit on top of — most importantly `l0_sst_count`, the number of
/// L0 SSTs a prefix scan has to consult. Legacy SSTs and prefixes outside the
/// configured incident-prefix extractor cannot use Bloom filters to skip
/// files. Newly built incident-prefix filters expose their pruning through
/// `sst_filter_prefix_negatives`. Values are
/// best-effort: a scope with no open writer or reader reports zeroes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphStorageMetricsSnapshot {
    /// Live count of L0 SSTs across the store. Gauge, updated on manifest poll.
    pub l0_sst_count: u64,
    /// Live count of compacted sorted runs. Gauge, updated on manifest poll.
    /// Its change over a window is the net of runs written and runs merged.
    pub sorted_run_count: u64,
    /// Live count of SSTs across the store, L0 and sorted runs. Gauge.
    pub sst_count: u64,
    /// Largest L0 SST count in any single segment tree. Gauge.
    pub segment_max_l0_sst_count: u64,
    /// Immutable memtables flushed to L0 over the store's life. Counter.
    pub immutable_memtable_flushes: u64,
    /// SlateDB `get` requests served (point lookups). Counter.
    pub get_requests: u64,
    /// SlateDB `scan` requests served (range scans). Counter.
    pub scan_requests: u64,
    /// Total in-memory size (memtables + immutable memtables), bytes. Gauge.
    pub total_mem_size_bytes: u64,
    /// Point lookups where an SST's bloom said "might be present" (the SST had
    /// to be opened). Counter.
    pub sst_filter_point_positives: u64,
    /// Point lookups where an SST's bloom proved absence — the SST was skipped
    /// with zero I/O. Counter. The whole benefit of a point-lookup index over a
    /// prefix scan lives in this counter.
    pub sst_filter_point_negatives: u64,
    /// Point-lookup filter positives that found nothing after opening the SST
    /// (bloom false-positive rate, numerator). Counter.
    pub sst_filter_point_false_positives: u64,
    /// Prefix scans where an SST's filter allowed the read. With no prefix
    /// extractor applicable this counts every SST every scan consults, making
    /// `sst_filter_prefix_positives / scan_requests` the average number of
    /// SSTs a single prefix scan opens. Counter.
    pub sst_filter_prefix_positives: u64,
    /// Prefix scans where an SST was skipped by a filter. Legacy SSTs and
    /// unsupported prefixes receive no prefix-filter help. Counter.
    pub sst_filter_prefix_negatives: u64,
    /// Prefix-scan filter positives that yielded no matching key — SSTs opened
    /// purely to prove emptiness (wasted I/O, measured directly). Counter.
    pub sst_filter_prefix_false_positives: u64,
    /// Writes throttled because unflushed bytes exceeded `max_unflushed_bytes`.
    /// Counter.
    pub backpressure_writes: u64,
    /// Writes stalled waiting on L0 (too many L0 SSTs, either globally or per
    /// key range). Counter, both stall types summed.
    pub l0_write_stalls: u64,
    /// Bytes written into output SSTs by compaction, all workers summed.
    /// Counter.
    pub compaction_bytes: u64,
    /// Compaction jobs currently executing, all workers summed. Gauge.
    pub running_compactions: u64,
    /// Unix timestamp (seconds) of the last completed compaction. Gauge; the
    /// distance from `now` is the compaction lag.
    pub last_compaction_timestamp_sec: u64,
    /// Bytes of collapsed key-value data written into memtables — the
    /// write-amplification denominator. Counter.
    pub memtable_write_bytes: u64,
    /// Bytes flushed to the WAL. Counter (write-amp numerator term).
    pub wal_flush_bytes: u64,
    /// WAL object upload attempts, including retries and failures. Counter.
    /// `write_commits / wal_buffer_flushes` approximates how many committed
    /// transactions share one upload (group commit size).
    pub wal_buffer_flushes: u64,
    /// WAL flush requests (timer, size and explicit triggers). Counter.
    pub wal_buffer_flush_requests: u64,
    /// Bytes flushed from immutable memtables into L0 SSTs. Counter
    /// (write-amp numerator term).
    pub l0_flush_bytes: u64,
    /// Block-cache hits on data blocks. Counter.
    pub block_cache_data_hits: u64,
    /// Block-cache misses on data blocks. Counter.
    pub block_cache_data_misses: u64,
    /// Block-cache hits on filter blocks (a bloom probe that cost no I/O).
    /// Counter.
    pub block_cache_filter_hits: u64,
    /// Block-cache misses on filter blocks. Counter.
    pub block_cache_filter_misses: u64,
    /// Object-store GET-family requests (get, get_range, head, …) across every
    /// component of the store. Counter — the actual I/O bill.
    pub object_store_get_requests: u64,
    /// Object-store PUT-family requests. Counter.
    pub object_store_put_requests: u64,
    /// [`Self::object_store_get_requests`] by the SlateDB component that made
    /// them, so foreground reads (`db`, `reader`) can be told from the
    /// compactor's and the garbage collector's. Counters.
    pub object_store_get_requests_by_component: ObjectStoreRequestsByComponent,
    /// [`Self::object_store_put_requests`] by component. Counters.
    pub object_store_put_requests_by_component: ObjectStoreRequestsByComponent,
    /// Disk-cache part reads answered from the node's volume. Counter.
    /// `disk_cache_part_accesses - disk_cache_part_hits` is the parts that went
    /// to the object store.
    pub disk_cache_part_hits: u64,
    /// Disk-cache part reads, hit or miss. Counter.
    pub disk_cache_part_accesses: u64,
    /// Bytes the disk cache holds on the node's volume. Gauge.
    pub disk_cache_bytes: u64,
    /// Bytes the disk cache has evicted to stay under its budget. Counter.
    pub disk_cache_evicted_bytes: u64,
}

/// Object-store requests of one op family by the SlateDB component that made
/// them: the `component` label of SlateDB's request counter. The writer `Db`
/// serves reads and writes its WAL and manifests as `db`, a `DbReader` reads
/// as `reader`, and the compactor and the garbage collector run beside them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectStoreRequestsByComponent {
    pub db: u64,
    pub reader: u64,
    pub compactor: u64,
    pub gc: u64,
}

impl ObjectStoreRequestsByComponent {
    /// The `component` label values, in the order [`Self::by_label`] lists them.
    pub const COMPONENTS: [&'static str; 4] = ["db", "reader", "compactor", "gc"];

    pub fn by_label(&self) -> [(&'static str, u64); 4] {
        let [db, reader, compactor, gc] = Self::COMPONENTS;
        [(db, self.db), (reader, self.reader), (compactor, self.compactor), (gc, self.gc)]
    }
}

#[derive(Clone)]
pub enum GraphWriteAuthority {
    ReadOnly,
    Promotable,
    Writer,
}

#[derive(Clone, Debug)]
pub enum GraphWriteOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
}

static LOCAL_WRITE_GUARD_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct LocalWriteGuard {
    pub token: String,
    _guard: OwnedMutexGuard<()>,
}

impl LocalWriteGuard {
    pub fn new(guard: OwnedMutexGuard<()>) -> Self {
        let counter = LOCAL_WRITE_GUARD_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            token: format!("{}-{}-{counter}", graph_now_millis(), std::process::id()),
            _guard: guard,
        }
    }

    pub async fn renew(&self) -> Result<()> {
        Ok(())
    }

    pub async fn release(self) -> Result<()> {
        Ok(())
    }
}

pub async fn finish_local_write<T>(guard: LocalWriteGuard, result: Result<T>) -> Result<T> {
    guard.release().await?;
    result
}

pub fn is_retryable_write_conflict(err: &GraphError) -> bool {
    matches!(err, GraphError::Slate(err) if matches!(err.kind(), ErrorKind::Transaction | ErrorKind::Invalid))
}
