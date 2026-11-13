//! Bounded-vocabulary memory attribution. Byte values are estimates, not heap ownership.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::{AtomicDurationHistogram, DurationHistogramSnapshot};

/// Fixed process-wide stages; never attach query IDs, tenants or cells.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // Some fixed stages are recorded only with client-api enabled.
#[repr(usize)]
pub enum MemoryStage {
    ClientPrepared,
    ClientResultBuffer,
    ClientPrepare,
    ClientNamespaceWait,
    ClientQueryWait,
    ClientExecute,
    ClientContextParameters,
    ShardWriteWait,
    ShardWriteActive,
    CursorBuffer,
    StorageWriter,
    StorageReader,
    StorageSnapshot,
    StorageWriterClose,
    StorageReaderClose,
    StorageReaderRefresh,
    ShardWritePipelineWait,
    ShardWriteDurability,
    ShardWriteActivePipeline,
    ShardWriteActiveInline,
    ShardWriteDurabilityInline,
    ShardWritePriorDurability,
}
const STAGES: [&str; 22] = [
    "client_prepared",
    "client_result_buffer",
    "client_prepare",
    "client_namespace_wait",
    "client_query_wait",
    "client_execute",
    "client_context_parameters",
    "shard_write_wait",
    "shard_write_active",
    "cursor_buffer",
    "storage_writer",
    "storage_reader",
    "storage_snapshot",
    "storage_writer_close",
    "storage_reader_close",
    "storage_reader_refresh",
    "shard_write_pipeline_wait",
    "shard_write_durability",
    "shard_write_active_pipeline",
    "shard_write_active_inline",
    "shard_write_durability_inline",
    "shard_write_prior_durability",
];
const STAGE_DETAIL_LIMIT: usize = 1_024;

impl MemoryStage {
    fn detail_limit(self) -> usize {
        let _ = self;
        STAGE_DETAIL_LIMIT
    }
}

#[derive(Default)]
struct State {
    next: u64,
    live: BTreeMap<(Instant, u64), u64>,
    overflow_items: u64,
    overflow_started: Option<Instant>,
    items: u64,
    bytes: u64,
    entered: u64,
    exited: u64,
}
impl State {
    fn oldest_seconds(&self) -> f64 {
        self.live
            .first_key_value()
            .map(|((start, _), _)| *start)
            .into_iter()
            .chain(self.overflow_started)
            .min()
            .map_or(0.0, |start| start.elapsed().as_secs_f64())
    }

    fn oldest_tracked_seconds(&self) -> f64 {
        self.live
            .first_key_value()
            .map(|((start, _), _)| *start)
            .map_or(0.0, |start| start.elapsed().as_secs_f64())
    }

    fn overflow_cohort_seconds(&self) -> f64 {
        self.overflow_started.map_or(0.0, |start| start.elapsed().as_secs_f64())
    }
}
#[derive(Default)]
struct Tracker {
    state: Mutex<State>,
    duration: AtomicDurationHistogram,
}
static TRACKERS: OnceLock<[Arc<Tracker>; STAGES.len()]> = OnceLock::new();
fn trackers() -> &'static [Arc<Tracker>; STAGES.len()] {
    TRACKERS.get_or_init(|| std::array::from_fn(|_| Arc::new(Tracker::default())))
}

/// A process-global sample. Stage byte estimates may overlap; see the runbook.
#[derive(Clone, Debug)]
pub struct MemoryDiagnosticSnapshot {
    pub stage: &'static str,
    pub items: u64,
    pub estimated_bytes: u64,
    /// Compatibility value combining retained detail and overflow cohort age.
    pub oldest_seconds: f64,
    pub oldest_tracked_seconds: f64,
    pub tracking_overflow_items: u64,
    pub overflow_cohort_seconds: f64,
    pub entered: u64,
    pub exited: u64,
    pub duration: DurationHistogramSnapshot,
}
/// All stages, including zeroes, with no I/O or async locks.
pub fn memory_diagnostic_snapshot() -> Vec<MemoryDiagnosticSnapshot> {
    trackers()
        .iter()
        .zip(STAGES)
        .map(|(tracker, stage)| {
            let state = tracker.state.lock().unwrap_or_else(|p| p.into_inner());
            let oldest_tracked_seconds = state.oldest_tracked_seconds();
            MemoryDiagnosticSnapshot {
                stage,
                items: state.items,
                estimated_bytes: state.bytes,
                oldest_seconds: state.oldest_seconds(),
                oldest_tracked_seconds,
                tracking_overflow_items: state.overflow_items,
                overflow_cohort_seconds: state.overflow_cohort_seconds(),
                entered: state.entered,
                exited: state.exited,
                duration: tracker.duration.snapshot(),
            }
        })
        .collect()
}

/// The token follows the object or future. Dropping on any exit removes it.
#[derive(Debug)]
pub struct MemoryDiagnosticGuard {
    tracker: Arc<Tracker>,
    detailed_key: Option<(Instant, u64)>,
    started: Instant,
    bytes: u64,
}
impl std::fmt::Debug for Tracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemoryTracker")
    }
}

impl MemoryDiagnosticGuard {
    pub fn new(stage: MemoryStage, bytes: u64) -> Self {
        Self::tracked_with_limit(Arc::clone(&trackers()[stage as usize]), bytes, stage.detail_limit())
    }
    fn tracked_with_limit(tracker: Arc<Tracker>, bytes: u64, limit: usize) -> Self {
        let started = Instant::now();
        let mut state = tracker.state.lock().unwrap_or_else(|p| p.into_inner());
        let id = state.next;
        state.next += 1;
        state.items += 1;
        state.entered += 1;
        state.bytes = state.bytes.saturating_add(bytes);
        let detailed_key = if state.live.len() < limit {
            let key = (started, id);
            state.live.insert(key, bytes);
            Some(key)
        } else {
            state.overflow_items += 1;
            state.overflow_started = Some(state.overflow_started.map_or(started, |oldest| oldest.min(started)));
            None
        };
        drop(state);
        Self {
            tracker,
            detailed_key,
            started,
            bytes,
        }
    }
    pub fn set_bytes(&mut self, bytes: u64) {
        let mut state = self.tracker.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(key) = self.detailed_key {
            *state.live.get_mut(&key).expect("live diagnostic guard") = bytes;
        }
        state.bytes = state.bytes.saturating_sub(self.bytes).saturating_add(bytes);
        self.bytes = bytes;
    }
}
impl Drop for MemoryDiagnosticGuard {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(key) = self.detailed_key {
            state.live.remove(&key);
        } else {
            state.overflow_items = state.overflow_items.saturating_sub(1);
            if state.overflow_items == 0 {
                state.overflow_started = None;
            }
        }
        state.items = state.items.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(self.bytes);
        state.exited += 1;
        self.tracker.duration.record(self.started.elapsed());
    }
}

/// Cloneable handles share one token; the last wrapper clone releases it.
pub struct TrackedMemory<T> {
    value: T,
    _guard: Arc<MemoryDiagnosticGuard>,
}
impl<T> TrackedMemory<T> {
    pub fn new(value: T, stage: MemoryStage) -> Self {
        Self {
            value,
            _guard: Arc::new(MemoryDiagnosticGuard::new(stage, 0)),
        }
    }
}
impl<T: Clone> Clone for TrackedMemory<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            _guard: Arc::clone(&self._guard),
        }
    }
}
impl<T> std::ops::Deref for TrackedMemory<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

// Request estimates are a view of the parent client allocation, not a second
// allocation. Tokio task boundaries intentionally do not inherit this context.
tokio::task_local! { pub static REQUEST_ESTIMATED_BYTES: u64; }
pub fn request_estimated_bytes() -> u64 {
    REQUEST_ESTIMATED_BYTES.try_with(|bytes| *bytes).unwrap_or(0)
}
