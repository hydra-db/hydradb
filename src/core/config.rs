use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use slatedb::config::{DbReaderOptions, PreloadLevel, Settings};
use slatedb::object_store::{path::Path, ObjectStore};
use slatedb::{Db, DbReader, DbReaderMode};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricsRecorder};

use crate::{GraphCachePolicy, Result, StorageSequence};

pub const DEFAULT_TRUSTED_APPEND_CHUNK_EDGES: usize = 4_096;
#[cfg(not(test))]
const GRAPH_READER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(10);
#[cfg(test)]
const GRAPH_READER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GraphReaderMode {
    /// Follow durable graph state while maintaining a SlateDB checkpoint that
    /// protects referenced objects from garbage collection.
    #[default]
    ManagedCheckpoint,
    /// Follow durable graph state without creating or refreshing checkpoints.
    /// Callers must refresh before consistency-critical work and retry from a
    /// newly opened reader if garbage collection races an older snapshot.
    FollowLatest,
}

impl GraphReaderMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManagedCheckpoint => "managed_checkpoint",
            Self::FollowLatest => "follow_latest",
        }
    }
}

impl From<GraphReaderMode> for DbReaderMode {
    fn from(mode: GraphReaderMode) -> Self {
        match mode {
            GraphReaderMode::ManagedCheckpoint => Self::ManagedCheckpoint,
            GraphReaderMode::FollowLatest => Self::FollowLatest,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphLimits {
    pub max_bulk_import_edges: usize,
    pub max_artifact_source_epochs: StorageSequence,
    pub max_traversal_hops: u8,
    pub max_artifact_build_edges: u64,
    pub max_query_result_vertices: usize,
    /// Cap on rows returned by a single query.
    ///
    /// Distinct from [`GraphLimits::max_query_result_vertices`]: one vertex can
    /// appear in many rows, so a row-shaped result is not bounded by a vertex
    /// count. Row paths that reported `query_result_rows` and
    /// `query_batch_result_rows` were previously measured against the vertex
    /// cap, which rejected wide results that named very few vertices.
    pub max_query_result_rows: usize,
    pub max_query_intermediate_rows: usize,
    pub max_query_index_candidates: usize,
    pub max_query_scan_edges: u64,
    pub max_query_runtime_ms: Option<u64>,
    /// Deadline on the causal-consistency wait in
    /// `GraphShard::wait_for_storage_sequence`, i.e. how long a read may spend
    /// polling its local reader for the cell's durable sequence to reach the
    /// client's bookmark before it declines with `SnapshotAhead`.
    ///
    /// It exists because the wait used to borrow `max_query_runtime_ms`, and
    /// borrowing the *whole query budget* meant a read that could not catch up
    /// burned all 30s and then died to the client watchdog at 29,999ms with a
    /// generic timeout — having executed nothing. `SnapshotAhead { cell_id,
    /// read_epoch, current_epoch }` names the problem exactly, and it was
    /// losing that race to the error that hides it, which is why staging saw
    /// 17–38s "prepare" times and essentially no `SnapshotAhead`. Change 3 of
    /// `docs/plans/2026-08-21-cell-affine-read-routing.md`; evidence in
    /// `docs/2026-08-21-read-path-30s-timeout-findings.md`.
    ///
    /// A plain `u64` rather than `max_query_runtime_ms`'s `Option<u64>`: an
    /// unbounded wait is precisely the defect, so "no deadline" must not be
    /// representable. Keep it strictly below `max_query_runtime_ms` — the point
    /// is for the wait to lose to nothing, so its own error is what the client
    /// sees. Zero is legal and means one refresh attempt, then decline.
    pub max_bookmark_wait_ms: u64,
    /// Cost cap on the WAL tail walk: the most WAL files a single
    /// `topology_tail_since` may read. Every file is one object-store
    /// round trip, and the span between an old index generation and the
    /// durable head grows with write *activity* (one file per WAL flush),
    /// not with graph size — so an uncapped walk can cost minutes to
    /// derive a delta of a few edges. Past the cap the tail declines
    /// (`Unavailable`), which sends the incremental index build to the
    /// full rebuild and the read path to snapshot adjacency: both cheaper
    /// than the walk the cap refused.
    pub max_wal_tail_files: u64,
}

impl Default for GraphLimits {
    fn default() -> Self {
        Self {
            max_bulk_import_edges: 1_000_000,
            max_artifact_source_epochs: 10_000_000,
            max_traversal_hops: 16,
            max_artifact_build_edges: 10_000_000,
            max_query_result_vertices: 100_000,
            max_query_result_rows: 100_000,
            max_query_intermediate_rows: 250_000,
            max_query_index_candidates: 250_000,
            max_query_scan_edges: 1_000_000,
            max_query_runtime_ms: Some(30_000),
            // 2s against a 30s query budget: long enough to absorb a writer
            // handoff, where the new owner's reader is a manifest refresh
            // behind, and short enough that the remaining 28s still buys a
            // retry on another node instead of a budget kill.
            max_bookmark_wait_ms: 2_000,
            // ~4096 files at 16-way concurrency is ~13 s of tail fetches
            // against real S3 — below a full rebuild at the scales where
            // the incremental path is worth attempting at all.
            max_wal_tail_files: 4_096,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphBackpressurePolicy {
    pub max_concurrent_graph_writes: usize,
    pub max_concurrent_artifact_builds: usize,
    pub max_concurrent_gc_jobs: usize,
}

impl Default for GraphBackpressurePolicy {
    fn default() -> Self {
        Self {
            max_concurrent_graph_writes: 1,
            max_concurrent_artifact_builds: 1,
            max_concurrent_gc_jobs: 1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct GraphCacheConfig {
    /// Process-wide RAM budget shared by every SlateDB reader and writer that
    /// uses the same object-store handle. Zero disables SlateDB RAM admission.
    pub slatedb_cache_bytes: usize,
    pub object_store_cache_dir: Option<PathBuf>,
    pub object_store_cache_bytes: Option<usize>,
    /// Disk-cache fetch granularity. Smaller parts reduce cold probe overfetch
    /// but may increase subsequent cache misses. Must be a positive multiple
    /// of 1 KiB; tune for the workload before changing the 4 MiB default.
    pub object_store_cache_part_bytes: usize,
    /// File-handle budget assigned to this graph runtime's SlateDB disk cache.
    /// Scoped runtimes divide it across all possible readers and writers
    /// before opening them, making the graph-node value process-wide.
    pub object_store_cache_max_open_file_handles: usize,
    pub object_store_cache_puts: bool,
    pub preload_sst_on_startup: bool,
    /// Number of immutable SlateDB WAL files fetched concurrently while a
    /// reader opens or catches up. This is bounded independently per retained
    /// graph scope.
    pub reader_wal_replay_concurrency: usize,
}

impl Default for GraphCacheConfig {
    fn default() -> Self {
        Self {
            slatedb_cache_bytes: 640 * 1024 * 1024,
            object_store_cache_dir: None,
            object_store_cache_bytes: None,
            object_store_cache_part_bytes: 4 * 1024 * 1024,
            object_store_cache_max_open_file_handles: 512,
            object_store_cache_puts: false,
            preload_sst_on_startup: false,
            reader_wal_replay_concurrency: 16,
        }
    }
}

impl GraphCacheConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn disk_cache(cache_dir: impl Into<PathBuf>, max_cache_size_bytes: usize) -> Self {
        Self::disk_cache_with_preload(cache_dir, max_cache_size_bytes, true)
    }

    pub fn disk_cache_without_preload(
        cache_dir: impl Into<PathBuf>,
        max_cache_size_bytes: usize,
    ) -> Self {
        Self::disk_cache_with_preload(cache_dir, max_cache_size_bytes, false)
    }

    pub fn disk_cache_with_preload(
        cache_dir: impl Into<PathBuf>,
        max_cache_size_bytes: usize,
        preload_sst_on_startup: bool,
    ) -> Self {
        Self {
            object_store_cache_dir: Some(cache_dir.into()),
            object_store_cache_bytes: Some(max_cache_size_bytes),
            object_store_cache_puts: true,
            preload_sst_on_startup,
            ..Self::default()
        }
    }

    fn apply_to_settings(&self, settings: &mut Settings) {
        settings.object_store_cache_options.part_size_bytes = self.object_store_cache_part_bytes;
        settings.object_store_cache_options.max_open_file_handles =
            self.object_store_cache_max_open_file_handles;
        if let Some(cache_dir) = &self.object_store_cache_dir {
            settings.object_store_cache_options.root_folder = Some(cache_dir.clone());
        }
        if let Some(max_cache_size_bytes) = self.object_store_cache_bytes {
            settings.object_store_cache_options.max_cache_size_bytes = Some(max_cache_size_bytes);
        }
        settings.object_store_cache_options.cache_on_flush = self.object_store_cache_puts;
        settings.object_store_cache_options.cache_on_compaction = self.object_store_cache_puts;
        if self.preload_sst_on_startup {
            settings
                .object_store_cache_options
                .preload_disk_cache_on_startup = Some(PreloadLevel::AllSst);
        }
    }

    fn apply_to_reader_options(&self, options: &mut DbReaderOptions) {
        options.object_store_cache_options.part_size_bytes = self.object_store_cache_part_bytes;
        options.object_store_cache_options.max_open_file_handles =
            self.object_store_cache_max_open_file_handles;
        if let Some(cache_dir) = &self.object_store_cache_dir {
            options.object_store_cache_options.root_folder = Some(cache_dir.clone());
        }
        if let Some(max_cache_size_bytes) = self.object_store_cache_bytes {
            options.object_store_cache_options.max_cache_size_bytes = Some(max_cache_size_bytes);
        }
        options.object_store_cache_options.cache_on_flush = false;
        options.object_store_cache_options.cache_on_compaction = false;
        if self.preload_sst_on_startup {
            options
                .object_store_cache_options
                .preload_disk_cache_on_startup = Some(PreloadLevel::AllSst);
        }
    }
}

/// Non-exhaustive: construct with [`Default::default`] and assign the fields you
/// care about. Every field stays `pub`, so nothing is hidden — but embedders may
/// not use an exhaustive struct literal, which is what lets this crate add
/// options without breaking them.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct GraphOpenOptions {
    pub limits: GraphLimits,
    pub cache: GraphCacheConfig,
    pub durability: GraphDurabilityConfig,
    pub cache_policy: GraphCachePolicy,
    pub backpressure_policy: GraphBackpressurePolicy,
    pub index_policy: GraphIndexPolicy,
    /// Controls whether read-only SlateDB handles maintain GC-protecting
    /// checkpoints. Ordinary graph readers retain the managed default.
    pub reader_mode: GraphReaderMode,
    /// Frequency of SlateDB's background manifest refresh. Callers using a
    /// long interval must explicitly refresh before consistency-critical work.
    pub reader_manifest_poll_interval: Duration,
    /// How long a fenced writer waits before it may re-open — one heartbeat
    /// interval, sized so the rival has refreshed its view and stood down.
    ///
    /// Decision 5 of `docs/plans/2026-07-25-rendezvous-placement.md` fixes the
    /// default at 5s and the node config validates `interval < timeout` at
    /// startup. It is settable here so a test can pace a fence in milliseconds
    /// instead of sleeping through the production value.
    pub fence_backoff_interval: Duration,
}

// Hand-written rather than derived: `Duration::default()` is zero, and a zero
// fence wait would let a fenced writer re-open immediately, which is the exact
// behaviour touch point (d) exists to stop.
impl Default for GraphOpenOptions {
    fn default() -> Self {
        Self {
            limits: GraphLimits::default(),
            cache: GraphCacheConfig::default(),
            durability: GraphDurabilityConfig::default(),
            cache_policy: GraphCachePolicy::default(),
            backpressure_policy: GraphBackpressurePolicy::default(),
            index_policy: GraphIndexPolicy::default(),
            reader_mode: GraphReaderMode::default(),
            reader_manifest_poll_interval: GRAPH_READER_MANIFEST_POLL_INTERVAL,
            fence_backoff_interval: DEFAULT_FENCE_BACKOFF_INTERVAL,
        }
    }
}

/// Decision 5's heartbeat interval, which is also the fenced-writer wait.
pub const DEFAULT_FENCE_BACKOFF_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphMemoryConfig {
    pub storage: GraphStorageMemoryConfig,
    pub max_matrix_adjacency_bytes: usize,
    pub max_graphblas_bytes: usize,
    #[cfg(feature = "opencypher")]
    pub max_relationship_rows_bytes: usize,
    #[cfg(feature = "opencypher")]
    pub max_source_relationship_rows_bytes: usize,
    #[cfg(feature = "opencypher")]
    pub max_relationship_property_rows_bytes: usize,
    pub max_concurrent_matrix_compilations: usize,
}

impl Default for GraphMemoryConfig {
    fn default() -> Self {
        Self {
            storage: GraphStorageMemoryConfig::default(),
            max_matrix_adjacency_bytes: 0,
            max_graphblas_bytes: 128 * 1024 * 1024,
            #[cfg(feature = "opencypher")]
            max_relationship_rows_bytes: 8 * 1024 * 1024,
            #[cfg(feature = "opencypher")]
            max_source_relationship_rows_bytes: 8 * 1024 * 1024,
            #[cfg(feature = "opencypher")]
            max_relationship_property_rows_bytes: 16 * 1024 * 1024,
            max_concurrent_matrix_compilations: 1,
        }
    }
}

impl GraphMemoryConfig {
    pub fn low_memory() -> Self {
        Self {
            storage: GraphStorageMemoryConfig::low_memory(),
            max_matrix_adjacency_bytes: 0,
            max_graphblas_bytes: 32 * 1024 * 1024,
            #[cfg(feature = "opencypher")]
            max_relationship_rows_bytes: 2 * 1024 * 1024,
            #[cfg(feature = "opencypher")]
            max_source_relationship_rows_bytes: 2 * 1024 * 1024,
            #[cfg(feature = "opencypher")]
            max_relationship_property_rows_bytes: 4 * 1024 * 1024,
            ..Self::default()
        }
    }

    pub(crate) fn matrix_compilation_permits(&self) -> usize {
        self.max_concurrent_matrix_compilations.max(1)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphStorageMemoryConfig {
    pub l0_sst_size_bytes: usize,
    pub max_unflushed_bytes: usize,
    pub max_wal_flushes_before_l0_flush: u64,
    pub l0_flush_parallelism: usize,
}

impl Default for GraphStorageMemoryConfig {
    fn default() -> Self {
        Self {
            l0_sst_size_bytes: 16 * 1024 * 1024,
            max_unflushed_bytes: 64 * 1024 * 1024,
            max_wal_flushes_before_l0_flush: Self::DEFAULT_MAX_WAL_FLUSHES_BEFORE_L0_FLUSH,
            l0_flush_parallelism: 1,
        }
    }
}

impl GraphStorageMemoryConfig {
    /// Bounds sparse-write WAL fan-in to eight replay waves at the default
    /// reader replay concurrency of sixteen.
    pub const DEFAULT_MAX_WAL_FLUSHES_BEFORE_L0_FLUSH: u64 = 128;
    pub const MAX_WAL_FLUSHES_BEFORE_L0_FLUSH: u64 = 4_096;

    pub fn low_memory() -> Self {
        Self {
            l0_sst_size_bytes: 4 * 1024 * 1024,
            max_unflushed_bytes: 16 * 1024 * 1024,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.l0_sst_size_bytes == 0 {
            return Err(crate::GraphError::CorruptValue {
                key: "storage_memory/l0_sst_size_bytes".to_string(),
                reason: "L0 SST size must be greater than zero".to_string(),
            });
        }
        if self.max_unflushed_bytes < self.l0_sst_size_bytes {
            return Err(crate::GraphError::CorruptValue {
                key: "storage_memory/max_unflushed_bytes".to_string(),
                reason: format!(
                    "max unflushed bytes {} must be at least the L0 SST size {}",
                    self.max_unflushed_bytes, self.l0_sst_size_bytes
                ),
            });
        }
        if self.max_wal_flushes_before_l0_flush == 0 {
            return Err(crate::GraphError::CorruptValue {
                key: "storage_memory/max_wal_flushes_before_l0_flush".to_string(),
                reason: "maximum WAL flush count must be greater than zero".to_string(),
            });
        }
        if self.max_wal_flushes_before_l0_flush > Self::MAX_WAL_FLUSHES_BEFORE_L0_FLUSH {
            return Err(crate::GraphError::CorruptValue {
                key: "storage_memory/max_wal_flushes_before_l0_flush".to_string(),
                reason: format!(
                    "maximum WAL flush count must be at most {}",
                    Self::MAX_WAL_FLUSHES_BEFORE_L0_FLUSH
                ),
            });
        }
        if self.l0_flush_parallelism == 0 {
            return Err(crate::GraphError::CorruptValue {
                key: "storage_memory/l0_flush_parallelism".to_string(),
                reason: "L0 flush parallelism must be greater than zero".to_string(),
            });
        }
        Ok(())
    }

    fn apply_to_settings(&self, settings: &mut Settings) -> Result<()> {
        self.validate()?;
        settings.l0_sst_size_bytes = self.l0_sst_size_bytes;
        settings.max_unflushed_bytes = self.max_unflushed_bytes;
        settings.max_wal_flushes_before_l0_flush = self.max_wal_flushes_before_l0_flush;
        settings.l0_flush_parallelism = self.l0_flush_parallelism;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GraphIndexPolicy {
    #[default]
    Full,
    OutboundOnly,
}

impl GraphIndexPolicy {
    pub fn write_reverse_index(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphDurabilityConfig {
    pub wal_flush_interval_ms: Option<u64>,
    pub await_durable_writes: bool,
}

impl Default for GraphDurabilityConfig {
    fn default() -> Self {
        Self {
            wal_flush_interval_ms: Some(Self::DEFAULT_WAL_FLUSH_INTERVAL_MS),
            await_durable_writes: true,
        }
    }
}

impl GraphDurabilityConfig {
    pub const DEFAULT_WAL_FLUSH_INTERVAL_MS: u64 = 10;

    pub fn slatedb_default() -> Self {
        Self {
            wal_flush_interval_ms: Some(100),
            await_durable_writes: true,
        }
    }

    pub fn low_latency_durable(flush_interval_ms: u64) -> Self {
        Self {
            wal_flush_interval_ms: Some(flush_interval_ms.max(1)),
            await_durable_writes: true,
        }
    }

    pub fn with_await_durable_writes(mut self, await_durable_writes: bool) -> Self {
        self.await_durable_writes = await_durable_writes;
        self
    }

    fn apply_to_settings(&self, settings: &mut Settings) {
        settings.flush_interval = self.wal_flush_interval_ms.map(Duration::from_millis);
    }
}

pub(crate) async fn open_graph_db(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
    storage_memory: &GraphStorageMemoryConfig,
    durability: &GraphDurabilityConfig,
    db_cache: Arc<dyn slatedb::db_cache::DbCache>,
) -> Result<(Db, Arc<DefaultMetricsRecorder>)> {
    let mut settings = Settings::default();
    cache.apply_to_settings(&mut settings);
    storage_memory.apply_to_settings(&mut settings)?;
    durability.apply_to_settings(&mut settings);
    // SlateDB registers its internal counters/gauges (L0 SST count, bloom
    // filter hits, request counts) through whatever recorder it is handed.
    // Without one it uses a no-op recorder and those numbers never leave the
    // process. We keep the handle so `GraphStore::storage_metrics` can snapshot
    // them for `/metrics`.
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = Db::builder(path, object_store)
        .with_settings(settings)
        .with_deferred_compactor_startup(true)
        .with_filter_policies(crate::prefix_filter::graph_filter_policies())
        .with_db_cache(db_cache)
        .with_metrics_recorder(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
        .build()
        .await?;
    Ok((db, recorder))
}

pub(crate) async fn open_graph_reader(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
    mode: GraphReaderMode,
    manifest_poll_interval: Duration,
    db_cache: Arc<dyn slatedb::db_cache::DbCache>,
) -> Result<(DbReader, Arc<DefaultMetricsRecorder>)> {
    let (builder, recorder) = graph_reader_builder(
        path,
        object_store,
        cache,
        mode,
        manifest_poll_interval,
        db_cache,
    );
    Ok((builder.build().await?, recorder))
}

// Keep reader configuration shared when tests inject a clock before opening it.
pub(crate) fn graph_reader_builder(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
    mode: GraphReaderMode,
    manifest_poll_interval: Duration,
    db_cache: Arc<dyn slatedb::db_cache::DbCache>,
) -> (slatedb::DbReaderBuilder<Path>, Arc<DefaultMetricsRecorder>) {
    let mut options = DbReaderOptions {
        manifest_poll_interval,
        ..DbReaderOptions::default()
    };
    cache.apply_to_reader_options(&mut options);
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let builder = DbReader::builder(path.into(), object_store)
        .with_options(options)
        .with_filter_policies(crate::prefix_filter::graph_filter_policies())
        .with_db_cache(db_cache)
        .with_wal_replay_concurrency(cache.reader_wal_replay_concurrency)
        .with_reader_mode(mode.into())
        .with_metrics_recorder(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>);
    (builder, recorder)
}

/// Snapshot the SlateDB storage metrics for one scope's open handles.
///
/// A writer node also holds a reader for the same cell; both point at the same
/// object-store path and the same manifest. Gauges (`l0_sst_count`, mem size)
/// therefore describe the same underlying state on both, so we take the writer's
/// value when a writer is present and fall back to the reader's — summing would
/// double-count. Counters (`request_count`, flushes) record each handle's own
/// distinct activity, so those are summed.
pub(crate) fn collect_storage_metrics(
    writer: Option<&DefaultMetricsRecorder>,
    reader: Option<&DefaultMetricsRecorder>,
) -> crate::core::state::GraphStorageMetricsSnapshot {
    use slatedb::compactor::stats::{BYTES_COMPACTED, LAST_COMPACTION_TS_SEC, RUNNING_COMPACTIONS};
    use slatedb::db_cache_stats::ACCESS_COUNT;
    use slatedb::db_stats::{
        BACKPRESSURE_COUNT, FILTER_KIND_LABEL, FILTER_KIND_POINT, FILTER_KIND_PREFIX,
        IMMUTABLE_MEMTABLE_FLUSHES, L0_FLUSH_BYTES, L0_SST_COUNT, L0_STALL_COUNT,
        MEMTABLE_WRITE_BYTES, REQUEST_COUNT, SEGMENT_MAX_L0_SST_COUNT,
        SST_FILTER_FALSE_POSITIVE_COUNT, SST_FILTER_NEGATIVE_COUNT, SST_FILTER_POSITIVE_COUNT,
        TOTAL_MEM_SIZE_BYTES,
    };
    use slatedb::instrumented_object_store_stats::REQUEST_COUNT as OBJECT_STORE_REQUEST_COUNT;
    use slatedb::wal_buffer_stats::WAL_FLUSH_BYTES;
    use slatedb_common::metrics::MetricValue;

    let writer_snap = writer.map(DefaultMetricsRecorder::snapshot);
    let reader_snap = reader.map(DefaultMetricsRecorder::snapshot);

    let as_u64 = |value: &MetricValue| -> u64 {
        match value {
            MetricValue::Counter(v) => *v,
            MetricValue::Gauge(v) | MetricValue::UpDownCounter(v) => (*v).max(0) as u64,
            MetricValue::Histogram { .. } => 0,
        }
    };
    let read = |snap: &Option<slatedb_common::metrics::Metrics>,
                name: &str,
                labels: &[(&str, &str)]|
     -> Option<u64> {
        let snap = snap.as_ref()?;
        let metric = if labels.is_empty() {
            snap.by_name(name).into_iter().next()
        } else {
            snap.by_name_and_labels(name, labels)
        }?;
        Some(as_u64(&metric.value))
    };
    // Gauge: writer preferred, reader fallback (same manifest, do not sum).
    let gauge = |name: &str| -> u64 {
        read(&writer_snap, name, &[])
            .or_else(|| read(&reader_snap, name, &[]))
            .unwrap_or(0)
    };
    // Counter: distinct per handle, sum both.
    let counter = |name: &str, labels: &[(&str, &str)]| -> u64 {
        read(&writer_snap, name, labels).unwrap_or(0)
            + read(&reader_snap, name, labels).unwrap_or(0)
    };
    // Some families fan out over labels we do not want to preserve —
    // `worker_id` on compactor counters, `(component, store_type, api)` on
    // object-store counters, the two `type`s of L0 stall. Sum every series of
    // the name whose labels include `required`, on both handles.
    let counter_sum = |name: &str, required: &[(&str, &str)]| -> u64 {
        let sum_one = |snap: &Option<slatedb_common::metrics::Metrics>| -> u64 {
            snap.as_ref().map_or(0, |snap| {
                snap.by_name(name)
                    .into_iter()
                    .filter(|metric| {
                        required.iter().all(|(key, value)| {
                            metric.labels.iter().any(|(k, v)| k == key && v == value)
                        })
                    })
                    .map(|metric| as_u64(&metric.value))
                    .sum()
            })
        };
        sum_one(&writer_snap) + sum_one(&reader_snap)
    };
    let filter_kind =
        |name: &str, kind: &str| -> u64 { counter(name, &[(FILTER_KIND_LABEL, kind)]) };

    crate::core::state::GraphStorageMetricsSnapshot {
        l0_sst_count: gauge(L0_SST_COUNT),
        segment_max_l0_sst_count: gauge(SEGMENT_MAX_L0_SST_COUNT),
        immutable_memtable_flushes: counter(IMMUTABLE_MEMTABLE_FLUSHES, &[]),
        get_requests: counter(REQUEST_COUNT, &[("op", "get")]),
        scan_requests: counter(REQUEST_COUNT, &[("op", "scan")]),
        total_mem_size_bytes: gauge(TOTAL_MEM_SIZE_BYTES),
        sst_filter_point_positives: filter_kind(SST_FILTER_POSITIVE_COUNT, FILTER_KIND_POINT),
        sst_filter_point_negatives: filter_kind(SST_FILTER_NEGATIVE_COUNT, FILTER_KIND_POINT),
        sst_filter_point_false_positives: filter_kind(
            SST_FILTER_FALSE_POSITIVE_COUNT,
            FILTER_KIND_POINT,
        ),
        sst_filter_prefix_positives: filter_kind(SST_FILTER_POSITIVE_COUNT, FILTER_KIND_PREFIX),
        sst_filter_prefix_negatives: filter_kind(SST_FILTER_NEGATIVE_COUNT, FILTER_KIND_PREFIX),
        sst_filter_prefix_false_positives: filter_kind(
            SST_FILTER_FALSE_POSITIVE_COUNT,
            FILTER_KIND_PREFIX,
        ),
        backpressure_writes: counter(BACKPRESSURE_COUNT, &[]),
        l0_write_stalls: counter_sum(L0_STALL_COUNT, &[]),
        compaction_bytes: counter_sum(BYTES_COMPACTED, &[]),
        // The compactor only runs inside the writer `Db`; summing the reader in
        // is harmless (its series do not exist) and keeps the closure uniform.
        running_compactions: counter_sum(RUNNING_COMPACTIONS, &[]),
        last_compaction_timestamp_sec: gauge(LAST_COMPACTION_TS_SEC),
        memtable_write_bytes: counter(MEMTABLE_WRITE_BYTES, &[]),
        wal_flush_bytes: counter(WAL_FLUSH_BYTES, &[]),
        l0_flush_bytes: counter(L0_FLUSH_BYTES, &[]),
        block_cache_data_hits: counter(
            ACCESS_COUNT,
            &[("entry_kind", "data_block"), ("result", "hit")],
        ),
        block_cache_data_misses: counter(
            ACCESS_COUNT,
            &[("entry_kind", "data_block"), ("result", "miss")],
        ),
        block_cache_filter_hits: counter(
            ACCESS_COUNT,
            &[("entry_kind", "filter"), ("result", "hit")],
        ),
        block_cache_filter_misses: counter(
            ACCESS_COUNT,
            &[("entry_kind", "filter"), ("result", "miss")],
        ),
        object_store_get_requests: counter_sum(OBJECT_STORE_REQUEST_COUNT, &[("op", "get")]),
        object_store_put_requests: counter_sum(OBJECT_STORE_REQUEST_COUNT, &[("op", "put")]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use slatedb::object_store::memory::InMemory;

    #[test]
    fn disk_cache_file_handle_budget_reaches_readers_and_writers() {
        let cache = GraphCacheConfig {
            object_store_cache_max_open_file_handles: 17,
            ..GraphCacheConfig::default()
        };
        let mut settings = Settings::default();
        cache.apply_to_settings(&mut settings);
        assert_eq!(
            settings.object_store_cache_options.max_open_file_handles,
            17
        );

        let mut reader = DbReaderOptions::default();
        cache.apply_to_reader_options(&mut reader);
        assert_eq!(reader.object_store_cache_options.max_open_file_handles, 17);
    }

    fn test_db_cache(object_store: &Arc<dyn ObjectStore>) -> Arc<dyn slatedb::db_cache::DbCache> {
        crate::process_slate_db_cache(
            object_store,
            GraphCacheConfig::default().slatedb_cache_bytes,
        )
        .unwrap()
    }

    async fn object_locations(object_store: &Arc<dyn ObjectStore>) -> Vec<String> {
        let mut locations = object_store
            .list(None)
            .map(|result| result.unwrap().location.to_string())
            .collect::<Vec<_>>()
            .await;
        locations.sort();
        locations
    }

    #[test]
    fn storage_memory_profiles_are_valid_and_bounded() {
        let durability = GraphDurabilityConfig::default();
        assert_eq!(durability.wal_flush_interval_ms, Some(10));
        assert!(durability.await_durable_writes);
        assert_eq!(
            GraphCacheConfig::default().reader_wal_replay_concurrency,
            16
        );
        assert_eq!(
            GraphCacheConfig::default().slatedb_cache_bytes,
            640 * 1024 * 1024
        );
        let balanced = GraphStorageMemoryConfig::default();
        balanced.validate().unwrap();
        assert_eq!(balanced.l0_sst_size_bytes, 16 * 1024 * 1024);
        assert_eq!(balanced.max_unflushed_bytes, 64 * 1024 * 1024);
        assert_eq!(balanced.max_wal_flushes_before_l0_flush, 128);

        let low_memory = GraphStorageMemoryConfig::low_memory();
        low_memory.validate().unwrap();
        assert_eq!(low_memory.l0_sst_size_bytes, 4 * 1024 * 1024);
        assert_eq!(low_memory.max_unflushed_bytes, 16 * 1024 * 1024);

        let memory = GraphMemoryConfig::low_memory();
        assert_eq!(memory.max_matrix_adjacency_bytes, 0);
        assert_eq!(memory.max_graphblas_bytes, 32 * 1024 * 1024);
        #[cfg(feature = "opencypher")]
        {
            assert_eq!(memory.max_relationship_rows_bytes, 2 * 1024 * 1024);
            assert_eq!(memory.max_source_relationship_rows_bytes, 2 * 1024 * 1024);
            assert_eq!(memory.max_relationship_property_rows_bytes, 4 * 1024 * 1024);
        }
    }

    /// End-to-end proof that the recorder wiring carries real numbers: a live
    /// store, real writes, a flush, point gets and a prefix scan, and then the
    /// snapshot must show them. The 1,200 keys matter — SlateDB only builds a
    /// bloom filter for SSTs with at least `min_filter_keys` (1,000) entries,
    /// and the filter-outcome counters are the series the prefix-scan work is
    /// judged by.
    #[tokio::test]
    async fn storage_metrics_flow_from_a_live_store() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("graph/storage-metrics-flow");
        let db_cache = crate::process_slate_db_cache(&object_store, 1024 * 1024).unwrap();
        let (writer, recorder) = open_graph_db(
            path,
            Arc::clone(&object_store),
            &GraphCacheConfig::default(),
            &GraphStorageMemoryConfig::default(),
            &GraphDurabilityConfig::default(),
            Arc::clone(&db_cache) as Arc<dyn slatedb::db_cache::DbCache>,
        )
        .await
        .unwrap();

        // One batch, not 1,200 individual puts: each put WAL-flushes, and every
        // `max_wal_flushes_before_l0_flush` of those rotates the memtable,
        // which would shred the keys across ~8 SSTs of ~150 keys each — all
        // below `min_filter_keys`, so none would carry a filter. (Production
        // relationship imports write batches, so this is also the honest
        // shape.)
        let mut batch = slatedb::WriteBatch::new();
        for i in 0..1200u32 {
            batch.put(format!("rprop_idx/{i:06}").as_bytes(), b"v");
        }
        writer.write(batch).await.unwrap();
        // `Db::flush()` only flushes the WAL when the WAL is enabled; the reads
        // below must hit an L0 SST for the filter counters to move, so force
        // the memtable down explicitly.
        writer
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();

        writer.get(b"rprop_idx/000500").await.unwrap();
        assert_eq!(writer.get(b"absent/nothing").await.unwrap(), None);
        let mut iter = writer
            .scan_prefix(b"rprop_idx/0005".as_slice(), ..)
            .await
            .unwrap();
        let mut scanned = 0;
        while iter.next().await.unwrap().is_some() {
            scanned += 1;
        }
        assert_eq!(scanned, 100, "rprop_idx/0005xx spans keys 000500–000599");

        let snapshot = collect_storage_metrics(Some(&recorder), None);
        assert!(snapshot.get_requests >= 2, "{snapshot:?}");
        assert!(snapshot.scan_requests >= 1, "{snapshot:?}");
        assert!(snapshot.immutable_memtable_flushes >= 1, "{snapshot:?}");
        assert!(snapshot.memtable_write_bytes > 0, "{snapshot:?}");
        assert!(snapshot.object_store_put_requests > 0, "{snapshot:?}");
        // The flushed SST has a filter, so the point gets consulted it …
        assert!(
            snapshot.sst_filter_point_positives + snapshot.sst_filter_point_negatives >= 1,
            "{snapshot:?}"
        );
        // … and so did the prefix scan, which the default bloom policy can
        // never say no to (no prefix extractor): every consult is a positive,
        // never a negative. This asymmetry is the entire write-cost story.
        assert!(snapshot.sst_filter_prefix_positives >= 1, "{snapshot:?}");
        assert_eq!(snapshot.sst_filter_prefix_negatives, 0, "{snapshot:?}");
        let cache = db_cache.snapshot();
        assert_eq!(cache.capacity_bytes, 1024 * 1024);
        assert!(cache.resident_bytes > 0, "{cache:?}");
        assert!(cache.resident_bytes <= cache.capacity_bytes, "{cache:?}");
        assert!(cache.entries > 0, "{cache:?}");

        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn follow_latest_reader_does_not_mutate_object_storage() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("graph/follow-latest-no-checkpoint");
        let (writer, _recorder) = open_graph_db(
            path.clone(),
            Arc::clone(&object_store),
            &GraphCacheConfig::default(),
            &GraphStorageMemoryConfig::default(),
            &GraphDurabilityConfig::default(),
            test_db_cache(&object_store),
        )
        .await
        .unwrap();
        writer.put(b"seed", b"value").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        let before = object_locations(&object_store).await;
        let (reader, _recorder) = open_graph_reader(
            path,
            Arc::clone(&object_store),
            &GraphCacheConfig::default(),
            GraphReaderMode::FollowLatest,
            Duration::from_secs(60 * 60),
            test_db_cache(&object_store),
        )
        .await
        .unwrap();
        reader.refresh().await.unwrap();
        assert_eq!(
            reader.get(b"seed").await.unwrap().as_deref(),
            Some(&b"value"[..])
        );
        reader.close().await.unwrap();

        assert_eq!(object_locations(&object_store).await, before);
    }

    #[tokio::test]
    async fn follow_latest_reader_observes_later_writes_after_explicit_refresh() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("graph/follow-latest-explicit-refresh");
        let (seed, _recorder) = open_graph_db(
            path.clone(),
            Arc::clone(&object_store),
            &GraphCacheConfig::default(),
            &GraphStorageMemoryConfig::default(),
            &GraphDurabilityConfig::default(),
            test_db_cache(&object_store),
        )
        .await
        .unwrap();
        seed.put(b"seed", b"value").await.unwrap();
        seed.flush().await.unwrap();
        seed.close().await.unwrap();

        let (reader, _recorder) = open_graph_reader(
            path.clone(),
            Arc::clone(&object_store),
            &GraphCacheConfig::default(),
            GraphReaderMode::FollowLatest,
            Duration::from_secs(60 * 60),
            test_db_cache(&object_store),
        )
        .await
        .unwrap();
        let (writer, _recorder) = open_graph_db(
            path,
            Arc::clone(&object_store),
            &GraphCacheConfig::default(),
            &GraphStorageMemoryConfig::default(),
            &GraphDurabilityConfig::default(),
            test_db_cache(&object_store),
        )
        .await
        .unwrap();
        writer.put(b"later", b"durable").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        reader.refresh().await.unwrap();
        assert_eq!(
            reader.get(b"later").await.unwrap().as_deref(),
            Some(&b"durable"[..])
        );
        reader.close().await.unwrap();
    }

    #[test]
    fn storage_memory_rejects_invalid_limits() {
        let mut config = GraphStorageMemoryConfig::default();
        config.max_unflushed_bytes = config.l0_sst_size_bytes - 1;
        assert!(config.validate().is_err());

        let config = GraphStorageMemoryConfig {
            max_wal_flushes_before_l0_flush: 0,
            ..GraphStorageMemoryConfig::default()
        };
        assert!(config.validate().is_err());

        let config = GraphStorageMemoryConfig {
            max_wal_flushes_before_l0_flush: 4_097,
            ..GraphStorageMemoryConfig::default()
        };
        assert!(config.validate().is_err());

        let config = GraphStorageMemoryConfig {
            l0_flush_parallelism: 0,
            ..GraphStorageMemoryConfig::default()
        };
        assert!(config.validate().is_err());
    }

    /// `GraphOpenOptions` and `GraphCachePolicy` are `#[non_exhaustive]`, so
    /// embedders construct them from `default()` and assign what they need.
    /// That is deliberate: it is what lets this crate add an option without
    /// breaking every downstream struct literal.
    ///
    /// This test pins the supported shape. It does **not** need editing when a
    /// field is added — if it ever does, the field was added in a way that
    /// breaks embedders, and that is the thing to reconsider.
    // `#[non_exhaustive]` is inert inside the defining crate, so clippy still
    // suggests the struct literal here. Embedders cannot use one, which is the
    // whole point of the test.
    #[allow(clippy::field_reassign_with_default)]
    #[test]
    fn public_options_stay_constructible_without_exhaustive_literals() {
        let mut cache_policy = GraphCachePolicy::default();
        cache_policy.max_matrix_artifacts = 1;
        cache_policy.max_graphblas_matrices = 1;
        cache_policy.sparse_kernel = crate::SparseKernelBackend::Adjacency;

        let mut options = GraphOpenOptions::default();
        options.limits = GraphLimits::default();
        options.cache_policy = cache_policy.clone();
        options.index_policy = GraphIndexPolicy::default();

        // Every field stays `pub`: nothing is hidden, only the literal is.
        assert_eq!(options.cache_policy, cache_policy);
        assert_eq!(options.reader_mode, GraphReaderMode::ManagedCheckpoint);
        assert_eq!(
            options.reader_manifest_poll_interval,
            GRAPH_READER_MANIFEST_POLL_INTERVAL
        );
        assert_eq!(
            options.cache_policy.sparse_kernel,
            crate::SparseKernelBackend::Adjacency
        );
    }
}
