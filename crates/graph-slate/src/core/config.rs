use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[allow(deprecated)] // `PreloadLevel` configures the deprecated `CachedObjectStore`.
use slatedb::config::PreloadLevel;
use slatedb::config::{CompressionCodec, DbReaderOptions, Settings};
use slatedb::object_store::{path::Path, ObjectStore};
use slatedb::{Db, DbReader, DbReaderMode};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricsRecorder};

use crate::{GraphCachePolicy, Result, StorageSequence};

pub const DEFAULT_TRUSTED_APPEND_CHUNK_EDGES: usize = 4_096;

/// Block compression for every SST a graph store writes: memtable flushes,
/// compaction output and offline import images alike. Graph keys are text with
/// long shared prefixes and zero-padded ids, so they compress well; see
/// the graph storage footprint analysis for the measured size and
/// read latency. Each SST records its own codec, so stores written before this
/// was set stay readable and are compressed as compaction rewrites them.
pub const GRAPH_SST_COMPRESSION_CODEC: Option<CompressionCodec> = Some(CompressionCodec::Zstd);

/// Largest SST, in stored (compressed) bytes, that compaction and offline
/// import write. A reader loads an SST's whole block index on its first read
/// of that SST, and compression packs about three times as many blocks into
/// each stored byte, so a size picked for uncompressed SSTs would triple the
/// index a cache miss has to fetch and decode. 64 MiB keeps that reload
/// smaller than it was before compression.
pub const GRAPH_SST_TARGET_BYTES: usize = 64 * 1024 * 1024;
const GRAPH_READER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(10);

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
    /// Vertices one GDS write-back transaction writes, and index entries one
    /// sweep transaction deletes: the batch the storage engine commits at
    /// once. The trade is memory held per chunk, two keys per vertex in the
    /// transaction's write set, against the number of commits a write-back
    /// makes and how the writer's backpressure meets them: a small chunk
    /// commits often and lets the unflushed bytes drain between commits,
    /// a large one commits rarely and may cross the writer's unflushed
    /// threshold inside a single commit. Capped by `max_bulk_import_edges`
    /// at use; zero is taken as one.
    pub write_back_chunk: usize,
    /// How long no write-back may have run before a detached sweep of a
    /// superseded write-back index generation starts, in milliseconds. A
    /// sweep that shares the writer with a write-back slows both: its
    /// tombstones and the write-back's puts meet in the same memtables and
    /// the writer's backpressure stalls them together. The calls of a batch
    /// of algorithms follow one another by a client round-trip, so a grace
    /// longer than that keeps every sweep out until the batch ends. Zero
    /// starts a sweep as soon as no write-back is in flight.
    pub write_back_sweep_grace_ms: u64,
    pub max_artifact_source_epochs: StorageSequence,
    pub max_traversal_hops: u8,
    pub max_artifact_build_edges: u64,
    pub max_query_result_vertices: usize,
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
    /// losing that race to the error that hides it, which is why reads saw
    /// 17–38s "prepare" times and essentially no `SnapshotAhead`. Change 3 of
    /// the cell-affine read routing design.
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
    /// than the walk the cap refused. A type whose change stamp has not
    /// moved past its generation is never walked, so the cap binds only
    /// while the indexer is behind on that type's own edge changes.
    pub max_wal_tail_files: u64,
}

impl Default for GraphLimits {
    fn default() -> Self {
        Self {
            max_bulk_import_edges: 1_000_000,
            write_back_chunk: 4_096,
            write_back_sweep_grace_ms: 1_000,
            max_artifact_source_epochs: 10_000_000,
            max_traversal_hops: 16,
            max_artifact_build_edges: 10_000_000,
            max_query_result_vertices: 100_000,
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
    /// Opt-in bounded cache measurement (ENG-3466). Off by default; when
    /// off, no observer exists and the per-op cost is one `Option` check.
    pub slatedb_cache_observe: bool,
    pub slatedb_cache_observe_max_resident_records: usize,
    pub slatedb_cache_observe_max_ghost_records: usize,
    pub slatedb_cache_observe_max_metadata_bytes: usize,
    pub slatedb_cache_observe_ghost_retention_secs: u64,
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
            slatedb_cache_observe: false,
            slatedb_cache_observe_max_resident_records: 65_536,
            slatedb_cache_observe_max_ghost_records: 65_536,
            slatedb_cache_observe_max_metadata_bytes: 16 * 1024 * 1024,
            slatedb_cache_observe_ghost_retention_secs: 3600,
        }
    }
}

impl GraphCacheConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Build the process-cache config, including the opt-in observer. The
    /// observer is `Some` only when `slatedb_cache_observe` is set.
    pub fn slatedb_cache_config(&self) -> crate::SharedSlateDbCacheConfig {
        let observe = self.slatedb_cache_observe.then(|| crate::ObserveConfig {
            max_resident_records: self.slatedb_cache_observe_max_resident_records.max(1),
            max_ghost_records: self.slatedb_cache_observe_max_ghost_records.max(1),
            max_metadata_bytes: self.slatedb_cache_observe_max_metadata_bytes.max(1),
            ghost_retention: std::time::Duration::from_secs(self.slatedb_cache_observe_ghost_retention_secs),
        });
        crate::SharedSlateDbCacheConfig {
            capacity_bytes: self.slatedb_cache_bytes,
            observe,
        }
    }

    pub fn disk_cache(cache_dir: impl Into<PathBuf>, max_cache_size_bytes: usize) -> Self {
        Self::disk_cache_with_preload(cache_dir, max_cache_size_bytes, true)
    }

    pub fn disk_cache_without_preload(cache_dir: impl Into<PathBuf>, max_cache_size_bytes: usize) -> Self {
        Self::disk_cache_with_preload(cache_dir, max_cache_size_bytes, false)
    }

    pub fn disk_cache_with_preload(cache_dir: impl Into<PathBuf>, max_cache_size_bytes: usize, preload_sst_on_startup: bool) -> Self {
        Self {
            object_store_cache_dir: Some(cache_dir.into()),
            object_store_cache_bytes: Some(max_cache_size_bytes),
            object_store_cache_puts: true,
            preload_sst_on_startup,
            ..Self::default()
        }
    }

    // SlateDB 0.18 deprecates `object_store_cache_options` and `CachedObjectStore` in favour of
    // `slatedb_mirror::ObjectStoreMirror`. Graph nodes use the shared disk cache instead. The move
    // to the mirror is separate work.
    #[allow(deprecated)]
    fn apply_to_settings(&self, settings: &mut Settings) {
        settings.object_store_cache_options.part_size_bytes = self.object_store_cache_part_bytes;
        settings.object_store_cache_options.max_open_file_handles = self.object_store_cache_max_open_file_handles;
        if let Some(cache_dir) = &self.object_store_cache_dir {
            settings.object_store_cache_options.root_folder = Some(cache_dir.clone());
        }
        if let Some(max_cache_size_bytes) = self.object_store_cache_bytes {
            settings.object_store_cache_options.max_cache_size_bytes = Some(max_cache_size_bytes);
        }
        settings.object_store_cache_options.cache_on_flush = self.object_store_cache_puts;
        settings.object_store_cache_options.cache_on_compaction = self.object_store_cache_puts;
        if self.preload_sst_on_startup {
            settings.object_store_cache_options.preload_disk_cache_on_startup = Some(PreloadLevel::AllSst);
        }
    }

    // SlateDB 0.18 deprecates `object_store_cache_options` and `CachedObjectStore` in favour of
    // `slatedb_mirror::ObjectStoreMirror`. Graph nodes use the shared disk cache instead. The move
    // to the mirror is separate work.
    #[allow(deprecated)]
    fn apply_to_reader_options(&self, options: &mut DbReaderOptions) {
        options.object_store_cache_options.part_size_bytes = self.object_store_cache_part_bytes;
        options.object_store_cache_options.max_open_file_handles = self.object_store_cache_max_open_file_handles;
        if let Some(cache_dir) = &self.object_store_cache_dir {
            options.object_store_cache_options.root_folder = Some(cache_dir.clone());
        }
        if let Some(max_cache_size_bytes) = self.object_store_cache_bytes {
            options.object_store_cache_options.max_cache_size_bytes = Some(max_cache_size_bytes);
        }
        options.object_store_cache_options.cache_on_flush = false;
        options.object_store_cache_options.cache_on_compaction = false;
        if self.preload_sst_on_startup {
            options.object_store_cache_options.preload_disk_cache_on_startup = Some(PreloadLevel::AllSst);
        }
    }
}

/// Construct with [`Default::default`] and assign the options to override.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// Decision 5 of the rendezvous placement design fixes the
    /// default at 5s and the node config validates `interval < timeout` at
    /// startup. It is settable here so a test can pace a fence in milliseconds
    /// instead of sleeping through the production value.
    pub fence_backoff_interval: Duration,
    /// Whether the experimental planner loads the indexer-published planner
    /// statistics, and how long a loaded copy is reused.
    pub planner_statistics: PlannerStatisticsConfig,
    /// Peers' writer frontiers (`GRAPH_BUDDY_WAL`). With a feed, each reader
    /// follows its writer's frontier instead of polling object storage for
    /// news; without one, readers behave as before.
    pub wal_feed: Option<crate::WalFeed>,
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
            planner_statistics: PlannerStatisticsConfig::default(),
            wal_feed: None,
        }
    }
}

/// The background manifest poll of a [`GraphOpenOptions::follow_latest_reader`]
/// handle: effectively never, since its users refresh before each read pass.
pub const FOLLOW_LATEST_READER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

impl GraphOpenOptions {
    /// Options for a shard opened read-only (`GraphShard::open_with_options`)
    /// that must never write to the store it reads: the indexer's cached
    /// readers, and tools pointed at a published store.
    ///
    /// `FollowLatest` makes the reader read-only at the object-store level.
    /// The default `ManagedCheckpoint` creates and refreshes a GC-protecting
    /// checkpoint, a write per scope that, across thousands of cached scopes,
    /// is thousands of periodic S3 writes, and on a published source is a
    /// change to it. Background polls would duplicate the explicit refresh
    /// these readers do before each pass, so the poll interval is a day.
    pub fn follow_latest_reader(max_wal_tail_files: u64, slatedb_cache_bytes: usize) -> Self {
        let mut options = Self {
            limits: GraphLimits {
                max_wal_tail_files,
                ..GraphLimits::default()
            },
            reader_mode: GraphReaderMode::FollowLatest,
            reader_manifest_poll_interval: FOLLOW_LATEST_READER_MANIFEST_POLL_INTERVAL,
            ..Self::default()
        };
        options.cache.slatedb_cache_bytes = slatedb_cache_bytes;
        options
    }
}

/// `GRAPH_PLANNER_STATISTICS` and `GRAPH_PLANNER_STATISTICS_TTL_MS` on the node.
///
/// Off by default: a disabled shard never reads the statistics object. On, a
/// shard keeps each cell's object in memory for `ttl` before reading it again.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannerStatisticsConfig {
    pub enabled: bool,
    pub ttl: Duration,
    /// Experimental queries a cell plans without statistics, within one
    /// TTL, before the shard asks the indexer to build them. Zero never
    /// asks. `GRAPH_PLANNER_STATISTICS_HINT_THRESHOLD`.
    pub hint_threshold: u32,
}

impl PlannerStatisticsConfig {
    pub const DEFAULT_TTL: Duration = Duration::from_secs(60);
    pub const DEFAULT_HINT_THRESHOLD: u32 = 5;
}

impl Default for PlannerStatisticsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ttl: Self::DEFAULT_TTL,
            hint_threshold: Self::DEFAULT_HINT_THRESHOLD,
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
    /// Assembled gds graphs kept between a project call and the algorithm
    /// calls on it: 8 bytes per projected vertex and 16 per projected edge.
    /// A graph over the limit is assembled by every call instead.
    #[cfg(feature = "opencypher")]
    pub max_gds_projected_graph_bytes: usize,
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
            #[cfg(feature = "opencypher")]
            max_gds_projected_graph_bytes: 256 * 1024 * 1024,
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
            #[cfg(feature = "opencypher")]
            max_gds_projected_graph_bytes: 32 * 1024 * 1024,
            ..Self::default()
        }
    }

    pub fn matrix_compilation_permits(&self) -> usize {
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
                reason: format!("maximum WAL flush count must be at most {}", Self::MAX_WAL_FLUSHES_BEFORE_L0_FLUSH),
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

/// The SlateDB settings every graph writer opens with. Separate from
/// [`open_graph_db`] so tests can check what the writer and its embedded
/// compactor are handed without writing enough data to observe it.
fn graph_writer_settings(
    cache: &GraphCacheConfig,
    storage_memory: &GraphStorageMemoryConfig,
    durability: &GraphDurabilityConfig,
) -> Result<Settings> {
    let mut settings = Settings {
        compression_codec: GRAPH_SST_COMPRESSION_CODEC,
        ..Settings::default()
    };
    if let Some(worker) = settings.compactor_options.as_mut().and_then(|compactor| compactor.worker.as_mut()) {
        worker.max_sst_size = GRAPH_SST_TARGET_BYTES;
    }
    cache.apply_to_settings(&mut settings);
    storage_memory.apply_to_settings(&mut settings)?;
    durability.apply_to_settings(&mut settings);
    Ok(settings)
}

pub(crate) async fn open_graph_db(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
    storage_memory: &GraphStorageMemoryConfig,
    durability: &GraphDurabilityConfig,
    db_cache: Arc<dyn slatedb::db_cache::DbCache>,
) -> Result<(Db, Arc<DefaultMetricsRecorder>)> {
    let settings = graph_writer_settings(cache, storage_memory, durability)?;
    // SlateDB registers its internal counters/gauges (L0 SST count, bloom
    // filter hits, request counts) through whatever recorder it is handed.
    // Without one it uses a no-op recorder and those numbers never leave the
    // process. We keep the handle so `SlateGraphStore::storage_metrics` can snapshot
    // them for `/metrics`.
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = Db::builder(path, object_store)
        .with_settings(settings)
        .with_deferred_compactor_startup(true)
        .with_filter_policies(crate::prefix_filter::graph_filter_policies())
        .with_db_cache(db_cache, crate::core::db_cache::next_db_cache_id())
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
    let (builder, recorder) = graph_reader_builder(path, object_store, cache, mode, manifest_poll_interval, db_cache);
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
        .with_db_cache(db_cache, crate::core::db_cache::next_db_cache_id())
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
    use slatedb::cached_object_store_stats::{
        CACHE_BYTES as DISK_CACHE_BYTES, EVICTED_BYTES as DISK_CACHE_EVICTED_BYTES, PART_ACCESS_COUNT as DISK_CACHE_PART_ACCESSES,
        PART_HIT_COUNT as DISK_CACHE_PART_HITS,
    };
    use slatedb::compactor::stats::{BYTES_COMPACTED, LAST_COMPACTION_TS_SEC, RUNNING_COMPACTIONS};
    use slatedb::db_cache_stats::ACCESS_COUNT;
    use slatedb::db_stats::{
        BACKPRESSURE_COUNT, FILTER_KIND_LABEL, FILTER_KIND_POINT, FILTER_KIND_PREFIX, IMMUTABLE_MEMTABLE_FLUSHES, L0_FLUSH_BYTES,
        L0_SST_COUNT, L0_STALL_COUNT, MEMTABLE_WRITE_BYTES, REQUEST_COUNT, SEGMENT_MAX_L0_SST_COUNT, SORTED_RUN_COUNT, SST_COUNT,
        SST_FILTER_FALSE_POSITIVE_COUNT, SST_FILTER_NEGATIVE_COUNT, SST_FILTER_POSITIVE_COUNT, TOTAL_MEM_SIZE_BYTES,
    };
    use slatedb::instrumented_object_store_stats::REQUEST_COUNT as OBJECT_STORE_REQUEST_COUNT;
    use slatedb::wal_buffer_stats::{WAL_BUFFER_FLUSHES, WAL_BUFFER_FLUSH_REQUESTS, WAL_FLUSH_BYTES};
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
    let read = |snap: &Option<slatedb_common::metrics::Metrics>, name: &str, labels: &[(&str, &str)]| -> Option<u64> {
        let snap = snap.as_ref()?;
        let metric = if labels.is_empty() {
            snap.by_name(name).into_iter().next()
        } else {
            snap.by_name_and_labels(name, labels)
        }?;
        Some(as_u64(&metric.value))
    };
    // Gauge: writer preferred, reader fallback (same manifest, do not sum).
    let gauge = |name: &str| -> u64 { read(&writer_snap, name, &[]).or_else(|| read(&reader_snap, name, &[])).unwrap_or(0) };
    // Counter: distinct per handle, sum both.
    let counter = |name: &str, labels: &[(&str, &str)]| -> u64 {
        read(&writer_snap, name, labels).unwrap_or(0) + read(&reader_snap, name, labels).unwrap_or(0)
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
                        required
                            .iter()
                            .all(|(key, value)| metric.labels.iter().any(|(k, v)| k == key && v == value))
                    })
                    .map(|metric| as_u64(&metric.value))
                    .sum()
            })
        };
        sum_one(&writer_snap) + sum_one(&reader_snap)
    };
    let filter_kind = |name: &str, kind: &str| -> u64 { counter(name, &[(FILTER_KIND_LABEL, kind)]) };
    let requests_by_component = |op: &str| {
        let requests = |component: &str| counter_sum(OBJECT_STORE_REQUEST_COUNT, &[("op", op), ("component", component)]);
        let [db, reader, compactor, gc] = crate::core::state::ObjectStoreRequestsByComponent::COMPONENTS;
        crate::core::state::ObjectStoreRequestsByComponent {
            db: requests(db),
            reader: requests(reader),
            compactor: requests(compactor),
            gc: requests(gc),
        }
    };

    crate::core::state::GraphStorageMetricsSnapshot {
        l0_sst_count: gauge(L0_SST_COUNT),
        sorted_run_count: gauge(SORTED_RUN_COUNT),
        sst_count: gauge(SST_COUNT),
        segment_max_l0_sst_count: gauge(SEGMENT_MAX_L0_SST_COUNT),
        immutable_memtable_flushes: counter(IMMUTABLE_MEMTABLE_FLUSHES, &[]),
        get_requests: counter(REQUEST_COUNT, &[("op", "get")]),
        scan_requests: counter(REQUEST_COUNT, &[("op", "scan")]),
        total_mem_size_bytes: gauge(TOTAL_MEM_SIZE_BYTES),
        sst_filter_point_positives: filter_kind(SST_FILTER_POSITIVE_COUNT, FILTER_KIND_POINT),
        sst_filter_point_negatives: filter_kind(SST_FILTER_NEGATIVE_COUNT, FILTER_KIND_POINT),
        sst_filter_point_false_positives: filter_kind(SST_FILTER_FALSE_POSITIVE_COUNT, FILTER_KIND_POINT),
        sst_filter_prefix_positives: filter_kind(SST_FILTER_POSITIVE_COUNT, FILTER_KIND_PREFIX),
        sst_filter_prefix_negatives: filter_kind(SST_FILTER_NEGATIVE_COUNT, FILTER_KIND_PREFIX),
        sst_filter_prefix_false_positives: filter_kind(SST_FILTER_FALSE_POSITIVE_COUNT, FILTER_KIND_PREFIX),
        backpressure_writes: counter(BACKPRESSURE_COUNT, &[]),
        l0_write_stalls: counter_sum(L0_STALL_COUNT, &[]),
        compaction_bytes: counter_sum(BYTES_COMPACTED, &[]),
        // The compactor only runs inside the writer `Db`; summing the reader in
        // is harmless (its series do not exist) and keeps the closure uniform.
        running_compactions: counter_sum(RUNNING_COMPACTIONS, &[]),
        last_compaction_timestamp_sec: gauge(LAST_COMPACTION_TS_SEC),
        memtable_write_bytes: counter(MEMTABLE_WRITE_BYTES, &[]),
        wal_flush_bytes: counter(WAL_FLUSH_BYTES, &[]),
        wal_buffer_flushes: counter(WAL_BUFFER_FLUSHES, &[]),
        wal_buffer_flush_requests: counter(WAL_BUFFER_FLUSH_REQUESTS, &[]),
        l0_flush_bytes: counter(L0_FLUSH_BYTES, &[]),
        block_cache_data_hits: counter(ACCESS_COUNT, &[("entry_kind", "data_block"), ("result", "hit")]),
        block_cache_data_misses: counter(ACCESS_COUNT, &[("entry_kind", "data_block"), ("result", "miss")]),
        block_cache_filter_hits: counter(ACCESS_COUNT, &[("entry_kind", "filter"), ("result", "hit")]),
        block_cache_filter_misses: counter(ACCESS_COUNT, &[("entry_kind", "filter"), ("result", "miss")]),
        object_store_get_requests: counter_sum(OBJECT_STORE_REQUEST_COUNT, &[("op", "get")]),
        object_store_put_requests: counter_sum(OBJECT_STORE_REQUEST_COUNT, &[("op", "put")]),
        object_store_get_requests_by_component: requests_by_component("get"),
        object_store_put_requests_by_component: requests_by_component("put"),
        // The writer and the reader each build their own disk cache over the
        // scope's one cache directory. Their hit, access and eviction counts
        // are each handle's own reads, so they sum; the byte gauge is each
        // handle's view of the same directory, so it takes one of them.
        disk_cache_part_hits: counter(DISK_CACHE_PART_HITS, &[]),
        disk_cache_part_accesses: counter(DISK_CACHE_PART_ACCESSES, &[]),
        disk_cache_bytes: gauge(DISK_CACHE_BYTES),
        disk_cache_evicted_bytes: counter(DISK_CACHE_EVICTED_BYTES, &[]),
    }
}
