use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use slatedb::config::{DbReaderOptions, PreloadLevel, Settings};
use slatedb::object_store::{path::Path, ObjectStore};
use slatedb::{Db, DbReader, DbReaderMode};

use crate::{GraphCachePolicy, Result, StorageSequence};

pub const DEFAULT_TRUSTED_APPEND_CHUNK_EDGES: usize = 4_096;
#[cfg(not(test))]
const GRAPH_READER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(10);
#[cfg(test)]
const GRAPH_READER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphLimits {
    pub max_bulk_import_edges: usize,
    pub max_artifact_source_epochs: StorageSequence,
    pub max_traversal_hops: u8,
    pub max_artifact_build_edges: u64,
    pub max_query_result_vertices: usize,
    pub max_query_intermediate_rows: usize,
    pub max_query_index_candidates: usize,
    pub max_query_scan_edges: u64,
    pub max_query_runtime_ms: Option<u64>,
}

impl Default for GraphLimits {
    fn default() -> Self {
        Self {
            max_bulk_import_edges: 1_000_000,
            max_artifact_source_epochs: 10_000_000,
            max_traversal_hops: 16,
            max_artifact_build_edges: 10_000_000,
            max_query_result_vertices: 100_000,
            max_query_intermediate_rows: 250_000,
            max_query_index_candidates: 250_000,
            max_query_scan_edges: 1_000_000,
            max_query_runtime_ms: Some(30_000),
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheConfig {
    pub object_store_cache_dir: Option<PathBuf>,
    pub object_store_cache_bytes: Option<usize>,
    pub object_store_cache_puts: bool,
    pub preload_sst_on_startup: bool,
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
        }
    }

    fn apply_to_settings(&self, settings: &mut Settings) {
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
            max_wal_flushes_before_l0_flush: 4_096,
            l0_flush_parallelism: 1,
        }
    }
}

impl GraphStorageMemoryConfig {
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
        if self.max_wal_flushes_before_l0_flush < 4_096 {
            return Err(crate::GraphError::CorruptValue {
                key: "storage_memory/max_wal_flushes_before_l0_flush".to_string(),
                reason: "maximum WAL flush count must be at least 4096".to_string(),
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
            wal_flush_interval_ms: Some(1),
            await_durable_writes: true,
        }
    }
}

impl GraphDurabilityConfig {
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
) -> Result<Db> {
    let mut settings = Settings::default();
    cache.apply_to_settings(&mut settings);
    storage_memory.apply_to_settings(&mut settings)?;
    durability.apply_to_settings(&mut settings);
    Ok(Db::builder(path, object_store)
        .with_settings(settings)
        .build()
        .await?)
}

pub(crate) async fn open_graph_reader(
    path: impl Into<Path>,
    object_store: Arc<dyn ObjectStore>,
    cache: &GraphCacheConfig,
) -> Result<DbReader> {
    let mut options = DbReaderOptions {
        manifest_poll_interval: GRAPH_READER_MANIFEST_POLL_INTERVAL,
        ..DbReaderOptions::default()
    };
    cache.apply_to_reader_options(&mut options);
    Ok(DbReader::builder(path, object_store)
        .with_options(options)
        .with_reader_mode(DbReaderMode::ManagedCheckpoint)
        .build()
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_memory_profiles_are_valid_and_bounded() {
        let balanced = GraphStorageMemoryConfig::default();
        balanced.validate().unwrap();
        assert_eq!(balanced.l0_sst_size_bytes, 16 * 1024 * 1024);
        assert_eq!(balanced.max_unflushed_bytes, 64 * 1024 * 1024);

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

    #[test]
    fn storage_memory_rejects_invalid_limits() {
        let mut config = GraphStorageMemoryConfig::default();
        config.max_unflushed_bytes = config.l0_sst_size_bytes - 1;
        assert!(config.validate().is_err());

        let config = GraphStorageMemoryConfig {
            max_wal_flushes_before_l0_flush: 4_095,
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
        assert_eq!(
            options.cache_policy.sparse_kernel,
            crate::SparseKernelBackend::Adjacency
        );
    }
}
