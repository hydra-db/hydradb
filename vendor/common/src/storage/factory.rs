//! Storage factory for creating storage instances from configuration.
//!
//! This module provides factory functions for creating storage backends
//! based on configuration, supporting both InMemory and SlateDB backends.

use std::sync::Arc;

use super::config::{BlockCacheConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use super::in_memory::InMemoryStorage;
use super::metrics_recorder::{MetricsRsRecorder, MixtricsBridge as MetricsRsRegistry};
use super::slate::{SlateDbStorage, SlateDbStorageReader};
use super::{MergeOperator, Storage, StorageError, StorageRead, StorageResult};
use slatedb::config::Settings;
pub use slatedb::db_cache::DbCache;
pub use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
pub use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
pub use slatedb::db_cache::{CachedEntry, CachedKey, SplitCache};
use slatedb::object_store::{self, ObjectStore};
pub use slatedb::{CompactorBuilder, DbBuilder};
use slatedb::{DbReader, FilterPolicy, PrefixExtractor, SstReader};
use tracing::info;
use uuid::Uuid;

/// Builder for creating storage instances from configuration.
///
/// `StorageBuilder` provides layered access to the underlying SlateDB
/// [`DbBuilder`], replacing the previous `StorageRuntime` middleman.
///
/// # Example
///
/// ```rust,ignore
/// use common::{StorageBuilder, StorageSemantics, create_object_store};
/// use common::storage::factory::CompactorBuilder;
///
/// // Simple usage:
/// let storage = StorageBuilder::new(&config.storage).await?
///     .with_semantics(StorageSemantics::new().with_merge_operator(Arc::new(MyOp)))
///     .build()
///     .await?;
///
/// // Escape hatch for low-level SlateDB configuration:
/// let storage = StorageBuilder::new(&config.storage).await?
///     .map_slatedb(|db| {
///         let obj_store = create_object_store(&slate_config.object_store).unwrap();
///         db.with_compactor_builder(
///             CompactorBuilder::new(slate_config.path.clone(), obj_store)
///                 .with_runtime(compaction_runtime.handle().clone())
///         )
///     })
///     .build()
///     .await?;
/// ```
pub struct StorageBuilder {
    inner: StorageBuilderInner,
    semantics: StorageSemantics,
}

enum StorageBuilderInner {
    InMemory,
    SlateDb {
        db_builder: Box<DbBuilder<String>>,
        /// Built alongside the writer so [`SlateDbStorage::slate_read`] can
        /// serve the count path from the same path/object store/cache.
        sst_reader: SstReader,
    },
}

impl StorageBuilder {
    /// Creates a new `StorageBuilder` from a [`StorageConfig`].
    ///
    /// For SlateDB configs this creates a [`DbBuilder`] with the configured
    /// path, object store, settings, and caches (if configured). When either
    /// `block_cache` or `meta_cache` is set, the two are combined into a
    /// [`SplitCache`] so data blocks and SST metadata can use independent
    /// policies. For InMemory configs it stores a sentinel so that `build()`
    /// returns an `InMemoryStorage`.
    pub async fn new(config: &StorageConfig) -> StorageResult<Self> {
        let inner = match config {
            StorageConfig::InMemory => StorageBuilderInner::InMemory,
            StorageConfig::SlateDb(slate_config) => {
                let object_store = create_object_store(&slate_config.object_store)?;
                let settings = load_slatedb_settings(slate_config)?;
                info!(
                    "create slatedb storage with config: {:?}, settings: {:?}",
                    slate_config, settings
                );
                let cache =
                    build_split_cache(&slate_config.block_cache, &slate_config.meta_cache).await?;
                let mut db_builder =
                    DbBuilder::new(slate_config.path.clone(), object_store.clone())
                        .with_settings(settings);
                if let Some(cache) = cache.clone() {
                    db_builder = db_builder.with_db_cache(cache);
                }
                // Share the writer's path/object store/cache so the count path
                // reads the same SSTs the writer produces.
                let sst_reader =
                    SstReader::new(slate_config.path.clone(), object_store, cache, None);
                StorageBuilderInner::SlateDb {
                    db_builder: Box::new(db_builder),
                    sst_reader,
                }
            }
        };
        Ok(Self {
            inner,
            semantics: StorageSemantics::default(),
        })
    }

    /// Sets the [`StorageSemantics`] (merge operator, etc.) for this builder.
    pub fn with_semantics(mut self, semantics: StorageSemantics) -> Self {
        self.semantics = semantics;
        self
    }

    /// Maps over the underlying [`DbBuilder`] for low-level SlateDB configuration.
    ///
    /// This is the escape hatch for any SlateDB knob not exposed by
    /// `StorageBuilder` itself (compactor builder, block cache, GC runtime, etc.).
    /// Use `db.with_db_cache(...)` inside the closure to override the
    /// config-driven block cache.
    ///
    /// For InMemory storage this is a no-op.
    pub fn map_slatedb(mut self, f: impl FnOnce(DbBuilder<String>) -> DbBuilder<String>) -> Self {
        if let StorageBuilderInner::SlateDb {
            db_builder,
            sst_reader,
        } = self.inner
        {
            self.inner = StorageBuilderInner::SlateDb {
                db_builder: Box::new(f(*db_builder)),
                sst_reader,
            };
        }
        self
    }

    /// Builds the storage instance.
    ///
    /// Applies semantics (merge operator) to the `DbBuilder` and calls `.build()`.
    pub async fn build(self) -> StorageResult<Arc<dyn Storage>> {
        match self.inner {
            StorageBuilderInner::InMemory => {
                let storage = match self.semantics.merge_operator {
                    Some(op) => InMemoryStorage::with_merge_operator(op),
                    None => InMemoryStorage::new(),
                };
                Ok(Arc::new(storage))
            }
            StorageBuilderInner::SlateDb {
                db_builder,
                sst_reader,
            } => {
                let mut db_builder = *db_builder;
                db_builder = db_builder.with_metrics_recorder(Arc::new(MetricsRsRecorder));
                if let Some(op) = self.semantics.merge_operator {
                    let adapter = SlateDbStorage::merge_operator_adapter(op);
                    db_builder = db_builder.with_merge_operator(Arc::new(adapter));
                }
                if let Some(policies) = self.semantics.filter_policies {
                    db_builder = db_builder.with_filter_policies(policies);
                }
                if let Some(extractor) = self.semantics.segment_extractor {
                    db_builder = db_builder.with_segment_extractor(extractor);
                }
                let db = db_builder.build().await.map_err(|e| {
                    StorageError::Storage(format!("Failed to create SlateDB: {}", e))
                })?;
                Ok(Arc::new(
                    SlateDbStorage::new(Arc::new(db)).with_sst_reader(sst_reader),
                ))
            }
        }
    }
}

/// Runtime options for read-only storage instances.
///
/// This struct holds non-serializable runtime configuration for `DbReader`.
/// Unlike `StorageBuilder`, it only exposes options relevant to readers
/// (currently just block cache).
#[derive(Default, Clone)]
pub struct StorageReaderRuntime {
    pub(crate) block_cache: Option<Arc<dyn DbCache>>,
    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) checkpoint_id: Option<Uuid>,
}

impl StorageReaderRuntime {
    /// Creates a new reader runtime with default options.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_cache(&self) -> Option<Arc<dyn DbCache>> {
        self.block_cache.clone()
    }

    /// Sets a block cache for SlateDB reads.
    ///
    /// When provided, the `DbReader` will use this cache for SST block lookups,
    /// reducing disk I/O on repeated reads. Use `FoyerCache::new_with_opts`
    /// to control capacity.
    ///
    /// This option only affects SlateDB storage; it is ignored for in-memory storage.
    pub fn with_block_cache(mut self, cache: Arc<dyn DbCache>) -> Self {
        self.block_cache = Some(cache);
        self
    }

    pub fn with_object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    /// Pins this reader to a specific SlateDB checkpoint.
    ///
    /// When set, the reader serves a consistent view of the database as of
    /// the checkpoint and does not advance with newer writes. The checkpoint
    /// must already exist in storage (typically created via
    /// [`crate::Storage::create_checkpoint`]). Only meaningful for SlateDB
    /// storage.
    pub fn with_checkpoint_id(mut self, id: Uuid) -> Self {
        self.checkpoint_id = Some(id);
        self
    }
}

/// Storage semantics configured by system crates.
///
/// This struct holds semantic concerns like merge operators that are specific
/// to each system (log, timeseries, vector). End users should not use this
/// directly - each system configures its own semantics internally.
///
/// # Internal Use Only
///
/// This type is public so that system crates (timeseries, vector, log) can
/// access it, but it is not intended for end-user consumption.
///
/// # Example (for system crate implementers)
///
/// ```rust,ignore
/// // In timeseries crate:
/// let semantics = StorageSemantics::new()
///     .with_merge_operator(Arc::new(TimeSeriesMergeOperator));
/// let storage = StorageBuilder::new(&config).await?
///     .with_semantics(semantics)
///     .build()
///     .await?;
/// ```
#[derive(Default)]
pub struct StorageSemantics {
    pub(crate) merge_operator: Option<Arc<dyn MergeOperator>>,
    pub(crate) filter_policies: Option<Vec<Arc<dyn FilterPolicy>>>,
    pub(crate) segment_extractor: Option<Arc<dyn PrefixExtractor>>,
}

impl StorageSemantics {
    /// Creates new storage semantics with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the merge operator for merge operations.
    ///
    /// The merge operator defines how values are combined during compaction.
    /// Each system (timeseries, vector) defines its own merge semantics.
    pub fn with_merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        self.merge_operator = Some(op);
        self
    }

    /// Sets the filter policies for SlateDB writers and readers.
    ///
    /// System crates use this to keep the writer, compactor, and standalone
    /// reader paths aligned on SST filter encoding/decoding behavior.
    pub fn with_filter_policies(mut self, policies: Vec<Arc<dyn FilterPolicy>>) -> Self {
        self.filter_policies = Some(policies);
        self
    }

    /// Sets the segment extractor (SlateDB RFC-0024) for writers and readers.
    ///
    /// SlateDB 0.14 persists the extractor's name in the manifest and refuses
    /// to open a writer or `DbReader` whose configured extractor doesn't match.
    /// Routing this through semantics keeps the writer, compactor, and
    /// standalone reader paths aligned on a single extractor.
    pub fn with_segment_extractor(mut self, extractor: Arc<dyn PrefixExtractor>) -> Self {
        self.segment_extractor = Some(extractor);
        self
    }
}

pub fn new_slatedb_compactor_builder(
    config: &StorageConfig,
) -> StorageResult<Option<CompactorBuilder<String>>> {
    match config {
        StorageConfig::InMemory => Ok(None),
        StorageConfig::SlateDb(slate_config) => {
            let object_store = create_object_store(&slate_config.object_store)?;
            let settings = load_slatedb_settings(slate_config)?;
            match settings.compactor_options {
                Some(compactor_options) => Ok(Some(
                    CompactorBuilder::new(slate_config.path.clone(), object_store)
                        .with_options(compactor_options),
                )),
                None => Ok(None),
            }
        }
    }
}

fn load_slatedb_settings(slate_config: &SlateDbStorageConfig) -> StorageResult<Settings> {
    match &slate_config.settings_path {
        Some(path) => Ok(Settings::from_file(path).map_err(|e| {
            StorageError::Storage(format!(
                "Failed to load SlateDB settings from {}: {}",
                path, e
            ))
        })?),
        None => Ok(Settings::load().unwrap_or_default()),
    }
}

/// Creates an object store from configuration without initializing SlateDB.
///
/// This is useful for cleanup operations where you need to access the object store
/// after the database has been closed.
pub fn create_object_store(config: &ObjectStoreConfig) -> StorageResult<Arc<dyn ObjectStore>> {
    match config {
        ObjectStoreConfig::InMemory => Ok(Arc::new(object_store::memory::InMemory::new())),
        ObjectStoreConfig::Aws(aws_config) => {
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_region(&aws_config.region)
                .with_bucket_name(&aws_config.bucket)
                .build()
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create AWS S3 store: {}", e))
                })?;
            Ok(Arc::new(store))
        }
        ObjectStoreConfig::Local(local_config) => {
            std::fs::create_dir_all(&local_config.path).map_err(|e| {
                StorageError::Storage(format!(
                    "Failed to create storage directory '{}': {}",
                    local_config.path, e
                ))
            })?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(&local_config.path)
                .map_err(|e| {
                StorageError::Storage(format!("Failed to create local filesystem store: {}", e))
            })?;
            Ok(Arc::new(store))
        }
    }
}

/// Creates a read-only storage instance based on configuration.
///
/// This function creates a storage backend that only supports read operations.
/// For SlateDB, it uses `DbReader` which does not participate in fencing,
/// allowing multiple readers to coexist with a single writer.
///
/// # Arguments
///
/// * `config` - The storage configuration specifying the backend type and settings.
/// * `semantics` - System-specific semantics like merge operators.
/// * `reader_options` - SlateDB reader options (e.g., manifest_poll_interval).
///   These are passed directly to `DbReader::open` for SlateDB storage.
///   Ignored for InMemory storage.
///
/// # Returns
///
/// Returns an `Arc<dyn StorageRead>` on success, or a `StorageError` on failure.
pub async fn create_storage_read(
    config: &StorageConfig,
    runtime: StorageReaderRuntime,
    semantics: StorageSemantics,
    reader_options: slatedb::config::DbReaderOptions,
) -> StorageResult<Arc<dyn StorageRead>> {
    match config {
        StorageConfig::InMemory => {
            // InMemory has no fencing, reuse existing implementation
            let storage = match semantics.merge_operator {
                Some(op) => InMemoryStorage::with_merge_operator(op),
                None => InMemoryStorage::new(),
            };
            Ok(Arc::new(storage))
        }
        StorageConfig::SlateDb(slate_config) => {
            let object_store = if let Some(object_store) = &runtime.object_store {
                object_store.clone()
            } else {
                create_object_store(&slate_config.object_store)?
            };

            // Prefer the runtime-provided cache (owned by the caller); fall
            // back to the config-driven split cache. SlateDB drives cache
            // shutdown from `DbReader::close()`, so we don't hold a handle.
            // The reader and its count-path `SstReader` share this cache.
            let cache = if let Some(cache) = runtime.block_cache {
                Some(cache)
            } else {
                build_split_cache(&slate_config.block_cache, &slate_config.meta_cache).await?
            };

            let mut builder = DbReader::builder(slate_config.path.clone(), object_store.clone())
                .with_options(reader_options)
                .with_metrics_recorder(Arc::new(MetricsRsRecorder));
            if let Some(checkpoint_id) = runtime.checkpoint_id {
                builder = builder.with_checkpoint_id(checkpoint_id);
            }
            if let Some(op) = semantics.merge_operator {
                let adapter = SlateDbStorage::merge_operator_adapter(op);
                builder = builder.with_merge_operator(Arc::new(adapter));
            }
            if let Some(policies) = semantics.filter_policies {
                builder = builder.with_filter_policies(policies);
            }
            if let Some(extractor) = semantics.segment_extractor {
                builder = builder.with_segment_extractor(extractor);
            }
            if let Some(cache) = cache.clone() {
                builder = builder.with_db_cache(cache);
            }
            let reader = builder.build().await.map_err(|e| {
                StorageError::Storage(format!("Failed to create SlateDB reader: {}", e))
            })?;
            let sst_reader = SstReader::new(slate_config.path.clone(), object_store, cache, None);
            Ok(Arc::new(
                SlateDbStorageReader::new(Arc::new(reader)).with_sst_reader(sst_reader),
            ))
        }
    }
}

/// Builds the combined SlateDB cache from the serializable data- and
/// metadata-cache configs.
///
/// When either side is configured, both are built (a `None` side caches
/// nothing for its block class) and wrapped in a [`SplitCache`] that routes
/// data blocks to the data cache and index/filter/stats blocks to the meta
/// cache. Returns `None` only when neither side is configured, so callers can
/// skip `with_db_cache` entirely.
///
/// SlateDB drives cache shutdown from `Db::close()` / `DbReader::close()`, so
/// callers do not need to retain a handle to close the cache themselves.
async fn build_split_cache(
    data: &Option<BlockCacheConfig>,
    meta: &Option<BlockCacheConfig>,
) -> StorageResult<Option<Arc<dyn DbCache>>> {
    if data.is_none() && meta.is_none() {
        return Ok(None);
    }
    let data_cache = build_cache(data, "data").await?;
    let meta_cache = build_cache(meta, "meta").await?;
    let split = SplitCache::new()
        .with_block_cache(data_cache)
        .with_meta_cache(meta_cache)
        .build();
    Ok(Some(Arc::new(split) as Arc<dyn DbCache>))
}

/// Builds a single [`DbCache`] from one cache config, or `None` when absent.
///
/// `label` distinguishes the data vs metadata cache in logs and in the foyer
/// cache name.
async fn build_cache(
    config: &Option<BlockCacheConfig>,
    label: &str,
) -> StorageResult<Option<Arc<dyn DbCache>>> {
    let Some(config) = config else {
        return Ok(None);
    };
    match config {
        BlockCacheConfig::FoyerHybrid(foyer_config) => {
            use foyer::{
                BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder,
                HybridCachePolicy, PsyncIoEngineConfig,
            };

            let memory_capacity = usize::try_from(foyer_config.memory_capacity).map_err(|_| {
                StorageError::Storage(format!(
                    "memory_capacity {} exceeds usize::MAX on this platform",
                    foyer_config.memory_capacity
                ))
            })?;
            let disk_capacity = usize::try_from(foyer_config.disk_capacity).map_err(|_| {
                StorageError::Storage(format!(
                    "disk_capacity {} exceeds usize::MAX on this platform",
                    foyer_config.disk_capacity
                ))
            })?;

            let buffer_pool_size = usize::try_from(foyer_config.effective_buffer_pool_size())
                .map_err(|_| {
                    StorageError::Storage(format!(
                        "buffer_pool_size {} exceeds usize::MAX on this platform",
                        foyer_config.effective_buffer_pool_size()
                    ))
                })?;
            let submit_queue_size_threshold =
                usize::try_from(foyer_config.submit_queue_size_threshold).map_err(|_| {
                    StorageError::Storage(format!(
                        "submit_queue_size_threshold {} exceeds usize::MAX on this platform",
                        foyer_config.submit_queue_size_threshold
                    ))
                })?;

            let policy = match foyer_config.write_policy {
                super::config::FoyerWritePolicy::WriteOnInsertion => {
                    HybridCachePolicy::WriteOnInsertion
                }
                super::config::FoyerWritePolicy::WriteOnEviction => {
                    HybridCachePolicy::WriteOnEviction
                }
            };

            let device = {
                #[cfg(target_os = "linux")]
                let builder = FsDeviceBuilder::new(&foyer_config.disk_path)
                    .with_capacity(disk_capacity)
                    .with_direct(true);
                #[cfg(not(target_os = "linux"))]
                let builder =
                    FsDeviceBuilder::new(&foyer_config.disk_path).with_capacity(disk_capacity);
                builder.build().map_err(|e| {
                    StorageError::Storage(format!("Failed to build foyer device: {}", e))
                })?
            };

            let cache = HybridCacheBuilder::new()
                .with_name(format!("slatedb_{label}_cache"))
                .with_metrics_registry(Box::new(MetricsRsRegistry))
                .with_policy(policy)
                .memory(memory_capacity)
                .with_weighter(|_, v: &CachedEntry| v.size())
                .storage()
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(
                    BlockEngineConfig::new(device)
                        .with_flushers(foyer_config.flushers)
                        .with_buffer_pool_size(buffer_pool_size)
                        .with_submit_queue_size_threshold(submit_queue_size_threshold),
                )
                .build()
                .await
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create hybrid cache: {}", e))
                })?;

            info!(
                cache = label,
                memory_mb = foyer_config.memory_capacity / (1024 * 1024),
                disk_mb = foyer_config.disk_capacity / (1024 * 1024),
                disk_path = %foyer_config.disk_path,
                write_policy = ?foyer_config.write_policy,
                flushers = foyer_config.flushers,
                buffer_pool_mb = foyer_config.effective_buffer_pool_size() / (1024 * 1024),
                submit_queue_threshold_mb =
                    foyer_config.submit_queue_size_threshold / (1024 * 1024),
                "hybrid cache enabled"
            );

            Ok(Some(
                Arc::new(FoyerHybridCache::new_with_cache(cache)) as Arc<dyn DbCache>
            ))
        }
        BlockCacheConfig::FoyerMemory(mem_config) => {
            let max_capacity = mem_config.capacity;
            let opts = match mem_config.shards {
                Some(shards) => FoyerCacheOptions {
                    max_capacity,
                    shards,
                },
                None => FoyerCacheOptions {
                    max_capacity,
                    ..FoyerCacheOptions::default()
                },
            };
            info!(
                cache = label,
                capacity_mb = max_capacity / (1024 * 1024),
                shards = opts.shards,
                "in-memory cache enabled"
            );
            Ok(Some(
                Arc::new(FoyerCache::new_with_opts(opts)) as Arc<dyn DbCache>
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::config::{
        FoyerHybridCacheConfig, FoyerMemoryCacheConfig, LocalObjectStoreConfig,
        SlateDbStorageConfig,
    };

    fn foyer_cache_config(
        memory_capacity: u64,
        disk_capacity: u64,
        disk_path: String,
    ) -> FoyerHybridCacheConfig {
        FoyerHybridCacheConfig {
            memory_capacity,
            disk_capacity,
            disk_path,
            write_policy: Default::default(),
            flushers: 4,
            buffer_pool_size: None,
            submit_queue_size_threshold: 1024 * 1024 * 1024,
        }
    }

    fn slatedb_config_with_local_dir(dir: &std::path::Path) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        })
    }

    #[tokio::test]
    async fn should_create_storage_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
            meta_cache: None,
        });

        let storage = StorageBuilder::new(&config).await.unwrap().build().await;

        assert!(
            storage.is_ok(),
            "expected config-driven block cache to work"
        );
    }

    #[tokio::test]
    async fn should_create_storage_with_split_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("data-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        // Hybrid data cache + in-memory meta cache combined into a SplitCache.
        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
            meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: 1024 * 1024,
                shards: None,
            })),
        });

        let storage = StorageBuilder::new(&config).await.unwrap().build().await;

        assert!(storage.is_ok(), "expected split cache to build");
    }

    #[tokio::test]
    async fn should_build_split_cache_with_only_meta_cache() {
        // A meta-only config still produces a SplitCache (data side caches
        // nothing); build_split_cache returns Some.
        let meta = Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
            capacity: 1024 * 1024,
            shards: Some(1),
        }));
        let cache = build_split_cache(&None, &meta).await.unwrap();
        assert!(cache.is_some(), "meta-only config should yield a cache");
    }

    #[tokio::test]
    async fn should_create_reader_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let slate_config = SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
            meta_cache: None,
        };

        // First open a writer so the reader has a manifest to read
        let writer = StorageBuilder::new(&StorageConfig::SlateDb(slate_config.clone()))
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        // Close writer before opening reader (SlateDB fencing)
        drop(writer);

        let reader = create_storage_read(
            &StorageConfig::SlateDb(slate_config),
            StorageReaderRuntime::new(),
            StorageSemantics::new(),
            slatedb::config::DbReaderOptions::default(),
        )
        .await;

        assert!(
            reader.is_ok(),
            "expected config-driven block cache on reader to work"
        );
    }

    #[cfg(target_pointer_width = "32")]
    #[tokio::test]
    async fn should_error_when_capacity_exceeds_usize() {
        // On 32-bit platforms, u64::MAX > usize::MAX triggers our overflow check.
        // On 64-bit this is a no-op, so gate on 32-bit.
        let config = BlockCacheConfig::FoyerHybrid(foyer_cache_config(
            u64::MAX,
            4 * 1024 * 1024,
            "/tmp/unused".to_string(),
        ));

        let result = build_cache(&Some(config), "data").await;
        assert!(result.is_err());
    }

    /// Helper: creates a SlateDb config whose block_cache disk_path is a regular file
    /// (not a directory), which foyer deterministically rejects.
    fn config_with_invalid_block_cache_disk_path(
        obj_dir: &std::path::Path,
        bad_disk_path: &str,
    ) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: obj_dir.to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                bad_disk_path.to_string(),
            ))),
            meta_cache: None,
        })
    }

    // Note: foyer panics (unwrap inside DirectFsDevice) on invalid disk paths
    // rather than returning an error. We isolate the panic to the create_storage
    // call via tokio::spawn so setup unwrap() failures don't mask regressions.
    #[tokio::test]
    async fn should_fail_when_config_cache_disk_path_is_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        // Use a regular file as disk_path — foyer expects a directory
        let bad_path = tmp.path().join("not-a-dir");
        std::fs::write(&bad_path, b"").unwrap();

        let config = config_with_invalid_block_cache_disk_path(
            &tmp.path().join("obj"),
            bad_path.to_str().unwrap(),
        );

        // Isolate the expected panic to just the build call
        let handle = tokio::spawn(async move {
            let _ = StorageBuilder::new(&config).await.unwrap().build().await;
        });
        let result = handle.await;
        assert!(
            result.is_err() && result.unwrap_err().is_panic(),
            "expected foyer to panic on invalid disk_path"
        );
    }

    #[tokio::test]
    async fn should_fail_reader_when_config_cache_disk_path_is_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        let bad_path = tmp.path().join("not-a-dir");
        std::fs::write(&bad_path, b"").unwrap();

        let slate_config = SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                bad_path.to_str().unwrap().to_string(),
            ))),
            meta_cache: None,
        };

        // First open a writer (without cache) so the reader has a manifest
        let writer = StorageBuilder::new(&StorageConfig::SlateDb(SlateDbStorageConfig {
            block_cache: None,
            ..slate_config.clone()
        }))
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
        drop(writer);

        // Isolate the expected panic to just the create_storage_read call
        let handle = tokio::spawn(async move {
            let _ = create_storage_read(
                &StorageConfig::SlateDb(slate_config),
                StorageReaderRuntime::new(),
                StorageSemantics::new(),
                slatedb::config::DbReaderOptions::default(),
            )
            .await;
        });
        let result = handle.await;
        assert!(
            result.is_err() && result.unwrap_err().is_panic(),
            "expected foyer to panic on invalid disk_path for reader"
        );
    }

    #[tokio::test]
    async fn reader_runtime_cache_should_take_precedence_over_config_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let bad_path = tmp.path().join("not-a-dir");
        std::fs::write(&bad_path, b"").unwrap();

        let slate_config = SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                bad_path.to_str().unwrap().to_string(),
            ))),
            meta_cache: None,
        };

        // First open a writer (without cache) so the reader has a manifest
        let writer = StorageBuilder::new(&StorageConfig::SlateDb(SlateDbStorageConfig {
            block_cache: None,
            ..slate_config.clone()
        }))
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
        drop(writer);

        // Runtime cache should bypass the invalid config cache
        let runtime_cache = FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 1024 * 1024,
            shards: 1,
        });
        let runtime = StorageReaderRuntime::new().with_block_cache(Arc::new(runtime_cache));

        let result = create_storage_read(
            &StorageConfig::SlateDb(slate_config),
            runtime,
            StorageSemantics::new(),
            slatedb::config::DbReaderOptions::default(),
        )
        .await;

        assert!(
            result.is_ok(),
            "reader runtime cache should take precedence, skipping invalid config cache"
        );
    }

    #[tokio::test]
    async fn should_return_none_when_no_cache_configured() {
        let result = build_split_cache(&None, &None).await.unwrap();
        assert!(result.is_none());
        // And the single-cache builder returns None for an absent config.
        assert!(build_cache(&None, "data").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_work_without_block_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let config = slatedb_config_with_local_dir(tmp.path());

        let storage = StorageBuilder::new(&config).await.unwrap().build().await;

        assert!(storage.is_ok());
    }

    #[test]
    fn new_slatedb_compactor_builder_should_be_none_for_in_memory() {
        // given / when - the in-memory backend has no compactor to configure
        let builder = new_slatedb_compactor_builder(&StorageConfig::InMemory).unwrap();

        // then
        assert!(builder.is_none());
    }

    #[tokio::test]
    async fn new_slatedb_compactor_builder_should_be_some_for_slatedb() {
        // given - a SlateDb config (default settings enable compaction)
        let tmp = tempfile::tempdir().unwrap();
        let config = slatedb_config_with_local_dir(tmp.path());

        // when
        let builder = new_slatedb_compactor_builder(&config).unwrap();

        // then - a compactor builder is produced for consumers to configure and
        // install via `StorageBuilder::map_slatedb`.
        assert!(builder.is_some());
    }
}
