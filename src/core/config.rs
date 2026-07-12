use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use slatedb::config::{DbReaderOptions, PreloadLevel, Settings};
use slatedb::object_store::{path::Path, ObjectStore};
use slatedb::{Db, DbReader};

use crate::{GraphCachePolicy, GraphEpoch, Result};

pub const DEFAULT_TRUSTED_APPEND_CHUNK_EDGES: usize = 32_768;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphLimits {
    pub max_bulk_import_edges: usize,
    pub max_artifact_source_epochs: GraphEpoch,
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
pub struct GraphRetentionPolicy {
    pub min_retained_epochs: GraphEpoch,
    pub read_lease_ttl_ms: u64,
    pub max_read_leases_to_scan: u64,
}

impl Default for GraphRetentionPolicy {
    fn default() -> Self {
        Self {
            min_retained_epochs: 0,
            read_lease_ttl_ms: 60_000,
            max_read_leases_to_scan: 10_000,
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
        settings.object_store_cache_options.cache_puts = self.object_store_cache_puts;
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
        options.object_store_cache_options.cache_puts = false;
        if self.preload_sst_on_startup {
            options
                .object_store_cache_options
                .preload_disk_cache_on_startup = Some(PreloadLevel::AllSst);
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphOpenOptions {
    pub limits: GraphLimits,
    pub cache: GraphCacheConfig,
    pub durability: GraphDurabilityConfig,
    pub cache_policy: GraphCachePolicy,
    pub retention_policy: GraphRetentionPolicy,
    pub backpressure_policy: GraphBackpressurePolicy,
    pub index_policy: GraphIndexPolicy,
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
    durability: &GraphDurabilityConfig,
) -> Result<Db> {
    let mut settings = Settings::default();
    cache.apply_to_settings(&mut settings);
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
    let mut options = DbReaderOptions::default();
    cache.apply_to_reader_options(&mut options);
    Ok(DbReader::builder(path, object_store)
        .with_options(options)
        .build()
        .await?)
}
