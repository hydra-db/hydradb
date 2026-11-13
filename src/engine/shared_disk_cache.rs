//! One SlateDB disk cache for every graph scope a node opens.
//!
//! SlateDB builds a disk cache inside every `Db` and `DbReader` from
//! `object_store_cache_options`. A scoped runtime keeps up to
//! `GRAPH_MAX_OPEN_SCOPES` of them, so the node's cache budget used to be split
//! evenly across that many slots: on prod, 48 GiB over 64 scopes left each
//! scope 768 MiB, however little the others held. A scope whose working set was
//! larger refetched the same SST parts from object storage on every read, and
//! evicted hundreds of MB a second, while most of the node's budget sat empty.
//!
//! [`SharedDiskCache`] instead builds one `CachedObjectStore` over the node's
//! object store, with the whole budget and one eviction order, and hands that
//! store to every scope as their object store. SlateDB documents this form:
//! "Construct it over the raw backend and pass it to SlateDB as the object
//! store itself" (`slatedb::cached_object_store::CachedObjectStore`, at the
//! pinned rev). Entries are keyed by object path and every scope's objects live
//! under its own prefix, so scopes cannot read each other's parts. Only
//! compacted SST reads, and SST writes when admitted, go through the cache;
//! manifests, WAL and listings pass through to the store below unchanged, which
//! is what SlateDB's per-database cache did as well.
//!
//! A scope then opens with no disk cache of its own
//! ([`SharedDiskCache::take`] clears it from the options), which is why the
//! scoped runtime refuses options that still carry one.
//!
//! SlateDB 0.18 deprecates `CachedObjectStore` in favour of
//! `slatedb_mirror::ObjectStoreMirror`. The type still works, so this module
//! allows the deprecation until the move to the mirror.

use std::path::PathBuf;
use std::sync::Arc;

#[allow(deprecated)] // See the module note on `CachedObjectStore`.
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::cached_object_store_stats::{CACHE_BYTES, EVICTED_BYTES, PART_ACCESS_COUNT, PART_HIT_COUNT};
use slatedb::object_store::ObjectStore;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder};

use crate::{GraphCacheConfig, GraphError, Result};

/// The subdirectory of the configured cache directory the shared cache owns.
/// The per-scope caches used `scopes/` beside it; see
/// [`SharedDiskCache::remove_per_scope_caches`].
const SHARED_DIR: &str = "shared";
const PER_SCOPE_DIR: &str = "scopes";

/// The node's one disk cache and the object store that reads through it.
#[derive(Clone)]
pub struct SharedDiskCache {
    store: Arc<dyn ObjectStore>,
    recorder: Arc<DefaultMetricsRecorder>,
    root: PathBuf,
    /// Metrics already warned about as missing or of an unexpected type, so
    /// each is reported once rather than on every collection.
    warned: Arc<std::sync::Mutex<std::collections::BTreeSet<&'static str>>>,
}

impl std::fmt::Debug for SharedDiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedDiskCache").field("root", &self.root).finish_non_exhaustive()
    }
}

/// The metric type SlateDB registers a cache metric as.
#[derive(Clone, Copy, Debug)]
enum Expected {
    Counter,
    Gauge,
}

impl Expected {
    fn as_str(self) -> &'static str {
        match self {
            Expected::Counter => "counter",
            Expected::Gauge => "gauge",
        }
    }
}

/// The shared disk cache's counters, for the whole node.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SharedDiskCacheMetrics {
    /// Part reads answered from the node's volume. Counter.
    pub part_hits: u64,
    /// Part reads, hit or miss; the difference went to object storage. Counter.
    pub part_accesses: u64,
    /// Bytes the cache holds on the node's volume. Gauge.
    pub bytes: u64,
    /// Bytes evicted to stay under the budget. Counter.
    pub evicted_bytes: u64,
}

impl SharedDiskCache {
    /// Take the disk cache out of `cache` and build it once over `backend`.
    ///
    /// `None` when `cache` configures no disk cache. Otherwise `cache` is left
    /// with none, so the scopes opened with it read through
    /// [`Self::object_store`] instead of building their own, and the budget,
    /// part size, file-handle limit and write admission it carried apply to
    /// the one shared cache. A preload request is dropped: it warms a single
    /// database's SSTs at open, which a cache shared by every scope cannot do.
    #[allow(deprecated)] // See the module note on `CachedObjectStore`.
    pub async fn take(backend: Arc<dyn ObjectStore>, cache: &mut GraphCacheConfig) -> Result<Option<Self>> {
        let Some(dir) = cache.object_store_cache_dir.take() else {
            return Ok(None);
        };
        let budget = cache.object_store_cache_bytes.take();
        cache.preload_sst_on_startup = false;
        Self::remove_per_scope_caches(&dir)?;
        let root = dir.join(SHARED_DIR);
        std::fs::create_dir_all(&root).map_err(|error| GraphError::CorruptValue {
            key: root.display().to_string(),
            reason: format!("cannot create the shared disk cache directory: {error}; fix: check GRAPH_DATA_CACHE_DIR is writable"),
        })?;
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let store = CachedObjectStore::builder(root.clone(), backend)
            .with_max_cache_size_bytes(budget)
            .with_part_size_bytes(cache.object_store_cache_part_bytes)
            .with_max_open_file_handles(cache.object_store_cache_max_open_file_handles)
            .with_cache_on_flush(cache.object_store_cache_puts)
            .with_cache_on_compaction(cache.object_store_cache_puts)
            .with_metrics_recorder(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
            .build()
            .await?;
        Ok(Some(Self {
            store,
            recorder,
            root,
            warned: Arc::default(),
        }))
    }

    /// The store every scope opens with: the node's store, read through the
    /// shared cache.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// The cache's counters now.
    ///
    /// SlateDB registers all four while building the cache, as three counters
    /// and a gauge. One that is missing or of another type means its
    /// instrumentation changed: that is warned once per metric, naming it and
    /// the type found, and the value reads as zero so the rest still exports.
    pub fn metrics(&self) -> SharedDiskCacheMetrics {
        let snapshot = self.recorder.snapshot();
        let read = |name: &'static str, expected: Expected| -> u64 {
            let found = snapshot.by_name(name).into_iter().next().map(|metric| &metric.value);
            match (expected, found) {
                (Expected::Counter, Some(MetricValue::Counter(value))) => *value,
                (Expected::Gauge, Some(MetricValue::Gauge(value))) => (*value).max(0) as u64,
                (_, found) => {
                    self.warn_once(name, expected, found);
                    0
                }
            }
        };
        SharedDiskCacheMetrics {
            part_hits: read(PART_HIT_COUNT, Expected::Counter),
            part_accesses: read(PART_ACCESS_COUNT, Expected::Counter),
            bytes: read(CACHE_BYTES, Expected::Gauge),
            evicted_bytes: read(EVICTED_BYTES, Expected::Counter),
        }
    }

    fn warn_once(&self, name: &'static str, expected: Expected, found: Option<&MetricValue>) {
        if !self.warned.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(name) {
            return;
        }
        let found = match found {
            None => "missing",
            Some(MetricValue::Counter(_)) => "counter",
            Some(MetricValue::Gauge(_)) => "gauge",
            Some(MetricValue::UpDownCounter(_)) => "up-down counter",
            Some(MetricValue::Histogram { .. }) => "histogram",
        };
        tracing::warn!(
            metric = name,
            expected = expected.as_str(),
            found,
            "shared disk cache metric is missing or of an unexpected type; reporting zero"
        );
    }

    /// Delete the per-scope caches an earlier build left on the volume. They
    /// are not read any more, and left in place they would hold space the
    /// shared cache's budget assumes it has.
    fn remove_per_scope_caches(dir: &std::path::Path) -> Result<()> {
        let stale = dir.join(PER_SCOPE_DIR);
        match std::fs::remove_dir_all(&stale) {
            Ok(()) => {
                tracing::info!(path = %stale.display(), "removed the per-scope disk caches; the node now shares one");
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(GraphError::CorruptValue {
                key: stale.display().to_string(),
                reason: format!("cannot remove the per-scope disk caches: {error}; fix: delete the directory and restart"),
            }),
        }
    }
}
