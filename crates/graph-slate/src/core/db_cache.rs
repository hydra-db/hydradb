use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;

use async_trait::async_trait;
use slatedb::db_cache::{CacheFetch, CacheLoader, CachedEntry, CachedKey, DbCache};
use slatedb::object_store::ObjectStore;

use crate::core::cache_observe::{
    CacheObserveSnapshot, CacheObserver, EntryKind, EpisodeContext, LoadMeasurement, ObserveConfig, PublicationGuard, RemovalReason,
    Residency, ScopeHandle,
};
use crate::{GraphError, Result};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedSlateDbCacheConfig {
    pub capacity_bytes: usize,
    pub observe: Option<ObserveConfig>,
}

impl SharedSlateDbCacheConfig {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            observe: None,
        }
    }

    pub fn with_observe(capacity_bytes: usize, observe: ObserveConfig) -> Self {
        Self {
            capacity_bytes,
            observe: Some(observe),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SlateDbCacheMetricsSnapshot {
    pub capacity_bytes: u64,
    /// Accounted payload bytes, excluding cache and observer metadata.
    pub resident_bytes: u64,
    pub entries: u64,
    pub observe: CacheObserveSnapshot,
}

struct ObservedEntry {
    value: CachedEntry,
    owner: ScopeHandle,
    kind: EntryKind,
    residency: Option<Arc<Residency>>,
    untracked_publication: Option<PublicationGuard>,
}

impl ObservedEntry {
    fn new(
        observer: &CacheObserver<CachedKey>,
        key: &CachedKey,
        owner: ScopeHandle,
        kind: EntryKind,
        value: CachedEntry,
        episode: Option<&EpisodeContext>,
    ) -> Self {
        let residency = observer.prepare(key, &owner, kind, value.size() as u64, episode);
        let untracked_publication = residency.is_none().then(|| observer.publication(key, &owner));
        Self {
            value,
            owner,
            kind,
            residency,
            untracked_publication,
        }
    }
}

struct ObserverListener {
    observer: Arc<CacheObserver<CachedKey>>,
}

impl foyer::EventListener for ObserverListener {
    type Key = CachedKey;
    type Value = ObservedEntry;

    fn on_leave(&self, reason: foyer::Event, key: &CachedKey, entry: &ObservedEntry) {
        let reason = match reason {
            foyer::Event::Evict => RemovalReason::CapacityEviction,
            foyer::Event::Replace => RemovalReason::Replacement,
            foyer::Event::Remove => RemovalReason::ExplicitRemove,
            foyer::Event::Clear => RemovalReason::Clear,
        };
        self.observer.removed(
            key,
            entry.residency.as_ref(),
            &entry.owner,
            entry.kind,
            reason,
            entry.value.size() as u64,
        );
        if let Some(publication) = &entry.untracked_publication {
            publication.ack();
        }
    }
}

fn record_admission(observer: &CacheObserver<CachedKey>, key: &CachedKey, entry: &foyer::CacheEntry<CachedKey, ObservedEntry>) {
    match &entry.value().residency {
        Some(residency) => observer.admitted(key, residency, || !entry.is_outdated()),
        None => {
            if observer.untracked_admission(key, || !entry.is_outdated()) {
                if let Some(publication) = &entry.value().untracked_publication {
                    publication.ack();
                }
            }
        }
    }
}

enum CacheBackend {
    Disabled,
    Plain(foyer::Cache<CachedKey, CachedEntry>),
    Observed(foyer::Cache<CachedKey, ObservedEntry>),
}

struct Observation {
    observer: Arc<CacheObserver<CachedKey>>,
    owner: ScopeHandle,
}

/// Scoped handles share one physical process cache. Only observed caches wrap
/// stored values; disabled observation keeps the original payload and loader path.
pub struct SharedSlateDbCache {
    backend: Arc<CacheBackend>,
    capacity_bytes: u64,
    observe_config: Option<ObserveConfig>,
    observation: Option<Observation>,
    // Retain the registered process handle while scoped facades exist. The
    // process registry must not allocate another cache when only facades remain.
    process_root: Option<Arc<SharedSlateDbCache>>,
}

struct LoaderMeasurementGuard {
    observer: Arc<CacheObserver<CachedKey>>,
    kind: EntryKind,
    episode: Option<EpisodeContext>,
    started: Instant,
    finished: bool,
}

impl LoaderMeasurementGuard {
    fn start(observer: Arc<CacheObserver<CachedKey>>, key: &CachedKey, kind: EntryKind) -> Self {
        let episode = observer.loader_episode(key);
        Self {
            observer,
            kind,
            episode,
            started: Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, result: &std::result::Result<CachedEntry, slatedb::Error>) {
        let elapsed = self.started.elapsed();
        let measurement = match result {
            Ok(value) => LoadMeasurement::Success {
                bytes: value.size() as u64,
                elapsed,
            },
            Err(_) => LoadMeasurement::Failed { elapsed },
        };
        self.observer.loaded(self.kind, self.episode.as_ref(), measurement);
        self.finished = true;
    }
}

impl Drop for LoaderMeasurementGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.observer.loaded(
                self.kind,
                self.episode.as_ref(),
                LoadMeasurement::Cancelled {
                    elapsed: self.started.elapsed(),
                },
            );
        }
    }
}

impl SharedSlateDbCache {
    pub fn with_config(config: SharedSlateDbCacheConfig) -> Self {
        let observer = config.observe.as_ref().map(|config| Arc::new(CacheObserver::new(config.clone())));
        let observation = observer.as_ref().map(|observer| Observation {
            observer: Arc::clone(observer),
            owner: observer.scope(""),
        });
        let backend = if config.capacity_bytes == 0 {
            CacheBackend::Disabled
        } else if let Some(observer) = observer {
            CacheBackend::Observed(
                foyer::CacheBuilder::new(config.capacity_bytes)
                    .with_weighter(|_, entry: &ObservedEntry| entry.value.size())
                    .with_event_listener(Arc::new(ObserverListener { observer }))
                    .build(),
            )
        } else {
            CacheBackend::Plain(
                foyer::CacheBuilder::new(config.capacity_bytes)
                    .with_weighter(|_, value: &CachedEntry| value.size())
                    .build(),
            )
        };
        Self {
            backend: Arc::new(backend),
            capacity_bytes: config.capacity_bytes as u64,
            observe_config: config.observe,
            observation,
            process_root: None,
        }
    }

    /// Attach a bounded diagnostic owner without creating another cache or
    /// changing the opaque physical keys supplied by SlateDB.
    pub fn scoped(self: &Arc<Self>, path: &str) -> Arc<Self> {
        let Some(observation) = &self.observation else {
            return Arc::clone(self);
        };
        Arc::new(Self {
            backend: Arc::clone(&self.backend),
            capacity_bytes: self.capacity_bytes,
            observe_config: self.observe_config.clone(),
            observation: Some(Observation {
                observer: Arc::clone(&observation.observer),
                owner: observation.observer.scope(path),
            }),
            process_root: Some(Arc::clone(self.process_root.as_ref().unwrap_or(self))),
        })
    }

    pub fn snapshot(&self) -> SlateDbCacheMetricsSnapshot {
        let (resident_bytes, entries, envelope_metadata_bytes) = match self.backend.as_ref() {
            CacheBackend::Disabled => (0, 0, 0),
            CacheBackend::Plain(cache) => (cache.usage() as u64, cache.entries() as u64, 0),
            CacheBackend::Observed(cache) => {
                let overhead = std::mem::size_of::<ObservedEntry>().saturating_sub(std::mem::size_of::<CachedEntry>()) as u64;
                (
                    cache.usage() as u64,
                    cache.entries() as u64,
                    (cache.entries() as u64).saturating_mul(overhead),
                )
            }
        };
        SlateDbCacheMetricsSnapshot {
            capacity_bytes: self.capacity_bytes,
            resident_bytes,
            entries,
            observe: self
                .observation
                .as_ref()
                .map_or_else(CacheObserveSnapshot::default, |context| CacheObserveSnapshot {
                    envelope_metadata_bytes,
                    ..context.observer.snapshot()
                }),
        }
    }

    /// Fetches an entry and reports how it was found. Only the call whose own loader ran
    /// reads object storage, so it is the miss. A resident entry or a concurrent fetch
    /// is a hit, as the `DbCache::fetch_block` contract defines.
    async fn fetch(&self, kind: EntryKind, key: CachedKey, loader: CacheLoader) -> std::result::Result<CacheFetch, slatedb::Error> {
        match self.backend.as_ref() {
            CacheBackend::Plain(cache) => {
                let loader_ran = Arc::new(AtomicBool::new(false));
                let flag = Arc::clone(&loader_ran);
                let entry = cache
                    .get_or_fetch(&key, move || async move {
                        flag.store(true, Ordering::Relaxed);
                        loader().await
                    })
                    .await
                    .map(|entry| entry.value().clone())
                    .map_err(|error| slatedb::Error::unavailable(error.to_string()))?;
                Ok(if loader_ran.load(Ordering::Relaxed) {
                    CacheFetch::miss(entry)
                } else {
                    CacheFetch::hit(entry)
                })
            }
            CacheBackend::Disabled => {
                let Some(context) = &self.observation else {
                    return loader().await.map(CacheFetch::miss);
                };
                context.observer.record_fetch_start(kind);
                context.observer.miss(&key, &context.owner, kind);
                let mut measurement = LoaderMeasurementGuard::start(Arc::clone(&context.observer), &key, kind);
                let result = loader().await;
                measurement.finish(&result);
                result.map(CacheFetch::miss)
            }
            CacheBackend::Observed(cache) => {
                let context = self.observation.as_ref().expect("observed backend has an observation context");
                let observer = Arc::clone(&context.observer);
                let owner = context.owner.clone();
                observer.record_fetch_start(kind);
                let loader_ran = Arc::new(AtomicBool::new(false));
                let flag = Arc::clone(&loader_ran);
                let loader_observer = Arc::clone(&observer);
                let loader_key = key.clone();
                let fetch = cache.get_or_fetch(&key, move || async move {
                    flag.store(true, Ordering::Relaxed);
                    let mut measurement = LoaderMeasurementGuard::start(Arc::clone(&loader_observer), &loader_key, kind);
                    let result = loader().await;
                    measurement.finish(&result);
                    result.map(|value| ObservedEntry::new(&loader_observer, &loader_key, owner, kind, value, measurement.episode.as_ref()))
                });
                // Foyer exposes whether this exact operation found a resident
                // entry, avoiding a racy preliminary contains lookup.
                let entry = match fetch.try_unwrap() {
                    Ok(entry) => {
                        observer.record_fetch_resident(kind);
                        record_admission(&observer, &key, &entry);
                        observer.hit(&key, entry.value().residency.as_ref(), kind);
                        return Ok(CacheFetch::hit(entry.value().value.clone()));
                    }
                    Err(fetch) => {
                        observer.miss(&key, &context.owner, kind);
                        let result = fetch.await;
                        if !loader_ran.load(Ordering::Relaxed) {
                            observer.record_coalesced_waiter(kind);
                        }
                        result.map_err(|error| slatedb::Error::unavailable(error.to_string()))?
                    }
                };
                // A coalesced waiter may resume before the leading caller.
                // Admission is idempotent for this actual returned token.
                record_admission(&observer, &key, &entry);
                let entry = entry.value().value.clone();
                Ok(if loader_ran.load(Ordering::Relaxed) {
                    CacheFetch::miss(entry)
                } else {
                    CacheFetch::hit(entry)
                })
            }
        }
    }

    async fn get(&self, kind: EntryKind, key: &CachedKey) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        match self.backend.as_ref() {
            CacheBackend::Disabled => {
                if let Some(context) = &self.observation {
                    context.observer.record_get_start(kind);
                    context.observer.miss(key, &context.owner, kind);
                }
                Ok(None)
            }
            CacheBackend::Plain(cache) => Ok(cache.get(key).map(|entry| entry.value().clone())),
            CacheBackend::Observed(cache) => {
                let context = self.observation.as_ref().expect("observed backend has an observation context");
                context.observer.record_get_start(kind);
                match cache.get(key) {
                    Some(entry) => {
                        record_admission(&context.observer, key, &entry);
                        context.observer.hit(key, entry.value().residency.as_ref(), kind);
                        Ok(Some(entry.value().value.clone()))
                    }
                    None => {
                        context.observer.miss(key, &context.owner, kind);
                        Ok(None)
                    }
                }
            }
        }
    }
}

#[async_trait]
impl DbCache for SharedSlateDbCache {
    async fn get_block(&self, key: &CachedKey) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        self.get(EntryKind::DataBlock, key).await
    }
    async fn get_index(&self, key: &CachedKey) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        self.get(EntryKind::Index, key).await
    }
    async fn get_filter(&self, key: &CachedKey) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        self.get(EntryKind::Filter, key).await
    }
    async fn get_stats(&self, key: &CachedKey) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
        self.get(EntryKind::Stats, key).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        match self.backend.as_ref() {
            CacheBackend::Disabled => {}
            CacheBackend::Plain(cache) => {
                cache.insert(key, value);
            }
            CacheBackend::Observed(cache) => {
                let context = self.observation.as_ref().expect("observed backend has an observation context");
                let episode = context.observer.loader_episode(&key);
                let entry = cache.insert(
                    key.clone(),
                    ObservedEntry::new(
                        &context.observer,
                        &key,
                        context.owner.clone(),
                        EntryKind::Unknown,
                        value,
                        episode.as_ref(),
                    ),
                );
                record_admission(&context.observer, &key, &entry);
            }
        }
    }

    async fn remove(&self, key: &CachedKey) {
        match self.backend.as_ref() {
            CacheBackend::Disabled => {}
            CacheBackend::Plain(cache) => {
                cache.remove(key);
            }
            CacheBackend::Observed(cache) => {
                cache.remove(key);
            }
        }
    }

    fn entry_count(&self) -> u64 {
        match self.backend.as_ref() {
            CacheBackend::Disabled => 0,
            CacheBackend::Plain(cache) => cache.entries() as u64,
            CacheBackend::Observed(cache) => cache.entries() as u64,
        }
    }
    async fn fetch_block(&self, key: CachedKey, loader: CacheLoader) -> std::result::Result<CacheFetch, slatedb::Error> {
        self.fetch(EntryKind::DataBlock, key, loader).await
    }
    async fn fetch_index(&self, key: CachedKey, loader: CacheLoader) -> std::result::Result<CacheFetch, slatedb::Error> {
        self.fetch(EntryKind::Index, key, loader).await
    }
    async fn fetch_filter(&self, key: CachedKey, loader: CacheLoader) -> std::result::Result<CacheFetch, slatedb::Error> {
        self.fetch(EntryKind::Filter, key, loader).await
    }
    async fn fetch_stats(&self, key: CachedKey, loader: CacheLoader) -> std::result::Result<CacheFetch, slatedb::Error> {
        self.fetch(EntryKind::Stats, key, loader).await
    }
}

static NEXT_DB_CACHE_ID: AtomicU64 = AtomicU64::new(1);

/// Returns a process-unique `db_cache_id` for one `Db` or `DbReader` open.
///
/// SlateDB keys every cache entry by this id. A fresh id per open matches the
/// SlateDB 0.16 behaviour, which numbered each cache wrapper. Opens never share
/// entries, so a graph path that is dropped and created again cannot read stale
/// blocks of a reused WAL file id.
pub fn next_db_cache_id() -> u64 {
    NEXT_DB_CACHE_ID.fetch_add(1, Ordering::Relaxed)
}

static PROCESS_SLATE_DB_CACHES: OnceLock<Mutex<BTreeMap<usize, Weak<SharedSlateDbCache>>>> = OnceLock::new();

pub fn process_slate_db_cache(object_store: &Arc<dyn ObjectStore>, capacity_bytes: usize) -> Result<Arc<SharedSlateDbCache>> {
    process_slate_db_cache_with_config(object_store, SharedSlateDbCacheConfig::new(capacity_bytes))
}

pub fn process_slate_db_cache_with_config(
    object_store: &Arc<dyn ObjectStore>,
    config: SharedSlateDbCacheConfig,
) -> Result<Arc<SharedSlateDbCache>> {
    let key = Arc::as_ptr(object_store) as *const () as usize;
    let caches = PROCESS_SLATE_DB_CACHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut caches = caches.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    caches.retain(|_, cache| cache.strong_count() > 0);
    if let Some(cache) = caches.get(&key).and_then(Weak::upgrade) {
        let configured = cache.capacity_bytes;
        if configured != config.capacity_bytes as u64 {
            return Err(GraphError::CorruptValue {
                key: "slatedb/process-cache".to_string(),
                reason: format!(
                    "object store already uses a {configured}-byte SlateDB cache; requested {} bytes",
                    config.capacity_bytes
                ),
            });
        }
        if cache.observe_config != config.observe {
            return Err(GraphError::CorruptValue {
                key: "slatedb/process-cache".to_string(),
                reason: "object store already uses a SlateDB cache with different observation settings; requested observation settings must match".to_string(),
            });
        }
        return Ok(cache);
    }
    let cache = Arc::new(SharedSlateDbCache::with_config(config));
    caches.insert(key, Arc::downgrade(&cache));
    Ok(cache)
}
