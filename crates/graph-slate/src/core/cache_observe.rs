//! Bounded measurement of actual cache residencies and retained removal history.
//!
//! Foyer values own their residency token. A removal identifies that exact token,
//! even when another value with the same physical key has already been admitted.
//! Callbacks update one observer shard directly, outside Foyer's locks. There is
//! no deferred callback queue or inference from callback delivery order.
//!
//! Logical misses and physical loaders have separate populations. A missing
//! retained ghost is unknown history. Intervals begin when the removal callback
//! is observed, rather than asserting a timestamp inside Foyer's shard lock.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{AtomicDurationHistogram, DurationHistogramSnapshot};

pub const MAX_OBSERVED_SCOPES: usize = 64;
pub const MAX_SCOPE_PATH_BYTES: usize = 1024;
const SHARD_COUNT: usize = 8;
const OWNER_COUNT: usize = MAX_OBSERVED_SCOPES + 1;
const GHOST_RECORD_BYTES: usize = 384;
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserveConfig {
    pub max_resident_records: usize,
    pub max_ghost_records: usize,
    /// Estimated dynamic record budget. Fixed observer counters are separate.
    pub max_metadata_bytes: usize,
    pub ghost_retention: Duration,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        Self {
            max_resident_records: 65_536,
            max_ghost_records: 65_536,
            max_metadata_bytes: 16 * 1024 * 1024,
            ghost_retention: Duration::from_secs(3600),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EntryKind {
    #[default]
    Unknown,
    DataBlock,
    Index,
    Filter,
    Stats,
}

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::DataBlock => "data_block",
            Self::Index => "index",
            Self::Filter => "filter",
            Self::Stats => "stats",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

pub const ENTRY_KIND_COUNT: usize = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovalReason {
    CapacityEviction,
    Replacement,
    ExplicitRemove,
    Clear,
}

impl RemovalReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CapacityEviction => "capacity_eviction",
            Self::Replacement => "replacement",
            Self::ExplicitRemove => "explicit_remove",
            Self::Clear => "clear",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

pub const REMOVAL_REASON_COUNT: usize = 4;

/// Elapsed microseconds. Keep this precision until after subtracting timestamps.
pub trait MonotonicClock: Send + Sync {
    fn now_us(&self) -> u64;
}

pub struct SystemClock(Instant);

impl Default for SystemClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl MonotonicClock for SystemClock {
    fn now_us(&self) -> u64 {
        self.0.elapsed().as_micros().min(u64::MAX as u128) as u64
    }
}

#[derive(Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance(&self, delta: Duration) {
        self.0.fetch_add(delta.as_micros().min(u64::MAX as u128) as u64, Ordering::SeqCst);
    }
}

impl MonotonicClock for ManualClock {
    fn now_us(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct ScopeCounters {
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
    misses_after_eviction: AtomicU64,
    reload_executions: AtomicU64,
    reload_bytes: AtomicU64,
    reload_failures: AtomicU64,
    reload_duration_us: AtomicU64,
    resident_dropped: AtomicU64,
    ghost_dropped: AtomicU64,
    ghost_expired: AtomicU64,
    metadata: [AtomicUsize; SHARD_COUNT],
    ghosts: [AtomicUsize; SHARD_COUNT],
    pending_publications: [AtomicU64; SHARD_COUNT],
}

struct ScopeRecord {
    slot: usize,
    path: String,
    counters: ScopeCounters,
}

#[derive(Clone)]
pub struct ScopeHandle(Arc<ScopeRecord>);

impl std::fmt::Debug for ScopeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ScopeHandle").field(&self.0.path).finish()
    }
}

/// A candidate may reach Foyer before its caller publishes admission. Until
/// publication/removal completes, this owner's shard history is uncertain.
pub struct PublicationGuard {
    owner: ScopeHandle,
    shard: usize,
    pending: AtomicBool,
}

impl PublicationGuard {
    pub fn ack(&self) {
        if self.pending.swap(false, Ordering::AcqRel) {
            self.owner.0.counters.pending_publications[self.shard].fetch_sub(1, Ordering::Release);
        }
    }
}

impl Drop for PublicationGuard {
    fn drop(&mut self) {
        self.ack();
    }
}

struct AcknowledgePublication<'a>(&'a PublicationGuard);

impl Drop for AcknowledgePublication<'_> {
    fn drop(&mut self) {
        self.0.ack();
    }
}

struct RecordPermit {
    owner: ScopeHandle,
    shard: usize,
    bytes: usize,
    token_slots: Option<Arc<[AtomicUsize; SHARD_COUNT]>>,
}

impl Drop for RecordPermit {
    fn drop(&mut self) {
        self.owner.0.counters.metadata[self.shard].fetch_sub(self.bytes, Ordering::Relaxed);
        if let Some(slots) = &self.token_slots {
            slots[self.shard].fetch_sub(1, Ordering::Relaxed);
        } else {
            self.owner.0.counters.ghosts[self.shard].fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy)]
enum ResidencyState {
    Pending,
    Resident { hits: u64 },
    Removed,
}

/// Identity and lifecycle of one actual Foyer value. Holds no cached payload.
/// A permit stays charged until the last value or observer reference is dropped.
pub struct Residency {
    id: u64,
    owner: ScopeHandle,
    kind: EntryKind,
    bytes: u64,
    episode: Option<u64>,
    state: Mutex<ResidencyState>,
    history_valid: AtomicBool,
    publication: PublicationGuard,
    _permit: RecordPermit,
}

struct GhostRecord {
    id: u64,
    at_us: u64,
    owner: ScopeHandle,
    reason: RemovalReason,
    _permit: RecordPermit,
}

struct ShardState<K> {
    resident: HashMap<K, Arc<Residency>>,
    ghost: HashMap<K, GhostRecord>,
    order: BTreeMap<(u64, u64), K>,
    owner_order: HashMap<usize, BTreeMap<(u64, u64), K>>,
}

impl<K> Default for ShardState<K> {
    fn default() -> Self {
        Self {
            resident: HashMap::new(),
            ghost: HashMap::new(),
            order: BTreeMap::new(),
            owner_order: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EpisodeContext {
    id: u64,
    owner: ScopeHandle,
    reason: RemovalReason,
}

#[derive(Clone, Debug)]
pub enum MissObservation {
    AfterEviction(EpisodeContext),
    AfterRemoval(RemovalReason),
    Unknown,
}

impl MissObservation {
    pub fn episode(&self) -> Option<EpisodeContext> {
        match self {
            Self::AfterEviction(episode) => Some(episode.clone()),
            _ => None,
        }
    }
}

pub enum LoadMeasurement {
    Success { bytes: u64, elapsed: Duration },
    Failed { elapsed: Duration },
    Cancelled { elapsed: Duration },
}

#[derive(Default)]
struct Counters {
    fetches: [AtomicU64; ENTRY_KIND_COUNT],
    gets: [AtomicU64; ENTRY_KIND_COUNT],
    hits: [AtomicU64; ENTRY_KIND_COUNT],
    tracked_hits: [AtomicU64; ENTRY_KIND_COUNT],
    misses_after_eviction: [AtomicU64; ENTRY_KIND_COUNT],
    misses_unknown: [AtomicU64; ENTRY_KIND_COUNT],
    admissions: [AtomicU64; ENTRY_KIND_COUNT],
    loader_executions: [AtomicU64; ENTRY_KIND_COUNT],
    loader_success_bytes: [AtomicU64; ENTRY_KIND_COUNT],
    loader_failures: [AtomicU64; ENTRY_KIND_COUNT],
    coalesced_waiters: [AtomicU64; ENTRY_KIND_COUNT],
    fetch_served_from_resident: [AtomicU64; ENTRY_KIND_COUNT],
    removals_by_reason: [AtomicU64; REMOVAL_REASON_COUNT],
    removal_bytes_by_reason: [AtomicU64; REMOVAL_REASON_COUNT],
    misses_by_removal_reason: [AtomicU64; REMOVAL_REASON_COUNT],
    returns_by_removal_reason: [AtomicU64; REMOVAL_REASON_COUNT],
    removals_untracked: AtomicU64,
    episodes_opened: AtomicU64,
    episodes_closed_returned: AtomicU64,
    episodes_closed_expired: AtomicU64,
    episodes_closed_dropped: AtomicU64,
    reinsertion_interval_ms_sum: AtomicU64,
    reinsertion_interval_ms_max: AtomicU64,
    reinsertion_intervals: AtomicU64,
    zero_hit_evictions: AtomicU64,
    readmissions_while_resident: AtomicU64,
    ghost_dropped: AtomicU64,
    ghost_expired: AtomicU64,
    resident_overflow: AtomicU64,
    untracked_hits: AtomicU64,
    ordering_gaps: AtomicU64,
    loader_cancelled: AtomicU64,
    reload_executions: AtomicU64,
    reload_success_bytes: AtomicU64,
    reload_failures: AtomicU64,
    scope_overflow: AtomicU64,
}

pub struct CacheObserver<K> {
    config: ObserveConfig,
    clock: Arc<dyn MonotonicClock>,
    generation: u64,
    sequence: AtomicU64,
    shards: [Mutex<ShardState<K>>; SHARD_COUNT],
    scopes: Mutex<Vec<ScopeHandle>>,
    overflow: ScopeHandle,
    token_slots: Arc<[AtomicUsize; SHARD_COUNT]>,
    counters: Counters,
    eviction_to_miss: AtomicDurationHistogram,
    reload_duration: AtomicDurationHistogram,
    reload_failed_duration: AtomicDurationHistogram,
}

impl<K: Clone + Eq + Hash> CacheObserver<K> {
    pub fn new(config: ObserveConfig) -> Self {
        Self::with_clock(config, Arc::new(SystemClock::default()))
    }

    pub fn with_clock(config: ObserveConfig, clock: Arc<dyn MonotonicClock>) -> Self {
        Self {
            config,
            clock,
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            sequence: AtomicU64::new(1),
            shards: std::array::from_fn(|_| Mutex::new(ShardState::default())),
            scopes: Mutex::new(Vec::new()),
            overflow: ScopeHandle(Arc::new(ScopeRecord {
                slot: 0,
                path: "__unattributed_or_overflow__".into(),
                counters: ScopeCounters::default(),
            })),
            token_slots: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
            counters: Counters::default(),
            eviction_to_miss: AtomicDurationHistogram::default(),
            reload_duration: AtomicDurationHistogram::default(),
            reload_failed_duration: AtomicDurationHistogram::default(),
        }
    }

    fn shard_index(key: &K) -> usize {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hash);
        hash.finish() as usize % SHARD_COUNT
    }

    pub fn scope(&self, path: &str) -> ScopeHandle {
        if path.is_empty() {
            return self.overflow.clone();
        }
        let mut scopes = self.scopes.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(scope) = scopes.iter().find(|scope| scope.0.path == path) {
            return scope.clone();
        }
        if scopes.len() == MAX_OBSERVED_SCOPES || path.len() > MAX_SCOPE_PATH_BYTES {
            self.counters.scope_overflow.fetch_add(1, Ordering::Relaxed);
            return self.overflow.clone();
        }
        let scope = ScopeHandle(Arc::new(ScopeRecord {
            slot: scopes.len() + 1,
            path: path.to_string(),
            counters: ScopeCounters::default(),
        }));
        scopes.push(scope.clone());
        scope
    }

    fn owner_budget(&self) -> usize {
        self.config.max_metadata_bytes / OWNER_COUNT / SHARD_COUNT
    }

    pub fn publication(&self, key: &K, owner: &ScopeHandle) -> PublicationGuard {
        let shard = Self::shard_index(key);
        owner.0.counters.pending_publications[shard].fetch_add(1, Ordering::AcqRel);
        PublicationGuard {
            owner: owner.clone(),
            shard,
            pending: AtomicBool::new(true),
        }
    }

    fn reserve(&self, owner: &ScopeHandle, shard: usize, bytes: usize, token: bool) -> Option<RecordPermit> {
        let used = &owner.0.counters.metadata[shard];
        let budget = self.owner_budget();
        used.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(bytes).filter(|next| *next <= budget)
        })
        .ok()?;
        if token
            && self.token_slots[shard]
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (current < self.config.max_resident_records).then_some(current + 1)
                })
                .is_err()
        {
            used.fetch_sub(bytes, Ordering::Relaxed);
            return None;
        }
        if !token
            && owner.0.counters.ghosts[shard]
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (current < (self.config.max_ghost_records / OWNER_COUNT).max(1)).then_some(current + 1)
                })
                .is_err()
        {
            used.fetch_sub(bytes, Ordering::Relaxed);
            return None;
        }
        Some(RecordPermit {
            owner: owner.clone(),
            shard,
            bytes,
            token_slots: token.then(|| self.token_slots.clone()),
        })
    }

    pub fn prepare(
        &self,
        key: &K,
        owner: &ScopeHandle,
        kind: EntryKind,
        bytes: u64,
        episode: Option<&EpisodeContext>,
    ) -> Option<Arc<Residency>> {
        // Include the token allocation and its resident-map key/Arc bookkeeping.
        let cost = std::mem::size_of::<Residency>() + std::mem::size_of::<K>() + 96;
        let permit = match self.reserve(owner, Self::shard_index(key), cost, true) {
            Some(permit) => permit,
            None => {
                self.counters.resident_overflow.fetch_add(1, Ordering::Relaxed);
                owner.0.counters.resident_dropped.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        Some(Arc::new(Residency {
            id: self.sequence.fetch_add(1, Ordering::Relaxed),
            owner: owner.clone(),
            kind,
            bytes,
            episode: episode.map(|episode| episode.id),
            state: Mutex::new(ResidencyState::Pending),
            history_valid: AtomicBool::new(true),
            publication: self.publication(key, owner),
            _permit: permit,
        }))
    }

    fn take_ghost(&self, state: &mut ShardState<K>, key: &K) -> Option<GhostRecord> {
        let ghost = state.ghost.remove(key)?;
        state.order.remove(&(ghost.at_us, ghost.id));
        if let Some(order) = state.owner_order.get_mut(&ghost.owner.0.slot) {
            order.remove(&(ghost.at_us, ghost.id));
        }
        Some(ghost)
    }

    fn drop_ghost(&self, ghost: GhostRecord, expired: bool) {
        if expired {
            self.counters.ghost_expired.fetch_add(1, Ordering::Relaxed);
            self.counters.episodes_closed_expired.fetch_add(1, Ordering::Relaxed);
            ghost.owner.0.counters.ghost_expired.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.ghost_dropped.fetch_add(1, Ordering::Relaxed);
            self.counters.episodes_closed_dropped.fetch_add(1, Ordering::Relaxed);
            ghost.owner.0.counters.ghost_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn expire(&self, state: &mut ShardState<K>, now: u64) {
        let retention = self.config.ghost_retention.as_micros().min(u64::MAX as u128) as u64;
        while let Some((&(at_us, _), _)) = state.order.first_key_value() {
            if now.saturating_sub(at_us) <= retention {
                break;
            }
            let (_, key) = state.order.pop_first().unwrap();
            if let Some(ghost) = self.take_ghost(state, &key) {
                self.drop_ghost(ghost, true);
            }
        }
    }

    pub fn admitted(&self, key: &K, residency: &Arc<Residency>, is_current: impl FnOnce() -> bool) {
        let mut lifecycle = residency.state.lock().unwrap_or_else(|p| p.into_inner());
        if !matches!(*lifecycle, ResidencyState::Pending) {
            return;
        }
        let mut state = self.shards[Self::shard_index(key)].lock().unwrap_or_else(|p| p.into_inner());
        if !is_current() {
            residency.history_valid.store(false, Ordering::Relaxed);
            self.counters.ordering_gaps.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let now = self.clock.now_us();
        self.expire(&mut state, now);
        *lifecycle = ResidencyState::Resident { hits: 0 };
        self.counters.admissions[residency.kind.index()].fetch_add(1, Ordering::Relaxed);
        if let Some(ghost) = self.take_ghost(&mut state, key) {
            if residency.episode == Some(ghost.id) {
                self.counters.episodes_closed_returned.fetch_add(1, Ordering::Relaxed);
                self.counters.returns_by_removal_reason[ghost.reason.index()].fetch_add(1, Ordering::Relaxed);
                if ghost.reason == RemovalReason::CapacityEviction {
                    let interval = now.saturating_sub(ghost.at_us) / 1000;
                    self.counters.reinsertion_intervals.fetch_add(1, Ordering::Relaxed);
                    self.counters.reinsertion_interval_ms_sum.fetch_add(interval, Ordering::Relaxed);
                    self.counters.reinsertion_interval_ms_max.fetch_max(interval, Ordering::Relaxed);
                }
            } else {
                self.drop_ghost(ghost, false);
            }
        }
        if let Some(previous) = state.resident.insert(key.clone(), residency.clone()) {
            previous.history_valid.store(false, Ordering::Relaxed);
            self.counters.ordering_gaps.fetch_add(1, Ordering::Relaxed);
            self.counters.readmissions_while_resident.fetch_add(1, Ordering::Relaxed);
        }
        drop(state);
        drop(lifecycle);
        residency.publication.ack();
    }

    pub fn untracked_admission(&self, key: &K, is_current: impl FnOnce() -> bool) -> bool {
        let mut state = self.shards[Self::shard_index(key)].lock().unwrap_or_else(|p| p.into_inner());
        if !is_current() {
            return false;
        }
        if let Some(previous) = state.resident.remove(key) {
            previous.history_valid.store(false, Ordering::Relaxed);
        }
        if let Some(ghost) = self.take_ghost(&mut state, key) {
            self.drop_ghost(ghost, false);
        }
        self.counters.ordering_gaps.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn hit(&self, _key: &K, residency: Option<&Arc<Residency>>, kind: EntryKind) {
        self.counters.hits[kind.index()].fetch_add(1, Ordering::Relaxed);
        if let Some(residency) = residency {
            let mut lifecycle = residency.state.lock().unwrap_or_else(|p| p.into_inner());
            if let ResidencyState::Resident { hits } = &mut *lifecycle {
                *hits += 1;
                self.counters.tracked_hits[kind.index()].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        self.counters.untracked_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn invalidate_history(&self, state: &mut ShardState<K>, key: &K) {
        // A callback without an observed residency cannot establish its order
        // against another same-key residency or removal. Keep actual resident
        // identity/reuse, but prevent either history from becoming a ghost.
        if let Some(current) = state.resident.get(key) {
            current.history_valid.store(false, Ordering::Relaxed);
        }
        if let Some(ghost) = self.take_ghost(state, key) {
            self.drop_ghost(ghost, false);
        }
        self.counters.ordering_gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn removed(
        &self,
        key: &K,
        residency: Option<&Arc<Residency>>,
        owner: &ScopeHandle,
        kind: EntryKind,
        reason: RemovalReason,
        bytes: u64,
    ) {
        self.counters.removals_by_reason[reason.index()].fetch_add(1, Ordering::Relaxed);
        self.counters.removal_bytes_by_reason[reason.index()].fetch_add(bytes, Ordering::Relaxed);
        if reason == RemovalReason::CapacityEviction {
            owner.0.counters.evictions.fetch_add(1, Ordering::Relaxed);
            owner.0.counters.evicted_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        let Some(residency) = residency else {
            self.counters.removals_untracked.fetch_add(1, Ordering::Relaxed);
            let mut state = self.shards[Self::shard_index(key)].lock().unwrap_or_else(|p| p.into_inner());
            self.invalidate_history(&mut state, key);
            return;
        };
        // Declared before the lifecycle/shard guards so acknowledgement runs
        // after their state updates and unlocks on every return path.
        let _publication = AcknowledgePublication(&residency.publication);
        let mut lifecycle = residency.state.lock().unwrap_or_else(|p| p.into_inner());
        let prior = *lifecycle;
        if matches!(prior, ResidencyState::Removed) {
            return;
        }
        *lifecycle = ResidencyState::Removed;
        let shard = Self::shard_index(key);
        let mut state = self.shards[shard].lock().unwrap_or_else(|p| p.into_inner());
        let now = self.clock.now_us();
        self.expire(&mut state, now);
        if matches!(prior, ResidencyState::Pending) {
            self.invalidate_history(&mut state, key);
            self.counters.removals_untracked.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !residency.history_valid.load(Ordering::Relaxed) {
            if state.resident.get(key).is_some_and(|current| current.id == residency.id) {
                state.resident.remove(key);
            }
            self.counters.removals_untracked.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if state.resident.get(key).is_some_and(|current| current.id != residency.id) {
            // A newer value is already observed. The old callback cannot create
            // a ghost for that resident key or change its reuse accounting.
            self.counters.ordering_gaps.fetch_add(1, Ordering::Relaxed);
            self.counters.removals_untracked.fetch_add(1, Ordering::Relaxed);
            return;
        }
        state.resident.remove(key);
        if let ResidencyState::Resident { hits: 0 } = prior {
            if reason == RemovalReason::CapacityEviction {
                self.counters.zero_hit_evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(ghost) = self.take_ghost(&mut state, key) {
            self.drop_ghost(ghost, false);
        }
        self.counters.episodes_opened.fetch_add(1, Ordering::Relaxed);
        let cost = GHOST_RECORD_BYTES + 3 * std::mem::size_of::<K>();
        let mut permit = None;
        loop {
            // The shard lock enforces the total cap even when a budget below
            // OWNER_COUNT cannot reserve one record for every owner.
            if state.ghost.len() < self.config.max_ghost_records {
                permit = self.reserve(owner, shard, cost, false);
                if permit.is_some() {
                    break;
                }
            }
            // Each owner has its own expiry index, avoiding scans of other
            // owners' records when this owner exhausts its quota.
            let oldest = state.owner_order.get_mut(&owner.0.slot).and_then(BTreeMap::pop_first);
            let Some((_, oldest)) = oldest else {
                break;
            };
            if let Some(ghost) = self.take_ghost(&mut state, &oldest) {
                self.drop_ghost(ghost, false);
            }
        }
        let Some(permit) = permit else {
            self.counters.ghost_dropped.fetch_add(1, Ordering::Relaxed);
            self.counters.episodes_closed_dropped.fetch_add(1, Ordering::Relaxed);
            owner.0.counters.ghost_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let ghost = GhostRecord {
            id: residency.id,
            at_us: now,
            owner: owner.clone(),
            reason,
            _permit: permit,
        };
        state.order.insert((now, ghost.id), key.clone());
        state
            .owner_order
            .entry(owner.0.slot)
            .or_default()
            .insert((now, ghost.id), key.clone());
        state.ghost.insert(key.clone(), ghost);
        let _ = kind;
    }

    pub fn miss(&self, key: &K, owner: &ScopeHandle, kind: EntryKind) -> MissObservation {
        let mut state = self.shards[Self::shard_index(key)].lock().unwrap_or_else(|p| p.into_inner());
        let now = self.clock.now_us();
        self.expire(&mut state, now);
        if let Some(ghost) = state.ghost.get(key) {
            if ghost.owner.0.counters.pending_publications[Self::shard_index(key)].load(Ordering::Acquire) != 0 {
                self.counters.ordering_gaps.fetch_add(1, Ordering::Relaxed);
                self.counters.misses_unknown[kind.index()].fetch_add(1, Ordering::Relaxed);
                return MissObservation::Unknown;
            }
            self.counters.misses_by_removal_reason[ghost.reason.index()].fetch_add(1, Ordering::Relaxed);
            if ghost.reason == RemovalReason::CapacityEviction {
                self.counters.misses_after_eviction[kind.index()].fetch_add(1, Ordering::Relaxed);
                owner.0.counters.misses_after_eviction.fetch_add(1, Ordering::Relaxed);
                self.eviction_to_miss.record_micros(now.saturating_sub(ghost.at_us));
                return MissObservation::AfterEviction(EpisodeContext {
                    id: ghost.id,
                    owner: ghost.owner.clone(),
                    reason: ghost.reason,
                });
            }
            return MissObservation::AfterRemoval(ghost.reason);
        }
        if state.resident.contains_key(key) {
            self.counters.ordering_gaps.fetch_add(1, Ordering::Relaxed);
        }
        self.counters.misses_unknown[kind.index()].fetch_add(1, Ordering::Relaxed);
        MissObservation::Unknown
    }

    /// Peek inside the actual loader without multiplying logical miss counts.
    pub fn loader_episode(&self, key: &K) -> Option<EpisodeContext> {
        let mut state = self.shards[Self::shard_index(key)].lock().unwrap_or_else(|p| p.into_inner());
        self.expire(&mut state, self.clock.now_us());
        state
            .ghost
            .get(key)
            .filter(|ghost| ghost.owner.0.counters.pending_publications[Self::shard_index(key)].load(Ordering::Acquire) == 0)
            .map(|ghost| EpisodeContext {
                id: ghost.id,
                owner: ghost.owner.clone(),
                reason: ghost.reason,
            })
    }

    pub fn loaded(&self, kind: EntryKind, episode: Option<&EpisodeContext>, measurement: LoadMeasurement) {
        self.counters.loader_executions[kind.index()].fetch_add(1, Ordering::Relaxed);
        match measurement {
            LoadMeasurement::Success { bytes, elapsed } => {
                self.counters.loader_success_bytes[kind.index()].fetch_add(bytes, Ordering::Relaxed);
                if let Some(episode) = episode.filter(|episode| episode.reason == RemovalReason::CapacityEviction) {
                    self.counters.reload_executions.fetch_add(1, Ordering::Relaxed);
                    self.counters.reload_success_bytes.fetch_add(bytes, Ordering::Relaxed);
                    self.reload_duration.record(elapsed);
                    episode.owner.0.counters.reload_executions.fetch_add(1, Ordering::Relaxed);
                    episode.owner.0.counters.reload_bytes.fetch_add(bytes, Ordering::Relaxed);
                    episode
                        .owner
                        .0
                        .counters
                        .reload_duration_us
                        .fetch_add(elapsed.as_micros().min(u64::MAX as u128) as u64, Ordering::Relaxed);
                }
            }
            LoadMeasurement::Failed { elapsed } => {
                self.counters.loader_failures[kind.index()].fetch_add(1, Ordering::Relaxed);
                if let Some(episode) = episode.filter(|episode| episode.reason == RemovalReason::CapacityEviction) {
                    self.counters.reload_failures.fetch_add(1, Ordering::Relaxed);
                    self.reload_failed_duration.record(elapsed);
                    episode.owner.0.counters.reload_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
            LoadMeasurement::Cancelled { elapsed: _ } => {
                self.counters.loader_cancelled.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_fetch_start(&self, kind: EntryKind) {
        self.counters.fetches[kind.index()].fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_get_start(&self, kind: EntryKind) {
        self.counters.gets[kind.index()].fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_coalesced_waiter(&self, kind: EntryKind) {
        self.counters.coalesced_waiters[kind.index()].fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_fetch_resident(&self, kind: EntryKind) {
        self.counters.fetch_served_from_resident[kind.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CacheObserveSnapshot {
        let scopes = self.scopes.lock().unwrap_or_else(|p| p.into_inner());
        let fixed_metadata_bytes = std::mem::size_of::<Self>()
            + std::mem::size_of::<[AtomicUsize; SHARD_COUNT]>()
            + 16
            + std::mem::size_of::<ScopeRecord>()
            + self.overflow.0.path.capacity()
            + 16
            + scopes.capacity() * std::mem::size_of::<ScopeHandle>()
            + scopes
                .iter()
                .map(|scope| std::mem::size_of::<ScopeRecord>() + scope.0.path.capacity() + 16)
                .sum::<usize>();
        let mut scope_handles = vec![self.overflow.clone()];
        scope_handles.extend(scopes.iter().cloned());
        drop(scopes);
        let mut owners: Vec<ScopeObserveSnapshot> = scope_handles.iter().map(ScopeObserveSnapshot::from_scope).collect();
        let mut resident_tracked = 0;
        let mut ghost_records = 0;
        for shard in &self.shards {
            let mut state = shard.lock().unwrap_or_else(|p| p.into_inner());
            self.expire(&mut state, self.clock.now_us());
            resident_tracked += state.resident.len() as u64;
            ghost_records += state.ghost.len() as u64;
            for residency in state.resident.values() {
                if let Some(owner) = owners.get_mut(residency.owner.0.slot) {
                    owner.resident_tracked += 1;
                    owner.resident_bytes += residency.bytes;
                }
            }
            for ghost in state.ghost.values() {
                if let Some(owner) = owners.get_mut(ghost.owner.0.slot) {
                    owner.ghost_records += 1;
                }
            }
        }
        // Snapshot cumulative scope counters after expiry too, so an expiry
        // first observed by this snapshot reconciles with aggregate counters.
        for (owner, handle) in owners.iter_mut().zip(scope_handles.iter()) {
            let resident_tracked = owner.resident_tracked;
            let resident_bytes = owner.resident_bytes;
            let ghost_records = owner.ghost_records;
            *owner = ScopeObserveSnapshot::from_scope(handle);
            owner.resident_tracked = resident_tracked;
            owner.resident_bytes = resident_bytes;
            owner.ghost_records = ghost_records;
            owner.metadata_bytes = handle
                .0
                .counters
                .metadata
                .iter()
                .map(|counter| counter.load(Ordering::Relaxed) as u64)
                .sum();
        }
        let metadata_bytes = owners.iter().map(|scope| scope.metadata_bytes).sum();
        let pending_admissions = owners.iter().map(|scope| scope.pending_admissions).sum();
        let intervals = self.counters.reinsertion_intervals.load(Ordering::Relaxed);
        macro_rules! vector {
            ($field:ident) => {
                self.counters
                    .$field
                    .iter()
                    .map(|counter| counter.load(Ordering::Relaxed))
                    .collect()
            };
        }
        macro_rules! scalar {
            ($field:ident) => {
                self.counters.$field.load(Ordering::Relaxed)
            };
        }
        CacheObserveSnapshot {
            generation: self.generation,
            fetches: vector!(fetches),
            gets: vector!(gets),
            hits: vector!(hits),
            tracked_hits: vector!(tracked_hits),
            misses_after_eviction: vector!(misses_after_eviction),
            misses_unknown: vector!(misses_unknown),
            admissions: vector!(admissions),
            loader_executions: vector!(loader_executions),
            loader_success_bytes: vector!(loader_success_bytes),
            loader_failures: vector!(loader_failures),
            coalesced_waiters: vector!(coalesced_waiters),
            fetch_served_from_resident: vector!(fetch_served_from_resident),
            removals_by_reason: vector!(removals_by_reason),
            removal_bytes_by_reason: vector!(removal_bytes_by_reason),
            misses_by_removal_reason: vector!(misses_by_removal_reason),
            returns_by_removal_reason: vector!(returns_by_removal_reason),
            removals_untracked: scalar!(removals_untracked),
            episodes_opened: scalar!(episodes_opened),
            episodes_closed_returned: scalar!(episodes_closed_returned),
            episodes_closed_expired: scalar!(episodes_closed_expired),
            episodes_closed_dropped: scalar!(episodes_closed_dropped),
            reinsertion_intervals: intervals,
            reinsertion_interval_ms_avg: scalar!(reinsertion_interval_ms_sum).checked_div(intervals).unwrap_or(0),
            reinsertion_interval_ms_max: scalar!(reinsertion_interval_ms_max),
            zero_hit_evictions: scalar!(zero_hit_evictions),
            readmissions_while_resident: scalar!(readmissions_while_resident),
            ghost_dropped: scalar!(ghost_dropped),
            ghost_expired: scalar!(ghost_expired),
            resident_overflow: scalar!(resident_overflow),
            untracked_hits: scalar!(untracked_hits),
            ordering_gaps: scalar!(ordering_gaps),
            resident_tracked,
            ghost_records,
            drops_observed: u64::from(
                scalar!(resident_overflow) + scalar!(ghost_dropped) + scalar!(ghost_expired) + scalar!(ordering_gaps) > 0,
            ),
            loader_cancelled: scalar!(loader_cancelled),
            reload_executions: scalar!(reload_executions),
            reload_success_bytes: scalar!(reload_success_bytes),
            reload_failures: scalar!(reload_failures),
            eviction_to_miss: self.eviction_to_miss.snapshot(),
            reload_duration: self.reload_duration.snapshot(),
            reload_failed_duration: self.reload_failed_duration.snapshot(),
            metadata_bytes,
            fixed_metadata_bytes: fixed_metadata_bytes as u64,
            envelope_metadata_bytes: 0,
            max_metadata_bytes: self.config.max_metadata_bytes as u64,
            max_resident_records: self.config.max_resident_records as u64,
            max_ghost_records: self.config.max_ghost_records as u64,
            ghost_retention_secs: self.config.ghost_retention.as_secs(),
            scope_overflow: scalar!(scope_overflow),
            pending_admissions,
            scopes: owners,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScopeObserveSnapshot {
    pub path: String,
    pub evictions: u64,
    pub evicted_bytes: u64,
    pub misses_after_eviction: u64,
    pub reload_executions: u64,
    pub reload_bytes: u64,
    pub reload_failures: u64,
    pub reload_duration_us: u64,
    pub resident_tracked: u64,
    pub resident_bytes: u64,
    pub ghost_records: u64,
    pub metadata_bytes: u64,
    pub resident_dropped: u64,
    pub ghost_dropped: u64,
    pub ghost_expired: u64,
    pub pending_admissions: u64,
}

impl ScopeObserveSnapshot {
    fn from_scope(scope: &ScopeHandle) -> Self {
        let counters = &scope.0.counters;
        Self {
            path: scope.0.path.clone(),
            evictions: counters.evictions.load(Ordering::Relaxed),
            evicted_bytes: counters.evicted_bytes.load(Ordering::Relaxed),
            misses_after_eviction: counters.misses_after_eviction.load(Ordering::Relaxed),
            reload_executions: counters.reload_executions.load(Ordering::Relaxed),
            reload_bytes: counters.reload_bytes.load(Ordering::Relaxed),
            reload_failures: counters.reload_failures.load(Ordering::Relaxed),
            reload_duration_us: counters.reload_duration_us.load(Ordering::Relaxed),
            resident_dropped: counters.resident_dropped.load(Ordering::Relaxed),
            ghost_dropped: counters.ghost_dropped.load(Ordering::Relaxed),
            ghost_expired: counters.ghost_expired.load(Ordering::Relaxed),
            pending_admissions: counters
                .pending_publications
                .iter()
                .map(|counter| counter.load(Ordering::Acquire))
                .sum(),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CacheObserveSnapshot {
    pub generation: u64,
    pub fetches: Vec<u64>,
    pub gets: Vec<u64>,
    pub hits: Vec<u64>,
    pub tracked_hits: Vec<u64>,
    pub misses_after_eviction: Vec<u64>,
    pub misses_unknown: Vec<u64>,
    pub admissions: Vec<u64>,
    pub loader_executions: Vec<u64>,
    pub loader_success_bytes: Vec<u64>,
    pub loader_failures: Vec<u64>,
    pub coalesced_waiters: Vec<u64>,
    pub fetch_served_from_resident: Vec<u64>,
    pub removals_by_reason: Vec<u64>,
    pub removal_bytes_by_reason: Vec<u64>,
    pub misses_by_removal_reason: Vec<u64>,
    pub returns_by_removal_reason: Vec<u64>,
    pub removals_untracked: u64,
    pub episodes_opened: u64,
    pub episodes_closed_returned: u64,
    pub episodes_closed_expired: u64,
    pub episodes_closed_dropped: u64,
    pub reinsertion_intervals: u64,
    pub reinsertion_interval_ms_avg: u64,
    pub reinsertion_interval_ms_max: u64,
    pub zero_hit_evictions: u64,
    pub readmissions_while_resident: u64,
    pub ghost_dropped: u64,
    pub ghost_expired: u64,
    pub resident_overflow: u64,
    pub untracked_hits: u64,
    pub resident_tracked: u64,
    pub ghost_records: u64,
    pub drops_observed: u64,
    pub ordering_gaps: u64,
    pub loader_cancelled: u64,
    pub reload_executions: u64,
    pub reload_success_bytes: u64,
    pub reload_failures: u64,
    pub eviction_to_miss: DurationHistogramSnapshot,
    pub reload_duration: DurationHistogramSnapshot,
    pub reload_failed_duration: DurationHistogramSnapshot,
    pub metadata_bytes: u64,
    pub fixed_metadata_bytes: u64,
    pub envelope_metadata_bytes: u64,
    pub max_metadata_bytes: u64,
    pub max_resident_records: u64,
    pub max_ghost_records: u64,
    pub ghost_retention_secs: u64,
    pub scope_overflow: u64,
    pub pending_admissions: u64,
    pub scopes: Vec<ScopeObserveSnapshot>,
}
