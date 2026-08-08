use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, OnceLock, Weak};
use std::time::{Duration, Instant};

use futures::{stream, StreamExt};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
use tokio::sync::Mutex;
use ulid::Ulid;

use crate::{validate_component, GraphError, GraphScope, Result};

const WRITER_LEASE_FORMAT: &str = "turbolay-writer-lease2";
const DEFAULT_WRITER_LEASE_DURATION: Duration = Duration::from_secs(30);
const SERVER_TIMESTAMP_RESOLUTION_GUARD: Duration = Duration::from_secs(1);
const MAX_WRITER_LEASE_CAS_ATTEMPTS: usize = 16;
const MAX_OBSERVED_WRITER_LEASES: usize = 65_536;
const MAX_CONCURRENT_WRITER_LEASE_RENEWALS: usize = 32;

static PROCESS_HOLDER_ID: OnceLock<String> = OnceLock::new();
static PROCESS_WRITER_LEASE_DIRECTORIES: OnceLock<
    StdMutex<BTreeMap<ProcessWriterLeaseDirectoryKey, Weak<ObjectStoreWriterLeaseDirectory>>>,
> = OnceLock::new();

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProcessWriterLeaseDirectoryKey {
    object_store: usize,
    base_path: String,
    lease_duration_ms: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ScopeLeaseUserKey {
    scope: GraphScope,
    node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterLeaseOwner {
    pub node_id: String,
    pub generation: u64,
    pub remaining_ms: u64,
}

#[derive(Debug)]
pub struct WriterLeaseRenewalFailure {
    pub scope: GraphScope,
    pub cell_id: String,
    pub error: GraphError,
    pub ownership_lost: bool,
}

#[derive(Clone)]
pub struct ObjectStoreWriterLeaseDirectory {
    base_path: String,
    object_store: Arc<dyn ObjectStore>,
    lease_duration: Duration,
    holder_id: String,
    local: Arc<StdMutex<BTreeMap<String, LocalWriterLease>>>,
    abandoned: Arc<StdMutex<BTreeMap<String, LocalWriterLease>>>,
    scope_users: Arc<StdMutex<BTreeMap<ScopeLeaseUserKey, usize>>>,
    observed: Arc<Mutex<BTreeMap<String, ObservedWriterLease>>>,
    clock_probe: Arc<AtomicU64>,
}

#[derive(Clone)]
struct LocalWriterLease {
    scope: GraphScope,
    cell_id: String,
    node_id: String,
    generation: u64,
    valid_until: Instant,
}

struct StoredWriterLease {
    node_id: String,
    holder_id: String,
    generation: u64,
    heartbeat: u64,
    duration: Duration,
    version: UpdateVersion,
    last_modified_ms: i64,
    released: bool,
}

struct ObservedWriterLease {
    fingerprint: WriterLeaseFingerprint,
    observed_at: Instant,
    remaining_at_observation: Duration,
}

type WriterLeaseFingerprint = (String, String, u64, u64, u64, bool);

impl ObjectStoreWriterLeaseDirectory {
    pub fn new(base_path: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self::with_duration(base_path, object_store, DEFAULT_WRITER_LEASE_DURATION)
    }

    pub fn with_duration(
        base_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        lease_duration: Duration,
    ) -> Self {
        Self::with_duration_and_holder(base_path, object_store, lease_duration, process_holder_id())
    }

    pub(crate) fn with_duration_and_holder(
        base_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        lease_duration: Duration,
        holder_id: impl Into<String>,
    ) -> Self {
        Self {
            base_path: base_path.into().trim_end_matches('/').to_string(),
            object_store,
            lease_duration: lease_duration.max(Duration::from_secs(1)),
            holder_id: holder_id.into(),
            local: Arc::new(StdMutex::new(BTreeMap::new())),
            abandoned: Arc::new(StdMutex::new(BTreeMap::new())),
            scope_users: Arc::new(StdMutex::new(BTreeMap::new())),
            observed: Arc::new(Mutex::new(BTreeMap::new())),
            clock_probe: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) fn register_scope(&self, scope: &GraphScope, node_id: &str) {
        let key = ScopeLeaseUserKey {
            scope: scope.clone(),
            node_id: node_id.to_string(),
        };
        *self.scope_users().entry(key).or_default() += 1;
    }

    pub async fn current_owner(
        &self,
        scope: &GraphScope,
        cell_id: &str,
    ) -> Result<Option<WriterLeaseOwner>> {
        validate_component("cell_id", cell_id)?;
        let path = self.lease_path(scope, cell_id);
        let Some(stored) = self.read_stored(&path).await? else {
            return Ok(None);
        };
        if stored.released {
            return Ok(None);
        }
        let remaining_ms = self.shared_remaining_ms(&path, &stored).await?;
        Ok((remaining_ms > 0).then_some(WriterLeaseOwner {
            node_id: stored.node_id,
            generation: stored.generation,
            remaining_ms,
        }))
    }

    pub(crate) fn holds_abandoned_local(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        node_id: &str,
    ) -> bool {
        let path = self.lease_path(scope, cell_id);
        let key = path.to_string();
        let mut abandoned = self.abandoned();
        let Some(lease) = abandoned.get(&key) else {
            return false;
        };
        if lease.node_id == node_id && Instant::now() < lease.valid_until {
            return true;
        }
        abandoned.remove(&key);
        false
    }

    pub async fn acquire_or_renew(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        node_id: &str,
    ) -> Result<u64> {
        self.acquire_or_renew_inner(scope, cell_id, node_id, false)
            .await
    }

    async fn acquire_or_renew_inner(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        node_id: &str,
        force_renew: bool,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("node_id", node_id)?;
        validate_component("writer_lease_holder", &self.holder_id)?;
        let path = self.lease_path(scope, cell_id);
        let cache_key = path.to_string();
        let renew_margin = self.lease_duration / 3;
        if force_renew {
            let local_is_valid = self.local().get(&cache_key).is_some_and(|lease| {
                lease.node_id == node_id && Instant::now() < lease.valid_until
            });
            if !local_is_valid {
                self.local().remove(&cache_key);
                return Err(GraphError::NotCellWriter {
                    cell_id: cell_id.to_string(),
                    owner: None,
                });
            }
        }
        if !force_renew {
            if let Some(local) = self.local().get(&cache_key) {
                if local.node_id == node_id && Instant::now() + renew_margin < local.valid_until {
                    return Ok(local.generation);
                }
            }
        }

        for _ in 0..MAX_WRITER_LEASE_CAS_ATTEMPTS {
            let current = self.read_stored(&path).await?;
            if let Some(stored) = current.as_ref().filter(|stored| !stored.released) {
                let remaining_ms = self.shared_remaining_ms(&path, stored).await?;
                let owned_by_this_process =
                    stored.node_id == node_id && stored.holder_id == self.holder_id;
                if remaining_ms > 0 && !owned_by_this_process {
                    self.local().remove(&cache_key);
                    self.abandoned().remove(&cache_key);
                    return Err(GraphError::NotCellWriter {
                        cell_id: cell_id.to_string(),
                        owner: Some(stored.node_id.clone()),
                    });
                }
            }

            let same_holder = current.as_ref().is_some_and(|stored| {
                !stored.released && stored.node_id == node_id && stored.holder_id == self.holder_id
            });
            let generation = current
                .as_ref()
                .map(|stored| {
                    if same_holder {
                        stored.generation
                    } else {
                        stored.generation.saturating_add(1)
                    }
                })
                .unwrap_or(1);
            let heartbeat = current
                .as_ref()
                .map(|stored| stored.heartbeat.saturating_add(1))
                .unwrap_or(1);
            let payload = encode_writer_lease(
                node_id,
                &self.holder_id,
                generation,
                heartbeat,
                self.lease_duration,
                false,
            )?;
            let mode = current
                .as_ref()
                .map(|stored| PutMode::Update(stored.version.clone()))
                .unwrap_or(PutMode::Create);
            let write_started = Instant::now();
            let write_result = self
                .object_store
                .put_opts(
                    &path,
                    PutPayload::from(payload.clone()),
                    PutOptions::from(mode),
                )
                .await;
            let write_result = match write_result {
                Err(ObjectStoreError::NotImplemented { .. }) if same_holder => {
                    // LocalFileSystem lacks conditional update. Overwrite is safe
                    // only for the still-valid incumbent; stale takeovers remain
                    // fail-closed because they require real compare-and-swap.
                    self.object_store
                        .put(&path, PutPayload::from(payload))
                        .await
                }
                result => result,
            };
            match write_result {
                Ok(_) => {
                    let valid_until = write_started + self.lease_duration;
                    if Instant::now() >= valid_until {
                        continue;
                    }
                    self.observe_successful_write(
                        &path,
                        (
                            node_id.to_string(),
                            self.holder_id.clone(),
                            generation,
                            heartbeat,
                            u64::try_from(self.lease_duration.as_millis()).unwrap_or(u64::MAX),
                            false,
                        ),
                        write_started,
                    )
                    .await;
                    let lease = LocalWriterLease {
                        scope: scope.clone(),
                        cell_id: cell_id.to_string(),
                        node_id: node_id.to_string(),
                        generation,
                        // Start the local validity window before the S3
                        // request. A slow response must shorten our lease,
                        // never let local authority outlive the durable
                        // object's server-timestamped expiry.
                        valid_until,
                    };
                    if force_renew {
                        let users = self.scope_users();
                        let key = ScopeLeaseUserKey {
                            scope: scope.clone(),
                            node_id: node_id.to_string(),
                        };
                        if !users.contains_key(&key) {
                            return Err(GraphError::NotCellWriter {
                                cell_id: cell_id.to_string(),
                                owner: Some(node_id.to_string()),
                            });
                        }
                        self.abandoned().remove(&cache_key);
                        self.local().insert(cache_key, lease);
                    } else {
                        self.abandoned().remove(&cache_key);
                        self.local().insert(cache_key, lease);
                    }
                    return Ok(generation);
                }
                Err(
                    ObjectStoreError::AlreadyExists { .. } | ObjectStoreError::Precondition { .. },
                ) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "writer_lease_cas",
            attempts: MAX_WRITER_LEASE_CAS_ATTEMPTS,
        })
    }

    pub async fn renew_local(&self, node_id: &str) -> Vec<WriterLeaseRenewalFailure> {
        let leases = self
            .local()
            .values()
            .filter(|lease| lease.node_id == node_id)
            .cloned()
            .collect::<Vec<_>>();
        stream::iter(leases)
            .map(|lease| async move {
                let result = self
                    .acquire_or_renew_inner(&lease.scope, &lease.cell_id, node_id, true)
                    .await;
                (lease, result)
            })
            .buffer_unordered(MAX_CONCURRENT_WRITER_LEASE_RENEWALS)
            .filter_map(|(lease, result)| async move {
                let Err(error) = result else {
                    return None;
                };
                let ownership_lost = !self.holds_valid_local(&lease.scope, &lease.cell_id, node_id);
                if ownership_lost {
                    self.local()
                        .remove(self.lease_path(&lease.scope, &lease.cell_id).as_ref());
                }
                Some(WriterLeaseRenewalFailure {
                    scope: lease.scope,
                    cell_id: lease.cell_id,
                    error,
                    ownership_lost,
                })
            })
            .collect()
            .await
    }

    pub async fn release_scope(
        &self,
        scope: &GraphScope,
        node_id: &str,
    ) -> Vec<(String, GraphError)> {
        let key = ScopeLeaseUserKey {
            scope: scope.clone(),
            node_id: node_id.to_string(),
        };
        let should_release = {
            let mut users = self.scope_users();
            match users.get_mut(&key) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                Some(_) => {
                    users.remove(&key);
                    true
                }
                None => true,
            }
        };
        if !should_release {
            return Vec::new();
        }
        let leases = {
            let mut local = self.local();
            let keys = local
                .iter()
                .filter(|(_, lease)| lease.scope == *scope && lease.node_id == node_id)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| local.remove(&key))
                .collect::<Vec<_>>()
        };
        let abandoned_leases = {
            let mut abandoned = self.abandoned();
            let keys = abandoned
                .iter()
                .filter(|(_, lease)| lease.scope == *scope && lease.node_id == node_id)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| abandoned.remove(&key))
                .collect::<Vec<_>>()
        };
        let mut failures = Vec::new();
        for lease in leases.into_iter().chain(abandoned_leases) {
            if let Err(error) = self
                .release_stored(scope, &lease.cell_id, node_id, lease.generation)
                .await
            {
                failures.push((lease.cell_id, error));
            }
        }
        failures
    }

    /// Stop renewing a scope without publishing an immediate ownership change.
    ///
    /// Scoped-runtime eviction is a local cache decision. Bolt clients can still
    /// hold a routing table that names this node, so marking the durable lease as
    /// released here creates an avoidable stale-route window. Parking the local
    /// record outside the renewal set lets the same process resume the lease if
    /// a cached route arrives, while an unused lease expires normally.
    pub(crate) fn abandon_scope(&self, scope: &GraphScope, node_id: &str) {
        let key = ScopeLeaseUserKey {
            scope: scope.clone(),
            node_id: node_id.to_string(),
        };
        let should_abandon = {
            let mut users = self.scope_users();
            match users.get_mut(&key) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                Some(_) => {
                    users.remove(&key);
                    true
                }
                None => true,
            }
        };
        if !should_abandon {
            return;
        }
        let leases = {
            let mut local = self.local();
            let keys = local
                .iter()
                .filter(|(_, lease)| lease.scope == *scope && lease.node_id == node_id)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| local.remove(&key).map(|lease| (key, lease)))
                .collect::<Vec<_>>()
        };
        let now = Instant::now();
        let mut abandoned = self.abandoned();
        abandoned.retain(|_, lease| now < lease.valid_until);
        abandoned.extend(leases);
    }

    pub async fn release_cell(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        node_id: &str,
    ) -> Result<()> {
        let path = self.lease_path(scope, cell_id);
        let lease = self
            .local()
            .remove(path.as_ref())
            .or_else(|| self.abandoned().remove(path.as_ref()));
        let Some(lease) = lease else {
            return Ok(());
        };
        self.release_stored(scope, cell_id, node_id, lease.generation)
            .await
    }

    pub(crate) fn holds_valid_local(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        node_id: &str,
    ) -> bool {
        self.local()
            .get(self.lease_path(scope, cell_id).as_ref())
            .is_some_and(|lease| lease.node_id == node_id && Instant::now() < lease.valid_until)
    }

    fn local(&self) -> StdMutexGuard<'_, BTreeMap<String, LocalWriterLease>> {
        self.local
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn abandoned(&self) -> StdMutexGuard<'_, BTreeMap<String, LocalWriterLease>> {
        self.abandoned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn scope_users(&self) -> StdMutexGuard<'_, BTreeMap<ScopeLeaseUserKey, usize>> {
        self.scope_users
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lease_path(&self, scope: &GraphScope, cell_id: &str) -> Path {
        Path::from(format!(
            "{}/_writer_leases/v2/{cell_id}",
            scope.scoped_store_path(&self.base_path)
        ))
    }

    async fn read_stored(&self, path: &Path) -> Result<Option<StoredWriterLease>> {
        let result = match self.object_store.get(path).await {
            Ok(result) => result,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let last_modified_ms = result.meta.last_modified.timestamp_millis();
        let bytes = result.bytes().await?;
        let decoded = decode_writer_lease(path, &bytes)?;
        Ok(Some(StoredWriterLease {
            node_id: decoded.node_id,
            holder_id: decoded.holder_id,
            generation: decoded.generation,
            heartbeat: decoded.heartbeat,
            duration: Duration::from_millis(decoded.duration_ms),
            version,
            last_modified_ms,
            released: decoded.released,
        }))
    }

    async fn shared_remaining_ms(&self, path: &Path, stored: &StoredWriterLease) -> Result<u64> {
        let duration_ms = u64::try_from(stored.duration.as_millis()).unwrap_or(u64::MAX);
        let fingerprint = (
            stored.node_id.clone(),
            stored.holder_id.clone(),
            stored.generation,
            stored.heartbeat,
            duration_ms,
            stored.released,
        );
        let now = Instant::now();
        let mut observed = self.observed.lock().await;
        if let Some(entry) = observed.get(path.as_ref()) {
            if entry.fingerprint == fingerprint {
                return Ok(remaining_after_elapsed(entry, now));
            }
        }
        drop(observed);

        let server_now = self.probe_server_time().await?;
        let durable_age = Duration::from_millis(
            u64::try_from(server_now.saturating_sub(stored.last_modified_ms)).unwrap_or(0),
        );
        let remaining = stored
            .duration
            .saturating_add(SERVER_TIMESTAMP_RESOLUTION_GUARD)
            .saturating_sub(durable_age);

        observed = self.observed.lock().await;
        if observed.len() >= MAX_OBSERVED_WRITER_LEASES && !observed.contains_key(path.as_ref()) {
            if let Some(key) = observed.keys().next().cloned() {
                observed.remove(&key);
            }
        }
        observed.insert(
            path.to_string(),
            ObservedWriterLease {
                fingerprint,
                observed_at: now,
                remaining_at_observation: remaining,
            },
        );
        Ok(u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX))
    }

    async fn probe_server_time(&self) -> Result<i64> {
        let path = Path::from(format!("{}/_coordination/v1/server-clock", self.base_path));
        let probe = self.clock_probe.fetch_add(1, Ordering::Relaxed);
        self.object_store
            .put(&path, PutPayload::from(probe.to_string()))
            .await?;
        Ok(self
            .object_store
            .head(&path)
            .await?
            .last_modified
            .timestamp_millis())
    }

    async fn observe_successful_write(
        &self,
        path: &Path,
        fingerprint: WriterLeaseFingerprint,
        observed_at: Instant,
    ) {
        let released = fingerprint.5;
        let duration = Duration::from_millis(fingerprint.4);
        self.observed.lock().await.insert(
            path.to_string(),
            ObservedWriterLease {
                fingerprint,
                observed_at,
                remaining_at_observation: if released {
                    Duration::ZERO
                } else {
                    duration.saturating_add(SERVER_TIMESTAMP_RESOLUTION_GUARD)
                },
            },
        );
    }

    async fn release_stored(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        node_id: &str,
        generation: u64,
    ) -> Result<()> {
        let path = self.lease_path(scope, cell_id);
        for _ in 0..MAX_WRITER_LEASE_CAS_ATTEMPTS {
            let Some(current) = self.read_stored(&path).await? else {
                return Ok(());
            };
            if current.node_id != node_id
                || current.holder_id != self.holder_id
                || current.generation != generation
            {
                return Ok(());
            }
            let heartbeat = current.heartbeat.saturating_add(1);
            let duration = current.duration;
            let payload = encode_writer_lease(
                node_id,
                &self.holder_id,
                generation,
                heartbeat,
                duration,
                true,
            )?;
            let write_started = Instant::now();
            let release_result = self
                .object_store
                .put_opts(
                    &path,
                    PutPayload::from(payload),
                    PutOptions::from(PutMode::Update(current.version)),
                )
                .await;
            let release_result = match release_result {
                Err(ObjectStoreError::NotImplemented { .. }) => {
                    // LocalFileSystem has no conditional update. The local lease
                    // was removed before this method and forced renewals cannot
                    // recreate it, so deleting the incumbent's record is safe.
                    self.object_store.delete(&path).await?;
                    return Ok(());
                }
                result => result,
            };
            match release_result {
                Ok(_) => {
                    self.observe_successful_write(
                        &path,
                        (
                            node_id.to_string(),
                            self.holder_id.clone(),
                            generation,
                            heartbeat,
                            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                            true,
                        ),
                        write_started,
                    )
                    .await;
                    return Ok(());
                }
                Err(
                    ObjectStoreError::AlreadyExists { .. } | ObjectStoreError::Precondition { .. },
                ) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "writer_lease_release_cas",
            attempts: MAX_WRITER_LEASE_CAS_ATTEMPTS,
        })
    }
}

pub(crate) fn process_writer_lease_directory(
    base_path: impl Into<String>,
    object_store: Arc<dyn ObjectStore>,
    lease_duration: Duration,
) -> Arc<ObjectStoreWriterLeaseDirectory> {
    let base_path = base_path.into().trim_end_matches('/').to_string();
    let lease_duration = lease_duration.max(Duration::from_secs(1));
    let key = ProcessWriterLeaseDirectoryKey {
        object_store: Arc::as_ptr(&object_store) as *const () as usize,
        base_path: base_path.clone(),
        lease_duration_ms: u64::try_from(lease_duration.as_millis()).unwrap_or(u64::MAX),
    };
    let directories =
        PROCESS_WRITER_LEASE_DIRECTORIES.get_or_init(|| StdMutex::new(BTreeMap::new()));
    let mut directories = directories
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    directories.retain(|_, directory| directory.strong_count() > 0);
    if let Some(directory) = directories.get(&key).and_then(Weak::upgrade) {
        return directory;
    }
    let directory = Arc::new(ObjectStoreWriterLeaseDirectory::with_duration(
        base_path,
        object_store,
        lease_duration,
    ));
    directories.insert(key, Arc::downgrade(&directory));
    directory
}

fn process_holder_id() -> String {
    PROCESS_HOLDER_ID
        .get_or_init(|| Ulid::new().to_string())
        .clone()
}

fn remaining_after_elapsed(entry: &ObservedWriterLease, now: Instant) -> u64 {
    let remaining = entry
        .remaining_at_observation
        .saturating_sub(now.saturating_duration_since(entry.observed_at));
    u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX)
}

fn encode_writer_lease(
    node_id: &str,
    holder_id: &str,
    generation: u64,
    heartbeat: u64,
    duration: Duration,
    released: bool,
) -> Result<String> {
    validate_component("node_id", node_id)?;
    validate_component("writer_lease_holder", holder_id)?;
    let duration_ms =
        u64::try_from(duration.as_millis()).map_err(|_| GraphError::CorruptValue {
            key: "writer-lease/duration".to_string(),
            reason: "writer lease duration exceeds u64 milliseconds".to_string(),
        })?;
    let state = if released { "released" } else { "active" };
    Ok(format!(
        "{WRITER_LEASE_FORMAT}\n{node_id}\n{holder_id}\n{generation}\n{heartbeat}\n{duration_ms}\n{state}\n"
    ))
}

struct DecodedWriterLease {
    node_id: String,
    holder_id: String,
    generation: u64,
    heartbeat: u64,
    duration_ms: u64,
    released: bool,
}

fn decode_writer_lease(path: &Path, bytes: &[u8]) -> Result<DecodedWriterLease> {
    let value = std::str::from_utf8(bytes).map_err(|error| GraphError::CorruptValue {
        key: path.to_string(),
        reason: format!("writer lease is not UTF-8: {error}"),
    })?;
    let mut lines = value.lines();
    if lines.next() != Some(WRITER_LEASE_FORMAT) {
        return Err(GraphError::CorruptValue {
            key: path.to_string(),
            reason: "unsupported writer lease format".to_string(),
        });
    }
    let node_id = lines.next().unwrap_or_default().to_string();
    let holder_id = lines.next().unwrap_or_default().to_string();
    validate_component("node_id", &node_id)?;
    validate_component("writer_lease_holder", &holder_id)?;
    let generation = parse_lease_u64(path, "generation", lines.next())?;
    let heartbeat = parse_lease_u64(path, "heartbeat", lines.next())?;
    let duration_ms = parse_lease_u64(path, "duration_ms", lines.next())?;
    let released = match lines.next() {
        Some("active") => false,
        Some("released") => true,
        _ => {
            return Err(GraphError::CorruptValue {
                key: path.to_string(),
                reason: "writer lease has an invalid state".to_string(),
            });
        }
    };
    if generation == 0 || heartbeat == 0 || duration_ms == 0 || lines.next().is_some() {
        return Err(GraphError::CorruptValue {
            key: path.to_string(),
            reason: "writer lease has an invalid duration or trailing fields".to_string(),
        });
    }
    Ok(DecodedWriterLease {
        node_id,
        holder_id,
        generation,
        heartbeat,
        duration_ms,
        released,
    })
}

fn parse_lease_u64(path: &Path, field: &str, value: Option<&str>) -> Result<u64> {
    value
        .unwrap_or_default()
        .parse::<u64>()
        .map_err(|error| GraphError::CorruptValue {
            key: path.to_string(),
            reason: format!("invalid writer lease {field}: {error}"),
        })
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;

    use super::*;

    fn directory(
        store: Arc<dyn ObjectStore>,
        holder: &str,
        duration: Duration,
    ) -> ObjectStoreWriterLeaseDirectory {
        ObjectStoreWriterLeaseDirectory::with_duration_and_holder(
            "graph/data",
            store,
            duration,
            holder,
        )
    }

    #[tokio::test]
    async fn only_one_process_acquires_a_cell_writer_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = directory(Arc::clone(&store), "holder-a", Duration::from_secs(5));
        let second = directory(store, "holder-b", Duration::from_secs(5));
        let scope = GraphScope::default();

        assert_eq!(
            first
                .acquire_or_renew(&scope, "cell-0", "node-0")
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            second
                .acquire_or_renew(&scope, "cell-0", "node-1")
                .await
                .unwrap_err(),
            GraphError::NotCellWriter { owner: Some(owner), .. } if owner == "node-0"
        ));
    }

    #[tokio::test]
    async fn duplicate_runtimes_do_not_release_a_shared_process_lease_early() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base_path = format!("graph/shared-{}", Ulid::new());
        let first = process_writer_lease_directory(
            base_path.clone(),
            Arc::clone(&store),
            Duration::from_secs(5),
        );
        let second =
            process_writer_lease_directory(base_path, Arc::clone(&store), Duration::from_secs(5));
        assert!(Arc::ptr_eq(&first, &second));

        let scope = GraphScope::default();
        first.register_scope(&scope, "node-a");
        second.register_scope(&scope, "node-a");
        first
            .acquire_or_renew(&scope, "cell-0", "node-a")
            .await
            .unwrap();

        assert!(first.release_scope(&scope, "node-a").await.is_empty());
        assert_eq!(
            second
                .current_owner(&scope, "cell-0")
                .await
                .unwrap()
                .unwrap()
                .node_id,
            "node-a"
        );

        assert!(second.release_scope(&scope, "node-a").await.is_empty());
        assert!(first
            .current_owner(&scope, "cell-0")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn abandoned_scope_keeps_durable_owner_and_can_resume() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let directory = directory(store, "holder-a", Duration::from_secs(5));
        let scope = GraphScope::default();
        directory.register_scope(&scope, "node-a");
        let generation = directory
            .acquire_or_renew(&scope, "cell-0", "node-a")
            .await
            .unwrap();

        directory.abandon_scope(&scope, "node-a");

        assert!(directory.holds_abandoned_local(&scope, "cell-0", "node-a"));
        assert_eq!(
            directory
                .current_owner(&scope, "cell-0")
                .await
                .unwrap()
                .unwrap()
                .node_id,
            "node-a"
        );
        directory.register_scope(&scope, "node-a");
        assert_eq!(
            directory
                .acquire_or_renew(&scope, "cell-0", "node-a")
                .await
                .unwrap(),
            generation
        );
    }

    #[tokio::test]
    async fn concurrent_contenders_produce_exactly_one_owner() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = directory(Arc::clone(&store), "holder-a", Duration::from_secs(5));
        let second = directory(store, "holder-b", Duration::from_secs(5));
        let scope = GraphScope::default();

        let (first_result, second_result) = tokio::join!(
            first.acquire_or_renew(&scope, "cell-0", "node-0"),
            second.acquire_or_renew(&scope, "cell-0", "node-1"),
        );
        assert_ne!(first_result.is_ok(), second_result.is_ok());
        let loser = first_result.err().or_else(|| second_result.err()).unwrap();
        assert!(matches!(
            loser,
            GraphError::NotCellWriter { owner: Some(_), .. }
        ));
    }

    #[tokio::test]
    async fn restarted_process_with_same_node_id_cannot_share_the_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old = directory(Arc::clone(&store), "old-process", Duration::from_secs(5));
        let replacement = directory(store, "new-process", Duration::from_secs(5));
        let scope = GraphScope::default();

        old.acquire_or_renew(&scope, "cell-0", "node-0")
            .await
            .unwrap();
        assert!(matches!(
            replacement
                .acquire_or_renew(&scope, "cell-0", "node-0")
                .await
                .unwrap_err(),
            GraphError::NotCellWriter { owner: Some(owner), .. } if owner == "node-0"
        ));
    }

    #[tokio::test]
    async fn fresh_observer_does_not_restart_an_expired_lease_window() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = directory(Arc::clone(&store), "holder-a", Duration::from_secs(1));
        let scope = GraphScope::default();
        first
            .acquire_or_renew(&scope, "cell-0", "node-0")
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(2_100)).await;

        let restarted = directory(store, "holder-b", Duration::from_secs(1));
        assert!(restarted
            .current_owner(&scope, "cell-0")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            restarted
                .acquire_or_renew(&scope, "cell-0", "node-1")
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn release_is_conditional_on_process_identity() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = directory(Arc::clone(&store), "holder-a", Duration::from_secs(5));
        let stale = directory(Arc::clone(&store), "holder-b", Duration::from_secs(5));
        let observer = directory(store, "holder-c", Duration::from_secs(5));
        let scope = GraphScope::default();

        first
            .acquire_or_renew(&scope, "cell-0", "node-0")
            .await
            .unwrap();
        stale
            .release_cell(&scope, "cell-0", "node-0")
            .await
            .unwrap();
        assert_eq!(
            observer
                .current_owner(&scope, "cell-0")
                .await
                .unwrap()
                .unwrap()
                .node_id,
            "node-0"
        );
    }
}
