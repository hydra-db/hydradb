//! Lease-owned, forward-only publication of a derived object in object storage.
//!
//! A *publication* is one logical object -- the planner statistics of a cell,
//! say -- that several processes may rebuild and that readers want the newest
//! of. It lives under one root path as:
//!
//! - `{root}/current`: the **control record**, one small object holding who
//!   owns the publication right now, under which ownership epoch and until
//!   when, plus what is published: the graph sequence it was built from, a
//!   unique publication id, and the immutable payload it points at;
//! - `{root}/payloads/{publication_id}`: the **payloads**, written once and
//!   never changed. A reader that holds a record can fetch its payload
//!   without racing a writer.
//!
//! Three rules make several writers safe without any locking service:
//!
//! 1. **Ownership is a lease, taken atomically.** [`PublicationStore::acquire`]
//!    reads the record and writes it back conditionally on the version it
//!    read, bumping the epoch, and only if no other owner holds a live lease.
//!    The expiry check and the write are one conditional update, so two
//!    takers cannot both win, and a crashed owner blocks nobody past its
//!    lease.
//! 2. **An epoch fences a stale owner.** Every write by an [`Ownership`]
//!    refreshes the record first and refuses to continue if the epoch moved:
//!    an owner that paused, lost its lease to a takeover and resumed gets
//!    [`PublicationError::Fenced`], never a write.
//! 3. **The published sequence only moves forward.** [`Ownership::publish`]
//!    uploads its payload first, then updates the record conditionally; on
//!    every retry it re-reads and refuses a sequence at or below the one
//!    published. A build from an older snapshot that finishes last is dropped.
//!
//! The record and payloads stay readable with no owner at all, so an expired
//! lease never hides the last publication. A `current` object that is not a
//! control record -- the layout before this crate -- reads back as
//! [`Current::Opaque`] until the first acquisition, which retains it as the
//! published payload (with the sequence [`PublicationStore::with_legacy_sequence`]
//! reads from it) before anything else happens, so a build that fails after
//! taking over leaves readers what they had.
//!
//! The conditional read-modify-write is `slatedb-txn-obj`'s
//! [`SimpleTransactionalObject`] over a storage protocol of this crate's own,
//! [`SingleObjectProtocol`], which keeps one physical `current` object and
//! uses the store's ETag or version for the condition instead of the numbered
//! files SlateDB's own protocol writes. The fork's epoch-fencing wrapper is not
//! used because it claims a new epoch unconditionally on initialization; the
//! lease check here has to be part of the same conditional write.

#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic, clippy::unwrap_used))]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::object_store::path::Path;
use slatedb::object_store::{Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use slatedb_txn_obj::{
    DirtyObject, ObjectCodec, SimpleTransactionalObject, TransactionalObject, TransactionalObjectError, TransactionalStorageProtocol,
};

/// Durable format identifier of the control record. The version is part of
/// it, so a reader rejects a newer record rather than misreading it.
const RECORD_MAGIC: &str = "hydradb-object-publication-v1";
const RECORD_NAME: &str = "current";
const PAYLOADS_DIR: &str = "payloads";
/// Conditional writes retried on a version conflict before giving up. Each
/// retry re-reads the record, so a long-running contender resolves within
/// a few rounds; exhaustion means the record is being hammered.
const MAX_ATTEMPTS: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    /// The ownership epoch moved under this owner: another process acquired
    /// the publication, and nothing this owner does may land.
    #[error("publication ownership was taken over")]
    Fenced,
    /// The conditional write kept losing to concurrent writers.
    #[error("publication record conflicted {attempts} times")]
    Conflict { attempts: usize },
    #[error("object store error: {0}")]
    ObjectStore(#[from] StoreError),
    /// The control record did not decode: the format is newer than this
    /// crate, or the object was damaged.
    #[error("invalid publication record: {0}")]
    InvalidRecord(String),
}

impl From<TransactionalObjectError> for PublicationError {
    fn from(error: TransactionalObjectError) -> Self {
        match error {
            TransactionalObjectError::ObjectStoreError(error) => Self::ObjectStore(error),
            TransactionalObjectError::Fenced => Self::Fenced,
            TransactionalObjectError::CallbackError(error) => Self::InvalidRecord(error.to_string()),
            other => Self::InvalidRecord(other.to_string()),
        }
    }
}

/// The version an object store reports for one object: its ETag, its
/// version id, or both. Equal versions mean the same stored bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectVersion {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl ObjectVersion {
    fn update(&self) -> UpdateVersion {
        UpdateVersion {
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        }
    }
}

/// What `current` says. Everything is optional because a record can exist
/// with an owner and nothing published, or with a publication and no owner.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ControlRecord {
    /// Bumped on every acquisition; an owner's writes carry the epoch it
    /// acquired under and are refused once it moved.
    pub epoch: u64,
    /// The session holding the lease, if any.
    pub owner: Option<String>,
    /// When the lease ends, as Unix milliseconds. Zero when unowned.
    pub lease_expires_at_ms: u64,
    /// The graph sequence the published payload was built from.
    pub published_sequence: Option<u64>,
    /// Unique per publication; the payload's name.
    pub publication_id: Option<String>,
    /// Path of the published payload, relative to the root.
    pub payload: Option<String>,
    /// The payload before it, kept one publication longer so a reader that
    /// loaded the previous record can still fetch what it points at.
    pub previous_payload: Option<String>,
    /// `current` existed but was not a control record: the object itself,
    /// in the layout before this crate. Unowned, and nothing published yet;
    /// the first acquisition retains these bytes as the published payload.
    /// Never encoded.
    pub legacy: Option<Bytes>,
}

impl ControlRecord {
    fn owned_by_other(&self, owner: &str, now_ms: u64) -> Option<(String, u64)> {
        match &self.owner {
            Some(holder) if holder != owner && self.lease_expires_at_ms > now_ms => Some((holder.clone(), self.lease_expires_at_ms)),
            _ => None,
        }
    }
}

/// Tab-separated text, one field per line, after the magic line.
struct RecordCodec;

impl ObjectCodec<ControlRecord> for RecordCodec {
    fn encode(&self, value: &ControlRecord) -> Bytes {
        let mut out = String::new();
        out.push_str(RECORD_MAGIC);
        out.push('\n');
        out.push_str(&format!("epoch\t{}\n", value.epoch));
        if let Some(owner) = &value.owner {
            out.push_str(&format!("owner\t{owner}\n"));
        }
        out.push_str(&format!("lease_expires_at_ms\t{}\n", value.lease_expires_at_ms));
        if let Some(sequence) = value.published_sequence {
            out.push_str(&format!("published_sequence\t{sequence}\n"));
        }
        if let Some(id) = &value.publication_id {
            out.push_str(&format!("publication_id\t{id}\n"));
        }
        if let Some(payload) = &value.payload {
            out.push_str(&format!("payload\t{payload}\n"));
        }
        if let Some(previous) = &value.previous_payload {
            out.push_str(&format!("previous_payload\t{previous}\n"));
        }
        Bytes::from(out)
    }

    fn decode(&self, bytes: &Bytes) -> Result<ControlRecord, Box<dyn std::error::Error + Send + Sync>> {
        let legacy = || ControlRecord {
            legacy: Some(bytes.clone()),
            ..ControlRecord::default()
        };
        let Ok(text) = std::str::from_utf8(bytes) else {
            return Ok(legacy());
        };
        let mut lines = text.lines();
        match lines.next() {
            Some(RECORD_MAGIC) => {}
            Some(other) if other.starts_with("hydradb-object-publication-") => {
                return Err(format!("unsupported publication record version {other}").into());
            }
            _ => return Ok(legacy()),
        }
        let mut record = ControlRecord::default();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let Some((field, value)) = line.split_once('\t') else {
                return Err(format!("invalid publication record line {line:?}").into());
            };
            let number = || value.parse::<u64>().map_err(|error| format!("invalid {field}: {error}"));
            match field {
                "epoch" => record.epoch = number()?,
                "owner" => record.owner = Some(value.to_string()),
                "lease_expires_at_ms" => record.lease_expires_at_ms = number()?,
                "published_sequence" => record.published_sequence = Some(number()?),
                "publication_id" => record.publication_id = Some(value.to_string()),
                "payload" => record.payload = Some(value.to_string()),
                "previous_payload" => record.previous_payload = Some(value.to_string()),
                other => return Err(format!("unknown publication record field {other}").into()),
            }
        }
        Ok(record)
    }
}

/// One physical object, versioned by the store's ETag or version id.
///
/// `slatedb-txn-obj`'s own object-store protocol writes every version as a
/// new numbered file. For a record that readers fetch by a fixed name, this
/// keeps one `current` and conditions each write on the version last read:
/// create-only when none was read, update-if-unchanged otherwise. A failed
/// condition is `ObjectVersionExists`, which the transactional object treats
/// as "refresh and retry".
pub struct SingleObjectProtocol<T> {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    codec: Box<dyn ObjectCodec<T>>,
}

impl<T> SingleObjectProtocol<T> {
    pub fn new(object_store: Arc<dyn ObjectStore>, path: Path, codec: Box<dyn ObjectCodec<T>>) -> Self {
        Self { object_store, path, codec }
    }
}

#[async_trait]
impl<T: Send + Sync> TransactionalStorageProtocol<T, ObjectVersion> for SingleObjectProtocol<T> {
    async fn write(&self, current_id: Option<ObjectVersion>, new_value: &T) -> Result<ObjectVersion, TransactionalObjectError> {
        let mode = match current_id {
            None => PutMode::Create,
            Some(version) => PutMode::Update(version.update()),
        };
        let payload = self.codec.encode(new_value);
        let result = match self.object_store.put_opts(&self.path, payload.into(), mode.into()).await {
            Ok(result) => result,
            Err(StoreError::AlreadyExists { .. }) | Err(StoreError::Precondition { .. }) | Err(StoreError::NotFound { .. }) => {
                return Err(TransactionalObjectError::ObjectVersionExists);
            }
            Err(error) => return Err(error.into()),
        };
        let mut version = ObjectVersion {
            e_tag: result.e_tag,
            version: result.version,
        };
        if version.e_tag.is_none() && version.version.is_none() {
            // A store that reports no version on PUT still reports one on
            // HEAD; without it the next conditional write could not be made.
            let meta = self.object_store.head(&self.path).await?;
            version = ObjectVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            };
        }
        Ok(version)
    }

    async fn try_read_latest(&self) -> Result<Option<(ObjectVersion, T)>, TransactionalObjectError> {
        let result = match self.object_store.get(&self.path).await {
            Ok(result) => result,
            Err(StoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let version = ObjectVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result.bytes().await?;
        let value = self.codec.decode(&bytes).map_err(TransactionalObjectError::CallbackError)?;
        Ok(Some((version, value)))
    }
}

/// What a reader finds at the root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Current {
    /// A record with a publication, and that publication's payload.
    Published { record: ControlRecord, payload: Bytes },
    /// A record with nothing published yet (an owner that has not finished).
    Unpublished(ControlRecord),
    /// `current` is not a control record: the layout before this crate. The
    /// bytes are the caller's to interpret.
    Opaque(Bytes),
}

/// The result of trying to take ownership.
#[derive(Debug)]
pub enum Acquire {
    Acquired(Box<Ownership>),
    /// Another session holds a live lease.
    Held {
        owner: String,
        expires_at_ms: u64,
    },
}

/// What publishing did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Publish {
    Published {
        publication_id: String,
    },
    /// The record already carries this sequence or a newer one; the payload
    /// was discarded.
    Superseded {
        current_sequence: u64,
    },
}

/// One publication's root in an object store, plus the clock leases are
/// judged by.
#[derive(Clone)]
pub struct PublicationStore {
    object_store: Arc<dyn ObjectStore>,
    root: Path,
    clock: Arc<dyn SystemClock>,
    legacy_sequence: Option<LegacySequence>,
}

/// Reads the sequence a legacy object was built from, so a retained one
/// keeps its place in the forward-only order. `None` for an object it
/// cannot read: any publication then replaces it.
pub type LegacySequence = Arc<dyn Fn(&Bytes) -> Option<u64> + Send + Sync>;

impl std::fmt::Debug for PublicationStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationStore").field("root", &self.root).finish()
    }
}

impl PublicationStore {
    pub fn new(object_store: Arc<dyn ObjectStore>, root: Path) -> Self {
        Self {
            object_store,
            root,
            clock: Arc::new(DefaultSystemClock::new()),
            legacy_sequence: None,
        }
    }

    /// Read the sequence of a legacy object with `read` when one is taken
    /// over, so a publication from an older snapshot does not replace it.
    /// Without it a retained legacy object has no sequence, and the first
    /// publication replaces it whatever its snapshot.
    pub fn with_legacy_sequence(mut self, read: LegacySequence) -> Self {
        self.legacy_sequence = Some(read);
        self
    }

    /// Judge leases by `clock` instead of the system clock.
    pub fn with_clock(mut self, clock: Arc<dyn SystemClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn record_path(&self) -> Path {
        Path::from(format!("{}/{RECORD_NAME}", self.root))
    }

    fn payload_path(&self, relative: &str) -> Path {
        Path::from(format!("{}/{relative}", self.root))
    }

    /// Write a payload once; a repeat of the same id is the same bytes.
    async fn upload_payload(&self, relative: &str, payload: Bytes) -> Result<(), PublicationError> {
        let path = self.payload_path(relative);
        match self.object_store.put_opts(&path, payload.into(), PutMode::Create.into()).await {
            Ok(_) | Err(StoreError::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Remove a payload, but only once the record is read and names neither
    /// it nor anything as its previous payload. A record that cannot be
    /// read leaves the payload in place: an orphan costs storage, a missing
    /// publication costs readers their statistics. Best effort throughout.
    async fn delete_payload_if_unreferenced(&self, relative: &str) {
        let referenced = match self.protocol().try_read_latest().await {
            Ok(Some((_, record))) => record.payload.as_deref() == Some(relative) || record.previous_payload.as_deref() == Some(relative),
            Ok(None) => false,
            Err(_) => return,
        };
        if !referenced {
            let _ = self.object_store.delete(&self.payload_path(relative)).await;
        }
    }

    fn now_ms(&self) -> u64 {
        self.clock.now().timestamp_millis().max(0) as u64
    }

    fn protocol(&self) -> Arc<dyn TransactionalStorageProtocol<ControlRecord, ObjectVersion>> {
        Arc::new(SingleObjectProtocol::new(
            Arc::clone(&self.object_store),
            self.record_path(),
            Box::new(RecordCodec),
        ))
    }

    /// What is published, without taking ownership and without any write.
    ///
    /// A payload the record points at can be missing for one reason only: a
    /// reader held a record two publications old while the newest publish
    /// removed that payload. The record is re-read once for that case, so
    /// the caller sees the publication that replaced it.
    pub async fn read_current(&self) -> Result<Option<Current>, PublicationError> {
        for _ in 0..2 {
            let result = match self.object_store.get(&self.record_path()).await {
                Ok(result) => result,
                Err(StoreError::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let bytes = result.bytes().await?;
            let record = RecordCodec
                .decode(&bytes)
                .map_err(|error| PublicationError::InvalidRecord(error.to_string()))?;
            if record.legacy.is_some() {
                return Ok(Some(Current::Opaque(bytes)));
            }
            let Some(payload) = &record.payload else {
                return Ok(Some(Current::Unpublished(record)));
            };
            match self.object_store.get(&self.payload_path(payload)).await {
                Ok(result) => {
                    let payload = result.bytes().await?;
                    return Ok(Some(Current::Published { record, payload }));
                }
                Err(StoreError::NotFound { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(PublicationError::InvalidRecord(
            "published payload is missing after re-reading the record".to_string(),
        ))
    }

    /// Take ownership for `lease`, or learn who holds it.
    ///
    /// The lease check and the write are one conditional update on the
    /// record's version: if another session acquired between the read and
    /// the write, the write fails and the check runs again on what it wrote.
    /// A record whose lease has lapsed is taken over with its publication
    /// fields kept. A `current` that is not a control record (the object
    /// itself, in the layout before this crate) is taken over too, and the
    /// object is retained as the published payload first, so a build that
    /// then fails or is declined leaves readers what they had.
    pub async fn acquire(&self, owner: &str, lease: Duration) -> Result<Acquire, PublicationError> {
        let protocol = self.protocol();
        let lease_ms = lease.as_millis().min(u128::from(u64::MAX)) as u64;
        // The legacy object, once uploaded as a payload: done at most once
        // per acquisition, and removed again if the takeover loses the race.
        let mut retained: Option<(String, Option<u64>)> = None;
        let outcome = self.acquire_inner(&protocol, owner, lease_ms, &mut retained).await;
        if let Some((relative, _)) = retained {
            let referenced =
                matches!(&outcome, Ok(Acquire::Acquired(ownership)) if ownership.record().payload.as_deref() == Some(relative.as_str()));
            if !referenced {
                self.delete_payload_if_unreferenced(&relative).await;
            }
        }
        outcome
    }

    async fn acquire_inner(
        &self,
        protocol: &Arc<dyn TransactionalStorageProtocol<ControlRecord, ObjectVersion>>,
        owner: &str,
        lease_ms: u64,
        retained: &mut Option<(String, Option<u64>)>,
    ) -> Result<Acquire, PublicationError> {
        let protocol = Arc::clone(protocol);
        for attempt in 1..=MAX_ATTEMPTS {
            let now = self.now_ms();
            let Some(mut object) = SimpleTransactionalObject::try_load(Arc::clone(&protocol)).await? else {
                let record = ControlRecord {
                    epoch: 1,
                    owner: Some(owner.to_string()),
                    lease_expires_at_ms: now.saturating_add(lease_ms),
                    ..ControlRecord::default()
                };
                let object = match SimpleTransactionalObject::init(Arc::clone(&protocol), record).await {
                    Ok(object) => object,
                    Err(error) if error.is_sequenced_write_conflict() => continue,
                    Err(TransactionalObjectError::ObjectStoreError(error)) => match self.landed_acquisition(&protocol, owner, 1).await {
                        Some(object) => object,
                        None => return Err(error.into()),
                    },
                    Err(error) => return Err(error.into()),
                };
                return Ok(Acquire::Acquired(Box::new(Ownership {
                    store: self.clone(),
                    object,
                    epoch: 1,
                    owner: owner.to_string(),
                })));
            };
            let record = object.object().clone();
            if let Some((holder, expires_at_ms)) = record.owned_by_other(owner, now) {
                return Ok(Acquire::Held {
                    owner: holder,
                    expires_at_ms,
                });
            }
            let epoch = record.epoch.saturating_add(1);
            let mut value = ControlRecord {
                epoch,
                owner: Some(owner.to_string()),
                lease_expires_at_ms: now.saturating_add(lease_ms),
                legacy: None,
                ..record.clone()
            };
            if let Some(legacy) = &record.legacy {
                let (relative, sequence) = match retained {
                    Some(retained) => retained.clone(),
                    None => {
                        let relative = format!("{PAYLOADS_DIR}/legacy-{}", ulid::Ulid::new());
                        self.upload_payload(&relative, legacy.clone()).await?;
                        let sequence = self.legacy_sequence.as_ref().and_then(|read| read(legacy));
                        retained.insert((relative, sequence)).clone()
                    }
                };
                value.published_sequence = sequence;
                value.publication_id = Some(relative.rsplit('/').next().unwrap_or(&relative).to_string());
                value.payload = Some(relative);
                value.previous_payload = None;
            }
            let dirty = DirtyObject { id: object.id(), value };
            let object = match object.update(dirty).await {
                Ok(()) => object,
                Err(error) if error.is_sequenced_write_conflict() => {
                    if attempt == MAX_ATTEMPTS {
                        break;
                    }
                    continue;
                }
                Err(TransactionalObjectError::ObjectStoreError(error)) => match self.landed_acquisition(&protocol, owner, epoch).await {
                    Some(object) => object,
                    None => return Err(error.into()),
                },
                Err(error) => return Err(error.into()),
            };
            return Ok(Acquire::Acquired(Box::new(Ownership {
                store: self.clone(),
                object,
                epoch,
                owner: owner.to_string(),
            })));
        }
        Err(PublicationError::Conflict { attempts: MAX_ATTEMPTS })
    }

    /// Whether an acquisition write that reported an error landed all the
    /// same: the record, re-read, names `owner` at `epoch`, which no other
    /// write can produce. The error is otherwise the answer, and the record
    /// is left as it is: a lease this session holds without knowing would
    /// block others until it expires.
    async fn landed_acquisition(
        &self,
        protocol: &Arc<dyn TransactionalStorageProtocol<ControlRecord, ObjectVersion>>,
        owner: &str,
        epoch: u64,
    ) -> Option<SimpleTransactionalObject<ControlRecord, ObjectVersion>> {
        match SimpleTransactionalObject::try_load(Arc::clone(protocol)).await {
            Ok(Some(object)) if object.object().owner.as_deref() == Some(owner) && object.object().epoch == epoch => Some(object),
            _ => None,
        }
    }
}

/// A held lease. Every write refreshes the record and refuses to continue
/// if the epoch moved, so a paused owner that was taken over cannot land
/// anything after it resumes.
pub struct Ownership {
    store: PublicationStore,
    object: SimpleTransactionalObject<ControlRecord, ObjectVersion>,
    epoch: u64,
    owner: String,
}

impl std::fmt::Debug for Ownership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ownership")
            .field("root", self.store.root())
            .field("epoch", &self.epoch)
            .field("owner", &self.owner)
            .finish()
    }
}

/// What a conditional update decided after re-reading the record: write
/// this record and answer that once it lands, or answer without writing.
enum Step<R> {
    Write(ControlRecord, R),
    Done(R),
}

impl Ownership {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// The record as last read by this owner.
    pub fn record(&self) -> &ControlRecord {
        self.object.object()
    }

    /// Refresh, check the epoch, let `decide` look at the record, and write
    /// what it returns conditionally; on a version conflict, do it again.
    ///
    /// A write whose response is lost (the store reports an error after the
    /// object landed) is told apart from a write that failed by re-reading
    /// once: `decide` sees the record as it is now, and answers `Done` if
    /// its write is there. The second such error in one update is returned.
    async fn update_with<R>(&mut self, mut decide: impl FnMut(&ControlRecord) -> Step<R>) -> Result<R, PublicationError> {
        let mut recovered = false;
        for attempt in 1..=MAX_ATTEMPTS {
            self.object.refresh().await?;
            if self.object.object().epoch != self.epoch {
                return Err(PublicationError::Fenced);
            }
            let (value, result) = match decide(self.object.object()) {
                Step::Done(result) => return Ok(result),
                Step::Write(value, result) => (value, result),
            };
            let dirty = DirtyObject {
                id: self.object.id(),
                value,
            };
            match self.object.update(dirty).await {
                Ok(()) => return Ok(result),
                Err(error) if error.is_sequenced_write_conflict() => {
                    if attempt == MAX_ATTEMPTS {
                        break;
                    }
                    continue;
                }
                Err(TransactionalObjectError::ObjectStoreError(error)) if !recovered => {
                    recovered = true;
                    if self.object.refresh().await.is_err() {
                        return Err(error.into());
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(PublicationError::Conflict { attempts: MAX_ATTEMPTS })
    }

    /// Extend the lease by `lease` from now.
    pub async fn renew(&mut self, lease: Duration) -> Result<(), PublicationError> {
        let lease_ms = lease.as_millis().min(u128::from(u64::MAX)) as u64;
        let now = self.store.now_ms();
        let owner = self.owner.clone();
        self.update_with(|record| {
            Step::Write(
                ControlRecord {
                    owner: Some(owner.clone()),
                    lease_expires_at_ms: now.saturating_add(lease_ms),
                    ..record.clone()
                },
                (),
            )
        })
        .await
    }

    /// Publish `payload` as built from `sequence`.
    ///
    /// The payload is uploaded first under a fresh id, so a lost response
    /// is retried harmlessly (the id is unique and the content fixed). Then
    /// the record is updated conditionally: re-read on every attempt, it
    /// refuses a sequence at or below the published one and refuses to
    /// continue if the epoch moved. The payload two publications old is
    /// removed on success; the one before this is kept for readers holding
    /// the previous record.
    pub async fn publish(&mut self, sequence: u64, payload: Bytes) -> Result<Publish, PublicationError> {
        self.publish_with(sequence, payload, false).await
    }

    /// [`Ownership::publish`], but a payload at the published sequence
    /// replaces the published one instead of being superseded. For a
    /// publication readers cannot use (one that does not decode), which a
    /// rebuild of the same snapshot could otherwise never repair. A newer
    /// sequence still wins over it.
    pub async fn republish(&mut self, sequence: u64, payload: Bytes) -> Result<Publish, PublicationError> {
        self.publish_with(sequence, payload, true).await
    }

    async fn publish_with(&mut self, sequence: u64, payload: Bytes, replace_same: bool) -> Result<Publish, PublicationError> {
        let publication_id = ulid::Ulid::new().to_string();
        let relative = format!("{PAYLOADS_DIR}/{publication_id}");
        self.store.upload_payload(&relative, payload).await?;
        let mut retired = None;
        let outcome = self
            .update_with(|record| {
                if record.publication_id.as_deref() == Some(publication_id.as_str()) {
                    // Our write landed; the response was lost.
                    return Step::Done(Publish::Published {
                        publication_id: publication_id.clone(),
                    });
                }
                let superseded = |current: &u64| *current > sequence || (*current == sequence && !replace_same);
                if let Some(current) = record.published_sequence.filter(superseded) {
                    return Step::Done(Publish::Superseded { current_sequence: current });
                }
                retired = record.previous_payload.clone();
                Step::Write(
                    ControlRecord {
                        published_sequence: Some(sequence),
                        publication_id: Some(publication_id.clone()),
                        payload: Some(relative.clone()),
                        previous_payload: record.payload.clone(),
                        ..record.clone()
                    },
                    Publish::Published {
                        publication_id: publication_id.clone(),
                    },
                )
            })
            .await;
        match &outcome {
            Ok(Publish::Published { .. }) => {
                if let Some(retired) = retired {
                    self.store.delete_payload_if_unreferenced(&retired).await;
                }
            }
            // Superseded: the record was read and does not name this
            // payload. An error: the write's outcome may be unknown (it
            // landed, the response was lost, and the recovery read failed
            // too), so the payload goes only once the record says it is
            // not the publication.
            Ok(Publish::Superseded { .. }) | Err(_) => self.store.delete_payload_if_unreferenced(&relative).await,
        }
        outcome
    }

    /// Give the lease up, keeping the publication. A lease already taken
    /// over has nothing to release.
    pub async fn release(mut self) -> Result<(), PublicationError> {
        let owner = self.owner.clone();
        match self
            .update_with(|record| {
                if record.owner.as_deref() != Some(owner.as_str()) {
                    return Step::Done(());
                }
                Step::Write(
                    ControlRecord {
                        owner: None,
                        lease_expires_at_ms: 0,
                        ..record.clone()
                    },
                    (),
                )
            })
            .await
        {
            Ok(()) | Err(PublicationError::Fenced) => Ok(()),
            Err(error) => Err(error),
        }
    }
}
