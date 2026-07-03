pub mod config;
pub mod factory;
pub mod in_memory;
pub mod loader;
pub mod metrics_recorder;
pub mod slate;
pub mod sst_blocks;
pub mod util;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::FilterContext;
use uuid::Uuid;

use crate::BytesRange;
use crate::storage::slate::SlateReadHandle;

/// Identifies a checkpoint of a storage backend at a point in time.
///
/// Checkpoints capture a manifest snapshot that a reader can later open
/// against to get a consistent view of the database at the time the
/// checkpoint was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub id: Uuid,
    pub manifest_id: u64,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub enum Ttl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfter(u64),
    ExpireAt(i64),
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct PutOptions {
    pub ttl: Ttl,
}

/// Encapsulates a record being put along with options specific to the put.
#[derive(Clone, Debug)]
pub struct PutRecordOp {
    pub record: Record,
    pub options: PutOptions,
}

impl PutRecordOp {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            options: PutOptions::default(),
        }
    }

    pub fn new_with_options(record: Record, options: PutOptions) -> Self {
        Self { record, options }
    }

    pub fn with_options(self, options: PutOptions) -> Self {
        Self {
            record: self.record,
            options,
        }
    }
}

/// Converts a Record to a PutRecordOp with default options
impl From<Record> for PutRecordOp {
    fn from(record: Record) -> Self {
        Self::new(record)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeOptions {
    pub ttl: Ttl,
}

/// Encapsulates a record written as part of a merge op along with options specific to the merge.
#[derive(Clone, Debug)]
pub struct MergeRecordOp {
    pub record: Record,
    pub options: MergeOptions,
}

impl MergeRecordOp {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            options: MergeOptions::default(),
        }
    }

    pub fn new_with_ttl(record: Record, options: MergeOptions) -> Self {
        Self { record, options }
    }
}

/// Converts a Record to a PutRecordOp with default options
impl From<Record> for MergeRecordOp {
    fn from(record: Record) -> Self {
        Self::new(record)
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub key: Bytes,
    pub value: Bytes,
}

impl Record {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    pub fn empty(key: Bytes) -> Self {
        Self::new(key, Bytes::new())
    }
}

#[derive(Clone, Debug)]
pub enum RecordOp {
    Put(PutRecordOp),
    Merge(MergeRecordOp),
    Delete(Bytes),
}

/// Options for write operations.
///
/// Controls the durability behavior of write operations like [`Storage::put`]
/// and [`Storage::put_with_options`].
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Whether to wait for the write to be durable before returning.
    ///
    /// When `true`, the operation will not return until the data has been
    /// persisted to durable storage (e.g., flushed to the WAL and acknowledged
    /// by the object store).
    ///
    /// When `false` (the default), the operation returns as soon as the data
    /// is in memory, providing lower latency but risking data loss on crash.
    pub await_durable: bool,
}

/// Error type for storage operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Storage-related errors
    Storage(String),
    /// Internal errors
    Internal(String),
}

impl std::error::Error for StorageError {}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StorageError::Storage(msg) => write!(f, "Storage error: {}", msg),
            StorageError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl StorageError {
    /// Converts a storage error to StorageError::Storage.
    pub fn from_storage(e: impl std::fmt::Display) -> Self {
        StorageError::Storage(e.to_string())
    }
}

/// Result type alias for storage operations
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Result of a write operation, containing the sequence number assigned by the storage engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteResult {
    /// The sequence number assigned to this write by the underlying storage engine.
    pub seqnum: u64,
}

/// Trait for merging existing values with new values.
///
/// Merge operators must be associative: `merge_batch(merge_batch(a, [b]), [c]) == merge_batch(a, merge_batch(b, [c]))`.
/// This ensures consistent merging behavior regardless of the order of operations.
pub trait MergeOperator: Send + Sync {
    /// Merges a batch of operands with an optional existing value.
    ///
    /// # Arguments
    /// * `key` - The key associated with the values being merged
    /// * `existing_value` - The current value stored in the database (if any)
    /// * `operands` - A slice of operands to merge, ordered from oldest to newest
    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes;
}

pub fn default_merge_batch(
    key: &Bytes,
    existing_value: Option<Bytes>,
    operands: &[Bytes],
    merge: impl Fn(&Bytes, Option<Bytes>, Bytes) -> Bytes,
) -> Bytes {
    let mut result = existing_value;
    for operand in operands {
        result = Some(merge(key, result, operand.clone()));
    }
    result.expect("merge_batch called with no existing value and no operands")
}

/// Iterator over storage records.
#[async_trait]
pub trait StorageIterator {
    /// Returns the next record from this iterator.
    ///
    /// Returns `Ok(None)` when the iterator is exhausted.
    async fn next(&mut self) -> StorageResult<Option<Record>>;
}

/// Common read operations supported by both Storage and StorageSnapshot.
///
/// This trait provides the core read methods that are shared between full storage
/// access and point-in-time snapshots. By extracting these common operations,
/// we can write code that works with both storage types.
#[async_trait]
pub trait StorageRead: Any + Send + Sync {
    /// Retrieves a single record by exact key.
    ///
    /// Returns `Ok(None)` if the key is not present.
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>>;

    /// Returns an iterator over records in the given range.
    ///
    /// The returned iterator is owned and does not borrow from the storage,
    /// allowing it to be stored in structs or passed across await points.
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>>;

    /// Returns an iterator over records whose key starts with `prefix`,
    /// restricted to `subrange`.
    ///
    /// `subrange` bounds are key *suffixes* interpreted relative to `prefix`: a
    /// bound `s` selects the full key `prefix ++ s`. Pass
    /// [`BytesRange::unbounded`] to scan the prefix's entire keyspace. Callers
    /// with a known sub-key ordering (e.g. the log's trailing sequence bytes)
    /// use this to bound the scan natively instead of filtering client-side.
    ///
    /// Backends that support prefix-aware SST filters (e.g. SlateDB with a
    /// configured `PrefixExtractor` or custom `FilterPolicy`) consult those
    /// filters here, allowing SSTs that contain no matching keys — or, with
    /// `filter_context`, no keys past a caller-supplied cursor — to be skipped
    /// without a block read. Backends without such filters fall back to a range
    /// scan over the prefix.
    ///
    /// `filter_context` is an opaque payload forwarded to the backend's custom
    /// filter policies (ignored by built-in filters and by backends without
    /// filter support); a scan parametrizes its filters through it. See
    /// [`slatedb::FilterContext`].
    ///
    /// The default implementation delegates to [`Self::scan_iter`] with the
    /// absolute range derived from `prefix` and `subrange`, which preserves
    /// correctness for any backend; backends that can do better should override
    /// this.
    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        _filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        self.scan_iter(BytesRange::from_prefix_and_subrange(&prefix, &subrange))
            .await
    }

    /// Collects all records in the range into a Vec.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan(&self, range: BytesRange) -> StorageResult<Vec<Record>> {
        let mut iter = self.scan_iter(range).await?;
        let mut records = Vec::new();
        while let Some(record) = iter.next().await? {
            records.push(record);
        }
        Ok(records)
    }

    /// Returns a handle to this backend's slatedb resources (a manifest
    /// snapshot + an `SstReader`) for the SST-walk count path, or `None` when
    /// the backend isn't slatedb or has no SST reader attached.
    ///
    /// This is a deliberate, single-method leak of slatedb internals rather
    /// than a storage-neutral abstraction: counting via the manifest needs
    /// concrete slatedb types, and callers that don't get a handle fall back
    /// to a plain scan. See [`SlateReadHandle`].
    fn slate_read(&self) -> Option<SlateReadHandle> {
        None
    }

    /// Closes the storage, releasing any resources.
    ///
    /// This method should be called before dropping the storage to ensure
    /// proper cleanup. For SlateDB, this releases the database fence.
    ///
    /// # Errors
    ///
    /// Returns an error if resource shutdown fails.
    async fn close(&self) -> StorageResult<()> {
        Ok(())
    }
}

/// A point-in-time snapshot of the storage layer.
///
/// Snapshots provide a consistent read-only view of the database at the time
/// the snapshot was created. Reads from a snapshot will not see any subsequent
/// writes to the underlying storage.
#[async_trait]
pub trait StorageSnapshot: StorageRead {}

/// The storage type encapsulates access to the underlying storage (e.g. SlateDB).
#[async_trait]
pub trait Storage: StorageRead {
    /// Applies a batch of mixed operations (puts, merges, and deletes) atomically.
    ///
    /// All operations in the batch are written together in a single `WriteBatch`,
    /// so either all succeed or none are visible. This is the primary write method
    /// used by flushers that produce a mix of operation types from a frozen delta.
    ///
    /// Uses `WriteOptions::default()` (`await_durable: false`).
    async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult> {
        self.apply_with_options(ops, WriteOptions::default()).await
    }

    /// Applies a batch of mixed operations with custom write options.
    ///
    /// `options.await_durable` controls whether this call returns only after
    /// the batch reaches durable storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be applied.
    async fn apply_with_options(
        &self,
        ops: Vec<RecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult>;

    /// Writes records to storage.
    ///
    /// Uses `WriteOptions::default()` (`await_durable: false`).
    async fn put(&self, records: Vec<PutRecordOp>) -> StorageResult<WriteResult> {
        self.put_with_options(records, WriteOptions::default())
            .await
    }

    /// Writes records to storage with custom options.
    ///
    /// This method allows control over durability behavior. Use this when you
    /// need to specify whether to wait for writes to be durable.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to write
    /// * `options` - Write options controlling durability behavior
    ///
    /// # Errors
    ///
    /// Returns an error if any write in the batch fails.
    async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult>;

    /// Merges values for the given keys using the configured merge operator.
    ///
    /// This method requires the underlying storage engine to be configured with
    /// a merge operator. If no merge operator is configured, this method will
    /// return a `StorageError::Storage` error.
    ///
    /// The merge operation is atomic - all merges in the batch are applied
    /// together or not at all.
    ///
    /// Uses `WriteOptions::default()` (`await_durable: false`).
    async fn merge(&self, records: Vec<MergeRecordOp>) -> StorageResult<WriteResult> {
        self.merge_with_options(records, WriteOptions::default())
            .await
    }

    /// Merges values with custom write options.
    ///
    /// `options.await_durable` controls whether this call returns only after
    /// the batch reaches durable storage.
    ///
    /// # Errors
    ///
    /// Returns an error if no merge operator is configured or if the merge
    /// operation fails.
    async fn merge_with_options(
        &self,
        records: Vec<MergeRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult>;

    /// Creates a point-in-time snapshot of the storage.
    ///
    /// The snapshot provides a consistent read-only view of the database at the time
    /// the snapshot was created. Reads from the snapshot will not see any subsequent
    /// writes to the underlying storage.
    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>>;

    /// Flushes all pending writes to durable storage.
    ///
    /// This ensures that all writes that have been acknowledged are persisted
    /// to durable storage. For SlateDB, this flushes the memtable to the WAL
    /// and object store.
    ///
    /// # Errors
    ///
    /// Returns an error if durability cannot be established.
    async fn flush(&self) -> StorageResult<()>;

    /// Subscribes to the durable sequence watermark.
    ///
    /// Returns a `watch::Receiver<u64>` that tracks the highest sequence number
    /// confirmed durable by the storage engine. For SlateDB, this maps to
    /// `DbStatus::durable_seq`. For in-memory storage, writes are immediately
    /// "durable" so the watermark matches the latest written seqnum.
    fn subscribe_durable(&self) -> tokio::sync::watch::Receiver<u64>;

    /// Creates a durable checkpoint of the current state.
    ///
    /// The returned [`CheckpointInfo::id`] can later be passed to a
    /// `StorageReaderRuntime` to open a reader pinned to this exact view of
    /// the database. For SlateDB, this maps to `Db::create_checkpoint` with
    /// `CheckpointScope::All` (flushes WALs and freezes the memtable so the
    /// checkpoint includes recent writes).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying backend does not support
    /// checkpoints, or if the checkpoint cannot be persisted.
    async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo>;
}
