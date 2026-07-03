use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;

use super::{
    MergeOperator, MergeRecordOp, PutRecordOp, Storage, StorageSnapshot, WriteOptions, WriteResult,
};
use crate::storage::RecordOp;
use crate::{
    BytesRange, CheckpointInfo, Record, StorageError, StorageIterator, StorageRead, StorageResult,
    Ttl,
};

/// Trait for providing the current time.
pub trait Clock: Send + Sync {
    /// Returns the current time as milliseconds since the Unix epoch.
    fn now(&self) -> i64;
}

/// Clock implementation that returns the real system time.
pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_millis() as i64
    }
}

/// Internal wrapper that stores a value alongside its expiration timestamp.
#[derive(Clone, Debug)]
struct StoredValue {
    value: Bytes,
    /// `None` means the value never expires.
    expire_ts: Option<i64>,
}

impl StoredValue {
    fn is_expired(&self, now: i64) -> bool {
        self.expire_ts.is_some_and(|ts| now >= ts)
    }
}

/// Computes the absolute expiration timestamp from TTL options.
fn compute_expire_ts(now: i64, ttl: Ttl, default_ttl: Option<u64>) -> Option<i64> {
    match ttl {
        Ttl::Default => default_ttl.map(|ms| now + ms as i64),
        Ttl::NoExpiry => None,
        Ttl::ExpireAfter(ms) => Some(now + ms as i64),
        Ttl::ExpireAt(ts) => Some(ts),
    }
}

/// In-memory implementation of the Storage trait using a BTreeMap.
///
/// This implementation stores all data in memory and is useful for testing
/// or scenarios where durability is not required. Supports TTL-based
/// expiration via a configurable [`Clock`].
pub struct InMemoryStorage {
    data: Arc<RwLock<BTreeMap<Bytes, StoredValue>>>,
    merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>,
    clock: Arc<dyn Clock>,
    default_ttl: Option<u64>,
    written_seq: std::sync::atomic::AtomicU64,
    durable_seq: std::sync::atomic::AtomicU64,
    durable_tx: tokio::sync::watch::Sender<u64>,
    defer_durability: bool,
}

impl InMemoryStorage {
    /// Creates a new InMemoryStorage instance with an empty store.
    pub fn new() -> Self {
        let (durable_tx, _) = tokio::sync::watch::channel(0);
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            merge_operator: None,
            clock: Arc::new(WallClock),
            default_ttl: None,
            written_seq: std::sync::atomic::AtomicU64::new(0),
            durable_seq: std::sync::atomic::AtomicU64::new(0),
            durable_tx,
            defer_durability: false,
        }
    }

    /// Creates a new InMemoryStorage instance with an optional merge operator.
    ///
    /// If a merge operator is provided, the `merge` method will use it to combine
    /// existing values with new values. If no merge operator is provided, the
    /// `merge` method will return an error.
    pub fn with_merge_operator(merge_operator: Arc<dyn MergeOperator + Send + Sync>) -> Self {
        let (durable_tx, _) = tokio::sync::watch::channel(0);
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            merge_operator: Some(merge_operator),
            clock: Arc::new(WallClock),
            default_ttl: None,
            written_seq: std::sync::atomic::AtomicU64::new(0),
            durable_seq: std::sync::atomic::AtomicU64::new(0),
            durable_tx,
            defer_durability: false,
        }
    }

    /// Sets a custom clock for TTL expiration checks.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the default TTL (in milliseconds) for records written with [`Ttl::Default`].
    pub fn with_default_ttl(mut self, ttl: u64) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Enables deferred durability mode.
    ///
    /// When enabled, writes are not marked as durable until [`flush()`](Storage::flush)
    /// or [`flush_to()`](Self::flush_to) is called. This is useful for testing
    /// `read_durable` behavior without depending on SlateDB internals.
    pub fn with_deferred_durability(mut self) -> Self {
        self.defer_durability = true;
        self
    }

    /// Advances the durable watermark to a specific sequence number.
    ///
    /// # Panics
    ///
    /// Panics if `seq` is greater than the current written sequence number
    /// or less than the current durable sequence number.
    pub fn flush_to(&self, seq: u64) {
        let written = self.written_seq.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            seq <= written,
            "cannot flush beyond written seqnum: flush_to({seq}) but written is {written}"
        );
        let durable = self.durable_seq.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            seq >= durable,
            "cannot move durable seqnum backwards: flush_to({seq}) but durable is {durable}"
        );
        self.durable_seq
            .store(seq, std::sync::atomic::Ordering::Relaxed);
        let _ = self.durable_tx.send(seq);
    }

    /// Allocates the next sequence number.
    ///
    /// When `defer_durability` is false (default), the sequence number is
    /// immediately marked as durable. When true, durability is deferred
    /// until an explicit flush.
    fn next_seqnum(&self) -> u64 {
        let seq = self
            .written_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if !self.defer_durability {
            self.durable_seq
                .store(seq, std::sync::atomic::Ordering::Relaxed);
            let _ = self.durable_tx.send(seq);
        }
        seq
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageRead for InMemoryStorage {
    /// Retrieves a single record by key from the in-memory store.
    ///
    /// Returns `None` if the key does not exist or has expired.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        match data.get(&key) {
            Some(stored) if !stored.is_expired(self.clock.now()) => {
                Ok(Some(Record::new(key, stored.value.clone())))
            }
            _ => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        let now = self.clock.now();
        let records: Vec<Record> = data
            .range((range.start_bound().cloned(), range.end_bound().cloned()))
            .filter(|(_, stored)| !stored.is_expired(now))
            .map(|(k, stored)| Record::new(k.clone(), stored.value.clone()))
            .collect();

        Ok(Box::new(InMemoryIterator { records, index: 0 }))
    }
}

struct InMemoryIterator {
    records: Vec<Record>,
    index: usize,
}

#[async_trait]
impl StorageIterator for InMemoryIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        if self.index >= self.records.len() {
            Ok(None)
        } else {
            let record = self.records[self.index].clone();
            self.index += 1;
            Ok(Some(record))
        }
    }
}

/// In-memory snapshot that holds a copy of the data at the time of snapshot creation.
///
/// Provides a consistent read-only view of the database at the time the snapshot was created.
/// Expired entries are filtered out at read time using the shared clock.
pub struct InMemoryStorageSnapshot {
    data: Arc<BTreeMap<Bytes, StoredValue>>,
    clock: Arc<dyn Clock>,
}

#[async_trait]
impl StorageRead for InMemoryStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        match self.data.get(&key) {
            Some(stored) if !stored.is_expired(self.clock.now()) => {
                Ok(Some(Record::new(key, stored.value.clone())))
            }
            _ => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let now = self.clock.now();
        let records: Vec<Record> = self
            .data
            .range((range.start_bound().cloned(), range.end_bound().cloned()))
            .filter(|(_, stored)| !stored.is_expired(now))
            .map(|(k, stored)| Record::new(k.clone(), stored.value.clone()))
            .collect();

        Ok(Box::new(InMemoryIterator { records, index: 0 }))
    }
}

#[async_trait]
impl StorageSnapshot for InMemoryStorageSnapshot {}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn apply_with_options(
        &self,
        records: Vec<RecordOp>,
        _options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut data = self
            .data
            .write()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire write lock: {}", e)))?;

        let now = self.clock.now();
        for record in records {
            match record {
                RecordOp::Put(op) => {
                    let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
                    data.insert(
                        op.record.key,
                        StoredValue {
                            value: op.record.value,
                            expire_ts,
                        },
                    );
                }
                RecordOp::Merge(op) => {
                    let existing_value = data
                        .get(&op.record.key)
                        .filter(|s| !s.is_expired(now))
                        .map(|s| s.value.clone());
                    let merged_value = self.merge_operator.as_ref().unwrap().merge_batch(
                        &op.record.key,
                        existing_value,
                        &[op.record.value],
                    );
                    let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
                    data.insert(
                        op.record.key,
                        StoredValue {
                            value: merged_value,
                            expire_ts,
                        },
                    );
                }
                RecordOp::Delete(key) => {
                    data.remove(&key);
                }
            }
        }

        Ok(WriteResult {
            seqnum: self.next_seqnum(),
        })
    }

    /// Writes a batch of records to the in-memory store.
    ///
    /// All records are written atomically within a single write lock acquisition.
    /// For in-memory storage, write options are ignored since there is no
    /// durable storage to await.
    async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        _options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut data = self
            .data
            .write()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire write lock: {}", e)))?;

        let now = self.clock.now();
        for op in records {
            let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
            data.insert(
                op.record.key,
                StoredValue {
                    value: op.record.value,
                    expire_ts,
                },
            );
        }

        Ok(WriteResult {
            seqnum: self.next_seqnum(),
        })
    }

    /// Merges values for the given keys using the configured merge operator.
    ///
    /// This method requires a merge operator to be configured during construction.
    /// For each record, it will:
    /// 1. Get the existing value (if any), excluding expired entries
    /// 2. Call the merge operator to combine existing and new values
    /// 3. Put the merged result back with the new TTL
    ///
    /// If no merge operator is configured, this method will return a
    /// `StorageError::Storage` error.
    async fn merge_with_options(
        &self,
        records: Vec<MergeRecordOp>,
        _options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let merge_op = self
            .merge_operator
            .as_ref()
            .ok_or_else(|| {
                StorageError::Storage(
                    "Merge operator not configured: in-memory storage requires a merge operator to be set during construction".to_string(),
                )
            })?;

        let mut data = self
            .data
            .write()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire write lock: {}", e)))?;

        let now = self.clock.now();
        for op in records {
            let existing_value = data
                .get(&op.record.key)
                .filter(|s| !s.is_expired(now))
                .map(|s| s.value.clone());
            let merged_value =
                merge_op.merge_batch(&op.record.key, existing_value, &[op.record.value]);
            let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
            data.insert(
                op.record.key,
                StoredValue {
                    value: merged_value,
                    expire_ts,
                },
            );
        }

        Ok(WriteResult {
            seqnum: self.next_seqnum(),
        })
    }

    /// Creates a point-in-time snapshot of the in-memory storage.
    ///
    /// The snapshot provides a consistent read-only view of the database at the time
    /// the snapshot was created. Reads from the snapshot will not see any subsequent
    /// writes to the underlying storage. Expired entries are filtered at read time.
    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        let snapshot_data = Arc::new(data.clone());

        Ok(Arc::new(InMemoryStorageSnapshot {
            data: snapshot_data,
            clock: self.clock.clone(),
        }))
    }

    fn subscribe_durable(&self) -> tokio::sync::watch::Receiver<u64> {
        self.durable_tx.subscribe()
    }

    async fn flush(&self) -> StorageResult<()> {
        if self.defer_durability {
            self.flush_to(self.written_seq.load(std::sync::atomic::Ordering::Relaxed));
        }
        Ok(())
    }

    async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
        Err(StorageError::Storage(
            "checkpoints are not supported for in-memory storage".to_string(),
        ))
    }
}

/// Injected failure that fires either once or on every call.
#[cfg(feature = "test-utils")]
#[derive(Clone)]
enum Failure {
    /// Error is returned once, then automatically cleared.
    Once(super::StorageError),
    /// Error is returned on every subsequent call until explicitly cleared.
    Persistent(super::StorageError),
}

#[cfg(feature = "test-utils")]
type FailSlot = arc_swap::ArcSwap<Option<Failure>>;

/// Checks a [`FailSlot`] and returns an error if one is set.
///
/// For [`Failure::Once`], the slot is atomically swapped to `None` so the
/// error fires exactly once. For [`Failure::Persistent`], the slot is left
/// unchanged.
#[cfg(feature = "test-utils")]
fn check_failure(slot: &FailSlot) -> super::StorageResult<()> {
    let guard = slot.load();
    match guard.as_ref() {
        None => Ok(()),
        Some(Failure::Persistent(err)) => Err(err.clone()),
        Some(Failure::Once(_)) => {
            // Swap to None; if another thread raced us, one of them gets the
            // error and the others pass through — reasonable for tests.
            let prev = slot.swap(Arc::new(None));
            match prev.as_ref() {
                Some(Failure::Once(err)) => Err(err.clone()),
                _ => Ok(()),
            }
        }
    }
}

/// A storage wrapper that delegates to an inner [`Storage`] but can inject
/// failures into `apply`, `put`/`put_with_options`, `flush`, and `snapshot` on demand.
///
/// Each failure slot is controlled by a lock-free [`ArcSwap`](arc_swap::ArcSwap).
/// The read path avoids introducing artificial synchronisation that could mask
/// concurrency bugs in the code under test.
///
/// Failures can be *persistent* (returned on every call until cleared) or
/// *once* (returned on the next call, then automatically cleared).
///
/// Gated behind the `test-utils` feature.
///
/// # Example
///
/// ```ignore
/// let inner = Arc::new(InMemoryStorage::new());
/// let storage = FailingStorage::wrap(inner);
/// storage.fail_put(StorageError::Storage("disk full".into()));
/// // every put_with_options call now returns Err(...)
///
/// storage.fail_flush_once(StorageError::Storage("io error".into()));
/// // only the next flush call returns Err(...), then auto-clears
/// ```
#[cfg(feature = "test-utils")]
pub struct FailingStorage {
    inner: Arc<dyn super::Storage>,
    fail_apply: FailSlot,
    fail_put: FailSlot,
    fail_flush: FailSlot,
    fail_snapshot: FailSlot,
}

#[cfg(feature = "test-utils")]
impl FailingStorage {
    /// Wraps an existing storage, with all failure injections initially `None`.
    pub fn wrap(inner: Arc<dyn super::Storage>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fail_apply: arc_swap::ArcSwap::from_pointee(None),
            fail_put: arc_swap::ArcSwap::from_pointee(None),
            fail_flush: arc_swap::ArcSwap::from_pointee(None),
            fail_snapshot: arc_swap::ArcSwap::from_pointee(None),
        })
    }

    /// Makes `apply` return the given error on every subsequent call.
    pub fn fail_apply(&self, err: super::StorageError) {
        self.fail_apply
            .store(Arc::new(Some(Failure::Persistent(err))));
    }

    /// Makes `apply` return the given error on the next call only.
    pub fn fail_apply_once(&self, err: super::StorageError) {
        self.fail_apply.store(Arc::new(Some(Failure::Once(err))));
    }

    /// Makes `put` and `put_with_options` return the given error on every subsequent call.
    pub fn fail_put(&self, err: super::StorageError) {
        self.fail_put
            .store(Arc::new(Some(Failure::Persistent(err))));
    }

    /// Makes `put` or `put_with_options` return the given error on the next call only.
    ///
    /// The failure fires on whichever of the two methods is called first, then
    /// automatically clears.
    pub fn fail_put_once(&self, err: super::StorageError) {
        self.fail_put.store(Arc::new(Some(Failure::Once(err))));
    }

    /// Makes `flush` return the given error on every subsequent call.
    pub fn fail_flush(&self, err: super::StorageError) {
        self.fail_flush
            .store(Arc::new(Some(Failure::Persistent(err))));
    }

    /// Makes `flush` return the given error on the next call only.
    pub fn fail_flush_once(&self, err: super::StorageError) {
        self.fail_flush.store(Arc::new(Some(Failure::Once(err))));
    }

    /// Makes `snapshot` return the given error on every subsequent call.
    pub fn fail_snapshot(&self, err: super::StorageError) {
        self.fail_snapshot
            .store(Arc::new(Some(Failure::Persistent(err))));
    }

    /// Makes `snapshot` return the given error on the next call only.
    pub fn fail_snapshot_once(&self, err: super::StorageError) {
        self.fail_snapshot.store(Arc::new(Some(Failure::Once(err))));
    }
}

#[cfg(feature = "test-utils")]
#[async_trait]
impl super::StorageRead for FailingStorage {
    async fn get(&self, key: Bytes) -> super::StorageResult<Option<crate::Record>> {
        self.inner.get(key).await
    }

    async fn scan_iter(
        &self,
        range: crate::BytesRange,
    ) -> super::StorageResult<Box<dyn super::StorageIterator + Send + 'static>> {
        self.inner.scan_iter(range).await
    }
}

#[cfg(feature = "test-utils")]
#[async_trait]
impl super::Storage for FailingStorage {
    async fn apply_with_options(
        &self,
        ops: Vec<super::RecordOp>,
        options: super::WriteOptions,
    ) -> super::StorageResult<super::WriteResult> {
        check_failure(&self.fail_apply)?;
        self.inner.apply_with_options(ops, options).await
    }

    async fn put_with_options(
        &self,
        records: Vec<super::PutRecordOp>,
        options: super::WriteOptions,
    ) -> super::StorageResult<super::WriteResult> {
        check_failure(&self.fail_put)?;
        self.inner.put_with_options(records, options).await
    }

    async fn merge_with_options(
        &self,
        records: Vec<super::MergeRecordOp>,
        options: super::WriteOptions,
    ) -> super::StorageResult<super::WriteResult> {
        self.inner.merge_with_options(records, options).await
    }

    async fn snapshot(&self) -> super::StorageResult<Arc<dyn super::StorageSnapshot>> {
        check_failure(&self.fail_snapshot)?;
        self.inner.snapshot().await
    }

    fn subscribe_durable(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.subscribe_durable()
    }

    async fn flush(&self) -> super::StorageResult<()> {
        check_failure(&self.fail_flush)?;
        self.inner.flush().await
    }

    async fn create_checkpoint(&self) -> super::StorageResult<CheckpointInfo> {
        self.inner.create_checkpoint().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::ops::Bound;

    /// Test merge operator that appends new value to existing value with a separator.
    struct AppendMergeOperator;

    impl MergeOperator for AppendMergeOperator {
        fn merge_batch(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operands: &[Bytes],
        ) -> Bytes {
            operands
                .iter()
                .fold(existing_value.unwrap_or_default(), |acc, operand| {
                    let mut result = BytesMut::from(acc);
                    if !result.is_empty() {
                        result.extend_from_slice(b",");
                    }
                    result.extend_from_slice(operand);
                    result.freeze()
                })
        }
    }

    #[tokio::test]
    async fn should_return_none_when_key_not_found() {
        // given
        let storage = InMemoryStorage::new();

        // when
        let result = storage.get(Bytes::from("missing_key")).await;

        // then
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_store_and_retrieve_record() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        // when
        storage
            .put(vec![Record::new(key.clone(), value.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.key, Bytes::from("test_key"));
        assert_eq!(record.value, value);
    }

    #[tokio::test]
    async fn should_overwrite_existing_key() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("test_key");
        let initial_value = Bytes::from("initial_value");
        let updated_value = Bytes::from("updated_value");

        // when
        storage
            .put(vec![Record::new(key.clone(), initial_value).into()])
            .await
            .unwrap();
        storage
            .put(vec![Record::new(key.clone(), updated_value.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, updated_value);
    }

    #[tokio::test]
    async fn should_store_multiple_records() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")),
            Record::new(Bytes::from("key2"), Bytes::from("value2")),
            Record::new(Bytes::from("key3"), Bytes::from("value3")),
        ];

        // when
        storage
            .put(records.iter().cloned().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // then
        for record in records {
            let retrieved = storage.get(record.key.clone()).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().value, record.value);
        }
    }

    #[tokio::test]
    async fn should_scan_all_records_when_unbounded() {
        // given
        let storage = InMemoryStorage::new();
        let records = [
            Record::new(Bytes::from("a"), Bytes::from("value_a")),
            Record::new(Bytes::from("b"), Bytes::from("value_b")),
            Record::new(Bytes::from("c"), Bytes::from("value_c")),
        ];
        storage
            .put(records.iter().cloned().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // when
        let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

        // then
        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[0].key, Bytes::from("a"));
        assert_eq!(scanned[1].key, Bytes::from("b"));
        assert_eq!(scanned[2].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_scan_records_with_prefix() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("prefix_a"), Bytes::from("value1")),
            Record::new(Bytes::from("prefix_b"), Bytes::from("value2")),
            Record::new(Bytes::from("other_c"), Bytes::from("value3")),
        ];
        storage
            .put(records.into_iter().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // when
        let scanned = storage
            .scan(BytesRange::prefix(Bytes::from("prefix_")))
            .await
            .unwrap();

        // then
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].key, Bytes::from("prefix_a"));
        assert_eq!(scanned[1].key, Bytes::from("prefix_b"));
    }

    #[tokio::test]
    async fn should_scan_records_in_bounded_range() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("a"), Bytes::from("value_a")),
            Record::new(Bytes::from("b"), Bytes::from("value_b")),
            Record::new(Bytes::from("c"), Bytes::from("value_c")),
            Record::new(Bytes::from("d"), Bytes::from("value_d")),
        ];
        storage
            .put(records.into_iter().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // when
        let range = BytesRange::new(
            Bound::Included(Bytes::from("b")),
            Bound::Excluded(Bytes::from("d")),
        );
        let scanned = storage.scan(range).await.unwrap();

        // then
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].key, Bytes::from("b"));
        assert_eq!(scanned[1].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_return_empty_vec_when_scanning_empty_storage() {
        // given
        let storage = InMemoryStorage::new();

        // when
        let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

        // then
        assert!(scanned.is_empty());
    }

    #[tokio::test]
    async fn should_iterate_over_records() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ];
        storage.put(records).await.unwrap();

        // when
        let mut iter = storage.scan_iter(BytesRange::unbounded()).await.unwrap();
        let first = iter.next().await.unwrap();
        let second = iter.next().await.unwrap();
        let third = iter.next().await.unwrap();

        // then
        assert!(first.is_some());
        assert_eq!(first.unwrap().key, Bytes::from("key1"));
        assert!(second.is_some());
        assert_eq!(second.unwrap().key, Bytes::from("key2"));
        assert!(third.is_none());
    }

    #[tokio::test]
    async fn should_create_snapshot_with_current_data() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            ])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();

        // then
        let result = snapshot.get(Bytes::from("key1")).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("value1"));
    }

    #[tokio::test]
    async fn should_not_see_writes_after_snapshot() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            ])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();
        storage
            .put(vec![
                Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
            ])
            .await
            .unwrap();

        // then
        let snapshot_result = snapshot.get(Bytes::from("key2")).await.unwrap();
        assert!(snapshot_result.is_none());

        let storage_result = storage.get(Bytes::from("key2")).await.unwrap();
        assert!(storage_result.is_some());
    }

    #[tokio::test]
    async fn should_scan_snapshot_independently() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![
                Record::new(Bytes::from("a"), Bytes::from("value_a")).into(),
            ])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();
        storage
            .put(vec![
                Record::new(Bytes::from("b"), Bytes::from("value_b")).into(),
            ])
            .await
            .unwrap();

        // then
        let snapshot_records = snapshot.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(snapshot_records.len(), 1);
        assert_eq!(snapshot_records[0].key, Bytes::from("a"));

        let storage_records = storage.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(storage_records.len(), 2);
    }

    #[tokio::test]
    async fn should_handle_empty_record() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("empty_key");

        // when
        storage
            .put(vec![Record::empty(key.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::new());
    }

    #[tokio::test]
    async fn should_return_error_when_merge_operator_not_configured() {
        // given
        let storage = InMemoryStorage::new();
        let record = Record::new(Bytes::from("key1"), Bytes::from("value1"));

        // when
        let result = storage.merge(vec![record.into()]).await;

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Merge operator not configured")
        );
    }

    #[tokio::test]
    async fn should_merge_when_key_does_not_exist() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let key = Bytes::from("new_key");
        let value = Bytes::from("value1");

        // when
        storage
            .merge(vec![Record::new(key.clone(), value.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, value);
    }

    #[tokio::test]
    async fn should_merge_when_key_exists() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let key = Bytes::from("key1");
        let initial_value = Bytes::from("value1");
        let new_value = Bytes::from("value2");

        storage
            .put(vec![Record::new(key.clone(), initial_value).into()])
            .await
            .unwrap();

        // when
        storage
            .merge(vec![Record::new(key.clone(), new_value).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("value1,value2"));
    }

    #[tokio::test]
    async fn should_merge_multiple_keys() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ];
        storage.put(records).await.unwrap();

        // when
        storage
            .merge(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1a")).into(),
                Record::new(Bytes::from("key2"), Bytes::from("value2a")).into(),
            ])
            .await
            .unwrap();

        // then
        let result1 = storage.get(Bytes::from("key1")).await.unwrap();
        assert_eq!(result1.unwrap().value, Bytes::from("value1,value1a"));

        let result2 = storage.get(Bytes::from("key2")).await.unwrap();
        assert_eq!(result2.unwrap().value, Bytes::from("value2,value2a"));
    }

    #[tokio::test]
    async fn should_return_monotonically_increasing_seqnums_from_put() {
        let storage = InMemoryStorage::new();

        let r1 = storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        let r2 = storage
            .put(vec![
                Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();
        let r3 = storage
            .put(vec![
                Record::new(Bytes::from("k3"), Bytes::from("v3")).into(),
            ])
            .await
            .unwrap();

        assert_eq!(r1.seqnum, 1);
        assert_eq!(r2.seqnum, 2);
        assert_eq!(r3.seqnum, 3);
    }

    #[tokio::test]
    async fn should_return_monotonically_increasing_seqnums_from_apply() {
        let storage = InMemoryStorage::new();

        let r1 = storage
            .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
                Bytes::from("k1"),
                Bytes::from("v1"),
            )))])
            .await
            .unwrap();
        let r2 = storage
            .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
                Bytes::from("k2"),
                Bytes::from("v2"),
            )))])
            .await
            .unwrap();

        assert_eq!(r1.seqnum, 1);
        assert_eq!(r2.seqnum, 2);
    }

    #[tokio::test]
    async fn should_share_seqnum_counter_across_write_methods() {
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);

        let r1 = storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        let r2 = storage
            .merge(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();
        let r3 = storage
            .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
                Bytes::from("k2"),
                Bytes::from("v3"),
            )))])
            .await
            .unwrap();

        assert_eq!(r1.seqnum, 1);
        assert_eq!(r2.seqnum, 2);
        assert_eq!(r3.seqnum, 3);
    }

    #[tokio::test]
    async fn should_start_durable_subscriber_at_zero() {
        let storage = InMemoryStorage::new();
        let rx = storage.subscribe_durable();
        assert_eq!(*rx.borrow(), 0);
    }

    #[tokio::test]
    async fn should_advance_durable_watermark_on_each_write() {
        let storage = InMemoryStorage::new();
        let rx = storage.subscribe_durable();

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 1);

        storage
            .put(vec![
                Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 2);
    }

    #[tokio::test]
    async fn should_merge_empty_values() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let key = Bytes::from("key1");

        // when
        storage
            .merge(vec![Record::empty(key.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::new());
    }

    #[tokio::test]
    async fn should_not_advance_durable_watermark_when_deferred() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        let rx = storage.subscribe_durable();

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0, "durable watermark should not advance");

        storage
            .put(vec![
                Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0, "durable watermark should still be 0");
    }

    #[tokio::test]
    async fn should_advance_durable_watermark_on_flush_when_deferred() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        let rx = storage.subscribe_durable();

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        storage
            .put(vec![
                Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0);

        storage.flush().await.unwrap();
        assert_eq!(*rx.borrow(), 2, "flush should advance to current seqnum");
    }

    #[tokio::test]
    async fn should_advance_durable_watermark_to_specific_seq() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        let rx = storage.subscribe_durable();

        // Write 3 records
        for i in 1..=3 {
            storage
                .put(vec![
                    Record::new(Bytes::from(format!("k{i}")), Bytes::from(format!("v{i}"))).into(),
                ])
                .await
                .unwrap();
        }
        assert_eq!(*rx.borrow(), 0);

        // Partially flush through seqnum 2
        storage.flush_to(2);
        assert_eq!(*rx.borrow(), 2, "should advance to requested seqnum");

        // Flush the rest
        storage.flush_to(3);
        assert_eq!(*rx.borrow(), 3);
    }

    #[tokio::test]
    #[should_panic(expected = "cannot move durable seqnum backwards")]
    async fn should_panic_when_flush_to_moves_backwards() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        for i in 1..=3 {
            storage
                .put(vec![
                    Record::new(Bytes::from(format!("k{i}")), Bytes::from(format!("v{i}"))).into(),
                ])
                .await
                .unwrap();
        }

        storage.flush_to(2);
        storage.flush_to(1);
    }

    #[tokio::test]
    #[should_panic(expected = "cannot flush beyond written seqnum")]
    async fn should_panic_when_flush_to_exceeds_written() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        storage.flush_to(5);
    }

    #[tokio::test]
    async fn should_see_data_in_snapshot_before_flush_when_deferred() {
        let storage = InMemoryStorage::new().with_deferred_durability();

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        // Data is written but not durable — snapshots should still see it
        let snapshot = storage.snapshot().await.unwrap();
        let result = snapshot.get(Bytes::from("k1")).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("v1"));

        // But durable watermark is still 0
        let rx = storage.subscribe_durable();
        assert_eq!(*rx.borrow(), 0);
    }

    #[tokio::test]
    async fn should_not_advance_durable_watermark_on_apply_when_deferred() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        let rx = storage.subscribe_durable();

        storage
            .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
                Bytes::from("k1"),
                Bytes::from("v1"),
            )))])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0);

        storage
            .apply(vec![RecordOp::Delete(Bytes::from("k1"))])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0);

        storage.flush().await.unwrap();
        assert_eq!(*rx.borrow(), 2);
    }

    #[tokio::test]
    async fn should_not_advance_durable_watermark_on_merge_when_deferred() {
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op).with_deferred_durability();
        let rx = storage.subscribe_durable();

        storage
            .merge(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0);

        storage
            .merge(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0);

        storage.flush().await.unwrap();
        assert_eq!(*rx.borrow(), 2);
    }

    #[tokio::test]
    async fn should_support_multiple_flush_cycles_when_deferred() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        let rx = storage.subscribe_durable();

        // First cycle: write and flush
        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();
        assert_eq!(*rx.borrow(), 1);

        // Second cycle: write more and flush again
        storage
            .put(vec![
                Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();
        storage
            .put(vec![
                Record::new(Bytes::from("k3"), Bytes::from("v3")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 1, "should not advance before second flush");

        storage.flush().await.unwrap();
        assert_eq!(*rx.borrow(), 3);
    }

    #[tokio::test]
    async fn should_flush_on_empty_storage_when_deferred() {
        let storage = InMemoryStorage::new().with_deferred_durability();
        let rx = storage.subscribe_durable();

        // Flushing with no writes should be fine (sends 0 again)
        storage.flush().await.unwrap();
        assert_eq!(*rx.borrow(), 0);

        // flush_to(0) should also be fine
        storage.flush_to(0);
        assert_eq!(*rx.borrow(), 0);
    }

    #[tokio::test]
    async fn should_defer_durability_across_mixed_write_methods() {
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op).with_deferred_durability();
        let rx = storage.subscribe_durable();

        // put, apply, merge — all should defer
        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();
        storage
            .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
                Bytes::from("k2"),
                Bytes::from("v2"),
            )))])
            .await
            .unwrap();
        storage
            .merge(vec![
                Record::new(Bytes::from("k3"), Bytes::from("v3")).into(),
            ])
            .await
            .unwrap();
        assert_eq!(*rx.borrow(), 0);

        // Partially flush through seqnum 2
        storage.flush_to(2);
        assert_eq!(*rx.borrow(), 2);

        // Flush the rest
        storage.flush().await.unwrap();
        assert_eq!(*rx.borrow(), 3);
    }

    #[tokio::test]
    async fn should_read_data_written_before_flush_when_deferred() {
        let storage = InMemoryStorage::new().with_deferred_durability();

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
                Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
            ])
            .await
            .unwrap();

        // get and scan see data even though it is not yet durable
        let result = storage.get(Bytes::from("k1")).await.unwrap();
        assert_eq!(result.unwrap().value, Bytes::from("v1"));

        let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(scanned.len(), 2);
    }
}
