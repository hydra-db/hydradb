use std::sync::Arc;

use crate::storage::sst_blocks;
use crate::storage::{MergeOptions, PutOptions};
use crate::{
    BytesRange, CheckpointInfo, Record, StorageError, StorageIterator, StorageRead, StorageResult,
    Ttl,
    storage::{
        MergeOperator, MergeRecordOp, PutRecordOp, RecordOp, Storage, StorageSnapshot,
        WriteOptions, WriteResult,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use slatedb::IterationOrder;
use slatedb::config::{CheckpointOptions, CheckpointScope, ScanOptions};
use slatedb::manifest::VersionedManifest;
use slatedb::{
    Db, DbIterator, DbReader, DbSnapshot, FilterContext, MergeOperator as SlateDbMergeOperator,
    MergeOperatorError, SstReader, WriteBatch, config::WriteOptions as SlateDbWriteOptions,
};
use tokio::sync::watch;

/// Adapter that wraps our `MergeOperator` trait to implement SlateDB's `MergeOperator` trait.
///
/// This allows using our common merge operator interface with SlateDB's merge functionality.
pub struct SlateDbMergeOperatorAdapter {
    operator: Arc<dyn MergeOperator>,
}

impl SlateDbMergeOperatorAdapter {
    fn new(operator: Arc<dyn MergeOperator>) -> Self {
        Self { operator }
    }
}

impl SlateDbMergeOperator for SlateDbMergeOperatorAdapter {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        Ok(self.operator.merge_batch(key, existing_value, &[value]))
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        if operands.is_empty() && existing_value.is_none() {
            return Err(MergeOperatorError::EmptyBatch);
        }
        Ok(self.operator.merge_batch(key, existing_value, operands))
    }
}

/// Returns the default scan options used for storage scans.
fn default_scan_options() -> ScanOptions {
    ScanOptions {
        durability_filter: Default::default(),
        dirty: false,
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: true,
        max_fetch_tasks: 4,
        order: IterationOrder::Ascending,
        filter_context: None,
    }
}

/// Where a [`SlateReadHandle`] reads its manifest from. Both variants expose a
/// live `manifest()`, so each count reflects the latest flushed state.
enum ManifestSource {
    /// The writer's `Db` — the manifest is always current.
    Db(Arc<Db>),
    /// A reader's `DbReader` — the manifest reflects its last poll.
    Reader(Arc<DbReader>),
}

impl ManifestSource {
    fn manifest(&self) -> VersionedManifest {
        match self {
            ManifestSource::Db(db) => db.manifest(),
            ManifestSource::Reader(reader) => reader.manifest(),
        }
    }
}

/// Self-contained slatedb resources for the SST-walk count path, returned by
/// [`StorageRead::slate_read`].
///
/// Owns a manifest source (a live `Db`/`DbReader`) plus an [`SstReader`] pinned
/// to the same path + object store. This is a deliberate, single-method leak of
/// slatedb internals: the count path needs a manifest and an SST reader,
/// neither of which belongs on the storage-neutral trait. The handle is
/// `'static`, so callers can hold it for the lifetime of a read view rather
/// than re-deriving it per call.
///
/// Each [`count_in_range`](SlateReadHandle::count_in_range) reads the manifest
/// fresh from the live source, so counts reflect the latest flushed state
/// without a second polling handle to keep in sync.
pub struct SlateReadHandle {
    source: ManifestSource,
    sst_reader: Arc<SstReader>,
}

impl SlateReadHandle {
    /// Counts physical write operations in `range` by walking the live
    /// manifest's persisted SSTs. See [`sst_blocks::count_in_range`].
    pub async fn count_in_range(
        &self,
        range: &BytesRange,
    ) -> StorageResult<sst_blocks::CountResult> {
        sst_blocks::count_in_range(&self.source.manifest(), &self.sst_reader, range).await
    }

    /// Returns the live manifest snapshot for metadata-only inspection — e.g.
    /// summarizing how data is distributed across the LSM tree. Reads no SST
    /// files; each call reflects the latest flushed state of the live source.
    pub fn manifest(&self) -> VersionedManifest {
        self.source.manifest()
    }
}

/// SlateDB-backed implementation of the Storage trait.
///
/// SlateDB is an embedded key-value store built on object storage, providing
/// LSM-tree semantics with cloud-native durability.
pub struct SlateDbStorage {
    pub(super) db: Arc<Db>,
    durable_tx: watch::Sender<u64>,
    durable_bridge_abort: tokio::task::AbortHandle,
    /// SST reader for the [`StorageRead::slate_read`] count path. Built by the
    /// production constructors ([`crate::StorageBuilder`]); `None` for the bare
    /// `new` used in tests that only exercise get/scan, where `slate_read`
    /// falls back to `None`. `Arc` so the handle can outlive a borrow of `self`.
    sst_reader: Option<Arc<SstReader>>,
}

impl SlateDbStorage {
    /// Creates a new SlateDbStorage instance wrapping the given SlateDB database.
    pub fn new(db: Arc<Db>) -> Self {
        let slate_rx = db.subscribe();
        let (durable_tx, _) = watch::channel(slate_rx.borrow().durable_seq);
        let task = tokio::spawn({
            let tx = durable_tx.clone();
            async move {
                let mut slate_rx = slate_rx;
                while slate_rx.changed().await.is_ok() {
                    let durable_seq = slate_rx.borrow_and_update().durable_seq;
                    // Use send_replace rather than send: SlateDB may publish
                    // a DbStatus change during `open` (e.g. manifest write)
                    // before any consumer has subscribed, and `send` would
                    // return Err on zero receivers — killing the bridge
                    // permanently. send_replace doesn't care about receiver
                    // count, so late subscribers still get future updates.
                    tx.send_replace(durable_seq);
                }
            }
        });

        Self {
            db,
            durable_tx,
            durable_bridge_abort: task.abort_handle(),
            sst_reader: None,
        }
    }

    /// Attaches an [`SstReader`] so [`StorageRead::slate_read`] can serve the
    /// SST-walk count path. Built against the same path + object store as `db`.
    pub fn with_sst_reader(mut self, sst_reader: SstReader) -> Self {
        self.sst_reader = Some(Arc::new(sst_reader));
        self
    }

    /// Returns the underlying SlateDB instance. Useful for callers that need to access
    /// slatedb-specific features like `DbStatus`
    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// Creates a SlateDB `MergeOperator` from our common `MergeOperator` trait.
    ///
    /// This adapter can be used when constructing a SlateDB database with a merge operator:
    /// ```rust,ignore
    /// use common::storage::MergeOperator;
    /// use slatedb::{DbBuilder, object_store::ObjectStore};
    ///
    /// let my_merge_op: Arc<dyn MergeOperator> = Arc::new(MyMergeOperator);
    /// let slate_merge_op = SlateDbStorage::merge_operator_adapter(my_merge_op);
    ///
    /// let db = DbBuilder::new("path", object_store)
    ///     .with_merge_operator(Arc::new(slate_merge_op))
    ///     .build()
    ///     .await?;
    /// ```
    pub fn merge_operator_adapter(operator: Arc<dyn MergeOperator>) -> SlateDbMergeOperatorAdapter {
        SlateDbMergeOperatorAdapter::new(operator)
    }
}

#[async_trait]
impl StorageRead for SlateDbStorage {
    /// Retrieves a single record by key from SlateDB.
    ///
    /// Returns `None` if the key does not exist.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .db
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .db
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    /// Slatedb consults its SST-level filters on `scan_prefix` but not on
    /// `scan`, so routing prefix scans through this path is what lets a
    /// configured `PrefixExtractor` (and custom `FilterPolicy`, parametrized by
    /// `filter_context`) actually skip SSTs.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = default_scan_options().with_filter_context(filter_context);
        let iter = self
            .db
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    fn slate_read(&self) -> Option<SlateReadHandle> {
        self.sst_reader.as_ref().map(|sst_reader| SlateReadHandle {
            // Live writer manifest: reflects everything flushed so far.
            source: ManifestSource::Db(Arc::clone(&self.db)),
            sst_reader: Arc::clone(sst_reader),
        })
    }

    async fn close(&self) -> StorageResult<()> {
        // Stop durable bridge first so no status subscriber outlives DB close.
        self.durable_bridge_abort.abort();
        // `Db::close()` drives block-cache shutdown (including flushing any
        // hybrid disk tier), so we don't manage the cache lifecycle here.
        self.db.close().await.map_err(StorageError::from_storage)?;
        Ok(())
    }
}

pub(super) struct SlateDbIterator {
    iter: DbIterator,
}

#[async_trait]
impl StorageIterator for SlateDbIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        match self.iter.next().await.map_err(StorageError::from_storage)? {
            Some(entry) => Ok(Some(Record::new(entry.key, entry.value))),
            None => Ok(None),
        }
    }
}

/// SlateDB snapshot wrapper that implements StorageSnapshot.
///
/// Provides a consistent read-only view of the database at the time the snapshot was created.
pub struct SlateDbStorageSnapshot {
    snapshot: Arc<DbSnapshot>,
}

#[async_trait]
impl StorageRead for SlateDbStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .snapshot
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .snapshot
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = default_scan_options().with_filter_context(filter_context);
        let iter = self
            .snapshot
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

#[async_trait]
impl StorageSnapshot for SlateDbStorageSnapshot {}

#[async_trait]
impl Storage for SlateDbStorage {
    async fn apply_with_options(
        &self,
        records: Vec<RecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            match op {
                RecordOp::Put(op) => {
                    batch.put_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Merge(op) => {
                    batch.merge_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Delete(key) => batch.delete(key),
            }
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.put_with_options(op.record.key, op.record.value, &op.options.into());
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    async fn merge_with_options(
        &self,
        records: Vec<MergeRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.merge_with_options(op.record.key, op.record.value, &op.options.into());
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(|e| {
                let error_msg = e.to_string();
                if error_msg.contains("merge operator") || error_msg.contains("not configured") {
                    StorageError::Storage(
                        "Merge operator not configured for this database".to_string(),
                    )
                } else {
                    StorageError::from_storage(e)
                }
            })?;
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    fn subscribe_durable(&self) -> watch::Receiver<u64> {
        self.durable_tx.subscribe()
    }

    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Arc::new(SlateDbStorageSnapshot { snapshot }))
    }

    async fn flush(&self) -> StorageResult<()> {
        self.db.flush().await.map_err(StorageError::from_storage)?;
        Ok(())
    }

    async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
        let result = self
            .db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(CheckpointInfo {
            id: result.id,
            manifest_id: result.manifest_id,
        })
    }
}

impl From<Ttl> for slatedb::config::Ttl {
    fn from(value: Ttl) -> Self {
        match value {
            Ttl::Default => slatedb::config::Ttl::Default,
            Ttl::NoExpiry => slatedb::config::Ttl::NoExpiry,
            Ttl::ExpireAfter(ts) => slatedb::config::Ttl::ExpireAfter(ts),
            Ttl::ExpireAt(ts) => slatedb::config::Ttl::ExpireAt(ts),
        }
    }
}

impl From<PutOptions> for slatedb::config::PutOptions {
    fn from(value: PutOptions) -> Self {
        Self {
            ttl: value.ttl.into(),
        }
    }
}

impl From<MergeOptions> for slatedb::config::MergeOptions {
    fn from(value: MergeOptions) -> Self {
        Self {
            ttl: value.ttl.into(),
        }
    }
}

/// Read-only SlateDB storage using `DbReader`.
///
/// This struct provides read-only access to a SlateDB database without fencing,
/// allowing multiple readers to coexist with a single writer.
pub struct SlateDbStorageReader {
    reader: Arc<DbReader>,
    /// SST reader for the [`StorageRead::slate_read`] count path; see
    /// [`SlateDbStorage::sst_reader`].
    sst_reader: Option<Arc<SstReader>>,
}

impl SlateDbStorageReader {
    /// Creates a new SlateDbStorageReader wrapping the given DbReader.
    pub fn new(reader: Arc<DbReader>) -> Self {
        Self {
            reader,
            sst_reader: None,
        }
    }

    /// Attaches an [`SstReader`] so [`StorageRead::slate_read`] can serve the
    /// SST-walk count path. Built against the same path + object store as the
    /// `DbReader`.
    pub fn with_sst_reader(mut self, sst_reader: SstReader) -> Self {
        self.sst_reader = Some(Arc::new(sst_reader));
        self
    }
}

#[async_trait]
impl StorageRead for SlateDbStorageReader {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .reader
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .reader
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = default_scan_options().with_filter_context(filter_context);
        let iter = self
            .reader
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    fn slate_read(&self) -> Option<SlateReadHandle> {
        self.sst_reader.as_ref().map(|sst_reader| SlateReadHandle {
            // The DbReader's last-polled manifest. Scans on this same handle
            // read the same view, so count and scan stay consistent.
            source: ManifestSource::Reader(Arc::clone(&self.reader)),
            sst_reader: Arc::clone(sst_reader),
        })
    }

    async fn close(&self) -> StorageResult<()> {
        // `DbReader::close()` drives block-cache shutdown itself.
        self.reader
            .close()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BytesRange;
    use slatedb::DbBuilder;
    use slatedb::config::Settings;
    use slatedb::object_store::memory::InMemory;
    use slatedb_common::clock::MockSystemClock;

    #[tokio::test]
    async fn should_read_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        // Create writer and write data
        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
                Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        // Create reader and verify data
        let reader = DbReader::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        let record = storage_reader.get(Bytes::from("key1")).await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, Bytes::from("value1"));

        let record = storage_reader.get(Bytes::from("key2")).await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, Bytes::from("value2"));

        let record = storage_reader.get(Bytes::from("key3")).await.unwrap();
        assert!(record.is_none());

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_scan_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        // Create writer and write data
        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("a"), Bytes::from("1")).into(),
                Record::new(Bytes::from("b"), Bytes::from("2")).into(),
                Record::new(Bytes::from("c"), Bytes::from("3")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        // Create reader and scan data
        let reader = DbReader::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        let mut iter = storage_reader
            .scan_iter(BytesRange::unbounded())
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(record) = iter.next().await.unwrap() {
            results.push((record.key, record.value));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (Bytes::from("a"), Bytes::from("1")));
        assert_eq!(results[1], (Bytes::from("b"), Bytes::from("2")));
        assert_eq!(results[2], (Bytes::from("c"), Bytes::from("3")));

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_coexist_writer_and_reader_without_fencing_error() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        // Create writer
        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // Write initial data
        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        // Create reader while writer is still open - this should NOT cause fencing error
        let reader = DbReader::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        // Reader can read the data
        let record = storage_reader.get(Bytes::from("key1")).await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, Bytes::from("value1"));

        // Writer can still write more data
        storage
            .put(vec![
                Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_set_expire_ts_based_on_ttl() {
        // given - storage configured with a 30 second default TTL
        let object_store = Arc::new(InMemory::new());
        let path = "/test/ttl_db";
        let clock = Arc::new(MockSystemClock::new());

        let db = DbBuilder::new(path, object_store.clone())
            .with_settings(Settings {
                default_ttl: Some(30_000),
                ..Default::default()
            })
            .with_system_clock(clock.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // Write three keys at time=0:
        //   key1: expires after 20 seconds
        //   key2: uses default TTL (30 seconds)
        //   key3: never expires
        storage
            .put(vec![
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key1"), Bytes::from("value1")),
                    PutOptions {
                        ttl: Ttl::ExpireAfter(20_000),
                    },
                ),
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key2"), Bytes::from("value2")),
                    PutOptions { ttl: Ttl::Default },
                ),
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key3"), Bytes::from("value3")),
                    PutOptions { ttl: Ttl::NoExpiry },
                ),
            ])
            .await
            .unwrap();

        // then - key1 has expire_ts = 20_000 (time=0 + 20s TTL)
        let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
        assert_eq!(kv1.expire_ts, Some(20_000));

        // then - key2 has expire_ts = 30_000 (time=0 + 30s default TTL)
        let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
        assert_eq!(kv2.expire_ts, Some(30_000));

        // then - key3 has no expire_ts (NoExpiry)
        let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
        assert_eq!(kv3.expire_ts, None);

        storage.close().await.unwrap();
    }

    /// Simple merge operator that concatenates existing and new values.
    struct ConcatMergeOperator;

    impl MergeOperator for ConcatMergeOperator {
        fn merge_batch(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operands: &[Bytes],
        ) -> Bytes {
            let mut result = existing_value.unwrap_or_default().to_vec();
            for operand in operands {
                result.extend_from_slice(operand);
            }
            Bytes::from(result)
        }
    }

    #[tokio::test]
    async fn should_set_expire_ts_on_merge_records_based_on_ttl() {
        // given - storage configured with a 30 second default TTL and a merge operator
        let object_store = Arc::new(InMemory::new());
        let path = "/test/merge_ttl_db";
        let clock = Arc::new(MockSystemClock::new());

        let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMergeOperator);
        let slate_merge_op = SlateDbStorage::merge_operator_adapter(merge_op);
        let db = DbBuilder::new(path, object_store.clone())
            .with_settings(Settings {
                default_ttl: Some(30_000),
                ..Default::default()
            })
            .with_system_clock(clock.clone())
            .with_merge_operator(Arc::new(slate_merge_op))
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // Merge three keys at time=0:
        //   key1: expires after 20 seconds
        //   key2: uses default TTL (30 seconds)
        //   key3: never expires
        storage
            .merge(vec![
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key1"), Bytes::from("v1")),
                    MergeOptions {
                        ttl: Ttl::ExpireAfter(20_000),
                    },
                ),
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key2"), Bytes::from("v2")),
                    MergeOptions { ttl: Ttl::Default },
                ),
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key3"), Bytes::from("v3")),
                    MergeOptions { ttl: Ttl::NoExpiry },
                ),
            ])
            .await
            .unwrap();

        // then - key1 has expire_ts = 20_000 (time=0 + 20s TTL)
        let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
        assert_eq!(kv1.value, Bytes::from("v1"));
        assert_eq!(kv1.expire_ts, Some(20_000));

        // then - key2 has expire_ts = 30_000 (time=0 + 30s default TTL)
        let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
        assert_eq!(kv2.value, Bytes::from("v2"));
        assert_eq!(kv2.expire_ts, Some(30_000));

        // then - key3 has no expire_ts (NoExpiry)
        let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
        assert_eq!(kv3.value, Bytes::from("v3"));
        assert_eq!(kv3.expire_ts, None);

        storage.close().await.unwrap();
    }

    /// Helper: open a DbReader against the same path/object_store and try to
    /// read a key. Returns `true` if the key is present.
    async fn reader_can_see(path: &str, object_store: Arc<InMemory>, key: &str) -> bool {
        reader_can_see_with_merge_op(path, object_store, key, None).await
    }

    async fn reader_can_see_with_merge_op(
        path: &str,
        object_store: Arc<InMemory>,
        key: &str,
        merge_op: Option<Arc<dyn SlateDbMergeOperator + Send + Sync>>,
    ) -> bool {
        let mut builder = DbReader::builder(path, object_store);
        if let Some(op) = merge_op {
            builder = builder.with_merge_operator(op);
        }
        let reader = builder.build().await.unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));
        storage_reader
            .get(Bytes::from(key.to_owned()))
            .await
            .unwrap()
            .is_some()
    }

    #[tokio::test]
    async fn put_defaults_to_not_await_durable() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/put_default_durability";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // put() uses WriteOptions::default() which is await_durable: false
        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        // Data is in memtable only — a reader (which reads from durable state) should NOT see it
        assert!(!reader_can_see(path, object_store.clone(), "k1").await);

        // After explicit flush, reader can see it
        storage.flush().await.unwrap();
        assert!(reader_can_see(path, object_store.clone(), "k1").await);

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn put_with_await_durable_true_is_visible_to_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/put_durable";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // Write with await_durable: true — should be flushed before returning
        storage
            .put_with_options(
                vec![Record::new(Bytes::from("k1"), Bytes::from("v1")).into()],
                WriteOptions {
                    await_durable: true,
                },
            )
            .await
            .unwrap();

        // Reader should see it immediately without explicit flush
        assert!(reader_can_see(path, object_store.clone(), "k1").await);

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn apply_defaults_to_not_await_durable() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/apply_default_durability";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // apply() delegates with WriteOptions::default() (await_durable: false)
        storage
            .apply(vec![RecordOp::Put(
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            )])
            .await
            .unwrap();

        assert!(!reader_can_see(path, object_store.clone(), "k1").await);

        storage.flush().await.unwrap();
        assert!(reader_can_see(path, object_store.clone(), "k1").await);

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn apply_with_await_durable_true_is_visible_to_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/apply_durable";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .apply_with_options(
                vec![RecordOp::Put(
                    Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
                )],
                WriteOptions {
                    await_durable: true,
                },
            )
            .await
            .unwrap();

        assert!(reader_can_see(path, object_store.clone(), "k1").await);

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn merge_defaults_to_not_await_durable() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/merge_default_durability";

        let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMergeOperator);
        let slate_merge_op = Arc::new(SlateDbStorage::merge_operator_adapter(merge_op.clone()));
        let db = DbBuilder::new(path, object_store.clone())
            .with_merge_operator(slate_merge_op.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // merge() delegates with WriteOptions::default() (await_durable: false)
        storage
            .merge(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        let reader_merge_op: Arc<dyn SlateDbMergeOperator + Send + Sync> =
            Arc::new(SlateDbStorage::merge_operator_adapter(merge_op.clone()));
        assert!(
            !reader_can_see_with_merge_op(
                path,
                object_store.clone(),
                "k1",
                Some(reader_merge_op.clone()),
            )
            .await
        );

        storage.flush().await.unwrap();
        assert!(
            reader_can_see_with_merge_op(path, object_store.clone(), "k1", Some(reader_merge_op),)
                .await
        );

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn merge_with_await_durable_true_is_visible_to_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/merge_durable";

        let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMergeOperator);
        let slate_merge_op = Arc::new(SlateDbStorage::merge_operator_adapter(merge_op.clone()));
        let db = DbBuilder::new(path, object_store.clone())
            .with_merge_operator(slate_merge_op.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .merge_with_options(
                vec![Record::new(Bytes::from("k1"), Bytes::from("v1")).into()],
                WriteOptions {
                    await_durable: true,
                },
            )
            .await
            .unwrap();

        let reader_merge_op: Arc<dyn SlateDbMergeOperator + Send + Sync> =
            Arc::new(SlateDbStorage::merge_operator_adapter(merge_op));
        assert!(
            reader_can_see_with_merge_op(path, object_store.clone(), "k1", Some(reader_merge_op),)
                .await
        );

        storage.close().await.unwrap();
    }
}
