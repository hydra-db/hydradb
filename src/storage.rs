//! Per-namespace substrate wrapper over [`common`] (RFC 0003 §"Decision":
//! *one SlateDB database per namespace*).
//!
//! [`GraphStorage`] owns the single `Arc<dyn Storage>` for one namespace, opened
//! with [`crate::merge::GraphMergeOperator`] registered. The single writer
//! (RFC 0002 D2) holds one of these and drives all puts/merges/deletes through
//! it. It is a thin, cloneable handle — cloning shares the same underlying
//! database.

use std::sync::Arc;

use bytes::Bytes;
use common::storage::{PutRecordOp, RecordOp};
use common::{
    Record, Storage, StorageBuilder, StorageConfig, StorageSemantics, WriteOptions, WriteResult,
};

use crate::Result;
use crate::serde::RecordType;

/// Per-namespace substrate wrapper: one SlateDB database (via `common`) with
/// the graph merge operator registered. The single writer holds one of these.
#[derive(Clone)]
pub struct GraphStorage {
    inner: Arc<dyn Storage>,
}

impl GraphStorage {
    /// Opens (or creates) a namespace from a storage config, registering
    /// [`crate::merge::GraphMergeOperator`]. NOTE: in M1+, every
    /// `DbReader`/compactor must be built with the SAME merge operator or reads
    /// of merge operands fail.
    pub async fn open(config: &StorageConfig) -> Result<Self> {
        let inner = StorageBuilder::new(config)
            .await?
            .with_semantics(
                StorageSemantics::new()
                    .with_merge_operator(Arc::new(crate::merge::GraphMergeOperator)),
            )
            .build()
            .await?;
        Ok(Self { inner })
    }

    /// Convenience: an in-memory namespace for tests.
    pub async fn in_memory() -> Result<Self> {
        Self::open(&StorageConfig::InMemory).await
    }

    /// Wraps an already-constructed storage handle directly, bypassing
    /// `open`/`in_memory` (M1 D5/D6 escape hatch).
    ///
    /// Used by fault-injection tests that need to intercept `apply` (e.g.
    /// `common::storage::in_memory::FailingStorage`, gated behind the
    /// `test-utils` feature) between a "before crash" writer and a "reopen
    /// after crash" writer built on the same underlying, un-wrapped handle.
    /// Also lets a test build a second `GraphStorage` over the exact same
    /// backing store for a minimal reader (RFC 0004 acceptance #3) without
    /// going through `common::create_storage_read` (whose `StorageConfig::InMemory`
    /// branch constructs a brand-new, disconnected `InMemoryStorage` — see the
    /// M1 D6 handoff note in `write.rs`).
    ///
    /// NOTE: does **not** register [`crate::merge::GraphMergeOperator`] itself
    /// — the caller must ensure `inner` already has it registered (both
    /// `open`/`in_memory` do; wrapping another `GraphStorage`'s own
    /// [`Self::inner`] handle preserves it automatically since it's the same
    /// underlying backend).
    pub fn from_storage(inner: Arc<dyn Storage>) -> Self {
        Self { inner }
    }

    /// The underlying storage handle (escape hatch for the write path).
    pub fn inner(&self) -> &Arc<dyn Storage> {
        &self.inner
    }

    /// Point read by fully-encoded graph key; returns the value bytes.
    pub async fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        let record = self.inner.get(key).await?;
        Ok(record.map(|r| r.value))
    }

    /// Atomic mixed batch (puts/merges/deletes) — the M1 write path uses this.
    pub async fn apply(&self, ops: Vec<RecordOp>) -> Result<WriteResult> {
        Ok(self.inner.apply(ops).await?)
    }

    /// Atomic mixed batch with explicit [`WriteOptions`] — the M1 write path's
    /// primary entrypoint (RFC 0004 §"Logical sequence protocol"): commits with
    /// `await_durable: true` and an injected `seqnum` equal to turbolay's own
    /// logical sequence, so the two never diverge (M1 integration point #2,
    /// `docs/impl/2026-07-03-m1-spike-and-seqnum-decision.md`).
    pub async fn apply_with_options(
        &self,
        ops: Vec<RecordOp>,
        options: WriteOptions,
    ) -> Result<WriteResult> {
        Ok(self.inner.apply_with_options(ops, options).await?)
    }

    /// Subscribes to the durable sequence watermark (RFC 0001 reader freshness
    /// gate: `durable_seq >= token`). With seqnum injection, this durable seq
    /// *is* turbolay's logical seq.
    pub fn subscribe_durable(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.subscribe_durable()
    }

    /// Convenience put of fully-formed records.
    pub async fn put(&self, records: Vec<PutRecordOp>) -> Result<WriteResult> {
        Ok(self.inner.put(records).await?)
    }

    /// Scans every record of one type (uses
    /// [`record_type_range`](crate::serde::keys::record_type_range)).
    pub async fn scan_record_type(&self, rt: RecordType) -> Result<Vec<Record>> {
        Ok(self
            .inner
            .scan(crate::serde::keys::record_type_range(rt))
            .await?)
    }

    /// Flush pending writes to durable storage.
    pub async fn flush(&self) -> Result<()> {
        Ok(self.inner.flush().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::Uid;
    use crate::serde::keys::{log_key, node_key};

    #[tokio::test]
    async fn should_roundtrip_a_node_record_through_put_get_and_scan() {
        // given — an in-memory namespace and one Node record
        let storage = GraphStorage::in_memory().await.expect("open in-memory");
        let key = node_key(Uid(1));
        let value = Bytes::from_static(b"node-1-value");
        let record = Record::new(key.clone(), value.clone());

        // when — put it, then read it back by key
        storage
            .put(vec![PutRecordOp::new(record.clone())])
            .await
            .expect("put node");
        let got = storage.get(key.clone()).await.expect("get node");

        // then — the value round-trips
        assert_eq!(got, Some(value.clone()));

        // and — scanning the Node record type returns exactly this record
        let scanned = storage
            .scan_record_type(RecordType::Node)
            .await
            .expect("scan nodes");
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].key, key);
        assert_eq!(scanned[0].value, value);
    }

    #[tokio::test]
    async fn should_isolate_scan_to_the_requested_record_type() {
        // given — one Node record and one Log record in the same namespace
        let storage = GraphStorage::in_memory().await.expect("open in-memory");
        let node = Record::new(node_key(Uid(7)), Bytes::from_static(b"node"));
        let log = Record::new(log_key(1), Bytes::from_static(b"log"));

        // when — both are written
        storage
            .put(vec![
                PutRecordOp::new(node.clone()),
                PutRecordOp::new(log.clone()),
            ])
            .await
            .expect("put node and log");

        // then — scanning Node returns only the node, not the log
        let nodes = storage
            .scan_record_type(RecordType::Node)
            .await
            .expect("scan nodes");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].key, node.key);
        assert_eq!(nodes[0].value, node.value);
    }
}
