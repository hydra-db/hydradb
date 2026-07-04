//! Crash-safe id allocation for one graph namespace (RFC 0004 §UID).
//!
//! Every id turbolay hands out — dense node/edge [`Uid`]s, interned
//! predicate/label/property `u32`s, and changelog sequence numbers — comes from
//! a [`common::SequenceAllocator`]. Each allocator reserves ids in blocks and
//! persists the block reservation (a `SeqBlock`) under its own `Meta` key. On
//! restart the allocator reloads that reservation and resumes at
//! `base + block_size`, so ids are **monotonic across process restarts and
//! never reused** even if a block was only partially consumed before the crash
//! (RFC 0004 §UID allocation; the M0 exit criterion).
//!
//! [`GraphAllocators`] bundles the five independent id-spaces owned by the
//! single writer. Allocation itself is a cheap in-memory bump; only when a
//! block is exhausted does an allocator return a `Record` the caller must
//! persist to durably reserve the next block. The caller batches that `Record`
//! into the same write it is already making (e.g. the node/edge mutation or the
//! `Xid -> uid` mapping), so id reservation costs no extra round-trip on the
//! common path.
//!
//! # Nil uid
//!
//! [`Uid::NIL`] (`Uid(0)`) is reserved as a sentinel, but M0 allocates densely
//! from `0`: the uid space is not offset to skip it. Whether a namespace ever
//! *stores* uid 0 is a write-path (M1) policy decision; the allocator stays
//! simple and dense here.
//!
//! # v0 4-billion-name ceiling
//!
//! Interned ids (`pred`/`label`/`prop`) are `u32`. The underlying allocator
//! yields `u64`, which is cast down. Exceeding `u32::MAX` distinct names in a
//! single space is a >4-billion-distinct-names condition that v0 does not
//! support; it is `debug_assert!`ed and would silently wrap in release. Raising
//! the ceiling (wider interned ids) is a post-v0 change.

use common::storage::RecordOp;
use common::{Record, SequenceAllocator, Storage};

use crate::Result;
use crate::serde::keys::{meta_key, xid_key};
use crate::serde::{LabelId, PredId, PropId, Uid};

/// `Meta`-key suffix for the dense node/edge uid sequence block.
const SEQ_UID: &[u8] = b"seq/uid";
/// `Meta`-key suffix for the interned predicate-id sequence block.
const SEQ_PRED: &[u8] = b"seq/pred";
/// `Meta`-key suffix for the interned label-id sequence block.
const SEQ_LABEL: &[u8] = b"seq/label";
/// `Meta`-key suffix for the interned property-key-id sequence block.
const SEQ_PROP: &[u8] = b"seq/prop";
/// `Meta`-key suffix for the changelog sequence block.
const SEQ_LOG: &[u8] = b"seq/log";

/// Casts an allocator's `u64` sequence to an interned `u32` id.
///
/// See the module-level note on the v0 4-billion-name ceiling: crossing
/// `u32::MAX` means >4 billion distinct names in one interned space, which v0
/// does not support. It is a `debug_assert!` (a logic bug if it fires), not a
/// recoverable error, so the allocation signatures stay infallible.
#[inline]
fn to_u32(seq: u64) -> u32 {
    debug_assert!(
        seq <= u32::MAX as u64,
        "interned id space exceeded u32::MAX ({seq}); v0 supports at most 4 billion distinct names"
    );
    seq as u32
}

/// Block-reserved, monotonic id spaces for one namespace (RFC 0004 §UID).
///
/// Each space is an independent [`SequenceAllocator`] whose `SeqBlock`
/// reservation lives under its own `Meta` key. Owned by the single writer (not
/// `Send`-shared): allocation mutates in-memory block cursors and is not
/// internally synchronized.
pub struct GraphAllocators {
    /// Dense node/edge uid space (`u64`).
    uid: SequenceAllocator,
    /// Interned predicate-id space (`u32`).
    pred: SequenceAllocator,
    /// Interned label-id space (`u32`).
    label: SequenceAllocator,
    /// Interned property-key-id space (`u32`).
    prop: SequenceAllocator,
    /// Changelog sequence space (`u64`).
    seq: SequenceAllocator,
}

impl GraphAllocators {
    /// Loads (or initializes) all five allocators from storage.
    ///
    /// Each allocator reloads its persisted `SeqBlock` (if any) from its `Meta`
    /// key and resumes at the next unreserved sequence, guaranteeing monotonic,
    /// reuse-free allocation across restarts.
    pub async fn load(storage: &dyn Storage) -> Result<Self> {
        Ok(Self {
            uid: SequenceAllocator::load(storage, meta_key(SEQ_UID)).await?,
            pred: SequenceAllocator::load(storage, meta_key(SEQ_PRED)).await?,
            label: SequenceAllocator::load(storage, meta_key(SEQ_LABEL)).await?,
            prop: SequenceAllocator::load(storage, meta_key(SEQ_PROP)).await?,
            seq: SequenceAllocator::load(storage, meta_key(SEQ_LOG)).await?,
        })
    }

    /// Allocates the next dense node/edge uid.
    ///
    /// If the returned `Record` is `Some`, the caller must persist it (batched
    /// into its own write) to durably reserve the new block before the uid may
    /// be considered committed.
    pub fn allocate_uid(&mut self) -> (Uid, Option<Record>) {
        let (seq, record) = self.uid.allocate_one();
        (Uid(seq), record)
    }

    /// Allocates the next interned predicate id.
    ///
    /// If the returned `Record` is `Some`, the caller must persist it to
    /// reserve the block. See the module note on the v0 4-billion-name ceiling.
    pub fn allocate_pred(&mut self) -> (PredId, Option<Record>) {
        let (seq, record) = self.pred.allocate_one();
        (PredId(to_u32(seq)), record)
    }

    /// Allocates the next interned label id.
    ///
    /// If the returned `Record` is `Some`, the caller must persist it to
    /// reserve the block. See the module note on the v0 4-billion-name ceiling.
    pub fn allocate_label(&mut self) -> (LabelId, Option<Record>) {
        let (seq, record) = self.label.allocate_one();
        (LabelId(to_u32(seq)), record)
    }

    /// Allocates the next interned property-key id.
    ///
    /// If the returned `Record` is `Some`, the caller must persist it to
    /// reserve the block. See the module note on the v0 4-billion-name ceiling.
    pub fn allocate_prop(&mut self) -> (PropId, Option<Record>) {
        let (seq, record) = self.prop.allocate_one();
        (PropId(to_u32(seq)), record)
    }

    /// Allocates the next changelog sequence number.
    ///
    /// If the returned `Record` is `Some`, the caller must persist it to
    /// reserve the block.
    pub fn next_seq(&mut self) -> (u64, Option<Record>) {
        self.seq.allocate_one()
    }
}

/// Resolves an external id to its uid, returning the `RecordOp`s (if any) the
/// caller must fold into its own atomic write batch (RFC 0004 §"xid -> uid";
/// M1 integration point #1).
///
/// This performs a read (the existing-mapping lookup) but never writes to
/// storage itself — persisting the returned ops is the caller's
/// responsibility, so they can be committed atomically together with the rest
/// of the write (e.g. the node record and its changelog entry).
///
/// Idempotent: the same `xid` always resolves to the same uid.
///
/// - If `xid` already has a mapping: returns its uid and an **empty** `Vec` —
///   there is nothing new to persist.
/// - If `xid` is new: allocates a uid (a cheap in-memory bump that may cross a
///   `SeqBlock` boundary) and returns the uid plus the ops to persist — the
///   `Xid -> uid` mapping (value = uid as `u64` **big-endian**, per RFC 0003),
///   and, if the allocation reserved a new block, that `SeqBlock` record too.
///   The caller MUST apply these ops atomically with the rest of its write:
///   handing out the uid without durably persisting its mapping (and any
///   block reservation) in the same batch would let a crash resurrect the
///   same uid for a different `xid` on restart.
pub async fn resolve_or_create_xid_batched(
    storage: &dyn Storage,
    allocs: &mut GraphAllocators,
    xid: &[u8],
) -> Result<(Uid, Vec<RecordOp>)> {
    let key = xid_key(xid);

    // Fast path: mapping already exists — nothing to persist.
    if let Some(record) = storage.get(key.clone()).await? {
        let bytes: [u8; 8] = record.value.as_ref().try_into().map_err(|_| {
            crate::Error::encoding(format!(
                "xid mapping value must be 8 bytes (u64 BE), got {}",
                record.value.len()
            ))
        })?;
        return Ok((Uid(u64::from_be_bytes(bytes)), Vec::new()));
    }

    // First sight: allocate a uid (in-memory only) and hand back the ops the
    // caller must persist — the mapping, plus any block reservation.
    let (uid, block_record) = allocs.allocate_uid();
    let mapping = Record::new(key, bytes::Bytes::copy_from_slice(&uid.get().to_be_bytes()));

    let mut ops = Vec::with_capacity(2);
    if let Some(block_record) = block_record {
        ops.push(RecordOp::Put(block_record.into()));
    }
    ops.push(RecordOp::Put(mapping.into()));

    Ok((uid, ops))
}

/// Eager variant of [`resolve_or_create_xid_batched`] that persists the
/// resolution's ops itself, in their own atomic batch.
///
/// Kept for callers that resolve an xid outside of a larger write batch (e.g.
/// the M0 smoke path in `main.rs`) and have no other ops to fold it into. The
/// write path (M1 deliverable #5) must use
/// [`resolve_or_create_xid_batched`] instead, so the mapping commits
/// atomically with the rest of the write rather than in its own round-trip.
pub async fn resolve_or_create_xid(
    storage: &dyn Storage,
    allocs: &mut GraphAllocators,
    xid: &[u8],
) -> Result<Uid> {
    let (uid, ops) = resolve_or_create_xid_batched(storage, allocs, xid).await?;
    if !ops.is_empty() {
        storage.apply(ops).await?;
    }
    Ok(uid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::StorageRead;
    use common::storage::in_memory::InMemoryStorage;

    /// Persists a returned block `Record`, if any, to reserve it in storage.
    async fn persist(storage: &InMemoryStorage, record: Option<Record>) {
        if let Some(record) = record {
            storage.put(vec![record.into()]).await.unwrap();
        }
    }

    #[tokio::test]
    async fn should_allocate_strictly_increasing_uids_within_a_run() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when
        let mut last: Option<Uid> = None;
        for _ in 0..10 {
            let (uid, record) = allocs.allocate_uid();
            persist(&storage, record).await;
            // then
            if let Some(prev) = last {
                assert!(uid > prev, "uid {uid:?} should exceed previous {prev:?}");
            }
            last = Some(uid);
        }
    }

    #[tokio::test]
    async fn should_not_reuse_uids_after_restart() {
        // given: allocate a batch, persisting every block reservation.
        let storage = InMemoryStorage::new();
        let mut highest = Uid::NIL;
        {
            let mut allocs = GraphAllocators::load(&storage).await.unwrap();
            for _ in 0..5 {
                let (uid, record) = allocs.allocate_uid();
                persist(&storage, record).await;
                highest = uid;
            }
        } // allocator dropped — simulates a restart.

        // when: reload from the same storage.
        let mut reloaded = GraphAllocators::load(&storage).await.unwrap();
        let (next, record) = reloaded.allocate_uid();
        persist(&storage, record).await;

        // then: the next uid is beyond every previously reserved uid.
        assert!(
            next > highest,
            "post-restart uid {next:?} must exceed pre-restart highest {highest:?}"
        );
    }

    #[tokio::test]
    async fn should_not_reuse_interned_ids_after_restart() {
        // given: allocate some predicate ids, persisting reservations.
        let storage = InMemoryStorage::new();
        let mut highest = PredId(0);
        {
            let mut allocs = GraphAllocators::load(&storage).await.unwrap();
            for _ in 0..3 {
                let (pred, record) = allocs.allocate_pred();
                persist(&storage, record).await;
                highest = pred;
            }
        }

        // when: reload and allocate again.
        let mut reloaded = GraphAllocators::load(&storage).await.unwrap();
        let (next, record) = reloaded.allocate_pred();
        persist(&storage, record).await;

        // then: no reuse of the pred space across the restart.
        assert!(
            next > highest,
            "post-restart pred {next:?} must exceed pre-restart highest {highest:?}"
        );
    }

    #[tokio::test]
    async fn should_advance_id_spaces_independently() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when: allocate several uids but no preds.
        for _ in 0..4 {
            let (_, record) = allocs.allocate_uid();
            persist(&storage, record).await;
        }
        let (pred, record) = allocs.allocate_pred();
        persist(&storage, record).await;

        // then: the pred space started fresh at 0, unmoved by uid allocation.
        assert_eq!(pred, PredId(0));
    }

    #[tokio::test]
    async fn should_resolve_same_xid_to_same_uid() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when
        let first = resolve_or_create_xid(&storage, &mut allocs, b"alice")
            .await
            .unwrap();
        let second = resolve_or_create_xid(&storage, &mut allocs, b"alice")
            .await
            .unwrap();

        // then
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn should_resolve_different_xids_to_different_uids() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when
        let a = resolve_or_create_xid(&storage, &mut allocs, b"alice")
            .await
            .unwrap();
        let b = resolve_or_create_xid(&storage, &mut allocs, b"bob")
            .await
            .unwrap();

        // then
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn should_persist_xid_mapping_as_uid_be_bytes() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when
        let uid = resolve_or_create_xid(&storage, &mut allocs, b"carol")
            .await
            .unwrap();

        // then: the stored mapping is the uid as a u64 big-endian.
        let record = storage.get(xid_key(b"carol")).await.unwrap().unwrap();
        assert_eq!(record.value.as_ref(), &uid.get().to_be_bytes());
    }

    #[tokio::test]
    async fn batched_new_xid_returns_mapping_and_block_ops_without_writing() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when: resolving a never-seen xid.
        let (uid, ops) = resolve_or_create_xid_batched(&storage, &mut allocs, b"dave")
            .await
            .unwrap();

        // then: the first uid allocation always crosses a fresh block
        // boundary, so the ops include both the block reservation and the
        // mapping — and nothing has actually been written yet.
        assert_eq!(ops.len(), 2, "expected block record + mapping record");
        assert!(
            storage.get(xid_key(b"dave")).await.unwrap().is_none(),
            "batched resolve must not write to storage itself"
        );

        // and: applying the returned ops makes the mapping (and block
        // reservation) durable.
        storage.apply(ops).await.unwrap();
        let record = storage.get(xid_key(b"dave")).await.unwrap().unwrap();
        assert_eq!(record.value.as_ref(), &uid.get().to_be_bytes());
    }

    #[tokio::test]
    async fn batched_uid_allocation_is_monotonic_across_new_xids() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when: resolving several distinct new xids, applying each op set as
        // we go (mirroring how the write path would fold + commit them).
        let mut last: Option<Uid> = None;
        for xid in [b"x0".as_slice(), b"x1", b"x2", b"x3"] {
            let (uid, ops) = resolve_or_create_xid_batched(&storage, &mut allocs, xid)
                .await
                .unwrap();
            if !ops.is_empty() {
                storage.apply(ops).await.unwrap();
            }
            // then
            if let Some(prev) = last {
                assert!(uid > prev, "uid {uid:?} should exceed previous {prev:?}");
            }
            last = Some(uid);
        }
    }

    #[tokio::test]
    async fn batched_existing_xid_returns_same_uid_and_zero_ops() {
        // given: an xid already resolved (and committed) once.
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();
        let (first, ops) = resolve_or_create_xid_batched(&storage, &mut allocs, b"erin")
            .await
            .unwrap();
        storage.apply(ops).await.unwrap();

        // when: resolving the same xid again.
        let (second, ops) = resolve_or_create_xid_batched(&storage, &mut allocs, b"erin")
            .await
            .unwrap();

        // then: same uid, and nothing new to persist.
        assert_eq!(first, second);
        assert!(ops.is_empty(), "existing xid must not yield new ops");
    }

    #[tokio::test]
    async fn batched_ops_applied_then_read_back_resolve_the_same_uid() {
        // given
        let storage = InMemoryStorage::new();
        let mut allocs = GraphAllocators::load(&storage).await.unwrap();

        // when: resolve a new xid via the batched path and apply its ops as
        // the caller's write batch would.
        let (uid, ops) = resolve_or_create_xid_batched(&storage, &mut allocs, b"frank")
            .await
            .unwrap();
        storage.apply(ops).await.unwrap();

        // then: a fresh resolve (even with a fresh allocator) reads back the
        // same uid from the committed mapping — the fast path, not a new
        // allocation.
        let mut reloaded = GraphAllocators::load(&storage).await.unwrap();
        let (again, ops) = resolve_or_create_xid_batched(&storage, &mut reloaded, b"frank")
            .await
            .unwrap();
        assert_eq!(uid, again);
        assert!(ops.is_empty());
    }

    #[tokio::test]
    async fn batched_uid_allocation_stays_monotonic_across_restart() {
        // given: resolve+apply a batch of new xids, then simulate a restart
        // by dropping the allocator and reloading from the same storage.
        let storage = InMemoryStorage::new();
        let mut highest = Uid::NIL;
        {
            let mut allocs = GraphAllocators::load(&storage).await.unwrap();
            for xid in [b"g0".as_slice(), b"g1", b"g2"] {
                let (uid, ops) = resolve_or_create_xid_batched(&storage, &mut allocs, xid)
                    .await
                    .unwrap();
                storage.apply(ops).await.unwrap();
                highest = uid;
            }
        } // allocator dropped — simulates a restart.

        // when: reload and resolve one more new xid.
        let mut reloaded = GraphAllocators::load(&storage).await.unwrap();
        let (next, ops) = resolve_or_create_xid_batched(&storage, &mut reloaded, b"g3")
            .await
            .unwrap();
        storage.apply(ops).await.unwrap();

        // then: the post-restart uid exceeds every pre-restart uid — no
        // reuse, even though resolution never persisted eagerly.
        assert!(
            next > highest,
            "post-restart uid {next:?} must exceed pre-restart highest {highest:?}"
        );
    }
}
