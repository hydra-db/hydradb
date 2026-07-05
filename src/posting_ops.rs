//! Posting-list operations: add / delete / split / rollup / neighbors (M1 D4,
//! RFC 0005 §"Add/delete mechanics", §"Splitting supernodes").
//!
//! This module turns the [`crate::posting::PostingValue`] type (M1 D1) and the
//! merge operator's fast-add union (M1 D3, [`crate::merge`]) into the actual
//! read/write operations a write path (M1 D5) drives:
//!
//! - [`add`] — size-adaptive fast add. Emits a `RecordOp::Merge` with a
//!   singleton `PostingValue::single` operand (format+kind header, matching
//!   the stored value's own wire format — see `crate::merge`'s module doc for
//!   why a bare `RoaringTreemap` operand doesn't work here), on the base
//!   `EdgeOut`/`EdgeIn` key or, if the list is already split, on the one
//!   `EdgePart` key whose `[min,max]` covers the new neighbor. Matches
//!   [`crate::merge::GraphMergeOperator`]'s operand contract exactly — see
//!   that module's rustdoc.
//! - [`maybe_split`] / [`maybe_split_with_threshold`] — explicit, periodic RMW
//!   the writer calls after adds: reads the merge-resolved posting, and if its
//!   `serialized_len()` has crossed the threshold, bin-splits it by median
//!   cardinality into part keys plus a `Split` manifest. **Never** part of the
//!   merge operator (split is non-associative, single-writer RMW — RFC 0005
//!   §"Splitting supernodes", the M1 handoff gotcha).
//! - [`delete_node_op`] / [`delete_edge_op`] — tombstone-and-filter delete.
//!   Bare singleton-treemap `RecordOp::Merge` into the `Meta` deleted-node /
//!   deleted-edge bitmaps. O(1), degree-independent; physical purge is vacuum
//!   (RFC 0012), not here.
//! - [`neighbors`] — the read path: resolve a posting (`Single` or the union of
//!   a `Split`'s parts), then subtract the per-`(anchor,pred)` deleted-edge
//!   bitmap and the caller-supplied deleted-node bitmap.
//! - [`maybe_rollup`] / [`maybe_rollup_with_threshold`] — explicit RMW the
//!   writer calls when a posting's deleted-edge bitmap has grown past 25% of
//!   its live cardinality: folds the tombstoned uids out of the stored
//!   posting (re-splitting or re-merging parts as the new size dictates) and
//!   drops the now-redundant deleted-edge bitmap.
//!
//! None of these functions write to storage themselves (except the plain
//! `get`s needed to route/resolve) — they return the `RecordOp`s for the
//! caller (the write path, M1 D5) to fold into its own atomic batch, the same
//! idiom [`crate::ids::resolve_or_create_xid_batched`] uses.

use bytes::{BufMut, Bytes, BytesMut};
use common::Record;
use common::storage::{MergeRecordOp, PutRecordOp, RecordOp};
use roaring::RoaringTreemap;

use crate::posting::{PartRef, PostingKind, PostingValue};
use crate::serde::keys::{edge_key, edge_part_key, meta_key};
use crate::serde::{Direction, PredId, Uid};
use crate::storage::GraphStorage;
use crate::{Error, Result};

/// A posting whose serialized size reaches this many bytes is bin-split into
/// parts (RFC 0005 §"Splitting supernodes", default 512 KiB). Tests force a
/// tiny threshold via [`maybe_split_with_threshold`]/[`maybe_rollup_with_threshold`]
/// to exercise the lifecycle without allocating hundreds of thousands of uids.
pub const SPLIT_THRESHOLD: usize = 512 * 1024;

/// Rollup trigger ratio (RFC 0005 §"Rollup"): a posting's deleted-edge bitmap
/// must exceed this fraction of the live cardinality before [`maybe_rollup`]
/// folds it in.
pub const ROLLUP_DELETED_RATIO: f64 = 0.25;

const META_DELETED_NODES: &[u8] = b"deleted_nodes";
const META_DELETED_EDGES_PREFIX: &[u8] = b"deleted_edges/";

// ---------------------------------------------------------------------------
// Bare-RoaringTreemap operand codec — matches `crate::merge`'s contract
// exactly (portable format, no PostingValue header).
// ---------------------------------------------------------------------------

fn encode_roaring(set: &RoaringTreemap) -> Bytes {
    let buf = BytesMut::with_capacity(set.serialized_size());
    let mut writer = buf.writer();
    set.serialize_into(&mut writer)
        .expect("serializing into a BytesMut writer cannot fail");
    writer.into_inner().freeze()
}

fn decode_roaring(bytes: &[u8]) -> Result<RoaringTreemap> {
    RoaringTreemap::deserialize_from(bytes)
        .map_err(|e| Error::encoding(format!("corrupt roaring bitmap bytes: {e}")))
}

fn singleton(v: u64) -> RoaringTreemap {
    let mut set = RoaringTreemap::new();
    set.insert(v);
    set
}

// ---------------------------------------------------------------------------
// Meta deleted-bitmap keys (RFC 0003 dispatch table / RFC 0005 §"Delete").
// `serde::keys` owns the generic `meta_key` builder; these are the specific
// sub-identifiers `crate::merge`'s dispatch already documents.
// ---------------------------------------------------------------------------

/// `Meta["deleted_nodes"]` — the whole-namespace tombstoned-node bitmap.
pub fn deleted_nodes_key() -> Bytes {
    meta_key(META_DELETED_NODES)
}

/// `Meta["deleted_edges"/pred_id/anchor_uid]` — the per-`(anchor,pred)`
/// tombstoned-neighbor bitmap (RFC 0005 §"Delete"). `anchor` is whichever
/// side the caller is filtering from (src for an `Out` read, dst for an `In`
/// read) — the key carries no direction, by design, since a delete tombstones
/// the same logical edge from both sides.
pub fn deleted_edges_key(pred: PredId, anchor: Uid) -> Bytes {
    let mut sub = BytesMut::with_capacity(META_DELETED_EDGES_PREFIX.len() + 12);
    sub.extend_from_slice(META_DELETED_EDGES_PREFIX);
    sub.put_u32(pred.get());
    sub.put_u64(anchor.get());
    meta_key(&sub)
}

// ---------------------------------------------------------------------------
// Add (fast path, O(1), degree-independent)
// ---------------------------------------------------------------------------

/// Finds the part whose key an `add` for `uid` must route to (RFC 0005
/// §"Add": "subsequent adds `merge` into the appropriate part key"). Parts are
/// contiguous, non-overlapping ranges in ascending `start_uid` order (a split
/// invariant maintained by [`maybe_split`]/[`maybe_rollup`]); the target is
/// the last part whose `start_uid <= uid`, or the first part if `uid` is
/// smaller than every part's start (extends the lowest part down).
fn find_target_part(parts: &[PartRef], uid: u64) -> &PartRef {
    debug_assert!(
        !parts.is_empty(),
        "a Split posting value must carry at least one part"
    );
    parts
        .iter()
        .rev()
        .find(|p| p.start_uid <= uid)
        .unwrap_or(&parts[0])
}

/// Adds `neighbor` to the `(dir, anchor, pred)` adjacency posting.
///
/// Reads the base key's manifest to route (a `Single` posting's whole value,
/// or a `Split` posting's small parts manifest — never a whole part's
/// members), then returns a single `RecordOp::Merge` with a singleton
/// operand on whichever key (`EdgeOut`/`EdgeIn` or the target `EdgePart`) the
/// merge operator ([`crate::merge::GraphMergeOperator`]) resolves. Does not
/// write to storage — the caller folds this into its atomic write batch.
///
/// The operand is a full [`PostingValue::single`] encoding (format+kind
/// header), **not** a bare `RoaringTreemap` — see `crate::merge`'s module doc
/// ("Operand encoding") for why: SlateDB's real merge-resolution algorithm may
/// re-feed a `merge_batch` call's own no-existing-value output back in as
/// another operand, so the operand format and that output format must match.
pub async fn add(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
    neighbor: Uid,
) -> Result<Vec<RecordOp>> {
    let base_key = edge_key(dir, anchor, pred);
    let existing = storage.get(base_key.clone()).await?;

    let target_key = match existing {
        None => base_key,
        Some(bytes) => {
            let posting = PostingValue::deserialize(&bytes)?;
            match posting.kind {
                PostingKind::Single(_) => base_key,
                PostingKind::Split(parts) => {
                    let target = find_target_part(&parts, neighbor.get());
                    edge_part_key(dir, anchor, pred, Uid(target.start_uid))
                }
            }
        }
    };

    let operand = PostingValue::single(singleton(neighbor.get())).serialize();
    Ok(vec![RecordOp::Merge(MergeRecordOp::new(Record::new(
        target_key, operand,
    )))])
}

// ---------------------------------------------------------------------------
// Delete (tombstone-and-filter, O(1))
// ---------------------------------------------------------------------------

/// `DeleteNode`: tombstones `uid` by unioning it into `Meta["deleted_nodes"]`.
/// Returns the `RecordOp::Merge` for the caller's batch; does not write to
/// storage. Physical purge is vacuum (RFC 0012), not this deliverable.
pub fn delete_node_op(uid: Uid) -> RecordOp {
    let operand = encode_roaring(&singleton(uid.get()));
    RecordOp::Merge(MergeRecordOp::new(Record::new(
        deleted_nodes_key(),
        operand,
    )))
}

/// `DeleteEdge`: tombstones `neighbor` out of `anchor`'s `pred` adjacency by
/// unioning it into `Meta["deleted_edges"/pred/anchor]`. RFC 0005 requires
/// this be recorded for **both** directions of an edge deletion — the caller
/// invokes this twice, once with `(anchor = src, neighbor = dst)` and once
/// with `(anchor = dst, neighbor = src)`, and folds both ops into one batch.
/// Nothing in the adjacency posting itself is touched here (O(1) regardless
/// of degree) — [`maybe_rollup`] folds the tombstone in later.
pub fn delete_edge_op(pred: PredId, anchor: Uid, neighbor: Uid) -> RecordOp {
    let operand = encode_roaring(&singleton(neighbor.get()));
    RecordOp::Merge(MergeRecordOp::new(Record::new(
        deleted_edges_key(pred, anchor),
        operand,
    )))
}

// ---------------------------------------------------------------------------
// neighbors (read path)
// ---------------------------------------------------------------------------

/// Resolves the live adjacency set for `(anchor, pred, dir)`: reads the
/// posting (a `Single` set, or a `Split` manifest's parts unioned together —
/// driven by the manifest's own `start_uid` list, not a range scan, since the
/// manifest already names every part key exactly), then subtracts the
/// per-`(anchor,pred)` deleted-edge bitmap and the caller-supplied
/// `deleted_nodes` bitmap (RFC 0005 §"Read path for one hop").
pub async fn neighbors(
    storage: &GraphStorage,
    anchor: Uid,
    pred: PredId,
    dir: Direction,
    deleted_nodes: &RoaringTreemap,
) -> Result<RoaringTreemap> {
    let base_key = edge_key(dir, anchor, pred);
    let mut set = match storage.get(base_key).await? {
        None => RoaringTreemap::new(),
        Some(bytes) => {
            resolve_whole_set(
                storage,
                dir,
                anchor,
                pred,
                &PostingValue::deserialize(&bytes)?,
            )
            .await?
        }
    };

    if let Some(bytes) = storage.get(deleted_edges_key(pred, anchor)).await? {
        set -= decode_roaring(&bytes)?;
    }
    set -= deleted_nodes;
    Ok(set)
}

/// Materializes a posting's whole member set: the inline set if `Single`, or
/// the union of every `EdgePart` named in a `Split` manifest.
async fn resolve_whole_set(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
    posting: &PostingValue,
) -> Result<RoaringTreemap> {
    match &posting.kind {
        PostingKind::Single(set) => Ok(set.clone()),
        PostingKind::Split(parts) => {
            let mut sets = Vec::with_capacity(parts.len());
            for part in parts {
                sets.push(read_part_set(storage, dir, anchor, pred, part).await?);
            }
            Ok(PostingValue::union_parts(&sets))
        }
    }
}

/// Reads one `EdgePart` key and returns its inline set. A part must itself be
/// `Single` — a nested split is a manifest-integrity bug (fail-closed).
async fn read_part_set(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
    part: &PartRef,
) -> Result<RoaringTreemap> {
    let key = edge_part_key(dir, anchor, pred, Uid(part.start_uid));
    let bytes = storage.get(key).await?.ok_or_else(|| {
        Error::encoding(format!(
            "split manifest names EdgePart start_uid={} but the key is missing",
            part.start_uid
        ))
    })?;
    match PostingValue::deserialize(&bytes)?.kind {
        PostingKind::Single(set) => Ok(set),
        PostingKind::Split(_) => Err(Error::encoding(
            "EdgePart key holds a nested Split posting value — manifest integrity bug",
        )),
    }
}

// ---------------------------------------------------------------------------
// Split detection + split (RMW, single-writer, never merge)
// ---------------------------------------------------------------------------

/// Bin-splits `set` by median cardinality (roaring `select`) until every part
/// serializes under `threshold`, recursing per RFC 0005 §"Splitting
/// supernodes". A part of cardinality <= 1 is returned as-is regardless of
/// serialized size (recursion base case — nothing left to split).
fn bin_split(set: &RoaringTreemap, threshold: usize) -> Vec<RoaringTreemap> {
    if set.len() <= 1 || PostingValue::single(set.clone()).serialized_len() <= threshold {
        return vec![set.clone()];
    }

    let mid = set.len() / 2;
    let pivot = set.select(mid).expect("mid < len, checked above");

    let mut lo = RoaringTreemap::new();
    let mut hi = RoaringTreemap::new();
    for v in set.iter() {
        if v < pivot {
            lo.insert(v);
        } else {
            hi.insert(v);
        }
    }

    // Degenerate guard: if the pivot failed to separate the set at all (should
    // not happen — uids are distinct so `select(mid)` strictly bounds the
    // upper half), stop recursing rather than looping forever.
    if lo.is_empty() || hi.is_empty() {
        return vec![set.clone()];
    }

    let mut parts = bin_split(&lo, threshold);
    parts.extend(bin_split(&hi, threshold));
    parts
}

/// Builds the `RecordOp::Put`s for a fresh set of part keys plus the `Split`
/// manifest value that names them (does not include the `Put` for the base
/// key itself — callers add that with the manifest's key).
fn build_split_ops(
    dir: Direction,
    anchor: Uid,
    pred: PredId,
    parts_sets: Vec<RoaringTreemap>,
) -> Result<(PostingValue, Vec<RecordOp>)> {
    let mut part_refs = Vec::with_capacity(parts_sets.len());
    let mut ops = Vec::with_capacity(parts_sets.len());

    for set in parts_sets {
        let start_uid = set
            .min()
            .ok_or_else(|| Error::encoding("cannot split an empty part"))?;
        let max_uid = set.max().expect("non-empty, min succeeded");
        let card = u32::try_from(set.len())
            .map_err(|_| Error::value("part cardinality exceeds u32::MAX"))?;

        let key = edge_part_key(dir, anchor, pred, Uid(start_uid));
        let value = PostingValue::single(set).serialize();
        ops.push(RecordOp::Put(PutRecordOp::new(Record::new(key, value))));
        part_refs.push(PartRef {
            start_uid,
            min_uid: start_uid,
            max_uid,
            card,
        });
    }

    Ok((PostingValue::split(part_refs), ops))
}

/// Explicit split-threshold check + split (RFC 0005 §"Splitting supernodes"),
/// with an overridable threshold so tests can force the lifecycle without
/// growing a real 512 KiB posting. [`maybe_split`] is the production entry
/// point, fixed at [`SPLIT_THRESHOLD`].
///
/// Reads the merge-resolved `PostingValue` at the base key.
///
/// - If it is `Single` and its `serialized_len()` has crossed `threshold`,
///   bin-splits it by median cardinality into part keys plus a `Split`
///   manifest and returns the `RecordOp::Put`s for both.
/// - If it is already `Split`, this is also where continued fast-adds into a
///   part (`EdgePart` values only ever merge, never rewrite the manifest —
///   see [`add`]) get reconciled: any part whose *current* stored size has
///   itself crossed `threshold` is bin-split further, flattening into fresh
///   sibling part entries in the same top-level manifest (an `EdgePart` value
///   is never itself a `Split` — RFC 0005/`crate::merge` forbid nesting — so
///   growth always flattens rather than recursing a second level).
///
/// A no-op (`Ok(vec![])`) if the key is absent or nothing has crossed
/// threshold. This is a single-writer RMW — **never** call it from the merge
/// operator (RFC 0005 §"Splitting supernodes" is explicit that split is
/// non-associative).
pub async fn maybe_split_with_threshold(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
    threshold: usize,
) -> Result<Vec<RecordOp>> {
    let base_key = edge_key(dir, anchor, pred);
    let Some(bytes) = storage.get(base_key.clone()).await? else {
        return Ok(vec![]);
    };
    let posting = PostingValue::deserialize(&bytes)?;

    match posting.kind {
        PostingKind::Single(set) => {
            if 2 + set.serialized_size() < threshold || set.is_empty() {
                return Ok(vec![]);
            }
            let parts_sets = bin_split(&set, threshold);
            if parts_sets.len() <= 1 {
                // Could not shrink below threshold by splitting (degenerate
                // guard in `bin_split` triggered) — nothing productive to do.
                return Ok(vec![]);
            }
            let (manifest, mut ops) = build_split_ops(dir, anchor, pred, parts_sets)?;
            ops.push(RecordOp::Put(PutRecordOp::new(Record::new(
                base_key,
                manifest.serialize(),
            ))));
            Ok(ops)
        }
        PostingKind::Split(parts) => {
            reconcile_oversized_parts(storage, dir, anchor, pred, base_key, parts, threshold).await
        }
    }
}

/// Re-splits any part of an existing manifest whose *current* stored size has
/// crossed `threshold` since the last split/rollup (continued fast-adds only
/// ever merge into a part's own value — see [`add`] — so the manifest's
/// per-part metadata is a point-in-time snapshot that this reconciles).
///
/// Every part is read here regardless (that's the only way to know whether it
/// needs re-splitting), so the min/max/card metadata for parts that stay
/// under threshold is opportunistically refreshed from that same read too —
/// otherwise a part that keeps absorbing fast-adds without ever crossing
/// threshold would carry a stale manifest entry forever. `start_uid` (the key
/// identity) never changes for a part that isn't re-split. Returns
/// `Ok(vec![])` if nothing in the manifest actually changed.
async fn reconcile_oversized_parts(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
    base_key: Bytes,
    parts: Vec<PartRef>,
    threshold: usize,
) -> Result<Vec<RecordOp>> {
    let mut new_parts = Vec::with_capacity(parts.len());
    let mut ops = Vec::new();
    let mut changed = false;

    for part in parts {
        let part_set = read_part_set(storage, dir, anchor, pred, &part).await?;
        if 2 + part_set.serialized_size() < threshold || part_set.len() <= 1 {
            let refreshed = PartRef {
                start_uid: part.start_uid,
                min_uid: part_set.min().unwrap_or(part.start_uid),
                max_uid: part_set.max().unwrap_or(part.start_uid),
                card: u32::try_from(part_set.len())
                    .map_err(|_| Error::value("part cardinality exceeds u32::MAX"))?,
            };
            if refreshed != part {
                changed = true;
            }
            new_parts.push(refreshed);
            continue;
        }

        let sub_sets = bin_split(&part_set, threshold);
        if sub_sets.len() <= 1 {
            // Degenerate guard triggered — cannot shrink this part further.
            new_parts.push(part);
            continue;
        }

        changed = true;
        let old_key = edge_part_key(dir, anchor, pred, Uid(part.start_uid));
        let reuses_old_key = sub_sets.iter().any(|s| s.min() == Some(part.start_uid));
        if !reuses_old_key {
            ops.push(RecordOp::Delete(old_key));
        }

        let (manifest, mut sub_ops) = build_split_ops(dir, anchor, pred, sub_sets)?;
        ops.append(&mut sub_ops);
        new_parts.extend(
            manifest
                .parts()
                .expect("build_split_ops always returns Split")
                .iter()
                .copied(),
        );
    }

    if !changed {
        return Ok(vec![]);
    }

    new_parts.sort_by_key(|p| p.start_uid);
    ops.push(RecordOp::Put(PutRecordOp::new(Record::new(
        base_key,
        PostingValue::split(new_parts).serialize(),
    ))));
    Ok(ops)
}

/// [`maybe_split_with_threshold`] at the production [`SPLIT_THRESHOLD`].
pub async fn maybe_split(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
) -> Result<Vec<RecordOp>> {
    maybe_split_with_threshold(storage, dir, anchor, pred, SPLIT_THRESHOLD).await
}

// ---------------------------------------------------------------------------
// Rollup (RMW, single-writer): fold the deleted-edge bitmap into the set.
// ---------------------------------------------------------------------------

/// Explicit rollup check + fold (RFC 0005 §"Rollup"), with an overridable
/// split-threshold so tests can force the re-split/re-merge decision without
/// a real 512 KiB posting. [`maybe_rollup`] is the production entry point.
///
/// If `Meta["deleted_edges"/pred/anchor]` is absent/empty, or its cardinality
/// is at or below [`ROLLUP_DELETED_RATIO`] of the posting's live cardinality,
/// this is a no-op (`Ok(vec![])`). Otherwise it materializes the whole live
/// set (unioning parts if split), subtracts the deleted-edge bitmap, deletes
/// the old part keys (if any), rewrites the base key as `Single` or a fresh
/// `Split` manifest depending on whether the shrunk set still exceeds
/// `threshold`, and drops the now-redundant deleted-edge bitmap key. This is
/// a single-writer RMW — **never** call it from the merge operator.
pub async fn maybe_rollup_with_threshold(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
    threshold: usize,
) -> Result<Vec<RecordOp>> {
    let deleted_key = deleted_edges_key(pred, anchor);
    let deleted = match storage.get(deleted_key.clone()).await? {
        Some(bytes) => decode_roaring(&bytes)?,
        None => return Ok(vec![]),
    };
    if deleted.is_empty() {
        return Ok(vec![]);
    }

    let base_key = edge_key(dir, anchor, pred);
    let Some(bytes) = storage.get(base_key.clone()).await? else {
        return Ok(vec![]);
    };
    let posting = PostingValue::deserialize(&bytes)?;

    let live_card: u64 = match &posting.kind {
        PostingKind::Single(set) => set.len(),
        PostingKind::Split(parts) => parts.iter().map(|p| p.card as u64).sum(),
    };
    if live_card == 0 {
        return Ok(vec![]);
    }
    if (deleted.len() as f64) <= ROLLUP_DELETED_RATIO * (live_card as f64) {
        return Ok(vec![]);
    }

    let old_part_keys: Vec<Bytes> = match &posting.kind {
        PostingKind::Single(_) => vec![],
        PostingKind::Split(parts) => parts
            .iter()
            .map(|p| edge_part_key(dir, anchor, pred, Uid(p.start_uid)))
            .collect(),
    };

    let mut whole = resolve_whole_set(storage, dir, anchor, pred, &posting).await?;
    whole -= &deleted;

    let mut ops = Vec::with_capacity(old_part_keys.len() + 2);
    for key in old_part_keys {
        ops.push(RecordOp::Delete(key));
    }

    if 2 + whole.serialized_size() < threshold {
        ops.push(RecordOp::Put(PutRecordOp::new(Record::new(
            base_key,
            PostingValue::single(whole).serialize(),
        ))));
    } else {
        let parts_sets = bin_split(&whole, threshold);
        let (manifest, mut part_ops) = build_split_ops(dir, anchor, pred, parts_sets)?;
        ops.append(&mut part_ops);
        ops.push(RecordOp::Put(PutRecordOp::new(Record::new(
            base_key,
            manifest.serialize(),
        ))));
    }

    // The deleted-edge bitmap is now folded in; drop it rather than write
    // back an empty bitmap (an absent key already means "nothing deleted").
    ops.push(RecordOp::Delete(deleted_key));

    Ok(ops)
}

/// [`maybe_rollup_with_threshold`] at the production [`SPLIT_THRESHOLD`].
pub async fn maybe_rollup(
    storage: &GraphStorage,
    dir: Direction,
    anchor: Uid,
    pred: PredId,
) -> Result<Vec<RecordOp>> {
    maybe_rollup_with_threshold(storage, dir, anchor, pred, SPLIT_THRESHOLD).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::GraphStorage;
    use std::collections::BTreeSet;

    /// A tiny split threshold so tests exercise the split/rollup lifecycle
    /// without allocating hundreds of thousands of uids. Each `u64` uid costs
    /// ~2-3 bytes in a roaring container in the worst case, so a few hundred
    /// bytes reliably crosses this within a couple hundred inserts.
    const TEST_THRESHOLD: usize = 512;

    /// `GraphStorage::in_memory` registers the real
    /// [`crate::merge::GraphMergeOperator`] (see `storage::GraphStorage::open`)
    /// — these tests exercise the actual merge path, not a mock.
    async fn open_storage() -> GraphStorage {
        GraphStorage::in_memory()
            .await
            .expect("open in-memory graph storage")
    }

    async fn apply(storage: &GraphStorage, ops: Vec<RecordOp>) {
        if !ops.is_empty() {
            storage.apply(ops).await.expect("apply ops");
        }
    }

    async fn do_add(storage: &GraphStorage, dir: Direction, anchor: Uid, pred: PredId, nb: u64) {
        let ops = add(storage, dir, anchor, pred, Uid(nb)).await.expect("add");
        apply(storage, ops).await;
    }

    // -- Split lifecycle (RFC 0005 acceptance #3) ----------------------------

    #[tokio::test]
    async fn should_start_single_cross_threshold_and_split_with_correct_manifest() {
        let storage = open_storage().await;
        let anchor = Uid(1);
        let pred = PredId(7);
        let dir = Direction::Out;

        let mut oracle: BTreeSet<u64> = BTreeSet::new();
        for uid in 0..400u64 {
            do_add(&storage, dir, anchor, pred, uid).await;
            oracle.insert(uid);

            let split_ops = maybe_split_with_threshold(&storage, dir, anchor, pred, TEST_THRESHOLD)
                .await
                .expect("maybe_split");
            apply(&storage, split_ops).await;
        }

        // Confirm it actually split at some point during the loop.
        let base_bytes = storage
            .get(edge_key(dir, anchor, pred))
            .await
            .unwrap()
            .unwrap();
        let posting = PostingValue::deserialize(&base_bytes).unwrap();
        let parts = posting.parts().expect("posting should have split by now");
        assert!(
            parts.len() >= 2,
            "expected at least 2 parts, got {}",
            parts.len()
        );

        // Manifest min/max/card are correct per part, and parts are
        // disjoint + cover the whole range contiguously.
        let mut prev_max: Option<u64> = None;
        let mut total_card = 0u64;
        for part in parts {
            let key = edge_part_key(dir, anchor, pred, Uid(part.start_uid));
            let part_bytes = storage.get(key).await.unwrap().unwrap();
            let part_set = match PostingValue::deserialize(&part_bytes).unwrap().kind {
                PostingKind::Single(s) => s,
                PostingKind::Split(_) => panic!("part must not itself be split"),
            };
            assert_eq!(part.min_uid, part_set.min().unwrap());
            assert_eq!(part.max_uid, part_set.max().unwrap());
            assert_eq!(part.card as u64, part_set.len());
            assert_eq!(part.start_uid, part_set.min().unwrap());
            if let Some(pm) = prev_max {
                assert!(pm < part.min_uid, "parts must be disjoint and ordered");
            }
            prev_max = Some(part.max_uid);
            total_card += part_set.len();
        }
        assert_eq!(total_card, oracle.len() as u64);

        // Whole-set read matches the oracle.
        let deleted_nodes = RoaringTreemap::new();
        let whole = neighbors(&storage, anchor, pred, dir, &deleted_nodes)
            .await
            .unwrap();
        let whole_set: BTreeSet<u64> = whole.iter().collect();
        assert_eq!(whole_set, oracle);

        // Range-filtered read (only parts overlapping [100, 200]) matches the
        // oracle restricted to the same range.
        let expected_range: BTreeSet<u64> = oracle.range(100..=200).copied().collect();
        let mut ranged = RoaringTreemap::new();
        for part in parts {
            if part.max_uid < 100 || part.min_uid > 200 {
                continue;
            }
            let key = edge_part_key(dir, anchor, pred, Uid(part.start_uid));
            let part_bytes = storage.get(key).await.unwrap().unwrap();
            if let PostingKind::Single(s) = PostingValue::deserialize(&part_bytes).unwrap().kind {
                ranged |= s;
            }
        }
        let ranged_filtered: BTreeSet<u64> =
            ranged.iter().filter(|v| (100..=200).contains(v)).collect();
        assert_eq!(ranged_filtered, expected_range);
    }

    // -- Delete correctness + rollup (RFC 0005 acceptance #4) ----------------

    #[tokio::test]
    async fn should_filter_tombstoned_uids_both_directions_and_rollup_without_resurfacing() {
        let storage = open_storage().await;
        let src = Uid(10);
        let dst_base = 1000u64;
        let pred = PredId(3);

        // Build a modest adjacency list both ways (src -> many dsts).
        let mut oracle: BTreeSet<u64> = BTreeSet::new();
        for i in 0..40u64 {
            let dst = dst_base + i;
            do_add(&storage, Direction::Out, src, pred, dst).await;
            do_add(&storage, Direction::In, Uid(dst), pred, src.get()).await;
            oracle.insert(dst);
        }

        // Delete a chunk of edges — tombstone both directions.
        let deleted: Vec<u64> = (dst_base..dst_base + 15).collect();
        let mut ops = Vec::new();
        for &dst in &deleted {
            ops.push(delete_edge_op(pred, src, Uid(dst)));
            ops.push(delete_edge_op(pred, Uid(dst), src));
        }
        apply(&storage, ops).await;
        for d in &deleted {
            oracle.remove(d);
        }

        let deleted_nodes = RoaringTreemap::new();

        // Out direction: src's neighbor set no longer contains the deleted dsts.
        let out = neighbors(&storage, src, pred, Direction::Out, &deleted_nodes)
            .await
            .unwrap();
        let out_set: BTreeSet<u64> = out.iter().collect();
        assert_eq!(out_set, oracle);

        // In direction: each deleted dst no longer lists src as a neighbor.
        for &dst in &deleted {
            let inbound = neighbors(&storage, Uid(dst), pred, Direction::In, &deleted_nodes)
                .await
                .unwrap();
            assert!(
                !inbound.contains(src.get()),
                "deleted edge must be filtered on the In side too"
            );
        }
        // A non-deleted dst still lists src.
        let surviving_dst = dst_base + 20;
        let inbound = neighbors(
            &storage,
            Uid(surviving_dst),
            pred,
            Direction::In,
            &deleted_nodes,
        )
        .await
        .unwrap();
        assert!(inbound.contains(src.get()));

        // Rollup: deleted (15) is > 25% of live-before-rollup (40) so it should fire.
        let rollup_ops = maybe_rollup(&storage, Direction::Out, src, pred)
            .await
            .expect("maybe_rollup");
        assert!(
            !rollup_ops.is_empty(),
            "rollup should fire: 15 deleted / 40 live > 25%"
        );
        apply(&storage, rollup_ops).await;

        // The deleted-edge bitmap is gone.
        assert!(
            storage
                .get(deleted_edges_key(pred, src))
                .await
                .unwrap()
                .is_none(),
            "rollup must clear the deleted-edge bitmap"
        );

        // The live set is unchanged (still matches oracle) post-rollup, and no
        // tombstoned uid resurfaces — even reading with an *empty* deleted
        // bitmap now (since rollup already physically removed them).
        let post_rollup = neighbors(&storage, src, pred, Direction::Out, &deleted_nodes)
            .await
            .unwrap();
        let post_rollup_set: BTreeSet<u64> = post_rollup.iter().collect();
        assert_eq!(post_rollup_set, oracle);
        for d in &deleted {
            assert!(!post_rollup_set.contains(d));
        }

        // A further rollup call is now a no-op (no deleted-edge bitmap left).
        let noop = maybe_rollup(&storage, Direction::Out, src, pred)
            .await
            .unwrap();
        assert!(noop.is_empty());
    }

    #[tokio::test]
    async fn should_not_rollup_below_the_deleted_ratio_threshold() {
        let storage = open_storage().await;
        let anchor = Uid(1);
        let pred = PredId(1);

        for i in 0..40u64 {
            do_add(&storage, Direction::Out, anchor, pred, i).await;
        }
        // Delete only 1 of 40 (2.5%), well under the 25% trigger.
        apply(&storage, vec![delete_edge_op(pred, anchor, Uid(0))]).await;

        let ops = maybe_rollup(&storage, Direction::Out, anchor, pred)
            .await
            .unwrap();
        assert!(ops.is_empty(), "rollup must not fire below the ratio");

        // The uid is still filtered at read time even though rollup didn't fire.
        let deleted_nodes = RoaringTreemap::new();
        let live = neighbors(&storage, anchor, pred, Direction::Out, &deleted_nodes)
            .await
            .unwrap();
        assert!(!live.contains(0));
    }

    // -- Supernode add touches one part (RFC 0005 acceptance #5) -------------

    #[tokio::test]
    async fn should_route_add_to_exactly_one_part_on_an_already_split_list() {
        let storage = open_storage().await;
        let anchor = Uid(5);
        let pred = PredId(2);
        let dir = Direction::Out;

        // Force a split with a small, deterministic set of two well-separated
        // ranges so we can assert exactly which part an add lands in. A tiny
        // threshold (well below `TEST_THRESHOLD`) guarantees the split fires
        // even though 100 densely-packed uids compress well.
        const TINY_THRESHOLD: usize = 64;
        for uid in 0..50u64 {
            do_add(&storage, dir, anchor, pred, uid).await;
        }
        for uid in 1000..1050u64 {
            do_add(&storage, dir, anchor, pred, uid).await;
        }
        let split_ops = maybe_split_with_threshold(&storage, dir, anchor, pred, TINY_THRESHOLD)
            .await
            .unwrap();
        apply(&storage, split_ops).await;

        let base_bytes = storage
            .get(edge_key(dir, anchor, pred))
            .await
            .unwrap()
            .unwrap();
        let posting = PostingValue::deserialize(&base_bytes).unwrap();
        let parts = posting.parts().expect("must be split").to_vec();
        assert!(parts.len() >= 2, "test setup should force >= 2 parts");

        // Snapshot every part key's bytes before the add.
        let mut before = Vec::new();
        for p in &parts {
            let key = edge_part_key(dir, anchor, pred, Uid(p.start_uid));
            before.push((key.clone(), storage.get(key).await.unwrap()));
        }

        // Add a neighbor that clearly belongs to the low-range part. Must be a
        // uid not already a member (0..50 are all already present) so the
        // part's stored bytes actually change — `contains` would make the
        // union a no-op otherwise. 60 sits between the two parts' ranges and
        // routes to the low part (its start_uid <= 60 < the high part's
        // start_uid), extending it upward.
        let new_uid = 60u64;
        let ops = add(&storage, dir, anchor, pred, Uid(new_uid))
            .await
            .unwrap();
        assert_eq!(ops.len(), 1, "a supernode add must emit exactly one op");
        let RecordOp::Merge(merge_op) = &ops[0] else {
            panic!("supernode add must be a Merge op, not Put/Delete");
        };
        let target_key = merge_op.record.key.clone();

        // The target key must be one of the existing part keys (not the base
        // manifest key, and not a rewrite of the whole set).
        assert_ne!(target_key, edge_key(dir, anchor, pred));
        assert!(
            before.iter().any(|(k, _)| *k == target_key),
            "add must target an existing EdgePart key"
        );

        apply(&storage, ops).await;

        // Exactly one part's stored value changed; every other part key (and
        // the manifest) is untouched.
        let mut changed = 0;
        for (key, before_val) in &before {
            let after_val = storage.get(key.clone()).await.unwrap();
            if after_val != *before_val {
                changed += 1;
                assert_eq!(*key, target_key, "only the routed-to part may change");
            }
        }
        assert_eq!(changed, 1, "exactly one part key must have changed");

        // And the new uid is now visible via `neighbors`.
        let deleted_nodes = RoaringTreemap::new();
        let live = neighbors(&storage, anchor, pred, dir, &deleted_nodes)
            .await
            .unwrap();
        assert!(live.contains(new_uid));
    }
}
