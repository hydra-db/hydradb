//! The record-tag-routed merge operator (RFC 0003 §"Merge-operator dispatch
//! table (D11)").
//!
//! **M1 D3.** Every write is still issued by a single writer (RFC 0002 D2),
//! but from M1 on some writes are expressed as SlateDB `merge` operands
//! instead of `put`/`delete` read-modify-writes, so the writer (and, in M1+,
//! every `DbReader`/compactor) must be opened with the *same* operator
//! registered — SlateDB refuses to resolve a stored merge operand without one.
//!
//! # RFC 0003 dispatch table
//!
//! `MergeOperator` routes on the record tag ([`crate::serde::keys::record_type_of`]),
//! and for `Meta` additionally on the sub-key kind
//! ([`crate::serde::keys::meta_kind`]). A merge operand on any non-associative
//! record type is a bug: single-writer RMW means the *only* way an operand
//! reaches a non-associative key is a code-path that emitted the wrong op, so
//! this is fail-closed (see "Fail-closed" below), not newest-wins.
//!
//! | Record type / meta-key                               | Operand kind          | Merge semantics                                             |
//! |------------------------------------------------------|-----------------------|------------------------------------------------------------|
//! | `Meta["deleted_nodes"]`                              | roaring `Treemap` (u64) | set **union** — tombstoned node uids, filtered at read (RFC 0004) |
//! | `Meta["deleted_edges"/pred_id/anchor_uid]`           | roaring `Treemap`     | set **union** — per-(anchor,pred) deleted dst/src uids (RFC 0005) |
//! | `EdgeOut` / `EdgeIn` / `EdgePart` (fast-add path)    | roaring set union (u64) | associative **union** into the posting set (RFC 0005)      |
//! | `Meta["count"/pred_id]`, corpus counters             | `i64` LE              | **sum** (degree / edge-count statistics)                   |
//! | `Index`, `Count`, `Node`, `EdgeProp`, `Schema*`, `Xid`, `Log`, and non-associative `Meta` (e.g. `latest_seq`, `seq/*`) | — | **no merge** — last-write-wins via `put` only; single-writer RMW |
//!
//! ## Operand encoding: bare `RoaringTreemap` for `Meta` bitmaps, full
//! `PostingValue` for the adjacency fast-add path
//!
//! RFC 0005 §"Add / delete mechanics" describes the operand as a bare
//! `roaring_singleton(neighbor)`, but the *wire* encoding actually used for
//! the adjacency fast-add path (`EdgeOut`/`EdgeIn`/`EdgePart`) is the full
//! [`crate::posting::PostingValue`] format (format+kind header, per RFC 0005
//! §"Posting value format") — **not** bare `RoaringTreemap` bytes, unlike the
//! `Meta` deleted-bitmap rows. This asymmetry is required by (and was
//! discovered via) SlateDB's real merge-resolution algorithm
//! (`slatedb::merge_operator::MergeOperatorIterator`), which the M1 in-memory
//! backend's own much simpler merge dispatch never exercises:
//!
//! - **Real SlateDB batches operand resolution and may re-feed its own
//!   intermediate output back in as another operand.** When resolving a run
//!   of merge entries for one key, SlateDB folds operands in batches of up to
//!   100 via `merge_batch(key, None, batch_of_raw_operands)`, collects each
//!   batch's result into a `results` list, and then — **even for a single
//!   batch** — makes one more `merge_batch(key, base_value, &results)` call to
//!   combine those intermediate results with the real base value (`None` if
//!   the key has no stored value yet). That means `merge_batch`'s own output
//!   when `existing_value` is `None` must be usable **as an operand** in a
//!   later call to the very same function — SlateDB's `MergeOperator` trait
//!   has no separate "partial merge" vs "full merge" method to distinguish
//!   the two cases, so the operand format and the "no-existing-value" output
//!   format must be identical.
//! - **The `Meta` deleted-bitmap union already satisfies this** — operand,
//!   existing value, and output are all bare `RoaringTreemap` bytes, so
//!   feeding a prior no-existing-value result back in as an operand decodes
//!   fine.
//! - **The adjacency fast-add path did not**, originally: operands were bare
//!   `RoaringTreemap` bytes but `merge_batch`'s output (and the *stored*,
//!   real-existing-value format) was a full `PostingValue::single(..)`. The
//!   very first write to a fresh adjacency key hit exactly the
//!   re-feed-as-operand case above, and `PostingValue`'s 2-byte
//!   `format`/`kind` header got mis-decoded as `RoaringTreemap` cookie bytes —
//!   a panic (`tests/slatedb_acceptance.rs`'s durable-gate test caught this;
//!   `InMemoryStorage`'s dispatch never re-feeds an intermediate result as an
//!   operand, so the M1-era in-memory-only test suite never saw it). The fix:
//!   adjacency fast-add operands are now `PostingValue::single(singleton)`
//!   bytes too (see [`crate::posting_ops::add`]), so decoding an operand and
//!   decoding a stored/existing value are the *same* code path, and this
//!   function's own no-existing-value output is trivially re-decodable as an
//!   operand.
//!
//! `EdgePart` is included alongside `EdgeOut`/`EdgeIn`: RFC 0005 §"Add"
//! states that once a list is split, "subsequent adds `merge` into the
//! appropriate part key" — a split part is itself a `Single` `PostingValue`,
//! merged exactly like an unsplit `EdgeOut`/`EdgeIn` value. Only the *creation*
//! of the split (rewriting the manifest) and rollup are single-writer RMW
//! (never merge) — see the M1 handoff gotcha this module's tests exercise.
//!
//! If an `EdgeOut`/`EdgeIn`/`EdgePart` key's *existing* value (or an operand)
//! decodes as a `Split` manifest, that is itself a bug: a merge should never
//! race a split rewrite (single-writer), so this operator fails closed on
//! that case too rather than silently corrupting the manifest.
//!
//! ## Fail-closed, expressed as a panic
//!
//! `common::storage::MergeOperator::merge_batch` returns a bare [`Bytes`] —
//! there is no `Result` in the trait signature, so an error return isn't
//! possible. With a single writer (RFC 0002 D2), a merge operand landing on a
//! non-associative key is not a runtime condition to recover from — it is a
//! bug in the code path that emitted the wrong op, and continuing would
//! silently corrupt data (e.g. last-write-wins clobbering a `Node` blob via
//! `merge` instead of `put`). This module therefore treats it as an invariant
//! violation and `panic!`s with a message identifying the key/record type, the
//! same posture RFC 0003 already takes for reserved-bit/tag violations
//! elsewhere in `serde::keys`.

use bytes::{BufMut, Bytes, BytesMut};
use common::storage::MergeOperator;
use roaring::RoaringTreemap;

use crate::RecordType;
use crate::posting::{PostingKind, PostingValue};
use crate::serde::keys::{self, MetaKind};

/// Record-tag-routed merge operator (RFC 0003 dispatch table).
///
/// Registered on the writer and on every `DbReader`/compactor (M1 handoff
/// gotcha) so a stored merge operand always resolves the same way regardless
/// of which SlateDB view read it.
#[derive(Debug, Default, Clone, Copy)]
pub struct GraphMergeOperator;

impl MergeOperator for GraphMergeOperator {
    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        // No operands: nothing to fold in. This can't violate any
        // non-associative invariant (there is no operand to reject), so it's
        // handled uniformly ahead of routing.
        if operands.is_empty() {
            return existing_value.unwrap_or_default();
        }

        let record_type = keys::record_type_of(key).unwrap_or_else(|e| {
            panic!(
                "GraphMergeOperator: merge operand on an unparseable key ({} bytes): {e}",
                key.len()
            )
        });

        match record_type {
            RecordType::EdgeOut | RecordType::EdgeIn | RecordType::EdgePart => {
                merge_posting_union(record_type, existing_value, operands)
            }
            RecordType::Meta => {
                let meta = keys::parse_meta_key(key)
                    .unwrap_or_else(|e| panic!("GraphMergeOperator: malformed Meta key: {e}"));
                match keys::meta_kind(&meta) {
                    MetaKind::DeletedBitmap => merge_bare_roaring_union(existing_value, operands),
                    MetaKind::Counter => merge_i64_sum(existing_value, operands),
                    MetaKind::Scalar => panic!(
                        "GraphMergeOperator: merge operand on non-associative Meta[{meta:?}] — \
                         Meta scalar keys are single-writer read-modify-write only, never merge"
                    ),
                }
            }
            RecordType::Index
            | RecordType::Count
            | RecordType::Node
            | RecordType::EdgeProp
            | RecordType::SchemaName
            | RecordType::SchemaId
            | RecordType::Xid
            | RecordType::Log => panic!(
                "GraphMergeOperator: merge operand on non-associative record type {record_type:?} \
                 ({} operand(s)) — single-writer RMW only, this is a bug in the write path",
                operands.len()
            ),
        }
    }
}

/// Decodes bare portable-format `RoaringTreemap` bytes, panicking (fail-closed)
/// on corruption — a merge operand or stored roaring value that doesn't
/// decode is a data-integrity bug, not a recoverable condition.
fn decode_roaring(bytes: &[u8], context: &str) -> RoaringTreemap {
    RoaringTreemap::deserialize_from(bytes)
        .unwrap_or_else(|e| panic!("GraphMergeOperator: corrupt roaring bytes for {context}: {e}"))
}

/// Serializes a `RoaringTreemap` to bare portable-format bytes (no header).
fn encode_roaring(set: &RoaringTreemap) -> Bytes {
    let buf = BytesMut::with_capacity(set.serialized_size());
    let mut writer = buf.writer();
    set.serialize_into(&mut writer)
        .expect("serializing into a BytesMut writer cannot fail");
    writer.into_inner().freeze()
}

/// `Meta` deleted-bitmap merge: existing value and every operand are bare
/// `RoaringTreemap` bytes; the result is their set union, re-encoded the same
/// way.
fn merge_bare_roaring_union(existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    let mut set = match existing_value {
        Some(bytes) => decode_roaring(&bytes, "Meta deleted-bitmap existing value"),
        None => RoaringTreemap::new(),
    };
    for operand in operands {
        set |= decode_roaring(operand, "Meta deleted-bitmap operand");
    }
    encode_roaring(&set)
}

/// `EdgeOut`/`EdgeIn`/`EdgePart` fast-add merge: the existing value (if any)
/// and every operand are each a full [`PostingValue`] (`Single` kind only —
/// see the module doc's "Operand encoding" section for why operands can't be
/// bare `RoaringTreemap` bytes here, unlike the `Meta` deleted-bitmap union).
/// Every inline set is unioned together; the result is re-wrapped as a
/// `Single` `PostingValue`, so this function's own no-existing-value output
/// is itself a valid operand for a later call — required by SlateDB's real
/// merge-resolution algorithm, which may re-feed such an output back in.
fn merge_posting_union(
    record_type: RecordType,
    existing_value: Option<Bytes>,
    operands: &[Bytes],
) -> Bytes {
    let decode_single = |bytes: &[u8], context: &str| -> RoaringTreemap {
        let posting = PostingValue::deserialize(bytes).unwrap_or_else(|e| {
            panic!("GraphMergeOperator: corrupt {record_type:?} {context}: {e}")
        });
        match posting.kind {
            PostingKind::Single(set) => set,
            PostingKind::Split(_) => panic!(
                "GraphMergeOperator: {context} for a Split {record_type:?} posting list — \
                 split/rollup are single-writer RMW, never merge (RFC 0005 §Splitting \
                 supernodes); this is a bug in the write path"
            ),
        }
    };

    let mut set = match existing_value {
        Some(bytes) => decode_single(&bytes, "posting value"),
        None => RoaringTreemap::new(),
    };
    for operand in operands {
        set |= decode_single(operand, "posting fast-add operand");
    }
    PostingValue::single(set).serialize()
}

/// `Meta["count"/pred_id]` (and similar corpus counters) merge: existing value
/// and every operand are 8-byte little-endian `i64`s, summed.
fn merge_i64_sum(existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    let mut sum: i64 = match existing_value {
        Some(bytes) => decode_i64_le(&bytes, "counter existing value"),
        None => 0,
    };
    for operand in operands {
        sum += decode_i64_le(operand, "counter operand");
    }
    Bytes::copy_from_slice(&sum.to_le_bytes())
}

/// Decodes an 8-byte little-endian `i64`, panicking (fail-closed) on the wrong
/// length.
fn decode_i64_le(bytes: &[u8], context: &str) -> i64 {
    let arr: [u8; 8] = bytes.try_into().unwrap_or_else(|_| {
        panic!(
            "GraphMergeOperator: corrupt i64 counter bytes for {context}: expected 8 bytes, got {}",
            bytes.len()
        )
    });
    i64::from_le_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::Uid;
    use crate::serde::keys::{count_key, edge_key, edge_part_key, log_key, meta_key, node_key};
    use crate::serde::{Direction, PredId};

    fn roaring_of(vals: &[u64]) -> Bytes {
        let set: RoaringTreemap = vals.iter().copied().collect();
        encode_roaring(&set)
    }

    /// A fast-add operand for the `EdgeOut`/`EdgeIn`/`EdgePart` path: a full
    /// `PostingValue::single` encoding, matching what [`crate::posting_ops::add`]
    /// actually emits (see the module doc's "Operand encoding" section) —
    /// deliberately *not* `roaring_of`'s bare `RoaringTreemap` bytes, which is
    /// only the right operand shape for the `Meta` deleted-bitmap union.
    fn posting_op_of(vals: &[u64]) -> Bytes {
        let set: RoaringTreemap = vals.iter().copied().collect();
        PostingValue::single(set).serialize()
    }

    fn treemap_from(bytes: &Bytes) -> RoaringTreemap {
        RoaringTreemap::deserialize_from(bytes.as_ref()).unwrap()
    }

    fn i64_bytes(v: i64) -> Bytes {
        Bytes::copy_from_slice(&v.to_le_bytes())
    }

    fn deleted_edges_meta_key(pred: PredId, anchor: Uid) -> Bytes {
        let mut sub = b"deleted_edges/".to_vec();
        sub.extend_from_slice(&pred.get().to_be_bytes());
        sub.extend_from_slice(&anchor.get().to_be_bytes());
        meta_key(&sub)
    }

    fn count_meta_key(pred: PredId) -> Bytes {
        let mut sub = b"count/".to_vec();
        sub.extend_from_slice(&pred.get().to_be_bytes());
        meta_key(&sub)
    }

    // -- Meta deleted-bitmap union ------------------------------------------

    #[test]
    fn should_union_deleted_nodes_operands_with_existing_value() {
        let op = GraphMergeOperator;
        let key = meta_key(b"deleted_nodes");
        let existing = Some(roaring_of(&[1, 2, 3]));
        let operands = [roaring_of(&[3, 4]), roaring_of(&[5])];

        let merged = op.merge_batch(&key, existing, &operands);

        assert_eq!(
            treemap_from(&merged),
            [1u64, 2, 3, 4, 5].into_iter().collect::<RoaringTreemap>()
        );
    }

    #[test]
    fn should_union_deleted_nodes_operands_with_no_existing_value() {
        let op = GraphMergeOperator;
        let key = meta_key(b"deleted_nodes");
        let operands = [roaring_of(&[10]), roaring_of(&[11, 12])];

        let merged = op.merge_batch(&key, None, &operands);

        assert_eq!(
            treemap_from(&merged),
            [10u64, 11, 12].into_iter().collect::<RoaringTreemap>()
        );
    }

    #[test]
    fn should_union_deleted_edges_operands_by_pred_and_anchor() {
        let op = GraphMergeOperator;
        let key = deleted_edges_meta_key(PredId(7), Uid(42));
        let existing = Some(roaring_of(&[100]));
        let operands = [roaring_of(&[200])];

        let merged = op.merge_batch(&key, existing, &operands);

        assert_eq!(
            treemap_from(&merged),
            [100u64, 200].into_iter().collect::<RoaringTreemap>()
        );
    }

    // -- EdgeOut/EdgeIn/EdgePart fast-add union -------------------------------

    #[test]
    fn should_union_edge_out_fast_add_operands_into_existing_posting_value() {
        let op = GraphMergeOperator;
        let key = edge_key(Direction::Out, Uid(1), PredId(2));
        let existing = Some(PostingValue::single([1u64, 2, 3].into_iter().collect()).serialize());
        let operands = [posting_op_of(&[4]), posting_op_of(&[5, 6])];

        let merged = op.merge_batch(&key, existing, &operands);

        let posting = PostingValue::deserialize(&merged).unwrap();
        assert_eq!(
            posting,
            PostingValue::single([1u64, 2, 3, 4, 5, 6].into_iter().collect())
        );
    }

    #[test]
    fn should_union_edge_in_fast_add_operands_with_no_existing_value() {
        let op = GraphMergeOperator;
        let key = edge_key(Direction::In, Uid(9), PredId(3));
        let operands = [posting_op_of(&[1]), posting_op_of(&[2])];

        let merged = op.merge_batch(&key, None, &operands);

        let posting = PostingValue::deserialize(&merged).unwrap();
        assert_eq!(
            posting,
            PostingValue::single([1u64, 2].into_iter().collect())
        );
    }

    #[test]
    fn should_union_edge_part_fast_add_operands_like_unsplit_lists() {
        let op = GraphMergeOperator;
        let key = edge_part_key(Direction::Out, Uid(1), PredId(2), Uid(1000));
        let existing =
            Some(PostingValue::single([1000u64, 1001].into_iter().collect()).serialize());
        let operands = [posting_op_of(&[1002])];

        let merged = op.merge_batch(&key, existing, &operands);

        let posting = PostingValue::deserialize(&merged).unwrap();
        assert_eq!(
            posting,
            PostingValue::single([1000u64, 1001, 1002].into_iter().collect())
        );
    }

    #[test]
    #[should_panic(expected = "Split")]
    fn should_fail_closed_when_existing_edge_posting_is_a_split_manifest() {
        let op = GraphMergeOperator;
        let key = edge_key(Direction::Out, Uid(1), PredId(2));
        let existing = Some(PostingValue::split(vec![]).serialize());
        let operands = [posting_op_of(&[1])];

        op.merge_batch(&key, existing, &operands);
    }

    // -- Counter (i64 LE sum) --------------------------------------------------

    #[test]
    fn should_sum_counter_operands_with_existing_value() {
        let op = GraphMergeOperator;
        let key = count_meta_key(PredId(3));
        let existing = Some(i64_bytes(10));
        let operands = [i64_bytes(1), i64_bytes(-2)];

        let merged = op.merge_batch(&key, existing, &operands);

        assert_eq!(i64::from_le_bytes(merged.as_ref().try_into().unwrap()), 9);
    }

    #[test]
    fn should_sum_counter_operands_with_no_existing_value() {
        let op = GraphMergeOperator;
        let key = count_meta_key(PredId(4));
        let operands = [i64_bytes(2), i64_bytes(3), i64_bytes(4)];

        let merged = op.merge_batch(&key, None, &operands);

        assert_eq!(i64::from_le_bytes(merged.as_ref().try_into().unwrap()), 9);
    }

    // -- Empty operands ---------------------------------------------------------

    #[test]
    fn should_return_existing_value_unchanged_when_operands_empty() {
        let op = GraphMergeOperator;
        let key = node_key(Uid(3));

        let merged = op.merge_batch(&key, Some(Bytes::from_static(b"keep")), &[]);

        assert_eq!(merged, Bytes::from_static(b"keep"));
    }

    #[test]
    fn should_return_empty_when_no_existing_and_no_operands() {
        let op = GraphMergeOperator;
        let key = node_key(Uid(4));

        assert_eq!(op.merge_batch(&key, None, &[]), Bytes::new());
    }

    // -- Fail-closed: non-associative record types --------------------------

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_node_merge_operand() {
        let op = GraphMergeOperator;
        let key = node_key(Uid(1));
        op.merge_batch(
            &key,
            Some(Bytes::from_static(b"v")),
            &[Bytes::from_static(b"op")],
        );
    }

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_index_merge_operand() {
        let op = GraphMergeOperator;
        let key = crate::serde::keys::index_key(PredId(1), b"tok");
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_count_merge_operand() {
        let op = GraphMergeOperator;
        let key = count_key(PredId(1), Direction::Out, 5);
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_log_merge_operand() {
        let op = GraphMergeOperator;
        let key = log_key(1);
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_schema_name_merge_operand() {
        let op = GraphMergeOperator;
        let key = crate::serde::keys::schema_name_key(crate::serde::SchemaKind::Label, b"Person");
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_schema_id_merge_operand() {
        let op = GraphMergeOperator;
        let key = crate::serde::keys::schema_id_key(crate::serde::SchemaKind::Label, 1);
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_xid_merge_operand() {
        let op = GraphMergeOperator;
        let key = crate::serde::keys::xid_key(b"external-id");
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    #[test]
    #[should_panic(expected = "non-associative record type")]
    fn should_fail_closed_on_edge_prop_merge_operand() {
        let op = GraphMergeOperator;
        let key = crate::serde::keys::edge_prop_key(Uid(1), PredId(2), Uid(3));
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    // -- Fail-closed: non-associative Meta sub-keys --------------------------

    #[test]
    #[should_panic(expected = "non-associative Meta")]
    fn should_fail_closed_on_latest_seq_merge_operand() {
        let op = GraphMergeOperator;
        let key = meta_key(b"latest_seq");
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    #[test]
    #[should_panic(expected = "non-associative Meta")]
    fn should_fail_closed_on_seq_block_merge_operand() {
        let op = GraphMergeOperator;
        let key = meta_key(b"seq/uid");
        op.merge_batch(&key, None, &[Bytes::from_static(b"op")]);
    }

    // -- Malformed key ----------------------------------------------------------

    #[test]
    #[should_panic(expected = "unparseable key")]
    fn should_fail_closed_on_malformed_short_key() {
        let op = GraphMergeOperator;
        let malformed = Bytes::from_static(b"\x00");
        op.merge_batch(&malformed, None, &[Bytes::from_static(b"v")]);
    }
}
