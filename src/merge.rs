//! The record-tag-routed merge operator (RFC 0003 §"Merge-operator dispatch
//! table (D11)").
//!
//! **M0 STUB.** In M0 no code path issues `merge` operands — every write is a
//! `put`/`delete` read-modify-write by the single writer (RFC 0002 D2). The
//! associative merges below land in M1 (RFC 0004/0005). This operator exists
//! now only so the writer (and, in M1+, every `DbReader`/compactor) is opened
//! with the *same* operator registered — SlateDB refuses to resolve a stored
//! merge operand without one, so the registration must be in place before the
//! first M1 merge is ever written.
//!
//! # RFC 0003 dispatch table (what the M1 implementer fills in)
//!
//! `MergeOperator` routes on the record tag ([`crate::serde::keys::record_type_of`]);
//! a merge operand on any non-associative record type is a bug (fail-closed in
//! M1). All operands must be associative.
//!
//! | Record type / meta-key                               | Operand kind          | Merge semantics                                             |
//! |------------------------------------------------------|-----------------------|------------------------------------------------------------|
//! | `Meta["deleted_nodes"]`                              | roaring `Treemap` (u64) | set **union** — tombstoned node uids, filtered at read (RFC 0004) |
//! | `Meta["deleted_edges"/pred_id/anchor_uid]`           | roaring `Treemap`     | set **union** — per-(anchor,pred) deleted dst/src uids (RFC 0005) |
//! | `EdgeOut` / `EdgeIn` (fast-add path)                 | roaring set union (u64) | associative **union** into the posting set (RFC 0005)      |
//! | `Meta["count"/pred_id]`, corpus counters             | `i64` LE              | **sum** (degree / edge-count statistics)                   |
//! | `Index`, `Count`, `Node`, `Schema*`, `Xid`, `Log`    | —                     | **no merge** — last-write-wins via `put` only; single-writer RMW |
//!
//! In M1 the roaring rows decode each operand as a `roaring::RoaringTreemap`,
//! fold them (and the existing value) with set union, and re-serialize; the
//! counter row decodes 8-byte little-endian `i64`s and sums them. The
//! non-associative rows must never receive an operand — M1 returns a
//! `MergeOperatorError` for them (fail-closed) rather than silently applying
//! newest-wins.

use bytes::Bytes;
use common::storage::MergeOperator;

/// Record-tag-routed merge operator (RFC 0003 dispatch table).
///
/// **M0 STUB.** The associative merges (deleted-bitmap roaring unions, i64
/// degree/edge counters, EdgeOut/EdgeIn fast-add set union) land in M1
/// (RFC 0004/0005). For M0 no code path issues `merge` ops, so this operator
/// exists only to be registered on the writer (and, in M1+, on every
/// DbReader/compactor). It applies **newest-operand-wins** and logs a warning
/// if it is ever actually invoked, since that would be an unexpected M0 merge.
#[derive(Debug, Default, Clone, Copy)]
pub struct GraphMergeOperator;

impl MergeOperator for GraphMergeOperator {
    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        // Route on the record tag purely for a helpful diagnostic; both Ok and
        // Err are fine here (a malformed key just means "unknown record type").
        let record_type = crate::serde::keys::record_type_of(key);
        tracing::warn!(
            ?record_type,
            operand_count = operands.len(),
            "GraphMergeOperator invoked in M0 (stub, newest-wins)"
        );

        // Newest-operand-wins: the last operand supersedes older operands and
        // the existing value; with no operands, fall back to the existing
        // value; with neither, an empty value.
        operands
            .last()
            .cloned()
            .or(existing_value)
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::Uid;
    use crate::serde::keys::node_key;

    #[test]
    fn should_return_newest_operand_when_existing_value_present() {
        // given — an existing value and two newer operands
        let op = GraphMergeOperator;
        let key = node_key(Uid(1));
        let existing = Some(Bytes::from_static(b"old"));
        let operands = [Bytes::from_static(b"mid"), Bytes::from_static(b"new")];

        // when
        let merged = op.merge_batch(&key, existing, &operands);

        // then — newest operand wins over both the existing value and the mid operand
        assert_eq!(merged, Bytes::from_static(b"new"));
    }

    #[test]
    fn should_return_newest_operand_when_no_existing_value() {
        // given — no existing value, two operands
        let op = GraphMergeOperator;
        let key = node_key(Uid(2));
        let operands = [Bytes::from_static(b"first"), Bytes::from_static(b"second")];

        // when
        let merged = op.merge_batch(&key, None, &operands);

        // then
        assert_eq!(merged, Bytes::from_static(b"second"));
    }

    #[test]
    fn should_return_existing_when_operands_empty() {
        // given — an existing value and no operands
        let op = GraphMergeOperator;
        let key = node_key(Uid(3));

        // when
        let merged = op.merge_batch(&key, Some(Bytes::from_static(b"keep")), &[]);

        // then — existing value is preserved
        assert_eq!(merged, Bytes::from_static(b"keep"));
    }

    #[test]
    fn should_return_empty_when_no_existing_and_no_operands() {
        // given — nothing to merge
        let op = GraphMergeOperator;
        let key = node_key(Uid(4));

        // when / then — empty (never panics)
        assert_eq!(op.merge_batch(&key, None, &[]), Bytes::new());
    }

    #[test]
    fn should_not_panic_on_malformed_short_key() {
        // given — a key too short to carry a valid record-type head
        let op = GraphMergeOperator;
        let malformed = Bytes::from_static(b"\x00");

        // when — routing on a bad key must not panic; newest-wins still applies
        let merged = op.merge_batch(&malformed, None, &[Bytes::from_static(b"v")]);

        // then
        assert_eq!(merged, Bytes::from_static(b"v"));
    }
}
