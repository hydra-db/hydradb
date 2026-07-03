//! Key builders and parsers — the inverse pairs for every graph record type
//! (RFC 0003).
//!
//! Each record type has a `*_key(...) -> Bytes` builder and a
//! `parse_*_key(&[u8]) -> Result<...>` parser that is its **exact inverse**.
//! Fixed-width components (uids, interned ids, degrees, seqs) are big-endian so
//! byte order equals numeric order; variable-length components (xids, schema
//! names, `exact` tokens) use `common`'s order-preserving
//! [`terminated_bytes`](common::serde::terminated_bytes) encoding.
//!
//! Scan-range helpers ([`record_type_range`], [`adjacency_part_range`],
//! [`index_pred_range`], [`index_token_range`], [`log_range`]) build the
//! [`BytesRange`]s the read path scans, expressed purely in encoded bytes.

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::KeyPrefix;
use common::serde::terminated_bytes;
use std::ops::Bound::{Excluded, Included};

use super::{Direction, KEY_HEAD_LEN, KEY_VERSION, PredId, RecordType, SUBSYSTEM, SchemaKind, Uid};
use crate::{Error, Result};

// ---------------------------------------------------------------------------
// Head (shared 3-byte prefix)
// ---------------------------------------------------------------------------

/// Starts a key buffer with the 3-byte head `[SUBSYSTEM][KEY_VERSION][tag]`.
fn head(rt: RecordType) -> BytesMut {
    let mut buf = BytesMut::with_capacity(KEY_HEAD_LEN + 24);
    buf.put_u8(SUBSYSTEM);
    buf.put_u8(KEY_VERSION);
    buf.put_u8(rt.tag_byte());
    buf
}

/// The bare 3-byte head, as an owned [`Bytes`] — the prefix of every record of
/// one type.
fn head_bytes(rt: RecordType) -> Bytes {
    let mut buf = BytesMut::with_capacity(KEY_HEAD_LEN);
    buf.put_u8(SUBSYSTEM);
    buf.put_u8(KEY_VERSION);
    buf.put_u8(rt.tag_byte());
    buf.freeze()
}

/// Validates the 3-byte head against `expected` and returns the tail (the
/// bytes after the head).
fn split_head(key: &[u8], expected: RecordType) -> Result<&[u8]> {
    if key.len() < KEY_HEAD_LEN {
        return Err(Error::encoding(format!(
            "key too short for head: need {KEY_HEAD_LEN} bytes, got {}",
            key.len()
        )));
    }
    KeyPrefix::from_bytes_with_validation(key, SUBSYSTEM, KEY_VERSION).map_err(Error::from)?;
    let rt = RecordType::from_tag_byte(key[2])?;
    if rt != expected {
        return Err(Error::encoding(format!(
            "expected record type {expected:?}, got {rt:?}"
        )));
    }
    Ok(&key[KEY_HEAD_LEN..])
}

/// Returns the [`RecordType`] a key belongs to, validating the head. Used by
/// the merge operator to route operands by record tag (RFC 0003 §dispatch).
pub fn record_type_of(key: &[u8]) -> Result<RecordType> {
    if key.len() < KEY_HEAD_LEN {
        return Err(Error::encoding(format!(
            "key too short for head: need {KEY_HEAD_LEN} bytes, got {}",
            key.len()
        )));
    }
    KeyPrefix::from_bytes_with_validation(key, SUBSYSTEM, KEY_VERSION).map_err(Error::from)?;
    RecordType::from_tag_byte(key[2])
}

// ---------------------------------------------------------------------------
// Fixed-width tail helpers
// ---------------------------------------------------------------------------

fn take_u64_be(tail: &mut &[u8], what: &str) -> Result<u64> {
    if tail.len() < 8 {
        return Err(Error::encoding(format!(
            "truncated {what}: need 8 bytes, got {}",
            tail.len()
        )));
    }
    let v = u64::from_be_bytes(tail[..8].try_into().expect("checked len"));
    *tail = &tail[8..];
    Ok(v)
}

fn take_u32_be(tail: &mut &[u8], what: &str) -> Result<u32> {
    if tail.len() < 4 {
        return Err(Error::encoding(format!(
            "truncated {what}: need 4 bytes, got {}",
            tail.len()
        )));
    }
    let v = u32::from_be_bytes(tail[..4].try_into().expect("checked len"));
    *tail = &tail[4..];
    Ok(v)
}

fn take_u8(tail: &mut &[u8], what: &str) -> Result<u8> {
    if tail.is_empty() {
        return Err(Error::encoding(format!("truncated {what}: need 1 byte")));
    }
    let v = tail[0];
    *tail = &tail[1..];
    Ok(v)
}

fn expect_end(tail: &[u8], what: &str) -> Result<()> {
    if !tail.is_empty() {
        return Err(Error::encoding(format!(
            "trailing bytes after {what}: {} extra",
            tail.len()
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Node  (0x3): [uid: u64 BE]
// ---------------------------------------------------------------------------

/// Builds a [`RecordType::Node`] key.
pub fn node_key(uid: Uid) -> Bytes {
    let mut buf = head(RecordType::Node);
    buf.put_u64(uid.get());
    buf.freeze()
}

/// Parses a [`RecordType::Node`] key.
pub fn parse_node_key(key: &[u8]) -> Result<Uid> {
    let mut tail = split_head(key, RecordType::Node)?;
    let uid = take_u64_be(&mut tail, "node uid")?;
    expect_end(tail, "node key")?;
    Ok(Uid(uid))
}

// ---------------------------------------------------------------------------
// EdgeOut (0x4) / EdgeIn (0x5): [anchor: u64 BE][pred: u32 BE]
// ---------------------------------------------------------------------------

/// Builds a whole-list adjacency key ([`RecordType::EdgeOut`] for
/// [`Direction::Out`], [`RecordType::EdgeIn`] for [`Direction::In`]). `anchor`
/// is the source uid for `Out` and the destination uid for `In`.
pub fn edge_key(dir: Direction, anchor: Uid, pred: PredId) -> Bytes {
    let mut buf = head(dir.record_type());
    buf.put_u64(anchor.get());
    buf.put_u32(pred.get());
    buf.freeze()
}

/// Parses an adjacency key, returning its direction, anchor uid, and predicate.
pub fn parse_edge_key(key: &[u8]) -> Result<(Direction, Uid, PredId)> {
    let dir = match record_type_of(key)? {
        RecordType::EdgeOut => Direction::Out,
        RecordType::EdgeIn => Direction::In,
        other => {
            return Err(Error::encoding(format!(
                "expected EdgeOut/EdgeIn, got {other:?}"
            )));
        }
    };
    let mut tail = &key[KEY_HEAD_LEN..];
    let anchor = take_u64_be(&mut tail, "edge anchor")?;
    let pred = take_u32_be(&mut tail, "edge pred")?;
    expect_end(tail, "edge key")?;
    Ok((dir, Uid(anchor), PredId(pred)))
}

// ---------------------------------------------------------------------------
// EdgePart (0x6): [dir:1][anchor: u64 BE][pred: u32 BE][start: u64 BE]
// ---------------------------------------------------------------------------

/// Builds a split-part key for an oversized adjacency posting list. `start` is
/// the first uid held by the part (the bin-split pivot, RFC 0005).
pub fn edge_part_key(dir: Direction, anchor: Uid, pred: PredId, start: Uid) -> Bytes {
    let mut buf = head(RecordType::EdgePart);
    buf.put_u8(dir.as_byte());
    buf.put_u64(anchor.get());
    buf.put_u32(pred.get());
    buf.put_u64(start.get());
    buf.freeze()
}

/// Parses a split-part key.
pub fn parse_edge_part_key(key: &[u8]) -> Result<(Direction, Uid, PredId, Uid)> {
    let mut tail = split_head(key, RecordType::EdgePart)?;
    let dir = Direction::from_byte(take_u8(&mut tail, "edge-part dir")?)?;
    let anchor = take_u64_be(&mut tail, "edge-part anchor")?;
    let pred = take_u32_be(&mut tail, "edge-part pred")?;
    let start = take_u64_be(&mut tail, "edge-part start")?;
    expect_end(tail, "edge-part key")?;
    Ok((dir, Uid(anchor), PredId(pred), Uid(start)))
}

// ---------------------------------------------------------------------------
// Index (0x7): [pred: u32 BE][token: order-preserving bytes]
// ---------------------------------------------------------------------------

/// Builds an [`RecordType::Index`] key. `token` is an order-preserving token
/// from [`super::token`] (fixed-width numeric, or `terminated` exact/hash).
pub fn index_key(pred: PredId, token: &[u8]) -> Bytes {
    let mut buf = head(RecordType::Index);
    buf.put_u32(pred.get());
    buf.extend_from_slice(token);
    buf.freeze()
}

/// Parses an [`RecordType::Index`] key, returning the predicate and the raw
/// token bytes (the token's own decoder in [`super::token`] interprets them).
pub fn parse_index_key(key: &[u8]) -> Result<(PredId, Bytes)> {
    let tail = split_head(key, RecordType::Index)?;
    let mut cur = tail;
    let pred = take_u32_be(&mut cur, "index pred")?;
    Ok((PredId(pred), Bytes::copy_from_slice(cur)))
}

// ---------------------------------------------------------------------------
// Count (0x8): [pred: u32 BE][dir:1][degree: u32 BE]
// ---------------------------------------------------------------------------

/// Builds a [`RecordType::Count`] key — nodes with exactly `degree` edges on
/// `pred` in direction `dir`.
pub fn count_key(pred: PredId, dir: Direction, degree: u32) -> Bytes {
    let mut buf = head(RecordType::Count);
    buf.put_u32(pred.get());
    buf.put_u8(dir.as_byte());
    buf.put_u32(degree);
    buf.freeze()
}

/// Parses a [`RecordType::Count`] key.
pub fn parse_count_key(key: &[u8]) -> Result<(PredId, Direction, u32)> {
    let mut tail = split_head(key, RecordType::Count)?;
    let pred = take_u32_be(&mut tail, "count pred")?;
    let dir = Direction::from_byte(take_u8(&mut tail, "count dir")?)?;
    let degree = take_u32_be(&mut tail, "count degree")?;
    expect_end(tail, "count key")?;
    Ok((PredId(pred), dir, degree))
}

// ---------------------------------------------------------------------------
// Xid (0x9): [xid: terminated_bytes]
// ---------------------------------------------------------------------------

/// Builds an [`RecordType::Xid`] (external-id → uid) key.
pub fn xid_key(xid: &[u8]) -> Bytes {
    let mut buf = head(RecordType::Xid);
    terminated_bytes::serialize(xid, &mut buf);
    buf.freeze()
}

/// Parses an [`RecordType::Xid`] key, returning the raw external id.
pub fn parse_xid_key(key: &[u8]) -> Result<Bytes> {
    let tail = split_head(key, RecordType::Xid)?;
    let mut cur = tail;
    let xid = terminated_bytes::deserialize(&mut cur).map_err(Error::from)?;
    expect_end(cur, "xid key")?;
    Ok(xid)
}

// ---------------------------------------------------------------------------
// Log (0xA): [seq: u64 BE]
// ---------------------------------------------------------------------------

/// Builds a [`RecordType::Log`] (changelog) key. Fixed 8-byte big-endian seq so
/// the tail scan `(W, latest]` is a bounded key-range scan (RFC 0003).
pub fn log_key(seq: u64) -> Bytes {
    let mut buf = head(RecordType::Log);
    buf.put_u64(seq);
    buf.freeze()
}

/// Parses a [`RecordType::Log`] key.
pub fn parse_log_key(key: &[u8]) -> Result<u64> {
    let mut tail = split_head(key, RecordType::Log)?;
    let seq = take_u64_be(&mut tail, "log seq")?;
    expect_end(tail, "log key")?;
    Ok(seq)
}

// ---------------------------------------------------------------------------
// Meta (0xB): [meta_key: bytes]
// ---------------------------------------------------------------------------

/// Builds a [`RecordType::Meta`] key. `meta` is an internal, fixed identifier
/// (e.g. `b"latest_seq"`, `b"seq/uid"`); it is the final key component so it is
/// stored raw (no terminator).
pub fn meta_key(meta: &[u8]) -> Bytes {
    let mut buf = head(RecordType::Meta);
    buf.extend_from_slice(meta);
    buf.freeze()
}

/// Parses a [`RecordType::Meta`] key, returning the raw meta identifier.
pub fn parse_meta_key(key: &[u8]) -> Result<Bytes> {
    let tail = split_head(key, RecordType::Meta)?;
    Ok(Bytes::copy_from_slice(tail))
}

// ---------------------------------------------------------------------------
// SchemaName (0x1): [kind:1][name: terminated_bytes]
// ---------------------------------------------------------------------------

/// Builds a [`RecordType::SchemaName`] (`name -> id`) key.
pub fn schema_name_key(kind: SchemaKind, name: &[u8]) -> Bytes {
    let mut buf = head(RecordType::SchemaName);
    buf.put_u8(kind.as_byte());
    terminated_bytes::serialize(name, &mut buf);
    buf.freeze()
}

/// Parses a [`RecordType::SchemaName`] key.
pub fn parse_schema_name_key(key: &[u8]) -> Result<(SchemaKind, Bytes)> {
    let tail = split_head(key, RecordType::SchemaName)?;
    let mut cur = tail;
    let kind = SchemaKind::from_byte(take_u8(&mut cur, "schema-name kind")?)?;
    let name = terminated_bytes::deserialize(&mut cur).map_err(Error::from)?;
    expect_end(cur, "schema-name key")?;
    Ok((kind, name))
}

// ---------------------------------------------------------------------------
// SchemaId (0x2): [kind:1][id: u32 BE]
// ---------------------------------------------------------------------------

/// Builds a [`RecordType::SchemaId`] (`id -> SchemaEntry`) key.
pub fn schema_id_key(kind: SchemaKind, id: u32) -> Bytes {
    let mut buf = head(RecordType::SchemaId);
    buf.put_u8(kind.as_byte());
    buf.put_u32(id);
    buf.freeze()
}

/// Parses a [`RecordType::SchemaId`] key.
pub fn parse_schema_id_key(key: &[u8]) -> Result<(SchemaKind, u32)> {
    let mut tail = split_head(key, RecordType::SchemaId)?;
    let kind = SchemaKind::from_byte(take_u8(&mut tail, "schema-id kind")?)?;
    let id = take_u32_be(&mut tail, "schema-id id")?;
    expect_end(tail, "schema-id key")?;
    Ok((kind, id))
}

// ---------------------------------------------------------------------------
// Scan-range helpers
// ---------------------------------------------------------------------------

/// A range covering **every** key of one record type — the whole tag's slice
/// of the keyspace.
pub fn record_type_range(rt: RecordType) -> BytesRange {
    BytesRange::prefix(head_bytes(rt))
}

/// A range covering all split parts of one `(dir, anchor, pred)` adjacency list
/// (all [`RecordType::EdgePart`] keys sharing that prefix).
pub fn adjacency_part_range(dir: Direction, anchor: Uid, pred: PredId) -> BytesRange {
    let mut buf = head(RecordType::EdgePart);
    buf.put_u8(dir.as_byte());
    buf.put_u64(anchor.get());
    buf.put_u32(pred.get());
    BytesRange::prefix(buf.freeze())
}

/// A range covering every index token for one predicate (all
/// [`RecordType::Index`] keys under `pred`) — the whole of a predicate's index.
pub fn index_pred_range(pred: PredId) -> BytesRange {
    let mut buf = head(RecordType::Index);
    buf.put_u32(pred.get());
    BytesRange::prefix(buf.freeze())
}

/// A closed token range `[lo_token, hi_token]` within one predicate's index —
/// the encoded form of a `>=`/`<=` range predicate. Both bounds are inclusive;
/// `lo`/`hi` are order-preserving tokens from [`super::token`].
pub fn index_token_range(pred: PredId, lo: &[u8], hi: &[u8]) -> BytesRange {
    BytesRange::new(Included(index_key(pred, lo)), Included(index_key(pred, hi)))
}

/// The changelog tail range `(after, through]` — exclusive of `after`,
/// inclusive of `through` (RFC 0001 index-watermark + changelog-tail merge).
pub fn log_range(after: u64, through: u64) -> BytesRange {
    BytesRange::new(Excluded(log_key(after)), Included(log_key(through)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_node_key() {
        // given / when / then
        for uid in [0u64, 1, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX] {
            let key = node_key(Uid(uid));
            assert_eq!(parse_node_key(&key).unwrap(), Uid(uid));
        }
    }

    #[test]
    fn should_roundtrip_edge_key_both_directions() {
        // given / when / then
        for dir in [Direction::Out, Direction::In] {
            let key = edge_key(dir, Uid(42), PredId(7));
            assert_eq!(parse_edge_key(&key).unwrap(), (dir, Uid(42), PredId(7)));
        }
    }

    #[test]
    fn should_roundtrip_edge_part_key() {
        // given / when / then
        let key = edge_part_key(Direction::In, Uid(9), PredId(3), Uid(1000));
        assert_eq!(
            parse_edge_part_key(&key).unwrap(),
            (Direction::In, Uid(9), PredId(3), Uid(1000))
        );
    }

    #[test]
    fn should_roundtrip_index_key() {
        // given — an opaque token
        let token = b"\x00\x01\xff hello";
        let key = index_key(PredId(5), token);

        // when
        let (pred, got) = parse_index_key(&key).unwrap();

        // then
        assert_eq!(pred, PredId(5));
        assert_eq!(got.as_ref(), token);
    }

    #[test]
    fn should_roundtrip_count_key() {
        let key = count_key(PredId(11), Direction::Out, 128);
        assert_eq!(
            parse_count_key(&key).unwrap(),
            (PredId(11), Direction::Out, 128)
        );
    }

    #[test]
    fn should_roundtrip_xid_key_with_embedded_special_bytes() {
        // given — an xid containing the terminator/escape bytes
        let xid = b"user\x00id\x01x";
        let key = xid_key(xid);

        // when / then
        assert_eq!(parse_xid_key(&key).unwrap().as_ref(), xid);
    }

    #[test]
    fn should_roundtrip_log_key() {
        let key = log_key(123456789);
        assert_eq!(parse_log_key(&key).unwrap(), 123456789);
    }

    #[test]
    fn should_roundtrip_meta_key() {
        let key = meta_key(b"latest_seq");
        assert_eq!(parse_meta_key(&key).unwrap().as_ref(), b"latest_seq");
    }

    #[test]
    fn should_roundtrip_schema_name_and_id_keys() {
        let nk = schema_name_key(SchemaKind::Predicate, b"KNOWS");
        assert_eq!(
            parse_schema_name_key(&nk).unwrap(),
            (SchemaKind::Predicate, Bytes::from_static(b"KNOWS"))
        );

        let ik = schema_id_key(SchemaKind::Label, 77);
        assert_eq!(parse_schema_id_key(&ik).unwrap(), (SchemaKind::Label, 77));
    }

    #[test]
    fn should_identify_record_type_of_any_key() {
        assert_eq!(record_type_of(&node_key(Uid(1))).unwrap(), RecordType::Node);
        assert_eq!(
            record_type_of(&edge_key(Direction::In, Uid(1), PredId(1))).unwrap(),
            RecordType::EdgeIn
        );
        assert_eq!(record_type_of(&log_key(1)).unwrap(), RecordType::Log);
    }

    #[test]
    fn should_reject_wrong_record_type_on_parse() {
        // given — a node key parsed as an edge key
        let key = node_key(Uid(1));

        // when / then
        assert!(parse_edge_key(&key).is_err());
        assert!(parse_log_key(&key).is_err());
    }

    #[test]
    fn should_reject_truncated_and_trailing_bytes() {
        // given — a valid node key
        let key = node_key(Uid(1));

        // truncated tail
        assert!(parse_node_key(&key[..key.len() - 1]).is_err());

        // trailing byte
        let mut extended = key.to_vec();
        extended.push(0x00);
        assert!(parse_node_key(&extended).is_err());
    }

    #[test]
    fn should_order_uids_by_key_bytes() {
        // The core contract the posting-list set algebra relies on: for a
        // fixed record type, key byte order == numeric uid order.
        let a = node_key(Uid(1));
        let b = node_key(Uid(2));
        let c = node_key(Uid(u32::MAX as u64 + 1));
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn should_cluster_index_tokens_under_their_predicate() {
        // given — the same token under two adjacent predicates
        let t = b"tok";
        let k5 = index_key(PredId(5), t);
        let k6 = index_key(PredId(6), t);

        // then — pred 5's whole range contains k5 but not k6
        let r5 = index_pred_range(PredId(5));
        assert!(r5.contains(&k5));
        assert!(!r5.contains(&k6));
        // and predicates are ordered
        assert!(k5 < k6);
    }

    #[test]
    fn should_bound_changelog_tail_exclusive_of_watermark() {
        // given — the tail (10, 13]
        let r = log_range(10, 13);

        // then — 10 excluded, 11..=13 included, 14 excluded
        assert!(!r.contains(&log_key(10)));
        assert!(r.contains(&log_key(11)));
        assert!(r.contains(&log_key(13)));
        assert!(!r.contains(&log_key(14)));
    }
}
