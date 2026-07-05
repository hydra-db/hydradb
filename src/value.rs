//! The node record, its scalar value type, and the changelog record (RFC
//! 0004 §"Node record", §"ChangeRecord schema").
//!
//! `NodeRecord` is the monolithic blob stored at every `Node[uid]` key: labels,
//! properties, and the external id. Serialization sits behind [`NodeCodec`];
//! v0 ships [`V0NodeCodec`], **hand-rolled fail-closed little-endian encoding** —
//! matching every other codec in this crate ([`crate::posting::PostingValue`],
//! [`crate::schema::SchemaEntry`]), not the `bincode` RFC 0004 mentions as a
//! default (bincode is not actually a dependency anywhere in this fork; the
//! codec is behind the trait either way, so the choice is not load-bearing —
//! RFC 0004 §"Alternatives considered").
//!
//! `ChangeRecord` is the value at every `Log[seq]` key (RFC 0004
//! §"Logical sequence protocol") — this module owns its shape and codec too;
//! *writing* changelog entries is the write path's job (M1 D5).
//!
//! House rule (opendata): keys are big-endian, **values are little-endian**.

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use common::serde::encoding::{decode_array_count, encode_array_count};

use crate::serde::{LabelId, PredId, PropId, Uid};
use crate::{Error, Result};

/// The default node-size cap (RFC 0004 §"Node size cap") — a node's encoded
/// `NodeRecord` larger than this is rejected with `oversize_node` rather than
/// written. Configurable per call via [`V0NodeCodec::encode_with_cap`].
pub const DEFAULT_NODE_SIZE_CAP: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// TypedValue
// ---------------------------------------------------------------------------

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_DATETIME: u8 = 6;

/// A scalar property value (RFC 0004 §"Data model"; the types RFC 0006's
/// tokenizers understand). Arrays are modeled as `list` predicates
/// (multi-value), not as a `TypedValue` variant.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Bytes),
    /// Epoch-millis timestamp (RFC 0006 §"DateTime is indexed as epoch-millis
    /// via the `int` tokenizer").
    DateTime(i64),
}

impl TypedValue {
    fn write_to(&self, buf: &mut BytesMut) {
        match self {
            TypedValue::Null => buf.put_u8(TAG_NULL),
            TypedValue::Bool(b) => {
                buf.put_u8(TAG_BOOL);
                buf.put_u8(u8::from(*b));
            }
            TypedValue::Int(i) => {
                buf.put_u8(TAG_INT);
                buf.put_i64_le(*i);
            }
            TypedValue::Float(f) => {
                buf.put_u8(TAG_FLOAT);
                buf.put_f64_le(*f);
            }
            TypedValue::String(s) => {
                buf.put_u8(TAG_STRING);
                write_blob(s.as_bytes(), buf);
            }
            TypedValue::Bytes(b) => {
                buf.put_u8(TAG_BYTES);
                write_blob(b, buf);
            }
            TypedValue::DateTime(millis) => {
                buf.put_u8(TAG_DATETIME);
                buf.put_i64_le(*millis);
            }
        }
    }

    fn read_from(cur: &mut &[u8]) -> Result<Self> {
        match take_u8(cur, "typed-value tag")? {
            TAG_NULL => Ok(TypedValue::Null),
            TAG_BOOL => Ok(TypedValue::Bool(take_bool(cur, "typed-value bool")?)),
            TAG_INT => Ok(TypedValue::Int(take_i64_le(cur, "typed-value int")?)),
            TAG_FLOAT => Ok(TypedValue::Float(take_f64_le(cur, "typed-value float")?)),
            TAG_STRING => {
                let bytes = read_blob(cur, "typed-value string")?;
                let s = String::from_utf8(bytes.to_vec())
                    .map_err(|e| Error::encoding(format!("invalid utf-8 in string value: {e}")))?;
                Ok(TypedValue::String(s))
            }
            TAG_BYTES => Ok(TypedValue::Bytes(Bytes::copy_from_slice(read_blob(
                cur,
                "typed-value bytes",
            )?))),
            TAG_DATETIME => Ok(TypedValue::DateTime(take_i64_le(
                cur,
                "typed-value datetime",
            )?)),
            other => Err(Error::encoding(format!(
                "invalid typed-value tag: 0x{other:02x}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// NodeRecord
// ---------------------------------------------------------------------------

/// The value at every `Node[uid]` key (RFC 0004 §"Node record — monolithic
/// blob"): the whole node as one value, so one `get` returns every property.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRecord {
    /// Sorted label ids. Sortedness is a caller invariant (write path); this
    /// type round-trips whatever order it is given.
    pub labels: Vec<LabelId>,
    /// `prop_id -> value`. A `BTreeMap` gives a canonical encode order.
    pub props: BTreeMap<PropId, TypedValue>,
    /// The external id, also indexed via `Xid[xid] -> uid`.
    pub xid: String,
}

/// Serialization strategy for [`NodeRecord`] (RFC 0004 §"Serialization sits
/// behind a `NodeCodec` trait" — a later codec, e.g. a zero-copy `rkyv`
/// fast-follow, can be swapped in without touching callers).
pub trait NodeCodec {
    fn encode(record: &NodeRecord) -> Result<Bytes>;
    fn decode(bytes: &[u8]) -> Result<NodeRecord>;
}

/// v0 default [`NodeCodec`]: hand-rolled, fail-closed, little-endian.
pub struct V0NodeCodec;

impl V0NodeCodec {
    /// Encodes `record`, rejecting it with `Error::Value` if the encoded size
    /// exceeds `cap` bytes (RFC 0004 §"Node size cap", `oversize_node`).
    pub fn encode_with_cap(record: &NodeRecord, cap: usize) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        encode_array_count(record.labels.len(), &mut buf);
        for label in &record.labels {
            buf.put_u32_le(label.get());
        }
        encode_array_count(record.props.len(), &mut buf);
        for (prop_id, value) in &record.props {
            buf.put_u32_le(prop_id.get());
            value.write_to(&mut buf);
        }
        write_blob(record.xid.as_bytes(), &mut buf);

        if buf.len() > cap {
            return Err(Error::value(format!(
                "oversize_node: encoded NodeRecord is {} bytes, cap is {cap} bytes",
                buf.len()
            )));
        }
        Ok(buf.freeze())
    }
}

impl NodeCodec for V0NodeCodec {
    fn encode(record: &NodeRecord) -> Result<Bytes> {
        Self::encode_with_cap(record, DEFAULT_NODE_SIZE_CAP)
    }

    fn decode(bytes: &[u8]) -> Result<NodeRecord> {
        let mut cur = bytes;
        let label_count = decode_array_count(&mut cur).map_err(Error::from)?;
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            labels.push(LabelId(take_u32_le(&mut cur, "node label id")?));
        }
        let prop_count = decode_array_count(&mut cur).map_err(Error::from)?;
        let mut props = BTreeMap::new();
        for _ in 0..prop_count {
            let prop_id = PropId(take_u32_le(&mut cur, "node prop id")?);
            let value = TypedValue::read_from(&mut cur)?;
            props.insert(prop_id, value);
        }
        let xid_bytes = read_blob(&mut cur, "node xid")?;
        let xid = String::from_utf8(xid_bytes.to_vec())
            .map_err(|e| Error::encoding(format!("invalid utf-8 in xid: {e}")))?;

        if !cur.is_empty() {
            return Err(Error::encoding(format!(
                "trailing bytes after node record: {} extra",
                cur.len()
            )));
        }
        Ok(NodeRecord { labels, props, xid })
    }
}

// ---------------------------------------------------------------------------
// EdgeProps (companion record for valued/faceted edges — RFC 0005
// §"Edge facets", as amended: one copy per edge, not per-projection)
// ---------------------------------------------------------------------------

/// The value at every [`crate::serde::keys::edge_prop_key`] — an edge's
/// properties, keyed by `prop_id` the same way [`NodeRecord::props`] is. A
/// `BTreeMap` gives a canonical encode order; this lives in `value.rs` (not
/// `serde::keys`) because it reuses [`TypedValue::write_to`]/`read_from`,
/// which are module-private.
pub type EdgeProps = BTreeMap<PropId, TypedValue>;

/// Encodes `props` exactly the way [`V0NodeCodec::encode_with_cap`] encodes a
/// node's `props` field: an [`encode_array_count`]-prefixed sequence of
/// `(prop_id: u32 LE, value: TypedValue)` pairs. No node-size cap applies
/// here — the companion record is its own, separate key.
pub fn encode_edge_props(props: &EdgeProps) -> Bytes {
    let mut buf = BytesMut::new();
    encode_array_count(props.len(), &mut buf);
    for (prop_id, value) in props {
        buf.put_u32_le(prop_id.get());
        value.write_to(&mut buf);
    }
    buf.freeze()
}

/// Decodes [`encode_edge_props`]'s output — the exact inverse. Fail-closed:
/// truncation, an unknown `TypedValue` tag, or trailing bytes are all `Err`,
/// never a panic (mirrors [`V0NodeCodec::decode`]'s trailing-bytes check).
pub fn decode_edge_props(bytes: &[u8]) -> Result<EdgeProps> {
    let mut cur = bytes;
    let count = decode_array_count(&mut cur).map_err(Error::from)?;
    let mut props = BTreeMap::new();
    for _ in 0..count {
        let prop_id = PropId(take_u32_le(&mut cur, "edge-prop prop id")?);
        let value = TypedValue::read_from(&mut cur)?;
        props.insert(prop_id, value);
    }
    if !cur.is_empty() {
        return Err(Error::encoding(format!(
            "trailing bytes after edge props: {} extra",
            cur.len()
        )));
    }
    Ok(props)
}

// ---------------------------------------------------------------------------
// ChangeRecord
// ---------------------------------------------------------------------------

const OP_UPSERT_NODE: u8 = 0;
const OP_UPSERT_EDGE: u8 = 1;
const OP_DELETE_NODE: u8 = 2;
const OP_DELETE_EDGE: u8 = 3;

/// The kind of write a [`ChangeRecord`] describes (RFC 0004 §"Write path").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    UpsertNode,
    UpsertEdge,
    DeleteNode,
    DeleteEdge,
}

impl ChangeOp {
    fn as_byte(self) -> u8 {
        match self {
            ChangeOp::UpsertNode => OP_UPSERT_NODE,
            ChangeOp::UpsertEdge => OP_UPSERT_EDGE,
            ChangeOp::DeleteNode => OP_DELETE_NODE,
            ChangeOp::DeleteEdge => OP_DELETE_EDGE,
        }
    }

    fn from_byte(b: u8) -> Result<Self> {
        match b {
            OP_UPSERT_NODE => Ok(ChangeOp::UpsertNode),
            OP_UPSERT_EDGE => Ok(ChangeOp::UpsertEdge),
            OP_DELETE_NODE => Ok(ChangeOp::DeleteNode),
            OP_DELETE_EDGE => Ok(ChangeOp::DeleteEdge),
            other => Err(Error::encoding(format!(
                "invalid change-record op byte: 0x{other:02x}"
            ))),
        }
    }
}

/// Labels added/removed by an `UpsertNode` (RFC 0004 §"ChangeRecord schema"'s
/// `label_delta`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LabelDelta {
    pub added: Vec<LabelId>,
    pub removed: Vec<LabelId>,
}

/// The value at every `Log[seq]` key (RFC 0004 §"ChangeRecord schema") — the
/// changelog record a lagging reader tail-scans to re-evaluate a pattern, or
/// an index backfill replays deterministically.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeRecord {
    pub seq: u64,
    pub op: ChangeOp,
    pub subject_uid: Uid,
    pub pred_id: Option<PredId>,
    /// The edge target, for edge ops.
    pub object_uid: Option<Uid>,
    /// A scalar property's before/after value, for node ops.
    pub value: Option<TypedValue>,
    pub label_delta: Option<LabelDelta>,
}

impl ChangeRecord {
    /// Serializes to the on-wire form (values are LE).
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.seq);
        buf.put_u8(self.op.as_byte());
        buf.put_u64_le(self.subject_uid.get());

        write_option(&mut buf, self.pred_id, |buf, pred| {
            buf.put_u32_le(pred.get())
        });
        write_option(&mut buf, self.object_uid, |buf, uid| {
            buf.put_u64_le(uid.get())
        });
        write_option(&mut buf, self.value.as_ref(), |buf, v| v.write_to(buf));
        write_option(&mut buf, self.label_delta.as_ref(), |buf, delta| {
            write_label_ids(&delta.added, buf);
            write_label_ids(&delta.removed, buf);
        });

        buf.freeze()
    }

    /// Deserializes from the on-wire form — the exact inverse of
    /// [`ChangeRecord::encode`]. Fail-closed: truncation, an unknown `op` or
    /// tag byte, or trailing bytes are all `Err`, never a panic.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cur = bytes;
        let seq = take_u64_le(&mut cur, "change-record seq")?;
        let op = ChangeOp::from_byte(take_u8(&mut cur, "change-record op")?)?;
        let subject_uid = Uid(take_u64_le(&mut cur, "change-record subject_uid")?);

        let pred_id = read_option(&mut cur, "change-record pred_id", |cur| {
            Ok(PredId(take_u32_le(cur, "change-record pred_id")?))
        })?;
        let object_uid = read_option(&mut cur, "change-record object_uid", |cur| {
            Ok(Uid(take_u64_le(cur, "change-record object_uid")?))
        })?;
        let value = read_option(&mut cur, "change-record value", TypedValue::read_from)?;
        let label_delta = read_option(&mut cur, "change-record label_delta", |cur| {
            Ok(LabelDelta {
                added: read_label_ids(cur, "change-record label_delta.added")?,
                removed: read_label_ids(cur, "change-record label_delta.removed")?,
            })
        })?;

        if !cur.is_empty() {
            return Err(Error::encoding(format!(
                "trailing bytes after change record: {} extra",
                cur.len()
            )));
        }
        Ok(ChangeRecord {
            seq,
            op,
            subject_uid,
            pred_id,
            object_uid,
            value,
            label_delta,
        })
    }
}

fn write_label_ids(ids: &[LabelId], buf: &mut BytesMut) {
    encode_array_count(ids.len(), buf);
    for id in ids {
        buf.put_u32_le(id.get());
    }
}

fn read_label_ids(cur: &mut &[u8], what: &str) -> Result<Vec<LabelId>> {
    let count = decode_array_count(cur).map_err(Error::from)?;
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(LabelId(take_u32_le(cur, what)?));
    }
    Ok(ids)
}

// ---------------------------------------------------------------------------
// Shared byte-level helpers (values are LE; a length-prefixed "blob" is a
// u32-LE length + raw bytes — unlike `common::serde::encoding::encode_utf8`,
// which caps at u16::MAX and panics past it, property values and xids are not
// guaranteed to stay under 64 KiB while still being under the 1 MiB node cap).
// ---------------------------------------------------------------------------

fn write_blob(bytes: &[u8], buf: &mut BytesMut) {
    buf.put_u32_le(bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

fn read_blob<'a>(cur: &mut &'a [u8], what: &str) -> Result<&'a [u8]> {
    let len = take_u32_le(cur, &format!("{what} length"))? as usize;
    if cur.len() < len {
        return Err(Error::encoding(format!(
            "truncated {what}: need {len} bytes, got {}",
            cur.len()
        )));
    }
    let (blob, rest) = cur.split_at(len);
    *cur = rest;
    Ok(blob)
}

fn write_option<T>(buf: &mut BytesMut, opt: Option<T>, write: impl FnOnce(&mut BytesMut, T)) {
    match opt {
        Some(v) => {
            buf.put_u8(1);
            write(buf, v);
        }
        None => buf.put_u8(0),
    }
}

fn read_option<T>(
    cur: &mut &[u8],
    what: &str,
    read: impl FnOnce(&mut &[u8]) -> Result<T>,
) -> Result<Option<T>> {
    match take_bool(cur, &format!("{what} presence flag"))? {
        true => Ok(Some(read(cur)?)),
        false => Ok(None),
    }
}

fn take_u8(cur: &mut &[u8], what: &str) -> Result<u8> {
    if cur.is_empty() {
        return Err(Error::encoding(format!("truncated {what}: need 1 byte")));
    }
    let b = cur[0];
    *cur = &cur[1..];
    Ok(b)
}

fn take_bool(cur: &mut &[u8], what: &str) -> Result<bool> {
    match take_u8(cur, what)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::encoding(format!(
            "invalid boolean byte for {what}: 0x{other:02x}"
        ))),
    }
}

fn take_u32_le(cur: &mut &[u8], what: &str) -> Result<u32> {
    if cur.len() < 4 {
        return Err(Error::encoding(format!(
            "truncated {what}: need 4 bytes, got {}",
            cur.len()
        )));
    }
    let v = u32::from_le_bytes(cur[..4].try_into().expect("checked len"));
    *cur = &cur[4..];
    Ok(v)
}

fn take_u64_le(cur: &mut &[u8], what: &str) -> Result<u64> {
    if cur.len() < 8 {
        return Err(Error::encoding(format!(
            "truncated {what}: need 8 bytes, got {}",
            cur.len()
        )));
    }
    let v = u64::from_le_bytes(cur[..8].try_into().expect("checked len"));
    *cur = &cur[8..];
    Ok(v)
}

fn take_i64_le(cur: &mut &[u8], what: &str) -> Result<i64> {
    take_u64_le(cur, what).map(|v| v as i64)
}

fn take_f64_le(cur: &mut &[u8], what: &str) -> Result<f64> {
    take_u64_le(cur, what).map(f64::from_bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v0_roundtrip(record: &NodeRecord) {
        let bytes = V0NodeCodec::encode(record).unwrap();
        assert_eq!(V0NodeCodec::decode(&bytes).unwrap(), *record);
    }

    #[test]
    fn should_roundtrip_typed_value_every_variant() {
        let values = [
            TypedValue::Null,
            TypedValue::Bool(true),
            TypedValue::Bool(false),
            TypedValue::Int(i64::MIN),
            TypedValue::Int(i64::MAX),
            TypedValue::Float(1.5),
            TypedValue::Float(-0.0),
            TypedValue::String(String::new()),
            TypedValue::String("hello, 世界!".to_string()),
            TypedValue::Bytes(Bytes::from_static(b"\x00\x01\xff")),
            TypedValue::DateTime(1_700_000_000_000),
        ];
        for v in values {
            let mut buf = BytesMut::new();
            v.write_to(&mut buf);
            let mut cur = &buf[..];
            let back = TypedValue::read_from(&mut cur).unwrap();
            assert_eq!(back, v);
            assert!(cur.is_empty());
        }
    }

    #[test]
    fn should_roundtrip_node_record_with_labels_props_and_xid() {
        let mut props = BTreeMap::new();
        props.insert(PropId(1), TypedValue::String("Alice".to_string()));
        props.insert(PropId(2), TypedValue::Int(30));
        let record = NodeRecord {
            labels: vec![LabelId(5), LabelId(9)],
            props,
            xid: "user:alice".to_string(),
        };
        v0_roundtrip(&record);
    }

    #[test]
    fn should_roundtrip_node_record_with_no_labels_or_props() {
        let record = NodeRecord {
            labels: Vec::new(),
            props: BTreeMap::new(),
            xid: String::new(),
        };
        v0_roundtrip(&record);
    }

    #[test]
    fn should_reject_oversize_node() {
        let mut props = BTreeMap::new();
        props.insert(PropId(1), TypedValue::Bytes(Bytes::from(vec![0u8; 100])));
        let record = NodeRecord {
            labels: Vec::new(),
            props,
            xid: String::new(),
        };
        assert!(V0NodeCodec::encode_with_cap(&record, 10).is_err());
        assert!(V0NodeCodec::encode_with_cap(&record, 10_000).is_ok());
    }

    #[test]
    fn should_reject_truncated_and_trailing_node_record_bytes() {
        let mut props = BTreeMap::new();
        props.insert(PropId(1), TypedValue::Int(7));
        let record = NodeRecord {
            labels: vec![LabelId(1)],
            props,
            xid: "x".to_string(),
        };
        let bytes = V0NodeCodec::encode(&record).unwrap();

        for cut in 0..bytes.len() {
            assert!(V0NodeCodec::decode(&bytes[..cut]).is_err());
        }
        let mut extended = bytes.to_vec();
        extended.push(0xAB);
        assert!(V0NodeCodec::decode(&extended).is_err());
    }

    #[test]
    fn should_roundtrip_edge_props_empty_and_populated() {
        let empty: EdgeProps = BTreeMap::new();
        assert_eq!(
            decode_edge_props(&encode_edge_props(&empty)).unwrap(),
            empty
        );

        let mut props: EdgeProps = BTreeMap::new();
        props.insert(PropId(1), TypedValue::String("blue".to_string()));
        props.insert(PropId(2), TypedValue::Int(42));
        props.insert(PropId(3), TypedValue::Null);
        assert_eq!(
            decode_edge_props(&encode_edge_props(&props)).unwrap(),
            props
        );
    }

    #[test]
    fn should_reject_truncated_and_trailing_edge_props_bytes() {
        let mut props: EdgeProps = BTreeMap::new();
        props.insert(PropId(1), TypedValue::Bool(true));
        props.insert(PropId(9), TypedValue::Float(1.5));
        let bytes = encode_edge_props(&props);

        for cut in 0..bytes.len() {
            assert!(decode_edge_props(&bytes[..cut]).is_err());
        }
        let mut extended = bytes.to_vec();
        extended.push(0xAB);
        assert!(decode_edge_props(&extended).is_err());
    }

    #[test]
    fn should_roundtrip_change_record_upsert_node_with_label_delta() {
        let record = ChangeRecord {
            seq: 42,
            op: ChangeOp::UpsertNode,
            subject_uid: Uid(1),
            pred_id: None,
            object_uid: None,
            value: Some(TypedValue::Int(9)),
            label_delta: Some(LabelDelta {
                added: vec![LabelId(1), LabelId(2)],
                removed: vec![LabelId(3)],
            }),
        };
        let bytes = record.encode();
        assert_eq!(ChangeRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn should_roundtrip_change_record_upsert_edge() {
        let record = ChangeRecord {
            seq: 7,
            op: ChangeOp::UpsertEdge,
            subject_uid: Uid(10),
            pred_id: Some(PredId(3)),
            object_uid: Some(Uid(20)),
            value: None,
            label_delta: None,
        };
        let bytes = record.encode();
        assert_eq!(ChangeRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn should_roundtrip_change_record_delete_ops() {
        for op in [ChangeOp::DeleteNode, ChangeOp::DeleteEdge] {
            let record = ChangeRecord {
                seq: 1,
                op,
                subject_uid: Uid(1),
                pred_id: None,
                object_uid: None,
                value: None,
                label_delta: None,
            };
            let bytes = record.encode();
            assert_eq!(ChangeRecord::decode(&bytes).unwrap(), record);
        }
    }

    #[test]
    fn should_reject_unknown_change_record_op_byte() {
        let mut buf = BytesMut::new();
        buf.put_u64_le(1);
        buf.put_u8(0xFF);
        assert!(ChangeRecord::decode(&buf).is_err());
    }

    #[test]
    fn should_reject_truncated_and_trailing_change_record_bytes() {
        let record = ChangeRecord {
            seq: 1,
            op: ChangeOp::UpsertEdge,
            subject_uid: Uid(1),
            pred_id: Some(PredId(1)),
            object_uid: Some(Uid(2)),
            value: Some(TypedValue::Bool(true)),
            label_delta: Some(LabelDelta::default()),
        };
        let bytes = record.encode();

        for cut in 0..bytes.len() {
            assert!(ChangeRecord::decode(&bytes[..cut]).is_err());
        }
        let mut extended = bytes.to_vec();
        extended.push(0xAB);
        assert!(ChangeRecord::decode(&extended).is_err());
    }

    #[test]
    fn should_not_panic_on_arbitrary_adversarial_bytes() {
        let adversarial: &[&[u8]] = &[
            &[],
            &[0x00],
            &[0xff; 20],
            &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF],
        ];
        for bytes in adversarial {
            let _ = V0NodeCodec::decode(bytes);
            let _ = ChangeRecord::decode(bytes);
        }
    }
}
