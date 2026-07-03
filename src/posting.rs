//! The posting-list value type (RFC 0005): the value stored at every
//! adjacency (`EdgeOut`/`EdgeIn`) and index (`Index`/`Count`) key.
//!
//! A [`PostingValue`] is either the whole compressed sorted set inline
//! ([`PostingKind::Single`]) or a manifest of split parts
//! ([`PostingKind::Split`]) whose members live at `EdgePart[..][start_uid]`
//! keys. This module owns the type, its on-wire codec, and the read-side
//! set-algebra surface (RFC 0005 §"Posting value format", §"Set algebra").
//! It does **not** implement the merge-fill (M1 D3), add/delete/split/neighbors
//! (M1 D4), or the write path (M1 D5) — see
//! `docs/impl/2026-07-03-m1-d1-posting-value-handoff.md`.
//!
//! House rule (opendata): keys are big-endian, **values are little-endian** —
//! every fixed-width integer in the on-wire format below is LE.

use bytes::{BufMut, Bytes, BytesMut};
use roaring::RoaringTreemap;

use crate::{Error, Result};

/// Format tag byte. `1` = roaring-v1, the only value written in v0.
/// Reserved: `2` = UidPack, `3` = CSR (RFC 0009) — the CSR-ready seam.
pub const FORMAT_ROARING_V1: u8 = 1;

const KIND_SINGLE: u8 = 0;
const KIND_SPLIT: u8 = 1;

/// On-wire width of one [`PartRef`]: `start_uid | min_uid | max_uid` (u64 LE
/// each) + `card` (u32 LE) = 8 + 8 + 8 + 4.
const PART_REF_LEN: usize = 28;

/// A skip-metadata entry for one part of a split (supernode) posting list.
///
/// `min`/`max`/`card` come from roaring in O(1); a ranged reader skips parts
/// whose `[min, max]` doesn't overlap its query range without decoding them
/// (RFC 0005 §"Splitting supernodes").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartRef {
    /// The first uid held by the part — reconstructs the part key via
    /// `edge_part_key(dir, anchor, pred, Uid(start_uid))`.
    pub start_uid: u64,
    /// The part's minimum member uid (`RoaringTreemap::min`).
    pub min_uid: u64,
    /// The part's maximum member uid (`RoaringTreemap::max`).
    pub max_uid: u64,
    /// The part's cardinality (`RoaringTreemap::len`).
    pub card: u32,
}

impl PartRef {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.start_uid);
        buf.put_u64_le(self.min_uid);
        buf.put_u64_le(self.max_uid);
        buf.put_u32_le(self.card);
    }

    fn read_from(bytes: &[u8]) -> Self {
        debug_assert_eq!(bytes.len(), PART_REF_LEN);
        let start_uid = u64::from_le_bytes(bytes[0..8].try_into().expect("checked len"));
        let min_uid = u64::from_le_bytes(bytes[8..16].try_into().expect("checked len"));
        let max_uid = u64::from_le_bytes(bytes[16..24].try_into().expect("checked len"));
        let card = u32::from_le_bytes(bytes[24..28].try_into().expect("checked len"));
        PartRef {
            start_uid,
            min_uid,
            max_uid,
            card,
        }
    }
}

/// The shape of a posting value's members.
#[derive(Debug, Clone, PartialEq)]
pub enum PostingKind {
    /// The whole set lives inline in this value.
    Single(RoaringTreemap),
    /// This value is a manifest; the members live in `EdgePart[..][start_uid]`
    /// keys named by each [`PartRef`].
    Split(Vec<PartRef>),
}

/// The value stored at every adjacency/index/count key (RFC 0005).
///
/// An empty `Single` set (`PostingValue::empty()`) is the canonical "no
/// members" value, as distinct from an absent key — whether an empty posting
/// is ever actually stored or is instead deleted is a later deliverable's
/// (D4/D5) call; this type just round-trips it either way.
#[derive(Debug, Clone, PartialEq)]
pub struct PostingValue {
    /// Always [`FORMAT_ROARING_V1`] in v0; unknown values are rejected on
    /// decode (the CSR-ready seam, RFC 0009).
    pub format: u8,
    pub kind: PostingKind,
}

impl PostingValue {
    /// Builds a `Single` value wrapping `set`.
    pub fn single(set: RoaringTreemap) -> Self {
        PostingValue {
            format: FORMAT_ROARING_V1,
            kind: PostingKind::Single(set),
        }
    }

    /// The canonical empty posting: `Single(∅)`.
    pub fn empty() -> Self {
        PostingValue::single(RoaringTreemap::new())
    }

    /// Builds a `Split` manifest value.
    pub fn split(parts: Vec<PartRef>) -> Self {
        PostingValue {
            format: FORMAT_ROARING_V1,
            kind: PostingKind::Split(parts),
        }
    }

    /// `true` iff this value is a split manifest.
    pub fn is_split(&self) -> bool {
        matches!(self.kind, PostingKind::Split(_))
    }

    /// The manifest's parts, if this value is a `Split`.
    pub fn parts(&self) -> Option<&[PartRef]> {
        match &self.kind {
            PostingKind::Split(parts) => Some(parts),
            PostingKind::Single(_) => None,
        }
    }

    /// The inline set, if this value is a `Single`.
    pub fn materialize_single(&self) -> Option<&RoaringTreemap> {
        match &self.kind {
            PostingKind::Single(set) => Some(set),
            PostingKind::Split(_) => None,
        }
    }

    /// Unions the given part sets into the whole-set read of a `Split` value.
    /// Order-independent. `parts` is supplied by the caller (D4 does the
    /// per-part storage `get`s); this just folds them.
    pub fn union_parts(parts: &[RoaringTreemap]) -> RoaringTreemap {
        let mut out = RoaringTreemap::new();
        for part in parts {
            out |= part;
        }
        out
    }

    /// Serializes to the on-wire format (RFC 0005; values are LE).
    ///
    /// ```text
    /// byte 0      : format  (u8)
    /// byte 1      : kind    (u8)   0 = Single, 1 = Split
    /// Single body : bytes[2..]     RoaringTreemap portable bytes
    /// Split body  : bytes[2..6]    part_count (u32 LE)
    ///               then part_count * 28-byte PartRefs
    /// ```
    pub fn serialize(&self) -> Bytes {
        match &self.kind {
            PostingKind::Single(set) => {
                let mut buf = BytesMut::with_capacity(2 + set.serialized_size());
                buf.put_u8(self.format);
                buf.put_u8(KIND_SINGLE);
                let mut writer = buf.writer();
                set.serialize_into(&mut writer)
                    .expect("serializing into a Vec/BytesMut cannot fail");
                writer.into_inner().freeze()
            }
            PostingKind::Split(parts) => {
                let mut buf = BytesMut::with_capacity(2 + 4 + parts.len() * PART_REF_LEN);
                buf.put_u8(self.format);
                buf.put_u8(KIND_SPLIT);
                buf.put_u32_le(parts.len() as u32);
                for part in parts {
                    part.write_to(&mut buf);
                }
                buf.freeze()
            }
        }
    }

    /// The exact on-wire length of [`Self::serialize`] — the 512 KiB
    /// split-threshold trigger input (D4), so it must reflect the real
    /// serialization, not an estimate.
    pub fn serialized_len(&self) -> usize {
        match &self.kind {
            PostingKind::Single(set) => 2 + set.serialized_size(),
            PostingKind::Split(parts) => 2 + 4 + parts.len() * PART_REF_LEN,
        }
    }

    /// Deserializes from the on-wire format. Fail-closed: unknown `format`,
    /// an invalid `kind` byte, or a truncated body all return `Err`, never
    /// panic.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 2 {
            return Err(Error::encoding(format!(
                "truncated posting value header: need 2 bytes, got {}",
                bytes.len()
            )));
        }
        let format = bytes[0];
        if format != FORMAT_ROARING_V1 {
            return Err(Error::encoding(format!(
                "unknown posting value format: {format}"
            )));
        }
        let kind_byte = bytes[1];
        let body = &bytes[2..];
        let kind = match kind_byte {
            KIND_SINGLE => {
                let set = RoaringTreemap::deserialize_from(body)
                    .map_err(|e| Error::encoding(format!("bad Single posting body: {e}")))?;
                PostingKind::Single(set)
            }
            KIND_SPLIT => {
                if body.len() < 4 {
                    return Err(Error::encoding(format!(
                        "truncated split part_count: need 4 bytes, got {}",
                        body.len()
                    )));
                }
                let part_count =
                    u32::from_le_bytes(body[0..4].try_into().expect("checked len")) as usize;
                let rest = &body[4..];
                let expected_len = part_count * PART_REF_LEN;
                if rest.len() != expected_len {
                    return Err(Error::encoding(format!(
                        "truncated split manifest: expected {expected_len} bytes for {part_count} parts, got {}",
                        rest.len()
                    )));
                }
                let mut parts = Vec::with_capacity(part_count);
                for chunk in rest.chunks_exact(PART_REF_LEN) {
                    parts.push(PartRef::read_from(chunk));
                }
                PostingKind::Split(parts)
            }
            other => {
                return Err(Error::encoding(format!(
                    "invalid posting kind byte: 0x{other:02x}"
                )));
            }
        };
        Ok(PostingValue { format, kind })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_of(vals: &[u64]) -> RoaringTreemap {
        vals.iter().copied().collect()
    }

    #[test]
    fn should_roundtrip_empty_single() {
        let v = PostingValue::empty();
        let bytes = v.serialize();
        assert_eq!(PostingValue::deserialize(&bytes).unwrap(), v);
    }

    #[test]
    fn should_roundtrip_single_across_32bit_boundary() {
        let set = set_of(&[0, 1, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX]);
        let v = PostingValue::single(set);
        let bytes = v.serialize();
        assert_eq!(PostingValue::deserialize(&bytes).unwrap(), v);
    }

    #[test]
    fn should_roundtrip_split_manifest() {
        let parts = vec![
            PartRef {
                start_uid: 0,
                min_uid: 0,
                max_uid: 99,
                card: 100,
            },
            PartRef {
                start_uid: 100,
                min_uid: 100,
                max_uid: 199,
                card: 100,
            },
        ];
        let v = PostingValue::split(parts);
        let bytes = v.serialize();
        assert_eq!(PostingValue::deserialize(&bytes).unwrap(), v);
    }

    #[test]
    fn should_roundtrip_empty_split_manifest() {
        let v = PostingValue::split(Vec::new());
        let bytes = v.serialize();
        assert_eq!(PostingValue::deserialize(&bytes).unwrap(), v);
    }

    #[test]
    fn should_report_serialized_len_matching_actual_serialization() {
        let v = PostingValue::single(set_of(&[1, 2, 3]));
        assert_eq!(v.serialized_len(), v.serialize().len());

        let v = PostingValue::split(vec![PartRef {
            start_uid: 1,
            min_uid: 1,
            max_uid: 2,
            card: 2,
        }]);
        assert_eq!(v.serialized_len(), v.serialize().len());
    }

    #[test]
    fn should_report_shape_via_is_split_and_parts() {
        let single = PostingValue::single(set_of(&[1]));
        assert!(!single.is_split());
        assert!(single.parts().is_none());
        assert!(single.materialize_single().is_some());

        let split = PostingValue::split(vec![PartRef {
            start_uid: 0,
            min_uid: 0,
            max_uid: 0,
            card: 1,
        }]);
        assert!(split.is_split());
        assert_eq!(split.parts().unwrap().len(), 1);
        assert!(split.materialize_single().is_none());
    }

    #[test]
    fn should_union_parts_order_independently() {
        let a = set_of(&[1, 3, 5]);
        let b = set_of(&[2, 4, 6]);
        let union_ab = PostingValue::union_parts(&[a.clone(), b.clone()]);
        let union_ba = PostingValue::union_parts(&[b, a]);
        assert_eq!(union_ab, union_ba);
        assert_eq!(union_ab, set_of(&[1, 2, 3, 4, 5, 6]));
    }

    #[test]
    fn should_reject_unknown_format_byte() {
        let bytes = [0xffu8, KIND_SINGLE];
        assert!(PostingValue::deserialize(&bytes).is_err());
    }

    #[test]
    fn should_reject_invalid_kind_byte() {
        let bytes = [FORMAT_ROARING_V1, 0x02];
        assert!(PostingValue::deserialize(&bytes).is_err());
    }

    #[test]
    fn should_reject_truncated_header() {
        assert!(PostingValue::deserialize(&[]).is_err());
        assert!(PostingValue::deserialize(&[FORMAT_ROARING_V1]).is_err());
    }

    #[test]
    fn should_reject_truncated_single_body() {
        // A valid header but garbage/truncated roaring body.
        let bytes = [FORMAT_ROARING_V1, KIND_SINGLE, 0x01, 0x02];
        assert!(PostingValue::deserialize(&bytes).is_err());
    }

    #[test]
    fn should_reject_truncated_split_part_count() {
        let bytes = [FORMAT_ROARING_V1, KIND_SPLIT, 0x00, 0x00];
        assert!(PostingValue::deserialize(&bytes).is_err());
    }

    #[test]
    fn should_reject_split_manifest_with_wrong_body_length() {
        // Claims 2 parts (56 bytes) but only supplies 10.
        let mut bytes = vec![FORMAT_ROARING_V1, KIND_SPLIT];
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 10]);
        assert!(PostingValue::deserialize(&bytes).is_err());
    }

    #[test]
    fn should_not_panic_on_random_adversarial_bytes() {
        // Fail-closed, no panics — mirror the M0 keyspace parser posture.
        let adversarial: &[&[u8]] = &[
            &[],
            &[0x00],
            &[0x01, 0x01, 0xff, 0xff, 0xff, 0xff],
            &[0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            &[0xaa; 100],
        ];
        for bytes in adversarial {
            let _ = PostingValue::deserialize(bytes);
        }
    }
}
