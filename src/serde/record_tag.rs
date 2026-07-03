//! Graph record types — the 1-byte tag that follows the [`KeyPrefix`] and
//! discriminates every record in the keyspace (RFC 0003).
//!
//! The tag is `common`'s [`RecordTag`]: the record type in the **high nibble**,
//! the low nibble reserved (always `0x0` in v0). Reserving a whole tag byte
//! (rather than folding the type into the version) keeps record types cheap to
//! add and keeps SlateDB's prefix-bloom effective per record type.
//!
//! [`KeyPrefix`]: common::serde::key_prefix::KeyPrefix
//! [`RecordTag`]: common::serde::record_tag::RecordTag

use common::serde::record_tag::RecordTag;

use crate::{Error, Result};

/// Every graph record type, tagged by its high-nibble discriminant (RFC 0003).
///
/// | Tag | Variant | Key tail | Value |
/// |-----|---------|----------|-------|
/// | `0x1` | [`SchemaName`](RecordType::SchemaName) | `[kind:1][name: terminated]` | `id: u32` BE |
/// | `0x2` | [`SchemaId`](RecordType::SchemaId) | `[kind:1][id: u32 BE]` | `SchemaEntry` |
/// | `0x3` | [`Node`](RecordType::Node) | `[uid: u64 BE]` | `NodeRecord` |
/// | `0x4` | [`EdgeOut`](RecordType::EdgeOut) | `[src: u64 BE][pred: u32 BE]` | posting list |
/// | `0x5` | [`EdgeIn`](RecordType::EdgeIn) | `[dst: u64 BE][pred: u32 BE]` | posting list |
/// | `0x6` | [`EdgePart`](RecordType::EdgePart) | `[dir:1][anchor:8][pred:4][start:8]` | posting part |
/// | `0x7` | [`Index`](RecordType::Index) | `[pred: u32 BE][token: order-preserving]` | posting list |
/// | `0x8` | [`Count`](RecordType::Count) | `[pred: u32 BE][dir:1][degree: u32 BE]` | posting list |
/// | `0x9` | [`Xid`](RecordType::Xid) | `[xid: terminated]` | `uid: u64` BE |
/// | `0xA` | [`Log`](RecordType::Log) | `[seq: u64 BE]` | `ChangeRecord` |
/// | `0xB` | [`Meta`](RecordType::Meta) | `[meta_key: bytes]` | scalar / bitmap / counter |
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RecordType {
    /// `name -> u32 id` interning lookup (write path).
    SchemaName = 0x1,
    /// `u32 id -> SchemaEntry` reverse lookup (read/return path).
    SchemaId = 0x2,
    /// A node record: label-id set + property blob.
    Node = 0x3,
    /// Forward adjacency posting list, keyed by source uid.
    EdgeOut = 0x4,
    /// Reverse adjacency posting list, keyed by destination uid.
    EdgeIn = 0x5,
    /// A split part of an oversized adjacency posting list.
    EdgePart = 0x6,
    /// A value/eq (or range) index posting list.
    Index = 0x7,
    /// A count (degree) index posting list.
    Count = 0x8,
    /// External-id (`xid`) → uid mapping.
    Xid = 0x9,
    /// The per-namespace changelog stream.
    Log = 0xA,
    /// Namespace metadata: `latest_seq`, seq-block reservations, counters, bitmaps.
    Meta = 0xB,
}

impl RecordType {
    /// The high-nibble discriminant (1..=15).
    #[inline]
    pub fn nibble(self) -> u8 {
        self as u8
    }

    /// The full tag byte (`nibble` in the high nibble, reserved `0x0` low).
    #[inline]
    pub fn tag_byte(self) -> u8 {
        RecordTag::new(self.nibble(), 0).as_byte()
    }

    /// Decodes a record type from its high-nibble discriminant.
    #[inline]
    pub fn from_nibble(nibble: u8) -> Result<Self> {
        Ok(match nibble {
            0x1 => RecordType::SchemaName,
            0x2 => RecordType::SchemaId,
            0x3 => RecordType::Node,
            0x4 => RecordType::EdgeOut,
            0x5 => RecordType::EdgeIn,
            0x6 => RecordType::EdgePart,
            0x7 => RecordType::Index,
            0x8 => RecordType::Count,
            0x9 => RecordType::Xid,
            0xA => RecordType::Log,
            0xB => RecordType::Meta,
            other => {
                return Err(Error::encoding(format!(
                    "unknown record type nibble: 0x{other:x}"
                )));
            }
        })
    }

    /// Decodes a record type from a full tag byte, validating the reserved
    /// low nibble is `0x0`.
    #[inline]
    pub fn from_tag_byte(byte: u8) -> Result<Self> {
        let tag = RecordTag::from_byte(byte).map_err(Error::from)?;
        if tag.reserved() != 0 {
            return Err(Error::encoding(format!(
                "graph record tag reserved bits must be 0, got 0x{:x}",
                tag.reserved()
            )));
        }
        Self::from_nibble(tag.record_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: &[RecordType] = &[
        RecordType::SchemaName,
        RecordType::SchemaId,
        RecordType::Node,
        RecordType::EdgeOut,
        RecordType::EdgeIn,
        RecordType::EdgePart,
        RecordType::Index,
        RecordType::Count,
        RecordType::Xid,
        RecordType::Log,
        RecordType::Meta,
    ];

    #[test]
    fn should_roundtrip_every_record_type_through_tag_byte() {
        // given / when / then — tag_byte and from_tag_byte are exact inverses
        for &rt in ALL {
            let byte = rt.tag_byte();
            assert_eq!(RecordType::from_tag_byte(byte).unwrap(), rt);
            // reserved low nibble is always zero in v0
            assert_eq!(byte & 0x0F, 0);
            assert_eq!(byte >> 4, rt.nibble());
        }
    }

    #[test]
    fn should_reject_unknown_nibble() {
        // given — 0xC..=0xF are unassigned
        for nibble in 0xC..=0xF {
            assert!(RecordType::from_nibble(nibble).is_err());
        }
    }

    #[test]
    fn should_reject_nonzero_reserved_bits() {
        // given — Node (0x3) with reserved bits set
        let byte = 0x35;

        // when / then
        assert!(RecordType::from_tag_byte(byte).is_err());
    }
}
