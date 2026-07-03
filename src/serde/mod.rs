//! The turbolay graph keyspace (RFC 0003).
//!
//! SlateDB is a plain ordered-byte-key store with **no custom comparators** —
//! key order is unsigned lexicographic byte order, full stop. Every logical
//! ordering turbolay needs (UID order for posting-list intersection, numeric
//! order for range predicates, sequence order for the changelog tail, prefix
//! clustering per predicate) is therefore baked into the *bytes* of the key.
//!
//! Every key is:
//!
//! ```text
//! | KeyPrefix (2B) | RecordTag (1B) | order-preserving tail... |
//!   ^ subsystem=0x05                  ^ record-type-specific
//!     version=0x01
//! ```
//!
//! This module holds the shared keyspace primitives: the subsystem/version
//! constants, the dense-id newtypes ([`Uid`], [`PredId`], [`LabelId`],
//! [`PropId`]), and the small enums ([`Direction`], [`SchemaKind`]) that appear
//! as fixed-width key components. The record-type tags live in
//! [`record_tag`]; the key builders/parsers in [`keys`]; the order-preserving
//! index-token encodings in [`token`].
//!
//! House rule (opendata): **keys are big-endian, values are little-endian.**

pub mod keys;
pub mod record_tag;
pub mod token;

pub use record_tag::RecordType;

/// Subsystem byte for the graph engine (RFC 0003).
///
/// This is the reserved `GRAPH = 0x05` slot in opendata's subsystem registry
/// (0x01 timeseries, 0x02 vector, 0x03 log, 0x04 keyvalue, **0x05 graph**).
/// It is defined locally here rather than in `common::serde::subsystem` so
/// turbolay does not have to mutate the shared crate; upstreaming the
/// registration is a follow-up.
pub const SUBSYSTEM: u8 = 0x05;

/// Key format version for the graph keyspace (RFC 0003).
pub const KEY_VERSION: u8 = 0x01;

/// The 3-byte key head shared by every graph record: `[SUBSYSTEM][KEY_VERSION][tag]`.
pub const KEY_HEAD_LEN: usize = 3;

/// Internal dense node/edge identifier (RFC 0004, D5).
///
/// UIDs are dense `u64`s allocated by [`crate::ids::GraphAllocators`]. Density
/// is load-bearing: it is what makes roaring compression and cheap set math
/// work (namidb documented the UUID mistake that forced binary-searched
/// `Vec<NodeId>` instead of offset math). Encoded **big-endian, fixed 8 bytes**
/// so byte order equals numeric order — the contract the posting-list set
/// algebra relies on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Uid(pub u64);

impl Uid {
    /// The reserved nil UID. Never allocated; used as a sentinel by later
    /// milestones (e.g. "no such node").
    pub const NIL: Uid = Uid(0);

    /// Returns the raw `u64`.
    #[inline]
    pub fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Uid {
    fn from(v: u64) -> Self {
        Uid(v)
    }
}

/// Interned predicate (edge-type) id — a `u32` allocated in the `Predicate`
/// id-space. Fixed-width big-endian for prefix clustering per predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PredId(pub u32);

impl PredId {
    #[inline]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for PredId {
    fn from(v: u32) -> Self {
        PredId(v)
    }
}

/// Interned node-label id — a `u32` allocated in the `Label` id-space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelId(pub u32);

impl LabelId {
    #[inline]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for LabelId {
    fn from(v: u32) -> Self {
        LabelId(v)
    }
}

/// Interned property-key id — a `u32` allocated in the `PropertyKey` id-space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropId(pub u32);

impl PropId {
    #[inline]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for PropId {
    fn from(v: u32) -> Self {
        PropId(v)
    }
}

/// Edge direction — selects the forward or reverse adjacency projection.
///
/// Every edge is materialized twice in one atomic batch (RFC 0003 §4.4):
/// [`Direction::Out`] keyed by source ([`RecordType::EdgeOut`]) and
/// [`Direction::In`] keyed by destination ([`RecordType::EdgeIn`]). The
/// [`Direction`] byte is also a fixed-width component of the count-index key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Direction {
    /// Forward adjacency: `(src, pred) -> {dst}`. Keyed by source uid.
    Out,
    /// Reverse adjacency: `(dst, pred) -> {src}`. Keyed by destination uid.
    In,
}

impl Direction {
    /// The 1-byte on-wire encoding (`Out = 0x00`, `In = 0x01`).
    #[inline]
    pub fn as_byte(self) -> u8 {
        match self {
            Direction::Out => 0x00,
            Direction::In => 0x01,
        }
    }

    /// Decodes a direction byte.
    #[inline]
    pub fn from_byte(b: u8) -> crate::Result<Self> {
        match b {
            0x00 => Ok(Direction::Out),
            0x01 => Ok(Direction::In),
            other => Err(crate::Error::encoding(format!(
                "invalid direction byte: 0x{other:02x}"
            ))),
        }
    }

    /// The adjacency record type this direction is stored under.
    #[inline]
    pub fn record_type(self) -> RecordType {
        match self {
            Direction::Out => RecordType::EdgeOut,
            Direction::In => RecordType::EdgeIn,
        }
    }
}

/// The three independent name-spaces interned into `u32` ids (RFC 0003).
///
/// A label and a predicate may share a name without colliding because each
/// kind is interned separately; the kind is a fixed-width component of the
/// `SchemaName` / `SchemaId` keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SchemaKind {
    /// Node label name-space.
    Label,
    /// Edge predicate name-space.
    Predicate,
    /// Property key name-space.
    PropertyKey,
}

impl SchemaKind {
    /// The 1-byte on-wire encoding (`Label = 0`, `Predicate = 1`, `PropertyKey = 2`).
    #[inline]
    pub fn as_byte(self) -> u8 {
        match self {
            SchemaKind::Label => 0,
            SchemaKind::Predicate => 1,
            SchemaKind::PropertyKey => 2,
        }
    }

    /// Decodes a schema-kind byte.
    #[inline]
    pub fn from_byte(b: u8) -> crate::Result<Self> {
        match b {
            0 => Ok(SchemaKind::Label),
            1 => Ok(SchemaKind::Predicate),
            2 => Ok(SchemaKind::PropertyKey),
            other => Err(crate::Error::encoding(format!(
                "invalid schema kind byte: 0x{other:02x}"
            ))),
        }
    }
}
