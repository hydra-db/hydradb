//! Name interning model and durable schema value format (RFC 0003
//! §"Schema keyspace").
//!
//! Names — node labels, edge predicates, property keys — are interned to
//! compact `u32` ids so the keyspace stores fixed-width ids instead of
//! repeating name strings for every vertex (schema-preserving encoding). Each
//! of the three name-spaces ([`SchemaKind`]) is interned **independently**, so
//! a label and a predicate may share a name without colliding.
//!
//! The durable source of truth is the KV keyspace:
//!
//! ```text
//! SchemaName [kind][name]  -> u32 id       (name -> id, consulted on write)
//! SchemaId   [kind][id]    -> SchemaEntry   (id -> name + directives, on read)
//! ```
//!
//! This module owns the [`SchemaEntry`] value format (the `SchemaId` value) and
//! an in-memory [`SchemaCache`] bimap that mirrors those records. The cache is
//! rebuilt from storage on open ([`SchemaCache::rebuild_from_storage`]); it is a
//! read-through accelerator, never the authority.
//!
//! House rule (opendata): **values are little-endian.** [`SchemaEntry::encode`]
//! and [`SchemaEntry::decode`] are exact inverses and reject truncated,
//! trailing, or unknown bytes rather than panicking.

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;

use common::StorageRead;
use common::serde::encoding::{decode_array_count, decode_utf8, encode_array_count, encode_utf8};

use crate::serde::keys::parse_schema_id_key;
use crate::serde::{RecordType, SchemaKind, keys::record_type_range};
use crate::{Error, Result};

/// Scalar type a predicate's value carries (for value indexes).
///
/// Encoded as a single little-endian byte in a [`SchemaEntry`]. `None` is the
/// default for pure edge predicates and labels that carry no scalar value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValType {
    /// No scalar value (edge predicate / label).
    None,
    /// UTF-8 string.
    String,
    /// 64-bit signed integer.
    Int,
    /// 64-bit IEEE float.
    Float,
    /// Boolean.
    Bool,
    /// Timestamp.
    DateTime,
}

impl ValType {
    /// The 1-byte on-wire encoding. Discriminants are stable and must not be
    /// reordered — they are persisted.
    fn as_byte(self) -> u8 {
        match self {
            ValType::None => 0,
            ValType::String => 1,
            ValType::Int => 2,
            ValType::Float => 3,
            ValType::Bool => 4,
            ValType::DateTime => 5,
        }
    }

    /// Decodes a value-type byte, erroring on an unknown discriminant.
    fn from_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(ValType::None),
            1 => Ok(ValType::String),
            2 => Ok(ValType::Int),
            3 => Ok(ValType::Float),
            4 => Ok(ValType::Bool),
            5 => Ok(ValType::DateTime),
            other => Err(Error::encoding(format!(
                "invalid val-type byte: 0x{other:02x}"
            ))),
        }
    }
}

/// Index tokenizer selecting how a predicate's value is tokenized for its index
/// (RFC 0006). Encoded as a single little-endian byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tokenizer {
    /// Whole-value exact match (order-preserving `terminated_bytes`).
    Exact,
    /// Full-text term tokenizer.
    Term,
    /// Hash of the value (equality only).
    Hash,
    /// Sortable integer token (range queries).
    Int,
    /// Sortable float token (range queries).
    Float,
}

impl Tokenizer {
    /// The 1-byte on-wire encoding. Discriminants are stable and must not be
    /// reordered — they are persisted.
    fn as_byte(self) -> u8 {
        match self {
            Tokenizer::Exact => 0,
            Tokenizer::Term => 1,
            Tokenizer::Hash => 2,
            Tokenizer::Int => 3,
            Tokenizer::Float => 4,
        }
    }

    /// Decodes a tokenizer byte, erroring on an unknown discriminant.
    fn from_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Tokenizer::Exact),
            1 => Ok(Tokenizer::Term),
            2 => Ok(Tokenizer::Hash),
            3 => Ok(Tokenizer::Int),
            4 => Ok(Tokenizer::Float),
            other => Err(Error::encoding(format!(
                "invalid tokenizer byte: 0x{other:02x}"
            ))),
        }
    }
}

/// Index-build directives for a predicate or property (RFC 0006).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Directives {
    /// Tokenizers to build value indexes with. Empty means no value index.
    pub index: Vec<Tokenizer>,
    /// Materialize the reverse (`EdgeIn`) adjacency. v0: always `true` for
    /// predicates (RFC 0003 D10).
    pub reverse: bool,
    /// Maintain the `Count` index.
    pub count: bool,
    /// Multi-value predicate (a node may have many values on this predicate).
    pub list: bool,
}

/// The durable value stored under a [`RecordType::SchemaId`] key (`id -> entry`).
///
/// Carries the interned name plus the index-build directives. Encoded
/// little-endian; [`SchemaEntry::encode`] / [`SchemaEntry::decode`] are exact
/// inverses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaEntry {
    /// The interned name (label / predicate / property key).
    pub name: String,
    /// The scalar type a predicate's value carries (or [`ValType::None`]).
    pub value_type: ValType,
    /// Index-build directives.
    pub directives: Directives,
}

impl SchemaEntry {
    /// Encodes the entry into its durable little-endian value form.
    ///
    /// Layout: `encode_utf8(name)` then `value_type` (1 byte), then the
    /// directives — `reverse`, `count`, `list` as one `0`/`1` byte each,
    /// followed by `encode_array_count(index.len())` and one tokenizer byte per
    /// entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        encode_utf8(&self.name, &mut buf);
        buf.put_u8(self.value_type.as_byte());
        buf.put_u8(u8::from(self.directives.reverse));
        buf.put_u8(u8::from(self.directives.count));
        buf.put_u8(u8::from(self.directives.list));
        encode_array_count(self.directives.index.len(), &mut buf);
        for tok in &self.directives.index {
            buf.put_u8(tok.as_byte());
        }
        buf.freeze()
    }

    /// Decodes an entry from its durable value form — the exact inverse of
    /// [`SchemaEntry::encode`].
    ///
    /// Errors (never panics) on truncation, an unknown value-type / tokenizer
    /// byte, an out-of-range boolean byte, or trailing bytes after the entry.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cur = bytes;
        let name = decode_utf8(&mut cur).map_err(Error::from)?;
        let value_type = ValType::from_byte(take_u8(&mut cur, "schema value_type")?)?;
        let reverse = take_bool(&mut cur, "schema reverse")?;
        let count = take_bool(&mut cur, "schema count")?;
        let list = take_bool(&mut cur, "schema list")?;
        let index_len = decode_array_count(&mut cur).map_err(Error::from)?;
        let mut index = Vec::with_capacity(index_len);
        for _ in 0..index_len {
            index.push(Tokenizer::from_byte(take_u8(
                &mut cur,
                "schema tokenizer",
            )?)?);
        }
        if !cur.is_empty() {
            return Err(Error::encoding(format!(
                "trailing bytes after schema entry: {} extra",
                cur.len()
            )));
        }
        Ok(SchemaEntry {
            name,
            value_type,
            directives: Directives {
                index,
                reverse,
                count,
                list,
            },
        })
    }
}

/// Reads one byte from `cur`, advancing it, or errors on truncation.
fn take_u8(cur: &mut &[u8], what: &str) -> Result<u8> {
    if cur.is_empty() {
        return Err(Error::encoding(format!("truncated {what}: need 1 byte")));
    }
    let b = cur[0];
    *cur = &cur[1..];
    Ok(b)
}

/// Reads a `0`/`1` boolean byte, erroring on any other value.
fn take_bool(cur: &mut &[u8], what: &str) -> Result<bool> {
    match take_u8(cur, what)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::encoding(format!(
            "invalid boolean byte for {what}: 0x{other:02x}"
        ))),
    }
}

/// Maps a [`SchemaKind`] to a dense index into [`SchemaCache`]'s per-kind slots.
fn kind_index(kind: SchemaKind) -> usize {
    kind.as_byte() as usize
}

/// One name-space's bidirectional map: `name -> id` and `id -> entry`.
#[derive(Debug, Default)]
struct KindMaps {
    by_name: HashMap<String, u32>,
    by_id: HashMap<u32, SchemaEntry>,
}

/// In-memory name↔id cache, one bimap per [`SchemaKind`].
///
/// The durable source of truth is the `SchemaName` / `SchemaId` records; this
/// cache mirrors them for I/O-free lookups on the hot path and is rebuilt from
/// storage on open ([`SchemaCache::rebuild_from_storage`]). Because each kind
/// has its own maps, a label and a predicate may share a name without
/// colliding.
#[derive(Debug, Default)]
pub struct SchemaCache {
    /// Per-kind maps, indexed by [`kind_index`] (`Label`, `Predicate`,
    /// `PropertyKey`).
    kinds: [KindMaps; 3],
}

impl SchemaCache {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts (or overwrites) a known `(kind, id, entry)` into the cache,
    /// keeping both directions of the bimap consistent.
    pub fn insert(&mut self, kind: SchemaKind, id: u32, entry: SchemaEntry) {
        let maps = &mut self.kinds[kind_index(kind)];
        maps.by_name.insert(entry.name.clone(), id);
        maps.by_id.insert(id, entry);
    }

    /// Looks up the interned id for a name (cache-only, no I/O).
    pub fn id_of(&self, kind: SchemaKind, name: &str) -> Option<u32> {
        self.kinds[kind_index(kind)].by_name.get(name).copied()
    }

    /// Looks up the entry (and thus name + directives) for an id (cache-only).
    pub fn entry_of(&self, kind: SchemaKind, id: u32) -> Option<&SchemaEntry> {
        self.kinds[kind_index(kind)].by_id.get(&id)
    }

    /// Rebuilds the whole cache by scanning every [`RecordType::SchemaId`]
    /// record in `storage`.
    ///
    /// Each record's key is parsed for its `(kind, id)` and its value is decoded
    /// into a [`SchemaEntry`]; a malformed key or value fails the rebuild.
    pub async fn rebuild_from_storage(storage: &dyn StorageRead) -> Result<Self> {
        let records = storage
            .scan(record_type_range(RecordType::SchemaId))
            .await
            .map_err(Error::from)?;
        let mut cache = Self::new();
        for record in records {
            let (kind, id) = parse_schema_id_key(&record.key)?;
            let entry = SchemaEntry::decode(&record.value)?;
            cache.insert(kind, id, entry);
        }
        Ok(cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::keys::schema_id_key;
    use common::storage::in_memory::InMemoryStorage;
    use common::storage::{Record, Storage};

    fn entry(name: &str, value_type: ValType, directives: Directives) -> SchemaEntry {
        SchemaEntry {
            name: name.to_string(),
            value_type,
            directives,
        }
    }

    fn assert_roundtrip(e: &SchemaEntry) {
        // when
        let bytes = e.encode();
        let decoded = SchemaEntry::decode(&bytes).unwrap();
        // then
        assert_eq!(&decoded, e);
    }

    #[test]
    fn should_roundtrip_entry_with_empty_directives() {
        // given
        let e = entry("name", ValType::String, Directives::default());
        // when / then
        assert_roundtrip(&e);
    }

    #[test]
    fn should_roundtrip_entry_with_multiple_tokenizers() {
        // given
        let e = entry(
            "friends",
            ValType::Int,
            Directives {
                index: vec![Tokenizer::Exact, Tokenizer::Int],
                reverse: true,
                count: true,
                list: true,
            },
        );
        // when / then
        assert_roundtrip(&e);
    }

    #[test]
    fn should_roundtrip_every_val_type() {
        // given / when / then
        for vt in [
            ValType::None,
            ValType::String,
            ValType::Int,
            ValType::Float,
            ValType::Bool,
            ValType::DateTime,
        ] {
            assert_roundtrip(&entry("p", vt, Directives::default()));
        }
    }

    #[test]
    fn should_roundtrip_every_bool_directive_combination() {
        // given / when / then — every reverse/count/list combination
        for reverse in [false, true] {
            for count in [false, true] {
                for list in [false, true] {
                    let e = entry(
                        "p",
                        ValType::Bool,
                        Directives {
                            index: vec![Tokenizer::Hash],
                            reverse,
                            count,
                            list,
                        },
                    );
                    assert_roundtrip(&e);
                }
            }
        }
    }

    #[test]
    fn should_roundtrip_every_tokenizer() {
        // given / when / then
        for tok in [
            Tokenizer::Exact,
            Tokenizer::Term,
            Tokenizer::Hash,
            Tokenizer::Int,
            Tokenizer::Float,
        ] {
            let e = entry(
                "p",
                ValType::Float,
                Directives {
                    index: vec![tok],
                    ..Directives::default()
                },
            );
            assert_roundtrip(&e);
        }
    }

    #[test]
    fn should_reject_truncated_bytes_on_decode() {
        // given — a valid encoding
        let e = entry(
            "n",
            ValType::Int,
            Directives {
                index: vec![Tokenizer::Exact, Tokenizer::Int],
                reverse: true,
                count: false,
                list: false,
            },
        );
        let bytes = e.encode();

        // when / then — every strict prefix must fail to decode
        for cut in 0..bytes.len() {
            assert!(
                SchemaEntry::decode(&bytes[..cut]).is_err(),
                "prefix of len {cut} should not decode"
            );
        }
    }

    #[test]
    fn should_reject_trailing_garbage_on_decode() {
        // given — a valid encoding with an extra byte appended
        let e = entry("n", ValType::None, Directives::default());
        let mut bytes = e.encode().to_vec();
        bytes.push(0xAB);

        // when / then
        assert!(SchemaEntry::decode(&bytes).is_err());
    }

    #[test]
    fn should_reject_unknown_val_type_byte() {
        // given — a name then an out-of-range value-type byte
        let mut buf = BytesMut::new();
        encode_utf8("n", &mut buf);
        buf.put_u8(0x7F);

        // when / then
        assert!(SchemaEntry::decode(&buf).is_err());
    }

    #[test]
    fn should_resolve_both_directions_after_insert() {
        // given
        let mut cache = SchemaCache::new();
        let e = entry("KNOWS", ValType::None, Directives::default());
        cache.insert(SchemaKind::Predicate, 7, e.clone());

        // when / then
        assert_eq!(cache.id_of(SchemaKind::Predicate, "KNOWS"), Some(7));
        assert_eq!(cache.entry_of(SchemaKind::Predicate, 7), Some(&e));
    }

    #[test]
    fn should_return_none_for_missing_lookups() {
        // given
        let cache = SchemaCache::new();

        // when / then
        assert_eq!(cache.id_of(SchemaKind::Label, "missing"), None);
        assert_eq!(cache.entry_of(SchemaKind::Label, 42), None);
    }

    #[test]
    fn should_not_collide_across_kinds_sharing_a_name() {
        // given — a label and a predicate with the same name, different ids
        let mut cache = SchemaCache::new();
        cache.insert(
            SchemaKind::Label,
            1,
            entry("Person", ValType::None, Directives::default()),
        );
        cache.insert(
            SchemaKind::Predicate,
            2,
            entry("Person", ValType::None, Directives::default()),
        );

        // when / then — each kind resolves to its own id
        assert_eq!(cache.id_of(SchemaKind::Label, "Person"), Some(1));
        assert_eq!(cache.id_of(SchemaKind::Predicate, "Person"), Some(2));
    }

    #[tokio::test]
    async fn should_rebuild_cache_from_storage() {
        // given — a store seeded with two SchemaId records of different kinds
        let storage = InMemoryStorage::new();
        let label = entry(
            "Person",
            ValType::None,
            Directives {
                reverse: true,
                ..Directives::default()
            },
        );
        let pred = entry(
            "KNOWS",
            ValType::Int,
            Directives {
                index: vec![Tokenizer::Exact, Tokenizer::Int],
                reverse: true,
                count: true,
                list: false,
            },
        );
        let records = vec![
            Record::new(schema_id_key(SchemaKind::Label, 1), label.encode()).into(),
            Record::new(schema_id_key(SchemaKind::Predicate, 9), pred.encode()).into(),
        ];
        storage.put(records).await.unwrap();

        // when
        let cache = SchemaCache::rebuild_from_storage(&storage).await.unwrap();

        // then — both entries come back under their own kind and id
        assert_eq!(cache.id_of(SchemaKind::Label, "Person"), Some(1));
        assert_eq!(cache.entry_of(SchemaKind::Label, 1), Some(&label));
        assert_eq!(cache.id_of(SchemaKind::Predicate, "KNOWS"), Some(9));
        assert_eq!(cache.entry_of(SchemaKind::Predicate, 9), Some(&pred));
    }
}
