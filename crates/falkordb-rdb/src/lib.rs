//! Bounds-checked decoder for FalkorDB per-key Redis `DUMP` payloads.
//!
//! The crate deliberately has no dependency on Turbolay. It validates every
//! envelope and matrix layer before exposing graph records, allowing import
//! code to parallelize decoding without granting malformed input access to the
//! destination object store.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod properties;
pub use properties::{intern, Properties};

const REDIS_MODULE_2: u8 = 7;
const MODULE_OPCODE_STRING: u64 = 5;
const MODULE_OPCODE_EOF: u64 = 0;
const GRAPH_MODULE: &str = "graphdata";
const SUPPORTED_RDB_VERSION: u16 = 13;
const SUPPORTED_ENCODING_VERSION: u16 = 19;
const MAX_METADATA_ITEMS: usize = 1024 * 1024;
const MAX_COLLECTION_ITEMS: usize = 64 * 1024 * 1024;
const MAX_SINGLE_ALLOCATION_BYTES: usize = 2 * 1024 * 1024 * 1024;
const MAX_IDENTITY_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const MSB: u64 = 1 << 63;

const SI_MAP: u64 = 1 << 0;
const SI_ARRAY: u64 = 1 << 3;
const SI_DATETIME: u64 = 1 << 5;
const SI_DATE: u64 = 1 << 7;
const SI_TIME: u64 = 1 << 8;
const SI_DURATION: u64 = 1 << 10;
const SI_STRING: u64 = 1 << 11;
const SI_BOOL: u64 = 1 << 12;
const SI_INT64: u64 = 1 << 13;
const SI_DOUBLE: u64 = 1 << 14;
const SI_NULL: u64 = 1 << 15;
const SI_POINT: u64 = 1 << 17;
const SI_VECTOR_F32: u64 = 1 << 18;
const SI_INTERN: u64 = 1 << 19;
const SI_INTERN_STRING: u64 = SI_STRING | SI_INTERN;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("{layer} at offset {offset}: {message}")]
    Invalid {
        layer: &'static str,
        offset: usize,
        message: String,
    },
    #[error("{layer} at offset {offset}: need {need} bytes, have {have}")]
    Truncated {
        layer: &'static str,
        offset: usize,
        need: usize,
        have: usize,
    },
    #[error("zstd tensor block failed: {0}")]
    Zstd(#[from] std::io::Error),
    #[error("read dump at offset {offset}: {source}")]
    Io {
        offset: u64,
        #[source]
        source: std::io::Error,
    },
}

pub type Result<T> = std::result::Result<T, DecodeError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DumpIdentity {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(#[serde(with = "lossless_float")] f64),
    String(String),
    Array(Vec<PropertyValue>),
    Point {
        #[serde(with = "lossless_float")]
        latitude: f64,
        #[serde(with = "lossless_float")]
        longitude: f64,
    },
    VectorF32(#[serde(with = "lossless_float::vector")] Vec<f32>),
    DateTime(i64),
    Date(i64),
    Time(i64),
    Duration(i64),
    Map(BTreeMap<String, PropertyValue>),
}

/// JSON has no NaN or infinity, and serde_json writes them as null. Floats keep
/// their value in every format: finite ones as numbers, the rest as the strings
/// "NaN", "Infinity" and "-Infinity", which deserialize back to the same value.
mod lossless_float {
    use std::fmt;

    use serde::de::{self, Deserializer, Visitor};
    use serde::ser::{SerializeSeq, Serializer};

    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        match non_finite_name(*value) {
            Some(name) => serializer.serialize_str(name),
            None => serializer.serialize_f64(*value),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        deserializer.deserialize_any(FloatVisitor)
    }

    fn non_finite_name(value: f64) -> Option<&'static str> {
        if value.is_nan() {
            Some("NaN")
        } else if value == f64::INFINITY {
            Some("Infinity")
        } else if value == f64::NEG_INFINITY {
            Some("-Infinity")
        } else {
            None
        }
    }

    struct FloatVisitor;

    impl Visitor<'_> for FloatVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a number, \"NaN\", \"Infinity\" or \"-Infinity\"")
        }

        fn visit_f64<E: de::Error>(self, value: f64) -> Result<f64, E> {
            Ok(value)
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<f64, E> {
            Ok(value as f64)
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<f64, E> {
            Ok(value as f64)
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<f64, E> {
            match value {
                "NaN" => Ok(f64::NAN),
                "Infinity" => Ok(f64::INFINITY),
                "-Infinity" => Ok(f64::NEG_INFINITY),
                other => Err(E::invalid_value(de::Unexpected::Str(other), &self)),
            }
        }
    }

    pub mod vector {
        use super::*;

        struct Element(f32);

        impl serde::Serialize for Element {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match non_finite_name(f64::from(self.0)) {
                    Some(name) => serializer.serialize_str(name),
                    None => serializer.serialize_f32(self.0),
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for Element {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                deserializer.deserialize_any(FloatVisitor).map(|value| Element(value as f32))
            }
        }

        pub fn serialize<S: Serializer>(values: &[f32], serializer: S) -> Result<S::Ok, S::Error> {
            let mut seq = serializer.serialize_seq(Some(values.len()))?;
            for value in values {
                seq.serialize_element(&Element(*value))?;
            }
            seq.end()
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f32>, D::Error> {
            let elements: Vec<Element> = serde::Deserialize::deserialize(deserializer)?;
            Ok(elements.into_iter().map(|element| element.0).collect())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub id: u64,
    pub name: String,
    #[serde(default)]
    pub index_count: usize,
    #[serde(default)]
    pub constraint_count: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub id: u64,
    pub properties: Properties,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct RelationshipCell {
    pub edge_type: String,
    pub src: u64,
    pub dst: u64,
    pub edge_id: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecodeDiagnostics {
    pub delta_delete_missing_from_base: u64,
    pub delta_insert_overlaps_base: u64,
    pub delta_insert_overlaps_delete: u64,
    pub delete_markers: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationDiagnostics {
    pub relationship_cells: u64,
    pub live_edge_records: u64,
    pub relationships_with_missing_endpoints: u64,
    pub cells_without_edge_records: u64,
    pub edge_records_without_cells: u64,
    pub reused_edge_id_cells: u64,
    pub relationship_pairs_without_adjacency: u64,
    pub adjacency_pairs_without_relationships: u64,
    /// Multi-edge cells whose edge lists the dump does not hold; each
    /// explains its adjacency pair.
    #[serde(default)]
    pub dangling_tensor_cells: u64,
    /// Multi-edge cells whose edge lists FalkorDB wrote empty; each explains
    /// its adjacency pair.
    #[serde(default)]
    pub empty_tensor_cells: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphDump {
    pub name: String,
    pub rdb_version: u16,
    pub encoding_version: u16,
    pub declared_nodes: u64,
    pub declared_edges: u64,
    pub declared_deleted_nodes: u64,
    pub declared_deleted_edges: u64,
    pub attributes: Vec<String>,
    pub node_schemas: Vec<Schema>,
    pub edge_schemas: Vec<Schema>,
    pub nodes: BTreeMap<u64, Entity>,
    pub edges: BTreeMap<u64, Entity>,
    pub labels: BTreeMap<u64, BTreeSet<String>>,
    pub relationships: Vec<RelationshipCell>,
    pub adjacency: BTreeSet<(u64, u64)>,
    /// Multi-edge cells whose edge-id lists the dump does not hold.
    #[serde(default)]
    pub dangling_tensor_cells: Vec<DanglingTensorCell>,
    /// Multi-edge cells whose edge-id lists FalkorDB wrote empty.
    #[serde(default)]
    pub empty_tensor_cells: Vec<EmptyTensorCell>,
    pub diagnostics: DecodeDiagnostics,
}

/// A relationship matrix cell marked as holding several edges whose edge-id
/// list FalkorDB did not write. FalkorDB 4.18 writes a relationship type's
/// lists only when the type's edge counter differs from its matrix's cell
/// count, so a drifted counter leaves every multi-edge cell of the type
/// pointing at memory the dump does not contain. The cell's type and
/// endpoints are known; its edges are not.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DanglingTensorCell {
    pub edge_type: String,
    pub src: u64,
    pub dst: u64,
    /// The cell's raw value: the multi-edge flag and FalkorDB's in-memory
    /// address of the list.
    pub value: u64,
}

/// A relationship matrix cell marked as holding several edges whose edge-id
/// list FalkorDB wrote with no ids. FalkorDB keeps such a cell after the
/// edges it held are deleted (seen in production on 2026-11-03), and reads
/// through it find no edges, so it decodes to none: only its pair, which the
/// adjacency matrix still holds, is reported.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EmptyTensorCell {
    pub edge_type: String,
    pub src: u64,
    pub dst: u64,
}

impl GraphDump {
    pub fn reconciliation_diagnostics(&self) -> ReconciliationDiagnostics {
        let live_edge_ids = self.edges.keys().copied().collect::<BTreeSet<_>>();
        let relationship_edge_ids = self
            .relationships
            .iter()
            .map(|relationship| relationship.edge_id)
            .collect::<BTreeSet<_>>();
        let relationship_pairs = self.typed_pairs();
        ReconciliationDiagnostics {
            relationship_cells: self.relationships.len() as u64,
            live_edge_records: self.edges.len() as u64,
            relationships_with_missing_endpoints: self
                .relationships
                .iter()
                .filter(|relationship| !self.nodes.contains_key(&relationship.src) || !self.nodes.contains_key(&relationship.dst))
                .count() as u64,
            cells_without_edge_records: self
                .relationships
                .iter()
                .filter(|relationship| !live_edge_ids.contains(&relationship.edge_id))
                .count() as u64,
            edge_records_without_cells: live_edge_ids.difference(&relationship_edge_ids).count() as u64,
            reused_edge_id_cells: self.relationships.len().saturating_sub(relationship_edge_ids.len()) as u64,
            relationship_pairs_without_adjacency: relationship_pairs.difference(&self.adjacency).count() as u64,
            adjacency_pairs_without_relationships: self.adjacency.difference(&relationship_pairs).count() as u64,
            dangling_tensor_cells: self.dangling_tensor_cells.len() as u64,
            empty_tensor_cells: self.empty_tensor_cells.len() as u64,
        }
    }

    /// Adjacency pairs no typed relationship explains: pairs FalkorDB kept
    /// after their last edge was deleted. Counted alone, without the rest of
    /// reconciliation_diagnostics, for a check on every graph.
    pub fn adjacency_pairs_without_relationships(&self) -> u64 {
        self.adjacency.difference(&self.typed_pairs()).count() as u64
    }

    /// The node pairs a typed relationship joins: every decoded relationship,
    /// every dangling multi-edge cell, whose edges exist though the dump holds
    /// no list of them, and every empty one, whose pair the adjacency matrix
    /// keeps after its edges were deleted.
    fn typed_pairs(&self) -> BTreeSet<(u64, u64)> {
        self.relationships
            .iter()
            .map(|relationship| (relationship.src, relationship.dst))
            .chain(self.dangling_tensor_cells.iter().map(|cell| (cell.src, cell.dst)))
            .chain(self.empty_tensor_cells.iter().map(|cell| (cell.src, cell.dst)))
            .collect()
    }

    pub fn validate_reconciliation(&self) -> Result<()> {
        if self.nodes.len() as u64 != self.declared_nodes {
            return invalid(
                "graph",
                0,
                format!("decoded {} live nodes, header declares {}", self.nodes.len(), self.declared_nodes),
            );
        }
        if self.edges.len() as u64 != self.declared_edges {
            return invalid(
                "graph",
                0,
                format!(
                    "decoded {} live edge records, header declares {}",
                    self.edges.len(),
                    self.declared_edges
                ),
            );
        }
        if let Some(node_id) = self.labels.keys().find(|node_id| !self.nodes.contains_key(node_id)) {
            return invalid("graph", 0, format!("label matrix references missing node {node_id}"));
        }
        // An adjacency pair no typed relationship explains is FalkorDB keeping
        // the pair after its last edge was deleted (1 of 2,060,706 pairs in a
        // 2026-11-03 production graph), as a relationship pair missing from
        // the adjacency is it dropping one. Neither has an edge to import, so
        // both are counted in reconciliation_diagnostics, not refused.
        Ok(())
    }
}

pub fn decode_dump(data: &[u8]) -> Result<GraphDump> {
    let envelope = decode_envelope(data)?;
    let mut stream = TaggedStream::new(envelope.chunks);

    let name = stream.c_string()?;
    let declared_nodes = stream.unsigned()?;
    let declared_edges = stream.unsigned()?;
    let declared_deleted_nodes = stream.unsigned()?;
    let declared_deleted_edges = stream.unsigned()?;
    let label_count = stream.unsigned()?;
    let relation_count_declared = stream.unsigned()?;
    let relation_count = stream.metadata_count(relation_count_declared, 9, "relationship type count")?;
    for _ in 0..relation_count {
        let _ = stream.unsigned()?;
    }
    let key_count = stream.unsigned()?;
    if key_count != 1 {
        return invalid("graph-header", stream.offset(), format!("key_count is {key_count}, expected 1"));
    }
    let declared = stream.unsigned()?;
    let attribute_count = stream.metadata_count(declared, 10, "attribute count")?;
    let mut attributes = allocated_vec(attribute_count, "attribute table", stream.offset())?;
    for _ in 0..attribute_count {
        attributes.push(stream.c_string()?);
    }
    let attribute_names = attributes.iter().map(|name| intern(name)).collect::<Vec<_>>();
    let node_schemas = read_schema(&mut stream)?;
    let edge_schemas = read_schema(&mut stream)?;
    if node_schemas.len() as u64 != label_count {
        return invalid(
            "schema",
            stream.offset(),
            format!("{} node schemas, header declares {label_count}", node_schemas.len()),
        );
    }
    if edge_schemas.len() != relation_count {
        return invalid(
            "schema",
            stream.offset(),
            format!("{} edge schemas, header declares {relation_count}", edge_schemas.len()),
        );
    }

    let declared = stream.unsigned()?;
    let payload_count = stream.metadata_count(declared, 18, "payload count")?;
    let mut payloads = allocated_vec(payload_count, "payload table", stream.offset())?;
    for _ in 0..payload_count {
        payloads.push((stream.unsigned()?, stream.unsigned()?));
    }

    let node_names: BTreeMap<u64, String> = node_schemas.iter().map(|s| (s.id, s.name.clone())).collect();
    let edge_names: BTreeMap<u64, String> = edge_schemas.iter().map(|s| (s.id, s.name.clone())).collect();
    let mut nodes = BTreeMap::new();
    let mut edges = BTreeMap::new();
    let mut labels: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut relationships = Vec::new();
    let mut adjacency = BTreeSet::new();
    let mut diagnostics = DecodeDiagnostics::default();
    let mut dangling_tensor_cells = Vec::new();
    let mut empty_tensor_cells = Vec::new();
    let mut covered = BTreeMap::<u64, u64>::new();

    for (state, count) in payloads {
        *covered.entry(state).or_default() += count;
        match state {
            1 => read_entities(&mut stream, count, &attribute_names, &mut nodes)?,
            2 | 4 => {
                let _ = stream.bytes()?;
            }
            3 => read_entities(&mut stream, count, &attribute_names, &mut edges)?,
            6 => {
                let declared = stream.unsigned()?;
                let matrix_count = stream.metadata_count(declared, 9, "label matrix count")?;
                for _ in 0..matrix_count {
                    let _label_id = stream.unsigned()?;
                    let _ = read_delta(&mut stream, false, &mut diagnostics)?;
                }
            }
            7 => {
                for expected in 0..relation_count {
                    let actual = stream.unsigned()?;
                    if actual != expected as u64 {
                        return invalid(
                            "relation-matrix",
                            stream.offset(),
                            format!("relation id {actual}, expected {expected}"),
                        );
                    }
                    let matrices = read_delta(&mut stream, true, &mut diagnostics)?;
                    let tensors = read_tensors(&mut stream)?;
                    let edge_type = edge_names.get(&actual).ok_or_else(|| DecodeError::Invalid {
                        layer: "relation-matrix",
                        offset: stream.offset(),
                        message: format!("relation id {actual} has no schema"),
                    })?;
                    relationships.extend(relation_cells(
                        edge_type,
                        matrices,
                        tensors,
                        &mut dangling_tensor_cells,
                        &mut empty_tensor_cells,
                        &mut diagnostics,
                    )?);
                }
            }
            8 => {
                for entry in read_delta(&mut stream, false, &mut diagnostics)? {
                    adjacency.insert((entry.row, entry.col));
                }
            }
            9 => {
                for entry in read_delta(&mut stream, false, &mut diagnostics)? {
                    let label = node_names.get(&entry.col).ok_or_else(|| DecodeError::Invalid {
                        layer: "label-matrix",
                        offset: stream.offset(),
                        message: format!("label id {} has no schema", entry.col),
                    })?;
                    labels.entry(entry.row).or_default().insert(label.clone());
                }
            }
            other => return invalid("payload", stream.offset(), format!("unknown payload state {other}")),
        }
    }
    if !stream.at_end()? {
        return invalid("typed-stream", stream.offset(), "bytes remain after final payload");
    }
    for (state, expected) in [
        (1, declared_nodes),
        (2, declared_deleted_nodes),
        (3, declared_edges),
        (4, declared_deleted_edges),
    ] {
        if covered.get(&state).copied().unwrap_or(0) != expected {
            return invalid(
                "payload",
                0,
                format!(
                    "state {state} covers {} rows, expected {expected}",
                    covered.get(&state).copied().unwrap_or(0)
                ),
            );
        }
    }
    for state in [6, 7, 8, 9] {
        if covered.get(&state).copied().unwrap_or(0) != 1 {
            return invalid("payload", 0, format!("state {state} must appear exactly once"));
        }
    }
    relationships.sort();
    let graph = GraphDump {
        name,
        rdb_version: envelope.rdb_version,
        encoding_version: envelope.encoding_version,
        declared_nodes,
        declared_edges,
        declared_deleted_nodes,
        declared_deleted_edges,
        attributes,
        node_schemas,
        edge_schemas,
        nodes,
        edges,
        labels,
        relationships,
        adjacency,
        dangling_tensor_cells,
        empty_tensor_cells,
        diagnostics,
    };
    graph.validate_reconciliation()?;
    Ok(graph)
}

/// Verify a Redis DUMP envelope and return its embedded FalkorDB graph name
/// without retaining the dump in memory or decoding its graph payload.
///
/// This is intended for an explicitly excluded snapshot object: its source
/// identity and byte integrity remain part of the import audit even though its
/// inconsistent graph body is not admitted to the destination image.
pub fn inspect_dump_identity<R>(reader: &mut R) -> Result<DumpIdentity>
where
    R: Read + Seek,
{
    let bytes = reader
        .seek(SeekFrom::End(0))
        .map_err(|source| DecodeError::Io { offset: 0, source })?;
    if bytes < 12 {
        return truncated("rdb-envelope", 0, 12, bytes as usize);
    }

    reader
        .seek(SeekFrom::End(-10))
        .map_err(|source| DecodeError::Io { offset: bytes, source })?;
    let mut footer = [0u8; 10];
    read_exact_at(reader, &mut footer, bytes - 10)?;
    let rdb_version = u16::from_le_bytes(footer[..2].try_into().unwrap());
    if rdb_version != SUPPORTED_RDB_VERSION {
        return invalid(
            "rdb-envelope",
            usize::try_from(bytes - 10).unwrap_or(usize::MAX),
            format!("RDB version {rdb_version}, expected {SUPPORTED_RDB_VERSION}"),
        );
    }
    let expected_crc = u64::from_le_bytes(footer[2..].try_into().unwrap());

    reader
        .seek(SeekFrom::Start(0))
        .map_err(|source| DecodeError::Io { offset: 0, source })?;
    let mut sha256 = Sha256::new();
    let mut crc = 0u64;
    let crc_bytes = bytes - 8;
    let mut offset = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    while offset < bytes {
        let wanted = usize::try_from((bytes - offset).min(buffer.len() as u64)).unwrap();
        read_exact_at(reader, &mut buffer[..wanted], offset)?;
        sha256.update(&buffer[..wanted]);
        let crc_count = usize::try_from(crc_bytes.saturating_sub(offset).min(wanted as u64)).unwrap();
        crc = crc64_jones_update(crc, &buffer[..crc_count]);
        offset += wanted as u64;
    }
    if crc != expected_crc {
        return invalid(
            "rdb-envelope",
            usize::try_from(bytes - 8).unwrap_or(usize::MAX),
            format!("CRC64 mismatch: expected {expected_crc:016x}, calculated {crc:016x}"),
        );
    }

    reader
        .seek(SeekFrom::Start(0))
        .map_err(|source| DecodeError::Io { offset: 0, source })?;
    let name = read_dump_name(reader)?;
    Ok(DumpIdentity {
        name,
        bytes,
        sha256: format!("{:x}", sha256.finalize()),
    })
}

fn read_dump_name<R: Read>(reader: &mut R) -> Result<String> {
    let mut stream = StreamCursor::new(reader);
    if stream.byte()? != REDIS_MODULE_2 {
        return invalid("rdb-envelope", 0, "object is not MODULE_2");
    }
    let (module_id, encoding) = stream.length()?;
    if encoding.is_some() {
        return invalid("rdb-envelope", stream.offset_usize(), "module id uses a special string encoding");
    }
    let name_bits = module_id >> 10;
    const CHARSET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut module = String::with_capacity(9);
    for k in 0..9 {
        module.push(CHARSET[((name_bits >> (6 * (8 - k))) & 63) as usize] as char);
    }
    let encoding_version = (module_id & 1023) as u16;
    if module != GRAPH_MODULE || encoding_version != SUPPORTED_ENCODING_VERSION {
        return invalid(
            "rdb-envelope",
            stream.offset_usize(),
            format!("module {module} encoding {encoding_version}, expected {GRAPH_MODULE} {SUPPORTED_ENCODING_VERSION}"),
        );
    }
    let (opcode, special) = stream.length()?;
    if special.is_some() || opcode != MODULE_OPCODE_STRING {
        return invalid(
            "rdb-envelope",
            stream.offset_usize(),
            format!("first module opcode is {opcode}, expected {MODULE_OPCODE_STRING}"),
        );
    }
    let first = stream.string(MAX_IDENTITY_CHUNK_BYTES)?;
    let chunks = if first.as_slice() == [6] {
        let (opcode, special) = stream.length()?;
        if special.is_some() || opcode != MODULE_OPCODE_STRING {
            return invalid(
                "rdb-envelope",
                stream.offset_usize(),
                format!("graph-name blob opcode is {opcode}, expected {MODULE_OPCODE_STRING}"),
            );
        }
        vec![first, stream.string(MAX_IDENTITY_CHUNK_BYTES)?]
    } else {
        vec![first]
    };
    TaggedStream::new(chunks).c_string()
}

struct StreamCursor<'a, R> {
    reader: &'a mut R,
    offset: u64,
}

impl<'a, R: Read> StreamCursor<'a, R> {
    fn new(reader: &'a mut R) -> Self {
        Self { reader, offset: 0 }
    }

    fn offset_usize(&self) -> usize {
        usize::try_from(self.offset).unwrap_or(usize::MAX)
    }

    fn byte(&mut self) -> Result<u8> {
        let mut byte = [0u8; 1];
        read_exact_at(self.reader, &mut byte, self.offset)?;
        self.offset += 1;
        Ok(byte[0])
    }

    fn bytes<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0u8; N];
        read_exact_at(self.reader, &mut bytes, self.offset)?;
        self.offset += N as u64;
        Ok(bytes)
    }

    fn length(&mut self) -> Result<(u64, Option<u8>)> {
        let first = self.byte()?;
        match first >> 6 {
            0 => Ok(((first & 0x3f) as u64, None)),
            1 => Ok(((((first & 0x3f) as u64) << 8) | self.byte()? as u64, None)),
            2 => match first {
                0x80 => Ok((u32::from_be_bytes(self.bytes::<4>()?) as u64, None)),
                0x81 => Ok((u64::from_be_bytes(self.bytes::<8>()?), None)),
                _ => invalid(
                    "rdb-envelope",
                    self.offset_usize().saturating_sub(1),
                    format!("invalid length marker {first:#x}"),
                ),
            },
            _ => Ok((0, Some(first & 0x3f))),
        }
    }

    fn string(&mut self, limit: usize) -> Result<Vec<u8>> {
        let (length, special) = self.length()?;
        match special {
            None => self.take_limited(length, limit, "RDB string length"),
            Some(0) => Ok(i8::from_le_bytes(self.bytes::<1>()?).to_string().into_bytes()),
            Some(1) => Ok(i16::from_le_bytes(self.bytes::<2>()?).to_string().into_bytes()),
            Some(2) => Ok(i32::from_le_bytes(self.bytes::<4>()?).to_string().into_bytes()),
            Some(3) => {
                let (compressed, c_special) = self.length()?;
                let (uncompressed, u_special) = self.length()?;
                if c_special.is_some() || u_special.is_some() {
                    return invalid("rdb-envelope", self.offset_usize(), "LZF lengths use special encoding");
                }
                let uncompressed = usize_from(uncompressed, "LZF identity output length", self.offset_usize())?;
                if uncompressed > limit {
                    return invalid(
                        "resource-limit",
                        self.offset_usize(),
                        format!("identity chunk requests {uncompressed} bytes, limit is {limit}"),
                    );
                }
                let compressed = self.take_limited(compressed, MAX_IDENTITY_CHUNK_BYTES, "LZF identity input length")?;
                lzf_decompress(&compressed, uncompressed)
            }
            Some(other) => invalid("rdb-envelope", self.offset_usize(), format!("unknown RDB string encoding {other}")),
        }
    }

    fn take_limited(&mut self, length: u64, limit: usize, purpose: &str) -> Result<Vec<u8>> {
        let length = usize_from(length, purpose, self.offset_usize())?;
        if length > limit {
            return invalid(
                "resource-limit",
                self.offset_usize(),
                format!("identity chunk requests {length} bytes, limit is {limit}"),
            );
        }
        let mut bytes = vec![0; length];
        read_exact_at(self.reader, &mut bytes, self.offset)?;
        self.offset += length as u64;
        Ok(bytes)
    }
}

fn read_exact_at<R: Read>(reader: &mut R, bytes: &mut [u8], offset: u64) -> Result<()> {
    reader.read_exact(bytes).map_err(|source| DecodeError::Io { offset, source })
}

struct Envelope {
    rdb_version: u16,
    encoding_version: u16,
    chunks: Vec<Vec<u8>>,
}

fn decode_envelope(data: &[u8]) -> Result<Envelope> {
    if data.len() < 12 {
        return truncated("rdb-envelope", 0, 12, data.len());
    }
    let footer = data.len() - 10;
    let rdb_version = u16::from_le_bytes(data[footer..footer + 2].try_into().unwrap());
    if rdb_version != SUPPORTED_RDB_VERSION {
        return invalid(
            "rdb-envelope",
            footer,
            format!("RDB version {rdb_version}, expected {SUPPORTED_RDB_VERSION}"),
        );
    }
    let expected_crc = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
    let actual_crc = crc64_jones(&data[..data.len() - 8]);
    if actual_crc != expected_crc {
        return invalid(
            "rdb-envelope",
            data.len() - 8,
            format!("CRC64 mismatch: expected {expected_crc:016x}, calculated {actual_crc:016x}"),
        );
    }
    let mut cursor = ByteCursor::new(&data[..footer], "rdb-envelope");
    if cursor.byte()? != REDIS_MODULE_2 {
        return invalid("rdb-envelope", 0, "object is not MODULE_2");
    }
    let (module_id, encoding) = cursor.length()?;
    if encoding.is_some() {
        return invalid("rdb-envelope", cursor.position(), "module id uses a special string encoding");
    }
    let name_bits = module_id >> 10;
    const CHARSET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut module = String::with_capacity(9);
    for k in 0..9 {
        module.push(CHARSET[((name_bits >> (6 * (8 - k))) & 63) as usize] as char);
    }
    let encoding_version = (module_id & 1023) as u16;
    if module != GRAPH_MODULE || encoding_version != SUPPORTED_ENCODING_VERSION {
        return invalid(
            "rdb-envelope",
            cursor.position(),
            format!("module {module} encoding {encoding_version}, expected {GRAPH_MODULE} {SUPPORTED_ENCODING_VERSION}"),
        );
    }
    let mut chunks = Vec::new();
    loop {
        let (opcode, special) = cursor.length()?;
        if special.is_some() {
            return invalid("rdb-envelope", cursor.position(), "module opcode uses a special encoding");
        }
        match opcode {
            MODULE_OPCODE_EOF => break,
            MODULE_OPCODE_STRING => chunks.push(cursor.string()?),
            other => return invalid("rdb-envelope", cursor.position(), format!("unsupported module opcode {other}")),
        }
    }
    if cursor.remaining() != 0 {
        return invalid(
            "rdb-envelope",
            cursor.position(),
            format!("{} trailing bytes after module EOF", cursor.remaining()),
        );
    }
    Ok(Envelope {
        rdb_version,
        encoding_version,
        chunks,
    })
}

struct ByteCursor<'a> {
    data: &'a [u8],
    position: usize,
    layer: &'static str,
}

impl<'a> ByteCursor<'a> {
    fn new(data: &'a [u8], layer: &'static str) -> Self {
        Self { data, position: 0, layer }
    }
    fn position(&self) -> usize {
        self.position
    }
    fn remaining(&self) -> usize {
        self.data.len() - self.position
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        if count > self.remaining() {
            return truncated(self.layer, self.position, count, self.remaining());
        }
        let start = self.position;
        self.position += count;
        Ok(&self.data[start..self.position])
    }
    fn length(&mut self) -> Result<(u64, Option<u8>)> {
        let first = self.byte()?;
        match first >> 6 {
            0 => Ok(((first & 0x3f) as u64, None)),
            1 => Ok(((((first & 0x3f) as u64) << 8) | self.byte()? as u64, None)),
            2 => match first {
                0x80 => Ok((u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64, None)),
                0x81 => Ok((u64::from_be_bytes(self.take(8)?.try_into().unwrap()), None)),
                _ => invalid(self.layer, self.position - 1, format!("invalid length marker {first:#x}")),
            },
            _ => Ok((0, Some(first & 0x3f))),
        }
    }
    fn string(&mut self) -> Result<Vec<u8>> {
        let (length, special) = self.length()?;
        match special {
            None => Ok(self.take(usize_from(length, "RDB string length", self.position)?)?.to_vec()),
            Some(0) => Ok(i8::from_le_bytes(self.take(1)?.try_into().unwrap()).to_string().into_bytes()),
            Some(1) => Ok(i16::from_le_bytes(self.take(2)?.try_into().unwrap()).to_string().into_bytes()),
            Some(2) => Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()).to_string().into_bytes()),
            Some(3) => {
                let (compressed, c_special) = self.length()?;
                let (uncompressed, u_special) = self.length()?;
                if c_special.is_some() || u_special.is_some() {
                    return invalid(self.layer, self.position, "LZF lengths use special encoding");
                }
                let compressed = self.take(usize_from(compressed, "LZF compressed length", self.position)?)?;
                lzf_decompress(compressed, usize_from(uncompressed, "LZF output length", self.position)?)
            }
            Some(other) => invalid(self.layer, self.position, format!("unknown RDB string encoding {other}")),
        }
    }
}

struct TaggedStream {
    chunks: Vec<Vec<u8>>,
    chunk: usize,
    position: usize,
    /// The bytes of every chunk before `chunk`, a taken blob's included.
    absolute: usize,
    /// The bytes of every chunk, so the bytes left are known without
    /// rescanning the chunks on each of the millions of count checks.
    total: usize,
}

impl TaggedStream {
    fn new(chunks: Vec<Vec<u8>>) -> Self {
        let total = chunks.iter().map(Vec::len).sum();
        Self {
            chunks,
            chunk: 0,
            position: 0,
            absolute: 0,
            total,
        }
    }
    fn offset(&self) -> usize {
        self.absolute
    }
    fn remaining_bytes(&self) -> usize {
        self.total.saturating_sub(self.absolute + self.position)
    }
    fn collection_count(&self, declared: u64, minimum_encoded_bytes: usize, purpose: &'static str) -> Result<usize> {
        self.collection_count_with_limit(declared, minimum_encoded_bytes, purpose, MAX_COLLECTION_ITEMS)
    }
    fn metadata_count(&self, declared: u64, minimum_encoded_bytes: usize, purpose: &'static str) -> Result<usize> {
        self.collection_count_with_limit(declared, minimum_encoded_bytes, purpose, MAX_METADATA_ITEMS)
    }
    fn collection_count_with_limit(
        &self,
        declared: u64,
        minimum_encoded_bytes: usize,
        purpose: &'static str,
        resource_limit: usize,
    ) -> Result<usize> {
        let count = usize_from(declared, purpose, self.offset())?;
        let input_limit = self.remaining_bytes() / minimum_encoded_bytes.max(1);
        if count > input_limit || count > resource_limit {
            return invalid(
                "typed-stream",
                self.offset(),
                format!("{purpose} {count} exceeds input-derived limit {input_limit} or resource limit {resource_limit}"),
            );
        }
        Ok(count)
    }
    fn ensure_chunk(&mut self) -> Result<()> {
        while self.chunk < self.chunks.len() && self.position == self.chunks[self.chunk].len() {
            self.absolute += self.chunks[self.chunk].len();
            self.chunk += 1;
            self.position = 0;
        }
        if self.chunk == self.chunks.len() {
            return truncated("typed-stream", self.absolute, 1, 0);
        }
        Ok(())
    }
    fn tag(&mut self, expected: u8) -> Result<()> {
        self.ensure_chunk()?;
        let offset = self.offset() + self.position;
        let actual = self.chunks[self.chunk][self.position];
        self.position += 1;
        if actual != expected {
            return invalid("typed-stream", offset, format!("tag {actual}, expected {expected}"));
        }
        Ok(())
    }
    fn fixed<const N: usize>(&mut self, tag: u8) -> Result<[u8; N]> {
        self.tag(tag)?;
        let remaining = self.chunks[self.chunk].len() - self.position;
        if remaining < N {
            return truncated("typed-stream", self.offset() + self.position, N, remaining);
        }
        let value = self.chunks[self.chunk][self.position..self.position + N].try_into().unwrap();
        self.position += N;
        Ok(value)
    }
    fn unsigned(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.fixed::<8>(4)?))
    }
    fn signed(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.fixed::<8>(3)?))
    }
    fn float(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.fixed::<8>(2)?))
    }
    fn bytes(&mut self) -> Result<Vec<u8>> {
        self.ensure_chunk()?;
        let offset = self.offset() + self.position;
        let tag = self.chunks[self.chunk][self.position];
        self.position += 1;
        if tag == 6 {
            if self.position != self.chunks[self.chunk].len() {
                return invalid("typed-stream", offset, "blob marker is not at end of chunk");
            }
            self.absolute += self.chunks[self.chunk].len();
            self.chunk += 1;
            self.position = 0;
            if self.chunk == self.chunks.len() {
                return truncated("typed-stream", self.absolute, 1, 0);
            }
            let result = std::mem::take(&mut self.chunks[self.chunk]);
            self.absolute += result.len();
            self.chunk += 1;
            Ok(result)
        } else if tag == 0 {
            let remaining = self.chunks[self.chunk].len() - self.position;
            if remaining < 8 {
                return truncated("typed-stream", self.offset() + self.position, 8, remaining);
            }
            let length = u64::from_le_bytes(self.chunks[self.chunk][self.position..self.position + 8].try_into().unwrap());
            self.position += 8;
            let length = usize_from(length, "typed byte length", offset)?;
            let remaining = self.chunks[self.chunk].len() - self.position;
            if remaining < length {
                return truncated("typed-stream", self.offset() + self.position, length, remaining);
            }
            let result = self.chunks[self.chunk][self.position..self.position + length].to_vec();
            self.position += length;
            Ok(result)
        } else {
            invalid("typed-stream", offset, format!("tag {tag}, expected bytes tag 0 or blob tag 6"))
        }
    }
    fn c_string(&mut self) -> Result<String> {
        let mut bytes = self.bytes()?;
        if bytes.pop() != Some(0) {
            return invalid("typed-stream", self.offset(), "string is not NUL terminated");
        }
        String::from_utf8(bytes).map_err(|error| DecodeError::Invalid {
            layer: "typed-stream",
            offset: self.offset(),
            message: format!("string is not UTF-8: {error}"),
        })
    }
    fn at_end(&mut self) -> Result<bool> {
        while self.chunk < self.chunks.len() && self.position == self.chunks[self.chunk].len() {
            self.absolute += self.chunks[self.chunk].len();
            self.chunk += 1;
            self.position = 0;
        }
        Ok(self.chunk == self.chunks.len())
    }
}

fn read_schema(stream: &mut TaggedStream) -> Result<Vec<Schema>> {
    let declared = stream.unsigned()?;
    let count = stream.metadata_count(declared, 36, "schema count")?;
    let mut schemas = allocated_vec(count, "schema table", stream.offset())?;
    for _ in 0..count {
        let id = stream.unsigned()?;
        let name = stream.c_string()?;
        let declared = stream.unsigned()?;
        let index_count = stream.metadata_count(declared, 28, "index count")?;
        for _ in 0..index_count {
            let _language = stream.c_string()?;
            let declared = stream.unsigned()?;
            let stopword_count = stream.metadata_count(declared, 10, "index stopword count")?;
            for _ in 0..stopword_count {
                let _ = stream.c_string()?;
            }
            let declared = stream.unsigned()?;
            // Every field has two strings and three fixed-width values. Vector
            // fields carry five additional values, but ordinary fields do not,
            // so their minimum valid encoding is 47 bytes rather than 55.
            let field_count = stream.metadata_count(declared, 47, "index field count")?;
            for _ in 0..field_count {
                let _field_name = stream.c_string()?;
                let field_type = stream.unsigned()?;
                let _weight = stream.float()?;
                let _nostem = stream.unsigned()?;
                let _phonetic = stream.c_string()?;
                if field_type & 0x10 != 0 {
                    for _ in 0..5 {
                        let _ = stream.unsigned()?;
                    }
                }
            }
        }
        let declared = stream.unsigned()?;
        let constraint_count = stream.metadata_count(declared, 18, "constraint count")?;
        for _ in 0..constraint_count {
            let _constraint_type = stream.unsigned()?;
            let declared = stream.unsigned()?;
            let attribute_count = stream.metadata_count(declared, 9, "constraint attribute count")?;
            for _ in 0..attribute_count {
                let _ = stream.unsigned()?;
            }
        }
        schemas.push(Schema {
            id,
            name,
            index_count,
            constraint_count,
        });
    }
    Ok(schemas)
}

fn read_entities(stream: &mut TaggedStream, count: u64, attributes: &[Arc<str>], output: &mut BTreeMap<u64, Entity>) -> Result<()> {
    let count = stream.collection_count(count, 18, "entity count")?;
    for _ in 0..count {
        let id = stream.unsigned()?;
        let declared = stream.unsigned()?;
        let property_count = stream.metadata_count(declared, 18, "property count")?;
        let mut properties = Vec::with_capacity(property_count);
        for _ in 0..property_count {
            let attribute = usize_from(stream.unsigned()?, "attribute id", stream.offset())?;
            let name = attributes.get(attribute).ok_or_else(|| DecodeError::Invalid {
                layer: "entity",
                offset: stream.offset(),
                message: format!("attribute id {attribute} outside table of {}", attributes.len()),
            })?;
            properties.push((Arc::clone(name), read_value(stream)?));
        }
        let properties = match Properties::from_entries(properties) {
            Ok(properties) => properties,
            Err(name) => return invalid("entity", stream.offset(), format!("entity {id} repeats property {name}")),
        };
        if output.insert(id, Entity { id, properties }).is_some() {
            return invalid("entity", stream.offset(), format!("entity id {id} appears twice"));
        }
    }
    Ok(())
}

fn read_value(stream: &mut TaggedStream) -> Result<PropertyValue> {
    let value_type = stream.unsigned()?;
    match value_type {
        SI_BOOL => Ok(PropertyValue::Bool(stream.signed()? != 0)),
        SI_INT64 => Ok(PropertyValue::Integer(stream.signed()?)),
        SI_DOUBLE => Ok(PropertyValue::Float(stream.float()?)),
        SI_STRING | SI_INTERN_STRING => {
            let mut value = stream.bytes()?;
            if value.pop() != Some(0) {
                return invalid("property", stream.offset(), "string property is not NUL terminated");
            }
            Ok(PropertyValue::String(String::from_utf8(value).map_err(|error| {
                DecodeError::Invalid {
                    layer: "property",
                    offset: stream.offset(),
                    message: format!("string property is not UTF-8: {error}"),
                }
            })?))
        }
        SI_ARRAY => {
            let declared = stream.unsigned()?;
            let count = stream.metadata_count(declared, 9, "array length")?;
            let mut values = allocated_vec(count, "property array", stream.offset())?;
            for _ in 0..count {
                values.push(read_value(stream)?);
            }
            Ok(PropertyValue::Array(values))
        }
        SI_POINT => Ok(PropertyValue::Point {
            latitude: stream.float()?,
            longitude: stream.float()?,
        }),
        SI_VECTOR_F32 => {
            let bytes = stream.bytes()?;
            if bytes.len() < 4 {
                return invalid("property", stream.offset(), "vector payload is shorter than dimension");
            }
            let count = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
            if bytes.len() != 4 + count * 4 {
                return invalid(
                    "property",
                    stream.offset(),
                    format!("vector dimension {count} carries {} bytes", bytes.len() - 4),
                );
            }
            let (chunks, remainder) = bytes[4..].as_chunks::<4>();
            debug_assert!(remainder.is_empty());
            let values = chunks.iter().map(|chunk| f32::from_le_bytes(*chunk)).collect();
            Ok(PropertyValue::VectorF32(values))
        }
        SI_DATETIME => Ok(PropertyValue::DateTime(stream.signed()?)),
        SI_DATE => Ok(PropertyValue::Date(stream.signed()?)),
        SI_TIME => Ok(PropertyValue::Time(stream.signed()?)),
        SI_DURATION => Ok(PropertyValue::Duration(stream.signed()?)),
        SI_NULL => Ok(PropertyValue::Null),
        SI_MAP => {
            let declared = stream.unsigned()?;
            let count = stream.metadata_count(declared, 19, "map length")?;
            let mut values = BTreeMap::new();
            for _ in 0..count {
                values.insert(stream.c_string()?, read_value(stream)?);
            }
            Ok(PropertyValue::Map(values))
        }
        other => invalid("property", stream.offset(), format!("unsupported SIValue type {other}")),
    }
}

#[derive(Clone)]
struct MatrixEntry {
    row: u64,
    col: u64,
    value: Option<u64>,
}
type DeltaEntries = Vec<MatrixEntry>;

struct Matrix {
    rows: u64,
    cols: u64,
    values: u64,
    format: i32,
    orientation: i32,
    iso: bool,
    vectors: BTreeMap<&'static str, Vector>,
}
struct Vector {
    bytes: Vec<u8>,
    kind: String,
    entries: u64,
}

fn read_vector(stream: &mut TaggedStream) -> Result<Vector> {
    let bytes = stream.bytes()?;
    let kind = stream.c_string()?;
    let entries = stream.unsigned()?;
    let declared_bytes = usize_from(stream.unsigned()?, "vector byte count", stream.offset())?;
    let _handling = stream.signed()?;
    if bytes.len() != declared_bytes {
        return invalid(
            "graphblas-vector",
            stream.offset(),
            format!("{} bytes, declares {declared_bytes}", bytes.len()),
        );
    }
    Ok(Vector { bytes, kind, entries })
}

fn read_matrix(stream: &mut TaggedStream) -> Result<Matrix> {
    let container = stream.bytes()?;
    if container.len() != 608 {
        return invalid(
            "graphblas-matrix",
            stream.offset(),
            format!("container is {} bytes, expected 608", container.len()),
        );
    }
    let rows = le_u64(&container, 0, "graphblas-matrix")?;
    let cols = le_u64(&container, 8, "graphblas-matrix")?;
    let values = le_u64(&container, 32, "graphblas-matrix")?;
    let format = le_i32(&container, 128, "graphblas-matrix")?;
    let orientation = le_i32(&container, 132, "graphblas-matrix")?;
    if !matches!(format, 1 | 2 | 4 | 8) {
        return invalid("graphblas-matrix", 128, format!("unknown format {format}"));
    }
    if orientation != 0 {
        return invalid(
            "graphblas-matrix",
            132,
            format!("column-major orientation {orientation} is unsupported"),
        );
    }
    // `GxB_Container_struct` stores 16 64-bit words, 16 32-bit words,
    // 16 vector pointers, and 16 matrix pointers before its boolean fields.
    // That puts `iso` at byte 448. Byte 480 is already in `void_future`; using
    // it makes ISO matrices look non-ISO and assigns unrelated edge IDs to
    // relationship cells when their single stored value is expanded.
    let iso = container[448] != 0;
    let mut vectors = BTreeMap::new();
    for name in ["x", "h", "p", "i", "b"] {
        vectors.insert(name, read_vector(stream)?);
    }
    Ok(Matrix {
        rows,
        cols,
        values,
        format,
        orientation,
        iso,
        vectors,
    })
}

fn read_delta(stream: &mut TaggedStream, with_values: bool, diagnostics: &mut DecodeDiagnostics) -> Result<DeltaEntries> {
    let base = matrix_entries(&read_matrix(stream)?, with_values)?;
    let inserts = matrix_entries(&read_matrix(stream)?, with_values)?;
    let deletes = matrix_entries(&read_matrix(stream)?, false)?;
    let delete_keys: BTreeSet<_> = deletes.iter().map(|e| (e.row, e.col)).collect();
    let insert_keys: BTreeSet<_> = inserts.iter().map(|e| (e.row, e.col)).collect();
    let base_keys: BTreeSet<_> = base.iter().map(|e| (e.row, e.col)).collect();
    diagnostics.delta_delete_missing_from_base += delete_keys.difference(&base_keys).count() as u64;
    diagnostics.delta_insert_overlaps_base += insert_keys.intersection(&base_keys).count() as u64;
    diagnostics.delta_insert_overlaps_delete += insert_keys.intersection(&delete_keys).count() as u64;
    let mut effective: BTreeMap<(u64, u64), MatrixEntry> = base
        .into_iter()
        .filter(|e| !delete_keys.contains(&(e.row, e.col)))
        .map(|e| ((e.row, e.col), e))
        .collect();
    for entry in inserts {
        effective.insert((entry.row, entry.col), entry);
    }
    Ok(effective.into_values().collect())
}

fn matrix_entries(matrix: &Matrix, with_values: bool) -> Result<Vec<MatrixEntry>> {
    debug_assert_eq!(matrix.orientation, 0);
    if matrix.values == 0 {
        return Ok(Vec::new());
    }
    let x = if with_values {
        Some(vector_u64(matrix.vectors.get("x").unwrap(), "values")?)
    } else {
        None
    };
    let value_at = |position: usize, dense_position: usize| -> Result<Option<u64>> {
        let Some(values) = x.as_ref() else {
            return Ok(None);
        };
        let index = if matrix.iso {
            0
        } else if matches!(matrix.format, 1 | 2) {
            position
        } else {
            dense_position
        };
        values.get(index).copied().map(Some).ok_or_else(|| DecodeError::Invalid {
            layer: "graphblas-matrix",
            offset: 0,
            message: format!("value index {index} outside {} entries", values.len()),
        })
    };
    let value_count = usize_from(matrix.values, "matrix values", 0)?;
    let mut output = allocated_vec(value_count, "matrix entries", 0)?;
    match matrix.format {
        1 | 2 => {
            let pointers = vector_indices(matrix.vectors.get("p").unwrap())?;
            let indices = vector_indices(matrix.vectors.get("i").unwrap())?;
            let hypers = if matrix.format == 1 {
                Some(vector_indices(matrix.vectors.get("h").unwrap())?)
            } else {
                None
            };
            let vector_count = match hypers.as_ref() {
                Some(hypers) => hypers.len(),
                None => usize_from(matrix.rows, "matrix rows", 0)?,
            };
            let required_pointers = vector_count.checked_add(1).ok_or_else(|| DecodeError::Invalid {
                layer: "graphblas-matrix",
                offset: 0,
                message: "matrix vector count overflows".into(),
            })?;
            if pointers.len() < required_pointers || pointers[vector_count] != matrix.values || indices.len() < value_count {
                return invalid("graphblas-matrix", 0, "sparse arrays disagree with nvals");
            }
            let used_pointers = &pointers[..=vector_count];
            if used_pointers.first().copied() != Some(0)
                || used_pointers.iter().any(|pointer| *pointer > matrix.values)
                || used_pointers.windows(2).any(|pair| pair[0] > pair[1])
            {
                return invalid("graphblas-matrix", 0, "sparse pointers are not ordered within nvals");
            }
            if hypers.as_ref().is_some_and(|rows| rows.iter().any(|row| *row >= matrix.rows))
                || indices[..value_count].iter().any(|column| *column >= matrix.cols)
            {
                return invalid("graphblas-matrix", 0, "sparse index is outside matrix dimensions");
            }
            for vector in 0..vector_count {
                let row = hypers.as_ref().map_or(vector as u64, |h| h[vector]);
                let start = pointers[vector] as usize;
                let end = pointers[vector + 1] as usize;
                for (offset, col) in indices[start..end].iter().copied().enumerate() {
                    let position = start + offset;
                    output.push(MatrixEntry {
                        row,
                        col,
                        value: value_at(position, position)?,
                    });
                }
            }
        }
        4 | 8 => {
            let cells = matrix.rows.checked_mul(matrix.cols).ok_or_else(|| DecodeError::Invalid {
                layer: "graphblas-matrix",
                offset: 0,
                message: "matrix dimensions overflow".into(),
            })?;
            let bitmap = matrix.vectors.get("b").unwrap();
            for dense in 0..usize_from(cells, "matrix cells", 0)? {
                let present = matrix.format == 8 || bitmap.bytes.get(dense).copied().unwrap_or(0) != 0;
                if present {
                    output.push(MatrixEntry {
                        row: dense as u64 / matrix.cols,
                        col: dense as u64 % matrix.cols,
                        value: value_at(output.len(), dense)?,
                    });
                }
            }
            if output.len() as u64 != matrix.values {
                return invalid(
                    "graphblas-matrix",
                    0,
                    format!("dense/bitmap yielded {} entries, declares {}", output.len(), matrix.values),
                );
            }
        }
        _ => unreachable!(),
    }
    Ok(output)
}

fn vector_indices(vector: &Vector) -> Result<Vec<u64>> {
    match vector.kind.as_str() {
        "GrB_UINT32" => parse_chunks(&vector.bytes, vector.entries, 4, |b| {
            u32::from_le_bytes(b.try_into().unwrap()) as u64
        }),
        "GrB_INT32" => parse_chunks(&vector.bytes, vector.entries, 4, |b| {
            i32::from_le_bytes(b.try_into().unwrap()) as i64 as u64
        }),
        "GrB_UINT64" => parse_chunks(&vector.bytes, vector.entries, 8, |b| u64::from_le_bytes(b.try_into().unwrap())),
        "GrB_INT64" => parse_chunks(&vector.bytes, vector.entries, 8, |b| {
            i64::from_le_bytes(b.try_into().unwrap()) as u64
        }),
        other => invalid("graphblas-vector", 0, format!("unsupported index type {other}")),
    }
}

fn vector_u64(vector: &Vector, purpose: &str) -> Result<Vec<u64>> {
    match vector.kind.as_str() {
        "GrB_UINT64" => parse_chunks(&vector.bytes, vector.entries, 8, |b| u64::from_le_bytes(b.try_into().unwrap())),
        "GrB_BOOL" => parse_chunks(&vector.bytes, vector.entries, 1, |b| b[0] as u64),
        other => invalid("graphblas-vector", 0, format!("unsupported {purpose} type {other}")),
    }
}

fn parse_chunks<F>(bytes: &[u8], entries: u64, width: usize, convert: F) -> Result<Vec<u64>>
where
    F: Fn(&[u8]) -> u64,
{
    let count = usize_from(entries, "vector entries", 0)?;
    let need = count.checked_mul(width).ok_or_else(|| DecodeError::Invalid {
        layer: "graphblas-vector",
        offset: 0,
        message: "vector byte length overflow".into(),
    })?;
    if bytes.len() < need {
        return truncated("graphblas-vector", 0, need, bytes.len());
    }
    if count > MAX_COLLECTION_ITEMS {
        return invalid(
            "resource-limit",
            0,
            format!("vector entries {count} exceed resource limit {MAX_COLLECTION_ITEMS}"),
        );
    }
    let mut values = allocated_vec(count, "GraphBLAS vector", 0)?;
    values.extend(bytes[..need].chunks_exact(width).map(convert));
    Ok(values)
}

fn read_tensors(stream: &mut TaggedStream) -> Result<BTreeMap<(u64, u64), Vec<u64>>> {
    let total = stream.unsigned()?;
    if total == 0 {
        return Ok(BTreeMap::new());
    }
    let mut tensors = BTreeMap::new();
    for _ in 0..2 {
        let declared = stream.unsigned()?;
        let tensor_count = stream.collection_count(declared, 27, "tensor count")?;
        for _ in 0..tensor_count {
            let row = stream.unsigned()?;
            let col = stream.unsigned()?;
            let ids = tensor_ids(&stream.bytes()?)?;
            if tensors.insert((row, col), ids).is_some() {
                return invalid("tensor", stream.offset(), format!("duplicate tensor at ({row},{col})"));
            }
        }
    }
    if tensors.len() as u64 != total {
        return invalid("tensor", stream.offset(), format!("{} tensors, declares {total}", tensors.len()));
    }
    Ok(tensors)
}

fn tensor_ids(blob: &[u8]) -> Result<Vec<u64>> {
    const HEADER: usize = 160;
    if blob.len() < HEADER {
        return truncated("tensor", 0, HEADER, blob.len());
    }
    let declared = le_u64(blob, 0, "tensor")?;
    if declared != blob.len() as u64 {
        return invalid("tensor", 0, format!("declares {declared} bytes, holds {}", blob.len()));
    }
    let encoding = le_u32(blob, 8, "tensor")?;
    let index_is_u32 = ((encoding >> 4) & 0xf) != 0;
    let nvals = usize_from(
        le_i64(blob, 48, "tensor")?.try_into().map_err(|_| DecodeError::Invalid {
            layer: "tensor",
            offset: 48,
            message: "negative nvals".into(),
        })?,
        "tensor nvals",
        48,
    )?;
    let mut lengths = [0usize; 5];
    for (index, length) in lengths.iter_mut().enumerate() {
        let value = le_i64(blob, 64 + index * 8, "tensor")?;
        *length = usize::try_from(value).map_err(|_| DecodeError::Invalid {
            layer: "tensor",
            offset: 64 + index * 8,
            message: "negative component length".into(),
        })?;
    }
    let mut block_counts = [0usize; 5];
    let mut methods = [0i32; 5];
    for index in 0..5 {
        block_counts[index] = usize::try_from(le_i32(blob, 120 + index * 8, "tensor")?).map_err(|_| DecodeError::Invalid {
            layer: "tensor",
            offset: 120 + index * 8,
            message: "negative block count".into(),
        })?;
        methods[index] = le_i32(blob, 124 + index * 8, "tensor")?;
    }
    let mut position = HEADER;
    let mut ends = Vec::with_capacity(5);
    for &count in &block_counts {
        if count > blob.len().saturating_sub(position) / 8 || count > MAX_COLLECTION_ITEMS {
            return invalid(
                "tensor",
                position,
                format!("block count {count} exceeds remaining tensor bytes or resource limit"),
            );
        }
        let mut component = allocated_vec(count, "tensor block table", position)?;
        for _ in 0..count {
            component.push(le_u64(blob, position, "tensor")? as usize);
            position += 8;
        }
        ends.push(component);
    }
    let mut ai = Vec::new();
    for index in 0..5 {
        let compressed_length = ends[index].last().copied().unwrap_or(0);
        if position.checked_add(compressed_length).is_none_or(|end| end > blob.len()) {
            return truncated("tensor", position, compressed_length, blob.len().saturating_sub(position));
        }
        let raw = &blob[position..position + compressed_length];
        position += compressed_length;
        if index != 3 {
            continue;
        }
        ai = decode_tensor_component(raw, lengths[index], &ends[index], methods[index])?;
    }
    let trailer = &blob[position..];
    let parts: Vec<_> = trailer.split(|byte| *byte == 0).collect();
    if parts.len() != 3 || !parts[2].is_empty() || !matches!(parts[1], b"" | b"GrB_BOOL" | b"GrB_UINT64") {
        return invalid("tensor", position, "unexpected tensor trailer");
    }
    let width = if index_is_u32 { 4 } else { 8 };
    if ai.len() % width != 0 {
        return invalid("tensor", 0, "Ai component is misaligned");
    }
    let mut ids = if index_is_u32 {
        ai.as_chunks::<4>()
            .0
            .iter()
            .map(|bytes| u32::from_le_bytes(*bytes) as u64)
            .collect::<Vec<_>>()
    } else {
        ai.as_chunks::<8>()
            .0
            .iter()
            .map(|bytes| u64::from_le_bytes(*bytes))
            .collect::<Vec<_>>()
    };
    if ids.len() < nvals {
        return invalid("tensor", 0, format!("Ai has {} ids, nvals is {nvals}", ids.len()));
    }
    ids.truncate(nvals);
    // An empty list is a multi-edge cell whose edges were all deleted
    // (EmptyTensorCell); relation_cells reports it.
    if ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return invalid("tensor", 0, "tensor ids are not strictly ascending");
    }
    Ok(ids)
}

fn decode_tensor_component(raw: &[u8], expected: usize, ends: &[usize], method: i32) -> Result<Vec<u8>> {
    let output = if ends.is_empty() {
        Vec::new()
    } else if method == -1 {
        raw.get(..expected)
            .ok_or(DecodeError::Truncated {
                layer: "tensor",
                offset: 0,
                need: expected,
                have: raw.len(),
            })?
            .to_vec()
    } else if (3000..4000).contains(&method) {
        let mut output = allocated_vec(expected, "decoded tensor component", 0)?;
        for block in 0..ends.len() {
            let start = if block == 0 { 0 } else { ends[block - 1] };
            let end = ends[block];
            if end > raw.len() || start > end {
                return invalid("tensor", start, "compressed block boundaries are invalid");
            }
            let expected_block = if block + 1 == ends.len() {
                expected - (block * expected / ends.len())
            } else {
                (block + 1) * expected / ends.len() - block * expected / ends.len()
            };
            let decoded = zstd::bulk::decompress(&raw[start..end], expected_block)?;
            if decoded.len() != expected_block {
                return invalid(
                    "tensor",
                    start,
                    format!("zstd block decoded to {} bytes, expected {expected_block}", decoded.len()),
                );
            }
            output.extend(decoded);
        }
        output
    } else {
        return invalid("tensor", 0, format!("unsupported compression method {method}"));
    };
    if output.len() != expected {
        return invalid(
            "tensor",
            0,
            format!("component decoded to {} bytes, expected {expected}", output.len()),
        );
    }
    Ok(output)
}

/// A relationship type's cells as one relationship per edge. A multi-edge cell
/// with no list is reported in `dangling` only when the type wrote no lists at
/// all, FalkorDB's drifted-counter case; a list missing from a type that wrote
/// lists is corruption. A multi-edge cell whose list is empty has no edges and
/// is reported in `empty`.
fn relation_cells(
    edge_type: &str,
    entries: DeltaEntries,
    tensors: BTreeMap<(u64, u64), Vec<u64>>,
    dangling: &mut Vec<DanglingTensorCell>,
    empty: &mut Vec<EmptyTensorCell>,
    diagnostics: &mut DecodeDiagnostics,
) -> Result<Vec<RelationshipCell>> {
    let mut output = Vec::new();
    let mut used = BTreeSet::new();
    for entry in entries {
        let value = entry.value.ok_or_else(|| DecodeError::Invalid {
            layer: "relation-matrix",
            offset: 0,
            message: "relationship matrix has no values".into(),
        })?;
        if value == MSB {
            diagnostics.delete_markers += 1;
            continue;
        }
        if value & MSB != 0 && tensors.is_empty() {
            dangling.push(DanglingTensorCell {
                edge_type: edge_type.to_string(),
                src: entry.row,
                dst: entry.col,
                value,
            });
            continue;
        }
        if value & MSB != 0 {
            let ids = tensors.get(&(entry.row, entry.col)).ok_or_else(|| DecodeError::Invalid {
                layer: "relation-matrix",
                offset: 0,
                message: format!("tensor cell ({},{}) has no blob", entry.row, entry.col),
            })?;
            used.insert((entry.row, entry.col));
            if ids.is_empty() {
                empty.push(EmptyTensorCell {
                    edge_type: edge_type.to_string(),
                    src: entry.row,
                    dst: entry.col,
                });
                continue;
            }
            for &edge_id in ids {
                output.push(RelationshipCell {
                    edge_type: edge_type.to_string(),
                    src: entry.row,
                    dst: entry.col,
                    edge_id,
                });
            }
        } else {
            output.push(RelationshipCell {
                edge_type: edge_type.to_string(),
                src: entry.row,
                dst: entry.col,
                edge_id: value,
            });
        }
    }
    if used.len() != tensors.len() {
        return invalid(
            "relation-matrix",
            0,
            format!("{} tensor blobs are unreferenced", tensors.len() - used.len()),
        );
    }
    Ok(output)
}

fn lzf_decompress(input: &[u8], expected: usize) -> Result<Vec<u8>> {
    let expansion_limit = input.len().saturating_mul(128);
    if expected > expansion_limit {
        return invalid(
            "resource-limit",
            0,
            format!(
                "LZF output declares {expected} bytes from {} input bytes, expansion limit is {expansion_limit}",
                input.len()
            ),
        );
    }
    // This is a byte buffer, not a collection of independently addressable
    // graph items. Large FalkorDB graphs legitimately contain encoded values
    // above MAX_COLLECTION_ITEMS bytes; retain the byte-allocation ceiling and
    // the 128x expansion bound without applying the item-count ceiling.
    let mut output = allocated_byte_vec(expected, "LZF output", 0)?;
    let mut position = 0;
    while position < input.len() {
        let control = input[position];
        position += 1;
        if control < 32 {
            let length = control as usize + 1;
            if position + length > input.len() {
                return truncated("lzf", position, length, input.len() - position);
            }
            output.extend_from_slice(&input[position..position + length]);
            position += length;
        } else {
            let mut length = (control >> 5) as usize;
            if length == 7 {
                if position == input.len() {
                    return truncated("lzf", position, 1, 0);
                }
                length += input[position] as usize;
                position += 1;
            }
            if position == input.len() {
                return truncated("lzf", position, 1, 0);
            }
            let distance = (((control & 0x1f) as usize) << 8) + input[position] as usize + 1;
            position += 1;
            length += 2;
            if distance > output.len() {
                return invalid("lzf", position, "back-reference precedes output");
            }
            for _ in 0..length {
                let value = output[output.len() - distance];
                output.push(value);
            }
        }
        if output.len() > expected {
            return invalid("lzf", position, "output exceeds declared length");
        }
    }
    if output.len() != expected {
        return invalid("lzf", position, format!("decoded {} bytes, expected {expected}", output.len()));
    }
    Ok(output)
}

pub fn crc64_jones(data: &[u8]) -> u64 {
    crc64_jones_update(0, data)
}

fn crc64_jones_update(mut crc: u64, data: &[u8]) -> u64 {
    for &byte in data {
        crc ^= byte as u64;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x95ac_9329_ac4b_c9b5
            } else {
                crc >> 1
            };
        }
    }
    crc
}

fn le_u32(data: &[u8], offset: usize, layer: &'static str) -> Result<u32> {
    Ok(u32::from_le_bytes(slice::<4>(data, offset, layer)?))
}
fn le_i32(data: &[u8], offset: usize, layer: &'static str) -> Result<i32> {
    Ok(i32::from_le_bytes(slice::<4>(data, offset, layer)?))
}
fn le_u64(data: &[u8], offset: usize, layer: &'static str) -> Result<u64> {
    Ok(u64::from_le_bytes(slice::<8>(data, offset, layer)?))
}
fn le_i64(data: &[u8], offset: usize, layer: &'static str) -> Result<i64> {
    Ok(i64::from_le_bytes(slice::<8>(data, offset, layer)?))
}
fn slice<const N: usize>(data: &[u8], offset: usize, layer: &'static str) -> Result<[u8; N]> {
    if offset.checked_add(N).is_none_or(|end| end > data.len()) {
        return truncated(layer, offset, N, data.len().saturating_sub(offset));
    }
    Ok(data[offset..offset + N].try_into().unwrap())
}
fn usize_from(value: u64, what: &str, offset: usize) -> Result<usize> {
    usize::try_from(value).map_err(|_| DecodeError::Invalid {
        layer: "bounds",
        offset,
        message: format!("{what} {value} does not fit usize"),
    })
}
fn allocated_vec<T>(count: usize, purpose: &'static str, offset: usize) -> Result<Vec<T>> {
    let allocation_bytes = count.checked_mul(size_of::<T>()).ok_or_else(|| DecodeError::Invalid {
        layer: "resource-limit",
        offset,
        message: format!("{purpose} allocation size overflows"),
    })?;
    if count > MAX_COLLECTION_ITEMS || allocation_bytes > MAX_SINGLE_ALLOCATION_BYTES {
        return invalid(
            "resource-limit",
            offset,
            format!(
                "{purpose} requests {count} items ({allocation_bytes} bytes), limit is {MAX_COLLECTION_ITEMS} items or {MAX_SINGLE_ALLOCATION_BYTES} bytes"
            ),
        );
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|error| DecodeError::Invalid {
        layer: "resource-limit",
        offset,
        message: format!("cannot reserve {count} items for {purpose}: {error}"),
    })?;
    Ok(values)
}

fn allocated_byte_vec(count: usize, purpose: &'static str, offset: usize) -> Result<Vec<u8>> {
    if count > MAX_SINGLE_ALLOCATION_BYTES {
        return invalid(
            "resource-limit",
            offset,
            format!("{purpose} requests {count} bytes, limit is {MAX_SINGLE_ALLOCATION_BYTES} bytes"),
        );
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|error| DecodeError::Invalid {
        layer: "resource-limit",
        offset,
        message: format!("cannot reserve {count} bytes for {purpose}: {error}"),
    })?;
    Ok(values)
}

fn invalid<T>(layer: &'static str, offset: usize, message: impl Into<String>) -> Result<T> {
    Err(DecodeError::Invalid {
        layer,
        offset,
        message: message.into(),
    })
}
fn truncated<T>(layer: &'static str, offset: usize, need: usize, have: usize) -> Result<T> {
    Err(DecodeError::Truncated { layer, offset, need, have })
}
