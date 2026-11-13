//! Canonical key/value encoder for offline graph images.
//!
//! The encoder owns Turbolay's storage format while the caller owns external
//! sorting and SlateDB publication. It deliberately emits only durable graph
//! state; request idempotency receipts and topology changelog entries describe
//! online history and do not belong in a snapshot image. The edge-type catalog
//! (`meta/matrix_dirty`) is state, not history: vertex deletes find a vertex's
//! incident edges only under the edge types it lists.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::codec::{
    encode_edge_record, encode_relationship_record, encode_u64, encode_vertex_metadata, encode_vertex_property_value_key,
    validate_component,
};
use crate::{keys, EdgeRecord, GraphError, RelationshipId, RelationshipMutation, Result, VertexId, VertexMetadata, VertexPropertyValue};

/// One final key/value pair for a new SlateDB image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineImageRow {
    pub key: Bytes,
    pub value: Bytes,
}

impl OfflineImageRow {
    fn new(key: String, value: impl Into<Bytes>) -> Self {
        Self {
            key: Bytes::from(key),
            value: value.into(),
        }
    }
}

/// Stateful encoder for one cell's final graph state.
///
/// The aggregates hold an entry per relationship or endpoint, millions for a
/// large scope, so they name an edge type by its index in `edge_types` and
/// build their keys only when `finish_rows` emits them.
pub struct OfflineGraphImageEncoder {
    cell_id: String,
    write_reverse_index: bool,
    edge_types: Vec<String>,
    edge_type_indexes: BTreeMap<String, EdgeTypeIndex>,
    out_degree: BTreeMap<(EdgeTypeIndex, VertexId), u64>,
    in_degree: BTreeMap<(EdgeTypeIndex, VertexId), u64>,
    /// Relationships per structural edge: the first one writes the edge.
    relationship_counts: BTreeMap<(EdgeTypeIndex, VertexId, VertexId), u64>,
    /// The `id` merge-index entries by type, endpoints and encoded value; two
    /// relationships on one entry leave it unproven (`None`).
    merge_identities: BTreeMap<(EdgeTypeIndex, VertexId, VertexId, Box<str>), Option<RelationshipId>>,
    max_relationship_id: RelationshipId,
    /// The last edge `structural_edge_rows` encoded. Its calls arrive in
    /// strictly increasing (type name, src, dst) order, so comparing with the
    /// last one refuses a repeated edge without a set of every edge seen.
    last_structural_edge: Option<(EdgeTypeIndex, VertexId, VertexId)>,
}

type EdgeTypeIndex = u32;

impl OfflineGraphImageEncoder {
    /// Mark this collection as using ordinary user `id` properties. Publish once
    /// per image, including an empty collection, before serving it.
    pub fn byog_id_semantics_row() -> OfflineImageRow {
        OfflineImageRow::new(
            keys::CYPHER_ID_SEMANTICS.to_owned(),
            Bytes::from_static(keys::CYPHER_ID_PROPERTIES_V1),
        )
    }

    pub fn new(cell_id: impl Into<String>, write_reverse_index: bool) -> Result<Self> {
        let cell_id = cell_id.into();
        validate_component("cell_id", &cell_id)?;
        Ok(Self {
            cell_id,
            write_reverse_index,
            edge_types: Vec::new(),
            edge_type_indexes: BTreeMap::new(),
            out_degree: BTreeMap::new(),
            in_degree: BTreeMap::new(),
            relationship_counts: BTreeMap::new(),
            merge_identities: BTreeMap::new(),
            max_relationship_id: 0,
            last_structural_edge: None,
        })
    }

    /// Encode one vertex and all of its lookup indexes.
    pub fn vertex_rows(&self, vertex_id: VertexId, metadata: &VertexMetadata) -> Result<Vec<OfflineImageRow>> {
        for label in &metadata.labels {
            validate_component("label", label)?;
        }
        for property in metadata.properties.keys() {
            validate_component("property", property)?;
        }
        let mut rows = Vec::with_capacity(1 + metadata.labels.len() + metadata.properties.len());
        rows.push(OfflineImageRow::new(
            keys::vertex(&self.cell_id, vertex_id),
            encode_vertex_metadata(metadata),
        ));
        for label in &metadata.labels {
            rows.push(OfflineImageRow::new(
                keys::vertex_label(&self.cell_id, label, vertex_id),
                encode_u64(vertex_id),
            ));
        }
        for (property, value) in &metadata.properties {
            if !value.is_scalar_indexable() {
                continue;
            }
            rows.push(OfflineImageRow::new(
                keys::vertex_property_index(&self.cell_id, property, &encode_vertex_property_value_key(value), vertex_id),
                encode_u64(vertex_id),
            ));
        }
        Ok(rows)
    }

    /// Encode one relationship and its final-state indexes.
    pub fn relationship_rows(&mut self, relationship: &RelationshipMutation) -> Result<Vec<OfflineImageRow>> {
        let expected_identity = VertexPropertyValue::Integer(relationship.relationship_id);
        if relationship.metadata.properties.get("id") != Some(&expected_identity) {
            return Err(GraphError::CorruptValue {
                key: format!("offline-image/relationship/{}", relationship.relationship_id),
                reason: "relationship id property does not match its stable relationship id".to_string(),
            });
        }

        self.relationship_rows_preserving_properties(relationship)
    }

    /// Encode a generic relationship without assigning meaning to user properties.
    /// Internal IDs still drive topology, record keys and allocation watermarks;
    /// an optional user `id` is indexed only when its endpoint identity is unique.
    pub fn relationship_rows_preserving_properties(&mut self, relationship: &RelationshipMutation) -> Result<Vec<OfflineImageRow>> {
        if relationship.cell_id != self.cell_id {
            return Err(GraphError::CorruptValue {
                key: format!("offline-image/relationship/{}", relationship.relationship_id),
                reason: format!(
                    "relationship cell {} does not match image cell {}",
                    relationship.cell_id, self.cell_id
                ),
            });
        }
        validate_component("edge_type", &relationship.edge_type)?;
        if self.last_structural_edge.is_some() {
            return Err(self.mixed_edge_kinds());
        }
        crate::stored_relationship_id(relationship.relationship_id, || {
            format!("offline-image/relationship/{}", relationship.relationship_id)
        })?;
        for property in relationship.metadata.properties.keys() {
            validate_component("property", property)?;
        }

        self.max_relationship_id = self.max_relationship_id.max(relationship.relationship_id);
        let edge_type = self.edge_type_index(&relationship.edge_type)?;
        let count = self
            .relationship_counts
            .entry((edge_type, relationship.src, relationship.dst))
            .or_default();
        *count += 1;

        let mut rows = Vec::with_capacity(4 + relationship.metadata.properties.len());
        if *count == 1 {
            *self.out_degree.entry((edge_type, relationship.src)).or_default() += 1;
            if self.write_reverse_index {
                *self.in_degree.entry((edge_type, relationship.dst)).or_default() += 1;
            }
            let record = EdgeRecord {
                cell_id: self.cell_id.clone(),
                edge_type: relationship.edge_type.clone(),
                src: relationship.src,
                dst: relationship.dst,
            };
            let value = encode_edge_record(&record);
            rows.push(OfflineImageRow::new(
                keys::out_edge(&self.cell_id, &relationship.edge_type, relationship.src, relationship.dst),
                value.clone(),
            ));
            if self.write_reverse_index {
                rows.push(OfflineImageRow::new(
                    keys::in_edge(&self.cell_id, &relationship.edge_type, relationship.dst, relationship.src),
                    value,
                ));
            }
        }

        let record = crate::RelationshipRecord {
            cell_id: self.cell_id.clone(),
            edge_type: relationship.edge_type.clone(),
            src: relationship.src,
            dst: relationship.dst,
            relationship_id: relationship.relationship_id,
            metadata: relationship.metadata.clone(),
        };
        let record_key = keys::relationship(
            &self.cell_id,
            &relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship.relationship_id,
        );
        rows.push(OfflineImageRow::new(record_key.clone(), encode_relationship_record(&record)));
        rows.push(OfflineImageRow::new(
            keys::relationship_id(&self.cell_id, relationship.relationship_id),
            record_key,
        ));
        for (property, value) in &relationship.metadata.properties {
            if !value.is_scalar_indexable() {
                continue;
            }
            let encoded = encode_vertex_property_value_key(value);
            rows.push(OfflineImageRow::new(
                keys::relationship_property_index(
                    &self.cell_id,
                    &relationship.edge_type,
                    property,
                    &encoded,
                    relationship.src,
                    relationship.dst,
                    relationship.relationship_id,
                ),
                encode_u64(relationship.relationship_id),
            ));
            if property == "id" {
                self.merge_identities
                    .entry((edge_type, relationship.src, relationship.dst, encoded.into_boxed_str()))
                    .and_modify(|identity| *identity = None)
                    .or_insert(Some(relationship.relationship_id));
            }
        }
        Ok(rows)
    }

    /// Encode one structural edge as `write_edge_mutations_batch` writes a new
    /// one: the out and, with the reverse index, in adjacency entries, each an
    /// edge record, and the endpoints' degree counters, which `finish_rows`
    /// emits. A bolt `CREATE` of a relationship with no properties takes that
    /// path, so there is no relationship record, id or count, and an encoder
    /// that encodes structural edges refuses relationships, and the reverse.
    ///
    /// Calls must arrive in strictly increasing (type name, src, dst) order,
    /// key order, which refuses a repeated edge without holding a set of the
    /// edges seen.
    pub fn structural_edge_rows(&mut self, edge_type: &str, src: VertexId, dst: VertexId) -> Result<Vec<OfflineImageRow>> {
        validate_component("edge_type", edge_type)?;
        if !self.relationship_counts.is_empty() {
            return Err(self.mixed_edge_kinds());
        }
        let edge_type_index = self.edge_type_index(edge_type)?;
        if let Some((last_type, last_src, last_dst)) = self.last_structural_edge {
            let previous = (self.edge_types[last_type as usize].as_str(), last_src, last_dst);
            if previous >= (edge_type, src, dst) {
                return Err(GraphError::CorruptValue {
                    key: keys::out_edge(&self.cell_id, edge_type, src, dst),
                    reason: format!(
                        "structural edges must arrive in strictly increasing (type, src, dst) order; previous {}/{last_src}/{last_dst}",
                        previous.0
                    ),
                });
            }
        }
        self.last_structural_edge = Some((edge_type_index, src, dst));
        *self.out_degree.entry((edge_type_index, src)).or_default() += 1;
        let value = encode_edge_record(&EdgeRecord {
            cell_id: self.cell_id.clone(),
            edge_type: edge_type.to_string(),
            src,
            dst,
        });
        let mut rows = Vec::with_capacity(2);
        rows.push(OfflineImageRow::new(
            keys::out_edge(&self.cell_id, edge_type, src, dst),
            value.clone(),
        ));
        if self.write_reverse_index {
            *self.in_degree.entry((edge_type_index, dst)).or_default() += 1;
            rows.push(OfflineImageRow::new(keys::in_edge(&self.cell_id, edge_type, dst, src), value));
        }
        Ok(rows)
    }

    fn mixed_edge_kinds(&self) -> GraphError {
        GraphError::CorruptValue {
            key: format!("offline-image/cell/{}", self.cell_id),
            reason: "an offline image encodes either structural edges or relationships, never both".to_string(),
        }
    }

    /// Emit aggregate counters, proven-unique relationship identity indexes and
    /// the edge-type catalog.
    pub fn finish_rows(self) -> Vec<OfflineImageRow> {
        let edge_types = self.edge_types;
        let mut rows = Vec::with_capacity(
            edge_types.len()
                + self.out_degree.len()
                + self.in_degree.len()
                + self.relationship_counts.len()
                + self.merge_identities.len()
                + usize::from(self.max_relationship_id > 0),
        );
        let name = |edge_type: EdgeTypeIndex| edge_types[edge_type as usize].as_str();
        for ((edge_type, src), count) in self.out_degree {
            rows.push(OfflineImageRow::new(
                keys::degree_out(&self.cell_id, name(edge_type), src),
                encode_u64(count),
            ));
        }
        for ((edge_type, dst), count) in self.in_degree {
            rows.push(OfflineImageRow::new(
                keys::degree_in(&self.cell_id, name(edge_type), dst),
                encode_u64(count),
            ));
        }
        for ((edge_type, src, dst), count) in self.relationship_counts {
            rows.push(OfflineImageRow::new(
                keys::relationship_count(&self.cell_id, name(edge_type), src, dst),
                encode_u64(count),
            ));
        }
        for ((edge_type, src, dst, encoded), relationship_id) in self.merge_identities {
            if let Some(relationship_id) = relationship_id {
                rows.push(OfflineImageRow::new(
                    keys::relationship_merge_index(&self.cell_id, name(edge_type), "id", &encoded, src, dst),
                    encode_u64(relationship_id),
                ));
            }
        }
        if self.max_relationship_id > 0 {
            rows.push(OfflineImageRow::new(
                keys::last_relationship_id(&self.cell_id),
                encode_u64(self.max_relationship_id),
            ));
        }
        // Writers mark an edge type dirty at their commit sequence. Image rows
        // all carry sequence 0, so a generation built from the image covers 0
        // and the indexer leaves it current until a write dirties the type.
        for edge_type in &edge_types {
            rows.push(OfflineImageRow::new(keys::matrix_dirty(&self.cell_id, edge_type), encode_u64(0)));
        }
        rows
    }

    /// Fold in an encoder that encoded another set of source vertices, so
    /// several threads can encode one image. A structural edge belongs to its
    /// source vertex, so with disjoint sources only the in-degrees can meet,
    /// and they add; an out-degree, relationship count or merge identity both
    /// encoders hold means the sources overlapped, and is refused.
    pub fn absorb_disjoint_sources(&mut self, other: Self) -> Result<()> {
        if other.cell_id != self.cell_id || other.write_reverse_index != self.write_reverse_index {
            return Err(GraphError::CorruptValue {
                key: format!("offline-image/cell/{}", other.cell_id),
                reason: format!("an encoder for cell {} cannot absorb one for cell {}", self.cell_id, other.cell_id),
            });
        }
        let structural = (self.last_structural_edge.is_some(), other.last_structural_edge.is_some());
        let relationships = (!self.relationship_counts.is_empty(), !other.relationship_counts.is_empty());
        if (structural.0 && relationships.1) || (structural.1 && relationships.0) {
            return Err(self.mixed_edge_kinds());
        }
        let indexes = other
            .edge_types
            .iter()
            .map(|edge_type| self.edge_type_index(edge_type))
            .collect::<Result<Vec<_>>>()?;
        // The order guard continues from the later of the two last edges.
        if let Some((edge_type, src, dst)) = other.last_structural_edge {
            let theirs = (indexes[edge_type as usize], src, dst);
            let name = |(edge_type, src, dst): (EdgeTypeIndex, VertexId, VertexId)| (self.edge_types[edge_type as usize].clone(), src, dst);
            if self.last_structural_edge.is_none_or(|ours| name(ours) < name(theirs)) {
                self.last_structural_edge = Some(theirs);
            }
        }
        let overlap = |what: &str| GraphError::CorruptValue {
            key: format!("offline-image/{}/{what}", self.cell_id),
            reason: "absorbed encoder shares a source vertex with this one".to_string(),
        };
        for ((edge_type, src), count) in other.out_degree {
            if self.out_degree.insert((indexes[edge_type as usize], src), count).is_some() {
                return Err(overlap("degree_out"));
            }
        }
        for ((edge_type, src, dst), count) in other.relationship_counts {
            if self
                .relationship_counts
                .insert((indexes[edge_type as usize], src, dst), count)
                .is_some()
            {
                return Err(overlap("rel_count"));
            }
        }
        for ((edge_type, src, dst, encoded), identity) in other.merge_identities {
            if self
                .merge_identities
                .insert((indexes[edge_type as usize], src, dst, encoded), identity)
                .is_some()
            {
                return Err(overlap("rmerge_idx"));
            }
        }
        for ((edge_type, dst), count) in other.in_degree {
            *self.in_degree.entry((indexes[edge_type as usize], dst)).or_default() += count;
        }
        self.max_relationship_id = self.max_relationship_id.max(other.max_relationship_id);
        Ok(())
    }

    /// The index of an edge type, assigned on its first relationship.
    fn edge_type_index(&mut self, edge_type: &str) -> Result<EdgeTypeIndex> {
        if let Some(&index) = self.edge_type_indexes.get(edge_type) {
            return Ok(index);
        }
        let index = EdgeTypeIndex::try_from(self.edge_types.len()).map_err(|_| GraphError::CorruptValue {
            key: format!("offline-image/edge-type/{edge_type}"),
            reason: "an offline image holds more edge types than an index can name".to_string(),
        })?;
        self.edge_types.push(edge_type.to_string());
        self.edge_type_indexes.insert(edge_type.to_string(), index);
        Ok(index)
    }
}
