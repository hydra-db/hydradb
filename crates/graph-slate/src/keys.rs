use super::{RelationshipId, StorageSequence, VertexId};

pub fn cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/")
}

pub fn cell_drop_marker(cell_id: &str) -> String {
    format!("graph/drop/cell/{cell_id}")
}

pub fn cell_drop_pending_marker(cell_id: &str) -> String {
    format!("graph/drop/pending/{cell_id}")
}

pub fn cell_drop_idempotency(cell_id: &str, idempotency_key: &str) -> String {
    format!("graph/drop/idem/{cell_id}/{idempotency_key}")
}

/// Written once by `hydradb::GraphShard::seal_offline_image`; its value is
/// unused. Only its commit matters: it takes an image built offline past
/// storage sequence 0.
pub fn offline_image_sealed(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/offline_image_sealed")
}

/// What one `gds.graph.project` selected and counted, as JSON. The project
/// call writes it on the cell's writer, drop deletes it, and the algorithms
/// read it at their snapshot on whichever node they run.
#[cfg(feature = "opencypher")]
pub fn gds_projection(cell_id: &str, graph_name: &str) -> String {
    format!("cell/{cell_id}/meta/gds_projection/{graph_name}")
}

/// The cell's label membership version: the start sequence of the last
/// transaction that put or deleted a label index entry
/// ([`vertex_label`]), as a `u64`. Absent until the first such transaction
/// on this layout, which every reader sees alike. Readers that cache what
/// they derive from label membership key on it, so every node agrees on
/// when the members changed without scanning them.
pub fn vertex_label_version(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/vertex_label_version")
}

pub fn last_relationship_id(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/last_relationship_id")
}

pub fn matrix_dirty_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/matrix_dirty/")
}

pub fn matrix_dirty(cell_id: &str, edge_type: &str) -> String {
    format!("{}{edge_type}", matrix_dirty_prefix(cell_id))
}

pub fn adjacency_generation(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/meta/adjacency_generation/{edge_type}")
}

/// One edge-changelog entry: the sequence is part of the key so entries never
/// overwrite each other, and seq/src/dst zero-padding makes lexicographic
/// order equal commit order. The 1-byte value is the edge's existence *after*
/// the commit (final state, not operation), so last-in-sequence wins.
pub fn xlog_entry(cell_id: &str, edge_type: &str, sequence: StorageSequence, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/xlog/{edge_type}/{sequence:020}/{src:020}/{dst:020}")
}

pub fn xlog_type_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/xlog/{edge_type}/")
}

/// Lowest sequence whose xlog entries are still retained for the edge type —
/// the coverage floor an incremental build checks before trusting the range
/// scan. Written by the writer (first entry sets it, GC advances it).
pub fn xlog_low_water(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/meta/xlog_low/{edge_type}")
}

pub fn idempotency(cell_id: &str, operation: &str, idempotency_key: &str) -> String {
    format!("cell/{cell_id}/idem/{operation}/{idempotency_key}")
}

pub fn edge(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/edge/{edge_type}/{src:020}/{dst:020}")
}

pub fn out_edge(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/e/out/{edge_type}/{src:020}/{dst:020}")
}

pub fn in_edge(cell_id: &str, edge_type: &str, dst: VertexId, src: VertexId) -> String {
    format!("cell/{cell_id}/e/in/{edge_type}/{dst:020}/{src:020}")
}

pub fn out_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/e/out/{edge_type}/")
}

pub fn out_edge_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/e/out/")
}

pub fn in_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/e/in/{edge_type}/")
}

pub fn out_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
    format!("cell/{cell_id}/e/out/{edge_type}/{src:020}/")
}

pub fn out_segment(cell_id: &str, edge_type: &str, src: VertexId, storage_sequence: StorageSequence, segment_id: &str) -> String {
    format!("cell/{cell_id}/seg/out/{edge_type}/{src:020}/{storage_sequence:020}/{segment_id}")
}

pub fn out_segment_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/seg/out/{edge_type}/")
}

pub fn out_segment_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/seg/out/")
}

pub fn out_segment_src_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
    format!("cell/{cell_id}/seg/out/{edge_type}/{src:020}/")
}

pub fn out_segment_tombstone(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/seg/tomb/out/{edge_type}/{src:020}/{dst:020}")
}

pub fn out_segment_tombstone_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/seg/tomb/out/{edge_type}/")
}

pub fn out_segment_tombstone_src_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
    format!("cell/{cell_id}/seg/tomb/out/{edge_type}/{src:020}/")
}

pub fn in_prefix(cell_id: &str, edge_type: &str, dst: VertexId) -> String {
    format!("cell/{cell_id}/e/in/{edge_type}/{dst:020}/")
}

pub fn edge_metadata(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/emeta/{edge_type}/{src:020}/{dst:020}")
}

pub fn relationship(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId, relationship_id: RelationshipId) -> String {
    format!("cell/{cell_id}/rel/{edge_type}/{src:020}/{dst:020}/{relationship_id:020}")
}

pub fn relationship_edge_prefix(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/rel/{edge_type}/{src:020}/{dst:020}/")
}

pub fn relationship_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/rel/")
}

#[cfg(feature = "opencypher")]
pub fn relationship_source_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
    format!("cell/{cell_id}/rel/{edge_type}/{src:020}/")
}

/// Every edge-metadata key of one source over one type: [`edge_metadata`]
/// without its destination.
#[cfg(feature = "opencypher")]
pub fn edge_metadata_source_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
    format!("cell/{cell_id}/emeta/{edge_type}/{src:020}/")
}

pub fn relationship_id(cell_id: &str, relationship_id: RelationshipId) -> String {
    format!("cell/{cell_id}/rel_id/{relationship_id:020}")
}

pub fn relationship_count(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/rel_count/{edge_type}/{src:020}/{dst:020}")
}

/// Every parallel-relationship counter of one edge type in a cell.
#[cfg(feature = "opencypher")]
pub fn relationship_count_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/rel_count/{edge_type}/")
}

pub fn degree_out_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/cnt/out/{edge_type}/")
}

pub fn degree_in_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/cnt/in/{edge_type}/")
}

/// Every out-degree counter of a cell, across edge types.
pub fn degree_out_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/cnt/out/")
}

pub fn degree_out(cell_id: &str, edge_type: &str, src: VertexId) -> String {
    format!("cell/{cell_id}/cnt/out/{edge_type}/{src:020}")
}

pub fn degree_in(cell_id: &str, edge_type: &str, dst: VertexId) -> String {
    format!("cell/{cell_id}/cnt/in/{edge_type}/{dst:020}")
}

pub fn vertex(cell_id: &str, vertex_id: VertexId) -> String {
    format!("cell/{cell_id}/vertex/{vertex_id:020}")
}

pub fn vertex_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/vertex/")
}

pub fn vertex_label(cell_id: &str, label: &str, vertex_id: VertexId) -> String {
    format!("cell/{cell_id}/vlabel/{label}/{vertex_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_label_prefix(cell_id: &str, label: &str) -> String {
    format!("cell/{cell_id}/vlabel/{label}/")
}

/// Every label index entry of a cell, across labels.
pub fn vertex_label_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/vlabel/")
}

/// Every vertex property index entry of a cell, across properties.
pub fn vertex_property_index_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/vprop_idx/")
}

pub fn vertex_property_index(cell_id: &str, property: &str, encoded_value: &str, vertex_id: VertexId) -> String {
    format!(
        "cell/{cell_id}/vprop_idx/{property}/{}",
        vertex_property_index_entry_suffix(encoded_value, vertex_id)
    )
}

/// The `<encoded value>/<vertex id>` tail of a vertex property index key. An
/// ordered scan seeks to a position inside one property's range and builds it
/// from this, so the zero padding that makes vertex IDs sort numerically rather
/// than lexically lives in one place: a seek position built by hand to a
/// different width lands in the wrong run. The caller spells the value however
/// the key holds it -- a scan whose prefix already carries the type tag passes
/// the value without it.
pub fn vertex_property_index_entry_suffix(encoded_value: &str, vertex_id: VertexId) -> String {
    format!("{encoded_value}/{vertex_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_property_index_prefix(cell_id: &str, property: &str, encoded_value: &str) -> String {
    format!("cell/{cell_id}/vprop_idx/{property}/{encoded_value}/")
}

#[cfg(feature = "opencypher")]
pub fn vertex_property_index_property_prefix(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/vprop_idx/{property}/")
}

/// The entries holding one encoded value under a property prefix, whether
/// that prefix is the classic [`vertex_property_index_property_prefix`] or
/// one generation of the write-back index: the layouts agree below it.
#[cfg(feature = "opencypher")]
pub fn vertex_property_index_value_prefix(property_prefix: &str, encoded_value: &str) -> String {
    format!("{property_prefix}{encoded_value}/")
}

/// One GDS write-back value: the property's value for one vertex, outside
/// the vertex record. A write-back puts this key blind and the next run
/// overwrites it in place, so no run reads or rewrites the record and its
/// label and property index entries, which is what makes the write-back a
/// stream of puts rather than a read-merge-rewrite per vertex. Reads
/// overlay every key under [`vertex_write_back_property_prefix`] onto the
/// decoded record, the overlay winning, so the record's copy of a property
/// written before it moved here is shadowed rather than stale. A vertex
/// deleted between an algorithm's projection and its write-back leaves an
/// orphan value key: harmless, since the overlay is only applied to a vertex
/// whose record exists, and removed by the vertex delete when one runs.
pub fn vertex_write_back_property(cell_id: &str, vertex_id: VertexId, property: &str) -> String {
    format!("cell/{cell_id}/vprop/{vertex_id:020}/{property}")
}

/// Every write-back value of one vertex.
pub fn vertex_write_back_property_prefix(cell_id: &str, vertex_id: VertexId) -> String {
    format!("cell/{cell_id}/vprop/{vertex_id:020}/")
}

pub fn vertex_write_back_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/vprop/")
}

/// One write-back property index entry, under a per-run generation. Below
/// the generation the layout is the `<encoded value>/<vertex id>` tail of
/// [`vertex_property_index`], so an equality seek, a prefix seek and the
/// ordered walk read either index through the same positions once they
/// hold the right prefix. A generation per run instead of deleting each
/// vertex's previous entry: deleting needs the previous value, which means
/// a read per vertex, the very cost the write-back layout removes. The new
/// run writes its entries beside the old ones, one catalog key
/// ([`vertex_write_back_generation`]) switches readers over, and the old
/// generation is swept afterwards.
pub fn vertex_write_back_index(
    cell_id: &str,
    property: &str,
    generation: StorageSequence,
    encoded_value: &str,
    vertex_id: VertexId,
) -> String {
    format!(
        "cell/{cell_id}/vprop_gidx/{property}/{generation:020}/{}",
        vertex_property_index_entry_suffix(encoded_value, vertex_id)
    )
}

/// Every entry of one generation of a write-back property's index: what a
/// reader scans once the catalog names the generation, in the shape of
/// [`vertex_property_index_property_prefix`].
pub fn vertex_write_back_index_generation_prefix(cell_id: &str, property: &str, generation: StorageSequence) -> String {
    format!(
        "cell/{cell_id}/vprop_gidx/{property}/{}",
        vertex_write_back_index_generation_suffix(generation)
    )
}

/// The part of [`vertex_write_back_index_generation_prefix`] below
/// [`vertex_write_back_index_property_prefix`]: where a scan of the
/// property's generations starts to begin at `generation`.
pub fn vertex_write_back_index_generation_suffix(generation: StorageSequence) -> String {
    format!("{generation:020}/")
}

/// Every generation of one write-back property's index, oldest first.
pub fn vertex_write_back_index_property_prefix(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/vprop_gidx/{property}/")
}

/// Every write-back index entry of a cell, across properties and generations.
pub fn vertex_write_back_index_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/vprop_gidx/")
}

/// The catalog: which generation of a write-back property's index is
/// current, as a big-endian u64. Written once per run, after every entry of
/// the generation is committed, so a reader never sees a half-built
/// generation. Absent for a property the write-back never wrote, which
/// sends readers to the classic [`vertex_property_index`].
pub fn vertex_write_back_generation(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/vprop_cur/{property}")
}

/// Every catalog entry of a cell: the write-back properties and their
/// current generations.
pub fn vertex_write_back_generation_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/vprop_cur/")
}

/// The pending marker of a write-back property: the generation, as a
/// big-endian u64, of a run that has started writing values and index
/// entries and not yet switched the catalog. Value keys are not per
/// generation, so from a run's first chunk to its switch the property's
/// point reads and its index disagree; while this key exists, reads and
/// hand writes of the property are refused. Put in its own transaction
/// before the first chunk and deleted in the switch's, so the switch and
/// the clear are one commit. A run that stops between the two leaves it,
/// and the property stays refused until a later run completes: that run
/// replaces it with its own generation, clears it at its switch, and its
/// sweep removes the stopped run's entries, which lie below the new
/// generation.
///
/// What a read refuses is what it holds of the property: a value key, or a
/// copy in the vertex record. A vertex the run has not reached yet, holding
/// neither, reads the property as absent, as a GDS write read mid-run does
/// in Neo4j. Refusing it too needs the properties a query reads, which a
/// vertex read does not receive: both engines hydrate whole records.
pub fn vertex_write_back_pending(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/vprop_pending/{property}")
}

/// Every pending marker of a cell.
pub fn vertex_write_back_pending_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/vprop_pending/")
}

/// The sweep floor of a write-back property: the generation, as a
/// big-endian u64, below which no index entry remains, written by a sweep
/// once it has deleted everything below the generation it swept to. The
/// next sweep opens its scan at this generation instead of the property's
/// first key, so it never climbs over the tombstones of generations swept
/// before it. Absent until the first sweep completes, which reads as zero.
/// Only sweeps write it: the vertex delete path removes entries without
/// moving the floor, and a floor behind the truth costs a longer scan, never
/// a wrong one.
pub fn vertex_write_back_sweep_floor(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/vprop_swept/{property}")
}

/// The planner statistics of a write-back property, written by the
/// write-back in the same transaction as its catalog switch and describing
/// the generation that switch names: a reader never sees a catalog without
/// the statistics of the generation it points at, and a value whose
/// recorded generation differs from the catalog's is stale and ignored. The
/// write-back holds every value it writes, so it is the one producer that
/// can count the property exactly without reading anything; the indexer's
/// object, built from a reader's pinned view, may describe a store from
/// before the property existed. The value is one planner statistics object
/// holding only this property (`encode_planner_statistics`), its
/// `sequence` header the generation.
///
/// The sweep and the vertex delete path leave it alone; a cell drop leaves
/// it too, as it leaves [`vertex_write_back_generation`] and
/// [`vertex_write_back_sweep_floor`]. A hand `SET` of the property moves
/// its value key and index entry but not these counts, which then drift by
/// the number of such writes until the next run rewrites them.
#[cfg(feature = "opencypher")]
pub fn vertex_write_back_statistics(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/vprop_stats/{property}")
}

/// The `(property, generation, encoded value, vertex id)` of a
/// [`vertex_write_back_index`] key, or `None` for any other key.
pub fn parse_vertex_write_back_index_key(key: &str) -> Option<(String, StorageSequence, String, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", _cell_id, "vprop_gidx", property, generation, encoded, vertex_id] => Some((
            (*property).to_string(),
            generation.parse().ok()?,
            (*encoded).to_string(),
            vertex_id.parse().ok()?,
        )),
        _ => None,
    }
}

/// Every edge metadata property index entry of a cell, across edge types.
pub fn edge_property_index_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/eprop_idx/")
}

/// Every relationship property index entry of a cell, across edge types.
pub fn relationship_property_index_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/rprop_idx/")
}

pub fn edge_property_index(cell_id: &str, edge_type: &str, property: &str, encoded_value: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/eprop_idx/{edge_type}/{property}/{encoded_value}/{src:020}/{dst:020}")
}

pub fn relationship_property_index(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
    src: VertexId,
    dst: VertexId,
    relationship_id: RelationshipId,
) -> String {
    format!("cell/{cell_id}/rprop_idx/{edge_type}/{property}/{encoded_value}/{src:020}/{dst:020}/{relationship_id:020}")
}

/// The point-get counterpart of [`relationship_property_index`]: the same
/// components minus the trailing relationship id, which lives in the value
/// instead of the key. `rprop_idx` answers "which relationships have this
/// value" by scan; this key answers "the relationship with this identity" by
/// point get, so the lookup can use bloom filters. Written only for the MERGE
/// identity property, and only while exactly one live relationship carries
/// the identity — parallel relationships may legitimately share a value, and
/// an ambiguous identity keeps no pointer and stays on the scan path. The
/// write/invalidate protocol lives in `shard/write.rs` beside
/// `RELATIONSHIP_IDENTITY_PROPERTY`.
pub fn relationship_merge_index(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!("cell/{cell_id}/rmerge_idx/{edge_type}/{property}/{encoded_value}/{src:020}/{dst:020}")
}

#[cfg(feature = "opencypher")]
pub fn relationship_property_index_prefix(cell_id: &str, edge_type: &str, property: &str, encoded_value: &str) -> String {
    format!("cell/{cell_id}/rprop_idx/{edge_type}/{property}/{encoded_value}/")
}

#[cfg(feature = "opencypher")]
pub fn relationship_property_index_property_prefix(cell_id: &str, edge_type: &str, property: &str) -> String {
    format!("cell/{cell_id}/rprop_idx/{edge_type}/{property}/")
}

#[cfg(feature = "opencypher")]
pub fn relationship_property_index_edge_prefix(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!("cell/{cell_id}/rprop_idx/{edge_type}/{property}/{encoded_value}/{src:020}/{dst:020}/")
}

#[cfg(feature = "opencypher")]
pub fn edge_property_index_prefix(cell_id: &str, edge_type: &str, property: &str, encoded_value: &str) -> String {
    format!("cell/{cell_id}/eprop_idx/{edge_type}/{property}/{encoded_value}/")
}

#[cfg(feature = "opencypher")]
pub fn edge_property_index_property_prefix(cell_id: &str, edge_type: &str, property: &str) -> String {
    format!("cell/{cell_id}/eprop_idx/{edge_type}/{property}/")
}
