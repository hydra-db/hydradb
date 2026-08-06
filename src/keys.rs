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
pub fn xlog_entry(
    cell_id: &str,
    edge_type: &str,
    sequence: StorageSequence,
    src: VertexId,
    dst: VertexId,
) -> String {
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

pub fn out_segment(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    storage_sequence: StorageSequence,
    segment_id: &str,
) -> String {
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

pub fn out_segment_tombstone(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
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

pub fn relationship(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    relationship_id: RelationshipId,
) -> String {
    format!("cell/{cell_id}/rel/{edge_type}/{src:020}/{dst:020}/{relationship_id:020}")
}

pub fn relationship_edge_prefix(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!("cell/{cell_id}/rel/{edge_type}/{src:020}/{dst:020}/")
}

pub fn relationship_cell_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/rel/")
}

#[cfg(feature = "opencypher")]
pub fn relationship_source_prefix(cell_id: &str, edge_type: &str, src: VertexId) -> String {
    format!("cell/{cell_id}/rel/{edge_type}/{src:020}/")
}

pub fn relationship_id(cell_id: &str, relationship_id: RelationshipId) -> String {
    format!("cell/{cell_id}/rel_id/{relationship_id:020}")
}

pub fn relationship_count(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/rel_count/{edge_type}/{src:020}/{dst:020}")
}

pub fn degree_out_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/cnt/out/{edge_type}/")
}

pub fn degree_in_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/cnt/in/{edge_type}/")
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

pub fn vertex_label(cell_id: &str, label: &str, vertex_id: VertexId) -> String {
    format!("cell/{cell_id}/vlabel/{label}/{vertex_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_label_prefix(cell_id: &str, label: &str) -> String {
    format!("cell/{cell_id}/vlabel/{label}/")
}

pub fn vertex_property_index(
    cell_id: &str,
    property: &str,
    encoded_value: &str,
    vertex_id: VertexId,
) -> String {
    format!("cell/{cell_id}/vprop_idx/{property}/{encoded_value}/{vertex_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_property_index_prefix(cell_id: &str, property: &str, encoded_value: &str) -> String {
    format!("cell/{cell_id}/vprop_idx/{property}/{encoded_value}/")
}

#[cfg(feature = "opencypher")]
pub fn vertex_property_index_property_prefix(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/vprop_idx/{property}/")
}

pub fn edge_property_index(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
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
    format!(
        "cell/{cell_id}/rprop_idx/{edge_type}/{property}/{encoded_value}/{src:020}/{dst:020}/{relationship_id:020}"
    )
}

#[cfg(feature = "opencypher")]
pub fn relationship_property_index_prefix(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
) -> String {
    format!("cell/{cell_id}/rprop_idx/{edge_type}/{property}/{encoded_value}/")
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
pub fn edge_property_index_prefix(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
) -> String {
    format!("cell/{cell_id}/eprop_idx/{edge_type}/{property}/{encoded_value}/")
}

#[cfg(feature = "opencypher")]
pub fn edge_property_index_property_prefix(
    cell_id: &str,
    edge_type: &str,
    property: &str,
) -> String {
    format!("cell/{cell_id}/eprop_idx/{edge_type}/{property}/")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_edge_type(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/qstats/edge_type/{edge_type}")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_vertex_label(cell_id: &str, label: &str) -> String {
    format!("cell/{cell_id}/qstats/vlabel/{label}")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_vertex_property(cell_id: &str, property: &str, encoded_value: &str) -> String {
    format!("cell/{cell_id}/qstats/vprop/{property}/{encoded_value}")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_vertex_property_prefix(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/qstats/vprop/{property}/")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_edge_property(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
) -> String {
    format!("cell/{cell_id}/qstats/eprop/{edge_type}/{property}/{encoded_value}")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_edge_property_prefix(cell_id: &str, edge_type: &str, property: &str) -> String {
    format!("cell/{cell_id}/qstats/eprop/{edge_type}/{property}/")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_vertex_property_histogram(cell_id: &str, property: &str) -> String {
    format!("cell/{cell_id}/qstats/vprop_hist/{property}")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_edge_property_histogram(
    cell_id: &str,
    edge_type: &str,
    property: &str,
) -> String {
    format!("cell/{cell_id}/qstats/eprop_hist/{edge_type}/{property}")
}

#[cfg(feature = "opencypher")]
pub fn query_stats_record_key(count_key: &str) -> String {
    format!("{count_key}/record")
}
