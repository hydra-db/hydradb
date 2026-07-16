use super::{RelationshipId, TopologySequence, VertexId};

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

pub fn last_epoch(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/last_epoch")
}

pub fn last_relationship_id(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/last_relationship_id")
}

pub fn mutation_log_epoch(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/mutation_log_epoch")
}

pub fn mutation_log_materialized_epoch(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/mutation_log_materialized_epoch")
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
    end_epoch: TopologySequence,
    start_epoch: TopologySequence,
    segment_id: &str,
) -> String {
    format!(
            "cell/{cell_id}/seg/out/{edge_type}/{src:020}/{end_epoch:020}/{start_epoch:020}/{segment_id}"
        )
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

pub fn outbox(
    cell_id: &str,
    epoch: TopologySequence,
    kind: super::DeltaKind,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
    let kind = match kind {
        super::DeltaKind::Plus => "plus",
        super::DeltaKind::Minus => "minus",
    };
    format!("cell/{cell_id}/outbox/{epoch:020}/{kind}/{edge_type}/{src:020}/{dst:020}")
}

pub fn outbox_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/outbox/")
}

pub fn outbox_batch(
    cell_id: &str,
    end_epoch: TopologySequence,
    start_epoch: TopologySequence,
    kind: super::DeltaKind,
    edge_type: &str,
    batch_id: &str,
) -> String {
    let kind = match kind {
        super::DeltaKind::Plus => "plus",
        super::DeltaKind::Minus => "minus",
    };
    format!(
            "cell/{cell_id}/outbox_batch/{end_epoch:020}/{start_epoch:020}/{kind}/{edge_type}/{batch_id}"
        )
}

pub fn outbox_batch_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/outbox_batch/")
}

pub fn mutation_batch(
    cell_id: &str,
    start_epoch: TopologySequence,
    idempotency_key: &str,
) -> String {
    format!("cell/{cell_id}/mutation_batch/{start_epoch:020}/{idempotency_key}")
}

pub fn mutation_log_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/mutation_log/")
}

pub fn mutation_log_entry(cell_id: &str, log_epoch: TopologySequence, batch_id: &str) -> String {
    format!("{}{log_epoch:020}/{batch_id}", mutation_log_prefix(cell_id))
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

pub fn delta_plus_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/delta/plus/{edge_type}/")
}

pub fn delta_minus_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/delta/minus/{edge_type}/")
}

pub fn owner_delta_prefix(
    cell_id: &str,
    kind: super::DeltaKind,
    edge_type: &str,
    direction: &str,
    owner: VertexId,
) -> String {
    let kind = match kind {
        super::DeltaKind::Plus => "plus",
        super::DeltaKind::Minus => "minus",
    };
    format!("cell/{cell_id}/delta_owner/{kind}/{edge_type}/{direction}/{owner:020}/")
}

pub fn owner_delta_kind_prefix(cell_id: &str, edge_type: &str, kind: super::DeltaKind) -> String {
    let kind = match kind {
        super::DeltaKind::Plus => "plus",
        super::DeltaKind::Minus => "minus",
    };
    format!("cell/{cell_id}/delta_owner/{kind}/{edge_type}/")
}

pub fn owner_delta(
    cell_id: &str,
    kind: super::DeltaKind,
    edge_type: &str,
    direction: &str,
    owner: VertexId,
    epoch: TopologySequence,
    neighbor: VertexId,
) -> String {
    format!(
        "{}{epoch:020}/{neighbor:020}",
        owner_delta_prefix(cell_id, kind, edge_type, direction, owner)
    )
}

pub fn pair_delta_kind_prefix(cell_id: &str, edge_type: &str, kind: super::DeltaKind) -> String {
    let kind = match kind {
        super::DeltaKind::Plus => "plus",
        super::DeltaKind::Minus => "minus",
    };
    format!("cell/{cell_id}/delta_pair/{kind}/{edge_type}/")
}

pub fn pair_delta_prefix(
    cell_id: &str,
    kind: super::DeltaKind,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!(
        "{}{src:020}/{dst:020}/",
        pair_delta_kind_prefix(cell_id, edge_type, kind)
    )
}

pub fn pair_delta(
    cell_id: &str,
    kind: super::DeltaKind,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    epoch: TopologySequence,
) -> String {
    format!(
        "{}{epoch:020}",
        pair_delta_prefix(cell_id, kind, edge_type, src, dst)
    )
}

pub fn delta_gc_watermark(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/meta/delta_gc/{edge_type}")
}
