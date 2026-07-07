use super::{GraphEpoch, RelationshipId, VertexId};

pub fn write_fence(cell_id: &str) -> String {
    format!("cell/{cell_id}/meta/write_fence")
}

pub fn read_lease_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/read_lease/")
}

pub fn read_lease(cell_id: &str, lease_id: &str) -> String {
    format!("{}{}", read_lease_prefix(cell_id), lease_id)
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
    end_epoch: GraphEpoch,
    start_epoch: GraphEpoch,
    segment_id: &str,
) -> String {
    format!(
            "cell/{cell_id}/seg/out/{edge_type}/{src:020}/{end_epoch:020}/{start_epoch:020}/{segment_id}"
        )
}

pub fn out_segment_edge_type_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/seg/out/{edge_type}/")
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

pub fn relationship_id(cell_id: &str, relationship_id: RelationshipId) -> String {
    format!("cell/{cell_id}/rel_id/{relationship_id:020}")
}

pub fn relationship_count(cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> String {
    format!("cell/{cell_id}/rel_count/{edge_type}/{src:020}/{dst:020}")
}

pub fn relationship_metadata_delta(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    relationship_id: RelationshipId,
    epoch: GraphEpoch,
) -> String {
    format!(
        "cell/{cell_id}/rel_delta/{edge_type}/{src:020}/{dst:020}/{relationship_id:020}/{epoch:020}"
    )
}

#[cfg(feature = "opencypher")]
pub fn relationship_metadata_delta_prefix(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    relationship_id: RelationshipId,
) -> String {
    format!("cell/{cell_id}/rel_delta/{edge_type}/{src:020}/{dst:020}/{relationship_id:020}/")
}

pub fn relationship_tombstone(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    relationship_id: RelationshipId,
) -> String {
    format!("cell/{cell_id}/rel_tomb/{edge_type}/{src:020}/{dst:020}/{relationship_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn relationship_tombstone_edge_prefix(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!("cell/{cell_id}/rel_tomb/{edge_type}/{src:020}/{dst:020}/")
}

pub fn edge_metadata_delta(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    epoch: GraphEpoch,
) -> String {
    format!("cell/{cell_id}/emeta_delta/{edge_type}/{src:020}/{dst:020}/{epoch:020}")
}

#[cfg(feature = "opencypher")]
pub fn edge_metadata_delta_prefix(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!("cell/{cell_id}/emeta_delta/{edge_type}/{src:020}/{dst:020}/")
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
    epoch: GraphEpoch,
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
    end_epoch: GraphEpoch,
    start_epoch: GraphEpoch,
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

pub fn mutation_batch(cell_id: &str, start_epoch: GraphEpoch, idempotency_key: &str) -> String {
    format!("cell/{cell_id}/mutation_batch/{start_epoch:020}/{idempotency_key}")
}

pub fn mutation_log_prefix(cell_id: &str) -> String {
    format!("cell/{cell_id}/mutation_log/")
}

pub fn mutation_log_entry(cell_id: &str, log_epoch: GraphEpoch, batch_id: &str) -> String {
    format!("{}{log_epoch:020}/{batch_id}", mutation_log_prefix(cell_id))
}

pub fn vertex(cell_id: &str, vertex_id: VertexId) -> String {
    format!("cell/{cell_id}/vertex/{vertex_id:020}")
}

pub fn vertex_delta(cell_id: &str, vertex_id: VertexId, epoch: GraphEpoch) -> String {
    format!("cell/{cell_id}/vertex_delta/{vertex_id:020}/{epoch:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_delta_prefix(cell_id: &str, vertex_id: VertexId) -> String {
    format!("cell/{cell_id}/vertex_delta/{vertex_id:020}/")
}

pub fn vertex_label(cell_id: &str, label: &str, vertex_id: VertexId) -> String {
    format!("cell/{cell_id}/vlabel/{label}/{vertex_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_label_prefix(cell_id: &str, label: &str) -> String {
    format!("cell/{cell_id}/vlabel/{label}/")
}

pub fn vertex_label_delta(
    cell_id: &str,
    label: &str,
    epoch: GraphEpoch,
    vertex_id: VertexId,
) -> String {
    format!("cell/{cell_id}/vlabel_delta/{label}/{epoch:020}/{vertex_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_label_delta_prefix(cell_id: &str, label: &str) -> String {
    format!("cell/{cell_id}/vlabel_delta/{label}/")
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

pub fn vertex_property_index_delta(
    cell_id: &str,
    property: &str,
    encoded_value: &str,
    epoch: GraphEpoch,
    vertex_id: VertexId,
) -> String {
    format!("cell/{cell_id}/vprop_delta/{property}/{encoded_value}/{epoch:020}/{vertex_id:020}")
}

#[cfg(feature = "opencypher")]
pub fn vertex_property_index_delta_prefix(
    cell_id: &str,
    property: &str,
    encoded_value: &str,
) -> String {
    format!("cell/{cell_id}/vprop_delta/{property}/{encoded_value}/")
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

pub struct RelationshipPropertyIndexDeltaKey<'a> {
    pub cell_id: &'a str,
    pub edge_type: &'a str,
    pub property: &'a str,
    pub encoded_value: &'a str,
    pub epoch: GraphEpoch,
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
}

pub fn relationship_property_index_delta(key: RelationshipPropertyIndexDeltaKey<'_>) -> String {
    format!(
        "cell/{}/rprop_delta/{}/{}/{}/{:020}/{:020}/{:020}/{:020}",
        key.cell_id,
        key.edge_type,
        key.property,
        key.encoded_value,
        key.epoch,
        key.src,
        key.dst,
        key.relationship_id
    )
}

#[cfg(feature = "opencypher")]
pub fn relationship_property_index_delta_prefix(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
) -> String {
    format!("cell/{cell_id}/rprop_delta/{edge_type}/{property}/{encoded_value}/")
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

pub fn edge_property_index_delta(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
    epoch: GraphEpoch,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!(
        "cell/{cell_id}/eprop_delta/{edge_type}/{property}/{encoded_value}/{epoch:020}/{src:020}/{dst:020}"
    )
}

#[cfg(feature = "opencypher")]
pub fn edge_property_index_delta_prefix(
    cell_id: &str,
    edge_type: &str,
    property: &str,
    encoded_value: &str,
) -> String {
    format!("cell/{cell_id}/eprop_delta/{edge_type}/{property}/{encoded_value}/")
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

#[cfg(test)]
pub fn delta_plus(
    cell_id: &str,
    edge_type: &str,
    epoch: GraphEpoch,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!("cell/{cell_id}/delta/plus/{edge_type}/{epoch:020}/{src:020}/{dst:020}")
}

pub fn delta_plus_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/delta/plus/{edge_type}/")
}

pub fn delta_minus_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/delta/minus/{edge_type}/")
}

#[cfg(test)]
pub fn delta_minus(
    cell_id: &str,
    edge_type: &str,
    epoch: GraphEpoch,
    src: VertexId,
    dst: VertexId,
) -> String {
    format!("cell/{cell_id}/delta/minus/{edge_type}/{epoch:020}/{src:020}/{dst:020}")
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

#[cfg(test)]
pub fn owner_delta(
    cell_id: &str,
    kind: super::DeltaKind,
    edge_type: &str,
    direction: &str,
    owner: VertexId,
    epoch: GraphEpoch,
    neighbor: VertexId,
) -> String {
    format!(
        "{}{epoch:020}/{neighbor:020}",
        owner_delta_prefix(cell_id, kind, edge_type, direction, owner)
    )
}

pub fn delta_gc_watermark(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/meta/delta_gc/{edge_type}")
}
