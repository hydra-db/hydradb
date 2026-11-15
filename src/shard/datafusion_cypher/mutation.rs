use crate::{GraphError, GraphShard, QueryMutationResult, QueryValue};
use hydradb_graph_provider::{GraphProviderError, MutationCounters, ReturnManifestRow, StatementReceiptLookup};
use hydradb_graph_types::{decode_value, CypherValue};
use std::collections::BTreeMap;

pub(crate) use hydradb_graph_slate::provider::mutation::scope_legacy_statement_receipt;

pub(crate) async fn complete_legacy_statement_receipt(
    shard: &GraphShard,
    lookup: StatementReceiptLookup,
    result: &QueryMutationResult,
) -> crate::Result<u64> {
    hydradb_graph_slate::provider::mutation::complete_legacy_statement_receipt(&shard.storage, lookup, || {
        Ok((legacy_mutation_counters(result), legacy_return_manifest(result)?))
    })
    .await
}

fn legacy_mutation_counters(result: &QueryMutationResult) -> MutationCounters {
    MutationCounters {
        nodes_created: result.created_vertices,
        relationships_created: result.created_relationships.max(result.created_edges),
        relationships_deleted: result.deleted_edges,
        properties_set: result.updated_vertices.saturating_add(result.updated_relationships),
        matched: result.matched_rows,
        ..MutationCounters::default()
    }
}

fn legacy_return_manifest(result: &QueryMutationResult) -> crate::Result<Vec<ReturnManifestRow>> {
    let Some(rows) = &result.returned_rows else {
        return Ok(Vec::new());
    };
    rows.rows
        .iter()
        .enumerate()
        .map(|(input_ordinal, row)| {
            Ok(ReturnManifestRow {
                input_ordinal: input_ordinal as u64,
                input_fingerprint: None,
                action_range: None,
                rows: vec![row.values.iter().map(legacy_query_value).collect::<crate::Result<Vec<_>>>()?],
                counters: legacy_mutation_counters(result),
            })
        })
        .collect()
}

fn legacy_query_value(value: &QueryValue) -> crate::Result<CypherValue> {
    Ok(match value {
        QueryValue::Null => CypherValue::Null,
        QueryValue::VertexId(value) => CypherValue::Integer(i128::from(*value)),
        QueryValue::Count(value) => CypherValue::Integer(i128::from(*value)),
        QueryValue::Bool(value) => CypherValue::Boolean(*value),
        QueryValue::Float(value) => CypherValue::Float(value.0),
        QueryValue::Property(value) => super::property_to_cypher(value),
        QueryValue::List(values) => CypherValue::List(values.iter().map(legacy_query_value).collect::<crate::Result<Vec<_>>>()?),
        QueryValue::Record(fields) => CypherValue::Record(
            fields
                .iter()
                .map(|(name, value)| Ok((name.clone(), legacy_query_value(value)?)))
                .collect::<crate::Result<BTreeMap<_, _>>>()?,
        ),
        QueryValue::Temporal(temporal) => legacy_temporal_value(temporal),
        QueryValue::Decimal(value) => CypherValue::Decimal(value.clone()),
        QueryValue::Bytes(value) => CypherValue::Bytes(value.clone()),
        QueryValue::Path(path) => {
            let nodes = path.nodes.iter().map(|node| hydradb_graph_types::NodeId(node.id)).collect();
            let mut relationships = Vec::with_capacity(path.relationships.len());
            for (index, relationship) in path.relationships.iter().enumerate() {
                let from = path.nodes[index].id;
                let to = path.nodes[index + 1].id;
                let identity = match relationship.id {
                    Some(id) => hydradb_graph_types::RelationshipIdentity::Identified(hydradb_graph_types::RelationshipId(id)),
                    None => hydradb_graph_types::RelationshipIdentity::Structural {
                        source: hydradb_graph_types::NodeId(relationship.src),
                        destination: hydradb_graph_types::NodeId(relationship.dst),
                        relationship_type: relationship.edge_type.clone(),
                    },
                };
                let orientation = if (relationship.src, relationship.dst) == (from, to) {
                    hydradb_graph_types::TraversalOrientation::Forward
                } else if (relationship.src, relationship.dst) == (to, from) {
                    hydradb_graph_types::TraversalOrientation::Reverse
                } else {
                    return Err(GraphError::CorruptValue {
                        key: "query/legacy/receipt_path".to_string(),
                        reason: "path relationship endpoints do not match the adjacent path nodes".to_string(),
                    });
                };
                relationships.push(hydradb_graph_types::PathRelationship::new(identity, orientation));
            }
            CypherValue::Path(
                hydradb_graph_types::Path::new(nodes, relationships).map_err(|error| GraphError::CorruptValue {
                    key: "query/legacy/receipt_path".to_string(),
                    reason: error.to_string(),
                })?,
            )
        }
        QueryValue::GraphValue(bytes) => decode_value(bytes).map_err(GraphProviderError::GraphValue)?,
    })
}

fn legacy_temporal_value(temporal: &crate::query::algebra::QueryTemporal) -> CypherValue {
    use crate::query::algebra::QueryTemporalKind;
    use hydradb_graph_types::CypherTemporal;

    let temporal = match temporal.kind {
        QueryTemporalKind::Date { days } => CypherTemporal::Date {
            days,
            text: temporal.text.clone(),
        },
        QueryTemporalKind::LocalTime { nanoseconds } => CypherTemporal::LocalTime {
            nanoseconds,
            text: temporal.text.clone(),
        },
        QueryTemporalKind::ZonedTime {
            nanoseconds,
            offset_seconds,
        } => CypherTemporal::ZonedTime {
            nanoseconds,
            offset_seconds,
            text: temporal.text.clone(),
        },
        QueryTemporalKind::LocalDatetime { seconds, nanoseconds } => CypherTemporal::LocalDateTime {
            seconds,
            nanoseconds,
            text: temporal.text.clone(),
        },
        QueryTemporalKind::ZonedDatetime {
            seconds,
            nanoseconds,
            offset_seconds,
        } => CypherTemporal::ZonedDateTime {
            seconds,
            nanoseconds,
            offset_seconds,
            text: temporal.text.clone(),
        },
        QueryTemporalKind::Duration {
            months,
            days,
            seconds,
            nanoseconds,
        } => CypherTemporal::Duration {
            months,
            days,
            seconds,
            nanoseconds,
            text: temporal.text.clone(),
        },
    };
    CypherValue::Temporal(temporal)
}
