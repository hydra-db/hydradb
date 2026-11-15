use boltr::error::BoltError;
use boltr::types::{
    BoltDate, BoltDateTime, BoltDict, BoltDuration, BoltLocalDateTime, BoltLocalTime, BoltNode, BoltPath, BoltTime,
    BoltUnboundRelationship, BoltValue,
};
#[cfg(feature = "datafusion-cypher-engine")]
use boltr::types::{BoltDateTimeZoneId, BoltPoint2D, BoltPoint3D, BoltRelationship};
#[cfg(feature = "datafusion-cypher-engine")]
use sha2::{Digest, Sha256};

use super::{ClientBookmark, ClientQueryTarget};
use crate::{GraphError, QueryFailureReason, QueryFloat, QueryParameterValue, QueryPath, QueryValue, VertexPropertyValue};
#[cfg(feature = "datafusion-cypher-engine")]
use hydradb_graph_types::{decode_value, CypherValue, RelationshipIdentity};

const MAX_QUERY_PARAMETER_DEPTH: usize = 16;

pub(super) fn highest_matching_bookmark(
    target: &ClientQueryTarget,
    bookmarks: Vec<String>,
) -> std::result::Result<Option<ClientBookmark>, BoltError> {
    let mut highest = None;
    for bookmark in bookmarks {
        let bookmark = ClientBookmark::parse(&bookmark).map_err(graph_error_to_bolt)?;
        if bookmark.target != *target {
            return Err(BoltError::Query {
                code: "Neo.ClientError.Transaction.InvalidBookmark".to_string(),
                message: "bookmark belongs to another graph scope or cell".to_string(),
            });
        }
        if highest
            .as_ref()
            .is_none_or(|current: &ClientBookmark| bookmark.epoch > current.epoch)
        {
            highest = Some(bookmark);
        }
    }
    Ok(highest)
}

pub(super) fn bolt_parameter_to_property(name: &str, value: &BoltValue) -> std::result::Result<VertexPropertyValue, BoltError> {
    match value {
        BoltValue::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        BoltValue::Integer(value) => Ok(VertexPropertyValue::from_i64(*value)),
        BoltValue::Float(value) if value.is_finite() => Ok(VertexPropertyValue::Float(QueryFloat(*value))),
        BoltValue::String(value) => Ok(VertexPropertyValue::String(value.clone())),
        BoltValue::Date(value) => i32::try_from(value.days)
            .ok()
            .filter(|days| crate::core::model::date_iso_string(*days).is_some())
            .map(VertexPropertyValue::Date)
            .ok_or_else(|| invalid_bolt_parameter(name, "date exceeds the supported calendar range")),
        BoltValue::Float(_) => Err(invalid_bolt_parameter(name, "float must be finite")),
        _ => Err(invalid_bolt_parameter(
            name,
            "only boolean, signed integer, finite float, and string parameters are supported",
        )),
    }
}

pub(super) fn bolt_parameter_to_query_value(name: &str, value: &BoltValue) -> std::result::Result<QueryParameterValue, BoltError> {
    bolt_parameter_to_query_value_at_depth(name, value, 0)
}

fn bolt_parameter_to_query_value_at_depth(
    name: &str,
    value: &BoltValue,
    depth: usize,
) -> std::result::Result<QueryParameterValue, BoltError> {
    if depth > MAX_QUERY_PARAMETER_DEPTH {
        return Err(invalid_bolt_parameter(name, "nested parameter depth exceeds 16"));
    }
    match value {
        BoltValue::List(values) => values
            .iter()
            .map(|value| bolt_parameter_to_query_value_at_depth(name, value, depth + 1))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(QueryParameterValue::List),
        BoltValue::Dict(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), bolt_parameter_to_query_value_at_depth(name, value, depth + 1)?)))
            .collect::<std::result::Result<std::collections::BTreeMap<_, _>, BoltError>>()
            .map(QueryParameterValue::Map),
        _ => bolt_parameter_to_property(name, value).map(QueryParameterValue::Scalar),
    }
}

fn invalid_bolt_parameter(name: &str, reason: &str) -> BoltError {
    BoltError::Query {
        code: "Neo.ClientError.Statement.TypeError".to_string(),
        message: format!("invalid parameter ${name}: {reason}"),
    }
}

pub(super) fn query_value_to_bolt(value: &QueryValue) -> std::result::Result<BoltValue, BoltError> {
    match value {
        QueryValue::Null => Ok(BoltValue::Null),
        QueryValue::VertexId(value) | QueryValue::Count(value) => i64::try_from(*value)
            .map(BoltValue::Integer)
            .map_err(|_| bolt_integer_overflow(*value)),
        QueryValue::Bool(value) => Ok(BoltValue::Boolean(*value)),
        QueryValue::Float(value) => Ok(BoltValue::Float(value.0)),
        QueryValue::Property(value) => property_value_to_bolt(value),
        QueryValue::List(values) => values
            .iter()
            .map(query_value_to_bolt)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(BoltValue::List),
        QueryValue::Path(path) => query_path_to_bolt(path).map(BoltValue::Path),
        QueryValue::Temporal(temporal) => Ok(temporal_to_bolt(temporal.kind)),
        // Bolt has no decimal: its exact literal, as a string.
        QueryValue::Decimal(text) => Ok(BoltValue::String(text.clone())),
        QueryValue::Bytes(bytes) => Ok(BoltValue::Bytes(bytes.clone())),
        // A record goes as a map, Bolt's own record-like value.
        QueryValue::Record(fields) => fields
            .iter()
            .map(|(name, value)| Ok((name.clone(), query_value_to_bolt(value)?)))
            .collect::<std::result::Result<BoltDict, BoltError>>()
            .map(BoltValue::Dict),
        #[cfg(feature = "datafusion-cypher-engine")]
        QueryValue::GraphValue(bytes) => decode_value(bytes)
            .map_err(|error| BoltError::Backend(format!("invalid canonical graph value: {error}")))
            .and_then(cypher_value_to_bolt),
        #[cfg(not(feature = "datafusion-cypher-engine"))]
        QueryValue::GraphValue(bytes) => Ok(BoltValue::Bytes(bytes.clone())),
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
fn cypher_value_to_bolt(value: CypherValue) -> std::result::Result<BoltValue, BoltError> {
    match value {
        CypherValue::Null | CypherValue::Missing => Ok(BoltValue::Null),
        CypherValue::Boolean(value) => Ok(BoltValue::Boolean(value)),
        CypherValue::Integer(value) => i64::try_from(value).map(BoltValue::Integer).map_err(|_| BoltError::Query {
            code: "Neo.ClientError.Statement.TypeError".to_string(),
            message: format!("integer {value} exceeds Bolt's signed 64-bit integer range"),
        }),
        CypherValue::Float(value) => Ok(BoltValue::Float(value)),
        CypherValue::String(value) => Ok(BoltValue::String(value)),
        // Bolt has no decimal type; retain its exact literal as a string.
        CypherValue::Decimal(value) => Ok(BoltValue::String(value)),
        CypherValue::Bytes(value) => Ok(BoltValue::Bytes(value)),
        CypherValue::List(values) => values
            .into_iter()
            .map(cypher_value_to_bolt)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(BoltValue::List),
        CypherValue::Map(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, cypher_value_to_bolt(value)?)))
            .collect::<std::result::Result<BoltDict, BoltError>>()
            .map(BoltValue::Dict),
        CypherValue::Record(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, cypher_value_to_bolt(value)?)))
            .collect::<std::result::Result<BoltDict, BoltError>>()
            .map(BoltValue::Dict),
        CypherValue::Node(node) => {
            let id = bolt_graph_id(node.0)?;
            Ok(BoltValue::Node(BoltNode {
                id,
                labels: Vec::new(),
                properties: BoltDict::new(),
                element_id: node.0.to_string(),
            }))
        }
        CypherValue::NodeEntity(node) => {
            let id = bolt_graph_id(node.id.0)?;
            let properties = node
                .properties
                .into_iter()
                .map(|(key, value)| Ok((key, cypher_value_to_bolt(value)?)))
                .collect::<std::result::Result<BoltDict, BoltError>>()?;
            Ok(BoltValue::Node(BoltNode {
                id,
                labels: node.labels,
                properties,
                element_id: node.id.0.to_string(),
            }))
        }
        CypherValue::Relationship(relationship) => {
            let (id, element_id, start, end) = match relationship.identity() {
                RelationshipIdentity::Structural {
                    source,
                    destination,
                    relationship_type,
                } => {
                    let (id, element_id) = structural_bolt_relationship_identity(*source, *destination, relationship_type);
                    (id, element_id, source.0, destination.0)
                }
                RelationshipIdentity::Identified(id) => (
                    i64::try_from(id.0).map_err(|_| bolt_graph_value_error("relationship id exceeds Bolt's signed integer range"))?,
                    id.0.to_string(),
                    relationship.source().0,
                    relationship.destination().0,
                ),
            };
            let start_node_id = bolt_graph_id(start)?;
            let end_node_id = bolt_graph_id(end)?;
            Ok(BoltValue::Relationship(BoltRelationship {
                id,
                start_node_id,
                end_node_id,
                rel_type: relationship.relationship_type().to_string(),
                properties: BoltDict::new(),
                element_id,
                start_element_id: start.to_string(),
                end_element_id: end.to_string(),
            }))
        }
        CypherValue::RelationshipEntity(relationship) => {
            let identity = &relationship.identity;
            let (id, element_id, start, end) = match identity.identity() {
                RelationshipIdentity::Structural {
                    source,
                    destination,
                    relationship_type,
                } => {
                    let (id, element_id) = structural_bolt_relationship_identity(*source, *destination, relationship_type);
                    (id, element_id, source.0, destination.0)
                }
                RelationshipIdentity::Identified(id) => (
                    i64::try_from(id.0).map_err(|_| bolt_graph_value_error("relationship id exceeds Bolt's signed integer range"))?,
                    id.0.to_string(),
                    identity.source().0,
                    identity.destination().0,
                ),
            };
            let start_node_id = bolt_graph_id(start)?;
            let end_node_id = bolt_graph_id(end)?;
            let properties = relationship
                .properties
                .into_iter()
                .map(|(key, value)| Ok((key, cypher_value_to_bolt(value)?)))
                .collect::<std::result::Result<BoltDict, BoltError>>()?;
            Ok(BoltValue::Relationship(BoltRelationship {
                id,
                start_node_id,
                end_node_id,
                rel_type: identity.relationship_type().to_string(),
                properties,
                element_id,
                start_element_id: start.to_string(),
                end_element_id: end.to_string(),
            }))
        }
        CypherValue::Path(_) => Err(bolt_graph_value_error(
            "canonical path value is missing relationship type metadata required by Bolt",
        )),
        CypherValue::Date(value) => Ok(BoltValue::Date(BoltDate {
            days: i64::from(value.days_since_epoch),
        })),
        CypherValue::Temporal(value) => Ok(cypher_temporal_to_bolt(value)),
        CypherValue::LocalTime(value) => Ok(BoltValue::LocalTime(BoltLocalTime {
            nanoseconds: i64::try_from(value.nanos_since_midnight)
                .map_err(|_| bolt_graph_value_error("local time exceeds Bolt's nanosecond range"))?,
        })),
        CypherValue::Time(value) => Ok(BoltValue::Time(BoltTime {
            nanoseconds: i64::try_from(value.local.nanos_since_midnight)
                .map_err(|_| bolt_graph_value_error("time exceeds Bolt's nanosecond range"))?,
            tz_offset_seconds: i64::from(value.offset_seconds),
        })),
        CypherValue::LocalDateTime(value) => {
            let total_nanos = i128::from(value.date.days_since_epoch)
                .checked_mul(86_400_000_000_000)
                .and_then(|days| days.checked_add(i128::from(value.time.nanos_since_midnight)))
                .ok_or_else(|| bolt_graph_value_error("local datetime exceeds Bolt's nanosecond range"))?;
            let (seconds, nanoseconds) = split_nanos(total_nanos)?;
            Ok(BoltValue::LocalDateTime(BoltLocalDateTime { seconds, nanoseconds }))
        }
        CypherValue::DateTime(value) => {
            let (seconds, nanoseconds) = split_nanos(value.unix_nanos)?;
            if let Some(timezone) = value.timezone {
                Ok(BoltValue::DateTimeZoneId(BoltDateTimeZoneId {
                    seconds,
                    nanoseconds,
                    tz_id: timezone,
                }))
            } else {
                Ok(BoltValue::DateTime(BoltDateTime {
                    seconds,
                    nanoseconds,
                    tz_offset_seconds: i64::from(value.offset_seconds),
                }))
            }
        }
        CypherValue::Duration(value) => {
            let seconds = value.nanos.div_euclid(1_000_000_000);
            let nanoseconds = value.nanos.rem_euclid(1_000_000_000);
            Ok(BoltValue::Duration(BoltDuration {
                months: value.months,
                days: value.days,
                seconds: i64::try_from(seconds).map_err(|_| bolt_graph_value_error("duration exceeds Bolt's second range"))?,
                nanoseconds: i64::try_from(nanoseconds).expect("normalized nanoseconds fit in i64"),
            }))
        }
        CypherValue::Point(value) => {
            let srid = i64::from(value.srid);
            match value.z {
                Some(z) => Ok(BoltValue::Point3D(BoltPoint3D {
                    srid,
                    x: value.x,
                    y: value.y,
                    z,
                })),
                None => Ok(BoltValue::Point2D(BoltPoint2D {
                    srid,
                    x: value.x,
                    y: value.y,
                })),
            }
        }
        CypherValue::Extension(value) => Err(bolt_graph_value_error(&format!(
            "Bolt does not support graph extension value {} version {}",
            value.type_name, value.version
        ))),
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
fn bolt_graph_id(value: u64) -> std::result::Result<i64, BoltError> {
    i64::try_from(value).map_err(|_| bolt_graph_value_error("graph id exceeds Bolt's signed integer range"))
}

#[cfg(feature = "datafusion-cypher-engine")]
fn structural_bolt_relationship_identity(
    source: hydradb_graph_types::NodeId,
    destination: hydradb_graph_types::NodeId,
    relationship_type: &str,
) -> (i64, String) {
    let mut digest = Sha256::new();
    digest.update(b"hydradb-bolt-structural-relationship-v1");
    digest.update(source.0.to_be_bytes());
    digest.update(destination.0.to_be_bytes());
    digest.update((relationship_type.len() as u64).to_be_bytes());
    digest.update(relationship_type.as_bytes());
    let digest = digest.finalize();
    let mut id_bytes = [0_u8; 8];
    id_bytes.copy_from_slice(&digest[..8]);
    let magnitude = u64::from_be_bytes(id_bytes) & (i64::MAX as u64);
    let id = -1_i64 - magnitude as i64;
    let element_id = format!("structural:{digest:x}");
    (id, element_id)
}

#[cfg(feature = "datafusion-cypher-engine")]
fn split_nanos(value: i128) -> std::result::Result<(i64, i64), BoltError> {
    let seconds =
        i64::try_from(value.div_euclid(1_000_000_000)).map_err(|_| bolt_graph_value_error("datetime exceeds Bolt's seconds range"))?;
    let nanoseconds = i64::try_from(value.rem_euclid(1_000_000_000)).expect("normalized nanoseconds fit in i64");
    Ok((seconds, nanoseconds))
}

#[cfg(feature = "datafusion-cypher-engine")]
fn bolt_graph_value_error(message: &str) -> BoltError {
    BoltError::Query {
        code: "Neo.ClientError.Statement.TypeError".to_string(),
        message: message.to_string(),
    }
}

/// A GQL temporal value as Bolt's temporal structures.
fn temporal_to_bolt(kind: crate::query::algebra::QueryTemporalKind) -> BoltValue {
    use crate::query::algebra::QueryTemporalKind;
    match kind {
        QueryTemporalKind::Date { days } => BoltValue::Date(BoltDate { days }),
        QueryTemporalKind::LocalTime { nanoseconds } => BoltValue::LocalTime(BoltLocalTime { nanoseconds }),
        QueryTemporalKind::ZonedTime {
            nanoseconds,
            offset_seconds,
        } => BoltValue::Time(BoltTime {
            nanoseconds,
            tz_offset_seconds: i64::from(offset_seconds),
        }),
        QueryTemporalKind::LocalDatetime { seconds, nanoseconds } => BoltValue::LocalDateTime(BoltLocalDateTime {
            seconds,
            nanoseconds: i64::from(nanoseconds),
        }),
        QueryTemporalKind::ZonedDatetime {
            seconds,
            nanoseconds,
            offset_seconds,
        } => BoltValue::DateTime(BoltDateTime {
            seconds,
            nanoseconds: i64::from(nanoseconds),
            tz_offset_seconds: i64::from(offset_seconds),
        }),
        QueryTemporalKind::Duration {
            months,
            days,
            seconds,
            nanoseconds,
        } => BoltValue::Duration(BoltDuration {
            months,
            days,
            seconds,
            nanoseconds: i64::from(nanoseconds),
        }),
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
fn cypher_temporal_to_bolt(value: hydradb_graph_types::CypherTemporal) -> BoltValue {
    use hydradb_graph_types::CypherTemporal;
    match value {
        CypherTemporal::Date { days, .. } => BoltValue::Date(BoltDate { days }),
        CypherTemporal::LocalTime { nanoseconds, .. } => BoltValue::LocalTime(BoltLocalTime { nanoseconds }),
        CypherTemporal::ZonedTime {
            nanoseconds,
            offset_seconds,
            ..
        } => BoltValue::Time(BoltTime {
            nanoseconds,
            tz_offset_seconds: i64::from(offset_seconds),
        }),
        CypherTemporal::LocalDateTime { seconds, nanoseconds, .. } => BoltValue::LocalDateTime(BoltLocalDateTime {
            seconds,
            nanoseconds: i64::from(nanoseconds),
        }),
        CypherTemporal::ZonedDateTime {
            seconds,
            nanoseconds,
            offset_seconds,
            ..
        } => BoltValue::DateTime(BoltDateTime {
            seconds,
            nanoseconds: i64::from(nanoseconds),
            tz_offset_seconds: i64::from(offset_seconds),
        }),
        CypherTemporal::Duration {
            months,
            days,
            seconds,
            nanoseconds,
            ..
        } => BoltValue::Duration(BoltDuration {
            months,
            days,
            seconds,
            nanoseconds: i64::from(nanoseconds),
        }),
    }
}

fn query_path_to_bolt(path: &QueryPath) -> std::result::Result<BoltPath, BoltError> {
    if path.nodes.len() != path.relationships.len().saturating_add(1) {
        return Err(BoltError::Backend(
            "native path node and relationship counts are inconsistent".to_string(),
        ));
    }
    let nodes = path
        .nodes
        .iter()
        .map(|node| {
            let id = i64::try_from(node.id).map_err(|_| bolt_integer_overflow(node.id))?;
            Ok(BoltNode {
                id,
                labels: node.labels.clone(),
                properties: property_map_to_bolt(&node.properties)?,
                element_id: node.id.to_string(),
            })
        })
        .collect::<std::result::Result<Vec<_>, BoltError>>()?;
    let relationships = path
        .relationships
        .iter()
        .enumerate()
        .map(|(index, relationship)| {
            let id = match relationship.id {
                Some(id) => i64::try_from(id).map_err(|_| bolt_integer_overflow(id))?,
                // Parenthesised deliberately: a method call binds tighter than
                // unary minus, so `-1_i64.saturating_sub(n)` reads as
                // `-(1 - n)`, which climbs into the positive ids that real
                // relationships occupy.
                None => (-1_i64).saturating_sub(index as i64),
            };
            Ok(BoltUnboundRelationship {
                id,
                rel_type: relationship.edge_type.clone(),
                properties: property_map_to_bolt(&relationship.properties)?,
                element_id: relationship.id.map_or_else(
                    || format!("structural:{}:{}:{}", relationship.edge_type, relationship.src, relationship.dst),
                    |id| id.to_string(),
                ),
            })
        })
        .collect::<std::result::Result<Vec<_>, BoltError>>()?;
    let mut indices = Vec::with_capacity(path.relationships.len().saturating_mul(2));
    for (index, relationship) in path.relationships.iter().enumerate() {
        let from = path.nodes[index].id;
        let to = path.nodes[index + 1].id;
        let relationship_index =
            i64::try_from(index + 1).map_err(|_| BoltError::Backend("native path contains too many relationships for Bolt".to_string()))?;
        let signed_index = if relationship.src == from && relationship.dst == to {
            relationship_index
        } else if relationship.src == to && relationship.dst == from {
            -relationship_index
        } else {
            return Err(BoltError::Backend(
                "native path relationship does not connect adjacent nodes".to_string(),
            ));
        };
        indices.push(signed_index);
        indices.push(i64::try_from(index + 1).map_err(|_| BoltError::Backend("native path contains too many nodes for Bolt".to_string()))?);
    }
    Ok(BoltPath {
        nodes,
        rels: relationships,
        indices,
    })
}

fn property_map_to_bolt(properties: &std::collections::BTreeMap<String, VertexPropertyValue>) -> std::result::Result<BoltDict, BoltError> {
    properties
        .iter()
        .map(|(key, value)| Ok((key.clone(), property_value_to_bolt(value)?)))
        .collect()
}

fn property_value_to_bolt(value: &VertexPropertyValue) -> std::result::Result<BoltValue, BoltError> {
    match value {
        VertexPropertyValue::Integer(value) => i64::try_from(*value)
            .map(BoltValue::Integer)
            .map_err(|_| bolt_integer_overflow(*value)),
        VertexPropertyValue::SignedInteger(value) => Ok(BoltValue::Integer(*value)),
        VertexPropertyValue::Bool(value) => Ok(BoltValue::Boolean(*value)),
        VertexPropertyValue::Float(value) => Ok(BoltValue::Float(value.0)),
        VertexPropertyValue::String(value) => Ok(BoltValue::String(value.clone())),
        VertexPropertyValue::Date(days) => Ok(BoltValue::Date(BoltDate { days: i64::from(*days) })),
        VertexPropertyValue::List(values) => match values {
            crate::VertexPropertyList::Empty => Ok(BoltValue::List(Vec::new())),
            crate::VertexPropertyList::Integer(values) => values
                .iter()
                .map(|value| match value {
                    crate::VertexPropertyInteger::Unsigned(value) => i64::try_from(*value)
                        .map(BoltValue::Integer)
                        .map_err(|_| bolt_integer_overflow(*value)),
                    crate::VertexPropertyInteger::Signed(value) => Ok(BoltValue::Integer(*value)),
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map(BoltValue::List),
            crate::VertexPropertyList::Bool(values) => Ok(BoltValue::List(values.iter().copied().map(BoltValue::Boolean).collect())),
            crate::VertexPropertyList::Float(values) => Ok(BoltValue::List(values.iter().map(|value| BoltValue::Float(value.0)).collect())),
            crate::VertexPropertyList::String(values) => Ok(BoltValue::List(values.iter().cloned().map(BoltValue::String).collect())),
            crate::VertexPropertyList::Date(values) => Ok(BoltValue::List(
                values
                    .iter()
                    .map(|days| BoltValue::Date(BoltDate { days: i64::from(*days) }))
                    .collect(),
            )),
        },
    }
}

fn bolt_integer_overflow(value: u64) -> BoltError {
    BoltError::Query {
        code: "Neo.ClientError.Statement.TypeError".to_string(),
        message: format!("value {value} exceeds Bolt's signed 64-bit integer range"),
    }
}

pub(super) fn explicit_transactions_unsupported() -> BoltError {
    BoltError::Transaction("explicit transactions are not supported; use auto-commit RUN queries".to_string())
}

pub(super) fn graph_error_to_bolt(error: GraphError) -> BoltError {
    match error {
        GraphError::GraphScopeAccessDenied { .. } => BoltError::Forbidden(error.to_string()),
        GraphError::AdmissionRejected { .. } => BoltError::ResourceExhausted(error.to_string()),
        GraphError::SnapshotAhead { .. } => BoltError::Query {
            code: "Neo.TransientError.Transaction.BookmarkTimeout".to_string(),
            message: error.to_string(),
        },
        GraphError::SnapshotUnavailable { .. } => BoltError::Query {
            code: "Neo.DatabaseError.Statement.ExecutionFailed".to_string(),
            message: error.to_string(),
        },
        // The query is supported; a write-back of a property it reads or
        // writes has started and not completed, and a later run of it clears
        // the refusal. A syntax code would tell the driver never to retry.
        GraphError::UnsupportedQuery {
            reason: QueryFailureReason::WriteBackInProgress,
            ..
        }
        | GraphError::GqlQuery {
            reason: QueryFailureReason::WriteBackInProgress,
            ..
        } => BoltError::Query {
            code: hydradb_protocol::codes::DATABASE_UNAVAILABLE.to_string(),
            message: error.to_string(),
        },
        // The query is supported; the index generation it reads trails the
        // WAL further than a read will walk, until the indexer publishes a
        // newer one. A syntax code would tell the driver never to retry.
        GraphError::UnsupportedQuery {
            reason: QueryFailureReason::IndexBehind,
            ..
        }
        | GraphError::GqlQuery {
            reason: QueryFailureReason::IndexBehind,
            ..
        } => BoltError::Query {
            code: hydradb_protocol::codes::DATABASE_UNAVAILABLE.to_string(),
            message: error.to_string(),
        },
        GraphError::InvalidKeyComponent { .. }
        | GraphError::MissingQueryParameter { .. }
        | GraphError::QueryParse { .. }
        | GraphError::UnsupportedQuery { .. }
        | GraphError::GqlQuery { .. }
        | GraphError::UnclassifiedQuery { .. } => BoltError::Query {
            code: "Neo.ClientError.Statement.InvalidSyntax".to_string(),
            message: error.to_string(),
        },
        // The query is fine; two of its rows set one property to different
        // values. A syntax code sends the caller to the wrong place.
        GraphError::BatchValueConflict { .. } => BoltError::Query {
            code: "Neo.ClientError.Statement.ArgumentError".to_string(),
            message: error.to_string(),
        },
        GraphError::QueryTimeout { .. } => BoltError::Query {
            code: "Neo.TransientError.Transaction.Terminated".to_string(),
            message: error.to_string(),
        },
        GraphError::AtomicVertexDeleteConflict => BoltError::Query {
            code: "Neo.TransientError.Transaction.DeadlockDetected".to_string(),
            message: error.to_string(),
        },
        // A shard rejection alone says nothing about earlier clauses in the statement.
        GraphError::ConditionalWriteConflict {
            operation: "validate_vertex_delete_preparation",
            ..
        } => BoltError::Query {
            code: "Neo.DatabaseError.Statement.ExecutionFailed".to_string(),
            message: "vertex delete preparation conflicted; earlier writes may have succeeded".to_string(),
        },
        GraphError::IdempotencyConflict { .. } => BoltError::Query {
            code: "Neo.ClientError.Transaction.Invalid".to_string(),
            message: error.to_string(),
        },
        #[cfg(feature = "datafusion-cypher-engine")]
        GraphError::DataFusionProvider(provider_error) => {
            use hydradb_graph_provider::GraphProviderError;

            let message = provider_error.to_string();
            match provider_error {
                GraphProviderError::Conflict { retryable: true } => BoltError::Query {
                    code: "Neo.TransientError.Transaction.DeadlockDetected".to_string(),
                    message,
                },
                GraphProviderError::Conflict { retryable: false } | GraphProviderError::IdempotencyConflict => BoltError::Query {
                    code: "Neo.ClientError.Transaction.Invalid".to_string(),
                    message,
                },
                GraphProviderError::BudgetExceeded { .. } | GraphProviderError::BatchTooLarge { .. } => {
                    BoltError::ResourceExhausted(message)
                }
                GraphProviderError::DeadlineExceeded | GraphProviderError::Cancelled => BoltError::Query {
                    code: "Neo.TransientError.Transaction.Terminated".to_string(),
                    message,
                },
                GraphProviderError::DurabilityUnknown { .. }
                | GraphProviderError::DurabilityNotConfirmed
                | GraphProviderError::PartialCompletion { .. } => BoltError::Query {
                    code: "Neo.DatabaseError.Statement.ExecutionFailed".to_string(),
                    message,
                },
                GraphProviderError::UnsupportedCapability { .. } | GraphProviderError::UnsupportedReceiptVersion { .. } => {
                    BoltError::Query {
                        code: "Neo.ClientError.Statement.Unsupported".to_string(),
                        message,
                    }
                }
                GraphProviderError::InvalidRequest(_) => BoltError::Query {
                    code: "Neo.ClientError.Statement.TypeError".to_string(),
                    message,
                },
                _ => BoltError::Backend("internal query execution error".to_string()),
            }
        }
        // Touch point (c). Drivers already know this code: discard the routing
        // table, re-route, retry. Without it a refused write arrives as an
        // opaque backend error and the driver retries into the same wrong node
        // until it gives up. The owner rides in the message, TiKV's
        // `NotLeader { leader_hint }` — and is absent, not guessed, when this
        // node has shed its view and has nothing truthful to point at.
        GraphError::NotCellWriter { .. } => BoltError::Query {
            code: hydradb_protocol::codes::NOT_A_LEADER.to_string(),
            message: error.to_string(),
        },
        // A routing refusal is transient and belongs to *this node*, so the
        // driver must move to the next router rather than fail the query. A
        // `ClientError` here would be read as "your request is wrong" and end
        // the attempt — which is the wrong answer for a node that has merely
        // shed its view of the fleet (decision 7) while every peer can still
        // answer. `TransientError` is what the drivers' retry logic is built
        // around, and `DatabaseUnavailable` is the honest reason: this node
        // cannot serve, ask another.
        GraphError::RoutingUnavailable { .. } => BoltError::Query {
            code: "Neo.TransientError.General.DatabaseUnavailable".to_string(),
            message: error.to_string(),
        },
        _ => {
            tracing::warn!(target: "hydradb", error = %error, "Bolt suppressed internal graph error");
            BoltError::Backend("internal query execution error".to_string())
        }
    }
}
