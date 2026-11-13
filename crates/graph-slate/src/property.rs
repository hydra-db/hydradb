use hydradb_graph_types::{CypherValue, DateValue};

use crate::{GraphError, QueryFailureReason, QueryFloat, Result, VertexPropertyInteger, VertexPropertyList, VertexPropertyValue};

pub fn property_to_cypher(value: &VertexPropertyValue) -> CypherValue {
    match value {
        VertexPropertyValue::Integer(value) => CypherValue::Integer(i128::from(*value)),
        VertexPropertyValue::SignedInteger(value) => CypherValue::Integer(i128::from(*value)),
        VertexPropertyValue::Bool(value) => CypherValue::Boolean(*value),
        VertexPropertyValue::Float(value) => CypherValue::Float(value.0),
        VertexPropertyValue::String(value) => CypherValue::String(value.clone()),
        VertexPropertyValue::Date(days_since_epoch) => CypherValue::Date(DateValue {
            days_since_epoch: *days_since_epoch,
        }),
        VertexPropertyValue::List(values) => CypherValue::List(match values {
            VertexPropertyList::Empty => Vec::new(),
            VertexPropertyList::Integer(values) => values
                .iter()
                .map(|value| match value {
                    VertexPropertyInteger::Unsigned(value) => CypherValue::Integer(i128::from(*value)),
                    VertexPropertyInteger::Signed(value) => CypherValue::Integer(i128::from(*value)),
                })
                .collect(),
            VertexPropertyList::Bool(values) => values.iter().copied().map(CypherValue::Boolean).collect(),
            VertexPropertyList::Float(values) => values.iter().map(|value| CypherValue::Float(value.0)).collect(),
            VertexPropertyList::String(values) => values.iter().cloned().map(CypherValue::String).collect(),
            VertexPropertyList::Date(values) => values
                .iter()
                .copied()
                .map(|days_since_epoch| CypherValue::Date(DateValue { days_since_epoch }))
                .collect(),
        }),
    }
}

pub fn storage_property(value: &CypherValue) -> Result<VertexPropertyValue> {
    match value {
        CypherValue::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        CypherValue::Integer(value) => storage_integer(*value).map(|value| match value {
            VertexPropertyInteger::Unsigned(value) => VertexPropertyValue::Integer(value),
            VertexPropertyInteger::Signed(value) => VertexPropertyValue::SignedInteger(value),
        }),
        CypherValue::Float(value) => Ok(VertexPropertyValue::Float(QueryFloat(*value))),
        CypherValue::String(value) => Ok(VertexPropertyValue::String(value.clone())),
        CypherValue::Date(value) if value.to_iso_string().is_some() => Ok(VertexPropertyValue::Date(value.days_since_epoch)),
        CypherValue::Date(_) => unsupported_property("property dates must be inside the supported calendar range"),
        CypherValue::List(values) => storage_list(values).map(VertexPropertyValue::List),
        _ => unsupported_property("properties support scalar values, dates, and flat homogeneous scalar lists"),
    }
}

fn storage_integer(value: i128) -> Result<VertexPropertyInteger> {
    u64::try_from(value)
        .map(VertexPropertyInteger::Unsigned)
        .or_else(|_| i64::try_from(value).map(VertexPropertyInteger::Signed))
        .map_err(|_| unsupported_property_error("SlateDB properties cannot store this i128 value"))
}

fn storage_list(values: &[CypherValue]) -> Result<VertexPropertyList> {
    if values.len() > crate::MAX_PROPERTY_LIST_ELEMENTS {
        return Err(crate::property_list_too_long_error(values.len()));
    }
    let Some(first) = values.first() else {
        return Ok(VertexPropertyList::Empty);
    };
    match first {
        CypherValue::Integer(_) => values
            .iter()
            .map(|value| match value {
                CypherValue::Integer(value) => storage_integer(*value),
                _ => unsupported_property("property lists must contain one element type"),
            })
            .collect::<Result<Vec<_>>>()
            .map(VertexPropertyList::Integer),
        CypherValue::Boolean(_) => values
            .iter()
            .map(|value| match value {
                CypherValue::Boolean(value) => Ok(*value),
                _ => unsupported_property("property lists must contain one element type"),
            })
            .collect::<Result<Vec<_>>>()
            .map(VertexPropertyList::Bool),
        CypherValue::Float(_) => values
            .iter()
            .map(|value| match value {
                CypherValue::Float(value) if value.is_finite() => Ok(QueryFloat(*value)),
                CypherValue::Float(_) => unsupported_property("property float lists require finite values"),
                _ => unsupported_property("property lists must contain one element type"),
            })
            .collect::<Result<Vec<_>>>()
            .map(VertexPropertyList::Float),
        CypherValue::String(_) => values
            .iter()
            .map(|value| match value {
                CypherValue::String(value) => Ok(value.clone()),
                _ => unsupported_property("property lists must contain one element type"),
            })
            .collect::<Result<Vec<_>>>()
            .map(VertexPropertyList::String),
        CypherValue::Date(_) => values
            .iter()
            .map(|value| match value {
                CypherValue::Date(value) if value.to_iso_string().is_some() => Ok(value.days_since_epoch),
                CypherValue::Date(_) => unsupported_property("property dates must be inside the supported calendar range"),
                _ => unsupported_property("property lists must contain one element type"),
            })
            .collect::<Result<Vec<_>>>()
            .map(VertexPropertyList::Date),
        _ => unsupported_property("property lists cannot contain null, nested, mixed, graph, map, or unsupported values"),
    }
}

fn unsupported_property<T>(feature: &'static str) -> Result<T> {
    Err(unsupported_property_error(feature))
}

fn unsupported_property_error(feature: &'static str) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Mutation,
        dialect: "Cypher25",
        feature: feature.into(),
    }
}
