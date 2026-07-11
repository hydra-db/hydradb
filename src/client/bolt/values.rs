use boltr::error::BoltError;
use boltr::types::BoltValue;

use super::{ClientBookmark, ClientQueryTarget};
use crate::{GraphError, QueryFloat, QueryParameterValue, QueryValue, VertexPropertyValue};

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

pub(super) fn bolt_parameter_to_property(
    name: &str,
    value: &BoltValue,
) -> std::result::Result<VertexPropertyValue, BoltError> {
    match value {
        BoltValue::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        BoltValue::Integer(value) => Ok(VertexPropertyValue::from_i64(*value)),
        BoltValue::Float(value) if value.is_finite() => {
            Ok(VertexPropertyValue::Float(QueryFloat(*value)))
        }
        BoltValue::String(value) => Ok(VertexPropertyValue::String(value.clone())),
        BoltValue::Float(_) => Err(invalid_bolt_parameter(name, "float must be finite")),
        _ => Err(invalid_bolt_parameter(
            name,
            "only boolean, signed integer, finite float, and string parameters are supported",
        )),
    }
}

pub(super) fn bolt_parameter_to_query_value(
    name: &str,
    value: &BoltValue,
) -> std::result::Result<QueryParameterValue, BoltError> {
    bolt_parameter_to_query_value_at_depth(name, value, 0)
}

fn bolt_parameter_to_query_value_at_depth(
    name: &str,
    value: &BoltValue,
    depth: usize,
) -> std::result::Result<QueryParameterValue, BoltError> {
    if depth > MAX_QUERY_PARAMETER_DEPTH {
        return Err(invalid_bolt_parameter(
            name,
            "nested parameter depth exceeds 16",
        ));
    }
    match value {
        BoltValue::List(values) => values
            .iter()
            .map(|value| bolt_parameter_to_query_value_at_depth(name, value, depth + 1))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(QueryParameterValue::List),
        BoltValue::Dict(values) => values
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    bolt_parameter_to_query_value_at_depth(name, value, depth + 1)?,
                ))
            })
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
    }
}

fn property_value_to_bolt(
    value: &VertexPropertyValue,
) -> std::result::Result<BoltValue, BoltError> {
    match value {
        VertexPropertyValue::Integer(value) => i64::try_from(*value)
            .map(BoltValue::Integer)
            .map_err(|_| bolt_integer_overflow(*value)),
        VertexPropertyValue::SignedInteger(value) => Ok(BoltValue::Integer(*value)),
        VertexPropertyValue::Bool(value) => Ok(BoltValue::Boolean(*value)),
        VertexPropertyValue::Float(value) => Ok(BoltValue::Float(value.0)),
        VertexPropertyValue::String(value) => Ok(BoltValue::String(value.clone())),
    }
}

fn bolt_integer_overflow(value: u64) -> BoltError {
    BoltError::Query {
        code: "Neo.ClientError.Statement.TypeError".to_string(),
        message: format!("value {value} exceeds Bolt's signed 64-bit integer range"),
    }
}

pub(super) fn explicit_transactions_unsupported() -> BoltError {
    BoltError::Transaction(
        "explicit transactions are not supported; use auto-commit RUN queries".to_string(),
    )
}

pub(super) fn graph_error_to_bolt(error: GraphError) -> BoltError {
    match error {
        GraphError::GraphScopeAccessDenied { .. } => BoltError::Forbidden(error.to_string()),
        GraphError::AdmissionRejected { .. } => BoltError::ResourceExhausted(error.to_string()),
        GraphError::SnapshotAhead { .. } => BoltError::Query {
            code: "Neo.TransientError.Transaction.BookmarkTimeout".to_string(),
            message: error.to_string(),
        },
        GraphError::InvalidKeyComponent { .. }
        | GraphError::MissingQueryParameter { .. }
        | GraphError::QueryParse { .. }
        | GraphError::UnsupportedQuery { .. } => BoltError::Query {
            code: "Neo.ClientError.Statement.InvalidSyntax".to_string(),
            message: error.to_string(),
        },
        GraphError::QueryTimeout { .. } => BoltError::Query {
            code: "Neo.TransientError.Transaction.Terminated".to_string(),
            message: error.to_string(),
        },
        _ => {
            tracing::warn!(target: "slatedb_graph_kernel", error = %error, "Bolt suppressed internal graph error");
            BoltError::Backend("internal query execution error".to_string())
        }
    }
}
