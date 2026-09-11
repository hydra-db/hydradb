use thiserror::Error;

use crate::{EdgeMetadata, VertexPropertyValue};

/// How an edge without the configured valid-from property is handled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MissingTemporalPropertyPolicy {
    Error,
    Exclude,
}

/// Valid-time options for a half-open query window `[window_start_ms, window_end_ms)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemporalPredicateConfig {
    pub(crate) valid_from_property: String,
    pub(crate) valid_to_property: String,
    pub(crate) window_start_ms: i64,
    pub(crate) window_end_ms: i64,
    pub(crate) missing_property_policy: MissingTemporalPropertyPolicy,
}

impl TemporalPredicateConfig {
    pub(crate) fn validate(&self) -> Result<(), TemporalPredicateError> {
        if self.valid_from_property.is_empty() {
            return Err(TemporalPredicateError::EmptyPropertyName {
                field: "valid_from_property",
            });
        }
        if self.valid_to_property.is_empty() {
            return Err(TemporalPredicateError::EmptyPropertyName {
                field: "valid_to_property",
            });
        }
        if self.valid_from_property == self.valid_to_property {
            return Err(TemporalPredicateError::DuplicatePropertyName {
                property: self.valid_from_property.clone(),
            });
        }
        if self.window_start_ms >= self.window_end_ms {
            return Err(TemporalPredicateError::InvalidQueryWindow {
                start_ms: self.window_start_ms,
                end_ms: self.window_end_ms,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TemporalPredicateOutcome {
    Admit,
    Exclude,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum TemporalPredicateError {
    #[error("temporal config field {field} must name a relationship property")]
    EmptyPropertyName { field: &'static str },
    #[error("valid-from and valid-to must use different properties, both were {property}")]
    DuplicatePropertyName { property: String },
    #[error("temporal query window must satisfy start < end, received [{start_ms}, {end_ms})")]
    InvalidQueryWindow { start_ms: i64, end_ms: i64 },
    #[error("relationship is missing required temporal property {property}")]
    MissingProperty { property: String },
    #[error("relationship temporal property {property} must be an i64-compatible integer")]
    InvalidPropertyType { property: String },
    #[error(
        "relationship temporal interval must satisfy start <= end, received [{start_ms}, {end_ms})"
    )]
    InvalidEdgeInterval { start_ms: i64, end_ms: i64 },
}

/// Evaluates valid-time overlap without reading storage or advancing traversal.
///
/// Both the edge and query use half-open intervals. An absent valid-to value is
/// interpreted as an unbounded end. The config must be validated before use.
pub(crate) fn evaluate_temporal_predicate(
    edge: &EdgeMetadata,
    config: &TemporalPredicateConfig,
) -> Result<TemporalPredicateOutcome, TemporalPredicateError> {
    config.validate()?;

    let Some(valid_from_value) = edge.properties.get(&config.valid_from_property) else {
        return match config.missing_property_policy {
            MissingTemporalPropertyPolicy::Error => Err(TemporalPredicateError::MissingProperty {
                property: config.valid_from_property.clone(),
            }),
            MissingTemporalPropertyPolicy::Exclude => Ok(TemporalPredicateOutcome::Exclude),
        };
    };
    let edge_start_ms = temporal_integer(valid_from_value).ok_or_else(|| {
        TemporalPredicateError::InvalidPropertyType {
            property: config.valid_from_property.clone(),
        }
    })?;

    let edge_end_ms = edge
        .properties
        .get(&config.valid_to_property)
        .map(|value| {
            temporal_integer(value).ok_or_else(|| TemporalPredicateError::InvalidPropertyType {
                property: config.valid_to_property.clone(),
            })
        })
        .transpose()?;

    if let Some(end_ms) = edge_end_ms {
        if edge_start_ms > end_ms {
            return Err(TemporalPredicateError::InvalidEdgeInterval {
                start_ms: edge_start_ms,
                end_ms,
            });
        }
    }

    let overlaps = edge_start_ms < config.window_end_ms
        && edge_end_ms.is_none_or(|end_ms| end_ms > config.window_start_ms);
    Ok(if overlaps {
        TemporalPredicateOutcome::Admit
    } else {
        TemporalPredicateOutcome::Exclude
    })
}

fn temporal_integer(value: &VertexPropertyValue) -> Option<i64> {
    match value {
        VertexPropertyValue::SignedInteger(value) => Some(*value),
        VertexPropertyValue::Integer(value) => i64::try_from(*value).ok(),
        VertexPropertyValue::Bool(_)
        | VertexPropertyValue::Float(_)
        | VertexPropertyValue::String(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(policy: MissingTemporalPropertyPolicy) -> TemporalPredicateConfig {
        TemporalPredicateConfig {
            valid_from_property: "valid_from_ms".to_string(),
            valid_to_property: "valid_to_ms".to_string(),
            window_start_ms: 100,
            window_end_ms: 200,
            missing_property_policy: policy,
        }
    }

    fn edge(start_ms: Option<i64>, end_ms: Option<i64>) -> EdgeMetadata {
        let mut metadata = EdgeMetadata::default();
        if let Some(start_ms) = start_ms {
            metadata =
                metadata.with_property("valid_from_ms", VertexPropertyValue::from_i64(start_ms));
        }
        if let Some(end_ms) = end_ms {
            metadata = metadata.with_property("valid_to_ms", VertexPropertyValue::from_i64(end_ms));
        }
        metadata
    }

    fn evaluate(start_ms: Option<i64>, end_ms: Option<i64>) -> TemporalPredicateOutcome {
        evaluate_temporal_predicate(
            &edge(start_ms, end_ms),
            &config(MissingTemporalPropertyPolicy::Error),
        )
        .expect("valid temporal edge")
    }

    #[test]
    fn edge_start_equal_to_query_start_is_admitted() {
        assert_eq!(
            evaluate(Some(100), Some(150)),
            TemporalPredicateOutcome::Admit
        );
    }

    #[test]
    fn edge_start_equal_to_query_end_is_excluded() {
        assert_eq!(
            evaluate(Some(200), Some(250)),
            TemporalPredicateOutcome::Exclude
        );
    }

    #[test]
    fn edge_end_equal_to_query_start_is_excluded() {
        assert_eq!(
            evaluate(Some(50), Some(100)),
            TemporalPredicateOutcome::Exclude
        );
    }

    #[test]
    fn edge_spanning_query_window_is_admitted() {
        assert_eq!(
            evaluate(Some(50), Some(250)),
            TemporalPredicateOutcome::Admit
        );
    }

    #[test]
    fn edge_strictly_inside_query_window_is_admitted() {
        assert_eq!(
            evaluate(Some(125), Some(175)),
            TemporalPredicateOutcome::Admit
        );
    }

    #[test]
    fn absent_valid_to_is_open_ended() {
        assert_eq!(evaluate(Some(150), None), TemporalPredicateOutcome::Admit);
    }

    #[test]
    fn missing_valid_from_under_error_policy_is_an_error() {
        assert_eq!(
            evaluate_temporal_predicate(
                &edge(None, Some(150)),
                &config(MissingTemporalPropertyPolicy::Error),
            ),
            Err(TemporalPredicateError::MissingProperty {
                property: "valid_from_ms".to_string(),
            })
        );
    }

    #[test]
    fn missing_valid_from_under_exclude_policy_is_excluded() {
        assert_eq!(
            evaluate_temporal_predicate(
                &edge(None, Some(150)),
                &config(MissingTemporalPropertyPolicy::Exclude),
            ),
            Ok(TemporalPredicateOutcome::Exclude)
        );
    }

    #[test]
    fn reversed_edge_interval_is_an_error_under_both_missing_policies() {
        for policy in [
            MissingTemporalPropertyPolicy::Error,
            MissingTemporalPropertyPolicy::Exclude,
        ] {
            assert_eq!(
                evaluate_temporal_predicate(&edge(Some(300), Some(100)), &config(policy)),
                Err(TemporalPredicateError::InvalidEdgeInterval {
                    start_ms: 300,
                    end_ms: 100,
                })
            );
        }
    }

    #[test]
    fn non_integer_temporal_property_is_an_error() {
        let metadata = EdgeMetadata::default().with_property(
            "valid_from_ms",
            VertexPropertyValue::String("100".to_string()),
        );
        assert_eq!(
            evaluate_temporal_predicate(&metadata, &config(MissingTemporalPropertyPolicy::Exclude),),
            Err(TemporalPredicateError::InvalidPropertyType {
                property: "valid_from_ms".to_string(),
            })
        );
    }

    #[test]
    fn unsigned_temporal_property_above_i64_max_is_an_error() {
        let metadata = EdgeMetadata::default().with_property(
            "valid_from_ms",
            VertexPropertyValue::Integer(i64::MAX as u64 + 1),
        );
        assert_eq!(
            evaluate_temporal_predicate(&metadata, &config(MissingTemporalPropertyPolicy::Error),),
            Err(TemporalPredicateError::InvalidPropertyType {
                property: "valid_from_ms".to_string(),
            })
        );
    }

    #[test]
    fn invalid_query_window_is_rejected() {
        for (start_ms, end_ms) in [(200, 100), (200, 200)] {
            let mut config = config(MissingTemporalPropertyPolicy::Error);
            config.window_start_ms = start_ms;
            config.window_end_ms = end_ms;
            assert_eq!(
                config.validate(),
                Err(TemporalPredicateError::InvalidQueryWindow { start_ms, end_ms })
            );
        }
    }

    #[test]
    fn property_names_are_validated() {
        let mut temporal_config = config(MissingTemporalPropertyPolicy::Error);
        temporal_config.valid_from_property.clear();
        assert_eq!(
            temporal_config.validate(),
            Err(TemporalPredicateError::EmptyPropertyName {
                field: "valid_from_property",
            })
        );

        temporal_config.valid_from_property = "valid_to_ms".to_string();
        assert_eq!(
            temporal_config.validate(),
            Err(TemporalPredicateError::DuplicatePropertyName {
                property: "valid_to_ms".to_string(),
            })
        );
    }
}
