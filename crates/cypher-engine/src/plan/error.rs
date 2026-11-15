use std::{error::Error, fmt};

use hydradb_cypher_ast::{operation, GqlStatus, QueryFailureReason};

pub type GraphPlanResult<T> = Result<T, GraphPlanError>;

/// The bucket travels beside the message rather than inside it, so the
/// message stays free text for logs and the bucket stays a closed label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphPlanError {
    InOperation {
        operation: operation::Operation,
        source: Box<GraphPlanError>,
    },
    /// Valid syntax the engine does not execute: GQLSTATUS `0A000`.
    UnsupportedCypher(QueryFailureReason, String),
    InvalidLogicalPlan(QueryFailureReason, String),
    MissingParameter(String),
    /// A query the language itself rejects -- a Syntax Rule it violates, or a
    /// constant that raises a data exception before any row is read -- with
    /// the condition it raises.
    Rejected {
        status: GqlStatus,
        reason: QueryFailureReason,
        message: String,
        /// For `42002`, the name that did not resolve.
        reference: Option<String>,
    },
}

impl GraphPlanError {
    pub fn in_operation(self, operation: operation::Operation) -> Self {
        if self.operation() != operation::UNRECOGNIZED || operation == operation::UNRECOGNIZED {
            self
        } else {
            Self::InOperation {
                operation,
                source: Box::new(self),
            }
        }
    }

    pub fn operation(&self) -> operation::Operation {
        match self {
            Self::InOperation { operation, .. } => *operation,
            _ => operation::UNRECOGNIZED,
        }
    }

    pub(crate) fn detail(&self) -> String {
        match self {
            Self::InOperation { source, .. } => source.detail(),
            Self::Rejected { message, .. } => message.clone(),
            other => other.to_string(),
        }
    }
    /// A query that violates a Syntax Rule with no more specific condition
    /// (§4.7.3): `42000`.
    pub fn syntax(reason: QueryFailureReason, message: impl Into<String>) -> Self {
        Self::Rejected {
            status: GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            reason,
            message: message.into(),
            reference: None,
        }
    }

    /// A reference to `name`, which is not in scope: `42002`.
    pub fn invalid_reference(reason: QueryFailureReason, name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Rejected {
            status: GqlStatus::INVALID_REFERENCE,
            reason,
            message: message.into(),
            reference: Some(name.into()),
        }
    }

    /// A data exception raised while planning, for example by a constant
    /// `LIMIT -1`.
    pub fn data(status: GqlStatus, reason: QueryFailureReason, message: impl Into<String>) -> Self {
        Self::Rejected {
            status,
            reason,
            message: message.into(),
            reference: None,
        }
    }

    pub fn failure_reason(&self) -> QueryFailureReason {
        match self {
            Self::InOperation { source, .. } => source.failure_reason(),
            Self::UnsupportedCypher(reason, _) | Self::InvalidLogicalPlan(reason, _) | Self::Rejected { reason, .. } => *reason,
            Self::MissingParameter(_) => QueryFailureReason::Parameter,
        }
    }

    /// The GQLSTATUS condition this failure raises.
    pub fn gql_status(&self) -> GqlStatus {
        match self {
            Self::InOperation { source, .. } => source.gql_status(),
            Self::UnsupportedCypher(..) => GqlStatus::FEATURE_NOT_SUPPORTED,
            Self::InvalidLogicalPlan(..) => GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            // A parameter the request did not supply is a reference to a
            // general parameter that does not exist.
            Self::MissingParameter(_) => GqlStatus::INVALID_REFERENCE,
            Self::Rejected { status, .. } => *status,
        }
    }

    /// The name a `42002` failure could not resolve.
    pub fn invalid_reference_name(&self) -> Option<&str> {
        match self {
            Self::InOperation { source, .. } => source.invalid_reference_name(),
            Self::MissingParameter(name) => Some(name),
            Self::Rejected { reference, .. } => reference.as_deref(),
            Self::UnsupportedCypher(..) | Self::InvalidLogicalPlan(..) => None,
        }
    }
}

impl fmt::Display for GraphPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InOperation { source, .. } => source.fmt(formatter),
            Self::UnsupportedCypher(_, reason) => {
                write!(formatter, "unsupported Cypher AST: {reason}")
            }
            Self::InvalidLogicalPlan(_, reason) => {
                write!(formatter, "invalid logical plan: {reason}")
            }
            Self::MissingParameter(name) => write!(formatter, "missing query parameter ${name}"),
            Self::Rejected { status, message, .. } => write!(formatter, "{status} {}: {message}", status.standard_description()),
        }
    }
}

impl Error for GraphPlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InOperation { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}
