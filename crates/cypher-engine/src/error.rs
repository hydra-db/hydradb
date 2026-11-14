use hydradb_cypher_ast::{operation, DiagnosticRecord, GqlStatus, GqlStatusObject, QueryFailureReason};
use thiserror::Error;

use crate::GraphPlanError;

pub type EngineResult<T> = Result<T, EngineError>;
pub type StorageResult<T> = Result<T, StorageError>;

/// A failure a storage backend reported while the engine was reading.
///
/// A backend may attach its own typed error. The engine never inspects it;
/// it rides along so the backend's caller can take it back with
/// [`EngineError::into_backend_error`] and answer with the error it would
/// have raised itself -- a timeout, an exhausted budget, a corrupt record --
/// rather than a gap in the query language, which is what a bare message
/// would otherwise be reported as.
#[derive(Debug, Error)]
#[error("graph storage error: {message}")]
pub struct StorageError {
    message: String,
    #[source]
    backend: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl StorageError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            backend: None,
        }
    }

    /// A failure carrying the backend's own error, displayed as its message.
    pub fn from_backend<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self {
            message: error.to_string(),
            backend: Some(Box::new(error)),
        }
    }

    /// The backend's own error, when it attached one of type `E`.
    pub fn backend_error<E: std::error::Error + 'static>(&self) -> Option<&E> {
        self.backend.as_deref().and_then(|error| error.downcast_ref::<E>())
    }

    /// Take the backend's own error back, when it attached one of type `E`.
    pub fn into_backend_error<E: std::error::Error + 'static>(self) -> Result<E, Self> {
        match self.backend {
            Some(backend) => match backend.downcast::<E>() {
                Ok(error) => Ok(*error),
                Err(backend) => Err(Self {
                    message: self.message,
                    backend: Some(backend),
                }),
            },
            None => Err(self),
        }
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("{source}")]
    InOperation {
        operation: operation::Operation,
        source: Box<EngineError>,
    },
    #[error("Cypher 25 parse error: {0}")]
    Parse(String),
    #[error(transparent)]
    Plan(#[from] GraphPlanError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("invalid graph storage response: {0}")]
    InvalidStorageResponse(String),
    #[error("unsupported graph physical plan: {1}")]
    UnsupportedPlan(QueryFailureReason, String),
    /// A data exception raised while evaluating a value: overflow, division
    /// by zero, an operand of the wrong type.
    #[error("{status} {}: {message}", status.standard_description())]
    Data { status: GqlStatus, message: String },
}

impl EngineError {
    pub(crate) fn in_operation(self, operation: operation::Operation) -> Self {
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
            Self::Plan(error) => error.operation(),
            _ => operation::UNRECOGNIZED,
        }
    }

    pub(crate) fn data_exception(&self) -> Option<(GqlStatus, String, operation::Operation)> {
        match self {
            Self::InOperation { source, .. } => source
                .data_exception()
                .map(|(status, message, _)| (status, message, self.operation())),
            Self::Data { status, message } => Some((*status, message.clone(), self.operation())),
            _ => None,
        }
    }

    fn invalid_reference_name(&self) -> Option<&str> {
        match self {
            Self::InOperation { source, .. } => source.invalid_reference_name(),
            Self::Plan(error) => error.invalid_reference_name(),
            _ => None,
        }
    }
    /// The bucket a query-failure metric files this under, or `None` for a
    /// failure that is not the query's.
    ///
    /// Storage failures reach the engine only because the adapter reports
    /// them through it. They are not a gap in the query language, so they get
    /// no reason rather than a guess; the backend's own error, when it
    /// attached one, says what they are -- see [`Self::into_backend_error`].
    pub fn failure_reason(&self) -> Option<QueryFailureReason> {
        match self {
            Self::InOperation { source, .. } => source.failure_reason(),
            Self::Parse(_) => Some(QueryFailureReason::ParseError),
            Self::Plan(error) => Some(error.failure_reason()),
            Self::UnsupportedPlan(reason, _) => Some(*reason),
            Self::Data { .. } => Some(QueryFailureReason::Evaluation),
            Self::Storage(_) | Self::InvalidStorageResponse(_) => None,
        }
    }

    /// A data exception with its condition.
    pub fn data(status: GqlStatus, message: impl Into<String>) -> Self {
        Self::Data {
            status,
            message: message.into(),
        }
    }

    /// The GQLSTATUS condition this failure raises.
    ///
    /// Text that does not parse is invalid syntax (`42001`). Valid syntax the
    /// engine does not execute is `0A000`, "feature not supported". A storage
    /// failure is not the query's, so it is the implementation-defined
    /// general processing exception (`50000`).
    pub fn gql_status(&self) -> GqlStatus {
        match self {
            Self::InOperation { source, .. } => source.gql_status(),
            Self::Parse(_) => GqlStatus::INVALID_SYNTAX,
            Self::Plan(error) => error.gql_status(),
            Self::UnsupportedPlan(QueryFailureReason::Evaluation, _) => GqlStatus::DATA_EXCEPTION,
            Self::UnsupportedPlan(..) => GqlStatus::FEATURE_NOT_SUPPORTED,
            Self::Data { status, .. } => *status,
            Self::Storage(_) | Self::InvalidStorageResponse(_) => GqlStatus::GENERAL_PROCESSING_EXCEPTION,
        }
    }

    /// The GQL-status object a failed outcome reports (§4.9.2): the
    /// condition, its standard description followed by this error's message,
    /// and a diagnostic record (§23.2) naming the operation and, for
    /// `42002`, the reference that did not resolve.
    pub fn status_object(&self) -> GqlStatusObject {
        let status = self.gql_status();
        let (operation, operation_code) = self.operation();
        let invalid_reference = self.invalid_reference_name().map(str::to_string);
        GqlStatusObject::with_detail(status, self.detail()).with_diagnostic(DiagnosticRecord {
            operation,
            operation_code,
            current_schema: None,
            invalid_reference,
            additional: self
                .failure_reason()
                .map(|reason| vec![("_classification".to_string(), reason.as_str().to_string())])
                .unwrap_or_default(),
        })
    }

    /// This error's own message, without any status prefix.
    fn detail(&self) -> String {
        match self {
            Self::InOperation { source, .. } => source.detail(),
            Self::Plan(error) => error.detail(),
            Self::Data { message, .. } => message.clone(),
            other => other.to_string(),
        }
    }

    /// The storage backend's own error of type `E`, when a storage failure
    /// carrying one is what failed the query; otherwise the error unchanged.
    pub fn into_backend_error<E: std::error::Error + 'static>(self) -> Result<E, Self> {
        match self {
            Self::InOperation { operation, source } => source.into_backend_error().map_err(|source| Self::InOperation {
                operation,
                source: Box::new(source),
            }),
            Self::Storage(error) => error.into_backend_error().map_err(Self::Storage),
            other => Err(other),
        }
    }
}
