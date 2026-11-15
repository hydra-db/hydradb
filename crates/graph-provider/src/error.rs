use thiserror::Error;

/// Resources that can be limited independently by a provider request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetResource {
    Batches,
    Bytes,
    Rows,
    WorkUnits,
}

impl std::fmt::Display for BudgetResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Batches => "batches",
            Self::Bytes => "bytes",
            Self::Rows => "rows",
            Self::WorkUnits => "work units",
        })
    }
}

/// Errors surfaced across the provider boundary.
///
/// Backend-specific errors should be mapped to `Backend` with a stable code;
/// callers should not need to parse vendor error strings to make retry or
/// public-error decisions.
#[derive(Debug, Error)]
pub enum GraphProviderError {
    #[error("graph not found")]
    GraphNotFound,

    #[error("unsupported graph provider capability: {capability}")]
    UnsupportedCapability { capability: &'static str },

    #[error("invalid provider request: {0}")]
    InvalidRequest(String),

    #[error("invalid canonical graph value: {0}")]
    GraphValue(#[from] hydradb_graph_types::CodecError),

    #[error("continuation token does not match this snapshot or operation")]
    InvalidContinuationToken,

    #[error("snapshot is no longer available")]
    SnapshotUnavailable,

    #[error("graph entity requested by input row {input_ordinal} is missing")]
    MissingEntity { input_ordinal: u64 },

    #[error("provider budget exceeded for {resource} (limit {limit})")]
    BudgetExceeded { resource: BudgetResource, limit: u64 },

    #[error("provider batch exceeded its bound: {actual} {resource}, maximum {maximum}")]
    BatchTooLarge { resource: &'static str, actual: u64, maximum: u64 },

    #[error("provider request was cancelled")]
    Cancelled,

    #[error("provider request deadline elapsed")]
    DeadlineExceeded,

    #[error("mutation conflict (retryable: {retryable})")]
    Conflict { retryable: bool },

    #[error("mutation receipt durability is not confirmed")]
    DurabilityNotConfirmed,

    #[error("mutation durability is unknown at sequence {sequence:?}: {message}")]
    DurabilityUnknown { sequence: Option<u64>, message: String },

    #[error("mutation partially completed action ranges {completed_ranges:?}: {cause}")]
    PartialCompletion {
        completed_ranges: Vec<(u64, u64)>,
        #[source]
        cause: Box<GraphProviderError>,
    },

    #[error("unsupported mutation receipt version {version}")]
    UnsupportedReceiptVersion { version: u16 },

    #[error("idempotency identity was reused for a different request")]
    IdempotencyConflict,

    #[error("provider backend error {code} (retryable: {retryable}): {message}")]
    Backend { code: String, message: String, retryable: bool },
}

pub type Result<T, E = GraphProviderError> = std::result::Result<T, E>;
