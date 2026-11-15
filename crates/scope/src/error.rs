/// Why a scope, or one of its spellings, was refused.
///
/// The kernel maps each variant to a specific `GraphError`
/// (`From<ScopeError> for GraphError` in `src/core/error.rs`), and that choice
/// fixes the error text, class and protocol code graph-node callers see.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ScopeError {
    /// A component outside `[A-Za-z0-9_.-]`, empty, or otherwise unusable.
    #[error("invalid {component} key component: {value}")]
    InvalidKeyComponent { component: &'static str, value: String },
    /// A structurally impossible value, such as an empty namespace path.
    #[error("corrupt value at {key}: {reason}")]
    CorruptValue { key: String, reason: String },
    /// A namespace path deeper than [`crate::MAX_NAMESPACE_DEPTH`].
    #[error("namespace depth {actual} exceeds limit {limit}")]
    DepthExceeded { actual: u64, limit: u64 },
    /// A Bolt database name that does not resolve to a scope.
    #[error("unknown graph database {database}")]
    UnknownDatabase { database: String },
}

/// `Result` with a [`ScopeError`].
pub type ScopeResult<T> = std::result::Result<T, ScopeError>;
