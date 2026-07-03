//! turbolay error type.
//!
//! One crate-wide [`Error`] wrapping the failure modes of the M0 layer:
//! key/value (de)serialization, the underlying [`common`] storage engine, and
//! id allocation. Keeps `common`'s error types behind our own surface so
//! callers depend on `turbolay::Error`, not on opendata internals.

use common::StorageError;
use common::sequence::SequenceError;
use common::serde::DeserializeError;
use common::serde::encoding::EncodingError;

/// Result alias used throughout turbolay.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced by the turbolay storage foundation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A key or value could not be decoded, or failed a structural invariant
    /// (wrong subsystem/version/record tag, truncated component, etc.).
    #[error("encoding error: {0}")]
    Encoding(String),

    /// The underlying `common`/SlateDB storage engine returned an error.
    #[error("storage error: {0}")]
    Storage(String),

    /// Id allocation (uid / interned id / changelog seq) failed.
    #[error("sequence error: {0}")]
    Sequence(String),

    /// A schema invariant was violated (unknown id on read, corrupt entry).
    #[error("schema error: {0}")]
    Schema(String),
}

impl Error {
    /// Convenience constructor for an encoding failure from any displayable value.
    pub fn encoding(msg: impl std::fmt::Display) -> Self {
        Error::Encoding(msg.to_string())
    }

    /// Convenience constructor for a schema failure from any displayable value.
    pub fn schema(msg: impl std::fmt::Display) -> Self {
        Error::Schema(msg.to_string())
    }
}

impl From<DeserializeError> for Error {
    fn from(e: DeserializeError) -> Self {
        Error::Encoding(e.message)
    }
}

impl From<EncodingError> for Error {
    fn from(e: EncodingError) -> Self {
        Error::Encoding(e.message)
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::Storage(e.to_string())
    }
}

impl From<SequenceError> for Error {
    fn from(e: SequenceError) -> Self {
        Error::Sequence(e.to_string())
    }
}
