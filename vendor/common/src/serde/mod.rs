//! Serialization utilities for OpenData.

pub mod encoding;
pub mod key_prefix;
pub mod record_tag;
pub mod seq_block;
pub mod sortable;
pub mod subsystem;
pub mod terminated_bytes;
pub mod varint;

/// Error type for deserialization failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeserializeError {
    pub message: String,
}

impl std::error::Error for DeserializeError {}

impl std::fmt::Display for DeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
