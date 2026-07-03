//! Sequence block value type for block-based ID allocation.
//!
//! This module provides the [`SeqBlock`] type, which represents a block of
//! allocated sequence numbers. It is used by OpenData systems (Log, Vector, etc.)
//! to implement efficient monotonically increasing ID allocation with crash recovery.
//!
//! # Format
//!
//! The `SeqBlock` value serializes to 16 bytes:
//!
//! ```text
//! | base_sequence (u64 BE) | block_size (u64 BE) |
//! ```
//!
//! The allocated range is `[base_sequence, base_sequence + block_size)`.
//!
//! # Usage
//!
//! Each system provides its own key format for storing the SeqBlock record.
//! For example:
//! - Log uses key `[0x01, 0x02]` (version + record type)
//! - Vector uses key `[0x01, 0x08]` (version + record type)

use bytes::{BufMut, Bytes, BytesMut};

use super::DeserializeError;

/// A block of allocated sequence numbers.
///
/// Stores the current sequence block allocation as two u64 values:
/// - `base_sequence`: Starting sequence number of the allocated block
/// - `block_size`: Number of sequence numbers in the block
///
/// The allocated range is `[base_sequence, base_sequence + block_size)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqBlock {
    /// Base sequence number of the allocated block
    pub base_sequence: u64,
    /// Size of the allocated block
    pub block_size: u64,
}

impl SeqBlock {
    /// Creates a new SeqBlock value.
    pub fn new(base_sequence: u64, block_size: u64) -> Self {
        Self {
            base_sequence,
            block_size,
        }
    }

    /// Encodes the value to bytes.
    ///
    /// Format: `| base_sequence (u64 BE) | block_size (u64 BE) |`
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64(self.base_sequence);
        buf.put_u64(self.block_size);
        buf.freeze()
    }

    /// Decodes a SeqBlock value from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < 16 {
            return Err(DeserializeError {
                message: format!(
                    "buffer too short for SeqBlock value: need 16 bytes, got {}",
                    data.len()
                ),
            });
        }

        let base_sequence = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let block_size = u64::from_be_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);

        Ok(SeqBlock {
            base_sequence,
            block_size,
        })
    }

    /// Returns the next sequence number after this block.
    ///
    /// This is the starting point for the next block allocation.
    pub fn next_base(&self) -> u64 {
        self.base_sequence + self.block_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_serialize_and_deserialize_seq_block() {
        // given
        let value = SeqBlock::new(1000, 100);

        // when
        let serialized = value.serialize();
        let deserialized = SeqBlock::deserialize(&serialized).unwrap();

        // then
        assert_eq!(deserialized, value);
        assert_eq!(serialized.len(), 16);
    }

    #[test]
    fn should_calculate_next_base() {
        // given
        let value = SeqBlock::new(1000, 100);

        // when
        let next = value.next_base();

        // then
        assert_eq!(next, 1100);
    }

    #[test]
    fn should_fail_deserialize_when_buffer_too_short() {
        // given
        let data = vec![0u8; 15]; // need 16 bytes

        // when
        let result = SeqBlock::deserialize(&data);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message
                .contains("buffer too short for SeqBlock value")
        );
    }

    #[test]
    fn should_serialize_in_big_endian() {
        // given
        let value = SeqBlock::new(0x0102030405060708, 0x1112131415161718);

        // when
        let serialized = value.serialize();

        // then
        assert_eq!(
            serialized.as_ref(),
            &[
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // base_sequence
                0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // block_size
            ]
        );
    }

    #[test]
    fn should_handle_zero_values() {
        // given
        let value = SeqBlock::new(0, 0);

        // when
        let serialized = value.serialize();
        let deserialized = SeqBlock::deserialize(&serialized).unwrap();

        // then
        assert_eq!(deserialized.base_sequence, 0);
        assert_eq!(deserialized.block_size, 0);
        assert_eq!(deserialized.next_base(), 0);
    }

    #[test]
    fn should_handle_max_values() {
        // given
        let value = SeqBlock::new(u64::MAX, u64::MAX);

        // when
        let serialized = value.serialize();
        let deserialized = SeqBlock::deserialize(&serialized).unwrap();

        // then
        assert_eq!(deserialized.base_sequence, u64::MAX);
        assert_eq!(deserialized.block_size, u64::MAX);
    }
}
