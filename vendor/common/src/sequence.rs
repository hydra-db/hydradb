//! Sequence number allocation and persistence.
//!
//! This module provides components for allocating monotonically increasing
//! sequence numbers with crash recovery. It provides:
//!
//! **[`SequenceAllocator`]**: Allocates individual sequence numbers from sequence
//!  blocks stored in storage. Tracks the current reserved block, and allocates new
//!  sequence numbers from it. When the block is out of sequence numbers, the
//!  allocator allocates a new block, and returns the corresponding Record in the
//!  return from `SequenceAllocator#allocate`. It is up to the caller to ensure
//!  that the record is persisted in storage. This allows the allocator to be used
//!  from implementations of `Delta` by including the sequence block writes in
//!  persisted deltas.
//!
//! # Design
//!
//! Block-based allocation reduces write amplification by pre-allocating ranges
//! of sequence numbers instead of persisting after every allocation. The allocator
//! tracks allocations via [`SeqBlock`] records:
//!
//! - `base_sequence`: Starting sequence number of the allocated block
//! - `block_size`: Number of sequence numbers in the block
//!
//! On crash recovery, the next block starts at `base_sequence + block_size`,
//! ensuring monotonicity even if some allocated sequences were unused.
//!
//! # Usage
//!
//! Each system provides its own key format for storing the SeqBlock record:
//!
//! ```ignore
//! use bytes::Bytes;
//! use common::sequence::{SeqBlockStore, SequenceAllocator};
//!
//! // Domain-specific key (e.g., Log uses [0x01, 0x02])
//! const MY_SEQ_BLOCK_KEY: &[u8] = &[0x01, 0x02];
//!
//! let key = Bytes::from_static(MY_SEQ_BLOCK_KEY);
//! let allocator = SequenceAllocator::load(storage.clone(), key);
//!
//! let (seq, put) = allocator.allocate_one().await?;
//! if let Some(put) = put {
//!     storage.put(vec![put]).await.unwrap();
//! }
//! ```

use bytes::Bytes;

use crate::serde::DeserializeError;
use crate::serde::seq_block::SeqBlock;
use crate::{Record, Storage, StorageError};

/// Default block size for sequence allocation.
pub const DEFAULT_BLOCK_SIZE: u64 = 4096;

/// Error type for sequence allocation operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceError {
    /// Storage operation failed
    Storage(StorageError),
    /// Deserialization failed
    Deserialize(DeserializeError),
}

impl std::error::Error for SequenceError {}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SequenceError::Storage(e) => write!(f, "storage error: {}", e),
            SequenceError::Deserialize(e) => write!(f, "deserialize error: {}", e),
        }
    }
}

impl From<StorageError> for SequenceError {
    fn from(err: StorageError) -> Self {
        SequenceError::Storage(err)
    }
}

impl From<DeserializeError> for SequenceError {
    fn from(err: DeserializeError) -> Self {
        SequenceError::Deserialize(err)
    }
}

/// Result type alias for sequence allocation operations.
pub type SequenceResult<T> = std::result::Result<T, SequenceError>;

#[derive(Clone, Debug)]
pub struct AllocatedSeqBlock {
    current_block: Option<SeqBlock>,
    next_sequence: u64,
}

impl AllocatedSeqBlock {
    fn remaining(&self) -> u64 {
        match &self.current_block {
            None => 0,
            Some(block) => block.next_base().saturating_sub(self.next_sequence),
        }
    }

    async fn load(storage: &dyn Storage, key: &Bytes) -> SequenceResult<Self> {
        let current_block = match storage.get(key.clone()).await? {
            Some(record) => Some(SeqBlock::deserialize(&record.value)?),
            None => None,
        };
        let next_sequence = current_block
            .as_ref()
            // in the event that block exists, set the next sequence to the next base
            // so that a new block is immediately allocated
            .map(|b| b.next_base())
            .unwrap_or(0);
        Ok(Self {
            current_block,
            next_sequence,
        })
    }
}

/// Allocates sequence numbers from pre-allocated blocks.
///
/// This allocator manages the lifecycle of sequence number allocation:
/// - Initialize from storage on startup
/// - Allocate individual sequence numbers
/// - Persist new blocks when the current block is exhausted
///
/// # Thread Safety
///
/// This struct is not inherently thread safe. It should be owned by a single writing task
#[derive(Debug)]
pub struct SequenceAllocator {
    key: Bytes,
    block: AllocatedSeqBlock,
}

impl SequenceAllocator {
    /// Creates a new allocator using the given storage and key.
    pub async fn load(storage: &dyn Storage, key: Bytes) -> SequenceResult<Self> {
        let block = AllocatedSeqBlock::load(storage, &key).await?;
        Ok(Self { key, block })
    }

    pub fn new(key: Bytes, block: AllocatedSeqBlock) -> Self {
        Self { key, block }
    }

    /// Returns the next sequence number that would be allocated.
    ///
    /// This does not consume any sequences; it just peeks at the current state.
    pub fn peek_next_sequence(&self) -> u64 {
        self.block.next_sequence
    }

    /// Allocates a single sequence number.
    ///
    /// Convenience method equivalent to `allocate(1)`.
    pub fn allocate_one(&mut self) -> (u64, Option<Record>) {
        self.allocate(1)
    }

    /// Allocates a contiguous range of sequence numbers.
    ///
    /// Returns a pair where the first element is the first sequence number in
    /// the allocated range. The caller can use sequences `base..base+count`.
    /// The second element is an `Option<Record>`. If the current block doesn't
    /// have enough sequences remaining, the remaining sequences are used and a
    /// new block is allocated for the rest. The returned record, if present,
    /// must be put to storage to reserve the full sequence range. Note that since
    /// blocks are contiguous (each starts where the previous ends), the returned
    /// sequence range is always contiguous.
    ///
    pub fn allocate(&mut self, count: u64) -> (u64, Option<Record>) {
        let remaining = self.block.remaining();

        // If current block can satisfy the request, use it
        if remaining >= count {
            let base_sequence = self.block.next_sequence;
            self.block.next_sequence += count;
            return (base_sequence, None);
        }

        // Need a new block. Use remaining sequences from current block first.
        let from_new_block = count - remaining;
        let (new_block, record) = self.init_next_block(from_new_block);

        // Base sequence is either from current block (if any remaining) or new block
        let base_sequence = if remaining > 0 {
            self.block.next_sequence
        } else {
            new_block.base_sequence
        };

        self.block.next_sequence = new_block.base_sequence + from_new_block;
        self.block.current_block = Some(new_block);

        (base_sequence, Some(record))
    }

    fn init_next_block(&self, min_count: u64) -> (SeqBlock, Record) {
        let base_sequence = match &self.block.current_block {
            Some(block) => block.next_base(),
            None => 0,
        };

        let block_size = min_count.max(DEFAULT_BLOCK_SIZE);
        let new_block = SeqBlock::new(base_sequence, block_size);

        let value: Bytes = new_block.serialize();
        let record = Record::new(self.key.clone(), value);
        (new_block, record)
    }

    pub fn freeze(self) -> (Bytes, AllocatedSeqBlock) {
        (self.key, self.block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use opendata_macros::storage_test;

    fn test_key() -> Bytes {
        Bytes::from_static(&[0x01, 0x02])
    }

    #[storage_test]
    async fn should_load_none_when_no_block_allocated(storage: Arc<dyn Storage>) {
        // when
        let block = AllocatedSeqBlock::load(storage.as_ref(), &test_key())
            .await
            .unwrap();

        // then
        assert_eq!(block.next_sequence, 0);
        assert_eq!(block.current_block, None);
    }

    #[storage_test]
    async fn should_load_first_block(storage: Arc<dyn Storage>) {
        // given:
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // when:
        let (seq, record) = allocator.allocate(1);

        // then:
        assert_eq!(seq, 0);
        assert!(record.is_some());
        let record = record.unwrap();
        let block = SeqBlock::deserialize(&record.value).unwrap();
        assert_eq!(block.base_sequence, 0);
        assert_eq!(block.block_size, DEFAULT_BLOCK_SIZE);
    }

    #[storage_test]
    async fn should_allocate_larger_block_when_requested(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // when:
        let large_count = DEFAULT_BLOCK_SIZE * 2;
        let (seq, record) = allocator.allocate(large_count);

        // then:
        assert_eq!(seq, 0);
        assert!(record.is_some());
        let record = record.unwrap();
        let block = SeqBlock::deserialize(&record.value).unwrap();
        assert_eq!(block.base_sequence, seq);
        assert_eq!(block.block_size, DEFAULT_BLOCK_SIZE * 2);
        let (seq, _) = allocator.allocate(1);
        assert_eq!(seq, large_count);
    }

    #[storage_test]
    async fn should_allocate_sequential_blocks(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();
        let mut puts = vec![];

        // when
        for _ in 0..(DEFAULT_BLOCK_SIZE * 3) {
            let (_, maybe_put) = allocator.allocate(1);
            maybe_put.inspect(|r| puts.push(r.clone()));
        }

        // then
        let blocks: Vec<_> = puts
            .into_iter()
            .map(|r| SeqBlock::deserialize(&r.value).unwrap())
            .collect();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].base_sequence, 0);
        assert_eq!(blocks[1].base_sequence, DEFAULT_BLOCK_SIZE);
        assert_eq!(blocks[2].base_sequence, DEFAULT_BLOCK_SIZE * 2);
    }

    #[storage_test]
    async fn should_recover_from_storage_on_initialize(storage: Arc<dyn Storage>) {
        // First instance allocates some blocks
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();
        allocator.allocate(DEFAULT_BLOCK_SIZE);
        let (_, put) = allocator.allocate(DEFAULT_BLOCK_SIZE);
        storage.put(vec![put.unwrap().into()]).await.unwrap();

        // when: Second instance should recover
        let mut allocator2 = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // then:
        assert_eq!(
            allocator2.block.current_block,
            Some(SeqBlock::new(DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE))
        );
        assert_eq!(allocator2.block.next_sequence, DEFAULT_BLOCK_SIZE * 2);
        let (seq, put) = allocator2.allocate(DEFAULT_BLOCK_SIZE);
        let block = SeqBlock::deserialize(&put.unwrap().value).unwrap();
        assert_eq!(block.base_sequence, DEFAULT_BLOCK_SIZE * 2);
        assert_eq!(seq, DEFAULT_BLOCK_SIZE * 2);
    }

    #[storage_test]
    async fn should_resume_from_next_block_on_initialize(storage: Arc<dyn Storage>) {
        // First instance allocates some blocks
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();
        let (_, put) = allocator.allocate(DEFAULT_BLOCK_SIZE / 2);
        storage.put(vec![put.unwrap().into()]).await.unwrap();

        // when: Second instance should recover
        let mut allocator2 = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // then:
        assert_eq!(
            allocator2.block.current_block,
            Some(SeqBlock::new(0, DEFAULT_BLOCK_SIZE))
        );
        assert_eq!(allocator2.block.next_sequence, DEFAULT_BLOCK_SIZE);
        let (seq, put) = allocator2.allocate(DEFAULT_BLOCK_SIZE);
        let block = SeqBlock::deserialize(&put.unwrap().value).unwrap();
        assert_eq!(block.base_sequence, DEFAULT_BLOCK_SIZE);
        assert_eq!(seq, DEFAULT_BLOCK_SIZE);
    }

    #[storage_test]
    async fn should_allocate_sequential_sequence_numbers(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // when
        let (seq1, _) = allocator.allocate_one();
        let (seq2, _) = allocator.allocate_one();
        let (seq3, _) = allocator.allocate_one();

        // then
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(seq3, 2);
    }

    #[storage_test]
    async fn should_allocate_batch_of_sequences(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // when
        let (seq1, _) = allocator.allocate(10);
        let (seq2, _) = allocator.allocate(5);

        // then
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 10);
    }

    #[storage_test]
    async fn should_span_blocks_when_batch_exceeds_remaining(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // Allocate most of the first block
        allocator.allocate(DEFAULT_BLOCK_SIZE - 10);

        // when - allocate more than remaining (10 left, request 25)
        let (seq, put) = allocator.allocate(25);

        // then - should get contiguous sequences starting at remaining position
        assert_eq!(seq, DEFAULT_BLOCK_SIZE - 10);
        assert!(put.is_some());
        let block = SeqBlock::deserialize(&put.unwrap().value).unwrap();
        assert_eq!(block.base_sequence, DEFAULT_BLOCK_SIZE);
    }

    #[storage_test]
    async fn should_allocate_new_block_when_exhausted(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // when - allocate entire first block plus one more
        allocator.allocate(DEFAULT_BLOCK_SIZE);
        let (seq, put) = allocator.allocate_one();

        // then - should be first sequence of second block
        assert_eq!(seq, DEFAULT_BLOCK_SIZE);
        assert!(put.is_some());
    }

    #[storage_test]
    async fn should_allocate_exactly_remaining(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // Use some sequences
        allocator.allocate(100);
        let remaining = DEFAULT_BLOCK_SIZE - 100;

        // when - allocate exactly the remaining amount
        let (seq, put) = allocator.allocate(remaining);

        // then - should use up exactly the current block
        assert_eq!(seq, 100);
        assert!(put.is_none());

        // and next allocation should come from new block
        let (next_seq, put) = allocator.allocate_one();
        assert_eq!(next_seq, DEFAULT_BLOCK_SIZE);
        assert!(put.is_some());
    }

    #[storage_test]
    async fn should_handle_large_batch_spanning_from_partial_block(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // Use most of first block, leaving 100
        allocator.allocate(DEFAULT_BLOCK_SIZE - 100);

        // when - request much more than remaining (spans into new block)
        let large_request = DEFAULT_BLOCK_SIZE + 500;
        let (seq, _) = allocator.allocate(large_request);

        // then - should start at the remaining position
        assert_eq!(seq, DEFAULT_BLOCK_SIZE - 100);

        // and next allocation continues after the large batch
        let (next_seq, _) = allocator.allocate_one();
        assert_eq!(next_seq, DEFAULT_BLOCK_SIZE - 100 + large_request);
    }

    #[storage_test]
    async fn should_peek_next_sequence_without_consuming(storage: Arc<dyn Storage>) {
        // given
        let mut allocator = SequenceAllocator::load(storage.as_ref(), test_key())
            .await
            .unwrap();

        // Allocate some sequences
        allocator.allocate(10);

        // when
        let peeked = allocator.peek_next_sequence();
        let (allocated, _) = allocator.allocate_one();

        // then - peeked should match what was allocated
        assert_eq!(peeked, allocated);
        assert_eq!(peeked, 10);
    }
}
