//! Byte utilities for key encoding and range queries.

use bytes::{Bytes, BytesMut};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

/// Computes the lexicographic successor of a byte sequence.
///
/// Returns the smallest byte sequence that is strictly greater than the input.
/// Returns `None` if no such sequence exists (i.e., input is empty or all `0xFF` bytes).
///
/// This is useful for computing exclusive upper bounds in range queries.
/// For example, to query all keys with prefix "foo", use the range `["foo", lex_lex_increment("foo"))`.
///
/// # Algorithm
///
/// Starting from the rightmost byte:
/// - If it's less than `0xFF`, increment it and return
/// - If it's `0xFF`, remove it and try to increment the previous byte
/// - If all bytes are `0xFF` (or input is empty), return `None`
///
/// # Examples
///
/// - `[0x61]` ("a") → `Some([0x62])` ("b")
/// - `[0x61, 0xFF]` → `Some([0x62])`
/// - `[0xFF]` → `None`
/// - `[]` → `None`
pub(crate) fn lex_increment(data: &[u8]) -> Option<Bytes> {
    if data.is_empty() {
        return None;
    }

    let mut result = BytesMut::from(data);

    // Work backwards, looking for a byte we can increment
    while let Some(last) = result.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(result.freeze());
        }
        // Last byte is 0xFF, truncate it and try the previous byte
        result.truncate(result.len() - 1);
    }

    // All bytes were 0xFF, no valid increment exists
    None
}

/// A range over byte sequences, used for key range queries.
#[derive(Clone, Debug)]
pub struct BytesRange {
    pub start: Bound<Bytes>,
    pub end: Bound<Bytes>,
}

impl BytesRange {
    pub fn new(start: Bound<Bytes>, end: Bound<Bytes>) -> Self {
        Self { start, end }
    }

    /// Creates a range that includes all keys with the given prefix.
    pub fn prefix(prefix: Bytes) -> Self {
        if prefix.is_empty() {
            Self::unbounded()
        } else {
            match lex_increment(&prefix) {
                Some(end) => Self {
                    start: Included(prefix),
                    end: Excluded(end),
                },
                None => Self {
                    start: Included(prefix),
                    end: Unbounded,
                },
            }
        }
    }

    pub fn contains(&self, k: &[u8]) -> bool {
        (match &self.start {
            Included(s) => k >= s,
            Excluded(s) => k > s,
            Unbounded => true,
        }) && (match &self.end {
            Included(e) => k <= e,
            Excluded(e) => k < e,
            Unbounded => true,
        })
    }

    /// Creates a range that scans everything.
    pub fn unbounded() -> Self {
        Self {
            start: Unbounded,
            end: Unbounded,
        }
    }

    /// Builds the absolute key range for a prefix scan narrowed to `subrange`.
    ///
    /// `subrange` bounds are key *suffixes* interpreted relative to `prefix`: a
    /// bound `s` selects the full key `prefix ++ s`. An unbounded subrange end
    /// falls back to the prefix's natural upper bound (its lexicographic
    /// successor), so `from_prefix_and_subrange(p, &BytesRange::unbounded())`
    /// equals [`Self::prefix`].
    ///
    /// This mirrors SlateDB's own `scan_prefix` subrange semantics so the
    /// in-memory fallback in [`StorageRead::scan_prefix_iter`] agrees with the
    /// SlateDB path byte-for-byte.
    ///
    /// [`StorageRead::scan_prefix_iter`]: crate::storage::StorageRead::scan_prefix_iter
    pub fn from_prefix_and_subrange(prefix: &[u8], subrange: &BytesRange) -> Self {
        let concat = |suffix: &Bytes| -> Bytes {
            let mut key = BytesMut::with_capacity(prefix.len() + suffix.len());
            key.extend_from_slice(prefix);
            key.extend_from_slice(suffix);
            key.freeze()
        };
        let start = match &subrange.start {
            Included(s) => Included(concat(s)),
            Excluded(s) => Excluded(concat(s)),
            Unbounded if prefix.is_empty() => Unbounded,
            Unbounded => Included(Bytes::copy_from_slice(prefix)),
        };
        let end = match &subrange.end {
            Included(e) => Included(concat(e)),
            Excluded(e) => Excluded(concat(e)),
            Unbounded => match lex_increment(prefix) {
                Some(successor) => Excluded(successor),
                None => Unbounded,
            },
        };
        Self { start, end }
    }
}

impl RangeBounds<Bytes> for BytesRange {
    fn start_bound(&self) -> Bound<&Bytes> {
        self.start.as_ref()
    }
    fn end_bound(&self) -> Bound<&Bytes> {
        self.end.as_ref()
    }
}

/// SlateDB 0.14 scans are bounded by its own [`slatedb::ByteRangeBounds`] trait
/// rather than `std::ops::RangeBounds`, so we map our bounds onto byte slices.
impl slatedb::ByteRangeBounds for BytesRange {
    fn start_bound(&self) -> Bound<&[u8]> {
        self.start.as_ref().map(Bytes::as_ref)
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        self.end.as_ref().map(Bytes::as_ref)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    // Property tests for increment

    proptest! {
        #[test]
        fn should_increment_produce_strictly_greater_result(data: Vec<u8>) {
            let all_ff = !data.is_empty() && data.iter().all(|&b| b == 0xFF);
            prop_assume!(!data.is_empty() && !all_ff);

            let incremented = lex_increment(&data).unwrap();
            prop_assert!(
                incremented.as_ref() > data.as_slice(),
                "lex_increment({:?}) = {:?} should be > input",
                data,
                incremented
            );
        }

        #[test]
        fn should_increment_produce_immediate_successor(data: Vec<u8>) {
            let all_ff = !data.is_empty() && data.iter().all(|&b| b == 0xFF);
            prop_assume!(!data.is_empty() && !all_ff);

            let incremented = lex_increment(&data).unwrap();

            if let Some(&last) = data.last() {
                if last < 0xFF {
                    let mut expected = data.clone();
                    *expected.last_mut().unwrap() += 1;
                    prop_assert_eq!(incremented.as_ref(), expected.as_slice());
                } else {
                    prop_assert!(incremented.len() < data.len());
                    prop_assert!(data.starts_with(&incremented[..incremented.len() - 1]));
                }
            }
        }

        #[test]
        fn should_prefix_range_contain_all_prefixed_keys(prefix: Vec<u8>, suffix: Vec<u8>) {
            prop_assume!(!prefix.is_empty());

            let range = BytesRange::prefix(Bytes::from(prefix.clone()));

            // The prefix itself should be included
            prop_assert!(range.contains(&prefix));

            // Any key with this prefix should be included
            let mut extended = prefix.clone();
            extended.extend(&suffix);
            prop_assert!(range.contains(&extended));
        }
    }

    // Concrete increment tests

    #[test]
    fn should_increment_simple_byte() {
        assert_eq!(lex_increment(b"a").unwrap().as_ref(), b"b");
        assert_eq!(lex_increment(&[0x00]).unwrap().as_ref(), &[0x01]);
        assert_eq!(lex_increment(&[0xFE]).unwrap().as_ref(), &[0xFF]);
    }

    #[test]
    fn should_increment_with_trailing_ff() {
        assert_eq!(lex_increment(&[0x61, 0xFF]).unwrap().as_ref(), &[0x62]);
        assert_eq!(
            lex_increment(&[0x61, 0xFF, 0xFF]).unwrap().as_ref(),
            &[0x62]
        );
        assert_eq!(
            lex_increment(&[0x00, 0xFF, 0xFF]).unwrap().as_ref(),
            &[0x01]
        );
    }

    #[test]
    fn should_return_none_for_non_incrementable() {
        assert!(lex_increment(&[]).is_none());
        assert!(lex_increment(&[0xFF]).is_none());
        assert!(lex_increment(&[0xFF, 0xFF]).is_none());
    }

    // BytesRange tests

    #[test]
    fn should_create_prefix_range() {
        let range = BytesRange::prefix(Bytes::from("foo"));

        assert!(range.contains(b"foo"));
        assert!(range.contains(b"foobar"));
        assert!(range.contains(b"foo\x00"));
        assert!(range.contains(b"foo\xFF"));

        assert!(!range.contains(b"fo"));
        assert!(!range.contains(b"fop"));
        assert!(!range.contains(b"fop\x00"));
    }

    #[test]
    fn should_handle_prefix_with_trailing_ff() {
        let range = BytesRange::prefix(Bytes::from_static(&[0x61, 0xFF]));

        assert!(range.contains(&[0x61, 0xFF]));
        assert!(range.contains(&[0x61, 0xFF, 0x00]));
        assert!(range.contains(&[0x61, 0xFF, 0xFF]));

        assert!(!range.contains(&[0x61]));
        assert!(!range.contains(&[0x62]));
    }

    #[test]
    fn should_handle_all_ff_prefix() {
        let range = BytesRange::prefix(Bytes::from_static(&[0xFF, 0xFF]));

        // Should be unbounded on the end
        assert!(range.contains(&[0xFF, 0xFF]));
        assert!(range.contains(&[0xFF, 0xFF, 0x00]));
        assert!(range.contains(&[0xFF, 0xFF, 0xFF, 0xFF]));

        assert!(!range.contains(&[0xFF]));
        assert!(!range.contains(&[0xFE, 0xFF]));
    }

    #[test]
    fn should_handle_empty_prefix() {
        let range = BytesRange::prefix(Bytes::new());

        // Empty prefix = unbounded = matches everything
        assert!(range.contains(b""));
        assert!(range.contains(b"anything"));
        assert!(range.contains(&[0xFF, 0xFF, 0xFF]));
    }
}
