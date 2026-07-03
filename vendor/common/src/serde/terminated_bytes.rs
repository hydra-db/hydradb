//! Terminated bytes serialization for lexicographically ordered keys.
//!
//! This module provides encoding and decoding for variable-length byte sequences
//! that preserves lexicographic ordering. Keys are escaped and terminated with `0x00`:
//!
//! - `0x00` → `0x01 0x01`
//! - `0x01` → `0x01 0x02`
//! - All other bytes unchanged
//! - Terminated with `0x00` delimiter
//!
//! Using `0x00` as the terminator ensures shorter keys sort before longer
//! keys with the same prefix (e.g., "/foo" < "/foo/bar").
//!
//! For prefix-based range queries, use `crate::bytes::lex_increment` to compute
//! the exclusive upper bound.

use bytes::{BufMut, Bytes, BytesMut};

use super::DeserializeError;
use crate::BytesRange;
use crate::bytes::lex_increment;
use std::ops::Bound::{Excluded, Included, Unbounded};

/// Terminator byte for terminated bytes encoding (lowest byte value)
const TERMINATOR_BYTE: u8 = 0x00;

/// Escape character for terminated bytes encoding
const ESCAPE_BYTE: u8 = 0x01;

/// Serializes raw bytes with escape sequences and `0x00` terminator.
///
/// Writes directly to the provided buffer. The encoding:
/// - `0x00` → `0x01 0x01`
/// - `0x01` → `0x01 0x02`
/// - All other bytes unchanged
/// - Terminated with `0x00`
///
/// This encoding preserves lexicographic ordering of the original byte sequences.
pub fn serialize_to_bytes(data: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    serialize(data, &mut buf);
    buf.freeze()
}

/// Serializes raw bytes with escape sequences and `0x00` terminator.
///
/// Writes directly to the provided buffer. The encoding:
/// - `0x00` → `0x01 0x01`
/// - `0x01` → `0x01 0x02`
/// - All other bytes unchanged
/// - Terminated with `0x00`
///
/// This encoding preserves lexicographic ordering of the original byte sequences.
pub fn serialize(data: &[u8], buf: &mut BytesMut) {
    for &byte in data {
        match byte {
            TERMINATOR_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x01);
            }
            ESCAPE_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x02);
            }
            _ => buf.put_u8(byte),
        }
    }
    buf.put_u8(TERMINATOR_BYTE);
}

/// Deserializes terminated bytes from a buffer, advancing past the terminator.
///
/// Returns the decoded raw bytes. The input buffer is advanced past the
/// terminator byte.
///
/// # Errors
///
/// Returns an error if:
/// - The buffer contains an incomplete escape sequence
/// - The buffer contains an invalid escape sequence
/// - The buffer is missing the terminator byte
pub fn deserialize(buf: &mut &[u8]) -> Result<Bytes, DeserializeError> {
    let mut result = BytesMut::new();
    let mut i = 0;

    while i < buf.len() {
        let byte = buf[i];

        if byte == TERMINATOR_BYTE {
            // Found terminator, consume it and return
            *buf = &buf[i + 1..];
            return Ok(result.freeze());
        }

        if byte == ESCAPE_BYTE {
            // Escape sequence - need next byte
            if i + 1 >= buf.len() {
                return Err(DeserializeError {
                    message: "truncated escape sequence in terminated bytes".to_string(),
                });
            }
            let next = buf[i + 1];
            match next {
                0x01 => result.put_u8(TERMINATOR_BYTE),
                0x02 => result.put_u8(ESCAPE_BYTE),
                _ => {
                    return Err(DeserializeError {
                        message: format!("invalid escape sequence: 0x01 0x{:02x}", next),
                    });
                }
            }
            i += 2;
        } else {
            result.put_u8(byte);
            i += 1;
        }
    }

    Err(DeserializeError {
        message: "unterminated bytes sequence (missing 0x00 terminator)".to_string(),
    })
}

/// Returns the position just past the terminator that closes a
/// terminated-bytes-encoded segment within `bytes`, starting the scan at
/// `start`.
///
/// Walks the encoded stream byte-by-byte, treating `ESCAPE_BYTE` (`0x01`)
/// as an escape that consumes the following byte regardless of value, and
/// stopping at the first `TERMINATOR_BYTE` (`0x00`) outside an escape
/// sequence. Returns `None` if the input ends before the terminator is
/// reached, including the case where the last byte is the start of an
/// escape sequence (truncation-unsafe — the next byte could shift the
/// terminator's position).
///
/// Unlike [`deserialize`], this function does not decode the payload — it
/// only locates the boundary. Callers that need just the end offset (e.g.
/// a prefix extractor sizing a hashable prefix) can use this without
/// allocating.
pub fn find_terminator_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i < bytes.len() {
        match bytes[i] {
            TERMINATOR_BYTE => return Some(i + 1),
            ESCAPE_BYTE => {
                // Escape byte consumes the next byte. If the stream ends
                // mid-escape we cannot decide where the terminator lands.
                i += 2;
                if i > bytes.len() {
                    return None;
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// Creates a [`BytesRange`] for scanning all keys with the given logical prefix.
///
/// The prefix is first incremented using `lex_increment`, then both bounds
/// are serialized using [`serialize`].
///
/// # Examples
///
/// ```ignore
/// // Find all keys starting with "user:"
/// let range = prefix_range(b"user:");
/// storage.scan(range).await?;
/// ```
pub fn prefix_range(prefix: &[u8]) -> BytesRange {
    if prefix.is_empty() {
        BytesRange::unbounded()
    } else {
        let start = serialize_to_bytes(prefix);
        match lex_increment(prefix) {
            Some(end) => BytesRange::new(Included(start), Excluded(serialize_to_bytes(&end))),
            None => BytesRange::new(Included(start), Unbounded),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    // Property tests

    proptest! {
        #[test]
        fn should_roundtrip_any_bytes(data: Vec<u8>) {
            let mut buf = BytesMut::new();
            serialize(&data, &mut buf);

            let mut slice = buf.as_ref();
            let decoded = deserialize(&mut slice).unwrap();

            prop_assert_eq!(decoded.as_ref(), data.as_slice());
            prop_assert!(slice.is_empty());
        }

        #[test]
        fn should_preserve_ordering(a: Vec<u8>, b: Vec<u8>) {
            let mut buf_a = BytesMut::new();
            let mut buf_b = BytesMut::new();
            serialize(&a, &mut buf_a);
            serialize(&b, &mut buf_b);

            prop_assert_eq!(
                a.cmp(&b),
                buf_a.as_ref().cmp(buf_b.as_ref()),
                "ordering mismatch: a={:?}, b={:?}, enc_a={:?}, enc_b={:?}",
                a,
                b,
                buf_a.as_ref(),
                buf_b.as_ref()
            );
        }

        #[test]
        fn should_prefix_range_contain_all_prefixed_keys(prefix: Vec<u8>, suffix: Vec<u8>) {
            prop_assume!(!prefix.is_empty());

            let mut full_key = prefix.clone();
            full_key.extend(&suffix);

            let range = prefix_range(&prefix);
            let serialized = serialize_to_bytes(&full_key);

            prop_assert!(
                range.contains(&serialized),
                "prefix_range({:?}) should contain {:?} (serialized: {:?})",
                prefix,
                full_key,
                serialized
            );
        }
    }

    // Concrete encoding tests (for spec verification)

    #[test]
    fn should_encode_special_bytes_correctly() {
        // 0x00 -> 0x01 0x01
        let mut buf = BytesMut::new();
        serialize(&[0x00], &mut buf);
        assert_eq!(buf.as_ref(), &[0x01, 0x01, 0x00]);

        // 0x01 -> 0x01 0x02
        buf.clear();
        serialize(&[0x01], &mut buf);
        assert_eq!(buf.as_ref(), &[0x01, 0x02, 0x00]);

        // 0xFF -> unchanged (just 0xFF + terminator)
        buf.clear();
        serialize(&[0xFF], &mut buf);
        assert_eq!(buf.as_ref(), &[0xFF, 0x00]);

        // empty -> just terminator
        buf.clear();
        serialize(&[], &mut buf);
        assert_eq!(buf.as_ref(), &[0x00]);
    }

    // Error case tests

    #[test]
    fn should_fail_deserialize_without_terminator() {
        let mut slice: &[u8] = &[0x61, 0x62, 0x63];
        assert!(deserialize(&mut slice).is_err());
    }

    #[test]
    fn should_fail_deserialize_with_truncated_escape() {
        let mut slice: &[u8] = &[0x61, 0x01];
        assert!(deserialize(&mut slice).is_err());
    }

    #[test]
    fn should_fail_deserialize_with_invalid_escape_sequence() {
        let mut slice: &[u8] = &[0x61, 0x01, 0x03, 0x00];
        assert!(deserialize(&mut slice).is_err());
    }

    #[test]
    fn should_advance_buffer_past_consumed_bytes() {
        let data = &[0x61, 0x00, 0xDE, 0xAD]; // "a" + terminator + extra
        let mut slice = &data[..];

        let decoded = deserialize(&mut slice).unwrap();

        assert_eq!(decoded.as_ref(), b"a");
        assert_eq!(slice, &[0xDE, 0xAD]);
    }

    #[test]
    fn should_find_terminator_end_on_simple_input() {
        // given — serialize "abc" so we know exactly where the terminator lands
        let mut buf = BytesMut::new();
        serialize(b"abc", &mut buf);
        // expected layout: 'a' 'b' 'c' 0x00 (4 bytes)

        // when / then — the terminator end sits one byte past the terminator
        assert_eq!(find_terminator_end(&buf, 0), Some(4));
    }

    #[test]
    fn should_find_terminator_end_skipping_escaped_zero_byte() {
        // given — payload contains 0x00 which encodes to 0x01 0x01, then "x",
        // then the real terminator. The inner 0x01 must NOT be mistaken for
        // the terminator.
        let mut buf = BytesMut::new();
        serialize(&[0x00, b'x'], &mut buf);
        // layout: 0x01 0x01 'x' 0x00 (4 bytes)

        // when / then
        assert_eq!(find_terminator_end(&buf, 0), Some(4));
    }

    #[test]
    fn should_find_terminator_end_skipping_escaped_escape_byte() {
        // given — payload contains 0x01 which encodes to 0x01 0x02. The 0x02
        // looks innocuous but the escape pair must be consumed atomically so
        // the position is correctly tracked.
        let mut buf = BytesMut::new();
        serialize(&[0x01, b'x'], &mut buf);
        // layout: 0x01 0x02 'x' 0x00 (4 bytes)

        // when / then
        assert_eq!(find_terminator_end(&buf, 0), Some(4));
    }

    #[test]
    fn should_find_terminator_end_from_offset_start() {
        // given — a synthetic header followed by an encoded payload. Searching
        // from offset 3 skips past the header bytes (which may legally contain
        // 0x00).
        let mut buf = BytesMut::from(&[0x00, 0xFF, 0x42][..]);
        serialize(b"key", &mut buf);
        // layout: [00 FF 42] [k e y 00]; payload terminator at index 6, end at 7

        // when / then
        assert_eq!(find_terminator_end(&buf, 3), Some(7));
    }

    #[test]
    fn should_return_none_when_terminator_absent() {
        // given — bytes without a terminator
        let bytes: &[u8] = b"abc";

        // when / then
        assert_eq!(find_terminator_end(bytes, 0), None);
    }

    #[test]
    fn should_return_none_when_input_ends_mid_escape() {
        // given — last byte is the escape byte itself
        let bytes: &[u8] = &[b'a', ESCAPE_BYTE];

        // when / then
        assert_eq!(find_terminator_end(bytes, 0), None);
    }

    #[test]
    fn should_return_none_when_start_is_past_end() {
        // given
        let bytes: &[u8] = &[0x00];

        // when / then
        assert_eq!(find_terminator_end(bytes, 5), None);
    }

    #[test]
    fn should_not_have_encoded_prefix_collision() {
        // Encoded "a" should not be a prefix of encoded "ab"
        let mut buf_a = BytesMut::new();
        serialize(b"a", &mut buf_a);
        let mut buf_ab = BytesMut::new();
        serialize(b"ab", &mut buf_ab);

        assert!(!buf_ab.starts_with(&buf_a));
    }
}
