//! Variable-length integer serialization for lexicographically ordered keys.
//!
//! This module provides encoding and decoding for unsigned integers using a compact
//! variable-length format that preserves lexicographic ordering.
//!
//! # Encoding scheme
//!
//! The first byte contains a length code in its high bits, with the remaining bits
//! storing the start of the value in big-endian order. Length code `L` means `L + 1`
//! total bytes.
//!
//! ## var_u32 (3-bit length code, 5 data bits in first byte)
//!
//! - First byte: `[LLL][DDDDD]`
//! - Data capacity: `5 + 8*L` bits
//!
//! | Length code | Total bytes | Data bits | Value range          |
//! |-------------|-------------|-----------|----------------------|
//! | 0           | 1           | 5         | 0 – 31               |
//! | 1           | 2           | 13        | 32 – 8,191           |
//! | 2           | 3           | 21        | 8,192 – 2,097,151    |
//! | 3           | 4           | 29        | 2M – 536M            |
//! | 4           | 5           | 37        | 536M – 2³²-1         |
//!
//! ## var_u64 (4-bit length code, 4 data bits in first byte)
//!
//! - First byte: `[LLLL][DDDD]`
//! - Data capacity: `4 + 8*L` bits
//!
//! | Length code | Total bytes | Data bits | Value range          |
//! |-------------|-------------|-----------|----------------------|
//! | 0           | 1           | 4         | 0 – 15               |
//! | 1           | 2           | 12        | 16 – 4,095           |
//! | 2           | 3           | 20        | 4,096 – 1,048,575    |
//! | 3           | 4           | 28        | 1M – 268M            |
//! | 4           | 5           | 36        | 268M – 68B           |
//! | 5           | 6           | 44        | 68B – 17T            |
//! | 6           | 7           | 52        | 17T – 4P             |
//! | 7           | 8           | 60        | 4P – 1E              |
//! | 8           | 9           | 68        | 1E – 2⁶⁴-1           |
//!
//! This ensures lexicographic byte comparison equals numeric comparison.

/// Variable-length u32 serialization.
///
/// Uses 3-bit length codes 0-4 (5 total bytes maximum), with 5 data bits in
/// the first byte.
///
/// | Length code | Total bytes | Data bits | Value range          |
/// |-------------|-------------|-----------|----------------------|
/// | 0           | 1           | 5         | 0 – 31               |
/// | 1           | 2           | 13        | 32 – 8,191           |
/// | 2           | 3           | 21        | 8,192 – 2,097,151    |
/// | 3           | 4           | 29        | 2M – 536M            |
/// | 4           | 5           | 37        | 536M – 2³²-1         |
pub mod var_u32 {
    use bytes::{BufMut, BytesMut};

    use crate::serde::DeserializeError;

    /// Maximum length code for u32 (5 bytes total, 37 data bits)
    const MAX_LEN_CODE: u8 = 4;

    /// Number of data bits in the first byte
    const FIRST_BYTE_DATA_BITS: u32 = 5;

    /// Mask for extracting data bits from first byte
    const FIRST_BYTE_DATA_MASK: u8 = 0x1F;

    /// Thresholds for each length code. Value < THRESHOLDS[i] uses length code i.
    const THRESHOLDS: [u64; 5] = [
        1 << 5,  // 32
        1 << 13, // 8,192
        1 << 21, // 2,097,152
        1 << 29, // 536,870,912
        1 << 37, // 137,438,953,472 (exceeds u32::MAX, so length code 4 covers the rest)
    ];

    #[inline]
    fn length_code(x: u32) -> u8 {
        let x = x as u64;
        for (i, &threshold) in THRESHOLDS.iter().enumerate() {
            if x < threshold {
                return i as u8;
            }
        }
        MAX_LEN_CODE
    }

    /// Serializes a u32 with 3-bit length code for lexicographic ordering.
    pub fn serialize(value: u32, buf: &mut BytesMut) {
        let len_code = length_code(value);
        let total_bytes = len_code as usize + 1;

        // Build all bytes into a stack-allocated array
        let mut bytes = [0u8; 5]; // max 5 bytes for var_u32

        // First byte: [LLL][DDDDD] - 3 bits length, 5 bits data
        let shift = 8 * (total_bytes - 1);
        let first_data_bits = ((value as u64) >> shift) as u8 & FIRST_BYTE_DATA_MASK;
        bytes[0] = (len_code << FIRST_BYTE_DATA_BITS) | first_data_bits;

        // Remaining bytes in big-endian order.
        // Using indexed loop instead of iter_mut().enumerate() for better performance;
        // the compiler optimizes bounds checks better with direct indexing here.
        #[allow(clippy::needless_range_loop)]
        for i in 1..total_bytes {
            bytes[i] = ((value >> (8 * (total_bytes - 1 - i))) & 0xFF) as u8;
        }

        buf.put_slice(&bytes[..total_bytes]);
    }

    /// Deserializes a var_u32 from a buffer, advancing past the consumed bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is empty (missing first byte)
    /// - The length code exceeds 4
    /// - The buffer doesn't contain enough bytes
    /// - The decoded value exceeds u32::MAX
    pub fn deserialize(buf: &mut &[u8]) -> Result<u32, DeserializeError> {
        if buf.is_empty() {
            return Err(DeserializeError {
                message: "unexpected end of input: missing var_u32 first byte".to_string(),
            });
        }

        let first_byte = buf[0];
        let len_code = first_byte >> FIRST_BYTE_DATA_BITS;

        if len_code > MAX_LEN_CODE {
            return Err(DeserializeError {
                message: format!(
                    "invalid var_u32 length code: {} (expected 0..{})",
                    len_code, MAX_LEN_CODE
                ),
            });
        }

        let total_bytes = len_code as usize + 1;
        if buf.len() < total_bytes {
            return Err(DeserializeError {
                message: format!(
                    "unexpected end of input: expected {} bytes, got {}",
                    total_bytes,
                    buf.len()
                ),
            });
        }

        // Extract the 5 data bits from first byte
        let mut value = (first_byte & FIRST_BYTE_DATA_MASK) as u64;

        // Extract remaining bytes
        for i in 1..total_bytes {
            value = (value << 8) | (buf[i] as u64);
        }

        if value > u32::MAX as u64 {
            return Err(DeserializeError {
                message: format!("var_u32 value {} exceeds u32::MAX", value),
            });
        }

        *buf = &buf[total_bytes..];
        Ok(value as u32)
    }

    #[cfg(test)]
    mod tests {
        use proptest::prelude::*;

        use super::*;

        // Property tests

        proptest! {
            #[test]
            fn should_roundtrip_any_value(value: u32) {
                let mut buf = BytesMut::new();
                serialize(value, &mut buf);

                let mut slice = buf.as_ref();
                let decoded = deserialize(&mut slice).unwrap();

                prop_assert_eq!(decoded, value);
                prop_assert!(slice.is_empty());
            }

            #[test]
            fn should_preserve_ordering(a: u32, b: u32) {
                let mut buf_a = BytesMut::new();
                let mut buf_b = BytesMut::new();
                serialize(a, &mut buf_a);
                serialize(b, &mut buf_b);

                prop_assert_eq!(
                    a.cmp(&b),
                    buf_a.as_ref().cmp(buf_b.as_ref()),
                    "ordering mismatch: a={}, b={}, enc_a={:?}, enc_b={:?}",
                    a,
                    b,
                    buf_a.as_ref(),
                    buf_b.as_ref()
                );
            }
        }

        // Concrete encoding tests (for documentation and spec verification)

        #[test]
        fn should_encode_boundary_values_correctly() {
            let cases: &[(u32, &[u8])] = &[
                // Length code 0: 1 byte, values 0-31 (5 data bits)
                (0, &[0x00]),
                (31, &[0x1F]),
                // Length code 1: 2 bytes, values 32-8191 (13 data bits)
                (32, &[0x20, 0x20]),
                (8191, &[0x3F, 0xFF]),
                // Length code 2: 3 bytes, values 8192-2097151 (21 data bits)
                (8192, &[0x40, 0x20, 0x00]),
                (2097151, &[0x5F, 0xFF, 0xFF]),
                // Length code 3: 4 bytes, values 2097152-536870911 (29 data bits)
                (2097152, &[0x60, 0x20, 0x00, 0x00]),
                (536870911, &[0x7F, 0xFF, 0xFF, 0xFF]),
                // Length code 4: 5 bytes, values 536870912-u32::MAX (37 data bits)
                (536870912, &[0x80, 0x20, 0x00, 0x00, 0x00]),
                // u32::MAX only needs 32 bits, so first 5 data bits are 0
                (u32::MAX, &[0x80, 0xFF, 0xFF, 0xFF, 0xFF]),
            ];

            for &(value, expected) in cases {
                let mut buf = BytesMut::new();
                serialize(value, &mut buf);
                assert_eq!(buf.as_ref(), expected, "encoding mismatch for {value:#x}");
            }
        }

        // Error case tests

        #[test]
        fn should_fail_deserialize_empty_buffer() {
            let mut slice: &[u8] = &[];
            let result = deserialize(&mut slice);
            assert!(result.is_err());
        }

        #[test]
        fn should_fail_deserialize_invalid_length_code() {
            // Length codes 5-7 are invalid for u32 (3 bits can encode 0-7)
            for len_code in 5..=7u8 {
                let data = &[len_code << 5, 0x00, 0x00, 0x00, 0x00, 0x00];
                let mut slice = &data[..];
                assert!(deserialize(&mut slice).is_err());
            }
        }

        #[test]
        fn should_fail_deserialize_truncated_payload() {
            // Length code 1 means 2 bytes total, but only 1 provided
            // First byte: 0b001_00000 = 0x20
            let data = &[0x20];
            let mut slice = &data[..];
            assert!(deserialize(&mut slice).is_err());
        }

        #[test]
        fn should_advance_buffer_past_consumed_bytes() {
            // Value 5 (length code 0, 1 byte) followed by extra bytes
            let data = &[0x05, 0xDE, 0xAD];
            let mut slice = &data[..];

            let decoded = deserialize(&mut slice).unwrap();

            assert_eq!(decoded, 5);
            assert_eq!(slice, &[0xDE, 0xAD]);
        }
    }
}

/// Variable-length u64 serialization.
///
/// Uses length codes 0-8 (9 total bytes maximum).
///
/// | Length code | Total bytes | Data bits | Value range        |
/// |-------------|-------------|-----------|-------------------|
/// | 0           | 1           | 4         | 0 – 15            |
/// | 1           | 2           | 12        | 16 – 4,095        |
/// | 2           | 3           | 20        | 4,096 – 1,048,575 |
/// | 3           | 4           | 28        | 1M – 268M         |
/// | 4           | 5           | 36        | 268M – 68B        |
/// | 5           | 6           | 44        | 68B – 17T         |
/// | 6           | 7           | 52        | 17T – 4P          |
/// | 7           | 8           | 60        | 4P – 1E           |
/// | 8           | 9           | 68        | 1E – 2⁶⁴-1        |
pub mod var_u64 {
    use bytes::{BufMut, BytesMut};

    use crate::serde::DeserializeError;

    /// Maximum length code for u64 (9 bytes total, 68 data bits)
    const MAX_LEN_CODE: u8 = 8;

    /// Thresholds for each length code. Value < THRESHOLDS[i] uses length code i.
    const THRESHOLDS: [u64; 9] = [
        1 << 4,   // 16
        1 << 12,  // 4,096
        1 << 20,  // 1,048,576
        1 << 28,  // 268,435,456
        1 << 36,  // 68,719,476,736
        1 << 44,  // 17,592,186,044,416
        1 << 52,  // 4,503,599,627,370,496
        1 << 60,  // 1,152,921,504,606,846,976
        u64::MAX, // Sentinel: length code 8 covers all remaining values
    ];

    #[inline]
    fn length_code(x: u64) -> u8 {
        for (i, &threshold) in THRESHOLDS.iter().enumerate() {
            if x < threshold {
                return i as u8;
            }
        }
        MAX_LEN_CODE
    }

    /// Serializes a u64 with 4-bit length code for lexicographic ordering.
    pub fn serialize(value: u64, buf: &mut BytesMut) {
        let len_code = length_code(value);
        let total_bytes = len_code as usize + 1;

        // Build all bytes into a stack-allocated array
        let mut bytes = [0u8; 9]; // max 9 bytes for var_u64

        // First byte: [LLLL][DDDD] - 4 bits length, 4 bits data
        let shift = 8 * (total_bytes - 1);
        let first_data_bits = if shift >= 64 {
            0
        } else {
            (value >> shift) as u8 & 0x0F
        };
        bytes[0] = (len_code << 4) | first_data_bits;

        // Remaining bytes in big-endian order.
        // Using indexed loop instead of iter_mut().enumerate() for better performance;
        // the compiler optimizes bounds checks better with direct indexing here.
        #[allow(clippy::needless_range_loop)]
        for i in 1..total_bytes {
            bytes[i] = ((value >> (8 * (total_bytes - 1 - i))) & 0xFF) as u8;
        }

        buf.put_slice(&bytes[..total_bytes]);
    }

    /// Deserializes a var_u64 from a buffer, advancing past the consumed bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is empty (missing first byte)
    /// - The length code exceeds 8
    /// - The buffer doesn't contain enough bytes
    pub fn deserialize(buf: &mut &[u8]) -> Result<u64, DeserializeError> {
        if buf.is_empty() {
            return Err(DeserializeError {
                message: "unexpected end of input: missing var_u64 first byte".to_string(),
            });
        }

        let first_byte = buf[0];
        let len_code = first_byte >> 4;

        if len_code > MAX_LEN_CODE {
            return Err(DeserializeError {
                message: format!(
                    "invalid var_u64 length code: {} (expected 0..{})",
                    len_code, MAX_LEN_CODE
                ),
            });
        }

        let total_bytes = len_code as usize + 1;
        if buf.len() < total_bytes {
            return Err(DeserializeError {
                message: format!(
                    "unexpected end of input: expected {} bytes, got {}",
                    total_bytes,
                    buf.len()
                ),
            });
        }

        // Extract the 4 data bits from first byte
        let mut value = (first_byte & 0x0F) as u64;

        // Extract remaining bytes
        for i in 1..total_bytes {
            value = (value << 8) | (buf[i] as u64);
        }

        *buf = &buf[total_bytes..];
        Ok(value)
    }

    #[cfg(test)]
    mod tests {
        use proptest::prelude::*;

        use super::*;

        // Property tests

        proptest! {
            #[test]
            fn should_roundtrip_any_value(value: u64) {
                let mut buf = BytesMut::new();
                serialize(value, &mut buf);

                let mut slice = buf.as_ref();
                let decoded = deserialize(&mut slice).unwrap();

                prop_assert_eq!(decoded, value);
                prop_assert!(slice.is_empty());
            }

            #[test]
            fn should_preserve_ordering(a: u64, b: u64) {
                let mut buf_a = BytesMut::new();
                let mut buf_b = BytesMut::new();
                serialize(a, &mut buf_a);
                serialize(b, &mut buf_b);

                prop_assert_eq!(
                    a.cmp(&b),
                    buf_a.as_ref().cmp(buf_b.as_ref()),
                    "ordering mismatch: a={}, b={}, enc_a={:?}, enc_b={:?}",
                    a,
                    b,
                    buf_a.as_ref(),
                    buf_b.as_ref()
                );
            }
        }

        // Concrete encoding tests (for documentation and spec verification)

        #[test]
        fn should_encode_boundary_values_correctly() {
            let cases: &[(u64, &[u8])] = &[
                // Length code 0: 1 byte, values 0-15
                (0, &[0x00]),
                (15, &[0x0F]),
                // Length code 1: 2 bytes, values 16-4095
                (16, &[0x10, 0x10]),
                (4095, &[0x1F, 0xFF]),
                // Length code 2: 3 bytes, values 4096-1048575
                (4096, &[0x20, 0x10, 0x00]),
                (1048575, &[0x2F, 0xFF, 0xFF]),
                // Length code 3: 4 bytes
                (1048576, &[0x30, 0x10, 0x00, 0x00]),
                (268435455, &[0x3F, 0xFF, 0xFF, 0xFF]),
                // Length code 4: 5 bytes
                (268435456, &[0x40, 0x10, 0x00, 0x00, 0x00]),
                (68719476735, &[0x4F, 0xFF, 0xFF, 0xFF, 0xFF]),
                // Length code 5: 6 bytes
                (68719476736, &[0x50, 0x10, 0x00, 0x00, 0x00, 0x00]),
                (17592186044415, &[0x5F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
                // Length code 6: 7 bytes
                (17592186044416, &[0x60, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00]),
                (
                    4503599627370495,
                    &[0x6F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                ),
                // Length code 7: 8 bytes
                (
                    4503599627370496,
                    &[0x70, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
                ),
                (
                    1152921504606846975,
                    &[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                ),
                // Length code 8: 9 bytes
                (
                    1152921504606846976, // 2^60
                    &[0x80, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
                ),
                // u64::MAX only needs 64 bits, so first 4 data bits are 0
                (
                    u64::MAX,
                    &[0x80, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                ),
            ];

            for &(value, expected) in cases {
                let mut buf = BytesMut::new();
                serialize(value, &mut buf);
                assert_eq!(buf.as_ref(), expected, "encoding mismatch for {value:#x}");
            }
        }

        // Error case tests

        #[test]
        fn should_fail_deserialize_empty_buffer() {
            let mut slice: &[u8] = &[];
            let result = deserialize(&mut slice);
            assert!(result.is_err());
        }

        #[test]
        fn should_fail_deserialize_invalid_length_code() {
            // Length codes 9-15 are invalid for u64
            for len_code in 9..=15u8 {
                let data = &[
                    len_code << 4,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                ];
                let mut slice = &data[..];
                assert!(deserialize(&mut slice).is_err());
            }
        }

        #[test]
        fn should_fail_deserialize_truncated_payload() {
            // Length code 1 means 2 bytes total, but only 1 provided
            let data = &[0x10];
            let mut slice = &data[..];
            assert!(deserialize(&mut slice).is_err());
        }

        #[test]
        fn should_advance_buffer_past_consumed_bytes() {
            // Value 5 (length code 0, 1 byte) followed by extra bytes
            let data = &[0x05, 0xDE, 0xAD];
            let mut slice = &data[..];

            let decoded = deserialize(&mut slice).unwrap();

            assert_eq!(decoded, 5);
            assert_eq!(slice, &[0xDE, 0xAD]);
        }
    }
}
