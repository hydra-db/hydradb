//! Sortable numeric encoding for lexicographic key ordering.
//!
//! This module provides utilities to encode signed integers and floating-point
//! numbers into byte sequences that sort correctly when compared lexicographically
//! (byte-by-byte). This is essential for key-value stores where keys are compared
//! as raw bytes.
//!
//! ## Why Sortable Encoding?
//!
//! Standard binary representations don't sort correctly:
//!
//! - **Signed integers**: Two's complement puts negative numbers after positive
//!   (e.g., -1 = 0xFF...FF sorts after 1 = 0x00...01)
//! - **Floating point**: IEEE 754 has complex sign/exponent/mantissa layout that
//!   doesn't match numeric order (e.g., -0.5 sorts after 1.0 in raw bytes)
//!
//! ## Encoding Schemes
//!
//! ### Signed Integers (i64)
//!
//! XOR with the sign bit mask (`0x8000_0000_0000_0000`):
//! - Flips the sign bit, making negative numbers have 0 in the high bit
//! - Negative numbers (originally 1xxx...) become (0xxx...) and sort first
//! - Positive numbers (originally 0xxx...) become (1xxx...) and sort second
//! - Preserves relative ordering within each group
//!
//! ### Floating Point (f64)
//!
//! IEEE 754 sortable encoding:
//! - If negative (sign bit set): flip ALL bits
//! - If positive (sign bit clear): flip only sign bit
//!
//! This works because:
//! - Positive floats: flipping sign bit puts them in the 1xxx range
//! - Negative floats: flipping all bits reverses their order (more negative → smaller)
//! - Special values (NaN, infinity) are handled correctly
//!
//! ## Usage
//!
//! Encode values before writing to key bytes (big-endian for lexicographic order):
//!
//! ```
//! use common::serde::sortable::{encode_i64_sortable, decode_i64_sortable};
//!
//! let value: i64 = -42;
//! let sortable = encode_i64_sortable(value);
//! let bytes = sortable.to_be_bytes(); // Use big-endian for keys
//!
//! // Later, decode:
//! let recovered = decode_i64_sortable(u64::from_be_bytes(bytes));
//! assert_eq!(recovered, value);
//! ```

/// Encode an i64 value for sortable byte comparison.
///
/// XORs with the sign bit to ensure negative numbers sort before positive.
/// The result should be written in big-endian format for correct lexicographic ordering.
#[inline]
pub const fn encode_i64_sortable(value: i64) -> u64 {
    (value as u64) ^ 0x8000_0000_0000_0000
}

/// Decode a sortable-encoded u64 back to the original i64 value.
#[inline]
pub const fn decode_i64_sortable(sortable: u64) -> i64 {
    (sortable ^ 0x8000_0000_0000_0000) as i64
}

/// Encode an f64 value for sortable byte comparison.
///
/// Uses IEEE 754 sortable encoding:
/// - Negative floats: flip all bits
/// - Positive floats: flip only sign bit
///
/// The result should be written in big-endian format for correct lexicographic ordering.
#[inline]
pub const fn encode_f64_sortable(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & 0x8000_0000_0000_0000 != 0 {
        // Negative: flip all bits
        !bits
    } else {
        // Positive: flip only sign bit
        bits ^ 0x8000_0000_0000_0000
    }
}

/// Decode a sortable-encoded u64 back to the original f64 value.
#[inline]
pub const fn decode_f64_sortable(sortable: u64) -> f64 {
    let bits = if sortable & 0x8000_0000_0000_0000 != 0 {
        // Was positive (high bit now set): flip sign bit
        sortable ^ 0x8000_0000_0000_0000
    } else {
        // Was negative (high bit now clear): flip all bits
        !sortable
    };
    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_i64_values() {
        let values = [
            i64::MIN,
            i64::MIN + 1,
            -1000,
            -1,
            0,
            1,
            1000,
            i64::MAX - 1,
            i64::MAX,
        ];

        for value in values {
            let encoded = encode_i64_sortable(value);
            let decoded = decode_i64_sortable(encoded);
            assert_eq!(decoded, value, "Roundtrip failed for {}", value);
        }
    }

    #[test]
    fn should_preserve_i64_ordering() {
        let values = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];

        for window in values.windows(2) {
            let (a, b) = (window[0], window[1]);
            let encoded_a = encode_i64_sortable(a);
            let encoded_b = encode_i64_sortable(b);
            assert!(
                encoded_a < encoded_b,
                "Ordering violated: {} (0x{:016x}) should be < {} (0x{:016x})",
                a,
                encoded_a,
                b,
                encoded_b
            );
        }
    }

    #[test]
    fn should_roundtrip_f64_values() {
        let values = [
            f64::NEG_INFINITY,
            f64::MIN,
            -1000.5,
            -1.0,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            1000.5,
            f64::MAX,
            f64::INFINITY,
        ];

        for value in values {
            let encoded = encode_f64_sortable(value);
            let decoded = decode_f64_sortable(encoded);
            assert_eq!(
                decoded.to_bits(),
                value.to_bits(),
                "Roundtrip failed for {}",
                value
            );
        }
    }

    #[test]
    fn should_preserve_f64_ordering() {
        let values = [
            f64::NEG_INFINITY,
            f64::MIN,
            -1000.5,
            -1.0,
            -0.0, // -0.0 and 0.0 have same numeric value but different bits
            0.0,
            1.0,
            1000.5,
            f64::MAX,
            f64::INFINITY,
        ];

        for window in values.windows(2) {
            let (a, b) = (window[0], window[1]);
            // Skip -0.0 vs 0.0 comparison (they're equal numerically)
            if a == b {
                continue;
            }
            let encoded_a = encode_f64_sortable(a);
            let encoded_b = encode_f64_sortable(b);
            assert!(
                encoded_a < encoded_b,
                "Ordering violated: {} (0x{:016x}) should be < {} (0x{:016x})",
                a,
                encoded_a,
                b,
                encoded_b
            );
        }
    }

    #[test]
    fn should_handle_f64_special_values() {
        // NaN roundtrips (though ordering of NaN is undefined)
        let nan = f64::NAN;
        let encoded = encode_f64_sortable(nan);
        let decoded = decode_f64_sortable(encoded);
        assert!(decoded.is_nan());
    }
}
