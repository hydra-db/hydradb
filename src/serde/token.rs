//! Order-preserving **index-token encodings** (RFC 0003, §"Numeric index
//! tokens" / Acceptance #3).
//!
//! A *token* is the value component of an [`Index`](super::RecordType::Index)
//! key. [`index_key(pred, token)`](super::keys::index_key) concatenates the
//! token after the predicate id, so the token's own bytes must sort in logical
//! value order for a range predicate (`>`, `<`, `>=`, `<=`) to become a single
//! bounded key-range scan. Each tokenizer here corresponds to a schema
//! `Tokenizer` directive (RFC 0006):
//!
//! | Tokenizer | Function        | Order-preserving? | Fidelity |
//! |-----------|-----------------|-------------------|----------|
//! | `exact`   | [`token_exact`] | yes               | full     |
//! | `int`     | [`token_int`]   | yes               | full     |
//! | `float`   | [`token_float`] | yes (NaN last)    | full     |
//! | `hash`    | [`token_hash`]  | **no** (equality) | lossy    |
//!
//! House rule: **keys are big-endian.** The numeric tokenizers reuse
//! `common::serde::sortable`'s sign-flipping encoders and then write the result
//! big-endian; the `exact` tokenizer reuses `common::serde::terminated_bytes`.

use bytes::Bytes;
use common::serde::sortable::{
    decode_f64_sortable, decode_i64_sortable, encode_f64_sortable, encode_i64_sortable,
};
use common::serde::terminated_bytes;

use crate::{Error, Result};

/// Width of a fixed-width numeric token (`int` / `float`), in bytes.
const NUMERIC_TOKEN_LEN: usize = 8;

// ---------------------------------------------------------------------------
// exact — full-fidelity, order-preserving, terminated
// ---------------------------------------------------------------------------

/// `exact` tokenizer: order-preserving, terminated encoding of an arbitrary
/// byte string.
///
/// Supports full-fidelity string/bytes equality **and** range predicates. Uses
/// [`terminated_bytes`], which escapes `0x00`/`0x01` and appends a `0x00`
/// terminator, guaranteeing that encoded `"a"` is not a prefix of encoded
/// `"ab"` — so value order survives the following key components (there are
/// none after a token, but the invariant keeps `"a" < "ab"` exact).
pub fn token_exact(value: &[u8]) -> Bytes {
    terminated_bytes::serialize_to_bytes(value)
}

/// Inverse of [`token_exact`]. Errors if the input is not a well-formed
/// terminated-bytes segment, or if any bytes remain after the terminator.
pub fn decode_exact(token: &[u8]) -> Result<Bytes> {
    let mut cur = token;
    let value = terminated_bytes::deserialize(&mut cur).map_err(Error::from)?;
    if !cur.is_empty() {
        return Err(Error::encoding(format!(
            "trailing bytes after exact token: {} extra",
            cur.len()
        )));
    }
    Ok(value)
}

// ---------------------------------------------------------------------------
// int — sortable i64, 8 bytes big-endian
// ---------------------------------------------------------------------------

/// `int` tokenizer: a sortable `i64` written as 8 big-endian bytes, so byte
/// order equals numeric order (negatives sort before positives).
pub fn token_int(v: i64) -> Bytes {
    Bytes::copy_from_slice(&encode_i64_sortable(v).to_be_bytes())
}

/// Inverse of [`token_int`]. Requires exactly 8 bytes.
pub fn decode_int(token: &[u8]) -> Result<i64> {
    let bits = u64::from_be_bytes(numeric_bytes(token, "int")?);
    Ok(decode_i64_sortable(bits))
}

// ---------------------------------------------------------------------------
// float — sortable f64, 8 bytes big-endian
// ---------------------------------------------------------------------------

/// `float` tokenizer: a sortable `f64` written as 8 big-endian bytes, so byte
/// order equals numeric order.
///
/// Policy (RFC 0003 Acceptance #3): `NaN` sorts **last** — after
/// `f64::INFINITY` — and therefore never falls inside a finite range. `-0.0`
/// and `+0.0` encode to distinct bytes (`-0.0` just below `+0.0`) but are
/// numerically equal.
pub fn token_float(v: f64) -> Bytes {
    Bytes::copy_from_slice(&encode_f64_sortable(v).to_be_bytes())
}

/// Inverse of [`token_float`]. Requires exactly 8 bytes. Round-trips `NaN`
/// (bit-preserving), though `NaN` payload bits are not otherwise meaningful.
pub fn decode_float(token: &[u8]) -> Result<f64> {
    let bits = u64::from_be_bytes(numeric_bytes(token, "float")?);
    Ok(decode_f64_sortable(bits))
}

/// Validates that `token` is exactly [`NUMERIC_TOKEN_LEN`] bytes and returns it
/// as a fixed-size array for `u64::from_be_bytes`.
fn numeric_bytes(token: &[u8], what: &str) -> Result<[u8; NUMERIC_TOKEN_LEN]> {
    token.try_into().map_err(|_| {
        Error::encoding(format!(
            "{what} token must be {NUMERIC_TOKEN_LEN} bytes, got {}",
            token.len()
        ))
    })
}

// ---------------------------------------------------------------------------
// hash — fixed 8-byte FNV-1a-64, equality only
// ---------------------------------------------------------------------------

/// FNV-1a-64 offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a-64 prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// `hash` tokenizer: a fixed 8-byte FNV-1a-64 digest of `value`, big-endian.
///
/// **Equality only — NOT order-preserving.** A hash token collapses arbitrary
/// values into a fixed-width digest, so it answers `= value` (and set
/// membership) but never a range predicate. Because it is lossy (distinct
/// values can collide, and the original value cannot be recovered), the query
/// planner sets a **re-fetch flag** for hash-tokenized predicates (RFC 0006):
/// matches from the index are candidates that the executor re-checks against
/// the materialized value.
///
/// FNV-1a is implemented inline (offset basis `0xcbf29ce484222325`, prime
/// `0x100000001b3`) rather than via a hasher crate so the digest is stable,
/// deterministic, and independent of any dependency's version or default seed.
pub fn token_hash(value: &[u8]) -> Bytes {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in value {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    Bytes::copy_from_slice(&hash.to_be_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ----- int ordering & round-trip -----

    #[test]
    fn should_order_int_tokens_by_numeric_value() {
        // given — a sweep spanning both signs and the extremes
        let values = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];

        // when — encode each
        let encoded: Vec<Bytes> = values.iter().map(|&v| token_int(v)).collect();

        // then — encoded bytes strictly increase with numeric value
        for pair in encoded.windows(2) {
            assert!(
                pair[0] < pair[1],
                "int token order violated: {:?} !< {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn should_roundtrip_int_tokens() {
        // given / when / then
        for v in [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX] {
            let token = token_int(v);
            assert_eq!(token.len(), NUMERIC_TOKEN_LEN);
            assert_eq!(decode_int(&token).unwrap(), v);
        }
    }

    proptest! {
        #[test]
        fn should_roundtrip_any_int(v: i64) {
            prop_assert_eq!(decode_int(&token_int(v)).unwrap(), v);
        }

        #[test]
        fn should_match_int_byte_order_to_numeric_order(a: i64, b: i64) {
            prop_assert_eq!(a.cmp(&b), token_int(a).cmp(&token_int(b)));
        }
    }

    // ----- float ordering & round-trip -----

    #[test]
    fn should_order_float_tokens_by_numeric_value() {
        // given — a sweep including ±∞, subnormals and ±0.0
        let values = [
            f64::NEG_INFINITY,
            f64::MIN,
            -1000.5,
            -1.0,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            f64::MAX,
            f64::INFINITY,
        ];

        // when / then — strictly increasing, skipping the -0.0/0.0 pair which
        // is numerically equal (encodes to distinct-but-adjacent bytes).
        for window in values.windows(2) {
            let (a, b) = (window[0], window[1]);
            if a == b {
                continue; // -0.0 vs 0.0
            }
            let (ea, eb) = (token_float(a), token_float(b));
            assert!(
                ea < eb,
                "float token order violated: {a} ({ea:?}) !< {b} ({eb:?})"
            );
        }
    }

    #[test]
    fn should_sort_nan_last() {
        // given — NaN policy: sorts after every finite value and after +∞
        let nan = token_float(f64::NAN);
        let inf = token_float(f64::INFINITY);

        // when / then
        assert!(
            nan > inf,
            "NaN must sort after +INFINITY: {nan:?} !> {inf:?}"
        );
    }

    #[test]
    fn should_roundtrip_float_tokens_including_nan() {
        // given — every-special sweep
        let values = [
            f64::NEG_INFINITY,
            f64::MIN,
            -1000.5,
            -1.0,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
        ];

        // when / then — bit-preserving round-trip (covers NaN and ±0.0)
        for v in values {
            let token = token_float(v);
            assert_eq!(token.len(), NUMERIC_TOKEN_LEN);
            assert_eq!(decode_float(&token).unwrap().to_bits(), v.to_bits());
        }
    }

    proptest! {
        #[test]
        fn should_roundtrip_any_float(bits: u64) {
            // build an arbitrary f64 (any bit pattern, including NaNs)
            let v = f64::from_bits(bits);
            prop_assert_eq!(decode_float(&token_float(v)).unwrap().to_bits(), v.to_bits());
        }
    }

    // ----- exact ordering & round-trip -----

    #[test]
    fn should_roundtrip_exact_with_embedded_special_bytes() {
        // given — a value carrying the terminator (0x00) and escape (0x01) bytes
        let value = b"a\x00b\x01c";
        let token = token_exact(value);

        // when / then
        assert_eq!(decode_exact(&token).unwrap().as_ref(), value);
    }

    #[test]
    fn should_order_exact_shorter_before_longer_prefix() {
        // given — "a" is a prefix of "ab"; the terminator makes it sort first
        let a = token_exact(b"a");
        let ab = token_exact(b"ab");

        // when / then
        assert!(a < ab, "exact token order violated: {a:?} !< {ab:?}");
    }

    proptest! {
        #[test]
        fn should_roundtrip_any_exact(value: Vec<u8>) {
            let decoded = decode_exact(&token_exact(&value)).unwrap();
            prop_assert_eq!(decoded.as_ref(), value.as_slice());
        }

        #[test]
        fn should_match_exact_byte_order_to_value_order(a: Vec<u8>, b: Vec<u8>) {
            prop_assert_eq!(a.cmp(&b), token_exact(&a).cmp(&token_exact(&b)));
        }
    }

    // ----- hash -----

    #[test]
    fn should_hash_deterministically_to_eight_bytes() {
        // given — the same input hashed twice, and a differing input
        let a1 = token_hash(b"hello");
        let a2 = token_hash(b"hello");
        let b = token_hash(b"world");

        // when / then — stable, fixed-width, and discriminating
        assert_eq!(a1.len(), NUMERIC_TOKEN_LEN);
        assert_eq!(a1, a2);
        assert_ne!(a1, b);
    }

    #[test]
    fn should_hash_empty_input_to_offset_basis() {
        // given — FNV-1a of the empty string is the offset basis, big-endian
        let token = token_hash(b"");

        // when / then — pins the exact algorithm/constants (version-independent)
        assert_eq!(token.as_ref(), &FNV_OFFSET_BASIS.to_be_bytes());
    }

    proptest! {
        #[test]
        fn should_hash_same_input_to_same_output(value: Vec<u8>) {
            prop_assert_eq!(token_hash(&value), token_hash(&value));
            prop_assert_eq!(token_hash(&value).len(), NUMERIC_TOKEN_LEN);
        }
    }

    // ----- decoder rejection -----

    #[test]
    fn should_reject_wrong_length_numeric_tokens() {
        // given — tokens that are not exactly 8 bytes
        for bad in [
            &b""[..],
            &b"\x00"[..],
            &b"\x00\x00\x00\x00\x00\x00\x00"[..],
            &[0u8; 9][..],
        ] {
            // when / then
            assert!(decode_int(bad).is_err(), "int accepted {} bytes", bad.len());
            assert!(
                decode_float(bad).is_err(),
                "float accepted {} bytes",
                bad.len()
            );
        }
    }

    #[test]
    fn should_reject_exact_token_with_trailing_bytes() {
        // given — a valid terminated segment with an extra byte appended
        let mut token = token_exact(b"hi").to_vec();
        token.push(0xAB);

        // when / then
        assert!(decode_exact(&token).is_err());
    }

    #[test]
    fn should_reject_unterminated_exact_token() {
        // given — no 0x00 terminator
        let token = b"hi";

        // when / then
        assert!(decode_exact(token).is_err());
    }
}
