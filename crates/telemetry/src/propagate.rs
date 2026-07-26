//! W3C Trace Context parsing and formatting. **No OpenTelemetry dependency.**
//!
//! This module exists so the kernel can carry a trace context across the query
//! transport without gaining an OTel dependency. `traceparent` is a 55-byte
//! ASCII string with a fixed layout:
//!
//! ```text
//! 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
//! ^^ ^------------------------------^ ^--------------^ ^^
//! |  trace-id (16 bytes)              span-id (8 bytes) flags
//! version
//! ```
//!
//! Parsing that needs no SDK, which is the point: `src/query/coordination.rs`
//! can treat the field as an opaque `Option<String>`, and only the binaries —
//! which do link OTel — ever turn it back into a context.
//!
//! Spec: <https://www.w3.org/TR/trace-context/>

use std::fmt;

/// Length of a well-formed `traceparent` header value.
const TRACEPARENT_LEN: usize = 55;

/// The only version this implementation emits.
const VERSION_00: u8 = 0x00;

/// The `sampled` bit of the trace-flags field.
pub const FLAG_SAMPLED: u8 = 0x01;

/// A parsed `traceparent`.
///
/// Deliberately a plain value type: `Copy`, no allocation, no interior
/// mutability, and no knowledge of any tracer. It can be built in a test by
/// hand, which is what keeps the transport plumbing testable without standing
/// up an SDK.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct TraceContext {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    flags: u8,
}

/// Why a `traceparent` was rejected.
///
/// Distinct variants rather than a bare `None` because the receiving side logs
/// these: a malformed header from a client is a different operational story
/// from an all-zero id produced by a broken instrumentation library, and
/// collapsing them hides the second.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceContextError {
    /// Not 55 bytes.
    Length {
        /// The length actually received.
        actual: usize,
    },
    /// Delimiters were not at positions 2, 35 and 52.
    Delimiters,
    /// A field contained a non-hex byte.
    NotHex {
        /// Which field: `version`, `trace-id`, `span-id` or `trace-flags`.
        field: &'static str,
    },
    /// Version `ff` is forbidden by the spec.
    ForbiddenVersion,
    /// This implementation understands version `00` only.
    UnsupportedVersion {
        /// The version byte received.
        version: u8,
    },
    /// An all-zero trace-id or span-id, which the spec declares invalid.
    ZeroId {
        /// Which id was zero: `trace-id` or `span-id`.
        field: &'static str,
    },
}

impl fmt::Display for TraceContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length { actual } => {
                write!(
                    f,
                    "traceparent must be {TRACEPARENT_LEN} bytes, got {actual}"
                )
            }
            Self::Delimiters => f.write_str("traceparent delimiters are misplaced"),
            Self::NotHex { field } => write!(f, "traceparent {field} is not hex"),
            Self::ForbiddenVersion => f.write_str("traceparent version ff is forbidden"),
            Self::UnsupportedVersion { version } => {
                write!(f, "unsupported traceparent version {version:02x}")
            }
            Self::ZeroId { field } => write!(f, "traceparent {field} is all zero"),
        }
    }
}

impl std::error::Error for TraceContextError {}

impl TraceContext {
    /// Build from raw ids. Returns `None` if either id is all zero, which the
    /// spec makes invalid.
    pub fn new(trace_id: [u8; 16], span_id: [u8; 8], sampled: bool) -> Option<Self> {
        if trace_id == [0; 16] || span_id == [0; 8] {
            return None;
        }
        Some(Self {
            trace_id,
            span_id,
            flags: if sampled { FLAG_SAMPLED } else { 0 },
        })
    }

    /// The 16-byte trace id.
    pub fn trace_id(&self) -> [u8; 16] {
        self.trace_id
    }

    /// The 8-byte span id of the parent.
    pub fn span_id(&self) -> [u8; 8] {
        self.span_id
    }

    /// Raw trace-flags byte.
    pub fn flags(&self) -> u8 {
        self.flags
    }

    /// Whether the upstream sampled this trace.
    pub fn is_sampled(&self) -> bool {
        self.flags & FLAG_SAMPLED != 0
    }

    /// Parse a `traceparent` header value.
    ///
    /// Strict by design. A lenient parser that accepts a truncated id produces
    /// a trace that silently fails to join its parent, which is worse than a
    /// rejected header because nothing reports it.
    pub fn parse(value: &str) -> Result<Self, TraceContextError> {
        let bytes = value.as_bytes();
        if bytes.len() != TRACEPARENT_LEN {
            return Err(TraceContextError::Length {
                actual: bytes.len(),
            });
        }
        if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
            return Err(TraceContextError::Delimiters);
        }

        let version =
            parse_hex_byte(&bytes[0..2]).ok_or(TraceContextError::NotHex { field: "version" })?;
        if version == 0xff {
            return Err(TraceContextError::ForbiddenVersion);
        }
        if version != VERSION_00 {
            return Err(TraceContextError::UnsupportedVersion { version });
        }

        let mut trace_id = [0u8; 16];
        decode_hex(&bytes[3..35], &mut trace_id)
            .ok_or(TraceContextError::NotHex { field: "trace-id" })?;
        if trace_id == [0; 16] {
            return Err(TraceContextError::ZeroId { field: "trace-id" });
        }

        let mut span_id = [0u8; 8];
        decode_hex(&bytes[36..52], &mut span_id)
            .ok_or(TraceContextError::NotHex { field: "span-id" })?;
        if span_id == [0; 8] {
            return Err(TraceContextError::ZeroId { field: "span-id" });
        }

        let flags = parse_hex_byte(&bytes[53..55]).ok_or(TraceContextError::NotHex {
            field: "trace-flags",
        })?;

        Ok(Self {
            trace_id,
            span_id,
            flags,
        })
    }
}

impl fmt::Display for TraceContext {
    /// Formats as a version-`00` `traceparent`, lower-case hex per the spec.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("00-")?;
        for byte in self.trace_id {
            write!(f, "{byte:02x}")?;
        }
        f.write_str("-")?;
        for byte in self.span_id {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "-{:02x}", self.flags)
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        // Upper-case is rejected: the spec requires lower-case hex, and
        // accepting both means two encodings of one id.
        _ => None,
    }
}

fn parse_hex_byte(pair: &[u8]) -> Option<u8> {
    Some(hex_value(pair[0])? << 4 | hex_value(pair[1])?)
}

fn decode_hex(source: &[u8], out: &mut [u8]) -> Option<()> {
    debug_assert_eq!(source.len(), out.len() * 2);
    for (index, slot) in out.iter_mut().enumerate() {
        *slot = parse_hex_byte(&source[index * 2..index * 2 + 2])?;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn round_trips_the_spec_example() {
        let parsed = TraceContext::parse(SAMPLE).expect("spec example must parse");
        assert!(parsed.is_sampled());
        assert_eq!(parsed.to_string(), SAMPLE);
    }

    #[test]
    fn unsampled_flag_round_trips() {
        let value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
        let parsed = TraceContext::parse(value).unwrap();
        assert!(!parsed.is_sampled());
        assert_eq!(parsed.to_string(), value);
    }

    #[test]
    fn rejects_wrong_length() {
        assert_eq!(
            TraceContext::parse("00-abc-def-01"),
            Err(TraceContextError::Length { actual: 13 })
        );
    }

    #[test]
    fn rejects_misplaced_delimiters() {
        let value = "004bf92f3577b34da6a3ce929d0e0e4736--00f067aa0ba902b7-01";
        assert_eq!(
            TraceContext::parse(value),
            Err(TraceContextError::Delimiters)
        );
    }

    #[test]
    fn rejects_forbidden_version_ff() {
        let value = "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert_eq!(
            TraceContext::parse(value),
            Err(TraceContextError::ForbiddenVersion)
        );
    }

    #[test]
    fn rejects_future_version() {
        let value = "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert_eq!(
            TraceContext::parse(value),
            Err(TraceContextError::UnsupportedVersion { version: 1 })
        );
    }

    #[test]
    fn rejects_zero_trace_id() {
        let value = "00-00000000000000000000000000000000-00f067aa0ba902b7-01";
        assert_eq!(
            TraceContext::parse(value),
            Err(TraceContextError::ZeroId { field: "trace-id" })
        );
    }

    #[test]
    fn rejects_zero_span_id() {
        let value = "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01";
        assert_eq!(
            TraceContext::parse(value),
            Err(TraceContextError::ZeroId { field: "span-id" })
        );
    }

    /// Upper-case hex is a valid id in the wrong encoding. Accepting it would
    /// mean one trace id with two spellings, so it is a parse error.
    #[test]
    fn rejects_upper_case_hex() {
        let value = "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01";
        assert_eq!(
            TraceContext::parse(value),
            Err(TraceContextError::NotHex { field: "trace-id" })
        );
    }

    #[test]
    fn rejects_non_hex_flags() {
        let value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0z";
        assert_eq!(
            TraceContext::parse(value),
            Err(TraceContextError::NotHex {
                field: "trace-flags"
            })
        );
    }

    #[test]
    fn new_rejects_zero_ids() {
        assert!(TraceContext::new([0; 16], [1; 8], true).is_none());
        assert!(TraceContext::new([1; 16], [0; 8], true).is_none());
        assert!(TraceContext::new([1; 16], [1; 8], true).is_some());
    }

    /// Unknown future flag bits must survive a round trip untouched — the spec
    /// requires forwarding them rather than masking to the bits we know.
    #[test]
    fn preserves_unknown_flag_bits() {
        let value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff";
        let parsed = TraceContext::parse(value).unwrap();
        assert_eq!(parsed.flags(), 0xff);
        assert_eq!(parsed.to_string(), value);
    }
}
