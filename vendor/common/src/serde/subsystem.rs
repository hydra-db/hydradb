//! Registry of subsystem byte assignments for the RFC 0001 record key prefix.
//!
//! Every key stored by an OpenData subsystem begins with the subsystem byte
//! defined here, followed by the version byte (see [`super::key_prefix`]).
//! Centralizing the assignments in one place keeps the on-disk byte space
//! collision-free as new subsystems are added.

/// Reserved sentinel — never a valid subsystem byte.
pub const RESERVED: u8 = 0x00;

/// Timeseries database (metrics, samples).
pub const TIMESERIES: u8 = 0x01;

/// Vector database (embeddings, ANN indexes).
pub const VECTOR: u8 = 0x02;

/// Log database (Kafka-style log with sequence ordering).
pub const LOG: u8 = 0x03;

/// KeyValue database.
pub const KEYVALUE: u8 = 0x04;

/// Returns the canonical name for a known subsystem byte, or `None`.
///
/// Intended for diagnostics, logging, and tooling that inspects raw keys.
pub fn name(byte: u8) -> Option<&'static str> {
    match byte {
        TIMESERIES => Some("timeseries"),
        VECTOR => Some("vector"),
        LOG => Some("log"),
        KEYVALUE => Some("keyvalue"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_resolve_known_subsystems_by_byte() {
        assert_eq!(name(TIMESERIES), Some("timeseries"));
        assert_eq!(name(VECTOR), Some("vector"));
        assert_eq!(name(LOG), Some("log"));
        assert_eq!(name(KEYVALUE), Some("keyvalue"));
    }

    #[test]
    fn should_return_none_for_reserved_and_unassigned_bytes() {
        assert_eq!(name(RESERVED), None);
        assert_eq!(name(0x42), None);
        assert_eq!(name(0xFF), None);
    }
}
