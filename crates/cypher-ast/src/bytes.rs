//! The canonical hexadecimal spelling of a byte value, without delimiters.

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}
