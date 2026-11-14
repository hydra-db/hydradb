//! The random install ID a usage report carries.

/// Format 16 caller-supplied random bytes as a 32-character lowercase hex ID.
pub fn install_id(bytes: [u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Whether `value` has the shape [`install_id`] produces.
pub fn is_install_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
