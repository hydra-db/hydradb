//! Strict client message decoding: field counts checked against the
//! signature, PackStream structure walked before decoding, nesting bounded.
//!
//! The walker is written here because boltr has no strict mode.
//! `decode_client_message` checks only "at least N fields", ignores trailing
//! bytes, and decodes values recursively with no depth limit, so a deeply
//! nested value from a client can overflow the stack. Every client body goes
//! through [`strict_decode_client_message`] before boltr decodes it.

use boltr::error::BoltError;
use boltr::message::decode::decode_client_message;
use boltr::message::{sig, ClientMessage};
use boltr::packstream::marker;
use bytes::Buf;

use super::BOLT_MAX_PACKSTREAM_DEPTH;

/// Decode one client message body, refusing anything malformed rather than
/// letting the codec guess.
pub fn strict_decode_client_message(data: &[u8]) -> Result<ClientMessage, BoltError> {
    if data.len() < 2 || data[0] & 0xf0 != marker::TINY_STRUCT_NIBBLE {
        return Err(BoltError::Protocol("Bolt message must be a PackStream structure".to_string()));
    }
    let field_count = usize::from(data[0] & 0x0f);
    let expected = expected_bolt_fields(data[1])?;
    if field_count != expected {
        return Err(BoltError::Protocol(format!(
            "Bolt message 0x{:02x} has {field_count} fields; expected {expected}",
            data[1]
        )));
    }
    let mut input = &data[2..];
    skip_packstream_values(&mut input, field_count)?;
    if input.has_remaining() {
        return Err(BoltError::Protocol("Bolt message contains trailing PackStream data".to_string()));
    }
    decode_client_message(data)
}

fn expected_bolt_fields(tag: u8) -> Result<usize, BoltError> {
    match tag {
        sig::HELLO | sig::LOGON | sig::PULL | sig::DISCARD | sig::BEGIN | sig::TELEMETRY => Ok(1),
        sig::RUN | sig::ROUTE => Ok(3),
        sig::LOGOFF | sig::GOODBYE | sig::RESET | sig::COMMIT | sig::ROLLBACK => Ok(0),
        _ => Err(BoltError::Protocol(format!("unknown Bolt message signature 0x{tag:02x}"))),
    }
}

/// Advance `input` past `count` PackStream values without decoding them,
/// checking structure and nesting on the way.
fn skip_packstream_values(input: &mut &[u8], count: usize) -> Result<(), BoltError> {
    let mut containers = vec![count];
    while let Some(remaining) = containers.last_mut() {
        if *remaining == 0 {
            containers.pop();
            continue;
        }
        *remaining -= 1;
        if !input.has_remaining() {
            return Err(BoltError::Protocol("PackStream value ended unexpectedly".to_string()));
        }
        let marker_byte = input.get_u8();
        let high = marker_byte & 0xf0;
        let low = usize::from(marker_byte & 0x0f);
        match marker_byte {
            marker::NULL | marker::FALSE | marker::TRUE => {}
            marker::FLOAT_64 | marker::INT_64 => skip_bytes(input, 8)?,
            marker::INT_8 => skip_bytes(input, 1)?,
            marker::INT_16 => skip_bytes(input, 2)?,
            marker::INT_32 => skip_bytes(input, 4)?,
            marker::BYTES_8 | marker::STRING_8 => {
                let len = read_length(input, 1)?;
                skip_bytes(input, len)?;
            }
            marker::BYTES_16 | marker::STRING_16 => {
                let len = read_length(input, 2)?;
                skip_bytes(input, len)?;
            }
            marker::BYTES_32 | marker::STRING_32 => {
                let len = read_length(input, 4)?;
                skip_bytes(input, len)?;
            }
            marker::LIST_8 => {
                let len = read_length(input, 1)?;
                push_container(&mut containers, len)?;
            }
            marker::LIST_16 => {
                let len = read_length(input, 2)?;
                push_container(&mut containers, len)?;
            }
            marker::LIST_32 => {
                let len = read_length(input, 4)?;
                push_container(&mut containers, len)?;
            }
            marker::DICT_8 => {
                let len = read_length(input, 1)?;
                push_container(&mut containers, dict_values(len)?)?;
            }
            marker::DICT_16 => {
                let len = read_length(input, 2)?;
                push_container(&mut containers, dict_values(len)?)?;
            }
            marker::DICT_32 => {
                let len = read_length(input, 4)?;
                push_container(&mut containers, dict_values(len)?)?;
            }
            _ if marker_byte <= 0x7f || marker_byte >= 0xf0 => {}
            _ if high == marker::TINY_STRING_NIBBLE => skip_bytes(input, low)?,
            _ if high == marker::TINY_LIST_NIBBLE => push_container(&mut containers, low)?,
            _ if high == marker::TINY_DICT_NIBBLE => push_container(&mut containers, dict_values(low)?)?,
            _ if high == marker::TINY_STRUCT_NIBBLE => {
                skip_bytes(input, 1)?;
                push_container(&mut containers, low)?;
            }
            _ => return Err(BoltError::Protocol(format!("unknown PackStream marker 0x{marker_byte:02x}"))),
        }
    }
    Ok(())
}

fn read_length(input: &mut &[u8], width: usize) -> Result<usize, BoltError> {
    if input.remaining() < width {
        return Err(BoltError::Protocol("PackStream length ended unexpectedly".to_string()));
    }
    match width {
        1 => Ok(usize::from(input.get_u8())),
        2 => Ok(usize::from(input.get_u16())),
        4 => usize::try_from(input.get_u32()).map_err(|_| BoltError::Protocol("PackStream length exceeds platform limits".to_string())),
        _ => unreachable!("PackStream length widths are fixed"),
    }
}

fn skip_bytes(input: &mut &[u8], len: usize) -> Result<(), BoltError> {
    if input.remaining() < len {
        return Err(BoltError::Protocol("PackStream payload ended unexpectedly".to_string()));
    }
    input.advance(len);
    Ok(())
}

fn dict_values(entries: usize) -> Result<usize, BoltError> {
    entries
        .checked_mul(2)
        .ok_or_else(|| BoltError::Protocol("PackStream dictionary is too large".to_string()))
}

fn push_container(containers: &mut Vec<usize>, values: usize) -> Result<(), BoltError> {
    if containers.len() >= BOLT_MAX_PACKSTREAM_DEPTH {
        return Err(BoltError::Protocol(format!(
            "PackStream nesting exceeds depth limit {BOLT_MAX_PACKSTREAM_DEPTH}"
        )));
    }
    containers.push(values);
    Ok(())
}
