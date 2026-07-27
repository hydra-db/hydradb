use boltr::chunk::ChunkReader;
use boltr::error::BoltError;
use boltr::message::decode::decode_client_message;
use boltr::message::{sig, ClientMessage};
use boltr::packstream::marker;
use bytes::Buf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use super::{
    BOLT_MAGIC, BOLT_MANIFEST_V1, BOLT_MAX_PACKSTREAM_DEPTH, BOLT_NO_VERSION,
    BOLT_SUPPORTED_VERSIONS,
};

pub(super) async fn bolt_server_handshake<S>(
    stream: &mut S,
) -> std::result::Result<(u8, u8), BoltError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut magic = [0_u8; 4];
    stream.read_exact(&mut magic).await?;
    if magic != BOLT_MAGIC {
        return Err(BoltError::Protocol(
            "invalid Bolt magic preamble".to_string(),
        ));
    }
    let mut proposals = [0_u8; 16];
    stream.read_exact(&mut proposals).await?;
    for proposal in proposals.chunks_exact(4) {
        if proposal == BOLT_MANIFEST_V1 {
            stream.write_all(&BOLT_MANIFEST_V1).await?;
            write_varint(stream, 1).await?;
            stream.write_all(&[0, 3, 4, 5]).await?;
            write_varint(stream, 0).await?;
            stream.flush().await?;
            let mut selected = [0_u8; 4];
            stream.read_exact(&mut selected).await?;
            let capabilities = read_varint(stream).await?;
            let version = exact_bolt_version(selected).filter(|_| capabilities == 0);
            return version.ok_or_else(|| {
                BoltError::Protocol(
                    "client selected an unsupported manifest version or capability".to_string(),
                )
            });
        }
        if let Some(version) = matching_bolt_version(proposal) {
            stream.write_all(&[0, 0, version.1, version.0]).await?;
            stream.flush().await?;
            return Ok(version);
        }
    }
    stream.write_all(&BOLT_NO_VERSION).await?;
    stream.flush().await?;
    Err(BoltError::Protocol(
        "no compatible Bolt protocol version".to_string(),
    ))
}

fn matching_bolt_version(proposal: &[u8]) -> Option<(u8, u8)> {
    if proposal.len() != 4 {
        return None;
    }
    let range = proposal[1];
    let minor = proposal[2];
    let major = proposal[3];
    BOLT_SUPPORTED_VERSIONS.iter().copied().find(|candidate| {
        candidate.0 == major && candidate.1 <= minor && candidate.1 >= minor.saturating_sub(range)
    })
}

fn exact_bolt_version(selected: [u8; 4]) -> Option<(u8, u8)> {
    if selected[0] != 0 || selected[1] != 0 {
        return None;
    }
    let candidate = (selected[3], selected[2]);
    BOLT_SUPPORTED_VERSIONS
        .contains(&candidate)
        .then_some(candidate)
}

async fn write_varint<S>(stream: &mut S, mut value: u64) -> std::result::Result<(), BoltError>
where
    S: AsyncWrite + Unpin + ?Sized,
{
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        stream.write_all(&[byte]).await?;
        if value == 0 {
            return Ok(());
        }
    }
}

async fn read_varint<S>(stream: &mut S) -> std::result::Result<u64, BoltError>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut value = 0_u64;
    for index in 0..10 {
        let byte = stream.read_u8().await?;
        if index == 9 && byte & 0x7e != 0 {
            return Err(BoltError::Protocol(
                "manifest VarInt is too large".to_string(),
            ));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(BoltError::Protocol(
        "manifest VarInt is too large".to_string(),
    ))
}

pub(super) async fn read_bolt_messages<R>(
    reader: R,
    sender: mpsc::Sender<std::result::Result<ClientMessage, BoltError>>,
    max_message_bytes: usize,
) where
    R: AsyncRead + Unpin,
{
    let mut reader = ChunkReader::new(reader);
    reader.set_max_message_size(max_message_bytes);
    loop {
        let message = match reader.read_message().await {
            Ok(message) if message.is_empty() => continue,
            Ok(message) => strict_decode_client_message(&message),
            Err(error) => Err(error),
        };
        let stop = message.is_err();
        if sender.send(message).await.is_err() || stop {
            return;
        }
    }
}

pub(super) fn strict_decode_client_message(
    data: &[u8],
) -> std::result::Result<ClientMessage, BoltError> {
    if data.len() < 2 || data[0] & 0xf0 != marker::TINY_STRUCT_NIBBLE {
        return Err(BoltError::Protocol(
            "Bolt message must be a PackStream structure".to_string(),
        ));
    }
    let field_count = usize::from(data[0] & 0x0f);
    let expected = expected_bolt_fields(data[1])?;
    if field_count != expected {
        return Err(BoltError::Protocol(format!(
            "Bolt message 0x{:02x} has {field_count} fields; expected {expected}",
            data[1]
        )));
    }
    validate_packstream_values(&data[2..], field_count)?;
    decode_client_message(data)
}

fn expected_bolt_fields(tag: u8) -> std::result::Result<usize, BoltError> {
    match tag {
        sig::HELLO | sig::LOGON | sig::PULL | sig::DISCARD | sig::BEGIN | sig::TELEMETRY => Ok(1),
        sig::RUN | sig::ROUTE => Ok(3),
        sig::LOGOFF | sig::GOODBYE | sig::RESET | sig::COMMIT | sig::ROLLBACK => Ok(0),
        _ => Err(BoltError::Protocol(format!(
            "unknown Bolt message signature 0x{tag:02x}"
        ))),
    }
}

fn validate_packstream_values(
    values: &[u8],
    root_values: usize,
) -> std::result::Result<(), BoltError> {
    let mut input = values;
    let mut containers = vec![root_values];
    while let Some(remaining) = containers.last_mut() {
        if *remaining == 0 {
            containers.pop();
            continue;
        }
        *remaining -= 1;
        if !input.has_remaining() {
            return Err(BoltError::Protocol(
                "PackStream value ended unexpectedly".to_string(),
            ));
        }
        let marker_byte = input.get_u8();
        let high = marker_byte & 0xf0;
        let low = usize::from(marker_byte & 0x0f);
        match marker_byte {
            marker::NULL | marker::FALSE | marker::TRUE => {}
            marker::FLOAT_64 | marker::INT_64 => skip_packstream_bytes(&mut input, 8)?,
            marker::INT_8 => skip_packstream_bytes(&mut input, 1)?,
            marker::INT_16 => skip_packstream_bytes(&mut input, 2)?,
            marker::INT_32 => skip_packstream_bytes(&mut input, 4)?,
            marker::BYTES_8 | marker::STRING_8 => {
                let len = read_packstream_length(&mut input, 1)?;
                skip_packstream_bytes(&mut input, len)?;
            }
            marker::BYTES_16 | marker::STRING_16 => {
                let len = read_packstream_length(&mut input, 2)?;
                skip_packstream_bytes(&mut input, len)?;
            }
            marker::BYTES_32 | marker::STRING_32 => {
                let len = read_packstream_length(&mut input, 4)?;
                skip_packstream_bytes(&mut input, len)?;
            }
            marker::LIST_8 => {
                let len = read_packstream_length(&mut input, 1)?;
                push_packstream_container(&mut containers, len)?;
            }
            marker::LIST_16 => {
                let len = read_packstream_length(&mut input, 2)?;
                push_packstream_container(&mut containers, len)?;
            }
            marker::LIST_32 => {
                let len = read_packstream_length(&mut input, 4)?;
                push_packstream_container(&mut containers, len)?;
            }
            marker::DICT_8 => {
                let len = read_packstream_length(&mut input, 1)?;
                push_packstream_container(&mut containers, checked_dict_values(len)?)?;
            }
            marker::DICT_16 => {
                let len = read_packstream_length(&mut input, 2)?;
                push_packstream_container(&mut containers, checked_dict_values(len)?)?;
            }
            marker::DICT_32 => {
                let len = read_packstream_length(&mut input, 4)?;
                push_packstream_container(&mut containers, checked_dict_values(len)?)?;
            }
            _ if marker_byte <= 0x7f || marker_byte >= 0xf0 => {}
            _ if high == marker::TINY_STRING_NIBBLE => {
                skip_packstream_bytes(&mut input, low)?;
            }
            _ if high == marker::TINY_LIST_NIBBLE => {
                push_packstream_container(&mut containers, low)?;
            }
            _ if high == marker::TINY_DICT_NIBBLE => {
                push_packstream_container(&mut containers, checked_dict_values(low)?)?;
            }
            _ if high == marker::TINY_STRUCT_NIBBLE => {
                skip_packstream_bytes(&mut input, 1)?;
                push_packstream_container(&mut containers, low)?;
            }
            _ => {
                return Err(BoltError::Protocol(format!(
                    "unknown PackStream marker 0x{marker_byte:02x}"
                )));
            }
        }
    }
    if input.has_remaining() {
        return Err(BoltError::Protocol(
            "Bolt message contains trailing PackStream data".to_string(),
        ));
    }
    Ok(())
}

fn read_packstream_length(
    input: &mut &[u8],
    width: usize,
) -> std::result::Result<usize, BoltError> {
    if input.remaining() < width {
        return Err(BoltError::Protocol(
            "PackStream length ended unexpectedly".to_string(),
        ));
    }
    match width {
        1 => Ok(usize::from(input.get_u8())),
        2 => Ok(usize::from(input.get_u16())),
        4 => usize::try_from(input.get_u32()).map_err(|_| {
            BoltError::Protocol("PackStream length exceeds platform limits".to_string())
        }),
        _ => unreachable!("PackStream length widths are fixed"),
    }
}

fn skip_packstream_bytes(input: &mut &[u8], len: usize) -> std::result::Result<(), BoltError> {
    if input.remaining() < len {
        return Err(BoltError::Protocol(
            "PackStream payload ended unexpectedly".to_string(),
        ));
    }
    input.advance(len);
    Ok(())
}

fn checked_dict_values(entries: usize) -> std::result::Result<usize, BoltError> {
    entries
        .checked_mul(2)
        .ok_or_else(|| BoltError::Protocol("PackStream dictionary is too large".to_string()))
}

fn push_packstream_container(
    containers: &mut Vec<usize>,
    values: usize,
) -> std::result::Result<(), BoltError> {
    if containers.len() >= BOLT_MAX_PACKSTREAM_DEPTH {
        return Err(BoltError::Protocol(format!(
            "PackStream nesting exceeds depth limit {BOLT_MAX_PACKSTREAM_DEPTH}"
        )));
    }
    containers.push(values);
    Ok(())
}
