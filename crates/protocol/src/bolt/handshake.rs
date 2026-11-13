//! The Bolt version handshake, both ends.
//!
//! Plain version proposals are matched by `boltr`. The manifest proposal
//! (Bolt 5.7+) is answered here, because boltr has no manifest negotiation:
//! without it a driver that offers only the manifest, or whose plain ranges
//! miss 5.1 to 5.4, would be refused. Proposals are honoured in the order the
//! client sent them, so a manifest wins only when no plain proposal before it
//! matches.

use boltr::error::BoltError;
use boltr::server::handshake::default_client_proposals;
use boltr::version::{encode_version, negotiate_version, NO_VERSION};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{BOLT_MAGIC, BOLT_MANIFEST_V1, BOLT_SUPPORTED_VERSIONS};

/// Answer a client's handshake. Returns the agreed `(major, minor)`.
pub async fn server_handshake<S>(stream: &mut S) -> Result<(u8, u8), BoltError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut magic = [0_u8; 4];
    stream.read_exact(&mut magic).await?;
    if magic != BOLT_MAGIC {
        return Err(BoltError::Protocol("invalid Bolt magic preamble".to_string()));
    }
    let mut proposals = [0_u8; 16];
    stream.read_exact(&mut proposals).await?;
    match choose(&proposals) {
        Choice::Plain((major, minor)) => {
            stream.write_all(&encode_version(major, minor)).await?;
            stream.flush().await?;
            Ok((major, minor))
        }
        Choice::Manifest => manifest_handshake(stream).await,
        Choice::None => {
            stream.write_all(&NO_VERSION).await?;
            stream.flush().await?;
            Err(BoltError::Protocol("no compatible Bolt protocol version".to_string()))
        }
    }
}

enum Choice {
    Plain((u8, u8)),
    Manifest,
    None,
}

/// The first proposal we can serve, in the client's order.
fn choose(proposals: &[u8; 16]) -> Choice {
    let manifest = proposals
        .as_chunks::<4>()
        .0
        .iter()
        .position(|proposal| *proposal == BOLT_MANIFEST_V1);
    let mut before_manifest = [0_u8; 16];
    let plain = match manifest {
        Some(index) => {
            before_manifest[..index * 4].copy_from_slice(&proposals[..index * 4]);
            &before_manifest
        }
        None => proposals,
    };
    match (negotiate_version(plain), manifest) {
        (Some(version), _) => Choice::Plain(version),
        (None, Some(_)) => Choice::Manifest,
        (None, None) => Choice::None,
    }
}

/// Offer 5.4 back to 5.1 with no capabilities, then read the client's pick.
async fn manifest_handshake<S>(stream: &mut S) -> Result<(u8, u8), BoltError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    stream.write_all(&BOLT_MANIFEST_V1).await?;
    write_varint(stream, 1).await?;
    stream.write_all(&[0, 3, 4, 5]).await?;
    write_varint(stream, 0).await?;
    stream.flush().await?;
    let mut selected = [0_u8; 4];
    stream.read_exact(&mut selected).await?;
    let capabilities = read_varint(stream).await?;
    let version = (selected[..2] == [0, 0])
        .then_some((selected[3], selected[2]))
        .filter(|version| BOLT_SUPPORTED_VERSIONS.contains(version) && capabilities == 0);
    version.ok_or_else(|| BoltError::Protocol("client selected an unsupported manifest version or capability".to_string()))
}

async fn write_varint<S>(stream: &mut S, mut value: u64) -> Result<(), BoltError>
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

async fn read_varint<S>(stream: &mut S) -> Result<u64, BoltError>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut value = 0_u64;
    for index in 0..10 {
        let byte = stream.read_u8().await?;
        if index == 9 && byte & 0x7e != 0 {
            return Err(BoltError::Protocol("manifest VarInt is too large".to_string()));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(BoltError::Protocol("manifest VarInt is too large".to_string()))
}

/// Open a connection as a client, proposing 5.4 down to 5.1 (`boltr`'s
/// default proposal). Returns the version the server picked, refusing one
/// outside the versions HydraDB speaks.
pub async fn client_handshake<S>(stream: &mut S) -> Result<(u8, u8), BoltError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let selected = boltr::server::handshake::client_handshake(stream, &default_client_proposals()).await?;
    if BOLT_SUPPORTED_VERSIONS.contains(&selected) {
        Ok(selected)
    } else {
        Err(BoltError::Protocol(format!(
            "server selected unsupported Bolt version {selected:?}"
        )))
    }
}
