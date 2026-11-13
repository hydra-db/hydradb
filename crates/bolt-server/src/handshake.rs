use std::time::Duration;

use boltr::error::BoltError;
use hydradb_protocol::bolt::server_handshake;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::Instant;

/// Answer the client's version handshake within `timeout`, and never past
/// `authentication_deadline`. The caller starts that deadline at accept, so
/// time spent in TLS counts against it. Returns the agreed `(major, minor)`.
pub async fn handshake<S>(stream: &mut S, timeout: Duration, authentication_deadline: Instant) -> Result<(u8, u8), BoltError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    tokio::time::timeout_at(
        std::cmp::min(Instant::now() + timeout, authentication_deadline),
        server_handshake(stream),
    )
    .await
    .map_err(|_| BoltError::Protocol("Bolt handshake timed out".to_string()))?
}
