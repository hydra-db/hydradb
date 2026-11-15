use std::time::Duration;

use boltr::error::BoltError;
use boltr::message::ServerMessage;
use hydradb_protocol::bolt::MessageWriter;
use tokio::io::AsyncWrite;

/// The shared framing writer under a write timeout: every response is flushed
/// as it is sent, and a client that stops reading cannot hold the connection
/// open past the timeout.
pub(crate) struct TimedWriter<W> {
    writer: MessageWriter<W>,
    timeout: Duration,
}

impl<W> TimedWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub(crate) fn new(writer: W, timeout: Duration) -> Self {
        Self {
            writer: MessageWriter::new(writer),
            timeout,
        }
    }

    pub(crate) async fn send(&mut self, message: &ServerMessage) -> Result<(), BoltError> {
        tokio::time::timeout(self.timeout, async {
            self.writer.send(message).await?;
            self.writer.flush().await
        })
        .await
        .map_err(|_| BoltError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "Bolt response write timed out")))?
    }
}
