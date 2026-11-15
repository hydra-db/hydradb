use boltr::error::BoltError;
use boltr::message::ClientMessage;
use hydradb_protocol::bolt::{strict_decode_client_message, MessageReader};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Read client messages until the stream fails or `sender` closes, strictly
/// decoding each, and stop after the first error.
///
/// Reading runs in its own task because the session `select!`s on new
/// requests while it waits for a result, and a framed read is not
/// cancel-safe: dropping one mid-message would desync the stream. The
/// channel's capacity bounds how many pipelined requests can queue.
pub(crate) async fn read_messages<R>(reader: R, sender: mpsc::Sender<Result<ClientMessage, BoltError>>, max_message_bytes: usize)
where
    R: AsyncRead + Unpin,
{
    let mut reader = MessageReader::new(reader, max_message_bytes);
    loop {
        let message = match reader.next().await {
            Ok(message) => strict_decode_client_message(message.body()),
            Err(error) => Err(error),
        };
        let stop = message.is_err();
        if sender.send(message).await.is_err() || stop {
            return;
        }
    }
}

/// The reader task, aborted when the session ends however it ends.
pub(crate) struct ReaderTask(JoinHandle<()>);

impl ReaderTask {
    pub(crate) fn new(task: JoinHandle<()>) -> Self {
        Self(task)
    }

    pub(crate) async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for ReaderTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
