//! Bolt chunked framing: the one place message boundaries are read and written.
//!
//! Chunking is exactly as the Bolt specification defines it, with no format of
//! its own: a message is a run of chunks, each a big-endian `u16` length and
//! that many bytes, ended by an empty chunk (`00 00`). An empty chunk between
//! messages is a NOOP keepalive.
//!
//! Callers deal in whole messages: [`MessageReader`] yields [`Message`]s and
//! [`MessageWriter`] writes anything [`Outgoing`], a [`Message`] being
//! forwarded or a `boltr` `ServerMessage` or `ClientMessage`. Nothing outside
//! this module sees a chunk, so the framing can change without touching the
//! code that routes and relays.
//!
//! # Performance
//!
//! Both directions use `boltr`'s chunk reader and writer. They are simple and
//! correct, and cost a relay more than necessary:
//!
//! - `ChunkReader` makes two `read_exact` calls per chunk, so a result of small
//!   RECORDs costs two syscalls per row
//! - each message is copied out without its chunk headers, and forwarding it
//!   encodes the body again and re-chunks it
//! - `ChunkWriter` writes header, chunk and terminator separately, so
//!   [`MessageWriter`] buffers them until [`MessageWriter::flush`]
//! - the reader cannot say whether a whole message is already buffered, so a
//!   caller cannot see a pipelined request and must flush before every read
//!
//! A relay is bound by syscalls and messages per second, not parsing. The
//! faster shape reads large blocks, hands out messages as slices that keep
//! their chunk headers, offers a non-blocking `try_next`, and writes those
//! framed bytes unchanged. [`Message`] and [`Outgoing`] can carry that without
//! changing callers. `examples/framing_bench.rs` measures the read and forward
//! path.

use boltr::chunk::{ChunkReader, ChunkWriter};
use boltr::error::BoltError;
use boltr::message::encode::{encode_client_message, encode_server_message};
use boltr::message::{sig, ClientMessage, ServerMessage};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};

/// The largest message a reader accepts unless told otherwise.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * bytesize::MIB as usize;
/// How much a [`MessageWriter`] buffers before it writes without being asked.
const WRITE_BUFFER_BYTES: usize = 64 * bytesize::KIB as usize;

/// One whole message.
#[derive(Clone, Debug)]
pub struct Message {
    /// The message signature, the second body byte.
    pub tag: u8,
    body: Bytes,
}

impl Message {
    /// Encode `message` once, to send it many times.
    pub fn encode(message: &impl Outgoing) -> Self {
        let mut body = BytesMut::new();
        message.encode_body(&mut body);
        Self {
            tag: body.get(1).copied().unwrap_or_default(),
            body: body.freeze(),
        }
    }

    /// The PackStream body: a structure header, then its fields.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Whether this is a RECORD, the only message streamed in bulk.
    pub fn is_record(&self) -> bool {
        self.tag == sig::RECORD
    }
}

/// Anything that goes out as one Bolt message.
pub trait Outgoing {
    /// Append the message's PackStream body, without framing, to `buf`.
    fn encode_body(&self, buf: &mut BytesMut);
}

impl Outgoing for Message {
    fn encode_body(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.body);
    }
}

impl Outgoing for ServerMessage {
    fn encode_body(&self, buf: &mut BytesMut) {
        encode_server_message(buf, self);
    }
}

impl Outgoing for ClientMessage {
    fn encode_body(&self, buf: &mut BytesMut) {
        encode_client_message(buf, self);
    }
}

/// Reads whole messages from a byte stream it owns.
pub struct MessageReader<R> {
    inner: ChunkReader<R>,
}

impl<R: AsyncRead + Unpin> MessageReader<R> {
    pub fn new(reader: R, max_message: usize) -> Self {
        let mut inner = ChunkReader::new(reader);
        inner.set_max_message_size(max_message);
        Self { inner }
    }

    /// The next message, skipping NOOPs. End of stream is an `UnexpectedEof`
    /// I/O error.
    pub async fn next(&mut self) -> Result<Message, BoltError> {
        loop {
            let body = self.inner.read_message().await?;
            match body.len() {
                0 => continue,
                1 => return Err(BoltError::Protocol("Bolt message must be a PackStream structure".into())),
                _ => {
                    return Ok(Message {
                        tag: body[1],
                        body: body.freeze(),
                    })
                }
            }
        }
    }
}

/// Writes whole messages to a byte stream it owns. Messages reach the stream
/// at [`MessageWriter::flush`], or earlier once the buffer fills.
pub struct MessageWriter<W> {
    out: BufWriter<W>,
    body: BytesMut,
}

impl<W: AsyncWrite + Unpin> MessageWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            out: BufWriter::with_capacity(WRITE_BUFFER_BYTES, writer),
            body: BytesMut::new(),
        }
    }

    /// Buffer one message.
    pub async fn send(&mut self, message: &impl Outgoing) -> Result<(), BoltError> {
        self.body.clear();
        message.encode_body(&mut self.body);
        ChunkWriter::new(&mut self.out).write_message(&self.body).await
    }

    /// Write everything buffered and flush the stream.
    pub async fn flush(&mut self) -> Result<(), BoltError> {
        self.out.flush().await?;
        Ok(())
    }

    /// The stream underneath. Bytes still buffered have not reached it.
    pub fn get_ref(&self) -> &W {
        self.out.get_ref()
    }
}
