use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;

use boltr::error::BoltError;
use boltr::message::{ClientMessage, ServerMessage};
use boltr::types::{BoltDict, BoltValue};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::limits::SessionLimits;
use crate::reader::{read_messages, ReaderTask};
use crate::writer::TimedWriter;

/// LOGON credentials, as the client sent them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Credentials {
    None,
    Bearer(String),
    Basic { principal: String, credentials: String },
}

impl Credentials {
    /// Read a LOGON `auth` dict. A missing `scheme` is `none`.
    pub fn from_logon(auth: &BoltDict) -> Result<Self, BoltError> {
        let scheme = auth.get("scheme").and_then(BoltValue::as_str).unwrap_or("none");
        match scheme {
            "none" => Ok(Self::None),
            "bearer" => Ok(Self::Bearer(required_string(auth, "credentials")?.to_string())),
            "basic" => Ok(Self::Basic {
                principal: required_string(auth, "principal")?.to_string(),
                credentials: required_string(auth, "credentials")?.to_string(),
            }),
            _ => Err(BoltError::Authentication(format!("unsupported authentication scheme {scheme}"))),
        }
    }
}

fn required_string<'a>(values: &'a BoltDict, name: &str) -> Result<&'a str, BoltError> {
    values
        .get(name)
        .and_then(BoltValue::as_str)
        .ok_or_else(|| BoltError::Authentication(format!("missing {name}")))
}

/// The state a [`SessionHandler`] leaves the session in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Ready,
    /// A result is open; PULL and DISCARD go to the handler.
    Streaming,
    /// A FAILURE was sent; everything but RESET and GOODBYE is IGNORED.
    Failed,
    /// The client said GOODBYE or went away while the handler waited.
    Close,
}

/// How a wait under [`SessionIo::interruptible`] ended.
#[derive(Debug)]
pub enum Interrupted<T> {
    Complete(T),
    /// The client sent RESET. It and every queued request were answered.
    Reset,
    /// The client sent GOODBYE or closed the stream.
    Goodbye,
}

/// What a backend does with one connection.
///
/// One handler per connection. The session calls [`SessionHandler::handle`]
/// with RUN, ROUTE, BEGIN, COMMIT and ROLLBACK while ready, and with PULL and
/// DISCARD while streaming; it answers every other message itself.
pub trait SessionHandler: Send {
    /// HELLO's `server`.
    fn server_agent(&self) -> String;

    /// HELLO's `connection_id`.
    fn connection_id(&self) -> String;

    fn is_authenticated(&self) -> bool;

    /// Check LOGON credentials. `false` sends an authentication FAILURE and
    /// closes the connection.
    fn logon(&mut self, credentials: Credentials) -> bool;

    /// Forget the authenticated principal and anything chosen under it.
    /// [`SessionHandler::clear_pending_result`] has already run.
    fn logoff(&mut self);

    /// Drop any unfinished result. Runs on RESET, LOGOFF, GOODBYE and when the
    /// session ends normally; not when it ends on an error.
    fn clear_pending_result(&mut self) -> impl Future<Output = ()> + Send;

    /// Answer one message through `io` and say which state follows. An `Err`
    /// ends the connection without clearing the pending result, so return one
    /// only when writing to the client failed.
    fn handle<W>(&mut self, message: ClientMessage, io: &mut SessionIo<W>) -> impl Future<Output = Result<Outcome, BoltError>> + Send
    where
        W: AsyncWrite + Unpin + Send;
}

/// The connection as a [`SessionHandler`] sees it: responses out, and a way
/// to wait on backend work while the client keeps pipelining.
pub struct SessionIo<W> {
    message_rx: mpsc::Receiver<Result<ClientMessage, BoltError>>,
    queued: VecDeque<ClientMessage>,
    writer: TimedWriter<W>,
    max_queued_messages: usize,
}

impl<W> SessionIo<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send(&mut self, message: &ServerMessage) -> Result<(), BoltError> {
        self.writer.send(message).await
    }

    pub async fn success(&mut self, metadata: BoltDict) -> Result<(), BoltError> {
        self.send(&ServerMessage::Success { metadata }).await
    }

    pub async fn failure(&mut self, error: &BoltError) -> Result<(), BoltError> {
        self.send(&ServerMessage::Failure {
            metadata: error.to_failure_metadata(),
        })
        .await
    }

    /// Await `task` while reading the client's next requests.
    ///
    /// RESET awaits `cancel`, answers IGNORED for the in-flight request and
    /// each queued one, then SUCCESS for itself. GOODBYE or end of stream
    /// awaits `cancel`. Other requests queue for after the task; one past the
    /// pipeline limit is an error and gets no reply. A reader error is
    /// returned as is. `cancel` runs at most once and only on those paths.
    pub async fn interruptible<T>(
        &mut self,
        task: impl Future<Output = T>,
        cancel: impl Future<Output = ()>,
    ) -> Result<Interrupted<T>, BoltError> {
        tokio::pin!(task);
        loop {
            tokio::select! {
                result = &mut task => {
                    return Ok(Interrupted::Complete(result));
                }
                incoming = self.message_rx.recv() => {
                    match incoming {
                        Some(Ok(ClientMessage::Reset)) => {
                            cancel.await;
                            let queued_before_reset = self.queued.len();
                            self.writer.send(&ServerMessage::Ignored).await?;
                            for _ in 0..queued_before_reset {
                                self.queued.pop_front();
                                self.writer.send(&ServerMessage::Ignored).await?;
                            }
                            self.writer.send(&ServerMessage::Success { metadata: BoltDict::new() }).await?;
                            return Ok(Interrupted::Reset);
                        }
                        Some(Ok(ClientMessage::Goodbye)) | None => {
                            cancel.await;
                            return Ok(Interrupted::Goodbye);
                        }
                        Some(Ok(message)) => {
                            if self.queued.len() >= self.max_queued_messages {
                                return Err(BoltError::ResourceExhausted(
                                    "Bolt pipelined message queue is full".to_string(),
                                ));
                            }
                            self.queued.push_back(message);
                        }
                        Some(Err(error)) => {
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum State {
    Negotiation,
    Authentication,
    Ready,
    Streaming,
    Failed,
}

/// Serve one connection whose version handshake is done, until the client
/// leaves, a deadline passes or writing fails.
///
/// `authentication_deadline` is the caller's, started at accept. The idle
/// timeout applies only once authenticated and restarts on every request; the
/// connection age counts from here. No deadline is checked while the handler
/// runs. LOGOFF restarts the authentication deadline. Ending on an error skips
/// [`SessionHandler::clear_pending_result`].
pub async fn run_session<S, H>(
    stream: S,
    peer_addr: SocketAddr,
    mut handler: H,
    limits: SessionLimits,
    mut authentication_deadline: Instant,
) -> Result<(), BoltError>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    H: SessionHandler,
{
    let (read_half, write_half) = tokio::io::split(stream);
    let (message_tx, message_rx) = mpsc::channel(limits.max_pipelined_messages);
    let reader_task = ReaderTask::new(tokio::spawn(read_messages(read_half, message_tx, limits.max_message_bytes)));
    let mut io = SessionIo {
        message_rx,
        queued: VecDeque::new(),
        writer: TimedWriter::new(write_half, limits.write_timeout),
        max_queued_messages: limits.max_pipelined_messages,
    };
    let mut state = State::Negotiation;
    let connection_deadline = Instant::now() + limits.max_connection_age;

    loop {
        let now = Instant::now();
        if now >= connection_deadline {
            break;
        }
        let authenticated = handler.is_authenticated();
        if !authenticated && now >= authentication_deadline {
            return Err(BoltError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Bolt authentication timeout elapsed",
            )));
        }
        let incoming = match io.queued.pop_front() {
            Some(message) => Some(Ok(message)),
            None => {
                tokio::select! {
                    incoming = io.message_rx.recv() => incoming,
                    _ = tokio::time::sleep(limits.idle_timeout), if authenticated => {
                        return Err(BoltError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Bolt connection idle timeout elapsed",
                        )));
                    }
                    _ = tokio::time::sleep_until(authentication_deadline), if !authenticated => {
                        return Err(BoltError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Bolt authentication timeout elapsed",
                        )));
                    }
                    _ = tokio::time::sleep_until(connection_deadline) => break,
                }
            }
        };
        let message = match incoming {
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                io.failure(&error).await?;
                state = State::Failed;
                continue;
            }
            None => break,
        };

        if state == State::Failed {
            match message {
                ClientMessage::Reset => {
                    handler.clear_pending_result().await;
                    io.success(BoltDict::new()).await?;
                    // Unauthenticated, this lands in authentication even when
                    // the failure came before HELLO, so a HELLO after it is
                    // refused.
                    state = if handler.is_authenticated() {
                        State::Ready
                    } else {
                        State::Authentication
                    };
                }
                ClientMessage::Goodbye => {
                    handler.clear_pending_result().await;
                    break;
                }
                _ => io.send(&ServerMessage::Ignored).await?,
            }
            continue;
        }

        match (state, message) {
            (State::Negotiation, ClientMessage::Hello { .. }) => {
                io.success(BoltDict::from([
                    ("server".to_string(), BoltValue::String(handler.server_agent())),
                    ("connection_id".to_string(), BoltValue::String(handler.connection_id())),
                    ("hints".to_string(), BoltValue::Dict(BoltDict::new())),
                ]))
                .await?;
                state = State::Authentication;
            }
            // A refused LOGON closes the connection rather than failing it.
            (State::Authentication, ClientMessage::Logon { auth }) => {
                let credentials = match Credentials::from_logon(&auth) {
                    Ok(credentials) => credentials,
                    Err(error) => {
                        io.failure(&error).await?;
                        break;
                    }
                };
                if handler.logon(credentials) {
                    io.success(BoltDict::new()).await?;
                    state = State::Ready;
                } else {
                    io.failure(&BoltError::Authentication("invalid credentials".to_string())).await?;
                    break;
                }
            }
            (_, ClientMessage::Goodbye) => {
                handler.clear_pending_result().await;
                break;
            }
            (State::Ready, ClientMessage::Reset) | (State::Streaming, ClientMessage::Reset) => {
                handler.clear_pending_result().await;
                io.success(BoltDict::new()).await?;
                state = State::Ready;
            }
            (State::Ready, ClientMessage::Logoff) => {
                handler.clear_pending_result().await;
                handler.logoff();
                authentication_deadline = Instant::now() + limits.authentication_timeout;
                io.success(BoltDict::new()).await?;
                state = State::Authentication;
            }
            (State::Ready, ClientMessage::Telemetry { .. }) => {
                io.success(BoltDict::new()).await?;
            }
            (
                State::Ready,
                message @ (ClientMessage::Run { .. }
                | ClientMessage::Route { .. }
                | ClientMessage::Begin { .. }
                | ClientMessage::Commit
                | ClientMessage::Rollback),
            )
            | (State::Streaming, message @ (ClientMessage::Pull { .. } | ClientMessage::Discard { .. })) => {
                state = match handler.handle(message, &mut io).await? {
                    Outcome::Ready => State::Ready,
                    Outcome::Streaming => State::Streaming,
                    Outcome::Failed => State::Failed,
                    Outcome::Close => break,
                };
            }
            _ => {
                io.failure(&BoltError::Protocol("message is invalid in the current Bolt state".to_string()))
                    .await?;
                state = State::Failed;
            }
        }
    }

    handler.clear_pending_result().await;
    reader_task.stop().await;
    tracing::debug!(target: "hydradb", %peer_addr, "Bolt session closed");
    Ok(())
}
