//! One client connection: handshake, authentication, ROUTE, and the loop that
//! turns each RUN, or each single-statement transaction, into a unit of work.

use std::sync::atomic::{AtomicU64, Ordering};

use hydradb_protocol::bolt::{
    decode_route, failure, server_agent, server_handshake, sig, strict_decode_client_message, success, ClientMessage, Message,
    MessageReader, MessageWriter, ServerMessage,
};
use hydradb_protocol::boltr::error::BoltError;
use hydradb_protocol::boltr::types::{BoltDict, BoltValue};
use hydradb_protocol::codes;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use turbolay_proxy_core::types::Protocol;
use turbolay_proxy_core::Placement;

use crate::unit::UnitEnd;
use crate::{Advertise, BoltProxy};

/// How the session continues after a unit or a transaction.
pub(crate) enum Continue {
    Ready,
    Failed,
    Closed,
}

impl From<UnitEnd> for Continue {
    fn from(end: UnitEnd) -> Self {
        match end {
            UnitEnd::Completed(_) | UnitEnd::Reset => Self::Ready,
            UnitEnd::Failed => Self::Failed,
            UnitEnd::Closed => Self::Closed,
        }
    }
}

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Negotiation,
    Authentication,
    Ready,
    Failed,
}

/// Per connection state the unit code shares.
pub(crate) struct Session<S> {
    pub(crate) reader: MessageReader<ReadHalf<S>>,
    pub(crate) writer: MessageWriter<WriteHalf<S>>,
}

impl<P: Placement> BoltProxy<P> {
    /// Serve one client connection until it closes. Returns the error that
    /// ended it, if any; a client hanging up is not an error.
    pub async fn serve<S>(&self, stream: S) -> Result<(), BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let metrics = self.router.metrics();
        metrics.client_connected(Protocol::Bolt);
        let result = self.serve_inner(stream).await;
        metrics.client_disconnected(Protocol::Bolt);
        match result {
            Err(BoltError::Io(error)) if is_hangup(&error) => Ok(()),
            other => other,
        }
    }

    async fn serve_inner<S>(&self, mut stream: S) -> Result<(), BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let deadline = tokio::time::Instant::now() + self.config.authentication_timeout;
        tokio::time::timeout_at(deadline, server_handshake(&mut stream))
            .await
            .map_err(|_| BoltError::Authentication("handshake timed out".into()))??;
        let id = format!("bolt-proxy-{}", NEXT_SESSION.fetch_add(1, Ordering::Relaxed));
        let (reader, writer) = tokio::io::split(stream);
        let mut session = Session {
            reader: MessageReader::new(reader, self.config.max_message_bytes),
            writer: MessageWriter::new(writer),
        };
        let mut state = State::Negotiation;
        // Set while the client is not logged in, whatever the state: a
        // protocol error before LOGON must not escape the deadline.
        let mut auth_deadline = Some(deadline);
        loop {
            // The reader cannot say whether the next request is already
            // buffered, so answers go out before every read.
            session.writer.flush().await?;
            let frame = match auth_deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, session.reader.next())
                    .await
                    .map_err(|_| BoltError::Authentication("authentication timed out".into()))??,
                None => session.reader.next().await?,
            };
            if state == State::Failed && !matches!(frame.tag, sig::RESET | sig::GOODBYE) {
                session.writer.send(&ServerMessage::Ignored).await?;
                continue;
            }
            match (state, frame.tag) {
                (_, sig::GOODBYE) => {
                    session.writer.flush().await?;
                    return Ok(());
                }
                (State::Negotiation, sig::HELLO) => {
                    strict_decode_client_message(frame.body())?;
                    session.writer.send(&hello_success(&id)).await?;
                    state = State::Authentication;
                }
                (State::Authentication, sig::LOGON) => {
                    if !self.logon(&mut session, &frame).await? {
                        return Ok(());
                    }
                    state = State::Ready;
                    auth_deadline = None;
                }
                // RESET clears a failure, never the need to log in.
                (State::Ready | State::Failed, sig::RESET) => {
                    session.writer.send(&success(BoltDict::new())).await?;
                    state = if auth_deadline.is_none() {
                        State::Ready
                    } else {
                        State::Authentication
                    };
                }
                (State::Ready, sig::LOGOFF) => {
                    session.writer.send(&success(BoltDict::new())).await?;
                    state = State::Authentication;
                    auth_deadline = Some(tokio::time::Instant::now() + self.config.authentication_timeout);
                }
                (State::Ready, sig::TELEMETRY) => session.writer.send(&success(BoltDict::new())).await?,
                (State::Ready, sig::ROUTE) => {
                    self.router.metrics().route_requests.inc();
                    let request = decode_route(frame.body())?;
                    let table = BoltDict::from([("rt".to_string(), BoltValue::Dict(self.routing_table(request.db)))]);
                    session.writer.send(&success(table)).await?;
                }
                (State::Ready, sig::RUN | sig::BEGIN) => {
                    let next = if frame.tag == sig::RUN {
                        self.run_unit(&mut session, frame).await?.into()
                    } else {
                        self.transaction(&mut session, &frame).await?
                    };
                    match next {
                        Continue::Ready => state = State::Ready,
                        Continue::Failed => state = State::Failed,
                        Continue::Closed => {
                            session.writer.flush().await?;
                            return Ok(());
                        }
                    }
                }
                _ => {
                    self.router.metrics().protocol_errors.inc();
                    session.writer.send(&invalid_in_state()).await?;
                    state = State::Failed;
                }
            }
        }
    }

    /// Answer a LOGON. False when the credentials were refused, which ends
    /// the connection.
    async fn logon<S>(&self, session: &mut Session<S>, frame: &Message) -> Result<bool, BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let ClientMessage::Logon { auth } = strict_decode_client_message(frame.body())? else {
            unreachable!("tag checked")
        };
        if self.authenticate(&auth) {
            session.writer.send(&success(BoltDict::new())).await?;
            return Ok(true);
        }
        self.router.metrics().auth_failures.inc();
        session.writer.send(&failure(codes::UNAUTHORIZED, "invalid credentials")).await?;
        session.writer.flush().await?;
        Ok(false)
    }

    /// Bearer, or basic with a nonempty principal, carrying the cluster token,
    /// which is what graph-node accepts. Compared in constant time.
    fn authenticate(&self, auth: &BoltDict) -> bool {
        let credentials = match auth.get("scheme").and_then(BoltValue::as_str) {
            Some("bearer") => auth.get("credentials").and_then(BoltValue::as_str),
            Some("basic") => {
                let principal = auth.get("principal").and_then(BoltValue::as_str).unwrap_or("");
                if principal.trim().is_empty() {
                    None
                } else {
                    auth.get("credentials").and_then(BoltValue::as_str)
                }
            }
            _ => None,
        };
        credentials.is_some_and(|credentials| bool::from(credentials.as_bytes().ct_eq(self.config.token.as_bytes())))
    }

    /// The ROUTE answer: every ready proxy replica for every role, so drivers
    /// spread units across replicas, or the configured service address.
    pub fn routing_table(&self, db: Option<String>) -> BoltDict {
        let addresses = match &self.config.advertise {
            Advertise::Service(address) => vec![address.clone()],
            Advertise::Replicas => {
                let proxies = self.router.current().proxies.clone();
                if proxies.is_empty() {
                    vec![self.config.self_address.clone()]
                } else {
                    proxies
                }
            }
        };
        let servers = ["ROUTE", "READ", "WRITE"]
            .into_iter()
            .map(|role| {
                BoltValue::Dict(BoltDict::from([
                    (
                        "addresses".to_string(),
                        BoltValue::List(addresses.iter().cloned().map(BoltValue::String).collect()),
                    ),
                    ("role".to_string(), BoltValue::String(role.to_string())),
                ]))
            })
            .collect();
        BoltDict::from([
            ("ttl".to_string(), BoltValue::Integer(self.config.route_ttl_secs)),
            (
                "db".to_string(),
                BoltValue::String(db.unwrap_or_else(|| self.resolver.base_database().to_string())),
            ),
            ("servers".to_string(), BoltValue::List(servers)),
        ])
    }
}

/// HELLO's SUCCESS: the server agent graph-node reports too, and this
/// session's id.
fn hello_success(connection_id: &str) -> ServerMessage {
    success(BoltDict::from([
        ("server".to_string(), BoltValue::String(server_agent(env!("CARGO_PKG_VERSION")))),
        ("connection_id".to_string(), BoltValue::String(connection_id.to_string())),
        ("hints".to_string(), BoltValue::Dict(BoltDict::new())),
    ]))
}

pub(crate) fn invalid_in_state() -> ServerMessage {
    failure(codes::REQUEST_INVALID, "message is invalid in the current Bolt state")
}

fn is_hangup(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
    )
}
