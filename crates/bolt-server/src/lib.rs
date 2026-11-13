//! One Bolt connection, after the listener has accepted it and after any TLS.
//!
//! - [`SessionLimits`]: message size, pipelining and the connection timeouts,
//!   with the validation both servers apply
//! - [`ConnectionGate`]: the cap on open connections
//! - [`handshake`]: the version handshake under the handshake and
//!   authentication deadlines
//! - [`run_session`]: the message loop. It owns the connection's states,
//!   deadlines, HELLO, LOGON, LOGOFF, RESET, GOODBYE, TELEMETRY and failure
//!   handling, and hands RUN, PULL, DISCARD, ROUTE and the transaction messages
//!   to a [`SessionHandler`]
//!
//! The listener, accept loop and TLS stay with each server: graph-node runs
//! tokio and rustls, the proxy pingora.

#![forbid(unsafe_code)]

mod gate;
mod handshake;
mod limits;
mod reader;
mod session;
mod writer;

pub use gate::{ConnectionGate, ConnectionPermit};
pub use handshake::handshake;
pub use limits::{SessionLimits, SessionLimitsError, MAX_PIPELINE_BUFFER_BYTES};
pub use session::{run_session, Credentials, Interrupted, Outcome, SessionHandler, SessionIo};
