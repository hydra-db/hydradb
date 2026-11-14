//! The few messages the proxy reads or builds itself: the RUN fields it routes
//! on, summaries, and the replies and requests it sends. Everything else it
//! forwards whole, without decoding.

use boltr::error::BoltError;
use boltr::message::decode::decode_server_message;
use boltr::message::{ClientMessage, ServerMessage};
use boltr::types::{BoltDict, BoltValue};

use super::strict::strict_decode_client_message;

/// What a RUN's `extra` says about routing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunExtra {
    /// The `db` the unit targets; `None` means the server's default database.
    pub db: Option<String>,
    /// `mode: "r"`. Anything else, including absent, is a write: a write sent to
    /// the owner is always correct.
    pub read: bool,
}

/// Read the routing fields of a RUN's `extra`. The body comes from a client,
/// so it is strictly decoded.
pub fn run_extra(body: &[u8]) -> Result<RunExtra, BoltError> {
    let ClientMessage::Run { extra, .. } = strict_decode_client_message(body)? else {
        return Err(BoltError::Protocol("not a RUN message".to_string()));
    };
    let db = match extra.get("db") {
        Some(BoltValue::String(db)) if !db.is_empty() => Some(db.clone()),
        Some(BoltValue::String(_) | BoltValue::Null) | None => None,
        Some(_) => return Err(BoltError::Protocol("RUN extra db must be a string".to_string())),
    };
    let read = matches!(extra.get("mode"), Some(BoltValue::String(mode)) if mode == "r");
    Ok(RunExtra { db, read })
}

/// A ROUTE request's target.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteRequest {
    pub db: Option<String>,
    pub bookmarks: Vec<String>,
}

/// Decode a ROUTE message.
pub fn decode_route(body: &[u8]) -> Result<RouteRequest, BoltError> {
    match strict_decode_client_message(body)? {
        ClientMessage::Route { bookmarks, extra, .. } => Ok(RouteRequest {
            db: extra
                .get("db")
                .and_then(BoltValue::as_str)
                .filter(|db| !db.is_empty())
                .map(str::to_string),
            bookmarks,
        }),
        other => Err(BoltError::Protocol(format!("expected ROUTE, got {other}"))),
    }
}

/// The fields read off a server summary: SUCCESS, FAILURE or IGNORED.
pub trait Summary {
    /// `has_more` on a PULL or DISCARD SUCCESS: more records are waiting.
    fn has_more(&self) -> bool;
    /// The FAILURE code.
    fn code(&self) -> Option<&str>;
    /// The FAILURE message.
    fn message(&self) -> Option<&str>;
}

impl Summary for ServerMessage {
    fn has_more(&self) -> bool {
        matches!(self, ServerMessage::Success { metadata } if matches!(metadata.get("has_more"), Some(BoltValue::Boolean(true))))
    }

    fn code(&self) -> Option<&str> {
        match self {
            ServerMessage::Failure { metadata } => metadata.get("code").and_then(BoltValue::as_str),
            _ => None,
        }
    }

    fn message(&self) -> Option<&str> {
        match self {
            ServerMessage::Failure { metadata } => metadata.get("message").and_then(BoltValue::as_str),
            _ => None,
        }
    }
}

/// Decode a summary message body. A RECORD is an error here: callers relay
/// records without decoding them.
pub fn decode_summary(body: &[u8]) -> Result<ServerMessage, BoltError> {
    match decode_server_message(body)? {
        ServerMessage::Record { .. } => Err(BoltError::Protocol("expected a summary, got RECORD".to_string())),
        summary => Ok(summary),
    }
}

/// A SUCCESS carrying `metadata`.
pub fn success(metadata: BoltDict) -> ServerMessage {
    ServerMessage::Success { metadata }
}

/// A FAILURE with a status code and a message, built the way graph-node
/// builds its own.
pub fn failure(code: &str, message: &str) -> ServerMessage {
    let error = BoltError::Query {
        code: code.to_string(),
        message: message.to_string(),
    };
    ServerMessage::Failure {
        metadata: error.to_failure_metadata(),
    }
}

/// A client HELLO. `bolt_agent` is sent from 5.3, where it became part of
/// HELLO.
pub fn hello(user_agent: &str, version: (u8, u8)) -> ClientMessage {
    let mut extra = BoltDict::from([("user_agent".to_string(), BoltValue::String(user_agent.to_string()))]);
    if version >= (5, 3) {
        extra.insert(
            "bolt_agent".to_string(),
            BoltValue::Dict(BoltDict::from([("product".to_string(), BoltValue::String(user_agent.to_string()))])),
        );
    }
    ClientMessage::Hello { extra }
}

/// A LOGON with a bearer token.
pub fn logon_bearer(token: &str) -> ClientMessage {
    ClientMessage::Logon {
        auth: BoltDict::from([
            ("scheme".to_string(), BoltValue::String("bearer".to_string())),
            ("credentials".to_string(), BoltValue::String(token.to_string())),
        ]),
    }
}
