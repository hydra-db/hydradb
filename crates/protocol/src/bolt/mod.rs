//! Bolt 5.1-5.4.

mod frame;
mod handshake;
mod message;
mod strict;

pub use boltr::message::{sig, ClientMessage, ServerMessage};
pub use frame::{Message, MessageReader, MessageWriter, Outgoing, DEFAULT_MAX_MESSAGE_BYTES};
pub use handshake::{client_handshake, server_handshake};
pub use message::{decode_route, decode_summary, failure, hello, logon_bearer, run_extra, success, RouteRequest, RunExtra, Summary};
pub use strict::strict_decode_client_message;

/// The four bytes every Bolt connection opens with.
pub const BOLT_MAGIC: [u8; 4] = boltr::version::BOLT_MAGIC;
/// The manifest negotiation proposal Bolt 5.7+ drivers send first.
pub const BOLT_MANIFEST_V1: [u8; 4] = [0x00, 0x00, 0x01, 0xff];
/// Versions HydraDB speaks, best first: the ones `boltr` negotiates.
pub const BOLT_SUPPORTED_VERSIONS: [(u8, u8); 4] = boltr::version::SUPPORTED_VERSIONS;
/// Deepest PackStream nesting a client message may carry.
pub const BOLT_MAX_PACKSTREAM_DEPTH: usize = 32;

/// The `server` agent HELLO's SUCCESS reports, for a build at `version`.
///
/// Neo4j drivers refuse a server whose agent does not start with `Neo4j/`;
/// the 5.x Python driver checks exactly that prefix and nothing after it. So
/// the agent leads with a Neo4j release, 5.26 (the 5.x long-term-support
/// line), and names the actual server after it, the shape other
/// Bolt-compatible servers use.
pub fn server_agent(version: &str) -> String {
    format!("Neo4j/5.26.0 compatible - SlateDBGraph/{version}")
}
