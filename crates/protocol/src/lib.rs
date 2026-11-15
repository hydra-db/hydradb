//! Wire protocol pieces shared by graph-node and the turbolay proxy.
//!
//! - [`bolt`] (feature `bolt`): framing, handshakes, strict message
//!   validation, the summary and RUN-extra readers the proxy routes on, and
//!   the encoders it answers with
//! - [`codes`]: the Neo4j status codes HydraDB sends
//! - [`owner`]: the one format and parse of the owner hint a refused write
//!   carries, so a node writes it and the proxy reads it with the same code
//! - [`http`]: header names and the error envelope of the HTTP query API

#![forbid(unsafe_code)]

#[cfg(feature = "bolt")]
pub mod bolt;
pub mod codes;
pub mod http;
pub mod owner;

#[cfg(feature = "bolt")]
pub use boltr;
