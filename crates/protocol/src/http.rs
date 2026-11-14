//! The HTTP query API's header names and error envelope.

use serde::{Deserialize, Serialize};

/// The namespace path of the target scope, `tenant/sub/leaf`.
pub const GRAPH_NAMESPACE_HEADER: &str = "x-graph-namespace";
/// `read` or `write`. Missing means write, so an old client lands on the owner.
/// The proxy routes on it; a node may ignore it.
pub const GRAPH_ACCESS_HEADER: &str = "x-graph-access";
/// The owning node id on a 421, the same value as the body's `owner`, so the
/// proxy can retry from the response head without reading the body.
pub const GRAPH_OWNER_HEADER: &str = "x-graph-owner";

/// The access mode an `x-graph-access` value names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpAccess {
    Read,
    Write,
}

/// Parse `x-graph-access`. Missing or unrecognised is a write: a write sent to
/// the owner is always correct, a write sent anywhere else is refused.
pub fn parse_access(value: Option<&str>) -> HttpAccess {
    match value.map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("read") => HttpAccess::Read,
        _ => HttpAccess::Write,
    }
}

/// `{"error": {...}}`, the body of every non-2xx answer.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HttpErrorEnvelope {
    pub error: HttpErrorBody,
}

/// The error itself.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HttpErrorBody {
    pub code: String,
    pub message: String,
    /// The node that owns the cell, on a 421. Absent, not null, when the node
    /// had nothing truthful to name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}
