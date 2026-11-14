//! Graph scope identity for HydraDB.
//!
//! A [`GraphScope`] names one tenant's graph: a [`NamespacePath`] plus a
//! [`GraphId`]. This crate owns that type and every spelling of it that crosses
//! a process boundary:
//!
//! - [`GraphScope`]'s `Display`, `"{namespace}/graphs/{graph_id}"`, which
//!   placement hashes. It is a wire format: changing it reshuffles which node
//!   owns every cell (`crates/placement/src/hash.rs`)
//! - the Bolt database name, `{base}.scope1.{tenant}.{sub_tenant}`, in
//!   [`database`]
//! - the HTTP `x-graph-namespace` header, in [`http`]
//!
//! It exists so a process that is not a graph-node, such as the turbolay proxy,
//! turns a request into the same scope a node would with the same code, not a
//! copy that drifts. The kernel re-exports everything here.

#![forbid(unsafe_code)]

pub mod database;
mod error;
pub mod http;
mod ids;

pub use error::{ScopeError, ScopeResult};
pub use ids::{
    encode_scope_namespace_id, validate_component, GraphId, GraphScope, NamespaceId, NamespacePath, DEFAULT_GRAPH_ID, DEFAULT_NAMESPACE_ID,
    MAX_NAMESPACE_DEPTH,
};
