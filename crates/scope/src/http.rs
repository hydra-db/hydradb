//! The HTTP spelling of a scope: graph id from the path, namespace from the
//! `x-graph-namespace` header.

use crate::{GraphId, GraphScope, NamespacePath, ScopeResult};

/// The header carrying the namespace path, `tenant/sub/leaf`.
pub const GRAPH_NAMESPACE_HEADER: &str = "x-graph-namespace";

/// Why a request's scope could not be built.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HttpScopeError {
    /// No `x-graph-namespace` header and the default namespace is not allowed.
    #[error("{GRAPH_NAMESPACE_HEADER} header is required")]
    MissingNamespace,
    /// A header or path value that is not a valid scope.
    #[error(transparent)]
    Invalid(#[from] crate::ScopeError),
}

/// Build the scope of an HTTP query request.
///
/// `namespace_header` is the header value, or `None` when it is absent or not
/// visible ASCII. A missing header falls back to the default namespace only
/// when `allow_default_namespace` is set, which graph-node does when its own
/// scope is the default one.
pub fn http_graph_scope(
    namespace_header: Option<&str>,
    graph_id: &str,
    allow_default_namespace: bool,
) -> Result<GraphScope, HttpScopeError> {
    let namespace = match namespace_header {
        Some(namespace) => NamespacePath::parse(namespace)?,
        None if allow_default_namespace => NamespacePath::default(),
        None => return Err(HttpScopeError::MissingNamespace),
    };
    Ok(GraphScope::new(namespace, graph_id_from_path(graph_id)?))
}

fn graph_id_from_path(graph_id: &str) -> ScopeResult<GraphId> {
    GraphId::new(graph_id.to_string())
}
