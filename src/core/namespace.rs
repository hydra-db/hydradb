//! Scope identity lives in `crates/scope` (`hydradb-scope`), so processes that
//! are not graph-nodes name a tenant with the same code. Re-exported here so
//! kernel call sites use kernel paths. `ScopeError` converts into `GraphError`
//! in `src/core/error.rs`.

pub use hydradb_scope::database::DatabaseName;
pub use hydradb_scope::{
    encode_scope_namespace_id, GraphId, GraphScope, NamespaceId, NamespacePath, DEFAULT_GRAPH_ID, DEFAULT_NAMESPACE_ID, MAX_NAMESPACE_DEPTH,
};

/// The scope a Bolt database name selects under the root scope `root`: the
/// base database names `root` itself, and a scoped name
/// (`hydradb_scope::database`) the tenant, and sub-tenant if any, under it.
/// The client resolver routes every scoped query through this, and offline
/// tools that write or read a scope's store call it, so a database name names
/// one store everywhere.
pub fn graph_scope_for_database(base_database: &DatabaseName, root: &GraphScope, database: &DatabaseName) -> crate::Result<GraphScope> {
    if database == base_database {
        return Ok(root.clone());
    }
    let scoped = hydradb_scope::database::parse_scoped_database(base_database, database)?;
    let mut namespace = root.namespace.child(scoped.tenant)?;
    if let Some(sub_tenant) = scoped.sub_tenant {
        namespace = namespace.child(sub_tenant)?;
    }
    Ok(GraphScope::new(namespace, root.graph_id.clone()))
}
