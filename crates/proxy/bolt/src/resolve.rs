use hydradb_scope::database::{parse_scoped_database, DatabaseName};
use hydradb_scope::{GraphScope, ScopeError};
use turbolay_proxy_types::RouteKey;

/// Turns a Bolt database name into a route key exactly as graph-node's
/// `HierarchicalClientDatabaseResolver` does, through the same
/// `hydradb_scope::database` functions.
#[derive(Clone, Debug)]
pub struct DatabaseResolver {
    base_database: DatabaseName,
    root_scope: GraphScope,
    cell_id: String,
}

impl DatabaseResolver {
    pub fn new(base_database: DatabaseName, root_scope: GraphScope, cell_id: String) -> Self {
        Self {
            base_database,
            root_scope,
            cell_id,
        }
    }

    pub fn base_database(&self) -> &DatabaseName {
        &self.base_database
    }

    /// The route key for a client's `database`; `None` is the base database.
    pub fn resolve(&self, database: Option<&str>) -> Result<RouteKey, ScopeError> {
        let Some(database) = database else {
            return RouteKey::new(&self.root_scope, &self.cell_id);
        };
        let database = DatabaseName::parse(database)?;
        if database == self.base_database {
            return RouteKey::new(&self.root_scope, &self.cell_id);
        }
        let scoped = parse_scoped_database(&self.base_database, &database)?;
        let mut namespace = self.root_scope.namespace.child(scoped.tenant)?;
        if let Some(sub_tenant) = scoped.sub_tenant {
            namespace = namespace.child(sub_tenant)?;
        }
        RouteKey::new(&GraphScope::new(namespace, self.root_scope.graph_id.clone()), &self.cell_id)
    }
}
