//! The turbolay proxy's Bolt adapter.
//!
//! A client session is terminated here: handshake, HELLO and LOGON are
//! answered by the proxy, ROUTE is answered from the route snapshot, and every
//! RUN becomes a unit of work that is routed by the core, admitted, and run on
//! a pooled backend connection that is logged in once with the cluster token.
//! An explicit transaction is served only when it holds one statement, which
//! runs as an auto-commit unit (see `transaction`).
//!
//! What the adapter decides on its own is only protocol mapping. Where a unit
//! goes, whether it may run now, and whether a failed attempt is retried are
//! all core calls, shared with the HTTP adapter.

#![forbid(unsafe_code)]

mod pool;
mod resolve;
mod session;
mod transaction;
mod unit;

use std::sync::Arc;
use std::time::Duration;

use hydradb_scope::database::DatabaseName;
use hydradb_scope::GraphScope;
use turbolay_proxy_core::{Placement, RendezvousPlacement, Router};

pub use pool::{BackendPools, PoolConfig};
pub use resolve::DatabaseResolver;

/// What the ROUTE answer advertises.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Advertise {
    /// Every ready proxy replica, so drivers spread units across replicas on
    /// their own and a new replica gets work from the next refresh.
    Replicas,
    /// Only this address, for callers that cannot reach pod addresses.
    Service(String),
}

/// Bolt adapter settings.
#[derive(Clone, Debug)]
pub struct BoltConfig {
    /// `GRAPH_DATABASE`: the database name that means the root scope.
    pub base_database: DatabaseName,
    /// `GRAPH_NAMESPACE` / `GRAPH_ID`: the root scope tenants hang under.
    pub root_scope: GraphScope,
    /// `GRAPH_CELL_ID`: the cell every unit targets, since a Bolt request has
    /// no field that names one.
    pub cell_id: String,
    /// The cluster token: what clients must present, and what the proxy logs
    /// in to backends with.
    pub token: Arc<str>,
    pub route_ttl_secs: i64,
    pub advertise: Advertise,
    /// The address advertised when the replica set is empty.
    pub self_address: String,
    pub max_message_bytes: usize,
    pub authentication_timeout: Duration,
    pub pool: PoolConfig,
}

/// The adapter: shared by every client session of one proxy process.
pub struct BoltProxy<P: Placement = RendezvousPlacement> {
    router: Arc<Router<P>>,
    pools: BackendPools,
    resolver: DatabaseResolver,
    config: BoltConfig,
}

impl<P: Placement> BoltProxy<P> {
    pub fn new(router: Arc<Router<P>>, config: BoltConfig) -> Self {
        let pools = BackendPools::new(
            config.pool.clone(),
            Arc::clone(&config.token),
            config.max_message_bytes,
            router.metrics().registry(),
        );
        let resolver = DatabaseResolver::new(config.base_database.clone(), config.root_scope.clone(), config.cell_id.clone());
        Self {
            router,
            pools,
            resolver,
            config,
        }
    }

    pub fn router(&self) -> &Arc<Router<P>> {
        &self.router
    }

    pub fn pools(&self) -> &BackendPools {
        &self.pools
    }

    pub fn config(&self) -> &BoltConfig {
        &self.config
    }

    /// Where a unit for `database` would go right now, best first, without
    /// sending anything. Returns the scope and the candidate node ids.
    pub async fn debug_route(&self, database: Option<&str>, read: bool) -> Result<(String, Vec<String>), String> {
        let key = self.resolver.resolve(database).map_err(|error| error.to_string())?;
        let operation = turbolay_proxy_core::types::Operation {
            access: if read {
                turbolay_proxy_core::types::Access::Read
            } else {
                turbolay_proxy_core::types::Access::Write
            },
            protocol: turbolay_proxy_core::types::Protocol::Bolt,
        };
        let route = self
            .router
            .route(&key, operation)
            .await
            .map_err(|_| "no graph node is configured".to_string())?;
        Ok((key.scope().to_string(), route.candidates.iter().map(|id| id.to_string()).collect()))
    }
}
