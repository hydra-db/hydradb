//! Backend membership and the proxy replica set, merged into one
//! [`RouteSnapshot`] and published on a watch channel. The router applies each
//! one, and routing reads its copy without locking.
//!
//! Two kinds of source feed it:
//!
//! - a [`MembershipSource`]: which graph-nodes exist. The one implementation
//!   is static config from the same `GRAPH_BOLT_NODE_ADDRESSES` the nodes read,
//!   so the proxy and the nodes hash over the same set by construction
//! - a [`ProxySetSource`]: which proxy replicas are ready, for the ROUTE answer
//!   and for dividing each node's budget. Static, or a polled file that stands
//!   in for EndpointSlice events in local runs and tests

#![forbid(unsafe_code)]

mod config;
mod sources;

use std::sync::Arc;

use tokio::sync::watch;
use turbolay_proxy_types::{NodeInfo, RouteSnapshot};

pub use config::{static_nodes, TopologyError};
pub use hydradb_placement::{NodeAddresses, NodeAddressesError};
pub use sources::{parse_proxy_file, FileProxySet, MembershipSource, ProxySetSource, StaticMembership, StaticProxySet};

/// Owns the live snapshot and publishes what the sources report. It is the
/// only publisher: it is not `Clone`, and [`Topology::run`] consumes it.
pub struct Topology {
    snapshot: watch::Sender<Arc<RouteSnapshot>>,
}

impl Topology {
    /// A topology starting from `initial` nodes and replicas, before any source
    /// has run, so routing works from the first request.
    pub fn new(initial_nodes: Vec<NodeInfo>, initial_proxies: Vec<String>) -> Self {
        Self {
            snapshot: watch::Sender::new(Arc::new(build(initial_nodes, initial_proxies))),
        }
    }

    /// The snapshot as of now.
    pub fn current(&self) -> Arc<RouteSnapshot> {
        Arc::clone(&self.snapshot.borrow())
    }

    /// Every snapshot published from now on.
    pub fn subscribe(&self) -> watch::Receiver<Arc<RouteSnapshot>> {
        self.snapshot.subscribe()
    }

    /// Run both sources and publish every change until both end.
    pub async fn run(self, membership: Box<dyn MembershipSource>, proxies: Box<dyn ProxySetSource>) {
        let current = self.current();
        let (nodes_tx, mut nodes_rx) = watch::channel(current.nodes.clone());
        let (proxies_tx, mut proxies_rx) = watch::channel(current.proxies.clone());
        tokio::spawn(membership.run(nodes_tx));
        tokio::spawn(proxies.run(proxies_tx));
        // A source that ends keeps its last value; the other still publishes.
        let (mut nodes_open, mut proxies_open) = (true, true);
        while nodes_open || proxies_open {
            tokio::select! {
                changed = nodes_rx.changed(), if nodes_open => if changed.is_err() {
                    nodes_open = false;
                    continue;
                },
                changed = proxies_rx.changed(), if proxies_open => if changed.is_err() {
                    proxies_open = false;
                    continue;
                },
            }
            let nodes = nodes_rx.borrow_and_update().clone();
            let proxies = proxies_rx.borrow_and_update().clone();
            self.publish(nodes, proxies);
        }
    }

    /// Publish a new snapshot if anything changed.
    fn publish(&self, nodes: Vec<NodeInfo>, proxies: Vec<String>) {
        self.snapshot.send_if_modified(|current| {
            let next = build(nodes, proxies);
            if next == **current {
                return false;
            }
            tracing::info!(nodes = next.nodes.len(), proxies = ?next.proxies, "route snapshot changed");
            *current = Arc::new(next);
            true
        });
    }
}

fn build(mut nodes: Vec<NodeInfo>, mut proxies: Vec<String>) -> RouteSnapshot {
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    proxies.sort();
    proxies.dedup();
    RouteSnapshot { nodes, proxies }
}
