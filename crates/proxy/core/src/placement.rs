use std::future::Future;

use smallvec::SmallVec;
use turbolay_proxy_types::{Access, NodeId, NodeInfo, RouteKey, RouteSnapshot};

/// Who can serve a route key, best first.
///
/// Async because the seam is for a lookup that may do I/O (a page server, or a
/// node that has to be started). Statically dispatched: the router is generic
/// over it, so the rendezvous implementation costs no allocation per unit.
pub trait Placement: Send + Sync + 'static {
    fn resolve(&self, key: &RouteKey, access: Access, snapshot: &RouteSnapshot) -> impl Future<Output = Vec<NodeId>> + Send;

    /// The single best candidate: what writes, and reads under `owner`, use.
    /// The default takes the head of [`Self::resolve`]; an implementation can
    /// answer it more cheaply.
    fn owner(&self, key: &RouteKey, access: Access, snapshot: &RouteSnapshot) -> impl Future<Output = Option<NodeId>> + Send {
        let ranked = self.resolve(key, access, snapshot);
        async move { ranked.await.into_iter().next() }
    }
}

/// Rendezvous hashing through `hydradb_placement::rank`, the same frozen score
/// every graph-node computes. Writes rank the whole fleet, exactly as the
/// nodes do: ownership is decided there, so a role cannot move it. Reads rank
/// the nodes that serve reads.
#[derive(Clone, Copy, Debug, Default)]
pub struct RendezvousPlacement;

/// Whether `node` is ranked for `access`.
fn eligible(node: &NodeInfo, access: Access) -> bool {
    access == Access::Write || node.roles.serves(access)
}

impl RendezvousPlacement {
    /// Every eligible node, best first. The same order as
    /// `hydradb_placement::rank`: frozen score descending, node id ascending on
    /// ties. Scored on the stack for fleets up to 16 nodes, so a unit costs one
    /// allocation, the result.
    pub fn rank(key: &RouteKey, access: Access, snapshot: &RouteSnapshot) -> Vec<NodeId> {
        let mut entries: SmallVec<[(u64, usize); 16]> = snapshot
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| eligible(node, access))
            .map(|(index, node)| (hydradb_placement::score(key.scope(), key.cell(), node.id.as_str()), index))
            .collect();
        // Nodes are sorted by id, so index order is id order for the tie break.
        entries.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        entries.iter().map(|(_, index)| snapshot.nodes[*index].id.clone()).collect()
    }

    /// The owner alone: the top of [`Self::rank`] without sorting or
    /// allocating for the rest.
    pub fn owner(key: &RouteKey, access: Access, snapshot: &RouteSnapshot) -> Option<NodeId> {
        snapshot
            .nodes
            .iter()
            .filter(|node| eligible(node, access))
            .map(|node| (hydradb_placement::score(key.scope(), key.cell(), node.id.as_str()), node))
            .max_by(|a, b| a.0.cmp(&b.0).then(b.1.id.cmp(&a.1.id)))
            .map(|(_, node)| node.id.clone())
    }
}

impl Placement for RendezvousPlacement {
    fn resolve(&self, key: &RouteKey, access: Access, snapshot: &RouteSnapshot) -> impl Future<Output = Vec<NodeId>> + Send {
        std::future::ready(Self::rank(key, access, snapshot))
    }

    fn owner(&self, key: &RouteKey, access: Access, snapshot: &RouteSnapshot) -> impl Future<Output = Option<NodeId>> + Send {
        std::future::ready(Self::owner(key, access, snapshot))
    }
}
