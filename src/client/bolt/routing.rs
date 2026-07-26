use async_trait::async_trait;

use super::*;
use crate::{validate_component, PlacementView};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoltRoutingServer {
    pub role: String,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoltRoutingTable {
    pub ttl_secs: i64,
    pub servers: Vec<BoltRoutingServer>,
}

impl BoltRoutingTable {
    pub fn new(ttl_secs: i64, servers: Vec<BoltRoutingServer>) -> Result<Self> {
        validate_bolt_routing_table(ttl_secs, &servers)?;
        Ok(Self { ttl_secs, servers })
    }
}

#[async_trait]
pub trait BoltRoutingTableProvider: Send + Sync {
    async fn routing_table(
        &self,
        database: &str,
        target: &ClientQueryTarget,
    ) -> Result<BoltRoutingTable>;
}

/// Bolt routing for object-store-native nodes.
///
/// Every advertised node can read any configured cell, so `READ` and `ROUTE`
/// name the whole live fleet. `WRITE` names exactly one node: the rendezvous
/// owner of the target cell, the same answer
/// [`RoutedGraphCluster::ensure_local_writer`] enforces. Advertising anything
/// else builds a loop no timeout breaks — routing sends the write to a node
/// that then refuses it as a non-owner, and the driver bounces between the two.
/// That is why decision 9 of `docs/plans/2026-07-25-rendezvous-placement.md`
/// deletes the old `with_preferred_writer_node` override rather than keeping it
/// as a pin.
///
/// # Liveness comes from the shared placement view, not from a probe
///
/// This used to fan out a `/readyz` probe to every configured node on every
/// routing refresh and advertise the reachable ones. Decision 4 deletes that
/// fan-out, and the reason is **consistency, not cost**: a probe is computed
/// per caller, so two drivers asking at the same instant could get different
/// answers, and rendezvous only converges if every reader derives ownership
/// from the *same* live set. One object-store LIST behind one
/// [`PlacementView`] gives that; N probes cannot. The `/readyz` endpoint
/// itself is untouched — the k8s readiness probe, the runtime smoke script and
/// the Jepsen harness still use it; only routing stopped calling it.
///
/// [`RoutedGraphCluster::ensure_local_writer`]: crate::RoutedGraphCluster
#[derive(Clone)]
pub struct ObjectStoreBoltRoutingTableProvider {
    node_addresses: BTreeMap<String, String>,
    /// The **shared** live set. This must be a clone of the handle the routed
    /// cluster holds: a second view over the same store reintroduces exactly
    /// the per-caller inconsistency deleting the probe removed.
    placement: PlacementView,
    routing_ttl_secs: i64,
}

impl ObjectStoreBoltRoutingTableProvider {
    pub fn new(
        node_addresses: impl IntoIterator<Item = (String, String)>,
        routing_ttl_secs: i64,
        placement: PlacementView,
    ) -> Result<Self> {
        if routing_ttl_secs <= 0 {
            return bolt_config_error("object-store routing TTL must be greater than zero");
        }
        let mut addresses = BTreeMap::new();
        for (node_id, address) in node_addresses {
            validate_component("node_id", &node_id)?;
            let address = address.trim().to_string();
            if address.is_empty() || addresses.insert(node_id, address).is_some() {
                return bolt_config_error(
                    "object-store routing node ids must be unique and addresses cannot be empty",
                );
            }
        }
        if addresses.is_empty() {
            return bolt_config_error("object-store routing requires at least one node address");
        }
        Ok(Self {
            node_addresses: addresses,
            placement,
            routing_ttl_secs,
        })
    }
}

/// This node cannot answer a routing request, and another node can.
///
/// Kept apart from [`bolt_config_error`] because the two reach a driver as
/// different classes: a config error is a `ClientError`, which ends the
/// attempt, while this is a `TransientError`, which sends the driver to the
/// next router. Decision 7 makes shedding a routine state — a node whose LIST
/// has been failing sheds while every peer keeps answering — so classifying it
/// as the client's mistake would turn one node's object-store trouble into a
/// failed query.
fn routing_unavailable<T>(reason: &str) -> Result<T> {
    Err(GraphError::RoutingUnavailable {
        reason: reason.to_string(),
    })
}

#[async_trait]
impl BoltRoutingTableProvider for ObjectStoreBoltRoutingTableProvider {
    async fn routing_table(
        &self,
        _database: &str,
        target: &ClientQueryTarget,
    ) -> Result<BoltRoutingTable> {
        // One snapshot for the whole table. Deriving the reader list and the
        // writer from two `view()` calls would let a refresh land between them
        // and produce a table naming two different fleets.
        let view = self.placement.view();
        let scope = target.scope.to_string();

        // `nodes()` is empty for a shed view (decision 7), so a node that has
        // lost sight of the fleet advertises nothing rather than a stale fleet.
        // Sorted by node id, because the underlying set is, so a table is stable
        // between refreshes that did not change the live set.
        let live_addresses = view
            .nodes()
            .iter()
            .filter_map(|node| self.node_addresses.get(&node.node_id).cloned())
            .collect::<Vec<_>>();
        if live_addresses.is_empty() {
            return routing_unavailable("no live graph node is addressable from this node");
        }

        // Deliberately resolved over the *unfiltered* live set, which is what
        // `ensure_local_writer` computes over: silently picking the runner-up
        // because the winner has no configured Bolt address would advertise a
        // node that refuses every write it is sent.
        let writer = match self.placement.owner_in(&view, &scope, &target.cell_id) {
            Some(owner) => match self.node_addresses.get(&owner) {
                Some(address) => address.clone(),
                // Config, not liveness: this node's directory and its Bolt
                // address map disagree about who the fleet is. `graph-node`
                // builds the directory *from* the address map's keys, so the
                // two cannot diverge there; reaching this means an embedder
                // built them separately, and no other router will answer
                // differently.
                None => {
                    return bolt_config_error(
                        "object-store routing has no Bolt address for the cell's owning node",
                    )
                }
            },
            // `None` covers both a known-empty fleet and a shed view. A routing
            // table has the same answer for either — there is no WRITE endpoint
            // to advertise — which is why collapsing them is safe here and is
            // not safe in `ensure_local_writer`.
            None => return routing_unavailable("no live node owns this cell"),
        };

        BoltRoutingTable::new(
            self.routing_ttl_secs,
            vec![
                BoltRoutingServer::new("ROUTE", live_addresses.clone())?,
                BoltRoutingServer::new("READ", live_addresses)?,
                BoltRoutingServer::new("WRITE", [writer])?,
            ],
        )
    }
}

impl BoltRoutingServer {
    pub fn new(
        role: impl Into<String>,
        addresses: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        let role = role.into().to_ascii_uppercase();
        if !matches!(role.as_str(), "ROUTE" | "READ" | "WRITE") {
            return Err(GraphError::InvalidKeyComponent {
                component: "bolt_routing_role",
                value: role,
            });
        }
        let addresses: Vec<_> = addresses
            .into_iter()
            .map(|address| address.trim().to_string())
            .collect();
        if addresses.is_empty()
            || addresses.iter().any(|address| {
                address.is_empty()
                    || address.len() > 1_024
                    || address
                        .chars()
                        .any(|character| character.is_control() || character.is_whitespace())
            })
        {
            return Err(GraphError::InvalidKeyComponent {
                component: "bolt_routing_address",
                value: addresses.join(","),
            });
        }
        Ok(Self { role, addresses })
    }
}

pub(super) fn validate_bolt_routing_table(
    ttl_secs: i64,
    servers: &[BoltRoutingServer],
) -> Result<()> {
    if ttl_secs <= 0 || servers.is_empty() {
        return bolt_config_error("Bolt routing table TTL and server list cannot be empty");
    }
    let mut roles = BTreeMap::new();
    for server in servers {
        let normalized = BoltRoutingServer::new(server.role.clone(), server.addresses.clone())?;
        if normalized != *server || roles.insert(server.role.as_str(), ()).is_some() {
            return bolt_config_error(
                "Bolt routing table roles must be normalized and appear exactly once",
            );
        }
    }
    if !["ROUTE", "READ", "WRITE"]
        .into_iter()
        .all(|role| roles.contains_key(role))
    {
        return bolt_config_error("Bolt routing table requires ROUTE, READ, and WRITE roles");
    }
    Ok(())
}
