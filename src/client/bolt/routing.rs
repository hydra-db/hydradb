use async_trait::async_trait;

use super::*;
use crate::{validate_component, ShardPlacement};

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

/// Bolt routing backed by the same deterministic placement used to open data
/// shards. Membership is supplied by service discovery (or a static deployment
/// directory); it is not persisted in a separate consensus/control database.
#[derive(Clone)]
pub struct RendezvousBoltRoutingTableProvider {
    placement: ShardPlacement,
    node_addresses: BTreeMap<String, String>,
    routing_ttl_secs: i64,
}

impl RendezvousBoltRoutingTableProvider {
    pub fn new(
        placement: ShardPlacement,
        node_addresses: impl IntoIterator<Item = (String, String)>,
        routing_ttl_secs: i64,
    ) -> Result<Self> {
        if routing_ttl_secs <= 0 {
            return bolt_config_error("rendezvous routing TTL must be greater than zero");
        }
        let mut addresses = BTreeMap::new();
        for (node_id, address) in node_addresses {
            validate_component("node_id", &node_id)?;
            let address = address.trim().to_string();
            if address.is_empty() || addresses.insert(node_id, address).is_some() {
                return bolt_config_error(
                    "rendezvous routing node ids must be unique and addresses cannot be empty",
                );
            }
        }
        if addresses.is_empty() {
            return bolt_config_error("rendezvous routing requires at least one node address");
        }
        for node_id in placement.node_ids() {
            if !addresses.contains_key(node_id) {
                return bolt_config_error(
                    "every rendezvous placement node must have a Bolt address",
                );
            }
        }
        Ok(Self {
            placement,
            node_addresses: addresses,
            routing_ttl_secs,
        })
    }
}

#[async_trait]
impl BoltRoutingTableProvider for RendezvousBoltRoutingTableProvider {
    async fn routing_table(
        &self,
        _database: &str,
        target: &ClientQueryTarget,
    ) -> Result<BoltRoutingTable> {
        let owner = self.placement.owner(&target.cell_id)?;
        let owner_address =
            self.node_addresses
                .get(owner)
                .cloned()
                .ok_or_else(|| GraphError::UnknownShard {
                    cell_id: target.cell_id.clone(),
                })?;
        BoltRoutingTable::new(
            self.routing_ttl_secs,
            vec![
                BoltRoutingServer::new("ROUTE", self.node_addresses.values().cloned())?,
                BoltRoutingServer::new("READ", [owner_address.clone()])?,
                BoltRoutingServer::new("WRITE", [owner_address])?,
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
