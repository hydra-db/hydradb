use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use super::*;
use crate::{validate_component, GraphControlClient, GraphNodeHealthState};

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

#[derive(Clone)]
pub struct ControllerBoltRoutingTableProvider {
    control: Arc<dyn GraphControlClient>,
    node_addresses: BTreeMap<String, String>,
    heartbeat_ttl: Duration,
    routing_ttl_secs: i64,
}

impl ControllerBoltRoutingTableProvider {
    pub fn new<C>(
        control: Arc<C>,
        node_addresses: impl IntoIterator<Item = (String, String)>,
        heartbeat_ttl: Duration,
        routing_ttl_secs: i64,
    ) -> Result<Self>
    where
        C: GraphControlClient + 'static,
    {
        if heartbeat_ttl.is_zero() || routing_ttl_secs <= 0 {
            return bolt_config_error(
                "controller routing heartbeat TTL and routing TTL must be greater than zero",
            );
        }
        let mut addresses = BTreeMap::new();
        for (node_id, address) in node_addresses {
            validate_component("node_id", &node_id)?;
            if address.trim().is_empty() || addresses.insert(node_id, address).is_some() {
                return bolt_config_error(
                    "controller routing node ids must be unique and addresses cannot be empty",
                );
            }
        }
        if addresses.is_empty() {
            return bolt_config_error("controller routing requires at least one node address");
        }
        Ok(Self {
            control,
            node_addresses: addresses,
            heartbeat_ttl,
            routing_ttl_secs,
        })
    }
}

#[async_trait]
impl BoltRoutingTableProvider for ControllerBoltRoutingTableProvider {
    async fn routing_table(
        &self,
        _database: &str,
        target: &ClientQueryTarget,
    ) -> Result<BoltRoutingTable> {
        if self.control.scope() != &target.scope {
            return Err(GraphError::GraphScopeMismatch {
                expected: self.control.scope().to_string(),
                actual: target.scope.to_string(),
            });
        }
        let now_ms = bolt_now_millis();
        let heartbeat_ttl_ms = u64::try_from(self.heartbeat_ttl.as_millis()).unwrap_or(u64::MAX);
        let mut live_addresses = BTreeMap::new();
        let mut heartbeat_remaining_ms = u64::MAX;
        for heartbeat in self.control.load_node_heartbeats().await? {
            let age_ms = now_ms.saturating_sub(heartbeat.last_seen_ms);
            if heartbeat.state != GraphNodeHealthState::Active
                || age_ms >= heartbeat_ttl_ms
                || heartbeat_ttl_ms.saturating_sub(age_ms) < 1_000
            {
                continue;
            }
            if let Some(address) = self.node_addresses.get(&heartbeat.node_id) {
                live_addresses.insert(heartbeat.node_id, address.clone());
                heartbeat_remaining_ms =
                    heartbeat_remaining_ms.min(heartbeat_ttl_ms.saturating_sub(age_ms));
            }
        }
        if live_addresses.is_empty() {
            return Err(GraphError::AdmissionRejected {
                operation: "bolt_routing_live_nodes",
                actual: 0,
                limit: 1,
            });
        }
        let lease = self
            .control
            .current_lease(&target.cell_id)
            .await?
            .filter(|lease| lease.expires_at_ms.saturating_sub(now_ms) >= 1_000)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: target.cell_id.clone(),
            })?;
        let write_address = live_addresses
            .get(&lease.owner_node_id)
            .cloned()
            .ok_or_else(|| GraphError::ShardNotOwned {
                cell_id: target.cell_id.clone(),
                owner_node_id: lease.owner_node_id.clone(),
                local_node_id: "no live Bolt endpoint".to_string(),
            })?;
        let route_addresses: Vec<_> = live_addresses.into_values().collect();
        let lease_remaining_secs = lease.expires_at_ms.saturating_sub(now_ms) / 1_000;
        let heartbeat_remaining_secs = heartbeat_remaining_ms / 1_000;
        let ttl_secs = self
            .routing_ttl_secs
            .min(i64::try_from(lease_remaining_secs).unwrap_or(i64::MAX))
            .min(i64::try_from(heartbeat_remaining_secs).unwrap_or(i64::MAX));
        BoltRoutingTable::new(
            ttl_secs,
            vec![
                BoltRoutingServer::new("ROUTE", route_addresses)?,
                // Only the lease owner opens this cell's shard. Advertising non-owners as
                // readers would send Bolt clients to nodes that must reject the query.
                BoltRoutingServer::new("READ", [write_address.clone()])?,
                BoltRoutingServer::new("WRITE", [write_address])?,
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

fn bolt_now_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}
