use async_trait::async_trait;
use futures::future::join_all;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::*;
use crate::validate_component;

const DEFAULT_ROUTING_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_millis(250);

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

/// Bolt routing for object-store-native nodes. Every advertised node can read
/// any configured cell. The first reachable node in stable preference order is
/// advertised for writes to preserve a warm writer cache. Reachability is only
/// routing affinity because SlateDB remains the authoritative writer fence.
#[derive(Clone)]
pub struct ObjectStoreBoltRoutingTableProvider {
    node_addresses: BTreeMap<String, String>,
    preferred_writer_node: String,
    routing_ttl_secs: i64,
    health_probe_timeout: Duration,
}

impl ObjectStoreBoltRoutingTableProvider {
    pub fn new(
        node_addresses: impl IntoIterator<Item = (String, String)>,
        routing_ttl_secs: i64,
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
        let preferred_writer_node = addresses
            .keys()
            .next()
            .expect("non-empty address map")
            .clone();
        Ok(Self {
            node_addresses: addresses,
            preferred_writer_node,
            routing_ttl_secs,
            health_probe_timeout: DEFAULT_ROUTING_HEALTH_PROBE_TIMEOUT,
        })
    }

    pub fn with_preferred_writer_node(mut self, node_id: impl Into<String>) -> Result<Self> {
        let node_id = node_id.into();
        validate_component("node_id", &node_id)?;
        if !self.node_addresses.contains_key(&node_id) {
            return bolt_config_error("preferred writer node must be present in routing addresses");
        }
        self.preferred_writer_node = node_id;
        Ok(self)
    }

    pub fn with_health_probe_timeout(mut self, probe_timeout: Duration) -> Result<Self> {
        if probe_timeout.is_zero() {
            return bolt_config_error("routing health probe timeout must be greater than zero");
        }
        self.health_probe_timeout = probe_timeout;
        Ok(self)
    }

    async fn reachable_nodes(&self) -> Vec<(String, String)> {
        let probes = self.node_addresses.iter().map(|(node_id, address)| {
            let node_id = node_id.clone();
            let address = address.clone();
            async move {
                let reachable = timeout(self.health_probe_timeout, TcpStream::connect(&address))
                    .await
                    .is_ok_and(|result| result.is_ok());
                reachable.then_some((node_id, address))
            }
        });
        join_all(probes).await.into_iter().flatten().collect()
    }
}

#[async_trait]
impl BoltRoutingTableProvider for ObjectStoreBoltRoutingTableProvider {
    async fn routing_table(
        &self,
        _database: &str,
        _target: &ClientQueryTarget,
    ) -> Result<BoltRoutingTable> {
        let reachable = self.reachable_nodes().await;
        if reachable.is_empty() {
            return bolt_config_error("object-store routing found no reachable graph nodes");
        }
        let addresses = reachable
            .iter()
            .map(|(_, address)| address.clone())
            .collect::<Vec<_>>();
        let writer = reachable
            .iter()
            .find(|(node_id, _)| node_id == &self.preferred_writer_node)
            .or_else(|| reachable.first())
            .expect("reachable node list was checked")
            .1
            .clone();
        BoltRoutingTable::new(
            self.routing_ttl_secs,
            vec![
                BoltRoutingServer::new("ROUTE", addresses.clone())?,
                BoltRoutingServer::new("READ", addresses)?,
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
