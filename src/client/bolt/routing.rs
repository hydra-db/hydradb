use async_trait::async_trait;
use futures::future::join_all;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
/// advertised for writes to preserve a warm writer cache. Nodes are advertised
/// only after their application readiness endpoint returns HTTP 200. SlateDB
/// remains the authoritative writer fence.
#[derive(Clone)]
pub struct ObjectStoreBoltRoutingTableProvider {
    node_addresses: BTreeMap<String, String>,
    readiness_addresses: BTreeMap<String, String>,
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
        let readiness_addresses = addresses
            .iter()
            .map(|(node_id, address)| {
                replace_address_port(address, 9090).map(|readiness| (node_id.clone(), readiness))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            node_addresses: addresses,
            readiness_addresses,
            preferred_writer_node,
            routing_ttl_secs,
            health_probe_timeout: DEFAULT_ROUTING_HEALTH_PROBE_TIMEOUT,
        })
    }

    pub fn with_readiness_addresses(
        mut self,
        readiness_addresses: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self> {
        let mut parsed = BTreeMap::new();
        for (node_id, address) in readiness_addresses {
            validate_component("node_id", &node_id)?;
            let address = address.trim().to_string();
            if address.is_empty() || parsed.insert(node_id, address).is_some() {
                return bolt_config_error(
                    "routing readiness node ids must be unique and addresses cannot be empty",
                );
            }
        }
        let readiness_addresses = parsed;
        if readiness_addresses.keys().ne(self.node_addresses.keys()) {
            return bolt_config_error(
                "routing readiness addresses must identify every configured graph node",
            );
        }
        self.readiness_addresses = readiness_addresses;
        Ok(self)
    }

    pub fn with_readiness_port(mut self, port: u16) -> Result<Self> {
        if port == 0 {
            return bolt_config_error("routing readiness port must be greater than zero");
        }
        self.readiness_addresses = self
            .node_addresses
            .iter()
            .map(|(node_id, address)| {
                replace_address_port(address, port).map(|readiness| (node_id.clone(), readiness))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(self)
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
            let readiness_address = self
                .readiness_addresses
                .get(&node_id)
                .expect("readiness addresses cover every graph node")
                .clone();
            async move {
                let reachable =
                    probe_node_readiness(&readiness_address, self.health_probe_timeout).await;
                reachable.then_some((node_id, address))
            }
        });
        join_all(probes).await.into_iter().flatten().collect()
    }
}

fn replace_address_port(address: &str, port: u16) -> Result<String> {
    let host = if let Some(rest) = address.strip_prefix('[') {
        let (host, suffix) =
            rest.split_once(']')
                .ok_or_else(|| GraphError::InvalidKeyComponent {
                    component: "bolt_routing_address",
                    value: address.to_string(),
                })?;
        if !suffix.starts_with(':') || suffix[1..].parse::<u16>().is_err() || host.is_empty() {
            return bolt_config_error("routing addresses must use host:port syntax");
        }
        format!("[{host}]")
    } else {
        address
            .rsplit_once(':')
            .filter(|(host, source_port)| !host.is_empty() && source_port.parse::<u16>().is_ok())
            .map(|(host, _)| host.to_string())
            .ok_or_else(|| GraphError::InvalidKeyComponent {
                component: "bolt_routing_address",
                value: address.to_string(),
            })?
    };
    Ok(format!("{host}:{port}"))
}

async fn probe_node_readiness(address: &str, probe_timeout: Duration) -> bool {
    let probe = async {
        let mut stream = TcpStream::connect(address).await?;
        let request =
            format!("GET /readyz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::with_capacity(64);
        while response.len() < 64 && !response.windows(2).any(|bytes| bytes == b"\r\n") {
            let mut chunk = [0_u8; 32];
            let bytes_read = stream.read(&mut chunk).await?;
            if bytes_read == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..bytes_read]);
        }
        Ok::<bool, std::io::Error>(
            response.starts_with(b"HTTP/1.1 200 ") || response.starts_with(b"HTTP/1.0 200 "),
        )
    };
    timeout(probe_timeout, probe)
        .await
        .is_ok_and(|result| result.unwrap_or(false))
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
