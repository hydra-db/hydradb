use std::fmt;

use hydradb_placement::NodeAddresses;
use turbolay_proxy_types::{NodeId, NodeInfo, Roles};

/// A membership configuration the proxy refuses to start with.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyError(pub String);

impl fmt::Display for TopologyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TopologyError {}

/// Join the Bolt and HTTP address lists into nodes. With an HTTP list, its ids
/// must be exactly the Bolt ids: a node the proxy can reach over one protocol
/// and not the other would route the two differently.
pub fn static_nodes(bolt: &NodeAddresses, http: Option<&NodeAddresses>) -> Result<Vec<NodeInfo>, TopologyError> {
    if let Some(http) = http {
        if !bolt.ids().eq(http.ids()) {
            return Err(TopologyError(format!(
                "HTTP node ids {:?} differ from Bolt node ids {:?}",
                http.ids().collect::<Vec<_>>(),
                bolt.ids().collect::<Vec<_>>()
            )));
        }
    }
    Ok(bolt
        .iter()
        .map(|(id, address)| NodeInfo {
            id: NodeId::new(id),
            bolt_addr: address.to_string(),
            http_addr: http.and_then(|http| http.get(id)).map(str::to_string),
            roles: Roles::default(),
        })
        .collect())
}
