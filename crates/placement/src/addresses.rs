//! The fleet's node addresses, as configured: `node-id=host:port,...`, the
//! format of `GRAPH_BOLT_NODE_ADDRESSES`. graph-node and the turbolay proxy
//! both parse it here, so they hash over the same set of ids.

use std::collections::BTreeMap;
use std::str::FromStr;

/// Node ids and their addresses, sorted by id. Never empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeAddresses(BTreeMap<String, String>);

/// Why a node address list was refused. The message leaves out the variable
/// name, so the caller can lead with it.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NodeAddressesError {
    /// An entry without `=`.
    #[error("entries must use node-id=host:port")]
    Malformed,
    /// An entry with an empty id or address, or an id seen before.
    #[error("contains an empty or duplicate node id/address")]
    EmptyOrDuplicate,
    /// No entries at all.
    #[error("must contain at least one endpoint")]
    Empty,
}

impl FromStr for NodeAddresses {
    type Err = NodeAddressesError;

    /// Entries and both sides of `=` are trimmed, and empty entries skipped.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let mut addresses = BTreeMap::new();
        for entry in raw.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
            let (node_id, address) = entry.split_once('=').ok_or(NodeAddressesError::Malformed)?;
            let (node_id, address) = (node_id.trim(), address.trim());
            if node_id.is_empty() || address.is_empty() || addresses.insert(node_id.to_string(), address.to_string()).is_some() {
                return Err(NodeAddressesError::EmptyOrDuplicate);
            }
        }
        if addresses.is_empty() {
            return Err(NodeAddressesError::Empty);
        }
        Ok(Self(addresses))
    }
}

impl NodeAddresses {
    /// The address of `node_id`.
    pub fn get(&self, node_id: &str) -> Option<&str> {
        self.0.get(node_id).map(String::as_str)
    }

    /// `(id, address)` pairs, sorted by id.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(id, address)| (id.as_str(), address.as_str()))
    }

    /// The node ids, sorted.
    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// The map from id to address.
    pub fn into_map(self) -> BTreeMap<String, String> {
        self.0
    }
}
