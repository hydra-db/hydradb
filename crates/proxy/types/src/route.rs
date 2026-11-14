use std::fmt;

use compact_str::CompactString;
use hydradb_scope::{validate_component, GraphScope, ScopeResult};

/// Whether a unit reads or writes. Bolt reads it from the RUN extra
/// `mode: "r"`, HTTP from `x-graph-access`. Missing is a write in both.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Access {
    Read,
    Write,
}

impl Access {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// Which adapter a unit came through, for metric labels.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Protocol {
    Bolt,
    Http,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bolt => "bolt",
            Self::Http => "http",
        }
    }
}

/// One unit of work as the core sees it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Operation {
    pub access: Access,
    pub protocol: Protocol,
}

/// A graph-node's id, the string placement hashes. Cheap to clone: ids up to
/// 24 bytes, enough for pod names like `graph-node-12`, are stored inline.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(CompactString);

impl NodeId {
    pub fn new(id: impl AsRef<str>) -> Self {
        Self(CompactString::new(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a node may serve. Nodes serve both by default; the field lets writes
/// rank only writer nodes when reader and writer pods are split.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Roles {
    pub reader: bool,
    pub writer: bool,
}

impl Default for Roles {
    fn default() -> Self {
        Self {
            reader: true,
            writer: true,
        }
    }
}

impl Roles {
    pub fn serves(self, access: Access) -> bool {
        match access {
            Access::Read => self.reader,
            Access::Write => self.writer,
        }
    }
}

/// A backend graph-node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub bolt_addr: String,
    pub http_addr: Option<String>,
    pub roles: Roles,
}

/// What a unit is routed by: the scope placement hashes and the cell.
///
/// Built only from a [`GraphScope`], which only `hydradb-scope` constructs, so
/// the proxy hashes exactly the string a node would.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RouteKey {
    scope: String,
    cell: String,
}

impl RouteKey {
    pub fn new(scope: &GraphScope, cell: &str) -> ScopeResult<Self> {
        validate_component("cell_id", cell)?;
        Ok(Self {
            scope: scope.to_string(),
            cell: cell.to_string(),
        })
    }

    /// `GraphScope`'s Display, the placement hash input.
    pub fn scope(&self) -> &str {
        &self.scope
    }

    pub fn cell(&self) -> &str {
        &self.cell
    }
}

/// Everything routing reads, swapped atomically as a whole.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteSnapshot {
    /// Backend nodes, sorted by id, so every replica iterates them the same way.
    pub nodes: Vec<NodeInfo>,
    /// Bolt addresses of the ready proxy replicas, sorted, for the ROUTE answer.
    pub proxies: Vec<String>,
}

impl RouteSnapshot {
    /// The position and info of a node, by id.
    pub fn node(&self, id: &str) -> Option<(usize, &NodeInfo)> {
        self.nodes
            .binary_search_by(|node| node.id.as_str().cmp(id))
            .ok()
            .map(|index| (index, &self.nodes[index]))
    }

    /// How many proxy replicas share each node's budget. Never zero: a replica
    /// that cannot see the set still counts itself.
    pub fn replica_count(&self) -> usize {
        self.proxies.len().max(1)
    }
}
