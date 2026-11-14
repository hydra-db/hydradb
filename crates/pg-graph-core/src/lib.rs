#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vertex {
    pub id: i64,
    pub sort_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub id: i64,
    pub source: i64,
    pub target: i64,
    pub kind: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSnapshot {
    pub vertices: Vec<Vertex>,
    pub edges: Vec<Edge>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Outgoing,
    Incoming,
    Both,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphError {
    DuplicateVertexId(i64),
    DuplicateEdgeId(i64),
    DanglingEndpoint {
        edge_id: i64,
        vertex_id: i64,
    },
    MissingVertex(i64),
    /// A seed that names a vertex by table label and key, where no such vertex exists.
    MissingVertexKey {
        label: String,
        key: String,
    },
    InvalidInput(&'static str),
    InvalidConfig(&'static str),
    InconsistentGraph(&'static str),
    Source(String),
    /// A work or size budget was exceeded; the request is rejected rather than truncated.
    LimitExceeded {
        operation: &'static str,
        actual: u64,
        limit: u64,
    },
    Cancelled,
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateVertexId(id) => write!(f, "duplicate vertex ID {id}"),
            Self::DuplicateEdgeId(id) => write!(f, "duplicate edge ID {id}"),
            Self::DanglingEndpoint { edge_id, vertex_id } => {
                write!(f, "edge {edge_id} refers to missing vertex {vertex_id}")
            }
            Self::MissingVertex(id) => write!(f, "missing vertex {id}"),
            Self::MissingVertexKey { label, key } => write!(f, "missing vertex {label} {key}"),
            Self::InvalidInput(message) | Self::InvalidConfig(message) | Self::InconsistentGraph(message) => f.write_str(message),
            Self::Source(message) => write!(f, "snapshot source: {message}"),
            Self::LimitExceeded { operation, actual, limit } => {
                write!(f, "{operation} limit exceeded: {actual} > {limit}")
            }
            Self::Cancelled => f.write_str("query cancelled"),
        }
    }
}

impl Error for GraphError {}

/// Proof that a graph has loaded the edges a traversal reads. Only this crate
/// makes one, inside `GraphRead::expand`, so a traversal can read a vertex's
/// edges only through the `Expansion` that loaded them.
#[derive(Clone, Copy)]
pub struct Expanded(());

/// A graph after `expand` loaded a frontier's edges: reads its vertices and
/// the edges it loaded.
pub struct Expansion<'g, G> {
    graph: &'g G,
    direction: Direction,
    token: Expanded,
}

impl<'g, G: GraphRead> Expansion<'g, G> {
    /// The graph, for vertex and order-key reads alongside the edges.
    pub fn graph(&self) -> &'g G {
        self.graph
    }

    /// The edges of a vertex in the expanded direction, in the order
    /// `GraphRead::loaded_edges` gives. A vertex whose edges the graph has not
    /// loaded is an error, not a panic.
    pub fn incident(&self, id: i64) -> Result<impl Iterator<Item = &'g Edge> + 'g, GraphError> {
        self.incident_after(id, None)
    }

    /// The edges of a vertex that come after edge `after` in that order, or
    /// all of them when `after` is `None`, found without stepping over the
    /// earlier ones, so a traversal can read a large vertex in batches.
    pub fn incident_after(&self, id: i64, after: Option<i64>) -> Result<impl Iterator<Item = &'g Edge> + 'g, GraphError> {
        self.graph
            .loaded_edges(self.token, id, self.direction, after)
            .ok_or(GraphError::InconsistentGraph(
                "a traversal read the edges of a vertex its graph did not load",
            ))
    }
}

/// A graph a traversal reads. A graph that loads on demand loads in
/// `load_edges` and `sort_by_order`, which take `&mut self`; its reads take
/// `&self`. A traversal reads edges only through `expand`, which loads a
/// whole frontier in one batch and then reads it, and orders vertices only
/// through `sort_by_order`, which loads what the order needs and sorts.
pub trait GraphRead {
    fn vertices(&self) -> &[Vertex];
    fn vertex(&self, id: i64) -> Option<&Vertex>;
    fn edge(&self, id: i64) -> Option<&Edge>;

    /// Loads the edges of every vertex in `ids` in `direction`, the frontier
    /// at once. In-memory graphs have nothing to do. Traversals call it
    /// through `expand`.
    fn load_edges(&mut self, _ids: &[i64], _direction: Direction) -> Result<(), GraphError> {
        Ok(())
    }

    /// The edges of a vertex in `direction`, or `None` when they are not
    /// loaded. An ordered graph returns them by source vertex order, then
    /// target vertex order, then edge id, a vertex's order being
    /// `(sort_key, id)`; a graph whose `ordered` is false returns them by edge
    /// id. With `after`, an edge of the vertex, only the edges after it in that
    /// order, reached without stepping over the earlier ones. Only an
    /// `Expansion` holds the `Expanded` it takes.
    fn loaded_edges(&self, token: Expanded, id: i64, direction: Direction, after: Option<i64>) -> Option<impl Iterator<Item = &Edge>>;

    /// Loads the edges of `frontier` in `direction` and returns the graph to
    /// read them.
    fn expand(&mut self, frontier: &[i64], direction: Direction) -> Result<Expansion<'_, Self>, GraphError>
    where
        Self: Sized,
    {
        self.load_edges(frontier, direction)?;
        Ok(Expansion {
            graph: self,
            direction,
            token: Expanded(()),
        })
    }

    /// False for a graph that loads its vertices' sort keys only on demand. The
    /// traversal then orders a layer with `sort_by_order` only when a limit
    /// cuts it, so it keeps the same vertices and edges an ordered graph
    /// keeps, and otherwise emits them in id order.
    fn ordered(&self) -> bool {
        true
    }

    /// The given vertices, each once, sorted by sort key, ties broken in the
    /// graph's `(label, key)` order, loading what that needs. The default
    /// sorts by `(sort_key, id)`, which is that order in a graph whose ids are
    /// numbered in `(label, key)` order, as a full load numbers them. A vertex
    /// the graph does not have is an error.
    ///
    /// A graph must not keep what it loads here for vertices it does not
    /// otherwise hold: a cut layer is ranked by calling this with each batch
    /// of candidates and the few kept so far, so a graph that cached every
    /// vertex it ordered would hold the whole layer's sort keys, unbounded by
    /// the traversal's limits.
    fn sort_by_order(&mut self, ids: &[i64]) -> Result<Vec<i64>, GraphError> {
        let mut keyed = ids
            .iter()
            .map(|&id| {
                let vertex = self.vertex(id).ok_or(GraphError::MissingVertex(id))?;
                Ok((vertex.sort_key.as_str(), id))
            })
            .collect::<Result<Vec<_>, GraphError>>()?;
        keyed.sort_unstable();
        keyed.dedup();
        Ok(keyed.into_iter().map(|(_, id)| id).collect())
    }
}
