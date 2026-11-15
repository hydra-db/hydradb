#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use hydradb_pg_graph_core::{Direction, Edge, Expanded, GraphError, GraphRead, GraphSnapshot, Vertex};
use std::collections::HashMap;

#[derive(Debug)]
pub struct CsrGraph {
    vertices: Vec<Vertex>,
    edges: Vec<Edge>,
    vertex_index: HashMap<i64, usize>,
    edge_index: HashMap<i64, usize>,
    outgoing: Adjacency,
    incoming: Adjacency,
}

#[derive(Debug)]
struct Adjacency {
    offsets: Vec<usize>,
    edges: Vec<usize>,
}

impl Adjacency {
    /// `ends` holds each edge's (source, target) vertex positions, in edge order.
    /// `check` is asked at every edge of both passes.
    fn build(
        vertex_count: usize,
        ends: &[(u32, u32)],
        outgoing: bool,
        check: &mut impl FnMut(usize) -> Result<(), GraphError>,
    ) -> Result<Self, GraphError> {
        let endpoint = |&(source, target): &(u32, u32)| if outgoing { source } else { target } as usize;
        let mut offsets = vec![0; vertex_count + 1];
        for (i, end) in ends.iter().enumerate() {
            check(i)?;
            offsets[endpoint(end) + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut cursor = offsets.clone();
        let mut indices = vec![0; ends.len()];
        for (i, end) in ends.iter().enumerate() {
            check(i)?;
            let vertex = endpoint(end);
            indices[cursor[vertex]] = i;
            cursor[vertex] += 1;
        }
        Ok(Self { offsets, edges: indices })
    }

    fn at(&self, vertex: usize) -> &[usize] {
        &self.edges[self.offsets[vertex]..self.offsets[vertex + 1]]
    }
}

/// Rows a build handles between cancellation checks.
const CHECK_EVERY: usize = 65_536;

impl CsrGraph {
    pub fn build(snapshot: GraphSnapshot) -> Result<Self, GraphError> {
        Self::build_cancellable(snapshot, || false)
    }

    /// Builds the graph, asking `cancelled` between phases, once more before it
    /// returns, and every `CHECK_EVERY` rows within a phase, including both
    /// adjacency builds, so a statement cancelled while a large graph is built
    /// stops then, not once the build ends. A sort runs to its end between
    /// checks.
    pub fn build_cancellable(mut snapshot: GraphSnapshot, mut cancelled: impl FnMut() -> bool) -> Result<Self, GraphError> {
        let mut check = |i: usize| {
            if i.is_multiple_of(CHECK_EVERY) && cancelled() {
                Err(GraphError::Cancelled)
            } else {
                Ok(())
            }
        };
        if snapshot.vertices.len() >= u32::MAX as usize || snapshot.edges.len() >= u32::MAX as usize {
            return Err(GraphError::InvalidInput("graph exceeds u32 vertex or edge positions"));
        }
        check(0)?;
        snapshot.vertices.sort_by(|a, b| (&a.sort_key, a.id).cmp(&(&b.sort_key, b.id)));
        let mut vertex_index = HashMap::with_capacity(snapshot.vertices.len());
        for (i, vertex) in snapshot.vertices.iter().enumerate() {
            check(i)?;
            if vertex_index.insert(vertex.id, i).is_some() {
                return Err(GraphError::DuplicateVertexId(vertex.id));
            }
        }
        check(0)?;
        let mut ids = snapshot.edges.iter().map(|edge| edge.id).collect::<Vec<_>>();
        ids.sort_unstable();
        if let Some(pair) = ids.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(GraphError::DuplicateEdgeId(pair[0]));
        }
        drop(ids);
        // Each edge's endpoint positions, looked up once, as the sort key.
        let mut order = Vec::with_capacity(snapshot.edges.len());
        for (i, edge) in snapshot.edges.iter().enumerate() {
            check(i)?;
            let position = |vertex_id: i64| {
                vertex_index.get(&vertex_id).map(|&p| p as u32).ok_or(GraphError::DanglingEndpoint {
                    edge_id: edge.id,
                    vertex_id,
                })
            };
            order.push((position(edge.source)?, position(edge.target)?, edge.id, i));
        }
        check(0)?;
        // Edge ids are unique, so the order is total and an unstable sort is exact.
        order.sort_unstable();
        let mut unsorted = snapshot.edges.into_iter().map(Some).collect::<Vec<_>>();
        let mut edges = Vec::with_capacity(order.len());
        let mut ends = Vec::with_capacity(order.len());
        let mut edge_index = HashMap::with_capacity(order.len());
        for (position, &(source, target, id, i)) in order.iter().enumerate() {
            check(position)?;
            edges.extend(unsorted[i].take());
            ends.push((source, target));
            edge_index.insert(id, position);
        }
        check(0)?;
        let outgoing = Adjacency::build(snapshot.vertices.len(), &ends, true, &mut check)?;
        check(0)?;
        let incoming = Adjacency::build(snapshot.vertices.len(), &ends, false, &mut check)?;
        check(0)?;
        Ok(Self {
            vertices: snapshot.vertices,
            edges,
            vertex_index,
            edge_index,
            outgoing,
            incoming,
        })
    }

    /// The edges of a vertex, by source vertex order, then target vertex
    /// order, then edge id. Every edge is in memory, so a path search over a
    /// loaded region reads them directly.
    pub fn incident(&self, id: i64, direction: Direction) -> Incident<'_> {
        let (mut outgoing, mut incoming) = (&[][..], &[][..]);
        if let Some(&vertex) = self.vertex_index.get(&id) {
            if direction != Direction::Incoming {
                outgoing = self.outgoing.at(vertex);
            }
            if direction != Direction::Outgoing {
                incoming = self.incoming.at(vertex);
            }
        }
        Incident::new(&self.edges, outgoing, incoming)
    }
}

/// The edges at two ascending lists of positions in `edges`, merged in position
/// order; a position in both lists, a self loop's, comes once.
pub struct Incident<'a> {
    edges: &'a [Edge],
    outgoing: &'a [usize],
    incoming: &'a [usize],
}

impl<'a> Incident<'a> {
    pub fn new(edges: &'a [Edge], outgoing: &'a [usize], incoming: &'a [usize]) -> Self {
        Self { edges, outgoing, incoming }
    }

    /// The edges after position `position`: both lists ascend, so each is cut
    /// by a binary search instead of stepping over the earlier positions.
    pub fn after(self, position: usize) -> Self {
        let rest = |list: &'a [usize]| &list[list.partition_point(|&p| p <= position)..];
        Self {
            edges: self.edges,
            outgoing: rest(self.outgoing),
            incoming: rest(self.incoming),
        }
    }
}

impl<'a> Iterator for Incident<'a> {
    type Item = &'a Edge;

    fn next(&mut self) -> Option<Self::Item> {
        let index = match (self.outgoing.first(), self.incoming.first()) {
            (None, None) => return None,
            (Some(&a), None) => a,
            (None, Some(&b)) => b,
            (Some(&a), Some(&b)) => a.min(b),
        };
        if self.outgoing.first() == Some(&index) {
            self.outgoing = &self.outgoing[1..];
        }
        if self.incoming.first() == Some(&index) {
            self.incoming = &self.incoming[1..];
        }
        Some(&self.edges[index])
    }
}

impl GraphRead for CsrGraph {
    fn vertices(&self) -> &[Vertex] {
        &self.vertices
    }

    fn vertex(&self, id: i64) -> Option<&Vertex> {
        self.vertex_index.get(&id).map(|&i| &self.vertices[i])
    }

    fn edge(&self, id: i64) -> Option<&Edge> {
        self.edge_index.get(&id).map(|&i| &self.edges[i])
    }

    /// Every edge is in memory, so every vertex's edges are loaded.
    fn loaded_edges(&self, _token: Expanded, id: i64, direction: Direction, after: Option<i64>) -> Option<impl Iterator<Item = &Edge>> {
        let edges = self.incident(id, direction);
        match after {
            None => Some(edges),
            Some(edge) => Some(edges.after(*self.edge_index.get(&edge)?)),
        }
    }
}
