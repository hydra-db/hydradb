#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use hydradb_pg_graph_core::{Direction, Edge, GraphError, GraphRead};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::mem::size_of;

mod paths;
pub use paths::{
    native_paths, NativePath, PathConfig, PathKind, PathQuery, PathResult, PathStats, DEFAULT_PATH_LEN, MAX_PATH_LEN, MAX_PATH_RESULTS,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryConfig {
    pub direction: Direction,
    pub edge_kind: Option<String>,
    pub max_depth: u32,
    pub max_vertices: usize,
    pub max_edges: usize,
    pub max_edge_visits: usize,
    pub max_result_bytes: usize,
    pub max_sources: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            edge_kind: None,
            max_depth: 5,
            max_vertices: 20_000,
            max_edges: 5_000,
            max_edge_visits: 1_000_000,
            max_result_bytes: 8 * 1024 * 1024,
            max_sources: 32,
        }
    }
}

impl QueryConfig {
    pub fn validate(&self) -> Result<(), GraphError> {
        if self.max_depth > 255 || self.max_vertices == 0 || self.max_vertices > 20_000 {
            return Err(GraphError::InvalidConfig(
                "max_depth must be <= 255 and max_vertices within 1..=20000",
            ));
        }
        if self.max_edges > 5_000 || self.max_edge_visits > 10_000_000 || self.max_result_bytes > 64 * 1024 * 1024 {
            return Err(GraphError::InvalidConfig(
                "edge, work or result-byte budget exceeds its hard ceiling",
            ));
        }
        if !(1..=128).contains(&self.max_sources) {
            return Err(GraphError::InvalidConfig("max_sources must be within 1..=128"));
        }
        Ok(())
    }
}

pub trait Checkpoint {
    fn is_cancelled(&mut self) -> bool;
}

impl<F: FnMut() -> bool> Checkpoint for F {
    fn is_cancelled(&mut self) -> bool {
        self()
    }
}

pub struct NoopCheckpoint;

impl Checkpoint for NoopCheckpoint {
    fn is_cancelled(&mut self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Truncation {
    Depth,
    Vertices,
    Edges,
    EdgeVisits,
    ResultBytes,
    Cancelled,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryStats {
    pub visited_vertices: usize,
    pub edge_visits: usize,
    pub returned_edges: usize,
    pub result_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResult<T> {
    pub items: Vec<T>,
    pub truncated: Vec<Truncation>,
    pub stats: QueryStats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReachedVertex {
    pub vertex_id: i64,
    pub depth: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReachedEdge {
    pub edge_id: i64,
    pub depth: u32,
}

struct Run<'a, C> {
    config: &'a QueryConfig,
    checkpoint: &'a mut C,
    stats: QueryStats,
    truncated: Vec<Truncation>,
    stopped: bool,
}

impl<'a, C: Checkpoint> Run<'a, C> {
    fn new(config: &'a QueryConfig, checkpoint: &'a mut C) -> Result<Self, GraphError> {
        config.validate()?;
        Ok(Self {
            config,
            checkpoint,
            stats: QueryStats::default(),
            truncated: Vec::new(),
            stopped: false,
        })
    }

    fn reason(&mut self, reason: Truncation) {
        if !self.truncated.contains(&reason) {
            self.truncated.push(reason);
        }
    }

    fn stop(&mut self, reason: Truncation) -> bool {
        self.reason(reason);
        self.stopped = true;
        false
    }

    fn poll(&mut self) -> bool {
        if self.checkpoint.is_cancelled() {
            return self.stop(Truncation::Cancelled);
        }
        !self.stopped
    }

    fn visit_edge(&mut self) -> bool {
        if !self.poll() {
            return false;
        }
        if self.stats.edge_visits == self.config.max_edge_visits {
            return self.stop(Truncation::EdgeVisits);
        }
        self.stats.edge_visits += 1;
        true
    }

    fn reserve(&mut self, bytes: usize) -> bool {
        if bytes > self.config.max_result_bytes - self.stats.result_bytes {
            return self.stop(Truncation::ResultBytes);
        }
        self.stats.result_bytes += bytes;
        true
    }

    fn finish<T>(self, items: Vec<T>) -> QueryResult<T> {
        QueryResult {
            items,
            truncated: self.truncated,
            stats: self.stats,
        }
    }
}

/// Seeds in `sort_by_order` order, without duplicates.
/// Limits cut them in this order, so every graph keeps the same ones.
fn ordered_ids<G: GraphRead>(graph: &mut G, ids: &[i64], limit: usize) -> Result<Vec<i64>, GraphError> {
    if ids.is_empty() || ids.len() > limit {
        return Err(GraphError::InvalidInput("seed list is empty or exceeds its configured limit"));
    }
    if let Some(&id) = ids.iter().find(|&&id| graph.vertex(id).is_none()) {
        return Err(GraphError::MissingVertex(id));
    }
    graph.sort_by_order(ids)
}

type VertexKey<'a> = (&'a str, i64);
type EdgeKey<'a> = (VertexKey<'a>, VertexKey<'a>, i64);

fn vertex_key<G: GraphRead>(graph: &G, id: i64) -> Result<VertexKey<'_>, GraphError> {
    let vertex = graph
        .vertex(id)
        .ok_or(GraphError::InconsistentGraph("adjacency refers to a missing vertex"))?;
    Ok((&vertex.sort_key, vertex.id))
}

fn edge_key<'a, G: GraphRead>(graph: &'a G, edge: &Edge) -> Result<EdgeKey<'a>, GraphError> {
    Ok((vertex_key(graph, edge.source)?, vertex_key(graph, edge.target)?, edge.id))
}

#[derive(PartialEq, Eq)]
enum Mode {
    Vertices,
    Edges,
}

#[derive(Default)]
struct Walk {
    vertices: Vec<ReachedVertex>,
    edges: Vec<ReachedEdge>,
}

fn walk<G: GraphRead, C: Checkpoint>(graph: &mut G, seeds: &[i64], mode: Mode, run: &mut Run<'_, C>) -> Result<Walk, GraphError> {
    let mut result = Walk::default();
    let mut seen = HashSet::new();
    let mut seen_edges = HashSet::new();
    let mut frontier = Vec::new();
    for &id in seeds {
        if !run.poll() {
            break;
        }
        if run.stats.visited_vertices == run.config.max_vertices {
            run.stop(Truncation::Vertices);
            break;
        }
        if mode == Mode::Vertices && !run.reserve(size_of::<ReachedVertex>()) {
            break;
        }
        seen.insert(id);
        frontier.push(id);
        result.vertices.push(ReachedVertex { vertex_id: id, depth: 0 });
        run.stats.visited_vertices += 1;
    }
    let mut depth = 0;
    while !frontier.is_empty() && !run.stopped {
        if depth == run.config.max_depth {
            if mode == Mode::Vertices {
                run.reason(Truncation::Depth);
            }
            break;
        }
        let (next, edges, vertex_cap, edge_cap) = if graph.ordered() {
            layer_ordered(graph, &frontier, &seen, &seen_edges, depth, &mode, run)?
        } else {
            layer_unordered(graph, &frontier, &seen, &seen_edges, depth, &mode, run)?
        };
        frontier.clear();
        for id in next {
            if mode == Mode::Vertices && !run.reserve(size_of::<ReachedVertex>()) {
                break;
            }
            seen.insert(id);
            frontier.push(id);
            result.vertices.push(ReachedVertex {
                vertex_id: id,
                depth: depth + 1,
            });
            run.stats.visited_vertices += 1;
        }
        for edge_id in edges {
            if !run.reserve(size_of::<ReachedEdge>()) {
                break;
            }
            seen_edges.insert(edge_id);
            result.edges.push(ReachedEdge { edge_id, depth: depth + 1 });
            run.stats.returned_edges += 1;
        }
        if vertex_cap {
            run.stop(Truncation::Vertices);
        }
        if edge_cap {
            run.stop(Truncation::Edges);
        }
        depth += 1;
    }
    Ok(result)
}

/// One layer's new vertices and its new edges, each in output order and within
/// the remaining limits, and whether a limit cut them.
type Layer = (Vec<i64>, Vec<i64>, bool, bool);

/// The layer of an ordered graph: vertices in `(sort_key, id)` order and edges
/// in `(source, target, id)` order, keeping the smallest when a limit cuts.
fn layer_ordered<G: GraphRead, C: Checkpoint>(
    graph: &mut G,
    frontier: &[i64],
    seen: &HashSet<i64>,
    seen_edges: &HashSet<i64>,
    depth: u32,
    mode: &Mode,
    run: &mut Run<'_, C>,
) -> Result<Layer, GraphError> {
    let mut next = BTreeSet::new();
    let mut edges = BTreeMap::new();
    let available = run.config.max_vertices - run.stats.visited_vertices;
    let edge_capacity = run.config.max_edges - run.stats.returned_edges;
    let (mut vertex_cap, mut edge_cap) = (false, false);
    let expansion = graph.expand(frontier, run.config.direction)?;
    let graph = expansion.graph();
    'layer: for &id in frontier {
        if !run.poll() {
            break;
        }
        for edge in expansion.incident(id)? {
            if !run.visit_edge() {
                break 'layer;
            }
            if run.config.edge_kind.as_ref().is_some_and(|kind| *kind != edge.kind) {
                continue;
            }
            let neighbor = neighbor_of(id, edge, run.config.direction)?;
            let key = vertex_key(graph, neighbor)?;
            if !seen.contains(&neighbor) && (*mode != Mode::Edges || depth + 1 < run.config.max_depth) {
                next.insert(key);
                if next.len() > available {
                    next.pop_last();
                    vertex_cap = true;
                }
            }
            if *mode == Mode::Edges && !seen_edges.contains(&edge.id) {
                edges.insert(edge_key(graph, edge)?, edge.id);
                if edges.len() > edge_capacity {
                    edges.pop_last();
                    edge_cap = true;
                }
            }
        }
    }
    Ok((
        next.into_iter().map(|(_, id)| id).collect(),
        edges.into_values().collect(),
        vertex_cap,
        edge_cap,
    ))
}

/// Relationships a cut layer ranks at a time: their candidates are sorted
/// together with the entries kept so far, then cut back to the limit.
const RANK_BATCH: usize = 8192;

/// The layer of a graph that loads sort keys on demand: the same vertices and
/// edges as `layer_ordered`. A first pass collects them in id order without
/// sort keys, holding at most one more than each limit allows. Only when a
/// limit cuts the layer does a second pass over the same relationships, not
/// charged again, rank them, `RANK_BATCH` at a time: `sort_by_order` sorts a
/// batch's vertices with those kept so far, or the ends of its edges with
/// those of the edges kept so far, and the smallest are kept as
/// `layer_ordered` keeps them. So a cut layer holds the entries it keeps and
/// one batch, never every candidate.
fn layer_unordered<G: GraphRead, C: Checkpoint>(
    graph: &mut G,
    frontier: &[i64],
    seen: &HashSet<i64>,
    seen_edges: &HashSet<i64>,
    depth: u32,
    mode: &Mode,
    run: &mut Run<'_, C>,
) -> Result<Layer, GraphError> {
    let direction = run.config.direction;
    let available = run.config.max_vertices - run.stats.visited_vertices;
    let edge_capacity = run.config.max_edges - run.stats.returned_edges;
    let expands = *mode != Mode::Edges || depth + 1 < run.config.max_depth;
    let wanted = |edge: &Edge| run.config.edge_kind.as_ref().is_none_or(|kind| *kind == edge.kind);
    let mut next: Vec<i64> = Vec::new();
    let mut in_next = HashSet::new();
    let mut edges: Vec<i64> = Vec::new();
    let mut in_layer = HashSet::new();
    let (mut vertex_cap, mut edge_cap) = (false, false);
    // Relationships examined, so the second pass reads the same ones.
    let mut examined = 0usize;
    {
        let expansion = graph.expand(frontier, direction)?;
        'layer: for &id in frontier {
            if !run.poll() {
                break;
            }
            for edge in expansion.incident(id)? {
                if !run.visit_edge() {
                    break 'layer;
                }
                examined += 1;
                if !wanted(edge) {
                    continue;
                }
                let neighbor = neighbor_of(id, edge, direction)?;
                if !vertex_cap && expands && !seen.contains(&neighbor) && in_next.insert(neighbor) {
                    next.push(neighbor);
                    if next.len() > available {
                        vertex_cap = true;
                        (next, in_next) = (Vec::new(), HashSet::new());
                    }
                }
                if !edge_cap && *mode == Mode::Edges && !seen_edges.contains(&edge.id) && in_layer.insert(edge.id) {
                    edges.push(edge.id);
                    if edges.len() > edge_capacity {
                        edge_cap = true;
                        (edges, in_layer) = (Vec::new(), HashSet::new());
                    }
                }
            }
        }
    }
    // A cut to nothing keeps nothing, so needs no keys.
    let rank_vertices = vertex_cap && available > 0;
    let rank_edges = edge_cap && edge_capacity > 0;
    if !rank_vertices && !rank_edges {
        next.sort_unstable();
        edges.sort_unstable();
        return Ok((next, edges, vertex_cap, edge_cap));
    }
    // The kept vertices, and the kept edges with their ends, each in order.
    let mut kept: Vec<i64> = Vec::new();
    let mut kept_edges: Vec<(i64, i64, i64)> = Vec::new();
    let lost = || GraphError::InconsistentGraph("sort_by_order did not return the vertices it was given");
    let mut remaining = examined;
    // The frontier vertex the next batch starts at, and the last of its edges
    // ranked, where that batch resumes. A batch spans frontier vertices, so a
    // layer of many small vertices is ranked in few batches, not one each.
    let (mut at, mut after) = (0, None);
    'frontier: while remaining > 0 && at < frontier.len() {
        if cancelled(run) {
            break;
        }
        let limit = RANK_BATCH.min(remaining);
        let mut vertices = kept.clone();
        let mut in_vertices = vertices.iter().copied().collect::<HashSet<_>>();
        let mut batch_edges = kept_edges.clone();
        let mut in_edges = batch_edges.iter().map(|&(edge, _, _)| edge).collect::<HashSet<_>>();
        let mut taken = 0;
        while taken < limit && at < frontier.len() {
            if cancelled(run) {
                break 'frontier;
            }
            let id = frontier[at];
            let before = taken;
            // Expanded already, so this loads nothing; the expansion ends
            // before `sort_by_order` takes the graph.
            let expansion = graph.expand(&[id], direction)?;
            for edge in expansion.incident_after(id, after)?.take(limit - before) {
                taken += 1;
                after = Some(edge.id);
                if !wanted(edge) {
                    continue;
                }
                let neighbor = neighbor_of(id, edge, direction)?;
                if rank_vertices && expands && !seen.contains(&neighbor) && in_vertices.insert(neighbor) {
                    vertices.push(neighbor);
                }
                if rank_edges && !seen_edges.contains(&edge.id) && in_edges.insert(edge.id) {
                    batch_edges.push((edge.id, edge.source, edge.target));
                }
            }
            // Fewer than asked for: this vertex's edges are done.
            if taken < limit {
                (at, after) = (at + 1, None);
            }
        }
        remaining -= taken;
        if vertices.len() > kept.len() {
            kept = graph.sort_by_order(&vertices)?.into_iter().take(available).collect();
        }
        if batch_edges.len() > kept_edges.len() {
            let ends = batch_edges
                .iter()
                .flat_map(|&(_, source, target)| [source, target])
                .collect::<Vec<_>>();
            let rank = graph
                .sort_by_order(&ends)?
                .into_iter()
                .enumerate()
                .map(|(position, vertex)| (vertex, position))
                .collect::<HashMap<_, _>>();
            let position = |vertex| rank.get(&vertex).copied().ok_or_else(lost);
            let mut ranked = batch_edges
                .into_iter()
                .map(|edge| Ok(((position(edge.1)?, position(edge.2)?, edge.0), edge)))
                .collect::<Result<Vec<_>, GraphError>>()?;
            ranked.sort_unstable_by_key(|&(key, _)| key);
            ranked.truncate(edge_capacity);
            kept_edges = ranked.into_iter().map(|(_, edge)| edge).collect();
        }
    }
    if vertex_cap {
        next = kept;
    } else {
        next.sort_unstable();
    }
    if edge_cap {
        edges = kept_edges.into_iter().map(|(edge, _, _)| edge).collect();
    } else {
        edges.sort_unstable();
    }
    Ok((next, edges, vertex_cap, edge_cap))
}

/// Whether the query was cancelled, recording it, for work done after a pass
/// whose limits may already have stopped the run.
fn cancelled<C: Checkpoint>(run: &mut Run<'_, C>) -> bool {
    if run.checkpoint.is_cancelled() {
        run.stop(Truncation::Cancelled);
        return true;
    }
    false
}

fn neighbor_of(id: i64, edge: &Edge, direction: Direction) -> Result<i64, GraphError> {
    if edge.source == id && direction != Direction::Incoming {
        Ok(edge.target)
    } else if edge.target == id && direction != Direction::Outgoing {
        Ok(edge.source)
    } else {
        Err(GraphError::InconsistentGraph("incident iterator returned an unrelated edge"))
    }
}

pub fn reachable<G: GraphRead, C: Checkpoint>(
    graph: &mut G,
    seeds: &[i64],
    config: &QueryConfig,
    checkpoint: &mut C,
) -> Result<QueryResult<ReachedVertex>, GraphError> {
    let mut run = Run::new(config, checkpoint)?;
    let seeds = ordered_ids(graph, seeds, config.max_sources)?;
    let result = walk(graph, &seeds, Mode::Vertices, &mut run)?;
    Ok(run.finish(result.vertices))
}

pub fn nearby_edges<G: GraphRead, C: Checkpoint>(
    graph: &mut G,
    seeds: &[i64],
    config: &QueryConfig,
    checkpoint: &mut C,
) -> Result<QueryResult<ReachedEdge>, GraphError> {
    let mut run = Run::new(config, checkpoint)?;
    let seeds = ordered_ids(graph, seeds, config.max_sources)?;
    let result = walk(graph, &seeds, Mode::Edges, &mut run)?;
    Ok(run.finish(result.edges))
}
