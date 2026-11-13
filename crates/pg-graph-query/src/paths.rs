//! Path searches: every simple path, shortest first, from sources to targets
//! or to any vertex.
//!
//! First ported from HydraDB's `src/shard/path_procedure.rs`, over a `CsrGraph`. Paths are
//! simple (no vertex repeats). The search runs over structural edges
//! `(kind, source, target)`; each structural path then expands into one path per
//! combination of parallel relationships, in edge-id order. The graph is
//! unweighted: every relationship counts 1, so shorter paths come first. Ties
//! break as in HydraDB: by vertex id, then structural edge, then relationship
//! id. `(sort_key, id)` order is not used.

use crate::Checkpoint;
use hydradb_pg_graph_core::{Direction, GraphError, GraphRead};
use hydradb_pg_graph_csr::CsrGraph;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

/// The deepest `max_depth`, as for the traversals; the work limits bound the search.
pub const MAX_PATH_LEN: u64 = 255;
/// `max_depth` when none is given: listing paths grows quickly with depth.
pub const DEFAULT_PATH_LEN: u64 = 16;
/// Caps `paths_per_pair` and `max_paths`.
pub const MAX_PATH_RESULTS: u64 = 100_000;

/// Path search options. An empty `edge_kinds` follows every kind;
/// `paths_per_pair` 0 keeps every shortest path of each pair.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PathConfig {
    pub edge_kinds: Vec<String>,
    pub direction: Option<String>,
    pub max_depth: Option<u64>,
    pub paths_per_pair: u64,
    pub max_paths: Option<u64>,
    /// With targets, one search per unordered pair: only source < target.
    pub unordered_pairs: bool,
    /// With `unordered_pairs`, shares `max_paths` across structural paths, so
    /// one with many parallel relationships does not take it all.
    pub balance_parallel_edges: bool,
    /// Bounds pruned vertices, listed paths and expanded variants.
    pub max_candidates: usize,
    /// Counted per relationship examined.
    pub max_edge_visits: u64,
    pub max_result_bytes: usize,
}

impl Default for PathConfig {
    fn default() -> Self {
        Self {
            edge_kinds: Vec::new(),
            direction: None,
            max_depth: None,
            paths_per_pair: 1,
            max_paths: None,
            unordered_pairs: false,
            balance_parallel_edges: false,
            max_candidates: 250_000,
            max_edge_visits: 1_000_000,
            max_result_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathKind {
    SinglePair,
    SingleSource,
    MultiSource,
}

/// `SinglePair` takes one source and one target, `SingleSource` one source and
/// no targets. `MultiSource` takes any sources and optional targets; ids that
/// are not in the graph are dropped.
pub struct PathQuery<'a> {
    pub kind: PathKind,
    pub sources: &'a [i64],
    pub targets: Option<&'a [i64]>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NativePath {
    pub vertices: Vec<i64>,
    /// Relationship (edge) ids, in path order.
    pub edges: Vec<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PathStats {
    pub edge_visits: u64,
    pub adjacency_vertices: usize,
    /// In-memory size of the returned paths, charged against `max_result_bytes`.
    pub result_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PathResult {
    pub items: Vec<NativePath>,
    pub stats: PathStats,
}

type Structural<'g> = (&'g str, i64, i64);
type Relationship<'g> = (&'g str, i64, i64, i64);
/// Parallel relationships of each structural edge the search kept, in edge-id
/// order, recorded as the vertex it was found at was scanned.
type Parallel<'g> = HashMap<Structural<'g>, Vec<i64>>;

struct Candidate<E> {
    nodes: Vec<i64>,
    edges: Vec<E>,
}

struct Settings<'c> {
    config: &'c PathConfig,
    kinds: BTreeSet<&'c str>,
    direction: Direction,
    max_len: usize,
    result_limit: usize,
}

impl PathConfig {
    /// Fails on the options `native_paths` would refuse for `kind`, so a
    /// caller that prepares a graph first can refuse them before that work.
    pub fn validate(&self, kind: PathKind) -> Result<(), GraphError> {
        self.settings(kind).map(|_| ())
    }

    fn settings(&self, kind: PathKind) -> Result<Settings<'_>, GraphError> {
        let direction = match self.direction.as_deref().map(str::to_ascii_lowercase).as_deref() {
            None | Some("outgoing") => Direction::Outgoing,
            Some("incoming") => Direction::Incoming,
            Some("both") => Direction::Both,
            Some(_) => return Err(GraphError::InvalidConfig("direction must be 'incoming', 'outgoing', or 'both'")),
        };
        let max_len = self.max_depth.unwrap_or(DEFAULT_PATH_LEN);
        if max_len > MAX_PATH_LEN {
            return Err(GraphError::LimitExceeded {
                operation: "path_max_depth",
                actual: max_len,
                limit: MAX_PATH_LEN,
            });
        }
        if self.max_paths == Some(0) {
            return Err(GraphError::InvalidConfig("max_paths must be greater than zero"));
        }
        if kind != PathKind::MultiSource && (self.unordered_pairs || self.balance_parallel_edges) {
            return Err(GraphError::InvalidConfig(
                "unordered_pairs and balance_parallel_edges need a multi-source search",
            ));
        }
        if self.balance_parallel_edges && !self.unordered_pairs {
            return Err(GraphError::InvalidConfig("balance_parallel_edges needs unordered_pairs"));
        }
        if self.paths_per_pair > MAX_PATH_RESULTS {
            return Err(GraphError::LimitExceeded {
                operation: "paths_per_pair",
                actual: self.paths_per_pair,
                limit: MAX_PATH_RESULTS,
            });
        }
        let result_limit = self.max_paths.unwrap_or(MAX_PATH_RESULTS);
        if result_limit > MAX_PATH_RESULTS {
            return Err(GraphError::LimitExceeded {
                operation: "max_paths",
                actual: result_limit,
                limit: MAX_PATH_RESULTS,
            });
        }
        if !(1..=1_000_000).contains(&self.max_candidates)
            || !(1..=10_000_000).contains(&self.max_edge_visits)
            || self.max_result_bytes > 64 * 1024 * 1024
        {
            return Err(GraphError::InvalidConfig(
                "max_candidates, max_edge_visits or max_result_bytes is outside its range",
            ));
        }
        Ok(Settings {
            config: self,
            kinds: self.edge_kinds.iter().map(String::as_str).collect(),
            direction,
            max_len: max_len as usize,
            result_limit: result_limit as usize,
        })
    }
}

fn check<C: Checkpoint>(checkpoint: &mut C) -> Result<(), GraphError> {
    if checkpoint.is_cancelled() {
        return Err(GraphError::Cancelled);
    }
    Ok(())
}

fn far_endpoint(current: i64, &(_, source, target): &Structural<'_>) -> Option<i64> {
    if source == current {
        Some(target)
    } else if target == current {
        Some(source)
    } else {
        None
    }
}

/// Charges one examined relationship, of any kind, against `max_edge_visits` and
/// polls for cancellation.
fn visit<C: Checkpoint>(s: &Settings<'_>, checkpoint: &mut C, stats: &mut PathStats) -> Result<(), GraphError> {
    stats.edge_visits += 1;
    if stats.edge_visits > s.config.max_edge_visits {
        return Err(GraphError::LimitExceeded {
            operation: "native_path_edges",
            actual: stats.edge_visits,
            limit: s.config.max_edge_visits,
        });
    }
    check(checkpoint)
}

/// Structural edges at `vertex` in `(kind, source, target)` order, as HydraDB's
/// per-vertex `BTreeSet<PathEdge>`. Also records each one's relationships in
/// `parallel`: `vertex`'s list in `direction` holds every relationship between
/// its two ends, so this one scan is all a path through it needs.
fn successors<'g, C: Checkpoint>(
    graph: &'g CsrGraph,
    s: &Settings<'_>,
    vertex: i64,
    direction: Direction,
    parallel: &mut Parallel<'g>,
    checkpoint: &mut C,
    stats: &mut PathStats,
) -> Result<Vec<Structural<'g>>, GraphError> {
    let mut edges = BTreeMap::<_, Vec<i64>>::new();
    for edge in graph.incident(vertex, direction) {
        visit(s, checkpoint, stats)?;
        if !s.kinds.is_empty() && !s.kinds.contains(edge.kind.as_str()) {
            continue;
        }
        let structural = (edge.kind.as_str(), edge.source, edge.target);
        edges.entry(structural).or_default().push(edge.id);
    }
    let structurals = edges.keys().copied().collect();
    for (structural, mut ids) in edges {
        // Found from either end, a structural edge has the same
        // relationships, so the first record stands.
        parallel.entry(structural).or_insert_with(|| {
            ids.sort_unstable();
            ids
        });
    }
    Ok(structurals)
}

fn vertex_limit(s: &Settings<'_>, seen: usize) -> Result<(), GraphError> {
    if seen > s.config.max_candidates {
        return Err(GraphError::LimitExceeded {
            operation: "native_path_vertices",
            actual: seen as u64,
            limit: s.config.max_candidates as u64,
        });
    }
    Ok(())
}

/// Successor lists for every path of at most `max_len` edges from a source to
/// a target, walking back from the targets, and the parallel relationships of
/// every structural edge in them.
///
/// The walk scans every vertex within `max_len - 1` hops of a target, and each
/// edge of such a path leads to one of them, so it sees every edge a path can
/// use. Lists are not restricted to vertices a source reaches: the search
/// follows them only from the sources.
#[allow(clippy::type_complexity)]
fn target_adjacency<'g, C: Checkpoint>(
    graph: &'g CsrGraph,
    s: &Settings<'_>,
    targets: &[i64],
    checkpoint: &mut C,
    stats: &mut PathStats,
) -> Result<(BTreeMap<i64, Vec<Structural<'g>>>, Parallel<'g>), GraphError> {
    let reverse = match s.direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    };
    let mut parallel = Parallel::new();
    let mut edges = BTreeSet::new();
    let mut seen = targets.iter().copied().collect::<HashSet<_>>();
    vertex_limit(s, seen.len())?;
    let mut frontier = targets.iter().copied().collect::<BTreeSet<_>>();
    for depth in 0..s.max_len {
        if frontier.is_empty() {
            break;
        }
        let mut next = BTreeSet::new();
        for vertex in frontier {
            check(checkpoint)?;
            for edge in successors(graph, s, vertex, reverse, &mut parallel, checkpoint, stats)? {
                // A successor touches the vertex it was found at.
                let Some(neighbour) = far_endpoint(vertex, &edge) else {
                    continue;
                };
                edges.insert(edge);
                // A vertex counts against max_candidates once it will be
                // expanded, as on the source side, and as it is found, so a
                // hub's layer stops at the limit; the last hop's edges are
                // kept without expanding their far ends.
                if depth + 1 < s.max_len && seen.insert(neighbour) {
                    vertex_limit(s, seen.len())?;
                    next.insert(neighbour);
                }
            }
        }
        frontier = next;
    }
    // Each edge goes to the list of the end a walk in `s.direction` leaves
    // from, in the `(kind, source, target)` order `successors` gives.
    let mut adjacency = BTreeMap::<i64, Vec<Structural<'g>>>::new();
    for edge @ (_, source, target) in edges {
        if s.direction != Direction::Incoming {
            adjacency.entry(source).or_default().push(edge);
        }
        if s.direction != Direction::Outgoing && source != target {
            adjacency.entry(target).or_default().push(edge);
        }
    }
    Ok((adjacency, parallel))
}

/// Successor lists for every vertex within `max_len - 1` hops of a source, or
/// with targets, for every path to one; and the parallel relationships of
/// every structural edge in them.
#[allow(clippy::type_complexity)]
fn path_adjacency<'g, C: Checkpoint>(
    graph: &'g CsrGraph,
    s: &Settings<'_>,
    sources: &[i64],
    targets: Option<&[i64]>,
    checkpoint: &mut C,
    stats: &mut PathStats,
) -> Result<(BTreeMap<i64, Vec<Structural<'g>>>, Parallel<'g>), GraphError> {
    if let Some(targets) = targets {
        let (adjacency, parallel) = target_adjacency(graph, s, targets, checkpoint, stats)?;
        stats.adjacency_vertices = adjacency.len();
        return Ok((adjacency, parallel));
    }
    let mut adjacency = BTreeMap::new();
    let mut parallel = Parallel::new();
    let mut seen = sources.iter().copied().collect::<BTreeSet<_>>();
    vertex_limit(s, seen.len())?;
    let mut frontier = seen.clone();
    for depth in 0..s.max_len {
        if frontier.is_empty() {
            break;
        }
        let mut next = BTreeSet::new();
        for vertex in frontier {
            check(checkpoint)?;
            let edges = successors(graph, s, vertex, s.direction, &mut parallel, checkpoint, stats)?;
            if depth + 1 < s.max_len {
                for edge in &edges {
                    let Some(neighbour) = far_endpoint(vertex, edge) else {
                        continue;
                    };
                    if seen.insert(neighbour) {
                        vertex_limit(s, seen.len())?;
                        next.insert(neighbour);
                    }
                }
            }
            adjacency.insert(vertex, edges);
        }
        frontier = next;
    }
    stats.adjacency_vertices = adjacency.len();
    Ok((adjacency, parallel))
}

fn candidate_limit(s: &Settings<'_>, operation: &'static str, actual: usize) -> GraphError {
    GraphError::LimitExceeded {
        operation,
        actual: actual as u64,
        limit: s.config.max_candidates as u64,
    }
}

/// Unweighted search for one target or none (`SingleSource`): breadth-first
/// over simple paths, so paths are found shortest first.
fn enumerate_unweighted<'g, C: Checkpoint>(
    s: &Settings<'_>,
    kind: PathKind,
    source: i64,
    target: Option<i64>,
    adjacency: &BTreeMap<i64, Vec<Structural<'g>>>,
    checkpoint: &mut C,
) -> Result<Vec<Candidate<Structural<'g>>>, GraphError> {
    let path_count = usize::try_from(s.config.paths_per_pair).unwrap_or(usize::MAX);
    let mut queue = VecDeque::from([(vec![source], Vec::<Structural<'g>>::new())]);
    let mut candidates = Vec::new();
    let mut minimum_depth = None;
    while let Some((nodes, edges)) = queue.pop_front() {
        check(checkpoint)?;
        if minimum_depth.is_some_and(|depth| edges.len() >= depth) || edges.len() >= s.max_len {
            continue;
        }
        // A queued path always holds its source.
        let Some(&current) = nodes.last() else {
            continue;
        };
        for edge in adjacency.get(&current).into_iter().flatten() {
            let Some(next) = far_endpoint(current, edge) else {
                continue;
            };
            if nodes.contains(&next) {
                continue;
            }
            let mut next_nodes = nodes.clone();
            next_nodes.push(next);
            let mut next_edges = edges.clone();
            next_edges.push(*edge);
            let at_target = target == Some(next);
            if kind == PathKind::SingleSource || at_target {
                if candidates.len() >= s.config.max_candidates {
                    return Err(candidate_limit(s, "native_path_candidates", candidates.len() + 1));
                }
                candidates.push(Candidate {
                    nodes: next_nodes.clone(),
                    edges: next_edges.clone(),
                });
                if s.config.paths_per_pair > 0 && candidates.len() >= path_count {
                    return Ok(candidates);
                }
                if s.config.paths_per_pair == 0 {
                    minimum_depth.get_or_insert(next_edges.len());
                }
            }
            if !(kind == PathKind::SinglePair && at_target) {
                queue.push_back((next_nodes, next_edges));
                if queue.len() + candidates.len() > s.config.max_candidates {
                    return Err(candidate_limit(s, "native_path_frontier_paths", queue.len() + candidates.len()));
                }
            }
        }
    }
    Ok(candidates)
}

/// Each vertex's hops to the nearest open target along the successor lists,
/// and which target that is, for vertices at most `max_len` hops away. Closing
/// a target recomputes only the vertices it was nearest to, from their
/// successors' distances, so answering many targets never rescans the whole
/// region; every recorded nearest target is open. Both walks poll for
/// cancellation.
struct Distances<'a, 'g> {
    adjacency: &'a BTreeMap<i64, Vec<Structural<'g>>>,
    reverse: HashMap<i64, Vec<i64>>,
    max_len: usize,
    hops: HashMap<i64, (usize, i64)>,
    /// The vertices each open target is nearest to.
    nearest_to: HashMap<i64, Vec<i64>>,
}

impl<'a, 'g> Distances<'a, 'g> {
    fn new<C: Checkpoint>(
        targets: &BTreeSet<i64>,
        adjacency: &'a BTreeMap<i64, Vec<Structural<'g>>>,
        max_len: usize,
        checkpoint: &mut C,
    ) -> Result<Self, GraphError> {
        let mut reverse = HashMap::<i64, Vec<i64>>::new();
        for (&vertex, edges) in adjacency {
            for edge in edges {
                if let Some(next) = far_endpoint(vertex, edge) {
                    reverse.entry(next).or_default().push(vertex);
                }
            }
        }
        let mut distances = Self {
            adjacency,
            reverse,
            max_len,
            hops: HashMap::new(),
            nearest_to: HashMap::new(),
        };
        let seeds = targets.iter().map(|&target| (target, 0, target)).collect();
        distances.settle(seeds, None, checkpoint)?;
        Ok(distances)
    }

    fn hops(&self, vertex: i64) -> Option<usize> {
        self.hops.get(&vertex).map(|&(hops, _)| hops)
    }

    /// Stops measuring to `target`: the vertices it was nearest to take the
    /// distance of their nearest successor that still has one, plus one.
    fn close<C: Checkpoint>(&mut self, target: i64, checkpoint: &mut C) -> Result<(), GraphError> {
        let affected = self
            .nearest_to
            .remove(&target)
            .unwrap_or_default()
            .into_iter()
            .collect::<HashSet<_>>();
        for vertex in &affected {
            self.hops.remove(vertex);
        }
        // One seed per affected vertex, its nearest successor's, however many
        // successors it has.
        let mut seeds = HashMap::<i64, (usize, i64)>::new();
        for &vertex in &affected {
            for edge in self.adjacency.get(&vertex).into_iter().flatten() {
                let Some(next) = far_endpoint(vertex, edge) else {
                    continue;
                };
                if let Some(&(hops, nearest)) = self.hops.get(&next) {
                    if hops < self.max_len && seeds.get(&vertex).is_none_or(|&(seeded, _)| hops + 1 < seeded) {
                        seeds.insert(vertex, (hops + 1, nearest));
                    }
                }
            }
        }
        let seeds = seeds.into_iter().map(|(vertex, (hops, nearest))| (vertex, hops, nearest)).collect();
        self.settle(seeds, Some(&affected), checkpoint)
    }

    /// Settles `seeds`, `(vertex, hops, nearest target)`, in order of hops,
    /// spreading back along `reverse` to vertices within `within`, or to any
    /// vertex when it is `None`. A vertex is queued again only at fewer hops
    /// than it already is, so the queue holds at most `max_len + 1` entries
    /// per vertex, however many edges lead to it.
    fn settle<C: Checkpoint>(
        &mut self,
        seeds: Vec<(i64, usize, i64)>,
        within: Option<&HashSet<i64>>,
        checkpoint: &mut C,
    ) -> Result<(), GraphError> {
        let mut layers = vec![Vec::new(); self.max_len + 1];
        let mut queued = HashMap::new();
        for (vertex, hops, nearest) in seeds {
            enqueue(&mut layers, &mut queued, vertex, hops, nearest);
        }
        for hops in 0..=self.max_len {
            for (vertex, nearest) in std::mem::take(&mut layers[hops]) {
                if self.hops.contains_key(&vertex) {
                    continue;
                }
                check(checkpoint)?;
                self.hops.insert(vertex, (hops, nearest));
                self.nearest_to.entry(nearest).or_default().push(vertex);
                if hops == self.max_len {
                    continue;
                }
                for &previous in self.reverse.get(&vertex).into_iter().flatten() {
                    if !self.hops.contains_key(&previous) && within.is_none_or(|within| within.contains(&previous)) {
                        enqueue(&mut layers, &mut queued, previous, hops + 1, nearest);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Queues `vertex` at `hops` unless it is already queued at no more.
fn enqueue(layers: &mut [Vec<(i64, i64)>], queued: &mut HashMap<i64, usize>, vertex: i64, hops: usize, nearest: i64) {
    if queued.get(&vertex).is_none_or(|&queued| hops < queued) {
        queued.insert(vertex, hops);
        layers[hops].push((vertex, nearest));
    }
}

/// Unweighted search from one source to many targets in one breadth-first pass.
/// A path is extended only while it can still reach, within `max_len`, a
/// target that wants more paths, so a target that is answered, or that nothing
/// reaches, does not keep the search growing paths for the others. Answering a
/// target updates only the distances it determined (`Distances`). Paths come
/// off the queue shortest first, so once `paths_per_pair` 0 has found a target's
/// shortest paths, no path queued later can reach it at that length.
fn enumerate_unweighted_to_targets<'g, C: Checkpoint>(
    s: &Settings<'_>,
    source: i64,
    targets: &[i64],
    adjacency: &BTreeMap<i64, Vec<Structural<'g>>>,
    checkpoint: &mut C,
) -> Result<BTreeMap<i64, Vec<Candidate<Structural<'g>>>>, GraphError> {
    if s.max_len == 0 || targets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let target_set = targets.iter().copied().collect::<BTreeSet<_>>();
    let mut grouped = targets.iter().map(|&target| (target, Vec::new())).collect::<BTreeMap<_, _>>();
    let mut minimum_depths = BTreeMap::<i64, usize>::new();
    let mut queue = VecDeque::from([(vec![source], Vec::<Structural<'g>>::new())]);
    let requested = usize::try_from(s.config.paths_per_pair).unwrap_or(usize::MAX);
    // Targets that want more paths, and each vertex's hops to the nearest.
    let mut open = target_set.clone();
    let mut distance = Distances::new(&open, adjacency, s.max_len, checkpoint)?;
    while let Some((nodes, edges)) = queue.pop_front() {
        check(checkpoint)?;
        if s.config.paths_per_pair == 0
            && minimum_depths.len() == target_set.len()
            && minimum_depths.values().copied().max().is_some_and(|depth| edges.len() >= depth)
        {
            break;
        }
        if edges.len() >= s.max_len {
            continue;
        }
        // A queued path always holds its source.
        let Some(&current) = nodes.last() else {
            continue;
        };
        for edge in adjacency.get(&current).into_iter().flatten() {
            let Some(next) = far_endpoint(current, edge) else {
                continue;
            };
            if nodes.contains(&next) {
                continue;
            }
            let mut next_nodes = nodes.clone();
            next_nodes.push(next);
            let mut next_edges = edges.clone();
            next_edges.push(*edge);
            if target_set.contains(&next) {
                let depth = next_edges.len();
                let candidate_count = grouped.values().map(Vec::len).sum::<usize>();
                let first = !minimum_depths.contains_key(&next);
                // Every target has a group, made with `grouped` above.
                let Some(group) = grouped.get_mut(&next) else {
                    continue;
                };
                let should_record = if s.config.paths_per_pair == 0 {
                    depth == *minimum_depths.entry(next).or_insert(depth)
                } else {
                    group.len() < requested
                };
                if should_record {
                    if candidate_count >= s.config.max_candidates {
                        return Err(candidate_limit(s, "native_path_candidates", candidate_count + 1));
                    }
                    group.push(Candidate {
                        nodes: next_nodes.clone(),
                        edges: next_edges.clone(),
                    });
                }
                let recorded = group.len();
                if s.config.paths_per_pair > 0 && grouped.values().all(|candidates| candidates.len() >= requested) {
                    return Ok(grouped);
                }
                let answered = if s.config.paths_per_pair == 0 {
                    first
                } else {
                    recorded >= requested
                };
                if answered && open.remove(&next) {
                    distance.close(next, checkpoint)?;
                }
            }
            if distance.hops(next).is_none_or(|hops| next_edges.len() + hops > s.max_len) {
                continue;
            }
            queue.push_back((next_nodes, next_edges));
            let candidate_count = grouped.values().map(Vec::len).sum::<usize>();
            if queue.len() + candidate_count > s.config.max_candidates {
                return Err(candidate_limit(s, "native_path_frontier_paths", queue.len() + candidate_count));
            }
        }
    }
    Ok(grouped)
}

/// One shortest path from `source` to each of `targets` it reaches: a
/// breadth-first walk over vertices, each visited once, so its cost grows with
/// the region rather than with the number of paths. It stops as soon as every
/// target is found, and each vertex it reaches counts against
/// `max_candidates`. Each vertex keeps the first path to it in result order:
/// `adjacency` lists each vertex's edges by neighbour, then structural edge
/// (`by_neighbour`), and the frontier is walked in that order, so the first
/// predecessor found is the one whose path sorts first, and the path to each
/// target is the first shortest path `select` would keep.
fn shortest_to_targets<'g, C: Checkpoint>(
    s: &Settings<'_>,
    source: i64,
    targets: &[i64],
    adjacency: &BTreeMap<i64, Vec<Structural<'g>>>,
    checkpoint: &mut C,
) -> Result<HashMap<i64, Candidate<Structural<'g>>>, GraphError> {
    let mut remaining = targets.iter().copied().collect::<HashSet<_>>();
    let mut parents = HashMap::<i64, (i64, Structural<'g>)>::new();
    let mut frontier = vec![source];
    'walk: for _ in 0..s.max_len {
        if frontier.is_empty() || remaining.is_empty() {
            break;
        }
        let mut next = Vec::new();
        for vertex in frontier {
            check(checkpoint)?;
            for edge in adjacency.get(&vertex).into_iter().flatten() {
                let Some(neighbour) = far_endpoint(vertex, edge) else {
                    continue;
                };
                if neighbour == source || parents.contains_key(&neighbour) {
                    continue;
                }
                // The source and every vertex reached so far.
                vertex_limit(s, parents.len() + 2)?;
                parents.insert(neighbour, (vertex, *edge));
                next.push(neighbour);
                if remaining.remove(&neighbour) && remaining.is_empty() {
                    break 'walk;
                }
            }
        }
        frontier = next;
    }
    let mut paths = HashMap::new();
    for &target in targets {
        check(checkpoint)?;
        if !parents.contains_key(&target) {
            continue;
        }
        let (mut nodes, mut edges) = (vec![target], Vec::new());
        let mut current = target;
        while current != source {
            check(checkpoint)?;
            let &(parent, edge) = parents
                .get(&current)
                .ok_or(GraphError::InconsistentGraph("missing breadth-first predecessor"))?;
            edges.push(edge);
            nodes.push(parent);
            current = parent;
        }
        nodes.reverse();
        edges.reverse();
        paths.insert(target, Candidate { nodes, edges });
    }
    Ok(paths)
}

/// Reorders each successor list from `(kind, source, target)` to neighbour,
/// then structural edge, the order a path compares its steps in. Once per
/// query, before any source's walk, so sources that pass through the same hub
/// share one sort of its list.
fn by_neighbour<C: Checkpoint>(adjacency: &mut BTreeMap<i64, Vec<Structural<'_>>>, checkpoint: &mut C) -> Result<(), GraphError> {
    for (&vertex, edges) in adjacency.iter_mut() {
        check(checkpoint)?;
        edges.sort_unstable_by_key(|edge| (far_endpoint(vertex, edge), *edge));
    }
    Ok(())
}

/// A structural path as one path, through the lowest-id relationship of each
/// structural edge.
fn first_variant(candidate: Candidate<Structural<'_>>, parallel: &Parallel<'_>) -> Result<NativePath, GraphError> {
    let edges = candidate
        .edges
        .iter()
        .map(|structural| {
            parallel
                .get(structural)
                .and_then(|ids| ids.first())
                .copied()
                .ok_or(GraphError::InconsistentGraph("structural edge without a relationship"))
        })
        .collect::<Result<_, _>>()?;
    Ok(NativePath {
        vertices: candidate.nodes,
        edges,
    })
}

/// The targets searched from `source`, in order. A source that is also a
/// target keeps it, reached by the path of no edges, its only simple path;
/// with `unordered_pairs` a pair counts once, from its smaller end.
fn eligible_targets(config: &PathConfig, source: i64, targets: &[i64]) -> Vec<i64> {
    targets
        .iter()
        .copied()
        .filter(|&target| !config.unordered_pairs || source < target)
        .collect()
}

/// The path of no edges from `source` to `target` when they are one vertex of
/// the graph; a search may name an id the graph does not have.
fn itself<'g>(graph: &CsrGraph, source: i64, target: i64) -> Option<Candidate<Structural<'g>>> {
    (source == target && graph.vertex(source).is_some()).then(|| Candidate {
        nodes: vec![source],
        edges: Vec::new(),
    })
}

fn enumerate_candidate_paths<'g, C: Checkpoint>(
    s: &Settings<'_>,
    source: i64,
    target: Option<i64>,
    adjacency: &BTreeMap<i64, Vec<Structural<'g>>>,
    checkpoint: &mut C,
) -> Result<Vec<Candidate<Structural<'g>>>, GraphError> {
    let kind = if target.is_some() {
        PathKind::SinglePair
    } else {
        PathKind::SingleSource
    };
    if s.max_len == 0 {
        return Ok(Vec::new());
    }
    enumerate_unweighted(s, kind, source, target, adjacency, checkpoint)
}

/// One candidate per combination of parallel relationships. Over
/// `max_candidates` is an error, or a cut when `truncate_at_limit` is set.
fn expand_relationships<'g, C: Checkpoint>(
    candidates: Vec<Candidate<Structural<'g>>>,
    parallel: &Parallel<'g>,
    max_candidates: usize,
    truncate_at_limit: bool,
    checkpoint: &mut C,
) -> Result<Vec<Candidate<Relationship<'g>>>, GraphError> {
    let limit = |actual: usize| GraphError::LimitExceeded {
        operation: "native_path_parallel_candidates",
        actual: actual as u64,
        limit: max_candidates as u64,
    };
    let mut expanded = Vec::new();
    for candidate in candidates {
        check(checkpoint)?;
        let mut variants = vec![Vec::with_capacity(candidate.edges.len())];
        for &structural in &candidate.edges {
            let ids = parallel.get(&structural).map_or(&[][..], Vec::as_slice);
            let product = variants.len().saturating_mul(ids.len());
            if !truncate_at_limit && expanded.len().saturating_add(product) > max_candidates {
                return Err(limit(expanded.len().saturating_add(product)));
            }
            let mut next = Vec::with_capacity(product.min(max_candidates));
            'variants: for variant in variants {
                for &id in ids {
                    if next.len() >= max_candidates {
                        break 'variants;
                    }
                    let mut edges: Vec<Relationship<'g>> = variant.clone();
                    edges.push((structural.0, structural.1, structural.2, id));
                    next.push(edges);
                }
            }
            variants = next;
        }
        for edges in variants {
            if expanded.len() >= max_candidates {
                if truncate_at_limit {
                    break;
                }
                return Err(limit(expanded.len() + 1));
            }
            expanded.push(Candidate {
                nodes: candidate.nodes.clone(),
                edges,
            });
        }
    }
    Ok(expanded)
}

/// Shortest paths first; `paths_per_pair` 0 keeps every shortest path.
fn select(s: &Settings<'_>, candidates: &mut Vec<Candidate<Relationship<'_>>>) {
    candidates.sort_by(|left, right| {
        left.edges
            .len()
            .cmp(&right.edges.len())
            .then_with(|| left.nodes.cmp(&right.nodes))
            .then_with(|| left.edges.cmp(&right.edges))
    });
    if s.config.balance_parallel_edges {
        return;
    }
    if s.config.paths_per_pair == 0 {
        if let Some(length) = candidates.first().map(|candidate| candidate.edges.len()) {
            candidates.retain(|candidate| candidate.edges.len() == length);
        }
    } else {
        candidates.truncate(usize::try_from(s.config.paths_per_pair).unwrap_or(usize::MAX));
    }
}

/// Takes one variant from each structural path in turn until `limit`.
fn fair_round_robin<E>(groups: Vec<Vec<Candidate<E>>>, limit: usize) -> Vec<Candidate<E>> {
    let mut groups = groups.into_iter().map(VecDeque::from).collect::<Vec<_>>();
    let mut selected = Vec::with_capacity(limit);
    while selected.len() < limit {
        let mut advanced = false;
        for group in &mut groups {
            let Some(candidate) = group.pop_front() else {
                continue;
            };
            selected.push(candidate);
            advanced = true;
            if selected.len() >= limit {
                break;
            }
        }
        if !advanced {
            break;
        }
    }
    selected
}

fn native_path(candidate: Candidate<Relationship<'_>>) -> NativePath {
    NativePath {
        vertices: candidate.nodes,
        edges: candidate.edges.into_iter().map(|edge| edge.3).collect(),
    }
}

/// Adds a path to the result, charging its in-memory size against
/// `max_result_bytes` as the traversal kernels charge theirs. Over the budget
/// is an error, like the procedures' other limits.
fn push_path(s: &Settings<'_>, items: &mut Vec<NativePath>, stats: &mut PathStats, path: NativePath) -> Result<(), GraphError> {
    stats.result_bytes += size_of::<NativePath>() + (path.vertices.len() + path.edges.len()) * size_of::<i64>();
    if stats.result_bytes > s.config.max_result_bytes {
        return Err(GraphError::LimitExceeded {
            operation: "native_path_result_bytes",
            actual: stats.result_bytes as u64,
            limit: s.config.max_result_bytes as u64,
        });
    }
    items.push(path);
    Ok(())
}

/// Runs a path procedure over a CSR. The search reads incident edges without
/// loading them, so it takes a graph that holds every vertex it can reach; a
/// caller that reads adjacency on demand loads the region first and builds a
/// `CsrGraph` of it.
pub fn native_paths<C: Checkpoint>(
    graph: &CsrGraph,
    query: &PathQuery<'_>,
    config: &PathConfig,
    checkpoint: &mut C,
) -> Result<PathResult, GraphError> {
    let s = config.settings(query.kind)?;
    check(checkpoint)?;
    let (sources, targets) = match query.kind {
        PathKind::SinglePair => match (query.sources, query.targets) {
            (&[source], Some(&[target])) => (vec![source], Some(vec![target])),
            _ => return Err(GraphError::InvalidInput("a single-pair search takes one source and one target")),
        },
        PathKind::SingleSource => match (query.sources, query.targets) {
            (&[source], None) => (vec![source], None),
            _ => return Err(GraphError::InvalidInput("a single-source search takes one source and no target")),
        },
        PathKind::MultiSource => {
            if config.unordered_pairs && query.targets.is_none() {
                return Err(GraphError::InvalidConfig("unordered_pairs needs targets"));
            }
            let existing = |ids: &[i64]| {
                ids.iter()
                    .copied()
                    .filter(|&id| graph.vertex(id).is_some())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
            };
            (existing(query.sources), query.targets.map(existing))
        }
    };
    let mut stats = PathStats::default();
    // No source starts a path, as when MSpaths names none the graph has.
    if sources.is_empty() {
        return Ok(PathResult { items: Vec::new(), stats });
    }
    let (mut adjacency, parallel) = path_adjacency(graph, &s, &sources, targets.as_deref(), checkpoint, &mut stats)?;
    // One path per pair needs only a shortest one: a walk over vertices finds
    // it without listing paths. The listing reads the lists in their own order.
    let one_path = targets.is_some() && config.paths_per_pair == 1 && !config.balance_parallel_edges;
    if one_path {
        by_neighbour(&mut adjacency, checkpoint)?;
    }
    let mut items = Vec::new();
    let mut structures = Vec::new();
    'sources: for &source in &sources {
        if let Some(targets) = targets.as_ref().filter(|_| one_path) {
            let eligible = eligible_targets(config, source, targets);
            let others = eligible.iter().copied().filter(|&target| target != source).collect::<Vec<_>>();
            let mut found = shortest_to_targets(&s, source, &others, &adjacency, checkpoint)?;
            for target in eligible {
                check(checkpoint)?;
                let Some(candidate) = found.remove(&target).or_else(|| itself(graph, source, target)) else {
                    continue;
                };
                push_path(&s, &mut items, &mut stats, first_variant(candidate, &parallel)?)?;
                if items.len() >= s.result_limit {
                    break 'sources;
                }
            }
            continue;
        }
        let groups = if let Some(targets) = targets.as_ref() {
            let eligible = eligible_targets(config, source, targets);
            let others = eligible.iter().copied().filter(|&target| target != source).collect::<Vec<_>>();
            let mut grouped = enumerate_unweighted_to_targets(&s, source, &others, &adjacency, checkpoint)?;
            eligible
                .into_iter()
                .map(|target| itself(graph, source, target).map_or_else(|| grouped.remove(&target).unwrap_or_default(), |path| vec![path]))
                .collect::<Vec<_>>()
        } else {
            targets
                .as_ref()
                .map(|targets| targets.iter().copied().map(Some).collect::<Vec<_>>())
                .unwrap_or_else(|| vec![None])
                .into_iter()
                .filter(|&target| target != Some(source) && (!config.unordered_pairs || target.is_none_or(|target| source < target)))
                .map(|target| enumerate_candidate_paths(&s, source, target, &adjacency, checkpoint))
                .collect::<Result<Vec<_>, _>>()?
        };
        for candidates in groups {
            if config.balance_parallel_edges {
                for structural in candidates {
                    structures.push(structural);
                    if structures.len() >= s.result_limit {
                        break 'sources;
                    }
                }
                continue;
            }
            let mut expanded = expand_relationships(candidates, &parallel, config.max_candidates, false, checkpoint)?;
            select(&s, &mut expanded);
            expanded.truncate(s.result_limit.saturating_sub(items.len()));
            for candidate in expanded {
                push_path(&s, &mut items, &mut stats, native_path(candidate))?;
                if items.len() >= s.result_limit {
                    break 'sources;
                }
            }
        }
    }
    if config.balance_parallel_edges {
        let count = structures.len();
        let base = s.result_limit.checked_div(count).unwrap_or(0);
        let remainder = s.result_limit.checked_rem(count).unwrap_or(0);
        let mut groups = Vec::with_capacity(count);
        for (index, structural) in structures.into_iter().enumerate() {
            let quota = base + usize::from(index < remainder);
            let mut variants = expand_relationships(vec![structural], &parallel, quota, true, checkpoint)?;
            select(&s, &mut variants);
            if !variants.is_empty() {
                groups.push(variants);
            }
        }
        for candidate in fair_round_robin(groups, s.result_limit) {
            push_path(&s, &mut items, &mut stats, native_path(candidate))?;
        }
    }
    Ok(PathResult { items, stats })
}
