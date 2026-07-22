use std::collections::{BTreeMap, BTreeSet};

use crate::{Result, VertexId};

pub(crate) type Adjacency = BTreeMap<VertexId, BTreeSet<VertexId>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphBlasCsc {
    pub vertices: Vec<VertexId>,
    pub pointers: Vec<u64>,
    pub indices: Vec<u64>,
}

impl GraphBlasCsc {
    pub(crate) fn to_adjacency(&self) -> Result<Adjacency> {
        if self.pointers.len() != self.vertices.len().saturating_add(1) {
            return Err(crate::GraphError::SparseKernel {
                backend: "CanonicalAdjacency",
                reason: "pointer count does not match the vertex dictionary".to_string(),
            });
        }
        let mut adjacency = Adjacency::new();
        for (source_ordinal, source) in self.vertices.iter().copied().enumerate() {
            let start = usize::try_from(self.pointers[source_ordinal]).map_err(|err| {
                crate::GraphError::SparseKernel {
                    backend: "CanonicalAdjacency",
                    reason: format!("source pointer does not fit usize: {err}"),
                }
            })?;
            let end = usize::try_from(self.pointers[source_ordinal + 1]).map_err(|err| {
                crate::GraphError::SparseKernel {
                    backend: "CanonicalAdjacency",
                    reason: format!("source pointer does not fit usize: {err}"),
                }
            })?;
            let row =
                self.indices
                    .get(start..end)
                    .ok_or_else(|| crate::GraphError::SparseKernel {
                        backend: "CanonicalAdjacency",
                        reason: format!("source row {source_ordinal} exceeds the index array"),
                    })?;
            if row.is_empty() {
                continue;
            }
            let mut neighbors = BTreeSet::new();
            for destination in row {
                let destination = usize::try_from(*destination).map_err(|err| {
                    crate::GraphError::SparseKernel {
                        backend: "CanonicalAdjacency",
                        reason: format!("destination ordinal does not fit usize: {err}"),
                    }
                })?;
                let vertex = self.vertices.get(destination).copied().ok_or_else(|| {
                    crate::GraphError::SparseKernel {
                        backend: "CanonicalAdjacency",
                        reason: format!("destination ordinal {destination} is out of bounds"),
                    }
                })?;
                neighbors.insert(vertex);
            }
            adjacency.insert(source, neighbors);
        }
        Ok(adjacency)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SparseKernelBackend {
    RustSparse,
    SuiteSparseGraphBlas,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SparseTraversal {
    pub vertices: Vec<VertexId>,
    pub edge_visits: u64,
    pub backend: SparseKernelBackend,
}

#[cfg(all(feature = "graphblas", feature = "opencypher"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SparseTraversalCount {
    pub vertices: u64,
    pub edge_visits: u64,
    pub backend: SparseKernelBackend,
}

pub(crate) fn default_matrix_kernel() -> SparseKernelBackend {
    if cfg!(feature = "graphblas") {
        SparseKernelBackend::SuiteSparseGraphBlas
    } else {
        SparseKernelBackend::RustSparse
    }
}

pub(crate) fn expand(
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
    backend: SparseKernelBackend,
) -> Result<SparseTraversal> {
    match backend {
        SparseKernelBackend::RustSparse => Ok(expand_rust(adjacency, starts, hops)),
        SparseKernelBackend::SuiteSparseGraphBlas => expand_graphblas(adjacency, starts, hops),
    }
}

#[cfg(all(test, feature = "graphblas"))]
pub(crate) fn expand_range(
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
    backend: SparseKernelBackend,
) -> Result<SparseTraversal> {
    match backend {
        SparseKernelBackend::RustSparse => {
            Ok(expand_range_rust(adjacency, starts, min_hops, max_hops))
        }
        SparseKernelBackend::SuiteSparseGraphBlas => {
            expand_range_graphblas(adjacency, starts, min_hops, max_hops)
        }
    }
}

#[cfg(feature = "graphblas")]
pub(crate) use graphblas::CompiledGraphBlasMatrix;

#[cfg(not(feature = "graphblas"))]
pub(crate) struct CompiledGraphBlasMatrix;

#[cfg(feature = "graphblas")]
pub(crate) fn compile_graphblas_matrix(adjacency: &Adjacency) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas(adjacency)
}

#[cfg(feature = "graphblas")]
pub(crate) fn compile_graphblas_csc(csc: &GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas_from_csc(csc)
}

#[cfg(feature = "graphblas")]
pub(crate) fn compile_graphblas_csc_owned(csc: GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas_from_csc_owned(csc)
}

#[cfg(feature = "graphblas")]
pub(crate) fn compact_csc_kernel_enabled() -> bool {
    graphblas::use_compact_csc_kernel()
}

#[cfg(feature = "graphblas")]
pub(crate) fn compile_graphblas_compact_csc_u32(
    vertices: Vec<VertexId>,
    pointers: Vec<u32>,
    indices: Vec<u32>,
) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new_from_compact_u32(vertices, pointers, indices)
}

pub(crate) fn graphblas_csc_from_adjacency(adjacency: &Adjacency) -> Result<GraphBlasCsc> {
    let vertices = graphblas_vertices_from_adjacency(adjacency)?;

    let mut pointers = vec![0_u64; vertices.len() + 1];
    for (src, dsts) in adjacency {
        let src_ordinal = csc_vertex_ordinal(&vertices, *src, "source")?;
        pointers[src_ordinal as usize + 1] += dsts.len() as u64;
    }
    for idx in 1..pointers.len() {
        pointers[idx] += pointers[idx - 1];
    }

    let mut indices = vec![0_u64; pointers.last().copied().unwrap_or(0) as usize];
    let mut offsets = pointers[..vertices.len()].to_vec();
    for (src, dsts) in adjacency {
        let src_ordinal = csc_vertex_ordinal(&vertices, *src, "source")?;
        let offset = &mut offsets[src_ordinal as usize];
        for dst in dsts {
            let dst_ordinal = csc_vertex_ordinal(&vertices, *dst, "destination")?;
            indices[*offset as usize] = dst_ordinal;
            *offset += 1;
        }
    }

    Ok(GraphBlasCsc {
        vertices,
        pointers,
        indices,
    })
}

fn csc_vertex_ordinal(vertices: &[VertexId], vertex: VertexId, role: &str) -> Result<u64> {
    vertices
        .binary_search(&vertex)
        .map(|idx| idx as u64)
        .map_err(|_| crate::GraphError::SparseKernel {
            backend: "SuiteSparseGraphBlas",
            reason: format!("missing CSC ordinal for {role} vertex {vertex}"),
        })
}

#[cfg(feature = "graphblas")]
pub(crate) fn expand_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
) -> Result<SparseTraversal> {
    expand_graphblas_compiled(compiled, adjacency, starts, hops)
}

#[cfg(feature = "graphblas")]
pub(crate) fn compiled_graphblas_contains_edge(
    compiled: &CompiledGraphBlasMatrix,
    src: VertexId,
    dst: VertexId,
) -> bool {
    compiled.contains_edge(src, dst)
}

#[cfg(feature = "graphblas")]
pub(crate) fn expand_range_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversal> {
    expand_range_graphblas_compiled(compiled, adjacency, starts, min_hops, max_hops)
}

#[cfg(all(feature = "graphblas", feature = "opencypher"))]
pub(crate) fn expand_range_count_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversalCount> {
    expand_range_count_graphblas_compiled(compiled, adjacency, starts, min_hops, max_hops)
}

#[cfg(all(feature = "graphblas", feature = "opencypher"))]
pub(crate) fn expand_range_window_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
    window: SparseTraversalWindow,
) -> Result<SparseTraversal> {
    expand_range_window_graphblas_compiled(compiled, adjacency, starts, min_hops, max_hops, window)
}

#[cfg(all(feature = "graphblas", feature = "opencypher"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SparseTraversalWindow {
    pub skip: u64,
    pub limit: Option<usize>,
    pub ascending: bool,
}

fn expand_rust(adjacency: &Adjacency, starts: &[VertexId], hops: u8) -> SparseTraversal {
    let start_set: BTreeSet<_> = starts.iter().copied().collect();
    let mut frontier = start_set.clone();
    let mut seen = start_set.clone();
    let mut edge_visits = 0_u64;
    for _ in 0..hops {
        let mut next = BTreeSet::new();
        for src in &frontier {
            if let Some(neighbors) = adjacency.get(src) {
                edge_visits += neighbors.len() as u64;
                for dst in neighbors {
                    if seen.insert(*dst) {
                        next.insert(*dst);
                    }
                }
            }
        }
        frontier = next;
        if frontier.is_empty() {
            break;
        }
    }
    SparseTraversal {
        vertices: seen
            .into_iter()
            .filter(|vertex| !start_set.contains(vertex))
            .collect(),
        edge_visits,
        backend: SparseKernelBackend::RustSparse,
    }
}

#[cfg(all(test, feature = "graphblas"))]
fn expand_range_rust(
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> SparseTraversal {
    let mut frontier: BTreeSet<_> = starts.iter().copied().collect();
    let mut reachable = BTreeSet::new();
    let mut edge_visits = 0_u64;
    if min_hops == 0 {
        reachable.extend(frontier.iter().copied());
    }
    if max_hops == 0 || frontier.is_empty() {
        return SparseTraversal {
            vertices: reachable.into_iter().collect(),
            edge_visits,
            backend: SparseKernelBackend::RustSparse,
        };
    }
    for depth in 1..=max_hops {
        let mut next = BTreeSet::new();
        for src in &frontier {
            if let Some(neighbors) = adjacency.get(src) {
                edge_visits = edge_visits.saturating_add(neighbors.len() as u64);
                next.extend(neighbors.iter().copied());
            }
        }
        if depth >= min_hops {
            reachable.extend(next.iter().copied());
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    SparseTraversal {
        vertices: reachable.into_iter().collect(),
        edge_visits,
        backend: SparseKernelBackend::RustSparse,
    }
}

fn graphblas_vertices_from_adjacency(adjacency: &Adjacency) -> Result<Vec<VertexId>> {
    let mut vertices = BTreeSet::new();
    for (src, dsts) in adjacency {
        vertices.insert(*src);
        vertices.extend(dsts.iter().copied());
    }
    if vertices.len() as u64 > GRAPHBLAS_INDEX_MAX {
        return Err(crate::GraphError::SparseKernel {
            backend: "SuiteSparseGraphBlas",
            reason: format!(
                "{} local vertices exceed GraphBLAS index limit",
                vertices.len()
            ),
        });
    }
    Ok(vertices.into_iter().collect())
}

const GRAPHBLAS_INDEX_MAX: u64 = (1_u64 << 60) - 1;

#[cfg(not(feature = "graphblas"))]
fn expand_graphblas(
    _adjacency: &Adjacency,
    _starts: &[VertexId],
    _hops: u8,
) -> Result<SparseTraversal> {
    Err(crate::GraphError::SparseKernel {
        backend: "SuiteSparseGraphBlas",
        reason: "crate was built without the graphblas feature".to_string(),
    })
}

#[cfg(not(feature = "graphblas"))]
#[allow(dead_code)]
fn compile_graphblas_from_csc(_csc: &GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    Err(crate::GraphError::SparseKernel {
        backend: "SuiteSparseGraphBlas",
        reason: "crate was built without the graphblas feature".to_string(),
    })
}

#[cfg(feature = "graphblas")]
fn expand_graphblas(
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
) -> Result<SparseTraversal> {
    graphblas::expand(adjacency, starts, hops)
}

#[cfg(all(test, feature = "graphblas"))]
fn expand_range_graphblas(
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversal> {
    graphblas::expand_range(adjacency, starts, min_hops, max_hops)
}

#[cfg(feature = "graphblas")]
fn compile_graphblas(adjacency: &Adjacency) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new(adjacency)
}

#[cfg(feature = "graphblas")]
fn compile_graphblas_from_csc(csc: &GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new_from_csc(csc)
}

#[cfg(feature = "graphblas")]
fn compile_graphblas_from_csc_owned(csc: GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new_from_csc_owned(csc)
}

#[cfg(feature = "graphblas")]
fn expand_graphblas_compiled(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
) -> Result<SparseTraversal> {
    compiled.expand(adjacency, starts, hops)
}

#[cfg(feature = "graphblas")]
fn expand_range_graphblas_compiled(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversal> {
    compiled.expand_range(adjacency, starts, min_hops, max_hops)
}

#[cfg(all(feature = "graphblas", feature = "opencypher"))]
fn expand_range_count_graphblas_compiled(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversalCount> {
    compiled.expand_range_count(adjacency, starts, min_hops, max_hops)
}

#[cfg(all(feature = "graphblas", feature = "opencypher"))]
fn expand_range_window_graphblas_compiled(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
    window: SparseTraversalWindow,
) -> Result<SparseTraversal> {
    compiled.expand_range_window(adjacency, starts, min_hops, max_hops, window)
}

#[cfg(feature = "graphblas")]
mod graphblas;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "graphblas")]
    static COMPACT_KERNEL_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn test_adjacency() -> Adjacency {
        BTreeMap::from([
            (1, BTreeSet::from([2, 3])),
            (2, BTreeSet::from([4])),
            (3, BTreeSet::from([4])),
            (4, BTreeSet::from([5, 6])),
            (42, BTreeSet::from([10, 11, 12, 13, 14, 15, 16])),
        ])
    }

    #[test]
    fn rust_sparse_kernel_expands_reachable_vertices() {
        let result = expand(
            &test_adjacency(),
            &[1, 42],
            3,
            SparseKernelBackend::RustSparse,
        )
        .expect("rust sparse expansion should succeed");
        assert_eq!(result.backend, SparseKernelBackend::RustSparse);
        assert_eq!(
            result.vertices,
            vec![2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[cfg(feature = "graphblas")]
    #[test]
    fn graphblas_kernel_matches_rust_sparse_kernel() {
        let _guard = COMPACT_KERNEL_ENV_LOCK.lock().expect("env lock poisoned");
        let adjacency = test_adjacency();
        let rust = expand(&adjacency, &[1, 42], 3, SparseKernelBackend::RustSparse)
            .expect("rust sparse expansion should succeed");
        let graphblas = expand(
            &adjacency,
            &[1, 42],
            3,
            SparseKernelBackend::SuiteSparseGraphBlas,
        )
        .expect("GraphBLAS expansion should succeed");
        assert_eq!(graphblas.backend, SparseKernelBackend::SuiteSparseGraphBlas);
        assert_eq!(graphblas.vertices, rust.vertices);
        assert_eq!(graphblas.edge_visits, rust.edge_visits);
    }

    #[cfg(all(feature = "graphblas", feature = "opencypher"))]
    #[test]
    fn graphblas_exact_hop_count_matches_materialized_range() {
        let _guard = COMPACT_KERNEL_ENV_LOCK.lock().expect("env lock poisoned");
        let adjacency = test_adjacency();
        let compiled = compile_graphblas_matrix(&adjacency).expect("matrix should compile");
        for hops in 0..=4 {
            let materialized =
                expand_range_compiled_graphblas(&compiled, &adjacency, &[1], hops, hops)
                    .expect("range expansion should succeed");
            let counted =
                expand_range_count_compiled_graphblas(&compiled, &adjacency, &[1], hops, hops)
                    .expect("count expansion should succeed");
            assert_eq!(counted.vertices, materialized.vertices.len() as u64);
            assert_eq!(counted.edge_visits, materialized.edge_visits);
        }
    }

    #[cfg(all(feature = "graphblas", feature = "opencypher"))]
    #[test]
    fn graphblas_exact_hop_count_preserves_materialized_cycle_semantics() {
        let _guard = COMPACT_KERNEL_ENV_LOCK.lock().expect("env lock poisoned");
        let adjacency = BTreeMap::from([(1, BTreeSet::from([2])), (2, BTreeSet::from([1, 3]))]);
        let compiled = compile_graphblas_matrix(&adjacency).expect("matrix should compile");
        for hops in 0..=4 {
            let materialized =
                expand_range_compiled_graphblas(&compiled, &adjacency, &[1], hops, hops)
                    .expect("cyclic range expansion should succeed");
            let counted =
                expand_range_count_compiled_graphblas(&compiled, &adjacency, &[1], hops, hops)
                    .expect("cyclic count expansion should succeed");
            assert_eq!(counted.vertices, materialized.vertices.len() as u64);
            assert_eq!(counted.edge_visits, materialized.edge_visits);
        }
    }

    #[cfg(all(feature = "graphblas", feature = "opencypher"))]
    #[test]
    fn graphblas_exact_hop_count_clears_reused_scratch() {
        let _guard = COMPACT_KERNEL_ENV_LOCK.lock().expect("env lock poisoned");
        let previous_kernel = std::env::var("GRAPH_COMPILED_KERNEL").ok();
        let previous_replicas = std::env::var("GRAPHBLAS_REPLICAS").ok();
        std::env::remove_var("GRAPH_COMPILED_KERNEL");
        std::env::set_var("GRAPHBLAS_REPLICAS", "1");

        let adjacency = test_adjacency();
        let compiled = compile_graphblas_matrix(&adjacency).expect("matrix should compile");
        let cases = [
            (vec![1], 1),
            (vec![42], 1),
            (vec![1], 3),
            (vec![999], 2),
            (vec![1, 1, 42], 0),
            (vec![2, 3], 2),
            (vec![1], 4),
        ];
        for _ in 0..3 {
            for (starts, hops) in &cases {
                let materialized =
                    expand_range_compiled_graphblas(&compiled, &adjacency, starts, *hops, *hops)
                        .expect("range expansion should succeed");
                let counted = expand_range_count_compiled_graphblas(
                    &compiled, &adjacency, starts, *hops, *hops,
                )
                .expect("count expansion should succeed");
                assert_eq!(counted.vertices, materialized.vertices.len() as u64);
                assert_eq!(counted.edge_visits, materialized.edge_visits);
            }
        }

        match previous_kernel {
            Some(value) => std::env::set_var("GRAPH_COMPILED_KERNEL", value),
            None => std::env::remove_var("GRAPH_COMPILED_KERNEL"),
        }
        match previous_replicas {
            Some(value) => std::env::set_var("GRAPHBLAS_REPLICAS", value),
            None => std::env::remove_var("GRAPHBLAS_REPLICAS"),
        }
    }

    #[cfg(all(feature = "graphblas", feature = "opencypher"))]
    #[test]
    fn graphblas_exact_hop_count_is_safe_across_replicas() {
        let _guard = COMPACT_KERNEL_ENV_LOCK.lock().expect("env lock poisoned");
        let previous_kernel = std::env::var("GRAPH_COMPILED_KERNEL").ok();
        let previous_replicas = std::env::var("GRAPHBLAS_REPLICAS").ok();
        std::env::remove_var("GRAPH_COMPILED_KERNEL");
        std::env::set_var("GRAPHBLAS_REPLICAS", "2");

        let adjacency = test_adjacency();
        let compiled = std::sync::Arc::new(
            compile_graphblas_matrix(&adjacency).expect("matrix should compile"),
        );
        assert_eq!(compiled.replica_count(), 2);
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let compiled = std::sync::Arc::clone(&compiled);
                let adjacency = &adjacency;
                scope.spawn(move || {
                    for iteration in 0..25 {
                        let starts = if (worker + iteration) % 2 == 0 {
                            vec![1]
                        } else {
                            vec![42]
                        };
                        let hops = ((worker + iteration) % 4) as u8;
                        let materialized = expand_range_compiled_graphblas(
                            &compiled, adjacency, &starts, hops, hops,
                        )
                        .expect("range expansion should succeed");
                        let counted = expand_range_count_compiled_graphblas(
                            &compiled, adjacency, &starts, hops, hops,
                        )
                        .expect("count expansion should succeed");
                        assert_eq!(counted.vertices, materialized.vertices.len() as u64);
                        assert_eq!(counted.edge_visits, materialized.edge_visits);
                    }
                });
            }
        });

        match previous_kernel {
            Some(value) => std::env::set_var("GRAPH_COMPILED_KERNEL", value),
            None => std::env::remove_var("GRAPH_COMPILED_KERNEL"),
        }
        match previous_replicas {
            Some(value) => std::env::set_var("GRAPHBLAS_REPLICAS", value),
            None => std::env::remove_var("GRAPHBLAS_REPLICAS"),
        }
    }

    #[cfg(feature = "graphblas")]
    #[test]
    fn compact_csc_kernel_matches_rust_sparse_kernel() {
        let _guard = COMPACT_KERNEL_ENV_LOCK.lock().expect("env lock poisoned");
        let previous = std::env::var("GRAPH_COMPILED_KERNEL").ok();
        std::env::set_var("GRAPH_COMPILED_KERNEL", "compact");

        let adjacency = test_adjacency();
        let rust = expand_range(&adjacency, &[1, 42], 1, 3, SparseKernelBackend::RustSparse)
            .expect("rust sparse expansion should succeed");
        let csc = graphblas_csc_from_adjacency(&adjacency).expect("CSC build should succeed");
        let compiled =
            compile_graphblas_csc_owned(csc).expect("compact CSC compile should succeed");
        let compact = expand_range_compiled_graphblas(&compiled, &BTreeMap::new(), &[1, 42], 1, 3)
            .expect("compact CSC expansion should succeed");

        match previous {
            Some(value) => std::env::set_var("GRAPH_COMPILED_KERNEL", value),
            None => std::env::remove_var("GRAPH_COMPILED_KERNEL"),
        }

        assert_eq!(compact.backend, SparseKernelBackend::RustSparse);
        assert_eq!(compact.vertices, rust.vertices);
        assert_eq!(compact.edge_visits, rust.edge_visits);
    }
}
