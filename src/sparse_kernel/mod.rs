//! Sparse traversal kernels.
//!
//! Three kernels live behind this module. They are *not* two backends with a
//! feature flag between them — they form a ladder, where each rung adds
//! capability and setup cost:
//!
//! | # | Kernel | Where | Representation | Compiled? |
//! |---|--------|-------|----------------|-----------|
//! | 1 | Adjacency BFS | [`expand_rust`] in this file | `BTreeMap<VertexId, BTreeSet<VertexId>>` | no |
//! | 2 | Compact CSC BFS | `graphblas::CompiledCompactCscMatrix` | flat `u32`/`u64` CSC + `seen` bitmap | yes |
//! | 3 | SuiteSparse GraphBLAS | `graphblas::expand_with_compiled` | `GrB_Matrix`, traversal is a masked `GrB_mxv` | yes |
//!
//! Kernel 2 contains no C. It lives in `graphblas.rs` because it shares that
//! module's compiled-matrix representation, not because it needs SuiteSparse.
//!
//! # Capability surface
//!
//! All three agree on results — the cross-kernel equivalence tests at the
//! bottom of this file assert kernel 1 and kernel 3 return identical vertex
//! sets. They do *not* agree on which operations exist:
//!
//! | Operation | 1 Adjacency | 2 Compact CSC | 3 SuiteSparse |
//! |-----------|-------------|---------------|---------------|
//! | `expand` (fixed hops) | yes | yes | yes |
//! | `expand_range` (min..max hops) | yes | yes | yes |
//! | `expand_range_count` (count pushdown) | no | yes | yes |
//! | `contains_edge` | no | yes | yes |
//! | Precompiled, cached per epoch | no | yes | yes |
//!
//! Skip/limit is *not* pushed into any kernel. `shard::query` materialises the
//! reachable set, sorts it ascending, caches it, and trims with
//! `graph_kernel_window_sorted_vertices`, on every rung of the ladder.
//!
//! Kernel 1 is therefore a strict subset. When the compiled path is
//! unavailable it returns `None` and callers fall through to kernel 1 (see
//! `shard::query`, counted by `query_rust_sparse_fallbacks`) — which is a
//! capability downgrade, not merely a slower path: counts must be answered by
//! materialising the full vertex set and counting it.
//!
//! # Cost model
//!
//! - **Kernel 1** allocates per frontier node and chases pointers through
//!   B-trees. No setup cost, works on whatever adjacency was hydrated.
//! - **Kernel 2** is the same algorithm over flat arrays. Needs a CSC build
//!   step, which is cheap and cacheable.
//! - **Kernel 3** folds "expand and exclude already-seen" into one masked
//!   `mxv`. It pays for a C dependency and, because a `GrB_Matrix` is not
//!   thread-safe, keeps up to four replicas of hot matrices
//!   (`graphblas_replica_count`) — a memory multiplier kernel 2 does not pay.
//!   Note that the descriptor built by `exact_count_descriptor` defaults to a
//!   single thread, so kernel 3's advantage rests on `mxv` efficiency rather
//!   than on parallelism.
//!
//! # Selecting a kernel
//!
//! One 3-valued enum, [`SparseKernelBackend`], names the rungs. It is carried
//! by `GraphCachePolicy::sparse_kernel`, parsed from `GRAPH_SPARSE_KERNEL` in
//! `bin/graph_node/config.rs`, and [`default_matrix_kernel`] is a pure read of
//! that field. The legacy `GRAPH_COMPILED_KERNEL=compact` override supplies the
//! field's *default value* only (see [`env_default_kernel`]), so an explicit
//! policy always outranks it and no traversal re-reads the environment.
//!
//! The choice is resolved once, at matrix-compile time, and baked into the
//! compiled artifact: `CompiledGraphBlasMatrix` stores the resolved
//! `SparseKernelBackend` it was built with and dispatches on that field, so
//! downstream code asks the artifact what it is rather than re-reading
//! configuration. Keeping that invariant is what makes the kernel set cheap to
//! extend — adding a rung to the ladder should not require touching call sites.
//!
//! Kernel 1 has no compiled form, so `SparseKernelBackend::Adjacency` is
//! enforced by *skipping* compilation: the artifact builders do not build a
//! matrix and `shard::query` returns `None`, which routes to the existing
//! `None`-fallthrough. It is a ceiling rather than a preference —
//! `matrix_reachable_with_kernel` clamps its kernel argument against the policy,
//! or a caller asking for a compiled rung would compile one anyway.
//!
//! Every traversal reports the rung that actually executed on
//! `SparseTraversal::backend`. Note that `shard::query` currently discards it,
//! so the Cypher path still cannot distinguish kernel 2 from kernel 3; only the
//! `matrix_reachable*` API surfaces it, via `MatrixTraversalResult`.

use std::collections::{BTreeMap, BTreeSet};

use crate::{GraphCachePolicy, Result, VertexId};

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

/// The three rungs of the kernel ladder described in the module docs.
///
/// Non-exhaustive: the ladder is designed to grow, and the whole point of the
/// consolidation is that adding a rung should not require touching call sites.
/// Downstream `match`es therefore need a wildcard arm.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SparseKernelBackend {
    /// Kernel 1: adjacency BFS over `BTreeMap`/`BTreeSet`. Never compiled.
    Adjacency,
    /// Kernel 2: BFS over the compiled flat CSC. No SuiteSparse involved.
    CompactCsc,
    /// Kernel 3: masked `GrB_mxv` over a compiled `GrB_Matrix`.
    #[default]
    SuiteSparse,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SparseTraversal {
    pub vertices: Vec<VertexId>,
    pub edge_visits: u64,
    pub backend: SparseKernelBackend,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SparseTraversalCount {
    pub vertices: u64,
    pub edge_visits: u64,
    pub backend: SparseKernelBackend,
}

/// The kernel a [`GraphCachePolicy`] starts at, resolved once per process.
///
/// The legacy `GRAPH_COMPILED_KERNEL` override supplies only this *default*, so
/// an explicit `sparse_kernel` — whether from `GRAPH_SPARSE_KERNEL` or set in
/// code — always wins. Resolving it here rather than at each traversal is what
/// keeps `std::env::var` off the query path: the environment is read once, on
/// first use, and never again.
pub(crate) fn env_default_kernel() -> SparseKernelBackend {
    static RESOLVED: std::sync::OnceLock<SparseKernelBackend> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| {
        if graphblas::use_compact_csc_kernel() {
            SparseKernelBackend::CompactCsc
        } else {
            SparseKernelBackend::SuiteSparse
        }
    })
}

/// Resolves the configured kernel for a shard. The policy field is the single
/// authority; see [`env_default_kernel`] for where that field's default is from.
pub(crate) fn default_matrix_kernel(policy: &GraphCachePolicy) -> SparseKernelBackend {
    policy.sparse_kernel
}

pub(crate) fn expand(
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
    backend: SparseKernelBackend,
) -> Result<SparseTraversal> {
    match backend {
        SparseKernelBackend::Adjacency => Ok(expand_rust(adjacency, starts, hops)),
        SparseKernelBackend::CompactCsc => {
            let compiled = compile_graphblas_matrix(adjacency, backend)?;
            compiled.expand(adjacency, starts, hops)
        }
        SparseKernelBackend::SuiteSparse => expand_graphblas(adjacency, starts, hops),
    }
}

pub(crate) use graphblas::CompiledGraphBlasMatrix;

pub(crate) fn compile_graphblas_matrix(
    adjacency: &Adjacency,
    kernel: SparseKernelBackend,
) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas(adjacency, kernel)
}

pub(crate) fn compile_graphblas_csc(
    csc: &GraphBlasCsc,
    kernel: SparseKernelBackend,
) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas_from_csc(csc, kernel)
}

pub(crate) fn compile_graphblas_csc_owned(
    csc: GraphBlasCsc,
    kernel: SparseKernelBackend,
) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas_from_csc_owned(csc, kernel)
}

pub(crate) fn compile_graphblas_compact_csc_u32(
    vertices: Vec<VertexId>,
    pointers: Vec<u32>,
    indices: Vec<u32>,
    kernel: SparseKernelBackend,
) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new_from_compact_u32(vertices, pointers, indices, kernel)
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

pub(crate) fn expand_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
) -> Result<SparseTraversal> {
    expand_graphblas_compiled(compiled, adjacency, starts, hops)
}

pub(crate) fn compiled_graphblas_kernel(compiled: &CompiledGraphBlasMatrix) -> SparseKernelBackend {
    compiled.kernel()
}

#[cfg(feature = "opencypher")]
pub(crate) fn compiled_graphblas_in_neighbors(
    compiled: &CompiledGraphBlasMatrix,
    vertex: VertexId,
) -> Vec<VertexId> {
    compiled.in_neighbors(vertex)
}

pub(crate) fn compiled_graphblas_out_neighbors(
    compiled: &CompiledGraphBlasMatrix,
    vertex: VertexId,
) -> Vec<VertexId> {
    compiled.out_neighbors(vertex)
}

#[cfg(feature = "opencypher")]
pub(crate) fn compiled_graphblas_cached_in_degree(
    compiled: &CompiledGraphBlasMatrix,
    vertex: VertexId,
) -> Option<u64> {
    compiled.cached_in_degree(vertex)
}

#[cfg(feature = "opencypher")]
pub(crate) fn compiled_graphblas_out_degree(
    compiled: &CompiledGraphBlasMatrix,
    vertex: VertexId,
) -> u64 {
    compiled.out_degree(vertex)
}

#[cfg(feature = "opencypher")]
pub(crate) fn compiled_graphblas_in_neighbors_intersecting(
    compiled: &CompiledGraphBlasMatrix,
    vertex: VertexId,
    allowed: &BTreeSet<VertexId>,
) -> Vec<VertexId> {
    compiled.in_neighbors_intersecting(vertex, allowed)
}

#[cfg(feature = "opencypher")]
pub(crate) fn compiled_graphblas_out_neighbors_intersecting(
    compiled: &CompiledGraphBlasMatrix,
    vertex: VertexId,
    allowed: &BTreeSet<VertexId>,
) -> Vec<VertexId> {
    compiled.out_neighbors_intersecting(vertex, allowed)
}

pub(crate) fn expand_range_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversal> {
    expand_range_graphblas_compiled(compiled, adjacency, starts, min_hops, max_hops)
}

#[cfg(feature = "opencypher")]
pub(crate) fn expand_range_count_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversalCount> {
    expand_range_count_graphblas_compiled(compiled, adjacency, starts, min_hops, max_hops)
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
        backend: SparseKernelBackend::Adjacency,
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

fn expand_graphblas(
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
) -> Result<SparseTraversal> {
    graphblas::expand(adjacency, starts, hops)
}

fn compile_graphblas(
    adjacency: &Adjacency,
    kernel: SparseKernelBackend,
) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new(adjacency, kernel)
}

fn compile_graphblas_from_csc(
    csc: &GraphBlasCsc,
    kernel: SparseKernelBackend,
) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new_from_csc(csc, kernel)
}

fn compile_graphblas_from_csc_owned(
    csc: GraphBlasCsc,
    kernel: SparseKernelBackend,
) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new_from_csc_owned(csc, kernel)
}

fn expand_graphblas_compiled(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
) -> Result<SparseTraversal> {
    compiled.expand(adjacency, starts, hops)
}

fn expand_range_graphblas_compiled(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversal> {
    compiled.expand_range(adjacency, starts, min_hops, max_hops)
}

#[cfg(feature = "opencypher")]
fn expand_range_count_graphblas_compiled(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversalCount> {
    compiled.expand_range_count(adjacency, starts, min_hops, max_hops)
}

mod graphblas;
