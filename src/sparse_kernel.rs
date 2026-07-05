use std::collections::{BTreeMap, BTreeSet};

use crate::{Result, VertexId};

pub(crate) type Adjacency = BTreeMap<VertexId, BTreeSet<VertexId>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphBlasCsc {
    pub vertices: Vec<VertexId>,
    pub pointers: Vec<u64>,
    pub indices: Vec<u64>,
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

#[cfg(feature = "graphblas")]
pub(crate) use graphblas::CompiledGraphBlasMatrix;

#[cfg(not(feature = "graphblas"))]
pub(crate) struct CompiledGraphBlasMatrix;

pub(crate) fn compile_graphblas_matrix(adjacency: &Adjacency) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas(adjacency)
}

pub(crate) fn compile_graphblas_csc(csc: &GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    compile_graphblas_from_csc(csc)
}

pub(crate) fn graphblas_csc_from_adjacency(adjacency: &Adjacency) -> Result<GraphBlasCsc> {
    let vertices = graphblas_vertices_from_adjacency(adjacency)?;
    let mut by_vertex = BTreeMap::new();
    for (idx, vertex) in vertices.iter().copied().enumerate() {
        by_vertex.insert(vertex, idx as u64);
    }

    let mut pointers = vec![0_u64; vertices.len() + 1];
    for (src, dsts) in adjacency {
        let Some(src_ordinal) = by_vertex.get(src).copied() else {
            return Err(crate::GraphError::SparseKernel {
                backend: "SuiteSparseGraphBlas",
                reason: format!("missing CSC ordinal for source vertex {src}"),
            });
        };
        pointers[src_ordinal as usize + 1] += dsts.len() as u64;
    }
    for idx in 1..pointers.len() {
        pointers[idx] += pointers[idx - 1];
    }

    let mut indices = vec![0_u64; pointers.last().copied().unwrap_or(0) as usize];
    let mut offsets = pointers[..vertices.len()].to_vec();
    for (src, dsts) in adjacency {
        let src_ordinal = by_vertex[src];
        let offset = &mut offsets[src_ordinal as usize];
        for dst in dsts {
            let Some(dst_ordinal) = by_vertex.get(dst).copied() else {
                return Err(crate::GraphError::SparseKernel {
                    backend: "SuiteSparseGraphBlas",
                    reason: format!("missing CSC ordinal for destination vertex {dst}"),
                });
            };
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

pub(crate) fn expand_compiled_graphblas(
    compiled: &CompiledGraphBlasMatrix,
    adjacency: &Adjacency,
    starts: &[VertexId],
    hops: u8,
) -> Result<SparseTraversal> {
    expand_graphblas_compiled(compiled, adjacency, starts, hops)
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
fn compile_graphblas(_adjacency: &Adjacency) -> Result<CompiledGraphBlasMatrix> {
    Err(crate::GraphError::SparseKernel {
        backend: "SuiteSparseGraphBlas",
        reason: "crate was built without the graphblas feature".to_string(),
    })
}

#[cfg(not(feature = "graphblas"))]
fn compile_graphblas_from_csc(_csc: &GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    Err(crate::GraphError::SparseKernel {
        backend: "SuiteSparseGraphBlas",
        reason: "crate was built without the graphblas feature".to_string(),
    })
}

#[cfg(not(feature = "graphblas"))]
fn expand_graphblas_compiled(
    _compiled: &CompiledGraphBlasMatrix,
    _adjacency: &Adjacency,
    _starts: &[VertexId],
    _hops: u8,
) -> Result<SparseTraversal> {
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

#[cfg(feature = "graphblas")]
fn compile_graphblas(adjacency: &Adjacency) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new(adjacency)
}

#[cfg(feature = "graphblas")]
fn compile_graphblas_from_csc(csc: &GraphBlasCsc) -> Result<CompiledGraphBlasMatrix> {
    graphblas::CompiledGraphBlasMatrix::new_from_csc(csc)
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
mod graphblas {
    use std::collections::BTreeMap;
    use std::ffi::c_void;
    use std::os::raw::c_int;
    use std::ptr::null_mut;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};

    use super::{
        graphblas_csc_from_adjacency, Adjacency, GraphBlasCsc, SparseKernelBackend, SparseTraversal,
    };
    use crate::{GraphError, Result, VertexId};

    type GrBInfo = c_int;
    type GrBFormat = c_int;
    type GrBIndex = u64;
    type GrBType = *mut c_void;
    type GrBMonoid = *mut c_void;
    type GrBSemiring = *mut c_void;
    type GrBMatrix = *mut c_void;
    type GrBVector = *mut c_void;
    type GrBDescriptor = *mut c_void;
    type GrBBinaryOp = *mut c_void;

    const GRB_SUCCESS: GrBInfo = 0;
    const GRB_BLOCKING: c_int = 1;
    const GRB_MATERIALIZE: c_int = 1;
    const GRB_CSC_FORMAT: GrBFormat = 1;
    const GRB_INDEX_MAX: u64 = (1_u64 << 60) - 1;

    #[link(name = "graphblas")]
    unsafe extern "C" {
        static GrB_BOOL: GrBType;
        static GrB_UINT64: GrBType;
        static GrB_DESC_S: GrBDescriptor;
        static GrB_DESC_SC: GrBDescriptor;
        static GrB_FIRST_UINT64: GrBBinaryOp;
        static GrB_LOR: GrBBinaryOp;
        static GrB_PLUS_UINT64: GrBBinaryOp;
        static GrB_LOR_LAND_SEMIRING_BOOL: GrBSemiring;
        static GrB_PLUS_MONOID_UINT64: GrBMonoid;

        fn GrB_init(mode: c_int) -> GrBInfo;
        fn GrB_Matrix_new(
            matrix: *mut GrBMatrix,
            value_type: GrBType,
            rows: GrBIndex,
            cols: GrBIndex,
        ) -> GrBInfo;
        fn GrB_Matrix_free(matrix: *mut GrBMatrix) -> GrBInfo;
        fn GrB_Matrix_import_BOOL(
            matrix: *mut GrBMatrix,
            value_type: GrBType,
            rows: GrBIndex,
            cols: GrBIndex,
            pointers: *const GrBIndex,
            indices: *const GrBIndex,
            values: *const bool,
            pointers_len: GrBIndex,
            indices_len: GrBIndex,
            values_len: GrBIndex,
            format: GrBFormat,
        ) -> GrBInfo;
        fn GrB_Matrix_wait(matrix: GrBMatrix, waitmode: c_int) -> GrBInfo;
        fn GrB_Vector_new(vector: *mut GrBVector, value_type: GrBType, size: GrBIndex) -> GrBInfo;
        fn GrB_Vector_free(vector: *mut GrBVector) -> GrBInfo;
        fn GrB_Vector_clear(vector: GrBVector) -> GrBInfo;
        fn GrB_Vector_build_BOOL(
            vector: GrBVector,
            indices: *const GrBIndex,
            values: *const bool,
            nvals: GrBIndex,
            dup: GrBBinaryOp,
        ) -> GrBInfo;
        fn GrB_Vector_build_UINT64(
            vector: GrBVector,
            indices: *const GrBIndex,
            values: *const u64,
            nvals: GrBIndex,
            dup: GrBBinaryOp,
        ) -> GrBInfo;
        fn GrB_Vector_nvals(nvals: *mut GrBIndex, vector: GrBVector) -> GrBInfo;
        fn GrB_Vector_extractTuples_BOOL(
            indices: *mut GrBIndex,
            values: *mut bool,
            nvals: *mut GrBIndex,
            vector: GrBVector,
        ) -> GrBInfo;
        fn GrB_Vector_wait(vector: GrBVector, waitmode: c_int) -> GrBInfo;
        fn GrB_Vector_eWiseAdd_BinaryOp(
            output: GrBVector,
            mask: GrBVector,
            accum: GrBBinaryOp,
            add: GrBBinaryOp,
            left: GrBVector,
            right: GrBVector,
            descriptor: GrBDescriptor,
        ) -> GrBInfo;
        fn GrB_Vector_eWiseMult_BinaryOp(
            output: GrBVector,
            mask: GrBVector,
            accum: GrBBinaryOp,
            multiply: GrBBinaryOp,
            left: GrBVector,
            right: GrBVector,
            descriptor: GrBDescriptor,
        ) -> GrBInfo;
        fn GrB_Vector_reduce_UINT64(
            output: *mut u64,
            accum: GrBBinaryOp,
            monoid: GrBMonoid,
            input: GrBVector,
            descriptor: GrBDescriptor,
        ) -> GrBInfo;
        fn GrB_mxv(
            output: GrBVector,
            mask: GrBVector,
            accum: GrBBinaryOp,
            semiring: GrBSemiring,
            matrix: GrBMatrix,
            input: GrBVector,
            descriptor: GrBDescriptor,
        ) -> GrBInfo;
    }

    static INIT: OnceLock<std::result::Result<(), String>> = OnceLock::new();

    struct Matrix(GrBMatrix);
    struct Vector(GrBVector);

    pub(crate) struct CompiledGraphBlasMatrix {
        replicas: Vec<Mutex<CompiledGraphBlasMatrixInner>>,
        next_replica: AtomicUsize,
    }

    struct CompiledGraphBlasMatrixInner {
        matrix: Option<Matrix>,
        ordinal_map: OrdinalMap,
        degree_vector: Option<Vector>,
    }

    unsafe impl Send for CompiledGraphBlasMatrixInner {}

    impl Drop for Matrix {
        fn drop(&mut self) {
            unsafe {
                let _ = GrB_Matrix_free(&mut self.0);
            }
        }
    }

    impl Drop for Vector {
        fn drop(&mut self) {
            unsafe {
                let _ = GrB_Vector_free(&mut self.0);
            }
        }
    }

    impl CompiledGraphBlasMatrix {
        pub(crate) fn new(adjacency: &Adjacency) -> Result<Self> {
            let csc = graphblas_csc_from_adjacency(adjacency)?;
            Self::new_from_csc(&csc)
        }

        pub(crate) fn new_from_csc(csc: &GraphBlasCsc) -> Result<Self> {
            validate_csc(csc)?;
            init()?;
            let replica_count = if csc.vertices.is_empty() {
                1
            } else {
                graphblas_replica_count()
            };
            let mut replicas = Vec::with_capacity(replica_count);
            for _ in 0..replica_count {
                replicas.push(Mutex::new(build_compiled_inner(csc)?));
            }
            Ok(Self {
                replicas,
                next_replica: AtomicUsize::new(0),
            })
        }

        pub(crate) fn expand(
            &self,
            adjacency: &Adjacency,
            starts: &[VertexId],
            hops: u8,
        ) -> Result<SparseTraversal> {
            let idx = self.next_replica.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
            let inner = self.replicas[idx]
                .lock()
                .map_err(|_| GraphError::SparseKernel {
                    backend: "SuiteSparseGraphBlas",
                    reason: "compiled matrix cache lock was poisoned".to_string(),
                })?;
            expand_with_compiled(adjacency, starts, hops, &inner)
        }
    }

    pub(super) fn expand(
        adjacency: &Adjacency,
        starts: &[VertexId],
        hops: u8,
    ) -> Result<SparseTraversal> {
        init()?;

        let csc = graphblas_csc_from_adjacency(adjacency)?;
        let compiled = build_compiled_inner(&csc)?;
        expand_with_compiled(adjacency, starts, hops, &compiled)
    }

    fn build_compiled_inner(csc: &GraphBlasCsc) -> Result<CompiledGraphBlasMatrixInner> {
        let ordinal_map = OrdinalMap::from_vertices(&csc.vertices)?;
        let matrix = if ordinal_map.is_empty() {
            None
        } else {
            Some(build_transposed_matrix(csc)?)
        };
        let degree_vector = if ordinal_map.is_empty() {
            None
        } else {
            Some(build_degree_vector(csc)?)
        };
        Ok(CompiledGraphBlasMatrixInner {
            matrix,
            degree_vector,
            ordinal_map,
        })
    }

    fn expand_with_compiled(
        _adjacency: &Adjacency,
        starts: &[VertexId],
        hops: u8,
        compiled: &CompiledGraphBlasMatrixInner,
    ) -> Result<SparseTraversal> {
        let Some(matrix) = compiled.matrix.as_ref() else {
            return Ok(empty_traversal());
        };
        let Some(degree_vector) = compiled.degree_vector.as_ref() else {
            return Ok(empty_traversal());
        };
        if compiled.ordinal_map.is_empty() || hops == 0 || starts.is_empty() {
            return Ok(empty_traversal());
        }

        let dimension = compiled.ordinal_map.len() as usize;
        let mut is_start = vec![false; dimension];
        let mut start_ordinals = Vec::with_capacity(starts.len());
        for start in starts {
            if let Some(ordinal) = compiled.ordinal_map.try_ordinal(*start) {
                let idx = ordinal as usize;
                if !is_start[idx] {
                    is_start[idx] = true;
                    start_ordinals.push(ordinal);
                }
            }
        }
        if start_ordinals.is_empty() {
            return Ok(empty_traversal());
        }

        let mut seen = vector_from_ordinals(compiled.ordinal_map.len(), &start_ordinals)?;
        let mut frontier = vector_from_ordinals(compiled.ordinal_map.len(), &start_ordinals)?;
        let mut degree_scratch = uint64_vector(compiled.ordinal_map.len())?;
        let mut edge_visits = 0_u64;

        for _ in 0..hops {
            edge_visits +=
                frontier_edge_visits_graphblas(degree_vector, &frontier, &mut degree_scratch)?;
            let next = masked_multiply(matrix, &frontier, &seen, compiled.ordinal_map.len())?;
            let next_count = vector_nvals(&next)?;
            if next_count == 0 {
                break;
            }
            union_into(&mut seen, &next)?;
            frontier = next;
        }

        let seen_ordinals = extract_ordinals(&seen)?;
        let mut vertices = Vec::with_capacity(seen_ordinals.len().saturating_sub(starts.len()));
        for ordinal in seen_ordinals {
            let idx = ordinal as usize;
            if idx < is_start.len() && !is_start[idx] {
                vertices.push(compiled.ordinal_map.vertex(ordinal)?);
            }
        }
        vertices.sort_unstable();
        Ok(SparseTraversal {
            vertices,
            edge_visits,
            backend: SparseKernelBackend::SuiteSparseGraphBlas,
        })
    }

    fn empty_traversal() -> SparseTraversal {
        SparseTraversal {
            vertices: Vec::new(),
            edge_visits: 0,
            backend: SparseKernelBackend::SuiteSparseGraphBlas,
        }
    }

    fn init() -> Result<()> {
        match INIT.get_or_init(|| unsafe {
            let info = GrB_init(GRB_BLOCKING);
            if info == GRB_SUCCESS {
                Ok(())
            } else {
                Err(format!("GrB_init returned GraphBLAS status {info}"))
            }
        }) {
            Ok(()) => Ok(()),
            Err(reason) => Err(GraphError::SparseKernel {
                backend: "SuiteSparseGraphBlas",
                reason: reason.clone(),
            }),
        }
    }

    fn graphblas_replica_count() -> usize {
        const DEFAULT_MAX_REPLICAS: usize = 4;
        const HARD_MAX_REPLICAS: usize = 64;
        if let Ok(value) = std::env::var("PHASE0_GRAPHBLAS_REPLICAS") {
            if let Ok(parsed) = value.parse::<usize>() {
                return parsed.clamp(1, HARD_MAX_REPLICAS);
            }
        }
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get().clamp(1, DEFAULT_MAX_REPLICAS))
            .unwrap_or(1)
    }

    fn build_transposed_matrix(csc: &GraphBlasCsc) -> Result<Matrix> {
        let dimension = csc.vertices.len() as GrBIndex;
        let edge_count = csc.indices.len();
        let values = vec![true; edge_count];
        let mut raw = null_mut();
        if edge_count > 0 {
            unsafe {
                check(
                    GrB_Matrix_import_BOOL(
                        &mut raw,
                        GrB_BOOL,
                        dimension,
                        dimension,
                        csc.pointers.as_ptr(),
                        csc.indices.as_ptr(),
                        values.as_ptr(),
                        csc.pointers.len() as GrBIndex,
                        csc.indices.len() as GrBIndex,
                        values.len() as GrBIndex,
                        GRB_CSC_FORMAT,
                    ),
                    "GrB_Matrix_import_BOOL",
                )?;
                let matrix = Matrix(raw);
                check(
                    GrB_Matrix_wait(matrix.0, GRB_MATERIALIZE),
                    "GrB_Matrix_wait",
                )?;
                return Ok(matrix);
            }
        }
        unsafe {
            check(
                GrB_Matrix_new(&mut raw, GrB_BOOL, dimension, dimension),
                "GrB_Matrix_new",
            )?;
        }
        Ok(Matrix(raw))
    }

    fn build_degree_vector(csc: &GraphBlasCsc) -> Result<Vector> {
        let dimension = csc.vertices.len() as GrBIndex;
        let mut raw = null_mut();
        unsafe {
            check(
                GrB_Vector_new(&mut raw, GrB_UINT64, dimension),
                "GrB_Vector_new",
            )?;
        }
        let vector = Vector(raw);
        let mut indices = Vec::new();
        let mut values = Vec::new();
        for ordinal in 0..csc.vertices.len() {
            let degree = csc.pointers[ordinal + 1] - csc.pointers[ordinal];
            if degree == 0 {
                continue;
            }
            indices.push(ordinal as GrBIndex);
            values.push(degree);
        }
        if !indices.is_empty() {
            unsafe {
                check(
                    GrB_Vector_build_UINT64(
                        vector.0,
                        indices.as_ptr(),
                        values.as_ptr(),
                        indices.len() as GrBIndex,
                        GrB_PLUS_UINT64,
                    ),
                    "GrB_Vector_build_UINT64",
                )?;
                check(
                    GrB_Vector_wait(vector.0, GRB_MATERIALIZE),
                    "GrB_Vector_wait",
                )?;
            }
        }
        Ok(vector)
    }

    fn vector_from_ordinals(dimension: GrBIndex, ordinals: &[GrBIndex]) -> Result<Vector> {
        let mut raw = null_mut();
        unsafe {
            check(
                GrB_Vector_new(&mut raw, GrB_BOOL, dimension),
                "GrB_Vector_new",
            )?;
        }
        let vector = Vector(raw);
        if ordinals.is_empty() {
            return Ok(vector);
        }
        let values = vec![true; ordinals.len()];
        unsafe {
            check(
                GrB_Vector_build_BOOL(
                    vector.0,
                    ordinals.as_ptr(),
                    values.as_ptr(),
                    ordinals.len() as GrBIndex,
                    GrB_LOR,
                ),
                "GrB_Vector_build_BOOL",
            )?;
        }
        Ok(vector)
    }

    fn uint64_vector(dimension: GrBIndex) -> Result<Vector> {
        let mut raw = null_mut();
        unsafe {
            check(
                GrB_Vector_new(&mut raw, GrB_UINT64, dimension),
                "GrB_Vector_new",
            )?;
        }
        Ok(Vector(raw))
    }

    fn masked_multiply(
        matrix: &Matrix,
        input: &Vector,
        seen: &Vector,
        dimension: GrBIndex,
    ) -> Result<Vector> {
        let mut raw = null_mut();
        unsafe {
            check(
                GrB_Vector_new(&mut raw, GrB_BOOL, dimension),
                "GrB_Vector_new",
            )?;
            check(
                GrB_mxv(
                    raw,
                    seen.0,
                    null_mut(),
                    GrB_LOR_LAND_SEMIRING_BOOL,
                    matrix.0,
                    input.0,
                    GrB_DESC_SC,
                ),
                "GrB_mxv",
            )?;
        }
        Ok(Vector(raw))
    }

    fn union_into(seen: &mut Vector, next: &Vector) -> Result<()> {
        unsafe {
            check(
                GrB_Vector_eWiseAdd_BinaryOp(
                    seen.0,
                    null_mut(),
                    null_mut(),
                    GrB_LOR,
                    seen.0,
                    next.0,
                    null_mut(),
                ),
                "GrB_Vector_eWiseAdd_BinaryOp",
            )?;
        }
        Ok(())
    }

    fn frontier_edge_visits_graphblas(
        degree_vector: &Vector,
        frontier: &Vector,
        scratch: &mut Vector,
    ) -> Result<u64> {
        unsafe {
            check(GrB_Vector_clear(scratch.0), "GrB_Vector_clear")?;
            check(
                GrB_Vector_eWiseMult_BinaryOp(
                    scratch.0,
                    frontier.0,
                    null_mut(),
                    GrB_FIRST_UINT64,
                    degree_vector.0,
                    degree_vector.0,
                    GrB_DESC_S,
                ),
                "GrB_Vector_eWiseMult_BinaryOp",
            )?;
            let mut edge_visits = 0_u64;
            check(
                GrB_Vector_reduce_UINT64(
                    &mut edge_visits,
                    null_mut(),
                    GrB_PLUS_MONOID_UINT64,
                    scratch.0,
                    null_mut(),
                ),
                "GrB_Vector_reduce_UINT64",
            )?;
            Ok(edge_visits)
        }
    }

    fn vector_nvals(vector: &Vector) -> Result<u64> {
        let mut nvals = 0_u64;
        unsafe {
            check(GrB_Vector_nvals(&mut nvals, vector.0), "GrB_Vector_nvals")?;
        }
        Ok(nvals)
    }

    fn extract_ordinals(vector: &Vector) -> Result<Vec<GrBIndex>> {
        let mut nvals = 0_u64;
        unsafe {
            check(GrB_Vector_nvals(&mut nvals, vector.0), "GrB_Vector_nvals")?;
        }
        let mut indices = vec![0_u64; nvals as usize];
        let mut capacity = nvals;
        unsafe {
            check(
                GrB_Vector_extractTuples_BOOL(
                    indices.as_mut_ptr(),
                    null_mut(),
                    &mut capacity,
                    vector.0,
                ),
                "GrB_Vector_extractTuples_BOOL",
            )?;
        }
        indices.truncate(capacity as usize);
        Ok(indices)
    }

    fn check(info: GrBInfo, operation: &'static str) -> Result<()> {
        if info == GRB_SUCCESS {
            return Ok(());
        }
        Err(GraphError::SparseKernel {
            backend: "SuiteSparseGraphBlas",
            reason: format!("{operation} returned GraphBLAS status {info}"),
        })
    }

    fn validate_csc(csc: &GraphBlasCsc) -> Result<()> {
        if csc.vertices.len() as u64 > GRB_INDEX_MAX {
            return Err(GraphError::SparseKernel {
                backend: "SuiteSparseGraphBlas",
                reason: format!(
                    "{} local vertices exceed GraphBLAS index limit",
                    csc.vertices.len()
                ),
            });
        }
        if csc.pointers.len() != csc.vertices.len() + 1 {
            return Err(GraphError::SparseKernel {
                backend: "SuiteSparseGraphBlas",
                reason: format!(
                    "CSC pointer count {} does not match vertex count {}",
                    csc.pointers.len(),
                    csc.vertices.len()
                ),
            });
        }
        if csc.pointers.first().copied() != Some(0) {
            return Err(GraphError::SparseKernel {
                backend: "SuiteSparseGraphBlas",
                reason: "CSC first pointer must be zero".to_string(),
            });
        }
        for window in csc.pointers.windows(2) {
            if window[0] > window[1] {
                return Err(GraphError::SparseKernel {
                    backend: "SuiteSparseGraphBlas",
                    reason: "CSC pointers must be monotonic".to_string(),
                });
            }
        }
        if csc.pointers.last().copied().unwrap_or(0) as usize != csc.indices.len() {
            return Err(GraphError::SparseKernel {
                backend: "SuiteSparseGraphBlas",
                reason: format!(
                    "CSC edge count {} does not match index count {}",
                    csc.pointers.last().copied().unwrap_or(0),
                    csc.indices.len()
                ),
            });
        }
        let dimension = csc.vertices.len() as u64;
        if let Some(index) = csc
            .indices
            .iter()
            .copied()
            .find(|index| *index >= dimension)
        {
            return Err(GraphError::SparseKernel {
                backend: "SuiteSparseGraphBlas",
                reason: format!("CSC row index {index} exceeds dimension {dimension}"),
            });
        }
        for window in csc.vertices.windows(2) {
            if window[0] >= window[1] {
                return Err(GraphError::SparseKernel {
                    backend: "SuiteSparseGraphBlas",
                    reason: "CSC vertices must be sorted and unique".to_string(),
                });
            }
        }
        Ok(())
    }

    struct OrdinalMap {
        by_vertex: BTreeMap<VertexId, GrBIndex>,
        by_ordinal: Vec<VertexId>,
    }

    impl OrdinalMap {
        fn from_vertices(vertices: &[VertexId]) -> Result<Self> {
            if vertices.len() as u64 > GRB_INDEX_MAX {
                return Err(GraphError::SparseKernel {
                    backend: "SuiteSparseGraphBlas",
                    reason: format!(
                        "{} local vertices exceed GraphBLAS index limit",
                        vertices.len()
                    ),
                });
            }
            for window in vertices.windows(2) {
                if window[0] >= window[1] {
                    return Err(GraphError::SparseKernel {
                        backend: "SuiteSparseGraphBlas",
                        reason: "ordinal vertices must be sorted and unique".to_string(),
                    });
                }
            }
            let by_ordinal = vertices.to_vec();
            let by_vertex = by_ordinal
                .iter()
                .enumerate()
                .map(|(idx, vertex)| (*vertex, idx as GrBIndex))
                .collect();
            Ok(Self {
                by_vertex,
                by_ordinal,
            })
        }

        fn len(&self) -> GrBIndex {
            self.by_ordinal.len() as GrBIndex
        }

        fn is_empty(&self) -> bool {
            self.by_ordinal.is_empty()
        }

        fn try_ordinal(&self, vertex: VertexId) -> Option<GrBIndex> {
            self.by_vertex.get(&vertex).copied()
        }

        fn vertex(&self, ordinal: GrBIndex) -> Result<VertexId> {
            self.by_ordinal
                .get(ordinal as usize)
                .copied()
                .ok_or_else(|| GraphError::SparseKernel {
                    backend: "SuiteSparseGraphBlas",
                    reason: format!("GraphBLAS returned invalid local ordinal {ordinal}"),
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
