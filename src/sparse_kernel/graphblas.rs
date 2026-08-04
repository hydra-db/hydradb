//! SuiteSparse:GraphBLAS FFI kernel.
//!
//! Also hosts `CompiledCompactCscMatrix`, the pure-Rust kernel that shares this
//! module's compiled-matrix representation without calling into SuiteSparse.
//! See the module docs in `mod.rs` for how the two relate.

use std::ffi::c_void;
use std::os::raw::c_int;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, TryLockError};

use super::{
    graphblas_csc_from_adjacency, Adjacency, GraphBlasCsc, SparseKernelBackend, SparseTraversal,
};
#[cfg(feature = "opencypher")]
use super::{SparseTraversalCount, SparseTraversalWindow};
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
const GRB_INVALID_VALUE: GrBInfo = -3;
const GRB_BLOCKING: c_int = 1;
const GRB_MATERIALIZE: c_int = 1;
const GRB_CSC_FORMAT: GrBFormat = 1;
const GRB_INDEX_MAX: u64 = (1_u64 << 60) - 1;
// GxB_NTHREADS, whose value changed between major GraphBLAS releases: 5 through
// v8, 7086 from v9 on. Neither library accepts the other's constant — both
// return GrB_INVALID_VALUE — so `init` tries the current one and falls back.
// Ubuntu 24.04, which both CI and the runtime image build against, still ships
// v7.4; Homebrew ships v10.
//
// v9 also stopped accepting the field on a *descriptor*: GxB_Desc_set_INT32
// returns GrB_INVALID_VALUE for it regardless of the constant used, even though
// the v10 header still lists it under GrB_Desc_Field. Thread count is now only
// settable globally (or per GxB_Context), which is what `init` does below.
const GXB_NTHREADS: c_int = 7086;
const GXB_NTHREADS_LEGACY: c_int = 5;

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
    fn GrB_Descriptor_new(descriptor: *mut GrBDescriptor) -> GrBInfo;
    fn GrB_Descriptor_free(descriptor: *mut GrBDescriptor) -> GrBInfo;
    // The v9 spelling of this is GrB_Global_set_INT32 against the GrB_GLOBAL
    // handle, but neither symbol exists before v9, so naming them here fails to
    // *link* on v7 whether or not the call is reached. GxB_Global_Option_set_INT32
    // is exported by v7 through v10 alike.
    fn GxB_Global_Option_set_INT32(field: c_int, value: i32) -> GrBInfo;
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
// Only consumed by the opencypher-gated inline-count query path.
#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
static INLINE_WORK_EDGES: OnceLock<usize> = OnceLock::new();

struct Matrix(GrBMatrix);
struct Vector(GrBVector);
struct Descriptor(GrBDescriptor);

pub(crate) struct CompiledGraphBlasMatrix {
    canonical_out: CompiledCompactCscMatrix,
    #[cfg(feature = "opencypher")]
    canonical_in: OnceLock<CompiledCompactCscMatrix>,
    // Resolved once by the constructor and never re-read from configuration.
    kernel: SparseKernelBackend,
    replicas: Vec<Mutex<CompiledGraphBlasMatrixInner>>,
    next_replica: AtomicUsize,
    #[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
    edge_count: usize,
    estimated_resident_bytes: usize,
}

struct CompiledCompactCscMatrix {
    vertices: Vec<VertexId>,
    pointers: CompactOrdinalVec,
    indices: CompactOrdinalVec,
}

enum CompactOrdinalVec {
    U32(Vec<u32>),
    U64(Vec<u64>),
}

struct CompiledGraphBlasMatrixInner {
    matrix: Option<Matrix>,
    ordinal_map: OrdinalMap,
    degree_vector: Option<Vector>,
    #[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
    count_scratch: Option<GraphBlasCountScratch>,
}

// Scratch buffers for the opencypher-gated inline-count path.
#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
struct GraphBlasCountScratch {
    frontier: Vector,
    next: Vector,
    degree: Vector,
    descriptor: Descriptor,
}

unsafe impl Send for CompiledGraphBlasMatrixInner {}

impl CompactOrdinalVec {
    fn from_u64(values: Vec<u64>) -> Self {
        if values.iter().all(|value| u32::try_from(*value).is_ok()) {
            Self::U32(values.into_iter().map(|value| value as u32).collect())
        } else {
            Self::U64(values)
        }
    }

    fn from_u32(values: Vec<u32>) -> Self {
        Self::U32(values)
    }

    fn get(&self, index: usize) -> usize {
        match self {
            Self::U32(values) => values[index] as usize,
            Self::U64(values) => values[index] as usize,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::U32(values) => values.len(),
            Self::U64(values) => values.len(),
        }
    }
}

impl CompiledCompactCscMatrix {
    fn from_csc(csc: &GraphBlasCsc) -> Self {
        Self {
            vertices: csc.vertices.clone(),
            pointers: CompactOrdinalVec::from_u64(csc.pointers.clone()),
            indices: CompactOrdinalVec::from_u64(csc.indices.clone()),
        }
    }

    fn from_owned_csc(csc: GraphBlasCsc) -> Self {
        Self {
            vertices: csc.vertices,
            pointers: CompactOrdinalVec::from_u64(csc.pointers),
            indices: CompactOrdinalVec::from_u64(csc.indices),
        }
    }

    fn from_u32_parts(vertices: Vec<VertexId>, pointers: Vec<u32>, indices: Vec<u32>) -> Self {
        Self {
            vertices,
            pointers: CompactOrdinalVec::from_u32(pointers),
            indices: CompactOrdinalVec::from_u32(indices),
        }
    }

    fn dimension(&self) -> usize {
        self.vertices.len()
    }

    fn ordinal(&self, vertex: VertexId) -> Option<usize> {
        self.vertices.binary_search(&vertex).ok()
    }

    fn degree(&self, ordinal: usize) -> u64 {
        self.pointer(ordinal + 1)
            .saturating_sub(self.pointer(ordinal)) as u64
    }

    fn pointer(&self, ordinal: usize) -> usize {
        self.pointers.get(ordinal)
    }

    fn neighbor_range(&self, ordinal: usize) -> std::ops::Range<usize> {
        self.pointer(ordinal)..self.pointer(ordinal + 1)
    }

    fn neighbor(&self, index: usize) -> usize {
        self.indices.get(index)
    }

    fn contains_edge(&self, src: VertexId, dst: VertexId) -> bool {
        let (Some(src_ordinal), Some(dst_ordinal)) = (self.ordinal(src), self.ordinal(dst)) else {
            return false;
        };
        self.neighbor_range(src_ordinal)
            .any(|index| self.neighbor(index) == dst_ordinal)
    }

    #[cfg(feature = "opencypher")]
    fn neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        let Some(ordinal) = self.ordinal(vertex) else {
            return Vec::new();
        };
        self.neighbor_range(ordinal)
            .map(|index| self.vertices[self.neighbor(index)])
            .collect()
    }

    #[cfg(feature = "opencypher")]
    fn transpose(&self) -> Self {
        let dimension = self.dimension();
        let mut pointers = vec![0_u64; dimension + 1];
        for index in 0..self.indices.len() {
            pointers[self.neighbor(index) + 1] += 1;
        }
        for index in 1..pointers.len() {
            pointers[index] += pointers[index - 1];
        }
        let mut indices = vec![0_u64; self.indices.len()];
        let mut offsets = pointers[..dimension].to_vec();
        for src in 0..dimension {
            for index in self.neighbor_range(src) {
                let dst = self.neighbor(index);
                indices[offsets[dst] as usize] = src as u64;
                offsets[dst] += 1;
            }
        }
        Self {
            vertices: self.vertices.clone(),
            pointers: CompactOrdinalVec::from_u64(pointers),
            indices: CompactOrdinalVec::from_u64(indices),
        }
    }

    fn start_ordinals(&self, starts: &[VertexId]) -> Vec<usize> {
        let mut ordinals = Vec::with_capacity(starts.len());
        for start in starts {
            if let Some(ordinal) = self.ordinal(*start) {
                ordinals.push(ordinal);
            }
        }
        ordinals.sort_unstable();
        ordinals.dedup();
        ordinals
    }

    fn expand(&self, starts: &[VertexId], hops: u8) -> Result<SparseTraversal> {
        if hops == 0 || self.vertices.is_empty() || starts.is_empty() {
            return Ok(compact_empty_traversal());
        }
        let start_ordinals = self.start_ordinals(starts);
        if start_ordinals.is_empty() {
            return Ok(compact_empty_traversal());
        }

        let dimension = self.dimension();
        let mut seen = vec![false; dimension];
        let mut start_seen = vec![false; dimension];
        let mut frontier = Vec::with_capacity(start_ordinals.len());
        for ordinal in start_ordinals {
            start_seen[ordinal] = true;
            seen[ordinal] = true;
            frontier.push(ordinal);
        }

        let mut next_seen = vec![0_u8; dimension];
        let mut edge_visits = 0_u64;
        for depth in 1..=hops {
            let marker = depth;
            let mut next = Vec::new();
            for src in &frontier {
                edge_visits = edge_visits.saturating_add(self.degree(*src));
                for index in self.neighbor_range(*src) {
                    let dst = self.neighbor(index);
                    if dst >= dimension || seen[dst] || next_seen[dst] == marker {
                        continue;
                    }
                    next_seen[dst] = marker;
                    next.push(dst);
                }
            }
            if next.is_empty() {
                break;
            }
            for dst in &next {
                seen[*dst] = true;
            }
            frontier = next;
        }

        let vertices = seen
            .into_iter()
            .enumerate()
            .filter_map(|(ordinal, reached)| {
                (reached && !start_seen[ordinal]).then_some(self.vertices[ordinal])
            })
            .collect();
        Ok(SparseTraversal {
            vertices,
            edge_visits,
            backend: SparseKernelBackend::CompactCsc,
        })
    }

    fn expand_range(
        &self,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
    ) -> Result<SparseTraversal> {
        let (vertices, edge_visits) = self.expand_range_ordinals(starts, min_hops, max_hops)?;
        Ok(SparseTraversal {
            vertices: vertices
                .into_iter()
                .map(|ordinal| self.vertices[ordinal])
                .collect(),
            edge_visits,
            backend: SparseKernelBackend::CompactCsc,
        })
    }

    #[cfg(feature = "opencypher")]
    fn expand_range_count(
        &self,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
    ) -> Result<SparseTraversalCount> {
        let (result_seen, edge_visits) = self.expand_range_bitmap(starts, min_hops, max_hops)?;
        Ok(SparseTraversalCount {
            vertices: result_seen.into_iter().filter(|seen| *seen).count() as u64,
            edge_visits,
            backend: SparseKernelBackend::CompactCsc,
        })
    }

    #[cfg(feature = "opencypher")]
    fn expand_range_window(
        &self,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
        window: SparseTraversalWindow,
    ) -> Result<SparseTraversal> {
        let (result_seen, edge_visits) = self.expand_range_bitmap(starts, min_hops, max_hops)?;
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::SparseKernel {
            backend: "CompactCsc",
            reason: format!("window skip {} does not fit in usize", window.skip),
        })?;
        let limit = window.limit.unwrap_or(usize::MAX);
        let mut skipped = 0_usize;
        let mut selected = Vec::with_capacity(limit.min(1024));
        if window.ascending {
            for (ordinal, seen) in result_seen.into_iter().enumerate() {
                if seen {
                    if skipped < skip {
                        skipped += 1;
                        continue;
                    }
                    selected.push(self.vertices[ordinal]);
                    if selected.len() == limit {
                        break;
                    }
                }
            }
        } else {
            for (ordinal, seen) in result_seen.into_iter().enumerate().rev() {
                if seen {
                    if skipped < skip {
                        skipped += 1;
                        continue;
                    }
                    selected.push(self.vertices[ordinal]);
                    if selected.len() == limit {
                        break;
                    }
                }
            }
        }
        Ok(SparseTraversal {
            vertices: selected,
            edge_visits,
            backend: SparseKernelBackend::CompactCsc,
        })
    }

    fn expand_range_ordinals(
        &self,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
    ) -> Result<(Vec<usize>, u64)> {
        let (result_seen, edge_visits) = self.expand_range_bitmap(starts, min_hops, max_hops)?;
        Ok((
            result_seen
                .into_iter()
                .enumerate()
                .filter_map(|(ordinal, reached)| reached.then_some(ordinal))
                .collect(),
            edge_visits,
        ))
    }

    fn expand_range_bitmap(
        &self,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
    ) -> Result<(Vec<bool>, u64)> {
        if min_hops > max_hops || self.vertices.is_empty() || starts.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let mut frontier = self.start_ordinals(starts);
        if frontier.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let dimension = self.dimension();
        let mut result_seen = vec![false; dimension];
        if min_hops == 0 {
            for ordinal in &frontier {
                result_seen[*ordinal] = true;
            }
        }
        let mut next_seen = vec![0_u8; dimension];
        let mut edge_visits = 0_u64;

        for depth in 1..=max_hops {
            let marker = depth;
            let mut next = Vec::new();
            for src in &frontier {
                edge_visits = edge_visits.saturating_add(self.degree(*src));
                for index in self.neighbor_range(*src) {
                    let dst = self.neighbor(index);
                    if dst >= dimension || next_seen[dst] == marker {
                        continue;
                    }
                    next_seen[dst] = marker;
                    next.push(dst);
                }
            }
            if depth >= min_hops {
                for dst in &next {
                    result_seen[*dst] = true;
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }

        Ok((result_seen, edge_visits))
    }
}

fn compact_empty_traversal() -> SparseTraversal {
    SparseTraversal {
        vertices: Vec::new(),
        edge_visits: 0,
        backend: SparseKernelBackend::CompactCsc,
    }
}

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

impl Drop for Descriptor {
    fn drop(&mut self) {
        unsafe {
            let _ = GrB_Descriptor_free(&mut self.0);
        }
    }
}

/// Narrows a configured kernel to one that has a compiled representation.
///
/// Kernel 1 has none, so a matrix compiled under `Adjacency` lands on the
/// compact CSC rung — its closest compiled peer. Callers that honour the
/// `Adjacency` setting skip compilation entirely, so this is only a guard.
fn compiled_kernel(requested: SparseKernelBackend) -> SparseKernelBackend {
    match requested {
        SparseKernelBackend::SuiteSparse => SparseKernelBackend::SuiteSparse,
        SparseKernelBackend::Adjacency | SparseKernelBackend::CompactCsc => {
            SparseKernelBackend::CompactCsc
        }
    }
}

fn compact_csc_query_bytes(one_orientation_bytes: usize) -> usize {
    #[cfg(feature = "opencypher")]
    {
        one_orientation_bytes.saturating_mul(2)
    }
    #[cfg(not(feature = "opencypher"))]
    {
        one_orientation_bytes
    }
}

impl CompiledGraphBlasMatrix {
    pub(crate) fn new(adjacency: &Adjacency, kernel: SparseKernelBackend) -> Result<Self> {
        let csc = graphblas_csc_from_adjacency(adjacency)?;
        Self::new_from_csc(&csc, kernel)
    }

    pub(crate) fn new_from_csc(csc: &GraphBlasCsc, kernel: SparseKernelBackend) -> Result<Self> {
        validate_csc(csc)?;
        let kernel = compiled_kernel(kernel);
        let canonical_out = CompiledCompactCscMatrix::from_csc(csc);
        if kernel == SparseKernelBackend::CompactCsc {
            return Ok(Self {
                canonical_out,
                #[cfg(feature = "opencypher")]
                canonical_in: OnceLock::new(),
                kernel,
                replicas: Vec::new(),
                next_replica: AtomicUsize::new(0),
                edge_count: csc.indices.len(),
                // Reserve both orientations in cache accounting. The reverse
                // CSC is allocated lazily, but allowing it to appear outside
                // the cache budget would make an incoming-path workload break
                // the configured resident-memory bound.
                estimated_resident_bytes: compact_csc_query_bytes(compact_csc_resident_bytes(csc)),
            });
        }
        init()?;
        let replica_count = if csc.vertices.is_empty() {
            1
        } else {
            graphblas_replica_count(csc)
        };
        let mut replicas = Vec::with_capacity(replica_count);
        for _ in 0..replica_count {
            replicas.push(Mutex::new(build_compiled_inner(csc)?));
        }
        Ok(Self {
            canonical_out,
            #[cfg(feature = "opencypher")]
            canonical_in: OnceLock::new(),
            kernel,
            replicas,
            next_replica: AtomicUsize::new(0),
            edge_count: csc.indices.len(),
            estimated_resident_bytes: native_graphblas_resident_bytes(csc, replica_count)
                .saturating_add(compact_csc_query_bytes(compact_csc_resident_bytes(csc))),
        })
    }

    pub(crate) fn new_from_csc_owned(
        csc: GraphBlasCsc,
        kernel: SparseKernelBackend,
    ) -> Result<Self> {
        validate_csc(&csc)?;
        let kernel = compiled_kernel(kernel);
        if kernel == SparseKernelBackend::CompactCsc {
            let compact_bytes = compact_csc_resident_bytes(&csc);
            let canonical_out = CompiledCompactCscMatrix::from_owned_csc(csc);
            let edge_count = canonical_out.indices.len();
            return Ok(Self {
                canonical_out,
                #[cfg(feature = "opencypher")]
                canonical_in: OnceLock::new(),
                kernel,
                replicas: Vec::new(),
                next_replica: AtomicUsize::new(0),
                edge_count,
                estimated_resident_bytes: compact_csc_query_bytes(compact_bytes),
            });
        }
        Self::new_from_csc(&csc, kernel)
    }

    pub(crate) fn new_from_compact_u32(
        vertices: Vec<VertexId>,
        pointers: Vec<u32>,
        indices: Vec<u32>,
        kernel: SparseKernelBackend,
    ) -> Result<Self> {
        validate_compact_u32_csc(&vertices, &pointers, &indices)?;
        let kernel = compiled_kernel(kernel);
        if kernel == SparseKernelBackend::CompactCsc {
            let one_orientation_bytes = vertices
                .capacity()
                .saturating_mul(std::mem::size_of::<VertexId>())
                .saturating_add(
                    pointers
                        .capacity()
                        .saturating_mul(std::mem::size_of::<u32>()),
                )
                .saturating_add(
                    indices
                        .capacity()
                        .saturating_mul(std::mem::size_of::<u32>()),
                );
            let canonical_out =
                CompiledCompactCscMatrix::from_u32_parts(vertices, pointers, indices);
            let edge_count = canonical_out.indices.len();
            return Ok(Self {
                canonical_out,
                #[cfg(feature = "opencypher")]
                canonical_in: OnceLock::new(),
                kernel,
                replicas: Vec::new(),
                next_replica: AtomicUsize::new(0),
                edge_count,
                estimated_resident_bytes: compact_csc_query_bytes(one_orientation_bytes),
            });
        }
        let csc = GraphBlasCsc {
            vertices,
            pointers: pointers.into_iter().map(u64::from).collect(),
            indices: indices.into_iter().map(u64::from).collect(),
        };
        Self::new_from_csc_owned(csc, kernel)
    }

    /// The kernel this artifact was compiled for. Downstream code asks the
    /// artifact rather than re-reading configuration.
    pub(crate) fn kernel(&self) -> SparseKernelBackend {
        self.kernel
    }

    pub(crate) fn expand(
        &self,
        adjacency: &Adjacency,
        starts: &[VertexId],
        hops: u8,
    ) -> Result<SparseTraversal> {
        if self.kernel == SparseKernelBackend::CompactCsc {
            return self.canonical_out.expand(starts, hops);
        }
        let inner = self.lock_available_replica()?;
        expand_with_compiled(adjacency, starts, hops, &inner)
    }

    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        self.estimated_resident_bytes
    }

    pub(crate) fn contains_edge(&self, src: VertexId, dst: VertexId) -> bool {
        self.canonical_out.contains_edge(src, dst)
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn in_neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        self.canonical_in
            .get_or_init(|| self.canonical_out.transpose())
            .neighbors(vertex)
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn out_neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        self.canonical_out.neighbors(vertex)
    }

    pub(crate) fn expand_range(
        &self,
        adjacency: &Adjacency,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
    ) -> Result<SparseTraversal> {
        if self.kernel == SparseKernelBackend::CompactCsc {
            return self.canonical_out.expand_range(starts, min_hops, max_hops);
        }
        let inner = self.lock_available_replica()?;
        expand_range_with_compiled(adjacency, starts, min_hops, max_hops, &inner)
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn expand_range_count(
        &self,
        adjacency: &Adjacency,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
    ) -> Result<SparseTraversalCount> {
        if self.kernel == SparseKernelBackend::CompactCsc {
            return self
                .canonical_out
                .expand_range_count(starts, min_hops, max_hops);
        }
        let mut inner = self.lock_available_replica()?;
        expand_range_count_with_compiled(adjacency, starts, min_hops, max_hops, &mut inner)
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn expand_range_window(
        &self,
        adjacency: &Adjacency,
        starts: &[VertexId],
        min_hops: u8,
        max_hops: u8,
        window: SparseTraversalWindow,
    ) -> Result<SparseTraversal> {
        if self.kernel == SparseKernelBackend::CompactCsc {
            return self
                .canonical_out
                .expand_range_window(starts, min_hops, max_hops, window);
        }
        let inner = self.lock_available_replica()?;
        expand_range_window_with_compiled(adjacency, starts, min_hops, max_hops, window, &inner)
    }

    #[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
    pub(crate) fn prefer_inline_count(&self, max_hops: u8) -> bool {
        const DEFAULT_INLINE_WORK_EDGES: usize = 50_000;
        let threshold = *INLINE_WORK_EDGES.get_or_init(|| {
            std::env::var("GRAPHBLAS_INLINE_WORK_EDGES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(DEFAULT_INLINE_WORK_EDGES)
        });
        self.edge_count.saturating_mul(usize::from(max_hops)) <= threshold
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
    pub(crate) fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    fn lock_available_replica(&self) -> Result<MutexGuard<'_, CompiledGraphBlasMatrixInner>> {
        let start = self.next_replica.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
        for offset in 0..self.replicas.len() {
            let index = (start + offset) % self.replicas.len();
            match self.replicas[index].try_lock() {
                Ok(inner) => return Ok(inner),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Poisoned(_)) => return Err(graphblas_replica_lock_error()),
            }
        }
        self.replicas[start]
            .lock()
            .map_err(|_| graphblas_replica_lock_error())
    }
}

fn graphblas_replica_lock_error() -> GraphError {
    GraphError::SparseKernel {
        backend: "SuiteSparseGraphBlas",
        reason: "compiled matrix cache lock was poisoned".to_string(),
    }
}

fn compact_csc_resident_bytes(csc: &GraphBlasCsc) -> usize {
    let pointer_bytes = if csc
        .pointers
        .iter()
        .all(|value| u32::try_from(*value).is_ok())
    {
        std::mem::size_of::<u32>()
    } else {
        std::mem::size_of::<u64>()
    };
    let index_bytes = if csc
        .indices
        .iter()
        .all(|value| u32::try_from(*value).is_ok())
    {
        std::mem::size_of::<u32>()
    } else {
        std::mem::size_of::<u64>()
    };
    csc.vertices
        .len()
        .saturating_mul(std::mem::size_of::<VertexId>())
        .saturating_add(csc.pointers.len().saturating_mul(pointer_bytes))
        .saturating_add(csc.indices.len().saturating_mul(index_bytes))
}

fn native_graphblas_resident_bytes(csc: &GraphBlasCsc, replicas: usize) -> usize {
    let per_replica = csc
        .vertices
        .len()
        .saturating_mul(std::mem::size_of::<VertexId>().saturating_mul(2))
        .saturating_add(
            csc.pointers
                .len()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(
            csc.indices
                .len()
                .saturating_mul(std::mem::size_of::<u64>() + std::mem::size_of::<bool>()),
        );
    per_replica.saturating_mul(replicas).saturating_mul(2)
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

#[cfg(test)]
pub(super) fn expand_range(
    adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<SparseTraversal> {
    init()?;

    let csc = graphblas_csc_from_adjacency(adjacency)?;
    let compiled = build_compiled_inner(&csc)?;
    expand_range_with_compiled(adjacency, starts, min_hops, max_hops, &compiled)
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
    let count_scratch = if ordinal_map.is_empty() {
        None
    } else {
        let dimension = ordinal_map.len();
        Some(GraphBlasCountScratch {
            frontier: vector_from_ordinals(dimension, &[])?,
            next: vector_from_ordinals(dimension, &[])?,
            degree: uint64_vector(dimension)?,
            descriptor: exact_count_descriptor()?,
        })
    };
    Ok(CompiledGraphBlasMatrixInner {
        matrix,
        degree_vector,
        ordinal_map,
        count_scratch,
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
        edge_visits += frontier_edge_visits_graphblas(
            degree_vector,
            &frontier,
            &mut degree_scratch,
            unsafe { GrB_DESC_S },
        )?;
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
        backend: SparseKernelBackend::SuiteSparse,
    })
}

fn expand_range_with_compiled(
    _adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
    compiled: &CompiledGraphBlasMatrixInner,
) -> Result<SparseTraversal> {
    let range = range_result_vector(starts, min_hops, max_hops, compiled)?;
    let Some(result) = range.result.as_ref() else {
        return Ok(empty_traversal());
    };
    let ordinals = extract_ordinals(result)?;
    let mut vertices = Vec::with_capacity(ordinals.len());
    for ordinal in ordinals {
        vertices.push(compiled.ordinal_map.vertex(ordinal)?);
    }
    vertices.sort_unstable();
    Ok(SparseTraversal {
        vertices,
        edge_visits: range.edge_visits,
        backend: SparseKernelBackend::SuiteSparse,
    })
}

#[cfg(feature = "opencypher")]
fn expand_range_count_with_compiled(
    _adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
    compiled: &mut CompiledGraphBlasMatrixInner,
) -> Result<SparseTraversalCount> {
    if min_hops == max_hops {
        return exact_hop_count_with_compiled(starts, max_hops, compiled);
    }
    let range = range_result_vector(starts, min_hops, max_hops, compiled)?;
    let Some(result) = range.result.as_ref() else {
        return Ok(SparseTraversalCount {
            vertices: 0,
            edge_visits: range.edge_visits,
            backend: SparseKernelBackend::SuiteSparse,
        });
    };
    Ok(SparseTraversalCount {
        vertices: vector_nvals(result)?,
        edge_visits: range.edge_visits,
        backend: SparseKernelBackend::SuiteSparse,
    })
}

#[cfg(feature = "opencypher")]
fn exact_hop_count_with_compiled(
    starts: &[VertexId],
    hops: u8,
    compiled: &mut CompiledGraphBlasMatrixInner,
) -> Result<SparseTraversalCount> {
    let Some(matrix) = compiled.matrix.as_ref() else {
        return Ok(SparseTraversalCount {
            vertices: 0,
            edge_visits: 0,
            backend: SparseKernelBackend::SuiteSparse,
        });
    };
    let Some(degree_vector) = compiled.degree_vector.as_ref() else {
        return Ok(SparseTraversalCount {
            vertices: 0,
            edge_visits: 0,
            backend: SparseKernelBackend::SuiteSparse,
        });
    };
    if starts.is_empty() || compiled.ordinal_map.is_empty() {
        return Ok(SparseTraversalCount {
            vertices: 0,
            edge_visits: 0,
            backend: SparseKernelBackend::SuiteSparse,
        });
    }

    let mut start_ordinals = Vec::with_capacity(starts.len());
    for start in starts {
        if let Some(ordinal) = compiled.ordinal_map.try_ordinal(*start) {
            start_ordinals.push(ordinal);
        }
    }
    start_ordinals.sort_unstable();
    start_ordinals.dedup();
    if start_ordinals.is_empty() {
        return Ok(SparseTraversalCount {
            vertices: 0,
            edge_visits: 0,
            backend: SparseKernelBackend::SuiteSparse,
        });
    }

    let scratch = compiled
        .count_scratch
        .as_mut()
        .ok_or_else(|| GraphError::SparseKernel {
            backend: "SuiteSparseGraphBlas",
            reason: "compiled count scratch is missing".to_string(),
        })?;
    reset_bool_vector(&mut scratch.frontier, &start_ordinals)?;
    clear_vector(&mut scratch.next)?;
    if hops == 0 {
        return Ok(SparseTraversalCount {
            vertices: start_ordinals.len() as u64,
            edge_visits: 0,
            backend: SparseKernelBackend::SuiteSparse,
        });
    }

    let mut edge_visits = 0_u64;
    for _ in 0..hops {
        edge_visits = edge_visits.saturating_add(frontier_edge_visits_graphblas(
            degree_vector,
            &scratch.frontier,
            &mut scratch.degree,
            scratch.descriptor.0,
        )?);
        multiply_into(
            matrix,
            &scratch.frontier,
            &mut scratch.next,
            scratch.descriptor.0,
        )?;
        let next_count = vector_nvals(&scratch.next)?;
        std::mem::swap(&mut scratch.frontier, &mut scratch.next);
        if next_count == 0 {
            return Ok(SparseTraversalCount {
                vertices: 0,
                edge_visits,
                backend: SparseKernelBackend::SuiteSparse,
            });
        }
    }

    Ok(SparseTraversalCount {
        vertices: vector_nvals(&scratch.frontier)?,
        edge_visits,
        backend: SparseKernelBackend::SuiteSparse,
    })
}

#[cfg(feature = "opencypher")]
fn expand_range_window_with_compiled(
    _adjacency: &Adjacency,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
    window: SparseTraversalWindow,
    compiled: &CompiledGraphBlasMatrixInner,
) -> Result<SparseTraversal> {
    let range = range_result_vector(starts, min_hops, max_hops, compiled)?;
    let Some(result) = range.result.as_ref() else {
        return Ok(empty_traversal());
    };
    let mut ordinals = extract_ordinals(result)?;
    ordinals.sort_unstable();
    if !window.ascending {
        ordinals.reverse();
    }
    let skip = usize::try_from(window.skip).map_err(|_| GraphError::SparseKernel {
        backend: "SuiteSparseGraphBlas",
        reason: format!("window skip {} does not fit in usize", window.skip),
    })?;
    let iter = ordinals.into_iter().skip(skip);
    let selected: Vec<_> = match window.limit {
        Some(limit) => iter.take(limit).collect(),
        None => iter.collect(),
    };
    let mut vertices = Vec::with_capacity(selected.len());
    for ordinal in selected {
        vertices.push(compiled.ordinal_map.vertex(ordinal)?);
    }
    Ok(SparseTraversal {
        vertices,
        edge_visits: range.edge_visits,
        backend: SparseKernelBackend::SuiteSparse,
    })
}

struct RangeVector {
    result: Option<Vector>,
    edge_visits: u64,
}

fn range_result_vector(
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
    compiled: &CompiledGraphBlasMatrixInner,
) -> Result<RangeVector> {
    let Some(matrix) = compiled.matrix.as_ref() else {
        return Ok(empty_range_vector());
    };
    let Some(degree_vector) = compiled.degree_vector.as_ref() else {
        return Ok(empty_range_vector());
    };
    if min_hops > max_hops || compiled.ordinal_map.is_empty() || starts.is_empty() {
        return Ok(empty_range_vector());
    }

    let mut start_ordinals = Vec::with_capacity(starts.len());
    let mut seen_start = vec![false; compiled.ordinal_map.len() as usize];
    for start in starts {
        if let Some(ordinal) = compiled.ordinal_map.try_ordinal(*start) {
            let idx = ordinal as usize;
            if !seen_start[idx] {
                seen_start[idx] = true;
                start_ordinals.push(ordinal);
            }
        }
    }
    if start_ordinals.is_empty() {
        return Ok(empty_range_vector());
    }

    let dimension = compiled.ordinal_map.len();
    let mut frontier = vector_from_ordinals(dimension, &start_ordinals)?;
    let mut result = if min_hops == 0 {
        vector_from_ordinals(dimension, &start_ordinals)?
    } else {
        vector_from_ordinals(dimension, &[])?
    };
    let mut degree_scratch = uint64_vector(dimension)?;
    let mut edge_visits = 0_u64;

    for depth in 1..=max_hops {
        edge_visits = edge_visits.saturating_add(frontier_edge_visits_graphblas(
            degree_vector,
            &frontier,
            &mut degree_scratch,
            unsafe { GrB_DESC_S },
        )?);
        let next = multiply(matrix, &frontier, dimension)?;
        let next_count = vector_nvals(&next)?;
        if depth >= min_hops {
            union_into(&mut result, &next)?;
        }
        if next_count == 0 {
            break;
        }
        frontier = next;
    }

    Ok(RangeVector {
        result: Some(result),
        edge_visits,
    })
}

fn empty_range_vector() -> RangeVector {
    RangeVector {
        result: None,
        edge_visits: 0,
    }
}

fn empty_traversal() -> SparseTraversal {
    SparseTraversal {
        vertices: Vec::new(),
        edge_visits: 0,
        backend: SparseKernelBackend::SuiteSparse,
    }
}

fn init() -> Result<()> {
    match INIT.get_or_init(|| unsafe {
        let info = GrB_init(GRB_BLOCKING);
        if info != GRB_SUCCESS {
            return Err(format!("GrB_init returned GraphBLAS status {info}"));
        }
        // Only applied when explicitly requested; otherwise GraphBLAS keeps its
        // own default (one thread per core). This used to be a descriptor field
        // scoped to the exact-count path, which v9 no longer accepts.
        if let Some(threads) = std::env::var("GRAPHBLAS_NTHREADS")
            .or_else(|_| std::env::var("GRAPHBLAS_EXACT_COUNT_THREADS"))
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|value| *value > 0)
        {
            let mut info = GxB_Global_Option_set_INT32(GXB_NTHREADS, threads);
            if info == GRB_INVALID_VALUE {
                // Pre-v9 library: the field is the same one under its old number.
                info = GxB_Global_Option_set_INT32(GXB_NTHREADS_LEGACY, threads);
            }
            if info != GRB_SUCCESS {
                return Err(format!(
                    "GxB_Global_Option_set_INT32(GxB_NTHREADS) returned GraphBLAS status {info}"
                ));
            }
        }
        Ok(())
    }) {
        Ok(()) => Ok(()),
        Err(reason) => Err(GraphError::SparseKernel {
            backend: "SuiteSparseGraphBlas",
            reason: reason.clone(),
        }),
    }
}

/// The legacy `GRAPH_COMPILED_KERNEL` switch, retained as a fallback default.
///
/// Compilation no longer consults it — constructors take the kernel explicitly.
/// It is read once, by `super::default_matrix_kernel`, and only when the policy
/// has been left at its default, so benchmark scripts keep working.
pub(crate) fn use_compact_csc_kernel() -> bool {
    std::env::var("GRAPH_COMPILED_KERNEL").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "compact" | "csc" | "compact-csc" | "rust-csc"
        )
    })
}

fn graphblas_replica_count(csc: &GraphBlasCsc) -> usize {
    const DEFAULT_MAX_REPLICAS: usize = 4;
    const HARD_MAX_REPLICAS: usize = 64;
    const DEFAULT_REPLICA_MEMORY_BUDGET_BYTES: usize = 16 * 1024 * 1024;
    if let Ok(value) = std::env::var("GRAPHBLAS_REPLICAS") {
        if let Ok(parsed) = value.parse::<usize>() {
            return parsed.clamp(1, HARD_MAX_REPLICAS);
        }
    }
    let cpu_limit = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().clamp(1, DEFAULT_MAX_REPLICAS))
        .unwrap_or(1);
    let memory_budget = std::env::var("GRAPHBLAS_REPLICA_MEMORY_BUDGET_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_REPLICA_MEMORY_BUDGET_BYTES);
    let one_replica_bytes = native_graphblas_resident_bytes(csc, 1).max(1);
    let memory_limit = (memory_budget / one_replica_bytes).max(1);
    cpu_limit.min(memory_limit)
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

#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn clear_vector(vector: &mut Vector) -> Result<()> {
    unsafe {
        check(GrB_Vector_clear(vector.0), "GrB_Vector_clear")?;
    }
    Ok(())
}

#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn reset_bool_vector(vector: &mut Vector, ordinals: &[GrBIndex]) -> Result<()> {
    clear_vector(vector)?;
    if ordinals.is_empty() {
        return Ok(());
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
    Ok(())
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

fn multiply(matrix: &Matrix, input: &Vector, dimension: GrBIndex) -> Result<Vector> {
    let mut raw = null_mut();
    unsafe {
        check(
            GrB_Vector_new(&mut raw, GrB_BOOL, dimension),
            "GrB_Vector_new",
        )?;
        check(
            GrB_mxv(
                raw,
                null_mut(),
                null_mut(),
                GrB_LOR_LAND_SEMIRING_BOOL,
                matrix.0,
                input.0,
                null_mut(),
            ),
            "GrB_mxv",
        )?;
    }
    Ok(Vector(raw))
}

#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn multiply_into(
    matrix: &Matrix,
    input: &Vector,
    output: &mut Vector,
    descriptor: GrBDescriptor,
) -> Result<()> {
    clear_vector(output)?;
    unsafe {
        check(
            GrB_mxv(
                output.0,
                null_mut(),
                null_mut(),
                GrB_LOR_LAND_SEMIRING_BOOL,
                matrix.0,
                input.0,
                descriptor,
            ),
            "GrB_mxv",
        )?;
    }
    Ok(())
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
    descriptor: GrBDescriptor,
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
                descriptor,
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
                descriptor,
            ),
            "GrB_Vector_reduce_UINT64",
        )?;
        Ok(edge_visits)
    }
}

fn exact_count_descriptor() -> Result<Descriptor> {
    let mut descriptor = null_mut();
    unsafe {
        check(GrB_Descriptor_new(&mut descriptor), "GrB_Descriptor_new")?;
    }
    Ok(Descriptor(descriptor))
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

fn validate_compact_u32_csc(
    vertices: &[VertexId],
    pointers: &[u32],
    indices: &[u32],
) -> Result<()> {
    if pointers.len() != vertices.len() + 1 {
        return Err(GraphError::SparseKernel {
            backend: "CompactCsc",
            reason: format!(
                "CSC pointer count {} does not match vertex count {}",
                pointers.len(),
                vertices.len()
            ),
        });
    }
    if pointers.first().copied() != Some(0) {
        return Err(GraphError::SparseKernel {
            backend: "CompactCsc",
            reason: "CSC first pointer must be zero".to_string(),
        });
    }
    for window in pointers.windows(2) {
        if window[0] > window[1] {
            return Err(GraphError::SparseKernel {
                backend: "CompactCsc",
                reason: "CSC pointers must be monotonic".to_string(),
            });
        }
    }
    if pointers.last().copied().unwrap_or(0) as usize != indices.len() {
        return Err(GraphError::SparseKernel {
            backend: "CompactCsc",
            reason: format!(
                "CSC edge count {} does not match index count {}",
                pointers.last().copied().unwrap_or(0),
                indices.len()
            ),
        });
    }
    if let Some(index) = indices
        .iter()
        .copied()
        .find(|index| *index as usize >= vertices.len())
    {
        return Err(GraphError::SparseKernel {
            backend: "CompactCsc",
            reason: format!("CSC row index {index} exceeds dimension {}", vertices.len()),
        });
    }
    for window in vertices.windows(2) {
        if window[0] >= window[1] {
            return Err(GraphError::SparseKernel {
                backend: "CompactCsc",
                reason: "CSC vertices must be sorted and unique".to_string(),
            });
        }
    }
    Ok(())
}

struct OrdinalMap {
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
        Ok(Self { by_ordinal })
    }

    fn len(&self) -> GrBIndex {
        self.by_ordinal.len() as GrBIndex
    }

    fn is_empty(&self) -> bool {
        self.by_ordinal.is_empty()
    }

    fn try_ordinal(&self, vertex: VertexId) -> Option<GrBIndex> {
        self.by_ordinal
            .binary_search(&vertex)
            .ok()
            .map(|idx| idx as GrBIndex)
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
