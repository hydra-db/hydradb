//! Whole-graph centrality over LAGraph.
//!
//! Both entry points take the graph as compacted ordinal pairs `0..vertices`,
//! build their own `LAGraph_Graph` from them and free every handle before
//! returning. Nothing is cached between calls: the caller assembles the pairs
//! from one index generation, so a cache would have to be keyed on it.
//!
//! PageRank is one `LAGr_PageRank` call, which handles sinks so scores sum to
//! 1, and which has no result to give when `itermax` passes unconverged.
//! Betweenness is `LAGr_Betweenness`, one batch of sources at a time.

use std::ffi::{c_char, c_void, CStr};
use std::os::raw::c_int;
use std::ptr::null_mut;

use super::graphblas::{
    check, init, GrBBinaryOp, GrBIndex, GrBInfo, GrBMatrix, GrBType, GrBVector, GrB_Matrix_new, GrB_Vector_nvals, Matrix, Vector,
};
use crate::{GraphError, Result};

type LAGraphGraph = *mut c_void;

const LAGRAPH_MSG_LEN: usize = 256;
const LAGRAPH_ADJACENCY_DIRECTED: c_int = 1;
const LAGRAPH_CONVERGENCE_FAILURE: c_int = -1005;

// Linux links the static archive `scripts/install_lagraph.sh` builds, and its
// objects are compiled with OpenMP, so the archive's GOMP references need
// libgomp named on the link line. libgraphblas already depends on it, so the
// runtime image has it.
#[link(name = "lagraph")]
#[cfg_attr(target_os = "linux", link(name = "gomp"))]
unsafe extern "C" {
    fn LAGr_Init(
        mode: c_int,
        user_malloc: Option<unsafe extern "C" fn(usize) -> *mut c_void>,
        user_calloc: Option<unsafe extern "C" fn(usize, usize) -> *mut c_void>,
        user_realloc: Option<unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void>,
        user_free: Option<unsafe extern "C" fn(*mut c_void)>,
        msg: *mut c_char,
    ) -> c_int;
    fn LAGraph_New(graph: *mut LAGraphGraph, matrix: *mut GrBMatrix, kind: c_int, msg: *mut c_char) -> c_int;
    fn LAGraph_Delete(graph: *mut LAGraphGraph, msg: *mut c_char) -> c_int;
    fn LAGraph_Cached_AT(graph: LAGraphGraph, msg: *mut c_char) -> c_int;
    fn LAGraph_Cached_OutDegree(graph: LAGraphGraph, msg: *mut c_char) -> c_int;
    fn LAGr_PageRank(
        centrality: *mut GrBVector,
        iterations: *mut c_int,
        graph: LAGraphGraph,
        damping: f32,
        tolerance: f32,
        itermax: c_int,
        msg: *mut c_char,
    ) -> c_int;
    fn LAGr_Betweenness(centrality: *mut GrBVector, graph: LAGraphGraph, sources: *const GrBIndex, ns: i32, msg: *mut c_char) -> c_int;
}

#[link(name = "graphblas")]
unsafe extern "C" {
    static GrB_FP32: GrBType;
    static GrB_FP64: GrBType;
    static GrB_FIRST_FP32: GrBBinaryOp;
    static GrB_FIRST_FP64: GrBBinaryOp;

    fn GrB_Matrix_build_FP32(
        matrix: GrBMatrix,
        rows: *const GrBIndex,
        cols: *const GrBIndex,
        values: *const f32,
        nvals: GrBIndex,
        dup: GrBBinaryOp,
    ) -> GrBInfo;
    fn GrB_Matrix_build_FP64(
        matrix: GrBMatrix,
        rows: *const GrBIndex,
        cols: *const GrBIndex,
        values: *const f64,
        nvals: GrBIndex,
        dup: GrBBinaryOp,
    ) -> GrBInfo;
    fn GrB_Vector_extractTuples_FP32(indices: *mut GrBIndex, values: *mut f32, nvals: *mut GrBIndex, vector: GrBVector) -> GrBInfo;
    fn GrB_Vector_extractTuples_FP64(indices: *mut GrBIndex, values: *mut f64, nvals: *mut GrBIndex, vector: GrBVector) -> GrBInfo;
}

/// A directed graph as ordinal pairs: `sources[i] -> targets[i]`, each below
/// `vertices`. Duplicate pairs collapse to one edge.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct OrdinalEdges {
    pub(crate) vertices: u64,
    pub(crate) sources: Vec<u64>,
    pub(crate) targets: Vec<u64>,
}

impl OrdinalEdges {
    pub(crate) fn new(vertices: u64) -> Self {
        Self {
            vertices,
            ..Self::default()
        }
    }

    pub(crate) fn push(&mut self, source: u64, target: u64) {
        self.sources.push(source);
        self.targets.push(target);
    }

    pub(crate) fn len(&self) -> usize {
        self.sources.len()
    }

    /// The edges the kernels compute on: the matrix is built with a FIRST
    /// duplicate operator, so a pair pushed twice (two projected types that
    /// both hold it, or an undirected projection of a pair stored both ways)
    /// is one edge. Counted on a sorted copy, so it costs a pass over the
    /// pairs and is meant for a count reported once, not per call.
    pub(crate) fn distinct_pairs(&self) -> u64 {
        let mut pairs = self.sources.iter().copied().zip(self.targets.iter().copied()).collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs.dedup();
        pairs.len() as u64
    }
}

struct Graph(LAGraphGraph);

impl Drop for Graph {
    fn drop(&mut self) {
        let mut msg = [0 as c_char; LAGRAPH_MSG_LEN];
        unsafe {
            let _ = LAGraph_Delete(&mut self.0, msg.as_mut_ptr());
        }
    }
}

/// `LAGr_Init` in place of `GrB_init`: it initializes GraphBLAS itself, with
/// the C allocator, and LAGraph's own semirings and state on top. Either can
/// be initialized only once per process.
pub(super) fn initialize(mode: c_int) -> std::result::Result<(), String> {
    let mut msg = [0 as c_char; LAGRAPH_MSG_LEN];
    let info = unsafe {
        LAGr_Init(
            mode,
            Some(libc::malloc),
            Some(libc::calloc),
            Some(libc::realloc),
            Some(libc::free),
            msg.as_mut_ptr(),
        )
    };
    if info < 0 {
        return Err(format!("LAGr_Init returned LAGraph status {info}: {}", message(&msg)));
    }
    Ok(())
}

/// PageRank scores by ordinal, summing to 1, once the L1 change between
/// iterations is at most `tolerance`, and the iterations that took. `None`
/// when `max_iterations` pass first.
pub(crate) fn pagerank(edges: &OrdinalEdges, damping: f32, tolerance: f32, max_iterations: u32) -> Result<Option<(Vec<f32>, u32)>> {
    let n = edges.vertices;
    if n == 0 {
        return Ok(Some((Vec::new(), 0)));
    }
    let graph = lagraph_graph(edges, EdgeValues::Fp32)?;
    let mut msg = [0 as c_char; LAGRAPH_MSG_LEN];
    unsafe {
        check_lagraph(
            LAGraph_Cached_OutDegree(graph.0, msg.as_mut_ptr()),
            "LAGraph_Cached_OutDegree",
            &msg,
        )?;
    }
    let mut centrality = Vector(null_mut());
    let mut iterations = 0;
    let info = unsafe {
        LAGr_PageRank(
            &mut centrality.0,
            &mut iterations,
            graph.0,
            damping,
            tolerance,
            c_int::try_from(max_iterations).unwrap_or(c_int::MAX),
            msg.as_mut_ptr(),
        )
    };
    if info == LAGRAPH_CONVERGENCE_FAILURE {
        return Ok(None);
    }
    check_lagraph(info, "LAGr_PageRank", &msg)?;
    Ok(Some((dense_fp32(&centrality, n)?, u32::try_from(iterations).unwrap_or(0))))
}

/// Betweenness centrality summed over `sources`, counting each ordered
/// source/target pair. Every vertex as a source gives the exact score; a
/// subset gives the sampled estimate.
///
/// Sources go to `LAGr_Betweenness` `batch` at a time (zero is taken as
/// one). Its working set grows with the batch, as
/// [`betweenness_source_bytes`] counts, so [`betweenness_batch`] sizes it
/// to the memory free; each source's contribution is computed on its own,
/// so the scores do not depend on it. Only one batch of `sources` is held at
/// a time. `check_budget` runs before every batch.
pub(crate) fn betweenness(
    edges: &OrdinalEdges,
    sources: impl IntoIterator<Item = u64>,
    batch: usize,
    mut check_budget: impl FnMut() -> Result<()>,
) -> Result<Vec<f64>> {
    let n = edges.vertices;
    let mut totals = vec![0.0_f64; usize::try_from(n).unwrap_or(usize::MAX)];
    let mut sources = sources.into_iter().peekable();
    if n == 0 || sources.peek().is_none() {
        return Ok(totals);
    }
    let graph = lagraph_graph(edges, EdgeValues::Fp64)?;
    let mut msg = [0 as c_char; LAGRAPH_MSG_LEN];
    // LAGraph takes the batch's length as an `i32`, so no batch is longer.
    let batch = batch.clamp(1, i32::MAX as usize);
    let mut ordinals = Vec::new();
    loop {
        ordinals.clear();
        ordinals.extend(sources.by_ref().take(batch));
        if ordinals.is_empty() {
            break;
        }
        check_budget()?;
        let mut centrality = Vector(null_mut());
        unsafe {
            check_lagraph(
                LAGr_Betweenness(
                    &mut centrality.0,
                    graph.0,
                    ordinals.as_ptr(),
                    ordinals.len() as i32,
                    msg.as_mut_ptr(),
                ),
                "LAGr_Betweenness",
                &msg,
            )?;
        }
        let (indices, values) = extract_fp64(&centrality)?;
        for (index, value) in indices.into_iter().zip(values) {
            totals[index as usize] += value;
        }
    }
    Ok(totals)
}

/// The most sources one `LAGr_Betweenness` call takes, however much memory
/// is free: past it the deadline would be checked too rarely.
const BETWEENNESS_MAX_BATCH: usize = 16;

/// Bytes `LAGr_Betweenness` holds for each source in a batch, at its peak in
/// the backtracking phase, as LAGraph 1.0.2 (`scripts/install_lagraph.sh`)
/// allocates them in `src/algorithm/LAGr_Betweenness.c`. Each matrix is
/// `sources x vertices`; FP64 is 8 bytes, and a bitmap entry adds 1:
///
/// - `paths`, FP64, bitmap or full: 9 per vertex.
/// - `bc_update`, FP64, full: 8 per vertex.
/// - `W`, FP64 workspace, bitmap on pull steps: 9 per vertex.
/// - the `W * A'` product before it replaces `W`, bitmap: 9 per vertex.
/// - `S[depth]`, one boolean structure per BFS level: a reached vertex sits
///   in one level, as an 8-byte index. A source reaches at most every vertex,
///   and at most one more vertex than there are edges.
///
/// The BFS phase's `frontier` is freed before the backtracking starts, and
/// what does not grow with the batch is [`betweenness_graph_bytes`].
fn betweenness_source_bytes(vertices: u64, edges: u64) -> u64 {
    let reached = vertices.min(edges.saturating_add(1));
    vertices.saturating_mul(9 + 8 + 9 + 9).saturating_add(reached.saturating_mul(8))
}

/// Bytes [`betweenness`] holds once per call, whatever the batch, while the
/// per-source matrices exist. FP64 and INT64 are 8 bytes, and a sparse
/// entry is an 8-byte index and its value:
///
/// - `totals`, FP64 per vertex: 8 per vertex.
/// - `A`, the FP64 matrix [`lagraph_graph`] builds from the pairs, by row,
///   with 16 per edge and [`betweenness_matrix_vector_bytes`] for its rows.
/// - `AT`, which `LAGr_Betweenness` requires of a directed graph and
///   `LAGraph_Cached_AT` transposes from `A`: the same again.
/// - the centrality vector `LAGr_Betweenness` assigns for every vertex,
///   full FP64: 8 per vertex.
///
/// Extracting that vector, 16 bytes per vertex, happens once the batch's
/// matrices are freed, which leaves more than that free. The pairs are
/// already resident, so not part of what is available, and GraphBLAS's
/// workspace while building `A` and `AT` comes before any per-source matrix
/// exists.
fn betweenness_graph_bytes(vertices: u64, edges: u64) -> u64 {
    let matrix = betweenness_matrix_vector_bytes(vertices, edges).saturating_add(edges.saturating_mul(16));
    vertices.saturating_mul(8 + 8).saturating_add(matrix.saturating_mul(2))
}

/// The bytes `A` or `AT` spends on its rows, beyond 16 per entry. A
/// sparse matrix holds a row pointer per row and one more, 8 bytes each; a
/// hypersparse one holds a row index and pointer per non-empty row, 16
/// bytes each. GraphBLAS makes a matrix hypersparse once its non-empty rows
/// are at most its rows times `GxB_HYPER_SWITCH` (the rule `GraphBLAS.h`
/// documents under `GxB_HYPER_SWITCH`), whose default `GxB_HYPER_DEFAULT`
/// is 1/16 and which nothing here changes. Non-empty rows are at most one
/// per vertex and one per edge, so a bound within the switch is hypersparse
/// for certain; past it the matrix can be either, and the larger is
/// counted.
fn betweenness_matrix_vector_bytes(vertices: u64, edges: u64) -> u64 {
    let rows = vertices.min(edges);
    let hypersparse = rows.saturating_mul(16);
    if vertices > 1 && rows.saturating_mul(16) <= vertices {
        hypersparse
    } else {
        vertices.saturating_add(1).saturating_mul(8).max(hypersparse)
    }
}

/// Sources per `LAGr_Betweenness` call over `vertices` and `edges`: as many
/// as fit in three quarters of `available` bytes, so the kernel never takes
/// the last of it, once [`betweenness_graph_bytes`] are set aside, up to
/// [`BETWEENNESS_MAX_BATCH`]. With nothing known about free memory there is
/// no bound to fit. A graph whose fixed costs, or whose fixed costs and one
/// source, do not fit is refused up front rather than run out of memory part
/// way.
pub(crate) fn betweenness_batch(vertices: u64, edges: u64, available: Option<u64>) -> Result<usize> {
    let Some(available) = available else {
        return Ok(BETWEENNESS_MAX_BATCH);
    };
    let budget = available / 4 * 3;
    let graph = betweenness_graph_bytes(vertices, edges);
    let Some(budget) = budget.checked_sub(graph) else {
        return Err(GraphError::AdmissionRejected {
            operation: "gds_betweenness_graph_bytes",
            actual: graph,
            limit: budget,
        });
    };
    let per_source = betweenness_source_bytes(vertices, edges).max(1);
    match budget / per_source {
        0 => Err(GraphError::AdmissionRejected {
            operation: "gds_betweenness_source_bytes",
            actual: per_source,
            limit: budget,
        }),
        fits => Ok(usize::try_from(fits).unwrap_or(usize::MAX).min(BETWEENNESS_MAX_BATCH)),
    }
}

/// The type an adjacency's entries are stored in. Every entry is 1, in the
/// type the algorithm computes in: PageRank works in FP32, betweenness in
/// FP64, so their products need no typecast.
#[derive(Clone, Copy)]
enum EdgeValues {
    Fp32,
    Fp64,
}

/// The adjacency moved into an `LAGraph_Graph`, with the transpose both
/// advanced algorithms require already cached. PageRank caches the
/// out-degree it also requires itself, so betweenness does not hold one.
/// Duplicate pairs collapse to one edge: the first of equal values is kept.
fn lagraph_graph(edges: &OrdinalEdges, values: EdgeValues) -> Result<Graph> {
    init()?;
    let n = edges.vertices;
    let value_type = match values {
        EdgeValues::Fp32 => unsafe { GrB_FP32 },
        EdgeValues::Fp64 => unsafe { GrB_FP64 },
    };
    let mut matrix = Matrix(null_mut());
    unsafe {
        check(GrB_Matrix_new(&mut matrix.0, value_type, n, n), "GrB_Matrix_new")?;
    }
    if edges.len() > 0 {
        let rows = edges.sources.as_ptr();
        let cols = edges.targets.as_ptr();
        let nvals = edges.len() as GrBIndex;
        unsafe {
            match values {
                EdgeValues::Fp32 => check(
                    GrB_Matrix_build_FP32(matrix.0, rows, cols, vec![1.0_f32; edges.len()].as_ptr(), nvals, GrB_FIRST_FP32),
                    "GrB_Matrix_build_FP32",
                )?,
                EdgeValues::Fp64 => check(
                    GrB_Matrix_build_FP64(matrix.0, rows, cols, vec![1.0_f64; edges.len()].as_ptr(), nvals, GrB_FIRST_FP64),
                    "GrB_Matrix_build_FP64",
                )?,
            }
        }
    }
    let mut msg = [0 as c_char; LAGRAPH_MSG_LEN];
    let mut graph = Graph(null_mut());
    unsafe {
        // Takes ownership of the matrix and leaves the handle NULL, so the
        // guard's free afterwards is a no-op.
        check_lagraph(
            LAGraph_New(&mut graph.0, &mut matrix.0, LAGRAPH_ADJACENCY_DIRECTED, msg.as_mut_ptr()),
            "LAGraph_New",
            &msg,
        )?;
        check_lagraph(LAGraph_Cached_AT(graph.0, msg.as_mut_ptr()), "LAGraph_Cached_AT", &msg)?;
    }
    Ok(graph)
}

/// Every position of a vector, zero where it holds no entry.
fn dense_fp32(vector: &Vector, n: GrBIndex) -> Result<Vec<f32>> {
    let mut nvals = 0;
    unsafe {
        check(GrB_Vector_nvals(&mut nvals, vector.0), "GrB_Vector_nvals")?;
    }
    let mut indices = vec![0; nvals as usize];
    let mut values = vec![0.0_f32; nvals as usize];
    let mut capacity = nvals;
    unsafe {
        check(
            GrB_Vector_extractTuples_FP32(indices.as_mut_ptr(), values.as_mut_ptr(), &mut capacity, vector.0),
            "GrB_Vector_extractTuples_FP32",
        )?;
    }
    let mut dense = vec![0.0_f32; n as usize];
    for (index, value) in indices.into_iter().zip(values).take(capacity as usize) {
        dense[index as usize] = value;
    }
    Ok(dense)
}

fn extract_fp64(vector: &Vector) -> Result<(Vec<GrBIndex>, Vec<f64>)> {
    let mut nvals = 0;
    unsafe {
        check(GrB_Vector_nvals(&mut nvals, vector.0), "GrB_Vector_nvals")?;
    }
    let mut indices = vec![0; nvals as usize];
    let mut values = vec![0.0_f64; nvals as usize];
    let mut capacity = nvals;
    unsafe {
        check(
            GrB_Vector_extractTuples_FP64(indices.as_mut_ptr(), values.as_mut_ptr(), &mut capacity, vector.0),
            "GrB_Vector_extractTuples_FP64",
        )?;
    }
    indices.truncate(capacity as usize);
    values.truncate(capacity as usize);
    Ok((indices, values))
}

/// LAGraph statuses: negative is an error, positive a warning such as
/// `LAGRAPH_CACHE_NOT_NEEDED`.
fn check_lagraph(info: c_int, operation: &'static str, msg: &[c_char; LAGRAPH_MSG_LEN]) -> Result<()> {
    if info >= 0 {
        return Ok(());
    }
    Err(GraphError::SparseKernel {
        backend: "LAGraph",
        reason: format!("{operation} returned LAGraph status {info}: {}", message(msg)),
    })
}

fn message(msg: &[c_char; LAGRAPH_MSG_LEN]) -> String {
    let mut terminated = *msg;
    terminated[LAGRAPH_MSG_LEN - 1] = 0;
    unsafe { CStr::from_ptr(terminated.as_ptr()) }.to_string_lossy().into_owned()
}
