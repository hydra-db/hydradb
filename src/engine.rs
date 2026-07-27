use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "query-transport")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::{local::LocalFileSystem, ObjectStore};

use crate::sparse_kernel::{
    compile_graphblas_compact_csc_u32, compile_graphblas_csc, compile_graphblas_csc_owned,
    compile_graphblas_matrix, expand_compiled_graphblas, CompiledGraphBlasMatrix,
};
use crate::sparse_kernel::{
    default_matrix_kernel, expand as expand_sparse, graphblas_csc_from_adjacency, GraphBlasCsc,
    SparseKernelBackend,
};
use crate::{
    decode_edge_record, decode_out_edge_segment, decode_relationship_record, decode_u64,
    encode_vertex_property_value_key, ensure_limit, parse_out_edge_segment_tombstone_key,
    parse_u64, segment_edge_visible, validate_component, EdgeRecord, GraphCacheEntryCounts,
    GraphCacheKind, GraphCacheMetricsSnapshot, GraphCacheResidentBytes, GraphCorrectnessReport,
    GraphError, GraphExportDigest, GraphMemoryConfig, GraphOpenOptions,
    GraphOperationalMetricsSnapshot, GraphScope, GraphShard, GraphStore, GraphWriteBatch,
    GraphWriteGuard, LocalWriteGuard, MatrixAdjacency, MatrixCacheKey, RelationshipId,
    RelationshipRecord, Result, StorageSequence, VertexId,
};
#[cfg(feature = "query-transport")]
use crate::{GraphId, NamespacePath};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum MatrixDirection {
    Out,
    In,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixArtifact {
    pub cell_id: String,
    pub edge_type: String,
    pub base_epoch: StorageSequence,
    pub tile_size: u64,
    pub out_tiles: u64,
    pub transpose_tiles: u64,
    pub edge_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraversalBackend {
    DirectSnapshot,
    MatrixSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixTraversalResult {
    pub backend: TraversalBackend,
    pub vertices: Vec<VertexId>,
    pub hops: u8,
    pub base_epoch: StorageSequence,
    pub edge_visits: u64,
    pub sparse_kernel: SparseKernelBackend,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkResult {
    pub direct: MatrixTraversalResult,
    pub matrix: MatrixTraversalResult,
    pub matrix_wins: bool,
}

pub struct GraphCluster {
    scope: GraphScope,
    shards: BTreeMap<String, GraphShard>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "query-transport",
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct ObjectStoreNodeDirectory {
    cells: BTreeSet<String>,
    nodes: BTreeSet<String>,
}

pub struct RoutedGraphCluster {
    scope: GraphScope,
    local_node_id: String,
    directory: ObjectStoreNodeDirectory,
    /// Who owns each cell's writer. A clone of the process-wide handle, shared
    /// with the Bolt routing provider — see `engine::placement`.
    placement: PlacementView,
    shards: BTreeMap<String, Arc<GraphShard>>,
    promotable: bool,
}

#[cfg(feature = "query-transport")]
struct ScopedRoutedClusterEntry {
    cluster: Arc<RoutedGraphCluster>,
    last_used: u64,
}

#[cfg(feature = "query-transport")]
pub struct ScopedRoutedGraphCluster {
    base_path: String,
    root_namespace: NamespacePath,
    graph_id: GraphId,
    local_node_id: String,
    directory: ObjectStoreNodeDirectory,
    placement: PlacementView,
    object_store: Arc<dyn ObjectStore>,
    scope_directory: ObjectStoreGraphScopeDirectory,
    options: GraphOpenOptions,
    memory: GraphMemoryConfig,
    max_open_scopes: usize,
    access_clock: AtomicU64,
    clusters: tokio::sync::Mutex<BTreeMap<GraphScope, ScopedRoutedClusterEntry>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphShardRuntimeMetrics {
    pub cell_id: String,
    pub operational: GraphOperationalMetricsSnapshot,
    pub cache: GraphCacheMetricsSnapshot,
    pub cache_entries: GraphCacheEntryCounts,
    pub cache_resident_bytes: GraphCacheResidentBytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedGraphShardRuntimeMetrics {
    pub scope: GraphScope,
    pub shard: GraphShardRuntimeMetrics,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactGcResult {
    pub deleted_keys: u64,
    pub retained_keys: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatrixTile {
    cell_id: String,
    edge_type: String,
    base_epoch: StorageSequence,
    tile_size: u64,
    direction: MatrixDirection,
    tile_row: u64,
    tile_col: u64,
    rows: BTreeMap<VertexId, Vec<VertexId>>,
}

struct TraversalVerifyRequest<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    read_epoch: StorageSequence,
    max_hops: u8,
    root_limit: usize,
    edges: &'a [EdgeRecord],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphBlasCscManifest {
    cell_id: String,
    edge_type: String,
    base_epoch: StorageSequence,
    chunk_size: u64,
    vertices_len: u64,
    pointers_len: u64,
    indices_len: u64,
    vertex_chunks: u64,
    pointer_chunks: u64,
    index_chunks: u64,
    checksum: u64,
}

#[derive(Default)]
struct MatrixRows {
    rows: BTreeMap<VertexId, Vec<VertexId>>,
    raw_edges: u64,
    live_edges: u64,
}

impl MatrixRows {
    fn push(&mut self, src: VertexId, dst: VertexId) {
        self.rows.entry(src).or_default().push(dst);
        self.raw_edges = self.raw_edges.saturating_add(1);
    }

    fn normalize(&mut self) {
        let mut live_edges = 0_u64;
        self.rows.retain(|_, destinations| {
            destinations.sort_unstable();
            destinations.dedup();
            live_edges = live_edges.saturating_add(destinations.len() as u64);
            !destinations.is_empty()
        });
        self.live_edges = live_edges;
    }

    fn to_adjacency(&self) -> MatrixAdjacency {
        self.rows
            .iter()
            .map(|(src, destinations)| (*src, destinations.iter().copied().collect()))
            .collect()
    }
}

mod artifact_build;
mod artifact_gc;
mod cluster;
mod index_store;
mod placement;
mod scope_directory;

pub use placement::{CellOwnership, PlacementConfig, PlacementRefreshHandle, PlacementView};

pub use scope_directory::ObjectStoreGraphScopeDirectory;
mod matrix_cache;
mod traversal;
mod verify;

pub use index_store::GraphIndexGeneration;

pub fn local_object_store(path: impl AsRef<std::path::Path>) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(path.as_ref())?) as Arc<dyn ObjectStore>)
}

pub fn object_store_from_env(env_file: Option<String>) -> Result<Arc<dyn ObjectStore>> {
    Ok(slatedb::admin::load_object_store_from_env(env_file)?)
}

fn matrix_rows_to_graphblas_csc(rows: &MatrixRows) -> Result<GraphBlasCsc> {
    let vertices = matrix_rows_vertices(rows);
    let pointers = matrix_rows_pointers(rows, &vertices)?;
    let indices = matrix_rows_indices(rows, &vertices)?;
    Ok(GraphBlasCsc {
        vertices,
        pointers,
        indices,
    })
}

fn matrix_rows_vertices(rows: &MatrixRows) -> Vec<VertexId> {
    let mut vertices = Vec::new();
    for (src, dsts) in &rows.rows {
        vertices.push(*src);
        vertices.extend(dsts.iter().copied());
    }
    vertices.sort_unstable();
    vertices.dedup();
    vertices
}

fn matrix_rows_pointers(rows: &MatrixRows, vertices: &[VertexId]) -> Result<Vec<u64>> {
    let mut pointers = Vec::with_capacity(vertices.len() + 1);
    let mut offset = 0_u64;
    pointers.push(offset);
    for vertex in vertices {
        let degree = rows.rows.get(vertex).map_or(0_usize, Vec::len);
        offset = offset
            .checked_add(degree as u64)
            .ok_or_else(|| GraphError::CorruptValue {
                key: "graphblas_csc_pointers".to_string(),
                reason: "CSC pointer offset overflow".to_string(),
            })?;
        pointers.push(offset);
    }
    Ok(pointers)
}

fn matrix_rows_indices(rows: &MatrixRows, vertices: &[VertexId]) -> Result<Vec<u64>> {
    let mut indices = Vec::with_capacity(rows.live_edges as usize);
    append_matrix_rows_indices(rows, vertices, |ordinal| {
        indices.push(ordinal);
        Ok(())
    })?;
    Ok(indices)
}

fn append_matrix_rows_indices(
    rows: &MatrixRows,
    vertices: &[VertexId],
    mut append: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    for src in vertices {
        let Some(dsts) = rows.rows.get(src) else {
            continue;
        };
        for dst in dsts {
            let ordinal = vertices
                .binary_search(dst)
                .map_err(|_| GraphError::CorruptValue {
                    key: "graphblas_csc_indices".to_string(),
                    reason: format!("destination vertex {dst} missing from CSC vertex dictionary"),
                })? as u64;
            append(ordinal)?;
        }
    }
    Ok(())
}

fn adjacency_from_edges(edges: &[EdgeRecord]) -> MatrixAdjacency {
    let mut adjacency = MatrixAdjacency::new();
    for edge in edges {
        adjacency.entry(edge.src).or_default().insert(edge.dst);
    }
    adjacency
}

fn adjacency_edge_count(adjacency: &MatrixAdjacency) -> u64 {
    adjacency.values().map(|dsts| dsts.len() as u64).sum()
}

fn adjacency_resident_bytes(adjacency: &MatrixAdjacency) -> usize {
    const BTREE_ENTRY_OVERHEAD_BYTES: usize = 32;
    const BTREE_SET_OVERHEAD_BYTES: usize = 48;
    adjacency
        .len()
        .saturating_mul(
            std::mem::size_of::<VertexId>()
                .saturating_add(BTREE_ENTRY_OVERHEAD_BYTES)
                .saturating_add(BTREE_SET_OVERHEAD_BYTES),
        )
        .saturating_add(
            usize::try_from(adjacency_edge_count(adjacency))
                .unwrap_or(usize::MAX)
                .saturating_mul(
                    std::mem::size_of::<VertexId>().saturating_add(BTREE_ENTRY_OVERHEAD_BYTES),
                ),
        )
}

const GRAPH_VERIFY_MISMATCH_SAMPLES: usize = 64;

type RelationshipPropertyIndexEntry = (String, String, VertexId, VertexId, RelationshipId);

fn graph_export_digest(
    cell_id: &str,
    edge_type: &str,
    read_epoch: StorageSequence,
    edges: &[EdgeRecord],
) -> GraphExportDigest {
    let (out_degrees, in_degrees) = degree_maps(edges);
    GraphExportDigest {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        read_epoch,
        live_edges: edges.len() as u64,
        edge_checksum: edge_checksum(edges),
        out_degree_checksum: degree_checksum(&out_degrees),
        in_degree_checksum: degree_checksum(&in_degrees),
    }
}

fn edge_checksum(edges: &[EdgeRecord]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    let mut sorted = edges.to_vec();
    sorted.sort_by_key(|edge| (edge.src, edge.dst));
    for edge in sorted {
        checksum_u64(&mut hash, edge.src);
        checksum_u64(&mut hash, edge.dst);
    }
    hash
}

fn degree_checksum(degrees: &BTreeMap<VertexId, u64>) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for (vertex, degree) in degrees {
        checksum_u64(&mut hash, *vertex);
        checksum_u64(&mut hash, *degree);
    }
    hash
}

fn edge_set(edges: &[EdgeRecord]) -> BTreeSet<(VertexId, VertexId)> {
    edges.iter().map(|edge| (edge.src, edge.dst)).collect()
}

fn degree_maps(edges: &[EdgeRecord]) -> (BTreeMap<VertexId, u64>, BTreeMap<VertexId, u64>) {
    let mut out = BTreeMap::<VertexId, u64>::new();
    let mut input = BTreeMap::<VertexId, u64>::new();
    for edge in edges {
        *out.entry(edge.src).or_default() += 1;
        *input.entry(edge.dst).or_default() += 1;
    }
    (out, input)
}

fn compare_edge_sets(
    label: &'static str,
    expected: &BTreeSet<(VertexId, VertexId)>,
    actual: &BTreeSet<(VertexId, VertexId)>,
    report: &mut GraphCorrectnessReport,
) {
    for (src, dst) in expected
        .difference(actual)
        .take(GRAPH_VERIFY_MISMATCH_SAMPLES)
    {
        record_mismatch(report, format!("{label}:missing src={src} dst={dst}"));
    }
    for (src, dst) in actual
        .difference(expected)
        .take(GRAPH_VERIFY_MISMATCH_SAMPLES)
    {
        record_mismatch(report, format!("{label}:extra src={src} dst={dst}"));
    }
    let missing = expected.difference(actual).count();
    let extra = actual.difference(expected).count();
    let sampled = missing
        .min(GRAPH_VERIFY_MISMATCH_SAMPLES)
        .saturating_add(extra.min(GRAPH_VERIFY_MISMATCH_SAMPLES));
    let unsampled = missing.saturating_add(extra).saturating_sub(sampled);
    report.mismatch_count = report.mismatch_count.saturating_add(unsampled as u64);
}

fn compare_degree_maps(
    label: &'static str,
    expected: &BTreeMap<VertexId, u64>,
    actual: &BTreeMap<VertexId, u64>,
    report: &mut GraphCorrectnessReport,
) {
    for (vertex, expected_degree) in expected {
        let actual_degree = actual.get(vertex).copied().unwrap_or_default();
        if actual_degree != *expected_degree {
            record_mismatch(
                report,
                format!(
                    "{label}:mismatch vertex={vertex} expected={expected_degree} actual={actual_degree}"
                ),
            );
        }
    }
    for (vertex, actual_degree) in actual {
        if *actual_degree > 0 && !expected.contains_key(vertex) {
            record_mismatch(
                report,
                format!("{label}:extra vertex={vertex} actual={actual_degree}"),
            );
        }
    }
}

fn compare_relationship_count_maps(
    label: &'static str,
    expected: &BTreeMap<(VertexId, VertexId), u64>,
    actual: &BTreeMap<(VertexId, VertexId), u64>,
    report: &mut GraphCorrectnessReport,
) {
    for ((src, dst), expected_count) in expected {
        let actual_count = actual.get(&(*src, *dst)).copied().unwrap_or_default();
        if actual_count != *expected_count {
            record_mismatch(
                report,
                format!(
                    "{label}:mismatch src={src} dst={dst} expected={expected_count} actual={actual_count}"
                ),
            );
        }
    }
    for ((src, dst), actual_count) in actual {
        if *actual_count > 0 && !expected.contains_key(&(*src, *dst)) {
            record_mismatch(
                report,
                format!("{label}:extra src={src} dst={dst} actual={actual_count}"),
            );
        }
    }
}

fn compare_relationship_property_index_sets(
    label: &'static str,
    expected: &BTreeSet<RelationshipPropertyIndexEntry>,
    actual: &BTreeSet<RelationshipPropertyIndexEntry>,
    report: &mut GraphCorrectnessReport,
) {
    for (property, encoded, src, dst, relationship_id) in expected
        .difference(actual)
        .take(GRAPH_VERIFY_MISMATCH_SAMPLES)
    {
        record_mismatch(
            report,
            format!(
                "{label}:missing property={property} value={encoded} src={src} dst={dst} relationship_id={relationship_id}"
            ),
        );
    }
    for (property, encoded, src, dst, relationship_id) in actual
        .difference(expected)
        .take(GRAPH_VERIFY_MISMATCH_SAMPLES)
    {
        record_mismatch(
            report,
            format!(
                "{label}:extra property={property} value={encoded} src={src} dst={dst} relationship_id={relationship_id}"
            ),
        );
    }
    let missing = expected.difference(actual).count();
    let extra = actual.difference(expected).count();
    let sampled = missing
        .min(GRAPH_VERIFY_MISMATCH_SAMPLES)
        .saturating_add(extra.min(GRAPH_VERIFY_MISMATCH_SAMPLES));
    let unsampled = missing.saturating_add(extra).saturating_sub(sampled);
    report.mismatch_count = report.mismatch_count.saturating_add(unsampled as u64);
}

fn parse_relationship_count_key(key: &str) -> Result<(VertexId, VertexId)> {
    match key.split('/').collect::<Vec<_>>().as_slice() {
        ["cell", _cell_id, "rel_count", _edge_type, src, dst] => Ok((
            parse_u64(key, src, "relationship_count_src")?,
            parse_u64(key, dst, "relationship_count_dst")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship count key".to_string(),
        }),
    }
}

fn parse_relationship_property_index_key_for_verify(
    key: &str,
) -> Result<(
    String,
    String,
    String,
    String,
    VertexId,
    VertexId,
    RelationshipId,
)> {
    match key.split('/').collect::<Vec<_>>().as_slice() {
        ["cell", cell_id, "rprop_idx", edge_type, property, encoded, src, dst, relationship_id] => {
            Ok((
                (*cell_id).to_string(),
                (*edge_type).to_string(),
                (*property).to_string(),
                (*encoded).to_string(),
                parse_u64(key, src, "src")?,
                parse_u64(key, dst, "dst")?,
                parse_u64(key, relationship_id, "relationship_id")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected relationship property index key".to_string(),
        }),
    }
}

fn record_mismatch(report: &mut GraphCorrectnessReport, message: String) {
    report.mismatch_count = report.mismatch_count.saturating_add(1);
    if report.mismatch_samples.len() < GRAPH_VERIFY_MISMATCH_SAMPLES {
        report.mismatch_samples.push(message);
    }
}

fn naive_reachable(adjacency: &MatrixAdjacency, root: VertexId, hops: u8) -> Vec<VertexId> {
    let mut frontier = BTreeSet::from([root]);
    let mut reachable = BTreeSet::new();
    for _ in 0..hops {
        let mut next = BTreeSet::new();
        for vertex in &frontier {
            if let Some(neighbors) = adjacency.get(vertex) {
                next.extend(neighbors.iter().copied());
            }
        }
        reachable.extend(next.iter().copied());
        frontier = next;
        if frontier.is_empty() {
            break;
        }
    }
    reachable.into_iter().collect()
}

fn direction_str(direction: MatrixDirection) -> &'static str {
    match direction {
        MatrixDirection::Out => "out",
        MatrixDirection::In => "in",
    }
}

fn parse_direction(value: &str) -> Result<MatrixDirection> {
    match value {
        "out" => Ok(MatrixDirection::Out),
        "in" => Ok(MatrixDirection::In),
        _ => Err(GraphError::CorruptValue {
            key: "matrix/direction".to_string(),
            reason: format!("unknown matrix direction {value}"),
        }),
    }
}

fn parse_last_key_component(key: &str, field: &str) -> Result<u64> {
    let Some(value) = key.rsplit('/').next() else {
        return corrupt(key, "missing key component");
    };
    parse_u64(key, value, field)
}

fn matrix_manifest_key(cell_id: &str, edge_type: &str, base_epoch: StorageSequence) -> String {
    format!("cell/{cell_id}/artifact/matrix_manifest/{edge_type}/{base_epoch:020}")
}

pub(crate) fn matrix_cleanup_marker_key(
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
) -> String {
    format!("cell/{cell_id}/meta/matrix_cleanup/{edge_type}/{base_epoch:020}")
}

async fn prepare_artifact_build(
    shard: &GraphShard,
    cell_id: &str,
    operation: &'static str,
    cleanup_marker_keys: &[String],
) -> Result<()> {
    let mut guards = Vec::new();
    let mut batch = GraphWriteBatch::new();
    for marker_key in cleanup_marker_keys {
        if let Some(marker) = shard.read_remote(marker_key).await? {
            guards.push(GraphWriteGuard::equals(marker_key, marker.as_ref()));
            batch.delete(marker_key);
        }
    }
    if batch.is_empty() {
        return Ok(());
    }
    let lock = shard.acquire_local_write_guard(cell_id, operation).await?;
    let result = shard
        .write_graph_batch_strict_guarded(cell_id, operation, guards, batch)
        .await;
    crate::finish_local_write(lock, result).await
}

async fn publish_artifact_records_guarded(
    shard: &GraphShard,
    cell_id: &str,
    operation: &'static str,
    cleanup_marker_keys: &[String],
    batch: GraphWriteBatch,
) -> Result<()> {
    let guards = cleanup_marker_keys
        .iter()
        .map(GraphWriteGuard::absent)
        .collect();
    shard
        .write_graph_batch_strict_guarded(cell_id, operation, guards, batch)
        .await
}

async fn publish_artifact_records_guarded_with_cell_lock(
    shard: &GraphShard,
    cell_id: &str,
    operation: &'static str,
    cleanup_marker_keys: &[String],
    batch: GraphWriteBatch,
) -> Result<()> {
    let lock = shard.acquire_local_write_guard(cell_id, operation).await?;
    let result =
        publish_artifact_records_guarded(shard, cell_id, operation, cleanup_marker_keys, batch)
            .await;
    crate::finish_local_write(lock, result).await
}

fn matrix_manifest_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/matrix_manifest/{edge_type}/")
}

fn matrix_tile_key(tile: &MatrixTile) -> String {
    format!(
        "cell/{}/artifact/matrix/{}/{:020}/{}/{:020}/{:020}",
        tile.cell_id,
        tile.edge_type,
        tile.base_epoch,
        direction_str(tile.direction),
        tile.tile_row,
        tile.tile_col
    )
}

fn matrix_tile_prefix(
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
    direction: MatrixDirection,
) -> String {
    format!(
        "cell/{cell_id}/artifact/matrix/{edge_type}/{base_epoch:020}/{}/",
        direction_str(direction)
    )
}

fn graphblas_csc_key(cell_id: &str, edge_type: &str, base_epoch: StorageSequence) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc/{edge_type}/{base_epoch:020}")
}

fn graphblas_csc_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc/{edge_type}/")
}

fn graphblas_csc_chunk_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/")
}

fn graphblas_csc_chunk_epoch_prefix(
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{base_epoch:020}/")
}

fn graphblas_csc_chunk_key(
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
    field: &str,
    chunk_id: u64,
) -> String {
    format!(
        "cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{base_epoch:020}/{field}/{chunk_id:020}"
    )
}

fn graph_artifact_gc_prefixes(cell_id: &str, edge_type: &str) -> Vec<String> {
    vec![
        matrix_manifest_prefix(cell_id, edge_type),
        format!("cell/{cell_id}/artifact/matrix/{edge_type}/"),
        graphblas_csc_prefix(cell_id, edge_type),
        graphblas_csc_chunk_prefix(cell_id, edge_type),
    ]
}

fn graph_artifact_epoch_from_key(key: &str) -> Result<Option<StorageSequence>> {
    let parts: Vec<_> = key.split('/').collect();
    let epoch = match parts.as_slice() {
        ["cell", _, "artifact", "matrix_manifest", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "matrix", _, base_epoch, ..] => Some(*base_epoch),
        ["cell", _, "artifact", "graphblas_csc", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "graphblas_csc_chunk", _, base_epoch, ..] => Some(*base_epoch),
        _ => None,
    };
    epoch
        .map(|value| parse_u64(key, value, "base_epoch"))
        .transpose()
}

const GRAPH_ARTIFACT_WRITE_BATCH_KEYS: usize = 512;
const GRAPH_ARTIFACT_GC_BATCH_KEYS: usize = 512;

#[allow(clippy::too_many_arguments)]
async fn put_artifact_record(
    shard: &GraphShard,
    artifact_locks: &[&LocalWriteGuard],
    cell_id: &str,
    operation: &'static str,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    key: String,
    value: Vec<u8>,
) -> Result<()> {
    batch.put(key, value);
    *pending_writes += 1;
    if *pending_writes >= GRAPH_ARTIFACT_WRITE_BATCH_KEYS {
        flush_artifact_put_batch(
            shard,
            artifact_locks,
            cell_id,
            operation,
            batch,
            pending_writes,
        )
        .await?;
    }
    Ok(())
}

async fn flush_artifact_put_batch(
    shard: &GraphShard,
    artifact_locks: &[&LocalWriteGuard],
    cell_id: &str,
    operation: &'static str,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
) -> Result<()> {
    if *pending_writes == 0 {
        return Ok(());
    }
    for lock in artifact_locks {
        lock.renew().await?;
    }
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    shard
        .write_graph_batch_strict_with_cell_lock(cell_id, operation, batch_to_write)
        .await?;
    for lock in artifact_locks {
        lock.renew().await?;
    }
    *pending_writes = 0;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MatrixArtifactCleanupResult {
    pub(crate) deleted_keys: u64,
    pub(crate) cleanup_errors: u64,
    pub(crate) skipped_published_manifest: bool,
}

impl MatrixArtifactCleanupResult {
    fn record_error<E>(
        &mut self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
        operation: &'static str,
        cleanup_step: &'static str,
        err: &E,
    ) where
        E: std::fmt::Display + ?Sized,
    {
        self.cleanup_errors = self.cleanup_errors.saturating_add(1);
        tracing::warn!(
            target: "slatedb_graph_kernel",
            cell_id,
            edge_type,
            base_epoch,
            operation,
            cleanup_step,
            error = %err,
            "matrix artifact abort cleanup step failed"
        );
    }
}

pub(crate) async fn cleanup_unpublished_matrix_artifact_epoch(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
    operation: &'static str,
) -> MatrixArtifactCleanupResult {
    let mut result = MatrixArtifactCleanupResult::default();
    let manifest_key = matrix_manifest_key(cell_id, edge_type, base_epoch);
    match shard.read_remote(&manifest_key).await {
        Ok(Some(_)) => {
            result.skipped_published_manifest = true;
            return result;
        }
        Ok(None) => {}
        Err(err) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "read_matrix_manifest",
                &err,
            );
            return result;
        }
    }

    let artifact_lock = match shard
        .acquire_local_artifact_guard(cell_id, edge_type, base_epoch, operation)
        .await
    {
        Ok(lock) => lock,
        Err(err) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "acquire_matrix_artifact_lock",
                &err,
            );
            return result;
        }
    };

    let cleanup_marker_key = matrix_cleanup_marker_key(cell_id, edge_type, base_epoch);
    let cleanup_token = artifact_lock.token.as_bytes().to_vec();
    let mut cleanup_claimed = false;

    let cleanup_run = async {
        match shard.read_remote(&manifest_key).await {
            Ok(Some(_)) => {
                result.skipped_published_manifest = true;
                return Ok(());
            }
            Ok(None) => {}
            Err(err) => {
                result.record_error(
                    cell_id,
                    edge_type,
                    base_epoch,
                    operation,
                    "recheck_matrix_manifest",
                    &err,
                );
                return Ok(());
            }
        }

        let mut claim_batch = GraphWriteBatch::new();
        claim_batch.put(&cleanup_marker_key, &cleanup_token);
        match shard
            .write_graph_batch_strict_guarded(
                cell_id,
                operation,
                vec![GraphWriteGuard::absent(&manifest_key)],
                claim_batch,
            )
            .await
        {
            Ok(()) => cleanup_claimed = true,
            Err(GraphError::ConditionalWriteConflict { key, .. }) if key == manifest_key => {
                result.skipped_published_manifest = true;
                return Ok(());
            }
            Err(err) => {
                result.record_error(
                    cell_id,
                    edge_type,
                    base_epoch,
                    operation,
                    "claim_matrix_cleanup_generation",
                    &err,
                );
                return Ok(());
            }
        }

        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        let graphblas_key = graphblas_csc_key(cell_id, edge_type, base_epoch);
        match shard.read_remote(&graphblas_key).await {
            Ok(Some(_)) => {
                batch.delete(graphblas_key.as_bytes());
                pending_deletes += 1;
            }
            Ok(None) => {}
            Err(err) => result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "read_graphblas_manifest",
                &err,
            ),
        }
        if pending_deletes >= GRAPH_ARTIFACT_GC_BATCH_KEYS
            && !flush_unpublished_artifact_gc_batch_best_effort(
                shard,
                cell_id,
                edge_type,
                base_epoch,
                operation,
                &manifest_key,
                &cleanup_marker_key,
                &cleanup_token,
                &artifact_lock,
                &mut batch,
                &mut pending_deletes,
                &mut result,
            )
            .await
        {
            return Ok(());
        }

        for (cleanup_step, prefix) in [
            (
                "scan_matrix_out_tiles",
                matrix_tile_prefix(cell_id, edge_type, base_epoch, MatrixDirection::Out),
            ),
            (
                "scan_matrix_in_tiles",
                matrix_tile_prefix(cell_id, edge_type, base_epoch, MatrixDirection::In),
            ),
            (
                "scan_graphblas_chunks",
                graphblas_csc_chunk_epoch_prefix(cell_id, edge_type, base_epoch),
            ),
        ] {
            let mut iter = match shard.scan_remote_prefix(&prefix).await {
                Ok(iter) => iter,
                Err(err) => {
                    result.record_error(
                        cell_id,
                        edge_type,
                        base_epoch,
                        operation,
                        cleanup_step,
                        &err,
                    );
                    continue;
                }
            };
            loop {
                let kv = match iter.next().await {
                    Ok(Some(kv)) => kv,
                    Ok(None) => break,
                    Err(err) => {
                        result.record_error(
                            cell_id,
                            edge_type,
                            base_epoch,
                            operation,
                            cleanup_step,
                            &err,
                        );
                        break;
                    }
                };
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                batch.delete(key.as_bytes());
                pending_deletes += 1;
                if pending_deletes >= GRAPH_ARTIFACT_GC_BATCH_KEYS
                    && !flush_unpublished_artifact_gc_batch_best_effort(
                        shard,
                        cell_id,
                        edge_type,
                        base_epoch,
                        operation,
                        &manifest_key,
                        &cleanup_marker_key,
                        &cleanup_token,
                        &artifact_lock,
                        &mut batch,
                        &mut pending_deletes,
                        &mut result,
                    )
                    .await
                {
                    return Ok(());
                }
            }
        }

        if flush_unpublished_artifact_gc_batch_best_effort(
            shard,
            cell_id,
            edge_type,
            base_epoch,
            operation,
            &manifest_key,
            &cleanup_marker_key,
            &cleanup_token,
            &artifact_lock,
            &mut batch,
            &mut pending_deletes,
            &mut result,
        )
        .await
        {
            shard.matrix_artifact_cache.lock().await.retain(|key, _| {
                key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch != base_epoch
            });
            shard.matrix_cache.lock().await.retain(|key, _| {
                key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch != base_epoch
            });
            shard.graphblas_cache.lock().await.retain(|key, _| {
                key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch != base_epoch
            });
        }

        Ok(())
    }
    .await;
    if cleanup_claimed {
        let mut release_batch = GraphWriteBatch::new();
        release_batch.delete(&cleanup_marker_key);
        if let Err(err) = shard
            .write_graph_batch_strict_guarded(
                cell_id,
                operation,
                vec![GraphWriteGuard::equals(&cleanup_marker_key, &cleanup_token)],
                release_batch,
            )
            .await
        {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "release_matrix_cleanup_generation",
                &err,
            );
        }
    }
    if let Err(err) = crate::finish_local_write(artifact_lock, cleanup_run).await {
        result.record_error(
            cell_id,
            edge_type,
            base_epoch,
            operation,
            "release_matrix_artifact_lock",
            &err,
        );
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn flush_unpublished_artifact_gc_batch_best_effort(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
    operation: &'static str,
    manifest_key: &str,
    cleanup_marker_key: &str,
    cleanup_token: &[u8],
    artifact_lock: &crate::LocalWriteGuard,
    batch: &mut GraphWriteBatch,
    pending_deletes: &mut usize,
    result: &mut MatrixArtifactCleanupResult,
) -> bool {
    if *pending_deletes == 0 {
        return true;
    }
    let attempted_deletes = *pending_deletes as u64;
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    *pending_deletes = 0;
    let locked_delete = async {
        artifact_lock.renew().await?;
        shard
            .write_graph_batch_strict_guarded(
                cell_id,
                operation,
                vec![
                    GraphWriteGuard::absent(manifest_key),
                    GraphWriteGuard::equals(cleanup_marker_key, cleanup_token),
                ],
                batch_to_write,
            )
            .await?;
        artifact_lock.renew().await
    }
    .await;
    match locked_delete {
        Ok(()) => {
            result.deleted_keys = result.deleted_keys.saturating_add(attempted_deletes);
        }
        Err(GraphError::ConditionalWriteConflict { key, .. }) if key == manifest_key => {
            result.skipped_published_manifest = true;
            tracing::warn!(
                target: "slatedb_graph_kernel",
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "matrix artifact abort cleanup skipped published manifest"
            );
            return false;
        }
        Err(GraphError::ConditionalWriteConflict {
            operation: conflict_operation,
            key,
        }) if key == cleanup_marker_key => {
            let err = GraphError::ConditionalWriteConflict {
                operation: conflict_operation,
                key,
            };
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "lost_matrix_cleanup_generation",
                &err,
            );
            return false;
        }
        Err(err) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "delete_artifact_batch",
                &err,
            );
        }
    }
    true
}

async fn flush_artifact_gc_batch(
    shard: &GraphShard,
    cell_id: &str,
    operation: &'static str,
    batch: &mut GraphWriteBatch,
    pending_deletes: &mut usize,
) -> Result<()> {
    if *pending_deletes == 0 {
        return Ok(());
    }
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    shard
        .write_graph_batch_strict_with_cell_lock(cell_id, operation, batch_to_write)
        .await?;
    *pending_deletes = 0;
    Ok(())
}

fn encode_matrix_artifact(artifact: &MatrixArtifact) -> Vec<u8> {
    format!(
        "matrix_manifest1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        artifact.cell_id,
        artifact.edge_type,
        artifact.base_epoch,
        artifact.tile_size,
        artifact.out_tiles,
        artifact.transpose_tiles,
        artifact.edge_count
    )
    .into_bytes()
}

fn decode_matrix_artifact(key: &str, value: &[u8]) -> Result<MatrixArtifact> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "matrix_manifest1" {
        return corrupt(key, "expected matrix_manifest1 record with 8 fields");
    }
    Ok(MatrixArtifact {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        tile_size: parse_u64(key, parts[4], "tile_size")?,
        out_tiles: parse_u64(key, parts[5], "out_tiles")?,
        transpose_tiles: parse_u64(key, parts[6], "transpose_tiles")?,
        edge_count: parse_u64(key, parts[7], "edge_count")?,
    })
}

const GRAPHBLAS_CSC_MAGIC: &[u8] = b"graphblas_csc1\n";
const GRAPHBLAS_CSC_MANIFEST_MAGIC: &str = "graph-graphblas-csc-manifest-v1";
const GRAPHBLAS_CSC_CHUNK_MAGIC: &[u8] = b"graphblas_csc_chunk1\n";
const GRAPHBLAS_CSC_CHUNK_U64S: usize = 64 * 1024;

#[allow(clippy::too_many_arguments)]
async fn append_graphblas_csc_chunks(
    shard: &GraphShard,
    artifact_lock: &LocalWriteGuard,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
    csc: &GraphBlasCsc,
) -> Result<GraphBlasCscManifest> {
    let vertex_chunks = append_graphblas_csc_field_chunks(
        shard,
        artifact_lock,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "vertices",
        &csc.vertices,
    )
    .await?;
    let pointer_chunks = append_graphblas_csc_field_chunks(
        shard,
        artifact_lock,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "pointers",
        &csc.pointers,
    )
    .await?;
    let index_chunks = append_graphblas_csc_field_chunks(
        shard,
        artifact_lock,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "indices",
        &csc.indices,
    )
    .await?;
    Ok(GraphBlasCscManifest {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        base_epoch,
        chunk_size: GRAPHBLAS_CSC_CHUNK_U64S as u64,
        vertices_len: csc.vertices.len() as u64,
        pointers_len: csc.pointers.len() as u64,
        indices_len: csc.indices.len() as u64,
        vertex_chunks,
        pointer_chunks,
        index_chunks,
        checksum: graphblas_csc_checksum(csc),
    })
}

#[allow(clippy::too_many_arguments)]
async fn append_graphblas_csc_chunks_from_rows(
    shard: &GraphShard,
    artifact_lock: &LocalWriteGuard,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
    rows: &MatrixRows,
) -> Result<GraphBlasCscManifest> {
    let vertices = matrix_rows_vertices(rows);
    let pointers = matrix_rows_pointers(rows, &vertices)?;
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    checksum_u64(&mut checksum, 0x01);
    for value in &vertices {
        checksum_u64(&mut checksum, *value);
    }
    checksum_u64(&mut checksum, 0x02);
    for value in &pointers {
        checksum_u64(&mut checksum, *value);
    }
    checksum_u64(&mut checksum, 0x03);

    let vertex_chunks = append_graphblas_csc_field_chunks(
        shard,
        artifact_lock,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "vertices",
        &vertices,
    )
    .await?;
    let pointer_chunks = append_graphblas_csc_field_chunks(
        shard,
        artifact_lock,
        batch,
        pending_writes,
        cell_id,
        edge_type,
        base_epoch,
        "pointers",
        &pointers,
    )
    .await?;

    let mut index_chunks = 0_u64;
    let mut index_chunk = Vec::with_capacity(GRAPHBLAS_CSC_CHUNK_U64S);
    for src in &vertices {
        let Some(dsts) = rows.rows.get(src) else {
            continue;
        };
        for dst in dsts {
            let ordinal = vertices
                .binary_search(dst)
                .map_err(|_| GraphError::CorruptValue {
                    key: "graphblas_csc_indices".to_string(),
                    reason: format!("destination vertex {dst} missing from CSC vertex dictionary"),
                })? as u64;
            checksum_u64(&mut checksum, ordinal);
            index_chunk.push(ordinal);
            if index_chunk.len() == GRAPHBLAS_CSC_CHUNK_U64S {
                let chunk_id = index_chunks;
                index_chunks = index_chunks.saturating_add(1);
                let values = std::mem::take(&mut index_chunk);
                put_artifact_record(
                    shard,
                    &[artifact_lock],
                    cell_id,
                    "build_matrix_tiles",
                    batch,
                    pending_writes,
                    graphblas_csc_chunk_key(cell_id, edge_type, base_epoch, "indices", chunk_id),
                    encode_graphblas_csc_chunk("indices", chunk_id, &values),
                )
                .await?;
                index_chunk = Vec::with_capacity(GRAPHBLAS_CSC_CHUNK_U64S);
            }
        }
    }
    if !index_chunk.is_empty() {
        let chunk_id = index_chunks;
        index_chunks = index_chunks.saturating_add(1);
        put_artifact_record(
            shard,
            &[artifact_lock],
            cell_id,
            "build_matrix_tiles",
            batch,
            pending_writes,
            graphblas_csc_chunk_key(cell_id, edge_type, base_epoch, "indices", chunk_id),
            encode_graphblas_csc_chunk("indices", chunk_id, &index_chunk),
        )
        .await?;
    }

    Ok(GraphBlasCscManifest {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        base_epoch,
        chunk_size: GRAPHBLAS_CSC_CHUNK_U64S as u64,
        vertices_len: vertices.len() as u64,
        pointers_len: pointers.len() as u64,
        indices_len: rows.live_edges,
        vertex_chunks,
        pointer_chunks,
        index_chunks,
        checksum,
    })
}

#[allow(clippy::too_many_arguments)]
async fn append_graphblas_csc_field_chunks(
    shard: &GraphShard,
    artifact_lock: &LocalWriteGuard,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: StorageSequence,
    field: &'static str,
    values: &[u64],
) -> Result<u64> {
    let mut chunks = 0_u64;
    for (chunk_id, chunk) in values.chunks(GRAPHBLAS_CSC_CHUNK_U64S).enumerate() {
        put_artifact_record(
            shard,
            &[artifact_lock],
            cell_id,
            "build_matrix_tiles",
            batch,
            pending_writes,
            graphblas_csc_chunk_key(cell_id, edge_type, base_epoch, field, chunk_id as u64),
            encode_graphblas_csc_chunk(field, chunk_id as u64, chunk),
        )
        .await?;
        chunks += 1;
    }
    Ok(chunks)
}

fn encode_graphblas_csc_manifest(manifest: &GraphBlasCscManifest) -> Vec<u8> {
    format!(
        "{GRAPHBLAS_CSC_MANIFEST_MAGIC}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        manifest.cell_id,
        manifest.edge_type,
        manifest.base_epoch,
        manifest.chunk_size,
        manifest.vertices_len,
        manifest.pointers_len,
        manifest.indices_len,
        manifest.vertex_chunks,
        manifest.pointer_chunks,
        manifest.index_chunks,
        manifest.checksum
    )
    .into_bytes()
}

fn decode_graphblas_csc_manifest(key: &str, value: &[u8]) -> Result<GraphBlasCscManifest> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 12 || parts[0] != GRAPHBLAS_CSC_MANIFEST_MAGIC {
        return corrupt(
            key,
            "expected graph-graphblas-csc-manifest-v1 record with 12 fields",
        );
    }
    let manifest = GraphBlasCscManifest {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        chunk_size: parse_u64(key, parts[4], "chunk_size")?,
        vertices_len: parse_u64(key, parts[5], "vertices_len")?,
        pointers_len: parse_u64(key, parts[6], "pointers_len")?,
        indices_len: parse_u64(key, parts[7], "indices_len")?,
        vertex_chunks: parse_u64(key, parts[8], "vertex_chunks")?,
        pointer_chunks: parse_u64(key, parts[9], "pointer_chunks")?,
        index_chunks: parse_u64(key, parts[10], "index_chunks")?,
        checksum: parse_u64(key, parts[11], "checksum")?,
    };
    if manifest.chunk_size != GRAPHBLAS_CSC_CHUNK_U64S as u64 {
        return corrupt(key, "unsupported GraphBLAS CSC chunk size");
    }
    if expected_chunk_count(manifest.vertices_len) != manifest.vertex_chunks
        || expected_chunk_count(manifest.pointers_len) != manifest.pointer_chunks
        || expected_chunk_count(manifest.indices_len) != manifest.index_chunks
    {
        return corrupt(key, "GraphBLAS CSC manifest chunk count mismatch");
    }
    Ok(manifest)
}

fn expected_chunk_count(len: u64) -> u64 {
    if len == 0 {
        0
    } else {
        len.div_ceil(GRAPHBLAS_CSC_CHUNK_U64S as u64)
    }
}

fn decode_graphblas_csc(
    key: &str,
    value: &[u8],
    expected_cell_id: &str,
    expected_edge_type: &str,
    expected_base_epoch: StorageSequence,
) -> Result<GraphBlasCsc> {
    if !value.starts_with(GRAPHBLAS_CSC_MAGIC) {
        return corrupt(key, "expected graphblas_csc1 binary artifact");
    }
    let mut cursor = GRAPHBLAS_CSC_MAGIC.len();
    let cell_id = decode_binary_string(key, value, &mut cursor, "cell_id")?;
    let edge_type = decode_binary_string(key, value, &mut cursor, "edge_type")?;
    let base_epoch = decode_binary_u64(key, value, &mut cursor, "base_epoch")?;
    if cell_id != expected_cell_id
        || edge_type != expected_edge_type
        || base_epoch != expected_base_epoch
    {
        return corrupt(key, "GraphBLAS CSC identity does not match key");
    }
    let vertices = decode_binary_u64s(key, value, &mut cursor, "vertices")?;
    let pointers = decode_binary_u64s(key, value, &mut cursor, "pointers")?;
    let indices = decode_binary_u64s(key, value, &mut cursor, "indices")?;
    if cursor != value.len() {
        return corrupt(key, "trailing bytes in graphblas CSC artifact");
    }
    let csc = GraphBlasCsc {
        vertices,
        pointers,
        indices,
    };
    validate_graphblas_csc_artifact(key, &csc)?;
    Ok(csc)
}

fn encode_graphblas_csc_chunk(field: &str, chunk_id: u64, values: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(GRAPHBLAS_CSC_CHUNK_MAGIC);
    encode_binary_string(&mut out, field);
    encode_binary_u64(&mut out, chunk_id);
    encode_binary_u64s(&mut out, values);
    out
}

fn decode_graphblas_csc_chunk(
    key: &str,
    value: &[u8],
    expected_field: &str,
    expected_chunk_id: u64,
) -> Result<Vec<u64>> {
    if !value.starts_with(GRAPHBLAS_CSC_CHUNK_MAGIC) {
        return corrupt(key, "expected graphblas_csc_chunk1 binary artifact");
    }
    let mut cursor = GRAPHBLAS_CSC_CHUNK_MAGIC.len();
    let field = decode_binary_string(key, value, &mut cursor, "field")?;
    let chunk_id = decode_binary_u64(key, value, &mut cursor, "chunk_id")?;
    if field != expected_field || chunk_id != expected_chunk_id {
        return corrupt(key, "GraphBLAS CSC chunk identity does not match key");
    }
    let values = decode_binary_u64s(key, value, &mut cursor, "values")?;
    if cursor != value.len() {
        return corrupt(key, "trailing bytes in GraphBLAS CSC chunk");
    }
    Ok(values)
}

fn decode_graphblas_csc_chunk_u32(
    key: &str,
    value: &[u8],
    expected_field: &str,
    expected_chunk_id: u64,
) -> Result<Vec<u32>> {
    if !value.starts_with(GRAPHBLAS_CSC_CHUNK_MAGIC) {
        return corrupt(key, "expected graphblas_csc_chunk1 binary artifact");
    }
    let mut cursor = GRAPHBLAS_CSC_CHUNK_MAGIC.len();
    let field = decode_binary_string(key, value, &mut cursor, "field")?;
    let chunk_id = decode_binary_u64(key, value, &mut cursor, "chunk_id")?;
    if field != expected_field || chunk_id != expected_chunk_id {
        return corrupt(key, "GraphBLAS CSC chunk identity does not match key");
    }
    let values = decode_binary_u32s_from_u64s(key, value, &mut cursor, "values")?;
    if cursor != value.len() {
        return corrupt(key, "trailing bytes in GraphBLAS CSC chunk");
    }
    Ok(values)
}

fn validate_graphblas_csc_artifact(key: &str, csc: &GraphBlasCsc) -> Result<()> {
    if csc.pointers.len() != csc.vertices.len() + 1 {
        return corrupt(key, "CSC pointer count does not match vertex count");
    }
    if csc.pointers.first().copied() != Some(0) {
        return corrupt(key, "CSC first pointer must be zero");
    }
    for window in csc.pointers.windows(2) {
        if window[0] > window[1] {
            return corrupt(key, "CSC pointers must be monotonic");
        }
    }
    if csc.pointers.last().copied().unwrap_or(0) as usize != csc.indices.len() {
        return corrupt(key, "CSC edge count does not match index count");
    }
    if let Some(index) = csc
        .indices
        .iter()
        .copied()
        .find(|index| *index >= csc.vertices.len() as u64)
    {
        return corrupt(key, format!("CSC index {index} exceeds vertex count"));
    }
    for window in csc.vertices.windows(2) {
        if window[0] >= window[1] {
            return corrupt(key, "CSC vertices must be sorted and unique");
        }
    }
    Ok(())
}

fn graphblas_csc_checksum(csc: &GraphBlasCsc) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    checksum_u64(&mut hash, 0x01);
    for value in &csc.vertices {
        checksum_u64(&mut hash, *value);
    }
    checksum_u64(&mut hash, 0x02);
    for value in &csc.pointers {
        checksum_u64(&mut hash, *value);
    }
    checksum_u64(&mut hash, 0x03);
    for value in &csc.indices {
        checksum_u64(&mut hash, *value);
    }
    hash
}

fn graphblas_csc_checksum_compact(vertices: &[VertexId], pointers: &[u32], indices: &[u32]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    checksum_u64(&mut hash, 0x01);
    for value in vertices {
        checksum_u64(&mut hash, *value);
    }
    checksum_u64(&mut hash, 0x02);
    for value in pointers {
        checksum_u64(&mut hash, u64::from(*value));
    }
    checksum_u64(&mut hash, 0x03);
    for value in indices {
        checksum_u64(&mut hash, u64::from(*value));
    }
    hash
}

fn checksum_u64(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn encode_matrix_tile(tile: &MatrixTile) -> Vec<u8> {
    let rows = tile
        .rows
        .iter()
        .map(|(row, cols)| format!("{row}:{}", encode_vertices(cols)))
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "matrix_tile1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        tile.cell_id,
        tile.edge_type,
        tile.base_epoch,
        tile.tile_size,
        direction_str(tile.direction),
        tile.tile_row,
        tile.tile_col,
        rows
    )
    .into_bytes()
}

fn decode_matrix_tile(key: &str, value: &[u8]) -> Result<MatrixTile> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 9 || parts[0] != "matrix_tile1" {
        return corrupt(key, "expected matrix_tile1 record with 9 fields");
    }
    let mut rows = BTreeMap::new();
    if !parts[8].is_empty() {
        for row in parts[8].split(';') {
            let Some((src, dsts)) = row.split_once(':') else {
                return corrupt(key, "invalid matrix row encoding");
            };
            rows.insert(
                parse_u64(key, src, "matrix_row")?,
                decode_vertices(key, dsts)?,
            );
        }
    }
    Ok(MatrixTile {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        tile_size: parse_u64(key, parts[4], "tile_size")?,
        direction: parse_direction(parts[5])?,
        tile_row: parse_u64(key, parts[6], "tile_row")?,
        tile_col: parse_u64(key, parts[7], "tile_col")?,
        rows,
    })
}

fn encode_vertices(vertices: &[VertexId]) -> String {
    vertices
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_vertices(key: &str, value: &str) -> Result<Vec<VertexId>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| parse_u64(key, part, "vertex"))
        .collect()
}

fn encode_binary_string(out: &mut Vec<u8>, value: &str) {
    encode_binary_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn encode_binary_u64s(out: &mut Vec<u8>, values: &[u64]) {
    encode_binary_u64(out, values.len() as u64);
    for value in values {
        encode_binary_u64(out, *value);
    }
}

fn encode_binary_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_binary_string(
    key: &str,
    value: &[u8],
    cursor: &mut usize,
    field: &str,
) -> Result<String> {
    let len = decode_binary_len(key, value, cursor, field)?;
    let bytes = take_binary(key, value, cursor, len, field)?;
    String::from_utf8(bytes.to_vec()).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("invalid UTF-8 in {field}: {err}"),
    })
}

fn decode_binary_u64s(
    key: &str,
    value: &[u8],
    cursor: &mut usize,
    field: &str,
) -> Result<Vec<u64>> {
    let len = decode_binary_len(key, value, cursor, field)?;
    let bytes = take_binary(
        key,
        value,
        cursor,
        len.checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("binary array {field} is too large"),
            })?,
        field,
    )?;
    bytes
        .chunks_exact(std::mem::size_of::<u64>())
        .map(|chunk| decode_binary_u64_bytes(key, chunk, field))
        .collect()
}

fn decode_binary_u32s_from_u64s(
    key: &str,
    value: &[u8],
    cursor: &mut usize,
    field: &str,
) -> Result<Vec<u32>> {
    let len = decode_binary_len(key, value, cursor, field)?;
    let bytes = take_binary(
        key,
        value,
        cursor,
        len.checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("binary array {field} is too large"),
            })?,
        field,
    )?;
    let mut out = Vec::with_capacity(len);
    for chunk in bytes.chunks_exact(std::mem::size_of::<u64>()) {
        let value = decode_binary_u64_bytes(key, chunk, field)?;
        out.push(u32::try_from(value).map_err(|_| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("binary array {field} value {value} does not fit u32"),
        })?);
    }
    Ok(out)
}

fn decode_binary_len(key: &str, value: &[u8], cursor: &mut usize, field: &str) -> Result<usize> {
    let raw = decode_binary_u64(key, value, cursor, field)?;
    usize::try_from(raw).map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("binary length for {field} does not fit usize"),
    })
}

fn decode_binary_u64(key: &str, value: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
    let bytes = take_binary(key, value, cursor, std::mem::size_of::<u64>(), field)?;
    decode_binary_u64_bytes(key, bytes, field)
}

fn decode_binary_u64_bytes(key: &str, bytes: &[u8], field: &str) -> Result<u64> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("binary field {field} has invalid u64 length"),
    })?;
    Ok(u64::from_le_bytes(bytes))
}

fn take_binary<'a>(
    key: &str,
    value: &'a [u8],
    cursor: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("binary cursor overflow while reading {field}"),
        })?;
    if end > value.len() {
        return corrupt(key, format!("truncated binary field {field}"));
    }
    let out = &value[*cursor..end];
    *cursor = end;
    Ok(out)
}

fn text_value<'a>(key: &str, value: &'a [u8]) -> Result<&'a str> {
    std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: err.to_string(),
    })
}

fn corrupt<T>(key: &str, reason: impl Into<String>) -> Result<T> {
    Err(GraphError::CorruptValue {
        key: key.to_string(),
        reason: reason.into(),
    })
}

fn matrix_profile_enabled() -> bool {
    std::env::var("GRAPH_PROFILE_MATRIX").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn record_matrix_profile(enabled: bool, stage: &str, elapsed: Duration, units: u64) {
    if enabled {
        eprintln!(
            "matrix_profile stage={stage} elapsed_us={} units={units}",
            elapsed.as_micros()
        );
    }
}

pub(crate) fn trim_process_memory_after_hydration() {
    if !std::env::var("GRAPH_TRIM_MEMORY_AFTER_HYDRATION").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }) {
        return;
    }
    trim_process_memory();
}

#[cfg(target_os = "linux")]
fn trim_process_memory() {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    unsafe {
        let _ = malloc_trim(0);
    }
}

#[cfg(not(target_os = "linux"))]
fn trim_process_memory() {}
