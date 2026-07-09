use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::future::join_all;
use slatedb::bytes::Bytes;
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions};
use slatedb::object_store::{local::LocalFileSystem, ObjectStore};
use slatedb::{Db, DbTransaction, ErrorKind, IsolationLevel, WriteBatch};
use tokio::sync::{watch, RwLock as TokioRwLock};
use tokio::task::JoinHandle;

#[cfg(feature = "graphblas")]
use crate::sparse_kernel::compile_graphblas_csc;
use crate::sparse_kernel::{
    compact_csc_kernel_enabled, compile_graphblas_compact_csc_u32, compile_graphblas_csc_owned,
    compile_graphblas_matrix, default_matrix_kernel, expand as expand_sparse,
    expand_compiled_graphblas, graphblas_csc_from_adjacency, CompiledGraphBlasMatrix, GraphBlasCsc,
    SparseKernelBackend,
};
use crate::{
    decode_delta_record, decode_edge_record, decode_out_edge_segment, decode_relationship_record,
    decode_u64, encode_vertex_property_value_key, ensure_limit, open_graph_db,
    parse_out_edge_segment_tombstone_key, parse_u64, segment_edge_visible, sort_deltas,
    validate_component, CellWriteLock, DeltaKind, DeltaRecord, EdgeRecord, GraphCacheConfig,
    GraphCacheKind, GraphCorrectnessReport, GraphDurabilityConfig, GraphEpoch, GraphError,
    GraphExportDigest, GraphOpenOptions, GraphShard, GraphWriteBatch, MatrixAdjacency,
    MatrixCacheKey, PostingChunkCacheKey, RelationshipId, RelationshipRecord, Result,
    SupernodeCacheKey, VertexId,
};

const GRAPH_PREALLOC_LIMIT: usize = 1_000_000;
const GRAPH_CHUNK_PREALLOC_LIMIT: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ArtifactDirection {
    Out,
    In,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostingChunk {
    pub cell_id: String,
    pub edge_type: String,
    pub direction: ArtifactDirection,
    pub owner: VertexId,
    pub base_epoch: GraphEpoch,
    pub chunk_id: u64,
    pub vertices: Vec<VertexId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PostingChunkManifest {
    cell_id: String,
    edge_type: String,
    direction: ArtifactDirection,
    owner: VertexId,
    base_epoch: GraphEpoch,
    chunk_count: u64,
    vertex_count: u64,
    chunk_checksums: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PostingArtifactManifest {
    cell_id: String,
    edge_type: String,
    base_epoch: GraphEpoch,
    owner_manifest_count: u64,
    chunk_count: u64,
    vertex_count: u64,
    checksum: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixArtifact {
    pub cell_id: String,
    pub edge_type: String,
    pub base_epoch: GraphEpoch,
    pub tile_size: u64,
    pub out_tiles: u64,
    pub transpose_tiles: u64,
    pub edge_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraversalBackend {
    PostingExpansion,
    MatrixOverlay,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixTraversalResult {
    pub backend: TraversalBackend,
    pub vertices: Vec<VertexId>,
    pub hops: u8,
    pub base_epoch: GraphEpoch,
    pub edge_visits: u64,
    pub delta_records_applied: u64,
    pub sparse_kernel: SparseKernelBackend,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkResult {
    pub posting: MatrixTraversalResult,
    pub matrix: MatrixTraversalResult,
    pub matrix_wins: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupernodeGroup {
    pub cell_id: String,
    pub edge_type: String,
    pub direction: ArtifactDirection,
    pub vertex_id: VertexId,
    pub base_epoch: GraphEpoch,
    pub degree: u64,
    pub chunk_count: u64,
    pub page_size: u64,
    pub chunk_bounds: Vec<SupernodeChunkBound>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupernodeChunkBound {
    pub chunk_id: u64,
    pub first: VertexId,
    pub last: VertexId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SupernodeArtifactManifest {
    cell_id: String,
    edge_type: String,
    base_epoch: GraphEpoch,
    group_count: u64,
    chunk_count: u64,
    degree: u64,
    checksum: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupernodePage {
    pub vertex_id: VertexId,
    pub edge_type: String,
    pub direction: ArtifactDirection,
    pub page_id: u64,
    pub vertices: Vec<VertexId>,
    pub has_next: bool,
}

pub struct GraphCluster {
    shards: BTreeMap<String, GraphShard>,
}

pub struct GraphControlPlane {
    db: Db,
    metrics: Arc<GraphControlMetrics>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphControlMetricsSnapshot {
    pub lease_acquire_attempts: u64,
    pub lease_acquire_successes: u64,
    pub lease_acquire_failures: u64,
    pub lease_renew_attempts: u64,
    pub lease_renew_successes: u64,
    pub lease_renew_failures: u64,
    pub lease_renew_lost: u64,
    pub lease_renew_retries: u64,
    pub lease_release_attempts: u64,
    pub lease_release_successes: u64,
    pub lease_release_failures: u64,
    pub metadata_cas_attempts: u64,
    pub metadata_cas_successes: u64,
    pub metadata_cas_conflicts: u64,
    pub watermark_advances: u64,
    pub watermark_rejects: u64,
    pub control_idempotency_commits: u64,
    pub control_idempotency_replays: u64,
    pub repair_runs: u64,
    pub repair_actions: u64,
    pub node_heartbeat_writes: u64,
    pub node_heartbeat_prunes: u64,
    pub controller_runs: u64,
    pub controller_reassignments: u64,
    pub controller_failovers: u64,
    pub controller_pending_failovers: u64,
}

#[derive(Default)]
struct GraphControlMetrics {
    lease_acquire_attempts: AtomicU64,
    lease_acquire_successes: AtomicU64,
    lease_acquire_failures: AtomicU64,
    lease_renew_attempts: AtomicU64,
    lease_renew_successes: AtomicU64,
    lease_renew_failures: AtomicU64,
    lease_renew_lost: AtomicU64,
    lease_renew_retries: AtomicU64,
    lease_release_attempts: AtomicU64,
    lease_release_successes: AtomicU64,
    lease_release_failures: AtomicU64,
    metadata_cas_attempts: AtomicU64,
    metadata_cas_successes: AtomicU64,
    metadata_cas_conflicts: AtomicU64,
    watermark_advances: AtomicU64,
    watermark_rejects: AtomicU64,
    control_idempotency_commits: AtomicU64,
    control_idempotency_replays: AtomicU64,
    repair_runs: AtomicU64,
    repair_actions: AtomicU64,
    node_heartbeat_writes: AtomicU64,
    node_heartbeat_prunes: AtomicU64,
    controller_runs: AtomicU64,
    controller_reassignments: AtomicU64,
    controller_failovers: AtomicU64,
    controller_pending_failovers: AtomicU64,
}

impl GraphControlMetrics {
    fn snapshot(&self) -> GraphControlMetricsSnapshot {
        GraphControlMetricsSnapshot {
            lease_acquire_attempts: self.lease_acquire_attempts.load(Ordering::Relaxed),
            lease_acquire_successes: self.lease_acquire_successes.load(Ordering::Relaxed),
            lease_acquire_failures: self.lease_acquire_failures.load(Ordering::Relaxed),
            lease_renew_attempts: self.lease_renew_attempts.load(Ordering::Relaxed),
            lease_renew_successes: self.lease_renew_successes.load(Ordering::Relaxed),
            lease_renew_failures: self.lease_renew_failures.load(Ordering::Relaxed),
            lease_renew_lost: self.lease_renew_lost.load(Ordering::Relaxed),
            lease_renew_retries: self.lease_renew_retries.load(Ordering::Relaxed),
            lease_release_attempts: self.lease_release_attempts.load(Ordering::Relaxed),
            lease_release_successes: self.lease_release_successes.load(Ordering::Relaxed),
            lease_release_failures: self.lease_release_failures.load(Ordering::Relaxed),
            metadata_cas_attempts: self.metadata_cas_attempts.load(Ordering::Relaxed),
            metadata_cas_successes: self.metadata_cas_successes.load(Ordering::Relaxed),
            metadata_cas_conflicts: self.metadata_cas_conflicts.load(Ordering::Relaxed),
            watermark_advances: self.watermark_advances.load(Ordering::Relaxed),
            watermark_rejects: self.watermark_rejects.load(Ordering::Relaxed),
            control_idempotency_commits: self.control_idempotency_commits.load(Ordering::Relaxed),
            control_idempotency_replays: self.control_idempotency_replays.load(Ordering::Relaxed),
            repair_runs: self.repair_runs.load(Ordering::Relaxed),
            repair_actions: self.repair_actions.load(Ordering::Relaxed),
            node_heartbeat_writes: self.node_heartbeat_writes.load(Ordering::Relaxed),
            node_heartbeat_prunes: self.node_heartbeat_prunes.load(Ordering::Relaxed),
            controller_runs: self.controller_runs.load(Ordering::Relaxed),
            controller_reassignments: self.controller_reassignments.load(Ordering::Relaxed),
            controller_failovers: self.controller_failovers.load(Ordering::Relaxed),
            controller_pending_failovers: self.controller_pending_failovers.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPlacement {
    owners: BTreeMap<String, String>,
}

pub struct RoutedGraphCluster {
    base_path: String,
    local_node_id: String,
    placement: ShardPlacement,
    object_store: Arc<dyn ObjectStore>,
    options: GraphOpenOptions,
    shards: BTreeMap<String, GraphShard>,
    leases: Arc<RwLock<BTreeMap<String, ShardLease>>>,
}

pub struct GraphNode {
    cluster: RoutedGraphCluster,
    control: Arc<GraphControlPlane>,
    lease_renewer: LeaseRenewalHandle,
    heartbeat: NodeHeartbeatHandle,
}

pub struct ManagedGraphNode {
    node: Arc<TokioRwLock<Option<GraphNode>>>,
    shard_refresher: ShardRefreshHandle,
    metrics: Arc<GraphNodeMaintenanceMetrics>,
}

#[derive(Clone, Debug)]
pub struct GraphNodeRuntimeConfig {
    pub lease_ttl: Duration,
    pub lease_renew_interval: Duration,
    pub shard_refresh_interval: Duration,
    pub options: GraphOpenOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardLease {
    pub cell_id: String,
    pub owner_node_id: String,
    pub lease_token: u64,
    pub expires_at_ms: u64,
}

pub struct LeaseRenewalHandle {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphNodeHealthState {
    Active,
    Draining,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphNodeHeartbeat {
    pub node_id: String,
    pub state: GraphNodeHealthState,
    pub started_at_ms: u64,
    pub last_seen_ms: u64,
    pub generation: u64,
}

pub struct NodeHeartbeatHandle {
    stop_tx: watch::Sender<bool>,
    state_tx: watch::Sender<GraphNodeHealthState>,
    task: JoinHandle<Result<()>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphClusterControllerConfig {
    pub cell_ids: Vec<String>,
    pub heartbeat_ttl: Duration,
    pub lease_ttl: Duration,
    pub rebalance_mode: GraphClusterRebalanceMode,
    pub discover_existing_cells: bool,
    pub max_expired_heartbeats_to_prune: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphClusterRebalanceMode {
    StabilityFirst,
    Rendezvous,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphShardReassignment {
    pub cell_id: String,
    pub previous_owner_node_id: Option<String>,
    pub new_owner_node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphPendingFailover {
    pub cell_id: String,
    pub current_owner_node_id: String,
    pub target_owner_node_id: String,
    pub lease_expires_at_ms: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphClusterControllerReport {
    pub now_ms: u64,
    pub controlled_cells: Vec<String>,
    pub active_nodes: Vec<String>,
    pub draining_nodes: Vec<String>,
    pub expired_nodes: Vec<String>,
    pub pruned_expired_nodes: Vec<String>,
    pub unassigned_cells: Vec<String>,
    pub reassignments: Vec<GraphShardReassignment>,
    pub failed_over_leases: Vec<ShardLease>,
    pub pending_failovers: Vec<GraphPendingFailover>,
}

pub struct GraphClusterControllerHandle {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

pub struct ShardRefreshHandle {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphShardRefreshReport {
    pub opened_cells: Vec<String>,
    pub closed_cells: Vec<String>,
    pub retained_cells: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphNodeMaintenanceMetricsSnapshot {
    pub shard_refresh_attempts: u64,
    pub shard_refresh_successes: u64,
    pub shard_refresh_failures: u64,
    pub shard_refresh_opened_cells: u64,
    pub shard_refresh_closed_cells: u64,
}

#[derive(Default)]
struct GraphNodeMaintenanceMetrics {
    shard_refresh_attempts: AtomicU64,
    shard_refresh_successes: AtomicU64,
    shard_refresh_failures: AtomicU64,
    shard_refresh_opened_cells: AtomicU64,
    shard_refresh_closed_cells: AtomicU64,
}

impl GraphNodeMaintenanceMetrics {
    fn snapshot(&self) -> GraphNodeMaintenanceMetricsSnapshot {
        GraphNodeMaintenanceMetricsSnapshot {
            shard_refresh_attempts: self.shard_refresh_attempts.load(Ordering::Relaxed),
            shard_refresh_successes: self.shard_refresh_successes.load(Ordering::Relaxed),
            shard_refresh_failures: self.shard_refresh_failures.load(Ordering::Relaxed),
            shard_refresh_opened_cells: self.shard_refresh_opened_cells.load(Ordering::Relaxed),
            shard_refresh_closed_cells: self.shard_refresh_closed_cells.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRollup {
    pub cell_id: String,
    pub edge_type: String,
    pub base_epoch: GraphEpoch,
    pub posting_chunks: u64,
    pub matrix_edge_count: u64,
    pub supernode_groups: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactGcResult {
    pub deleted_keys: u64,
    pub retained_keys: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeltaGcResult {
    pub deleted_delta_keys: u64,
    pub retained_delta_keys: u64,
    pub compacted_through_epoch: GraphEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatrixTile {
    cell_id: String,
    edge_type: String,
    base_epoch: GraphEpoch,
    tile_size: u64,
    direction: ArtifactDirection,
    tile_row: u64,
    tile_col: u64,
    rows: BTreeMap<VertexId, Vec<VertexId>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SupernodeDeltaOverlay {
    plus: BTreeSet<VertexId>,
    minus: BTreeSet<VertexId>,
}

impl SupernodeDeltaOverlay {
    fn is_empty(&self) -> bool {
        self.plus.is_empty() && self.minus.is_empty()
    }
}

struct TraversalVerifyRequest<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    read_epoch: GraphEpoch,
    max_hops: u8,
    root_limit: usize,
    edges: &'a [EdgeRecord],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphBlasCscManifest {
    cell_id: String,
    edge_type: String,
    base_epoch: GraphEpoch,
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
        self.rows.retain(|_, dsts| {
            dsts.sort_unstable();
            dsts.dedup();
            live_edges = live_edges.saturating_add(dsts.len() as u64);
            !dsts.is_empty()
        });
        self.live_edges = live_edges;
    }

    fn reversed(&self) -> MatrixRows {
        let mut reversed = MatrixRows::default();
        for (src, dsts) in &self.rows {
            for dst in dsts {
                reversed.push(*dst, *src);
            }
        }
        reversed.normalize();
        reversed
    }

    fn to_adjacency(&self) -> MatrixAdjacency {
        self.rows
            .iter()
            .map(|(src, dsts)| (*src, dsts.iter().copied().collect()))
            .collect()
    }
}

mod artifact_build;
mod cluster;
mod control_metadata;
mod control_plane;
mod controller;
mod supernode;
mod traversal;
mod verify;

pub use control_metadata::{
    GraphControlCellDropReport, GraphControlEdgeWatermark, GraphControlIdempotencyRecord,
    GraphControlRepairReport, GraphControlWatermark, GraphShardCatalogEntry,
};

pub fn local_object_store(path: impl AsRef<std::path::Path>) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(path.as_ref())?) as Arc<dyn ObjectStore>)
}

pub fn object_store_from_env(env_file: Option<String>) -> Result<Arc<dyn ObjectStore>> {
    Ok(slatedb::admin::load_object_store_from_env(env_file)?)
}

const GRAPH_CONTROL_TXN_MAX_RETRIES: usize = 32;
const GRAPH_CONTROLLER_EXPIRED_HEARTBEAT_PRUNE_LIMIT: usize = 1024;
const CONTROL_PLACEMENT_PREFIX: &str = "control/placement/";
const CONTROL_NODE_PREFIX: &str = "control/node/";

fn control_placement_key(cell_id: &str) -> String {
    format!("{CONTROL_PLACEMENT_PREFIX}{cell_id}")
}

fn control_node_key(node_id: &str) -> String {
    format!("{CONTROL_NODE_PREFIX}{node_id}")
}

fn control_lease_key(cell_id: &str) -> String {
    format!("control/lease/{cell_id}")
}

fn control_lease_token_key(cell_id: &str) -> String {
    format!("control/lease_token/{cell_id}")
}

fn encode_control_placement(cell_id: &str, owner_node_id: &str) -> Vec<u8> {
    format!("placement1\t{cell_id}\t{owner_node_id}\n").into_bytes()
}

fn decode_control_placement(key: &str, value: &[u8]) -> Result<(String, String)> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 3 || parts[0] != "placement1" {
        return corrupt(key, "expected placement1 record with 3 fields");
    }
    validate_component("cell_id", parts[1])?;
    validate_component("node_id", parts[2])?;
    Ok((parts[1].to_string(), parts[2].to_string()))
}

fn encode_node_health_state(state: GraphNodeHealthState) -> &'static str {
    match state {
        GraphNodeHealthState::Active => "active",
        GraphNodeHealthState::Draining => "draining",
    }
}

fn decode_node_health_state(key: &str, value: &str) -> Result<GraphNodeHealthState> {
    match value {
        "active" => Ok(GraphNodeHealthState::Active),
        "draining" => Ok(GraphNodeHealthState::Draining),
        _ => corrupt(key, format!("unknown node health state {value}")),
    }
}

fn encode_node_heartbeat(heartbeat: &GraphNodeHeartbeat) -> Vec<u8> {
    format!(
        "node1\t{}\t{}\t{}\t{}\t{}\n",
        heartbeat.node_id,
        encode_node_health_state(heartbeat.state),
        heartbeat.started_at_ms,
        heartbeat.last_seen_ms,
        heartbeat.generation
    )
    .into_bytes()
}

fn decode_node_heartbeat(key: &str, value: &[u8]) -> Result<GraphNodeHeartbeat> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 6 || parts[0] != "node1" {
        return corrupt(key, "expected node1 record with 6 fields");
    }
    validate_component("node_id", parts[1])?;
    Ok(GraphNodeHeartbeat {
        node_id: parts[1].to_string(),
        state: decode_node_health_state(key, parts[2])?,
        started_at_ms: parse_u64(key, parts[3], "started_at_ms")?,
        last_seen_ms: parse_u64(key, parts[4], "last_seen_ms")?,
        generation: parse_u64(key, parts[5], "generation")?,
    })
}

fn encode_shard_lease(lease: &ShardLease) -> Vec<u8> {
    format!(
        "lease1\t{}\t{}\t{}\t{}\n",
        lease.cell_id, lease.owner_node_id, lease.lease_token, lease.expires_at_ms
    )
    .into_bytes()
}

fn decode_shard_lease(key: &str, value: &[u8]) -> Result<ShardLease> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 5 || parts[0] != "lease1" {
        return corrupt(key, "expected lease1 record with 5 fields");
    }
    validate_component("cell_id", parts[1])?;
    validate_component("node_id", parts[2])?;
    Ok(ShardLease {
        cell_id: parts[1].to_string(),
        owner_node_id: parts[2].to_string(),
        lease_token: parse_u64(key, parts[3], "lease_token")?,
        expires_at_ms: parse_u64(key, parts[4], "expires_at_ms")?,
    })
}

async fn read_control_txn(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    txn.mark_read([key.as_bytes()])?;
    Ok(txn
        .get_with_options(key.as_bytes(), &control_read_options())
        .await?)
}

async fn read_control_counter_txn(txn: &DbTransaction, key: &str) -> Result<u64> {
    match read_control_txn(txn, key).await? {
        Some(value) => decode_u64_be(key, &value),
        None => Ok(0),
    }
}

async fn commit_control_txn(txn: DbTransaction) -> Result<()> {
    let options = WriteOptions {
        await_durable: true,
        ..Default::default()
    };
    txn.commit_with_options(&options).await?;
    Ok(())
}

fn control_read_options() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    }
}

fn control_scan_options() -> ScanOptions {
    ScanOptions::default()
        .with_durability_filter(DurabilityLevel::Remote)
        .with_cache_blocks(false)
}

fn encode_u64_be(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_u64_be(key: &str, value: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("expected 8 bytes, got {}", value.len()),
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn lease_ttl_ms(ttl: Duration) -> Result<u64> {
    u64::try_from(ttl.as_millis()).map_err(|err| GraphError::CorruptValue {
        key: "control/lease_ttl".to_string(),
        reason: format!("lease ttl is too large: {err}"),
    })
}

fn now_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> GraphError {
    GraphError::CorruptValue {
        key: "control/lease_lock".to_string(),
        reason: "lease state lock poisoned".to_string(),
    }
}

fn append_posting_chunks(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
    chunk_size: usize,
    mut adjacency: BTreeMap<VertexId, Vec<VertexId>>,
    chunks: &mut Vec<PostingChunk>,
) {
    for (owner, vertices) in adjacency.iter_mut() {
        vertices.sort_unstable();
        vertices.dedup();
        for (chunk_id, chunk) in vertices.chunks(chunk_size).enumerate() {
            chunks.push(PostingChunk {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                direction,
                owner: *owner,
                base_epoch,
                chunk_id: chunk_id as u64,
                vertices: chunk.to_vec(),
            });
        }
    }
}

fn posting_manifests_from_chunks(chunks: &[PostingChunk]) -> Result<Vec<PostingChunkManifest>> {
    let mut grouped = BTreeMap::<
        (String, String, ArtifactDirection, VertexId, GraphEpoch),
        Vec<&PostingChunk>,
    >::new();
    for chunk in chunks {
        grouped
            .entry((
                chunk.cell_id.clone(),
                chunk.edge_type.clone(),
                chunk.direction,
                chunk.owner,
                chunk.base_epoch,
            ))
            .or_default()
            .push(chunk);
    }

    let mut manifests = Vec::with_capacity(grouped.len());
    for ((cell_id, edge_type, direction, owner, base_epoch), mut owner_chunks) in grouped {
        owner_chunks.sort_by_key(|chunk| chunk.chunk_id);
        let mut vertex_count = 0_u64;
        let mut chunk_checksums = Vec::with_capacity(owner_chunks.len());
        for (expected_id, chunk) in owner_chunks.iter().enumerate() {
            let expected_id = expected_id as u64;
            if chunk.chunk_id != expected_id {
                return Err(GraphError::CorruptValue {
                    key: posting_key(chunk),
                    reason: format!(
                        "posting chunk ids must be contiguous before publish: expected {expected_id}, got {}",
                        chunk.chunk_id
                    ),
                });
            }
            vertex_count = vertex_count.saturating_add(chunk.vertices.len() as u64);
            chunk_checksums.push(posting_chunk_checksum(chunk));
        }
        manifests.push(PostingChunkManifest {
            cell_id,
            edge_type,
            direction,
            owner,
            base_epoch,
            chunk_count: owner_chunks.len() as u64,
            vertex_count,
            chunk_checksums,
        });
    }
    Ok(manifests)
}

fn posting_artifact_manifest_from_owner_manifests(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    manifests: &[PostingChunkManifest],
) -> Result<Option<PostingArtifactManifest>> {
    if manifests.is_empty() {
        return Ok(None);
    }
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    checksum_u64(&mut checksum, base_epoch);
    let mut chunk_count = 0_u64;
    let mut vertex_count = 0_u64;
    for manifest in manifests {
        if manifest.cell_id != cell_id
            || manifest.edge_type != edge_type
            || manifest.base_epoch != base_epoch
        {
            return Err(GraphError::CorruptValue {
                key: posting_manifest_key(
                    &manifest.cell_id,
                    &manifest.edge_type,
                    manifest.direction,
                    manifest.owner,
                    manifest.base_epoch,
                ),
                reason: "posting owner manifest does not belong to artifact epoch".to_string(),
            });
        }
        checksum_u64(
            &mut checksum,
            match manifest.direction {
                ArtifactDirection::Out => 1,
                ArtifactDirection::In => 2,
            },
        );
        checksum_u64(&mut checksum, manifest.owner);
        checksum_u64(&mut checksum, manifest.chunk_count);
        checksum_u64(&mut checksum, manifest.vertex_count);
        for chunk_checksum in &manifest.chunk_checksums {
            checksum_u64(&mut checksum, *chunk_checksum);
        }
        chunk_count = chunk_count.saturating_add(manifest.chunk_count);
        vertex_count = vertex_count.saturating_add(manifest.vertex_count);
    }
    Ok(Some(PostingArtifactManifest {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        base_epoch,
        owner_manifest_count: manifests.len() as u64,
        chunk_count,
        vertex_count,
        checksum,
    }))
}

#[allow(clippy::too_many_arguments)]
fn append_current_supernode_chunk(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
    owner: VertexId,
    chunks: &mut Vec<PostingChunk>,
    chunk_bounds: &mut Vec<SupernodeChunkBound>,
    vertices: &mut Vec<VertexId>,
) {
    let chunk_id = chunk_bounds.len() as u64;
    if let (Some(first), Some(last)) = (vertices.first().copied(), vertices.last().copied()) {
        chunk_bounds.push(SupernodeChunkBound {
            chunk_id,
            first,
            last,
        });
    }
    chunks.push(PostingChunk {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        direction,
        owner,
        base_epoch,
        chunk_id,
        vertices: std::mem::take(vertices),
    });
}

#[allow(clippy::too_many_arguments)]
fn append_supernode_chunks(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
    degree_threshold: u64,
    chunk_size: usize,
    mut adjacency: BTreeMap<VertexId, Vec<VertexId>>,
    chunks: &mut Vec<PostingChunk>,
    groups: &mut Vec<SupernodeGroup>,
) {
    for (owner, vertices) in adjacency.iter_mut() {
        vertices.sort_unstable();
        vertices.dedup();
        let degree = vertices.len() as u64;
        if degree < degree_threshold {
            continue;
        }
        let first_chunk_id = chunks.len() as u64;
        let mut chunk_bounds = Vec::new();
        for (offset, chunk) in vertices.chunks(chunk_size).enumerate() {
            let chunk_id = offset as u64;
            if let (Some(first), Some(last)) = (chunk.first(), chunk.last()) {
                chunk_bounds.push(SupernodeChunkBound {
                    chunk_id,
                    first: *first,
                    last: *last,
                });
            }
            chunks.push(PostingChunk {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                direction,
                owner: *owner,
                base_epoch,
                chunk_id,
                vertices: chunk.to_vec(),
            });
        }
        groups.push(SupernodeGroup {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            direction,
            vertex_id: *owner,
            base_epoch,
            degree,
            chunk_count: (chunks.len() as u64).saturating_sub(first_chunk_id),
            page_size: chunk_size as u64,
            chunk_bounds,
        });
    }
}

fn supernode_chunk_bounds(chunks: &[&PostingChunk]) -> Vec<SupernodeChunkBound> {
    let mut bounds: Vec<_> = chunks
        .iter()
        .filter_map(|chunk| {
            let first = chunk.vertices.first().copied()?;
            let last = chunk.vertices.last().copied()?;
            Some(SupernodeChunkBound {
                chunk_id: chunk.chunk_id,
                first,
                last,
            })
        })
        .collect();
    bounds.sort_by_key(|bound| (bound.first, bound.last, bound.chunk_id));
    bounds
}

fn supernode_bound_for_vertex(
    group: &SupernodeGroup,
    vertex: VertexId,
) -> Option<&SupernodeChunkBound> {
    let idx = group
        .chunk_bounds
        .partition_point(|bound| bound.last < vertex);
    let bound = group.chunk_bounds.get(idx)?;
    (bound.first <= vertex && vertex <= bound.last).then_some(bound)
}

fn rendezvous_score(cell_id: &str, node_id: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    cell_id.hash(&mut hasher);
    node_id.hash(&mut hasher);
    hasher.finish()
}

fn matrix_tiles_from_edges(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    tile_size: u64,
    direction: ArtifactDirection,
    edges: &[EdgeRecord],
) -> Vec<MatrixTile> {
    let mut tiles: BTreeMap<(u64, u64), BTreeMap<VertexId, Vec<VertexId>>> = BTreeMap::new();
    for edge in edges {
        let (row, col) = match direction {
            ArtifactDirection::Out => (edge.src, edge.dst),
            ArtifactDirection::In => (edge.dst, edge.src),
        };
        let key = (row / tile_size, col / tile_size);
        tiles
            .entry(key)
            .or_default()
            .entry(row)
            .or_default()
            .push(col);
    }
    tiles
        .into_iter()
        .map(|((tile_row, tile_col), mut rows)| {
            for values in rows.values_mut() {
                values.sort_unstable();
                values.dedup();
            }
            MatrixTile {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                base_epoch,
                tile_size,
                direction,
                tile_row,
                tile_col,
                rows,
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn append_matrix_tiles_from_rows(
    shard: &GraphShard,
    artifact_lock: &CellWriteLock,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    tile_size: u64,
    direction: ArtifactDirection,
    rows: &BTreeMap<VertexId, Vec<VertexId>>,
) -> Result<u64> {
    let mut current_tile_row = None;
    let mut tile_columns: BTreeMap<u64, BTreeMap<VertexId, Vec<VertexId>>> = BTreeMap::new();
    let mut tile_count = 0_u64;

    for (row, cols) in rows {
        let tile_row = row / tile_size;
        if let Some(flush_tile_row) = current_tile_row.filter(|current| *current != tile_row) {
            tile_count = tile_count.saturating_add(
                flush_matrix_tile_row(
                    shard,
                    artifact_lock,
                    batch,
                    pending_writes,
                    cell_id,
                    edge_type,
                    base_epoch,
                    tile_size,
                    direction,
                    flush_tile_row,
                    &mut tile_columns,
                )
                .await?,
            );
        }
        current_tile_row = Some(tile_row);
        for col in cols {
            tile_columns
                .entry(col / tile_size)
                .or_default()
                .entry(*row)
                .or_default()
                .push(*col);
        }
    }

    if let Some(tile_row) = current_tile_row {
        tile_count = tile_count.saturating_add(
            flush_matrix_tile_row(
                shard,
                artifact_lock,
                batch,
                pending_writes,
                cell_id,
                edge_type,
                base_epoch,
                tile_size,
                direction,
                tile_row,
                &mut tile_columns,
            )
            .await?,
        );
    }
    Ok(tile_count)
}

#[allow(clippy::too_many_arguments)]
async fn flush_matrix_tile_row(
    shard: &GraphShard,
    artifact_lock: &CellWriteLock,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    tile_size: u64,
    direction: ArtifactDirection,
    tile_row: u64,
    tile_columns: &mut BTreeMap<u64, BTreeMap<VertexId, Vec<VertexId>>>,
) -> Result<u64> {
    let mut tile_count = 0_u64;
    let columns = std::mem::take(tile_columns);
    for (tile_col, mut rows) in columns {
        for cols in rows.values_mut() {
            cols.sort_unstable();
            cols.dedup();
        }
        let tile = MatrixTile {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            base_epoch,
            tile_size,
            direction,
            tile_row,
            tile_col,
            rows,
        };
        put_artifact_record(
            shard,
            Some(artifact_lock),
            cell_id,
            "build_matrix_tiles",
            batch,
            pending_writes,
            matrix_tile_key(&tile),
            encode_matrix_tile(&tile),
        )
        .await?;
        tile_count = tile_count.saturating_add(1);
    }
    Ok(tile_count)
}

#[cfg(feature = "graphblas")]
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

#[cfg(feature = "graphblas")]
fn matrix_rows_indices(rows: &MatrixRows, vertices: &[VertexId]) -> Result<Vec<u64>> {
    let mut indices = Vec::with_capacity(rows.live_edges as usize);
    append_matrix_rows_indices(rows, vertices, |ordinal| {
        indices.push(ordinal);
        Ok(())
    })?;
    Ok(indices)
}

#[cfg(feature = "graphblas")]
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

const GRAPH_VERIFY_MISMATCH_SAMPLES: usize = 64;

type RelationshipPropertyIndexEntry = (String, String, VertexId, VertexId, RelationshipId);

fn graph_export_digest(
    cell_id: &str,
    edge_type: &str,
    read_epoch: GraphEpoch,
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
    sorted.sort_by_key(|edge| (edge.src, edge.dst, edge.epoch));
    for edge in sorted {
        checksum_u64(&mut hash, edge.src);
        checksum_u64(&mut hash, edge.dst);
        checksum_u64(&mut hash, edge.epoch);
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

pub(crate) fn apply_delta_overlay(
    adjacency: &mut BTreeMap<VertexId, BTreeSet<VertexId>>,
    deltas: Vec<DeltaRecord>,
    base_epoch: GraphEpoch,
    read_epoch: GraphEpoch,
) -> u64 {
    let mut applied = 0_u64;
    for delta in deltas {
        if delta.edge.epoch <= base_epoch || delta.edge.epoch > read_epoch {
            continue;
        }
        applied += 1;
        match delta.kind {
            DeltaKind::Plus => {
                adjacency
                    .entry(delta.edge.src)
                    .or_default()
                    .insert(delta.edge.dst);
            }
            DeltaKind::Minus => {
                if let Some(row) = adjacency.get_mut(&delta.edge.src) {
                    row.remove(&delta.edge.dst);
                }
            }
        }
    }
    applied
}

fn next_live_base_vertex(
    vertices: &[VertexId],
    index: &mut usize,
    overlay: &SupernodeDeltaOverlay,
) -> Option<VertexId> {
    while let Some(vertex) = vertices.get(*index).copied() {
        *index += 1;
        if !overlay.minus.contains(&vertex) {
            return Some(vertex);
        }
    }
    None
}

fn merge_next_vertex(base: Option<VertexId>, plus: Option<VertexId>) -> Option<VertexId> {
    match (base, plus) {
        (Some(base), Some(plus)) => Some(base.min(plus)),
        (Some(base), None) => Some(base),
        (None, Some(plus)) => Some(plus),
        (None, None) => None,
    }
}

fn direction_str(direction: ArtifactDirection) -> &'static str {
    match direction {
        ArtifactDirection::Out => "out",
        ArtifactDirection::In => "in",
    }
}

fn parse_direction(value: &str) -> Result<ArtifactDirection> {
    match value {
        "out" => Ok(ArtifactDirection::Out),
        "in" => Ok(ArtifactDirection::In),
        other => Err(GraphError::CorruptValue {
            key: "direction".to_string(),
            reason: format!("invalid artifact direction {other}"),
        }),
    }
}

fn parse_last_key_component(key: &str, field: &str) -> Result<u64> {
    let Some(value) = key.rsplit('/').next() else {
        return corrupt(key, "missing key component");
    };
    parse_u64(key, value, field)
}

fn posting_key(chunk: &PostingChunk) -> String {
    posting_chunk_key(
        &chunk.cell_id,
        &chunk.edge_type,
        chunk.direction,
        chunk.owner,
        chunk.base_epoch,
        chunk.chunk_id,
    )
}

fn posting_chunk_key(
    cell_id: &str,
    edge_type: &str,
    direction: ArtifactDirection,
    owner: VertexId,
    base_epoch: GraphEpoch,
    chunk_id: u64,
) -> String {
    format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/{}/{owner:020}/{base_epoch:020}/{chunk_id:020}",
        direction_str(direction)
    )
}

fn posting_prefix(
    cell_id: &str,
    edge_type: &str,
    direction: ArtifactDirection,
    owner: VertexId,
    base_epoch: GraphEpoch,
) -> String {
    format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/{}/{owner:020}/{base_epoch:020}/",
        direction_str(direction)
    )
}

fn posting_manifest_key(
    cell_id: &str,
    edge_type: &str,
    direction: ArtifactDirection,
    owner: VertexId,
    base_epoch: GraphEpoch,
) -> String {
    format!(
        "cell/{cell_id}/artifact/posting_manifest/{edge_type}/{}/{owner:020}/{base_epoch:020}",
        direction_str(direction)
    )
}

fn posting_manifest_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/posting_manifest/{edge_type}/")
}

fn posting_artifact_manifest_key(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> String {
    format!("cell/{cell_id}/artifact/posting_epoch_manifest/{edge_type}/{base_epoch:020}")
}

fn posting_artifact_manifest_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/posting_epoch_manifest/{edge_type}/")
}

fn encode_posting_chunk(chunk: &PostingChunk) -> Vec<u8> {
    format!(
        "posting1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        chunk.cell_id,
        chunk.edge_type,
        direction_str(chunk.direction),
        chunk.owner,
        chunk.base_epoch,
        chunk.chunk_id,
        encode_vertices(&chunk.vertices)
    )
    .into_bytes()
}

fn decode_posting_chunk(key: &str, value: &[u8]) -> Result<PostingChunk> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "posting1" {
        return corrupt(key, "expected posting1 record with 8 fields");
    }
    Ok(PostingChunk {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        direction: parse_direction(parts[3])?,
        owner: parse_u64(key, parts[4], "owner")?,
        base_epoch: parse_u64(key, parts[5], "base_epoch")?,
        chunk_id: parse_u64(key, parts[6], "chunk_id")?,
        vertices: decode_vertices(key, parts[7])?,
    })
}

fn posting_chunk_checksum(chunk: &PostingChunk) -> u64 {
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    checksum_u64(
        &mut checksum,
        match chunk.direction {
            ArtifactDirection::Out => 1,
            ArtifactDirection::In => 2,
        },
    );
    checksum_u64(&mut checksum, chunk.owner);
    checksum_u64(&mut checksum, chunk.base_epoch);
    checksum_u64(&mut checksum, chunk.chunk_id);
    checksum_u64(&mut checksum, chunk.vertices.len() as u64);
    for vertex in &chunk.vertices {
        checksum_u64(&mut checksum, *vertex);
    }
    checksum
}

fn encode_posting_manifest(manifest: &PostingChunkManifest) -> Vec<u8> {
    format!(
        "posting_manifest1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        manifest.cell_id,
        manifest.edge_type,
        direction_str(manifest.direction),
        manifest.owner,
        manifest.base_epoch,
        manifest.chunk_count,
        manifest.vertex_count,
        encode_u64_list(&manifest.chunk_checksums)
    )
    .into_bytes()
}

fn decode_posting_manifest(key: &str, value: &[u8]) -> Result<PostingChunkManifest> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 9 || parts[0] != "posting_manifest1" {
        return corrupt(key, "expected posting_manifest1 record with 9 fields");
    }
    let chunk_count = parse_u64(key, parts[6], "chunk_count")?;
    let chunk_checksums = decode_u64_list(key, parts[8])?;
    let expected_checksums =
        usize::try_from(chunk_count).map_err(|err| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("posting manifest chunk count does not fit usize: {err}"),
        })?;
    if chunk_checksums.len() != expected_checksums {
        return corrupt(
            key,
            "posting manifest checksum count does not match chunk count",
        );
    }
    validate_posting_manifest(
        key,
        PostingChunkManifest {
            cell_id: parts[1].to_string(),
            edge_type: parts[2].to_string(),
            direction: parse_direction(parts[3])?,
            owner: parse_u64(key, parts[4], "owner")?,
            base_epoch: parse_u64(key, parts[5], "base_epoch")?,
            chunk_count,
            vertex_count: parse_u64(key, parts[7], "vertex_count")?,
            chunk_checksums,
        },
    )
}

fn validate_posting_manifest(
    key: &str,
    manifest: PostingChunkManifest,
) -> Result<PostingChunkManifest> {
    if manifest.vertex_count > 0 && manifest.chunk_count == 0 {
        return corrupt(key, "posting manifest vertex count requires chunks");
    }
    let parts: Vec<_> = key.split('/').collect();
    let ["cell", cell_id, "artifact", "posting_manifest", edge_type, direction, owner, base_epoch] =
        parts.as_slice()
    else {
        return corrupt(key, "invalid posting manifest key");
    };
    if manifest.cell_id != *cell_id
        || manifest.edge_type != *edge_type
        || direction_str(manifest.direction) != *direction
        || manifest.owner != parse_u64(key, owner, "owner")?
        || manifest.base_epoch != parse_u64(key, base_epoch, "base_epoch")?
    {
        return corrupt(key, "posting manifest key does not match value");
    }
    Ok(manifest)
}

fn encode_posting_artifact_manifest(manifest: &PostingArtifactManifest) -> Vec<u8> {
    format!(
        "posting_epoch_manifest1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        manifest.cell_id,
        manifest.edge_type,
        manifest.base_epoch,
        manifest.owner_manifest_count,
        manifest.chunk_count,
        manifest.vertex_count,
        manifest.checksum
    )
    .into_bytes()
}

fn decode_posting_artifact_manifest(key: &str, value: &[u8]) -> Result<PostingArtifactManifest> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "posting_epoch_manifest1" {
        return corrupt(key, "expected posting_epoch_manifest1 record with 8 fields");
    }
    validate_posting_artifact_manifest(
        key,
        PostingArtifactManifest {
            cell_id: parts[1].to_string(),
            edge_type: parts[2].to_string(),
            base_epoch: parse_u64(key, parts[3], "base_epoch")?,
            owner_manifest_count: parse_u64(key, parts[4], "owner_manifest_count")?,
            chunk_count: parse_u64(key, parts[5], "chunk_count")?,
            vertex_count: parse_u64(key, parts[6], "vertex_count")?,
            checksum: parse_u64(key, parts[7], "checksum")?,
        },
    )
}

fn validate_posting_artifact_manifest(
    key: &str,
    manifest: PostingArtifactManifest,
) -> Result<PostingArtifactManifest> {
    if manifest.vertex_count > 0 && manifest.chunk_count == 0 {
        return corrupt(key, "posting epoch manifest vertex count requires chunks");
    }
    if manifest.chunk_count > 0 && manifest.owner_manifest_count == 0 {
        return corrupt(
            key,
            "posting epoch manifest chunk count requires owner manifests",
        );
    }
    let parts: Vec<_> = key.split('/').collect();
    let ["cell", cell_id, "artifact", "posting_epoch_manifest", edge_type, base_epoch] =
        parts.as_slice()
    else {
        return corrupt(key, "invalid posting epoch manifest key");
    };
    if manifest.cell_id != *cell_id
        || manifest.edge_type != *edge_type
        || manifest.base_epoch != parse_u64(key, base_epoch, "base_epoch")?
    {
        return corrupt(key, "posting epoch manifest key does not match value");
    }
    Ok(manifest)
}

fn matrix_manifest_key(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> String {
    format!("cell/{cell_id}/artifact/matrix_manifest/{edge_type}/{base_epoch:020}")
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
    base_epoch: GraphEpoch,
    direction: ArtifactDirection,
) -> String {
    format!(
        "cell/{cell_id}/artifact/matrix/{edge_type}/{base_epoch:020}/{}/",
        direction_str(direction)
    )
}

fn graphblas_csc_key(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> String {
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
    base_epoch: GraphEpoch,
) -> String {
    format!("cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{base_epoch:020}/")
}

fn graphblas_csc_chunk_key(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    field: &str,
    chunk_id: u64,
) -> String {
    format!(
        "cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{base_epoch:020}/{field}/{chunk_id:020}"
    )
}

fn rollup_key(cell_id: &str, edge_type: &str, base_epoch: GraphEpoch) -> String {
    format!("cell/{cell_id}/rollup/{edge_type}/{base_epoch:020}")
}

fn rollup_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/rollup/{edge_type}/")
}

fn graph_artifact_gc_prefixes(cell_id: &str, edge_type: &str) -> Vec<String> {
    vec![
        format!("cell/{cell_id}/artifact/posting/{edge_type}/"),
        posting_manifest_prefix(cell_id, edge_type),
        posting_artifact_manifest_prefix(cell_id, edge_type),
        matrix_manifest_prefix(cell_id, edge_type),
        format!("cell/{cell_id}/artifact/matrix/{edge_type}/"),
        graphblas_csc_prefix(cell_id, edge_type),
        graphblas_csc_chunk_prefix(cell_id, edge_type),
        format!("cell/{cell_id}/artifact/supernode/{edge_type}/"),
        supernode_artifact_manifest_prefix(cell_id, edge_type),
        rollup_prefix(cell_id, edge_type),
    ]
}

fn graph_artifact_epoch_from_key(key: &str) -> Result<Option<GraphEpoch>> {
    let parts: Vec<_> = key.split('/').collect();
    let epoch = match parts.as_slice() {
        ["cell", _, "artifact", "posting", _, _, _, base_epoch, ..] => Some(*base_epoch),
        ["cell", _, "artifact", "posting_manifest", _, _, _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "posting_epoch_manifest", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "matrix_manifest", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "matrix", _, base_epoch, ..] => Some(*base_epoch),
        ["cell", _, "artifact", "graphblas_csc", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "graphblas_csc_chunk", _, base_epoch, ..] => Some(*base_epoch),
        ["cell", _, "artifact", "supernode", _, _, _, base_epoch] => Some(*base_epoch),
        ["cell", _, "artifact", "supernode_epoch_manifest", _, base_epoch] => Some(*base_epoch),
        ["cell", _, "rollup", _, base_epoch] => Some(*base_epoch),
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
    artifact_lock: Option<&CellWriteLock>,
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
            artifact_lock,
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
    artifact_lock: Option<&CellWriteLock>,
    cell_id: &str,
    operation: &'static str,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
) -> Result<()> {
    if *pending_writes == 0 {
        return Ok(());
    }
    if let Some(lock) = artifact_lock {
        lock.renew().await?;
    }
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    shard
        .write_graph_batch_strict(cell_id, operation, batch_to_write)
        .await?;
    if let Some(lock) = artifact_lock {
        lock.renew().await?;
    }
    *pending_writes = 0;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PostingArtifactCleanupResult {
    pub(crate) deleted_keys: u64,
    pub(crate) cleanup_errors: u64,
    pub(crate) skipped_published_manifest: bool,
}

impl PostingArtifactCleanupResult {
    fn record_error<E>(
        &mut self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
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
            "posting artifact abort cleanup step failed"
        );
    }
}

pub(crate) async fn cleanup_unpublished_posting_artifact_epoch(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    operation: &'static str,
) -> PostingArtifactCleanupResult {
    let mut result = PostingArtifactCleanupResult::default();
    let manifest_key = posting_artifact_manifest_key(cell_id, edge_type, base_epoch);
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
                "read_posting_epoch_manifest",
                &err,
            );
            return result;
        }
    }

    let artifact_lock = match shard
        .acquire_posting_artifact_write_lock(cell_id, edge_type, base_epoch, operation)
        .await
    {
        Ok(lock) => lock,
        Err(err) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "acquire_posting_artifact_lock",
                &err,
            );
            return result;
        }
    };

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
                    "recheck_posting_epoch_manifest",
                    &err,
                );
                return Ok(());
            }
        }

        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        for (cleanup_step, prefix) in [
            (
                "scan_posting_chunks",
                format!("cell/{cell_id}/artifact/posting/{edge_type}/"),
            ),
            (
                "scan_posting_owner_manifests",
                posting_manifest_prefix(cell_id, edge_type),
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
                match graph_artifact_epoch_from_key(&key) {
                    Ok(Some(epoch)) if epoch == base_epoch => {
                        batch.delete(key.as_bytes());
                        pending_deletes += 1;
                    }
                    Ok(_) => {}
                    Err(err) => result.record_error(
                        cell_id,
                        edge_type,
                        base_epoch,
                        operation,
                        cleanup_step,
                        &err,
                    ),
                }
                if pending_deletes >= GRAPH_ARTIFACT_GC_BATCH_KEYS
                    && !flush_unpublished_posting_artifact_gc_batch_best_effort(
                        shard,
                        cell_id,
                        edge_type,
                        base_epoch,
                        operation,
                        &manifest_key,
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

        if flush_unpublished_posting_artifact_gc_batch_best_effort(
            shard,
            cell_id,
            edge_type,
            base_epoch,
            operation,
            &manifest_key,
            &artifact_lock,
            &mut batch,
            &mut pending_deletes,
            &mut result,
        )
        .await
        {
            shard.posting_chunk_cache.lock().await.retain(|key, _| {
                key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch != base_epoch
            });
        }

        Ok(())
    }
    .await;
    if let Err(err) = crate::release_cell_write_lock(artifact_lock, cleanup_run).await {
        result.record_error(
            cell_id,
            edge_type,
            base_epoch,
            operation,
            "release_posting_artifact_lock",
            &err,
        );
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn flush_unpublished_posting_artifact_gc_batch_best_effort(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    operation: &'static str,
    manifest_key: &str,
    artifact_lock: &CellWriteLock,
    batch: &mut GraphWriteBatch,
    pending_deletes: &mut usize,
    result: &mut PostingArtifactCleanupResult,
) -> bool {
    if *pending_deletes == 0 {
        return true;
    }
    if let Err(err) = artifact_lock.renew().await {
        result.record_error(
            cell_id,
            edge_type,
            base_epoch,
            operation,
            "renew_posting_artifact_lock",
            &err,
        );
        return false;
    }
    match shard.read_remote(manifest_key).await {
        Ok(Some(_)) => {
            result.skipped_published_manifest = true;
            return false;
        }
        Ok(None) => {}
        Err(err) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "recheck_posting_epoch_manifest_before_delete",
                &err,
            );
            return false;
        }
    }
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    let delete_count = *pending_deletes as u64;
    match shard
        .write_graph_batch_strict(cell_id, operation, batch_to_write)
        .await
    {
        Ok(()) => {
            result.deleted_keys = result.deleted_keys.saturating_add(delete_count);
            *pending_deletes = 0;
            true
        }
        Err(err) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "delete_unpublished_posting_artifacts",
                &err,
            );
            *pending_deletes = 0;
            true
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SupernodeArtifactCleanupResult {
    pub(crate) deleted_keys: u64,
    pub(crate) cleanup_errors: u64,
    pub(crate) skipped_published_manifest: bool,
}

impl SupernodeArtifactCleanupResult {
    fn record_error<E>(
        &mut self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
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
            "supernode artifact abort cleanup step failed"
        );
    }
}

pub(crate) async fn cleanup_unpublished_supernode_artifact_epoch(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    operation: &'static str,
) -> SupernodeArtifactCleanupResult {
    let mut result = SupernodeArtifactCleanupResult::default();
    let manifest_key = supernode_artifact_manifest_key(cell_id, edge_type, base_epoch);
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
                "read_supernode_epoch_manifest",
                &err,
            );
            return result;
        }
    }

    let artifact_lock = match shard
        .acquire_supernode_artifact_write_lock(cell_id, edge_type, base_epoch, operation)
        .await
    {
        Ok(lock) => lock,
        Err(err) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "acquire_supernode_artifact_lock",
                &err,
            );
            return result;
        }
    };

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
                    "recheck_supernode_epoch_manifest",
                    &err,
                );
                return Ok(());
            }
        }

        let mut batch = GraphWriteBatch::new();
        let mut pending_deletes = 0_usize;
        let posting_manifest_key = posting_artifact_manifest_key(cell_id, edge_type, base_epoch);
        let posting_epoch_published = match shard.read_remote(&posting_manifest_key).await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(err) => {
                result.record_error(
                    cell_id,
                    edge_type,
                    base_epoch,
                    operation,
                    "read_posting_epoch_manifest_before_supernode_cleanup",
                    &err,
                );
                true
            }
        };
        let mut prefixes = vec![(
            "scan_supernode_groups",
            format!("cell/{cell_id}/artifact/supernode/{edge_type}/"),
        )];
        if !posting_epoch_published {
            prefixes.push((
                "scan_unprotected_supernode_posting_chunks",
                format!("cell/{cell_id}/artifact/posting/{edge_type}/"),
            ));
        }
        for (cleanup_step, prefix) in prefixes {
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
                match graph_artifact_epoch_from_key(&key) {
                    Ok(Some(epoch)) if epoch == base_epoch => {
                        batch.delete(key.as_bytes());
                        pending_deletes += 1;
                    }
                    Ok(_) => {}
                    Err(err) => result.record_error(
                        cell_id,
                        edge_type,
                        base_epoch,
                        operation,
                        cleanup_step,
                        &err,
                    ),
                }
                if pending_deletes >= GRAPH_ARTIFACT_GC_BATCH_KEYS
                    && !flush_unpublished_supernode_artifact_gc_batch_best_effort(
                        shard,
                        cell_id,
                        edge_type,
                        base_epoch,
                        operation,
                        &manifest_key,
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

        if flush_unpublished_supernode_artifact_gc_batch_best_effort(
            shard,
            cell_id,
            edge_type,
            base_epoch,
            operation,
            &manifest_key,
            &artifact_lock,
            &mut batch,
            &mut pending_deletes,
            &mut result,
        )
        .await
        {
            shard.supernode_group_cache.lock().await.retain(|key, _| {
                key.cell_id != cell_id || key.edge_type != edge_type || key.base_epoch != base_epoch
            });
            shard
                .materialized_supernode_cache
                .lock()
                .await
                .retain(|key, _| {
                    key.cell_id != cell_id
                        || key.edge_type != edge_type
                        || key.base_epoch != base_epoch
                });
            if !posting_epoch_published {
                shard.posting_chunk_cache.lock().await.retain(|key, _| {
                    key.cell_id != cell_id
                        || key.edge_type != edge_type
                        || key.base_epoch != base_epoch
                });
            }
        }

        Ok(())
    }
    .await;
    if let Err(err) = crate::release_cell_write_lock(artifact_lock, cleanup_run).await {
        result.record_error(
            cell_id,
            edge_type,
            base_epoch,
            operation,
            "release_supernode_artifact_lock",
            &err,
        );
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn flush_unpublished_supernode_artifact_gc_batch_best_effort(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    operation: &'static str,
    manifest_key: &str,
    artifact_lock: &CellWriteLock,
    batch: &mut GraphWriteBatch,
    pending_deletes: &mut usize,
    result: &mut SupernodeArtifactCleanupResult,
) -> bool {
    if *pending_deletes == 0 {
        return true;
    }
    let attempted_deletes = *pending_deletes as u64;
    let batch_to_write = std::mem::replace(batch, GraphWriteBatch::new());
    *pending_deletes = 0;
    let locked_delete = async {
        artifact_lock.renew().await?;
        match shard.read_remote(manifest_key).await {
            Ok(Some(_)) => {
                return Err(GraphError::CorruptValue {
                    key: manifest_key.to_string(),
                    reason: "supernode manifest appeared before abort cleanup flush".to_string(),
                });
            }
            Ok(None) => {}
            Err(err) => return Err(err),
        }
        shard
            .write_graph_batch_strict(cell_id, operation, batch_to_write)
            .await?;
        artifact_lock.renew().await
    }
    .await;
    match locked_delete {
        Ok(()) => {
            result.deleted_keys = result.deleted_keys.saturating_add(attempted_deletes);
        }
        Err(GraphError::CorruptValue { key, reason }) if key == manifest_key => {
            result.skipped_published_manifest = true;
            tracing::warn!(
                target: "slatedb_graph_kernel",
                cell_id,
                edge_type,
                base_epoch,
                operation,
                reason,
                "supernode artifact abort cleanup skipped published manifest"
            );
            return false;
        }
        Err(err @ GraphError::CellWriteConflict { .. }) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "delete_unpublished_supernode_artifacts",
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
                "delete_unpublished_supernode_artifacts",
                &err,
            );
        }
    }
    true
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
        base_epoch: GraphEpoch,
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
    base_epoch: GraphEpoch,
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
        .acquire_matrix_artifact_write_lock(cell_id, edge_type, base_epoch, operation)
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
                matrix_tile_prefix(cell_id, edge_type, base_epoch, ArtifactDirection::Out),
            ),
            (
                "scan_matrix_in_tiles",
                matrix_tile_prefix(cell_id, edge_type, base_epoch, ArtifactDirection::In),
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
    if let Err(err) = crate::release_cell_write_lock(artifact_lock, cleanup_run).await {
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
    base_epoch: GraphEpoch,
    operation: &'static str,
    manifest_key: &str,
    artifact_lock: &crate::CellWriteLock,
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
        match shard.read_remote(manifest_key).await {
            Ok(Some(_)) => {
                return Err(GraphError::CorruptValue {
                    key: manifest_key.to_string(),
                    reason: "matrix manifest appeared before abort cleanup flush".to_string(),
                });
            }
            Ok(None) => {}
            Err(err) => return Err(err),
        }
        shard
            .write_graph_batch_strict(cell_id, operation, batch_to_write)
            .await?;
        artifact_lock.renew().await
    }
    .await;
    match locked_delete {
        Ok(()) => {
            result.deleted_keys = result.deleted_keys.saturating_add(attempted_deletes);
        }
        Err(GraphError::CorruptValue { key, reason }) if key == manifest_key => {
            result.skipped_published_manifest = true;
            tracing::warn!(
                target: "slatedb_graph_kernel",
                cell_id,
                edge_type,
                base_epoch,
                operation,
                reason,
                "matrix artifact abort cleanup skipped published manifest"
            );
            return false;
        }
        Err(err @ GraphError::CellWriteConflict { .. }) => {
            result.record_error(
                cell_id,
                edge_type,
                base_epoch,
                operation,
                "delete_artifact_batch",
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

fn encode_graph_rollup(rollup: &GraphRollup) -> Vec<u8> {
    format!(
        "graph_rollup1\t{}\t{}\t{}\t{}\t{}\t{}\n",
        rollup.cell_id,
        rollup.edge_type,
        rollup.base_epoch,
        rollup.posting_chunks,
        rollup.matrix_edge_count,
        rollup.supernode_groups
    )
    .into_bytes()
}

fn decode_graph_rollup(key: &str, value: &[u8]) -> Result<GraphRollup> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 7 || parts[0] != "graph_rollup1" {
        return corrupt(key, "expected graph_rollup1 record with 7 fields");
    }
    Ok(GraphRollup {
        cell_id: parts[1].to_string(),
        edge_type: parts[2].to_string(),
        base_epoch: parse_u64(key, parts[3], "base_epoch")?,
        posting_chunks: parse_u64(key, parts[4], "posting_chunks")?,
        matrix_edge_count: parse_u64(key, parts[5], "matrix_edge_count")?,
        supernode_groups: parse_u64(key, parts[6], "supernode_groups")?,
    })
}

const GRAPHBLAS_CSC_MAGIC: &[u8] = b"graphblas_csc1\n";
const GRAPHBLAS_CSC_MANIFEST_MAGIC: &str = "graphblas_csc_manifest2";
const GRAPHBLAS_CSC_CHUNK_MAGIC: &[u8] = b"graphblas_csc_chunk1\n";
const GRAPHBLAS_CSC_CHUNK_U64S: usize = 64 * 1024;

#[allow(clippy::too_many_arguments)]
async fn append_graphblas_csc_chunks(
    shard: &GraphShard,
    artifact_lock: &CellWriteLock,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
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
    artifact_lock: &CellWriteLock,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
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
                    Some(artifact_lock),
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
            Some(artifact_lock),
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
    artifact_lock: &CellWriteLock,
    batch: &mut GraphWriteBatch,
    pending_writes: &mut usize,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    field: &'static str,
    values: &[u64],
) -> Result<u64> {
    let mut chunks = 0_u64;
    for (chunk_id, chunk) in values.chunks(GRAPHBLAS_CSC_CHUNK_U64S).enumerate() {
        put_artifact_record(
            shard,
            Some(artifact_lock),
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
            "expected graphblas_csc_manifest2 record with 12 fields",
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
    expected_base_epoch: GraphEpoch,
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

fn supernode_group_key(group: &SupernodeGroup) -> String {
    format!(
        "cell/{}/artifact/supernode/{}/{}/{:020}/{:020}",
        group.cell_id,
        group.edge_type,
        direction_str(group.direction),
        group.vertex_id,
        group.base_epoch
    )
}

fn supernode_group_prefix(
    cell_id: &str,
    edge_type: &str,
    direction: ArtifactDirection,
    vertex_id: VertexId,
) -> String {
    format!(
        "cell/{cell_id}/artifact/supernode/{edge_type}/{}/{vertex_id:020}/",
        direction_str(direction)
    )
}

fn supernode_artifact_manifest_key(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
) -> String {
    format!("cell/{cell_id}/artifact/supernode_epoch_manifest/{edge_type}/{base_epoch:020}")
}

fn supernode_artifact_manifest_prefix(cell_id: &str, edge_type: &str) -> String {
    format!("cell/{cell_id}/artifact/supernode_epoch_manifest/{edge_type}/")
}

fn supernode_artifact_manifest_from_groups(
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    groups: &[SupernodeGroup],
) -> Result<Option<SupernodeArtifactManifest>> {
    if groups.is_empty() {
        return Ok(None);
    }
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    checksum_u64(&mut checksum, base_epoch);
    let mut chunk_count = 0_u64;
    let mut degree = 0_u64;
    let mut ordered_groups: Vec<_> = groups.iter().collect();
    ordered_groups.sort_by_key(|group| (group.direction, group.vertex_id));
    for group in ordered_groups {
        if group.cell_id != cell_id
            || group.edge_type != edge_type
            || group.base_epoch != base_epoch
        {
            return Err(GraphError::CorruptValue {
                key: supernode_group_key(group),
                reason: "supernode group does not belong to artifact epoch".to_string(),
            });
        }
        checksum_u64(
            &mut checksum,
            match group.direction {
                ArtifactDirection::Out => 1,
                ArtifactDirection::In => 2,
            },
        );
        checksum_u64(&mut checksum, group.vertex_id);
        checksum_u64(&mut checksum, group.degree);
        checksum_u64(&mut checksum, group.chunk_count);
        checksum_u64(&mut checksum, group.page_size);
        for bound in &group.chunk_bounds {
            checksum_u64(&mut checksum, bound.chunk_id);
            checksum_u64(&mut checksum, bound.first);
            checksum_u64(&mut checksum, bound.last);
        }
        chunk_count = chunk_count.saturating_add(group.chunk_count);
        degree = degree.saturating_add(group.degree);
    }
    Ok(Some(SupernodeArtifactManifest {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        base_epoch,
        group_count: groups.len() as u64,
        chunk_count,
        degree,
        checksum,
    }))
}

async fn ensure_supernode_artifact_publish_compatible(
    shard: &GraphShard,
    cell_id: &str,
    edge_type: &str,
    base_epoch: GraphEpoch,
    manifest: Option<&SupernodeArtifactManifest>,
) -> Result<()> {
    let key = supernode_artifact_manifest_key(cell_id, edge_type, base_epoch);
    let Some(value) = shard.read_remote(&key).await? else {
        return Ok(());
    };
    let existing = decode_supernode_artifact_manifest(&key, &value)?;
    match manifest {
        Some(manifest) if existing == *manifest => Ok(()),
        Some(_) => Err(GraphError::CorruptValue {
            key,
            reason: "existing supernode artifact manifest is incompatible with rebuild".to_string(),
        }),
        None => Err(GraphError::CorruptValue {
            key,
            reason: "existing supernode artifact manifest exists for empty rebuild".to_string(),
        }),
    }
}

fn encode_supernode_group(group: &SupernodeGroup) -> Vec<u8> {
    format!(
        "supernode3\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        group.cell_id,
        group.edge_type,
        direction_str(group.direction),
        group.vertex_id,
        group.base_epoch,
        group.degree,
        group.chunk_count,
        group.page_size,
        encode_supernode_chunk_bounds(&group.chunk_bounds)
    )
    .into_bytes()
}

fn decode_supernode_group(key: &str, value: &[u8]) -> Result<SupernodeGroup> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts[0] == "supernode1" {
        if parts.len() != 8 {
            return corrupt(key, "expected supernode1 record with 8 fields");
        }
        return validate_supernode_group(
            key,
            SupernodeGroup {
                cell_id: parts[1].to_string(),
                edge_type: parts[2].to_string(),
                direction: parse_direction(parts[3])?,
                vertex_id: parse_u64(key, parts[4], "vertex_id")?,
                base_epoch: parse_u64(key, parts[5], "base_epoch")?,
                degree: parse_u64(key, parts[6], "degree")?,
                chunk_count: parse_u64(key, parts[7], "chunk_count")?,
                page_size: 0,
                chunk_bounds: Vec::new(),
            },
        );
    }
    if parts[0] == "supernode2" {
        if parts.len() != 9 {
            return corrupt(key, "expected supernode2 record with 9 fields");
        }
        return validate_supernode_group(
            key,
            SupernodeGroup {
                cell_id: parts[1].to_string(),
                edge_type: parts[2].to_string(),
                direction: parse_direction(parts[3])?,
                vertex_id: parse_u64(key, parts[4], "vertex_id")?,
                base_epoch: parse_u64(key, parts[5], "base_epoch")?,
                degree: parse_u64(key, parts[6], "degree")?,
                chunk_count: parse_u64(key, parts[7], "chunk_count")?,
                page_size: parse_u64(key, parts[8], "page_size")?,
                chunk_bounds: Vec::new(),
            },
        );
    }
    if parts.len() != 10 || parts[0] != "supernode3" {
        return corrupt(key, "expected supernode3 record with 10 fields");
    }
    validate_supernode_group(
        key,
        SupernodeGroup {
            cell_id: parts[1].to_string(),
            edge_type: parts[2].to_string(),
            direction: parse_direction(parts[3])?,
            vertex_id: parse_u64(key, parts[4], "vertex_id")?,
            base_epoch: parse_u64(key, parts[5], "base_epoch")?,
            degree: parse_u64(key, parts[6], "degree")?,
            chunk_count: parse_u64(key, parts[7], "chunk_count")?,
            page_size: parse_u64(key, parts[8], "page_size")?,
            chunk_bounds: decode_supernode_chunk_bounds(key, parts[9])?,
        },
    )
}

fn encode_supernode_artifact_manifest(manifest: &SupernodeArtifactManifest) -> Vec<u8> {
    format!(
        "supernode_epoch_manifest1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        manifest.cell_id,
        manifest.edge_type,
        manifest.base_epoch,
        manifest.group_count,
        manifest.chunk_count,
        manifest.degree,
        manifest.checksum
    )
    .into_bytes()
}

fn decode_supernode_artifact_manifest(
    key: &str,
    value: &[u8],
) -> Result<SupernodeArtifactManifest> {
    let text = text_value(key, value)?;
    let parts: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
    if parts.len() != 8 || parts[0] != "supernode_epoch_manifest1" {
        return corrupt(
            key,
            "expected supernode_epoch_manifest1 record with 8 fields",
        );
    }
    validate_supernode_artifact_manifest(
        key,
        SupernodeArtifactManifest {
            cell_id: parts[1].to_string(),
            edge_type: parts[2].to_string(),
            base_epoch: parse_u64(key, parts[3], "base_epoch")?,
            group_count: parse_u64(key, parts[4], "group_count")?,
            chunk_count: parse_u64(key, parts[5], "chunk_count")?,
            degree: parse_u64(key, parts[6], "degree")?,
            checksum: parse_u64(key, parts[7], "checksum")?,
        },
    )
}

fn validate_supernode_artifact_manifest(
    key: &str,
    manifest: SupernodeArtifactManifest,
) -> Result<SupernodeArtifactManifest> {
    if manifest.degree > 0 && manifest.chunk_count == 0 {
        return corrupt(key, "supernode epoch manifest degree requires chunks");
    }
    if manifest.chunk_count > 0 && manifest.group_count == 0 {
        return corrupt(key, "supernode epoch manifest chunks require groups");
    }
    let parts: Vec<_> = key.split('/').collect();
    let ["cell", cell_id, "artifact", "supernode_epoch_manifest", edge_type, base_epoch] =
        parts.as_slice()
    else {
        return corrupt(key, "invalid supernode epoch manifest key");
    };
    if manifest.cell_id != *cell_id
        || manifest.edge_type != *edge_type
        || manifest.base_epoch != parse_u64(key, base_epoch, "base_epoch")?
    {
        return corrupt(key, "supernode epoch manifest key does not match value");
    }
    Ok(manifest)
}

fn validate_supernode_group(key: &str, group: SupernodeGroup) -> Result<SupernodeGroup> {
    if group.degree > 0 && group.chunk_count == 0 {
        return corrupt(key, "supernode degree requires at least one chunk");
    }
    if group.page_size > 0 {
        let capacity = group
            .chunk_count
            .checked_mul(group.page_size)
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: "supernode chunk capacity overflow".to_string(),
            })?;
        if group.degree > capacity {
            return corrupt(key, "supernode degree exceeds chunk capacity");
        }
    }
    if group.chunk_bounds.len() as u64 > group.chunk_count {
        return corrupt(key, "supernode chunk bounds exceed chunk count");
    }
    let mut previous = None;
    for bound in &group.chunk_bounds {
        if bound.chunk_id >= group.chunk_count {
            return corrupt(key, "supernode chunk bound id exceeds chunk count");
        }
        if bound.first > bound.last {
            return corrupt(key, "supernode chunk bound range is inverted");
        }
        if let Some((prev_first, prev_last, prev_id)) = previous {
            if (bound.first, bound.last, bound.chunk_id) <= (prev_first, prev_last, prev_id) {
                return corrupt(key, "supernode chunk bounds must be sorted and unique");
            }
        }
        previous = Some((bound.first, bound.last, bound.chunk_id));
    }
    Ok(group)
}

fn encode_supernode_chunk_bounds(bounds: &[SupernodeChunkBound]) -> String {
    bounds
        .iter()
        .map(|bound| format!("{}:{}:{}", bound.chunk_id, bound.first, bound.last))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_supernode_chunk_bounds(key: &str, value: &str) -> Result<Vec<SupernodeChunkBound>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut bounds = Vec::new();
    for item in value.split(',') {
        let parts: Vec<_> = item.split(':').collect();
        if parts.len() != 3 {
            return corrupt(key, "invalid supernode chunk bound encoding");
        }
        bounds.push(SupernodeChunkBound {
            chunk_id: parse_u64(key, parts[0], "chunk_id")?,
            first: parse_u64(key, parts[1], "chunk_first")?,
            last: parse_u64(key, parts[2], "chunk_last")?,
        });
    }
    bounds.sort_by_key(|bound| (bound.first, bound.last, bound.chunk_id));
    Ok(bounds)
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

fn encode_u64_list(values: &[u64]) -> String {
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_u64_list(key: &str, value: &str) -> Result<Vec<u64>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| parse_u64(key, part, "u64_list_value"))
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

#[cfg(test)]
mod artifact_publish_tests {
    use super::*;
    use crate::graph_now_millis;
    use slatedb::object_store::{memory::InMemory, ObjectStoreExt};

    #[tokio::test]
    async fn artifact_batch_flush_renews_held_lock() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = GraphShard::open_standalone_writer(
            "graph/artifact-lock-renew",
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        let cell_id = "reddit-home";
        let edge_type = "LOCK_RENEW_EDGE";
        let lock = shard
            .acquire_posting_artifact_write_lock(
                cell_id,
                edge_type,
                1,
                "artifact_batch_flush_renews_held_lock",
            )
            .await
            .unwrap();
        let stale_payload = crate::encode_cell_write_lock_record(
            cell_id,
            "artifact_batch_flush_renews_held_lock",
            &lock.owner_token,
            0,
            1,
            crate::CellWriteLockState::Active,
        );
        lock.object_store
            .put(&lock.path, stale_payload.into())
            .await
            .unwrap();

        let mut batch = GraphWriteBatch::new();
        let mut pending_writes = 0_usize;
        for idx in 0..GRAPH_ARTIFACT_WRITE_BATCH_KEYS {
            put_artifact_record(
                &shard,
                Some(&lock),
                cell_id,
                "artifact_batch_flush_renews_held_lock",
                &mut batch,
                &mut pending_writes,
                format!("cell/{cell_id}/artifact/test/{edge_type}/{idx:020}"),
                b"artifact-test".to_vec(),
            )
            .await
            .unwrap();
        }
        assert_eq!(pending_writes, 0);

        let current = lock.object_store.get(&lock.path).await.unwrap();
        let value = current.bytes().await.unwrap();
        let record = crate::decode_cell_write_lock_record(lock.path.as_ref(), &value).unwrap();
        assert_eq!(record.owner_token, lock.owner_token);
        assert!(record.expires_at_ms > graph_now_millis());
        lock.release().await.unwrap();
    }
}
