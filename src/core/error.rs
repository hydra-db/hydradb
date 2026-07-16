use thiserror::Error;

use crate::TopologySequence;
#[derive(Debug, Error)]
pub enum GraphError {
    #[error("slatedb error: {0}")]
    Slate(#[from] slatedb::Error),
    #[error("object store error: {0}")]
    ObjectStore(#[from] slatedb::object_store::Error),
    #[error("cell write conflict for {operation} on {cell_id}")]
    CellWriteConflict {
        operation: &'static str,
        cell_id: String,
    },
    #[error("conditional graph write conflict for {operation} at {key}")]
    ConditionalWriteConflict {
        operation: &'static str,
        key: String,
    },
    #[error("{operation} requires await_durable_writes=true: {reason}")]
    UnsafeDurabilityConfig {
        operation: &'static str,
        reason: String,
    },
    #[error("invalid {component} key component: {value}")]
    InvalidKeyComponent {
        component: &'static str,
        value: String,
    },
    #[error("corrupt value at {key}: {reason}")]
    CorruptValue { key: String, reason: String },
    #[error("idempotency key conflict for {operation} request key {idempotency_key}")]
    IdempotencyConflict {
        operation: &'static str,
        idempotency_key: String,
    },
    #[error(
        "control metadata conflict at {key}: expected generation {expected_generation:?}, actual generation {actual_generation:?}"
    )]
    ControlMetadataConflict {
        key: String,
        expected_generation: Option<u64>,
        actual_generation: Option<u64>,
    },
    #[error("{operation} exhausted retry budget after {attempts} attempts")]
    RetryExhausted {
        operation: &'static str,
        attempts: usize,
    },
    #[error(
        "control watermark regression for {field} on cell {cell_id}: requested {requested_epoch}, current {current_epoch}"
    )]
    ControlWatermarkRegression {
        cell_id: String,
        field: &'static str,
        requested_epoch: TopologySequence,
        current_epoch: TopologySequence,
    },
    #[error(
        "snapshot epoch {read_epoch} is ahead of current epoch {current_epoch} for cell {cell_id}"
    )]
    SnapshotAhead {
        cell_id: String,
        read_epoch: TopologySequence,
        current_epoch: TopologySequence,
    },
    #[error("no shard placement exists for cell {cell_id}")]
    UnknownShard { cell_id: String },
    #[error("graph scope mismatch: expected {expected}, received {actual}")]
    GraphScopeMismatch { expected: String, actual: String },
    #[error("principal {principal} is not authorized to {action} graph scope {scope}")]
    GraphScopeAccessDenied {
        principal: String,
        action: &'static str,
        scope: String,
    },
    #[error("cell {cell_id} is owned by node {owner_node_id}, not local node {local_node_id}")]
    ShardNotOwned {
        cell_id: String,
        owner_node_id: String,
        local_node_id: String,
    },
    #[error("{operation} requires the fenced SlateDB writer for cell {cell_id}")]
    WriteRequiresWriter {
        operation: &'static str,
        cell_id: String,
    },
    #[error("operation requires writable SlateDB shard storage")]
    ReadOnlyShardStorage,
    #[error("cell {cell_id} has been dropped; {operation} is rejected")]
    CellDropped {
        operation: &'static str,
        cell_id: String,
    },
    #[error(
        "snapshot epoch {read_epoch} for cell {cell_id} edge {edge_type} is below compacted watermark {min_epoch}"
    )]
    SnapshotExpired {
        cell_id: String,
        edge_type: String,
        read_epoch: TopologySequence,
        min_epoch: TopologySequence,
    },
    #[error(
        "{operation} snapshot epoch {read_epoch} for cell {cell_id} edge {edge_type} changed while building; current epoch is {current_epoch}"
    )]
    SnapshotChanged {
        operation: &'static str,
        cell_id: String,
        edge_type: String,
        read_epoch: TopologySequence,
        current_epoch: TopologySequence,
    },
    #[error(
        "{operation} stats snapshot epoch {read_epoch} for cell {cell_id} changed while refreshing; current epoch is {current_epoch}"
    )]
    QueryStatsSnapshotChanged {
        operation: &'static str,
        cell_id: String,
        read_epoch: TopologySequence,
        current_epoch: TopologySequence,
    },
    #[error("{operation} rejected by admission control: actual {actual} exceeds limit {limit}")]
    AdmissionRejected {
        operation: &'static str,
        actual: u64,
        limit: u64,
    },
    #[error("sparse kernel {backend} failed: {reason}")]
    SparseKernel {
        backend: &'static str,
        reason: String,
    },
    #[error("{dialect} parse error: {reason}")]
    QueryParse {
        dialect: &'static str,
        reason: String,
    },
    #[error("missing {dialect} query parameter ${name}")]
    MissingQueryParameter { dialect: &'static str, name: String },
    #[error("{operation} exceeded query timeout after {elapsed_ms} ms; limit is {limit_ms} ms")]
    QueryTimeout {
        operation: &'static str,
        elapsed_ms: u64,
        limit_ms: u64,
    },
    #[error("{dialect} query is not supported yet: {feature}")]
    UnsupportedQuery {
        dialect: &'static str,
        feature: String,
    },
}

pub type Result<T> = std::result::Result<T, GraphError>;
