use thiserror::Error;

use crate::GraphEpoch;
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
    #[error(
        "control watermark regression for {field} on cell {cell_id}: requested {requested_epoch}, current {current_epoch}"
    )]
    ControlWatermarkRegression {
        cell_id: String,
        field: &'static str,
        requested_epoch: GraphEpoch,
        current_epoch: GraphEpoch,
    },
    #[error(
        "snapshot epoch {read_epoch} is ahead of current epoch {current_epoch} for cell {cell_id}"
    )]
    SnapshotAhead {
        cell_id: String,
        read_epoch: GraphEpoch,
        current_epoch: GraphEpoch,
    },
    #[error("no shard placement exists for cell {cell_id}")]
    UnknownShard { cell_id: String },
    #[error("cell {cell_id} is owned by node {owner_node_id}, not local node {local_node_id}")]
    ShardNotOwned {
        cell_id: String,
        owner_node_id: String,
        local_node_id: String,
    },
    #[error("cell {cell_id} is currently leased by node {owner_node_id} until {expires_at_ms}")]
    ShardLeaseHeld {
        cell_id: String,
        owner_node_id: String,
        expires_at_ms: u64,
    },
    #[error("node {node_id} does not hold current lease token {lease_token} for cell {cell_id}")]
    StaleShardLease {
        cell_id: String,
        node_id: String,
        lease_token: u64,
    },
    #[error("{operation} requires an active graph control-plane lease for cell {cell_id}")]
    WriteRequiresLease {
        operation: &'static str,
        cell_id: String,
    },
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
        read_epoch: GraphEpoch,
        min_epoch: GraphEpoch,
    },
    #[error(
        "{operation} snapshot epoch {read_epoch} for cell {cell_id} edge {edge_type} changed while building; current epoch is {current_epoch}"
    )]
    SnapshotChanged {
        operation: &'static str,
        cell_id: String,
        edge_type: String,
        read_epoch: GraphEpoch,
        current_epoch: GraphEpoch,
    },
    #[error(
        "{operation} stats snapshot epoch {read_epoch} for cell {cell_id} changed while refreshing; current epoch is {current_epoch}"
    )]
    QueryStatsSnapshotChanged {
        operation: &'static str,
        cell_id: String,
        read_epoch: GraphEpoch,
        current_epoch: GraphEpoch,
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
    #[error("{operation} would violate retention for cell {cell_id}: requested epoch {requested_epoch}, safe epoch {safe_epoch}")]
    RetentionViolation {
        operation: &'static str,
        cell_id: String,
        requested_epoch: GraphEpoch,
        safe_epoch: GraphEpoch,
    },
}

pub type Result<T> = std::result::Result<T, GraphError>;
