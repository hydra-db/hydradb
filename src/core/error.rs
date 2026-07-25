use thiserror::Error;

use crate::StorageSequence;
#[derive(Debug, Error)]
pub enum GraphError {
    #[error("slatedb error: {0}")]
    Slate(#[from] slatedb::Error),
    #[error("object store error: {0}")]
    ObjectStore(#[from] slatedb::object_store::Error),
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
        requested_epoch: StorageSequence,
        current_epoch: StorageSequence,
    },
    #[error(
        "snapshot epoch {read_epoch} is ahead of current epoch {current_epoch} for cell {cell_id}"
    )]
    SnapshotAhead {
        cell_id: String,
        read_epoch: StorageSequence,
        current_epoch: StorageSequence,
    },
    #[error("cell {cell_id} is not present in the node directory")]
    UnknownShard { cell_id: String },
    #[error("graph scope mismatch: expected {expected}, received {actual}")]
    GraphScopeMismatch { expected: String, actual: String },
    #[error("principal {principal} is not authorized to {action} graph scope {scope}")]
    GraphScopeAccessDenied {
        principal: String,
        action: &'static str,
        scope: String,
    },
    #[error("{operation} requires the fenced SlateDB writer for cell {cell_id}")]
    WriteRequiresWriter {
        operation: &'static str,
        cell_id: String,
    },
    /// This node is not the rendezvous owner of the cell, so it declined to open
    /// the writer — touch point (b) of
    /// `docs/plans/2026-07-25-rendezvous-placement.md`.
    ///
    /// `owner` is an `Option` and not a `String` because the two refusals are
    /// different answers. `Some(node_id)` is *a live peer owns this cell*, which
    /// a driver can act on: discard the routing table, re-route, retry. `None`
    /// is *this node has shed its view of the fleet* (decision 7) and has
    /// nothing truthful to point at — a stale guess would send the write to a
    /// node that may itself have shed. Modelled on TiKV's
    /// `NotLeader { leader_hint }`, whose hint is optional for the same reason.
    #[error("cell {cell_id} is not owned by this node; owner: {}", owner.as_deref().unwrap_or("unknown"))]
    NotCellWriter {
        cell_id: String,
        owner: Option<String>,
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
        read_epoch: StorageSequence,
        min_epoch: StorageSequence,
    },
    #[error(
        "{operation} snapshot epoch {read_epoch} for cell {cell_id} edge {edge_type} changed while building; current epoch is {current_epoch}"
    )]
    SnapshotChanged {
        operation: &'static str,
        cell_id: String,
        edge_type: String,
        read_epoch: StorageSequence,
        current_epoch: StorageSequence,
    },
    #[error(
        "{operation} stats snapshot epoch {read_epoch} for cell {cell_id} changed while refreshing; current epoch is {current_epoch}"
    )]
    QueryStatsSnapshotChanged {
        operation: &'static str,
        cell_id: String,
        read_epoch: StorageSequence,
        current_epoch: StorageSequence,
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
