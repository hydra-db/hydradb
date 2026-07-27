use thiserror::Error;

use crate::StorageSequence;

#[cfg(test)]
mod tests;

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
    /// This node cannot answer a routing request right now — it has no live
    /// fleet to advertise, or no owner for the cell being asked about.
    ///
    /// Distinct from a routing *misconfiguration* because a driver must treat
    /// the two differently, and only one of them is the driver's problem. This
    /// is a **transient** condition of one node: past decision 7's grace window
    /// a node that cannot LIST sheds its view of the fleet, and shedding is a
    /// routine state, not a broken deployment. A driver that receives it should
    /// ask the next router in its list, which every peer that can still LIST
    /// will answer.
    ///
    /// It carries no node hint, deliberately. A node in this state either has no
    /// view of the fleet or knows the fleet has no owner for the cell; there is
    /// nothing truthful to point at, and the driver already holds the router
    /// list it needs. That is the same rule as `NotCellWriter { owner: None }`.
    #[error("routing is unavailable on this node: {reason}")]
    RoutingUnavailable { reason: String },
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

impl GraphError {
    /// Every value [`Self::class`] can return, in [`Self::class_index`] order.
    ///
    /// The array *is* the vocabulary. [`Self::class_index`] returns a position
    /// in it and [`Self::class`] is a lookup, so a class name and the slot a
    /// per-class metric counts it in cannot drift apart: there is one match, not
    /// two.
    ///
    /// The strings are the wire vocabulary and must stay identical to
    /// `turbolay_telemetry::ErrorClass::as_str` minus its `other`, which nothing
    /// in this tree constructs.
    pub const CLASSES: [&'static str; 10] = [
        "contention",
        "fencing",
        "freshness",
        "admission",
        "query",
        "authz",
        "corruption",
        "config",
        "storage",
        "kernel",
    ];

    /// How many classes there are, for dimensioning an array by class.
    ///
    /// Derived from [`Self::CLASSES`] rather than written out, so a class added
    /// to the vocabulary resizes every such array instead of silently leaving
    /// the new one uncounted.
    pub const CLASS_COUNT: usize = Self::CLASSES.len();

    // Positions in `CLASSES`, so the match arms in `class_index` read as the
    // names they are. `class_constants_index_their_own_name` pins each one
    // against the array, which is the only place the two can disagree.
    pub(crate) const CLASS_CONTENTION: usize = 0;
    pub(crate) const CLASS_FENCING: usize = 1;
    pub(crate) const CLASS_FRESHNESS: usize = 2;
    pub(crate) const CLASS_ADMISSION: usize = 3;
    pub(crate) const CLASS_QUERY: usize = 4;
    pub(crate) const CLASS_AUTHZ: usize = 5;
    pub(crate) const CLASS_CORRUPTION: usize = 6;
    pub(crate) const CLASS_CONFIG: usize = 7;
    pub(crate) const CLASS_STORAGE: usize = 8;
    pub(crate) const CLASS_KERNEL: usize = 9;

    /// The coarse failure class recorded as the `error.class` span attribute.
    ///
    /// Errors otherwise reach a log as `%error` display strings, which carry a
    /// key and an epoch and are therefore unique per occurrence — a backend
    /// cannot group them, so it cannot chart a rate or alert on a change in
    /// one. This is the grouping key.
    ///
    /// # Why the mapping is here rather than in `turbolay-telemetry`
    ///
    /// This resolves the open decision in
    /// `docs/plans/2026-07-26-otel-telemetry-crate.md` §8 in favour of an
    /// inherent method. It keeps the match arms next to the variants they
    /// classify, so adding a variant without classifying it is a compile error
    /// rather than a silent `other`; and returning `&'static str` means the
    /// kernel gains no telemetry dependency, preserving the direction of the
    /// arrow that lets `cargo test` stay free of `opentelemetry-*`.
    ///
    /// **`contention` and `fencing` are expected, not alarming.** Retries and
    /// writer handover are how the system is supposed to behave under
    /// concurrency. The class exists so a dashboard can chart the rate and
    /// alert on a *change* in it, not so each occurrence pages someone.
    pub fn class(&self) -> &'static str {
        Self::CLASSES[self.class_index()]
    }

    /// The same classification as [`Self::class`], as a position in
    /// [`Self::CLASSES`].
    ///
    /// This is where the taxonomy actually lives, and the exhaustive match with
    /// no wildcard and no `other` arm is the point of it: a `GraphError` variant
    /// added without a class is a compile error here, not a silent bucket. It
    /// returns an index rather than a string because the per-class error
    /// counters are `[AtomicU64; Self::CLASS_COUNT]` — an array indexed in O(1)
    /// with no hashing and no allocation on a failure path, and one whose length
    /// is the vocabulary's own.
    pub fn class_index(&self) -> usize {
        match self {
            Self::ConditionalWriteConflict { .. }
            | Self::IdempotencyConflict { .. }
            | Self::ControlMetadataConflict { .. }
            | Self::RetryExhausted { .. } => Self::CLASS_CONTENTION,

            // `RoutingUnavailable` joins this group rather than getting one of
            // its own: it means the caller reached a node that does not own the
            // cell and the owner is not yet known, which is the same transient,
            // retry-and-it-resolves shape as losing a fence. Commit 4ab2c2b
            // made it transient rather than a syntax error for exactly that
            // reason, and classifying it apart would split one operational
            // question across two dashboard series.
            Self::WriteRequiresWriter { .. }
            | Self::NotCellWriter { .. }
            | Self::UnknownShard { .. }
            | Self::CellDropped { .. }
            | Self::RoutingUnavailable { .. } => Self::CLASS_FENCING,

            Self::SnapshotAhead { .. }
            | Self::SnapshotExpired { .. }
            | Self::SnapshotChanged { .. }
            | Self::QueryStatsSnapshotChanged { .. }
            | Self::ControlWatermarkRegression { .. } => Self::CLASS_FRESHNESS,

            Self::AdmissionRejected { .. } | Self::QueryTimeout { .. } => Self::CLASS_ADMISSION,

            Self::QueryParse { .. }
            | Self::UnsupportedQuery { .. }
            | Self::MissingQueryParameter { .. } => Self::CLASS_QUERY,

            Self::GraphScopeMismatch { .. } | Self::GraphScopeAccessDenied { .. } => {
                Self::CLASS_AUTHZ
            }

            Self::CorruptValue { .. } | Self::InvalidKeyComponent { .. } => Self::CLASS_CORRUPTION,

            // Both are a refusal to act on how this process was configured, not
            // a failure of the operation itself: one rejects an unsafe
            // durability setting, the other a write to a shard opened read-only.
            Self::UnsafeDurabilityConfig { .. } | Self::ReadOnlyShardStorage => Self::CLASS_CONFIG,

            Self::Slate(_) | Self::ObjectStore(_) => Self::CLASS_STORAGE,

            Self::SparseKernel { .. } => Self::CLASS_KERNEL,
        }
    }
}

#[cfg(test)]
impl GraphError {
    /// One error per class, in [`Self::CLASSES`] order.
    ///
    /// Lives here rather than in a test module because two of them need it —
    /// `error/tests.rs` for the taxonomy, `core/metrics/tests.rs` for the
    /// counters — and a second copy of the table is a second thing that can be
    /// wrong. `one_error_per_class_is_in_classes_order` is what makes the order
    /// a fact rather than a comment.
    pub(crate) fn one_per_class() -> [Self; Self::CLASS_COUNT] {
        [
            Self::ConditionalWriteConflict {
                operation: "test",
                key: "cell/edge".into(),
            },
            Self::NotCellWriter {
                cell_id: "cell".into(),
                owner: None,
            },
            Self::SnapshotAhead {
                cell_id: "cell".into(),
                read_epoch: 9,
                current_epoch: 4,
            },
            Self::AdmissionRejected {
                operation: "test",
                actual: 2,
                limit: 1,
            },
            Self::QueryParse {
                dialect: "cypher",
                reason: "unexpected token".into(),
            },
            Self::GraphScopeMismatch {
                expected: "a".into(),
                actual: "b".into(),
            },
            Self::CorruptValue {
                key: "cell/edge".into(),
                reason: "short header".into(),
            },
            Self::ReadOnlyShardStorage,
            Self::ObjectStore(slatedb::object_store::Error::Generic {
                store: "test",
                source: "injected".into(),
            }),
            Self::SparseKernel {
                backend: "rust",
                reason: "dimension mismatch".into(),
            },
        ]
    }
}

pub type Result<T> = std::result::Result<T, GraphError>;
