use std::collections::BTreeMap;
use std::time::Duration;

pub use hydradb_cypher_ast::QueryFailureReason;
use thiserror::Error;

use crate::StorageSequence;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("slatedb error: {0}")]
    Slate(#[from] slatedb::Error),
    #[cfg(feature = "datafusion-cypher-engine")]
    #[error("DataFusion graph provider error: {0}")]
    DataFusionProvider(#[from] hydradb_graph_provider::GraphProviderError),
    #[error("object store error: {0}")]
    ObjectStore(#[from] slatedb::object_store::Error),
    #[error("conditional graph write conflict for {operation} at {key}")]
    ConditionalWriteConflict { operation: &'static str, key: String },
    /// The entire statement was a single native delete batch rejected during preparation.
    #[error("vertex delete preparation conflicted with a concurrent write")]
    AtomicVertexDeleteConflict,
    #[error("durability of committed storage sequence {sequence} is unknown: {message}")]
    DurabilityUnknown { sequence: StorageSequence, message: String },
    #[error("{operation} requires await_durable_writes=true: {reason}")]
    UnsafeDurabilityConfig { operation: &'static str, reason: String },
    #[error(
        "routed writer configuration mismatch for {path} on node {node_id}: existing fence backoff {existing:?}, requested {requested:?}"
    )]
    RoutedWriterConfigMismatch {
        path: String,
        node_id: String,
        existing: Duration,
        requested: Duration,
    },
    #[error("invalid {component} key component: {value}")]
    InvalidKeyComponent { component: &'static str, value: String },
    #[error("corrupt value at {key}: {reason}")]
    CorruptValue { key: String, reason: String },
    /// A request reused an idempotency key that the store already resolved to a
    /// different outcome.
    ///
    /// `reason` names *which* check failed, and it is the whole point of the
    /// variant carrying three fields instead of two. The operation and the key
    /// together say only "these two requests disagreed"; they cannot say
    /// whether a replayed key found a stored record for a different payload
    /// (the caller is minting keys that collide) or whether an identity the
    /// payload carries is already bound to something else (the caller is
    /// minting *ids* that collide). Those are different bugs on the caller's
    /// side with different fixes. Bolt exposes the complete message as a
    /// non-retryable `Neo.ClientError.Transaction.Invalid`, so callers can stop
    /// retrying and diagnose the failed check directly.
    #[error("idempotency key conflict for {operation} request key {idempotency_key}: {reason}")]
    IdempotencyConflict {
        operation: &'static str,
        idempotency_key: String,
        reason: &'static str,
    },
    #[error("control metadata conflict at {key}: expected generation {expected_generation:?}, actual generation {actual_generation:?}")]
    ControlMetadataConflict {
        key: String,
        expected_generation: Option<u64>,
        actual_generation: Option<u64>,
    },
    #[error("{operation} exhausted retry budget after {attempts} attempts")]
    RetryExhausted { operation: &'static str, attempts: usize },
    #[error("control watermark regression for {field} on cell {cell_id}: requested {requested_epoch}, current {current_epoch}")]
    ControlWatermarkRegression {
        cell_id: String,
        field: &'static str,
        requested_epoch: StorageSequence,
        current_epoch: StorageSequence,
    },
    #[error("snapshot epoch {read_epoch} is ahead of current epoch {current_epoch} for cell {cell_id}")]
    SnapshotAhead {
        cell_id: String,
        read_epoch: StorageSequence,
        current_epoch: StorageSequence,
    },
    /// A sequence number cannot reopen an old SlateDB snapshot. This is a
    /// storage-view failure, not invalid Cypher. Retrying the same sequence
    /// cannot recover it; callers must retain the actual snapshot.
    #[error(
        "graph snapshot unavailable for cell {cell_id}: requested storage sequence {read_epoch}, current sequence {current_epoch}; historical sequences cannot reopen a SlateDB snapshot"
    )]
    SnapshotUnavailable {
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
    WriteRequiresWriter { operation: &'static str, cell_id: String },
    /// This node is not the rendezvous owner of the cell, so it declined to open
    /// the writer — touch point (b) of
    /// the rendezvous placement design.
    ///
    /// `owner` is an `Option` and not a `String` because the two refusals are
    /// different answers. `Some(node_id)` is *a live peer owns this cell*, which
    /// a driver can act on: discard the routing table, re-route, retry. `None`
    /// is *this node has shed its view of the fleet* (decision 7) and has
    /// nothing truthful to point at — a stale guess would send the write to a
    /// node that may itself have shed. Modelled on TiKV's
    /// `NotLeader { leader_hint }`, whose hint is optional for the same reason.
    ///
    /// The text is `hydradb_protocol::owner`'s format, which the turbolay proxy
    /// parses to retry at the owner, so the two cannot drift.
    #[error("{}", hydradb_protocol::owner::format_not_cell_writer(cell_id, owner.as_deref()))]
    NotCellWriter { cell_id: String, owner: Option<String> },
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
    CellDropped { operation: &'static str, cell_id: String },
    #[error("snapshot epoch {read_epoch} for cell {cell_id} edge {edge_type} is below compacted watermark {min_epoch}")]
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
    #[error("{operation} rejected by admission control: actual {actual} exceeds limit {limit}")]
    AdmissionRejected { operation: &'static str, actual: u64, limit: u64 },
    #[error("sparse kernel {backend} failed: {reason}")]
    SparseKernel { backend: &'static str, reason: String },
    #[error("{dialect} parse error: {reason}")]
    QueryParse { dialect: &'static str, reason: String },
    #[error("missing {dialect} query parameter ${name}")]
    MissingQueryParameter { dialect: &'static str, name: String },
    #[error("{operation} exceeded query timeout after {elapsed_ms} ms; limit is {limit_ms} ms")]
    QueryTimeout {
        operation: &'static str,
        elapsed_ms: u64,
        limit_ms: u64,
    },
    #[error("{operation} could not allocate query working memory: {reason}")]
    QueryAllocation { operation: &'static str, reason: String },
    /// Also carries request misuse and evaluation failures that predate a
    /// dedicated variant; `reason` is what tells them apart for metrics, and
    /// never appears in the message.
    #[error("{dialect} query is not supported yet: {feature}")]
    UnsupportedQuery {
        dialect: &'static str,
        feature: String,
        reason: QueryFailureReason,
    },
    /// A failure that has always surfaced as `UnsupportedQuery` but whose real
    /// cause is unknown here, so it carries no [`QueryFailureReason`]: a
    /// failure another node reported over the query transport without one
    /// (a peer too old to send its failure typed), or a storage error the
    /// experimental engine received from its adapter without a typed error.
    /// Timeouts, budgets, admission and internal errors are neither: the
    /// engine and the transport both carry them typed.
    ///
    /// Worded, classed and mapped to clients exactly as `UnsupportedQuery`, so
    /// nothing a client or an existing series sees changes. It exists only so
    /// the query-failure counter can leave it out rather than file a timeout
    /// under `unsupported_other`.
    #[error("{dialect} query is not supported yet: {feature}")]
    UnclassifiedQuery { dialect: &'static str, feature: String },
    /// A query the experimental engine refused or failed, with the
    /// GQL-status object it reports (ISO/IEC 39075 §4.9, §23). Worded,
    /// classed, counted and given the client code [`Self::UnsupportedQuery`]
    /// is, so nothing a client or a metric saw before changes; the status
    /// rides along for Bolt and HTTP to report.
    #[error("{dialect} query is not supported yet: {feature}")]
    GqlQuery {
        dialect: &'static str,
        feature: String,
        reason: QueryFailureReason,
        status: Box<GqlStatusReport>,
    },
    /// A routed query failed on its owner with an error the owner keeps
    /// internal -- storage, corruption, kernel and the like. The details are
    /// in the owner's log; `class` is the owner's error class, one of
    /// [`Self::CLASSES`], so this node's metrics file it where the owner did.
    /// Clients get the internal-error answer the owner would have given.
    #[error("{key}: internal query execution error on the owner (class {class})")]
    RemoteInternal { key: String, class: &'static str },
    /// One write batch sets a property of one vertex or edge to two different
    /// values, so the batch has no single outcome to commit.
    ///
    /// Not an `UnsupportedQuery`: the statement is valid and supported, and it
    /// is the rows the client sent that disagree. It surfaced as one until
    /// an internal tracking item fixed it, which is why Bolt answered a data conflict with
    /// `Neo.ClientError.Statement.InvalidSyntax` and callers went looking for
    /// a bad query. Bolt now reports `Neo.ClientError.Statement.ArgumentError`.
    /// The message keeps its old wording so log searches still match.
    #[error("conflicting metadata values for {target} in one batch")]
    BatchValueConflict { target: String },
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
    /// `hydradb_telemetry::ErrorClass::as_str` minus its `other`, which nothing
    /// in this tree constructs.
    pub const CLASSES: [&'static str; 12] = [
        "contention",
        "fencing",
        "routing",
        "freshness",
        "admission",
        "timeout",
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
    pub const CLASS_CONTENTION: usize = 0;
    pub const CLASS_FENCING: usize = 1;
    pub const CLASS_ROUTING: usize = 2;
    pub const CLASS_FRESHNESS: usize = 3;
    pub const CLASS_ADMISSION: usize = 4;
    pub const CLASS_TIMEOUT: usize = 5;
    pub const CLASS_QUERY: usize = 6;
    pub const CLASS_AUTHZ: usize = 7;
    pub const CLASS_CORRUPTION: usize = 8;
    pub const CLASS_CONFIG: usize = 9;
    pub const CLASS_STORAGE: usize = 10;
    pub const CLASS_KERNEL: usize = 11;

    /// The coarse failure class recorded as the `error.class` span attribute.
    ///
    /// Errors otherwise reach a log as `%error` display strings, which carry a
    /// key and an epoch and are therefore unique per occurrence — a backend
    /// cannot group them, so it cannot chart a rate or alert on a change in
    /// one. This is the grouping key.
    ///
    /// # Why the mapping is here rather than in `hydradb-telemetry`
    ///
    /// This resolves the open decision in
    /// the telemetry crate design §8 in favour of an
    /// inherent method. It keeps the match arms next to the variants they
    /// classify, so adding a variant without classifying it is a compile error
    /// rather than a silent `other`; and returning `&'static str` means the
    /// kernel gains no telemetry dependency, preserving the direction of the
    /// arrow that lets `cargo test` stay free of `opentelemetry-*`.
    ///
    /// **`contention`, `routing`, and occasional `fencing` are expected, not
    /// individually alarming.** The classes exist so a dashboard can separate
    /// wrong-node retries from actual writer loss and alert on changes in rate.
    pub fn class(&self) -> &'static str {
        Self::CLASSES[self.class_index()]
    }

    /// Why a `query`-class failure happened, as a closed metric label.
    ///
    /// `Some` only when [`Self::class`] is `query`, and for every query-class
    /// variant except [`Self::UnclassifiedQuery`], whose cause is unknown --
    /// and which is usually not a query failure at all.
    pub fn failure_reason(&self) -> Option<QueryFailureReason> {
        match self {
            Self::UnsupportedQuery { reason, .. } | Self::GqlQuery { reason, .. } => Some(*reason),
            Self::QueryParse { .. } => Some(QueryFailureReason::ParseError),
            Self::MissingQueryParameter { .. } => Some(QueryFailureReason::Parameter),
            Self::QueryAllocation { .. } => Some(QueryFailureReason::Other),
            // Counted where it always was, so the failure series stays
            // continuous; the Bolt code is what changed.
            Self::BatchValueConflict { .. } => Some(QueryFailureReason::Mutation),
            _ => None,
        }
    }

    /// The bounded work or timeout operation carried by limit failures.
    ///
    /// Every producer supplies a static name, so this is safe to attach to
    /// telemetry. Other error classes have no equivalent field.
    pub fn limit_operation(&self) -> Option<&'static str> {
        match self {
            Self::AdmissionRejected { operation, .. } | Self::QueryTimeout { operation, .. } => Some(operation),
            _ => None,
        }
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
            | Self::AtomicVertexDeleteConflict
            | Self::IdempotencyConflict { .. }
            | Self::ControlMetadataConflict { .. }
            | Self::RetryExhausted { .. } => Self::CLASS_CONTENTION,

            #[cfg(feature = "datafusion-cypher-engine")]
            Self::DataFusionProvider(
                hydradb_graph_provider::GraphProviderError::Conflict { .. }
                | hydradb_graph_provider::GraphProviderError::IdempotencyConflict,
            ) => Self::CLASS_CONTENTION,

            Self::WriteRequiresWriter { .. } | Self::CellDropped { .. } => Self::CLASS_FENCING,

            // These errors mean the request reached a node that cannot serve
            // this cell. They are driver-routing outcomes, not evidence that a
            // SlateDB writer was fenced.
            Self::NotCellWriter { .. } | Self::UnknownShard { .. } | Self::RoutingUnavailable { .. } => Self::CLASS_ROUTING,

            Self::SnapshotAhead { .. }
            | Self::SnapshotUnavailable { .. }
            | Self::SnapshotExpired { .. }
            | Self::SnapshotChanged { .. }
            | Self::ControlWatermarkRegression { .. } => Self::CLASS_FRESHNESS,

            #[cfg(feature = "datafusion-cypher-engine")]
            Self::DataFusionProvider(hydradb_graph_provider::GraphProviderError::SnapshotUnavailable) => Self::CLASS_FRESHNESS,

            Self::AdmissionRejected { .. } => Self::CLASS_ADMISSION,

            #[cfg(feature = "datafusion-cypher-engine")]
            Self::DataFusionProvider(
                hydradb_graph_provider::GraphProviderError::BudgetExceeded { .. }
                | hydradb_graph_provider::GraphProviderError::BatchTooLarge { .. },
            ) => Self::CLASS_ADMISSION,

            Self::QueryTimeout { .. } => Self::CLASS_TIMEOUT,

            #[cfg(feature = "datafusion-cypher-engine")]
            Self::DataFusionProvider(hydradb_graph_provider::GraphProviderError::DeadlineExceeded) => Self::CLASS_TIMEOUT,

            Self::QueryParse { .. }
            | Self::UnsupportedQuery { .. }
            | Self::GqlQuery { .. }
            | Self::UnclassifiedQuery { .. }
            | Self::QueryAllocation { .. }
            | Self::MissingQueryParameter { .. }
            | Self::BatchValueConflict { .. } => Self::CLASS_QUERY,

            #[cfg(feature = "datafusion-cypher-engine")]
            Self::DataFusionProvider(
                hydradb_graph_provider::GraphProviderError::InvalidRequest(_)
                | hydradb_graph_provider::GraphProviderError::GraphValue(_)
                | hydradb_graph_provider::GraphProviderError::MissingEntity { .. }
                | hydradb_graph_provider::GraphProviderError::UnsupportedCapability { .. }
                | hydradb_graph_provider::GraphProviderError::InvalidContinuationToken
                | hydradb_graph_provider::GraphProviderError::UnsupportedReceiptVersion { .. },
            ) => Self::CLASS_QUERY,

            Self::GraphScopeMismatch { .. } | Self::GraphScopeAccessDenied { .. } => Self::CLASS_AUTHZ,

            Self::CorruptValue { .. } | Self::InvalidKeyComponent { .. } => Self::CLASS_CORRUPTION,

            // Both are a refusal to act on how this process was configured, not
            // a failure of the operation itself: one rejects an unsafe
            // durability setting, the other a write to a shard opened read-only.
            Self::UnsafeDurabilityConfig { .. } | Self::RoutedWriterConfigMismatch { .. } | Self::ReadOnlyShardStorage => {
                Self::CLASS_CONFIG
            }

            Self::Slate(error) if matches!(error.kind(), slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)) => Self::CLASS_FENCING,

            Self::DurabilityUnknown { .. } | Self::Slate(_) | Self::ObjectStore(_) => Self::CLASS_STORAGE,

            #[cfg(feature = "datafusion-cypher-engine")]
            Self::DataFusionProvider(_) => Self::CLASS_STORAGE,

            Self::SparseKernel { .. } => Self::CLASS_KERNEL,

            Self::RemoteInternal { class, .. } => Self::CLASSES.iter().position(|known| known == class).unwrap_or(Self::CLASS_STORAGE),
        }
    }
}

pub type Result<T> = std::result::Result<T, GraphError>;

/// A GQL-status object (ISO/IEC 39075 §4.9.2) as the transports carry it:
/// the five-character GQLSTATUS, its description, and the diagnostic record
/// (§23.2) as named fields, `OPERATION`, `OPERATION_CODE`, `CURRENT_SCHEMA`,
/// `INVALID_REFERENCE` and implementation-defined `_`-prefixed ones.
#[cfg_attr(feature = "query-transport", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GqlStatusReport {
    pub gql_status: String,
    pub description: String,
    #[cfg_attr(feature = "query-transport", serde(default, skip_serializing_if = "BTreeMap::is_empty"))]
    pub diagnostic: BTreeMap<String, String>,
}

/// Each scope refusal maps to the `GraphError` whose text, class and protocol
/// code graph-node callers rely on: a depth overrun is an admission refusal
/// and an unknown database an invalid request.
impl From<hydradb_scope::ScopeError> for GraphError {
    fn from(error: hydradb_scope::ScopeError) -> Self {
        use hydradb_scope::ScopeError;
        match error {
            ScopeError::InvalidKeyComponent { component, value } => Self::InvalidKeyComponent { component, value },
            ScopeError::CorruptValue { key, reason } => Self::CorruptValue { key, reason },
            ScopeError::DepthExceeded { actual, limit } => Self::AdmissionRejected {
                operation: "namespace_depth",
                actual,
                limit,
            },
            ScopeError::UnknownDatabase { database } => Self::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "ClientProtocol",
                feature: format!("unknown graph database {database}"),
            },
        }
    }
}
