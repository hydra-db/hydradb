use crate::{BudgetLimits, BudgetTracker, CancellationToken, GraphProviderError, Result, SnapshotIdentity};
use arrow_array::RecordBatch;
use futures::{stream::BoxStream, Stream, StreamExt};
use hydradb_graph_types::{CypherValue, NodeId, RelationshipId, RelationshipIdentity};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EntityKind {
    Node,
    Relationship,
}

/// Limits on each batch produced by a graph read operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchBounds {
    pub max_rows: u64,
    pub max_bytes: u64,
}

impl BatchBounds {
    pub fn new(max_rows: u64, max_bytes: u64) -> Result<Self> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(GraphProviderError::InvalidRequest(
                "batch row and byte bounds must be non-zero".into(),
            ));
        }
        Ok(Self { max_rows, max_bytes })
    }

    pub fn from_budget(budget: BudgetLimits) -> Self {
        Self {
            max_rows: budget.max_batch_rows,
            max_bytes: budget.max_batch_bytes,
        }
    }
}

/// A streaming sequence of bounded Arrow batches from one pinned view.
///
/// This wrapper validates batch sizes and aggregate read budgets even when a
/// provider implementation accidentally emits an oversized batch. It also
/// checks cancellation and deadlines between batches. Providers remain
/// responsible for checking these signals during long-running I/O and work
/// within a batch.
pub struct GraphBatchStream {
    inner: BoxStream<'static, Result<RecordBatch>>,
    bounds: BatchBounds,
    budget: BudgetTracker,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    terminated: bool,
}

impl std::fmt::Debug for GraphBatchStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphBatchStream")
            .field("bounds", &self.bounds)
            .field("budget", &self.budget.snapshot())
            .field("terminated", &self.terminated)
            .finish_non_exhaustive()
    }
}

impl GraphBatchStream {
    pub fn new<S>(stream: S, bounds: BatchBounds, budget: BudgetTracker, cancellation: CancellationToken, deadline: Option<Instant>) -> Self
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        let limits = budget.limits();
        let bounds = BatchBounds {
            max_rows: bounds.max_rows.min(limits.max_batch_rows),
            max_bytes: bounds.max_bytes.min(limits.max_batch_bytes),
        };
        Self {
            inner: stream.boxed(),
            bounds,
            budget,
            cancellation,
            deadline,
            terminated: false,
        }
    }

    pub fn budget_snapshot(&self) -> crate::BudgetSnapshot {
        self.budget.snapshot()
    }

    pub fn empty(budget: BudgetTracker, cancellation: CancellationToken, deadline: Option<Instant>) -> Self {
        Self::new(
            futures::stream::empty(),
            BatchBounds::from_budget(budget.limits()),
            budget,
            cancellation,
            deadline,
        )
    }
}

impl Stream for GraphBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        if this.cancellation.is_cancelled() {
            this.terminated = true;
            return Poll::Ready(Some(Err(GraphProviderError::Cancelled)));
        }
        if this.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            this.terminated = true;
            return Poll::Ready(Some(Err(GraphProviderError::DeadlineExceeded)));
        }

        match this.inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.terminated = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.terminated = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(batch))) => {
                let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
                let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
                if rows > this.bounds.max_rows {
                    this.terminated = true;
                    return Poll::Ready(Some(Err(GraphProviderError::BatchTooLarge {
                        resource: "rows",
                        actual: rows,
                        maximum: this.bounds.max_rows,
                    })));
                }
                if bytes > this.bounds.max_bytes {
                    this.terminated = true;
                    return Poll::Ready(Some(Err(GraphProviderError::BatchTooLarge {
                        resource: "bytes",
                        actual: bytes,
                        maximum: this.bounds.max_bytes,
                    })));
                }
                if let Err(error) = this.budget.consume_batch(rows, bytes) {
                    this.terminated = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(batch)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.terminated {
            (0, Some(0))
        } else {
            self.inner.size_hint()
        }
    }
}

/// Cursor tied to a snapshot, operator, and immutable plan fingerprint.
///
/// Tokens are opaque to clients and must not be resumed against another view
/// or a changed plan. The provider may place backend-specific cursor bytes in
/// `position` but cannot redefine these binding rules.
#[derive(Clone, Eq, PartialEq)]
pub struct ContinuationToken {
    snapshot: SnapshotIdentity,
    operator: Arc<str>,
    plan_fingerprint: [u8; 32],
    position: Arc<[u8]>,
}

impl ContinuationToken {
    pub fn new(
        snapshot: &SnapshotIdentity,
        operator: impl Into<Arc<str>>,
        plan_fingerprint: [u8; 32],
        position: impl Into<Arc<[u8]>>,
    ) -> Result<Self> {
        let operator = operator.into();
        let position = position.into();
        if operator.is_empty() || position.is_empty() {
            return Err(GraphProviderError::InvalidRequest(
                "continuation operator and cursor position must be non-empty".into(),
            ));
        }
        Ok(Self {
            snapshot: snapshot.clone(),
            operator,
            plan_fingerprint,
            position,
        })
    }

    pub fn validate_for(&self, snapshot: &SnapshotIdentity, operator: &str, plan_fingerprint: &[u8; 32]) -> Result<()> {
        if &self.snapshot != snapshot || self.operator.as_ref() != operator || &self.plan_fingerprint != plan_fingerprint {
            return Err(GraphProviderError::InvalidContinuationToken);
        }
        Ok(())
    }

    pub fn position(&self) -> &[u8] {
        &self.position
    }
}

impl std::fmt::Debug for ContinuationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuationToken")
            .field("snapshot", &self.snapshot)
            .field("operator", &self.operator)
            .field("plan_fingerprint", &"[REDACTED]")
            .field("position", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageOrder {
    /// The provider guarantees deterministic order and can issue stable cursors.
    Stable,
    /// Results are unordered; continuation may be used only within this view.
    Unordered,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRequest {
    pub limit: usize,
    pub order: PageOrder,
    pub continuation: Option<ContinuationToken>,
}

impl PageRequest {
    pub fn new(limit: usize, order: PageOrder, continuation: Option<ContinuationToken>) -> Result<Self> {
        if limit == 0 {
            return Err(GraphProviderError::InvalidRequest("page size must be non-zero".into()));
        }
        if continuation.is_some() && order != PageOrder::Stable {
            return Err(GraphProviderError::InvalidRequest(
                "continuation requires stable page ordering".into(),
            ));
        }
        Ok(Self {
            limit,
            order,
            continuation,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyProjection {
    pub properties: PropertySelection,
    pub include_labels: bool,
}

impl PropertyProjection {
    pub fn none() -> Self {
        Self {
            properties: PropertySelection::None,
            include_labels: false,
        }
    }

    pub fn keys(keys: Vec<Arc<str>>) -> Self {
        Self {
            properties: PropertySelection::Keys(keys),
            include_labels: false,
        }
    }

    pub fn all() -> Self {
        Self {
            properties: PropertySelection::All,
            include_labels: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PropertySelection {
    None,
    Keys(Vec<Arc<str>>),
    All,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    NodeIdEquals(NodeId),
    RelationshipIdEquals(RelationshipId),
    LabelEquals(Arc<str>),
    RelationshipTypeEquals(Arc<str>),
    PropertyExists(Arc<str>),
    PropertyEquals { key: Arc<str>, value: CypherValue },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScanSource {
    Nodes {
        labels: Vec<Arc<str>>,
    },
    Relationships {
        relationship_types: Vec<Arc<str>>,
        direction: RelationshipDirection,
        source_node_id: Option<NodeId>,
        destination_node_id: Option<NodeId>,
    },
    Seek {
        index: IndexDescriptor,
        keys: Vec<IndexSeekKey>,
        internal_id: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScanOrderKey {
    NodeId,
    RelationshipId,
    Property(Arc<str>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanOrder {
    pub key: ScanOrderKey,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanFetch {
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScanRequest {
    pub source: ScanSource,
    pub projection: PropertyProjection,
    pub predicates: Vec<Predicate>,
    pub order: Vec<ScanOrder>,
    pub fetch: Option<ScanFetch>,
}

/// Acceptance is positional with respect to the request predicates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredicateAcceptance {
    /// The provider applies the predicate exactly, so no residual is needed.
    Exact,
    /// The provider may return false positives but must not omit matches.
    Inexact,
    /// The provider does not apply this predicate.
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanAccessPath {
    NodeScan,
    RelationshipScan,
    IndexSeek,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanAcceptance {
    access_path: ScanAccessPath,
    predicates: Vec<PredicateAcceptance>,
    projection: bool,
    order: bool,
    fetch: Option<ScanFetch>,
}

impl ScanAcceptance {
    pub fn new(
        request: &ScanRequest,
        access_path: ScanAccessPath,
        predicates: Vec<PredicateAcceptance>,
        projection: bool,
        order: bool,
        fetch: Option<ScanFetch>,
    ) -> Result<Self> {
        if predicates.len() != request.predicates.len() {
            return Err(GraphProviderError::InvalidRequest(
                "one acceptance is required per predicate".into(),
            ));
        }
        if order && request.order.is_empty() {
            return Err(GraphProviderError::InvalidRequest("cannot accept an unrequested order".into()));
        }
        if let Some(accepted) = fetch {
            if Some(accepted) != request.fetch
                || predicates.iter().any(|status| *status != PredicateAcceptance::Exact)
                || (!request.order.is_empty() && !order)
            {
                return Err(GraphProviderError::InvalidRequest(
                    "fetch requires the requested limit and exact preceding semantics".into(),
                ));
            }
        }
        Ok(Self {
            access_path,
            predicates,
            projection,
            order,
            fetch,
        })
    }

    pub fn access_path(&self) -> ScanAccessPath {
        self.access_path
    }
    pub fn predicates(&self) -> &[PredicateAcceptance] {
        &self.predicates
    }
    pub fn projection(&self) -> bool {
        self.projection
    }
    pub fn order(&self) -> bool {
        self.order
    }
    pub fn fetch(&self) -> Option<ScanFetch> {
        self.fetch
    }
}

/// Execute pages from the pinned view. Page limits are not global fetch limits;
/// callers must continue paging before applying residual filters and sorts.
pub trait PreparedGraphScan: Send + Sync {
    fn acceptance(&self) -> &ScanAcceptance;
    fn execute(&self, page: PageRequest) -> Result<GraphBatchStream>;
}

struct LegacyPreparedScan<'a, T: ?Sized> {
    view: &'a T,
    request: ScanRequest,
    acceptance: ScanAcceptance,
}

impl<T: GraphReadView + ?Sized> PreparedGraphScan for LegacyPreparedScan<'_, T> {
    fn acceptance(&self) -> &ScanAcceptance {
        &self.acceptance
    }

    fn execute(&self, page: PageRequest) -> Result<GraphBatchStream> {
        let mut projection = PropertyProjection::all();
        projection.include_labels = true;
        match &self.request.source {
            ScanSource::Nodes { labels } => self.view.scan_nodes(NodeScanSpec {
                labels: labels.clone(),
                projection,
                predicates: vec![],
                page,
            }),
            ScanSource::Relationships {
                relationship_types,
                direction,
                source_node_id,
                destination_node_id,
            } => self.view.scan_relationships(RelationshipScanSpec {
                relationship_types: relationship_types.clone(),
                direction: *direction,
                source_node_id: *source_node_id,
                destination_node_id: *destination_node_id,
                projection,
                predicates: vec![],
                page,
            }),
            ScanSource::Seek { .. } => Err(GraphProviderError::UnsupportedCapability {
                capability: "prepared seek",
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeScanSpec {
    pub labels: Vec<Arc<str>>,
    pub projection: PropertyProjection,
    pub predicates: Vec<Predicate>,
    pub page: PageRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipScanSpec {
    pub relationship_types: Vec<Arc<str>>,
    pub direction: RelationshipDirection,
    pub source_node_id: Option<NodeId>,
    pub destination_node_id: Option<NodeId>,
    pub projection: PropertyProjection,
    pub predicates: Vec<Predicate>,
    pub page: PageRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDescriptor {
    pub entity: EntityKind,
    pub token: Arc<str>,
    pub properties: Vec<Arc<str>>,
    pub unique: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexSeekKey {
    pub input_ordinal: u64,
    pub values: Vec<CypherValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SeekSpec {
    pub index: IndexDescriptor,
    pub keys: Vec<IndexSeekKey>,
    /// Selects the provider's native entity identity rather than an indexed
    /// user property. `false` remains a property seek even when that property
    /// is named `id`.
    pub internal_id: bool,
    pub page: PageRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpansionInput {
    /// Preserves input multiplicity even when several rows share the same node.
    pub input_ordinal: u64,
    pub source_node_id: NodeId,
    pub target_node_id: Option<NodeId>,
    /// Canonical relationship identities already used by this input trail.
    /// Structural identities retain stored endpoint orientation, independent
    /// of traversal direction; identified identities exclude only that record.
    pub excluded_relationships: Vec<RelationshipIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpansionDepth {
    Fixed,
    Variable { min_hops: u32, max_hops: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipConstraints {
    pub types: Vec<Arc<str>>,
    pub direction: RelationshipDirection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathMode {
    Endpoints,
    Relationships,
    FullPath,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationshipUniqueness {
    /// Repeated relationships are allowed (walk semantics).
    Walk,
    /// A relationship may occur at most once in one expansion result.
    Trail,
    /// Neither nodes nor relationships may repeat within a path.
    SimplePath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpansionRequest {
    pub inputs: Vec<ExpansionInput>,
    pub constraints: RelationshipConstraints,
    pub depth: ExpansionDepth,
    pub path_mode: PathMode,
    pub uniqueness: RelationshipUniqueness,
    /// Optional per-input expansion cap. Global limits still apply through the
    /// request budget and the bounded result stream.
    pub max_results_per_input: Option<usize>,
}

impl ExpansionRequest {
    pub fn validate(&self) -> Result<()> {
        if let ExpansionDepth::Variable { min_hops, max_hops } = self.depth {
            if max_hops < min_hops {
                return Err(GraphProviderError::InvalidRequest(
                    "variable expansion max_hops must be at least min_hops".into(),
                ));
            }
        }
        if self.max_results_per_input == Some(0) {
            return Err(GraphProviderError::InvalidRequest(
                "per-input expansion cap must be non-zero".into(),
            ));
        }
        let mut ordinals = std::collections::HashSet::with_capacity(self.inputs.len());
        if self.inputs.iter().any(|input| !ordinals.insert(input.input_ordinal)) {
            return Err(GraphProviderError::InvalidRequest("expansion input ordinals must be unique".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeFetchKey {
    pub input_ordinal: u64,
    pub node_id: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationshipFetchKey {
    pub input_ordinal: u64,
    pub relationship_id: RelationshipId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeFetchRequest {
    pub entities: Vec<NodeFetchKey>,
    pub projection: PropertyProjection,
    pub missing: MissingEntityPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipFetchRequest {
    pub entities: Vec<RelationshipFetchKey>,
    pub projection: PropertyProjection,
    pub missing: MissingEntityPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissingEntityPolicy {
    /// Emit a binding row with explicit missing identity/existence state.
    Preserve,
    /// Return `GraphProviderError::MissingEntity` if a requested record is
    /// absent from this pinned snapshot.
    Error,
}

/// Pinned read access to graph data. Every operation must use this view's
/// snapshot and return bounded batches. Output batches use graph-types' public
/// canonical Arrow schemas. Duplicate input ordinals and fetch request rows are
/// preserved; fetch operations never silently drop an absent entity.
#[async_trait::async_trait]
pub trait GraphReadView: Send + Sync {
    fn snapshot_identity(&self) -> &SnapshotIdentity;

    async fn prepare_scan(&self, request: ScanRequest) -> Result<Box<dyn PreparedGraphScan + '_>> {
        if matches!(request.source, ScanSource::Seek { .. }) {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "prepared seek",
            });
        }
        let access_path = match &request.source {
            ScanSource::Nodes { .. } => ScanAccessPath::NodeScan,
            ScanSource::Relationships { .. } => ScanAccessPath::RelationshipScan,
            ScanSource::Seek { .. } => unreachable!(),
        };
        let acceptance = ScanAcceptance::new(
            &request,
            access_path,
            vec![PredicateAcceptance::Unsupported; request.predicates.len()],
            false,
            false,
            None,
        )?;
        Ok(Box::new(LegacyPreparedScan {
            view: self,
            request,
            acceptance,
        }))
    }

    fn scan_nodes(&self, spec: NodeScanSpec) -> Result<GraphBatchStream>;

    fn scan_relationships(&self, spec: RelationshipScanSpec) -> Result<GraphBatchStream>;

    fn seek(&self, spec: SeekSpec) -> Result<GraphBatchStream>;

    fn expand_batch(&self, request: ExpansionRequest) -> Result<GraphBatchStream>;

    fn fetch_nodes(&self, request: NodeFetchRequest) -> Result<GraphBatchStream>;

    fn fetch_relationships(&self, request: RelationshipFetchRequest) -> Result<GraphBatchStream>;
}
