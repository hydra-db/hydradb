use crate::{
    BudgetLimits, BudgetTracker, CancellationToken, GraphProviderError, GraphReadView, GraphScope, IdempotencyIdentity,
    MutationFingerprint, ProviderId, Result, StatementSemanticsVersion, WriterAdmissionToken,
};
use async_trait::async_trait;
use hydradb_graph_types::{encode_value, CypherValue, NodeId, RelationshipId};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Instant, SystemTime},
};

/// Version of the persisted chunk/statement receipt envelope.
pub const MUTATION_RECEIPT_VERSION: u16 = 3;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MutationAtomicityPolicy {
    /// All staged actions commit in one provider transaction or none do.
    Statement,
    /// Explicit deterministic checkpoints may commit earlier action ranges.
    LegacyChunked,
}

impl std::fmt::Display for MutationAtomicityPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Statement => "statement",
            Self::LegacyChunked => "legacy_chunked",
        })
    }
}

/// Inclusive deterministic range of statement action ordinals.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ActionRange {
    pub first: u64,
    pub last: u64,
}

impl ActionRange {
    pub fn new(first: u64, last: u64) -> Result<Self> {
        if last < first {
            return Err(GraphProviderError::InvalidRequest("action range end precedes its start".into()));
        }
        Ok(Self { first, last })
    }

    pub fn len(&self) -> u128 {
        if self.is_empty() {
            0
        } else {
            u128::from(self.last) - u128::from(self.first) + 1
        }
    }

    pub fn is_empty(&self) -> bool {
        self.last < self.first
    }
}

/// Opaque causal token returned by a committed write.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CommitToken {
    provider: ProviderId,
    scope: GraphScope,
    bytes: Arc<[u8]>,
}

impl CommitToken {
    pub fn new(provider: ProviderId, scope: GraphScope, bytes: impl Into<Arc<[u8]>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(GraphProviderError::InvalidRequest("commit token must be non-empty".into()));
        }
        Ok(Self { provider, scope, bytes })
    }

    pub fn provider(&self) -> &ProviderId {
        &self.provider
    }

    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Debug for CommitToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitToken")
            .field("provider", &self.provider)
            .field("scope", &self.scope)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

pub type MutationProperties = BTreeMap<String, CypherValue>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationTarget {
    Node(NodeId),
    Relationship(RelationshipId),
}

/// A resolved, ordered graph write. Values are already bound and evaluated;
/// providers never need to interpret Cypher expressions or parameter slots.
#[derive(Clone, Debug, PartialEq)]
pub enum MutationOperation {
    CreateNode {
        identity: Option<NodeId>,
        labels: Vec<String>,
        properties: MutationProperties,
    },
    CreateRelationship {
        source: NodeId,
        destination: NodeId,
        relationship_type: String,
        properties: MutationProperties,
    },
    SetProperty {
        target: MutationTarget,
        key: String,
        /// Cypher `null` has SET-removal semantics and must not be stored as a
        /// present property; `Missing` is rejected recursively by the batch.
        value: CypherValue,
    },
    RemoveProperty {
        target: MutationTarget,
        key: String,
    },
    DeleteNode {
        node: NodeId,
        detach: bool,
    },
    DeleteRelationship {
        relationship: RelationshipId,
    },
    MergeNode {
        identity: Option<NodeId>,
        labels: Vec<String>,
        match_properties: MutationProperties,
        on_create: MutationProperties,
        on_match: MutationProperties,
    },
    MergeRelationship {
        source: NodeId,
        destination: NodeId,
        relationship_type: String,
        match_properties: MutationProperties,
        on_create: MutationProperties,
        on_match: MutationProperties,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutationAction {
    pub action_ordinal: u64,
    pub clause_ordinal: u32,
    pub input_ordinal: u64,
    pub operation: MutationOperation,
}

/// One contiguous range of fully resolved mutation actions.
#[derive(Clone, Debug, PartialEq)]
pub struct MutationBatch {
    pub sequence: u64,
    pub range: ActionRange,
    pub semantics_version: StatementSemanticsVersion,
    pub actions: Vec<MutationAction>,
    estimated_bytes: u64,
}

impl MutationBatch {
    pub fn new(
        sequence: u64,
        range: ActionRange,
        semantics_version: StatementSemanticsVersion,
        actions: Vec<MutationAction>,
    ) -> Result<Self> {
        if semantics_version.0 == 0 {
            return Err(GraphProviderError::InvalidRequest(
                "statement semantics version must be non-zero".into(),
            ));
        }
        if actions.len() as u128 != range.len() {
            return Err(GraphProviderError::InvalidRequest(
                "mutation action count must match its contiguous ordinal range".into(),
            ));
        }
        let mut estimated_bytes = 0u64;
        for (offset, action) in actions.iter().enumerate() {
            let expected_ordinal = u128::from(range.first) + offset as u128;
            if expected_ordinal > u128::from(u64::MAX) || action.action_ordinal != expected_ordinal as u64 {
                return Err(GraphProviderError::InvalidRequest(
                    "mutation action ordinals must exactly match the declared range".into(),
                ));
            }
            estimated_bytes = estimated_bytes
                .checked_add(action.estimated_bytes()?)
                .ok_or_else(|| GraphProviderError::InvalidRequest("mutation batch size overflow".into()))?;
        }
        Ok(Self {
            sequence,
            range,
            semantics_version,
            actions,
            estimated_bytes,
        })
    }

    /// Conservative serialized payload size using the canonical graph value
    /// codec. Providers use this with the batch and request-wide budgets before
    /// applying any action in the batch.
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub fn validate_bounds(&self, limits: BudgetLimits) -> Result<()> {
        let rows = u64::try_from(self.actions.len()).unwrap_or(u64::MAX);
        if rows > limits.max_batch_rows {
            return Err(GraphProviderError::BatchTooLarge {
                resource: "rows",
                actual: rows,
                maximum: limits.max_batch_rows,
            });
        }
        if self.estimated_bytes > limits.max_batch_bytes {
            return Err(GraphProviderError::BatchTooLarge {
                resource: "bytes",
                actual: self.estimated_bytes,
                maximum: limits.max_batch_bytes,
            });
        }
        Ok(())
    }
}

impl MutationAction {
    fn estimated_bytes(&self) -> Result<u64> {
        let mut bytes = 32u64;
        match &self.operation {
            MutationOperation::CreateNode {
                identity: _,
                labels,
                properties,
            } => {
                bytes = add_string_lengths(bytes, labels.iter().map(String::len))?;
                bytes = add_string_lengths(bytes, properties.keys().map(String::len))?;
                bytes = add_u64_bytes(bytes, encoded_values_size(properties.values())?)?;
            }
            MutationOperation::CreateRelationship {
                relationship_type,
                properties,
                ..
            } => {
                bytes = add_string_bytes(bytes, relationship_type)?;
                bytes = add_string_lengths(bytes, properties.keys().map(String::len))?;
                bytes = add_u64_bytes(bytes, encoded_values_size(properties.values())?)?;
            }
            MutationOperation::SetProperty { key, value, .. } => {
                validate_storable_value(value)?;
                bytes = add_string_bytes(bytes, key)?;
                bytes = add_bytes(bytes, encode_value(value)?.len())?;
            }
            MutationOperation::RemoveProperty { key, .. } => {
                bytes = add_string_bytes(bytes, key)?;
            }
            MutationOperation::DeleteNode { .. } | MutationOperation::DeleteRelationship { .. } => {}
            MutationOperation::MergeNode {
                identity: _,
                labels,
                match_properties,
                on_create,
                on_match,
            } => {
                bytes = add_string_lengths(bytes, labels.iter().map(String::len))?;
                for properties in [match_properties, on_create, on_match] {
                    bytes = add_string_lengths(bytes, properties.keys().map(String::len))?;
                    bytes = add_u64_bytes(bytes, encoded_values_size(properties.values())?)?;
                }
            }
            MutationOperation::MergeRelationship {
                relationship_type,
                match_properties,
                on_create,
                on_match,
                ..
            } => {
                bytes = add_string_bytes(bytes, relationship_type)?;
                for properties in [match_properties, on_create, on_match] {
                    bytes = add_string_lengths(bytes, properties.keys().map(String::len))?;
                    bytes = add_u64_bytes(bytes, encoded_values_size(properties.values())?)?;
                }
            }
        }
        Ok(bytes)
    }
}

fn add_bytes(current: u64, bytes: usize) -> Result<u64> {
    add_u64_bytes(current, u64::try_from(bytes).unwrap_or(u64::MAX))
}

fn add_string_lengths(current: u64, mut lengths: impl Iterator<Item = usize>) -> Result<u64> {
    let total = lengths.try_fold(0u64, |total, length| {
        let length = u64::try_from(length).unwrap_or(u64::MAX);
        let framed_length = length
            .checked_add(8)
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation batch size overflow".into()))?;
        total
            .checked_add(framed_length)
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation batch size overflow".into()))
    })?;
    add_u64_bytes(current, total)
}

fn add_string_bytes(current: u64, value: &str) -> Result<u64> {
    let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
    let framed_length = length
        .checked_add(8)
        .ok_or_else(|| GraphProviderError::InvalidRequest("mutation batch size overflow".into()))?;
    add_u64_bytes(current, framed_length)
}

fn add_u64_bytes(current: u64, bytes: u64) -> Result<u64> {
    current
        .checked_add(bytes)
        .ok_or_else(|| GraphProviderError::InvalidRequest("mutation batch size overflow".into()))
}

fn encoded_values_size<'a>(values: impl IntoIterator<Item = &'a CypherValue>) -> Result<u64> {
    values.into_iter().try_fold(0u64, |size, value| {
        validate_storable_value(value)?;
        let encoded_size = u64::try_from(encode_value(value)?.len()).unwrap_or(u64::MAX);
        size.checked_add(encoded_size)
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation value size overflow".into()))
    })
}

fn validate_storable_value(value: &CypherValue) -> Result<()> {
    match value {
        CypherValue::Missing => Err(GraphProviderError::InvalidRequest(
            "missing values cannot be stored as graph properties".into(),
        )),
        CypherValue::List(values) => {
            for value in values {
                validate_storable_value(value)?;
            }
            Ok(())
        }
        CypherValue::Map(properties) => {
            for value in properties.values() {
                validate_storable_value(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MutationCounters {
    pub nodes_created: u64,
    pub nodes_deleted: u64,
    pub relationships_created: u64,
    pub relationships_deleted: u64,
    pub properties_set: u64,
    pub properties_removed: u64,
    pub matched: u64,
    pub merged: u64,
}

impl MutationCounters {
    pub fn checked_add_assign(&mut self, other: &Self) -> Result<()> {
        let updated = Self {
            nodes_created: add_counter(self.nodes_created, other.nodes_created)?,
            nodes_deleted: add_counter(self.nodes_deleted, other.nodes_deleted)?,
            relationships_created: add_counter(self.relationships_created, other.relationships_created)?,
            relationships_deleted: add_counter(self.relationships_deleted, other.relationships_deleted)?,
            properties_set: add_counter(self.properties_set, other.properties_set)?,
            properties_removed: add_counter(self.properties_removed, other.properties_removed)?,
            matched: add_counter(self.matched, other.matched)?,
            merged: add_counter(self.merged, other.merged)?,
        };
        *self = updated;
        Ok(())
    }
}

fn add_counter(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| GraphProviderError::InvalidRequest("mutation counter overflow".into()))
}

#[derive(Clone, Debug)]
pub struct AppliedBindings {
    pub counters: MutationCounters,
    pub applied_range: ActionRange,
    /// Per-action identities and branch results are persisted so legacy
    /// checkpoints can resume without re-running already committed writes.
    pub action_outcomes: Vec<MutationActionOutcome>,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutationActionOutcome {
    pub action_ordinal: u64,
    pub input_ordinal: u64,
    /// Every entity resolved or affected by this action, in deterministic
    /// order. MERGE can resolve one input binding to multiple stored entities.
    pub targets: Arc<Vec<MutationTarget>>,
    pub counters: MutationCounters,
    /// `Some(true)` means MERGE created the entity; `Some(false)` means it
    /// matched. Non-MERGE actions use `None`.
    pub merge_created: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReturnManifestRow {
    pub input_ordinal: u64,
    /// Stable identity of the original input row for safe legacy recovery.
    /// Older receipts do not carry this value; those rows cannot be skipped
    /// during partial recovery without first validating their identity.
    pub input_fingerprint: Option<[u8; 32]>,
    /// The row's mutation range; `None` records a row with no mutation action.
    pub action_range: Option<ActionRange>,
    /// Zero or more RETURN rows produced by this input row. This is distinct
    /// from input multiplicity: one MERGE input can produce many result rows.
    pub rows: Vec<Vec<CypherValue>>,
    pub counters: MutationCounters,
}

#[derive(Clone, Debug)]
pub struct StatementReplay {
    pub receipt: CommitReceipt,
    pub return_manifest: Vec<ReturnManifestRow>,
}

#[derive(Clone, Debug)]
pub struct StatementReceiptLookup {
    pub scope: GraphScope,
    pub idempotency: IdempotencyIdentity,
    pub fingerprint: MutationFingerprint,
    pub semantics_version: StatementSemanticsVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementReceiptOrigin {
    DataFusion,
    Legacy,
}

#[derive(Clone, Debug)]
pub enum StatementReceiptResolution {
    NotFound,
    InProgress {
        origin: StatementReceiptOrigin,
        completed_actions: Vec<CompletedActionRange>,
        counters: MutationCounters,
        return_manifest: Vec<ReturnManifestRow>,
    },
    Committed {
        origin: StatementReceiptOrigin,
        replay: Box<StatementReplay>,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ChunkBoundary {
    pub id: [u8; 32],
    pub actions: ActionRange,
}

impl ChunkBoundary {
    pub fn new(id: [u8; 32], actions: ActionRange) -> Self {
        Self { id, actions }
    }
}

#[derive(Clone, Debug)]
pub struct ChunkReceipt {
    pub receipt_version: u16,
    pub boundary: ChunkBoundary,
    pub provider: ProviderId,
    pub scope: GraphScope,
    pub idempotency: IdempotencyIdentity,
    pub fingerprint: MutationFingerprint,
    pub semantics_version: StatementSemanticsVersion,
    pub policy: MutationAtomicityPolicy,
    pub status: MutationStatus,
    pub completed_actions: Vec<CompletedActionRange>,
    pub counters: MutationCounters,
    pub return_manifest: Vec<ReturnManifestRow>,
    pub commit_token: CommitToken,
    pub durability: CommitDurability,
    pub committed_at: SystemTime,
}

impl ChunkReceipt {
    pub fn validate_for(&self, request: &MutationRequest) -> Result<()> {
        if self.receipt_version != MUTATION_RECEIPT_VERSION {
            return Err(GraphProviderError::UnsupportedReceiptVersion {
                version: self.receipt_version,
            });
        }
        if request.atomicity != MutationAtomicityPolicy::LegacyChunked
            || self.policy != request.atomicity
            || self.scope != request.scope
            || self.commit_token.scope() != &request.scope
            || &self.provider != self.commit_token.provider()
        {
            return Err(GraphProviderError::InvalidRequest(
                "chunk receipt policy, provider, or scope mismatch".into(),
            ));
        }
        if self.idempotency != request.idempotency
            || self.fingerprint != request.fingerprint
            || self.semantics_version != request.semantics_version
        {
            return Err(GraphProviderError::IdempotencyConflict);
        }
        if self.durability != CommitDurability::Durable {
            return Err(GraphProviderError::DurabilityNotConfirmed);
        }
        if !range_is_covered(self.boundary.actions, &self.completed_actions) {
            return Err(GraphProviderError::InvalidRequest(
                "chunk receipt does not cover its boundary with completed actions".into(),
            ));
        }
        Ok(())
    }
}

fn range_is_covered(target: ActionRange, completed: &[CompletedActionRange]) -> bool {
    let mut next = target.first;
    for range in completed {
        if range.0.last < next {
            continue;
        }
        if range.0.first > next {
            return false;
        }
        if range.0.last >= target.last {
            return true;
        }
        let Some(after) = range.0.last.checked_add(1) else {
            return true;
        };
        next = after;
    }
    false
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CompletedActionRange(pub ActionRange);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationStatus {
    Committed,
    Replayed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitDurability {
    /// The configured provider durability barrier has completed.
    Durable,
    /// The provider accepted the commit but the barrier is not yet complete.
    Pending,
    /// The outcome cannot be determined from this response; resolve by receipt.
    Unknown,
}

#[derive(Clone, Debug)]
pub struct CommitReceipt {
    pub receipt_version: u16,
    pub provider: ProviderId,
    pub scope: GraphScope,
    pub idempotency: IdempotencyIdentity,
    pub fingerprint: MutationFingerprint,
    pub semantics_version: StatementSemanticsVersion,
    pub policy: MutationAtomicityPolicy,
    pub status: MutationStatus,
    pub completed_actions: Vec<CompletedActionRange>,
    pub counters: MutationCounters,
    pub return_manifest: Vec<ReturnManifestRow>,
    pub commit_token: CommitToken,
    pub durability: CommitDurability,
    pub committed_at: SystemTime,
}

impl CommitReceipt {
    /// Reject a receipt that does not correspond to the request whose outcome
    /// the caller is resolving. This is required for ambiguous commit recovery.
    pub fn validate_for(&self, request: &MutationRequest) -> Result<()> {
        if self.receipt_version != MUTATION_RECEIPT_VERSION {
            return Err(GraphProviderError::UnsupportedReceiptVersion {
                version: self.receipt_version,
            });
        }
        if self.scope != request.scope || self.commit_token.scope() != &request.scope || &self.provider != self.commit_token.provider() {
            return Err(GraphProviderError::InvalidRequest(
                "commit receipt provider or scope mismatch".into(),
            ));
        }
        if self.idempotency != request.idempotency
            || self.fingerprint != request.fingerprint
            || self.semantics_version != request.semantics_version
        {
            return Err(GraphProviderError::IdempotencyConflict);
        }
        if self.policy != request.atomicity {
            return Err(GraphProviderError::InvalidRequest(
                "commit receipt atomicity policy mismatch".into(),
            ));
        }
        Ok(())
    }
}

/// All state needed to begin a mutation is supplied before the provider opens
/// a transaction. The provider must not weaken the selected policy or switch
/// policy after this point.
#[derive(Clone, Debug)]
pub struct MutationRequest {
    pub scope: GraphScope,
    pub writer_admission: WriterAdmissionToken,
    pub idempotency: IdempotencyIdentity,
    pub fingerprint: MutationFingerprint,
    pub atomicity: MutationAtomicityPolicy,
    pub semantics_version: StatementSemanticsVersion,
    pub deadline: Option<Instant>,
    pub cancellation: CancellationToken,
    pub budget: BudgetLimits,
    pub max_io_concurrency: usize,
    pub requirements: crate::MutationRequirements,
}

impl MutationRequest {
    pub fn validate(&self) -> Result<()> {
        self.budget.validate()?;
        if self.max_io_concurrency == 0 {
            return Err(GraphProviderError::InvalidRequest("I/O concurrency must be non-zero".into()));
        }
        if self.semantics_version.0 == 0 {
            return Err(GraphProviderError::InvalidRequest(
                "statement semantics version must be non-zero".into(),
            ));
        }
        if self.cancellation.is_cancelled() {
            return Err(GraphProviderError::Cancelled);
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(GraphProviderError::DeadlineExceeded);
        }
        Ok(())
    }

    pub fn budget_tracker(&self) -> BudgetTracker {
        self.budget.tracker()
    }
}

/// Transaction-scoped mutation API. Implementations must provide read-your-
/// writes from `read_view`; `statement` checkpoints are forbidden, while
/// `legacy_chunked` checkpoints must durably record their deterministic range
/// and idempotency receipt in the same commit as the corresponding changes.
/// Implementations enforce cancellation/deadlines and charge applied batches to
/// the request budget before effects become checkpointable. Rejected batches
/// must not become a completed progress range.
#[async_trait]
pub trait GraphMutationSession: Send {
    fn read_view(&self) -> Arc<dyn GraphReadView>;

    /// Return the request's shared tracker when the session performs retained
    /// provider-side allocations. The mutation runtime uses this same tracker
    /// for later expansion buffers.
    fn budget_tracker(&self) -> Option<BudgetTracker> {
        None
    }

    /// Immutable view used to produce the statement's input rows. Mutation
    /// overlays may advance across checkpoints, but operators consuming this
    /// view must remain pinned to the statement's starting snapshot.
    fn input_read_view(&self) -> Arc<dyn GraphReadView> {
        self.read_view()
    }

    /// A completed idempotent statement can be replayed without executing
    /// mutation actions. Partial legacy recovery returns no full replay.
    fn replay(&self) -> Option<StatementReplay> {
        None
    }

    /// Previously committed RETURN rows for a partial legacy resume.
    fn return_manifest(&self) -> Vec<ReturnManifestRow> {
        Vec::new()
    }

    async fn apply(&mut self, batch: MutationBatch) -> Result<AppliedBindings>;

    /// Persist a bounded RETURN row with the next legacy checkpoint, or with
    /// the statement receipt at final commit.
    async fn record_return_manifest(&mut self, _row: ReturnManifestRow) -> Result<()> {
        Err(GraphProviderError::UnsupportedCapability {
            capability: "mutation RETURN replay manifest",
        })
    }

    async fn checkpoint(&mut self, boundary: ChunkBoundary) -> Result<ChunkReceipt>;

    async fn commit(self: Box<Self>) -> Result<CommitReceipt>;

    async fn abort(self: Box<Self>) -> Result<()>;
}
