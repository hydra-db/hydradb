use std::{
    collections::BTreeMap,
    mem::size_of,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use hydradb_graph_provider::{
    ActionRange, AppliedBindings, ChunkBoundary, ChunkReceipt, CommitDurability, CommitReceipt, CommitToken, CompletedActionRange,
    GraphMutationSession as ProviderMutationSession, GraphProviderError, GraphReadView, MutationAction, MutationActionOutcome,
    MutationAtomicityPolicy, MutationBatch, MutationCounters, MutationOperation, MutationRequest, MutationStatus, MutationTarget,
    ProviderId, Result, ReturnManifestRow, SnapshotId, SnapshotIdentity, StatementReceiptLookup, StatementReceiptOrigin,
    StatementReceiptResolution, StatementReplay,
};
use hydradb_graph_types::{decode_value, encode_value, CypherValue};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::write::DataFusionWriteGuard;
use crate::{
    provider::read::{backend_error, provider_backend_error},
    DbTransaction, GraphError, SlateGraphStore,
};

use super::SlateGraphReadView;

tokio::task_local! {
    static LEGACY_STATEMENT_RECEIPT: StatementReceiptLookup;
}

pub(super) struct SlateGraphMutationSession {
    coordinator: Arc<SlateGraphStore>,
    provider: ProviderId,
    request: MutationRequest,
    read_view: Arc<dyn GraphReadView>,
    input_read_view: Arc<dyn GraphReadView>,
    txn: Option<Arc<Mutex<Option<DbTransaction>>>>,
    guard: Option<DataFusionWriteGuard>,
    receipt_key: String,
    budget: hydradb_graph_provider::BudgetTracker,
    counters: MutationCounters,
    completed: Vec<CompletedActionRange>,
    committed_completed: Vec<CompletedActionRange>,
    action_outcomes: Vec<MutationActionOutcome>,
    return_manifest: Vec<ReturnManifestRow>,
    checkpoints: Vec<StoredCheckpoint>,
    next_action: u64,
    next_batch_sequence: u64,
    next_node_id: Option<u64>,
    write_back_catalog: crate::write::DataFusionWriteBackCatalog,
    pending_range: Option<ActionRange>,
    pending_row_ranges: BTreeMap<u64, ActionRange>,
    replayed: Option<CommitReceipt>,
    poisoned: bool,
}

#[derive(Clone)]
struct StoredReceipt {
    origin: StatementReceiptOrigin,
    status: StoredStatus,
    policy: MutationAtomicityPolicy,
    fingerprint: hydradb_graph_provider::MutationFingerprint,
    semantics_version: hydradb_graph_provider::StatementSemanticsVersion,
    sequence: u64,
    counters: MutationCounters,
    completed: Vec<CompletedActionRange>,
    action_outcomes: Vec<MutationActionOutcome>,
    return_manifest: Vec<ReturnManifestRow>,
    checkpoints: Vec<StoredCheckpoint>,
    committed_at: SystemTime,
}

#[derive(Clone)]
struct StoredCheckpoint {
    boundary: ChunkBoundary,
    sequence: u64,
    range_count: usize,
    manifest_count: usize,
    counters: MutationCounters,
    committed_at: SystemTime,
}

pub async fn scope_legacy_statement_receipt<F>(lookup: StatementReceiptLookup, future: F) -> F::Output
where
    F: std::future::Future,
{
    LEGACY_STATEMENT_RECEIPT.scope(lookup, future).await
}

pub async fn stage_legacy_statement_receipt(txn: &DbTransaction, sequence: u64) -> crate::Result<()> {
    let Ok(lookup) = LEGACY_STATEMENT_RECEIPT.try_with(Clone::clone) else {
        return Ok(());
    };
    let key = statement_receipt_key_for(&lookup.scope, &lookup.idempotency);
    let existing = crate::write::datafusion_read_receipt(txn, &key).await?;
    let mut receipt = match existing {
        Some(bytes) => {
            let mut receipt = decode_receipt(&bytes)?;
            validate_legacy_receipt(&receipt, &lookup)?;
            if receipt.status == StoredStatus::Committed {
                // Legacy retries retain their operation-level replay behavior.
                // Keep the original shared receipt for cross-engine replay.
                return Ok(());
            }
            receipt.sequence = sequence;
            receipt
        }
        None => empty_legacy_receipt(&lookup, sequence, StoredStatus::InProgress),
    };
    receipt.committed_at = SystemTime::now();
    txn.put(key.as_bytes(), encode_receipt(&receipt)?.as_slice())?;
    Ok(())
}

pub async fn complete_legacy_statement_receipt(
    shard: &SlateGraphStore,
    lookup: StatementReceiptLookup,
    result: impl FnOnce() -> crate::Result<(MutationCounters, Vec<ReturnManifestRow>)> + Send,
) -> crate::Result<u64> {
    let (txn, guard) = shard.begin_datafusion_transaction(lookup.scope.graph()).await?;
    let sequence = txn.seqnum().checked_add(1).ok_or_else(|| GraphError::CorruptValue {
        key: "storage_sequence".to_string(),
        reason: "SlateDB sequence overflow".to_string(),
    })?;
    let key = statement_receipt_key_for(&lookup.scope, &lookup.idempotency);
    if let Some(bytes) = crate::write::datafusion_read_receipt(&txn, &key).await? {
        let stored = decode_receipt(&bytes)?;
        validate_legacy_receipt(&stored, &lookup)?;
        if stored.status == StoredStatus::Committed {
            // Do not replace the original RETURN manifest with a legacy retry's
            // result (for example DELETE finds no rows on its second execution).
            return Ok(stored.sequence);
        }
    }
    let (counters, return_manifest) = result()?;
    let mut receipt = empty_legacy_receipt(&lookup, sequence, StoredStatus::Committed);
    receipt.counters = counters;
    receipt.return_manifest = return_manifest;
    receipt.committed_at = SystemTime::now();
    txn.put(key.as_bytes(), encode_receipt(&receipt)?.as_slice())?;
    let pipeline = guard.pipeline.clone();
    let committed_sequence = shard.commit_datafusion_transaction(txn, pipeline).await?.unwrap_or(sequence);
    drop(guard);
    Ok(committed_sequence)
}

fn empty_legacy_receipt(lookup: &StatementReceiptLookup, sequence: u64, status: StoredStatus) -> StoredReceipt {
    StoredReceipt {
        origin: StatementReceiptOrigin::Legacy,
        status,
        policy: MutationAtomicityPolicy::LegacyChunked,
        fingerprint: lookup.fingerprint,
        semantics_version: lookup.semantics_version,
        sequence,
        counters: MutationCounters::default(),
        completed: Vec::new(),
        action_outcomes: Vec::new(),
        return_manifest: Vec::new(),
        checkpoints: Vec::new(),
        committed_at: SystemTime::now(),
    }
}

fn validate_legacy_receipt(receipt: &StoredReceipt, lookup: &StatementReceiptLookup) -> crate::Result<()> {
    if receipt.origin != StatementReceiptOrigin::Legacy
        || receipt.fingerprint != lookup.fingerprint
        || receipt.semantics_version != lookup.semantics_version
        || receipt.policy != MutationAtomicityPolicy::LegacyChunked
    {
        return Err(legacy_receipt_conflict(
            lookup,
            "statement identity does not match its existing receipt",
        ));
    }
    Ok(())
}

fn legacy_receipt_conflict(lookup: &StatementReceiptLookup, reason: &'static str) -> GraphError {
    GraphError::IdempotencyConflict {
        operation: "cypher_statement",
        idempotency_key: lookup.idempotency.key().to_string(),
        reason,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoredStatus {
    InProgress,
    Committed,
}

impl SlateGraphMutationSession {
    pub(super) async fn begin(coordinator: Arc<SlateGraphStore>, provider: ProviderId, request: MutationRequest) -> Result<Self> {
        let shard = coordinator;
        let cell_id = request.scope.graph().to_owned();
        let receipt_key = statement_receipt_key(&request);
        // Read committed receipts before acquiring writer admission. A fully
        // completed retry must be a pure replay, not an empty write txn.
        if let Some(stored) = read_snapshot_receipt(&shard, &receipt_key)
            .await?
            .as_deref()
            .map(decode_receipt)
            .transpose()?
        {
            validate_stored_request(&stored, &request)?;
            if stored.status == StoredStatus::InProgress && stored.origin != StatementReceiptOrigin::DataFusion {
                return Err(GraphProviderError::IdempotencyConflict);
            }
            if stored.status == StoredStatus::Committed {
                let read_view = open_session_read_view(&shard, &provider, &request, None).await?;
                let receipt = receipt_from_stored(&provider, &request, stored.clone(), MutationStatus::Replayed, durability(&shard));
                let input_read_view = Arc::clone(&read_view);
                return Ok(Self {
                    coordinator: Arc::clone(&shard),
                    provider,
                    request: request.clone(),
                    read_view,
                    input_read_view,
                    txn: None,
                    guard: None,
                    receipt_key,
                    budget: request.budget.tracker(),
                    counters: stored.counters,
                    completed: stored.completed.clone(),
                    committed_completed: stored.completed,
                    action_outcomes: stored.action_outcomes,
                    return_manifest: stored.return_manifest,
                    checkpoints: stored.checkpoints,
                    next_action: 0,
                    next_batch_sequence: 0,
                    next_node_id: None,
                    write_back_catalog: Default::default(),
                    pending_range: None,
                    pending_row_ranges: BTreeMap::new(),
                    replayed: Some(receipt),
                    poisoned: false,
                });
            }
        }

        let (txn, guard) = shard.begin_datafusion_transaction(&cell_id).await.map_err(provider_backend_error)?;
        let txn = Arc::new(Mutex::new(Some(txn)));
        let prior = {
            let transaction = txn.lock().await;
            read_receipt(transaction.as_ref().expect("new transaction is open"), &receipt_key).await?
        };
        let stored = prior.as_deref().map(decode_receipt).transpose()?;
        if let Some(stored) = &stored {
            validate_stored_request(stored, &request)?;
            if stored.status == StoredStatus::InProgress && stored.origin != StatementReceiptOrigin::DataFusion {
                return Err(GraphProviderError::IdempotencyConflict);
            }
            if stored.status == StoredStatus::Committed {
                drop(txn);
                drop(guard);
                let read_view = open_session_read_view(&shard, &provider, &request, None).await?;
                let receipt = receipt_from_stored(&provider, &request, stored.clone(), MutationStatus::Replayed, durability(&shard));
                let input_read_view = Arc::clone(&read_view);
                return Ok(Self {
                    coordinator: Arc::clone(&shard),
                    provider,
                    request: request.clone(),
                    read_view,
                    input_read_view,
                    txn: None,
                    guard: None,
                    receipt_key,
                    budget: request.budget.tracker(),
                    counters: stored.counters.clone(),
                    completed: stored.completed.clone(),
                    committed_completed: stored.completed.clone(),
                    action_outcomes: stored.action_outcomes.clone(),
                    return_manifest: stored.return_manifest.clone(),
                    checkpoints: stored.checkpoints.clone(),
                    next_action: stored.completed.last().map_or(0, |range| range.0.last.saturating_add(1)),
                    next_batch_sequence: stored.completed.len() as u64,
                    next_node_id: None,
                    write_back_catalog: Default::default(),
                    pending_range: None,
                    pending_row_ranges: BTreeMap::new(),
                    replayed: Some(receipt),
                    poisoned: false,
                });
            }
            if stored.policy != MutationAtomicityPolicy::LegacyChunked {
                return Err(GraphProviderError::IdempotencyConflict);
            }
        }
        let stored = stored.filter(|stored| stored.status == StoredStatus::InProgress);
        let (input_read_view, read_view) = open_session_read_views(&shard, &provider, &request, Some(txn.clone())).await?;
        let mut session = Self {
            coordinator: Arc::clone(&shard),
            provider,
            request: request.clone(),
            read_view,
            input_read_view,
            txn: Some(txn),
            guard: Some(guard),
            receipt_key,
            budget: request.budget.tracker(),
            counters: stored
                .as_ref()
                .map_or_else(MutationCounters::default, |stored| stored.counters.clone()),
            completed: stored.as_ref().map_or_else(Vec::new, |stored| stored.completed.clone()),
            committed_completed: stored.as_ref().map_or_else(Vec::new, |stored| stored.completed.clone()),
            action_outcomes: stored.as_ref().map_or_else(Vec::new, |stored| stored.action_outcomes.clone()),
            return_manifest: stored.as_ref().map_or_else(Vec::new, |stored| stored.return_manifest.clone()),
            checkpoints: stored.as_ref().map_or_else(Vec::new, |stored| stored.checkpoints.clone()),
            next_action: stored
                .as_ref()
                .and_then(|stored| stored.completed.last())
                .map_or(0, |range| range.0.last.saturating_add(1)),
            next_batch_sequence: stored.as_ref().map_or(0, |stored| stored.completed.len() as u64),
            next_node_id: None,
            write_back_catalog: Default::default(),
            pending_range: None,
            pending_row_ranges: BTreeMap::new(),
            replayed: None,
            poisoned: false,
        };
        session.pending_range = None;
        Ok(session)
    }

    fn check_request(&self) -> Result<()> {
        if self.request.cancellation.is_cancelled() {
            return Err(GraphProviderError::Cancelled);
        }
        if self.request.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(GraphProviderError::DeadlineExceeded);
        }
        Ok(())
    }

    fn partial_error(&self, cause: GraphProviderError) -> GraphProviderError {
        if self.request.atomicity != MutationAtomicityPolicy::LegacyChunked || self.committed_completed.is_empty() {
            return cause;
        }
        GraphProviderError::PartialCompletion {
            completed_ranges: self.committed_completed.iter().map(|range| (range.0.first, range.0.last)).collect(),
            cause: Box::new(cause),
        }
    }

    fn coordinator(&self) -> &SlateGraphStore {
        &self.coordinator
    }

    async fn update_read_view(&mut self) -> Result<()> {
        self.read_view = open_session_read_view(self.coordinator(), &self.provider, &self.request, self.txn.clone()).await?;
        Ok(())
    }

    async fn commit_transaction(&mut self) -> Result<u64> {
        let shared_txn = self
            .txn
            .take()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation session has no open transaction".into()))?;
        let mut transaction = shared_txn.lock().await;
        let txn = transaction
            .take()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation transaction is already closed".into()))?;
        let sequence = txn.seqnum().checked_add(1).ok_or_else(|| GraphProviderError::Backend {
            code: "sequence_overflow".into(),
            message: "SlateDB sequence overflow".into(),
            retryable: false,
        })?;
        let pipeline = self.guard.as_ref().and_then(|guard| guard.pipeline.clone());
        self.coordinator()
            .commit_datafusion_transaction(txn, pipeline)
            .await
            .map_err(provider_backend_error)?;
        drop(transaction);
        self.guard.take();
        Ok(sequence)
    }

    async fn reopen_transaction(&mut self) -> Result<()> {
        let (txn, guard) = self
            .coordinator()
            .begin_datafusion_transaction(self.request.scope.graph())
            .await
            .map_err(provider_backend_error)?;
        self.txn = Some(Arc::new(Mutex::new(Some(txn))));
        self.guard = Some(guard);
        self.next_node_id = None;
        self.write_back_catalog.reset();
        self.update_read_view().await?;
        Ok(())
    }
}

#[async_trait]
impl ProviderMutationSession for SlateGraphMutationSession {
    fn read_view(&self) -> Arc<dyn GraphReadView> {
        self.read_view.clone()
    }

    fn budget_tracker(&self) -> Option<hydradb_graph_provider::BudgetTracker> {
        Some(self.budget.clone())
    }

    fn input_read_view(&self) -> Arc<dyn GraphReadView> {
        self.input_read_view.clone()
    }

    fn replay(&self) -> Option<StatementReplay> {
        self.replayed.as_ref().map(|receipt| StatementReplay {
            receipt: receipt.clone(),
            return_manifest: receipt.return_manifest.clone(),
        })
    }

    fn return_manifest(&self) -> Vec<ReturnManifestRow> {
        self.return_manifest.clone()
    }

    async fn apply(&mut self, batch: MutationBatch) -> Result<AppliedBindings> {
        self.check_request()?;
        if self.replayed.is_some() {
            return Err(GraphProviderError::InvalidRequest("cannot apply to a replayed mutation".into()));
        }
        if self.poisoned {
            return Err(GraphProviderError::InvalidRequest(
                "mutation session is aborted after an earlier action failure".into(),
            ));
        }
        batch.validate_bounds(self.request.budget)?;
        if batch.range.last < self.next_action {
            if batch.sequence >= self.next_batch_sequence || !self.completed.contains(&CompletedActionRange(batch.range)) {
                return Err(GraphProviderError::InvalidRequest(
                    "replayed action range does not match a completed receipt boundary".into(),
                ));
            }
            let action_count = self
                .action_outcomes
                .iter()
                .filter(|outcome| (batch.range.first..=batch.range.last).contains(&outcome.action_ordinal))
                .count();
            self.budget
                .consume_retained_bytes(
                    u64::try_from(action_count)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(size_of::<MutationActionOutcome>() as u64),
                )
                .map_err(|error| self.partial_error(error))?;
            let mut action_outcomes = Vec::new();
            action_outcomes
                .try_reserve_exact(action_count)
                .map_err(|error| self.partial_error(backend_error("allocation", error)))?;
            action_outcomes.extend(
                self.action_outcomes
                    .iter()
                    .filter(|outcome| (batch.range.first..=batch.range.last).contains(&outcome.action_ordinal))
                    .cloned(),
            );
            if action_outcomes.len() != batch.actions.len()
                || action_outcomes.iter().zip(&batch.actions).any(|(outcome, action)| {
                    outcome.action_ordinal != action.action_ordinal || outcome.input_ordinal != action.input_ordinal
                })
            {
                return Err(GraphProviderError::InvalidRequest(
                    "completed receipt is missing deterministic mutation action outcomes".into(),
                ));
            }
            let mut counters = MutationCounters::default();
            for outcome in &action_outcomes {
                counters.checked_add_assign(&outcome.counters)?;
            }
            return Ok(AppliedBindings {
                counters,
                applied_range: batch.range,
                action_outcomes,
                replayed: true,
            });
        }
        if batch.range.first < self.next_action {
            return Err(GraphProviderError::IdempotencyConflict);
        }
        if batch.sequence != self.next_batch_sequence {
            return Err(GraphProviderError::InvalidRequest(
                "mutation batches must arrive in contiguous sequence order".into(),
            ));
        }
        if batch.range.first != self.next_action {
            return Err(GraphProviderError::InvalidRequest(
                "mutation action ranges must be contiguous".into(),
            ));
        }
        self.budget
            .consume_batch(batch.actions.len() as u64, batch.estimated_bytes())
            .map_err(|error| self.partial_error(error))?;
        let action_outcome_bytes = u64::try_from(batch.actions.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(size_of::<MutationActionOutcome>() as u64);
        self.budget
            .consume_retained_bytes(action_outcome_bytes)
            .map_err(|error| self.partial_error(error))?;
        let mut action_outcomes = Vec::new();
        action_outcomes
            .try_reserve_exact(batch.actions.len())
            .map_err(|error| self.partial_error(backend_error("allocation", error)))?;
        let required_capacity = self
            .action_outcomes
            .len()
            .checked_add(batch.actions.len())
            .ok_or_else(|| self.partial_error(backend_error("allocation", "action outcome capacity overflow")))?;
        if required_capacity > self.action_outcomes.capacity() {
            let current_capacity = self.action_outcomes.capacity();
            let target_capacity = current_capacity.checked_mul(2).unwrap_or(required_capacity).max(required_capacity);
            let additional_capacity = target_capacity - current_capacity;
            let additional_bytes = u64::try_from(additional_capacity)
                .unwrap_or(u64::MAX)
                .saturating_mul(size_of::<MutationActionOutcome>() as u64);
            self.budget
                .consume_retained_bytes(additional_bytes)
                .map_err(|error| self.partial_error(error))?;
            self.action_outcomes
                .try_reserve_exact(target_capacity - self.action_outcomes.len())
                .map_err(|error| self.partial_error(backend_error("allocation", error)))?;
        }
        self.check_request()?;
        prevalidate_actions(&batch.actions).map_err(|error| self.partial_error(error))?;
        if let Some(pending) = self.pending_range {
            if pending.last.checked_add(1) != Some(batch.range.first) {
                return Err(GraphProviderError::InvalidRequest(
                    "uncheckpointed mutation ranges must remain contiguous".into(),
                ));
            }
        }

        let cell_id = self.request.scope.graph().to_owned();
        let shard = Arc::clone(&self.coordinator);
        let shared_txn = self
            .txn
            .as_ref()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation session has no open transaction".into()))?
            .clone();
        let transaction = shared_txn.lock().await;
        let txn = transaction
            .as_ref()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation transaction is already closed".into()))?;
        let mut batch_counters = MutationCounters::default();
        for action in &batch.actions {
            self.check_request()?;
            let result = shard
                .apply_datafusion_mutation(
                    txn,
                    &cell_id,
                    &action.operation,
                    &mut self.next_node_id,
                    &self.request,
                    &self.budget,
                    &mut self.write_back_catalog,
                )
                .await;
            match result {
                Ok((targets, counters)) => {
                    if let Err(error) = batch_counters.checked_add_assign(&counters) {
                        self.poisoned = true;
                        return Err(self.partial_error(error));
                    }
                    let merge_created = matches!(
                        &action.operation,
                        MutationOperation::MergeNode { .. } | MutationOperation::MergeRelationship { .. }
                    )
                    .then_some(counters.nodes_created > 0 || counters.relationships_created > 0);
                    action_outcomes.push(MutationActionOutcome {
                        action_ordinal: action.action_ordinal,
                        input_ordinal: action.input_ordinal,
                        targets,
                        counters,
                        merge_created,
                    });
                }
                Err(error) => {
                    self.poisoned = true;
                    return Err(self.partial_error(provider_backend_error(error)));
                }
            }
        }
        if let Err(error) = self.counters.checked_add_assign(&batch_counters) {
            self.poisoned = true;
            return Err(self.partial_error(error));
        }
        // A statement transaction spans many input rows, while each manifest
        // describes only the actions for one input row (possibly in several batches).
        for outcome in &action_outcomes {
            let range = self.pending_row_ranges.entry(outcome.input_ordinal).or_insert(ActionRange {
                first: outcome.action_ordinal,
                last: outcome.action_ordinal,
            });
            range.last = outcome.action_ordinal;
        }
        self.completed.push(CompletedActionRange(batch.range));
        self.action_outcomes.extend(action_outcomes.iter().cloned());
        self.pending_range = Some(match self.pending_range {
            Some(previous) => ActionRange {
                first: previous.first,
                last: batch.range.last,
            },
            None => batch.range,
        });
        self.next_action = batch.range.last.saturating_add(1);
        self.next_batch_sequence = self.next_batch_sequence.saturating_add(1);
        drop(transaction);
        Ok(AppliedBindings {
            counters: batch_counters,
            applied_range: batch.range,
            action_outcomes,
            replayed: false,
        })
    }

    async fn record_return_manifest(&mut self, row: ReturnManifestRow) -> Result<()> {
        self.check_request()?;
        if self.replayed.is_some() {
            return Err(GraphProviderError::InvalidRequest(
                "cannot append RETURN output to a replayed mutation".into(),
            ));
        }
        if self
            .return_manifest
            .iter()
            .any(|existing| existing.input_ordinal == row.input_ordinal)
        {
            return match self
                .return_manifest
                .iter()
                .find(|existing| existing.input_ordinal == row.input_ordinal)
            {
                Some(existing) if existing == &row => Ok(()),
                _ => Err(GraphProviderError::IdempotencyConflict),
            };
        }
        if row.action_range != self.pending_row_ranges.get(&row.input_ordinal).copied()
            || row.action_range.is_some_and(|range| {
                let first = self.action_outcomes.partition_point(|outcome| outcome.action_ordinal < range.first);
                self.action_outcomes[first..]
                    .iter()
                    .take_while(|outcome| outcome.action_ordinal <= range.last)
                    .any(|outcome| outcome.input_ordinal != row.input_ordinal)
            })
        {
            return Err(GraphProviderError::InvalidRequest(
                "RETURN manifest range must match the input row's pending mutation range".into(),
            ));
        }
        if self.return_manifest.len() as u64 >= self.request.budget.max_total_rows {
            return Err(GraphProviderError::BudgetExceeded {
                resource: hydradb_graph_provider::BudgetResource::Rows,
                limit: self.request.budget.max_total_rows,
            });
        }
        let mut encoded_bytes = std::mem::size_of::<ReturnManifestRow>();
        for value in row.rows.iter().flatten() {
            let value_bytes = encode_value(value)?.len();
            encoded_bytes = encoded_bytes
                .checked_add(value_bytes.saturating_mul(2))
                .ok_or_else(|| GraphProviderError::InvalidRequest("RETURN manifest size overflow".into()))?;
        }
        self.budget
            .consume_retained_bytes(u64::try_from(encoded_bytes).unwrap_or(u64::MAX))?;
        self.return_manifest
            .try_reserve_exact(1)
            .map_err(|error| backend_error("allocation", error))?;
        self.pending_row_ranges.remove(&row.input_ordinal);
        self.return_manifest.push(row);
        Ok(())
    }

    async fn checkpoint(&mut self, boundary: ChunkBoundary) -> Result<ChunkReceipt> {
        self.check_request()?;
        if self.request.atomicity != MutationAtomicityPolicy::LegacyChunked {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "statement checkpoint",
            });
        }
        if !self.coordinator().mutation_state().await_durable_writes {
            return Err(GraphProviderError::UnsupportedCapability {
                capability: "durable legacy chunk receipts",
            });
        }
        if self.poisoned {
            return Err(GraphProviderError::InvalidRequest(
                "cannot checkpoint an aborted mutation session".into(),
            ));
        }
        if let Some(checkpoint) = self.checkpoints.iter().find(|checkpoint| checkpoint.boundary.id == boundary.id) {
            if checkpoint.boundary != boundary {
                return Err(GraphProviderError::IdempotencyConflict);
            }
            return chunk_receipt_from_checkpoint(&self.provider, &self.request, checkpoint, &self.completed, &self.return_manifest);
        }
        if self.pending_range != Some(boundary.actions) {
            return Err(GraphProviderError::InvalidRequest(
                "checkpoint must cover the complete contiguous uncheckpointed action range".into(),
            ));
        }
        let shared_txn = self
            .txn
            .as_ref()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation session has no open transaction".into()))?
            .clone();
        let transaction = shared_txn.lock().await;
        let txn = transaction
            .as_ref()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation transaction is already closed".into()))?;
        let sequence = txn
            .seqnum()
            .checked_add(1)
            .ok_or_else(|| GraphProviderError::InvalidRequest("SlateDB sequence overflow".into()))?;
        let committed_at = SystemTime::now();
        let checkpoint = StoredCheckpoint {
            boundary,
            sequence,
            range_count: self.completed.len(),
            manifest_count: self.return_manifest.len(),
            counters: self.counters.clone(),
            committed_at,
        };
        self.checkpoints
            .try_reserve_exact(1)
            .map_err(|error| self.partial_error(backend_error("allocation", error)))?;
        self.checkpoints.push(checkpoint.clone());
        let stored = StoredReceipt {
            origin: StatementReceiptOrigin::DataFusion,
            status: StoredStatus::InProgress,
            policy: self.request.atomicity,
            fingerprint: self.request.fingerprint,
            semantics_version: self.request.semantics_version,
            sequence,
            counters: self.counters.clone(),
            completed: self.completed.clone(),
            action_outcomes: self.action_outcomes.clone(),
            return_manifest: self.return_manifest.clone(),
            checkpoints: self.checkpoints.clone(),
            committed_at,
        };
        let encoded = encode_receipt(&stored)?;
        txn.put(self.receipt_key.as_bytes(), encoded.as_slice())
            .map_err(provider_backend_error)?;
        drop(transaction);
        let committed_sequence = self.commit_transaction().await.map_err(|error| self.partial_error(error))?;
        self.committed_completed = self.completed.clone();
        self.pending_range = None;
        self.pending_row_ranges.clear();
        if let Err(error) = self.reopen_transaction().await {
            return Err(self.partial_error(error));
        }
        let mut persisted_checkpoint = checkpoint;
        persisted_checkpoint.sequence = committed_sequence;
        chunk_receipt_from_checkpoint(
            &self.provider,
            &self.request,
            &persisted_checkpoint,
            &self.completed,
            &self.return_manifest,
        )
    }

    async fn commit(mut self: Box<Self>) -> Result<CommitReceipt> {
        self.check_request()?;
        if self.poisoned {
            return Err(GraphProviderError::InvalidRequest(
                "cannot commit an aborted mutation session".into(),
            ));
        }
        if let Some(receipt) = self.replayed.take() {
            return Ok(receipt);
        }
        let shared_txn = self
            .txn
            .as_ref()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation session has no open transaction".into()))?
            .clone();
        let transaction = shared_txn.lock().await;
        let txn = transaction
            .as_ref()
            .ok_or_else(|| GraphProviderError::InvalidRequest("mutation transaction is already closed".into()))?;
        let sequence = txn
            .seqnum()
            .checked_add(1)
            .ok_or_else(|| GraphProviderError::InvalidRequest("SlateDB sequence overflow".into()))?;
        let stored = StoredReceipt {
            origin: StatementReceiptOrigin::DataFusion,
            status: StoredStatus::Committed,
            policy: self.request.atomicity,
            fingerprint: self.request.fingerprint,
            semantics_version: self.request.semantics_version,
            sequence,
            counters: self.counters.clone(),
            completed: self.completed.clone(),
            action_outcomes: self.action_outcomes.clone(),
            return_manifest: self.return_manifest.clone(),
            checkpoints: self.checkpoints.clone(),
            committed_at: SystemTime::now(),
        };
        let encoded = encode_receipt(&stored)?;
        txn.put(self.receipt_key.as_bytes(), encoded.as_slice())
            .map_err(provider_backend_error)?;
        drop(transaction);
        let shard = Arc::clone(&self.coordinator);
        let durability = durability(&shard);
        let sequence = self.commit_transaction().await.map_err(|error| self.partial_error(error))?;
        Ok(receipt_from_stored(
            &self.provider,
            &self.request,
            StoredReceipt { sequence, ..stored },
            MutationStatus::Committed,
            durability,
        ))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(shared_txn) = self.txn.take() {
            shared_txn.lock().await.take();
        }
        self.guard.take();
        Ok(())
    }
}

async fn open_session_read_view(
    shard: &SlateGraphStore,
    provider: &ProviderId,
    request: &MutationRequest,
    transaction: Option<Arc<Mutex<Option<DbTransaction>>>>,
) -> Result<Arc<dyn GraphReadView>> {
    let (snapshot, identity, read_request) = session_read_view_parts(shard, provider, request, &transaction).await?;
    Ok(Arc::new(SlateGraphReadView::new(
        snapshot,
        identity,
        read_request,
        transaction,
        shard.mutation_state().index_policy,
    )))
}

async fn open_session_read_views(
    shard: &SlateGraphStore,
    provider: &ProviderId,
    request: &MutationRequest,
    transaction: Option<Arc<Mutex<Option<DbTransaction>>>>,
) -> Result<(Arc<dyn GraphReadView>, Arc<dyn GraphReadView>)> {
    let (snapshot, identity, read_request) = session_read_view_parts(shard, provider, request, &transaction).await?;
    let input_view = Arc::new(SlateGraphReadView::new(
        snapshot.clone(),
        identity.clone(),
        read_request.clone(),
        None,
        shard.mutation_state().index_policy,
    ));
    let mutation_view = Arc::new(SlateGraphReadView::new(
        snapshot,
        identity,
        read_request,
        transaction,
        shard.mutation_state().index_policy,
    ));
    Ok((input_view, mutation_view))
}

async fn session_read_view_parts(
    shard: &SlateGraphStore,
    provider: &ProviderId,
    request: &MutationRequest,
    transaction: &Option<Arc<Mutex<Option<DbTransaction>>>>,
) -> Result<(
    Arc<crate::GraphStorageSnapshot>,
    SnapshotIdentity,
    hydradb_graph_provider::ReadRequest,
)> {
    let snapshot = shard.snapshot().await.map_err(provider_backend_error)?;
    // Transaction-backed reads must identify the transaction's pinned view,
    // not a newer database snapshot that happened to open afterward.
    let version = match &transaction {
        Some(transaction) => transaction.lock().await.as_ref().map_or(snapshot.seq(), |txn| txn.seqnum()),
        None => snapshot.seq(),
    };
    let mut id_bytes = version.to_be_bytes().to_vec();
    id_bytes.extend_from_slice(&snapshot.last_wal_id().unwrap_or_default().to_be_bytes());
    let identity = SnapshotIdentity::new(provider.clone(), request.scope.clone(), SnapshotId::new(id_bytes)?, Some(version));
    let read_request = hydradb_graph_provider::ReadRequest {
        scope: request.scope.clone(),
        causal_lower_bound: None,
        deadline: request.deadline,
        cancellation: request.cancellation.clone(),
        budget: request.budget,
        max_io_concurrency: request.max_io_concurrency,
        requirements: hydradb_graph_provider::ReadRequirements::default(),
    };
    Ok((snapshot, identity, read_request))
}

fn prevalidate_actions(actions: &[MutationAction]) -> Result<()> {
    for action in actions {
        match &action.operation {
            MutationOperation::CreateNode { labels, properties, .. } => {
                for label in labels {
                    crate::validate_component("label", label).map_err(provider_backend_error)?;
                }
                validate_properties(properties)?;
            }
            MutationOperation::CreateRelationship {
                relationship_type,
                properties,
                ..
            } => {
                crate::validate_component("edge_type", relationship_type).map_err(provider_backend_error)?;
                validate_properties(properties)?;
            }
            MutationOperation::SetProperty { key, value, .. } => {
                crate::validate_component("property", key).map_err(provider_backend_error)?;
                validate_property(value)?;
            }
            MutationOperation::RemoveProperty { key, .. } => crate::validate_component("property", key).map_err(provider_backend_error)?,
            MutationOperation::DeleteNode { detach: true, .. } => {
                return Err(GraphProviderError::UnsupportedCapability {
                    capability: "DETACH DELETE",
                });
            }
            MutationOperation::DeleteNode { .. } | MutationOperation::DeleteRelationship { .. } => {}
            MutationOperation::MergeNode {
                labels,
                match_properties,
                on_create,
                on_match,
                ..
            } => {
                for label in labels {
                    crate::validate_component("label", label).map_err(provider_backend_error)?;
                }
                validate_match_properties(match_properties)?;
                validate_properties(on_create)?;
                validate_properties(on_match)?;
            }
            MutationOperation::MergeRelationship {
                relationship_type,
                match_properties,
                on_create,
                on_match,
                ..
            } => {
                crate::validate_component("edge_type", relationship_type).map_err(provider_backend_error)?;
                validate_match_properties(match_properties)?;
                validate_properties(on_create)?;
                validate_properties(on_match)?;
            }
        }
    }
    Ok(())
}

fn validate_properties(properties: &hydradb_graph_provider::MutationProperties) -> Result<()> {
    for (key, value) in properties {
        crate::validate_component("property", key).map_err(provider_backend_error)?;
        validate_property(value)?;
    }
    Ok(())
}

fn validate_match_properties(properties: &hydradb_graph_provider::MutationProperties) -> Result<()> {
    validate_properties(properties)?;
    if properties
        .values()
        .any(|value| matches!(value, hydradb_graph_types::CypherValue::Null))
    {
        return Err(GraphProviderError::InvalidRequest(
            "MERGE match properties cannot contain null".into(),
        ));
    }
    Ok(())
}

fn validate_property(value: &hydradb_graph_types::CypherValue) -> Result<()> {
    if matches!(value, hydradb_graph_types::CypherValue::Null) {
        return Ok(());
    }
    crate::write::provider_property(value)
        .map(|_| ())
        .map_err(|error| GraphProviderError::InvalidRequest(error.to_string()))
}

fn statement_receipt_key(request: &MutationRequest) -> String {
    statement_receipt_key_for(&request.scope, &request.idempotency)
}

fn statement_receipt_key_for(scope: &hydradb_graph_provider::GraphScope, identity: &hydradb_graph_provider::IdempotencyIdentity) -> String {
    let mut digest = Sha256::new();
    digest.update(identity.principal().as_bytes());
    digest.update([0]);
    digest.update(identity.key().as_bytes());
    crate::keys::idempotency(scope.graph(), "cypher_statement", &hex(&digest.finalize()))
}

async fn read_receipt(txn: &DbTransaction, key: &str) -> Result<Option<Vec<u8>>> {
    crate::write::datafusion_read_receipt(txn, key)
        .await
        .map_err(provider_backend_error)
}

async fn read_snapshot_receipt(store: &SlateGraphStore, key: &str) -> Result<Option<Vec<u8>>> {
    let snapshot = store.snapshot().await.map_err(provider_backend_error)?;
    snapshot
        .get_with_options(key.as_bytes(), &crate::remote_read_options())
        .await
        .map(|value| value.map(|value| value.to_vec()))
        .map_err(provider_backend_error)
}

fn validate_stored_request(stored: &StoredReceipt, request: &MutationRequest) -> Result<()> {
    if stored.fingerprint != request.fingerprint
        || stored.policy != request.atomicity
        || stored.semantics_version != request.semantics_version
    {
        return Err(GraphProviderError::IdempotencyConflict);
    }
    Ok(())
}

// This persisted wire version is independent of the provider's mutation
// contract version. V5 is required for multiple ordered targets on one action
// and multiple ordered RETURN rows per input; v4 stores one target and one flat
// value list, so down-converting would lose replay semantics. A v4-only reader
// rejects this header as UnsupportedReceiptVersion { version: 0 }, and receipt
// lookup propagates that error instead of treating the key as absent.
fn encode_receipt(receipt: &StoredReceipt) -> Result<Vec<u8>> {
    let mut body = String::from("hydradb-cypher-statement-receipt-v5\n");
    body.push_str(&format!(
        "status\t{}\n",
        match receipt.status {
            StoredStatus::InProgress => "in_progress",
            StoredStatus::Committed => "committed",
        }
    ));
    body.push_str(&format!(
        "policy\t{}\n",
        match receipt.policy {
            MutationAtomicityPolicy::Statement => "statement",
            MutationAtomicityPolicy::LegacyChunked => "legacy_chunked",
        }
    ));
    body.push_str(&format!(
        "origin\t{}\n",
        match receipt.origin {
            StatementReceiptOrigin::DataFusion => "datafusion",
            StatementReceiptOrigin::Legacy => "legacy",
        }
    ));
    body.push_str(&format!("fingerprint\t{}\n", hex(&receipt.fingerprint.0)));
    body.push_str(&format!("semantics\t{}\n", receipt.semantics_version.0));
    body.push_str(&format!("sequence\t{}\n", receipt.sequence));
    body.push_str(&format!(
        "committed_at_ms\t{}\n",
        receipt.committed_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
    ));
    body.push_str(&format!(
        "counters\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        receipt.counters.nodes_created,
        receipt.counters.nodes_deleted,
        receipt.counters.relationships_created,
        receipt.counters.relationships_deleted,
        receipt.counters.properties_set,
        receipt.counters.properties_removed,
        receipt.counters.matched,
        receipt.counters.merged
    ));
    for range in &receipt.completed {
        body.push_str(&format!("range\t{}\t{}\n", range.0.first, range.0.last));
    }
    for outcome in &receipt.action_outcomes {
        let target = outcome
            .targets
            .iter()
            .map(|target| match target {
                MutationTarget::Node(id) => format!("node:{}", id.0),
                MutationTarget::Relationship(id) => format!("relationship:{}", id.0),
            })
            .collect::<Vec<_>>()
            .join(",");
        let target = if target.is_empty() { "none".to_owned() } else { target };
        let merge = match outcome.merge_created {
            None => "none",
            Some(true) => "created",
            Some(false) => "matched",
        };
        body.push_str(&format!(
            "action\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            outcome.action_ordinal,
            outcome.input_ordinal,
            target,
            merge,
            outcome.counters.nodes_created,
            outcome.counters.nodes_deleted,
            outcome.counters.relationships_created,
            outcome.counters.relationships_deleted,
            outcome.counters.properties_set,
            outcome.counters.properties_removed,
            outcome.counters.matched,
            outcome.counters.merged,
        ));
    }
    for row in &receipt.return_manifest {
        let (first, last) = row.action_range.map_or(("-".to_owned(), "-".to_owned()), |range| {
            (range.first.to_string(), range.last.to_string())
        });
        let rows = row
            .rows
            .iter()
            .map(|values| {
                values
                    .iter()
                    .map(|value| encode_value(value).map(|bytes| hex(&bytes)).map_err(GraphProviderError::GraphValue))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        body.push_str(&format!(
            "return\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            row.input_ordinal,
            first,
            last,
            row.counters.nodes_created,
            row.counters.nodes_deleted,
            row.counters.relationships_created,
            row.counters.relationships_deleted,
            row.counters.properties_set,
            row.counters.properties_removed,
            row.counters.matched,
            row.counters.merged,
            rows.len(),
        ));
        for values in rows {
            body.push_str(&format!("return_row\t{}\n", values.len()));
            for value in values {
                body.push_str(&format!("return_value\t{}\n", value));
            }
        }
        if let Some(fingerprint) = row.input_fingerprint {
            body.push_str(&format!("return_identity\t{}\t{}\n", row.input_ordinal, hex(&fingerprint)));
        }
    }
    for checkpoint in &receipt.checkpoints {
        body.push_str(&format!(
            "checkpoint\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            hex(&checkpoint.boundary.id),
            checkpoint.boundary.actions.first,
            checkpoint.boundary.actions.last,
            checkpoint.sequence,
            checkpoint.range_count,
            checkpoint.manifest_count,
            checkpoint.committed_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
            checkpoint.counters.nodes_created,
            checkpoint.counters.nodes_deleted,
            checkpoint.counters.relationships_created,
            checkpoint.counters.relationships_deleted,
            checkpoint.counters.properties_set,
            checkpoint.counters.properties_removed,
            checkpoint.counters.matched,
            checkpoint.counters.merged,
        ));
    }
    Ok(body.into_bytes())
}

struct PendingReturn {
    input_ordinal: u64,
    action_range: Option<ActionRange>,
    counters: MutationCounters,
    rows_remaining: usize,
    current_values_remaining: Option<usize>,
    current_values: Vec<CypherValue>,
    rows: Vec<Vec<CypherValue>>,
}

impl PendingReturn {
    fn finish_values(&mut self) -> Result<bool> {
        if self.current_values_remaining != Some(0) {
            return Err(invalid_receipt("incomplete RETURN manifest row"));
        }
        self.rows.push(std::mem::take(&mut self.current_values));
        self.current_values_remaining = None;
        self.rows_remaining = self
            .rows_remaining
            .checked_sub(1)
            .ok_or_else(|| invalid_receipt("too many RETURN manifest rows"))?;
        Ok(self.rows_remaining == 0)
    }

    fn into_manifest(self) -> ReturnManifestRow {
        ReturnManifestRow {
            input_ordinal: self.input_ordinal,
            input_fingerprint: None,
            action_range: self.action_range,
            rows: self.rows,
            counters: self.counters,
        }
    }
}

fn finish_pending_return(pending_return: &mut Option<PendingReturn>, return_manifest: &mut Vec<ReturnManifestRow>) {
    if pending_return.as_ref().is_some_and(|pending| pending.rows_remaining == 0) {
        return_manifest.push(pending_return.take().expect("completed RETURN row exists").into_manifest());
    }
}

fn decode_receipt(bytes: &[u8]) -> Result<StoredReceipt> {
    const MAX_RECEIPT_BYTES: usize = 64 * 1024 * 1024;
    if bytes.len() > MAX_RECEIPT_BYTES {
        return Err(invalid_receipt("receipt exceeds its maximum encoded size"));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| GraphProviderError::Backend {
        code: "invalid_receipt".into(),
        message: "receipt is not UTF-8".into(),
        retryable: false,
    })?;
    let mut lines = text.lines();
    let version = match lines.next() {
        Some("hydradb-cypher-statement-receipt-v5") => 5,
        Some("hydradb-cypher-statement-receipt-v4") => 4,
        Some("hydradb-cypher-statement-receipt-v3") => 3,
        Some("hydradb-cypher-statement-receipt-v2") => 2,
        Some("hydradb-cypher-statement-receipt-v1") => {
            return Err(GraphProviderError::UnsupportedReceiptVersion { version: 1 });
        }
        _ => return Err(GraphProviderError::UnsupportedReceiptVersion { version: 0 }),
    };
    let mut status = None;
    let mut policy = None;
    let mut origin = None;
    let mut fingerprint = None;
    let mut semantics_version = None;
    let mut sequence = None;
    let mut committed_at = None;
    let mut counters = None;
    let mut completed = Vec::new();
    let mut action_outcomes = Vec::new();
    let mut return_manifest = Vec::new();
    let mut checkpoints = Vec::new();
    let mut pending_return: Option<PendingReturn> = None;
    for line in lines {
        let parts = line.split('\t').collect::<Vec<_>>();
        match parts.as_slice() {
            ["status", value] => {
                status = Some(match *value {
                    "in_progress" => StoredStatus::InProgress,
                    "committed" => StoredStatus::Committed,
                    _ => return Err(invalid_receipt("invalid status")),
                })
            }
            ["policy", value] => {
                policy = Some(match *value {
                    "statement" => MutationAtomicityPolicy::Statement,
                    "legacy_chunked" => MutationAtomicityPolicy::LegacyChunked,
                    _ => return Err(invalid_receipt("invalid atomicity policy")),
                })
            }
            ["origin", value] => {
                origin = Some(match *value {
                    "datafusion" => StatementReceiptOrigin::DataFusion,
                    "legacy" => StatementReceiptOrigin::Legacy,
                    _ => return Err(invalid_receipt("invalid receipt origin")),
                })
            }
            ["fingerprint", value] => fingerprint = Some(hydradb_graph_provider::MutationFingerprint(parse_hex_32(value)?)),
            ["semantics", value] => {
                semantics_version = Some(hydradb_graph_provider::StatementSemanticsVersion(
                    value.parse().map_err(|_| invalid_receipt("invalid semantics version"))?,
                ))
            }
            ["sequence", value] => sequence = Some(value.parse().map_err(|_| invalid_receipt("invalid sequence"))?),
            ["committed_at_ms", value] => {
                committed_at = Some(UNIX_EPOCH + Duration::from_millis(value.parse().map_err(|_| invalid_receipt("invalid commit time"))?))
            }
            ["counters", nodes_created, nodes_deleted, rels_created, rels_deleted, props_set, props_removed, matched, merged] => {
                counters = Some(MutationCounters {
                    nodes_created: parse_num(nodes_created)?,
                    nodes_deleted: parse_num(nodes_deleted)?,
                    relationships_created: parse_num(rels_created)?,
                    relationships_deleted: parse_num(rels_deleted)?,
                    properties_set: parse_num(props_set)?,
                    properties_removed: parse_num(props_removed)?,
                    matched: parse_num(matched)?,
                    merged: parse_num(merged)?,
                })
            }
            ["range", first, last] => completed.push(CompletedActionRange(
                ActionRange::new(parse_num(first)?, parse_num(last)?).map_err(|_| invalid_receipt("invalid action range"))?,
            )),
            ["action", ordinal, input, target, merge, nodes_created, nodes_deleted, rels_created, rels_deleted, props_set, props_removed, matched, merged] =>
            {
                let targets = if version >= 5 {
                    if *target == "none" {
                        Vec::new()
                    } else {
                        target.split(',').map(parse_action_target).collect::<Result<Vec<_>>>()?
                    }
                } else if *target == "none" {
                    Vec::new()
                } else {
                    vec![parse_action_target(target)?]
                };
                let merge_created = match *merge {
                    "none" => None,
                    "created" => Some(true),
                    "matched" => Some(false),
                    _ => return Err(invalid_receipt("invalid MERGE outcome")),
                };
                action_outcomes.push(MutationActionOutcome {
                    action_ordinal: parse_num(ordinal)?,
                    input_ordinal: parse_num(input)?,
                    targets: Arc::new(targets),
                    counters: MutationCounters {
                        nodes_created: parse_num(nodes_created)?,
                        nodes_deleted: parse_num(nodes_deleted)?,
                        relationships_created: parse_num(rels_created)?,
                        relationships_deleted: parse_num(rels_deleted)?,
                        properties_set: parse_num(props_set)?,
                        properties_removed: parse_num(props_removed)?,
                        matched: parse_num(matched)?,
                        merged: parse_num(merged)?,
                    },
                    merge_created,
                });
            }
            ["return", input, first, last, nodes_created, nodes_deleted, rels_created, rels_deleted, props_set, props_removed, matched, merged, value_count] =>
            {
                if pending_return.is_some() {
                    return Err(invalid_receipt("nested RETURN manifest rows"));
                }
                let action_range = match (*first, *last) {
                    ("-", "-") => None,
                    (first, last) => Some(
                        ActionRange::new(parse_num(first)?, parse_num(last)?)
                            .map_err(|_| invalid_receipt("invalid RETURN manifest action range"))?,
                    ),
                };
                let count = value_count
                    .parse::<usize>()
                    .map_err(|_| invalid_receipt("invalid RETURN manifest value count"))?;
                if count == 0 {
                    return_manifest.push(ReturnManifestRow {
                        input_ordinal: parse_num(input)?,
                        input_fingerprint: None,
                        action_range,
                        rows: Vec::new(),
                        counters: MutationCounters {
                            nodes_created: parse_num(nodes_created)?,
                            nodes_deleted: parse_num(nodes_deleted)?,
                            relationships_created: parse_num(rels_created)?,
                            relationships_deleted: parse_num(rels_deleted)?,
                            properties_set: parse_num(props_set)?,
                            properties_removed: parse_num(props_removed)?,
                            matched: parse_num(matched)?,
                            merged: parse_num(merged)?,
                        },
                    });
                } else {
                    if pending_return.is_some() {
                        return Err(invalid_receipt("nested RETURN manifest rows"));
                    }
                    let mut rows = Vec::new();
                    let mut current_values = Vec::new();
                    let (rows_remaining, current_values_remaining) = if version >= 5 {
                        rows.try_reserve_exact(count)
                            .map_err(|_| invalid_receipt("RETURN manifest is too large"))?;
                        (count, None)
                    } else {
                        current_values
                            .try_reserve_exact(count)
                            .map_err(|_| invalid_receipt("RETURN manifest is too large"))?;
                        (1, Some(count))
                    };
                    pending_return = Some(PendingReturn {
                        input_ordinal: parse_num(input)?,
                        action_range,
                        counters: MutationCounters {
                            nodes_created: parse_num(nodes_created)?,
                            nodes_deleted: parse_num(nodes_deleted)?,
                            relationships_created: parse_num(rels_created)?,
                            relationships_deleted: parse_num(rels_deleted)?,
                            properties_set: parse_num(props_set)?,
                            properties_removed: parse_num(props_removed)?,
                            matched: parse_num(matched)?,
                            merged: parse_num(merged)?,
                        },
                        rows_remaining,
                        current_values_remaining,
                        current_values,
                        rows,
                    });
                }
            }
            ["return_row", value_count] => {
                if version < 5 {
                    return Err(invalid_receipt("RETURN row marker in an older receipt version"));
                }
                let Some(pending) = pending_return.as_mut() else {
                    return Err(invalid_receipt("RETURN row without manifest header"));
                };
                if pending.current_values_remaining.is_some() || pending.rows_remaining == 0 {
                    return Err(invalid_receipt("unexpected RETURN row marker"));
                }
                let count = value_count
                    .parse::<usize>()
                    .map_err(|_| invalid_receipt("invalid RETURN manifest value count"))?;
                pending
                    .current_values
                    .try_reserve_exact(count)
                    .map_err(|_| invalid_receipt("RETURN manifest is too large"))?;
                pending.current_values_remaining = Some(count);
                if count == 0 {
                    pending.finish_values()?;
                    finish_pending_return(&mut pending_return, &mut return_manifest);
                }
            }
            ["return_value", value] => {
                let Some(pending) = pending_return.as_mut() else {
                    return Err(invalid_receipt("RETURN value without manifest header"));
                };
                let Some(remaining) = pending.current_values_remaining.as_mut() else {
                    return Err(invalid_receipt("RETURN value without a row marker"));
                };
                if *remaining == 0 {
                    return Err(invalid_receipt("too many RETURN values"));
                }
                let value = decode_value(&parse_hex(value)?).map_err(GraphProviderError::GraphValue)?;
                pending.current_values.push(value);
                *remaining -= 1;
                if *remaining == 0 {
                    pending.finish_values()?;
                    finish_pending_return(&mut pending_return, &mut return_manifest);
                }
            }
            ["return_identity", input, fingerprint] => {
                if version < 4 {
                    return Err(invalid_receipt("input identity in an older receipt version"));
                }
                let input_ordinal = parse_num(input)?;
                let Some(row) = return_manifest.iter_mut().find(|row| row.input_ordinal == input_ordinal) else {
                    return Err(invalid_receipt("input identity without RETURN manifest row"));
                };
                if row.input_fingerprint.replace(parse_hex_32(fingerprint)?).is_some() {
                    return Err(invalid_receipt("duplicate RETURN input identity"));
                }
            }
            ["checkpoint", id, first, last, checkpoint_sequence, range_count, manifest_count, committed_at_ms, nodes_created, nodes_deleted, rels_created, rels_deleted, props_set, props_removed, matched, merged] =>
            {
                let id = parse_hex_32(id)?;
                let boundary = ChunkBoundary::new(
                    id,
                    ActionRange::new(parse_num(first)?, parse_num(last)?)
                        .map_err(|_| invalid_receipt("invalid checkpoint action range"))?,
                );
                checkpoints.push(StoredCheckpoint {
                    boundary,
                    sequence: parse_num(checkpoint_sequence)?,
                    range_count: range_count.parse().map_err(|_| invalid_receipt("invalid checkpoint range count"))?,
                    manifest_count: manifest_count
                        .parse()
                        .map_err(|_| invalid_receipt("invalid checkpoint manifest count"))?,
                    committed_at: UNIX_EPOCH + Duration::from_millis(parse_num(committed_at_ms)?),
                    counters: MutationCounters {
                        nodes_created: parse_num(nodes_created)?,
                        nodes_deleted: parse_num(nodes_deleted)?,
                        relationships_created: parse_num(rels_created)?,
                        relationships_deleted: parse_num(rels_deleted)?,
                        properties_set: parse_num(props_set)?,
                        properties_removed: parse_num(props_removed)?,
                        matched: parse_num(matched)?,
                        merged: parse_num(merged)?,
                    },
                });
            }
            _ => return Err(invalid_receipt("unknown or malformed receipt field")),
        }
    }
    if pending_return.is_some()
        || checkpoints
            .iter()
            .any(|checkpoint| checkpoint.range_count > completed.len() || checkpoint.manifest_count > return_manifest.len())
    {
        return Err(invalid_receipt("incomplete RETURN manifest or checkpoint state"));
    }
    if version >= 3 && origin.is_none() {
        return Err(invalid_receipt("missing receipt origin"));
    }
    Ok(StoredReceipt {
        // V2 receipts predate origin tracking and were written only by the
        // DataFusion mutation provider.
        origin: origin.unwrap_or(StatementReceiptOrigin::DataFusion),
        status: status.ok_or_else(|| invalid_receipt("missing status"))?,
        policy: policy.ok_or_else(|| invalid_receipt("missing policy"))?,
        fingerprint: fingerprint.ok_or_else(|| invalid_receipt("missing fingerprint"))?,
        semantics_version: semantics_version.ok_or_else(|| invalid_receipt("missing semantics version"))?,
        sequence: sequence.ok_or_else(|| invalid_receipt("missing sequence"))?,
        counters: counters.ok_or_else(|| invalid_receipt("missing counters"))?,
        completed,
        action_outcomes,
        return_manifest,
        checkpoints,
        committed_at: committed_at.ok_or_else(|| invalid_receipt("missing commit time"))?,
    })
}

fn receipt_from_stored(
    provider: &ProviderId,
    request: &MutationRequest,
    stored: StoredReceipt,
    status: MutationStatus,
    durability: CommitDurability,
) -> CommitReceipt {
    CommitReceipt {
        receipt_version: hydradb_graph_provider::MUTATION_RECEIPT_VERSION,
        provider: provider.clone(),
        scope: request.scope.clone(),
        idempotency: request.idempotency.clone(),
        fingerprint: request.fingerprint,
        semantics_version: request.semantics_version,
        policy: stored.policy,
        status,
        completed_actions: stored.completed,
        counters: stored.counters,
        return_manifest: stored.return_manifest,
        commit_token: CommitToken::new(provider.clone(), request.scope.clone(), stored.sequence.to_be_bytes().to_vec())
            .expect("eight-byte sequence is a valid token"),
        durability,
        committed_at: stored.committed_at,
    }
}

fn chunk_receipt_from_checkpoint(
    provider: &ProviderId,
    request: &MutationRequest,
    checkpoint: &StoredCheckpoint,
    completed: &[CompletedActionRange],
    return_manifest: &[ReturnManifestRow],
) -> Result<ChunkReceipt> {
    if checkpoint.range_count > completed.len() || checkpoint.manifest_count > return_manifest.len() {
        return Err(invalid_receipt("checkpoint points beyond persisted replay state"));
    }
    Ok(ChunkReceipt {
        receipt_version: hydradb_graph_provider::MUTATION_RECEIPT_VERSION,
        boundary: checkpoint.boundary,
        provider: provider.clone(),
        scope: request.scope.clone(),
        idempotency: request.idempotency.clone(),
        fingerprint: request.fingerprint,
        semantics_version: request.semantics_version,
        policy: request.atomicity,
        status: MutationStatus::Committed,
        completed_actions: completed[..checkpoint.range_count].to_vec(),
        counters: checkpoint.counters.clone(),
        return_manifest: return_manifest[..checkpoint.manifest_count].to_vec(),
        commit_token: CommitToken::new(provider.clone(), request.scope.clone(), checkpoint.sequence.to_be_bytes().to_vec())?,
        durability: CommitDurability::Durable,
        committed_at: checkpoint.committed_at,
    })
}

pub(super) async fn resolve_statement_receipt(
    shard: &SlateGraphStore,
    provider: &ProviderId,
    lookup: StatementReceiptLookup,
) -> Result<StatementReceiptResolution> {
    let key = statement_receipt_key_for(&lookup.scope, &lookup.idempotency);
    let Some(bytes) = read_snapshot_receipt(shard, &key).await? else {
        return Ok(StatementReceiptResolution::NotFound);
    };
    let stored = decode_receipt(&bytes)?;
    if stored.fingerprint != lookup.fingerprint || stored.semantics_version != lookup.semantics_version {
        return Err(GraphProviderError::IdempotencyConflict);
    }
    match stored.status {
        StoredStatus::InProgress => {
            if stored.policy != MutationAtomicityPolicy::LegacyChunked {
                return Err(GraphProviderError::IdempotencyConflict);
            }
            Ok(StatementReceiptResolution::InProgress {
                origin: stored.origin,
                completed_actions: stored.completed,
                counters: stored.counters,
                return_manifest: stored.return_manifest,
            })
        }
        StoredStatus::Committed => {
            let receipt = CommitReceipt {
                receipt_version: hydradb_graph_provider::MUTATION_RECEIPT_VERSION,
                provider: provider.clone(),
                scope: lookup.scope.clone(),
                idempotency: lookup.idempotency,
                fingerprint: lookup.fingerprint,
                semantics_version: lookup.semantics_version,
                policy: stored.policy,
                status: MutationStatus::Replayed,
                completed_actions: stored.completed,
                counters: stored.counters,
                return_manifest: stored.return_manifest.clone(),
                commit_token: CommitToken::new(provider.clone(), lookup.scope, stored.sequence.to_be_bytes().to_vec())?,
                durability: durability(shard),
                committed_at: stored.committed_at,
            };
            Ok(StatementReceiptResolution::Committed {
                origin: stored.origin,
                replay: Box::new(StatementReplay {
                    return_manifest: receipt.return_manifest.clone(),
                    receipt,
                }),
            })
        }
    }
}

fn durability(shard: &SlateGraphStore) -> CommitDurability {
    if shard.mutation_state().await_durable_writes {
        CommitDurability::Durable
    } else {
        CommitDurability::Pending
    }
}

fn invalid_receipt(message: &str) -> GraphProviderError {
    GraphProviderError::Backend {
        code: "invalid_receipt".into(),
        message: message.into(),
        retryable: false,
    }
}

fn parse_num(value: &str) -> Result<u64> {
    value.parse().map_err(|_| invalid_receipt("invalid integer"))
}

fn parse_action_target(value: &str) -> Result<MutationTarget> {
    match value {
        target if target.starts_with("node:") => Ok(MutationTarget::Node(hydradb_graph_types::NodeId(parse_num(&target[5..])?))),
        target if target.starts_with("relationship:") => Ok(MutationTarget::Relationship(hydradb_graph_types::RelationshipId(parse_num(
            &target[13..],
        )?))),
        _ => Err(invalid_receipt("invalid action target")),
    }
}

fn parse_hex_32(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(invalid_receipt("invalid fingerprint length"));
    }
    let mut bytes = [0_u8; 32];
    for (index, output) in bytes.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|_| invalid_receipt("invalid fingerprint encoding"))?;
    }
    Ok(bytes)
}

fn parse_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(invalid_receipt("invalid hex value length"));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| invalid_receipt("invalid hex value encoding")))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}
