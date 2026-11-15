//! Provider-backed execution for ordered graph mutation programs.
//!
//! Actions are issued in clause order and input-row order. Statement-atomic
//! sessions stay staged until `MutationSink` drains RETURN output and commits;
//! legacy-chunked sessions checkpoint after each input row.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    mem::size_of,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
};

use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array, ListArray, MapArray, StringArray,
        StructArray, UInt64Array,
    },
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Fields, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{common::DataFusionError, execution::SendableRecordBatchStream, physical_plan::stream::RecordBatchStreamAdapter};
use futures::{future::BoxFuture, lock::Mutex as AsyncMutex, stream, TryStreamExt};
use hydradb_graph_plan::{
    Direction as PlanDirection, EntityKind, GraphBinaryOperator, GraphExpression, MutationOperation as PlanOperation, MutationPolicy,
    MutationProgram, MutationRelationship, Projection, ScalarValue, Symbol, UnaryOperator,
};
use hydradb_graph_provider::{
    ActionRange, ChunkBoundary, GraphMutationSession, GraphProviderError, GraphReadView, MissingEntityPolicy, MutationAction,
    MutationActionOutcome, MutationBatch, MutationCounters, MutationOperation, MutationProperties, MutationRequest, MutationTarget,
    NodeFetchKey, NodeFetchRequest, PropertyProjection, RelationshipFetchKey, RelationshipFetchRequest, ReturnManifestRow,
    StatementSemanticsVersion,
};
use hydradb_graph_types::{
    canonical_value_data_type, encode_value_stable, semantic_cmp, semantic_eq_three_valued, value_array, value_from_array, CypherValue,
    NodeEntity, NodeId, Path, PathRelationship, RelationshipEntity, RelationshipId, RelationshipIdentity, RelationshipRef,
    TraversalOrientation,
};
use sha2::{Digest, Sha256};

use crate::context::{
    provider_error, GraphExecutionContext, MutationExecutionFinalizer, MutationExecutionPermit, MutationProgramOutput,
    MutationProgramRuntime,
};

/// Default runtime translating bound graph-plan mutations into provider action
/// batches and preserving the request's atomicity policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProviderMutationProgramRuntime;

impl ProviderMutationProgramRuntime {
    pub fn new() -> Self {
        Self
    }
}

/// Validate storage-neutral mutation support before a provider session is opened.
/// The whole program is checked, including unselected branches and FOREACH bodies,
/// so a late unsupported form cannot leave earlier legacy checkpoints committed.
pub fn validate_mutation_program(program: &MutationProgram) -> datafusion::common::Result<()> {
    fn expression(value: &GraphExpression) -> datafusion::common::Result<()> {
        match value {
            GraphExpression::Function { name, arguments, distinct } => {
                if *distinct
                    || !matches!(
                        name.as_str().to_ascii_lowercase().as_str(),
                        "coalesce"
                            | "tointeger"
                            | "toint"
                            | "tofloat"
                            | "tostring"
                            | "id"
                            | "size"
                            | "date"
                            | crate::value_construction::LIST_ELEMENT_FUNCTION
                    )
                {
                    return Err(DataFusionError::NotImplemented(format!(
                        "unsupported mutation expression function `{}` (DISTINCT={distinct})",
                        name.as_str()
                    )));
                }
                arguments.iter().try_for_each(expression)
            }
            GraphExpression::Binary { left, right, .. } => {
                expression(left)?;
                expression(right)
            }
            GraphExpression::Unary { expression: inner, .. } => expression(inner),
            GraphExpression::List(items) => items.iter().try_for_each(expression),
            GraphExpression::Map(items) => items.iter().try_for_each(|(_, value)| expression(value)),
            GraphExpression::Case { branches, fallback } => {
                for (condition, result) in branches {
                    expression(condition)?;
                    expression(result)?;
                }
                if let Some(fallback) = fallback {
                    expression(fallback)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    fn updates(items: &[hydradb_graph_plan::MutationUpdate]) -> datafusion::common::Result<()> {
        for update in items {
            reject_reserved_identity_target(&update.target)?;
            expression(&update.target)?;
            expression(&update.value)?;
        }
        Ok(())
    }
    fn reject_reserved_identity_target(target: &GraphExpression) -> datafusion::common::Result<()> {
        if matches!(
            target,
            GraphExpression::Property { entity, property }
                if entity.kind == EntityKind::Node && property.as_str() == "id"
        ) {
            return Err(provider_error(GraphProviderError::InvalidRequest(
                "graph identity `id` cannot be changed or removed".into(),
            )));
        }
        Ok(())
    }
    fn validate(operation: &PlanOperation) -> datafusion::common::Result<()> {
        match operation {
            PlanOperation::Merge { pattern, .. } if pattern.vertices.len() != 1 || !pattern.relationships.is_empty() => {
                return Err(DataFusionError::NotImplemented(
                    "multi-entity MERGE requires whole-pattern matching; only single-node MERGE is supported".into(),
                ));
            }
            PlanOperation::Create { pattern } | PlanOperation::Merge { pattern, .. } => {
                for vertex in &pattern.vertices {
                    for (_, value) in &vertex.properties {
                        expression(value)?;
                    }
                }
                for relationship in &pattern.relationships {
                    for (_, value) in &relationship.properties {
                        expression(value)?;
                    }
                }
                if let PlanOperation::Merge { on_match, on_create, .. } = operation {
                    updates(on_match)?;
                    updates(on_create)?;
                }
            }
            PlanOperation::Foreach { list, steps, .. } => {
                expression(list)?;
                steps.iter().try_for_each(validate)?;
            }
            PlanOperation::Set { updates: items } => updates(items)?,
            PlanOperation::Remove { targets } | PlanOperation::Delete { targets, .. } => {
                for target in targets {
                    if matches!(operation, PlanOperation::Remove { .. }) {
                        reject_reserved_identity_target(target)?;
                    }
                    expression(target)?;
                }
            }
        }
        Ok(())
    }
    program.steps.iter().try_for_each(|step| validate(&step.operation))?;
    program.returning.iter().try_for_each(|item| expression(&item.expression))
}

impl MutationProgramRuntime for ProviderMutationProgramRuntime {
    fn execute(
        &self,
        program: MutationProgram,
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        session: Box<dyn GraphMutationSession>,
        permit: MutationExecutionPermit,
        graph_context: Arc<GraphExecutionContext>,
    ) -> datafusion::common::Result<MutationProgramOutput> {
        validate_mutation_program(&program)?;
        let request = graph_context.mutation_request()?.clone();
        let policy_matches = matches!(
            (program.policy, request.atomicity),
            (
                MutationPolicy::Statement,
                hydradb_graph_provider::MutationAtomicityPolicy::Statement
            ) | (
                MutationPolicy::LegacyChunked,
                hydradb_graph_provider::MutationAtomicityPolicy::LegacyChunked
            )
        );
        if !policy_matches {
            return Err(DataFusionError::Plan(
                "mutation program policy differs from the admitted request".into(),
            ));
        }
        if !program.preserves_clause_order() {
            return Err(DataFusionError::Plan(
                "mutation program steps are not in strict clause order".into(),
            ));
        }
        let replay = session.replay();
        let replay_rows = replay.as_ref().map(|replay| {
            replay
                .return_manifest
                .iter()
                .flat_map(|manifest| manifest.rows.iter().cloned())
                .collect::<Vec<_>>()
        });
        let replay_receipt = replay.as_ref().map(|replay| replay.receipt.clone());
        let persisted_rows = session.return_manifest();
        let mut initial_counters = replay_receipt.as_ref().map(|receipt| receipt.counters.clone()).unwrap_or_default();
        if replay_receipt.is_none() {
            for row in &persisted_rows {
                initial_counters.checked_add_assign(&row.counters).map_err(provider_error)?;
            }
        }
        let return_manifest = persisted_rows.iter().cloned().map(|row| (row.input_ordinal, row)).collect();
        let resume_manifest = persisted_rows.into_iter().map(|row| (row.input_ordinal, row)).collect();
        let session = Arc::new(AsyncMutex::new(Some(session)));
        let counters = Arc::new(Mutex::new(initial_counters));
        let state = MutationStreamState {
            input,
            output_schema,
            program,
            request: request.clone(),
            session: Arc::clone(&session),
            counters: Arc::clone(&counters),
            graph_context: Arc::clone(&graph_context),
            next_action: 0,
            next_batch_sequence: 0,
            next_input_ordinal: 0,
            property_cache: MutationPropertyCache::default(),
            return_manifest,
            resume_manifest,
            replay_rows,
            replay_index: 0,
            done: false,
        };
        let output_schema = Arc::clone(&state.output_schema);
        let output_stream = stream::try_unfold(state, |mut state| async move {
            loop {
                if state.done {
                    return Ok(None);
                }
                if let Err(error) = check_mutation_context(&state.graph_context) {
                    abort_state(&state.session).await;
                    return Err(error);
                }
                if let Some(replay_rows) = state.replay_rows.as_ref() {
                    if state.output_schema.fields().is_empty() {
                        return Ok(None);
                    }
                    if state.replay_index >= replay_rows.len() {
                        return Ok(None);
                    }
                    let batch_size = usize::try_from(state.request.budget.max_batch_rows).unwrap_or(usize::MAX).max(1);
                    let end = state.replay_index.saturating_add(batch_size).min(replay_rows.len());
                    let values = replay_rows[state.replay_index..end].to_vec();
                    state.replay_index = end;
                    let output = rows_to_batch(&values, &state.output_schema)?;
                    // Entity-shaped Arrow outputs may carry only identities. Replay
                    // must use the persisted complete values, just as first execution
                    // does, rather than reconstructing public rows from that batch.
                    state
                        .graph_context
                        .validate_mutation_result_before_checkpoint_with_values(&output, &values)?;
                    return Ok(Some((output, state)));
                }
                let batch = match next_input_batch(&mut state).await {
                    Ok(batch) => batch,
                    Err(error) => {
                        abort_state(&state.session).await;
                        return Err(error);
                    }
                };
                let Some(batch) = batch else {
                    if !state.resume_manifest.is_empty() {
                        abort_state(&state.session).await;
                        return Err(provider_error(GraphProviderError::IdempotencyConflict));
                    }
                    return Ok(None);
                };
                match execute_input_batch(&mut state, batch).await {
                    Ok(Some(output)) => return Ok(Some((output, state))),
                    Ok(None) => continue,
                    Err(error) => {
                        abort_state(&state.session).await;
                        return Err(error);
                    }
                }
            }
        });
        let finalizer = ProviderMutationFinalizer {
            session,
            request,
            replay_receipt,
            cancellation: graph_context.cancellation().clone(),
            cancelled: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            _permit: permit,
        };
        Ok(MutationProgramOutput {
            stream: Box::pin(RecordBatchStreamAdapter::new(output_schema, output_stream)),
            finalizer: Box::new(finalizer),
            counters,
        })
    }
}

struct MutationStreamState {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    program: MutationProgram,
    request: MutationRequest,
    session: Arc<AsyncMutex<Option<Box<dyn GraphMutationSession>>>>,
    counters: Arc<Mutex<MutationCounters>>,
    graph_context: Arc<GraphExecutionContext>,
    next_action: u64,
    next_batch_sequence: u64,
    next_input_ordinal: u64,
    property_cache: MutationPropertyCache,
    return_manifest: BTreeMap<u64, ReturnManifestRow>,
    resume_manifest: BTreeMap<u64, ReturnManifestRow>,
    replay_rows: Option<Vec<Vec<CypherValue>>>,
    replay_index: usize,
    done: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum PropertyEntity {
    Node(NodeId),
    Relationship(RelationshipId),
}

#[derive(Default)]
struct MutationPropertyCache {
    values: HashMap<(PropertyEntity, String), CypherValue>,
}

impl MutationPropertyCache {
    fn get(&self, target: MutationTarget, key: &str) -> Option<&CypherValue> {
        self.values.get(&(property_entity(target), key.to_owned()))
    }

    fn set(&mut self, target: MutationTarget, key: impl Into<String>, value: CypherValue) {
        self.values.insert((property_entity(target), key.into()), value);
    }

    fn invalidate(&mut self, target: MutationTarget, key: &str) {
        self.values.remove(&(property_entity(target), key.to_owned()));
    }

    fn remove_entity(&mut self, target: MutationTarget) {
        let entity = property_entity(target);
        self.values.retain(|(key, _), _| *key != entity);
    }
}

fn property_entity(target: MutationTarget) -> PropertyEntity {
    match target {
        MutationTarget::Node(id) => PropertyEntity::Node(id),
        MutationTarget::Relationship(id) => PropertyEntity::Relationship(id),
    }
}

struct ProviderMutationFinalizer {
    session: Arc<AsyncMutex<Option<Box<dyn GraphMutationSession>>>>,
    request: MutationRequest,
    replay_receipt: Option<hydradb_graph_provider::CommitReceipt>,
    cancellation: hydradb_graph_provider::CancellationToken,
    cancelled: AtomicBool,
    completed: AtomicBool,
    _permit: MutationExecutionPermit,
}

impl MutationExecutionFinalizer for ProviderMutationFinalizer {
    fn cancel(&self) {
        self.cancelled.store(true, AtomicOrdering::Release);
        self.cancellation.cancel();
        if let Some(mut session) = self.session.try_lock() {
            // Dropping the uncommitted provider session is the synchronous
            // cancellation fallback if the downstream dropped the sink.
            session.take();
        }
    }

    fn commit(self: Box<Self>) -> futures::future::BoxFuture<'static, datafusion::common::Result<hydradb_graph_provider::CommitReceipt>> {
        Box::pin(async move {
            if self.cancelled.load(AtomicOrdering::Acquire) || self.cancellation.is_cancelled() {
                return Err(DataFusionError::Execution("cancelled mutation cannot commit".into()));
            }
            if self.completed.swap(true, AtomicOrdering::AcqRel) {
                return Err(DataFusionError::Execution("mutation was already finalized".into()));
            }
            if let Some(receipt) = self.replay_receipt.clone() {
                receipt.validate_for(&self.request).map_err(provider_error)?;
                self.session.lock().await.take();
                return Ok(receipt);
            }
            let session = self
                .session
                .lock()
                .await
                .take()
                .ok_or_else(|| DataFusionError::Execution("mutation session was already aborted".into()))?;
            let receipt = session.commit().await.map_err(provider_error)?;
            receipt.validate_for(&self.request).map_err(provider_error)?;
            Ok(receipt)
        })
    }

    fn abort(self: Box<Self>) -> futures::future::BoxFuture<'static, datafusion::common::Result<()>> {
        self.cancellation.cancel();
        self.cancelled.store(true, AtomicOrdering::Release);
        Box::pin(async move {
            if self.completed.swap(true, AtomicOrdering::AcqRel) {
                return Ok(());
            }
            let session = self.session.lock().await.take();
            if let Some(session) = session {
                session.abort().await.map_err(provider_error)?;
            }
            Ok(())
        })
    }
}

impl Drop for ProviderMutationFinalizer {
    fn drop(&mut self) {
        if !self.completed.load(AtomicOrdering::Acquire) {
            self.cancellation.cancel();
            self.cancelled.store(true, AtomicOrdering::Release);
            if let Some(mut session) = self.session.try_lock() {
                session.take();
            }
        }
    }
}

async fn abort_state(session: &Arc<AsyncMutex<Option<Box<dyn GraphMutationSession>>>>) {
    let session = session.lock().await.take();
    if let Some(session) = session {
        let _ = session.abort().await;
    }
}

fn check_mutation_context(context: &GraphExecutionContext) -> datafusion::common::Result<()> {
    if context.cancellation().is_cancelled() {
        return Err(DataFusionError::Execution("graph mutation was cancelled".into()));
    }
    if context.deadline().is_some_and(|deadline| std::time::Instant::now() >= deadline) {
        return Err(DataFusionError::Execution("graph mutation deadline exceeded".into()));
    }
    Ok(())
}

async fn next_input_batch(state: &mut MutationStreamState) -> datafusion::common::Result<Option<RecordBatch>> {
    if state.program.steps.len() <= 1 || state.next_input_ordinal > 0 {
        return state.input.try_next().await;
    }

    let mut only_row = None;
    while let Some(batch) = state.input.try_next().await? {
        check_mutation_context(&state.graph_context)?;
        if batch.num_rows() == 0 {
            continue;
        }
        if batch.num_rows() > 1 || only_row.is_some() {
            return Err(DataFusionError::NotImplemented(
                "multiple mutation clauses over multiple input rows require clause-major execution".into(),
            ));
        }
        only_row = Some(batch);
    }
    Ok(only_row)
}

async fn execute_input_batch(state: &mut MutationStreamState, input: RecordBatch) -> datafusion::common::Result<Option<RecordBatch>> {
    let first_action = state.next_action;
    let mut return_rows = Vec::with_capacity(input.num_rows());
    let mut session_guard = state.session.lock().await;
    let session = session_guard
        .as_mut()
        .ok_or_else(|| DataFusionError::Execution("mutation session is no longer active".into()))?;
    for row_index in 0..input.num_rows() {
        check_mutation_context(&state.graph_context)?;
        let bindings = decode_input_row(&input, row_index)?;
        let input_ordinal = state.next_input_ordinal;
        let input_fingerprint = input_row_fingerprint(&bindings)?;
        if let Some(previous) = state.resume_manifest.remove(&input_ordinal) {
            if previous.input_fingerprint != Some(input_fingerprint) {
                return Err(provider_error(GraphProviderError::IdempotencyConflict));
            }
        }
        let row_first_action = state.next_action;
        let mut row_counters = MutationCounters::default();
        state.next_input_ordinal = state
            .next_input_ordinal
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("mutation input ordinal overflow".into()))?;
        let mut binding_rows = vec![bindings];
        for step in &state.program.steps {
            let clause =
                u32::try_from(step.ordinal).map_err(|_| DataFusionError::Plan("mutation clause ordinal exceeds provider range".into()))?;
            let mut cursor = ActionCursor {
                next_action: &mut state.next_action,
                next_batch_sequence: &mut state.next_batch_sequence,
                semantics_version: state.request.semantics_version,
                counters: &state.counters,
                row_counters: &mut row_counters,
            };
            let mut next_binding_rows = Vec::new();
            for bindings in binding_rows {
                next_binding_rows.extend(
                    execute_operation(
                        session.as_mut(),
                        &step.operation,
                        bindings,
                        input_ordinal,
                        clause,
                        &mut cursor,
                        &mut state.property_cache,
                        &state.graph_context,
                    )
                    .await?,
                );
            }
            binding_rows = next_binding_rows;
        }
        let action_range = (state.next_action > row_first_action)
            .then(|| ActionRange::new(row_first_action, state.next_action - 1).map_err(provider_error))
            .transpose()?;
        let (mut values, already_recorded) = if let Some(manifest) = state.return_manifest.get(&input_ordinal) {
            if manifest.action_range != action_range {
                return Err(provider_error(GraphProviderError::InvalidRequest(
                    "resumed mutation row action range does not match its persisted return manifest".into(),
                )));
            }
            (manifest.rows.clone(), true)
        } else if state.program.returning.is_empty() {
            (Vec::new(), false)
        } else {
            let mut rows = Vec::with_capacity(binding_rows.len());
            for bindings in &binding_rows {
                rows.push(evaluate_returning(&state.program.returning, bindings, &mut state.property_cache, &state.graph_context).await?);
            }
            (rows, false)
        };
        if !state.program.returning.is_empty() {
            let read_view = session.read_view();
            let input_read_view = state.graph_context.input_read_view();
            for row in &mut values {
                for value in row {
                    *value = materialize_mutation_result_value(value.clone(), Arc::clone(&read_view), Arc::clone(&input_read_view)).await?;
                }
            }
        }
        if !values.is_empty() && !state.output_schema.fields().is_empty() {
            let result_rows = rows_to_batch(&values, &state.output_schema)?;
            state
                .graph_context
                .validate_mutation_result_before_checkpoint_with_values(&result_rows, &values)?;
        }
        let manifest = ReturnManifestRow {
            input_ordinal,
            input_fingerprint: Some(input_fingerprint),
            action_range,
            rows: values.clone(),
            counters: if already_recorded {
                state
                    .return_manifest
                    .get(&input_ordinal)
                    .map(|row| row.counters.clone())
                    .unwrap_or_default()
            } else {
                row_counters
            },
        };
        if !already_recorded {
            session.record_return_manifest(manifest.clone()).await.map_err(provider_error)?;
            if state.request.atomicity == hydradb_graph_provider::MutationAtomicityPolicy::LegacyChunked {
                if let Some(actions) = action_range {
                    checkpoint_row(session.as_mut(), &state.request, input_ordinal, actions).await?;
                    state.graph_context.update_mutation_read_view(session.read_view())?;
                    state.property_cache.values.clear();
                }
            }
            state.return_manifest.insert(input_ordinal, manifest);
        }
        return_rows.extend(values);
    }
    if state.program.returning.is_empty() || state.output_schema.fields().is_empty() || return_rows.is_empty() {
        return Ok(None);
    }
    if state.next_action < first_action {
        return Err(DataFusionError::Execution("mutation action ordinal regressed".into()));
    }
    rows_to_batch(&return_rows, &state.output_schema).map(Some)
}

struct ActionCursor<'a> {
    next_action: &'a mut u64,
    next_batch_sequence: &'a mut u64,
    semantics_version: StatementSemanticsVersion,
    counters: &'a Arc<Mutex<MutationCounters>>,
    row_counters: &'a mut MutationCounters,
}

#[derive(Clone)]
enum ResolvedBinding {
    Node(String),
    Relationship {
        name: String,
        source: NodeId,
        destination: NodeId,
        relationship_type: String,
    },
}

enum MergeExpansion {
    Passthrough {
        bindings: BTreeMap<String, CypherValue>,
        created: bool,
    },
    Targets {
        bindings: BTreeMap<String, CypherValue>,
        created: bool,
        targets: Arc<Vec<MutationTarget>>,
        binding: Option<ResolvedBinding>,
    },
}

// These arguments form one mutation-row execution frame. Keeping them explicit
// makes recursive FOREACH evaluation and mutable borrow boundaries auditable.
#[allow(clippy::too_many_arguments)]
fn execute_operation<'a, 'c>(
    session: &'a mut dyn GraphMutationSession,
    operation: &'a PlanOperation,
    bindings: BTreeMap<String, CypherValue>,
    input_ordinal: u64,
    clause_ordinal: u32,
    cursor: &'a mut ActionCursor<'c>,
    property_cache: &'a mut MutationPropertyCache,
    context: &'a GraphExecutionContext,
) -> BoxFuture<'a, datafusion::common::Result<Vec<BTreeMap<String, CypherValue>>>> {
    Box::pin(async move {
        if let PlanOperation::Merge { .. } = operation {
            return execute_merge(
                session,
                operation,
                bindings,
                input_ordinal,
                clause_ordinal,
                cursor,
                property_cache,
                context,
            )
            .await;
        }
        let mut bindings = bindings;
        execute_operation_single(
            session,
            operation,
            &mut bindings,
            input_ordinal,
            clause_ordinal,
            cursor,
            property_cache,
            context,
        )
        .await?;
        Ok(vec![bindings])
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_operation_single<'a, 'c>(
    session: &'a mut dyn GraphMutationSession,
    operation: &'a PlanOperation,
    bindings: &'a mut BTreeMap<String, CypherValue>,
    input_ordinal: u64,
    clause_ordinal: u32,
    cursor: &'a mut ActionCursor<'c>,
    property_cache: &'a mut MutationPropertyCache,
    context: &'a GraphExecutionContext,
) -> BoxFuture<'a, datafusion::common::Result<()>> {
    Box::pin(async move {
        check_mutation_context(context)?;
        match operation {
            PlanOperation::Create { pattern } => {
                for vertex in &pattern.vertices {
                    if let Some(binding) = &vertex.binding {
                        if let Some(existing) = bindings.get(binding.symbol.as_str()) {
                            if as_node_id(existing).is_none() {
                                return Err(provider_error(GraphProviderError::InvalidRequest(
                                    "CREATE pattern vertex binding is not a node".into(),
                                )));
                            }
                            // A variable already bound by an earlier MATCH or
                            // CREATE clause is an endpoint, not a new vertex.
                            continue;
                        }
                    }
                    let mut properties = evaluate_properties_async(&vertex.properties, bindings, property_cache, context, false).await?;
                    let identity = take_node_identity(&mut properties)?;
                    let action = MutationOperation::CreateNode {
                        identity,
                        labels: vertex.labels.iter().map(|label| label.as_str().to_owned()).collect(),
                        properties,
                    };
                    let binding = vertex
                        .binding
                        .as_ref()
                        .map(|binding| ResolvedBinding::Node(binding.symbol.as_str().to_owned()));
                    let _ = emit_action(
                        session,
                        action,
                        binding,
                        bindings,
                        input_ordinal,
                        clause_ordinal,
                        cursor,
                        property_cache,
                        context,
                    )
                    .await?;
                }
                for relationship in &pattern.relationships {
                    let (source, destination, relationship_type) = relationship_endpoints(relationship, bindings)?;
                    let properties = evaluate_properties_async(&relationship.properties, bindings, property_cache, context, false).await?;
                    let action = MutationOperation::CreateRelationship {
                        source,
                        destination,
                        relationship_type: relationship_type.clone(),
                        properties,
                    };
                    let binding = relationship.binding.as_ref().map(|binding| ResolvedBinding::Relationship {
                        name: binding.symbol.as_str().to_owned(),
                        source,
                        destination,
                        relationship_type,
                    });
                    let _ = emit_action(
                        session,
                        action,
                        binding,
                        bindings,
                        input_ordinal,
                        clause_ordinal,
                        cursor,
                        property_cache,
                        context,
                    )
                    .await?;
                }
            }
            PlanOperation::Merge { .. } => unreachable!("MERGE is handled by execute_operation"),
            PlanOperation::Set { updates } => {
                for update in updates {
                    let (target, key) = mutation_property_target_async(&update.target, bindings, property_cache, context).await?;
                    let value = evaluate_expression_async(&update.value, bindings, property_cache, context).await?;
                    let action = match value {
                        CypherValue::Null => MutationOperation::RemoveProperty { target, key },
                        CypherValue::Missing => {
                            return Err(provider_error(GraphProviderError::InvalidRequest(
                                "SET property evaluated to a missing value".into(),
                            )));
                        }
                        value => MutationOperation::SetProperty { target, key, value },
                    };
                    let _ = emit_action(
                        session,
                        action,
                        None,
                        bindings,
                        input_ordinal,
                        clause_ordinal,
                        cursor,
                        property_cache,
                        context,
                    )
                    .await?;
                }
            }
            PlanOperation::Remove { targets } => {
                for target in targets {
                    let (target, key) = mutation_property_target_async(target, bindings, property_cache, context).await?;
                    let _ = emit_action(
                        session,
                        MutationOperation::RemoveProperty { target, key },
                        None,
                        bindings,
                        input_ordinal,
                        clause_ordinal,
                        cursor,
                        property_cache,
                        context,
                    )
                    .await?;
                }
            }
            PlanOperation::Delete { targets, detach } => {
                for target in targets {
                    let value = evaluate_expression_async(target, bindings, property_cache, context).await?;
                    let operation = match value {
                        CypherValue::Node(node) => MutationOperation::DeleteNode { node, detach: *detach },
                        CypherValue::Relationship(relationship) => match relationship.identity() {
                            RelationshipIdentity::Identified(relationship) => MutationOperation::DeleteRelationship {
                                relationship: *relationship,
                            },
                            RelationshipIdentity::Structural { .. } => {
                                return Err(provider_error(GraphProviderError::UnsupportedCapability {
                                    capability: "deleting structural relationships",
                                }));
                            }
                        },
                        CypherValue::Null | CypherValue::Missing => continue,
                        _ => {
                            return Err(provider_error(GraphProviderError::InvalidRequest(
                                "DELETE target is not a node or relationship".into(),
                            )));
                        }
                    };
                    let _ = emit_action(
                        session,
                        operation,
                        None,
                        bindings,
                        input_ordinal,
                        clause_ordinal,
                        cursor,
                        property_cache,
                        context,
                    )
                    .await?;
                }
            }
            PlanOperation::Foreach { variable, list, steps } => {
                let values = match evaluate_expression_async(list, bindings, property_cache, context).await? {
                    CypherValue::List(values) => values,
                    CypherValue::Null | CypherValue::Missing => Vec::new(),
                    _ => {
                        return Err(provider_error(GraphProviderError::InvalidRequest(
                            "FOREACH input must be a list".into(),
                        )));
                    }
                };
                for value in values {
                    check_mutation_context(context)?;
                    let retained_bytes = estimated_binding_map_bytes(bindings)
                        .saturating_add(size_of::<BTreeMap<String, CypherValue>>() as u64)
                        .saturating_add(estimated_binding_entry_bytes(
                            variable.symbol.as_str(),
                            estimated_value_heap_bytes(&value),
                        ));
                    context.budget().consume_retained(1, retained_bytes).map_err(provider_error)?;
                    let mut nested_rows = vec![{
                        let mut nested = bindings.clone();
                        nested.insert(variable.symbol.as_str().to_owned(), value);
                        nested
                    }];
                    for step in steps {
                        let mut next_rows = Vec::new();
                        for nested in nested_rows {
                            next_rows.extend(
                                execute_operation(
                                    session,
                                    step,
                                    nested,
                                    input_ordinal,
                                    clause_ordinal,
                                    cursor,
                                    property_cache,
                                    context,
                                )
                                .await?,
                            );
                        }
                        nested_rows = next_rows;
                    }
                }
            }
        }
        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_merge(
    session: &mut dyn GraphMutationSession,
    operation: &PlanOperation,
    bindings: BTreeMap<String, CypherValue>,
    input_ordinal: u64,
    clause_ordinal: u32,
    cursor: &mut ActionCursor<'_>,
    property_cache: &mut MutationPropertyCache,
    context: &GraphExecutionContext,
) -> datafusion::common::Result<Vec<BTreeMap<String, CypherValue>>> {
    let PlanOperation::Merge {
        pattern,
        on_match,
        on_create,
    } = operation
    else {
        unreachable!("execute_merge receives a MERGE operation")
    };
    let mut rows = vec![(bindings, false)];
    for vertex in &pattern.vertices {
        let mut expansions = reserve_merge_expansions(rows.len(), context)?;
        for (mut bindings, pattern_created) in rows {
            if vertex
                .binding
                .as_ref()
                .is_some_and(|binding| matches!(bindings.get(binding.symbol.as_str()), Some(CypherValue::Node(_))))
            {
                expansions.push(MergeExpansion::Passthrough {
                    bindings,
                    created: pattern_created,
                });
                continue;
            }
            let mut match_properties = evaluate_properties_async(&vertex.properties, &bindings, property_cache, context, true).await?;
            let identity = take_node_identity(&mut match_properties)?;
            let outcome = emit_action(
                session,
                MutationOperation::MergeNode {
                    identity,
                    labels: vertex.labels.iter().map(|label| label.as_str().to_owned()).collect(),
                    match_properties,
                    on_create: MutationProperties::new(),
                    on_match: MutationProperties::new(),
                },
                None,
                &mut bindings,
                input_ordinal,
                clause_ordinal,
                cursor,
                property_cache,
                context,
            )
            .await?
            .ok_or_else(|| {
                provider_error(GraphProviderError::InvalidRequest(
                    "provider omitted the MERGE action outcome".into(),
                ))
            })?;
            let created = outcome.merge_created.ok_or_else(|| {
                provider_error(GraphProviderError::InvalidRequest(
                    "provider omitted whether MERGE created or matched the node".into(),
                ))
            })?;
            if outcome.targets.is_empty() {
                return Err(provider_error(GraphProviderError::InvalidRequest(
                    "provider returned no target for a successful MERGE".into(),
                )));
            }
            if outcome.targets.iter().any(|target| !matches!(target, MutationTarget::Node(_))) {
                return Err(provider_error(GraphProviderError::InvalidRequest(
                    "provider returned a non-node target for node MERGE".into(),
                )));
            }
            let binding = vertex
                .binding
                .as_ref()
                .map(|binding| ResolvedBinding::Node(binding.symbol.as_str().to_owned()));
            expansions.push(MergeExpansion::Targets {
                bindings,
                created: pattern_created || created,
                targets: outcome.targets,
                binding,
            });
        }
        rows = expand_merge_rows(expansions, context)?;
    }
    for relationship in &pattern.relationships {
        let mut expansions = reserve_merge_expansions(rows.len(), context)?;
        for (mut bindings, pattern_created) in rows {
            if relationship
                .binding
                .as_ref()
                .is_some_and(|binding| matches!(bindings.get(binding.symbol.as_str()), Some(CypherValue::Relationship(_))))
            {
                expansions.push(MergeExpansion::Passthrough {
                    bindings,
                    created: pattern_created,
                });
                continue;
            }
            let (source, destination, relationship_type) = relationship_endpoints(relationship, &bindings)?;
            let match_properties = evaluate_properties_async(&relationship.properties, &bindings, property_cache, context, true).await?;
            let outcome = emit_action(
                session,
                MutationOperation::MergeRelationship {
                    source,
                    destination,
                    relationship_type: relationship_type.clone(),
                    match_properties,
                    on_create: MutationProperties::new(),
                    on_match: MutationProperties::new(),
                },
                None,
                &mut bindings,
                input_ordinal,
                clause_ordinal,
                cursor,
                property_cache,
                context,
            )
            .await?
            .ok_or_else(|| {
                provider_error(GraphProviderError::InvalidRequest(
                    "provider omitted the MERGE action outcome".into(),
                ))
            })?;
            let created = outcome.merge_created.ok_or_else(|| {
                provider_error(GraphProviderError::InvalidRequest(
                    "provider omitted whether MERGE created or matched the relationship".into(),
                ))
            })?;
            if outcome.targets.is_empty() {
                return Err(provider_error(GraphProviderError::InvalidRequest(
                    "provider returned no target for a successful MERGE".into(),
                )));
            }
            if outcome
                .targets
                .iter()
                .any(|target| !matches!(target, MutationTarget::Relationship(_)))
            {
                return Err(provider_error(GraphProviderError::InvalidRequest(
                    "provider returned a non-relationship target for relationship MERGE".into(),
                )));
            }
            let binding = relationship.binding.as_ref().map(|binding| ResolvedBinding::Relationship {
                name: binding.symbol.as_str().to_owned(),
                source,
                destination,
                relationship_type,
            });
            expansions.push(MergeExpansion::Targets {
                bindings,
                created: pattern_created || created,
                targets: outcome.targets,
                binding,
            });
        }
        rows = expand_merge_rows(expansions, context)?;
    }
    let output_bytes = estimated_length(rows.len()).saturating_mul(size_of::<BTreeMap<String, CypherValue>>() as u64);
    context.budget().consume_retained_bytes(output_bytes).map_err(provider_error)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(rows.len())
        .map_err(|error| DataFusionError::Execution(format!("failed to reserve MERGE output rows: {error}")))?;
    for (mut bindings, pattern_created) in rows {
        apply_merge_branch(
            session,
            if pattern_created { on_create } else { on_match },
            &mut bindings,
            input_ordinal,
            clause_ordinal,
            cursor,
            property_cache,
            context,
        )
        .await?;
        output.push(bindings);
    }
    Ok(output)
}

fn reserve_merge_expansions(capacity: usize, context: &GraphExecutionContext) -> datafusion::common::Result<Vec<MergeExpansion>> {
    let bytes = estimated_length(capacity).saturating_mul(size_of::<MergeExpansion>() as u64);
    context.budget().consume_retained_bytes(bytes).map_err(provider_error)?;
    let mut expansions = Vec::new();
    expansions
        .try_reserve_exact(capacity)
        .map_err(|error| DataFusionError::Execution(format!("failed to reserve MERGE expansion groups: {error}")))?;
    Ok(expansions)
}

fn expand_merge_rows(
    expansions: Vec<MergeExpansion>,
    context: &GraphExecutionContext,
) -> datafusion::common::Result<Vec<(BTreeMap<String, CypherValue>, bool)>> {
    let output_count = expansions.iter().try_fold(0_usize, |count, expansion| {
        let rows = match expansion {
            MergeExpansion::Passthrough { .. } => 1,
            MergeExpansion::Targets { targets, .. } => targets.len(),
        };
        count
            .checked_add(rows)
            .ok_or_else(|| DataFusionError::Execution("MERGE expansion row count overflow".into()))
    })?;
    let mut retained_bytes = estimated_length(output_count).saturating_mul(size_of::<(BTreeMap<String, CypherValue>, bool)>() as u64);
    for expansion in &expansions {
        if let MergeExpansion::Targets {
            bindings,
            targets,
            binding,
            ..
        } = expansion
        {
            retained_bytes = retained_bytes
                .saturating_add(estimated_length(targets.len().saturating_sub(1)).saturating_mul(estimated_binding_map_bytes(bindings)));
            if let Some(binding) = binding {
                let entry_bytes = match binding {
                    ResolvedBinding::Node(name) => estimated_binding_entry_bytes(name, 0),
                    ResolvedBinding::Relationship {
                        name, relationship_type, ..
                    } => estimated_binding_entry_bytes(name, estimated_string_bytes(relationship_type.len())),
                };
                retained_bytes = retained_bytes.saturating_add(estimated_length(targets.len()).saturating_mul(entry_bytes));
            }
        }
    }
    check_mutation_context(context)?;
    context.budget().consume_retained_bytes(retained_bytes).map_err(provider_error)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(output_count)
        .map_err(|error| DataFusionError::Execution(format!("failed to reserve MERGE binding rows: {error}")))?;

    for expansion in expansions {
        match expansion {
            MergeExpansion::Passthrough { bindings, created } => rows.push((bindings, created)),
            MergeExpansion::Targets {
                bindings,
                created,
                targets,
                binding,
            } => {
                let first_index = rows.len();
                rows.push((BTreeMap::new(), created));
                for target in targets.iter().skip(1).copied() {
                    check_mutation_context(context)?;
                    let mut resolved = bindings.clone();
                    insert_resolved_target(&mut resolved, binding.as_ref(), target)?;
                    rows.push((resolved, created));
                }
                let mut first = bindings;
                insert_resolved_target(&mut first, binding.as_ref(), targets[0])?;
                rows[first_index].0 = first;
            }
        }
    }
    Ok(rows)
}

fn insert_resolved_target(
    bindings: &mut BTreeMap<String, CypherValue>,
    binding: Option<&ResolvedBinding>,
    target: MutationTarget,
) -> datafusion::common::Result<()> {
    if let Some(binding) = binding {
        let (name, value) = resolve_target_binding(binding, target)?;
        bindings.insert(name, value);
    }
    Ok(())
}

fn estimated_binding_map_bytes(bindings: &BTreeMap<String, CypherValue>) -> u64 {
    (size_of::<BTreeMap<String, CypherValue>>() as u64).saturating_add(estimated_map_entries_bytes(bindings))
}

fn estimated_map_entries_bytes(entries: &BTreeMap<String, CypherValue>) -> u64 {
    let dynamic_bytes = entries.iter().fold(0_u64, |total, (key, value)| {
        total
            .saturating_add(estimated_string_bytes(key.len()))
            .saturating_add(estimated_value_heap_bytes(value))
    });
    estimated_btree_node_bytes(entries.len()).saturating_add(dynamic_bytes)
}

fn estimated_binding_entry_bytes(key: &str, value_heap_bytes: u64) -> u64 {
    estimated_btree_node_bytes(1)
        .saturating_add(estimated_string_bytes(key.len()))
        .saturating_add(value_heap_bytes)
}

fn estimated_btree_node_bytes(entry_count: usize) -> u64 {
    if entry_count == 0 {
        return 0;
    }
    // Account for full B-tree nodes, not just occupied key/value slots. This
    // deliberately overestimates partially filled nodes and split allocations.
    const NODE_KEY_CAPACITY: usize = 11;
    const NODE_CHILD_CAPACITY: usize = 12;
    const MINIMUM_KEYS_PER_NODE: usize = 5;
    let node_size = NODE_KEY_CAPACITY
        .saturating_mul(size_of::<(String, CypherValue)>())
        .saturating_add(NODE_CHILD_CAPACITY.saturating_mul(size_of::<usize>()))
        .saturating_add(size_of::<usize>() * 3);
    let node_count = entry_count.saturating_add(MINIMUM_KEYS_PER_NODE - 1) / MINIMUM_KEYS_PER_NODE + 1;
    estimated_length(node_size).saturating_mul(estimated_length(node_count))
}

fn estimated_value_heap_bytes(value: &CypherValue) -> u64 {
    match value {
        CypherValue::String(value) | CypherValue::Decimal(value) => estimated_string_bytes(value.len()),
        CypherValue::List(values) => estimated_buffer_bytes(values.len(), size_of::<CypherValue>()).saturating_add(
            values
                .iter()
                .fold(0_u64, |total, value| total.saturating_add(estimated_value_heap_bytes(value))),
        ),
        CypherValue::Map(values) | CypherValue::Record(values) => estimated_map_entries_bytes(values),
        CypherValue::NodeEntity(node) => estimated_buffer_bytes(node.labels.len(), size_of::<String>())
            .saturating_add(
                node.labels
                    .iter()
                    .fold(0_u64, |total, label| total.saturating_add(estimated_string_bytes(label.len()))),
            )
            .saturating_add(estimated_map_entries_bytes(&node.properties)),
        CypherValue::Relationship(relationship) => estimated_relationship_heap_bytes(relationship),
        CypherValue::RelationshipEntity(relationship) => {
            estimated_relationship_heap_bytes(&relationship.identity).saturating_add(estimated_map_entries_bytes(&relationship.properties))
        }
        CypherValue::Path(path) => estimated_buffer_bytes(path.nodes().len(), size_of::<NodeId>())
            .saturating_add(estimated_buffer_bytes(path.relationships().len(), size_of::<PathRelationship>()))
            .saturating_add(path.relationships().iter().fold(0_u64, |total, relationship| {
                total.saturating_add(estimated_relationship_identity_heap_bytes(relationship.identity()))
            })),
        CypherValue::DateTime(value) => value
            .timezone
            .as_deref()
            .map_or(0, |timezone| estimated_string_bytes(timezone.len())),
        CypherValue::Extension(value) => {
            estimated_string_bytes(value.type_name.len()).saturating_add(estimated_buffer_bytes(value.payload.len(), size_of::<u8>()))
        }
        CypherValue::Bytes(value) => estimated_buffer_bytes(value.len(), size_of::<u8>()),
        CypherValue::Temporal(value) => {
            let text = match value {
                hydradb_graph_types::CypherTemporal::Date { text, .. }
                | hydradb_graph_types::CypherTemporal::LocalTime { text, .. }
                | hydradb_graph_types::CypherTemporal::ZonedTime { text, .. }
                | hydradb_graph_types::CypherTemporal::LocalDateTime { text, .. }
                | hydradb_graph_types::CypherTemporal::ZonedDateTime { text, .. }
                | hydradb_graph_types::CypherTemporal::Duration { text, .. } => text,
            };
            estimated_string_bytes(text.len())
        }
        CypherValue::Null
        | CypherValue::Missing
        | CypherValue::Boolean(_)
        | CypherValue::Integer(_)
        | CypherValue::Float(_)
        | CypherValue::Node(_)
        | CypherValue::Date(_)
        | CypherValue::LocalTime(_)
        | CypherValue::Time(_)
        | CypherValue::LocalDateTime(_)
        | CypherValue::Duration(_)
        | CypherValue::Point(_) => 0,
    }
}

fn estimated_relationship_heap_bytes(relationship: &RelationshipRef) -> u64 {
    estimated_string_bytes(relationship.relationship_type().len())
        .saturating_add(estimated_relationship_identity_heap_bytes(relationship.identity()))
}

fn estimated_relationship_identity_heap_bytes(identity: &RelationshipIdentity) -> u64 {
    match identity {
        RelationshipIdentity::Structural { relationship_type, .. } => estimated_string_bytes(relationship_type.len()),
        RelationshipIdentity::Identified(_) => 0,
    }
}

fn estimated_string_bytes(length: usize) -> u64 {
    if length == 0 {
        0
    } else {
        estimated_length(length).saturating_add(16)
    }
}

fn estimated_buffer_bytes(length: usize, element_size: usize) -> u64 {
    if length == 0 {
        0
    } else {
        estimated_length(length)
            .saturating_mul(estimated_length(element_size))
            .saturating_add(16)
    }
}

fn estimated_length(length: usize) -> u64 {
    u64::try_from(length).unwrap_or(u64::MAX)
}

// An action needs deterministic receipt coordinates and the row overlay state
// that implements read-own-writes.
#[allow(clippy::too_many_arguments)]
async fn emit_action(
    session: &mut dyn GraphMutationSession,
    operation: MutationOperation,
    binding: Option<ResolvedBinding>,
    bindings: &mut BTreeMap<String, CypherValue>,
    input_ordinal: u64,
    clause_ordinal: u32,
    cursor: &mut ActionCursor<'_>,
    property_cache: &mut MutationPropertyCache,
    context: &GraphExecutionContext,
) -> datafusion::common::Result<Option<MutationActionOutcome>> {
    let overlay_operation = operation.clone();
    let binding_for_cache = binding.clone();
    let ordinal = *cursor.next_action;
    *cursor.next_action = ordinal
        .checked_add(1)
        .ok_or_else(|| DataFusionError::Execution("mutation action ordinal overflow".into()))?;
    let range = ActionRange::new(ordinal, ordinal).map_err(provider_error)?;
    let batch = MutationBatch::new(
        *cursor.next_batch_sequence,
        range,
        cursor.semantics_version,
        vec![MutationAction {
            action_ordinal: ordinal,
            clause_ordinal,
            input_ordinal,
            operation,
        }],
    )
    .map_err(provider_error)?;
    *cursor.next_batch_sequence = cursor
        .next_batch_sequence
        .checked_add(1)
        .ok_or_else(|| DataFusionError::Execution("mutation batch sequence overflow".into()))?;
    let applied = session.apply(batch).await.map_err(provider_error)?;
    context.update_mutation_read_view(session.read_view())?;
    if applied.applied_range != range {
        return Err(provider_error(GraphProviderError::InvalidRequest(
            "provider applied a mutation range different from the requested range".into(),
        )));
    }
    if !applied.replayed {
        cursor
            .counters
            .lock()
            .map_err(|_| DataFusionError::Execution("mutation counters were poisoned".into()))?
            .checked_add_assign(&applied.counters)
            .map_err(provider_error)?;
        cursor.row_counters.checked_add_assign(&applied.counters).map_err(provider_error)?;
    }
    let action_outcome = applied
        .action_outcomes
        .into_iter()
        .find(|outcome| outcome.action_ordinal == ordinal);
    if action_outcome
        .as_ref()
        .is_some_and(|outcome| outcome.input_ordinal != input_ordinal)
    {
        return Err(provider_error(GraphProviderError::InvalidRequest(
            "provider returned an action outcome for a different input row".into(),
        )));
    }
    if applied.replayed
        && matches!(
            &overlay_operation,
            MutationOperation::MergeNode { .. } | MutationOperation::MergeRelationship { .. }
        )
    {
        let expansion_rows = action_outcome.as_ref().map_or(0, |outcome| outcome.targets.len().saturating_sub(1));
        context
            .budget()
            .consume_retained(estimated_length(expansion_rows), 0)
            .map_err(provider_error)?;
    }
    let resolved_binding = if let Some(binding) = binding {
        let (name, value) = install_resolved_binding(action_outcome.as_ref(), binding)?;
        bindings.insert(name, value.clone());
        Some(value)
    } else {
        None
    };
    if applied.replayed {
        match &overlay_operation {
            MutationOperation::SetProperty { target, key, .. } | MutationOperation::RemoveProperty { target, key } => {
                // A replayed expression may have been re-evaluated against the
                // post-checkpoint view, so its attempted value is not necessarily
                // the value stored by the completed action.
                property_cache.invalidate(*target, key);
            }
            _ => update_property_cache(
                &overlay_operation,
                binding_for_cache.as_ref(),
                resolved_binding.as_ref(),
                property_cache,
            ),
        }
    } else {
        update_property_cache(
            &overlay_operation,
            binding_for_cache.as_ref(),
            resolved_binding.as_ref(),
            property_cache,
        );
    }
    Ok(action_outcome)
}

fn take_node_identity(properties: &mut MutationProperties) -> datafusion::common::Result<Option<NodeId>> {
    let Some(value) = properties.remove("id") else {
        return Ok(None);
    };
    let CypherValue::Integer(value) = value else {
        return Err(provider_error(GraphProviderError::InvalidRequest(
            "node id property must be an integer".into(),
        )));
    };
    let value = u64::try_from(value).map_err(|_| {
        provider_error(GraphProviderError::InvalidRequest(
            "node id property must be a non-negative 64-bit integer".into(),
        ))
    })?;
    Ok(Some(NodeId(value)))
}

fn update_property_cache(
    operation: &MutationOperation,
    binding: Option<&ResolvedBinding>,
    resolved: Option<&CypherValue>,
    cache: &mut MutationPropertyCache,
) {
    let resolved_target = || match resolved {
        Some(CypherValue::Node(node)) => Some(MutationTarget::Node(*node)),
        Some(CypherValue::Relationship(relationship)) => match relationship.identity() {
            RelationshipIdentity::Identified(id) => Some(MutationTarget::Relationship(*id)),
            RelationshipIdentity::Structural { .. } => None,
        },
        _ => None,
    };
    match operation {
        MutationOperation::CreateNode { properties, .. }
        | MutationOperation::MergeNode {
            match_properties: properties,
            ..
        } => {
            if matches!(binding, Some(ResolvedBinding::Node(_))) {
                if let Some(target) = resolved_target() {
                    for (key, value) in properties {
                        cache.set(target, key.clone(), value.clone());
                    }
                }
            }
        }
        MutationOperation::CreateRelationship { properties, .. }
        | MutationOperation::MergeRelationship {
            match_properties: properties,
            ..
        } => {
            if matches!(binding, Some(ResolvedBinding::Relationship { .. })) {
                if let Some(target) = resolved_target() {
                    for (key, value) in properties {
                        cache.set(target, key.clone(), value.clone());
                    }
                }
            }
        }
        MutationOperation::SetProperty { target, key, value } => cache.set(*target, key.clone(), value.clone()),
        MutationOperation::RemoveProperty { target, key } => cache.set(*target, key.clone(), CypherValue::Null),
        MutationOperation::DeleteNode { node, .. } => cache.remove_entity(MutationTarget::Node(*node)),
        MutationOperation::DeleteRelationship { relationship } => cache.remove_entity(MutationTarget::Relationship(*relationship)),
    }
}

fn install_resolved_binding(
    outcome: Option<&MutationActionOutcome>,
    binding: ResolvedBinding,
) -> datafusion::common::Result<(String, CypherValue)> {
    let outcome = outcome.ok_or_else(|| DataFusionError::Execution("provider omitted the mutation action outcome".into()))?;
    match outcome.targets.as_slice() {
        [target] => resolve_target_binding(&binding, *target),
        [] => Err(DataFusionError::Execution("provider omitted a requested mutation binding".into())),
        _ => Err(provider_error(GraphProviderError::InvalidRequest(
            "multi-target provider outcomes must be expanded before binding installation".into(),
        ))),
    }
}

fn resolve_target_binding(binding: &ResolvedBinding, target: MutationTarget) -> datafusion::common::Result<(String, CypherValue)> {
    match (binding, target) {
        (ResolvedBinding::Node(name), MutationTarget::Node(id)) => Ok((name.clone(), CypherValue::Node(id))),
        (
            ResolvedBinding::Relationship {
                name,
                source,
                destination,
                relationship_type,
            },
            MutationTarget::Relationship(id),
        ) => Ok((
            name.clone(),
            CypherValue::Relationship(RelationshipRef::identified(id, *source, *destination, relationship_type.clone())),
        )),
        _ => Err(provider_error(GraphProviderError::InvalidRequest(
            "provider action outcome target does not match the requested binding".into(),
        ))),
    }
}

async fn checkpoint_row(
    session: &mut dyn GraphMutationSession,
    request: &MutationRequest,
    input_ordinal: u64,
    actions: ActionRange,
) -> datafusion::common::Result<()> {
    let mut digest = Sha256::new();
    digest.update(request.semantics_version.0.to_be_bytes());
    digest.update(input_ordinal.to_be_bytes());
    digest.update(actions.first.to_be_bytes());
    digest.update(actions.last.to_be_bytes());
    let id: [u8; 32] = digest.finalize().into();
    let boundary = ChunkBoundary::new(id, actions);
    let receipt = session.checkpoint(boundary).await.map_err(provider_error)?;
    receipt.validate_for(request).map_err(provider_error)?;
    if receipt.boundary != boundary {
        return Err(provider_error(GraphProviderError::InvalidRequest(
            "provider checkpoint receipt does not match the requested input-row boundary".into(),
        )));
    }
    Ok(())
}

fn relationship_endpoints(
    relationship: &MutationRelationship,
    bindings: &BTreeMap<String, CypherValue>,
) -> datafusion::common::Result<(NodeId, NodeId, String)> {
    let source = relationship
        .from
        .as_ref()
        .and_then(|binding| bindings.get(binding.symbol.as_str()))
        .and_then(as_node_id)
        .ok_or_else(|| DataFusionError::Plan("relationship mutation has no resolved source node".into()))?;
    let destination = relationship
        .to
        .as_ref()
        .and_then(|binding| bindings.get(binding.symbol.as_str()))
        .and_then(as_node_id)
        .ok_or_else(|| DataFusionError::Plan("relationship mutation has no resolved destination node".into()))?;
    let relationship_type = match relationship.types.as_slice() {
        [relationship_type] => relationship_type.as_str().to_owned(),
        [] => {
            return Err(provider_error(GraphProviderError::UnsupportedCapability {
                capability: "untyped relationship mutations",
            }));
        }
        _ => {
            return Err(provider_error(GraphProviderError::UnsupportedCapability {
                capability: "multi-type relationship mutations",
            }));
        }
    };
    let (source, destination) = match relationship.direction {
        PlanDirection::Outgoing | PlanDirection::Undirected => (source, destination),
        PlanDirection::Incoming => (destination, source),
    };
    Ok((source, destination, relationship_type))
}

async fn evaluate_properties_async(
    properties: &[(Symbol, GraphExpression)],
    bindings: &BTreeMap<String, CypherValue>,
    cache: &mut MutationPropertyCache,
    context: &GraphExecutionContext,
    preserve_null: bool,
) -> datafusion::common::Result<MutationProperties> {
    let mut output = MutationProperties::new();
    for (key, expression) in properties {
        let value = evaluate_expression_async(expression, bindings, cache, context).await?;
        match value {
            CypherValue::Missing => {
                return Err(provider_error(GraphProviderError::InvalidRequest(
                    "mutation property evaluated to a missing value".into(),
                )))
            }
            CypherValue::Null if !preserve_null => {}
            value => {
                output.insert(key.as_str().to_owned(), value);
            }
        }
    }
    Ok(output)
}

async fn mutation_property_target_async(
    expression: &GraphExpression,
    bindings: &BTreeMap<String, CypherValue>,
    cache: &mut MutationPropertyCache,
    context: &GraphExecutionContext,
) -> datafusion::common::Result<(MutationTarget, String)> {
    let GraphExpression::Property { entity, property } = expression else {
        return Err(provider_error(GraphProviderError::InvalidRequest(
            "SET and REMOVE targets must be entity properties".into(),
        )));
    };
    let value = evaluate_expression_async(&GraphExpression::Binding(entity.clone()), bindings, cache, context).await?;
    let target = match value {
        CypherValue::Node(node) => MutationTarget::Node(node),
        CypherValue::Relationship(relationship) => match relationship.identity() {
            RelationshipIdentity::Identified(relationship) => MutationTarget::Relationship(*relationship),
            RelationshipIdentity::Structural { .. } => {
                return Err(provider_error(GraphProviderError::UnsupportedCapability {
                    capability: "mutating structural relationship properties",
                }));
            }
        },
        _ => {
            return Err(provider_error(GraphProviderError::InvalidRequest(
                "property target is not a graph entity".into(),
            )))
        }
    };
    Ok((target, property.as_str().to_owned()))
}

// A MERGE branch shares the parent action's deterministic coordinates and row
// overlay. Explicit inputs prevent accidental branch re-evaluation.
#[allow(clippy::too_many_arguments)]
async fn apply_merge_branch(
    session: &mut dyn GraphMutationSession,
    updates: &[hydradb_graph_plan::MutationUpdate],
    bindings: &mut BTreeMap<String, CypherValue>,
    input_ordinal: u64,
    clause_ordinal: u32,
    cursor: &mut ActionCursor<'_>,
    cache: &mut MutationPropertyCache,
    context: &GraphExecutionContext,
) -> datafusion::common::Result<()> {
    for update in updates {
        let (target, key) = mutation_property_target_async(&update.target, bindings, cache, context).await?;
        let value = evaluate_expression_async(&update.value, bindings, cache, context).await?;
        let operation = match value {
            CypherValue::Null => MutationOperation::RemoveProperty { target, key },
            CypherValue::Missing => {
                return Err(provider_error(GraphProviderError::InvalidRequest(
                    "MERGE branch property evaluated to a missing value".into(),
                )));
            }
            value => MutationOperation::SetProperty { target, key, value },
        };
        let _ = emit_action(
            session,
            operation,
            None,
            bindings,
            input_ordinal,
            clause_ordinal,
            cursor,
            cache,
            context,
        )
        .await?;
    }
    Ok(())
}

fn evaluate_expression_async<'a>(
    expression: &'a GraphExpression,
    bindings: &'a BTreeMap<String, CypherValue>,
    cache: &'a mut MutationPropertyCache,
    context: &'a GraphExecutionContext,
) -> BoxFuture<'a, datafusion::common::Result<CypherValue>> {
    Box::pin(async move {
        match expression {
            GraphExpression::Binding(binding) => Ok(bindings.get(binding.symbol.as_str()).cloned().unwrap_or(CypherValue::Null)),
            GraphExpression::Property { entity, property } => {
                let Some(value) = bindings.get(entity.symbol.as_str()) else {
                    return Ok(CypherValue::Null);
                };
                if let CypherValue::Map(map) = value {
                    return Ok(map.get(property.as_str()).cloned().unwrap_or(CypherValue::Null));
                }
                let Some(target) = mutation_target(value) else {
                    return Ok(CypherValue::Null);
                };
                if let Some(value) = cache.get(target, property.as_str()) {
                    return Ok(value.clone());
                }
                let value = fetch_mutation_property(context.read_view(), target, property.as_str()).await?;
                cache.set(target, property.as_str(), value.clone());
                Ok(value)
            }
            GraphExpression::Parameter(parameter) => context
                .parameter(parameter.as_str())
                .cloned()
                .ok_or_else(|| DataFusionError::Plan(format!("mutation parameter ${} was not supplied", parameter.as_str()))),
            GraphExpression::Literal(value) => scalar_value(value),
            GraphExpression::Binary { left, operator, right } => {
                let left = evaluate_expression_async(left, bindings, cache, context).await?;
                let right = evaluate_expression_async(right, bindings, cache, context).await?;
                evaluate_binary(left, *operator, right)
            }
            GraphExpression::Unary { operator, expression } => {
                let value = evaluate_expression_async(expression, bindings, cache, context).await?;
                match operator {
                    UnaryOperator::Not => Ok(truth_value(value).map_or(CypherValue::Null, |value| CypherValue::Boolean(!value))),
                    UnaryOperator::Negate => match value {
                        CypherValue::Integer(value) => value
                            .checked_neg()
                            .map(CypherValue::Integer)
                            .ok_or_else(|| DataFusionError::Execution("integer negation overflow".into())),
                        CypherValue::Float(value) => Ok(CypherValue::Float(-value)),
                        CypherValue::Null | CypherValue::Missing => Ok(CypherValue::Null),
                        _ => Err(DataFusionError::Execution("unary negation requires a number".into())),
                    },
                    UnaryOperator::IsNull => Ok(CypherValue::Boolean(matches!(value, CypherValue::Null | CypherValue::Missing))),
                    UnaryOperator::IsNotNull => Ok(CypherValue::Boolean(!matches!(value, CypherValue::Null | CypherValue::Missing))),
                }
            }
            GraphExpression::Function { name, arguments, distinct } => {
                if *distinct {
                    return Err(provider_error(GraphProviderError::UnsupportedCapability {
                        capability: "DISTINCT mutation expressions",
                    }));
                }
                if name.as_str().eq_ignore_ascii_case("coalesce") {
                    for argument in arguments {
                        let value = evaluate_expression_async(argument, bindings, cache, context).await?;
                        if !matches!(value, CypherValue::Null | CypherValue::Missing) {
                            return Ok(value);
                        }
                    }
                    return Ok(CypherValue::Null);
                }
                let mut values = Vec::with_capacity(arguments.len());
                for argument in arguments {
                    values.push(evaluate_expression_async(argument, bindings, cache, context).await?);
                }
                evaluate_function(name.as_str(), values)
            }
            GraphExpression::List(values) => {
                let mut items = Vec::with_capacity(values.len());
                for value in values {
                    items.push(evaluate_expression_async(value, bindings, cache, context).await?);
                }
                Ok(CypherValue::List(items))
            }
            GraphExpression::Map(values) => {
                let mut map = BTreeMap::new();
                for (key, value) in values {
                    map.insert(
                        key.as_str().to_owned(),
                        evaluate_expression_async(value, bindings, cache, context).await?,
                    );
                }
                Ok(CypherValue::Map(map))
            }
            GraphExpression::Case { branches, fallback } => {
                for (condition, result) in branches {
                    if truth_value(evaluate_expression_async(condition, bindings, cache, context).await?) == Some(true) {
                        return evaluate_expression_async(result, bindings, cache, context).await;
                    }
                }
                match fallback {
                    Some(fallback) => evaluate_expression_async(fallback, bindings, cache, context).await,
                    None => Ok(CypherValue::Null),
                }
            }
        }
    })
}

fn scalar_value(value: &ScalarValue) -> datafusion::common::Result<CypherValue> {
    Ok(match value {
        ScalarValue::Null => CypherValue::Null,
        ScalarValue::Boolean(value) => CypherValue::Boolean(*value),
        ScalarValue::Integer(value) => CypherValue::Integer(*value),
        ScalarValue::Float(value) => CypherValue::Float(
            value
                .parse()
                .map_err(|_| DataFusionError::Plan(format!("invalid floating-point literal `{value}`")))?,
        ),
        ScalarValue::String(value) => CypherValue::String(value.to_string()),
    })
}

fn mutation_target(value: &CypherValue) -> Option<MutationTarget> {
    match value {
        CypherValue::Node(node) => Some(MutationTarget::Node(*node)),
        CypherValue::Relationship(relationship) => match relationship.identity() {
            RelationshipIdentity::Identified(id) => Some(MutationTarget::Relationship(*id)),
            RelationshipIdentity::Structural { .. } => None,
        },
        _ => None,
    }
}

async fn fetch_mutation_property(
    view: Arc<dyn GraphReadView>,
    target: MutationTarget,
    property: &str,
) -> datafusion::common::Result<CypherValue> {
    if property == "id" {
        if let MutationTarget::Node(node) = target {
            return Ok(CypherValue::Integer(i128::from(node.0)));
        }
    }
    let projection = PropertyProjection::keys(vec![Arc::from(property)]);
    match target {
        MutationTarget::Node(node) => {
            let mut batches = view
                .fetch_nodes(NodeFetchRequest {
                    entities: vec![NodeFetchKey {
                        input_ordinal: 0,
                        node_id: node,
                    }],
                    projection,
                    missing: MissingEntityPolicy::Preserve,
                })
                .map_err(provider_error)?;
            if let Some(batch) = batches.try_next().await.map_err(provider_error)? {
                if batch.num_rows() == 0 || !batch.column(2).as_any().downcast_ref::<BooleanArray>().unwrap().value(0) {
                    return Ok(CypherValue::Null);
                }
                return Ok(property_map_value(batch.column(4), 0, property)?.unwrap_or(CypherValue::Null));
            }
        }
        MutationTarget::Relationship(relationship) => {
            let mut batches = view
                .fetch_relationships(RelationshipFetchRequest {
                    entities: vec![RelationshipFetchKey {
                        input_ordinal: 0,
                        relationship_id: relationship,
                    }],
                    projection,
                    missing: MissingEntityPolicy::Preserve,
                })
                .map_err(provider_error)?;
            if let Some(batch) = batches.try_next().await.map_err(provider_error)? {
                if batch.num_rows() == 0 || !batch.column(2).as_any().downcast_ref::<BooleanArray>().unwrap().value(0) {
                    return Ok(CypherValue::Null);
                }
                let relationship = batch.column(1).as_any().downcast_ref::<StructArray>().unwrap();
                let properties = relationship
                    .fields()
                    .iter()
                    .position(|field| field.name() == "properties")
                    .ok_or_else(|| DataFusionError::Execution("relationship binding has no properties field".into()))?;
                return Ok(property_map_value(relationship.column(properties), 0, property)?.unwrap_or(CypherValue::Null));
            }
        }
    }
    Ok(CypherValue::Null)
}

fn property_map_value(array: &ArrayRef, row: usize, key: &str) -> datafusion::common::Result<Option<CypherValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let map = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Execution("entity properties column is not a Map".into()))?;
    let offsets = map.value_offsets();
    let entries = map.entries();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("property map keys are not strings".into()))?;
    for index in offsets[row] as usize..offsets[row + 1] as usize {
        if keys.value(index) == key {
            return value_from_array(entries.column(1).as_ref(), index)
                .map(Some)
                .map_err(|error| DataFusionError::Execution(error.to_string()));
        }
    }
    Ok(None)
}

fn evaluate_binary(left: CypherValue, operator: GraphBinaryOperator, right: CypherValue) -> datafusion::common::Result<CypherValue> {
    use GraphBinaryOperator as Op;
    if matches!(operator, Op::And | Op::Or) {
        let left = truth_value(left);
        let right = truth_value(right);
        return Ok(match operator {
            Op::And if left == Some(false) || right == Some(false) => CypherValue::Boolean(false),
            Op::And if left == Some(true) && right == Some(true) => CypherValue::Boolean(true),
            Op::Or if left == Some(true) || right == Some(true) => CypherValue::Boolean(true),
            Op::Or if left == Some(false) && right == Some(false) => CypherValue::Boolean(false),
            _ => CypherValue::Null,
        });
    }
    if operator == Op::In {
        return match right {
            CypherValue::List(values) => Ok(hydradb_graph_types::semantic_list_membership(&left, &values)
                .map(CypherValue::Boolean)
                .unwrap_or(CypherValue::Null)),
            CypherValue::Null | CypherValue::Missing => Ok(CypherValue::Null),
            _ => Err(DataFusionError::Execution("IN requires a list on the right-hand side".into())),
        };
    }
    if matches!(left, CypherValue::Null | CypherValue::Missing) || matches!(right, CypherValue::Null | CypherValue::Missing) {
        return Ok(CypherValue::Null);
    }
    match operator {
        Op::Equal => Ok(semantic_eq_three_valued(&left, &right)
            .map(CypherValue::Boolean)
            .unwrap_or(CypherValue::Null)),
        Op::NotEqual => Ok(semantic_eq_three_valued(&left, &right)
            .map(|equal| CypherValue::Boolean(!equal))
            .unwrap_or(CypherValue::Null)),
        Op::LessThan => Ok(CypherValue::Boolean(semantic_cmp(&left, &right) == Ordering::Less)),
        Op::LessThanOrEqual => Ok(CypherValue::Boolean(semantic_cmp(&left, &right) != Ordering::Greater)),
        Op::GreaterThan => Ok(CypherValue::Boolean(semantic_cmp(&left, &right) == Ordering::Greater)),
        Op::GreaterThanOrEqual => Ok(CypherValue::Boolean(semantic_cmp(&left, &right) != Ordering::Less)),
        Op::Add => arithmetic(left, right, |a, b| a.checked_add(b), |a, b| a + b),
        Op::Subtract => arithmetic(left, right, |a, b| a.checked_sub(b), |a, b| a - b),
        Op::Multiply => arithmetic(left, right, |a, b| a.checked_mul(b), |a, b| a * b),
        Op::Divide => match (left, right) {
            (CypherValue::Integer(_), CypherValue::Integer(0)) => {
                Err(DataFusionError::Execution("division by zero in mutation expression".into()))
            }
            (CypherValue::Integer(left), CypherValue::Integer(right)) => left
                .checked_div(right)
                .map(CypherValue::Integer)
                .ok_or_else(|| DataFusionError::Execution("integer division overflow".into())),
            (left, right) => {
                let (left, right) = (numeric_float(&left)?, numeric_float(&right)?);
                if right == 0.0 {
                    Err(DataFusionError::Execution("division by zero in mutation expression".into()))
                } else {
                    Ok(CypherValue::Float(left / right))
                }
            }
        },
        Op::In => unreachable!(),
        Op::StartsWith | Op::EndsWith | Op::Contains => {
            let (CypherValue::String(left), CypherValue::String(right)) = (left, right) else {
                return Err(DataFusionError::Execution("string predicate requires string operands".into()));
            };
            let result = match operator {
                Op::StartsWith => left.starts_with(&right),
                Op::EndsWith => left.ends_with(&right),
                Op::Contains => left.contains(&right),
                _ => unreachable!(),
            };
            Ok(CypherValue::Boolean(result))
        }
        Op::And | Op::Or => unreachable!(),
    }
}

fn arithmetic(
    left: CypherValue,
    right: CypherValue,
    integer: impl FnOnce(i128, i128) -> Option<i128>,
    float: impl FnOnce(f64, f64) -> f64,
) -> datafusion::common::Result<CypherValue> {
    match (left, right) {
        (CypherValue::Integer(left), CypherValue::Integer(right)) => integer(left, right)
            .map(CypherValue::Integer)
            .ok_or_else(|| DataFusionError::Execution("integer arithmetic overflow".into())),
        (left, right) => Ok(CypherValue::Float(float(numeric_float(&left)?, numeric_float(&right)?))),
    }
}

fn numeric_float(value: &CypherValue) -> datafusion::common::Result<f64> {
    match value {
        CypherValue::Integer(value) => Ok(*value as f64),
        CypherValue::Float(value) => Ok(*value),
        _ => Err(DataFusionError::Execution("arithmetic requires numeric values".into())),
    }
}

fn truth_value(value: CypherValue) -> Option<bool> {
    match value {
        CypherValue::Boolean(value) => Some(value),
        CypherValue::Null | CypherValue::Missing => None,
        _ => None,
    }
}

fn evaluate_function(name: &str, arguments: Vec<CypherValue>) -> datafusion::common::Result<CypherValue> {
    match name.to_ascii_lowercase().as_str() {
        // `list[index]`, with the same rules the query path uses.
        crate::value_construction::LIST_ELEMENT_FUNCTION => match arguments.as_slice() {
            [list, index] => crate::value_construction::list_element(list, index),
            _ => Err(DataFusionError::Internal("list element access takes a list and an index".into())),
        },
        "tointeger" | "toint" => crate::to_integer::to_integer_graph_value(arguments.first()),
        "tofloat" => match arguments.first() {
            Some(CypherValue::Integer(value)) => Ok(CypherValue::Float(*value as f64)),
            Some(CypherValue::Float(value)) => Ok(CypherValue::Float(*value)),
            Some(CypherValue::String(value)) => value
                .parse::<f64>()
                .map(CypherValue::Float)
                .map_err(|_| DataFusionError::Execution("toFloat conversion failed".into())),
            Some(CypherValue::Null | CypherValue::Missing) | None => Ok(CypherValue::Null),
            _ => Err(DataFusionError::Execution("toFloat conversion failed".into())),
        },
        "tostring" => match arguments.first() {
            Some(CypherValue::String(value)) => Ok(CypherValue::String(value.clone())),
            Some(CypherValue::Integer(value)) => Ok(CypherValue::String(value.to_string())),
            Some(CypherValue::Float(value)) => Ok(CypherValue::String(value.to_string())),
            Some(CypherValue::Boolean(value)) => Ok(CypherValue::String(value.to_string())),
            Some(CypherValue::Null | CypherValue::Missing) | None => Ok(CypherValue::Null),
            _ => Err(provider_error(GraphProviderError::UnsupportedCapability {
                capability: "string conversion for graph entities",
            })),
        },
        "id" => match arguments.first() {
            Some(CypherValue::Node(node)) => Ok(CypherValue::Integer(i128::from(node.0))),
            Some(CypherValue::Relationship(relationship)) => match relationship.identity() {
                RelationshipIdentity::Identified(id) => Ok(CypherValue::Integer(i128::from(id.0))),
                RelationshipIdentity::Structural { .. } => Ok(CypherValue::Null),
            },
            _ => Ok(CypherValue::Null),
        },
        "size" => match arguments.first() {
            Some(CypherValue::String(value)) => Ok(CypherValue::Integer(value.chars().count() as i128)),
            Some(CypherValue::List(values)) => Ok(CypherValue::Integer(values.len() as i128)),
            Some(CypherValue::Map(values)) => Ok(CypherValue::Integer(values.len() as i128)),
            Some(CypherValue::Null | CypherValue::Missing) | None => Ok(CypherValue::Null),
            _ => Err(DataFusionError::Execution("size requires a string, list, or map".into())),
        },
        "date" => match arguments.as_slice() {
            [CypherValue::String(value)] => hydradb_graph_types::DateValue::parse_iso(value)
                .map(CypherValue::Date)
                .ok_or_else(|| DataFusionError::Execution(format!("invalid date `{value}`; expected an ISO 8601 date such as YYYY-MM-DD"))),
            [CypherValue::Date(value)] => Ok(CypherValue::Date(*value)),
            [CypherValue::Null | CypherValue::Missing] => Ok(CypherValue::Null),
            _ => Err(DataFusionError::Execution("date requires one string or date argument".into())),
        },
        _ => Err(provider_error(GraphProviderError::UnsupportedCapability {
            capability: "mutation expression function",
        })),
    }
}

fn decode_input_row(batch: &RecordBatch, row: usize) -> datafusion::common::Result<BTreeMap<String, CypherValue>> {
    let mut values = BTreeMap::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        values.insert(
            field.name().clone(),
            decode_array_value(batch.column(index), row, field.data_type())?,
        );
    }
    Ok(values)
}

fn input_row_fingerprint(bindings: &BTreeMap<String, CypherValue>) -> datafusion::common::Result<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(b"hydradb-cypher-input-row-v1");
    digest.update(u64::try_from(bindings.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (name, value) in bindings {
        let name = name.as_bytes();
        let identity_value = stable_input_value(value);
        let encoded = encode_value_stable(&identity_value).map_err(|error| DataFusionError::Execution(error.to_string()))?;
        digest.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(name);
        digest.update(u64::try_from(encoded.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(encoded);
    }
    Ok(digest.finalize().into())
}

fn stable_input_value(value: &CypherValue) -> CypherValue {
    match value {
        CypherValue::NodeEntity(entity) => CypherValue::Node(entity.id),
        CypherValue::RelationshipEntity(entity) => CypherValue::Relationship(entity.identity.clone()),
        CypherValue::List(values) => CypherValue::List(values.iter().map(stable_input_value).collect()),
        CypherValue::Map(values) => CypherValue::Map(values.iter().map(|(key, value)| (key.clone(), stable_input_value(value))).collect()),
        CypherValue::Record(values) => {
            CypherValue::Record(values.iter().map(|(key, value)| (key.clone(), stable_input_value(value))).collect())
        }
        value => value.clone(),
    }
}

fn decode_array_value(array: &ArrayRef, row: usize, data_type: &DataType) -> datafusion::common::Result<CypherValue> {
    if array.is_null(row) {
        return Ok(CypherValue::Null);
    }
    if data_type == &canonical_value_data_type() {
        return value_from_array(array.as_ref(), row).map_err(|error| DataFusionError::Execution(error.to_string()));
    }
    match data_type {
        DataType::Boolean => Ok(CypherValue::Boolean(
            array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row),
        )),
        DataType::Int64 => Ok(CypherValue::Integer(i128::from(
            array.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
        ))),
        DataType::UInt64 => Ok(CypherValue::Node(NodeId(
            array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row),
        ))),
        DataType::Float64 => Ok(CypherValue::Float(
            array.as_any().downcast_ref::<Float64Array>().unwrap().value(row),
        )),
        DataType::Utf8 => Ok(CypherValue::String(
            array.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_owned(),
        )),
        DataType::List(field) if field.data_type() == &canonical_value_data_type() => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| DataFusionError::Execution("expected list array".into()))?;
            let values = list.value(row);
            let items = (0..values.len())
                .map(|index| value_from_array(values.as_ref(), index).map_err(|error| DataFusionError::Execution(error.to_string())))
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            Ok(CypherValue::List(items))
        }
        DataType::Struct(fields) if field_named(fields, "record_id").is_some() => decode_relationship(array, row),
        DataType::Struct(fields) if field_named(fields, "nodes").is_some() && field_named(fields, "relationships").is_some() => {
            decode_path(array, row)
        }
        DataType::Struct(_) => Err(provider_error(GraphProviderError::UnsupportedCapability {
            capability: "mutation input struct type",
        })),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            let scalar = datafusion::common::ScalarValue::try_from_array(array, row)?;
            Ok(CypherValue::Integer(scalar_to_i128(&scalar)?))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            let scalar = datafusion::common::ScalarValue::try_from_array(array, row)?;
            Ok(CypherValue::Integer(scalar_to_i128(&scalar)?))
        }
        DataType::Float32 => {
            let scalar = datafusion::common::ScalarValue::try_from_array(array, row)?;
            match scalar {
                datafusion::common::ScalarValue::Float32(Some(value)) => Ok(CypherValue::Float(f64::from(value))),
                _ => Ok(CypherValue::Null),
            }
        }
        _ => Err(provider_error(GraphProviderError::UnsupportedCapability {
            capability: "mutation input Arrow value type",
        })),
    }
}

fn scalar_to_i128(value: &datafusion::common::ScalarValue) -> datafusion::common::Result<i128> {
    use datafusion::common::ScalarValue as S;
    match value {
        S::Int8(Some(value)) => Ok(i128::from(*value)),
        S::Int16(Some(value)) => Ok(i128::from(*value)),
        S::Int32(Some(value)) => Ok(i128::from(*value)),
        S::Int64(Some(value)) => Ok(i128::from(*value)),
        S::UInt8(Some(value)) => Ok(i128::from(*value)),
        S::UInt16(Some(value)) => Ok(i128::from(*value)),
        S::UInt32(Some(value)) => Ok(i128::from(*value)),
        S::UInt64(Some(value)) => Ok(i128::from(*value)),
        _ => Err(DataFusionError::Execution("expected an integer Arrow scalar".into())),
    }
}

fn decode_relationship(array: &ArrayRef, row: usize) -> datafusion::common::Result<CypherValue> {
    let relationships = array.as_any().downcast_ref::<StructArray>().unwrap();
    let source = u64_field(relationships, row, "source")?;
    let destination = u64_field(relationships, row, "destination")?;
    let relationship_type = string_field(relationships, row, "relationship_type")?;
    let kind = string_field(relationships, row, "kind")?;
    let record_id = nullable_u64_field(relationships, row, "record_id")?;
    let reference = match (kind.as_str(), record_id) {
        ("structural", _) | (_, None) => RelationshipRef::structural(NodeId(source), NodeId(destination), relationship_type),
        (_, Some(record_id)) => {
            RelationshipRef::identified(RelationshipId(record_id), NodeId(source), NodeId(destination), relationship_type)
        }
    };
    Ok(CypherValue::Relationship(reference))
}

fn decode_path(array: &ArrayRef, row: usize) -> datafusion::common::Result<CypherValue> {
    let path = array.as_any().downcast_ref::<StructArray>().unwrap();
    let nodes_index = field_named(path.fields(), "nodes").unwrap();
    let relationships_index = field_named(path.fields(), "relationships").unwrap();
    let nodes = path
        .column(nodes_index)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Execution("path nodes are not a list".into()))?
        .value(row);
    let nodes = nodes
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFusionError::Execution("path node identifiers are not UInt64".into()))?;
    let node_ids = (0..nodes.len()).map(|index| NodeId(nodes.value(index))).collect::<Vec<_>>();
    let relationships = path
        .column(relationships_index)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Execution("path relationships are not a list".into()))?
        .value(row);
    let relationships = relationships.as_any().downcast_ref::<StructArray>().unwrap();
    let mut path_relationships = Vec::with_capacity(relationships.len());
    for index in 0..relationships.len() {
        let identity = relationships
            .column(field_named(relationships.fields(), "identity").unwrap())
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let kind = identity
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt8Array>()
            .unwrap()
            .value(index);
        let source = nullable_u64_field(identity, index, "source")?;
        let destination = nullable_u64_field(identity, index, "destination")?;
        let relationship_type = nullable_string_field(identity, index, "relationship_type")?.unwrap_or_default();
        let record_id = nullable_u64_field(identity, index, "record_id")?;
        let identity = if kind == 1 {
            RelationshipIdentity::Identified(RelationshipId(
                record_id.ok_or_else(|| DataFusionError::Execution("identified path relationship has no record id".into()))?,
            ))
        } else {
            RelationshipIdentity::Structural {
                source: NodeId(source.ok_or_else(|| DataFusionError::Execution("structural path has no source".into()))?),
                destination: NodeId(destination.ok_or_else(|| DataFusionError::Execution("structural path has no destination".into()))?),
                relationship_type,
            }
        };
        let orientation = relationships
            .column(field_named(relationships.fields(), "orientation").unwrap())
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt8Array>()
            .unwrap()
            .value(index);
        path_relationships.push(PathRelationship::new(
            identity,
            if orientation == 0 {
                TraversalOrientation::Forward
            } else {
                TraversalOrientation::Reverse
            },
        ));
    }
    Path::new(node_ids, path_relationships)
        .map(CypherValue::Path)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

fn field_named(fields: &Fields, name: &str) -> Option<usize> {
    fields.iter().position(|field| field.name() == name)
}

fn u64_field(array: &StructArray, row: usize, name: &str) -> datafusion::common::Result<u64> {
    nullable_u64_field(array, row, name)?.ok_or_else(|| DataFusionError::Execution(format!("relationship field `{name}` is null")))
}

fn nullable_u64_field(array: &StructArray, row: usize, name: &str) -> datafusion::common::Result<Option<u64>> {
    let index = field_named(array.fields(), name).ok_or_else(|| DataFusionError::Execution(format!("missing UInt64 field `{name}`")))?;
    let values = array
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("field `{name}` is not UInt64")))?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

fn string_field(array: &StructArray, row: usize, name: &str) -> datafusion::common::Result<String> {
    nullable_string_field(array, row, name)?.ok_or_else(|| DataFusionError::Execution(format!("relationship field `{name}` is null")))
}

fn nullable_string_field(array: &StructArray, row: usize, name: &str) -> datafusion::common::Result<Option<String>> {
    let index = field_named(array.fields(), name).ok_or_else(|| DataFusionError::Execution(format!("missing string field `{name}`")))?;
    let values = array
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution(format!("field `{name}` is not Utf8")))?;
    Ok((!values.is_null(row)).then(|| values.value(row).to_owned()))
}

fn materialize_mutation_result_value(
    value: CypherValue,
    read_view: Arc<dyn GraphReadView>,
    input_read_view: Arc<dyn GraphReadView>,
) -> BoxFuture<'static, datafusion::common::Result<CypherValue>> {
    Box::pin(async move {
        Ok(match value {
            CypherValue::Node(id) => {
                let entity = fetch_node_entity(Arc::clone(&read_view), id)
                    .await?
                    .or(fetch_node_entity(input_read_view, id).await?);
                CypherValue::NodeEntity(entity.unwrap_or(NodeEntity {
                    id,
                    labels: Vec::new(),
                    properties: BTreeMap::new(),
                }))
            }
            CypherValue::Relationship(identity) => match identity.identity() {
                RelationshipIdentity::Identified(id) => {
                    let entity = fetch_relationship_entity(Arc::clone(&read_view), *id)
                        .await?
                        .or(fetch_relationship_entity(input_read_view, *id).await?);
                    CypherValue::RelationshipEntity(entity.unwrap_or(RelationshipEntity {
                        identity,
                        properties: BTreeMap::new(),
                    }))
                }
                RelationshipIdentity::Structural { .. } => CypherValue::RelationshipEntity(RelationshipEntity {
                    identity,
                    properties: BTreeMap::new(),
                }),
            },
            CypherValue::List(values) => {
                let mut entities = Vec::with_capacity(values.len());
                for value in values {
                    entities.push(materialize_mutation_result_value(value, Arc::clone(&read_view), Arc::clone(&input_read_view)).await?);
                }
                CypherValue::List(entities)
            }
            CypherValue::Map(values) => {
                let mut entities = BTreeMap::new();
                for (key, value) in values {
                    entities.insert(
                        key,
                        materialize_mutation_result_value(value, Arc::clone(&read_view), Arc::clone(&input_read_view)).await?,
                    );
                }
                CypherValue::Map(entities)
            }
            other => other,
        })
    })
}

async fn fetch_node_entity(read_view: Arc<dyn GraphReadView>, id: NodeId) -> datafusion::common::Result<Option<NodeEntity>> {
    let request = NodeFetchRequest {
        entities: vec![NodeFetchKey {
            input_ordinal: 0,
            node_id: id,
        }],
        projection: PropertyProjection {
            include_labels: true,
            ..PropertyProjection::all()
        },
        missing: MissingEntityPolicy::Preserve,
    };
    let mut batches = read_view.fetch_nodes(request).map_err(provider_error)?;
    while let Some(batch) = batches.try_next().await.map_err(provider_error)? {
        let exists_index = batch
            .schema()
            .index_of("exists")
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let exists = batch
            .column(exists_index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Execution("node fetch existence values are not Boolean".into()))?;
        let Some(row) = (0..batch.num_rows()).find(|row| exists.value(*row)) else {
            continue;
        };
        let id_index = batch
            .schema()
            .index_of("node_id")
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let ids = batch
            .column(id_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Execution("fetched node identifiers are not UInt64".into()))?;
        if ids.is_null(row) || ids.value(row) != id.0 {
            return Err(DataFusionError::Execution("provider returned a mismatched node identity".into()));
        }
        let labels_index = batch
            .schema()
            .index_of("labels")
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let labels = batch
            .column(labels_index)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("node labels are not a list".into()))?;
        let label_values = labels.value(row);
        let labels = label_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("node label values are not strings".into()))?;
        let labels = (0..labels.len()).map(|index| labels.value(index).to_owned()).collect();
        let properties_index = batch
            .schema()
            .index_of("properties")
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let properties = property_map_all(batch.column(properties_index), row)?;
        return Ok(Some(NodeEntity { id, labels, properties }));
    }
    Ok(None)
}

async fn fetch_relationship_entity(
    read_view: Arc<dyn GraphReadView>,
    id: RelationshipId,
) -> datafusion::common::Result<Option<RelationshipEntity>> {
    let request = RelationshipFetchRequest {
        entities: vec![RelationshipFetchKey {
            input_ordinal: 0,
            relationship_id: id,
        }],
        projection: PropertyProjection::all(),
        missing: MissingEntityPolicy::Preserve,
    };
    let mut batches = read_view.fetch_relationships(request).map_err(provider_error)?;
    while let Some(batch) = batches.try_next().await.map_err(provider_error)? {
        let exists_index = batch
            .schema()
            .index_of("exists")
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let exists = batch
            .column(exists_index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Execution("relationship fetch existence values are not Boolean".into()))?;
        let Some(row) = (0..batch.num_rows()).find(|row| exists.value(*row)) else {
            continue;
        };
        let relationship_index = batch
            .schema()
            .index_of("relationship")
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let relationship_array = batch
            .column(relationship_index)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("fetched relationship is not a struct".into()))?;
        let CypherValue::Relationship(identity) = decode_relationship(batch.column(relationship_index), row)? else {
            return Err(DataFusionError::Execution("provider returned a non-relationship binding".into()));
        };
        if identity.identity() != &RelationshipIdentity::Identified(id) {
            return Err(DataFusionError::Execution(
                "provider returned a mismatched relationship identity".into(),
            ));
        }
        let properties_index = field_named(relationship_array.fields(), "properties")
            .ok_or_else(|| DataFusionError::Execution("relationship binding has no properties".into()))?;
        let properties = property_map_all(relationship_array.column(properties_index), row)?;
        return Ok(Some(RelationshipEntity { identity, properties }));
    }
    Ok(None)
}

fn property_map_all(array: &ArrayRef, row: usize) -> datafusion::common::Result<BTreeMap<String, CypherValue>> {
    if array.is_null(row) {
        return Ok(BTreeMap::new());
    }
    let map = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Execution("entity properties column is not a Map".into()))?;
    let offsets = map.value_offsets();
    let entries = map.entries();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("property map keys are not strings".into()))?;
    let mut properties = BTreeMap::new();
    for index in offsets[row] as usize..offsets[row + 1] as usize {
        properties.insert(
            keys.value(index).to_owned(),
            value_from_array(entries.column(1).as_ref(), index).map_err(|error| DataFusionError::Execution(error.to_string()))?,
        );
    }
    Ok(properties)
}

async fn evaluate_returning(
    projections: &[Projection],
    bindings: &BTreeMap<String, CypherValue>,
    cache: &mut MutationPropertyCache,
    context: &GraphExecutionContext,
) -> datafusion::common::Result<Vec<CypherValue>> {
    let mut values = Vec::new();
    for projection in projections {
        values.push(evaluate_expression_async(&projection.expression, bindings, cache, context).await?);
    }
    Ok(values)
}

fn rows_to_batch(rows: &[Vec<CypherValue>], schema: &SchemaRef) -> datafusion::common::Result<RecordBatch> {
    if rows.iter().any(|row| row.len() != schema.fields().len()) {
        return Err(DataFusionError::Plan(
            "mutation RETURN projections do not match output schema".into(),
        ));
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let values = rows.iter().map(|row| row[index].clone()).collect::<Vec<_>>();
        columns.push(values_to_array(&values, field)?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(Into::into)
}

fn values_to_array(values: &[CypherValue], field: &Field) -> datafusion::common::Result<ArrayRef> {
    let null = |value: &CypherValue| matches!(value, CypherValue::Null | CypherValue::Missing);
    let mismatch = |value: &CypherValue| {
        DataFusionError::Execution(format!(
            "RETURN value {value:?} does not match declared Arrow type {}",
            field.data_type()
        ))
    };
    Ok(match field.data_type() {
        data_type if data_type == &canonical_value_data_type() => {
            Arc::new(value_array(values).map_err(|error| DataFusionError::Execution(error.to_string()))?) as ArrayRef
        }
        DataType::UInt64 => Arc::new(UInt64Array::from(
            values
                .iter()
                .map(|value| match value {
                    CypherValue::Null | CypherValue::Missing => Ok(None),
                    CypherValue::Node(id) => Ok(Some(id.0)),
                    CypherValue::NodeEntity(entity) => Ok(Some(entity.id.0)),
                    CypherValue::Integer(id) => u64::try_from(*id)
                        .map(Some)
                        .map_err(|_| DataFusionError::Execution("RETURN integer does not fit UInt64".into())),
                    _ => Err(mismatch(value)),
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    CypherValue::Null | CypherValue::Missing => Ok(None),
                    CypherValue::Integer(value) => i64::try_from(*value)
                        .map(Some)
                        .map_err(|_| DataFusionError::Execution("RETURN integer does not fit Int64".into())),
                    CypherValue::Node(value) => i64::try_from(value.0)
                        .map(Some)
                        .map_err(|_| DataFusionError::Execution("RETURN node id does not fit Int64".into())),
                    CypherValue::NodeEntity(value) => i64::try_from(value.id.0)
                        .map(Some)
                        .map_err(|_| DataFusionError::Execution("RETURN node id does not fit Int64".into())),
                    _ => Err(mismatch(value)),
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        )),
        DataType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| match value {
                    CypherValue::Null | CypherValue::Missing => Ok(None),
                    CypherValue::Boolean(value) => Ok(Some(*value)),
                    _ => Err(mismatch(value)),
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    CypherValue::Null | CypherValue::Missing => Ok(None),
                    CypherValue::Float(value) => Ok(Some(*value)),
                    CypherValue::Integer(value) => Ok(Some(*value as f64)),
                    _ => Err(mismatch(value)),
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        )),
        DataType::Float32 => Arc::new(Float32Array::from(
            values
                .iter()
                .map(|value| match value {
                    CypherValue::Null | CypherValue::Missing => Ok(None),
                    CypherValue::Float(value) => Ok(Some(*value as f32)),
                    CypherValue::Integer(value) => Ok(Some(*value as f32)),
                    _ => Err(mismatch(value)),
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        )),
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| match value {
                    CypherValue::Null | CypherValue::Missing => Ok(None),
                    CypherValue::String(value) => Ok(Some(value.as_str())),
                    _ => Err(mismatch(value)),
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        )),
        DataType::Decimal128(precision, scale) => {
            let raw = values
                .iter()
                .map(|value| match value {
                    CypherValue::Null | CypherValue::Missing => Ok(None),
                    CypherValue::Integer(value) if *scale == 0 => Ok(Some(*value)),
                    CypherValue::Integer(value) if *scale > 0 => 10_i128
                        .checked_pow(*scale as u32)
                        .and_then(|scale| value.checked_mul(scale))
                        .map(Some)
                        .ok_or_else(|| DataFusionError::Execution("RETURN decimal value overflows".into())),
                    _ => Err(mismatch(value)),
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            Arc::new(
                Decimal128Array::from(raw)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?,
            )
        }
        DataType::List(item_field) if item_field.data_type() == &canonical_value_data_type() => {
            let mut offsets = Vec::with_capacity(values.len() + 1);
            let mut flattened = Vec::new();
            let mut validity = Vec::with_capacity(values.len());
            offsets.push(0_i32);
            for value in values {
                match value {
                    CypherValue::List(items) => {
                        validity.push(true);
                        flattened.extend(items.iter().cloned());
                    }
                    CypherValue::Null | CypherValue::Missing => validity.push(false),
                    _ => return Err(DataFusionError::Execution("RETURN list column contains a non-list value".into())),
                }
                offsets.push(i32::try_from(flattened.len()).map_err(|_| DataFusionError::Execution("RETURN list is too large".into()))?);
            }
            let values = Arc::new(value_array(&flattened).map_err(|error| DataFusionError::Execution(error.to_string()))?) as ArrayRef;
            Arc::new(ListArray::try_new(
                Arc::clone(item_field),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                Some(validity.into()),
            )?) as ArrayRef
        }
        DataType::Struct(fields) if field_named(fields, "record_id").is_some() => relationship_array(values, fields)?,
        DataType::Struct(_) => {
            return Err(provider_error(GraphProviderError::UnsupportedCapability {
                capability: "mutation RETURN struct encoding",
            }));
        }
        _ => {
            if values.iter().all(null) {
                datafusion::arrow::array::new_null_array(field.data_type(), values.len())
            } else {
                return Err(provider_error(GraphProviderError::UnsupportedCapability {
                    capability: "mutation RETURN Arrow type",
                }));
            }
        }
    })
}

fn relationship_array(values: &[CypherValue], fields: &Fields) -> datafusion::common::Result<ArrayRef> {
    let mut sources = Vec::with_capacity(values.len());
    let mut destinations = Vec::with_capacity(values.len());
    let mut types = Vec::with_capacity(values.len());
    let mut kinds = Vec::with_capacity(values.len());
    let mut ids = Vec::with_capacity(values.len());
    let mut validity = Vec::with_capacity(values.len());
    for value in values {
        match value {
            CypherValue::Relationship(relationship) => {
                sources.push(Some(relationship.source().0));
                destinations.push(Some(relationship.destination().0));
                types.push(Some(relationship.relationship_type()));
                match relationship.identity() {
                    RelationshipIdentity::Structural { .. } => {
                        kinds.push(Some("structural"));
                        ids.push(None);
                    }
                    RelationshipIdentity::Identified(id) => {
                        kinds.push(Some("identified"));
                        ids.push(Some(id.0));
                    }
                }
                validity.push(true);
            }
            CypherValue::RelationshipEntity(entity) => {
                let relationship = &entity.identity;
                sources.push(Some(relationship.source().0));
                destinations.push(Some(relationship.destination().0));
                types.push(Some(relationship.relationship_type()));
                match relationship.identity() {
                    RelationshipIdentity::Structural { .. } => {
                        kinds.push(Some("structural"));
                        ids.push(None);
                    }
                    RelationshipIdentity::Identified(id) => {
                        kinds.push(Some("identified"));
                        ids.push(Some(id.0));
                    }
                }
                validity.push(true);
            }
            CypherValue::Null | CypherValue::Missing => {
                sources.push(None);
                destinations.push(None);
                types.push(None);
                kinds.push(None);
                ids.push(None);
                validity.push(false);
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "RETURN relationship column contains a non-relationship value".into(),
                ))
            }
        }
    }
    let source = field_named(fields, "source").ok_or_else(|| DataFusionError::Execution("relationship schema has no source".into()))?;
    let destination =
        field_named(fields, "destination").ok_or_else(|| DataFusionError::Execution("relationship schema has no destination".into()))?;
    let relationship_type =
        field_named(fields, "relationship_type").ok_or_else(|| DataFusionError::Execution("relationship schema has no type".into()))?;
    let kind = field_named(fields, "kind").ok_or_else(|| DataFusionError::Execution("relationship schema has no kind".into()))?;
    let record_id =
        field_named(fields, "record_id").ok_or_else(|| DataFusionError::Execution("relationship schema has no record_id".into()))?;
    let properties =
        field_named(fields, "properties").ok_or_else(|| DataFusionError::Execution("relationship schema has no properties".into()))?;
    let properties = empty_property_maps(values.len(), fields[properties].data_type())?;
    let arrays = vec![
        Arc::new(UInt64Array::from(sources)) as ArrayRef,
        Arc::new(UInt64Array::from(destinations)) as ArrayRef,
        Arc::new(StringArray::from(types)) as ArrayRef,
        Arc::new(StringArray::from(kinds)) as ArrayRef,
        Arc::new(UInt64Array::from(ids)) as ArrayRef,
        properties,
    ];
    let array = StructArray::try_new(fields.clone(), arrays, Some(validity.into()))?;
    let _ = (source, destination, relationship_type, kind, record_id);
    Ok(Arc::new(array))
}

fn empty_property_maps(rows: usize, data_type: &DataType) -> datafusion::common::Result<ArrayRef> {
    let DataType::Map(entry_field, ordered) = data_type else {
        return Err(DataFusionError::Execution("relationship properties field is not a map".into()));
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        return Err(DataFusionError::Execution("property map entries are not a struct".into()));
    };
    let keys = Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef;
    let values = Arc::new(value_array(&[]).map_err(|error| DataFusionError::Execution(error.to_string()))?) as ArrayRef;
    let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)?;
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32; rows + 1]));
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(entry_field),
        offsets,
        entries,
        None,
        *ordered,
    )?))
}

fn as_node_id(value: &CypherValue) -> Option<NodeId> {
    match value {
        CypherValue::Node(id) => Some(*id),
        _ => None,
    }
}
