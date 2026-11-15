//! Query-scoped graph execution state and bounded stream utilities.
//!
//! The context pins the provider view and carries request budgets, parameters,
//! and mutation ownership through DataFusion's cloned task contexts. Physical
//! plans never capture provider state, so plan reuse cannot reuse a stale
//! snapshot or transaction.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Instant;

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::future::BoxFuture;
use futures::{stream, StreamExt, TryStreamExt};
use hydradb_graph_plan::MutationProgram;
use hydradb_graph_provider::{
    BudgetLimits, BudgetTracker, CancellationToken, CommitReceipt, GraphCapabilities, GraphMutationSession, GraphProviderError,
    GraphReadView, MutationAtomicityPolicy, MutationCounters, MutationRequest, ReadOperation, ReadRequest, ReadRequirements,
};
use hydradb_graph_types::CypherValue;

use crate::logical::GraphLogicalNode;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;

type MutationResultValidator = Arc<dyn Fn(&RecordBatch, &[Vec<CypherValue>]) -> Result<()> + Send + Sync>;

/// One execution owner for a non-duplicable mutation effect.
///
/// The lease is shared by every clone/rewrite of a physical mutation plan.
/// Exactly one partition may claim it. Repeated executions, including replay
/// by a DataFusion scheduler, fail closed rather than repeating writes.
#[derive(Debug, Default)]
pub struct MutationExecutionLease {
    claimed: AtomicBool,
}

impl MutationExecutionLease {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn claim(self: &Arc<Self>) -> Result<MutationExecutionPermit> {
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                DataFusionError::Execution("mutation execution lease was already claimed; effectful plans are single-owner".into())
            })?;
        Ok(MutationExecutionPermit { lease: Arc::clone(self) })
    }

    pub fn is_claimed(&self) -> bool {
        self.claimed.load(Ordering::Acquire)
    }
}

/// Unique proof that this task owns the query's mutation effect.
#[derive(Debug)]
pub struct MutationExecutionPermit {
    lease: Arc<MutationExecutionLease>,
}

impl MutationExecutionPermit {
    pub fn lease(&self) -> &Arc<MutationExecutionLease> {
        &self.lease
    }
}

/// Runtime bridge for non-mutation graph operators.
///
/// The callback receives physical children rather than pre-started streams so
/// correlated apply implementations can execute the inner branch once per
/// outer binding. Its query context always contains the same pinned read view.
pub trait GraphOperatorRuntime: Send + Sync {
    fn execute(
        &self,
        node: Arc<GraphLogicalNode>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        partition: usize,
        task_context: Arc<TaskContext>,
        graph_context: Arc<GraphExecutionContext>,
    ) -> Result<SendableRecordBatchStream>;
}

/// Adapter from the bound graph-plan mutation IR and input rows to the
/// provider's ordered MutationBatch/apply/checkpoint/commit protocol.
///
/// The runtime stages mutations and returns RETURN batches together with a
/// deferred finalizer. It must not commit when its result stream reaches EOF:
/// `MutationSink` still has to validate the complete output against query
/// budgets first. Errors, cancellation, or dropping the sink stream must make
/// commit impossible and abort staged effects.
pub trait MutationProgramRuntime: Send + Sync {
    fn execute(
        &self,
        program: MutationProgram,
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        session: Box<dyn GraphMutationSession>,
        permit: MutationExecutionPermit,
        graph_context: Arc<GraphExecutionContext>,
    ) -> Result<MutationProgramOutput>;
}

/// Mutation RETURN data and the deferred transaction decision associated
/// with it. The physical `MutationSink` drains and validates `stream` before
/// invoking `finalizer.commit()`.
pub struct MutationProgramOutput {
    pub stream: SendableRecordBatchStream,
    pub finalizer: Box<dyn MutationExecutionFinalizer>,
    /// Updated as actions are applied and readable after the sink drains.
    pub counters: Arc<Mutex<MutationCounters>>,
}

/// Deferred commit/abort control for one mutation session.
///
/// Implementations must keep all effects staged until `commit` is called.
/// `cancel` must synchronously signal that the transaction is no longer
/// eligible to commit; dropping an unfinalized implementation must also abort
/// or arrange equivalent cancellation cleanup.
pub trait MutationExecutionFinalizer: Send {
    fn cancel(&self);
    fn commit(self: Box<Self>) -> BoxFuture<'static, Result<CommitReceipt>>;
    fn abort(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
}

/// Runtime state bound to one query execution and one retained graph snapshot.
pub struct GraphExecutionContext {
    read_view: RwLock<Arc<dyn GraphReadView>>,
    input_read_view: Arc<dyn GraphReadView>,
    capabilities: GraphCapabilities,
    read_request: Option<ReadRequest>,
    mutation_request: Option<MutationRequest>,
    budget: BudgetTracker,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    parameters: std::collections::HashMap<Arc<str>, CypherValue>,
    correlations: CorrelationScopes,
    mutation_session: Mutex<Option<Box<dyn GraphMutationSession>>>,
    mutation_finalizer: Mutex<Option<Box<dyn MutationExecutionFinalizer>>>,
    mutation_counters: Mutex<Option<Arc<Mutex<MutationCounters>>>>,
    mutation_commit_receipt: Mutex<Option<CommitReceipt>>,
    mutation_result_validator: RwLock<Option<MutationResultValidator>>,
    mutation_result_rows_prevalidated: Mutex<u64>,
    mutation_lease: Arc<MutationExecutionLease>,
    operator_runtime: Option<Arc<dyn GraphOperatorRuntime>>,
    mutation_runtime: Option<Arc<dyn MutationProgramRuntime>>,
}

impl fmt::Debug for GraphExecutionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphExecutionContext")
            .field("snapshot", &self.snapshot_identity())
            .field("budget", &self.budget.snapshot())
            .field(
                "mutation_session",
                &self.mutation_session.lock().map(|s| s.is_some()).unwrap_or(false),
            )
            .field("mutation_claimed", &self.mutation_lease.is_claimed())
            .finish_non_exhaustive()
    }
}

impl GraphExecutionContext {
    /// Create a read-only query context after the caller has opened a retained
    /// view. Capability and scope validation happen before the plan executes.
    pub fn for_read(
        read_view: Arc<dyn GraphReadView>,
        capabilities: GraphCapabilities,
        request: &ReadRequest,
        parameters: std::collections::HashMap<Arc<str>, CypherValue>,
        operator_runtime: Arc<dyn GraphOperatorRuntime>,
    ) -> Result<Self> {
        capabilities.validate_read(request).map_err(provider_error)?;
        if read_view.snapshot_identity().scope != request.scope {
            return Err(DataFusionError::Plan(
                "graph read view is pinned to a different graph scope than its request".into(),
            ));
        }
        Ok(Self {
            read_view: RwLock::new(Arc::clone(&read_view)),
            input_read_view: read_view,
            capabilities,
            read_request: Some(request.clone()),
            mutation_request: None,
            budget: request.budget.tracker(),
            cancellation: request.cancellation.clone(),
            deadline: request.deadline,
            parameters,
            correlations: CorrelationScopes::default(),
            mutation_session: Mutex::new(None),
            mutation_finalizer: Mutex::new(None),
            mutation_counters: Mutex::new(None),
            mutation_commit_receipt: Mutex::new(None),
            mutation_result_validator: RwLock::new(None),
            mutation_result_rows_prevalidated: Mutex::new(0),
            mutation_lease: Arc::new(MutationExecutionLease::new()),
            operator_runtime: Some(operator_runtime),
            mutation_runtime: None,
        })
    }

    /// Create a write-query context from its already admitted mutation
    /// session. Call `GraphCapabilities::validate_mutation` before opening the
    /// session; validation here is repeated defensively.
    pub fn for_mutation(
        session: Box<dyn GraphMutationSession>,
        capabilities: GraphCapabilities,
        request: &MutationRequest,
        parameters: std::collections::HashMap<Arc<str>, CypherValue>,
        operator_runtime: Arc<dyn GraphOperatorRuntime>,
        mutation_runtime: Arc<dyn MutationProgramRuntime>,
    ) -> Result<Self> {
        capabilities.validate_mutation(request).map_err(provider_error)?;
        let budget = match session.budget_tracker() {
            Some(budget) if budget.limits() != request.budget => {
                return Err(DataFusionError::Plan(
                    "mutation session budget tracker does not match its request".into(),
                ));
            }
            Some(budget) => budget,
            None => request.budget.tracker(),
        };
        let read_request = ReadRequest {
            scope: request.scope.clone(),
            causal_lower_bound: None,
            deadline: request.deadline,
            cancellation: request.cancellation.clone(),
            budget: request.budget,
            max_io_concurrency: 1,
            requirements: ReadRequirements {
                retained_snapshot: true,
                read_own_writes: true,
                ..ReadRequirements::default()
            },
        };
        capabilities.validate_read(&read_request).map_err(provider_error)?;
        let input_read_view = session.input_read_view();
        let read_view = session.read_view();
        if read_view.snapshot_identity().scope != request.scope {
            return Err(DataFusionError::Plan(
                "mutation read view is pinned to a different graph scope than its request".into(),
            ));
        }
        if input_read_view.snapshot_identity().scope != request.scope {
            return Err(DataFusionError::Plan(
                "mutation input view is pinned to a different graph scope than its request".into(),
            ));
        }
        Ok(Self {
            read_view: RwLock::new(read_view),
            input_read_view,
            capabilities,
            read_request: Some(read_request),
            mutation_request: Some(request.clone()),
            budget,
            cancellation: request.cancellation.clone(),
            deadline: request.deadline,
            parameters,
            correlations: CorrelationScopes::default(),
            mutation_session: Mutex::new(Some(session)),
            mutation_finalizer: Mutex::new(None),
            mutation_counters: Mutex::new(None),
            mutation_commit_receipt: Mutex::new(None),
            mutation_result_validator: RwLock::new(None),
            mutation_result_rows_prevalidated: Mutex::new(0),
            mutation_lease: Arc::new(MutationExecutionLease::new()),
            operator_runtime: Some(operator_runtime),
            mutation_runtime: Some(mutation_runtime),
        })
    }

    pub fn read_view(&self) -> Arc<dyn GraphReadView> {
        self.read_view.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Return the immutable view that supplies the statement's input rows.
    /// Unlike `read_view`, this does not advance when mutation checkpoints
    /// replace the session's read-own-writes overlay.
    pub fn input_read_view(&self) -> Arc<dyn GraphReadView> {
        Arc::clone(&self.input_read_view)
    }

    pub fn snapshot_identity(&self) -> hydradb_graph_provider::SnapshotIdentity {
        self.read_view().snapshot_identity().clone()
    }

    /// Replace the live mutation read view after a provider action or
    /// checkpoint, keeping the context handle stable for all graph operators.
    pub fn update_mutation_read_view(&self, read_view: Arc<dyn GraphReadView>) -> Result<()> {
        let request = self.mutation_request()?;
        if read_view.snapshot_identity().scope != request.scope {
            return Err(DataFusionError::Plan(
                "updated mutation read view is pinned to a different graph scope".into(),
            ));
        }
        *self
            .read_view
            .write()
            .map_err(|_| DataFusionError::Execution("graph read view lock was poisoned".into()))? = read_view;
        Ok(())
    }

    /// Install a query-specific validator for each fully materialized mutation
    /// RETURN batch. The MutationSink invokes it after enforcing Arrow budgets
    /// and before committing provider effects.
    pub fn set_mutation_result_validator(&self, validator: MutationResultValidator) -> Result<()> {
        self.mutation_request()?;
        let mut slot = self
            .mutation_result_validator
            .write()
            .map_err(|_| DataFusionError::Execution("mutation result validator lock was poisoned".into()))?;
        if slot.is_some() {
            return Err(DataFusionError::Execution("mutation result validator is already installed".into()));
        }
        *slot = Some(validator);
        Ok(())
    }

    pub fn validate_mutation_result(&self, batch: &RecordBatch) -> Result<()> {
        let Some(validator) = self.mutation_result_validator()? else {
            return Ok(());
        };
        let mut prevalidated = self
            .mutation_result_rows_prevalidated
            .lock()
            .map_err(|_| DataFusionError::Execution("prevalidated mutation row counter was poisoned".into()))?;
        let skipped = (*prevalidated).min(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        *prevalidated -= skipped;
        drop(prevalidated);
        let start = usize::try_from(skipped).unwrap_or(usize::MAX).min(batch.num_rows());
        if start < batch.num_rows() {
            validator(&batch.slice(start, batch.num_rows() - start), &[])?;
        }
        Ok(())
    }

    /// Validate a RETURN row before its legacy checkpoint becomes durable.
    /// MutationSink later consumes the corresponding prevalidated-row count so
    /// the root materializer is not invoked twice for the same result.
    pub fn validate_mutation_result_before_checkpoint(&self, batch: &RecordBatch) -> Result<()> {
        self.validate_mutation_result_before_checkpoint_with_values(batch, &[])
    }

    /// Validate a RETURN row using the engine values that produced its public
    /// Arrow representation. The sidecar retains complete graph entity metadata
    /// when the physical output schema only carries an entity identity.
    pub fn validate_mutation_result_before_checkpoint_with_values(&self, batch: &RecordBatch, values: &[Vec<CypherValue>]) -> Result<()> {
        let Some(validator) = self.mutation_result_validator()? else {
            return Ok(());
        };
        if !values.is_empty() && values.len() != batch.num_rows() {
            return Err(DataFusionError::Execution(format!(
                "mutation result sidecar has {} rows for an Arrow batch of {} rows",
                values.len(),
                batch.num_rows()
            )));
        }
        validator(batch, values)?;
        let mut prevalidated = self
            .mutation_result_rows_prevalidated
            .lock()
            .map_err(|_| DataFusionError::Execution("prevalidated mutation row counter was poisoned".into()))?;
        *prevalidated = prevalidated
            .checked_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .ok_or_else(|| DataFusionError::Execution("prevalidated mutation row count overflow".into()))?;
        Ok(())
    }

    fn mutation_result_validator(&self) -> Result<Option<MutationResultValidator>> {
        self.mutation_result_validator
            .read()
            .map(|validator| validator.clone())
            .map_err(|_| DataFusionError::Execution("mutation result validator lock was poisoned".into()))
    }

    pub fn capabilities(&self) -> &GraphCapabilities {
        &self.capabilities
    }

    pub fn mutation_request(&self) -> Result<&MutationRequest> {
        self.mutation_request
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("query context has no mutation request".into()))
    }

    /// Install the runtime's live counter accumulator for the query result.
    pub fn install_mutation_counters(&self, counters: Arc<Mutex<MutationCounters>>) -> Result<()> {
        let mut slot = self
            .mutation_counters
            .lock()
            .map_err(|_| DataFusionError::Execution("mutation counter lock was poisoned".into()))?;
        if slot.is_some() {
            return Err(DataFusionError::Execution("mutation counters are already installed".into()));
        }
        *slot = Some(counters);
        Ok(())
    }

    pub fn mutation_counters(&self) -> Result<Option<MutationCounters>> {
        let counters = self
            .mutation_counters
            .lock()
            .map_err(|_| DataFusionError::Execution("mutation counter lock was poisoned".into()))?
            .as_ref()
            .cloned();
        counters
            .map(|counters| {
                counters
                    .lock()
                    .map(|counters| counters.clone())
                    .map_err(|_| DataFusionError::Execution("mutation counter accumulator was poisoned".into()))
            })
            .transpose()
    }

    /// Check a physical operator against the capability snapshot that was
    /// validated before opening this query's graph view/session.
    pub fn validate_operator(&self, node: &GraphLogicalNode) -> Result<()> {
        use crate::logical::{EntityKind, GraphOperator};
        let read_operation = match &node.operator {
            GraphOperator::GraphScan { .. } => Some(ReadOperation::NodeScan),
            GraphOperator::GraphSeek { internal_id: true, .. } | GraphOperator::GraphSeekParameter { internal_id: true, .. } => {
                Some(ReadOperation::FetchNodes)
            }
            GraphOperator::GraphSeek { .. } | GraphOperator::GraphSeekParameter { .. } => Some(ReadOperation::Seek),
            GraphOperator::Expand { .. } | GraphOperator::ExpandInto { .. } => Some(ReadOperation::Expand),
            GraphOperator::VariableExpand { .. } => Some(ReadOperation::VariableExpand),
            GraphOperator::GraphHydrate { binding, .. } => Some(fetch_operation(node, binding)),
            GraphOperator::Exists {
                entity_kind: EntityKind::Node,
                ..
            } => Some(ReadOperation::FetchNodes),
            GraphOperator::Exists {
                entity_kind: EntityKind::Relationship,
                ..
            } => Some(ReadOperation::FetchRelationships),
            _ => None,
        };
        if let Some(operation) = read_operation {
            let mut request = self
                .read_request
                .clone()
                .ok_or_else(|| DataFusionError::Plan("graph read operator has no ReadRequest in its query context".into()))?;
            request.requirements.operations.insert(operation);
            request.requirements.retained_snapshot = true;
            self.capabilities.validate_read(&request).map_err(provider_error)?;
        }
        match &node.operator {
            GraphOperator::GraphScan { .. }
            | GraphOperator::GraphSeek { .. }
            | GraphOperator::GraphSeekParameter { .. }
            | GraphOperator::Expand { .. }
            | GraphOperator::ExpandInto { .. }
            | GraphOperator::VariableExpand { .. }
            | GraphOperator::GraphHydrate { .. }
            | GraphOperator::Exists { .. } => Ok(()),
            GraphOperator::OptionalGraphApply | GraphOperator::CorrelatedApply => {
                Err(provider_error(GraphProviderError::UnsupportedCapability {
                    capability: "optional/correlated graph apply",
                }))
            }
            GraphOperator::MutationProgram { .. } => {
                let request = self
                    .mutation_request
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("mutation plan has no mutation request in its query context".into()))?;
                if let GraphOperator::MutationProgram { atomicity } = &node.operator {
                    if mutation_policy(*atomicity) != request.atomicity {
                        return Err(DataFusionError::Plan(
                            "mutation plan policy differs from the admitted MutationRequest".into(),
                        ));
                    }
                }
                self.capabilities.validate_mutation(request).map_err(provider_error)
            }
            GraphOperator::MutationSink { atomicity } => {
                let request = self
                    .mutation_request
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("MutationSink has no mutation request in its query context".into()))?;
                if mutation_policy(*atomicity) != request.atomicity {
                    return Err(DataFusionError::Plan(
                        "MutationSink policy differs from the admitted MutationRequest".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    pub fn validate_plan(&self, plan: &datafusion::logical_expr::LogicalPlan) -> Result<()> {
        validate_plan_capabilities(plan, &self.capabilities, self.read_request.as_ref(), self.mutation_request.as_ref())
    }

    pub fn budget(&self) -> &BudgetTracker {
        &self.budget
    }

    pub fn budget_limits(&self) -> BudgetLimits {
        self.budget.limits()
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn parameter(&self, name: &str) -> Option<&CypherValue> {
        self.parameters.get(name)
    }

    /// Lookup a value bound by an enclosing correlated apply. The innermost
    /// scope shadows outer scopes while a nested branch is running.
    pub fn correlation(&self, name: &str) -> Result<Option<CypherValue>> {
        self.correlations.get(name)
    }

    /// Push one outer-row binding scope for a correlated branch. Keep the
    /// returned guard alive while draining that branch's stream.
    pub fn push_correlations(self: &Arc<Self>, bindings: std::collections::HashMap<Arc<str>, CypherValue>) -> Result<CorrelationGuard> {
        self.correlations.push(bindings)
    }

    pub fn operator_runtime(&self) -> Result<&Arc<dyn GraphOperatorRuntime>> {
        self.operator_runtime
            .as_ref()
            .ok_or_else(|| DataFusionError::Execution("no graph operator runtime is installed".into()))
    }

    pub fn mutation_runtime(&self) -> Result<&Arc<dyn MutationProgramRuntime>> {
        self.mutation_runtime
            .as_ref()
            .ok_or_else(|| DataFusionError::Execution("no graph mutation runtime is installed".into()))
    }

    pub fn take_mutation_session(&self) -> Result<Box<dyn GraphMutationSession>> {
        self.mutation_session
            .lock()
            .map_err(|_| DataFusionError::Execution("graph mutation session lock was poisoned".into()))?
            .take()
            .ok_or_else(|| DataFusionError::Execution("graph mutation session was already taken".into()))
    }

    /// Register the deferred decision returned by the mutation runtime.
    pub fn install_mutation_finalizer(&self, finalizer: Box<dyn MutationExecutionFinalizer>) -> Result<()> {
        let mut slot = self
            .mutation_finalizer
            .lock()
            .map_err(|_| DataFusionError::Execution("graph mutation finalizer lock was poisoned".into()))?;
        if slot.is_some() {
            return Err(DataFusionError::Execution(
                "a mutation execution finalizer is already installed".into(),
            ));
        }
        *slot = Some(finalizer);
        Ok(())
    }

    /// Commit only after MutationSink has drained and validated all RETURN
    /// batches. A failed commit is not followed by an unsafe retry or abort.
    pub async fn commit_mutation(&self) -> Result<CommitReceipt> {
        let finalizer = self
            .mutation_finalizer
            .lock()
            .map_err(|_| DataFusionError::Execution("graph mutation finalizer lock was poisoned".into()))?
            .take()
            .ok_or_else(|| DataFusionError::Execution("no deferred mutation finalizer is installed".into()))?;
        let receipt = finalizer.commit().await?;
        *self
            .mutation_commit_receipt
            .lock()
            .map_err(|_| DataFusionError::Execution("mutation commit receipt lock was poisoned".into()))? = Some(receipt.clone());
        Ok(receipt)
    }

    /// The validated provider commit receipt is available after MutationSink
    /// drains RETURN output and commits successfully.
    pub fn mutation_commit_receipt(&self) -> Result<Option<CommitReceipt>> {
        self.mutation_commit_receipt
            .lock()
            .map(|receipt| receipt.clone())
            .map_err(|_| DataFusionError::Execution("mutation commit receipt lock was poisoned".into()))
    }

    /// Abort staged effects after a result, budget, or cancellation failure.
    pub async fn abort_mutation(&self) -> Result<()> {
        self.cancellation.cancel();
        let finalizer = self
            .mutation_finalizer
            .lock()
            .map_err(|_| DataFusionError::Execution("graph mutation finalizer lock was poisoned".into()))?
            .take();
        if let Some(finalizer) = finalizer {
            finalizer.abort().await
        } else {
            Ok(())
        }
    }

    /// Synchronously make a pending mutation ineligible for commit. Used when
    /// downstream drops the MutationSink stream before it reaches EOF.
    pub fn cancel_mutation(&self) {
        self.cancellation.cancel();
        if let Ok(finalizer) = self.mutation_finalizer.lock() {
            if let Some(finalizer) = finalizer.as_ref() {
                finalizer.cancel();
            }
        }
    }

    pub fn mutation_lease(&self) -> &Arc<MutationExecutionLease> {
        &self.mutation_lease
    }

    pub fn claim_mutation(&self) -> Result<MutationExecutionPermit> {
        self.mutation_lease.claim()
    }

    /// Attach this context to a DataFusion session. Child task contexts inherit
    /// the same Arc, snapshot identity, and mutation lease.
    pub fn install(self, config: &mut datafusion::execution::context::SessionConfig) {
        config.set_extension(Arc::new(self));
    }

    /// Retrieve query-scoped graph state inside a physical operator.
    pub fn from_task_context(context: &TaskContext) -> Result<Arc<Self>> {
        context
            .session_config()
            .get_extension::<Self>()
            .ok_or_else(|| DataFusionError::Execution("no graph execution context is installed".into()))
    }
}

/// Restores the previous correlated scope when a branch completes or is
/// cancelled/dropped.
#[derive(Debug)]
pub struct CorrelationGuard {
    scopes: CorrelationScopes,
}

impl Drop for CorrelationGuard {
    fn drop(&mut self) {
        self.scopes.pop();
    }
}

/// Values bound by one correlated outer row.
pub type CorrelationBindings = std::collections::HashMap<Arc<str>, CypherValue>;
type CorrelationScopeStack = Vec<CorrelationBindings>;

/// Stack of nested outer-row bindings used by correlated apply runtimes.
#[derive(Clone, Debug, Default)]
pub struct CorrelationScopes(Arc<Mutex<CorrelationScopeStack>>);

impl CorrelationScopes {
    pub fn get(&self, name: &str) -> Result<Option<CypherValue>> {
        let scopes = self
            .0
            .lock()
            .map_err(|_| DataFusionError::Execution("graph correlation lock was poisoned".into()))?;
        Ok(scopes.iter().rev().find_map(|scope| scope.get(name).cloned()))
    }

    pub fn push(&self, bindings: CorrelationBindings) -> Result<CorrelationGuard> {
        self.0
            .lock()
            .map_err(|_| DataFusionError::Execution("graph correlation lock was poisoned".into()))?
            .push(bindings);
        Ok(CorrelationGuard { scopes: self.clone() })
    }

    fn pop(&self) {
        if let Ok(mut scopes) = self.0.lock() {
            scopes.pop();
        }
    }
}

/// Validate every graph operation in a logical plan before opening a retained
/// read view or mutation session. This preserves the provider contract's
/// fail-before-side-effects ordering.
pub fn validate_plan_capabilities(
    plan: &datafusion::logical_expr::LogicalPlan,
    capabilities: &GraphCapabilities,
    read_request: Option<&ReadRequest>,
    mutation_request: Option<&MutationRequest>,
) -> Result<()> {
    use crate::logical::{EntityKind, GraphOperator};
    let mut nodes = Vec::new();
    collect_graph_nodes(plan, &mut nodes);
    let mut read_operations = std::collections::BTreeSet::new();
    let mut selected_policy = None;
    for node in nodes {
        match &node.operator {
            GraphOperator::GraphScan { .. } => {
                read_operations.insert(ReadOperation::NodeScan);
            }
            GraphOperator::GraphSeek { internal_id: true, .. } | GraphOperator::GraphSeekParameter { internal_id: true, .. } => {
                read_operations.insert(ReadOperation::FetchNodes);
            }
            GraphOperator::GraphSeek { .. } | GraphOperator::GraphSeekParameter { .. } => {
                read_operations.insert(ReadOperation::Seek);
            }
            GraphOperator::Expand { .. } | GraphOperator::ExpandInto { .. } => {
                read_operations.insert(ReadOperation::Expand);
            }
            GraphOperator::VariableExpand { .. } => {
                read_operations.insert(ReadOperation::VariableExpand);
            }
            GraphOperator::GraphHydrate { binding, .. } => {
                read_operations.insert(fetch_operation(node, binding));
            }
            GraphOperator::Exists {
                entity_kind: EntityKind::Node,
                ..
            } => {
                read_operations.insert(ReadOperation::FetchNodes);
            }
            GraphOperator::Exists {
                entity_kind: EntityKind::Relationship,
                ..
            } => {
                read_operations.insert(ReadOperation::FetchRelationships);
            }
            GraphOperator::MutationProgram { atomicity } | GraphOperator::MutationSink { atomicity } => {
                let request =
                    mutation_request.ok_or_else(|| DataFusionError::Plan("mutation plan requires a validated MutationRequest".into()))?;
                if mutation_policy(*atomicity) != request.atomicity {
                    return Err(DataFusionError::Plan(
                        "logical mutation policy differs from the admitted MutationRequest".into(),
                    ));
                }
                if selected_policy.is_some_and(|previous| previous != *atomicity) {
                    return Err(DataFusionError::Plan("one logical plan contains mixed mutation policies".into()));
                }
                selected_policy = Some(*atomicity);
            }
            GraphOperator::OptionalGraphApply | GraphOperator::CorrelatedApply => {
                return Err(provider_error(GraphProviderError::UnsupportedCapability {
                    capability: "optional/correlated graph apply",
                }));
            }
        }
    }
    if !read_operations.is_empty() {
        let mut request = read_request
            .cloned()
            .ok_or_else(|| DataFusionError::Plan("graph read operators require a validated ReadRequest".into()))?;
        request.requirements.operations.extend(read_operations);
        request.requirements.retained_snapshot = true;
        capabilities.validate_read(&request).map_err(provider_error)?;
    }
    if selected_policy.is_some() {
        let request = mutation_request.ok_or_else(|| DataFusionError::Plan("mutation plan requires a validated MutationRequest".into()))?;
        capabilities.validate_mutation(request).map_err(provider_error)?;
    }
    Ok(())
}

fn mutation_policy(policy: crate::logical::MutationAtomicity) -> MutationAtomicityPolicy {
    match policy {
        crate::logical::MutationAtomicity::Statement => MutationAtomicityPolicy::Statement,
        crate::logical::MutationAtomicity::LegacyChunked => MutationAtomicityPolicy::LegacyChunked,
    }
}

fn fetch_operation(node: &GraphLogicalNode, binding: &str) -> ReadOperation {
    let is_relationship = node
        .inputs
        .first()
        .and_then(|input| input.schema().as_arrow().field_with_name(binding).ok())
        .is_some_and(|field| matches!(field.data_type(), datafusion::arrow::datatypes::DataType::Struct(_)));
    if is_relationship {
        ReadOperation::FetchRelationships
    } else {
        ReadOperation::FetchNodes
    }
}

fn collect_graph_nodes<'a>(plan: &'a datafusion::logical_expr::LogicalPlan, nodes: &mut Vec<&'a GraphLogicalNode>) {
    if let Some(node) = crate::logical::graph_node(plan) {
        nodes.push(node);
    }
    for input in plan.inputs() {
        collect_graph_nodes(input, nodes);
    }
}

pub(crate) fn provider_error(error: GraphProviderError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Convert a provider stream to DataFusion's stream error type while retaining
/// the known output schema.
pub fn provider_batches(schema: SchemaRef, batches: hydradb_graph_provider::GraphBatchStream) -> SendableRecordBatchStream {
    let output_schema = Arc::clone(&schema);
    let mapped = batches.map(|result| result.map_err(provider_error));
    Box::pin(RecordBatchStreamAdapter::new(output_schema, mapped))
}

/// Split any oversized DataFusion batches without buffering the full stream.
///
/// Each upstream batch is retained only until its slices have been yielded;
/// memory use is therefore bounded by one provider batch plus one output
/// batch. Provider streams should also apply their own per-batch byte and row
/// limits before conversion to DataFusion's error type.
pub fn bounded_batches(input: SendableRecordBatchStream, max_rows: usize) -> Result<SendableRecordBatchStream> {
    if max_rows == 0 {
        return Err(DataFusionError::Plan("graph batch size must be positive".into()));
    }
    let schema: SchemaRef = input.schema();
    let output_schema = Arc::clone(&schema);
    let bounded = stream::try_unfold((input, VecDeque::new()), move |(mut input, mut pending)| async move {
        loop {
            if let Some(batch) = pending.pop_front() {
                return Ok(Some((batch, (input, pending))));
            }
            let Some(batch) = input.try_next().await? else {
                return Ok(None);
            };
            if batch.num_rows() <= max_rows {
                return Ok(Some((batch, (input, pending))));
            }
            let mut offset = 0;
            while offset < batch.num_rows() {
                let length = max_rows.min(batch.num_rows() - offset);
                pending.push_back(batch.slice(offset, length));
                offset += length;
            }
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, bounded)))
}
