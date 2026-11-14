//! DataFusion physical planning and execution barrier for graph extensions.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements, Partitioning,
    PlanProperties, ReplaceChildrenOptions,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::{stream, TryStreamExt};

use crate::context::GraphExecutionContext;
use crate::logical::{validate_expressions, GraphLogicalNode, GraphOperator};

/// DataFusion extension planner for every graph logical node in this crate.
#[derive(Debug, Default)]
pub struct GraphExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for GraphExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<GraphLogicalNode>() else {
            return Ok(None);
        };
        validate_expressions(node)?;
        if logical_inputs.len() != physical_inputs.len() || logical_inputs.len() != node.inputs.len() {
            return internal_err!("{} has mismatched logical/physical input counts", node.operator.name());
        }
        Ok(Some(Arc::new(GraphOperatorExec::try_new(
            Arc::new(node.clone()),
            physical_inputs.to_vec(),
        )?)))
    }
}

/// Physical implementation used for ordinary graph operators and the mutation
/// boundary. One partition serializes ordinal-sensitive graph work and writes.
#[derive(Debug)]
pub struct GraphOperatorExec {
    node: Arc<GraphLogicalNode>,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
    name: String,
}

impl GraphOperatorExec {
    pub fn try_new(node: Arc<GraphLogicalNode>, inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        if inputs.len() != node.inputs.len() {
            return internal_err!("{} physical input count does not match logical input count", node.operator.name());
        }
        if matches!(&node.operator, GraphOperator::MutationSink { .. }) && inputs.len() != 1 {
            return internal_err!("MutationSink requires exactly one input");
        }
        if matches!(&node.operator, GraphOperator::MutationSink { .. })
            && !node
                .inputs
                .first()
                .and_then(crate::logical::graph_node)
                .is_some_and(|input| matches!(&input.operator, GraphOperator::MutationProgram { .. }))
        {
            return internal_err!("MutationSink must directly own a MutationProgram logical node");
        }
        let schema = Arc::new(node.schema.as_arrow().clone());
        let emission = if matches!(&node.operator, GraphOperator::MutationSink { .. }) {
            EmissionType::Final
        } else {
            EmissionType::Incremental
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            emission,
            Boundedness::Bounded,
        ));
        let name = format!("{}Exec", node.operator.name());
        Ok(Self {
            node,
            inputs,
            schema,
            properties,
            name,
        })
    }

    fn execute_mutation(&self, context: Arc<TaskContext>, graph_context: Arc<GraphExecutionContext>) -> Result<SendableRecordBatchStream> {
        let program = self
            .node
            .mutation_program
            .clone()
            .ok_or_else(|| DataFusionError::Plan("MutationProgram lost its bound mutation payload".into()))?;
        let input_plan = self
            .inputs
            .first()
            .ok_or_else(|| DataFusionError::Plan("MutationProgram is missing its input plan".into()))?;
        let permit = graph_context.claim_mutation()?;
        let session = graph_context.take_mutation_session()?;
        let input = if session.replay().is_some() {
            Box::pin(RecordBatchStreamAdapter::new(input_plan.schema(), stream::empty())) as SendableRecordBatchStream
        } else {
            crate::context::bounded_batches(
                input_plan.execute(0, context)?,
                usize::try_from(graph_context.budget_limits().max_batch_rows).unwrap_or(usize::MAX),
            )?
        };
        let runtime = Arc::clone(graph_context.mutation_runtime()?);
        let output = runtime.execute(
            program,
            input,
            Arc::clone(&self.schema),
            session,
            permit,
            Arc::clone(&graph_context),
        )?;
        graph_context.install_mutation_counters(Arc::clone(&output.counters))?;
        graph_context.install_mutation_finalizer(output.finalizer)?;
        Ok(output.stream)
    }

    fn execute_sink(&self, context: Arc<TaskContext>, graph_context: Arc<GraphExecutionContext>) -> Result<SendableRecordBatchStream> {
        let child = self
            .inputs
            .first()
            .ok_or_else(|| DataFusionError::Plan("MutationSink is missing its MutationProgram input".into()))?;
        let child_stream = child.execute(0, context)?;
        let completion = MutationSinkCompletion::new(Arc::clone(&graph_context));
        let input = crate::context::bounded_batches(
            child_stream,
            usize::try_from(graph_context.budget_limits().max_batch_rows).unwrap_or(usize::MAX),
        )?;
        let schema = Arc::clone(&self.schema);
        let output_schema = Arc::clone(&self.schema);
        let limits = graph_context.budget_limits();
        let result_context = Arc::clone(&graph_context);

        // Consume the complete child result before yielding the first row. This
        // prevents an outer LIMIT or downstream cancellation from stopping a
        // write halfway through. The request-wide budget bounds this spool;
        // commit remains deferred until all result checks have passed.
        let materialized = stream::once(async move {
            let mut completion = completion;
            let drained = input
                .try_fold(
                    (Vec::new(), 0_u64, 0_u64, 0_u64),
                    |(mut batches, mut rows, mut bytes, mut batch_count), batch| {
                        let result_context = Arc::clone(&result_context);
                        async move {
                            rows = rows.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
                            bytes = bytes.saturating_add(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX));
                            batch_count = batch_count.saturating_add(1);
                            if u64::try_from(batch.num_rows()).unwrap_or(u64::MAX) > limits.max_batch_rows {
                                return Err(DataFusionError::Execution(
                                    "MutationSink received a batch larger than the query batch-row bound".into(),
                                ));
                            }
                            if u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX) > limits.max_batch_bytes {
                                return Err(DataFusionError::Execution(
                                    "MutationSink received a batch larger than the query batch-byte bound".into(),
                                ));
                            }
                            if rows > limits.max_total_rows {
                                return Err(DataFusionError::Execution(
                                    "MutationSink result exceeded the query row budget before commit".into(),
                                ));
                            }
                            if bytes > limits.max_total_bytes {
                                return Err(DataFusionError::Execution(
                                    "MutationSink result exceeded the query byte budget before commit".into(),
                                ));
                            }
                            if batch_count > limits.max_batches {
                                return Err(DataFusionError::Execution(
                                    "MutationSink result exceeded the query batch-count budget".into(),
                                ));
                            }
                            result_context.validate_mutation_result(&batch)?;
                            batches.push(batch);
                            Ok((batches, rows, bytes, batch_count))
                        }
                    },
                )
                .await;
            let batches = match drained {
                Ok((batches, _, _, _)) => {
                    let result = graph_context.commit_mutation().await;
                    completion.completed = true;
                    result?;
                    batches
                }
                Err(error) => {
                    let _abort_result = graph_context.abort_mutation().await;
                    completion.completed = true;
                    // Preserve the primary error, particularly typed provider
                    // conflict/durability failures wrapped as DataFusion
                    // External errors.
                    return Err(error);
                }
            };
            let batches = stream::iter(batches.into_iter().map(Ok));
            Ok::<SendableRecordBatchStream, DataFusionError>(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, materialized)))
    }
}

/// Signals cancellation if the downstream drops MutationSink before the
/// result is fully drained and committed.
struct MutationSinkCompletion {
    graph_context: Arc<GraphExecutionContext>,
    completed: bool,
}

impl MutationSinkCompletion {
    fn new(graph_context: Arc<GraphExecutionContext>) -> Self {
        Self {
            graph_context,
            completed: false,
        }
    }
}

impl Drop for MutationSinkCompletion {
    fn drop(&mut self) {
        if !self.completed {
            self.graph_context.cancel_mutation();
        }
    }
}

impl DisplayAs for GraphOperatorExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.name, self.node.operator)
    }
}

impl ExecutionPlan for GraphOperatorExec {
    fn name(&self) -> &str {
        &self.name
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    #[allow(deprecated)]
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(children, ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute))
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != self.node.inputs.len() {
            return internal_err!(
                "{} expects {} children, received {}",
                self.name,
                self.node.inputs.len(),
                children.len()
            );
        }
        Ok(Arc::new(Self::try_new(Arc::clone(&self.node), children)?))
    }

    fn apply_expressions(&self, _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::SinglePartition; self.inputs.len()])
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "{} only supports partition 0, received {partition}",
                self.name
            )));
        }
        let graph_context = GraphExecutionContext::from_task_context(&context)?;
        graph_context.validate_operator(&self.node)?;
        if graph_context.cancellation().is_cancelled() {
            return Err(DataFusionError::Execution("graph query was cancelled".into()));
        }
        if graph_context.deadline().is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(DataFusionError::Execution("graph query deadline exceeded".into()));
        }
        match &self.node.operator {
            GraphOperator::MutationProgram { .. } => self.execute_mutation(context, graph_context),
            GraphOperator::MutationSink { .. } => self.execute_sink(context, graph_context),
            _ => crate::context::bounded_batches(
                graph_context.operator_runtime()?.execute(
                    Arc::clone(&self.node),
                    self.inputs.clone(),
                    partition,
                    context,
                    Arc::clone(&graph_context),
                )?,
                usize::try_from(graph_context.budget_limits().max_batch_rows).unwrap_or(usize::MAX),
            ),
        }
    }
}

/// Convenience builder for embedding graph nodes in DataFusion's default
/// physical planner alongside other extension planners.
pub fn graph_extension_planners() -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
    vec![Arc::new(GraphExtensionPlanner)]
}
