use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::context::{QueryPlanner, SessionConfig};
use datafusion::execution::{SendableRecordBatchStream, SessionState, SessionStateBuilder, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use hydradb_graph_datafusion::graph_extension_planners;
use hydradb_graph_datafusion::GraphExecutionContext;
use hydradb_graph_plan::{GraphExpression, GraphIr, GraphIrKind, GraphSeekPredicate, MutationOperation, MutationPolicy};
use hydradb_graph_types::CypherValue;

use crate::cache::{ParameterType, TemplateCache, TemplateKey};
use crate::classification::QueryEffect;
use crate::error::{DfError, DfResult};
use crate::lower::{lower_graph_ir, LoweringState};
use crate::template::PreparedTemplate;

/// Long-lived query frontend. It caches immutable bound graph templates only;
/// logical/physical DataFusion plans and execution contexts are fresh per call.
pub struct CypherDfEngine {
    state: Arc<SessionState>,
    templates: Mutex<TemplateCache<PreparedTemplate>>,
    batch_size: usize,
}

impl CypherDfEngine {
    /// Default capacity for immutable prepared templates.
    pub const DEFAULT_TEMPLATE_CACHE_CAPACITY: usize = 256;
    /// Default maximum number of graph rows emitted per batch.
    pub const DEFAULT_BATCH_SIZE: usize = 1024;

    pub fn new(state: SessionState, cache_capacity: usize, batch_size: usize) -> DfResult<Self> {
        if batch_size == 0 {
            return Err(DfError::Invalid("graph batch size must be positive".into()));
        }
        Ok(Self {
            state: Arc::new(state),
            templates: Mutex::new(TemplateCache::new(cache_capacity)),
            batch_size,
        })
    }

    /// Build a production engine with conservative DataFusion defaults and the
    /// graph extension physical planner. Tests and custom runtimes can continue
    /// to use [`Self::new`] with their own `SessionState`.
    pub fn production() -> DfResult<Self> {
        Self::production_with_limits(Self::DEFAULT_TEMPLATE_CACHE_CAPACITY, Self::DEFAULT_BATCH_SIZE)
    }

    /// Build a production engine with custom template-cache capacity and graph
    /// batch size, retaining the graph-aware DataFusion planner/configuration.
    pub fn production_with_limits(cache_capacity: usize, batch_size: usize) -> DfResult<Self> {
        if batch_size == 0 {
            return Err(DfError::Invalid("graph batch size must be positive".into()));
        }
        let state = production_session_state(batch_size);
        Self::new(state, cache_capacity, batch_size)
    }

    /// Return a cached immutable template or bind the query once. The key
    /// includes only structural/type/configuration inputs, never parameter
    /// values or provider runtime handles.
    pub fn prepare(
        &self,
        query: &str,
        parameters: &BTreeMap<String, CypherValue>,
        schema_revision: u64,
        semantic_codec_version: u32,
        capability_fingerprint: [u8; 32],
        mutation_policy: MutationPolicy,
    ) -> DfResult<Arc<PreparedTemplate>> {
        let key = TemplateKey::for_parameters(
            "neo4j-cypher-25",
            query,
            parameters,
            schema_revision,
            semantic_codec_version,
            capability_fingerprint,
            mutation_policy,
        );
        let mut cache = self
            .templates
            .lock()
            .map_err(|_| DfError::Invalid("prepared-template cache is poisoned".into()))?;
        if let Some(template) = cache.get(&key) {
            return Ok(template);
        }
        let template = Arc::new(PreparedTemplate::compile(
            query,
            parameters
                .iter()
                .map(|(name, value)| (name.clone(), ParameterType::of_value(value))),
            schema_revision,
            semantic_codec_version,
            capability_fingerprint,
            mutation_policy,
        )?);
        cache.insert(key, Arc::clone(&template));
        Ok(template)
    }

    /// EXPLAIN never opens a provider view or mutation session and redacts all
    /// per-request parameter values by explaining only the immutable graph IR.
    pub fn explain(&self, template: &PreparedTemplate) -> String {
        template.explain()
    }

    /// Execute a template with fresh request parameters and a fresh provider
    /// execution context. The runtime factory is called only after the
    /// template has been classified and lowered to a physical plan, allowing
    /// callers to validate capabilities and open a read view or one mutation
    /// session only after unsupported/planning errors have been rejected.
    pub async fn execute<F, Fut>(
        &self,
        template: Arc<PreparedTemplate>,
        parameters: BTreeMap<String, CypherValue>,
        mutation_policy: MutationPolicy,
        runtime_factory: F,
    ) -> DfResult<SendableRecordBatchStream>
    where
        F: FnOnce(QueryEffect, Arc<GraphIr>, Arc<BTreeMap<String, CypherValue>>) -> Fut,
        Fut: Future<Output = DfResult<Arc<GraphExecutionContext>>>,
    {
        if template.key().mutation_policy != mutation_policy {
            return Err(DfError::Invalid(
                "request mutation policy differs from the prepared-template key".into(),
            ));
        }
        validate_parameters(&template, &parameters)?;
        let parameters = Arc::new(parameters);
        let logical = lower_graph_ir(
            template.graph_ir(),
            &LoweringState {
                parameters: &parameters,
                batch_size: self.batch_size,
                mutation_policy,
            },
        )?;
        let optimized = self.state.optimize(&logical)?;
        let physical = self
            .state
            .query_planner()
            .create_physical_plan(&optimized, self.state.as_ref())
            .await?;

        // Classification and physical capability planning happen before this
        // callback can begin a provider view or mutation session.
        let graph_context = runtime_factory(template.effect(), Arc::clone(template.graph_ir()), Arc::clone(&parameters)).await?;

        let mut config = self.state.config().clone();
        config.set_extension(graph_context);
        let task_context = Arc::new(TaskContext::new(
            None,
            self.state.session_id().to_string(),
            config,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::clone(self.state.runtime_env()),
        ));
        Ok(execute_stream(physical, task_context)?)
    }

    pub fn cached_template_count(&self) -> DfResult<usize> {
        self.templates
            .lock()
            .map(|cache| cache.len())
            .map_err(|_| DfError::Invalid("prepared-template cache is poisoned".into()))
    }
}

fn production_session_state(batch_size: usize) -> SessionState {
    let mut config = SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(1)
        .with_repartition_joins(false)
        .with_repartition_aggregations(false)
        .with_repartition_file_scans(false)
        .with_repartition_windows(false)
        .with_repartition_sorts(false);
    let optimizer = &mut config.options_mut().optimizer;
    optimizer.enable_dynamic_filter_pushdown = false;
    optimizer.enable_join_dynamic_filter_pushdown = false;
    optimizer.enable_aggregate_dynamic_filter_pushdown = false;
    optimizer.enable_topk_dynamic_filter_pushdown = false;
    optimizer.enable_round_robin_repartition = false;

    SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .with_query_planner(Arc::new(GraphExtensionQueryPlanner::new()))
        .build()
}

struct GraphExtensionQueryPlanner {
    planner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for GraphExtensionQueryPlanner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GraphExtensionQueryPlanner")
    }
}

impl GraphExtensionQueryPlanner {
    fn new() -> Self {
        Self {
            planner: DefaultPhysicalPlanner::with_extension_planners(graph_extension_planners()),
        }
    }
}

#[async_trait]
impl QueryPlanner for GraphExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.planner.create_physical_plan(logical_plan, session_state).await
    }
}

fn validate_parameters(template: &PreparedTemplate, parameters: &BTreeMap<String, CypherValue>) -> DfResult<()> {
    let mut required = std::collections::BTreeSet::new();
    collect_ir_parameters(template.graph_ir(), &mut required);
    for name in required {
        let actual = parameters.get(&name).ok_or_else(|| DfError::MissingParameter(name.clone()))?;
        let expected = template
            .key()
            .parameter_types
            .iter()
            .find(|(parameter, _)| parameter == &name)
            .map(|(_, parameter_type)| *parameter_type)
            .ok_or_else(|| DfError::Invalid(format!("prepared template has no type for parameter `${name}`")))?;
        if ParameterType::of_value(actual) != expected {
            return Err(DfError::Invalid(format!(
                "parameter `${name}` changed type after template preparation"
            )));
        }
    }
    Ok(())
}

fn collect_ir_parameters(ir: &GraphIr, output: &mut std::collections::BTreeSet<String>) {
    match &ir.kind {
        GraphIrKind::SingleRow => {}
        GraphIrKind::NodeScan { input, .. }
        | GraphIrKind::RelationshipScan { input, .. }
        | GraphIrKind::GraphSeek { input, .. }
        | GraphIrKind::GraphProcedure { input, .. } => {
            if let Some(input) = input {
                collect_ir_parameters(input, output);
            }
            match &ir.kind {
                GraphIrKind::GraphSeek { predicate, .. } => collect_seek_parameters(predicate, output),
                GraphIrKind::GraphProcedure { procedure, .. } => {
                    for expression in &procedure.arguments {
                        collect_expression_parameters(expression, output);
                    }
                }
                _ => {}
            }
        }
        GraphIrKind::Filter { input, predicate } => {
            collect_ir_parameters(input, output);
            collect_expression_parameters(predicate, output);
        }
        GraphIrKind::Project { input, items } => {
            collect_ir_parameters(input, output);
            collect_projection_parameters(items, output);
        }
        GraphIrKind::Expand { input, .. }
        | GraphIrKind::VariableExpand { input, .. }
        | GraphIrKind::GraphHydrate { input, .. }
        | GraphIrKind::Sort { input, .. } => collect_ir_parameters(input, output),
        GraphIrKind::Exists { input, subquery, .. } => {
            collect_ir_parameters(input, output);
            collect_ir_parameters(subquery, output);
        }
        GraphIrKind::OptionalApply { left, right, .. } | GraphIrKind::CorrelatedApply { left, right, .. } => {
            collect_ir_parameters(left, output);
            collect_ir_parameters(right, output);
        }
        GraphIrKind::Unwind { input, expression, .. } => {
            collect_ir_parameters(input, output);
            collect_expression_parameters(expression, output);
        }
        GraphIrKind::Union { arms, .. } => {
            for arm in arms {
                collect_ir_parameters(arm, output);
            }
        }
        GraphIrKind::Aggregation {
            input,
            group_by,
            aggregates,
        } => {
            collect_ir_parameters(input, output);
            collect_projection_parameters(group_by, output);
            for aggregate in aggregates {
                if let Some(argument) = &aggregate.argument {
                    collect_expression_parameters(argument, output);
                }
            }
        }
        GraphIrKind::Window { input, calls } => {
            collect_ir_parameters(input, output);
            for call in calls {
                for expression in &call.arguments {
                    collect_expression_parameters(expression, output);
                }
                for expression in &call.partition_by {
                    collect_expression_parameters(expression, output);
                }
                for sort in &call.order_by {
                    collect_expression_parameters(&sort.expression, output);
                }
            }
        }
        GraphIrKind::ResultWindow { input, skip, limit } => {
            collect_ir_parameters(input, output);
            if let Some(expression) = skip {
                collect_expression_parameters(expression, output);
            }
            if let Some(expression) = limit {
                collect_expression_parameters(expression, output);
            }
        }
        GraphIrKind::MutationProgram { input, program } => {
            collect_ir_parameters(input, output);
            for step in &program.steps {
                collect_mutation_parameters(&step.operation, output);
            }
            collect_projection_parameters(&program.returning, output);
        }
    }
}

fn collect_seek_parameters(predicate: &GraphSeekPredicate, output: &mut std::collections::BTreeSet<String>) {
    match predicate {
        GraphSeekPredicate::Id(expression) => collect_expression_parameters(expression, output),
        GraphSeekPredicate::PropertyEquals { value, .. } | GraphSeekPredicate::PropertyIn { values: value, .. } => {
            collect_expression_parameters(value, output);
        }
        GraphSeekPredicate::Composite(values) => {
            for (_, expression) in values {
                collect_expression_parameters(expression, output);
            }
        }
    }
}

fn collect_projection_parameters(projections: &[hydradb_graph_plan::Projection], output: &mut std::collections::BTreeSet<String>) {
    for projection in projections {
        collect_expression_parameters(&projection.expression, output);
    }
}

fn collect_mutation_parameters(operation: &MutationOperation, output: &mut std::collections::BTreeSet<String>) {
    match operation {
        MutationOperation::Create { pattern } => collect_mutation_pattern(pattern, output),
        MutationOperation::Merge {
            pattern,
            on_match,
            on_create,
        } => {
            collect_mutation_pattern(pattern, output);
            collect_mutation_updates(on_match, output);
            collect_mutation_updates(on_create, output);
        }
        MutationOperation::Set { updates } => collect_mutation_updates(updates, output),
        MutationOperation::Remove { targets } | MutationOperation::Delete { targets, .. } => {
            for expression in targets {
                collect_expression_parameters(expression, output);
            }
        }
        MutationOperation::Foreach { list, steps, .. } => {
            collect_expression_parameters(list, output);
            for step in steps {
                collect_mutation_parameters(step, output);
            }
        }
    }
}

fn collect_mutation_pattern(pattern: &hydradb_graph_plan::MutationPattern, output: &mut std::collections::BTreeSet<String>) {
    for vertex in &pattern.vertices {
        for (_, expression) in &vertex.properties {
            collect_expression_parameters(expression, output);
        }
    }
    for relationship in &pattern.relationships {
        for (_, expression) in &relationship.properties {
            collect_expression_parameters(expression, output);
        }
    }
}

fn collect_mutation_updates(updates: &[hydradb_graph_plan::MutationUpdate], output: &mut std::collections::BTreeSet<String>) {
    for update in updates {
        collect_expression_parameters(&update.target, output);
        collect_expression_parameters(&update.value, output);
    }
}

fn collect_expression_parameters(expression: &GraphExpression, output: &mut std::collections::BTreeSet<String>) {
    match expression {
        GraphExpression::Parameter(name) => {
            output.insert(name.as_str().to_owned());
        }
        GraphExpression::Binary { left, right, .. } => {
            collect_expression_parameters(left, output);
            collect_expression_parameters(right, output);
        }
        GraphExpression::Unary { expression, .. } => collect_expression_parameters(expression, output),
        GraphExpression::Function { arguments, .. } | GraphExpression::List(arguments) => {
            for argument in arguments {
                collect_expression_parameters(argument, output);
            }
        }
        GraphExpression::Map(entries) => {
            for (_, expression) in entries {
                collect_expression_parameters(expression, output);
            }
        }
        GraphExpression::Case { branches, fallback } => {
            for (condition, value) in branches {
                collect_expression_parameters(condition, output);
                collect_expression_parameters(value, output);
            }
            if let Some(fallback) = fallback {
                collect_expression_parameters(fallback, output);
            }
        }
        GraphExpression::Binding(_) | GraphExpression::Property { .. } | GraphExpression::Literal(_) => {}
    }
}
