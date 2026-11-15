//! Provider-backed execution for graph read operators.
//!
//! The runtime is deliberately query-scoped: it obtains its retained read view,
//! parameters, cancellation token, deadline, and budgets from the execution
//! context supplied by DataFusion for each invocation.

use std::{collections::HashMap, sync::Arc, time::Instant};

use datafusion::{
    arrow::{
        array::{new_null_array, Array, ArrayRef, BooleanArray, ListArray, StringArray, StructArray, UInt64Array, UInt8Array},
        buffer::{OffsetBuffer, ScalarBuffer},
        compute::{filter, take},
        datatypes::{DataType, Field, SchemaRef},
        record_batch::RecordBatch,
    },
    common::{DataFusionError, Result},
    execution::{context::TaskContext, SendableRecordBatchStream},
    logical_expr::Expr,
    physical_plan::{stream::RecordBatchStreamAdapter, ExecutionPlan},
};
use futures::{stream, StreamExt, TryStreamExt};
use hydradb_graph_provider::{
    EntityKind as ProviderEntityKind, ExpansionDepth, ExpansionInput, ExpansionRequest, GraphBatchStream, GraphProviderError,
    IndexDescriptor, IndexSeekKey, MissingEntityPolicy, NodeFetchKey, NodeFetchRequest, PageOrder, PageRequest, PathMode,
    PropertyProjection, RelationshipConstraints, RelationshipDirection, RelationshipFetchKey, RelationshipFetchRequest,
    RelationshipUniqueness as ProviderUniqueness, ScanAccessPath, ScanRequest, ScanSource, SeekSpec,
};
use hydradb_graph_types::{value_array, value_from_array, CypherValue, NodeId, RelationshipId, RelationshipIdentity, RelationshipRef};

use crate::{
    context::{provider_error, GraphExecutionContext, GraphOperatorRuntime},
    logical::{Direction, EntityKind, GraphLogicalNode, GraphOperator, RelationshipUniqueness},
};

/// Default read runtime backed by the query's pinned [`GraphReadView`].
#[derive(Clone, Copy, Debug, Default)]
pub struct ProviderGraphOperatorRuntime;

impl ProviderGraphOperatorRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl GraphOperatorRuntime for ProviderGraphOperatorRuntime {
    fn execute(
        &self,
        node: Arc<GraphLogicalNode>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        partition: usize,
        task_context: Arc<TaskContext>,
        graph_context: Arc<GraphExecutionContext>,
    ) -> Result<SendableRecordBatchStream> {
        graph_context.validate_operator(&node)?;
        check_query(&graph_context)?;
        let output_schema = Arc::new(node.schema.as_arrow().clone());
        match &node.operator {
            GraphOperator::GraphScan { binding, labels, .. } => {
                if !inputs.is_empty() {
                    return Err(DataFusionError::Plan("GraphScan cannot have physical inputs".into()));
                }
                scan_nodes(binding, labels, output_schema, &graph_context)
            }
            GraphOperator::GraphSeek {
                binding,
                property,
                value_expression,
                internal_id,
                ..
            } => {
                let expression = node
                    .expressions
                    .get(*value_expression as usize)
                    .ok_or_else(|| DataFusionError::Plan("GraphSeek references a missing value expression".into()))?;
                let value = literal_value(expression)?;
                seek_nodes(binding, property, value, *internal_id, output_schema, &graph_context)
            }
            GraphOperator::GraphSeekParameter {
                binding,
                property,
                parameter,
                internal_id,
                ..
            } => {
                let value = graph_context
                    .parameter(parameter)
                    .cloned()
                    .ok_or_else(|| DataFusionError::Plan(format!("GraphSeek parameter ${parameter} was not supplied")))?;
                seek_nodes(binding, property, value, *internal_id, output_schema, &graph_context)
            }
            GraphOperator::Expand {
                source,
                target,
                relationship,
                relationship_type,
                direction,
                uniqueness,
                excluded_relationships,
                ..
            }
            | GraphOperator::ExpandInto {
                source,
                target,
                relationship,
                relationship_type,
                direction,
                uniqueness,
                excluded_relationships,
                ..
            } => {
                let input = one_input(&inputs, partition, task_context)?;
                expansion_stream(
                    input,
                    output_schema,
                    source,
                    target,
                    relationship,
                    relationship_type,
                    *direction,
                    ExpansionDepth::Fixed,
                    PathMode::Endpoints,
                    match uniqueness {
                        RelationshipUniqueness::Path => ProviderUniqueness::Trail,
                        RelationshipUniqueness::None => ProviderUniqueness::Walk,
                    },
                    excluded_relationships,
                    false,
                    Arc::clone(&graph_context),
                )
            }
            GraphOperator::VariableExpand {
                source,
                target,
                relationship,
                relationship_type,
                direction,
                min_hops,
                max_hops,
                uniqueness,
                excluded_relationships,
                ..
            } => {
                let input = one_input(&inputs, partition, task_context)?;
                expansion_stream(
                    input,
                    output_schema,
                    source,
                    target,
                    relationship,
                    relationship_type,
                    *direction,
                    ExpansionDepth::Variable {
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                    },
                    PathMode::Relationships,
                    match uniqueness {
                        RelationshipUniqueness::Path => ProviderUniqueness::Trail,
                        RelationshipUniqueness::None => ProviderUniqueness::Walk,
                    },
                    excluded_relationships,
                    true,
                    Arc::clone(&graph_context),
                )
            }
            GraphOperator::GraphHydrate { binding, properties, .. } => {
                let input = one_input(&inputs, partition, task_context)?;
                hydrate_stream(input, output_schema, binding, properties, Arc::clone(&graph_context))
            }
            GraphOperator::Exists { binding, entity_kind } => {
                let input = one_input(&inputs, partition, task_context)?;
                exists_stream(input, output_schema, binding, *entity_kind, Arc::clone(&graph_context))
            }
            GraphOperator::OptionalGraphApply | GraphOperator::CorrelatedApply => {
                Err(provider_error(GraphProviderError::UnsupportedCapability {
                    capability: "optional/correlated graph apply",
                }))
            }
            GraphOperator::MutationProgram { .. } | GraphOperator::MutationSink { .. } => Err(DataFusionError::Plan(
                "mutation operators must execute through the mutation runtime".into(),
            )),
        }
    }
}

fn one_input(inputs: &[Arc<dyn ExecutionPlan>], partition: usize, task_context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    if inputs.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "graph operator expected one physical input, received {}",
            inputs.len()
        )));
    }
    inputs[0].execute(partition, task_context)
}

fn check_query(context: &GraphExecutionContext) -> Result<()> {
    if context.cancellation().is_cancelled() {
        return Err(DataFusionError::Execution("graph query was cancelled".into()));
    }
    if context.deadline().is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(DataFusionError::Execution("graph query deadline exceeded".into()));
    }
    Ok(())
}

fn page_request(context: &GraphExecutionContext) -> PageRequest {
    let rows = context.budget_limits().max_total_rows;
    let limit = usize::try_from(rows).unwrap_or(usize::MAX).saturating_add(1);
    PageRequest {
        limit: limit.max(1),
        continuation: None,
        order: PageOrder::Unordered,
    }
}

fn scan_nodes(binding: &str, labels: &[String], schema: SchemaRef, context: &GraphExecutionContext) -> Result<SendableRecordBatchStream> {
    let view = context.input_read_view();
    let request = ScanRequest {
        source: ScanSource::Nodes {
            labels: labels.iter().map(|label| Arc::<str>::from(label.as_str())).collect(),
        },
        projection: PropertyProjection::none(),
        predicates: Vec::new(),
        order: Vec::new(),
        fetch: None,
    };
    let page = page_request(context);
    let provider = stream::once(async move {
        let prepared = view.prepare_scan(request).await.map_err(provider_error)?;
        if prepared.acceptance().access_path() != ScanAccessPath::NodeScan {
            return Err(DataFusionError::Execution(
                "prepared node scan selected a non-node access path".into(),
            ));
        }
        let stream = prepared.execute(page).map_err(provider_error)?;
        Ok::<_, DataFusionError>(stream.map(|result| result.map_err(provider_error)).boxed())
    })
    .try_flatten();
    let id_index = schema
        .index_of(binding)
        .map_err(|_| DataFusionError::Plan(format!("GraphScan output is missing binding `{binding}`")))?;
    if schema.fields().len() != 1 || schema.field(id_index).data_type() != &DataType::UInt64 {
        return Err(DataFusionError::Plan(
            "GraphScan output must contain one UInt64 node binding".into(),
        ));
    }
    let output_schema = Arc::clone(&schema);
    let mapped = provider.map(move |result| {
        let batch = result?;
        let ids = batch
            .columns()
            .first()
            .ok_or_else(|| DataFusionError::Execution("node scan returned no node_id column".into()))?;
        RecordBatch::try_new(Arc::clone(&output_schema), vec![Arc::clone(ids)]).map_err(Into::into)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
}

fn seek_nodes(
    binding: &str,
    property: &str,
    value: CypherValue,
    internal_id: bool,
    schema: SchemaRef,
    context: &GraphExecutionContext,
) -> Result<SendableRecordBatchStream> {
    if matches!(value, CypherValue::Null | CypherValue::Missing) {
        return Ok(empty_stream(schema));
    }
    let id_index = schema
        .index_of(binding)
        .map_err(|_| DataFusionError::Plan(format!("GraphSeek output is missing binding `{binding}`")))?;
    if schema.fields().len() != 1 || schema.field(id_index).data_type() != &DataType::UInt64 {
        return Err(DataFusionError::Plan(
            "GraphSeek output must contain one UInt64 node binding".into(),
        ));
    }
    if internal_id {
        let Some(id) = (match value {
            CypherValue::Integer(id) => u64::try_from(id).ok(),
            CypherValue::Float(id) if id.is_finite() && id.fract() == 0.0 && id >= 0.0 && id < 2_f64.powi(64) => {
                let identity = id as u64;
                ((identity as f64) == id).then_some(identity)
            }
            CypherValue::Node(id) => Some(id.0),
            _ => None,
        }) else {
            return Ok(empty_stream(schema));
        };
        let provider = context
            .input_read_view()
            .fetch_nodes(NodeFetchRequest {
                entities: vec![NodeFetchKey {
                    input_ordinal: 0,
                    node_id: NodeId(id),
                }],
                projection: PropertyProjection::none(),
                missing: MissingEntityPolicy::Preserve,
            })
            .map_err(provider_error)?;
        let stream_schema = Arc::clone(&schema);
        let mapped = provider.filter_map(move |result| {
            let schema = Arc::clone(&stream_schema);
            async move {
                let batch = match result {
                    Ok(batch) => batch,
                    Err(error) => return Some(Err(provider_error(error))),
                };
                let exists = match batch.column(2).as_any().downcast_ref::<BooleanArray>() {
                    Some(exists) => exists,
                    None => {
                        return Some(Err(DataFusionError::Execution(
                            "node fetch returned a non-boolean exists column".into(),
                        )))
                    }
                };
                if batch.num_rows() == 0 || !exists.value(0) {
                    return None;
                }
                Some(RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(batch.column(1))]).map_err(Into::into))
            }
        });
        return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)));
    }

    let provider = context
        .input_read_view()
        .seek(SeekSpec {
            index: IndexDescriptor {
                entity: ProviderEntityKind::Node,
                token: Arc::from(""),
                properties: vec![Arc::from(property)],
                unique: false,
            },
            keys: vec![IndexSeekKey {
                input_ordinal: 0,
                values: vec![value],
            }],
            internal_id: false,
            page: page_request(context),
        })
        .map_err(provider_error)?;
    let stream_schema = Arc::clone(&schema);
    let mapped = provider.filter_map(move |result| {
        let schema = Arc::clone(&stream_schema);
        async move {
            let batch = match result {
                Ok(batch) => batch,
                Err(error) => return Some(Err(provider_error(error))),
            };
            let ids = match batch.columns().get(1) {
                Some(ids) => ids,
                None => return Some(Err(DataFusionError::Execution("seek returned no node_id column".into()))),
            };
            let exists = match batch
                .columns()
                .get(2)
                .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            {
                Some(exists) => exists,
                None => return Some(Err(DataFusionError::Execution("seek returned no exists column".into()))),
            };
            let filtered = match filter(ids.as_ref(), exists) {
                Ok(filtered) => filtered,
                Err(error) => return Some(Err(error.into())),
            };
            if filtered.is_empty() {
                None
            } else {
                Some(RecordBatch::try_new(Arc::clone(&schema), vec![filtered]).map_err(Into::into))
            }
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
}

fn literal_value(expression: &Expr) -> Result<CypherValue> {
    let expression = match expression {
        Expr::Alias(alias) => alias.expr.as_ref(),
        expression => expression,
    };
    let Expr::Literal(value, _) = expression else {
        return Err(provider_error(GraphProviderError::UnsupportedCapability {
            capability: "non-literal graph seek expression",
        }));
    };
    use datafusion::common::ScalarValue as Scalar;
    Ok(match value {
        Scalar::Null => CypherValue::Null,
        Scalar::Boolean(value) => value.map_or(CypherValue::Null, CypherValue::Boolean),
        Scalar::Int8(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::Int16(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::Int32(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::Int64(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::UInt8(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::UInt16(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::UInt32(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::UInt64(value) => value.map_or(CypherValue::Null, |value| CypherValue::Integer(i128::from(value))),
        Scalar::Float32(value) => value.map_or(CypherValue::Null, |value| CypherValue::Float(f64::from(value))),
        Scalar::Float64(value) => value.map_or(CypherValue::Null, CypherValue::Float),
        Scalar::Utf8(value) | Scalar::LargeUtf8(value) => value.clone().map_or(CypherValue::Null, CypherValue::String),
        Scalar::Decimal128(value, _, scale) => value.map_or(CypherValue::Null, |value| {
            if *scale == 0 {
                CypherValue::Integer(value)
            } else {
                CypherValue::Float(value as f64 / 10_f64.powi(*scale as i32))
            }
        }),
        _ => {
            return Err(provider_error(GraphProviderError::UnsupportedCapability {
                capability: "graph seek literal type",
            }));
        }
    })
}

fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), stream::empty()))
}

struct ExpansionState {
    input: SendableRecordBatchStream,
    active: Option<(RecordBatch, GraphBatchStream)>,
    source: String,
    target: String,
    relationship: String,
    relationship_type: String,
    direction: Direction,
    depth: ExpansionDepth,
    path_mode: PathMode,
    uniqueness: ProviderUniqueness,
    excluded_relationships: Vec<String>,
    variable: bool,
    output_schema: SchemaRef,
    context: Arc<GraphExecutionContext>,
}

#[allow(clippy::too_many_arguments)] // Mirrors the provider's complete expansion request.
fn expansion_stream(
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    source: &str,
    target: &str,
    relationship: &str,
    relationship_type: &str,
    direction: Direction,
    depth: ExpansionDepth,
    path_mode: PathMode,
    uniqueness: ProviderUniqueness,
    excluded_relationships: &[String],
    variable: bool,
    context: Arc<GraphExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let output_schema_for_stream = Arc::clone(&output_schema);
    let state = ExpansionState {
        input,
        active: None,
        source: source.to_owned(),
        target: target.to_owned(),
        relationship: relationship.to_owned(),
        relationship_type: relationship_type.to_owned(),
        direction,
        depth,
        path_mode,
        uniqueness,
        excluded_relationships: excluded_relationships.to_vec(),
        variable,
        output_schema,
        context,
    };
    let mapped = stream::try_unfold(state, |mut state| async move {
        loop {
            check_query(&state.context)?;
            if let Some((input, provider)) = state.active.as_mut() {
                match provider.try_next().await.map_err(provider_error)? {
                    Some(batch) => {
                        let output = expansion_output_batch(
                            input,
                            &batch,
                            &state.output_schema,
                            &state.source,
                            &state.target,
                            &state.relationship,
                            state.variable,
                        )?;
                        return Ok(Some((output, state)));
                    }
                    None => {
                        state.active = None;
                        continue;
                    }
                }
            }
            let Some(input) = state.input.try_next().await? else {
                return Ok(None);
            };
            let source_index = input
                .schema()
                .index_of(&state.source)
                .map_err(|_| DataFusionError::Plan(format!("expansion input is missing source binding `{}`", state.source)))?;
            let target_index = input.schema().index_of(&state.target).ok();
            let source_ids = input
                .column(source_index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Plan(format!("expansion source binding `{}` must be UInt64", state.source)))?;
            let targets = target_index
                .map(|index| {
                    input
                        .column(index)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| DataFusionError::Plan(format!("expansion target binding `{}` must be UInt64", state.target)))
                })
                .transpose()?;
            let mut expansion_inputs = Vec::with_capacity(input.num_rows());
            for row in 0..input.num_rows() {
                if source_ids.is_null(row) {
                    continue;
                }
                let target_node_id = targets.and_then(|targets| (!targets.is_null(row)).then(|| NodeId(targets.value(row))));
                expansion_inputs.push(ExpansionInput {
                    input_ordinal: row as u64,
                    source_node_id: NodeId(source_ids.value(row)),
                    target_node_id,
                    excluded_relationships: if state.uniqueness == ProviderUniqueness::Trail {
                        relationship_identities_for_row(&input, row, &state.excluded_relationships)?
                    } else {
                        Vec::new()
                    },
                });
            }
            if expansion_inputs.is_empty() {
                continue;
            }
            let request = ExpansionRequest {
                inputs: expansion_inputs,
                constraints: RelationshipConstraints {
                    types: vec![Arc::from(state.relationship_type.as_str())],
                    direction: provider_direction(state.direction),
                },
                depth: state.depth,
                path_mode: state.path_mode,
                uniqueness: state.uniqueness,
                max_results_per_input: None,
            };
            let provider = state.context.input_read_view().expand_batch(request).map_err(provider_error)?;
            state.active = Some((input, provider));
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema_for_stream, mapped)))
}

fn provider_direction(direction: Direction) -> RelationshipDirection {
    match direction {
        Direction::Outgoing => RelationshipDirection::Outgoing,
        Direction::Incoming => RelationshipDirection::Incoming,
        Direction::Undirected => RelationshipDirection::Both,
    }
}

fn relationship_identities_for_row(input: &RecordBatch, row: usize, bindings: &[String]) -> Result<Vec<RelationshipIdentity>> {
    let mut ids = Vec::new();
    for binding in bindings {
        let index = input
            .schema()
            .index_of(binding)
            .map_err(|_| DataFusionError::Plan(format!("trail input is missing relationship binding `{binding}`")))?;
        collect_relationship_identities(input.column(index), row, &mut ids)?;
    }
    Ok(ids)
}

fn collect_relationship_identities(array: &ArrayRef, row: usize, ids: &mut Vec<RelationshipIdentity>) -> Result<()> {
    if array.is_null(row) {
        return Ok(());
    }
    if array.data_type() == &hydradb_graph_types::canonical_value_data_type() {
        let value = value_from_array(array.as_ref(), row).map_err(|error| DataFusionError::Execution(error.to_string()))?;
        return collect_value_relationship_identities(value, ids);
    }
    match array.data_type() {
        DataType::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| DataFusionError::Execution("relationship list binding is not a ListArray".into()))?;
            let items = list.value(row);
            for item in 0..items.len() {
                collect_relationship_identities(&items, item, ids)?;
            }
        }
        DataType::Struct(fields) => {
            if let Some(index) = field_index(fields, "record_id") {
                let relationship = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| DataFusionError::Execution("relationship binding is not a StructArray".into()))?;
                let record_id = array_optional_u64(relationship.column(index), row)?;
                let identity = if let Some(record_id) = record_id {
                    RelationshipIdentity::Identified(RelationshipId(record_id))
                } else {
                    let endpoint = |name: &str| -> Result<NodeId> {
                        let index = field_index(fields, name)
                            .ok_or_else(|| DataFusionError::Execution(format!("structural relationship has no {name}")))?;
                        let id = array_optional_u64(relationship.column(index), row)?
                            .ok_or_else(|| DataFusionError::Execution(format!("structural relationship has null {name}")))?;
                        Ok(NodeId(id))
                    };
                    let type_index = field_index(fields, "relationship_type")
                        .ok_or_else(|| DataFusionError::Execution("structural relationship has no type".into()))?;
                    let types = relationship
                        .column(type_index)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Execution("structural relationship type is not Utf8".into()))?;
                    if types.is_null(row) {
                        return Err(DataFusionError::Execution("structural relationship has null type".into()));
                    }
                    RelationshipIdentity::Structural {
                        source: endpoint("source")?,
                        destination: endpoint("destination")?,
                        relationship_type: types.value(row).into(),
                    }
                };
                if !ids.contains(&identity) {
                    ids.push(identity);
                }
            } else if let Some(index) = field_index(fields, "identity") {
                let value = array.as_any().downcast_ref::<StructArray>().unwrap().column(index);
                collect_relationship_identities(value, row, ids)?;
            } else if let Some(index) = field_index(fields, "relationships") {
                let value = array.as_any().downcast_ref::<StructArray>().unwrap().column(index);
                collect_relationship_identities(value, row, ids)?;
            } else {
                return Err(DataFusionError::Plan("excluded binding is not a relationship or path".into()));
            }
        }
        _ => return Err(DataFusionError::Plan("excluded binding is not a relationship or path".into())),
    }
    Ok(())
}

fn collect_value_relationship_identities(value: CypherValue, ids: &mut Vec<RelationshipIdentity>) -> Result<()> {
    let identities = match value {
        CypherValue::Null | CypherValue::Missing => return Ok(()),
        CypherValue::Relationship(reference) => vec![reference.identity().clone()],
        CypherValue::RelationshipEntity(entity) => vec![entity.identity.identity().clone()],
        CypherValue::Path(path) => path
            .relationships()
            .iter()
            .map(|relationship| relationship.identity().clone())
            .collect(),
        CypherValue::List(values) => {
            for value in values {
                collect_value_relationship_identities(value, ids)?;
            }
            return Ok(());
        }
        _ => return Err(DataFusionError::Plan("excluded binding is not a relationship or path".into())),
    };
    for identity in identities {
        if !ids.contains(&identity) {
            ids.push(identity);
        }
    }
    Ok(())
}

fn expansion_output_batch(
    input: &RecordBatch,
    provider: &RecordBatch,
    output_schema: &SchemaRef,
    source: &str,
    target: &str,
    relationship: &str,
    variable: bool,
) -> Result<RecordBatch> {
    let ordinals = provider
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFusionError::Execution("expansion returned non-UInt64 input ordinals".into()))?;
    let mut input_rows = Vec::with_capacity(provider.num_rows());
    for row in 0..provider.num_rows() {
        let ordinal = ordinals.value(row);
        let ordinal = usize::try_from(ordinal).map_err(|_| DataFusionError::Execution("expansion input ordinal overflow".into()))?;
        if ordinal >= input.num_rows() {
            return Err(DataFusionError::Execution(
                "expansion returned an out-of-range input ordinal".into(),
            ));
        }
        input_rows.push(ordinal as u64);
    }
    let indices = UInt64Array::from(input_rows);
    let mut columns = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        if let Ok(index) = input.schema().index_of(field.name()) {
            columns.push(take(input.column(index).as_ref(), &indices, None)?);
        } else if field.name() == target {
            columns.push(Arc::clone(provider.column(2)));
        } else if field.name() == relationship {
            if variable {
                columns.push(variable_relationship_lists(provider.column(4), field)?);
            } else {
                columns.push(Arc::clone(provider.column(3)));
            }
        } else if field.name() == source {
            let source_index = input
                .schema()
                .index_of(source)
                .map_err(|_| DataFusionError::Plan(format!("expansion input is missing source binding `{source}`")))?;
            columns.push(take(input.column(source_index).as_ref(), &indices, None)?);
        } else {
            return Err(DataFusionError::Plan(format!(
                "expansion output field `{}` has no provider mapping",
                field.name()
            )));
        }
    }
    RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(Into::into)
}

fn variable_relationship_lists(paths: &ArrayRef, field: &Field) -> Result<ArrayRef> {
    let paths = paths
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution("variable expansion path column is not a struct".into()))?;
    let relationships_index = paths
        .fields()
        .iter()
        .position(|field| field.name() == "relationships")
        .ok_or_else(|| DataFusionError::Execution("path column is missing relationships".into()))?;
    let relationships = paths
        .column(relationships_index)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Execution("path relationships column is not a list".into()))?;
    let mut offsets = Vec::with_capacity(paths.len() + 1);
    let mut flat = Vec::new();
    offsets.push(0_i32);
    for row in 0..paths.len() {
        if !paths.is_null(row) {
            let entries = relationships.value(row);
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Execution("path relationship list is not a struct".into()))?;
            for entry in 0..entries.len() {
                flat.push(decode_path_relationship(entries, entry)?);
            }
        }
        offsets.push(i32::try_from(flat.len()).map_err(|_| DataFusionError::Execution("variable path is too large".into()))?);
    }
    let values = Arc::new(value_array(&flat).map_err(|error| DataFusionError::Execution(error.to_string()))?) as ArrayRef;
    let DataType::List(item_field) = field.data_type() else {
        return Err(DataFusionError::Plan(format!(
            "variable relationship binding `{}` is not a list",
            field.name()
        )));
    };
    ListArray::try_new(Arc::clone(item_field), OffsetBuffer::new(ScalarBuffer::from(offsets)), values, None)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(Into::into)
}

fn decode_path_relationship(entries: &StructArray, row: usize) -> Result<CypherValue> {
    let identity_index = entries
        .fields()
        .iter()
        .position(|field| field.name() == "identity")
        .ok_or_else(|| DataFusionError::Execution("path relationship is missing its identity".into()))?;
    let identity = entries
        .column(identity_index)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution("path relationship identity is not a struct".into()))?;
    let kind = array_u8(identity.column(0), row)?;
    let source = array_optional_u64(identity.column(1), row)?;
    let destination = array_optional_u64(identity.column(2), row)?;
    let relationship_type = identity
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .and_then(|array| (!array.is_null(row)).then(|| array.value(row).to_owned()))
        .unwrap_or_default();
    let record_id = array_optional_u64(identity.column(4), row)?;
    let reference = if kind == 1 {
        RelationshipRef::identified(
            RelationshipId(record_id.ok_or_else(|| DataFusionError::Execution("identified path relationship has no id".into()))?),
            NodeId(source.unwrap_or_default()),
            NodeId(destination.unwrap_or_default()),
            relationship_type,
        )
    } else {
        RelationshipRef::structural(
            NodeId(source.ok_or_else(|| DataFusionError::Execution("structural path relationship has no source".into()))?),
            NodeId(destination.ok_or_else(|| DataFusionError::Execution("structural path relationship has no destination".into()))?),
            relationship_type,
        )
    };
    let _orientation = entries
        .fields()
        .iter()
        .position(|field| field.name() == "orientation")
        .map(|index| array_u8(entries.column(index), row))
        .transpose()?;
    Ok(CypherValue::Relationship(reference))
}

fn array_optional_u64(array: &ArrayRef, row: usize) -> Result<Option<u64>> {
    let values = array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFusionError::Execution("expected UInt64 relationship identity field".into()))?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

fn array_u8(array: &ArrayRef, row: usize) -> Result<u8> {
    array
        .as_any()
        .downcast_ref::<UInt8Array>()
        .map(|values| values.value(row))
        .ok_or_else(|| DataFusionError::Execution("expected UInt8 graph discriminator".into()))
}

fn hydrate_stream(
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    binding: &str,
    properties: &[String],
    context: Arc<GraphExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let stream_schema = Arc::clone(&output_schema);
    let binding = binding.to_owned();
    let properties = properties.to_vec();
    let mapped = stream::try_unfold(input, move |mut input| {
        let output_schema = Arc::clone(&output_schema);
        let binding = binding.clone();
        let properties = properties.clone();
        let context = Arc::clone(&context);
        async move {
            check_query(&context)?;
            let Some(batch) = input.try_next().await? else {
                return Ok(None);
            };
            let output = hydrate_batch(&batch, &output_schema, &binding, &properties, &context).await?;
            Ok(Some((output, input)))
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, mapped)))
}

async fn hydrate_batch(
    input: &RecordBatch,
    output_schema: &SchemaRef,
    binding: &str,
    properties: &[String],
    context: &GraphExecutionContext,
) -> Result<RecordBatch> {
    let binding_index = input
        .schema()
        .index_of(binding)
        .map_err(|_| DataFusionError::Plan(format!("GraphHydrate input is missing binding `{binding}`")))?;
    let mut hydrated = HashMap::<String, Vec<CypherValue>>::new();
    for property in properties {
        hydrated.insert(property.clone(), vec![CypherValue::Null; input.num_rows()]);
    }
    let input_schema = input.schema();
    let dtype = input_schema.field(binding_index).data_type();
    if dtype == &DataType::UInt64 {
        let ids = input
            .column(binding_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Plan(format!("GraphHydrate node binding `{binding}` is not UInt64")))?;
        if let Some(values) = hydrated.get_mut("id") {
            for (row, value) in values.iter_mut().enumerate().take(input.num_rows()) {
                if !ids.is_null(row) {
                    *value = CypherValue::Integer(i128::from(ids.value(row)));
                }
            }
        }
        let fetched_properties = properties.iter().filter(|property| property.as_str() != "id").collect::<Vec<_>>();
        let entities = (0..input.num_rows())
            .filter(|row| !ids.is_null(*row))
            .map(|row| NodeFetchKey {
                input_ordinal: row as u64,
                node_id: NodeId(ids.value(row)),
            })
            .collect::<Vec<_>>();
        if !entities.is_empty() && !fetched_properties.is_empty() {
            let provider = context
                .input_read_view()
                .fetch_nodes(NodeFetchRequest {
                    entities,
                    projection: PropertyProjection::keys(
                        fetched_properties
                            .iter()
                            .map(|property| Arc::<str>::from(property.as_str()))
                            .collect(),
                    ),
                    missing: MissingEntityPolicy::Preserve,
                })
                .map_err(provider_error)?;
            let mut provider = provider;
            while let Some(batch) = provider.try_next().await.map_err(provider_error)? {
                let ordinals = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| DataFusionError::Execution("node fetch returned a non-UInt64 input ordinal".into()))?;
                let map = batch.column(4);
                for row in 0..batch.num_rows() {
                    let ordinal = usize::try_from(ordinals.value(row))
                        .map_err(|_| DataFusionError::Execution("node fetch ordinal overflow".into()))?;
                    for property in &fetched_properties {
                        if let Some(value) = property_map_value(map, row, property.as_str())? {
                            hydrated.get_mut(property.as_str()).expect("initialized hydration property")[ordinal] = value;
                        }
                    }
                }
            }
        }
    } else if matches!(dtype, DataType::Struct(_)) {
        let relationships = input
            .column(binding_index)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan(format!("GraphHydrate relationship binding `{binding}` is not a struct")))?;
        let record_id_index = field_index(relationships.fields(), "record_id");
        let properties_index = field_index(relationships.fields(), "properties");
        let record_ids = record_id_index.and_then(|index| relationships.column(index).as_any().downcast_ref::<UInt64Array>());
        let mut entities = Vec::new();
        for row in 0..input.num_rows() {
            if relationships.is_null(row) {
                continue;
            }
            if let Some(record_ids) = record_ids {
                if !record_ids.is_null(row) {
                    entities.push(RelationshipFetchKey {
                        input_ordinal: row as u64,
                        relationship_id: RelationshipId(record_ids.value(row)),
                    });
                    continue;
                }
            }
            if let Some(index) = properties_index {
                for property in properties {
                    if let Some(value) = property_map_value(relationships.column(index), row, property)? {
                        hydrated.get_mut(property).expect("initialized hydration property")[row] = value;
                    }
                }
            }
        }
        if !entities.is_empty() {
            let provider = context
                .input_read_view()
                .fetch_relationships(RelationshipFetchRequest {
                    entities,
                    projection: PropertyProjection::keys(properties.iter().map(|property| Arc::<str>::from(property.as_str())).collect()),
                    missing: MissingEntityPolicy::Preserve,
                })
                .map_err(provider_error)?;
            let mut provider = provider;
            while let Some(batch) = provider.try_next().await.map_err(provider_error)? {
                let ordinals = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| DataFusionError::Execution("relationship fetch returned a non-UInt64 input ordinal".into()))?;
                let rels = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| DataFusionError::Execution("relationship fetch returned a non-struct relationship column".into()))?;
                let prop_index = field_index(rels.fields(), "properties")
                    .ok_or_else(|| DataFusionError::Execution("relationship output has no properties field".into()))?;
                for row in 0..batch.num_rows() {
                    if rels.is_null(row) {
                        continue;
                    }
                    let ordinal = usize::try_from(ordinals.value(row))
                        .map_err(|_| DataFusionError::Execution("relationship fetch ordinal overflow".into()))?;
                    for property in properties {
                        if let Some(value) = property_map_value(rels.column(prop_index), row, property)? {
                            hydrated.get_mut(property).expect("initialized hydration property")[ordinal] = value;
                        }
                    }
                }
            }
        }
    } else {
        return Err(provider_error(GraphProviderError::UnsupportedCapability {
            capability: "hydration of non-entity bindings",
        }));
    }

    let mut columns = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        if let Ok(index) = input.schema().index_of(field.name()) {
            columns.push(Arc::clone(input.column(index)));
        } else if let Some(property) = field.name().strip_prefix(binding).and_then(|name| name.strip_prefix('.')) {
            let property_values = hydrated
                .get(property)
                .ok_or_else(|| DataFusionError::Plan(format!("GraphHydrate output field `{}` was not requested", field.name())))?;
            columns
                .push(Arc::new(value_array(property_values).map_err(|error| DataFusionError::Execution(error.to_string()))?) as ArrayRef);
        } else {
            return Err(DataFusionError::Plan(format!(
                "GraphHydrate output field `{}` has no input or fetched value",
                field.name()
            )));
        }
    }
    RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(Into::into)
}

fn property_map_value(array: &ArrayRef, row: usize, key: &str) -> Result<Option<CypherValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let map = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::MapArray>()
        .ok_or_else(|| DataFusionError::Execution("entity properties column is not a Map".into()))?;
    let offsets = map.value_offsets();
    let entries = map.entries();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("property map keys are not strings".into()))?;
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    for index in start..end {
        if keys.value(index) == key {
            return value_from_array(entries.column(1).as_ref(), index)
                .map(Some)
                .map_err(|error| DataFusionError::Execution(error.to_string()));
        }
    }
    Ok(None)
}

fn field_index(fields: &datafusion::arrow::datatypes::Fields, name: &str) -> Option<usize> {
    fields.iter().position(|field| field.name() == name)
}

fn exists_stream(
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    binding: &str,
    entity_kind: EntityKind,
    context: Arc<GraphExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let stream_schema = Arc::clone(&output_schema);
    let binding = binding.to_owned();
    let mapped = stream::try_unfold(input, move |mut input| {
        let output_schema = Arc::clone(&output_schema);
        let binding = binding.clone();
        let context = Arc::clone(&context);
        async move {
            check_query(&context)?;
            let Some(batch) = input.try_next().await? else {
                return Ok(None);
            };
            let output = exists_batch(&batch, &output_schema, &binding, entity_kind, &context).await?;
            Ok(Some((output, input)))
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, mapped)))
}

async fn exists_batch(
    input: &RecordBatch,
    output_schema: &SchemaRef,
    binding: &str,
    entity_kind: EntityKind,
    context: &GraphExecutionContext,
) -> Result<RecordBatch> {
    let index = input
        .schema()
        .index_of(binding)
        .map_err(|_| DataFusionError::Plan(format!("Exists input is missing binding `{binding}`")))?;
    let mut exists = vec![false; input.num_rows()];
    match entity_kind {
        EntityKind::Node => {
            let ids = input
                .column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Plan(format!("Exists node binding `{binding}` must be UInt64")))?;
            let entities = (0..input.num_rows())
                .filter(|row| !ids.is_null(*row))
                .map(|row| NodeFetchKey {
                    input_ordinal: row as u64,
                    node_id: NodeId(ids.value(row)),
                })
                .collect::<Vec<_>>();
            if !entities.is_empty() {
                let mut provider = context
                    .input_read_view()
                    .fetch_nodes(NodeFetchRequest {
                        entities,
                        projection: PropertyProjection::none(),
                        missing: MissingEntityPolicy::Preserve,
                    })
                    .map_err(provider_error)?;
                while let Some(batch) = provider.try_next().await.map_err(provider_error)? {
                    let ordinals = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| DataFusionError::Execution("node fetch returned a non-UInt64 ordinal".into()))?;
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| DataFusionError::Execution("node fetch returned a non-boolean exists column".into()))?;
                    for row in 0..batch.num_rows() {
                        exists[ordinals.value(row) as usize] = values.value(row);
                    }
                }
            }
        }
        EntityKind::Relationship => {
            let relationships = input
                .column(index)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Plan(format!("Exists relationship binding `{binding}` must be a struct")))?;
            let id_index = field_index(relationships.fields(), "record_id")
                .ok_or_else(|| DataFusionError::Execution("relationship binding lacks record_id".into()))?;
            let ids = relationships
                .column(id_index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Execution("relationship record_id is not UInt64".into()))?;
            let mut entities = Vec::new();
            for (row, exists_value) in exists.iter_mut().enumerate() {
                if relationships.is_null(row) {
                    continue;
                }
                if ids.is_null(row) {
                    *exists_value = true;
                } else {
                    entities.push(RelationshipFetchKey {
                        input_ordinal: row as u64,
                        relationship_id: RelationshipId(ids.value(row)),
                    });
                }
            }
            if !entities.is_empty() {
                let mut provider = context
                    .input_read_view()
                    .fetch_relationships(RelationshipFetchRequest {
                        entities,
                        projection: PropertyProjection::none(),
                        missing: MissingEntityPolicy::Preserve,
                    })
                    .map_err(provider_error)?;
                while let Some(batch) = provider.try_next().await.map_err(provider_error)? {
                    let ordinals = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| DataFusionError::Execution("relationship fetch returned a non-UInt64 ordinal".into()))?;
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| DataFusionError::Execution("relationship fetch returned a non-boolean exists column".into()))?;
                    for row in 0..batch.num_rows() {
                        exists[ordinals.value(row) as usize] = values.value(row);
                    }
                }
            }
        }
    }
    let has_boolean_output = output_schema.fields().iter().any(|field| {
        !input.schema().fields().iter().any(|input_field| input_field.name() == field.name()) && field.data_type() == &DataType::Boolean
    });
    if !has_boolean_output {
        let predicate = BooleanArray::from(exists);
        let filtered = datafusion::arrow::compute::filter_record_batch(input, &predicate)?;
        return project_batch(&filtered, output_schema);
    }
    let mut columns = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        if let Ok(index) = input.schema().index_of(field.name()) {
            columns.push(Arc::clone(input.column(index)));
        } else if field.data_type() == &DataType::Boolean {
            columns.push(Arc::new(BooleanArray::from(exists.clone())) as ArrayRef);
        } else {
            columns.push(new_null_array(field.data_type(), input.num_rows()));
        }
    }
    RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(Into::into)
}

fn project_batch(input: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            input
                .schema()
                .index_of(field.name())
                .map(|index| Arc::clone(input.column(index)))
                .map_err(|_| DataFusionError::Plan(format!("Exists output field `{}` is not available", field.name())))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(Into::into)
}
