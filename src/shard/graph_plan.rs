use crate::QueryFailureReason;
use std::future::Future;
use std::pin::Pin;

use hydradb_graph_plan::{
    BoundValue, GraphPhysicalPlan, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection, ScalarValue, Symbol,
};

use super::*;

#[derive(Clone, Debug)]
struct PlannedVertex {
    id: VertexId,
    metadata: VertexMetadata,
}

#[derive(Clone, Debug, Default)]
struct PlannedRow {
    bindings: BTreeMap<Symbol, PlannedVertex>,
}

#[derive(Clone, Debug)]
enum EvaluatedValue {
    Null,
    Vertex(VertexId),
    Scalar(VertexPropertyValue),
    Truth(Option<bool>),
}

impl GraphShard {
    /// Execute the exact KV plan selected by [`hydradb_graph_plan::plan_physical`].
    ///
    /// This boundary deliberately accepts no logical plan or Cypher AST. The
    /// executor can only run the access operators already present in `plan`,
    /// which keeps execution and `EXPLAIN` on the same physical artifact.
    pub async fn execute_graph_physical_plan(&self, context: QueryContext, plan: GraphPhysicalPlan) -> Result<QueryResultSet> {
        if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "Cypher25",
                feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
            });
        }
        if context.read_epoch.is_none() {
            let snapshot = if context.uses_refreshed_reader() {
                self.storage.strong_snapshot().await?
            } else {
                self.storage.snapshot().await?
            };
            let read_epoch = snapshot.seq();
            let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
            return SlateGraphStore::scope_snapshot(snapshot, Box::pin(self.execute_graph_physical_plan_inner(context, plan))).await;
        }
        Box::pin(self.execute_graph_physical_plan_inner(context, plan)).await
    }

    async fn execute_graph_physical_plan_inner(&self, context: QueryContext, plan: GraphPhysicalPlan) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let read_epoch = context.validated_read_epoch().ok_or_else(|| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "Cypher25",
            feature: "physical plan execution requires a validated storage snapshot".to_string(),
        })?;
        let storage_sequence = context.validated_storage_sequence();
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("graph_physical_plan")?;

        let GraphPhysicalPlan::Project { input, items } = plan else {
            return Err(unsupported_physical_plan(
                "a result-producing physical plan must have Project at its root",
            ));
        };
        let binding_rows = self
            .execute_binding_plan(&context.cell_id, read_epoch, input.as_ref(), &budget)
            .await?;
        self.ensure_graph_plan_rows("graph_physical_intermediate_rows", binding_rows.len())?;

        let columns = items.iter().map(|item| QueryColumn::new(projection_name(item))).collect::<Vec<_>>();
        let mut rows = Vec::with_capacity(binding_rows.len());
        for binding_row in binding_rows {
            budget.check("graph_physical_project")?;
            let values = items
                .iter()
                .map(|item| evaluate_expression(&item.expression, &binding_row).map(evaluated_to_query_value))
                .collect::<Result<Vec<_>>>()?;
            let row = QueryRow::new(values);
            budget.account_result_row(&row)?;
            rows.push(row);
        }

        let skip = usize::try_from(context.result_window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: context.result_window.skip,
            limit: usize::MAX as u64,
        })?;
        let mut rows = rows.into_iter().skip(skip).collect::<Vec<_>>();
        if let Some(limit) = context.result_window.limit {
            ensure_limit("query_result_limit", limit as u64, self.limits.max_query_result_vertices as u64)?;
            rows.truncate(limit);
        } else {
            ensure_limit("query_result_rows", rows.len() as u64, self.limits.max_query_result_vertices as u64)?;
        }

        let result = QueryResultSet::new(columns, rows).with_read_epoch(read_epoch);
        Ok(match storage_sequence {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        })
    }

    fn execute_binding_plan<'a>(
        &'a self,
        cell_id: &'a str,
        read_epoch: StorageSequence,
        plan: &'a GraphPhysicalPlan,
        budget: &'a QueryBudget,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PlannedRow>>> + Send + 'a>> {
        Box::pin(async move {
            budget.check("graph_physical_operator")?;
            match plan {
                GraphPhysicalPlan::VertexPropertySeek {
                    binding,
                    labels,
                    property,
                    value,
                } => {
                    let value = bound_value_to_property(value)?;
                    let vertex_ids = self
                        .scan_vertex_property_index_at(cell_id, property.as_str(), &value, read_epoch, budget)
                        .await?;
                    self.hydrate_planned_vertices(cell_id, read_epoch, binding, labels, vertex_ids, budget)
                        .await
                }
                GraphPhysicalPlan::VertexPropertyMultiSeek {
                    binding,
                    labels,
                    property,
                    values,
                } => {
                    let mut vertex_ids = BTreeSet::new();
                    for value in values {
                        let value = bound_value_to_property(value)?;
                        vertex_ids.extend(
                            self.scan_vertex_property_index_at(cell_id, property.as_str(), &value, read_epoch, budget)
                                .await?,
                        );
                        self.ensure_query_index_candidates("graph_physical_multi_seek_candidates", vertex_ids.len())?;
                    }
                    self.hydrate_planned_vertices(cell_id, read_epoch, binding, labels, vertex_ids.into_iter().collect(), budget)
                        .await
                }
                GraphPhysicalPlan::VertexLabelScan { binding, label } => {
                    let vertex_ids = self.scan_vertex_label_index_at(cell_id, label.as_str(), read_epoch, budget).await?;
                    self.hydrate_planned_vertices(cell_id, read_epoch, binding, std::slice::from_ref(label), vertex_ids, budget)
                        .await
                }
                GraphPhysicalPlan::AllVertexScan { binding } => Err(unsupported_physical_plan(format!(
                    "AllVertexScan for {binding} is not executable until HydraDB has a canonical vertex-universe index"
                ))),
                GraphPhysicalPlan::Filter { input, predicate } => {
                    let rows = self.execute_binding_plan(cell_id, read_epoch, input, budget).await?;
                    let mut filtered = Vec::with_capacity(rows.len());
                    for row in rows {
                        budget.check("graph_physical_filter")?;
                        if matches!(evaluate_expression(predicate, &row)?, EvaluatedValue::Truth(Some(true))) {
                            filtered.push(row);
                        }
                    }
                    self.ensure_graph_plan_rows("graph_physical_filter_rows", filtered.len())?;
                    Ok(filtered)
                }
                GraphPhysicalPlan::Project { .. } => Err(unsupported_physical_plan(
                    "nested Project operators are not supported in the initial executor slice",
                )),
            }
        })
    }

    async fn hydrate_planned_vertices(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        binding: &Symbol,
        labels: &[Symbol],
        vertex_ids: Vec<VertexId>,
        budget: &QueryBudget,
    ) -> Result<Vec<PlannedRow>> {
        self.ensure_query_index_candidates("graph_physical_access_candidates", vertex_ids.len())?;
        let hydrated = self.vertex_metadata_batch_at(cell_id, &vertex_ids, read_epoch, budget).await?;
        let mut rows = Vec::with_capacity(hydrated.len());
        for (id, metadata) in hydrated {
            budget.check("graph_physical_metadata")?;
            if !labels.iter().all(|label| metadata.labels.contains(label.as_str())) {
                continue;
            }
            rows.push(PlannedRow {
                bindings: BTreeMap::from([(binding.clone(), PlannedVertex { id, metadata })]),
            });
        }
        self.ensure_graph_plan_rows("graph_physical_access_rows", rows.len())?;
        Ok(rows)
    }

    fn ensure_graph_plan_rows(&self, operation: &'static str, rows: usize) -> Result<()> {
        ensure_limit(operation, rows as u64, self.limits.max_query_intermediate_rows as u64)
    }
}

fn evaluate_expression(expression: &PhysicalExpression, row: &PlannedRow) -> Result<EvaluatedValue> {
    Ok(match expression {
        PhysicalExpression::Binding(binding) => {
            let vertex = row
                .bindings
                .get(binding)
                .ok_or_else(|| unsupported_physical_plan(format!("unbound vertex variable {binding}")))?;
            EvaluatedValue::Vertex(vertex.id)
        }
        PhysicalExpression::Property { binding, property } => {
            let vertex = row
                .bindings
                .get(binding)
                .ok_or_else(|| unsupported_physical_plan(format!("unbound vertex variable {binding}")))?;
            match vertex.metadata.properties.get(property.as_str()) {
                Some(value) => EvaluatedValue::Scalar(value.clone()),
                None => EvaluatedValue::Null,
            }
        }
        PhysicalExpression::Value(value) => match &value.value {
            ScalarValue::Null => EvaluatedValue::Null,
            _ => EvaluatedValue::Scalar(bound_value_to_property(value)?),
        },
        PhysicalExpression::Binary { left, operator, right } => {
            let left = evaluate_expression(left, row)?;
            let right = evaluate_expression(right, row)?;
            match operator {
                PhysicalBinaryOperator::Equal => EvaluatedValue::Truth(evaluate_equal(left, right)),
                PhysicalBinaryOperator::Or => EvaluatedValue::Truth(evaluate_or(as_truth(left)?, as_truth(right)?)),
            }
        }
    })
}

fn evaluate_equal(left: EvaluatedValue, right: EvaluatedValue) -> Option<bool> {
    match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => None,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right)) => Some(left == right),
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => Some(super::query::vertex_property_values_equal(&left, &right)),
        (EvaluatedValue::Truth(left), EvaluatedValue::Truth(right)) => match (left, right) {
            (Some(left), Some(right)) => Some(left == right),
            _ => None,
        },
        _ => Some(false),
    }
}

fn as_truth(value: EvaluatedValue) -> Result<Option<bool>> {
    match value {
        EvaluatedValue::Truth(value) => Ok(value),
        EvaluatedValue::Scalar(VertexPropertyValue::Bool(value)) => Ok(Some(value)),
        EvaluatedValue::Null => Ok(None),
        _ => Err(unsupported_physical_plan("OR operands must evaluate to booleans")),
    }
}

fn evaluate_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn evaluated_to_query_value(value: EvaluatedValue) -> QueryValue {
    match value {
        EvaluatedValue::Null | EvaluatedValue::Truth(None) => QueryValue::Null,
        EvaluatedValue::Vertex(vertex_id) => QueryValue::VertexId(vertex_id),
        EvaluatedValue::Scalar(value) => QueryValue::Property(value),
        EvaluatedValue::Truth(Some(value)) => QueryValue::Bool(value),
    }
}

fn bound_value_to_property(value: &BoundValue) -> Result<VertexPropertyValue> {
    match &value.value {
        ScalarValue::Null => Err(unsupported_physical_plan("NULL cannot be used as a property-index seek key")),
        ScalarValue::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        ScalarValue::Integer(value) => match u64::try_from(*value) {
            Ok(value) => Ok(VertexPropertyValue::Integer(value)),
            Err(_) => i64::try_from(*value)
                .map(VertexPropertyValue::SignedInteger)
                .map_err(|_| unsupported_physical_plan("integer is outside HydraDB's i64/u64 range")),
        },
        ScalarValue::Float(value) => {
            let parsed = value
                .parse::<f64>()
                .map_err(|error| unsupported_physical_plan(format!("invalid floating-point value {value}: {error}")))?;
            if !parsed.is_finite() {
                return Err(unsupported_physical_plan("non-finite floating-point values are not supported"));
            }
            Ok(VertexPropertyValue::Float(QueryFloat(parsed)))
        }
        ScalarValue::String(value) => Ok(VertexPropertyValue::String(value.to_string())),
    }
}

fn projection_name(projection: &PhysicalProjection) -> String {
    projection
        .alias
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| expression_name(&projection.expression))
}

fn expression_name(expression: &PhysicalExpression) -> String {
    match expression {
        PhysicalExpression::Binding(binding) => binding.to_string(),
        PhysicalExpression::Property { binding, property } => {
            format!("{binding}.{property}")
        }
        PhysicalExpression::Value(value) => match &value.value {
            ScalarValue::Null => "NULL".to_string(),
            ScalarValue::Boolean(value) => value.to_string(),
            ScalarValue::Integer(value) => value.to_string(),
            ScalarValue::Float(value) | ScalarValue::String(value) => value.to_string(),
        },
        PhysicalExpression::Binary { .. } => "expression".to_string(),
    }
}

fn unsupported_physical_plan(feature: impl Into<String>) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Other,
        dialect: "Cypher25",
        feature: feature.into(),
    }
}
