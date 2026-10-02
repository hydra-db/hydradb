use hydradb_cypher_ast::QueryFailureReason;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

use crate::{
    AggregateFunction, BoundValue, GraphPhysicalPlan, PatternDirection, PhysicalBinaryOperator,
    PhysicalExpression, PhysicalSort, PhysicalUnaryOperator, ScalarValue, Symbol,
};

use crate::prepare::{prepare_query, prepare_query_with_statistics};
use crate::{
    scalar_values_equal, EngineError, EngineResult, ExpandDirection, ExpandRequest, GraphRead,
    GraphStorage, OrderedPropertyScanRequest, OrderedScanPosition, ParameterMap, PreparedQuery,
    PropertyBound, PropertyEqualityCandidate, QueryColumn, QueryResult, QueryRow, QueryValue,
    ReadRequest, RelationshipId, RelationshipRecord, SortDirection, VertexId, VertexRecord,
};

#[derive(Clone, Debug, Default)]
struct BindingRow {
    vertices: BTreeMap<Symbol, VertexRecord>,
    relationships: BTreeMap<Symbol, RelationshipRecord>,
}

struct ExpandSpec<'a> {
    from: &'a Symbol,
    relationship: Option<&'a Symbol>,
    relationship_types: &'a [Symbol],
    direction: PatternDirection,
    to: &'a Symbol,
    target_labels: &'a [Symbol],
}

#[derive(Clone)]
struct TraversalState {
    row: BindingRow,
    current: VertexId,
    used_relationships: BTreeSet<RelationshipId>,
}

#[derive(Clone, Debug)]
enum EvaluatedValue {
    Null,
    Vertex(VertexId),
    Relationship(RelationshipId),
    Scalar(ScalarValue),
    Truth(Option<bool>),
}

pub struct CypherEngine<S> {
    storage: S,
}

impl<S> CypherEngine<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub fn prepare(&self, query: &str, parameters: &ParameterMap) -> EngineResult<PreparedQuery> {
        prepare_query(query, parameters)
    }

    /// Preparation with list-valued parameters, which `IN` needs. `prepare`
    /// stays list-free so existing callers are unaffected.
    pub fn prepare_with_lists(
        &self,
        query: &str,
        parameters: &ParameterMap,
        lists: &crate::ListParameterMap,
    ) -> EngineResult<PreparedQuery> {
        prepare_query_with_statistics(query, parameters, lists, &crate::UnknownStatistics)
    }

    /// Lower without selecting a physical plan. This preserves both parameter
    /// maps while a storage adapter loads statistics for the pinned snapshot.
    pub fn lower_with_lists(
        &self,
        query: &str,
        parameters: &ParameterMap,
        lists: &crate::ListParameterMap,
    ) -> EngineResult<crate::LogicalQuery> {
        crate::prepare::lower_query(query, parameters, lists)
    }

    pub fn prepare_with_statistics(
        &self,
        query: &str,
        parameters: &ParameterMap,
        statistics: &dyn crate::StatisticsProvider,
    ) -> EngineResult<PreparedQuery> {
        prepare_query_with_statistics(
            query,
            parameters,
            &crate::ListParameterMap::new(),
            statistics,
        )
    }
}

impl<S: GraphStorage> CypherEngine<S> {
    pub async fn execute(
        &self,
        query: &str,
        parameters: &ParameterMap,
    ) -> EngineResult<QueryResult> {
        let prepared = self.prepare(query, parameters)?;
        self.execute_prepared(&prepared).await
    }

    /// Executes the exact physical artifact held by `prepared` without
    /// re-parsing, re-lowering, or re-planning.
    pub async fn execute_prepared(&self, prepared: &PreparedQuery) -> EngineResult<QueryResult> {
        let mut read = self.storage.begin_read(ReadRequest::default()).await?;
        execute_result_plan(read.as_mut(), &prepared.physical).await
    }
}

// Operator dispatch. Every physical operator invocation enters through one of
// the two `execute_*_plan` wrappers below, which bracket the real dispatch with
// `GraphRead::operator_started`/`operator_finished`. That pair is the whole of
// the executor's profiling surface: operator bodies stay unaware of it, and a
// new operator is observed the moment it is dispatched.

fn execute_result_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
) -> Pin<Box<dyn Future<Output = EngineResult<QueryResult>> + Send + 'a>> {
    Box::pin(async move {
        read.operator_started(plan);
        let result = dispatch_result_plan(&mut *read, plan).await;
        read.operator_finished(plan, result.as_ref().map_or(0, |result| result.rows.len()));
        result
    })
}

fn dispatch_result_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
) -> Pin<Box<dyn Future<Output = EngineResult<QueryResult>> + Send + 'a>> {
    Box::pin(async move {
        let GraphPhysicalPlan::Union { arms, all } = plan else {
            return execute_project(read, plan).await;
        };
        let mut combined: Option<QueryResult> = None;
        for arm in arms {
            let result = execute_result_plan(read, arm).await?;
            if let Some(combined) = &mut combined {
                if combined.columns != result.columns {
                    return Err(unsupported(
                        QueryFailureReason::Union,
                        "all UNION arms must return the same column names",
                    ));
                }
            } else {
                combined = Some(QueryResult {
                    columns: result.columns.clone(),
                    rows: Vec::new(),
                });
            }
            let combined = combined.as_mut().expect("union result initialized");
            for row in result.rows {
                if *all || !combined.rows.contains(&row) {
                    combined.rows.push(row);
                }
            }
        }
        combined.ok_or_else(|| {
            unsupported(QueryFailureReason::Union, "UNION requires at least one arm")
        })
    })
}

async fn execute_project(
    read: &mut dyn GraphRead,
    plan: &GraphPhysicalPlan,
) -> EngineResult<QueryResult> {
    let GraphPhysicalPlan::Project {
        input,
        items,
        distinct,
        post_sort,
        post_skip,
        post_limit,
    } = plan
    else {
        return Err(unsupported(
            QueryFailureReason::Other,
            "a result-producing plan must have Project at its root",
        ));
    };
    let binding_rows = execute_binding_plan(read, input, None).await?;
    let columns = items
        .iter()
        .map(|item| QueryColumn {
            name: item.column_name(),
        })
        .collect::<Vec<_>>();
    let aggregate = items
        .iter()
        .any(|item| expression_contains_aggregate(&item.expression));
    let mut projected = if aggregate {
        aggregate_project_rows(&binding_rows, items, post_sort)?
    } else {
        binding_rows
            .iter()
            .map(|binding_row| {
                let values = items
                    .iter()
                    .map(|item| {
                        evaluate_expression(&item.expression, binding_row).map(to_query_value)
                    })
                    .collect::<EngineResult<Vec<_>>>()?;
                Ok((QueryRow { values }, Vec::new()))
            })
            .collect::<EngineResult<Vec<_>>>()?
    };
    if *distinct {
        let mut unique = Vec::with_capacity(projected.len());
        for row in projected {
            if !unique
                .iter()
                .any(|(existing, _): &(QueryRow, Vec<QueryValue>)| existing == &row.0)
            {
                unique.push(row);
            }
        }
        projected = unique;
    }
    if aggregate && !post_sort.is_empty() {
        projected
            .sort_by(|(_, left), (_, right)| compare_projected_sort_keys(left, right, post_sort));
    }
    let skip = post_skip
        .as_ref()
        .map(|count| window_count(count, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = post_limit
        .as_ref()
        .map(|count| window_count(count, "LIMIT"))
        .transpose()?;
    let mut rows = projected
        .into_iter()
        .skip(skip)
        .map(|(row, _)| row)
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(QueryResult { columns, rows })
}

fn expression_contains_aggregate(expression: &PhysicalExpression) -> bool {
    match expression {
        PhysicalExpression::Aggregate { .. } => true,
        PhysicalExpression::InList { expression, .. } => expression_contains_aggregate(expression),
        PhysicalExpression::Unary { expression, .. } => expression_contains_aggregate(expression),
        PhysicalExpression::Binary { left, right, .. } => {
            expression_contains_aggregate(left) || expression_contains_aggregate(right)
        }
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_) => false,
    }
}

fn aggregate_project_rows(
    rows: &[BindingRow],
    items: &[crate::PhysicalProjection],
    sort: &[PhysicalSort],
) -> EngineResult<Vec<(QueryRow, Vec<QueryValue>)>> {
    let mut grouping = Vec::new();
    for item in items {
        collect_grouping_expressions(&item.expression, &mut grouping);
    }
    let mut groups: Vec<(Vec<QueryValue>, Vec<&BindingRow>)> = Vec::new();
    for row in rows {
        let key = grouping
            .iter()
            .map(|expression| evaluate_expression(expression, row).map(to_query_value))
            .collect::<EngineResult<Vec<_>>>()?;
        if let Some((_, grouped)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            grouped.push(row);
        } else {
            groups.push((key, vec![row]));
        }
    }
    if groups.is_empty() && grouping.is_empty() {
        groups.push((Vec::new(), Vec::new()));
    }

    groups
        .into_iter()
        .map(|(_, group)| {
            let values = items
                .iter()
                .map(|item| evaluate_group_expression(&item.expression, &group))
                .collect::<EngineResult<Vec<_>>>()?;
            let sort_values = sort
                .iter()
                .map(|item| evaluate_group_expression(&item.expression, &group))
                .collect::<EngineResult<Vec<_>>>()?;
            Ok((QueryRow { values }, sort_values))
        })
        .collect()
}

fn collect_grouping_expressions<'a>(
    expression: &'a PhysicalExpression,
    grouping: &mut Vec<&'a PhysicalExpression>,
) {
    if matches!(expression, PhysicalExpression::Aggregate { .. }) {
        return;
    }
    if !expression_contains_aggregate(expression) {
        if expression_requires_row(expression) && !grouping.contains(&expression) {
            grouping.push(expression);
        }
        return;
    }
    match expression {
        PhysicalExpression::Unary { expression, .. } => {
            collect_grouping_expressions(expression, grouping);
        }
        PhysicalExpression::Binary { left, right, .. } => {
            collect_grouping_expressions(left, grouping);
            collect_grouping_expressions(right, grouping);
        }
        // Only the probe can carry an aggregate; the candidate list is
        // constant and never contributes a grouping key.
        PhysicalExpression::InList { expression, .. } => {
            collect_grouping_expressions(expression, grouping);
        }
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_)
        | PhysicalExpression::Aggregate { .. } => {}
    }
}

fn expression_requires_row(expression: &PhysicalExpression) -> bool {
    match expression {
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Property { .. } => true,
        PhysicalExpression::Unary { expression, .. } => expression_requires_row(expression),
        PhysicalExpression::Binary { left, right, .. } => {
            expression_requires_row(left) || expression_requires_row(right)
        }
        // The candidate list is constant, so only the tested expression can
        // make an `IN` row-dependent.
        PhysicalExpression::InList { expression, .. } => expression_requires_row(expression),
        PhysicalExpression::Value(_) | PhysicalExpression::Aggregate { .. } => false,
    }
}

fn evaluate_group_expression(
    expression: &PhysicalExpression,
    rows: &[&BindingRow],
) -> EngineResult<QueryValue> {
    match expression {
        PhysicalExpression::Aggregate {
            function,
            expression,
        } => evaluate_aggregate(*function, expression.as_deref(), rows),
        PhysicalExpression::Unary {
            operator,
            expression,
        } => {
            let value = evaluate_group_expression(expression, rows)?;
            evaluate_unary(*operator, query_value_to_evaluated(value)?).map(to_query_value)
        }
        PhysicalExpression::Binary {
            left,
            operator,
            right,
        } => {
            let left = evaluate_group_expression(left, rows)?;
            let right = evaluate_group_expression(right, rows)?;
            evaluate_binary(
                *operator,
                query_value_to_evaluated(left)?,
                query_value_to_evaluated(right)?,
            )
            .map(to_query_value)
        }
        PhysicalExpression::Value(_) => {
            evaluate_expression(expression, &BindingRow::default()).map(to_query_value)
        }
        _ => rows
            .first()
            .ok_or_else(|| {
                unsupported(
                    QueryFailureReason::Evaluation,
                    "grouping expression has no input row",
                )
            })
            .and_then(|row| evaluate_expression(expression, row))
            .map(to_query_value),
    }
}

fn evaluate_aggregate(
    function: AggregateFunction,
    expression: Option<&PhysicalExpression>,
    rows: &[&BindingRow],
) -> EngineResult<QueryValue> {
    let values = match expression {
        Some(expression) => rows
            .iter()
            .map(|row| evaluate_expression(expression, row).map(to_query_value))
            .collect::<EngineResult<Vec<_>>>()?,
        None => Vec::new(),
    };
    match function {
        AggregateFunction::Count => Ok(QueryValue::Count(match expression {
            None => rows.len() as u64,
            Some(_) => values
                .iter()
                .filter(|value| !matches!(value, QueryValue::Null))
                .count() as u64,
        })),
        AggregateFunction::Collect => Ok(QueryValue::List(
            values
                .into_iter()
                .filter(|value| !matches!(value, QueryValue::Null))
                .collect(),
        )),
        AggregateFunction::Sum | AggregateFunction::Average => aggregate_numeric(function, &values),
    }
}

fn aggregate_numeric(
    function: AggregateFunction,
    values: &[QueryValue],
) -> EngineResult<QueryValue> {
    let mut integer_sum = 0_i128;
    let mut float_sum = 0_f64;
    let mut has_float = false;
    let mut count = 0_u64;
    for value in values {
        match value {
            QueryValue::Null => continue,
            QueryValue::Scalar(ScalarValue::Integer(value)) if !has_float => {
                integer_sum = integer_sum.checked_add(*value).ok_or_else(|| {
                    unsupported(
                        QueryFailureReason::Evaluation,
                        "aggregate integer sum overflowed",
                    )
                })?;
            }
            QueryValue::Scalar(ScalarValue::Integer(value)) => float_sum += *value as f64,
            QueryValue::Scalar(ScalarValue::Float(value)) => {
                if !has_float {
                    float_sum = integer_sum as f64;
                    has_float = true;
                }
                float_sum += value.parse::<f64>().map_err(|_| {
                    unsupported(
                        QueryFailureReason::Evaluation,
                        "aggregate contains an invalid float",
                    )
                })?;
            }
            _ => {
                return Err(unsupported(
                    QueryFailureReason::Evaluation,
                    "sum and avg require numeric values",
                ))
            }
        }
        count += 1;
    }
    if count == 0 {
        return Ok(QueryValue::Null);
    }
    if function == AggregateFunction::Average {
        let total = if has_float {
            float_sum
        } else {
            integer_sum as f64
        };
        return Ok(QueryValue::Scalar(ScalarValue::Float(
            (total / count as f64).to_string().into(),
        )));
    }
    if has_float {
        Ok(QueryValue::Scalar(ScalarValue::Float(
            float_sum.to_string().into(),
        )))
    } else {
        Ok(QueryValue::Scalar(ScalarValue::Integer(integer_sum)))
    }
}

fn compare_projected_sort_keys(
    left: &[QueryValue],
    right: &[QueryValue],
    items: &[PhysicalSort],
) -> Ordering {
    for ((left, right), item) in left.iter().zip(right).zip(items) {
        let ordering = compare_query_values(left, right);
        let ordering = match item.direction {
            crate::SortDirection::Ascending => ordering,
            crate::SortDirection::Descending => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_query_values(left: &QueryValue, right: &QueryValue) -> Ordering {
    match (left, right) {
        (QueryValue::Null, QueryValue::Null) => Ordering::Equal,
        (QueryValue::Null, _) => Ordering::Greater,
        (_, QueryValue::Null) => Ordering::Less,
        (QueryValue::VertexId(left), QueryValue::VertexId(right))
        | (QueryValue::RelationshipId(left), QueryValue::RelationshipId(right))
        | (QueryValue::Count(left), QueryValue::Count(right)) => left.cmp(right),
        (QueryValue::Scalar(left), QueryValue::Scalar(right)) => compare_scalar_values(left, right),
        (QueryValue::List(left), QueryValue::List(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_query_values(left, right))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        _ => query_value_rank(left).cmp(&query_value_rank(right)),
    }
}

fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::VertexId(_) => 0,
        QueryValue::RelationshipId(_) => 1,
        QueryValue::Count(_) => 2,
        QueryValue::Scalar(_) => 3,
        QueryValue::List(_) => 4,
        QueryValue::Null => u8::MAX,
    }
}

fn execute_binding_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    execute_binding_plan_seeded(read, plan, row_limit, None)
}

fn execute_binding_plan_seeded<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        read.operator_started(plan);
        let result = dispatch_binding_plan(&mut *read, plan, row_limit, seed).await;
        read.operator_finished(plan, result.as_ref().map_or(0, Vec::len));
        result
    })
}

fn dispatch_binding_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        if row_limit == Some(0) {
            return Ok(Vec::new());
        }
        read.checkpoint("cypher_execute_operator")?;
        match plan {
            GraphPhysicalPlan::Union { .. } => Err(unsupported(
                QueryFailureReason::Union,
                "Union cannot be nested below a binding operator",
            )),
            GraphPhysicalPlan::VertexIdSeek {
                binding,
                labels,
                value,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let id = vertex_id_value(value)?;
                let rows = hydrate_rows(read, binding, labels, vec![id], row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::VertexPropertySeek {
                binding,
                labels,
                property,
                value,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let value = seek_value(value)?;
                let ids = read
                    .seek_vertices_by_property(property.as_str(), value)
                    .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::VertexPropertyMultiSeek {
                binding,
                labels,
                property,
                values,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let unique_values = values
                    .iter()
                    .map(seek_value)
                    .collect::<EngineResult<BTreeSet<_>>>()?
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                // One batched request rather than one seek per value: the
                // storage adapter can overlap the index lookups, which is the
                // difference between N round trips and one on a remote store.
                let ids = read
                    .seek_vertices_by_property_values(property.as_str(), &unique_values)
                    .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::VertexPropertyScan {
                binding,
                labels,
                property,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let ids = read.scan_vertices_by_property(property.as_str()).await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::OrderedVertexPropertyScan {
                binding,
                labels,
                property,
                direction,
                prefix,
                lower,
                upper,
                required,
                items,
                residual,
            } => {
                // A tie key the index stores is satisfied by arrival order, so
                // the walk can stop the moment it has enough rows. Any other
                // tie key reorders within a primary value, so the walk must
                // first read that value out -- and must not let the backend
                // drop any of it.
                let ties_by_other_key = items.get(1).is_some_and(|item| {
                    !matches!(&item.expression, PhysicalExpression::Identity(tie) if tie == binding)
                });
                let mut request = OrderedPropertyScanRequest {
                    property: property.to_string(),
                    labels: labels.iter().map(ToString::to_string).collect(),
                    ascending: *direction == SortDirection::Ascending,
                    // With no explicit tie key preserve the native scan's
                    // stable ID order, including descending primary scans.
                    id_ascending: items
                        .get(1)
                        .map_or(*direction == SortDirection::Ascending, |item| {
                            item.direction == SortDirection::Ascending
                        }),
                    prefix: prefix
                        .as_ref()
                        .map(|value| bound_string(value, "STARTS WITH"))
                        .transpose()?,
                    lower: lower
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: bound_string_value(value, "lower bound")?,
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()?,
                    upper: upper
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: bound_string_value(value, "upper bound")?,
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()?,
                    // The enclosing window fixed `required`; an outer limit can
                    // only make it smaller.
                    required: row_limit.map_or(*required, |limit| limit.min(*required)),
                    after: None,
                    // Both reasons the backend's own top-of-page selection
                    // would be wrong for this operator: it cannot see the
                    // residual that may reject everything it kept, and it
                    // orders ties by vertex ID rather than by the tie key
                    // this walk was allowed to carry.
                    index_order: residual.is_some() || ties_by_other_key,
                };
                // Whether this walk has to read a primary value's whole run
                // before it can rank within it. An `index_order` page arrives
                // in the index's own order, which ranks ties by vertex ID in
                // the scan's direction; any other tie order -- another key, or
                // IDs the other way round -- needs the run read out first.
                // Retention stays bounded regardless: the walk keeps only the
                // best `target` survivors as it goes.
                let drains_ties = request.index_order
                    && (ties_by_other_key || request.id_ascending != request.ascending);
                // A correlated seed already holds the vertex: the removed
                // Filter must still be honored, so check it here instead of
                // trusting the label-only seed shortcut.
                if let Some(vertex) = seed.and_then(|seed| seed.vertices.get(binding)) {
                    let accepted = labels
                        .iter()
                        .all(|label| vertex.labels.contains(label.as_str()))
                        && matches!(
                            vertex.properties.get(request.property.as_str()),
                            Some(ScalarValue::String(value)) if request.accepts(value)
                        )
                        && residual_accepts(residual.as_ref(), binding, vertex)?;
                    return Ok(accepted
                        .then(|| seed.cloned())
                        .flatten()
                        .into_iter()
                        .collect());
                }
                if request.required == 0 {
                    return Ok(Vec::new());
                }
                // A residual rejects rows the index happily returned, and a
                // rejected row does not count against `required`. So one page
                // can come back with too few survivors, and stopping there
                // would answer with fewer rows than the query asked for --
                // quietly, since nothing about it is an error. Keep asking.
                let target = request.required;
                let mut rows: Vec<BindingRow> = Vec::new();
                // While draining, `rows` is kept ranked by `items`, and these
                // are its sort keys, index for index.
                let mut ranked_keys: Vec<Vec<EvaluatedValue>> = Vec::new();
                let mut page = request.required;
                let mut examined = 0usize;
                let mut probed = false;
                loop {
                    request.required = page;
                    let scanned = read.scan_vertices_by_property_ordered(&request).await?;
                    let exhausted = scanned.exhausted;
                    request.after = scanned.last.clone();
                    let produced = scanned.vertices.len();
                    let kept_before = rows.len();
                    for vertex in scanned.vertices {
                        // Rows arrive in rank order unless the walk is
                        // draining, so once the window is full nothing later
                        // in the page can enter it.
                        if !drains_ties && rows.len() >= target {
                            break;
                        }
                        read.checkpoint("cypher_ordered_scan_vertex")?;
                        // Defensive re-check: a backend that returns a superset is
                        // still correct, one that returns a wrong row is not.
                        if !labels
                            .iter()
                            .all(|label| vertex.labels.contains(label.as_str()))
                            || !matches!(
                                vertex.properties.get(request.property.as_str()),
                                Some(ScalarValue::String(value)) if request.accepts(value)
                            )
                        {
                            continue;
                        }
                        // The scan replaced a Filter, so this is what keeps the
                        // result correct. A rejected row is not a row: it must not
                        // count against `required`, which is why the walk has to
                        // keep reading rather than stop at a full page.
                        if !residual_accepts(residual.as_ref(), binding, &vertex)? {
                            continue;
                        }
                        let row = BindingRow {
                            vertices: BTreeMap::from([(binding.clone(), vertex)]),
                            relationships: BTreeMap::new(),
                        };
                        // Draining a run ranks within it, so only the best
                        // `target` survivors can matter. Each one either takes
                        // its place among them or is dropped on arrival: the
                        // run can be the whole index, and even one row past the
                        // window would fail a window sized at the row budget.
                        if drains_ties {
                            insert_ranked(&mut rows, &mut ranked_keys, row, items, target)?;
                        } else {
                            rows.push(row);
                        }
                        read.check_intermediate_rows(
                            "cypher_ordered_scan_intermediate_rows",
                            rows.len(),
                        )?;
                    }
                    examined = examined.saturating_add(produced);
                    if exhausted || produced == 0 {
                        break;
                    }
                    if rows.len() >= target {
                        if !drains_ties
                            || tie_boundary_passed(
                                &rows,
                                binding,
                                request.property.as_str(),
                                target,
                                request.after.as_ref(),
                            )
                        {
                            break;
                        }
                        // Only rows still sharing the boundary value can change
                        // the answer now, and on a distinct property there are
                        // none. Ask for the smallest page rather than letting
                        // the miss-rate growth below widen one.
                        page = target;
                        continue;
                    }
                    // The walk has read far past what it was asked for and is
                    // still short, so the residual rather than the window is
                    // deciding this query. One of its equalities may name only
                    // a handful of vertices, and seeking those beats walking
                    // whatever is left. Ask the storage probe, which answers
                    // `None` once every equality overflows its cap -- the same
                    // evidence that no seek is narrow enough to be worth
                    // preferring. That answer cannot change as the walk goes
                    // on, so ask once.
                    if !probed && examined >= target.saturating_mul(ORDERED_PROBE_TRIGGER) {
                        probed = true;
                        if let Some(residual) = residual.as_ref() {
                            let candidates = equality_candidates(residual, binding);
                            if !candidates.is_empty() {
                                read.checkpoint("cypher_ordered_scan_probe")?;
                                if let Some(ids) =
                                    read.probe_selective_equality(&candidates).await?
                                {
                                    // The probe answers for a whole conjunct,
                                    // so its vertices are a superset of every
                                    // row the residual can accept, including
                                    // the ones already collected. Answering
                                    // from it alone is therefore complete, and
                                    // simpler than reconciling two partial
                                    // walks of the same population.
                                    let hydrated = hydrate_rows(
                                        read,
                                        binding,
                                        labels,
                                        ids,
                                        None,
                                        Some(residual),
                                    )
                                    .await?;
                                    // Hydration applied the labels and the
                                    // residual; the window bounds the scan
                                    // would have enforced are still owed.
                                    let within_window = hydrated
                                        .into_iter()
                                        .filter(|row| {
                                            row.vertices.get(binding).is_some_and(|vertex| {
                                                matches!(
                                                    vertex
                                                        .properties
                                                        .get(request.property.as_str()),
                                                    Some(ScalarValue::String(value))
                                                        if request.accepts(value)
                                                )
                                            })
                                        })
                                        .collect();
                                    let mut answer = sort_rows(within_window, items)?;
                                    truncate_to_limit(&mut answer, Some(target));
                                    return merge_seed_rows(answer, seed);
                                }
                            }
                        }
                    }
                    // A window the residual barely matches would otherwise cost
                    // a round trip per surviving row. Grow with the miss rate,
                    // and cap it so a window it never matches cannot ask for
                    // the whole index in one page.
                    let kept = rows.len() - kept_before;
                    page = if kept == 0 {
                        page.saturating_mul(ORDERED_PAGE_GROWTH)
                    } else {
                        page.saturating_mul(produced).saturating_div(kept.max(1))
                    }
                    .clamp(target, target.saturating_mul(ORDERED_PAGE_CEILING));
                }
                let mut rows = sort_rows(rows, items)?;
                truncate_to_limit(&mut rows, Some(target));
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::VertexLabelScan {
                binding,
                label,
                labels,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                // A limit cannot stop the anchor scan early when additional
                // labels still need in-memory validation: the first N anchor
                // rows may all fail the remaining labels.
                let scan_limit = (labels.len() == 1).then_some(row_limit).flatten();
                let ids = read
                    .scan_vertices_by_label(label.as_str(), scan_limit)
                    .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::AllVertexScan { binding } => {
                if let Some(row) = bound_seed_row(seed, binding, &[]) {
                    return Ok(row);
                }
                let ids = read.scan_all_vertices().await?;
                let rows = hydrate_rows(read, binding, &[], ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::RelationshipPropertySeek {
                from,
                source_labels,
                relationship,
                relationship_type,
                property,
                value,
                direction,
                to,
                target_labels,
            } => {
                let value = seek_value(value)?;
                let relationships = read
                    .seek_relationships_by_property(
                        relationship_type.as_str(),
                        property.as_str(),
                        value,
                    )
                    .await?;
                relationship_seek_rows(
                    read,
                    relationships,
                    from,
                    source_labels,
                    relationship,
                    relationship_type,
                    *direction,
                    to,
                    target_labels,
                    row_limit,
                    seed,
                )
                .await
            }
            GraphPhysicalPlan::Expand {
                input,
                from,
                relationship,
                relationship_types,
                direction,
                to,
                target_labels,
            } => {
                // An input row can expand to zero rows, so limiting the input
                // would be semantically incorrect. Bound the expansion output
                // instead.
                let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
                expand_rows(
                    read,
                    rows,
                    ExpandSpec {
                        from,
                        relationship: relationship.as_ref(),
                        relationship_types,
                        direction: *direction,
                        to,
                        target_labels,
                    },
                    row_limit,
                )
                .await
            }
            GraphPhysicalPlan::VariableExpand {
                input,
                from,
                relationship_types,
                direction,
                to,
                target_labels,
                min_hops,
                max_hops,
            } => {
                let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
                variable_expand_rows(
                    read,
                    rows,
                    from,
                    relationship_types,
                    *direction,
                    to,
                    target_labels,
                    *min_hops,
                    *max_hops,
                    row_limit,
                )
                .await
            }
            GraphPhysicalPlan::NaturalJoin {
                left,
                right,
                optional,
            } => {
                let left_rows = execute_binding_plan_seeded(read, left, None, seed).await?;
                let mut joined = Vec::new();
                for left_row in left_rows {
                    read.checkpoint("cypher_natural_join_left")?;
                    let right_rows =
                        execute_binding_plan_seeded(read, right, None, Some(&left_row)).await?;
                    if *optional && right_rows.is_empty() {
                        joined.push(left_row);
                    } else {
                        joined.extend(right_rows);
                    }
                    read.check_intermediate_rows("cypher_join_intermediate_rows", joined.len())?;
                    if let Some(limit) = row_limit {
                        if joined.len() >= limit {
                            joined.truncate(limit);
                            return Ok(joined);
                        }
                    }
                }
                Ok(joined)
            }
            GraphPhysicalPlan::Filter { input, predicate } => {
                // The planner commits to an access path from predicate order
                // alone, so a broad property can anchor a query that a second,
                // narrow one would have answered directly. Offer the backend
                // every equality on the binding and let a bounded probe
                // decide. Correlated execution keeps the planned path: `seed`
                // already fixes the vertex, so there is nothing to narrow.
                if seed.is_none() {
                    if let Some((binding, labels)) = probe_replaceable_access(input) {
                        let candidates = equality_candidates(predicate, binding);
                        if distinct_properties(&candidates) >= 2 {
                            read.checkpoint("cypher_equality_probe")?;
                            if let Some(ids) = read.probe_selective_equality(&candidates).await? {
                                // The probe proved one conjunct covers these
                                // vertices. Labels and the whole predicate are
                                // still applied while they are hydrated, so
                                // this replaces the Filter rather than
                                // weakening it.
                                return hydrate_rows(
                                    read,
                                    binding,
                                    labels,
                                    ids,
                                    row_limit,
                                    Some(predicate),
                                )
                                .await;
                            }
                        }
                    }
                }
                // A filter may discard any prefix, so it must consume an
                // unbounded child. It can still stop materializing its own
                // output once a parent LIMIT is satisfied.
                let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
                let mut filtered = Vec::new();
                for row in rows {
                    read.checkpoint("cypher_filter_rows")?;
                    match evaluate_expression(predicate, &row)? {
                        EvaluatedValue::Truth(Some(true)) => {
                            filtered.push(row);
                            read.check_intermediate_rows(
                                "cypher_filter_intermediate_rows",
                                filtered.len(),
                            )?;
                            if row_limit.is_some_and(|limit| filtered.len() >= limit) {
                                break;
                            }
                        }
                        EvaluatedValue::Truth(Some(false) | None) => {}
                        _ => {
                            return Err(unsupported(
                                QueryFailureReason::Evaluation,
                                "Filter predicate did not evaluate to a boolean",
                            ));
                        }
                    }
                }
                Ok(filtered)
            }
            GraphPhysicalPlan::Sort { input, items, .. } => {
                // Sorting needs the complete input to identify the first row.
                let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
                let mut rows = sort_rows(rows, items)?;
                truncate_to_limit(&mut rows, row_limit);
                read.check_intermediate_rows("cypher_sort_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Skip { input, count } => {
                let count = window_count(count, "SKIP")?;
                let child_limit = row_limit
                    .map(|limit| {
                        count.checked_add(limit).ok_or_else(|| {
                            unsupported(
                                QueryFailureReason::OrderWindow,
                                "SKIP plus LIMIT exceeds the supported usize range",
                            )
                        })
                    })
                    .transpose()?;
                let rows = execute_binding_plan_seeded(read, input, child_limit, seed).await?;
                let mut rows = rows.into_iter().skip(count).collect::<Vec<_>>();
                truncate_to_limit(&mut rows, row_limit);
                read.check_intermediate_rows("cypher_skip_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Limit { input, count } => {
                let count = window_count(count, "LIMIT")?;
                let effective_limit = Some(row_limit.map_or(count, |outer| outer.min(count)));
                let mut rows =
                    execute_binding_plan_seeded(read, input, effective_limit, seed).await?;
                truncate_to_limit(&mut rows, effective_limit);
                read.check_intermediate_rows("cypher_limit_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Project { .. } => Err(unsupported(
                QueryFailureReason::Return,
                "nested Project is outside the initial engine slice",
            )),
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn relationship_seek_rows(
    read: &mut dyn GraphRead,
    relationships: Vec<RelationshipRecord>,
    from: &Symbol,
    source_labels: &[Symbol],
    relationship_binding: &Symbol,
    relationship_type: &Symbol,
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
    row_limit: Option<usize>,
    seed: Option<&BindingRow>,
) -> EngineResult<Vec<BindingRow>> {
    let relationships = canonical_relationships(read, relationships)?;
    let vertex_ids = relationships
        .values()
        .flat_map(|relationship| [relationship.source, relationship.target])
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let vertices = read
        .hydrate_vertices(&vertex_ids)
        .await?
        .into_iter()
        .map(|vertex| (vertex.id, vertex))
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();
    for relationship in relationships.into_values() {
        read.checkpoint("cypher_relationship_property_seek")?;
        if relationship.relationship_type != relationship_type.as_str() {
            continue;
        }
        // Neither endpoint is bound on this path, so an undirected hop matches
        // the edge under both assignments and yields both rows. A self-loop is
        // the exception: the two assignments are the same row.
        let orientations: &[(VertexId, VertexId)] = match direction {
            PatternDirection::Outgoing => &[(relationship.source, relationship.target)],
            PatternDirection::Incoming => &[(relationship.target, relationship.source)],
            PatternDirection::Undirected if relationship.source == relationship.target => {
                &[(relationship.source, relationship.target)]
            }
            PatternDirection::Undirected => &[
                (relationship.source, relationship.target),
                (relationship.target, relationship.source),
            ],
        };
        let mut limit_reached = false;
        for &(source_id, target_id) in orientations {
            let (Some(source), Some(target)) = (vertices.get(&source_id), vertices.get(&target_id))
            else {
                continue;
            };
            if !source_labels
                .iter()
                .all(|label| source.labels.contains(label.as_str()))
                || !target_labels
                    .iter()
                    .all(|label| target.labels.contains(label.as_str()))
            {
                continue;
            }
            let row = BindingRow {
                vertices: BTreeMap::from([
                    (from.clone(), source.clone()),
                    (to.clone(), target.clone()),
                ]),
                relationships: BTreeMap::from([(
                    relationship_binding.clone(),
                    relationship.clone(),
                )]),
            };
            let Some(row) =
                seed.map_or(Some(row.clone()), |seed| merge_compatible_rows(seed, &row))
            else {
                continue;
            };
            rows.push(row);
            read.check_intermediate_rows("cypher_relationship_property_seek_rows", rows.len())?;
            if row_limit.is_some_and(|limit| rows.len() >= limit) {
                limit_reached = true;
                break;
            }
        }
        if limit_reached {
            break;
        }
    }
    Ok(rows)
}

fn bound_seed_row(
    seed: Option<&BindingRow>,
    binding: &Symbol,
    labels: &[Symbol],
) -> Option<Vec<BindingRow>> {
    let seed = seed?;
    let vertex = seed.vertices.get(binding)?;
    Some(
        labels
            .iter()
            .all(|label| vertex.labels.contains(label.as_str()))
            .then(|| seed.clone())
            .into_iter()
            .collect(),
    )
}

fn merge_seed_rows(
    rows: Vec<BindingRow>,
    seed: Option<&BindingRow>,
) -> EngineResult<Vec<BindingRow>> {
    let Some(seed) = seed else {
        return Ok(rows);
    };
    Ok(rows
        .into_iter()
        .filter_map(|row| merge_compatible_rows(seed, &row))
        .collect())
}

fn truncate_to_limit<T>(rows: &mut Vec<T>, row_limit: Option<usize>) {
    if let Some(limit) = row_limit {
        rows.truncate(limit);
    }
}

/// Insert `row` into `rows`, which holds at most `limit` rows ranked by
/// `items`, with `keys` holding their sort keys index for index.
///
/// Equal keys keep arrival order and a row that ties the worst held one is
/// dropped, so the result is exactly what a stable sort of every row followed
/// by truncation to `limit` would keep -- without ever holding more than
/// `limit` rows.
fn insert_ranked(
    rows: &mut Vec<BindingRow>,
    keys: &mut Vec<Vec<EvaluatedValue>>,
    row: BindingRow,
    items: &[PhysicalSort],
    limit: usize,
) -> EngineResult<()> {
    let key = items
        .iter()
        .map(|item| evaluate_expression(&item.expression, &row))
        .collect::<EngineResult<Vec<_>>>()?;
    let at = keys.partition_point(|held| compare_sort_keys(held, &key, items) != Ordering::Greater);
    if at >= limit {
        return Ok(());
    }
    if rows.len() == limit {
        rows.pop();
        keys.pop();
    }
    rows.insert(at, row);
    keys.insert(at, key);
    Ok(())
}

fn sort_rows(mut rows: Vec<BindingRow>, items: &[PhysicalSort]) -> EngineResult<Vec<BindingRow>> {
    let keys = rows
        .iter()
        .map(|row| {
            items
                .iter()
                .map(|item| evaluate_expression(&item.expression, row))
                .collect::<EngineResult<Vec<_>>>()
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let mut keyed = rows.drain(..).zip(keys).collect::<Vec<_>>();
    keyed.sort_by(|(_, left), (_, right)| compare_sort_keys(left, right, items));
    Ok(keyed.into_iter().map(|(row, _)| row).collect())
}

fn compare_sort_keys(
    left: &[EvaluatedValue],
    right: &[EvaluatedValue],
    items: &[PhysicalSort],
) -> Ordering {
    for ((left, right), item) in left.iter().zip(right).zip(items) {
        let ordering = compare_evaluated_values(left, right);
        let ordering = match item.direction {
            crate::SortDirection::Ascending => ordering,
            crate::SortDirection::Descending => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_evaluated_values(left: &EvaluatedValue, right: &EvaluatedValue) -> Ordering {
    match (left, right) {
        (
            EvaluatedValue::Null | EvaluatedValue::Truth(None),
            EvaluatedValue::Null | EvaluatedValue::Truth(None),
        ) => Ordering::Equal,
        (EvaluatedValue::Null | EvaluatedValue::Truth(None), _) => Ordering::Greater,
        (_, EvaluatedValue::Null | EvaluatedValue::Truth(None)) => Ordering::Less,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right))
        | (EvaluatedValue::Relationship(left), EvaluatedValue::Relationship(right)) => {
            left.cmp(right)
        }
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => {
            compare_scalar_values(left, right)
        }
        (EvaluatedValue::Truth(Some(left)), EvaluatedValue::Truth(Some(right))) => left.cmp(right),
        _ => evaluated_value_rank(left).cmp(&evaluated_value_rank(right)),
    }
}

fn evaluated_value_rank(value: &EvaluatedValue) -> u8 {
    match value {
        EvaluatedValue::Truth(Some(_)) => 1,
        EvaluatedValue::Vertex(_) => 2,
        EvaluatedValue::Relationship(_) => 3,
        EvaluatedValue::Scalar(value) => 4 + scalar_value_rank(value),
        EvaluatedValue::Null | EvaluatedValue::Truth(None) => u8::MAX,
    }
}

fn scalar_value_rank(value: &ScalarValue) -> u8 {
    match value {
        ScalarValue::Boolean(_) => 0,
        ScalarValue::Integer(_) | ScalarValue::Float(_) => 1,
        ScalarValue::String(_) => 2,
        ScalarValue::Null => u8::MAX,
    }
}

fn compare_scalar_values(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left, right) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Greater,
        (_, ScalarValue::Null) => Ordering::Less,
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => left.cmp(right),
        (ScalarValue::Integer(left), ScalarValue::Integer(right)) => left.cmp(right),
        (ScalarValue::Float(left), ScalarValue::Float(right)) => {
            compare_float_spellings(left, right)
        }
        (ScalarValue::Integer(left), ScalarValue::Float(right)) => right
            .parse::<f64>()
            .ok()
            .map(|right| compare_integer_float(*left, right))
            .unwrap_or_else(|| left.to_string().cmp(&right.to_string())),
        (ScalarValue::Float(left), ScalarValue::Integer(right)) => left
            .parse::<f64>()
            .ok()
            .map(|left| compare_integer_float(*right, left).reverse())
            .unwrap_or_else(|| left.to_string().cmp(&right.to_string())),
        (ScalarValue::String(left), ScalarValue::String(right)) => left.cmp(right),
        _ => scalar_value_rank(left).cmp(&scalar_value_rank(right)),
    }
}

fn compare_float_spellings(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(left), Ok(right)) => left
            .partial_cmp(&right)
            .unwrap_or_else(|| left.total_cmp(&right)),
        _ => left.cmp(right),
    }
}

fn compare_integer_float(integer: i128, float: f64) -> Ordering {
    const I128_INCLUSIVE_LOWER: f64 = -170141183460469231731687303715884105728.0;
    const I128_EXCLUSIVE_UPPER: f64 = 170141183460469231731687303715884105728.0;
    if float.is_nan() {
        return Ordering::Greater;
    }
    if float < I128_INCLUSIVE_LOWER {
        return Ordering::Greater;
    }
    if float >= I128_EXCLUSIVE_UPPER {
        return Ordering::Less;
    }
    let truncated = float as i128;
    match integer.cmp(&truncated) {
        Ordering::Equal if float.fract().is_sign_positive() && float.fract() > 0.0 => {
            Ordering::Less
        }
        Ordering::Equal if float.fract().is_sign_negative() && float.fract() < 0.0 => {
            Ordering::Greater
        }
        ordering => ordering,
    }
}

/// The binding and required labels of a vertex access path whose candidate set
/// a runtime probe may replace.
///
/// A `VertexIdSeek` is already a point lookup and an
/// `OrderedVertexPropertyScan` owns its own bounded window, so neither has a
/// candidate set worth replacing.
fn probe_replaceable_access(plan: &GraphPhysicalPlan) -> Option<(&Symbol, &[Symbol])> {
    match plan {
        GraphPhysicalPlan::VertexPropertySeek {
            binding, labels, ..
        }
        | GraphPhysicalPlan::VertexPropertyMultiSeek {
            binding, labels, ..
        }
        | GraphPhysicalPlan::VertexPropertyScan {
            binding, labels, ..
        }
        | GraphPhysicalPlan::VertexLabelScan {
            binding, labels, ..
        } => Some((binding, labels)),
        GraphPhysicalPlan::AllVertexScan { binding } => Some((binding, &[])),
        _ => None,
    }
}

/// The equality restrictions on `binding` that are conjuncts of `predicate`.
///
/// Only conjuncts qualify. The vertices satisfying one conjunct are a superset
/// of the rows the whole predicate accepts, which is what makes a probe result
/// usable as the complete candidate set. One side of a disjunct is not, unless
/// both sides restrict the same property, in which case their union still is.
fn equality_candidates(
    predicate: &PhysicalExpression,
    binding: &Symbol,
) -> Vec<PropertyEqualityCandidate> {
    let mut candidates = Vec::new();
    collect_equality_candidates(predicate, binding, &mut candidates);
    candidates.dedup();
    candidates
}

fn collect_equality_candidates(
    predicate: &PhysicalExpression,
    binding: &Symbol,
    candidates: &mut Vec<PropertyEqualityCandidate>,
) {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        collect_equality_candidates(left, binding, candidates);
        collect_equality_candidates(right, binding, candidates);
        return;
    }
    if let Some(candidate) = equality_candidate(predicate, binding) {
        candidates.push(candidate);
    }
}

fn equality_candidate(
    predicate: &PhysicalExpression,
    binding: &Symbol,
) -> Option<PropertyEqualityCandidate> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => {
            let (property, value) = candidate_property_value(left, right, binding)
                .or_else(|| candidate_property_value(right, left, binding))?;
            Some(PropertyEqualityCandidate {
                property,
                values: vec![value],
            })
        }
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let left = equality_candidate(left, binding)?;
            let right = equality_candidate(right, binding)?;
            if left.property != right.property {
                return None;
            }
            let mut values = left.values;
            for value in right.values {
                if !values.contains(&value) {
                    values.push(value);
                }
            }
            Some(PropertyEqualityCandidate {
                property: left.property,
                values,
            })
        }
        PhysicalExpression::InList { expression, values } => {
            let property = candidate_property(expression, binding)?;
            // NULL is not a seekable key, and Cypher's three-valued IN already
            // resolves it through the predicate itself.
            let values = values
                .iter()
                .filter(|value| !matches!(value.value, ScalarValue::Null))
                .map(|value| value.value.clone())
                .collect::<Vec<_>>();
            (!values.is_empty()).then_some(PropertyEqualityCandidate { property, values })
        }
        _ => None,
    }
}

fn candidate_property_value(
    property: &PhysicalExpression,
    value: &PhysicalExpression,
    binding: &Symbol,
) -> Option<(String, ScalarValue)> {
    let property = candidate_property(property, binding)?;
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    if matches!(value.value, ScalarValue::Null) {
        return None;
    }
    Some((property, value.value.clone()))
}

fn candidate_property(expression: &PhysicalExpression, binding: &Symbol) -> Option<String> {
    let PhysicalExpression::Property {
        binding: candidate_binding,
        property,
    } = expression
    else {
        return None;
    };
    (candidate_binding == binding).then(|| property.to_string())
}

/// How many different properties the candidates restrict. One is the property
/// the planner already anchored on, so a probe can only confirm it; two or more
/// means there is a genuine choice the planner never costed.
fn distinct_properties(candidates: &[PropertyEqualityCandidate]) -> usize {
    candidates
        .iter()
        .map(|candidate| candidate.property.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

/// Evaluate a scan's residual against one vertex.
///
/// `None` accepts everything, which is the case where the index proved the
/// whole predicate and no Filter was removed.
fn residual_accepts(
    residual: Option<&PhysicalExpression>,
    binding: &Symbol,
    vertex: &VertexRecord,
) -> EngineResult<bool> {
    let Some(residual) = residual else {
        return Ok(true);
    };
    let row = BindingRow {
        vertices: BTreeMap::from([(binding.clone(), vertex.clone())]),
        relationships: BTreeMap::new(),
    };
    match evaluate_expression(residual, &row)? {
        EvaluatedValue::Truth(Some(true)) => Ok(true),
        EvaluatedValue::Truth(Some(false) | None) => Ok(false),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            "ordered scan residual did not evaluate to a boolean",
        )),
    }
}

/// How much wider a page grows when a residual rejected every row of the last
/// one, leaving no miss rate to scale by.
const ORDERED_PAGE_GROWTH: usize = 4;

/// The most a page may exceed the rows still wanted. A residual that matches
/// almost nothing would otherwise walk the whole index in one request, and the
/// walk is supposed to be bounded work.
const ORDERED_PAGE_CEILING: usize = 64;

/// How far past its target an ordered walk reads before asking whether an
/// equality in its residual could be seeked instead.
///
/// The planner cannot tell a selective equality from a broad one, so the choice
/// between walking the sorted index and seeking an equality is settled here,
/// where both populations can be measured. This is the price of asking: low
/// enough that a query the walk cannot serve gives up after bounded work,
/// high enough that a window a residual thins by an ordinary fraction reaches
/// its target first and never pays for the probe at all.
const ORDERED_PROBE_TRIGGER: usize = 16;

/// Whether the walk has read past the primary value that decides the answer's
/// edge, and so has seen every row a tie key could reorder into it.
///
/// Rows arrive in primary-key order, which puts the row at `target - 1` on the
/// boundary. Rows sharing its value may sort ahead of it under a tie key the
/// index does not store, so all of them are candidates; rows past it lose on
/// the primary key, which no tie key can undo. `position` is where the index
/// left off rather than where the survivors did -- the residual rejecting a row
/// says nothing about how far the scan reached.
fn tie_boundary_passed(
    rows: &[BindingRow],
    binding: &Symbol,
    property: &str,
    target: usize,
    position: Option<&OrderedScanPosition>,
) -> bool {
    let Some(position) = position else {
        return false;
    };
    let boundary = rows
        .get(target.saturating_sub(1))
        .and_then(|row| row.vertices.get(binding))
        .and_then(|vertex| match vertex.properties.get(property) {
            Some(ScalarValue::String(value)) => Some(value.as_ref()),
            _ => None,
        });
    boundary.is_some_and(|boundary| position.value != boundary)
}

async fn hydrate_rows(
    read: &mut dyn GraphRead,
    binding: &Symbol,
    labels: &[Symbol],
    ids: Vec<VertexId>,
    row_limit: Option<usize>,
    residual: Option<&PhysicalExpression>,
) -> EngineResult<Vec<BindingRow>> {
    let unique_ids = ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }

    let batch_size = row_limit.unwrap_or(unique_ids.len()).max(1);
    let mut rows = Vec::new();
    for ids in unique_ids.chunks(batch_size) {
        read.checkpoint("cypher_hydrate_rows")?;
        let mut vertices = read.hydrate_vertices(ids).await?;
        vertices.sort_by_key(|vertex| vertex.id);
        vertices.dedup_by_key(|vertex| vertex.id);
        for vertex in vertices {
            read.checkpoint("cypher_hydrate_vertex")?;
            if !labels
                .iter()
                .all(|label| vertex.labels.contains(label.as_str()))
            {
                continue;
            }
            let row = BindingRow {
                vertices: BTreeMap::from([(binding.clone(), vertex)]),
                relationships: BTreeMap::new(),
            };
            // A residual is only present when the caller removed the Filter
            // that would otherwise have applied it, so rejecting here is the
            // only thing keeping the result correct.
            if let Some(residual) = residual {
                match evaluate_expression(residual, &row)? {
                    EvaluatedValue::Truth(Some(true)) => {}
                    EvaluatedValue::Truth(Some(false) | None) => continue,
                    _ => {
                        return Err(unsupported(
                            QueryFailureReason::Evaluation,
                            "Filter predicate did not evaluate to a boolean",
                        ));
                    }
                }
            }
            rows.push(row);
            read.check_intermediate_rows("cypher_hydrate_intermediate_rows", rows.len())?;
            if row_limit.is_some_and(|limit| rows.len() >= limit) {
                return Ok(rows);
            }
        }
    }
    Ok(rows)
}

async fn expand_rows(
    read: &mut dyn GraphRead,
    rows: Vec<BindingRow>,
    spec: ExpandSpec<'_>,
    row_limit: Option<usize>,
) -> EngineResult<Vec<BindingRow>> {
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }
    let input_vertex_ids = rows
        .iter()
        .map(|row| {
            read.checkpoint("cypher_expand_inputs")?;
            binding_vertex(row, spec.from).map(|vertex| vertex.id)
        })
        .collect::<EngineResult<BTreeSet<_>>>()?;
    if input_vertex_ids.is_empty() {
        return Ok(Vec::new());
    }

    let requested_types = spec
        .relationship_types
        .iter()
        .map(Symbol::as_str)
        .collect::<BTreeSet<_>>();
    let mut by_input = BTreeMap::<VertexId, Vec<(VertexId, RelationshipRecord)>>::new();
    let mut relationship_rows = 0_usize;
    // An undirected hop runs as both storage orientations and merges here,
    // inside pattern matching, rather than as two plan-level union arms. Union
    // arms complete independently, so ORDER BY, DISTINCT and LIMIT would each
    // apply per arm instead of once across the merged rows.
    let mut seen_per_input = BTreeMap::<VertexId, BTreeSet<RelationshipId>>::new();
    for storage_direction in spec.direction.storage_directions().iter().copied() {
        let request = ExpandRequest {
            input_vertex_ids: input_vertex_ids.iter().copied().collect(),
            direction: storage_direction,
            relationship_types: spec
                .relationship_types
                .iter()
                .map(ToString::to_string)
                .collect(),
        };
        let returned = read.expand_relationships(&request).await?;
        let relationships = canonical_relationships(read, returned)?;
        for relationship in relationships.into_values() {
            read.checkpoint("cypher_expand_relationships")?;
            if !requested_types.is_empty()
                && !requested_types.contains(relationship.relationship_type.as_str())
            {
                continue;
            }
            let (input, target) = match storage_direction {
                ExpandDirection::Outgoing => (relationship.source, relationship.target),
                ExpandDirection::Incoming => (relationship.target, relationship.source),
            };
            if !input_vertex_ids.contains(&input) {
                continue;
            }
            // A self-loop is returned by both orientations. Keying on the
            // relationship identity reports it once while still reporting both
            // edges of a reciprocal pair, which are distinct relationships.
            if !seen_per_input
                .entry(input)
                .or_default()
                .insert(relationship.id)
            {
                continue;
            }
            by_input
                .entry(input)
                .or_default()
                .push((target, relationship));
            relationship_rows = relationship_rows.saturating_add(1);
            read.check_intermediate_rows("cypher_expand_relationship_rows", relationship_rows)?;
        }
    }
    for relationships in by_input.values_mut() {
        relationships.sort_by_key(|(target, relationship)| (*target, relationship.id));
    }

    let target_ids = by_input
        .values()
        .flatten()
        .map(|(target, _)| *target)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut hydrated = read.hydrate_vertices(&target_ids).await?;
    hydrated.sort_by_key(|vertex| vertex.id);
    hydrated.dedup_by_key(|vertex| vertex.id);
    let targets = hydrated
        .into_iter()
        .map(|vertex| (vertex.id, vertex))
        .collect::<BTreeMap<_, _>>();

    let mut expanded = Vec::new();
    for row in rows {
        read.checkpoint("cypher_expand_output_rows")?;
        let input = binding_vertex(&row, spec.from)?.id;
        for (target_id, relationship) in by_input.get(&input).into_iter().flatten() {
            read.checkpoint("cypher_expand_output_rows")?;
            let Some(target) = targets.get(target_id) else {
                continue;
            };
            if !spec
                .target_labels
                .iter()
                .all(|label| target.labels.contains(label.as_str()))
            {
                continue;
            }
            let mut result = row.clone();
            if let Some(existing) = result.vertices.get(spec.to) {
                if existing != target {
                    continue;
                }
            } else {
                result.vertices.insert(spec.to.clone(), target.clone());
            }
            if let Some(binding) = spec.relationship {
                if let Some(existing) = result.relationships.get(binding) {
                    if existing != relationship {
                        continue;
                    }
                } else {
                    result
                        .relationships
                        .insert(binding.clone(), relationship.clone());
                }
            }
            expanded.push(result);
            read.check_intermediate_rows("cypher_expand_intermediate_rows", expanded.len())?;
            if row_limit.is_some_and(|limit| expanded.len() >= limit) {
                return Ok(expanded);
            }
        }
    }
    Ok(expanded)
}

#[allow(clippy::too_many_arguments)]
async fn variable_expand_rows(
    read: &mut dyn GraphRead,
    rows: Vec<BindingRow>,
    from: &Symbol,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
    min_hops: u8,
    max_hops: u8,
    row_limit: Option<usize>,
) -> EngineResult<Vec<BindingRow>> {
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }
    let mut frontier = rows
        .into_iter()
        .map(|row| {
            let current = binding_vertex(&row, from)?.id;
            Ok(TraversalState {
                row,
                current,
                used_relationships: BTreeSet::new(),
            })
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let mut output = Vec::new();

    if min_hops == 0 {
        for state in &frontier {
            let source = binding_vertex(&state.row, from)?.clone();
            if target_labels
                .iter()
                .all(|label| source.labels.contains(label.as_str()))
            {
                let mut row = state.row.clone();
                if row
                    .vertices
                    .get(to)
                    .is_none_or(|existing| existing == &source)
                {
                    row.vertices.insert(to.clone(), source);
                    output.push(row);
                }
            }
        }
    }

    // Undirected variable-length reachability is rejected during lowering: the
    // two orientations compound at every hop, so it is a different search
    // rather than two runs of this one. Guarded here so a future lowering
    // change cannot silently produce half the answer.
    let [storage_direction] = direction.storage_directions() else {
        return Err(unsupported(
            QueryFailureReason::Pattern,
            "variable-length undirected relationships are not supported",
        ));
    };
    let storage_direction = *storage_direction;

    for depth in 1..=max_hops {
        if frontier.is_empty() {
            break;
        }
        let input_vertex_ids = frontier
            .iter()
            .map(|state| state.current)
            .collect::<BTreeSet<_>>();
        let request = ExpandRequest {
            input_vertex_ids: input_vertex_ids.iter().copied().collect(),
            direction: storage_direction,
            relationship_types: relationship_types.iter().map(ToString::to_string).collect(),
        };
        let returned = read.expand_relationships(&request).await?;
        let relationships = canonical_relationships(read, returned)?;
        let requested_types = relationship_types
            .iter()
            .map(Symbol::as_str)
            .collect::<BTreeSet<_>>();
        let mut by_input = BTreeMap::<VertexId, Vec<(VertexId, RelationshipRecord)>>::new();
        for relationship in relationships.into_values() {
            read.checkpoint("cypher_variable_expand_relationships")?;
            if !requested_types.is_empty()
                && !requested_types.contains(relationship.relationship_type.as_str())
            {
                continue;
            }
            let (input, target) = match storage_direction {
                ExpandDirection::Outgoing => (relationship.source, relationship.target),
                ExpandDirection::Incoming => (relationship.target, relationship.source),
            };
            if input_vertex_ids.contains(&input) {
                by_input
                    .entry(input)
                    .or_default()
                    .push((target, relationship));
            }
        }
        let target_ids = by_input
            .values()
            .flatten()
            .map(|(target, _)| *target)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let targets = read
            .hydrate_vertices(&target_ids)
            .await?
            .into_iter()
            .map(|vertex| (vertex.id, vertex))
            .collect::<BTreeMap<_, _>>();
        let mut next = Vec::new();
        for state in frontier {
            for (target_id, relationship) in by_input.get(&state.current).into_iter().flatten() {
                read.checkpoint("cypher_variable_expand_paths")?;
                if state.used_relationships.contains(&relationship.id) {
                    continue;
                }
                let Some(target) = targets.get(target_id) else {
                    continue;
                };
                let mut used_relationships = state.used_relationships.clone();
                used_relationships.insert(relationship.id);
                let next_state = TraversalState {
                    row: state.row.clone(),
                    current: *target_id,
                    used_relationships,
                };
                if depth >= min_hops
                    && target_labels
                        .iter()
                        .all(|label| target.labels.contains(label.as_str()))
                    && next_state
                        .row
                        .vertices
                        .get(to)
                        .is_none_or(|existing| existing == target)
                {
                    let mut row = next_state.row.clone();
                    row.vertices.insert(to.clone(), target.clone());
                    output.push(row);
                    read.check_intermediate_rows(
                        "cypher_variable_expand_output_rows",
                        output.len(),
                    )?;
                    if row_limit.is_some_and(|limit| output.len() >= limit) {
                        return Ok(output);
                    }
                }
                next.push(next_state);
                read.check_intermediate_rows("cypher_variable_expand_frontier", next.len())?;
            }
        }
        frontier = next;
    }
    Ok(output)
}

fn canonical_relationships(
    read: &dyn GraphRead,
    relationships: Vec<RelationshipRecord>,
) -> EngineResult<BTreeMap<RelationshipId, RelationshipRecord>> {
    let mut canonical = BTreeMap::new();
    for relationship in relationships {
        read.checkpoint("cypher_canonical_relationships")?;
        if let Some(previous) = canonical.insert(relationship.id, relationship.clone()) {
            if previous != relationship {
                return Err(EngineError::InvalidStorageResponse(format!(
                    "relationship {} was returned with conflicting records",
                    relationship.id
                )));
            }
        }
    }
    Ok(canonical)
}

fn merge_compatible_rows(left: &BindingRow, right: &BindingRow) -> Option<BindingRow> {
    if left.vertices.iter().any(|(binding, vertex)| {
        right
            .vertices
            .get(binding)
            .is_some_and(|other| other != vertex)
    }) || left.relationships.iter().any(|(binding, relationship)| {
        right
            .relationships
            .get(binding)
            .is_some_and(|other| other != relationship)
    }) || left
        .vertices
        .keys()
        .any(|binding| right.relationships.contains_key(binding))
        || left
            .relationships
            .keys()
            .any(|binding| right.vertices.contains_key(binding))
    {
        return None;
    }
    let mut merged = left.clone();
    merged.vertices.extend(right.vertices.clone());
    merged.relationships.extend(right.relationships.clone());
    Some(merged)
}

fn evaluate_expression(
    expression: &PhysicalExpression,
    row: &BindingRow,
) -> EngineResult<EvaluatedValue> {
    Ok(match expression {
        PhysicalExpression::Binding(binding) => {
            if let Some(vertex) = row.vertices.get(binding) {
                EvaluatedValue::Vertex(vertex.id)
            } else if let Some(relationship) = row.relationships.get(binding) {
                EvaluatedValue::Relationship(relationship.id)
            } else {
                EvaluatedValue::Null
            }
        }
        PhysicalExpression::Identity(binding) => {
            if let Some(vertex) = row.vertices.get(binding) {
                EvaluatedValue::Vertex(vertex.id)
            } else if let Some(relationship) = row.relationships.get(binding) {
                EvaluatedValue::Relationship(relationship.id)
            } else {
                EvaluatedValue::Null
            }
        }
        PhysicalExpression::Property { binding, property } => {
            let properties = if let Some(vertex) = row.vertices.get(binding) {
                &vertex.properties
            } else if let Some(relationship) = row.relationships.get(binding) {
                &relationship.properties
            } else {
                return Ok(EvaluatedValue::Null);
            };
            match properties.get(property.as_str()) {
                None | Some(ScalarValue::Null) => EvaluatedValue::Null,
                Some(value) => EvaluatedValue::Scalar(value.clone()),
            }
        }
        PhysicalExpression::Value(value) => match &value.value {
            ScalarValue::Null => EvaluatedValue::Null,
            value => EvaluatedValue::Scalar(value.clone()),
        },
        PhysicalExpression::Aggregate { .. } => {
            return Err(unsupported(
                QueryFailureReason::Return,
                "aggregate expressions must be evaluated by the projection stage",
            ));
        }
        PhysicalExpression::Unary {
            operator,
            expression,
        } => evaluate_unary(*operator, evaluate_expression(expression, row)?)?,
        PhysicalExpression::Binary {
            left,
            operator,
            right,
        } => evaluate_binary(
            *operator,
            evaluate_expression(left, row)?,
            evaluate_expression(right, row)?,
        )?,
        PhysicalExpression::InList { expression, values } => {
            evaluate_in_list(evaluate_expression(expression, row)?, values)
        }
    })
}

/// Cypher membership, which is three-valued.
///
/// An empty list is `false` even for a null probe: there is nothing it could
/// match. Otherwise a null probe is unknown, a hit is true, and a miss is
/// unknown when the list contains a null, because that null might have been
/// the match.
fn evaluate_in_list(probe: EvaluatedValue, values: &[BoundValue]) -> EvaluatedValue {
    if values.is_empty() {
        return EvaluatedValue::Truth(Some(false));
    }
    let EvaluatedValue::Scalar(probe) = probe else {
        return EvaluatedValue::Truth(None);
    };
    let mut saw_null = false;
    for candidate in values {
        match &candidate.value {
            ScalarValue::Null => saw_null = true,
            value if scalar_values_equal(&probe, value) => {
                return EvaluatedValue::Truth(Some(true))
            }
            _ => {}
        }
    }
    EvaluatedValue::Truth(if saw_null { None } else { Some(false) })
}

fn evaluate_unary(
    operator: PhysicalUnaryOperator,
    value: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    match (operator, value) {
        (PhysicalUnaryOperator::IsNull, value) => Ok(EvaluatedValue::Truth(Some(is_null(&value)))),
        (PhysicalUnaryOperator::IsNotNull, value) => {
            Ok(EvaluatedValue::Truth(Some(!is_null(&value))))
        }
        (PhysicalUnaryOperator::Not, value) => {
            Ok(EvaluatedValue::Truth(as_truth(value)?.map(|value| !value)))
        }
        (PhysicalUnaryOperator::Plus, EvaluatedValue::Scalar(ScalarValue::Integer(value))) => {
            Ok(EvaluatedValue::Scalar(ScalarValue::Integer(value)))
        }
        (PhysicalUnaryOperator::Minus, EvaluatedValue::Scalar(ScalarValue::Integer(value))) => {
            Ok(EvaluatedValue::Scalar(ScalarValue::Integer(
                value.checked_neg().ok_or_else(|| {
                    unsupported(
                        QueryFailureReason::Evaluation,
                        "integer unary minus overflowed",
                    )
                })?,
            )))
        }
        (_, EvaluatedValue::Null) => Ok(EvaluatedValue::Null),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            "unary plus/minus currently requires an integer",
        )),
    }
}

fn evaluate_binary(
    operator: PhysicalBinaryOperator,
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    Ok(match operator {
        PhysicalBinaryOperator::Equal => EvaluatedValue::Truth(evaluate_equal(left, right)),
        PhysicalBinaryOperator::NotEqual => {
            EvaluatedValue::Truth(evaluate_equal(left, right).map(|value| !value))
        }
        PhysicalBinaryOperator::LessThan => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_lt))
        }
        PhysicalBinaryOperator::LessThanOrEqual => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_le))
        }
        PhysicalBinaryOperator::GreaterThan => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_gt))
        }
        PhysicalBinaryOperator::GreaterThanOrEqual => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_ge))
        }
        PhysicalBinaryOperator::And => {
            EvaluatedValue::Truth(evaluate_and(as_truth(left)?, as_truth(right)?))
        }
        PhysicalBinaryOperator::Or => {
            EvaluatedValue::Truth(evaluate_or(as_truth(left)?, as_truth(right)?))
        }
        PhysicalBinaryOperator::Add
        | PhysicalBinaryOperator::Subtract
        | PhysicalBinaryOperator::Multiply
        | PhysicalBinaryOperator::Divide
        | PhysicalBinaryOperator::Modulo => evaluate_integer_arithmetic(operator, left, right)?,
        PhysicalBinaryOperator::StartsWith => evaluate_starts_with(left, right)?,
    })
}

fn query_value_to_evaluated(value: QueryValue) -> EngineResult<EvaluatedValue> {
    match value {
        QueryValue::Null => Ok(EvaluatedValue::Null),
        QueryValue::VertexId(id) => Ok(EvaluatedValue::Vertex(id)),
        QueryValue::RelationshipId(id) => Ok(EvaluatedValue::Relationship(id)),
        QueryValue::Count(value) => Ok(EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(
            value,
        )))),
        QueryValue::Scalar(value) => Ok(EvaluatedValue::Scalar(value)),
        QueryValue::List(_) => Err(unsupported(
            QueryFailureReason::Return,
            "list aggregates cannot be operands of scalar expressions",
        )),
    }
}

fn binding_vertex<'a>(row: &'a BindingRow, binding: &Symbol) -> EngineResult<&'a VertexRecord> {
    row.vertices.get(binding).ok_or_else(|| {
        unsupported(
            QueryFailureReason::Other,
            format!("unbound vertex variable {binding}"),
        )
    })
}

fn evaluate_equal(left: EvaluatedValue, right: EvaluatedValue) -> Option<bool> {
    match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => None,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right)) => Some(left == right),
        (EvaluatedValue::Relationship(left), EvaluatedValue::Relationship(right)) => {
            Some(left == right)
        }
        (
            EvaluatedValue::Vertex(left) | EvaluatedValue::Relationship(left),
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
        )
        | (
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
            EvaluatedValue::Vertex(left) | EvaluatedValue::Relationship(left),
        ) => u64::try_from(right).ok().map(|right| left == right),
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => {
            Some(scalar_values_equal(&left, &right))
        }
        (EvaluatedValue::Truth(Some(left)), EvaluatedValue::Truth(Some(right))) => {
            Some(left == right)
        }
        (EvaluatedValue::Truth(None), _) | (_, EvaluatedValue::Truth(None)) => None,
        _ => Some(false),
    }
}

fn evaluate_ordering(left: EvaluatedValue, right: EvaluatedValue) -> Option<Ordering> {
    match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => None,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right))
        | (EvaluatedValue::Relationship(left), EvaluatedValue::Relationship(right)) => {
            Some(left.cmp(&right))
        }
        (
            EvaluatedValue::Vertex(left) | EvaluatedValue::Relationship(left),
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
        ) => i128::from(left).partial_cmp(&right),
        (
            EvaluatedValue::Scalar(ScalarValue::Integer(left)),
            EvaluatedValue::Vertex(right) | EvaluatedValue::Relationship(right),
        ) => left.partial_cmp(&i128::from(right)),
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => {
            Some(compare_scalar_values(&left, &right))
        }
        (EvaluatedValue::Truth(Some(left)), EvaluatedValue::Truth(Some(right))) => {
            Some(left.cmp(&right))
        }
        (EvaluatedValue::Truth(None), _) | (_, EvaluatedValue::Truth(None)) => None,
        _ => None,
    }
}

fn evaluate_integer_arithmetic(
    operator: PhysicalBinaryOperator,
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    let (left, right) = match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => {
            return Ok(EvaluatedValue::Null);
        }
        (
            EvaluatedValue::Scalar(ScalarValue::Integer(left)),
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
        ) => (left, right),
        _ => {
            return Err(unsupported(
                QueryFailureReason::Evaluation,
                "arithmetic operands must be integers",
            ))
        }
    };
    let value = match operator {
        PhysicalBinaryOperator::Add => left.checked_add(right),
        PhysicalBinaryOperator::Subtract => left.checked_sub(right),
        PhysicalBinaryOperator::Multiply => left.checked_mul(right),
        PhysicalBinaryOperator::Divide if right != 0 => left.checked_div(right),
        PhysicalBinaryOperator::Modulo if right != 0 => left.checked_rem(right),
        PhysicalBinaryOperator::Divide | PhysicalBinaryOperator::Modulo => {
            return Err(unsupported(
                QueryFailureReason::Evaluation,
                "arithmetic division by zero",
            ));
        }
        _ => {
            return Err(unsupported(
                QueryFailureReason::Evaluation,
                "operator is not arithmetic",
            ))
        }
    }
    .ok_or_else(|| {
        unsupported(
            QueryFailureReason::Evaluation,
            "integer arithmetic overflowed",
        )
    })?;
    Ok(EvaluatedValue::Scalar(ScalarValue::Integer(value)))
}

fn evaluate_starts_with(
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => Ok(EvaluatedValue::Truth(None)),
        (
            EvaluatedValue::Scalar(ScalarValue::String(value)),
            EvaluatedValue::Scalar(ScalarValue::String(prefix)),
        ) => Ok(EvaluatedValue::Truth(Some(
            value.starts_with(prefix.as_ref()),
        ))),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            "STARTS WITH operands must be strings",
        )),
    }
}

fn is_null(value: &EvaluatedValue) -> bool {
    matches!(
        value,
        EvaluatedValue::Null
            | EvaluatedValue::Scalar(ScalarValue::Null)
            | EvaluatedValue::Truth(None)
    )
}

fn as_truth(value: EvaluatedValue) -> EngineResult<Option<bool>> {
    match value {
        EvaluatedValue::Truth(value) => Ok(value),
        EvaluatedValue::Scalar(ScalarValue::Boolean(value)) => Ok(Some(value)),
        EvaluatedValue::Null => Ok(None),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            "OR operands must evaluate to booleans",
        )),
    }
}

fn evaluate_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn evaluate_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn to_query_value(value: EvaluatedValue) -> QueryValue {
    match value {
        EvaluatedValue::Null | EvaluatedValue::Truth(None) => QueryValue::Null,
        EvaluatedValue::Vertex(id) => QueryValue::VertexId(id),
        EvaluatedValue::Relationship(id) => QueryValue::RelationshipId(id),
        EvaluatedValue::Scalar(value) => QueryValue::Scalar(value),
        EvaluatedValue::Truth(Some(value)) => QueryValue::Scalar(ScalarValue::Boolean(value)),
    }
}

fn bound_string(value: &BoundValue, role: &'static str) -> EngineResult<String> {
    match &value.value {
        ScalarValue::String(value) => Ok(value.to_string()),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            format!("ordered scan {role} must be a string"),
        )),
    }
}

fn bound_string_value(value: &BoundValue, role: &'static str) -> EngineResult<ScalarValue> {
    bound_string(value, role).map(|value| ScalarValue::String(value.into_boxed_str()))
}

fn seek_value(value: &BoundValue) -> EngineResult<&ScalarValue> {
    if matches!(value.value, ScalarValue::Null) {
        return Err(unsupported(
            QueryFailureReason::Evaluation,
            "NULL cannot be a property seek key",
        ));
    }
    Ok(&value.value)
}

fn vertex_id_value(value: &BoundValue) -> EngineResult<VertexId> {
    let ScalarValue::Integer(value) = value.value else {
        return Err(unsupported(
            QueryFailureReason::Evaluation,
            "vertex id must be an integer",
        ));
    };
    VertexId::try_from(value).map_err(|_| {
        unsupported(
            QueryFailureReason::Evaluation,
            "vertex id must be non-negative and fit u64",
        )
    })
}

fn window_count(value: &BoundValue, field: &'static str) -> EngineResult<usize> {
    let ScalarValue::Integer(value) = value.value else {
        return Err(unsupported(
            QueryFailureReason::OrderWindow,
            format!("{field} count is not an integer"),
        ));
    };
    usize::try_from(value).map_err(|_| {
        unsupported(
            QueryFailureReason::OrderWindow,
            format!("{field} count is out of range"),
        )
    })
}

fn unsupported(bucket: QueryFailureReason, reason: impl Into<String>) -> EngineError {
    EngineError::UnsupportedPlan(bucket, reason.into())
}
