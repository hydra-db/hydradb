use super::access::{
    best_vertex_count, equality_constraint, equality_constraint_for_binding, ordered_window_restrictions, prefer_index,
    property_equality_estimate, property_scan_constraint, property_values_estimate,
};
use crate::vertex_scan::{SlateDbDataSource, VertexOutput, VertexScanRequest, VertexWindow};
use hydradb_cypher_ast::{GqlStatus, QueryFailureReason};
use std::collections::{BTreeMap, BTreeSet};

use super::lower::MAX_IN_LIST_VALUES;

use crate::{
    BoundValue, GraphLogicalPlan, GraphPhysicalPlan, GraphPlanError, GraphPlanResult, LogicalBinaryOperator, LogicalExpression,
    LogicalInList, LogicalProjection, PatternDirection, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection, PhysicalSort,
    PhysicalUnaryOperator, ScalarValue, StatisticsProvider, Symbol, UnknownStatistics, ValueOrigin,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalPlanningContext {
    pub parameters: BTreeMap<Symbol, ScalarValue>,
    /// List-valued parameters, kept apart from `parameters` because
    /// `ScalarValue` has no list variant and must not grow one: it is also the
    /// stored-property type, and a list is not storable as a property. Only
    /// `IN` reads this map.
    pub list_parameters: BTreeMap<Symbol, Vec<ScalarValue>>,
}

impl PhysicalPlanningContext {
    pub fn with_parameter(mut self, name: impl Into<Symbol>, value: ScalarValue) -> Self {
        self.parameters.insert(name.into(), value);
        self
    }

    pub fn with_list_parameter(mut self, name: impl Into<Symbol>, values: Vec<ScalarValue>) -> Self {
        self.list_parameters.insert(name.into(), values);
        self
    }
}

pub fn plan_physical(logical: &GraphLogicalPlan, context: &PhysicalPlanningContext) -> GraphPlanResult<GraphPhysicalPlan> {
    plan_physical_with_statistics(logical, context, &UnknownStatistics)
}

pub fn plan_physical_with_statistics(
    logical: &GraphLogicalPlan,
    context: &PhysicalPlanningContext,
    statistics: &dyn StatisticsProvider,
) -> GraphPlanResult<GraphPhysicalPlan> {
    plan_physical_with_provider(logical, context, &SlateDbDataSource::new(statistics))
}

pub fn plan_physical_with_provider(
    logical: &GraphLogicalPlan,
    context: &PhysicalPlanningContext,
    provider: &SlateDbDataSource<'_>,
) -> GraphPlanResult<GraphPhysicalPlan> {
    plan_physical_operation(logical, context, provider).map_err(|error| error.in_operation(logical.operation()))
}

fn plan_physical_operation(
    logical: &GraphLogicalPlan,
    context: &PhysicalPlanningContext,
    provider: &SlateDbDataSource<'_>,
) -> GraphPlanResult<GraphPhysicalPlan> {
    let statistics = provider.statistics();
    match logical {
        GraphLogicalPlan::Union { arms, all, conjunction } => Ok(GraphPhysicalPlan::Union {
            arms: arms
                .iter()
                .map(|arm| plan_physical_with_provider(arm, context, provider))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            all: *all,
            conjunction: *conjunction,
        }),
        GraphLogicalPlan::Next { input, next } => Ok(GraphPhysicalPlan::Next {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            next: Box::new(plan_physical_with_provider(next, context, provider)?),
        }),
        GraphLogicalPlan::Finish { input } => Ok(GraphPhysicalPlan::Finish {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
        }),
        GraphLogicalPlan::BindPath {
            input,
            names,
            parts,
            node_groups,
        } => Ok(GraphPhysicalPlan::BindPath {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            names: names.clone(),
            parts: parts.clone(),
            node_groups: node_groups.clone(),
        }),
        GraphLogicalPlan::SelectPaths {
            input,
            first,
            last,
            fixed_edges,
            lists,
            kind,
            count,
        } => Ok(GraphPhysicalPlan::SelectPaths {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            first: first.clone(),
            last: last.clone(),
            fixed_edges: *fixed_edges,
            lists: lists.clone(),
            kind: *kind,
            count: bind_expression(count, context)?,
        }),
        GraphLogicalPlan::PathUnion { inputs, words } => Ok(GraphPhysicalPlan::PathUnion {
            inputs: inputs
                .iter()
                .map(|input| plan_physical_with_provider(input, context, provider))
                .collect::<GraphPlanResult<_>>()?,
            words: words.clone(),
        }),
        GraphLogicalPlan::SingleRow => Ok(GraphPhysicalPlan::SingleRow),
        GraphLogicalPlan::Unwind {
            input,
            expression,
            binding,
            requires_list,
            position,
        } => Ok(GraphPhysicalPlan::Unwind {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            expression: bind_expression(expression, context)?,
            binding: binding.clone(),
            requires_list: *requires_list,
            position: position.clone(),
        }),
        GraphLogicalPlan::NodeScan { binding, labels } => Ok(GraphPhysicalPlan::VertexScan {
            scan: provider.prepare_vertex_scan(VertexScanRequest::new(binding.clone(), labels.clone())),
        }),
        GraphLogicalPlan::Expand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        } => Ok(expand_or_type_probe(
            plan_physical_with_provider(input, context, provider)?,
            from,
            relationship,
            relationship_types,
            *direction,
            to,
            target_labels,
        )),
        GraphLogicalPlan::VariableExpand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
            trail,
            nodes,
            iteration,
        } => Ok(GraphPhysicalPlan::VariableExpand {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            from: from.clone(),
            relationship: relationship.clone(),
            relationship_types: relationship_types.clone(),
            direction: *direction,
            to: to.clone(),
            target_labels: target_labels.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            trail: *trail,
            nodes: *nodes,
            iteration: iteration
                .as_ref()
                .map(|iteration| -> GraphPlanResult<_> {
                    Ok(crate::IterationPlan {
                        plan: Box::new(plan_physical_with_provider(&iteration.plan, context, provider)?),
                        source: iteration.source.clone(),
                        relationship: iteration.relationship.clone(),
                        target: iteration.target.clone(),
                    })
                })
                .transpose()?,
        }),
        GraphLogicalPlan::Exists { input, subquery, binding } => Ok(GraphPhysicalPlan::Exists {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            subquery: Box::new(plan_physical_with_provider(subquery, context, provider)?),
            binding: binding.clone(),
        }),
        GraphLogicalPlan::ScalarSubquery {
            input,
            subquery,
            column,
            binding,
        } => Ok(GraphPhysicalPlan::ScalarSubquery {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            subquery: Box::new(plan_physical_with_provider(subquery, context, provider)?),
            column: column.clone(),
            binding: binding.clone(),
        }),
        GraphLogicalPlan::CallProcedure {
            input,
            subquery,
            imports,
            columns,
            outputs_to_record,
            optional,
        } => Ok(GraphPhysicalPlan::CallProcedure {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            subquery: Box::new(plan_physical_with_provider(subquery, context, provider)?),
            imports: imports.clone(),
            columns: columns.clone(),
            outputs_to_record: *outputs_to_record,
            optional: *optional,
        }),
        GraphLogicalPlan::NaturalJoin { left, right, optional } => Ok(GraphPhysicalPlan::NaturalJoin {
            left: Box::new(plan_physical_with_provider(left, context, provider)?),
            right: Box::new(plan_physical_with_provider(right, context, provider)?),
            optional: *optional,
        }),
        GraphLogicalPlan::Filter {
            operation,
            input,
            predicate,
        } => plan_filter(input, predicate, *operation, context, provider).map_err(|error| error.in_operation(*operation)),
        GraphLogicalPlan::Sort { input, items } => Ok(GraphPhysicalPlan::Sort {
            estimated_input_rows: estimate_rows(input, context, statistics),
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            items: items
                .iter()
                .map(|item| {
                    Ok(PhysicalSort {
                        expression: bind_expression(&item.expression, context)?,
                        direction: item.direction,
                        nulls: item.nulls,
                    })
                })
                .collect::<GraphPlanResult<Vec<_>>>()?,
        }),
        GraphLogicalPlan::Skip { input, count } => Ok(GraphPhysicalPlan::Skip {
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            count: bind_window_count(count, "SKIP", context)?,
        }),
        GraphLogicalPlan::Limit { input, count } => {
            let count = bind_window_count(count, "LIMIT", context)?;
            if let Some(window) = plan_ordered_window(input, &count, context, provider)? {
                return Ok(window);
            }
            Ok(GraphPhysicalPlan::Limit {
                input: Box::new(plan_physical_with_provider(input, context, provider)?),
                count,
            })
        }
        GraphLogicalPlan::Project {
            context: projection_context,
            operation,
            input,
            items,
            distinct,
            post_sort,
            post_skip,
            post_limit,
        } => Ok(GraphPhysicalPlan::Project {
            context: projection_context.clone(),
            operation: *operation,
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            items: items
                .iter()
                .map(|item| bind_projection(item, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            distinct: *distinct,
            post_sort: post_sort
                .iter()
                .map(|item| {
                    Ok(PhysicalSort {
                        expression: bind_expression(&item.expression, context)?,
                        direction: item.direction,
                        nulls: item.nulls,
                    })
                })
                .collect::<GraphPlanResult<Vec<_>>>()?,
            post_skip: post_skip
                .as_ref()
                .map(|count| bind_window_count(count, "SKIP", context))
                .transpose()?,
            post_limit: post_limit
                .as_ref()
                .map(|count| bind_window_count(count, "LIMIT", context))
                .transpose()?,
        }),
    }
}

fn index_sort_items(items: &[crate::LogicalSort], context: &PhysicalPlanningContext) -> GraphPlanResult<Option<Vec<PhysicalSort>>> {
    // Indexes implement the implicit null ordering; moving nulls needs a sort.
    if items.iter().any(|item| item.nulls.is_some()) {
        return Ok(None);
    }
    items
        .iter()
        .map(|item| {
            Ok(PhysicalSort {
                expression: bind_expression(&item.expression, context)?,
                direction: item.direction,
                nulls: None,
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()
        .map(Some)
}

/// Rewrite `LIMIT n` over `[SKIP s] ORDER BY b.p [, b.id] FILTER(b.p ...)
/// SCAN b` into a bounded ordered index walk. This is the legacy engine's
/// ordered-string-index fast path expressed as a physical operator, extended
/// to every stored scalar type: the index walks booleans, numbers and strings
/// in the same total order `ORDER BY` uses, under
/// [`compare_property_values`](crate::compare_property_values).
///
/// Terms the index proves become exact restrictions; the rest stay as a
/// residual the scan evaluates on each walked row. The walk still needs
/// somewhere to start, so a predicate that leaves every term residual keeps
/// the generic plan.
fn plan_ordered_window(
    input: &GraphLogicalPlan,
    limit: &BoundValue,
    context: &PhysicalPlanningContext,
    provider: &SlateDbDataSource<'_>,
) -> GraphPlanResult<Option<GraphPhysicalPlan>> {
    let (skip, sorted) = match input {
        GraphLogicalPlan::Skip { input, count } => (Some(bind_window_count(count, "SKIP", context)?), input.as_ref()),
        other => (None, other),
    };
    let GraphLogicalPlan::Sort { input: filtered, items } = sorted else {
        return Ok(None);
    };
    // An inline property map and a WHERE clause lower to separate Filters, so
    // the sort sits over a stack of them rather than over one. Peel the whole
    // stack: each layer is a conjunct of the same predicate.
    let mut predicates = Vec::new();
    let mut scanned = filtered.as_ref();
    while let GraphLogicalPlan::Filter {
        operation: _,
        input,
        predicate,
    } = scanned
    {
        predicates.push(predicate);
        scanned = input.as_ref();
    }
    // A sort on a relationship property walks the relationship index instead,
    // so it is a different operator with the same shape. It takes the peeled
    // stack as one predicate, which is what it was before lowering split it.
    if matches!(scanned, GraphLogicalPlan::Expand { .. }) {
        let Some(predicate) = fold_logical_conjuncts(&predicates) else {
            return Ok(None);
        };
        return plan_ordered_relationship_window(scanned, &predicate, items, skip, limit, context);
    }
    let GraphLogicalPlan::NodeScan { binding, labels } = scanned else {
        return Ok(None);
    };

    let Some(items) = index_sort_items(items, context)? else {
        return Ok(None);
    };
    // A speculative rewrite must decline before binding predicates that the
    // generic plan would attribute to its Filter rather than this Limit.
    if !provider.supports_ordering(binding, &items) {
        return Ok(None);
    }
    let predicates = predicates
        .into_iter()
        .map(|predicate| bind_expression(predicate, context))
        .collect::<GraphPlanResult<Vec<_>>>()?;
    let limit_count = window_count_usize(limit);
    let skip_count = skip.as_ref().map(window_count_usize).unwrap_or(0);
    let Some(required) = limit_count.checked_add(skip_count) else {
        return Ok(None);
    };
    let scan = provider.prepare_vertex_scan(VertexScanRequest {
        binding: binding.clone(),
        labels: labels.clone(),
        predicates,
        output: VertexOutput::Record,
        window: Some(VertexWindow {
            ordering: items.clone(),
            required,
        }),
    });
    if !scan.properties().window_accepted || scan.properties().ordering != items || !scan.properties().residual_predicates.is_empty() {
        return Ok(None);
    }
    let mut plan = GraphPhysicalPlan::VertexScan { scan };
    if let Some(skip) = skip {
        plan = GraphPhysicalPlan::Skip {
            input: Box::new(plan),
            count: skip,
        };
    }
    Ok(Some(GraphPhysicalPlan::Limit {
        input: Box::new(plan),
        count: limit.clone(),
    }))
}

/// The relationship counterpart of [`plan_ordered_window`]: rewrite
/// `LIMIT n` over `[SKIP s] ORDER BY r.p, ... FILTER(...) EXPAND(a)-[r:T]->(b)
/// FROM SCAN a` into a bounded ordered walk of the `T.p` relationship index.
///
/// Planned only where the walk yields exactly the expansion's rows: one
/// relationship type, a named relationship, a directed hop whose source is a
/// plain node scan of `a`, and a leading sort key on `r`. Any later sort keys
/// are allowed, because the executor reads the whole boundary tie of `r.p`
/// before it settles the window. Predicate tiers, the equality-anchor
/// exclusion and the start-point rule are the vertex walk's.
fn plan_ordered_relationship_window(
    expanded: &GraphLogicalPlan,
    predicate: &LogicalExpression,
    items: &[crate::LogicalSort],
    skip: Option<BoundValue>,
    limit: &BoundValue,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<Option<GraphPhysicalPlan>> {
    let GraphLogicalPlan::Expand {
        input: source,
        from,
        relationship: Some(relationship),
        relationship_types,
        direction,
        to,
        target_labels,
    } = expanded
    else {
        return Ok(None);
    };
    let [relationship_type] = relationship_types.as_slice() else {
        return Ok(None);
    };
    if *direction == PatternDirection::Undirected {
        return Ok(None);
    }
    let GraphLogicalPlan::NodeScan {
        binding: source_binding,
        labels: source_labels,
    } = source.as_ref()
    else {
        return Ok(None);
    };
    if source_binding != from || from == to {
        return Ok(None);
    }
    let Some(items) = index_sort_items(items, context)? else {
        return Ok(None);
    };
    let Some(PhysicalSort {
        expression: PhysicalExpression::Property {
            binding: sort_binding,
            property,
        },
        direction: sort_direction,
        ..
    }) = items.first()
    else {
        return Ok(None);
    };
    if sort_binding != relationship {
        return Ok(None);
    }
    let (property, sort_direction) = (property.clone(), *sort_direction);
    let bound_predicate = bind_expression(predicate, context)?;
    let Some((window, residual)) = ordered_window_restrictions(bound_predicate, relationship, &property) else {
        return Ok(None);
    };
    let skip_count = skip.as_ref().map(window_count_usize).unwrap_or(0);
    let Some(required) = window_count_usize(limit).checked_add(skip_count) else {
        return Ok(None);
    };
    let mut plan = GraphPhysicalPlan::OrderedRelationshipPropertyScan {
        from: from.clone(),
        source_labels: source_labels.clone(),
        relationship: relationship.clone(),
        relationship_type: relationship_type.clone(),
        direction: *direction,
        to: to.clone(),
        target_labels: target_labels.clone(),
        property,
        sort_direction,
        prefix: window.prefix,
        lower: window.lower,
        upper: window.upper,
        required,
        items,
        residual,
    };
    if let Some(skip) = skip {
        plan = GraphPhysicalPlan::Skip {
            input: Box::new(plan),
            count: skip,
        };
    }
    Ok(Some(GraphPhysicalPlan::Limit {
        input: Box::new(plan),
        count: limit.clone(),
    }))
}

/// The same fold before binding, for a path that hands its predicate on as one
/// logical expression rather than binding each conjunct itself.
fn fold_logical_conjuncts(conjuncts: &[&LogicalExpression]) -> Option<LogicalExpression> {
    conjuncts
        .iter()
        .rev()
        .copied()
        .cloned()
        .reduce(|left, right| LogicalExpression::Binary {
            left: Box::new(left),
            operator: crate::LogicalBinaryOperator::And,
            right: Box::new(right),
        })
}

fn window_count_usize(value: &BoundValue) -> usize {
    match value.value {
        // `bind_window_count` already rejected negatives and overflow.
        ScalarValue::Integer(count) => usize::try_from(count).unwrap_or(usize::MAX),
        _ => usize::MAX,
    }
}

fn plan_filter(
    input: &GraphLogicalPlan,
    predicate: &LogicalExpression,
    operation: hydradb_cypher_ast::operation::Operation,
    context: &PhysicalPlanningContext,
    provider: &SlateDbDataSource<'_>,
) -> GraphPlanResult<GraphPhysicalPlan> {
    let statistics = provider.statistics();
    // A predicate that holds of no row is kept as itself, over the plan as
    // written: the executor answers it without running that plan, which no
    // access path or pushed-down residual would do.
    if matches!(predicate, LogicalExpression::Literal(ScalarValue::Boolean(false))) {
        return Ok(GraphPhysicalPlan::Filter {
            operation,
            input: Box::new(plan_physical_with_provider(input, context, provider)?),
            predicate: bind_expression(predicate, context)?,
        });
    }
    if let Some(plan) = plan_vertex_filter_stack(input, predicate, operation, context, provider)? {
        return Ok(plan);
    }
    let bound_predicate = bind_expression(predicate, context)?;
    if let GraphLogicalPlan::Expand {
        input: source,
        from,
        relationship,
        relationship_types,
        direction,
        to,
        target_labels,
    } = input
    {
        if let (
            Some(relationship),
            [relationship_type],
            GraphLogicalPlan::NodeScan {
                binding: source_binding,
                labels: source_labels,
            },
            Some((constraint_binding, property, values)),
        ) = (
            relationship,
            relationship_types.as_slice(),
            source.as_ref(),
            relationship
                .as_ref()
                .and_then(|binding| equality_constraint_for_binding(&bound_predicate, binding)),
        ) {
            // One value seeks once; several -- `r.p = $a OR r.p = $b`,
            // `r.p IN $list` -- seek once per value, under the cap an `IN`
            // list already has, so an `OR` chain cannot fan out further than
            // a list may.
            if source_binding == from && constraint_binding == *relationship && !values.is_empty() && values.len() <= MAX_IN_LIST_VALUES {
                let seek_rows = relationship_values_estimate(relationship_types, &property, &values, statistics);
                let seek = if values.len() == 1 {
                    GraphPhysicalPlan::RelationshipPropertySeek {
                        from: from.clone(),
                        source_labels: source_labels.clone(),
                        relationship: relationship.clone(),
                        relationship_type: relationship_type.clone(),
                        property,
                        value: values.into_iter().next().expect("one value"),
                        direction: *direction,
                        to: to.clone(),
                        target_labels: target_labels.clone(),
                    }
                } else {
                    GraphPhysicalPlan::RelationshipPropertyMultiSeek {
                        from: from.clone(),
                        source_labels: source_labels.clone(),
                        relationship: relationship.clone(),
                        relationship_type: relationship_type.clone(),
                        property,
                        values,
                        direction: *direction,
                        to: to.clone(),
                        target_labels: target_labels.clone(),
                    }
                };
                let expand_rows = estimate_anchored_expansion_rows(source, relationship_types, *direction, statistics);
                let access = if prefer_index(seek_rows, expand_rows) {
                    seek
                } else {
                    physical_expand(
                        plan_physical_with_provider(source, context, provider)?,
                        from,
                        &Some(relationship.clone()),
                        relationship_types,
                        *direction,
                        to,
                        target_labels,
                    )
                };
                return Ok(filter_over(access, bound_predicate, operation));
            }
        }
        if let (
            Some(relationship),
            [relationship_type],
            GraphLogicalPlan::NodeScan {
                binding: source_binding,
                labels: source_labels,
            },
        ) = (relationship, relationship_types.as_slice(), source.as_ref())
        {
            // The probe never hands the filter a relationship below the
            // bound, and evaluation is eager (see `filter_over`): a residual
            // conjunct that errors on such a row -- `1 / r.timestamp` at zero
            // -- would turn the expansion's error into an empty result. Only
            // a predicate that cannot fail may lose those rows unseen.
            if source_binding == from && total_condition(&bound_predicate) {
                if let Some((property, lower, inclusive)) = integer_relationship_lower_bound(&bound_predicate, relationship) {
                    let fallback = physical_expand(
                        plan_physical_with_provider(source, context, provider)?,
                        from,
                        &Some(relationship.clone()),
                        relationship_types,
                        *direction,
                        to,
                        target_labels,
                    );
                    return Ok(filter_over(
                        GraphPhysicalPlan::RelationshipPropertyRangeProbe {
                            from: from.clone(),
                            source_labels: source_labels.clone(),
                            relationship: relationship.clone(),
                            relationship_type: relationship_type.clone(),
                            property,
                            lower,
                            inclusive,
                            direction: *direction,
                            to: to.clone(),
                            target_labels: target_labels.clone(),
                            fallback: Box::new(fallback),
                        },
                        bound_predicate,
                        operation,
                    ));
                }
            }
        }
        let bindings = expression_bindings(predicate);
        if bindings.iter().all(|binding| *binding == from) {
            return Ok(physical_expand(
                plan_filter(source, predicate, operation, context, provider)?,
                from,
                relationship,
                relationship_types,
                *direction,
                to,
                target_labels,
            ));
        }
    }

    // A request-independent predicate on the right scan of a join can select
    // that scan's provider access without changing left-to-right evaluation.
    // Do not do the symmetric left rewrite: an empty left point set could
    // suppress fallible work in the right branch.
    if let GraphLogicalPlan::NaturalJoin { left, right, optional } = input {
        if let GraphLogicalPlan::NodeScan { binding, labels } = right.as_ref() {
            let bindings = expression_bindings(predicate);
            if total_condition(&bound_predicate) && !bindings.is_empty() && bindings.iter().all(|candidate| *candidate == binding) {
                let scan = provider.prepare_vertex_scan(VertexScanRequest {
                    binding: binding.clone(),
                    labels: labels.clone(),
                    predicates: vec![bound_predicate.clone()],
                    output: VertexOutput::Record,
                    window: None,
                });
                if scan.access_kind() != "id-points" {
                    return Ok(filter_over(
                        plan_physical_with_provider(input, context, provider)?,
                        bound_predicate,
                        operation,
                    ));
                }
                let access = GraphPhysicalPlan::NaturalJoin {
                    left: Box::new(plan_physical_with_provider(left, context, provider)?),
                    right: Box::new(GraphPhysicalPlan::VertexScan { scan }),
                    optional: *optional,
                };
                return Ok(filter_over(access, bound_predicate, operation));
            }
        }
    }

    let access = match input {
        GraphLogicalPlan::NodeScan { binding, labels } => GraphPhysicalPlan::VertexScan {
            scan: provider.prepare_vertex_scan(VertexScanRequest {
                binding: binding.clone(),
                labels: labels.clone(),
                predicates: vec![bound_predicate.clone()],
                output: VertexOutput::Record,
                window: None,
            }),
        },
        _ => plan_physical_with_provider(input, context, provider)?,
    };
    Ok(filter_over(access, bound_predicate, operation))
}

/// Bind the safe predicate prefix over a vertex scan before asking its provider
/// to choose access. Keep the filter stack and its eager-error barriers intact.
fn plan_vertex_filter_stack(
    input: &GraphLogicalPlan,
    predicate: &LogicalExpression,
    operation: hydradb_cypher_ast::operation::Operation,
    context: &PhysicalPlanningContext,
    provider: &SlateDbDataSource<'_>,
) -> GraphPlanResult<Option<GraphPhysicalPlan>> {
    let mut layers = vec![(predicate, operation)];
    let mut scanned = input;
    while let GraphLogicalPlan::Filter {
        operation,
        input,
        predicate,
    } = scanned
    {
        layers.push((predicate, *operation));
        scanned = input.as_ref();
    }
    let GraphLogicalPlan::NodeScan { binding, labels } = scanned else {
        return Ok(None);
    };
    // Bound outermost first, the order the unranked planner binds them in, so
    // a missing parameter is reported the same way either way.
    let mut bound = layers
        .into_iter()
        .map(|(layer, operation)| {
            bind_expression(layer, context)
                .map(|layer| (layer, operation))
                .map_err(|error| error.in_operation(operation))
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    bound.reverse();
    let peeled = 1 + bound[1..].iter().take_while(|(layer, _)| total_condition(layer)).count();
    let access = GraphPhysicalPlan::VertexScan {
        scan: provider.prepare_vertex_scan(VertexScanRequest {
            binding: binding.clone(),
            labels: labels.clone(),
            predicates: bound[..peeled].iter().map(|(layer, _)| layer.clone()).collect(),
            output: VertexOutput::Record,
            window: None,
        }),
    };
    let mut bound = bound.into_iter();
    let (innermost, operation) = bound.next().expect("innermost layer");
    let mut plan = filter_over(access, innermost, operation);
    for (layer, operation) in bound {
        plan = filter_over(plan, layer, operation);
    }
    Ok(Some(plan))
}

/// An AND term may provide a safe lower bound even when other terms (for
/// example `IS NULL OR = ''`) stay as residual predicates. Do not infer a
/// bound through OR: either branch could admit a value below it.
fn integer_relationship_lower_bound(predicate: &PhysicalExpression, relationship: &Symbol) -> Option<(Symbol, BoundValue, bool)> {
    let PhysicalExpression::Binary { left, operator, right } = predicate else {
        return None;
    };
    if *operator == PhysicalBinaryOperator::And {
        return integer_relationship_lower_bound(left, relationship).or_else(|| integer_relationship_lower_bound(right, relationship));
    }
    let property = |expression: &PhysicalExpression| match expression {
        PhysicalExpression::Property { binding, property } if binding == relationship => Some(property.clone()),
        _ => None,
    };
    let integer = |expression: &PhysicalExpression| match expression {
        PhysicalExpression::Value(value) if matches!(value.value, ScalarValue::Integer(_)) => Some(value.clone()),
        _ => None,
    };
    match operator {
        PhysicalBinaryOperator::GreaterThan | PhysicalBinaryOperator::GreaterThanOrEqual => Some((
            property(left)?,
            integer(right)?,
            *operator == PhysicalBinaryOperator::GreaterThanOrEqual,
        )),
        PhysicalBinaryOperator::LessThan | PhysicalBinaryOperator::LessThanOrEqual => Some((
            property(right)?,
            integer(left)?,
            *operator == PhysicalBinaryOperator::LessThanOrEqual,
        )),
        _ => None,
    }
}

/// A `Filter` over `access`, merged into it when `access` is already one.
///
/// Two logical filters over one access -- a pattern's inline equalities and a
/// `WHERE` clause naming the same binding -- otherwise plan as a stack. That
/// costs a second pass over every row, and it hides the outer predicate from
/// `probe_replaceable_access`, which looks through a filter only to a bare
/// access: a narrow equality in the outer filter never reaches the probe, so
/// the query stays anchored on whichever property the planner picked. Merging
/// is also what keeps the probe's replacement sound, because the single
/// predicate it re-applies is the conjunction the stack enforced.
///
/// Only when the outer predicate cannot fail. Evaluation is eager -- `Binary`
/// evaluates both operands before combining them -- so a conjunction does not
/// short-circuit. A stack discards a row the inner predicate rejects before the
/// outer one is ever evaluated on it; a merged `And` evaluates both, so an outer
/// predicate that errors on such a row would fail a query that used to succeed.
/// `total_condition` is what rules that out, and a predicate it rejects keeps
/// the stack and forgoes the probe.
///
/// The inner predicate stays on the left so the merged predicate reads, and
/// explains, in the order the stack enforced.
fn filter_over(
    access: GraphPhysicalPlan,
    predicate: PhysicalExpression,
    operation: hydradb_cypher_ast::operation::Operation,
) -> GraphPhysicalPlan {
    match access {
        GraphPhysicalPlan::Filter {
            operation: inner_operation,
            input,
            predicate: inner,
        } if inner_operation == operation && total_condition(&predicate) => GraphPhysicalPlan::Filter {
            operation,
            input,
            predicate: PhysicalExpression::Binary {
                left: Box::new(inner),
                operator: PhysicalBinaryOperator::And,
                right: Box::new(predicate),
            },
        },
        access => GraphPhysicalPlan::Filter {
            operation,
            input: Box::new(access),
            predicate,
        },
    }
}

/// Whether evaluating `expression` as a condition can only yield a truth value,
/// never an error.
///
/// Comparisons yield unknown for operands that do not compare, and the null
/// tests are total by definition. Arithmetic is not: it divides by zero and
/// overflows. `STARTS WITH` is not: it rejects a non-string. `Not`, `And` and
/// `Or` are total only over operands that are themselves truth-valued.
pub(crate) fn total_condition(expression: &PhysicalExpression) -> bool {
    match expression {
        PhysicalExpression::Binary { left, operator, right } => match operator {
            PhysicalBinaryOperator::Equal
            | PhysicalBinaryOperator::NotEqual
            | PhysicalBinaryOperator::LessThan
            | PhysicalBinaryOperator::LessThanOrEqual
            | PhysicalBinaryOperator::GreaterThan
            | PhysicalBinaryOperator::GreaterThanOrEqual => total_value(left) && total_value(right),
            PhysicalBinaryOperator::And | PhysicalBinaryOperator::Or | PhysicalBinaryOperator::Xor => {
                total_condition(left) && total_condition(right)
            }
            _ => false,
        },
        PhysicalExpression::Unary { operator, expression } => match operator {
            PhysicalUnaryOperator::IsNull | PhysicalUnaryOperator::IsNotNull => total_value(expression),
            PhysicalUnaryOperator::Not => total_condition(expression),
            PhysicalUnaryOperator::Plus | PhysicalUnaryOperator::Minus => false,
        },
        PhysicalExpression::InList { expression, .. } => total_value(expression),
        _ => false,
    }
}

/// Whether evaluating `expression` as a value can only yield a value, never an
/// error. Only the leaves qualify: every compound value expression is either
/// arithmetic or an aggregate.
///
/// A label or element test (`n:L`, `IS DIRECTED`, `PROPERTY_EXISTS`, ...) is
/// not one: on a name holding a value that is no node or edge it raises
/// `22G03`, so merging it into an earlier filter would raise for rows that
/// filter rejects.
fn total_value(expression: &PhysicalExpression) -> bool {
    matches!(
        expression,
        PhysicalExpression::Property { .. }
            | PhysicalExpression::Value(_)
            | PhysicalExpression::Binding(_)
            | PhysicalExpression::Identity(_)
            | PhysicalExpression::PathNodesDistinct { .. }
    )
}

/// Enter through the relationship type instead of the anchor label, when the
/// anchor is a bare scan.
///
/// A bare scan means no index narrowed the anchor, so the expansion visits the
/// whole label population and pays a neighbour read per vertex to discover that
/// most of them have no edge of this type. Entering through the type instead
/// costs one read per edge that exists.
///
/// No statistic decides this, because none is needed and none may be available:
/// the probe measures the type at execution time and declines a type too broad
/// to read whole, falling back to exactly the plan this replaced. The cost of
/// guessing wrong is therefore bounded by the probe's own entry budget, which is
/// why this is emitted whenever it *could* pay rather than when statistics say
/// it will.
fn expand_or_type_probe(
    input: GraphPhysicalPlan,
    from: &Symbol,
    relationship: &Option<Symbol>,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
) -> GraphPhysicalPlan {
    // A narrowed anchor is already cheap to expand from, and the probe would
    // read the whole type to serve a handful of rows. Only a bare scan is worth
    // replacing.
    let source_labels = match &input {
        GraphPhysicalPlan::VertexScan { scan } if scan.is_unrestricted_population() => scan.labels().to_vec(),
        _ => {
            return physical_expand(input, from, relationship, relationship_types, direction, to, target_labels);
        }
    };
    // One named type and one relationship binding: the probe reads a single
    // type's key space, and it binds the relationship on every row it returns
    // rather than optionally.
    let ([relationship_type], Some(relationship_binding)) = (relationship_types, relationship) else {
        return physical_expand(input, from, relationship, relationship_types, direction, to, target_labels);
    };
    // Undirected is deferred, not unsupported: the storage walk is orientation
    // agnostic and `relationship_seek_rows` already yields both assignments, but
    // the surrounding undirected semantics are in flight and this is not the
    // change that should settle them.
    if matches!(direction, PatternDirection::Undirected) {
        return physical_expand(input, from, relationship, relationship_types, direction, to, target_labels);
    }
    let relationship_type = relationship_type.clone();
    let relationship_binding = relationship_binding.clone();
    let fallback = physical_expand(input, from, relationship, relationship_types, direction, to, target_labels);
    GraphPhysicalPlan::RelationshipTypeProbe {
        from: from.clone(),
        source_labels,
        relationship: relationship_binding,
        relationship_type,
        direction,
        to: to.clone(),
        target_labels: target_labels.to_vec(),
        fallback: Box::new(fallback),
    }
}

fn physical_expand(
    input: GraphPhysicalPlan,
    from: &Symbol,
    relationship: &Option<Symbol>,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
) -> GraphPhysicalPlan {
    GraphPhysicalPlan::Expand {
        input: Box::new(input),
        from: from.clone(),
        relationship: relationship.clone(),
        relationship_types: relationship_types.to_vec(),
        direction,
        to: to.clone(),
        target_labels: target_labels.to_vec(),
    }
}

/// Resolves an `IN` right-hand side to concrete bound values.
///
/// An empty list is accepted and matches nothing. Callers page id sets down to
/// empty often enough that rejecting it would be a correctness trap rather than
/// a helpful error, and Cypher already defines `x IN []` as false.
fn bind_in_list(list: &LogicalInList, context: &PhysicalPlanningContext) -> GraphPlanResult<Vec<BoundValue>> {
    let values = match list {
        LogicalInList::Values(elements) => elements
            .iter()
            .map(|element| match bind_expression(element, context)? {
                PhysicalExpression::Value(value) => Ok(value),
                _ => Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Where,
                    "IN list elements must be literals or parameters".to_string(),
                )),
            })
            .collect::<GraphPlanResult<Vec<_>>>()?,
        LogicalInList::Parameter(parameter) => {
            if let Some(values) = context.list_parameters.get(parameter) {
                values
                    .iter()
                    .map(|value| BoundValue {
                        value: value.clone(),
                        origin: ValueOrigin::Parameter(parameter.clone()),
                    })
                    .collect()
            } else if let Some(value) = context.parameters.get(parameter) {
                // A scalar bound where a list was expected is a one-element
                // membership test rather than an error.
                vec![BoundValue {
                    value: value.clone(),
                    origin: ValueOrigin::Parameter(parameter.clone()),
                }]
            } else {
                return Err(GraphPlanError::MissingParameter(parameter.as_str().to_string()));
            }
        }
    };
    if values.len() > MAX_IN_LIST_VALUES {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Where,
            format!("IN list of {} values exceeds the {MAX_IN_LIST_VALUES}-value limit", values.len()),
        ));
    }
    Ok(values)
}

fn expression_bindings(expression: &LogicalExpression) -> BTreeSet<&Symbol> {
    expression.binding_references()
}

fn bind_projection(projection: &LogicalProjection, context: &PhysicalPlanningContext) -> GraphPlanResult<PhysicalProjection> {
    Ok(PhysicalProjection {
        expression: bind_expression(&projection.expression, context)?,
        alias: projection.alias.clone(),
    })
}

fn bind_expression(expression: &LogicalExpression, context: &PhysicalPlanningContext) -> GraphPlanResult<PhysicalExpression> {
    Ok(match expression {
        LogicalExpression::Binding(binding) => PhysicalExpression::Binding(binding.clone()),
        LogicalExpression::Identity(binding) => PhysicalExpression::Identity(binding.clone()),
        LogicalExpression::PathNodesDistinct { simple, parts } => PhysicalExpression::PathNodesDistinct {
            simple: *simple,
            parts: parts.clone(),
        },
        LogicalExpression::ElementTest { binding, test } => PhysicalExpression::ElementTest {
            binding: binding.clone(),
            test: test.clone(),
        },
        LogicalExpression::Property { binding, property } => PhysicalExpression::Property {
            binding: binding.clone(),
            property: property.clone(),
        },
        LogicalExpression::Parameter(parameter) => {
            if let Some(value) = context.parameters.get(parameter) {
                PhysicalExpression::Value(BoundValue {
                    value: value.clone(),
                    origin: ValueOrigin::Parameter(parameter.clone()),
                })
            } else if let Some(values) = context.list_parameters.get(parameter) {
                // A list parameter read as a value -- `UNWIND $values`,
                // `RETURN $values` -- is the list of its elements.
                PhysicalExpression::List(
                    values
                        .iter()
                        .map(|value| {
                            PhysicalExpression::Value(BoundValue {
                                value: value.clone(),
                                origin: ValueOrigin::Parameter(parameter.clone()),
                            })
                        })
                        .collect(),
                )
            } else {
                return Err(GraphPlanError::MissingParameter(parameter.as_str().to_string()));
            }
        }
        LogicalExpression::Case {
            operand,
            branches,
            default,
        } => PhysicalExpression::Case {
            operand: operand
                .as_deref()
                .map(|operand| bind_expression(operand, context).map(Box::new))
                .transpose()?,
            branches: branches
                .iter()
                .map(|(when, then)| Ok((bind_expression(when, context)?, bind_expression(then, context)?)))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            default: default
                .as_deref()
                .map(|default| bind_expression(default, context).map(Box::new))
                .transpose()?,
        },
        LogicalExpression::List(items) => PhysicalExpression::List(
            items
                .iter()
                .map(|item| bind_expression(item, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
        ),
        LogicalExpression::Literal(value) => PhysicalExpression::Value(BoundValue {
            value: value.clone(),
            origin: ValueOrigin::Literal,
        }),
        LogicalExpression::Aggregate {
            function,
            distinct,
            expression,
            fraction,
            group,
        } => PhysicalExpression::Aggregate {
            group: group.clone(),
            function: *function,
            distinct: *distinct,
            expression: expression
                .as_ref()
                .map(|expression| bind_expression(expression, context).map(Box::new))
                .transpose()?,
            fraction: fraction
                .as_ref()
                .map(|fraction| bind_expression(fraction, context).map(Box::new))
                .transpose()?,
        },
        LogicalExpression::Function { function, arguments } => PhysicalExpression::Function {
            function: *function,
            arguments: arguments
                .iter()
                .map(|argument| bind_expression(argument, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
        },
        LogicalExpression::TypeTest {
            expression,
            value_type,
            negated,
        } => PhysicalExpression::TypeTest {
            expression: Box::new(bind_expression(expression, context)?),
            value_type: value_type.clone(),
            negated: *negated,
        },
        LogicalExpression::Cast {
            expression,
            value_type,
            request,
        } => PhysicalExpression::Cast {
            expression: Box::new(bind_expression(expression, context)?),
            value_type: value_type.clone(),
            request: request
                .as_deref()
                .map(|request| bind_expression(request, context).map(Box::new))
                .transpose()?,
        },
        LogicalExpression::Record(fields) => PhysicalExpression::Record(
            fields
                .iter()
                .map(|(name, value)| Ok((name.clone(), bind_expression(value, context)?)))
                .collect::<GraphPlanResult<Vec<_>>>()?,
        ),
        LogicalExpression::Let { bindings, body } => PhysicalExpression::Let {
            bindings: bindings
                .iter()
                .map(|(name, value)| Ok((name.clone(), bind_expression(value, context)?)))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            body: Box::new(bind_expression(body, context)?),
        },
        LogicalExpression::PathConstructor(elements) => PhysicalExpression::PathConstructor(
            elements
                .iter()
                .map(|element| bind_expression(element, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
        ),
        LogicalExpression::Field { expression, field } => PhysicalExpression::Field {
            expression: Box::new(bind_expression(expression, context)?),
            field: field.clone(),
        },
        LogicalExpression::Unary { operator, expression } => PhysicalExpression::Unary {
            operator: match operator {
                crate::LogicalUnaryOperator::Not => PhysicalUnaryOperator::Not,
                crate::LogicalUnaryOperator::Plus => PhysicalUnaryOperator::Plus,
                crate::LogicalUnaryOperator::Minus => PhysicalUnaryOperator::Minus,
                crate::LogicalUnaryOperator::IsNull => PhysicalUnaryOperator::IsNull,
                crate::LogicalUnaryOperator::IsNotNull => PhysicalUnaryOperator::IsNotNull,
            },
            expression: Box::new(bind_expression(expression, context)?),
        },
        LogicalExpression::In { expression, list } => PhysicalExpression::InList {
            expression: Box::new(bind_expression(expression, context)?),
            values: bind_in_list(list, context)?,
        },
        LogicalExpression::Binary { left, operator, right } => PhysicalExpression::Binary {
            left: Box::new(bind_expression(left, context)?),
            operator: match operator {
                LogicalBinaryOperator::Equal => PhysicalBinaryOperator::Equal,
                LogicalBinaryOperator::NotEqual => PhysicalBinaryOperator::NotEqual,
                LogicalBinaryOperator::LessThan => PhysicalBinaryOperator::LessThan,
                LogicalBinaryOperator::LessThanOrEqual => PhysicalBinaryOperator::LessThanOrEqual,
                LogicalBinaryOperator::GreaterThan => PhysicalBinaryOperator::GreaterThan,
                LogicalBinaryOperator::GreaterThanOrEqual => PhysicalBinaryOperator::GreaterThanOrEqual,
                LogicalBinaryOperator::And => PhysicalBinaryOperator::And,
                LogicalBinaryOperator::Or => PhysicalBinaryOperator::Or,
                LogicalBinaryOperator::Xor => PhysicalBinaryOperator::Xor,
                LogicalBinaryOperator::Add => PhysicalBinaryOperator::Add,
                LogicalBinaryOperator::Subtract => PhysicalBinaryOperator::Subtract,
                LogicalBinaryOperator::Multiply => PhysicalBinaryOperator::Multiply,
                LogicalBinaryOperator::Divide => PhysicalBinaryOperator::Divide,
                LogicalBinaryOperator::Modulo => PhysicalBinaryOperator::Modulo,
                LogicalBinaryOperator::StartsWith => PhysicalBinaryOperator::StartsWith,
                LogicalBinaryOperator::EndsWith => PhysicalBinaryOperator::EndsWith,
                LogicalBinaryOperator::Contains => PhysicalBinaryOperator::Contains,
                LogicalBinaryOperator::Concatenate => PhysicalBinaryOperator::Concatenate,
                LogicalBinaryOperator::In => PhysicalBinaryOperator::In,
            },
            right: Box::new(bind_expression(right, context)?),
        },
    })
}

fn bind_window_count(
    expression: &LogicalExpression,
    field: &'static str,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<BoundValue> {
    let bound = bind_expression(expression, context)?;
    let (value, origin) = constant_integer(&bound, field)?;
    if value < 0 {
        return Err(GraphPlanError::data(
            GqlStatus::NEGATIVE_LIMIT_VALUE,
            QueryFailureReason::OrderWindow,
            format!("{field} cannot be negative"),
        ));
    }
    if field == "SKIP" && u64::try_from(value).is_err() {
        return Err(GraphPlanError::data(
            GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
            QueryFailureReason::OrderWindow,
            "SKIP exceeds the supported u64 range".to_string(),
        ));
    }
    if field == "LIMIT" && usize::try_from(value).is_err() {
        return Err(GraphPlanError::data(
            GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
            QueryFailureReason::OrderWindow,
            "LIMIT exceeds the supported usize range".to_string(),
        ));
    }
    Ok(BoundValue {
        value: ScalarValue::Integer(value),
        origin,
    })
}

fn constant_integer(expression: &PhysicalExpression, field: &'static str) -> GraphPlanResult<(i128, ValueOrigin)> {
    match expression {
        PhysicalExpression::Value(BoundValue {
            value: ScalarValue::Integer(value),
            origin,
        }) => Ok((*value, origin.clone())),
        PhysicalExpression::Unary { operator, expression } => {
            let (value, origin) = constant_integer(expression, field)?;
            match operator {
                PhysicalUnaryOperator::Not => Err(GraphPlanError::InvalidLogicalPlan(
                    QueryFailureReason::OrderWindow,
                    format!("{field} does not accept NOT"),
                )),
                PhysicalUnaryOperator::IsNull | PhysicalUnaryOperator::IsNotNull => Err(GraphPlanError::InvalidLogicalPlan(
                    QueryFailureReason::OrderWindow,
                    format!("{field} does not accept IS NULL"),
                )),
                PhysicalUnaryOperator::Plus => Ok((value, origin)),
                PhysicalUnaryOperator::Minus => value.checked_neg().map(|value| (value, origin)).ok_or_else(|| {
                    GraphPlanError::data(
                        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
                        QueryFailureReason::OrderWindow,
                        format!("{field} constant expression overflowed"),
                    )
                }),
            }
        }
        PhysicalExpression::Binary { left, operator, right } => {
            let (left, left_origin) = constant_integer(left, field)?;
            let (right, right_origin) = constant_integer(right, field)?;
            let origin = match (left_origin, right_origin) {
                (ValueOrigin::Parameter(parameter), _) | (_, ValueOrigin::Parameter(parameter)) => ValueOrigin::Parameter(parameter),
                _ => ValueOrigin::Literal,
            };
            let value = match operator {
                PhysicalBinaryOperator::Add => left.checked_add(right),
                PhysicalBinaryOperator::Subtract => left.checked_sub(right),
                PhysicalBinaryOperator::Multiply => left.checked_mul(right),
                PhysicalBinaryOperator::Divide if right != 0 => left.checked_div(right),
                PhysicalBinaryOperator::Modulo if right != 0 => left.checked_rem(right),
                PhysicalBinaryOperator::Divide | PhysicalBinaryOperator::Modulo => {
                    return Err(GraphPlanError::data(
                        GqlStatus::DIVISION_BY_ZERO,
                        QueryFailureReason::OrderWindow,
                        format!("{field} divides by zero"),
                    ))
                }
                _ => {
                    return Err(GraphPlanError::InvalidLogicalPlan(
                        QueryFailureReason::OrderWindow,
                        format!("{field} contains a non-arithmetic operator"),
                    ))
                }
            }
            .ok_or_else(|| {
                GraphPlanError::data(
                    GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
                    QueryFailureReason::OrderWindow,
                    format!("{field} constant expression overflowed"),
                )
            })?;
            Ok((value, origin))
        }
        // §20.3 General Rules: a null is not allowed, and anything else must
        // be an exact number with scale 0.
        PhysicalExpression::Value(BoundValue {
            value: ScalarValue::Null,
            origin: ValueOrigin::Parameter(parameter),
        }) => Err(GraphPlanError::data(
            GqlStatus::NULL_VALUE_NOT_ALLOWED,
            QueryFailureReason::OrderWindow,
            format!("{field} parameter ${parameter} is null"),
        )),
        PhysicalExpression::Value(BoundValue {
            origin: ValueOrigin::Parameter(parameter),
            ..
        }) => Err(GraphPlanError::data(
            GqlStatus::INVALID_VALUE_TYPE,
            QueryFailureReason::OrderWindow,
            format!("{field} parameter ${parameter} must be an integer"),
        )),
        _ => Err(GraphPlanError::InvalidLogicalPlan(
            QueryFailureReason::OrderWindow,
            format!("{field} must be a constant integer"),
        )),
    }
}

fn estimate_rows(plan: &GraphLogicalPlan, context: &PhysicalPlanningContext, statistics: &dyn StatisticsProvider) -> Option<u64> {
    match plan {
        GraphLogicalPlan::Union { arms, .. } => {
            let total = arms.iter().try_fold(0_u64, |total, arm| {
                Some(total.saturating_add(estimate_rows(arm, context, statistics)?))
            })?;
            Some(total)
        }
        GraphLogicalPlan::SingleRow => Some(1),
        GraphLogicalPlan::Unwind { input, expression, .. } => {
            let input_rows = estimate_rows(input, context, statistics)?;
            match bind_expression(expression, context).ok()? {
                PhysicalExpression::List(items) => Some(input_rows.saturating_mul(items.len() as u64)),
                _ => Some(input_rows),
            }
        }
        GraphLogicalPlan::NodeScan { labels, .. } => best_vertex_count(labels, statistics),
        GraphLogicalPlan::Filter {
            operation: _,
            input,
            predicate,
        } => {
            let input_rows = estimate_rows(input, context, statistics);
            let bound = bind_expression(predicate, context).ok()?;
            estimate_filter_rows(input, &bound, input_rows, statistics)
        }
        GraphLogicalPlan::Sort { input, .. } | GraphLogicalPlan::Project { input, .. } => estimate_rows(input, context, statistics),
        GraphLogicalPlan::Skip { input, count } => {
            let rows = estimate_rows(input, context, statistics)?;
            let skip = bind_window_count(count, "SKIP", context).ok()?;
            let ScalarValue::Integer(skip) = skip.value else {
                return None;
            };
            Some(rows.saturating_sub(u64::try_from(skip).ok()?))
        }
        GraphLogicalPlan::Limit { input, count } => {
            let rows = estimate_rows(input, context, statistics)?;
            let limit = bind_window_count(count, "LIMIT", context).ok()?;
            let ScalarValue::Integer(limit) = limit.value else {
                return None;
            };
            Some(rows.min(u64::try_from(limit).ok()?))
        }
        GraphLogicalPlan::Expand {
            input,
            relationship_types,
            direction,
            target_labels,
            ..
        } => estimate_expand_rows(input, relationship_types, *direction, target_labels, context, statistics),
        GraphLogicalPlan::VariableExpand {
            input,
            relationship_types,
            direction,
            target_labels,
            min_hops,
            max_hops,
            ..
        } => {
            let one_hop = estimate_expand_rows(input, relationship_types, *direction, target_labels, context, statistics)?;
            // An unbounded walk has no estimate worth giving.
            let hops = u64::from((*max_hops)?.saturating_sub(*min_hops).saturating_add(1));
            Some(one_hop.saturating_mul(hops))
        }
        GraphLogicalPlan::NaturalJoin { left, right, .. }
        | GraphLogicalPlan::CallProcedure {
            input: left,
            subquery: right,
            ..
        } => Some(estimate_rows(left, context, statistics)?.saturating_mul(estimate_rows(right, context, statistics)?)),
        GraphLogicalPlan::Exists { input, .. }
        | GraphLogicalPlan::ScalarSubquery { input, .. }
        | GraphLogicalPlan::Finish { input }
        | GraphLogicalPlan::SelectPaths { input, .. }
        | GraphLogicalPlan::BindPath { input, .. } => estimate_rows(input, context, statistics),
        GraphLogicalPlan::PathUnion { inputs, .. } => inputs.iter().try_fold(0_u64, |total, input| {
            Some(total.saturating_add(estimate_rows(input, context, statistics)?))
        }),
        GraphLogicalPlan::Next { next, .. } => estimate_rows(next, context, statistics),
    }
}

/// Relationships an equality on `relationship.property` matches across all of
/// `values`: each value's exact count, else a bloom filter's proof of absence,
/// else the histogram's average per distinct value. `None` when any value has
/// no estimate.
fn relationship_values_estimate(
    relationship_types: &[Symbol],
    property: &Symbol,
    values: &[BoundValue],
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let histogram_estimate = statistics
        .relationship_property_statistics(relationship_types, property)
        .as_ref()
        .and_then(property_equality_estimate);
    values.iter().try_fold(0_u64, |total, value| {
        let estimate = statistics
            .relationship_property_value_count(relationship_types, property, &value.value)
            .or_else(|| {
                statistics
                    .relationship_bloom_may_contain(relationship_types, property, &value.value)
                    .and_then(|possibly_present| (!possibly_present).then_some(0))
            })
            .or(histogram_estimate)?;
        Some(total.saturating_add(estimate))
    })
}

fn estimate_filter_rows(
    input: &GraphLogicalPlan,
    predicate: &PhysicalExpression,
    input_rows: Option<u64>,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    if let GraphLogicalPlan::NodeScan { binding, labels } = input {
        if let Some((constraint_binding, property, values)) = equality_constraint(predicate) {
            if constraint_binding == *binding {
                if let Some(rows) = property_values_estimate(labels, &property, &values, statistics) {
                    return Some(input_rows.map_or(rows, |input| input.min(rows)));
                }
            }
        }
        if let Some((constraint_binding, property)) = property_scan_constraint(predicate) {
            if constraint_binding == *binding {
                if let Some(rows) = statistics
                    .property_statistics(labels, &property)
                    .and_then(|stats| stats.non_null_count)
                {
                    return Some(input_rows.map_or(rows, |input| input.min(rows)));
                }
            }
        }
    }
    if let GraphLogicalPlan::Expand {
        relationship: Some(relationship),
        relationship_types,
        ..
    } = input
    {
        if let Some((binding, property, values)) = equality_constraint_for_binding(predicate, relationship) {
            if binding == *relationship {
                if let Some(rows) = relationship_values_estimate(relationship_types, &property, &values, statistics) {
                    return Some(input_rows.map_or(rows, |input| input.min(rows)));
                }
            }
        }
    }
    // Unknown selectivity is not evidence. Preserve the input estimate rather
    // than inventing a reduction that could make a scan appear cheaper than a
    // known index access.
    input_rows
}

fn estimate_expand_rows(
    input: &GraphLogicalPlan,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    _target_labels: &[Symbol],
    context: &PhysicalPlanningContext,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let input_rows = estimate_rows(input, context, statistics);
    if input_rows == Some(0) {
        return Some(0);
    }
    estimate_anchored_expansion_rows(input, relationship_types, direction, statistics)
        .or_else(|| statistics.relationship_count(relationship_types))
}

fn estimate_anchored_expansion_rows(
    input: &GraphLogicalPlan,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let source_labels = vertex_labels_for_plan(input)?;
    statistics.relationship_expansion_count(&source_labels, relationship_types, direction)
}

fn vertex_labels_for_plan(plan: &GraphLogicalPlan) -> Option<Vec<Symbol>> {
    match plan {
        GraphLogicalPlan::NodeScan { labels, .. } => Some(labels.clone()),
        GraphLogicalPlan::Filter { input, .. }
        | GraphLogicalPlan::Sort { input, .. }
        | GraphLogicalPlan::Skip { input, .. }
        | GraphLogicalPlan::Limit { input, .. }
        | GraphLogicalPlan::Project { input, .. } => vertex_labels_for_plan(input),
        _ => None,
    }
}
