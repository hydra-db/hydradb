//! Ordered source rewrites. Neither rule reorders joins or hops.
use std::sync::Arc;

use hydradb_cypher_ast::{operation, Identifier, LabelExpression};

use crate::{
    BoundValue, ElementTestKind, GraphLogicalPlan as Plan, LogicalBinaryOperator as Binary, LogicalExpression as Expr, LogicalProjection,
    PhysicalPlanningContext, ScalarFunction, ScalarValue, Symbol, ValueOrigin,
};

struct IdStream {
    alias: Symbol,
    source: Symbol,
    key: Expr,
    list: Expr,
}

pub(crate) fn rewrite(plan: &Arc<Plan>, context: &PhysicalPlanningContext) -> Arc<Plan> {
    if !has_join(plan) {
        return Arc::clone(plan);
    }
    let mut rewritten = plan.as_ref().clone();
    // First normalize connected comma patterns within the UNWIND's right side.
    connect(&mut rewritten);
    collapse_stream(&mut rewritten, context);
    connect(&mut rewritten);
    Arc::new(rewritten)
}

fn inputs(plan: &Plan) -> Vec<&Plan> {
    match plan {
        Plan::Filter { input, .. }
        | Plan::Project { input, .. }
        | Plan::Sort { input, .. }
        | Plan::Skip { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Expand { input, .. }
        | Plan::BoundRelationship { input, .. }
        | Plan::VariableExpand { input, .. }
        | Plan::BindPath { input, .. }
        | Plan::Unwind { input, .. } => vec![input],
        Plan::NaturalJoin {
            left,
            right,
            optional: false,
        } => vec![left, right],
        _ => vec![],
    }
}

fn inputs_mut(plan: &mut Plan) -> Vec<&mut Plan> {
    match plan {
        Plan::Filter { input, .. }
        | Plan::Project { input, .. }
        | Plan::Sort { input, .. }
        | Plan::Skip { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Expand { input, .. }
        | Plan::BoundRelationship { input, .. }
        | Plan::VariableExpand { input, .. }
        | Plan::BindPath { input, .. }
        | Plan::Unwind { input, .. } => vec![input],
        Plan::NaturalJoin {
            left,
            right,
            optional: false,
        } => vec![left, right],
        _ => vec![],
    }
}

fn has_join(plan: &Plan) -> bool {
    matches!(plan, Plan::NaturalJoin { optional: false, .. }) || inputs(plan).into_iter().any(has_join)
}

/// Follow the right-hand MATCH pipeline to its bound source scan.
/// Stop at multi-input operators, projections and subquery/window boundaries.
fn source_scan(plan: &Plan) -> Option<&Symbol> {
    match plan {
        Plan::NodeScan {
            binding, lookup_ids: None, ..
        } => Some(binding),
        Plan::Filter { input, .. }
        | Plan::Expand { input, .. }
        | Plan::BoundRelationship { input, .. }
        | Plan::VariableExpand { input, .. }
        | Plan::BindPath { input, .. } => source_scan(input),
        _ => None,
    }
}

/// Whether the right-hand MATCH pipeline starts from a reused relationship
/// over the seed row itself: `BoundRelationship(SingleRow)`, which lowering
/// emits when the relationship rather than a scan binds the path's first
/// node. Correlated, that row is the left row, so the pipeline runs over the
/// left side's rows directly, as one whose source scan the left side produces
/// does.
fn starts_from_row(plan: &Plan) -> bool {
    match plan {
        Plan::BoundRelationship { input, .. } if matches!(input.as_ref(), Plan::SingleRow) => true,
        Plan::Filter { input, .. }
        | Plan::Expand { input, .. }
        | Plan::BoundRelationship { input, .. }
        | Plan::VariableExpand { input, .. }
        | Plan::BindPath { input, .. } => starts_from_row(input),
        _ => false,
    }
}

fn no_hops(plan: &Plan) -> bool {
    match plan {
        Plan::NodeScan { .. } => true,
        Plan::Filter { input, .. } => no_hops(input),
        _ => false,
    }
}

fn identity(expression: &Expr, binding: &Symbol) -> bool {
    matches!(expression, Expr::Identity(name) if name == binding)
}

fn key_equality<'a>(expression: &'a Expr, source: &Symbol, alias: &Symbol) -> Option<&'a Expr> {
    match expression {
        Expr::Binary {
            left,
            operator: Binary::Equal,
            right,
        } => {
            let key = if identity(left, source) {
                right
            } else if identity(right, source) {
                left
            } else {
                return None;
            };
            match key.as_ref() {
                Expr::Binding(name) if name == alias => Some(key),
                _ => None,
            }
        }
        Expr::Binary {
            left,
            operator: Binary::And,
            right,
        } => key_equality(left, source, alias).or_else(|| key_equality(right, source, alias)),
        _ => None,
    }
}

fn source_key<'a>(plan: &'a Plan, source: &Symbol, alias: &Symbol) -> Option<&'a Expr> {
    match plan {
        Plan::Expand { input, .. }
        | Plan::BoundRelationship { input, .. }
        | Plan::VariableExpand { input, .. }
        | Plan::BindPath { input, .. } => source_key(input, source, alias),
        Plan::Filter { input, predicate, .. } => {
            if no_hops(input) {
                key_equality(predicate, source, alias).or_else(|| source_key(input, source, alias))
            } else {
                source_key(input, source, alias)
            }
        }
        _ => None,
    }
}

fn id_stream(plan: &Plan) -> Option<IdStream> {
    if let Plan::NaturalJoin {
        left,
        right,
        optional: false,
    } = plan
    {
        if let Plan::Unwind {
            input,
            expression,
            binding,
            requires_list: false,
            position: None,
        } = left.as_ref()
        {
            if matches!(input.as_ref(), Plan::SingleRow) && matches!(expression, Expr::Parameter(_) | Expr::List(_)) {
                let source = source_scan(right)?;
                if source == binding {
                    return None;
                }
                let key = source_key(right, source, binding)?.clone();
                return Some(IdStream {
                    alias: binding.clone(),
                    source: source.clone(),
                    key,
                    list: expression.clone(),
                });
            }
        }
    }
    None
}

fn remove_key(expression: &Expr, stream: &IdStream) -> Option<Expr> {
    if key_equality(expression, &stream.source, &stream.alias) == Some(&stream.key)
        && matches!(
            expression,
            Expr::Binary {
                operator: Binary::Equal,
                ..
            }
        )
    {
        return None;
    }
    if let Expr::Binary {
        left,
        operator: Binary::And,
        right,
    } = expression
    {
        return match (remove_key(left, stream), remove_key(right, stream)) {
            (Some(left), Some(right)) => Some(Expr::Binary {
                left: Box::new(left),
                operator: Binary::And,
                right: Box::new(right),
            }),
            (left, right) => left.or(right),
        };
    }
    Some(expression.clone())
}

fn lookup_values(stream: &IdStream, context: &PhysicalPlanningContext) -> Option<Vec<BoundValue>> {
    let values = match &stream.list {
        Expr::Parameter(parameter) => {
            // Match bind_expression's precedence, including reserved session
            // and request parameters. A list sidecar must not override a scalar.
            if context.parameters.contains_key(parameter) {
                return None;
            }
            context
                .list_parameters
                .get(parameter)?
                .iter()
                .cloned()
                .map(|value| BoundValue {
                    value,
                    origin: ValueOrigin::Parameter(parameter.clone()),
                })
                .collect::<Vec<_>>()
        }
        Expr::List(items) => items
            .iter()
            .map(|item| match item {
                Expr::Literal(value) => Some(BoundValue {
                    value: value.clone(),
                    origin: ValueOrigin::Literal,
                }),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?,
        _ => return None,
    };
    values
        .iter()
        .all(|value| matches!(value.value, ScalarValue::Integer(id) if u64::try_from(id).is_ok()))
        .then_some(values)
}

fn collapse_stream(plan: &mut Plan, context: &PhysicalPlanningContext) {
    for input in inputs_mut(plan) {
        collapse_stream(input, context);
    }
    if let Some((stream, values)) = id_stream(plan).and_then(|stream| lookup_values(&stream, context).map(|values| (stream, values))) {
        if let Plan::NaturalJoin { right, .. } = plan {
            let mut source = right.as_ref().clone();
            set_lookup(&mut source, &stream, &values);
            *plan = source;
        }
    }
}

fn set_lookup(plan: &mut Plan, stream: &IdStream, values: &[BoundValue]) {
    match plan {
        Plan::NodeScan { lookup_ids, binding, .. } if binding == &stream.source => {
            *lookup_ids = Some(values.to_vec());
            // Preserve the removed join's scalar binding for every consumer,
            // including filters inside this spine and operators above it.
            *plan = Plan::Project {
                context: Default::default(),
                operation: operation::MATCH,
                input: Box::new(plan.clone()),
                items: vec![
                    LogicalProjection {
                        expression: Expr::Binding(stream.source.clone()),
                        alias: None,
                    },
                    LogicalProjection {
                        expression: Expr::Function {
                            function: ScalarFunction::Id,
                            arguments: vec![Expr::Binding(stream.source.clone())],
                        },
                        alias: Some(stream.alias.clone()),
                    },
                ],
                distinct: false,
                post_sort: Vec::new(),
                post_skip: None,
                post_limit: None,
            };
        }
        Plan::Filter { input, predicate, .. } if no_hops(input) => {
            if let Some(residual) = remove_key(predicate, stream) {
                *predicate = residual;
                set_lookup(input, stream, values);
            } else {
                let mut source = input.as_ref().clone();
                set_lookup(&mut source, stream, values);
                *plan = source;
            }
        }
        Plan::Expand { input, .. }
        | Plan::BoundRelationship { input, .. }
        | Plan::VariableExpand { input, .. }
        | Plan::BindPath { input, .. }
        | Plan::Filter { input, .. } => set_lookup(input, stream, values),
        _ => {}
    }
}

fn produces_vertex(plan: &Plan, name: &Symbol) -> bool {
    match plan {
        Plan::NodeScan { binding, .. } => binding == name,
        Plan::Expand { input, to, .. } | Plan::VariableExpand { input, to, .. } => to == name || produces_vertex(input, name),
        Plan::BoundRelationship { input, from, to, .. } => from == name || to == name || produces_vertex(input, name),
        Plan::Filter { input, .. }
        | Plan::Sort { input, .. }
        | Plan::Skip { input, .. }
        | Plan::Limit { input, .. }
        | Plan::BindPath { input, .. }
        | Plan::Unwind { input, .. } => produces_vertex(input, name),
        Plan::NaturalJoin { left, right, optional: false } => produces_vertex(left, name) || produces_vertex(right, name),
        Plan::Project { input, items, context, .. }
            if context.fixed.is_empty() && context.defined.is_empty() => items.iter().any(|item|
            matches!(&item.expression, Expr::Binding(source) if item.alias.as_ref().unwrap_or(source) == name && produces_vertex(input, source))),
        _ => false,
    }
}

fn graft(plan: &mut Plan, left: &Plan) {
    match plan {
        Plan::NodeScan { binding, labels, .. } => {
            let binding = binding.clone();
            let labels = labels.clone();
            *plan = left.clone();
            if !labels.is_empty() {
                *plan = Plan::Filter {
                    operation: operation::MATCH,
                    input: Box::new(plan.clone()),
                    predicate: Expr::ElementTest {
                        binding,
                        test: ElementTestKind::Labeled(LabelExpression::And(
                            labels
                                .iter()
                                .map(|label| LabelExpression::Label(Identifier::new(label.as_str())))
                                .collect(),
                        )),
                    },
                };
            }
        }
        // The seed row a reused relationship starts from: the left rows.
        Plan::SingleRow => *plan = left.clone(),
        Plan::Expand { input, .. }
        | Plan::BoundRelationship { input, .. }
        | Plan::VariableExpand { input, .. }
        | Plan::BindPath { input, .. }
        | Plan::Filter { input, .. } => graft(input, left),
        _ => unreachable!("source spine was checked"),
    }
}

fn connect(plan: &mut Plan) {
    for input in inputs_mut(plan) {
        connect(input);
    }
    if let Plan::NaturalJoin {
        left,
        right,
        optional: false,
    } = plan
    {
        if source_scan(right).is_some_and(|source| produces_vertex(left, source)) || starts_from_row(right) {
            let mut linear = right.as_ref().clone();
            graft(&mut linear, left);
            *plan = linear;
        }
    }
}
