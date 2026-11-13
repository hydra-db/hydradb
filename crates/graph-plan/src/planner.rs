use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BoundValue, GraphLogicalPlan, GraphPhysicalPlan, GraphPlanError, GraphPlanResult, LogicalBinaryOperator, LogicalExpression,
    LogicalProjection, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection, ScalarValue, Symbol, ValueOrigin,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalPlanningContext {
    pub parameters: BTreeMap<Symbol, ScalarValue>,
}

impl PhysicalPlanningContext {
    pub fn with_parameter(mut self, name: impl Into<Symbol>, value: ScalarValue) -> Self {
        self.parameters.insert(name.into(), value);
        self
    }
}

pub fn plan_physical(logical: &GraphLogicalPlan, context: &PhysicalPlanningContext) -> GraphPlanResult<GraphPhysicalPlan> {
    match logical {
        GraphLogicalPlan::NodeScan { binding, labels } => Ok(fallback_scan(binding, labels)),
        GraphLogicalPlan::Filter { input, predicate } => plan_filter(input, predicate, context),
        GraphLogicalPlan::Project { input, items } => Ok(GraphPhysicalPlan::Project {
            input: Box::new(plan_physical(input, context)?),
            items: items
                .iter()
                .map(|item| bind_projection(item, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
        }),
    }
}

fn plan_filter(
    input: &GraphLogicalPlan,
    predicate: &LogicalExpression,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<GraphPhysicalPlan> {
    let bound_predicate = bind_expression(predicate, context)?;
    let access = match input {
        GraphLogicalPlan::NodeScan { binding, labels } => match equality_constraint(&bound_predicate) {
            Some((constraint_binding, property, values)) if constraint_binding == *binding => {
                if values.len() == 1 {
                    GraphPhysicalPlan::VertexPropertySeek {
                        binding: binding.clone(),
                        labels: labels.clone(),
                        property,
                        value: values.into_iter().next().expect("one value"),
                    }
                } else {
                    GraphPhysicalPlan::VertexPropertyMultiSeek {
                        binding: binding.clone(),
                        labels: labels.clone(),
                        property,
                        values,
                    }
                }
            }
            _ => fallback_scan(binding, labels),
        },
        _ => plan_physical(input, context)?,
    };
    Ok(GraphPhysicalPlan::Filter {
        input: Box::new(access),
        predicate: bound_predicate,
    })
}

fn fallback_scan(binding: &Symbol, labels: &[Symbol]) -> GraphPhysicalPlan {
    match labels {
        [label, ..] => GraphPhysicalPlan::VertexLabelScan {
            binding: binding.clone(),
            label: label.clone(),
        },
        [] => GraphPhysicalPlan::AllVertexScan { binding: binding.clone() },
    }
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
        LogicalExpression::Property { binding, property } => PhysicalExpression::Property {
            binding: binding.clone(),
            property: property.clone(),
        },
        LogicalExpression::Parameter(parameter) => {
            let value = context
                .parameters
                .get(parameter)
                .cloned()
                .ok_or_else(|| GraphPlanError::MissingParameter(parameter.as_str().to_string()))?;
            PhysicalExpression::Value(BoundValue {
                value,
                origin: ValueOrigin::Parameter(parameter.clone()),
            })
        }
        LogicalExpression::Literal(value) => PhysicalExpression::Value(BoundValue {
            value: value.clone(),
            origin: ValueOrigin::Literal,
        }),
        LogicalExpression::Binary { left, operator, right } => PhysicalExpression::Binary {
            left: Box::new(bind_expression(left, context)?),
            operator: match operator {
                LogicalBinaryOperator::Equal => PhysicalBinaryOperator::Equal,
                LogicalBinaryOperator::Or => PhysicalBinaryOperator::Or,
            },
            right: Box::new(bind_expression(right, context)?),
        },
    })
}

fn equality_constraint(predicate: &PhysicalExpression) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => property_value(left, right).or_else(|| property_value(right, left)),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let (left_binding, left_property, left_values) = equality_constraint(left)?;
            let (right_binding, right_property, right_values) = equality_constraint(right)?;
            if left_binding != right_binding || left_property != right_property {
                return None;
            }
            let mut unique = BTreeSet::new();
            unique.extend(left_values);
            unique.extend(right_values);
            Some((left_binding, left_property, unique.into_iter().collect()))
        }
        _ => None,
    }
}

fn property_value(property: &PhysicalExpression, value: &PhysicalExpression) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
    let PhysicalExpression::Property { binding, property } = property else {
        return None;
    };
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    (!matches!(value.value, ScalarValue::Null)).then(|| (binding.clone(), property.clone(), vec![value.clone()]))
}
