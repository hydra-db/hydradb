use hydradb_cypher_ast::{BinaryOperator, Clause, Document, Expression, Literal, PatternElement, ProjectionItems, Statement};

use crate::{
    GraphLogicalPlan, GraphPlanError, GraphPlanResult, LogicalBinaryOperator, LogicalExpression, LogicalProjection, ScalarValue, Symbol,
};

pub fn lower_cypher_ast(document: &Document) -> GraphPlanResult<GraphLogicalPlan> {
    let [Statement::Query(query)] = document.statements.as_slice() else {
        return Err(GraphPlanError::UnsupportedCypher(
            "expected exactly one typed query statement".to_string(),
        ));
    };
    let [Clause::Match(matched), Clause::Return(returned)] = query.clauses.as_slice() else {
        return Err(GraphPlanError::UnsupportedCypher("expected MATCH followed by RETURN".to_string()));
    };
    if matched.optional || matched.mode.is_some() || matched.patterns.len() != 1 {
        return Err(GraphPlanError::UnsupportedCypher(
            "the first slice supports one required MATCH pattern".to_string(),
        ));
    }
    let [PatternElement::Node(node)] = matched.patterns[0].elements.as_slice() else {
        return Err(GraphPlanError::UnsupportedCypher(
            "the first slice supports one node pattern".to_string(),
        ));
    };
    if node.properties.is_some() || node.predicate.is_some() {
        return Err(GraphPlanError::UnsupportedCypher(
            "inline node predicates are not lowered yet".to_string(),
        ));
    }
    let binding = node
        .variable
        .as_ref()
        .ok_or_else(|| GraphPlanError::UnsupportedCypher("the matched node must have a binding".to_string()))?;
    let mut plan = GraphLogicalPlan::NodeScan {
        binding: Symbol::from(binding.as_str()),
        labels: node.labels.iter().map(|label| Symbol::from(label.as_str())).collect(),
    };
    if let Some(predicate) = &matched.predicate {
        plan = GraphLogicalPlan::Filter {
            input: Box::new(plan),
            predicate: lower_expression(predicate)?,
        };
    }
    if returned.quantifier.is_some()
        || !returned.group_by.is_empty()
        || !returned.order_by.is_empty()
        || returned.skip.is_some()
        || returned.limit.is_some()
    {
        return Err(GraphPlanError::UnsupportedCypher(
            "aggregation and result windows are not lowered yet".to_string(),
        ));
    }
    let ProjectionItems::Items(items) = &returned.items else {
        return Err(GraphPlanError::UnsupportedCypher("RETURN * is not lowered yet".to_string()));
    };
    let items = items
        .iter()
        .map(|item| {
            Ok(LogicalProjection {
                expression: lower_expression(&item.expression)?,
                alias: item.alias.as_ref().map(|alias| Symbol::from(alias.as_str())),
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    Ok(GraphLogicalPlan::Project {
        input: Box::new(plan),
        items,
    })
}

fn lower_expression(expression: &Expression) -> GraphPlanResult<LogicalExpression> {
    Ok(match expression {
        Expression::Identifier(identifier) => LogicalExpression::Binding(Symbol::from(identifier.as_str())),
        Expression::Property { target, key } => {
            let Expression::Identifier(binding) = target.as_ref() else {
                return Err(GraphPlanError::UnsupportedCypher("property targets must be bindings".to_string()));
            };
            LogicalExpression::Property {
                binding: Symbol::from(binding.as_str()),
                property: Symbol::from(key.as_str()),
            }
        }
        Expression::Parameter(parameter) => LogicalExpression::Parameter(Symbol::from(parameter.as_str())),
        Expression::Literal(literal) => LogicalExpression::Literal(match literal {
            Literal::Null => ScalarValue::Null,
            Literal::Boolean(value) => ScalarValue::Boolean(*value),
            Literal::Integer(value) => ScalarValue::Integer(*value),
            Literal::Float(value) => ScalarValue::Float(value.clone()),
            Literal::String(value) => ScalarValue::String(value.clone()),
            Literal::Decimal(_) | Literal::Bytes(_) => {
                return Err(GraphPlanError::UnsupportedCypher(
                    "decimal and byte string literals are not supported".to_string(),
                ))
            }
        }),
        Expression::Binary { left, operator, right } => LogicalExpression::Binary {
            left: Box::new(lower_expression(left)?),
            operator: match operator {
                BinaryOperator::Equal => LogicalBinaryOperator::Equal,
                BinaryOperator::Or => LogicalBinaryOperator::Or,
                _ => {
                    return Err(GraphPlanError::UnsupportedCypher(format!(
                        "binary operator {operator:?} is not lowered yet"
                    )))
                }
            },
            right: Box::new(lower_expression(right)?),
        },
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                "expression is outside the initial typed slice".to_string(),
            ))
        }
    })
}
