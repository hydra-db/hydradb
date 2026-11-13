use std::fmt::Write;

use crate::{
    BoundValue, GraphPhysicalPlan, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection, ScalarValue, Symbol, ValueOrigin,
};

pub fn explain_physical_plan(plan: &GraphPhysicalPlan) -> String {
    let mut output = String::new();
    render_plan(plan, 0, &mut output);
    output
}

fn render_plan(plan: &GraphPhysicalPlan, depth: usize, output: &mut String) {
    let indent = "  ".repeat(depth);
    match plan {
        GraphPhysicalPlan::VertexPropertySeek {
            binding,
            labels,
            property,
            value,
        } => {
            let _ = writeln!(
                output,
                "{indent}VertexPropertySeek(binding={binding}, labels={}, property={property}, value={})",
                render_symbols(labels),
                render_value(value)
            );
        }
        GraphPhysicalPlan::VertexPropertyMultiSeek {
            binding,
            labels,
            property,
            values,
        } => {
            let values = values.iter().map(render_value).collect::<Vec<_>>().join(", ");
            let _ = writeln!(
                output,
                "{indent}VertexPropertyMultiSeek(binding={binding}, labels={}, property={property}, values=[{values}])",
                render_symbols(labels)
            );
        }
        GraphPhysicalPlan::VertexLabelScan { binding, label } => {
            let _ = writeln!(output, "{indent}VertexLabelScan(binding={binding}, label={label})");
        }
        GraphPhysicalPlan::AllVertexScan { binding } => {
            let _ = writeln!(output, "{indent}AllVertexScan(binding={binding})");
        }
        GraphPhysicalPlan::Filter { input, predicate } => {
            let _ = writeln!(output, "{indent}FilterExec(predicate={})", render_expression(predicate));
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Project { input, items } => {
            let items = items.iter().map(render_projection).collect::<Vec<_>>().join(", ");
            let _ = writeln!(output, "{indent}ProjectExec(items=[{items}])");
            render_plan(input, depth + 1, output);
        }
    }
}

fn render_projection(projection: &PhysicalProjection) -> String {
    let expression = render_expression(&projection.expression);
    match &projection.alias {
        Some(alias) => format!("{expression} AS {alias}"),
        None => expression,
    }
}

fn render_expression(expression: &PhysicalExpression) -> String {
    match expression {
        PhysicalExpression::Binding(binding) => binding.to_string(),
        PhysicalExpression::Property { binding, property } => format!("{binding}.{property}"),
        PhysicalExpression::Value(value) => render_value(value),
        PhysicalExpression::Binary { left, operator, right } => {
            let operator = match operator {
                PhysicalBinaryOperator::Equal => "=",
                PhysicalBinaryOperator::Or => "OR",
            };
            format!("({} {operator} {})", render_expression(left), render_expression(right))
        }
    }
}

fn render_value(value: &BoundValue) -> String {
    match &value.origin {
        ValueOrigin::Parameter(name) => format!("${name}"),
        ValueOrigin::Literal => match &value.value {
            ScalarValue::Null => "NULL".to_string(),
            ScalarValue::Boolean(value) => value.to_string(),
            ScalarValue::Integer(value) => value.to_string(),
            ScalarValue::Float(value) => value.to_string(),
            ScalarValue::String(value) => format!("'{value}'"),
        },
    }
}

fn render_symbols(symbols: &[Symbol]) -> String {
    format!("[{}]", symbols.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "))
}
