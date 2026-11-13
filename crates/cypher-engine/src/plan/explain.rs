use std::fmt::{self, Write};

use crate::{
    BoundValue, GraphPhysicalPlan, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection, PhysicalSort, PhysicalUnaryOperator,
    ScalarValue, Symbol, ValueOrigin,
};
use hydradb_cypher_ast::{NullOrdering, PathSearchKind, QueryConjunction};

pub fn explain_physical_plan(plan: &GraphPhysicalPlan) -> String {
    let mut output = String::new();
    render_plan(plan, 0, &mut output);
    output
}

thread_local! {
    /// Set only while [`explain_physical_plan_shape`] renders. Rendering is
    /// synchronous, so a thread-local scoped to one call cannot leak into
    /// another plan's output.
    static REDACT_LITERALS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn redacting_literals() -> bool {
    REDACT_LITERALS.with(std::cell::Cell::get)
}

/// A physical field the planner *derived* from something the shape must not
/// split on: a literal, as `OrderedVertexPropertyScan.required` is derived
/// from `SKIP + LIMIT`, or the statistics behind `SortExec`'s estimate.
///
/// These are plain integers by the time they reach a plan, so [`write_value`]
/// never sees them and redacting the `BoundValue` they came from does not
/// redact the copy. Rendered as `?` in a shape, they stop `LIMIT 5` and
/// `LIMIT 10` — or one query before and after a statistics refresh — from
/// hashing to two plan identities for one operator tree.
pub(crate) fn render_derived(value: impl fmt::Display) -> String {
    if redacting_literals() {
        "?".to_string()
    } else {
        value.to_string()
    }
}

/// [`explain_physical_plan`] with every literal value rendered as `?` and
/// every value list elided.
///
/// Parameters already render by name, so this is the plan with no tenant
/// value in it: safe to attach to a trace, and stable across requests that
/// differ only in literals, which is what makes it hashable into a plan
/// identity.
pub fn explain_physical_plan_shape(plan: &GraphPhysicalPlan) -> String {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            REDACT_LITERALS.with(|redact| redact.set(false));
        }
    }
    REDACT_LITERALS.with(|redact| redact.set(true));
    let _reset = Reset;
    explain_physical_plan(plan)
}

fn render_plan(plan: &GraphPhysicalPlan, depth: usize, output: &mut String) {
    let indent = "  ".repeat(depth);
    match plan {
        GraphPhysicalPlan::Union { arms, all, conjunction } => {
            let name = plan.operator_name();
            let _ = if *conjunction == QueryConjunction::Otherwise {
                writeln!(output, "{indent}{name}")
            } else {
                writeln!(output, "{indent}{name}(all={all})")
            };
            for arm in arms {
                render_plan(arm, depth + 1, output);
            }
        }
        GraphPhysicalPlan::VertexScan { scan } => scan.render(depth, output),
        GraphPhysicalPlan::OrderedRelationshipPropertyScan {
            from,
            source_labels,
            relationship,
            relationship_type,
            direction,
            to,
            target_labels,
            property,
            sort_direction,
            prefix,
            lower,
            upper,
            required,
            items,
            residual,
        } => {
            let residual = residual.as_ref().map(render_expression).unwrap_or_else(|| "none".to_string());
            let prefix = prefix.as_ref().map(render_value).unwrap_or_else(|| "none".to_string());
            let lower = render_bound(lower.as_ref());
            let upper = render_bound(upper.as_ref());
            let _ = writeln!(
                output,
                "{indent}OrderedRelationshipPropertyScan(from={from}, source_labels={}, relationship={relationship}, type={relationship_type}, direction={direction}, to={to}, target_labels={}, property={property}, sort={sort_direction}, prefix={prefix}, lower={lower}, upper={upper}, required={required}, items=[{}], residual={residual})",
                render_symbols(source_labels),
                render_symbols(target_labels),
                SortList(items)
            );
        }
        GraphPhysicalPlan::SingleRow => {
            let _ = writeln!(output, "{indent}SingleRowExec");
        }
        GraphPhysicalPlan::Unwind {
            input,
            expression,
            binding,
            position,
            ..
        } => {
            let position = match position {
                Some((name, first)) => format!(", position={name} from {first}"),
                None => String::new(),
            };
            let _ = writeln!(
                output,
                "{indent}UnwindExec(binding={binding}, expression={}{position})",
                render_expression(expression)
            );
            render_plan(input, depth + 1, output);
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
            let _ = writeln!(
                output,
                "{indent}RelationshipPropertySeek(from={from}, source_labels={}, relationship={relationship}, type={relationship_type}, property={property}, value={}, direction={direction}, to={to}, target_labels={})",
                render_symbols(source_labels),
                render_value(value),
                render_symbols(target_labels)
            );
        }
        GraphPhysicalPlan::RelationshipPropertyMultiSeek {
            from,
            source_labels,
            relationship,
            relationship_type,
            property,
            values,
            direction,
            to,
            target_labels,
        } => {
            // As for the vertex multi-seek: a plan shape must not vary with
            // how many values a list bound.
            let values = if redacting_literals() {
                "...".to_string()
            } else {
                values.iter().map(render_value).collect::<Vec<_>>().join(", ")
            };
            let _ = writeln!(
                output,
                "{indent}RelationshipPropertyMultiSeek(from={from}, source_labels={}, relationship={relationship}, type={relationship_type}, property={property}, values=[{values}], direction={direction}, to={to}, target_labels={})",
                render_symbols(source_labels),
                render_symbols(target_labels)
            );
        }
        GraphPhysicalPlan::RelationshipPropertyRangeProbe {
            from,
            source_labels,
            relationship,
            relationship_type,
            property,
            lower,
            inclusive,
            direction,
            to,
            target_labels,
            fallback,
        } => {
            let comparison = if *inclusive { ">=" } else { ">" };
            let _ = writeln!(
                output,
                "{indent}RelationshipPropertyRangeProbe(from={from}, source_labels={}, relationship={relationship}, type={relationship_type}, property={property}, lower{comparison}{}, direction={direction}, to={to}, target_labels={})",
                render_symbols(source_labels),
                render_value(lower),
                render_symbols(target_labels)
            );
            render_plan(fallback, depth + 1, output);
        }
        GraphPhysicalPlan::RelationshipTypeProbe {
            from,
            source_labels,
            relationship,
            relationship_type,
            direction,
            to,
            target_labels,
            fallback,
        } => {
            let _ = writeln!(
                output,
                "{indent}RelationshipTypeProbe(from={from}, source_labels={}, relationship={relationship}, type={relationship_type}, direction={direction}, to={to}, target_labels={})",
                render_symbols(source_labels),
                render_symbols(target_labels)
            );
            render_plan(fallback, depth + 1, output);
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
            let relationship = relationship.as_ref().map(ToString::to_string).unwrap_or_else(|| "_".to_string());
            let _ = writeln!(
                output,
                "{indent}ExpandExec(from={from}, relationship={relationship}, types={}, direction={direction}, to={to}, target_labels={})",
                render_symbols(relationship_types),
                render_symbols(target_labels)
            );
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::VariableExpand {
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
        } => {
            // `*1..2` and `*1..5` are the same operator tree over the same
            // schema names, so they are one shape however differently they
            // read: the bounds come from pattern literals exactly as `LIMIT`
            // does, and `LimitExec(count=?)` has always been redacted.
            let max_hops = max_hops.map(render_derived).unwrap_or_default();
            let hops = format!("{}..{max_hops}", render_derived(min_hops));
            // Printed only when bound, so an unbound walk keeps the shape and
            // plan hash it has always had.
            let relationship = relationship
                .as_ref()
                .map(|relationship| format!(", relationship={relationship}"))
                .unwrap_or_default();
            // Printed only for a walk, so a trail keeps its shape and hash.
            let walk = if *trail { "" } else { ", walk" };
            // Likewise printed only when a path mode prunes the walk.
            let walk = match nodes {
                crate::WalkNodes::Any => walk.to_string(),
                crate::WalkNodes::Simple => format!("{walk}, simple"),
                crate::WalkNodes::Acyclic => format!("{walk}, acyclic"),
            };
            let _ = writeln!(
                output,
                "{indent}VariableExpandExec(from={from}{relationship}, types={}, direction={direction}, to={to}, target_labels={}, hops={hops}{walk})",
                render_symbols(relationship_types),
                render_symbols(target_labels)
            );
            render_plan(input, depth + 1, output);
            if let Some(iteration) = iteration {
                let _ = writeln!(
                    output,
                    "{indent}  Iteration(source={}, relationship={}, target={})",
                    iteration.source, iteration.relationship, iteration.target
                );
                render_plan(&iteration.plan, depth + 2, output);
            }
        }
        GraphPhysicalPlan::Exists { input, subquery, binding } => {
            let _ = writeln!(output, "{indent}ExistsMarkExec(binding={binding})");
            render_plan(input, depth + 1, output);
            render_plan(subquery, depth + 1, output);
        }
        GraphPhysicalPlan::ScalarSubquery {
            input,
            subquery,
            column,
            binding,
        } => {
            let _ = writeln!(output, "{indent}ScalarSubqueryExec(binding={binding}, column={column})");
            render_plan(input, depth + 1, output);
            render_plan(subquery, depth + 1, output);
        }
        GraphPhysicalPlan::CallProcedure {
            input,
            subquery,
            columns,
            optional,
            ..
        } => {
            let columns = columns.iter().map(Symbol::as_str).collect::<Vec<_>>().join(", ");
            let _ = writeln!(output, "{indent}CallProcedureExec(columns=[{columns}], optional={optional})");
            render_plan(input, depth + 1, output);
            render_plan(subquery, depth + 1, output);
        }
        GraphPhysicalPlan::Next { input, next } => {
            let _ = writeln!(output, "{indent}{}", plan.operator_name());
            render_plan(input, depth + 1, output);
            render_plan(next, depth + 1, output);
        }
        GraphPhysicalPlan::Finish { input } => {
            let _ = writeln!(output, "{indent}FinishExec");
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::BindPath { input, names, .. } => {
            let _ = writeln!(output, "{indent}BindPathExec(names={})", render_symbols(names));
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::SelectPaths {
            input,
            first,
            last,
            kind,
            count,
            ..
        } => {
            let kind = match kind {
                PathSearchKind::Any => "any",
                PathSearchKind::Shortest => "shortest",
                PathSearchKind::ShortestGroups => "shortest_groups",
            };
            let _ = writeln!(
                output,
                "{indent}SelectPathsExec(first={first}, last={last}, search={kind}, count={})",
                render_expression(count)
            );
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::PathUnion { inputs, words } => {
            let _ = writeln!(output, "{indent}PathUnionExec(multiset={})", words.is_none());
            for input in inputs {
                render_plan(input, depth + 1, output);
            }
        }
        GraphPhysicalPlan::NaturalJoin { left, right, optional } => {
            let name = if *optional { "LeftOuterNaturalJoinExec" } else { "NaturalJoinExec" };
            let _ = writeln!(output, "{indent}{name}");
            render_plan(left, depth + 1, output);
            render_plan(right, depth + 1, output);
        }
        GraphPhysicalPlan::Filter {
            operation: _,
            input,
            predicate,
        } => {
            let _ = writeln!(output, "{indent}FilterExec(predicate={})", render_expression(predicate));
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Sort {
            input,
            items,
            estimated_input_rows,
        } => {
            let items = items.iter().map(render_sort).collect::<Vec<_>>().join(", ");
            // One `?` for both arms while redacting: whether statistics were
            // available is a property of the moment, not of the plan, and a
            // shape that told them apart would give one query two identities
            // either side of a statistics refresh.
            let estimate = if redacting_literals() {
                "?".to_string()
            } else {
                estimated_input_rows
                    .map(|rows| rows.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            };
            let _ = writeln!(output, "{indent}SortExec(items=[{items}], estimated_input_rows={estimate})");
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Skip { input, count } => {
            let _ = writeln!(output, "{indent}SkipExec(count={})", render_value(count));
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Limit { input, count } => {
            let _ = writeln!(output, "{indent}LimitExec(count={})", render_value(count));
            render_plan(input, depth + 1, output);
        }
        GraphPhysicalPlan::Project {
            context,
            input,
            items,
            distinct,
            post_skip,
            post_limit,
            ..
        } => {
            let items = items.iter().map(render_projection).collect::<Vec<_>>().join(", ");
            // A window applied after aggregation or DISTINCT has no operator
            // of its own, so it is shown here rather than silently dropped.
            let window = [("skip", post_skip), ("limit", post_limit)]
                .into_iter()
                .filter_map(|(name, count)| count.as_ref().map(|count| format!(", {name}={}", render_value(count))))
                .collect::<String>();
            let frame = if context.fixed.is_empty() && context.defined.is_empty() && !context.explicit_grouping {
                String::new()
            } else {
                format!(
                    ", fixed={:?}, table={:?}, defines={:?}, explicit_grouping={}",
                    context.fixed, context.table_columns, context.defined, context.explicit_grouping
                )
            };
            let _ = writeln!(output, "{indent}ProjectExec(distinct={distinct}, items=[{items}]{window}{frame})");
            render_plan(input, depth + 1, output);
        }
    }
}

pub(crate) fn render_bound(bound: Option<&(BoundValue, bool)>) -> String {
    match bound {
        Some((value, true)) => format!("inclusive {}", render_value(value)),
        Some((value, false)) => format!("exclusive {}", render_value(value)),
        None => "none".to_string(),
    }
}

pub(crate) fn render_sort(sort: &PhysicalSort) -> String {
    let mut output = String::new();
    let _ = write_sort(&mut output, sort);
    output
}

struct SortList<'a>(&'a [PhysicalSort]);

impl fmt::Display for SortList<'_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, sort) in self.0.iter().enumerate() {
            if index > 0 {
                output.write_str(", ")?;
            }
            write_sort(output, sort)?;
        }
        Ok(())
    }
}

fn write_sort(output: &mut impl Write, sort: &PhysicalSort) -> fmt::Result {
    write_expression(output, &sort.expression)?;
    write!(output, " {}", sort.direction)?;
    match sort.nulls {
        Some(NullOrdering::First) => output.write_str(" NULLS FIRST"),
        Some(NullOrdering::Last) => output.write_str(" NULLS LAST"),
        None => Ok(()),
    }
}

fn render_projection(projection: &PhysicalProjection) -> String {
    let expression = render_expression(&projection.expression);
    match &projection.alias {
        Some(alias) => format!("{expression} AS {alias}"),
        None => expression,
    }
}

pub(crate) fn render_expression(expression: &PhysicalExpression) -> String {
    let mut output = String::new();
    let _ = write_expression(&mut output, expression);
    output
}

// Elide parameter-expanded literal lists, while retaining schema references
// and operators in lists computed from rows.
fn literal_list(items: &[PhysicalExpression]) -> bool {
    items.iter().all(|item| match item {
        PhysicalExpression::Value(_) => true,
        PhysicalExpression::List(items) => literal_list(items),
        _ => false,
    })
}

fn write_expression(output: &mut impl Write, expression: &PhysicalExpression) -> fmt::Result {
    match expression {
        PhysicalExpression::Binding(binding) => write!(output, "{binding}"),
        PhysicalExpression::Identity(binding) => write!(output, "{binding}.id"),
        PhysicalExpression::ElementTest { binding, test } => write!(output, "{}", test.expression_name(binding)),
        PhysicalExpression::TypeTest {
            expression,
            value_type,
            negated,
        } => {
            write_expression(output, expression)?;
            write!(output, " IS {}TYPED {value_type}", if *negated { "NOT " } else { "" })
        }
        PhysicalExpression::Cast {
            expression, value_type, ..
        } => {
            write!(output, "CAST(")?;
            write_expression(output, expression)?;
            write!(output, " AS {value_type})")
        }
        PhysicalExpression::Let { bindings, body } => {
            write!(output, "LET ")?;
            for (index, (name, value)) in bindings.iter().enumerate() {
                if index > 0 {
                    write!(output, ", ")?;
                }
                write!(output, "{name} = ")?;
                write_expression(output, value)?;
            }
            write!(output, " IN ")?;
            write_expression(output, body)?;
            write!(output, " END")
        }
        PhysicalExpression::PathConstructor(elements) => {
            write!(output, "PATH[")?;
            for (index, element) in elements.iter().enumerate() {
                if index > 0 {
                    write!(output, ", ")?;
                }
                write_expression(output, element)?;
            }
            write!(output, "]")
        }
        PhysicalExpression::Record(fields) => {
            write!(output, "{{")?;
            for (index, (name, value)) in fields.iter().enumerate() {
                if index > 0 {
                    write!(output, ", ")?;
                }
                write!(output, "{name}: ")?;
                write_expression(output, value)?;
            }
            write!(output, "}}")
        }
        PhysicalExpression::Field { expression, field } => {
            write_expression(output, expression)?;
            write!(output, ".{field}")
        }
        PhysicalExpression::PathNodesDistinct { simple, parts } => write!(
            output,
            "{}({})",
            if *simple { "simple" } else { "acyclic" },
            parts.iter().map(|part| part.name().as_str()).collect::<Vec<_>>().join(", ")
        ),
        PhysicalExpression::Property { binding, property } => {
            write!(output, "{binding}.{property}")
        }
        PhysicalExpression::Value(value) => write_value(output, value),
        // Candidate values are rendered as a count rather than inline: an IN
        // list can hold hundreds of entries, and EXPLAIN output is read by
        // people.
        PhysicalExpression::InList { expression, values } => {
            write_expression(output, expression)?;
            if redacting_literals() {
                output.write_str(" IN [...]")
            } else {
                write!(output, " IN [{} values]", values.len())
            }
        }
        PhysicalExpression::Aggregate {
            function,
            distinct,
            expression,
            fraction,
            ..
        } => {
            write!(output, "{}(", function.name())?;
            if *distinct {
                output.write_str("DISTINCT ")?;
            }
            match expression.as_deref() {
                Some(expression) => write_expression(output, expression)?,
                None => output.write_char('*')?,
            }
            if let Some(fraction) = fraction.as_deref() {
                output.write_str(", ")?;
                write_expression(output, fraction)?;
            }
            output.write_char(')')
        }
        PhysicalExpression::Case {
            operand,
            branches,
            default,
        } => {
            output.write_str("CASE")?;
            if let Some(operand) = operand {
                output.write_char(' ')?;
                write_expression(output, operand)?;
            }
            for (when, then) in branches {
                output.write_str(" WHEN ")?;
                write_expression(output, when)?;
                output.write_str(" THEN ")?;
                write_expression(output, then)?;
            }
            if let Some(default) = default {
                output.write_str(" ELSE ")?;
                write_expression(output, default)?;
            }
            output.write_str(" END")
        }
        PhysicalExpression::List(items) => {
            if redacting_literals() && literal_list(items) {
                return output.write_str("[...]");
            }
            output.write_char('[')?;
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.write_str(", ")?;
                }
                write_expression(output, item)?;
            }
            output.write_char(']')
        }
        PhysicalExpression::Function {
            function: super::ScalarFunction::IdProperty,
            arguments,
        } => {
            write_expression(output, &arguments[0])?;
            write!(output, ".id")
        }
        PhysicalExpression::Function { function, arguments } => {
            write!(output, "{}(", function.name())?;
            for (index, argument) in arguments.iter().enumerate() {
                if index > 0 {
                    output.write_str(", ")?;
                }
                write_expression(output, argument)?;
            }
            output.write_char(')')
        }
        PhysicalExpression::Unary { operator, expression } => {
            let (prefix, suffix) = match operator {
                PhysicalUnaryOperator::Not => ("NOT ", ""),
                PhysicalUnaryOperator::Plus => ("+", ""),
                PhysicalUnaryOperator::Minus => ("-", ""),
                PhysicalUnaryOperator::IsNull => ("", " IS NULL"),
                PhysicalUnaryOperator::IsNotNull => ("", " IS NOT NULL"),
            };
            output.write_str(prefix)?;
            write_expression(output, expression)?;
            output.write_str(suffix)
        }
        PhysicalExpression::Binary { left, operator, right } => {
            let operator = match operator {
                PhysicalBinaryOperator::Equal => "=",
                PhysicalBinaryOperator::NotEqual => "<>",
                PhysicalBinaryOperator::LessThan => "<",
                PhysicalBinaryOperator::LessThanOrEqual => "<=",
                PhysicalBinaryOperator::GreaterThan => ">",
                PhysicalBinaryOperator::GreaterThanOrEqual => ">=",
                PhysicalBinaryOperator::And => "AND",
                PhysicalBinaryOperator::Or => "OR",
                PhysicalBinaryOperator::Xor => "XOR",
                PhysicalBinaryOperator::Add => "+",
                PhysicalBinaryOperator::Subtract => "-",
                PhysicalBinaryOperator::Multiply => "*",
                PhysicalBinaryOperator::Divide => "/",
                PhysicalBinaryOperator::Modulo => "%",
                PhysicalBinaryOperator::StartsWith => "STARTS WITH",
                PhysicalBinaryOperator::EndsWith => "ENDS WITH",
                PhysicalBinaryOperator::Contains => "CONTAINS",
                PhysicalBinaryOperator::Concatenate => "||",
                PhysicalBinaryOperator::In => "IN",
            };
            output.write_char('(')?;
            write_expression(output, left)?;
            write!(output, " {operator} ")?;
            write_expression(output, right)?;
            output.write_char(')')
        }
    }
}

pub(crate) fn render_value(value: &BoundValue) -> String {
    let mut output = String::new();
    let _ = write_value(&mut output, value);
    output
}

fn write_value(output: &mut impl Write, value: &BoundValue) -> fmt::Result {
    match &value.origin {
        ValueOrigin::Parameter(name) => write!(output, "${name}"),
        ValueOrigin::Literal if redacting_literals() => output.write_str("?"),
        ValueOrigin::Literal => match &value.value {
            ScalarValue::Null => output.write_str("NULL"),
            ScalarValue::Boolean(value) => write!(output, "{value}"),
            ScalarValue::Integer(value) => write!(output, "{value}"),
            ScalarValue::Float(value) => write!(output, "{value}"),
            ScalarValue::String(value) => write!(output, "'{value}'"),
            ScalarValue::Decimal(value) => write!(output, "{value}M"),
            ScalarValue::Bytes(value) => write!(output, "X'{}'", hydradb_cypher_ast::bytes::hex(value)),
            ScalarValue::Date(value) => write!(output, "DATE_EPOCH_DAY({value})"),
            ScalarValue::List(values) => write!(output, "{values:?}"),
        },
    }
}

pub(crate) fn render_symbols(symbols: &[Symbol]) -> String {
    format!("[{}]", symbols.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "))
}
