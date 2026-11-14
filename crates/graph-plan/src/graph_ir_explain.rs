use std::fmt::Write;

use crate::{BoundSymbol, GraphIr, GraphIrKind, MutationStep};

/// Render the backend-neutral graph IR in a deterministic, storage-free form.
pub fn explain_graph_ir(plan: &GraphIr) -> String {
    let mut output = String::new();
    render(plan, 0, &mut output);
    output
}

fn render(plan: &GraphIr, depth: usize, output: &mut String) {
    use GraphIrKind::*;

    match &plan.kind {
        SingleRow => line(output, depth, "SingleRow"),
        NodeScan { input, binding, labels } => {
            line(
                output,
                depth,
                &format!("NodeScan({}:Node, labels={})", binding.symbol, symbols(labels)),
            );
            render_optional_input(input.as_deref(), depth, output);
        }
        RelationshipScan { input, binding, types } => {
            line(
                output,
                depth,
                &format!("RelationshipScan({}:Relationship, types={})", binding.symbol, symbols(types)),
            );
            render_optional_input(input.as_deref(), depth, output);
        }
        GraphSeek { input, binding, predicate } => {
            line(
                output,
                depth,
                &format!("GraphSeek(binding={}, predicate={predicate:?})", binding.symbol),
            );
            render_optional_input(input.as_deref(), depth, output);
        }
        Filter { input, predicate } => {
            line(output, depth, &format!("Filter(predicate={predicate:?})"));
            render_child("input", input, depth, output);
        }
        Project { input, items } => {
            line(output, depth, &format!("Project(items={})", items.len()));
            render_child("input", input, depth, output);
        }
        Expand {
            input,
            from,
            relationship,
            to,
            types,
            direction,
            mode,
            uniqueness,
            excluded_relationships,
        } => {
            line(
                output,
                depth,
                &format!(
                    "Expand(from={}, relationship={}, to={}, types={}, direction={direction:?}, mode={mode:?}, uniqueness={uniqueness:?}, excluded_relationships={})",
                    from.symbol,
                    relationship.symbol,
                    to.symbol,
                    symbols(types),
                    bound_symbols(excluded_relationships)
                ),
            );
            render_child("input", input, depth, output);
        }
        VariableExpand {
            input,
            from,
            relationship,
            to,
            path,
            types,
            direction,
            bounds,
            uniqueness,
            excluded_relationships,
        } => {
            let relationship = relationship
                .as_ref()
                .map_or_else(|| "_".to_owned(), |symbol| symbol.symbol.to_string());
            let path = path.as_ref().map_or_else(|| "_".to_owned(), |symbol| symbol.symbol.to_string());
            let max_hops = bounds.maximum_hops.map_or_else(|| "unbounded".to_owned(), |max| max.to_string());
            line(
                output,
                depth,
                &format!(
                    "VariableExpand(from={}, relationship={relationship}, to={}, path={path}, types={}, hops={}..={max_hops}, direction={direction:?}, uniqueness={uniqueness:?}, excluded_relationships={})",
                    from.symbol,
                    to.symbol,
                    symbols(types),
                    bounds.minimum_hops,
                    bound_symbols(excluded_relationships)
                ),
            );
            render_child("input", input, depth, output);
        }
        GraphHydrate { input, entity, properties } => {
            line(
                output,
                depth,
                &format!("GraphHydrate(entity={}, properties={})", entity.symbol, symbols(properties)),
            );
            render_child("input", input, depth, output);
        }
        Exists {
            input,
            subquery,
            correlation,
            output: binding,
            negated,
        } => {
            line(
                output,
                depth,
                &format!(
                    "Exists(output={}, negated={negated}, correlation={})",
                    binding.symbol,
                    bound_symbols(correlation)
                ),
            );
            render_child("input", input, depth, output);
            render_child("subquery", subquery, depth, output);
        }
        OptionalApply {
            left,
            right,
            correlation,
            output: apply_output,
        } => {
            line(
                output,
                depth,
                &format!(
                    "OptionalApply(correlation={}, null_extend={})",
                    bound_symbols(correlation),
                    bound_symbols(&apply_output.nullable)
                ),
            );
            render_child("left", left, depth, output);
            render_child("right", right, depth, output);
        }
        CorrelatedApply { left, right, correlation } => {
            line(
                output,
                depth,
                &format!("CorrelatedApply(correlation={})", bound_symbols(correlation)),
            );
            render_child("left", left, depth, output);
            render_child("right", right, depth, output);
        }
        Unwind {
            input,
            expression,
            binding,
        } => {
            line(
                output,
                depth,
                &format!("Unwind(expression={expression:?}, binding={})", binding.symbol),
            );
            render_child("input", input, depth, output);
        }
        Union { arms, mode, outputs } => {
            line(output, depth, &format!("Union(mode={mode:?}, outputs={})", bound_symbols(outputs)));
            for (index, arm) in arms.iter().enumerate() {
                render_child(&format!("arm[{index}]"), arm, depth, output);
            }
        }
        Aggregation {
            input,
            group_by,
            aggregates,
        } => {
            line(
                output,
                depth,
                &format!("Aggregation(groups={}, aggregates={})", group_by.len(), aggregates.len()),
            );
            render_child("input", input, depth, output);
        }
        Sort { input, keys } => {
            line(output, depth, &format!("Sort(keys={})", keys.len()));
            render_child("input", input, depth, output);
        }
        Window { input, calls } => {
            line(output, depth, &format!("Window(calls={})", calls.len()));
            render_child("input", input, depth, output);
        }
        ResultWindow { input, skip, limit } => {
            line(output, depth, &format!("ResultWindow(skip={skip:?}, limit={limit:?})"));
            render_child("input", input, depth, output);
        }
        GraphProcedure { input, procedure } => {
            line(
                output,
                depth,
                &format!("GraphProcedure(name={}, effect={:?})", procedure.name, procedure.effect),
            );
            render_optional_input(input.as_deref(), depth, output);
        }
        MutationProgram { input, program } => {
            let steps = program.steps.iter().map(step_name).collect::<Vec<_>>().join(", ");
            line(
                output,
                depth,
                &format!("MutationProgram(policy={:?}, steps=[{steps}])", program.policy),
            );
            render_child("input", input, depth, output);
        }
    }
}

fn step_name(step: &MutationStep) -> String {
    format!("{}:{}", step.ordinal, step.operation.name())
}

fn render_optional_input(input: Option<&GraphIr>, depth: usize, output: &mut String) {
    if let Some(input) = input {
        render_child("input", input, depth, output);
    }
}

fn render_child(name: &str, child: &GraphIr, depth: usize, output: &mut String) {
    line(output, depth + 1, &format!("{name}:"));
    render(child, depth + 2, output);
}

fn line(output: &mut String, depth: usize, value: &str) {
    let _ = writeln!(output, "{}{value}", "  ".repeat(depth));
}

fn symbols(symbols: &[crate::Symbol]) -> String {
    format!("[{}]", symbols.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "))
}

fn bound_symbols(symbols: &[BoundSymbol]) -> String {
    format!(
        "[{}]",
        symbols
            .iter()
            .map(|symbol| symbol.symbol.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}
