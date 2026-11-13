//! Whether this engine can run a plan, decided without touching storage.
//!
//! The engine used to answer "can you run this?" only by
//! starting to run it and surfacing [`EngineError::UnsupportedPlan`] from
//! whichever operator first noticed. That answer arrives after a read session is
//! open and, for a routing caller, too late to be useful: a router wants the
//! answer *before* it commits a request to this engine, and it wants it without
//! paying for a snapshot.
//!
//! So capability is a value, not an error. [`logical_capability`] decides
//! everything a lowered plan settles and [`physical_capability`] everything the
//! chosen operators settle; both are pure walks over a plan. A caller that must
//! fail converts with [`EngineCapability::into_result`]; a caller that is only
//! asking - capability-only mode, an allowlist, a shadow router - reads the
//! answer and moves on.
//!
//! [`UnsupportedShape::kind`] is a closed vocabulary of static tokens rather
//! than a formatted sentence, so it can label a metric or key a parity-matrix
//! row without carrying query text into either.

use hydradb_cypher_ast::QueryFailureReason;
use std::collections::BTreeSet;

use crate::prepare::column_names;
use crate::{EngineError, EngineResult, GraphLogicalPlan, GraphPhysicalPlan, PhysicalExpression, PhysicalProjection, Symbol};

/// A plan's root is not a result-producing shape.
pub const RESULT_PLAN_ROOT: &str = "result_plan_root";
/// `UNION` with no arms produces no columns to agree on.
pub const UNION_EMPTY: &str = "union_empty";
/// `UNION` arms disagree about their column names.
pub const UNION_COLUMN_MISMATCH: &str = "union_column_mismatch";
/// `UNION` arms below a binding operator bind different names.
pub const UNION_BINDING_MISMATCH: &str = "union_binding_mismatch";
/// A projection below a result plan computes a value it never names, so
/// nothing downstream could refer to it.
pub const NESTED_PROJECT_UNNAMED: &str = "nested_project_unnamed";
/// A projection below a result plan aggregated, back when grouping at a scope
/// boundary was not implemented. No longer produced; kept so a consumer
/// matching on the token still compiles.
pub const NESTED_PROJECT_AGGREGATE: &str = "nested_project_aggregate";
/// Variable-length expansion with no single storage direction, back when the
/// traversal ran one direction only. No longer produced; kept so a consumer
/// matching on the token still compiles.
pub const UNDIRECTED_VARIABLE_LENGTH: &str = "undirected_variable_length";
/// An expansion whose source binding its own input never binds.
///
/// Distinct from a source that is merely *unbound at run time*, which an
/// OPTIONAL MATCH produces legitimately and which expands to nothing. This is
/// the plan-level case: no row could ever bind it, so the expansion is a
/// mistake rather than a miss.
pub const EXPAND_FROM_UNBOUND: &str = "expand_from_unbound";

/// Whether the engine can run a plan.
///
/// Deliberately not `Result<(), EngineError>`. An unsupported shape is not a
/// failure until somebody decides to fail on it, and the capability-only
/// mode exists precisely to ask the question without one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineCapability {
    Supported,
    Unsupported(UnsupportedShape),
}

/// A shape this engine cannot run, named by a stable token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsupportedShape {
    /// One of the `const` tokens in this module. Bounded cardinality, no query
    /// text, no literals: safe as a metric label and as a matrix key.
    pub kind: &'static str,
    /// Human detail for the public error message. May name plan operators and
    /// schema identifiers; never parameter values or literals.
    pub detail: String,
}

impl UnsupportedShape {
    pub(crate) fn new(kind: &'static str, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    /// The error this shape becomes when a caller does choose to fail.
    ///
    /// Execution's own guards build their errors through here, so the message a
    /// mid-execution rejection prints and the message the pre-execution check
    /// prints cannot drift apart.
    pub(crate) fn into_error(self) -> EngineError {
        EngineError::UnsupportedPlan(self.reason(), self.detail)
    }

    /// The public failure class this shape reports.
    ///
    /// These are the buckets legacy files the same refusals under, so a shape
    /// both routes decline declines identically. The token is the engine's own
    /// name for the shape and is finer-grained than the class; several tokens
    /// share a class on purpose.
    pub(crate) fn reason(&self) -> QueryFailureReason {
        match self.kind {
            UNION_EMPTY | UNION_COLUMN_MISMATCH | UNION_BINDING_MISMATCH => QueryFailureReason::Union,
            UNDIRECTED_VARIABLE_LENGTH => QueryFailureReason::Pattern,
            RESULT_PLAN_ROOT | NESTED_PROJECT_UNNAMED | NESTED_PROJECT_AGGREGATE => QueryFailureReason::Return,
            _ => QueryFailureReason::Other,
        }
    }
}

impl EngineCapability {
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }

    pub fn unsupported_kind(&self) -> Option<&'static str> {
        match self {
            Self::Supported => None,
            Self::Unsupported(shape) => Some(shape.kind),
        }
    }

    pub fn into_result(self) -> EngineResult<()> {
        match self {
            Self::Supported => Ok(()),
            Self::Unsupported(shape) => Err(shape.into_error()),
        }
    }
}

type ShapeResult = Result<(), UnsupportedShape>;

fn unsupported(kind: &'static str, detail: impl Into<String>) -> ShapeResult {
    Err(UnsupportedShape::new(kind, detail))
}

fn capability(result: ShapeResult) -> EngineCapability {
    match result {
        Ok(()) => EngineCapability::Supported,
        Err(shape) => EngineCapability::Unsupported(shape),
    }
}

/// Everything decidable from a lowered plan, before any statistics are loaded.
///
/// This is the check that makes "no storage request is issued for an
/// unsupported shape" true rather than nearly true: it runs before the snapshot
/// the statistics loader would read through.
pub fn logical_capability(plan: &GraphLogicalPlan) -> EngineCapability {
    capability(logical_result_plan(plan))
}

/// Everything the chosen physical operators settle, including the column
/// agreement between `UNION` arms that only naming can decide.
pub fn physical_capability(plan: &GraphPhysicalPlan) -> EngineCapability {
    capability(physical_result_plan(plan))
}

fn logical_result_plan(plan: &GraphLogicalPlan) -> ShapeResult {
    match plan {
        GraphLogicalPlan::Union { arms, .. } => {
            if arms.is_empty() {
                return unsupported(UNION_EMPTY, "UNION requires at least one arm");
            }
            arms.iter().try_for_each(logical_result_plan)
        }
        GraphLogicalPlan::Project { input, .. } | GraphLogicalPlan::Finish { input } => logical_binding_plan(input),
        GraphLogicalPlan::Next { input, next } => {
            logical_result_plan(input)?;
            logical_result_plan(next)
        }
        _ => unsupported(RESULT_PLAN_ROOT, "a result-producing plan must have Project or Union at its root"),
    }
}

/// The name an item of a projection below a result plan binds: its alias, a
/// bare variable's own name, or for `r.id` its result column's name, `r.id`
/// (not `r`, whose record or element it reads). Any other computed item has
/// none, and the shape is refused rather than run with a value under no
/// name. Execution names items with this same function.
pub(crate) fn nested_projection_name(item: &PhysicalProjection) -> Option<Symbol> {
    match (&item.alias, &item.expression) {
        (Some(alias), _) => Some(alias.clone()),
        (None, PhysicalExpression::Binding(binding)) => Some(binding.clone()),
        (None, PhysicalExpression::Identity(_)) => Some(Symbol::from(item.column_name().as_str())),
        (None, _) => None,
    }
}

fn logical_binding_plan(plan: &GraphLogicalPlan) -> ShapeResult {
    match plan {
        GraphLogicalPlan::Union { arms, .. } => arms.iter().try_for_each(logical_binding_plan),
        GraphLogicalPlan::Project { input, items, .. } => {
            for item in items {
                if item.alias.is_none()
                    && !matches!(
                        item.expression,
                        crate::LogicalExpression::Binding(_) | crate::LogicalExpression::Identity(_)
                    )
                {
                    return unsupported(
                        NESTED_PROJECT_UNNAMED,
                        "a projection below a result plan must alias every computed item",
                    );
                }
            }
            logical_binding_plan(input)
        }
        GraphLogicalPlan::VariableExpand { input, iteration, .. } => {
            logical_binding_plan(input)?;
            iteration.as_ref().map_or(Ok(()), |iteration| logical_binding_plan(&iteration.plan))
        }
        GraphLogicalPlan::SingleRow | GraphLogicalPlan::NodeScan { .. } => Ok(()),
        GraphLogicalPlan::Unwind { input, .. } => logical_binding_plan(input),
        GraphLogicalPlan::Expand { input, .. }
        | GraphLogicalPlan::Filter { input, .. }
        | GraphLogicalPlan::Sort { input, .. }
        | GraphLogicalPlan::Skip { input, .. }
        | GraphLogicalPlan::Limit { input, .. } => logical_binding_plan(input),
        GraphLogicalPlan::NaturalJoin { left, right, .. } => {
            logical_binding_plan(left)?;
            logical_binding_plan(right)
        }
        // A subquery that ends in RETURN is a result plan of its own.
        GraphLogicalPlan::Exists { input, subquery, .. } => {
            logical_binding_plan(input)?;
            match subquery.as_ref() {
                GraphLogicalPlan::Project { .. } | GraphLogicalPlan::Union { .. } => logical_result_plan(subquery),
                _ => logical_binding_plan(subquery),
            }
        }
        GraphLogicalPlan::ScalarSubquery { input, subquery, .. } | GraphLogicalPlan::CallProcedure { input, subquery, .. } => {
            logical_binding_plan(input)?;
            logical_result_plan(subquery)
        }
        GraphLogicalPlan::Finish { .. } | GraphLogicalPlan::Next { .. } => {
            unsupported(RESULT_PLAN_ROOT, "statement results occur only at the query root")
        }
        GraphLogicalPlan::SelectPaths { input, .. } | GraphLogicalPlan::BindPath { input, .. } => logical_binding_plan(input),
        GraphLogicalPlan::PathUnion { inputs, .. } => inputs.iter().try_for_each(logical_binding_plan),
    }
}

fn physical_result_plan(plan: &GraphPhysicalPlan) -> ShapeResult {
    match plan {
        GraphPhysicalPlan::Union { arms, .. } => {
            let Some(first) = arms.first() else {
                return unsupported(UNION_EMPTY, "UNION requires at least one arm");
            };
            arms.iter().try_for_each(physical_result_plan)?;

            // Column agreement is a capability fact, not a runtime one: the
            // names are in the plan, so nothing has to execute to find a
            // mismatch, and `execute_result_plan` used to discover it only
            // after running an arm and collecting its rows.
            let expected = column_names(first).map_err(|error| UnsupportedShape::new(UNION_COLUMN_MISMATCH, error.to_string()))?;
            for arm in arms.iter().skip(1) {
                let names = column_names(arm).map_err(|error| UnsupportedShape::new(UNION_COLUMN_MISMATCH, error.to_string()))?;
                if names != expected {
                    return unsupported(UNION_COLUMN_MISMATCH, "all UNION arms must return the same column names");
                }
            }
            Ok(())
        }
        GraphPhysicalPlan::Project { input, .. } | GraphPhysicalPlan::Finish { input } => physical_binding_plan(input),
        GraphPhysicalPlan::Next { input, next } => {
            physical_result_plan(input)?;
            physical_result_plan(next)
        }
        _ => unsupported(RESULT_PLAN_ROOT, "a result-producing plan must have Project or Union at its root"),
    }
}

fn physical_binding_plan(plan: &GraphPhysicalPlan) -> ShapeResult {
    match plan {
        GraphPhysicalPlan::Union { arms, .. } => {
            // Arms of a binding-level union must bind the same names, or the
            // rows they produce are not the same shape and nothing downstream
            // could read them uniformly. Decidable from the plan, so it is a
            // capability fact rather than a runtime surprise.
            if let Some(first) = arms.first() {
                let expected = produced_symbols(first);
                for arm in arms.iter().skip(1) {
                    if produced_symbols(arm) != expected {
                        return unsupported(
                            UNION_BINDING_MISMATCH,
                            "UNION arms below a binding operator must bind the same names",
                        );
                    }
                }
            }
            arms.iter().try_for_each(physical_binding_plan)
        }
        GraphPhysicalPlan::Project { input, items, .. } => {
            for item in items {
                if nested_projection_name(item).is_none() {
                    return unsupported(
                        NESTED_PROJECT_UNNAMED,
                        "a projection below a result plan must alias every computed item",
                    );
                }
            }
            physical_binding_plan(input)
        }
        GraphPhysicalPlan::VariableExpand {
            input, from, iteration, ..
        } => {
            expand_source_is_bindable(input, from)?;
            physical_binding_plan(input)?;
            iteration
                .as_ref()
                .map_or(Ok(()), |iteration| physical_binding_plan(&iteration.plan))
        }
        GraphPhysicalPlan::Expand { input, from, .. } => {
            expand_source_is_bindable(input, from)?;
            physical_binding_plan(input)
        }
        GraphPhysicalPlan::SingleRow
        | GraphPhysicalPlan::VertexScan { .. }
        | GraphPhysicalPlan::RelationshipPropertySeek { .. }
        | GraphPhysicalPlan::RelationshipPropertyMultiSeek { .. }
        | GraphPhysicalPlan::OrderedRelationshipPropertyScan { .. } => Ok(()),
        // Same reasoning as the range probe below: the probe answers from the
        // edge index or declines to its fallback at run time, so the plan is
        // runnable only if the fallback is.
        GraphPhysicalPlan::RelationshipTypeProbe { fallback, .. } => physical_binding_plan(fallback),
        GraphPhysicalPlan::Unwind { input, .. }
        | GraphPhysicalPlan::Filter { input, .. }
        | GraphPhysicalPlan::Sort { input, .. }
        | GraphPhysicalPlan::Skip { input, .. }
        | GraphPhysicalPlan::Limit { input, .. } => physical_binding_plan(input),
        // The probe answers from the edge index or declines to its fallback at
        // run time, so the plan is runnable only if the fallback is: a shape
        // the probe cannot express is not made supported by the probe winning.
        GraphPhysicalPlan::RelationshipPropertyRangeProbe { fallback, .. } => physical_binding_plan(fallback),
        GraphPhysicalPlan::NaturalJoin { left, right, .. } => {
            physical_binding_plan(left)?;
            physical_binding_plan(right)
        }
        GraphPhysicalPlan::Exists { input, subquery, .. } => {
            physical_binding_plan(input)?;
            match subquery.as_ref() {
                GraphPhysicalPlan::Project { .. } | GraphPhysicalPlan::Union { .. } => physical_result_plan(subquery),
                _ => physical_binding_plan(subquery),
            }
        }
        GraphPhysicalPlan::ScalarSubquery { input, subquery, .. } | GraphPhysicalPlan::CallProcedure { input, subquery, .. } => {
            physical_binding_plan(input)?;
            physical_result_plan(subquery)
        }
        GraphPhysicalPlan::Finish { .. } | GraphPhysicalPlan::Next { .. } => {
            unsupported(RESULT_PLAN_ROOT, "statement results occur only at the query root")
        }
        GraphPhysicalPlan::SelectPaths { input, .. } | GraphPhysicalPlan::BindPath { input, .. } => physical_binding_plan(input),
        GraphPhysicalPlan::PathUnion { inputs, .. } => inputs.iter().try_for_each(physical_binding_plan),
    }
}

/// An expansion's source has to be a name its input can bind.
///
/// This is what lets execution treat an unbound source as an optional miss and
/// expand it to nothing. Without the check, a plan that names a binding
/// nothing produces would quietly return no rows instead of saying the plan is
/// wrong; with it, the only way to reach execution with an unbound source is
/// the one execution knows how to handle.
fn expand_source_is_bindable(input: &GraphPhysicalPlan, from: &Symbol) -> ShapeResult {
    if produced_symbols(input).contains(from) {
        Ok(())
    } else {
        unsupported(
            EXPAND_FROM_UNBOUND,
            format!("expansion reads binding {from}, which its input never binds"),
        )
    }
}

/// The names a plan binds, for comparing the arms of a binding-level union.
pub(crate) fn produced_symbols(plan: &GraphPhysicalPlan) -> BTreeSet<Symbol> {
    let mut symbols = BTreeSet::new();
    collect_produced_symbols(plan, &mut symbols);
    symbols
}

fn collect_produced_symbols(plan: &GraphPhysicalPlan, symbols: &mut BTreeSet<Symbol>) {
    match plan {
        GraphPhysicalPlan::SingleRow => {}
        GraphPhysicalPlan::Unwind {
            input, binding, position, ..
        } => {
            collect_produced_symbols(input, symbols);
            symbols.insert(binding.clone());
            if let Some((position, _)) = position {
                symbols.insert(position.clone());
            }
        }
        GraphPhysicalPlan::Union { arms, .. } => {
            // Mismatched arms are refused elsewhere; describing the union by
            // its first arm keeps that refusal from depending on itself.
            if let Some(first) = arms.first() {
                collect_produced_symbols(first, symbols);
            }
        }
        GraphPhysicalPlan::Project { items, .. } => {
            // A projection replaces the scope rather than adding to it.
            symbols.clear();
            for item in items {
                if let Some(alias) = &item.alias {
                    symbols.insert(alias.clone());
                } else if let PhysicalExpression::Binding(binding) | PhysicalExpression::Identity(binding) = &item.expression {
                    symbols.insert(binding.clone());
                }
            }
        }
        GraphPhysicalPlan::VertexScan { scan } => {
            symbols.insert(scan.binding().clone());
        }
        GraphPhysicalPlan::RelationshipPropertySeek {
            from, relationship, to, ..
        }
        | GraphPhysicalPlan::RelationshipPropertyMultiSeek {
            from, relationship, to, ..
        }
        | GraphPhysicalPlan::OrderedRelationshipPropertyScan {
            from, relationship, to, ..
        } => {
            symbols.insert(from.clone());
            symbols.insert(relationship.clone());
            symbols.insert(to.clone());
        }
        // Described through its fallback, as the range probe is: either branch
        // binds the same names, and the fallback is the plan the probe replaced.
        GraphPhysicalPlan::RelationshipTypeProbe {
            from,
            relationship,
            to,
            fallback,
            ..
        } => {
            collect_produced_symbols(fallback, symbols);
            symbols.insert(from.clone());
            symbols.insert(relationship.clone());
            symbols.insert(to.clone());
        }
        GraphPhysicalPlan::Expand {
            input, relationship, to, ..
        } => {
            collect_produced_symbols(input, symbols);
            if let Some(relationship) = relationship {
                symbols.insert(relationship.clone());
            }
            symbols.insert(to.clone());
        }
        GraphPhysicalPlan::VariableExpand {
            input, relationship, to, ..
        } => {
            collect_produced_symbols(input, symbols);
            // `[rs:R*1..3]` binds its relationship list as well.
            symbols.extend(relationship.iter().cloned());
            symbols.insert(to.clone());
        }
        // Either branch binds the same names, and the fallback is the plan the
        // probe replaced, so it is the honest description of both.
        GraphPhysicalPlan::RelationshipPropertyRangeProbe {
            from,
            relationship,
            to,
            fallback,
            ..
        } => {
            collect_produced_symbols(fallback, symbols);
            symbols.insert(from.clone());
            symbols.insert(relationship.clone());
            symbols.insert(to.clone());
        }
        GraphPhysicalPlan::NaturalJoin { left, right, .. } => {
            collect_produced_symbols(left, symbols);
            collect_produced_symbols(right, symbols);
        }
        // The subquery's own names stay inside it; only its mark escapes.
        GraphPhysicalPlan::Exists { input, binding, .. } | GraphPhysicalPlan::ScalarSubquery { input, binding, .. } => {
            collect_produced_symbols(input, symbols);
            symbols.insert(binding.clone());
        }
        GraphPhysicalPlan::CallProcedure { input, columns, .. } => {
            collect_produced_symbols(input, symbols);
            symbols.extend(columns.iter().cloned());
        }
        GraphPhysicalPlan::Finish { input } | GraphPhysicalPlan::SelectPaths { input, .. } => collect_produced_symbols(input, symbols),
        // Every operand's names: those of the others are null on its rows.
        GraphPhysicalPlan::PathUnion { inputs, .. } => {
            for input in inputs {
                collect_produced_symbols(input, symbols);
            }
        }
        GraphPhysicalPlan::BindPath {
            input, names, node_groups, ..
        } => {
            collect_produced_symbols(input, symbols);
            symbols.extend(names.iter().cloned());
            symbols.extend(node_groups.iter().map(|(name, _)| name.clone()));
        }
        GraphPhysicalPlan::Next { next, .. } => collect_produced_symbols(next, symbols),
        GraphPhysicalPlan::Filter { input, .. }
        | GraphPhysicalPlan::Sort { input, .. }
        | GraphPhysicalPlan::Skip { input, .. }
        | GraphPhysicalPlan::Limit { input, .. } => collect_produced_symbols(input, symbols),
    }
}

/// The error an execution-time guard raises for a shape this module names.
///
/// Execution keeps its guards - a capability check that was not consulted must
/// not become a wrong answer - but builds their errors here so the two cannot
/// print different things about the same shape.
pub(crate) fn shape_error(kind: &'static str, detail: impl Into<String>) -> EngineError {
    UnsupportedShape::new(kind, detail).into_error()
}
