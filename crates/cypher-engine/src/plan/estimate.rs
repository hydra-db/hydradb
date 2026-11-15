//! Row estimates per physical operator, for comparing against what execution
//! actually produced.
//!
//! Vertex scans retain the estimate prepared by their provider. For relational
//! operators this is an observer, not the cost model: the planner chooses plans from
//! logical-plan estimates in `planner.rs`, and nothing here feeds back into
//! that choice. It answers "what did statistics predict for this operator",
//! using the same statistics calls and the same conventions the planner uses,
//! so a large miss here points at the statistics or the model rather than at
//! a second, divergent estimator. The rules follow the planner's:
//!
//! - unknown selectivity is not evidence, so a `Filter` keeps its input's
//!   estimate rather than inventing a reduction;
//! - a join multiplies its inputs;
//! - a window clamps its input.

use crate::{BoundValue, GraphPhysicalPlan, PropertyStatistics, ScalarValue, StatisticsProvider};

/// Estimates for every operator of `plan`, in pre-order (the order
/// [`GraphPhysicalPlan::children`] walks). `None` where statistics give no
/// answer.
pub fn estimate_operator_rows(plan: &GraphPhysicalPlan, statistics: &dyn StatisticsProvider) -> Vec<Option<u64>> {
    let mut estimates = Vec::new();
    estimate_into(plan, statistics, &mut estimates);
    estimates
}

fn estimate_into(plan: &GraphPhysicalPlan, statistics: &dyn StatisticsProvider, estimates: &mut Vec<Option<u64>>) -> Option<u64> {
    let slot = estimates.len();
    estimates.push(None);
    let inputs = plan
        .children()
        .into_iter()
        .map(|child| estimate_into(child, statistics, estimates))
        .collect::<Vec<_>>();
    let input = inputs.first().copied().flatten();
    let estimate = match plan {
        GraphPhysicalPlan::Union { .. } => inputs.iter().try_fold(0_u64, |total, arm| Some(total.saturating_add((*arm)?))),
        GraphPhysicalPlan::VertexScan { scan } => scan.properties().estimated_rows,
        GraphPhysicalPlan::OrderedRelationshipPropertyScan {
            relationship_type,
            property,
            required,
            ..
        } => {
            let required = *required as u64;
            Some(
                statistics
                    .relationship_property_statistics(std::slice::from_ref(relationship_type), property)
                    .and_then(|stats| stats.non_null_count)
                    .map_or(required, |rows| rows.min(required)),
            )
        }
        GraphPhysicalPlan::SingleRow => Some(1),
        GraphPhysicalPlan::Unwind { expression, .. } => match expression {
            crate::PhysicalExpression::List(items) => input.map(|rows| rows.saturating_mul(items.len() as u64)),
            _ => input,
        },
        GraphPhysicalPlan::RelationshipPropertySeek {
            relationship_type,
            property,
            value,
            ..
        } => {
            let types = std::slice::from_ref(relationship_type);
            statistics
                .relationship_property_value_count(types, property, &value.value)
                .or_else(|| {
                    statistics
                        .relationship_property_statistics(types, property)
                        .as_ref()
                        .and_then(equality_rows)
                })
        }
        GraphPhysicalPlan::RelationshipPropertyMultiSeek {
            relationship_type,
            property,
            values,
            ..
        } => {
            let types = std::slice::from_ref(relationship_type);
            let histogram = statistics
                .relationship_property_statistics(types, property)
                .as_ref()
                .and_then(equality_rows);
            values.iter().try_fold(0_u64, |total, value| {
                let rows = statistics
                    .relationship_property_value_count(types, property, &value.value)
                    .or(histogram)?;
                Some(total.saturating_add(rows))
            })
        }
        GraphPhysicalPlan::RelationshipPropertyRangeProbe { .. } | GraphPhysicalPlan::RelationshipTypeProbe { .. } => input,
        GraphPhysicalPlan::Expand {
            input: source,
            relationship_types,
            direction,
            ..
        } => expand_rows(input, source, relationship_types, *direction, statistics),
        GraphPhysicalPlan::VariableExpand {
            input: source,
            relationship_types,
            direction,
            min_hops,
            max_hops,
            ..
        } => {
            let hops = u64::from((*max_hops)?.saturating_sub(*min_hops).saturating_add(1));
            expand_rows(input, source, relationship_types, *direction, statistics).map(|one_hop| one_hop.saturating_mul(hops))
        }
        GraphPhysicalPlan::NaturalJoin { .. } | GraphPhysicalPlan::CallProcedure { .. } => {
            Some(inputs.first().copied()??.saturating_mul(inputs.get(1).copied()??))
        }
        GraphPhysicalPlan::Filter { .. } | GraphPhysicalPlan::Sort { .. } | GraphPhysicalPlan::Project { .. } => input,
        // One row out for every row in.
        GraphPhysicalPlan::Exists { .. } | GraphPhysicalPlan::ScalarSubquery { .. } | GraphPhysicalPlan::Finish { .. } => input,
        // At most every row in; how many fewer depends on the graph.
        GraphPhysicalPlan::SelectPaths { .. } => input,
        GraphPhysicalPlan::BindPath { .. } => input,
        // At most every operand's rows.
        GraphPhysicalPlan::PathUnion { .. } => inputs.iter().try_fold(0_u64, |total, input| Some(total.saturating_add((*input)?))),
        GraphPhysicalPlan::Next { .. } => inputs.get(1).copied().flatten(),
        GraphPhysicalPlan::Skip { count, .. } => Some(input?.saturating_sub(constant_count(count)?)),
        GraphPhysicalPlan::Limit { count, .. } => Some(input?.min(constant_count(count)?)),
    };
    estimates[slot] = estimate;
    estimate
}

fn equality_rows(statistics: &PropertyStatistics) -> Option<u64> {
    let count = statistics.non_null_count?;
    if count == 0 {
        return Some(0);
    }
    let distinct = statistics.distinct_count?;
    (distinct > 0).then(|| count.saturating_add(distinct - 1) / distinct)
}

fn expand_rows(
    input_rows: Option<u64>,
    source: &GraphPhysicalPlan,
    relationship_types: &[crate::Symbol],
    direction: crate::PatternDirection,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    if input_rows == Some(0) {
        return Some(0);
    }
    source_labels(source)
        .and_then(|labels| statistics.relationship_expansion_count(labels, relationship_types, direction))
        .or_else(|| statistics.relationship_count(relationship_types))
}

fn source_labels(plan: &GraphPhysicalPlan) -> Option<&[crate::Symbol]> {
    match plan {
        GraphPhysicalPlan::VertexScan { scan } => scan.population_labels(),
        GraphPhysicalPlan::Filter { input, .. }
        | GraphPhysicalPlan::Sort { input, .. }
        | GraphPhysicalPlan::Skip { input, .. }
        | GraphPhysicalPlan::Limit { input, .. } => source_labels(input),
        _ => None,
    }
}

fn constant_count(count: &BoundValue) -> Option<u64> {
    match &count.value {
        ScalarValue::Integer(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}
