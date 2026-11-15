use super::{GuardedAccess, PreparedVertexScan, VertexAccess, VertexOutput, VertexScanRequest};
use crate::plan::access::{
    best_vertex_count, collect_ordered_window_predicate, equality_constraint, fold_conjuncts, ordered_window_operand,
    ordered_window_restrictions, prefer_index, property_scan_constraint, property_values_estimate, OrderedWindowPredicate,
};
use crate::{
    BoundValue, PhysicalBinaryOperator, PhysicalExpression, PropertyPointSet, ScalarValue, StatisticsProvider, Symbol, UnknownStatistics,
};
use std::collections::{BTreeMap, BTreeSet};
/// Index entries a statistics-chosen access may read for each one it
/// expects, before it declines to its fallback.
const GUARD_READ_FACTOR: u64 = 10;
/// The smallest read limit, so a stale estimate of zero still reads a page.
/// Equal to the runtime probe's entry budget.
const GUARD_MIN_READ_LIMIT: u64 = 256;
/// The largest read limit for a range seek. Its reads are bounded by this
/// limit rather than charged to the query's shared index-candidate tally, so
/// this is what bounds the entries a declined walk costs before the fallback
/// runs. Well below the default cap, which the backend no longer enforces on
/// this read; the executor also clamps it under a lower configured cap.
const RANGE_SEEK_MAX_READ_LIMIT: u64 = 50_000;

/// One way into a vertex population, with what statistics expect it to read.
enum AccessCandidate {
    Equality {
        property: Symbol,
        values: Vec<BoundValue>,
    },
    Range {
        property: Symbol,
        lower: Option<(BoundValue, bool)>,
        upper: Option<(BoundValue, bool)>,
    },
}

impl AccessCandidate {
    /// Equalities win ties: a seek reads exactly its values' entries, while a
    /// range estimate assumes values spread evenly between min and max.
    fn kind_rank(&self) -> u8 {
        match self {
            Self::Equality { .. } => 0,
            Self::Range { .. } => 1,
        }
    }
}

fn legacy_node_scan_access(
    binding: &Symbol,
    labels: &[Symbol],
    bound_predicate: &PhysicalExpression,
    statistics: &dyn StatisticsProvider,
) -> VertexAccess {
    let scan = fallback_scan(binding, labels, statistics);
    let scan_rows = best_vertex_count(labels, statistics);
    match identity_constraint(bound_predicate) {
        Some((constraint_binding, value)) if constraint_binding == *binding => VertexAccess::IdPoint { value },
        _ => match equality_constraint(bound_predicate) {
            Some((constraint_binding, property, values)) if constraint_binding == *binding => {
                let seek_rows = property_values_estimate(labels, &property, &values, statistics);
                if !prefer_index(seek_rows, scan_rows) {
                    scan
                } else {
                    property_seek(property, values)
                }
            }
            _ => match property_scan_constraint(bound_predicate) {
                Some((constraint_binding, property)) if constraint_binding == *binding => {
                    let property_rows = statistics
                        .property_statistics(labels, &property)
                        .and_then(|stats| stats.non_null_count);
                    if prefer_index(property_rows, scan_rows) {
                        VertexAccess::PropertyPresence { property }
                    } else {
                        scan
                    }
                }
                _ => scan,
            },
        },
    }
}
fn property_seek(property: Symbol, values: Vec<BoundValue>) -> VertexAccess {
    let points = if let [value] = values.as_slice() {
        PropertyPointSet::Single(value.clone())
    } else {
        PropertyPointSet::Multiple(values)
    };
    VertexAccess::PropertyPoints { property, points }
}
fn plan_ranked_node_scan(
    binding: &Symbol,
    labels: &[Symbol],
    predicate: &PhysicalExpression,
    statistics: &dyn StatisticsProvider,
) -> Option<VertexAccess> {
    if let Some((constraint_binding, value)) = identity_constraint(predicate) {
        if constraint_binding == *binding {
            return Some(VertexAccess::IdPoint { value });
        }
    }
    let mut conjuncts = Vec::new();
    collect_conjuncts(predicate, &mut conjuncts);

    let mut candidates: Vec<(u64, AccessCandidate)> = Vec::new();
    let mut equality_properties = BTreeSet::new();
    for conjunct in &conjuncts {
        let Some((constraint_binding, property, values)) = equality_constraint(conjunct) else {
            continue;
        };
        if constraint_binding != *binding {
            continue;
        }
        equality_properties.insert(property.clone());
        // No index key holds NaN, an infinity or an integer past i64/u64 a
        // seek could find, and a backend may refuse to encode one; the Filter
        // answers it instead. Skipped rather than declined over, since the
        // unranked planner would seek it first if written first.
        if !values.iter().all(|value| seekable_value(&value.value)) {
            continue;
        }
        // An equality statistics cannot estimate may be the narrowest one, and
        // ranking only the rest would run a known-broad seek without the
        // runtime probe that could have found it. Leave the binding to the
        // unranked planner, whose probe weighs every equality as it reads.
        let estimate = property_values_estimate(labels, &property, &values, statistics)?;
        candidates.push((estimate, AccessCandidate::Equality { property, values }));
    }
    // A range on a property an equality already restricts reads at least the
    // equality's entries, so only the equality is offered for it.
    let mut range_properties = Vec::new();
    for conjunct in &conjuncts {
        if let Some(property) = range_comparison_property(conjunct, binding) {
            if !equality_properties.contains(&property) && !range_properties.contains(&property) {
                range_properties.push(property);
            }
        }
    }
    for property in range_properties {
        // Two bounds on one property fill one window, so `p >= a AND p < b`
        // is one candidate with both ends set.
        let mut window = OrderedWindowPredicate::default();
        let mut residual = Vec::new();
        collect_ordered_window_predicate(predicate, binding, &property, &mut window, &mut residual);
        if window.lower.is_none() && window.upper.is_none() {
            continue;
        }
        // Unlike an equality, a range with no estimate is only skipped: the
        // unranked planner never seeks a range and the probe never weighs
        // one, so declining to rank would not let anything find it narrow.
        if let Some(estimate) = range_estimate(labels, &property, window.lower.as_ref(), window.upper.as_ref(), statistics) {
            candidates.push((
                estimate,
                AccessCandidate::Range {
                    property,
                    lower: window.lower,
                    upper: window.upper,
                },
            ));
        }
    }
    let (estimate, best) = candidates
        .into_iter()
        .enumerate()
        .min_by_key(|(order, (estimate, candidate))| (*estimate, candidate.kind_rank(), *order))
        .map(|(_, candidate)| candidate)?;
    // Whatever statistics choose declines to the plan chosen without them,
    // which the enclosing Filter may still probe: a wrong estimate is never
    // worse than today.
    let fallback = || {
        let request = VertexScanRequest {
            binding: binding.clone(),
            labels: labels.to_vec(),
            predicates: vec![predicate.clone()],
            output: VertexOutput::Record,
            window: None,
        };
        let access = legacy_node_scan_access(binding, labels, predicate, &UnknownStatistics);
        Box::new(PreparedVertexScan::new(request, access, &UnknownStatistics))
    };
    if let Some(label_rows) = best_vertex_count(labels, statistics).filter(|label_rows| estimate > *label_rows) {
        return Some(match fallback_scan(binding, labels, statistics) {
            VertexAccess::LabelPrefix { label } => {
                let estimated_rows = statistics.vertex_count(std::slice::from_ref(&label)).unwrap_or(label_rows);
                VertexAccess::Guarded {
                    access: GuardedAccess::LabelScan { label },
                    estimated_rows,
                    read_limit: guard_read_limit(estimated_rows),
                    fallback: fallback(),
                }
            }
            // Unreachable while only a labelled binding has a count, and
            // harmless if not: the unguarded scan is what step 3 planned.
            other => other,
        });
    }
    Some(match best {
        AccessCandidate::Equality { property, values } => VertexAccess::Guarded {
            access: GuardedAccess::PropertySeek { property, values },
            estimated_rows: estimate,
            read_limit: guard_read_limit(estimate),
            fallback: fallback(),
        },
        AccessCandidate::Range { property, lower, upper } => VertexAccess::Range {
            property,
            lower,
            upper,
            estimated_rows: estimate,
            read_limit: range_read_limit(estimate),
            fallback: fallback(),
        },
    })
}
fn fallback_scan(_binding: &Symbol, labels: &[Symbol], statistics: &dyn StatisticsProvider) -> VertexAccess {
    match labels {
        [] => VertexAccess::AllVertices,
        _ => VertexAccess::LabelPrefix {
            label: labels
                .iter()
                .min_by_key(|label| statistics.vertex_count(std::slice::from_ref(label)).unwrap_or(u64::MAX))
                .expect("nonempty labels")
                .clone(),
        },
    }
}
fn guard_read_limit(estimate: u64) -> usize {
    let limit = estimate.saturating_mul(GUARD_READ_FACTOR).max(GUARD_MIN_READ_LIMIT);
    usize::try_from(limit).unwrap_or(usize::MAX)
}

fn range_read_limit(estimate: u64) -> usize {
    let limit = estimate
        .saturating_mul(GUARD_READ_FACTOR)
        .clamp(GUARD_MIN_READ_LIMIT, RANGE_SEEK_MAX_READ_LIMIT);
    usize::try_from(limit).unwrap_or(usize::MAX)
}

fn collect_conjuncts<'a>(predicate: &'a PhysicalExpression, conjuncts: &mut Vec<&'a PhysicalExpression>) {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        collect_conjuncts(left, conjuncts);
        collect_conjuncts(right, conjuncts);
    } else {
        conjuncts.push(predicate);
    }
}

fn range_comparison_property(conjunct: &PhysicalExpression, binding: &Symbol) -> Option<Symbol> {
    let PhysicalExpression::Binary { left, operator, right } = conjunct else {
        return None;
    };
    if !matches!(
        operator,
        PhysicalBinaryOperator::LessThan
            | PhysicalBinaryOperator::LessThanOrEqual
            | PhysicalBinaryOperator::GreaterThan
            | PhysicalBinaryOperator::GreaterThanOrEqual
    ) {
        return None;
    }
    let (property, operand) = match (left.as_ref(), right.as_ref()) {
        (PhysicalExpression::Property { binding: b, property }, operand) if b == binding => (property, operand),
        (operand, PhysicalExpression::Property { binding: b, property }) if b == binding => (property, operand),
        _ => return None,
    };
    ordered_window_operand(operand).map(|_| property.clone())
}

fn range_estimate(
    labels: &[Symbol],
    property: &Symbol,
    lower: Option<&(BoundValue, bool)>,
    upper: Option<&(BoundValue, bool)>,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let total = statistics.property_statistics(labels, property)?.non_null_count?;
    let range = statistics.property_numeric_range(labels, property)?;
    let lower = match lower {
        Some((value, inclusive)) => Some((numeric_bound(value)?, *inclusive)),
        None => None,
    };
    let upper = match upper {
        Some((value, inclusive)) => Some((numeric_bound(value)?, *inclusive)),
        None => None,
    };
    let share = numeric_share(range, lower, upper)?;
    let numeric_entries = range.entries.min(total);
    let numeric = ((share * numeric_entries as f64).ceil() as u64).min(numeric_entries);
    let other_types = if lower.is_none() || upper.is_none() {
        total.saturating_sub(range.entries)
    } else {
        0
    };
    Some(numeric.saturating_add(other_types).min(total))
}

fn seekable_value(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Float(value) => value.parse::<f64>().is_ok_and(f64::is_finite),
        ScalarValue::Integer(value) => (i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(value),
        ScalarValue::Null | ScalarValue::Boolean(_) | ScalarValue::String(_) => true,
        // These values have no entries in scalar property indexes.
        ScalarValue::Decimal(_) | ScalarValue::Bytes(_) | ScalarValue::Date(_) | ScalarValue::List(_) => false,
    }
}

fn numeric_bound(value: &BoundValue) -> Option<f64> {
    let number = match &value.value {
        ScalarValue::Integer(value) => *value as f64,
        ScalarValue::Float(value) => value.parse::<f64>().ok()?,
        ScalarValue::Decimal(value) => value.to_f64(),
        ScalarValue::Null
        | ScalarValue::Boolean(_)
        | ScalarValue::String(_)
        | ScalarValue::Bytes(_)
        | ScalarValue::Date(_)
        | ScalarValue::List(_) => return None,
    };
    number.is_finite().then_some(number)
}

fn numeric_share(range: crate::PropertyNumericRange, lower: Option<(f64, bool)>, upper: Option<(f64, bool)>) -> Option<f64> {
    let (min, max) = (range.min, range.max);
    if !(min.is_finite() && max.is_finite()) || min > max {
        return None;
    }
    if min == max {
        let above = lower.is_none_or(|(bound, inclusive)| min > bound || (inclusive && min == bound));
        let below = upper.is_none_or(|(bound, inclusive)| min < bound || (inclusive && min == bound));
        return Some(if above && below { 1.0 } else { 0.0 });
    }
    let low = lower.map_or(min, |(bound, _)| bound.max(min));
    let high = upper.map_or(max, |(bound, _)| bound.min(max));
    Some(((high - low) / (max - min)).clamp(0.0, 1.0))
}

fn identity_constraint(predicate: &PhysicalExpression) -> Option<(Symbol, BoundValue)> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => identity_value(left, right).or_else(|| identity_value(right, left)),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => identity_constraint(left).or_else(|| identity_constraint(right)),
        _ => None,
    }
}

fn identity_value(identity: &PhysicalExpression, value: &PhysicalExpression) -> Option<(Symbol, BoundValue)> {
    let PhysicalExpression::Identity(binding) = identity else {
        return None;
    };
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    Some((binding.clone(), value.clone()))
}

fn identity_collection(predicate: &PhysicalExpression) -> Option<(Symbol, Vec<BoundValue>)> {
    match predicate {
        PhysicalExpression::InList { expression, values } => {
            let PhysicalExpression::Identity(binding) = expression.as_ref() else {
                return None;
            };
            Some((binding.clone(), normalize_vertex_ids(values.iter().cloned())))
        }
        PhysicalExpression::Binary {
            operator: PhysicalBinaryOperator::Or,
            ..
        } => identity_or_values(predicate),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => {
            // A single equality owns legacy validation and evaluation order.
            if identity_constraint(predicate).is_some() {
                return None;
            }
            match (identity_collection(left), identity_collection(right)) {
                (Some((left_binding, left_values)), Some((right_binding, right_values))) if left_binding == right_binding => {
                    let right = right_values.iter().filter_map(vertex_id).collect::<BTreeSet<_>>();
                    Some((
                        left_binding,
                        left_values
                            .into_iter()
                            .filter(|value| vertex_id(value).is_some_and(|id| right.contains(&id)))
                            .collect(),
                    ))
                }
                // Do not extract one identity collection from a larger AND.
                // The other conjunct can be fallible, and a point read could
                // otherwise make that eager error unreachable.
                (Some(_), None) | (None, Some(_)) => None,
                _ => None,
            }
        }
        _ => None,
    }
}

fn identity_or_values(predicate: &PhysicalExpression) -> Option<(Symbol, Vec<BoundValue>)> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => identity_value(left, right)
            .or_else(|| identity_value(right, left))
            .map(|(binding, value)| (binding, normalize_vertex_ids([value]))),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let (left_binding, left_values) = identity_or_values(left)?;
            let (right_binding, right_values) = identity_or_values(right)?;
            (left_binding == right_binding).then(|| (left_binding, normalize_vertex_ids(left_values.into_iter().chain(right_values))))
        }
        PhysicalExpression::InList { .. } => identity_collection(predicate),
        _ => None,
    }
}

fn normalize_vertex_ids(values: impl IntoIterator<Item = BoundValue>) -> Vec<BoundValue> {
    values
        .into_iter()
        .filter_map(|value| vertex_id(&value).map(|id| (id, value)))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect()
}

fn vertex_id(value: &BoundValue) -> Option<u64> {
    let ScalarValue::Integer(value) = value.value else {
        return None;
    };
    u64::try_from(value).ok()
}

pub(super) fn prepare_access(request: &VertexScanRequest, statistics: &dyn StatisticsProvider) -> VertexAccess {
    if let Some(ordered) = ordered_access(request) {
        return VertexAccess::OrderedProperty(Box::new(ordered));
    }
    if let Some(predicate) = fold_conjuncts(request.predicates.clone()) {
        if identity_constraint(&predicate).is_none() {
            if let Some((binding, values)) = identity_collection(&predicate) {
                if binding == request.binding {
                    return VertexAccess::IdPoints { values };
                }
            }
        }
    }
    if statistics.ranks_access_candidates() {
        if let Some(predicate) = fold_conjuncts(request.predicates.clone()) {
            if let Some(access) = plan_ranked_node_scan(&request.binding, &request.labels, &predicate, statistics) {
                return access;
            }
        }
    }
    match request.predicates.first() {
        Some(predicate) => legacy_node_scan_access(&request.binding, &request.labels, predicate, statistics),
        None => fallback_scan(&request.binding, &request.labels, statistics),
    }
}

pub(super) fn ordered_property<'a>(binding: &Symbol, ordering: &'a [crate::PhysicalSort]) -> Option<&'a Symbol> {
    let [first, rest @ ..] = ordering else {
        return None;
    };
    if ordering.iter().any(|item| item.nulls.is_some()) {
        return None;
    }
    let PhysicalExpression::Property {
        binding: sort_binding,
        property,
    } = &first.expression
    else {
        return None;
    };
    if sort_binding != binding {
        return None;
    }
    match rest {
        [] => {}
        [tie] => match &tie.expression {
            PhysicalExpression::Identity(tie_binding) | PhysicalExpression::Property { binding: tie_binding, .. }
                if tie_binding == binding => {}
            _ => return None,
        },
        _ => return None,
    }
    Some(property)
}

fn ordered_access(request: &VertexScanRequest) -> Option<super::OrderedVertexStorageAccess> {
    let window_request = request.window.as_ref()?;
    let property = ordered_property(&request.binding, &window_request.ordering)?;
    let first = window_request.ordering.first()?;
    let predicate = fold_conjuncts(request.predicates.clone())?;
    let (window, residual) = ordered_window_restrictions(predicate, &request.binding, property)?;
    Some(super::OrderedVertexStorageAccess {
        property: property.clone(),
        direction: first.direction,
        prefix: window.prefix,
        lower: window.lower,
        upper: window.upper,
        required: window_request.required,
        items: window_request.ordering.clone(),
        residual,
    })
}
