//! Predicate restrictions and cardinality arithmetic shared by vertex and relationship access planning.
use crate::model::compare_property_values;
use crate::{BoundValue, PhysicalBinaryOperator, PhysicalExpression, PhysicalUnaryOperator, ScalarValue, StatisticsProvider, Symbol};
use std::collections::BTreeSet;
/// Restrictions on one string property that an ordered property index can
/// prove on its own, so no residual `Filter` is needed above the scan.
#[derive(Default)]
pub(crate) struct OrderedWindowPredicate {
    pub(crate) prefix: Option<BoundValue>,
    pub(crate) lower: Option<(BoundValue, bool)>,
    pub(crate) upper: Option<(BoundValue, bool)>,
}

pub(crate) fn best_vertex_count(labels: &[Symbol], statistics: &dyn StatisticsProvider) -> Option<u64> {
    statistics.vertex_count(labels).or_else(|| {
        labels
            .iter()
            .filter_map(|label| statistics.vertex_count(std::slice::from_ref(label)))
            .min()
    })
}

pub(crate) fn property_equality_estimate(statistics: &crate::PropertyStatistics) -> Option<u64> {
    let count = statistics.non_null_count?;
    if count == 0 {
        return Some(0);
    }
    let distinct = statistics.distinct_count?;
    (distinct > 0).then(|| count.saturating_add(distinct - 1) / distinct)
}

pub(crate) fn property_values_estimate(
    labels: &[Symbol],
    property: &Symbol,
    values: &[BoundValue],
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let histogram_estimate = statistics
        .property_statistics(labels, property)
        .as_ref()
        .and_then(property_equality_estimate);
    values.iter().try_fold(0_u64, |total, value| {
        let estimate = statistics
            .property_value_count(labels, property, &value.value)
            .or_else(|| {
                statistics
                    .bloom_may_contain(labels, property, &value.value)
                    .and_then(|possibly_present| (!possibly_present).then_some(0))
            })
            .or(histogram_estimate)?;
        Some(total.saturating_add(estimate))
    })
}

pub(crate) fn prefer_index(index_rows: Option<u64>, scan_rows: Option<u64>) -> bool {
    match (index_rows, scan_rows) {
        (Some(index), Some(scan)) => index <= scan,
        (Some(_), None) | (None, _) => true,
    }
}

pub(crate) fn equality_constraint(predicate: &PhysicalExpression) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
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
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => equality_constraint(left).or_else(|| equality_constraint(right)),
        // `x.p IN [a, b, c]` is the same candidate set as `x.p = a OR x.p = b
        // OR x.p = c`, so it reaches the existing multi-seek rather than
        // degrading to a scan. An empty list yields no constraint, which
        // leaves the residual filter to reject every row.
        PhysicalExpression::InList { expression, values } => {
            let (binding, property) = expression_property(expression)?;
            // Filter out NULL candidates: NULL cannot be a property seek key,
            // and Cypher's three-valued IN semantics already handle NULLs via
            // the residual predicate (x IN [1, NULL] → true when x=1, NULL
            // otherwise).
            let non_null = values.iter().filter(|value| !matches!(value.value, ScalarValue::Null));
            let unique: BTreeSet<BoundValue> = non_null.clone().map(index_key).collect::<Option<BTreeSet<_>>>()?;
            if unique.is_empty() {
                return None;
            }
            Some((binding, property, unique.into_iter().collect()))
        }
        _ => None,
    }
}

pub(crate) fn equality_constraint_for_binding(
    predicate: &PhysicalExpression,
    wanted: &Symbol,
) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        return equality_constraint_for_binding(left, wanted).or_else(|| equality_constraint_for_binding(right, wanted));
    }
    equality_constraint(predicate).filter(|(binding, ..)| binding == wanted)
}

pub(crate) fn property_scan_constraint(predicate: &PhysicalExpression) -> Option<(Symbol, Symbol)> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => property_scan_constraint(left).or_else(|| property_scan_constraint(right)),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let left = property_scan_constraint(left)?;
            let right = property_scan_constraint(right)?;
            (left == right).then_some(left)
        }
        PhysicalExpression::Unary {
            operator: PhysicalUnaryOperator::Not,
            ..
        } => None,
        PhysicalExpression::Binary {
            left,
            operator:
                PhysicalBinaryOperator::Equal
                | PhysicalBinaryOperator::LessThan
                | PhysicalBinaryOperator::LessThanOrEqual
                | PhysicalBinaryOperator::GreaterThan
                | PhysicalBinaryOperator::GreaterThanOrEqual,
            right,
        } => indexable_property_comparison(left, right).or_else(|| indexable_property_comparison(right, left)),
        PhysicalExpression::InList { expression, values }
            if values.iter().any(|value| !matches!(value.value, ScalarValue::Null))
                && values
                    .iter()
                    .filter(|value| !matches!(value.value, ScalarValue::Null))
                    .all(|value| index_key(value).is_some()) =>
        {
            expression_property(expression)
        }
        PhysicalExpression::InList { .. } => None,
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::ElementTest { .. }
        | PhysicalExpression::PathNodesDistinct { .. }
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_)
        | PhysicalExpression::Aggregate { .. }
        | PhysicalExpression::Function { .. }
        | PhysicalExpression::List(_)
        | PhysicalExpression::Case { .. }
        | PhysicalExpression::Unary { .. }
        | PhysicalExpression::TypeTest { .. }
        | PhysicalExpression::Cast { .. }
        | PhysicalExpression::Record(_)
        | PhysicalExpression::Let { .. }
        | PhysicalExpression::PathConstructor(_)
        | PhysicalExpression::Field { .. }
        | PhysicalExpression::Binary { .. } => None,
    }
}

fn expression_property(expression: &PhysicalExpression) -> Option<(Symbol, Symbol)> {
    let PhysicalExpression::Property { binding, property } = expression else {
        return None;
    };
    Some((binding.clone(), property.clone()))
}

fn property_value(property: &PhysicalExpression, value: &PhysicalExpression) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
    let PhysicalExpression::Property { binding, property } = property else {
        return None;
    };
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    if matches!(value.value, ScalarValue::Null) {
        return None;
    }
    Some((binding.clone(), property.clone(), vec![index_key(value)?]))
}

fn index_key(value: &BoundValue) -> Option<BoundValue> {
    let key = match &value.value {
        ScalarValue::Decimal(decimal) => match decimal.integer() {
            Some(integer) if (i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(&integer) => ScalarValue::Integer(integer),
            // Past every stored integer (or not integral), a decimal equals a
            // stored number only as the float that holds it exactly:
            // `1E20M` is the float 1e20.
            _ => {
                let float = decimal.to_f64();
                if crate::Decimal::from_f64_exact(float).is_none_or(|exact| exact != *decimal) {
                    return None;
                }
                ScalarValue::Float(format!("{float:?}").into())
            }
        },
        ScalarValue::Bytes(_) | ScalarValue::Date(_) | ScalarValue::List(_) => return None,
        _ => return Some(value.clone()),
    };
    Some(BoundValue {
        value: key,
        origin: value.origin.clone(),
    })
}

pub(crate) fn ordered_window_restrictions(
    predicate: PhysicalExpression,
    binding: &Symbol,
    property: &Symbol,
) -> Option<(OrderedWindowPredicate, Option<PhysicalExpression>)> {
    let mut window = OrderedWindowPredicate::default();
    let mut residual = Vec::new();
    collect_ordered_window_predicate(&predicate, binding, property, &mut window, &mut residual);
    // An equality on the sorted property names the exact values wanted, and a
    // seek on them returns those rows already in order.
    if residual.iter().any(|conjunct| {
        matches!(
            equality_constraint(conjunct),
            Some((constraint_binding, constraint_property, _))
                if constraint_binding == *binding && constraint_property == *property
        )
    }) {
        return None;
    }
    // Every term went to the residual, so the index bounds nothing: that is a
    // full scan wearing an operator's name.
    if window.prefix.is_none() && window.lower.is_none() && window.upper.is_none() {
        return None;
    }
    Some((window, fold_conjuncts(residual)))
}

pub(crate) fn collect_ordered_window_predicate(
    predicate: &PhysicalExpression,
    binding: &Symbol,
    property: &Symbol,
    window: &mut OrderedWindowPredicate,
    residual: &mut Vec<PhysicalExpression>,
) {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        collect_ordered_window_predicate(left, binding, property, window, residual);
        collect_ordered_window_predicate(right, binding, property, window, residual);
        return;
    }
    if !fill_ordered_window_slot(predicate, binding, property, window) {
        // An `Or` stays residual -- neither side restricts on its own -- but a
        // bound every branch implies is still sound for the walk to start
        // from. This is the keyset shape `p > x OR (p = x AND id > y)`, and
        // its descending mirror `p < x OR (p = x AND id < y)`, so both ends
        // are tried: a descending walk starts from the upper bound.
        for lower in [true, false] {
            let slot = if lower { &mut window.lower } else { &mut window.upper };
            if slot.is_none() {
                if let Some(bound) = implied_window_bound(predicate, binding, property, lower) {
                    *slot = Some(bound);
                }
            }
        }
        residual.push(predicate.clone());
    }
}

pub(crate) fn ordered_window_operand(expression: &PhysicalExpression) -> Option<BoundValue> {
    match expression {
        PhysicalExpression::Value(value) if !matches!(value.value, ScalarValue::Null) => index_key(value),
        PhysicalExpression::Unary {
            operator: PhysicalUnaryOperator::Minus,
            expression,
        } => match expression.as_ref() {
            PhysicalExpression::Value(BoundValue {
                value: ScalarValue::Integer(value),
                origin,
            }) => value.checked_neg().map(|value| BoundValue {
                value: ScalarValue::Integer(value),
                origin: origin.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

fn implied_window_bound(predicate: &PhysicalExpression, binding: &Symbol, property: &Symbol, lower: bool) -> Option<(BoundValue, bool)> {
    let PhysicalExpression::Binary { left, operator, right } = predicate else {
        return None;
    };
    // Ordered by the same total order the index walks, so a bound on any
    // stored scalar type participates, not only a string one.
    let order = |left: &(BoundValue, bool), right: &(BoundValue, bool)| compare_property_values(&left.0.value, &right.0.value);
    match operator {
        // Either branch may be the one that matches, so only a bound both
        // imply is sound -- the weaker of the two.
        PhysicalBinaryOperator::Or => {
            let left = implied_window_bound(left, binding, property, lower)?;
            let right = implied_window_bound(right, binding, property, lower)?;
            let weaker = if lower {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
            Some(match order(&left, &right) {
                ordering if ordering == weaker => left,
                std::cmp::Ordering::Equal => (left.0, left.1 || right.1),
                _ => right,
            })
        }
        PhysicalBinaryOperator::And => {
            match (
                implied_window_bound(left, binding, property, lower),
                implied_window_bound(right, binding, property, lower),
            ) {
                // Both branches hold, so the stronger bound is sound.
                (Some(left), Some(right)) => {
                    let stronger = if lower {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    };
                    Some(match order(&left, &right) {
                        ordering if ordering == stronger => left,
                        std::cmp::Ordering::Equal => (left.0, left.1 && right.1),
                        _ => right,
                    })
                }
                (bound @ Some(_), None) | (None, bound @ Some(_)) => bound,
                (None, None) => None,
            }
        }
        _ => {
            let is_target = |expression: &PhysicalExpression| {
                matches!(
                    expression,
                    PhysicalExpression::Property { binding: b, property: p }
                        if b == binding && p == property
                )
            };
            let (value, operator) = if is_target(left) {
                (ordered_window_operand(right)?, *operator)
            } else if is_target(right) {
                let flipped = match operator {
                    PhysicalBinaryOperator::LessThan => PhysicalBinaryOperator::GreaterThan,
                    PhysicalBinaryOperator::LessThanOrEqual => PhysicalBinaryOperator::GreaterThanOrEqual,
                    PhysicalBinaryOperator::GreaterThan => PhysicalBinaryOperator::LessThan,
                    PhysicalBinaryOperator::GreaterThanOrEqual => PhysicalBinaryOperator::LessThanOrEqual,
                    PhysicalBinaryOperator::Equal => PhysicalBinaryOperator::Equal,
                    _ => return None,
                };
                (ordered_window_operand(left)?, flipped)
            } else {
                return None;
            };
            match (operator, lower) {
                (PhysicalBinaryOperator::GreaterThan, true) | (PhysicalBinaryOperator::LessThan, false) => Some((value, false)),
                (PhysicalBinaryOperator::GreaterThanOrEqual, true)
                | (PhysicalBinaryOperator::LessThanOrEqual, false)
                | (PhysicalBinaryOperator::Equal, _) => Some((value, true)),
                _ => None,
            }
        }
    }
}

fn fill_ordered_window_slot(
    predicate: &PhysicalExpression,
    binding: &Symbol,
    property: &Symbol,
    window: &mut OrderedWindowPredicate,
) -> bool {
    let PhysicalExpression::Binary { left, operator, right } = predicate else {
        return false;
    };
    let is_target = |expression: &PhysicalExpression| {
        matches!(
            expression,
            PhysicalExpression::Property { binding: b, property: p } if b == binding && p == property
        )
    };
    // Any non-null operand: a range comparison on a stored value decides
    // exactly what the index order does. Only a prefix needs a string.
    let scalar_value = ordered_window_operand;
    // Normalize `value OP property` into `property OP' value`.
    let (value, operator) = if is_target(left) {
        (scalar_value(right), *operator)
    } else if is_target(right) {
        let flipped = match operator {
            PhysicalBinaryOperator::LessThan => PhysicalBinaryOperator::GreaterThan,
            PhysicalBinaryOperator::LessThanOrEqual => PhysicalBinaryOperator::GreaterThanOrEqual,
            PhysicalBinaryOperator::GreaterThan => PhysicalBinaryOperator::LessThan,
            PhysicalBinaryOperator::GreaterThanOrEqual => PhysicalBinaryOperator::LessThanOrEqual,
            // `'x' STARTS WITH n.p` is not an index restriction on n.p.
            _ => return false,
        };
        (scalar_value(left), flipped)
    } else {
        return false;
    };
    let Some(value) = value else {
        return false;
    };
    let slot = match operator {
        PhysicalBinaryOperator::StartsWith if matches!(value.value, ScalarValue::String(_)) => &mut window.prefix,
        PhysicalBinaryOperator::GreaterThan | PhysicalBinaryOperator::GreaterThanOrEqual => {
            let inclusive = operator == PhysicalBinaryOperator::GreaterThanOrEqual;
            return window.lower.is_none() && {
                window.lower = Some((value, inclusive));
                true
            };
        }
        PhysicalBinaryOperator::LessThan | PhysicalBinaryOperator::LessThanOrEqual => {
            let inclusive = operator == PhysicalBinaryOperator::LessThanOrEqual;
            return window.upper.is_none() && {
                window.upper = Some((value, inclusive));
                true
            };
        }
        _ => return false,
    };
    if slot.is_some() {
        return false;
    }
    *slot = Some(value);
    true
}

pub(crate) fn fold_conjuncts(conjuncts: Vec<PhysicalExpression>) -> Option<PhysicalExpression> {
    conjuncts.into_iter().reduce(|left, right| PhysicalExpression::Binary {
        left: Box::new(left),
        operator: PhysicalBinaryOperator::And,
        right: Box::new(right),
    })
}

fn indexable_property_comparison(property: &PhysicalExpression, value: &PhysicalExpression) -> Option<(Symbol, Symbol)> {
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    index_key(value)?;
    expression_property(property)
}
