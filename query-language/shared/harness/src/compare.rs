use crate::values::{Node, Relationship, Value};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    Ordered,
    Unordered,
}

fn multiset<T>(left: &[T], right: &[T], eq: impl Fn(&T, &T) -> bool) -> bool {
    if left.len() != right.len() {
        return false;
    }
    // Matching rather than sorting preserves multiplicities and supports nested values.
    // Augmenting paths are required when numeric equality is not transitive.
    fn augment<T>(i: usize, a: &[T], b: &[T], eq: &impl Fn(&T, &T) -> bool, seen: &mut [bool], matched: &mut [Option<usize>]) -> bool {
        for j in 0..b.len() {
            if !seen[j] && eq(&a[i], &b[j]) {
                seen[j] = true;
                if matched[j].is_none() || augment(matched[j].unwrap(), a, b, eq, seen, matched) {
                    matched[j] = Some(i);
                    return true;
                }
            }
        }
        false
    }
    let mut matched = vec![None; right.len()];
    (0..left.len()).all(|i| augment(i, left, right, &eq, &mut vec![false; right.len()], &mut matched))
}

fn map_eq(a: &BTreeMap<String, Value>, b: &BTreeMap<String, Value>, list_order: Order) -> bool {
    a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|other| value_eq(v, other, list_order)))
}
fn node_eq(a: &Node, b: &Node, order: Order) -> bool {
    a.labels == b.labels && map_eq(&a.properties, &b.properties, order)
}
fn rel_eq(a: &Relationship, b: &Relationship, order: Order) -> bool {
    a.relationship_type == b.relationship_type && map_eq(&a.properties, &b.properties, order)
}

fn exact_integer(float: f64) -> Option<i128> {
    if !float.is_finite() || float.fract() != 0.0 {
        return None;
    }
    if float == 0.0 {
        return Some(0);
    }
    let bits = float.to_bits();
    let negative = bits >> 63 != 0;
    let exponent = ((bits >> 52) & 0x7ff) as i32 - 1023;
    if exponent < 0 {
        return None;
    }
    let significand = u128::from((bits & ((1_u64 << 52) - 1)) | (1_u64 << 52));
    let magnitude = if exponent >= 52 {
        let shift = (exponent - 52) as u32;
        if shift >= 128 || significand > (u128::MAX >> shift) {
            return None;
        }
        significand << shift
    } else {
        let discarded = 52 - exponent;
        let mask = (1_u128 << discarded) - 1;
        if significand & mask != 0 {
            return None;
        }
        significand >> discarded
    };
    if negative {
        let minimum_magnitude = 1_u128 << 127;
        if magnitude == minimum_magnitude {
            Some(i128::MIN)
        } else {
            i128::try_from(magnitude).ok().map(|value| -value)
        }
    } else {
        i128::try_from(magnitude).ok()
    }
}

/// Structural comparison independent of engine-assigned graph identities.
pub fn value_eq(a: &Value, b: &Value, list_order: Order) -> bool {
    use Value::*;
    match (a, b) {
        (Null, Null) => true,
        (Boolean(a), Boolean(b)) => a == b,
        (Integer(a), Integer(b)) => a == b,
        (Float(a), Float(b)) => {
            let (a, b) = (f64::from_bits(*a), f64::from_bits(*b));
            a == b || (a.is_nan() && b.is_nan())
        }
        (Integer(i), Float(f)) | (Float(f), Integer(i)) => {
            let f = f64::from_bits(*f);
            exact_integer(f) == Some(*i)
        }
        (String(a), String(b)) => a == b,
        (List(a), List(b)) => match list_order {
            Order::Ordered => a.len() == b.len() && a.iter().zip(b).all(|(a, b)| value_eq(a, b, list_order)),
            Order::Unordered => multiset(a, b, |a, b| value_eq(a, b, list_order)),
        },
        (Map(a), Map(b)) => map_eq(a, b, list_order),
        (Node(a), Node(b)) => node_eq(a, b, list_order),
        (Relationship(a), Relationship(b)) => rel_eq(a, b, list_order),
        (Path(a), Path(b)) => {
            node_eq(&a.start, &b.start, list_order)
                && a.segments.len() == b.segments.len()
                && a.segments
                    .iter()
                    .zip(&b.segments)
                    .all(|((ad, ar, an), (bd, br, bn))| ad == bd && rel_eq(ar, br, list_order) && node_eq(an, bn, list_order))
        }
        _ => false,
    }
}

/// Compare rows after mapping actual columns to expected column names.
pub fn rows_equal(
    expected_columns: &[String],
    expected: &[Vec<Value>],
    actual_columns: &[String],
    actual: &[Vec<Value>],
    rows: Order,
    lists: Order,
) -> bool {
    if expected_columns.len() != actual_columns.len() || expected.len() != actual.len() {
        return false;
    }
    let mut mapping = Vec::new();
    for (i, col) in expected_columns.iter().enumerate() {
        if expected_columns[..i].contains(col) {
            return false;
        }
        let Some(j) = actual_columns.iter().position(|x| x == col) else {
            return false;
        };
        if actual_columns.iter().filter(|x| *x == col).count() != 1 {
            return false;
        }
        mapping.push(j);
    }
    if expected.iter().any(|r| r.len() != expected_columns.len()) || actual.iter().any(|r| r.len() != actual_columns.len()) {
        return false;
    }
    let eq = |a: &Vec<Value>, b: &Vec<Value>| a.iter().zip(&mapping).all(|(v, j)| value_eq(v, &b[*j], lists));
    match rows {
        Order::Ordered => expected.iter().zip(actual).all(|(a, b)| eq(a, b)),
        Order::Unordered => multiset(expected, actual, eq),
    }
}
