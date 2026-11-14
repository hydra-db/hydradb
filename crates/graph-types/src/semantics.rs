use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::{CypherValue, MapValue, PointValue, RelationshipIdentity, RelationshipRef};

/// Compare two values using Cypher semantics rather than representation-level
/// equality. Integers, decimals and exactly equal floats compare equal,
/// recursively in lists and maps; null and missing remain distinct.
pub fn semantic_eq(left: &CypherValue, right: &CypherValue) -> bool {
    match (left, right) {
        (CypherValue::Integer(left), CypherValue::Float(right)) => integer_float_equal(*left, *right),
        (CypherValue::Float(left), CypherValue::Integer(right)) => integer_float_equal(*right, *left),
        (CypherValue::Integer(left), CypherValue::Decimal(right)) => decimal_cmp_integer(right, *left) == Ordering::Equal,
        (CypherValue::Decimal(left), CypherValue::Integer(right)) => decimal_cmp_integer(left, *right) == Ordering::Equal,
        (CypherValue::Float(left), CypherValue::Decimal(right)) => decimal_cmp_float(right, *left) == Ordering::Equal,
        (CypherValue::Decimal(left), CypherValue::Float(right)) => decimal_cmp_float(left, *right) == Ordering::Equal,
        (CypherValue::Decimal(left), CypherValue::Decimal(right)) => decimal_cmp(left, right) == Ordering::Equal,
        (CypherValue::Float(left), CypherValue::Float(right)) => left == right,
        (CypherValue::Node(left), CypherValue::NodeEntity(right)) => *left == right.id,
        (CypherValue::NodeEntity(left), CypherValue::Node(right)) => left.id == *right,
        (CypherValue::NodeEntity(left), CypherValue::NodeEntity(right)) => left.id == right.id,
        (CypherValue::List(left), CypherValue::List(right)) => {
            left.len() == right.len() && left.iter().zip(right).all(|(left, right)| semantic_eq(left, right))
        }
        (CypherValue::Map(left), CypherValue::Map(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|((left_key, left_value), (right_key, right_value))| left_key == right_key && semantic_eq(left_value, right_value))
        }
        (CypherValue::Record(left), CypherValue::Record(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|((left_key, left_value), (right_key, right_value))| left_key == right_key && semantic_eq(left_value, right_value))
        }
        (CypherValue::Relationship(left), CypherValue::Relationship(right)) => relationship_identity_eq(left, right),
        (CypherValue::Relationship(left), CypherValue::RelationshipEntity(right)) => left.identity() == right.identity.identity(),
        (CypherValue::RelationshipEntity(left), CypherValue::Relationship(right)) => left.identity.identity() == right.identity(),
        (CypherValue::RelationshipEntity(left), CypherValue::RelationshipEntity(right)) => {
            left.identity.identity() == right.identity.identity()
        }
        (CypherValue::Path(left), CypherValue::Path(right)) => {
            left.nodes() == right.nodes()
                && left.relationships().len() == right.relationships().len()
                && left
                    .relationships()
                    .iter()
                    .zip(right.relationships())
                    .all(|(left, right)| left.orientation() == right.orientation() && left.identity() == right.identity())
        }
        (CypherValue::Point(left), CypherValue::Point(right)) => point_eq(left, right),
        _ => left == right,
    }
}

pub fn semantic_eq_three_valued(left: &CypherValue, right: &CypherValue) -> Option<bool> {
    if matches!(left, CypherValue::Null | CypherValue::Missing) || matches!(right, CypherValue::Null | CypherValue::Missing) {
        return None;
    }
    match (left, right) {
        (CypherValue::List(left), CypherValue::List(right)) => semantic_sequence_eq(left, right),
        (CypherValue::Map(left), CypherValue::Map(right)) | (CypherValue::Record(left), CypherValue::Record(right)) => {
            semantic_map_eq(left, right)
        }
        _ => Some(semantic_eq(left, right)),
    }
}

pub fn semantic_list_membership(needle: &CypherValue, values: &[CypherValue]) -> Option<bool> {
    if values.is_empty() {
        return Some(false);
    }
    if matches!(needle, CypherValue::Null | CypherValue::Missing) {
        return None;
    }
    let mut unknown = false;
    for value in values {
        match semantic_eq_three_valued(needle, value) {
            Some(true) => return Some(true),
            Some(false) => {}
            None => unknown = true,
        }
    }
    (!unknown).then_some(false)
}

fn semantic_sequence_eq(left: &[CypherValue], right: &[CypherValue]) -> Option<bool> {
    if left.len() != right.len() {
        return Some(false);
    }
    let mut unknown = false;
    for (left, right) in left.iter().zip(right) {
        match semantic_eq_three_valued(left, right) {
            Some(true) => {}
            Some(false) => return Some(false),
            None => unknown = true,
        }
    }
    (!unknown).then_some(true)
}

fn semantic_map_eq(left: &MapValue, right: &MapValue) -> Option<bool> {
    if left.len() != right.len() {
        return Some(false);
    }
    let mut unknown = false;
    for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right) {
        if left_key != right_key {
            return Some(false);
        }
        match semantic_eq_three_valued(left_value, right_value) {
            Some(true) => {}
            Some(false) => return Some(false),
            None => unknown = true,
        }
    }
    (!unknown).then_some(true)
}

/// Total, deterministic ordering for Cypher values. Numeric values are ordered
/// by numeric magnitude across integer and float representations. NaNs retain
/// the current engine's `f64::total_cmp` ordering, including signed NaNs.
pub fn semantic_cmp(left: &CypherValue, right: &CypherValue) -> Ordering {
    use CypherValue::*;

    match (left, right) {
        (Null, Null) | (Missing, Missing) => Ordering::Equal,
        (Null, _) => Ordering::Greater,
        (_, Null) => Ordering::Less,
        (Missing, _) => Ordering::Greater,
        (_, Missing) => Ordering::Less,
        (Boolean(left), Boolean(right)) => left.cmp(right),
        (Integer(left), Integer(right)) => left.cmp(right),
        (Integer(left), Float(right)) => compare_integer_float(*left, *right),
        (Float(left), Integer(right)) => compare_integer_float(*right, *left).reverse(),
        (Integer(left), Decimal(right)) => decimal_cmp_integer(right, *left).reverse(),
        (Decimal(left), Integer(right)) => decimal_cmp_integer(left, *right),
        (Float(left), Decimal(right)) => decimal_cmp_float(right, *left).reverse(),
        (Decimal(left), Float(right)) => decimal_cmp_float(left, *right),
        (Decimal(left), Decimal(right)) => decimal_cmp(left, right),
        (Float(left), Float(right)) => left.partial_cmp(right).unwrap_or_else(|| left.total_cmp(right)),
        (String(left), String(right)) => left.cmp(right),
        (Bytes(left), Bytes(right)) => left.cmp(right),
        (List(left), List(right)) => compare_sequences(left, right),
        (Map(left), Map(right)) => compare_maps(left, right),
        (Record(left), Record(right)) => compare_maps(left, right),
        (Node(left), Node(right)) => left.cmp(right),
        (Node(left), NodeEntity(right)) => left.cmp(&right.id),
        (NodeEntity(left), Node(right)) => left.id.cmp(right),
        (NodeEntity(left), NodeEntity(right)) => left.id.cmp(&right.id),
        (Relationship(left), Relationship(right)) => left.identity().cmp(right.identity()),
        (Relationship(left), RelationshipEntity(right)) => left.identity().cmp(right.identity.identity()),
        (RelationshipEntity(left), Relationship(right)) => left.identity.identity().cmp(right.identity()),
        (RelationshipEntity(left), RelationshipEntity(right)) => left.identity.identity().cmp(right.identity.identity()),
        (Path(left), Path(right)) => left.cmp(right),
        (Date(left), Date(right)) => left.cmp(right),
        (LocalTime(left), LocalTime(right)) => left.cmp(right),
        (Time(left), Time(right)) => left.cmp(right),
        (LocalDateTime(left), LocalDateTime(right)) => left.cmp(right),
        (DateTime(left), DateTime(right)) => left.cmp(right),
        (Duration(left), Duration(right)) => left.cmp(right),
        (Point(left), Point(right)) => compare_points(left, right),
        (Extension(left), Extension(right)) => left.cmp(right),
        (Temporal(left), Temporal(right)) => left.cmp(right),
        _ => value_rank(left).cmp(&value_rank(right)),
    }
}

/// Hash a value using Cypher equality semantics. Values which compare equal
/// under [`semantic_eq`] always feed identical bytes to the caller's hasher.
pub fn hash_semantic<H: Hasher>(value: &CypherValue, state: &mut H) {
    use CypherValue::*;

    match value {
        Null => 0_u8.hash(state),
        Missing => 1_u8.hash(state),
        Boolean(value) => {
            2_u8.hash(state);
            value.hash(state);
        }
        Integer(value) => {
            3_u8.hash(state);
            hash_numeric_parts(&decimal_parts(&value.to_string()).expect("integer is a decimal value"), state);
        }
        Float(value) => {
            3_u8.hash(state);
            hash_float_semantic(*value, state);
        }
        Decimal(value) => {
            3_u8.hash(state);
            match decimal_parts(value) {
                Some(parts) => hash_numeric_parts(&parts, state),
                None => value.hash(state),
            }
        }
        String(value) => {
            4_u8.hash(state);
            value.hash(state);
        }
        Bytes(value) => {
            18_u8.hash(state);
            value.hash(state);
        }
        List(values) => {
            5_u8.hash(state);
            values.len().hash(state);
            for value in values {
                hash_semantic(value, state);
            }
        }
        Map(values) => {
            6_u8.hash(state);
            values.len().hash(state);
            for (key, value) in values {
                key.hash(state);
                hash_semantic(value, state);
            }
        }
        Record(values) => {
            19_u8.hash(state);
            values.len().hash(state);
            for (key, value) in values {
                key.hash(state);
                hash_semantic(value, state);
            }
        }
        Node(value) => {
            7_u8.hash(state);
            value.hash(state);
        }
        NodeEntity(value) => {
            7_u8.hash(state);
            value.id.hash(state);
        }
        Relationship(value) => {
            8_u8.hash(state);
            hash_relationship_identity(value.identity(), state);
        }
        RelationshipEntity(value) => {
            8_u8.hash(state);
            hash_relationship_identity(value.identity.identity(), state);
        }
        Path(value) => {
            9_u8.hash(state);
            value.nodes().hash(state);
            value.relationships().len().hash(state);
            for relationship in value.relationships() {
                hash_relationship_identity(relationship.identity(), state);
                relationship.orientation().hash(state);
            }
        }
        Date(value) => {
            10_u8.hash(state);
            value.hash(state);
        }
        LocalTime(value) => {
            11_u8.hash(state);
            value.hash(state);
        }
        Time(value) => {
            12_u8.hash(state);
            value.hash(state);
        }
        LocalDateTime(value) => {
            13_u8.hash(state);
            value.hash(state);
        }
        DateTime(value) => {
            14_u8.hash(state);
            value.hash(state);
        }
        Duration(value) => {
            15_u8.hash(state);
            value.hash(state);
        }
        Point(value) => {
            16_u8.hash(state);
            value.srid.hash(state);
            hash_float_semantic(value.x, state);
            hash_float_semantic(value.y, state);
            match value.z {
                Some(z) => {
                    1_u8.hash(state);
                    hash_float_semantic(z, state);
                }
                None => 0_u8.hash(state),
            }
        }
        Extension(value) => {
            17_u8.hash(state);
            value.hash(state);
        }
        Temporal(value) => {
            20_u8.hash(state);
            value.hash(state);
        }
    }
}

fn relationship_identity_eq(left: &RelationshipRef, right: &RelationshipRef) -> bool {
    match (left.identity(), right.identity()) {
        (RelationshipIdentity::Structural { .. }, RelationshipIdentity::Structural { .. })
        | (RelationshipIdentity::Identified(_), RelationshipIdentity::Identified(_)) => left.identity() == right.identity(),
        _ => false,
    }
}

fn integer_float_equal(integer: i128, float: f64) -> bool {
    const I128_LOWER: f64 = -170141183460469231731687303715884105728.0;
    const I128_UPPER_EXCLUSIVE: f64 = 170141183460469231731687303715884105728.0;
    float.is_finite() && float.fract() == 0.0 && (I128_LOWER..I128_UPPER_EXCLUSIVE).contains(&float) && float as i128 == integer
}

fn compare_integer_float(integer: i128, float: f64) -> Ordering {
    const I128_LOWER: f64 = -170141183460469231731687303715884105728.0;
    const I128_UPPER_EXCLUSIVE: f64 = 170141183460469231731687303715884105728.0;
    if float.is_nan() {
        return if float.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    if float < I128_LOWER {
        return Ordering::Greater;
    }
    if float >= I128_UPPER_EXCLUSIVE {
        return Ordering::Less;
    }
    let truncated = float as i128;
    match integer.cmp(&truncated) {
        Ordering::Equal if float.fract() > 0.0 => Ordering::Less,
        Ordering::Equal if float.fract() < 0.0 => Ordering::Greater,
        ordering => ordering,
    }
}

fn hash_float_semantic<H: Hasher>(value: f64, state: &mut H) {
    if let Some(parts) = exact_float_decimal(value) {
        hash_numeric_parts(&parts, state);
        return;
    }
    if value == 0.0 {
        0_u64.hash(state);
    } else {
        value.to_bits().hash(state);
    }
}

fn hash_relationship_identity<H: Hasher>(identity: &RelationshipIdentity, state: &mut H) {
    match identity {
        RelationshipIdentity::Structural {
            source,
            destination,
            relationship_type,
        } => {
            0_u8.hash(state);
            source.hash(state);
            destination.hash(state);
            relationship_type.hash(state);
        }
        RelationshipIdentity::Identified(id) => {
            1_u8.hash(state);
            id.hash(state);
        }
    }
}

fn value_rank(value: &CypherValue) -> u8 {
    match value {
        CypherValue::Boolean(_) => 0,
        CypherValue::Integer(_) | CypherValue::Float(_) | CypherValue::Decimal(_) => 1,
        CypherValue::String(_) => 2,
        CypherValue::Bytes(_) => 3,
        CypherValue::Node(_) | CypherValue::NodeEntity(_) => 4,
        CypherValue::Relationship(_) | CypherValue::RelationshipEntity(_) => 5,
        CypherValue::Path(_) => 6,
        CypherValue::Date(_) => 7,
        CypherValue::LocalTime(_) => 8,
        CypherValue::Time(_) => 9,
        CypherValue::LocalDateTime(_) => 10,
        CypherValue::DateTime(_) => 11,
        CypherValue::Duration(_) => 12,
        CypherValue::Point(_) => 13,
        CypherValue::Extension(_) => 14,
        CypherValue::List(_) => 15,
        CypherValue::Map(_) => 16,
        CypherValue::Null => 17,
        CypherValue::Missing => 18,
        CypherValue::Record(_) => 19,
        CypherValue::Temporal(_) => 20,
    }
}

#[derive(Clone, Debug)]
struct DecimalParts {
    negative: bool,
    digits: String,
    scale: usize,
}

fn decimal_parts(value: &str) -> Option<DecimalParts> {
    let (negative, value) = match value.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let mut split = value.split('.');
    let whole = split.next()?;
    let fraction = split.next().unwrap_or_default();
    if split.next().is_some()
        || (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{whole}{fraction}");
    if let Some(first_nonzero) = digits.bytes().position(|byte| byte != b'0') {
        digits.drain(..first_nonzero);
    } else {
        return Some(DecimalParts {
            negative: false,
            digits: "0".to_owned(),
            scale: 0,
        });
    }
    let mut scale = fraction.len();
    while scale > 0 && digits.ends_with('0') {
        digits.pop();
        scale -= 1;
    }
    Some(DecimalParts { negative, digits, scale })
}

fn decimal_cmp(left: &str, right: &str) -> Ordering {
    match (decimal_parts(left), decimal_parts(right)) {
        (Some(left), Some(right)) => compare_decimal_parts(&left, &right),
        _ => left.cmp(right),
    }
}

fn decimal_cmp_integer(decimal: &str, integer: i128) -> Ordering {
    match decimal_parts(decimal) {
        Some(decimal) => compare_decimal_parts(&decimal, &decimal_parts(&integer.to_string()).expect("integer is decimal")),
        None => decimal.cmp(&integer.to_string()),
    }
}

fn decimal_cmp_float(decimal: &str, float: f64) -> Ordering {
    if float.is_nan() {
        return if float.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    if float.is_infinite() {
        return if float.is_sign_positive() {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let Some(decimal_value_parts) = decimal_parts(decimal) else {
        return decimal.cmp(&format!("{float:.80}"));
    };
    if let Some(float_parts) = exact_float_decimal(float) {
        return compare_decimal_parts(&decimal_value_parts, &float_parts);
    }
    if decimal_value_parts.digits == "0" && float != 0.0 {
        return if float.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    let float_text = format!("{float:.80}");
    decimal_parts(&float_text)
        .map(|float_parts| compare_decimal_parts(&decimal_value_parts, &float_parts))
        .unwrap_or_else(|| decimal.cmp(&float_text))
}

fn compare_decimal_parts(left: &DecimalParts, right: &DecimalParts) -> Ordering {
    let left_is_zero = left.digits == "0";
    let right_is_zero = right.digits == "0";
    match (left_is_zero, right_is_zero) {
        (true, true) => return Ordering::Equal,
        (true, false) => return if right.negative { Ordering::Greater } else { Ordering::Less },
        (false, true) => return if left.negative { Ordering::Less } else { Ordering::Greater },
        (false, false) => {}
    }
    match left.negative.cmp(&right.negative).reverse() {
        Ordering::Equal => {}
        ordering => return ordering,
    }
    let left_magnitude = left.digits.len() as i64 - left.scale as i64;
    let right_magnitude = right.digits.len() as i64 - right.scale as i64;
    let magnitude = left_magnitude.cmp(&right_magnitude).then_with(|| {
        let width = left.digits.len().max(right.digits.len());
        (0..width)
            .map(|index| {
                left.digits
                    .as_bytes()
                    .get(index)
                    .copied()
                    .unwrap_or(b'0')
                    .cmp(&right.digits.as_bytes().get(index).copied().unwrap_or(b'0'))
            })
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    if left.negative {
        magnitude.reverse()
    } else {
        magnitude
    }
}

fn exact_float_decimal(value: f64) -> Option<DecimalParts> {
    if !value.is_finite() {
        return None;
    }
    let bits = value.to_bits();
    let negative = bits >> 63 != 0;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (mut mantissa, mut exponent) = if exponent_bits == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1_u64 << 52), exponent_bits - 1075)
    };
    if mantissa == 0 {
        return Some(DecimalParts {
            negative: false,
            digits: "0".to_owned(),
            scale: 0,
        });
    }
    while exponent < 0 && mantissa % 2 == 0 {
        mantissa /= 2;
        exponent += 1;
    }
    let (digits, scale) = if exponent >= 0 {
        let mut digits = mantissa.to_string();
        for _ in 0..exponent {
            digits = multiply_decimal_digits(&digits, 2);
        }
        (digits, 0)
    } else {
        let scale = exponent.unsigned_abs() as usize;
        let mut digits = mantissa.to_string();
        for _ in 0..scale {
            digits = multiply_decimal_digits(&digits, 5);
        }
        (digits, scale)
    };
    normalized_decimal_parts(negative, digits, scale)
}

fn multiply_decimal_digits(digits: &str, factor: u8) -> String {
    let mut output = Vec::with_capacity(digits.len() + 1);
    let mut carry = 0_u16;
    for digit in digits.bytes().rev() {
        let product = u16::from(digit - b'0') * u16::from(factor) + carry;
        output.push((product % 10) as u8 + b'0');
        carry = product / 10;
    }
    while carry > 0 {
        output.push((carry % 10) as u8 + b'0');
        carry /= 10;
    }
    output.reverse();
    String::from_utf8(output).expect("decimal digit multiplication only produces ASCII")
}

fn normalized_decimal_parts(negative: bool, mut digits: String, mut scale: usize) -> Option<DecimalParts> {
    let first_nonzero = digits.bytes().position(|byte| byte != b'0')?;
    digits.drain(..first_nonzero);
    while scale > 0 && digits.ends_with('0') {
        digits.pop();
        scale -= 1;
    }
    Some(DecimalParts { negative, digits, scale })
}

fn decimal_integer(parts: &DecimalParts) -> Option<i128> {
    if parts.scale != 0 {
        return None;
    }
    format!("{}{}", if parts.negative { "-" } else { "" }, parts.digits).parse().ok()
}

fn hash_decimal_parts<H: Hasher>(parts: &DecimalParts, state: &mut H) {
    parts.negative.hash(state);
    parts.digits.hash(state);
    parts.scale.hash(state);
}

fn hash_numeric_parts<H: Hasher>(parts: &DecimalParts, state: &mut H) {
    if let Some(integer) = decimal_integer(parts) {
        integer.hash(state);
    } else {
        hash_decimal_parts(parts, state);
    }
}

fn compare_sequences(left: &[CypherValue], right: &[CypherValue]) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        match semantic_cmp(left, right) {
            Ordering::Equal => continue,
            ordering => return ordering,
        }
    }
    left.len().cmp(&right.len())
}

fn compare_maps(left: &MapValue, right: &MapValue) -> Ordering {
    for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right) {
        match left_key.cmp(right_key) {
            Ordering::Equal => match semantic_cmp(left_value, right_value) {
                Ordering::Equal => continue,
                ordering => return ordering,
            },
            ordering => return ordering,
        }
    }
    left.len().cmp(&right.len())
}

fn point_eq(left: &PointValue, right: &PointValue) -> bool {
    left.srid == right.srid
        && left.x == right.x
        && left.y == right.y
        && match (left.z, right.z) {
            (Some(left), Some(right)) => left == right,
            (None, None) => true,
            _ => false,
        }
}

fn compare_points(left: &PointValue, right: &PointValue) -> Ordering {
    left.srid
        .cmp(&right.srid)
        .then_with(|| compare_float_values(left.x, right.x))
        .then_with(|| compare_float_values(left.y, right.y))
        .then_with(|| match (left.z, right.z) {
            (Some(left), Some(right)) => compare_float_values(left, right),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        })
}

fn compare_float_values(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or_else(|| left.total_cmp(&right))
}
