//! Value types at run time: whether a value is of a type (GQL §19.6), and
//! CAST (§20.8).
//!
//! The engine types values as it evaluates them, so a cast §20.8 Table 4
//! rules out before running raises `22G03` here (an extension, as for every
//! statically wrong operand). A CAST to an exact numeric type rounds to the
//! nearest, a half away from zero (IA005); to an approximate one, to the
//! nearest, a half to even. Truncating whitespace (IV023) is Unicode's
//! White_Space.

use std::cmp::Ordering;

use hydradb_cypher_ast::GqlStatus;

use super::{is_null, EvaluatedValue};
use crate::{Decimal, EngineError, EngineResult, FloatFormat, GqlType, GqlTypeKind, ScalarValue, TemporalKind};

/// Whether `value` is of `value_type`.
pub(super) fn is_of_type(value: &EvaluatedValue, value_type: &GqlType) -> bool {
    if is_null(value) {
        return value_type.includes_null();
    }
    match &value_type.kind {
        GqlTypeKind::Immaterial => false,
        GqlTypeKind::Boolean => matches!(
            value,
            EvaluatedValue::Truth(Some(_)) | EvaluatedValue::Scalar(ScalarValue::Boolean(_))
        ),
        GqlTypeKind::String { min, max } => match value {
            EvaluatedValue::Scalar(ScalarValue::String(text)) => {
                let length = text.chars().count() as u64;
                length >= *min && max.is_none_or(|max| length <= max)
            }
            _ => false,
        },
        GqlTypeKind::Integer { signed, precision } => match value {
            EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => integer_fits(*integer, *signed, *precision),
            // An exact number of integral value is an integer, whatever its
            // scale.
            EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => {
                decimal.integer().is_some_and(|integer| integer_fits(integer, *signed, *precision))
            }
            _ => false,
        },
        GqlTypeKind::Decimal { precision, scale } => match value {
            EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => {
                Decimal::from_integer(*integer).is_ok_and(|decimal| decimal_fits(decimal, *precision, *scale))
            }
            EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => decimal_fits(*decimal, *precision, *scale),
            _ => false,
        },
        GqlTypeKind::Bytes { min, max } => match value {
            EvaluatedValue::Scalar(ScalarValue::Bytes(bytes)) => {
                let length = bytes.len() as u64;
                length >= *min && max.is_none_or(|max| length <= max)
            }
            _ => false,
        },
        GqlTypeKind::Float(format) => match value {
            EvaluatedValue::Scalar(ScalarValue::Float(text)) => text
                .parse::<f64>()
                .is_ok_and(|float| round_to_format(float, *format) == Some(float) || float.is_nan()),
            _ => false,
        },
        GqlTypeKind::Date
        | GqlTypeKind::LocalTime
        | GqlTypeKind::ZonedTime
        | GqlTypeKind::LocalDatetime
        | GqlTypeKind::ZonedDatetime
        | GqlTypeKind::Duration(_) => match value {
            EvaluatedValue::Temporal(temporal) => temporal_kind_fits(temporal.kind(), &value_type.kind),
            _ => false,
        },
        GqlTypeKind::Node => matches!(value, EvaluatedValue::Vertex(_)),
        GqlTypeKind::Edge => matches!(value, EvaluatedValue::Relationship(_)),
        GqlTypeKind::Path => matches!(value, EvaluatedValue::Path(_)),
        GqlTypeKind::List { element, max } => match value {
            EvaluatedValue::List(items) => {
                max.is_none_or(|max| items.len() as u64 <= max) && items.iter().all(|item| is_of_type(item, element))
            }
            _ => false,
        },
        // §4.15.4: a closed record type's records have exactly its fields.
        GqlTypeKind::Record(fields) => match value {
            EvaluatedValue::Record(record) => fields.as_ref().is_none_or(|fields| {
                record.len() == fields.len()
                    && record
                        .iter()
                        .zip(fields)
                        .all(|((name, value), (field, value_type))| name == field && is_of_type(value, value_type))
            }),
            _ => false,
        },
        GqlTypeKind::Any => true,
        GqlTypeKind::PropertyValue => matches!(
            value,
            EvaluatedValue::Truth(Some(_))
                | EvaluatedValue::Scalar(
                    ScalarValue::Boolean(_) | ScalarValue::Integer(_) | ScalarValue::Float(_) | ScalarValue::String(_)
                )
        ),
        GqlTypeKind::Union(components) => components.iter().any(|component| is_of_type(value, component)),
    }
}

/// Whether an integer is in a binary exact numeric type's range.
fn integer_fits(integer: i128, signed: bool, precision: u32) -> bool {
    if signed {
        precision >= 127 || (-(1_i128 << precision) <= integer && integer < (1_i128 << precision))
    } else {
        integer >= 0 && (precision >= 127 || integer < (1_i128 << precision))
    }
}

/// `float` rounded to the nearest value of `format`, a half to even, or
/// `None` when it is outside the format's finite range.
fn round_to_format(float: f64, format: FloatFormat) -> Option<f64> {
    if !float.is_finite() {
        return Some(float);
    }
    match format {
        FloatFormat::Binary64 | FloatFormat::Binary128 | FloatFormat::Binary256 => Some(float),
        FloatFormat::Binary32 => {
            let rounded = float as f32;
            rounded.is_finite().then_some(f64::from(rounded))
        }
        FloatFormat::Binary16 => round_binary16(float, || Some(Ordering::Equal)),
    }
}

fn invalid_type(message: &str) -> EngineError {
    EngineError::data(GqlStatus::INVALID_VALUE_TYPE, message)
}

fn out_of_range() -> EngineError {
    EngineError::data(
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
        "the value is outside the target type of CAST",
    )
}

fn decimal_result<T>(result: Result<T, crate::decimal::DecimalOverflow>) -> EngineResult<T> {
    super::decimal_result(result, || "the value is outside the target type of CAST".into())
}

fn invalid_character_value() -> EngineError {
    EngineError::data(
        GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST,
        "the string is not a literal of the target type of CAST",
    )
}

/// `CAST(value AS value_type)` (GQL §20.8 General Rules 2 and 4).
pub(super) fn cast(value: EvaluatedValue, value_type: &GqlType, request: Option<&EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    if is_null(&value) {
        return if value_type.includes_null() {
            Ok(EvaluatedValue::Null)
        } else {
            Err(invalid_type("CAST of the null value to a type without it"))
        };
    }
    match &value_type.kind {
        // GR 4 a) and c): a dynamic union or reference type keeps the value.
        GqlTypeKind::Any
        | GqlTypeKind::PropertyValue
        | GqlTypeKind::Union(_)
        | GqlTypeKind::Node
        | GqlTypeKind::Edge
        | GqlTypeKind::Path => {
            if is_of_type(&value, value_type) {
                Ok(value)
            } else if matches!(value_type.kind, GqlTypeKind::Path) && matches!(value, EvaluatedValue::List(_)) {
                Err(EngineError::UnsupportedPlan(
                    hydradb_cypher_ast::QueryFailureReason::Evaluation,
                    "CAST of a list to a path is not supported".to_string(),
                ))
            } else {
                Err(invalid_type("CAST to a type the value is not of"))
            }
        }
        GqlTypeKind::Immaterial => Err(invalid_type("CAST to a type with no material value")),
        // GR 4 k) to p).
        GqlTypeKind::Date
        | GqlTypeKind::LocalTime
        | GqlTypeKind::ZonedTime
        | GqlTypeKind::LocalDatetime
        | GqlTypeKind::ZonedDatetime
        | GqlTypeKind::Duration(_) => cast_to_temporal(value, &value_type.kind, request),
        // GR 4 e): an open record type keeps the record; a closed one takes
        // its fields from it, each cast to its type.
        GqlTypeKind::Record(fields) => match (value, fields) {
            (record @ EvaluatedValue::Record(_), None) => Ok(record),
            (EvaluatedValue::Record(record), Some(fields)) => {
                let mut cast_fields = Vec::new();
                for (name, value_type) in fields {
                    let Some((_, value)) = record.iter().find(|(field, _)| field == name) else {
                        return Err(EngineError::data(
                            GqlStatus::RECORD_FIELDS_DO_NOT_MATCH,
                            format!("the record has no field {name} for the target type of CAST"),
                        ));
                    };
                    cast_fields.push((name.clone(), cast(value.clone(), value_type, request)?));
                }
                Ok(EvaluatedValue::Record(cast_fields))
            }
            _ => Err(invalid_type("CAST of a value that is not a record to a record type")),
        },
        GqlTypeKind::Boolean => cast_to_boolean(value, value_type),
        GqlTypeKind::Integer { signed, precision } => cast_to_integer(value, *signed, *precision),
        GqlTypeKind::Decimal { precision, scale } => cast_to_decimal(value, *precision, *scale),
        GqlTypeKind::Bytes { min, max } => cast_to_bytes(value, *min, *max),
        GqlTypeKind::Float(format) => cast_to_float(value, *format),
        GqlTypeKind::String { min, max } => cast_to_string(value, *min, *max),
        // GR 4 d).
        GqlTypeKind::List { element, max } => match value {
            EvaluatedValue::List(items) => {
                let items = items
                    .into_iter()
                    .map(|item| cast(item, element, request))
                    .collect::<EngineResult<Vec<_>>>()?;
                if max.is_some_and(|max| items.len() as u64 > max) {
                    return Err(EngineError::data(
                        GqlStatus::LIST_DATA_RIGHT_TRUNCATION,
                        "the list is longer than the target type of CAST allows",
                    ));
                }
                Ok(EvaluatedValue::List(items))
            }
            _ => Err(invalid_type("CAST of a value that is not a list to a list type")),
        },
    }
}

/// GR 4 q).
fn cast_to_boolean(value: EvaluatedValue, value_type: &GqlType) -> EngineResult<EvaluatedValue> {
    match value {
        EvaluatedValue::Truth(Some(truth)) | EvaluatedValue::Scalar(ScalarValue::Boolean(truth)) => Ok(EvaluatedValue::Truth(Some(truth))),
        EvaluatedValue::Scalar(ScalarValue::String(text)) => match text.trim().to_ascii_uppercase().as_str() {
            "TRUE" => Ok(EvaluatedValue::Truth(Some(true))),
            "FALSE" => Ok(EvaluatedValue::Truth(Some(false))),
            // UNKNOWN is the null value of the Boolean type.
            "UNKNOWN" if value_type.includes_null() => Ok(EvaluatedValue::Null),
            "UNKNOWN" => Err(invalid_type("CAST of UNKNOWN to a type without the null value")),
            _ => Err(invalid_character_value()),
        },
        _ => Err(invalid_type("CAST to BOOLEAN of a value that is not a boolean or a string")),
    }
}

/// GR 4 g) and h).
fn cast_to_integer(value: EvaluatedValue, signed: bool, precision: u32) -> EngineResult<EvaluatedValue> {
    let integer = match value {
        EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => integer,
        EvaluatedValue::Scalar(ScalarValue::Float(text)) => {
            let float = text.parse::<f64>().map_err(|_| out_of_range())?;
            round_float_to_integer(float)?
        }
        EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => decimal.round(),
        EvaluatedValue::Scalar(ScalarValue::String(text)) => match exact_literal_to_integer(text.trim(), signed) {
            Some(integer) => integer,
            None => match numeric_literal(text.trim(), signed).ok_or_else(invalid_character_value)? {
                Number::Integer(integer) => integer,
                Number::Float(float) => round_float_to_integer(float)?,
            },
        },
        _ => {
            return Err(invalid_type(
                "CAST to an exact numeric type of a value that is not a number or a string",
            ))
        }
    };
    if !integer_fits(integer, signed, precision) {
        return Err(out_of_range());
    }
    // The engine's integers run from the least INT64 to the greatest UINT64
    // (deviations L20): a value a wider type holds but the engine does not
    // is refused, not reported out of the type's range.
    if !(i128::from(i64::MIN) <= integer && integer <= i128::from(u64::MAX)) {
        return Err(EngineError::data(
            GqlStatus::FEATURE_NOT_SUPPORTED,
            "the engine holds integers from -2^63 to 2^64 - 1 only",
        ));
    }
    Ok(EvaluatedValue::Scalar(ScalarValue::Integer(integer)))
}

/// An exact numeric literal with a period, or an exponent and `M`, `9007199254740993.0`,
/// rounded to an integer a half away from zero (IA005) in decimal, never
/// through a binary float. `None` when `text` is no such literal (an
/// approximate one, a radix integer) or its value leaves `i128`.
fn exact_literal_to_integer(text: &str, signed: bool) -> Option<i128> {
    let (negative, body) = match text.as_bytes().first() {
        Some(b'-') if signed => (true, &text[1..]),
        Some(b'+') if signed => (false, &text[1..]),
        _ => (false, text),
    };
    if body.starts_with("0x") || body.starts_with("0o") || body.starts_with("0b") || body.ends_with(['f', 'F', 'd', 'D']) {
        return None;
    }
    // An exponent without `M` makes the literal approximate: its value is
    // the binary64 nearest it, which the float path rounds.
    let exact = body.strip_suffix(['m', 'M']);
    if exact.is_none() && body.contains(['e', 'E']) {
        return None;
    }
    let body = exact.unwrap_or(body);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        // An exponent's underscores sit only between digits, as anywhere.
        Some(index) => {
            let exponent = &body[index + 1..];
            let (sign, digits) = match exponent.as_bytes().first() {
                Some(b'-') => ("-", &exponent[1..]),
                Some(b'+') => ("", &exponent[1..]),
                _ => ("", exponent),
            };
            (
                &body[..index],
                format!("{sign}{}", separated_digits(digits, 10)?).parse::<i64>().ok()?,
            )
        }
        None => (body, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let whole = if whole.is_empty() {
        String::new()
    } else {
        separated_digits(whole, 10)?
    };
    let fraction = if fraction.is_empty() {
        String::new()
    } else {
        separated_digits(fraction, 10)?
    };
    let digits = format!("{whole}{fraction}");
    if digits.is_empty() {
        return None;
    }
    // The value is `digits` times 10^-scale.
    let scale = i64::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    let magnitude = if scale <= 0 {
        let shift = u32::try_from(-scale).ok()?;
        digits.parse::<u128>().ok()?.checked_mul(10_u128.checked_pow(shift)?)?
    } else {
        let kept = digits.len().saturating_sub(usize::try_from(scale).ok()?);
        let integer = if kept == 0 { 0 } else { digits[..kept].parse::<u128>().ok()? };
        // The first digit dropped decides; any digits past it only matter
        // for a tie, and a half rounds away from zero either way.
        let first_dropped = if usize::try_from(scale).ok()? > digits.len() {
            b'0'
        } else {
            digits.as_bytes()[kept]
        };
        if first_dropped >= b'5' {
            integer.checked_add(1)?
        } else {
            integer
        }
    };
    crate::temporal::signed(negative, magnitude)
}

/// IA005: to the nearest integer, a half away from zero.
fn round_float_to_integer(float: f64) -> EngineResult<i128> {
    let rounded = float.round();
    if !rounded.is_finite() || rounded.abs() >= 2_f64.powi(100) {
        return Err(out_of_range());
    }
    Ok(rounded as i128)
}

/// GR 4 i).
fn cast_to_float(value: EvaluatedValue, format: FloatFormat) -> EngineResult<EvaluatedValue> {
    // The exact value is rounded once, to the target format: through
    // binary64 first, a value would round twice.
    let exact = match value {
        EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => Exact::Integer(integer),
        EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => Exact::Decimal(decimal.to_string()),
        EvaluatedValue::Scalar(ScalarValue::Float(text)) => Exact::Float(text.parse::<f64>().map_err(|_| out_of_range())?),
        EvaluatedValue::Scalar(ScalarValue::String(text)) => {
            exact_numeric_literal(text.trim(), true).ok_or_else(invalid_character_value)?
        }
        _ => {
            return Err(invalid_type(
                "CAST to an approximate numeric type of a value that is not a number or a string",
            ))
        }
    };
    let rounded = round_exact(exact, format)
        .filter(|rounded| rounded.is_finite())
        .ok_or_else(out_of_range)?;
    Ok(EvaluatedValue::Scalar(ScalarValue::Float(format!("{rounded:?}").into())))
}

/// `exact` rounded once to the nearest value of `format`, a half to even, or
/// `None` when it is outside the format's finite range.
fn round_exact(exact: Exact, format: FloatFormat) -> Option<f64> {
    let binary32 = |rounded: f32| rounded.is_finite().then_some(f64::from(rounded));
    match (exact, format) {
        (Exact::Float(float), format) => round_to_format(float, format),
        (Exact::Integer(integer), FloatFormat::Binary32) => binary32(integer as f32),
        (Exact::Decimal(literal), FloatFormat::Binary32) => binary32(literal.parse::<f32>().ok()?),
        // An integer binary16 holds is a binary64 one too; any other is past
        // its range either way.
        (Exact::Integer(integer), FloatFormat::Binary16) => round_to_format(integer as f64, FloatFormat::Binary16),
        (Exact::Decimal(literal), FloatFormat::Binary16) => {
            // Binary64 holds every binary16 value and midpoint exactly, so its
            // nearest value rounds as the decimal does, except at a midpoint
            // the decimal is only near: there the decimal breaks the tie.
            let near = literal.parse::<f64>().ok()?;
            round_binary16(near, || compare_decimal_magnitude(&literal, near))
        }
        (Exact::Integer(integer), FloatFormat::Binary64 | FloatFormat::Binary128 | FloatFormat::Binary256) => Some(integer as f64),
        (Exact::Decimal(literal), FloatFormat::Binary64 | FloatFormat::Binary128 | FloatFormat::Binary256) => literal.parse::<f64>().ok(),
    }
}

/// `float` rounded to binary16. At a midpoint, `tie` says whether the exact
/// value's magnitude is above (away from zero), at (to even) or below it.
fn round_binary16(float: f64, tie: impl FnOnce() -> Option<Ordering>) -> Option<f64> {
    if !float.is_finite() || float == 0.0 {
        return Some(float);
    }
    // Ten fraction bits; below 2^-14 the spacing stays 2^-24.
    let exponent = (((float.to_bits() >> 52) & 0x7ff) as i32 - 1023).max(-14);
    let quantum = 2_f64.powi(exponent - 10);
    let scaled = float / quantum;
    let steps = if scaled.fract().abs() == 0.5 {
        match tie()? {
            Ordering::Greater => scaled.round(),
            Ordering::Equal => scaled.round_ties_even(),
            Ordering::Less => scaled.trunc(),
        }
    } else {
        scaled.round()
    };
    let rounded = steps * quantum;
    (rounded.abs() <= 65504.0).then_some(rounded)
}

/// How a decimal literal's magnitude compares with `float`'s, exactly.
fn compare_decimal_magnitude(literal: &str, float: f64) -> Option<Ordering> {
    let (digits, exponent) = decimal_digits(literal)?;
    let (float_digits, float_exponent) = float_decimal_digits(float)?;
    Some(compare_digits((&digits, exponent), (&float_digits, float_exponent)))
}

/// A decimal literal's magnitude as significant digits `d` and `e`, the
/// value `0.d * 10^e`; zero has no digits.
fn decimal_digits(literal: &str) -> Option<(String, i64)> {
    let literal = literal.trim_start_matches(['-', '+']);
    let (mantissa, exponent) = match literal.find(['e', 'E']) {
        Some(index) => (&literal[..index], literal[index + 1..].parse::<i64>().ok()?),
        None => (literal, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all = format!("{whole}{fraction}");
    let leading = all.len() - all.trim_start_matches('0').len();
    let digits = all.trim_matches('0').to_string();
    let whole_digits = i64::try_from(whole.len()).ok()? - i64::try_from(leading).ok()?;
    Some((digits, exponent.checked_add(whole_digits)?))
}

/// A finite binary64 magnitude's exact decimal digits, as [`decimal_digits`]
/// gives them, when its exact expansion fits 128 bits (every binary16
/// midpoint does).
fn float_decimal_digits(float: f64) -> Option<(String, i64)> {
    let bits = float.abs().to_bits();
    let biased = (bits >> 52) as i64;
    let fraction = bits & ((1 << 52) - 1);
    let (mut mantissa, mut exponent) = if biased == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1 << 52), biased - 1075)
    };
    while mantissa != 0 && mantissa % 2 == 0 {
        mantissa /= 2;
        exponent += 1;
    }
    // m * 2^e is m * 5^-e / 10^-e.
    let (digits, scale) = if exponent >= 0 {
        (u128::from(mantissa).checked_shl(u32::try_from(exponent).ok()?)?, 0)
    } else {
        let scale = exponent.unsigned_abs();
        (
            u128::from(mantissa).checked_mul(5_u128.checked_pow(u32::try_from(scale).ok()?)?)?,
            scale,
        )
    };
    decimal_digits(&format!("{digits}e-{scale}"))
}

/// Compares `0.d * 10^e` magnitudes.
fn compare_digits((left, left_exponent): (&str, i64), (right, right_exponent): (&str, i64)) -> Ordering {
    match (left.is_empty(), right.is_empty()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        (false, false) => {}
    }
    left_exponent.cmp(&right_exponent).then_with(|| {
        let width = left.len().max(right.len());
        format!("{left:0<width$}").cmp(&format!("{right:0<width$}"))
    })
}

/// GR 4 j): the value's literal, then fitted to the type's lengths.
fn cast_to_string(value: EvaluatedValue, min: u64, max: Option<u64>) -> EngineResult<EvaluatedValue> {
    let (text, truncates) = match value {
        EvaluatedValue::Scalar(ScalarValue::String(text)) => (text.to_string(), true),
        EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => (integer.to_string(), false),
        // GR 4 j) i): the exact literal at the decimal's scale.
        EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => (decimal.to_string(), false),
        EvaluatedValue::Scalar(ScalarValue::Float(text)) => {
            let float = text.parse::<f64>().map_err(|_| out_of_range())?;
            (approximate_literal(float), false)
        }
        // GR 4 j) iv): the literal's string, without its type word.
        EvaluatedValue::Temporal(temporal) => (temporal.to_literal_string(), false),
        // §20.8 General Rule 4 j) v) 2) C) says "TV is SV", and reads as
        // "TV is Y": the result is a string.
        EvaluatedValue::Truth(Some(truth)) | EvaluatedValue::Scalar(ScalarValue::Boolean(truth)) => {
            ((if truth { "TRUE" } else { "FALSE" }).to_string(), false)
        }
        _ => return Err(invalid_type("CAST to a string type of a value with no literal")),
    };
    let length = text.chars().count() as u64;
    let text = match max {
        // A string is cut to the maximum length; any other value too long
        // for it raises 22001.
        Some(max) if length > max && truncates => text.chars().take(max as usize).collect(),
        Some(max) if length > max => {
            return Err(EngineError::data(
                GqlStatus::STRING_DATA_RIGHT_TRUNCATION,
                "the literal is longer than the target type of CAST allows",
            ))
        }
        _ => text,
    };
    let length = text.chars().count() as u64;
    // `min` is at most `MAXIMUM_STRING_LENGTH`, so the padding is bounded;
    // it is still reserved fallibly rather than aborting the process.
    let text = if length < min {
        let mut padded = text;
        let padding = usize::try_from(min - length).map_err(|_| out_of_range())?;
        padded
            .try_reserve_exact(padding)
            .map_err(|_| EngineError::data(GqlStatus::DATA_EXCEPTION, "the padded string does not fit in memory"))?;
        padded.extend(std::iter::repeat_n(' ', padding));
        padded
    } else {
        text
    };
    Ok(EvaluatedValue::Scalar(ScalarValue::String(text.into())))
}

/// GR 4 j) ii) 1): `0E0`, or the shortest literal whose mantissa is one
/// nonzero digit, a period and digits, such as `1.5E0` and `1.0E2`.
fn approximate_literal(float: f64) -> String {
    if float == 0.0 {
        return "0E0".to_string();
    }
    if !float.is_finite() {
        return if float.is_nan() {
            "NaN".to_string()
        } else if float > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    let shortest = format!("{:e}", float.abs());
    let (mantissa, exponent) = shortest.split_once('e').unwrap_or((shortest.as_str(), "0"));
    let mantissa = if mantissa.contains('.') {
        mantissa.to_string()
    } else {
        format!("{mantissa}.0")
    };
    let sign = if float < 0.0 { "-" } else { "" };
    format!("{sign}{mantissa}E{exponent}")
}

/// A number a numeric literal denotes: an integer, or, for an approximate
/// literal or an exact one with a fraction, its nearest binary64 value.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Number {
    Integer(i128),
    Float(f64),
}

/// `text` read as a <signed numeric literal> (§21.2), or, when `signed` is
/// false, an <unsigned numeric literal>: underscores only between digits,
/// `0x`, `0o` and `0b` integers, an exact number with a period, and an
/// approximate one with an exponent or an `F` or `D` suffix.
fn numeric_literal(text: &str, signed: bool) -> Option<Number> {
    match exact_numeric_literal(text, signed)? {
        Exact::Integer(integer) => Some(Number::Integer(integer)),
        Exact::Decimal(literal) => literal.parse::<f64>().ok().map(Number::Float),
        Exact::Float(float) => Some(Number::Float(float)),
    }
}

/// A number held exactly, so that a conversion rounds it once.
enum Exact {
    Integer(i128),
    /// A decimal literal `str::parse` reads, `-12.5e3`.
    Decimal(String),
    Float(f64),
}

/// A numeric literal's exact value: an integer, or the decimal it writes.
fn exact_numeric_literal(text: &str, signed: bool) -> Option<Exact> {
    let (negative, body) = match text.as_bytes().first() {
        Some(b'-') if signed => (true, &text[1..]),
        Some(b'+') if signed => (false, &text[1..]),
        _ => (false, text),
    };
    Some(match unsigned_numeric_literal(body)? {
        // The sign applies to the magnitude, so the least INT128 is read; a
        // magnitude past INT128 is still the integer it writes, held as its
        // exact decimal text.
        Unsigned::Integer(magnitude) => match crate::temporal::signed(negative, magnitude) {
            Some(integer) => Exact::Integer(integer),
            None => Exact::Decimal(format!("{}{magnitude}.0e0", if negative { "-" } else { "" })),
        },
        Unsigned::Decimal(literal) if negative => Exact::Decimal(format!("-{literal}")),
        Unsigned::Decimal(literal) => Exact::Decimal(literal),
        Unsigned::Float(float) => Exact::Float(if negative { -float } else { float }),
    })
}

/// An unsigned numeric literal: an integer's magnitude, or a decimal literal.
enum Unsigned {
    Integer(u128),
    Decimal(String),
    Float(f64),
}

fn unsigned_numeric_literal(text: &str) -> Option<Unsigned> {
    for (prefix, radix) in [("0x", 16), ("0o", 8), ("0b", 2)] {
        if let Some(digits) = text.strip_prefix(prefix) {
            let digits = separated_digits(digits, radix)?;
            return Some(
                u128::from_str_radix(&digits, radix).map_or_else(|_| Unsigned::Float(beyond_u128(&digits, radix)), Unsigned::Integer),
            );
        }
    }
    // `F` and `D` make a literal approximate, `M` exact; without either an
    // exponent makes it approximate (§21.2: an approximate numeric literal
    // is a mantissa and an exponent).
    let (body, suffix) = match text.as_bytes().last() {
        Some(b'f' | b'F' | b'd' | b'D') => (&text[..text.len() - 1], Some(true)),
        Some(b'm' | b'M') => (&text[..text.len() - 1], Some(false)),
        _ => (text, None),
    };
    let approximate_suffix = suffix == Some(true);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(index) => (&body[..index], Some(&body[index + 1..])),
        None => (body, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (mantissa, None),
    };
    let whole = if whole.is_empty() && fraction.is_some_and(|fraction| !fraction.is_empty()) {
        String::new()
    } else {
        separated_digits(whole, 10)?
    };
    let fraction = match fraction {
        Some("") => Some(String::new()),
        Some(fraction) => Some(separated_digits(fraction, 10)?),
        None => None,
    };
    let exponent = match exponent {
        Some(exponent) => {
            let (sign, digits) = match exponent.as_bytes().first() {
                Some(b'-') => ("-", &exponent[1..]),
                Some(b'+') => ("", &exponent[1..]),
                _ => ("", exponent),
            };
            Some(format!("{sign}{}", separated_digits(digits, 10)?))
        }
        None => None,
    };
    if exponent.is_none() && fraction.is_none() && !approximate_suffix {
        // Past an integer's range the literal is still a number, too large
        // for an integer type (22003) but not for a float.
        return Some(
            whole
                .parse::<u128>()
                .map_or_else(|_| Unsigned::Decimal(format!("{whole}.0e0")), Unsigned::Integer),
        );
    }
    let literal = format!(
        "{}.{}e{}",
        if whole.is_empty() { "0" } else { &whole },
        fraction.as_deref().filter(|fraction| !fraction.is_empty()).unwrap_or("0"),
        exponent.as_deref().unwrap_or("0")
    );
    // An approximate literal's value is the binary64 nearest it, rounded
    // here once; an exact one keeps its digits.
    Some(if suffix.unwrap_or(exponent.is_some()) {
        Unsigned::Float(literal.parse::<f64>().ok()?)
    } else {
        Unsigned::Decimal(literal)
    })
}

/// A radix 2, 8 or 16 integer literal past `u128`, rounded once to binary64:
/// its top 127 bits with a sticky bit for the rest round as the whole does.
/// Any such value is past every narrower format's range.
fn beyond_u128(digits: &str, radix: u32) -> f64 {
    let width = radix.trailing_zeros();
    let bits: Vec<bool> = digits
        .chars()
        .filter_map(|digit| digit.to_digit(radix))
        .flat_map(|digit| (0..width).rev().map(move |bit| digit >> bit & 1 == 1))
        .skip_while(|bit| !bit)
        .collect();
    let top = bits.iter().take(127).fold(0_u128, |top, bit| top << 1 | u128::from(*bit));
    let sticky = bits.iter().skip(127).any(|bit| *bit);
    let shift = i32::try_from(bits.len().saturating_sub(127)).unwrap_or(i32::MAX);
    ((top << 1 | u128::from(sticky)) as f64) * 2_f64.powi(shift.saturating_sub(1))
}

/// Digits of `radix` with single underscores between them, the underscores
/// removed; `None` for anything else.
fn separated_digits(text: &str, radix: u32) -> Option<String> {
    hydradb_cypher_ast::numeric::separated_digits(text, radix)
}

/// Whether a temporal value of `kind` is of the temporal type `target`.
fn temporal_kind_fits(kind: TemporalKind, target: &GqlTypeKind) -> bool {
    matches!(
        (kind, target),
        (TemporalKind::Date, GqlTypeKind::Date)
            | (TemporalKind::LocalTime, GqlTypeKind::LocalTime)
            | (TemporalKind::ZonedTime, GqlTypeKind::ZonedTime)
            | (TemporalKind::LocalDatetime, GqlTypeKind::LocalDatetime)
            | (TemporalKind::ZonedDatetime, GqlTypeKind::ZonedDatetime)
            | (TemporalKind::YearMonthDuration, GqlTypeKind::Duration(Some(true) | None))
            | (TemporalKind::DayTimeDuration, GqlTypeKind::Duration(Some(false) | None))
    )
}

/// GR 4 k) to p): a string read as the target's literal, or another
/// temporal value converted. A zoned-to-local datetime adds the source
/// displacement (§20.8 GR4 n)); a time attaches the current date after wrapping.
fn cast_to_temporal(value: EvaluatedValue, target: &GqlTypeKind, request: Option<&EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    use crate::temporal::{add_duration, parse_date, parse_datetime, parse_duration, parse_time, DurationValue, TemporalValue};
    let format_error = || {
        if matches!(target, GqlTypeKind::Duration(_)) {
            EngineError::data(GqlStatus::INVALID_DURATION_FORMAT, "the string is no duration of the target type")
        } else {
            EngineError::data(GqlStatus::INVALID_DATETIME_FORMAT, "the string is no instant of the target type")
        }
    };
    let offset = super::time::CURRENT_OFFSET;
    let displacement = |offset: i32| DurationValue::DayTime {
        nanoseconds: i128::from(offset) * 1_000_000_000,
    };
    let today = || -> EngineResult<chrono::NaiveDate> {
        match request {
            Some(EvaluatedValue::Scalar(ScalarValue::Integer(nanos))) => crate::temporal::request_datetime(*nanos, offset)
                .map(|now| now.date())
                .ok_or_else(|| EngineError::data(GqlStatus::DATETIME_FIELD_OVERFLOW, "the request timestamp is out of range")),
            _ => Err(EngineError::data(GqlStatus::DATA_EXCEPTION, "the request timestamp is not set")),
        }
    };
    let converted = match value {
        EvaluatedValue::Scalar(ScalarValue::String(text)) => {
            let text = text.trim();
            match target {
                GqlTypeKind::Date => parse_date(text).map(TemporalValue::Date),
                GqlTypeKind::LocalTime => match parse_time(text) {
                    Some((time, None)) => Some(TemporalValue::LocalTime(time)),
                    _ => None,
                },
                GqlTypeKind::ZonedTime => match parse_time(text) {
                    Some((time, Some(offset))) => Some(TemporalValue::ZonedTime { time, offset }),
                    _ => None,
                },
                GqlTypeKind::LocalDatetime => match parse_datetime(text) {
                    Some((datetime, None)) => Some(TemporalValue::LocalDatetime(datetime)),
                    _ => None,
                },
                GqlTypeKind::ZonedDatetime => match parse_datetime(text) {
                    Some((datetime, Some(offset))) => Some(TemporalValue::ZonedDatetime { datetime, offset }),
                    _ => None,
                },
                _ => parse_duration(text).ok().map(TemporalValue::Duration),
            }
            .ok_or_else(format_error)?
        }
        EvaluatedValue::Temporal(source) => {
            let midnight = chrono::NaiveTime::MIN;
            match (target, source) {
                (GqlTypeKind::Duration(_), duration @ TemporalValue::Duration(_)) => duration,
                (GqlTypeKind::Date, TemporalValue::Date(date)) => TemporalValue::Date(date),
                (GqlTypeKind::Date, TemporalValue::LocalDatetime(datetime) | TemporalValue::ZonedDatetime { datetime, .. }) => {
                    TemporalValue::Date(datetime.date())
                }
                (GqlTypeKind::LocalTime, TemporalValue::LocalTime(time) | TemporalValue::ZonedTime { time, .. }) => {
                    TemporalValue::LocalTime(time)
                }
                (GqlTypeKind::LocalTime, TemporalValue::LocalDatetime(datetime) | TemporalValue::ZonedDatetime { datetime, .. }) => {
                    TemporalValue::LocalTime(datetime.time())
                }
                (GqlTypeKind::ZonedTime, zoned @ TemporalValue::ZonedTime { .. }) => zoned,
                (GqlTypeKind::ZonedTime, TemporalValue::LocalTime(time)) => TemporalValue::ZonedTime { time, offset },
                (GqlTypeKind::ZonedTime, TemporalValue::ZonedDatetime { datetime, offset }) => TemporalValue::ZonedTime {
                    time: datetime.time(),
                    offset,
                },
                (GqlTypeKind::ZonedTime, TemporalValue::LocalDatetime(datetime)) => TemporalValue::ZonedTime {
                    time: datetime.time(),
                    offset,
                },
                (GqlTypeKind::LocalDatetime, TemporalValue::LocalDatetime(datetime)) => TemporalValue::LocalDatetime(datetime),
                (GqlTypeKind::LocalDatetime, TemporalValue::ZonedDatetime { datetime, offset }) => {
                    add_duration(TemporalValue::LocalDatetime(datetime), displacement(offset)).map_err(super::time::temporal_error)?
                }
                (GqlTypeKind::LocalDatetime, TemporalValue::Date(date)) => TemporalValue::LocalDatetime(date.and_time(midnight)),
                (GqlTypeKind::LocalDatetime, TemporalValue::LocalTime(time)) => TemporalValue::LocalDatetime(today()?.and_time(time)),
                (GqlTypeKind::LocalDatetime, TemporalValue::ZonedTime { time, offset }) => {
                    let TemporalValue::LocalTime(time) =
                        add_duration(TemporalValue::LocalTime(time), displacement(offset)).map_err(super::time::temporal_error)?
                    else {
                        unreachable!()
                    };
                    TemporalValue::LocalDatetime(today()?.and_time(time))
                }
                (GqlTypeKind::ZonedDatetime, zoned @ TemporalValue::ZonedDatetime { .. }) => zoned,
                (GqlTypeKind::ZonedDatetime, TemporalValue::LocalDatetime(datetime)) => TemporalValue::ZonedDatetime { datetime, offset },
                (GqlTypeKind::ZonedDatetime, TemporalValue::Date(date)) => TemporalValue::ZonedDatetime {
                    datetime: date.and_time(midnight),
                    offset,
                },
                (GqlTypeKind::ZonedDatetime, TemporalValue::LocalTime(time)) => TemporalValue::ZonedDatetime {
                    datetime: today()?.and_time(time),
                    offset,
                },
                (GqlTypeKind::ZonedDatetime, TemporalValue::ZonedTime { time, offset }) => TemporalValue::ZonedDatetime {
                    datetime: today()?.and_time(time),
                    offset,
                },
                _ => return Err(invalid_type("CAST between these temporal types is not valid")),
            }
        }
        _ => {
            return Err(invalid_type(
                "CAST to a temporal type of a value that is not a string or a temporal value",
            ))
        }
    };
    if temporal_kind_fits(converted.kind(), target) {
        Ok(EvaluatedValue::Temporal(converted))
    } else if matches!(target, GqlTypeKind::Duration(_)) {
        Err(format_error())
    } else {
        Err(invalid_type("CAST to a temporal type of a value of another"))
    }
}

/// Whether an exact number has at most `precision` digits, `scale` of them
/// after the point, losing nothing.
fn decimal_fits(decimal: Decimal, precision: u32, scale: u32) -> bool {
    decimal
        .rescale(scale)
        .is_ok_and(|rescaled| rescaled == decimal && rescaled.precision() <= precision)
}

/// GR 4 g): to a decimal type, rounded to its scale, a half away from zero
/// (IA005); more digits than its precision raise `22003`.
fn cast_to_decimal(value: EvaluatedValue, precision: u32, scale: u32) -> EngineResult<EvaluatedValue> {
    let exact = match value {
        EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => Decimal::from_integer(integer).map_err(|_| out_of_range())?,
        EvaluatedValue::Scalar(ScalarValue::Decimal(decimal)) => decimal,
        // The float's own value, rounded once, at the target's scale.
        EvaluatedValue::Scalar(ScalarValue::Float(text)) => {
            let float = text.parse::<f64>().map_err(|_| out_of_range())?;
            decimal_result(Decimal::from_f64_at_scale(float, scale))?
        }
        EvaluatedValue::Scalar(ScalarValue::String(text)) => {
            let text = text.trim();
            match exact_numeric_literal(text, true).ok_or_else(invalid_character_value)? {
                Exact::Integer(integer) => decimal_result(Decimal::from_integer(integer))?,
                // An approximate literal (`2F`, `0.1D`, `0.1E0`) denotes its
                // binary64 value, which is cast as a float is; an exact one
                // is read in decimal, digit for digit.
                Exact::Float(float) => decimal_result(Decimal::from_f64_at_scale(float, scale))?,
                Exact::Decimal(_) => crate::decimal::parse_at_scale(text, scale).map_err(|error| match error {
                    crate::decimal::DecimalParseError::Malformed => invalid_character_value(),
                    crate::decimal::DecimalParseError::Overflow => out_of_range(),
                })?,
            }
        }
        _ => return Err(invalid_type("CAST to DECIMAL of a value that is not a number or a string")),
    };
    let rounded = decimal_result(exact.rescale(scale))?;
    if rounded.precision() > precision {
        return Err(out_of_range());
    }
    Ok(EvaluatedValue::Scalar(ScalarValue::Decimal(rounded)))
}

/// GR 4 r): a byte string cut to the maximum length, or padded with `X'00'`
/// to the minimum.
fn cast_to_bytes(value: EvaluatedValue, min: u64, max: Option<u64>) -> EngineResult<EvaluatedValue> {
    let EvaluatedValue::Scalar(ScalarValue::Bytes(bytes)) = value else {
        return Err(invalid_type("CAST to a byte string type of a value that is not a byte string"));
    };
    let mut bytes = bytes.into_vec();
    if let Some(max) = max {
        bytes.truncate(usize::try_from(max).unwrap_or(usize::MAX));
    }
    // `min` is at most `MAXIMUM_BYTES_LENGTH`; the padding is still
    // reserved fallibly rather than aborting the process.
    let min = usize::try_from(min).map_err(|_| out_of_range())?;
    if bytes.len() < min {
        bytes
            .try_reserve_exact(min - bytes.len())
            .map_err(|_| EngineError::data(GqlStatus::DATA_EXCEPTION, "the padded byte string does not fit in memory"))?;
        bytes.resize(min, 0);
    }
    Ok(EvaluatedValue::Scalar(ScalarValue::Bytes(bytes.into())))
}
