//! Numeric and character-string semantics: arithmetic (ISO GQL §20.21), the
//! numeric value functions (§20.22), concatenation (§20.23) and the
//! character string functions (§20.24).
//!
//! Each exception is the condition the standard names for it, with
//! Technical Corrigendum 1 applied: `22003` for a result outside its type,
//! `22012` for a zero divisor, `22011` for a negative substring length,
//! `22027` for a trim character that is not one character, `2201E`, `2201F`,
//! `2201Z` and `2202M` for arguments outside a function's domain, and
//! `22G03` for an operand of the wrong type.
//!
//! INTEGER is 64-bit and FLOAT is IEEE 754 binary64. Feature GA01 is not
//! claimed, so an operation never produces an infinity or a NaN: a result
//! outside the finite range raises `22003` instead.

use std::cmp::Ordering;

use hydradb_cypher_ast::GqlStatus;
use unicode_normalization::UnicodeNormalization;

use super::{evaluate_equal, is_null, EvaluatedValue};
use crate::{Decimal, EngineError, EngineResult, PhysicalBinaryOperator, PhysicalUnaryOperator, ScalarFunction, ScalarValue};

/// A numeric operand, read once.
#[derive(Clone, Copy)]
enum Number {
    Integer(i128),
    Float(f64),
    /// An exact decimal (GV17).
    Decimal(Decimal),
}

impl Number {
    fn exact_integer(self) -> Option<i128> {
        match self {
            Self::Integer(value) => Some(value),
            Self::Decimal(value) => value.integer(),
            Self::Float(_) => None,
        }
    }

    fn whole_integer(self) -> Option<i128> {
        self.exact_integer().or_else(|| match self {
            // Saturation preserves the ordering of counts beyond every
            // possible string length; substring clamps them to that length.
            Self::Float(value) if value.fract() == 0.0 && value.is_finite() => Some(value as i128),
            _ => None,
        })
    }

    fn to_float(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Float(value) => value,
            Self::Decimal(value) => value.to_f64(),
        }
    }

    /// The exact value, `None` for a float.
    fn as_decimal(self) -> Option<EngineResult<Decimal>> {
        match self {
            Self::Integer(value) => Some(super::decimal_result(Decimal::from_integer(value), || {
                format!("a decimal result has more than {} digits", crate::decimal::MAXIMUM_DIGITS)
            })),
            Self::Decimal(value) => Some(Ok(value)),
            Self::Float(_) => None,
        }
    }

    /// How the value orders against 1, exactly; `None` for NaN.
    fn cmp_one(self) -> Option<Ordering> {
        match self {
            Self::Integer(value) => Some(value.cmp(&1)),
            Self::Decimal(value) => Some(value.cmp_integer(1)),
            Self::Float(value) => value.partial_cmp(&1.0),
        }
    }

    /// How the value's magnitude orders against 1, exactly; `None` for NaN.
    fn magnitude_cmp_one(self) -> Option<Ordering> {
        match self {
            Self::Integer(value) => Some(value.unsigned_abs().cmp(&1)),
            Self::Decimal(value) => Some(value.abs().cmp_integer(1)),
            Self::Float(value) => value.abs().partial_cmp(&1.0),
        }
    }

    /// `self - 1` for an exact number within a half of 1, computed exactly:
    /// where a logarithm must start from the offset, which converting the
    /// value to a float first would round away (1.00000000000000000001 is
    /// 1.0 as a float). A float, or an integer, is its own nearest float.
    fn offset_from_one(self) -> Option<f64> {
        self.offset_from(1)
    }

    /// `self - target` for an exact number within a half of `target`,
    /// computed exactly, as [`Self::offset_from_one`] is near 1.
    fn offset_from(self, target: i128) -> Option<f64> {
        let Self::Decimal(value) = self else { return None };
        let offset = value.checked_sub(Decimal::from_integer(target).ok()?).ok()?;
        (offset.abs().cmp_f64(0.5) == Some(Ordering::Less)).then(|| offset.to_f64())
    }

    /// [`Self::offset_from_one`] for a decimal no binary64 holds, whose
    /// distance from 1 its nearest float would round away.
    fn inexact_offset_from_one(self) -> Option<f64> {
        self.inexact_offset_from(1)
    }

    /// [`Self::offset_from`] for a decimal no binary64 holds.
    fn inexact_offset_from(self, target: i128) -> Option<f64> {
        let Self::Decimal(value) = self else { return None };
        if Decimal::from_f64_exact(value.to_f64()).is_some_and(|exact| exact == value) {
            return None;
        }
        self.offset_from(target)
    }

    /// The arc cosine of a value in [-1, 1]. Near ±1 a decimal keeps its
    /// distance `d` from the boundary, which its nearest float would round
    /// away (`acos(0.99999999999999999999M)` is about 1.4e-10, not 0):
    /// `acos(1 - d)` is `2 asin(sqrt(d / 2))`, `acos(-1 + d)` its complement
    /// to pi.
    fn acos(self) -> f64 {
        if let Some(offset) = self.inexact_offset_from(1) {
            return 2.0 * (-offset / 2.0).sqrt().asin();
        }
        if let Some(offset) = self.inexact_offset_from(-1) {
            return std::f64::consts::PI - 2.0 * (offset / 2.0).sqrt().asin();
        }
        self.to_float().acos()
    }

    /// The arc sine of a value in [-1, 1], from the distance to ±1 near
    /// them as [`Self::acos`] is.
    fn asin(self) -> f64 {
        if let Some(offset) = self.inexact_offset_from(1) {
            return std::f64::consts::FRAC_PI_2 - 2.0 * (-offset / 2.0).sqrt().asin();
        }
        if let Some(offset) = self.inexact_offset_from(-1) {
            return 2.0 * (offset / 2.0).sqrt().asin() - std::f64::consts::FRAC_PI_2;
        }
        self.to_float().asin()
    }

    /// The natural logarithm of a positive value: of its offset from 1 near
    /// 1, of its nearest float elsewhere, where that float is as precise as
    /// the result (`ln(1e-20M)` is about -46.05).
    fn ln(self) -> f64 {
        self.offset_from_one().map_or_else(|| self.to_float().ln(), f64::ln_1p)
    }

    /// The base-10 logarithm of a positive value. A decimal splits into its
    /// digits and its scale, so a power of ten is exact: `log10(0.01M)` is
    /// `log10(1) - 2`, -2.
    fn log10(self) -> f64 {
        if let Some(offset) = self.offset_from_one() {
            return offset.ln_1p() / std::f64::consts::LN_10;
        }
        match self {
            Self::Decimal(value) => (value.mantissa as f64).log10() - f64::from(value.scale),
            other => other.to_float().log10(),
        }
    }

    /// The base-2 logarithm of a positive value.
    fn log2(self) -> f64 {
        self.offset_from_one()
            .map_or_else(|| self.to_float().log2(), |offset| offset.ln_1p() / std::f64::consts::LN_2)
    }

    /// Whether the value is exactly `integer`.
    fn is(self, integer: i128) -> bool {
        match self {
            Self::Integer(value) => value == integer,
            Self::Decimal(value) => value.cmp_integer(integer) == Ordering::Equal,
            Self::Float(value) => value == integer as f64,
        }
    }
}

/// An exact decimal result.
fn decimal(value: Result<Decimal, crate::decimal::DecimalOverflow>) -> EngineResult<EvaluatedValue> {
    super::decimal_result(value, || {
        format!("a decimal result has more than {} digits", crate::decimal::MAXIMUM_DIGITS)
    })
    .map(|value| EvaluatedValue::Scalar(ScalarValue::Decimal(value)))
}

fn type_error(message: impl Into<String>) -> EngineError {
    EngineError::data(GqlStatus::INVALID_VALUE_TYPE, message)
}

fn out_of_range(message: impl Into<String>) -> EngineError {
    EngineError::data(GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE, message)
}

/// The number a value holds, `None` for null, or `22G03` for anything else.
fn number(value: &EvaluatedValue, role: &str) -> EngineResult<Option<Number>> {
    match value {
        value if is_null(value) => Ok(None),
        EvaluatedValue::Scalar(ScalarValue::Integer(value)) => Ok(Some(Number::Integer(*value))),
        EvaluatedValue::Scalar(ScalarValue::Decimal(value)) => Ok(Some(Number::Decimal(*value))),
        EvaluatedValue::Scalar(ScalarValue::Float(spelling)) => spelling
            .parse::<f64>()
            .map(|value| Some(Number::Float(value)))
            .map_err(|_| type_error(format!("{role} is not a valid float"))),
        _ => Err(type_error(format!("{role} must be a number"))),
    }
}

/// An INTEGER result, which must fit 64 bits.
fn integer(value: i128) -> EngineResult<EvaluatedValue> {
    if i64::try_from(value).is_err() {
        return Err(out_of_range("integer result is outside the 64-bit INTEGER range"));
    }
    Ok(EvaluatedValue::Scalar(ScalarValue::Integer(value)))
}

/// A FLOAT result, which must be finite.
fn float(value: f64) -> EngineResult<EvaluatedValue> {
    if !value.is_finite() {
        return Err(out_of_range("float result is outside the FLOAT range"));
    }
    Ok(EvaluatedValue::Scalar(ScalarValue::Float(format!("{value:?}").into())))
}

fn string_value(value: String) -> EvaluatedValue {
    EvaluatedValue::Scalar(ScalarValue::String(value.into()))
}

/// Whether any operand is null. An operator or function then answers null
/// before it reads any operand's type (§20.21 General Rule 1, §20.22 and
/// §20.24 alike), so `'x' + null` is null, as a missing property makes it.
fn any_null<'a>(operands: impl IntoIterator<Item = &'a EvaluatedValue>) -> bool {
    operands.into_iter().any(is_null)
}

/// The string a value holds, `None` for null, or `22G03` for anything else.
fn string<'a>(value: &'a EvaluatedValue, function: &str) -> EngineResult<Option<&'a str>> {
    match value {
        value if is_null(value) => Ok(None),
        EvaluatedValue::Scalar(ScalarValue::String(value)) => Ok(Some(value)),
        _ => Err(type_error(format!("{function} requires a character string"))),
    }
}

/// `+ - * / %` (§20.21). Two integers give an integer; a float operand makes
/// the result a float. Integer division truncates toward zero, and `%` keeps
/// the sign of the dividend.
pub(super) fn evaluate_arithmetic(
    operator: PhysicalBinaryOperator,
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    if any_null([&left, &right]) {
        return Ok(EvaluatedValue::Null);
    }
    let (Some(left), Some(right)) = (number(&left, "an arithmetic operand")?, number(&right, "an arithmetic operand")?) else {
        return Ok(EvaluatedValue::Null);
    };
    if let (Number::Integer(left), Number::Integer(right)) = (left, right) {
        let value = match operator {
            PhysicalBinaryOperator::Add => left.checked_add(right),
            PhysicalBinaryOperator::Subtract => left.checked_sub(right),
            PhysicalBinaryOperator::Multiply => left.checked_mul(right),
            PhysicalBinaryOperator::Divide | PhysicalBinaryOperator::Modulo if right == 0 => {
                return Err(EngineError::data(GqlStatus::DIVISION_BY_ZERO, "arithmetic division by zero"));
            }
            PhysicalBinaryOperator::Divide => left.checked_div(right),
            PhysicalBinaryOperator::Modulo => left.checked_rem(right),
            _ => return Err(EngineError::data(GqlStatus::DATA_EXCEPTION, "operator is not arithmetic")),
        }
        .ok_or_else(|| out_of_range("integer arithmetic overflowed"))?;
        return integer(value);
    }
    // §20.21 Syntax Rule 2 b): exact operands give an exact result; a float
    // operand makes it a float.
    if let (Some(left), Some(right)) = (left.as_decimal(), right.as_decimal()) {
        let (left, right) = (left?, right?);
        let division_by_zero = || EngineError::data(GqlStatus::DIVISION_BY_ZERO, "arithmetic division by zero");
        return decimal(match operator {
            PhysicalBinaryOperator::Add => left.checked_add(right),
            PhysicalBinaryOperator::Subtract => left.checked_sub(right),
            PhysicalBinaryOperator::Multiply => left.checked_mul(right),
            PhysicalBinaryOperator::Divide => left.checked_div(right).ok_or_else(division_by_zero)?,
            PhysicalBinaryOperator::Modulo => left.checked_rem(right).ok_or_else(division_by_zero)?,
            _ => return Err(EngineError::data(GqlStatus::DATA_EXCEPTION, "operator is not arithmetic")),
        });
    }
    let (left, right) = (left.to_float(), right.to_float());
    let value = match operator {
        PhysicalBinaryOperator::Add => left + right,
        PhysicalBinaryOperator::Subtract => left - right,
        PhysicalBinaryOperator::Multiply => left * right,
        PhysicalBinaryOperator::Divide | PhysicalBinaryOperator::Modulo if right == 0.0 => {
            return Err(EngineError::data(GqlStatus::DIVISION_BY_ZERO, "arithmetic division by zero"));
        }
        PhysicalBinaryOperator::Divide => left / right,
        PhysicalBinaryOperator::Modulo => left % right,
        _ => return Err(EngineError::data(GqlStatus::DATA_EXCEPTION, "operator is not arithmetic")),
    };
    float(value)
}

/// Unary `+` and `-` on a number (§20.21).
pub(super) fn evaluate_sign(operator: PhysicalUnaryOperator, value: EvaluatedValue) -> EngineResult<EvaluatedValue> {
    let Some(value) = number(&value, "the operand of a sign")? else {
        return Ok(EvaluatedValue::Null);
    };
    match (operator, value) {
        (PhysicalUnaryOperator::Minus, Number::Integer(value)) => {
            integer(value.checked_neg().ok_or_else(|| out_of_range("integer unary minus overflowed"))?)
        }
        (PhysicalUnaryOperator::Minus, Number::Float(value)) => float(-value),
        (PhysicalUnaryOperator::Minus, Number::Decimal(value)) => decimal(Ok(value.negate())),
        (_, Number::Integer(value)) => integer(value),
        (_, Number::Float(value)) => float(value),
        (_, Number::Decimal(value)) => decimal(Ok(value)),
    }
}

/// `a || b` on character strings (§20.23). Two NFC inputs give an NFC
/// result; otherwise the result is the plain concatenation (IW017).
pub(super) fn evaluate_concatenate(left: EvaluatedValue, right: EvaluatedValue) -> EngineResult<EvaluatedValue> {
    if any_null([&left, &right]) {
        return Ok(EvaluatedValue::Null);
    }
    // §20.13 General Rule 1: paths join where the first ends and the second
    // starts. IL015 bounds a path only by memory, so 22G10 never arises.
    if let (EvaluatedValue::Path(_), _) | (_, EvaluatedValue::Path(_)) = (&left, &right) {
        return match (left, right) {
            (left, right) if is_null(&left) || is_null(&right) => Ok(EvaluatedValue::Null),
            (EvaluatedValue::Path(mut left), EvaluatedValue::Path(right)) => {
                if left.nodes.last().map(|node| node.id) != right.nodes.first().map(|node| node.id) {
                    return Err(EngineError::data(
                        GqlStatus::MALFORMED_PATH,
                        "|| joins two paths only where the first ends and the second starts",
                    ));
                }
                let right = *right;
                left.nodes.extend(right.nodes.into_iter().skip(1));
                left.relationships.extend(right.relationships);
                Ok(EvaluatedValue::Path(left))
            }
            _ => Err(type_error("|| joins two paths, two character strings or two byte strings")),
        };
    }
    // §20.23 General Rule 3: byte strings join byte by byte.
    match (&left, &right) {
        (EvaluatedValue::Scalar(ScalarValue::Bytes(left)), EvaluatedValue::Scalar(ScalarValue::Bytes(right))) => {
            return Ok(EvaluatedValue::Scalar(ScalarValue::Bytes(
                [left.as_ref(), right.as_ref()].concat().into(),
            )));
        }
        (EvaluatedValue::Scalar(ScalarValue::Bytes(_)), _) | (_, EvaluatedValue::Scalar(ScalarValue::Bytes(_))) => {
            return Err(type_error("|| joins two character strings or two byte strings"));
        }
        _ => {}
    }
    let (Some(left), Some(right)) = (string(&left, "||")?, string(&right, "||")?) else {
        return Ok(EvaluatedValue::Null);
    };
    let joined = format!("{left}{right}");
    let normalized = unicode_normalization::is_nfc(left) && unicode_normalization::is_nfc(right);
    Ok(string_value(if normalized { joined.nfc().collect() } else { joined }))
}

/// The functions of §20.7 (`NULLIF`), §20.22 and §20.24.
pub(super) fn evaluate_gql_function(function: ScalarFunction, arguments: Vec<EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    let argument = |index: usize| arguments.get(index).cloned().unwrap_or(EvaluatedValue::Null);
    match function {
        // `CASE WHEN a = b THEN NULL ELSE a END`.
        ScalarFunction::NullIf => {
            let first = argument(0);
            Ok(match evaluate_equal(first.clone(), argument(1)) {
                Some(true) => EvaluatedValue::Null,
                _ => first,
            })
        }
        // §20.22 General Rule 3: the bytes in a byte string.
        ScalarFunction::ByteLength => Ok(match argument(0) {
            value if is_null(&value) => EvaluatedValue::Null,
            EvaluatedValue::Scalar(ScalarValue::Bytes(bytes)) => EvaluatedValue::Scalar(ScalarValue::Integer(bytes.len() as i128)),
            _ => return Err(type_error("BYTE_LENGTH requires a byte string")),
        }),
        ScalarFunction::CharLength => {
            let value = argument(0);
            Ok(match string(&value, function.name())? {
                None => EvaluatedValue::Null,
                Some(value) => EvaluatedValue::Scalar(ScalarValue::Integer(value.chars().count() as i128)),
            })
        }
        ScalarFunction::Upper | ScalarFunction::Lower => {
            let value = argument(0);
            let Some(value) = string(&value, function.name())? else {
                return Ok(EvaluatedValue::Null);
            };
            let folded = if function == ScalarFunction::Upper {
                value.to_uppercase()
            } else {
                value.to_lowercase()
            };
            // A fold of a normalized string is normalized again (§20.24 GR 3d).
            Ok(string_value(if unicode_normalization::is_nfc(value) {
                folded.nfc().collect()
            } else {
                folded
            }))
        }
        ScalarFunction::Left | ScalarFunction::Right => substring(function, &argument(0), &argument(1)),
        ScalarFunction::TrimBoth | ScalarFunction::TrimLeading | ScalarFunction::TrimTrailing => {
            single_character_trim(function, &argument(0), arguments.get(1))
        }
        ScalarFunction::LTrim | ScalarFunction::RTrim | ScalarFunction::BTrim => {
            multi_character_trim(function, &argument(0), arguments.get(1))
        }
        ScalarFunction::Normalize => normalize(&argument(0), arguments.get(1)),
        ScalarFunction::IsTrue | ScalarFunction::IsFalse | ScalarFunction::IsUnknown => {
            let truth = match argument(0) {
                EvaluatedValue::Truth(value) => value,
                EvaluatedValue::Scalar(ScalarValue::Boolean(value)) => Some(value),
                value if is_null(&value) => None,
                _ => return Err(type_error("a boolean test needs a boolean")),
            };
            Ok(EvaluatedValue::Truth(Some(match function {
                ScalarFunction::IsTrue => truth == Some(true),
                ScalarFunction::IsFalse => truth == Some(false),
                _ => truth.is_none(),
            })))
        }
        // §19.7: unknown for a null string.
        ScalarFunction::IsNormalized => {
            let value = argument(0);
            let Some(source) = string(&value, "IS NORMALIZED")? else {
                return Ok(EvaluatedValue::Truth(None));
            };
            let form = match argument(1) {
                EvaluatedValue::Scalar(ScalarValue::String(form)) => form,
                _ => "NFC".into(),
            };
            Ok(EvaluatedValue::Truth(Some(is_normalized(source, &form)?)))
        }
        _ => numeric_function(function, &arguments),
    }
}

/// `LEFT(s, n)` and `RIGHT(s, n)` (§20.24 General Rule 1).
fn substring(function: ScalarFunction, source: &EvaluatedValue, length: &EvaluatedValue) -> EngineResult<EvaluatedValue> {
    if any_null([source, length]) {
        return Ok(EvaluatedValue::Null);
    }
    // §20.25 General Rule 1: of a byte string, its first or last bytes.
    if let EvaluatedValue::Scalar(ScalarValue::Bytes(bytes)) = source {
        let Some(length) = number(length, "a byte string length")? else {
            return Ok(EvaluatedValue::Null);
        };
        let length = length
            .whole_integer()
            .ok_or_else(|| type_error("a byte string length must be a whole number"))?;
        if length < 0 {
            return Err(EngineError::data(
                GqlStatus::SUBSTRING_ERROR,
                "a byte string length cannot be negative",
            ));
        }
        let taken = usize::try_from(length).unwrap_or(usize::MAX).min(bytes.len());
        let part = if function == ScalarFunction::Left {
            &bytes[..taken]
        } else {
            &bytes[bytes.len() - taken..]
        };
        return Ok(EvaluatedValue::Scalar(ScalarValue::Bytes(part.into())));
    }
    let source = string(source, function.name())?;
    let length = number(length, "a string length")?;
    let (Some(source), Some(length)) = (source, length) else {
        return Ok(EvaluatedValue::Null);
    };
    let length = length
        .whole_integer()
        .ok_or_else(|| type_error("a string length must be a whole number"))?;
    if length < 0 {
        return Err(EngineError::data(GqlStatus::SUBSTRING_ERROR, "a string length cannot be negative"));
    }
    let characters = source.chars().count();
    let taken = usize::try_from(length).unwrap_or(usize::MAX).min(characters);
    Ok(string_value(if function == ScalarFunction::Left {
        source.chars().take(taken).collect()
    } else {
        source.chars().skip(characters - taken).collect()
    }))
}

/// `TRIM([LEADING | TRAILING | BOTH] [c] FROM s)` and `TRIM(s)` (§20.24
/// General Rule 4): `c`, a single space by default, removed from one or both
/// ends.
fn single_character_trim(
    function: ScalarFunction,
    source: &EvaluatedValue,
    character: Option<&EvaluatedValue>,
) -> EngineResult<EvaluatedValue> {
    if any_null([source].into_iter().chain(character)) {
        return Ok(EvaluatedValue::Null);
    }
    // §20.25 General Rule 2, GF07: a byte string trims one byte, X'00' by
    // default.
    if let EvaluatedValue::Scalar(ScalarValue::Bytes(bytes)) = source {
        let trimmed = match character {
            None => 0,
            Some(EvaluatedValue::Scalar(ScalarValue::Bytes(trim))) if trim.len() == 1 => trim[0],
            Some(EvaluatedValue::Scalar(ScalarValue::Bytes(_))) => {
                return Err(EngineError::data(
                    GqlStatus::TRIM_ERROR,
                    "the TRIM byte string must be exactly one byte",
                ))
            }
            Some(_) => return Err(type_error("a byte string is trimmed by a byte string")),
        };
        let start = if function == ScalarFunction::TrimTrailing {
            0
        } else {
            bytes.iter().position(|byte| *byte != trimmed).unwrap_or(bytes.len())
        };
        let end = if function == ScalarFunction::TrimLeading {
            bytes.len()
        } else {
            bytes.iter().rposition(|byte| *byte != trimmed).map_or(start, |index| index + 1)
        };
        return Ok(EvaluatedValue::Scalar(ScalarValue::Bytes(bytes[start..end.max(start)].into())));
    }
    let Some(source) = string(source, "TRIM")? else {
        return Ok(EvaluatedValue::Null);
    };
    let character = match character {
        None => ' ',
        Some(EvaluatedValue::Scalar(ScalarValue::String(value))) => {
            let mut characters = value.chars();
            match (characters.next(), characters.next()) {
                (Some(character), None) => character,
                _ => {
                    return Err(EngineError::data(
                        GqlStatus::TRIM_ERROR,
                        "the TRIM character must be exactly one character",
                    ))
                }
            }
        }
        Some(_) => {
            return Err(EngineError::data(
                GqlStatus::VALUES_NOT_COMPARABLE,
                "the TRIM character and its source must both be character strings",
            ))
        }
    };
    Ok(string_value(
        match function {
            ScalarFunction::TrimLeading => source.trim_start_matches(character),
            ScalarFunction::TrimTrailing => source.trim_end_matches(character),
            _ => source.trim_matches(character),
        }
        .to_string(),
    ))
}

/// `LTRIM`, `RTRIM` and `BTRIM` (§20.24 General Rule 5): every character in
/// the set, a single space by default, removed from one or both ends.
fn multi_character_trim(
    function: ScalarFunction,
    source: &EvaluatedValue,
    characters: Option<&EvaluatedValue>,
) -> EngineResult<EvaluatedValue> {
    if any_null([source].into_iter().chain(characters)) {
        return Ok(EvaluatedValue::Null);
    }
    let Some(source) = string(source, function.name())? else {
        return Ok(EvaluatedValue::Null);
    };
    // A set built once: a lookup per character trimmed, not a scan of the
    // trim characters for each (two long strings would cost their product).
    let set: std::collections::BTreeSet<char> = match characters {
        None => std::collections::BTreeSet::from([' ']),
        Some(value) if is_null(value) => return Ok(EvaluatedValue::Null),
        Some(EvaluatedValue::Scalar(ScalarValue::String(value))) => value.chars().collect(),
        Some(_) => {
            return Err(EngineError::data(
                GqlStatus::VALUES_NOT_COMPARABLE,
                format!("{} characters and source must both be character strings", function.name()),
            ))
        }
    };
    if source.is_empty() || set.is_empty() {
        return Ok(string_value(source.to_string()));
    }
    let in_set = |character: char| set.contains(&character);
    Ok(string_value(
        match function {
            ScalarFunction::LTrim => source.trim_start_matches(in_set),
            ScalarFunction::RTrim => source.trim_end_matches(in_set),
            _ => source.trim_matches(in_set),
        }
        .to_string(),
    ))
}

/// `NORMALIZE(s [, form])` (§20.24 General Rule 2), NFC by default.
fn normalize(source: &EvaluatedValue, form: Option<&EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    let Some(source) = string(source, "NORMALIZE")? else {
        return Ok(EvaluatedValue::Null);
    };
    let form = match form {
        None => "NFC",
        Some(EvaluatedValue::Scalar(ScalarValue::String(form))) => form,
        Some(_) => return Err(type_error("NORMALIZE takes NFC, NFD, NFKC or NFKD")),
    };
    Ok(string_value(normal_form(source, form)?))
}

/// `source` in the Unicode normal form `form` names (UAX #15).
fn normal_form(source: &str, form: &str) -> EngineResult<String> {
    Ok(match form.to_ascii_uppercase().as_str() {
        "NFC" => source.nfc().collect(),
        "NFD" => source.nfd().collect(),
        "NFKC" => source.nfkc().collect(),
        "NFKD" => source.nfkd().collect(),
        _ => return Err(type_error(format!("{form} is not a Unicode normal form"))),
    })
}

/// Whether `source` is already in the Unicode normal form `form` names.
fn is_normalized(source: &str, form: &str) -> EngineResult<bool> {
    Ok(match form.to_ascii_uppercase().as_str() {
        "NFC" => unicode_normalization::is_nfc(source),
        "NFD" => unicode_normalization::is_nfd(source),
        "NFKC" => unicode_normalization::is_nfkc(source),
        "NFKD" => unicode_normalization::is_nfkd(source),
        _ => return Err(type_error(format!("{form} is not a Unicode normal form"))),
    })
}

/// The numeric value functions of §20.22.
fn numeric_function(function: ScalarFunction, arguments: &[EvaluatedValue]) -> EngineResult<EvaluatedValue> {
    if any_null(arguments) {
        return Ok(EvaluatedValue::Null);
    }
    let mut numbers = Vec::with_capacity(arguments.len());
    for argument in arguments {
        match number(argument, function.name())? {
            Some(value) => numbers.push(value),
            None => return Ok(EvaluatedValue::Null),
        }
    }
    let first = numbers
        .first()
        .copied()
        .ok_or_else(|| type_error(format!("{} needs an argument", function.name())))?;
    let value = first.to_float();
    match function {
        ScalarFunction::Abs => match first {
            Number::Integer(value) => integer(value.checked_abs().ok_or_else(|| out_of_range("ABS overflowed"))?),
            Number::Float(value) => float(value.abs()),
            Number::Decimal(value) => decimal(Ok(value.abs())),
        },
        ScalarFunction::Floor | ScalarFunction::Ceil => match first {
            Number::Integer(value) => integer(value),
            // An exact number rounds to an exact integer, of scale 0.
            Number::Decimal(value) => {
                let whole = value.integer().unwrap_or_else(|| {
                    let toward_zero = value.mantissa / 10_i128.pow(value.scale);
                    match (function == ScalarFunction::Floor, value.mantissa < 0) {
                        (true, true) => toward_zero - 1,
                        (false, false) => toward_zero + 1,
                        _ => toward_zero,
                    }
                });
                decimal(Decimal::from_integer(whole))
            }
            Number::Float(value) => float(if function == ScalarFunction::Floor {
                value.floor()
            } else {
                value.ceil()
            }),
        },
        ScalarFunction::Mod => {
            let divisor = numbers[1];
            evaluate_arithmetic(PhysicalBinaryOperator::Modulo, numeric_value(first), numeric_value(divisor))
        }
        ScalarFunction::Sqrt => power(Number::Float(value), Number::Float(0.5)),
        ScalarFunction::Power => power(first, numbers[1]),
        ScalarFunction::Exp => float(value.exp()),
        ScalarFunction::Ln => natural_logarithm(first),
        // Cypher's one-argument `log` is the natural logarithm; GQL's has a
        // base first.
        ScalarFunction::Log if numbers.len() == 1 => natural_logarithm(first),
        ScalarFunction::Log => general_logarithm(first, numbers[1]),
        ScalarFunction::Log10 => general_logarithm(Number::Integer(10), first),
        // The domain is checked on the exact value: 1.00000000000000000001
        // is outside it, though its nearest float is not.
        ScalarFunction::Asin | ScalarFunction::Acos if first.magnitude_cmp_one().is_none_or(Ordering::is_gt) => Err(EngineError::data(
            GqlStatus::INVALID_ARGUMENT_FOR_TRIGONOMETRIC_FUNCTION,
            format!("{} is defined on [-1, 1]", function.name()),
        )),
        ScalarFunction::Asin => float(first.asin()),
        ScalarFunction::Acos => float(first.acos()),
        ScalarFunction::Atan => float(value.atan()),
        ScalarFunction::Sin => float(value.sin()),
        ScalarFunction::Cos => float(value.cos()),
        ScalarFunction::Tan => float(value.tan()),
        ScalarFunction::Cot => float(1.0 / value.tan()),
        ScalarFunction::Sinh => float(value.sinh()),
        ScalarFunction::Cosh => float(value.cosh()),
        ScalarFunction::Tanh => float(value.tanh()),
        ScalarFunction::Degrees => float(value.to_degrees()),
        ScalarFunction::Radians => float(value.to_radians()),
        other => Err(EngineError::data(
            GqlStatus::DATA_EXCEPTION,
            format!("{} is not a numeric function", other.name()),
        )),
    }
}

fn numeric_value(value: Number) -> EvaluatedValue {
    match value {
        Number::Integer(value) => EvaluatedValue::Scalar(ScalarValue::Integer(value)),
        Number::Float(value) => EvaluatedValue::Scalar(ScalarValue::Float(format!("{value:?}").into())),
        Number::Decimal(value) => EvaluatedValue::Scalar(ScalarValue::Decimal(value)),
    }
}

/// `POWER(base, exponent)` (§20.22 General Rule 11).
fn power(base: Number, exponent: Number) -> EngineResult<EvaluatedValue> {
    // A decimal base near 1 keeps its distance from 1, which its float would
    // lose: 1.00000000000000000001M is 1.0 as a float, and its 10^20th power
    // is about e, not 1 (as `LN` keeps it).
    if let Some(offset) = base.inexact_offset_from_one() {
        return float((exponent.to_float() * offset.ln_1p()).exp());
    }
    let base = base.to_float();
    let invalid = |message: &str| Err(EngineError::data(GqlStatus::INVALID_ARGUMENT_FOR_POWER_FUNCTION, message));
    // An exact exponent keeps its parity and its fraction, which a binary64
    // can lose: an odd exponent above 2^53 converts to an even float, and
    // 1.00000000000000000001M to 1.
    if base < 0.0 {
        let integral = exponent.exact_integer();
        match (integral, exponent) {
            (Some(integral), _) => {
                let magnitude = (-base).powf(exponent.to_float());
                return float(if integral % 2 != 0 { -magnitude } else { magnitude });
            }
            (None, Number::Decimal(_)) => {
                return invalid("a negative number has no real power with a fractional exponent");
            }
            (None, _) => {}
        }
    }
    let exponent = exponent.to_float();
    if base == 0.0 {
        return if exponent < 0.0 {
            invalid("zero cannot be raised to a negative power")
        } else if exponent == 0.0 {
            float(1.0)
        } else {
            float(0.0)
        };
    }
    if base < 0.0 && exponent.fract() != 0.0 {
        return invalid("a negative number has no real power with a fractional exponent");
    }
    float(base.powf(exponent))
}

/// `LN(n)` (§20.22 General Rule 9).
fn natural_logarithm(value: Number) -> EngineResult<EvaluatedValue> {
    // Every exact value's sign survives its conversion to a float.
    if value.to_float() <= 0.0 {
        return Err(EngineError::data(
            GqlStatus::INVALID_ARGUMENT_FOR_NATURAL_LOGARITHM,
            "the natural logarithm is defined on positive numbers",
        ));
    }
    float(value.ln())
}

/// `LOG(base, n)` (§20.22 General Rule 8, as Corrigendum 1 amends it).
fn general_logarithm(base: Number, value: Number) -> EngineResult<EvaluatedValue> {
    if value.to_float() <= 0.0 || base.to_float() <= 0.0 || base.cmp_one() == Some(Ordering::Equal) {
        return Err(EngineError::data(
            GqlStatus::INVALID_ARGUMENT_FOR_GENERAL_LOGARITHM_FUNCTION,
            "a logarithm needs a positive argument and a positive base other than 1",
        ));
    }
    // The dedicated routines are exact where change of base rounds:
    // `LOG10(1000)` is 3, not 2.9999999999999996.
    float(if base.is(10) {
        value.log10()
    } else if base.is(2) {
        value.log2()
    } else {
        value.ln() / base.ln()
    })
}
