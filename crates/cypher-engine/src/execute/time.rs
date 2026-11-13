//! Temporal values at run time: the datetime and duration functions (GQL
//! §20.27, §20.29), their arithmetic (§20.26, §20.28) and its conditions.
//!
//! Each function's last argument is the request timestamp (§20.27 General
//! Rule 1), nanoseconds since the Unix epoch, which the planner adds. The
//! current time zone displacement is UTC: the engine has no session time
//! zone (GS15 is not claimed).

use chrono::{NaiveDate, NaiveTime};
use hydradb_cypher_ast::GqlStatus;

use super::{is_null, EvaluatedValue};
use crate::temporal::{
    add_duration, duration_between, parse_date, parse_datetime, parse_duration, parse_time, request_datetime, DurationValue, TemporalError,
    TemporalValue,
};
use crate::{EngineError, EngineResult, PhysicalBinaryOperator, PhysicalUnaryOperator, ScalarFunction, ScalarValue};

/// The current time zone displacement: UTC.
pub(super) const CURRENT_OFFSET: i32 = 0;

/// The condition a temporal failure raises.
pub(super) fn temporal_error(error: TemporalError) -> EngineError {
    let (status, message) = match error {
        TemporalError::InvalidFormat => (
            GqlStatus::INVALID_DATETIME_FORMAT,
            "the string is no date, time or datetime of that type",
        ),
        TemporalError::InvalidDuration => (GqlStatus::INVALID_DURATION_FORMAT, "the string is no duration of one unit group"),
        TemporalError::Overflow => (
            GqlStatus::DATETIME_FIELD_OVERFLOW,
            "the temporal result is outside the supported range",
        ),
        TemporalError::NotComparable => (GqlStatus::VALUES_NOT_COMPARABLE, "the temporal operands are not comparable"),
        TemporalError::IncompatibleUnitGroups => (
            GqlStatus::INCOMPATIBLE_TEMPORAL_INSTANT_UNIT_GROUPS,
            "the durations are of different unit groups",
        ),
        TemporalError::DivisionByZero => (GqlStatus::DIVISION_BY_ZERO, "a duration divided by zero"),
    };
    EngineError::data(status, message)
}

fn invalid_type(message: &str) -> EngineError {
    EngineError::data(GqlStatus::INVALID_VALUE_TYPE, message)
}

fn interval_error(error: TemporalError) -> EngineError {
    match error {
        TemporalError::Overflow => EngineError::data(GqlStatus::INTERVAL_FIELD_OVERFLOW, "the duration is too long"),
        error => temporal_error(error),
    }
}

/// The current request's instant at the current displacement.
fn now(request: &EvaluatedValue) -> EngineResult<chrono::NaiveDateTime> {
    match request {
        EvaluatedValue::Scalar(ScalarValue::Integer(nanos)) => {
            request_datetime(*nanos, CURRENT_OFFSET).ok_or_else(|| temporal_error(TemporalError::Overflow))
        }
        _ => Err(EngineError::data(GqlStatus::DATA_EXCEPTION, "the request timestamp is not set")),
    }
}

/// `DATE`, `ZONED_TIME`, `LOCAL_TIME`, `ZONED_DATETIME`, `LOCAL_DATETIME`,
/// `DURATION` and `DURATION_BETWEEN`.
pub(super) fn evaluate_temporal_function(function: ScalarFunction, mut arguments: Vec<EvaluatedValue>) -> EngineResult<EvaluatedValue> {
    let request = arguments.pop().unwrap_or(EvaluatedValue::Null);
    if arguments.iter().any(is_null) {
        return Ok(EvaluatedValue::Null);
    }
    let value = match (function, arguments.as_slice()) {
        // §20.27 General Rule 10 a): the current instant.
        (ScalarFunction::Date, []) => TemporalValue::Date(now(&request)?.date()),
        (ScalarFunction::LocalTime, []) => TemporalValue::LocalTime(now(&request)?.time()),
        (ScalarFunction::ZonedTime, []) => TemporalValue::ZonedTime {
            time: now(&request)?.time(),
            offset: CURRENT_OFFSET,
        },
        (ScalarFunction::LocalDatetime, []) => TemporalValue::LocalDatetime(now(&request)?),
        (ScalarFunction::ZonedDatetime, []) => TemporalValue::ZonedDatetime {
            datetime: now(&request)?,
            offset: CURRENT_OFFSET,
        },
        // General Rule 10 b): from a string or a record.
        (ScalarFunction::Date, [argument]) => match argument {
            EvaluatedValue::Scalar(ScalarValue::String(text)) => {
                TemporalValue::Date(parse_date(text).ok_or_else(|| temporal_error(TemporalError::InvalidFormat))?)
            }
            EvaluatedValue::Record(fields) => TemporalValue::Date(record_date(fields, true)?),
            _ => return Err(invalid_type("DATE takes a date string or a record")),
        },
        (ScalarFunction::LocalTime | ScalarFunction::ZonedTime, [argument]) => {
            let zoned = function == ScalarFunction::ZonedTime;
            let (time, offset) = match argument {
                EvaluatedValue::Scalar(ScalarValue::String(text)) => {
                    parse_time(text).ok_or_else(|| temporal_error(TemporalError::InvalidFormat))?
                }
                EvaluatedValue::Record(fields) => record_time(fields, zoned, &[])?,
                _ => return Err(invalid_type("a time function takes a time string or a record")),
            };
            match (zoned, offset) {
                (false, None) => TemporalValue::LocalTime(time),
                (false, Some(_)) => return Err(temporal_error(TemporalError::InvalidFormat)),
                (true, offset) => TemporalValue::ZonedTime {
                    time,
                    offset: offset.unwrap_or(CURRENT_OFFSET),
                },
            }
        }
        (ScalarFunction::LocalDatetime | ScalarFunction::ZonedDatetime, [argument]) => {
            let zoned = function == ScalarFunction::ZonedDatetime;
            let (datetime, offset) = match argument {
                EvaluatedValue::Scalar(ScalarValue::String(text)) => {
                    parse_datetime(text).ok_or_else(|| temporal_error(TemporalError::InvalidFormat))?
                }
                EvaluatedValue::Record(fields) => {
                    if !DATE_FIELDS.iter().all(|name| field(fields, name).is_some()) {
                        return Err(invalid_field_names());
                    }
                    let date = record_date(fields, false)?;
                    let (time, offset) = record_time(fields, zoned, &DATE_FIELDS)?;
                    (date.and_time(time), offset)
                }
                _ => return Err(invalid_type("a datetime function takes a datetime string or a record")),
            };
            match (zoned, offset) {
                (false, None) => TemporalValue::LocalDatetime(datetime),
                (false, Some(_)) => return Err(temporal_error(TemporalError::InvalidFormat)),
                (true, offset) => TemporalValue::ZonedDatetime {
                    datetime,
                    offset: offset.unwrap_or(CURRENT_OFFSET),
                },
            }
        }
        // §20.29.
        (ScalarFunction::Duration, [argument]) => TemporalValue::Duration(match argument {
            EvaluatedValue::Scalar(ScalarValue::String(text)) => parse_duration(text).map_err(temporal_error)?,
            EvaluatedValue::Record(fields) => record_duration(fields)?,
            _ => return Err(invalid_type("DURATION takes a duration string or a record")),
        }),
        // §20.28 General Rules 2, 3 and 5 d).
        (ScalarFunction::DurationBetween, [from, to, qualifier @ ..]) => {
            let year_month =
                matches!(qualifier, [EvaluatedValue::Scalar(ScalarValue::String(qualifier))] if qualifier.as_ref() == "YEAR TO MONTH");
            let (EvaluatedValue::Temporal(from), EvaluatedValue::Temporal(to)) = (from, to) else {
                return Err(invalid_type("DURATION_BETWEEN takes two temporal instants"));
            };
            if matches!(from, TemporalValue::Duration(_)) || matches!(to, TemporalValue::Duration(_)) {
                return Err(invalid_type("DURATION_BETWEEN takes two temporal instants"));
            }
            // Syntax Rule 3 d): two times give a day and time-based duration.
            if year_month && matches!(from, TemporalValue::LocalTime(_) | TemporalValue::ZonedTime { .. }) {
                return Err(invalid_type("DURATION_BETWEEN two times is DAY TO SECOND"));
            }
            TemporalValue::Duration(duration_between(*from, *to, year_month).map_err(temporal_error)?)
        }
        _ => return Err(invalid_type("a temporal function was given arguments it does not take")),
    };
    Ok(EvaluatedValue::Temporal(value))
}

fn field<'a>(fields: &'a [(Box<str>, EvaluatedValue)], name: &str) -> Option<&'a EvaluatedValue> {
    fields.iter().find(|(field, _)| field.as_ref() == name).map(|(_, value)| value)
}

fn field_integer(fields: &[(Box<str>, EvaluatedValue)], name: &str, default: i128) -> EngineResult<i128> {
    match field(fields, name) {
        None => Ok(default),
        Some(EvaluatedValue::Scalar(ScalarValue::Integer(value))) => Ok(*value),
        Some(_) => Err(invalid_value()),
    }
}

fn invalid_value() -> EngineError {
    EngineError::data(
        GqlStatus::INVALID_DATETIME_FUNCTION_VALUE,
        "a datetime function field is out of range",
    )
}

fn invalid_field_names() -> EngineError {
    EngineError::data(
        GqlStatus::INVALID_DATETIME_FUNCTION_FIELD_NAME,
        "the record's fields are not a set the datetime function takes",
    )
}

/// §20.27 General Rules 2, 6 and 7 b): `year`, then `month`, then `day`.
/// A DATE's `year`, `month` and `day`. With `alone`, the record is a DATE's
/// and holds no other field; a datetime's also holds time fields, which
/// [`record_time`] checks.
fn record_date(fields: &[(Box<str>, EvaluatedValue)], alone: bool) -> EngineResult<NaiveDate> {
    let has = |name: &str| field(fields, name).is_some();
    if alone && fields.iter().any(|(name, _)| !DATE_FIELDS.contains(&name.as_ref())) {
        return Err(invalid_field_names());
    }
    if !has("year") || (has("day") && !has("month")) {
        return Err(invalid_field_names());
    }
    let year = i32::try_from(field_integer(fields, "year", 0)?).map_err(|_| invalid_value())?;
    let month = u32::try_from(field_integer(fields, "month", 1)?).map_err(|_| invalid_value())?;
    let day = u32::try_from(field_integer(fields, "day", 1)?).map_err(|_| invalid_value())?;
    NaiveDate::from_ymd_opt(year, month, day).ok_or_else(invalid_value)
}

/// The fields a DATE's record may hold (§20.27 General Rule 2).
const DATE_FIELDS: [&str; 3] = ["year", "month", "day"];

/// §20.27 GR3–6, 8 b): an hour-led time set, optionally zoned.
fn record_time(fields: &[(Box<str>, EvaluatedValue)], zoned: bool, date_fields: &[&str]) -> EngineResult<(NaiveTime, Option<i32>)> {
    const TIME_FIELDS: [&str; 7] = ["hour", "minute", "second", "millisecond", "microsecond", "nanosecond", "timezone"];
    if fields
        .iter()
        .any(|(name, _)| !TIME_FIELDS.contains(&name.as_ref()) && !date_fields.contains(&name.as_ref()))
    {
        return Err(invalid_field_names());
    }
    let has = |name: &str| field(fields, name).is_some();
    let subseconds = ["millisecond", "microsecond", "nanosecond"]
        .into_iter()
        .filter(|name| has(name))
        .count();
    let complete = |name: &str, before: &str| !has(name) || has(before);
    if (!has("hour") && !(zoned && date_fields.is_empty() && fields.len() == 1 && has("timezone")))
        || !complete("minute", "hour")
        || !complete("second", "minute")
        || (subseconds > 0 && !has("second"))
        || subseconds > 1
        || (has("timezone") && !zoned)
    {
        return Err(invalid_field_names());
    }
    let unsigned = |name, default, most: i128| {
        let value = field_integer(fields, name, default)?;
        if (0..=most).contains(&value) {
            Ok(value as u32)
        } else {
            Err(invalid_value())
        }
    };
    let nanos = if has("millisecond") {
        unsigned("millisecond", 0, 999)? * 1_000_000
    } else if has("microsecond") {
        unsigned("microsecond", 0, 999_999)? * 1_000
    } else {
        unsigned("nanosecond", 0, 999_999_999)?
    };
    let time = NaiveTime::from_hms_nano_opt(
        unsigned("hour", 0, 23)?,
        unsigned("minute", 0, 59)?,
        unsigned("second", 0, 59)?,
        nanos,
    )
    .ok_or_else(invalid_value)?;
    let offset = match field(fields, "timezone") {
        None => None,
        Some(EvaluatedValue::Scalar(ScalarValue::String(shift))) => Some(
            parse_time(&format!("00:00{shift}"))
                .and_then(|(_, offset)| offset)
                .ok_or_else(invalid_value)?,
        ),
        Some(_) => return Err(invalid_value()),
    };
    Ok((time, offset))
}

/// §20.29 General Rules 2 and 3: `years` and `months`, or `days` to the
/// subsecond units; with `years` or `months`, a year and month-based
/// duration of them.
fn record_duration(fields: &[(Box<str>, EvaluatedValue)]) -> EngineResult<DurationValue> {
    const YEAR_MONTH: [&str; 2] = ["years", "months"];
    const DAY_TIME: [&str; 7] = ["days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"];
    if fields
        .iter()
        .any(|(name, _)| !YEAR_MONTH.contains(&name.as_ref()) && !DAY_TIME.contains(&name.as_ref()))
        || fields.is_empty()
    {
        return Err(EngineError::data(
            GqlStatus::INVALID_DURATION_FUNCTION_FIELD_NAME,
            "the record's fields are not duration units",
        ));
    }
    let amount = |name| match field(fields, name) {
        None => Ok(0),
        Some(EvaluatedValue::Scalar(ScalarValue::Integer(value))) => Ok(*value),
        Some(_) => Err(temporal_error(TemporalError::InvalidDuration)),
    };
    let year_month = fields.iter().any(|(name, _)| YEAR_MONTH.contains(&name.as_ref()));
    // A duration is of one unit group (§4.16.6.3): a record of both would
    // be a different amount whichever group it kept.
    if year_month && fields.iter().any(|(name, _)| DAY_TIME.contains(&name.as_ref())) {
        return Err(EngineError::data(
            GqlStatus::INVALID_DURATION_FUNCTION_FIELD_NAME,
            "a duration's fields are of one unit group: years and months, or days to nanoseconds",
        ));
    }
    if year_month {
        let (years, months) = (amount("years")?, amount("months")?);
        let months = years
            .checked_mul(12)
            .and_then(|years| years.checked_add(months))
            .and_then(|months| i64::try_from(months).ok())
            .ok_or_else(|| EngineError::data(GqlStatus::INTERVAL_FIELD_OVERFLOW, "the duration is too long"))?;
        return Ok(DurationValue::YearMonth { months });
    }
    let units: [(&str, i128); 7] = [
        ("days", 86_400_000_000_000),
        ("hours", 3_600_000_000_000),
        ("minutes", 60_000_000_000),
        ("seconds", 1_000_000_000),
        ("milliseconds", 1_000_000),
        ("microseconds", 1_000),
        ("nanoseconds", 1),
    ];
    let mut nanoseconds: i128 = 0;
    for (name, scale) in units {
        nanoseconds = amount(name)?
            .checked_mul(scale)
            .and_then(|amount| nanoseconds.checked_add(amount))
            .ok_or_else(|| EngineError::data(GqlStatus::INTERVAL_FIELD_OVERFLOW, "the duration is too long"))?;
    }
    Ok(DurationValue::DayTime { nanoseconds })
}

/// Arithmetic with a temporal operand (Table 1, §20.26, §20.28), or `None`
/// when neither operand is temporal.
pub(super) fn temporal_arithmetic(
    operator: PhysicalBinaryOperator,
    left: &EvaluatedValue,
    right: &EvaluatedValue,
) -> Option<EngineResult<EvaluatedValue>> {
    use PhysicalBinaryOperator::{Add, Divide, Multiply, Subtract};
    let left_temporal = matches!(left, EvaluatedValue::Temporal(_));
    let right_temporal = matches!(right, EvaluatedValue::Temporal(_));
    if !left_temporal && !right_temporal {
        return None;
    }
    if is_null(left) || is_null(right) {
        return Some(Ok(EvaluatedValue::Null));
    }
    let factor = |value: &EvaluatedValue| match value {
        EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => Some(Ok(*integer as f64)),
        EvaluatedValue::Scalar(ScalarValue::Float(text)) => {
            Some(text.parse::<f64>().map_err(|_| invalid_type("a duration factor is a number")))
        }
        _ => None,
    };
    let result = match (operator, left, right) {
        (
            Add | Subtract,
            EvaluatedValue::Temporal(TemporalValue::Duration(left)),
            EvaluatedValue::Temporal(TemporalValue::Duration(right)),
        ) => if operator == Subtract {
            left.minus(*right)
        } else {
            left.plus(*right)
        }
        .map(TemporalValue::Duration)
        .map_err(interval_error),
        (Add | Subtract, EvaluatedValue::Temporal(instant), EvaluatedValue::Temporal(TemporalValue::Duration(duration))) => {
            if operator == Subtract {
                crate::temporal::subtract_duration(*instant, *duration)
            } else {
                add_duration(*instant, *duration)
            }
            .map_err(temporal_error)
        }
        (Add, EvaluatedValue::Temporal(TemporalValue::Duration(duration)), EvaluatedValue::Temporal(instant)) => {
            add_duration(*instant, *duration).map_err(temporal_error)
        }
        // Table 1: an instant minus an instant is their duration, as
        // DURATION_BETWEEN(right, left) (NOTE 153).
        (Subtract, EvaluatedValue::Temporal(later), EvaluatedValue::Temporal(earlier)) => duration_between(*earlier, *later, false)
            .map(TemporalValue::Duration)
            .map_err(temporal_error),
        (Multiply, EvaluatedValue::Temporal(TemporalValue::Duration(duration)), number)
        | (Multiply, number, EvaluatedValue::Temporal(TemporalValue::Duration(duration))) => match number {
            EvaluatedValue::Scalar(ScalarValue::Integer(integer)) => duration
                .multiply_integer(*integer)
                .map(TemporalValue::Duration)
                .map_err(interval_error),
            number => match factor(number) {
                Some(Ok(factor)) => duration.multiply(factor).map(TemporalValue::Duration).map_err(interval_error),
                Some(Err(error)) => Err(error),
                None => Err(invalid_type("a duration is multiplied by a number")),
            },
        },
        (Divide, EvaluatedValue::Temporal(TemporalValue::Duration(duration)), EvaluatedValue::Scalar(ScalarValue::Integer(divisor))) => {
            duration
                .divide_integer(*divisor)
                .map(TemporalValue::Duration)
                .map_err(interval_error)
        }
        (Divide, EvaluatedValue::Temporal(TemporalValue::Duration(duration)), number) => match factor(number) {
            Some(Ok(divisor)) => duration.divide(divisor).map(TemporalValue::Duration).map_err(interval_error),
            Some(Err(error)) => Err(error),
            None => Err(invalid_type("a duration is divided by a number")),
        },
        _ => Err(invalid_type("no arithmetic combines these temporal operands")),
    };
    Some(result.map(EvaluatedValue::Temporal))
}

/// `+d` and `-d` for a duration (§20.28 <duration factor>), or `None` when
/// the operand is not temporal.
pub(super) fn temporal_sign(operator: PhysicalUnaryOperator, value: &EvaluatedValue) -> Option<EngineResult<EvaluatedValue>> {
    let EvaluatedValue::Temporal(temporal) = value else {
        return None;
    };
    Some(match (operator, temporal) {
        (PhysicalUnaryOperator::Plus, TemporalValue::Duration(_)) => Ok(value.clone()),
        // A duration out of its range is an interval field overflow, as for
        // `d1 - d2`.
        (PhysicalUnaryOperator::Minus, TemporalValue::Duration(duration)) => duration
            .negate()
            .map(|duration| EvaluatedValue::Temporal(TemporalValue::Duration(duration)))
            .map_err(interval_error),
        _ => Err(invalid_type("only a duration has a sign")),
    })
}

/// `ABS(d)` (§20.29 <duration absolute value function>).
pub(super) fn duration_absolute(value: &EvaluatedValue) -> Option<EngineResult<EvaluatedValue>> {
    match value {
        EvaluatedValue::Temporal(TemporalValue::Duration(duration)) => Some(
            duration
                .absolute()
                .map(|duration| EvaluatedValue::Temporal(TemporalValue::Duration(duration)))
                .map_err(|_| EngineError::data(GqlStatus::INTERVAL_FIELD_OVERFLOW, "the duration's magnitude is too long")),
        ),
        _ => None,
    }
}
