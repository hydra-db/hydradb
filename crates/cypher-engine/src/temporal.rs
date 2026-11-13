//! GQL's temporal values (§4.16.6): the five temporal instants and the two
//! kinds of temporal duration, read from and written as their ISO 8601
//! strings (§21.2), compared, and combined by the arithmetic of §20.26 and
//! §20.28 on the Gregorian calendar.
//!
//! Seconds have nanosecond precision (IL024: 9). A zoned instant carries its
//! time zone displacement in seconds east of UTC, and its time of day is the
//! local one at that displacement; two zoned instants compare as the instants
//! they denote in UTC. A day and time-based duration counts nanoseconds, so
//! it is normalized as §20.28 General Rule 6 asks, a day being 24 hours; a
//! year and month-based one counts months.

use std::cmp::Ordering;

use chrono::{Datelike, Duration as ChronoDuration, Months, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use hydradb_cypher_ast::{IntervalField, IntervalQualifier};

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NANOS_PER_DAY: i128 = 86_400 * NANOS_PER_SECOND;

/// A temporal instant or a temporal duration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalValue {
    Date(NaiveDate),
    LocalTime(NaiveTime),
    /// The local time of day at a displacement of `offset` seconds east of
    /// UTC.
    ZonedTime {
        time: NaiveTime,
        offset: i32,
    },
    LocalDatetime(NaiveDateTime),
    /// The local date and time at a displacement of `offset` seconds east
    /// of UTC.
    ZonedDatetime {
        datetime: NaiveDateTime,
        offset: i32,
    },
    Duration(DurationValue),
}

/// A temporal duration of one unit group (§4.16.6.3).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DurationValue {
    /// The year and month-based unit group.
    YearMonth { months: i64 },
    /// The day and time-based unit group.
    DayTime { nanoseconds: i128 },
}

/// Which temporal type a value is of, by its most specific type.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalKind {
    Date,
    LocalTime,
    ZonedTime,
    LocalDatetime,
    ZonedDatetime,
    YearMonthDuration,
    DayTimeDuration,
}

/// Why a temporal operation failed, each the condition the standard names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalError {
    /// `22007`: a string that is not the instant asked for.
    InvalidFormat,
    /// `22G0H`: a string that is no duration.
    InvalidDuration,
    /// `22008`: a result outside the supported range.
    Overflow,
    /// `22G04`: operands that cannot be compared or combined.
    NotComparable,
    /// `22G14`: durations of different unit groups.
    IncompatibleUnitGroups,
    /// `22012`: a duration divided by zero.
    DivisionByZero,
}

impl TemporalValue {
    pub fn kind(&self) -> TemporalKind {
        match self {
            Self::Date(_) => TemporalKind::Date,
            Self::LocalTime(_) => TemporalKind::LocalTime,
            Self::ZonedTime { .. } => TemporalKind::ZonedTime,
            Self::LocalDatetime(_) => TemporalKind::LocalDatetime,
            Self::ZonedDatetime { .. } => TemporalKind::ZonedDatetime,
            Self::Duration(DurationValue::YearMonth { .. }) => TemporalKind::YearMonthDuration,
            Self::Duration(DurationValue::DayTime { .. }) => TemporalKind::DayTimeDuration,
        }
    }

    /// The GQL literal of the value, as `CAST(... AS STRING)` writes it
    /// (§20.8 General Rule 4 j) iv)) without the type word.
    pub fn to_literal_string(&self) -> String {
        match self {
            Self::Date(date) => format_date(*date),
            Self::LocalTime(time) => format_time(*time),
            Self::ZonedTime { time, offset } => format!("{}{}", format_time(*time), format_offset(*offset)),
            Self::LocalDatetime(datetime) => format!("{}T{}", format_date(datetime.date()), format_time(datetime.time())),
            Self::ZonedDatetime { datetime, offset } => format!(
                "{}T{}{}",
                format_date(datetime.date()),
                format_time(datetime.time()),
                format_offset(*offset)
            ),
            Self::Duration(duration) => duration.to_literal_string(),
        }
    }

    /// The type word of the value's literal: `DATE`, `TIME`, `DATETIME`
    /// or `DURATION`.
    pub fn literal_keyword(&self) -> &'static str {
        match self {
            Self::Date(_) => "DATE",
            Self::LocalTime(_) | Self::ZonedTime { .. } => "TIME",
            Self::LocalDatetime(_) | Self::ZonedDatetime { .. } => "DATETIME",
            Self::Duration(_) => "DURATION",
        }
    }
}

/// The ISO 8601 text of a SQL datetime literal's value (GL12): its date, its
/// time with any shift, or both with `T` between.
pub fn iso_text(date: Option<NaiveDate>, time: Option<(NaiveTime, Option<i32>)>) -> String {
    let time = time.map(|(time, offset)| format!("{}{}", format_time(time), offset.map(format_offset).unwrap_or_default()));
    match (date.map(format_date), time) {
        (Some(date), Some(time)) => format!("{date}T{time}"),
        (Some(date), None) => date,
        (None, Some(time)) => time,
        (None, None) => String::new(),
    }
}

fn format_date(date: NaiveDate) -> String {
    let year = date.year();
    if (0..=9999).contains(&year) {
        format!("{year:04}-{:02}-{:02}", date.month(), date.day())
    } else {
        format!("{year:+05}-{:02}-{:02}", date.month(), date.day())
    }
}

fn format_time(time: NaiveTime) -> String {
    format!(
        "{:02}:{:02}:{:02}.{:09}",
        time.hour(),
        time.minute(),
        time.second(),
        time.nanosecond().min(999_999_999)
    )
}

/// The shortest time shift: `Z`, `+hh`, or `+hh:mm`.
fn format_offset(offset: i32) -> String {
    if offset == 0 {
        return "Z".to_string();
    }
    let sign = if offset < 0 { '-' } else { '+' };
    let minutes = offset.unsigned_abs() / 60;
    let (hours, minutes) = (minutes / 60, minutes % 60);
    if minutes == 0 {
        format!("{sign}{hours:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}")
    }
}

impl DurationValue {
    /// `P1Y2M`, `P0M`; `P1DT2H3M4.5S`, `PT0S`.
    pub fn to_literal_string(&self) -> String {
        match *self {
            Self::YearMonth { months } => {
                let (years, months) = (months / 12, months % 12);
                let mut text = "P".to_string();
                if years != 0 {
                    text.push_str(&format!("{years}Y"));
                }
                if months != 0 || years == 0 {
                    text.push_str(&format!("{months}M"));
                }
                text
            }
            Self::DayTime { nanoseconds } => {
                let days = nanoseconds / NANOS_PER_DAY;
                let rest = nanoseconds % NANOS_PER_DAY;
                let hours = rest / (3600 * NANOS_PER_SECOND);
                let rest = rest % (3600 * NANOS_PER_SECOND);
                let minutes = rest / (60 * NANOS_PER_SECOND);
                let rest = rest % (60 * NANOS_PER_SECOND);
                let (seconds, fraction) = (rest / NANOS_PER_SECOND, rest % NANOS_PER_SECOND);
                let mut text = "P".to_string();
                if days != 0 {
                    text.push_str(&format!("{days}D"));
                }
                let mut time = String::new();
                if hours != 0 {
                    time.push_str(&format!("{hours}H"));
                }
                if minutes != 0 {
                    time.push_str(&format!("{minutes}M"));
                }
                if seconds != 0 || fraction != 0 || (days == 0 && time.is_empty()) {
                    let sign = if rest < 0 && seconds == 0 { "-" } else { "" };
                    if fraction == 0 {
                        time.push_str(&format!("{seconds}S"));
                    } else {
                        time.push_str(&format!("{sign}{seconds}.{:09}S", fraction.abs()));
                    }
                }
                if !time.is_empty() {
                    text.push('T');
                    text.push_str(&time);
                }
                text
            }
        }
    }

    pub fn negate(self) -> Result<Self, TemporalError> {
        Ok(match self {
            Self::YearMonth { months } => Self::YearMonth {
                months: months.checked_neg().ok_or(TemporalError::Overflow)?,
            },
            Self::DayTime { nanoseconds } => Self::DayTime {
                nanoseconds: nanoseconds.checked_neg().ok_or(TemporalError::Overflow)?,
            },
        })
    }

    /// The duration's magnitude; an overflow when it does not fit, never a
    /// different amount.
    pub fn absolute(self) -> Result<Self, TemporalError> {
        Ok(match self {
            Self::YearMonth { months } => Self::YearMonth {
                months: months.checked_abs().ok_or(TemporalError::Overflow)?,
            },
            Self::DayTime { nanoseconds } => Self::DayTime {
                nanoseconds: nanoseconds.checked_abs().ok_or(TemporalError::Overflow)?,
            },
        })
    }

    pub fn plus(self, other: Self) -> Result<Self, TemporalError> {
        match (self, other) {
            (Self::YearMonth { months: left }, Self::YearMonth { months: right }) => Ok(Self::YearMonth {
                months: left.checked_add(right).ok_or(TemporalError::Overflow)?,
            }),
            (Self::DayTime { nanoseconds: left }, Self::DayTime { nanoseconds: right }) => Ok(Self::DayTime {
                nanoseconds: left.checked_add(right).ok_or(TemporalError::Overflow)?,
            }),
            _ => Err(TemporalError::IncompatibleUnitGroups),
        }
    }

    /// `self` less `other`, subtracted directly: negating `other` first would
    /// overflow at the least amount even where the difference fits.
    pub fn minus(self, other: Self) -> Result<Self, TemporalError> {
        match (self, other) {
            (Self::YearMonth { months: left }, Self::YearMonth { months: right }) => Ok(Self::YearMonth {
                months: left.checked_sub(right).ok_or(TemporalError::Overflow)?,
            }),
            (Self::DayTime { nanoseconds: left }, Self::DayTime { nanoseconds: right }) => Ok(Self::DayTime {
                nanoseconds: left.checked_sub(right).ok_or(TemporalError::Overflow)?,
            }),
            _ => Err(TemporalError::IncompatibleUnitGroups),
        }
    }

    /// ISO 8601-2 §14.3: each unit multiplied; a fraction of the smallest
    /// unit rounds to the nearest, a half away from zero (IA005).
    pub fn multiply(self, factor: f64) -> Result<Self, TemporalError> {
        if !factor.is_finite() {
            return Err(TemporalError::Overflow);
        }
        // An integral factor multiplies exactly, as an integral divisor
        // divides; only a fractional one goes through binary floating point.
        if factor.fract() == 0.0 && factor.abs() < 1e36 {
            return self.multiply_integer(factor as i128);
        }
        self.scaled_exactly(factor, false)
    }

    /// The duration times `factor`, or over it with `divide`, exactly: the
    /// float is m * 2^e, so only the final remainder of the smallest unit
    /// rounds, a half away from zero (IA005), never the whole amount.
    fn scaled_exactly(self, factor: f64, divide: bool) -> Result<Self, TemporalError> {
        let overflow = TemporalError::Overflow;
        Ok(match self {
            Self::YearMonth { months } => Self::YearMonth {
                months: i64::try_from(scale_by_float(i128::from(months), factor, divide).ok_or(overflow)?).map_err(|_| overflow)?,
            },
            Self::DayTime { nanoseconds } => Self::DayTime {
                nanoseconds: scale_by_float(nanoseconds, factor, divide).ok_or(overflow)?,
            },
        })
    }

    pub fn multiply_integer(self, factor: i128) -> Result<Self, TemporalError> {
        Ok(match self {
            Self::YearMonth { months } => Self::YearMonth {
                months: i128::from(months)
                    .checked_mul(factor)
                    .and_then(|months| i64::try_from(months).ok())
                    .ok_or(TemporalError::Overflow)?,
            },
            Self::DayTime { nanoseconds } => Self::DayTime {
                nanoseconds: nanoseconds.checked_mul(factor).ok_or(TemporalError::Overflow)?,
            },
        })
    }

    pub fn divide(self, divisor: f64) -> Result<Self, TemporalError> {
        if divisor == 0.0 {
            return Err(TemporalError::DivisionByZero);
        }
        // An integral divisor divides exactly; only a fractional one goes
        // through binary floating point.
        if divisor.fract() == 0.0 && divisor.abs() < 1e36 {
            return self.divide_integer(divisor as i128);
        }
        if !divisor.is_finite() {
            return Err(TemporalError::Overflow);
        }
        self.scaled_exactly(divisor, true)
    }

    /// Division by an integer, exact: only the final remainder of the
    /// smallest unit rounds, to the nearest, a half away from zero (IA005).
    pub fn divide_integer(self, divisor: i128) -> Result<Self, TemporalError> {
        if divisor == 0 {
            return Err(TemporalError::DivisionByZero);
        }
        let divide = |amount: i128| -> Result<i128, TemporalError> {
            let quotient = amount.checked_div(divisor).ok_or(TemporalError::Overflow)?;
            let remainder = (amount % divisor).unsigned_abs();
            // |r| >= |d| - |r| is 2|r| >= |d| without doubling past i128.
            if remainder >= divisor.unsigned_abs() - remainder {
                let away = if (amount < 0) == (divisor < 0) { 1 } else { -1 };
                quotient.checked_add(away).ok_or(TemporalError::Overflow)
            } else {
                Ok(quotient)
            }
        };
        Ok(match self {
            Self::YearMonth { months } => Self::YearMonth {
                months: i64::try_from(divide(i128::from(months))?).map_err(|_| TemporalError::Overflow)?,
            },
            Self::DayTime { nanoseconds } => Self::DayTime {
                nanoseconds: divide(nanoseconds)?,
            },
        })
    }
}

/// `amount * factor`, or `amount / factor` with `divide`, rounded once to an
/// integer a half away from zero; `None` past i128. `factor` is finite and
/// not zero; as a binary64 it is exactly m * 2^e.
fn scale_by_float(amount: i128, factor: f64, divide: bool) -> Option<i128> {
    // Nothing scaled is nothing, however wide the factor.
    if amount == 0 {
        return Some(0);
    }
    let bits = factor.to_bits();
    let negative = (amount < 0) != (bits >> 63 == 1);
    let biased = ((bits >> 52) & 0x7ff) as i64;
    let fraction = bits & ((1 << 52) - 1);
    let (mantissa, exponent) = if biased == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1 << 52), biased - 1075)
    };
    let magnitude = amount.unsigned_abs();
    let result: u128 = if divide {
        // |amount| * 2^-e / m, one bit at a time: the remainder stays below m.
        let mantissa = u128::from(mantissa);
        let (mut quotient, mut remainder) = (magnitude / mantissa, magnitude % mantissa);
        if exponent >= 0 {
            // An integral divisor m * 2^e: divided directly when it fits.
            // Past u128 it is at least 2^128, and a magnitude at most 2^127,
            // so the quotient is below a half but for 2^127 / 2^128, a half
            // exactly, which rounds away from zero.
            let Some(divisor) = mantissa
                .checked_shl(u32::try_from(exponent).ok()?)
                .filter(|divisor| divisor >> exponent == mantissa)
            else {
                let half = magnitude == 1 << 127 && mantissa.is_power_of_two() && i64::from(mantissa.trailing_zeros()) + exponent == 128;
                return signed(negative, u128::from(half));
            };
            let (quotient, remainder) = (magnitude / divisor, magnitude % divisor);
            return signed(negative, quotient + u128::from(remainder >= divisor - remainder));
        }
        for _ in 0..exponent.unsigned_abs() {
            quotient = quotient.checked_mul(2)?;
            remainder <<= 1;
            if remainder >= mantissa {
                remainder -= mantissa;
                quotient += 1;
            }
        }
        quotient + u128::from(remainder >= mantissa - remainder)
    } else {
        // |amount| * m in 192 bits, then shifted by e.
        let mantissa = u128::from(mantissa);
        let (low, high) = (magnitude & u128::from(u64::MAX), magnitude >> 64);
        let (low, high) = (low * mantissa, high * mantissa);
        let (lo, carry) = low.overflowing_add(high << 64);
        let hi = (high >> 64) + u128::from(carry);
        if exponent >= 0 {
            let shift = u32::try_from(exponent).ok()?;
            if hi != 0 || shift >= 128 || lo.leading_zeros() < shift {
                return None;
            }
            lo << shift
        } else {
            let shift = exponent.unsigned_abs();
            let bit = |index: u64| -> u128 {
                match index {
                    0..=127 => (lo >> index) & 1,
                    128..=255 => (hi >> (index - 128)) & 1,
                    _ => 0,
                }
            };
            let floor = match shift {
                0..=127 => {
                    if hi.checked_shr(shift as u32).unwrap_or(0) != 0 {
                        return None;
                    }
                    (lo >> shift) | hi.checked_shl(128 - shift as u32).unwrap_or(0)
                }
                128..=255 => hi >> (shift - 128),
                _ => 0,
            };
            floor.checked_add(bit(shift - 1))?
        }
    };
    signed(negative, result)
}

/// `magnitude` with its sign, if it fits: the least i128 is a magnitude no
/// positive i128 holds, so the sign is applied before the conversion.
pub(crate) fn signed(negative: bool, magnitude: u128) -> Option<i128> {
    if negative {
        0_i128.checked_sub_unsigned(magnitude)
    } else {
        i128::try_from(magnitude).ok()
    }
}

/// How two temporal values order, `None` when they are not essentially
/// comparable (§4.16.6): different instant types or duration unit groups.
pub fn compare(left: &TemporalValue, right: &TemporalValue) -> Option<Ordering> {
    use TemporalValue::*;
    match (left, right) {
        (Date(left), Date(right)) => Some(left.cmp(right)),
        (LocalTime(left), LocalTime(right)) => Some(left.cmp(right)),
        (ZonedTime { time: left, offset: lo }, ZonedTime { time: right, offset: ro }) => {
            Some(utc_time_nanos(*left, *lo).cmp(&utc_time_nanos(*right, *ro)))
        }
        (LocalDatetime(left), LocalDatetime(right)) => Some(left.cmp(right)),
        (
            ZonedDatetime {
                datetime: left,
                offset: lo,
            },
            ZonedDatetime {
                datetime: right,
                offset: ro,
            },
        ) => Some(utc_datetime_nanos(*left, *lo).cmp(&utc_datetime_nanos(*right, *ro))),
        (Duration(DurationValue::YearMonth { months: left }), Duration(DurationValue::YearMonth { months: right })) => {
            Some(left.cmp(right))
        }
        (Duration(DurationValue::DayTime { nanoseconds: left }), Duration(DurationValue::DayTime { nanoseconds: right })) => {
            Some(left.cmp(right))
        }
        _ => None,
    }
}

fn time_nanos(time: NaiveTime) -> i128 {
    i128::from(time.num_seconds_from_midnight()) * NANOS_PER_SECOND + i128::from(time.nanosecond().min(999_999_999))
}

fn utc_time_nanos(time: NaiveTime, offset: i32) -> i128 {
    time_nanos(time) - i128::from(offset) * NANOS_PER_SECOND
}

/// The UTC instant of a zoned datetime, in nanoseconds since the epoch. In
/// `i128` no datetime chrono holds and no offset can overflow it, where
/// subtracting the offset from the datetime itself can leave chrono's range.
fn utc_datetime_nanos(datetime: NaiveDateTime, offset: i32) -> i128 {
    let utc = datetime.and_utc();
    i128::from(utc.timestamp()) * NANOS_PER_SECOND + i128::from(utc.timestamp_subsec_nanos()) - i128::from(offset) * NANOS_PER_SECOND
}

/// The instant a zoned time or datetime names, by which two of them are
/// equal however their offsets differ (§4.16.6): `None` for any other value.
/// The first part tells a time from a datetime.
pub fn zoned_instant(value: &TemporalValue) -> Option<(bool, i128)> {
    match value {
        TemporalValue::ZonedTime { time, offset } => Some((false, utc_time_nanos(*time, *offset))),
        TemporalValue::ZonedDatetime { datetime, offset } => Some((true, utc_datetime_nanos(*datetime, *offset))),
        _ => None,
    }
}

fn time_from_nanos(nanos: i128) -> NaiveTime {
    let nanos = nanos.rem_euclid(NANOS_PER_DAY);
    NaiveTime::from_num_seconds_from_midnight_opt((nanos / NANOS_PER_SECOND) as u32, (nanos % NANOS_PER_SECOND) as u32)
        .expect("a time of day in range")
}

fn add_nanos(datetime: NaiveDateTime, nanos: i128) -> Result<NaiveDateTime, TemporalError> {
    // Both parts take the sign of `nanos`, so the datetime between the two
    // steps lies between the start and the answer: neither step leaves the
    // supported range unless the answer does.
    let seconds = i64::try_from(nanos / NANOS_PER_SECOND).map_err(|_| TemporalError::Overflow)?;
    let fraction = (nanos % NANOS_PER_SECOND) as i64;
    datetime
        .checked_add_signed(ChronoDuration::try_seconds(seconds).ok_or(TemporalError::Overflow)?)
        .and_then(|datetime| datetime.checked_add_signed(ChronoDuration::nanoseconds(fraction)))
        .ok_or(TemporalError::Overflow)
}

fn add_months(datetime: NaiveDateTime, months: i64) -> Result<NaiveDateTime, TemporalError> {
    let magnitude = u32::try_from(months.unsigned_abs()).map_err(|_| TemporalError::Overflow)?;
    // Past a month's end the day is its last (ISO 8601-2 §14.2).
    if months >= 0 {
        datetime.checked_add_months(Months::new(magnitude))
    } else {
        datetime.checked_sub_months(Months::new(magnitude))
    }
    .ok_or(TemporalError::Overflow)
}

/// An instant plus a duration (§20.26 General Rule 4): the instant's own
/// type and displacement. A date moves by whole days, a time of day wraps
/// at midnight, and a year and month-based duration leaves a time of day
/// alone.
pub fn add_duration(instant: TemporalValue, duration: DurationValue) -> Result<TemporalValue, TemporalError> {
    use TemporalValue::*;
    let midnight = NaiveTime::MIN;
    Ok(match (instant, duration) {
        (Duration(_), _) => return Err(TemporalError::NotComparable),
        (Date(date), DurationValue::YearMonth { months }) => Date(add_months(date.and_time(midnight), months)?.date()),
        (Date(date), DurationValue::DayTime { nanoseconds }) => {
            let days = i64::try_from(nanoseconds / NANOS_PER_DAY).map_err(|_| TemporalError::Overflow)?;
            Date(
                date.checked_add_signed(ChronoDuration::try_days(days).ok_or(TemporalError::Overflow)?)
                    .ok_or(TemporalError::Overflow)?,
            )
        }
        (time @ (LocalTime(_) | ZonedTime { .. }), DurationValue::YearMonth { .. }) => time,
        (LocalTime(time), DurationValue::DayTime { nanoseconds }) => {
            LocalTime(time_from_nanos(time_nanos(time) + nanoseconds.rem_euclid(NANOS_PER_DAY)))
        }
        (ZonedTime { time, offset }, DurationValue::DayTime { nanoseconds }) => ZonedTime {
            // A time of day wraps, so only the duration's part of a day moves it.
            time: time_from_nanos(time_nanos(time) + nanoseconds.rem_euclid(NANOS_PER_DAY)),
            offset,
        },
        (LocalDatetime(datetime), DurationValue::YearMonth { months }) => LocalDatetime(add_months(datetime, months)?),
        (LocalDatetime(datetime), DurationValue::DayTime { nanoseconds }) => LocalDatetime(add_nanos(datetime, nanoseconds)?),
        (ZonedDatetime { datetime, offset }, DurationValue::YearMonth { months }) => ZonedDatetime {
            datetime: add_months(datetime, months)?,
            offset,
        },
        (ZonedDatetime { datetime, offset }, DurationValue::DayTime { nanoseconds }) => ZonedDatetime {
            datetime: add_nanos(datetime, nanoseconds)?,
            offset,
        },
    })
}

/// An instant minus a duration, by the same rules as [`add_duration`]. A
/// unit is negated only where it moves the instant: a time of day ignores
/// a year and month-based duration, so `TIME '12:00'` minus the least such
/// duration is 12:00, where negating the duration first would overflow.
/// An amount too large to negate moves no date or datetime within range.
pub fn subtract_duration(instant: TemporalValue, duration: DurationValue) -> Result<TemporalValue, TemporalError> {
    use TemporalValue::*;
    match (instant, duration) {
        (time @ (LocalTime(_) | ZonedTime { .. }), DurationValue::YearMonth { .. }) => Ok(time),
        // A time of day wraps at midnight, so only the duration's part of a
        // day moves it; that part negates without overflow, as the least
        // duration itself would not.
        (time @ (LocalTime(_) | ZonedTime { .. }), DurationValue::DayTime { nanoseconds }) => add_duration(
            time,
            DurationValue::DayTime {
                nanoseconds: -nanoseconds.rem_euclid(NANOS_PER_DAY),
            },
        ),
        (instant, DurationValue::YearMonth { months }) => add_duration(
            instant,
            DurationValue::YearMonth {
                months: months.checked_neg().ok_or(TemporalError::Overflow)?,
            },
        ),
        (instant, DurationValue::DayTime { nanoseconds }) => add_duration(
            instant,
            DurationValue::DayTime {
                nanoseconds: nanoseconds.checked_neg().ok_or(TemporalError::Overflow)?,
            },
        ),
    }
}

/// `DURATION_BETWEEN(from, to)` (§20.28 General Rule 5 d)): `to` minus
/// `from`, in days for dates and in nanoseconds otherwise, then in the unit
/// group asked for. A year and month-based result counts the whole calendar
/// months between, truncating (IA005).
pub fn duration_between(from: TemporalValue, to: TemporalValue, year_month: bool) -> Result<DurationValue, TemporalError> {
    use TemporalValue::*;
    let midnight = NaiveTime::MIN;
    // Each end as a datetime, for whole months, and as its instant in
    // nanoseconds, exact for a span of any length.
    let (from_datetime, to_datetime, from_nanos, to_nanos) = match (from, to) {
        (Date(from), Date(to)) => {
            let (from, to) = (from.and_time(midnight), to.and_time(midnight));
            (Ok(from), Ok(to), utc_datetime_nanos(from, 0), utc_datetime_nanos(to, 0))
        }
        (LocalDatetime(from), LocalDatetime(to)) => (Ok(from), Ok(to), utc_datetime_nanos(from, 0), utc_datetime_nanos(to, 0)),
        (
            ZonedDatetime {
                datetime: from,
                offset: fo,
            },
            ZonedDatetime {
                datetime: to,
                offset: to_offset,
            },
        ) => {
            let utc = |datetime: NaiveDateTime, offset: i32| {
                datetime
                    .checked_sub_signed(ChronoDuration::seconds(i64::from(offset)))
                    .ok_or(TemporalError::Overflow)
            };
            // Shifted only for whole months; the day-time span is the
            // instants', which no datetime's range limits.
            (
                utc(from, fo),
                utc(to, to_offset),
                utc_datetime_nanos(from, fo),
                utc_datetime_nanos(to, to_offset),
            )
        }
        (LocalTime(from), LocalTime(to)) => {
            return Ok(DurationValue::DayTime {
                nanoseconds: time_nanos(to) - time_nanos(from),
            })
        }
        (
            ZonedTime { time: from, offset: fo },
            ZonedTime {
                time: to,
                offset: to_offset,
            },
        ) => {
            return Ok(DurationValue::DayTime {
                nanoseconds: utc_time_nanos(to, to_offset) - utc_time_nanos(from, fo),
            })
        }
        _ => return Err(TemporalError::NotComparable),
    };
    if year_month {
        let (from_datetime, to_datetime) = (from_datetime?, to_datetime?);
        let months = |datetime: NaiveDateTime| i64::from(datetime.year()) * 12 + i64::from(datetime.month0());
        let mut whole = months(to_datetime) - months(from_datetime);
        // A month not yet complete does not count.
        let reached = add_months(from_datetime, whole)?;
        if whole > 0 && reached > to_datetime {
            whole -= 1;
        } else if whole < 0 && reached < to_datetime {
            whole += 1;
        }
        return Ok(DurationValue::YearMonth { months: whole });
    }
    let nanoseconds = to_nanos - from_nanos;
    Ok(DurationValue::DayTime {
        nanoseconds: if matches!(from, Date(_)) {
            nanoseconds / NANOS_PER_DAY * NANOS_PER_DAY
        } else {
            nanoseconds
        },
    })
}

/// A date string (§21.2 SR59), including reduced year/month precision.
///
/// Shared with every other engine and storage path; see `hydradb_cypher_date`.
pub use hydradb_cypher_date::parse_date;

/// A time string (§21.2 Syntax Rule 62): `hh:mm[:ss[.f]]` or
/// `hhmm[ss[.f]]`, and a time shift, `Z`, `±hh`, `±hh:mm` or `±hhmm`, if it
/// has one.
pub fn parse_time(text: &str) -> Option<(NaiveTime, Option<i32>)> {
    let (time, offset) = split_offset(text)?;
    let (whole, fraction) = match time.split_once(['.', ',']) {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (time, None),
    };
    let parts: Vec<&str> = if whole.contains(':') {
        whole.split(':').collect()
    } else {
        whole
            .as_bytes()
            .chunks(2)
            .map(|chunk| std::str::from_utf8(chunk).unwrap_or(""))
            .collect()
    };
    if parts.is_empty()
        || parts.len() > 3
        || parts
            .iter()
            .any(|part| part.len() != 2 || !part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    if fraction.is_some() && parts.len() != 3 {
        return None;
    }
    let number = |index: usize| parts.get(index).map_or(Some(0), |part| part.parse::<u32>().ok());
    let nanos = match fraction {
        None => 0,
        Some(fraction) if !fraction.is_empty() && fraction.len() <= 9 && fraction.bytes().all(|byte| byte.is_ascii_digit()) => {
            format!("{fraction:0<9}").parse::<u32>().ok()?
        }
        Some(_) => return None,
    };
    Some((NaiveTime::from_hms_nano_opt(number(0)?, number(1)?, number(2)?, nanos)?, offset))
}

/// A time with its shift split off: `Z`, or a sign and digits at the end.
fn split_offset(text: &str) -> Option<(&str, Option<i32>)> {
    if let Some(time) = text.strip_suffix(['Z', 'z']) {
        return Some((time, Some(0)));
    }
    let Some(index) = text.rfind(['+', '-']) else {
        return Some((text, None));
    };
    let (time, shift) = (&text[..index], &text[index..]);
    let sign = if shift.starts_with('-') { -1 } else { 1 };
    // Exactly `HH`, `HHMM` or `HH:MM`: a misplaced colon is no offset.
    let digit = |byte: &u8| byte.is_ascii_digit();
    let digits: String = match &shift.as_bytes()[1..] {
        [h1, h2] if digit(h1) && digit(h2) => shift[1..].to_string(),
        [h1, h2, m1, m2] if [h1, h2, m1, m2].into_iter().all(digit) => shift[1..].to_string(),
        [h1, h2, b':', m1, m2] if [h1, h2, m1, m2].into_iter().all(digit) => format!("{}{}", &shift[1..3], &shift[4..6]),
        _ => return None,
    };
    let hours: i32 = digits[..2].parse().ok()?;
    let minutes: i32 = if digits.len() == 4 { digits[2..].parse().ok()? } else { 0 };
    if hours > 18 || minutes > 59 {
        return None;
    }
    Some((time, Some(sign * (hours * 3600 + minutes * 60))))
}

/// A datetime string (§21.2 Syntax Rule 64): a date, `T`, and a time.
pub fn parse_datetime(text: &str) -> Option<(NaiveDateTime, Option<i32>)> {
    let (date, time) = text.split_once(['T', 't'])?;
    let (time, offset) = parse_time(time)?;
    Some((parse_date(date)?.and_time(time), offset))
}

/// A SQL date string (ISO/IEC 9075-2 §5.3, GL12): `years-months-days`, each
/// an unsigned integer, so months and days may have one digit.
pub fn parse_sql_date(text: &str) -> Option<NaiveDate> {
    let [year, month, day] = text.split('-').collect::<Vec<_>>()[..] else {
        return None;
    };
    NaiveDate::from_ymd_opt(i32::try_from(sql_field(year, 4)?).ok()?, sql_field(month, 2)?, sql_field(day, 2)?)
}

/// A field of a SQL datetime string: one to `most` digits.
fn sql_field(part: &str, most: usize) -> Option<u32> {
    if part.is_empty() || part.len() > most || !part.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    part.parse().ok()
}

/// A SQL time string (ISO/IEC 9075-2 §5.3, GL12): `hours:minutes:seconds`,
/// the seconds with an optional fraction, then an optional time zone
/// interval, `±hh:mm`.
pub fn parse_sql_time(text: &str) -> Option<(NaiveTime, Option<i32>)> {
    let (time, offset) = match text.rfind(['+', '-']) {
        Some(index) => {
            let (time, zone) = (&text[..index], &text[index..]);
            let sign = if zone.starts_with('-') { -1 } else { 1 };
            let (hours, minutes) = zone[1..].split_once(':')?;
            let (hours, minutes) = (
                i32::try_from(sql_field(hours, 2)?).ok()?,
                i32::try_from(sql_field(minutes, 2)?).ok()?,
            );
            if hours > 18 || minutes > 59 {
                return None;
            }
            (time, Some(sign * (hours * 3600 + minutes * 60)))
        }
        None => (text, None),
    };
    let [hours, minutes, seconds] = time.split(':').collect::<Vec<_>>()[..] else {
        return None;
    };
    let (seconds, fraction) = match seconds.split_once('.') {
        Some((seconds, fraction)) => (seconds, fraction),
        None => (seconds, ""),
    };
    if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let nanos = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<9}").parse().ok()?
    };
    let time = NaiveTime::from_hms_nano_opt(sql_field(hours, 2)?, sql_field(minutes, 2)?, sql_field(seconds, 2)?, nanos)?;
    Some((time, offset))
}

/// A SQL timestamp string (ISO/IEC 9075-2 §5.3, GL12): a date, a space, and
/// a time.
pub fn parse_sql_timestamp(text: &str) -> Option<(NaiveDateTime, Option<i32>)> {
    let (date, time) = text.split_once(' ')?;
    let (time, offset) = parse_sql_time(time)?;
    Some((parse_sql_date(date)?.and_time(time), offset))
}

/// The ISO 8601 duration string a SQL interval literal is (§21.2 Syntax
/// Rule 72): `text` is the sign written before its string, `+` when none
/// is, then the string; `qualifier` is its interval qualifier. `None` when the string does not
/// have the qualifier's fields (ISO/IEC 9075-2 §5.3 and §10.1).
pub fn sql_interval(text: &str, qualifier: IntervalQualifier) -> Option<String> {
    use IntervalField::{Day, Hour, Minute, Month, Second, Year};
    let end = qualifier.end.unwrap_or(qualifier.start);
    // §10.1 Syntax Rules: the start field is more significant than the end
    // one, and a year-month interval has no day-time field.
    if (qualifier.end.is_some() && qualifier.start >= end) || (qualifier.start <= Month && end > Month) {
        return None;
    }
    let leading = qualifier.leading_precision.unwrap_or(2);
    let fractional = qualifier.fractional_precision.unwrap_or(6);
    if leading == 0 || leading > 18 || fractional > 9 {
        return None;
    }
    // The sign before the string, then an optional one inside it; exactly
    // one minus negates (Syntax Rule 72 h)).
    let (outer, string) = (text.starts_with('-'), text.get(1..)?);
    let (inner, body) = match string.strip_prefix(['+', '-']) {
        Some(body) => (string.starts_with('-'), body),
        None => (false, string),
    };
    let minus = outer != inner;
    // The fields the qualifier spans, and the separator before each after
    // the first.
    let fields: Vec<IntervalField> = [Year, Month, Day, Hour, Minute, Second]
        .into_iter()
        .filter(|field| *field >= qualifier.start && *field <= end)
        .collect();
    let mut values = Vec::new();
    let mut rest = body;
    for (index, field) in fields.iter().enumerate() {
        let separator = match fields.get(index + 1) {
            None => None,
            Some(Month) => Some('-'),
            Some(Hour) => Some(' '),
            Some(_) => Some(':'),
        };
        let (value, after) = match separator {
            Some(separator) => rest.split_once(separator)?,
            None => (rest, ""),
        };
        rest = after;
        values.push((*field, value));
    }
    let mut text = String::from(if minus { "-P" } else { "P" });
    let mut time = String::new();
    for (index, (field, value)) in values.into_iter().enumerate() {
        let (whole, fraction) = match (field, value.split_once('.')) {
            (Second, Some((whole, fraction))) => (whole, Some(fraction)),
            _ => (value, None),
        };
        let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
        if !digits(whole) {
            return None;
        }
        // The leading field has its precision; the others their ranges.
        let number: u64 = whole.parse().ok()?;
        if index == 0 {
            if whole.len() > leading as usize {
                return None;
            }
        } else if whole.len() > 2
            || number
                >= match field {
                    Month => 12,
                    Hour => 24,
                    _ => 60,
                }
        {
            return None;
        }
        if let Some(fraction) = fraction {
            if fraction.len() > fractional as usize || !(fraction.is_empty() || digits(fraction)) {
                return None;
            }
        }
        let amount = match fraction {
            Some(fraction) if !fraction.is_empty() => format!("{number}.{fraction}"),
            _ => number.to_string(),
        };
        match field {
            Year => text.push_str(&format!("{amount}Y")),
            Month => text.push_str(&format!("{amount}M")),
            Day => text.push_str(&format!("{amount}D")),
            Hour => time.push_str(&format!("{amount}H")),
            Minute => time.push_str(&format!("{amount}M")),
            Second => time.push_str(&format!("{amount}S")),
        }
    }
    if !time.is_empty() {
        text.push('T');
        text.push_str(&time);
    }
    Some(text)
}

/// A duration string (§21.2 Syntax Rules 68 and 71): years and months, or
/// days, hours, minutes and seconds, each amount with its own sign, a
/// leading minus negating it all; weeks are seven days.
pub fn parse_duration(text: &str) -> Result<DurationValue, TemporalError> {
    let invalid = TemporalError::InvalidDuration;
    let (negative, body) = match text.strip_prefix('-') {
        Some(body) => (true, body),
        None => (false, text),
    };
    let body = body.strip_prefix(['P', 'p']).ok_or(invalid)?;
    let (date_part, time_part) = match body.split_once(['T', 't']) {
        Some((date, time)) if !time.is_empty() => (date, Some(time)),
        Some(_) => return Err(invalid),
        None => (body, None),
    };
    let mut months: i128 = 0;
    let mut nanos: i128 = 0;
    let (mut year_month, mut day_time) = (false, false);
    let mut fields = 0;
    for (part, in_time) in [(date_part, false), (time_part.unwrap_or(""), true)] {
        let mut rest = part;
        let mut order = 0;
        while !rest.is_empty() {
            let unit_index = rest.find(|character: char| character.is_ascii_alphabetic()).ok_or(invalid)?;
            let (amount, unit) = (
                &rest[..unit_index],
                rest[unit_index..].chars().next().ok_or(invalid)?.to_ascii_uppercase(),
            );
            rest = &rest[unit_index + 1..];
            let (rank, scale, months_unit) = match (in_time, unit) {
                (false, 'Y') => (1, 12, true),
                (false, 'M') => (2, 1, true),
                (false, 'W') => (3, 7 * NANOS_PER_DAY, false),
                (false, 'D') => (4, NANOS_PER_DAY, false),
                (true, 'H') => (5, 3600 * NANOS_PER_SECOND, false),
                (true, 'M') => (6, 60 * NANOS_PER_SECOND, false),
                (true, 'S') => (7, NANOS_PER_SECOND, false),
                _ => return Err(invalid),
            };
            if rank <= order {
                return Err(invalid);
            }
            order = rank;
            fields += 1;
            let (sign, magnitude) = match amount.strip_prefix('-') {
                Some(magnitude) => (-1, magnitude),
                None => (1, amount),
            };
            let (whole, fraction) = match magnitude.split_once(['.', ',']) {
                Some((whole, fraction)) if unit == 'S' && in_time => (whole, fraction),
                Some(_) => return Err(invalid),
                None => (magnitude, ""),
            };
            if whole.is_empty()
                || !whole.bytes().all(|byte| byte.is_ascii_digit())
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
                || fraction.len() > 9
            {
                return Err(invalid);
            }
            let whole: i128 = whole.parse().map_err(|_| TemporalError::Overflow)?;
            // Each amount is checked as it scales and accumulates: a value
            // beyond the range is an overflow, never a wrapped duration.
            let overflow = || TemporalError::Overflow;
            if months_unit {
                year_month = true;
                let amount = whole
                    .checked_mul(scale)
                    .and_then(|amount| amount.checked_mul(sign))
                    .ok_or_else(overflow)?;
                months = months.checked_add(amount).ok_or_else(overflow)?;
            } else {
                day_time = true;
                let fraction: i128 = if fraction.is_empty() {
                    0
                } else {
                    format!("{fraction:0<9}").parse().map_err(|_| invalid)?
                };
                let amount = whole
                    .checked_mul(scale)
                    .and_then(|amount| amount.checked_add(fraction))
                    .and_then(|amount| amount.checked_mul(sign))
                    .ok_or_else(overflow)?;
                nanos = nanos.checked_add(amount).ok_or_else(overflow)?;
            }
        }
    }
    if fields == 0 || year_month == day_time {
        return Err(invalid);
    }
    // The sign is applied before the range is checked: the least duration's
    // magnitude is one past the greatest, so negating it afterwards would
    // refuse a literal the type holds.
    // Checked: two amounts that each fit may sum to the least i128, whose
    // negation does not.
    let (months, nanos) = if negative {
        (
            months.checked_neg().ok_or(TemporalError::Overflow)?,
            nanos.checked_neg().ok_or(TemporalError::Overflow)?,
        )
    } else {
        (months, nanos)
    };
    Ok(if year_month {
        DurationValue::YearMonth {
            months: i64::try_from(months).map_err(|_| TemporalError::Overflow)?,
        }
    } else {
        DurationValue::DayTime { nanoseconds: nanos }
    })
}

/// The current instant of the request (§20.27 General Rule 1), from its
/// nanoseconds since the Unix epoch, at the displacement `offset`.
pub fn request_datetime(nanos_since_epoch: i128, offset: i32) -> Option<NaiveDateTime> {
    let seconds = i64::try_from(nanos_since_epoch.div_euclid(NANOS_PER_SECOND)).ok()?;
    let fraction = nanos_since_epoch.rem_euclid(NANOS_PER_SECOND) as u32;
    chrono::DateTime::from_timestamp(seconds + i64::from(offset), fraction).map(|datetime| datetime.naive_utc())
}

/// Seconds since the Unix epoch, in UTC, and the nanoseconds of the second.
pub fn epoch_seconds(datetime: NaiveDateTime, offset: i32) -> (i64, u32) {
    // From the local timestamp, without shifting the datetime itself past
    // chrono's range: the UTC second is the local one less the offset.
    let local = datetime.and_utc();
    (
        local.timestamp() - i64::from(offset),
        local.timestamp_subsec_nanos().min(999_999_999),
    )
}

/// A time of day as nanoseconds since midnight.
pub fn nanos_of_day(time: NaiveTime) -> i64 {
    time_nanos(time) as i64
}
