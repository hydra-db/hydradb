//! Exact decimal numbers (GQL §4.16.5, GV17): a mantissa and a scale, the
//! number being `mantissa / 10^scale`.
//!
//! ID034, ID065, ID066, ID067, IA011: a decimal holds up to 38 digits. The
//! sum or difference has the larger scale of its operands and the product
//! their scales' sum (§20.21 Syntax Rule 2 b)); a quotient has the larger
//! scale plus 6, at most 38. A result is rounded to its scale, a half away
//! from zero, and one with too many digits raises `22003`.

use std::cmp::Ordering;

mod magnitude;
use magnitude::Magnitude;

/// The most digits a decimal holds (ID034).
pub const MAXIMUM_DIGITS: u32 = 38;

/// The scale a quotient gains (ID067).
const QUOTIENT_SCALE: u32 = 6;

/// An exact number with a scale. Two decimals are equal when their values
/// are, whatever their scales: `1.0` is `1`.
#[derive(Clone, Copy, Debug)]
pub struct Decimal {
    pub mantissa: i128,
    pub scale: u32,
}

/// Too many digits for a decimal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalOverflow;

/// Private exact operands, including integers wider than a public decimal.
#[derive(Clone, Copy)]
pub(crate) struct ExactNumber {
    mantissa: i128,
    scale: u32,
}

impl From<i128> for ExactNumber {
    fn from(mantissa: i128) -> Self {
        Self { mantissa, scale: 0 }
    }
}

impl From<Decimal> for ExactNumber {
    fn from(value: Decimal) -> Self {
        Self {
            mantissa: value.mantissa,
            scale: value.scale,
        }
    }
}

impl ExactNumber {
    /// Subtract before rounding to binary64, without restricting the
    /// difference to a public decimal's 38 digits. Aligned i128 operands
    /// at scales 0..=38 and their sum fit in the private 256-bit magnitude.
    pub(crate) fn difference_to_f64(self, other: Self) -> Result<f64, DecimalOverflow> {
        let scale = self.scale.max(other.scale);
        let align = |value: Self| {
            let factor = 10_u128.checked_pow(scale - value.scale).ok_or(DecimalOverflow)?;
            Ok(Magnitude::product(value.mantissa.unsigned_abs(), factor))
        };
        let (left, right) = (align(self)?, align(other)?);
        let (negative, difference) = if (self.mantissa < 0) != (other.mantissa < 0) {
            (self.mantissa < 0, left.checked_add(right))
        } else if left >= right {
            (self.mantissa < 0, left.checked_sub(right))
        } else {
            (self.mantissa >= 0, right.checked_sub(left))
        };
        let magnitude = difference.ok_or(DecimalOverflow)?.to_f64() / 10_u128.checked_pow(scale).ok_or(DecimalOverflow)? as f64;
        Ok(if negative { -magnitude } else { magnitude })
    }
}

fn power(exponent: u32) -> Option<i128> {
    10_i128.checked_pow(exponent)
}

/// The digits of `value`'s magnitude, which for the least i128 no positive
/// i128 holds.
fn digit_count(value: i128) -> u32 {
    let mut magnitude = value.unsigned_abs();
    let mut digits = 1;
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    digits
}

/// `numerator / denominator` to the nearest integer, a half away from zero.
fn divide_rounding(numerator: i128, denominator: i128) -> i128 {
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    if remainder.unsigned_abs() * 2 >= denominator.unsigned_abs() {
        quotient + if (numerator < 0) == (denominator < 0) { 1 } else { -1 }
    } else {
        quotient
    }
}

impl Decimal {
    pub fn new(mantissa: i128, scale: u32) -> Result<Self, DecimalOverflow> {
        let decimal = Self { mantissa, scale };
        if scale > MAXIMUM_DIGITS || digit_count(mantissa) > MAXIMUM_DIGITS {
            return Err(DecimalOverflow);
        }
        Ok(decimal)
    }

    pub fn from_integer(integer: i128) -> Result<Self, DecimalOverflow> {
        Self::new(integer, 0)
    }

    /// The value at `scale`, rounding a half away from zero.
    pub fn rescale(self, scale: u32) -> Result<Self, DecimalOverflow> {
        let mantissa = match scale.cmp(&self.scale) {
            Ordering::Equal => self.mantissa,
            Ordering::Greater => self
                .mantissa
                .checked_mul(power(scale - self.scale).ok_or(DecimalOverflow)?)
                .ok_or(DecimalOverflow)?,
            Ordering::Less => match power(self.scale - scale) {
                Some(divisor) => divide_rounding(self.mantissa, divisor),
                None => 0,
            },
        };
        Self::new(mantissa, scale)
    }

    /// Whether the value is an integer.
    pub fn integer(self) -> Option<i128> {
        let divisor = power(self.scale)?;
        (self.mantissa % divisor == 0).then(|| self.mantissa / divisor)
    }

    /// The integer part, toward zero: what Cypher's `toInteger` keeps.
    pub fn truncate(self) -> i128 {
        power(self.scale).map_or(0, |divisor| self.mantissa / divisor)
    }

    /// The integer nearest the value, a half away from zero.
    pub fn round(self) -> i128 {
        power(self.scale).map_or(0, |divisor| divide_rounding(self.mantissa, divisor))
    }

    pub fn to_f64(self) -> f64 {
        // Through the literal, so the nearest binary64 value is found.
        self.to_string().parse().unwrap_or(f64::NAN)
    }

    /// The decimal of the binary64 value, at the shortest scale that holds
    /// it, rounding past 38 digits.
    pub fn from_f64(float: f64) -> Result<Self, DecimalOverflow> {
        if !float.is_finite() {
            return Err(DecimalOverflow);
        }
        parse(&format!("{float:?}")).ok_or(DecimalOverflow)
    }

    /// Both magnitudes at the larger scale, in 256 bits: an operand scaled
    /// up may pass 38 digits on the way to a result that does not.
    fn aligned_wide(self, other: Self) -> (Magnitude, Magnitude, u32) {
        let scale = self.scale.max(other.scale);
        let widen = |decimal: Self| Magnitude::product(decimal.mantissa.unsigned_abs(), 10_u128.pow(scale - decimal.scale));
        (widen(self), widen(other), scale)
    }

    /// A signed 256-bit magnitude as a decimal at `scale`, if it fits.
    fn from_wide(negative: bool, magnitude: Magnitude, scale: u32) -> Result<Self, DecimalOverflow> {
        let magnitude = magnitude
            .to_u128()
            .and_then(|value| i128::try_from(value).ok())
            .ok_or(DecimalOverflow)?;
        Self::new(if negative { -magnitude } else { magnitude }, scale)
    }

    pub fn checked_add(self, other: Self) -> Result<Self, DecimalOverflow> {
        let (left, right, scale) = self.aligned_wide(other);
        let (left_negative, right_negative) = (self.mantissa < 0, other.mantissa < 0);
        if left_negative == right_negative {
            return Self::from_wide(left_negative, left.checked_add(right).ok_or(DecimalOverflow)?, scale);
        }
        // Opposite signs: the larger magnitude's sign, their difference.
        if left >= right {
            Self::from_wide(left_negative, left.checked_sub(right).ok_or(DecimalOverflow)?, scale)
        } else {
            Self::from_wide(right_negative, right.checked_sub(left).ok_or(DecimalOverflow)?, scale)
        }
    }

    pub fn checked_sub(self, other: Self) -> Result<Self, DecimalOverflow> {
        self.checked_add(other.negate())
    }

    pub fn checked_mul(self, other: Self) -> Result<Self, DecimalOverflow> {
        Self::new(
            self.mantissa.checked_mul(other.mantissa).ok_or(DecimalOverflow)?,
            self.scale + other.scale,
        )
    }

    /// `None` for a zero divisor.
    pub fn checked_div(self, other: Self) -> Option<Result<Self, DecimalOverflow>> {
        if other.mantissa == 0 {
            return None;
        }
        let scale = (self.scale.max(other.scale) + QUOTIENT_SCALE).min(MAXIMUM_DIGITS);
        // self / other at `scale`: mantissa = m1 * 10^shift / m2, with
        // shift = scale + s2 - s1, never negative since scale >= s1.
        let shift = scale + other.scale - self.scale;
        Some(quotient_digits(self.mantissa, other.mantissa, shift).and_then(|mantissa| Self::new(mantissa, scale)))
    }

    /// The remainder of `self / other`, at the larger scale.
    pub fn checked_rem(self, other: Self) -> Option<Result<Self, DecimalOverflow>> {
        if other.mantissa == 0 {
            return None;
        }
        // The remainder takes the dividend's sign, as integer `%` does.
        let (left, right, scale) = self.aligned_wide(other);
        Some(Self::from_wide(self.mantissa < 0, left.remainder(right), scale))
    }

    pub fn negate(self) -> Self {
        Self {
            mantissa: -self.mantissa,
            scale: self.scale,
        }
    }

    pub fn abs(self) -> Self {
        Self {
            mantissa: self.mantissa.abs(),
            scale: self.scale,
        }
    }

    /// The precision the value needs: its digits, at least its scale's.
    pub fn precision(self) -> u32 {
        digit_count(self.mantissa).max(self.scale)
    }
}

/// An exact running total of decimals. The site holding an intermediate
/// result has an implementation-dependent type (UV007): here 256 bits at
/// the largest scale yet seen, so the order the values come in cannot make
/// a sum overflow on its way to a total a decimal holds. Only the total is
/// held to 38 digits.
#[derive(Clone, Copy, Debug, Default)]
pub struct DecimalSum {
    negative: bool,
    magnitude: Magnitude,
    scale: u32,
}

impl DecimalSum {
    pub fn add(&mut self, value: Decimal) -> Result<(), DecimalOverflow> {
        let scale = self.scale.max(value.scale);
        let current = self.magnitude.checked_scale(scale - self.scale).ok_or(DecimalOverflow)?;
        let added = Magnitude::product(value.mantissa.unsigned_abs(), 10_u128.pow(scale - value.scale));
        let added_negative = value.mantissa < 0;
        (self.negative, self.magnitude) = if self.negative == added_negative {
            (self.negative, current.checked_add(added).ok_or(DecimalOverflow)?)
        } else if current >= added {
            (self.negative, current.checked_sub(added).ok_or(DecimalOverflow)?)
        } else {
            (added_negative, added.checked_sub(current).ok_or(DecimalOverflow)?)
        };
        self.scale = scale;
        Ok(())
    }

    /// The total as a decimal, if it fits one.
    pub fn total(self) -> Result<Decimal, DecimalOverflow> {
        Decimal::from_wide(self.negative && self.magnitude != Magnitude::ZERO, self.magnitude, self.scale)
    }

    /// The total divided by `count`, from the wide total, so an average a
    /// decimal holds is not refused because the sum behind it is not. The
    /// scale is the total's plus [`QUOTIENT_SCALE`], as a decimal division
    /// gives, or, where that has too many digits, the largest scale that
    /// fits; the last place rounds a half away from zero.
    pub fn average(self, count: u64) -> Result<Decimal, DecimalOverflow> {
        let count = u128::from(count);
        let negative = self.negative && self.magnitude != Magnitude::ZERO;
        for scale in (0..=(self.scale + QUOTIENT_SCALE).min(MAXIMUM_DIGITS)).rev() {
            // The exact total divided once, and rounded once: scaled up
            // first where the average keeps more places than the total, and
            // otherwise divided by the count and the dropped places together.
            let quotient = if scale >= self.scale {
                self.magnitude
                    .checked_scale(scale - self.scale)
                    .and_then(|numerator| divide_rounded(numerator, count, 1))
            } else {
                10_u128
                    .checked_pow(self.scale - scale)
                    .and_then(|dropped| divide_rounded(self.magnitude, count, dropped))
            };
            let Some(quotient) = quotient else { continue };
            if let Ok(average) = Decimal::from_wide(negative, quotient, scale) {
                return Ok(average);
            }
        }
        Err(DecimalOverflow)
    }
}

/// `numerator / (count * factor)`, rounded once, a half away from zero.
/// Two exact floor divisions compose (`(n / f) / c` is `n / (f * c)` for
/// floors), and the remainder they leave, `r2 * f + r1`, decides the one
/// rounding against the whole divisor, all in 256 bits since `f * c` may
/// pass 128.
fn divide_rounded(numerator: Magnitude, count: u128, factor: u128) -> Option<Magnitude> {
    let (partial, first) = numerator.divide(factor)?;
    let (quotient, second) = partial.divide(count)?;
    let remainder = Magnitude::product(second, factor).checked_add(Magnitude::from(first))?;
    if remainder.checked_add(remainder)? >= Magnitude::product(count, factor) {
        quotient.checked_add(Magnitude::ONE)
    } else {
        Some(quotient)
    }
}

/// A finite binary64's sign, and its magnitude as `m * 2^e`.
fn float_parts(float: f64) -> (bool, u64, i32) {
    let bits = float.to_bits();
    let negative = bits >> 63 == 1;
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1 << 52) - 1);
    if exponent == 0 {
        (negative, fraction, -1074)
    } else {
        (negative, fraction | (1 << 52), exponent - 1075)
    }
}

/// `10^38`, the scale every exact comparison is made at.
const COMPARISON_SCALE: u128 = 100_000_000_000_000_000_000_000_000_000_000_000_000;

impl Decimal {
    /// |self| times 10^38: exact, since a decimal has at most 38 digits.
    fn scaled_magnitude(self) -> Magnitude {
        Magnitude::product(self.mantissa.unsigned_abs(), 10_u128.pow(MAXIMUM_DIGITS - self.scale))
    }

    /// How the decimal orders against an integer, exactly.
    pub fn cmp_integer(self, integer: i128) -> Ordering {
        Self::signed_order(self.mantissa.signum(), integer.signum(), || {
            self.scaled_magnitude()
                .cmp(&Magnitude::product(integer.unsigned_abs(), COMPARISON_SCALE))
        })
    }

    /// How the decimal orders against a float, exactly: against the float's
    /// own binary value, not the decimal's nearest binary64. `None` for NaN.
    pub fn cmp_f64(self, float: f64) -> Option<Ordering> {
        if float.is_nan() {
            return None;
        }
        if float.is_infinite() {
            return Some(if float > 0.0 { Ordering::Less } else { Ordering::Greater });
        }
        let (negative, mantissa, exponent) = float_parts(float);
        let float_sign = if mantissa == 0 {
            0
        } else if negative {
            -1
        } else {
            1
        };
        Some(Self::signed_order(self.mantissa.signum(), float_sign, || {
            // |float| * 10^38, as its floor and whether a fraction is left.
            let scaled = Magnitude::product(u128::from(mantissa), COMPARISON_SCALE);
            let (floor, fraction) = if exponent >= 0 {
                match scaled.checked_shl(exponent as u32) {
                    Some(shifted) => (shifted, false),
                    // Past 256 bits: larger than any decimal.
                    None => return Ordering::Less,
                }
            } else {
                scaled.shr(exponent.unsigned_abs())
            };
            match self.scaled_magnitude().cmp(&floor) {
                Ordering::Equal if fraction => Ordering::Less,
                order => order,
            }
        }))
    }

    /// The order of two signed values from their signs, and from `magnitudes`
    /// when the signs agree and are not zero.
    fn signed_order(left: i128, right: i128, magnitudes: impl FnOnce() -> Ordering) -> Ordering {
        match left.cmp(&right) {
            Ordering::Equal if left == 0 => Ordering::Equal,
            Ordering::Equal if left < 0 => magnitudes().reverse(),
            Ordering::Equal => magnitudes(),
            order => order,
        }
    }

    /// The float's exact binary value rounded to `scale`, a half away from
    /// zero: `CAST(0.1 AS DECIMAL(21, 20))` is 0.10000000000000000555, the
    /// float's own digits, not those of its shortest spelling.
    pub fn from_f64_at_scale(float: f64, scale: u32) -> Result<Self, DecimalOverflow> {
        if !float.is_finite() || scale > MAXIMUM_DIGITS {
            return Err(DecimalOverflow);
        }
        let (negative, mantissa, exponent) = float_parts(float);
        // m * 2^e * 10^scale, in 256 bits.
        let scaled = Magnitude::product(u128::from(mantissa), 10_u128.pow(scale));
        let magnitude = if exponent >= 0 {
            scaled.checked_shl(exponent as u32).ok_or(DecimalOverflow)?
        } else {
            let shift = exponent.unsigned_abs();
            let (floor, _) = scaled.shr(shift);
            // The first bit shifted off is the half.
            let (half, _) = scaled.shr(shift - 1);
            if half.is_odd() {
                floor.checked_add(Magnitude::ONE).ok_or(DecimalOverflow)?
            } else {
                floor
            }
        };
        Self::from_wide(negative, magnitude, scale)
    }

    /// The float's exact value as a decimal, when it has at most 38 digits:
    /// `0.5` is `0.5`, but `0.1`, exactly 0.1000000000000000055..., has none.
    pub fn from_f64_exact(float: f64) -> Option<Self> {
        if !float.is_finite() {
            return None;
        }
        let (negative, mut mantissa, mut exponent) = float_parts(float);
        while exponent < 0 && mantissa != 0 && mantissa % 2 == 0 {
            mantissa /= 2;
            exponent += 1;
        }
        let magnitude: i128 = if exponent >= 0 {
            i128::from(mantissa).checked_mul(2_i128.checked_pow(exponent as u32)?)?
        } else {
            // m / 2^k is m * 5^k / 10^k.
            i128::from(mantissa).checked_mul(5_i128.checked_pow(exponent.unsigned_abs())?)?
        };
        let scale = if exponent >= 0 { 0 } else { exponent.unsigned_abs() };
        Self::new(if negative { -magnitude } else { magnitude }, scale).ok()
    }
}

impl Decimal {
    /// The value without trailing zeros in its fraction: one form for all
    /// the scales a value is written at.
    pub(crate) fn normalized(self) -> (i128, u32) {
        let (mut mantissa, mut scale) = (self.mantissa, self.scale);
        while scale > 0 && mantissa % 10 == 0 {
            mantissa /= 10;
            scale -= 1;
        }
        (mantissa, scale)
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.normalized() == other.normalized()
    }
}

impl Eq for Decimal {}

impl std::hash::Hash for Decimal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.normalized().hash(state);
    }
}

impl Ord for Decimal {
    /// Exact: the magnitudes at one scale, multiplied out in 256 bits, so
    /// two decimals that differ in their last digit never tie.
    fn cmp(&self, other: &Self) -> Ordering {
        let sign = |mantissa: i128| mantissa.signum();
        match sign(self.mantissa).cmp(&sign(other.mantissa)) {
            Ordering::Equal => {}
            unequal => return unequal,
        }
        let scale = self.scale.max(other.scale);
        // Both scales are at most 38, so each factor fits u128.
        let magnitude = |decimal: &Self| Magnitude::product(decimal.mantissa.unsigned_abs(), 10_u128.pow(scale - decimal.scale));
        let order = magnitude(self).cmp(&magnitude(other));
        if self.mantissa < 0 {
            order.reverse()
        } else {
            order
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The exact numeric literal without its sign or suffix: `-1.50`, `12`.
impl std::fmt::Display for Decimal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let digits = self.mantissa.unsigned_abs().to_string();
        let sign = if self.mantissa < 0 { "-" } else { "" };
        let scale = self.scale as usize;
        if scale == 0 {
            return write!(formatter, "{sign}{digits}");
        }
        let digits = format!("{digits:0>width$}", width = scale + 1);
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        write!(formatter, "{sign}{whole}.{fraction}")
    }
}

/// `numerator * 10^shift / denominator` to the nearest integer, a half away
/// from zero, by long division one decimal digit at a time: no intermediate
/// is wider than the remainder times ten, so a quotient that fits is never
/// lost to an overflowing numerator.
fn quotient_digits(numerator: i128, denominator: i128, shift: u32) -> Result<i128, DecimalOverflow> {
    let negative = (numerator < 0) != (denominator < 0);
    let (numerator, denominator) = (numerator.unsigned_abs(), denominator.unsigned_abs());
    let (mut quotient, mut remainder) = (numerator / denominator, numerator % denominator);
    for _ in 0..shift {
        let (digit, rest) = Magnitude::product(remainder, 10).divide(denominator).ok_or(DecimalOverflow)?;
        let digit = digit.to_u128().ok_or(DecimalOverflow)?;
        quotient = quotient
            .checked_mul(10)
            .and_then(|quotient| quotient.checked_add(digit))
            .ok_or(DecimalOverflow)?;
        remainder = rest;
    }
    // A half or more of the last place rounds away from zero; `remainder`
    // is below `denominator`, so this compares 2r with d without doubling.
    if remainder >= denominator - remainder {
        quotient = quotient.checked_add(1).ok_or(DecimalOverflow)?;
    }
    let quotient = i128::try_from(quotient).map_err(|_| DecimalOverflow)?;
    Ok(if negative { -quotient } else { quotient })
}

/// An unsigned decimal in common or scientific notation, underscores and a
/// trailing `M` removed: `1.50`, `.5`, `2`, `1.5E2`. The scale is the
/// digits after the period less the exponent, never below zero.
pub fn parse(text: &str) -> Option<Decimal> {
    parse_rounded(text, MAXIMUM_DIGITS).ok()
}

/// Why a decimal's text could not be read: it is not a decimal at all, or it
/// is one with more digits than a decimal holds. A cast tells them apart
/// (`22018` and `22003`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalParseError {
    Malformed,
    Overflow,
}

/// As [`parse`], with any fraction past `scale` rounded off once, a half
/// away from zero, in decimal: a cast to DECIMAL(p, s) reads its string at
/// s directly, never rounding at 38 digits first and at s again.
pub fn parse_at_scale(text: &str, scale: u32) -> Result<Decimal, DecimalParseError> {
    parse_rounded(text, scale.min(MAXIMUM_DIGITS))
}

fn parse_rounded(text: &str, limit: u32) -> Result<Decimal, DecimalParseError> {
    use hydradb_cypher_ast::numeric::{decimal_spelling, separated_digits};
    use DecimalParseError::{Malformed, Overflow};
    let normalized = decimal_spelling(text);
    let text = normalized.as_str();
    let text = text.strip_suffix(['m', 'M']).unwrap_or(text);
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(index) => {
            let exponent = &text[index + 1..];
            let (sign, digits) = match exponent.strip_prefix(['+', '-']) {
                Some(digits) => (&exponent[..1], digits),
                None => ("", exponent),
            };
            let digits = separated_digits(digits, 10).ok_or(Malformed)?;
            (&text[..index], format!("{sign}{digits}").parse::<i64>().map_err(|_| Malformed)?)
        }
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let part = |part: &str| {
        if part.is_empty() {
            Ok(String::new())
        } else {
            separated_digits(part, 10).ok_or(Malformed)
        }
    };
    let whole = part(whole)?;
    let fraction = part(fraction)?;
    let digits = format!("{whole}{fraction}");
    if digits.is_empty() {
        return Err(Malformed);
    }
    let fraction_digits = fraction.len() as i64;
    let mut digits = digits.trim_start_matches('0').to_string();
    if digits.is_empty() {
        digits.push('0');
    }
    let mut scale = fraction_digits.checked_sub(exponent).ok_or(Overflow)?;
    let maximum = i64::from(MAXIMUM_DIGITS);
    let limit = i64::from(limit);
    // Too long for a decimal, or finer than the limit: trailing fraction
    // zeros go first, exactly, so `1.000...0` with forty zeros is still one.
    while (digits.len() as i64 > maximum || scale > limit) && scale > 0 && digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
        scale -= 1;
    }
    // Still more fraction than the limit: round it there, once.
    if scale > limit {
        digits = round_off(&digits, usize::try_from(scale - limit).map_err(|_| Overflow)?);
        scale = limit;
    }
    let mut value: i128 = digits.parse().map_err(|_| Overflow)?;
    // Zero is zero at any exponent: `0E40` needs no power of ten.
    if value == 0 {
        scale = scale.max(0);
    }
    if scale < 0 {
        value = value
            .checked_mul(power(u32::try_from(-scale).map_err(|_| Overflow)?).ok_or(Overflow)?)
            .ok_or(Overflow)?;
        scale = 0;
    }
    Decimal::new(if negative { -value } else { value }, u32::try_from(scale).map_err(|_| Overflow)?).map_err(|_| Overflow)
}

/// `digits` with its last `drop` digits rounded off, a half away from zero.
fn round_off(digits: &str, drop: usize) -> String {
    if drop >= digits.len() {
        let half = drop == digits.len() && digits.as_bytes()[0] >= b'5';
        return if half { "1" } else { "0" }.to_string();
    }
    let (kept, dropped) = digits.split_at(digits.len() - drop);
    let mut kept = kept.as_bytes().to_vec();
    if dropped.as_bytes()[0] >= b'5' {
        let mut index = kept.len();
        loop {
            if index == 0 {
                kept.insert(0, b'1');
                break;
            }
            index -= 1;
            if kept[index] == b'9' {
                kept[index] = b'0';
            } else {
                kept[index] += 1;
                break;
            }
        }
    }
    String::from_utf8(kept).expect("ASCII digits")
}
