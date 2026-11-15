//! The Cypher date-string parser.
//!
//! Every path that turns text into a date uses this function: the
//! experimental engine's `date(...)` and casts, the DataFusion engine's
//! `date(...)`, and stored date properties through
//! `hydradb_graph_types::DateValue::parse_iso`. One parser means a date that
//! can be written can always be queried with the same string, and the two
//! engines cannot drift apart on what they accept.
//!
//! The crate depends only on chrono so that `hydradb-cypher-engine` can use it
//! without taking on Arrow through `hydradb-graph-types`.

use chrono::NaiveDate;

/// A date string (§21.2 SR59), including reduced year/month precision.
///
/// Accepts `YYYY-MM-DD`, `YYYY-MM` (the first of the month), `YYYY` (the first
/// of January), the basic form `YYYYMMDD`, and an optional leading `+` or `-`
/// sign on the year. Years have at least four digits; months and days have
/// exactly two. The day must exist on the proleptic Gregorian calendar.
pub fn parse_date(text: &str) -> Option<NaiveDate> {
    let (sign, digits) = match text.as_bytes().first()? {
        b'+' => (1, &text[1..]),
        b'-' => (-1, &text[1..]),
        _ => (1, text),
    };
    let parts = digits.split('-').collect::<Vec<_>>();
    let (year, month, day) = match parts.as_slice() {
        [year, month, day] => (*year, *month, *day),
        [year, month] => (*year, *month, "01"),
        [year] if year.len() == 4 => (*year, "01", "01"),
        [basic] if basic.len() == 8 && basic.bytes().all(|byte| byte.is_ascii_digit()) => (&basic[..4], &basic[4..6], &basic[6..]),
        _ => return None,
    };
    let all_digits = |part: &str, length: usize| part.len() == length && part.bytes().all(|byte| byte.is_ascii_digit());
    if year.len() < 4 || !year.bytes().all(|byte| byte.is_ascii_digit()) || !all_digits(month, 2) || !all_digits(day, 2) {
        return None;
    }
    NaiveDate::from_ymd_opt(sign * year.parse::<i32>().ok()?, month.parse().ok()?, day.parse().ok()?)
}
