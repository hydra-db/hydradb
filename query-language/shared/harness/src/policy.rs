//! Exact divergence matching; raw verdicts are never rewritten as passes.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Divergences {
    pub schema_version: u32,
    pub divergences: Vec<Divergence>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Divergence {
    pub corpus_revision: String,
    pub case_id: String,
    pub engine: String,
    pub layer: String,
    pub expected_signature: String,
    pub reason: String,
    pub owner: String,
    pub reference: String,
    pub review_by: String,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub case_id: String,
    pub engine: String,
    pub layer: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail(String),
    Other,
}

fn valid_date(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes.iter().enumerate().all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
    {
        return false;
    }
    let y: u32 = s[0..4].parse().unwrap();
    let m: u32 = s[5..7].parse().unwrap();
    let d: u32 = s[8..10].parse().unwrap();
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400)) => 29,
        2 => 28,
        _ => 0,
    };
    y > 0 && d > 0 && d <= days
}

/// Return all policy violations so an aggregation gate can report every problem.
pub fn validate(
    policy: &Divergences,
    revision: &str,
    today: &str,
    known_cases: &BTreeSet<String>,
    outcomes: &BTreeMap<Key, Outcome>,
) -> Vec<String> {
    let mut errors = Vec::new();
    if policy.schema_version != 1 {
        errors.push("unsupported divergence schema".into());
    }
    if !valid_date(today) {
        errors.push("invalid evaluation date".into());
    }
    let mut entries = BTreeMap::new();
    for d in &policy.divergences {
        let key = Key {
            case_id: d.case_id.clone(),
            engine: d.engine.clone(),
            layer: d.layer.clone(),
        };
        let fields = [
            &d.corpus_revision,
            &d.case_id,
            &d.engine,
            &d.layer,
            &d.expected_signature,
            &d.reason,
            &d.owner,
            &d.reference,
            &d.review_by,
        ];
        if fields.iter().any(|s| s.trim().is_empty() || s.contains('*')) {
            errors.push(format!("empty or wildcard divergence: {key:?}"));
        }
        if d.corpus_revision != revision {
            errors.push(format!("wrong corpus revision: {key:?}"));
        }
        if !known_cases.contains(&d.case_id) {
            errors.push(format!("unknown case: {key:?}"));
        }
        if !valid_date(&d.review_by) || d.review_by.as_str() < today {
            errors.push(format!("invalid or expired review date: {key:?}"));
        }
        if entries.insert(key.clone(), d).is_some() {
            errors.push(format!("duplicate divergence: {key:?}"));
        }
    }
    for (key, entry) in &entries {
        match outcomes.get(key) {
            Some(Outcome::Fail(signature)) if *signature == entry.expected_signature => {}
            Some(Outcome::Pass) => errors.push(format!("unexpected pass: {key:?}")),
            Some(Outcome::Fail(_)) => errors.push(format!("mismatch signature changed: {key:?}")),
            _ => errors.push(format!("unobserved divergence: {key:?}")),
        }
    }
    for (key, outcome) in outcomes {
        if matches!(outcome, Outcome::Fail(_)) && !entries.contains_key(key) {
            errors.push(format!("unexpected failure: {key:?}"));
        }
    }
    errors
}
