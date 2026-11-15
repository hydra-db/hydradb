//! Turning running totals into "how much since the last report".
//!
//! The node's counters only ever grow, from process start. Prometheus reads the
//! same counters and must not see them reset, so the report never resets them:
//! it keeps a copy of the totals it last sent and subtracts.
//!
//! A total that went *down* is a reset — the process restarted, the counter
//! wrapped, or the scope it belonged to closed and reopened — and is reported
//! as its current value, the rule Prometheus' `rate()` uses. Totals are kept
//! per fine-grained key (a scope, cell and operator, say) and differenced
//! before they are added up, so a scope that closes simply stops contributing
//! instead of pulling the sum below the last report.

use std::collections::BTreeMap;

use crate::report::Histogram;

/// What one counter did since the last read.
pub fn counter_delta(current: u64, previous: u64) -> u64 {
    if current >= previous {
        current - previous
    } else {
        current
    }
}

/// [`counter_delta`] for every key of `current`. A key `previous` lacks is new
/// and counts in full; a key only `previous` has is gone and counts nothing.
pub fn map_delta<K: Ord + Clone>(current: &BTreeMap<K, u64>, previous: &BTreeMap<K, u64>) -> BTreeMap<K, u64> {
    current
        .iter()
        .map(|(key, value)| (key.clone(), counter_delta(*value, previous.get(key).copied().unwrap_or(0))))
        .collect()
}

impl Histogram {
    /// The observations since `previous`. A histogram any of whose buckets or
    /// sum went down was reset, and counts in full.
    pub fn delta(&self, previous: &Self) -> Self {
        let reset = self.sum_us < previous.sum_us
            || self.bucket_counts.len() != previous.bucket_counts.len()
            || self
                .bucket_counts
                .iter()
                .zip(&previous.bucket_counts)
                .any(|(current, previous)| current < previous);
        if reset {
            return self.clone();
        }
        Self {
            bucket_counts: self.bucket_counts.iter().zip(&previous.bucket_counts).map(|(c, p)| c - p).collect(),
            sum_us: self.sum_us - previous.sum_us,
        }
    }

    /// Whether nothing was observed.
    pub fn is_empty(&self) -> bool {
        self.bucket_counts.iter().all(|count| *count == 0)
    }
}
