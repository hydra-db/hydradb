//! Fixed-bucket latency histograms.
//!
//! # Why this exists rather than a dependency
//!
//! The kernel's 65 counters are cheap enough to leave always-on because every
//! one of them is a `Relaxed` [`AtomicU64`] with no allocation and no lock. A
//! latency distribution has to meet the same bar, because the sites that need
//! one — [`crate::client::service`]'s query path, the shard's row-query path —
//! are the hottest paths in the process.
//!
//! The obvious alternatives do not meet it:
//!
//! - OpenTelemetry's `Histogram` takes an `RwLock` read, a
//!   `HashMap<Vec<KeyValue>, _>` lookup and then a `std::sync::Mutex` *per
//!   attribute set* on every `record`. That is a lock, a hash and a `Vec`
//!   allocation in the middle of the query path, and it would make this the
//!   first metric in the codebase that is not lock-free. It also cannot live
//!   here at all: `crates/telemetry` must not become a kernel dependency.
//! - `hdrhistogram` is ~1500 buckets at default precision, which no exposition
//!   format we use can carry.
//! - A DDSketch crate has a *non-fixed* bucket set, which is precisely what
//!   neither Prometheus nor OTel explicit-bucket exposition can represent.
//!
//! So: 18 relaxed counters and a sum. The entire correctness surface is
//! [`AtomicDurationHistogram::bucket_index`] and one constant table.
//!
//! # What this buys, and what it does not
//!
//! With a 2–2.5× ratio ladder, a quantile read out of these buckets is an
//! estimate whose worst-case relative error is bounded by (ratio − 1). In
//! practice that is 20–30%. Concretely: this answers *"is p99 10 ms or
//! 100 ms"* and *"did p99 move by 2×"*. It does **not** answer *"did p99 move
//! from 42 ms to 47 ms"* — that needs a relative-error sketch, and the
//! condition for revisiting is written down in
//! `docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Inclusive upper bounds, in microseconds.
///
/// Prometheus `le` semantics: a value *equal* to a bound belongs to that
/// bound's bucket. Three of these rungs are fixed by the code rather than by
/// taste, and moving them silently breaks a cross-check:
///
/// - **500 ms** is [`crate::query::coordination`]'s `slow_query_log_threshold`
///   default. The cumulative count at this bound and the `slow_queries` counter
///   measure the same event, so they must be reconcilable.
/// - **30 s** is both `DEFAULT_MAX_QUERY_RUNTIME_MS` and
///   `DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS`. Above this bound means "timed out",
///   which is what the overflow bucket should be saying.
/// - **100 µs** is the floor because nothing user-facing in an object-store
///   backed graph completes below it except a pure cache hit. Everything under
///   the floor lands in bucket 0, which is all the resolution that region
///   deserves.
///
/// 17 finite bounds rather than a ~104-bucket quarter-octave ladder: in
/// Prometheus each bound is a series per label set, and the cardinality budget
/// is the constraint the rest of the metrics design is built around.
pub const DURATION_BUCKET_BOUNDS_US: [u64; 17] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000,
];

/// Number of buckets, including the `+Inf` overflow bucket at the end.
pub const DURATION_BUCKET_COUNT: usize = DURATION_BUCKET_BOUNDS_US.len() + 1;

/// A lock-free latency histogram over [`DURATION_BUCKET_BOUNDS_US`].
///
/// `#[repr(align(64))]` is load-bearing rather than decorative: without it the
/// bucket array shares a cache line with whatever the enclosing struct's layout
/// puts beside it, and every neighbouring counter's `fetch_add` false-shares
/// with every observation recorded here.
#[repr(align(64))]
#[derive(Debug, Default)]
pub(crate) struct AtomicDurationHistogram {
    buckets: [AtomicU64; DURATION_BUCKET_COUNT],
    sum_us: AtomicU64,
}

impl AtomicDurationHistogram {
    /// Index of the bucket a microsecond value belongs to.
    ///
    /// `partition_point` counts the bounds strictly below `micros`, which is
    /// exactly the index of the first bound that is `>=` it — so a value equal
    /// to a bound lands in that bound's bucket, per `le` semantics. A value
    /// above every bound yields [`DURATION_BUCKET_BOUNDS_US`]`.len()`, the
    /// overflow bucket.
    #[inline]
    fn bucket_index(micros: u64) -> usize {
        DURATION_BUCKET_BOUNDS_US.partition_point(|bound| *bound < micros)
    }

    /// Record one observation.
    ///
    /// Two independent `Relaxed` increments. They are deliberately not made
    /// atomic with respect to each other: a snapshot taken between them
    /// undercounts `sum_us` by at most the in-flight concurrency, which is
    /// bounded, and the alternative is a lock on the query path. `count` is
    /// derived from the buckets rather than stored, so `_count` and the
    /// `+Inf` bucket agree by construction regardless.
    #[inline]
    pub(crate) fn record_micros(&self, micros: u64) {
        self.buckets[Self::bucket_index(micros)].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
    }

    /// Record one observation from a [`Duration`].
    #[inline]
    pub(crate) fn record(&self, duration: Duration) {
        self.record_micros(crate::codec::duration_micros_u64(duration));
    }

    /// Take a point-in-time copy.
    pub(crate) fn snapshot(&self) -> DurationHistogramSnapshot {
        let mut bucket_counts = [0u64; DURATION_BUCKET_COUNT];
        for (slot, bucket) in bucket_counts.iter_mut().zip(self.buckets.iter()) {
            *slot = bucket.load(Ordering::Relaxed);
        }
        DurationHistogramSnapshot {
            bucket_counts,
            sum_us: self.sum_us.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of an [`AtomicDurationHistogram`].
///
/// Carries *per-bucket* counts, not cumulative ones — that is what the atomics
/// hold and what OTel's histogram data point wants. Prometheus needs the
/// cumulative form, which [`Self::cumulative`] derives. One type, two
/// renderings, so the two exports cannot disagree about `le`.
///
/// The derives are not optional: this lands inside `ClientQueryMetricsSnapshot`
/// and `GraphOperationalMetricsSnapshot`, and transitively inside
/// `GraphShardRuntimeMetrics` and `ScopedGraphShardRuntimeMetrics`, all of
/// which derive all five.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DurationHistogramSnapshot {
    /// Per-bucket observation counts. The final element is the `+Inf` overflow.
    pub bucket_counts: [u64; DURATION_BUCKET_COUNT],
    /// Sum of all observed microseconds.
    pub sum_us: u64,
}

impl DurationHistogramSnapshot {
    /// Total observations, derived from the buckets rather than stored.
    ///
    /// This is why there is no `count` field: deriving it makes `_count` and
    /// the `+Inf` cumulative bucket equal by construction, and removes one
    /// contended atomic from the hot path.
    pub fn count(&self) -> u64 {
        self.bucket_counts.iter().sum()
    }

    /// Cumulative `(bound, count)` pairs in Prometheus `le` order.
    ///
    /// `None` as the bound means `+Inf`, whose count equals [`Self::count`].
    pub fn cumulative(&self) -> impl Iterator<Item = (Option<u64>, u64)> + '_ {
        let mut running = 0u64;
        self.bucket_counts
            .iter()
            .enumerate()
            .map(move |(index, count)| {
                running += *count;
                (DURATION_BUCKET_BOUNDS_US.get(index).copied(), running)
            })
    }

    /// Mean observed microseconds, or `None` with no observations.
    ///
    /// Kept because every duration this type replaces was previously a sum
    /// whose only readable statistic was a mean; nothing that read one should
    /// have to change.
    pub fn mean_us(&self) -> Option<f64> {
        let count = self.count();
        (count > 0).then(|| self.sum_us as f64 / count as f64)
    }
}

#[cfg(test)]
mod tests;
