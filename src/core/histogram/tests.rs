use super::*;

/// A ladder that is not strictly increasing makes `partition_point` meaningless
/// and every quantile read off it wrong. Cheap to assert, catastrophic to miss.
#[test]
fn bounds_are_strictly_increasing() {
    for pair in DURATION_BUCKET_BOUNDS_US.windows(2) {
        assert!(
            pair[0] < pair[1],
            "bounds must strictly increase, found {} then {}",
            pair[0],
            pair[1]
        );
    }
}

/// The three rungs the rest of the system cross-checks against. If someone
/// re-cuts the ladder, these are the ones that may not silently disappear.
#[test]
fn bounds_that_other_code_depends_on_are_present() {
    // `slow_query_log_threshold` default: the cumulative count here and the
    // `slow_queries` counter measure the same event.
    assert!(DURATION_BUCKET_BOUNDS_US.contains(&500_000));
    // `DEFAULT_MAX_QUERY_RUNTIME_MS` and `DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS`.
    assert!(DURATION_BUCKET_BOUNDS_US.contains(&30_000_000));
    // The floor.
    assert_eq!(DURATION_BUCKET_BOUNDS_US[0], 100);
}

/// `le` semantics: a value equal to a bound belongs to *that* bound's bucket,
/// not the next one up. Getting this off by one shifts every quantile.
#[test]
fn a_value_equal_to_a_bound_lands_in_that_bounds_bucket() {
    for (index, bound) in DURATION_BUCKET_BOUNDS_US.iter().enumerate() {
        let histogram = AtomicDurationHistogram::default();
        histogram.record_micros(*bound);
        let snapshot = histogram.snapshot();
        assert_eq!(
            snapshot.bucket_counts[index], 1,
            "{bound}us should sit in bucket {index}, got {:?}",
            snapshot.bucket_counts
        );
        assert_eq!(snapshot.count(), 1);
    }
}

/// Every bucket must be reachable. A bucket no value can land in is a bucket
/// carrying no signal, and points at a bad ladder.
#[test]
fn every_bucket_is_reachable() {
    for index in 0..DURATION_BUCKET_COUNT {
        let value = match index {
            0 => 1,
            i if i < DURATION_BUCKET_BOUNDS_US.len() => DURATION_BUCKET_BOUNDS_US[i - 1] + 1,
            // The overflow bucket.
            _ => DURATION_BUCKET_BOUNDS_US[DURATION_BUCKET_BOUNDS_US.len() - 1] + 1,
        };
        let histogram = AtomicDurationHistogram::default();
        histogram.record_micros(value);
        assert_eq!(
            histogram.snapshot().bucket_counts[index],
            1,
            "no value reaches bucket {index}"
        );
    }
}

/// Anything above the ceiling means "timed out" and must land in `+Inf`.
#[test]
fn values_above_the_ceiling_overflow() {
    let histogram = AtomicDurationHistogram::default();
    histogram.record_micros(u64::MAX);
    histogram.record_micros(30_000_001);
    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.bucket_counts[DURATION_BUCKET_COUNT - 1], 2);
    assert_eq!(snapshot.count(), 2);
}

/// Zero is a legitimate observation, not an edge case to reject.
#[test]
fn zero_lands_in_the_first_bucket() {
    let histogram = AtomicDurationHistogram::default();
    histogram.record_micros(0);
    assert_eq!(histogram.snapshot().bucket_counts[0], 1);
}

/// `count()` is derived from the buckets precisely so that it cannot disagree
/// with the `+Inf` cumulative bucket. Assert they are equal.
#[test]
fn derived_count_matches_the_final_cumulative_bucket() {
    let histogram = AtomicDurationHistogram::default();
    for micros in [1, 150, 900, 40_000, 700_000, 45_000_000] {
        histogram.record_micros(micros);
    }
    let snapshot = histogram.snapshot();
    let (bound, cumulative) = snapshot.cumulative().last().expect("at least one bucket");
    assert_eq!(bound, None, "the last cumulative bound is +Inf");
    assert_eq!(cumulative, snapshot.count());
    assert_eq!(snapshot.count(), 6);
}

/// Cumulative counts must never decrease, or `histogram_quantile` produces
/// nonsense.
#[test]
fn cumulative_counts_are_monotonic() {
    let histogram = AtomicDurationHistogram::default();
    for micros in [50, 50, 300, 1_500, 1_500, 1_500, 9_000_000] {
        histogram.record_micros(micros);
    }
    let snapshot = histogram.snapshot();
    let mut previous = 0u64;
    for (_, cumulative) in snapshot.cumulative() {
        assert!(cumulative >= previous, "cumulative went backwards");
        previous = cumulative;
    }
    assert_eq!(previous, snapshot.count());
}

/// The sum is the one statistic the replaced counters already exposed, so it
/// must survive the conversion exactly.
#[test]
fn sum_is_exact() {
    let histogram = AtomicDurationHistogram::default();
    let values = [7u64, 250, 999, 12_345, 6_000_000];
    for micros in values {
        histogram.record_micros(micros);
    }
    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.sum_us, values.iter().sum::<u64>());
    assert_eq!(
        snapshot.mean_us(),
        Some(values.iter().sum::<u64>() as f64 / 5.0)
    );
}

#[test]
fn an_empty_histogram_has_no_mean() {
    let snapshot = AtomicDurationHistogram::default().snapshot();
    assert_eq!(snapshot.count(), 0);
    assert_eq!(snapshot.sum_us, 0);
    assert_eq!(snapshot.mean_us(), None);
    assert_eq!(snapshot, DurationHistogramSnapshot::default());
}

/// `Duration` and raw-microsecond recording must agree, since call sites use
/// whichever is closer to hand.
#[test]
fn duration_and_micros_recording_agree() {
    let from_duration = AtomicDurationHistogram::default();
    from_duration.record(Duration::from_micros(3_500));
    let from_micros = AtomicDurationHistogram::default();
    from_micros.record_micros(3_500);
    assert_eq!(from_duration.snapshot(), from_micros.snapshot());
}

/// Concurrent recording must not lose observations — the whole reason these are
/// atomics rather than a `Mutex`.
#[test]
fn concurrent_records_are_not_lost() {
    let histogram = std::sync::Arc::new(AtomicDurationHistogram::default());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let histogram = std::sync::Arc::clone(&histogram);
        handles.push(std::thread::spawn(move || {
            for _ in 0..1_000 {
                histogram.record_micros(1_234);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker panicked");
    }
    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.count(), 8_000);
    assert_eq!(snapshot.sum_us, 8_000 * 1_234);
}
