use super::*;

/// Durations chosen to straddle the ladder rather than to be round: one below
/// the 100us floor, one exactly on a bound, several interior, and one past the
/// 30s top bound so the overflow bucket is exercised too. A derivation that
/// only works when every observation lands in a middle bucket is not a
/// derivation.
const RECORDED_MICROS: [u64; 8] = [1, 100, 137, 2_500, 9_999, 480_000, 1_250_000, 45_000_000];

/// `query_rows_duration_us` is no longer stored -- it is `sum_us` off the
/// histogram. The public field kept its name and its type, so it must also keep
/// its exact value: the same integer the old `fetch_add` sum would have held,
/// to the bit, with no rounding introduced by the bucketing.
#[test]
fn query_rows_duration_us_is_the_exact_sum_of_the_recorded_durations() {
    let metrics = GraphOperationalMetrics::default();
    for micros in RECORDED_MICROS {
        metrics.query_rows_latency.record_micros(micros);
    }

    let expected: u64 = RECORDED_MICROS.iter().sum();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.query_rows_duration_us, expected);
    // Derived, not merely equal: the field and the histogram are one quantity,
    // so they cannot be allowed to drift into agreeing only by coincidence.
    assert_eq!(
        snapshot.query_rows_duration_us,
        snapshot.query_rows_latency.sum_us
    );
    assert_eq!(
        snapshot.query_rows_latency.count(),
        RECORDED_MICROS.len() as u64
    );
}

/// With nothing recorded the field must read zero, not the empty histogram's
/// mean or any other stand-in. The old atomic started at zero and callers such
/// as `src/tests.rs`'s `> 0` assertion depend on that.
#[test]
fn query_rows_duration_us_is_zero_before_anything_is_recorded() {
    let snapshot = GraphOperationalMetrics::default().snapshot();

    assert_eq!(snapshot.query_rows_duration_us, 0);
    assert_eq!(
        snapshot.query_rows_latency,
        DurationHistogramSnapshot::default()
    );
}

/// The saturating conversion at the recording sites means a single absurd
/// observation is clamped to `u64::MAX`; the sum then wraps exactly as the old
/// `fetch_add` did. Pinned so a future switch to `saturating_add` inside the
/// histogram is a deliberate, visible change rather than a silent one.
#[test]
fn the_sum_wraps_exactly_as_the_old_fetch_add_did() {
    let metrics = GraphOperationalMetrics::default();
    metrics.query_rows_latency.record_micros(u64::MAX);
    metrics.query_rows_latency.record_micros(3);

    assert_eq!(metrics.snapshot().query_rows_duration_us, 2);
}
