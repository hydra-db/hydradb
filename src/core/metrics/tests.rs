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

/// The enumeration both exports are driven from. Keyed by the Rust identifier
/// and by nothing else: a Prometheus `graph_*` name or an OTel `db.*` name
/// appearing anywhere in this crate is the thing the key is chosen to prevent.
#[test]
fn the_field_enumeration_is_keyed_by_the_rust_identifier() {
    let metrics = GraphOperationalMetrics::default();
    metrics.query_rows_latency.record_micros(137);
    let snapshot = metrics.snapshot();

    let histograms: Vec<&'static str> = snapshot.histogram_fields().map(|(key, _)| key).collect();
    assert_eq!(histograms, vec!["query_rows_latency"]);

    let counters: Vec<(&'static str, u64)> = snapshot.counter_fields().collect();
    assert!(counters
        .iter()
        .any(|(key, value)| *key == "query_rows_duration_us" && *value == 137));
    assert!(
        !counters.iter().any(|(key, _)| *key == "query_rows_latency"),
        "a histogram must not also be enumerated as a counter"
    );
}

/// The enumeration is the *whole* struct. Nothing may be silently unexported
/// because it was left out of one of the three lists -- which the exhaustive
/// destructuring inside `snapshot_fields!` enforces at compile time, and this
/// pins the arithmetic so a field moved from one list to the other is visible.
#[test]
fn every_field_is_enumerated_exactly_once() {
    let snapshot = GraphOperationalMetricsSnapshot::default();
    let counters = snapshot.counter_fields().count();
    let histograms = snapshot.histogram_fields().count();
    let class_counters = snapshot.class_counter_fields().count();

    assert_eq!(counters, 35);
    assert_eq!(histograms, 1);
    // One field, ten rows: the enumeration is already flattened by class.
    assert_eq!(class_counters, GraphError::CLASS_COUNT);

    let mut keys: Vec<&'static str> = snapshot
        .counter_fields()
        .map(|(key, _)| key)
        .chain(snapshot.histogram_fields().map(|(key, _)| key))
        .chain(
            snapshot
                .class_counter_fields()
                .map(|(key, _, _)| key)
                // The rows repeat the field name once per class by design; the
                // duplicate check below is about fields, not rows.
                .take(1),
        )
        .collect();
    let total = keys.len();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), total, "a field is enumerated twice");
}

/// The class dimension, end to end: one failure of each class, and every one of
/// the ten lands in its own slot and in no other. This is the assertion the
/// whole `[AtomicU64; CLASS_COUNT]` shape exists to make true, and a mapping
/// that collapsed two classes onto one index would pass every other test here.
///
/// Gated with the recorder it drives: both of its call sites are
/// `opencypher`-only, so under `default = []` there is no row-query path to
/// fail. `just test-opencypher` and `just test-native` cover it.
#[cfg(feature = "opencypher")]
#[test]
fn each_class_increments_its_own_slot_and_no_other() {
    for (index, error) in GraphError::one_per_class().into_iter().enumerate() {
        let metrics = GraphOperationalMetrics::default();
        metrics.record_query_rows_failure(&error);
        let snapshot = metrics.snapshot();

        let expected: [u64; GraphError::CLASS_COUNT] =
            std::array::from_fn(|slot| u64::from(slot == index));
        assert_eq!(
            snapshot.query_rows_failed_by_class,
            expected,
            "{} landed outside its own slot",
            error.class()
        );
    }
}

/// The scalar and the array are one measurement recorded once, so the array
/// must sum to the scalar for any mix of classes. If the two ever have to be
/// reconciled by a dashboard rather than by construction, this is the guarantee
/// that was lost.
#[cfg(feature = "opencypher")]
#[test]
fn the_per_class_counts_sum_to_the_undimensioned_total() {
    let metrics = GraphOperationalMetrics::default();
    let errors = GraphError::one_per_class();
    // Deliberately lopsided: three of one class and one of another, so a test
    // that only ever sees one observation per slot cannot be what passes.
    for error in errors.iter().take(2) {
        metrics.record_query_rows_failure(error);
    }
    for _ in 0..2 {
        metrics.record_query_rows_failure(&errors[0]);
    }

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.query_rows_failed, 4);
    assert_eq!(
        snapshot.query_rows_failed_by_class.iter().sum::<u64>(),
        snapshot.query_rows_failed
    );
    assert_eq!(snapshot.query_rows_failed_by_class[0], 3);
    assert_eq!(snapshot.query_rows_failed_by_class[1], 1);
}

/// The rows an export layer will consume: the field's Rust identifier, the
/// class *name* -- not its offset -- and the count. Pairing the name with the
/// count inside the enumeration is the point; an export handed an array and a
/// separate vocabulary is an export that can zip them wrongly.
#[cfg(feature = "opencypher")]
#[test]
fn class_counter_rows_carry_the_class_name_beside_its_count() {
    let metrics = GraphOperationalMetrics::default();
    metrics.record_query_rows_failure(&GraphError::QueryParse {
        dialect: "cypher",
        reason: "unexpected token".into(),
    });
    let snapshot = metrics.snapshot();

    let rows: Vec<(&'static str, &'static str, u64)> = snapshot.class_counter_fields().collect();
    assert_eq!(rows.len(), GraphError::CLASS_COUNT);
    assert!(rows
        .iter()
        .all(|(field, _, _)| *field == "query_rows_failed_by_class"));

    let classes: Vec<&'static str> = rows.iter().map(|(_, class, _)| *class).collect();
    assert_eq!(classes, GraphError::CLASSES.to_vec());

    let nonzero: Vec<(&'static str, u64)> = rows
        .iter()
        .filter(|(_, _, count)| *count > 0)
        .map(|(_, class, count)| (*class, *count))
        .collect();
    assert_eq!(nonzero, vec![("query", 1)]);
}

/// A per-class counter is not also a scalar counter, in either direction. The
/// three buckets partition the struct; overlap would double-count on export.
#[test]
fn a_class_counter_is_not_also_enumerated_as_a_scalar() {
    let snapshot = GraphOperationalMetricsSnapshot::default();

    assert!(!snapshot
        .counter_fields()
        .any(|(key, _)| key == "query_rows_failed_by_class"));
    assert!(!snapshot
        .histogram_fields()
        .any(|(key, _)| key == "query_rows_failed_by_class"));
    assert!(!snapshot
        .class_counter_fields()
        .any(|(key, _, _)| key == "query_rows_failed"));
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
