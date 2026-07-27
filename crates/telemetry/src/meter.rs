//! The meter side of the pipeline: observable instruments fed from cached
//! snapshots, and the histogram family that exists because OpenTelemetry has no
//! observable histogram.
//!
//! # Why a family of counters and not a histogram
//!
//! The kernel computes its own fixed-bound duration histogram — a `[u64; 18]`
//! of bucket counts plus a sum of microseconds, updated with one relaxed
//! `fetch_add` per observation. Getting that into OTLP hits two absent
//! features, not two awkward ones:
//!
//! - `opentelemetry` 0.32 has `u64_observable_counter`,
//!   `f64_observable_up_down_counter` and `u64_observable_gauge`. For
//!   histograms it has only the **synchronous** `f64_histogram` and
//!   `u64_histogram`. There is no observable histogram in the API.
//! - `opentelemetry_sdk` 0.32 has no `MetricProducer` trait, so an external
//!   source of already-aggregated data points cannot be registered against a
//!   reader either.
//!
//! So the buckets leave as a family of [`opentelemetry::metrics::ObservableCounter`]s
//! carrying an [`crate::semconv::LE`] label — one series per bucket — plus a
//! `.sum` and a `.count`. That is precisely what a Prometheus histogram *is*,
//! `histogram_quantile` over the family works unchanged, and after a
//! collector's OTLP→Prometheus conversion the series are named
//! `…_bucket{le="…"}`, `…_sum` and `…_count`, which is the shape every existing
//! query expects.
//!
//! The cost is real and belongs in the runbook: a vendor's *native* latency
//! widget looks for a histogram data point and will not light up on a family of
//! sums. That is a property of the SDK, not of the metric's name, and it is
//! paid whatever the instrument is called.
//!
//! # Why not a synchronous `Histogram` fed from the snapshot
//!
//! Because a snapshot is cumulative and `Histogram::record` is not. Replaying
//! the delta between two snapshots into a synchronous histogram would mean
//! `record`ing a value the process never measured — the bucket *index* is
//! recoverable from a snapshot but the *value* is not, so every replayed
//! observation would have to be invented at, say, the bucket's upper bound.
//! That is not a rounding error; it moves every quantile to a bucket edge and
//! makes the sum disagree with the buckets by construction. It also
//! re-introduces the per-observation lock the kernel's array exists to avoid:
//! `ValueMap::measure` takes an `RwLock` read, hashes an attribute `Vec` and
//! then takes a `Mutex` per attribute set.
//!
//! # The cross-crate contract
//!
//! This crate must not name a kernel type — its `Cargo.toml` forbids the
//! dependency, and the arrow pointing this way is what keeps the kernel free of
//! OpenTelemetry. So the entire interface is three pieces of plain data:
//!
//! 1. `&[u64]` — the finite bucket upper bounds, in **microseconds**, ascending
//!    ([`ObservableHistogram::register`]).
//! 2. `&[u64]` — the bucket counts, one per bound plus one overflow bucket.
//! 3. `u64` — the sum, in microseconds.
//!
//! ([`ObservableHistogram::record_snapshot`] for the last two.) The kernel owns
//! the ladder and the arithmetic; this module owns the exposition.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use thiserror::Error;

use crate::semconv::{MetricLabel, LE};

/// The unit a histogram's bounds and sum are **exported** in.
///
/// The kernel measures in microseconds and only ever measures in microseconds;
/// this is a conversion applied at the export boundary and nowhere else.
///
/// [`HistogramUnit::Seconds`] exists for exactly one instrument.
/// `db.client.operation.duration` is a stable OTel semantic convention and
/// semconv fixes it in seconds, so a series claiming that name must be in
/// seconds or it is worse than a series with a Turbolay name. Everything else
/// keeps microseconds under a `turbolay.*` name. A bound table in seconds must
/// never leak back towards the kernel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistogramUnit {
    /// Export as the kernel measures: microseconds. UCUM `us`.
    Microseconds,
    /// Divide by 1e6 at the boundary. UCUM `s`.
    Seconds,
}

impl HistogramUnit {
    /// The UCUM unit annotation for the `.sum` instrument.
    pub fn ucum(self) -> &'static str {
        match self {
            Self::Microseconds => "us",
            Self::Seconds => "s",
        }
    }

    /// Render one finite bucket bound as an `le` label value.
    pub fn render_bound(self, bound_us: u64) -> String {
        match self {
            Self::Microseconds => bound_us.to_string(),
            // `{}` on f64 is the shortest representation that round-trips, so
            // 100 µs renders as `0.0001` and 30 s as `30` — not `0.000100000`
            // and not `3.0e1`. Two nodes rendering the same bound must produce
            // the same string, or the backend sees two series.
            Self::Seconds => (bound_us as f64 / 1_000_000.0).to_string(),
        }
    }

    /// Scale a microsecond sum into the export unit.
    pub fn scale_sum(self, sum_us: u64) -> f64 {
        match self {
            Self::Microseconds => sum_us as f64,
            Self::Seconds => sum_us as f64 / 1_000_000.0,
        }
    }
}

/// The `le` value of the overflow bucket.
pub const LE_INFINITY: &str = "+Inf";

/// What an exported histogram family is called and what it means.
///
/// The name is the OTel metric name *stem*: three instruments are derived from
/// it, `{name}.bucket`, `{name}.sum` and `{name}.count`.
#[derive(Clone, Copy, Debug)]
pub struct HistogramSpec {
    /// Metric name stem. `db.*` where a semantic convention genuinely exists,
    /// `turbolay.*` otherwise.
    pub name: &'static str,
    /// One-line description, exported as the instrument description.
    pub description: &'static str,
    /// The unit the bounds and the sum are rendered in.
    pub unit: HistogramUnit,
}

/// Why a snapshot or a ladder was rejected.
///
/// Everything here is a programming error rather than a runtime condition, and
/// none of it is a panic: these are reported from a collection task that runs
/// forever, and a panicking metrics task takes the node's telemetry with it for
/// the rest of the process's life.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum HistogramError {
    /// The bucket bounds were empty, or not strictly ascending. A cumulative
    /// rendering of a non-monotonic ladder is meaningless.
    #[error("histogram {name}: bucket bounds must be non-empty and strictly ascending")]
    Bounds {
        /// The metric name stem.
        name: &'static str,
    },

    /// The snapshot did not have one count per bound plus one overflow bucket.
    #[error(
        "histogram {name}: expected {expected} bucket counts (bounds + overflow), got {found}"
    )]
    BucketCount {
        /// The metric name stem.
        name: &'static str,
        /// Bounds plus one.
        expected: usize,
        /// What the caller passed.
        found: usize,
    },

    /// The same label key was supplied twice for one series.
    #[error("histogram {name}: label {key} was supplied twice")]
    DuplicateLabel {
        /// The metric name stem.
        name: &'static str,
        /// The repeated key.
        key: &'static str,
    },

    /// A caller tried to supply `le`, which this module owns.
    #[error("histogram {name}: {LE} is reserved for the bucket bound")]
    ReservedLabel {
        /// The metric name stem.
        name: &'static str,
    },
}

/// One recorded series: the label values it is keyed by, its bucket counts and
/// its sum.
#[derive(Clone, Debug, Default)]
struct Series {
    buckets: Vec<u64>,
    sum_us: u64,
}

/// Label values, canonicalised by sorting on the key so that two callers
/// passing the same labels in different orders address the same series.
type SeriesKey = Vec<(&'static str, String)>;

type SeriesMap = HashMap<SeriesKey, Series>;

/// One observation the callbacks report: an attribute set and a value.
///
/// Public so the rendering can be asserted on directly. Standing up an
/// `SdkMeterProvider` and a collector to find out whether the buckets are
/// cumulative is a test of the SDK, not of this code.
#[derive(Clone, Debug)]
pub struct Observation<T> {
    /// The attribute set, including `le` for bucket observations.
    pub attributes: Vec<KeyValue>,
    /// The reported value.
    pub value: T,
}

/// A kernel-side histogram, exported as an observable counter family.
///
/// Registration builds the three instruments; [`Self::record_snapshot`]
/// publishes the latest values. The two are deliberately decoupled: the
/// collection task is `async` (the snapshot is behind a mutex-taking `async fn`
/// in the kernel) and an OTel callback is a plain `Fn` run on the SDK's own OS
/// thread, so the callback must never do anything but read.
///
/// A series that stops being recorded keeps reporting its last values. That is
/// the right behaviour for a cumulative counter — a gap would look like a reset
/// and produce a spurious `rate()` spike — but it does mean a shard that closes
/// leaves its series frozen until the process exits.
#[derive(Debug)]
pub struct ObservableHistogram {
    name: &'static str,
    bucket_count: usize,
    state: Arc<RwLock<SeriesMap>>,
    le_values: Arc<[String]>,
    unit: HistogramUnit,
}

impl ObservableHistogram {
    /// Register the three instruments on `meter` and return the handle the
    /// collection task records into.
    ///
    /// `bounds` is the kernel's ladder: finite bucket upper bounds in
    /// microseconds, strictly ascending. It is passed in rather than defined
    /// here because the kernel owns it — exporting it once from there is what
    /// stops the Prometheus rendering and the OTLP rendering from disagreeing
    /// about where a bucket ends.
    pub fn register(
        meter: &Meter,
        spec: HistogramSpec,
        bounds: &[u64],
    ) -> Result<Self, HistogramError> {
        if bounds.is_empty() || bounds.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(HistogramError::Bounds { name: spec.name });
        }

        let mut le_values: Vec<String> = bounds
            .iter()
            .map(|bound| spec.unit.render_bound(*bound))
            .collect();
        le_values.push(LE_INFINITY.to_string());
        let le_values: Arc<[String]> = le_values.into();

        let histogram = Self {
            name: spec.name,
            bucket_count: bounds.len() + 1,
            state: Arc::new(RwLock::new(HashMap::new())),
            le_values,
            unit: spec.unit,
        };

        // Each instrument reads the same cached map. The handles are dropped:
        // in 0.32 an `ObservableCounter` is a `PhantomData` marker and the
        // callback lives in the meter's pipeline, so keeping them buys nothing.
        let buckets = histogram.shallow_clone();
        let _ = meter
            .u64_observable_counter(format!("{}.bucket", spec.name))
            .with_description(format!(
                "{} — cumulative count of observations at or below the le bound",
                spec.description
            ))
            .with_unit("1")
            .with_callback(move |observer| {
                for observation in buckets.bucket_observations() {
                    observer.observe(observation.value, &observation.attributes);
                }
            })
            .build();

        let sums = histogram.shallow_clone();
        let _ = meter
            .f64_observable_counter(format!("{}.sum", spec.name))
            .with_description(format!("{} — sum of observed durations", spec.description))
            .with_unit(spec.unit.ucum())
            .with_callback(move |observer| {
                for observation in sums.sum_observations() {
                    observer.observe(observation.value, &observation.attributes);
                }
            })
            .build();

        let counts = histogram.shallow_clone();
        let _ = meter
            .u64_observable_counter(format!("{}.count", spec.name))
            .with_description(format!("{} — number of observations", spec.description))
            .with_unit("1")
            .with_callback(move |observer| {
                for observation in counts.count_observations() {
                    observer.observe(observation.value, &observation.attributes);
                }
            })
            .build();

        Ok(histogram)
    }

    /// Publish the latest snapshot for one series.
    ///
    /// This is the whole cross-crate contract: label values, per-bucket counts
    /// (**not** cumulative — the cumulative rendering happens here, once) and a
    /// microsecond sum. Nothing about the kernel's types appears in the
    /// signature.
    ///
    /// Callers must not pass [`crate::semconv::LE`]; this module owns it.
    pub fn record_snapshot(
        &self,
        labels: &[(MetricLabel, &str)],
        buckets: &[u64],
        sum_us: u64,
    ) -> Result<(), HistogramError> {
        if buckets.len() != self.bucket_count {
            return Err(HistogramError::BucketCount {
                name: self.name,
                expected: self.bucket_count,
                found: buckets.len(),
            });
        }

        let mut key: SeriesKey = labels
            .iter()
            .map(|(label, value)| (label.key(), (*value).to_string()))
            .collect();
        if key.iter().any(|(name, _)| *name == LE) {
            return Err(HistogramError::ReservedLabel { name: self.name });
        }
        key.sort_unstable();
        if let Some(duplicate) = key
            .windows(2)
            .find(|pair| pair[0].0 == pair[1].0)
            .map(|pair| pair[0].0)
        {
            return Err(HistogramError::DuplicateLabel {
                name: self.name,
                key: duplicate,
            });
        }

        let series = Series {
            buckets: buckets.to_vec(),
            sum_us,
        };
        self.write().insert(key, series);
        Ok(())
    }

    /// How many series this histogram is currently reporting.
    ///
    /// The cardinality of one instrument family is `series_count() ×
    /// (bucket_count + 2)`, which is the number worth watching before adding a
    /// dimension rather than after.
    pub fn series_count(&self) -> usize {
        self.read().len()
    }

    /// Number of buckets, including the `+Inf` overflow.
    pub fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    /// What the `.bucket` callback reports: one observation per series per
    /// bucket, **cumulative**, keyed by `le`.
    ///
    /// Cumulative rather than per-bucket because that is what `le` means and
    /// what `histogram_quantile` assumes. The kernel counts per bucket, since
    /// that is one `fetch_add`; the accumulation is a rendering concern and
    /// lives here, once, so the two exports cannot disagree about it.
    pub fn bucket_observations(&self) -> Vec<Observation<u64>> {
        let series = self.read();
        let mut out = Vec::with_capacity(series.len() * self.bucket_count);
        for (key, entry) in series.iter() {
            if entry.buckets.len() != self.bucket_count {
                continue;
            }
            let mut running = 0u64;
            for (index, count) in entry.buckets.iter().enumerate() {
                running = running.saturating_add(*count);
                let mut attributes = attributes_of(key);
                attributes.push(KeyValue::new(LE, self.le_values[index].clone()));
                out.push(Observation {
                    attributes,
                    value: running,
                });
            }
        }
        out
    }

    /// What the `.sum` callback reports, in the export unit.
    pub fn sum_observations(&self) -> Vec<Observation<f64>> {
        self.read()
            .iter()
            .map(|(key, entry)| Observation {
                attributes: attributes_of(key),
                value: self.unit.scale_sum(entry.sum_us),
            })
            .collect()
    }

    /// What the `.count` callback reports.
    ///
    /// Derived by summing the buckets rather than carried as its own counter,
    /// so `.count` and `le="+Inf"` agree by construction instead of by two
    /// `fetch_add`s staying in step.
    pub fn count_observations(&self) -> Vec<Observation<u64>> {
        self.read()
            .iter()
            .map(|(key, entry)| Observation {
                attributes: attributes_of(key),
                value: entry
                    .buckets
                    .iter()
                    .copied()
                    .fold(0u64, u64::saturating_add),
            })
            .collect()
    }

    /// A handle onto the same state, for the callbacks.
    fn shallow_clone(&self) -> Self {
        Self {
            name: self.name,
            bucket_count: self.bucket_count,
            state: Arc::clone(&self.state),
            le_values: Arc::clone(&self.le_values),
            unit: self.unit,
        }
    }

    /// Lock poisoning is recovered from rather than propagated. The only writer
    /// is `record_snapshot`, which cannot panic while holding the lock, and a
    /// metrics pipeline that stops for the life of the process because
    /// something else panicked is a worse outcome than a stale series.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, SeriesMap> {
        self.state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, SeriesMap> {
        self.state
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

fn attributes_of(key: &SeriesKey) -> Vec<KeyValue> {
    key.iter()
        .map(|(name, value)| KeyValue::new(*name, value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semconv::{L_CELL_ID, L_DB_SYSTEM_NAME};

    /// A three-bound ladder: four buckets with the overflow.
    const BOUNDS: &[u64] = &[100, 1_000, 30_000_000];

    fn spec(unit: HistogramUnit) -> HistogramSpec {
        HistogramSpec {
            name: "turbolay.test.duration",
            description: "test",
            unit,
        }
    }

    /// Registration needs a `Meter`, and a `Meter` with no provider behind it
    /// is a no-op — which is exactly what these tests want, since they assert
    /// on the observation vectors rather than on what a collector received.
    fn histogram(unit: HistogramUnit) -> ObservableHistogram {
        let meter = opentelemetry::global::meter("turbolay-telemetry-tests");
        ObservableHistogram::register(&meter, spec(unit), BOUNDS).expect("bounds are ascending")
    }

    fn le_of(observation: &Observation<u64>) -> String {
        observation
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == LE)
            .map(|attribute| attribute.value.to_string())
            .expect("bucket observations carry le")
    }

    #[test]
    fn buckets_are_rendered_cumulatively_and_end_at_infinity() {
        let histogram = histogram(HistogramUnit::Microseconds);
        histogram
            .record_snapshot(&[(L_CELL_ID, "cell-7")], &[3, 4, 0, 1], 900)
            .expect("shape matches");

        let observations = histogram.bucket_observations();
        assert_eq!(observations.len(), 4);
        let rendered: Vec<(String, u64)> = observations
            .iter()
            .map(|observation| (le_of(observation), observation.value))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("100".to_string(), 3),
                ("1000".to_string(), 7),
                ("30000000".to_string(), 7),
                ("+Inf".to_string(), 8),
            ]
        );
    }

    /// `_count` and `le="+Inf"` must agree by construction, not by luck. This
    /// is the property the kernel's missing `count` field buys.
    #[test]
    fn count_equals_the_infinity_bucket() {
        let histogram = histogram(HistogramUnit::Microseconds);
        histogram
            .record_snapshot(&[(L_CELL_ID, "cell-1")], &[1, 2, 3, 4], 12_345)
            .expect("shape matches");

        let infinity = histogram
            .bucket_observations()
            .into_iter()
            .find(|observation| le_of(observation) == LE_INFINITY)
            .expect("an overflow bucket");
        let count = histogram.count_observations();
        assert_eq!(count.len(), 1);
        assert_eq!(count[0].value, infinity.value);
        assert_eq!(count[0].value, 10);
    }

    /// The one instrument with a stable semantic-convention name is fixed in
    /// seconds while the kernel measures in microseconds. The conversion is
    /// here and nowhere else.
    #[test]
    fn seconds_conversion_happens_at_the_boundary() {
        let histogram = histogram(HistogramUnit::Seconds);
        histogram
            .record_snapshot(&[], &[1, 0, 0, 0], 2_500_000)
            .expect("shape matches");

        let bounds: Vec<String> = histogram.bucket_observations().iter().map(le_of).collect();
        assert_eq!(bounds, vec!["0.0001", "0.001", "30", "+Inf"]);
        assert_eq!(histogram.sum_observations()[0].value, 2.5);
    }

    #[test]
    fn microseconds_are_rendered_as_integers() {
        let histogram = histogram(HistogramUnit::Microseconds);
        histogram
            .record_snapshot(&[], &[0, 0, 0, 0], 7)
            .expect("shape matches");
        assert_eq!(histogram.sum_observations()[0].value, 7.0);
        assert_eq!(
            histogram.bucket_observations()[0]
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == LE)
                .map(|attribute| attribute.value.to_string()),
            Some("100".to_string())
        );
    }

    /// A snapshot of the wrong width is a mismatch between the kernel's ladder
    /// and the registered one. It must be an error the collection task can log,
    /// not a panic on the SDK's thread.
    #[test]
    fn a_mismatched_ladder_is_an_error_not_a_panic() {
        let histogram = histogram(HistogramUnit::Microseconds);
        let error = histogram
            .record_snapshot(&[], &[1, 2, 3], 0)
            .expect_err("three counts for four buckets");
        assert_eq!(
            error,
            HistogramError::BucketCount {
                name: "turbolay.test.duration",
                expected: 4,
                found: 3,
            }
        );
        assert!(histogram.bucket_observations().is_empty());
    }

    #[test]
    fn bounds_must_be_non_empty_and_ascending() {
        let meter = opentelemetry::global::meter("turbolay-telemetry-tests");
        for bad in [&[][..], &[100, 100][..], &[1_000, 100][..]] {
            assert!(matches!(
                ObservableHistogram::register(&meter, spec(HistogramUnit::Microseconds), bad),
                Err(HistogramError::Bounds { .. })
            ));
        }
    }

    /// Labels are a set, not a sequence. Two callers ordering them differently
    /// must address one series, or a dashboard silently doubles.
    #[test]
    fn label_order_does_not_create_a_second_series() {
        let histogram = histogram(HistogramUnit::Microseconds);
        histogram
            .record_snapshot(
                &[(L_CELL_ID, "cell-3"), (L_DB_SYSTEM_NAME, "neo4j")],
                &[1, 0, 0, 0],
                10,
            )
            .expect("shape matches");
        histogram
            .record_snapshot(
                &[(L_DB_SYSTEM_NAME, "neo4j"), (L_CELL_ID, "cell-3")],
                &[2, 0, 0, 0],
                20,
            )
            .expect("shape matches");

        assert_eq!(histogram.series_count(), 1);
        assert_eq!(histogram.count_observations()[0].value, 2);
    }

    #[test]
    fn distinct_label_values_are_distinct_series() {
        let histogram = histogram(HistogramUnit::Microseconds);
        for cell in ["cell-1", "cell-2", "cell-3"] {
            histogram
                .record_snapshot(&[(L_CELL_ID, cell)], &[1, 0, 0, 0], 5)
                .expect("shape matches");
        }
        assert_eq!(histogram.series_count(), 3);
        assert_eq!(
            histogram.bucket_observations().len(),
            3 * histogram.bucket_count()
        );
    }

    #[test]
    fn a_duplicate_label_is_rejected() {
        let histogram = histogram(HistogramUnit::Microseconds);
        let error = histogram
            .record_snapshot(
                &[(L_CELL_ID, "cell-1"), (L_CELL_ID, "cell-2")],
                &[1, 0, 0, 0],
                1,
            )
            .expect_err("one key, two values");
        assert!(matches!(error, HistogramError::DuplicateLabel { .. }));
    }

    /// Re-recording replaces the series rather than accumulating. The source is
    /// cumulative already; adding to it would double-count every interval.
    #[test]
    fn recording_twice_replaces_rather_than_accumulates() {
        let histogram = histogram(HistogramUnit::Microseconds);
        histogram
            .record_snapshot(&[(L_CELL_ID, "cell-9")], &[5, 0, 0, 0], 500)
            .expect("shape matches");
        histogram
            .record_snapshot(&[(L_CELL_ID, "cell-9")], &[6, 0, 0, 0], 600)
            .expect("shape matches");

        assert_eq!(histogram.series_count(), 1);
        assert_eq!(histogram.count_observations()[0].value, 6);
        assert_eq!(histogram.sum_observations()[0].value, 600.0);
    }

    #[test]
    fn nothing_recorded_reports_nothing() {
        let histogram = histogram(HistogramUnit::Microseconds);
        assert!(histogram.bucket_observations().is_empty());
        assert!(histogram.sum_observations().is_empty());
        assert!(histogram.count_observations().is_empty());
    }
}
