//! The meter side of the pipeline: observable instruments fed from cached
//! snapshots, and the histogram family that exists because OpenTelemetry has no
//! observable histogram.
//!
//! Two instrument kinds live here, and they share a shape rather than an
//! implementation. [`ObservableCounter`] mirrors one cumulative `u64` per label
//! set; [`ObservableHistogram`] mirrors a whole `[u64; N]` ladder plus a sum as
//! three instruments. Both cache what the collection task published and both are
//! read synchronously by an OTel callback that never awaits and never locks
//! anything the kernel's hot paths touch.
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
//! OpenTelemetry. So the entire interface is four pieces of plain data:
//!
//! 1. `&[u64]` — the finite bucket upper bounds, in **microseconds**, ascending
//!    ([`ObservableHistogram::register`]).
//! 2. `&[u64]` — the bucket counts, one per bound plus one overflow bucket.
//! 3. `u64` — the sum, in microseconds.
//! 4. `u64` — one counter's cumulative total ([`ObservableCounter::record`]).
//!
//! ([`ObservableHistogram::record_snapshot`] for 2 and 3.) The kernel owns the
//! ladder and the arithmetic; this module owns the exposition. The binaries own
//! the *names*, which is why no metric name appears in this file outside a test:
//! the kernel enumerates by Rust identifier and `src/bin/graph_node/` holds the
//! two name tables.

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
/// seconds or it is worse than a series with a HydraDB name. Everything else
/// keeps microseconds under a `hydradb.*` name. A bound table in seconds must
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
    /// `hydradb.*` otherwise.
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
    #[error("histogram {name}: expected {expected} bucket counts (bounds + overflow), got {found}")]
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

/// The unit an exported counter is reported in.
///
/// Three values, and the vocabulary is closed on purpose. Every kernel counter is
/// a count of events, a **cumulative sum of microseconds** — the `_us`
/// fields that `rate(sum) / rate(count)` turns into a mean — or a cumulative sum
/// of bytes, reported unscaled for the same reason microseconds are. There is no
/// `Seconds`, and its absence is the design: [`HistogramUnit::Seconds`] exists
/// because one semantic convention fixes one *histogram* in seconds, whereas
/// scaling a counter here would make it disagree with the `_microseconds` series
/// the Prometheus endpoint renders from the same field, and nothing downstream
/// could detect the factor of a million.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterUnit {
    /// A count of events. UCUM `1`.
    Count,
    /// A cumulative sum of microseconds. UCUM `us`.
    Microseconds,
    /// A cumulative sum of bytes. UCUM `By`.
    Bytes,
}

impl CounterUnit {
    /// The UCUM unit annotation for the instrument.
    pub fn ucum(self) -> &'static str {
        match self {
            Self::Count => "1",
            Self::Microseconds => "us",
            Self::Bytes => "By",
        }
    }
}

/// What an exported counter is called and what it means.
///
/// One instrument, not three: unlike [`HistogramSpec`] there is no `.bucket` /
/// `.sum` / `.count` derivation, so `name` is the metric name exactly as it
/// reaches the wire.
#[derive(Clone, Copy, Debug)]
pub struct CounterSpec {
    /// Metric name. `db.*` where a semantic convention genuinely exists,
    /// `hydradb.*` otherwise — and for counters it is always the latter, because
    /// the one stable database metric is a histogram.
    pub name: &'static str,
    /// One-line description, exported as the instrument description.
    pub description: &'static str,
    /// The unit the value is reported in.
    pub unit: CounterUnit,
}

/// Why a counter observation was rejected.
///
/// Deliberately its own type rather than a share of [`HistogramError`]: a counter
/// has no ladder, so `Bounds` and `BucketCount` are unreachable for it, and an
/// error enum two thirds of whose variants cannot occur is a worse signature than
/// two small enums. Both variants are programming errors and neither is a panic,
/// for the reason [`HistogramError`] gives.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CounterError {
    /// The same label key was supplied twice for one series.
    #[error("counter {name}: label {key} was supplied twice")]
    DuplicateLabel {
        /// The metric name.
        name: &'static str,
        /// The repeated key.
        key: &'static str,
    },

    /// A caller tried to supply `le`, which belongs to a histogram family.
    ///
    /// A scalar counter carrying `le` claims to be one bucket of a distribution
    /// it is not part of, and after a collector's Prometheus conversion it would
    /// sit in the same name space as [`ObservableHistogram`]'s buckets.
    #[error("counter {name}: {LE} belongs to a histogram family, not to a scalar counter")]
    ReservedLabel {
        /// The metric name.
        name: &'static str,
    },
}

/// A cumulative kernel counter, exported as one `u64` observable counter.
///
/// # Why observable, and why cumulative
///
/// The source is an `AtomicU64` that only ever increases. An OTel
/// `Counter::add()` takes a **delta**, so mirroring a cumulative source through a
/// synchronous counter means storing the last exported value and adding the
/// difference — a second copy of the state that can drift, and one that produces a
/// spurious spike or a spurious zero at every process restart.
/// `u64_observable_counter` reports the absolute value and lets the SDK compute
/// temporality, so the mapping is exact and there is no arithmetic to get wrong.
///
/// [`Self::record`] therefore **replaces** rather than accumulates. Adding would
/// double-count every interval, since the value handed in is already the total.
///
/// # What it does not do
///
/// It does not scale, sum across series, or invent a zero. A series that has never
/// been recorded is not reported at all — an observable instrument that reports a
/// zero for a label set that does not exist is how a dashboard grows rows for
/// shards that were never opened. A series that *stops* being recorded keeps
/// reporting its last value, which for a cumulative counter is the correct
/// degradation: a gap reads as a reset and produces a spurious `rate()` spike.
///
/// # Labels
///
/// `&[(MetricLabel, &str)]`, the same as [`ObservableHistogram::record_snapshot`],
/// which is what makes "no `hydradb.scope` on a metric" a type error rather than
/// a review comment — [`MetricLabel`]'s constructor is private to
/// [`crate::semconv`].
#[derive(Debug)]
pub struct ObservableCounter {
    name: &'static str,
    state: Arc<RwLock<HashMap<SeriesKey, u64>>>,
}

impl ObservableCounter {
    /// Register one `u64` observable counter on `meter`.
    ///
    /// Infallible, and that is the honest signature rather than a `Result` kept
    /// for symmetry with [`ObservableHistogram::register`]: there is no ladder to
    /// validate. The registration itself cannot be observed to fail — the SDK
    /// validates instrument names internally and silently drops what it rejects,
    /// which is why the proof that a name survives is an export-level test and
    /// not a return value.
    ///
    /// The handle the builder returns is dropped for the same reason it is for
    /// histograms: in `opentelemetry` 0.32 an
    /// [`opentelemetry::metrics::ObservableCounter`] is a `PhantomData` marker and
    /// the callback lives in the meter's pipeline, so keeping it buys nothing.
    pub fn register(meter: &Meter, spec: CounterSpec) -> Self {
        let counter = Self {
            name: spec.name,
            state: Arc::new(RwLock::new(HashMap::new())),
        };

        let observed = counter.shallow_clone();
        let _ = meter
            .u64_observable_counter(spec.name)
            .with_description(spec.description)
            .with_unit(spec.unit.ucum())
            .with_callback(move |observer| {
                for observation in observed.observations() {
                    observer.observe(observation.value, &observation.attributes);
                }
            })
            .build();

        counter
    }

    /// Publish the latest cumulative value for one series.
    ///
    /// `value` is the absolute total, not an increment. Callers must not pass
    /// [`crate::semconv::LE`].
    pub fn record(&self, labels: &[(MetricLabel, &str)], value: u64) -> Result<(), CounterError> {
        let key = self.series_key(labels)?;
        self.write().insert(key, value);
        Ok(())
    }

    /// How many series this counter is currently reporting.
    ///
    /// One instrument's cardinality, directly — there is no bucket multiplier the
    /// way there is for [`ObservableHistogram::series_count`].
    pub fn series_count(&self) -> usize {
        self.read().len()
    }

    /// What the callback reports: one observation per recorded series.
    ///
    /// Public for the same reason the histogram's observation vectors are:
    /// standing up an `SdkMeterProvider` to find out whether a label survived is a
    /// test of the SDK, not of this code.
    pub fn observations(&self) -> Vec<Observation<u64>> {
        self.read()
            .iter()
            .map(|(key, value)| Observation {
                attributes: attributes_of(key),
                value: *value,
            })
            .collect()
    }

    /// Canonicalise a label set into a series key, rejecting `le` and duplicates.
    ///
    /// Sorted on the key so two callers passing the same labels in different
    /// orders address one series rather than two.
    fn series_key(&self, labels: &[(MetricLabel, &str)]) -> Result<SeriesKey, CounterError> {
        let mut key: SeriesKey = labels.iter().map(|(label, value)| (label.key(), (*value).to_string())).collect();
        if key.iter().any(|(name, _)| *name == LE) {
            return Err(CounterError::ReservedLabel { name: self.name });
        }
        key.sort_unstable();
        if let Some(duplicate) = key.windows(2).find(|pair| pair[0].0 == pair[1].0).map(|pair| pair[0].0) {
            return Err(CounterError::DuplicateLabel {
                name: self.name,
                key: duplicate,
            });
        }
        Ok(key)
    }

    /// A handle onto the same state, for the callback.
    fn shallow_clone(&self) -> Self {
        Self {
            name: self.name,
            state: Arc::clone(&self.state),
        }
    }

    /// Poisoning is recovered from rather than propagated, for the reason the
    /// histogram's accessor gives: a metrics pipeline that stops for the life of
    /// the process because something else panicked is worse than a stale series.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<SeriesKey, u64>> {
        self.state.read().unwrap_or_else(|poison| poison.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<SeriesKey, u64>> {
        self.state.write().unwrap_or_else(|poison| poison.into_inner())
    }
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
    pub fn register(meter: &Meter, spec: HistogramSpec, bounds: &[u64]) -> Result<Self, HistogramError> {
        if bounds.is_empty() || bounds.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(HistogramError::Bounds { name: spec.name });
        }

        let mut le_values: Vec<String> = bounds.iter().map(|bound| spec.unit.render_bound(*bound)).collect();
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
    pub fn record_snapshot(&self, labels: &[(MetricLabel, &str)], buckets: &[u64], sum_us: u64) -> Result<(), HistogramError> {
        if buckets.len() != self.bucket_count {
            return Err(HistogramError::BucketCount {
                name: self.name,
                expected: self.bucket_count,
                found: buckets.len(),
            });
        }

        let mut key: SeriesKey = labels.iter().map(|(label, value)| (label.key(), (*value).to_string())).collect();
        if key.iter().any(|(name, _)| *name == LE) {
            return Err(HistogramError::ReservedLabel { name: self.name });
        }
        key.sort_unstable();
        if let Some(duplicate) = key.windows(2).find(|pair| pair[0].0 == pair[1].0).map(|pair| pair[0].0) {
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
                value: entry.buckets.iter().copied().fold(0u64, u64::saturating_add),
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
        self.state.read().unwrap_or_else(|poison| poison.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, SeriesMap> {
        self.state.write().unwrap_or_else(|poison| poison.into_inner())
    }
}

fn attributes_of(key: &SeriesKey) -> Vec<KeyValue> {
    key.iter().map(|(name, value)| KeyValue::new(*name, value.clone())).collect()
}

/// A cached non-monotonic measurement for a bounded set of metric labels.
#[derive(Debug)]
pub struct ObservableGauge {
    name: &'static str,
    state: Arc<RwLock<HashMap<SeriesKey, f64>>>,
}
impl ObservableGauge {
    /// Register a gauge; the collection task must publish current values.
    pub fn register(meter: &Meter, name: &'static str, description: &'static str, unit: &'static str) -> Self {
        let state = Arc::new(RwLock::new(HashMap::<SeriesKey, f64>::new()));
        let observed = Arc::clone(&state);
        let _ = meter
            .f64_observable_gauge(name)
            .with_description(description)
            .with_unit(unit)
            .with_callback(move |observer| {
                for (key, value) in observed.read().unwrap_or_else(|p| p.into_inner()).iter() {
                    observer.observe(*value, &attributes_of(key));
                }
            })
            .build();
        Self { name, state }
    }
    /// Replace one series, including zeroes when its tracked objects drain.
    pub fn record(&self, labels: &[(MetricLabel, &str)], value: f64) -> Result<(), CounterError> {
        let key = self.series_key(labels)?;
        self.state.write().unwrap_or_else(|p| p.into_inner()).insert(key, value);
        Ok(())
    }

    /// Stop reporting one series, for a measurement that can no longer be
    /// taken. A gauge's last value is a claim about now, so repeating it after
    /// its source went away would report a reading nobody took. Forgetting a
    /// series that was never recorded does nothing.
    pub fn forget(&self, labels: &[(MetricLabel, &str)]) -> Result<(), CounterError> {
        let key = self.series_key(labels)?;
        self.state.write().unwrap_or_else(|p| p.into_inner()).remove(&key);
        Ok(())
    }

    /// Canonicalise a label set into a series key, rejecting `le` and duplicates.
    fn series_key(&self, labels: &[(MetricLabel, &str)]) -> Result<SeriesKey, CounterError> {
        let mut key: SeriesKey = labels.iter().map(|(label, value)| (label.key(), (*value).to_string())).collect();
        if key.iter().any(|(name, _)| *name == LE) {
            return Err(CounterError::ReservedLabel { name: self.name });
        }
        key.sort_unstable();
        if let Some(duplicate) = key.windows(2).find(|pair| pair[0].0 == pair[1].0).map(|pair| pair[0].0) {
            return Err(CounterError::DuplicateLabel {
                name: self.name,
                key: duplicate,
            });
        }
        Ok(key)
    }
    /// The latest values, also used to verify reset and attribute semantics.
    pub fn observations(&self) -> Vec<Observation<f64>> {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(key, value)| Observation {
                attributes: attributes_of(key),
                value: *value,
            })
            .collect()
    }
}
