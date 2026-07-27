//! What a metrics exporter actually receives from a registered histogram
//! family, through a real `SdkMeterProvider`.
//!
//! The unit tests in `meter.rs` assert on the observation vectors the callbacks
//! report, which is the arithmetic — cumulative buckets, `le` rendering, the
//! seconds conversion. They cannot assert that the callbacks are ever *called*,
//! because a `Meter` with no provider behind it is a no-op and building the
//! instruments against one is exactly what makes those tests cheap.
//!
//! This file closes that gap. The whole reason the buckets leave as a family of
//! observable counters rather than as a histogram data point is a property of
//! the SDK, so the claim "the family reaches the exporter as 18 series with an
//! `le` label" is a claim about the SDK and has to be checked against it. In
//! particular the SDK validates instrument names and silently drops instruments
//! whose names it rejects — `db.client.operation.duration.bucket` being fine is
//! not something to take on faith.
//!
//! No collector and no network: a `PushMetricExporter` implemented here
//! captures the `ResourceMetrics` a forced flush produces.

#![cfg(feature = "otlp")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};

use opentelemetry::metrics::MeterProvider as _;
use turbolay_telemetry::meter::{HistogramSpec, HistogramUnit, ObservableHistogram, LE_INFINITY};
use turbolay_telemetry::semconv::{LE, L_CELL_ID};

/// One exported series, flattened to what the assertions are about.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Series {
    metric: String,
    unit: String,
    cell_id: Option<String>,
    le: Option<String>,
    value: u64,
}

#[derive(Clone, Debug, Default)]
struct Capture(Arc<Mutex<Vec<Series>>>);

impl PushMetricExporter for Capture {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut captured = self.0.lock().expect("capture lock");
        for scope in metrics.scope_metrics() {
            for metric in scope.metrics() {
                let points: Vec<(Vec<(String, String)>, u64)> = match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| (attributes_of(point.attributes()), point.value()))
                        .collect(),
                    AggregatedMetrics::F64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| {
                            (
                                attributes_of(point.attributes()),
                                point.value().round() as u64,
                            )
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                for (attributes, value) in points {
                    captured.push(Series {
                        metric: metric.name().to_string(),
                        unit: metric.unit().to_string(),
                        cell_id: lookup(&attributes, L_CELL_ID.key()),
                        le: lookup(&attributes, LE),
                        value,
                    });
                }
            }
        }
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

fn attributes_of<'a>(
    attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>,
) -> Vec<(String, String)> {
    attributes
        .map(|attribute| {
            (
                attribute.key.as_str().to_string(),
                attribute.value.to_string(),
            )
        })
        .collect()
}

fn lookup(attributes: &[(String, String)], key: &str) -> Option<String> {
    attributes
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.clone())
}

const BOUNDS: [u64; 17] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000,
];

/// The real kernel ladder, exported under the one name with a stable semantic
/// convention, reaches the exporter as 18 bucket series plus a sum and a count
/// — with the bounds in seconds, which is what semconv fixes that metric in.
#[test]
fn the_bucket_family_reaches_the_exporter_with_le_labels() {
    let capture = Capture::default();
    // An hour, so nothing but the forced flush ever collects.
    let reader = PeriodicReader::builder(capture.clone())
        .with_interval(Duration::from_secs(3_600))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("turbolay.test");

    let histogram = ObservableHistogram::register(
        &meter,
        HistogramSpec {
            name: "db.client.operation.duration",
            description: "client query latency",
            unit: HistogramUnit::Seconds,
        },
        &BOUNDS,
    )
    .expect("the kernel ladder is ascending");

    // Two observations: one at 300 µs (bucket `le=500`), one past every bound.
    let mut counts = [0u64; 18];
    counts[2] = 1;
    counts[17] = 1;
    histogram
        .record_snapshot(&[(L_CELL_ID, "cell-7")], &counts, 40_000_300)
        .expect("18 counts for 17 bounds");

    provider.force_flush().expect("flush");
    let captured = capture.0.lock().expect("capture lock").clone();

    let buckets: Vec<&Series> = captured
        .iter()
        .filter(|series| series.metric == "db.client.operation.duration.bucket")
        .collect();
    assert_eq!(
        buckets.len(),
        18,
        "one series per bucket including the overflow, got {captured:#?}"
    );
    assert!(
        buckets
            .iter()
            .all(|series| series.cell_id.as_deref() == Some("cell-7")),
        "the cell_id label must survive onto every bucket series"
    );

    // Cumulative: nothing below 500 µs, one observation from 0.0005 onwards,
    // and the second only in +Inf.
    let at = |le: &str| {
        buckets
            .iter()
            .find(|series| series.le.as_deref() == Some(le))
            .unwrap_or_else(|| panic!("no bucket at le={le} in {buckets:#?}"))
            .value
    };
    assert_eq!(at("0.0001"), 0);
    assert_eq!(at("0.00025"), 0);
    assert_eq!(at("0.0005"), 1);
    assert_eq!(at("30"), 1);
    assert_eq!(at(LE_INFINITY), 2);

    let sum = captured
        .iter()
        .find(|series| series.metric == "db.client.operation.duration.sum")
        .expect("a sum series");
    assert_eq!(sum.unit, "s", "semconv fixes this metric in seconds");
    assert_eq!(sum.value, 40, "40.0000003 s, rounded by the capture");

    let count = captured
        .iter()
        .find(|series| series.metric == "db.client.operation.duration.count")
        .expect("a count series");
    assert_eq!(
        count.value,
        at(LE_INFINITY),
        "_count and le=+Inf must agree by construction"
    );

    provider.shutdown().expect("shutdown");
}

/// A histogram registered but never recorded into must produce no series at
/// all. An observable instrument that reports a zero for a series that does not
/// exist is how a dashboard grows rows for shards that were never opened.
#[test]
fn an_unrecorded_histogram_exports_nothing() {
    let capture = Capture::default();
    let reader = PeriodicReader::builder(capture.clone())
        .with_interval(Duration::from_secs(3_600))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("turbolay.test");

    let _histogram = ObservableHistogram::register(
        &meter,
        HistogramSpec {
            name: "turbolay.query.rows.duration",
            description: "shard row-query latency",
            unit: HistogramUnit::Microseconds,
        },
        &BOUNDS,
    )
    .expect("the kernel ladder is ascending");

    provider.force_flush().expect("flush");
    assert!(
        capture.0.lock().expect("capture lock").is_empty(),
        "no snapshot recorded means no series"
    );

    provider.shutdown().expect("shutdown");
}
