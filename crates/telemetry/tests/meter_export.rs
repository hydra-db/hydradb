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
//! A `PushMetricExporter` implemented here captures the `ResourceMetrics` a
//! forced flush produces, with no collector and no network.
//!
//! The last two tests do use a socket, deliberately. Everything above them
//! proves the *rendering* is right; they prove the pipeline is **connected** —
//! that a series recorded through the handle
//! [`hydradb_telemetry::TelemetryGuard::providers`] returns actually leaves the
//! process as OTLP. That is a different claim, and it is the one that was false
//! for two commits while every rendering test passed: the meter provider was
//! built, held and unreachable. A test that constructs an `SdkMeterProvider`
//! itself can never catch that, because constructing one is precisely the step
//! production code was not doing.

#![cfg(feature = "otlp")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};

use hydradb_telemetry::meter::{
    CounterSpec, CounterUnit, HistogramSpec, HistogramUnit, ObservableCounter, ObservableHistogram,
    LE_INFINITY,
};
use hydradb_telemetry::semconv::{
    DB_OPERATION_READ, DB_OPERATION_WRITE, DB_SYSTEM_NEO4J, LE, L_CELL_ID, L_DB_OPERATION_NAME,
    L_DB_SYSTEM_NAME,
};
use hydradb_telemetry::{ServiceIdentity, TelemetryConfig};
use opentelemetry::metrics::MeterProvider as _;

/// One exported series, flattened to what the assertions are about.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Series {
    metric: String,
    unit: String,
    cell_id: Option<String>,
    operation: Option<String>,
    le: Option<String>,
    value: u64,
    /// Whether the SDK classified the data point as a **monotonic** sum.
    ///
    /// Captured because it is the one property that distinguishes a counter from
    /// a gauge on the wire, and getting it wrong is invisible in the value: a
    /// non-monotonic sum makes every `rate()` and `increase()` over the series
    /// wrong without changing a single number a dashboard displays directly.
    monotonic: bool,
}

#[derive(Clone, Debug, Default)]
struct Capture(Arc<Mutex<Vec<Series>>>);

/// One data point as the capture reads it out of a sum: its attribute set, its
/// value, and whether the sum it came from is monotonic.
type Point = (Vec<(String, String)>, u64, bool);

impl PushMetricExporter for Capture {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut captured = self.0.lock().expect("capture lock");
        for scope in metrics.scope_metrics() {
            for metric in scope.metrics() {
                let points: Vec<Point> = match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| {
                            (
                                attributes_of(point.attributes()),
                                point.value(),
                                sum.is_monotonic(),
                            )
                        })
                        .collect(),
                    AggregatedMetrics::F64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| {
                            (
                                attributes_of(point.attributes()),
                                point.value().round() as u64,
                                sum.is_monotonic(),
                            )
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                for (attributes, value, monotonic) in points {
                    captured.push(Series {
                        metric: metric.name().to_string(),
                        unit: metric.unit().to_string(),
                        cell_id: lookup(&attributes, L_CELL_ID.key()),
                        operation: lookup(&attributes, L_DB_OPERATION_NAME.key()),
                        le: lookup(&attributes, LE),
                        value,
                        monotonic,
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
    let meter = provider.meter("hydradb.test");

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
    let meter = provider.meter("hydradb.test");

    let _histogram = ObservableHistogram::register(
        &meter,
        HistogramSpec {
            name: "hydradb.query.rows.duration",
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

/// The read/write split reaches the exporter as **one instrument name and two
/// series**, told apart by `db.operation.name`.
///
/// This is the property `db.client.operation.duration.read` / `….write` existed
/// as a workaround for. It is worth an exporter-level test rather than an
/// observation-vector one because the failure it guards against is *silent
/// collapse*: two series recorded under one instrument name with no
/// distinguishing attribute do not error, do not warn and do not duplicate —
/// they merge, and the merged number looks entirely plausible. Only counting
/// what an exporter received can tell the difference.
#[test]
fn read_and_write_share_one_instrument_and_stay_two_series() {
    let capture = Capture::default();
    let reader = PeriodicReader::builder(capture.clone())
        .with_interval(Duration::from_secs(3_600))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("hydradb.test");

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

    let mut reads = [0u64; 18];
    reads[0] = 5;
    let mut writes = [0u64; 18];
    writes[17] = 2;

    histogram
        .record_snapshot(
            &[
                (L_DB_SYSTEM_NAME, DB_SYSTEM_NEO4J),
                (L_DB_OPERATION_NAME, DB_OPERATION_READ),
            ],
            &reads,
            400,
        )
        .expect("18 counts for 17 bounds");
    histogram
        .record_snapshot(
            &[
                (L_DB_SYSTEM_NAME, DB_SYSTEM_NEO4J),
                (L_DB_OPERATION_NAME, DB_OPERATION_WRITE),
            ],
            &writes,
            90_000_000,
        )
        .expect("18 counts for 17 bounds");

    provider.force_flush().expect("flush");
    let captured = capture.0.lock().expect("capture lock").clone();

    let names: Vec<&str> = {
        let mut names: Vec<&str> = captured
            .iter()
            .map(|series| series.metric.as_str())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    };
    assert_eq!(
        names,
        vec![
            "db.client.operation.duration.bucket",
            "db.client.operation.duration.count",
            "db.client.operation.duration.sum",
        ],
        "the split must not reappear as a name suffix"
    );

    let counts: Vec<&Series> = captured
        .iter()
        .filter(|series| series.metric == "db.client.operation.duration.count")
        .collect();
    assert_eq!(
        counts.len(),
        2,
        "two populations, two series: {captured:#?}"
    );
    let count_of = |operation: &str| {
        counts
            .iter()
            .find(|series| series.operation.as_deref() == Some(operation))
            .unwrap_or_else(|| panic!("no {operation} series in {counts:#?}"))
            .value
    };
    assert_eq!(count_of(DB_OPERATION_READ), 5);
    assert_eq!(count_of(DB_OPERATION_WRITE), 2);

    // 36 bucket series, not 18: the `le` dimension multiplies the operation
    // dimension rather than replacing it.
    assert_eq!(
        captured
            .iter()
            .filter(|series| series.metric == "db.client.operation.duration.bucket")
            .count(),
        36
    );

    provider.shutdown().expect("shutdown");
}

/// An [`ObservableCounter`] reaches the exporter as one **monotonic** `u64` sum
/// per recorded label set, in the declared unit.
///
/// Three claims the observation vectors in `meter.rs` cannot make, because they
/// stop at what the callback reports:
///
/// - The callback is *called at all*. An observable instrument whose callback the
///   SDK never invokes is silent, and silence is indistinguishable from a counter
///   that is genuinely zero.
/// - The data point is monotonic. That is what makes it a counter rather than a
///   gauge, and it is the property `rate()` rests on.
/// - The `us` unit survives onto the metric. The µs sums are the counters an
///   operator divides to get a mean, and a unit lost in registration is a series
///   whose scale has to be guessed.
#[test]
fn a_counter_reaches_the_exporter_as_a_monotonic_sum_per_cell() {
    let capture = Capture::default();
    let reader = PeriodicReader::builder(capture.clone())
        .with_interval(Duration::from_secs(3_600))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("hydradb.test");

    let counter = ObservableCounter::register(
        &meter,
        CounterSpec {
            name: "hydradb.shard.gc.duration.sum",
            description: "cumulative microseconds spent in GC",
            unit: CounterUnit::Microseconds,
        },
    );
    counter
        .record(&[(L_CELL_ID, "cell-a")], 4_000)
        .expect("one label");
    counter
        .record(&[(L_CELL_ID, "cell-b")], 9_000)
        .expect("one label");
    // Cumulative source: the later value replaces rather than adds.
    counter
        .record(&[(L_CELL_ID, "cell-a")], 4_500)
        .expect("one label");

    provider.force_flush().expect("flush");
    let mut captured = capture.0.lock().expect("capture lock").clone();
    captured.sort();

    assert_eq!(
        captured,
        vec![
            Series {
                metric: "hydradb.shard.gc.duration.sum".to_string(),
                unit: "us".to_string(),
                cell_id: Some("cell-a".to_string()),
                operation: None,
                le: None,
                value: 4_500,
                monotonic: true,
            },
            Series {
                metric: "hydradb.shard.gc.duration.sum".to_string(),
                unit: "us".to_string(),
                cell_id: Some("cell-b".to_string()),
                operation: None,
                le: None,
                value: 9_000,
                monotonic: true,
            },
        ],
        "two cells, one series each, replaced not accumulated"
    );

    provider.shutdown().expect("shutdown");
}

/// A registered counter that was never recorded into must export **no** series.
///
/// The mirror of `an_unrecorded_histogram_exports_nothing`, and it matters more
/// for counters: `graph-node` will register every counter instrument at boot and
/// only ever record the cells it actually opens, so an instrument that reported a
/// zero on registration would put a series on the wire for every cell in the
/// fleet from every node in it.
#[test]
fn an_unrecorded_counter_exports_nothing() {
    let capture = Capture::default();
    let reader = PeriodicReader::builder(capture.clone())
        .with_interval(Duration::from_secs(3_600))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();

    let _counter = ObservableCounter::register(
        &provider.meter("hydradb.test"),
        CounterSpec {
            name: "hydradb.shard.write.retries",
            description: "write retries",
            unit: CounterUnit::Count,
        },
    );

    provider.force_flush().expect("flush");
    assert!(
        capture.0.lock().expect("capture lock").is_empty(),
        "nothing recorded means no series"
    );

    provider.shutdown().expect("shutdown");
}

/// Every request the stand-in collector received: request path and raw body.
type CapturedRequests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// A minimal HTTP sink standing in for a collector, capturing what was POSTed.
///
/// Closes each connection after answering so the exporter cannot keep one alive
/// and leave the last body unread in a socket buffer at assertion time.
fn spawn_collector() -> (String, CapturedRequests) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let endpoint = format!("http://{}", listener.local_addr().expect("bound"));
    let captured: CapturedRequests = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let Ok(peer) = stream.try_clone() else {
                continue;
            };
            let mut reader = BufReader::new(peer);

            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let path = request_line
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();

            let mut length = 0usize;
            loop {
                let mut header = String::new();
                match reader.read_line(&mut header) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
                if header == "\r\n" || header == "\n" {
                    break;
                }
                let lowered = header.to_ascii_lowercase();
                if let Some(value) = lowered.strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap_or(0);
                }
            }

            let mut body = vec![0u8; length];
            if reader.read_exact(&mut body).is_err() {
                body.clear();
            }
            sink.lock().expect("sink lock").push((path, body));

            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\n\
                  content-type: application/x-protobuf\r\n\
                  content-length: 0\r\n\
                  connection: close\r\n\r\n",
            );
            let _ = stream.flush();
        }
    });

    (endpoint, captured)
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    let needle = needle.as_bytes();
    haystack.len() >= needle.len()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// **The end-to-end claim.** Instruments registered against the meter
/// `TelemetryGuard::providers()` hands out leave the process as an OTLP
/// `/v1/metrics` request — a histogram family *and* a counter, on one meter.
///
/// Every other test in this file builds its own `SdkMeterProvider`, which is
/// exactly the step production code was missing — so none of them could fail
/// while the metrics path was unreachable, and none of them did. This one goes
/// through `hydradb_telemetry::init`, so it exercises the accessor, the
/// `PeriodicReader`'s own OS thread, the blocking HTTP client that thread
/// requires, and the endpoint composition in `otlp::build`. If any one of those
/// is wrong, nothing arrives.
///
/// Both instrument kinds are asserted in **one** test rather than two, and that is
/// a constraint rather than a convenience: `hydradb_telemetry::init` installs the
/// process-global `tracing` subscriber, so a second call in the same test binary
/// returns `TelemetryError::AlreadyInitialised`. One process, one `init`, one
/// guard — so the counter's proof of export shares this one, which incidentally
/// makes it a stronger claim: the two kinds coexist on a single meter without one
/// shadowing the other's registration.
///
/// The body is checked as bytes rather than decoded: protobuf encodes strings
/// literally, so the instrument name and the label key are substrings of the
/// frame, and asserting on them needs no `prost` dependency in a test whose
/// point is that the wire format is not the thing under test.
#[test]
fn a_metric_recorded_through_the_guards_meter_reaches_an_otlp_exporter() {
    let (endpoint, captured) = spawn_collector();

    let mut config = TelemetryConfig::new(ServiceIdentity::GraphNode);
    config.otlp_endpoint = Some(endpoint);
    // Short, so the periodic path is the one that fires and the test does not
    // rest on shutdown's final collection alone.
    config.metric_export_interval = Duration::from_millis(200);
    config.export_timeout = Duration::from_secs(5);

    let guard = hydradb_telemetry::init(config).expect("telemetry installs");
    let (histogram, counter) = {
        let providers = guard
            .providers()
            .expect("an endpoint must produce providers");
        let meter = providers.meter("hydradb.graph_node");
        (
            ObservableHistogram::register(
                &meter,
                HistogramSpec {
                    name: "db.client.operation.duration",
                    description: "client query latency",
                    unit: HistogramUnit::Seconds,
                },
                &BOUNDS,
            )
            .expect("the kernel ladder is ascending"),
            ObservableCounter::register(
                &meter,
                CounterSpec {
                    name: "hydradb.shard.write.retries",
                    description: "optimistic write retries",
                    unit: CounterUnit::Count,
                },
            ),
        )
    };
    counter
        .record(&[(L_CELL_ID, "cell-a")], 17)
        .expect("cell_id alone");

    let mut reads = [0u64; 18];
    reads[1] = 3;
    histogram
        .record_snapshot(
            &[
                (L_DB_SYSTEM_NAME, DB_SYSTEM_NEO4J),
                (L_DB_OPERATION_NAME, DB_OPERATION_READ),
            ],
            &reads,
            600,
        )
        .expect("18 counts for 17 bounds");
    let mut writes = [0u64; 18];
    writes[16] = 1;
    histogram
        .record_snapshot(
            &[
                (L_DB_SYSTEM_NAME, DB_SYSTEM_NEO4J),
                (L_DB_OPERATION_NAME, DB_OPERATION_WRITE),
            ],
            &writes,
            25_000_000,
        )
        .expect("18 counts for 17 bounds");

    let deadline = Instant::now() + Duration::from_secs(30);
    let body = loop {
        let found = captured
            .lock()
            .expect("sink lock")
            .iter()
            .find(|(path, body)| {
                path.ends_with("/v1/metrics")
                    && contains(body, "db.client.operation.duration.bucket")
                    // Both kinds in the *same* frame: one collection cycle reads
                    // every callback registered on the meter, so a counter
                    // arriving a cycle later than the histogram would mean the
                    // two are not on one pipeline.
                    && contains(body, "hydradb.shard.write.retries")
            })
            .map(|(_, body)| body.clone());
        if let Some(body) = found {
            break body;
        }
        assert!(
            Instant::now() < deadline,
            "no OTLP /v1/metrics request carried the instrument; captured {:?}",
            captured
                .lock()
                .expect("sink lock")
                .iter()
                .map(|(path, body)| (path.clone(), body.len()))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(50));
    };

    guard.shutdown();

    assert!(
        contains(&body, "db.client.operation.duration.sum"),
        "the sum instrument did not reach the exporter"
    );
    assert!(
        contains(&body, "db.client.operation.duration.count"),
        "the count instrument did not reach the exporter"
    );
    assert!(
        contains(&body, L_DB_OPERATION_NAME.key()),
        "the label that keeps read and write apart did not reach the exporter"
    );
    assert!(
        contains(&body, DB_OPERATION_READ) && contains(&body, DB_OPERATION_WRITE),
        "only one of the two operations reached the exporter"
    );
    assert!(
        contains(&body, LE),
        "the bucket bound label did not reach the exporter"
    );
    assert!(
        contains(&body, L_CELL_ID.key()) && contains(&body, "cell-a"),
        "the counter's only dimension did not reach the exporter"
    );
    // The deliberate semconv non-conformance, asserted rather than described:
    // `db.namespace` is required-if-applicable and it is applicable — the
    // namespace is `hydradb.scope`, which is the unbounded tenant root the
    // label registry exists to keep off metrics. Its absence is the design.
    assert!(
        !contains(&body, "db.namespace"),
        "db.namespace reached the exporter; its only value is the unbounded scope"
    );
    assert!(
        !contains(&body, "hydradb.scope"),
        "the tenant root reached a metric label"
    );
}
