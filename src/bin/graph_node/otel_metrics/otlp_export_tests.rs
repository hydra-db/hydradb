//! What an OTLP collector receives from the counter instruments
//! [`super::NodeCounters`] registers — through `hydradb_telemetry::init`, a real
//! `PeriodicReader`, and a socket.
//!
//! # Why a socket, and why not a capture exporter
//!
//! `crates/telemetry/tests/meter_export.rs` proves the *wrapper*: an
//! `ObservableCounter` reaches an exporter as a monotonic sum per label set, in
//! the declared unit. It cannot prove anything about this binary, because it
//! cannot name a kernel snapshot or either name table — the telemetry crate is
//! forbidden the dependency, which is what keeps the kernel free of
//! OpenTelemetry.
//!
//! What is left to prove is the join: that the instrument names
//! [`super::OTEL_COUNTERS`] holds survive registration, that the values
//! [`super::shard_counter_totals`] computes are what leaves the process, and that
//! `hydradb.cell_id` is the only dimension on the way out. That claim is about a
//! pipeline, so the test builds no `SdkMeterProvider` of its own: it goes through
//! `hydradb_telemetry::init` and takes the meter from
//! `TelemetryGuard::providers()`, which is the step production code was missing
//! for two commits while every rendering test passed. A capture exporter would
//! also need `opentelemetry_sdk` as a dev-dependency of the root package, on every
//! `cargo test` of a crate whose whole point is that a bare `cargo test` pulls no
//! `opentelemetry-*`; a loopback `TcpListener` needs nothing but `std`.
//!
//! # One test, and why it cannot be two
//!
//! `hydradb_telemetry::init` installs the process-global `tracing` subscriber, so
//! a second call in the same test binary returns
//! `TelemetryError::AlreadyInitialised`. One process, one `init`, one guard.
//! Everything that does not need the socket is a plain test in
//! [`super::tests`] instead, where `just test-server-runtime` actually runs it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hydradb_telemetry::semconv::L_CELL_ID;
use hydradb_telemetry::{ServiceIdentity, TelemetryConfig};
use slatedb_graph_kernel::{
    GraphCacheMetricsSnapshot, GraphId, GraphOperationalMetricsSnapshot, GraphScope,
    GraphShardRuntimeMetrics, NamespaceId, ScopedGraphShardRuntimeMetrics,
};

use super::{CounterSource, NodeCounters};

/// Every request the stand-in collector received: request path and raw body.
type CapturedRequests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// A minimal HTTP sink standing in for a collector, capturing what was POSTed.
///
/// Closes each connection after answering so the exporter cannot keep one alive
/// and leave the last body unread in a socket buffer at assertion time. The twin
/// of `crates/telemetry/tests/meter_export.rs::spawn_collector`, duplicated rather
/// than shared because sharing it would mean shipping an HTTP server in the
/// telemetry crate's public surface to serve one test in another package.
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

/// Two tenants with work on `cell-a` and one on `cell-b`, the shape
/// `local_shard_runtime_metrics` returns on a node hosting two namespaces.
fn shards() -> Vec<ScopedGraphShardRuntimeMetrics> {
    [
        ("alpha", "cell-a", 3u64, 700u64),
        ("beta", "cell-a", 4, 90),
        ("alpha", "cell-b", 5, 1),
    ]
    .into_iter()
    .map(
        |(tenant, cell_id, write_attempts, gc_duration_us)| ScopedGraphShardRuntimeMetrics {
            scope: GraphScope::tenant(
                NamespaceId::new(tenant).expect("a valid namespace id"),
                GraphId::default(),
            ),
            shard: GraphShardRuntimeMetrics {
                cell_id: cell_id.to_string(),
                operational: GraphOperationalMetricsSnapshot {
                    write_attempts,
                    gc_duration_us,
                    ..Default::default()
                },
                cache: GraphCacheMetricsSnapshot::default(),
                cache_entries: Default::default(),
                cache_resident_bytes: Default::default(),
            },
        },
    )
    .collect()
}

/// **The end-to-end claim.** The shard counter instruments registered against the
/// meter `TelemetryGuard::providers()` hands out leave the process as an OTLP
/// `/v1/metrics` request, dimensioned by `hydradb.cell_id` and nothing else.
///
/// Four things are checked, in two places, and the split is not arbitrary — each
/// is checked where it is *observable*:
///
/// - The **names** and the **label key** are checked on the wire, as bytes.
///   Protobuf encodes strings literally, so `hydradb.shard.write.attempts` and
///   `hydradb.cell_id` are substrings of the frame, and asserting on them needs no
///   `prost` dependency in a test whose point is that the wire format is not the
///   thing under test. This is the assertion the SDK can quietly defeat: it
///   validates instrument names internally and *drops* what it rejects, so a name
///   surviving is not something to take on faith.
/// - The **values** are checked through the instrument, because on the wire they
///   are varints rather than text. `cell-a`'s 7 is the sum of two tenants' 3 and
///   4, which is the number that distinguishes a per-cell series from whichever
///   scope happened to be recorded last.
/// - The **absence** of `hydradb.scope` is checked on the wire, because that is
///   the only place it could appear.
#[test]
fn the_shard_counters_reach_an_otlp_collector_summed_by_cell() {
    let (endpoint, captured) = spawn_collector();

    let mut config = TelemetryConfig::new(ServiceIdentity::GraphNode);
    config.otlp_endpoint = Some(endpoint);
    // Short, so the periodic path is the one that fires and the test does not rest
    // on shutdown's final collection alone.
    config.metric_export_interval = Duration::from_millis(200);
    config.export_timeout = Duration::from_secs(5);

    let guard = hydradb_telemetry::init(config).expect("telemetry installs");
    let counters = NodeCounters::register(
        guard
            .providers()
            .expect("an endpoint must produce providers"),
    );
    counters
        .record_shard_totals(&shards())
        .expect("cell_id alone");

    // The values, before the wire: the callback reports what was published, and
    // what was published is the per-cell sum.
    let mut attempts: Vec<(String, u64)> = counters
        .observations(CounterSource::Shard, "write_attempts")
        .iter()
        .map(|observation| {
            let cell = observation
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == L_CELL_ID.key())
                .map(|attribute| attribute.value.to_string())
                .expect("every shard counter carries cell_id");
            assert_eq!(
                observation.attributes.len(),
                1,
                "cell_id is the only dimension: {:?}",
                observation.attributes
            );
            (cell, observation.value)
        })
        .collect();
    attempts.sort();
    assert_eq!(
        attempts,
        vec![("cell-a".to_string(), 7), ("cell-b".to_string(), 5)],
        "two tenants on cell-a must sum to 7, not report 3 or 4"
    );
    assert_eq!(
        counters
            .observations(CounterSource::Shard, "query_rows_duration_us")
            .len(),
        0,
        "the derived counter restates a histogram sum and has no instrument"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    let body = loop {
        let found = captured
            .lock()
            .expect("sink lock")
            .iter()
            .find(|(path, body)| {
                path.ends_with("/v1/metrics") && contains(body, "hydradb.shard.write.attempts")
            })
            .map(|(_, body)| body.clone());
        if let Some(body) = found {
            break body;
        }
        assert!(
            Instant::now() < deadline,
            "no OTLP /v1/metrics request carried a shard counter; captured {:?}",
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

    // A second family, and a microsecond one: one name arriving proves the meter
    // is reachable, two prove the registration loop is.
    assert!(
        contains(&body, "hydradb.shard.gc.duration.sum"),
        "the microsecond counters did not reach the exporter"
    );
    assert!(
        contains(&body, L_CELL_ID.key()),
        "the counters' only dimension did not reach the exporter"
    );
    assert!(
        contains(&body, "cell-a") && contains(&body, "cell-b"),
        "only one cell's series reached the exporter"
    );
    // The description is the kernel's field identifier rather than a sentence,
    // which is the join back to the code and to `/metrics` — see `OtelCounter`.
    assert!(
        contains(&body, "write_attempts"),
        "the kernel field identifier did not survive as the description"
    );
    // §1.4, asserted rather than described: `scope` is the unbounded tenant root,
    // `/metrics` carries it on three families, and OTLP carries it nowhere. Both
    // tenant names are in the fixture, so this is not vacuous.
    assert!(
        !contains(&body, "hydradb.scope"),
        "the tenant root reached a metric label"
    );
    assert!(
        !contains(&body, "alpha") && !contains(&body, "beta"),
        "a namespace name reached the wire through some other attribute"
    );
}
