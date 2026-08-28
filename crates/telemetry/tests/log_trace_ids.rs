//! The stdout log line carries the ids that link it to its trace.
//!
//! This is the half of the OTLP-log-exporter removal that can silently regress.
//! Dropping the `OpenTelemetryTracingBridge` also drops the `trace_id`/`span_id`
//! stamping it did for free on the OTLP log record, and in Kubernetes stdout is
//! then the *only* route logs take out of the pod. Vector's `prep_otlp` remap
//! reads these two keys by name and decodes them into the OTLP `logRecord`'s
//! dedicated `traceId`/`spanId` fields — which is what a backend's log-to-trace
//! deep link resolves against. Log attributes do not work for that, so a line
//! missing these keys breaks the jump from a log to its trace with no error
//! anywhere.
//!
//! The unit test in `layers.rs` covers the no-tracer case (the keys are absent).
//! Only this file can cover the case that matters, because populating them
//! requires a real `tracing_opentelemetry` layer over a real tracer — exactly
//! the machinery `capture_json` has no way to install.
//!
//! The sampler is pinned at ratio 0.0 on purpose: an unsampled span still has a
//! valid trace id, and the ids must be emitted regardless of the sampling
//! decision. Two log lines carrying the same trace id are correlatable with each
//! other whether or not the trace itself was kept.

#![cfg(feature = "otlp")]

use std::sync::{Arc, Mutex};

use hydradb_telemetry::layers::{HydraDBJson, RedactingFields};
use hydradb_telemetry::sampling::HydraDBSampler;
use hydradb_telemetry::{ServiceIdentity, TelemetryConfig};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;

/// Collects the rendered stdout lines.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture mutex").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `body` under a subscriber carrying both the JSON fmt layer and a real
/// OTel trace layer — the production pairing — and return the captured lines.
fn capture_with_tracer(body: impl FnOnce()) -> Vec<serde_json::Value> {
    let capture = Capture::default();
    let config = TelemetryConfig::new(ServiceIdentity::GraphNode);

    let provider = SdkTracerProvider::builder()
        .with_sampler(HydraDBSampler::new(0.0))
        .build();
    let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("test"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .fmt_fields(RedactingFields::json())
        .event_format(HydraDBJson::new(&config))
        .with_writer(capture.clone());

    let subscriber = tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer);
    tracing::subscriber::with_default(subscriber, body);

    let bytes = capture.0.lock().expect("capture mutex").clone();
    String::from_utf8(bytes)
        .expect("the formatter writes UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("each line must be valid JSON"))
        .collect()
}

/// The shape contract with Vector: lower-case hex, 32 and 16 characters, and
/// non-zero. `prep_otlp` guards on exactly `^[0-9a-f]{32}$` / `^[0-9a-f]{16}$`
/// and skips anything else, so an id in any other spelling is silently dropped
/// on the way to the backend rather than rejected loudly.
#[test]
fn an_event_inside_a_span_carries_hex_trace_and_span_ids() {
    let lines = capture_with_tracer(|| {
        let span = tracing::info_span!("read");
        let _entered = span.enter();
        tracing::info!("query served");
    });

    assert_eq!(lines.len(), 1, "one event, one line");

    let trace_id = lines[0]["trace_id"].as_str().expect("trace_id is a string");
    let span_id = lines[0]["span_id"].as_str().expect("span_id is a string");

    assert_eq!(trace_id.len(), 32, "a W3C trace id is 16 bytes of hex");
    assert_eq!(span_id.len(), 16, "a W3C span id is 8 bytes of hex");
    assert!(
        trace_id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "lower-case hex only: {trace_id}"
    );
    assert!(
        span_id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "lower-case hex only: {span_id}"
    );
    assert_ne!(trace_id, "0".repeat(32), "a zero id resolves to no trace");
    assert_ne!(span_id, "0".repeat(16), "a zero id resolves to no span");
}

/// Two events in the same span share a trace id, and events in different spans
/// do not. This is the property the whole change exists to preserve: grouping
/// log lines by request survives the exporter removal.
#[test]
fn ids_group_events_by_span_not_by_line() {
    let lines = capture_with_tracer(|| {
        {
            let span = tracing::info_span!("read");
            let _entered = span.enter();
            tracing::info!("started");
            tracing::info!("finished");
        }
        {
            let span = tracing::info_span!("write");
            let _entered = span.enter();
            tracing::info!("started");
        }
    });

    assert_eq!(lines.len(), 3);
    assert_eq!(
        lines[0]["trace_id"], lines[1]["trace_id"],
        "one span, one trace"
    );
    assert_eq!(
        lines[0]["span_id"], lines[1]["span_id"],
        "one span, one span id"
    );
    assert_ne!(
        lines[0]["trace_id"], lines[2]["trace_id"],
        "a separate root span is a separate trace"
    );
}
