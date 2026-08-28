//! End-to-end head-sampling behaviour, through a real `tracing` subscriber.
//!
//! The unit tests in `sampling.rs` hand attribute slices straight to
//! [`ShouldSample::should_sample`], which is the one thing production never
//! does. Every attribute in this codebase reaches the sampler — or fails to —
//! by way of `tracing`: a `tracing::info_span!` with some fields set at the
//! callsite and others declared `tracing::field::Empty` and filled in later with
//! `Span::record`. Whether a given field is visible to the sampler is decided
//! entirely by that machinery, so a test that skips it can pass while the real
//! path does the opposite.
//!
//! Hence this file. Everything here goes through
//! `tracing_opentelemetry::layer()` over a real [`SdkTracerProvider`] carrying
//! [`HydraDBSampler`], and asserts on what a [`SpanProcessor`] actually
//! receives. A dropped span is never handed to a processor, so "did this span
//! survive" is simply "did anything arrive".
//!
//! The ratio is pinned at 0.0 throughout: it makes the ratio path a constant, so
//! every assertion below is about the force path and nothing else.

#![cfg(feature = "otlp")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use hydradb_telemetry::sampling::HydraDBSampler;
use hydradb_telemetry::semconv;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanProcessor};
use tracing_subscriber::layer::SubscriberExt;

/// Collects whatever survives sampling.
#[derive(Debug, Default, Clone)]
struct Capture(Arc<Mutex<Vec<SpanData>>>);

impl SpanProcessor for Capture {
    fn on_start(&self, _span: &mut opentelemetry_sdk::trace::Span, _cx: &opentelemetry::Context) {}

    fn on_end(&self, span: SpanData) {
        self.0.lock().unwrap().push(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
}

/// Run `body` with the sampler installed at `ratio`, and return the names of
/// the spans that reached the exporter.
fn exported_spans(ratio: f64, body: impl FnOnce()) -> Vec<String> {
    let capture = Capture::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(HydraDBSampler::new(ratio))
        .with_span_processor(capture.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer().with_tracer(provider.tracer("hydradb-head-sampling-test")),
    );
    tracing::subscriber::with_default(subscriber, body);
    let _ = provider.force_flush();
    let exported = capture.0.lock().unwrap();
    exported.iter().map(|span| span.name.to_string()).collect()
}

/// The shape every one of the seven force sites in `src/` actually has: the
/// field is declared empty at the callsite because its value is not known yet,
/// the span is entered, the work runs, and only then is the value recorded.
///
/// It must not keep the trace, and the point of asserting it here is that this
/// is not an accident of ordering to be tidied up later — it is what head
/// sampling *is*. The decision was taken when the span started. Anything that
/// wants to change it has to be a tail decision, which is why those sites record
/// [`semconv::SAMPLING_TAIL_KEEP`] and this one is empty.
#[test]
fn a_force_recorded_after_the_span_starts_cannot_keep_the_trace() {
    let exported = exported_spans(0.0, || {
        let span =
            tracing::info_span!("query.plan", hydradb.sampling.force = tracing::field::Empty,);
        let entered = span.enter();
        // …the planner runs, and only now is the verdict known…
        span.record("hydradb.sampling.force", true);
        drop(entered);
    });
    assert!(
        exported.is_empty(),
        "a post-hoc force kept the trace, so the head sampler is reading \
         attributes it cannot have seen at span start: {exported:?}"
    );
}

/// The contract that *is* honoured: known before the work starts, on a span
/// that starts a trace.
#[test]
fn a_force_set_at_span_creation_keeps_the_trace() {
    for exported in [
        exported_spans(0.0, || {
            let _entered =
                tracing::info_span!("client.query", hydradb.sampling.force = true).entered();
        }),
        // A stringly-typed caller must work too — `Value::as_str` is what the
        // sampler compares, and a bool and a &str arrive as different variants.
        exported_spans(0.0, || {
            let _entered =
                tracing::info_span!("client.query", hydradb.sampling.force = "true").entered();
        }),
    ] {
        assert_eq!(exported, vec!["client.query".to_string()]);
    }
}

/// Rule 1 beats rule 3: a forced child of a dropped root stays dropped.
///
/// This is the second reason the seven sites in `src/` could not have been
/// rescued by moving their attribute to creation time. Most of them sit on child
/// spans — `query.plan`, `query.execute`, `write.bookmark`, `artifact.lookup`,
/// `storage.wal_tail` — where the force attribute is not consulted at all, so
/// even a correctly-timed force would have been a second no-op behind the first.
/// A surviving child would be a trace with a hole where its root should be,
/// missing the scope, fingerprint and correlation id that make the child mean
/// anything.
#[test]
fn a_forced_child_cannot_resurrect_a_dropped_root() {
    let exported = exported_spans(0.0, || {
        let root = tracing::info_span!("client.query");
        let _root = root.enter();
        let _child = tracing::info_span!("query.plan", hydradb.sampling.force = true).entered();
    });
    assert!(
        exported.is_empty(),
        "a child overruled its dropped parent, which produces a trace with a \
         hole in the middle: {exported:?}"
    );
}

/// `hydradb.query.full_scan` is a *data* attribute and must never steer
/// sampling, even when a callsite does supply it at creation time.
///
/// It used to force a keep, which was dead code twice over — recorded only
/// after the planner had run, and only on a child span. Reviving it by hoisting
/// the field would be worse than the dead code it replaced: full scans are
/// common in an analytics workload, so the configured ratio would silently
/// become 100% for a whole class of query. Keep-worthiness is stated with
/// [`semconv::SAMPLING_FORCE`], which means that and nothing else.
#[test]
fn a_data_attribute_at_span_creation_does_not_keep_the_trace() {
    let exported = exported_spans(0.0, || {
        let _entered = tracing::info_span!("query.plan", hydradb.query.full_scan = true).entered();
    });
    assert!(
        exported.is_empty(),
        "a workload attribute steered the sampler; the configured ratio is not \
         the ratio anyone gets: {exported:?}"
    );
}

/// The tail marker is the collector's input and is inert in this process,
/// whenever it is set. If this ever starts keeping traces the two mechanisms
/// have been conflated again, and post-hoc callsites will once more read like a
/// guarantee they do not carry.
#[test]
fn the_tail_keep_marker_is_inert_in_the_head_sampler() {
    let at_creation = exported_spans(0.0, || {
        let _entered =
            tracing::info_span!("client.query", hydradb.sampling.tail_keep = "error").entered();
    });
    assert!(at_creation.is_empty(), "{at_creation:?}");

    let after_the_fact = exported_spans(0.0, || {
        let span = tracing::info_span!(
            "client.query",
            hydradb.sampling.tail_keep = tracing::field::Empty,
        );
        let entered = span.enter();
        span.record("hydradb.sampling.tail_keep", "error");
        drop(entered);
    });
    assert!(after_the_fact.is_empty(), "{after_the_fact:?}");
}

/// The registry's two sampling keys are distinct strings. Trivial, and the
/// whole bug in one assertion: one name for two mechanisms with different
/// reachability is what made a dead call site look live.
#[test]
fn the_head_and_tail_sampling_keys_are_different_attributes() {
    assert_ne!(semconv::SAMPLING_FORCE, semconv::SAMPLING_TAIL_KEEP);
}

/// The named write-path spans keep working through the real subscriber, not
/// just through a direct `should_sample` call — they are the reason the ratio
/// can be turned down at all.
#[test]
fn writer_spans_survive_a_zero_ratio() {
    let exported = exported_spans(0.0, || {
        let _entered = tracing::info_span!("writer.promote").entered();
    });
    assert_eq!(exported, vec!["writer.promote".to_string()]);
}
