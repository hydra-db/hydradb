//! Turning the active span into a W3C `traceparent`, and back.
//!
//! # Why these are free functions and not a trait implementation
//!
//! The kernel declares a `TraceContextBridge` trait for exactly these two
//! operations, and the obvious move is to implement it here. That would be
//! wrong: implementing a trait requires naming it, naming it requires
//! depending on `slatedb-graph-kernel`, and this crate must not — it is the
//! same arrow `turbolay-placement` keeps pointing away from the kernel, and
//! the reason `cargo test` never pulls `opentelemetry-*`.
//!
//! So neither side names the other. The kernel declares the trait, this module
//! provides the two operations as plain functions, and the **binary** — which
//! already depends on both, and is the only place that legitimately knows about
//! both — writes the dozen-line adapter that joins them.
//!
//! # Why the kernel cannot do this itself
//!
//! A `tracing` span id is an internal, per-subscriber handle. It is not an
//! OpenTelemetry trace id and cannot be converted into one. Only code linked
//! against `tracing-opentelemetry` can read the OTel context off a span, which
//! is why this lives behind the `otlp` feature: with no exporter there are no
//! OpenTelemetry ids to propagate, and inventing some would produce a
//! `traceparent` that joins to nothing.

use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::propagate::TraceContext;

/// Format the currently active span as a `traceparent`.
///
/// `None` when no span is active or the context is invalid — the normal case
/// for a span created while no tracer is running. It never fabricates one.
pub fn current_traceparent() -> Option<String> {
    let context = tracing::Span::current().context();
    let span = context.span();
    let span_context = span.span_context();
    if !span_context.is_valid() {
        return None;
    }
    // Unsampled contexts are propagated too, rather than dropped. The remote
    // node's sampler has to see the parent's flags to reach the *same*
    // decision; withholding the header would let one node keep a trace that the
    // next silently restarts, which reads in a backend as two unrelated traces
    // instead of one consistently dropped one.
    let traceparent = TraceContext::new(
        span_context.trace_id().to_bytes(),
        span_context.span_id().to_bytes(),
        span_context.is_sampled(),
    )?;
    Some(traceparent.to_string())
}

/// Make `span` a child of the remote trace named by `traceparent`.
///
/// A malformed value is ignored. This is the boundary where a peer's bytes
/// become part of our trace graph, so the value is re-parsed here in full
/// rather than trusted from the caller's length check.
pub fn adopt_remote_parent(span: &tracing::Span, traceparent: &str) {
    let Ok(parsed) = TraceContext::parse(traceparent) else {
        return;
    };
    let span_context = SpanContext::new(
        TraceId::from_bytes(parsed.trace_id()),
        opentelemetry::trace::SpanId::from_bytes(parsed.span_id()),
        TraceFlags::new(parsed.flags()),
        // Remote: this context was created in another process. The SDK uses the
        // flag to decide the parent is not one of its own spans, which is what
        // makes a backend draw a cross-service edge rather than treat it as a
        // local parent it has somehow lost.
        true,
        // `tracestate` is not carried across this transport: it is
        // vendor-specific and nothing in this system produces one.
        TraceState::default(),
    );
    // Discarded deliberately. `set_parent` fails only when no
    // `tracing-opentelemetry` layer is installed, which is a build-time
    // property of the process, not a property of this request — and a request
    // must not fail because its trace could not be joined.
    let _ = span.set_parent(Context::new().with_remote_span_context(span_context));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The path a `--features otlp` build takes with no collector configured.
    /// It must not fabricate a header.
    #[test]
    fn no_active_trace_yields_no_traceparent() {
        assert!(current_traceparent().is_none());
    }

    /// Rubbish from a peer must be dropped, not panic and not poison the span.
    #[test]
    fn a_malformed_traceparent_is_ignored() {
        let span = tracing::info_span!("test");
        adopt_remote_parent(&span, "not a traceparent");
        adopt_remote_parent(&span, "");
        adopt_remote_parent(&span, &"0".repeat(55));
    }

    /// The exact wire form the transport carries, from the W3C spec's example.
    #[test]
    fn a_well_formed_traceparent_is_adopted() {
        let span = tracing::info_span!("test");
        adopt_remote_parent(
            &span,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
    }
}
