//! Carrying a W3C trace context across the internal query transport.
//!
//! # Why this is a hook and not a dependency
//!
//! The kernel emits through the plain [`tracing`] facade and must not depend on
//! `turbolay-telemetry` or on any `opentelemetry-*` crate — the rule
//! `turbolay-placement` follows, and the reason `cargo test` stays free of the
//! OTel dependency tree.
//!
//! That rule collides with a fact about `tracing`: a span's id is an internal
//! per-subscriber handle, **not** an OpenTelemetry trace id. So the kernel
//! cannot format a `traceparent` for an outbound request, and cannot attach an
//! inbound one to a span, using the facade alone. Both operations need the
//! `tracing-opentelemetry` bridge, which lives on the far side of the
//! dependency arrow.
//!
//! The way out is an inversion: the kernel declares what it needs
//! ([`TraceContextBridge`]), the binary installs an implementation at startup,
//! and with nothing installed every call here is a cheap `None`. Tests,
//! benchmarks and any build without the `otlp` feature take that path and pay
//! one relaxed atomic load.
//!
//! # What crosses the wire
//!
//! A `traceparent` and nothing else. It is 55 bytes of printable ASCII with a
//! fixed layout, it is defined by W3C rather than by us, and it identifies a
//! trace — it carries no query text, no parameters and no tenant data. That is
//! what makes it safe to log and safe to accept from a peer.
//!
//! `tracestate` is deliberately not carried. It is vendor-specific, unbounded
//! in practice, and nothing in this system produces one.

use std::sync::OnceLock;

/// Converts between the ambient `tracing` span and a W3C `traceparent`.
///
/// Implemented in `turbolay-telemetry` over `tracing-opentelemetry`, and
/// installed once by whichever binary owns the subscriber.
pub trait TraceContextBridge: Send + Sync {
    /// Format the currently active span as a `traceparent`, if there is one and
    /// it is sampled.
    ///
    /// Returning `None` is normal and must stay cheap: it is what an unsampled
    /// trace, a disabled subscriber and a process with no exporter all look
    /// like.
    fn current_traceparent(&self) -> Option<String>;

    /// Make `span` a child of the remote trace named by `traceparent`.
    ///
    /// Called before `span` is entered. A malformed value must be ignored
    /// rather than raised — see [`adopt_remote_parent`].
    fn adopt_remote_parent(&self, span: &tracing::Span, traceparent: &str);
}

static BRIDGE: OnceLock<&'static dyn TraceContextBridge> = OnceLock::new();

/// Install the process-wide bridge. Call once, from `main`, after the
/// subscriber is installed.
///
/// Returns `Err` if one is already installed. Nothing in the kernel calls this;
/// it exists for the binaries.
pub fn install_trace_context_bridge(
    bridge: &'static dyn TraceContextBridge,
) -> std::result::Result<(), &'static str> {
    BRIDGE
        .set(bridge)
        .map_err(|_| "a trace context bridge is already installed")
}

/// The `traceparent` for the currently active span, or `None`.
///
/// `None` whenever no bridge is installed, which is every test and every build
/// without the `otlp` feature.
///
/// Gated: only the query transport sends one, so without that feature there is
/// no outbound request to attach it to.
#[cfg(feature = "query-transport")]
pub fn current_traceparent() -> Option<String> {
    BRIDGE.get()?.current_traceparent()
}

/// Attach an inbound `traceparent` to `span`, joining the caller's trace.
///
/// **A bad value is dropped, never raised.** The trace context arrives from a
/// peer over the internal transport, and a request that is otherwise valid must
/// not fail because its telemetry header is malformed — that would turn an
/// observability feature into an availability risk, and it would do so first
/// during exactly the mixed-version rollout this field was designed to survive.
/// The span simply starts a new trace, which is the pre-5b behaviour.
///
/// Length is checked here rather than in the bridge so the guard holds no
/// matter which implementation is installed. A `traceparent` is a fixed 55
/// bytes; anything else is not one.
///
/// Gated on the two transports that can carry an inbound trace context: the
/// internal query transport (node to node) and Bolt (caller to node).
#[cfg(any(feature = "query-transport", feature = "bolt-server"))]
pub fn adopt_remote_parent(span: &tracing::Span, traceparent: Option<&str>) {
    let Some(traceparent) = traceparent else {
        return;
    };
    if traceparent.len() != TRACEPARENT_LEN || !traceparent.is_ascii() {
        return;
    }
    if let Some(bridge) = BRIDGE.get() {
        bridge.adopt_remote_parent(span, traceparent);
    }
}

/// Length of a well-formed `traceparent`, per W3C Trace Context.
///
/// Mirrors `turbolay_telemetry::propagate::TRACEPARENT_LEN`; the two cannot be
/// shared, because sharing them is the dependency this module exists to avoid.
#[cfg(any(feature = "query-transport", feature = "bolt-server"))]
const TRACEPARENT_LEN: usize = 55;

// The bridge is only reachable through a transport, so with neither feature on
// there is nothing here to exercise.
#[cfg(all(test, any(feature = "query-transport", feature = "bolt-server")))]
mod tests {
    use super::*;

    /// The path every test and every non-`otlp` build takes.
    #[cfg(feature = "query-transport")]
    #[test]
    fn no_bridge_installed_yields_no_traceparent() {
        assert!(current_traceparent().is_none());
    }

    /// A peer sending rubbish must not be able to fail a request, and must not
    /// reach the bridge at all.
    #[test]
    fn malformed_values_are_dropped_rather_than_raised() {
        let span = tracing::info_span!("test");
        adopt_remote_parent(&span, None);
        adopt_remote_parent(&span, Some(""));
        adopt_remote_parent(&span, Some("00-too-short-01"));
        adopt_remote_parent(&span, Some(&"x".repeat(4096)));
        // A correctly sized value that is not ASCII still must not panic.
        adopt_remote_parent(&span, Some(&"é".repeat(TRACEPARENT_LEN)));
    }
}
