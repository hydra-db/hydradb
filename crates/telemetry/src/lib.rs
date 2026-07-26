//! Telemetry for the Turbolay binaries: one subscriber, two service
//! identities, and an optional OTLP pipeline for logs and traces.
//!
//! # The shape of this crate
//!
//! `turbolay-telemetry` does **not** depend on `slatedb-graph-kernel`, and the
//! kernel must not gain a dependency on it. The arrow points this way for the
//! same reason it does in `turbolay-placement`: the kernel emits through the
//! plain [`tracing`] facade — which compiles to a no-op when no subscriber is
//! installed, so it stays free in tests and benchmarks — and this crate owns
//! only the *subscriber* side, deciding how those spans and events become OTLP.
//!
//! The practical payoff is that adding OpenTelemetry touches no `[features]`
//! entry in the root manifest. Every existing feature combination keeps
//! compiling unchanged, and `cargo test` never pulls `opentelemetry-*`.
//!
//! # Usage
//!
//! ```no_run
//! use turbolay_telemetry::{ServiceIdentity, TelemetryConfig};
//!
//! # fn main() -> Result<(), turbolay_telemetry::TelemetryError> {
//! let guard = turbolay_telemetry::init(TelemetryConfig::from_env(
//!     ServiceIdentity::GraphNode,
//! ))?;
//! // … run the server …
//! guard.shutdown();
//! # Ok(())
//! # }
//! ```
//!
//! [`init`] is total: with no `OTEL_EXPORTER_OTLP_ENDPOINT` set it installs the
//! fmt layer alone and returns successfully. A missing collector is never a
//! reason a node fails to boot.
//!
//! # What lives where
//!
//! | Module | Contents |
//! |---|---|
//! | [`config`] | [`ServiceIdentity`], [`TelemetryConfig`], environment resolution |
//! | [`semconv`] | the `turbolay.*` attribute registry |
//! | [`redact`] | the field denylist and the visitor that applies it |
//! | [`propagate`] | W3C `traceparent`, with no OTel dependency |
//! | [`error_class`] | the `error.class` vocabulary and [`Outcome`] |
//! | [`layers`] | subscriber assembly |
//! | `otlp` | exporter wiring — `otlp` feature only |
//! | `sampling` | the head sampler — `otlp` feature only |
//!
//! # Status
//!
//! This crate is Step 1 of `docs/plans/2026-07-26-otel-telemetry-crate.md`. It
//! is standalone and complete on its own terms: nothing in `src/` calls it yet,
//! and wiring the two binaries is a separate change. Steps 2–5 — the read,
//! write and indexing span trees, then cross-node propagation — instrument the
//! kernel against the vocabulary defined here.
//!
//! ## Wiring it in
//!
//! The crate is deliberately **not** yet a workspace member, so it cannot
//! conflict with concurrent edits to the root manifest. Three steps, all in the
//! same commit:
//!
//! 1. Add `"crates/telemetry"` to `[workspace] members` in the root
//!    `Cargo.toml`.
//! 2. Hoist the six `opentelemetry-*` versions from this crate's `Cargo.toml`
//!    into the root `[workspace.dependencies]`, and switch them to
//!    `{ workspace = true, optional = true }` here — `optional` is not
//!    inheritable and must stay at the use site. `chrono`, `serde_json`,
//!    `thiserror`, `tracing` and `tracing-subscriber` already inherit.
//! 3. Replace `init_tracing()` in `src/bin/graph-node.rs:227` and the
//!    `tracing_subscriber::fmt()` block at `src/bin/graph-indexer.rs:44` with
//!    [`init`], and hold the returned guard until the end of `main`.
//!
//! Until step 1 lands, build and test it directly:
//! `cargo test -p turbolay-telemetry --features otlp`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod error_class;
pub mod layers;
pub mod propagate;
pub mod redact;
pub mod semconv;

#[cfg(feature = "otlp")]
pub mod bridge;
#[cfg(feature = "otlp")]
pub mod otlp;
#[cfg(feature = "otlp")]
pub mod sampling;

pub use config::{OtlpProtocol, ServiceIdentity, TelemetryConfig};
pub use error_class::{ErrorClass, Outcome};
pub use propagate::{TraceContext, TraceContextError};

use thiserror::Error;

/// Why telemetry initialisation failed.
///
/// Deliberately small. Almost everything that can go wrong — an unparseable
/// sampling ratio, a malformed header, a missing pod name — is *degraded*
/// rather than raised, because a node that will not start is a worse outcome
/// than a node with imperfect telemetry.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TelemetryError {
    /// A subscriber was already installed in this process.
    #[error("a global tracing subscriber is already installed")]
    AlreadyInitialised,

    /// The `EnvFilter` directives did not parse.
    #[error("invalid log filter {directives:?}: {source}")]
    Filter {
        /// The directives as given.
        directives: String,
        /// The underlying parse error.
        #[source]
        source: tracing_subscriber::filter::ParseError,
    },

    /// The OTLP exporter could not be built.
    #[error("could not build the OTLP exporter: {0}")]
    Exporter(String),
}

/// Keeps the telemetry pipeline alive, and flushes it on the way out.
///
/// Must be held for the life of the process. Dropping it — or calling
/// [`TelemetryGuard::shutdown`] — flushes any batched spans and logs. Without
/// that flush the last few seconds before a shutdown are lost, which is
/// precisely the window that matters when diagnosing why a pod restarted.
#[must_use = "dropping the guard immediately shuts telemetry down"]
pub struct TelemetryGuard {
    #[cfg(feature = "otlp")]
    providers: Option<otlp::Providers>,
    /// Keeps the type inhabited and `Drop`-able when `otlp` is off.
    #[cfg(not(feature = "otlp"))]
    _private: (),
}

impl TelemetryGuard {
    /// Flush and shut down the exporters.
    ///
    /// Idempotent, and equivalent to dropping the guard. Prefer calling it
    /// explicitly at the end of `main` so the flush happens somewhere visible
    /// rather than in a destructor whose ordering is easy to get wrong.
    pub fn shutdown(self) {
        drop(self);
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otlp")]
        if let Some(providers) = self.providers.take() {
            providers.shutdown();
        }
    }
}

impl std::fmt::Debug for TelemetryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryGuard").finish_non_exhaustive()
    }
}

/// Install the global subscriber.
///
/// Call once, as early in `main` as possible. Returns [`TelemetryError`] only
/// for the two conditions a caller can actually act on: a filter that does not
/// parse, and a subscriber that is already installed.
///
/// With [`TelemetryConfig::otlp_endpoint`] unset — or with the `otlp` feature
/// off — this installs the fmt layer alone and no exporter, which is what tests
/// and local runs want.
pub fn init(config: TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    layers::install(config)
}
