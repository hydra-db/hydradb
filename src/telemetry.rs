//! `tracing` initialization for turbolay.
//!
//! Observability is a cross-cutting concern that spans every milestone
//! (RFC 0017). At M0 there is nothing to instrument beyond the storage
//! foundation, but wiring the subscriber up now means every span and event we
//! add later (write-path phase timers, invariant counters, the instrumented
//! object store) is emitted from day one.
//!
//! Library code emits spans/events via the `tracing` macros and stays
//! subscriber-agnostic; only the binary (or a test harness) installs a
//! subscriber via [`init`] / [`try_init`].
//!
//! # Metrics are a separate, exporter-agnostic facade (RFC 0017 Phase 0)
//!
//! [`crate::obs`]'s recording sites (the instrumented object store, the
//! write-path phase timers, `turbolay_latest_seq`, the invariant counters)
//! call the `metrics` facade directly and need nothing installed here — the
//! facade is a no-op until a recorder is installed, which is deliberately
//! *not* this module's job. A Prometheus (or other) exporter is an HTTP-plane
//! concern that lands with the service (M3, RFC 0017 §3.8/§8 Phase 2), kept
//! separate so every recording site stays exporter-agnostic in the meantime.

use tracing_subscriber::EnvFilter;

/// Installs a `fmt` subscriber driven by the `RUST_LOG` environment variable,
/// returning an error if a global subscriber is already set.
///
/// Falls back to the `info` level when `RUST_LOG` is unset or unparseable.
/// Prefer this in tests and embedders that may race to initialize; use
/// [`init`] in a binary's `main` where a double-init is a programming error.
pub fn try_init() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()?;
    Ok(())
}

/// Installs the global `tracing` subscriber, panicking if one is already set.
///
/// Intended for a binary's `main`. See [`try_init`] for the fallible variant.
pub fn init() {
    try_init().expect("failed to install tracing subscriber");
}
