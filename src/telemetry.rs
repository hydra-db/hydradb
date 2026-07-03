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
