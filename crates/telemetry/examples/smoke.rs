//! Prints a few lines through a real, globally installed subscriber.
//!
//! This exists because the failure it guards against is invisible to a unit
//! test. The layer tests use `with_default` and a local subscriber; the bug
//! they cannot see lives in the *global* stack, where attaching an empty OTLP
//! layer vector makes `register_callsite` return `Interest::never()` and
//! silences every callsite in the process — fmt layer included. The process
//! boots, works, and prints nothing.
//!
//! So: run it and look.
//!
//! ```sh
//! # Must print four lines, one of them carrying span context.
//! cargo run -p turbolay-telemetry --features otlp --example smoke
//!
//! # Must behave identically — this is the ordinary "built with otlp, no
//! # collector deployed" configuration.
//! OTEL_EXPORTER_OTLP_ENDPOINT= cargo run -p turbolay-telemetry --features otlp --example smoke
//!
//! # And without the feature at all.
//! cargo run -p turbolay-telemetry --example smoke
//! ```
//!
//! Exits non-zero if `init` fails, so it is usable as a shell check.

use turbolay_telemetry::{ServiceIdentity, TelemetryConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let telemetry =
        turbolay_telemetry::init(TelemetryConfig::from_env(ServiceIdentity::GraphNode))?;

    tracing::info!("smoke: first line, outside any span");

    // `turbolay.writer.epoch` is declared Empty and filled later, the way every
    // deferred attribute on the write path is. The creation-time
    // `turbolay.cell_id` must survive that `record` — it used not to.
    let span = tracing::info_span!(
        "smoke.parent",
        turbolay.cell_id = "cell-0",
        turbolay.writer.epoch = tracing::field::Empty,
    );
    span.record("turbolay.writer.epoch", 7_u64);
    span.in_scope(|| {
        tracing::info!(turbolay.read_epoch = 41_u64, "smoke: inside a span");
        let child = tracing::info_span!("smoke.child", turbolay.generation = 412_u64);
        child.in_scope(|| {
            tracing::warn!("smoke: nested, and this one is a warning");
        });
    });

    telemetry.shutdown();
    Ok(())
}
