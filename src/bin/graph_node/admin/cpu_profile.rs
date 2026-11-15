//! On-demand, process-wide CPU samples on the graph node's admin listener.
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use pprof::protos::Message;
use tokio::sync::Semaphore;

use super::{resource_profile::Options, valid_control_bearer, AdminState};

// ITIMER_PROF and SIGPROF belong to the process, even if it has multiple listeners.
static CAPTURE: Semaphore = Semaphore::const_new(1);
const FREQUENCY_HZ: i32 = 49;

pub(super) async fn handle(State(state): State<AdminState>, headers: HeaderMap, Query(options): Query<Options>) -> Response {
    if !valid_control_bearer(&headers, &state.control_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let duration = match options.duration() {
        Ok(d) => d,
        Err(e) => return e.into_response(),
    };
    let Ok(permit) = CAPTURE.try_acquire() else {
        return (StatusCode::CONFLICT, "a CPU profile is already in progress").into_response();
    };

    // Symbol resolution can block. Keep the permit in this task so client disconnects
    // cannot start overlapping captures while the first report is still being built.
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        capture(duration)
    })
    .await
    {
        Ok(Ok(profile)) => (
            [("content-type", "application/octet-stream"), ("cache-control", "no-store")],
            profile,
        )
            .into_response(),
        Ok(Err(error)) => {
            tracing::warn!(%error, "CPU profile capture failed");
            (StatusCode::SERVICE_UNAVAILABLE, "CPU profile unavailable").into_response()
        }
        Err(error) => {
            tracing::error!(%error, "CPU profile task failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn capture(duration: Duration) -> pprof::Result<Vec<u8>> {
    let builder = pprof::ProfilerGuardBuilder::default().frequency(FREQUENCY_HZ);
    // Skip the loader and unwinder when interrupted inside them by SIGPROF.
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    let builder = builder.blocklist(&["libc.so", "libgcc", "libpthread", "ld-linux", "libunwind", "vdso"]);
    let guard = builder.build()?;
    std::thread::sleep(duration);
    let report = guard
        .report()
        .frames_post_processor(|stack| {
            // The unwinder starts in the signal handler. pprof removes the signal
            // trampoline but can leave its own Rust callbacks above the interrupted PC.
            if let Some(last) = stack.frames.iter().rposition(|frame| {
                frame
                    .iter()
                    .any(|symbol| symbol.name().contains("pprof::profiler::perf_signal_handler"))
            }) {
                stack.frames.drain(..=last);
            }
        })
        .build()?;
    drop(guard);
    Ok(report.pprof()?.encode_to_vec())
}
