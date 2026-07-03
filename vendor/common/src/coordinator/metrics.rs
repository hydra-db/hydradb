//! Metrics for the write coordinator.
//!
//! These metric names are stable strings shared between the coordinator and
//! the flush task. Calls are no-ops when no global recorder is installed.

// ── Write-queue ingress (the channel that backs WriteCoordinatorHandle) ──

/// Histogram of how long it takes to enqueue a coordinator command.
/// Labels: `command` ∈ {`write`, `write_timeout`, `try_write`, `flush`},
/// `status` ∈ {`ok`, `backpressure`, `timeout`, `shutdown`}.
pub(crate) const COORDINATOR_SEND_DURATION_SECONDS: &str =
    "opendata_write_coordinator_send_duration_seconds";

/// Counter incremented when the queue rejects a send because it is full.
/// Labels: `command`, `reason` ∈ {`full`, `timeout`, `closed`}.
pub(crate) const COORDINATOR_QUEUE_BACKPRESSURE_TOTAL: &str =
    "opendata_write_coordinator_queue_backpressure_total";

/// Approximate depth of the write queue, sampled on each send.
/// Labels: `channel`.
pub(crate) const COORDINATOR_QUEUE_DEPTH: &str = "opendata_write_coordinator_queue_depth";

// ── Delta lifecycle (apply / freeze / flush dispatch) ──

/// Histogram of `Delta::apply` durations on the coordinator hot path.
pub(crate) const COORDINATOR_DELTA_APPLY_DURATION_SECONDS: &str =
    "opendata_write_coordinator_delta_apply_duration_seconds";

/// Estimated current delta size in bytes (gauge), sampled after each apply.
pub(crate) const COORDINATOR_DELTA_ESTIMATED_BYTES: &str =
    "opendata_write_coordinator_delta_estimated_bytes";

/// Histogram of `Delta::freeze` durations.
pub(crate) const COORDINATOR_DELTA_FREEZE_DURATION_SECONDS: &str =
    "opendata_write_coordinator_delta_freeze_duration_seconds";

/// Histogram of how long the coordinator waits when sending a flush event to
/// the flush task. A high value means the flush task is the bottleneck and
/// new writes will not be accepted until the event is dispatched.
pub(crate) const COORDINATOR_FLUSH_EVENT_SEND_DURATION_SECONDS: &str =
    "opendata_write_coordinator_flush_event_send_duration_seconds";

/// Approximate depth of the flush event channel, sampled on each send.
pub(crate) const COORDINATOR_FLUSH_EVENT_QUEUE_DEPTH: &str =
    "opendata_write_coordinator_flush_event_queue_depth";

/// Counter of flush triggers, labeled by reason.
/// Labels: `reason` ∈ {`size_threshold`, `interval`, `explicit`, `shutdown`}.
pub(crate) const COORDINATOR_FLUSH_TOTAL: &str = "opendata_write_coordinator_flush_total";

// ── Flush task (background) ──

/// Histogram of `Flusher::flush_delta` durations on the background flush task.
pub(crate) const COORDINATOR_FLUSH_DELTA_DURATION_SECONDS: &str =
    "opendata_write_coordinator_flush_delta_duration_seconds";

/// Histogram of `Flusher::flush_storage` durations on the background flush task.
pub(crate) const COORDINATOR_FLUSH_STORAGE_DURATION_SECONDS: &str =
    "opendata_write_coordinator_flush_storage_duration_seconds";

/// Describe all coordinator metrics on the global recorder. Safe to call more
/// than once; the `metrics` crate dedupes descriptions.
pub fn describe_coordinator_metrics() {
    metrics::describe_histogram!(
        COORDINATOR_SEND_DURATION_SECONDS,
        "Latency of submitting a command to the write coordinator queue (seconds)"
    );
    metrics::describe_counter!(
        COORDINATOR_QUEUE_BACKPRESSURE_TOTAL,
        "Coordinator queue rejections due to a full or closed queue"
    );
    metrics::describe_gauge!(
        COORDINATOR_QUEUE_DEPTH,
        "Approximate write coordinator queue depth, sampled on each send"
    );
    metrics::describe_histogram!(
        COORDINATOR_DELTA_APPLY_DURATION_SECONDS,
        "Time spent in Delta::apply on the coordinator hot path (seconds)"
    );
    metrics::describe_gauge!(
        COORDINATOR_DELTA_ESTIMATED_BYTES,
        "Estimated bytes in the current (mutable) delta"
    );
    metrics::describe_histogram!(
        COORDINATOR_DELTA_FREEZE_DURATION_SECONDS,
        "Time spent freezing the current delta on the coordinator hot path (seconds)"
    );
    metrics::describe_histogram!(
        COORDINATOR_FLUSH_EVENT_SEND_DURATION_SECONDS,
        "Time spent dispatching a flush event from the coordinator to the flush task (seconds)"
    );
    metrics::describe_gauge!(
        COORDINATOR_FLUSH_EVENT_QUEUE_DEPTH,
        "Approximate depth of the coordinator's flush-event channel"
    );
    metrics::describe_counter!(
        COORDINATOR_FLUSH_TOTAL,
        "Coordinator flush triggers labeled by reason"
    );
    metrics::describe_histogram!(
        COORDINATOR_FLUSH_DELTA_DURATION_SECONDS,
        "Background Flusher::flush_delta duration (seconds)"
    );
    metrics::describe_histogram!(
        COORDINATOR_FLUSH_STORAGE_DURATION_SECONDS,
        "Background Flusher::flush_storage duration (seconds)"
    );
}
