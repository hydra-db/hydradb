use crate::core::memory_diagnostics::{request_estimated_bytes, MemoryDiagnosticGuard, MemoryStage};
use crate::*;
use slatedb::DbTransaction;
use std::sync::{atomic::Ordering, Arc};
use tokio::sync::OwnedSemaphorePermit;
use tracing::Instrument as _;
/// The graph write permit, held from [`SlateGraphStore::acquire_graph_write_permit`]
/// until drop.
///
/// A guard around the semaphore permit rather than the permit itself so the
/// hold is measured on every release path — a committed write, an error
/// returned with `?`, a cancelled future, a panic unwinding past the holder —
/// and not only where a caller remembered to record it. `write_permit_held_us`
/// accumulates the hold; `write_permit_wait_us` and
/// `write_permit_acquisitions` are recorded once at construction, so the three
/// agree on which acquisitions they describe.
pub struct GraphWritePermit {
    _permit: OwnedSemaphorePermit,
    acquired_at: std::time::Instant,
    metrics: Arc<GraphOperationalMetrics>,
}

impl GraphWritePermit {
    fn acquired(permit: OwnedSemaphorePermit, queued_at: std::time::Instant, metrics: Arc<GraphOperationalMetrics>) -> Self {
        let acquired_at = std::time::Instant::now();
        metrics.write_permit_acquisitions.fetch_add(1, Ordering::Relaxed);
        metrics
            .write_permit_wait_us
            .fetch_add(elapsed_micros(queued_at, acquired_at), Ordering::Relaxed);
        Self {
            _permit: permit,
            acquired_at,
            metrics,
        }
    }
}

impl Drop for GraphWritePermit {
    fn drop(&mut self) {
        let held = elapsed_micros(self.acquired_at, std::time::Instant::now());
        self.metrics.write_permit_held_us.fetch_add(held, Ordering::Relaxed);
    }
}

/// `later - earlier` in whole microseconds, saturating rather than wrapping on
/// a clock that did not advance.
fn elapsed_micros(earlier: std::time::Instant, later: std::time::Instant) -> u64 {
    u64::try_from(later.saturating_duration_since(earlier).as_micros()).unwrap_or(u64::MAX)
}

impl SlateGraphStore {
    pub async fn acquire_local_write_guard_no_fence(&self, cell_id: &str, operation: &'static str) -> Result<LocalWriteGuard> {
        validate_component("cell_id", cell_id)?;
        validate_component("operation", operation)?;
        let guard = LocalWriteGuard::new(
            Arc::clone(&self.mutation_state().local_write_guard)
                .lock_owned()
                .instrument(tracing::info_span!(
                    "shard.local_write_guard",
                    hydradb.cell_id = %cell_id,
                    hydradb.write.operation = operation
                ))
                .await,
        );
        self.await_prior_write_durability().await?;
        Ok(guard)
    }

    pub async fn validate_changing_write(&self) -> Result<()> {
        if self.mutation_state().await_durable_writes && self.writer()?.snapshot().is_ok() {
            return Ok(());
        }
        self.refresh_writer_fence().await
    }

    pub async fn acquire_graph_write_permit(
        &self,
        operation: &'static str,
    ) -> Result<(GraphWritePermit, MemoryDiagnosticGuard, MemoryDiagnosticGuard)> {
        self.mutation_state()
            .operation_metrics
            .write_attempts
            .fetch_add(1, Ordering::Relaxed);
        let bytes = request_estimated_bytes();
        let waiting = MemoryDiagnosticGuard::new(MemoryStage::ShardWriteWait, bytes);
        let queued_at = std::time::Instant::now();
        if self.mutation_state().graph_write_gate.available_permits() == 0 {
            self.mutation_state()
                .operation_metrics
                .backpressure_waits
                .fetch_add(1, Ordering::Relaxed);
        }
        let acquire = self.mutation_state().graph_write_gate.clone().acquire_owned();
        let permit = match crate::QueryCancellationToken::current() {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return Err(GraphError::QueryTimeout {
                    operation: "query_cancelled",
                    elapsed_ms: 0,
                    limit_ms: 0,
                }),
                permit = acquire => permit.map_err(|err| GraphError::CorruptValue {
                    key: format!("backpressure/{operation}"),
                    reason: format!("operation gate closed: {err}"),
                })?,
            },
            None => acquire.await.map_err(|err| GraphError::CorruptValue {
                key: format!("backpressure/{operation}"),
                reason: format!("operation gate closed: {err}"),
            })?,
        };
        let permit = GraphWritePermit::acquired(permit, queued_at, Arc::clone(&self.mutation_state().operation_metrics));
        drop(waiting);
        let hold = if crate::write_pipeline::current().is_some() {
            MemoryStage::ShardWriteActivePipeline
        } else {
            MemoryStage::ShardWriteActiveInline
        };
        Ok((
            permit,
            MemoryDiagnosticGuard::new(MemoryStage::ShardWriteActive, bytes),
            MemoryDiagnosticGuard::new(hold, 0),
        ))
    }

    pub fn ensure_write_authority(&self, cell_id: &str, operation: &'static str) -> Result<()> {
        let span = tracing::info_span!(
            "writer.authority",
            hydradb.cell_id = %cell_id,
            error.class = tracing::field::Empty,
        );
        let _entered = span.enter();
        match &self.mutation_state().write_authority {
            GraphWriteAuthority::ReadOnly => Err(GraphError::WriteRequiresWriter {
                operation,
                cell_id: cell_id.to_string(),
            }),
            GraphWriteAuthority::Promotable | GraphWriteAuthority::Writer => self.writer().map(|_| ()),
        }
        .inspect_err(|error| {
            tracing::Span::current().record("error.class", error.class());
        })
    }

    pub async fn validate_write_fence_txn(&self, txn: &DbTransaction, cell_id: &str, operation: &'static str) -> Result<()> {
        if operation != "drop_cell" {
            let drop_marker = keys::cell_drop_marker(cell_id);
            let pending_drop_marker = keys::cell_drop_pending_marker(cell_id);
            let (dropped, pending) = tokio::join!(read_txn_remote(txn, &drop_marker), read_txn_remote(txn, &pending_drop_marker),);
            if dropped?.is_some() || pending?.is_some() {
                return Err(GraphError::CellDropped {
                    operation,
                    cell_id: cell_id.to_string(),
                });
            }
        }
        self.ensure_write_authority(cell_id, operation)
    }
}
