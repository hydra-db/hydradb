use crate::core::memory_diagnostics::{request_estimated_bytes, MemoryDiagnosticGuard, MemoryStage, TrackedMemory};
use crate::*;
use slatedb::{
    config::{DurabilityLevel, WriteOptions},
    DbTransaction,
};
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic::Ordering, Arc};
use tokio::sync::OwnedSemaphorePermit;
use tracing::Instrument as _;

pub const MAX_PENDING_DURABLE_WRITES: usize = 16;

tokio::task_local! {
    static PIPELINE: Arc<WritePipeline>;
}

pub struct WritePipeline {
    writer: TrackedMemory<slatedb::Db>,
    path: String,
    writer_epoch: u64,
    required_sequence: AtomicU64,
    _admission: OwnedSemaphorePermit,
}

pub fn current() -> Option<Arc<WritePipeline>> {
    PIPELINE.try_with(Arc::clone).ok()
}

pub fn read_durability() -> DurabilityLevel {
    if current().is_some() {
        DurabilityLevel::Memory
    } else {
        DurabilityLevel::Remote
    }
}

pub async fn wait_durable(writer: &slatedb::Db, sequence: u64) -> Result<()> {
    let mut status = writer.subscribe();
    loop {
        if status.borrow_and_update().durable_seq >= sequence {
            return Ok(());
        }
        // Surface the exact closed/fenced error from this writer generation.
        writer.snapshot()?;
        status.changed().await.map_err(|error| GraphError::CorruptValue {
            key: "write_pipeline/durability".into(),
            reason: format!("writer status channel closed: {error}"),
        })?;
    }
}

impl WritePipeline {
    fn validate_writer(&self, storage: &SlateGraphStore) -> Result<()> {
        if storage.writer_epoch() != Some(self.writer_epoch) {
            return Err(GraphError::ConditionalWriteConflict {
                operation: "write_pipeline_writer_changed",
                key: self.path.clone(),
            });
        }
        Ok(())
    }

    pub async fn commit_local(self: &Arc<Self>, txn: DbTransaction, sequence: u64) -> Result<Option<StorageSequence>> {
        let context = Arc::clone(self);
        let (send, receive) = tokio::sync::oneshot::channel();
        let commit_span = tracing::Span::current();
        // The owned task retains admission and the exact writer until durability,
        // even if its client disappears during local apply or the S3 wait.
        tokio::spawn(async move {
            match txn
                .commit_with_options(&WriteOptions { seqnum: sequence })
                .instrument(commit_span)
                .await
            {
                Ok(handle) => {
                    let committed = handle.map(|handle| handle.seqnum());
                    if let Some(sequence) = committed {
                        context.required_sequence.fetch_max(sequence, Ordering::AcqRel);
                    }
                    let _ = send.send(Ok(committed));
                    let _durability = MemoryDiagnosticGuard::new(MemoryStage::ShardWriteDurability, 0);
                    let _ = wait_durable(&context.writer, context.required_sequence.load(Ordering::Acquire)).await;
                }
                Err(error) => {
                    let _ = send.send(Err(error.into()));
                }
            }
        });
        receive.await.map_err(|error| GraphError::CorruptValue {
            key: "write_pipeline/local_commit".into(),
            reason: format!("local commit task did not complete: {error}"),
        })?
    }
}

/// Admission context retained by a provider mutation session. Unlike
/// `run_write_pipeline`, the transaction spans several async session calls, so
/// its local durability context is owned explicitly rather than task-locally.
#[cfg(feature = "datafusion-cypher-engine")]
pub async fn begin_datafusion_write_pipeline(shard: &SlateGraphStore) -> Result<Option<Arc<WritePipeline>>> {
    if !shard.mutation_state().await_durable_writes {
        return Ok(None);
    }
    let admission = Arc::clone(&shard.mutation_state().write_pipeline_gate)
        .acquire_owned()
        .await
        .map_err(|error| GraphError::CorruptValue {
            key: "datafusion_cypher/write_pipeline".into(),
            reason: error.to_string(),
        })?;
    let writer = shard.writer()?;
    let snapshot = writer.snapshot()?;
    let writer_epoch = writer.status().current_manifest.writer_epoch();
    Ok(Some(Arc::new(WritePipeline {
        writer,
        writer_epoch,
        path: shard.store_path().to_string(),
        required_sequence: AtomicU64::new(snapshot.seq()),
        _admission: admission,
    })))
}

#[cfg(feature = "datafusion-cypher-engine")]
pub async fn commit_datafusion_write_pipeline(
    shard: &SlateGraphStore,
    pipeline: Arc<WritePipeline>,
    txn: DbTransaction,
    sequence: u64,
) -> Result<Option<StorageSequence>> {
    pipeline.validate_writer(shard)?;
    let committed = pipeline.commit_local(txn, sequence).await?;
    if let Some(sequence) = committed {
        wait_durable(&pipeline.writer, sequence)
            .await
            .map_err(|error| GraphError::DurabilityUnknown {
                sequence,
                message: error.to_string(),
            })?;
    }
    Ok(committed)
}

impl SlateGraphStore {
    pub async fn await_prior_write_durability(&self) -> Result<()> {
        if !self.mutation_state().await_durable_writes {
            return Ok(());
        }
        if let Some(context) = current().filter(|context| context.path == self.store_path().as_ref()) {
            return context.validate_writer(self);
        }
        // Legacy/maintenance transactions still read Remote. Drain predecessors
        // under their local guard so they never base a new sequence on old data.
        let writer = self.writer()?;
        let mut draining = None;
        let result = async {
            let sequence = writer.snapshot()?.seq();
            // Record only a real drain; an already durable writer waits for nothing.
            if writer.status().durable_seq < sequence {
                draining = Some(MemoryDiagnosticGuard::new(MemoryStage::ShardWritePriorDurability, 0));
            }
            wait_durable(&writer, sequence).await
        }
        .await;
        if result.is_err() {
            self.refresh_writer_fence().await?;
        }
        drop(draining);
        result
    }
}

impl SlateGraphStore {
    pub async fn run_write_pipeline_with<T>(&self, operation: impl Future<Output = Result<T>>, fence_noops: bool) -> Result<T> {
        if !self.mutation_state().await_durable_writes {
            return operation.await;
        }
        if let Some(context) = current().filter(|context| context.path == self.store_path().as_ref()) {
            context.validate_writer(self)?;
            let result = operation.await;
            // A sibling in this context may commit while this call runs, so an
            // advanced sequence does not prove this call committed. Fence every
            // success; no caller nests a fencing family today.
            if fence_noops && result.is_ok() {
                self.refresh_writer_fence().await?;
            }
            return result;
        }
        let waiting = MemoryDiagnosticGuard::new(MemoryStage::ShardWritePipelineWait, request_estimated_bytes());
        if self.mutation_state().write_pipeline_gate.available_permits() == 0 {
            self.mutation_state()
                .operation_metrics
                .backpressure_waits
                .fetch_add(1, Ordering::Relaxed);
        }
        let acquire = Arc::clone(&self.mutation_state().write_pipeline_gate)
            .acquire_owned()
            .instrument(tracing::info_span!("shard.write_pipeline_admission"));
        let admission = match crate::QueryCancellationToken::current() {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return Err(GraphError::QueryTimeout { operation: "query_cancelled", elapsed_ms: 0, limit_ms: 0 }),
                permit = acquire => permit,
            },
            None => acquire.await,
        }
        .map_err(|error| GraphError::CorruptValue {
            key: "write_pipeline/admission".into(),
            reason: error.to_string(),
        })?;
        drop(waiting);
        let writer = self.writer()?;
        let sequence = match writer.snapshot() {
            Ok(snapshot) => snapshot.seq(),
            Err(error) => {
                // Preserve fenced-handle cleanup and re-open backoff. Healthy
                // writers never need a manifest GET at this entry point.
                self.refresh_writer_fence().await?;
                return Err(error.into());
            }
        };
        let writer_epoch = writer.status().current_manifest.writer_epoch();
        let context = Arc::new(WritePipeline {
            writer,
            writer_epoch,
            path: self.store_path().to_string(),
            required_sequence: AtomicU64::new(sequence),
            _admission: admission,
        });
        let result = PIPELINE.scope(Arc::clone(&context), operation).await;
        // A no-op wrote no WAL, so no create-only upload fences it: check the
        // manifest now, after the permit is released, before acknowledging.
        if fence_noops && result.is_ok() && context.required_sequence.load(Ordering::Acquire) == sequence {
            self.refresh_writer_fence().await?;
        }
        // All local guards and the graph admission permit have dropped here.
        // No-op and replay responses also wait for the state they observed.
        let required = context
            .required_sequence
            .load(Ordering::Acquire)
            .max(context.writer.snapshot()?.seq());
        let durable = wait_durable(&context.writer, required)
            .instrument(tracing::info_span!("storage.durability_wait", hydradb.storage_sequence = required));
        match crate::QueryCancellationToken::current() {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return Err(GraphError::QueryTimeout { operation: "query_cancelled", elapsed_ms: 0, limit_ms: 0 }),
                result = durable => result?,
            },
            None => durable.await?,
        }
        result
    }
}
