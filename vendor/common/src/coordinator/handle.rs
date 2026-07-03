use super::metrics;
use super::{BroadcastedView, WriteCommand};
use super::{Delta, Durability, WriteError, WriteResult};
use crate::StorageRead;
use crate::coordinator::traits::EpochStamped;
use crate::storage::StorageSnapshot;
use futures::FutureExt;
use futures::future::Shared;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

/// A point-in-time view of all applied writes, broadcast by the coordinator
/// on freeze and flush events.
///
/// Each field corresponds to a stage in the write pipeline, ordered by
/// increasing durability:
///
/// - `current` — the active delta, still accepting writes (Applied).
/// - `frozen` — deltas that have been sealed but not yet flushed to storage
///   (Applied, ordered newest-first).
/// - `snapshot` — the storage snapshot, updated after each flush. Contains
///   all data up through the most recently written delta.
/// - `last_written_delta` — the most recently written delta (Written). Data
///   has been written to storage but not necessarily synced to disk. A
///   separate `FlushStorage` step advances the durable watermark.
pub struct View<D: Delta> {
    pub current: D::DeltaView,
    pub frozen: Vec<EpochStamped<D::FrozenView>>,
    pub snapshot: Arc<dyn StorageSnapshot>,
    pub last_written_delta: Option<EpochStamped<D::FrozenView>>,
}

impl<D: Delta> Clone for View<D> {
    fn clone(&self) -> Self {
        Self {
            current: self.current.clone(),
            frozen: self.frozen.clone(),
            snapshot: self.snapshot.clone(),
            last_written_delta: self.last_written_delta.clone(),
        }
    }
}

/// Receivers for durability watermark updates.
///
/// Each receiver tracks the highest epoch that has reached the corresponding
/// [`Durability`] level. See [`Durability`] for details on each level.
#[derive(Clone)]
pub struct EpochWatcher {
    pub applied_rx: watch::Receiver<u64>,
    pub written_rx: watch::Receiver<u64>,
    pub durable_rx: watch::Receiver<u64>,
}

impl EpochWatcher {
    /// Waits until the given epoch has reached the specified durability level.
    ///
    /// Returns `Err` if the corresponding [`EpochWatermarks`](super::EpochWatermarks)
    /// was dropped (i.e. the writer shut down).
    pub async fn wait(
        &mut self,
        epoch: u64,
        durability: Durability,
    ) -> Result<(), watch::error::RecvError> {
        let rx = match durability {
            Durability::Applied => &mut self.applied_rx,
            Durability::Written => &mut self.written_rx,
            Durability::Durable => &mut self.durable_rx,
        };
        rx.wait_for(|curr| *curr >= epoch).await.map(|_| ())
    }
}

/// Successful write application with its assigned epoch.
#[derive(Clone, Debug)]
pub(crate) struct WriteApplied<M> {
    pub epoch: u64,
    pub result: M,
}

/// Failed write application with its assigned epoch.
#[derive(Clone, Debug)]
pub(crate) struct WriteFailed {
    pub epoch: u64,
    pub error: String,
}

/// Result payload sent through the oneshot channel for a write or flush.
pub(crate) type EpochResult<M> = Result<WriteApplied<M>, WriteFailed>;

/// Handle returned from a write or flush operation.
///
/// Provides the epoch assigned to the operation, the apply result (for writes),
/// and allows waiting for the operation to reach a desired durability level.
pub struct WriteHandle<M: Clone + Send + 'static = ()> {
    inner: Shared<oneshot::Receiver<EpochResult<M>>>,
    watchers: EpochWatcher,
}

impl<M: Clone + Send + 'static> WriteHandle<M> {
    pub(crate) fn new(rx: oneshot::Receiver<EpochResult<M>>, watchers: EpochWatcher) -> Self {
        Self {
            inner: rx.shared(),
            watchers,
        }
    }

    async fn recv(&self) -> WriteResult<WriteApplied<M>> {
        self.inner
            .clone()
            .await
            .map_err(|_| WriteError::Shutdown)?
            .map_err(|e| WriteError::ApplyError(e.epoch, e.error))
    }

    /// Returns the epoch assigned to this write.
    ///
    /// Epochs are assigned when the coordinator dequeues the write, so this
    /// method blocks until sequencing completes. Epochs are monotonically
    /// increasing and reflect the actual write order.
    pub async fn epoch(&self) -> WriteResult<u64> {
        Ok(self.recv().await?.epoch)
    }

    /// Wait for the write to reach the specified durability level.
    ///
    /// Returns the apply result produced by [`Delta::apply`] once the
    /// requested durability level has been reached.
    pub async fn wait(&mut self, durability: Durability) -> WriteResult<M> {
        let WriteApplied { epoch, result } = self.recv().await?;

        self.watchers
            .wait(epoch, durability)
            .await
            .map_err(|_| WriteError::Shutdown)?;
        Ok(result)
    }
}

/// Handle for submitting writes to the coordinator.
///
/// This is the main interface for interacting with the write coordinator.
/// It can be cloned and shared across tasks.
pub struct WriteCoordinatorHandle<D: Delta> {
    name: Arc<str>,
    write_tx: mpsc::Sender<WriteCommand<D>>,
    watchers: EpochWatcher,
    view: Arc<BroadcastedView<D>>,
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    pub(crate) fn new(
        name: String,
        write_tx: mpsc::Sender<WriteCommand<D>>,
        watchers: EpochWatcher,
        view: Arc<BroadcastedView<D>>,
    ) -> Self {
        Self {
            name: Arc::from(name),
            write_tx,
            watchers,
            view,
        }
    }

    /// Returns the highest epoch that has been flushed to storage.
    ///
    /// This is a non-blocking snapshot of the current flushed watermark.
    /// Returns 0 if no data has been flushed yet.
    pub fn flushed_epoch(&self) -> u64 {
        *self.watchers.written_rx.borrow()
    }

    /// Sample queue depth on each send. Cheap atomic reads on `mpsc::Sender`.
    fn record_queue_depth(&self) {
        let max = self.write_tx.max_capacity();
        let free = self.write_tx.capacity();
        ::metrics::gauge!(
            metrics::COORDINATOR_QUEUE_DEPTH,
            "channel" => self.name.to_string(),
        )
        .set(max.saturating_sub(free) as f64);
    }

    fn record_send(&self, command: &'static str, status: &'static str, started: Instant) {
        ::metrics::histogram!(
            metrics::COORDINATOR_SEND_DURATION_SECONDS,
            "command" => command,
            "status" => status,
        )
        .record(started.elapsed().as_secs_f64());
    }

    fn record_backpressure(&self, command: &'static str, reason: &'static str) {
        ::metrics::counter!(
            metrics::COORDINATOR_QUEUE_BACKPRESSURE_TOTAL,
            "command" => command,
            "reason" => reason,
        )
        .increment(1);
    }
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    /// Submit a write to the coordinator with a timeout.
    ///
    /// Unlike [`write`](Self::try_write), which fails immediately when the queue
    /// is full, this method waits up to `timeout` for space.
    ///
    /// # Errors
    ///
    /// - [`WriteError::TimeoutError`] — the queue remained full for the
    ///   entire duration. Contains the original write so it can be retried
    ///   without cloning.
    /// - [`WriteError::Shutdown`] — the coordinator has stopped.
    pub async fn write_timeout(
        &self,
        write: D::Write,
        timeout: Duration,
    ) -> Result<WriteHandle<D::ApplyResult>, WriteError<D::Write>> {
        const COMMAND: &str = "write_timeout";
        self.record_queue_depth();
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        let send_result = self
            .write_tx
            .send_timeout(
                WriteCommand::Write {
                    write,
                    result_tx: tx,
                },
                timeout,
            )
            .await;
        match send_result {
            Ok(()) => {
                self.record_send(COMMAND, "ok", started);
                Ok(WriteHandle::new(rx, self.watchers.clone()))
            }
            Err(mpsc::error::SendTimeoutError::Timeout(WriteCommand::Write { write, .. })) => {
                self.record_send(COMMAND, "timeout", started);
                self.record_backpressure(COMMAND, "timeout");
                Err(WriteError::TimeoutError(write))
            }
            Err(mpsc::error::SendTimeoutError::Closed(WriteCommand::Write { .. })) => {
                self.record_send(COMMAND, "shutdown", started);
                self.record_backpressure(COMMAND, "closed");
                Err(WriteError::Shutdown)
            }
            Err(_) => unreachable!("sent a Write command"),
        }
    }

    /// Submit a write to the coordinator, blocking indefinitely until there is
    /// room in the channel.
    ///
    /// # Errors
    ///
    /// - [`WriteError::TimeoutError`] — the queue remained full for the
    ///   entire duration. Contains the original write so it can be retried
    ///   without cloning.
    /// - [`WriteError::Shutdown`] — the coordinator has stopped.
    pub async fn write(
        &self,
        write: D::Write,
    ) -> Result<WriteHandle<D::ApplyResult>, WriteError<D::Write>> {
        const COMMAND: &str = "write";
        self.record_queue_depth();
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        let send_result = self
            .write_tx
            .send(WriteCommand::Write {
                write,
                result_tx: tx,
            })
            .await;
        match send_result {
            Ok(()) => {
                self.record_send(COMMAND, "ok", started);
                Ok(WriteHandle::new(rx, self.watchers.clone()))
            }
            Err(mpsc::error::SendError(WriteCommand::Write { .. })) => {
                self.record_send(COMMAND, "shutdown", started);
                self.record_backpressure(COMMAND, "closed");
                Err(WriteError::Shutdown)
            }
            Err(_) => unreachable!("sent a Write command"),
        }
    }

    /// Submit a write to the coordinator.
    ///
    /// Returns a handle that can be used to retrieve the apply result
    /// and wait for the write to reach a desired durability level. On
    /// failure the original write is returned inside the error so it
    /// can be retried without cloning.
    pub async fn try_write(
        &self,
        write: D::Write,
    ) -> Result<WriteHandle<D::ApplyResult>, WriteError<D::Write>> {
        const COMMAND: &str = "try_write";
        self.record_queue_depth();
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        let send_result = self.write_tx.try_send(WriteCommand::Write {
            write,
            result_tx: tx,
        });
        match send_result {
            Ok(()) => {
                self.record_send(COMMAND, "ok", started);
                Ok(WriteHandle::new(rx, self.watchers.clone()))
            }
            Err(mpsc::error::TrySendError::Full(WriteCommand::Write { write, .. })) => {
                self.record_send(COMMAND, "backpressure", started);
                self.record_backpressure(COMMAND, "full");
                Err(WriteError::Backpressure(write))
            }
            Err(mpsc::error::TrySendError::Closed(WriteCommand::Write { .. })) => {
                self.record_send(COMMAND, "shutdown", started);
                self.record_backpressure(COMMAND, "closed");
                Err(WriteError::Shutdown)
            }
            Err(_) => unreachable!("sent a Write command"),
        }
    }

    /// Request a flush of the current delta.
    ///
    /// This will trigger a flush even if the flush threshold has not been reached.
    /// When `flush_storage` is true, the flush will also call `storage.flush()`
    /// to guarantee durability, and the durable watermark will be advanced.
    /// Returns a handle that can be used to wait for the flush to complete.
    pub async fn flush(&self, flush_storage: bool) -> WriteResult<WriteHandle> {
        const COMMAND: &str = "flush";
        self.record_queue_depth();
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        let send_result = self.write_tx.try_send(WriteCommand::Flush {
            epoch_tx: tx,
            flush_storage,
        });
        match send_result {
            Ok(()) => {
                self.record_send(COMMAND, "ok", started);
                Ok(WriteHandle::new(rx, self.watchers.clone()))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.record_send(COMMAND, "backpressure", started);
                self.record_backpressure(COMMAND, "full");
                Err(WriteError::Backpressure(()))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.record_send(COMMAND, "shutdown", started);
                self.record_backpressure(COMMAND, "closed");
                Err(WriteError::Shutdown)
            }
        }
    }

    pub fn view(&self) -> Arc<View<D>> {
        self.view.current()
    }

    pub fn subscribe(&self) -> (broadcast::Receiver<Arc<View<D>>>, Arc<View<D>>) {
        self.view.subscribe()
    }
}

impl<D: Delta> Clone for WriteCoordinatorHandle<D> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            write_tx: self.write_tx.clone(),
            watchers: self.watchers.clone(),
            view: self.view.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;

    fn create_watchers(
        applied: watch::Receiver<u64>,
        flushed: watch::Receiver<u64>,
        durable: watch::Receiver<u64>,
    ) -> EpochWatcher {
        EpochWatcher {
            applied_rx: applied,
            written_rx: flushed,
            durable_rx: durable,
        }
    }

    #[tokio::test]
    async fn should_return_epoch_when_assigned() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when
        tx.send(Ok(WriteApplied {
            epoch: 42,
            result: (),
        }))
        .unwrap();
        let result = handle.epoch().await;

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn should_allow_multiple_epoch_calls() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 42,
            result: (),
        }))
        .unwrap();

        // when
        let result1 = handle.epoch().await;
        let result2 = handle.epoch().await;
        let result3 = handle.epoch().await;

        // then
        assert_eq!(result1.unwrap(), 42);
        assert_eq!(result2.unwrap(), 42);
        assert_eq!(result3.unwrap(), 42);
    }

    #[tokio::test]
    async fn should_return_apply_result_from_wait() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<String> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when
        tx.send(Ok(WriteApplied {
            epoch: 1,
            result: "hello".to_string(),
        }))
        .unwrap();

        // then
        assert_eq!(handle.wait(Durability::Applied).await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn should_return_immediately_when_watermark_already_reached() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64); // watermark already at 100
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 50,
            result: (),
        }))
        .unwrap(); // epoch is 50, watermark is 100

        // when
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_wait_until_watermark_reaches_epoch() {
        // given
        let (tx, rx) = oneshot::channel();
        let (applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 10,
            result: (),
        }))
        .unwrap();

        // when - spawn a task to update the watermark after a delay
        let wait_task = tokio::spawn(async move { handle.wait(Durability::Applied).await });

        tokio::task::yield_now().await;
        applied_tx.send(5).unwrap(); // still below epoch
        tokio::task::yield_now().await;
        applied_tx.send(10).unwrap(); // reaches epoch

        let result = wait_task.await.unwrap();

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_wait_for_correct_durability_level() {
        // given - set up watchers with different values
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64);
        let (_flushed_tx, flushed_rx) = watch::channel(50u64);
        let (durable_tx, durable_rx) = watch::channel(10u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 25,
            result: (),
        }))
        .unwrap();

        // when - wait for Durable (watermark is 10, epoch is 25)
        let wait_task = tokio::spawn(async move { handle.wait(Durability::Durable).await });

        tokio::task::yield_now().await;
        durable_tx.send(25).unwrap(); // update durable watermark

        let result = wait_task.await.unwrap();

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_propagate_epoch_error_in_wait() {
        // given
        let (tx, rx) = oneshot::channel::<EpochResult<()>>();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle = WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when - drop the sender without sending
        drop(tx);
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(matches!(result, Err(WriteError::Shutdown)));
    }

    #[tokio::test]
    async fn should_propagate_apply_error_in_wait() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when - send an error
        tx.send(Err(WriteFailed {
            epoch: 1,
            error: "apply error".into(),
        }))
        .unwrap();
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(
            matches!(result, Err(WriteError::ApplyError(epoch, msg)) if epoch == 1 && msg == "apply error")
        );
    }

    #[tokio::test]
    async fn epoch_watcher_should_resolve_when_watermark_reached() {
        // given
        let (applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut watcher = create_watchers(applied_rx, flushed_rx, durable_rx);

        // when
        let wait_task = tokio::spawn(async move { watcher.wait(5, Durability::Applied).await });
        tokio::task::yield_now().await;
        applied_tx.send(5).unwrap();

        // then
        assert!(wait_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn epoch_watcher_should_resolve_immediately_when_already_reached() {
        // given
        let (_applied_tx, applied_rx) = watch::channel(10u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut watcher = create_watchers(applied_rx, flushed_rx, durable_rx);

        // when/then
        assert!(watcher.wait(5, Durability::Applied).await.is_ok());
    }

    #[tokio::test]
    async fn epoch_watcher_should_select_correct_durability_receiver() {
        // given
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (durable_tx, durable_rx) = watch::channel(0u64);
        let mut watcher = create_watchers(applied_rx, flushed_rx, durable_rx);

        // when
        let wait_task = tokio::spawn(async move { watcher.wait(3, Durability::Durable).await });
        tokio::task::yield_now().await;
        durable_tx.send(3).unwrap();

        // then
        assert!(wait_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn epoch_watcher_should_return_error_on_sender_drop() {
        // given
        let (applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut watcher = create_watchers(applied_rx, flushed_rx, durable_rx);

        // when
        drop(applied_tx);
        let result = watcher.wait(1, Durability::Applied).await;

        // then
        assert!(result.is_err());
    }
}
