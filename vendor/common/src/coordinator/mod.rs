#![allow(unused)]

mod error;
mod handle;
pub(crate) mod metrics;
pub mod subscriber;
mod traits;

use std::collections::HashMap;
use std::ops::Range;
use std::ops::{Deref, DerefMut};

pub use error::{WriteError, WriteResult};
use futures::stream::{self, SelectAll, StreamExt};
pub use handle::{View, WriteCoordinatorHandle, WriteHandle};
pub use metrics::describe_coordinator_metrics;
pub use subscriber::{SubscribeError, ViewMonitor, ViewSubscriber};
pub use traits::{Delta, Durability, EpochStamped, Flusher};

/// Event sent from the write coordinator task to the flush task.
enum FlushEvent<D: Delta> {
    /// Flush a frozen delta to storage.
    FlushDelta { frozen: EpochStamped<D::Frozen> },
    /// Ensure storage durability (e.g. call storage.flush()).
    FlushStorage,
}

// Internal use only
use crate::StorageRead;
use crate::storage::StorageSnapshot;
use async_trait::async_trait;
pub use handle::EpochWatcher;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant, Interval, interval_at};
use tokio_util::sync::CancellationToken;

/// Configuration for the write coordinator.
#[derive(Debug, Clone)]
pub struct WriteCoordinatorConfig {
    /// Maximum number of pending writes in the queue.
    pub queue_capacity: usize,
    /// Interval at which to trigger automatic flushes.
    pub flush_interval: Duration,
    /// Delta size threshold at which to trigger a flush.
    pub flush_size_threshold: usize,
}

impl Default for WriteCoordinatorConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            flush_interval: Duration::from_secs(10),
            flush_size_threshold: 64 * 1024 * 1024, // 64 MB
        }
    }
}

pub(crate) enum WriteCommand<D: Delta> {
    Write {
        write: D::Write,
        result_tx: oneshot::Sender<handle::EpochResult<D::ApplyResult>>,
    },
    Flush {
        epoch_tx: oneshot::Sender<handle::EpochResult<()>>,
        flush_storage: bool,
    },
}

/// The write coordinator manages write ordering, batching, and durability.
///
/// It accepts writes through `WriteCoordinatorHandle`, applies them to a `Delta`,
/// and coordinates flushing through a `Flusher`.
pub struct WriteCoordinator<D: Delta, F: Flusher<D>> {
    handles: HashMap<String, WriteCoordinatorHandle<D>>,
    pause_handles: HashMap<String, PauseHandle>,
    stop_tok: CancellationToken,
    tasks: Option<(WriteCoordinatorTask<D>, FlushTask<D, F>)>,
    write_task_jh: Option<tokio::task::JoinHandle<Result<(), String>>>,
    view: Arc<BroadcastedView<D>>,
}

impl<D: Delta, F: Flusher<D>> WriteCoordinator<D, F> {
    pub fn new(
        config: WriteCoordinatorConfig,
        channels: Vec<impl ToString>,
        initial_context: D::Context,
        initial_snapshot: Arc<dyn StorageSnapshot>,
        flusher: F,
    ) -> WriteCoordinator<D, F> {
        let (watermarks, watcher) = EpochWatermarks::new();
        let watermarks = Arc::new(watermarks);

        // Create a write channel per named input
        let mut write_rxs = Vec::with_capacity(channels.len());
        let mut write_txs = HashMap::new();
        let mut pause_handles = HashMap::new();
        for name in &channels {
            let (write_tx, write_rx, pause_hdl) = pausable_channel(config.queue_capacity);
            write_rxs.push(write_rx);
            write_txs.insert(name.to_string(), write_tx);
            pause_handles.insert(name.to_string(), pause_hdl);
        }

        // this is the channel that sends FlushEvents to be flushed
        // by a background task so that the process of converting deltas
        // to storage operations is non-blocking. for now, we apply no
        // backpressure on this channel, so writes will block if more than
        // one flush is pending
        let (flush_tx, flush_rx) = mpsc::channel(2);

        let flush_stop_tok = CancellationToken::new();
        let stop_tok = CancellationToken::new();
        let write_task = WriteCoordinatorTask::new(
            config,
            initial_context,
            initial_snapshot,
            write_rxs,
            flush_tx,
            watermarks.clone(),
            stop_tok.clone(),
            flush_stop_tok.clone(),
        );

        let view = write_task.view.clone();

        let mut handles = HashMap::new();
        for name in channels {
            let name = name.to_string();
            let write_tx = write_txs.remove(&name).expect("unreachable");
            handles.insert(
                name.clone(),
                WriteCoordinatorHandle::new(name, write_tx, watcher.clone(), view.clone()),
            );
        }

        let flush_task = FlushTask {
            flusher,
            stop_tok: flush_stop_tok,
            flush_rx,
            watermarks: watermarks.clone(),
            view: view.clone(),
            last_flushed_epoch: 0,
        };

        Self {
            handles,
            pause_handles,
            tasks: Some((write_task, flush_task)),
            write_task_jh: None,
            stop_tok,
            view,
        }
    }

    pub fn handle(&self, name: &str) -> WriteCoordinatorHandle<D> {
        self.handles
            .get(name)
            .expect("unknown channel name")
            .clone()
    }

    pub fn pause_handle(&self, name: &str) -> PauseHandle {
        self.pause_handles
            .get(name)
            .expect("unknown channel name")
            .clone()
    }

    pub fn start(&mut self) {
        let Some((write_task, flush_task)) = self.tasks.take() else {
            // already started
            return;
        };
        let flush_task_jh = flush_task.run();
        let write_task_jh = write_task.run(flush_task_jh);
        self.write_task_jh = Some(write_task_jh);
    }

    pub async fn stop(mut self) -> Result<(), String> {
        let Some(write_task_jh) = self.write_task_jh.take() else {
            return Ok(());
        };
        self.stop_tok.cancel();
        write_task_jh.await.map_err(|e| e.to_string())?
    }

    pub fn view(&self) -> Arc<View<D>> {
        self.view.current()
    }

    pub fn subscribe(&self) -> (ViewSubscriber<D>, ViewMonitor) {
        let (view_rx, initial_view) = self.view.subscribe();
        ViewSubscriber::new(view_rx, initial_view)
    }
}

struct WriteCoordinatorTask<D: Delta> {
    config: WriteCoordinatorConfig,
    delta: CurrentDelta<D>,
    flush_tx: mpsc::Sender<FlushEvent<D>>,
    write_rxs: Vec<PausableReceiver<D>>,
    watermarks: Arc<EpochWatermarks>,
    view: Arc<BroadcastedView<D>>,
    epoch: u64,
    delta_start_epoch: u64,
    flush_interval: Interval,
    stop_tok: CancellationToken,
    flush_stop_tok: CancellationToken,
}

impl<D: Delta> WriteCoordinatorTask<D> {
    /// Create a new write coordinator with the given flusher.
    ///
    /// This is useful for testing with mock flushers.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: WriteCoordinatorConfig,
        initial_context: D::Context,
        initial_snapshot: Arc<dyn StorageSnapshot>,
        write_rxs: Vec<PausableReceiver<D>>,
        flush_tx: mpsc::Sender<FlushEvent<D>>,
        watermarks: Arc<EpochWatermarks>,
        stop_tok: CancellationToken,
        flush_stop_tok: CancellationToken,
    ) -> Self {
        let delta = D::init(initial_context);

        let initial_view = View {
            current: delta.reader(),
            frozen: vec![],
            snapshot: initial_snapshot,
            last_written_delta: None,
        };
        let initial_view = Arc::new(BroadcastedView::new(initial_view));

        let flush_interval = interval_at(
            Instant::now() + config.flush_interval,
            config.flush_interval,
        );
        Self {
            config,
            delta: CurrentDelta::new(delta),
            write_rxs,
            flush_tx,
            watermarks,
            view: initial_view,
            // Epochs start at 1 because watch channels initialize to 0 (meaning "nothing
            // processed yet"). If the first write had epoch 0, wait() would return
            // immediately since the condition `watermark < epoch` would be `0 < 0` = false.
            epoch: 1,
            delta_start_epoch: 1,
            flush_interval,
            stop_tok,
            flush_stop_tok,
        }
    }

    /// Run the coordinator event loop.
    pub fn run(
        mut self,
        flush_task_jh: tokio::task::JoinHandle<WriteResult<()>>,
    ) -> tokio::task::JoinHandle<Result<(), String>> {
        tokio::task::spawn(async move { self.run_coordinator(flush_task_jh).await })
    }

    async fn run_coordinator(
        mut self,
        flush_task_jh: tokio::task::JoinHandle<WriteResult<()>>,
    ) -> Result<(), String> {
        // Reset the interval to start fresh from when run() is called
        self.flush_interval.reset();

        // Merge all write receivers into a single stream
        let mut write_stream: SelectAll<_> = SelectAll::new();
        for rx in self.write_rxs.drain(..) {
            write_stream.push(
                stream::unfold(
                    rx,
                    |mut rx| async move { rx.recv().await.map(|cmd| (cmd, rx)) },
                )
                .boxed(),
            );
        }

        loop {
            tokio::select! {
                cmd = write_stream.next() => {
                    match cmd {
                        Some(WriteCommand::Write { write, result_tx }) => {
                            self.handle_write(write, result_tx).await?;
                        }
                        Some(WriteCommand::Flush { epoch_tx, flush_storage }) => {
                            // Send back the epoch of the last processed write
                            let _ = epoch_tx.send(Ok(handle::WriteApplied {
                                epoch: self.epoch.saturating_sub(1),
                                result: (),
                            }));
                            self.handle_flush(FlushReason::Explicit, flush_storage).await;
                        }
                        None => {
                            // All write channels closed
                            break;
                        }
                    }
                }

                _ = self.flush_interval.tick() => {
                    self.handle_flush(FlushReason::Interval, false).await;
                }

                _ = self.stop_tok.cancelled() => {
                    break;
                }
            }
        }

        // Flush any remaining pending writes before shutdown
        self.handle_flush(FlushReason::Shutdown, false).await;

        // Signal the flush task to stop
        self.flush_stop_tok.cancel();
        // Wait for the flush task to complete and propagate any errors
        flush_task_jh
            .await
            .map_err(|e| format!("flush task panicked: {}", e))?
            .map_err(|e| format!("flush task error: {}", e))
    }

    async fn handle_write(
        &mut self,
        op: D::Write,
        result_tx: oneshot::Sender<handle::EpochResult<D::ApplyResult>>,
    ) -> Result<(), String> {
        let write_epoch = self.epoch;
        self.epoch += 1;

        let apply_start = std::time::Instant::now();
        let result = self.delta.apply(op);
        ::metrics::histogram!(metrics::COORDINATOR_DELTA_APPLY_DURATION_SECONDS)
            .record(apply_start.elapsed().as_secs_f64());

        // Ignore error if receiver was dropped (fire-and-forget write)
        let _ = result_tx.send(
            result
                .map(|apply_result| handle::WriteApplied {
                    epoch: write_epoch,
                    result: apply_result,
                })
                .map_err(|e| handle::WriteFailed {
                    epoch: write_epoch,
                    error: e,
                }),
        );

        // Ignore error if no watchers are listening - this is non-fatal
        self.watermarks.update_applied(write_epoch);

        let estimated = self.delta.estimate_size();
        ::metrics::gauge!(metrics::COORDINATOR_DELTA_ESTIMATED_BYTES).set(estimated as f64);
        if estimated >= self.config.flush_size_threshold {
            self.handle_flush(FlushReason::SizeThreshold, false).await;
        }

        Ok(())
    }

    async fn handle_flush(&mut self, reason: FlushReason, flush_storage: bool) {
        self.flush_if_delta_has_writes(reason).await;
        if flush_storage {
            self.send_flush_event(FlushEvent::FlushStorage).await;
        }
    }

    async fn flush_if_delta_has_writes(&mut self, reason: FlushReason) {
        if self.epoch == self.delta_start_epoch {
            return;
        }

        self.flush_interval.reset();

        ::metrics::counter!(metrics::COORDINATOR_FLUSH_TOTAL, "reason" => reason.as_str())
            .increment(1);

        let epoch_range = self.delta_start_epoch..self.epoch;
        self.delta_start_epoch = self.epoch;

        let freeze_start = std::time::Instant::now();
        let (frozen, frozen_reader) = self.delta.freeze_and_init();
        ::metrics::histogram!(metrics::COORDINATOR_DELTA_FREEZE_DURATION_SECONDS)
            .record(freeze_start.elapsed().as_secs_f64());
        // Reset estimated bytes gauge: the new delta starts empty.
        ::metrics::gauge!(metrics::COORDINATOR_DELTA_ESTIMATED_BYTES).set(0.0);

        let stamped_frozen = EpochStamped::new(frozen, epoch_range.clone());
        let stamped_frozen_reader = EpochStamped::new(frozen_reader, epoch_range.clone());
        let reader = self.delta.reader();
        // update the view before sending the flush msg to ensure the flusher sees
        // the frozen reader when updating the view post-flush
        self.view.update_delta_frozen(stamped_frozen_reader, reader);
        // this is the blocking section of the flush, new writes will not be accepted
        // until the event is sent to the FlushTask
        self.send_flush_event(FlushEvent::FlushDelta {
            frozen: stamped_frozen,
        })
        .await;
    }

    async fn send_flush_event(&self, event: FlushEvent<D>) {
        // Sample queue depth (in-flight events) just before sending. `max_capacity`
        // and `capacity` are cheap atomic reads.
        let max = self.flush_tx.max_capacity();
        let free = self.flush_tx.capacity();
        ::metrics::gauge!(metrics::COORDINATOR_FLUSH_EVENT_QUEUE_DEPTH)
            .set(max.saturating_sub(free) as f64);

        let send_start = std::time::Instant::now();
        let _ = self.flush_tx.send(event).await;
        ::metrics::histogram!(metrics::COORDINATOR_FLUSH_EVENT_SEND_DURATION_SECONDS)
            .record(send_start.elapsed().as_secs_f64());
    }
}

/// Reason a flush was triggered. Used as a low-cardinality `reason` label.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FlushReason {
    SizeThreshold,
    Interval,
    Explicit,
    Shutdown,
}

impl FlushReason {
    fn as_str(self) -> &'static str {
        match self {
            FlushReason::SizeThreshold => "size_threshold",
            FlushReason::Interval => "interval",
            FlushReason::Explicit => "explicit",
            FlushReason::Shutdown => "shutdown",
        }
    }
}

struct FlushTask<D: Delta, F: Flusher<D>> {
    flusher: F,
    stop_tok: CancellationToken,
    flush_rx: mpsc::Receiver<FlushEvent<D>>,
    watermarks: Arc<EpochWatermarks>,
    view: Arc<BroadcastedView<D>>,
    last_flushed_epoch: u64,
}

impl<D: Delta, F: Flusher<D>> FlushTask<D, F> {
    fn run(mut self) -> tokio::task::JoinHandle<WriteResult<()>> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = self.flush_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        self.handle_event(event).await?;
                    }
                    _ = self.stop_tok.cancelled() => {
                        break;
                    }
                }
            }
            // drain all remaining flush events
            while let Ok(event) = self.flush_rx.try_recv() {
                self.handle_event(event).await;
            }
            Ok(())
        })
    }

    async fn handle_event(&mut self, event: FlushEvent<D>) -> WriteResult<()> {
        match event {
            FlushEvent::FlushDelta { frozen } => self.handle_flush(frozen).await,
            FlushEvent::FlushStorage => {
                let start = std::time::Instant::now();
                let result = self
                    .flusher
                    .flush_storage()
                    .await
                    .map_err(|e| WriteError::FlushError(e.to_string()));
                ::metrics::histogram!(metrics::COORDINATOR_FLUSH_STORAGE_DURATION_SECONDS)
                    .record(start.elapsed().as_secs_f64());
                result?;
                self.watermarks.update_durable(self.last_flushed_epoch);
                Ok(())
            }
        }
    }

    async fn handle_flush(&mut self, frozen: EpochStamped<D::Frozen>) -> WriteResult<()> {
        let delta = frozen.val;
        let epoch_range = frozen.epoch_range;
        let start = std::time::Instant::now();
        let result = self
            .flusher
            .flush_delta(delta, &epoch_range)
            .await
            .map_err(|e| WriteError::FlushError(e.to_string()));
        ::metrics::histogram!(metrics::COORDINATOR_FLUSH_DELTA_DURATION_SECONDS)
            .record(start.elapsed().as_secs_f64());
        let snapshot = result?;
        self.last_flushed_epoch = epoch_range.end - 1;
        // Publish the refreshed view first so any waiter awoken by the
        // watermark notification below observes a view that already includes
        // this flush. Reversing the order races on multi-thread runtimes:
        // a waiter on `Written` can schedule on another worker and read
        // `view.snapshot` before this task completes the view update.
        self.view.update_flush_finished(snapshot, epoch_range);
        self.watermarks.update_written(self.last_flushed_epoch);
        Ok(())
    }
}

struct CurrentDelta<D: Delta> {
    delta: Option<D>,
}

impl<D: Delta> Deref for CurrentDelta<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        match &self.delta {
            Some(d) => d,
            None => panic!("current delta not initialized"),
        }
    }
}

impl<D: Delta> DerefMut for CurrentDelta<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.delta {
            Some(d) => d,
            None => panic!("current delta not initialized"),
        }
    }
}

impl<D: Delta> CurrentDelta<D> {
    fn new(delta: D) -> Self {
        Self { delta: Some(delta) }
    }

    fn freeze_and_init(&mut self) -> (D::Frozen, D::FrozenView) {
        let Some(delta) = self.delta.take() else {
            panic!("delta not initialized");
        };
        let (frozen, frozen_reader, context) = delta.freeze();
        let new_delta = D::init(context);
        self.delta = Some(new_delta);
        (frozen, frozen_reader)
    }
}

pub struct EpochWatermarks {
    applied_tx: tokio::sync::watch::Sender<u64>,
    written_tx: tokio::sync::watch::Sender<u64>,
    durable_tx: tokio::sync::watch::Sender<u64>,
}

impl EpochWatermarks {
    pub fn new() -> (Self, EpochWatcher) {
        let (applied_tx, applied_rx) = tokio::sync::watch::channel(0);
        let (written_tx, written_rx) = tokio::sync::watch::channel(0);
        let (durable_tx, durable_rx) = tokio::sync::watch::channel(0);
        let watcher = EpochWatcher {
            applied_rx,
            written_rx,
            durable_rx,
        };
        let watermarks = EpochWatermarks {
            applied_tx,
            written_tx,
            durable_tx,
        };
        (watermarks, watcher)
    }

    pub fn update_applied(&self, epoch: u64) {
        let _ = self.applied_tx.send(epoch);
    }

    pub fn update_written(&self, epoch: u64) {
        let _ = self.written_tx.send(epoch);
    }

    pub fn update_durable(&self, epoch: u64) {
        let _ = self.durable_tx.send(epoch);
    }
}

pub(crate) struct BroadcastedView<D: Delta> {
    inner: Mutex<BroadcastedViewInner<D>>,
}

impl<D: Delta> BroadcastedView<D> {
    fn new(initial_view: View<D>) -> Self {
        let (view_tx, _) = broadcast::channel(16);
        Self {
            inner: Mutex::new(BroadcastedViewInner {
                view: Arc::new(initial_view),
                view_tx,
            }),
        }
    }

    fn update_flush_finished(&self, snapshot: Arc<dyn StorageSnapshot>, epoch_range: Range<u64>) {
        self.inner
            .lock()
            .expect("lock poisoned")
            .update_flush_finished(snapshot, epoch_range);
    }

    fn update_delta_frozen(&self, frozen: EpochStamped<D::FrozenView>, reader: D::DeltaView) {
        self.inner
            .lock()
            .expect("lock poisoned")
            .update_delta_frozen(frozen, reader);
    }

    fn current(&self) -> Arc<View<D>> {
        self.inner.lock().expect("lock poisoned").current()
    }

    fn subscribe(&self) -> (broadcast::Receiver<Arc<View<D>>>, Arc<View<D>>) {
        self.inner.lock().expect("lock poisoned").subscribe()
    }
}

struct BroadcastedViewInner<D: Delta> {
    view: Arc<View<D>>,
    view_tx: tokio::sync::broadcast::Sender<Arc<View<D>>>,
}

impl<D: Delta> BroadcastedViewInner<D> {
    fn update_flush_finished(
        &mut self,
        snapshot: Arc<dyn StorageSnapshot>,
        epoch_range: Range<u64>,
    ) {
        let mut new_frozen = self.view.frozen.clone();
        let last = new_frozen
            .pop()
            .expect("frozen should not be empty when flush completes");
        assert_eq!(last.epoch_range, epoch_range);
        self.view = Arc::new(View {
            current: self.view.current.clone(),
            frozen: new_frozen,
            snapshot,
            last_written_delta: Some(last),
        });
        self.view_tx.send(self.view.clone());
    }

    fn update_delta_frozen(&mut self, frozen: EpochStamped<D::FrozenView>, reader: D::DeltaView) {
        // Update read state: add frozen delta to front, update current reader
        let mut new_frozen = vec![frozen];
        new_frozen.extend(self.view.frozen.iter().cloned());
        self.view = Arc::new(View {
            current: reader,
            frozen: new_frozen,
            snapshot: self.view.snapshot.clone(),
            last_written_delta: self.view.last_written_delta.clone(),
        });
        self.view_tx.send(self.view.clone());
    }

    fn current(&self) -> Arc<View<D>> {
        self.view.clone()
    }

    fn subscribe(&self) -> (broadcast::Receiver<Arc<View<D>>>, Arc<View<D>>) {
        (self.view_tx.subscribe(), self.view.clone())
    }
}

struct PausableReceiver<D: Delta> {
    pause_rx: Option<watch::Receiver<bool>>,
    rx: mpsc::Receiver<WriteCommand<D>>,
}

impl<D: Delta> PausableReceiver<D> {
    async fn recv(&mut self) -> Option<WriteCommand<D>> {
        if let Some(pause_rx) = self.pause_rx.as_mut() {
            pause_rx.wait_for(|v| !*v).await;
        }
        self.rx.recv().await
    }
}

/// A handle that can be used to pause/unpause the coordinator's consumption from a write queue
#[derive(Clone)]
pub struct PauseHandle {
    pause_tx: tokio::sync::watch::Sender<bool>,
}

impl PauseHandle {
    pub fn pause(&self) {
        self.pause_tx.send_replace(true);
    }

    pub fn unpause(&self) {
        self.pause_tx.send_replace(false);
    }
}

fn pausable_channel<D: Delta>(
    capacity: usize,
) -> (
    mpsc::Sender<WriteCommand<D>>,
    PausableReceiver<D>,
    PauseHandle,
) {
    let (pause_tx, pause_rx) = watch::channel(false);
    let (tx, rx) = mpsc::channel(capacity);
    (
        tx,
        PausableReceiver {
            pause_rx: Some(pause_rx),
            rx,
        },
        PauseHandle { pause_tx },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BytesRange;
    use crate::coordinator::Durability;
    use crate::storage::in_memory::{InMemoryStorage, InMemoryStorageSnapshot};
    use crate::storage::{PutRecordOp, Record, StorageSnapshot};
    use crate::{Storage, StorageRead};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::{HashMap, HashSet};
    use std::ops::Range;
    use std::sync::Mutex;
    // ============================================================================
    // Test Infrastructure
    // ============================================================================

    #[derive(Clone, Debug)]
    struct TestWrite {
        key: String,
        value: u64,
        size: usize,
    }

    /// Context carries state that must persist across deltas (like sequence allocation)
    #[derive(Clone, Debug, Default)]
    struct TestContext {
        next_seq: u64,
        error: Option<String>,
    }

    /// A shared reader that sees writes as they are applied to the delta.
    #[derive(Clone, Debug, Default)]
    struct TestDeltaReader {
        data: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl TestDeltaReader {
        fn get(&self, key: &str) -> Option<u64> {
            self.data.lock().unwrap().get(key).copied()
        }
    }

    /// Delta accumulates writes with sequence numbers.
    /// Stores the context directly and updates it in place.
    #[derive(Debug)]
    struct TestDelta {
        context: TestContext,
        writes: HashMap<String, (u64, u64)>,
        key_values: Arc<Mutex<HashMap<String, u64>>>,
        total_size: usize,
    }

    #[derive(Clone, Debug)]
    struct FrozenTestDelta {
        writes: HashMap<String, (u64, u64)>,
    }

    impl std::fmt::Debug for View<TestDelta> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("View<TestDelta>")
        }
    }

    impl Delta for TestDelta {
        type Context = TestContext;
        type Write = TestWrite;
        type DeltaView = TestDeltaReader;
        type Frozen = FrozenTestDelta;
        type FrozenView = Arc<HashMap<String, u64>>;
        type ApplyResult = ();

        fn init(context: Self::Context) -> Self {
            Self {
                context,
                writes: HashMap::default(),
                key_values: Arc::new(Mutex::new(HashMap::default())),
                total_size: 0,
            }
        }

        fn apply(&mut self, write: Self::Write) -> Result<(), String> {
            if let Some(error) = &self.context.error {
                return Err(error.clone());
            }

            let seq = self.context.next_seq;
            self.context.next_seq += 1;

            self.writes.insert(write.key.clone(), (seq, write.value));
            self.total_size += write.size;
            self.key_values
                .lock()
                .unwrap()
                .insert(write.key, write.value);
            Ok(())
        }

        fn estimate_size(&self) -> usize {
            self.total_size
        }

        fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
            let frozen = FrozenTestDelta {
                writes: self.writes,
            };
            let frozen_view = Arc::new(self.key_values.lock().unwrap().clone());
            (frozen, frozen_view, self.context)
        }

        fn reader(&self) -> Self::DeltaView {
            TestDeltaReader {
                data: self.key_values.clone(),
            }
        }
    }

    /// Shared state for TestFlusher - allows test to inspect and control behavior
    #[derive(Default)]
    struct TestFlusherState {
        flushed_events: Vec<Arc<EpochStamped<FrozenTestDelta>>>,
        /// Signals when a flush starts (before blocking)
        flush_started_tx: Option<oneshot::Sender<()>>,
        /// Blocks flush until signaled
        unblock_rx: Option<mpsc::Receiver<()>>,
    }

    #[derive(Clone)]
    struct TestFlusher {
        state: Arc<Mutex<TestFlusherState>>,
        storage: Arc<InMemoryStorage>,
    }

    impl Default for TestFlusher {
        fn default() -> Self {
            Self {
                state: Arc::new(Mutex::new(TestFlusherState::default())),
                storage: Arc::new(InMemoryStorage::new()),
            }
        }
    }

    impl TestFlusher {
        /// Create a flusher that blocks until signaled, with a notification when flush starts.
        /// Returns (flusher, flush_started_rx, unblock_tx).
        fn with_flush_control() -> (Self, oneshot::Receiver<()>, mpsc::Sender<()>) {
            let (started_tx, started_rx) = oneshot::channel();
            let (unblock_tx, unblock_rx) = mpsc::channel(1);
            let flusher = Self {
                state: Arc::new(Mutex::new(TestFlusherState {
                    flushed_events: Vec::new(),
                    flush_started_tx: Some(started_tx),
                    unblock_rx: Some(unblock_rx),
                })),
                storage: Arc::new(InMemoryStorage::new()),
            };
            (flusher, started_rx, unblock_tx)
        }

        fn flushed_events(&self) -> Vec<Arc<EpochStamped<FrozenTestDelta>>> {
            self.state.lock().unwrap().flushed_events.clone()
        }

        async fn initial_snapshot(&self) -> Arc<dyn StorageSnapshot> {
            self.storage.snapshot().await.unwrap()
        }
    }

    #[async_trait]
    impl Flusher<TestDelta> for TestFlusher {
        async fn flush_delta(
            &mut self,
            frozen: FrozenTestDelta,
            epoch_range: &Range<u64>,
        ) -> Result<Arc<dyn StorageSnapshot>, String> {
            // Signal that flush has started
            let flush_started_tx = {
                let mut state = self.state.lock().unwrap();
                state.flush_started_tx.take()
            };
            if let Some(tx) = flush_started_tx {
                let _ = tx.send(());
            }

            // Block if test wants to control timing
            let unblock_rx = {
                let mut state = self.state.lock().unwrap();
                state.unblock_rx.take()
            };
            if let Some(mut rx) = unblock_rx {
                rx.recv().await;
            }

            // Write records to storage
            let records: Vec<PutRecordOp> = frozen
                .writes
                .iter()
                .map(|(key, (seq, value))| {
                    let mut buf = Vec::with_capacity(16);
                    buf.extend_from_slice(&seq.to_le_bytes());
                    buf.extend_from_slice(&value.to_le_bytes());
                    Record::new(Bytes::from(key.clone()), Bytes::from(buf)).into()
                })
                .collect();
            self.storage
                .put(records)
                .await
                .map_err(|e| format!("{}", e))?;

            // Record the flush
            {
                let mut state = self.state.lock().unwrap();
                state
                    .flushed_events
                    .push(Arc::new(EpochStamped::new(frozen, epoch_range.clone())));
            }

            self.storage.snapshot().await.map_err(|e| format!("{}", e))
        }

        async fn flush_storage(&self) -> Result<(), String> {
            // Signal that flush has started
            let flush_started_tx = {
                let mut state = self.state.lock().unwrap();
                state.flush_started_tx.take()
            };
            if let Some(tx) = flush_started_tx {
                let _ = tx.send(());
            }

            // Block if test wants to control timing
            let unblock_rx = {
                let mut state = self.state.lock().unwrap();
                state.unblock_rx.take()
            };
            if let Some(mut rx) = unblock_rx {
                rx.recv().await;
            }

            Ok(())
        }
    }

    fn test_config() -> WriteCoordinatorConfig {
        WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600), // Long interval to avoid timer flushes
            flush_size_threshold: usize::MAX,
        }
    }

    async fn assert_snapshot_has_rows(
        snapshot: &Arc<dyn StorageSnapshot>,
        expected: &[(&str, u64, u64)],
    ) {
        let records = snapshot.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(
            records.len(),
            expected.len(),
            "expected {} rows but snapshot has {}",
            expected.len(),
            records.len()
        );
        let mut actual: Vec<(String, u64, u64)> = records
            .iter()
            .map(|r| {
                let key = String::from_utf8(r.key.to_vec()).unwrap();
                let seq = u64::from_le_bytes(r.value[0..8].try_into().unwrap());
                let value = u64::from_le_bytes(r.value[8..16].try_into().unwrap());
                (key, seq, value)
            })
            .collect();
        actual.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected: Vec<(&str, u64, u64)> = expected.to_vec();
        expected.sort_by(|a, b| a.0.cmp(b.0));
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            assert_eq!(
                actual.0, expected.0,
                "key mismatch: got {:?}, expected {:?}",
                actual.0, expected.0
            );
            assert_eq!(
                actual.1, expected.1,
                "seq mismatch for key {:?}: got {}, expected {}",
                actual.0, actual.1, expected.1
            );
            assert_eq!(
                actual.2, expected.2,
                "value mismatch for key {:?}: got {}, expected {}",
                actual.0, actual.2, expected.2
            );
        }
    }

    // ============================================================================
    // Basic Write Flow Tests
    // ============================================================================

    #[tokio::test]
    async fn should_assign_monotonic_epochs() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher,
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let write1 = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let write2 = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let write3 = handle
            .try_write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        let epoch1 = write1.epoch().await.unwrap();
        let epoch2 = write2.epoch().await.unwrap();
        let epoch3 = write3.epoch().await.unwrap();

        // then
        assert!(epoch1 < epoch2);
        assert!(epoch2 < epoch3);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_apply_writes_in_order() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let mut last_write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush(false).await.unwrap();
        // Wait for flush to complete via watermark
        last_write.wait(Durability::Written).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let frozen_delta = &events[0];
        let delta = &frozen_delta.val;
        // Writing key "a" 3x overwrites; last write wins with seq=2 (0-indexed)
        let (seq, value) = delta.writes.get("a").unwrap();
        assert_eq!(*value, 3);
        assert_eq!(*seq, 2);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test]
    async fn should_update_applied_watermark_after_each_write() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher,
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let mut write_handle = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();

        // then - wait should succeed immediately after write is applied
        let result = write_handle.wait(Durability::Applied).await;
        assert!(result.is_ok());

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test]
    async fn should_propagate_apply_error_to_handle() {
        // given
        let flusher = TestFlusher::default();
        let context = TestContext {
            error: Some("apply error".to_string()),
            ..Default::default()
        };
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            context,
            flusher.initial_snapshot().await,
            flusher,
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();

        let result = write.epoch().await;

        // then
        assert!(
            matches!(result, Err(WriteError::ApplyError(epoch, msg)) if epoch == 1 && msg == "apply error")
        );

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Manual Flush Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_on_command() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let mut write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        write.wait(Durability::Written).await.unwrap();

        // then
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_wait_on_flush_handle() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let mut flush_handle = handle.flush(false).await.unwrap();

        // then - can wait directly on the flush handle
        flush_handle.wait(Durability::Written).await.unwrap();
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_return_correct_epoch_from_flush_handle() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher,
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let write1 = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let write2 = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let flush_handle = handle.flush(false).await.unwrap();

        // then - flush handle epoch should be the last write's epoch
        let flush_epoch = flush_handle.epoch().await.unwrap();
        let write2_epoch = write2.epoch().await.unwrap();
        assert_eq!(flush_epoch, write2_epoch);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_all_pending_writes_in_flush() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let mut last_write = handle
            .try_write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush(false).await.unwrap();
        last_write.wait(Durability::Written).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let frozen_delta = &events[0];
        assert_eq!(frozen_delta.val.writes.len(), 3);
        let snapshot = flusher.storage.snapshot().await.unwrap();
        assert_snapshot_has_rows(&snapshot, &[("a", 0, 1), ("b", 1, 2), ("c", 2, 3)]).await;

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_skip_flush_when_no_new_writes() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let mut write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        write.wait(Durability::Written).await.unwrap();

        // Second flush with no new writes
        handle.flush(false).await.unwrap();

        // Synchronization: write and wait for applied to ensure the flush command
        // has been processed (commands are processed in order)
        let sync_write = handle
            .try_write(TestWrite {
                key: "sync".into(),
                value: 0,
                size: 1,
            })
            .await
            .unwrap();
        sync_write.epoch().await.unwrap();

        // then - only one flush should have occurred (the second flush was a no-op)
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_update_written_watermark_after_flush() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher,
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let mut write_handle = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush(false).await.unwrap();

        // then - wait for Written should succeed after flush completes
        let result = write_handle.wait(Durability::Written).await;
        assert!(result.is_ok());

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Timer-Based Flush Tests
    // ============================================================================

    #[tokio::test(start_paused = true)]
    async fn should_flush_on_flush_interval() {
        // given - create coordinator with short flush interval
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_millis(100),
            flush_size_threshold: usize::MAX,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - ensure coordinator task runs and then write something
        tokio::task::yield_now().await;
        let mut write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        write.wait(Durability::Applied).await.unwrap();

        // then - no flush should have happened yet (interval was reset in run())
        assert_eq!(flusher.flushed_events().len(), 0);

        // when - advance time past the flush interval from when run() was called
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        // then - flush should have happened
        assert_eq!(flusher.flushed_events().len(), 1);
        let snapshot = flusher.storage.snapshot().await.unwrap();
        assert_snapshot_has_rows(&snapshot, &[("a", 0, 1)]).await;

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Size-Threshold Flush Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_when_size_threshold_exceeded() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: 100, // Low threshold for testing
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - write that exceeds threshold
        let mut write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 150,
            })
            .await
            .unwrap();
        write.wait(Durability::Written).await.unwrap();

        // then
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_accumulate_until_threshold() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: 100,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - small writes that accumulate
        for i in 0..5 {
            let mut w = handle
                .try_write(TestWrite {
                    key: format!("key{}", i),
                    value: i,
                    size: 15,
                })
                .await
                .unwrap();
            w.wait(Durability::Applied).await.unwrap();
        }

        // then - no flush yet (75 bytes < 100 threshold)
        assert_eq!(flusher.flushed_events().len(), 0);

        // when - write that pushes over threshold
        let mut final_write = handle
            .try_write(TestWrite {
                key: "final".into(),
                value: 999,
                size: 30,
            })
            .await
            .unwrap();
        final_write.wait(Durability::Written).await.unwrap();

        // then - should have flushed (105 bytes > 100 threshold)
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Non-Blocking Flush (Concurrency) Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_accept_writes_during_flush() {
        // given
        let (flusher, flush_started_rx, unblock_tx) = TestFlusher::with_flush_control();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when: trigger a flush and wait for it to start (proving it's in progress)
        let write1 = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        flush_started_rx.await.unwrap(); // wait until flush is actually in progress

        // then: writes during blocked flush still succeed
        let write2 = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        assert!(write2.epoch().await.unwrap() > write1.epoch().await.unwrap());

        // cleanup
        unblock_tx.send(()).await.unwrap();
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_assign_new_epochs_during_flush() {
        // given
        let (flusher, flush_started_rx, unblock_tx) = TestFlusher::with_flush_control();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when: write, flush, then write more during blocked flush
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        flush_started_rx.await.unwrap(); // wait until flush is actually in progress

        // Writes during blocked flush get new epochs
        let w1 = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let w2 = handle
            .try_write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        // then: epochs continue incrementing
        let e1 = w1.epoch().await.unwrap();
        let e2 = w2.epoch().await.unwrap();
        assert!(e1 < e2);

        // cleanup
        unblock_tx.send(()).await.unwrap();
        coordinator.stop().await;
    }

    // ============================================================================
    // Backpressure Tests
    // ============================================================================

    #[tokio::test]
    async fn should_return_backpressure_when_queue_full() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        // Don't start coordinator - queue will fill

        // when - fill the queue
        let _ = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;
        let _ = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await;

        // Third write should fail with backpressure
        let result = handle
            .try_write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await;

        // then
        assert!(matches!(result, Err(WriteError::Backpressure(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_accept_writes_after_queue_drains() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");

        // Fill queue without processing
        let _ = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;
        let mut write_b = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();

        // when - start coordinator to drain queue and wait for it to process writes
        coordinator.start();
        write_b.wait(Durability::Applied).await.unwrap();

        // then - writes should succeed now
        let result = handle
            .try_write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await;
        assert!(result.is_ok());

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Shutdown Tests
    // ============================================================================

    #[tokio::test]
    async fn should_shutdown_cleanly_when_stop_called() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let result = coordinator.stop().await;

        // then - coordinator should return Ok
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_pending_writes_on_shutdown() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600), // Long interval - won't trigger
            flush_size_threshold: usize::MAX,          // High threshold - won't trigger
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - write without explicit flush, then shutdown
        let write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let epoch = write.epoch().await.unwrap();

        // Drop handle to trigger shutdown
        coordinator.stop().await;

        // then - pending writes should have been flushed
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let epoch_range = &events[0].epoch_range;
        assert!(epoch_range.contains(&epoch));
    }

    #[tokio::test]
    async fn should_return_shutdown_error_after_coordinator_stops() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // Stop coordinator
        coordinator.stop().await;

        // when
        let result = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;

        // then
        assert!(matches!(result, Err(WriteError::Shutdown)));
    }

    // ============================================================================
    // Epoch Range Tracking Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_track_epoch_range_in_flush_event() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let mut last_write = handle
            .try_write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush(false).await.unwrap();
        last_write.wait(Durability::Written).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let epoch_range = &events[0].epoch_range;
        assert_eq!(epoch_range.start, 1);
        assert_eq!(epoch_range.end, 4); // exclusive: one past the last epoch (3)

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_have_contiguous_epoch_ranges() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - first batch
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let mut write2 = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        write2.wait(Durability::Written).await.unwrap();

        // when - second batch
        let mut write3 = handle
            .try_write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        write3.wait(Durability::Written).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 2);

        let range1 = &events[0].epoch_range;
        let range2 = &events[1].epoch_range;

        // Ranges should be contiguous (end of first == start of second)
        assert_eq!(range1.end, range2.start);
        assert_eq!(range1, &(1..3));
        assert_eq!(range2, &(3..4));

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_exact_epochs_in_range() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - write and capture the assigned epochs
        let write1 = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let epoch1 = write1.epoch().await.unwrap();

        let mut write2 = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let epoch2 = write2.epoch().await.unwrap();

        handle.flush(false).await.unwrap();
        write2.wait(Durability::Written).await.unwrap();

        // then - the epoch_range should contain exactly the epochs assigned to writes
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let epoch_range = &events[0].epoch_range;

        // The range should start at the first write's epoch
        assert_eq!(epoch_range.start, epoch1);
        // The range end should be one past the last write's epoch (exclusive)
        assert_eq!(epoch_range.end, epoch2 + 1);
        // Both epochs should be contained in the range
        assert!(epoch_range.contains(&epoch1));
        assert!(epoch_range.contains(&epoch2));

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // State Carryover (ID Allocation) Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_preserve_context_across_flushes() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - write key "a" in first batch (seq 0)
        let mut write1 = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        write1.wait(Durability::Written).await.unwrap();

        // Write to key "a" again in second batch (seq 1)
        let mut write2 = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();
        write2.wait(Durability::Written).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 2);

        // Batch 1: "a" with seq 0
        let (seq1, _) = events[0].val.writes.get("a").unwrap();
        assert_eq!(*seq1, 0);

        // Batch 2: "a" with seq 1 (sequence continues)
        let (seq2, _) = events[1].val.writes.get("a").unwrap();
        assert_eq!(*seq2, 1);

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Subscribe Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_receive_view_on_subscribe() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        let (mut subscriber, _) = coordinator.subscribe();
        subscriber.initialize();
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();

        // then - first broadcast is on freeze (delta added to frozen)
        let result = subscriber.recv().await;
        assert!(result.is_ok());

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_snapshot_in_view_after_flush() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        let (mut subscriber, _) = coordinator.subscribe();
        subscriber.initialize();
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();

        // First broadcast: freeze (frozen delta added)
        let _ = subscriber.recv().await.unwrap();
        // Second broadcast: flush complete (snapshot updated)
        let result = subscriber.recv().await.unwrap();

        // then - snapshot should contain the flushed data
        assert_snapshot_has_rows(&result.snapshot, &[("a", 0, 1)]).await;

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_delta_in_view_after_flush() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        let (mut subscriber, _) = coordinator.subscribe();
        subscriber.initialize();
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 42,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();

        // First broadcast: freeze
        let _ = subscriber.recv().await.unwrap();
        // Second broadcast: flush complete
        let result = subscriber.recv().await.unwrap();

        // then - last_written_delta should contain the write we made
        let flushed = result.last_written_delta.as_ref().unwrap();
        assert_eq!(flushed.val.get("a"), Some(&42));

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_epoch_range_in_view_after_flush() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        let (mut subscriber, _) = coordinator.subscribe();
        subscriber.initialize();
        coordinator.start();

        // when
        let write1 = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let write2 = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();

        // First broadcast: freeze
        let _ = subscriber.recv().await.unwrap();
        // Second broadcast: flush complete
        let result = subscriber.recv().await.unwrap();

        // then - epoch range from last_written_delta should contain the epochs
        let flushed = result.last_written_delta.as_ref().unwrap();
        let epoch1 = write1.epoch().await.unwrap();
        let epoch2 = write2.epoch().await.unwrap();
        assert!(flushed.epoch_range.contains(&epoch1));
        assert!(flushed.epoch_range.contains(&epoch2));
        assert_eq!(flushed.epoch_range.start, epoch1);
        assert_eq!(flushed.epoch_range.end, epoch2 + 1);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_broadcast_frozen_delta_on_freeze() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        let (mut subscriber, _) = coordinator.subscribe();
        subscriber.initialize();
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();

        // then - first broadcast should have the frozen delta in the frozen vec
        let state = subscriber.recv().await.unwrap();
        assert_eq!(state.frozen.len(), 1);
        assert!(state.frozen[0].val.contains_key("a"));

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_remove_frozen_delta_after_flush_complete() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        let (mut subscriber, _) = coordinator.subscribe();
        subscriber.initialize();
        coordinator.start();

        // when
        handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush(false).await.unwrap();

        // First broadcast: freeze (frozen has 1 entry)
        let state1 = subscriber.recv().await.unwrap();
        assert_eq!(state1.frozen.len(), 1);

        // Second broadcast: flush complete (frozen is empty, last_written_delta set)
        let state2 = subscriber.recv().await.unwrap();
        assert_eq!(state2.frozen.len(), 0);
        assert!(state2.last_written_delta.is_some());

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_recover_from_message_lost_subscriber() {
        // given - a coordinator with a small broadcast buffer (16)
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        let (mut subscriber, _) = coordinator.subscribe();
        subscriber.initialize();
        coordinator.start();

        // when - write and flush enough times to overflow the broadcast buffer
        // (capacity is 16, so ~20 flushes should cause lag)
        // The subscriber never called recv(), so all broadcasts accumulate and overflow the buffer
        for i in 0..20 {
            let _write_handle = handle
                .try_write(TestWrite {
                    key: format!("key_{}", i),
                    value: i as u64,
                    size: 10,
                })
                .await
                .unwrap();
            let _ = handle.flush(false).await.unwrap();
        }

        // make changes durable
        let mut watermark = handle
            .flush(true)
            .await
            .expect("flush(true) should succeed");

        // wait for durability watermark
        watermark.wait(Durability::Durable).await;

        // expect SubscribeError to be MessageLost
        let result = subscriber
            .recv()
            .await
            .expect_err("expected recv() to yield an error");
        assert!(matches!(result, SubscribeError::MessageLost));

        // when - resubscribe to recover
        let (rx, initial_view) = handle.subscribe();
        (subscriber, _) = ViewSubscriber::new(rx, initial_view);
        let view = subscriber.initialize();

        // then - the fresh view should reflect the current state
        // (all 20 writes should be in the snapshot after all the flushes)
        let records = view.snapshot.scan(BytesRange::unbounded()).await.unwrap();
        assert!(
            records.len() >= 20,
            "expected at least 20 rows, got {}",
            records.len()
        );

        // and - we should be able to receive future broadcasts
        let _write_handle = handle
            .try_write(TestWrite {
                key: "post_recovery".into(),
                value: 100,
                size: 10,
            })
            .await
            .unwrap();
        let _ = handle.flush(false).await.unwrap();

        let result = subscriber.recv().await;
        assert!(result.is_ok());

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Durable Flush Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_even_when_no_writes_if_flush_storage() {
        // given
        let flusher = TestFlusher::default();
        let storage = Arc::new(InMemoryStorage::new());
        let snapshot = storage.snapshot().await.unwrap();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            snapshot,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - flush with flush_storage but no pending writes
        let mut flush_handle = handle.flush(true).await.unwrap();
        flush_handle.wait(Durability::Durable).await.unwrap();

        // then - flusher was called (durable event sent) but no delta was recorded
        // (TestFlusher only records events with deltas)
        assert_eq!(flusher.flushed_events().len(), 0);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_advance_durable_watermark() {
        // given
        let flusher = TestFlusher::default();
        let storage = Arc::new(InMemoryStorage::new());
        let snapshot = storage.snapshot().await.unwrap();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            snapshot,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when - write and flush with durable
        let mut write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let mut flush_handle = handle.flush(true).await.unwrap();

        // then - can wait for Durable durability level
        flush_handle.wait(Durability::Durable).await.unwrap();
        write.wait(Durability::Durable).await.unwrap();
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test]
    async fn should_see_applied_write_via_view() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher,
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let mut write = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 42,
                size: 10,
            })
            .await
            .unwrap();
        write.wait(Durability::Applied).await.unwrap();

        // then
        let view = coordinator.view();
        assert_eq!(view.current.get("a"), Some(42));

        // cleanup
        coordinator.stop().await;
    }

    // ============================================================================
    // Multi-Channel Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_writes_from_multiple_channels() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["ch1".to_string(), "ch2".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let ch1 = coordinator.handle("ch1");
        let ch2 = coordinator.handle("ch2");
        coordinator.start();

        // when - write to both channels, waiting for each to be applied
        // to ensure deterministic ordering
        let mut w1 = ch1
            .try_write(TestWrite {
                key: "a".into(),
                value: 10,
                size: 10,
            })
            .await
            .unwrap();
        w1.wait(Durability::Applied).await.unwrap();

        let mut w2 = ch2
            .try_write(TestWrite {
                key: "b".into(),
                value: 20,
                size: 10,
            })
            .await
            .unwrap();
        w2.wait(Durability::Applied).await.unwrap();

        let mut w3 = ch1
            .try_write(TestWrite {
                key: "c".into(),
                value: 30,
                size: 10,
            })
            .await
            .unwrap();
        w3.wait(Durability::Applied).await.unwrap();

        ch1.flush(false).await.unwrap();
        w3.wait(Durability::Written).await.unwrap();

        // then - snapshot should contain writes from both channels
        let snapshot = flusher.storage.snapshot().await.unwrap();
        assert_snapshot_has_rows(&snapshot, &[("a", 0, 10), ("b", 1, 20), ("c", 2, 30)]).await;

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_succeed_with_write_timeout_when_queue_has_space() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        let mut wh = handle
            .write_timeout(
                TestWrite {
                    key: "a".into(),
                    value: 1,
                    size: 10,
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        // then
        let result = wh.wait(Durability::Applied).await;
        assert!(result.is_ok());

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test]
    async fn should_timeout_when_queue_full() {
        // given - queue_capacity=2, coordinator NOT started so nothing drains
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");

        // fill the queue
        let _ = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;
        let _ = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await;

        // when - third write should time out
        let result = handle
            .write_timeout(
                TestWrite {
                    key: "c".into(),
                    value: 3,
                    size: 10,
                },
                Duration::from_millis(10),
            )
            .await;

        // then
        assert!(matches!(result, Err(WriteError::TimeoutError(_))));
    }

    #[tokio::test]
    async fn should_return_write_in_timeout_error() {
        // given - queue_capacity=1, coordinator NOT started
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 1,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");

        // fill the queue
        let _ = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;

        // when
        let result = handle
            .write_timeout(
                TestWrite {
                    key: "retry_me".into(),
                    value: 42,
                    size: 10,
                },
                Duration::from_millis(10),
            )
            .await;
        let Err(err) = result else {
            panic!("expected TimeoutError");
        };

        // then - original write is returned inside the error
        let write = err.into_inner().expect("should contain the write");
        assert_eq!(write.key, "retry_me");
        assert_eq!(write.value, 42);
    }

    #[tokio::test]
    async fn should_return_write_in_backpressure_error() {
        // given - queue_capacity=1, coordinator NOT started
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 1,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");

        // fill the queue
        let _ = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;

        // when
        let result = handle
            .try_write(TestWrite {
                key: "retry_me".into(),
                value: 42,
                size: 10,
            })
            .await;
        let Err(err) = result else {
            panic!("expected Backpressure");
        };

        // then - original write is returned inside the error
        let write = err.into_inner().expect("should contain the write");
        assert_eq!(write.key, "retry_me");
        assert_eq!(write.value, 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_succeed_when_queue_drains_within_timeout() {
        // given - queue_capacity=2, coordinator started so it drains
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");

        // fill the queue before starting
        let _ = handle
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;
        let _ = handle
            .try_write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await;

        // when - start coordinator (begins draining) and write with generous timeout
        coordinator.start();
        let result = handle
            .write_timeout(
                TestWrite {
                    key: "c".into(),
                    value: 3,
                    size: 10,
                },
                Duration::from_secs(5),
            )
            .await;

        // then
        assert!(result.is_ok());

        // cleanup
        coordinator.stop().await;
    }

    #[tokio::test]
    async fn should_return_shutdown_on_write_timeout_after_coordinator_stops() {
        // given
        let flusher = TestFlusher::default();
        let mut coordinator = WriteCoordinator::new(
            test_config(),
            vec!["default".to_string()],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle = coordinator.handle("default");
        coordinator.start();

        // when
        coordinator.stop().await;
        let result = handle
            .write_timeout(
                TestWrite {
                    key: "a".into(),
                    value: 1,
                    size: 10,
                },
                Duration::from_secs(1),
            )
            .await;

        // then
        assert!(matches!(result, Err(WriteError::Shutdown)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_pause_and_resume_write_channel() {
        // given
        let flusher = TestFlusher::default();
        let mut config = test_config();
        config.flush_size_threshold = usize::MAX;
        config.flush_interval = Duration::from_hours(24);
        let mut coordinator = WriteCoordinator::new(
            config,
            vec!["a", "b"],
            TestContext::default(),
            flusher.initial_snapshot().await,
            flusher.clone(),
        );
        let handle_a = coordinator.handle("a");
        let handle_b = coordinator.handle("b");
        let pause_handle = coordinator.pause_handle("a");

        // pause before starting the coordinator. otherwise
        // there's a race condition where the PausableReceiver
        // makes it past pause_rx.wait_for(|v| !*v).await; and
        // waits for recv before we pause the handle - we could
        // change the behavior of the PausableReceiver to first
        // wait on recv and then check pause before returning
        // but that feels weird and is not necessary for the use
        // cases we have in mind
        pause_handle.pause();
        coordinator.start();

        // when
        let mut result_a = handle_a
            .try_write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 1,
            })
            .await
            .unwrap();
        for i in 0..1000 {
            handle_b
                .try_write(TestWrite {
                    key: format!("b{}", i),
                    value: i,
                    size: 1,
                })
                .await
                .unwrap()
                .wait(Durability::Applied)
                .await
                .unwrap();
        }

        // then
        // make sure the data only contains keys from b (so a was never processed)
        let data = coordinator.view().current.data.lock().unwrap().clone();
        let mut expected = (0..1000).map(|i| format!("b{}", i)).collect::<HashSet<_>>();
        assert_eq!(data.keys().cloned().collect::<HashSet<_>>(), expected);
        pause_handle.unpause();
        // after resuming, wait for the data to include keys from a
        result_a.wait(Durability::Applied).await.unwrap();
        let data = coordinator.view().current.data.lock().unwrap().clone();
        expected.insert("a".into());
        assert_eq!(data.keys().cloned().collect::<HashSet<_>>(), expected);
    }
}
