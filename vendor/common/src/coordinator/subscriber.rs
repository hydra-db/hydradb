//! View subscription and progress monitoring.
//!
//! When a caller subscribes to a [`WriteCoordinator`](super::WriteCoordinator),
//! it receives a paired [`ViewSubscriber`] and [`ViewMonitor`]:
//!
//! - The **subscriber** receives [`View`] broadcasts from the coordinator
//!   (on freeze and flush events) and advances epoch watermarks to signal
//!   how far it has processed.
//! - The **monitor** lets other tasks wait until the subscriber has reached
//!   a given epoch at a given durability level.
//!
//! Together they form a progress-tracking channel: the subscriber drives
//! forward, and the monitor synchronizes on that progress.
//!
//! # Usage
//!
//! ```ignore
//! // Subscribe to the coordinator.
//! let (mut subscriber, monitor) = coordinator.subscribe();
//!
//! // Spawn a task that processes views as they arrive.
//! tokio::spawn(async move {
//!     // Initialize returns the initial view, captured atomically with
//!     // the subscription. Must be called before recv().
//!     let initial_view = subscriber.initialize();
//!     // Bootstrap read state from initial_view ...
//!
//!     loop {
//!         match subscriber.recv().await {
//!             Ok(view) => {
//!                 // Process the view (update read state)...
//!                 subscriber.update_durable(epoch);
//!             }
//!             Err(SubscribeError::MessageLost) => {
//!                 // Re-subscribe to get a consistent initial view, then
//!                 // recover db-specific state from the snapshot.
//!                 let (rx, view) = handle.subscribe();
//!                 (subscriber, _) = ViewSubscriber::new(rx, view);
//!                 let view = subscriber.initialize();
//!                 // db.recover_from_view(&view).await?;
//!             }
//!             Err(SubscribeError::Shutdown) => break,
//!             Err(SubscribeError::NotInitialized) => unreachable!(),
//!         }
//!     }
//! });
//!
//! // Elsewhere, wait for the subscriber to catch up.
//! monitor.clone().wait(epoch, Durability::Durable).await?;
//! ```

use std::sync::Arc;

use tokio::sync::broadcast;

use super::traits::EpochStamped;
use super::{Delta, Durability, EpochWatcher, EpochWatermarks, View};

/// Error type for subscriber and monitor operations.
#[derive(Debug)]
pub enum SubscribeError {
    /// The coordinator has shut down.
    Shutdown,
    /// [`ViewSubscriber::recv()`] was called before [`ViewSubscriber::initialize()`].
    NotInitialized,
    /// A message was lost indicating that the subscriber is lagging behind the coordinator's progress.
    MessageLost,
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscribeError::Shutdown => write!(f, "coordinator shut down"),
            SubscribeError::NotInitialized => {
                write!(f, "initialize() must be called before recv()")
            }
            SubscribeError::MessageLost => {
                write!(f, "message was lost, subscriber is lagging behind")
            }
        }
    }
}

impl std::error::Error for SubscribeError {}

/// Receives [`View`] broadcasts from the coordinator and advances epoch
/// watermarks to signal progress. Paired with a [`ViewMonitor`] that can
/// wait on the watermarks this subscriber advances.
pub struct ViewSubscriber<D: Delta> {
    view_rx: broadcast::Receiver<Arc<View<D>>>,
    initial_view: Option<Arc<View<D>>>,
    watermarks: Arc<EpochWatermarks>,
}

impl<D: Delta> ViewSubscriber<D> {
    /// Creates a new `ViewSubscriber` and paired `ViewMonitor`.
    pub fn new(
        view_rx: broadcast::Receiver<Arc<View<D>>>,
        initial_view: Arc<View<D>>,
    ) -> (Self, ViewMonitor) {
        let (watermarks, watcher) = EpochWatermarks::new();
        let watermarks = Arc::new(watermarks);
        let subscriber = Self {
            view_rx,
            initial_view: Some(initial_view),
            watermarks: watermarks.clone(),
        };
        let monitor = ViewMonitor {
            watcher,
            watermarks,
        };
        (subscriber, monitor)
    }

    /// Takes the initial view captured at subscription time, marking the
    /// subscriber as ready to receive broadcasts.
    ///
    /// Must be called exactly once before [`recv()`](Self::recv). The initial
    /// view is captured atomically with the broadcast subscription, so it is
    /// safe to use even when subscribing to an active writer.
    pub fn initialize(&mut self) -> Arc<View<D>> {
        self.initial_view
            .take()
            .expect("initialize() must be called exactly once")
    }

    /// Receives the next view broadcast from the coordinator.
    ///
    /// Returns [`SubscribeError::NotInitialized`] if [`initialize()`](Self::initialize)
    /// has not been called.
    pub async fn recv(&mut self) -> Result<Arc<View<D>>, SubscribeError> {
        if self.initial_view.is_some() {
            return Err(SubscribeError::NotInitialized);
        }
        match self.view_rx.recv().await {
            Ok(view) => Ok(view),
            Err(broadcast::error::RecvError::Lagged(_)) => Err(SubscribeError::MessageLost), // the subscriber is lagging behind the coordinator's progress.
            Err(broadcast::error::RecvError::Closed) => Err(SubscribeError::Shutdown),
        }
    }

    /// Advances the applied watermark, signaling that the reader has processed
    /// up through the given epoch.
    pub fn update_applied(&self, epoch: u64) {
        self.watermarks.update_applied(epoch);
    }

    /// Advances the flushed watermark, signaling that the reader has processed
    /// up through the given epoch.
    pub fn update_written(&self, epoch: u64) {
        self.watermarks.update_written(epoch);
    }

    /// Advances the durable watermark, signaling that the reader has processed
    /// up through the given epoch.
    pub fn update_durable(&self, epoch: u64) {
        self.watermarks.update_durable(epoch);
    }
}

/// Monitors the progress of a paired [`ViewSubscriber`] through epoch
/// watermarks.
///
/// Cloneable — multiple tasks can wait on the same subscriber's progress.
#[derive(Clone)]
pub struct ViewMonitor {
    watcher: EpochWatcher,
    #[allow(dead_code)]
    watermarks: Arc<EpochWatermarks>,
}

impl ViewMonitor {
    /// Waits until the subscriber has processed at least `epoch` at the
    /// given [`Durability`] level.
    pub async fn wait(&mut self, epoch: u64, durability: Durability) -> Result<(), SubscribeError> {
        self.watcher
            .wait(epoch, durability)
            .await
            .map_err(|_| SubscribeError::Shutdown)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a paired watermarks and `ViewMonitor` without needing a
    /// full coordinator. This is sufficient for testing the update/wait
    /// contract since those only touch the watermark channels.
    fn create_pair() -> (Arc<EpochWatermarks>, ViewMonitor) {
        let (watermarks, watcher) = EpochWatermarks::new();
        let watermarks = Arc::new(watermarks);
        let monitor = ViewMonitor {
            watcher,
            watermarks: watermarks.clone(),
        };
        (watermarks, monitor)
    }

    #[tokio::test]
    async fn should_update_and_wait_applied() {
        // given
        let (watermarks, mut monitor) = create_pair();

        // when
        watermarks.update_applied(5);

        // then
        monitor.wait(5, Durability::Applied).await.unwrap();
    }

    #[tokio::test]
    async fn should_update_and_wait_flushed() {
        // given
        let (watermarks, mut monitor) = create_pair();

        // when
        watermarks.update_written(3);

        // then
        monitor.wait(3, Durability::Written).await.unwrap();
    }

    #[tokio::test]
    async fn should_update_and_wait_durable() {
        // given
        let (watermarks, mut monitor) = create_pair();

        // when
        watermarks.update_durable(7);

        // then
        monitor.wait(7, Durability::Durable).await.unwrap();
    }

    #[tokio::test]
    async fn should_wait_for_epoch_already_reached() {
        // given
        let (watermarks, mut monitor) = create_pair();

        // when - advance past the epoch we'll wait for
        watermarks.update_durable(10);

        // then - waiting for a lower epoch returns immediately
        monitor.wait(5, Durability::Durable).await.unwrap();
    }

    #[tokio::test]
    async fn should_track_levels_independently() {
        // given
        let (watermarks, mut monitor) = create_pair();

        // when - advance each level to a different epoch
        watermarks.update_applied(3);
        watermarks.update_written(2);
        watermarks.update_durable(1);

        // then - each level tracks independently
        monitor.wait(3, Durability::Applied).await.unwrap();
        monitor.wait(2, Durability::Written).await.unwrap();
        monitor.wait(1, Durability::Durable).await.unwrap();
    }
}
