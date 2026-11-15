use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The cap on open connections. A connection over the cap is refused before
/// anything is written to it.
#[derive(Clone)]
pub struct ConnectionGate(Arc<Semaphore>);

/// One open connection's place under the cap, released on drop.
pub struct ConnectionPermit {
    _permit: OwnedSemaphorePermit,
}

impl ConnectionGate {
    pub fn new(max_connections: usize) -> Self {
        Self(Arc::new(Semaphore::new(max_connections)))
    }

    /// A permit, or `None` when the gate is full. Never waits.
    pub fn try_acquire(&self) -> Option<ConnectionPermit> {
        let permit = Arc::clone(&self.0).try_acquire_owned().ok()?;
        Some(ConnectionPermit { _permit: permit })
    }
}
