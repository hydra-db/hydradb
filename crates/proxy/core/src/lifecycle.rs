use tokio::sync::watch;

/// Where the proxy is in its life: serving, then stopped, with no draining
/// state between them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleState {
    Serving,
    Stopped,
}

/// A watch on the proxy's state, shared by both adapters and `/readyz`.
#[derive(Clone)]
pub struct Lifecycle {
    tx: watch::Sender<LifecycleState>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self {
            tx: watch::Sender::new(LifecycleState::Serving),
        }
    }
}

impl Lifecycle {
    pub fn state(&self) -> LifecycleState {
        *self.tx.borrow()
    }

    pub fn is_serving(&self) -> bool {
        self.state() == LifecycleState::Serving
    }

    pub fn stop(&self) {
        self.tx.send_replace(LifecycleState::Stopped);
    }

    pub fn subscribe(&self) -> watch::Receiver<LifecycleState> {
        self.tx.subscribe()
    }
}
