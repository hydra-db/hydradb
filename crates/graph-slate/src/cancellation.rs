use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[derive(Clone, Debug, Default)]
pub struct QueryCancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

tokio::task_local! {
    static ACTIVE_QUERY_CANCELLATION: QueryCancellationToken;
}

impl QueryCancellationToken {
    pub async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        ACTIVE_QUERY_CANCELLATION.scope(self.clone(), future).await
    }

    pub fn current() -> Option<Self> {
        ACTIVE_QUERY_CANCELLATION.try_with(Clone::clone).ok()
    }

    /// Run `future` with `token` as its active cancellation. A spawned task
    /// does not inherit the spawning task's locals, so work moved onto its own
    /// task re-enters the request's token here to stay cancellable.
    #[cfg(any(feature = "opencypher", feature = "query-transport"))]
    pub async fn reenter<F: std::future::Future>(token: Option<Self>, future: F) -> F::Output {
        match token {
            Some(token) => ACTIVE_QUERY_CANCELLATION.scope(token, future).await,
            None => future.await,
        }
    }

    /// Run `future` under a token nothing cancels: work that must not stop
    /// part-way once it has started writing.
    #[cfg(feature = "opencypher")]
    pub async fn shielded<F: std::future::Future>(future: F) -> F::Output {
        ACTIVE_QUERY_CANCELLATION.scope(Self::new(), future).await
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

impl PartialEq for QueryCancellationToken {
    fn eq(&self, other: &Self) -> bool {
        self.is_cancelled() == other.is_cancelled()
    }
}

impl Eq for QueryCancellationToken {}
