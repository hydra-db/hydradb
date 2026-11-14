//! Failed-query reports and a bounded HTTP exporter, independent of the database.

use std::{sync::Arc, time::Duration};

use reqwest::{header, Client, Url};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, Semaphore};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub const MAX_EVENT_BYTES: usize = 128 * 1024;
const MAX_RETAINED_BYTES: usize = 8 * 1024 * 1024;

/// Versioned wire format shared by GraphNode and the receiver. No parameter values,
/// credentials, cursor tokens or bookmarks are included.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryFailureEvent {
    pub schema_version: u16,
    pub event_id: String,
    pub timestamp_unix_ms: u64,
    pub node_id: String,
    pub server_version: String,
    pub query_id: String,
    pub protocol: String,
    pub query: String,
    pub query_truncated: bool,
    pub query_sha256: String,
    pub namespace: String,
    pub graph_id: String,
    pub cell_id: String,
    pub correlation_id: Option<String>,
    pub caller_step: Option<String>,
    pub parameter_count: usize,
    pub parameter_names: Vec<String>,
    pub configured_engine: String,
    pub engine: String,
    pub stage: String,
    pub elapsed_us: u64,
    pub timeout_ms: Option<u64>,
    pub consistency: String,
    pub page_size: Option<usize>,
    pub cursor_continuation: bool,
    pub read_epoch: Option<u64>,
    pub error_class: String,
    pub failure_reason: Option<String>,
    pub error_operation: Option<String>,
    pub error: String,
    pub error_truncated: bool,
}

#[derive(Clone)]
pub struct QueryFailureReporter(Arc<ReporterInner>);

struct ReporterInner {
    sender: mpsc::Sender<QueuedReport>,
    byte_budget: Arc<Semaphore>,
    stop: watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct QueuedReport {
    build: Box<dyn FnOnce() -> QueryFailureEvent + Send>,
    _bytes: OwnedSemaphorePermit,
}

impl QueuedReport {
    async fn deliver(self, client: &Client, endpoint: &Url) {
        let Self { build, _bytes } = self;
        deliver(client, endpoint, &build()).await;
    }
}

/// Parse endpoints without exposing credentials in errors or following redirects.
pub fn http_url(value: &str) -> Result<Url, Error> {
    let url = Url::parse(value).map_err(|_| "invalid telemetry HTTP URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err("telemetry URL must use HTTP(S), without user info, query or fragment".into());
    }
    Ok(url)
}

pub fn bearer_headers(token: Option<&str>) -> Result<header::HeaderMap, Error> {
    let mut headers = header::HeaderMap::new();
    if let Some(token) = token {
        if token.trim().is_empty() {
            return Err("telemetry bearer token must not be empty".into());
        }
        let mut value = header::HeaderValue::from_str(&format!("Bearer {}", token.trim()))?;
        value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, value);
    }
    Ok(headers)
}

impl QueryFailureReporter {
    /// Must be called inside a Tokio runtime. Admission caps retained payloads at
    /// 8 MiB and queued reports at 256. In-flight delivery retains its byte permit.
    pub fn new(endpoint: Url, token: Option<&str>) -> Result<Self, Error> {
        let endpoint = http_url(endpoint.as_str())?;
        let client = Client::builder()
            .default_headers(bearer_headers(token)?)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(2))
            .build()?;
        let (sender, mut receiver) = mpsc::channel::<QueuedReport>(256);
        let (stop, mut stopping) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    biased;
                    _ = stopping.changed() => {
                        receiver.close();
                        break;
                    }
                    event = receiver.recv() => match event {
                        Some(event) => event,
                        None => return,
                    }
                };
                event.deliver(&client, &endpoint).await;
            }
            while let Some(event) = receiver.recv().await {
                event.deliver(&client, &endpoint).await;
            }
        });
        Ok(Self(Arc::new(ReporterInner {
            sender,
            byte_budget: Arc::new(Semaphore::new(MAX_RETAINED_BYTES)),
            stop,
            task: Mutex::new(Some(task)),
        })))
    }

    /// Reserve queue space and retained payload bytes before invoking `snapshot`.
    /// `retained_bytes` counts heap allocations owned by its returned formatter;
    /// the formatter and queue-entry sizes are added here. Never waits for space.
    pub fn report<F>(&self, retained_bytes: usize, snapshot: impl FnOnce() -> F)
    where
        F: FnOnce() -> QueryFailureEvent + Send + 'static,
    {
        let Ok(slot) = self.0.sender.try_reserve() else {
            tracing::warn!("query telemetry event dropped: exporter queue full or closed");
            return;
        };
        let bytes = retained_bytes
            .saturating_add(size_of::<F>())
            .saturating_add(size_of::<QueuedReport>());
        let bytes = u32::try_from(bytes).unwrap_or(u32::MAX);
        let Ok(bytes) = Arc::clone(&self.0.byte_budget).try_acquire_many_owned(bytes) else {
            tracing::warn!("query telemetry event dropped: exporter byte budget exhausted");
            return;
        };
        slot.send(QueuedReport {
            build: Box::new(snapshot()),
            _bytes: bytes,
        });
    }

    pub async fn shutdown(&self, deadline: Duration) {
        let _ = self.0.stop.send(true);
        if let Some(mut task) = self.0.task.lock().await.take() {
            if tokio::time::timeout(deadline, &mut task).await.is_err() {
                task.abort();
                tracing::warn!("query telemetry drain timed out; queued events dropped");
            }
        }
    }
}

async fn deliver(client: &Client, endpoint: &Url, event: &QueryFailureEvent) {
    for attempt in 0..3 {
        let result = client.post(endpoint.clone()).json(event).send().await;
        let retry = match result {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => response.status().is_server_error() || response.status().as_u16() == 429,
            Err(_) => true,
        };
        if !retry || attempt == 2 {
            tracing::warn!(event_id = %event.event_id, "query telemetry delivery failed; event dropped");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
    }
}
