use std::{
    env,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use hydradb_pg_usage_report::PgUsageReport;
use hydradb_query_telemetry::{http_url, Error, QueryFailureEvent, MAX_EVENT_BYTES};
use hydradb_usage_report::{UsageReport, MAX_REPORT_BYTES};
use reqwest::{header, Client, Url};
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, watch};

/// One row type the receiver buffers and batch-inserts into its own table.
trait Row: Send + 'static {
    const TABLE: &'static str;
    /// What the batches log as, for operators reading the receiver's output.
    const NAME: &'static str;
    /// The table's `CREATE TABLE IF NOT EXISTS`, for a single server.
    const SCHEMA: &'static str;
    fn row(&self) -> Result<serde_json::Value, Error>;
}

impl Row for QueryFailureEvent {
    const TABLE: &'static str = "query_failures";
    const NAME: &'static str = "query failure";
    const SCHEMA: &'static str = include_str!("schema.sql");

    fn row(&self) -> Result<serde_json::Value, Error> {
        Ok(json!({
            "event_id": self.event_id, "timestamp_unix_ms": self.timestamp_unix_ms,
            "node_id": self.node_id, "namespace": self.namespace, "graph_id": self.graph_id,
            "cell_id": self.cell_id, "engine": self.engine, "stage": self.stage,
            "error_class": self.error_class, "failure_reason": self.failure_reason.as_deref().unwrap_or(""),
            "query": self.query, "elapsed_us": self.elapsed_us, "event": serde_json::to_string(self)?,
        }))
    }
}

impl Row for UsageReport {
    const TABLE: &'static str = "usage_reports";
    const NAME: &'static str = "usage report";
    const SCHEMA: &'static str = include_str!("usage_schema.sql");

    /// The payload is the report re-serialized from its type, not the body as
    /// received: fields this receiver does not know are dropped rather than
    /// stored unchecked.
    fn row(&self) -> Result<serde_json::Value, Error> {
        Ok(json!({
            "report_id": self.report_id, "sent_at_unix_ms": self.sent_at_unix_ms,
            "install_id": self.install_id, "run_id": self.run_id,
            "reason": serde_json::to_value(self.reason)?, "version": self.build.version,
            "payload": serde_json::to_string(self)?,
        }))
    }
}

impl Row for PgUsageReport {
    const TABLE: &'static str = "pg_usage_reports";
    const NAME: &'static str = "pg_hydradb usage report";
    const SCHEMA: &'static str = include_str!("pg_usage_schema.sql");

    /// Re-serialized from its type, as [`UsageReport`]'s is.
    fn row(&self) -> Result<serde_json::Value, Error> {
        Ok(json!({
            "report_id": self.report_id, "sent_at_unix_ms": self.sent_at_unix_ms,
            "install_id": self.install_id, "server_started_at_unix_ms": self.server_started_at_unix_ms,
            "extension_version": self.build.extension_version, "postgres_major": self.build.postgres_major,
            "payload": serde_json::to_string(self)?,
        }))
    }
}

struct Receiver {
    failures: mpsc::Sender<QueryFailureEvent>,
    usage: mpsc::Sender<UsageReport>,
    pg_usage: mpsc::Sender<PgUsageReport>,
    accepted: AtomicU64,
    /// Applies to `/v1/query-failures` only. Usage reports come from every
    /// install, whose binaries cannot keep a token secret, so that route is
    /// open and relies on its size limit, its check, and the queue bound; rate
    /// limits belong at the ingress in front of it.
    authorization: Option<String>,
    failures_healthy: Arc<AtomicBool>,
    usage_healthy: Arc<AtomicBool>,
    pg_usage_healthy: Arc<AtomicBool>,
    inserted: Arc<AtomicU64>,
}

impl Receiver {
    fn admit<T>(&self, sender: &mpsc::Sender<T>, item: T) -> StatusCode {
        match sender.try_send(item) {
            Ok(()) => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                StatusCode::ACCEPTED
            }
            Err(mpsc::error::TrySendError::Full(_)) => StatusCode::TOO_MANY_REQUESTS,
            Err(mpsc::error::TrySendError::Closed(_)) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

struct ClickHouse {
    client: Client,
    url: Url,
    database: String,
    /// The cluster of a replicated ClickHouse, whose tables must exist on
    /// every replica; see [`ClickHouse::create_table`].
    cluster: Option<String>,
}

impl ClickHouse {
    fn from_env() -> Result<Self, Error> {
        let url = http_url(&env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into()))?;
        let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into());
        if database.is_empty() || !database.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err("CLICKHOUSE_DATABASE must contain only ASCII letters, digits and underscores".into());
        }
        let cluster = env::var("CLICKHOUSE_CLUSTER").ok().filter(|cluster| !cluster.is_empty());
        if cluster
            .as_deref()
            .is_some_and(|cluster| !cluster.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'))
        {
            return Err("CLICKHOUSE_CLUSTER must contain only ASCII letters, digits, underscores and hyphens".into());
        }
        let mut headers = header::HeaderMap::new();
        let mut user = header::HeaderValue::from_str(&env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()))?;
        user.set_sensitive(true);
        headers.insert("X-ClickHouse-User", user);
        if let Ok(path) = env::var("CLICKHOUSE_PASSWORD_FILE") {
            let mut password = header::HeaderValue::from_str(std::fs::read_to_string(path)?.trim())?;
            password.set_sensitive(true);
            headers.insert("X-ClickHouse-Key", password);
        }
        let client = Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            client,
            url,
            database,
            cluster,
        })
    }

    async fn request(&self, query: &str, body: String) -> Result<(), Error> {
        let response = self
            .client
            .post(self.url.clone())
            .query(&[
                ("database", self.database.as_str()),
                ("query", query),
                ("wait_end_of_query", "1"),
                // An `ON CLUSTER` statement otherwise answers with a status row
                // per replica, which the empty-body check below would refuse.
                // `none` drops the rows but still fails if any replica fails.
                ("distributed_ddl_output_mode", "none"),
            ])
            .header(header::CONTENT_LENGTH, body.len())
            .body(body)
            .send()
            .await
            .map_err(|error| error.without_url())?;
        if !response.status().is_success() {
            return Err(format!("ClickHouse returned HTTP {}", response.status()).into());
        }
        // A successful INSERT/DDL has an empty body. Check it too: an HTTP 200
        // alone is not proof of success with ClickHouse's streaming HTTP API.
        let mut response = response;
        while let Some(chunk) = response.chunk().await.map_err(|error| error.without_url())? {
            if chunk.iter().any(|byte| !byte.is_ascii_whitespace()) {
                return Err("ClickHouse returned an unexpected INSERT/DDL response".into());
            }
        }
        Ok(())
    }

    /// One statement per request: ClickHouse's HTTP API runs one at a time.
    async fn initialize(&self) -> Result<(), Error> {
        self.request(&self.create_table::<QueryFailureEvent>(), String::new()).await?;
        self.request(&self.create_table::<UsageReport>(), String::new()).await?;
        self.request(&self.create_table::<PgUsageReport>(), String::new()).await
    }

    /// On a cluster, the table is created `ON CLUSTER` with the replicated
    /// engine, so it exists on every replica and a row inserted through any
    /// of them reaches all. The single-server statement would create a local
    /// table on whichever replica took the request, and inserts and reads
    /// landing on another replica would fail.
    fn create_table<T: Row>(&self) -> String {
        let Some(cluster) = &self.cluster else {
            return T::SCHEMA.to_owned();
        };
        let create = format!("CREATE TABLE IF NOT EXISTS {}", T::TABLE);
        T::SCHEMA
            .replacen(&create, &format!("{create} ON CLUSTER '{cluster}'"), 1)
            .replacen("ENGINE = ReplacingMergeTree", "ENGINE = ReplicatedReplacingMergeTree", 1)
    }

    async fn insert<T: Row>(&self, rows: &[T]) -> Result<(), Error> {
        let mut body = String::new();
        for row in rows {
            body.push_str(&serde_json::to_string(&row.row()?)?);
            body.push('\n');
        }
        self.request(&format!("INSERT INTO {} FORMAT JSONEachRow", T::TABLE), body).await
    }
}

async fn receive(State(state): State<Arc<Receiver>>, headers: HeaderMap, Json(event): Json<QueryFailureEvent>) -> StatusCode {
    if let Some(expected) = &state.authorization {
        let supplied = headers.get(header::AUTHORIZATION).map(|value| value.as_bytes()).unwrap_or_default();
        if !bool::from(supplied.ct_eq(expected.as_bytes())) {
            return StatusCode::UNAUTHORIZED;
        }
    }
    if event.schema_version != 1 || event.event_id.is_empty() || !matches!(event.stage.as_str(), "prepare" | "execute") {
        return StatusCode::UNPROCESSABLE_ENTITY;
    }
    state.admit(&state.failures, event)
}

/// `POST /v1/usage`: no token, by design (see [`Receiver::authorization`]).
/// A report that fails [`hydradb_usage_report::validate`] is refused before it
/// is buffered.
async fn receive_usage(State(state): State<Arc<Receiver>>, Json(report): Json<UsageReport>) -> StatusCode {
    if hydradb_usage_report::validate(&report).is_err() {
        return StatusCode::UNPROCESSABLE_ENTITY;
    }
    state.admit(&state.usage, report)
}

/// `POST /v1/pg-usage`: pg_hydradb's report, open and checked like
/// `/v1/usage`, with [`hydradb_pg_usage_report::validate`].
async fn receive_pg_usage(State(state): State<Arc<Receiver>>, Json(report): Json<PgUsageReport>) -> StatusCode {
    if hydradb_pg_usage_report::validate(&report).is_err() {
        return StatusCode::UNPROCESSABLE_ENTITY;
    }
    state.admit(&state.pg_usage, report)
}

async fn health(State(state): State<Arc<Receiver>>) -> Json<serde_json::Value> {
    let inserted = state.inserted.load(Ordering::Relaxed);
    let failures_healthy = state.failures_healthy.load(Ordering::Relaxed);
    let usage_healthy = state.usage_healthy.load(Ordering::Relaxed);
    let pg_usage_healthy = state.pg_usage_healthy.load(Ordering::Relaxed);
    let queued = |capacity: usize, max: usize| max - capacity;
    Json(json!({"queued": queued(state.failures.capacity(), state.failures.max_capacity())
            + queued(state.usage.capacity(), state.usage.max_capacity())
            + queued(state.pg_usage.capacity(), state.pg_usage.max_capacity()),
        "pending": state.accepted.load(Ordering::Relaxed).saturating_sub(inserted),
        "inserted": inserted, "clickhouse_healthy": failures_healthy && usage_healthy && pg_usage_healthy,
        "clickhouse_healthy_by_table": {"query_failures": failures_healthy, "usage_reports": usage_healthy,
            "pg_usage_reports": pg_usage_healthy}}))
}

async fn ready(State(state): State<Arc<Receiver>>) -> StatusCode {
    if state.failures_healthy.load(Ordering::Relaxed)
        && state.usage_healthy.load(Ordering::Relaxed)
        && state.pg_usage_healthy.load(Ordering::Relaxed)
        && state.failures.capacity() > 0
        && state.usage.capacity() > 0
        && state.pg_usage.capacity() > 0
    {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn flush<T: Row>(clickhouse: &ClickHouse, batch: &mut Vec<T>, healthy: &AtomicBool, inserted: &AtomicU64) {
    if batch.is_empty() {
        return;
    }
    let mut delay = Duration::from_secs(1);
    loop {
        match clickhouse.insert(batch).await {
            Ok(()) => {
                healthy.store(true, Ordering::Relaxed);
                inserted.fetch_add(batch.len() as u64, Ordering::Relaxed);
                batch.clear();
                return;
            }
            Err(error) => {
                healthy.store(false, Ordering::Relaxed);
                eprintln!(
                    "query telemetry: {} batch insert failed ({error:?}); retaining {} rows for retry",
                    T::NAME,
                    batch.len()
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(10));
            }
        }
    }
}

struct WorkerConfig {
    batch_size: usize,
    interval: Duration,
    healthy: Arc<AtomicBool>,
    inserted: Arc<AtomicU64>,
}

async fn worker<T: Row>(
    clickhouse: Arc<ClickHouse>,
    mut receiver: mpsc::Receiver<T>,
    mut stop: watch::Receiver<bool>,
    config: WorkerConfig,
) {
    let WorkerConfig {
        batch_size,
        interval,
        healthy,
        inserted,
    } = config;
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut batch = Vec::with_capacity(batch_size);
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => { receiver.close(); break; }
            _ = tick.tick() => flush(&clickhouse, &mut batch, &healthy, &inserted).await,
            row = receiver.recv() => match row {
                Some(row) => {
                    batch.push(row);
                    if batch.len() >= batch_size { flush(&clickhouse, &mut batch, &healthy, &inserted).await; }
                }
                None => break,
            }
        }
    }
    while let Some(row) = receiver.recv().await {
        batch.push(row);
        if batch.len() >= batch_size {
            flush(&clickhouse, &mut batch, &healthy, &inserted).await;
        }
    }
    flush(&clickhouse, &mut batch, &healthy, &inserted).await;
}

fn positive(name: &str, default: usize, max: usize) -> Result<usize, Error> {
    let value = env::var(name).ok().map(|v| v.parse::<usize>()).transpose()?.unwrap_or(default);
    if value == 0 || value > max {
        return Err(format!("{name} must be in 1..={max}").into());
    }
    Ok(value)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn router(state: Arc<Receiver>) -> Router {
    Router::new()
        .route("/v1/query-failures", post(receive))
        // Its own, smaller limit: a usage report is a few KB, and this route
        // takes no token.
        .route("/v1/usage", post(receive_usage).layer(DefaultBodyLimit::max(MAX_REPORT_BYTES)))
        .route(
            "/v1/pg-usage",
            post(receive_pg_usage).layer(DefaultBodyLimit::max(hydradb_pg_usage_report::MAX_REPORT_BYTES)),
        )
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .layer(DefaultBodyLimit::max(MAX_EVENT_BYTES))
        .with_state(state)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let address: SocketAddr = env::var("QUERY_TELEMETRY_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:18500".into())
        .parse()?;
    let capacity = positive("QUERY_TELEMETRY_BUFFER_EVENTS", 1024, 16384)?;
    let batch_size = positive("QUERY_TELEMETRY_BATCH_EVENTS", 100, 1024)?;
    let interval = Duration::from_millis(positive("QUERY_TELEMETRY_FLUSH_MS", 1000, 60000)? as u64);
    let token = env::var("QUERY_TELEMETRY_TOKEN_FILE")
        .ok()
        .map(std::fs::read_to_string)
        .transpose()?;
    hydradb_query_telemetry::bearer_headers(token.as_deref())?;
    let clickhouse = Arc::new(ClickHouse::from_env()?);
    clickhouse.initialize().await?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    let (failures, failure_receiver) = mpsc::channel(capacity);
    let (usage, usage_receiver) = mpsc::channel(capacity);
    let (pg_usage, pg_usage_receiver) = mpsc::channel(capacity);
    let (stop, stopping) = watch::channel(false);
    let inserted = Arc::new(AtomicU64::new(0));
    let state = Arc::new(Receiver {
        failures,
        usage,
        pg_usage,
        accepted: AtomicU64::new(0),
        authorization: token.map(|t| format!("Bearer {}", t.trim())),
        failures_healthy: Arc::new(AtomicBool::new(true)),
        usage_healthy: Arc::new(AtomicBool::new(true)),
        pg_usage_healthy: Arc::new(AtomicBool::new(true)),
        inserted: inserted.clone(),
    });
    let config = |healthy| WorkerConfig {
        batch_size,
        interval,
        healthy,
        inserted: inserted.clone(),
    };
    let mut failure_task = tokio::spawn(worker(
        clickhouse.clone(),
        failure_receiver,
        stopping.clone(),
        config(state.failures_healthy.clone()),
    ));
    let mut usage_task = tokio::spawn(worker(
        clickhouse.clone(),
        usage_receiver,
        stopping.clone(),
        config(state.usage_healthy.clone()),
    ));
    let mut pg_usage_task = tokio::spawn(worker(
        clickhouse,
        pg_usage_receiver,
        stopping,
        config(state.pg_usage_healthy.clone()),
    ));
    eprintln!("query telemetry receiver listening on {address}");
    let (shutdown, mut shutting_down) = watch::channel(false);
    let server = axum::serve(listener, router(state)).with_graceful_shutdown(async move {
        shutdown_signal().await;
        let _ = shutdown.send(true);
    });
    let server = std::future::IntoFuture::into_future(server);
    tokio::pin!(server);
    let result = tokio::select! {
        result = &mut server => Some(result),
        _ = shutting_down.changed() => None,
    };
    // One deadline covers HTTP connections and both ClickHouse workers.
    let drain = async {
        let http_result = match result {
            Some(result) => result,
            None => server.await,
        };
        let _ = stop.send(true);
        (&mut failure_task).await?;
        (&mut usage_task).await?;
        (&mut pg_usage_task).await?;
        http_result?;
        Ok::<_, Error>(())
    };
    match tokio::time::timeout(Duration::from_secs(30), drain).await {
        Ok(result) => result?,
        Err(_) => {
            failure_task.abort();
            usage_task.abort();
            pg_usage_task.abort();
            return Err("query telemetry shutdown timed out; buffered events were not persisted".into());
        }
    }
    Ok(())
}
