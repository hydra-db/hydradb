//! The anonymous usage report (ENG-3474): GraphNode's half.
//!
//! `hydradb_usage_report` holds what the sender and receiver agree on; this
//! module gathers the node's inputs and sends.
//!
//! **Off the query path.** Every number here is a counter the node already
//! keeps for `/metrics` and OTel. A background task copies them on an interval,
//! subtracts what it last sent, and POSTs one small JSON report. Queries never
//! wait on it; at most one report is in flight and nothing is queued. The only
//! per-query cost this adds is in `ClientQueryService`: one relaxed load to
//! see that the first success and first failure are already recorded.
//!
//! **Nothing customer-named leaves the node.** Scope and cell names key the
//! per-shard totals in memory, so a closed scope can be differenced correctly,
//! and are dropped when operator totals are summed by operator name. Only
//! counter, class, reason, engine and operator names — vocabularies Turbolay
//! defines — reach the report.
//!
//! **Totals are only marked sent once the receiver accepts them.** A report
//! that fails is dropped, but the next one is measured from the last report
//! that got through, so its counts are not lost unless the process exits
//! first. A clean shutdown sends what is left, within a short budget.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hydradb::{ClientQueryMetricsSnapshot, ClientQueryService, PlacementView, ScopedGraphShardRuntimeMetrics, ScopedRoutedGraphCluster};
use hydradb_usage_report::delta::map_delta;
use hydradb_usage_report::report::{Build, Cluster, FailureCount, FirstQuery, Histogram, ReportReason};
use hydradb_usage_report::{install_id, is_install_id, UsageReport, MAX_REPORT_BYTES, SCHEMA_VERSION};
use slatedb::object_store::{path::Path, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions};

/// The scalar query counters a report carries. An allowlist, so a counter added
/// to the snapshot for `/metrics` does not reach the report unreviewed.
const QUERY_COUNTERS: &[&str] = &[
    "queries_started",
    "queries_completed",
    "queries_failed",
    "rows_returned",
    "cancellations",
    "auth_failures",
    "scope_denials",
    "backpressure_waits",
    "prepare_requests",
];

/// The first report goes out this long after start, or after one interval if
/// that is shorter, so a new install shows up the same day.
const FIRST_REPORT_DELAY: Duration = Duration::from_secs(10 * 60);
/// Per attempt; a periodic report makes up to [`ATTEMPTS`].
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const ATTEMPTS: u32 = 3;
/// Bound the entire install ID read/create sequence, including store retries.
const INSTALL_ID_TIMEOUT: Duration = Duration::from_secs(30);
/// The most a clean shutdown waits for its final report.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(2);
/// The most a report waits for the shards' operator totals. They sit behind
/// the same locks the OTel collector bounds its wait on; past this, the
/// report goes without them and the next one covers both periods.
const SHARD_BUDGET: Duration = Duration::from_secs(10);

async fn get_or_create_install_id(store: &Arc<dyn ObjectStore>, data_path: &str) -> Option<String> {
    let path = Path::from(data_path).join("_usage_report").join("v1").join("install_id");
    match read_install_id(store, &path).await {
        Ok(id) => return id,
        Err(Error::NotFound { .. }) => {}
        Err(error) => {
            tracing::info!(%error, "usage reporting disabled: cannot read install id");
            return None;
        }
    }
    let mut bytes = [0; 16];
    if let Err(error) = getrandom::fill(&mut bytes) {
        tracing::info!(%error, "usage reporting disabled: cannot generate install id");
        return None;
    }
    let id = install_id(bytes);
    let options = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match store.put_opts(&path, id.clone().into(), options).await {
        Ok(_) => Some(id),
        Err(Error::AlreadyExists { .. }) => match read_install_id(store, &path).await {
            Ok(id) => id,
            Err(error) => {
                tracing::info!(%error, "usage reporting disabled: cannot read install id");
                None
            }
        },
        Err(error) => {
            tracing::info!(%error, "usage reporting disabled: cannot create install id");
            None
        }
    }
}

async fn read_install_id(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<Option<String>, Error> {
    let bytes = store.get(path).await?.bytes().await?;
    match std::str::from_utf8(&bytes) {
        Ok(id) if is_install_id(id) => Ok(Some(id.to_owned())),
        _ => {
            tracing::warn!(%path, "usage reporting disabled: invalid stored install id; leaving it unchanged");
            Ok(None)
        }
    }
}

/// Everything the reporter reads, handed over once at startup.
pub struct ReporterInputs {
    pub url: reqwest::Url,
    pub interval: Duration,
    pub store: Arc<dyn ObjectStore>,
    pub data_path: String,
    pub process_started_unix_ms: u64,
    pub service: ClientQueryService,
    pub node: Arc<ScopedRoutedGraphCluster>,
    pub placement: PlacementView,
    pub cells: usize,
}

/// The running reporter. [`Self::stop`] sends what is left and ends it.
pub struct UsageReporter {
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl UsageReporter {
    pub fn start(inputs: ReporterInputs) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(SEND_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        tracing::info!(
            url = %inputs.url,
            interval_s = inputs.interval.as_secs(),
            "anonymous usage reporting is on; set GRAPH_USAGE_REPORT_ENABLED=false to turn it off"
        );
        let (stop, stopping) = tokio::sync::watch::channel(false);
        let provider = std::env::var("CLOUD_PROVIDER").unwrap_or_default().trim().to_ascii_lowercase();
        let task = tokio::spawn(run(inputs, client, stopping, provider));
        Ok(Self { stop, task })
    }

    /// Send the final report and end the task. Bounded by
    /// [`SHUTDOWN_BUDGET`] plus a small scheduling margin, and never fails.
    /// Both metrics collection and the final POST share that budget.
    ///
    /// The task is gone when this returns, aborted if need be: it holds the
    /// node and the query service, and the node cannot close while it does.
    pub async fn stop(self) {
        let _ = self.stop.send(true);
        let mut task = self.task;
        if tokio::time::timeout(SHUTDOWN_BUDGET + Duration::from_millis(250), &mut task)
            .await
            .is_err()
        {
            tracing::debug!("final usage report did not finish in time");
            task.abort();
            let _ = task.await;
        }
    }
}

async fn run(inputs: ReporterInputs, client: reqwest::Client, mut stopping: tokio::sync::watch::Receiver<bool>, provider: String) {
    let install_id = tokio::select! {
        biased;
        _ = stopping.changed() => return,
        result = tokio::time::timeout(INSTALL_ID_TIMEOUT, async {
            if provider.is_empty() || provider == "memory" {
                tracing::info!("usage reporting disabled: the in-memory store has no lasting install id");
                None
            } else {
                get_or_create_install_id(&inputs.store, &inputs.data_path).await
            }
        }) => {
            match result {
                Ok(Some(id)) => id,
                Ok(None) => return,
                Err(_) => {
                    tracing::info!("usage reporting disabled: the install id was not resolved within 30 seconds");
                    return;
                }
            }
        }
    };
    tracing::info!(%install_id, "install id");
    let mut reporter = Reporter::new(&inputs, install_id);
    let mut delivery_failed = false;
    let mut wait = FIRST_REPORT_DELAY.min(inputs.interval);
    loop {
        tokio::select! {
            biased;
            _ = stopping.changed() => break,
            _ = tokio::time::sleep(wait) => {}
        }
        tokio::select! {
            biased;
            _ = stopping.changed() => break,
            _ = async {
                let report = reporter.build(&inputs, ReportReason::Periodic).await;
                if send(&client, &inputs.url, &report, ATTEMPTS).await {
                    reporter.mark_sent(report);
                } else {
                    log_delivery_failure(&mut delivery_failed);
                }
            } => {}
        }
        wait = jittered(inputs.interval);
    }
    let sent = tokio::time::timeout(SHUTDOWN_BUDGET, async {
        let report = reporter.build(&inputs, ReportReason::Shutdown).await;
        send(&client, &inputs.url, &report, 1).await
    })
    .await;
    if !matches!(sent, Ok(true)) {
        log_delivery_failure(&mut delivery_failed);
        tracing::debug!("final usage report was not sent");
    }
}

fn log_delivery_failure(delivery_failed: &mut bool) {
    if std::mem::replace(delivery_failed, true) {
        tracing::debug!("usage report was not delivered; its counts carry into the next report");
    } else {
        tracing::info!("usage report was not delivered; its counts carry into the next report");
    }
}

/// `interval` give or take a tenth, so a fleet started together does not
/// report together.
fn jittered(interval: Duration) -> Duration {
    let unit = (ulid::Ulid::new().random() % 2_001) as f64 / 1_000.0 - 1.0;
    interval.mul_f64(1.0 + unit / 10.0)
}

fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}

/// One POST, retried on a network error, `429` or `5xx`. `true` once the
/// receiver accepts it.
async fn send(client: &reqwest::Client, url: &reqwest::Url, report: &UsageReport, attempts: u32) -> bool {
    let body = match serde_json::to_vec(report) {
        Ok(body) if body.len() <= MAX_REPORT_BYTES => body,
        Ok(body) => {
            tracing::warn!(bytes = body.len(), "usage report is larger than the receiver accepts; not sent");
            return false;
        }
        Err(error) => {
            tracing::warn!(%error, "usage report could not be serialized");
            return false;
        }
    };
    let mut delay = Duration::from_secs(1);
    for attempt in 1..=attempts {
        let response = client
            .post(url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.clone())
            .send()
            .await;
        let retry = match response {
            Ok(response) if response.status().is_success() => return true,
            Ok(response) => {
                let status = response.status();
                tracing::debug!(%status, attempt, "usage report was rejected");
                status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
            }
            Err(error) => {
                tracing::debug!(error = %error.without_url(), attempt, "usage report could not be sent");
                true
            }
        };
        if !retry || attempt == attempts {
            break;
        }
        tokio::time::sleep(delay).await;
        delay *= 2;
    }
    false
}

/// Per-scope operator totals, keyed `(scope, cell, operator, counter)`. The
/// scope and cell keep a closed scope from distorting the sum (see
/// `hydradb_usage_report::delta`); they never leave this process.
type OperatorTotals = BTreeMap<(String, String, &'static str, &'static str), u64>;

/// The running totals as of one read.
#[derive(Clone, Default)]
struct Totals {
    queries: BTreeMap<String, u64>,
    classes: BTreeMap<String, u64>,
    reasons: BTreeMap<(String, String, String), u64>,
    latency: BTreeMap<String, Histogram>,
    /// `None` when the shards did not answer within [`SHARD_BUDGET`].
    operators: Option<OperatorTotals>,
}

impl Totals {
    fn read(query: &ClientQueryMetricsSnapshot, shards: Option<&[ScopedGraphShardRuntimeMetrics]>) -> Self {
        let queries = query
            .counter_fields()
            .filter(|(name, _)| QUERY_COUNTERS.contains(name))
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        let classes = query
            .class_counter_fields()
            .filter(|(field, _, _)| *field == "queries_failed_by_class")
            .map(|(_, class, count)| (class.to_owned(), count))
            .collect();
        let reasons = query
            .failure_counter_fields()
            .filter_map(|(field, stage, reason, count)| {
                let engine = field.strip_prefix("queries_failed_by_reason_")?;
                Some(((engine.to_owned(), stage.to_owned(), reason.to_owned()), count))
            })
            .collect();
        let latency = query
            .histogram_fields()
            .filter(|(name, _)| name.starts_with("read_latency_") || name.starts_with("write_latency_"))
            .map(|(name, histogram)| {
                (
                    name.to_owned(),
                    Histogram {
                        bucket_counts: histogram.bucket_counts.to_vec(),
                        sum_us: histogram.sum_us,
                    },
                )
            })
            .collect();
        let operators = shards.map(|shards| {
            let mut totals = OperatorTotals::new();
            for shard in shards {
                for operator in &shard.shard.operational.experimental_operators {
                    for (counter, value) in operator.counter_fields() {
                        let key = (shard.scope.to_string(), shard.shard.cell_id.clone(), operator.operator, counter);
                        *totals.entry(key).or_default() += value;
                    }
                }
            }
            totals
        });
        Self {
            queries,
            classes,
            reasons,
            latency,
            operators,
        }
    }
}

/// What the reporter remembers between reports.
struct Reporter {
    install_id: String,
    run_id: String,
    /// Kept until a report is delivered. A report re-sent after a lost reply
    /// then replaces the copy the receiver got, rather than counting twice.
    report_id: String,
    process_started_unix_ms: u64,
    build: Build,
    /// The totals the last delivered report was measured against.
    sent: Totals,
    sent_at_unix_ms: u64,
    /// The `sent_at_unix_ms` of the last report built, delivered or not. The
    /// next one is stamped after it, because the receiver keeps the copy of a
    /// `report_id` with the later stamp, and the clock can step back.
    stamped_unix_ms: u64,
    /// What the report being built was measured against, kept until it is
    /// known whether it was delivered.
    pending: Option<Totals>,
}

impl Reporter {
    fn new(inputs: &ReporterInputs, install_id: String) -> Self {
        let started = unix_ms(SystemTime::now()).max(inputs.process_started_unix_ms);
        Self {
            install_id,
            run_id: ulid::Ulid::new().to_string(),
            report_id: ulid::Ulid::new().to_string(),
            process_started_unix_ms: inputs.process_started_unix_ms,
            build: Build {
                version: hydradb_telemetry::build_info::BUILD_INFO.version_string(),
                os: std::env::consts::OS.to_owned(),
                arch: std::env::consts::ARCH.to_owned(),
                object_store: std::env::var("CLOUD_PROVIDER")
                    .map(|provider| provider.trim().to_ascii_lowercase())
                    .unwrap_or_default(),
                cypher_engine: inputs.service.cypher_engine().as_str().to_owned(),
            },
            sent: Totals {
                operators: Some(OperatorTotals::new()),
                ..Totals::default()
            },
            sent_at_unix_ms: started,
            stamped_unix_ms: started,
            pending: None,
        }
    }

    async fn build(&mut self, inputs: &ReporterInputs, reason: ReportReason) -> UsageReport {
        let shards = tokio::time::timeout(SHARD_BUDGET, inputs.node.local_shard_runtime_metrics())
            .await
            .ok();
        if shards.is_none() {
            tracing::debug!("shard metrics exceeded their budget; this usage report goes without operator totals");
        }
        let current = Totals::read(&inputs.service.metrics(), shards.as_deref());
        let report = self.report(&current, inputs, reason);
        self.pending = Some(current);
        self.stamped_unix_ms = report.sent_at_unix_ms;
        report
    }

    /// The report for `current`, measured against what was last delivered.
    fn report(&self, current: &Totals, inputs: &ReporterInputs, reason: ReportReason) -> UsageReport {
        let sent_at_unix_ms = unix_ms(SystemTime::now()).max(self.stamped_unix_ms + 1);
        let reasons = map_delta(&current.reasons, &self.sent.reasons);
        let first = inputs.service.first_query_outcomes();
        let after_start = |at: Option<u64>| at.map(|at| at.saturating_sub(self.process_started_unix_ms));
        UsageReport {
            schema_version: SCHEMA_VERSION,
            report_id: self.report_id.clone(),
            install_id: self.install_id.clone(),
            run_id: self.run_id.clone(),
            reason,
            sent_at_unix_ms,
            period_start_unix_ms: self.sent_at_unix_ms,
            process_started_unix_ms: self.process_started_unix_ms,
            build: self.build.clone(),
            cluster: Cluster {
                live_nodes: u32::try_from(inputs.placement.live_nodes().len()).unwrap_or(u32::MAX),
                cells: u32::try_from(inputs.cells).unwrap_or(u32::MAX),
            },
            queries: map_delta(&current.queries, &self.sent.queries),
            failures_by_class: map_delta(&current.classes, &self.sent.classes),
            failures_by_reason: reasons
                .into_iter()
                .filter(|(_, count)| *count > 0)
                .map(|((engine, stage, reason), count)| FailureCount {
                    engine,
                    stage,
                    reason,
                    count,
                })
                .collect(),
            latency_bucket_bounds_us: hydradb::DURATION_BUCKET_BOUNDS_US.to_vec(),
            latency: current
                .latency
                .iter()
                // A histogram with no previous read differs in length from
                // the empty default, which `delta` treats as a reset: in full.
                .map(|(name, histogram)| {
                    (
                        name.clone(),
                        histogram.delta(&self.sent.latency.get(name).cloned().unwrap_or_default()),
                    )
                })
                .filter(|(_, histogram)| !histogram.is_empty())
                .collect(),
            operators: operator_report(current.operators.as_ref(), self.sent.operators.as_ref()),
            first_query: FirstQuery {
                completed_after_start_ms: after_start(first.completed_unix_ms),
                failed_after_start_ms: after_start(first.failed_unix_ms),
            },
        }
    }

    /// `report` was delivered: measure the next one from it.
    fn mark_sent(&mut self, report: UsageReport) {
        if let Some(mut current) = self.pending.take() {
            // Shards that did not answer this time: keep measuring operators
            // from the last totals that were read, so the next report covers
            // both periods.
            if current.operators.is_none() {
                current.operators = self.sent.operators.take();
            }
            self.sent = current;
            self.sent_at_unix_ms = report.sent_at_unix_ms;
            self.report_id = ulid::Ulid::new().to_string();
        }
    }
}

/// Operator totals since the last report, summed over scopes and cells by
/// operator name. Empty when the shards did not answer this time.
fn operator_report(current: Option<&OperatorTotals>, sent: Option<&OperatorTotals>) -> BTreeMap<String, BTreeMap<String, u64>> {
    let mut report: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let Some(current) = current else {
        return report;
    };
    let empty = OperatorTotals::new();
    for ((_, _, operator, counter), value) in map_delta(current, sent.unwrap_or(&empty)) {
        *report
            .entry(operator.to_owned())
            .or_default()
            .entry(counter.to_owned())
            .or_default() += value;
    }
    report.retain(|_, counters| counters.get("requests").copied().unwrap_or(0) > 0);
    report
}
