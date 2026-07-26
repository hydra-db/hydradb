use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures::StreamExt;
use slatedb::object_store::path::Path;
use slatedb_graph_kernel::{
    object_store_from_env, GraphCluster, GraphError, GraphId, GraphScope, NamespaceId,
    NamespacePath, ObjectStoreGraphScopeDirectory,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::field::Empty;
use tracing::Instrument;
use turbolay_telemetry::{semconv, ErrorClass, Outcome, ServiceIdentity, TelemetryConfig};

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// One indexing failure, with the context that identifies it kept structured.
///
/// The cycle used to accumulate `Vec<String>` and join it with `"; "` into a
/// single `io::Error`, which the caller logged as one `warn`. A cycle failing
/// on one cell out of eight was then indistinguishable from one failing on all
/// eight, because every element was stringified and merged precisely at the
/// moment its scope, cell and edge type became useful. The fields survive to
/// the top now, and flattening is a [`fmt::Display`] concern.
#[derive(Debug)]
struct IndexFailure {
    /// Which step of the cycle raised it. A bounded set, safe as a dimension.
    stage: &'static str,
    /// `error.class`, per `GraphError::class`.
    class: &'static str,
    scope: Option<String>,
    cell_id: Option<String>,
    edge_type: Option<String>,
    /// The human prefix the flattened string used to carry, unchanged.
    context: String,
    /// The underlying error's `Display`, when there was one.
    detail: Option<String>,
}

impl IndexFailure {
    /// A failure with no `GraphError` behind it — a configuration mismatch
    /// rather than an operation that was attempted and refused.
    fn config(stage: &'static str, context: String) -> Self {
        Self {
            stage,
            class: ErrorClass::Config.as_str(),
            scope: None,
            cell_id: None,
            edge_type: None,
            context,
            detail: None,
        }
    }

    /// A failure carrying a kernel error, classified by the kernel.
    fn kernel(stage: &'static str, context: String, error: &GraphError) -> Self {
        Self {
            stage,
            class: error.class(),
            scope: None,
            cell_id: None,
            edge_type: None,
            context,
            detail: Some(error.to_string()),
        }
    }

    fn with_scope(mut self, scope: &str) -> Self {
        self.scope = Some(scope.to_string());
        self
    }

    fn with_cell(mut self, cell_id: &str) -> Self {
        self.cell_id = Some(cell_id.to_string());
        self
    }

    fn with_edge_type(mut self, edge_type: &str) -> Self {
        self.edge_type = Some(edge_type.to_string());
        self
    }

    fn scope(&self) -> &str {
        self.scope.as_deref().unwrap_or_default()
    }

    fn cell_id(&self) -> &str {
        self.cell_id.as_deref().unwrap_or_default()
    }

    fn edge_type(&self) -> &str {
        self.edge_type.as_deref().unwrap_or_default()
    }
}

impl fmt::Display for IndexFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.detail {
            Some(detail) => write!(formatter, "{}: {detail}", self.context),
            None => formatter.write_str(&self.context),
        }
    }
}

/// Every failure a cycle produced, in the order it produced them.
///
/// `Display` reproduces the flattened `"; "`-joined string the caller has
/// always logged, so nothing downstream that reads the message loses anything;
/// the difference is that the structure is still there underneath it.
#[derive(Debug, Default)]
struct CycleFailures {
    failures: Vec<IndexFailure>,
}

impl CycleFailures {
    fn push(&mut self, failure: IndexFailure) {
        self.failures.push(failure);
    }

    fn absorb(&mut self, other: CycleFailures) {
        self.failures.extend(other.failures);
    }

    fn is_empty(&self) -> bool {
        self.failures.is_empty()
    }

    fn len(&self) -> usize {
        self.failures.len()
    }

    /// The failure a readiness transition should be attributed to. First rather
    /// than worst: the cycle stops being trustworthy at the first thing that
    /// broke, and a stable choice is what makes the event joinable.
    fn first(&self) -> Option<&IndexFailure> {
        self.failures.first()
    }

    fn into_result(self) -> Result<(), Self> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl fmt::Display for CycleFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, failure) in self.failures.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{failure}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CycleFailures {}

/// Record a failure on the innermost span that produced it.
///
/// Both halves matter. The span attributes are what a trace backend groups by;
/// the `warn` is what somebody tailing a pod sees, and it carries the same
/// fields so the flat log is exactly as specific as the trace.
fn record_failure(span: &tracing::Span, failure: &IndexFailure) {
    span.record(semconv::OUTCOME, Outcome::Failed.as_str());
    span.record(semconv::ERROR_CLASS, failure.class);
    span.in_scope(|| {
        tracing::warn!(
            stage = failure.stage,
            turbolay.scope = failure.scope(),
            turbolay.cell_id = failure.cell_id(),
            turbolay.edge_type = failure.edge_type(),
            turbolay.outcome = Outcome::Failed.as_str(),
            error.class = failure.class,
            error = %failure,
            "graph index step failed"
        );
    });
}

/// Mark a span as having succeeded.
fn record_success(span: &tracing::Span) {
    span.record(semconv::OUTCOME, Outcome::Success.as_str());
}

#[derive(Default)]
struct IndexerMetrics {
    ready: AtomicBool,
    cycles: AtomicU64,
    successful_cycles: AtomicU64,
    failed_cycles: AtomicU64,
    open_failures: AtomicU64,
    generations_published: AtomicU64,
    generation_failures: AtomicU64,
    generations_deleted: AtomicU64,
    last_success_ms: AtomicU64,
}

struct IndexerAdminServer {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<std::io::Result<()>>,
}

#[tokio::main]
async fn main() -> RuntimeResult<()> {
    // One subscriber for both binaries, so a shared log sink separates a node
    // line from an indexer line by field rather than by message text. `init` is
    // total: with `OTEL_EXPORTER_OTLP_ENDPOINT` unset it installs the fmt layer
    // alone and returns `Ok`, so a missing collector never stops the indexer
    // booting. The guard is held to the end of `main` because dropping it is
    // what flushes batched spans and logs — without that flush the last seconds
    // before a pod restart are lost, which is exactly the window that matters.
    let telemetry =
        turbolay_telemetry::init(TelemetryConfig::from_env(ServiceIdentity::GraphIndexer))?;

    let data_path = env_value("GRAPH_DATA_PATH", "graph/data");
    let root_scope = graph_scope()?;
    let cells = env_value("GRAPH_CELLS", "cell-0")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if cells.is_empty() {
        return Err("GRAPH_CELLS must contain at least one cell".into());
    }
    let interval =
        Duration::from_millis(env_value("GRAPH_INDEXER_INTERVAL_MS", "5000").parse::<u64>()?);
    if interval.is_zero() {
        return Err("GRAPH_INDEXER_INTERVAL_MS must be greater than zero".into());
    }
    let retain_previous = env_value("GRAPH_INDEXER_RETAIN_PREVIOUS", "1").parse::<usize>()?;
    let admin_addr = env_value("GRAPH_INDEXER_ADMIN_ADDR", "0.0.0.0:9091").parse::<SocketAddr>()?;

    let metrics = Arc::new(IndexerMetrics::default());
    let admin = IndexerAdminServer::bind(admin_addr, Arc::clone(&metrics)).await?;
    // An empty graph has no SlateDB manifest yet. The indexer is healthy and
    // ready to observe that namespace even though there is nothing to build.
    metrics.ready.store(true, Ordering::Release);
    let object_store = object_store_from_env(None)?;
    let scope_directory = ObjectStoreGraphScopeDirectory::new(
        data_path.clone(),
        root_scope.namespace.clone(),
        root_scope.graph_id.clone(),
        Arc::clone(&object_store),
    );
    let mut shutdown = Box::pin(shutdown_signal());
    tracing::info!(scope = %root_scope, ?cells, "graph indexer started");

    // Mirrors `metrics.ready`, which was stored `true` above. Kept alongside so
    // the readiness *transition* can be detected without re-reading an atomic
    // that another task may have moved.
    let mut ready = true;

    loop {
        metrics.cycles.fetch_add(1, Ordering::Relaxed);
        // The indexer has no client parent, so this is a trace root.
        let cycle_span = tracing::info_span!(
            "index.cycle",
            turbolay.scope = %root_scope,
            cycle = metrics.cycles.load(Ordering::Relaxed),
            failure_count = Empty,
            turbolay.outcome = Empty,
            error.class = Empty,
        );
        let outcome = run_registered_scopes_cycle(
            &data_path,
            &scope_directory,
            &cells,
            Arc::clone(&object_store),
            retain_previous,
            &metrics,
        )
        .instrument(cycle_span.clone())
        .await;

        match outcome {
            Ok(()) => {
                metrics.successful_cycles.fetch_add(1, Ordering::Relaxed);
                metrics
                    .last_success_ms
                    .store(unix_time_ms(), Ordering::Relaxed);
                metrics.ready.store(true, Ordering::Release);
                record_success(&cycle_span);
                if !ready {
                    // Only the transition, never the steady state.
                    cycle_span.in_scope(|| {
                        tracing::info!(
                            turbolay.scope = %root_scope,
                            turbolay.outcome = Outcome::Success.as_str(),
                            "graph indexer readiness regained"
                        );
                    });
                }
                ready = true;
            }
            Err(failures) => {
                metrics.failed_cycles.fetch_add(1, Ordering::Relaxed);
                metrics.ready.store(false, Ordering::Release);
                cycle_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                cycle_span.record("failure_count", failures.len());
                if let Some(first) = failures.first() {
                    cycle_span.record(semconv::ERROR_CLASS, first.class);
                }
                if ready {
                    // `metrics.ready` going false is what a Kubernetes probe
                    // acts on, so the flip gets its own event with the failing
                    // cell attached — a pod going unready then connects to the
                    // specific cell that caused it rather than to a timestamp.
                    // Recorded on transitions only; a cycle that was already
                    // failing has said this once.
                    let first = failures.first();
                    cycle_span.in_scope(|| {
                        tracing::error!(
                            turbolay.scope = first.map(IndexFailure::scope).unwrap_or_default(),
                            turbolay.cell_id = first.map(IndexFailure::cell_id).unwrap_or_default(),
                            turbolay.edge_type =
                                first.map(IndexFailure::edge_type).unwrap_or_default(),
                            error.class = first.map(|failure| failure.class).unwrap_or_default(),
                            stage = first.map(|failure| failure.stage).unwrap_or_default(),
                            failure_count = failures.len(),
                            "graph indexer readiness lost"
                        );
                    });
                }
                ready = false;
                // Each failure was already recorded on the span that produced
                // it; this line stays for continuity, and is now a summary
                // rather than the only place the detail exists.
                cycle_span.in_scope(|| {
                    tracing::warn!(
                        failure_count = failures.len(),
                        error = %failures,
                        "graph index cycle failed; retrying"
                    );
                });
            }
        }

        tokio::select! {
            result = &mut shutdown => {
                result?;
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
    metrics.ready.store(false, Ordering::Release);
    admin.stop().await?;
    tracing::info!(scope = %root_scope, "graph indexer stopped");
    telemetry.shutdown();
    Ok(())
}

async fn run_registered_scopes_cycle(
    data_path: &str,
    scope_directory: &ObjectStoreGraphScopeDirectory,
    cells: &[String],
    object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    retain_previous: usize,
    metrics: &IndexerMetrics,
) -> Result<(), CycleFailures> {
    let mut failures = CycleFailures::default();

    let discovery_span = tracing::info_span!(
        "index.scope_discovery",
        scope_count = Empty,
        turbolay.outcome = Empty,
        error.class = Empty,
    );
    let scopes = match scope_directory
        .list()
        .instrument(discovery_span.clone())
        .await
    {
        Ok(scopes) => {
            discovery_span.record("scope_count", scopes.len());
            record_success(&discovery_span);
            scopes
        }
        Err(error) => {
            let failure = IndexFailure::kernel(
                "scope_discovery",
                "list registered scopes".to_string(),
                &error,
            );
            record_failure(&discovery_span, &failure);
            failures.push(failure);
            return Err(failures);
        }
    };

    for scope in scopes {
        let scope_name = scope.to_string();
        let scope_span = tracing::info_span!(
            "index.scope",
            turbolay.scope = %scope_name,
            turbolay.outcome = Empty,
            error.class = Empty,
        );

        let has_data_span = tracing::info_span!(
            parent: &scope_span,
            "index.scope_has_data",
            turbolay.scope = %scope_name,
            has_data = Empty,
            turbolay.outcome = Empty,
            error.class = Empty,
        );
        match scope_has_data(data_path, &scope, cells, &object_store)
            .instrument(has_data_span.clone())
            .await
        {
            Ok(true) => {
                has_data_span.record("has_data", true);
                record_success(&has_data_span);
            }
            Ok(false) => {
                // An empty namespace is the healthy steady state, not an
                // absence of work to report.
                has_data_span.record("has_data", false);
                has_data_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
                scope_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
                continue;
            }
            Err(error) => {
                // This used to be a `?`, which discarded every failure already
                // accumulated on the way here. The scan still stops — no work
                // changes — but what stops is only the scan, not the record.
                let failure = IndexFailure::kernel(
                    "scope_has_data",
                    format!("scan scope {scope_name}"),
                    &error,
                )
                .with_scope(&scope_name);
                record_failure(&has_data_span, &failure);
                scope_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                failures.push(failure);
                break;
            }
        }

        let open_span = tracing::info_span!(
            parent: &scope_span,
            "index.cluster_open",
            turbolay.scope = %scope_name,
            cell_count = cells.len(),
            turbolay.outcome = Empty,
            error.class = Empty,
        );
        let cluster = match GraphCluster::open_cells_scoped(
            data_path.to_string(),
            scope.clone(),
            cells.to_vec(),
            Arc::clone(&object_store),
        )
        .instrument(open_span.clone())
        .await
        {
            Ok(cluster) => {
                record_success(&open_span);
                cluster
            }
            Err(error) => {
                metrics.open_failures.fetch_add(1, Ordering::Relaxed);
                let failure = IndexFailure::kernel(
                    "cluster_open",
                    format!("open scope {scope_name}"),
                    &error,
                )
                .with_scope(&scope_name);
                record_failure(&open_span, &failure);
                scope_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                failures.push(failure);
                continue;
            }
        };

        let mut scope_failed = false;
        // Instrumenting the call rather than parenting a span by hand is what
        // makes every `index.cell` below a child of this scope.
        if let Err(inner) = run_index_cycle(&cluster, &scope_name, cells, retain_previous, metrics)
            .instrument(scope_span.clone())
            .await
        {
            scope_failed = true;
            failures.absorb(inner);
        }

        let close_span = tracing::info_span!(
            parent: &scope_span,
            "index.cluster_close",
            turbolay.scope = %scope_name,
            turbolay.outcome = Empty,
            error.class = Empty,
        );
        if let Err(error) = cluster.close().instrument(close_span.clone()).await {
            let failure =
                IndexFailure::kernel("cluster_close", format!("close scope {scope_name}"), &error)
                    .with_scope(&scope_name);
            record_failure(&close_span, &failure);
            scope_failed = true;
            failures.push(failure);
        } else {
            record_success(&close_span);
        }

        if scope_failed {
            scope_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
        } else {
            record_success(&scope_span);
        }
    }

    failures.into_result()
}

async fn scope_has_data(
    data_path: &str,
    scope: &GraphScope,
    cells: &[String],
    object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
) -> slatedb_graph_kernel::Result<bool> {
    let scope_path = scope.scoped_store_path(data_path);
    for cell_id in cells {
        let prefix = Path::from(format!("{scope_path}/{cell_id}"));
        if object_store
            .list(Some(&prefix))
            .next()
            .await
            .transpose()?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn run_index_cycle(
    cluster: &GraphCluster,
    scope: &str,
    cells: &[String],
    retain_previous: usize,
    metrics: &IndexerMetrics,
) -> Result<(), CycleFailures> {
    let mut failures = CycleFailures::default();
    for cell_id in cells {
        let cell_span = tracing::info_span!(
            "index.cell",
            turbolay.scope = %scope,
            turbolay.cell_id = %cell_id,
            dirty_edge_types = Empty,
            turbolay.outcome = Empty,
            error.class = Empty,
        );

        let Some(shard) = cluster.shard(cell_id) else {
            let failure = IndexFailure::config(
                "missing_cell",
                format!("index scope {scope}: missing configured cell {cell_id}"),
            )
            .with_scope(scope)
            .with_cell(cell_id);
            record_failure(&cell_span, &failure);
            failures.push(failure);
            continue;
        };

        let refresh_span = tracing::info_span!(
            parent: &cell_span,
            "index.refresh_sequence",
            turbolay.scope = %scope,
            turbolay.cell_id = %cell_id,
            turbolay.base_sequence = Empty,
            turbolay.outcome = Empty,
            error.class = Empty,
        );
        match shard
            .refresh_storage_sequence(cell_id)
            .instrument(refresh_span.clone())
            .await
        {
            Ok(sequence) => {
                refresh_span.record(semconv::BASE_SEQUENCE, sequence);
                record_success(&refresh_span);
            }
            Err(error) => {
                let failure = IndexFailure::kernel(
                    "refresh_sequence",
                    format!("index scope {scope}: refresh {cell_id}"),
                    &error,
                )
                .with_scope(scope)
                .with_cell(cell_id);
                record_failure(&refresh_span, &failure);
                cell_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                failures.push(failure);
                continue;
            }
        }

        let discover_span = tracing::info_span!(
            parent: &cell_span,
            "index.discover_dirty",
            turbolay.scope = %scope,
            turbolay.cell_id = %cell_id,
            dirty_count = Empty,
            turbolay.outcome = Empty,
            error.class = Empty,
        );
        let dirty = match shard
            .dirty_graph_index_edge_types(cell_id)
            .instrument(discover_span.clone())
            .await
        {
            Ok(dirty) => {
                discover_span.record("dirty_count", dirty.len());
                record_success(&discover_span);
                dirty
            }
            Err(error) => {
                let failure = IndexFailure::kernel(
                    "discover_dirty",
                    format!("index scope {scope}: discover dirty edge types for {cell_id}"),
                    &error,
                )
                .with_scope(scope)
                .with_cell(cell_id);
                record_failure(&discover_span, &failure);
                cell_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                failures.push(failure);
                continue;
            }
        };
        cell_span.record("dirty_edge_types", dirty.len());

        let mut cell_failed = false;
        let mut cell_built = false;
        for (edge_type, dirty_sequence) in dirty {
            let edge_span = tracing::info_span!(
                parent: &cell_span,
                "index.edge_type",
                turbolay.scope = %scope,
                turbolay.cell_id = %cell_id,
                turbolay.edge_type = %edge_type,
                dirty_sequence,
                turbolay.generation = Empty,
                turbolay.base_sequence = Empty,
                turbolay.outcome = Empty,
                error.class = Empty,
            );

            let read_span = tracing::info_span!(
                parent: &edge_span,
                "index.read_current",
                turbolay.scope = %scope,
                turbolay.cell_id = %cell_id,
                turbolay.edge_type = %edge_type,
                present = Empty,
                turbolay.generation = Empty,
                turbolay.base_sequence = Empty,
                turbolay.outcome = Empty,
                error.class = Empty,
            );
            let current = match shard
                .current_graph_index(cell_id, &edge_type)
                .instrument(read_span.clone())
                .await
            {
                Ok(current) => {
                    read_span.record("present", current.is_some());
                    if let Some(generation) = current.as_ref() {
                        read_span.record(semconv::GENERATION, generation.generation.as_str());
                        read_span.record(semconv::BASE_SEQUENCE, generation.base_sequence);
                    }
                    record_success(&read_span);
                    current
                }
                Err(error) => {
                    let failure = IndexFailure::kernel(
                        "read_current",
                        format!("index scope {scope}: read index {cell_id}/{edge_type}"),
                        &error,
                    )
                    .with_scope(scope)
                    .with_cell(cell_id)
                    .with_edge_type(&edge_type);
                    record_failure(&read_span, &failure);
                    edge_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                    cell_failed = true;
                    failures.push(failure);
                    continue;
                }
            };

            if let Some(generation) = current
                .as_ref()
                .filter(|generation| generation.base_sequence >= dirty_sequence)
            {
                // The normal case, and until now completely invisible: an idle
                // but healthy indexer and a stopped one produced identical
                // output, which is nothing. An explicit `skipped` outcome is
                // what distinguishes them, and that distinction is most of what
                // an indexer has to report.
                edge_span.record(semconv::GENERATION, generation.generation.as_str());
                edge_span.record(semconv::BASE_SEQUENCE, generation.base_sequence);
                edge_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
                edge_span.in_scope(|| {
                    tracing::debug!(
                        turbolay.scope = %scope,
                        turbolay.cell_id = %cell_id,
                        turbolay.edge_type = %edge_type,
                        turbolay.generation = %generation.generation,
                        turbolay.base_sequence = generation.base_sequence,
                        turbolay.outcome = Outcome::Skipped.as_str(),
                        dirty_sequence,
                        "graph index generation already current"
                    );
                });
                continue;
            }

            let build_span = tracing::info_span!(
                parent: &edge_span,
                "artifact.build",
                turbolay.scope = %scope,
                turbolay.cell_id = %cell_id,
                turbolay.edge_type = %edge_type,
                turbolay.generation = Empty,
                turbolay.base_sequence = Empty,
                edge_count = Empty,
                turbolay.outcome = Empty,
                error.class = Empty,
            );
            let generation = match shard
                .build_graph_index(cell_id, &edge_type)
                .instrument(build_span.clone())
                .await
            {
                Ok(generation) => {
                    build_span.record(semconv::GENERATION, generation.generation.as_str());
                    build_span.record(semconv::BASE_SEQUENCE, generation.base_sequence);
                    build_span.record("edge_count", generation.edge_count);
                    record_success(&build_span);
                    generation
                }
                Err(error) => {
                    metrics.generation_failures.fetch_add(1, Ordering::Relaxed);
                    let failure = IndexFailure::kernel(
                        "artifact_build",
                        format!("index scope {scope}: build index {cell_id}/{edge_type}"),
                        &error,
                    )
                    .with_scope(scope)
                    .with_cell(cell_id)
                    .with_edge_type(&edge_type);
                    record_failure(&build_span, &failure);
                    edge_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                    cell_failed = true;
                    failures.push(failure);
                    continue;
                }
            };

            metrics
                .generations_published
                .fetch_add(1, Ordering::Relaxed);
            edge_span.record(semconv::GENERATION, generation.generation.as_str());
            edge_span.record(semconv::BASE_SEQUENCE, generation.base_sequence);

            // The CAS pointer swap happens inside `build_graph_index`, which
            // this binary cannot wrap from here, so this span reports the swap's
            // *outcome* rather than timing it — the publish latency is inside
            // `artifact.build` above. `build_graph_index` returns the manifest
            // that ended up current, so a returned generation identical to the
            // one `index.read_current` saw means the pointer never moved and
            // some other writer's generation is ahead of ours.
            let pointer_advanced = current
                .as_ref()
                .is_none_or(|previous| previous.generation != generation.generation);
            let publish_outcome = if pointer_advanced {
                Outcome::Success
            } else {
                Outcome::Skipped
            };
            let publish_span = tracing::info_span!(
                parent: &edge_span,
                "artifact.publish",
                turbolay.scope = %scope,
                turbolay.cell_id = %cell_id,
                turbolay.edge_type = %edge_type,
                turbolay.generation = %generation.generation,
                turbolay.base_sequence = generation.base_sequence,
                content_hash = %generation.generation,
                checksum = generation.checksum,
                edge_count = generation.edge_count,
                last_wal_id = generation.last_wal_id,
                pointer_advanced,
                turbolay.outcome = publish_outcome.as_str(),
            );
            publish_span.in_scope(|| {
                tracing::info!(
                    turbolay.scope = %scope,
                    turbolay.cell_id = %cell_id,
                    turbolay.edge_type = %edge_type,
                    turbolay.generation = %generation.generation,
                    turbolay.base_sequence = generation.base_sequence,
                    turbolay.outcome = publish_outcome.as_str(),
                    edge_count = generation.edge_count,
                    pointer_advanced,
                    "graph index generation published"
                );
            });
            drop(publish_span);

            // BFG-014 is an unfenced GC: this span on a cell whose writer epoch
            // moved underneath it is what that failure mode looks like, which is
            // why the delete count is an attribute rather than a counter.
            let gc_span = tracing::info_span!(
                parent: &edge_span,
                "artifact.gc",
                turbolay.scope = %scope,
                turbolay.cell_id = %cell_id,
                turbolay.edge_type = %edge_type,
                turbolay.generation = %generation.generation,
                turbolay.base_sequence = generation.base_sequence,
                retain_previous,
                deleted = Empty,
                turbolay.outcome = Empty,
                error.class = Empty,
            );
            match shard
                .gc_graph_index_generations(cell_id, &edge_type, retain_previous)
                .instrument(gc_span.clone())
                .await
            {
                Ok(deleted) => {
                    gc_span.record("deleted", deleted);
                    gc_span.record(
                        semconv::OUTCOME,
                        if deleted == 0 {
                            Outcome::Skipped.as_str()
                        } else {
                            Outcome::Success.as_str()
                        },
                    );
                    metrics
                        .generations_deleted
                        .fetch_add(deleted, Ordering::Relaxed);
                }
                Err(error) => {
                    // Unchanged in effect: cleanup that fails has never failed
                    // the cycle, because the generation it was tidying up after
                    // is already published and readable.
                    let failure = IndexFailure::kernel(
                        "artifact_gc",
                        format!("index scope {scope}: cleanup {cell_id}/{edge_type}"),
                        &error,
                    )
                    .with_scope(scope)
                    .with_cell(cell_id)
                    .with_edge_type(&edge_type);
                    record_failure(&gc_span, &failure);
                }
            }

            record_success(&edge_span);
            cell_built = true;
        }

        if cell_failed {
            cell_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
        } else if cell_built {
            record_success(&cell_span);
        } else {
            cell_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
        }
    }

    failures.into_result()
}

impl IndexerAdminServer {
    async fn bind(addr: SocketAddr, metrics: Arc<IndexerMetrics>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let router = Router::new()
            .route("/livez", get(|| async { StatusCode::OK }))
            .route("/readyz", get(indexer_readiness))
            .route("/metrics", get(indexer_metrics))
            .with_state(metrics);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    while stop_rx.changed().await.is_ok() {
                        if *stop_rx.borrow() {
                            return;
                        }
                    }
                })
                .await
        });
        Ok(Self { stop_tx, task })
    }

    async fn stop(self) -> RuntimeResult<()> {
        let _ = self.stop_tx.send(true);
        self.task.await??;
        Ok(())
    }
}

async fn indexer_readiness(State(metrics): State<Arc<IndexerMetrics>>) -> StatusCode {
    if metrics.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn indexer_metrics(State(metrics): State<Arc<IndexerMetrics>>) -> Response {
    let output = format!(
        concat!(
            "# TYPE graph_indexer_ready gauge\n",
            "graph_indexer_ready {}\n",
            "# TYPE graph_indexer_cycles counter\n",
            "graph_indexer_cycles {}\n",
            "# TYPE graph_indexer_successful_cycles counter\n",
            "graph_indexer_successful_cycles {}\n",
            "# TYPE graph_indexer_failed_cycles counter\n",
            "graph_indexer_failed_cycles {}\n",
            "# TYPE graph_indexer_open_failures counter\n",
            "graph_indexer_open_failures {}\n",
            "# TYPE graph_indexer_generations_published counter\n",
            "graph_indexer_generations_published {}\n",
            "# TYPE graph_indexer_generation_failures counter\n",
            "graph_indexer_generation_failures {}\n",
            "# TYPE graph_indexer_generations_deleted counter\n",
            "graph_indexer_generations_deleted {}\n",
            "# TYPE graph_indexer_last_success_ms gauge\n",
            "graph_indexer_last_success_ms {}\n",
        ),
        u8::from(metrics.ready.load(Ordering::Acquire)),
        metrics.cycles.load(Ordering::Relaxed),
        metrics.successful_cycles.load(Ordering::Relaxed),
        metrics.failed_cycles.load(Ordering::Relaxed),
        metrics.open_failures.load(Ordering::Relaxed),
        metrics.generations_published.load(Ordering::Relaxed),
        metrics.generation_failures.load(Ordering::Relaxed),
        metrics.generations_deleted.load(Ordering::Relaxed),
        metrics.last_success_ms.load(Ordering::Relaxed),
    );
    (
        [
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        output,
    )
        .into_response()
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn graph_scope() -> RuntimeResult<GraphScope> {
    let namespace = NamespacePath::new(
        env_value("GRAPH_NAMESPACE", "default")
            .split('/')
            .map(|segment| NamespaceId::new(segment.to_string()))
            .collect::<slatedb_graph_kernel::Result<Vec<_>>>()?,
    )?;
    Ok(GraphScope::new(
        namespace,
        GraphId::new(env_value("GRAPH_ID", "default"))?,
    ))
}

fn env_value(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn shutdown_signal() -> RuntimeResult<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;
    use slatedb_graph_kernel::EdgeMutation;

    use super::*;

    #[tokio::test]
    async fn indexer_discovers_registered_scopes_and_ignores_empty_ones() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let scope = GraphScope::new(
            root.child(NamespaceId::new("dGVuYW50LWE").unwrap())
                .unwrap()
                .child(NamespaceId::new("Y29sbGVjdGlvbi1h").unwrap())
                .unwrap(),
            graph_id.clone(),
        );
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            root,
            graph_id,
            Arc::clone(&object_store),
        );
        directory.register(&scope).await.unwrap();
        let metrics = IndexerMetrics::default();

        run_registered_scopes_cycle(
            "graph/data",
            &directory,
            &["cell-0".to_string()],
            Arc::clone(&object_store),
            1,
            &metrics,
        )
        .await
        .unwrap();
        assert_eq!(metrics.open_failures.load(Ordering::Relaxed), 0);

        let writer = GraphCluster::open_cells_standalone_writers_scoped(
            "graph/data",
            scope.clone(),
            ["cell-0"],
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        writer
            .shard("cell-0")
            .unwrap()
            .write_edge(EdgeMutation {
                cell_id: "cell-0".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "indexer-scope-write".to_string(),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        run_registered_scopes_cycle(
            "graph/data",
            &directory,
            &["cell-0".to_string()],
            Arc::clone(&object_store),
            1,
            &metrics,
        )
        .await
        .unwrap();
        assert_eq!(metrics.generations_published.load(Ordering::Relaxed), 1);

        let reader = GraphCluster::open_cells_scoped("graph/data", scope, ["cell-0"], object_store)
            .await
            .unwrap();
        assert!(reader
            .shard("cell-0")
            .unwrap()
            .current_graph_index("cell-0", "FOLLOWS")
            .await
            .unwrap()
            .is_some());
        reader.close().await.unwrap();
    }
}
