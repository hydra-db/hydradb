use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use futures::StreamExt;
use hydradb_telemetry::{semconv, ErrorClass, Outcome, ServiceIdentity, TelemetryConfig};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStoreExt, PutMode, UpdateVersion};
use slatedb_graph_kernel::{
    object_store_from_env, GraphCluster, GraphError, GraphId, GraphIndexBuildPath,
    GraphIndexGeneration, GraphLimits, GraphOpenOptions, GraphScope, NamespaceId, NamespacePath,
    ObjectStoreGraphScopeDirectory,
};
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tracing::field::Empty;
use tracing::Instrument;

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_SCOPE_CONCURRENCY: usize = 8;
const MAX_SCOPE_CONCURRENCY: usize = 64;
const DEFAULT_SCOPES_PER_CYCLE: usize = 256;
const MAX_SCOPES_PER_CYCLE: usize = 16_384;
const DEFAULT_MAX_OPEN_SCOPES: usize = 128;
const MAX_OPEN_SCOPES: usize = 4_096;
const DEFAULT_READINESS_FAILURE_THRESHOLD: u64 = 3;
const MAX_READINESS_FAILURE_THRESHOLD: u64 = 100;
const HOT_SCOPE_BUDGET_DIVISOR: usize = 4;
const HOT_SCOPE_IDLE_RECHECKS: u8 = 2;
const MAX_SCOPE_CURSOR_BYTES: usize = 4_096;

struct ScopeCursor {
    last_scope: Option<String>,
    update_version: Option<UpdateVersion>,
}

enum ScopeCursorAdvance {
    Advanced(ScopeCursor),
    LostRace,
}

#[derive(Debug, Default)]
struct RegisteredScopesCycle {
    full_sweep_completed: bool,
}

#[derive(Default)]
struct RegisteredScopeSchedule {
    scopes: Vec<GraphScope>,
    scope_names: Vec<String>,
    cursor: Option<ScopeCursor>,
}

struct CachedScopeCluster {
    cluster: Arc<GraphCluster>,
    last_used: u64,
    hot: bool,
    idle_rechecks: u8,
}

/// Long-lived read-only scope handles for the indexer.
///
/// Opening a SlateDB reader reconstructs its durable WAL view. Closing every
/// scope after every five-second sweep paid that reconstruction cost over and
/// over, even when nothing changed. This cache keeps the reader and its parsed
/// WAL state alive, while an LRU bound prevents the indexer from retaining an
/// unbounded number of tenant scopes.
struct IndexerScopeCache {
    data_path: String,
    cells: Vec<String>,
    object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    open_options: GraphOpenOptions,
    max_open_scopes: usize,
    access_clock: AtomicU64,
    clusters: AsyncMutex<BTreeMap<GraphScope, CachedScopeCluster>>,
    metrics: Arc<IndexerMetrics>,
}

impl IndexerScopeCache {
    fn new(
        data_path: String,
        cells: Vec<String>,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        open_options: GraphOpenOptions,
        max_open_scopes: usize,
        metrics: Arc<IndexerMetrics>,
    ) -> Self {
        Self {
            data_path,
            cells,
            object_store,
            open_options,
            max_open_scopes,
            access_clock: AtomicU64::new(0),
            clusters: AsyncMutex::new(BTreeMap::new()),
            metrics,
        }
    }

    async fn cluster_for_scope(
        &self,
        scope: &GraphScope,
    ) -> slatedb_graph_kernel::Result<(Arc<GraphCluster>, bool)> {
        let access = self.access_clock.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut clusters = self.clusters.lock().await;
            if let Some(entry) = clusters.get_mut(scope) {
                entry.last_used = access;
                self.metrics
                    .scope_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok((Arc::clone(&entry.cluster), true));
            }
        }

        self.metrics
            .scope_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        let opened = Arc::new(
            GraphCluster::open_cells_scoped_with_options(
                self.data_path.clone(),
                scope.clone(),
                self.cells.clone(),
                Arc::clone(&self.object_store),
                self.open_options.clone(),
            )
            .await?,
        );

        let mut evicted = None;
        let mut admission_error = None;
        let selected = {
            let mut clusters = self.clusters.lock().await;
            if let Some(entry) = clusters.get_mut(scope) {
                entry.last_used = access;
                Some(Arc::clone(&entry.cluster))
            } else {
                if clusters.len() >= self.max_open_scopes {
                    let candidate = clusters
                        .iter()
                        .filter(|(_, entry)| Arc::strong_count(&entry.cluster) == 1)
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(scope, _)| scope.clone());
                    if let Some(candidate) = candidate {
                        evicted = clusters.remove(&candidate).map(|entry| entry.cluster);
                        self.metrics
                            .scope_cache_evictions
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        admission_error = Some(GraphError::AdmissionRejected {
                            operation: "indexer_open_scopes",
                            actual: clusters.len().saturating_add(1) as u64,
                            limit: self.max_open_scopes as u64,
                        });
                    }
                }
                if admission_error.is_some() {
                    None
                } else {
                    clusters.insert(
                        scope.clone(),
                        CachedScopeCluster {
                            cluster: Arc::clone(&opened),
                            last_used: access,
                            hot: false,
                            idle_rechecks: 0,
                        },
                    );
                    self.metrics
                        .scope_cache_entries
                        .store(clusters.len() as u64, Ordering::Release);
                    Some(Arc::clone(&opened))
                }
            }
        };

        let Some(selected) = selected else {
            if let Err(error) = opened.close().await {
                self.record_close_failure(scope, "admission_rejected", &error);
            }
            return Err(admission_error.expect("admission error must accompany a rejected open"));
        };

        if !Arc::ptr_eq(&selected, &opened) {
            if let Err(error) = opened.close().await {
                self.record_close_failure(scope, "duplicate_open", &error);
            }
        }
        if let Some(cluster) = evicted {
            if let Err(error) = cluster.close().await {
                self.record_close_failure(scope, "eviction", &error);
            }
        }
        Ok((selected, false))
    }

    async fn hot_scopes(&self, limit: usize) -> Vec<GraphScope> {
        let clusters = self.clusters.lock().await;
        let mut hot = clusters
            .iter()
            .filter(|(_, entry)| entry.hot)
            .map(|(scope, entry)| (scope.clone(), entry.last_used))
            .collect::<Vec<_>>();
        hot.sort_by_key(|(_, last_used)| std::cmp::Reverse(*last_used));
        hot.into_iter()
            .take(limit)
            .map(|(scope, _)| scope)
            .collect()
    }

    async fn finish_scope(
        &self,
        scope: &GraphScope,
        expected: &Arc<GraphCluster>,
        cache_hit: bool,
        built: bool,
    ) {
        let removed = {
            let mut clusters = self.clusters.lock().await;
            let remove = match clusters.get_mut(scope) {
                Some(entry) if Arc::ptr_eq(&entry.cluster, expected) && built => {
                    entry.hot = true;
                    entry.idle_rechecks = 0;
                    false
                }
                Some(entry) if Arc::ptr_eq(&entry.cluster, expected) && cache_hit && entry.hot => {
                    entry.idle_rechecks = entry.idle_rechecks.saturating_add(1);
                    entry.idle_rechecks >= HOT_SCOPE_IDLE_RECHECKS
                }
                Some(entry) if Arc::ptr_eq(&entry.cluster, expected) => true,
                _ => false,
            };
            let removed = remove.then(|| clusters.remove(scope)).flatten();
            self.metrics
                .scope_cache_entries
                .store(clusters.len() as u64, Ordering::Release);
            removed.map(|entry| entry.cluster)
        };
        if let Some(cluster) = removed {
            if let Err(error) = cluster.close().await {
                self.record_close_failure(scope, "idle_scope", &error);
            }
        }
    }

    async fn invalidate(
        &self,
        scope: &GraphScope,
        expected: &Arc<GraphCluster>,
    ) -> slatedb_graph_kernel::Result<()> {
        let removed = {
            let mut clusters = self.clusters.lock().await;
            let matches = clusters
                .get(scope)
                .is_some_and(|entry| Arc::ptr_eq(&entry.cluster, expected));
            let removed = matches.then(|| clusters.remove(scope)).flatten();
            self.metrics
                .scope_cache_entries
                .store(clusters.len() as u64, Ordering::Release);
            removed.map(|entry| entry.cluster)
        };
        if let Some(cluster) = removed {
            cluster.close().await?;
        }
        Ok(())
    }

    async fn invalidate_scope(&self, scope: &GraphScope) -> slatedb_graph_kernel::Result<()> {
        let removed = {
            let mut clusters = self.clusters.lock().await;
            let removed = clusters.remove(scope).map(|entry| entry.cluster);
            self.metrics
                .scope_cache_entries
                .store(clusters.len() as u64, Ordering::Release);
            removed
        };
        if let Some(cluster) = removed {
            cluster.close().await?;
        }
        Ok(())
    }

    async fn retain_registered(&self, registered: &BTreeSet<GraphScope>) {
        let removed = {
            let mut clusters = self.clusters.lock().await;
            let stale = clusters
                .iter()
                .filter(|(scope, entry)| {
                    !registered.contains(*scope) && Arc::strong_count(&entry.cluster) == 1
                })
                .map(|(scope, _)| scope.clone())
                .collect::<Vec<_>>();
            let removed = stale
                .into_iter()
                .filter_map(|scope| clusters.remove(&scope).map(|entry| (scope, entry.cluster)))
                .collect::<Vec<_>>();
            self.metrics
                .scope_cache_entries
                .store(clusters.len() as u64, Ordering::Release);
            removed
        };
        for (scope, cluster) in removed {
            if let Err(error) = cluster.close().await {
                self.record_close_failure(&scope, "scope_removed", &error);
            }
        }
    }

    async fn close(&self) -> slatedb_graph_kernel::Result<()> {
        let clusters = std::mem::take(&mut *self.clusters.lock().await);
        self.metrics.scope_cache_entries.store(0, Ordering::Release);
        let mut failures = Vec::new();
        for (scope, entry) in clusters {
            if let Err(error) = entry.cluster.close().await {
                failures.push(format!("{scope}: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(GraphError::CorruptValue {
                key: "indexer/scope-cache/close".to_string(),
                reason: failures.join("; "),
            })
        }
    }

    fn record_close_failure(&self, scope: &GraphScope, reason: &str, error: &GraphError) {
        self.metrics
            .scope_cache_close_failures
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            hydradb.scope = %scope,
            reason,
            error = %error,
            "indexer scope cache could not close reader"
        );
    }
}

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
            hydradb.scope = failure.scope(),
            hydradb.cell_id = failure.cell_id(),
            hydradb.edge_type = failure.edge_type(),
            hydradb.outcome = Outcome::Failed.as_str(),
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

/// The Prometheus label name for the cell dimension, paired with the registry
/// entry that authorises it.
///
/// `/metrics` has always spelled its labels bare (`cell_id`) and the registry
/// spells its keys dotted (`hydradb.cell_id`); this is the one place in the
/// indexer where the two vocabularies meet, so the pairing is written down
/// rather than derived. The pairing also carries §1.3's rule: only a key the
/// registry classified as a metric dimension has a [`semconv::MetricLabel`]
/// constant, and `MetricLabel`'s constructor is private to the registry, so a
/// `scope` dimension declared this way does not compile — [`semconv::SCOPE`] is
/// a `&str`. That is not airtight, since a bare `"scope"` written straight into
/// the format string below would compile, which is why
/// `no_indexer_series_carries_scope` asserts the same thing against the
/// rendered output.
const CELL_ID_LABEL: (&str, semconv::MetricLabel) = ("cell_id", semconv::L_CELL_ID);

/// The Prometheus label name for the edge-type dimension. See [`CELL_ID_LABEL`].
const EDGE_TYPE_LABEL: (&str, semconv::MetricLabel) = ("edge_type", semconv::L_EDGE_TYPE);

/// The most `{cell_id, edge_type}` pairs the indexer reports separately.
///
/// `cell_id` is bounded by configuration — `GRAPH_CELLS`, a comma-separated
/// list read once at startup. `edge_type` is **not bounded by anything**. It is
/// a free-form string minted by whoever wrote the edge, checked only for its
/// character class (`validate_component`, `src/codec.rs:175`); no schema
/// declares the legal set, and `dirty_graph_index_edge_types` discovers them by
/// listing keys rather than by consulting one. The indexer also sweeps *every*
/// registered scope in the namespace, so its edge-type dimension is the union
/// across tenants, not one tenant's schema.
///
/// So the product is capped rather than trusted. Past the cap every new pair
/// folds into [`OVERFLOW_LABEL`]: the counter **totals stay exact** — no
/// increment is ever dropped — and only the attribution degrades.
/// `graph_indexer_dimensions` reports the live pair count so that degradation
/// is visible instead of silent.
const MAX_DIMENSIONS: usize = 512;

/// The value both dimensions take once [`MAX_DIMENSIONS`] is reached.
///
/// Not a legal `cell_id` or `edge_type` — `validate_component` permits only
/// ASCII alphanumerics, `_`, `-` and `.`, so a real value can contain neither
/// pair of underscores in this position by construction, and the sentinel
/// cannot collide with a genuine series.
const OVERFLOW_LABEL: &str = "__overflow__";

/// How many series each `{cell_id, edge_type}` pair costs.
const DIMENSIONED_FAMILIES: usize = 6;

/// The three counters that carry `{cell_id, edge_type}`.
///
/// One enumeration, one name table: [`DimensionedCounters::series`]
/// destructures `Self` with no `..`, so a fourth counter added to this struct
/// does not compile until it has been given a Prometheus name. That is the
/// property `snapshot_fields!` buys the kernel in `8d7e939`; there is one
/// exposition here rather than two, so there is one name table rather than two.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DimensionedCounters {
    generations_published: u64,
    generation_failures: u64,
    generations_deleted: u64,
    /// Edges scanned + encoded by full rebuilds. Before the incremental
    /// builder lands this is the total work the indexer does; after, it is
    /// the cost of fallbacks only. The before/after impact of the
    /// incremental index build is `rate(graph_indexer_full_build_edges)`
    /// dropping to (nearly) zero while
    /// `rate(graph_indexer_incremental_delta_edges)` tracks actual churn.
    full_build_edges: u64,
    /// Changed edges applied by incremental builds — the only work the
    /// delta path performs.
    incremental_delta_edges: u64,
    /// Incremental attempts that declined (no previous generation, WAL tail
    /// collected, payload missing) and fell back to a full rebuild.
    incremental_fallbacks: u64,
}

impl DimensionedCounters {
    /// Every field, paired with the series name it renders as.
    fn series(&self) -> [(&'static str, u64); DIMENSIONED_FAMILIES] {
        // No `..` in this pattern, deliberately.
        let Self {
            generations_published,
            generation_failures,
            generations_deleted,
            full_build_edges,
            incremental_delta_edges,
            incremental_fallbacks,
        } = *self;
        [
            ("graph_indexer_generations_published", generations_published),
            ("graph_indexer_generation_failures", generation_failures),
            ("graph_indexer_generations_deleted", generations_deleted),
            ("graph_indexer_full_build_edges", full_build_edges),
            (
                "graph_indexer_incremental_delta_edges",
                incremental_delta_edges,
            ),
            ("graph_indexer_incremental_fallbacks", incremental_fallbacks),
        ]
    }
}

#[derive(Default)]
struct IndexerMetrics {
    ready: AtomicBool,
    cycles: AtomicU64,
    successful_cycles: AtomicU64,
    failed_cycles: AtomicU64,
    full_sweeps: AtomicU64,
    consecutive_failed_cycles: AtomicU64,
    scopes_processed: AtomicU64,
    scopes_deferred: AtomicU64,
    hot_scope_rechecks: AtomicU64,
    open_failures: AtomicU64,
    scope_cache_hits: AtomicU64,
    scope_cache_misses: AtomicU64,
    scope_cache_evictions: AtomicU64,
    scope_cache_close_failures: AtomicU64,
    scope_cache_entries: AtomicU64,
    /// `generations_published`, `generation_failures` and `generations_deleted`,
    /// keyed by `{cell_id, edge_type}`.
    ///
    /// The four counters above stay process-global because they describe the
    /// process: a cycle is not a property of a cell. These three describe work
    /// done *to* a cell, and every site that increments them already holds both
    /// identifiers — the same fields [`IndexFailure`] keeps structured so they
    /// survive to the top. `graph_indexer_generation_failures` going up used to
    /// say only "a generation failed"; it now says which one.
    ///
    /// `scope` is deliberately absent. It is one value per tenant and unbounded
    /// by product decision, and no dimension of a metric may be unbounded.
    ///
    /// A `Mutex` rather than an atomic per pair because the key set is
    /// discovered at runtime. It is never held across an `await`, and every
    /// increment sits behind an object-store round trip that costs several
    /// orders of magnitude more than the lock. Entries are never removed: a
    /// counter series that disappears reads to Prometheus as a reset.
    dimensioned: Mutex<BTreeMap<(String, String), DimensionedCounters>>,
    last_success_ms: AtomicU64,
    last_full_sweep_ms: AtomicU64,
}

impl IndexerMetrics {
    /// Apply `record` to the counters for one `{cell_id, edge_type}` pair,
    /// folding into [`OVERFLOW_LABEL`] once the map is full.
    fn dimension<F: FnOnce(&mut DimensionedCounters)>(
        &self,
        cell_id: &str,
        edge_type: &str,
        record: F,
    ) {
        let mut counters = self.lock_dimensioned();
        let key = (cell_id.to_string(), edge_type.to_string());
        // A pair already being tracked keeps its own series however full the
        // map is; only genuinely new pairs can be turned away.
        let entry = if counters.contains_key(&key) || counters.len() < MAX_DIMENSIONS {
            counters.entry(key).or_default()
        } else {
            counters
                .entry((OVERFLOW_LABEL.to_string(), OVERFLOW_LABEL.to_string()))
                .or_default()
        };
        record(entry);
    }

    fn record_generation_published(&self, cell_id: &str, edge_type: &str) {
        self.dimension(cell_id, edge_type, |counters| {
            counters.generations_published = counters.generations_published.saturating_add(1);
        });
    }

    fn record_generation_failure(&self, cell_id: &str, edge_type: &str) {
        self.dimension(cell_id, edge_type, |counters| {
            counters.generation_failures = counters.generation_failures.saturating_add(1);
        });
    }

    fn record_generations_deleted(&self, cell_id: &str, edge_type: &str, deleted: u64) {
        self.dimension(cell_id, edge_type, |counters| {
            counters.generations_deleted = counters.generations_deleted.saturating_add(deleted);
        });
    }

    fn record_full_build_edges(&self, cell_id: &str, edge_type: &str, edges: u64) {
        self.dimension(cell_id, edge_type, |counters| {
            counters.full_build_edges = counters.full_build_edges.saturating_add(edges);
        });
    }

    fn record_incremental_delta_edges(&self, cell_id: &str, edge_type: &str, delta_edges: u64) {
        self.dimension(cell_id, edge_type, |counters| {
            counters.incremental_delta_edges =
                counters.incremental_delta_edges.saturating_add(delta_edges);
        });
    }

    fn record_incremental_fallback(&self, cell_id: &str, edge_type: &str) {
        self.dimension(cell_id, edge_type, |counters| {
            counters.incremental_fallbacks = counters.incremental_fallbacks.saturating_add(1);
        });
    }

    fn dimensioned_snapshot(&self) -> BTreeMap<(String, String), DimensionedCounters> {
        self.lock_dimensioned().clone()
    }

    /// A poisoned metrics mutex must not take the indexer down with it. The map
    /// holds counters and nothing else, so the worst a panicking writer can
    /// leave behind is a half-applied increment.
    fn lock_dimensioned(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<(String, String), DimensionedCounters>> {
        self.dimensioned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

fn record_failed_cycle_readiness(
    metrics: &IndexerMetrics,
    readiness_failure_threshold: u64,
) -> (u64, bool) {
    let consecutive_failures = metrics
        .consecutive_failed_cycles
        .fetch_add(1, Ordering::AcqRel)
        .saturating_add(1);
    let readiness_lost = consecutive_failures >= readiness_failure_threshold;
    if readiness_lost {
        metrics.ready.store(false, Ordering::Release);
    }
    (consecutive_failures, readiness_lost)
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
        hydradb_telemetry::init(TelemetryConfig::from_env(ServiceIdentity::GraphIndexer))?;

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
    let build_mode = IndexBuildMode::from_env()?;
    // Below this many edges the incremental path loses: the full scan rides
    // the block cache while the incremental build pays real object-store
    // round trips (previous payload GET, WAL tail GETs, per-edge resolution
    // reads). Measured in `examples/incremental_index_bench.rs` against
    // MinIO: a 200k-edge graph rebuilt 2x faster full, a 1M-edge graph 3.8x
    // faster incrementally. Tune per deployment cache size.
    let incremental_min_edges =
        env_value("GRAPH_INDEXER_INCREMENTAL_MIN_EDGES", "250000").parse::<u64>()?;
    let scope_concurrency = env_value(
        "GRAPH_INDEXER_SCOPE_CONCURRENCY",
        &DEFAULT_SCOPE_CONCURRENCY.to_string(),
    )
    .parse::<usize>()?;
    if !(1..=MAX_SCOPE_CONCURRENCY).contains(&scope_concurrency) {
        return Err(format!(
            "GRAPH_INDEXER_SCOPE_CONCURRENCY must be between 1 and {MAX_SCOPE_CONCURRENCY}"
        )
        .into());
    }
    let scopes_per_cycle = env_value(
        "GRAPH_INDEXER_SCOPES_PER_CYCLE",
        &DEFAULT_SCOPES_PER_CYCLE.to_string(),
    )
    .parse::<usize>()?;
    if !(scope_concurrency..=MAX_SCOPES_PER_CYCLE).contains(&scopes_per_cycle) {
        return Err(format!(
            "GRAPH_INDEXER_SCOPES_PER_CYCLE must be between GRAPH_INDEXER_SCOPE_CONCURRENCY ({scope_concurrency}) and {MAX_SCOPES_PER_CYCLE}"
        )
        .into());
    }
    let readiness_failure_threshold = env_value(
        "GRAPH_INDEXER_READINESS_FAILURE_THRESHOLD",
        &DEFAULT_READINESS_FAILURE_THRESHOLD.to_string(),
    )
    .parse::<u64>()?;
    if !(1..=MAX_READINESS_FAILURE_THRESHOLD).contains(&readiness_failure_threshold) {
        return Err(format!(
            "GRAPH_INDEXER_READINESS_FAILURE_THRESHOLD must be between 1 and {MAX_READINESS_FAILURE_THRESHOLD}"
        )
        .into());
    }
    let max_open_scopes = env_value(
        "GRAPH_INDEXER_MAX_OPEN_SCOPES",
        &DEFAULT_MAX_OPEN_SCOPES.to_string(),
    )
    .parse::<usize>()?;
    if !(scope_concurrency..=MAX_OPEN_SCOPES).contains(&max_open_scopes) {
        return Err(format!(
            "GRAPH_INDEXER_MAX_OPEN_SCOPES must be between GRAPH_INDEXER_SCOPE_CONCURRENCY ({scope_concurrency}) and {MAX_OPEN_SCOPES}"
        )
        .into());
    }
    // The tail cost gate. Each WAL file in the span between the previous
    // generation and the durable head is one object-store round trip, and the
    // span grows with write activity, not graph size — an uncapped walk can
    // spend minutes deriving a delta of a few edges (the staging regression
    // behind `interactive/incremental-build-cost.html`). Past the cap the
    // incremental attempt declines and the full rebuild runs instead, which
    // past that point is the cheaper of the two.
    let max_wal_tail_files = env_value(
        "GRAPH_INDEXER_MAX_TAIL_FILES",
        &GraphLimits::default().max_wal_tail_files.to_string(),
    )
    .parse::<u64>()?;
    // `GraphOpenOptions` is `#[non_exhaustive]`: constructed from `default()`
    // with fields assigned, per its own docs.
    let mut open_options = GraphOpenOptions::default();
    open_options.limits = GraphLimits {
        max_wal_tail_files,
        ..GraphLimits::default()
    };
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
    let scope_cache = Arc::new(IndexerScopeCache::new(
        data_path.clone(),
        cells.clone(),
        Arc::clone(&object_store),
        open_options.clone(),
        max_open_scopes,
        Arc::clone(&metrics),
    ));
    let mut shutdown = Box::pin(shutdown_signal());
    tracing::info!(
        scope = %root_scope,
        ?cells,
        build_mode = build_mode.as_str(),
        incremental_min_edges,
        max_wal_tail_files,
        scope_concurrency,
        scopes_per_cycle,
        max_open_scopes,
        readiness_failure_threshold,
        "graph indexer started"
    );

    // Mirrors `metrics.ready`, which was stored `true` above. Kept alongside so
    // the readiness *transition* can be detected without re-reading an atomic
    // that another task may have moved.
    let mut ready = true;
    // A registry snapshot is retained for one complete cursor rotation. Listing
    // every registered scope before each bounded pass would replace one large
    // S3 LIST with many identical large S3 LISTs.
    let mut registered_scopes = RegisteredScopeSchedule::default();

    loop {
        metrics.cycles.fetch_add(1, Ordering::Relaxed);
        // The indexer has no client parent, so this is a trace root.
        let cycle_span = tracing::info_span!(
            "index.cycle",
            hydradb.scope = %root_scope,
            cycle = metrics.cycles.load(Ordering::Relaxed),
            failure_count = Empty,
            hydradb.outcome = Empty,
            error.class = Empty,
        );
        let outcome = run_registered_scopes_cycle(
            &data_path,
            &scope_directory,
            &mut registered_scopes,
            Arc::clone(&object_store),
            retain_previous,
            build_mode,
            incremental_min_edges,
            scope_concurrency,
            scopes_per_cycle,
            &scope_cache,
            &metrics,
        )
        .instrument(cycle_span.clone())
        .await;

        match outcome {
            Ok(cycle) => {
                metrics.successful_cycles.fetch_add(1, Ordering::Relaxed);
                let completed_at = unix_time_ms();
                metrics
                    .last_success_ms
                    .store(completed_at, Ordering::Relaxed);
                if cycle.full_sweep_completed {
                    metrics.full_sweeps.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .last_full_sweep_ms
                        .store(completed_at, Ordering::Relaxed);
                }
                metrics
                    .consecutive_failed_cycles
                    .store(0, Ordering::Release);
                metrics.ready.store(true, Ordering::Release);
                record_success(&cycle_span);
                if !ready {
                    // Only the transition, never the steady state.
                    cycle_span.in_scope(|| {
                        tracing::info!(
                            hydradb.scope = %root_scope,
                            hydradb.outcome = Outcome::Success.as_str(),
                            "graph indexer readiness regained"
                        );
                    });
                }
                ready = true;
            }
            Err(failures) => {
                metrics.failed_cycles.fetch_add(1, Ordering::Relaxed);
                let (consecutive_failures, readiness_lost) =
                    record_failed_cycle_readiness(&metrics, readiness_failure_threshold);
                cycle_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                cycle_span.record("failure_count", failures.len());
                if let Some(first) = failures.first() {
                    cycle_span.record(semconv::ERROR_CLASS, first.class);
                }
                if ready && readiness_lost {
                    // `metrics.ready` going false is what a Kubernetes probe
                    // acts on, so the flip gets its own event with the failing
                    // cell attached — a pod going unready then connects to the
                    // specific cell that caused it rather than to a timestamp.
                    // Recorded on transitions only; a cycle that was already
                    // failing has said this once.
                    let first = failures.first();
                    cycle_span.in_scope(|| {
                        tracing::error!(
                            hydradb.scope = first.map(IndexFailure::scope).unwrap_or_default(),
                            hydradb.cell_id = first.map(IndexFailure::cell_id).unwrap_or_default(),
                            hydradb.edge_type =
                                first.map(IndexFailure::edge_type).unwrap_or_default(),
                            error.class = first.map(|failure| failure.class).unwrap_or_default(),
                            stage = first.map(|failure| failure.stage).unwrap_or_default(),
                            failure_count = failures.len(),
                            consecutive_failures,
                            readiness_failure_threshold,
                            "graph indexer readiness lost"
                        );
                    });
                }
                if readiness_lost {
                    ready = false;
                }
                // Each failure was already recorded on the span that produced
                // it; this line stays for continuity, and is now a summary
                // rather than the only place the detail exists.
                cycle_span.in_scope(|| {
                    tracing::warn!(
                        failure_count = failures.len(),
                        consecutive_failures,
                        readiness_failure_threshold,
                        readiness_lost,
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
    scope_cache.close().await?;
    admin.stop().await?;
    tracing::info!(scope = %root_scope, "graph indexer stopped");
    telemetry.shutdown();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_registered_scopes_cycle(
    data_path: &str,
    scope_directory: &ObjectStoreGraphScopeDirectory,
    schedule: &mut RegisteredScopeSchedule,
    object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    scope_concurrency: usize,
    scopes_per_cycle: usize,
    scope_cache: &Arc<IndexerScopeCache>,
    metrics: &IndexerMetrics,
) -> Result<RegisteredScopesCycle, CycleFailures> {
    let mut failures = CycleFailures::default();
    if !(1..=MAX_SCOPE_CONCURRENCY).contains(&scope_concurrency) {
        failures.push(IndexFailure::config(
            "scope_scheduler",
            format!("scope concurrency must be between 1 and {MAX_SCOPE_CONCURRENCY}"),
        ));
        return Err(failures);
    }
    if !(scope_concurrency..=MAX_SCOPES_PER_CYCLE).contains(&scopes_per_cycle) {
        failures.push(IndexFailure::config(
            "scope_scheduler",
            format!(
                "scopes per cycle must be between scope concurrency ({scope_concurrency}) and {MAX_SCOPES_PER_CYCLE}"
            ),
        ));
        return Err(failures);
    }

    if schedule.scopes.is_empty() {
        let discovery_span = tracing::info_span!(
            "index.scope_discovery",
            scope_count = Empty,
            hydradb.outcome = Empty,
            error.class = Empty,
        );
        schedule.scopes = match scope_directory
            .list()
            .instrument(discovery_span.clone())
            .await
        {
            Ok(mut scopes) => {
                scopes.sort();
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
        schedule.scope_names = schedule.scopes.iter().map(ToString::to_string).collect();

        let registered = schedule.scopes.iter().cloned().collect::<BTreeSet<_>>();
        scope_cache.retain_registered(&registered).await;
    }
    if schedule.scopes.is_empty() {
        return Ok(RegisteredScopesCycle {
            full_sweep_completed: true,
        });
    }

    let cursor_path = scope_cursor_path(data_path, scope_directory.root_scope());
    let cursor = if let Some(cursor) = schedule.cursor.take() {
        cursor
    } else {
        match load_scope_cursor(&object_store, &cursor_path).await {
            Ok(cursor) => cursor,
            Err(error) => {
                let failure = IndexFailure::kernel(
                    "scope_cursor_load",
                    format!("load indexer scope cursor {cursor_path}"),
                    &error,
                );
                failures.push(failure);
                return Err(failures);
            }
        }
    };
    let starting_cursor = cursor.last_scope.clone();
    let starting_position = starting_cursor.as_deref().and_then(|last_scope| {
        schedule
            .scope_names
            .binary_search_by(|candidate| candidate.as_str().cmp(last_scope))
            .ok()
    });
    let starting_cursor_registered = starting_position.is_some();
    let final_registered_scope = schedule.scope_names.last().map(String::as_str);
    let mut cursor_progressed = false;
    let mut ending_cursor = cursor.last_scope.clone();
    let mut cursor = Some(cursor);

    let hot_budget = if scopes_per_cycle == 1 {
        0
    } else {
        (scopes_per_cycle / HOT_SCOPE_BUDGET_DIVISOR)
            .max(1)
            .min(scope_concurrency)
            .min(scopes_per_cycle - 1)
    };
    let mut hot_scopes = scope_cache.hot_scopes(hot_budget).await;
    let cold_budget = scopes_per_cycle.saturating_sub(hot_scopes.len());
    let start_index =
        starting_position.map_or(0, |position| (position + 1) % schedule.scopes.len());
    let mut scopes = Vec::with_capacity(cold_budget.min(schedule.scopes.len()));
    for offset in 0..schedule.scopes.len() {
        let scope = &schedule.scopes[(start_index + offset) % schedule.scopes.len()];
        scopes.push(scope.clone());
        if scopes.len() == cold_budget {
            break;
        }
    }
    let fair_set = scopes.iter().cloned().collect::<BTreeSet<_>>();
    hot_scopes.retain(|scope| !fair_set.contains(scope));

    let processed = hot_scopes.len().saturating_add(scopes.len());
    metrics
        .scopes_processed
        .fetch_add(processed as u64, Ordering::Relaxed);
    metrics
        .hot_scope_rechecks
        .fetch_add(hot_scopes.len() as u64, Ordering::Relaxed);
    metrics.scopes_deferred.fetch_add(
        schedule.scopes.len().saturating_sub(processed) as u64,
        Ordering::Relaxed,
    );

    for batch in hot_scopes.chunks(scope_concurrency) {
        failures.absorb(
            run_scope_batch(
                batch,
                retain_previous,
                build_mode,
                incremental_min_edges,
                scope_cache,
                metrics,
                scope_concurrency,
            )
            .await,
        );
    }

    for batch in scopes.chunks(scope_concurrency) {
        let batch_last_scope = batch.last().map(ToString::to_string).unwrap_or_default();
        failures.absorb(
            run_scope_batch(
                batch,
                retain_previous,
                build_mode,
                incremental_min_edges,
                scope_cache,
                metrics,
                scope_concurrency,
            )
            .await,
        );

        match advance_scope_cursor(
            &object_store,
            &cursor_path,
            cursor
                .take()
                .expect("a valid cursor must precede every fair-scan batch"),
            &batch_last_scope,
        )
        .await
        {
            Ok(ScopeCursorAdvance::Advanced(next)) => {
                ending_cursor.clone_from(&next.last_scope);
                cursor = Some(next);
                cursor_progressed = true;
            }
            Ok(ScopeCursorAdvance::LostRace) => {
                tracing::info!(
                    cursor = %cursor_path,
                    last_scope = %batch_last_scope,
                    "another indexer advanced the scope cursor"
                );
                // Do not overwrite the winner with a cursor based on stale
                // progress. Rediscovery on the next cycle resumes from the
                // winner's durable boundary.
                break;
            }
            Err(error) => {
                failures.push(IndexFailure::kernel(
                    "scope_cursor_advance",
                    format!("advance indexer scope cursor {cursor_path}"),
                    &error,
                ));
                break;
            }
        }
    }

    let full_sweep_completed = completed_scope_sweep(
        cursor_progressed,
        starting_cursor.as_deref(),
        ending_cursor.as_deref(),
        final_registered_scope,
        starting_cursor_registered,
    );
    schedule.cursor = cursor;
    failures.into_result().map(|()| {
        if full_sweep_completed {
            schedule.scopes.clear();
            schedule.scope_names.clear();
        }
        RegisteredScopesCycle {
            full_sweep_completed,
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_scope_batch(
    batch: &[GraphScope],
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    scope_cache: &IndexerScopeCache,
    metrics: &IndexerMetrics,
    scope_concurrency: usize,
) -> CycleFailures {
    let mut failures = CycleFailures::default();
    let runs = futures::stream::iter(batch.iter().cloned())
        .map(|scope| {
            run_registered_scope(
                scope,
                retain_previous,
                build_mode,
                incremental_min_edges,
                scope_cache,
                metrics,
            )
        })
        .buffer_unordered(scope_concurrency);
    let mut runs = std::pin::pin!(runs);
    while let Some(scope_failures) = runs.next().await {
        failures.absorb(scope_failures);
    }
    failures
}

#[allow(clippy::too_many_arguments)]
async fn run_registered_scope(
    scope: GraphScope,
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    scope_cache: &IndexerScopeCache,
    metrics: &IndexerMetrics,
) -> CycleFailures {
    let mut failures = CycleFailures::default();
    {
        let scope_name = scope.to_string();
        let scope_span = tracing::info_span!(
            "index.scope",
            hydradb.scope = %scope_name,
            hydradb.outcome = Empty,
            error.class = Empty,
        );

        let has_data_span = tracing::info_span!(
            parent: &scope_span,
            "index.scope_has_data",
            hydradb.scope = %scope_name,
            has_data = Empty,
            hydradb.outcome = Empty,
            error.class = Empty,
        );
        match scope_has_data(
            &scope_cache.data_path,
            &scope,
            &scope_cache.cells,
            &scope_cache.object_store,
        )
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
                if let Err(error) = scope_cache.invalidate_scope(&scope).await {
                    let failure = IndexFailure::kernel(
                        "cluster_close",
                        format!("close empty scope {scope_name}"),
                        &error,
                    )
                    .with_scope(&scope_name);
                    record_failure(&scope_span, &failure);
                    failures.push(failure);
                }
                return failures;
            }
            Err(error) => {
                let failure = IndexFailure::kernel(
                    "scope_has_data",
                    format!("scan scope {scope_name}"),
                    &error,
                )
                .with_scope(&scope_name);
                record_failure(&has_data_span, &failure);
                scope_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                failures.push(failure);
                return failures;
            }
        }

        let open_span = tracing::info_span!(
            parent: &scope_span,
            "index.cluster_open",
            hydradb.scope = %scope_name,
            cell_count = scope_cache.cells.len(),
            hydradb.outcome = Empty,
            error.class = Empty,
        );
        let (cluster, cache_hit) = match scope_cache
            .cluster_for_scope(&scope)
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
                return failures;
            }
        };

        let mut scope_failed = false;
        let mut scope_built = false;
        // Instrumenting the call rather than parenting a span by hand is what
        // makes every `index.cell` below a child of this scope.
        match run_index_cycle(
            &cluster,
            &scope_name,
            &scope_cache.cells,
            retain_previous,
            build_mode,
            incremental_min_edges,
            metrics,
        )
        .instrument(scope_span.clone())
        .await
        {
            Ok(built) => scope_built = built,
            Err(inner) => {
                scope_failed = true;
                failures.absorb(inner);
            }
        }

        if scope_failed {
            if let Err(error) = scope_cache.invalidate(&scope, &cluster).await {
                let failure = IndexFailure::kernel(
                    "cluster_close",
                    format!("close failed scope {scope_name}"),
                    &error,
                )
                .with_scope(&scope_name);
                record_failure(&scope_span, &failure);
                failures.push(failure);
            }
        }

        if scope_failed {
            scope_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
        } else {
            scope_cache
                .finish_scope(&scope, &cluster, cache_hit, scope_built)
                .await;
            record_success(&scope_span);
        }
    }
    failures
}

fn scope_cursor_path(data_path: &str, root_scope: GraphScope) -> Path {
    Path::from(format!(
        "{data_path}/_graph_indexer/v1/{root_scope}/scope-cursor"
    ))
}

#[cfg(test)]
fn rotate_scopes_after(scopes: &mut [GraphScope], last_scope: Option<&str>) {
    if scopes.is_empty() {
        return;
    }
    let Some(last_scope) = last_scope else {
        return;
    };
    let Some(position) = scopes
        .iter()
        .position(|scope| scope.to_string() == last_scope)
    else {
        return;
    };
    let next = position.saturating_add(1) % scopes.len();
    scopes.rotate_left(next);
}

fn completed_scope_sweep(
    cursor_progressed: bool,
    starting_cursor: Option<&str>,
    ending_cursor: Option<&str>,
    final_scope: Option<&str>,
    starting_cursor_registered: bool,
) -> bool {
    if !cursor_progressed {
        return false;
    }
    if ending_cursor == final_scope && ending_cursor.is_some() {
        return true;
    }

    // A bounded pass commonly crosses the end and continues at the beginning
    // when the registry size is not divisible by the cycle budget. Hot scopes
    // can create the same shape because they are checked before the fair slice
    // and removed from it. In both cases the durable cursor moves backwards.
    // Starting exactly at the final scope is different: that begins a new
    // sweep and must not immediately report another completed one.
    starting_cursor_registered
        && matches!(
            (starting_cursor, ending_cursor, final_scope),
            (Some(start), Some(end), Some(final_scope))
                if start != final_scope && end < start
        )
}

async fn load_scope_cursor(
    object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
    path: &Path,
) -> slatedb_graph_kernel::Result<ScopeCursor> {
    match object_store.get(path).await {
        Ok(result) => {
            let update_version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let value = result.bytes().await?;
            if value.len() > MAX_SCOPE_CURSOR_BYTES {
                return Err(GraphError::CorruptValue {
                    key: path.to_string(),
                    reason: format!(
                        "indexer scope cursor is {} bytes; maximum is {MAX_SCOPE_CURSOR_BYTES}",
                        value.len()
                    ),
                });
            }
            let last_scope =
                std::str::from_utf8(&value).map_err(|error| GraphError::CorruptValue {
                    key: path.to_string(),
                    reason: format!("indexer scope cursor is not UTF-8: {error}"),
                })?;
            Ok(ScopeCursor {
                last_scope: Some(last_scope.to_string()),
                update_version: Some(update_version),
            })
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(ScopeCursor {
            last_scope: None,
            update_version: None,
        }),
        Err(error) => Err(error.into()),
    }
}

async fn advance_scope_cursor(
    object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
    path: &Path,
    cursor: ScopeCursor,
    last_scope: &str,
) -> slatedb_graph_kernel::Result<ScopeCursorAdvance> {
    let mode = cursor
        .update_version
        .map_or(PutMode::Create, PutMode::Update);
    match object_store
        .put_opts(
            path,
            Bytes::copy_from_slice(last_scope.as_bytes()).into(),
            mode.into(),
        )
        .await
    {
        Ok(result) => Ok(ScopeCursorAdvance::Advanced(ScopeCursor {
            last_scope: Some(last_scope.to_string()),
            update_version: Some(UpdateVersion {
                e_tag: result.e_tag,
                version: result.version,
            }),
        })),
        Err(slatedb::object_store::Error::AlreadyExists { .. })
        | Err(slatedb::object_store::Error::Precondition { .. })
        | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(ScopeCursorAdvance::LostRace),
        Err(error) => Err(error.into()),
    }
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
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    metrics: &IndexerMetrics,
) -> Result<bool, CycleFailures> {
    let mut failures = CycleFailures::default();
    let mut built_any = false;
    for cell_id in cells {
        let cell_span = tracing::info_span!(
            "index.cell",
            hydradb.scope = %scope,
            hydradb.cell_id = %cell_id,
            dirty_edge_types = Empty,
            hydradb.outcome = Empty,
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
            hydradb.scope = %scope,
            hydradb.cell_id = %cell_id,
            hydradb.base_sequence = Empty,
            hydradb.outcome = Empty,
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
            hydradb.scope = %scope,
            hydradb.cell_id = %cell_id,
            dirty_count = Empty,
            hydradb.outcome = Empty,
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
                hydradb.scope = %scope,
                hydradb.cell_id = %cell_id,
                hydradb.edge_type = %edge_type,
                dirty_sequence,
                hydradb.generation = Empty,
                hydradb.base_sequence = Empty,
                hydradb.outcome = Empty,
                error.class = Empty,
            );

            let read_span = tracing::info_span!(
                parent: &edge_span,
                "index.read_current",
                hydradb.scope = %scope,
                hydradb.cell_id = %cell_id,
                hydradb.edge_type = %edge_type,
                present = Empty,
                hydradb.generation = Empty,
                hydradb.base_sequence = Empty,
                hydradb.outcome = Empty,
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
                        hydradb.scope = %scope,
                        hydradb.cell_id = %cell_id,
                        hydradb.edge_type = %edge_type,
                        hydradb.generation = %generation.generation,
                        hydradb.base_sequence = generation.base_sequence,
                        hydradb.outcome = Outcome::Skipped.as_str(),
                        dirty_sequence,
                        "graph index generation already current"
                    );
                });
                continue;
            }

            let build_span = tracing::info_span!(
                parent: &edge_span,
                "artifact.build",
                hydradb.scope = %scope,
                hydradb.cell_id = %cell_id,
                hydradb.edge_type = %edge_type,
                hydradb.generation = Empty,
                hydradb.base_sequence = Empty,
                edge_count = Empty,
                build_mode = Empty,
                hydradb.outcome = Empty,
                error.class = Empty,
            );

            // The size floor: below `incremental_min_edges` the full scan
            // rides the block cache while the incremental path pays real
            // object-store round trips, so a small graph rebuilds faster the
            // old way (`examples/incremental_index_bench.rs` has the
            // numbers). A floor skip is a deliberate policy choice, not an
            // incremental attempt that declined — it records as a plain full
            // build and leaves the fallback counter alone.
            let attempt_incremental = build_mode == IndexBuildMode::Incremental
                && current
                    .as_ref()
                    .is_some_and(|generation| generation.edge_count >= incremental_min_edges);
            let outcome: Result<(GraphIndexGeneration, GraphIndexBuildPath), GraphError> =
                if attempt_incremental {
                    shard
                        .build_graph_index_auto(cell_id, &edge_type)
                        .instrument(build_span.clone())
                        .await
                } else {
                    shard
                        .build_graph_index(cell_id, &edge_type)
                        .instrument(build_span.clone())
                        .await
                        .map(|generated| {
                            let edge_count = generated.edge_count;
                            (generated, GraphIndexBuildPath::Full { edges: edge_count })
                        })
                };
            let generation = match outcome {
                Ok((generation, build_path)) => {
                    build_span.record(semconv::GENERATION, generation.generation.as_str());
                    build_span.record(semconv::BASE_SEQUENCE, generation.base_sequence);
                    build_span.record("edge_count", generation.edge_count);
                    build_span.record("build_mode", build_mode.as_str());

                    match build_path {
                        GraphIndexBuildPath::Full { edges } => {
                            metrics.record_full_build_edges(cell_id, &edge_type, edges);
                            if attempt_incremental {
                                metrics.record_incremental_fallback(cell_id, &edge_type);
                            }
                        }
                        GraphIndexBuildPath::Incremental { delta_edges } => {
                            metrics.record_incremental_delta_edges(
                                cell_id,
                                &edge_type,
                                delta_edges,
                            );
                        }
                        GraphIndexBuildPath::Current => {}
                    }
                    record_success(&build_span);
                    generation
                }
                Err(error) => {
                    metrics.record_generation_failure(cell_id, &edge_type);
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

            metrics.record_generation_published(cell_id, &edge_type);
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
                hydradb.scope = %scope,
                hydradb.cell_id = %cell_id,
                hydradb.edge_type = %edge_type,
                hydradb.generation = %generation.generation,
                hydradb.base_sequence = generation.base_sequence,
                content_hash = %generation.generation,
                checksum = generation.checksum,
                edge_count = generation.edge_count,
                last_wal_id = generation.last_wal_id,
                pointer_advanced,
                hydradb.outcome = publish_outcome.as_str(),
            );
            publish_span.in_scope(|| {
                tracing::info!(
                    hydradb.scope = %scope,
                    hydradb.cell_id = %cell_id,
                    hydradb.edge_type = %edge_type,
                    hydradb.generation = %generation.generation,
                    hydradb.base_sequence = generation.base_sequence,
                    hydradb.outcome = publish_outcome.as_str(),
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
                hydradb.scope = %scope,
                hydradb.cell_id = %cell_id,
                hydradb.edge_type = %edge_type,
                hydradb.generation = %generation.generation,
                hydradb.base_sequence = generation.base_sequence,
                retain_previous,
                deleted = Empty,
                hydradb.outcome = Empty,
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
                    metrics.record_generations_deleted(cell_id, &edge_type, deleted);
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

            // xlog retention rides the same cleanup step. Best-effort twice
            // over: a reader-mode shard (the deployed topology) returns
            // Ok(0) without writing — retention is then the writer node's
            // job — and like generation GC above, a failure never fails the
            // cycle the entries were tidying up after.
            let xlog_gc_span = tracing::info_span!(
                parent: &edge_span,
                "artifact.xlog_gc",
                hydradb.scope = %scope,
                hydradb.cell_id = %cell_id,
                hydradb.edge_type = %edge_type,
                deleted = Empty,
                hydradb.outcome = Empty,
                error.class = Empty,
            );
            match shard
                .gc_topology_changelog(cell_id, &edge_type)
                .instrument(xlog_gc_span.clone())
                .await
            {
                Ok(deleted) => {
                    xlog_gc_span.record("deleted", deleted);
                    xlog_gc_span.record(
                        semconv::OUTCOME,
                        if deleted == 0 {
                            Outcome::Skipped.as_str()
                        } else {
                            Outcome::Success.as_str()
                        },
                    );
                }
                Err(error) => {
                    let failure = IndexFailure::kernel(
                        "artifact_xlog_gc",
                        format!("index scope {scope}: xlog cleanup {cell_id}/{edge_type}"),
                        &error,
                    )
                    .with_scope(scope)
                    .with_cell(cell_id)
                    .with_edge_type(&edge_type);
                    record_failure(&xlog_gc_span, &failure);
                }
            }

            record_success(&edge_span);
            cell_built = true;
        }

        if cell_failed {
            cell_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
        } else if cell_built {
            built_any = true;
            record_success(&cell_span);
        } else {
            cell_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
        }
    }

    failures.into_result().map(|()| built_any)
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
    (
        [
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        render_metrics(&metrics),
    )
        .into_response()
}

/// The `/metrics` body.
///
/// Split out of the handler so the exposition can be asserted on directly
/// rather than through an HTTP round trip; the handler adds headers and nothing
/// else. The three dimensioned families keep the position they had when they
/// were scalars, so a diff of the output shows labels appearing and no series
/// moving.
fn render_metrics(metrics: &IndexerMetrics) -> String {
    let mut output = format!(
        concat!(
            "# TYPE graph_indexer_ready gauge\n",
            "graph_indexer_ready {}\n",
            "# TYPE graph_indexer_cycles counter\n",
            "graph_indexer_cycles {}\n",
            "# TYPE graph_indexer_successful_cycles counter\n",
            "graph_indexer_successful_cycles {}\n",
            "# TYPE graph_indexer_failed_cycles counter\n",
            "graph_indexer_failed_cycles {}\n",
            "# TYPE graph_indexer_full_sweeps counter\n",
            "graph_indexer_full_sweeps {}\n",
            "# TYPE graph_indexer_consecutive_failed_cycles gauge\n",
            "graph_indexer_consecutive_failed_cycles {}\n",
            "# TYPE graph_indexer_scopes_processed counter\n",
            "graph_indexer_scopes_processed {}\n",
            "# TYPE graph_indexer_scopes_deferred counter\n",
            "graph_indexer_scopes_deferred {}\n",
            "# TYPE graph_indexer_hot_scope_rechecks counter\n",
            "graph_indexer_hot_scope_rechecks {}\n",
            "# TYPE graph_indexer_open_failures counter\n",
            "graph_indexer_open_failures {}\n",
            "# TYPE graph_indexer_scope_cache_hits counter\n",
            "graph_indexer_scope_cache_hits {}\n",
            "# TYPE graph_indexer_scope_cache_misses counter\n",
            "graph_indexer_scope_cache_misses {}\n",
            "# TYPE graph_indexer_scope_cache_evictions counter\n",
            "graph_indexer_scope_cache_evictions {}\n",
            "# TYPE graph_indexer_scope_cache_close_failures counter\n",
            "graph_indexer_scope_cache_close_failures {}\n",
            "# TYPE graph_indexer_scope_cache_entries gauge\n",
            "graph_indexer_scope_cache_entries {}\n",
        ),
        u8::from(metrics.ready.load(Ordering::Acquire)),
        metrics.cycles.load(Ordering::Relaxed),
        metrics.successful_cycles.load(Ordering::Relaxed),
        metrics.failed_cycles.load(Ordering::Relaxed),
        metrics.full_sweeps.load(Ordering::Relaxed),
        metrics.consecutive_failed_cycles.load(Ordering::Relaxed),
        metrics.scopes_processed.load(Ordering::Relaxed),
        metrics.scopes_deferred.load(Ordering::Relaxed),
        metrics.hot_scope_rechecks.load(Ordering::Relaxed),
        metrics.open_failures.load(Ordering::Relaxed),
        metrics.scope_cache_hits.load(Ordering::Relaxed),
        metrics.scope_cache_misses.load(Ordering::Relaxed),
        metrics.scope_cache_evictions.load(Ordering::Relaxed),
        metrics.scope_cache_close_failures.load(Ordering::Relaxed),
        metrics.scope_cache_entries.load(Ordering::Acquire),
    );
    append_dimensioned(&mut output, &metrics.dimensioned_snapshot());
    output.push_str(&format!(
        concat!(
            "# TYPE graph_indexer_last_success_ms gauge\n",
            "graph_indexer_last_success_ms {}\n",
            "# TYPE graph_indexer_last_full_sweep_ms gauge\n",
            "graph_indexer_last_full_sweep_ms {}\n",
        ),
        metrics.last_success_ms.load(Ordering::Relaxed),
        metrics.last_full_sweep_ms.load(Ordering::Relaxed),
    ));
    output
}

/// The three `{cell_id, edge_type}` families, plus the gauge that says how much
/// of the cardinality budget they are using.
///
/// Family-major, and the `# TYPE` line comes from the same enumeration the
/// samples do, so a family with no pairs yet still declares itself instead of
/// vanishing from the scrape until the first generation is built.
fn append_dimensioned(
    output: &mut String,
    counters: &BTreeMap<(String, String), DimensionedCounters>,
) {
    let cell = CELL_ID_LABEL.0;
    let edge = EDGE_TYPE_LABEL.0;
    for (slot, (name, _)) in DimensionedCounters::default().series().iter().enumerate() {
        output.push_str(&format!("# TYPE {name} counter\n"));
        for ((cell_id, edge_type), value) in counters {
            let (_, count) = value.series()[slot];
            let cell_value = escape_label_value(cell_id);
            let edge_value = escape_label_value(edge_type);
            output.push_str(&format!(
                "{name}{{{cell}=\"{cell_value}\",{edge}=\"{edge_value}\"}} {count}\n"
            ));
        }
    }
    // Additive, and the reason the cap is safe to have: without it, a fleet
    // that has quietly saturated `MAX_DIMENSIONS` and is folding everything
    // into `__overflow__` looks exactly like one that has not.
    output.push_str(&format!(
        concat!(
            "# TYPE graph_indexer_dimensions gauge\n",
            "graph_indexer_dimensions {}\n",
        ),
        counters.len(),
    ));
}

/// Escape a Prometheus label value.
///
/// Defensive rather than load-bearing: every value that reaches here today has
/// been through `validate_component`, which permits only ASCII alphanumerics,
/// `_`, `-` and `.`, so none of the three characters below can appear. That is
/// a three-hop argument through the kernel, and it is one refactor away from
/// being wrong — an unescaped `"` in a label value does not error, it produces
/// an exposition the scraper rejects wholesale.
fn escape_label_value(value: &str) -> Cow<'_, str> {
    if !value
        .bytes()
        .any(|byte| matches!(byte, b'\\' | b'"' | b'\n'))
    {
        return Cow::Borrowed(value);
    }
    let mut escaped = String::with_capacity(value.len() + 8);
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(character),
        }
    }
    Cow::Owned(escaped)
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

/// How a cycle builds a dirty edge type's index, set by
/// `GRAPH_INDEXER_BUILD_MODE`.
///
/// A kill switch rather than a permanent knob. The incremental path patches
/// the previous generation with the WAL-tail delta instead of rescanning
/// canonical storage; it defaults to off so deploying this binary changes
/// nothing, and an operator who sees trouble sets the variable back to
/// `full` and restarts rather than rolling back a release.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum IndexBuildMode {
    /// Always rescan canonical storage. The behaviour before incremental
    /// builds existed, and the default.
    #[default]
    Full,
    /// Try the delta path, fall back to a full rebuild whenever it declines.
    Incremental,
}

impl IndexBuildMode {
    const VAR: &'static str = "GRAPH_INDEXER_BUILD_MODE";

    fn from_env() -> RuntimeResult<Self> {
        Self::parse(&env_value(Self::VAR, "full"))
    }

    /// Split from [`Self::from_env`] so the accepted spellings are testable
    /// without mutating process environment, which no parallel test may do.
    fn parse(value: &str) -> RuntimeResult<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "full" => Ok(Self::Full),
            "incremental" => Ok(Self::Incremental),
            other => Err(format!(
                "{} must be `full` or `incremental`, got `{other}`",
                Self::VAR
            )
            .into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Incremental => "incremental",
        }
    }
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

    fn test_scope_cache(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        metrics: Arc<IndexerMetrics>,
        max_open_scopes: usize,
    ) -> Arc<IndexerScopeCache> {
        Arc::new(IndexerScopeCache::new(
            "graph/data".to_string(),
            vec!["cell-0".to_string()],
            object_store,
            GraphOpenOptions::default(),
            max_open_scopes,
            metrics,
        ))
    }

    /// The kill switch defaults to the pre-existing behaviour, and refuses a
    /// spelling it does not understand rather than silently choosing one — an
    /// operator who typos `incremenal` during an incident must be told, not
    /// quietly left on the other path.
    #[test]
    fn index_build_mode_parses_known_spellings_and_rejects_the_rest() {
        assert_eq!(IndexBuildMode::default(), IndexBuildMode::Full);
        assert_eq!(IndexBuildMode::parse("full").unwrap(), IndexBuildMode::Full);
        assert_eq!(
            IndexBuildMode::parse("  Incremental ").unwrap(),
            IndexBuildMode::Incremental,
            "case and surrounding whitespace must not change the meaning"
        );
        let error = IndexBuildMode::parse("incremenal")
            .expect_err("a misspelling must not resolve to a mode")
            .to_string();
        assert!(
            error.contains(IndexBuildMode::VAR) && error.contains("incremenal"),
            "the error must name the variable and the offending value: {error}"
        );
    }

    #[test]
    fn scope_order_resumes_after_the_durable_cursor() {
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let scope = |tenant: &str| {
            GraphScope::new(
                root.child(NamespaceId::new(tenant).unwrap()).unwrap(),
                graph_id.clone(),
            )
        };
        let first = scope("tenant-a");
        let second = scope("tenant-b");
        let third = scope("tenant-c");
        let mut scopes = vec![first.clone(), second.clone(), third.clone()];

        rotate_scopes_after(&mut scopes, Some(&second.to_string()));

        assert_eq!(scopes, vec![third, first, second]);
    }

    #[test]
    fn full_sweep_detects_a_non_divisible_rotation_boundary() {
        assert!(completed_scope_sweep(
            true,
            Some("scope-b"),
            Some("scope-a"),
            Some("scope-c"),
            true,
        ));
        assert!(!completed_scope_sweep(
            true,
            Some("scope-c"),
            Some("scope-a"),
            Some("scope-c"),
            true,
        ));
        assert!(!completed_scope_sweep(
            true,
            Some("removed-scope"),
            Some("scope-a"),
            Some("scope-c"),
            false,
        ));
    }

    #[tokio::test]
    async fn scope_cursor_cas_never_overwrites_another_indexer() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let path = Path::from("graph/data/_graph_indexer/test/scope-cursor");
        let first_observer = load_scope_cursor(&object_store, &path).await.unwrap();
        let stale_observer = load_scope_cursor(&object_store, &path).await.unwrap();

        let ScopeCursorAdvance::Advanced(first_cursor) =
            advance_scope_cursor(&object_store, &path, first_observer, "scope-a")
                .await
                .unwrap()
        else {
            panic!("the first indexer must create the cursor");
        };
        assert!(matches!(
            advance_scope_cursor(&object_store, &path, stale_observer, "scope-b")
                .await
                .unwrap(),
            ScopeCursorAdvance::LostRace
        ));
        assert_eq!(
            load_scope_cursor(&object_store, &path)
                .await
                .unwrap()
                .last_scope
                .as_deref(),
            Some("scope-a"),
            "a stale indexer must not replace the winning cursor"
        );

        assert!(matches!(
            advance_scope_cursor(&object_store, &path, first_cursor, "scope-b")
                .await
                .unwrap(),
            ScopeCursorAdvance::Advanced(_)
        ));
    }

    #[tokio::test]
    async fn scope_cache_reuses_and_evicts_idle_readers() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(object_store, Arc::clone(&metrics), 1);
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let first_scope = GraphScope::new(
            root.child(NamespaceId::new("tenant-a").unwrap()).unwrap(),
            GraphId::new("hydradb").unwrap(),
        );
        let second_scope = GraphScope::new(
            root.child(NamespaceId::new("tenant-b").unwrap()).unwrap(),
            GraphId::new("hydradb").unwrap(),
        );

        let (first, first_hit) = cache.cluster_for_scope(&first_scope).await.unwrap();
        let (reused, reused_hit) = cache.cluster_for_scope(&first_scope).await.unwrap();
        assert!(!first_hit);
        assert!(reused_hit);
        assert!(Arc::ptr_eq(&first, &reused));
        drop(first);
        drop(reused);

        let (second, second_hit) = cache.cluster_for_scope(&second_scope).await.unwrap();
        assert!(!second_hit);
        drop(second);

        assert_eq!(metrics.scope_cache_hits.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.scope_cache_misses.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.scope_cache_evictions.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 1);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn idle_scan_misses_do_not_evict_hot_scope_readers() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(object_store, Arc::clone(&metrics), 2);
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let scope = |tenant: &str| {
            GraphScope::new(
                root.child(NamespaceId::new(tenant).unwrap()).unwrap(),
                GraphId::new("hydradb").unwrap(),
            )
        };
        let hot_scope = scope("hot");
        let idle_scope = scope("idle");

        let (hot, hot_hit) = cache.cluster_for_scope(&hot_scope).await.unwrap();
        cache.finish_scope(&hot_scope, &hot, hot_hit, true).await;
        let (idle, idle_hit) = cache.cluster_for_scope(&idle_scope).await.unwrap();
        cache
            .finish_scope(&idle_scope, &idle, idle_hit, false)
            .await;

        assert_eq!(cache.hot_scopes(10).await, vec![hot_scope.clone()]);
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 1);
        let (reused, reused_hit) = cache.cluster_for_scope(&hot_scope).await.unwrap();
        assert!(reused_hit);
        assert!(Arc::ptr_eq(&hot, &reused));
        cache.close().await.unwrap();
    }

    #[test]
    fn readiness_tolerates_one_transient_cycle_failure() {
        let metrics = IndexerMetrics::default();
        metrics.ready.store(true, Ordering::Release);

        assert_eq!(record_failed_cycle_readiness(&metrics, 3), (1, false));
        assert!(metrics.ready.load(Ordering::Acquire));
        assert_eq!(record_failed_cycle_readiness(&metrics, 3), (2, false));
        assert!(metrics.ready.load(Ordering::Acquire));
        assert_eq!(record_failed_cycle_readiness(&metrics, 3), (3, true));
        assert!(!metrics.ready.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn scheduler_processes_only_the_configured_fair_slice() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            root.clone(),
            graph_id.clone(),
            Arc::clone(&object_store),
        );
        let mut scopes = Vec::new();
        for tenant in ["tenant-a", "tenant-b", "tenant-c", "tenant-d", "tenant-e"] {
            let scope = GraphScope::new(
                root.child(NamespaceId::new(tenant).unwrap()).unwrap(),
                graph_id.clone(),
            );
            directory.register(&scope).await.unwrap();
            scopes.push(scope);
        }
        scopes.sort();
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(Arc::clone(&object_store), Arc::clone(&metrics), 1);
        let mut registered_scopes = RegisteredScopeSchedule::default();

        let first_cycle = run_registered_scopes_cycle(
            "graph/data",
            &directory,
            &mut registered_scopes,
            Arc::clone(&object_store),
            1,
            IndexBuildMode::Full,
            250_000,
            1,
            2,
            &cache,
            &metrics,
        )
        .await
        .unwrap();
        assert!(!first_cycle.full_sweep_completed);

        assert_eq!(metrics.scopes_processed.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.scopes_deferred.load(Ordering::Relaxed), 3);
        assert_eq!(registered_scopes.scopes.len(), 5);
        assert!(registered_scopes.cursor.is_some());
        let cursor = load_scope_cursor(
            &object_store,
            &scope_cursor_path("graph/data", directory.root_scope()),
        )
        .await
        .unwrap();
        let expected_scope = scopes[1].to_string();
        assert_eq!(cursor.last_scope.as_deref(), Some(expected_scope.as_str()));

        let mut completed = false;
        for _ in 0..2 {
            completed = run_registered_scopes_cycle(
                "graph/data",
                &directory,
                &mut registered_scopes,
                Arc::clone(&object_store),
                1,
                IndexBuildMode::Full,
                250_000,
                1,
                2,
                &cache,
                &metrics,
            )
            .await
            .unwrap()
            .full_sweep_completed;
        }
        assert!(completed);
        assert!(registered_scopes.scopes.is_empty());
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn scope_scheduler_rejects_unsafe_concurrency_without_panicking() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            root,
            GraphId::new("hydradb").unwrap(),
            Arc::clone(&object_store),
        );
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(Arc::clone(&object_store), Arc::clone(&metrics), 1);
        let mut registered_scopes = RegisteredScopeSchedule::default();

        for invalid in [0, MAX_SCOPE_CONCURRENCY + 1] {
            let failures = run_registered_scopes_cycle(
                "graph/data",
                &directory,
                &mut registered_scopes,
                Arc::clone(&object_store),
                1,
                IndexBuildMode::Full,
                250_000,
                invalid,
                1,
                &cache,
                &metrics,
            )
            .await
            .expect_err("unsafe concurrency must be rejected before chunking scopes");

            assert_eq!(
                failures.first().map(|failure| failure.stage),
                Some("scope_scheduler")
            );
        }
    }

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
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(Arc::clone(&object_store), Arc::clone(&metrics), 1);
        let mut registered_scopes = RegisteredScopeSchedule::default();

        run_registered_scopes_cycle(
            "graph/data",
            &directory,
            &mut registered_scopes,
            Arc::clone(&object_store),
            1,
            IndexBuildMode::Full,
            250_000,
            1,
            1,
            &cache,
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
            &mut registered_scopes,
            Arc::clone(&object_store),
            1,
            IndexBuildMode::Full,
            250_000,
            1,
            1,
            &cache,
            &metrics,
        )
        .await
        .unwrap();

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
                src: 2,
                dst: 3,
                idempotency_key: "indexer-scope-later-write".to_string(),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        run_registered_scopes_cycle(
            "graph/data",
            &directory,
            &mut registered_scopes,
            Arc::clone(&object_store),
            1,
            IndexBuildMode::Full,
            250_000,
            1,
            1,
            &cache,
            &metrics,
        )
        .await
        .unwrap();
        assert_eq!(
            metrics.dimensioned_snapshot(),
            BTreeMap::from([(
                ("cell-0".to_string(), "FOLLOWS".to_string()),
                DimensionedCounters {
                    generations_published: 2,
                    generation_failures: 0,
                    generations_deleted: 0,
                    // The warm cached reader must refresh before the second
                    // build, so it scans one edge and then both edges.
                    full_build_edges: 3,
                    incremental_delta_edges: 0,
                    incremental_fallbacks: 0,
                },
            )]),
            "the publish should be attributed to the cell and edge type that produced it",
        );
        assert_eq!(metrics.scope_cache_misses.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.scope_cache_hits.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 1);

        let reader = GraphCluster::open_cells_scoped(
            "graph/data",
            scope,
            ["cell-0"],
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        assert_eq!(
            reader
                .shard("cell-0")
                .unwrap()
                .current_graph_index("cell-0", "FOLLOWS")
                .await
                .unwrap()
                .map(|generation| generation.edge_count),
            Some(2)
        );
        reader.close().await.unwrap();
        cache.close().await.unwrap();
    }

    /// Every series a scrape sees when nothing has been indexed yet.
    ///
    /// Pinned in full rather than probed, because the point of this change is
    /// that the process-global half did *not* move: `ready` through
    /// `open_failures` and `last_success_ms` keep their exact names, their
    /// absence of labels and their positions, and the six families that carry
    /// labels still declare a `# TYPE` line with no samples under it.
    #[test]
    fn an_idle_indexer_declares_every_family() {
        let metrics = IndexerMetrics::default();
        metrics.ready.store(true, Ordering::Release);
        metrics.cycles.store(3, Ordering::Relaxed);

        assert_eq!(
            render_metrics(&metrics),
            concat!(
                "# TYPE graph_indexer_ready gauge\n",
                "graph_indexer_ready 1\n",
                "# TYPE graph_indexer_cycles counter\n",
                "graph_indexer_cycles 3\n",
                "# TYPE graph_indexer_successful_cycles counter\n",
                "graph_indexer_successful_cycles 0\n",
                "# TYPE graph_indexer_failed_cycles counter\n",
                "graph_indexer_failed_cycles 0\n",
                "# TYPE graph_indexer_full_sweeps counter\n",
                "graph_indexer_full_sweeps 0\n",
                "# TYPE graph_indexer_consecutive_failed_cycles gauge\n",
                "graph_indexer_consecutive_failed_cycles 0\n",
                "# TYPE graph_indexer_scopes_processed counter\n",
                "graph_indexer_scopes_processed 0\n",
                "# TYPE graph_indexer_scopes_deferred counter\n",
                "graph_indexer_scopes_deferred 0\n",
                "# TYPE graph_indexer_hot_scope_rechecks counter\n",
                "graph_indexer_hot_scope_rechecks 0\n",
                "# TYPE graph_indexer_open_failures counter\n",
                "graph_indexer_open_failures 0\n",
                "# TYPE graph_indexer_scope_cache_hits counter\n",
                "graph_indexer_scope_cache_hits 0\n",
                "# TYPE graph_indexer_scope_cache_misses counter\n",
                "graph_indexer_scope_cache_misses 0\n",
                "# TYPE graph_indexer_scope_cache_evictions counter\n",
                "graph_indexer_scope_cache_evictions 0\n",
                "# TYPE graph_indexer_scope_cache_close_failures counter\n",
                "graph_indexer_scope_cache_close_failures 0\n",
                "# TYPE graph_indexer_scope_cache_entries gauge\n",
                "graph_indexer_scope_cache_entries 0\n",
                "# TYPE graph_indexer_generations_published counter\n",
                "# TYPE graph_indexer_generation_failures counter\n",
                "# TYPE graph_indexer_generations_deleted counter\n",
                "# TYPE graph_indexer_full_build_edges counter\n",
                "# TYPE graph_indexer_incremental_delta_edges counter\n",
                "# TYPE graph_indexer_incremental_fallbacks counter\n",
                "# TYPE graph_indexer_dimensions gauge\n",
                "graph_indexer_dimensions 0\n",
                "# TYPE graph_indexer_last_success_ms gauge\n",
                "graph_indexer_last_success_ms 0\n",
                "# TYPE graph_indexer_last_full_sweep_ms gauge\n",
                "graph_indexer_last_full_sweep_ms 0\n",
            )
        );
    }

    #[test]
    fn the_three_failure_families_carry_cell_and_edge_type() {
        let metrics = IndexerMetrics::default();
        metrics.record_generation_published("cell-0", "FOLLOWS");
        metrics.record_generation_published("cell-1", "RELATES");
        metrics.record_generation_failure("cell-1", "RELATES");
        metrics.record_generations_deleted("cell-0", "FOLLOWS", 4);

        let output = render_metrics(&metrics);
        for expected in [
            "graph_indexer_generations_published{cell_id=\"cell-0\",edge_type=\"FOLLOWS\"} 1\n",
            "graph_indexer_generations_published{cell_id=\"cell-1\",edge_type=\"RELATES\"} 1\n",
            "graph_indexer_generation_failures{cell_id=\"cell-0\",edge_type=\"FOLLOWS\"} 0\n",
            "graph_indexer_generation_failures{cell_id=\"cell-1\",edge_type=\"RELATES\"} 1\n",
            "graph_indexer_generations_deleted{cell_id=\"cell-0\",edge_type=\"FOLLOWS\"} 4\n",
            "graph_indexer_dimensions 2\n",
        ] {
            assert!(
                output.contains(expected),
                "missing {expected:?} in\n{output}"
            );
        }
    }

    /// §1.3's rule, asserted against the exposition rather than trusted.
    ///
    /// `scope` is one value per tenant and unbounded by product decision, and
    /// the indexer sweeps every registered scope, so it is the single label a
    /// well-meaning change is most likely to add here. `MetricLabel` stops the
    /// declared route; this closes the undeclared one.
    #[test]
    fn no_indexer_series_carries_scope() {
        let metrics = IndexerMetrics::default();
        metrics.record_generation_published("cell-0", "FOLLOWS");

        let output = render_metrics(&metrics);
        assert!(!output.contains("scope="), "scope leaked into\n{output}");
        assert!(
            !output.contains(semconv::SCOPE),
            "scope leaked into\n{output}"
        );
    }

    #[test]
    fn the_two_dimensions_are_registry_metric_labels() {
        for (prometheus, label) in [CELL_ID_LABEL, EDGE_TYPE_LABEL] {
            assert!(
                semconv::METRIC_LABELS.contains(&label),
                "{label} is not classified as a metric label",
            );
            assert_eq!(
                label.key(),
                format!("hydradb.{prometheus}"),
                "the Prometheus name and the registry key have drifted",
            );
        }
    }

    /// Past the cap, attribution degrades and the totals do not.
    ///
    /// The second assertion is the one that matters: an operator who has
    /// saturated the budget still gets a correct `sum(...)` over the family,
    /// which is what every alert on these counters is built from.
    #[test]
    fn the_dimension_map_is_capped_and_the_totals_survive() {
        let metrics = IndexerMetrics::default();
        let recorded = MAX_DIMENSIONS + 64;
        for index in 0..recorded {
            metrics.record_generation_published("cell-0", &format!("EDGE-{index}"));
        }
        // Every pair already tracked keeps its own series, cap or no cap.
        metrics.record_generation_published("cell-0", "EDGE-0");

        let snapshot = metrics.dimensioned_snapshot();
        assert_eq!(
            snapshot.len(),
            MAX_DIMENSIONS + 1,
            "the map should hold the cap plus one overflow bucket",
        );
        assert_eq!(
            snapshot
                .values()
                .map(|counters| counters.generations_published)
                .sum::<u64>(),
            recorded as u64 + 1,
            "folding into the overflow bucket must not drop an increment",
        );
        assert_eq!(
            snapshot[&(OVERFLOW_LABEL.to_string(), OVERFLOW_LABEL.to_string())],
            DimensionedCounters {
                generations_published: 64,
                generation_failures: 0,
                generations_deleted: 0,
                full_build_edges: 0,
                incremental_delta_edges: 0,
                incremental_fallbacks: 0,
            },
        );
        assert_eq!(
            snapshot[&("cell-0".to_string(), "EDGE-0".to_string())].generations_published,
            2,
            "a tracked pair should keep counting after the cap is reached",
        );
        assert!(render_metrics(&metrics).contains(&format!(
            "graph_indexer_dimensions {}\n",
            MAX_DIMENSIONS + 1
        )));
    }

    #[test]
    fn label_values_are_escaped() {
        assert!(matches!(
            escape_label_value("cell-0"),
            Cow::Borrowed("cell-0")
        ));
        assert_eq!(escape_label_value("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
