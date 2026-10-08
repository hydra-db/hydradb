use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures::StreamExt;
use hydradb::{
    object_store_from_env, GraphCluster, GraphError, GraphId, GraphIndexBuildPath,
    GraphIndexGeneration, GraphLimits, GraphOpenOptions, GraphReaderMode, GraphScope,
    GraphScopeChange, NamespaceId, NamespacePath, ObjectStoreGraphScopeDirectory,
};
use hydradb_telemetry::{semconv, ErrorClass, Outcome, ServiceIdentity, TelemetryConfig};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStoreExt, PutMode, UpdateVersion};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex as AsyncMutex, Notify, Semaphore};
use tokio::task::JoinHandle;
use tracing::field::Empty;
use tracing::Instrument;

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_SCOPE_CONCURRENCY: usize = 8;
const MAX_SCOPE_CONCURRENCY: usize = 64;
const SCOPE_PROBE_CONCURRENCY_MULTIPLIER: usize = 2;
const MAX_SCOPE_PROBE_CONCURRENCY: usize = 128;
const DEFAULT_SCOPES_PER_CYCLE: usize = 256;
const MAX_SCOPES_PER_CYCLE: usize = 16_384;
const MIN_SCOPE_CURSOR_CHECKPOINT_SCOPES: usize = 64;
const DEFAULT_MAX_OPEN_SCOPES: usize = 128;
const MAX_OPEN_SCOPES: usize = 16_384;
const DEFAULT_DIRTY_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MIN_DIRTY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_DIRTY_POLL_INTERVAL: Duration = Duration::from_secs(60);
const CHANGE_FAILURE_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const INDEXER_READER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

fn indexer_open_options(
    max_wal_tail_files: u64,
    slatedb_cache_bytes: usize,
    canonical_vertex_membership: bool,
) -> GraphOpenOptions {
    let mut options = GraphOpenOptions::default();
    options.unstable_canonical_vertex_membership = canonical_vertex_membership;
    options.limits = GraphLimits {
        max_wal_tail_files,
        ..GraphLimits::default()
    };
    // Cached indexer readers explicitly refresh before every build. Keeping a
    // managed checkpoint per cached scope creates thousands of periodic S3
    // writes, while frequent background polls duplicate the explicit refresh.
    // FollowLatest makes these readers read-only at the object-store level;
    // failed reads invalidate the scope and reopen it on the next pass.
    options.reader_mode = GraphReaderMode::FollowLatest;
    options.reader_manifest_poll_interval = INDEXER_READER_MANIFEST_POLL_INTERVAL;
    options.cache.slatedb_cache_bytes = slatedb_cache_bytes;
    options
}

fn parse_canonical_vertex_membership(value: &str) -> RuntimeResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(
            format!("invalid boolean GRAPH_UNSTABLE_CANONICAL_VERTEX_MEMBERSHIP={other}").into(),
        ),
    }
}
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
    covered_sequences: BTreeMap<String, u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScopeCacheDisposition {
    Hit,
    Admitted,
    Bypassed,
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
    scope_run_gates: AsyncMutex<BTreeMap<GraphScope, Weak<AsyncMutex<()>>>>,
    scope_permits: Semaphore,
    metrics: Arc<IndexerMetrics>,
}

impl IndexerScopeCache {
    fn new(
        data_path: String,
        cells: Vec<String>,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        open_options: GraphOpenOptions,
        max_open_scopes: usize,
        scope_concurrency: usize,
        metrics: Arc<IndexerMetrics>,
    ) -> Self {
        metrics
            .scope_cache_capacity
            .store(max_open_scopes as u64, Ordering::Release);
        Self {
            data_path,
            cells,
            object_store,
            open_options,
            max_open_scopes,
            access_clock: AtomicU64::new(0),
            clusters: AsyncMutex::new(BTreeMap::new()),
            scope_run_gates: AsyncMutex::new(BTreeMap::new()),
            scope_permits: Semaphore::new(scope_concurrency),
            metrics,
        }
    }

    async fn scope_run_gate(&self, scope: &GraphScope) -> Arc<AsyncMutex<()>> {
        let mut gates = self.scope_run_gates.lock().await;
        if let Some(gate) = gates.get(scope).and_then(Weak::upgrade) {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(scope.clone(), Arc::downgrade(&gate));
        gate
    }

    async fn resident_cluster_for_scope(&self, scope: &GraphScope) -> Option<Arc<GraphCluster>> {
        let access = self.access_clock.fetch_add(1, Ordering::Relaxed) + 1;
        let mut clusters = self.clusters.lock().await;
        let entry = clusters.get_mut(scope)?;
        entry.last_used = access;
        self.metrics
            .scope_cache_hits
            .fetch_add(1, Ordering::Relaxed);
        Some(Arc::clone(&entry.cluster))
    }

    async fn covered_sequences_for_scope(
        &self,
        scope: &GraphScope,
        expected: &Arc<GraphCluster>,
    ) -> BTreeMap<String, u64> {
        let clusters = self.clusters.lock().await;
        clusters
            .get(scope)
            .filter(|entry| Arc::ptr_eq(&entry.cluster, expected))
            .map(|entry| entry.covered_sequences.clone())
            .unwrap_or_default()
    }

    async fn update_covered_sequences(
        &self,
        scope: &GraphScope,
        expected: &Arc<GraphCluster>,
        covered_sequences: &BTreeMap<String, u64>,
    ) {
        let mut clusters = self.clusters.lock().await;
        let Some(entry) = clusters
            .get_mut(scope)
            .filter(|entry| Arc::ptr_eq(&entry.cluster, expected))
        else {
            return;
        };
        for (cell_id, sequence) in covered_sequences {
            entry.covered_sequences.insert(cell_id.clone(), *sequence);
        }
    }

    async fn cluster_for_scope(
        &self,
        scope: &GraphScope,
    ) -> hydradb::Result<(Arc<GraphCluster>, ScopeCacheDisposition)> {
        if let Some(cluster) = self.resident_cluster_for_scope(scope).await {
            return Ok((cluster, ScopeCacheDisposition::Hit));
        }

        let access = self.access_clock.fetch_add(1, Ordering::Relaxed) + 1;
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

        let (selected, disposition) = {
            let mut clusters = self.clusters.lock().await;
            if let Some(entry) = clusters.get_mut(scope) {
                entry.last_used = access;
                (Arc::clone(&entry.cluster), ScopeCacheDisposition::Hit)
            } else if clusters.len() >= self.max_open_scopes {
                self.metrics
                    .scope_cache_admission_bypasses
                    .fetch_add(1, Ordering::Relaxed);
                (Arc::clone(&opened), ScopeCacheDisposition::Bypassed)
            } else {
                clusters.insert(
                    scope.clone(),
                    CachedScopeCluster {
                        cluster: Arc::clone(&opened),
                        last_used: access,
                        hot: false,
                        idle_rechecks: 0,
                        covered_sequences: BTreeMap::new(),
                    },
                );
                self.metrics
                    .scope_cache_entries
                    .store(clusters.len() as u64, Ordering::Release);
                (Arc::clone(&opened), ScopeCacheDisposition::Admitted)
            }
        };

        if !Arc::ptr_eq(&selected, &opened) {
            if let Err(error) = opened.close().await {
                self.record_close_failure(scope, "duplicate_open", &error);
            }
        }
        Ok((selected, disposition))
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
        disposition: ScopeCacheDisposition,
        built: bool,
    ) {
        if disposition == ScopeCacheDisposition::Bypassed {
            if built {
                self.admit_built_scope(scope, expected).await;
            } else if let Err(error) = expected.close().await {
                self.record_close_failure(scope, "admission_bypassed", &error);
            }
            return;
        }

        let mut demoted = false;
        {
            let mut clusters = self.clusters.lock().await;
            match clusters.get_mut(scope) {
                Some(entry) if Arc::ptr_eq(&entry.cluster, expected) && built => {
                    entry.hot = true;
                    entry.idle_rechecks = 0;
                }
                Some(entry)
                    if Arc::ptr_eq(&entry.cluster, expected)
                        && disposition == ScopeCacheDisposition::Hit
                        && entry.hot =>
                {
                    entry.idle_rechecks = entry.idle_rechecks.saturating_add(1);
                    if entry.idle_rechecks >= HOT_SCOPE_IDLE_RECHECKS {
                        entry.hot = false;
                        entry.idle_rechecks = 0;
                        demoted = true;
                    }
                }
                _ => {}
            }
        }
        if demoted {
            self.metrics
                .scope_cache_idle_demotions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn finish_failed_scope(
        &self,
        scope: &GraphScope,
        expected: &Arc<GraphCluster>,
        disposition: ScopeCacheDisposition,
    ) -> hydradb::Result<()> {
        if disposition == ScopeCacheDisposition::Bypassed {
            expected.close().await
        } else {
            self.invalidate(scope, expected).await
        }
    }

    async fn admit_built_scope(&self, scope: &GraphScope, expected: &Arc<GraphCluster>) {
        let access = self.access_clock.fetch_add(1, Ordering::Relaxed) + 1;
        let mut evicted = None;
        let admitted = {
            let mut clusters = self.clusters.lock().await;
            if let Some(entry) = clusters.get_mut(scope) {
                entry.last_used = access;
                entry.hot = true;
                entry.idle_rechecks = 0;
                false
            } else {
                if clusters.len() >= self.max_open_scopes {
                    let candidate = clusters
                        .iter()
                        .filter(|(_, entry)| Arc::strong_count(&entry.cluster) == 1)
                        .min_by_key(|(_, entry)| (entry.hot, entry.last_used))
                        .map(|(scope, _)| scope.clone());
                    if let Some(candidate) = candidate {
                        evicted = clusters.remove(&candidate).map(|entry| entry.cluster);
                        self.metrics
                            .scope_cache_evictions
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                if clusters.len() >= self.max_open_scopes {
                    false
                } else {
                    clusters.insert(
                        scope.clone(),
                        CachedScopeCluster {
                            cluster: Arc::clone(expected),
                            last_used: access,
                            hot: true,
                            idle_rechecks: 0,
                            covered_sequences: BTreeMap::new(),
                        },
                    );
                    self.metrics
                        .scope_cache_entries
                        .store(clusters.len() as u64, Ordering::Release);
                    self.metrics
                        .scope_cache_promotions
                        .fetch_add(1, Ordering::Relaxed);
                    true
                }
            }
        };

        if !admitted {
            if let Err(error) = expected.close().await {
                self.record_close_failure(scope, "promotion_bypassed", &error);
            }
        }
        if let Some(cluster) = evicted {
            if let Err(error) = cluster.close().await {
                self.record_close_failure(scope, "promotion_eviction", &error);
            }
        }
    }

    async fn invalidate(
        &self,
        scope: &GraphScope,
        expected: &Arc<GraphCluster>,
    ) -> hydradb::Result<()> {
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

    async fn invalidate_scope(&self, scope: &GraphScope) -> hydradb::Result<()> {
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

    async fn close(&self) -> hydradb::Result<()> {
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

#[derive(Debug, Default)]
struct IndexCycleOutcome {
    built: bool,
    covered_sequences: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
struct RegisteredScopeOutcome {
    failures: CycleFailures,
    covered_sequences: BTreeMap<String, u64>,
    empty: bool,
}

struct FairScopeRun {
    failures: CycleFailures,
    cursor: Option<ScopeCursor>,
    ending_cursor: Option<String>,
    cursor_progressed: bool,
}

struct ScopeChangeRun {
    scope: GraphScope,
    already_covered: Vec<GraphScopeChange>,
    pending: Vec<GraphScopeChange>,
    outcome: Option<RegisteredScopeOutcome>,
    coverage_error: Option<GraphError>,
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
    scope_cache_admission_bypasses: AtomicU64,
    scope_cache_promotions: AtomicU64,
    scope_cache_idle_demotions: AtomicU64,
    scope_cache_close_failures: AtomicU64,
    scope_cache_entries: AtomicU64,
    scope_cache_capacity: AtomicU64,
    registered_scopes: AtomicU64,
    scope_cache_last_warned_shortfall: AtomicU64,
    change_notifications_observed: AtomicU64,
    change_notifications_cleared: AtomicU64,
    changed_scopes_processed: AtomicU64,
    change_notification_failures: AtomicU64,
    change_push_wakes: AtomicU64,
    change_push_rejections: AtomicU64,
    change_push_throttles: AtomicU64,
    pending_change_notifications: AtomicU64,
    last_change_success_ms: AtomicU64,
    last_change_lag_ms: AtomicU64,
    stage_timings: Mutex<BTreeMap<(IndexWorkKind, IndexStage), StageTiming>>,
    /// `generations_published`, `generation_failures` and `generations_deleted`,
    /// keyed by `{cell_id, edge_type}`.
    ///
    /// The counters above stay process-global because they describe the
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum IndexWorkKind {
    Change,
    Sweep,
}

impl IndexWorkKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Change => "change",
            Self::Sweep => "sweep",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum IndexStage {
    Pass,
    ListChanges,
    ScopeTotal,
    ScopeQueue,
    ScopeHasData,
    ClusterOpen,
    RefreshSequence,
    DiscoverDirty,
    ReadCurrent,
    ArtifactBuild,
    ArtifactGc,
    XlogGc,
    ClearChanges,
}

impl IndexStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::ListChanges => "list_changes",
            Self::ScopeTotal => "scope_total",
            Self::ScopeQueue => "scope_queue",
            Self::ScopeHasData => "scope_has_data",
            Self::ClusterOpen => "cluster_open",
            Self::RefreshSequence => "refresh_sequence",
            Self::DiscoverDirty => "discover_dirty",
            Self::ReadCurrent => "read_current",
            Self::ArtifactBuild => "artifact_build",
            Self::ArtifactGc => "artifact_gc",
            Self::XlogGc => "xlog_gc",
            Self::ClearChanges => "clear_changes",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct StageTiming {
    count: u64,
    elapsed_micros: u64,
    max_micros: u64,
}

struct StageTimer<'a> {
    metrics: &'a IndexerMetrics,
    work: IndexWorkKind,
    stage: IndexStage,
    started: Instant,
}

impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .record_stage(self.work, self.stage, self.started.elapsed());
    }
}

impl IndexerMetrics {
    fn time_stage(&self, work: IndexWorkKind, stage: IndexStage) -> StageTimer<'_> {
        StageTimer {
            metrics: self,
            work,
            stage,
            started: Instant::now(),
        }
    }

    fn record_stage(&self, work: IndexWorkKind, stage: IndexStage, elapsed: Duration) {
        let elapsed_micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        let mut timings = self
            .stage_timings
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let timing = timings.entry((work, stage)).or_default();
        timing.count = timing.count.saturating_add(1);
        timing.elapsed_micros = timing.elapsed_micros.saturating_add(elapsed_micros);
        timing.max_micros = timing.max_micros.max(elapsed_micros);
    }

    fn stage_timings_snapshot(&self) -> BTreeMap<(IndexWorkKind, IndexStage), StageTiming> {
        self.stage_timings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn record_scope_cache_population(
        &self,
        registered_scopes: usize,
        capacity: usize,
    ) -> Option<usize> {
        self.registered_scopes
            .store(registered_scopes as u64, Ordering::Release);
        let shortfall = registered_scopes.saturating_sub(capacity);
        let previous = self
            .scope_cache_last_warned_shortfall
            .swap(shortfall as u64, Ordering::AcqRel);
        (shortfall > 0 && previous != shortfall as u64).then_some(shortfall)
    }

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

#[derive(Clone)]
struct IndexerAdminState {
    metrics: Arc<IndexerMetrics>,
    change_wake: Arc<Notify>,
    wake_token: Arc<str>,
    wake_limiter: Arc<WakeRateLimiter>,
}

struct WakeRateLimiter {
    min_interval: Duration,
    last_accepted: Mutex<Option<Instant>>,
}

impl WakeRateLimiter {
    fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_accepted: Mutex::new(None),
        }
    }

    fn try_accept(&self) -> bool {
        let now = Instant::now();
        let mut last_accepted = self
            .last_accepted
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if last_accepted
            .as_ref()
            .is_some_and(|last| now.duration_since(*last) < self.min_interval)
        {
            return false;
        }
        *last_accepted = Some(now);
        true
    }
}

#[tokio::main]
async fn main() -> RuntimeResult<()> {
    // Before the subscriber: see `build_info::version_flag_requested`.
    if hydradb_telemetry::build_info::version_flag_requested() {
        println!(
            "{} {}",
            ServiceIdentity::GraphIndexer.binary(),
            hydradb_telemetry::BUILD_INFO.long_version()
        );
        return Ok(());
    }

    // One subscriber for both binaries, so a shared log sink separates a node
    // line from an indexer line by field rather than by message text. `init` is
    // total: with `OTEL_EXPORTER_OTLP_ENDPOINT` unset it installs the fmt layer
    // alone and returns `Ok`, so a missing collector never stops the indexer
    // booting. The guard is held to the end of `main` because dropping it is
    // what flushes batched spans and logs — without that flush the last seconds
    // before a pod restart are lost, which is exactly the window that matters.
    let telemetry =
        hydradb_telemetry::init(TelemetryConfig::from_env(ServiceIdentity::GraphIndexer))?;
    hydradb_telemetry::build_info::log();

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
    let dirty_poll_interval = Duration::from_millis(
        env_value(
            "GRAPH_INDEXER_DIRTY_POLL_INTERVAL_MS",
            &DEFAULT_DIRTY_POLL_INTERVAL.as_millis().to_string(),
        )
        .parse::<u64>()?,
    );
    if !(MIN_DIRTY_POLL_INTERVAL..=MAX_DIRTY_POLL_INTERVAL).contains(&dirty_poll_interval) {
        return Err(format!(
            "GRAPH_INDEXER_DIRTY_POLL_INTERVAL_MS must be between {} and {}",
            MIN_DIRTY_POLL_INTERVAL.as_millis(),
            MAX_DIRTY_POLL_INTERVAL.as_millis()
        )
        .into());
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
    let slatedb_cache_bytes = env_value(
        "GRAPH_SLATE_DB_CACHE_BYTES",
        &(640usize * 1024 * 1024).to_string(),
    )
    .parse::<usize>()?;
    let canonical_vertex_membership = parse_canonical_vertex_membership(&env_value(
        "GRAPH_UNSTABLE_CANONICAL_VERTEX_MEMBERSHIP",
        "false",
    ))?;
    let open_options = indexer_open_options(
        max_wal_tail_files,
        slatedb_cache_bytes,
        canonical_vertex_membership,
    );
    let admin_addr = env_value("GRAPH_INDEXER_ADMIN_ADDR", "0.0.0.0:9091").parse::<SocketAddr>()?;
    let wake_token = read_indexer_wake_token()?;

    let metrics = Arc::new(IndexerMetrics::default());
    let change_wake = Arc::new(Notify::new());
    let admin = IndexerAdminServer::bind(
        admin_addr,
        Arc::clone(&metrics),
        Arc::clone(&change_wake),
        wake_token,
        dirty_poll_interval,
    )
    .await?;
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
        scope_concurrency,
        Arc::clone(&metrics),
    ));
    let (scope_change_stop, scope_change_task) = start_scope_change_worker(
        scope_directory.clone(),
        retain_previous,
        build_mode,
        incremental_min_edges,
        scope_concurrency,
        dirty_poll_interval,
        Arc::clone(&scope_cache),
        Arc::clone(&metrics),
        change_wake,
    );
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
        dirty_poll_interval_ms = dirty_poll_interval.as_millis() as u64,
        reader_mode = open_options.reader_mode.as_str(),
        reader_manifest_poll_interval_ms = open_options
            .reader_manifest_poll_interval
            .as_millis() as u64,
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

        let mut continue_sweep_immediately = false;
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
                } else {
                    continue_sweep_immediately = true;
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
            _ = tokio::time::sleep(if continue_sweep_immediately {
                Duration::ZERO
            } else {
                interval
            }) => {}
        }
    }
    metrics.ready.store(false, Ordering::Release);
    let _ = scope_change_stop.send(true);
    scope_change_task.await??;
    scope_cache.close().await?;
    admin.stop().await?;
    tracing::info!(scope = %root_scope, "graph indexer stopped");
    telemetry.shutdown();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn start_scope_change_worker(
    scope_directory: ObjectStoreGraphScopeDirectory,
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    scope_concurrency: usize,
    poll_interval: Duration,
    scope_cache: Arc<IndexerScopeCache>,
    metrics: Arc<IndexerMetrics>,
    change_wake: Arc<Notify>,
) -> (watch::Sender<bool>, JoinHandle<RuntimeResult<()>>) {
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            let failures = run_scope_change_pass(
                &scope_directory,
                retain_previous,
                build_mode,
                incremental_min_edges,
                scope_concurrency,
                &scope_cache,
                &metrics,
            )
            .await;
            let retry_delay = if failures.is_empty() {
                poll_interval
            } else {
                metrics
                    .change_notification_failures
                    .fetch_add(failures.len() as u64, Ordering::Relaxed);
                tracing::warn!(
                    failure_count = failures.len(),
                    error = %failures,
                    "graph index change fast lane failed; fair sweep will retry"
                );
                poll_interval.max(CHANGE_FAILURE_RETRY_INTERVAL)
            };

            if failures.is_empty() {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return Ok(());
                        }
                    }
                    _ = change_wake.notified() => {}
                    _ = tokio::time::sleep(retry_delay) => {}
                }
            } else {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(retry_delay) => {}
                }
            }
        }
    });
    (stop_tx, task)
}

#[allow(clippy::too_many_arguments)]
async fn run_scope_change_pass(
    scope_directory: &ObjectStoreGraphScopeDirectory,
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    scope_concurrency: usize,
    scope_cache: &IndexerScopeCache,
    metrics: &IndexerMetrics,
) -> CycleFailures {
    let _pass_timer = metrics.time_stage(IndexWorkKind::Change, IndexStage::Pass);
    let list_timer = metrics.time_stage(IndexWorkKind::Change, IndexStage::ListChanges);
    let changes_result = scope_directory.list_changes().await;
    drop(list_timer);
    let changes = match changes_result {
        Ok(changes) => changes,
        Err(error) => {
            let mut failures = CycleFailures::default();
            failures.push(IndexFailure::kernel(
                "scope_change_discovery",
                "list changed graph scopes".to_string(),
                &error,
            ));
            return failures;
        }
    };
    metrics
        .pending_change_notifications
        .store(changes.len() as u64, Ordering::Release);
    if changes.is_empty() {
        return CycleFailures::default();
    }
    metrics
        .change_notifications_observed
        .fetch_add(changes.len() as u64, Ordering::Relaxed);

    let mut grouped = BTreeMap::<GraphScope, Vec<GraphScopeChange>>::new();
    for change in changes {
        grouped
            .entry(change.scope.clone())
            .or_default()
            .push(change);
    }

    let runs = futures::stream::iter(grouped)
        .map(|(scope, changes)| async move {
            let (already_covered, pending) =
                match partition_scope_changes(scope_directory, &changes).await {
                    Ok(partition) => partition,
                    Err(error) => {
                        return ScopeChangeRun {
                            scope,
                            already_covered: Vec::new(),
                            pending: changes,
                            outcome: None,
                            coverage_error: Some(error),
                        };
                    }
                };
            let outcome = if pending.is_empty() {
                None
            } else {
                Some(
                    run_registered_scope(
                        scope.clone(),
                        retain_previous,
                        build_mode,
                        incremental_min_edges,
                        scope_cache,
                        metrics,
                        IndexWorkKind::Change,
                    )
                    .await,
                )
            };
            ScopeChangeRun {
                scope,
                already_covered,
                pending,
                outcome,
                coverage_error: None,
            }
        })
        .buffer_unordered(scope_concurrency);
    let mut runs = std::pin::pin!(runs);
    let mut failures = CycleFailures::default();
    let mut cleared = 0u64;
    while let Some(run) = runs.next().await {
        let ScopeChangeRun {
            scope,
            mut already_covered,
            pending,
            outcome,
            coverage_error,
        } = run;
        if let Some(error) = coverage_error {
            failures.push(
                IndexFailure::kernel(
                    "scope_change_coverage_read",
                    format!("read graph index change coverage for {scope}"),
                    &error,
                )
                .with_scope(&scope.to_string()),
            );
            continue;
        }

        let mut processed = false;
        if let Some(outcome) = outcome {
            if !outcome.failures.is_empty() {
                failures.absorb(outcome.failures);
            } else {
                processed = true;
                let coverage = if outcome.empty {
                    maximum_hint_sequences(&pending)
                } else {
                    outcome.covered_sequences
                };
                let mut acknowledged = BTreeMap::new();
                for (cell_id, sequence) in coverage {
                    match scope_directory
                        .acknowledge_sequence(&scope, &cell_id, sequence)
                        .await
                    {
                        Ok(()) => {
                            acknowledged.insert(cell_id, sequence);
                        }
                        Err(error) => failures.push(
                            IndexFailure::kernel(
                                "scope_change_coverage_write",
                                format!(
                                    "persist graph index change coverage for {scope}/{cell_id}"
                                ),
                                &error,
                            )
                            .with_scope(&scope.to_string())
                            .with_cell(&cell_id),
                        ),
                    }
                }
                already_covered.extend(pending.into_iter().filter(|change| {
                    match (&change.cell_id, change.sequence) {
                        (Some(cell_id), Some(sequence)) => acknowledged
                            .get(cell_id)
                            .is_some_and(|covered| *covered >= sequence),
                        (None, None) => true,
                        _ => false,
                    }
                }));
            }
        }

        if !already_covered.is_empty() {
            let clear_timer = metrics.time_stage(IndexWorkKind::Change, IndexStage::ClearChanges);
            let clear_result = scope_directory.clear_changes(&already_covered).await;
            drop(clear_timer);
            if let Err(error) = clear_result {
                failures.push(
                    IndexFailure::kernel(
                        "scope_change_clear",
                        format!("clear graph index change notifications for {scope}"),
                        &error,
                    )
                    .with_scope(&scope.to_string()),
                );
                continue;
            }
            cleared = cleared.saturating_add(already_covered.len() as u64);
        }

        if processed {
            let completed_at = unix_time_ms();
            let oldest = already_covered
                .iter()
                .map(|change| change.created_at.timestamp_millis().max(0) as u64)
                .min()
                .unwrap_or(completed_at);
            metrics
                .last_change_success_ms
                .store(completed_at, Ordering::Relaxed);
            metrics
                .last_change_lag_ms
                .store(completed_at.saturating_sub(oldest), Ordering::Relaxed);
            metrics
                .changed_scopes_processed
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    metrics
        .change_notifications_cleared
        .fetch_add(cleared, Ordering::Relaxed);
    metrics
        .pending_change_notifications
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
            Some(pending.saturating_sub(cleared))
        })
        .ok();
    failures
}

async fn partition_scope_changes(
    scope_directory: &ObjectStoreGraphScopeDirectory,
    changes: &[GraphScopeChange],
) -> hydradb::Result<(Vec<GraphScopeChange>, Vec<GraphScopeChange>)> {
    let mut watermarks = BTreeMap::<String, Option<u64>>::new();
    for change in changes {
        if let Some(cell_id) = change.cell_id.as_ref() {
            if !watermarks.contains_key(cell_id) {
                watermarks.insert(
                    cell_id.clone(),
                    scope_directory
                        .covered_sequence(&change.scope, cell_id)
                        .await?,
                );
            }
        }
    }
    let mut covered = Vec::new();
    let mut pending = Vec::new();
    for change in changes.iter().cloned() {
        let is_covered = match (&change.cell_id, change.sequence) {
            (Some(cell_id), Some(sequence)) => watermarks
                .get(cell_id)
                .copied()
                .flatten()
                .is_some_and(|covered| covered >= sequence),
            _ => false,
        };
        if is_covered {
            covered.push(change);
        } else {
            pending.push(change);
        }
    }
    Ok((covered, pending))
}

fn maximum_hint_sequences(changes: &[GraphScopeChange]) -> BTreeMap<String, u64> {
    let mut maximum = BTreeMap::new();
    for change in changes {
        if let (Some(cell_id), Some(sequence)) = (&change.cell_id, change.sequence) {
            maximum
                .entry(cell_id.clone())
                .and_modify(|current: &mut u64| *current = (*current).max(sequence))
                .or_insert(sequence);
        }
    }
    maximum
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
        if let Some(capacity_shortfall) = metrics
            .record_scope_cache_population(schedule.scopes.len(), scope_cache.max_open_scopes)
        {
            tracing::warn!(
                registered_scopes = schedule.scopes.len(),
                scope_cache_capacity = scope_cache.max_open_scopes,
                capacity_shortfall,
                "indexer scope cache cannot retain every registered scope"
            );
        }

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
    let cursor = Some(cursor);

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

    let FairScopeRun {
        failures: fair_failures,
        cursor,
        ending_cursor,
        cursor_progressed,
    } = run_fair_scope_stream(
        &scopes,
        retain_previous,
        build_mode,
        incremental_min_edges,
        scope_cache,
        metrics,
        scope_concurrency,
        &object_store,
        &cursor_path,
        cursor,
    )
    .await;
    failures.absorb(fair_failures);

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
                IndexWorkKind::Sweep,
            )
        })
        .buffer_unordered(scope_concurrency);
    let mut runs = std::pin::pin!(runs);
    while let Some(scope_outcome) = runs.next().await {
        failures.absorb(scope_outcome.failures);
    }
    failures
}

#[allow(clippy::too_many_arguments)]
async fn run_fair_scope_stream(
    scopes: &[GraphScope],
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    scope_cache: &IndexerScopeCache,
    metrics: &IndexerMetrics,
    scope_concurrency: usize,
    object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
    cursor_path: &Path,
    mut cursor: Option<ScopeCursor>,
) -> FairScopeRun {
    let mut failures = CycleFailures::default();
    let mut ending_cursor = cursor
        .as_ref()
        .and_then(|current| current.last_scope.clone());
    let mut cursor_progressed = false;
    if scopes.is_empty() {
        return FairScopeRun {
            failures,
            cursor,
            ending_cursor,
            cursor_progressed,
        };
    }

    // Most retained scopes only need one read-only reader refresh to prove that
    // their previously covered storage sequence is still current. Keep that
    // high-latency object-store probe wider than the build gate; dirty scopes
    // still acquire `scope_permits` before discovery or GraphBLAS work.
    let probe_concurrency = scope_probe_concurrency(scope_concurrency);
    let runs = futures::stream::iter(scopes.iter().cloned().enumerate())
        .map(|(index, scope)| async move {
            (
                index,
                run_registered_scope(
                    scope,
                    retain_previous,
                    build_mode,
                    incremental_min_edges,
                    scope_cache,
                    metrics,
                    IndexWorkKind::Sweep,
                )
                .await,
            )
        })
        .buffer_unordered(probe_concurrency);
    let mut runs = std::pin::pin!(runs);
    let mut completed = vec![false; scopes.len()];
    let mut contiguous_completed = 0usize;
    let checkpoint_stride = scope_concurrency.max(MIN_SCOPE_CURSOR_CHECKPOINT_SCOPES);
    let mut next_checkpoint = Some(checkpoint_stride.min(scopes.len()));

    'results: while let Some((index, scope_outcome)) = runs.next().await {
        failures.absorb(scope_outcome.failures);
        completed[index] = true;
        while contiguous_completed < completed.len() && completed[contiguous_completed] {
            contiguous_completed += 1;
        }

        while let Some(checkpoint) =
            next_checkpoint.filter(|checkpoint| contiguous_completed >= *checkpoint)
        {
            let last_scope = scopes[checkpoint - 1].to_string();
            match advance_scope_cursor(
                object_store,
                cursor_path,
                cursor
                    .take()
                    .expect("a valid cursor must precede every fair-scan checkpoint"),
                &last_scope,
            )
            .await
            {
                Ok(ScopeCursorAdvance::Advanced(next)) => {
                    ending_cursor.clone_from(&next.last_scope);
                    cursor = Some(next);
                    cursor_progressed = true;
                    next_checkpoint = (checkpoint < scopes.len()).then(|| {
                        checkpoint
                            .saturating_add(checkpoint_stride)
                            .min(scopes.len())
                    });
                }
                Ok(ScopeCursorAdvance::LostRace) => {
                    tracing::info!(
                        cursor = %cursor_path,
                        last_scope,
                        "another indexer advanced the scope cursor"
                    );
                    break 'results;
                }
                Err(error) => {
                    failures.push(IndexFailure::kernel(
                        "scope_cursor_advance",
                        format!("advance indexer scope cursor {cursor_path}"),
                        &error,
                    ));
                    break 'results;
                }
            }
        }
    }

    FairScopeRun {
        failures,
        cursor,
        ending_cursor,
        cursor_progressed,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_registered_scope(
    scope: GraphScope,
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    scope_cache: &IndexerScopeCache,
    metrics: &IndexerMetrics,
    work: IndexWorkKind,
) -> RegisteredScopeOutcome {
    let _scope_timer = metrics.time_stage(work, IndexStage::ScopeTotal);
    // A changed-scope hint can overlap the fair recovery sweep. Serialize the
    // same scope first. The global build permit is acquired only after a fair
    // sweep proves that the scope changed, so remote refresh latency does not
    // occupy scarce GraphBLAS/build capacity.
    let queue_timer = metrics.time_stage(work, IndexStage::ScopeQueue);
    let run_gate = scope_cache.scope_run_gate(&scope).await;
    let _run_guard = run_gate.lock().await;
    drop(queue_timer);
    let mut failures = CycleFailures::default();
    let mut covered_sequences = BTreeMap::new();
    let mut refreshed_sequences = BTreeMap::new();
    {
        let scope_name = scope.to_string();
        let scope_span = tracing::info_span!(
            "index.scope",
            hydradb.scope = %scope_name,
            hydradb.outcome = Empty,
            error.class = Empty,
        );

        let cached_cluster = scope_cache.resident_cluster_for_scope(&scope).await;
        if cached_cluster.is_none() {
            let has_data_span = tracing::info_span!(
                parent: &scope_span,
                "index.scope_has_data",
                hydradb.scope = %scope_name,
                has_data = Empty,
                hydradb.outcome = Empty,
                error.class = Empty,
            );
            let has_data_timer = metrics.time_stage(work, IndexStage::ScopeHasData);
            let has_data_result = scope_has_data(
                &scope_cache.data_path,
                &scope,
                &scope_cache.cells,
                &scope_cache.object_store,
            )
            .instrument(has_data_span.clone())
            .await;
            drop(has_data_timer);
            match has_data_result {
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
                    return RegisteredScopeOutcome {
                        failures,
                        empty: true,
                        ..RegisteredScopeOutcome::default()
                    };
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
                    return RegisteredScopeOutcome {
                        failures,
                        ..RegisteredScopeOutcome::default()
                    };
                }
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
        let open_timer = metrics.time_stage(work, IndexStage::ClusterOpen);
        let cluster_result = if let Some(cluster) = cached_cluster {
            Ok((cluster, ScopeCacheDisposition::Hit))
        } else {
            scope_cache
                .cluster_for_scope(&scope)
                .instrument(open_span.clone())
                .await
        };
        drop(open_timer);
        let (cluster, cache_disposition) = match cluster_result {
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
                return RegisteredScopeOutcome {
                    failures,
                    ..RegisteredScopeOutcome::default()
                };
            }
        };

        let mut scope_failed = false;
        let mut scope_built = false;
        let known_covered_sequences = scope_cache
            .covered_sequences_for_scope(&scope, &cluster)
            .await;

        if work == IndexWorkKind::Sweep && known_covered_sequences.len() == scope_cache.cells.len()
        {
            match refresh_covered_scope(&cluster, &scope_name, &scope_cache.cells, metrics, work)
                .instrument(scope_span.clone())
                .await
            {
                Ok(sequences)
                    if sequences.iter().all(|(cell_id, sequence)| {
                        known_covered_sequences.get(cell_id) == Some(sequence)
                    }) =>
                {
                    covered_sequences = sequences;
                    scope_cache
                        .finish_scope(&scope, &cluster, cache_disposition, false)
                        .await;
                    scope_cache
                        .update_covered_sequences(&scope, &cluster, &covered_sequences)
                        .await;
                    scope_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
                    return RegisteredScopeOutcome {
                        failures,
                        covered_sequences,
                        empty: false,
                    };
                }
                Ok(sequences) => refreshed_sequences = sequences,
                Err(inner) => {
                    failures.absorb(inner);
                    if let Err(error) = scope_cache
                        .finish_failed_scope(&scope, &cluster, cache_disposition)
                        .await
                    {
                        failures.push(
                            IndexFailure::kernel(
                                "cluster_close",
                                format!("close failed scope {scope_name}"),
                                &error,
                            )
                            .with_scope(&scope_name),
                        );
                    }
                    scope_span.record(semconv::OUTCOME, Outcome::Failed.as_str());
                    return RegisteredScopeOutcome {
                        failures,
                        covered_sequences,
                        empty: false,
                    };
                }
            }
        }

        let build_queue_timer = metrics.time_stage(work, IndexStage::ScopeQueue);
        let _permit = scope_cache
            .scope_permits
            .acquire()
            .await
            .expect("the indexer scope semaphore remains open for the runtime");
        drop(build_queue_timer);
        // Instrumenting the call rather than parenting a span by hand is what
        // makes every `index.cell` below a child of this scope.
        match run_index_cycle(
            &cluster,
            &scope_name,
            &scope_cache.cells,
            retain_previous,
            build_mode,
            incremental_min_edges,
            &known_covered_sequences,
            &refreshed_sequences,
            metrics,
            work,
        )
        .instrument(scope_span.clone())
        .await
        {
            Ok(cycle) => {
                scope_built = cycle.built;
                covered_sequences = cycle.covered_sequences;
            }
            Err(inner) => {
                scope_failed = true;
                failures.absorb(inner);
            }
        }

        if scope_failed {
            if let Err(error) = scope_cache
                .finish_failed_scope(&scope, &cluster, cache_disposition)
                .await
            {
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
                .finish_scope(&scope, &cluster, cache_disposition, scope_built)
                .await;
            scope_cache
                .update_covered_sequences(&scope, &cluster, &covered_sequences)
                .await;
            record_success(&scope_span);
        }
    }
    RegisteredScopeOutcome {
        failures,
        covered_sequences,
        empty: false,
    }
}

async fn refresh_covered_scope(
    cluster: &GraphCluster,
    scope: &str,
    cells: &[String],
    metrics: &IndexerMetrics,
    work: IndexWorkKind,
) -> Result<BTreeMap<String, u64>, CycleFailures> {
    let mut failures = CycleFailures::default();
    let mut refreshed_sequences = BTreeMap::new();

    for cell_id in cells {
        let Some(shard) = cluster.shard(cell_id) else {
            failures.push(
                IndexFailure::config(
                    "missing_cell",
                    format!("index scope {scope}: missing configured cell {cell_id}"),
                )
                .with_scope(scope)
                .with_cell(cell_id),
            );
            continue;
        };
        let refresh_span = tracing::info_span!(
            "index.refresh_sequence",
            hydradb.scope = %scope,
            hydradb.cell_id = %cell_id,
            hydradb.base_sequence = Empty,
            hydradb.outcome = Empty,
            error.class = Empty,
        );
        let refresh_timer = metrics.time_stage(work, IndexStage::RefreshSequence);
        let refresh_result = shard
            .refresh_storage_sequence(cell_id)
            .instrument(refresh_span.clone())
            .await;
        drop(refresh_timer);
        match refresh_result {
            Ok(sequence) => {
                refresh_span.record(semconv::BASE_SEQUENCE, sequence);
                record_success(&refresh_span);
                refreshed_sequences.insert(cell_id.clone(), sequence);
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
                failures.push(failure);
            }
        }
    }

    failures.into_result().map(|()| refreshed_sequences)
}

fn scope_probe_concurrency(scope_concurrency: usize) -> usize {
    scope_concurrency
        .saturating_mul(SCOPE_PROBE_CONCURRENCY_MULTIPLIER)
        .min(MAX_SCOPE_PROBE_CONCURRENCY)
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
) -> hydradb::Result<ScopeCursor> {
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
) -> hydradb::Result<ScopeCursorAdvance> {
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
) -> hydradb::Result<bool> {
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

#[allow(clippy::too_many_arguments)]
async fn run_index_cycle(
    cluster: &GraphCluster,
    scope: &str,
    cells: &[String],
    retain_previous: usize,
    build_mode: IndexBuildMode,
    incremental_min_edges: u64,
    known_covered_sequences: &BTreeMap<String, u64>,
    refreshed_sequences: &BTreeMap<String, u64>,
    metrics: &IndexerMetrics,
    work: IndexWorkKind,
) -> Result<IndexCycleOutcome, CycleFailures> {
    let mut failures = CycleFailures::default();
    let mut built_any = false;
    let mut covered_sequences = BTreeMap::new();
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

        let refreshed_sequence = if let Some(sequence) = refreshed_sequences.get(cell_id) {
            *sequence
        } else {
            let refresh_span = tracing::info_span!(
                parent: &cell_span,
                "index.refresh_sequence",
                hydradb.scope = %scope,
                hydradb.cell_id = %cell_id,
                hydradb.base_sequence = Empty,
                hydradb.outcome = Empty,
                error.class = Empty,
            );
            let refresh_timer = metrics.time_stage(work, IndexStage::RefreshSequence);
            let refresh_result = shard
                .refresh_storage_sequence(cell_id)
                .instrument(refresh_span.clone())
                .await;
            drop(refresh_timer);
            match refresh_result {
                Ok(sequence) => {
                    refresh_span.record(semconv::BASE_SEQUENCE, sequence);
                    record_success(&refresh_span);
                    sequence
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
        };

        if known_covered_sequences
            .get(cell_id)
            .is_some_and(|covered| *covered == refreshed_sequence)
        {
            covered_sequences.insert(cell_id.clone(), refreshed_sequence);
            cell_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
            continue;
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
        let discover_timer = metrics.time_stage(work, IndexStage::DiscoverDirty);
        let dirty_result = shard
            .dirty_graph_index_edge_types(cell_id)
            .instrument(discover_span.clone())
            .await;
        drop(discover_timer);
        let dirty = match dirty_result {
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
            let read_timer = metrics.time_stage(work, IndexStage::ReadCurrent);
            let current_result = shard
                .current_graph_index(cell_id, &edge_type)
                .instrument(read_span.clone())
                .await;
            drop(read_timer);
            let current = match current_result {
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
            let build_timer = metrics.time_stage(work, IndexStage::ArtifactBuild);
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
            drop(build_timer);
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
            let gc_timer = metrics.time_stage(work, IndexStage::ArtifactGc);
            let gc_result = shard
                .gc_graph_index_generations(cell_id, &edge_type, retain_previous)
                .instrument(gc_span.clone())
                .await;
            drop(gc_timer);
            match gc_result {
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
            let xlog_gc_timer = metrics.time_stage(work, IndexStage::XlogGc);
            let xlog_gc_result = shard
                .gc_topology_changelog(cell_id, &edge_type)
                .instrument(xlog_gc_span.clone())
                .await;
            drop(xlog_gc_timer);
            match xlog_gc_result {
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
            covered_sequences.insert(cell_id.clone(), refreshed_sequence);
            record_success(&cell_span);
        } else {
            covered_sequences.insert(cell_id.clone(), refreshed_sequence);
            cell_span.record(semconv::OUTCOME, Outcome::Skipped.as_str());
        }
    }

    failures.into_result().map(|()| IndexCycleOutcome {
        built: built_any,
        covered_sequences,
    })
}

impl IndexerAdminServer {
    async fn bind(
        addr: SocketAddr,
        metrics: Arc<IndexerMetrics>,
        change_wake: Arc<Notify>,
        wake_token: String,
        wake_min_interval: Duration,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let router = Router::new()
            .route("/livez", get(|| async { StatusCode::OK }))
            .route("/readyz", get(indexer_readiness))
            .route("/metrics", get(indexer_metrics))
            .route("/v1/changes:process", post(process_index_changes))
            .with_state(IndexerAdminState {
                metrics,
                change_wake,
                wake_token: Arc::from(wake_token),
                wake_limiter: Arc::new(WakeRateLimiter::new(wake_min_interval)),
            });
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

async fn indexer_readiness(State(state): State<IndexerAdminState>) -> StatusCode {
    if state.metrics.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn indexer_metrics(State(state): State<IndexerAdminState>) -> Response {
    (
        [
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        render_metrics(&state.metrics),
    )
        .into_response()
}

async fn process_index_changes(
    State(state): State<IndexerAdminState>,
    headers: HeaderMap,
) -> StatusCode {
    if !valid_wake_bearer(&headers, &state.wake_token) {
        state
            .metrics
            .change_push_rejections
            .fetch_add(1, Ordering::Relaxed);
        return StatusCode::UNAUTHORIZED;
    }
    if !state.wake_limiter.try_accept() {
        state
            .metrics
            .change_push_throttles
            .fetch_add(1, Ordering::Relaxed);
        return StatusCode::ACCEPTED;
    }
    state
        .metrics
        .change_push_wakes
        .fetch_add(1, Ordering::Relaxed);
    state.change_wake.notify_one();
    StatusCode::ACCEPTED
}

fn valid_wake_bearer(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some((scheme, supplied)) = value.split_once(' ') else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer")
        && !supplied.is_empty()
        && bool::from(supplied.as_bytes().ct_eq(expected.as_bytes()))
}

/// The `/metrics` body.
///
/// Split out of the handler so the exposition can be asserted on directly
/// rather than through an HTTP round trip; the handler adds headers and nothing
/// else. The three dimensioned families keep the position they had when they
/// were scalars, so a diff of the output shows labels appearing and no series
/// moving.
fn render_metrics(metrics: &IndexerMetrics) -> String {
    // Same series, same labels, first in the body — see the graph-node side in
    // `graph_node/admin.rs`. A build-info gauge only pays for itself if every
    // service in the fleet reports it identically.
    let mut output = hydradb_telemetry::build_info::prometheus_gauge();
    output.push_str(&format!(
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
            "# TYPE graph_indexer_scope_cache_admission_bypasses counter\n",
            "graph_indexer_scope_cache_admission_bypasses {}\n",
            "# TYPE graph_indexer_scope_cache_promotions counter\n",
            "graph_indexer_scope_cache_promotions {}\n",
            "# TYPE graph_indexer_scope_cache_idle_demotions counter\n",
            "graph_indexer_scope_cache_idle_demotions {}\n",
            "# TYPE graph_indexer_scope_cache_close_failures counter\n",
            "graph_indexer_scope_cache_close_failures {}\n",
            "# TYPE graph_indexer_scope_cache_entries gauge\n",
            "graph_indexer_scope_cache_entries {}\n",
            "# TYPE graph_indexer_scope_cache_capacity gauge\n",
            "graph_indexer_scope_cache_capacity {}\n",
            "# TYPE graph_indexer_registered_scopes gauge\n",
            "graph_indexer_registered_scopes {}\n",
            "# TYPE graph_indexer_change_notifications_observed counter\n",
            "graph_indexer_change_notifications_observed {}\n",
            "# TYPE graph_indexer_change_notifications_cleared counter\n",
            "graph_indexer_change_notifications_cleared {}\n",
            "# TYPE graph_indexer_changed_scopes_processed counter\n",
            "graph_indexer_changed_scopes_processed {}\n",
            "# TYPE graph_indexer_change_notification_failures counter\n",
            "graph_indexer_change_notification_failures {}\n",
            "# TYPE graph_indexer_change_push_wakes counter\n",
            "graph_indexer_change_push_wakes {}\n",
            "# TYPE graph_indexer_change_push_rejections counter\n",
            "graph_indexer_change_push_rejections {}\n",
            "# TYPE graph_indexer_change_push_throttles counter\n",
            "graph_indexer_change_push_throttles {}\n",
            "# TYPE graph_indexer_pending_change_notifications gauge\n",
            "graph_indexer_pending_change_notifications {}\n",
            "# TYPE graph_indexer_last_change_success_ms gauge\n",
            "graph_indexer_last_change_success_ms {}\n",
            "# TYPE graph_indexer_last_change_lag_ms gauge\n",
            "graph_indexer_last_change_lag_ms {}\n",
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
        metrics
            .scope_cache_admission_bypasses
            .load(Ordering::Relaxed),
        metrics.scope_cache_promotions.load(Ordering::Relaxed),
        metrics.scope_cache_idle_demotions.load(Ordering::Relaxed),
        metrics.scope_cache_close_failures.load(Ordering::Relaxed),
        metrics.scope_cache_entries.load(Ordering::Acquire),
        metrics.scope_cache_capacity.load(Ordering::Acquire),
        metrics.registered_scopes.load(Ordering::Acquire),
        metrics
            .change_notifications_observed
            .load(Ordering::Relaxed),
        metrics.change_notifications_cleared.load(Ordering::Relaxed),
        metrics.changed_scopes_processed.load(Ordering::Relaxed),
        metrics.change_notification_failures.load(Ordering::Relaxed),
        metrics.change_push_wakes.load(Ordering::Relaxed),
        metrics.change_push_rejections.load(Ordering::Relaxed),
        metrics.change_push_throttles.load(Ordering::Relaxed),
        metrics.pending_change_notifications.load(Ordering::Acquire),
        metrics.last_change_success_ms.load(Ordering::Relaxed),
        metrics.last_change_lag_ms.load(Ordering::Relaxed),
    ));
    append_dimensioned(&mut output, &metrics.dimensioned_snapshot());
    append_stage_timings(&mut output, &metrics.stage_timings_snapshot());
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

fn append_stage_timings(
    output: &mut String,
    timings: &BTreeMap<(IndexWorkKind, IndexStage), StageTiming>,
) {
    output.push_str("# TYPE graph_indexer_stage_duration_seconds summary\n");
    for ((work, stage), timing) in timings {
        let labels = format!("work=\"{}\",stage=\"{}\"", work.as_str(), stage.as_str());
        output.push_str(&format!(
            "graph_indexer_stage_duration_seconds_sum{{{labels}}} {:.6}\n",
            timing.elapsed_micros as f64 / 1_000_000.0
        ));
        output.push_str(&format!(
            "graph_indexer_stage_duration_seconds_count{{{labels}}} {}\n",
            timing.count
        ));
    }
    output.push_str("# TYPE graph_indexer_stage_duration_seconds_max gauge\n");
    for ((work, stage), timing) in timings {
        output.push_str(&format!(
            "graph_indexer_stage_duration_seconds_max{{work=\"{}\",stage=\"{}\"}} {:.6}\n",
            work.as_str(),
            stage.as_str(),
            timing.max_micros as f64 / 1_000_000.0
        ));
    }
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
            .collect::<hydradb::Result<Vec<_>>>()?,
    )?;
    Ok(GraphScope::new(
        namespace,
        GraphId::new(env_value("GRAPH_ID", "default"))?,
    ))
}

fn env_value(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn read_indexer_wake_token() -> RuntimeResult<String> {
    let path = env_value(
        "GRAPH_AUTH_TOKEN_FILE",
        "/var/run/secrets/slatedb-graph/auth-token",
    );
    let token = std::fs::read_to_string(&path)?.trim().to_string();
    if token.len() < 32 || token.eq_ignore_ascii_case("change-me") {
        return Err(format!(
            "GRAPH_AUTH_TOKEN_FILE {path} must contain at least 32 non-placeholder characters"
        )
        .into());
    }
    Ok(token)
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
    use hydradb::EdgeMutation;
    use slatedb::object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn push_endpoint_authenticates_and_rate_limits_wakes() {
        let metrics = Arc::new(IndexerMetrics::default());
        let change_wake = Arc::new(Notify::new());
        let state = IndexerAdminState {
            metrics: Arc::clone(&metrics),
            change_wake: Arc::clone(&change_wake),
            wake_token: Arc::from("correct-token"),
            wake_limiter: Arc::new(WakeRateLimiter::new(Duration::from_millis(20))),
        };

        let status = process_index_changes(State(state.clone()), HeaderMap::new()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(metrics.change_push_rejections.load(Ordering::Relaxed), 1);

        let mut wrong_headers = HeaderMap::new();
        wrong_headers.insert(AUTHORIZATION, "Bearer wrong-token".parse().unwrap());
        let status = process_index_changes(State(state.clone()), wrong_headers).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(metrics.change_push_rejections.load(Ordering::Relaxed), 2);

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer correct-token".parse().unwrap());
        let status = process_index_changes(State(state.clone()), headers.clone()).await;

        assert_eq!(status, StatusCode::ACCEPTED);
        tokio::time::timeout(Duration::from_millis(50), change_wake.notified())
            .await
            .expect("push endpoint should leave a wake permit");
        assert_eq!(metrics.change_push_wakes.load(Ordering::Relaxed), 1);

        let status = process_index_changes(State(state.clone()), headers.clone()).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), change_wake.notified())
                .await
                .is_err(),
            "throttled request must not wake object-store discovery"
        );
        assert_eq!(metrics.change_push_throttles.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.change_push_wakes.load(Ordering::Relaxed), 1);

        tokio::time::sleep(Duration::from_millis(25)).await;
        let status = process_index_changes(State(state), headers).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        tokio::time::timeout(Duration::from_millis(50), change_wake.notified())
            .await
            .expect("wake should be accepted after the rate-limit interval");
        assert_eq!(metrics.change_push_wakes.load(Ordering::Relaxed), 2);
    }

    fn test_scope_cache(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        metrics: Arc<IndexerMetrics>,
        max_open_scopes: usize,
    ) -> Arc<IndexerScopeCache> {
        Arc::new(IndexerScopeCache::new(
            "graph/data".to_string(),
            vec!["cell-0".to_string()],
            object_store,
            indexer_open_options(
                GraphLimits::default().max_wal_tail_files,
                GraphOpenOptions::default().cache.slatedb_cache_bytes,
                false,
            ),
            max_open_scopes,
            DEFAULT_SCOPE_CONCURRENCY,
            metrics,
        ))
    }

    #[test]
    fn indexer_readers_are_checkpoint_free_and_refresh_on_demand() {
        let options = indexer_open_options(123, 456, false);

        assert_eq!(options.limits.max_wal_tail_files, 123);
        assert_eq!(options.cache.slatedb_cache_bytes, 456);
        assert!(!options.unstable_canonical_vertex_membership);
        assert_eq!(options.reader_mode, GraphReaderMode::FollowLatest);
        assert_eq!(
            options.reader_manifest_poll_interval,
            INDEXER_READER_MANIFEST_POLL_INTERVAL
        );
    }

    #[tokio::test]
    async fn indexer_can_build_a_completed_membership_cell() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let scope = GraphScope::default();
        let options = indexer_open_options(123, 456, true);
        let writer = hydradb::GraphShard::open_standalone_writer_with_options(
            format!("{}/cell-0", scope.scoped_store_path("graph/data")),
            Arc::clone(&object_store),
            options.clone(),
        )
        .await
        .unwrap();
        assert!(
            writer
                .unstable_backfill_vertex_membership(
                    "cell-0",
                    hydradb::VertexMembershipBackfillOptions::default(),
                )
                .await
                .unwrap()
                .complete
        );
        writer
            .write_edge(EdgeMutation {
                cell_id: "cell-0".into(),
                edge_type: "FOLLOWS".into(),
                src: 1,
                dst: 2,
                idempotency_key: "membership-indexer-edge".into(),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        let metrics = Arc::new(IndexerMetrics::default());
        let cache = IndexerScopeCache::new(
            "graph/data".into(),
            vec!["cell-0".into()],
            object_store,
            options,
            1,
            1,
            Arc::clone(&metrics),
        );
        let (cluster, _) = cache.cluster_for_scope(&scope).await.unwrap();
        let outcome = run_index_cycle(
            &cluster,
            &scope.to_string(),
            &["cell-0".into()],
            1,
            IndexBuildMode::Full,
            250_000,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &metrics,
            IndexWorkKind::Sweep,
        )
        .await
        .unwrap();
        assert!(outcome.built);
        let shard = cluster.shard("cell-0").unwrap();
        assert!(shard
            .current_graph_index("cell-0", "FOLLOWS")
            .await
            .unwrap()
            .is_some());
        for id in [1, 2] {
            assert!(shard
                .vertex_metadata_if_exists("cell-0", id)
                .await
                .unwrap()
                .is_some());
        }
        drop(cluster);
        cache.close().await.unwrap();
    }

    #[test]
    fn canonical_membership_setting_rejects_invalid_values() {
        for value in ["true", "1", "on", "yes", " TRUE "] {
            assert!(parse_canonical_vertex_membership(value).unwrap());
        }
        for value in ["false", "0", "off", "no"] {
            assert!(!parse_canonical_vertex_membership(value).unwrap());
        }
        assert!(parse_canonical_vertex_membership("tru")
            .unwrap_err()
            .to_string()
            .contains("GRAPH_UNSTABLE_CANONICAL_VERTEX_MEMBERSHIP"));
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

    #[test]
    fn clean_scope_probes_run_wider_than_dirty_builds_with_a_hard_cap() {
        assert_eq!(scope_probe_concurrency(1), 2);
        assert_eq!(scope_probe_concurrency(16), 32);
        assert_eq!(scope_probe_concurrency(MAX_SCOPE_CONCURRENCY), 128);
        assert_eq!(scope_probe_concurrency(usize::MAX), 128);
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
    async fn full_scope_cache_bypasses_cold_scans_without_churning_residents() {
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

        let (first, first_disposition) = cache.cluster_for_scope(&first_scope).await.unwrap();
        let (reused, reused_disposition) = cache.cluster_for_scope(&first_scope).await.unwrap();
        assert_eq!(first_disposition, ScopeCacheDisposition::Admitted);
        assert_eq!(reused_disposition, ScopeCacheDisposition::Hit);
        assert!(Arc::ptr_eq(&first, &reused));
        drop(first);
        drop(reused);

        let (second, second_disposition) = cache.cluster_for_scope(&second_scope).await.unwrap();
        assert_eq!(second_disposition, ScopeCacheDisposition::Bypassed);
        cache
            .finish_scope(&second_scope, &second, second_disposition, false)
            .await;

        let (resident, resident_disposition) = cache.cluster_for_scope(&first_scope).await.unwrap();
        assert_eq!(resident_disposition, ScopeCacheDisposition::Hit);
        drop(resident);

        assert_eq!(metrics.scope_cache_hits.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.scope_cache_misses.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.scope_cache_evictions.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics
                .scope_cache_admission_bypasses
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 1);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn clean_scopes_remain_cached_and_idle_hot_scopes_are_demoted() {
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

        let (hot, hot_disposition) = cache.cluster_for_scope(&hot_scope).await.unwrap();
        cache
            .finish_scope(&hot_scope, &hot, hot_disposition, true)
            .await;
        let (idle, idle_disposition) = cache.cluster_for_scope(&idle_scope).await.unwrap();
        cache
            .finish_scope(&idle_scope, &idle, idle_disposition, false)
            .await;

        assert_eq!(cache.hot_scopes(10).await, vec![hot_scope.clone()]);
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 2);
        let (reused_idle, idle_hit) = cache.cluster_for_scope(&idle_scope).await.unwrap();
        assert_eq!(idle_hit, ScopeCacheDisposition::Hit);
        assert!(Arc::ptr_eq(&idle, &reused_idle));

        for _ in 0..HOT_SCOPE_IDLE_RECHECKS {
            let (reused_hot, hot_hit) = cache.cluster_for_scope(&hot_scope).await.unwrap();
            assert_eq!(hot_hit, ScopeCacheDisposition::Hit);
            assert!(Arc::ptr_eq(&hot, &reused_hot));
            cache
                .finish_scope(&hot_scope, &reused_hot, hot_hit, false)
                .await;
        }
        assert!(cache.hot_scopes(10).await.is_empty());
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 2);
        assert_eq!(
            metrics.scope_cache_idle_demotions.load(Ordering::Relaxed),
            1
        );
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn resident_scope_skips_redundant_existence_scan() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(Arc::clone(&object_store), Arc::clone(&metrics), 1);
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let scope = GraphScope::new(
            root.child(NamespaceId::new("tenant-a").unwrap()).unwrap(),
            GraphId::new("hydradb").unwrap(),
        );
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
                idempotency_key: "resident-scope-edge".to_string(),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        for _ in 0..2 {
            let outcome = run_registered_scope(
                scope.clone(),
                1,
                IndexBuildMode::Full,
                250_000,
                &cache,
                &metrics,
                IndexWorkKind::Sweep,
            )
            .await;
            assert!(outcome.failures.is_empty(), "{}", outcome.failures);
        }

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
                idempotency_key: "resident-scope-edge-2".to_string(),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();
        let changed = run_registered_scope(
            scope.clone(),
            1,
            IndexBuildMode::Full,
            250_000,
            &cache,
            &metrics,
            IndexWorkKind::Sweep,
        )
        .await;
        assert!(changed.failures.is_empty(), "{}", changed.failures);

        let timings = metrics.stage_timings_snapshot();
        assert_eq!(
            timings
                .get(&(IndexWorkKind::Sweep, IndexStage::ScopeTotal))
                .map(|timing| timing.count),
            Some(3)
        );
        assert_eq!(
            timings
                .get(&(IndexWorkKind::Sweep, IndexStage::ScopeHasData))
                .map(|timing| timing.count),
            Some(1),
            "the resident reader makes another object-store existence scan unnecessary"
        );
        assert_eq!(
            timings
                .get(&(IndexWorkKind::Sweep, IndexStage::RefreshSequence))
                .map(|timing| timing.count),
            Some(3)
        );
        assert_eq!(
            timings
                .get(&(IndexWorkKind::Sweep, IndexStage::DiscoverDirty))
                .map(|timing| timing.count),
            Some(2),
            "only the unchanged storage sequence should skip dirty-index discovery"
        );
        assert_eq!(metrics.scope_cache_misses.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.scope_cache_hits.load(Ordering::Relaxed), 2);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn built_bypassed_scope_displaces_oldest_idle_resident() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(object_store, Arc::clone(&metrics), 1);
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let scope = |tenant: &str| {
            GraphScope::new(
                root.child(NamespaceId::new(tenant).unwrap()).unwrap(),
                GraphId::new("hydradb").unwrap(),
            )
        };
        let idle_scope = scope("idle");
        let built_scope = scope("built");

        let (idle, idle_disposition) = cache.cluster_for_scope(&idle_scope).await.unwrap();
        cache
            .finish_scope(&idle_scope, &idle, idle_disposition, false)
            .await;
        drop(idle);
        let (built, built_disposition) = cache.cluster_for_scope(&built_scope).await.unwrap();
        assert_eq!(built_disposition, ScopeCacheDisposition::Bypassed);
        cache
            .finish_scope(&built_scope, &built, built_disposition, true)
            .await;

        let (reused, disposition) = cache.cluster_for_scope(&built_scope).await.unwrap();
        assert_eq!(disposition, ScopeCacheDisposition::Hit);
        assert!(Arc::ptr_eq(&built, &reused));
        assert_eq!(metrics.scope_cache_evictions.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.scope_cache_promotions.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 1);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_bypassed_scope_closes_without_invalidating_residents() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(object_store, Arc::clone(&metrics), 1);
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let scope = |tenant: &str| {
            GraphScope::new(
                root.child(NamespaceId::new(tenant).unwrap()).unwrap(),
                GraphId::new("hydradb").unwrap(),
            )
        };
        let resident_scope = scope("resident");
        let failed_scope = scope("failed");

        let (resident, resident_disposition) =
            cache.cluster_for_scope(&resident_scope).await.unwrap();
        assert_eq!(resident_disposition, ScopeCacheDisposition::Admitted);
        drop(resident);

        let (failed, failed_disposition) = cache.cluster_for_scope(&failed_scope).await.unwrap();
        assert_eq!(failed_disposition, ScopeCacheDisposition::Bypassed);
        cache
            .finish_failed_scope(&failed_scope, &failed, failed_disposition)
            .await
            .unwrap();

        let (reused, disposition) = cache.cluster_for_scope(&resident_scope).await.unwrap();
        assert_eq!(disposition, ScopeCacheDisposition::Hit);
        assert_eq!(metrics.scope_cache_entries.load(Ordering::Acquire), 1);
        drop(reused);
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
        assert_eq!(metrics.registered_scopes.load(Ordering::Acquire), 5);
        assert_eq!(metrics.scope_cache_capacity.load(Ordering::Acquire), 1);
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

    #[test]
    fn stable_scope_cache_shortfall_warns_once() {
        let metrics = IndexerMetrics::default();

        assert_eq!(metrics.record_scope_cache_population(5, 2), Some(3));
        assert_eq!(metrics.record_scope_cache_population(5, 2), None);
        assert_eq!(metrics.record_scope_cache_population(6, 2), Some(4));
        assert_eq!(metrics.record_scope_cache_population(2, 2), None);
        assert_eq!(metrics.record_scope_cache_population(5, 2), Some(3));
        assert_eq!(metrics.registered_scopes.load(Ordering::Acquire), 5);
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

    #[tokio::test]
    async fn changed_scope_fast_lane_builds_and_acknowledges_exact_hints() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let scope = GraphScope::new(
            root.child(NamespaceId::new("tenant-fast").unwrap())
                .unwrap()
                .child(NamespaceId::new("collection-fast").unwrap())
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
                idempotency_key: "fast-lane-write".to_string(),
            })
            .await
            .unwrap();
        let sequence = writer
            .shard("cell-0")
            .unwrap()
            .current_storage_sequence("cell-0")
            .await
            .unwrap();
        writer.close().await.unwrap();

        directory
            .notify_changed(&scope, "cell-0", sequence)
            .await
            .unwrap();
        directory
            .notify_changed(&scope, "cell-0", sequence)
            .await
            .unwrap();
        let metrics = Arc::new(IndexerMetrics::default());
        let cache = test_scope_cache(Arc::clone(&object_store), Arc::clone(&metrics), 1);

        let failures = run_scope_change_pass(
            &directory,
            1,
            IndexBuildMode::Full,
            250_000,
            1,
            &cache,
            &metrics,
        )
        .await;

        assert!(failures.is_empty(), "fast lane failed: {failures}");
        assert!(directory.list_changes().await.unwrap().is_empty());
        assert_eq!(
            metrics
                .change_notifications_observed
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            metrics.change_notifications_cleared.load(Ordering::Relaxed),
            2
        );
        assert_eq!(metrics.changed_scopes_processed.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.pending_change_notifications.load(Ordering::Acquire),
            0
        );

        // A notification can land after the build that already covered its
        // commit. The durable sequence watermark must clear it without opening
        // or refreshing the scope a second time.
        directory
            .notify_changed(&scope, "cell-0", sequence)
            .await
            .unwrap();
        let second_failures = run_scope_change_pass(
            &directory,
            1,
            IndexBuildMode::Full,
            250_000,
            1,
            &cache,
            &metrics,
        )
        .await;
        assert!(
            second_failures.is_empty(),
            "covered hint cleanup failed: {second_failures}"
        );
        assert!(directory.list_changes().await.unwrap().is_empty());
        assert_eq!(
            metrics
                .change_notifications_observed
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(
            metrics.change_notifications_cleared.load(Ordering::Relaxed),
            3
        );
        assert_eq!(metrics.changed_scopes_processed.load(Ordering::Relaxed), 1);
        let timings = metrics.stage_timings_snapshot();
        assert_eq!(
            timings
                .get(&(IndexWorkKind::Change, IndexStage::ScopeTotal))
                .map(|timing| timing.count),
            Some(1)
        );
        for stage in [
            IndexStage::Pass,
            IndexStage::ListChanges,
            IndexStage::ScopeTotal,
            IndexStage::ScopeQueue,
            IndexStage::ScopeHasData,
            IndexStage::ClusterOpen,
            IndexStage::RefreshSequence,
            IndexStage::DiscoverDirty,
            IndexStage::ReadCurrent,
            IndexStage::ArtifactBuild,
            IndexStage::ArtifactGc,
            IndexStage::XlogGc,
            IndexStage::ClearChanges,
        ] {
            assert!(
                timings.contains_key(&(IndexWorkKind::Change, stage)),
                "missing timing for {stage:?}"
            );
        }

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
            Some(1)
        );
        reader.close().await.unwrap();
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn sequence_coverage_never_skips_a_newer_hint() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn slatedb::object_store::ObjectStore>;
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let scope = GraphScope::new(
            root.child(NamespaceId::new("tenant-a").unwrap()).unwrap(),
            graph_id.clone(),
        );
        let directory =
            ObjectStoreGraphScopeDirectory::new("graph/data", root, graph_id, object_store);
        directory
            .acknowledge_sequence(&scope, "cell-0", 10)
            .await
            .unwrap();
        directory.notify_changed(&scope, "cell-0", 9).await.unwrap();
        directory
            .notify_changed(&scope, "cell-0", 11)
            .await
            .unwrap();

        let changes = directory.list_changes().await.unwrap();
        let (covered, pending) = partition_scope_changes(&directory, &changes).await.unwrap();

        assert_eq!(
            covered
                .iter()
                .map(|change| change.sequence)
                .collect::<Vec<_>>(),
            vec![Some(9)]
        );
        assert_eq!(
            pending
                .iter()
                .map(|change| change.sequence)
                .collect::<Vec<_>>(),
            vec![Some(11)]
        );
    }

    /// Every series a scrape sees when nothing has been indexed yet.
    ///
    /// Pinned in full rather than probed, because the point of this change is
    /// that the process-global half did *not* move: `ready` through
    /// `open_failures` and `last_success_ms` keep their exact names, their
    /// absence of labels and their positions, and the six families that carry
    /// labels still declare a `# TYPE` line with no samples under it.
    ///
    /// The build-info preamble is stripped rather than pinned: it names the
    /// commit, so its text differs on every build and no literal could match
    /// it. `strip_prefix` still proves the document opens with exactly that
    /// series, and what stays pinned below is the property this test is for —
    /// that adding it moved nothing underneath.
    #[test]
    fn an_idle_indexer_declares_every_family() {
        let metrics = IndexerMetrics::default();
        metrics.ready.store(true, Ordering::Release);
        metrics.cycles.store(3, Ordering::Relaxed);

        let document = render_metrics(&metrics);
        let body = document
            .strip_prefix(&hydradb_telemetry::build_info::prometheus_gauge())
            .expect("the document opens with the build-info series");

        assert_eq!(
            body,
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
                "# TYPE graph_indexer_scope_cache_admission_bypasses counter\n",
                "graph_indexer_scope_cache_admission_bypasses 0\n",
                "# TYPE graph_indexer_scope_cache_promotions counter\n",
                "graph_indexer_scope_cache_promotions 0\n",
                "# TYPE graph_indexer_scope_cache_idle_demotions counter\n",
                "graph_indexer_scope_cache_idle_demotions 0\n",
                "# TYPE graph_indexer_scope_cache_close_failures counter\n",
                "graph_indexer_scope_cache_close_failures 0\n",
                "# TYPE graph_indexer_scope_cache_entries gauge\n",
                "graph_indexer_scope_cache_entries 0\n",
                "# TYPE graph_indexer_scope_cache_capacity gauge\n",
                "graph_indexer_scope_cache_capacity 0\n",
                "# TYPE graph_indexer_registered_scopes gauge\n",
                "graph_indexer_registered_scopes 0\n",
                "# TYPE graph_indexer_change_notifications_observed counter\n",
                "graph_indexer_change_notifications_observed 0\n",
                "# TYPE graph_indexer_change_notifications_cleared counter\n",
                "graph_indexer_change_notifications_cleared 0\n",
                "# TYPE graph_indexer_changed_scopes_processed counter\n",
                "graph_indexer_changed_scopes_processed 0\n",
                "# TYPE graph_indexer_change_notification_failures counter\n",
                "graph_indexer_change_notification_failures 0\n",
                "# TYPE graph_indexer_change_push_wakes counter\n",
                "graph_indexer_change_push_wakes 0\n",
                "# TYPE graph_indexer_change_push_rejections counter\n",
                "graph_indexer_change_push_rejections 0\n",
                "# TYPE graph_indexer_change_push_throttles counter\n",
                "graph_indexer_change_push_throttles 0\n",
                "# TYPE graph_indexer_pending_change_notifications gauge\n",
                "graph_indexer_pending_change_notifications 0\n",
                "# TYPE graph_indexer_last_change_success_ms gauge\n",
                "graph_indexer_last_change_success_ms 0\n",
                "# TYPE graph_indexer_last_change_lag_ms gauge\n",
                "graph_indexer_last_change_lag_ms 0\n",
                "# TYPE graph_indexer_generations_published counter\n",
                "# TYPE graph_indexer_generation_failures counter\n",
                "# TYPE graph_indexer_generations_deleted counter\n",
                "# TYPE graph_indexer_full_build_edges counter\n",
                "# TYPE graph_indexer_incremental_delta_edges counter\n",
                "# TYPE graph_indexer_incremental_fallbacks counter\n",
                "# TYPE graph_indexer_dimensions gauge\n",
                "graph_indexer_dimensions 0\n",
                "# TYPE graph_indexer_stage_duration_seconds summary\n",
                "# TYPE graph_indexer_stage_duration_seconds_max gauge\n",
                "# TYPE graph_indexer_last_success_ms gauge\n",
                "graph_indexer_last_success_ms 0\n",
                "# TYPE graph_indexer_last_full_sweep_ms gauge\n",
                "graph_indexer_last_full_sweep_ms 0\n",
            )
        );
    }

    #[test]
    fn stage_timings_expose_bounded_work_and_stage_labels() {
        let metrics = IndexerMetrics::default();
        metrics.record_stage(
            IndexWorkKind::Change,
            IndexStage::ArtifactBuild,
            Duration::from_millis(12),
        );
        metrics.record_stage(
            IndexWorkKind::Change,
            IndexStage::ArtifactBuild,
            Duration::from_millis(8),
        );

        let output = render_metrics(&metrics);
        assert!(output.contains(
            "graph_indexer_stage_duration_seconds_sum{work=\"change\",stage=\"artifact_build\"} 0.020000\n"
        ));
        assert!(output.contains(
            "graph_indexer_stage_duration_seconds_count{work=\"change\",stage=\"artifact_build\"} 2\n"
        ));
        assert!(output.contains(
            "graph_indexer_stage_duration_seconds_max{work=\"change\",stage=\"artifact_build\"} 0.012000\n"
        ));
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
