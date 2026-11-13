//! Request decomposition and slow-plan sampling for the experimental route
//! (Workstream 11).
//!
//! Every experimental read gets one `query.experimental` span. It splits the
//! request into parse, lower, snapshot, statistics wait, physical planning,
//! storage I/O, operator CPU and result conversion, and it names the plan by a
//! hash of its redacted shape. The same split feeds per-cell counters, so a
//! dashboard reads the mean and a trace reads one request. Nothing needs to be
//! added during an incident.
//!
//! What goes where:
//!
//! - **Metric labels:** `cell_id` only, through the existing per-cell counters.
//!   Route and fallback are separate counters, not labels.
//! - **Span attributes on every request:** stage times, plan hash, capability,
//!   route, storage calls, the widest intermediate result, and what chose the
//!   access path (`hydradb.query.access_path.*`): statistics, the runtime
//!   probe, the guard, or neither. Only a property *name* is recorded there.
//! - **Only on a request that trips a sampling trigger:** the plan shape
//!   (`hydradb.query.plan`, literals already elided) and a WARN event. Such a
//!   request is also marked `hydradb.sampling.tail_keep` so the collector
//!   keeps its trace.
//!
//! Query text, parameter values and property values never appear.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hydradb_cypher_engine::{
    plan_shape_hash, ExecutionProfile, GraphPhysicalPlan, LowerTimings, OperatorProfile, OperatorProfiler, PreparedQuery,
    StatisticsAccessReport, StatisticsProvider,
};

use super::super::query_optimizer::{slow_query_threshold_ms, wide_plan_threshold_rows};
use super::GraphShard;
use crate::core::state::ReadAccounting;
use crate::{graph_now_millis, ExperimentalOperatorMetricsSnapshot, GraphError};

/// An operator estimate is a miss when it is off from the rows produced by at
/// least this ratio...
const ESTIMATE_MISS_RATIO: f64 = 10.0;
/// ...and either side reached this many rows. Below it a large ratio is a
/// rounding error on a cheap operator, not a bad plan.
const ESTIMATE_MISS_MIN_ROWS: u64 = 1_000;

tokio::task_local! {
    /// The work counters of the experimental request being executed on this
    /// task. A task-local rather than a field on the storage adapter so the
    /// adapter's many operator bodies need no extra argument.
    static REQUEST_WORK: Arc<RequestWork>;
}

/// Storage work the adapter reports while a plan executes.
#[derive(Default)]
pub(super) struct RequestWork {
    storage_us: AtomicU64,
    storage_calls: AtomicU64,
    widest_rows: AtomicU64,
    /// SlateDB reads issued under this request, for per-operator attribution.
    reads: Arc<ReadAccounting>,
    /// Installed once the plan exists. A mutex, not a `RefCell`: the adapter
    /// reports from `&self` methods and the future must stay `Send`. The
    /// executor is sequential, so it is never contended.
    profiler: Mutex<Option<OperatorProfiler>>,
    /// What chose the access path: the probe's runs and the reports of
    /// statistics-chosen accesses. Same locking argument as `profiler`.
    access: Mutex<AccessObservation>,
}

/// The runtime probe and the statistics-chosen accesses of one request.
#[derive(Clone, Debug, Default)]
pub(super) struct AccessObservation {
    probe_runs: u64,
    probe_us: u64,
    /// Some probe answered, so its vertices replaced the planned access.
    probe_answered: bool,
    /// Statistics-chosen accesses that read past their guard and declined.
    guard_stops: u64,
    /// The report that read the most index entries, which describes the
    /// access that dominated the request when several bindings report.
    widest: Option<StatisticsAccessReport>,
}

/// What chose a request's access path. One value per request; when more than
/// one applies, the earlier variant wins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AccessPathSource {
    /// Statistics chose, the access read past its guard, and the plan chosen
    /// without statistics (probed as today) answered instead.
    Guard,
    /// The runtime probe answered: today's path.
    Probe,
    /// Statistics chose and their access answered without the probe.
    Statistics,
    /// Neither: the planner's first equality, as before statistics.
    Default,
}

impl AccessPathSource {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Guard => "guard",
            Self::Probe => "probe",
            Self::Statistics => "statistics",
            Self::Default => "default",
        }
    }
}

impl AccessObservation {
    pub(super) fn source(&self) -> AccessPathSource {
        if self.guard_stops > 0 {
            AccessPathSource::Guard
        } else if self.probe_answered {
            AccessPathSource::Probe
        } else if self.widest.is_some() {
            AccessPathSource::Statistics
        } else {
            AccessPathSource::Default
        }
    }

    fn record_report(&mut self, report: &StatisticsAccessReport) {
        if report.declined {
            self.guard_stops += 1;
        }
        if self.widest.as_ref().is_none_or(|widest| report.entries_read > widest.entries_read) {
            self.widest = Some(report.clone());
        }
    }
}

impl RequestWork {
    /// Run `future` with this request's counters installed for the adapter.
    pub(super) async fn scope<F: std::future::Future>(self: &Arc<Self>, future: F) -> F::Output {
        REQUEST_WORK.scope(Arc::clone(self), self.reads.scope(future)).await
    }

    /// Start attributing work to the operators of `plan`, which must be the
    /// plan value the executor will run.
    pub(super) fn profile(&self, plan: &GraphPhysicalPlan) {
        *self.lock_profiler() = Some(OperatorProfiler::new(plan));
    }

    fn lock_profiler(&self) -> std::sync::MutexGuard<'_, Option<OperatorProfiler>> {
        self.profiler.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn take_profile(&self) -> Option<ExecutionProfile> {
        self.lock_profiler().take().map(OperatorProfiler::finish)
    }

    fn lock_access(&self) -> std::sync::MutexGuard<'_, AccessObservation> {
        self.access.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn access(&self) -> AccessObservation {
        self.lock_access().clone()
    }
}

/// One run of the runtime equality probe, and whether it answered.
pub(super) fn record_probe(elapsed_us: u64, answered: bool) {
    let _ = REQUEST_WORK.try_with(|work| {
        let mut access = work.lock_access();
        access.probe_runs += 1;
        access.probe_us = access.probe_us.saturating_add(elapsed_us);
        access.probe_answered |= answered;
    });
}

/// A statistics-chosen access finished or declined.
pub(super) fn record_statistics_access(report: &StatisticsAccessReport) {
    let _ = REQUEST_WORK.try_with(|work| work.lock_access().record_report(report));
}

fn with_profiler(charge: impl FnOnce(&mut OperatorProfiler)) {
    let _ = REQUEST_WORK.try_with(|work| {
        if let Some(profiler) = work.lock_profiler().as_mut() {
            charge(profiler);
        }
    });
}

/// Count one storage-adapter call and charge it to the running operator.
/// `call_reads` is the `(requests, bytes)` the call itself issued, measured in
/// its own accounting scope. A no-op outside a scoped request, such as adapter
/// unit tests that call it directly.
pub(super) fn record_storage_call(call: &'static str, elapsed_us: u64, rows_out: usize, call_reads: (u64, u64)) {
    let _ = REQUEST_WORK.try_with(|work| {
        work.storage_us.fetch_add(elapsed_us, Ordering::Relaxed);
        work.storage_calls.fetch_add(1, Ordering::Relaxed);
        work.widest_rows.fetch_max(rows_out as u64, Ordering::Relaxed);
        if let Some(profiler) = work.lock_profiler().as_mut() {
            profiler.record_storage_call(Duration::from_micros(elapsed_us), call_reads.0, call_reads.1);
            // Classified by adapter call rather than reported from each body,
            // so the operator bodies stay free of accounting.
            match call {
                "hydrate_vertices" | "scan_vertices_by_property_ordered" => {
                    profiler.record_hydrated_vertices(rows_out);
                }
                "expand_relationships"
                | "expand_relationships_between"
                | "seek_relationships_by_property"
                | "seek_relationships_by_property_values"
                | "scan_relationships_by_property_ordered" => {
                    profiler.record_scanned_relationships(rows_out);
                }
                _ => {}
            }
        }
    });
}

pub(super) fn record_full_scan() {
    with_profiler(OperatorProfiler::record_full_scan);
}

/// Note an intermediate row collection the executor asked to admit.
pub(super) fn record_intermediate_rows(rows: usize) {
    let _ = REQUEST_WORK.try_with(|work| {
        work.widest_rows.fetch_max(rows as u64, Ordering::Relaxed);
        if let Some(profiler) = work.lock_profiler().as_mut() {
            profiler.record_retained_rows(rows);
        }
    });
}

pub(super) fn operator_started(plan: &GraphPhysicalPlan) {
    with_profiler(|profiler| profiler.operator_started(plan));
}

pub(super) fn operator_finished(plan: &GraphPhysicalPlan, rows_out: usize) {
    with_profiler(|profiler| profiler.operator_finished(plan, rows_out));
}

pub(super) fn record_retained_rows(rows: usize) {
    with_profiler(|profiler| profiler.record_retained_rows(rows));
}

pub(super) fn record_hydrated_vertices(vertices: usize) {
    with_profiler(|profiler| profiler.record_hydrated_vertices(vertices));
}

/// The span every experimental read runs under. Declared here so the field
/// list and the code that records into it cannot drift apart: `tracing` drops
/// a `record` for an undeclared field without a word.
pub(super) fn request_span() -> tracing::Span {
    tracing::info_span!(
        "query.experimental",
        hydradb.query.route = "experimental",
        hydradb.query.capability = tracing::field::Empty,
        hydradb.query.plan_hash = tracing::field::Empty,
        hydradb.query.plan = tracing::field::Empty,
        hydradb.query.full_scan = tracing::field::Empty,
        hydradb.query.stage.parse_us = tracing::field::Empty,
        hydradb.query.stage.lower_us = tracing::field::Empty,
        hydradb.query.stage.bind_us = tracing::field::Empty,
        hydradb.query.stage.lower_cache_hit = tracing::field::Empty,
        hydradb.query.stage.snapshot_us = tracing::field::Empty,
        hydradb.query.stage.statistics_us = tracing::field::Empty,
        hydradb.query.stage.plan_us = tracing::field::Empty,
        hydradb.query.stage.execute_us = tracing::field::Empty,
        hydradb.query.stage.storage_us = tracing::field::Empty,
        hydradb.query.stage.operator_cpu_us = tracing::field::Empty,
        hydradb.query.stage.result_us = tracing::field::Empty,
        hydradb.query.storage.calls = tracing::field::Empty,
        hydradb.query.stats.sequence = tracing::field::Empty,
        hydradb.query.stats.age_ms = tracing::field::Empty,
        hydradb.query.access_path.source = tracing::field::Empty,
        hydradb.query.access_path.kind = tracing::field::Empty,
        hydradb.query.access_path.property = tracing::field::Empty,
        hydradb.query.access_path.rows_estimated = tracing::field::Empty,
        hydradb.query.access_path.rows_actual = tracing::field::Empty,
        hydradb.query.access_path.estimate_ratio = tracing::field::Empty,
        hydradb.query.access_path.probe_ran = tracing::field::Empty,
        hydradb.query.access_path.probe_us = tracing::field::Empty,
        hydradb.query.access_path.guard_stopped = tracing::field::Empty,
        hydradb.query.rows_widest = tracing::field::Empty,
        hydradb.query.rows_materialized = tracing::field::Empty,
        hydradb.query.rows_returned = tracing::field::Empty,
        hydradb.storage.remote_gets = tracing::field::Empty,
        hydradb.storage.remote_get_bytes = tracing::field::Empty,
        hydradb.storage.ranged_gets = tracing::field::Empty,
        hydradb.storage.puts = tracing::field::Empty,
        hydradb.storage.put_bytes = tracing::field::Empty,
        hydradb.sampling.tail_keep = tracing::field::Empty,
        error.class = tracing::field::Empty,
    )
}

/// Wall time of each stage of one experimental request, and what the plan was.
#[derive(Default)]
pub(super) struct RequestObservation {
    pub(super) lowering: Duration,
    pub(super) lower_timings: LowerTimings,
    pub(super) lower_cache_hit: bool,
    pub(super) snapshot: Duration,
    pub(super) statistics: Duration,
    pub(super) plan: Duration,
    pub(super) execute: Duration,
    pub(super) result: Duration,
    pub(super) rows_materialized: usize,
    pub(super) rows_returned: usize,
    /// Set once physical planning succeeds. Its absence after an error means
    /// the engine could not plan the query.
    pub(super) plan_identity: Option<PlanIdentity>,
    pub(super) work: Arc<RequestWork>,
    /// Per-operator work, with estimates attached, once execution returned.
    pub(super) profile: Option<ExecutionProfile>,
    /// `(storage_sequence, built_at_ms)` of the planner statistics object the
    /// request loaded, when `GRAPH_PLANNER_STATISTICS` is on and one exists.
    pub(super) planner_statistics: Option<(u64, u64)>,
}

pub(super) struct PlanIdentity {
    hash: u64,
    may_scan_inventory: bool,
    /// Rendered once, because the hash needs it; attached to the span only
    /// when the request is sampled.
    shape: String,
}

impl PlanIdentity {
    pub(super) fn of(prepared: &PreparedQuery) -> Self {
        let shape = prepared.plan_shape();
        Self {
            hash: plan_shape_hash(&shape),
            may_scan_inventory: prepared.physical.may_scan_inventory(),
            shape,
        }
    }
}

/// Why a request's trace is worth keeping. The plan verdicts outrank the
/// clock, as on the legacy route: a slow *and* wide plan is reported as wide,
/// because that is the half an operator can act on.
fn sampling_reason(full_scan: bool, widest_rows: u64, estimate_missed: bool, elapsed: Duration) -> Option<&'static str> {
    if full_scan {
        Some(reason::FULL_SCAN)
    } else if widest_rows >= wide_plan_threshold_rows() {
        Some(reason::WIDE)
    } else if estimate_missed {
        Some(reason::ESTIMATE_ERROR)
    } else if elapsed.as_millis() as u64 >= slow_query_threshold_ms() {
        Some(reason::SLOW)
    } else {
        None
    }
}

/// Tail-keep reasons. `full_scan` and `wide_plan` are the legacy route's
/// values, so one collector policy covers both engines.
mod reason {
    pub(super) const FULL_SCAN: &str = "full_scan";
    pub(super) const WIDE: &str = "wide_plan";
    pub(super) const ESTIMATE_ERROR: &str = "estimate_error";
    pub(super) const SLOW: &str = "slow";
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl RequestObservation {
    /// Split the preparation wall time into parser, AST lowering and
    /// everything else: parameter conversion, the template-cache lookup and
    /// binding. Parse and lower are what the template build measured, so both
    /// are zero on a cache hit; bind is the remainder and is paid every time.
    pub(super) fn preparation_split(&self) -> (Duration, Duration, Duration) {
        let LowerTimings { parse, lower } = self.lower_timings;
        let bind = self.lowering.saturating_sub(parse).saturating_sub(lower);
        (parse, lower, bind)
    }

    /// Collect the operator profile after execution, successful or not, and
    /// attach the estimates `statistics` gives for `plan`.
    pub(super) fn collect_profile(&mut self, plan: &GraphPhysicalPlan, statistics: &dyn StatisticsProvider) {
        self.profile = self.work.take_profile().map(|mut profile| {
            profile.attach_estimates(plan, statistics);
            profile
        });
    }

    /// Record the finished request on `span`, into `shard`'s counters, and,
    /// when a trigger fires, as a sampled plan. `elapsed` is the whole
    /// request as the shard saw it.
    pub(super) fn finish<T>(&self, span: &tracing::Span, shard: &GraphShard, elapsed: Duration, result: &Result<T, GraphError>) {
        let (parse, lower, bind) = self.preparation_split();
        let storage = Duration::from_micros(self.work.storage_us.load(Ordering::Relaxed));
        let operator_cpu = self.execute.saturating_sub(storage);
        let storage_calls = self.work.storage_calls.load(Ordering::Relaxed);
        // The storage calls and intermediate checks see each input, but an
        // operator that only combines its inputs, such as UNION, reports to
        // neither; the materialized result bounds what it produced.
        let widest_rows = self.work.widest_rows.load(Ordering::Relaxed).max(self.rows_materialized as u64);

        span.record("hydradb.query.stage.parse_us", micros(parse));
        span.record("hydradb.query.stage.lower_us", micros(lower));
        span.record("hydradb.query.stage.bind_us", micros(bind));
        span.record("hydradb.query.stage.lower_cache_hit", self.lower_cache_hit);
        span.record("hydradb.query.stage.snapshot_us", micros(self.snapshot));
        span.record("hydradb.query.stage.statistics_us", micros(self.statistics));
        span.record("hydradb.query.stage.plan_us", micros(self.plan));
        span.record("hydradb.query.stage.execute_us", micros(self.execute));
        span.record("hydradb.query.stage.storage_us", micros(storage));
        span.record("hydradb.query.stage.operator_cpu_us", micros(operator_cpu));
        span.record("hydradb.query.stage.result_us", micros(self.result));
        span.record("hydradb.query.storage.calls", storage_calls);
        if let Some((sequence, built_at_ms)) = self.planner_statistics {
            span.record("hydradb.query.stats.sequence", sequence);
            span.record("hydradb.query.stats.age_ms", graph_now_millis().saturating_sub(built_at_ms));
        }
        let access = self.work.access();
        let source = access.source();
        record_access_path(span, &access, source);
        span.record("hydradb.query.rows_widest", widest_rows);
        span.record("hydradb.query.rows_materialized", self.rows_materialized as u64);
        span.record("hydradb.query.rows_returned", self.rows_returned as u64);
        span.record(
            "hydradb.query.capability",
            if self.plan_identity.is_some() { "supported" } else { "unsupported" },
        );
        if let Err(error) = result {
            span.record("error.class", error.class());
        }

        let metrics = &shard.operation_metrics;
        for (counter, value) in [
            (&metrics.query_experimental_requests, 1),
            (&metrics.query_experimental_parse_us, micros(parse)),
            (&metrics.query_experimental_lower_us, micros(lower)),
            (&metrics.query_experimental_bind_us, micros(bind)),
            (&metrics.query_experimental_snapshot_us, micros(self.snapshot)),
            (&metrics.query_experimental_statistics_us, micros(self.statistics)),
            (&metrics.query_experimental_plan_us, micros(self.plan)),
            (&metrics.query_experimental_execute_us, micros(self.execute)),
            (&metrics.query_experimental_storage_us, micros(storage)),
            (&metrics.query_experimental_storage_calls, storage_calls),
            (&metrics.query_experimental_result_us, micros(self.result)),
            (&metrics.query_experimental_guard_stops, access.guard_stops),
        ] {
            counter.fetch_add(value, Ordering::Relaxed);
        }
        // Estimate accuracy of the statistics-chosen access, as sums the
        // dashboard divides and as a miss count either way. The widest
        // access stands for the request, as it does on the span.
        if let Some(report) = &access.widest {
            metrics
                .query_experimental_access_rows_estimated
                .fetch_add(report.estimated_rows, Ordering::Relaxed);
            metrics
                .query_experimental_access_rows_actual
                .fetch_add(report.entries_read, Ordering::Relaxed);
            let (estimated, actual) = (report.estimated_rows.max(1), report.entries_read.max(1));
            if actual > estimated.saturating_mul(2) {
                metrics.query_experimental_access_underestimates.fetch_add(1, Ordering::Relaxed);
            } else if actual.saturating_mul(2) < estimated {
                metrics.query_experimental_access_overestimates.fetch_add(1, Ordering::Relaxed);
            }
        }
        // The same request split by what chose its access path, so the mean
        // latency of plan-chosen requests reads against probe-chosen ones, and
        // the histogram gives their quantiles. Recorded together, so the
        // histogram's count and sum are the two counters by construction.
        let (requests, latency_us, latency) = match source {
            AccessPathSource::Statistics => (
                &metrics.query_experimental_access_statistics_requests,
                &metrics.query_experimental_access_statistics_us,
                &metrics.query_experimental_access_statistics_latency,
            ),
            AccessPathSource::Probe => (
                &metrics.query_experimental_access_probe_requests,
                &metrics.query_experimental_access_probe_us,
                &metrics.query_experimental_access_probe_latency,
            ),
            AccessPathSource::Guard => (
                &metrics.query_experimental_access_guard_requests,
                &metrics.query_experimental_access_guard_us,
                &metrics.query_experimental_access_guard_latency,
            ),
            AccessPathSource::Default => (
                &metrics.query_experimental_access_default_requests,
                &metrics.query_experimental_access_default_us,
                &metrics.query_experimental_access_default_latency,
            ),
        };
        let elapsed_us = micros(elapsed);
        requests.fetch_add(1, Ordering::Relaxed);
        latency_us.fetch_add(elapsed_us, Ordering::Relaxed);
        latency.record_micros(elapsed_us);

        let worst_estimate = self
            .profile
            .as_ref()
            .and_then(|profile| profile.worst_estimate(ESTIMATE_MISS_MIN_ROWS))
            .filter(|(_, ratio)| *ratio >= ESTIMATE_MISS_RATIO);
        if let Some(profile) = &self.profile {
            fold_operator_totals(shard, profile);
        }

        let Some(plan) = &self.plan_identity else {
            return;
        };
        let plan_hash = format!("{:016x}", plan.hash);
        // The plan says a scan is possible; the profile says whether one ran.
        // A range probe that answers never runs its fallback's scan.
        let full_scan = plan.may_scan_inventory && self.profile.as_ref().is_none_or(ExecutionProfile::ran_full_scan);
        span.record("hydradb.query.plan_hash", plan_hash.as_str());
        span.record("hydradb.query.full_scan", full_scan);
        let Some(reason) = sampling_reason(full_scan, widest_rows, worst_estimate.is_some(), elapsed) else {
            return;
        };
        metrics.query_experimental_sampled_plans.fetch_add(1, Ordering::Relaxed);
        let shape = plan.shape.as_str();
        span.record("hydradb.sampling.tail_keep", reason);
        span.record("hydradb.query.plan", shape);
        if let Some(profile) = &self.profile {
            emit_operator_spans(span, profile);
        }
        let hottest = self.profile.as_ref().and_then(ExecutionProfile::hottest);
        tracing::warn!(
            parent: span,
            reason,
            hydradb.query.hottest_operator = hottest.map_or("", |operator| operator.operator),
            hydradb.query.hottest_operator_self_us =
                hottest.map_or(0, |operator| micros(operator.self_elapsed)),
            hydradb.query.worst_estimate_operator =
                worst_estimate.map_or("", |(operator, _)| operator.operator),
            hydradb.query.worst_estimate_ratio = worst_estimate.map_or(0.0, |(_, ratio)| ratio),
            hydradb.query.plan_hash = %plan_hash,
            hydradb.query.full_scan = full_scan,
            hydradb.query.rows_widest = widest_rows,
            hydradb.query.storage.calls = storage_calls,
            elapsed_us = micros(elapsed),
            hydradb.query.stage.parse_us = micros(parse),
            hydradb.query.stage.lower_us = micros(lower),
            hydradb.query.stage.bind_us = micros(bind),
            hydradb.query.stage.snapshot_us = micros(self.snapshot),
            hydradb.query.stage.statistics_us = micros(self.statistics),
            hydradb.query.stage.plan_us = micros(self.plan),
            hydradb.query.stage.storage_us = micros(storage),
            hydradb.query.stage.operator_cpu_us = micros(operator_cpu),
            hydradb.query.stage.result_us = micros(self.result),
            hydradb.query.plan = %shape,
            "experimental query plan warrants attention"
        );
    }
}

/// What chose the access path, on the request span. Names a property, never a
/// value: the report carries no value to record.
fn record_access_path(span: &tracing::Span, access: &AccessObservation, source: AccessPathSource) {
    span.record("hydradb.query.access_path.source", source.as_str());
    span.record("hydradb.query.access_path.probe_ran", access.probe_runs > 0);
    if access.probe_runs > 0 {
        span.record("hydradb.query.access_path.probe_us", access.probe_us);
    }
    span.record("hydradb.query.access_path.guard_stopped", access.guard_stops > 0);
    let Some(report) = &access.widest else {
        return;
    };
    span.record("hydradb.query.access_path.kind", report.kind.as_str());
    if let Some(property) = &report.property {
        span.record("hydradb.query.access_path.property", property.as_str());
    }
    span.record("hydradb.query.access_path.rows_estimated", report.estimated_rows);
    span.record("hydradb.query.access_path.rows_actual", report.entries_read);
    // Actual over estimated, so above one is an underestimate and below one
    // an overestimate. Both sides are floored at one, as the operator ratio
    // is, so an empty read is not an infinite miss.
    let (estimated, actual) = ((report.estimated_rows as f64).max(1.0), (report.entries_read as f64).max(1.0));
    span.record("hydradb.query.access_path.estimate_ratio", actual / estimated);
}

/// Fold one request's operator profile into the shard's per-operator totals:
/// one lock per request.
fn fold_operator_totals(shard: &GraphShard, profile: &ExecutionProfile) {
    let request = request_operator_totals(profile);
    let mut totals = shard
        .operation_metrics
        .experimental_operators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for operator in &request {
        totals
            .entry(operator.operator)
            .or_insert_with(|| ExperimentalOperatorMetricsSnapshot {
                operator: operator.operator,
                ..ExperimentalOperatorMetricsSnapshot::default()
            })
            .accumulate(operator);
    }
}

/// One request's contribution to the per-operator totals, one entry per
/// operator node that ran.
///
/// Operators of one kind can appear more than once in a plan, for example a
/// `VertexLabelScan` in each arm of a UNION. `requests` and `estimate_misses`
/// both count requests, so each kind adds at most one to either however many
/// of its nodes ran or missed; the work counters add up across nodes.
fn request_operator_totals(profile: &ExecutionProfile) -> Vec<ExperimentalOperatorMetricsSnapshot> {
    let mut seen = std::collections::BTreeSet::new();
    let mut missed_kinds = std::collections::BTreeSet::new();
    profile
        .operators
        .iter()
        .filter(|operator| operator.invocations > 0)
        .map(|operator| {
            let missed = operator
                .estimated_rows
                .is_some_and(|estimated| estimated.max(operator.rows_out) >= ESTIMATE_MISS_MIN_ROWS)
                && operator.estimate_error().is_some_and(|ratio| ratio >= ESTIMATE_MISS_RATIO);
            ExperimentalOperatorMetricsSnapshot {
                operator: operator.operator,
                requests: u64::from(seen.insert(operator.operator)),
                invocations: operator.invocations,
                rows_in: operator.rows_in,
                rows_out: operator.rows_out,
                self_us: micros(operator.self_elapsed),
                storage_us: micros(operator.storage_elapsed),
                storage_requests: operator.storage_requests,
                storage_bytes: operator.storage_bytes,
                hydrated_vertices: operator.hydrated_vertices,
                scanned_relationships: operator.scanned_relationships,
                peak_retained_rows: operator.peak_retained_rows,
                estimate_misses: u64::from(missed && missed_kinds.insert(operator.operator)),
            }
        })
        .collect()
}

/// One `query.experimental.operator_profile` span per operator, children of
/// the request span, for a sampled request only. Opened after the fact, so a
/// span's own duration is meaningless: read `elapsed_us`, `self_us` and
/// `cpu_us`.
fn emit_operator_spans(parent: &tracing::Span, profile: &ExecutionProfile) {
    for operator in &profile.operators {
        operator_profile_span(parent, operator).in_scope(|| {});
    }
}

fn operator_profile_span(parent: &tracing::Span, operator: &OperatorProfile) -> tracing::Span {
    let span = tracing::info_span!(
        parent: parent,
        "query.experimental.operator_profile",
        hydradb.query.operator = operator.operator,
        hydradb.query.operator.access = operator.access,
        hydradb.query.operator.id = operator.id as u64,
        hydradb.query.operator.parent_id = tracing::field::Empty,
        hydradb.query.operator.invocations = operator.invocations,
        hydradb.query.operator.rows_in = operator.rows_in,
        hydradb.query.operator.rows_out = operator.rows_out,
        hydradb.query.operator.rows_estimated = tracing::field::Empty,
        hydradb.query.operator.estimate_ratio = tracing::field::Empty,
        hydradb.query.operator.elapsed_us = micros(operator.elapsed),
        hydradb.query.operator.self_us = micros(operator.self_elapsed),
        hydradb.query.operator.cpu_us = micros(operator.cpu_elapsed()),
        hydradb.query.operator.storage_us = micros(operator.storage_elapsed),
        hydradb.query.operator.storage_requests = operator.storage_requests,
        hydradb.query.operator.storage_bytes = operator.storage_bytes,
        hydradb.query.operator.hydrated_vertices = operator.hydrated_vertices,
        hydradb.query.operator.scanned_relationships = operator.scanned_relationships,
        hydradb.query.operator.peak_retained_rows = operator.peak_retained_rows,
    );
    if let Some(parent_id) = operator.parent {
        span.record("hydradb.query.operator.parent_id", parent_id as u64);
    }
    if let Some(estimated) = operator.estimated_rows {
        span.record("hydradb.query.operator.rows_estimated", estimated);
    }
    if let Some(ratio) = operator.estimate_error() {
        span.record("hydradb.query.operator.estimate_ratio", ratio);
    }
    span
}
