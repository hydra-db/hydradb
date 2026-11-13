//! Per-operator execution profiles (Workstream 11).
//!
//! The executor reports each operator invocation through
//! [`GraphRead::operator_started`](crate::GraphRead::operator_started) and
//! [`GraphRead::operator_finished`](crate::GraphRead::operator_finished). A
//! backend that wants numbers keeps an [`OperatorProfiler`], forwards those two
//! hooks to it, and charges it the storage work it does in between. The
//! profiler attributes that work to whichever operator is running, so no
//! operator body has to know it is being measured.
//!
//! Counting is always cheap: a handful of integer additions and two clock
//! reads per invocation. Whether the result becomes spans, and whether
//! estimates are attached, is the backend's decision after the query ends.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::{GraphPhysicalPlan, StatisticsProvider};

/// Everything one physical operator did during one query, summed over its
/// invocations. A correlated operator (the right side of a join) is invoked
/// once per outer row; its totals cover all of them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OperatorProfile {
    /// Pre-order position in the plan. `0` is the root.
    pub id: usize,
    /// The parent operator's `id`, `None` for the root.
    pub parent: Option<usize>,
    /// [`GraphPhysicalPlan::operator_name`]: a closed vocabulary.
    pub operator: &'static str,
    /// Provider-selected access strategy; separate from the physical operator name.
    pub access: Option<&'static str>,
    pub invocations: u64,
    /// Actual unbound all-vertex scans, excluding correlated seed reuse.
    pub full_scans: u64,
    /// Rows the operator's inputs produced for it.
    pub rows_in: u64,
    pub rows_out: u64,
    /// Wall time including inputs.
    pub elapsed: Duration,
    /// Wall time excluding inputs: this operator's own storage I/O and CPU.
    pub self_elapsed: Duration,
    /// Part of `self_elapsed` spent inside storage calls. Operator CPU is
    /// `self_elapsed - storage_elapsed`.
    pub storage_elapsed: Duration,
    /// Backend requests issued for this operator. For the HydraDB adapter,
    /// SlateDB point reads and range scans; see
    /// [`OperatorProfiler::record_storage_call`].
    pub storage_requests: u64,
    /// Bytes the backend reported reading for this operator. Backends define
    /// what they can measure; see their documentation.
    pub storage_bytes: u64,
    /// Whole vertex records materialized.
    pub hydrated_vertices: u64,
    /// Relationship records returned by storage.
    pub scanned_relationships: u64,
    /// Largest row collection this operator held at once, over all
    /// invocations: the larger of what it reported while working and what it
    /// returned.
    pub peak_retained_rows: u64,
    /// The planner's row estimate for this operator, when statistics could
    /// give one. Attached by [`ExecutionProfile::attach_estimates`].
    pub estimated_rows: Option<u64>,
}

impl OperatorProfile {
    /// Operator CPU: own time not spent waiting on storage.
    pub fn cpu_elapsed(&self) -> Duration {
        self.self_elapsed.saturating_sub(self.storage_elapsed)
    }

    /// How far the estimate was from the rows produced, as a ratio of at
    /// least `1.0`. Both sides are floored at one row so an empty result is
    /// not an infinite miss.
    ///
    /// `None` without an estimate, and `None` where comparing would mislead:
    /// an operator that only passes its input's estimate through (a filter's
    /// selectivity is unknown by design, so its estimate is an upper bound;
    /// a probe likewise carries its fallback's estimate), and an
    /// operator run more than once (a correlated input sees one outer row at
    /// a time while its estimate describes the whole population).
    pub fn estimate_error(&self) -> Option<f64> {
        if self.invocations != 1
            || matches!(
                self.operator,
                "FilterExec"
                    | "SortExec"
                    | "ProjectExec"
                    | "SkipExec"
                    | "LimitExec"
                    | "UnionExec"
                    | "RelationshipPropertyRangeProbe"
                    | "RelationshipTypeProbe"
            )
        {
            return None;
        }
        let estimated = self.estimated_rows? as f64;
        let actual = self.rows_out as f64;
        let (estimated, actual) = (estimated.max(1.0), actual.max(1.0));
        Some(estimated.max(actual) / estimated.min(actual))
    }
}

/// Per-operator profiles for one query, in plan pre-order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionProfile {
    pub operators: Vec<OperatorProfile>,
}

impl ExecutionProfile {
    /// Attach prepared scan estimates and derive relational estimates from
    /// `statistics`. A later statistics refresh cannot rewrite a scan decision.
    pub fn attach_estimates(&mut self, plan: &GraphPhysicalPlan, statistics: &dyn StatisticsProvider) {
        let estimates = crate::plan::estimate_operator_rows(plan, statistics);
        for (operator, estimate) in self.operators.iter_mut().zip(estimates) {
            operator.estimated_rows = estimate;
        }
    }

    /// Whether an `AllVertexScan` actually ran. Narrower than
    /// [`GraphPhysicalPlan::may_scan_inventory`]: a range probe's fallback
    /// can hold a scan that a successful probe never executes.
    pub fn ran_full_scan(&self) -> bool {
        self.operators.iter().any(|operator| operator.full_scans > 0)
    }

    /// The operator with the most own time, which is where a slow query's
    /// time actually went.
    pub fn hottest(&self) -> Option<&OperatorProfile> {
        self.operators
            .iter()
            .filter(|operator| operator.invocations > 0)
            .max_by_key(|operator| operator.self_elapsed)
    }

    /// The operator whose estimate missed by the largest ratio, among those
    /// where either side reached `min_rows`. Small operators miss by large
    /// ratios without mattering.
    pub fn worst_estimate(&self, min_rows: u64) -> Option<(&OperatorProfile, f64)> {
        self.operators
            .iter()
            .filter(|operator| {
                operator.invocations > 0
                    && operator
                        .estimated_rows
                        .is_some_and(|estimated| estimated.max(operator.rows_out) >= min_rows)
            })
            .filter_map(|operator| Some((operator, operator.estimate_error()?)))
            .max_by(|(_, left), (_, right)| left.total_cmp(right))
    }
}

/// The concurrent branch of one execution a hook belongs to, and the branch
/// that started it. Lane 0 is the execution itself. A branch run side by side
/// with others keeps its own operator stack, so its operators nest under their
/// own parents rather than under whichever sibling started last.
#[derive(Clone, Copy, Debug)]
struct Lane {
    id: usize,
    parent: usize,
}

tokio::task_local! {
    static LANE: Lane;
}

static NEXT_LANE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

fn current_lane() -> Lane {
    LANE.try_with(|lane| *lane).unwrap_or(Lane { id: 0, parent: 0 })
}

/// Run `future` as a new concurrent branch of the current execution: profiler
/// hooks made while it is polled are kept on the branch's own operator stack.
pub(crate) async fn in_new_lane<F: std::future::Future>(future: F) -> F::Output {
    let parent = current_lane().id;
    let id = NEXT_LANE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    LANE.scope(Lane { id, parent }, future).await
}

struct Frame {
    id: usize,
    started: Instant,
    children_elapsed: Duration,
    rows_in: u64,
    peak_retained_rows: u64,
}

/// Attributes execution work to physical operators. See the module docs.
///
/// Operators are identified by address within the plan passed to
/// [`Self::new`], so the profiler must observe an execution of that same
/// plan value. A hook for a plan node it does not know is ignored rather than
/// trusted.
pub struct OperatorProfiler {
    ids: HashMap<usize, usize>,
    operators: Vec<OperatorProfile>,
    stacks: HashMap<usize, Vec<Frame>>,
}

impl OperatorProfiler {
    pub fn new(plan: &GraphPhysicalPlan) -> Self {
        let mut profiler = Self {
            ids: HashMap::new(),
            operators: Vec::new(),
            stacks: HashMap::new(),
        };
        profiler.register(plan, None);
        profiler
    }

    fn register(&mut self, plan: &GraphPhysicalPlan, parent: Option<usize>) {
        let id = self.operators.len();
        self.ids.insert(std::ptr::from_ref(plan) as usize, id);
        self.operators.push(OperatorProfile {
            id,
            parent,
            operator: plan.operator_name(),
            access: match plan {
                GraphPhysicalPlan::VertexScan { scan } => Some(scan.access_kind()),
                _ => None,
            },
            ..OperatorProfile::default()
        });
        for child in plan.children() {
            self.register(child, Some(id));
        }
    }

    pub fn operator_started(&mut self, plan: &GraphPhysicalPlan) {
        let Some(&id) = self.ids.get(&(std::ptr::from_ref(plan) as usize)) else {
            return;
        };
        self.stacks.entry(current_lane().id).or_default().push(Frame {
            id,
            started: Instant::now(),
            children_elapsed: Duration::ZERO,
            rows_in: 0,
            peak_retained_rows: 0,
        });
    }

    pub fn operator_finished(&mut self, plan: &GraphPhysicalPlan, rows_out: usize) {
        let known = self.ids.get(&(std::ptr::from_ref(plan) as usize)).copied();
        let lane = current_lane();
        let Some(stack) = self.stacks.get_mut(&lane.id) else {
            return;
        };
        if known.is_none() || stack.last().map(|frame| frame.id) != known {
            return;
        }
        let frame = stack.pop().expect("checked above");
        // A concurrent lane ends with its root; each `UNION` run starts fresh
        // lanes, so an emptied one would otherwise stay for the whole query.
        if stack.is_empty() && lane.id != 0 {
            self.stacks.remove(&lane.id);
        }
        let elapsed = frame.started.elapsed();
        let rows_out = rows_out as u64;
        let operator = &mut self.operators[frame.id];
        operator.invocations += 1;
        operator.rows_in += frame.rows_in;
        operator.rows_out += rows_out;
        operator.elapsed += elapsed;
        operator.self_elapsed += elapsed.saturating_sub(frame.children_elapsed);
        operator.peak_retained_rows = operator.peak_retained_rows.max(frame.peak_retained_rows).max(rows_out);
        // A branch's root reports to the operator that started the branch.
        // Branches overlap, so their summed time can exceed the parent's wall
        // time; its self time then saturates at zero rather than going wrong.
        let own_parent = self.stacks.get(&lane.id).is_some_and(|stack| !stack.is_empty());
        let parent_lane = if own_parent || lane.id == 0 { lane.id } else { lane.parent };
        if let Some(parent) = self.stacks.get_mut(&parent_lane).and_then(|stack| stack.last_mut()) {
            parent.children_elapsed += elapsed;
            parent.rows_in += rows_out;
        }
    }

    fn current(&mut self) -> Option<&mut OperatorProfile> {
        let id = self.stacks.get(&current_lane().id)?.last()?.id;
        Some(&mut self.operators[id])
    }

    /// One storage call made by the running operator: its wall time, the
    /// backend requests it issued and the bytes they returned. A backend that
    /// cannot see below its own API reports one request per call.
    pub fn record_storage_call(&mut self, elapsed: Duration, requests: u64, bytes: u64) {
        if let Some(operator) = self.current() {
            operator.storage_requests += requests;
            operator.storage_elapsed += elapsed;
            operator.storage_bytes += bytes;
        }
    }

    pub fn record_full_scan(&mut self) {
        if let Some(operator) = self.current() {
            operator.full_scans += 1;
        }
    }

    pub fn record_hydrated_vertices(&mut self, vertices: usize) {
        if let Some(operator) = self.current() {
            operator.hydrated_vertices += vertices as u64;
        }
    }

    pub fn record_scanned_relationships(&mut self, relationships: usize) {
        if let Some(operator) = self.current() {
            operator.scanned_relationships += relationships as u64;
        }
    }

    pub fn record_retained_rows(&mut self, rows: usize) {
        if let Some(frame) = self.stacks.get_mut(&current_lane().id).and_then(|stack| stack.last_mut()) {
            frame.peak_retained_rows = frame.peak_retained_rows.max(rows as u64);
        }
    }

    pub fn finish(self) -> ExecutionProfile {
        ExecutionProfile { operators: self.operators }
    }
}
