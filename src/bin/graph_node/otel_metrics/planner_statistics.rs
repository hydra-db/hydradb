//! The planner statistics object each cell plans with, as gauges.
//!
//! A gauge of the *object in use*, not of the indexer's last publish: the
//! shard's cache reports what it holds when it finishes a read, and the age is
//! dated from the object's `built_at_ms` when the snapshot is taken, so it
//! keeps growing between reloads exactly as the plans' estimates go stale.
//!
//! Per cell, summed over the scopes on it like every other `cell_id` family:
//! `loaded` counts the scopes holding an object, and the age and sequence are
//! those of the **oldest** one, because staleness is the question these answer.
//!
//! Nothing loaded -- the flag is off, nothing is published, the object was
//! corrupt, or no query has planned since the shard opened -- renders
//! `loaded 0` and no age or sequence series on `/metrics`. OTel gauges cannot
//! withdraw a series, so there the same state is age and sequence `0` beside
//! `loaded 0`; read the age only where `loaded` is positive. A cell whose last
//! scope left the node is reported that way once, so its last observation does
//! not outlive it.

use std::collections::BTreeMap;
#[cfg(feature = "otlp")]
use std::collections::BTreeSet;

use hydradb::{PlannerStatisticsLoadedSnapshot, ScopedGraphShardRuntimeMetrics};

/// Prometheus family names, also the vocabulary the dashboard queries.
pub(crate) const PROMETHEUS_LOADED: &str = "graph_planner_statistics_loaded";
pub(crate) const PROMETHEUS_AGE: &str = "graph_planner_statistics_age_milliseconds";
pub(crate) const PROMETHEUS_SEQUENCE: &str = "graph_planner_statistics_storage_sequence";

/// OTel instrument names.
pub(crate) const OTEL_LOADED: &str = "hydradb.shard.planner_statistics.loaded";
pub(crate) const OTEL_AGE: &str = "hydradb.shard.planner_statistics.age";
pub(crate) const OTEL_SEQUENCE: &str = "hydradb.shard.planner_statistics.storage_sequence";

/// One cell's planner statistics across the scopes on it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CellPlannerStatistics {
    /// Scopes on the cell whose shard holds an object.
    pub(crate) loaded: u64,
    /// The oldest object among them; `None` when `loaded` is zero.
    pub(crate) oldest: Option<PlannerStatisticsLoadedSnapshot>,
}

/// Every cell on the node, including cells with nothing loaded, by `cell_id`.
pub(crate) fn by_cell(shards: &[ScopedGraphShardRuntimeMetrics]) -> BTreeMap<&str, CellPlannerStatistics> {
    let mut cells: BTreeMap<&str, CellPlannerStatistics> = BTreeMap::new();
    for shard in shards {
        let cell = cells.entry(shard.shard.cell_id.as_str()).or_default();
        let Some(statistics) = shard.shard.operational.planner_statistics else {
            continue;
        };
        cell.loaded += 1;
        if cell.oldest.is_none_or(|oldest| statistics.age_ms > oldest.age_ms) {
            cell.oldest = Some(statistics);
        }
    }
    cells
}

/// The cells to record this collection: every cell on the node, plus each cell
/// reported last time that has since left it, as nothing loaded. `reported`
/// becomes the cells on the node now, so a departed cell is zeroed once.
#[cfg(feature = "otlp")]
pub(crate) fn cells_to_record(
    reported: &mut BTreeSet<String>,
    shards: &[ScopedGraphShardRuntimeMetrics],
) -> BTreeMap<String, CellPlannerStatistics> {
    let mut cells = by_cell(shards)
        .into_iter()
        .map(|(cell_id, cell)| (cell_id.to_string(), cell))
        .collect::<BTreeMap<_, _>>();
    let current = cells.keys().cloned().collect::<BTreeSet<_>>();
    for departed in reported.difference(&current) {
        cells.insert(departed.clone(), CellPlannerStatistics::default());
    }
    *reported = current;
    cells
}

/// The `/metrics` half. The `# TYPE` lines are unconditional so the families
/// declare themselves on a node with no shards.
pub(crate) fn append(output: &mut String, shards: &[ScopedGraphShardRuntimeMetrics]) {
    use std::fmt::Write as _;
    for name in [PROMETHEUS_LOADED, PROMETHEUS_AGE, PROMETHEUS_SEQUENCE] {
        writeln!(output, "# TYPE {name} gauge").unwrap();
    }
    for (cell_id, cell) in by_cell(shards) {
        writeln!(output, "{PROMETHEUS_LOADED}{{cell_id=\"{cell_id}\"}} {}", cell.loaded).unwrap();
        if let Some(oldest) = cell.oldest {
            writeln!(output, "{PROMETHEUS_AGE}{{cell_id=\"{cell_id}\"}} {}", oldest.age_ms).unwrap();
            writeln!(output, "{PROMETHEUS_SEQUENCE}{{cell_id=\"{cell_id}\"}} {}", oldest.storage_sequence).unwrap();
        }
    }
}

/// The OTel half: three observable gauges keyed by `hydradb.cell_id`.
#[cfg(feature = "otlp")]
#[derive(Debug)]
pub(super) struct Gauges {
    loaded: hydradb_telemetry::meter::ObservableGauge,
    age: hydradb_telemetry::meter::ObservableGauge,
    sequence: hydradb_telemetry::meter::ObservableGauge,
    /// The cells recorded last collection, to zero one that has left.
    reported: std::sync::Mutex<BTreeSet<String>>,
}

#[cfg(feature = "otlp")]
impl Gauges {
    pub(super) fn register(providers: &hydradb_telemetry::otlp::Providers) -> Self {
        use hydradb_telemetry::meter::ObservableGauge;
        let meter = providers.meter(super::METER_NAME);
        Self {
            loaded: ObservableGauge::register(
                &meter,
                OTEL_LOADED,
                "Scopes on the cell whose shard holds a planner statistics object",
                "{object}",
            ),
            age: ObservableGauge::register(
                &meter,
                OTEL_AGE,
                "Age of the oldest planner statistics object in use on the cell, from its build time; 0 when none is loaded",
                "ms",
            ),
            sequence: ObservableGauge::register(
                &meter,
                OTEL_SEQUENCE,
                "Storage sequence the oldest planner statistics object in use on the cell was built at; 0 when none is loaded",
                "{sequence}",
            ),
            reported: std::sync::Mutex::default(),
        }
    }

    pub(super) fn record(&self, shards: &[ScopedGraphShardRuntimeMetrics]) {
        use hydradb_telemetry::semconv::L_CELL_ID;
        let cells = {
            let mut reported = self.reported.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            cells_to_record(&mut reported, shards)
        };
        for (cell_id, cell) in &cells {
            let labels = [(L_CELL_ID, cell_id.as_str())];
            let oldest = cell.oldest.unwrap_or_default();
            for result in [
                self.loaded.record(&labels, cell.loaded as f64),
                self.age.record(&labels, oldest.age_ms as f64),
                self.sequence.record(&labels, oldest.storage_sequence as f64),
            ] {
                if let Err(error) = result {
                    tracing::warn!(%error, "planner statistics gauge was not published");
                }
            }
        }
    }
}
