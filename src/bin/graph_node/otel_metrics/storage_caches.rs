//! SlateDB's two read caches: the block cache in memory, per cell, and the
//! object store cache on the node's volume, for the whole node.
//!
//! The block cache rows are per cell, summed over the scopes on it, like every
//! family added after the scoped storage families: `scope` is an unbounded
//! tenant root and nothing new joins the families that already carry it. The
//! block cache counters are already on `/metrics` per scope, as
//! `graph_storage_block_cache_accesses`, so only the OTLP export gains them
//! here. Their values come from each open scope's SlateDB recorder, so a scope
//! that closes takes its counts out of the cell's sum. The sum then falls, and
//! a `rate()` reads that as a counter reset. Scopes close on rebalance and
//! eviction, rarely next to a query pass, and carrying closed scopes' totals
//! would need a running total per cell that the storage snapshot does not keep.
//!
//! The disk cache rows describe the node's one shared disk cache
//! (`hydradb::SharedDiskCache`): every scope on every cell reads through it,
//! so there is no per-cell or per-scope share to report, and its counters run
//! for the life of the process whatever scopes open and close. A node with no
//! disk cache declares the families and reports no series.

use std::collections::BTreeMap;

use hydradb::{GraphStorageMetricsSnapshot, ScopedGraphShardRuntimeMetrics, SharedDiskCacheMetrics};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Counter,
    ByteCounter,
    ByteGauge,
}

struct Row<S> {
    /// `None` for a series `/metrics` already renders elsewhere.
    prometheus: Option<&'static str>,
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    otlp: &'static str,
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    description: &'static str,
    kind: Kind,
    value: fn(&S) -> u64,
}

/// The block cache, per cell, from each scope's storage snapshot.
const CELL_ROWS: [Row<GraphStorageMetricsSnapshot>; 4] = [
    Row {
        prometheus: None,
        otlp: "hydradb.storage.block_cache.data.hits",
        description: "SlateDB block cache hits on data blocks",
        kind: Kind::Counter,
        value: |s| s.block_cache_data_hits,
    },
    Row {
        prometheus: None,
        otlp: "hydradb.storage.block_cache.data.misses",
        description: "SlateDB block cache misses on data blocks",
        kind: Kind::Counter,
        value: |s| s.block_cache_data_misses,
    },
    Row {
        prometheus: None,
        otlp: "hydradb.storage.block_cache.filter.hits",
        description: "SlateDB block cache hits on filter blocks",
        kind: Kind::Counter,
        value: |s| s.block_cache_filter_hits,
    },
    Row {
        prometheus: None,
        otlp: "hydradb.storage.block_cache.filter.misses",
        description: "SlateDB block cache misses on filter blocks",
        kind: Kind::Counter,
        value: |s| s.block_cache_filter_misses,
    },
];

/// The node's shared disk cache.
const NODE_ROWS: [Row<SharedDiskCacheMetrics>; 4] = [
    Row {
        prometheus: Some("graph_storage_disk_cache_part_hits_total"),
        otlp: "hydradb.storage.disk_cache.part_hits",
        description: "Object store cache part reads answered from the node's volume",
        kind: Kind::Counter,
        value: |s| s.part_hits,
    },
    Row {
        prometheus: Some("graph_storage_disk_cache_part_accesses_total"),
        otlp: "hydradb.storage.disk_cache.part_accesses",
        description: "Object store cache part reads, hit or miss",
        kind: Kind::Counter,
        value: |s| s.part_accesses,
    },
    Row {
        prometheus: Some("graph_storage_disk_cache_bytes"),
        otlp: "hydradb.storage.disk_cache.size",
        description: "Bytes the object store cache holds on the node's volume",
        kind: Kind::ByteGauge,
        value: |s| s.bytes,
    },
    Row {
        prometheus: Some("graph_storage_disk_cache_evicted_bytes_total"),
        otlp: "hydradb.storage.disk_cache.evicted",
        description: "Bytes the object store cache evicted to stay under its budget",
        kind: Kind::ByteCounter,
        value: |s| s.evicted_bytes,
    },
];

/// Each cell on the node with its block cache rows summed over the scopes on
/// it, in [`CELL_ROWS`] order.
fn by_cell(shards: &[ScopedGraphShardRuntimeMetrics]) -> BTreeMap<&str, [u64; CELL_ROWS.len()]> {
    let mut cells: BTreeMap<&str, [u64; CELL_ROWS.len()]> = BTreeMap::new();
    for shard in shards {
        let sums = cells.entry(shard.shard.cell_id.as_str()).or_default();
        for (sum, row) in sums.iter_mut().zip(&CELL_ROWS) {
            *sum = sum.saturating_add((row.value)(&shard.shard.storage));
        }
    }
    cells
}

fn prometheus_type(kind: Kind) -> &'static str {
    if kind == Kind::ByteGauge {
        "gauge"
    } else {
        "counter"
    }
}

/// The `/metrics` half: the disk cache families, declared unconditionally so
/// they exist on a node with no disk cache, and one unlabelled series each
/// when it has one. The block cache rows have no `/metrics` series here.
pub(crate) fn append(output: &mut String, disk: Option<SharedDiskCacheMetrics>) {
    use std::fmt::Write as _;
    for row in &NODE_ROWS {
        if let Some(name) = row.prometheus {
            writeln!(output, "# TYPE {name} {}", prometheus_type(row.kind)).unwrap();
        }
    }
    if let Some(disk) = disk {
        for row in &NODE_ROWS {
            if let Some(name) = row.prometheus {
                writeln!(output, "{name} {}", (row.value)(&disk)).unwrap();
            }
        }
    }
}

#[cfg(feature = "otlp")]
#[derive(Debug)]
enum Instrument {
    Counter(hydradb_telemetry::meter::ObservableCounter),
    Gauge(hydradb_telemetry::meter::ObservableGauge),
}

#[cfg(feature = "otlp")]
impl Instrument {
    fn record(
        &self,
        labels: &[(hydradb_telemetry::semconv::MetricLabel, &str)],
        value: u64,
    ) -> Result<(), hydradb_telemetry::meter::CounterError> {
        match self {
            Instrument::Counter(counter) => counter.record(labels, value),
            Instrument::Gauge(gauge) => gauge.record(labels, value as f64),
        }
    }
}

/// The OTLP half: the block cache keyed by `hydradb.cell_id`, the disk cache
/// for the node.
#[cfg(feature = "otlp")]
#[derive(Debug)]
pub(super) struct Instruments {
    cell_instruments: Vec<Instrument>,
    node_instruments: Vec<Instrument>,
    /// The UCUM unit each instrument was registered with: [`CELL_ROWS`], then
    /// [`NODE_ROWS`].
    #[cfg_attr(not(test), allow(dead_code))]
    units: Vec<&'static str>,
}

#[cfg(feature = "otlp")]
impl Instruments {
    pub(super) fn register(providers: &hydradb_telemetry::otlp::Providers) -> Self {
        use hydradb_telemetry::meter::{CounterSpec, CounterUnit, ObservableCounter, ObservableGauge};
        let meter = providers.meter(super::METER_NAME);
        // One instrument per row, with the UCUM unit it carries.
        macro_rules! register {
            ($rows:expr) => {
                $rows
                    .iter()
                    .map(|row| {
                        let counter = |unit: CounterUnit| {
                            let spec = CounterSpec {
                                name: row.otlp,
                                description: row.description,
                                unit,
                            };
                            (Instrument::Counter(ObservableCounter::register(&meter, spec)), unit.ucum())
                        };
                        match row.kind {
                            Kind::Counter => counter(CounterUnit::Count),
                            Kind::ByteCounter => counter(CounterUnit::Bytes),
                            Kind::ByteGauge => (
                                Instrument::Gauge(ObservableGauge::register(&meter, row.otlp, row.description, "By")),
                                "By",
                            ),
                        }
                    })
                    .unzip::<_, _, Vec<_>, Vec<_>>()
            };
        }
        let (cell_instruments, mut units) = register!(CELL_ROWS);
        let (node_instruments, node_units) = register!(NODE_ROWS);
        units.extend(node_units);
        Self {
            cell_instruments,
            node_instruments,
            units,
        }
    }

    pub(super) fn record(&self, shards: &[ScopedGraphShardRuntimeMetrics]) {
        use hydradb_telemetry::semconv::L_CELL_ID;
        for (cell_id, sums) in &by_cell(shards) {
            let labels = [(L_CELL_ID, *cell_id)];
            for (instrument, value) in self.cell_instruments.iter().zip(sums) {
                if let Err(error) = instrument.record(&labels, *value) {
                    tracing::warn!(%cell_id, %error, "storage cache metric was not published");
                }
            }
        }
    }

    /// Record the node's disk cache; nothing when it has none.
    pub(super) fn record_disk(&self, disk: Option<SharedDiskCacheMetrics>) {
        let Some(disk) = disk else {
            return;
        };
        for (instrument, row) in self.node_instruments.iter().zip(&NODE_ROWS) {
            if let Err(error) = instrument.record(&[], (row.value)(&disk)) {
                tracing::warn!(%error, "disk cache metric was not published");
            }
        }
    }
}
