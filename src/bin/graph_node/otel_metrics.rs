//! The OTel half of the duration-histogram export, and the name table it is
//! driven by.
//!
//! # Two exports, one enumeration
//!
//! The kernel enumerates its histograms by **Rust identifier** — `read_latency`,
//! `query_rows_latency` — and knows nothing about either exposition vocabulary
//! (`slatedb_graph_kernel::ClientQueryMetricsSnapshot::histogram_fields` and
//! friends). The two vocabularies live here and in [`crate::admin`]: this module
//! holds the OTel names, `admin.rs` holds the Prometheus names, and
//! [`tests::every_histogram_field_reaches_both_exports`] fails the build if a
//! field the kernel enumerates is missing from either. That is the property
//! §1.6 of `docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md` asks
//! for: adding a histogram cannot silently reach one export and not the other.
//!
//! # Names
//!
//! §1.9: `db.*` where a semantic convention genuinely exists, `turbolay.*`
//! otherwise. Of the five histograms exactly one quantity has a semconv name —
//! client query duration is `db.client.operation.duration`, stable, and fixed in
//! **seconds** while the kernel measures in microseconds. The conversion is one
//! call at the export boundary ([`ExportUnit`]) and a bound table in seconds
//! never travels back towards the kernel.
//!
//! `db.namespace` is **deliberately omitted** from the `db.client.*` family even
//! though semconv marks it required-if-applicable. It is applicable — the
//! namespace is the `scope` — and `scope` is the unbounded tenant root the
//! metric-label registry exists to keep off metrics. A conformance checker will
//! flag its absence; that flag is the design, not a bug.
//!
//! ## The one place this deviates from the plan, and why
//!
//! §1.10 splits client latency into a read and a write distribution, and §1.9
//! names both `db.client.operation.duration`. Semconv separates them with
//! `db.operation.name`, but that key is not in
//! `turbolay_telemetry::semconv::METRIC_LABELS` and `MetricLabel`'s constructor
//! is private to that module, so the label cannot be attached from here. Two
//! series under one instrument name with no distinguishing label would collapse
//! into one — silently re-conflating exactly what the split exists to separate.
//!
//! So the two are separate instruments, `db.client.operation.duration.read` and
//! `…​.write`, keeping the semconv stem as a prefix. When `L_DB_OPERATION_NAME`
//! is added to the registry, collapse the two rows below into one row named
//! `db.client.operation.duration` and pass the operation as a label; the
//! enumeration and both name tables are already shaped for it.
//!
//! # Wiring, and what is still missing
//!
//! [`NodeHistograms::register`] takes a
//! `turbolay_telemetry::otlp::Providers`, and **nothing in this process can
//! produce one**: `turbolay_telemetry::init` returns a `TelemetryGuard` whose
//! `providers` field is private with no accessor, and nothing calls
//! `opentelemetry::global::set_meter_provider`. So the OTel half registers no
//! instruments yet. It needs exactly one of two one-line additions to
//! `crates/telemetry` — a `TelemetryGuard::providers()` accessor, or a
//! `set_meter_provider` call in `layers::install` — after which `main` builds a
//! `NodeHistograms` once and an interval task calls the `record_*` methods with
//! the same snapshots `/metrics` serves. Everything on this side of that line
//! is here.
//!
//! `record_transport` has a second gap of its own: `graph-node` instantiates
//! neither `TcpQueryServer` nor `TcpQueryCellClient`, so it holds no
//! `QueryTransportMetricsSnapshot` to record. The name table and the rendering
//! cover `rpc_latency` and `serve_latency` regardless, because the field lives
//! in the kernel and the export must not be the thing that decides which
//! binary is allowed to have it.
//!
//! # Why a counter family and not a histogram data point
//!
//! `opentelemetry` 0.32 has no observable histogram and `opentelemetry_sdk`
//! 0.32 has no `MetricProducer`, so a histogram computed in the kernel and read
//! from a cached snapshot cannot reach OTLP as a histogram data point. It
//! reaches it as a family of observable counters keyed by `le`, which is what a
//! Prometheus histogram already is. `turbolay_telemetry::meter` owns that
//! rendering, and owning it in one place is what stops the two exports
//! disagreeing about where a bucket ends. See that module for the full
//! argument.

// Only the recording half names a snapshot, and that half is behind `otlp`.
#[cfg(feature = "otlp")]
use slatedb_graph_kernel::DurationHistogramSnapshot;

/// The unit an exported histogram's bounds and sum are **rendered** in.
///
/// The kernel measures in microseconds and only ever measures in microseconds.
/// This is a boundary conversion, and it exists because
/// `db.client.operation.duration` is a stable semantic convention fixed in
/// seconds — a series claiming that name in microseconds is worse than a series
/// with a Turbolay name.
///
/// It is deliberately shared by both exports. The names may diverge; the *unit*
/// must not, or `histogram_quantile` over the Prometheus family and the same
/// query over the OTLP family answer differently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExportUnit {
    /// Export as the kernel measures. UCUM `us`.
    Microseconds,
    /// Divide by 1e6 at the boundary. UCUM `s`.
    Seconds,
}

impl ExportUnit {
    /// Render one finite bucket bound as an `le` label value.
    ///
    /// Character-for-character what `turbolay_telemetry::meter::HistogramUnit`
    /// does, and it has to be: two renderings of the same bound that differ by
    /// a trailing zero are two series to every backend downstream. `{}` on
    /// `f64` is the shortest representation that round-trips, so 100 µs renders
    /// as `0.0001` and 30 s as `30`.
    pub fn render_bound(self, bound_us: u64) -> String {
        match self {
            Self::Microseconds => bound_us.to_string(),
            Self::Seconds => (bound_us as f64 / 1_000_000.0).to_string(),
        }
    }

    /// Render a microsecond sum in the export unit.
    pub fn render_sum(self, sum_us: u64) -> String {
        match self {
            Self::Microseconds => sum_us.to_string(),
            Self::Seconds => (sum_us as f64 / 1_000_000.0).to_string(),
        }
    }

    /// The same choice, spelled the way the meter spells it.
    #[cfg(feature = "otlp")]
    fn meter_unit(self) -> turbolay_telemetry::meter::HistogramUnit {
        use turbolay_telemetry::meter::HistogramUnit;
        match self {
            Self::Microseconds => HistogramUnit::Microseconds,
            Self::Seconds => HistogramUnit::Seconds,
        }
    }
}

/// One row of the OTel name table.
#[derive(Clone, Copy, Debug)]
pub struct OtelHistogram {
    /// The kernel's Rust identifier — the key both name tables are keyed by.
    pub field: &'static str,
    /// OTel metric name stem. The meter derives `{name}.bucket`, `{name}.sum`
    /// and `{name}.count` from it.
    pub name: &'static str,
    /// Instrument description.
    pub description: &'static str,
    /// Bound and sum unit.
    pub unit: ExportUnit,
}

/// The OTel name table. One row per histogram the kernel enumerates.
///
/// Adding a histogram to a snapshot type and not adding it here fails
/// [`tests::every_histogram_field_reaches_both_exports`].
pub const OTEL_HISTOGRAMS: &[OtelHistogram] = &[
    OtelHistogram {
        field: "read_latency",
        name: "db.client.operation.duration.read",
        description: "End-to-end client read execution",
        unit: ExportUnit::Seconds,
    },
    OtelHistogram {
        field: "write_latency",
        name: "db.client.operation.duration.write",
        description: "End-to-end client mutation execution",
        unit: ExportUnit::Seconds,
    },
    OtelHistogram {
        field: "query_rows_latency",
        name: "turbolay.query.rows.duration",
        description: "Shard row-query execution",
        unit: ExportUnit::Microseconds,
    },
    OtelHistogram {
        field: "rpc_latency",
        name: "turbolay.query.transport.rpc.duration",
        description: "Query-transport client RPC round-trip",
        unit: ExportUnit::Microseconds,
    },
    OtelHistogram {
        field: "serve_latency",
        name: "turbolay.query.transport.serve.duration",
        description: "Query-transport server executor time",
        unit: ExportUnit::Microseconds,
    },
];

/// The OTel name and unit for a kernel field identifier.
pub fn otel_histogram(field: &str) -> Option<&'static OtelHistogram> {
    OTEL_HISTOGRAMS.iter().find(|export| export.field == field)
}

/// The instrumentation scope every instrument registered here belongs to.
#[cfg(feature = "otlp")]
const METER_NAME: &str = "turbolay.graph_node";

/// Every registered histogram family, keyed by the kernel's field identifier.
///
/// Registration builds the instruments once; the `record_*` methods publish the
/// latest cached snapshot into them. The OTel callbacks run on the periodic
/// reader's own OS thread and only ever read, which is why nothing here is
/// `async` and why a failure is returned rather than panicked.
#[cfg(feature = "otlp")]
#[derive(Debug)]
pub struct NodeHistograms {
    registered:
        std::collections::HashMap<&'static str, turbolay_telemetry::meter::ObservableHistogram>,
}

#[cfg(feature = "otlp")]
impl NodeHistograms {
    /// Register every row of [`OTEL_HISTOGRAMS`] against the process's meter.
    ///
    /// The ladder comes from the kernel
    /// ([`slatedb_graph_kernel::DURATION_BUCKET_BOUNDS_US`]) rather than being
    /// restated here, so the Prometheus rendering in [`crate::admin`] and this
    /// one cannot disagree about where a bucket ends.
    pub fn register(
        providers: &turbolay_telemetry::otlp::Providers,
    ) -> Result<Self, turbolay_telemetry::meter::HistogramError> {
        use turbolay_telemetry::meter::{HistogramSpec, ObservableHistogram};

        let meter = providers.meter(METER_NAME);
        let mut registered = std::collections::HashMap::with_capacity(OTEL_HISTOGRAMS.len());
        for export in OTEL_HISTOGRAMS {
            let histogram = ObservableHistogram::register(
                &meter,
                HistogramSpec {
                    name: export.name,
                    description: export.description,
                    unit: export.unit.meter_unit(),
                },
                &slatedb_graph_kernel::DURATION_BUCKET_BOUNDS_US,
            )?;
            registered.insert(export.field, histogram);
        }
        Ok(Self { registered })
    }

    /// Publish one field's snapshot.
    ///
    /// A field with no registered instrument is a hole in [`OTEL_HISTOGRAMS`],
    /// which `every_histogram_field_reaches_both_exports` makes impossible to
    /// ship. It is skipped rather than raised because losing one series is a
    /// better outcome on a metrics thread than losing the interval.
    fn record(
        &self,
        field: &'static str,
        labels: &[(turbolay_telemetry::semconv::MetricLabel, &str)],
        snapshot: &DurationHistogramSnapshot,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        let Some(histogram) = self.registered.get(field) else {
            debug_assert!(false, "{field} is enumerated but not in OTEL_HISTOGRAMS");
            return Ok(());
        };
        histogram.record_snapshot(labels, &snapshot.bucket_counts, snapshot.sum_us)
    }

    /// The process-global client query histograms.
    ///
    /// Carries `db.system.name` because that is the attribute a vendor's
    /// database view keys on, and putting `db.*` names on the wire while
    /// omitting it would pay the cost of §1.9's vocabulary split and collect
    /// none of the benefit. It carries no `scope`: that is §1.4's whole point,
    /// and the label registry makes it a type error rather than a review
    /// comment.
    pub fn record_client(
        &self,
        snapshot: &slatedb_graph_kernel::ClientQueryMetricsSnapshot,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        use turbolay_telemetry::semconv::{DB_SYSTEM_NEO4J, L_DB_SYSTEM_NAME};

        for (field, histogram) in snapshot.histogram_fields() {
            self.record(field, &[(L_DB_SYSTEM_NAME, DB_SYSTEM_NEO4J)], histogram)?;
        }
        Ok(())
    }

    /// One shard's operational histograms, labelled by `cell_id` alone.
    ///
    /// Never `cell_id × edge_type`: an 18-bucket family times 96 is 1,728
    /// series per instrument per node, which is where §1.3's cardinality
    /// arithmetic stops being affordable. And never `scope`, which `/metrics`
    /// does carry — that divergence is deliberate and is the reason both
    /// exports exist.
    pub fn record_shard(
        &self,
        metrics: &slatedb_graph_kernel::ScopedGraphShardRuntimeMetrics,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        use turbolay_telemetry::semconv::L_CELL_ID;

        for (field, histogram) in metrics.shard.operational.histogram_fields() {
            self.record(
                field,
                &[(L_CELL_ID, metrics.shard.cell_id.as_str())],
                histogram,
            )?;
        }
        Ok(())
    }

    /// The query-transport histograms.
    ///
    /// Unlabelled: only one of `rpc_latency` and `serve_latency` is ever
    /// non-empty on a given instance, so the instrument name already says which
    /// side of the wire it was measured on.
    pub fn record_transport(
        &self,
        snapshot: &slatedb_graph_kernel::QueryTransportMetricsSnapshot,
    ) -> Result<(), turbolay_telemetry::meter::HistogramError> {
        for (field, histogram) in snapshot.histogram_fields() {
            self.record(field, &[], histogram)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use slatedb_graph_kernel::{
        ClientQueryMetricsSnapshot, GraphOperationalMetricsSnapshot, QueryTransportMetricsSnapshot,
        DURATION_BUCKET_BOUNDS_US, DURATION_BUCKET_COUNT,
    };

    use super::*;
    use crate::admin::{prometheus_histogram, PROMETHEUS_HISTOGRAMS};

    /// Every histogram field the kernel enumerates, from every snapshot type
    /// that has one. This is the enumeration both exports are derived from, so
    /// it is also the only list this test may consult.
    fn enumerated_fields() -> Vec<&'static str> {
        let client = ClientQueryMetricsSnapshot::default();
        let operational = GraphOperationalMetricsSnapshot::default();
        let transport = QueryTransportMetricsSnapshot::default();
        client
            .histogram_fields()
            .map(|(field, _)| field)
            .chain(operational.histogram_fields().map(|(field, _)| field))
            .chain(transport.histogram_fields().map(|(field, _)| field))
            .collect()
    }

    /// §1.6: "the two must not disagree about names". A histogram that reaches
    /// one export and not the other is the failure mode that section is most
    /// worried about producing in a year, and this is the cheap test it asks
    /// for — extended to the reverse direction, because a name table row for a
    /// field nobody records is dead weight that reads like a missing recording
    /// site.
    #[test]
    fn every_histogram_field_reaches_both_exports() {
        let enumerated = enumerated_fields();
        assert!(
            !enumerated.is_empty(),
            "the kernel enumerates no histograms at all"
        );

        for field in &enumerated {
            assert!(
                otel_histogram(field).is_some(),
                "{field} is recorded by the kernel but has no OTel name in OTEL_HISTOGRAMS"
            );
            assert!(
                prometheus_histogram(field).is_some(),
                "{field} is recorded by the kernel but has no Prometheus name in PROMETHEUS_HISTOGRAMS"
            );
        }

        for export in OTEL_HISTOGRAMS {
            assert!(
                enumerated.contains(&export.field),
                "OTEL_HISTOGRAMS names {}, which no snapshot enumerates",
                export.field
            );
        }
        for export in PROMETHEUS_HISTOGRAMS {
            assert!(
                enumerated.contains(&export.field),
                "PROMETHEUS_HISTOGRAMS names {}, which no snapshot enumerates",
                export.field
            );
        }
    }

    /// The names may diverge — `/metrics` keeps `graph_*` and OTLP takes
    /// `db.*`/`turbolay.*` — but a bucket bound rendered two different ways is
    /// a single measurement that answers two different questions.
    #[test]
    fn the_two_exports_agree_about_the_unit() {
        for field in enumerated_fields() {
            let otel = otel_histogram(field).expect("named for OTel");
            let prometheus = prometheus_histogram(field).expect("named for Prometheus");
            assert_eq!(
                otel.unit, prometheus.unit,
                "{field} is exported in two different units"
            );
        }
    }

    /// A Prometheus name in seconds whose unit says microseconds — or the
    /// reverse — is a series every dashboard reads off by a factor of a
    /// million, and nothing downstream can detect it.
    #[test]
    fn prometheus_names_carry_their_unit_suffix() {
        for export in PROMETHEUS_HISTOGRAMS {
            let expected = match export.unit {
                ExportUnit::Seconds => "_seconds",
                ExportUnit::Microseconds => "_microseconds",
            };
            assert!(
                export.name.ends_with(expected),
                "{} is exported in {:?} but is not named {expected}",
                export.name,
                export.unit
            );
        }
    }

    /// Two histograms sharing an instrument name is two populations collapsing
    /// into one series — which is precisely what the read/write split exists to
    /// prevent, and precisely what would happen if both rows took the bare
    /// semconv name. See the module docs.
    #[test]
    fn no_two_fields_share_an_exported_name() {
        for table in [
            OTEL_HISTOGRAMS
                .iter()
                .map(|export| export.name)
                .collect::<Vec<_>>(),
            PROMETHEUS_HISTOGRAMS
                .iter()
                .map(|export| export.name)
                .collect::<Vec<_>>(),
        ] {
            let mut sorted = table.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                table.len(),
                "two fields share a name: {table:?}"
            );
        }
    }

    /// `le` is the join between the two exports. The seconds rendering is the
    /// one that can go wrong — `0.000100000` and `0.0001` are two series.
    #[test]
    fn bounds_render_identically_to_the_meters_rendering() {
        let microseconds: Vec<String> = DURATION_BUCKET_BOUNDS_US
            .iter()
            .map(|bound| ExportUnit::Microseconds.render_bound(*bound))
            .collect();
        assert_eq!(microseconds.first().map(String::as_str), Some("100"));
        assert_eq!(microseconds.last().map(String::as_str), Some("30000000"));

        let seconds: Vec<String> = DURATION_BUCKET_BOUNDS_US
            .iter()
            .map(|bound| ExportUnit::Seconds.render_bound(*bound))
            .collect();
        assert_eq!(seconds.first().map(String::as_str), Some("0.0001"));
        assert_eq!(seconds.last().map(String::as_str), Some("30"));
        assert_eq!(ExportUnit::Seconds.render_sum(2_500_000), "2.5");
        assert_eq!(ExportUnit::Microseconds.render_sum(2_500_000), "2500000");
    }

    /// The ladder is exported once from the kernel precisely so nothing here
    /// restates it. If that ever stops holding, every `le` in both exports is
    /// off by one bucket.
    #[test]
    fn the_ladder_comes_from_the_kernel() {
        assert_eq!(DURATION_BUCKET_BOUNDS_US.len() + 1, DURATION_BUCKET_COUNT);
    }
}
