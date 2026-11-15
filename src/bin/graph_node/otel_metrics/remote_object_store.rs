//! Remote object store traffic, exported from the process totals the counting
//! store in `hydradb::object_store_from_env` keeps. One table drives both
//! exports, so Prometheus and OTLP cannot disagree about which counters exist.

use hydradb::RemoteObjectStoreSnapshot;

/// One exported counter: its Prometheus name, its OTLP name, what it means,
/// whether it counts bytes, and where its value comes from.
struct Row {
    prometheus: &'static str,
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    otlp: &'static str,
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    description: &'static str,
    #[cfg_attr(not(feature = "otlp"), allow(dead_code))]
    bytes: bool,
    value: fn(&RemoteObjectStoreSnapshot) -> u64,
}

const ROWS: [Row; 5] = [
    Row {
        prometheus: "graph_object_store_remote_gets_total",
        otlp: "hydradb.object_store.remote.gets",
        description:
            "GET calls into the remote object store client below the disk cache, ranged or not; client-internal retries not counted",
        bytes: false,
        value: |s| s.gets,
    },
    Row {
        prometheus: "graph_object_store_remote_ranged_gets_total",
        otlp: "hydradb.object_store.remote.ranged_gets",
        description: "The GET requests that asked for a byte range",
        bytes: false,
        value: |s| s.ranged_gets,
    },
    Row {
        prometheus: "graph_object_store_remote_get_bytes_total",
        otlp: "hydradb.object_store.remote.get_bytes",
        description: "GET body bytes delivered to callers, counted as chunks arrive; not wire bytes",
        bytes: true,
        value: |s| s.get_bytes,
    },
    Row {
        prometheus: "graph_object_store_remote_puts_total",
        otlp: "hydradb.object_store.remote.puts",
        description: "PUT calls attempted against the remote object store client, one per multipart part",
        bytes: false,
        value: |s| s.puts,
    },
    Row {
        prometheus: "graph_object_store_remote_put_bytes_total",
        otlp: "hydradb.object_store.remote.put_bytes",
        description: "Payload bytes of the PUT calls attempted, not bytes confirmed transferred",
        bytes: true,
        value: |s| s.put_bytes,
    },
];

#[cfg(feature = "otlp")]
#[derive(Debug)]
pub(super) struct Instruments {
    #[allow(clippy::type_complexity)]
    counters: Vec<(hydradb_telemetry::meter::ObservableCounter, fn(&RemoteObjectStoreSnapshot) -> u64)>,
}

#[cfg(feature = "otlp")]
impl Instruments {
    pub(super) fn register(providers: &hydradb_telemetry::otlp::Providers) -> Self {
        use hydradb_telemetry::meter::{CounterSpec, CounterUnit, ObservableCounter};
        let meter = providers.meter(super::METER_NAME);
        let counters = ROWS
            .iter()
            .map(|row| {
                let spec = CounterSpec {
                    name: row.otlp,
                    description: row.description,
                    unit: if row.bytes { CounterUnit::Bytes } else { CounterUnit::Count },
                };
                (ObservableCounter::register(&meter, spec), row.value)
            })
            .collect();
        Self { counters }
    }

    pub(super) fn record(&self) {
        let snapshot = hydradb::remote_object_store_snapshot();
        for (counter, value) in &self.counters {
            if let Err(error) = counter.record(&[], value(&snapshot)) {
                tracing::warn!(%error, "remote object store counter was not published");
            }
        }
    }
}

/// Direct Prometheus export, independent of the OTLP collector.
pub(crate) fn append(output: &mut String) {
    render(output, &hydradb::remote_object_store_snapshot());
}

fn render(output: &mut String, snapshot: &RemoteObjectStoreSnapshot) {
    use std::fmt::Write as _;
    for row in &ROWS {
        writeln!(output, "# TYPE {} counter", row.prometheus).unwrap();
        writeln!(output, "{} {}", row.prometheus, (row.value)(snapshot)).unwrap();
    }
}
