//! The proxy's metrics, in one Prometheus registry rendered as text on
//! `/metrics`. Other proxy crates register their own metrics into
//! [`Metrics::registry`] so one scrape sees everything.

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};
use turbolay_proxy_types::{Access, NodeId, Protocol};

use crate::nodes::NodeRegistry;
use crate::router::ReadPolicy;

/// How one attempt at a node ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteOutcome {
    Ok,
    NodeError,
    RefusedNotOwner,
    ConnectFailed,
    LostBeforePull,
    LostAfterPull,
    AdmitTimeout,
}

impl RouteOutcome {
    const ALL: [Self; 7] = [
        Self::Ok,
        Self::NodeError,
        Self::RefusedNotOwner,
        Self::ConnectFailed,
        Self::LostBeforePull,
        Self::LostAfterPull,
        Self::AdmitTimeout,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NodeError => "node_error",
            Self::RefusedNotOwner => "refused_not_owner",
            Self::ConnectFailed => "connect_failed",
            Self::LostBeforePull => "lost_before_pull",
            Self::LostAfterPull => "lost_after_pull",
            Self::AdmitTimeout => "admit_timeout",
        }
    }
}

/// Why a unit was retried.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    NotOwner,
    ConnectFailed,
    LostBeforePull,
}

impl RetryClass {
    const ALL: [Self; 3] = [Self::NotOwner, Self::ConnectFailed, Self::LostBeforePull];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotOwner => "not_owner",
            Self::ConnectFailed => "connect_failed",
            Self::LostBeforePull => "lost_before_pull",
        }
    }
}

const PROTOCOLS: [Protocol; 2] = [Protocol::Bolt, Protocol::Http];
const ACCESSES: [Access; 2] = [Access::Read, Access::Write];

fn counter(registry: &Registry, name: &str, help: &str) -> IntCounter {
    let counter = IntCounter::new(name, help).expect("valid metric");
    registry.register(Box::new(counter.clone())).expect("metric registered once");
    counter
}

fn counter_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let counter = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
    registry.register(Box::new(counter.clone())).expect("metric registered once");
    counter
}

fn gauge(registry: &Registry, name: &str, help: &str) -> IntGauge {
    let gauge = IntGauge::new(name, help).expect("valid metric");
    registry.register(Box::new(gauge.clone())).expect("metric registered once");
    gauge
}

fn gauge_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let gauge = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric");
    registry.register(Box::new(gauge.clone())).expect("metric registered once");
    gauge
}

/// The per node metric families, shared by every node's [`NodeCounters`].
#[derive(Clone)]
pub(crate) struct NodeMetrics {
    pub(crate) admit_queued: IntCounter,
    pub(crate) admit_queued_nanos: IntCounter,
    pub(crate) admit_timeouts: IntCounter,
    units_in_flight: IntGaugeVec,
    permits: IntGaugeVec,
    pool_waits: IntCounterVec,
    pool_wait_nanos: IntCounterVec,
    bytes_relayed: IntCounterVec,
    attempts: IntCounterVec,
}

impl NodeMetrics {
    fn new(registry: &Registry) -> Self {
        Self {
            admit_queued: counter(
                registry,
                "turbolay_proxy_admit_queued_total",
                "Admissions that had to wait for a permit.",
            ),
            admit_queued_nanos: counter(
                registry,
                "turbolay_proxy_admit_queued_nanos_total",
                "Time admissions spent waiting for a permit, in nanoseconds.",
            ),
            admit_timeouts: counter(
                registry,
                "turbolay_proxy_admit_timeouts_total",
                "Admissions refused because no permit freed up in time.",
            ),
            units_in_flight: gauge_vec(
                registry,
                "turbolay_proxy_node_units_in_flight",
                "Units admitted to a node and not yet finished, from this replica.",
                &["node"],
            ),
            permits: gauge_vec(
                registry,
                "turbolay_proxy_node_permits",
                "This replica's permit share for a node.",
                &["node"],
            ),
            pool_waits: counter_vec(
                registry,
                "turbolay_proxy_pool_waits_total",
                "Backend connection checkouts.",
                &["node"],
            ),
            pool_wait_nanos: counter_vec(
                registry,
                "turbolay_proxy_pool_wait_nanos_total",
                "Time spent checking out backend connections, in nanoseconds.",
                &["node"],
            ),
            bytes_relayed: counter_vec(
                registry,
                "turbolay_proxy_bytes_relayed_total",
                "Response bytes relayed from a node.",
                &["node"],
            ),
            attempts: counter_vec(
                registry,
                "turbolay_proxy_attempts_total",
                "Attempts at a node, by how they ended.",
                &["node", "protocol", "access", "outcome"],
            ),
        }
    }

    /// The counters of a node joining the snapshot.
    pub(crate) fn node(&self, id: &NodeId) -> NodeCounters {
        let node = [id.as_str()];
        NodeCounters {
            id: id.clone(),
            attempts: self.attempts.clone(),
            pool_waits: self.pool_waits.with_label_values(&node),
            pool_wait_nanos: self.pool_wait_nanos.with_label_values(&node),
            bytes_relayed: self.bytes_relayed.with_label_values(&node),
        }
    }

    /// Drop the series of a node that left the snapshot.
    pub(crate) fn forget(&self, id: &NodeId) {
        let node = id.as_str();
        for family in [&self.units_in_flight, &self.permits] {
            let _ = family.remove_label_values(&[node]);
        }
        for family in [&self.pool_waits, &self.pool_wait_nanos, &self.bytes_relayed] {
            let _ = family.remove_label_values(&[node]);
        }
        for protocol in PROTOCOLS {
            for access in ACCESSES {
                for outcome in RouteOutcome::ALL {
                    let _ = self
                        .attempts
                        .remove_label_values(&[node, protocol.as_str(), access.as_str(), outcome.as_str()]);
                }
            }
        }
    }
}

/// Per node counters, kept on the node's admission state.
pub struct NodeCounters {
    id: NodeId,
    attempts: IntCounterVec,
    pub pool_waits: IntCounter,
    pub pool_wait_nanos: IntCounter,
    pub bytes_relayed: IntCounter,
}

impl NodeCounters {
    pub fn record(&self, protocol: Protocol, access: Access, outcome: RouteOutcome) {
        self.attempts
            .with_label_values(&[self.id.as_str(), protocol.as_str(), access.as_str(), outcome.as_str()])
            .inc();
    }

    pub fn attempts(&self, protocol: Protocol, access: Access, outcome: RouteOutcome) -> u64 {
        self.attempts
            .get_metric_with_label_values(&[self.id.as_str(), protocol.as_str(), access.as_str(), outcome.as_str()])
            .map_or(0, |counter| counter.get())
    }
}

/// Proxy-wide metrics.
pub struct Metrics {
    registry: Registry,
    replicas: IntGauge,
    pub(crate) snapshot_changes: IntCounter,
    client_connections: IntGaugeVec,
    client_connections_total: IntCounterVec,
    units: IntCounterVec,
    retries: IntCounterVec,
    reads_by_policy: IntCounterVec,
    pub owner_cache_hits: IntCounter,
    pub owner_cache_updates: IntCounter,
    pub owner_hints_unknown_node: IntCounter,
    pub route_requests: IntCounter,
    pub auth_failures: IntCounter,
    pub protocol_errors: IntCounter,
    pub multi_statement_transactions: IntCounter,
    pub rollbacks_after_write: IntCounter,
    pub(crate) nodes: NodeMetrics,
}

impl Default for Metrics {
    fn default() -> Self {
        let registry = Registry::new();
        let metrics = Self {
            replicas: gauge(
                &registry,
                "turbolay_proxy_replicas",
                "Ready proxy replicas sharing each node's budget.",
            ),
            snapshot_changes: counter(
                &registry,
                "turbolay_proxy_snapshot_changes_total",
                "Route snapshots applied after the first.",
            ),
            client_connections: gauge_vec(
                &registry,
                "turbolay_proxy_client_connections",
                "Open client connections.",
                &["protocol"],
            ),
            client_connections_total: counter_vec(
                &registry,
                "turbolay_proxy_client_connections_total",
                "Client connections accepted.",
                &["protocol"],
            ),
            units: counter_vec(
                &registry,
                "turbolay_proxy_units_total",
                "Units of work received.",
                &["protocol", "access"],
            ),
            retries: counter_vec(&registry, "turbolay_proxy_retries_total", "Units retried, by why.", &["class"]),
            reads_by_policy: counter_vec(
                &registry,
                "turbolay_proxy_reads_by_policy_total",
                "Reads routed, by read policy.",
                &["policy"],
            ),
            owner_cache_hits: counter(
                &registry,
                "turbolay_proxy_owner_cache_hits_total",
                "Writes routed by a learned owner.",
            ),
            owner_cache_updates: counter(&registry, "turbolay_proxy_owner_cache_updates_total", "Owner hints learned."),
            owner_hints_unknown_node: counter(
                &registry,
                "turbolay_proxy_owner_hints_unknown_node_total",
                "Owner hints naming a node outside the snapshot.",
            ),
            route_requests: counter(&registry, "turbolay_proxy_route_requests_total", "Bolt ROUTE requests answered."),
            auth_failures: counter(&registry, "turbolay_proxy_auth_failures_total", "Client authentication failures."),
            protocol_errors: counter(&registry, "turbolay_proxy_protocol_errors_total", "Client protocol errors."),
            multi_statement_transactions: counter(
                &registry,
                "turbolay_proxy_multi_statement_transactions_total",
                "Transactions refused a second statement.",
            ),
            rollbacks_after_write: counter(
                &registry,
                "turbolay_proxy_rollbacks_after_write_total",
                "ROLLBACKs refused because the transaction's write already committed.",
            ),
            nodes: NodeMetrics::new(&registry),
            registry,
        };
        // Every proxy-wide series exists from the start, at zero, so a query
        // sees a zero rather than no series.
        for protocol in PROTOCOLS {
            metrics.client_connections.with_label_values(&[protocol.as_str()]);
            metrics.client_connections_total.with_label_values(&[protocol.as_str()]);
            for access in ACCESSES {
                metrics.units.with_label_values(&[protocol.as_str(), access.as_str()]);
            }
        }
        for class in RetryClass::ALL {
            metrics.retries.with_label_values(&[class.as_str()]);
        }
        for policy in [ReadPolicy::Fleet, ReadPolicy::Owner] {
            metrics.reads_by_policy.with_label_values(&[policy.as_str()]);
        }
        metrics
    }
}

impl Metrics {
    /// The registry `/metrics` renders. Register other metrics here to serve
    /// them from the same endpoint.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn client_connected(&self, protocol: Protocol) {
        self.client_connections.with_label_values(&[protocol.as_str()]).inc();
        self.client_connections_total.with_label_values(&[protocol.as_str()]).inc();
    }

    pub fn client_disconnected(&self, protocol: Protocol) {
        self.client_connections.with_label_values(&[protocol.as_str()]).dec();
    }

    pub fn unit(&self, protocol: Protocol, access: Access) {
        self.units.with_label_values(&[protocol.as_str(), access.as_str()]).inc();
    }

    pub fn units(&self, protocol: Protocol, access: Access) -> u64 {
        self.units.with_label_values(&[protocol.as_str(), access.as_str()]).get()
    }

    pub fn retry(&self, class: RetryClass) {
        self.retries.with_label_values(&[class.as_str()]).inc();
    }

    pub fn retries(&self, class: RetryClass) -> u64 {
        self.retries.with_label_values(&[class.as_str()]).get()
    }

    pub fn read_routed(&self, policy: ReadPolicy) {
        self.reads_by_policy.with_label_values(&[policy.as_str()]).inc();
    }

    /// Prometheus text exposition of the registry, with the gauges read from
    /// the per node state set first.
    pub fn render(&self, nodes: &NodeRegistry, replicas: usize) -> String {
        self.replicas.set(replicas as i64);
        for node in nodes.all() {
            let label = [node.id.as_str()];
            self.nodes.units_in_flight.with_label_values(&label).set(node.in_flight() as i64);
            self.nodes.permits.with_label_values(&label).set(node.share() as i64);
        }
        let mut out = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut out)
            .expect("text encoding into a Vec cannot fail");
        String::from_utf8(out).expect("the text format is UTF-8")
    }
}
