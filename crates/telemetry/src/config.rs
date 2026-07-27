//! Service identity and the environment-driven configuration.

use std::time::Duration;

/// Which binary is emitting.
///
/// The two binaries currently log in two different formats with no service or
/// binary name on either, so the only way to tell a `graph-node` line from a
/// `graph-indexer` line in a shared sink is to recognise the message text.
/// This type is what fixes that, and it is the one argument [`crate::init`]
/// cannot default.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ServiceIdentity {
    /// `src/bin/graph-node.rs` — serves Bolt and HTTP, owns cell writers.
    GraphNode,
    /// `src/bin/graph-indexer.rs` — the artifact compile loop.
    GraphIndexer,
}

impl ServiceIdentity {
    /// The OTel `service.name` resource attribute. This is what a backend
    /// groups and filters by.
    pub fn service_name(self) -> &'static str {
        match self {
            Self::GraphNode => "turbolay-graph-node",
            Self::GraphIndexer => "turbolay-graph-indexer",
        }
    }

    /// A flat `binary` field stamped on every log line.
    ///
    /// Redundant with `service.name` on purpose. Resource attributes live
    /// outside the log record, so somebody tailing a pod with `grep` at 2am
    /// cannot see them. Both, not one.
    pub fn binary(self) -> &'static str {
        match self {
            Self::GraphNode => "graph-node",
            Self::GraphIndexer => "graph-indexer",
        }
    }

    /// The binary's own `EnvFilter` variable, checked before `RUST_LOG`.
    ///
    /// Separate variables matter more than they look: today both binaries read
    /// `RUST_LOG`, so turning the indexer up to `debug` in a shared chart also
    /// turns up three graph-nodes serving live query traffic.
    pub fn log_env_var(self) -> &'static str {
        match self {
            Self::GraphNode => "GRAPH_NODE_LOG",
            Self::GraphIndexer => "GRAPH_INDEXER_LOG",
        }
    }
}

/// Which OTLP wire encoding to use.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum OtlpProtocol {
    /// `http/protobuf`. The default: no `tonic`/`hyper` stack, and it traverses
    /// proxies and service meshes without configuration.
    #[default]
    HttpProtobuf,
    /// `grpc`. Somewhat more efficient at high span volume, which is not
    /// currently the constraint.
    Grpc,
}

impl OtlpProtocol {
    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "http/protobuf" | "http/proto" => Some(Self::HttpProtobuf),
            "grpc" => Some(Self::Grpc),
            _ => None,
        }
    }
}

/// Everything [`crate::init`] needs.
///
/// Built with [`TelemetryConfig::from_env`] in the binaries and by hand in
/// tests. Nothing here reads a clock or opens a socket; construction is pure,
/// which is what makes the resolution rules below testable without an
/// environment.
#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    /// Which binary. No default.
    pub identity: ServiceIdentity,
    /// `EnvFilter` directives.
    pub filter: String,
    /// Emit JSON rather than human-readable text.
    pub json: bool,
    /// OTLP collector endpoint. `None` disables the exporter entirely.
    pub otlp_endpoint: Option<String>,
    /// Extra OTLP headers, typically auth, as `key=value` pairs.
    pub otlp_headers: Vec<(String, String)>,
    /// OTLP wire encoding.
    pub otlp_protocol: OtlpProtocol,
    /// Head sampling ratio for traces with no forced-keep reason.
    pub sample_ratio: f64,
    /// Export batch timeout.
    pub export_timeout: Duration,
    /// How often the metrics reader collects and exports.
    ///
    /// Also the period of the collection task that feeds the observable
    /// instruments, and therefore how often the two cache-gauge sets take their
    /// mutexes — the one metrics cost that is not a relaxed atomic load.
    pub metric_export_interval: Duration,
    /// `service.version`.
    pub service_version: String,
    /// `service.instance.id` — the pod name.
    pub instance_id: String,
    /// `deployment.environment.name`.
    pub deployment_environment: Option<String>,
    /// Read-path slow-query threshold, in milliseconds.
    pub slow_query_ms: u64,
}

impl TelemetryConfig {
    /// Default sampling ratio. A starting guess, not a derived number — revisit
    /// once the span volume from a three-node staging cluster is known.
    pub const DEFAULT_SAMPLE_RATIO: f64 = 0.05;

    /// Default slow-query threshold.
    pub const DEFAULT_SLOW_QUERY_MS: u64 = 1_000;

    /// Default metric export interval: 60s.
    ///
    /// Matched to a Prometheus scrape rather than picked for smoothness. The
    /// counters are lock-free atomic loads and could be collected far more
    /// often; the two cache-gauge sets take a dozen cache mutexes per cell per
    /// collection, and those are the same mutexes the read path takes on every
    /// lookup. 60s keeps that to once a minute.
    pub const DEFAULT_METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

    /// A config that installs the fmt layer only. The base for tests, and what
    /// [`Self::from_env`] degrades to when no OTLP endpoint is set.
    pub fn new(identity: ServiceIdentity) -> Self {
        Self {
            identity,
            filter: "info".to_string(),
            json: true,
            otlp_endpoint: None,
            otlp_headers: Vec::new(),
            otlp_protocol: OtlpProtocol::default(),
            sample_ratio: Self::DEFAULT_SAMPLE_RATIO,
            export_timeout: Duration::from_secs(10),
            metric_export_interval: Self::DEFAULT_METRIC_EXPORT_INTERVAL,
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            instance_id: "unknown".to_string(),
            deployment_environment: None,
            slow_query_ms: Self::DEFAULT_SLOW_QUERY_MS,
        }
    }

    /// Read the configuration from the process environment.
    ///
    /// Standard `OTEL_*` names wherever OTel defines one — the collector
    /// sidecar already sets those, and nobody should have to learn a
    /// Turbolay-specific spelling for an endpoint.
    ///
    /// **An unset `OTEL_EXPORTER_OTLP_ENDPOINT` means no exporter.** Tests,
    /// `cargo run` and the examples must not need a collector, must not block
    /// on startup, and must not print a connection error every five seconds.
    pub fn from_env(identity: ServiceIdentity) -> Self {
        Self::from_env_with(identity, |key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] against an arbitrary lookup, so the resolution rules
    /// can be tested without mutating the real environment — which is process
    /// global and makes concurrent tests flaky.
    pub fn from_env_with<F>(identity: ServiceIdentity, get: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut config = Self::new(identity);

        // Per-binary filter first, then RUST_LOG, then the `info` default.
        config.filter = get(identity.log_env_var())
            .or_else(|| get("RUST_LOG"))
            .unwrap_or_else(|| "info".to_string());

        if let Some(value) = get("GRAPH_LOG_FORMAT") {
            config.json = !value.eq_ignore_ascii_case("text");
        }

        config.otlp_endpoint = get("OTEL_EXPORTER_OTLP_ENDPOINT")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        if let Some(raw) = get("OTEL_EXPORTER_OTLP_HEADERS") {
            config.otlp_headers = parse_headers(&raw);
        }

        if let Some(protocol) = get("OTEL_EXPORTER_OTLP_PROTOCOL")
            .as_deref()
            .and_then(OtlpProtocol::parse)
        {
            config.otlp_protocol = protocol;
        }

        // An unparseable or out-of-range ratio falls back to the default rather
        // than failing startup. Telemetry misconfiguration must never be the
        // reason a node will not boot.
        if let Some(ratio) = get("OTEL_TRACES_SAMPLER_ARG")
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|ratio| (0.0..=1.0).contains(ratio))
        {
            config.sample_ratio = ratio;
        }

        if let Some(ms) = get("GRAPH_SLOW_QUERY_MS").and_then(|value| value.trim().parse().ok()) {
            config.slow_query_ms = ms;
        }

        // `OTEL_METRIC_EXPORT_INTERVAL` is the OTel-defined name and is in
        // milliseconds. Zero is rejected rather than honoured: it would spin the
        // reader thread and, with it, the collection task holding the cache
        // mutexes — a telemetry setting that can stall the read path is one a
        // typo must not be able to reach.
        if let Some(interval) = get("OTEL_METRIC_EXPORT_INTERVAL")
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
        {
            config.metric_export_interval = interval;
        }

        if let Some(version) = get("GRAPH_BUILD_VERSION") {
            config.service_version = version;
        }

        config.instance_id = get("POD_NAME")
            .or_else(|| get("HOSTNAME"))
            .unwrap_or_else(|| "unknown".to_string());

        config.deployment_environment =
            get("DEPLOYMENT_ENVIRONMENT").or_else(|| get("ENVIRONMENT"));

        config
    }

    /// Whether an OTLP exporter will be installed.
    pub fn otlp_enabled(&self) -> bool {
        self.otlp_endpoint.is_some()
    }
}

/// Parse the `OTEL_EXPORTER_OTLP_HEADERS` format: comma-separated `key=value`.
///
/// A value may itself contain `=` (base64 padding in a bearer token is the
/// common case), so the split is on the *first* `=` only.
fn parse_headers(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            let key = key.trim();
            if key.is_empty() {
                return None;
            }
            Some((key.to_string(), value.trim().to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_from<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn identities_are_distinct_everywhere() {
        let node = ServiceIdentity::GraphNode;
        let indexer = ServiceIdentity::GraphIndexer;
        assert_ne!(node.service_name(), indexer.service_name());
        assert_ne!(node.binary(), indexer.binary());
        assert_ne!(node.log_env_var(), indexer.log_env_var());
    }

    #[test]
    fn no_endpoint_means_no_exporter() {
        let config = TelemetryConfig::from_env_with(ServiceIdentity::GraphNode, env_from(&[]));
        assert!(!config.otlp_enabled());
    }

    /// An endpoint set to the empty string is a Helm template that rendered
    /// nothing, not a request to export to "".
    #[test]
    fn blank_endpoint_is_treated_as_unset() {
        let config = TelemetryConfig::from_env_with(
            ServiceIdentity::GraphNode,
            env_from(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "   ")]),
        );
        assert!(!config.otlp_enabled());
    }

    #[test]
    fn per_binary_filter_beats_rust_log() {
        let config = TelemetryConfig::from_env_with(
            ServiceIdentity::GraphIndexer,
            env_from(&[("RUST_LOG", "warn"), ("GRAPH_INDEXER_LOG", "debug")]),
        );
        assert_eq!(config.filter, "debug");
    }

    /// The node must not pick up the indexer's variable, which is the entire
    /// point of having two.
    #[test]
    fn binaries_do_not_read_each_others_filters() {
        let config = TelemetryConfig::from_env_with(
            ServiceIdentity::GraphNode,
            env_from(&[("GRAPH_INDEXER_LOG", "trace"), ("RUST_LOG", "warn")]),
        );
        assert_eq!(config.filter, "warn");
    }

    #[test]
    fn falls_back_to_info() {
        let config = TelemetryConfig::from_env_with(ServiceIdentity::GraphNode, env_from(&[]));
        assert_eq!(config.filter, "info");
    }

    #[test]
    fn headers_split_on_first_equals_only() {
        let parsed = parse_headers("authorization=Bearer abc==,x-tenant=acme");
        assert_eq!(
            parsed,
            vec![
                ("authorization".to_string(), "Bearer abc==".to_string()),
                ("x-tenant".to_string(), "acme".to_string()),
            ]
        );
    }

    #[test]
    fn malformed_headers_are_skipped_not_fatal() {
        let parsed = parse_headers("novalue,=orphan,good=1");
        assert_eq!(parsed, vec![("good".to_string(), "1".to_string())]);
    }

    #[test]
    fn protocol_defaults_to_http_protobuf() {
        let config = TelemetryConfig::from_env_with(ServiceIdentity::GraphNode, env_from(&[]));
        assert_eq!(config.otlp_protocol, OtlpProtocol::HttpProtobuf);
    }

    #[test]
    fn protocol_parses_grpc() {
        let config = TelemetryConfig::from_env_with(
            ServiceIdentity::GraphNode,
            env_from(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")]),
        );
        assert_eq!(config.otlp_protocol, OtlpProtocol::Grpc);
    }

    /// Telemetry misconfiguration must never be why a node fails to boot.
    #[test]
    fn nonsense_sampler_arg_falls_back_to_default() {
        for value in ["banana", "-1", "1.5", ""] {
            let config = TelemetryConfig::from_env_with(
                ServiceIdentity::GraphNode,
                env_from(&[("OTEL_TRACES_SAMPLER_ARG", value)]),
            );
            assert_eq!(
                config.sample_ratio,
                TelemetryConfig::DEFAULT_SAMPLE_RATIO,
                "{value} should have fallen back"
            );
        }
    }

    #[test]
    fn sampler_arg_accepts_the_range_ends() {
        for (value, expected) in [("0", 0.0), ("1", 1.0), ("0.25", 0.25)] {
            let config = TelemetryConfig::from_env_with(
                ServiceIdentity::GraphNode,
                env_from(&[("OTEL_TRACES_SAMPLER_ARG", value)]),
            );
            assert_eq!(config.sample_ratio, expected);
        }
    }

    #[test]
    fn metric_interval_defaults_to_a_minute() {
        let config = TelemetryConfig::from_env_with(ServiceIdentity::GraphNode, env_from(&[]));
        assert_eq!(config.metric_export_interval, Duration::from_secs(60));
    }

    #[test]
    fn metric_interval_is_read_in_milliseconds() {
        let config = TelemetryConfig::from_env_with(
            ServiceIdentity::GraphNode,
            env_from(&[("OTEL_METRIC_EXPORT_INTERVAL", " 15000 ")]),
        );
        assert_eq!(config.metric_export_interval, Duration::from_secs(15));
    }

    /// Zero would spin the reader thread, and the reader thread is what takes
    /// the cache mutexes. It falls back rather than being honoured.
    #[test]
    fn a_nonsense_metric_interval_falls_back() {
        for value in ["0", "banana", "-1", ""] {
            let config = TelemetryConfig::from_env_with(
                ServiceIdentity::GraphNode,
                env_from(&[("OTEL_METRIC_EXPORT_INTERVAL", value)]),
            );
            assert_eq!(
                config.metric_export_interval,
                TelemetryConfig::DEFAULT_METRIC_EXPORT_INTERVAL,
                "{value} should have fallen back"
            );
        }
    }

    #[test]
    fn pod_name_wins_over_hostname() {
        let config = TelemetryConfig::from_env_with(
            ServiceIdentity::GraphNode,
            env_from(&[("POD_NAME", "node-2"), ("HOSTNAME", "box")]),
        );
        assert_eq!(config.instance_id, "node-2");
    }
}
