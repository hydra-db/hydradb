//! Configuration from the environment. Names shared with graph-node
//! (`GRAPH_*`) keep graph-node's spelling and defaults, so one configmap
//! configures both and they hash over the same fleet by construction.
//!
//! [`Settings`] declares every variable, its type and its default. clap parses
//! them from `--NAME=value` arguments built from the environment rather than
//! through its own `env` support, because a variable set to an empty or blank
//! value must count as unset, as graph-node treats it, and clap would take the
//! empty string as the value.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use clap::builder::BoolishValueParser;
use clap::{ArgAction, CommandFactory, Parser, ValueEnum};
use hydradb_scope::database::DatabaseName;
use hydradb_scope::{GraphId, GraphScope, NamespacePath};
use turbolay_proxy_core::types::NodeInfo;
use turbolay_proxy_core::ReadPolicy;
use turbolay_proxy_topology::{static_nodes, NodeAddresses};

pub type ConfigResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Parse a node address list, naming the variable in the error as graph-node
/// does.
fn node_addresses(name: &str, raw: &str) -> ConfigResult<NodeAddresses> {
    raw.parse().map_err(|error| format!("{name} {error}").into())
}

/// Where the replica set comes from.
#[derive(Clone, Debug)]
pub enum ProxySet {
    Static(Vec<String>),
    File {
        path: PathBuf,
        interval: Duration,
    },
    #[cfg_attr(not(feature = "k8s"), allow(dead_code))]
    K8s {
        namespace: String,
        service: String,
        port: u16,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProxySetKind {
    Static,
    File,
    K8s,
}

/// Every variable the proxy reads. Each is also a `--NAME=value` flag, and a
/// flag wins over the variable.
#[derive(Debug, Parser)]
#[command(name = "turbolay-proxy", args_override_self = true)]
struct Settings {
    #[arg(long = "TURBOLAY_PROXY_BOLT_ADDR", default_value = "0.0.0.0:7687")]
    bolt_addr: String,
    #[arg(long = "TURBOLAY_PROXY_HTTP_ADDR", default_value = "0.0.0.0:8443")]
    http_addr: String,
    #[arg(long = "TURBOLAY_PROXY_ADMIN_ADDR", default_value = "0.0.0.0:9090")]
    admin_addr: String,
    #[arg(long = "GRAPH_BOLT_NODE_ADDRESSES")]
    bolt_node_addresses: String,
    #[arg(long = "TURBOLAY_PROXY_HTTP_NODE_ADDRESSES")]
    http_node_addresses: Option<String>,
    #[arg(long = "TURBOLAY_PROXY_ADVERTISED_BOLT_ADDR")]
    advertised_bolt_addr: Option<String>,
    #[arg(long = "TURBOLAY_PROXY_SET", value_enum, default_value = "static")]
    proxy_set: ProxySetKind,
    #[arg(long = "TURBOLAY_PROXY_PEERS")]
    peers: Option<String>,
    #[arg(long = "TURBOLAY_PROXY_PEERS_FILE")]
    peers_file: Option<PathBuf>,
    #[arg(long = "TURBOLAY_PROXY_PEERS_POLL_MS", default_value_t = 250)]
    peers_poll_ms: u64,
    #[arg(long = "TURBOLAY_PROXY_K8S_NAMESPACE")]
    k8s_namespace: Option<String>,
    #[arg(long = "TURBOLAY_PROXY_K8S_SERVICE")]
    k8s_service: Option<String>,
    #[arg(long = "TURBOLAY_PROXY_K8S_PORT", default_value_t = 7687)]
    k8s_port: u16,
    /// `replicas` or `service:<host:port>`.
    #[arg(long = "TURBOLAY_PROXY_ADVERTISE", default_value = "replicas")]
    advertise: String,
    #[arg(long = "GRAPH_READ_ROUTING")]
    read_routing: Option<String>,
    #[arg(long = "GRAPH_DATABASE", default_value = "default")]
    database: String,
    #[arg(long = "GRAPH_NAMESPACE", default_value = "default")]
    namespace: String,
    #[arg(long = "GRAPH_ID", default_value = "default")]
    graph_id: String,
    #[arg(long = "GRAPH_CELL_ID", default_value = "cell-0")]
    cell_id: String,
    #[arg(long = "GRAPH_AUTH_TOKEN_FILE", default_value = "/var/run/secrets/slatedb-graph/auth-token")]
    auth_token_file: PathBuf,
    #[arg(long = "GRAPH_ALLOW_PLAINTEXT", action = ArgAction::Set, value_parser = BoolishValueParser::new(), default_value = "false")]
    allow_plaintext: bool,
    #[arg(long = "GRAPH_TLS_CERTIFICATE")]
    tls_certificate: Option<PathBuf>,
    #[arg(long = "GRAPH_TLS_PRIVATE_KEY")]
    tls_private_key: Option<PathBuf>,
    #[arg(long = "TURBOLAY_PROXY_NODE_BUDGET", default_value_t = 256)]
    node_budget: usize,
    #[arg(long = "TURBOLAY_PROXY_ADMIT_WAIT_MS", default_value_t = 5_000)]
    admit_wait_ms: u64,
    #[arg(long = "TURBOLAY_PROXY_ROUTE_TTL_SECS", default_value_t = 10)]
    route_ttl_secs: i64,
    #[arg(long = "TURBOLAY_PROXY_MAX_ATTEMPTS", default_value_t = 3)]
    max_attempts: u32,
    /// Defaults to the available parallelism.
    #[arg(long = "TURBOLAY_PROXY_THREADS")]
    threads: Option<usize>,
    #[arg(long = "TURBOLAY_PROXY_UPSTREAM_H2", action = ArgAction::Set, value_parser = BoolishValueParser::new(), default_value = "true")]
    upstream_h2: bool,
    #[arg(long = "TURBOLAY_PROXY_MAX_H2_STREAMS", default_value_t = 64)]
    max_h2_streams: usize,
}

impl Settings {
    /// Parse the variables in `values` that [`Settings`] declares, skipping
    /// any whose value is blank, then the command line `flags` over them.
    fn parse(values: &BTreeMap<String, String>, flags: Vec<String>) -> ConfigResult<Self> {
        let args: Vec<String> = Self::command()
            .get_arguments()
            .filter_map(|arg| arg.get_long())
            .filter_map(|name| {
                let value = values.get(name)?.trim();
                (!value.is_empty()).then(|| format!("--{name}={value}"))
            })
            .collect();
        Ok(Self::try_parse_from(
            std::iter::once("turbolay-proxy".to_string()).chain(args).chain(flags),
        )?)
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bolt_addr: String,
    pub http_addr: String,
    pub admin_addr: String,
    pub advertised_bolt_addr: String,
    pub advertise_service: Option<String>,
    pub nodes: Vec<NodeInfo>,
    pub proxy_set: ProxySet,
    pub read_policy: ReadPolicy,
    pub base_database: DatabaseName,
    pub root_scope: GraphScope,
    pub cell_id: String,
    pub token: String,
    pub tls: Option<(PathBuf, PathBuf)>,
    pub node_budget: usize,
    pub admit_wait: Duration,
    pub route_ttl_secs: i64,
    pub max_attempts: u32,
    pub threads: usize,
    pub http_enabled: bool,
    pub upstream_h2: bool,
    pub max_h2_streams: usize,
}

impl Config {
    pub fn from_env() -> ConfigResult<Self> {
        Self::parse(std::env::vars().collect(), std::env::args().skip(1).collect())
    }

    fn parse(values: BTreeMap<String, String>, flags: Vec<String>) -> ConfigResult<Self> {
        let settings = Settings::parse(&values, flags)?;
        let bolt = node_addresses("GRAPH_BOLT_NODE_ADDRESSES", &settings.bolt_node_addresses)?;
        let http = settings
            .http_node_addresses
            .as_deref()
            .map(|raw| node_addresses("TURBOLAY_PROXY_HTTP_NODE_ADDRESSES", raw))
            .transpose()?;
        let http_enabled = http.is_some();
        let nodes = static_nodes(&bolt, http.as_ref())?;
        let advertised_bolt_addr = settings.advertised_bolt_addr.unwrap_or_else(|| settings.bolt_addr.clone());
        let proxy_set = match settings.proxy_set {
            ProxySetKind::Static => ProxySet::Static(
                settings
                    .peers
                    .map(|peers| {
                        peers
                            .split(',')
                            .map(|peer| peer.trim().to_string())
                            .filter(|peer| !peer.is_empty())
                            .collect()
                    })
                    .unwrap_or_else(|| vec![advertised_bolt_addr.clone()]),
            ),
            ProxySetKind::File => ProxySet::File {
                path: settings
                    .peers_file
                    .ok_or("TURBOLAY_PROXY_SET=file needs TURBOLAY_PROXY_PEERS_FILE")?,
                interval: Duration::from_millis(settings.peers_poll_ms),
            },
            ProxySetKind::K8s => ProxySet::K8s {
                namespace: settings
                    .k8s_namespace
                    .ok_or("TURBOLAY_PROXY_SET=k8s needs TURBOLAY_PROXY_K8S_NAMESPACE")?,
                service: settings
                    .k8s_service
                    .ok_or("TURBOLAY_PROXY_SET=k8s needs TURBOLAY_PROXY_K8S_SERVICE")?,
                port: settings.k8s_port,
            },
        };
        let advertise_service = match settings.advertise.as_str() {
            "replicas" => None,
            other => Some(
                other
                    .strip_prefix("service:")
                    .ok_or_else(|| format!("invalid TURBOLAY_PROXY_ADVERTISE={other}; expected replicas or service:<host:port>"))?
                    .to_string(),
            ),
        };
        let root_scope = GraphScope::new(NamespacePath::parse(&settings.namespace)?, GraphId::new(settings.graph_id)?);
        let token_file = settings.auth_token_file;
        let token = std::fs::read_to_string(&token_file)
            .map_err(|error| format!("reading {}: {error}", token_file.display()))?
            .trim()
            .to_string();
        if token.len() < 32 || token.eq_ignore_ascii_case("change-me") {
            return Err("graph auth token must contain at least 32 non-placeholder characters".into());
        }
        let tls = if settings.allow_plaintext {
            None
        } else {
            Some((
                settings
                    .tls_certificate
                    .ok_or("GRAPH_TLS_CERTIFICATE is required unless GRAPH_ALLOW_PLAINTEXT=true")?,
                settings
                    .tls_private_key
                    .ok_or("GRAPH_TLS_PRIVATE_KEY is required unless GRAPH_ALLOW_PLAINTEXT=true")?,
            ))
        };
        Ok(Self {
            http_addr: settings.http_addr,
            admin_addr: settings.admin_addr,
            bolt_addr: settings.bolt_addr,
            advertised_bolt_addr,
            advertise_service,
            nodes,
            proxy_set,
            read_policy: ReadPolicy::parse(settings.read_routing.as_deref())?,
            base_database: DatabaseName::parse(&settings.database)?,
            root_scope,
            cell_id: settings.cell_id,
            token,
            tls,
            node_budget: settings.node_budget,
            admit_wait: Duration::from_millis(settings.admit_wait_ms),
            route_ttl_secs: settings.route_ttl_secs,
            max_attempts: settings.max_attempts,
            threads: settings
                .threads
                .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, usize::from)),
            http_enabled,
            upstream_h2: settings.upstream_h2,
            max_h2_streams: settings.max_h2_streams,
        })
    }
}
