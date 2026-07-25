use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use slatedb_graph_kernel::{
    GraphBackpressurePolicy, GraphCacheConfig, GraphCachePolicy, GraphDurabilityConfig, GraphId,
    GraphIndexPolicy, GraphLimits, GraphMemoryConfig, GraphOpenOptions, GraphScope,
    GraphStorageMemoryConfig, NamespaceId, NamespacePath, SparseKernelBackend,
};

type ConfigResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub node_id: String,
    pub scope: GraphScope,
    pub cell_id: String,
    pub cells: Vec<String>,
    pub database: String,
    pub data_path: String,
    pub data_cache_dir: PathBuf,
    pub data_cache_bytes: usize,
    pub l0_sst_size_bytes: usize,
    pub max_unflushed_bytes: usize,
    pub max_wal_flushes_before_l0_flush: u64,
    pub l0_flush_parallelism: usize,
    pub max_matrix_adjacencies: usize,
    pub max_matrix_adjacency_bytes: usize,
    pub max_graphblas_matrices: usize,
    pub max_graphblas_bytes: usize,
    pub sparse_kernel: SparseKernelBackend,
    pub max_relationship_rows_bytes: usize,
    pub max_source_relationship_rows_bytes: usize,
    pub max_relationship_property_rows_bytes: usize,
    pub max_concurrent_hydrations: usize,
    pub max_concurrent_matrix_compilations: usize,
    pub max_open_scopes: usize,
    pub index_discovery_interval: Duration,
    pub bolt_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub bolt_node_addresses: BTreeMap<String, String>,
    pub auth_token_file: PathBuf,
    pub tls_certificate: Option<PathBuf>,
    pub tls_private_key: Option<PathBuf>,
    pub allow_plaintext: bool,
    pub max_concurrent_queries: usize,
    pub max_query_scan_edges: u64,
    pub max_query_runtime_ms: u64,
    pub max_server_cursors: usize,
    pub max_cursor_buffer_bytes: u64,
    pub cursor_ttl: Duration,
    pub max_bolt_connections: usize,
    pub default_page_size: usize,
    pub graceful_shutdown_timeout: Duration,
}

impl RuntimeConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Self::from_values(std::env::vars().collect())
    }

    fn from_values(values: BTreeMap<String, String>) -> ConfigResult<Self> {
        let namespace = value(&values, "GRAPH_NAMESPACE", "default");
        let namespace = NamespacePath::new(
            namespace
                .split('/')
                .map(|segment| NamespaceId::new(segment.to_string()))
                .collect::<slatedb_graph_kernel::Result<Vec<_>>>()?,
        )?;
        let scope = GraphScope::new(
            namespace,
            GraphId::new(value(&values, "GRAPH_ID", "default"))?,
        );
        let cell_id = value(&values, "GRAPH_CELL_ID", "cell-0");
        let cells = value(&values, "GRAPH_CELLS", &cell_id)
            .split(',')
            .map(str::trim)
            .filter(|cell| !cell.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        if cells.is_empty() || !cells.iter().any(|cell| cell == &cell_id) {
            return invalid("GRAPH_CELLS must contain GRAPH_CELL_ID");
        }
        let allow_plaintext = parse_bool(&values, "GRAPH_ALLOW_PLAINTEXT", false)?;
        let tls_certificate = optional_path(&values, "GRAPH_TLS_CERTIFICATE");
        let tls_private_key = optional_path(&values, "GRAPH_TLS_PRIVATE_KEY");
        if !allow_plaintext && (tls_certificate.is_none() || tls_private_key.is_none()) {
            return invalid(
                "graph runtime requires GRAPH_TLS_CERTIFICATE and GRAPH_TLS_PRIVATE_KEY unless GRAPH_ALLOW_PLAINTEXT=true",
            );
        }
        let advertised_bolt_addr = value(&values, "GRAPH_ADVERTISED_BOLT_ADDR", "localhost:7687");
        if advertised_bolt_addr.trim().is_empty()
            || advertised_bolt_addr.chars().any(char::is_whitespace)
        {
            return invalid("GRAPH_ADVERTISED_BOLT_ADDR must be a non-empty host:port");
        }
        let bolt_node_addresses = parse_node_addresses(
            &values,
            &value(
                &values,
                "GRAPH_BOLT_NODE_ADDRESSES",
                &format!(
                    "{}={advertised_bolt_addr}",
                    value(&values, "GRAPH_NODE_ID", "graph-node-0")
                ),
            ),
        )?;
        Ok(Self {
            node_id: value(&values, "GRAPH_NODE_ID", "graph-node-0"),
            scope,
            cell_id,
            cells,
            database: value(&values, "GRAPH_DATABASE", "default"),
            data_path: value(&values, "GRAPH_DATA_PATH", "graph/data"),
            data_cache_dir: PathBuf::from(value(
                &values,
                "GRAPH_DATA_CACHE_DIR",
                "/var/cache/slatedb/data",
            )),
            data_cache_bytes: parse_usize(
                &values,
                "GRAPH_DATA_CACHE_BYTES",
                8 * 1024 * 1024 * 1024,
            )?,
            l0_sst_size_bytes: parse_usize(&values, "GRAPH_L0_SST_SIZE_BYTES", 16 * 1024 * 1024)?,
            max_unflushed_bytes: parse_usize(
                &values,
                "GRAPH_MAX_UNFLUSHED_BYTES",
                64 * 1024 * 1024,
            )?,
            max_wal_flushes_before_l0_flush: parse_u64(
                &values,
                "GRAPH_MAX_WAL_FLUSHES_BEFORE_L0_FLUSH",
                4_096,
            )?,
            l0_flush_parallelism: parse_usize(&values, "GRAPH_L0_FLUSH_PARALLELISM", 1)?,
            max_matrix_adjacencies: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_MATRIX_ADJACENCIES",
                0,
            )?,
            max_matrix_adjacency_bytes: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_MATRIX_ADJACENCY_BYTES",
                0,
            )?,
            max_graphblas_matrices: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_GRAPHBLAS_MATRICES",
                16,
            )?,
            max_graphblas_bytes: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_GRAPHBLAS_BYTES",
                128 * 1024 * 1024,
            )?,
            sparse_kernel: parse_sparse_kernel(&values, "GRAPH_SPARSE_KERNEL")?,
            max_relationship_rows_bytes: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_RELATIONSHIP_ROWS_BYTES",
                8 * 1024 * 1024,
            )?,
            max_source_relationship_rows_bytes: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_SOURCE_RELATIONSHIP_ROWS_BYTES",
                8 * 1024 * 1024,
            )?,
            max_relationship_property_rows_bytes: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_RELATIONSHIP_PROPERTY_ROWS_BYTES",
                16 * 1024 * 1024,
            )?,
            max_concurrent_hydrations: parse_usize(&values, "GRAPH_MAX_CONCURRENT_HYDRATIONS", 2)?,
            max_concurrent_matrix_compilations: parse_usize(
                &values,
                "GRAPH_MAX_CONCURRENT_MATRIX_COMPILATIONS",
                1,
            )?,
            max_open_scopes: parse_usize(&values, "GRAPH_MAX_OPEN_SCOPES", 8)?,
            index_discovery_interval: parse_duration(
                &values,
                "GRAPH_INDEX_DISCOVERY_INTERVAL_MS",
                5_000,
            )?,
            bolt_addr: parse_socket(&values, "GRAPH_BOLT_ADDR", "0.0.0.0:7687")?,
            http_addr: parse_socket(&values, "GRAPH_HTTP_ADDR", "0.0.0.0:8443")?,
            admin_addr: parse_socket(&values, "GRAPH_ADMIN_ADDR", "0.0.0.0:9090")?,
            bolt_node_addresses,
            auth_token_file: PathBuf::from(value(
                &values,
                "GRAPH_AUTH_TOKEN_FILE",
                "/var/run/secrets/slatedb-graph/auth-token",
            )),
            tls_certificate,
            tls_private_key,
            allow_plaintext,
            max_concurrent_queries: parse_usize(&values, "GRAPH_MAX_CONCURRENT_QUERIES", 256)?,
            max_query_scan_edges: parse_u64(&values, "GRAPH_MAX_QUERY_SCAN_EDGES", 1_000_000)?,
            max_query_runtime_ms: parse_u64(&values, "GRAPH_MAX_QUERY_RUNTIME_MS", 30_000)?,
            max_server_cursors: parse_usize(&values, "GRAPH_MAX_SERVER_CURSORS", 1_024)?,
            max_cursor_buffer_bytes: parse_u64(
                &values,
                "GRAPH_MAX_CURSOR_BUFFER_BYTES",
                64 * 1024 * 1024,
            )?,
            cursor_ttl: parse_duration(&values, "GRAPH_CURSOR_TTL_MS", 60_000)?,
            max_bolt_connections: parse_usize(&values, "GRAPH_MAX_BOLT_CONNECTIONS", 4_096)?,
            default_page_size: parse_usize(&values, "GRAPH_DEFAULT_PAGE_SIZE", 1_024)?,
            graceful_shutdown_timeout: parse_duration(
                &values,
                "GRAPH_GRACEFUL_SHUTDOWN_MS",
                30_000,
            )?,
        })
    }

    pub fn graph_open_options(&self) -> GraphOpenOptions {
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: self.max_query_scan_edges,
                max_query_runtime_ms: Some(self.max_query_runtime_ms),
                ..GraphLimits::default()
            },
            cache: GraphCacheConfig::disk_cache_without_preload(
                &self.data_cache_dir,
                self.data_cache_bytes,
            ),
            durability: GraphDurabilityConfig::default(),
            cache_policy: GraphCachePolicy {
                max_matrix_adjacencies: self.max_matrix_adjacencies,
                max_graphblas_matrices: self.max_graphblas_matrices,
                max_concurrent_hydrations: self.max_concurrent_hydrations,
                sparse_kernel: self.sparse_kernel,
                ..GraphCachePolicy::default()
            },
            backpressure_policy: GraphBackpressurePolicy::default(),
            index_policy: GraphIndexPolicy::Full,
        }
    }

    pub fn graph_memory_config(&self) -> GraphMemoryConfig {
        GraphMemoryConfig {
            storage: GraphStorageMemoryConfig {
                l0_sst_size_bytes: self.l0_sst_size_bytes,
                max_unflushed_bytes: self.max_unflushed_bytes,
                max_wal_flushes_before_l0_flush: self.max_wal_flushes_before_l0_flush,
                l0_flush_parallelism: self.l0_flush_parallelism,
            },
            max_matrix_adjacency_bytes: self.max_matrix_adjacency_bytes,
            max_graphblas_bytes: self.max_graphblas_bytes,
            max_relationship_rows_bytes: self.max_relationship_rows_bytes,
            max_source_relationship_rows_bytes: self.max_source_relationship_rows_bytes,
            max_relationship_property_rows_bytes: self.max_relationship_property_rows_bytes,
            max_concurrent_matrix_compilations: self.max_concurrent_matrix_compilations,
        }
    }

    pub fn read_auth_token(&self) -> ConfigResult<String> {
        let token = std::fs::read_to_string(&self.auth_token_file)?
            .trim()
            .to_string();
        if token.len() < 32 || token.eq_ignore_ascii_case("change-me") {
            return invalid("graph auth token must contain at least 32 non-placeholder characters");
        }
        Ok(token)
    }
}

fn value(values: &BTreeMap<String, String>, name: &str, default: &str) -> String {
    values
        .get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn optional_path(values: &BTreeMap<String, String>, name: &str) -> Option<PathBuf> {
    values
        .get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn parse_socket(
    values: &BTreeMap<String, String>,
    name: &str,
    default: &str,
) -> ConfigResult<SocketAddr> {
    Ok(value(values, name, default)
        .parse()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, format!("invalid {name}: {err}")))?)
}

fn parse_duration(
    values: &BTreeMap<String, String>,
    name: &str,
    default_ms: u64,
) -> ConfigResult<Duration> {
    let millis = parse_u64(values, name, default_ms)?;
    if millis == 0 {
        return invalid(format!("{name} must be greater than zero"));
    }
    Ok(Duration::from_millis(millis))
}

fn parse_u64(values: &BTreeMap<String, String>, name: &str, default: u64) -> ConfigResult<u64> {
    let raw = value(values, name, &default.to_string());
    let parsed = raw
        .parse::<u64>()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, format!("invalid {name}: {err}")))?;
    if parsed == 0 {
        return invalid(format!("{name} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_usize(
    values: &BTreeMap<String, String>,
    name: &str,
    default: usize,
) -> ConfigResult<usize> {
    usize::try_from(parse_u64(values, name, default as u64)?).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("{name} does not fit usize"),
        )
        .into()
    })
}

fn parse_usize_allow_zero(
    values: &BTreeMap<String, String>,
    name: &str,
    default: usize,
) -> ConfigResult<usize> {
    let raw = value(values, name, &default.to_string());
    let parsed = raw
        .parse::<u64>()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, format!("invalid {name}: {err}")))?;
    usize::try_from(parsed).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("{name} does not fit usize"),
        )
        .into()
    })
}

fn parse_sparse_kernel(
    values: &BTreeMap<String, String>,
    name: &str,
) -> ConfigResult<SparseKernelBackend> {
    match value(values, name, "suitesparse")
        .to_ascii_lowercase()
        .as_str()
    {
        "adjacency" => Ok(SparseKernelBackend::Adjacency),
        "compact" => Ok(SparseKernelBackend::CompactCsc),
        "suitesparse" => Ok(SparseKernelBackend::SuiteSparse),
        other => invalid(format!(
            "invalid {name}={other}; expected adjacency, compact or suitesparse"
        )),
    }
}

fn parse_bool(values: &BTreeMap<String, String>, name: &str, default: bool) -> ConfigResult<bool> {
    match value(values, name, if default { "true" } else { "false" })
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => invalid(format!("invalid boolean {name}={other}")),
    }
}

fn parse_node_addresses(
    _values: &BTreeMap<String, String>,
    raw: &str,
) -> ConfigResult<BTreeMap<String, String>> {
    let mut addresses = BTreeMap::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (node_id, address) = entry.split_once('=').ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "GRAPH_BOLT_NODE_ADDRESSES entries must use node-id=host:port",
            )
        })?;
        let node_id = node_id.trim();
        let address = address.trim();
        if node_id.is_empty()
            || address.is_empty()
            || addresses
                .insert(node_id.to_string(), address.to_string())
                .is_some()
        {
            return invalid(
                "GRAPH_BOLT_NODE_ADDRESSES contains an empty or duplicate node id/address",
            );
        }
    }
    if addresses.is_empty() {
        return invalid("GRAPH_BOLT_NODE_ADDRESSES must contain at least one endpoint");
    }
    Ok(addresses)
}

fn invalid<T>(message: impl Into<String>) -> ConfigResult<T> {
    Err(Error::new(ErrorKind::InvalidInput, message.into()).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_runtime_requires_tls() {
        let values = BTreeMap::from([("GRAPH_AUTH_TOKEN_FILE".to_string(), "/token".to_string())]);
        let error = RuntimeConfig::from_values(values).unwrap_err();
        assert!(error.to_string().contains("requires GRAPH_TLS_CERTIFICATE"));
    }

    #[test]
    fn plaintext_runtime_config_is_explicit_and_bounded() {
        let values = BTreeMap::from([("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string())]);
        let config = RuntimeConfig::from_values(values).unwrap();
        assert_eq!(config.max_query_scan_edges, 1_000_000);
        assert_eq!(config.max_query_runtime_ms, 30_000);
        assert_eq!(config.max_server_cursors, 1_024);
        assert_eq!(config.max_cursor_buffer_bytes, 64 * 1024 * 1024);
        assert_eq!(config.cursor_ttl, Duration::from_secs(60));
        assert_eq!(config.l0_sst_size_bytes, 16 * 1024 * 1024);
        assert_eq!(config.max_unflushed_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_concurrent_hydrations, 2);
        assert_eq!(config.max_open_scopes, 8);
        assert_eq!(config.index_discovery_interval, Duration::from_secs(5));
        let memory = config.graph_memory_config();
        assert_eq!(memory.max_graphblas_bytes, 128 * 1024 * 1024);
        assert_eq!(memory.max_matrix_adjacency_bytes, 0);
        assert_eq!(memory.max_relationship_rows_bytes, 8 * 1024 * 1024);
        assert_eq!(memory.max_source_relationship_rows_bytes, 8 * 1024 * 1024);
        assert_eq!(
            memory.max_relationship_property_rows_bytes,
            16 * 1024 * 1024
        );
    }

    #[test]
    fn graph_node_config_applies_query_scan_edge_limit() {
        let values = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            ("GRAPH_MAX_QUERY_SCAN_EDGES".to_string(), "64".to_string()),
        ]);
        let config = RuntimeConfig::from_values(values).unwrap();
        assert_eq!(config.max_query_scan_edges, 64);
        assert_eq!(config.graph_open_options().limits.max_query_scan_edges, 64);
    }

    #[test]
    fn graph_node_config_selects_the_sparse_kernel() {
        let base = || BTreeMap::from([("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string())]);
        let config = RuntimeConfig::from_values(base()).unwrap();
        assert_eq!(config.sparse_kernel, SparseKernelBackend::SuiteSparse);
        assert_eq!(
            config.graph_open_options().cache_policy.sparse_kernel,
            SparseKernelBackend::SuiteSparse
        );

        let mut values = base();
        values.insert("GRAPH_SPARSE_KERNEL".to_string(), "Compact".to_string());
        let config = RuntimeConfig::from_values(values).unwrap();
        assert_eq!(config.sparse_kernel, SparseKernelBackend::CompactCsc);

        let mut values = base();
        values.insert("GRAPH_SPARSE_KERNEL".to_string(), "ADJACENCY".to_string());
        let config = RuntimeConfig::from_values(values).unwrap();
        assert_eq!(config.sparse_kernel, SparseKernelBackend::Adjacency);

        let mut values = base();
        values.insert("GRAPH_SPARSE_KERNEL".to_string(), "cuda".to_string());
        let error = RuntimeConfig::from_values(values).unwrap_err();
        assert!(error.to_string().contains("GRAPH_SPARSE_KERNEL"));
    }

    #[test]
    fn graph_node_config_can_disable_heavy_memory_caches() {
        let values = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            ("GRAPH_MAX_MATRIX_ADJACENCIES".to_string(), "0".to_string()),
            (
                "GRAPH_MAX_MATRIX_ADJACENCY_BYTES".to_string(),
                "0".to_string(),
            ),
            ("GRAPH_MAX_GRAPHBLAS_MATRICES".to_string(), "0".to_string()),
            ("GRAPH_MAX_GRAPHBLAS_BYTES".to_string(), "0".to_string()),
            (
                "GRAPH_MAX_RELATIONSHIP_ROWS_BYTES".to_string(),
                "0".to_string(),
            ),
            (
                "GRAPH_MAX_SOURCE_RELATIONSHIP_ROWS_BYTES".to_string(),
                "0".to_string(),
            ),
            (
                "GRAPH_MAX_RELATIONSHIP_PROPERTY_ROWS_BYTES".to_string(),
                "0".to_string(),
            ),
        ]);
        let config = RuntimeConfig::from_values(values).unwrap();
        let options = config.graph_open_options();
        let memory = config.graph_memory_config();
        assert_eq!(options.cache_policy.max_matrix_adjacencies, 0);
        assert_eq!(memory.max_matrix_adjacency_bytes, 0);
        assert_eq!(options.cache_policy.max_graphblas_matrices, 0);
        assert_eq!(memory.max_graphblas_bytes, 0);
        assert_eq!(memory.max_relationship_rows_bytes, 0);
        assert_eq!(memory.max_source_relationship_rows_bytes, 0);
        assert_eq!(memory.max_relationship_property_rows_bytes, 0);
    }
}
