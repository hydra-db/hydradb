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
    pub reader_wal_replay_concurrency: usize,
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
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub writer_lease_duration: Duration,
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
    pub bolt_authentication_timeout: Duration,
    pub bolt_idle_timeout: Duration,
    pub bolt_max_connection_age: Duration,
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
        // Decision 5 of docs/plans/2026-07-25-rendezvous-placement.md. A node is
        // live while its heartbeat object is younger than the timeout, so an
        // interval at or past the timeout means every heartbeat expires before
        // its own writer refreshes it and the entire fleet reads as dead — no
        // node owns any cell, and nothing about the symptom points at the
        // config. `parse_duration` already rejects zero, which is the same
        // failure by a different route: a publisher that never ticks.
        let heartbeat_interval = parse_duration(&values, "GRAPH_HEARTBEAT_INTERVAL_MS", 5_000)?;
        let heartbeat_timeout = parse_duration(&values, "GRAPH_HEARTBEAT_TIMEOUT_MS", 15_000)?;
        if heartbeat_interval >= heartbeat_timeout {
            return invalid(format!(
                "GRAPH_HEARTBEAT_INTERVAL_MS ({}) must be less than GRAPH_HEARTBEAT_TIMEOUT_MS ({})",
                heartbeat_interval.as_millis(),
                heartbeat_timeout.as_millis(),
            ));
        }
        let writer_lease_duration = parse_duration(&values, "GRAPH_WRITER_LEASE_MS", 30_000)?;
        if writer_lease_duration < Duration::from_secs(3) {
            return invalid("GRAPH_WRITER_LEASE_MS must be at least 3000");
        }
        if writer_lease_duration > Duration::from_secs(300) {
            return invalid("GRAPH_WRITER_LEASE_MS must be at most 300000");
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
        let config = Self {
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
            reader_wal_replay_concurrency: parse_usize(
                &values,
                "GRAPH_READER_WAL_REPLAY_CONCURRENCY",
                16,
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
                GraphStorageMemoryConfig::DEFAULT_MAX_WAL_FLUSHES_BEFORE_L0_FLUSH,
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
            heartbeat_interval,
            heartbeat_timeout,
            writer_lease_duration,
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
            bolt_authentication_timeout: parse_duration(
                &values,
                "GRAPH_BOLT_AUTHENTICATION_TIMEOUT_MS",
                30_000,
            )?,
            bolt_idle_timeout: parse_duration(
                &values,
                "GRAPH_BOLT_IDLE_TIMEOUT_MS",
                15 * 60 * 1_000,
            )?,
            bolt_max_connection_age: parse_duration(
                &values,
                "GRAPH_BOLT_MAX_CONNECTION_AGE_MS",
                60 * 60 * 1_000,
            )?,
            default_page_size: parse_usize(&values, "GRAPH_DEFAULT_PAGE_SIZE", 1_024)?,
            graceful_shutdown_timeout: parse_duration(
                &values,
                "GRAPH_GRACEFUL_SHUTDOWN_MS",
                30_000,
            )?,
        };
        config.graph_memory_config().storage.validate()?;
        Ok(config)
    }

    pub fn graph_open_options(&self) -> GraphOpenOptions {
        let mut options = GraphOpenOptions::default();
        options.limits = GraphLimits {
            max_query_scan_edges: self.max_query_scan_edges,
            max_query_runtime_ms: Some(self.max_query_runtime_ms),
            ..GraphLimits::default()
        };
        options.cache = GraphCacheConfig::disk_cache_without_preload(
            &self.data_cache_dir,
            self.data_cache_bytes,
        );
        options.cache.reader_wal_replay_concurrency = self.reader_wal_replay_concurrency;
        options.durability = GraphDurabilityConfig::default();
        options.cache_policy = {
            let mut cache_policy = GraphCachePolicy::default();
            cache_policy.max_matrix_adjacencies = self.max_matrix_adjacencies;
            cache_policy.max_graphblas_matrices = self.max_graphblas_matrices;
            cache_policy.max_concurrent_hydrations = self.max_concurrent_hydrations;
            cache_policy.sparse_kernel = self.sparse_kernel;
            cache_policy
        };
        options.backpressure_policy = GraphBackpressurePolicy::default();
        options.index_policy = GraphIndexPolicy::Full;
        options
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
        // Every other failure in this module names the variable it came from
        // (`invalid {name}: {err}`); a bare `?` here was the one exception, and
        // it surfaced as `Error: Os { code: 21, kind: IsADirectory }` with
        // nothing tying it to a path or a setting. The node has already reached
        // placement state `fresh` by this point, so the operator sees a healthy
        // startup followed by a raw errno.
        //
        // `err.kind()` is carried through rather than flattened to
        // `InvalidInput`: NotFound and IsADirectory are different operator
        // mistakes -- an unmounted secret versus a directory where a file was
        // expected -- and the kind is what a caller would match on.
        let path = self.auth_token_file.display();
        let token = std::fs::read_to_string(&self.auth_token_file)
            .map_err(|err| {
                Error::new(
                    err.kind(),
                    format!("cannot read GRAPH_AUTH_TOKEN_FILE at {path}: {err}"),
                )
            })?
            .trim()
            .to_string();
        if token.len() < 32 || token.eq_ignore_ascii_case("change-me") {
            // Named too, so that a deployment mounting more than one secret
            // knows which file was rejected.
            return invalid(format!(
                "graph auth token at {path} (GRAPH_AUTH_TOKEN_FILE) must contain at least 32 non-placeholder characters"
            ));
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
    // Absent means "whatever GraphCachePolicy defaults to", which is where the
    // legacy GRAPH_COMPILED_KERNEL override lands. Defaulting to the literal
    // string "suitesparse" here would make an unset variable indistinguishable
    // from an explicit one and silently outrank that override.
    let Some(raw) = values
        .get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(GraphCachePolicy::default().sparse_kernel);
    };
    match raw.to_ascii_lowercase().as_str() {
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

    fn plaintext_config_with_auth_token(path: &std::path::Path) -> RuntimeConfig {
        let values = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            (
                "GRAPH_AUTH_TOKEN_FILE".to_string(),
                path.to_str().unwrap().to_string(),
            ),
        ]);
        RuntimeConfig::from_values(values).unwrap()
    }

    #[test]
    fn missing_auth_token_file_names_the_path_and_keeps_its_kind() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("absent-token");
        let error = plaintext_config_with_auth_token(&path)
            .read_auth_token()
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("GRAPH_AUTH_TOKEN_FILE"), "{message}");
        assert!(message.contains(path.to_str().unwrap()), "{message}");
        assert_eq!(
            error.downcast_ref::<Error>().map(Error::kind),
            Some(ErrorKind::NotFound),
            "{message}"
        );
    }

    #[test]
    fn auth_token_directory_names_the_path_instead_of_a_raw_errno() {
        // The GitHub-runner half of issue #101: `GRAPH_AUTH_TOKEN_FILE` points
        // at a directory and startup exits with a bare `Os { code: 21 }`. The
        // kind is deliberately not asserted -- platforms disagree on what
        // reading a directory returns -- but the path must be in the message.
        let directory = tempfile::tempdir().unwrap();
        let error = plaintext_config_with_auth_token(directory.path())
            .read_auth_token()
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("GRAPH_AUTH_TOKEN_FILE"), "{message}");
        assert!(
            message.contains(directory.path().to_str().unwrap()),
            "{message}"
        );
    }

    #[test]
    fn short_auth_token_names_the_file_it_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth-token");
        std::fs::write(&path, "change-me").unwrap();
        let error = plaintext_config_with_auth_token(&path)
            .read_auth_token()
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("non-placeholder"), "{message}");
        assert!(message.contains(path.to_str().unwrap()), "{message}");
    }

    #[test]
    fn readable_auth_token_file_is_trimmed_and_accepted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth-token");
        std::fs::write(&path, "  local-development-token-32-bytes\n").unwrap();
        let token = plaintext_config_with_auth_token(&path)
            .read_auth_token()
            .unwrap();
        assert_eq!(token, "local-development-token-32-bytes");
    }

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
        assert_eq!(config.max_wal_flushes_before_l0_flush, 128);
        assert_eq!(config.max_concurrent_hydrations, 2);
        assert_eq!(config.max_open_scopes, 8);
        assert_eq!(config.reader_wal_replay_concurrency, 16);
        assert_eq!(config.index_discovery_interval, Duration::from_secs(5));
        assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(15));
        assert_eq!(config.writer_lease_duration, Duration::from_secs(30));
        assert_eq!(config.bolt_authentication_timeout, Duration::from_secs(30));
        assert_eq!(config.bolt_idle_timeout, Duration::from_secs(15 * 60));
        assert_eq!(config.bolt_max_connection_age, Duration::from_secs(60 * 60));
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
    fn graph_node_rejects_unsafe_wal_flush_bounds() {
        for value in ["0", "4097"] {
            let values = BTreeMap::from([
                ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
                (
                    "GRAPH_MAX_WAL_FLUSHES_BEFORE_L0_FLUSH".to_string(),
                    value.to_string(),
                ),
            ]);
            RuntimeConfig::from_values(values).expect_err("unsafe WAL flush bound must fail");
        }
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
    fn graph_node_rejects_an_unsafe_writer_lease_window() {
        let values = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            ("GRAPH_WRITER_LEASE_MS".to_string(), "2999".to_string()),
        ]);
        let error = RuntimeConfig::from_values(values).unwrap_err();
        assert!(error
            .to_string()
            .contains("GRAPH_WRITER_LEASE_MS must be at least 3000"));
    }

    #[test]
    fn graph_node_rejects_an_excessive_writer_lease_window() {
        let values = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            ("GRAPH_WRITER_LEASE_MS".to_string(), "300001".to_string()),
        ]);
        let error = RuntimeConfig::from_values(values).unwrap_err();
        assert!(error
            .to_string()
            .contains("GRAPH_WRITER_LEASE_MS must be at most 300000"));
    }

    #[test]
    fn graph_node_config_applies_reader_wal_replay_concurrency() {
        let values = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            (
                "GRAPH_READER_WAL_REPLAY_CONCURRENCY".to_string(),
                "32".to_string(),
            ),
        ]);
        let config = RuntimeConfig::from_values(values).unwrap();
        assert_eq!(config.reader_wal_replay_concurrency, 32);
        assert_eq!(
            config
                .graph_open_options()
                .cache
                .reader_wal_replay_concurrency,
            32
        );

        let invalid = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            (
                "GRAPH_READER_WAL_REPLAY_CONCURRENCY".to_string(),
                "0".to_string(),
            ),
        ]);
        let error = RuntimeConfig::from_values(invalid).unwrap_err();
        assert!(error
            .to_string()
            .contains("GRAPH_READER_WAL_REPLAY_CONCURRENCY must be greater than zero"));
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
    fn heartbeat_interval_must_be_shorter_than_the_timeout() {
        // Both rejections describe the same production failure — every node's
        // heartbeat expires before anything refreshes it, so the fleet computes
        // an empty live set and no cell has an owner. Decision 5.
        let base = || BTreeMap::from([("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string())]);

        let mut values = base();
        values.insert("GRAPH_HEARTBEAT_INTERVAL_MS".to_string(), "0".to_string());
        let error = RuntimeConfig::from_values(values).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("GRAPH_HEARTBEAT_INTERVAL_MS must be greater than zero"),
            "unexpected error: {error}"
        );

        let mut values = base();
        values.insert(
            "GRAPH_HEARTBEAT_INTERVAL_MS".to_string(),
            "15000".to_string(),
        );
        let error = RuntimeConfig::from_values(values).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must be less than GRAPH_HEARTBEAT_TIMEOUT_MS"),
            "unexpected error: {error}"
        );

        let mut values = base();
        values.insert(
            "GRAPH_HEARTBEAT_INTERVAL_MS".to_string(),
            "20000".to_string(),
        );
        let error = RuntimeConfig::from_values(values).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must be less than GRAPH_HEARTBEAT_TIMEOUT_MS"),
            "unexpected error: {error}"
        );

        let mut values = base();
        values.insert(
            "GRAPH_HEARTBEAT_INTERVAL_MS".to_string(),
            "2000".to_string(),
        );
        values.insert("GRAPH_HEARTBEAT_TIMEOUT_MS".to_string(), "6000".to_string());
        let config = RuntimeConfig::from_values(values).unwrap();
        assert_eq!(config.heartbeat_interval, Duration::from_secs(2));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(6));
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
