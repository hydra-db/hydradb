use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use hydradb::{
    BoltReadRouting, CypherEngineMode, GraphBackpressurePolicy, GraphCacheConfig, GraphCachePolicy,
    GraphDurabilityConfig, GraphId, GraphIndexPolicy, GraphLimits, GraphMemoryConfig,
    GraphOpenOptions, GraphScope, GraphStorageMemoryConfig, NamespaceId, NamespacePath,
    PlannerStatisticsConfig, SparseKernelBackend,
};

type ConfigResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const MAX_WAL_FLUSH_INTERVAL_MS: u64 = 1_000;

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
    pub data_cache_part_bytes: usize,
    pub data_cache_max_open_files: usize,
    pub slatedb_cache_bytes: usize,
    pub reader_wal_replay_concurrency: usize,
    pub wal_flush_interval_ms: u64,
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
    pub max_native_path_result_bytes: usize,
    pub max_concurrent_hydrations: usize,
    pub max_concurrent_matrix_compilations: usize,
    pub max_open_scopes: usize,
    pub index_discovery_interval: Duration,
    pub indexer_notify_url: Option<String>,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub writer_lease_duration: Duration,
    pub bolt_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub bolt_node_addresses: BTreeMap<String, String>,
    pub read_routing: BoltReadRouting,
    pub cypher_engine: CypherEngineMode,
    pub planner_statistics: bool,
    pub planner_statistics_ttl: Duration,
    pub planner_statistics_hint_threshold: u32,
    pub auth_token_file: PathBuf,
    pub tls_certificate: Option<PathBuf>,
    pub tls_private_key: Option<PathBuf>,
    pub allow_plaintext: bool,
    pub max_concurrent_queries: usize,
    pub max_query_scan_edges: u64,
    pub max_query_runtime_ms: u64,
    pub max_bookmark_wait_ms: u64,
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
                .collect::<hydradb::Result<Vec<_>>>()?,
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
            return invalid("graph runtime requires GRAPH_TLS_CERTIFICATE and GRAPH_TLS_PRIVATE_KEY unless GRAPH_ALLOW_PLAINTEXT=true");
        }
        let advertised_bolt_addr = value(&values, "GRAPH_ADVERTISED_BOLT_ADDR", "localhost:7687");
        if advertised_bolt_addr.trim().is_empty()
            || advertised_bolt_addr.chars().any(char::is_whitespace)
        {
            return invalid("GRAPH_ADVERTISED_BOLT_ADDR must be a non-empty host:port");
        }
        // Decision 5 of the rendezvous placement design. A node is
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
        let wal_flush_interval_ms = parse_u64(
            &values,
            "GRAPH_WAL_FLUSH_INTERVAL_MS",
            GraphDurabilityConfig::DEFAULT_WAL_FLUSH_INTERVAL_MS,
        )?;
        if wal_flush_interval_ms > MAX_WAL_FLUSH_INTERVAL_MS {
            return invalid(format!(
                "GRAPH_WAL_FLUSH_INTERVAL_MS must be at most {MAX_WAL_FLUSH_INTERVAL_MS}"
            ));
        }
        // Change 3 of the cell-affine read-routing design. The
        // causal-consistency wait is a *part* of a query, so a budget at or
        // past the whole query's is no budget at all: it is what the wait
        // borrowed before, and it meant a read that could not catch up burned
        // 30s and died to the client watchdog with a generic timeout instead
        // of the `SnapshotAhead` that names the cell and both epochs. Rejecting
        // the inversion here keeps that failure a config error at startup
        // rather than a 30s kill in staging with nothing in the logs.
        let max_query_runtime_ms = parse_u64(&values, "GRAPH_MAX_QUERY_RUNTIME_MS", 30_000)?;
        let max_bookmark_wait_ms = parse_u64(&values, "GRAPH_MAX_BOOKMARK_WAIT_MS", 2_000)?;
        if max_bookmark_wait_ms >= max_query_runtime_ms {
            return invalid(format!(
                "GRAPH_MAX_BOOKMARK_WAIT_MS ({max_bookmark_wait_ms}) must be less than GRAPH_MAX_QUERY_RUNTIME_MS ({max_query_runtime_ms})"
            ));
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
            data_cache_part_bytes: parse_usize(
                &values,
                "GRAPH_DATA_CACHE_PART_BYTES",
                GraphCacheConfig::default().object_store_cache_part_bytes,
            )?,
            data_cache_max_open_files: parse_usize(
                &values,
                "GRAPH_DATA_CACHE_MAX_OPEN_FILES",
                GraphCacheConfig::default().object_store_cache_max_open_file_handles,
            )?,
            slatedb_cache_bytes: parse_usize_allow_zero(
                &values,
                "GRAPH_SLATE_DB_CACHE_BYTES",
                640 * 1024 * 1024,
            )?,
            reader_wal_replay_concurrency: parse_usize(
                &values,
                "GRAPH_READER_WAL_REPLAY_CONCURRENCY",
                16,
            )?,
            wal_flush_interval_ms,
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
            max_native_path_result_bytes: parse_usize_allow_zero(
                &values,
                "GRAPH_MAX_NATIVE_PATH_RESULT_BYTES",
                8 * 1024 * 1024,
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
            indexer_notify_url: optional_value(&values, "GRAPH_INDEXER_NOTIFY_URL"),
            heartbeat_interval,
            heartbeat_timeout,
            writer_lease_duration,
            bolt_addr: parse_socket(&values, "GRAPH_BOLT_ADDR", "0.0.0.0:7687")?,
            http_addr: parse_socket(&values, "GRAPH_HTTP_ADDR", "0.0.0.0:8443")?,
            admin_addr: parse_socket(&values, "GRAPH_ADMIN_ADDR", "0.0.0.0:9090")?,
            bolt_node_addresses,
            read_routing: parse_read_routing(&values, "GRAPH_READ_ROUTING")?,
            cypher_engine: parse_cypher_engine(&values, "GRAPH_CYPHER_ENGINE")?,
            planner_statistics: parse_planner_statistics(&values, "GRAPH_PLANNER_STATISTICS")?,
            planner_statistics_ttl: parse_duration(
                &values,
                "GRAPH_PLANNER_STATISTICS_TTL_MS",
                PlannerStatisticsConfig::DEFAULT_TTL.as_millis() as u64,
            )?,
            planner_statistics_hint_threshold: parse_u32_allow_zero(
                &values,
                "GRAPH_PLANNER_STATISTICS_HINT_THRESHOLD",
                PlannerStatisticsConfig::DEFAULT_HINT_THRESHOLD,
            )?,
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
            max_query_runtime_ms,
            max_bookmark_wait_ms,
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
        if !config.data_cache_part_bytes.is_multiple_of(1024) {
            return invalid("GRAPH_DATA_CACHE_PART_BYTES must be a multiple of 1024".to_string());
        }
        let minimum_cache_open_files = config
            .max_open_scopes
            .checked_mul(config.cells.len())
            .and_then(|handles| handles.checked_mul(2))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "GRAPH_MAX_OPEN_SCOPES and GRAPH_CELLS overflow the cache handle calculation",
                )
            })?;
        if config.data_cache_max_open_files < minimum_cache_open_files {
            return invalid(format!(
                "GRAPH_DATA_CACHE_MAX_OPEN_FILES ({}) must be at least GRAPH_MAX_OPEN_SCOPES × GRAPH_CELLS count × 2 ({minimum_cache_open_files})",
                config.data_cache_max_open_files,
            ));
        }
        Ok(config)
    }

    pub fn graph_open_options(&self) -> GraphOpenOptions {
        let mut options = GraphOpenOptions::default();
        options.limits = GraphLimits {
            max_query_scan_edges: self.max_query_scan_edges,
            max_query_runtime_ms: Some(self.max_query_runtime_ms),
            max_bookmark_wait_ms: self.max_bookmark_wait_ms,
            ..GraphLimits::default()
        };
        options.cache = GraphCacheConfig::disk_cache_without_preload(
            &self.data_cache_dir,
            self.data_cache_bytes,
        );
        options.cache.reader_wal_replay_concurrency = self.reader_wal_replay_concurrency;
        options.cache.object_store_cache_part_bytes = self.data_cache_part_bytes;
        options.cache.object_store_cache_max_open_file_handles = self.data_cache_max_open_files;
        options.cache.slatedb_cache_bytes = self.slatedb_cache_bytes;
        options.durability = GraphDurabilityConfig::low_latency_durable(self.wal_flush_interval_ms);
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
        options.planner_statistics = PlannerStatisticsConfig {
            enabled: self.planner_statistics,
            ttl: self.planner_statistics_ttl,
            hint_threshold: self.planner_statistics_hint_threshold,
        };
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
            max_native_path_result_bytes: self.max_native_path_result_bytes,
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

fn optional_value(values: &BTreeMap<String, String>, name: &str) -> Option<String> {
    values
        .get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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

fn parse_u32_allow_zero(
    values: &BTreeMap<String, String>,
    name: &str,
    default: u32,
) -> ConfigResult<u32> {
    let raw = value(values, name, &default.to_string());
    raw.parse::<u32>()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, format!("invalid {name}: {err}")).into())
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

/// `GRAPH_READ_ROUTING = owner | fleet`, default `fleet`.
///
/// Unlike `GRAPH_SPARSE_KERNEL`, absent and `fleet` mean the same thing: there
/// is no lower-precedence override for this to defer to, so the default is a
/// literal rather than "whatever the library picks". Defaulting to `fleet`
/// keeps today's fleet-wide `READ` list until an operator asks for cell-affine
/// reads, which is the point of change 2 of
/// the cell-affine read-routing design: flipping read routing
/// changes the load shape of the whole fleet, so reverting it must cost an env
/// change rather than an image build.
fn parse_read_routing(
    values: &BTreeMap<String, String>,
    name: &str,
) -> ConfigResult<BoltReadRouting> {
    match value(values, name, "fleet").to_ascii_lowercase().as_str() {
        "fleet" => Ok(BoltReadRouting::Fleet),
        "owner" => Ok(BoltReadRouting::Owner),
        other => invalid(format!("invalid {name}={other}; expected owner or fleet")),
    }
}

/// `GRAPH_CYPHER_ENGINE = legacy | experimental`, default `legacy`.
///
/// The runtime switch is intentionally independent from the Cargo feature: an
/// operator cannot select code that was not compiled into the node, and merely
/// compiling the experimental adapter never changes production behavior.
fn parse_cypher_engine(
    values: &BTreeMap<String, String>,
    name: &str,
) -> ConfigResult<CypherEngineMode> {
    match value(values, name, "legacy").to_ascii_lowercase().as_str() {
        "legacy" => Ok(CypherEngineMode::Legacy),
        "experimental" => {
            #[cfg(feature = "experimental-cypher-engine")]
            {
                Ok(CypherEngineMode::Experimental)
            }
            #[cfg(not(feature = "experimental-cypher-engine"))]
            {
                invalid(format!(
                    "{name}=experimental requires the experimental-cypher-engine Cargo feature"
                ))
            }
        }
        other => invalid(format!(
            "invalid {name}={other}; expected legacy or experimental"
        )),
    }
}

/// `GRAPH_PLANNER_STATISTICS`, default off: whether the experimental planner
/// loads the statistics the indexer publishes. Like `GRAPH_CYPHER_ENGINE`, it
/// cannot select code the node was built without.
fn parse_planner_statistics(values: &BTreeMap<String, String>, name: &str) -> ConfigResult<bool> {
    let enabled = parse_bool(values, name, false)?;
    if enabled && !cfg!(feature = "experimental-cypher-engine") {
        return invalid(format!(
            "{name}=true requires the experimental-cypher-engine Cargo feature"
        ));
    }
    Ok(enabled)
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
    fn custom_native_path_result_bytes_is_parsed_and_passed_to_memory_config() {
        let values = BTreeMap::from([
            ("GRAPH_ALLOW_PLAINTEXT".to_string(), "true".to_string()),
            (
                "GRAPH_MAX_NATIVE_PATH_RESULT_BYTES".to_string(),
                "33554432".to_string(),
            ),
        ]);
        let config = RuntimeConfig::from_values(values).unwrap();
        assert_eq!(config.max_native_path_result_bytes, 32 * 1024 * 1024);
        assert_eq!(
            config.graph_memory_config().max_native_path_result_bytes,
            32 * 1024 * 1024
        );
    }
}
