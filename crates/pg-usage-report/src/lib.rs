//! The anonymous usage report pg_hydradb sends once an hour, shared
//! by the extension that builds it and the receiver that checks it.
//!
//! A report holds counts, ranges, timestamps and codes from the fixed lists
//! below. It never holds graph, table, column or label names, keys, seeds,
//! query options or anything else a user typed: [`validate`] refuses a report
//! with a name it does not know, so free text cannot ride along in one.
//!
//! Activity counters are running totals since the server started, not
//! differences since the last report, so a lost report loses nothing. The
//! receiver keeps the latest report per `(install_id, server_started_at_unix_ms)`
//! and sums those.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const REPORT_VERSION: u32 = 1;

/// The SQL functions whose calls are counted, with latency and rows read.
pub const QUERY_FUNCTIONS: [&str; 4] = ["reachable_vertices", "reachable_edges", "paths_from", "paths_between"];

/// The SQL functions whose calls are counted, and nothing else.
pub const ADMIN_FUNCTIONS: [&str; 3] = ["create_graph", "refresh_graph", "drop_graph"];

/// Why a call failed or a registration or schema change was refused. A code
/// is counted where the extension raises the error, so it is counted even
/// though the error rolls its transaction back.
pub const FAILURES: [&str; 28] = [
    // Queries.
    "unknown_graph",
    "privileges",
    "needs_refresh",
    "source_unfit",
    "triggers_disabled",
    "session_dependent_type",
    "rls_caller",
    "missing_seed",
    "invalid_options",
    "limit_exceeded",
    "max_snapshot_bytes",
    "max_result_bytes",
    "strict",
    "query_other",
    // create_graph and refresh_graph.
    "isolation_level",
    "not_ordinary_table",
    "not_permanent_table",
    "inheritance",
    "rls_owner",
    "not_owner",
    "generated_column",
    "nullable_column",
    "no_unique_index",
    "unsupported_type",
    "nondeterministic_collation",
    "endpoint_type_mismatch",
    "long_key",
    // Event triggers.
    "schema_change_refused",
];

/// Why a query returned less than everything it reached, as the result's
/// `truncated` names it.
pub const TRUNCATIONS: [&str; 6] = ["depth", "vertices", "edges", "edge_visits", "result_bytes", "cancelled"];

/// Upper bounds, exclusive, of the latency buckets in milliseconds. A
/// histogram has one more bucket than bounds: the last counts the rest.
pub const LATENCY_MS_BOUNDS: [u64; 5] = [1, 10, 100, 1_000, 10_000];

/// Upper bounds, exclusive, of the buckets for rows a query read: 0, 1-9,
/// 10-99, ... and 1M or more.
pub const ROWS_BOUNDS: [u64; 7] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

/// Upper bounds, exclusive, of the buckets for the hops a result reached:
/// 0, 1, 2, 3, 4-5, 6-10 and 11 or more.
pub const HOPS_BOUNDS: [u64; 6] = [1, 2, 3, 4, 6, 11];

/// Sizes reported as a single value, by their power of ten.
pub const MAGNITUDES: [&str; 11] = ["0", "1+", "10+", "100+", "1k+", "10k+", "100k+", "1M+", "10M+", "100M+", "1B+"];

/// The receiver's body limit, and the edge's in front of it. A sender trims
/// its report to fit with [`fit`].
pub const MAX_REPORT_BYTES: usize = 16 * 1024;

/// The most one [`GraphUsage`] serializes to, with its separating comma.
const GRAPH_BYTES: usize = 61;

pub const MAX_DATABASES: usize = 256;
pub const MAX_GRAPHS_PER_DATABASE: usize = 1024;
/// create_graph refuses more than 64 of each.
pub const MAX_TABLES_PER_GRAPH: u32 = 64;

/// The bucket of `value` among `bounds`.
pub fn bucket(bounds: &[u64], value: u64) -> usize {
    bounds.iter().take_while(|&&bound| value >= bound).count()
}

/// `value` by its power of ten, one of [`MAGNITUDES`].
pub fn magnitude(value: u64) -> &'static str {
    let index = value.checked_ilog10().map_or(0, |digits| digits as usize + 1);
    MAGNITUDES[index.min(MAGNITUDES.len() - 1)]
}

/// The install ID of a PostgreSQL cluster: the first 128 bits of SHA-256
/// over a version tag and the cluster's system identifier, as 32 hex digits.
///
/// `initdb` picks the system identifier at random-ish (time and PID), it is
/// the same on every start and on physical replicas, and `pg_dump` does not
/// carry it, so a restored copy is a new install. Hashing it keeps the
/// identifier itself out of the report.
pub fn install_id(system_identifier: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pg_hydradb-install-id-v1\0");
    digest.update(system_identifier.to_be_bytes());
    hex(&digest.finalize()[..16])
}

/// 32 hex digits for a report ID, from 16 random bytes.
pub fn report_id(random: [u8; 16]) -> String {
    hex(&random)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgUsageReport {
    pub version: u32,
    /// Random per report. A report re-sent after a failed delivery keeps it,
    /// so the receiver stores it once.
    pub report_id: String,
    pub install_id: String,
    pub sent_at_unix_ms: u64,
    /// When the server started. Activity totals count from here.
    pub server_started_at_unix_ms: u64,
    pub build: Build,
    /// One entry per database with pg_hydradb installed, in no particular
    /// order and without names.
    pub databases: Vec<DatabaseUsage>,
    /// Databases with pg_hydradb this report leaves out: ones the background
    /// worker could not read, for want of a worker slot or because the read
    /// failed, and ones [`fit`] dropped to stay under [`MAX_REPORT_BYTES`].
    /// It also counts databases the worker could not tell had pg_hydradb.
    pub databases_unread: u32,
    /// Running totals since the server started. Absent unless pg_hydradb is
    /// in `shared_preload_libraries`, which the counters' shared memory needs.
    pub activity: Option<Activity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Build {
    pub extension_version: String,
    pub postgres_major: u32,
    pub os: String,
    pub arch: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseUsage {
    pub graphs: Vec<GraphUsage>,
    /// Graphs left out of `graphs` by [`fit`].
    #[serde(default)]
    pub graphs_omitted: u32,
    /// Rows of `hydradb.graph_vertices` and `hydradb.graph_adjacency`, as the
    /// planner estimates them (no table is scanned), and their size on disk
    /// with indexes. Each is one of [`MAGNITUDES`].
    pub vertices: String,
    pub edges: String,
    pub bytes: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphUsage {
    pub vertex_tables: u32,
    pub edge_tables: u32,
    /// Flagged for `refresh_graph`, so its queries fail until it runs.
    pub needs_refresh: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Activity {
    /// Keyed by [`QUERY_FUNCTIONS`].
    pub queries: BTreeMap<String, QueryUsage>,
    /// Calls, keyed by [`ADMIN_FUNCTIONS`].
    pub admin: BTreeMap<String, u64>,
    /// Keyed by [`FAILURES`].
    pub failures: BTreeMap<String, u64>,
    /// Results cut short, keyed by [`TRUNCATIONS`].
    pub truncations: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryUsage {
    pub calls: u64,
    pub failures: u64,
    /// Every call, failed or not, by [`LATENCY_MS_BOUNDS`].
    pub latency_ms: Vec<u64>,
    /// Successful calls by the vertices and edges they read from the
    /// adjacency tables ([`ROWS_BOUNDS`]), and by the most hops in their
    /// result ([`HOPS_BOUNDS`]).
    pub vertices_read: Vec<u64>,
    pub edges_read: Vec<u64>,
    pub hops: Vec<u64>,
}

fn serialized_len(report: &PgUsageReport) -> usize {
    serde_json::to_vec(report).map_or(usize::MAX, |body| body.len())
}

/// Trims `report` until it serializes to at most [`MAX_REPORT_BYTES`]. The
/// databases' graph lists share the room left once every database is listed,
/// each keeping up to an equal share and smaller lists passing on what they do
/// not use; what a list gives up is counted in `graphs_omitted`. If the
/// databases alone do not fit, the last ones are dropped and counted in
/// `databases_unread`.
pub fn fit(report: &mut PgUsageReport) {
    if serialized_len(report) <= MAX_REPORT_BYTES {
        return;
    }
    // Every graph starts out omitted, so each `graphs_omitted` is measured at
    // the most it can reach. Keeping a graph only lowers it, so the sizes
    // measured below never grow. The counts saturate, so the final ones are
    // set from the counts the report came with, not by subtracting.
    let mut omitted = Vec::with_capacity(report.databases.len());
    let mut graphs = report
        .databases
        .iter_mut()
        .map(|d| {
            let list = std::mem::take(&mut d.graphs);
            omitted.push(d.graphs_omitted);
            d.graphs_omitted = d.graphs_omitted.saturating_add(list.len() as u32);
            list
        })
        .collect::<Vec<_>>();
    while !report.databases.is_empty() && serialized_len(report) > MAX_REPORT_BYTES {
        report.databases.pop();
        graphs.pop();
        omitted.pop();
        report.databases_unread += 1;
    }
    let mut slots = MAX_REPORT_BYTES.saturating_sub(serialized_len(report)) / GRAPH_BYTES;
    let mut order = (0..graphs.len()).collect::<Vec<_>>();
    order.sort_by_key(|&index| graphs[index].len());
    for (position, &index) in order.iter().enumerate() {
        let share = slots / (order.len() - position);
        let list = &mut graphs[index];
        let keep = list.len().min(share);
        slots -= keep;
        let database = &mut report.databases[index];
        database.graphs_omitted = omitted[index].saturating_add((list.len() - keep) as u32);
        list.truncate(keep);
        database.graphs = std::mem::take(list);
    }
    // GRAPH_BYTES is an upper bound, so this only runs if it is wrong.
    while serialized_len(report) > MAX_REPORT_BYTES {
        let Some(database) = report
            .databases
            .iter_mut()
            .filter(|d| !d.graphs.is_empty())
            .max_by_key(|d| d.graphs.len())
        else {
            break;
        };
        database.graphs.pop();
        database.graphs_omitted = database.graphs_omitted.saturating_add(1);
    }
}

/// Why the receiver refuses a report.
#[derive(Debug, PartialEq, Eq)]
pub struct Invalid(pub String);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Invalid {}

fn check(ok: bool, message: impl FnOnce() -> String) -> Result<(), Invalid> {
    if ok {
        Ok(())
    } else {
        Err(Invalid(message()))
    }
}

fn is_hex_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// A short build string: a version, an OS or an architecture.
fn is_build_word(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+'))
}

fn check_keys<V>(map: &BTreeMap<String, V>, allowed: &[&str], what: &str) -> Result<(), Invalid> {
    match map.keys().find(|key| !allowed.contains(&key.as_str())) {
        Some(key) => Err(Invalid(format!("unknown {what} {key:?}"))),
        None => Ok(()),
    }
}

fn check_histogram(counts: &[u64], bounds: &[u64], what: &str) -> Result<(), Invalid> {
    check(counts.len() == bounds.len() + 1, || {
        format!("{what} has {} buckets, not {}", counts.len(), bounds.len() + 1)
    })
}

/// The receiver's check: the version, the ID shapes, the bounds on every
/// collection, and that every name is one of the fixed lists.
pub fn validate(report: &PgUsageReport) -> Result<(), Invalid> {
    check(report.version == REPORT_VERSION, || {
        format!("unsupported version {}", report.version)
    })?;
    check(is_hex_id(&report.report_id), || "report_id must be 32 hex digits".into())?;
    check(is_hex_id(&report.install_id), || "install_id must be 32 hex digits".into())?;
    let build = &report.build;
    for (field, value) in [
        ("extension_version", &build.extension_version),
        ("os", &build.os),
        ("arch", &build.arch),
    ] {
        check(is_build_word(value), || format!("build.{field} is not a short identifier"))?;
    }
    check((10..=99).contains(&build.postgres_major), || {
        format!("postgres_major {} is out of range", build.postgres_major)
    })?;
    check(report.databases.len() <= MAX_DATABASES, || {
        format!("more than {MAX_DATABASES} databases")
    })?;
    for database in &report.databases {
        check(database.graphs.len() <= MAX_GRAPHS_PER_DATABASE, || {
            format!("more than {MAX_GRAPHS_PER_DATABASE} graphs in a database")
        })?;
        for (field, value) in [
            ("vertices", &database.vertices),
            ("edges", &database.edges),
            ("bytes", &database.bytes),
        ] {
            check(MAGNITUDES.contains(&value.as_str()), || {
                format!("databases[].{field} {value:?} is not a magnitude")
            })?;
        }
        for graph in &database.graphs {
            check(
                (1..=MAX_TABLES_PER_GRAPH).contains(&graph.vertex_tables) && graph.edge_tables <= MAX_TABLES_PER_GRAPH,
                || "a graph's table counts are out of range".into(),
            )?;
        }
    }
    if let Some(activity) = &report.activity {
        check_keys(&activity.queries, &QUERY_FUNCTIONS, "query function")?;
        check_keys(&activity.admin, &ADMIN_FUNCTIONS, "admin function")?;
        check_keys(&activity.failures, &FAILURES, "failure")?;
        check_keys(&activity.truncations, &TRUNCATIONS, "truncation")?;
        for usage in activity.queries.values() {
            check(usage.failures <= usage.calls, || "more failures than calls".into())?;
            check_histogram(&usage.latency_ms, &LATENCY_MS_BOUNDS, "latency_ms")?;
            check_histogram(&usage.vertices_read, &ROWS_BOUNDS, "vertices_read")?;
            check_histogram(&usage.edges_read, &ROWS_BOUNDS, "edges_read")?;
            check_histogram(&usage.hops, &HOPS_BOUNDS, "hops")?;
        }
    }
    Ok(())
}
