//! The usage report's shape, and the check the receiver runs on it.
//!
//! **What a report may hold.** Counts, durations, and names from vocabularies
//! Turbolay defines: counter field names, error classes, failure reasons,
//! engines, physical operator names. Never query text, parameters, labels,
//! relationship types, property names or values, scope, graph, cell or node
//! names. [`validate`] enforces the shape of that rule at the receiver: every
//! name must be a short identifier, so free text cannot ride in a name.
//!
//! **Numbers are since the last report**, from one process (`run_id`). The
//! nodes of one deployment share `install_id` and each send their own reports;
//! the receiver adds them up. See [`crate::delta`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Bumped when a field changes meaning or is removed. Adding a field does not
/// bump it: an older receiver ignores fields it does not know.
pub const SCHEMA_VERSION: u32 = 1;

/// The largest report the receiver accepts. A full report is a few KB; the
/// rest is headroom for operators and failure reasons added later.
pub const MAX_REPORT_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageReport {
    pub schema_version: u32,
    /// A ULID the receiver keeps one row for, the one with the latest
    /// `sent_at_unix_ms`. A sender keeps it until the report is delivered: one
    /// re-sent with the counts since then replaces a copy that arrived without
    /// its reply, instead of counting it twice.
    pub report_id: String,
    /// The deployment: see [`crate::install_id`].
    pub install_id: String,
    /// The process: a ULID drawn at startup. A new one under the same install
    /// is a restart.
    pub run_id: String,
    pub reason: ReportReason,
    pub sent_at_unix_ms: u64,
    /// The previous report's `sent_at_unix_ms`, or process start for the first.
    pub period_start_unix_ms: u64,
    pub process_started_unix_ms: u64,
    pub build: Build,
    pub cluster: Cluster,
    /// Scalar query counters by field name, such as `queries_completed`.
    pub queries: BTreeMap<String, u64>,
    /// Failed queries by error class (`timeout`, `query`, `storage`, ...).
    pub failures_by_class: BTreeMap<String, u64>,
    /// Failed queries by engine, stage and reason. Rows with a zero count are
    /// left out.
    pub failures_by_reason: Vec<FailureCount>,
    /// Upper bounds of the latency buckets, in microseconds. Each histogram
    /// has one more bucket, for everything above the last bound.
    pub latency_bucket_bounds_us: Vec<u64>,
    /// Query latency by histogram field name, such as
    /// `read_latency_experimental`.
    pub latency: BTreeMap<String, Histogram>,
    /// Physical operator totals from the experimental engine, by operator name
    /// and then counter name (`invocations`, `self_us`, ...).
    pub operators: BTreeMap<String, BTreeMap<String, u64>>,
    pub first_query: FirstQuery,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportReason {
    /// The regular interval.
    #[default]
    Periodic,
    /// A clean shutdown, carrying what the process had not reported yet.
    Shutdown,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Build {
    /// `0.1.0+a1b2c3d4e5f6`: version and commit.
    pub version: String,
    /// `linux`, `macos`, ...
    pub os: String,
    /// `x86_64`, `aarch64`, ...
    pub arch: String,
    /// slatedb's `CLOUD_PROVIDER`: `aws`, `local`, ...
    pub object_store: String,
    /// The Cypher engine queries run through by default.
    pub cypher_engine: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    /// Nodes in this node's live view, itself included.
    pub live_nodes: u32,
    /// Cells the deployment is configured with.
    pub cells: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureCount {
    pub engine: String,
    /// `prepare` or `execute`.
    pub stage: String,
    pub reason: String,
    pub count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Histogram {
    /// Observations per bucket, not cumulative; see
    /// [`UsageReport::latency_bucket_bounds_us`].
    pub bucket_counts: Vec<u64>,
    pub sum_us: u64,
}

/// How long after the process started its first query succeeded, and its
/// first query failed. Repeated in every report of the process; the receiver
/// keeps an install's earliest.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirstQuery {
    pub completed_after_start_ms: Option<u64>,
    pub failed_after_start_ms: Option<u64>,
}

/// Why the receiver turned a report away.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invalid(pub &'static str);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Invalid {}

// Bounds generous enough for growth and tight enough that a report cannot be
// used to store anything else.
const MAX_NAME_BYTES: usize = 64;
const MAX_QUERY_COUNTERS: usize = 64;
const MAX_CLASSES: usize = 32;
const MAX_FAILURE_ROWS: usize = 256;
const MAX_HISTOGRAMS: usize = 16;
const MAX_BUCKETS: usize = 64;
const MAX_OPERATORS: usize = 64;
const MAX_OPERATOR_COUNTERS: usize = 32;
const MAX_CLUSTER: u32 = 100_000;

/// The receiver's check: a report that passes has the right version, IDs in
/// the shapes the sender makes, and nothing but short identifiers where names
/// go. It cannot prove the numbers are honest — anyone can POST — only that
/// the report cannot carry free text.
pub fn validate(report: &UsageReport) -> Result<(), Invalid> {
    if report.schema_version != SCHEMA_VERSION {
        return Err(Invalid("unsupported schema_version"));
    }
    if !crate::is_install_id(&report.install_id) {
        return Err(Invalid("install_id is not 32 lowercase hex characters"));
    }
    if !is_ulid(&report.report_id) || !is_ulid(&report.run_id) {
        return Err(Invalid("report_id and run_id must be ULIDs"));
    }
    if report.period_start_unix_ms > report.sent_at_unix_ms || report.process_started_unix_ms > report.period_start_unix_ms {
        return Err(Invalid("timestamps out of order"));
    }
    let build = &report.build;
    for value in [&build.version, &build.os, &build.arch, &build.object_store, &build.cypher_engine] {
        if !is_token(value) {
            return Err(Invalid("build fields must be short identifiers"));
        }
    }
    if report.cluster.live_nodes > MAX_CLUSTER || report.cluster.cells > MAX_CLUSTER {
        return Err(Invalid("cluster size out of range"));
    }
    if report.queries.len() > MAX_QUERY_COUNTERS || !report.queries.keys().all(|name| is_name(name)) {
        return Err(Invalid("queries must be a few named counters"));
    }
    if report.failures_by_class.len() > MAX_CLASSES || !report.failures_by_class.keys().all(|name| is_name(name)) {
        return Err(Invalid("failures_by_class must be a few named counters"));
    }
    if report.failures_by_reason.len() > MAX_FAILURE_ROWS
        || !report
            .failures_by_reason
            .iter()
            .all(|row| is_name(&row.engine) && is_name(&row.stage) && is_name(&row.reason))
    {
        return Err(Invalid("failures_by_reason must be a few named rows"));
    }
    if report.latency_bucket_bounds_us.windows(2).any(|bounds| bounds[0] >= bounds[1]) {
        return Err(Invalid("latency bucket bounds must be strictly increasing"));
    }
    let buckets = report.latency_bucket_bounds_us.len() + 1;
    if buckets > MAX_BUCKETS || report.latency.len() > MAX_HISTOGRAMS {
        return Err(Invalid("too many latency histograms or buckets"));
    }
    if !report
        .latency
        .iter()
        .all(|(name, histogram)| is_name(name) && histogram.bucket_counts.len() == buckets)
    {
        return Err(Invalid("each latency histogram needs a name and one count per bucket"));
    }
    if report.operators.len() > MAX_OPERATORS
        || !report.operators.iter().all(|(name, counters)| {
            is_name(name) && counters.len() <= MAX_OPERATOR_COUNTERS && counters.keys().all(|counter| is_name(counter))
        })
    {
        return Err(Invalid("operators must be a few named operators with named counters"));
    }
    Ok(())
}

/// An identifier: letters, digits and `_`, starting with a letter. Counter
/// fields, error classes, failure reasons, engines and operator names all fit.
fn is_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    value.len() <= MAX_NAME_BYTES
        && bytes.next().is_some_and(|first| first.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// A version, OS or architecture string: identifier characters plus `.`, `+`
/// and `-`, as in `0.1.0+a1b2c3d4e5f6`.
fn is_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_NAME_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'+' | b'-'))
}

/// A ULID as `ulid` prints it: 26 characters of Crockford base32, upper case.
fn is_ulid(value: &str) -> bool {
    value.len() == 26
        && value.as_bytes()[0] <= b'7'
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (byte.is_ascii_uppercase() && !matches!(byte, b'I' | b'L' | b'O' | b'U')))
}
