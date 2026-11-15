//! The anonymous usage report.
//!
//! `hydradb.usage_report()` builds it. With pg_hydradb in
//! `shared_preload_libraries`, the report also holds activity totals kept in
//! shared memory, and a background worker sends it once an hour; turning
//! `pg_hydradb.usage_report` off stops the sending. The format, its fixed
//! lists of names and the receiver's check live in `hydradb-pg-usage-report`.
//!
//! Counters live in shared memory, not a table, because a refusal aborts its
//! transaction and would roll a table's counter back with it.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::sync::atomic::{
    AtomicBool, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hydradb_pg_usage_report as report;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{pg_shmem_init, JsonB, PgAtomic};
use serde::Deserialize;
use serde_json::Value;

pub(crate) static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub(crate) static URL: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(DEFAULT_URL));
pub(crate) static INTERVAL_S: GucSetting<i32> = GucSetting::<i32>::new(HOUR_S);

const DEFAULT_URL: &CStr = c"https://telemetry.hydradb.com/v1/pg-usage";
const HOUR_S: i32 = 3_600;
const DAY_S: i32 = 86_400;
/// The first report goes out this long after the worker starts, or after one
/// interval if that is shorter.
const FIRST_REPORT_S: u64 = 600;
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const SEND_TRIES: u32 = 3;

const LATENCY_BUCKETS: usize = report::LATENCY_MS_BOUNDS.len() + 1;
const ROWS_BUCKETS: usize = report::ROWS_BOUNDS.len() + 1;
const HOPS_BUCKETS: usize = report::HOPS_BOUNDS.len() + 1;

/// Index of each SQL query function in `report::QUERY_FUNCTIONS`.
#[derive(Clone, Copy)]
pub(crate) enum Function {
    ReachableVertices = 0,
    ReachableEdges = 1,
    PathsFrom = 2,
    PathsBetween = 3,
}

#[derive(Default)]
struct QueryCounters {
    calls: AtomicU64,
    failures: AtomicU64,
    latency_ms: [AtomicU64; LATENCY_BUCKETS],
    vertices_read: [AtomicU64; ROWS_BUCKETS],
    edges_read: [AtomicU64; ROWS_BUCKETS],
    hops: [AtomicU64; HOPS_BUCKETS],
}

/// Running totals since shared memory was set up: at server start, and again
/// after a crash restart, which resets them and `started_at_unix_ms` together.
#[derive(Default)]
pub(crate) struct Counters {
    started_at_unix_ms: AtomicU64,
    queries: [QueryCounters; report::QUERY_FUNCTIONS.len()],
    admin: [AtomicU64; report::ADMIN_FUNCTIONS.len()],
    failures: [AtomicU64; report::FAILURES.len()],
    truncations: [AtomicU64; report::TRUNCATIONS.len()],
}

// SAFETY: atomics only, with no pointers, valid in every process mapping it.
unsafe impl PGRXSharedMemory for Counters {}

/// Room for one database's summary, at `MAX_GRAPHS_PER_DATABASE` graphs.
const SLOT_BYTES: usize = 64 * 1024;

/// Where a per-database worker leaves its summary for the reporting worker.
pub(crate) struct Slot {
    state: u8,
    len: usize,
    bytes: [u8; SLOT_BYTES],
}

const SLOT_EMPTY: u8 = 0;
const SLOT_ABSENT: u8 = 1;
const SLOT_FILLED: u8 = 2;

impl Default for Slot {
    fn default() -> Self {
        Self {
            state: SLOT_EMPTY,
            len: 0,
            bytes: [0; SLOT_BYTES],
        }
    }
}

// SAFETY: plain bytes, read and written under its lock.
unsafe impl PGRXSharedMemory for Slot {}

// SAFETY: the names are this extension's own.
static COUNTERS: PgAtomic<Counters> = unsafe { PgAtomic::new(c"pg_hydradb usage counters") };
static SLOT: PgLwLock<Slot> = unsafe { PgLwLock::new(c"pg_hydradb usage slot") };

/// Set in the postmaster when pg_hydradb is preloaded, and inherited by every
/// process it starts: only then does the shared memory exist.
static PRELOADED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// The failure code the current call raised, set where it is raised.
    static FAILURE: Cell<Option<&'static str>> = const { Cell::new(None) };
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// Called from `_PG_init`.
pub(crate) fn init() {
    GucRegistry::define_bool_guc(
        c"pg_hydradb.usage_report",
        c"Send HydraDB an anonymous usage report once an hour.",
        c"Counts, ranges and timestamps only; never names, keys or query text. \
          Needs pg_hydradb in shared_preload_libraries to send anything.",
        &ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_hydradb.usage_report_url",
        c"Where the usage report is sent.",
        c"",
        &URL,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"pg_hydradb.usage_report_interval",
        c"Seconds between usage reports.",
        c"",
        &INTERVAL_S,
        1,
        DAY_S * 7,
        GucContext::Sighup,
        GucFlags::UNIT_S,
    );
    // SAFETY: read in _PG_init, on the postmaster's only thread.
    if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }
    PRELOADED.store(true, Relaxed);
    pg_shmem_init!(
        COUNTERS = Counters {
            started_at_unix_ms: AtomicU64::new(now_unix_ms()),
            ..Default::default()
        }
    );
    pg_shmem_init!(SLOT);
    if ENABLED.get() {
        BackgroundWorkerBuilder::new("pg_hydradb usage report")
            .set_type("pg_hydradb usage report")
            .set_library("pg_hydradb")
            .set_function("pg_hydradb_usage_report_main")
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(3600)))
            .load();
    }
}

fn counters() -> Option<&'static Counters> {
    PRELOADED.load(Relaxed).then(|| COUNTERS.get())
}

fn count(list: &[&str], counters: &[AtomicU64], name: &str) {
    if let Some(index) = list.iter().position(|known| *known == name) {
        counters[index].fetch_add(1, Relaxed);
    }
}

/// Records why the current call fails, where the failure is raised.
pub(crate) fn fail(code: &'static str) {
    FAILURE.set(Some(code));
}

/// Records why the current call fails, unless its raiser already did.
pub(crate) fn fail_unless_set(code: &'static str) {
    if FAILURE.get().is_none() {
        FAILURE.set(Some(code));
    }
}

/// Counts an admin call or a refusal by name; unknown names are ignored. SQL
/// calls it as `hydradb.usage_count` just before a refusal is raised.
#[pg_extern(volatile, parallel_safe)]
fn usage_count(event: &str) {
    if let Some(counters) = counters() {
        count(&report::ADMIN_FUNCTIONS, &counters.admin, event);
        count(&report::FAILURES, &counters.failures, event);
    }
}

/// Counts one query call. Dropped when the call ends, by returning or by an
/// error unwinding through it, which counts as a failure.
pub(crate) struct Call {
    function: Function,
    started: Instant,
    succeeded: bool,
}

impl Call {
    pub(crate) fn start(function: Function) -> Self {
        FAILURE.set(None);
        Self {
            function,
            started: Instant::now(),
            succeeded: false,
        }
    }

    /// A call that returned `result`, having read `vertices` and `edges`.
    pub(crate) fn succeeded(&mut self, result: &Value, vertices: usize, edges: usize) {
        self.succeeded = true;
        let Some(counters) = counters() else { return };
        let query = &counters.queries[self.function as usize];
        query.vertices_read[report::bucket(&report::ROWS_BOUNDS, vertices as u64)].fetch_add(1, Relaxed);
        query.edges_read[report::bucket(&report::ROWS_BOUNDS, edges as u64)].fetch_add(1, Relaxed);
        query.hops[report::bucket(&report::HOPS_BOUNDS, hops(result))].fetch_add(1, Relaxed);
        for reason in result["truncated"].as_array().into_iter().flatten().filter_map(Value::as_str) {
            count(&report::TRUNCATIONS, &counters.truncations, reason);
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        let Some(counters) = counters() else { return };
        let query = &counters.queries[self.function as usize];
        // Before `failures`, which publishes it with Release: a snapshot that
        // reads a failure (Acquire) then reads its call too.
        query.calls.fetch_add(1, Relaxed);
        let elapsed = self.started.elapsed().as_millis() as u64;
        query.latency_ms[report::bucket(&report::LATENCY_MS_BOUNDS, elapsed)].fetch_add(1, Relaxed);
        if !self.succeeded {
            query.failures.fetch_add(1, Release);
            let code = FAILURE.take().unwrap_or("query_other");
            count(&report::FAILURES, &counters.failures, code);
        }
    }
}

/// The most hops in a result: the deepest item a traversal returned, or the
/// longest path.
fn hops(result: &Value) -> u64 {
    result["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| {
            item["depth"]
                .as_u64()
                .unwrap_or_else(|| item["edges"].as_array().map_or(0, |edges| edges.len() as u64))
        })
        .max()
        .unwrap_or(0)
}

fn activity(counters: &Counters) -> report::Activity {
    let load = |values: &[AtomicU64]| values.iter().map(|value| value.load(Relaxed)).collect::<Vec<_>>();
    let named = |names: &[&str], values: &[AtomicU64]| {
        names
            .iter()
            .zip(values)
            .map(|(name, value)| (name.to_string(), value.load(Relaxed)))
            .filter(|(_, value)| *value > 0)
            .collect::<BTreeMap<_, _>>()
    };
    report::Activity {
        queries: report::QUERY_FUNCTIONS
            .iter()
            .zip(&counters.queries)
            .map(|(name, query)| {
                // `failures` first: every failure it includes has its call in
                // `calls` by the time `calls` is read, so a concurrent call can
                // never leave the snapshot with more failures than calls,
                // which `validate` would refuse.
                let failures = query.failures.load(Acquire);
                let usage = report::QueryUsage {
                    calls: query.calls.load(Relaxed),
                    failures,
                    latency_ms: load(&query.latency_ms),
                    vertices_read: load(&query.vertices_read),
                    edges_read: load(&query.edges_read),
                    hops: load(&query.hops),
                };
                (name.to_string(), usage)
            })
            .collect(),
        admin: named(&report::ADMIN_FUNCTIONS, &counters.admin),
        failures: named(&report::FAILURES, &counters.failures),
        truncations: named(&report::TRUNCATIONS, &counters.truncations),
    }
}

fn random_bytes() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    // SAFETY: writes exactly `bytes.len()` bytes into `bytes`.
    if !unsafe { pg_sys::pg_strong_random(bytes.as_mut_ptr().cast(), bytes.len()) } {
        // Uniqueness is all a report ID needs.
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        bytes = (nanos ^ (std::process::id() as u128) << 64).to_le_bytes();
    }
    bytes
}

fn build() -> report::Build {
    report::Build {
        extension_version: env!("CARGO_PKG_VERSION").into(),
        postgres_major: pg_sys::PG_VERSION_NUM / 10_000,
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
    }
}

fn new_report(report_id: String, databases: Vec<report::DatabaseUsage>, unread: u32) -> report::PgUsageReport {
    let counters = counters();
    report::PgUsageReport {
        version: report::REPORT_VERSION,
        report_id,
        // SAFETY: reads a value fixed at startup.
        install_id: report::install_id(unsafe { pg_sys::GetSystemIdentifier() }),
        sent_at_unix_ms: now_unix_ms(),
        server_started_at_unix_ms: counters.map_or(0, |counters| counters.started_at_unix_ms.load(Relaxed)),
        build: build(),
        databases,
        databases_unread: unread,
        activity: counters.map(activity),
    }
}

/// What `DATABASE_SQL` returns.
#[derive(Deserialize)]
struct DatabaseRow {
    graphs: Vec<report::GraphUsage>,
    /// Every registered graph, including those past the listing limit.
    graph_count: u64,
    vertices: f64,
    edges: f64,
    bytes: f64,
}

/// The current database's graphs and adjacency tables. It lists the first `$1`
/// graphs and counts all of them. Row counts are the planner's estimates, so
/// nothing large is scanned; a table never analyzed is counted, up to 100,000
/// rows.
const DATABASE_SQL: &str = "
SELECT jsonb_build_object(
    'graphs', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
            'vertex_tables', (SELECT count(*) FROM hydradb.graph_vertex_tables v WHERE v.graph = g.name),
            'edge_tables', (SELECT count(*) FROM hydradb.graph_edge_tables e WHERE e.graph = g.name),
            'needs_refresh', g.needs_refresh IS NOT NULL) ORDER BY g.registration)
        FROM (SELECT * FROM hydradb.graphs ORDER BY registration LIMIT $1) g), '[]'),
    'graph_count', (SELECT count(*) FROM hydradb.graphs),
    'vertices', (SELECT CASE WHEN reltuples >= 0 THEN reltuples
        ELSE (SELECT count(*) FROM (SELECT 1 FROM hydradb.graph_vertices LIMIT 100000) s) END
        FROM pg_class WHERE oid = 'hydradb.graph_vertices'::regclass),
    'edges', (SELECT CASE WHEN reltuples >= 0 THEN reltuples
        ELSE (SELECT count(*) FROM (SELECT 1 FROM hydradb.graph_adjacency LIMIT 100000) s) END
        FROM pg_class WHERE oid = 'hydradb.graph_adjacency'::regclass),
    'bytes', pg_total_relation_size('hydradb.graph_vertices') + pg_total_relation_size('hydradb.graph_adjacency'))";

fn database_usage() -> Result<report::DatabaseUsage, String> {
    let row = Spi::get_one_with_args::<JsonB>(DATABASE_SQL, &[(report::MAX_GRAPHS_PER_DATABASE as i64).into()])
        .map_err(|error| error.to_string())?
        .ok_or("no database summary")?;
    let row: DatabaseRow = serde_json::from_value(row.0).map_err(|error| error.to_string())?;
    let magnitude = |value: f64| report::magnitude(value.max(0.0) as u64).to_string();
    let omitted = row.graph_count.saturating_sub(row.graphs.len() as u64);
    Ok(report::DatabaseUsage {
        graphs: row.graphs,
        graphs_omitted: u32::try_from(omitted).unwrap_or(u32::MAX),
        vertices: magnitude(row.vertices),
        edges: magnitude(row.edges),
        bytes: magnitude(row.bytes),
    })
}

/// The report this server would send, with only the current database. The
/// background worker's report covers every database with pg_hydradb. Without
/// preloading, `activity` is null.
#[pg_extern(volatile, parallel_unsafe)]
fn usage_report() -> JsonB {
    let database = database_usage().unwrap_or_else(|error| pgrx::error!("usage report: {error}"));
    let report = new_report(report::report_id(random_bytes()), vec![database], 0);
    JsonB(serde_json::to_value(report).unwrap_or_else(|error| pgrx::error!("usage report: {error}")))
}

/// The reporting worker. It connects to no database: it reads the list of
/// databases from the shared catalog, and starts one short-lived worker per
/// database to read that database's summary.
/// How often reports go out, for the startup log line.
fn every(interval_s: i32) -> String {
    match interval_s {
        HOUR_S => "once an hour".to_owned(),
        DAY_S => "once a day".to_owned(),
        s if s > 0 && s % HOUR_S == 0 => format!("every {} hours", s / HOUR_S),
        s => format!("every {} s", s.max(1)),
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_hydradb_usage_report_main(_argument: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(None, None);
    log!(
        "pg_hydradb: sending an anonymous usage report {} to {}; set pg_hydradb.usage_report = off to stop",
        every(INTERVAL_S.get()),
        url()
    );
    let interval = || Duration::from_secs(INTERVAL_S.get().max(1) as u64);
    let mut next = Instant::now() + interval().min(Duration::from_secs(FIRST_REPORT_S));
    // Kept until a report is delivered, so a re-sent report replaces the
    // stored one instead of counting twice.
    let mut pending: Option<String> = None;
    let mut failures = 0u32;
    loop {
        let wait = next.saturating_duration_since(Instant::now());
        if !BackgroundWorker::wait_latch(Some(wait.max(Duration::from_millis(10)))) {
            break;
        }
        if BackgroundWorker::sighup_received() {
            // SAFETY: the documented way for a worker to reload its settings.
            unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };
        }
        if Instant::now() < next {
            continue;
        }
        if ENABLED.get() {
            let report_id = pending.get_or_insert_with(|| report::report_id(random_bytes())).clone();
            let (databases, unread) = read_databases();
            let mut report = new_report(report_id, databases, unread);
            report::fit(&mut report);
            match send(&url(), &report) {
                Ok(()) => {
                    pending = None;
                    failures = 0;
                }
                Err(error) => {
                    failures += 1;
                    if failures == 1 {
                        log!("pg_hydradb: could not send the usage report, will retry next time: {error}");
                    } else {
                        debug1!("pg_hydradb: could not send the usage report: {error}");
                    }
                }
            }
        }
        let jitter = interval().as_millis() as u64 / 10;
        let offset = u64::from_le_bytes(random_bytes()[..8].try_into().unwrap_or_default()) % (2 * jitter + 1);
        next = Instant::now() + interval() - Duration::from_millis(jitter) + Duration::from_millis(offset);
    }
}

fn url() -> String {
    URL.get().map(|url| url.to_string_lossy().into_owned()).unwrap_or_default()
}

fn send(url: &str, report: &report::PgUsageReport) -> Result<(), String> {
    report::validate(report).map_err(|error| format!("invalid report: {error}"))?;
    let body = serde_json::to_vec(report).map_err(|error| error.to_string())?;
    let agent = ureq::AgentBuilder::new().timeout(SEND_TIMEOUT).build();
    let mut last = String::new();
    for attempt in 0..SEND_TRIES {
        if attempt > 0 {
            std::thread::sleep(Duration::from_secs(1 << (attempt - 1)));
        }
        match agent.post(url).set("content-type", "application/json").send_bytes(&body) {
            Ok(_) => return Ok(()),
            // A 4xx will not change on a retry.
            Err(ureq::Error::Status(status, _)) if (400..500).contains(&status) && status != 429 => {
                return Err(format!("HTTP {status}"));
            }
            Err(ureq::Error::Status(status, _)) => last = format!("HTTP {status}"),
            Err(error) => last = error.to_string(),
        }
    }
    Err(last)
}

/// OIDs of the databases a worker can connect to.
fn database_oids() -> Vec<pg_sys::Oid> {
    BackgroundWorker::transaction(|| {
        let mut oids = Vec::new();
        // SAFETY: a heap scan of the shared catalog pg_database inside a
        // transaction, as autovacuum's get_database_list does it.
        unsafe {
            let relation = pg_sys::table_open(pg_sys::DatabaseRelationId, pg_sys::AccessShareLock as _);
            let scan = pg_sys::table_beginscan_catalog(relation, 0, std::ptr::null_mut());
            loop {
                let tuple = pg_sys::heap_getnext(scan, pg_sys::ScanDirection::ForwardScanDirection);
                if tuple.is_null() {
                    break;
                }
                let header = (*tuple).t_data;
                let form = header
                    .cast::<u8>()
                    .add((*header).t_hoff as usize)
                    .cast::<pg_sys::FormData_pg_database>();
                // -2 marks a database a DROP DATABASE left half dropped.
                if (*form).datallowconn && !(*form).datistemplate && (*form).datconnlimit != -2 {
                    oids.push((*form).oid);
                }
            }
            pg_sys::heap_endscan(scan);
            pg_sys::table_close(relation, pg_sys::AccessShareLock as _);
        }
        oids
    })
}

/// The summary of every database with pg_hydradb, and how many databases
/// could not be read.
fn read_databases() -> (Vec<report::DatabaseUsage>, u32) {
    let mut databases = Vec::new();
    let mut unread = 0u32;
    for oid in database_oids() {
        if databases.len() == report::MAX_DATABASES {
            unread += 1;
            continue;
        }
        *SLOT.exclusive() = Slot::default();
        let worker = BackgroundWorkerBuilder::new("pg_hydradb usage report reader")
            .set_type("pg_hydradb usage report")
            .set_library("pg_hydradb")
            .set_function("pg_hydradb_usage_report_database_main")
            .enable_spi_access()
            .set_restart_time(None)
            .set_argument(Some(pg_sys::Datum::from(oid)))
            // SAFETY: reads this process's PID.
            .set_notify_pid(unsafe { pg_sys::MyProcPid })
            .load_dynamic();
        let finished = worker.ok().is_some_and(|worker| worker.wait_for_shutdown().is_ok());
        let slot = SLOT.share();
        match (finished, slot.state) {
            (true, SLOT_ABSENT) => {}
            (true, SLOT_FILLED) => match serde_json::from_slice(&slot.bytes[..slot.len]) {
                Ok(database) => databases.push(database),
                Err(_) => unread += 1,
            },
            _ => unread += 1,
        }
    }
    (databases, unread)
}

/// Reads one database's summary into `SLOT`, or marks it absent when the
/// database does not have pg_hydradb.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_hydradb_usage_report_database_main(argument: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    // SAFETY: the reporting worker passed a database OID.
    let oid = unsafe { pg_sys::Oid::from_datum(argument, false) };
    BackgroundWorker::connect_worker_to_spi_by_oid(oid, None);
    let summary = BackgroundWorker::transaction(|| {
        // Its reads are small: a few catalog rows, counts capped at 100,000,
        // and sizes. Only a lock can hold one up, say an exclusive lock on
        // hydradb.graphs, and the reporting worker waits for this one before
        // reading the next database, so a lock kept for good would stop
        // every report. lock_timeout bounds that wait in a background
        // worker too; statement_timeout would not, since PostgreSQL arms it
        // only for a client's statement, not for SPI. A database whose read
        // times out is counted in databases_unread.
        Spi::run("SET LOCAL lock_timeout = '5s'").map_err(|error| error.to_string())?;
        let installed = Spi::get_one::<bool>("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_hydradb')")
            .map_err(|error| error.to_string())?
            .unwrap_or(false);
        if !installed {
            return Ok(None);
        }
        let database = database_usage()?;
        serde_json::to_vec(&database).map(Some).map_err(|error| error.to_string())
    });
    let mut slot = SLOT.exclusive();
    match summary {
        Ok(None) => slot.state = SLOT_ABSENT,
        Ok(Some(bytes)) if bytes.len() <= SLOT_BYTES => {
            slot.bytes[..bytes.len()].copy_from_slice(&bytes);
            slot.len = bytes.len();
            slot.state = SLOT_FILLED;
        }
        _ => {}
    }
}
