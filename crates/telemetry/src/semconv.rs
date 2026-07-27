//! The `turbolay.*` attribute registry.
//!
//! One table, used by all three paths. Consistency is the whole point: the
//! read, write and indexing paths correlate through *attribute equality* — no
//! parent/child edge connects a `graph-node` write to the `graph-indexer` cycle
//! that consumes it — so a name spelled two ways is a join that silently
//! returns nothing.
//!
//! Every constant here is the full dotted attribute key, so call sites read as
//! `span.record(semconv::CELL_ID, …)` and a typo is a compile error rather than
//! a missing dashboard row.
//!
//! # Cardinality
//!
//! [`CELL_ID`], [`EDGE_TYPE`], [`NODE_ID`], [`QUERY_ACCESS_PATH`] and
//! [`KERNEL`] are bounded by deployment size and are safe anywhere.
//!
//! [`QUERY_FINGERPRINT`] is bounded by the number of distinct query *shapes*,
//! which for a fixed application is small — that is precisely why it is a
//! fingerprint and not the query text.
//!
//! [`SCOPE`] is unbounded in principle, since it grows with tenant count. That
//! is acceptable on spans, where the backend indexes rather than pre-aggregates,
//! and it is the reason the plan defers mirroring these onto metric labels.
//! Do not turn `SCOPE` into a metric dimension.
//!
//! # What is never recorded
//!
//! Query parameter values, property values, vertex/edge property maps, bearer
//! tokens and bookmarks. That rule is enforced structurally by
//! [`crate::redact`] rather than left to call-site discipline.

/// Graph scope — the tenant root. See the cardinality note above.
pub const SCOPE: &str = "turbolay.scope";

/// Cell identifier. **The** join key across the read, write and indexing paths.
pub const CELL_ID: &str = "turbolay.cell_id";

/// Which `graph-node` this span ran on.
pub const NODE_ID: &str = "turbolay.node_id";

/// The epoch a read was pinned to. Mandatory on read-path spans: the whole
/// BFG-007 / BFG-009 / BFG-011 family of bugs is a question about this value
/// versus what was actually visible.
pub const READ_EPOCH: &str = "turbolay.read_epoch";

/// Epoch returned by a successful commit.
pub const COMMIT_EPOCH: &str = "turbolay.commit_epoch";

/// Compiled artifact generation. Carried on both the indexing path (what was
/// produced) and the read path (what was consumed).
pub const GENERATION: &str = "turbolay.generation";

/// The storage sequence a generation was built from.
pub const BASE_SEQUENCE: &str = "turbolay.base_sequence";

/// Edge type. Bounded by schema, safe as a dimension.
pub const EDGE_TYPE: &str = "turbolay.edge_type";

/// Query shape hash with parameters elided. Never the query text.
pub const QUERY_FINGERPRINT: &str = "turbolay.query.fingerprint";

/// Per-pattern access path, from the planner's `RowQueryPlan`.
pub const QUERY_ACCESS_PATH: &str = "turbolay.query.access_path";

/// Optimizer passes the plan went through.
pub const QUERY_OPTIMIZER_PASSES: &str = "turbolay.query.optimizer_passes";

/// Planner's cardinality estimate.
pub const QUERY_ROWS_ESTIMATED: &str = "turbolay.query.rows_estimated";

/// Rows actually returned. The pair with [`QUERY_ROWS_ESTIMATED`] is how a
/// mis-costed plan is spotted without reading the planner.
pub const QUERY_ROWS_RETURNED: &str = "turbolay.query.rows_returned";

/// The alarm bit: the plan contained a full scan or a fallback pass. Set it
/// regardless of elapsed time — a full-scanning query that returns in 3ms today
/// is a timeout after the tenant grows, and only this attribute sees it coming.
pub const QUERY_FULL_SCAN: &str = "turbolay.query.full_scan";

/// Which rung of the sparse-kernel ladder served the traversal.
pub const KERNEL: &str = "turbolay.kernel";

/// SlateDB writer epoch held during a write.
pub const WRITER_EPOCH: &str = "turbolay.writer.epoch";

/// Retry count attributed to a single mutation. The kernel already counts
/// retries in aggregate; this is what attributes them to one operation.
pub const WRITER_RETRIES: &str = "turbolay.writer.retries";

/// Node named by the advisory cell-writer record as the last to promote.
/// Grouping fence events by [`CELL_ID`] and counting distinct values of this
/// attribute over a window is the writer ping-pong, as one query.
pub const WRITER_LAST_PROMOTED_BY: &str = "turbolay.writer.last_promoted_by";

/// Epoch recorded alongside [`WRITER_LAST_PROMOTED_BY`].
pub const WRITER_LAST_PROMOTED_EPOCH: &str = "turbolay.writer.last_promoted_epoch";

/// Timestamp recorded alongside [`WRITER_LAST_PROMOTED_BY`].
pub const WRITER_LAST_PROMOTED_AT: &str = "turbolay.writer.last_promoted_at";

/// Requested read consistency.
pub const CONSISTENCY: &str = "turbolay.consistency";

/// What rendezvous said about who owns the cell: `local`, `remote`, `unowned`
/// or `unknown`.
///
/// Distinct from [`OUTCOME`], which is a closed `success | skipped | failed`
/// vocabulary about a unit of work. This is about *routing*, and the two
/// answers that are neither success nor failure are the interesting ones:
/// `unowned` is an empty fleet, `unknown` is a node that has shed its view and
/// is refusing rather than guessing.
///
/// Without it the only ownership answer that reaches telemetry is `remote`,
/// and only as text inside a `NotCellWriter` display — so "how often does a
/// write land on a node that does not own the cell" is an inference from error
/// strings rather than a query. Bounded to four values, so it is safe on a
/// metric label as well as a span.
pub const PLACEMENT_OWNERSHIP: &str = "turbolay.placement.ownership";

/// Placement view state: `fresh`, `grace` or `shed`.
pub const PLACEMENT_STATE: &str = "turbolay.placement.state";

/// Previous placement view state on a transition event.
pub const PLACEMENT_PREVIOUS_STATE: &str = "turbolay.placement.previous_state";

/// Number of live nodes carried by the current placement view.
pub const PLACEMENT_LIVE_NODES: &str = "turbolay.placement.live_nodes";

/// Delay applied before the next writer re-open attempt.
pub const WRITER_REOPEN_DELAY_MS: &str = "turbolay.writer.reopen_delay_ms";

/// Maximum delay one client request is allowed to wait for a writer re-open.
pub const WRITER_REOPEN_CAP_MS: &str = "turbolay.writer.reopen_cap_ms";

/// Caller-supplied request identifier, read from Bolt `tx_metadata`.
///
/// This is what joins a Turbolay span to the caller's own log line. Nothing
/// else does: the query fingerprint says *which statement*, and only this says
/// *which invocation of it*.
///
/// Three properties are load-bearing.
///
/// **Unbounded by construction** — one value per inbound request. That is the
/// whole point, and it is why this must **never** become a metric label. It
/// belongs in the spans-and-logs bucket alongside [`SCOPE`], more emphatically
/// than [`SCOPE`] does.
///
/// **Never minted by Turbolay.** An absent value stays absent. A
/// server-invented id matches nothing upstream and is worse than no field at
/// all, because it looks like a join key and is not.
///
/// **Untrusted.** It arrives from any Bolt client and becomes both a span
/// attribute and a log field, so it is validated on arrival — bounded length,
/// printable ASCII, rejected rather than truncated.
pub const CORRELATION_ID: &str = "turbolay.correlation_id";

/// Caller-supplied label for the operation that issued the statement, read
/// from Bolt `tx_metadata` under the same rules as [`CORRELATION_ID`].
///
/// Where [`CORRELATION_ID`] identifies the request, this identifies the step
/// within it — one caller operation may issue many statements, several of them
/// looping over batches, and without this they are indistinguishable.
pub const CALLER_STEP: &str = "turbolay.caller.step";

/// Outcome of a unit of indexing work — see [`crate::Outcome`]. Distinguishing
/// "nothing to do" from "not running" is most of what an indexer needs to
/// report, and only an explicit outcome does that.
pub const OUTCOME: &str = "turbolay.outcome";

/// Coarse failure class. See [`crate::ErrorClass`].
pub const ERROR_CLASS: &str = "error.class";

/// Force the head sampler to keep the trace. **Creation-time only, and only on
/// a span that starts a trace.**
///
/// Both halves of that sentence are load-bearing, and getting either wrong
/// produces a call site that reads like a guarantee and does nothing:
///
/// - `ShouldSample` runs once, when a span starts, and is handed the attributes
///   *present at that moment*. A field declared `tracing::field::Empty` and
///   filled in later by `Span::record` is not among them, so a post-hoc
///   `span.record(SAMPLING_FORCE, true)` is a silent no-op.
/// - The sampler defers to a valid parent, because re-deciding per span is what
///   produces traces with holes in the middle. So this attribute is consulted
///   only where there is no parent to defer to.
///
/// Anything whose keep-worthiness is discovered *while the work runs* — an
/// error, a full scan the planner only just found — cannot use this. Use
/// [`SAMPLING_TAIL_KEEP`], which is the collector's input, and read
/// [`crate::sampling`] for why the two cannot be the same key.
pub const SAMPLING_FORCE: &str = "turbolay.sampling.force";

/// Marks a trace as worth keeping *after* the fact, for the collector's
/// tail-sampling processor.
///
/// The value is the reason — `error` or `full_scan` — rather than a bare
/// `true`, so one attribute supports separate retention rates per reason
/// instead of forcing every alarm through one policy.
///
/// This is deliberately **not** [`SAMPLING_FORCE`] and deliberately invisible to
/// the head sampler. By the time a call site knows to set it, the head sampling
/// decision for the trace has already been taken and the sibling spans have
/// already been dropped; no in-process mechanism can undo that. The separate
/// name is what stops the two from being confused again — see
/// [`crate::sampling`] for the collector policy this requires.
pub const SAMPLING_TAIL_KEEP: &str = "turbolay.sampling.tail_keep";

/// [`SAMPLING_TAIL_KEEP`] value: the span recorded a failure.
pub const SAMPLING_TAIL_KEEP_ERROR: &str = "error";

/// [`SAMPLING_TAIL_KEEP`] value: the planner produced a full scan or a fallback
/// pass. See [`QUERY_FULL_SCAN`].
pub const SAMPLING_TAIL_KEEP_FULL_SCAN: &str = "full_scan";

/// Wire protocol spoken to the client, per OTel semantic conventions.
/// Turbolay speaks Bolt, so APM database views key off `neo4j`. This describes
/// the protocol, not a claim about the implementation.
pub const DB_SYSTEM_NAME: &str = "db.system.name";

/// Value for [`DB_SYSTEM_NAME`].
pub const DB_SYSTEM_NEO4J: &str = "neo4j";

/// Every `turbolay.*` key defined above, for tests and for the redaction
/// layer's allowlist cross-check.
pub const ALL_TURBOLAY_KEYS: &[&str] = &[
    SCOPE,
    CELL_ID,
    NODE_ID,
    READ_EPOCH,
    COMMIT_EPOCH,
    GENERATION,
    BASE_SEQUENCE,
    EDGE_TYPE,
    QUERY_FINGERPRINT,
    QUERY_ACCESS_PATH,
    QUERY_OPTIMIZER_PASSES,
    QUERY_ROWS_ESTIMATED,
    QUERY_ROWS_RETURNED,
    QUERY_FULL_SCAN,
    KERNEL,
    WRITER_EPOCH,
    WRITER_RETRIES,
    WRITER_LAST_PROMOTED_BY,
    WRITER_LAST_PROMOTED_EPOCH,
    WRITER_LAST_PROMOTED_AT,
    CONSISTENCY,
    PLACEMENT_OWNERSHIP,
    PLACEMENT_STATE,
    PLACEMENT_PREVIOUS_STATE,
    PLACEMENT_LIVE_NODES,
    WRITER_REOPEN_DELAY_MS,
    WRITER_REOPEN_CAP_MS,
    CORRELATION_ID,
    CALLER_STEP,
    OUTCOME,
    SAMPLING_FORCE,
    SAMPLING_TAIL_KEEP,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_turbolay_key_is_namespaced() {
        for key in ALL_TURBOLAY_KEYS {
            assert!(
                key.starts_with("turbolay."),
                "{key} is missing the turbolay. namespace"
            );
        }
    }

    #[test]
    fn keys_are_unique() {
        let mut sorted = ALL_TURBOLAY_KEYS.to_vec();
        sorted.sort_unstable();
        let before = sorted.len();
        sorted.dedup();
        assert_eq!(before, sorted.len(), "duplicate key in ALL_TURBOLAY_KEYS");
    }

    /// A key that survives redaction is a key that reaches the exporter, so the
    /// registry and the denylist must not overlap. If a future attribute is
    /// named `…parameters` this test is the thing that catches it.
    #[test]
    fn no_registry_key_is_redacted() {
        for key in ALL_TURBOLAY_KEYS {
            assert!(
                !crate::redact::is_redacted(key),
                "{key} is in the registry but would be redacted before export"
            );
        }
    }
}
