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
//! Every key here is either a [`MetricLabel`] or is in [`SPAN_ONLY_KEYS`], and
//! the partition is enforced by a test rather than by this paragraph. Read
//! [`METRIC_LABELS`] for the list and the reasoning; what follows is the
//! intuition behind it.
//!
//! A key is a metric label when it is something you *group by* and its value
//! set is closed — [`KERNEL`], [`OUTCOME`], [`PLACEMENT_OWNERSHIP`],
//! [`PLACEMENT_STATE`] and [`QUERY_FULL_SCAN`] are enums or bools;
//! [`CELL_ID`] and [`EDGE_TYPE`] are bounded per node by deployment size and
//! by schema. Everything else is span-only, for one of three reasons:
//!
//! - **Unbounded by construction.** [`SCOPE`] grows with tenant count,
//!   [`CORRELATION_ID`] takes one value per request, [`CALLER_STEP`] is
//!   supplied by a process Turbolay does not control, and
//!   [`QUERY_FINGERPRINT`] is minted *before* validation, so an authenticated
//!   client can mint series directly. [`QUERY_ACCESS_PATH`] and
//!   [`QUERY_OPTIMIZER_PASSES`] are comma-joined sequences and therefore
//!   combinatorial — an earlier version of this comment called
//!   `QUERY_ACCESS_PATH` safe anywhere, which was the one sentence in this
//!   crate that could have talked somebody into an unbounded metric.
//! - **Monotonic.** Epochs, sequences, timestamps and [`GENERATION`] (a
//!   SHA-256 digest) take a new value on every write, tick or rebuild.
//! - **A measurement, not a key.** [`QUERY_ROWS_RETURNED`],
//!   [`PLACEMENT_LIVE_NODES`], [`WRITER_RETRIES`] and the two reopen delays
//!   are what an instrument *records*; keying by them is keying by the answer.
//!
//! [`NODE_ID`] is the one genuine judgement call. It is bounded by fleet size,
//! but it is already on every exported record as the `service.instance.id`
//! resource attribute, so as a metric dimension it would duplicate a resource
//! attribute and double the exposure to id churn on rescale. Span-only.
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

/// Bucket upper bound on an exported histogram family — the Prometheus `le`
/// convention, spelled the same way in the OTLP export.
///
/// This is the one registry key that is **never a span attribute**. It exists
/// because OpenTelemetry has no observable histogram and the Rust SDK has no
/// `MetricProducer`, so a histogram computed in the kernel and read from a
/// cached snapshot cannot reach OTLP as a histogram data point. It reaches it
/// as a family of observable counters, one series per bucket, keyed by this
/// label — which is what a Prometheus histogram already is, and over which
/// `histogram_quantile` works unchanged. See [`crate::meter`].
///
/// It is a registry key rather than a bare `"le"` at the one call site so that
/// the partition test below stays total: a [`MetricLabel`] built from a string
/// nobody added to the registry would otherwise escape the classification
/// entirely.
///
/// Bounded by the ladder length — 18 values, closed at compile time.
pub const LE: &str = "le";

/// An attribute key that is allowed to be a **metric dimension**.
///
/// The constructor is private to this module, so a `MetricLabel` can only name
/// a key that appears in [`METRIC_LABELS`] below. Every meter helper takes
/// `&[(MetricLabel, &str)]` rather than `&[KeyValue]`, which turns "do not put
/// `scope` on a metric" from a review comment into a type error.
///
/// The rule is structural for the same reason redaction is
/// ([`crate::redact`]), but by a different mechanism, and the difference is
/// worth knowing. Redaction is a *runtime* denylist because it defends against
/// field names invented anywhere in the kernel, which this crate cannot see.
/// Metric labels are attached by code this crate owns, so the compiler can
/// enforce it — and where the compiler can, a runtime filter is strictly worse:
/// it fails silently on a Tuesday instead of loudly at `cargo build`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct MetricLabel(&'static str);

impl MetricLabel {
    /// The dotted attribute key.
    pub const fn key(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for MetricLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// [`CELL_ID`] as a metric dimension. Bounded per node by `GRAPH_CELLS`.
pub const L_CELL_ID: MetricLabel = MetricLabel(CELL_ID);

/// [`EDGE_TYPE`] as a metric dimension. Bounded by the tenant's schema.
///
/// Note that the *product* `cell_id × edge_type` is what a per-shard series
/// costs: 8 cells and 12 edge types is 96 series per instrument per node.
/// Affordable for a counter, not for an 18-bucket histogram family.
pub const L_EDGE_TYPE: MetricLabel = MetricLabel(EDGE_TYPE);

/// [`KERNEL`] as a metric dimension. Three values — the sparse-kernel ladder.
pub const L_KERNEL: MetricLabel = MetricLabel(KERNEL);

/// [`OUTCOME`] as a metric dimension. Three values.
pub const L_OUTCOME: MetricLabel = MetricLabel(OUTCOME);

/// [`ERROR_CLASS`] as a metric dimension.
///
/// Ten reachable values, not the eleven the enum declares: `GraphError::class`
/// is an exhaustive match with no `other` arm and `ErrorClass::Other` is
/// constructed nowhere, so the eleventh exists in the type and cannot reach a
/// label. The distinction matters because ten is the number an operator
/// multiplies out when deciding whether a `{cell_id, edge_type, error_class}`
/// series is affordable.
pub const L_ERROR_CLASS: MetricLabel = MetricLabel(ERROR_CLASS);

/// [`PLACEMENT_OWNERSHIP`] as a metric dimension. Four values.
pub const L_PLACEMENT_OWNERSHIP: MetricLabel = MetricLabel(PLACEMENT_OWNERSHIP);

/// [`PLACEMENT_STATE`] as a metric dimension. Three values, closed by
/// `crates/placement/src/liveness.rs` and pinned by a test there.
///
/// This one earns label status by being *asked for* as a metric: "how many
/// instances are in `shed` right now" is a count of distinct instances by
/// state, which a metric can answer only if the state is a dimension.
pub const L_PLACEMENT_STATE: MetricLabel = MetricLabel(PLACEMENT_STATE);

/// [`PLACEMENT_PREVIOUS_STATE`] as a metric dimension. Same closed vocabulary
/// as [`L_PLACEMENT_STATE`], so a transition counter can carry both ends.
pub const L_PLACEMENT_PREVIOUS_STATE: MetricLabel = MetricLabel(PLACEMENT_PREVIOUS_STATE);

/// [`QUERY_FULL_SCAN`] as a metric dimension. Two values, and the single most
/// actionable one in the set: a rate of full-scanning queries is a leading
/// indicator, where the elapsed time is a lagging one.
pub const L_QUERY_FULL_SCAN: MetricLabel = MetricLabel(QUERY_FULL_SCAN);

/// [`DB_SYSTEM_NAME`] as a metric dimension. Exactly one value.
///
/// It is here as a consequence of naming the two metrics that have a genuine
/// semantic convention `db.*`: this is the attribute a vendor's database view
/// keys off, so putting `db.*` names on the wire and then omitting it would
/// pay the cost of the naming split and collect none of the benefit.
pub const L_DB_SYSTEM_NAME: MetricLabel = MetricLabel(DB_SYSTEM_NAME);

/// [`LE`] as a metric dimension — bucket bounds on an exported histogram
/// family. Never a span attribute.
pub const L_LE: MetricLabel = MetricLabel(LE);

/// Every key that may be a metric dimension. The other half of
/// [`SPAN_ONLY_KEYS`]; together they partition [`ALL_REGISTRY_KEYS`].
pub const METRIC_LABELS: &[MetricLabel] = &[
    L_CELL_ID,
    L_EDGE_TYPE,
    L_KERNEL,
    L_OUTCOME,
    L_ERROR_CLASS,
    L_PLACEMENT_OWNERSHIP,
    L_PLACEMENT_STATE,
    L_PLACEMENT_PREVIOUS_STATE,
    L_QUERY_FULL_SCAN,
    L_DB_SYSTEM_NAME,
    L_LE,
];

/// Every key that must stay on spans and logs and must never become a metric
/// dimension. See the cardinality note at the top of the module for the three
/// reasons a key lands here.
pub const SPAN_ONLY_KEYS: &[&str] = &[
    SCOPE,
    NODE_ID,
    READ_EPOCH,
    COMMIT_EPOCH,
    GENERATION,
    BASE_SEQUENCE,
    QUERY_FINGERPRINT,
    QUERY_ACCESS_PATH,
    QUERY_OPTIMIZER_PASSES,
    QUERY_ROWS_ESTIMATED,
    QUERY_ROWS_RETURNED,
    WRITER_EPOCH,
    WRITER_RETRIES,
    WRITER_LAST_PROMOTED_BY,
    WRITER_LAST_PROMOTED_EPOCH,
    WRITER_LAST_PROMOTED_AT,
    CONSISTENCY,
    PLACEMENT_LIVE_NODES,
    WRITER_REOPEN_DELAY_MS,
    WRITER_REOPEN_CAP_MS,
    CORRELATION_ID,
    CALLER_STEP,
    SAMPLING_FORCE,
    SAMPLING_TAIL_KEEP,
];

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

/// Every key this crate defines, `turbolay.`-namespaced or not.
///
/// [`ALL_TURBOLAY_KEYS`] stays the namespaced subset, because that is what the
/// namespace test and the redaction cross-check are about. This is the superset
/// the classification tests iterate, and the distinction is not pedantry: the
/// three keys that are *not* `turbolay.`-namespaced are [`ERROR_CLASS`],
/// [`DB_SYSTEM_NAME`] and [`LE`] — one of which is first on the safe-label list
/// and all three of which a test over `ALL_TURBOLAY_KEYS` would classify
/// vacuously, passing while checking nothing.
pub const ALL_REGISTRY_KEYS: &[&str] = &[
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
    ERROR_CLASS,
    DB_SYSTEM_NAME,
    LE,
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
        for key in ALL_REGISTRY_KEYS {
            assert!(
                !crate::redact::is_redacted(key),
                "{key} is in the registry but would be redacted before export"
            );
        }
    }

    /// The superset must actually be one. Cheap, and it is what keeps a key
    /// added to `ALL_TURBOLAY_KEYS` alone from skipping the classification
    /// below.
    #[test]
    fn the_registry_contains_every_turbolay_key() {
        for key in ALL_TURBOLAY_KEYS {
            assert!(
                ALL_REGISTRY_KEYS.contains(key),
                "{key} is in ALL_TURBOLAY_KEYS but not in ALL_REGISTRY_KEYS"
            );
        }
        assert_eq!(
            ALL_REGISTRY_KEYS.len(),
            ALL_TURBOLAY_KEYS.len() + 3,
            "ALL_REGISTRY_KEYS is ALL_TURBOLAY_KEYS plus error.class, \
             db.system.name and le — no more and no less"
        );
    }

    #[test]
    fn registry_keys_are_unique() {
        let mut sorted = ALL_REGISTRY_KEYS.to_vec();
        sorted.sort_unstable();
        let before = sorted.len();
        sorted.dedup();
        assert_eq!(before, sorted.len(), "duplicate key in ALL_REGISTRY_KEYS");
    }

    /// The point of the whole exercise: a new attribute cannot be added to the
    /// registry without deciding, in the same commit, whether it may be a
    /// metric dimension. Neither list can gain a member without the other being
    /// considered.
    #[test]
    fn every_registry_key_is_classified_exactly_once() {
        for key in ALL_REGISTRY_KEYS {
            let is_label = METRIC_LABELS.iter().any(|label| label.key() == *key);
            let is_span_only = SPAN_ONLY_KEYS.contains(key);
            assert!(
                is_label ^ is_span_only,
                "{key} is in neither METRIC_LABELS nor SPAN_ONLY_KEYS, or in both"
            );
        }
    }

    /// The complement of the partition test, and the one that catches the
    /// mistake `error.class` was already sitting in: a `MetricLabel` built from
    /// a string nobody added to the registry would otherwise escape the
    /// classification entirely, because the partition test iterates the
    /// registry and would never see it.
    #[test]
    fn every_metric_label_is_a_registry_key() {
        for label in METRIC_LABELS {
            let key = label.key();
            assert!(
                ALL_REGISTRY_KEYS.contains(&key),
                "{key} is a MetricLabel but is not in ALL_REGISTRY_KEYS"
            );
        }
    }

    #[test]
    fn every_span_only_key_is_a_registry_key() {
        for key in SPAN_ONLY_KEYS {
            assert!(
                ALL_REGISTRY_KEYS.contains(key),
                "{key} is in SPAN_ONLY_KEYS but is not in ALL_REGISTRY_KEYS"
            );
        }
    }

    /// The three keys the classification is most likely to get wrong, asserted
    /// by name rather than by rule. `scope` and `correlation_id` are the two
    /// unbounded keys the type exists to keep off metrics; `error.class` is the
    /// non-namespaced one an `ALL_TURBOLAY_KEYS`-shaped test cannot see.
    #[test]
    fn the_load_bearing_classifications_are_the_expected_ones() {
        assert!(SPAN_ONLY_KEYS.contains(&SCOPE));
        assert!(SPAN_ONLY_KEYS.contains(&CORRELATION_ID));
        assert!(SPAN_ONLY_KEYS.contains(&QUERY_FINGERPRINT));
        assert!(METRIC_LABELS.iter().any(|l| l.key() == ERROR_CLASS));
    }

    #[test]
    fn metric_labels_are_unique() {
        let mut sorted: Vec<&str> = METRIC_LABELS.iter().map(|label| label.key()).collect();
        sorted.sort_unstable();
        let before = sorted.len();
        sorted.dedup();
        assert_eq!(before, sorted.len(), "duplicate key in METRIC_LABELS");
    }
}
