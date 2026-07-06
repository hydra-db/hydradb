//! RFC 0017 Phase 0 observability spine: the `metrics` facade wiring, the
//! instrumented [`ObjectStore`] wrapper (§3.1), the write-path phase/latest-seq
//! metric registry (§3.2), and the invariant-counter taxonomy (§3.7).
//!
//! **Scope is Phase 0 only** (RFC 0017 §8): recording sites for code that
//! already exists in M1 — the object-store wrapper, the write fan-out, and
//! the merge operator's fail-closed panics. Query-phase timers, the
//! slow-query log, `debug: true`, the `m/obs_heartbeat` key, and the full
//! Phase-1 metric matrix are **out of scope** here; they land with the read
//! path (M2) and the HTTP plane (M3) respectively (RFC 0017 §8's phasing
//! table).
//!
//! Every recording site below calls straight into the `metrics` facade
//! (`metrics::counter!`/`histogram!`/`gauge!`). The facade is a no-op unless a
//! recorder is installed (`metrics::set_global_recorder`/`with_local_recorder`)
//! — turbolay does not install one; that is an exporter concern, deliberately
//! deferred to the HTTP/admin plane (M3, RFC 0017 §3.8, Decision 2). Nothing
//! in this module can fail a write or a read: telemetry is loseable by design
//! (RFC 0017 §7) and never participates in any `WriteBatch`.
//!
//! # Cardinality rules (RFC 0017 §6, binding)
//!
//! Every label used anywhere in this module is a **bounded, closed enum**
//! (`op`, `outcome`, `direction`, `phase`, `kind`) whose values are the
//! `&'static str` constants defined alongside each metric. **Never** put a
//! UID, xid, seq, token, object path, or property value in a label — that is
//! exactly the unbounded-cardinality mistake RFC 0017 §6 forbids. Anything
//! that unbounded belongs in a structured log event, not a metric series.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use common::object_store::path::Path;
use common::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use futures::stream::{BoxStream, Stream, StreamExt};

// ===========================================================================
// P0 — object store (RFC 0017 §3.1)
// ===========================================================================

/// Metric names and label constants for the instrumented [`ObjectStore`]
/// wrapper ([`InstrumentedObjectStore`]).
pub mod objstore {
    use std::time::Duration;

    /// Histogram of object-store request latency. Labels: `{op}`.
    pub const REQUEST_DURATION_SECONDS: &str = "turbolay_objstore_request_duration_seconds";
    /// Counter of object-store requests. Labels: `{op, outcome}`.
    pub const REQUESTS_TOTAL: &str = "turbolay_objstore_requests_total";
    /// Counter of object-store bytes transferred. Labels: `{op, direction}`.
    pub const BYTES_TOTAL: &str = "turbolay_objstore_bytes_total";

    /// `op` label values (RFC 0017 §3.1's closed enum).
    pub const OP_GET: &str = "get";
    pub const OP_PUT: &str = "put";
    pub const OP_DELETE: &str = "delete";
    pub const OP_LIST: &str = "list";
    pub const OP_HEAD: &str = "head";

    /// `outcome` label values.
    pub const OUTCOME_OK: &str = "ok";
    pub const OUTCOME_ERROR: &str = "error";

    /// `direction` label values.
    pub const DIRECTION_TX: &str = "tx";
    pub const DIRECTION_RX: &str = "rx";

    /// Records one completed request: a duration sample plus one
    /// `requests_total{op,outcome}` increment.
    pub(crate) fn record_request(op: &'static str, outcome: &'static str, elapsed: Duration) {
        metrics::histogram!(REQUEST_DURATION_SECONDS, "op" => op).record(elapsed.as_secs_f64());
        metrics::counter!(REQUESTS_TOTAL, "op" => op, "outcome" => outcome).increment(1);
    }

    /// Records a byte count for one direction of one op (put=tx, get=rx).
    pub(crate) fn record_bytes(op: &'static str, direction: &'static str, bytes: u64) {
        metrics::counter!(BYTES_TOTAL, "op" => op, "direction" => direction).increment(bytes);
    }
}

/// Wraps an [`ObjectStore`] handle and counts/times/byte-meters every request
/// through it (RFC 0017 §3.1, Decision 3). turbolay constructs the
/// `object_store` handle it hands to SlateDB (via `common::StorageBuilder`,
/// see [`crate::storage::GraphStorage::open`]), so wrapping it here requires
/// no SlateDB fork and no upstream patch (D1-safe) — every S3
/// GET/PUT/DELETE/LIST/HEAD SlateDB itself issues against this handle is
/// counted, regardless of what SlateDB's own stats surface exposes.
///
/// Delegates every method to `inner`; the primary IO ops
/// (`put_opts`/`get_opts`/`delete_stream`/`list`/`list_with_delimiter`) are
/// timed and counted. `get_ranges`/`get_range`/`head`/`delete` are not
/// overridden directly: `object_store`'s own default/`ObjectStoreExt`
/// blanket implementations decompose every one of them into calls to
/// `get_opts`/`delete_stream` on `self`, so they are instrumented for free
/// through those two seams (and, in `get_opts`'s case, correctly reflect
/// however many *coalesced* underlying requests `get_ranges` actually made —
/// see `object_store::util::coalesce_ranges` — rather than one sample per
/// caller-requested range). `copy_opts`/`put_multipart_opts` delegate plainly:
/// `copy` has no slot in RFC 0017 §3.1's closed `op` enum
/// (`get|put|delete|list|head`), and inventing one would violate the
/// cardinality rule (§6) this module documents.
pub struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl InstrumentedObjectStore {
    /// Wraps `inner`. Every request that reaches `inner` from this point on
    /// is observed.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for InstrumentedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstrumentedObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for InstrumentedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InstrumentedObjectStore({})", self.inner)
    }
}

fn outcome_of<T>(result: &ObjectStoreResult<T>) -> &'static str {
    match result {
        Ok(_) => objstore::OUTCOME_OK,
        Err(_) => objstore::OUTCOME_ERROR,
    }
}

/// A stream wrapper that emits exactly one [`objstore::record_request`] call
/// per wrapped `list`/`delete_stream` invocation — covering the whole
/// bulk/streaming call, not one sample per yielded item (a `list()` page can
/// yield thousands of objects for one underlying LIST request; counting per
/// item would grossly overstate request volume, exactly the kind of
/// dishonest counter RFC 0017 §6/§7 warns against). Reports on natural
/// end-of-stream, or on `Drop` if the caller stops polling before then, so a
/// partially-consumed stream still yields exactly one observation rather than
/// zero or several.
struct InstrumentedStream<S> {
    inner: S,
    start: Instant,
    op: &'static str,
    reported: bool,
    saw_error: bool,
}

impl<S> InstrumentedStream<S> {
    fn new(inner: S, op: &'static str) -> Self {
        Self {
            inner,
            start: Instant::now(),
            op,
            reported: false,
            saw_error: false,
        }
    }

    fn report_once(&mut self) {
        if !self.reported {
            self.reported = true;
            let outcome = if self.saw_error {
                objstore::OUTCOME_ERROR
            } else {
                objstore::OUTCOME_OK
            };
            objstore::record_request(self.op, outcome, self.start.elapsed());
        }
    }
}

impl<S> Drop for InstrumentedStream<S> {
    fn drop(&mut self) {
        self.report_once();
    }
}

impl<S, T> Stream for InstrumentedStream<S>
where
    S: Stream<Item = ObjectStoreResult<T>> + Unpin,
{
    type Item = ObjectStoreResult<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_next(cx);
        match &poll {
            Poll::Ready(Some(Err(_))) => this.saw_error = true,
            Poll::Ready(None) => this.report_once(),
            _ => {}
        }
        poll
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let len = payload.content_length() as u64;
        let start = Instant::now();
        let result = self.inner.put_opts(location, payload, opts).await;
        objstore::record_request(objstore::OP_PUT, outcome_of(&result), start.elapsed());
        if result.is_ok() {
            objstore::record_bytes(objstore::OP_PUT, objstore::DIRECTION_TX, len);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        // Plain delegate: this call only opens an upload session (no bytes
        // transferred yet — the per-part bytes aren't visible at this seam),
        // and turbolay's M1 write path never multipart-uploads.
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let is_head = options.head;
        let op = if is_head {
            objstore::OP_HEAD
        } else {
            objstore::OP_GET
        };
        let start = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        objstore::record_request(op, outcome_of(&result), start.elapsed());
        if let Ok(get_result) = &result {
            // A HEAD request transfers no body; only meter bytes for a real GET.
            if !is_head {
                let n = get_result.range.end.saturating_sub(get_result.range.start);
                objstore::record_bytes(objstore::OP_GET, objstore::DIRECTION_RX, n);
            }
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        InstrumentedStream::new(self.inner.delete_stream(locations), objstore::OP_DELETE).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        InstrumentedStream::new(self.inner.list(prefix), objstore::OP_LIST).boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        InstrumentedStream::new(
            self.inner.list_with_offset(prefix, offset),
            objstore::OP_LIST,
        )
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let start = Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        objstore::record_request(objstore::OP_LIST, outcome_of(&result), start.elapsed());
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        // Plain delegate — see the struct doc: `copy` has no slot in RFC
        // 0017 §3.1's closed `op` enum.
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        // Plain delegate — see the struct doc: `rename` has no slot in RFC
        // 0017 §3.1's closed `op` enum.
        self.inner.rename_opts(from, to, options).await
    }
}

// ===========================================================================
// P2 — write fan-out (RFC 0017 §3.2)
// ===========================================================================

/// Metric names and label constants for the write fan-out
/// ([`crate::write::Writer`]'s phase timers, `turbolay_latest_seq`, and the
/// optional per-request outcome counter).
pub mod write {
    use std::time::Duration;

    /// Histogram of write fan-out phase durations. Labels: `{phase}`.
    pub const PHASE_DURATION_SECONDS: &str = "turbolay_write_phase_duration_seconds";
    /// Gauge: the highest logical seq durably committed so far.
    pub const LATEST_SEQ: &str = "turbolay_latest_seq";
    /// Counter of write requests. Labels: `{op, outcome}`.
    pub const REQUESTS_TOTAL: &str = "turbolay_write_requests_total";

    /// `phase` label values — RFC 0004's fan-out steps, verbatim.
    pub const PHASE_ENCODE_NODE: &str = "encode_node";
    pub const PHASE_ENCODE_OUT: &str = "encode_out";
    pub const PHASE_ENCODE_IN: &str = "encode_in";
    /// M1 has no index framework yet (RFC 0006 is M2) — this phase is
    /// recorded at a fixed, genuinely-near-zero timing (there is no work to
    /// time) so the taxonomy is stable and dashboards don't show "no data"
    /// for a phase that will carry real weight once indexing lands.
    pub const PHASE_INDEX_FANOUT: &str = "index_fanout";
    /// The durable `apply_with_options` `WriteBatch` PUT — expected to
    /// dominate every other phase (RFC 0017 §4).
    pub const PHASE_BATCH_COMMIT: &str = "batch_commit";
    /// The `Meta[latest_seq]` bump — bundled into the same atomic batch as
    /// everything else in M1, so this times only the encode+push of that one
    /// `RecordOp`, not a separate round trip.
    pub const PHASE_LATEST_SEQ: &str = "latest_seq";

    /// `op` label values.
    pub const OP_UPSERT_NODE: &str = "upsert_node";
    pub const OP_UPSERT_EDGE: &str = "upsert_edge";
    pub const OP_DELETE: &str = "delete";

    /// `outcome` label values.
    pub const OUTCOME_OK: &str = "ok";
    pub const OUTCOME_OVERSIZE_NODE: &str = "oversize_node";
    pub const OUTCOME_ERROR: &str = "error";

    /// Records one phase-duration sample.
    pub(crate) fn record_phase(phase: &'static str, elapsed: Duration) {
        metrics::histogram!(PHASE_DURATION_SECONDS, "phase" => phase).record(elapsed.as_secs_f64());
    }

    /// Sets the `turbolay_latest_seq` gauge to `seq` (only called after a
    /// commit durably succeeds).
    pub(crate) fn set_latest_seq(seq: u64) {
        metrics::gauge!(LATEST_SEQ).set(seq as f64);
    }

    /// Records one write request's terminal outcome.
    pub(crate) fn record_request(op: &'static str, outcome: &'static str) {
        metrics::counter!(REQUESTS_TOTAL, "op" => op, "outcome" => outcome).increment(1);
    }
}

// ===========================================================================
// Invariant counters (RFC 0017 §3.7) — HONEST subset
// ===========================================================================

/// `turbolay_invariant_violations_total{kind}` — the should-be-zero counter
/// converting RFC series "structurally impossible" claims into monitored
/// facts (RFC 0017 §3.7). Registers the **full** `kind` taxonomy from RFC
/// 0017 §3.7 (nine kinds); **only `MERGE_REJECTED` is wired** to a genuine M1
/// observation site (`src/merge.rs`'s fail-closed panics). Every other kind is
/// registered here as a documented constant — a dashboard querying it will
/// simply never see a sample until the corresponding M2+ checker/scan lands.
/// This is the honesty rule this workstream operates under: no fabricated
/// check, no new I/O added purely to make a counter move.
pub mod invariants {
    /// Counter of invariant violations. Labels: `{kind}`.
    pub const VIOLATIONS_TOTAL: &str = "turbolay_invariant_violations_total";

    /// An `EdgeOut` with no matching `EdgeIn` (or vice versa) — RFC 0004's
    /// "every edge written twice in one atomic batch" (D10) contract.
    ///
    /// **Unwired.** Detecting this needs a consistency *checker* that reads
    /// both projections and cross-checks them — there is no such read path
    /// in M1 (no query/index layer yet). Candidate M2 site: a
    /// `posting_ops`-level or query-planner-level cross-check.
    pub const PROJECTION_ASYMMETRY: &str = "projection_asymmetry";

    /// An index posting-list entry pointing at a UID whose node/edge no
    /// longer satisfies the indexed predicate — RFC 0006's index-consistency
    /// contract.
    ///
    /// **Unwired.** RFC 0006 (the index framework) is M2 scope; there is no
    /// index in M1 to have orphaned entries in.
    pub const ORPHANED_INDEX_ENTRY: &str = "orphaned_index_entry";

    /// A `Xid` lookup resolved to a UID with no corresponding node record, or
    /// a node whose `xid` doesn't round-trip — RFC 0004's `xid → uid`
    /// mapping-recovery contract (D5).
    ///
    /// **Unwired.** [`crate::write::Writer::lookup_uid`] deliberately does a
    /// cheap `Xid → uid` mapping read only and does *not* also read the node
    /// record back — adding that read purely to make this counter move would
    /// be exactly the kind of new-I/O-for-a-metric this workstream's honesty
    /// rule forbids. Candidate M2 site: a read-path/recovery consistency scan.
    pub const XID_UID_MISS: &str = "xid_uid_miss";

    /// A gap in the `Log[seq]` sequence between watermark and latest — RFC
    /// 0004's monotonic single-writer seq contract (D4).
    ///
    /// **Unwired.** [`crate::write::Writer::from_storage`] recovery reads
    /// `Meta[latest_seq]` only; it does not scan the changelog, so there is no
    /// site here without adding a changelog scan to writer-open purely to
    /// serve this counter. Candidate M2 site: a build-loop tick or explicit
    /// recovery-time changelog scan (RFC 0006).
    pub const CHANGELOG_GAP: &str = "changelog_gap";

    /// `m/wm/{id} > m/latest_seq` observed — RFC 0006's "never an
    /// overestimate" watermark contract.
    ///
    /// **Unwired.** No index/watermark exists in M1 (RFC 0006 is M2 scope).
    pub const WATERMARK_OVERSHOOT: &str = "watermark_overshoot";

    /// A posting stream yielded a non-strictly-ascending UID — RFC 0005's
    /// sorted-UID stream contract that intersection relies on.
    ///
    /// **Unwired.** No read/intersection path exists yet to observe posting
    /// order on (M2, RFC 0007). [`crate::posting`]'s own decode already
    /// trusts the sortedness invariant it is given; a debug-assert-style
    /// checker belongs at the point where postings are actually iterated for
    /// intersection, which does not exist in M1.
    pub const POSTING_ORDER_VIOLATION: &str = "posting_order_violation";

    /// The merge operator fired its reject arm (RFC 0003 dispatch table) —
    /// some path issued a merge to a non-merge-associative key (D11).
    ///
    /// **Wired.** [`crate::merge::GraphMergeOperator`]'s three fail-closed
    /// `panic!` arms (the non-associative `RecordType` arm, the
    /// `MetaKind::Scalar` arm, and the Split-manifest arm inside
    /// `merge_posting_union`) each increment this counter immediately before
    /// panicking — the one genuine M1 observation site for this taxonomy.
    pub const MERGE_REJECTED: &str = "merge_rejected";

    /// RFC 0004's defensive recovery point-read failed on writer open (the
    /// writer refuses to start).
    ///
    /// **Unwired.** No such named "defensive recovery point-read" exists in
    /// current M1 code or in RFC 0004 as written — [`crate::write::Writer`]'s
    /// recovery-on-open is "reload the in-memory accelerators from durable
    /// storage," not a repair pass with a distinguished probe step (see
    /// `write.rs`'s own module doc, "Recovery on open (M1 D6)"). Registered
    /// for when that probe is specified and added.
    pub const RECOVERY_PROBE_FAILED: &str = "recovery_probe_failed";

    /// Any versioned-envelope or layout decode failure (node / posting /
    /// changelog / index) — corruption or a format bug, never
    /// workload-dependent.
    ///
    /// **Unwired.** `src/value.rs` and `src/posting.rs` have many individual
    /// decode-error return sites (`Error::Encoding`), but centralizing them
    /// (e.g. at the `Error::encoding` constructor) would also catch
    /// *encode-side input validation* errors that share the same
    /// constructor (e.g. `write.rs`'s `xid_utf8` rejecting a non-UTF-8 client
    /// xid) — those are workload-dependent, not "should be zero," and
    /// counting them here would violate this kind's own definition. Properly
    /// wiring this needs per-call-site discrimination between "corrupt
    /// stored/operand bytes" and "bad caller input," which is more than a
    /// Phase-0 recording-site change; left for M2.
    pub const DECODE_ERROR: &str = "decode_error";

    /// Increments `turbolay_invariant_violations_total{kind}`.
    pub fn record(kind: &'static str) {
        metrics::counter!(VIOLATIONS_TOTAL, "kind" => kind).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use common::object_store::ObjectStoreExt;
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ForwardingProbeStore {
        inner: common::object_store::memory::InMemory,
        list_calls: AtomicUsize,
        list_with_offset_calls: AtomicUsize,
        copy_calls: AtomicUsize,
        rename_calls: AtomicUsize,
    }

    impl Default for ForwardingProbeStore {
        fn default() -> Self {
            Self {
                inner: common::object_store::memory::InMemory::new(),
                list_calls: AtomicUsize::new(0),
                list_with_offset_calls: AtomicUsize::new(0),
                copy_calls: AtomicUsize::new(0),
                rename_calls: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Debug for ForwardingProbeStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ForwardingProbeStore").finish()
        }
    }

    impl fmt::Display for ForwardingProbeStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ForwardingProbeStore")
        }
    }

    fn not_implemented(operation: &str) -> common::object_store::Error {
        common::object_store::Error::NotImplemented {
            operation: operation.to_string(),
            implementer: "ForwardingProbeStore".to_string(),
        }
    }

    #[async_trait]
    impl ObjectStore for ForwardingProbeStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            futures::stream::once(async { Err(not_implemented("list fallback used")) }).boxed()
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.list_with_offset_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            _from: &Path,
            _to: &Path,
            _options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.copy_calls.fetch_add(1, Ordering::SeqCst);
            Err(not_implemented("copy fallback used"))
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> ObjectStoreResult<()> {
            self.rename_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.rename_opts(from, to, options).await
        }
    }

    // -- Delegation round-trip (no recorder needed) ----------------------

    #[tokio::test]
    async fn should_round_trip_put_then_get_through_the_delegate() {
        // given — a wrapped in-memory store
        let inner: Arc<dyn ObjectStore> = Arc::new(common::object_store::memory::InMemory::new());
        let wrapped = InstrumentedObjectStore::new(inner);
        let path = Path::from("obs-test/round-trip");

        // when — put then get through the wrapper
        wrapped
            .put(&path, Bytes::from_static(b"hello-instrumented").into())
            .await
            .expect("put through wrapper");
        let got = wrapped
            .get(&path)
            .await
            .expect("get through wrapper")
            .bytes()
            .await
            .expect("read body");

        // then — delegation is transparent: bytes round-trip exactly
        assert_eq!(got, Bytes::from_static(b"hello-instrumented"));
    }

    #[tokio::test]
    async fn should_report_not_found_for_a_missing_key_through_the_delegate() {
        let inner: Arc<dyn ObjectStore> = Arc::new(common::object_store::memory::InMemory::new());
        let wrapped = InstrumentedObjectStore::new(inner);
        let path = Path::from("obs-test/missing");

        let err = wrapped.get(&path).await.unwrap_err();
        assert!(
            matches!(err, common::object_store::Error::NotFound { .. }),
            "expected NotFound to pass through the wrapper unchanged, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_forward_offset_list_and_rename_without_trait_default_fallbacks() {
        let inner = Arc::new(ForwardingProbeStore::default());
        let wrapped = InstrumentedObjectStore::new(inner.clone());
        let prefix = Path::from("obs-test/offset");
        let first = Path::from("obs-test/offset/a");
        let second = Path::from("obs-test/offset/b");
        let renamed = Path::from("obs-test/offset/renamed");

        wrapped
            .put(&first, Bytes::from_static(b"a").into())
            .await
            .unwrap();
        wrapped
            .put(&second, Bytes::from_static(b"b").into())
            .await
            .unwrap();

        let listed: Vec<_> = wrapped
            .list_with_offset(Some(&prefix), &first)
            .try_collect()
            .await
            .expect("wrapper must forward list_with_offset directly");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, second);
        assert_eq!(inner.list_with_offset_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            inner.list_calls.load(Ordering::SeqCst),
            0,
            "wrapper must not fall back to list()+client-side filtering"
        );

        wrapped
            .rename_opts(&second, &renamed, RenameOptions::default())
            .await
            .expect("wrapper must forward rename_opts directly");
        assert_eq!(inner.rename_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            inner.copy_calls.load(Ordering::SeqCst),
            0,
            "wrapper must not fall back to copy_opts()+delete"
        );
        let got = wrapped.get(&renamed).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, Bytes::from_static(b"b"));
    }

    // -- Metric values, via a tiny in-test `metrics::Recorder` -----------
    //
    // The `metrics` crate's `with_local_recorder` scopes a recorder to the
    // calling OS thread for the duration of a closure (no global-recorder
    // installation, so this cannot race or interfere with any other test in
    // the same process — global recorders can only ever be installed once
    // per process, which is not safe to do from a library's own test suite).
    // We drive the async code with a manually-built current-thread runtime so
    // every `.await` resolves on the very same thread the recorder is scoped
    // to. No new dependency: everything here is the `metrics` crate's own
    // public `Recorder`/`*Fn` traits (no `metrics-util`).

    /// One captured emission: `(metric_name, labels, value)`.
    type Emission = (String, Vec<(String, String)>, f64);

    #[derive(Clone, Default)]
    struct Events(Arc<std::sync::Mutex<Vec<Emission>>>);

    impl Events {
        fn saw(&self, name: &str, label: (&str, &str)) -> bool {
            self.0.lock().unwrap().iter().any(|(n, labels, _)| {
                n == name && labels.iter().any(|(k, v)| k == label.0 && v == label.1)
            })
        }
    }

    struct Sink {
        name: String,
        labels: Vec<(String, String)>,
        events: Events,
    }

    impl Sink {
        fn push(&self, value: f64) {
            self.events
                .0
                .lock()
                .unwrap()
                .push((self.name.clone(), self.labels.clone(), value));
        }
    }

    impl metrics::CounterFn for Sink {
        fn increment(&self, value: u64) {
            self.push(value as f64);
        }
        fn absolute(&self, value: u64) {
            self.push(value as f64);
        }
    }

    impl metrics::GaugeFn for Sink {
        fn increment(&self, value: f64) {
            self.push(value);
        }
        fn decrement(&self, value: f64) {
            self.push(value);
        }
        fn set(&self, value: f64) {
            self.push(value);
        }
    }

    impl metrics::HistogramFn for Sink {
        fn record(&self, value: f64) {
            self.push(value);
        }
    }

    struct CapturingRecorder {
        events: Events,
    }

    fn labels_of(key: &metrics::Key) -> Vec<(String, String)> {
        key.labels()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect()
    }

    impl metrics::Recorder for CapturingRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::from_arc(Arc::new(Sink {
                name: key.name().to_string(),
                labels: labels_of(key),
                events: self.events.clone(),
            }))
        }
        fn register_gauge(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            metrics::Gauge::from_arc(Arc::new(Sink {
                name: key.name().to_string(),
                labels: labels_of(key),
                events: self.events.clone(),
            }))
        }
        fn register_histogram(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::from_arc(Arc::new(Sink {
                name: key.name().to_string(),
                labels: labels_of(key),
                events: self.events.clone(),
            }))
        }
    }

    #[test]
    fn should_emit_objstore_request_and_byte_metrics_for_put_and_get() {
        // given — a capturing recorder scoped to this thread
        let events = Events::default();
        let recorder = CapturingRecorder {
            events: events.clone(),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // when — put then get through the wrapper, entirely inside the
        // local-recorder scope (single OS thread throughout, via `block_on`)
        metrics::with_local_recorder(&recorder, || {
            rt.block_on(async {
                let inner: Arc<dyn ObjectStore> =
                    Arc::new(common::object_store::memory::InMemory::new());
                let wrapped = InstrumentedObjectStore::new(inner);
                let path = Path::from("obs-test/metrics");
                wrapped
                    .put(&path, Bytes::from_static(b"12345").into())
                    .await
                    .unwrap();
                wrapped.get(&path).await.unwrap().bytes().await.unwrap();
            });
        });

        // then — both ops produced request-count, duration, and byte samples
        assert!(events.saw(objstore::REQUESTS_TOTAL, ("op", "put")));
        assert!(events.saw(objstore::REQUESTS_TOTAL, ("op", "get")));
        assert!(events.saw(objstore::REQUEST_DURATION_SECONDS, ("op", "put")));
        assert!(events.saw(objstore::REQUEST_DURATION_SECONDS, ("op", "get")));
        assert!(events.saw(objstore::BYTES_TOTAL, ("direction", "tx")));
        assert!(events.saw(objstore::BYTES_TOTAL, ("direction", "rx")));
        assert!(events.saw(objstore::REQUESTS_TOTAL, ("outcome", "ok")));
    }

    #[test]
    fn should_emit_invariant_violation_counter_with_the_given_kind() {
        let events = Events::default();
        let recorder = CapturingRecorder {
            events: events.clone(),
        };

        metrics::with_local_recorder(&recorder, || {
            invariants::record(invariants::MERGE_REJECTED);
        });

        assert!(events.saw(invariants::VIOLATIONS_TOTAL, ("kind", "merge_rejected")));
    }
}
