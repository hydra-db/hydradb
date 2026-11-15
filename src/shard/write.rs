use super::*;
use futures::{stream, FutureExt as _, StreamExt as _, TryStreamExt as _};
pub(crate) use hydradb_graph_slate::write::*;
use tracing::Instrument as _;

/// Largest relationship import whose guarded reads are warmed before the
/// writer permit. Matches the cached-read limit (`MAX_CACHED_REMOTE_ITEMS`), so
/// a bulk import cannot sweep the block cache.
const RELATIONSHIP_IMPORT_WARM_MAX_ROWS: usize = 1_024;
/// Entries read per identity-scan prefix when warming; enough to admit its
/// blocks without walking a pathological number of parallel relationships.
#[cfg(feature = "opencypher")]
const RELATIONSHIP_IMPORT_WARM_MAX_SCAN_ENTRIES: usize = 8;

/// Reads a warmed relationship import may make before the writer permit.
///
/// The row gate keeps a bulk import out of the cache entirely; this bounds what
/// a batch inside that gate can admit, which the row count does not: one row
/// costs up to two endpoint keys, the direct edge, two degree counters, the
/// relationship counter, the merge pointer, and then either the record that
/// pointer names or a bounded property-index scan. A batch that exhausts this
/// stops warming and the guarded transaction reads the rest as it always has.
const RELATIONSHIP_IMPORT_WARM_MAX_READS: usize = 4_096;
const VERTEX_DELETE_READ_CONCURRENCY: usize = 16;
const METADATA_NOOP_PREFLIGHT_MAX_ITEMS: usize = 32;
static DELETE_PREPARATION_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);
const DELETE_PREPARATION_MAX_VERTICES: usize = 4096;
const DELETE_PREPARATION_BYTES: u64 = 8 * 1024 * 1024;
const DELETE_APPLY_WARM_MAX_KEYS: usize = 1024;
/// Reads a relationship delete may make warming its guarded reads before the
/// writer permit: the same bound as the vertex-delete apply warm, shared by the
/// point keys and the survivor-check prefixes.
#[cfg(feature = "opencypher")]
const RELATIONSHIP_DELETE_WARM_MAX_READS: usize = DELETE_APPLY_WARM_MAX_KEYS;

/// What [`GraphShard::set_property_on_existing_vertices`] did.
#[cfg(feature = "opencypher")]
pub(crate) struct WriteBackOutcome {
    pub(crate) written: u64,
    pub(crate) skipped: u64,
    /// The written scores in index order, the copy the statistics counted.
    pub(crate) ordered: crate::engine::IndexOrderedFloats,
    /// Vertices outside the run whose value the new generation carries.
    pub(crate) carried: u64,
    /// Older-generation index entries the carry walked, and how long it took.
    pub(crate) carry_scanned: u64,
    pub(crate) carry_millis: u64,
}

/// What a write-back's carry-forward walk found
/// ([`GraphShard::carry_forward_write_back_entries`]).
#[cfg(feature = "opencypher")]
#[derive(Default)]
struct CarryForward {
    values: crate::engine::CarriedIndexValues,
    carried: u64,
    scanned: u64,
    millis: u64,
}

/// Older-generation index entries a carry-forward walks between two budget
/// checks.
#[cfg(feature = "opencypher")]
const CARRY_FORWARD_CHECK_STRIDE: u64 = 4_096;

/// A detached sweep's entry in the shard's pending sweep targets, from the
/// spawn until the sweep takes its target. Dropped untaken, by a sweep that
/// failed or was aborted before it took its target, it clears the entry, so
/// the property's next write-back spawns a sweep rather than raising the
/// target of one that will never run.
#[cfg(feature = "opencypher")]
struct PendingWriteBackSweep {
    shard: std::sync::Arc<GraphShard>,
    key: Option<(String, String)>,
    published: StorageSequence,
}

#[cfg(feature = "opencypher")]
impl PendingWriteBackSweep {
    /// The newest generation published for the property while this sweep
    /// was pending. From here on the property has no pending sweep, and its
    /// next write-back spawns one.
    fn take(&mut self) -> StorageSequence {
        let Some(key) = self.key.take() else {
            return self.published;
        };
        let target = self
            .shard
            .write_back_sweep_targets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&key);
        target.unwrap_or(self.published).max(self.published)
    }
}

#[cfg(feature = "opencypher")]
impl Drop for PendingWriteBackSweep {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.shard
                .write_back_sweep_targets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&key);
        }
    }
}

/// The span of one sweep of superseded write-back index generations. Its
/// `deletes`, `batches`, `waited_ms`, `floor` and `scanned` fields are
/// recorded by the sweep at its end: the time it spent waiting out
/// write-backs, the generation its scan opened at, and the keys the scan
/// yielded. A
/// `root` span has no parent, for a sweep detached from the request that
/// published the generation; otherwise the span hangs under the current one.
#[cfg(feature = "opencypher")]
fn write_back_sweep_span(cell_id: &str, root: bool) -> tracing::Span {
    if root {
        tracing::info_span!(
            parent: None,
            "gds.write_back.sweep",
            hydradb.cell_id = %cell_id,
            hydradb.write.deletes = tracing::field::Empty,
            hydradb.write.batches = tracing::field::Empty,
            hydradb.write.waited_ms = tracing::field::Empty,
            hydradb.write.floor = tracing::field::Empty,
            hydradb.write.scanned = tracing::field::Empty,
        )
    } else {
        tracing::info_span!(
            "gds.write_back.sweep",
            hydradb.cell_id = %cell_id,
            hydradb.write.deletes = tracing::field::Empty,
            hydradb.write.batches = tracing::field::Empty,
            hydradb.write.waited_ms = tracing::field::Empty,
            hydradb.write.floor = tracing::field::Empty,
            hydradb.write.scanned = tracing::field::Empty,
        )
    }
}
/// Most relationship deletions a batch may carry and still be warmed. Above it
/// the warm is skipped before a key is built, so a bulk delete cannot spend its
/// preparation assembling keys the read budget would never reach. Matches the
/// import warm's row gate. Replay guards do not count: each is one point key,
/// so they are capped at the read budget instead of gating the batch.
#[cfg(feature = "opencypher")]
const RELATIONSHIP_DELETE_WARM_MAX_ROWS: usize = RELATIONSHIP_IMPORT_WARM_MAX_ROWS;
/// Survivor checks in flight at once inside the guarded relationship delete.
#[cfg(feature = "opencypher")]
const RELATIONSHIP_DELETE_SURVIVOR_CONCURRENCY: usize = 16;

async fn run_cancellable_delete_preparation<T>(preparation: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    match crate::QueryCancellationToken::current() {
        Some(token) => tokio::select! {
            biased;
            _ = token.cancelled() => Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                elapsed_ms: 0,
                limit_ms: 0,
            }),
            result = preparation => result,
        },
        None => preparation.await,
    }
}

/// Stamp a failure onto the span that raised it.
///
/// §6 of the telemetry crate design: the innermost span is
/// the one that says *why*. A `contention` class on `storage.commit` is a
/// conflicting transaction; the same class at the root is only "a write failed".
fn record_error_class(error: &GraphError) {
    tracing::Span::current().record("error.class", error.class());
}

/// One `shard.write_txn` span per mutation, spanning the whole retry loop.
///
/// The span sits on the retry loop rather than on the individual `*_txn` call
/// the plan's tree names, because `hydradb.writer.retries` only exists in the
/// loop: a span per attempt would be a span *per retry*, with no single span
/// carrying the count and nothing for a dashboard to group on.
/// `GraphOperationalMetricsSnapshot` already counts `write_attempts`,
/// `write_commits` and `write_retries` in aggregate — what was missing is
/// attribution to a specific mutation, and the loop is where that lives.
/// The `storage.txn` span for one relationship import attempt.
///
/// `import_relationships_batch_txn_locked` records its phase timings on
/// `tracing::Span::current()`, which is this span, not `shard.write_txn`. A
/// field a span did not declare is silently dropped, so while this span was a
/// bare `storage.txn` none of the `hydradb.relimport.*` fields ever reached a
/// trace. They are declared here, where they are recorded. (The same timings
/// also feed the `hydradb.shard.relationship_import.*` counters, which were
/// unaffected.)
fn relationship_import_txn_span(operation: &'static str, relationships: usize) -> tracing::Span {
    tracing::info_span!(
        "storage.txn",
        hydradb.write.operation = operation,
        hydradb.relimport.relationships = relationships as u64,
        hydradb.relimport.endpoint_check_us = tracing::field::Empty,
        hydradb.relimport.identity_scan_us = tracing::field::Empty,
        hydradb.relimport.identity_pointer_hits = tracing::field::Empty,
        hydradb.relimport.identity_pointer_misses = tracing::field::Empty,
        hydradb.relimport.record_read_us = tracing::field::Empty,
        hydradb.relimport.structural_check_us = tracing::field::Empty,
        hydradb.relimport.segment_scans = tracing::field::Empty,
        hydradb.relimport.segment_neighbors = tracing::field::Empty,
        hydradb.relimport.counter_read_us = tracing::field::Empty,
        hydradb.relimport.commit_us = tracing::field::Empty,
    )
}

fn write_txn_span(cell_id: &str, edge_type: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(
        "shard.write_txn",
        hydradb.cell_id = %cell_id,
        hydradb.edge_type = tracing::field::Empty,
        hydradb.commit_epoch = tracing::field::Empty,
        hydradb.writer.retries = tracing::field::Empty,
        error.class = tracing::field::Empty,
        // Per-phase attribution for the relationship CREATE/MERGE path, the
        // transaction every Bolt `UNWIND` relationship write runs. Each field
        // is recorded the moment its phase completes rather than at the end,
        // so a transaction that dies mid-flight (the 30 s query-timeout shape)
        // still shows which phases it got through — the first missing field is
        // the phase that ate the budget. Empty on every other write operation.
        hydradb.relimport.endpoint_check_us = tracing::field::Empty,
        hydradb.relimport.identity_scan_us = tracing::field::Empty,
        hydradb.relimport.identity_pointer_hits = tracing::field::Empty,
        hydradb.relimport.identity_pointer_misses = tracing::field::Empty,
        hydradb.relimport.record_read_us = tracing::field::Empty,
        hydradb.relimport.structural_check_us = tracing::field::Empty,
        hydradb.relimport.segment_scans = tracing::field::Empty,
        hydradb.relimport.segment_neighbors = tracing::field::Empty,
        hydradb.relimport.counter_read_us = tracing::field::Empty,
        hydradb.relimport.commit_us = tracing::field::Empty,
    );
    if let Some(edge_type) = edge_type {
        span.record("hydradb.edge_type", edge_type);
    }
    span
}

/// The one edge type a batch touches, or `None` if it spans several.
///
/// A batch built by the coordinator from a single `UNWIND` carries one edge
/// type, which is the case worth labelling. A mixed batch gets no attribute
/// rather than a misleading one.
fn common_edge_type(mutations: &[EdgeMutation]) -> Option<&str> {
    let first = mutations.first()?.edge_type.as_str();
    mutations.iter().all(|mutation| mutation.edge_type == first).then_some(first)
}

/// Records `hydradb.writer.retries` on `shard.write_txn` however the retry
/// loop exits, so no early return can forget it.
struct WriteRetryCount {
    span: tracing::Span,
    retries: u64,
}

impl WriteRetryCount {
    /// Must be constructed inside the `shard.write_txn` span.
    fn new() -> Self {
        Self {
            span: tracing::Span::current(),
            retries: 0,
        }
    }

    fn note_retry(&mut self) {
        self.retries += 1;
    }
}

impl Drop for WriteRetryCount {
    fn drop(&mut self) {
        self.span.record("hydradb.writer.retries", self.retries);
    }
}

/// `storage.commit`: the SlateDB commit, carrying the epoch it produced.
///
/// A thin wrapper over [`commit_txn_strict`] — same call, same durability
/// argument, same error. It exists so the commit is a span of its own, which is
/// what separates "the transaction conflicted" from "the transaction took nine
/// seconds to become durable".
async fn commit_txn_traced(
    txn: DbTransaction,
    await_durable: bool,
    cell_id: &str,
    edge_type: &str,
    commit_epoch: StorageSequence,
) -> Result<()> {
    let span = tracing::info_span!(
        "storage.commit",
        hydradb.cell_id = %cell_id,
        hydradb.edge_type = %edge_type,
        hydradb.commit_epoch = commit_epoch,
        hydradb.commit.local_only = crate::shard::write_pipeline::current().is_some(),
        error.class = tracing::field::Empty,
    );
    async { commit_txn_strict(txn, await_durable).await.inspect_err(record_error_class) }
        .instrument(span)
        .await
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IncidentEdge {
    edge_type: String,
    src: VertexId,
    dst: VertexId,
}

struct VertexDeleteDiscovery {
    edges: BTreeSet<IncidentEdge>,
    catalog: BTreeMap<String, bytes::Bytes>,
}

struct PreparedVertexDelete {
    vertices: BTreeSet<VertexId>,
    epoch: StorageSequence,
    discovery: VertexDeleteDiscovery,
}

#[cfg_attr(not(any(feature = "opencypher", test)), allow(dead_code))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VertexDeleteBatchMode {
    RejectConnected,
    Detach,
    IsolatedOnly,
    DetachAndIsolated,
}

impl VertexDeleteBatchMode {
    fn operation(self) -> &'static str {
        match self {
            Self::RejectConnected => "delete_vertices_batch",
            Self::Detach => "detach_delete_vertices_batch",
            Self::IsolatedOnly => "delete_isolated_vertices_batch",
            Self::DetachAndIsolated => "delete_vertices_and_isolated_candidates_batch",
        }
    }

    fn idempotency_operation(self) -> &'static str {
        match self {
            Self::IsolatedOnly => "isolated-vertex-delete",
            Self::DetachAndIsolated => "source-cleanup-vertex-delete",
            Self::RejectConnected | Self::Detach => "vertex-delete",
        }
    }
}

fn vertex_matches_path_node_constraints(vertex_id: VertexId, metadata: &VertexMetadata, constraints: &VertexMetadata) -> bool {
    constraints.labels.iter().all(|label| metadata.labels.contains(label))
        && constraints.properties.iter().all(|(property, expected)| {
            if property == "id" {
                return delete_constraint_property_values_equal(&VertexPropertyValue::Integer(vertex_id), expected);
            }
            metadata
                .properties
                .get(property)
                .is_some_and(|existing| delete_constraint_property_values_equal(existing, expected))
        })
}

fn delete_constraint_property_values_equal(left: &VertexPropertyValue, right: &VertexPropertyValue) -> bool {
    #[cfg(feature = "opencypher")]
    {
        super::query::vertex_property_values_equal(left, right)
    }
    #[cfg(not(feature = "opencypher"))]
    {
        left == right
    }
}

const VERTEX_DELETE_LOCK_RENEW_ITEMS: u64 = 64;

#[derive(Clone, Copy)]
struct RelationshipImportOptions<'a> {
    endpoint_labels: Option<(&'a str, &'a str)>,
    create_always: bool,
    update_existing_metadata: bool,
    merge_policy: Option<&'a QueryBatchMergePolicy>,
    #[cfg(feature = "opencypher")]
    guarded_rows: Option<&'a BTreeMap<RelationshipId, Vec<EdgeMetadata>>>,
    operation: &'static str,
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyTxnLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    value: &'a VertexPropertyValue,
}

/// How one MERGE row's identity resolved against the `rmerge_idx` pointer.
/// Every variant carries the pointer key so the pointer-update pass can write
/// or delete it without recomputing the encoding — `Hit` included, because a
/// hit whose row is then updated has its pointer blindly invalidated by the
/// index hooks and needs the deferred write to restore it.
#[cfg(feature = "opencypher")]
enum MergePointerOutcome {
    Hit(String),
    Miss(String),
    Stale(String),
}

#[cfg(feature = "opencypher")]
impl MergePointerOutcome {
    fn pointer_key(&self) -> &str {
        match self {
            Self::Hit(key) | Self::Miss(key) | Self::Stale(key) => key,
        }
    }
}

/// One MERGE row's identity resolution, carried from the concurrent lookup
/// stream through to id allocation: the row as submitted, whichever live
/// relationships already carry its identity (empty means it is an insert),
/// and how the pointer answered.
#[cfg(feature = "opencypher")]
struct MergeIdentityLookup {
    relationship: RelationshipMutation,
    existing: Vec<RelationshipRecord>,
    outcome: MergePointerOutcome,
}

/// Record a pointer-write candidate. The first resolution of an identity in a
/// batch proposes its row; a second resolution to a *different* row proves the
/// identity ambiguous within the batch and demotes the entry to a delete,
/// because a pointer may only exist while exactly one live row carries its
/// identity.
#[cfg(feature = "opencypher")]
fn note_merge_pointer(updates: &mut BTreeMap<String, Option<RelationshipId>>, pointer_key: &str, relationship_id: RelationshipId) {
    match updates.get(pointer_key) {
        None => {
            updates.insert(pointer_key.to_string(), Some(relationship_id));
        }
        Some(Some(existing)) if *existing == relationship_id => {}
        Some(_) => {
            updates.insert(pointer_key.to_string(), None);
        }
    }
}

impl GraphShard {
    /// Take a database built offline past storage sequence 0.
    ///
    /// SlateDB's offline image builder writes every row at sequence 0, and
    /// the edge writers read a transaction at sequence 0 as a cell with no
    /// data yet (`fresh_cell`): they skip reading endpoint vertex records,
    /// degree and relationship counters, and existing edges. The first edge
    /// batch on an imported image therefore replaced its endpoints' records
    /// with empty ones, dropping their labels and properties. Sealing
    /// commits one marker, so every later write reads what the image holds.
    /// A database already past sequence 0 is left as it is; returns whether
    /// this call committed the marker.
    ///
    /// The same transaction starts the topology changelog of every edge type
    /// the image lists (`matrix_dirty`): its low-water mark is the seal's
    /// commit sequence. Every image row is at sequence 0 and the seal changes
    /// no edge, so every edge change from there on is logged. Without the
    /// mark the first edge write would set it at its own sequence, past any
    /// generation built from the image, and the indexer's next build of that
    /// type would be a full one (`XlogDelta::CoverageGap`) instead of the
    /// incremental build a loaded store gets.
    pub async fn seal_offline_image(&self, cell_id: &str) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "seal_offline_image")?;
        self.run_write_pipeline(async {
            let _permit = self.acquire_graph_write_permit("seal_offline_image").await?;
            let _writer = self.writer_lane(cell_id).lock().await;
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            if txn.seqnum() > 0 {
                return Ok(false);
            }
            self.validate_write_fence_txn(&txn, cell_id, "seal_offline_image").await?;
            let commit_sequence = txn.seqnum() + 1;
            let prefix = keys::matrix_dirty_prefix(cell_id);
            let mut edge_types = Vec::new();
            let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let edge_type = key
                    .strip_prefix(&prefix)
                    .filter(|edge_type| !edge_type.is_empty())
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: key.clone(),
                        reason: "invalid graph index dirty marker key".to_string(),
                    })?
                    .to_string();
                edge_types.push(edge_type);
            }
            drop(iter);
            for edge_type in edge_types {
                let low_key = keys::xlog_low_water(cell_id, &edge_type);
                if read_txn_remote(&txn, &low_key).await?.is_none() {
                    txn.put(low_key.as_bytes(), encode_u64(commit_sequence))?;
                }
            }
            txn.put(keys::offline_image_sealed(cell_id).as_bytes(), [1_u8].as_slice())?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(true)
        })
        .await
    }

    /// Set `property` to `scores[i]` on `vertices[i]`: the write-back of a
    /// `gds.*.write` call. Nothing is read. Each value goes to the vertex's own
    /// [`keys::vertex_write_back_property`] key and its index entry to a
    /// generation of [`keys::vertex_write_back_index`] fixed by this call's
    /// first transaction; once every chunk is committed, one catalog key
    /// ([`keys::vertex_write_back_generation`]) switches readers to the new
    /// generation and the old one is swept. A vertex deleted after the
    /// algorithm read its graph gets an orphan value key and index entry: no
    /// read can see them, because a value is only overlaid on a record that
    /// exists and every index hit is verified against its record, and the
    /// vertex delete path removes them should the vertex be deleted later.
    /// So nothing is skipped, and [`WriteBackOutcome::skipped`] is always
    /// zero; it stays because the procedure reports it.
    ///
    /// Value keys are not per generation, so between the first chunk and the
    /// switch the property's point reads and its index disagree. The run
    /// marks the property pending first ([`keys::vertex_write_back_pending`])
    /// and the switch clears the mark; reads and hand writes of a marked
    /// property are refused, including after a run that stopped before its
    /// switch, until a later run of the property completes. Before its switch
    /// a run carries the property's live values on vertices outside it into
    /// its generation ([`Self::carry_forward_write_back_entries`]), so the
    /// generation it publishes indexes every vertex that holds a value, and
    /// the sweep of the older ones takes nothing a read can still see.
    ///
    /// Each chunk is its own transaction under the write permit, the writer
    /// lane and the fence check, and `budget` is checked between chunks. A
    /// chunk builds its values from `vertices` and `scores` as it writes them,
    /// so the call holds no value per vertex beyond the caller's two slices
    /// and the sorted copy the statistics count, 8 bytes a vertex each, and
    /// for vertices not given in ascending order a sorted copy of their ids
    /// for the carry, 8 bytes more.
    ///
    /// The whole call runs as one write pipeline context, so it waits for
    /// durability once, at the end, on the highest sequence any chunk
    /// committed. Every chunk's commit already carries its own durability
    /// wait in the background, and the reply only needs the last one; a wait
    /// per chunk would serialise the call on the WAL upload latency once per
    /// chunk for no extra guarantee. The trade-off is that the call holds one
    /// of the pipeline's bounded admissions (`MAX_PENDING_DURABLE_WRITES`)
    /// for its whole run instead of one per chunk, so a large write-back
    /// occupies a slot other writers could use for longer. The writer-changed
    /// check still happens per chunk: each chunk's pipeline call validates
    /// the writer against the outer context before its transaction runs.
    ///
    /// The sweep of the previous generation starts once the catalog switch
    /// is durable and runs on a task of its own, on the shard's owned
    /// handle, outside that pipeline context and under its own span: the
    /// reply does not wait for it, and its cost stays visible apart from the
    /// write. A shard that was never shared has no handle a task could hold
    /// and nothing can outlive it, so there the sweep runs inside the call.
    ///
    /// The call also publishes the property's planner statistics, counted
    /// from `scores` and the carried values after the carry and written
    /// beside the catalog in the switch's transaction
    /// ([`keys::vertex_write_back_statistics`]). The write-back is the one
    /// producer that holds every value of its generation and so can count it
    /// exactly without a read; the indexer's object is built from a reader's
    /// view pinned at its open, which may predate the property. The count
    /// runs on a copy of the scores in index order, sorted before the first
    /// chunk, which the call returns for the caller's own summary of them
    /// rather than have it sort a second copy. The carried values are held
    /// for the count, 8 bytes each for a float: none when the run covers
    /// the vertices the last one did.
    ///
    /// The sort and the count each walk every value, so they run on the
    /// shard's blocking compute path rather than on the async worker, under
    /// `budget`: checked before each starts and periodically while it runs,
    /// so a cancelled or expired call stops. GDS writes have no result-row
    /// cap, which is why neither is left inline. The scores move to the
    /// sort's blocking task and come back with their sorted copy, rather
    /// than being copied for it.
    ///
    /// `gate` is the gds compute permit a caller holds over its scores. It
    /// moves into each blocking task with what that task holds, the scores
    /// for the sort and the sorted copy and carried values for the count,
    /// and comes back with them, then is held to the end of the call: a call
    /// dropped on a cancel or a disconnect cannot free the gate while a
    /// detached task still runs and holds a value per vertex.
    ///
    /// `scores` holds one score per vertex; a different length is the
    /// algorithm's error and is refused before anything is written.
    /// `vertices` is in the projection's order, which need not be ascending.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn set_property_on_existing_vertices(
        &self,
        cell_id: &str,
        property: &str,
        vertices: &[VertexId],
        scores: Vec<f64>,
        budget: &QueryBudget,
        gate: Option<tokio::sync::SemaphorePermit<'static>>,
    ) -> Result<WriteBackOutcome> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        if scores.len() != vertices.len() {
            return Err(GraphError::SparseKernel {
                backend: "gds_write_back",
                reason: format!("{} scores for {} vertices", scores.len(), vertices.len()),
            });
        }
        self.ensure_write_authority(cell_id, "set_property_on_existing_vertices")?;
        budget.check("gds_write_back_statistics")?;
        // The carry looks vertices up by id. A label projection's are
        // ascending already; a `'*'` projection numbers them as its edges
        // meet them, and the lookup takes a sorted copy, sorted with the
        // scores' copy off the async worker.
        let unsorted_ids = (!vertices.is_sorted()).then(|| vertices.to_vec());
        let (scores, ordered, sorted_ids, gate) = {
            let span = tracing::info_span!("gds.write_back.statistics", hydradb.write.rows = scores.len());
            let count_budget = budget.clone();
            super::query::run_graph_compute(Arc::clone(&self.operation_metrics), "gds_write_back_statistics", move || {
                let _span = span.entered();
                let ordered = crate::engine::IndexOrderedFloats::sort(&scores, || count_budget.check("gds_write_back_statistics"))?;
                let sorted_ids = unsorted_ids.map(|mut ids| {
                    ids.sort_unstable();
                    ids
                });
                count_budget.check("gds_write_back_statistics")?;
                Ok((scores, ordered, sorted_ids, gate))
            })
            .await?
        };
        // In flight from the first chunk to the catalog switch. The sweeps
        // wait on this count, so it is released before the sweep below is
        // dispatched: the inline form would otherwise wait on this very call.
        let in_flight = self.write_back_activity.begin();
        let chunk_size = self.write_back_chunk();
        let (written, generation, ordered, carry, _gate) = self
            .run_write_pipeline(async {
                if vertices.is_empty() {
                    return Ok((0, None, ordered, CarryForward::default(), gate));
                }
                // Marked pending before the first value moves, so a run that
                // stops anywhere from here to the switch leaves the property
                // refused rather than torn.
                let generation = self
                    .write_back_locked(cell_id, "set_property_on_existing_vertices", 1, Some(budget), || {
                        self.begin_write_back_generation_txn(cell_id, property)
                    })
                    .await?;
                let mut written = 0_u64;
                for (vertices, scores) in vertices.chunks(chunk_size).zip(scores.chunks(chunk_size)) {
                    self.write_back_locked(cell_id, "set_property_on_existing_vertices", vertices.len(), Some(budget), || {
                        self.set_property_on_existing_vertices_txn(cell_id, property, vertices, scores, generation)
                    })
                    .await?;
                    written += vertices.len() as u64;
                }
                // Still marked: the older generations are unreadable and hand
                // writes of the property refused, so carrying their live
                // entries into this one races nothing. Read from a snapshot
                // taken now, not the one the call's reads were pinned to
                // before the algorithm ran: a hand write that landed between
                // that pin and the marker must be carried as it is now.
                let current = self.storage.writer_snapshot().await?;
                let mut carry = SlateGraphStore::scope_snapshot(
                    current,
                    self.carry_forward_write_back_entries(cell_id, property, generation, sorted_ids.as_deref().unwrap_or(vertices), budget),
                )
                .await?;
                let carried = std::mem::take(&mut carry.values);
                // The gate goes with the sorted copy and the carried values,
                // as it went with the scores to the sort: a call dropped
                // mid-count must not free it while the count holds them.
                let (ordered, statistics, gate) = {
                    let span = tracing::info_span!(
                        "gds.write_back.statistics",
                        hydradb.write.rows = ordered.as_slice().len() + carried.len()
                    );
                    let count_budget = budget.clone();
                    let count_property = property.to_string();
                    super::query::run_graph_compute(Arc::clone(&self.operation_metrics), "gds_write_back_statistics", move || {
                        let _span = span.entered();
                        let statistics = crate::engine::exact_float_property_statistics(&count_property, &ordered, carried, || {
                            count_budget.check("gds_write_back_statistics")
                        })?;
                        Ok((ordered, statistics, gate))
                    })
                    .await?
                };
                let statistics = crate::engine::encode_write_back_property_statistics(cell_id, property, generation, &statistics);
                self.write_back_locked(cell_id, "publish_write_back_generation", 1, Some(budget), || {
                    self.publish_write_back_generation_txn(cell_id, property, generation, &statistics)
                })
                .await?;
                Ok((written, Some(generation), ordered, carry, gate))
            })
            .await?;
        drop(in_flight);
        // This node's planner reads the new counts on its next query rather
        // than at the end of the TTL. Other nodes serving the cell have no
        // such signal and pick them up at theirs.
        #[cfg(feature = "experimental-cypher-engine")]
        if generation.is_some() {
            self.planner_statistics_cache.invalidate(cell_id);
        }
        if let Some(generation) = generation {
            match self.self_handle.get().and_then(std::sync::Weak::upgrade) {
                Some(shard) => shard.spawn_write_back_sweep(cell_id.to_string(), property.to_string(), generation),
                None => {
                    self.sweep_write_back_index_generations(cell_id, property, generation, budget)
                        .await?;
                }
            }
        }
        Ok(WriteBackOutcome {
            written,
            skipped: 0,
            ordered,
            carried: carry.carried,
            carry_scanned: carry.scanned,
            carry_millis: carry.millis,
        })
    }

    /// Carry `property`'s live values on vertices outside this run into its
    /// generation, before the switch: at the switch the new generation
    /// indexes every vertex that has a value key for the property, not only
    /// the run's. A run leaves the property where it was on vertices it did
    /// not score, as a GDS write does, and those stay found by the index.
    ///
    /// Walks the index entries of every generation below `generation` from
    /// the sweep floor: the catalog's, those of stopped runs, and any a
    /// running sweep has not reached. An entry of a vertex outside the run
    /// (`vertices` is ascending) is carried when the vertex's value
    /// key still holds the value the entry indexes; a value key that is gone
    /// or holds another value makes the entry stale, and it is left to the
    /// sweep. A vertex is carried once, however many generations index its
    /// value. The run's marker refuses hand writes and deletes of the
    /// property meanwhile, so the value keys hold still under the walk.
    ///
    /// One read walk of the older generations per run, plus a point read and
    /// a put per vertex outside the run: a run over the same vertices as the
    /// last reads the walk and writes nothing.
    #[cfg(feature = "opencypher")]
    async fn carry_forward_write_back_entries(
        &self,
        cell_id: &str,
        property: &str,
        generation: StorageSequence,
        vertices: &[VertexId],
        budget: &QueryBudget,
    ) -> Result<CarryForward> {
        let started = std::time::Instant::now();
        let floor_key = keys::vertex_write_back_sweep_floor(cell_id, property);
        let floor = match self.read_remote(&floor_key).await? {
            Some(value) => Some(decode_u64(&floor_key, &value)?),
            None => None,
        };
        let prefix = keys::vertex_write_back_index_property_prefix(cell_id, property);
        let start = floor.map(|floor| keys::vertex_write_back_index_generation_suffix(floor).into_bytes());
        let mut iter = self.scan_remote_prefix_from(&prefix, start).await?;
        let chunk_size = self.write_back_chunk();
        let mut carry = CarryForward::default();
        let mut carried_vertices = std::collections::HashSet::new();
        let mut batch = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let Some((_, entry_generation, encoded, vertex_id)) = keys::parse_vertex_write_back_index_key(&key) else {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "expected a write-back index key".to_string(),
                });
            };
            if entry_generation >= generation {
                break;
            }
            if carry.scanned % CARRY_FORWARD_CHECK_STRIDE == 0 {
                budget.check("gds_write_back_carry_forward")?;
            }
            carry.scanned += 1;
            if vertices.binary_search(&vertex_id).is_ok() || carried_vertices.contains(&vertex_id) {
                continue;
            }
            let value_key = keys::vertex_write_back_property(cell_id, vertex_id, property);
            let Some(held) = self.read_remote(&value_key).await? else {
                continue;
            };
            let held = std::str::from_utf8(&held).map_err(|err| GraphError::CorruptValue {
                key: value_key.clone(),
                reason: err.to_string(),
            })?;
            let held = decode_vertex_property_value_record(&value_key, held)?;
            if !held.is_scalar_indexable() || encode_vertex_property_value_key(&held) != encoded {
                continue;
            }
            carried_vertices.insert(vertex_id);
            carry.values.push(&held);
            batch.push((
                keys::vertex_write_back_index(cell_id, property, generation, &encoded, vertex_id),
                vertex_id,
            ));
            if batch.len() >= chunk_size {
                self.write_back_locked(cell_id, "carry_forward_write_back_entries", batch.len(), Some(budget), || {
                    self.put_write_back_index_entries_txn(cell_id, &batch)
                })
                .await?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.write_back_locked(cell_id, "carry_forward_write_back_entries", batch.len(), Some(budget), || {
                self.put_write_back_index_entries_txn(cell_id, &batch)
            })
            .await?;
        }
        carry.carried = carried_vertices.len() as u64;
        carry.millis = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        Ok(carry)
    }

    /// Put write-back index entries, each naming its vertex.
    #[cfg(feature = "opencypher")]
    async fn put_write_back_index_entries_txn(&self, cell_id: &str, entries: &[(String, VertexId)]) -> Result<()> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "carry_forward_write_back_entries")
            .await?;
        let result = async {
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            self.validate_write_fence_txn(&txn, cell_id, "carry_forward_write_back_entries")
                .instrument(tracing::info_span!("write.fence_validate"))
                .await?;
            self.validate_changing_write().await?;
            let epoch = next_epoch_txn(&txn, cell_id).await?;
            for (key, vertex_id) in entries {
                txn.put(key.as_bytes(), encode_u64(*vertex_id).as_slice())?;
            }
            commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        .await;
        finish_local_write(lock, result).await
    }

    /// Run the sweep of `property`'s superseded index generations on a task
    /// of its own, holding this owned handle, so the caller's reply does not
    /// wait for it.
    ///
    /// The task has no query budget: the request that published the
    /// generation has been answered, and a sweep stopped by a deadline would
    /// only leave the old generation for the next sweep to find. The shard
    /// bounds it instead: `close` aborts every sweep still running, which is
    /// safe because a sweep is rebuildable work, since the next sweep of the
    /// property deletes everything below the current generation, whatever an
    /// earlier one left.
    ///
    /// The sweep yields to write-backs rather than share the writer with
    /// them: it starts only once no write-back has run for the configured
    /// grace, and parks between batches while one is in flight.
    ///
    /// A property has at most one pending sweep: one that has not yet taken
    /// the generation it sweeps to, which it does once it holds the GC
    /// permit. A write-back that completes while one is pending raises that
    /// sweep's target and spawns nothing, so a batch of write-backs that
    /// keeps restarting the grace leaves one task per property rather than
    /// one per call, and that task sweeps to the newest generation. Beyond
    /// that, sweeps of one property are not serialised: a pending sweep may
    /// start while an earlier one still runs where the GC gate admits more
    /// than one job. Each reads the catalog for its floor and deletes only
    /// below it, so the overlap costs duplicate tombstones and nothing else,
    /// and a lane per property would add state for no correctness.
    ///
    /// The span is a root that follows from the request's span rather than a
    /// child of it: a child holds its parent open until it closes, which
    /// would stretch the request's recorded duration over the very work this
    /// detaches from it.
    #[cfg(feature = "opencypher")]
    fn spawn_write_back_sweep(self: std::sync::Arc<Self>, cell_id: String, property: String, published: StorageSequence) {
        {
            let mut targets = self
                .write_back_sweep_targets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match targets.entry((cell_id.clone(), property.clone())) {
                std::collections::hash_map::Entry::Occupied(mut pending) => {
                    let target = pending.get_mut();
                    *target = (*target).max(published);
                    return;
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(published);
                }
            }
        }
        // Made before the task, so a task aborted before its first poll
        // still clears its entry when its future is dropped.
        let mut pending = PendingWriteBackSweep {
            shard: std::sync::Arc::clone(&self),
            key: Some((cell_id.clone(), property.clone())),
            published,
        };
        let span = write_back_sweep_span(&cell_id, true);
        span.follows_from(tracing::Span::current());
        let shard = std::sync::Arc::clone(&self);
        let grace = std::time::Duration::from_millis(self.limits.write_back_sweep_grace_ms);
        let task = tokio::spawn(async move {
            let budget = QueryBudget::new(None, None);
            match shard
                .sweep_write_back_index_generations_under(span, &cell_id, &property, || pending.take(), Some(grace), &budget)
                .await
            {
                Ok(_outcome) => {}
                Err(error) => tracing::warn!(
                    hydradb.cell_id = %cell_id,
                    hydradb.property = %property,
                    error = %error,
                    "write-back sweep failed; the next sweep of the property covers what it left"
                ),
            }
        });
        let mut sweeps = self.write_back_sweeps.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        sweeps.retain(|sweep| !sweep.is_finished());
        sweeps.push(task);
    }

    #[cfg(feature = "opencypher")]
    fn take_write_back_sweeps(&self) -> Vec<tokio::task::JoinHandle<()>> {
        std::mem::take(&mut *self.write_back_sweeps.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
    }

    /// Stop every detached sweep and wait until each has stopped, before the
    /// store closes under them. Aborted rather than finished: a sweep is
    /// rebuildable work, and a shutdown has nothing to gain from a batch of
    /// tombstones it could as well leave to the next sweep.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn abort_write_back_sweeps(&self) {
        for sweep in self.take_write_back_sweeps() {
            sweep.abort();
            // Cancellation is the expected outcome; a panic in a sweep was
            // reported by the task that panicked.
            let _ = sweep.await;
        }
    }

    /// Rows one write-back transaction writes and one sweep transaction
    /// deletes: the configured chunk, never more than a bulk import may
    /// carry, never less than one. Each holds the writer lane for its
    /// transaction, so the chunk also bounds how long other writes wait
    /// behind a large write-back.
    #[cfg(feature = "opencypher")]
    fn write_back_chunk(&self) -> usize {
        self.limits.write_back_chunk.min(self.limits.max_bulk_import_edges).max(1)
    }

    /// One write-back transaction under the write permit, the writer lane
    /// and the conflict retry loop, inside its own pipeline context.
    ///
    /// With a `budget`, both waits give up at the call's deadline or
    /// cancellation and the budget is checked once admitted: a gds call holds
    /// its compute gate and its whole score vector while it waits, so a busy
    /// writer must not keep them past the deadline, and an expired or
    /// cancelled call must not start another transaction. Work with no
    /// caller to answer passes none and waits its turn.
    #[cfg(feature = "opencypher")]
    async fn write_back_locked<T, F, Fut>(
        &self,
        cell_id: &str,
        operation: &'static str,
        rows: usize,
        budget: Option<&QueryBudget>,
        txn: F,
    ) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.run_write_pipeline(async {
            let permit = self.acquire_graph_write_permit(operation).instrument(tracing::info_span!(
                "shard.write_permit",
                hydradb.cell_id = %cell_id,
                hydradb.write.operation = operation
            ));
            let _permit = match budget {
                Some(budget) => budget.until_deadline(operation, permit).await??,
                None => permit.await?,
            };
            let lane = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id));
            let _writer = match budget {
                Some(budget) => {
                    let writer = budget.until_deadline(operation, lane).await?;
                    budget.check(operation)?;
                    writer
                }
                None => lane.await,
            };
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match txn()
                    .instrument(tracing::info_span!("storage.txn", hydradb.write.rows = rows))
                    .await
                {
                    Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                    result => return result,
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
        })
        .await
    }

    /// The start of a write-back run: its generation, this transaction's
    /// epoch, which is unique and increasing across runs, put as the
    /// property's pending marker ([`keys::vertex_write_back_pending`]). A
    /// marker a stopped run left is replaced. Returns the generation.
    #[cfg(feature = "opencypher")]
    async fn begin_write_back_generation_txn(&self, cell_id: &str, property: &str) -> Result<StorageSequence> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "begin_write_back_generation")
            .await?;
        let result = async {
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            self.validate_write_fence_txn(&txn, cell_id, "begin_write_back_generation")
                .instrument(tracing::info_span!("write.fence_validate"))
                .await?;
            self.validate_changing_write().await?;
            let epoch = next_epoch_txn(&txn, cell_id).await?;
            txn.put(
                keys::vertex_write_back_pending(cell_id, property).as_bytes(),
                encode_u64(epoch).as_slice(),
            )?;
            commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(epoch)
        }
        .await;
        finish_local_write(lock, result).await
    }

    /// One chunk of a write-back: a value key and an index entry in
    /// `generation` per vertex, `scores[i]` on `vertices[i]`, put blind.
    #[cfg(feature = "opencypher")]
    async fn set_property_on_existing_vertices_txn(
        &self,
        cell_id: &str,
        property: &str,
        vertices: &[VertexId],
        scores: &[f64],
        generation: StorageSequence,
    ) -> Result<()> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "set_property_on_existing_vertices")
            .await?;
        let result = async {
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            self.validate_write_fence_txn(&txn, cell_id, "set_property_on_existing_vertices")
                .await?;
            self.validate_changing_write().await?;
            let epoch = next_epoch_txn(&txn, cell_id).await?;
            for (&vertex_id, &score) in vertices.iter().zip(scores) {
                let value = VertexPropertyValue::Float(QueryFloat(score));
                txn.put(
                    keys::vertex_write_back_property(cell_id, vertex_id, property).as_bytes(),
                    encode_vertex_property_value_record(&value).as_bytes(),
                )?;
                txn.put(
                    keys::vertex_write_back_index(cell_id, property, generation, &encode_vertex_property_value_key(&value), vertex_id)
                        .as_bytes(),
                    encode_u64(vertex_id).as_slice(),
                )?;
            }
            commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        .await;
        finish_local_write(lock, result).await
    }

    /// The catalog switch: readers of `property` resolve `generation` from
    /// here on. One key, committed after every entry of the generation, so
    /// the switch is atomic and a reader never sees a generation still being
    /// built. The generation's planner statistics go in the same
    /// transaction, so a reader never sees the one without the other, and
    /// the run's pending marker is deleted in it, so the property is never
    /// both switched and refused, nor unrefused and unswitched.
    #[cfg(feature = "opencypher")]
    async fn publish_write_back_generation_txn(
        &self,
        cell_id: &str,
        property: &str,
        generation: StorageSequence,
        statistics: &[u8],
    ) -> Result<()> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "publish_write_back_generation")
            .await?;
        let result = async {
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            self.validate_write_fence_txn(&txn, cell_id, "publish_write_back_generation")
                .instrument(tracing::info_span!("write.fence_validate"))
                .await?;
            self.validate_changing_write().await?;
            let epoch = next_epoch_txn(&txn, cell_id).await?;
            txn.put(
                keys::vertex_write_back_generation(cell_id, property).as_bytes(),
                encode_u64(generation).as_slice(),
            )?;
            txn.put(keys::vertex_write_back_statistics(cell_id, property).as_bytes(), statistics)?;
            // Cleared only when it is this run's: a marker another run of
            // the property put after this one began is that run's, still
            // writing.
            let pending = keys::vertex_write_back_pending(cell_id, property);
            let held = read_txn_remote(&txn, &pending)
                .await?
                .map(|value| decode_u64(&pending, &value))
                .transpose()?;
            if held == Some(generation) {
                txn.delete(pending.as_bytes())?;
            }
            commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        .await;
        finish_local_write(lock, result).await
    }

    /// Raise the sweep floor of `property` to `floor`: no index entry below
    /// it remains. Never lowered: two sweeps may overlap where the GC gate
    /// admits more than one job, and the one that swept further must not
    /// have its floor pulled back by the one that finishes later.
    #[cfg(feature = "opencypher")]
    async fn advance_write_back_sweep_floor_txn(&self, cell_id: &str, property: &str, floor: StorageSequence) -> Result<()> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "advance_write_back_sweep_floor")
            .await?;
        let result = async {
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            self.validate_write_fence_txn(&txn, cell_id, "advance_write_back_sweep_floor")
                .instrument(tracing::info_span!("write.fence_validate"))
                .await?;
            self.validate_changing_write().await?;
            let key = keys::vertex_write_back_sweep_floor(cell_id, property);
            let held = match read_txn_remote(&txn, &key).await? {
                Some(value) => decode_u64(&key, &value)?,
                None => 0,
            };
            if held >= floor {
                return Ok(());
            }
            let epoch = next_epoch_txn(&txn, cell_id).await?;
            txn.put(key.as_bytes(), encode_u64(floor).as_slice())?;
            commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        .await;
        finish_local_write(lock, result).await
    }

    /// Delete every index generation of `property` older than the current
    /// one, in batches, under the GC permit. Generations sort oldest first
    /// under the property prefix, so one scan from its start yields every
    /// entry to delete in order and stops at the first entry of the current
    /// generation.
    ///
    /// The current generation is the newer of `published`, the one the
    /// calling write-back just switched the catalog to, and what the catalog
    /// holds. The caller's own switch is passed in because it may not be
    /// visible to a read yet: a read at remote durability sees a commit only
    /// once its WAL is uploaded, and a sweep that read the previous
    /// generation back would stop at that generation's first entry and
    /// delete nothing. The catalog is still read, inside the pipeline
    /// context so the read is at memory durability, as a floor: a later run
    /// may have published a newer generation since, and a sweep never
    /// removes a generation the catalog names or one newer than it. A run
    /// whose generation is still being written is safe either way, since its
    /// generation is newer than anything published.
    ///
    /// The storage engine at this revision has no range delete, so a swept
    /// generation costs one tombstone per entry through the writer, which
    /// compaction later drops. A revision with range deletes turns a whole
    /// generation into one range tombstone and makes this a single small
    /// write; adopting one is a separate change. Returns the entries
    /// deleted.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn sweep_write_back_index_generations(
        &self,
        cell_id: &str,
        property: &str,
        published: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        let span = write_back_sweep_span(cell_id, false);
        self.sweep_write_back_index_generations_under(span, cell_id, property, || published, None, budget)
            .await
            .map(|outcome| outcome.deleted)
    }

    /// Wait until no write-back has run for `grace`: none in flight now,
    /// and none begun or in flight once the grace has elapsed. A write-back
    /// that begins during the grace restarts it, so the calls of a batch of
    /// algorithms that follow one another inside the grace keep the sweep
    /// out until the batch ends. Returns the time spent waiting.
    #[cfg(feature = "opencypher")]
    async fn wait_for_write_back_quiet(&self, grace: std::time::Duration) -> std::time::Duration {
        let waiting = std::time::Instant::now();
        loop {
            self.write_back_activity.wait_idle().await;
            if grace.is_zero() {
                break;
            }
            let begun = self.write_back_activity.started();
            tokio::time::sleep(grace).await;
            if self.write_back_activity.in_flight() == 0 && self.write_back_activity.started() == begun {
                break;
            }
        }
        waiting.elapsed()
    }

    /// [`Self::sweep_write_back_index_generations`] under a span the caller
    /// made, so a sweep run inside a request and one detached from it differ
    /// only in where their span hangs and whether they wait out write-backs
    /// before starting: `grace` is that wait, and `None` starts at once, as
    /// a sweep inside a request must. `published` is asked for once the
    /// sweep holds the GC permit, so a detached sweep that waited takes the
    /// newest generation published while it did.
    #[cfg(feature = "opencypher")]
    async fn sweep_write_back_index_generations_under(
        &self,
        span: tracing::Span,
        cell_id: &str,
        property: &str,
        published: impl FnOnce() -> StorageSequence,
        grace: Option<std::time::Duration>,
        budget: &QueryBudget,
    ) -> Result<crate::core::state::WriteBackSweepOutcome> {
        // Recorded on the handle, not on the current span: the handle names
        // this span whether or not the subscriber tracks entered spans.
        let sweep = span.clone();
        async {
            // Before the GC permit: a sweep waiting out a batch of
            // write-backs must not hold the permit from other GC meanwhile.
            let mut waited = match grace {
                Some(grace) => self.wait_for_write_back_quiet(grace).await,
                None => std::time::Duration::ZERO,
            };
            let _gc = self.acquire_gc_permit("sweep_write_back_index_generations").await?;
            let published = published();
            let catalog_key = keys::vertex_write_back_generation(cell_id, property);
            let floor_key = keys::vertex_write_back_sweep_floor(cell_id, property);
            let prefix = keys::vertex_write_back_index_property_prefix(cell_id, property);
            let batch_size = self.write_back_chunk();
            // A pipeline context holds one of the pipeline's bounded
            // admissions, and a write-back counts itself in flight before it
            // asks for one. So the sweep never parks inside a context: where
            // the GC gate admits as many sweeps as the pipeline has
            // admissions, sweeps parked holding them all would wait on a
            // write-back that waits on them, and every other writer behind
            // it. The sweep runs its batches in a context that ends as soon
            // as a write-back is in flight, parks outside it, and opens a
            // new one to resume. The batches between two parks share their
            // context, as the write path's chunks do, so durability is
            // awaited once per run of batches rather than once per batch;
            // the extra wait a park adds falls while a write-back has the
            // writer anyway. The catalog read and the opening of the walk
            // take a context of their own, so both read at memory
            // durability.
            //
            // One iterator serves every batch. It is opened before the sweep
            // deletes anything, so its snapshot holds no tombstone of the
            // sweep's own and cannot hand back a key already deleted, whenever
            // those tombstones would otherwise become visible; and one walk of
            // the prefix is the least a sweep that has to find its keys can
            // read. Reopening a scan per batch would start each one by
            // climbing over every tombstone the earlier batches left in the
            // memtables, so the scanning, not the committing, would grow with
            // the number of batches.
            //
            // The walk opens at the floor the last completed sweep left, not
            // at the property's first key: the tombstones of every generation
            // swept before stay in the tree until compaction drops them, and
            // a walk from the first key climbs over all of them, so its cost
            // would grow with every sweep rather than with the generation
            // being swept. A sweep that fails or is aborted leaves the floor
            // where it was, and the next one re-walks from there: slower,
            // never wrong.
            let (current, floor, mut iter) = self
                .run_write_pipeline(async {
                    let catalog = match self.read_remote(&catalog_key).await? {
                        Some(value) => decode_u64(&catalog_key, &value)?,
                        None => 0,
                    };
                    let floor = match self.read_remote(&floor_key).await? {
                        Some(value) => Some(decode_u64(&floor_key, &value)?),
                        None => None,
                    };
                    let start = floor.map(|floor| keys::vertex_write_back_index_generation_suffix(floor).into_bytes());
                    let iter = self.scan_remote_prefix_from(&prefix, start).await?;
                    Ok((published.max(catalog), floor.unwrap_or(0), iter))
                })
                .await?;
            let (mut deleted, mut batches, mut scanned) = (0_u64, 0_u64, 0_u64);
            let mut exhausted = false;
            loop {
                let write_back_arrived = self
                    .run_write_pipeline(async {
                        while !exhausted {
                            budget.check("sweep_write_back_index_generations")?;
                            // A write-back that arrived since the last batch
                            // has the writer: leave the context to park.
                            if self.write_back_activity.in_flight() > 0 {
                                return Ok(true);
                            }
                            let mut batch = Vec::with_capacity(batch_size);
                            while batch.len() < batch_size {
                                let Some(kv) = iter.next().await? else {
                                    exhausted = true;
                                    break;
                                };
                                scanned += 1;
                                let key = String::from_utf8_lossy(&kv.key).into_owned();
                                let Some((_, generation, _, _)) = keys::parse_vertex_write_back_index_key(&key) else {
                                    return Err(GraphError::CorruptValue {
                                        key,
                                        reason: "expected a write-back index key".to_string(),
                                    });
                                };
                                if generation >= current {
                                    exhausted = true;
                                    break;
                                }
                                batch.push(key);
                            }
                            if batch.is_empty() {
                                break;
                            }
                            self.write_back_locked(cell_id, "sweep_write_back_index_generations", batch.len(), Some(budget), || {
                                self.delete_keys_txn(cell_id, "sweep_write_back_index_generations", &batch)
                            })
                            .await?;
                            deleted += batch.len() as u64;
                            batches += 1;
                        }
                        // Every exit above leaves nothing below `current`:
                        // the walk either ran out of keys or met the first
                        // entry at or past it. A floor already there is not
                        // rewritten.
                        if floor < current {
                            self.write_back_locked(cell_id, "advance_write_back_sweep_floor", 1, Some(budget), || {
                                self.advance_write_back_sweep_floor_txn(cell_id, property, current)
                            })
                            .await?;
                        }
                        Ok(false)
                    })
                    .await?;
                if !write_back_arrived {
                    break;
                }
                // Park until the write-back is done, then resume the walk.
                // No new grace here: the sweep holds the GC permit and its
                // iterator, and a batch slipped between two write-backs costs
                // them no more than resuming would.
                let parking = std::time::Instant::now();
                self.write_back_activity.wait_idle().await;
                waited += parking.elapsed();
            }
            let outcome = crate::core::state::WriteBackSweepOutcome {
                deleted,
                batches,
                waited,
                floor,
                scanned,
            };
            sweep.record("hydradb.write.deletes", outcome.deleted);
            sweep.record("hydradb.write.batches", outcome.batches);
            sweep.record("hydradb.write.waited_ms", outcome.waited.as_millis() as u64);
            sweep.record("hydradb.write.floor", outcome.floor);
            sweep.record("hydradb.write.scanned", outcome.scanned);
            Ok(outcome)
        }
        .instrument(span)
        .await
    }

    /// One transaction that tombstones `keys`.
    #[cfg(feature = "opencypher")]
    async fn delete_keys_txn(&self, cell_id: &str, operation: &'static str, keys: &[String]) -> Result<()> {
        let lock = self.acquire_local_write_guard_no_fence(cell_id, operation).await?;
        let result = async {
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            self.validate_write_fence_txn(&txn, cell_id, operation)
                .instrument(tracing::info_span!("write.fence_validate"))
                .await?;
            self.validate_changing_write().await?;
            let epoch = next_epoch_txn(&txn, cell_id).await?;
            for key in keys {
                txn.delete(key.as_bytes())?;
            }
            commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        .await;
        finish_local_write(lock, result).await
    }

    pub async fn set_vertex_metadata(&self, cell_id: &str, vertex_id: VertexId, metadata: VertexMetadata) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        validate_vertex_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_vertex_metadata")?;
        self.run_write_pipeline(async {
            let _permit = self
                .acquire_graph_write_permit("set_vertex_metadata")
                .instrument(
                    tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_vertex_metadata"),
                )
                .await?;
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self
                    .set_vertex_metadata_txn(cell_id, vertex_id, metadata.clone())
                    .instrument(tracing::info_span!("storage.txn"))
                    .await
                {
                    Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                    Ok(()) => {
                        self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    result => return result,
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
        })
        .await
    }

    pub async fn set_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        self.set_vertex_metadata_batch_checked(cell_id, updates, None).await
    }

    /// Replace each vertex's record with `next` only if it is still the
    /// `read` record its statement computed `next` from, all or none. A
    /// statement reads at a snapshot taken before this transaction: without
    /// the check, a write committed in between (another `SET`, a node batch)
    /// was overwritten, and a vertex deleted in between was recreated.
    /// Refuses with `ConditionalWriteConflict` when any record changed or is
    /// gone, writing nothing; the statement re-runs on a fresh snapshot.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn swap_vertex_metadata_batch(
        &self,
        cell_id: &str,
        swaps: BTreeMap<VertexId, (VertexMetadata, VertexMetadata)>,
    ) -> Result<usize> {
        let mut read = BTreeMap::new();
        let mut next = Vec::with_capacity(swaps.len());
        for (vertex_id, (before, after)) in swaps {
            read.insert(vertex_id, before);
            next.push((vertex_id, after));
        }
        self.set_vertex_metadata_batch_checked(cell_id, next, Some(&read)).await
    }

    async fn set_vertex_metadata_batch_checked(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
        read: Option<&BTreeMap<VertexId, VertexMetadata>>,
    ) -> Result<usize> {
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "set_vertex_metadata_batch")?;
        let updates = coalesce_vertex_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "set_vertex_metadata_batch",
            updates.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        self.run_write_pipeline(async {
        let _permit = self
            .acquire_graph_write_permit("set_vertex_metadata_batch")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_vertex_metadata_batch"))
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_vertex_metadata_batch_txn(cell_id, updates.clone(), read)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(changed) => {
                    if changed > 0 {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
        }).await
    }

    pub async fn merge_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
        merge_policy: Option<&QueryBatchMergePolicy>,
    ) -> Result<usize> {
        validate_component("cell_id", cell_id)?;
        if let Some(policy) = merge_policy {
            policy.validate()?;
        }
        self.ensure_write_authority(cell_id, "merge_vertex_metadata_batch")?;
        let updates = group_vertex_metadata_merges(updates, merge_policy)?;
        if updates.is_empty() {
            return Ok(0);
        }
        // A guarded vertex can keep several patches, and each is applied and
        // cloned on retry, so the limit counts patches rather than vertices.
        ensure_limit(
            "merge_vertex_metadata_batch",
            updates.iter().map(|(_, patches)| patches.len() as u64).sum(),
            self.limits.max_bulk_import_edges as u64,
        )?;
        #[cfg(feature = "opencypher")]
        let started = std::time::Instant::now();
        let span = write_txn_span(cell_id, None);
        let result = self.run_write_pipeline(async {
            let admission = self
                .acquire_graph_write_permit("merge_vertex_metadata_batch")
                .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "merge_vertex_metadata_batch"));
            tokio::pin!(admission);
            // An unchanged merge is a read-only operation and can linearize at
            // this durable snapshot, before the busy writer. Changed
            // batches are re-read under the existing locks; never commit preflight data.
            let _permit = if merge_policy.is_none()
                && updates.len() <= METADATA_NOOP_PREFLIGHT_MAX_ITEMS
                && self.graph_write_gate.available_permits() == 0
            {
                let preflight_started = std::time::Instant::now();
                // Join the queue immediately. A speculative read must not add
                // its I/O latency before a changing write can acquire the guard.
                let (permit, unchanged) = tokio::select! {
                    biased;
                    permit = &mut admission => (Some(permit?), false),
                    unchanged = self.vertex_metadata_merges_are_noops(cell_id, &updates)
                        .instrument(tracing::info_span!("write.noop_preflight", hydradb.cell_id = %cell_id)) => (None, unchanged?),
                };
                if unchanged {
                    let elapsed_us = preflight_started.elapsed().as_micros() as u64;
                    self.operation_metrics.merge_vertex_metadata_batches_profiled.fetch_add(1, Ordering::Relaxed);
                    self.operation_metrics.merge_vertex_metadata_batch_items.fetch_add(updates.len() as u64, Ordering::Relaxed);
                    self.operation_metrics.merge_vertex_metadata_read_us.fetch_add(elapsed_us, Ordering::Relaxed);
                    self.operation_metrics.merge_vertex_metadata_txn_us.fetch_add(elapsed_us, Ordering::Relaxed);
                    self.operation_metrics.merge_vertex_metadata_nochange_exits.fetch_add(1, Ordering::Relaxed);
                    return Ok(0);
                }
                match permit {
                    Some(permit) => permit,
                    None => admission.await?,
                }
            } else {
                admission.await?
            };
            let mut retries = WriteRetryCount::new();
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self
                    .merge_vertex_metadata_batch_txn(cell_id, updates.clone(), merge_policy)
                    .instrument(tracing::info_span!("storage.txn"))
                    .await
                {
                    Err(err)
                        if is_retryable_write_conflict(&err)
                            && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                    {
                        self.operation_metrics
                            .write_retries
                            .fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        tokio::task::yield_now().await;
                    }
                    Ok(changed) => {
                        return Ok(changed);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
            .inspect_err(record_error_class)
        }
        .instrument(span))
        .await;
        // No new WAL means no create-only fencing barrier. Check ownership
        // after releasing the foreground gate before acknowledging a no-op.
        let result = if result.as_ref().is_ok_and(|changed| *changed == 0) {
            self.storage.refresh_writer_fence().await.map(|()| 0)
        } else {
            result
        };
        if result.as_ref().is_ok_and(|changed| *changed > 0) {
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
        }
        #[cfg(feature = "opencypher")]
        self.operation_metrics
            .merge_vertex_metadata_batch_latency
            .record_micros(started.elapsed().as_micros() as u64);
        result
    }

    pub async fn import_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let _latency = self.operation_metrics.import_vertex_metadata_batch_latency.start_timer();
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "import_vertex_metadata_batch")?;
        let updates = coalesce_vertex_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "import_vertex_metadata_batch",
            updates.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        self.run_write_pipeline_fencing_noops(self.import_vertex_metadata_batch_inline(cell_id, updates))
            .await
    }

    async fn import_vertex_metadata_batch_inline(&self, cell_id: &str, updates: Vec<(VertexId, VertexMetadata)>) -> Result<usize> {
        let _permit = self
            .acquire_graph_write_permit("import_vertex_metadata_batch")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "import_vertex_metadata_batch"))
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .import_vertex_metadata_batch_txn(cell_id, updates.clone())
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(changed) => {
                    if changed > 0 {
                        self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn set_vertex_metadata_txn(&self, cell_id: &str, vertex_id: VertexId, metadata: VertexMetadata) -> Result<()> {
        let lock = self.acquire_local_write_guard_no_fence(cell_id, "set_vertex_metadata").await?;
        let result = self.set_vertex_metadata_txn_locked(cell_id, vertex_id, metadata).await;
        finish_local_write(lock, result).await
    }

    async fn set_vertex_metadata_txn_locked(&self, cell_id: &str, vertex_id: VertexId, metadata: VertexMetadata) -> Result<()> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "set_vertex_metadata")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let vertex_key = keys::vertex(cell_id, vertex_id);
        let existing = decode_vertex_record(&vertex_key, read_txn_remote(&txn, &vertex_key).await?.as_ref())?;
        // The replacement is the vertex as reads will see it, so it is
        // compared with that view, and the write-back values it omits go.
        let (seen, mut omitted) = overlaid_vertex_metadata_txn(&txn, cell_id, vertex_id, existing.as_ref()).await?;
        if seen.as_ref() == Some(&metadata) {
            self.storage.refresh_writer_fence().await?;
            return Ok(());
        }
        omitted.retain(|(property, _)| !metadata.properties.contains_key(property));
        self.validate_changing_write().await?;
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        let mut generations = WriteBackGenerations::default();
        apply_vertex_metadata_update_txn(&txn, cell_id, vertex_id, existing.as_ref(), &metadata, epoch, &mut generations).await?;
        remove_write_back_properties_txn(&txn, cell_id, vertex_id, &omitted, &mut generations).await?;
        commit_txn_strict(txn, self.await_durable_writes).await
    }

    /// Creates one standalone vertex: the Cypher `CREATE (n:Label {id: ..., ...})`
    /// with no relationship.
    ///
    /// Unlike the endpoints of a relationship `CREATE`, which merge into an
    /// existing vertex, this follows Cypher `CREATE` and the DataFusion route:
    /// an id that already exists is rejected with `node identity {id} already
    /// exists`. A retry with the same idempotency key and payload replays the
    /// original result instead, so a client that lost the response can resend.
    pub async fn create_vertex(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: VertexMetadata,
        idempotency_key: &str,
    ) -> Result<VertexCreateResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("idempotency_key", idempotency_key)?;
        validate_vertex_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "create_vertex")?;
        self.run_write_pipeline(async {
            let _permit = self
                .acquire_graph_write_permit("create_vertex")
                .instrument(
                    tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "create_vertex"),
                )
                .await?;
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self
                    .create_vertex_txn(cell_id, vertex_id, &metadata, idempotency_key)
                    .instrument(tracing::info_span!("storage.txn"))
                    .await
                {
                    Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        if !result.already_created {
                            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                        }
                        return Ok(result);
                    }
                    Err(err) => return Err(err),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
        })
        .await
    }

    async fn create_vertex_txn(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: &VertexMetadata,
        idempotency_key: &str,
    ) -> Result<VertexCreateResult> {
        let lock = self.acquire_local_write_guard_no_fence(cell_id, "create_vertex").await?;
        let result = self.create_vertex_txn_locked(cell_id, vertex_id, metadata, idempotency_key).await;
        finish_local_write(lock, result).await
    }

    async fn create_vertex_txn_locked(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: &VertexMetadata,
        idempotency_key: &str,
    ) -> Result<VertexCreateResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "create_vertex")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let fingerprint = vertex_create_fingerprint(cell_id, vertex_id, metadata);
        let idem_key = keys::idempotency(cell_id, "vertex-create", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            let replayed = decode_vertex_create_idempotency(&idem_key, cell_id, vertex_id, idempotency_key, fingerprint, &value)?;
            self.storage.refresh_writer_fence().await?;
            return Ok(replayed);
        }
        let vertex_key = keys::vertex(cell_id, vertex_id);
        if read_txn_remote(&txn, &vertex_key).await?.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "Cypher25",
                feature: format!("node identity {vertex_id} already exists"),
            });
        }
        self.validate_changing_write().await?;
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        let mut generations = WriteBackGenerations::default();
        apply_vertex_metadata_update_txn(&txn, cell_id, vertex_id, None, metadata, epoch, &mut generations).await?;
        txn.put(
            idem_key.as_bytes(),
            encode_vertex_create_idempotency(cell_id, vertex_id, fingerprint, epoch),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(VertexCreateResult {
            epoch,
            already_created: false,
        })
    }

    async fn set_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
        read: Option<&BTreeMap<VertexId, VertexMetadata>>,
    ) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "set_vertex_metadata_batch")
            .await?;
        let result = self.set_vertex_metadata_batch_txn_locked(cell_id, updates, read).await;
        finish_local_write(lock, result).await
    }

    async fn vertex_metadata_merges_are_noops(&self, cell_id: &str, updates: &[(VertexId, Vec<VertexMetadata>)]) -> Result<bool> {
        let Ok(_hydration_permit) = Arc::clone(&self.hydration_gate).try_acquire_owned() else {
            return Ok(false);
        };
        let snapshot = self.storage.writer()?.durable_snapshot().await?;
        let options = crate::codec::remote_read_options();
        for marker in [keys::cell_drop_marker(cell_id), keys::cell_drop_pending_marker(cell_id)] {
            if snapshot.get_with_options(marker.as_bytes(), &options).await?.is_some() {
                return Err(GraphError::CellDropped {
                    operation: "merge_vertex_metadata_batch",
                    cell_id: cell_id.to_string(),
                });
            }
        }
        self.ensure_write_authority(cell_id, "merge_vertex_metadata_batch")?;
        // A write-back property's current value is not in the record this
        // preflight compares with, so a patch naming one is left to the
        // transaction. One bloom-filtered point miss per distinct name in
        // the usual case, where the write-back never wrote it.
        let named = updates
            .iter()
            .flat_map(|(_, patches)| patches.iter().flat_map(|patch| patch.properties.keys()))
            .collect::<BTreeSet<_>>();
        for property in named {
            let catalog = keys::vertex_write_back_generation(cell_id, property);
            if snapshot.get_with_options(catalog.as_bytes(), &options).await?.is_some() {
                return Ok(false);
            }
        }
        let keys = updates
            .iter()
            .map(|(vertex_id, _)| keys::vertex(cell_id, *vertex_id))
            .collect::<Vec<_>>();
        let snapshot = &snapshot;
        let options = &options;
        let existing = stream::iter(keys.into_iter().map(|key| async move {
            let value = snapshot.get_with_options(key.as_bytes(), options).await?;
            Ok::<_, GraphError>((key, value))
        }))
        .buffer_unordered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
        .try_collect::<BTreeMap<_, _>>()
        .await?;
        for (vertex_id, patches) in updates {
            let key = keys::vertex(cell_id, *vertex_id);
            // Merging into an absent vertex creates it, even with an empty patch.
            let Some(previous) = decode_vertex_record(&key, existing.get(&key).and_then(Option::as_ref))? else {
                return Ok(false);
            };
            for patch in patches {
                if !patch.labels.is_subset(&previous.labels)
                    || patch
                        .properties
                        .iter()
                        .any(|(name, value)| previous.properties.get(name) != Some(value))
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn merge_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, Vec<VertexMetadata>)>,
        merge_policy: Option<&QueryBatchMergePolicy>,
    ) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "merge_vertex_metadata_batch")
            .await?;
        let result = self.merge_vertex_metadata_batch_txn_locked(cell_id, updates, merge_policy).await;
        finish_local_write(lock, result).await
    }

    async fn set_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
        read: Option<&BTreeMap<VertexId, VertexMetadata>>,
    ) -> Result<usize> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "set_vertex_metadata_batch").await?;
        let mut changed = Vec::new();
        // Properties a replacement drops that live in the write-back
        // layout: the record diff cannot see them, so they are removed
        // apart.
        let mut removed = Vec::new();
        let read_options = relationship_import_read_options(updates.len());
        // Keep the extra raw read buffers bounded independently of batch size.
        // All reads still belong to this serializable transaction and its guard.
        for chunk in updates.chunks(RELATIONSHIP_IMPORT_READ_CONCURRENCY) {
            let existing = read_txn_remote_many_with_options(
                &txn,
                chunk.iter().map(|(vertex, _)| keys::vertex(cell_id, *vertex)).collect(),
                &read_options,
            )
            .await?;
            for (vertex_id, metadata) in chunk {
                let vertex_key = keys::vertex(cell_id, *vertex_id);
                let existing = decode_vertex_record(&vertex_key, existing.get(&vertex_key).and_then(Option::as_ref))?;
                // A replacement is the vertex as reads will see it, and a
                // statement's `read` is what it saw: both are compared with
                // the record and its write-back values laid over it. The
                // record diff still starts from the record alone, since a
                // write-back value is never in it.
                let (seen, mut values) = overlaid_vertex_metadata_txn(&txn, cell_id, *vertex_id, existing.as_ref()).await?;
                if read.is_some_and(|read| seen.as_ref() != read.get(vertex_id)) {
                    return Err(GraphError::ConditionalWriteConflict {
                        operation: "swap_vertex_metadata_batch",
                        key: vertex_key,
                    });
                }
                if seen.as_ref() != Some(metadata) {
                    values.retain(|(property, _)| !metadata.properties.contains_key(property));
                    if !values.is_empty() {
                        removed.push((*vertex_id, values));
                    }
                    changed.push((*vertex_id, existing, metadata.clone()));
                }
            }
        }
        if changed.is_empty() {
            self.storage.refresh_writer_fence().await?;
            return Ok(0);
        }
        self.validate_changing_write().await?;
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        let mut generations = WriteBackGenerations::default();
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous.as_ref(), metadata, epoch, &mut generations).await?;
        }
        for (vertex_id, values) in &removed {
            remove_write_back_properties_txn(&txn, cell_id, *vertex_id, values, &mut generations).await?;
        }
        let changed_count = changed.len();
        commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
        Ok(changed_count)
    }

    async fn merge_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, Vec<VertexMetadata>)>,
        merge_policy: Option<&QueryBatchMergePolicy>,
    ) -> Result<usize> {
        let txn_started = std::time::Instant::now();
        let batch_items = updates.len() as u64;
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "merge_vertex_metadata_batch")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let read_started = std::time::Instant::now();
        let vertex_keys: Vec<String> = updates.iter().map(|(vertex_id, _)| keys::vertex(cell_id, *vertex_id)).collect();
        let existing = read_txn_remote_many(&txn, vertex_keys.iter().cloned()).await?;
        let mut changed = Vec::new();
        // Shared with the updates below, which look up the same names.
        let mut generations = WriteBackGenerations::default();
        for (vertex_id, patches) in updates {
            let vertex_key = keys::vertex(cell_id, vertex_id);
            let previous = existing
                .get(&vertex_key)
                .and_then(Option::as_ref)
                .map(|value| decode_vertex_metadata(&vertex_key, value))
                .transpose()?;
            // Merged into the vertex as reads see it, write-back values
            // included, so a merge guard compares against the current value,
            // and compared with that same view to tell whether it changed.
            let (merged, changes) = match &previous {
                Some(record) => {
                    let base = write_back_patch_base_txn(&txn, cell_id, vertex_id, record, &mut generations).await?;
                    let merged = apply_vertex_metadata_merges(&base, true, patches, merge_policy, &vertex_key)?;
                    let changes = merged != *base;
                    (merged, changes)
                }
                None => (
                    apply_vertex_metadata_merges(&VertexMetadata::default(), false, patches, merge_policy, &vertex_key)?,
                    true,
                ),
            };
            if changes {
                changed.push((vertex_id, previous, merged));
            }
        }
        let read_us = read_started.elapsed().as_micros() as u64;
        if changed.is_empty() {
            self.operation_metrics
                .merge_vertex_metadata_batches_profiled
                .fetch_add(1, Ordering::Relaxed);
            self.operation_metrics
                .merge_vertex_metadata_batch_items
                .fetch_add(batch_items, Ordering::Relaxed);
            self.operation_metrics
                .merge_vertex_metadata_read_us
                .fetch_add(read_us, Ordering::Relaxed);
            self.operation_metrics
                .merge_vertex_metadata_txn_us
                .fetch_add(txn_started.elapsed().as_micros() as u64, Ordering::Relaxed);
            self.operation_metrics
                .merge_vertex_metadata_nochange_exits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(0);
        }
        self.validate_changing_write().await?;
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous.as_ref(), metadata, epoch, &mut generations).await?;
        }
        let changed_count = changed.len();
        commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
        self.operation_metrics
            .merge_vertex_metadata_batches_profiled
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .merge_vertex_metadata_batch_items
            .fetch_add(batch_items, Ordering::Relaxed);
        self.operation_metrics
            .merge_vertex_metadata_read_us
            .fetch_add(read_us, Ordering::Relaxed);
        self.operation_metrics
            .merge_vertex_metadata_txn_us
            .fetch_add(txn_started.elapsed().as_micros() as u64, Ordering::Relaxed);
        Ok(changed_count)
    }

    async fn import_vertex_metadata_batch_txn(&self, cell_id: &str, updates: Vec<(VertexId, VertexMetadata)>) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "import_vertex_metadata_batch")
            .await?;
        let result = self.import_vertex_metadata_batch_txn_locked(cell_id, updates).await;
        finish_local_write(lock, result).await
    }

    async fn import_vertex_metadata_batch_txn_locked(&self, cell_id: &str, updates: Vec<(VertexId, VertexMetadata)>) -> Result<usize> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "import_vertex_metadata_batch").await?;
        let mut changed = Vec::new();
        for (vertex_id, metadata) in updates {
            let vertex_key = keys::vertex(cell_id, vertex_id);
            let existing = decode_vertex_record(&vertex_key, read_txn_remote(&txn, &vertex_key).await?.as_ref())?;
            if existing.as_ref() == Some(&metadata) {
                continue;
            }
            if existing.as_ref().is_some_and(|previous| *previous != VertexMetadata::default()) {
                return Err(GraphError::CorruptValue {
                    key: vertex_key,
                    reason: format!("vertex {vertex_id} already has different metadata during import"),
                });
            }
            changed.push((vertex_id, existing, metadata));
        }
        if changed.is_empty() {
            return Ok(0);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        let mut generations = WriteBackGenerations::default();
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous.as_ref(), metadata, epoch, &mut generations).await?;
        }
        let changed_count = changed.len();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed_count)
    }

    pub async fn delete_vertex(&self, cell_id: &str, vertex_id: VertexId, idempotency_key: &str) -> Result<VertexDeleteResult> {
        self.delete_vertex_with_options(cell_id, vertex_id, idempotency_key, false).await
    }

    pub async fn detach_delete_vertex(&self, cell_id: &str, vertex_id: VertexId, idempotency_key: &str) -> Result<VertexDeleteResult> {
        self.delete_vertex_with_options(cell_id, vertex_id, idempotency_key, true).await
    }

    pub(crate) async fn delete_vertex_mutations_batch(
        &self,
        cell_id: &str,
        deletions: Vec<(VertexId, String)>,
        detach: bool,
    ) -> Result<Vec<VertexDeleteResult>> {
        let mode = if detach {
            VertexDeleteBatchMode::Detach
        } else {
            VertexDeleteBatchMode::RejectConnected
        };
        let started = std::time::Instant::now();
        let result = self.delete_vertex_mutations_batch_with_mode(cell_id, deletions, mode).await;
        if detach {
            self.operation_metrics
                .detach_delete_vertices_batch_latency
                .record_micros(started.elapsed().as_micros() as u64);
        }
        result
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn delete_isolated_vertex_mutations_batch(
        &self,
        cell_id: &str,
        deletions: Vec<(VertexId, String, VertexMetadata)>,
    ) -> Result<Vec<VertexDeleteResult>> {
        self.delete_vertex_mutation_requests_batch_with_mode(
            cell_id,
            deletions
                .into_iter()
                .map(|(vertex_id, idempotency_key, constraints)| (vertex_id, idempotency_key, false, constraints))
                .collect(),
            VertexDeleteBatchMode::IsolatedOnly,
        )
        .await
    }

    async fn delete_vertex_mutations_batch_with_mode(
        &self,
        cell_id: &str,
        deletions: Vec<(VertexId, String)>,
        mode: VertexDeleteBatchMode,
    ) -> Result<Vec<VertexDeleteResult>> {
        let detach = mode == VertexDeleteBatchMode::Detach;
        self.delete_vertex_mutation_requests_batch_with_mode(
            cell_id,
            deletions
                .into_iter()
                .map(|(vertex_id, idempotency_key)| (vertex_id, idempotency_key, detach, VertexMetadata::default()))
                .collect(),
            mode,
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn delete_vertices_and_isolated_candidates_batch(
        &self,
        cell_id: &str,
        deletions: Vec<(VertexId, String, bool, VertexMetadata)>,
    ) -> Result<Vec<VertexDeleteResult>> {
        let started = std::time::Instant::now();
        let result = self
            .delete_vertex_mutation_requests_batch_with_mode(cell_id, deletions, VertexDeleteBatchMode::DetachAndIsolated)
            .await;
        self.operation_metrics
            .delete_vertices_and_isolated_candidates_batch_latency
            .record_micros(started.elapsed().as_micros() as u64);
        result
    }

    async fn delete_vertex_mutation_requests_batch_with_mode(
        &self,
        cell_id: &str,
        mut deletions: Vec<(VertexId, String, bool, VertexMetadata)>,
        mode: VertexDeleteBatchMode,
    ) -> Result<Vec<VertexDeleteResult>> {
        validate_component("cell_id", cell_id)?;
        let operation = mode.operation();
        self.ensure_write_authority(cell_id, operation)?;

        let mut unique_vertices = BTreeSet::new();
        deletions.retain(|(vertex_id, _, _, _)| unique_vertices.insert(*vertex_id));
        for (_, idempotency_key, _, _) in &deletions {
            validate_component("idempotency_key", idempotency_key)?;
        }
        ensure_limit(operation, deletions.len() as u64, self.limits.max_query_intermediate_rows as u64)?;
        if deletions.is_empty() {
            return Ok(Vec::new());
        }

        let span = write_txn_span(cell_id, None);
        self.run_write_pipeline(
            async {
                let mut retries = WriteRetryCount::new();
                for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                    // Bound both active S3 discovery and prepared results waiting for
                    // admission. No graph writer guard is held during this phase.
                    let preparation = async {
                        let mut preparation_slot = if self.writes_reverse_index() && deletions.len() <= DELETE_PREPARATION_MAX_VERTICES {
                            Some(
                                DELETE_PREPARATION_SLOTS
                                    .acquire()
                                    .instrument(tracing::info_span!("write.delete_preparation_admission"))
                                    .await
                                    .expect("static semaphore is never closed"),
                            )
                        } else {
                            None
                        };
                        let prepared = self
                            .prepare_vertex_delete(cell_id, &deletions, mode)
                            .instrument(tracing::info_span!(
                                "write.delete_prepare",
                                hydradb.delete.vertices = deletions.len(),
                                hydradb.delete.scanned_entries = tracing::field::Empty,
                                hydradb.delete.retained_edges = tracing::field::Empty,
                            ))
                            .await
                            .inspect_err(record_error_class)?;
                        // Fallback and replay-only deletes retain no prepared data.
                        // Do not charge their writer wait or guarded discovery against
                        // the process-wide preparation budget for unrelated graphs.
                        if prepared.is_none() {
                            drop(preparation_slot.take());
                        }
                        Ok::<_, GraphError>((preparation_slot, prepared))
                    };
                    // Only admission and read-only preparation may be dropped here.
                    // Client deadlines signal this token; do not give retries a new
                    // timeout or race cancellation against an applied transaction.
                    let (preparation_slot, prepared) = run_cancellable_delete_preparation(preparation).await?;
                    let _permit = self
                        .acquire_graph_write_permit(operation)
                        .instrument(
                            tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = operation),
                        )
                        .await?;
                    let _writer = self
                        .writer_lane(cell_id)
                        .lock()
                        .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                        .await;
                    let lock = self.acquire_local_write_guard_no_fence(cell_id, operation).await?;
                    let result = self
                        .delete_vertex_mutations_batch_txn_locked(cell_id, &deletions, mode, operation, &lock, prepared)
                        .instrument(tracing::info_span!("storage.txn"))
                        .await;
                    match finish_local_write(lock, result).await {
                        Err(err)
                            if (is_retryable_write_conflict(&err)
                                || matches!(
                                    &err,
                                    GraphError::ConditionalWriteConflict {
                                        operation: "validate_vertex_delete_preparation",
                                        ..
                                    }
                                ))
                                && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                        {
                            self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                            retries.note_retry();
                            drop(_writer);
                            drop(_permit);
                            drop(preparation_slot);
                            tokio::task::yield_now().await;
                        }
                        Ok(results) => {
                            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                            if let Some(epoch) = results.iter().map(|result| result.epoch).max() {
                                tracing::Span::current().record("hydradb.commit_epoch", epoch);
                            }
                            return Ok(results);
                        }
                        result => return result.inspect_err(record_error_class),
                    }
                }
                Err(GraphError::RetryExhausted {
                    operation: "graph transaction",
                    attempts: GRAPH_TXN_MAX_RETRIES,
                })
                .inspect_err(record_error_class)
            }
            .instrument(span),
        )
        .await
    }

    async fn prepare_vertex_delete(
        &self,
        cell_id: &str,
        deletions: &[(VertexId, String, bool, VertexMetadata)],
        mode: VertexDeleteBatchMode,
    ) -> Result<Option<PreparedVertexDelete>> {
        if !self.writes_reverse_index() || deletions.len() > DELETE_PREPARATION_MAX_VERTICES {
            return Ok(None);
        }
        let snapshot = self.snapshot(cell_id).await?;
        let probes = deletions
            .iter()
            .map(|(vertex, idempotency_key, detach, _)| {
                (
                    *vertex,
                    *detach,
                    keys::idempotency(cell_id, mode.idempotency_operation(), idempotency_key),
                )
            })
            .collect::<Vec<_>>();
        let pending = stream::iter(probes)
            .map(|(vertex, detach, key)| {
                let snapshot = &snapshot;
                async move {
                    tokio::task::consume_budget().await;
                    Ok::<_, GraphError>(
                        snapshot
                            .storage_snapshot
                            .get_with_options(key.as_bytes(), &slatedb::config::ReadOptions::default())
                            .await?
                            .is_none()
                            .then_some((vertex, detach)),
                    )
                }
                .boxed()
            })
            .buffer_unordered(VERTEX_DELETE_READ_CONCURRENCY)
            .boxed()
            .try_collect::<Vec<_>>()
            .await?;
        let vertices = pending.iter().flatten().map(|(vertex, _)| *vertex).collect::<BTreeSet<_>>();
        if vertices.is_empty() {
            return Ok(None);
        }
        let detach = pending
            .iter()
            .flatten()
            .filter(|(_, detach)| *detach)
            .map(|(vertex, _)| *vertex)
            .collect::<BTreeSet<_>>();
        let isolation_detach =
            matches!(mode, VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated).then_some(&detach);
        match self
            .indexed_vertex_delete_discovery(
                &snapshot,
                &vertices,
                isolation_detach,
                None,
                &slatedb::config::ScanOptions::default().with_cache_blocks(true),
            )
            .await
        {
            Ok(discovery) => {
                if !detach.is_empty() && mode != VertexDeleteBatchMode::RejectConnected {
                    self.warm_vertex_delete_apply_reads(&snapshot, &vertices, &detach, &discovery.edges)
                        .instrument(tracing::info_span!("write.delete_warm_apply_reads"))
                        .await?;
                }
                Ok(Some(PreparedVertexDelete {
                    vertices,
                    epoch: snapshot.read_epoch,
                    discovery,
                }))
            }
            // Very large cleanups retain the existing guarded path rather than
            // multiplying retained edge memory across preparing requests.
            Err(GraphError::AdmissionRejected {
                operation: "prepare_vertex_delete_bytes",
                ..
            }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn warm_vertex_delete_apply_reads(
        &self,
        snapshot: &GraphSnapshot<'_>,
        vertices: &BTreeSet<VertexId>,
        detach: &BTreeSet<VertexId>,
        edges: &BTreeSet<IncidentEdge>,
    ) -> Result<()> {
        // Discovery can find an incoming edge without reading its canonical
        // outgoing block. Warm the small cleanup's apply working set before
        // acquiring the writer gate; otherwise those cold point reads serialize
        // every writer. Values are discarded: the transaction still reads and
        // marks every key at its own snapshot, including concurrent updates.
        // Large cleanups keep the existing path and cannot sweep the cache.
        if !self.storage.has_block_cache()
            || vertices.len().saturating_add(edges.len().saturating_mul(5)).saturating_add(2) > DELETE_APPLY_WARM_MAX_KEYS
        {
            return Ok(());
        }
        let cell_id = &snapshot.cell_id;
        let mut keys = vertices
            .iter()
            .map(|vertex| keys::vertex(cell_id, *vertex))
            .collect::<BTreeSet<_>>();
        keys.extend([keys::cell_drop_marker(cell_id), keys::cell_drop_pending_marker(cell_id)]);
        for edge in edges {
            keys.extend([
                keys::out_edge(cell_id, &edge.edge_type, edge.src, edge.dst),
                keys::edge_metadata(cell_id, &edge.edge_type, edge.src, edge.dst),
                keys::degree_out(cell_id, &edge.edge_type, edge.src),
                keys::degree_in(cell_id, &edge.edge_type, edge.dst),
                keys::xlog_low_water(cell_id, &edge.edge_type),
            ]);
        }
        // The point warm and the relationship warm share one budget, so charge
        // the keys read here against it before handing the rest on.
        let relationship_budget = DELETE_APPLY_WARM_MAX_KEYS.saturating_sub(keys.len());
        stream::iter(keys)
            .map(|key| async move {
                tokio::task::consume_budget().await;
                snapshot
                    .storage_snapshot
                    .get_with_options(key.as_bytes(), &ReadOptions::default())
                    .await?;
                Ok::<_, GraphError>(())
            })
            .buffer_unordered(VERTEX_DELETE_READ_CONCURRENCY)
            .try_for_each(|()| async { Ok(()) })
            .await?;
        // The guarded delete finds each vertex's write-back values by a scan
        // of the vertex's value prefix (`delete_vertex_record_txn`). Warm that
        // prefix too, under the same contract, so the scan inside the writer
        // guard reads cached blocks; usually it finds nothing, but finding
        // nothing still reads the block that would hold it.
        stream::iter(vertices.iter().copied())
            .map(|vertex| async move {
                tokio::task::consume_budget().await;
                let prefix = keys::vertex_write_back_property_prefix(cell_id, vertex);
                let mut entries = snapshot
                    .storage_snapshot
                    .scan_prefix_with_options(
                        prefix.as_bytes(),
                        ..,
                        &slatedb::config::ScanOptions::default().with_cache_blocks(true),
                    )
                    .await?;
                while entries.next().await?.is_some() {}
                Ok::<_, GraphError>(())
            })
            .buffer_unordered(VERTEX_DELETE_READ_CONCURRENCY)
            .try_for_each(|()| async { Ok(()) })
            .await?;
        self.warm_vertex_delete_relationship_scans(snapshot, detach, edges, relationship_budget)
            .await;
        Ok(())
    }

    /// Warm the relationship-record prefix of every edge the guarded apply will
    /// detach. `prepare_vertex_delete_edge_txn` scans these prefixes inside the
    /// writer guard, and they were the one apply read the point warm above did
    /// not cover, so a cold prefix cost an object-store round trip per batch
    /// with every other writer queued behind it.
    ///
    /// Same contract as the point warm: values are discarded and the guarded
    /// transaction scans each prefix again at its own snapshot, so records
    /// written after this warm are still seen (they are in the memtable or a
    /// newer SST, which the guarded scan merges). Only immutable SST blocks are
    /// admitted here, so a cached block cannot go stale.
    ///
    /// `budget` is what the point warm left of `DELETE_APPLY_WARM_MAX_KEYS`, so
    /// the two together make at most that many reads before the guard; a cleanup
    /// that exhausts it stops warming and the guarded scan reads the rest as it
    /// always has.
    ///
    /// Every read is charged before it is made: the prefix seek, and each step
    /// of the iterator over what it returns. An empty prefix can still cost an
    /// object-store read — the discovery scan makes the same point — so charging
    /// only the records would let a detach of plain structural edges open a
    /// prefix per edge on an exhausted budget and warm nothing by it, and
    /// charging a record after reading it would let every concurrent scan admit
    /// one more block past the bound. `hydradb.warm.entries` stays the count of
    /// records read, with the seeks reported beside it.
    ///
    /// Warming cannot fail the delete. It fills the block cache and hands the
    /// transaction no data, so a read error here is recorded on the span and
    /// the prefix is left cold for the guarded scan to read.
    async fn warm_vertex_delete_relationship_scans(
        &self,
        snapshot: &GraphSnapshot<'_>,
        detach: &BTreeSet<VertexId>,
        edges: &BTreeSet<IncidentEdge>,
        budget: usize,
    ) {
        if budget == 0 {
            return;
        }
        let cell_id = &snapshot.cell_id;
        let prefixes = edges
            .iter()
            .filter(|edge| detach.contains(&edge.src) || detach.contains(&edge.dst))
            .map(|edge| keys::relationship_edge_prefix(cell_id, &edge.edge_type, edge.src, edge.dst))
            .collect::<BTreeSet<_>>();
        if prefixes.is_empty() {
            return;
        }
        let span = tracing::info_span!(
            "write.delete_warm_relationship_scans",
            hydradb.warm.prefixes = prefixes.len() as u64,
            hydradb.warm.budget = budget as u64,
            hydradb.warm.seeks = tracing::field::Empty,
            hydradb.warm.entries = tracing::field::Empty,
            hydradb.warm.truncated = tracing::field::Empty,
            hydradb.warm.failed = tracing::field::Empty,
        );
        // `spent` is the budget: seeks and records together. `seeks` and `entries`
        // are what the span reports.
        let spent = std::sync::atomic::AtomicUsize::new(0);
        let seeks = std::sync::atomic::AtomicUsize::new(0);
        let entries = std::sync::atomic::AtomicUsize::new(0);
        let truncated = std::sync::atomic::AtomicBool::new(false);
        let failed = std::sync::atomic::AtomicBool::new(false);
        let scan_options = ScanOptions::default().with_cache_blocks(true);
        stream::iter(prefixes)
            .map(|prefix| {
                let (spent, seeks, entries, truncated, failed, scan_options) =
                    (&spent, &seeks, &entries, &truncated, &failed, &scan_options);
                async move {
                    tokio::task::consume_budget().await;
                    if truncated.load(Ordering::Relaxed) {
                        return;
                    }
                    // Every read is charged before it is made, seeks and
                    // records alike, so the budget is a bound on reads rather
                    // than on what they returned: a charge landing exactly on the
                    // budget still gets its read, and the next one does not
                    // happen at all.
                    if spent.fetch_add(1, Ordering::Relaxed).saturating_add(1) > budget {
                        truncated.store(true, Ordering::Relaxed);
                        return;
                    }
                    seeks.fetch_add(1, Ordering::Relaxed);
                    let warm = async {
                        let mut iter = snapshot
                            .storage_snapshot
                            .scan_prefix_with_options(prefix.as_bytes(), .., scan_options)
                            .await?;
                        loop {
                            if spent.fetch_add(1, Ordering::Relaxed).saturating_add(1) > budget {
                                truncated.store(true, Ordering::Relaxed);
                                break;
                            }
                            if iter.next().await?.is_none() {
                                break;
                            }
                            entries.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok::<_, GraphError>(())
                    };
                    if let Err(error) = warm.await {
                        failed.store(true, Ordering::Relaxed);
                        tracing::debug!(%error, "relationship-scan warm failed; the guarded scan reads this prefix cold");
                    }
                }
            })
            .buffer_unordered(VERTEX_DELETE_READ_CONCURRENCY)
            .for_each(|()| async {})
            .instrument(span.clone())
            .await;
        span.record("hydradb.warm.seeks", seeks.load(Ordering::Relaxed) as u64);
        span.record("hydradb.warm.entries", entries.load(Ordering::Relaxed) as u64);
        span.record("hydradb.warm.truncated", truncated.load(Ordering::Relaxed));
        span.record("hydradb.warm.failed", failed.load(Ordering::Relaxed));
    }

    async fn vertex_delete_preparation_is_current(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        prepared: &PreparedVertexDelete,
    ) -> Result<bool> {
        let span = tracing::Span::current();
        span.record("hydradb.delete.prepared_epoch", prepared.epoch);
        span.record("hydradb.delete.validation_epoch", txn.seqnum());
        let outcome = |current: bool, reason: &'static str, scanned: u64| {
            span.record("hydradb.delete.validation_result", reason);
            span.record("hydradb.delete.validation_xlog_entries", scanned);
            current
        };
        if txn.seqnum() == prepared.epoch {
            return Ok(outcome(true, "same_epoch", 0));
        }
        let prefix = keys::matrix_dirty_prefix(cell_id);
        // These bounded validation scans recur under contention. Reuse their
        // immutable blocks within the existing shared cache budget.
        let scan_options = ScanOptions::default().with_cache_blocks(true);
        let mut iter = txn.scan_prefix_with_options(prefix.as_bytes(), .., &scan_options).await?;
        let mut expected = prepared.discovery.catalog.iter();
        let mut changed_types = Vec::new();
        while let Some(kv) = iter.next().await? {
            let Some((key, value)) = expected.next() else {
                return Ok(outcome(false, "edge_type_added", 0));
            };
            if key.as_bytes() != kv.key.as_ref() {
                return Ok(outcome(false, "edge_type_catalog_changed", 0));
            }
            if value != &kv.value {
                changed_types.push(key.strip_prefix(&prefix).expect("validated catalog key"));
            }
        }
        if expected.next().is_some() {
            return Ok(outcome(false, "edge_type_removed", 0));
        }
        drop(iter);
        // Independent edge-type histories must not serialize cold object reads
        // while the writer guard is held. Keep one budget across every scan;
        // bounded in-flight reads can overshoot it only by the concurrency.
        let changes_scanned = std::sync::atomic::AtomicU64::new(0);
        let scan_limit = self.limits.max_query_scan_edges.min(4096);
        let mut validations = stream::iter(changed_types)
            .map(|edge_type| {
                let changes_scanned = &changes_scanned;
                let scan_options = &scan_options;
                async move {
                    // Unrelated topology changes are safe only when retained
                    // history proves no requested vertex changed.
                    let low_key = keys::xlog_low_water(cell_id, edge_type);
                    let Some(low) = read_txn_remote(txn, &low_key).await? else {
                        return Ok::<_, GraphError>((false, "xlog_floor_missing"));
                    };
                    let from = prepared.epoch.saturating_add(1);
                    if decode_u64(&low_key, &low)? > from {
                        return Ok((false, "xlog_coverage_gap"));
                    }
                    let xlog_prefix = keys::xlog_type_prefix(cell_id, edge_type);
                    let mut delta = txn
                        .scan_prefix_with_options(
                            xlog_prefix.as_bytes(),
                            format!("{from:020}").into_bytes()..format!("{:020}", txn.seqnum().saturating_add(1)).into_bytes(),
                            scan_options,
                        )
                        .await?;
                    while let Some(change) = delta.next().await? {
                        let scanned = changes_scanned.fetch_add(1, Ordering::Relaxed) + 1;
                        if scanned > scan_limit {
                            return Ok((false, "xlog_scan_limit"));
                        }
                        let key = String::from_utf8_lossy(&change.key);
                        let (_, src, dst) = crate::shard::xlog::parse_xlog_entry_key(&xlog_prefix, &key)?;
                        crate::shard::xlog::decode_xlog_exists(&key, &change.value)?;
                        if prepared.vertices.contains(&src) || prepared.vertices.contains(&dst) {
                            return Ok((false, "target_topology_changed"));
                        }
                    }
                    Ok((true, "unchanged_targets"))
                }
                .instrument(tracing::info_span!("write.delete_validate_type", hydradb.edge_type = %edge_type))
                .boxed()
            })
            .buffered(VERTEX_DELETE_READ_CONCURRENCY)
            .boxed();
        while let Some((current, reason)) = validations.try_next().await? {
            if !current {
                return Ok(outcome(false, reason, changes_scanned.load(Ordering::Relaxed)));
            }
        }
        Ok(outcome(true, "unchanged_targets", changes_scanned.load(Ordering::Relaxed)))
    }

    async fn delete_vertex_mutations_batch_txn_locked(
        &self,
        cell_id: &str,
        deletions: &[(VertexId, String, bool, VertexMetadata)],
        mode: VertexDeleteBatchMode,
        operation: &'static str,
        lock: &LocalWriteGuard,
        prepared: Option<PreparedVertexDelete>,
    ) -> Result<Vec<VertexDeleteResult>> {
        let txn_started = std::time::Instant::now();
        let batch_items = deletions.len() as u64;
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let current_epoch = txn.seqnum();
        let commit_epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".to_string(),
            reason: "epoch overflow during vertex delete batch".to_string(),
        })?;

        let mut results = vec![None; deletions.len()];
        let read_started = std::time::Instant::now();
        let idem_keys: Vec<String> = deletions
            .iter()
            .map(|(_, idempotency_key, _, _)| keys::idempotency(cell_id, mode.idempotency_operation(), idempotency_key))
            .collect();
        let idem_values = read_txn_remote_many(&txn, idem_keys.iter().cloned()).await?;
        let mut non_replay_indices = Vec::new();
        for (index, (vertex_id, idempotency_key, _, _)) in deletions.iter().enumerate() {
            let idem_key = &idem_keys[index];
            if let Some(value) = idem_values.get(idem_key).and_then(Option::as_ref) {
                results[index] = Some(decode_vertex_delete_idempotency(
                    idem_key,
                    cell_id,
                    *vertex_id,
                    idempotency_key,
                    value,
                )?);
            } else {
                non_replay_indices.push(index);
            }
        }
        let vertex_keys: Vec<String> = non_replay_indices
            .iter()
            .map(|&index| keys::vertex(cell_id, deletions[index].0))
            .collect();
        let vertex_values = read_txn_remote_many(&txn, vertex_keys.iter().cloned()).await?;
        let mut pending = Vec::new();
        for (i, &index) in non_replay_indices.iter().enumerate() {
            let (vertex_id, idempotency_key, detach, constraints) = &deletions[index];
            let vertex_key = &vertex_keys[i];
            let previous = decode_vertex_record(vertex_key, vertex_values.get(vertex_key).and_then(Option::as_ref))?;
            pending.push((index, *vertex_id, idempotency_key.clone(), previous, *detach, constraints.clone()));
        }
        let read_us = read_started.elapsed().as_micros() as u64;
        if pending.is_empty() {
            // A replay has no durable WAL barrier to establish ownership.
            self.storage.refresh_writer_fence().await?;
            self.operation_metrics
                .delete_vertex_batch_batches_profiled
                .fetch_add(1, Ordering::Relaxed);
            self.operation_metrics
                .delete_vertex_batch_items
                .fetch_add(batch_items, Ordering::Relaxed);
            self.operation_metrics
                .delete_vertex_batch_read_us
                .fetch_add(read_us, Ordering::Relaxed);
            self.operation_metrics
                .delete_vertex_batch_txn_us
                .fetch_add(txn_started.elapsed().as_micros() as u64, Ordering::Relaxed);
            self.operation_metrics
                .delete_vertex_batch_all_replays
                .fetch_add(1, Ordering::Relaxed);
            return results
                .into_iter()
                .map(|result| {
                    result.ok_or_else(|| GraphError::CorruptValue {
                        key: "vertex-delete-batch".to_string(),
                        reason: "completed vertex deletion has no result".to_string(),
                    })
                })
                .collect();
        }

        let vertices = pending.iter().map(|(_, vertex_id, _, _, _, _)| *vertex_id).collect::<BTreeSet<_>>();
        let detach_vertices = pending
            .iter()
            .filter(|(_, _, _, _, detach, _)| *detach)
            .map(|(_, vertex_id, _, _, _, _)| *vertex_id)
            .collect::<BTreeSet<_>>();
        let write_reverse_index = self.writes_reverse_index();
        let incident_edges = if let Some(prepared) = prepared {
            if !async {
                if vertices != prepared.vertices {
                    tracing::Span::current().record("hydradb.delete.validation_result", "pending_vertices_changed");
                    tracing::Span::current().record("hydradb.delete.validation_xlog_entries", 0_u64);
                    return Ok(false);
                }
                self.vertex_delete_preparation_is_current(&txn, cell_id, &prepared).await
            }
            .instrument(tracing::info_span!(
                "write.delete_validate_preparation",
                hydradb.delete.prepared_epoch = prepared.epoch,
                hydradb.delete.validation_epoch = txn.seqnum(),
                hydradb.delete.validation_result = tracing::field::Empty,
                hydradb.delete.validation_xlog_entries = tracing::field::Empty,
            ))
            .await?
            {
                return Err(GraphError::ConditionalWriteConflict {
                    operation: "validate_vertex_delete_preparation",
                    key: cell_id.to_string(),
                });
            }
            prepared.discovery.edges
        } else if write_reverse_index {
            let isolation_detach_set =
                matches!(mode, VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated).then_some(&detach_vertices);
            self.indexed_vertex_delete_incidents_at(cell_id, &vertices, isolation_detach_set, current_epoch, lock)
                .instrument(tracing::info_span!(
                    "write.delete_incident_discovery",
                    hydradb.delete.vertices = vertices.len(),
                    hydradb.delete.detach_vertices = detach_vertices.len(),
                    hydradb.delete.scanned_entries = tracing::field::Empty,
                    hydradb.delete.retained_edges = tracing::field::Empty,
                ))
                .await?
        } else {
            self.full_scan_incident_edges_for_vertices_at(cell_id, &vertices, current_epoch, lock)
                .await?
        };
        if mode == VertexDeleteBatchMode::RejectConnected && !incident_edges.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "Graph",
                feature: format!(
                    "DELETE vertex batch requires DETACH because it has {} incident edge(s)",
                    incident_edges.len()
                ),
            });
        }

        let vertex_order = pending
            .iter()
            .enumerate()
            .map(|(pending_index, (_, vertex_id, _, _, _, _))| (*vertex_id, pending_index))
            .collect::<BTreeMap<_, _>>();
        let connected_vertices = incident_edges
            .iter()
            .filter(|edge| !detach_vertices.contains(&edge.src) && !detach_vertices.contains(&edge.dst))
            .flat_map(|edge| [edge.src, edge.dst])
            .filter(|vertex_id| vertices.contains(vertex_id))
            .collect::<BTreeSet<_>>();
        if matches!(mode, VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated) {
            let absent = VertexMetadata::default();
            let connected_constraint_mismatches = pending
                .iter()
                .filter(|(_, vertex_id, _, previous, detach, constraints)| {
                    !*detach
                        && connected_vertices.contains(vertex_id)
                        && !vertex_matches_path_node_constraints(*vertex_id, previous.as_ref().unwrap_or(&absent), constraints)
                })
                .count();
            if connected_constraint_mismatches > 0 {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "Graph",
                    feature: format!(
                        "DELETE vertex batch requires DETACH because {connected_constraint_mismatches} predicate-mismatched vertex/vertices have incident edges"
                    ),
                });
            }
        }
        let mut edge_counts = vec![0_u64; pending.len()];
        let mut relationship_counts = vec![0_u64; pending.len()];
        let mut out_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut segment_edges_by_type_src = BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
        let relationship_reads = std::sync::atomic::AtomicU64::new(0);
        let mut prepared_edges = stream::iter(
            incident_edges
                .into_iter()
                .filter(|edge| detach_vertices.contains(&edge.src) || detach_vertices.contains(&edge.dst))
                .enumerate(),
        )
        .map(|(index, edge)| {
            let txn = &txn;
            let relationship_reads = &relationship_reads;
            async move {
                let state =
                    prepare_vertex_delete_edge_txn(txn, cell_id, &edge, relationship_reads, self.limits.max_query_intermediate_rows)
                        .instrument(tracing::info_span!("write.delete_edge_prefetch", hydradb.cell_id = %cell_id))
                        .await?;
                Ok::<_, GraphError>((index, edge, state))
            }
        })
        .buffered(VERTEX_DELETE_READ_CONCURRENCY);
        while let Some((edge_index, edge, (canonical, previous_edge_metadata, relationships))) = prepared_edges.try_next().await? {
            renew_vertex_delete_lock_after_items(lock, edge_index as u64).await?;
            let owner = [edge.src, edge.dst]
                .into_iter()
                .filter(|vertex_id| detach_vertices.contains(vertex_id))
                .filter_map(|vertex_id| vertex_order.get(&vertex_id).copied())
                .min()
                .ok_or_else(|| GraphError::CorruptValue {
                    key: format!("edge/{}/{}/{}", edge.edge_type, edge.src, edge.dst),
                    reason: "incident edge is not owned by a requested vertex".to_string(),
                })?;
            let mutation = EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge.edge_type,
                src: edge.src,
                dst: edge.dst,
                idempotency_key: format!("{}.detach-edge.{}.{}.{}", pending[owner].2, edge.src, edge.dst, edge_index),
            };

            let out_edge_key = keys::out_edge(cell_id, &mutation.edge_type, edge.src, edge.dst);
            let segment_epoch = if canonical || write_reverse_index {
                None
            } else {
                let cache_key = (mutation.edge_type.clone(), edge.src);
                match segment_edges_by_type_src.get(&cache_key) {
                    Some(edges) => edges.get(&edge.dst).copied(),
                    None => {
                        let edges = out_segment_edges_for_src_txn(&txn, cell_id, &mutation.edge_type, edge.src, current_epoch).await?;
                        let epoch = edges.get(&edge.dst).copied();
                        segment_edges_by_type_src.insert(cache_key, edges);
                        epoch
                    }
                }
            };
            let relationships = delete_prepared_relationships_for_structural_edge_txn(&txn, &mutation, &relationships)?;
            if canonical || segment_epoch.is_some() {
                self.mark_topology_change_txn(&txn, cell_id, &mutation.edge_type, commit_epoch, &[(edge.src, edge.dst, false)])
                    .await?;
                if !previous_edge_metadata.properties.is_empty() {
                    apply_edge_metadata_update_txn(
                        &txn,
                        EdgeMetadataTarget {
                            cell_id,
                            edge_type: &mutation.edge_type,
                            src: edge.src,
                            dst: edge.dst,
                        },
                        &previous_edge_metadata,
                        &EdgeMetadata::default(),
                        commit_epoch,
                    )?;
                }

                if canonical {
                    txn.delete(keys::edge(cell_id, &mutation.edge_type, edge.src, edge.dst).as_bytes())?;
                    txn.delete(out_edge_key.as_bytes())?;
                    if write_reverse_index {
                        txn.delete(keys::in_edge(cell_id, &mutation.edge_type, edge.dst, edge.src).as_bytes())?;
                        *in_decrements.entry((mutation.edge_type.clone(), edge.dst)).or_insert(0) += 1;
                    }
                } else {
                    txn.put(
                        keys::out_segment_tombstone(cell_id, &mutation.edge_type, edge.src, edge.dst).as_bytes(),
                        encode_u64(commit_epoch),
                    )?;
                }
                *out_decrements.entry((mutation.edge_type.clone(), edge.src)).or_insert(0) += 1;
            }
            edge_counts[owner] = edge_counts[owner].saturating_add(1);
            relationship_counts[owner] = relationship_counts[owner].saturating_add(relationships);
        }
        drop(prepared_edges);

        let counter_keys: Vec<String> = out_decrements
            .keys()
            .map(|(edge_type, src)| keys::degree_out(cell_id, edge_type, *src))
            .chain(
                in_decrements
                    .keys()
                    .map(|(edge_type, dst)| keys::degree_in(cell_id, edge_type, *dst)),
            )
            .collect();
        let counter_values = read_txn_remote_many(&txn, counter_keys.iter().cloned()).await?;
        for ((edge_type, src), decrement) in out_decrements {
            let key = keys::degree_out(cell_id, &edge_type, src);
            let base = match counter_values.get(&key).and_then(Option::as_ref) {
                Some(value) => decode_u64(&key, value)?,
                None => 0,
            };
            txn.put(key.as_bytes(), encode_u64(base.saturating_sub(decrement)))?;
        }
        for ((edge_type, dst), decrement) in in_decrements {
            let key = keys::degree_in(cell_id, &edge_type, dst);
            let base = match counter_values.get(&key).and_then(Option::as_ref) {
                Some(value) => decode_u64(&key, value)?,
                None => 0,
            };
            txn.put(key.as_bytes(), encode_u64(base.saturating_sub(decrement)))?;
        }

        let mut generations = WriteBackGenerations::default();
        for (pending_index, (result_index, vertex_id, idempotency_key, previous, detach, constraints)) in pending.into_iter().enumerate() {
            let existed = previous.is_some();
            let previous = previous.unwrap_or_default();
            let isolation_mode = matches!(mode, VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated);
            let matches_pattern = vertex_matches_path_node_constraints(vertex_id, &previous, &constraints);
            let connected = !detach && isolation_mode && matches_pattern && connected_vertices.contains(&vertex_id);
            // An isolated candidate is the node of a `MATCH ... WHERE NOT (n)--()`
            // pattern, so one its pattern does not match is not deleted. An empty
            // record is a vertex too: deleting it is a deletion.
            let selected = detach || !isolation_mode || matches_pattern;
            let vertex_deleted = existed && selected && !connected;
            if vertex_deleted {
                delete_vertex_record_txn(&txn, cell_id, vertex_id, &previous, &mut generations).await?;
            }
            let changed = vertex_deleted || edge_counts[pending_index] > 0 || relationship_counts[pending_index] > 0;
            let result = VertexDeleteResult {
                epoch: if changed { commit_epoch } else { current_epoch },
                vertex_deleted,
                incident_edges_deleted: edge_counts[pending_index],
                relationships_deleted: relationship_counts[pending_index],
            };
            txn.put(
                keys::idempotency(cell_id, mode.idempotency_operation(), &idempotency_key).as_bytes(),
                encode_vertex_delete_idempotency(cell_id, vertex_id, &result),
            )?;
            results[result_index] = Some(result);
        }

        // Every pending deletion writes an idempotency record, including an
        // absent vertex. Its durable WAL commit fences a superseded writer.
        self.validate_changing_write().await?;
        commit_txn_traced(txn, self.await_durable_writes, cell_id, "", commit_epoch).await?;
        self.operation_metrics
            .delete_vertex_batch_batches_profiled
            .fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .delete_vertex_batch_items
            .fetch_add(batch_items, Ordering::Relaxed);
        self.operation_metrics
            .delete_vertex_batch_read_us
            .fetch_add(read_us, Ordering::Relaxed);
        self.operation_metrics
            .delete_vertex_batch_txn_us
            .fetch_add(txn_started.elapsed().as_micros() as u64, Ordering::Relaxed);
        results
            .into_iter()
            .map(|result| {
                result.ok_or_else(|| GraphError::CorruptValue {
                    key: "vertex-delete-batch".to_string(),
                    reason: "vertex deletion has no result".to_string(),
                })
            })
            .collect()
    }

    async fn delete_vertex_with_options(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
        detach: bool,
    ) -> Result<VertexDeleteResult> {
        let mut results = self
            .delete_vertex_mutations_batch(cell_id, vec![(vertex_id, idempotency_key.to_string())], detach)
            .await?;
        Ok(results.pop().expect("one requested vertex has one deletion result"))
    }

    async fn indexed_vertex_delete_incidents_at(
        &self,
        cell_id: &str,
        vertex_ids: &BTreeSet<VertexId>,
        isolation_detach_set: Option<&BTreeSet<VertexId>>,
        read_epoch: StorageSequence,
        lock: &LocalWriteGuard,
    ) -> Result<BTreeSet<IncidentEdge>> {
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let discovery = self
            .indexed_vertex_delete_discovery(
                &snapshot,
                vertex_ids,
                isolation_detach_set,
                Some(lock),
                &slatedb::config::ScanOptions::default().with_cache_blocks(true),
            )
            .await?;
        drop(discovery.catalog);
        Ok(discovery.edges)
    }

    async fn indexed_vertex_delete_discovery(
        &self,
        snapshot: &GraphSnapshot<'_>,
        vertex_ids: &BTreeSet<VertexId>,
        isolation_detach_set: Option<&BTreeSet<VertexId>>,
        lock: Option<&LocalWriteGuard>,
        scan_options: &slatedb::config::ScanOptions,
    ) -> Result<VertexDeleteDiscovery> {
        let cell_id = &snapshot.cell_id;
        // Adjacent vertex/type prefixes often share immutable SST blocks. These
        // discovery reads need reuse, unlike one-pass relationship imports.
        // Admission stays bounded by the existing process-wide SlateDB cache.
        let edge_type_prefix = keys::matrix_dirty_prefix(cell_id);
        let mut edge_type_iter = snapshot
            .storage_snapshot
            .scan_prefix_with_options(edge_type_prefix.as_bytes(), .., scan_options)
            .await?;
        let mut edge_types = BTreeSet::new();
        let mut catalog = BTreeMap::new();
        let retained_bytes = std::sync::atomic::AtomicU64::new(0);
        let mut scanned = 0_u64;
        while let Some(kv) = edge_type_iter.next().await? {
            tokio::task::consume_budget().await;
            scanned = scanned.saturating_add(1);
            if let Some(lock) = lock {
                renew_vertex_delete_lock_after_items(lock, scanned).await?;
            }
            ensure_limit("delete_vertex_scan_incident_edges", scanned, self.limits.max_query_scan_edges)?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge_type = key
                .strip_prefix(&edge_type_prefix)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| GraphError::CorruptValue {
                    key: key.clone(),
                    reason: "matrix dirty key has no edge type".to_string(),
                })?;
            validate_component("edge_type", edge_type)?;
            edge_types.insert(edge_type.to_string());
            let bytes = retained_bytes.fetch_add((key.len() * 2 + kv.value.len() + 128) as u64, Ordering::Relaxed)
                + (key.len() * 2 + kv.value.len() + 128) as u64;
            if lock.is_none() {
                ensure_limit("prepare_vertex_delete_bytes", bytes, DELETE_PREPARATION_BYTES)?;
            }
            // Do not pin a whole SST block through an eight-byte Bytes slice
            // while prepared results wait for writer admission.
            catalog.insert(key, bytes::Bytes::copy_from_slice(&kv.value));
        }

        // Empty prefix seeks can require object-store I/O too. Keep one bounded
        // fan-out across the entire batch, using the same pinned snapshot and
        // shared scan/retention limits, including when preparing without a guard.
        let scanned = std::sync::atomic::AtomicU64::new(scanned);
        let edges = Mutex::new(BTreeSet::new());
        type Prefixes<'a> = Box<dyn Iterator<Item = String> + Send + 'a>;
        type ScanJob<'a> = (Prefixes<'a>, bool);
        let jobs: Box<dyn Iterator<Item = ScanJob<'_>> + Send + '_> = Box::new(vertex_ids.iter().flat_map(|vertex| {
            let prefixes: Prefixes<'_> = Box::new(edge_types.iter().flat_map(move |edge_type| {
                [
                    keys::out_prefix(cell_id, edge_type, *vertex),
                    keys::in_prefix(cell_id, edge_type, *vertex),
                ]
                .into_iter()
            }));
            let witness_only = isolation_detach_set.is_some_and(|detach| !detach.contains(vertex));
            // Do not speculate past a candidate's first surviving edge.
            // Detach needs every prefix; isolation needs one sequential
            // search per candidate, overlapped with other candidates.
            let jobs: Box<dyn Iterator<Item = ScanJob<'_>> + Send + '_> = if witness_only {
                Box::new(std::iter::once((prefixes, true)))
            } else {
                Box::new(prefixes.map(|prefix| (Box::new(std::iter::once(prefix)) as Prefixes<'_>, false)))
            };
            jobs
        }));
        let mut scans = stream::iter(jobs)
            .map(|(prefixes, witness_only)| {
                let snapshot = &snapshot;
                let scanned = &scanned;
                let edges = &edges;
                let retained_bytes = &retained_bytes;
                async move {
                    'prefixes: for prefix in prefixes {
                        tokio::task::consume_budget().await;
                        let mut iter = snapshot
                            .storage_snapshot
                            .scan_prefix_with_options(prefix.as_bytes(), .., scan_options)
                            .await?;
                        while let Some(kv) = iter.next().await? {
                            tokio::task::consume_budget().await;
                            let count = scanned.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                            if let Some(lock) = lock {
                                renew_vertex_delete_lock_after_items(lock, count).await?;
                            }
                            ensure_limit("delete_vertex_scan_incident_edges", count, self.limits.max_query_scan_edges)?;
                            let key = String::from_utf8_lossy(&kv.key).into_owned();
                            let record = decode_edge_record(&key, &kv.value)?;
                            // Detach vertices own all removed edges. For an isolated
                            // candidate, only one edge surviving that detach is needed.
                            if witness_only
                                && isolation_detach_set.is_some_and(|detach| detach.contains(&record.src) || detach.contains(&record.dst))
                            {
                                continue;
                            }
                            let mut edges = edges.lock().await;
                            let edge = IncidentEdge {
                                edge_type: record.edge_type,
                                src: record.src,
                                dst: record.dst,
                            };
                            if lock.is_none() && !edges.contains(&edge) {
                                let size = (edge.edge_type.len() + 128) as u64;
                                let bytes = retained_bytes.fetch_add(size, Ordering::Relaxed) + size;
                                ensure_limit("prepare_vertex_delete_bytes", bytes, DELETE_PREPARATION_BYTES)?;
                            }
                            edges.insert(edge);
                            ensure_limit(
                                "delete_vertex_incident_edges",
                                edges.len() as u64,
                                self.limits.max_query_intermediate_rows as u64,
                            )?;
                            if witness_only {
                                break 'prefixes;
                            }
                        }
                    }
                    Ok::<_, GraphError>(())
                }
                .boxed()
            })
            .buffer_unordered(VERTEX_DELETE_READ_CONCURRENCY)
            .boxed();
        while scans.try_next().await?.is_some() {}
        drop(scans);
        let scanned = scanned.load(Ordering::Relaxed);
        let edges = edges.into_inner();

        tracing::Span::current().record("hydradb.delete.scanned_entries", scanned);
        tracing::Span::current().record("hydradb.delete.retained_edges", edges.len() as u64);
        ensure_limit(
            "delete_vertex_incident_edges",
            edges.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        Ok(VertexDeleteDiscovery { edges, catalog })
    }

    async fn full_scan_incident_edges_for_vertices_at(
        &self,
        cell_id: &str,
        vertex_ids: &BTreeSet<VertexId>,
        read_epoch: StorageSequence,
        lock: &LocalWriteGuard,
    ) -> Result<BTreeSet<IncidentEdge>> {
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let scan_options = slatedb::config::ScanOptions::default();
        let read_options = slatedb::config::ReadOptions::default();
        let mut edges = BTreeSet::new();
        let mut scanned = 0_u64;

        let mut out_iter = snapshot
            .storage_snapshot
            .scan_prefix_with_options(keys::out_edge_cell_prefix(cell_id).as_bytes(), .., &scan_options)
            .await?;
        while let Some(kv) = out_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit("delete_vertex_scan_edges", scanned, self.limits.max_query_scan_edges)?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = parse_edge_record_key(&key)?;
            if vertex_ids.contains(&record.src) || vertex_ids.contains(&record.dst) {
                edges.insert(IncidentEdge {
                    edge_type: record.edge_type,
                    src: record.src,
                    dst: record.dst,
                });
            }
        }

        let mut relationship_iter = snapshot
            .storage_snapshot
            .scan_prefix_with_options(keys::relationship_cell_prefix(cell_id).as_bytes(), .., &scan_options)
            .await?;
        while let Some(kv) = relationship_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit("delete_vertex_scan_relationships", scanned, self.limits.max_query_scan_edges)?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            if vertex_ids.contains(&record.src) || vertex_ids.contains(&record.dst) {
                edges.insert(IncidentEdge {
                    edge_type: record.edge_type,
                    src: record.src,
                    dst: record.dst,
                });
            }
        }

        let mut segment_iter = snapshot
            .storage_snapshot
            .scan_prefix_with_options(keys::out_segment_cell_prefix(cell_id).as_bytes(), .., &scan_options)
            .await?;
        while let Some(kv) = segment_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit("delete_vertex_scan_segments", scanned, self.limits.max_query_scan_edges)?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > read_epoch {
                continue;
            }
            for dst in segment.destinations {
                if !vertex_ids.contains(&segment.src) && !vertex_ids.contains(&dst) {
                    continue;
                }
                let tombstone_key = keys::out_segment_tombstone(cell_id, &segment.edge_type, segment.src, dst);
                let tombstone_epoch = match snapshot
                    .storage_snapshot
                    .get_with_options(tombstone_key.as_bytes(), &read_options)
                    .await?
                {
                    Some(value) => Some(decode_u64(&tombstone_key, &value)?),
                    None => None,
                };
                if segment_edge_visible(segment.storage_sequence, tombstone_epoch) {
                    edges.insert(IncidentEdge {
                        edge_type: segment.edge_type.clone(),
                        src: segment.src,
                        dst,
                    });
                }
            }
        }

        ensure_limit(
            "delete_vertex_incident_edges",
            edges.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        Ok(edges)
    }

    pub async fn drop_cell(&self, cell_id: &str, idempotency_key: &str) -> Result<GraphCellDropResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "drop_cell")?;

        let _permit = self
            .acquire_graph_write_permit("drop_cell")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "drop_cell"))
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            let lock = self.acquire_local_write_guard(cell_id, "drop_cell").await?;
            let result = self
                .drop_cell_locked(cell_id, idempotency_key, &lock)
                .instrument(tracing::info_span!("storage.txn"))
                .await;
            match finish_local_write(lock, result).await {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn drop_cell_locked(&self, cell_id: &str, idempotency_key: &str, lock: &LocalWriteGuard) -> Result<GraphCellDropResult> {
        let idem_key = keys::cell_drop_idempotency(cell_id, idempotency_key);
        let marker_key = keys::cell_drop_marker(cell_id);
        let pending_marker_key = keys::cell_drop_pending_marker(cell_id);
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_cell_drop_idempotency(&idem_key, cell_id, idempotency_key, &value);
        }
        if let Some(value) = read_txn_remote(&txn, &marker_key).await? {
            let result = GraphCellDropResult {
                marker_epoch: decode_u64(&marker_key, &value)?,
                deleted_keys: 0,
                batches: 0,
                already_dropped: true,
            };
            txn.put(idem_key.as_bytes(), encode_cell_drop_idempotency(cell_id, idempotency_key, &result))?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        }
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell").await?;
        let marker_epoch = match read_txn_remote(&txn, &pending_marker_key).await? {
            Some(value) => decode_u64(&pending_marker_key, &value)?,
            None => {
                let epoch = txn.seqnum().saturating_add(1);
                txn.put(pending_marker_key.as_bytes(), encode_u64(epoch))?;
                epoch
            }
        };
        commit_txn_strict(txn, self.await_durable_writes).await?;

        let mut deleted_keys = 0_u64;
        let mut batches = 0_u64;
        let mut pending = Vec::new();
        let mut iter = self.scan_remote_prefix(&keys::cell_prefix(cell_id)).await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            if key == pending_marker_key {
                continue;
            }
            pending.push(key);
            if pending.len() >= GRAPH_MAINTENANCE_BATCH_KEYS {
                lock.renew().await?;
                let deleted = self.flush_drop_cell_batch(cell_id, &mut pending).await?;
                lock.renew().await?;
                deleted_keys = deleted_keys.saturating_add(deleted);
                batches = batches.saturating_add(1);
            }
        }
        if !pending.is_empty() {
            lock.renew().await?;
            let deleted = self.flush_drop_cell_batch(cell_id, &mut pending).await?;
            lock.renew().await?;
            deleted_keys = deleted_keys.saturating_add(deleted);
            batches = batches.saturating_add(1);
        }

        let result = GraphCellDropResult {
            marker_epoch,
            deleted_keys,
            batches,
            already_dropped: false,
        };
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell").await?;
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_cell_drop_idempotency(&idem_key, cell_id, idempotency_key, &value);
        }
        txn.put(marker_key.as_bytes(), encode_u64(marker_epoch))?;
        txn.delete(pending_marker_key.as_bytes())?;
        txn.put(idem_key.as_bytes(), encode_cell_drop_idempotency(cell_id, idempotency_key, &result))?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    async fn flush_drop_cell_batch(&self, cell_id: &str, keys_to_delete: &mut Vec<String>) -> Result<u64> {
        if keys_to_delete.is_empty() {
            return Ok(0);
        }
        let keys = std::mem::take(keys_to_delete);
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell").await?;
        for key in &keys {
            txn.delete(key.as_bytes())?;
        }
        let deleted = keys.len() as u64;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(deleted)
    }

    pub async fn set_edge_metadata(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let _latency = self.operation_metrics.set_edge_metadata_latency.start_timer();
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_edge_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_edge_metadata")?;
        self.run_write_pipeline_fencing_noops(self.set_edge_metadata_inline(cell_id, edge_type, src, dst, metadata))
            .await
    }

    async fn set_edge_metadata_inline(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let _permit = self
            .acquire_graph_write_permit("set_edge_metadata")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_edge_metadata"),
            )
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_edge_metadata_txn(cell_id, edge_type, src, dst, metadata.clone())
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn set_edge_metadata_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        let _latency = self.operation_metrics.set_edge_metadata_batch_latency.start_timer();
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "set_edge_metadata_batch")?;
        let updates = coalesce_edge_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "set_edge_metadata_batch",
            updates.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        self.run_write_pipeline_fencing_noops(self.set_edge_metadata_batch_inline(cell_id, edge_type, updates))
            .await
    }

    async fn set_edge_metadata_batch_inline(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: Vec<(VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        let _permit = self
            .acquire_graph_write_permit("set_edge_metadata_batch")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_edge_metadata_batch"),
            )
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_edge_metadata_batch_txn(cell_id, edge_type, updates.clone())
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(changed) => {
                    if changed > 0 {
                        self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn import_relationships_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = RelationshipMutation>,
        idempotency_key: &str,
    ) -> Result<RelationshipImportResult> {
        self.import_relationships_batch_with_endpoint_labels(
            cell_id,
            edge_type,
            relationships.into_iter().collect(),
            idempotency_key,
            RelationshipImportOptions {
                endpoint_labels: None,
                create_always: false,
                update_existing_metadata: false,
                merge_policy: None,
                #[cfg(feature = "opencypher")]
                guarded_rows: None,
                operation: "import_relationships_batch",
            },
        )
        .await
    }

    /// Installs the unique relationship-identity point indexes for a verified
    /// offline import batch. Ordinary CREATE/import paths deliberately remove
    /// these accelerators because they cannot prove uniqueness. An offline
    /// image builder can: every supplied relationship id is its persisted
    /// `id` property, and this method validates the durable record before it
    /// publishes the pointer.
    pub async fn install_relationship_merge_indexes_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = RelationshipMutation>,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_write_authority(cell_id, "install_relationship_merge_indexes_batch")?;
        let relationships = relationships.into_iter().collect::<Vec<_>>();
        if relationships.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "install_relationship_merge_indexes_batch",
            relationships.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let _permit = self
            .acquire_graph_write_permit("install_relationship_merge_indexes_batch")
            .instrument(tracing::info_span!(
                "shard.write_permit",
                hydradb.cell_id = %cell_id,
                hydradb.write.operation = "install_relationship_merge_indexes_batch"
            ))
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .install_relationship_merge_indexes_batch_txn(cell_id, edge_type, &relationships)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(error) if is_retryable_write_conflict(&error) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(installed) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(installed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "install relationship merge indexes",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn install_relationship_merge_indexes_batch_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
    ) -> Result<u64> {
        self.validate_changing_write().await?;
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "install_relationship_merge_indexes_batch")
            .await?;
        let record_keys = relationships
            .iter()
            .map(|relationship| keys::relationship(cell_id, edge_type, relationship.src, relationship.dst, relationship.relationship_id))
            .collect::<Vec<_>>();
        let records = read_txn_remote_many(&txn, record_keys.iter().cloned()).await?;
        let mut pointers = BTreeMap::<String, RelationshipId>::new();
        for (relationship, record_key) in relationships.iter().zip(record_keys) {
            if relationship.cell_id != cell_id || relationship.edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key: record_key,
                    reason: "offline relationship identity does not match the batch".into(),
                });
            }
            let expected_identity = VertexPropertyValue::Integer(relationship.relationship_id);
            if relationship.metadata.properties.get(RELATIONSHIP_IDENTITY_PROPERTY) != Some(&expected_identity) {
                return Err(GraphError::CorruptValue {
                    key: record_key,
                    reason: "offline relationship id property does not match its stable id".into(),
                });
            }
            let value = records
                .get(&record_key)
                .and_then(Option::as_ref)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: record_key.clone(),
                    reason: "offline relationship record is missing".into(),
                })?;
            let persisted = decode_relationship_record(&record_key, value)?;
            if persisted.cell_id != relationship.cell_id
                || persisted.edge_type != relationship.edge_type
                || persisted.src != relationship.src
                || persisted.dst != relationship.dst
                || persisted.relationship_id != relationship.relationship_id
                || persisted.metadata != relationship.metadata
            {
                return Err(GraphError::CorruptValue {
                    key: record_key,
                    reason: "offline relationship record differs from the verified input".into(),
                });
            }
            let pointer_key = keys::relationship_merge_index(
                cell_id,
                edge_type,
                RELATIONSHIP_IDENTITY_PROPERTY,
                &encode_vertex_property_value_key(&expected_identity),
                relationship.src,
                relationship.dst,
            );
            if let Some(existing) = pointers.insert(pointer_key.clone(), relationship.relationship_id) {
                if existing != relationship.relationship_id {
                    return Err(GraphError::CorruptValue {
                        key: pointer_key,
                        reason: "offline relationship identity is not unique".into(),
                    });
                }
            }
        }
        let pointer_values = read_txn_remote_many(&txn, pointers.keys().cloned()).await?;
        for (pointer_key, relationship_id) in &pointers {
            if let Some(existing) = pointer_values.get(pointer_key).and_then(Option::as_ref) {
                let existing = decode_u64(pointer_key, existing)?;
                if existing != *relationship_id {
                    return Err(GraphError::CorruptValue {
                        key: pointer_key.clone(),
                        reason: format!("offline relationship identity already points to {existing}, expected {relationship_id}"),
                    });
                }
            }
            txn.put(pointer_key.as_bytes(), encode_u64(*relationship_id))?;
        }
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(pointers.len() as u64)
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn create_relationships_batch_between_labeled_vertices(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = RelationshipMutation>,
        idempotency_key: &str,
        source_label: &str,
        destination_label: &str,
    ) -> Result<RelationshipImportResult> {
        validate_component("source_label", source_label)?;
        validate_component("destination_label", destination_label)?;
        let started = std::time::Instant::now();
        let result = self
            .import_relationships_batch_with_endpoint_labels(
                cell_id,
                edge_type,
                relationships.into_iter().collect(),
                idempotency_key,
                RelationshipImportOptions {
                    endpoint_labels: Some((source_label, destination_label)),
                    create_always: true,
                    update_existing_metadata: false,
                    merge_policy: None,
                    #[cfg(feature = "opencypher")]
                    guarded_rows: None,
                    operation: "create_relationships_batch_between_labeled_vertices",
                },
            )
            .await;
        self.operation_metrics
            .create_relationships_batch_latency
            .record_micros(started.elapsed().as_micros() as u64);
        result
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn merge_relationships_batch_between_labeled_vertices(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = RelationshipMutation>,
        idempotency_key: &str,
        endpoint_labels: (&str, &str),
        merge_policy: Option<&QueryBatchMergePolicy>,
    ) -> Result<RelationshipImportResult> {
        let (source_label, destination_label) = endpoint_labels;
        validate_component("source_label", source_label)?;
        validate_component("destination_label", destination_label)?;
        let started = std::time::Instant::now();
        let result = self
            .import_relationships_batch_with_endpoint_labels(
                cell_id,
                edge_type,
                relationships.into_iter().collect(),
                idempotency_key,
                RelationshipImportOptions {
                    endpoint_labels: Some((source_label, destination_label)),
                    create_always: false,
                    update_existing_metadata: true,
                    merge_policy,
                    #[cfg(feature = "opencypher")]
                    guarded_rows: None,
                    operation: "merge_relationships_batch_between_labeled_vertices",
                },
            )
            .await;
        self.operation_metrics
            .merge_relationships_batch_latency
            .record_micros(started.elapsed().as_micros() as u64);
        result
    }

    async fn import_relationships_batch_with_endpoint_labels(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: Vec<RelationshipMutation>,
        idempotency_key: &str,
        options: RelationshipImportOptions<'_>,
    ) -> Result<RelationshipImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if let Some(policy) = options.merge_policy {
            policy.validate()?;
        }
        self.ensure_write_authority(cell_id, options.operation)?;

        let guarded_rows = mixed_guarded_relationship_rows(&relationships, options.merge_policy, self.limits.max_bulk_import_edges)?;
        let ordered_fingerprint = if guarded_rows.is_empty() {
            None
        } else {
            // These shapes were previously rejected, so no old receipt exists.
            // Hash every row: changing a losing patch must not replay a different
            // request as though only its eventual highest guard mattered.
            let mut ordered = relationships.clone();
            ordered.sort_by_key(|row| (row.relationship_id, row.src, row.dst, row.edge_type.clone()));
            Some(relationship_import_fingerprint(cell_id, edge_type, &ordered))
        };
        #[cfg(feature = "opencypher")]
        let options = RelationshipImportOptions {
            guarded_rows: Some(&guarded_rows),
            ..options
        };
        let mut relationships = if options.create_always {
            validate_relationship_creates(cell_id, edge_type, relationships)?
        } else {
            coalesce_relationship_imports(
                cell_id,
                edge_type,
                relationships,
                options.update_existing_metadata.then_some(options.merge_policy),
            )?
        };
        if relationships.is_empty() {
            let epoch = self.current_epoch(cell_id).await?;
            return Ok(RelationshipImportResult {
                start_epoch: epoch,
                end_epoch: epoch,
                relationships_inserted: 0,
                relationships_already_existed: 0,
                structural_edges_inserted: 0,
                structural_edges_already_existed: 0,
            });
        }
        ensure_limit(
            "import_relationships_batch",
            relationships.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        relationships.sort_by_key(|relationship| {
            (
                relationship.relationship_id,
                relationship.src,
                relationship.dst,
                relationship.edge_type.clone(),
            )
        });
        let fingerprint = ordered_fingerprint.unwrap_or_else(|| relationship_import_fingerprint(cell_id, edge_type, &relationships));

        let span = write_txn_span(cell_id, Some(edge_type));
        let result = self.run_write_pipeline(async {
            let mut retries = WriteRetryCount::new();
            // Best effort: a failed warm only means the guarded reads stay
            // cold, which is today's behaviour. Real storage errors surface
            // again from the guarded transaction.
            if let Err(error) = self
                .warm_relationship_import_reads(cell_id, edge_type, &relationships, idempotency_key, options)
                .await
            {
                tracing::debug!(%error, "relationship import warm failed; guarded reads stay cold");
            }
            let _permit = self
                .acquire_graph_write_permit(options.operation)
                .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = options.operation))
                .await?;
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self
                    .import_relationships_batch_txn(
                        cell_id,
                        edge_type,
                        &relationships,
                        idempotency_key,
                        fingerprint,
                        options,
                    )
                    .instrument(relationship_import_txn_span(options.operation, relationships.len()))
                    .await
                {
                    Err(err)
                        if is_retryable_write_conflict(&err)
                            && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                    {
                        self.operation_metrics
                            .write_retries
                            .fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        tracing::Span::current().record("hydradb.commit_epoch", result.end_epoch);
                        return Ok(result);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
            .inspect_err(record_error_class)
        }
        .instrument(span))
        .await;
        if result.is_ok() {
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Warm, before the writer permit, the reads a relationship import makes
    /// while holding it.
    ///
    /// On staging a MERGE batch held the permit for ~100 ms, ~58 ms of it in
    /// the identity lookup and ~33 ms reading degree counters
    /// (`hydradb.shard.relationship_import.*`). Both were cold object-store
    /// reads. A new relationship has no merge pointer, so the lookup falls
    /// back to a property-index scan, and that scan runs with
    /// `cache_blocks(false)`: it went to object storage every time, with every
    /// other writer queued behind it.
    ///
    /// Values are discarded. The guarded transaction still reads every key at
    /// its own snapshot and merges the memtable and newer SSTs, so writes made
    /// after this warm are still seen; only immutable SST blocks are admitted
    /// here, so a cached block cannot go stale. SlateDB checks the block cache
    /// on every read whatever `cache_blocks` says (the flag only controls
    /// admission), so the guarded scan keeps its uncached options and still
    /// hits what this warm admitted. Batches above the cached-read limit are
    /// skipped so an import cannot sweep the cache.
    async fn warm_relationship_import_reads(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
        idempotency_key: &str,
        options: RelationshipImportOptions<'_>,
    ) -> Result<()> {
        if !self.storage.has_block_cache() || relationships.len() > RELATIONSHIP_IMPORT_WARM_MAX_ROWS {
            return Ok(());
        }
        let span = tracing::info_span!(
            "write.relimport_warm",
            hydradb.relimport.relationships = relationships.len() as u64,
            hydradb.warm.budget = RELATIONSHIP_IMPORT_WARM_MAX_READS as u64,
            hydradb.warm.replay = tracing::field::Empty,
            hydradb.warm.point_keys = tracing::field::Empty,
            hydradb.warm.identity_pointers = tracing::field::Empty,
            hydradb.warm.identity_scans = tracing::field::Empty,
            hydradb.warm.reads = tracing::field::Empty,
            hydradb.warm.truncated = tracing::field::Empty,
        );
        let snapshot = self.snapshot(cell_id).instrument(span.clone()).await?;
        let storage = &snapshot.storage_snapshot;
        let read_options = ReadOptions::default();

        // An already-recorded idempotency key means the guarded transaction will
        // return its durable result without reading a single relationship, so
        // warming the batch behind it buys nothing. Read that one key first and
        // stop; the guarded read of it is warm either way. A key recorded for a
        // different batch is the same story — that request fails on the
        // fingerprint rather than reading the rows.
        let idempotency_key_name = keys::idempotency(cell_id, "relationship-import", idempotency_key);
        let replay = storage
            .get_with_options(idempotency_key_name.as_bytes(), &read_options)
            .await?
            .is_some();
        span.record("hydradb.warm.replay", replay);
        if replay {
            return Ok(());
        }
        let budget = WarmBudget::new(RELATIONSHIP_IMPORT_WARM_MAX_READS);

        // Every point key the guarded transaction reads, over-approximated to
        // every endpoint and pair in the batch whether or not the edge turns
        // out to be new: the idempotency record, the fence markers, the id
        // counter, both endpoints (label check), the direct edge (structural
        // check) and the degree and relationship counters. The counters are
        // the measured cost; the rest are cheap and warmed so no guarded point
        // read is left cold.
        let write_reverse_index = self.writes_reverse_index();
        let mut point_keys = BTreeSet::from([
            keys::cell_drop_marker(cell_id),
            keys::cell_drop_pending_marker(cell_id),
            keys::last_relationship_id(cell_id),
        ]);
        for relationship in relationships {
            if options.endpoint_labels.is_some() {
                point_keys.insert(keys::vertex(cell_id, relationship.src));
                point_keys.insert(keys::vertex(cell_id, relationship.dst));
            }
            point_keys.insert(keys::out_edge(cell_id, edge_type, relationship.src, relationship.dst));
            point_keys.insert(keys::degree_out(cell_id, edge_type, relationship.src));
            if write_reverse_index {
                point_keys.insert(keys::degree_in(cell_id, edge_type, relationship.dst));
            }
            point_keys.insert(keys::relationship_count(cell_id, edge_type, relationship.src, relationship.dst));
        }
        span.record("hydradb.warm.point_keys", point_keys.len() as u64);
        let warm_points = stream::iter(point_keys)
            .map(|key| {
                let (read_options, budget) = (&read_options, &budget);
                async move {
                    tokio::task::consume_budget().await;
                    if !budget.charge() {
                        return Ok::<_, GraphError>(());
                    }
                    storage.get_with_options(key.as_bytes(), read_options).await?;
                    Ok::<_, GraphError>(())
                }
                .boxed()
            })
            .buffer_unordered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
            .boxed()
            .try_for_each(|()| async { Ok(()) });

        #[cfg(feature = "opencypher")]
        let warm_identities = self
            .warm_relationship_merge_identities(storage, cell_id, edge_type, relationships, options, &budget)
            .boxed();
        #[cfg(not(feature = "opencypher"))]
        let warm_identities = {
            let _ = options;
            async { Ok::<_, GraphError>(WarmedIdentities::default()) }
        };

        let warmed = async { tokio::try_join!(warm_points, warm_identities) }
            .instrument(span.clone())
            .await;
        // The budget is reported however the warm ended: a failure that stopped
        // it early still spent what it spent.
        span.record("hydradb.warm.reads", budget.reads());
        span.record("hydradb.warm.truncated", budget.spent());
        let ((), identities) = warmed?;
        span.record("hydradb.warm.identity_pointers", identities.pointers);
        span.record("hydradb.warm.identity_scans", identities.scans);
        Ok(())
    }

    /// The MERGE identity lookup, warmed: the merge pointer for every row, then
    /// the record it names, or the property-index scan the guarded lookup falls
    /// back to when there is no pointer (always true for a new relationship).
    #[cfg(feature = "opencypher")]
    async fn warm_relationship_merge_identities(
        &self,
        storage: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
        options: RelationshipImportOptions<'_>,
        budget: &WarmBudget,
    ) -> Result<WarmedIdentities> {
        if options.create_always || !options.update_existing_metadata {
            return Ok(WarmedIdentities::default());
        }
        let read_options = ReadOptions::default();
        let scan_options = ScanOptions::default().with_cache_blocks(true);
        let pointers = std::sync::atomic::AtomicU64::new(0);
        let scans = std::sync::atomic::AtomicU64::new(0);
        stream::iter(relationships)
            .map(|relationship| {
                let (read_options, scan_options, pointers, scans, budget) = (&read_options, &scan_options, &pointers, &scans, budget);
                async move {
                    tokio::task::consume_budget().await;
                    if budget.spent() {
                        return Ok::<_, GraphError>(());
                    }
                    let identity = VertexPropertyValue::Integer(relationship.relationship_id);
                    if relationship.metadata.properties.get(RELATIONSHIP_IDENTITY_PROPERTY) != Some(&identity) {
                        // The guarded lookup rejects this row; nothing to warm.
                        return Ok::<_, GraphError>(());
                    }
                    let encoded = encode_vertex_property_value_key(&identity);
                    let pointer_key = keys::relationship_merge_index(
                        cell_id,
                        edge_type,
                        RELATIONSHIP_IDENTITY_PROPERTY,
                        &encoded,
                        relationship.src,
                        relationship.dst,
                    );
                    if !budget.charge() {
                        return Ok(());
                    }
                    pointers.fetch_add(1, Ordering::Relaxed);
                    if let Some(pointer) = storage.get_with_options(pointer_key.as_bytes(), read_options).await? {
                        if let Ok(relationship_id) = decode_u64(&pointer_key, &pointer) {
                            let record_key = keys::relationship(cell_id, edge_type, relationship.src, relationship.dst, relationship_id);
                            if !budget.charge() {
                                return Ok(());
                            }
                            // Trust the pointer exactly as far as the guarded
                            // lookup does: it must name a live record that
                            // carries this identity. Otherwise that lookup falls
                            // back to the property-index scan, and leaving here
                            // would hand it a cold one while it holds the
                            // permit. A record that fails to decode is the same
                            // case — corruption stays loud in the guarded read,
                            // which is where it can be reported.
                            let named = storage.get_with_options(record_key.as_bytes(), read_options).await?;
                            let answered = named
                                .and_then(|value| decode_relationship_record(&record_key, &value).ok())
                                .is_some_and(|record| record.metadata.properties.get(RELATIONSHIP_IDENTITY_PROPERTY) == Some(&identity));
                            if answered {
                                return Ok(());
                            }
                        }
                    }
                    if !budget.charge() {
                        return Ok(());
                    }
                    scans.fetch_add(1, Ordering::Relaxed);
                    let prefix = keys::relationship_property_index_edge_prefix(
                        cell_id,
                        edge_type,
                        RELATIONSHIP_IDENTITY_PROPERTY,
                        &encoded,
                        relationship.src,
                        relationship.dst,
                    );
                    let mut iter = storage.scan_prefix_with_options(prefix.as_bytes(), .., scan_options).await?;
                    // One identity names one row except for legitimately
                    // parallel relationships; a few entries admit the blocks.
                    for _ in 0..RELATIONSHIP_IMPORT_WARM_MAX_SCAN_ENTRIES {
                        // `iter.next()` is the read, so the charge comes first.
                        // Checking afterwards lets every in-flight scan admit one
                        // more block once the budget is gone, and reports a read
                        // count that does not include them.
                        if !budget.charge() {
                            break;
                        }
                        if iter.next().await?.is_none() {
                            break;
                        }
                    }
                    Ok(())
                }
                .boxed()
            })
            .buffer_unordered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
            .boxed()
            .try_for_each(|()| async { Ok(()) })
            .await?;
        Ok(WarmedIdentities {
            pointers: pointers.load(Ordering::Relaxed),
            scans: scans.load(Ordering::Relaxed),
        })
    }

    async fn set_edge_metadata_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let lock = self.acquire_local_write_guard_no_fence(cell_id, "set_edge_metadata").await?;
        let result = self.set_edge_metadata_txn_locked(cell_id, edge_type, src, dst, metadata).await;
        finish_local_write(lock, result).await
    }

    async fn set_edge_metadata_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "set_edge_metadata")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let current_epoch = txn.seqnum();
        if edge_epoch_at_txn(&txn, cell_id, edge_type, src, dst, current_epoch)
            .await?
            .is_none()
        {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "GraphQuery",
                feature: "cannot set metadata for a missing edge".to_string(),
            });
        }
        let edge_metadata_key = keys::edge_metadata(cell_id, edge_type, src, dst);
        let previous = match read_txn_remote(&txn, &edge_metadata_key).await? {
            Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
            None => EdgeMetadata::default(),
        };
        if previous == metadata {
            return Ok(false);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        apply_edge_metadata_update_txn(
            &txn,
            EdgeMetadataTarget {
                cell_id,
                edge_type,
                src,
                dst,
            },
            &previous,
            &metadata,
            epoch,
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(true)
    }

    async fn set_edge_metadata_batch_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: Vec<(VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        let lock = self.acquire_local_write_guard_no_fence(cell_id, "set_edge_metadata_batch").await?;
        let result = self.set_edge_metadata_batch_txn_locked(cell_id, edge_type, updates).await;
        finish_local_write(lock, result).await
    }

    async fn set_edge_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: Vec<(VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "set_edge_metadata_batch").await?;
        let current_epoch = txn.seqnum();
        let mut changed = Vec::new();
        for (src, dst, metadata) in updates {
            if edge_epoch_at_txn(&txn, cell_id, edge_type, src, dst, current_epoch)
                .await?
                .is_none()
            {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "GraphQuery",
                    feature: format!("cannot set metadata for missing edge {edge_type}({src}->{dst})"),
                });
            }
            let edge_metadata_key = keys::edge_metadata(cell_id, edge_type, src, dst);
            let previous = match read_txn_remote(&txn, &edge_metadata_key).await? {
                Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
                None => EdgeMetadata::default(),
            };
            if previous != metadata {
                changed.push((src, dst, previous, metadata));
            }
        }
        if changed.is_empty() {
            return Ok(0);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        for (src, dst, previous, metadata) in &changed {
            apply_edge_metadata_update_txn(
                &txn,
                EdgeMetadataTarget {
                    cell_id,
                    edge_type,
                    src: *src,
                    dst: *dst,
                },
                previous,
                metadata,
                epoch,
            )?;
        }
        let changed_count = changed.len();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed_count)
    }

    async fn import_relationships_batch_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
        idempotency_key: &str,
        fingerprint: u64,
        options: RelationshipImportOptions<'_>,
    ) -> Result<RelationshipImportResult> {
        let lock = self.acquire_local_write_guard_no_fence(cell_id, options.operation).await?;
        let result = self
            .import_relationships_batch_txn_locked(cell_id, edge_type, relationships, idempotency_key, fingerprint, options)
            .await;
        finish_local_write(lock, result).await
    }

    async fn import_relationships_batch_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: &[RelationshipMutation],
        idempotency_key: &str,
        fingerprint: u64,
        options: RelationshipImportOptions<'_>,
    ) -> Result<RelationshipImportResult> {
        self.validate_changing_write().await?;
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, options.operation).await?;

        // A replay is defined by the original request, not by whether its
        // MATCH endpoints still exist at replay time. Consult the durable
        // result before evaluating the current endpoint snapshot.
        let idem_key = keys::idempotency(cell_id, "relationship-import", idempotency_key);
        if let Some(value) = read_txn_remote_uncached(&txn, &idem_key).await? {
            // This branch does not commit, so it cannot validate ownership at
            // the WAL barrier. Preserve the explicit replay fencing check.
            self.storage.refresh_writer_fence().await?;
            self.operation_metrics
                .relationship_import_idempotency_replays
                .fetch_add(1, Ordering::Relaxed);
            return decode_relationship_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }
        // Checked here, after the receipt: a retry of an import an older node
        // accepted replays its result above, whatever ids it carried; only
        // rows about to be written may not take a generated id.
        for relationship in relationships {
            crate::stored_relationship_id(relationship.relationship_id, || {
                format!("cell/{cell_id}/relationship_import/{edge_type}/{}", relationship.relationship_id)
            })?;
        }

        let mut relationships = relationships.to_vec();
        // Phase timing for this transaction. Span attributes land as each
        // phase completes (so a timed-out attempt still shows its completed
        // phases); the metrics recorder runs once, after the commit, so the
        // counter sums only ever describe whole batches.
        let mut profile = RelationshipImportProfile::default();
        let txn_span = tracing::Span::current();
        let endpoint_check_started = std::time::Instant::now();
        if let Some((source_label, destination_label)) = options.endpoint_labels {
            let mut required_labels = BTreeMap::<VertexId, BTreeSet<&str>>::new();
            for relationship in &relationships {
                for (vertex, label) in [(relationship.src, source_label), (relationship.dst, destination_label)] {
                    required_labels.entry(vertex).or_default().insert(label);
                }
            }
            let endpoint_keys = required_labels.keys().map(|vertex| keys::vertex(cell_id, *vertex));
            let endpoint_values = read_txn_remote_many(&txn, endpoint_keys).await?;
            let mut matched_labels = BTreeMap::<VertexId, BTreeSet<String>>::new();
            for (vertex, labels) in required_labels {
                let key = keys::vertex(cell_id, vertex);
                let Some(value) = endpoint_values.get(&key).and_then(Option::as_ref) else {
                    continue;
                };
                let metadata = decode_vertex_metadata(&key, value)?;
                for label in labels {
                    if metadata.labels.contains(label) {
                        matched_labels.entry(vertex).or_default().insert(label.to_string());
                    }
                }
            }
            let requested = relationships.len();
            relationships.retain(|relationship| {
                matched_labels
                    .get(&relationship.src)
                    .is_some_and(|labels| labels.contains(source_label))
                    && matched_labels
                        .get(&relationship.dst)
                        .is_some_and(|labels| labels.contains(destination_label))
            });
            let skipped = requested.saturating_sub(relationships.len());
            if skipped > 0 {
                tracing::debug!(
                    cell_id,
                    edge_type,
                    skipped,
                    "skipped relationship rows whose MATCH endpoints were absent"
                );
            }
        }
        if options.endpoint_labels.is_some() {
            profile.endpoint_check = endpoint_check_started.elapsed();
            txn_span.record("hydradb.relimport.endpoint_check_us", duration_micros_u64(profile.endpoint_check));
        }
        let current_epoch = txn.seqnum();
        let current_relationship_id = read_counter_txn(&txn, &keys::last_relationship_id(cell_id)).await?;
        let mut prefetched_relationships = BTreeMap::<RelationshipId, RelationshipRecord>::new();
        let mut next_relationship_id = current_relationship_id;
        // `rmerge_idx` pointer writes the MERGE branch has proven correct:
        // `Some(id)` when exactly one live row carries the identity, `None`
        // (delete) when the identity is ambiguous. Applied after the insert
        // and update loops, whose index hooks blindly invalidate the pointer,
        // so within this transaction these entries have the last word.
        #[cfg(feature = "opencypher")]
        let mut merge_pointer_updates = BTreeMap::<String, Option<RelationshipId>>::new();
        if options.create_always {
            let allocated =
                next_available_relationship_ids_txn(&txn, cell_id, &mut next_relationship_id, relationships.len(), "CREATE").await?;
            for (relationship, relationship_id) in relationships.iter_mut().zip(allocated) {
                relationship.relationship_id = relationship_id;
            }
        } else if options.update_existing_metadata {
            #[cfg(not(feature = "opencypher"))]
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "relationship MERGE requires the opencypher feature".to_string(),
            });
            #[cfg(feature = "opencypher")]
            {
                // Identity resolution: point get on the unique `rmerge_idx`
                // pointer first, prefix scan only on a miss. The get can use
                // bloom filters; the scan structurally cannot (it must open
                // every overlapping SST), so the pointer is what keeps this
                // phase flat as the scope's file count grows. A miss falls
                // back to the scan and heals the pointer for the next batch,
                // so pre-pointer data never needs a backfill.
                let identity_scan_started = std::time::Instant::now();
                let pointer_read_options = relationship_import_read_options(relationships.len());
                let lookups = stream::iter(relationships.into_iter().map(|relationship| {
                    let txn = &txn;
                    let pointer_read_options = &pointer_read_options;
                    async move {
                        let external_id = relationship.relationship_id;
                        let identity = VertexPropertyValue::Integer(external_id);
                        if relationship.metadata.properties.get(RELATIONSHIP_IDENTITY_PROPERTY) != Some(&identity) {
                            return Err(GraphError::CorruptValue {
                                key: format!("cell/{cell_id}/relationship-merge/{edge_type}/{}/{external_id}", relationship.src),
                                reason: "relationship MERGE identity metadata does not match the parsed id".to_string(),
                            });
                        }
                        let pointer_key = keys::relationship_merge_index(
                            cell_id,
                            edge_type,
                            RELATIONSHIP_IDENTITY_PROPERTY,
                            &encode_vertex_property_value_key(&identity),
                            relationship.src,
                            relationship.dst,
                        );
                        if let Some(pointer) = read_txn_remote_with_options(txn, &pointer_key, pointer_read_options).await? {
                            // The pointer is a cache of a proof, never an
                            // authority: trust it only if it decodes, its
                            // target record is live, and that record actually
                            // carries this identity. Anything else falls back
                            // to the scan, whose result replaces the pointer.
                            // An undecodable pointer is deliberately a
                            // fallback rather than a `CorruptValue` error —
                            // the scan reconstructs the truth, so corruption
                            // in this accelerator key must not make an
                            // identity permanently un-MERGEable. Record
                            // corruption stays loud: `decode_relationship_
                            // record` below still propagates its error.
                            if let Ok(relationship_id) = decode_u64(&pointer_key, &pointer) {
                                let record_key =
                                    keys::relationship(cell_id, edge_type, relationship.src, relationship.dst, relationship_id);
                                if let Some(value) = read_txn_remote_with_options(txn, &record_key, pointer_read_options).await? {
                                    let record = decode_relationship_record(&record_key, &value)?;
                                    if record.metadata.properties.get(RELATIONSHIP_IDENTITY_PROPERTY) == Some(&identity) {
                                        return Ok::<_, GraphError>(MergeIdentityLookup {
                                            relationship,
                                            existing: vec![record],
                                            outcome: MergePointerOutcome::Hit(pointer_key),
                                        });
                                    }
                                }
                            }
                            // Dead target, undecodable value, or a live
                            // target with a different identity: the scan is
                            // the authority, and the heal pass replaces or
                            // deletes the pointer from its answer.
                            let existing = relationship_records_for_edge_property_txn(
                                txn,
                                RelationshipPropertyTxnLookup {
                                    cell_id,
                                    edge_type,
                                    src: relationship.src,
                                    dst: relationship.dst,
                                    property: RELATIONSHIP_IDENTITY_PROPERTY,
                                    value: &identity,
                                },
                            )
                            .await?;
                            return Ok(MergeIdentityLookup {
                                relationship,
                                existing,
                                outcome: MergePointerOutcome::Stale(pointer_key),
                            });
                        }
                        let existing = relationship_records_for_edge_property_txn(
                            txn,
                            RelationshipPropertyTxnLookup {
                                cell_id,
                                edge_type,
                                src: relationship.src,
                                dst: relationship.dst,
                                property: RELATIONSHIP_IDENTITY_PROPERTY,
                                value: &identity,
                            },
                        )
                        .await?;
                        Ok(MergeIdentityLookup {
                            relationship,
                            existing,
                            outcome: MergePointerOutcome::Miss(pointer_key),
                        })
                    }
                }))
                .buffered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
                // Heal-on-read: a resolution to exactly one record earns (or
                // re-affirms) the pointer that answers by point get; a scan
                // that resolved several (parallel relationships legitimately
                // sharing an identity value — MERGE updates them all) must
                // leave no pointer, since a pointer can only ever name one
                // row. Duplicate identities within this batch are ambiguous
                // for the same reason. Recorded into `merge_pointer_updates`
                // rather than written here; rows with no match get their
                // pointer when the insert below allocates their id.
                for lookup in &lookups {
                    match lookup.outcome {
                        MergePointerOutcome::Hit(_) => profile.identity_pointer_hits += 1,
                        MergePointerOutcome::Miss(_) | MergePointerOutcome::Stale(_) => profile.identity_pointer_misses += 1,
                    }
                    let pointer_key = lookup.outcome.pointer_key();
                    match lookup.existing.as_slice() {
                        [record] => note_merge_pointer(&mut merge_pointer_updates, pointer_key, record.relationship_id),
                        [] => {}
                        _ => {
                            merge_pointer_updates.insert(pointer_key.to_string(), None);
                        }
                    }
                }
                profile.identity_scan = identity_scan_started.elapsed();
                txn_span.record("hydradb.relimport.identity_scan_us", duration_micros_u64(profile.identity_scan));
                txn_span.record("hydradb.relimport.identity_pointer_hits", profile.identity_pointer_hits);
                txn_span.record("hydradb.relimport.identity_pointer_misses", profile.identity_pointer_misses);
                let missing = lookups.iter().filter(|lookup| lookup.existing.is_empty()).count();
                let allocated = next_available_relationship_ids_txn(&txn, cell_id, &mut next_relationship_id, missing, "MERGE").await?;
                let mut allocated = allocated.into_iter();
                let mut resolved = Vec::with_capacity(lookups.len());
                for lookup in lookups {
                    let MergeIdentityLookup {
                        relationship,
                        existing,
                        outcome,
                    } = lookup;
                    let patches = options.guarded_rows.and_then(|rows| rows.get(&relationship.relationship_id));
                    if existing.is_empty() {
                        let mut inserted = relationship;
                        if let (Some(patches), Some(policy)) = (patches, options.merge_policy) {
                            inserted.metadata = apply_guarded_relationship_rows(
                                None,
                                patches,
                                policy,
                                inserted.relationship_id,
                                &keys::relationship_id(cell_id, inserted.relationship_id),
                            )?;
                        }
                        inserted.relationship_id = allocated.next().ok_or_else(|| GraphError::CorruptValue {
                            key: keys::last_relationship_id(cell_id),
                            reason: "relationship id allocation returned too few ids".to_string(),
                        })?;
                        note_merge_pointer(&mut merge_pointer_updates, outcome.pointer_key(), inserted.relationship_id);
                        resolved.push(inserted);
                    } else {
                        for record in existing {
                            let mut matched = relationship.clone();
                            if let (Some(patches), Some(policy)) = (patches, options.merge_policy) {
                                matched.metadata = apply_guarded_relationship_rows(
                                    Some(&record.metadata),
                                    patches,
                                    policy,
                                    matched.relationship_id,
                                    &keys::relationship_id(cell_id, record.relationship_id),
                                )?;
                            }
                            matched.relationship_id = record.relationship_id;
                            prefetched_relationships.insert(record.relationship_id, record);
                            resolved.push(matched);
                        }
                    }
                }
                relationships = resolved;
            }
        }
        let max_requested_relationship_id = relationships
            .iter()
            .map(|relationship| relationship.relationship_id)
            .max()
            .unwrap_or(0);
        let fresh_cell = current_epoch == 0;
        let mut relationships_inserted = Vec::new();
        let mut relationships_updated = Vec::<(RelationshipRecord, EdgeMetadata)>::new();
        let mut relationships_already_existed = 0_u64;
        let unresolved = relationships
            .iter()
            .filter(|relationship| !prefetched_relationships.contains_key(&relationship.relationship_id))
            .map(|relationship| {
                (
                    relationship.relationship_id,
                    keys::relationship(cell_id, edge_type, relationship.src, relationship.dst, relationship.relationship_id),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let id_keys = relationships
            .iter()
            .map(|relationship| keys::relationship_id(cell_id, relationship.relationship_id));
        let record_read_started = std::time::Instant::now();
        let id_values = read_txn_remote_many(&txn, id_keys).await?;
        profile.record_read += record_read_started.elapsed();
        let mut record_keys = BTreeMap::<RelationshipId, (String, bool)>::new();
        for (relationship_id, record) in &prefetched_relationships {
            let id_key = keys::relationship_id(cell_id, *relationship_id);
            let Some(value) = id_values.get(&id_key).and_then(Option::as_ref) else {
                continue;
            };
            let target_key = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
                key: id_key,
                reason: format!("relationship id pointer is not UTF-8: {err}"),
            })?;
            let prefetched_key = keys::relationship(&record.cell_id, &record.edge_type, record.src, record.dst, record.relationship_id);
            if target_key != prefetched_key {
                // Preserve the durable id pointer as the authority when the
                // property index and id index disagree. The identity check
                // below will surface the same conflict as the serial path.
                record_keys.insert(*relationship_id, (target_key.to_string(), true));
            }
        }
        for (relationship_id, fallback_key) in &unresolved {
            let id_key = keys::relationship_id(cell_id, *relationship_id);
            match id_values.get(&id_key).and_then(Option::as_ref) {
                Some(value) => {
                    let target_key = std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
                        key: id_key,
                        reason: format!("relationship id pointer is not UTF-8: {err}"),
                    })?;
                    record_keys.insert(*relationship_id, (target_key.to_string(), true));
                }
                None => {
                    record_keys.insert(*relationship_id, (fallback_key.clone(), false));
                }
            }
        }
        let record_read_started = std::time::Instant::now();
        let record_values = read_txn_remote_many(&txn, record_keys.values().map(|(key, _)| key.clone())).await?;
        profile.record_read += record_read_started.elapsed();
        txn_span.record("hydradb.relimport.record_read_us", duration_micros_u64(profile.record_read));
        for (relationship_id, (record_key, required)) in record_keys {
            match record_values.get(&record_key).and_then(Option::as_ref) {
                Some(value) => {
                    prefetched_relationships.insert(relationship_id, decode_relationship_record(&record_key, value)?);
                }
                None if required => {
                    return Err(GraphError::CorruptValue {
                        key: keys::relationship_id(cell_id, relationship_id),
                        reason: format!("relationship id points at missing record {record_key}"),
                    });
                }
                None => {}
            }
        }
        for relationship in &relationships {
            let rel_key = keys::relationship(cell_id, edge_type, relationship.src, relationship.dst, relationship.relationship_id);
            let existing = prefetched_relationships.get(&relationship.relationship_id).cloned();
            if let Some(existing) = existing {
                let requested = RelationshipRecord {
                    cell_id: cell_id.to_string(),
                    edge_type: edge_type.to_string(),
                    src: relationship.src,
                    dst: relationship.dst,
                    relationship_id: relationship.relationship_id,
                    metadata: relationship.metadata.clone(),
                };
                if existing.cell_id != requested.cell_id
                    || existing.edge_type != requested.edge_type
                    || existing.src != requested.src
                    || existing.dst != requested.dst
                    || existing.relationship_id != requested.relationship_id
                {
                    // This conflict is about the *payload's* identity, not the
                    // request key: the relationship id the caller sent already
                    // resolves to another edge. It reaches the log with the
                    // same `IdempotencyConflict` display as a replayed request
                    // key does, and the two have opposite fixes — so the pair
                    // that disagreed is logged here, where both are in hand.
                    // Every field is an id or a type name, never user data.
                    tracing::warn!(
                        target: "hydradb",
                        idempotency_key,
                        existing.cell_id = %existing.cell_id,
                        existing.edge_type = %existing.edge_type,
                        existing.src = existing.src,
                        existing.dst = existing.dst,
                        existing.relationship_id = existing.relationship_id,
                        requested.cell_id = %requested.cell_id,
                        requested.edge_type = %requested.edge_type,
                        requested.src = requested.src,
                        requested.dst = requested.dst,
                        requested.relationship_id = requested.relationship_id,
                        "relationship import rejected: relationship id is bound to a different edge",
                    );
                    return Err(GraphError::IdempotencyConflict {
                        operation: "relationship-import",
                        idempotency_key: idempotency_key.to_string(),
                        reason: "the relationship id in the payload is already bound to a \
                                 different edge",
                    });
                }
                if existing.metadata != requested.metadata {
                    if !options.update_existing_metadata {
                        return Err(GraphError::IdempotencyConflict {
                            operation: "relationship-import",
                            idempotency_key: idempotency_key.to_string(),
                            reason: "the relationship exists with different properties and this \
                                     operation does not update them",
                        });
                    }
                    let mut requested_metadata = requested.metadata.clone();
                    if let Some(policy) = options.merge_policy {
                        let Some(properties) =
                            guarded_metadata_patch(&existing.metadata.properties, requested_metadata.properties, policy, &rel_key)?
                        else {
                            relationships_already_existed = relationships_already_existed.saturating_add(1);
                            continue;
                        };
                        requested_metadata.properties = properties;
                    }
                    let next_metadata = merge_edge_metadata(&existing.metadata, &requested_metadata);
                    if next_metadata != existing.metadata {
                        let previous_metadata = existing.metadata.clone();
                        let mut updated = existing;
                        updated.metadata = next_metadata;
                        relationships_updated.push((updated, previous_metadata));
                    }
                }
                relationships_already_existed = relationships_already_existed.saturating_add(1);
            } else {
                relationships_inserted.push(relationship.clone());
            }
        }

        let mut structural_edges = BTreeSet::<(VertexId, VertexId)>::new();
        for relationship in &relationships_inserted {
            structural_edges.insert((relationship.src, relationship.dst));
        }
        let mut structural_edges_to_insert = structural_edges.clone();
        let structural_check_started = std::time::Instant::now();
        if !fresh_cell && !structural_edges.is_empty() {
            let direct_edge_values = read_txn_remote_many(
                &txn,
                structural_edges
                    .iter()
                    .map(|(src, dst)| keys::out_edge(cell_id, edge_type, *src, *dst)),
            )
            .await?;
            let mut missing_by_src = BTreeMap::<VertexId, BTreeSet<VertexId>>::new();
            for (src, dst) in &structural_edges {
                let key = keys::out_edge(cell_id, edge_type, *src, *dst);
                if direct_edge_values.get(&key).and_then(Option::as_ref).is_some() {
                    structural_edges_to_insert.remove(&(*src, *dst));
                } else {
                    missing_by_src.entry(*src).or_default().insert(*dst);
                }
            }
            let segment_neighbors = stream::iter(missing_by_src.keys().copied().map(|src| {
                let txn = &txn;
                async move {
                    let neighbors = out_segment_neighbors_for_src_txn(txn, cell_id, edge_type, src, current_epoch).await?;
                    Ok::<_, GraphError>((src, neighbors))
                }
            }))
            .buffered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
            .try_collect::<BTreeMap<_, _>>()
            .await?;
            // The volume behind the fan-out question: how many source
            // vertices fell through to a segment scan, and how many
            // destinations those scans materialised to answer per-pair
            // yes/no existence checks.
            profile.segment_scans = segment_neighbors.len() as u64;
            profile.segment_neighbors = segment_neighbors.values().map(|neighbors| neighbors.len() as u64).sum();
            for (src, destinations) in missing_by_src {
                if let Some(neighbors) = segment_neighbors.get(&src) {
                    for dst in destinations {
                        if neighbors.contains(&dst) {
                            structural_edges_to_insert.remove(&(src, dst));
                        }
                    }
                }
            }
        }
        profile.structural_check = structural_check_started.elapsed();
        txn_span.record(
            "hydradb.relimport.structural_check_us",
            duration_micros_u64(profile.structural_check),
        );
        txn_span.record("hydradb.relimport.segment_scans", profile.segment_scans);
        txn_span.record("hydradb.relimport.segment_neighbors", profile.segment_neighbors);
        let structural_edges_inserted = u64::try_from(structural_edges_to_insert.len()).map_err(|err| GraphError::CorruptValue {
            key: "relationship_import".to_string(),
            reason: format!("too many structural edges in one import: {err}"),
        })?;
        let structural_edges_already_existed = u64::try_from(structural_edges.len().saturating_sub(structural_edges_to_insert.len()))
            .map_err(|err| GraphError::CorruptValue {
                key: "relationship_import".to_string(),
                reason: format!("too many existing structural edges in one import: {err}"),
            })?;
        let relationships_inserted_count = u64::try_from(relationships_inserted.len()).map_err(|err| GraphError::CorruptValue {
            key: "relationship_import".to_string(),
            reason: format!("too many relationships in one import: {err}"),
        })?;
        let changed = relationships_inserted_count > 0 || structural_edges_inserted > 0 || !relationships_updated.is_empty();
        let epoch = if changed {
            current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
                key: "storage_sequence".to_string(),
                reason: "epoch overflow during relationship import".to_string(),
            })?
        } else {
            current_epoch
        };
        let result = RelationshipImportResult {
            start_epoch: epoch,
            end_epoch: epoch,
            relationships_inserted: relationships_inserted_count,
            relationships_already_existed,
            structural_edges_inserted,
            structural_edges_already_existed,
        };

        let write_reverse_index = self.writes_reverse_index();
        let mut out_increments = BTreeMap::<VertexId, u64>::new();
        let mut in_increments = BTreeMap::<VertexId, u64>::new();
        for (src, dst) in &structural_edges_to_insert {
            *out_increments.entry(*src).or_insert(0) += 1;
            if write_reverse_index {
                *in_increments.entry(*dst).or_insert(0) += 1;
            }
        }
        let mut relationship_count_increments = BTreeMap::<(VertexId, VertexId), u64>::new();
        for relationship in &relationships_inserted {
            *relationship_count_increments
                .entry((relationship.src, relationship.dst))
                .or_insert(0) += 1;
        }
        let counter_keys = out_increments
            .keys()
            .map(|src| keys::degree_out(cell_id, edge_type, *src))
            .chain(in_increments.keys().map(|dst| keys::degree_in(cell_id, edge_type, *dst)))
            .chain(
                relationship_count_increments
                    .keys()
                    .map(|(src, dst)| keys::relationship_count(cell_id, edge_type, *src, *dst)),
            );
        let counter_read_started = std::time::Instant::now();
        let counter_values = if fresh_cell {
            BTreeMap::new()
        } else {
            read_txn_remote_many(&txn, counter_keys).await?
        };
        profile.counter_read = counter_read_started.elapsed();
        txn_span.record("hydradb.relimport.counter_read_us", duration_micros_u64(profile.counter_read));
        let topology_changes = structural_edges_to_insert
            .iter()
            .map(|(src, dst)| (*src, *dst, true))
            .collect::<Vec<_>>();
        if !topology_changes.is_empty() {
            self.mark_topology_change_txn(&txn, cell_id, edge_type, epoch, &topology_changes)
                .await?;
        }
        for (src, dst) in structural_edges_to_insert {
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
            };
            let edge_value = encode_edge_record(&record);
            txn.put(keys::out_edge(cell_id, edge_type, src, dst).as_bytes(), &edge_value)?;
            if write_reverse_index {
                txn.put(keys::in_edge(cell_id, edge_type, dst, src).as_bytes(), &edge_value)?;
            }
        }
        for (src, increment) in out_increments {
            let key = keys::degree_out(cell_id, edge_type, src);
            let base = counter_value(&counter_values, &key, fresh_cell)?;
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for (dst, increment) in in_increments {
                let key = keys::degree_in(cell_id, edge_type, dst);
                let base = counter_value(&counter_values, &key, fresh_cell)?;
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        for relationship in &relationships_inserted {
            let record = RelationshipRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: relationship.src,
                dst: relationship.dst,
                relationship_id: relationship.relationship_id,
                metadata: relationship.metadata.clone(),
            };
            let value = encode_relationship_record(&record);
            txn.put(
                keys::relationship(cell_id, edge_type, relationship.src, relationship.dst, relationship.relationship_id).as_bytes(),
                &value,
            )?;
            txn.put(
                keys::relationship_id(cell_id, relationship.relationship_id).as_bytes(),
                keys::relationship(cell_id, edge_type, relationship.src, relationship.dst, relationship.relationship_id).as_bytes(),
            )?;
            put_relationship_property_indexes_txn(&txn, &record)?;
        }
        for (record, previous_metadata) in &relationships_updated {
            let key = keys::relationship(cell_id, edge_type, record.src, record.dst, record.relationship_id);
            txn.put(key.as_bytes(), encode_relationship_record(record).as_slice())?;
            delete_relationship_property_indexes_txn(&txn, record, previous_metadata)?;
            put_relationship_property_indexes_txn(&txn, record)?;
        }
        for ((src, dst), increment) in relationship_count_increments {
            let key = keys::relationship_count(cell_id, edge_type, src, dst);
            let base = counter_value(&counter_values, &key, fresh_cell)?;
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if max_requested_relationship_id > current_relationship_id {
            txn.put(
                keys::last_relationship_id(cell_id).as_bytes(),
                encode_u64(max_requested_relationship_id),
            )?;
        }
        // A labelled import only kept rows whose endpoints already exist.
        if options.endpoint_labels.is_none() {
            ensure_vertex_records_txn(
                &txn,
                cell_id,
                relationships_inserted
                    .iter()
                    .flat_map(|relationship| [relationship.src, relationship.dst]),
                &BTreeSet::new(),
                fresh_cell,
            )
            .await?;
        }
        txn.put(
            idem_key.as_bytes(),
            encode_relationship_import_idempotency(idempotency_key, fingerprint, &result),
        )?;
        // Last so these outrank the blind invalidations the property-index
        // hooks above issued for the same keys within this transaction.
        #[cfg(feature = "opencypher")]
        for (pointer_key, update) in &merge_pointer_updates {
            match update {
                Some(relationship_id) => txn.put(pointer_key.as_bytes(), encode_u64(*relationship_id).as_slice())?,
                None => txn.delete(pointer_key.as_bytes())?,
            }
        }
        let commit_started = std::time::Instant::now();
        commit_txn_traced(txn, self.await_durable_writes, cell_id, edge_type, epoch).await?;
        profile.commit = commit_started.elapsed();
        txn_span.record("hydradb.relimport.commit_us", duration_micros_u64(profile.commit));
        self.record_relationship_import_profile(&profile);
        Ok(result)
    }

    pub async fn create_relationship(&self, mutation: EdgeMutation, edge_metadata: EdgeMetadata) -> Result<RelationshipCreateResult> {
        self.create_relationship_with_full_metadata(mutation, VertexMetadata::default(), VertexMetadata::default(), edge_metadata)
            .await
    }

    pub async fn create_relationship_with_vertex_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    ) -> Result<RelationshipCreateResult> {
        self.create_relationship_with_full_metadata(mutation, src_metadata, dst_metadata, EdgeMetadata::default())
            .await
    }

    pub async fn create_relationship_with_full_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    ) -> Result<RelationshipCreateResult> {
        let _latency = self.operation_metrics.create_relationship_latency.start_timer();
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        validate_edge_metadata(&edge_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "create_relationship")?;
        // A self-loop with conflicting endpoint metadata fails here.
        let metadata_updates = coalesce_vertex_metadata_updates([(mutation.src, src_metadata), (mutation.dst, dst_metadata)])?;
        self.run_write_pipeline_fencing_noops(self.create_relationship_with_full_metadata_inline(mutation, metadata_updates, edge_metadata))
            .await
    }

    async fn create_relationship_with_full_metadata_inline(
        &self,
        mutation: EdgeMutation,
        metadata_updates: Vec<(VertexId, VertexMetadata)>,
        edge_metadata: EdgeMetadata,
    ) -> Result<RelationshipCreateResult> {
        let fingerprint = relationship_create_fingerprint(&mutation, metadata_updates.as_slice(), &edge_metadata);
        let _permit = self
            .acquire_graph_write_permit("create_relationship")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "create_relationship"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .create_relationship_txn(&mutation, &metadata_updates, &edge_metadata, fingerprint)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn create_relationship_txn(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
        fingerprint: u64,
    ) -> Result<RelationshipCreateResult> {
        let lock = self
            .acquire_local_write_guard_no_fence(&mutation.cell_id, "create_relationship")
            .await?;
        let result = self
            .create_relationship_txn_locked(mutation, metadata_updates, edge_metadata, fingerprint)
            .await;
        finish_local_write(lock, result).await
    }

    async fn create_relationship_txn_locked(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
        fingerprint: u64,
    ) -> Result<RelationshipCreateResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "create_relationship")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %mutation.cell_id))
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "relationship-create", &mutation.idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_create_idempotency(&idem_key, mutation, fingerprint, &value);
        }

        let current_epoch = txn.seqnum();
        let existing_edge_epoch = edge_epoch_at_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            current_epoch,
        )
        .await?;
        let structural_edge_inserted = existing_edge_epoch.is_none();
        let epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".to_string(),
            reason: "epoch overflow during relationship create".to_string(),
        })?;

        let mut relationship_id = next_relationship_id(
            read_counter_txn(&txn, &keys::last_relationship_id(&mutation.cell_id)).await?,
            &mutation.cell_id,
        )?;
        loop {
            if read_txn_remote(&txn, &keys::relationship_id(&mutation.cell_id, relationship_id))
                .await?
                .is_none()
            {
                break;
            }
            relationship_id = next_relationship_id(relationship_id, &mutation.cell_id)?;
        }

        let mut changed_metadata = Vec::new();
        // Shared with the updates below, which look up the same names.
        let mut generations = WriteBackGenerations::default();
        for (vertex_id, requested) in metadata_updates {
            let vertex_key = keys::vertex(&mutation.cell_id, *vertex_id);
            let existing = decode_vertex_record(&vertex_key, read_txn_remote(&txn, &vertex_key).await?.as_ref())?;
            // Merged into the vertex as reads see it, and compared with that
            // view: write-back values are not in the record.
            let (next, changes) = match &existing {
                Some(record) => {
                    let base = write_back_patch_base_txn(&txn, &mutation.cell_id, *vertex_id, record, &mut generations).await?;
                    let next = merge_vertex_metadata(&base, requested);
                    let changes = next != *base;
                    (next, changes)
                }
                None => (merge_vertex_metadata(&VertexMetadata::default(), requested), true),
            };
            if changes {
                changed_metadata.push((*vertex_id, existing, next));
            }
        }
        txn.put(
            keys::last_relationship_id(&mutation.cell_id).as_bytes(),
            encode_u64(relationship_id),
        )?;
        for (vertex_id, previous, next) in &changed_metadata {
            apply_vertex_metadata_update_txn(
                &txn,
                &mutation.cell_id,
                *vertex_id,
                previous.as_ref(),
                next,
                epoch,
                &mut generations,
            )
            .await?;
        }
        let written = metadata_updates.iter().map(|(vertex_id, _)| *vertex_id).collect::<BTreeSet<_>>();
        ensure_vertex_records_txn(&txn, &mutation.cell_id, [mutation.src, mutation.dst], &written, false).await?;

        if structural_edge_inserted {
            let record = EdgeRecord {
                cell_id: mutation.cell_id.clone(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
            };
            let edge_value = encode_edge_record(&record);
            self.mark_topology_change_txn(
                &txn,
                &mutation.cell_id,
                &mutation.edge_type,
                epoch,
                &[(mutation.src, mutation.dst, true)],
            )
            .await?;
            let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
            let out_degree = read_counter_txn(&txn, &out_degree_key).await? + 1;
            let in_degree = if self.writes_reverse_index() {
                let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
                let in_degree = read_counter_txn(&txn, &in_degree_key).await? + 1;
                Some((in_degree_key, in_degree))
            } else {
                None
            };
            txn.put(
                keys::out_edge(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
                &edge_value,
            )?;
            if self.writes_reverse_index() {
                txn.put(
                    keys::in_edge(&mutation.cell_id, &mutation.edge_type, mutation.dst, mutation.src).as_bytes(),
                    &edge_value,
                )?;
            }
            txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
            if let Some((in_degree_key, in_degree)) = in_degree {
                txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
            }
        }

        let record = RelationshipRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
            relationship_id,
            metadata: edge_metadata.clone(),
        };
        let relationship_key = keys::relationship(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst, relationship_id);
        txn.put(relationship_key.as_bytes(), encode_relationship_record(&record))?;
        txn.put(
            keys::relationship_id(&mutation.cell_id, relationship_id).as_bytes(),
            relationship_key.as_bytes(),
        )?;
        let relationship_count_key = keys::relationship_count(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
        let relationship_count = read_counter_txn(&txn, &relationship_count_key).await? + 1;
        txn.put(relationship_count_key.as_bytes(), encode_u64(relationship_count))?;
        put_relationship_property_indexes_txn(&txn, &record)?;

        let result = RelationshipCreateResult {
            epoch,
            relationship_id,
            structural_edge_inserted,
            already_created: false,
        };
        txn.put(
            idem_key.as_bytes(),
            encode_relationship_create_idempotency(mutation, fingerprint, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub async fn set_relationship_metadata(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        relationship_id: RelationshipId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_edge_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_relationship_metadata")?;
        self.run_write_pipeline_fencing_noops(self.set_relationship_metadata_inline(
            cell_id,
            edge_type,
            src,
            dst,
            relationship_id,
            metadata,
        ))
        .await
    }

    async fn set_relationship_metadata_inline(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        relationship_id: RelationshipId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let _permit = self
            .acquire_graph_write_permit("set_relationship_metadata")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_relationship_metadata"))
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .set_relationship_metadata_txn(cell_id, edge_type, src, dst, relationship_id, metadata.clone())
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(changed) => {
                    if changed {
                        self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(changed);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn set_relationship_metadata_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        relationship_id: RelationshipId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "set_relationship_metadata")
            .await?;
        let result = self
            .set_relationship_metadata_txn_locked(cell_id, edge_type, src, dst, relationship_id, metadata)
            .await;
        finish_local_write(lock, result).await
    }

    async fn set_relationship_metadata_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        relationship_id: RelationshipId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "set_relationship_metadata")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let key = keys::relationship(cell_id, edge_type, src, dst, relationship_id);
        let Some(value) = read_txn_remote(&txn, &key).await? else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Mutation,
                dialect: "GraphQuery",
                feature: "cannot set metadata for a missing relationship".to_string(),
            });
        };
        let mut record = decode_relationship_record(&key, &value)?;
        if record.metadata == metadata {
            return Ok(false);
        }
        let previous = record.metadata.clone();
        record.metadata = metadata.clone();
        txn.put(key.as_bytes(), encode_relationship_record(&record).as_slice())?;
        delete_relationship_property_indexes_txn(&txn, &record, &previous)?;
        put_relationship_property_indexes_txn(&txn, &record)?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(true)
    }

    /// Every relationship and structural-edge property update of one Cypher
    /// statement in one transaction: [`Self::set_relationship_metadata`] and
    /// [`Self::set_edge_metadata`] applied together, so the statement commits
    /// all of its updates or none of them and pays one durable WAL wait.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn set_relationship_metadata_batch(&self, cell_id: &str, updates: Vec<RelationshipMetadataUpdate>) -> Result<usize> {
        validate_component("cell_id", cell_id)?;
        let mut by_target = BTreeMap::<(String, VertexId, VertexId, Option<RelationshipId>), (EdgeMetadata, Option<EdgeMetadata>)>::new();
        for update in updates {
            validate_component("edge_type", &update.edge_type)?;
            validate_edge_metadata(&update.metadata)?;
            let target = (update.edge_type, update.src, update.dst, update.relationship_id);
            match by_target.get(&target) {
                Some(existing) if existing != &(update.metadata.clone(), update.read.clone()) => {
                    return Err(GraphError::BatchValueConflict {
                        target: format!("relationship {}({}->{}) {:?}", target.0, target.1, target.2, target.3),
                    });
                }
                Some(_) => {}
                None => {
                    by_target.insert(target, (update.metadata, update.read));
                }
            }
        }
        self.ensure_write_authority(cell_id, "set_relationship_metadata_batch")?;
        if by_target.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "set_relationship_metadata_batch",
            by_target.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let updates = by_target
            .into_iter()
            .map(
                |((edge_type, src, dst, relationship_id), (metadata, read))| RelationshipMetadataUpdate {
                    edge_type,
                    src,
                    dst,
                    relationship_id,
                    metadata,
                    read,
                },
            )
            .collect::<Vec<_>>();
        self.run_write_pipeline_fencing_noops(async {
            let _permit = self
                .acquire_graph_write_permit("set_relationship_metadata_batch")
                .instrument(tracing::info_span!(
                    "shard.write_permit",
                    hydradb.cell_id = %cell_id,
                    hydradb.write.operation = "set_relationship_metadata_batch"
                ))
                .await?;
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                let lock = self
                    .acquire_local_write_guard_no_fence(cell_id, "set_relationship_metadata_batch")
                    .await?;
                let result = self
                    .set_relationship_metadata_batch_txn_locked(cell_id, &updates)
                    .instrument(tracing::info_span!("storage.txn"))
                    .await;
                match finish_local_write(lock, result).await {
                    Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                    Ok(changed) => {
                        if changed > 0 {
                            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                        }
                        return Ok(changed);
                    }
                    result => return result,
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
        })
        .await
    }

    #[cfg(feature = "opencypher")]
    async fn set_relationship_metadata_batch_txn_locked(&self, cell_id: &str, updates: &[RelationshipMetadataUpdate]) -> Result<usize> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "set_relationship_metadata_batch")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let current_epoch = txn.seqnum();
        let record_keys = updates
            .iter()
            .filter_map(|update| {
                update
                    .relationship_id
                    .map(|relationship_id| keys::relationship(cell_id, &update.edge_type, update.src, update.dst, relationship_id))
            })
            .collect::<Vec<_>>();
        let edge_metadata_keys = updates
            .iter()
            .filter(|update| update.relationship_id.is_none())
            .map(|update| keys::edge_metadata(cell_id, &update.edge_type, update.src, update.dst))
            .collect::<Vec<_>>();
        let existing = read_txn_remote_many(&txn, record_keys.into_iter().chain(edge_metadata_keys)).await?;

        let mut changed_records = Vec::new();
        let mut changed_edges = Vec::new();
        for update in updates {
            match update.relationship_id {
                Some(relationship_id) => {
                    let key = keys::relationship(cell_id, &update.edge_type, update.src, update.dst, relationship_id);
                    let Some(value) = existing.get(&key).and_then(Option::as_ref) else {
                        // Deleted after the statement read it: a lost race.
                        if update.read.is_some() {
                            return Err(GraphError::ConditionalWriteConflict {
                                operation: "set_relationship_metadata_batch",
                                key,
                            });
                        }
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Mutation,
                            dialect: "GraphQuery",
                            feature: "cannot set metadata for a missing relationship".to_string(),
                        });
                    };
                    let mut record = decode_relationship_record(&key, value)?;
                    if update.read.as_ref().is_some_and(|read| read != &record.metadata) {
                        return Err(GraphError::ConditionalWriteConflict {
                            operation: "set_relationship_metadata_batch",
                            key,
                        });
                    }
                    if record.metadata != update.metadata {
                        let previous = std::mem::replace(&mut record.metadata, update.metadata.clone());
                        changed_records.push((key, record, previous));
                    }
                }
                None => {
                    if edge_epoch_at_txn(&txn, cell_id, &update.edge_type, update.src, update.dst, current_epoch)
                        .await?
                        .is_none()
                    {
                        if update.read.is_some() {
                            return Err(GraphError::ConditionalWriteConflict {
                                operation: "set_relationship_metadata_batch",
                                key: keys::edge_metadata(cell_id, &update.edge_type, update.src, update.dst),
                            });
                        }
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Mutation,
                            dialect: "GraphQuery",
                            feature: format!(
                                "cannot set metadata for missing edge {}({}->{})",
                                update.edge_type, update.src, update.dst
                            ),
                        });
                    }
                    let key = keys::edge_metadata(cell_id, &update.edge_type, update.src, update.dst);
                    let previous = match existing.get(&key).and_then(Option::as_ref) {
                        Some(value) => decode_edge_metadata(&key, value)?,
                        None => EdgeMetadata::default(),
                    };
                    if update.read.as_ref().is_some_and(|read| read != &previous) {
                        return Err(GraphError::ConditionalWriteConflict {
                            operation: "set_relationship_metadata_batch",
                            key,
                        });
                    }
                    if previous != update.metadata {
                        changed_edges.push((update, previous));
                    }
                }
            }
        }
        let changed = changed_records.len() + changed_edges.len();
        if changed == 0 {
            return Ok(0);
        }
        for (key, record, previous) in &changed_records {
            txn.put(key.as_bytes(), encode_relationship_record(record).as_slice())?;
            delete_relationship_property_indexes_txn(&txn, record, previous)?;
            put_relationship_property_indexes_txn(&txn, record)?;
        }
        if !changed_edges.is_empty() {
            let epoch = next_epoch_txn(&txn, cell_id).await?;
            for (update, previous) in &changed_edges {
                apply_edge_metadata_update_txn(
                    &txn,
                    EdgeMetadataTarget {
                        cell_id,
                        edge_type: &update.edge_type,
                        src: update.src,
                        dst: update.dst,
                    },
                    previous,
                    &update.metadata,
                    epoch,
                )?;
            }
        }
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed)
    }

    pub async fn delete_relationship(&self, mutation: EdgeMutation, relationship_id: RelationshipId) -> Result<DeleteResult> {
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_relationship")?;
        self.run_write_pipeline_fencing_noops(self.delete_relationship_inline(mutation, relationship_id))
            .await
    }

    async fn delete_relationship_inline(&self, mutation: EdgeMutation, relationship_id: RelationshipId) -> Result<DeleteResult> {
        let _permit = self
            .acquire_graph_write_permit("delete_relationship")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "delete_relationship"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .delete_relationship_txn(&mutation, relationship_id)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn delete_relationship_txn(&self, mutation: &EdgeMutation, relationship_id: RelationshipId) -> Result<DeleteResult> {
        let lock = self
            .acquire_local_write_guard_no_fence(&mutation.cell_id, "delete_relationship")
            .await?;
        let result = self.delete_relationship_txn_locked(mutation, relationship_id).await;
        finish_local_write(lock, result).await
    }

    async fn delete_relationship_txn_locked(&self, mutation: &EdgeMutation, relationship_id: RelationshipId) -> Result<DeleteResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "delete_relationship")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %mutation.cell_id))
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "relationship-delete", &mutation.idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_delete_idempotency(&idem_key, mutation, relationship_id, &value);
        }

        let current_epoch = txn.seqnum();
        let key = keys::relationship(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst, relationship_id);
        let Some(value) = read_txn_remote(&txn, &key).await? else {
            let result = DeleteResult {
                epoch: current_epoch,
                deleted: false,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_relationship_delete_idempotency(mutation, relationship_id, &result),
            )?;
            commit_txn_traced(
                txn,
                self.await_durable_writes,
                &mutation.cell_id,
                &mutation.edge_type,
                current_epoch,
            )
            .await?;
            return Ok(result);
        };
        let record = decode_relationship_record(&key, &value)?;
        let epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".to_string(),
            reason: "epoch overflow during relationship delete".to_string(),
        })?;
        let other_live_relationships = live_relationships_for_edge_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            current_epoch,
        )
        .await?
        .into_iter()
        .any(|record| record.relationship_id != relationship_id);
        txn.delete(key.as_bytes())?;
        txn.delete(keys::relationship_id(&mutation.cell_id, relationship_id).as_bytes())?;
        let relationship_count_key = keys::relationship_count(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
        let relationship_count = read_counter_txn(&txn, &relationship_count_key).await?.saturating_sub(1);
        if relationship_count == 0 {
            txn.delete(relationship_count_key.as_bytes())?;
        } else {
            txn.put(relationship_count_key.as_bytes(), encode_u64(relationship_count))?;
        }
        delete_relationship_property_indexes_txn(&txn, &record, &record.metadata)?;

        if !other_live_relationships {
            delete_structural_edge_txn(self.writes_reverse_index(), &self.xlog_floor_ensured, &txn, mutation, epoch).await?;
        }

        let result = DeleteResult { epoch, deleted: true };
        txn.put(
            idem_key.as_bytes(),
            encode_relationship_delete_idempotency(mutation, relationship_id, &result),
        )?;
        commit_txn_traced(txn, self.await_durable_writes, &mutation.cell_id, &mutation.edge_type, epoch).await?;
        Ok(result)
    }

    #[cfg(feature = "opencypher")]
    // Retained for internal callers that only consume deletion counts.
    #[allow(dead_code)]
    pub(crate) async fn delete_relationship_mutations_batch(
        &self,
        cell_id: &str,
        deletions: Vec<(EdgeMutation, RelationshipId)>,
    ) -> Result<(u64, u64)> {
        let started = std::time::Instant::now();
        let result = self.delete_relationship_mutations_batch_with_topology(cell_id, deletions).await;
        self.operation_metrics
            .delete_relationship_mutations_batch_latency
            .record_micros(started.elapsed().as_micros() as u64);
        let result = result?;
        Ok((result.deleted, result.already_deleted))
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn delete_relationship_mutations_batch_with_topology(
        &self,
        cell_id: &str,
        deletions: Vec<(EdgeMutation, RelationshipId)>,
    ) -> Result<RelationshipDeleteBatchResult> {
        self.delete_relationship_mutations_batch_with_guards(cell_id, deletions, Vec::new())
            .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn delete_relationship_mutations_batch_with_guards(
        &self,
        cell_id: &str,
        deletions: Vec<(EdgeMutation, RelationshipId)>,
        structural_delete_guards: Vec<EdgeMutation>,
    ) -> Result<RelationshipDeleteBatchResult> {
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "delete_relationship_mutations_batch")?;
        if deletions.is_empty() {
            return Ok(RelationshipDeleteBatchResult::default());
        }
        ensure_limit(
            "delete_relationship_mutations_batch",
            deletions.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        let mutations = deletions.iter().map(|(mutation, _)| mutation.clone()).collect::<Vec<_>>();
        validate_edge_mutations_for_cell(cell_id, &mutations, "delete_relationship_mutations_batch")?;
        ensure_limit(
            "reserve_edge_delete_noops_batch",
            structural_delete_guards.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        validate_edge_mutations_for_cell(cell_id, &structural_delete_guards, "reserve_edge_delete_noops_batch")?;
        validate_unique_delete_mutation_identities(&structural_delete_guards)?;
        let mut identities = BTreeSet::new();
        let mut idempotency_keys = BTreeSet::new();
        for (mutation, relationship_id) in &deletions {
            if !idempotency_keys.insert(&mutation.idempotency_key) {
                return Err(GraphError::IdempotencyConflict {
                    operation: "relationship-delete",
                    idempotency_key: mutation.idempotency_key.clone(),
                    reason: "the same idempotency key appears twice in one batch",
                });
            }
            if !identities.insert((mutation.edge_type.clone(), mutation.src, mutation.dst, *relationship_id)) {
                return Err(GraphError::IdempotencyConflict {
                    operation: "relationship-delete",
                    idempotency_key: mutation.idempotency_key.clone(),
                    reason: "the same relationship appears twice in one batch",
                });
            }
        }
        self.run_write_pipeline_fencing_noops(self.delete_relationship_mutations_batch_with_guards_inline(
            cell_id,
            deletions,
            structural_delete_guards,
        ))
        .await
    }

    #[cfg(feature = "opencypher")]
    async fn delete_relationship_mutations_batch_with_guards_inline(
        &self,
        cell_id: &str,
        deletions: Vec<(EdgeMutation, RelationshipId)>,
        structural_delete_guards: Vec<EdgeMutation>,
    ) -> Result<RelationshipDeleteBatchResult> {
        let mutations = deletions.iter().map(|(mutation, _)| mutation.clone()).collect::<Vec<_>>();
        let span = write_txn_span(cell_id, common_edge_type(&mutations));
        async {
            let mut retries = WriteRetryCount::new();
            // Cancellable, as vertex-delete preparation is: a query that expires
            // while the warm waits on object storage stops here, before it takes
            // the permit, instead of holding query capacity until the warm ends.
            // Any other warm failure is best effort: the guarded reads stay
            // cold, which is today's behaviour, and real storage errors surface
            // again from the guarded transaction.
            match run_cancellable_delete_preparation(self.warm_relationship_delete_reads(
                cell_id,
                &deletions,
                &structural_delete_guards,
            ))
            .await
            {
                Ok(()) => {}
                Err(
                    error @ GraphError::QueryTimeout {
                        operation: "query_cancelled",
                        ..
                    },
                ) => return Err(error).inspect_err(record_error_class),
                Err(error) => {
                    tracing::debug!(%error, "relationship delete warm failed; guarded reads stay cold");
                }
            }
            let _permit = self
                .acquire_graph_write_permit("delete_relationship_mutations_batch")
                .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "delete_relationship_mutations_batch"))
                .await?;
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self
                    .delete_relationship_mutations_batch_txn(cell_id, &deletions, &structural_delete_guards)
                    .instrument(tracing::info_span!("storage.txn"))
                    .await
                {
                    Err(error)
                        if is_retryable_write_conflict(&error)
                            && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                    {
                        self.operation_metrics
                            .write_retries
                            .fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
                        return Ok(result);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
            .inspect_err(record_error_class)
        }
        .instrument(span)
        .await
    }

    /// Warm what the guarded relationship delete reads, before the writer
    /// permit. Production traces put the whole cost of a slow relationship
    /// delete inside the guard, on reads that went to object storage: a few
    /// survivor-check prefix scans (`has_surviving_relationship_txn`, which
    /// scans uncached), and the replay-guard idempotency keys.
    ///
    /// Two parts, running concurrently:
    ///
    /// - point keys: the fence markers, the replay-guard and relationship-delete
    ///   idempotency keys, each relationship record, and for every edge the
    ///   relationship counter plus what a structural delete reads (direct edge,
    ///   edge metadata, degree counters, xlog low-water). The structural keys
    ///   are over-approximated to every edge, since which edges lose their last
    ///   relationship is only known under the guard.
    /// - one survivor-check prefix per edge, read no further than the guarded
    ///   check can: the relationships being deleted, then one more.
    ///
    /// Bounded two ways. A batch of more than `RELATIONSHIP_DELETE_WARM_MAX_ROWS`
    /// deletions is skipped before any key is built, and replay guards beyond
    /// one budget's worth are never turned into keys, so preparation work is
    /// bounded as well as reads. Within that, `RELATIONSHIP_DELETE_WARM_MAX_READS` is split
    /// between the parts rather than raced for (`split_relationship_delete_warm_budget`):
    /// the survivor scans are the measured cost, and a shared allowance let a
    /// point-heavy batch spend it before a single scan ran.
    ///
    /// Same contract as the other warms: values are discarded, and the guarded
    /// transaction reads every key again at its own snapshot, so writes made
    /// after the warm are still seen. Only immutable SST blocks are admitted,
    /// so a cached block cannot go stale. Every read is charged before it is
    /// made; a part that exhausts its share stops warming and the guarded
    /// transaction reads the rest cold, as it always has.
    #[cfg(feature = "opencypher")]
    async fn warm_relationship_delete_reads(
        &self,
        cell_id: &str,
        deletions: &[(EdgeMutation, RelationshipId)],
        structural_delete_guards: &[EdgeMutation],
    ) -> Result<()> {
        if !self.storage.has_block_cache() {
            return Ok(());
        }
        let span = tracing::info_span!(
            "write.reldelete_warm",
            hydradb.reldelete.relationships = deletions.len() as u64,
            hydradb.warm.budget = RELATIONSHIP_DELETE_WARM_MAX_READS as u64,
            hydradb.warm.skipped = tracing::field::Empty,
            hydradb.warm.point_keys = tracing::field::Empty,
            hydradb.warm.prefixes = tracing::field::Empty,
            hydradb.warm.point_reads = tracing::field::Empty,
            hydradb.warm.prefix_reads = tracing::field::Empty,
            hydradb.warm.reads = tracing::field::Empty,
            hydradb.warm.truncated = tracing::field::Empty,
        );
        let skipped = deletions.len() > RELATIONSHIP_DELETE_WARM_MAX_ROWS;
        span.record("hydradb.warm.skipped", skipped);
        if skipped {
            return Ok(());
        }
        let snapshot = self.snapshot(cell_id).instrument(span.clone()).await?;
        let storage = &snapshot.storage_snapshot;
        let read_options = ReadOptions::default();
        let scan_options = ScanOptions::default().with_cache_blocks(true);

        let write_reverse_index = self.writes_reverse_index();
        let mut point_keys = BTreeSet::from([keys::cell_drop_marker(cell_id), keys::cell_drop_pending_marker(cell_id)]);
        // More guard keys than one budget could never all be read.
        point_keys.extend(
            structural_delete_guards
                .iter()
                .take(RELATIONSHIP_DELETE_WARM_MAX_READS)
                .map(|guard| keys::idempotency(cell_id, "delete", &guard.idempotency_key)),
        );
        let mut deleting_by_edge = BTreeMap::<(&str, VertexId, VertexId), BTreeSet<RelationshipId>>::new();
        for (mutation, relationship_id) in deletions {
            point_keys.insert(keys::idempotency(cell_id, "relationship-delete", &mutation.idempotency_key));
            point_keys.insert(keys::relationship(
                cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
                *relationship_id,
            ));
            deleting_by_edge
                .entry((mutation.edge_type.as_str(), mutation.src, mutation.dst))
                .or_default()
                .insert(*relationship_id);
        }
        for (edge_type, src, dst) in deleting_by_edge.keys() {
            point_keys.extend([
                keys::relationship_count(cell_id, edge_type, *src, *dst),
                keys::out_edge(cell_id, edge_type, *src, *dst),
                keys::edge_metadata(cell_id, edge_type, *src, *dst),
                keys::degree_out(cell_id, edge_type, *src),
                keys::xlog_low_water(cell_id, edge_type),
            ]);
            if write_reverse_index {
                point_keys.insert(keys::degree_in(cell_id, edge_type, *dst));
            }
        }
        span.record("hydradb.warm.point_keys", point_keys.len() as u64);
        span.record("hydradb.warm.prefixes", deleting_by_edge.len() as u64);
        // A prefix costs its seek plus at most `deleting + 1` records.
        let prefix_need = deleting_by_edge
            .values()
            .fold(0_usize, |need, deleting| need.saturating_add(deleting.len().saturating_add(2)));
        let (point_share, prefix_share) = split_relationship_delete_warm_budget(point_keys.len(), prefix_need);
        let point_budget = WarmBudget::new(point_share);
        let prefix_budget = WarmBudget::new(prefix_share);

        let warm_points = stream::iter(point_keys)
            .map(|key| {
                let (read_options, budget) = (&read_options, &point_budget);
                async move {
                    tokio::task::consume_budget().await;
                    if !budget.charge() {
                        return Ok::<_, GraphError>(());
                    }
                    storage.get_with_options(key.as_bytes(), read_options).await?;
                    Ok::<_, GraphError>(())
                }
                .boxed()
            })
            .buffer_unordered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
            .boxed()
            .try_for_each(|()| async { Ok(()) });
        let warm_prefixes = stream::iter(&deleting_by_edge)
            .map(|((edge_type, src, dst), deleting)| {
                let (scan_options, budget) = (&scan_options, &prefix_budget);
                async move {
                    tokio::task::consume_budget().await;
                    // The seek is a read too: an empty prefix can still cost an
                    // object-store request.
                    if !budget.charge() {
                        return Ok::<_, GraphError>(());
                    }
                    let prefix = keys::relationship_edge_prefix(cell_id, edge_type, *src, *dst);
                    let mut iter = storage.scan_prefix_with_options(prefix.as_bytes(), .., scan_options).await?;
                    // Read exactly what the guarded check reads: records up to
                    // and including the first one it is not deleting. Each
                    // deleted id appears once, so that is at most every deleted
                    // record and one more. A record that fails to decode stops
                    // the warm too; the guarded check is where it gets reported.
                    loop {
                        if !budget.charge() {
                            break;
                        }
                        let Some(kv) = iter.next().await? else {
                            break;
                        };
                        let key = String::from_utf8_lossy(&kv.key);
                        match decode_relationship_record(&key, &kv.value) {
                            Ok(record) if deleting.contains(&record.relationship_id) => {}
                            _ => break,
                        }
                    }
                    Ok::<_, GraphError>(())
                }
                .boxed()
            })
            .buffer_unordered(RELATIONSHIP_DELETE_SURVIVOR_CONCURRENCY)
            .boxed()
            .try_for_each(|()| async { Ok(()) });

        let warmed = async { tokio::try_join!(warm_points, warm_prefixes) }
            .instrument(span.clone())
            .await;
        // The budget is reported however the warm ended: a failure that stopped
        // it early still spent what it spent.
        span.record("hydradb.warm.point_reads", point_budget.reads());
        span.record("hydradb.warm.prefix_reads", prefix_budget.reads());
        span.record("hydradb.warm.reads", point_budget.reads() + prefix_budget.reads());
        span.record("hydradb.warm.truncated", point_budget.spent() || prefix_budget.spent());
        warmed.map(|_| ())
    }

    #[cfg(feature = "opencypher")]
    async fn delete_relationship_mutations_batch_txn(
        &self,
        cell_id: &str,
        deletions: &[(EdgeMutation, RelationshipId)],
        structural_delete_guards: &[EdgeMutation],
    ) -> Result<RelationshipDeleteBatchResult> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "delete_relationship_mutations_batch")
            .await?;
        let result = self
            .delete_relationship_mutations_batch_txn_locked(cell_id, deletions, structural_delete_guards)
            .await;
        finish_local_write(lock, result).await
    }

    #[cfg(feature = "opencypher")]
    async fn delete_relationship_mutations_batch_txn_locked(
        &self,
        cell_id: &str,
        deletions: &[(EdgeMutation, RelationshipId)],
        structural_delete_guards: &[EdgeMutation],
    ) -> Result<RelationshipDeleteBatchResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "delete_relationship_mutations_batch")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let current_epoch = txn.seqnum();
        // The structural replay guards must become durable with the identified
        // deletions, without a separate commit or a second turn in the write queue.
        reserve_edge_delete_noops_txn(&txn, cell_id, structural_delete_guards)
            .instrument(tracing::info_span!(
                "write.delete_replay_guards",
                items = structural_delete_guards.len()
            ))
            .await?;
        let commit_epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".to_string(),
            reason: "epoch overflow during relationship delete batch".to_string(),
        })?;
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut pending = Vec::new();

        let read_keys = deletions.iter().flat_map(|(mutation, relationship_id)| {
            [
                keys::idempotency(cell_id, "relationship-delete", &mutation.idempotency_key),
                keys::relationship(cell_id, &mutation.edge_type, mutation.src, mutation.dst, *relationship_id),
            ]
        });
        let existing = read_txn_remote_many(&txn, read_keys)
            .instrument(tracing::info_span!("write.delete_canonical_read", items = deletions.len()))
            .await?;
        for (mutation, relationship_id) in deletions {
            let idem_key = keys::idempotency(cell_id, "relationship-delete", &mutation.idempotency_key);
            if let Some(value) = existing.get(&idem_key).and_then(Option::as_ref) {
                let result = decode_relationship_delete_idempotency(&idem_key, mutation, *relationship_id, value)?;
                if result.deleted {
                    deleted = deleted.saturating_add(1);
                } else {
                    already_deleted = already_deleted.saturating_add(1);
                }
                continue;
            }

            let relationship_key = keys::relationship(cell_id, &mutation.edge_type, mutation.src, mutation.dst, *relationship_id);
            let Some(value) = existing.get(&relationship_key).and_then(Option::as_ref) else {
                let result = DeleteResult {
                    epoch: current_epoch,
                    deleted: false,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_relationship_delete_idempotency(mutation, *relationship_id, &result),
                )?;
                already_deleted = already_deleted.saturating_add(1);
                continue;
            };
            let record = decode_relationship_record(&relationship_key, value)?;
            pending.push((mutation.clone(), *relationship_id, relationship_key, idem_key, record));
        }

        let mut deleting_by_edge = BTreeMap::<(String, VertexId, VertexId), BTreeSet<RelationshipId>>::new();
        let mut mutation_by_edge = BTreeMap::<(String, VertexId, VertexId), EdgeMutation>::new();
        for (mutation, relationship_id, _, _, _) in &pending {
            let edge = (mutation.edge_type.clone(), mutation.src, mutation.dst);
            deleting_by_edge.entry(edge.clone()).or_default().insert(*relationship_id);
            mutation_by_edge.entry(edge).or_insert_with(|| mutation.clone());
        }

        // Every read the batch needs before it writes: whether each edge keeps a
        // relationship, and each edge's relationship counter. These ran one
        // edge at a time, and a few cold survivor scans in a batch held the
        // writer guard for seconds. They are read-only and nothing is written
        // until all of them return, so they run concurrently against the one
        // transaction, as `read_txn_remote_many` already does; each still
        // records its read for conflict detection. `buffered` keeps the
        // structural deletes in edge order.
        let survivor_checks = stream::iter(&deleting_by_edge)
            .map(|((edge_type, src, dst), deleting)| {
                let txn = &txn;
                async move {
                    let has_survivor = has_surviving_relationship_txn(txn, cell_id, edge_type, *src, *dst, deleting)
                        .instrument(tracing::info_span!("write.delete_survivor_check", items = deleting.len()))
                        .await?;
                    Ok::<_, GraphError>((!has_survivor).then(|| (edge_type.clone(), *src, *dst)))
                }
                .boxed()
            })
            .buffered(RELATIONSHIP_DELETE_SURVIVOR_CONCURRENCY)
            .boxed()
            .try_collect::<Vec<_>>();
        let count_keys = deleting_by_edge
            .keys()
            .map(|(edge_type, src, dst)| keys::relationship_count(cell_id, edge_type, *src, *dst));
        let count_reads = read_txn_remote_many(&txn, count_keys);
        let (survivors, counts) = tokio::try_join!(survivor_checks, count_reads)?;
        let structural_deletes = survivors.into_iter().flatten().collect::<Vec<_>>();

        for (mutation, relationship_id, relationship_key, idem_key, record) in pending {
            txn.delete(relationship_key.as_bytes())?;
            txn.delete(keys::relationship_id(cell_id, relationship_id).as_bytes())?;
            delete_relationship_property_indexes_txn(&txn, &record, &record.metadata)?;
            let result = DeleteResult {
                epoch: commit_epoch,
                deleted: true,
            };
            txn.put(
                idem_key.as_bytes(),
                encode_relationship_delete_idempotency(&mutation, relationship_id, &result),
            )?;
            deleted = deleted.saturating_add(1);
        }

        for ((edge_type, src, dst), deleting) in &deleting_by_edge {
            let count_key = keys::relationship_count(cell_id, edge_type, *src, *dst);
            let count = match counts.get(&count_key).and_then(Option::as_ref) {
                Some(value) => decode_u64(&count_key, value)?,
                None => 0,
            }
            .saturating_sub(deleting.len() as u64);
            if count == 0 {
                txn.delete(count_key.as_bytes())?;
            } else {
                txn.put(count_key.as_bytes(), encode_u64(count))?;
            }
        }
        let mut topology_changed = false;
        for edge in structural_deletes {
            let mutation = mutation_by_edge
                .get(&edge)
                .expect("structural delete came from a pending relationship");
            topology_changed |=
                delete_structural_edge_txn(self.writes_reverse_index(), &self.xlog_floor_ensured, &txn, mutation, commit_epoch).await?;
        }
        let topology_sequence = topology_changed.then_some(commit_epoch);

        let recorded_epoch = if deleted > 0 { commit_epoch } else { current_epoch };
        let edge_type = deletions
            .first()
            .map(|(mutation, _)| mutation.edge_type.as_str())
            .filter(|first| deletions.iter().all(|(mutation, _)| mutation.edge_type == *first))
            .unwrap_or("");
        commit_txn_traced(txn, self.await_durable_writes, cell_id, edge_type, recorded_epoch).await?;
        Ok(RelationshipDeleteBatchResult {
            deleted,
            already_deleted,
            topology_sequence,
        })
    }

    pub async fn write_edge(&self, mutation: EdgeMutation) -> Result<CommitResult> {
        let _latency = self.operation_metrics.write_edge_latency.start_timer();
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge")?;
        self.run_write_pipeline_fencing_noops(self.write_edge_inline(mutation)).await
    }

    pub(crate) async fn write_edge_inline(&self, mutation: EdgeMutation) -> Result<CommitResult> {
        let span = write_txn_span(&mutation.cell_id, Some(&mutation.edge_type));
        async {
            let mut retries = WriteRetryCount::new();
            let _permit = self
                .acquire_graph_write_permit("write_edge")
                .instrument(
                    tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "write_edge"),
                )
                .await?;
            let _writer = self
                .writer_lane(&mutation.cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self.write_edge_txn(&mutation).instrument(tracing::info_span!("storage.txn")).await {
                    Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                        tracing::Span::current().record("hydradb.commit_epoch", result.epoch);
                        return Ok(result);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
            .inspect_err(record_error_class)
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn write_edge_txn(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
        let lock = self.acquire_local_write_guard_no_fence(&mutation.cell_id, "write_edge").await?;
        let result = self.write_edge_txn_locked(mutation).await;
        finish_local_write(lock, result).await
    }

    async fn write_edge_txn_locked(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
        self.write_edge_txn_locked_with_metadata(mutation, &[], &EdgeMetadata::default(), "write_edge")
            .await
    }

    pub async fn write_edge_with_vertex_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    ) -> Result<CommitResult> {
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge_with_vertex_metadata")?;
        // A self-loop with conflicting endpoint metadata fails here.
        let metadata_updates = coalesce_vertex_metadata_updates([(mutation.src, src_metadata), (mutation.dst, dst_metadata)])?;
        self.run_write_pipeline_fencing_noops(self.write_edge_with_vertex_metadata_inline(mutation, metadata_updates))
            .await
    }

    async fn write_edge_with_vertex_metadata_inline(
        &self,
        mutation: EdgeMutation,
        metadata_updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<CommitResult> {
        let _permit = self
            .acquire_graph_write_permit("write_edge_with_vertex_metadata")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "write_edge_with_vertex_metadata"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_with_vertex_metadata_txn(&mutation, &metadata_updates)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn write_edge_with_vertex_metadata_txn(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
    ) -> Result<CommitResult> {
        let lock = self
            .acquire_local_write_guard_no_fence(&mutation.cell_id, "write_edge_with_vertex_metadata")
            .await?;
        let result = self
            .write_edge_txn_locked_with_metadata(
                mutation,
                metadata_updates,
                &EdgeMetadata::default(),
                "write_edge_with_vertex_metadata",
            )
            .await;
        finish_local_write(lock, result).await
    }

    pub async fn write_edge_with_full_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    ) -> Result<CommitResult> {
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        validate_edge_metadata(&edge_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge_with_full_metadata")?;
        // A self-loop with conflicting endpoint metadata fails here.
        let metadata_updates = coalesce_vertex_metadata_updates([(mutation.src, src_metadata), (mutation.dst, dst_metadata)])?;
        self.run_write_pipeline_fencing_noops(self.write_edge_with_full_metadata_inline(mutation, metadata_updates, edge_metadata))
            .await
    }

    async fn write_edge_with_full_metadata_inline(
        &self,
        mutation: EdgeMutation,
        metadata_updates: Vec<(VertexId, VertexMetadata)>,
        edge_metadata: EdgeMetadata,
    ) -> Result<CommitResult> {
        let _permit = self
            .acquire_graph_write_permit("write_edge_with_full_metadata")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "write_edge_with_full_metadata"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_with_full_metadata_txn(&mutation, &metadata_updates, &edge_metadata)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    async fn write_edge_with_full_metadata_txn(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
    ) -> Result<CommitResult> {
        let lock = self
            .acquire_local_write_guard_no_fence(&mutation.cell_id, "write_edge_with_full_metadata")
            .await?;
        let result = self
            .write_edge_txn_locked_with_metadata(mutation, metadata_updates, edge_metadata, "write_edge_with_full_metadata")
            .await;
        finish_local_write(lock, result).await
    }

    async fn write_edge_txn_locked_with_metadata(
        &self,
        mutation: &EdgeMutation,
        metadata_updates: &[(VertexId, VertexMetadata)],
        edge_metadata: &EdgeMetadata,
        operation: &'static str,
    ) -> Result<CommitResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, operation)
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %mutation.cell_id))
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "create", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_commit_idempotency(&idem_key, mutation, &value);
        }

        let current_epoch = txn.seqnum();
        let existing_edge_epoch = edge_epoch_at_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            current_epoch,
        )
        .await?;

        let mut changed_metadata = Vec::new();
        // Shared with the updates below, which look up the same names.
        let mut generations = WriteBackGenerations::default();
        for (vertex_id, requested) in metadata_updates {
            let vertex_key = keys::vertex(&mutation.cell_id, *vertex_id);
            let existing = decode_vertex_record(&vertex_key, read_txn_remote(&txn, &vertex_key).await?.as_ref())?;
            // Merged into the vertex as reads see it, and compared with that
            // view: write-back values are not in the record.
            let (next, changes) = match &existing {
                Some(record) => {
                    let base = write_back_patch_base_txn(&txn, &mutation.cell_id, *vertex_id, record, &mut generations).await?;
                    let next = merge_vertex_metadata(&base, requested);
                    let changes = next != *base;
                    (next, changes)
                }
                None => (merge_vertex_metadata(&VertexMetadata::default(), requested), true),
            };
            if changes {
                changed_metadata.push((*vertex_id, existing, next));
            }
        }
        let edge_metadata_key = keys::edge_metadata(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
        let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
            Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
            None => EdgeMetadata::default(),
        };
        let next_edge_metadata = merge_edge_metadata(&previous_edge_metadata, edge_metadata);
        let edge_metadata_changed = previous_edge_metadata != next_edge_metadata;

        if let Some(existing_epoch) = existing_edge_epoch {
            if changed_metadata.is_empty() && !edge_metadata_changed {
                let result = CommitResult {
                    epoch: existing_epoch,
                    already_existed: true,
                };
                txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
                commit_txn_traced(txn, self.await_durable_writes, &mutation.cell_id, &mutation.edge_type, result.epoch).await?;
                return Ok(result);
            }
        }

        let epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".to_string(),
            reason: "epoch overflow".to_string(),
        })?;
        let result = CommitResult {
            epoch,
            already_existed: existing_edge_epoch.is_some(),
        };
        for (vertex_id, previous, next) in &changed_metadata {
            apply_vertex_metadata_update_txn(
                &txn,
                &mutation.cell_id,
                *vertex_id,
                previous.as_ref(),
                next,
                epoch,
                &mut generations,
            )
            .await?;
        }
        if edge_metadata_changed {
            apply_edge_metadata_update_txn(
                &txn,
                EdgeMetadataTarget {
                    cell_id: &mutation.cell_id,
                    edge_type: &mutation.edge_type,
                    src: mutation.src,
                    dst: mutation.dst,
                },
                &previous_edge_metadata,
                &next_edge_metadata,
                epoch,
            )?;
        }
        if existing_edge_epoch.is_some() {
            txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
            commit_txn_traced(txn, self.await_durable_writes, &mutation.cell_id, &mutation.edge_type, epoch).await?;
            return Ok(result);
        }

        let record = EdgeRecord {
            cell_id: mutation.cell_id.clone(),
            edge_type: mutation.edge_type.clone(),
            src: mutation.src,
            dst: mutation.dst,
        };
        let edge_value = encode_edge_record(&record);
        let written = metadata_updates.iter().map(|(vertex_id, _)| *vertex_id).collect::<BTreeSet<_>>();
        ensure_vertex_records_txn(&txn, &mutation.cell_id, [mutation.src, mutation.dst], &written, false).await?;
        self.mark_topology_change_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            epoch,
            &[(mutation.src, mutation.dst, true)],
        )
        .await?;
        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let out_degree = read_counter_txn(&txn, &out_degree_key).await? + 1;
        let in_degree = if self.writes_reverse_index() {
            let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key).await? + 1;
            Some((in_degree_key, in_degree))
        } else {
            None
        };

        txn.put(
            keys::out_edge(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
            &edge_value,
        )?;
        if self.writes_reverse_index() {
            txn.put(
                keys::in_edge(&mutation.cell_id, &mutation.edge_type, mutation.dst, mutation.src).as_bytes(),
                &edge_value,
            )?;
        }
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
        txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;

        commit_txn_traced(txn, self.await_durable_writes, &mutation.cell_id, &mutation.edge_type, epoch).await?;
        Ok(result)
    }

    pub async fn delete_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<EdgeDeleteBatchResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        let mut edges: Vec<_> = edges.into_iter().collect();
        ensure_limit("delete_edges_batch", edges.len() as u64, self.limits.max_bulk_import_edges as u64)?;
        edges.sort_unstable();
        edges.dedup();
        let mutations = edges.into_iter().map(|(src, dst)| EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src,
            dst,
            idempotency_key: format!("{idempotency_key}-{src:020}-{dst:020}"),
        });
        self.delete_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn delete_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<EdgeDeleteBatchResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "delete_edges_batch_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }
        ensure_limit(
            "delete_edges_batch_chunk_size",
            chunk_size as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;

        let mut start_epoch: Option<StorageSequence> = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut results = Vec::new();
        let mut chunk_id = 0_usize;
        let mut chunk = Vec::with_capacity(chunk_size);
        for edge in edges {
            chunk.push(edge);
            if chunk.len() < chunk_size {
                continue;
            }
            let result = self
                .delete_edges_batch(
                    cell_id,
                    edge_type,
                    chunk.drain(..),
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                )
                .await?;
            if result.deleted > 0 {
                start_epoch = Some(start_epoch.map_or(result.start_epoch, |epoch| epoch.min(result.start_epoch)));
            }
            end_epoch = end_epoch.max(result.end_epoch);
            deleted = deleted.saturating_add(result.deleted);
            already_deleted = already_deleted.saturating_add(result.already_deleted);
            results.extend(result.results);
            chunk_id += 1;
        }
        if !chunk.is_empty() {
            let result = self
                .delete_edges_batch(
                    cell_id,
                    edge_type,
                    chunk.drain(..),
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                )
                .await?;
            if result.deleted > 0 {
                start_epoch = Some(start_epoch.map_or(result.start_epoch, |epoch| epoch.min(result.start_epoch)));
            }
            end_epoch = end_epoch.max(result.end_epoch);
            deleted = deleted.saturating_add(result.deleted);
            already_deleted = already_deleted.saturating_add(result.already_deleted);
            results.extend(result.results);
        }

        Ok(EdgeDeleteBatchResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            deleted,
            already_deleted,
            results,
        })
    }

    pub async fn delete_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeDeleteBatchResult> {
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, "delete_edge_mutations_batch")?;

        let mutations: Vec<_> = mutations.into_iter().collect();
        if mutations.is_empty() {
            let epoch = self.current_epoch(cell_id).await?;
            return Ok(EdgeDeleteBatchResult {
                start_epoch: epoch,
                end_epoch: epoch,
                deleted: 0,
                already_deleted: 0,
                results: Vec::new(),
            });
        }
        ensure_limit(
            "delete_edge_mutations_batch",
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        validate_edge_mutations_for_cell(cell_id, &mutations, "delete_edge_mutations_batch")?;
        validate_unique_delete_mutation_identities(&mutations)?;
        self.run_write_pipeline_fencing_noops(self.delete_edge_mutations_batch_inline(cell_id, mutations))
            .await
    }

    async fn delete_edge_mutations_batch_inline(&self, cell_id: &str, mutations: Vec<EdgeMutation>) -> Result<EdgeDeleteBatchResult> {
        let span = write_txn_span(cell_id, common_edge_type(&mutations));
        async {
            let mut retries = WriteRetryCount::new();
            let _permit = self
                .acquire_graph_write_permit("delete_edge_mutations_batch")
                .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "delete_edge_mutations_batch"))
                .await?;
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self
                    .delete_edge_mutations_batch_txn(cell_id, &mutations)
                    .instrument(tracing::info_span!("storage.txn"))
                    .await
                {
                    Err(err)
                        if is_retryable_write_conflict(&err)
                            && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                    {
                        self.operation_metrics
                            .write_retries
                            .fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::Span::current().record("hydradb.commit_epoch", result.end_epoch);
                        return Ok(result);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
            .inspect_err(record_error_class)
        }
        .instrument(span)
        .await
    }

    async fn delete_edge_mutations_batch_txn(&self, cell_id: &str, mutations: &[EdgeMutation]) -> Result<EdgeDeleteBatchResult> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "delete_edge_mutations_batch")
            .await?;
        let result = self.delete_edge_mutations_batch_txn_locked(cell_id, mutations).await;
        finish_local_write(lock, result).await
    }

    async fn delete_edge_mutations_batch_txn_locked(&self, cell_id: &str, mutations: &[EdgeMutation]) -> Result<EdgeDeleteBatchResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "delete_edge_mutations_batch")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;

        let current_epoch = txn.seqnum();
        let commit_epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: "storage_sequence".to_string(),
            reason: "SlateDB storage sequence overflow during edge delete batch".to_string(),
        })?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut out_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut segment_edges_by_type_src = BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
        let write_reverse_index = self.writes_reverse_index();

        for mutation in mutations {
            let idem_key = keys::idempotency(cell_id, "delete", &mutation.idempotency_key);
            if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
                let result = decode_delete_idempotency(&idem_key, mutation, &value)?;
                if result.deleted {
                    deleted = deleted.saturating_add(1);
                } else {
                    already_deleted = already_deleted.saturating_add(1);
                }
                results.push(result);
                continue;
            }

            let edge_key = keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            let canonical = match read_txn_remote(&txn, &edge_key).await? {
                Some(value) => Some(decode_edge_record(&edge_key, &value)?),
                None => None,
            };
            let segment_epoch = if canonical.is_none() && !write_reverse_index {
                let cache_key = (mutation.edge_type.clone(), mutation.src);
                match segment_edges_by_type_src.get(&cache_key) {
                    Some(edges) => edges.get(&mutation.dst).copied(),
                    None => {
                        let edges = out_segment_edges_for_src_txn(&txn, cell_id, &mutation.edge_type, mutation.src, current_epoch).await?;
                        let epoch = edges.get(&mutation.dst).copied();
                        segment_edges_by_type_src.insert(cache_key, edges);
                        epoch
                    }
                }
            } else {
                None
            };

            if canonical.is_none() && segment_epoch.is_none() {
                let result = DeleteResult {
                    epoch: current_epoch,
                    deleted: false,
                };
                txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
                already_deleted = already_deleted.saturating_add(1);
                results.push(result);
                continue;
            }

            next_epoch = commit_epoch;
            let result = DeleteResult {
                epoch: next_epoch,
                deleted: true,
            };
            self.mark_topology_change_txn(
                &txn,
                cell_id,
                &mutation.edge_type,
                next_epoch,
                &[(mutation.src, mutation.dst, false)],
            )
            .await?;
            let edge_metadata_key = keys::edge_metadata(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
                Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
                None => EdgeMetadata::default(),
            };
            delete_relationships_for_structural_edge_txn(&txn, mutation, current_epoch).await?;
            if !previous_edge_metadata.properties.is_empty() {
                apply_edge_metadata_update_txn(
                    &txn,
                    EdgeMetadataTarget {
                        cell_id,
                        edge_type: &mutation.edge_type,
                        src: mutation.src,
                        dst: mutation.dst,
                    },
                    &previous_edge_metadata,
                    &EdgeMetadata::default(),
                    next_epoch,
                )?;
            }

            if canonical.is_some() {
                txn.delete(keys::edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes())?;
                txn.delete(edge_key.as_bytes())?;
                if write_reverse_index {
                    txn.delete(keys::in_edge(cell_id, &mutation.edge_type, mutation.dst, mutation.src).as_bytes())?;
                }
                if write_reverse_index {
                    *in_decrements.entry((mutation.edge_type.clone(), mutation.dst)).or_insert(0) += 1;
                }
            } else {
                txn.put(
                    keys::out_segment_tombstone(cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
                    encode_u64(next_epoch),
                )?;
            }
            *out_decrements.entry((mutation.edge_type.clone(), mutation.src)).or_insert(0) += 1;
            txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
            deleted = deleted.saturating_add(1);
            results.push(result);
        }
        for ((edge_type, src), decrement) in out_decrements {
            let key = keys::degree_out(cell_id, &edge_type, src);
            let base = read_counter_txn(&txn, &key).await?;
            txn.put(key.as_bytes(), encode_u64(base.saturating_sub(decrement)))?;
        }
        if write_reverse_index {
            for ((edge_type, dst), decrement) in in_decrements {
                let key = keys::degree_in(cell_id, &edge_type, dst);
                let base = read_counter_txn(&txn, &key).await?;
                txn.put(key.as_bytes(), encode_u64(base.saturating_sub(decrement)))?;
            }
        }
        let deleted_start_epoch = results.iter().filter(|result| result.deleted).map(|result| result.epoch).min();
        let deleted_end_epoch = results.iter().filter(|result| result.deleted).map(|result| result.epoch).max();
        commit_txn_traced(txn, self.await_durable_writes, cell_id, "", commit_epoch).await?;
        Ok(EdgeDeleteBatchResult {
            start_epoch: deleted_start_epoch.unwrap_or(current_epoch),
            end_epoch: deleted_end_epoch.unwrap_or(next_epoch),
            deleted,
            already_deleted,
            results,
        })
    }

    pub async fn delete_edge(&self, mutation: EdgeMutation) -> Result<DeleteResult> {
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_edge")?;
        self.run_write_pipeline_fencing_noops(self.delete_edge_inline(mutation)).await
    }

    async fn delete_edge_inline(&self, mutation: EdgeMutation) -> Result<DeleteResult> {
        let span = write_txn_span(&mutation.cell_id, Some(&mutation.edge_type));
        async {
            let mut retries = WriteRetryCount::new();
            let _permit = self
                .acquire_graph_write_permit("delete_edge")
                .instrument(
                    tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "delete_edge"),
                )
                .await?;
            let _writer = self
                .writer_lane(&mutation.cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self.delete_edge_txn(&mutation).instrument(tracing::info_span!("storage.txn")).await {
                    Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                        tracing::Span::current().record("hydradb.commit_epoch", result.epoch);
                        return Ok(result);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
            .inspect_err(record_error_class)
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn delete_edge_txn(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let lock = self.acquire_local_write_guard_no_fence(&mutation.cell_id, "delete_edge").await?;
        let result = self.delete_edge_txn_locked(mutation).await;
        finish_local_write(lock, result).await
    }

    async fn delete_edge_txn_locked(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "delete_edge").await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "delete", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_delete_idempotency(&idem_key, mutation, &value);
        }

        let canonical_key = keys::edge(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
        let edge_key = keys::out_edge(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);

        let Some(existing) = read_txn_remote(&txn, &edge_key).await? else {
            let current_epoch = txn.seqnum();
            let segment_edge = if self.writes_reverse_index() {
                None
            } else {
                self.out_segment_edge_record_at(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst, current_epoch)
                    .await?
            };
            let Some((segment_sequence, _segment_edge)) = segment_edge else {
                let result = DeleteResult {
                    epoch: current_epoch,
                    deleted: false,
                };
                txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
                commit_txn_traced(
                    txn,
                    self.await_durable_writes,
                    &mutation.cell_id,
                    &mutation.edge_type,
                    current_epoch,
                )
                .await?;
                return Ok(result);
            };
            let tombstone_key = keys::out_segment_tombstone(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            if let Some(value) = read_txn_remote(&txn, &tombstone_key).await? {
                let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
                if !segment_edge_visible(segment_sequence, Some(tombstone_epoch)) {
                    let result = DeleteResult {
                        epoch: current_epoch,
                        deleted: false,
                    };
                    txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
                    commit_txn_traced(
                        txn,
                        self.await_durable_writes,
                        &mutation.cell_id,
                        &mutation.edge_type,
                        current_epoch,
                    )
                    .await?;
                    return Ok(result);
                }
            }
            let epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
                key: "storage_sequence".to_string(),
                reason: "epoch overflow".to_string(),
            })?;
            let result = DeleteResult { epoch, deleted: true };
            self.mark_topology_change_txn(
                &txn,
                &mutation.cell_id,
                &mutation.edge_type,
                epoch,
                &[(mutation.src, mutation.dst, false)],
            )
            .await?;
            let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
            let out_degree = read_counter_txn(&txn, &out_degree_key).await?.saturating_sub(1);
            let edge_metadata_key = keys::edge_metadata(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
                Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
                None => EdgeMetadata::default(),
            };

            delete_relationships_for_structural_edge_txn(&txn, mutation, current_epoch).await?;
            if !previous_edge_metadata.properties.is_empty() {
                apply_edge_metadata_update_txn(
                    &txn,
                    EdgeMetadataTarget {
                        cell_id: &mutation.cell_id,
                        edge_type: &mutation.edge_type,
                        src: mutation.src,
                        dst: mutation.dst,
                    },
                    &previous_edge_metadata,
                    &EdgeMetadata::default(),
                    epoch,
                )?;
            }
            txn.put(tombstone_key.as_bytes(), encode_u64(epoch))?;
            txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
            txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;
            commit_txn_traced(txn, self.await_durable_writes, &mutation.cell_id, &mutation.edge_type, epoch).await?;
            return Ok(result);
        };

        decode_edge_record(&edge_key, &existing)?;
        let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
        let result = DeleteResult { epoch, deleted: true };
        self.mark_topology_change_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            epoch,
            &[(mutation.src, mutation.dst, false)],
        )
        .await?;

        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let out_degree = read_counter_txn(&txn, &out_degree_key).await?.saturating_sub(1);
        let in_degree = if self.writes_reverse_index() {
            let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key).await?.saturating_sub(1);
            Some((in_degree_key, in_degree))
        } else {
            None
        };
        let edge_metadata_key = keys::edge_metadata(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst);
        let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
            Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
            None => EdgeMetadata::default(),
        };

        delete_relationships_for_structural_edge_txn(&txn, mutation, epoch.saturating_sub(1)).await?;
        if !previous_edge_metadata.properties.is_empty() {
            apply_edge_metadata_update_txn(
                &txn,
                EdgeMetadataTarget {
                    cell_id: &mutation.cell_id,
                    edge_type: &mutation.edge_type,
                    src: mutation.src,
                    dst: mutation.dst,
                },
                &previous_edge_metadata,
                &EdgeMetadata::default(),
                epoch,
            )?;
        }
        txn.delete(canonical_key.as_bytes())?;
        txn.delete(keys::out_edge(&mutation.cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes())?;
        txn.delete(keys::in_edge(&mutation.cell_id, &mutation.edge_type, mutation.dst, mutation.src).as_bytes())?;
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
        txn.put(idem_key.as_bytes(), encode_delete_idempotency(mutation, &result))?;

        commit_txn_traced(txn, self.await_durable_writes, &mutation.cell_id, &mutation.edge_type, epoch).await?;
        Ok(result)
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_with_options(cell_id, edge_type, edges, idempotency_key, BulkImportOptions::default())
            .await
    }

    pub async fn bulk_append_edges_trusted(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_append_edges_trusted_bounded(cell_id, edge_type, edges, idempotency_key, DEFAULT_TRUSTED_APPEND_CHUNK_EDGES)
            .await
    }

    pub async fn bulk_append_edges_trusted_bounded(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        max_edges_per_commit: usize,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if max_edges_per_commit == 0 {
            return Err(GraphError::CorruptValue {
                key: "trusted_append_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }
        let edges: Vec<_> = edges.into_iter().collect();
        if edges.len() > max_edges_per_commit {
            return self
                .bulk_import_edges_chunked_with_options(
                    cell_id,
                    edge_type,
                    edges,
                    idempotency_key,
                    max_edges_per_commit,
                    BulkImportOptions::checked_batch_append(),
                )
                .await;
        }
        self.bulk_import_edges_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            BulkImportOptions::checked_batch_append(),
        )
        .await
    }

    /// Appends outbound adjacency under an explicit operation identity.
    ///
    /// Reusing `idempotency_key` returns the original result without changing
    /// graph state. A different key is a new append intent, even when its
    /// content matches an earlier import, and restores requested edges that
    /// were deleted after that import.
    pub async fn bulk_append_out_adjacency_segment_trusted(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: impl IntoIterator<Item = VertexId>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if self.writes_reverse_index() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "GraphWrite",
                feature: "segment trusted append requires outbound-only index policy".to_string(),
            });
        }
        self.ensure_write_authority(cell_id, "bulk_append_out_adjacency_segment_trusted")?;

        let mut dsts: Vec<_> = dsts.into_iter().collect();
        ensure_limit(
            "bulk_append_out_adjacency_segment_trusted",
            dsts.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        dsts.sort_unstable();
        dsts.dedup();
        let edges: Vec<_> = dsts.iter().copied().map(|dst| (src, dst)).collect();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _permit = self
            .acquire_graph_write_permit("bulk_append_out_adjacency_segment_trusted")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "bulk_append_out_adjacency_segment_trusted"))
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_append_out_adjacency_segment_trusted_txn(cell_id, edge_type, src, &dsts, idempotency_key, fingerprint)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn bulk_import_edges_with_options(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "bulk_import_edges")?;

        let mut edges: Vec<_> = edges.into_iter().collect();
        ensure_limit("bulk_import_edges", edges.len() as u64, self.limits.max_bulk_import_edges as u64)?;
        edges.sort_unstable();
        edges.dedup();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _permit = self
            .acquire_graph_write_permit("bulk_import_edges")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "bulk_import_edges"),
            )
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_import_edges_txn(cell_id, edge_type, &edges, idempotency_key, fingerprint, options)
                .instrument(tracing::info_span!("storage.txn"))
                .await
            {
                Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                    self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                    return Ok(result);
                }
                result => return result,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "graph transaction",
            attempts: GRAPH_TXN_MAX_RETRIES,
        })
    }

    pub async fn write_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
    ) -> Result<EdgeMutationBatchResult> {
        self.write_edge_mutations_batch_with_endpoint_labels(cell_id, mutations.into_iter().collect(), None, "write_edge_mutations_batch")
            .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn write_edge_mutations_batch_between_labeled_vertices(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
        source_label: &str,
        destination_label: &str,
    ) -> Result<EdgeMutationBatchResult> {
        validate_component("source_label", source_label)?;
        validate_component("destination_label", destination_label)?;
        self.write_edge_mutations_batch_with_endpoint_labels(
            cell_id,
            mutations.into_iter().collect(),
            Some((source_label, destination_label)),
            "write_edge_mutations_batch_between_labeled_vertices",
        )
        .await
    }

    async fn write_edge_mutations_batch_with_endpoint_labels(
        &self,
        cell_id: &str,
        mutations: Vec<EdgeMutation>,
        endpoint_labels: Option<(&str, &str)>,
        operation: &'static str,
    ) -> Result<EdgeMutationBatchResult> {
        let _latency = self.operation_metrics.write_edge_mutations_batch_latency.start_timer();
        // Validate before pipeline admission: a rejected request must not
        // wait for other writes' WAL uploads or hold a pending-write slot.
        validate_component("cell_id", cell_id)?;
        self.ensure_write_authority(cell_id, operation)?;

        if mutations.is_empty() {
            let epoch = self.current_epoch(cell_id).await?;
            return Ok(EdgeMutationBatchResult {
                start_epoch: epoch,
                end_epoch: epoch,
                inserted: 0,
                already_existed: 0,
                results: Vec::new(),
            });
        }
        ensure_limit(operation, mutations.len() as u64, self.limits.max_bulk_import_edges as u64)?;
        for mutation in &mutations {
            validate_component("cell_id", &mutation.cell_id)?;
            validate_component("edge_type", &mutation.edge_type)?;
            validate_component("idempotency_key", &mutation.idempotency_key)?;
            if mutation.cell_id != cell_id {
                return Err(GraphError::CorruptValue {
                    key: format!("cell/{cell_id}/write_edge_mutations_batch"),
                    reason: format!("batch contains mutation for different cell {}", mutation.cell_id),
                });
            }
        }
        self.run_write_pipeline_fencing_noops(self.write_edge_mutations_batch_with_endpoint_labels_inline(
            cell_id,
            mutations,
            endpoint_labels,
            operation,
        ))
        .await
    }

    async fn write_edge_mutations_batch_with_endpoint_labels_inline(
        &self,
        cell_id: &str,
        mutations: Vec<EdgeMutation>,
        endpoint_labels: Option<(&str, &str)>,
        operation: &'static str,
    ) -> Result<EdgeMutationBatchResult> {
        let span = write_txn_span(cell_id, common_edge_type(&mutations));
        async {
            let mut retries = WriteRetryCount::new();
            let _permit = self
                .acquire_graph_write_permit(operation)
                .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = operation))
                .await?;
            let _writer = self
                .writer_lane(cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self
                    .write_edge_mutations_batch_txn(cell_id, &mutations, operation, endpoint_labels)
                    .instrument(tracing::info_span!("storage.txn"))
                    .await
                {
                    Err(err) if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES => {
                        self.operation_metrics.write_retries.fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        tokio::task::yield_now().await;
                    }
                    Ok(result) => {
                        self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                        tracing::Span::current().record("hydradb.commit_epoch", result.end_epoch);
                        return Ok(result);
                    }
                    result => return result.inspect_err(record_error_class),
                }
            }
            Err(GraphError::RetryExhausted {
                operation: "graph transaction",
                attempts: GRAPH_TXN_MAX_RETRIES,
            })
            .inspect_err(record_error_class)
        }
        .instrument(span)
        .await
    }

    pub async fn ingest_edge_mutations(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = EdgeMutation>,
        options: EdgeIngestOptions,
    ) -> Result<EdgeIngestResult> {
        validate_component("cell_id", cell_id)?;
        if options.batch_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "edge_ingest_batch_size".to_string(),
                reason: "batch size must be greater than zero".to_string(),
            });
        }
        if self.limits.max_bulk_import_edges == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "ingest_edge_mutations",
                actual: 1,
                limit: 0,
            });
        }

        let batch_size = options.batch_size.min(self.limits.max_bulk_import_edges);
        let mut chunk = Vec::with_capacity(batch_size);
        let mut start_epoch = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;
        let mut batches = 0_u64;
        let mut mutations_seen = 0_u64;

        for mutation in mutations {
            mutations_seen = mutations_seen.saturating_add(1);
            chunk.push(mutation);
            if chunk.len() == batch_size {
                let result = self.write_edge_mutations_batch(cell_id, std::mem::take(&mut chunk)).await?;
                merge_ingest_batch(
                    &result,
                    &mut start_epoch,
                    &mut end_epoch,
                    &mut inserted,
                    &mut already_existed,
                    &mut batches,
                );
                chunk = Vec::with_capacity(batch_size);
            }
        }
        if !chunk.is_empty() {
            let result = self.write_edge_mutations_batch(cell_id, chunk).await?;
            merge_ingest_batch(
                &result,
                &mut start_epoch,
                &mut end_epoch,
                &mut inserted,
                &mut already_existed,
                &mut batches,
            );
        }

        Ok(EdgeIngestResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            inserted,
            already_existed,
            batches,
            mutations: mutations_seen,
        })
    }

    pub(crate) async fn write_edge_mutations_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
        endpoint_labels: Option<(&str, &str)>,
    ) -> Result<EdgeMutationBatchResult> {
        let lock = self.acquire_local_write_guard_no_fence(cell_id, operation).await?;
        let result = self
            .write_edge_mutations_batch_txn_locked(cell_id, mutations, operation, endpoint_labels)
            .await;
        finish_local_write(lock, result).await
    }

    async fn write_edge_mutations_batch_txn_locked(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
        operation: &'static str,
        endpoint_labels: Option<(&str, &str)>,
    ) -> Result<EdgeMutationBatchResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;

        let mut idempotency_keys = BTreeSet::new();
        for mutation in mutations {
            if !idempotency_keys.insert(mutation.idempotency_key.clone()) {
                return Err(GraphError::IdempotencyConflict {
                    operation: "create",
                    idempotency_key: mutation.idempotency_key.clone(),
                    reason: "the same key appears twice in one batch",
                });
            }
        }

        let current_epoch = txn.seqnum();
        let commit_epoch = current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
            key: operation.to_string(),
            reason: "SlateDB storage sequence overflow during edge mutation batch".to_string(),
        })?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut known_edges = BTreeMap::<(String, VertexId, VertexId), StorageSequence>::new();
        let mut validated_endpoints = BTreeSet::<(VertexId, String)>::new();
        let mut segment_edges_by_type_src = BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
        let mut out_increments = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_increments = BTreeMap::<(String, VertexId), u64>::new();
        let write_reverse_index = self.writes_reverse_index();
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;
        // Endpoints of inserted edges. A labelled batch has already proven its
        // endpoints exist, so only an unlabelled one establishes them.
        let mut inserted_endpoints = BTreeSet::<VertexId>::new();

        for mutation in mutations {
            let idem_key = keys::idempotency(cell_id, "create", &mutation.idempotency_key);
            if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
                let result = decode_commit_idempotency(&idem_key, mutation, &value)?;
                if result.already_existed {
                    already_existed = already_existed.saturating_add(1);
                } else {
                    inserted = inserted.saturating_add(1);
                }
                results.push(result);
                continue;
            }

            if let Some((source_label, destination_label)) = endpoint_labels {
                for (vertex, label) in [(mutation.src, source_label), (mutation.dst, destination_label)] {
                    if !validated_endpoints.insert((vertex, label.to_string())) {
                        continue;
                    }
                    let key = keys::vertex(cell_id, vertex);
                    let Some(value) = read_txn_remote(&txn, &key).await? else {
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Mutation,
                            dialect: "OpenCypher",
                            feature: format!("MATCH endpoint vertex {vertex} with label {label} does not exist"),
                        });
                    };
                    let metadata = decode_vertex_metadata(&key, &value)?;
                    if !metadata.labels.contains(label) {
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Mutation,
                            dialect: "OpenCypher",
                            feature: format!("MATCH endpoint vertex {vertex} does not have label {label}"),
                        });
                    }
                }
            }

            let identity = (mutation.edge_type.clone(), mutation.src, mutation.dst);
            if let Some(epoch) = known_edges.get(&identity).copied() {
                let result = CommitResult {
                    epoch,
                    already_existed: true,
                };
                txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            let edge_key = keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
            if let Some(value) = read_txn_remote(&txn, &edge_key).await? {
                decode_edge_record(&edge_key, &value)?;
                let result = CommitResult {
                    epoch: current_epoch,
                    already_existed: true,
                };
                known_edges.insert(identity, current_epoch);
                txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            let segment_cache_key = (mutation.edge_type.clone(), mutation.src);
            let segment_epoch = match segment_edges_by_type_src.get(&segment_cache_key) {
                Some(edges) => edges.get(&mutation.dst).copied(),
                None => {
                    let edges = out_segment_edges_for_src_txn(&txn, cell_id, &mutation.edge_type, mutation.src, current_epoch).await?;
                    let epoch = edges.get(&mutation.dst).copied();
                    segment_edges_by_type_src.insert(segment_cache_key, edges);
                    epoch
                }
            };
            if let Some(epoch) = segment_epoch {
                let result = CommitResult {
                    epoch,
                    already_existed: true,
                };
                known_edges.insert(identity, epoch);
                txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            next_epoch = commit_epoch;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: mutation.edge_type.clone(),
                src: mutation.src,
                dst: mutation.dst,
            };
            let result = CommitResult {
                epoch: next_epoch,
                already_existed: false,
            };
            let edge_value = encode_edge_record(&record);
            self.mark_topology_change_txn(
                &txn,
                cell_id,
                &mutation.edge_type,
                next_epoch,
                &[(mutation.src, mutation.dst, true)],
            )
            .await?;
            txn.put(
                keys::out_edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
                &edge_value,
            )?;
            if write_reverse_index {
                txn.put(
                    keys::in_edge(cell_id, &mutation.edge_type, mutation.dst, mutation.src).as_bytes(),
                    &edge_value,
                )?;
            }
            txn.put(idem_key.as_bytes(), encode_commit_idempotency(mutation, &result))?;
            known_edges.insert(identity, next_epoch);
            if endpoint_labels.is_none() {
                inserted_endpoints.extend([mutation.src, mutation.dst]);
            }
            *out_increments.entry((mutation.edge_type.clone(), mutation.src)).or_insert(0) += 1;
            if write_reverse_index {
                *in_increments.entry((mutation.edge_type.clone(), mutation.dst)).or_insert(0) += 1;
            }
            inserted = inserted.saturating_add(1);
            results.push(result);
        }

        for ((edge_type, src), increment) in out_increments {
            let key = keys::degree_out(cell_id, &edge_type, src);
            let base = read_counter_txn(&txn, &key).await?;
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for ((edge_type, dst), increment) in in_increments {
                let key = keys::degree_in(cell_id, &edge_type, dst);
                let base = read_counter_txn(&txn, &key).await?;
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        ensure_vertex_records_txn(&txn, cell_id, inserted_endpoints, &BTreeSet::new(), current_epoch == 0).await?;
        let inserted_start_epoch = results
            .iter()
            .filter(|result| !result.already_existed)
            .map(|result| result.epoch)
            .min();
        let inserted_end_epoch = results
            .iter()
            .filter(|result| !result.already_existed)
            .map(|result| result.epoch)
            .max();
        commit_txn_traced(txn, self.await_durable_writes, cell_id, "", commit_epoch).await?;
        Ok(EdgeMutationBatchResult {
            start_epoch: inserted_start_epoch.unwrap_or(current_epoch),
            end_epoch: inserted_end_epoch.unwrap_or(next_epoch),
            inserted,
            already_existed,
            results,
        })
    }

    pub async fn write_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges(cell_id, edge_type, edges, idempotency_key).await
    }

    pub async fn write_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn bulk_import_edges_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked_with_options(cell_id, edge_type, edges, idempotency_key, chunk_size, BulkImportOptions::default())
            .await
    }

    pub async fn bulk_append_edges_trusted_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_chunked_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            chunk_size,
            BulkImportOptions::checked_batch_append(),
        )
        .await
    }

    pub async fn bulk_import_edges_chunked_with_options(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("idempotency_key", idempotency_key)?;
        if chunk_size == 0 {
            return Err(GraphError::CorruptValue {
                key: "bulk_import_chunk_size".to_string(),
                reason: "chunk size must be greater than zero".to_string(),
            });
        }

        let mut edges: Vec<_> = edges.into_iter().collect();
        edges.sort_unstable_by_key(|(src, dst)| (bulk_import_chunk_order(*src, *dst), *src, *dst));
        edges.dedup();

        let mut start_epoch = None;
        let mut end_epoch = self.current_epoch(cell_id).await?;
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;
        let mut chunk = Vec::with_capacity(chunk_size);
        let mut chunk_id = 0_u64;
        for edge in edges {
            chunk.push(edge);
            if chunk.len() == chunk_size {
                let result = self
                    .bulk_import_edges_with_options(
                        cell_id,
                        edge_type,
                        std::mem::take(&mut chunk),
                        &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                        options,
                    )
                    .await?;
                start_epoch.get_or_insert(result.start_epoch);
                end_epoch = result.end_epoch;
                inserted = inserted.saturating_add(result.inserted);
                already_existed = already_existed.saturating_add(result.already_existed);
                chunk_id = chunk_id.saturating_add(1);
                chunk = Vec::with_capacity(chunk_size);
            }
        }
        if !chunk.is_empty() {
            let result = self
                .bulk_import_edges_with_options(
                    cell_id,
                    edge_type,
                    chunk,
                    &format!("{idempotency_key}-chunk-{chunk_id:020}"),
                    options,
                )
                .await?;
            start_epoch.get_or_insert(result.start_epoch);
            end_epoch = result.end_epoch;
            inserted = inserted.saturating_add(result.inserted);
            already_existed = already_existed.saturating_add(result.already_existed);
        }
        crate::engine::trim_process_memory_after_hydration();

        Ok(BulkImportResult {
            start_epoch: start_epoch.unwrap_or(end_epoch),
            end_epoch,
            inserted,
            already_existed,
        })
    }

    pub(crate) async fn bulk_append_out_adjacency_segment_trusted_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: &[VertexId],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<BulkImportResult> {
        let lock = self
            .acquire_local_write_guard(cell_id, "bulk_append_out_adjacency_segment_trusted")
            .await?;
        let result = self
            .bulk_append_out_adjacency_segment_trusted_txn_locked(cell_id, edge_type, src, dsts, idempotency_key, fingerprint)
            .await;
        finish_local_write(lock, result).await
    }

    async fn bulk_append_out_adjacency_segment_trusted_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: &[VertexId],
        idempotency_key: &str,
        fingerprint: u64,
    ) -> Result<BulkImportResult> {
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_append_out_adjacency_segment_trusted")
            .await?;
        let idem_key = keys::idempotency(cell_id, "segment-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }
        let current_epoch = txn.seqnum();
        let existing = out_neighbors_for_src_txn(&txn, cell_id, edge_type, src, current_epoch).await?;
        let inserted_dsts: Vec<_> = dsts.iter().copied().filter(|dst| !existing.contains(dst)).collect();
        let already_existed = u64::try_from(dsts.len().saturating_sub(inserted_dsts.len())).map_err(|err| GraphError::CorruptValue {
            key: "segment_import".to_string(),
            reason: format!("too many existing edges in one segment import: {err}"),
        })?;
        let inserted = u64::try_from(inserted_dsts.len()).map_err(|err| GraphError::CorruptValue {
            key: "segment_import".to_string(),
            reason: format!("too many edges in one segment import: {err}"),
        })?;
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
                key: "segment_import".to_string(),
                reason: "SlateDB storage sequence overflow during segment import".to_string(),
            })?
        };
        let end_epoch = start_epoch;
        let result = BulkImportResult {
            start_epoch,
            end_epoch,
            inserted,
            already_existed,
        };

        if inserted > 0 {
            ensure_vertex_records_txn(
                &txn,
                cell_id,
                std::iter::once(src).chain(inserted_dsts.iter().copied()),
                &BTreeSet::new(),
                current_epoch == 0,
            )
            .await?;
            let changes: Vec<(VertexId, VertexId, bool)> = inserted_dsts.iter().map(|dst| (src, *dst, true)).collect();
            self.mark_topology_change_txn(&txn, cell_id, edge_type, end_epoch, &changes).await?;
            for dst in &inserted_dsts {
                txn.delete(keys::out_segment_tombstone(cell_id, edge_type, src, *dst).as_bytes())?;
            }
            txn.put(
                keys::out_segment(cell_id, edge_type, src, end_epoch, idempotency_key).as_bytes(),
                encode_out_edge_segment_records(&inserted_dsts),
            )?;
            let degree_key = keys::degree_out(cell_id, edge_type, src);
            let base = if current_epoch == 0 {
                0
            } else {
                read_counter_txn(&txn, &degree_key).await?
            };
            txn.put(degree_key.as_bytes(), encode_u64(base + inserted))?;
        }
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    pub(crate) async fn bulk_import_edges_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: &[(VertexId, VertexId)],
        idempotency_key: &str,
        fingerprint: u64,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        let lock = self.acquire_local_write_guard(cell_id, "bulk_import_edges").await?;
        let result = self
            .bulk_import_edges_txn_locked(cell_id, edge_type, edges, idempotency_key, fingerprint, options)
            .await;
        finish_local_write(lock, result).await
    }

    async fn bulk_import_edges_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: &[(VertexId, VertexId)],
        idempotency_key: &str,
        fingerprint: u64,
        options: BulkImportOptions,
    ) -> Result<BulkImportResult> {
        let preflight_started = std::time::Instant::now();
        let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_import_edges").await?;
        let idem_key = keys::idempotency(cell_id, "bulk-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }

        let current_epoch = txn.seqnum();
        let fresh_cell = current_epoch == 0;
        let mut already_existed = 0_u64;
        let mut inserted_edges = Vec::new();
        let mut segment_neighbors_by_src = BTreeMap::<VertexId, BTreeSet<VertexId>>::new();
        for (src, dst) in edges.iter().copied() {
            if options.duplicate_policy.check_existing() && !fresh_cell {
                if read_txn_remote(&txn, &keys::out_edge(cell_id, edge_type, src, dst))
                    .await?
                    .is_some()
                {
                    already_existed += 1;
                    continue;
                }
                let segment_exists = match segment_neighbors_by_src.get(&src) {
                    Some(neighbors) => neighbors.contains(&dst),
                    None => {
                        let neighbors = out_segment_neighbors_for_src_txn(&txn, cell_id, edge_type, src, current_epoch).await?;
                        let exists = neighbors.contains(&dst);
                        segment_neighbors_by_src.insert(src, neighbors);
                        exists
                    }
                };
                if segment_exists {
                    already_existed += 1;
                    continue;
                }
            }
            inserted_edges.push((src, dst));
        }
        let preflight_elapsed = preflight_started.elapsed();

        let inserted = u64::try_from(inserted_edges.len()).map_err(|err| GraphError::CorruptValue {
            key: "bulk_import".to_string(),
            reason: format!("too many edges in one import: {err}"),
        })?;
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch.checked_add(1).ok_or_else(|| GraphError::CorruptValue {
                key: "bulk_import".to_string(),
                reason: "SlateDB storage sequence overflow during bulk import".to_string(),
            })?
        };
        let end_epoch = start_epoch;
        let result = BulkImportResult {
            start_epoch,
            end_epoch,
            inserted,
            already_existed,
        };

        let write_reverse_index = self.writes_reverse_index();
        let mut out_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        let mut in_increments = std::collections::BTreeMap::<VertexId, u64>::new();
        let batch_build_started = std::time::Instant::now();
        for (src, dst) in inserted_edges.iter().copied() {
            let epoch = end_epoch;
            let record = EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
            };
            let edge_value = encode_edge_record(&record);
            self.mark_topology_change_txn(&txn, cell_id, edge_type, epoch, &[(src, dst, true)])
                .await?;
            txn.put(keys::out_edge(cell_id, edge_type, src, dst).as_bytes(), &edge_value)?;
            if write_reverse_index {
                txn.put(keys::in_edge(cell_id, edge_type, dst, src).as_bytes(), &edge_value)?;
            }
            *out_increments.entry(src).or_insert(0) += 1;
            if write_reverse_index {
                *in_increments.entry(dst).or_insert(0) += 1;
            }
        }
        let batch_build_elapsed = batch_build_started.elapsed();

        let counter_read_started = std::time::Instant::now();
        for (src, increment) in out_increments {
            let key = keys::degree_out(cell_id, edge_type, src);
            let base = if fresh_cell { 0 } else { read_counter_txn(&txn, &key).await? };
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for (dst, increment) in in_increments {
                let key = keys::degree_in(cell_id, edge_type, dst);
                let base = if fresh_cell { 0 } else { read_counter_txn(&txn, &key).await? };
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        let counter_read_elapsed = counter_read_started.elapsed();
        ensure_vertex_records_txn(
            &txn,
            cell_id,
            inserted_edges.iter().flat_map(|(src, dst)| [*src, *dst]),
            &BTreeSet::new(),
            fresh_cell,
        )
        .await?;
        if inserted > 0 {
            // The per-edge loop above already logged each inserted edge to the
            // xlog at this same epoch; this re-mark only refreshes the dirty
            // marker, so it carries no changes.
            self.mark_topology_change_txn(&txn, cell_id, edge_type, end_epoch, &[]).await?;
        }
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;

        let commit_started = std::time::Instant::now();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        let commit_elapsed = commit_started.elapsed();
        self.record_bulk_import_profile(preflight_elapsed, batch_build_elapsed, counter_read_elapsed, commit_elapsed);
        Ok(result)
    }
}

fn validate_unique_delete_mutation_identities(mutations: &[EdgeMutation]) -> Result<()> {
    let mut identities = BTreeMap::<(&str, VertexId, VertexId), &str>::new();
    for mutation in mutations {
        let identity = (mutation.edge_type.as_str(), mutation.src, mutation.dst);
        if let Some(first_key) = identities.insert(identity, mutation.idempotency_key.as_str()) {
            return Err(GraphError::IdempotencyConflict {
                operation: "delete",
                idempotency_key: format!("{first_key},{}", mutation.idempotency_key),
                reason: "two keys in one batch delete the same edge",
            });
        }
    }
    Ok(())
}

async fn renew_vertex_delete_lock_after_items(lock: &LocalWriteGuard, items: u64) -> Result<()> {
    if items == 0
        || (items >= VERTEX_DELETE_LOCK_RENEW_ITEMS && items / VERTEX_DELETE_LOCK_RENEW_ITEMS * VERTEX_DELETE_LOCK_RENEW_ITEMS == items)
    {
        lock.renew().await?;
    }
    Ok(())
}

fn coalesce_vertex_metadata_updates(
    updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
) -> Result<Vec<(VertexId, VertexMetadata)>> {
    let mut by_vertex = BTreeMap::<VertexId, VertexMetadata>::new();
    for (vertex_id, metadata) in updates {
        validate_vertex_metadata(&metadata)?;
        let entry = by_vertex.entry(vertex_id).or_default();
        entry.labels.extend(metadata.labels);
        for (property, value) in metadata.properties {
            match entry.properties.get(&property) {
                Some(existing) if existing != &value => {
                    return Err(GraphError::BatchValueConflict {
                        target: format!("vertex {vertex_id} property {property}"),
                    });
                }
                _ => {
                    entry.properties.insert(property, value);
                }
            }
        }
    }
    Ok(by_vertex.into_iter().collect())
}

/// Groups an `UNWIND … MERGE … SET` batch by vertex the way Neo4j applies it:
/// one row at a time, in order, so when two rows set the same property the
/// later row wins rather than the batch failing.
///
/// Without a guard every row lands, so a vertex's rows fold into one patch:
/// labels accumulate and a later value replaces an earlier one. With an
/// update guard whether a row lands depends on what the rows
/// before it wrote, so every row that carries the guard property is its own
/// patch, and so is each run of rows without it; the transaction applies them
/// in order with [`apply_vertex_metadata_merges`]. Only adjacent rows without
/// the guard fold, since they always land or wait together.
fn group_vertex_metadata_merges(
    updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
    merge_policy: Option<&QueryBatchMergePolicy>,
) -> Result<Vec<(VertexId, Vec<VertexMetadata>)>> {
    let guard = merge_policy.and_then(QueryBatchMergePolicy::guard);
    let carries_guard = |metadata: &VertexMetadata| guard.is_some_and(|guard| metadata.properties.contains_key(guard));
    let mut by_vertex = BTreeMap::<VertexId, Vec<VertexMetadata>>::new();
    for (vertex_id, metadata) in updates {
        validate_vertex_metadata(&metadata)?;
        let rows = by_vertex.entry(vertex_id).or_default();
        match rows.last_mut() {
            Some(folded) if !carries_guard(folded) && !carries_guard(&metadata) => {
                fold_vertex_metadata_row(folded, metadata, merge_policy);
            }
            _ => rows.push(metadata),
        }
    }
    Ok(by_vertex.into_iter().collect())
}

/// Folds `row` into the row before it: labels accumulate and a later value
/// replaces an earlier one. Create-only properties belong to the first row,
/// including its absent properties: a follower cannot fill a missing value.
fn fold_vertex_metadata_row(folded: &mut VertexMetadata, row: VertexMetadata, merge_policy: Option<&QueryBatchMergePolicy>) {
    folded.labels.extend(row.labels);
    for (property, value) in row.properties {
        let create_only = merge_policy.is_some_and(|policy| policy.create_only_properties.contains(&property));
        if !create_only {
            folded.properties.insert(property, value);
        }
    }
}

/// Applies one vertex's patches from [`group_vertex_metadata_merges`] in
/// order, each against what the patches before it left, so a later row sees
/// the vertex as existing and is guarded against the newest value an earlier
/// row wrote.
///
/// A row without the guard property cannot be checked on its own, so it lands
/// with the guarded row before it. When that row does not land, or when no
/// guarded row comes before it, it waits for the next guarded row that does,
/// folded in ahead of it; if none does, it is dropped with the rows it rode
/// on. Rows without the guard can still create a vertex, but against an
/// existing vertex with no guarded row at all they fail, as a lone row would.
fn apply_vertex_metadata_merges(
    previous: &VertexMetadata,
    mut existed: bool,
    patches: Vec<VertexMetadata>,
    merge_policy: Option<&QueryBatchMergePolicy>,
    vertex_key: &str,
) -> Result<VertexMetadata> {
    let mut merged = previous.clone();
    let Some(policy) = merge_policy else {
        for patch in patches {
            merged.labels.extend(patch.labels);
            merged.properties.extend(patch.properties);
        }
        return Ok(merged);
    };
    let Some(guard) = policy.guard() else {
        // Every row lands; one that finds the vertex leaves its create-only
        // properties as they are.
        for patch in patches {
            merged.labels.extend(patch.labels);
            for (property, value) in patch.properties {
                if !(existed && policy.create_only_properties.contains(&property)) {
                    merged.properties.insert(property, value);
                }
            }
            existed = true;
        }
        return Ok(merged);
    };
    // Rows without the guard that have not landed yet.
    let mut waiting: Option<VertexMetadata> = None;
    // Whether the last guarded row landed.
    let mut landed = false;
    let mut guarded = false;
    for patch in patches {
        if !patch.properties.contains_key(guard) {
            if landed {
                // Riding on the row that just landed, as if folded into it.
                merged.labels.extend(patch.labels);
                for (property, value) in patch.properties {
                    if !policy.create_only_properties.contains(&property) {
                        merged.properties.insert(property, value);
                    }
                }
            } else {
                match waiting.as_mut() {
                    Some(folded) => fold_vertex_metadata_row(folded, patch, merge_policy),
                    None => waiting = Some(patch),
                }
            }
            continue;
        }
        let mut row = match waiting.clone() {
            Some(mut folded) => {
                fold_vertex_metadata_row(&mut folded, patch, merge_policy);
                folded
            }
            None => patch,
        };
        guarded = true;
        if existed {
            let Some(properties) = guarded_metadata_patch(&merged.properties, row.properties, policy, vertex_key)? else {
                landed = false;
                continue;
            };
            row.properties = properties;
        }
        merged.labels.extend(row.labels);
        merged.properties.extend(row.properties);
        existed = true;
        landed = true;
        waiting = None;
    }
    if let Some(row) = waiting {
        if !existed {
            merged.labels.extend(row.labels);
            merged.properties.extend(row.properties);
        } else if !guarded {
            // No guarded row reached this vertex, so the rows are checked as
            // they stand, and fail for want of the guard.
            guarded_metadata_patch(&merged.properties, row.properties, policy, vertex_key)?;
        }
    }
    Ok(merged)
}

fn coalesce_edge_metadata_updates(
    updates: impl IntoIterator<Item = (VertexId, VertexId, EdgeMetadata)>,
) -> Result<Vec<(VertexId, VertexId, EdgeMetadata)>> {
    let mut by_edge = BTreeMap::<(VertexId, VertexId), EdgeMetadata>::new();
    for (src, dst, metadata) in updates {
        validate_edge_metadata(&metadata)?;
        match by_edge.get(&(src, dst)) {
            Some(existing) if existing != &metadata => {
                return Err(GraphError::BatchValueConflict {
                    target: format!("edge {src}->{dst}"),
                });
            }
            Some(_) => {}
            None => {
                by_edge.insert((src, dst), metadata);
            }
        }
    }
    Ok(by_edge.into_iter().map(|((src, dst), metadata)| (src, dst, metadata)).collect())
}

fn validate_relationship_creates(
    cell_id: &str,
    edge_type: &str,
    relationships: impl IntoIterator<Item = RelationshipMutation>,
) -> Result<Vec<RelationshipMutation>> {
    relationships
        .into_iter()
        .map(|relationship| {
            validate_relationship_batch_entry(cell_id, edge_type, &relationship)?;
            Ok(relationship)
        })
        .collect()
}

fn validate_relationship_batch_entry(cell_id: &str, edge_type: &str, relationship: &RelationshipMutation) -> Result<()> {
    validate_component("cell_id", &relationship.cell_id)?;
    validate_component("edge_type", &relationship.edge_type)?;
    validate_edge_metadata(&relationship.metadata)?;
    if relationship.cell_id != cell_id {
        return Err(GraphError::CorruptValue {
            key: format!("cell/{cell_id}/relationship_import"),
            reason: format!("batch contains relationship for different cell {}", relationship.cell_id),
        });
    }
    if relationship.edge_type != edge_type {
        return Err(GraphError::CorruptValue {
            key: format!("cell/{cell_id}/relationship_import/{edge_type}"),
            reason: format!("batch contains relationship for different edge type {}", relationship.edge_type),
        });
    }
    Ok(())
}

/// Only mixed-shape guarded groups need their original rows at transaction
/// time. Uniform groups keep their existing fold and replay fingerprint.
fn mixed_guarded_relationship_rows(
    relationships: &[RelationshipMutation],
    policy: Option<&QueryBatchMergePolicy>,
    limit: usize,
) -> Result<BTreeMap<RelationshipId, Vec<EdgeMetadata>>> {
    if policy.is_none() {
        return Ok(BTreeMap::new());
    }
    // Inspect borrowed shapes first. Reject as soon as an oversized batch is
    // known to require ordered patches, before copying any property values.
    let mut first = BTreeMap::<RelationshipId, &EdgeMetadata>::new();
    let mut mixed = BTreeSet::new();
    for row in relationships {
        if let Some(shape) = first.get(&row.relationship_id) {
            if !shape.properties.keys().eq(row.metadata.properties.keys()) {
                ensure_limit("import_relationships_batch", relationships.len() as u64, limit as u64)?;
                mixed.insert(row.relationship_id);
            }
        } else {
            first.insert(row.relationship_id, &row.metadata);
            ensure_limit("import_relationships_batch", first.len() as u64, limit as u64)?;
        }
    }
    let mut groups = BTreeMap::<RelationshipId, Vec<EdgeMetadata>>::new();
    for row in relationships {
        if mixed.contains(&row.relationship_id) {
            groups.entry(row.relationship_id).or_default().push(row.metadata.clone());
        }
    }
    Ok(groups)
}

#[cfg(feature = "opencypher")]
fn apply_guarded_relationship_rows(
    previous: Option<&EdgeMetadata>,
    patches: &[EdgeMetadata],
    policy: &QueryBatchMergePolicy,
    external_id: RelationshipId,
    key: &str,
) -> Result<EdgeMetadata> {
    let identity = VertexPropertyValue::Integer(external_id);
    let mut current = previous.cloned();
    for patch in patches {
        if patch.properties.get(RELATIONSHIP_IDENTITY_PROPERTY) != Some(&identity) {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: "relationship MERGE row identity metadata does not match the parsed id".into(),
            });
        }
        if let Some(existing) = &mut current {
            if *existing == *patch {
                continue;
            }
            if let Some(properties) = guarded_metadata_patch(&existing.properties, patch.properties.clone(), policy, key)? {
                existing.properties.extend(properties);
            }
        } else {
            current = Some(patch.clone());
        }
    }
    Ok(current.unwrap_or_default())
}

/// De-duplicates a relationship import by relationship id.
///
/// `fold` is `Some` on the Cypher `MERGE` path, which updates an existing
/// relationship's properties: rows for one relationship then fold the way the
/// rows of `UNWIND … MERGE … SET` apply in Neo4j, one at a time and in order,
/// instead of failing the batch. Without a guard a later value replaces an
/// earlier one. With an `update_if_newer_by` guard the row with the highest
/// guard value stands (among equal values the first, or with
/// `update_if_not_older_by` the last) and create-only values
/// come from the first row, which is what applying the rows in order produces
/// against any stored relationship when all rows set the same properties.
/// Mixed property sets retain their ordered patches separately and are applied
/// to each resolved record inside the guarded transaction.
/// Rows binding one relationship id to different endpoints always fail: the id
/// is the relationship's identity here, not an ordinary property.
///
/// Without `fold`, as for plain imports that never update properties, any two
/// rows that differ fail.
fn coalesce_relationship_imports(
    cell_id: &str,
    edge_type: &str,
    relationships: impl IntoIterator<Item = RelationshipMutation>,
    fold: Option<Option<&QueryBatchMergePolicy>>,
) -> Result<Vec<RelationshipMutation>> {
    let conflict = |relationship_id: RelationshipId, reason: &'static str| GraphError::IdempotencyConflict {
        operation: "relationship-import",
        // Not the request key — this check runs before the request
        // key is in scope, so the id that collided stands in for it.
        // The reason says so, because a key shaped like a padded
        // integer otherwise reads as a truncated request key.
        idempotency_key: format!("{relationship_id:020}"),
        reason,
    };
    let mut by_id = BTreeMap::<RelationshipId, RelationshipMutation>::new();
    for relationship in relationships {
        validate_relationship_batch_entry(cell_id, edge_type, &relationship)?;
        let Some(existing) = by_id.get_mut(&relationship.relationship_id) else {
            by_id.insert(relationship.relationship_id, relationship);
            continue;
        };
        if *existing == relationship {
            continue;
        }
        let Some(merge_policy) = fold else {
            return Err(conflict(
                relationship.relationship_id,
                "the batch carries this relationship id twice with different \
                 endpoints or properties (key shown is the relationship id)",
            ));
        };
        if (&existing.cell_id, &existing.edge_type, existing.src, existing.dst)
            != (&relationship.cell_id, &relationship.edge_type, relationship.src, relationship.dst)
        {
            return Err(conflict(
                relationship.relationship_id,
                "the batch binds this relationship id to two different edges (key shown is the relationship id)",
            ));
        }
        let Some(policy) = merge_policy else {
            existing.metadata.properties.extend(relationship.metadata.properties);
            continue;
        };
        let Some(guard) = policy.guard() else {
            for (property, value) in relationship.metadata.properties {
                if !policy.create_only_properties.contains(&property) {
                    existing.metadata.properties.insert(property, value);
                }
            }
            continue;
        };
        let (Some(kept_guard), Some(requested_guard)) =
            (existing.metadata.properties.get(guard), relationship.metadata.properties.get(guard))
        else {
            return Err(conflict(
                relationship.relationship_id,
                "the batch carries this relationship id twice without its update guard (key shown is the relationship id)",
            ));
        };
        let key = keys::relationship_id(cell_id, relationship.relationship_id);
        if guard_lands(guard_ordering(kept_guard, requested_guard, guard, &key)?, policy) {
            for (property, value) in relationship.metadata.properties {
                if !policy.create_only_properties.contains(&property) {
                    existing.metadata.properties.insert(property, value);
                }
            }
        }
    }
    Ok(by_id.into_values().collect())
}

fn merge_vertex_metadata(previous: &VertexMetadata, requested: &VertexMetadata) -> VertexMetadata {
    let mut next = previous.clone();
    next.labels.extend(requested.labels.iter().cloned());
    next.properties.extend(
        requested
            .properties
            .iter()
            .map(|(property, value)| (property.clone(), value.clone())),
    );
    next
}

fn guarded_metadata_patch(
    previous: &BTreeMap<String, VertexPropertyValue>,
    mut requested: BTreeMap<String, VertexPropertyValue>,
    policy: &QueryBatchMergePolicy,
    key: &str,
) -> Result<Option<BTreeMap<String, VertexPropertyValue>>> {
    policy.validate()?;
    for property in &policy.create_only_properties {
        requested.remove(property);
    }
    let Some(guard) = policy.guard() else {
        return Ok(Some(requested));
    };
    let requested_guard = requested.get(guard).ok_or_else(|| GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Mutation,
        dialect: "QueryBatch",
        feature: format!("guarded merge is missing incoming property {guard}"),
    })?;
    let Some(previous_guard) = previous.get(guard) else {
        return Ok(Some(requested));
    };
    if !guard_lands(guard_ordering(previous_guard, requested_guard, guard, key)?, policy) {
        return Ok(None);
    }
    Ok(Some(requested))
}

/// Whether a row lands given how the stored guard value orders against the
/// row's: a newer value always does, an equal one under `update_if_not_older_by`.
fn guard_lands(stored_against_incoming: std::cmp::Ordering, policy: &QueryBatchMergePolicy) -> bool {
    match stored_against_incoming {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => policy.lands_on_equal(),
        std::cmp::Ordering::Greater => false,
    }
}

/// Orders two values of a merge policy's update guard property.
fn guard_ordering(previous: &VertexPropertyValue, requested: &VertexPropertyValue, guard: &str, key: &str) -> Result<std::cmp::Ordering> {
    match (previous, requested) {
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Integer(right)) => Ok(left.cmp(right)),
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::SignedInteger(right)) => Ok(left.cmp(right)),
        (VertexPropertyValue::Float(left), VertexPropertyValue::Float(right)) => Ok(left.cmp(right)),
        (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => Ok(left.cmp(right)),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("guarded merge property {guard} must have matching ordered scalar types"),
        }),
    }
}

/// One property update for [`GraphShard::set_relationship_metadata_batch`]:
/// an identified relationship when `relationship_id` is set, otherwise the
/// structural edge's own metadata.
#[cfg(feature = "opencypher")]
#[derive(Clone, Debug)]
pub(crate) struct RelationshipMetadataUpdate {
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) dst: VertexId,
    pub(crate) relationship_id: Option<RelationshipId>,
    pub(crate) metadata: EdgeMetadata,
    /// The metadata `metadata` was computed from. When set, the update
    /// applies only if the relationship still carries it, and otherwise the
    /// batch refuses with `ConditionalWriteConflict`, writing nothing.
    pub(crate) read: Option<EdgeMetadata>,
}

/// One read allowance shared by every phase of a warm.
///
/// The phases run concurrently, so the allowance is a counter they share rather
/// than a limit each applies to itself: two phases each holding their own copy
/// of the budget spend twice what was promised.
/// What an identity warm touched, for the caller to report.
#[derive(Default)]
struct WarmedIdentities {
    pointers: u64,
    scans: u64,
}

struct WarmBudget {
    budget: usize,
    charged: std::sync::atomic::AtomicUsize,
    made: std::sync::atomic::AtomicUsize,
    truncated: std::sync::atomic::AtomicBool,
}

impl WarmBudget {
    fn new(budget: usize) -> Self {
        Self {
            budget,
            charged: std::sync::atomic::AtomicUsize::new(0),
            made: std::sync::atomic::AtomicUsize::new(0),
            truncated: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Charge one read. `false` means the allowance is gone and the caller must
    /// not make the read: the charge is taken before the read, so no more than
    /// `budget` of them ever happen.
    fn charge(&self) -> bool {
        if self.charged.fetch_add(1, Ordering::Relaxed).saturating_add(1) > self.budget {
            self.truncated.store(true, Ordering::Relaxed);
            return false;
        }
        self.made.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn spent(&self) -> bool {
        self.truncated.load(Ordering::Relaxed)
    }

    fn reads(&self) -> u64 {
        self.made.load(Ordering::Relaxed) as u64
    }
}

/// Split `RELATIONSHIP_DELETE_WARM_MAX_READS` between a relationship delete's
/// point keys and its survivor prefixes, given what each would read in full.
///
/// When both fit, each gets its need. Otherwise the prefixes, which are the
/// measured cost under the writer guard, are guaranteed half the budget (or
/// all they need, if less) and take more when the point keys leave it unused;
/// the point keys get the rest. Returns `(point, prefix)`.
#[cfg(feature = "opencypher")]
fn split_relationship_delete_warm_budget(point_need: usize, prefix_need: usize) -> (usize, usize) {
    let total = RELATIONSHIP_DELETE_WARM_MAX_READS;
    let prefix = prefix_need.min(total.saturating_sub(point_need).max(total / 2));
    (point_need.min(total - prefix), prefix)
}

#[cfg(feature = "opencypher")]
fn relationship_import_scan_options() -> ScanOptions {
    ScanOptions::default().with_cache_blocks(false)
}

#[cfg(feature = "opencypher")]
async fn relationship_records_for_edge_property_txn(
    txn: &DbTransaction,
    lookup: RelationshipPropertyTxnLookup<'_>,
) -> Result<Vec<RelationshipRecord>> {
    let RelationshipPropertyTxnLookup {
        cell_id,
        edge_type,
        src,
        dst,
        property,
        value,
    } = lookup;
    if !value.is_scalar_indexable() {
        let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
        let mut iter = txn
            .scan_prefix_with_options(prefix.as_bytes(), .., &relationship_import_scan_options())
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            if record.metadata.properties.get(property) == Some(value) {
                relationships.push(record);
            }
        }
        return Ok(relationships);
    }
    let encoded = encode_vertex_property_value_key(value);
    let prefix = keys::relationship_property_index_edge_prefix(cell_id, edge_type, property, &encoded, src, dst);
    let scan_options = relationship_import_scan_options();
    let mut iter = txn.scan_prefix_with_options(prefix.as_bytes(), .., &scan_options).await?;
    let mut relationship_keys = BTreeMap::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let (parsed_cell_id, parsed_edge_type, parsed_property, parsed_encoded, parsed_src, parsed_dst, relationship_id) =
            parse_relationship_property_index_key(&key)?;
        if parsed_cell_id != cell_id
            || parsed_edge_type != edge_type
            || parsed_property != property
            || parsed_encoded != encoded
            || parsed_src != src
            || parsed_dst != dst
        {
            return Err(GraphError::CorruptValue {
                key,
                reason: "relationship property index escaped its requested prefix".to_string(),
            });
        }
        relationship_keys.insert(relationship_id, keys::relationship(cell_id, edge_type, src, dst, relationship_id));
    }
    let record_values = read_txn_remote_many(txn, relationship_keys.values().cloned()).await?;
    let mut relationships = Vec::with_capacity(relationship_keys.len());
    for (relationship_id, record_key) in relationship_keys {
        let Some(record_value) = record_values.get(&record_key).and_then(Option::as_ref) else {
            return Err(GraphError::CorruptValue {
                key: keys::relationship_property_index(cell_id, edge_type, property, &encoded, src, dst, relationship_id),
                reason: format!("relationship property index points at missing {record_key}"),
            });
        };
        let record = decode_relationship_record(&record_key, record_value)?;
        if record.metadata.properties.get(property) == Some(value) {
            relationships.push(record);
        }
    }
    relationships.sort_by_key(|record| record.relationship_id);
    relationships.dedup_by_key(|record| record.relationship_id);
    Ok(relationships)
}

#[cfg(feature = "opencypher")]
async fn reserve_edge_delete_noops_txn(txn: &DbTransaction, cell_id: &str, mutations: &[EdgeMutation]) -> Result<()> {
    let idem_keys = mutations
        .iter()
        .map(|mutation| keys::idempotency(cell_id, "delete", &mutation.idempotency_key))
        .collect::<Vec<_>>();
    let values = read_txn_remote_many(txn, idem_keys.iter().cloned()).await?;
    for (mutation, key) in mutations.iter().zip(idem_keys) {
        if let Some(value) = values.get(&key).and_then(Option::as_ref) {
            decode_delete_idempotency(&key, mutation, value)?;
        } else {
            txn.put(
                key.as_bytes(),
                encode_delete_idempotency(
                    mutation,
                    &DeleteResult {
                        epoch: txn.seqnum(),
                        deleted: false,
                    },
                ),
            )?;
        }
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
async fn has_surviving_relationship_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    deleting: &BTreeSet<RelationshipId>,
) -> Result<bool> {
    let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let record = decode_relationship_record(&key, &kv.value)?;
        if !deleting.contains(&record.relationship_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `write.index_update`: the dirty marker, adjacency generation, and xlog
/// entries — everything the indexer later consumes, written in the mutating
/// transaction itself.
///
/// This is the changelog's chokepoint: every topology mutation site must call
/// it, and the `changes` parameter forces each site to state, at compile
/// time, exactly which edges changed and their existence *after* the commit.
/// Over-logging is safe — values are final state, not operations, so a
/// duplicate entry for the same `(src, dst)` at the same epoch is one key —
/// but a *missed* site silently corrupts the incremental index, which is why
/// the old delta-free signature no longer exists. Property-only updates pass
/// an empty slice: they mark dirty but log nothing, because properties are
/// not part of the CSC matrix.
///
/// The epoch stamped into the xlog key is the transaction's commit sequence,
/// exactly — `commit_txn_strict_with_sequence` pins it and SlateDB rejects
/// any other (`batch_write.rs` in the pinned fork returns
/// `InvalidSequenceNumber` for anything but the requested sequence).
///
/// It is also the one place the write path and the indexing path name the
/// same `(cell_id, edge_type, epoch)`. That triple is the attribute join §5
/// relies on to answer "did the write, the compile and the read agree on a
/// generation", which is the BFG-006 / BFG-013 / BFG-014 question.
impl GraphShard {
    pub(crate) async fn mark_topology_change_txn(
        &self,
        txn: &DbTransaction,
        cell_id: &str,
        edge_type: &str,
        epoch: StorageSequence,
        changes: &[(VertexId, VertexId, bool)],
    ) -> Result<()> {
        mark_topology_change_txn_inner(&self.xlog_floor_ensured, txn, cell_id, edge_type, epoch, changes).await
    }
}

async fn prepare_vertex_delete_edge_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge: &IncidentEdge,
    relationship_reads: &std::sync::atomic::AtomicU64,
    relationship_limit: usize,
) -> Result<(bool, EdgeMetadata, Vec<RelationshipRecord>)> {
    let out_key = keys::out_edge(cell_id, &edge.edge_type, edge.src, edge.dst);
    let metadata_key = keys::edge_metadata(cell_id, &edge.edge_type, edge.src, edge.dst);
    // One child span per read. `delete_edge_prefetch` above them lasts as long
    // as the slowest of the three, so on its own it cannot say which read went
    // to object storage. SlateDB's block-cache hit/miss counters are global and
    // scans fetch blocks on spawned tasks, so a per-read hit flag is not
    // available; a cached or memtable read finishes in about a millisecond and
    // an object-store read in tens, so the span duration is the signal.
    let edge_record_span = tracing::info_span!(
        "write.delete_edge_prefetch.edge_record",
        hydradb.edge_type = %edge.edge_type,
        hydradb.read.found = tracing::field::Empty,
    );
    let metadata_span = tracing::info_span!(
        "write.delete_edge_prefetch.metadata",
        hydradb.edge_type = %edge.edge_type,
        hydradb.read.found = tracing::field::Empty,
    );
    let relationships_span = tracing::info_span!(
        "write.delete_edge_prefetch.relationships",
        hydradb.edge_type = %edge.edge_type,
        hydradb.read.cache_blocks = true,
        hydradb.read.records = tracing::field::Empty,
    );
    let read_relationships = {
        let span = relationships_span.clone();
        async move {
            let prefix = keys::relationship_edge_prefix(cell_id, &edge.edge_type, edge.src, edge.dst);
            // Admit fetched blocks to the bounded block cache, as the incident
            // discovery and validation scans already do. The default scan
            // options discard them, so every repeat of this prefix went back to
            // object storage while holding the writer guard. Only the cache
            // admission changes: this read still runs in the guarded
            // transaction and sees every write committed before it.
            let scan_options = ScanOptions::default().with_cache_blocks(true);
            let mut iter = txn.scan_prefix_with_options(prefix.as_bytes(), .., &scan_options).await?;
            let mut records = Vec::new();
            while let Some(kv) = iter.next().await? {
                // One shared allowance covers all concurrently prepared edges, not
                // a fresh record budget for every edge or prefetch window.
                let count = relationship_reads.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                ensure_limit("delete_vertex_relationship_records", count, relationship_limit as u64)?;
                let key = String::from_utf8_lossy(&kv.key);
                records.push(decode_relationship_record(&key, &kv.value)?);
            }
            span.record("hydradb.read.records", records.len() as u64);
            Ok::<_, GraphError>(records)
        }
        .instrument(relationships_span)
    };
    let (canonical, metadata, relationships) = tokio::try_join!(
        read_txn_remote_traced(txn, &out_key, edge_record_span),
        read_txn_remote_traced(txn, &metadata_key, metadata_span),
        read_relationships,
    )?;
    let canonical = match canonical {
        Some(value) => {
            decode_edge_record(&out_key, &value)?;
            true
        }
        None => false,
    };
    let metadata = match metadata {
        Some(value) => decode_edge_metadata(&metadata_key, &value)?,
        None => EdgeMetadata::default(),
    };
    Ok((canonical, metadata, relationships))
}

/// `read_txn_remote` inside `span`, recording whether the key existed.
async fn read_txn_remote_traced(txn: &DbTransaction, key: &str, span: tracing::Span) -> Result<Option<Bytes>> {
    let value = read_txn_remote(txn, key).instrument(span.clone()).await?;
    span.record("hydradb.read.found", value.is_some());
    Ok(value)
}
