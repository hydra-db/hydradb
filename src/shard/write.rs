use super::*;
use crate::QueryFailureReason;
#[cfg(all(test, feature = "opencypher"))]
#[path = "seconds_queue_replay.rs"]
mod seconds_queue_replay;
use futures::{stream, FutureExt as _, StreamExt as _, TryStreamExt as _};
use tracing::Instrument as _;

const RELATIONSHIP_IMPORT_READ_CONCURRENCY: usize = 32;
const VERTEX_DELETE_READ_CONCURRENCY: usize = 16;
const METADATA_NOOP_PREFLIGHT_MAX_ITEMS: usize = 32;

static DELETE_PREPARATION_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);
const DELETE_PREPARATION_MAX_VERTICES: usize = 4096;
const DELETE_PREPARATION_BYTES: u64 = 8 * 1024 * 1024;
const DELETE_APPLY_WARM_MAX_KEYS: usize = 1024;

async fn run_cancellable_delete_preparation<T>(
    preparation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
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

#[cfg(test)]
mod cleanup_cancellation_tests {
    use super::*;

    #[tokio::test]
    async fn cleanup_cancellation_releases_pipeline_while_preparation_slots_are_busy() {
        let shard = GraphShard::open_standalone_writer(
            "graph/cancel-preparation-admission",
            Arc::new(slatedb::object_store::memory::InMemory::new()),
        )
        .await
        .unwrap();
        shard
            .set_vertex_metadata("cell-a", 1, VertexMetadata::default().with_label("Source"))
            .await
            .unwrap();
        let epoch = shard.current_epoch("cell-a").await.unwrap();
        // Hold every real preparation slot, without sleeping or spawning a
        // second cleanup that could interfere with other tests' storage gates.
        let slots = DELETE_PREPARATION_SLOTS.acquire_many(4).await.unwrap();
        let token = crate::QueryCancellationToken::new();
        let mut deleting = Box::pin(token.scope(shard.detach_delete_vertex("cell-a", 1, "delete")));
        assert!(futures::poll!(deleting.as_mut()).is_pending());
        assert_eq!(shard.write_pipeline_gate.available_permits(), 15);
        token.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut deleting).await;
        drop(deleting);
        drop(slots);
        assert!(matches!(
            result.unwrap(),
            Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                ..
            })
        ));
        assert_eq!(shard.write_pipeline_gate.available_permits(), 16);
        assert_eq!(shard.current_epoch("cell-a").await.unwrap(), epoch);
        // Cancellation is scoped to the request, not the shard or its next user.
        assert!(
            shard
                .detach_delete_vertex("cell-a", 1, "delete")
                .await
                .unwrap()
                .vertex_deleted
        );
        shard.close().await.unwrap();
    }
}

#[cfg(all(test, feature = "opencypher", feature = "query-transport"))]
#[path = "write_latency_replay.rs"]
mod latency_replay;

/// Stamp a failure onto the span that raised it.
///
/// §6 of `docs/plans/2026-07-26-otel-telemetry-crate.md`: the innermost span is
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
    mutations
        .iter()
        .all(|mutation| mutation.edge_type == first)
        .then_some(first)
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
    async {
        commit_txn_strict(txn, await_durable)
            .await
            .inspect_err(record_error_class)
    }
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

fn vertex_matches_path_node_constraints(
    vertex_id: VertexId,
    metadata: &VertexMetadata,
    constraints: &VertexMetadata,
) -> bool {
    constraints
        .labels
        .iter()
        .all(|label| metadata.labels.contains(label))
        && constraints.properties.iter().all(|(property, expected)| {
            if property == "id" {
                return delete_constraint_property_values_equal(
                    &VertexPropertyValue::Integer(vertex_id),
                    expected,
                );
            }
            metadata
                .properties
                .get(property)
                .is_some_and(|existing| delete_constraint_property_values_equal(existing, expected))
        })
}

fn delete_constraint_property_values_equal(
    left: &VertexPropertyValue,
    right: &VertexPropertyValue,
) -> bool {
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
fn note_merge_pointer(
    updates: &mut BTreeMap<String, Option<RelationshipId>>,
    pointer_key: &str,
    relationship_id: RelationshipId,
) {
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
    pub async fn set_vertex_metadata(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: VertexMetadata,
    ) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        validate_vertex_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_vertex_metadata")?;
        self.run_write_pipeline(async {
        let _permit = self
            .acquire_graph_write_permit("set_vertex_metadata")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_vertex_metadata"))
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
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(()) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(());
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

    pub async fn set_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (VertexId, VertexMetadata)>,
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
                .set_vertex_metadata_batch_txn(cell_id, updates.clone())
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
        let updates = coalesce_vertex_metadata_updates(updates)?;
        if updates.is_empty() {
            return Ok(0);
        }
        ensure_limit(
            "merge_vertex_metadata_batch",
            updates.len() as u64,
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
            self.db.refresh_writer_fence().await.map(|()| 0)
        } else {
            result
        };
        if result.as_ref().is_ok_and(|changed| *changed > 0) {
            self.operation_metrics
                .write_commits
                .fetch_add(1, Ordering::Relaxed);
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
    }

    async fn set_vertex_metadata_txn(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: VertexMetadata,
    ) -> Result<()> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "set_vertex_metadata")
            .await?;
        let result = self
            .set_vertex_metadata_txn_locked(cell_id, vertex_id, metadata)
            .await;
        finish_local_write(lock, result).await
    }

    async fn set_vertex_metadata_txn_locked(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: VertexMetadata,
    ) -> Result<()> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_vertex_metadata")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let vertex_key = keys::vertex(cell_id, vertex_id);
        let previous = match read_txn_remote(&txn, &vertex_key).await? {
            Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
            None => VertexMetadata::default(),
        };
        if previous == metadata {
            self.db.refresh_writer_fence().await?;
            return Ok(());
        }
        self.validate_changing_write().await?;
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        apply_vertex_metadata_update_txn(&txn, cell_id, vertex_id, &previous, &metadata, epoch)?;
        commit_txn_strict(txn, self.await_durable_writes).await
    }

    async fn set_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "set_vertex_metadata_batch")
            .await?;
        let result = self
            .set_vertex_metadata_batch_txn_locked(cell_id, updates)
            .await;
        finish_local_write(lock, result).await
    }

    async fn vertex_metadata_merges_are_noops(
        &self,
        cell_id: &str,
        updates: &[(VertexId, VertexMetadata)],
    ) -> Result<bool> {
        let Ok(_hydration_permit) = Arc::clone(&self.hydration_gate).try_acquire_owned() else {
            return Ok(false);
        };
        let snapshot = self.db.writer()?.durable_snapshot().await?;
        let options = crate::codec::remote_read_options();
        for marker in [
            keys::cell_drop_marker(cell_id),
            keys::cell_drop_pending_marker(cell_id),
        ] {
            if snapshot
                .get_with_options(marker.as_bytes(), &options)
                .await?
                .is_some()
            {
                return Err(GraphError::CellDropped {
                    operation: "merge_vertex_metadata_batch",
                    cell_id: cell_id.to_string(),
                });
            }
        }
        self.ensure_write_authority(cell_id, "merge_vertex_metadata_batch")?;
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
        for (vertex_id, patch) in updates {
            let key = keys::vertex(cell_id, *vertex_id);
            let previous = match existing.get(&key).and_then(Option::as_ref) {
                Some(value) => decode_vertex_metadata(&key, value)?,
                None => VertexMetadata::default(),
            };
            if !patch.labels.is_subset(&previous.labels)
                || patch
                    .properties
                    .iter()
                    .any(|(name, value)| previous.properties.get(name) != Some(value))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn merge_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
        merge_policy: Option<&QueryBatchMergePolicy>,
    ) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, "merge_vertex_metadata_batch")
            .await?;
        let result = self
            .merge_vertex_metadata_batch_txn_locked(cell_id, updates, merge_policy)
            .await;
        finish_local_write(lock, result).await
    }

    async fn set_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_vertex_metadata_batch")
            .await?;
        let mut changed = Vec::new();
        let read_options = relationship_import_read_options(updates.len());
        // Keep the extra raw read buffers bounded independently of batch size.
        // All reads still belong to this serializable transaction and its guard.
        for chunk in updates.chunks(RELATIONSHIP_IMPORT_READ_CONCURRENCY) {
            let existing = read_txn_remote_many_with_options(
                &txn,
                chunk
                    .iter()
                    .map(|(vertex, _)| keys::vertex(cell_id, *vertex))
                    .collect(),
                &read_options,
            )
            .await?;
            for (vertex_id, metadata) in chunk {
                let vertex_key = keys::vertex(cell_id, *vertex_id);
                let previous = match existing.get(&vertex_key).and_then(Option::as_ref) {
                    Some(value) => decode_vertex_metadata(&vertex_key, value)?,
                    None => VertexMetadata::default(),
                };
                if previous != *metadata {
                    changed.push((*vertex_id, previous, metadata.clone()));
                }
            }
        }
        if changed.is_empty() {
            self.db.refresh_writer_fence().await?;
            return Ok(0);
        }
        self.validate_changing_write().await?;
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous, metadata, epoch)?;
        }
        let changed_count = changed.len();
        commit_txn_traced(txn, self.await_durable_writes, cell_id, "", epoch).await?;
        Ok(changed_count)
    }

    async fn merge_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
        merge_policy: Option<&QueryBatchMergePolicy>,
    ) -> Result<usize> {
        let txn_started = std::time::Instant::now();
        let batch_items = updates.len() as u64;
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "merge_vertex_metadata_batch")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let read_started = std::time::Instant::now();
        let vertex_keys: Vec<String> = updates
            .iter()
            .map(|(vertex_id, _)| keys::vertex(cell_id, *vertex_id))
            .collect();
        let existing = read_txn_remote_many(&txn, vertex_keys.iter().cloned()).await?;
        let mut changed = Vec::new();
        for (vertex_id, mut patch) in updates {
            let vertex_key = keys::vertex(cell_id, vertex_id);
            let (previous, existed) = match existing.get(&vertex_key).and_then(Option::as_ref) {
                Some(value) => (decode_vertex_metadata(&vertex_key, value)?, true),
                None => (VertexMetadata::default(), false),
            };
            if existed {
                if let Some(policy) = merge_policy {
                    let Some(properties) = guarded_metadata_patch(
                        &previous.properties,
                        patch.properties,
                        policy,
                        &vertex_key,
                    )?
                    else {
                        continue;
                    };
                    patch.properties = properties;
                }
            }
            let mut merged = previous.clone();
            merged.labels.extend(patch.labels);
            merged.properties.extend(patch.properties);
            if previous != merged {
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
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous, metadata, epoch)?;
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

    async fn import_vertex_metadata_batch_txn(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let lock = self
            .acquire_local_write_guard(cell_id, "import_vertex_metadata_batch")
            .await?;
        let result = self
            .import_vertex_metadata_batch_txn_locked(cell_id, updates)
            .await;
        finish_local_write(lock, result).await
    }

    async fn import_vertex_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        updates: Vec<(VertexId, VertexMetadata)>,
    ) -> Result<usize> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "import_vertex_metadata_batch")
            .await?;
        let mut changed = Vec::new();
        for (vertex_id, metadata) in updates {
            let vertex_key = keys::vertex(cell_id, vertex_id);
            let previous = match read_txn_remote(&txn, &vertex_key).await? {
                Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
                None => VertexMetadata::default(),
            };
            if previous == metadata {
                continue;
            }
            if previous != VertexMetadata::default() {
                return Err(GraphError::CorruptValue {
                    key: vertex_key,
                    reason: format!(
                        "vertex {vertex_id} already has different metadata during import"
                    ),
                });
            }
            changed.push((vertex_id, previous, metadata));
        }
        if changed.is_empty() {
            return Ok(0);
        }
        let epoch = next_epoch_txn(&txn, cell_id).await?;
        for (vertex_id, previous, metadata) in &changed {
            apply_vertex_metadata_update_txn(&txn, cell_id, *vertex_id, previous, metadata, epoch)?;
        }
        let changed_count = changed.len();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(changed_count)
    }

    pub async fn delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
    ) -> Result<VertexDeleteResult> {
        self.delete_vertex_with_options(cell_id, vertex_id, idempotency_key, false)
            .await
    }

    pub async fn detach_delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        idempotency_key: &str,
    ) -> Result<VertexDeleteResult> {
        self.delete_vertex_with_options(cell_id, vertex_id, idempotency_key, true)
            .await
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
        let result = self
            .delete_vertex_mutations_batch_with_mode(cell_id, deletions, mode)
            .await;
        if detach {
            self.operation_metrics
                .detach_delete_vertices_batch_latency
                .record_micros(started.elapsed().as_micros() as u64);
        }
        result
    }

    #[cfg(any(feature = "opencypher", test))]
    pub(crate) async fn delete_isolated_vertex_mutations_batch(
        &self,
        cell_id: &str,
        deletions: Vec<(VertexId, String, VertexMetadata)>,
    ) -> Result<Vec<VertexDeleteResult>> {
        self.delete_vertex_mutation_requests_batch_with_mode(
            cell_id,
            deletions
                .into_iter()
                .map(|(vertex_id, idempotency_key, constraints)| {
                    (vertex_id, idempotency_key, false, constraints)
                })
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
                .map(|(vertex_id, idempotency_key)| {
                    (
                        vertex_id,
                        idempotency_key,
                        detach,
                        VertexMetadata::default(),
                    )
                })
                .collect(),
            mode,
        )
        .await
    }

    #[cfg(any(feature = "opencypher", test))]
    pub(crate) async fn delete_vertices_and_isolated_candidates_batch(
        &self,
        cell_id: &str,
        deletions: Vec<(VertexId, String, bool, VertexMetadata)>,
    ) -> Result<Vec<VertexDeleteResult>> {
        let started = std::time::Instant::now();
        let result = self
            .delete_vertex_mutation_requests_batch_with_mode(
                cell_id,
                deletions,
                VertexDeleteBatchMode::DetachAndIsolated,
            )
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
        ensure_limit(
            operation,
            deletions.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        if deletions.is_empty() {
            return Ok(Vec::new());
        }

        let span = write_txn_span(cell_id, None);
        self.run_write_pipeline(async {
            let mut retries = WriteRetryCount::new();
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                // Bound both active S3 discovery and prepared results waiting for
                // admission. No graph writer guard is held during this phase.
                let preparation = async {
                    let mut preparation_slot = if self.writes_reverse_index()
                        && deletions.len() <= DELETE_PREPARATION_MAX_VERTICES
                    {
                        Some(DELETE_PREPARATION_SLOTS.acquire()
                            .instrument(tracing::info_span!("write.delete_preparation_admission"))
                            .await.expect("static semaphore is never closed"))
                    } else {
                        None
                    };
                    let prepared = self.prepare_vertex_delete(cell_id, &deletions, mode)
                        .instrument(tracing::info_span!(
                            "write.delete_prepare",
                            hydradb.delete.vertices = deletions.len(),
                            hydradb.delete.scanned_entries = tracing::field::Empty,
                            hydradb.delete.retained_edges = tracing::field::Empty,
                        ))
                        .await.inspect_err(record_error_class)?;
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
                    .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = operation))
                    .await?;
                let _writer = self
                    .writer_lane(cell_id)
                    .lock()
                    .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
                    .await;
                let lock = self.acquire_local_write_guard_no_fence(cell_id, operation).await?;
                let result = self
                    .delete_vertex_mutations_batch_txn_locked(
                        cell_id, &deletions, mode, operation, &lock, prepared,
                    )
                    .instrument(tracing::info_span!("storage.txn"))
                    .await;
                match finish_local_write(lock, result).await {
                    Err(err)
                        if (is_retryable_write_conflict(&err)
                            || matches!(&err, GraphError::ConditionalWriteConflict { operation: "validate_vertex_delete_preparation", .. }))
                            && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                    {
                        self.operation_metrics
                            .write_retries
                            .fetch_add(1, Ordering::Relaxed);
                        retries.note_retry();
                        drop(_writer);
                        drop(_permit);
                        drop(preparation_slot);
                        tokio::task::yield_now().await;
                    }
                    Ok(results) => {
                        self.operation_metrics
                            .write_commits
                            .fetch_add(1, Ordering::Relaxed);
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
        .instrument(span))
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
                            .get_with_options(
                                key.as_bytes(),
                                &slatedb::config::ReadOptions::default(),
                            )
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
        let vertices = pending
            .iter()
            .flatten()
            .map(|(vertex, _)| *vertex)
            .collect::<BTreeSet<_>>();
        if vertices.is_empty() {
            return Ok(None);
        }
        let detach = pending
            .iter()
            .flatten()
            .filter(|(_, detach)| *detach)
            .map(|(vertex, _)| *vertex)
            .collect::<BTreeSet<_>>();
        let isolation_detach = matches!(
            mode,
            VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated
        )
        .then_some(&detach);
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
                    self.warm_vertex_delete_apply_reads(&snapshot, &vertices, &discovery.edges)
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
        edges: &BTreeSet<IncidentEdge>,
    ) -> Result<()> {
        // Discovery can find an incoming edge without reading its canonical
        // outgoing block. Warm the small cleanup's apply working set before
        // acquiring the writer gate; otherwise those cold point reads serialize
        // every writer. Values are discarded: the transaction still reads and
        // marks every key at its own snapshot, including concurrent updates.
        // Large cleanups keep the existing path and cannot sweep the cache.
        if !self.db.has_block_cache()
            || vertices
                .len()
                .saturating_add(edges.len().saturating_mul(5))
                .saturating_add(2)
                > DELETE_APPLY_WARM_MAX_KEYS
        {
            return Ok(());
        }
        let cell_id = &snapshot.cell_id;
        let mut keys = vertices
            .iter()
            .map(|vertex| keys::vertex(cell_id, *vertex))
            .collect::<BTreeSet<_>>();
        keys.extend([
            keys::cell_drop_marker(cell_id),
            keys::cell_drop_pending_marker(cell_id),
        ]);
        for edge in edges {
            keys.extend([
                keys::out_edge(cell_id, &edge.edge_type, edge.src, edge.dst),
                keys::edge_metadata(cell_id, &edge.edge_type, edge.src, edge.dst),
                keys::degree_out(cell_id, &edge.edge_type, edge.src),
                keys::degree_in(cell_id, &edge.edge_type, edge.dst),
                keys::xlog_low_water(cell_id, &edge.edge_type),
            ]);
        }
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
            .await
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
        let mut iter = txn
            .scan_prefix_with_options(prefix.as_bytes(), .., &scan_options)
            .await?;
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
                            format!("{from:020}").into_bytes()
                                ..format!("{:020}", txn.seqnum().saturating_add(1)).into_bytes(),
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
                return Ok(outcome(
                    false,
                    reason,
                    changes_scanned.load(Ordering::Relaxed),
                ));
            }
        }
        Ok(outcome(
            true,
            "unchanged_targets",
            changes_scanned.load(Ordering::Relaxed),
        ))
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, operation)
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;
        let current_epoch = txn.seqnum();
        let commit_epoch =
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "storage_sequence".to_string(),
                    reason: "epoch overflow during vertex delete batch".to_string(),
                })?;

        let mut results = vec![None; deletions.len()];
        let read_started = std::time::Instant::now();
        let idem_keys: Vec<String> = deletions
            .iter()
            .map(|(_, idempotency_key, _, _)| {
                keys::idempotency(cell_id, mode.idempotency_operation(), idempotency_key)
            })
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
            let previous = match vertex_values.get(vertex_key).and_then(Option::as_ref) {
                Some(value) => decode_vertex_metadata(vertex_key, value)?,
                None => VertexMetadata::default(),
            };
            pending.push((
                index,
                *vertex_id,
                idempotency_key.clone(),
                previous,
                *detach,
                constraints.clone(),
            ));
        }
        let read_us = read_started.elapsed().as_micros() as u64;
        if pending.is_empty() {
            // A replay has no durable WAL barrier to establish ownership.
            self.db.refresh_writer_fence().await?;
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

        let vertices = pending
            .iter()
            .map(|(_, vertex_id, _, _, _, _)| *vertex_id)
            .collect::<BTreeSet<_>>();
        let detach_vertices = pending
            .iter()
            .filter(|(_, _, _, _, detach, _)| *detach)
            .map(|(_, vertex_id, _, _, _, _)| *vertex_id)
            .collect::<BTreeSet<_>>();
        let write_reverse_index = self.writes_reverse_index();
        let incident_edges = if let Some(prepared) = prepared {
            if !async {
                if vertices != prepared.vertices {
                    tracing::Span::current().record(
                        "hydradb.delete.validation_result",
                        "pending_vertices_changed",
                    );
                    tracing::Span::current()
                        .record("hydradb.delete.validation_xlog_entries", 0_u64);
                    return Ok(false);
                }
                self.vertex_delete_preparation_is_current(&txn, cell_id, &prepared)
                    .await
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
            let isolation_detach_set = matches!(
                mode,
                VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated
            )
            .then_some(&detach_vertices);
            self.indexed_vertex_delete_incidents_at(
                cell_id,
                &vertices,
                isolation_detach_set,
                current_epoch,
                lock,
            )
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
            .filter(|edge| {
                !detach_vertices.contains(&edge.src) && !detach_vertices.contains(&edge.dst)
            })
            .flat_map(|edge| [edge.src, edge.dst])
            .filter(|vertex_id| vertices.contains(vertex_id))
            .collect::<BTreeSet<_>>();
        if matches!(
            mode,
            VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated
        ) {
            let connected_constraint_mismatches = pending
                .iter()
                .filter(|(_, vertex_id, _, previous, detach, constraints)| {
                    !*detach
                        && connected_vertices.contains(vertex_id)
                        && !vertex_matches_path_node_constraints(*vertex_id, previous, constraints)
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
        let mut segment_edges_by_type_src =
            BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
        let relationship_reads = std::sync::atomic::AtomicU64::new(0);
        let mut prepared_edges = stream::iter(
            incident_edges
                .into_iter()
                .filter(|edge| {
                    detach_vertices.contains(&edge.src) || detach_vertices.contains(&edge.dst)
                })
                .enumerate(),
        )
        .map(|(index, edge)| {
            let txn = &txn;
            let relationship_reads = &relationship_reads;
            async move {
                let state = prepare_vertex_delete_edge_txn(
                    txn,
                    cell_id,
                    &edge,
                    relationship_reads,
                    self.limits.max_query_intermediate_rows,
                )
                .instrument(
                    tracing::info_span!("write.delete_edge_prefetch", hydradb.cell_id = %cell_id),
                )
                .await?;
                Ok::<_, GraphError>((index, edge, state))
            }
        })
        .buffered(VERTEX_DELETE_READ_CONCURRENCY);
        while let Some((edge_index, edge, (canonical, previous_edge_metadata, relationships))) =
            prepared_edges.try_next().await?
        {
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
                idempotency_key: format!(
                    "{}.detach-edge.{}.{}.{}",
                    pending[owner].2, edge.src, edge.dst, edge_index
                ),
            };

            let out_edge_key = keys::out_edge(cell_id, &mutation.edge_type, edge.src, edge.dst);
            let segment_epoch = if canonical || write_reverse_index {
                None
            } else {
                let cache_key = (mutation.edge_type.clone(), edge.src);
                match segment_edges_by_type_src.get(&cache_key) {
                    Some(edges) => edges.get(&edge.dst).copied(),
                    None => {
                        let edges = out_segment_edges_for_src_txn(
                            &txn,
                            cell_id,
                            &mutation.edge_type,
                            edge.src,
                            current_epoch,
                        )
                        .await?;
                        let epoch = edges.get(&edge.dst).copied();
                        segment_edges_by_type_src.insert(cache_key, edges);
                        epoch
                    }
                }
            };
            let relationships = delete_prepared_relationships_for_structural_edge_txn(
                &txn,
                &mutation,
                &relationships,
            )?;
            if canonical || segment_epoch.is_some() {
                self.mark_topology_change_txn(
                    &txn,
                    cell_id,
                    &mutation.edge_type,
                    commit_epoch,
                    &[(edge.src, edge.dst, false)],
                )
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
                    txn.delete(
                        keys::edge(cell_id, &mutation.edge_type, edge.src, edge.dst).as_bytes(),
                    )?;
                    txn.delete(out_edge_key.as_bytes())?;
                    if write_reverse_index {
                        txn.delete(
                            keys::in_edge(cell_id, &mutation.edge_type, edge.dst, edge.src)
                                .as_bytes(),
                        )?;
                        *in_decrements
                            .entry((mutation.edge_type.clone(), edge.dst))
                            .or_insert(0) += 1;
                    }
                } else {
                    txn.put(
                        keys::out_segment_tombstone(
                            cell_id,
                            &mutation.edge_type,
                            edge.src,
                            edge.dst,
                        )
                        .as_bytes(),
                        encode_u64(commit_epoch),
                    )?;
                }
                *out_decrements
                    .entry((mutation.edge_type.clone(), edge.src))
                    .or_insert(0) += 1;
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

        for (
            pending_index,
            (result_index, vertex_id, idempotency_key, previous, detach, constraints),
        ) in pending.into_iter().enumerate()
        {
            let requires_isolation =
                matches!(
                    mode,
                    VertexDeleteBatchMode::IsolatedOnly | VertexDeleteBatchMode::DetachAndIsolated
                ) && vertex_matches_path_node_constraints(vertex_id, &previous, &constraints);
            let connected =
                !detach && requires_isolation && connected_vertices.contains(&vertex_id);
            let vertex_deleted =
                !connected && (!previous.labels.is_empty() || !previous.properties.is_empty());
            if vertex_deleted {
                apply_vertex_metadata_update_txn(
                    &txn,
                    cell_id,
                    vertex_id,
                    &previous,
                    &VertexMetadata::default(),
                    commit_epoch,
                )?;
            }
            let changed = vertex_deleted
                || edge_counts[pending_index] > 0
                || relationship_counts[pending_index] > 0;
            let result = VertexDeleteResult {
                epoch: if changed { commit_epoch } else { current_epoch },
                vertex_deleted,
                incident_edges_deleted: edge_counts[pending_index],
                relationships_deleted: relationship_counts[pending_index],
            };
            txn.put(
                keys::idempotency(cell_id, mode.idempotency_operation(), &idempotency_key)
                    .as_bytes(),
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
            .delete_vertex_mutations_batch(
                cell_id,
                vec![(vertex_id, idempotency_key.to_string())],
                detach,
            )
            .await?;
        Ok(results
            .pop()
            .expect("one requested vertex has one deletion result"))
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
            ensure_limit(
                "delete_vertex_scan_incident_edges",
                scanned,
                self.limits.max_query_scan_edges,
            )?;
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
            let bytes = retained_bytes.fetch_add(
                (key.len() * 2 + kv.value.len() + 128) as u64,
                Ordering::Relaxed,
            ) + (key.len() * 2 + kv.value.len() + 128) as u64;
            if lock.is_none() {
                ensure_limit(
                    "prepare_vertex_delete_bytes",
                    bytes,
                    DELETE_PREPARATION_BYTES,
                )?;
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
        let jobs: Box<dyn Iterator<Item = ScanJob<'_>> + Send + '_> =
            Box::new(vertex_ids.iter().flat_map(|vertex| {
                let prefixes: Prefixes<'_> =
                    Box::new(edge_types.iter().flat_map(move |edge_type| {
                        [
                            keys::out_prefix(cell_id, edge_type, *vertex),
                            keys::in_prefix(cell_id, edge_type, *vertex),
                        ]
                        .into_iter()
                    }));
                let witness_only =
                    isolation_detach_set.is_some_and(|detach| !detach.contains(vertex));
                // Do not speculate past a candidate's first surviving edge.
                // Detach needs every prefix; isolation needs one sequential
                // search per candidate, overlapped with other candidates.
                let jobs: Box<dyn Iterator<Item = ScanJob<'_>> + Send + '_> =
                    if witness_only {
                        Box::new(std::iter::once((prefixes, true)))
                    } else {
                        Box::new(prefixes.map(|prefix| {
                            (Box::new(std::iter::once(prefix)) as Prefixes<'_>, false)
                        }))
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
                            ensure_limit(
                                "delete_vertex_scan_incident_edges",
                                count,
                                self.limits.max_query_scan_edges,
                            )?;
                            let key = String::from_utf8_lossy(&kv.key).into_owned();
                            let record = decode_edge_record(&key, &kv.value)?;
                            // Detach vertices own all removed edges. For an isolated
                            // candidate, only one edge surviving that detach is needed.
                            if witness_only
                                && isolation_detach_set.is_some_and(|detach| {
                                    detach.contains(&record.src) || detach.contains(&record.dst)
                                })
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
                                let bytes =
                                    retained_bytes.fetch_add(size, Ordering::Relaxed) + size;
                                ensure_limit(
                                    "prepare_vertex_delete_bytes",
                                    bytes,
                                    DELETE_PREPARATION_BYTES,
                                )?;
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
            .scan_prefix_with_options(
                keys::out_edge_cell_prefix(cell_id).as_bytes(),
                ..,
                &scan_options,
            )
            .await?;
        while let Some(kv) = out_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit(
                "delete_vertex_scan_edges",
                scanned,
                self.limits.max_query_scan_edges,
            )?;
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
            .scan_prefix_with_options(
                keys::relationship_cell_prefix(cell_id).as_bytes(),
                ..,
                &scan_options,
            )
            .await?;
        while let Some(kv) = relationship_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit(
                "delete_vertex_scan_relationships",
                scanned,
                self.limits.max_query_scan_edges,
            )?;
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
            .scan_prefix_with_options(
                keys::out_segment_cell_prefix(cell_id).as_bytes(),
                ..,
                &scan_options,
            )
            .await?;
        while let Some(kv) = segment_iter.next().await? {
            scanned = scanned.saturating_add(1);
            renew_vertex_delete_lock_after_items(lock, scanned).await?;
            ensure_limit(
                "delete_vertex_scan_segments",
                scanned,
                self.limits.max_query_scan_edges,
            )?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > read_epoch {
                continue;
            }
            for dst in segment.destinations {
                if !vertex_ids.contains(&segment.src) && !vertex_ids.contains(&dst) {
                    continue;
                }
                let tombstone_key =
                    keys::out_segment_tombstone(cell_id, &segment.edge_type, segment.src, dst);
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

    pub async fn drop_cell(
        &self,
        cell_id: &str,
        idempotency_key: &str,
    ) -> Result<GraphCellDropResult> {
        validate_component("cell_id", cell_id)?;
        validate_component("idempotency_key", idempotency_key)?;
        self.ensure_write_authority(cell_id, "drop_cell")?;

        let _permit = self.acquire_graph_write_permit("drop_cell")
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
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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

    async fn drop_cell_locked(
        &self,
        cell_id: &str,
        idempotency_key: &str,
        lock: &LocalWriteGuard,
    ) -> Result<GraphCellDropResult> {
        let idem_key = keys::cell_drop_idempotency(cell_id, idempotency_key);
        let marker_key = keys::cell_drop_marker(cell_id);
        let pending_marker_key = keys::cell_drop_pending_marker(cell_id);
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
            txn.put(
                idem_key.as_bytes(),
                encode_cell_drop_idempotency(cell_id, idempotency_key, &result),
            )?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            return Ok(result);
        }
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell")
            .await?;
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell")
            .await?;
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_cell_drop_idempotency(&idem_key, cell_id, idempotency_key, &value);
        }
        txn.put(marker_key.as_bytes(), encode_u64(marker_epoch))?;
        txn.delete(pending_marker_key.as_bytes())?;
        txn.put(
            idem_key.as_bytes(),
            encode_cell_drop_idempotency(cell_id, idempotency_key, &result),
        )?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(result)
    }

    async fn flush_drop_cell_batch(
        &self,
        cell_id: &str,
        keys_to_delete: &mut Vec<String>,
    ) -> Result<u64> {
        if keys_to_delete.is_empty() {
            return Ok(0);
        }
        let keys = std::mem::take(keys_to_delete);
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "drop_cell")
            .await?;
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
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_edge_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_edge_metadata")?;
        let _permit = self
            .acquire_graph_write_permit("set_edge_metadata")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_edge_metadata"))
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
                Err(err)
                    if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
                {
                    self.operation_metrics
                        .write_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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
        let _permit = self
            .acquire_graph_write_permit("set_edge_metadata_batch")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "set_edge_metadata_batch"))
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
                operation: "import_relationships_batch",
            },
        )
        .await
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

        let mut relationships = if options.create_always {
            validate_relationship_creates(cell_id, edge_type, relationships)?
        } else {
            coalesce_relationship_imports(cell_id, edge_type, relationships)?
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
        let fingerprint = relationship_import_fingerprint(cell_id, edge_type, &relationships);

        let span = write_txn_span(cell_id, Some(edge_type));
        let result = self.run_write_pipeline(async {
            let mut retries = WriteRetryCount::new();
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
            self.operation_metrics
                .write_commits
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn set_edge_metadata_txn(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        metadata: EdgeMetadata,
    ) -> Result<bool> {
        let lock = self
            .acquire_local_write_guard(cell_id, "set_edge_metadata")
            .await?;
        let result = self
            .set_edge_metadata_txn_locked(cell_id, edge_type, src, dst, metadata)
            .await;
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
        let lock = self
            .acquire_local_write_guard(cell_id, "set_edge_metadata_batch")
            .await?;
        let result = self
            .set_edge_metadata_batch_txn_locked(cell_id, edge_type, updates)
            .await;
        finish_local_write(lock, result).await
    }

    async fn set_edge_metadata_batch_txn_locked(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: Vec<(VertexId, VertexId, EdgeMetadata)>,
    ) -> Result<usize> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "set_edge_metadata_batch")
            .await?;
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
                    feature: format!(
                        "cannot set metadata for missing edge {edge_type}({src}->{dst})"
                    ),
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
        let lock = self
            .acquire_local_write_guard_no_fence(cell_id, options.operation)
            .await?;
        let result = self
            .import_relationships_batch_txn_locked(
                cell_id,
                edge_type,
                relationships,
                idempotency_key,
                fingerprint,
                options,
            )
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, options.operation)
            .await?;

        // A replay is defined by the original request, not by whether its
        // MATCH endpoints still exist at replay time. Consult the durable
        // result before evaluating the current endpoint snapshot.
        let idem_key = keys::idempotency(cell_id, "relationship-import", idempotency_key);
        if let Some(value) = read_txn_remote_uncached(&txn, &idem_key).await? {
            // This branch does not commit, so it cannot validate ownership at
            // the WAL barrier. Preserve the explicit replay fencing check.
            self.db.refresh_writer_fence().await?;
            self.operation_metrics
                .relationship_import_idempotency_replays
                .fetch_add(1, Ordering::Relaxed);
            return decode_relationship_import_idempotency(
                &idem_key,
                idempotency_key,
                fingerprint,
                &value,
            );
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
                for (vertex, label) in [
                    (relationship.src, source_label),
                    (relationship.dst, destination_label),
                ] {
                    required_labels.entry(vertex).or_default().insert(label);
                }
            }
            let endpoint_keys = required_labels
                .keys()
                .map(|vertex| keys::vertex(cell_id, *vertex));
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
                        matched_labels
                            .entry(vertex)
                            .or_default()
                            .insert(label.to_string());
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
            txn_span.record(
                "hydradb.relimport.endpoint_check_us",
                duration_micros_u64(profile.endpoint_check),
            );
        }
        let current_epoch = txn.seqnum();
        let current_relationship_id =
            read_counter_txn(&txn, &keys::last_relationship_id(cell_id)).await?;
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
            let allocated = next_available_relationship_ids_txn(
                &txn,
                cell_id,
                &mut next_relationship_id,
                relationships.len(),
                "CREATE",
            )
            .await?;
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
                        if relationship.metadata.properties.get(RELATIONSHIP_IDENTITY_PROPERTY)
                            != Some(&identity)
                        {
                            return Err(GraphError::CorruptValue {
                                key: format!(
                                    "cell/{cell_id}/relationship-merge/{edge_type}/{}/{external_id}",
                                    relationship.src
                                ),
                                reason: "relationship MERGE identity metadata does not match the parsed id"
                                    .to_string(),
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
                        if let Some(pointer) =
                            read_txn_remote_with_options(txn, &pointer_key, pointer_read_options)
                                .await?
                        {
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
                                let record_key = keys::relationship(
                                    cell_id,
                                    edge_type,
                                    relationship.src,
                                    relationship.dst,
                                    relationship_id,
                                );
                                if let Some(value) = read_txn_remote_with_options(
                                    txn,
                                    &record_key,
                                    pointer_read_options,
                                )
                                .await?
                                {
                                    let record = decode_relationship_record(&record_key, &value)?;
                                    if record
                                        .metadata
                                        .properties
                                        .get(RELATIONSHIP_IDENTITY_PROPERTY)
                                        == Some(&identity)
                                    {
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
                        MergePointerOutcome::Miss(_) | MergePointerOutcome::Stale(_) => {
                            profile.identity_pointer_misses += 1
                        }
                    }
                    let pointer_key = lookup.outcome.pointer_key();
                    match lookup.existing.as_slice() {
                        [record] => note_merge_pointer(
                            &mut merge_pointer_updates,
                            pointer_key,
                            record.relationship_id,
                        ),
                        [] => {}
                        _ => {
                            merge_pointer_updates.insert(pointer_key.to_string(), None);
                        }
                    }
                }
                profile.identity_scan = identity_scan_started.elapsed();
                txn_span.record(
                    "hydradb.relimport.identity_scan_us",
                    duration_micros_u64(profile.identity_scan),
                );
                txn_span.record(
                    "hydradb.relimport.identity_pointer_hits",
                    profile.identity_pointer_hits,
                );
                txn_span.record(
                    "hydradb.relimport.identity_pointer_misses",
                    profile.identity_pointer_misses,
                );
                let missing = lookups
                    .iter()
                    .filter(|lookup| lookup.existing.is_empty())
                    .count();
                let allocated = next_available_relationship_ids_txn(
                    &txn,
                    cell_id,
                    &mut next_relationship_id,
                    missing,
                    "MERGE",
                )
                .await?;
                let mut allocated = allocated.into_iter();
                let mut resolved = Vec::with_capacity(lookups.len());
                for lookup in lookups {
                    let MergeIdentityLookup {
                        relationship,
                        existing,
                        outcome,
                    } = lookup;
                    if existing.is_empty() {
                        let mut inserted = relationship;
                        inserted.relationship_id =
                            allocated.next().ok_or_else(|| GraphError::CorruptValue {
                                key: keys::last_relationship_id(cell_id),
                                reason: "relationship id allocation returned too few ids"
                                    .to_string(),
                            })?;
                        note_merge_pointer(
                            &mut merge_pointer_updates,
                            outcome.pointer_key(),
                            inserted.relationship_id,
                        );
                        resolved.push(inserted);
                    } else {
                        for record in existing {
                            let mut matched = relationship.clone();
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
            .filter(|relationship| {
                !prefetched_relationships.contains_key(&relationship.relationship_id)
            })
            .map(|relationship| {
                (
                    relationship.relationship_id,
                    keys::relationship(
                        cell_id,
                        edge_type,
                        relationship.src,
                        relationship.dst,
                        relationship.relationship_id,
                    ),
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
            let target_key =
                std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
                    key: id_key,
                    reason: format!("relationship id pointer is not UTF-8: {err}"),
                })?;
            let prefetched_key = keys::relationship(
                &record.cell_id,
                &record.edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            );
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
                    let target_key =
                        std::str::from_utf8(value).map_err(|err| GraphError::CorruptValue {
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
        let record_values =
            read_txn_remote_many(&txn, record_keys.values().map(|(key, _)| key.clone())).await?;
        profile.record_read += record_read_started.elapsed();
        txn_span.record(
            "hydradb.relimport.record_read_us",
            duration_micros_u64(profile.record_read),
        );
        for (relationship_id, (record_key, required)) in record_keys {
            match record_values.get(&record_key).and_then(Option::as_ref) {
                Some(value) => {
                    prefetched_relationships.insert(
                        relationship_id,
                        decode_relationship_record(&record_key, value)?,
                    );
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
            let rel_key = keys::relationship(
                cell_id,
                edge_type,
                relationship.src,
                relationship.dst,
                relationship.relationship_id,
            );
            let existing = prefetched_relationships
                .get(&relationship.relationship_id)
                .cloned();
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
                        let Some(properties) = guarded_metadata_patch(
                            &existing.metadata.properties,
                            requested_metadata.properties,
                            policy,
                            &rel_key,
                        )?
                        else {
                            relationships_already_existed =
                                relationships_already_existed.saturating_add(1);
                            continue;
                        };
                        requested_metadata.properties = properties;
                    }
                    let next_metadata =
                        merge_edge_metadata(&existing.metadata, &requested_metadata);
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
                if direct_edge_values
                    .get(&key)
                    .and_then(Option::as_ref)
                    .is_some()
                {
                    structural_edges_to_insert.remove(&(*src, *dst));
                } else {
                    missing_by_src.entry(*src).or_default().insert(*dst);
                }
            }
            let segment_neighbors = stream::iter(missing_by_src.keys().copied().map(|src| {
                let txn = &txn;
                async move {
                    let neighbors = out_segment_neighbors_for_src_txn(
                        txn,
                        cell_id,
                        edge_type,
                        src,
                        current_epoch,
                    )
                    .await?;
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
            profile.segment_neighbors = segment_neighbors
                .values()
                .map(|neighbors| neighbors.len() as u64)
                .sum();
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
        txn_span.record(
            "hydradb.relimport.segment_neighbors",
            profile.segment_neighbors,
        );
        let structural_edges_inserted =
            u64::try_from(structural_edges_to_insert.len()).map_err(|err| {
                GraphError::CorruptValue {
                    key: "relationship_import".to_string(),
                    reason: format!("too many structural edges in one import: {err}"),
                }
            })?;
        let structural_edges_already_existed = u64::try_from(
            structural_edges
                .len()
                .saturating_sub(structural_edges_to_insert.len()),
        )
        .map_err(|err| GraphError::CorruptValue {
            key: "relationship_import".to_string(),
            reason: format!("too many existing structural edges in one import: {err}"),
        })?;
        let relationships_inserted_count =
            u64::try_from(relationships_inserted.len()).map_err(|err| {
                GraphError::CorruptValue {
                    key: "relationship_import".to_string(),
                    reason: format!("too many relationships in one import: {err}"),
                }
            })?;
        let changed = relationships_inserted_count > 0
            || structural_edges_inserted > 0
            || !relationships_updated.is_empty();
        let epoch = if changed {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
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
            .chain(
                in_increments
                    .keys()
                    .map(|dst| keys::degree_in(cell_id, edge_type, *dst)),
            )
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
        txn_span.record(
            "hydradb.relimport.counter_read_us",
            duration_micros_u64(profile.counter_read),
        );
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
            txn.put(
                keys::out_edge(cell_id, edge_type, src, dst).as_bytes(),
                &edge_value,
            )?;
            if write_reverse_index {
                txn.put(
                    keys::in_edge(cell_id, edge_type, dst, src).as_bytes(),
                    &edge_value,
                )?;
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
                keys::relationship(
                    cell_id,
                    edge_type,
                    relationship.src,
                    relationship.dst,
                    relationship.relationship_id,
                )
                .as_bytes(),
                &value,
            )?;
            txn.put(
                keys::relationship_id(cell_id, relationship.relationship_id).as_bytes(),
                keys::relationship(
                    cell_id,
                    edge_type,
                    relationship.src,
                    relationship.dst,
                    relationship.relationship_id,
                )
                .as_bytes(),
            )?;
            put_relationship_property_indexes_txn(&txn, &record)?;
        }
        for (record, previous_metadata) in &relationships_updated {
            let key = keys::relationship(
                cell_id,
                edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            );
            txn.put(
                key.as_bytes(),
                encode_relationship_record(record).as_slice(),
            )?;
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
        txn.put(
            idem_key.as_bytes(),
            encode_relationship_import_idempotency(idempotency_key, fingerprint, &result),
        )?;
        // Last so these outrank the blind invalidations the property-index
        // hooks above issued for the same keys within this transaction.
        #[cfg(feature = "opencypher")]
        for (pointer_key, update) in &merge_pointer_updates {
            match update {
                Some(relationship_id) => txn.put(
                    pointer_key.as_bytes(),
                    encode_u64(*relationship_id).as_slice(),
                )?,
                None => txn.delete(pointer_key.as_bytes())?,
            }
        }
        let commit_started = std::time::Instant::now();
        commit_txn_traced(txn, self.await_durable_writes, cell_id, edge_type, epoch).await?;
        profile.commit = commit_started.elapsed();
        txn_span.record(
            "hydradb.relimport.commit_us",
            duration_micros_u64(profile.commit),
        );
        self.record_relationship_import_profile(&profile);
        Ok(result)
    }

    pub async fn create_relationship(
        &self,
        mutation: EdgeMutation,
        edge_metadata: EdgeMetadata,
    ) -> Result<RelationshipCreateResult> {
        self.create_relationship_with_full_metadata(
            mutation,
            VertexMetadata::default(),
            VertexMetadata::default(),
            edge_metadata,
        )
        .await
    }

    pub async fn create_relationship_with_vertex_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    ) -> Result<RelationshipCreateResult> {
        self.create_relationship_with_full_metadata(
            mutation,
            src_metadata,
            dst_metadata,
            EdgeMetadata::default(),
        )
        .await
    }

    pub async fn create_relationship_with_full_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    ) -> Result<RelationshipCreateResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        validate_edge_metadata(&edge_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "create_relationship")?;

        let metadata_updates = coalesce_vertex_metadata_updates([
            (mutation.src, src_metadata),
            (mutation.dst, dst_metadata),
        ])?;
        let fingerprint =
            relationship_create_fingerprint(&mutation, metadata_updates.as_slice(), &edge_metadata);
        let _permit = self
            .acquire_graph_write_permit("create_relationship")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "create_relationship"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(
                tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id),
            )
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .create_relationship_txn(&mutation, &metadata_updates, &edge_metadata, fingerprint)
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
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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
            .acquire_local_write_guard(&mutation.cell_id, "create_relationship")
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "create_relationship")
            .instrument(
                tracing::info_span!("write.fence_validate", hydradb.cell_id = %mutation.cell_id),
            )
            .await?;
        let idem_key = keys::idempotency(
            &mutation.cell_id,
            "relationship-create",
            &mutation.idempotency_key,
        );
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_create_idempotency(
                &idem_key,
                mutation,
                fingerprint,
                &value,
            );
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
        let epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
                key: "storage_sequence".to_string(),
                reason: "epoch overflow during relationship create".to_string(),
            })?;

        let mut relationship_id =
            read_counter_txn(&txn, &keys::last_relationship_id(&mutation.cell_id))
                .await?
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_relationship_id(&mutation.cell_id),
                    reason: "relationship id overflow".to_string(),
                })?;
        loop {
            if read_txn_remote(
                &txn,
                &keys::relationship_id(&mutation.cell_id, relationship_id),
            )
            .await?
            .is_none()
            {
                break;
            }
            relationship_id =
                relationship_id
                    .checked_add(1)
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: keys::last_relationship_id(&mutation.cell_id),
                        reason: "relationship id overflow".to_string(),
                    })?;
        }

        let mut changed_metadata = Vec::new();
        for (vertex_id, requested) in metadata_updates {
            let vertex_key = keys::vertex(&mutation.cell_id, *vertex_id);
            let previous = match read_txn_remote(&txn, &vertex_key).await? {
                Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
                None => VertexMetadata::default(),
            };
            let next = merge_vertex_metadata(&previous, requested);
            if previous != next {
                changed_metadata.push((*vertex_id, previous, next));
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
                previous,
                next,
                epoch,
            )?;
        }

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
            let out_degree_key =
                keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
            let out_degree = read_counter_txn(&txn, &out_degree_key).await? + 1;
            let in_degree = if self.writes_reverse_index() {
                let in_degree_key =
                    keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
                let in_degree = read_counter_txn(&txn, &in_degree_key).await? + 1;
                Some((in_degree_key, in_degree))
            } else {
                None
            };
            txn.put(
                keys::out_edge(
                    &mutation.cell_id,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                )
                .as_bytes(),
                &edge_value,
            )?;
            if self.writes_reverse_index() {
                txn.put(
                    keys::in_edge(
                        &mutation.cell_id,
                        &mutation.edge_type,
                        mutation.dst,
                        mutation.src,
                    )
                    .as_bytes(),
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
        let relationship_key = keys::relationship(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            relationship_id,
        );
        txn.put(
            relationship_key.as_bytes(),
            encode_relationship_record(&record),
        )?;
        txn.put(
            keys::relationship_id(&mutation.cell_id, relationship_id).as_bytes(),
            relationship_key.as_bytes(),
        )?;
        let relationship_count_key = keys::relationship_count(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let relationship_count = read_counter_txn(&txn, &relationship_count_key).await? + 1;
        txn.put(
            relationship_count_key.as_bytes(),
            encode_u64(relationship_count),
        )?;
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
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_edge_metadata(&metadata)?;
        self.ensure_write_authority(cell_id, "set_relationship_metadata")?;
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
                .set_relationship_metadata_txn(
                    cell_id,
                    edge_type,
                    src,
                    dst,
                    relationship_id,
                    metadata.clone(),
                )
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
                    if changed {
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
            .acquire_local_write_guard(cell_id, "set_relationship_metadata")
            .await?;
        let result = self
            .set_relationship_metadata_txn_locked(
                cell_id,
                edge_type,
                src,
                dst,
                relationship_id,
                metadata,
            )
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
        txn.put(
            key.as_bytes(),
            encode_relationship_record(&record).as_slice(),
        )?;
        delete_relationship_property_indexes_txn(&txn, &record, &previous)?;
        put_relationship_property_indexes_txn(&txn, &record)?;
        commit_txn_strict(txn, self.await_durable_writes).await?;
        Ok(true)
    }

    pub async fn delete_relationship(
        &self,
        mutation: EdgeMutation,
        relationship_id: RelationshipId,
    ) -> Result<DeleteResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_relationship")?;

        let _permit = self
            .acquire_graph_write_permit("delete_relationship")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "delete_relationship"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(
                tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id),
            )
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .delete_relationship_txn(&mutation, relationship_id)
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
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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

    async fn delete_relationship_txn(
        &self,
        mutation: &EdgeMutation,
        relationship_id: RelationshipId,
    ) -> Result<DeleteResult> {
        let lock = self
            .acquire_local_write_guard(&mutation.cell_id, "delete_relationship")
            .await?;
        let result = self
            .delete_relationship_txn_locked(mutation, relationship_id)
            .await;
        finish_local_write(lock, result).await
    }

    async fn delete_relationship_txn_locked(
        &self,
        mutation: &EdgeMutation,
        relationship_id: RelationshipId,
    ) -> Result<DeleteResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "delete_relationship")
            .instrument(
                tracing::info_span!("write.fence_validate", hydradb.cell_id = %mutation.cell_id),
            )
            .await?;
        let idem_key = keys::idempotency(
            &mutation.cell_id,
            "relationship-delete",
            &mutation.idempotency_key,
        );
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_relationship_delete_idempotency(
                &idem_key,
                mutation,
                relationship_id,
                &value,
            );
        }

        let current_epoch = txn.seqnum();
        let key = keys::relationship(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
            relationship_id,
        );
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
        let epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
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
        let relationship_count_key = keys::relationship_count(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let relationship_count = read_counter_txn(&txn, &relationship_count_key)
            .await?
            .saturating_sub(1);
        if relationship_count == 0 {
            txn.delete(relationship_count_key.as_bytes())?;
        } else {
            txn.put(
                relationship_count_key.as_bytes(),
                encode_u64(relationship_count),
            )?;
        }
        delete_relationship_property_indexes_txn(&txn, &record, &record.metadata)?;

        if !other_live_relationships {
            delete_structural_edge_txn(self, &txn, mutation, epoch).await?;
        }

        let result = DeleteResult {
            epoch,
            deleted: true,
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
            epoch,
        )
        .await?;
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
        let result = self
            .delete_relationship_mutations_batch_with_topology(cell_id, deletions)
            .await;
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
        let mutations = deletions
            .iter()
            .map(|(mutation, _)| mutation.clone())
            .collect::<Vec<_>>();
        validate_edge_mutations_for_cell(
            cell_id,
            &mutations,
            "delete_relationship_mutations_batch",
        )?;
        ensure_limit(
            "reserve_edge_delete_noops_batch",
            structural_delete_guards.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        validate_edge_mutations_for_cell(
            cell_id,
            &structural_delete_guards,
            "reserve_edge_delete_noops_batch",
        )?;
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
            if !identities.insert((
                mutation.edge_type.clone(),
                mutation.src,
                mutation.dst,
                *relationship_id,
            )) {
                return Err(GraphError::IdempotencyConflict {
                    operation: "relationship-delete",
                    idempotency_key: mutation.idempotency_key.clone(),
                    reason: "the same relationship appears twice in one batch",
                });
            }
        }

        let span = write_txn_span(cell_id, common_edge_type(&mutations));
        async {
            let mut retries = WriteRetryCount::new();
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

    #[cfg(feature = "opencypher")]
    async fn delete_relationship_mutations_batch_txn(
        &self,
        cell_id: &str,
        deletions: &[(EdgeMutation, RelationshipId)],
        structural_delete_guards: &[EdgeMutation],
    ) -> Result<RelationshipDeleteBatchResult> {
        let lock = self
            .acquire_local_write_guard(cell_id, "delete_relationship_mutations_batch")
            .await?;
        let result = self
            .delete_relationship_mutations_batch_txn_locked(
                cell_id,
                deletions,
                structural_delete_guards,
            )
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
        let commit_epoch =
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "storage_sequence".to_string(),
                    reason: "epoch overflow during relationship delete batch".to_string(),
                })?;
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut pending = Vec::new();

        let read_keys = deletions.iter().flat_map(|(mutation, relationship_id)| {
            [
                keys::idempotency(cell_id, "relationship-delete", &mutation.idempotency_key),
                keys::relationship(
                    cell_id,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                    *relationship_id,
                ),
            ]
        });
        let existing = read_txn_remote_many(&txn, read_keys)
            .instrument(tracing::info_span!(
                "write.delete_canonical_read",
                items = deletions.len()
            ))
            .await?;
        for (mutation, relationship_id) in deletions {
            let idem_key =
                keys::idempotency(cell_id, "relationship-delete", &mutation.idempotency_key);
            if let Some(value) = existing.get(&idem_key).and_then(Option::as_ref) {
                let result = decode_relationship_delete_idempotency(
                    &idem_key,
                    mutation,
                    *relationship_id,
                    value,
                )?;
                if result.deleted {
                    deleted = deleted.saturating_add(1);
                } else {
                    already_deleted = already_deleted.saturating_add(1);
                }
                continue;
            }

            let relationship_key = keys::relationship(
                cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
                *relationship_id,
            );
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
            pending.push((
                mutation.clone(),
                *relationship_id,
                relationship_key,
                idem_key,
                record,
            ));
        }

        let mut deleting_by_edge =
            BTreeMap::<(String, VertexId, VertexId), BTreeSet<RelationshipId>>::new();
        let mut mutation_by_edge = BTreeMap::<(String, VertexId, VertexId), EdgeMutation>::new();
        for (mutation, relationship_id, _, _, _) in &pending {
            let edge = (mutation.edge_type.clone(), mutation.src, mutation.dst);
            deleting_by_edge
                .entry(edge.clone())
                .or_default()
                .insert(*relationship_id);
            mutation_by_edge
                .entry(edge)
                .or_insert_with(|| mutation.clone());
        }

        let mut structural_deletes = Vec::new();
        for ((edge_type, src, dst), deleting) in &deleting_by_edge {
            let has_survivor =
                has_surviving_relationship_txn(&txn, cell_id, edge_type, *src, *dst, deleting)
                    .instrument(tracing::info_span!(
                        "write.delete_survivor_check",
                        items = deleting.len()
                    ))
                    .await?;
            if !has_survivor {
                structural_deletes.push((edge_type.clone(), *src, *dst));
            }
        }

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
            let count = read_counter_txn(&txn, &count_key)
                .await?
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
                delete_structural_edge_txn(self, &txn, mutation, commit_epoch).await?;
        }
        let topology_sequence = topology_changed.then_some(commit_epoch);

        let recorded_epoch = if deleted > 0 {
            commit_epoch
        } else {
            current_epoch
        };
        let edge_type = deletions
            .first()
            .map(|(mutation, _)| mutation.edge_type.as_str())
            .filter(|first| {
                deletions
                    .iter()
                    .all(|(mutation, _)| mutation.edge_type == *first)
            })
            .unwrap_or("");
        commit_txn_traced(
            txn,
            self.await_durable_writes,
            cell_id,
            edge_type,
            recorded_epoch,
        )
        .await?;
        Ok(RelationshipDeleteBatchResult {
            deleted,
            already_deleted,
            topology_sequence,
        })
    }

    pub async fn write_edge(&self, mutation: EdgeMutation) -> Result<CommitResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge")?;

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
                .instrument(
                    tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id),
                )
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self.write_edge_txn(&mutation).instrument(tracing::info_span!("storage.txn")).await {
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
        let lock = self
            .acquire_local_write_guard(&mutation.cell_id, "write_edge")
            .await?;
        let result = self.write_edge_txn_locked(mutation).await;
        finish_local_write(lock, result).await
    }

    async fn write_edge_txn_locked(&self, mutation: &EdgeMutation) -> Result<CommitResult> {
        self.write_edge_txn_locked_with_metadata(
            mutation,
            &[],
            &EdgeMetadata::default(),
            "write_edge",
        )
        .await
    }

    pub async fn write_edge_with_vertex_metadata(
        &self,
        mutation: EdgeMutation,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    ) -> Result<CommitResult> {
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge_with_vertex_metadata")?;

        let metadata_updates = coalesce_vertex_metadata_updates([
            (mutation.src, src_metadata),
            (mutation.dst, dst_metadata),
        ])?;
        let _permit = self
            .acquire_graph_write_permit("write_edge_with_vertex_metadata")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "write_edge_with_vertex_metadata"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(
                tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id),
            )
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_with_vertex_metadata_txn(&mutation, &metadata_updates)
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
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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
            .acquire_local_write_guard(&mutation.cell_id, "write_edge_with_vertex_metadata")
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
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        validate_vertex_metadata(&src_metadata)?;
        validate_vertex_metadata(&dst_metadata)?;
        validate_edge_metadata(&edge_metadata)?;
        self.ensure_write_authority(&mutation.cell_id, "write_edge_with_full_metadata")?;

        let metadata_updates = coalesce_vertex_metadata_updates([
            (mutation.src, src_metadata),
            (mutation.dst, dst_metadata),
        ])?;
        let _permit = self
            .acquire_graph_write_permit("write_edge_with_full_metadata")
            .instrument(
                tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "write_edge_with_full_metadata"),
            )
            .await?;
        let _writer = self
            .writer_lane(&mutation.cell_id)
            .lock()
            .instrument(
                tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id),
            )
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .write_edge_with_full_metadata_txn(&mutation, &metadata_updates, &edge_metadata)
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
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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
            .acquire_local_write_guard(&mutation.cell_id, "write_edge_with_full_metadata")
            .await?;
        let result = self
            .write_edge_txn_locked_with_metadata(
                mutation,
                metadata_updates,
                edge_metadata,
                "write_edge_with_full_metadata",
            )
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, operation)
            .instrument(
                tracing::info_span!("write.fence_validate", hydradb.cell_id = %mutation.cell_id),
            )
            .await?;
        let fingerprint = relationship_create_fingerprint(mutation, metadata_updates, edge_metadata);
        let idem_key = keys::idempotency(&mutation.cell_id, "create", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_commit_idempotency(&idem_key, mutation, fingerprint, &value);
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
        for (vertex_id, requested) in metadata_updates {
            let vertex_key = keys::vertex(&mutation.cell_id, *vertex_id);
            let previous = match read_txn_remote(&txn, &vertex_key).await? {
                Some(value) => decode_vertex_metadata(&vertex_key, &value)?,
                None => VertexMetadata::default(),
            };
            let next = merge_vertex_metadata(&previous, requested);
            if previous != next {
                changed_metadata.push((*vertex_id, previous, next));
            }
        }
        let edge_metadata_key = keys::edge_metadata(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
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
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, fingerprint, &result),
                )?;
                commit_txn_traced(
                    txn,
                    self.await_durable_writes,
                    &mutation.cell_id,
                    &mutation.edge_type,
                    result.epoch,
                )
                .await?;
                return Ok(result);
            }
        }

        let epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| GraphError::CorruptValue {
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
                previous,
                next,
                epoch,
            )?;
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
            txn.put(
                idem_key.as_bytes(),
                encode_commit_idempotency(mutation, fingerprint, &result),
            )?;
            commit_txn_traced(
                txn,
                self.await_durable_writes,
                &mutation.cell_id,
                &mutation.edge_type,
                epoch,
            )
            .await?;
            return Ok(result);
        }

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
            let in_degree_key =
                keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key).await? + 1;
            Some((in_degree_key, in_degree))
        } else {
            None
        };

        txn.put(
            keys::out_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
            &edge_value,
        )?;
        if self.writes_reverse_index() {
            txn.put(
                keys::in_edge(
                    &mutation.cell_id,
                    &mutation.edge_type,
                    mutation.dst,
                    mutation.src,
                )
                .as_bytes(),
                &edge_value,
            )?;
        }
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
        txn.put(
            idem_key.as_bytes(),
            encode_commit_idempotency(mutation, fingerprint, &result),
        )?;

        commit_txn_traced(
            txn,
            self.await_durable_writes,
            &mutation.cell_id,
            &mutation.edge_type,
            epoch,
        )
        .await?;
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
        ensure_limit(
            "delete_edges_batch",
            edges.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
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
                start_epoch = Some(
                    start_epoch.map_or(result.start_epoch, |epoch| epoch.min(result.start_epoch)),
                );
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
                start_epoch = Some(
                    start_epoch.map_or(result.start_epoch, |epoch| epoch.min(result.start_epoch)),
                );
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

    async fn delete_edge_mutations_batch_txn(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
    ) -> Result<EdgeDeleteBatchResult> {
        let lock = self
            .acquire_local_write_guard(cell_id, "delete_edge_mutations_batch")
            .await?;
        let result = self
            .delete_edge_mutations_batch_txn_locked(cell_id, mutations)
            .await;
        finish_local_write(lock, result).await
    }

    async fn delete_edge_mutations_batch_txn_locked(
        &self,
        cell_id: &str,
        mutations: &[EdgeMutation],
    ) -> Result<EdgeDeleteBatchResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "delete_edge_mutations_batch")
            .instrument(tracing::info_span!("write.fence_validate", hydradb.cell_id = %cell_id))
            .await?;

        let current_epoch = txn.seqnum();
        let commit_epoch =
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "storage_sequence".to_string(),
                    reason: "SlateDB storage sequence overflow during edge delete batch"
                        .to_string(),
                })?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut deleted = 0_u64;
        let mut already_deleted = 0_u64;
        let mut out_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_decrements = BTreeMap::<(String, VertexId), u64>::new();
        let mut segment_edges_by_type_src =
            BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
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
                        let edges = out_segment_edges_for_src_txn(
                            &txn,
                            cell_id,
                            &mutation.edge_type,
                            mutation.src,
                            current_epoch,
                        )
                        .await?;
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
                txn.put(
                    idem_key.as_bytes(),
                    encode_delete_idempotency(mutation, &result),
                )?;
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
            let edge_metadata_key =
                keys::edge_metadata(cell_id, &mutation.edge_type, mutation.src, mutation.dst);
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
                txn.delete(
                    keys::edge(cell_id, &mutation.edge_type, mutation.src, mutation.dst).as_bytes(),
                )?;
                txn.delete(edge_key.as_bytes())?;
                if write_reverse_index {
                    txn.delete(
                        keys::in_edge(cell_id, &mutation.edge_type, mutation.dst, mutation.src)
                            .as_bytes(),
                    )?;
                }
                if write_reverse_index {
                    *in_decrements
                        .entry((mutation.edge_type.clone(), mutation.dst))
                        .or_insert(0) += 1;
                }
            } else {
                txn.put(
                    keys::out_segment_tombstone(
                        cell_id,
                        &mutation.edge_type,
                        mutation.src,
                        mutation.dst,
                    )
                    .as_bytes(),
                    encode_u64(next_epoch),
                )?;
            }
            *out_decrements
                .entry((mutation.edge_type.clone(), mutation.src))
                .or_insert(0) += 1;
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
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
        let deleted_start_epoch = results
            .iter()
            .filter(|result| result.deleted)
            .map(|result| result.epoch)
            .min();
        let deleted_end_epoch = results
            .iter()
            .filter(|result| result.deleted)
            .map(|result| result.epoch)
            .max();
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
        validate_component("cell_id", &mutation.cell_id)?;
        validate_component("edge_type", &mutation.edge_type)?;
        validate_component("idempotency_key", &mutation.idempotency_key)?;
        self.ensure_write_authority(&mutation.cell_id, "delete_edge")?;

        let span = write_txn_span(&mutation.cell_id, Some(&mutation.edge_type));
        async {
            let mut retries = WriteRetryCount::new();
            let _permit = self.acquire_graph_write_permit("delete_edge")
                .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %mutation.cell_id, hydradb.write.operation = "delete_edge"))
                .await?;
            let _writer = self
                .writer_lane(&mutation.cell_id)
                .lock()
                .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %mutation.cell_id))
                .await;
            for attempt in 0..GRAPH_TXN_MAX_RETRIES {
                match self.delete_edge_txn(&mutation).instrument(tracing::info_span!("storage.txn")).await {
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
        let lock = self
            .acquire_local_write_guard(&mutation.cell_id, "delete_edge")
            .await?;
        let result = self.delete_edge_txn_locked(mutation).await;
        finish_local_write(lock, result).await
    }

    async fn delete_edge_txn_locked(&self, mutation: &EdgeMutation) -> Result<DeleteResult> {
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, &mutation.cell_id, "delete_edge")
            .await?;
        let idem_key = keys::idempotency(&mutation.cell_id, "delete", &mutation.idempotency_key);

        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_delete_idempotency(&idem_key, mutation, &value);
        }

        let canonical_key = keys::edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let edge_key = keys::out_edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );

        let Some(existing) = read_txn_remote(&txn, &edge_key).await? else {
            let current_epoch = txn.seqnum();
            let segment_edge = if self.writes_reverse_index() {
                None
            } else {
                self.out_segment_edge_record_at(
                    &mutation.cell_id,
                    &mutation.edge_type,
                    mutation.src,
                    mutation.dst,
                    current_epoch,
                )
                .await?
            };
            let Some((segment_sequence, _segment_edge)) = segment_edge else {
                let result = DeleteResult {
                    epoch: current_epoch,
                    deleted: false,
                };
                txn.put(
                    idem_key.as_bytes(),
                    encode_delete_idempotency(mutation, &result),
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
            let tombstone_key = keys::out_segment_tombstone(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            );
            if let Some(value) = read_txn_remote(&txn, &tombstone_key).await? {
                let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
                if !segment_edge_visible(segment_sequence, Some(tombstone_epoch)) {
                    let result = DeleteResult {
                        epoch: current_epoch,
                        deleted: false,
                    };
                    txn.put(
                        idem_key.as_bytes(),
                        encode_delete_idempotency(mutation, &result),
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
                }
            }
            let epoch = current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: "storage_sequence".to_string(),
                    reason: "epoch overflow".to_string(),
                })?;
            let result = DeleteResult {
                epoch,
                deleted: true,
            };
            self.mark_topology_change_txn(
                &txn,
                &mutation.cell_id,
                &mutation.edge_type,
                epoch,
                &[(mutation.src, mutation.dst, false)],
            )
            .await?;
            let out_degree_key =
                keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
            let out_degree = read_counter_txn(&txn, &out_degree_key)
                .await?
                .saturating_sub(1);
            let edge_metadata_key = keys::edge_metadata(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            );
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
            txn.put(
                idem_key.as_bytes(),
                encode_delete_idempotency(mutation, &result),
            )?;
            commit_txn_traced(
                txn,
                self.await_durable_writes,
                &mutation.cell_id,
                &mutation.edge_type,
                epoch,
            )
            .await?;
            return Ok(result);
        };

        decode_edge_record(&edge_key, &existing)?;
        let epoch = next_epoch_txn(&txn, &mutation.cell_id).await?;
        let result = DeleteResult {
            epoch,
            deleted: true,
        };
        self.mark_topology_change_txn(
            &txn,
            &mutation.cell_id,
            &mutation.edge_type,
            epoch,
            &[(mutation.src, mutation.dst, false)],
        )
        .await?;

        let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
        let out_degree = read_counter_txn(&txn, &out_degree_key)
            .await?
            .saturating_sub(1);
        let in_degree = if self.writes_reverse_index() {
            let in_degree_key =
                keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
            let in_degree = read_counter_txn(&txn, &in_degree_key)
                .await?
                .saturating_sub(1);
            Some((in_degree_key, in_degree))
        } else {
            None
        };
        let edge_metadata_key = keys::edge_metadata(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        );
        let previous_edge_metadata = match read_txn_remote(&txn, &edge_metadata_key).await? {
            Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
            None => EdgeMetadata::default(),
        };

        delete_relationships_for_structural_edge_txn(&txn, mutation, epoch.saturating_sub(1))
            .await?;
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
        txn.delete(
            keys::out_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.src,
                mutation.dst,
            )
            .as_bytes(),
        )?;
        txn.delete(
            keys::in_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.dst,
                mutation.src,
            )
            .as_bytes(),
        )?;
        txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
        if let Some((in_degree_key, in_degree)) = in_degree {
            txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
        }
        txn.put(
            idem_key.as_bytes(),
            encode_delete_idempotency(mutation, &result),
        )?;

        commit_txn_traced(
            txn,
            self.await_durable_writes,
            &mutation.cell_id,
            &mutation.edge_type,
            epoch,
        )
        .await?;
        Ok(result)
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_import_edges_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            BulkImportOptions::default(),
        )
        .await
    }

    pub async fn bulk_append_edges_trusted(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<BulkImportResult> {
        self.bulk_append_edges_trusted_bounded(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            DEFAULT_TRUSTED_APPEND_CHUNK_EDGES,
        )
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
                .bulk_append_out_adjacency_segment_trusted_txn(
                    cell_id,
                    edge_type,
                    src,
                    &dsts,
                    idempotency_key,
                    fingerprint,
                )
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
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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
        ensure_limit(
            "bulk_import_edges",
            edges.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        edges.sort_unstable();
        edges.dedup();
        let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);

        let _permit = self.acquire_graph_write_permit("bulk_import_edges")
            .instrument(tracing::info_span!("shard.write_permit", hydradb.cell_id = %cell_id, hydradb.write.operation = "bulk_import_edges"))
            .await?;
        let _writer = self
            .writer_lane(cell_id)
            .lock()
            .instrument(tracing::info_span!("shard.writer_lane", hydradb.cell_id = %cell_id))
            .await;
        for attempt in 0..GRAPH_TXN_MAX_RETRIES {
            match self
                .bulk_import_edges_txn(
                    cell_id,
                    edge_type,
                    &edges,
                    idempotency_key,
                    fingerprint,
                    options,
                )
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
                Ok(result) => {
                    self.operation_metrics
                        .write_commits
                        .fetch_add(1, Ordering::Relaxed);
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
        self.write_edge_mutations_batch_with_endpoint_labels(
            cell_id,
            mutations.into_iter().collect(),
            None,
            "write_edge_mutations_batch",
        )
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
        ensure_limit(
            operation,
            mutations.len() as u64,
            self.limits.max_bulk_import_edges as u64,
        )?;
        for mutation in &mutations {
            validate_component("cell_id", &mutation.cell_id)?;
            validate_component("edge_type", &mutation.edge_type)?;
            validate_component("idempotency_key", &mutation.idempotency_key)?;
            if mutation.cell_id != cell_id {
                return Err(GraphError::CorruptValue {
                    key: format!("cell/{cell_id}/write_edge_mutations_batch"),
                    reason: format!(
                        "batch contains mutation for different cell {}",
                        mutation.cell_id
                    ),
                });
            }
        }

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
                let result = self
                    .write_edge_mutations_batch(cell_id, std::mem::take(&mut chunk))
                    .await?;
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
        let lock = self.acquire_local_write_guard(cell_id, operation).await?;
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
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
        let commit_epoch =
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: operation.to_string(),
                    reason: "SlateDB storage sequence overflow during edge mutation batch"
                        .to_string(),
                })?;
        let mut next_epoch = current_epoch;
        let mut results = Vec::with_capacity(mutations.len());
        let mut known_edges = BTreeMap::<(String, VertexId, VertexId), StorageSequence>::new();
        let mut validated_endpoints = BTreeSet::<(VertexId, String)>::new();
        let mut segment_edges_by_type_src =
            BTreeMap::<(String, VertexId), BTreeMap<VertexId, StorageSequence>>::new();
        let mut out_increments = BTreeMap::<(String, VertexId), u64>::new();
        let mut in_increments = BTreeMap::<(String, VertexId), u64>::new();
        let write_reverse_index = self.writes_reverse_index();
        let mut inserted = 0_u64;
        let mut already_existed = 0_u64;

        for mutation in mutations {
            let fingerprint = relationship_create_fingerprint(mutation, &[], &EdgeMetadata::default());
            let idem_key = keys::idempotency(cell_id, "create", &mutation.idempotency_key);
            if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
                let result = decode_commit_idempotency(&idem_key, mutation, fingerprint, &value)?;
                if result.already_existed {
                    already_existed = already_existed.saturating_add(1);
                } else {
                    inserted = inserted.saturating_add(1);
                }
                results.push(result);
                continue;
            }

            if let Some((source_label, destination_label)) = endpoint_labels {
                for (vertex, label) in [
                    (mutation.src, source_label),
                    (mutation.dst, destination_label),
                ] {
                    if !validated_endpoints.insert((vertex, label.to_string())) {
                        continue;
                    }
                    let key = keys::vertex(cell_id, vertex);
                    let Some(value) = read_txn_remote(&txn, &key).await? else {
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Mutation,
                            dialect: "OpenCypher",
                            feature: format!(
                                "MATCH endpoint vertex {vertex} with label {label} does not exist"
                            ),
                        });
                    };
                    let metadata = decode_vertex_metadata(&key, &value)?;
                    if !metadata.labels.contains(label) {
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Mutation,
                            dialect: "OpenCypher",
                            feature: format!(
                                "MATCH endpoint vertex {vertex} does not have label {label}"
                            ),
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
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, fingerprint, &result),
                )?;
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
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, fingerprint, &result),
                )?;
                already_existed = already_existed.saturating_add(1);
                results.push(result);
                continue;
            }

            let segment_cache_key = (mutation.edge_type.clone(), mutation.src);
            let segment_epoch = match segment_edges_by_type_src.get(&segment_cache_key) {
                Some(edges) => edges.get(&mutation.dst).copied(),
                None => {
                    let edges = out_segment_edges_for_src_txn(
                        &txn,
                        cell_id,
                        &mutation.edge_type,
                        mutation.src,
                        current_epoch,
                    )
                    .await?;
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
                txn.put(
                    idem_key.as_bytes(),
                    encode_commit_idempotency(mutation, fingerprint, &result),
                )?;
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
                    keys::in_edge(cell_id, &mutation.edge_type, mutation.dst, mutation.src)
                        .as_bytes(),
                    &edge_value,
                )?;
            }
            txn.put(
                idem_key.as_bytes(),
                encode_commit_idempotency(mutation, fingerprint, &result),
            )?;
            known_edges.insert(identity, next_epoch);
            *out_increments
                .entry((mutation.edge_type.clone(), mutation.src))
                .or_insert(0) += 1;
            if write_reverse_index {
                *in_increments
                    .entry((mutation.edge_type.clone(), mutation.dst))
                    .or_insert(0) += 1;
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
        self.bulk_import_edges(cell_id, edge_type, edges, idempotency_key)
            .await
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
        self.bulk_import_edges_chunked_with_options(
            cell_id,
            edge_type,
            edges,
            idempotency_key,
            chunk_size,
            BulkImportOptions::default(),
        )
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
            .bulk_append_out_adjacency_segment_trusted_txn_locked(
                cell_id,
                edge_type,
                src,
                dsts,
                idempotency_key,
                fingerprint,
            )
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_append_out_adjacency_segment_trusted")
            .await?;
        let idem_key = keys::idempotency(cell_id, "segment-import", idempotency_key);
        if let Some(value) = read_txn_remote(&txn, &idem_key).await? {
            return decode_bulk_import_idempotency(&idem_key, idempotency_key, fingerprint, &value);
        }
        let current_epoch = txn.seqnum();
        let existing =
            out_neighbors_for_src_txn(&txn, cell_id, edge_type, src, current_epoch).await?;
        let inserted_dsts: Vec<_> = dsts
            .iter()
            .copied()
            .filter(|dst| !existing.contains(dst))
            .collect();
        let already_existed = u64::try_from(dsts.len().saturating_sub(inserted_dsts.len()))
            .map_err(|err| GraphError::CorruptValue {
                key: "segment_import".to_string(),
                reason: format!("too many existing edges in one segment import: {err}"),
            })?;
        let inserted =
            u64::try_from(inserted_dsts.len()).map_err(|err| GraphError::CorruptValue {
                key: "segment_import".to_string(),
                reason: format!("too many edges in one segment import: {err}"),
            })?;
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
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
            let changes: Vec<(VertexId, VertexId, bool)> =
                inserted_dsts.iter().map(|dst| (src, *dst, true)).collect();
            self.mark_topology_change_txn(&txn, cell_id, edge_type, end_epoch, &changes)
                .await?;
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
        let lock = self
            .acquire_local_write_guard(cell_id, "bulk_import_edges")
            .await?;
        let result = self
            .bulk_import_edges_txn_locked(
                cell_id,
                edge_type,
                edges,
                idempotency_key,
                fingerprint,
                options,
            )
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
        let txn = self
            .db
            .writer()?
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.validate_write_fence_txn(&txn, cell_id, "bulk_import_edges")
            .await?;
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
                        let neighbors = out_segment_neighbors_for_src_txn(
                            &txn,
                            cell_id,
                            edge_type,
                            src,
                            current_epoch,
                        )
                        .await?;
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

        let inserted =
            u64::try_from(inserted_edges.len()).map_err(|err| GraphError::CorruptValue {
                key: "bulk_import".to_string(),
                reason: format!("too many edges in one import: {err}"),
            })?;
        let start_epoch = if inserted == 0 {
            current_epoch
        } else {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
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
            txn.put(
                keys::out_edge(cell_id, edge_type, src, dst).as_bytes(),
                &edge_value,
            )?;
            if write_reverse_index {
                txn.put(
                    keys::in_edge(cell_id, edge_type, dst, src).as_bytes(),
                    &edge_value,
                )?;
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
            let base = if fresh_cell {
                0
            } else {
                read_counter_txn(&txn, &key).await?
            };
            txn.put(key.as_bytes(), encode_u64(base + increment))?;
        }
        if write_reverse_index {
            for (dst, increment) in in_increments {
                let key = keys::degree_in(cell_id, edge_type, dst);
                let base = if fresh_cell {
                    0
                } else {
                    read_counter_txn(&txn, &key).await?
                };
                txn.put(key.as_bytes(), encode_u64(base + increment))?;
            }
        }
        let counter_read_elapsed = counter_read_started.elapsed();
        if inserted > 0 {
            // The per-edge loop above already logged each inserted edge to the
            // xlog at this same epoch; this re-mark only refreshes the dirty
            // marker, so it carries no changes.
            self.mark_topology_change_txn(&txn, cell_id, edge_type, end_epoch, &[])
                .await?;
        }
        txn.put(
            idem_key.as_bytes(),
            encode_bulk_import_idempotency(idempotency_key, fingerprint, &result),
        )?;

        let commit_started = std::time::Instant::now();
        commit_txn_strict(txn, self.await_durable_writes).await?;
        let commit_elapsed = commit_started.elapsed();
        self.record_bulk_import_profile(
            preflight_elapsed,
            batch_build_elapsed,
            counter_read_elapsed,
            commit_elapsed,
        );
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
        || (items >= VERTEX_DELETE_LOCK_RENEW_ITEMS
            && items / VERTEX_DELETE_LOCK_RENEW_ITEMS * VERTEX_DELETE_LOCK_RENEW_ITEMS == items)
    {
        lock.renew().await?;
    }
    Ok(())
}

fn validate_vertex_metadata(metadata: &VertexMetadata) -> Result<()> {
    for label in &metadata.labels {
        validate_component("label", label)?;
    }
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}

fn validate_edge_metadata(metadata: &EdgeMetadata) -> Result<()> {
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
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
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "GraphQuery",
                        feature: format!(
                            "conflicting metadata values for vertex {vertex_id} property {property}"
                        ),
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

fn coalesce_edge_metadata_updates(
    updates: impl IntoIterator<Item = (VertexId, VertexId, EdgeMetadata)>,
) -> Result<Vec<(VertexId, VertexId, EdgeMetadata)>> {
    let mut by_edge = BTreeMap::<(VertexId, VertexId), EdgeMetadata>::new();
    for (src, dst, metadata) in updates {
        validate_edge_metadata(&metadata)?;
        match by_edge.get(&(src, dst)) {
            Some(existing) if existing != &metadata => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "GraphQuery",
                    feature: format!(
                        "conflicting metadata values for edge {src}->{dst} in one batch"
                    ),
                });
            }
            Some(_) => {}
            None => {
                by_edge.insert((src, dst), metadata);
            }
        }
    }
    Ok(by_edge
        .into_iter()
        .map(|((src, dst), metadata)| (src, dst, metadata))
        .collect())
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

fn validate_relationship_batch_entry(
    cell_id: &str,
    edge_type: &str,
    relationship: &RelationshipMutation,
) -> Result<()> {
    validate_component("cell_id", &relationship.cell_id)?;
    validate_component("edge_type", &relationship.edge_type)?;
    validate_edge_metadata(&relationship.metadata)?;
    if relationship.cell_id != cell_id {
        return Err(GraphError::CorruptValue {
            key: format!("cell/{cell_id}/relationship_import"),
            reason: format!(
                "batch contains relationship for different cell {}",
                relationship.cell_id
            ),
        });
    }
    if relationship.edge_type != edge_type {
        return Err(GraphError::CorruptValue {
            key: format!("cell/{cell_id}/relationship_import/{edge_type}"),
            reason: format!(
                "batch contains relationship for different edge type {}",
                relationship.edge_type
            ),
        });
    }
    Ok(())
}

fn coalesce_relationship_imports(
    cell_id: &str,
    edge_type: &str,
    relationships: impl IntoIterator<Item = RelationshipMutation>,
) -> Result<Vec<RelationshipMutation>> {
    let mut by_id = BTreeMap::<RelationshipId, RelationshipMutation>::new();
    for relationship in relationships {
        validate_relationship_batch_entry(cell_id, edge_type, &relationship)?;
        match by_id.get(&relationship.relationship_id) {
            Some(existing) if existing != &relationship => {
                return Err(GraphError::IdempotencyConflict {
                    operation: "relationship-import",
                    // Not the request key — this check runs before the request
                    // key is in scope, so the id that collided stands in for it.
                    // The reason says so, because a key shaped like a padded
                    // integer otherwise reads as a truncated request key.
                    idempotency_key: format!("{:020}", relationship.relationship_id),
                    reason: "the batch carries this relationship id twice with different \
                             endpoints or properties (key shown is the relationship id)",
                });
            }
            Some(_) => {}
            None => {
                by_id.insert(relationship.relationship_id, relationship);
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
    let requested_guard = requested
        .get(&policy.update_if_newer_by)
        .cloned()
        .ok_or_else(|| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Mutation,
            dialect: "QueryBatch",
            feature: format!(
                "guarded merge is missing incoming property {}",
                policy.update_if_newer_by
            ),
        })?;
    for property in &policy.create_only_properties {
        requested.remove(property);
    }
    let Some(previous_guard) = previous.get(&policy.update_if_newer_by) else {
        return Ok(Some(requested));
    };
    let ordering = match (previous_guard, &requested_guard) {
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Integer(right)) => {
            left.cmp(right)
        }
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::SignedInteger(right)) => {
            left.cmp(right)
        }
        (VertexPropertyValue::Float(left), VertexPropertyValue::Float(right)) => left.cmp(right),
        (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => left.cmp(right),
        _ => {
            return Err(GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!(
                    "guarded merge property {} must have matching ordered scalar types",
                    policy.update_if_newer_by
                ),
            });
        }
    };
    if ordering != std::cmp::Ordering::Less {
        return Ok(None);
    }
    Ok(Some(requested))
}

#[cfg(test)]
mod guarded_metadata_patch_tests {
    use super::*;

    fn policy() -> QueryBatchMergePolicy {
        QueryBatchMergePolicy {
            update_if_newer_by: "updated_at".to_string(),
            create_only_properties: BTreeSet::from(["created_at".to_string()]),
        }
    }

    #[test]
    fn rejects_an_older_patch() {
        let previous = BTreeMap::from([(
            "updated_at".to_string(),
            VertexPropertyValue::String("2026-08-03T11:00:00+00:00".to_string()),
        )]);
        let requested = BTreeMap::from([(
            "updated_at".to_string(),
            VertexPropertyValue::String("2026-08-03T10:00:00+00:00".to_string()),
        )]);

        assert!(
            guarded_metadata_patch(&previous, requested, &policy(), "vertex/1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn accepts_a_newer_patch_without_replacing_create_only_properties() {
        let previous = BTreeMap::from([
            (
                "created_at".to_string(),
                VertexPropertyValue::String("2026-08-03T09:00:00+00:00".to_string()),
            ),
            (
                "updated_at".to_string(),
                VertexPropertyValue::String("2026-08-03T10:00:00+00:00".to_string()),
            ),
        ]);
        let requested = BTreeMap::from([
            (
                "created_at".to_string(),
                VertexPropertyValue::String("2026-08-03T11:00:00+00:00".to_string()),
            ),
            (
                "updated_at".to_string(),
                VertexPropertyValue::String("2026-08-03T11:00:00+00:00".to_string()),
            ),
        ]);

        let patch = guarded_metadata_patch(&previous, requested, &policy(), "vertex/1")
            .unwrap()
            .unwrap();
        assert!(!patch.contains_key("created_at"));
        assert_eq!(
            patch.get("updated_at"),
            Some(&VertexPropertyValue::String(
                "2026-08-03T11:00:00+00:00".to_string()
            ))
        );
    }

    #[test]
    fn rejects_an_equal_version_patch() {
        let previous = BTreeMap::from([
            (
                "name".to_string(),
                VertexPropertyValue::String("current".to_string()),
            ),
            (
                "updated_at".to_string(),
                VertexPropertyValue::String("2026-08-03T10:00:00+00:00".to_string()),
            ),
        ]);
        let requested = BTreeMap::from([
            (
                "name".to_string(),
                VertexPropertyValue::String("conflicting replay".to_string()),
            ),
            (
                "updated_at".to_string(),
                VertexPropertyValue::String("2026-08-03T10:00:00+00:00".to_string()),
            ),
        ]);

        assert!(
            guarded_metadata_patch(&previous, requested, &policy(), "vertex/1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_a_guard_that_is_also_create_only() {
        let overlapping_policy = QueryBatchMergePolicy {
            update_if_newer_by: "updated_at".to_string(),
            create_only_properties: BTreeSet::from(["updated_at".to_string()]),
        };
        let previous = BTreeMap::from([(
            "updated_at".to_string(),
            VertexPropertyValue::String("2026-08-03T10:00:00+00:00".to_string()),
        )]);
        let requested = BTreeMap::from([(
            "updated_at".to_string(),
            VertexPropertyValue::String("2026-08-03T11:00:00+00:00".to_string()),
        )]);

        assert!(matches!(
            guarded_metadata_patch(&previous, requested, &overlapping_policy, "vertex/1"),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }

    #[test]
    fn rejects_a_patch_without_its_guard_even_when_the_record_has_no_guard() {
        let requested = BTreeMap::from([(
            "name".to_string(),
            VertexPropertyValue::String("new name".to_string()),
        )]);

        assert!(matches!(
            guarded_metadata_patch(&BTreeMap::new(), requested, &policy(), "vertex/1"),
            Err(GraphError::UnsupportedQuery { .. })
        ));
    }

    #[test]
    fn preserves_create_only_properties_when_an_existing_record_lacks_the_guard() {
        let previous = BTreeMap::from([(
            "created_at".to_string(),
            VertexPropertyValue::String("2026-08-03T09:00:00+00:00".to_string()),
        )]);
        let requested = BTreeMap::from([
            (
                "created_at".to_string(),
                VertexPropertyValue::String("2026-08-03T11:00:00+00:00".to_string()),
            ),
            (
                "updated_at".to_string(),
                VertexPropertyValue::String("2026-08-03T11:00:00+00:00".to_string()),
            ),
        ]);

        let patch = guarded_metadata_patch(&previous, requested, &policy(), "vertex/1")
            .unwrap()
            .unwrap();
        assert!(!patch.contains_key("created_at"));
        assert!(patch.contains_key("updated_at"));
    }
}

fn merge_edge_metadata(previous: &EdgeMetadata, requested: &EdgeMetadata) -> EdgeMetadata {
    let mut next = previous.clone();
    next.properties.extend(
        requested
            .properties
            .iter()
            .map(|(property, value)| (property.clone(), value.clone())),
    );
    next
}

fn apply_vertex_metadata_update_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    previous: &VertexMetadata,
    next: &VertexMetadata,
    _epoch: StorageSequence,
) -> Result<()> {
    validate_vertex_metadata(next)?;
    let vertex_key = keys::vertex(cell_id, vertex_id);
    delete_vertex_metadata_indexes_txn(txn, cell_id, vertex_id, previous)?;
    if next.labels.is_empty() && next.properties.is_empty() {
        txn.delete(vertex_key.as_bytes())?;
    } else {
        txn.put(
            vertex_key.as_bytes(),
            encode_vertex_metadata(next).as_slice(),
        )?;
        put_vertex_metadata_indexes_txn(txn, cell_id, vertex_id, next)?;
    }
    Ok(())
}

fn put_vertex_metadata_indexes_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    metadata: &VertexMetadata,
) -> Result<()> {
    for label in &metadata.labels {
        txn.put(
            keys::vertex_label(cell_id, label, vertex_id).as_bytes(),
            encode_u64(vertex_id).as_slice(),
        )?;
    }
    for (property, value) in &metadata.properties {
        txn.put(
            keys::vertex_property_index(
                cell_id,
                property,
                &encode_vertex_property_value_key(value),
                vertex_id,
            )
            .as_bytes(),
            encode_u64(vertex_id).as_slice(),
        )?;
    }
    Ok(())
}

fn delete_vertex_metadata_indexes_txn(
    txn: &DbTransaction,
    cell_id: &str,
    vertex_id: VertexId,
    metadata: &VertexMetadata,
) -> Result<()> {
    for label in &metadata.labels {
        txn.delete(keys::vertex_label(cell_id, label, vertex_id).as_bytes())?;
    }
    for (property, value) in &metadata.properties {
        txn.delete(
            keys::vertex_property_index(
                cell_id,
                property,
                &encode_vertex_property_value_key(value),
                vertex_id,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn apply_edge_metadata_update_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    previous: &EdgeMetadata,
    next: &EdgeMetadata,
    _epoch: StorageSequence,
) -> Result<()> {
    validate_edge_metadata(next)?;
    let edge_metadata_key =
        keys::edge_metadata(target.cell_id, target.edge_type, target.src, target.dst);
    delete_edge_metadata_indexes_txn(txn, target, previous)?;
    if next.properties.is_empty() {
        txn.delete(edge_metadata_key.as_bytes())?;
    } else {
        txn.put(
            edge_metadata_key.as_bytes(),
            encode_edge_metadata(next).as_slice(),
        )?;
        put_edge_metadata_indexes_txn(txn, target, next)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct EdgeMetadataTarget<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
}

fn put_edge_metadata_indexes_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    metadata: &EdgeMetadata,
) -> Result<()> {
    for (property, value) in &metadata.properties {
        txn.put(
            keys::edge_property_index(
                target.cell_id,
                target.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                target.src,
                target.dst,
            )
            .as_bytes(),
            encode_u64(target.dst).as_slice(),
        )?;
    }
    Ok(())
}

/// The property MERGE resolves relationship identity by. Not guaranteed
/// unique: parallel relationships between the same endpoints may share a
/// value, and MERGE matches all of them. The `rmerge_idx` pointer therefore
/// only exists while exactly one live row carries an identity — the MERGE
/// path writes it under that proof, and every other write that could mint a
/// duplicate invalidates it (see the index hooks below).
const RELATIONSHIP_IDENTITY_PROPERTY: &str = "id";

fn put_relationship_property_indexes_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
) -> Result<()> {
    for (property, value) in &record.metadata.properties {
        txn.put(
            keys::relationship_property_index(
                &record.cell_id,
                &record.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
            encode_u64(record.relationship_id).as_slice(),
        )?;
    }
    // Invalidate, never write: this hook runs on every path that puts a
    // relationship record — CREATE and plain imports included — any of which
    // may be minting a second row with the same identity value. A deleted
    // pointer costs the next MERGE one fallback scan, which re-earns it if
    // the identity is still unique; the MERGE path's deferred pointer writes
    // run after these hooks and overrule this delete inside its own
    // transaction.
    if let Some(value) = record
        .metadata
        .properties
        .get(RELATIONSHIP_IDENTITY_PROPERTY)
    {
        txn.delete(
            keys::relationship_merge_index(
                &record.cell_id,
                &record.edge_type,
                RELATIONSHIP_IDENTITY_PROPERTY,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

async fn read_txn_remote_many(
    txn: &DbTransaction,
    keys: impl IntoIterator<Item = String>,
) -> Result<BTreeMap<String, Option<Bytes>>> {
    let keys = keys.into_iter().collect::<BTreeSet<_>>();
    let options = relationship_import_read_options(keys.len());
    read_txn_remote_many_with_options(txn, keys, &options).await
}

async fn read_txn_remote_many_with_options(
    txn: &DbTransaction,
    keys: BTreeSet<String>,
    options: &ReadOptions,
) -> Result<BTreeMap<String, Option<Bytes>>> {
    // Small point-read sets repeat across adjacent relationship batches and
    // are safe to admit to the process-wide bounded cache. Large sets remain
    // uncached so migration traffic cannot replace the complete hot working
    // set with one-use blocks. mark_read still preserves serializable conflict
    // detection in either case.
    stream::iter(keys.into_iter().map(|key| async move {
        let value = read_txn_remote_with_options(txn, &key, options).await?;
        Ok::<_, GraphError>((key, value))
    }))
    .buffer_unordered(RELATIONSHIP_IMPORT_READ_CONCURRENCY)
    .try_collect()
    .await
}

async fn read_txn_remote_uncached(txn: &DbTransaction, key: &str) -> Result<Option<Bytes>> {
    read_txn_remote_with_options(txn, key, &relationship_import_uncached_read_options()).await
}

async fn read_txn_remote_with_options(
    txn: &DbTransaction,
    key: &str,
    options: &ReadOptions,
) -> Result<Option<Bytes>> {
    txn.mark_read([key.as_bytes()])?;
    Ok(txn.get_with_options(key.as_bytes(), options).await?)
}

fn relationship_import_read_options(expected_items: usize) -> ReadOptions {
    remote_read_options_for_expected_items(expected_items as u64)
}

fn relationship_import_uncached_read_options() -> ReadOptions {
    remote_read_options_for_expected_items(u64::MAX)
}

#[cfg(feature = "opencypher")]
fn relationship_import_scan_options() -> ScanOptions {
    ScanOptions::default().with_cache_blocks(false)
}

#[cfg(test)]
mod relationship_import_cache_tests {
    use super::*;

    #[test]
    fn bounded_point_reads_use_the_shared_slatedb_block_cache() {
        let read = relationship_import_read_options(1_024);
        assert_eq!(read.durability_filter, DurabilityLevel::Remote);
        assert!(read.cache_blocks);
    }

    #[test]
    fn high_cardinality_reads_do_not_enter_the_slatedb_block_cache() {
        let read = relationship_import_read_options(1_025);
        assert_eq!(read.durability_filter, DurabilityLevel::Remote);
        assert!(!read.cache_blocks);
        assert!(!relationship_import_uncached_read_options().cache_blocks);
        #[cfg(feature = "opencypher")]
        assert!(!relationship_import_scan_options().cache_blocks);
    }
}

fn counter_value(
    values: &BTreeMap<String, Option<Bytes>>,
    key: &str,
    fresh_cell: bool,
) -> Result<u64> {
    if fresh_cell {
        return Ok(0);
    }
    match values.get(key) {
        Some(Some(value)) => decode_u64(key, value),
        Some(None) => Ok(0),
        None => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "batched counter read did not include the requested key".to_string(),
        }),
    }
}

async fn next_available_relationship_ids_txn(
    txn: &DbTransaction,
    cell_id: &str,
    cursor: &mut RelationshipId,
    count: usize,
    operation: &str,
) -> Result<Vec<RelationshipId>> {
    let mut available = Vec::with_capacity(count);
    while available.len() < count {
        let remaining = count - available.len();
        let mut candidates = Vec::with_capacity(remaining);
        for _ in 0..remaining {
            *cursor = cursor
                .checked_add(1)
                .ok_or_else(|| GraphError::CorruptValue {
                    key: keys::last_relationship_id(cell_id),
                    reason: format!("relationship id overflow during {operation}"),
                })?;
            candidates.push(*cursor);
        }
        let existing = read_txn_remote_many(
            txn,
            candidates
                .iter()
                .map(|relationship_id| keys::relationship_id(cell_id, *relationship_id)),
        )
        .await?;
        for relationship_id in candidates {
            let key = keys::relationship_id(cell_id, relationship_id);
            if existing.get(&key).and_then(Option::as_ref).is_none() {
                available.push(relationship_id);
            }
        }
    }
    Ok(available)
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
    let encoded = encode_vertex_property_value_key(value);
    let prefix = keys::relationship_property_index_edge_prefix(
        cell_id, edge_type, property, &encoded, src, dst,
    );
    let scan_options = relationship_import_scan_options();
    let mut iter = txn
        .scan_prefix_with_options(prefix.as_bytes(), .., &scan_options)
        .await?;
    let mut relationship_keys = BTreeMap::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let (
            parsed_cell_id,
            parsed_edge_type,
            parsed_property,
            parsed_encoded,
            parsed_src,
            parsed_dst,
            relationship_id,
        ) = parse_relationship_property_index_key(&key)?;
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
        relationship_keys.insert(
            relationship_id,
            keys::relationship(cell_id, edge_type, src, dst, relationship_id),
        );
    }
    let record_values = read_txn_remote_many(txn, relationship_keys.values().cloned()).await?;
    let mut relationships = Vec::with_capacity(relationship_keys.len());
    for (relationship_id, record_key) in relationship_keys {
        let Some(record_value) = record_values.get(&record_key).and_then(Option::as_ref) else {
            return Err(GraphError::CorruptValue {
                key: keys::relationship_property_index(
                    cell_id,
                    edge_type,
                    property,
                    &encoded,
                    src,
                    dst,
                    relationship_id,
                ),
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
async fn reserve_edge_delete_noops_txn(
    txn: &DbTransaction,
    cell_id: &str,
    mutations: &[EdgeMutation],
) -> Result<()> {
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

async fn live_relationships_for_edge_txn(
    txn: &DbTransaction,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    _read_epoch: StorageSequence,
) -> Result<Vec<RelationshipRecord>> {
    let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
    let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut records = Vec::new();
    while let Some(kv) = iter.next().await? {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        records.push(decode_relationship_record(&key, &kv.value)?);
    }
    Ok(records)
}

async fn delete_structural_edge_txn(
    shard: &GraphShard,
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    epoch: StorageSequence,
) -> Result<bool> {
    let edge_key = keys::out_edge(
        &mutation.cell_id,
        &mutation.edge_type,
        mutation.src,
        mutation.dst,
    );
    if read_txn_remote(txn, &edge_key).await?.is_none() {
        return Ok(false);
    }
    shard
        .mark_topology_change_txn(
            txn,
            &mutation.cell_id,
            &mutation.edge_type,
            epoch,
            &[(mutation.src, mutation.dst, false)],
        )
        .await?;
    let out_degree_key = keys::degree_out(&mutation.cell_id, &mutation.edge_type, mutation.src);
    let out_degree = read_counter_txn(txn, &out_degree_key)
        .await?
        .saturating_sub(1);
    let in_degree = if shard.writes_reverse_index() {
        let in_degree_key = keys::degree_in(&mutation.cell_id, &mutation.edge_type, mutation.dst);
        let in_degree = read_counter_txn(txn, &in_degree_key)
            .await?
            .saturating_sub(1);
        Some((in_degree_key, in_degree))
    } else {
        None
    };
    let edge_metadata_key = keys::edge_metadata(
        &mutation.cell_id,
        &mutation.edge_type,
        mutation.src,
        mutation.dst,
    );
    let previous_edge_metadata = match read_txn_remote(txn, &edge_metadata_key).await? {
        Some(value) => decode_edge_metadata(&edge_metadata_key, &value)?,
        None => EdgeMetadata::default(),
    };
    if !previous_edge_metadata.properties.is_empty() {
        apply_edge_metadata_update_txn(
            txn,
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
    txn.delete(
        keys::edge(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        )
        .as_bytes(),
    )?;
    txn.delete(edge_key.as_bytes())?;
    if shard.writes_reverse_index() {
        txn.delete(
            keys::in_edge(
                &mutation.cell_id,
                &mutation.edge_type,
                mutation.dst,
                mutation.src,
            )
            .as_bytes(),
        )?;
    }
    txn.put(out_degree_key.as_bytes(), encode_u64(out_degree))?;
    if let Some((in_degree_key, in_degree)) = in_degree {
        txn.put(in_degree_key.as_bytes(), encode_u64(in_degree))?;
    }
    Ok(true)
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
        let span = tracing::info_span!(
            "write.index_update",
            hydradb.cell_id = %cell_id,
            hydradb.edge_type = %edge_type,
            hydradb.commit_epoch = epoch,
            hydradb.xlog_changes = changes.len() as u64,
        );
        span.in_scope(|| {
            txn.put(
                keys::matrix_dirty(cell_id, edge_type).as_bytes(),
                encode_u64(epoch),
            )?;
            txn.put(
                keys::adjacency_generation(cell_id, edge_type).as_bytes(),
                encode_u64(epoch),
            )
        })?;
        if changes.is_empty() {
            return Ok(());
        }
        // Coverage floor: the first-ever logged change for the pair sets the
        // low-water mark to its own epoch, so the builder knows coverage
        // begins here. The in-memory set only caches *confirmed presence* —
        // a pending put is never cached, so a rolled-back transaction cannot
        // strand the floor.
        let floor_token = format!("{cell_id}/{edge_type}");
        let ensured = self
            .xlog_floor_ensured
            .read()
            .expect("xlog floor cache poisoned")
            .contains(&floor_token);
        if !ensured {
            let low_key = keys::xlog_low_water(cell_id, edge_type);
            match read_txn_remote(txn, &low_key).await? {
                Some(_) if crate::shard::write_pipeline::current().is_none() => {
                    self.xlog_floor_ensured
                        .write()
                        .expect("xlog floor cache poisoned")
                        .insert(floor_token);
                }
                Some(_) => {}
                None => {
                    txn.put(low_key.as_bytes(), encode_u64(epoch))?;
                }
            }
        }
        span.in_scope(|| {
            for (src, dst, exists) in changes {
                txn.put(
                    keys::xlog_entry(cell_id, edge_type, epoch, *src, *dst).as_bytes(),
                    if *exists { &[1u8][..] } else { &[0u8][..] },
                )?;
            }
            Ok(())
        })
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
    let read_relationships = async {
        let prefix = keys::relationship_edge_prefix(cell_id, &edge.edge_type, edge.src, edge.dst);
        let mut iter = txn.scan_prefix(prefix.as_bytes(), ..).await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            // One shared allowance covers all concurrently prepared edges, not
            // a fresh record budget for every edge or prefetch window.
            let count = relationship_reads
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            ensure_limit(
                "delete_vertex_relationship_records",
                count,
                relationship_limit as u64,
            )?;
            let key = String::from_utf8_lossy(&kv.key);
            records.push(decode_relationship_record(&key, &kv.value)?);
        }
        Ok::<_, GraphError>(records)
    };
    let (canonical, metadata, relationships) = tokio::try_join!(
        read_txn_remote(txn, &out_key),
        read_txn_remote(txn, &metadata_key),
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

async fn delete_relationships_for_structural_edge_txn(
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    read_epoch: StorageSequence,
) -> Result<u64> {
    let relationships = live_relationships_for_edge_txn(
        txn,
        &mutation.cell_id,
        &mutation.edge_type,
        mutation.src,
        mutation.dst,
        read_epoch,
    )
    .await?;
    delete_prepared_relationships_for_structural_edge_txn(txn, mutation, &relationships)
}

fn delete_prepared_relationships_for_structural_edge_txn(
    txn: &DbTransaction,
    mutation: &EdgeMutation,
    relationships: &[RelationshipRecord],
) -> Result<u64> {
    for record in relationships {
        txn.delete(
            keys::relationship(
                &record.cell_id,
                &record.edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
        )?;
        txn.delete(keys::relationship_id(&record.cell_id, record.relationship_id).as_bytes())?;
        delete_relationship_property_indexes_txn(txn, record, &record.metadata)?;
    }
    txn.delete(
        keys::relationship_count(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        )
        .as_bytes(),
    )?;
    u64::try_from(relationships.len()).map_err(|err| GraphError::CorruptValue {
        key: keys::relationship_edge_prefix(
            &mutation.cell_id,
            &mutation.edge_type,
            mutation.src,
            mutation.dst,
        ),
        reason: format!("too many relationships to delete with structural edge: {err}"),
    })
}

fn delete_relationship_property_indexes_txn(
    txn: &DbTransaction,
    record: &RelationshipRecord,
    metadata: &EdgeMetadata,
) -> Result<()> {
    for (property, value) in &metadata.properties {
        txn.delete(
            keys::relationship_property_index(
                &record.cell_id,
                &record.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
                record.relationship_id,
            )
            .as_bytes(),
        )?;
    }
    // A blind delete: the pointer may name a different relationship if two
    // live rows ever shared an identity, but a missing pointer only costs the
    // next MERGE one fallback scan, which rewrites it (heal-on-read). A wrong
    // surviving pointer is the outcome this avoids.
    if let Some(value) = metadata.properties.get(RELATIONSHIP_IDENTITY_PROPERTY) {
        txn.delete(
            keys::relationship_merge_index(
                &record.cell_id,
                &record.edge_type,
                RELATIONSHIP_IDENTITY_PROPERTY,
                &encode_vertex_property_value_key(value),
                record.src,
                record.dst,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn delete_edge_metadata_indexes_txn(
    txn: &DbTransaction,
    target: EdgeMetadataTarget<'_>,
    metadata: &EdgeMetadata,
) -> Result<()> {
    for (property, value) in &metadata.properties {
        txn.delete(
            keys::edge_property_index(
                target.cell_id,
                target.edge_type,
                property,
                &encode_vertex_property_value_key(value),
                target.src,
                target.dst,
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}
