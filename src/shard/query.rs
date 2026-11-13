use super::topology_tail::{expand_range_with_overlay, GraphTopologyOverlay, GraphTopologyTail};
use super::*;
use crate::QueryFailureReason;
#[cfg(feature = "opencypher")]
use futures::{stream, StreamExt as _, TryStreamExt as _};
#[cfg(feature = "opencypher")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "opencypher")]
use std::time::Instant;
use tracing::Instrument as _;

#[cfg(feature = "opencypher")]
use crate::core::metrics::CypherRoute;
#[cfg(feature = "opencypher")]
use crate::query::opencypher::RowInList;

/// Carries one scan allowance across outgoing readers and frontier vertices.
pub(super) struct StorageNeighborScanBudget<'a> {
    pub query: &'a QueryBudget,
    pub scanned_records: &'a mut u64,
    pub operation: &'static str,
}

impl StorageNeighborScanBudget<'_> {
    fn charge(&mut self, limit: u64) -> Result<()> {
        *self.scanned_records = self.scanned_records.saturating_add(1);
        ensure_limit(self.operation, *self.scanned_records, limit)
    }
}

// `pub(super)` so the experimental ordered walks, which hydrate in the same
// bounded batches, share one definition with the legacy scans. The
// `experimental-cypher-engine` feature implies `opencypher`, so one gate
// covers both users.
#[cfg(feature = "opencypher")]
pub(super) const QUERY_METADATA_HYDRATION_CONCURRENCY: usize = 16;

/// Smallest batch of candidate records a bounded, unanchored node scan reads
/// at once. A `LIMIT` smaller than this still reads one batch, so a selective
/// `WHERE` costs a few batched reads rather than a round trip per candidate.
#[cfg(feature = "opencypher")]
const LIMITED_NODE_SCAN_BATCH: usize = 256;

#[cfg(feature = "opencypher")]
pub(super) const ORDERED_RELATIONSHIP_FETCH_BATCH: usize = 128;
#[cfg(feature = "opencypher")]
const ORDERED_RELATIONSHIP_FETCH_CONCURRENCY: usize = 32;

#[cfg(feature = "opencypher")]
pub(super) struct IndexedRelationshipBatch<'a> {
    pub cell_id: &'a str,
    pub edge_type: &'a str,
    pub property: &'a str,
    pub candidates: &'a [(VertexId, VertexId, RelationshipId, String)],
    pub read_epoch: StorageSequence,
    pub inherit_structural: bool,
    pub budget: &'a QueryBudget,
    pub memo: &'a mut IndexedRelationshipPairMemo,
}

/// What a relationship property equality reads from its two indexes: the
/// endpoint pairs the structural index names (their properties may be
/// inherited by every relationship on the pair), and each relationship-index
/// entry as (src, dst, relationship id, encoded value).
#[cfg(feature = "experimental-cypher-engine")]
#[derive(Debug, Default)]
pub(super) struct RelationshipPropertyEntries {
    pub structural_pairs: BTreeSet<(VertexId, VertexId)>,
    pub identified: Vec<(VertexId, VertexId, RelationshipId, String)>,
}

/// Cache only liveness and the absence of structural properties. The entry
/// bound is also a memory bound: no arbitrary user property blobs are retained.
#[cfg(feature = "opencypher")]
#[derive(Default)]
pub(super) struct IndexedRelationshipPairMemo {
    scope: Option<(String, String, StorageSequence, bool)>,
    pairs: BTreeMap<(VertexId, VertexId), bool>,
}

#[cfg(feature = "opencypher")]
pub(crate) const QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES: usize = 512;

/// How many equality candidates one runtime access-path probe ranks, and so
/// how many index walks it runs at once.
///
/// The walks are concurrent and `QueryBudget::read_only_io` bounds time and
/// cancellation, not parallel I/O, so without this a wide inline property map
/// would open one object-store scan per property. The cap is also what keeps
/// the probe useful: every candidate divides one fixed candidate allowance, so
/// past a handful each slice is too small to prove anything and every walk
/// declines after paying for itself.
///
/// Candidates past the cap are dropped in predicate order. That is the
/// ordering this probe exists to stop trusting, but a vertex pattern pinned by
/// more than this many equalities is not a shape worth widening the fan-out
/// for.
#[cfg(feature = "opencypher")]
pub(crate) const QUERY_PROPERTY_PROBE_CANDIDATES: usize = 8;

#[cfg(feature = "opencypher")]
const ORDERED_PROPERTY_INDEX_TYPE_PREFIXES: [&str; 5] = ["b", "i", "j", "n", "s"];

/// Sparse bitmap of edge endpoints returned by a bounded property-index probe.
/// Each source owns 64-target words, so structural and identified index rows
/// for the same edge collapse before metadata is hydrated.
#[cfg(feature = "experimental-cypher-engine")]
#[derive(Default)]
struct EdgeCandidateBitmap(BTreeMap<VertexId, BTreeMap<u64, u64>>);

#[cfg(feature = "experimental-cypher-engine")]
impl EdgeCandidateBitmap {
    fn insert(&mut self, source: VertexId, target: VertexId) {
        *self.0.entry(source).or_default().entry(target / 64).or_default() |= 1_u64 << (target % 64);
    }

    fn into_pairs(self) -> Vec<(VertexId, VertexId)> {
        let mut pairs = Vec::new();
        for (source, words) in self.0 {
            for (word, mut bits) in words {
                while bits != 0 {
                    let bit = bits.trailing_zeros() as u64;
                    pairs.push((source, word * 64 + bit));
                    bits &= bits - 1;
                }
            }
        }
        pairs
    }
}

#[cfg(feature = "experimental-cypher-engine")]
pub(crate) struct RelationshipLowerBoundProbe<'a> {
    pub cell_id: &'a str,
    pub edge_type: &'a str,
    pub property: &'a str,
    pub lower: i128,
    pub read_epoch: StorageSequence,
    pub budget: &'a QueryBudget,
    pub max_entries: usize,
}

#[cfg(feature = "experimental-cypher-engine")]
pub(crate) struct RelationshipTypeProbe<'a> {
    pub cell_id: &'a str,
    pub edge_type: &'a str,
    pub read_epoch: StorageSequence,
    pub budget: &'a QueryBudget,
    pub max_entries: usize,
}

pub(super) async fn run_graph_compute<T, F>(metrics: Arc<GraphOperationalMetrics>, operation: &'static str, compute: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    // `kernel.expand` covers the queue wait as well as the compute, because
    // `spawn_blocking` can sit behind a saturated blocking pool for longer than
    // the traversal itself takes and the aggregate counters cannot tell the two
    // apart on a single query. The span cannot follow the closure onto the
    // blocking thread, which is exactly why the wrapper is the right place for
    // it. `operation` is the rung of the sparse-kernel ladder that ran.
    let span = tracing::info_span!("kernel.expand", hydradb.kernel = operation);
    let queued_at = std::time::Instant::now();
    tokio::task::spawn_blocking(move || {
        let queue_us = queued_at.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
        metrics.graph_compute_tasks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics
            .graph_compute_queue_us
            .fetch_add(queue_us, std::sync::atomic::Ordering::Relaxed);
        let compute_started = std::time::Instant::now();
        let result = compute();
        metrics.graph_compute_duration_us.fetch_add(
            compute_started.elapsed().as_micros().try_into().unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        result
    })
    .instrument(span)
    .await
    .map_err(|err| GraphError::CorruptValue {
        key: format!("query/compute/{operation}"),
        reason: format!("graph compute task failed: {err}"),
    })?
}

#[cfg_attr(not(feature = "opencypher"), allow(dead_code))]
fn run_graph_compute_inline<T, F>(metrics: Arc<GraphOperationalMetrics>, compute: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    metrics.graph_compute_tasks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let compute_started = std::time::Instant::now();
    let result = compute();
    metrics.graph_compute_duration_us.fetch_add(
        compute_started.elapsed().as_micros().try_into().unwrap_or(u64::MAX),
        std::sync::atomic::Ordering::Relaxed,
    );
    result
}

/// Parse `query` as a mutation for the experimental route, or answer `None`
/// when it is a read.
///
/// The route peels mutations off before dispatching, because the new engine has
/// no mutation operators. Asking the mutation parser directly is the wrong
/// question: it answers `Err` for a *read* whose shape it cannot lower, and
/// that error escaped as the request's own failure -- so
/// `MATCH ... WITH u MATCH ...` reached the client classed as a mutation
/// failure, although the legacy route answers it and the new engine lowers it.
/// Filed under `unsupported_mutation`, it was close to undiagnosable.
///
/// Access classification is the right question, and it walks clause kinds
/// rather than trying to lower anything. A read goes straight to the engine; a
/// query that cannot be classified keeps the old behaviour, so nothing that
/// used to reach the mutation parser stops reaching it.
#[cfg(feature = "opencypher")]
fn parse_experimental_route_mutation(query: &str, context: &QueryContext) -> Result<Option<ParsedMutationQuery>> {
    use crate::query::opencypher::{classify_opencypher_query_access, OpenCypherQueryAccess};

    if matches!(classify_opencypher_query_access(query), Ok(OpenCypherQueryAccess::Read)) {
        return Ok(None);
    }
    parse_opencypher_mutation_query_with_list_parameters(query, &context.parameters, &context.list_parameters)
}

impl GraphShard {
    pub(crate) async fn uses_id_properties(&self) -> Result<bool> {
        self.cypher_id_properties
            .get_or_try_init(|| async {
                let snapshot = self.storage.snapshot().await?;
                match snapshot
                    .get_with_options(keys::CYPHER_ID_SEMANTICS.as_bytes(), &remote_read_options())
                    .await?
                {
                    None => Ok(false),
                    Some(value) if value.as_ref() == keys::CYPHER_ID_PROPERTIES_V1 => Ok(true),
                    Some(_) => Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::InvalidRequest,
                        dialect: "Cypher",
                        feature: "unrecognized collection id semantics".to_owned(),
                    }),
                }
            })
            .await
            .copied()
    }

    async fn ensure_cypher_layout(&self, context: &QueryContext) -> Result<()> {
        if self.uses_id_properties().await? && context.cypher_engine != CypherEngineMode::Experimental {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "Cypher",
                feature: "BYOG collections require the experimental Cypher engine for property id semantics".to_owned(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn ensure_managed_cypher_layout(&self) -> Result<()> {
        if self.uses_id_properties().await? {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "Cypher",
                feature: "BYOG collections do not support the managed Cypher fallback".to_owned(),
            });
        }
        Ok(())
    }

    pub(crate) async fn edge_exists_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        topology_sequence: StorageSequence,
    ) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::out_edge(cell_id, edge_type, src, dst);
        if snapshot.get_with_options(key.as_bytes(), &remote_read_options()).await?.is_some() {
            return Ok(true);
        }

        let tombstone_key = keys::out_segment_tombstone(cell_id, edge_type, src, dst);
        let tombstone_epoch = match snapshot.get_with_options(tombstone_key.as_bytes(), &remote_read_options()).await? {
            Some(value) => {
                let epoch = decode_u64(&tombstone_key, &value)?;
                (epoch <= topology_sequence).then_some(epoch)
            }
            None => None,
        };
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > topology_sequence {
                break;
            }
            if segment
                .destinations
                .iter()
                .copied()
                .any(|candidate| candidate == dst && segment_edge_visible(segment.storage_sequence, tombstone_epoch))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) async fn out_neighbors_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        topology_sequence: StorageSequence,
    ) -> Result<Vec<VertexId>> {
        self.out_neighbors_in_storage_snapshot_inner(snapshot, cell_id, edge_type, src, topology_sequence, None)
            .await
    }

    #[cfg(feature = "opencypher")]
    #[allow(dead_code)] // Used only when `experimental-cypher-engine` is also enabled.
    pub(crate) async fn out_neighbors_in_storage_snapshot_for_query(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        topology_sequence: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        let mut scanned_records = 0;
        self.out_neighbors_in_storage_snapshot_inner(
            snapshot,
            cell_id,
            edge_type,
            src,
            topology_sequence,
            Some(StorageNeighborScanBudget {
                query: budget,
                scanned_records: &mut scanned_records,
                operation: "query_out_neighbors_storage_records",
            }),
        )
        .await
    }

    /// The canonical adjacency keys end in zero-padded numeric target order,
    /// including in existing SSTs. Segment-backed sources also have destinations
    /// outside this range and must use the ordinary segment/tombstone merge.
    #[cfg(feature = "experimental-cypher-engine")]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn out_neighbor_page(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        after: Option<VertexId>,
        size: usize,
        budget: &QueryBudget,
    ) -> Result<Option<Vec<VertexId>>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        budget
            .read_only_io("query_out_neighbor_page", async {
                if after.is_none() {
                    let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
                    let mut segments = snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options_for_expected_items(1))
                        .await?;
                    if segments.next().await?.is_some() {
                        return Ok(None);
                    }
                }
                // A selective predicate can exhaust a hub across many small pages.
                // Use the same cache admission as the full outgoing adjacency scan.
                let options = remote_scan_options_for_expected_items(
                    self.degree_for_cache_admission(snapshot, &keys::degree_out(cell_id, edge_type, src))
                        .await?,
                );
                let prefix = keys::out_prefix(cell_id, edge_type, src);
                let start = after
                    .map(|id| std::ops::Bound::Excluded(format!("{id:020}").into_bytes()))
                    .unwrap_or(std::ops::Bound::Unbounded);
                let mut edges = snapshot
                    .scan_prefix_with_options(prefix.as_bytes(), (start, std::ops::Bound::Unbounded), &options)
                    .await?;
                let mut page = Vec::new();
                while page.len() < size {
                    budget.check("query_out_neighbor_page")?;
                    let Some(kv) = edges.next().await? else { break };
                    let key = String::from_utf8_lossy(&kv.key);
                    let dst = decode_edge_record(&key, &kv.value)?.dst;
                    if !key.strip_prefix(&prefix).is_some_and(|suffix| suffix == format!("{dst:020}")) {
                        return Err(GraphError::CorruptValue {
                            key: key.into_owned(),
                            reason: "outgoing adjacency key and target disagree".to_owned(),
                        });
                    }
                    page.push(dst);
                }
                Ok(Some(page))
            })
            .await
    }

    pub(super) async fn out_neighbors_in_storage_snapshot_inner(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        topology_sequence: StorageSequence,
        scan_budget: Option<StorageNeighborScanBudget<'_>>,
    ) -> Result<Vec<VertexId>> {
        let budget = scan_budget.as_ref().map(|scan| scan.query);
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        check_optional_query_budget(budget, "query_out_neighbors_storage_scan")?;
        // The edge keys, the segment tombstones and the segments are three
        // independent ranges; only the edge scan's cache admission waits for
        // the degree. Read side by side, a listing is two round trips deep
        // rather than four. Every record still charges the scan allowance as
        // it is read, so a hub stops at the limit wherever it is reached.
        let scan_budget = std::sync::Mutex::new(scan_budget);
        let charge = || -> Result<()> {
            if let Some(scan) = scan_budget.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_mut() {
                scan.charge(self.limits.max_query_scan_edges)?;
            }
            Ok(())
        };
        let edges = async {
            let scan_options = remote_scan_options_for_expected_items(
                self.degree_for_cache_admission(snapshot, &keys::degree_out(cell_id, edge_type, src))
                    .await?,
            );
            check_optional_query_budget(budget, "query_out_neighbors_storage_scan")?;
            let prefix = keys::out_prefix(cell_id, edge_type, src);
            let mut iter = snapshot.scan_prefix_with_options(prefix.as_bytes(), .., &scan_options).await?;
            let mut neighbors = BTreeSet::new();
            loop {
                check_optional_query_budget(budget, "query_out_neighbors_edge_scan")?;
                let Some(kv) = iter.next().await? else {
                    break;
                };
                check_optional_query_budget(budget, "query_out_neighbors_edge_scan")?;
                charge()?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                neighbors.insert(decode_edge_record(&key, &kv.value)?.dst);
                self.ensure_optional_query_intermediate_limit(budget, "query_out_neighbors_vertices", neighbors.len())?;
            }
            Ok::<_, GraphError>(neighbors)
        };
        let tombstones = async {
            let tombstone_prefix = keys::out_segment_tombstone_src_prefix(cell_id, edge_type, src);
            let mut tombstone_iter = snapshot
                .scan_prefix_with_options(tombstone_prefix.as_bytes(), .., &remote_scan_options())
                .await?;
            let mut tombstones = BTreeMap::new();
            loop {
                check_optional_query_budget(budget, "query_out_neighbors_tombstone_scan")?;
                let Some(kv) = tombstone_iter.next().await? else {
                    break;
                };
                check_optional_query_budget(budget, "query_out_neighbors_tombstone_scan")?;
                charge()?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let (key_cell_id, key_edge_type, key_src, dst) = parse_out_edge_segment_tombstone_key(&key)?;
                if key_cell_id != cell_id || key_edge_type != edge_type || key_src != src {
                    return Err(GraphError::CorruptValue {
                        key,
                        reason: "segment tombstone identity does not match snapshot prefix".to_string(),
                    });
                }
                let epoch = decode_u64(&key, &kv.value)?;
                if epoch <= topology_sequence {
                    tombstones.insert(dst, epoch);
                    self.ensure_optional_query_intermediate_limit(budget, "query_out_neighbors_tombstones", tombstones.len())?;
                }
            }
            Ok::<_, GraphError>(tombstones)
        };
        // Segment destinations are judged against the tombstones once both are
        // read. A destination is visible when any segment holding it is newer
        // than its tombstone, which is when its newest one is, so only that is
        // kept: the destinations, not the segments, are held. Each is charged
        // to the scan allowance as it is read, which bounds what is held; the
        // row limit applies only to the visible neighbours, as before, so a
        // deleted destination never counts against it.
        let segments = async {
            let segment_prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
            let mut segment_iter = snapshot
                .scan_prefix_with_options(segment_prefix.as_bytes(), .., &remote_scan_options())
                .await?;
            let mut newest = BTreeMap::<VertexId, StorageSequence>::new();
            loop {
                check_optional_query_budget(budget, "query_out_neighbors_segment_scan")?;
                let Some(kv) = segment_iter.next().await? else {
                    break;
                };
                check_optional_query_budget(budget, "query_out_neighbors_segment_scan")?;
                charge()?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let segment = decode_out_edge_segment(&key, &kv.value)?;
                if segment.storage_sequence > topology_sequence {
                    break;
                }
                for dst in segment.destinations.iter().copied() {
                    check_optional_query_budget(budget, "query_out_neighbors_segment_edges")?;
                    charge()?;
                    let sequence = newest.entry(dst).or_insert(segment.storage_sequence);
                    *sequence = (*sequence).max(segment.storage_sequence);
                }
            }
            Ok::<_, GraphError>(newest)
        };
        let (mut neighbors, tombstones, segment_destinations) = futures::try_join!(edges, tombstones, segments)?;
        for (dst, sequence) in segment_destinations {
            if segment_edge_visible(sequence, tombstones.get(&dst).copied()) {
                neighbors.insert(dst);
                self.ensure_optional_query_intermediate_limit(budget, "query_out_neighbors_vertices", neighbors.len())?;
            }
        }
        Ok(neighbors.into_iter().collect())
    }

    fn ensure_optional_query_intermediate_limit(&self, budget: Option<&QueryBudget>, operation: &'static str, rows: usize) -> Result<()> {
        if budget.is_some() {
            ensure_limit(operation, rows as u64, self.limits.max_query_intermediate_rows as u64)?;
        }
        Ok(())
    }

    pub(crate) async fn out_degree_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::degree_out(cell_id, edge_type, src);
        match snapshot.get_with_options(key.as_bytes(), &remote_read_options()).await? {
            Some(value) => decode_u64(&key, &value),
            None => Ok(0),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn in_degree_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::degree_in(cell_id, edge_type, dst);
        match snapshot.get_with_options(key.as_bytes(), &remote_read_options()).await? {
            Some(value) => decode_u64(&key, &value),
            None => Ok(0),
        }
    }

    pub(crate) async fn in_neighbors_in_storage_snapshot(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let scan_options = remote_scan_options_for_expected_items(
            self.degree_for_cache_admission(snapshot, &keys::degree_in(cell_id, edge_type, dst))
                .await?,
        );
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = snapshot.scan_prefix_with_options(prefix.as_bytes(), .., &scan_options).await?;
        let mut neighbors = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            neighbors.insert(decode_edge_record(&key, &kv.value)?.src);
        }
        Ok(neighbors.into_iter().collect())
    }

    async fn degree_for_cache_admission(&self, snapshot: &GraphStorageSnapshot, key: &str) -> Result<u64> {
        match snapshot.get_with_options(key.as_bytes(), &remote_read_options()).await? {
            Some(value) => decode_u64(key, &value),
            // Missing counters must fail closed for cache admission. The scan
            // remains correct; it simply cannot displace known hot blocks.
            None => Ok(u64::MAX),
        }
    }

    pub async fn execute_cypher(&self, context: QueryContext, query: &str) -> Result<QueryOutput> {
        self.ensure_cypher_layout(&context).await?;
        #[cfg(feature = "datafusion-cypher-engine")]
        if context.cypher_engine == CypherEngineMode::DataFusion {
            let shard = self.shared_datafusion_self()?;
            return shard.execute_datafusion_cypher(context, query).await;
        }
        if context.cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "opencypher")]
            {
                if let Some(procedure) =
                    crate::query::path_procedure::parse_native_procedure(query, &context.parameters, self.limits.max_traversal_hops)?
                {
                    self.ensure_managed_cypher_layout().await?;
                    self.operation_metrics.record_cypher_route(CypherRoute::NativePathFallback);
                    return Box::pin(self.execute_native_procedure_rows(context, procedure))
                        .await
                        .map(QueryOutput::Rows);
                }
                if let Some(parsed) = parse_experimental_route_mutation(query, &context)? {
                    self.operation_metrics.record_cypher_route(CypherRoute::MutationFallback);
                    let result = Box::pin(self.execute_parsed_opencypher_mutation_with_receipt(context, query, parsed)).await?;
                    return Ok(QueryOutput::Mutation(result));
                }
            }
            return self.execute_cypher_rows(context, query).await.map(QueryOutput::Rows);
        }
        #[cfg(feature = "opencypher")]
        self.operation_metrics.record_cypher_route(CypherRoute::Legacy);
        Box::pin(self.execute_opencypher(context, query)).await
    }

    pub async fn execute_cypher_rows(&self, context: QueryContext, query: &str) -> Result<QueryResultSet> {
        self.ensure_cypher_layout(&context).await?;
        #[cfg(feature = "datafusion-cypher-engine")]
        if context.cypher_engine == CypherEngineMode::DataFusion {
            let shard = self.shared_datafusion_self()?;
            return shard.execute_datafusion_cypher_rows(context, query).await;
        }
        if context.cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "opencypher")]
            {
                if let Some(procedure) =
                    crate::query::path_procedure::parse_native_procedure(query, &context.parameters, self.limits.max_traversal_hops)?
                {
                    self.ensure_managed_cypher_layout().await?;
                    self.operation_metrics.record_cypher_route(CypherRoute::NativePathFallback);
                    return Box::pin(self.execute_native_procedure_rows(context, procedure)).await;
                }
                if let Some(parsed) = parse_experimental_route_mutation(query, &context)? {
                    self.operation_metrics.record_cypher_route(CypherRoute::MutationFallback);
                    let mutation = Box::pin(self.execute_parsed_opencypher_mutation_with_receipt(context, query, parsed)).await?;
                    return Ok(mutation.into_result_set());
                }
            }
            #[cfg(feature = "experimental-cypher-engine")]
            {
                self.operation_metrics.record_cypher_route(CypherRoute::Experimental);
                return super::experimental_cypher::execute_experimental_cypher_rows(self, context, query).await;
            }
            #[cfg(not(feature = "experimental-cypher-engine"))]
            {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "Cypher25",
                    feature: "the experimental-cypher-engine Cargo feature is not enabled".to_string(),
                });
            }
        }
        #[cfg(feature = "opencypher")]
        self.operation_metrics.record_cypher_route(CypherRoute::Legacy);
        Box::pin(self.execute_opencypher_rows(context, query)).await
    }

    pub async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        self.ensure_cypher_layout(&context).await?;
        #[cfg(feature = "datafusion-cypher-engine")]
        if context.cypher_engine == CypherEngineMode::DataFusion {
            let shard = self.shared_datafusion_self()?;
            return shard.execute_datafusion_cypher_rows_page(context, query, cursor, page_size).await;
        }
        if context.cypher_engine == CypherEngineMode::Experimental {
            #[cfg(feature = "opencypher")]
            {
                if let Some(procedure) =
                    crate::query::path_procedure::parse_native_procedure(query, &context.parameters, self.limits.max_traversal_hops)?
                {
                    self.ensure_managed_cypher_layout().await?;
                    self.operation_metrics.record_cypher_route(CypherRoute::NativePathFallback);
                    return Box::pin(self.execute_native_path_rows_page(context, query, procedure, cursor, page_size)).await;
                }
            }
            #[cfg(feature = "opencypher")]
            if cursor.is_none() {
                if let Some(parsed) = parse_experimental_route_mutation(query, &context)? {
                    self.operation_metrics.record_cypher_route(CypherRoute::MutationFallback);
                    let mutation = Box::pin(self.execute_parsed_opencypher_mutation_with_receipt(context, query, parsed)).await?;
                    let rows = mutation.into_result_set();
                    return Ok(QueryResultPage::new(rows.columns, rows.rows, None));
                }
            }
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "Cypher25",
                feature: if cursor.is_some() {
                    "experimental mutations cannot continue from a cursor"
                } else {
                    "direct experimental read pages are not implemented; use the client server cursor"
                }
                .to_string(),
            });
        }
        #[cfg(feature = "opencypher")]
        self.operation_metrics.record_cypher_route(CypherRoute::Legacy);
        Box::pin(self.execute_opencypher_rows_page(context, query, cursor, page_size)).await
    }

    pub async fn execute_opencypher(&self, context: QueryContext, query: &str) -> Result<QueryOutput> {
        #[cfg(feature = "opencypher")]
        self.ensure_managed_cypher_layout().await?;
        #[cfg(feature = "opencypher")]
        {
            if let Some(procedure) =
                crate::query::path_procedure::parse_native_procedure(query, &context.parameters, self.limits.max_traversal_hops)?
            {
                return Box::pin(self.execute_native_procedure_rows(context, procedure))
                    .await
                    .map(QueryOutput::Rows);
            }
            if let Some(parsed) =
                parse_opencypher_mutation_query_with_list_parameters(query, &context.parameters, &context.list_parameters)?
            {
                let result = Box::pin(self.execute_parsed_opencypher_mutation_with_receipt(context, query, parsed)).await?;
                return Ok(QueryOutput::Mutation(result));
            }
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters, &context.list_parameters)
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            let result_set = Box::pin(self.execute_parsed_opencypher_rows(context, parsed.clone())).await?;
            Ok(query_result_set_to_output(&parsed, result_set))
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
    }

    pub async fn execute_opencypher_rows(&self, context: QueryContext, query: &str) -> Result<QueryResultSet> {
        #[cfg(feature = "opencypher")]
        self.ensure_managed_cypher_layout().await?;
        #[cfg(feature = "opencypher")]
        {
            if let Some(procedure) =
                crate::query::path_procedure::parse_native_procedure(query, &context.parameters, self.limits.max_traversal_hops)?
            {
                return Box::pin(self.execute_native_procedure_rows(context, procedure)).await;
            }
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters, &context.list_parameters)
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            Box::pin(self.execute_parsed_opencypher_rows(context, parsed)).await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query);
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
    }

    pub async fn execute_opencypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        #[cfg(feature = "opencypher")]
        self.ensure_managed_cypher_layout().await?;
        #[cfg(feature = "opencypher")]
        {
            if let Some(procedure) =
                crate::query::path_procedure::parse_native_procedure(query, &context.parameters, self.limits.max_traversal_hops)?
            {
                return Box::pin(self.execute_native_path_rows_page(context, query, procedure, cursor, page_size)).await;
            }
            let parsed = self
                .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters, &context.list_parameters)
                .await?;
            let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
            if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::InvalidRequest,
                    dialect: "OpenCypher",
                    feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
                });
            }
            if context.read_epoch.is_none() {
                let snapshot = if context.uses_refreshed_reader() {
                    self.storage.strong_snapshot().await?
                } else {
                    self.storage.snapshot().await?
                };
                let read_epoch = snapshot.seq();
                let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
                return SlateGraphStore::scope_snapshot(
                    snapshot,
                    Box::pin(self.execute_parsed_opencypher_rows_page(context, parsed, cursor, page_size)),
                )
                .await;
            }
            Box::pin(self.execute_parsed_opencypher_rows_page(context, parsed, cursor, page_size)).await
        }
        #[cfg(not(feature = "opencypher"))]
        {
            let _ = (context, query, cursor, page_size);
            Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "enable the opencypher Cargo feature to parse Cypher".to_string(),
            })
        }
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_rows_page(
        &self,
        context: QueryContext,
        mut parsed: ParsedRowQuery,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        let cursor_offset = cursor.map_or(0, |cursor| cursor.offset);
        let started = std::time::Instant::now();
        let result_budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        match self
            .try_execute_source_relationship_id_rows_page(&context, &parsed, cursor_offset, page_size)
            .await
        {
            Ok(Some(page)) => {
                if let Err(error) = result_budget.account_result_rows(&page.rows) {
                    self.record_streaming_query_rows_failure(started, &error);
                    return Err(error);
                }
                self.record_streaming_query_rows_success(page.rows.len(), started);
                return Ok(page);
            }
            Ok(None) => {}
            Err(err) => {
                self.record_streaming_query_rows_failure(started, &err);
                return Err(err);
            }
        }
        match Box::pin(self.try_execute_graph_kernel_opencypher_rows_page(&context, &parsed, cursor_offset, page_size)).await {
            Ok(Some(page)) => {
                if let Err(error) = result_budget.account_result_rows(&page.rows) {
                    self.record_streaming_query_rows_failure(started, &error);
                    return Err(error);
                }
                self.record_streaming_query_rows_success(page.rows.len(), started);
                return Ok(page);
            }
            Ok(None) => {}
            Err(err) => {
                self.record_streaming_query_rows_failure(started, &err);
                return Err(err);
            }
        }

        match self
            .try_execute_streaming_opencypher_rows_page(&context, &parsed, cursor_offset, page_size)
            .await
        {
            Ok(Some(page)) => {
                if let Err(error) = result_budget.account_result_rows(&page.rows) {
                    self.record_streaming_query_rows_failure(started, &error);
                    return Err(error);
                }
                self.record_streaming_query_rows_success(page.rows.len(), started);
                return Ok(page);
            }
            Ok(None) => {}
            Err(err) => {
                self.record_streaming_query_rows_failure(started, &err);
                return Err(err);
            }
        }

        let mut context = self.query_page_context(context, cursor_offset, page_size)?;
        // The execution window includes one probe row to determine whether a
        // cursor follows. Account only the page after that probe is removed.
        context.max_result_bytes = None;
        parsed.window = QueryWindow::default();
        let mut result_set = Box::pin(self.execute_parsed_opencypher_rows(context, parsed)).await?;
        let next_cursor = if result_set.rows.len() > page_size {
            result_set.rows.truncate(page_size);
            Some(QueryCursorToken::new(cursor_offset.checked_add(page_size as u64).ok_or_else(
                || GraphError::AdmissionRejected {
                    operation: "query_cursor_offset",
                    actual: u64::MAX,
                    limit: u64::MAX - 1,
                },
            )?))
        } else {
            None
        };
        let page = QueryResultPage::new(result_set.columns, result_set.rows, next_cursor).with_statuses(result_set.statuses);
        result_budget.account_result_rows(&page.rows)?;
        Ok(page)
    }

    pub async fn execute_query_statement(&self, context: QueryContext, statement: QueryStatement) -> Result<QueryOutput> {
        let plan = self.plan_query_statement(context, statement)?;
        self.execute_query_plan(plan).await
    }

    pub fn plan_query_statement(&self, context: QueryContext, statement: QueryStatement) -> Result<QueryPlan> {
        QueryPlanner::plan(&context, &statement)
    }

    #[cfg(feature = "opencypher")]
    pub async fn explain_opencypher_rows(&self, context: QueryContext, query: &str) -> Result<RowQueryPlan> {
        validate_component("cell_id", &context.cell_id)?;
        if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
            });
        }
        let parsed = self
            .parsed_opencypher_row_query(&context.cell_id, query, &context.parameters, &context.list_parameters)
            .await?;
        let context = merge_opencypher_window(context, opencypher_outer_window(&parsed))?;
        let read_epoch = self.query_read_epoch(&context).await?;
        self.explain_row_query_plan_with_stats(&context.cell_id, read_epoch, &parsed).await
    }

    #[cfg(feature = "opencypher")]
    async fn parsed_opencypher_row_query(
        &self,
        cell_id: &str,
        query: &str,
        parameters: &BTreeMap<String, VertexPropertyValue>,
        lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
    ) -> Result<ParsedRowQuery> {
        // Parameter values are baked into the lowered tree, so anything
        // parameterised must bypass the cache — lists included.
        if !parameters.is_empty() || !lists.is_empty() {
            return parse_opencypher_row_query_with_list_parameters(query, parameters, lists);
        }
        let key = ParsedRowQueryCacheKey::new(query);
        if let Some(parsed) = self.parsed_row_query_cache.lock().await.get(&key) {
            self.cache_metrics.record_hit(GraphCacheKind::ParsedRowQuery);
            return Ok(parsed);
        }

        self.cache_metrics.record_miss(GraphCacheKind::ParsedRowQuery);
        let parsed = parse_opencypher_row_query_with_list_parameters(query, parameters, lists)?;
        self.parsed_row_query_cache
            .lock()
            .await
            .insert(key, parsed.clone(), cell_id.to_string(), false, &self.cache_metrics);
        Ok(parsed)
    }

    #[cfg(feature = "opencypher")]
    // `skip_all` is not tidiness: `query` holds the lowered predicate tree with
    // every parameter value already substituted in, and `context` holds the
    // parameter map itself. Neither may ever reach a span.
    #[tracing::instrument(
        name = "query.execute",
        level = "info",
        skip_all,
        fields(
            hydradb.cell_id = %context.cell_id,
            hydradb.read_epoch = tracing::field::Empty,
            hydradb.query.rows_returned = tracing::field::Empty,
            hydradb.query.runtime_property_index = tracing::field::Empty,
            hydradb.storage.remote_gets = tracing::field::Empty,
            hydradb.storage.remote_get_bytes = tracing::field::Empty,
            hydradb.storage.ranged_gets = tracing::field::Empty,
            hydradb.storage.puts = tracing::field::Empty,
            hydradb.storage.put_bytes = tracing::field::Empty,
            error.class = tracing::field::Empty,
            error.operation = tracing::field::Empty,
            hydradb.sampling.tail_keep = tracing::field::Empty,
        )
    )]
    async fn execute_parsed_opencypher_rows(&self, context: QueryContext, query: ParsedRowQuery) -> Result<QueryResultSet> {
        if context.read_epoch.is_some() && context.validated_read_epoch().is_none() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "historical graph epochs are not storage snapshots; execute against a current SlateDB snapshot".to_string(),
            });
        }
        self.operation_metrics.query_rows_started.fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let remote_io = crate::engine::remote_io::RemoteIoWindow::open();
        let result = if context.read_epoch.is_none() {
            // The epoch every freshness bug is an argument about is decided
            // here, by whichever of the two readers the context selected — so
            // the choice is a field, not an inference from which code path ran.
            let snapshot = async {
                if context.uses_refreshed_reader() {
                    self.storage.strong_snapshot().await
                } else {
                    self.storage.snapshot().await
                }
            }
            .instrument(tracing::info_span!(
                "storage.snapshot",
                hydradb.cell_id = %context.cell_id,
                refreshed_reader = context.uses_refreshed_reader(),
            ))
            .await;
            match snapshot {
                Ok(snapshot) => {
                    let read_epoch = snapshot.seq();
                    tracing::Span::current().record("hydradb.read_epoch", read_epoch);
                    let context = context.with_validated_storage_read_epoch(read_epoch, read_epoch);
                    SlateGraphStore::scope_snapshot(snapshot, Box::pin(self.execute_parsed_opencypher_rows_inner(context, query))).await
                }
                Err(err) => Err(err),
            }
        } else {
            Box::pin(self.execute_parsed_opencypher_rows_inner(context, query)).await
        };
        self.operation_metrics.query_rows_latency.record(started.elapsed());
        let span = tracing::Span::current();
        remote_io.record(&span);
        match &result {
            Ok(result_set) => {
                self.operation_metrics.query_rows_completed.fetch_add(1, Ordering::Relaxed);
                self.operation_metrics
                    .query_rows_returned
                    .fetch_add(result_set.rows.len() as u64, Ordering::Relaxed);
                span.record("hydradb.query.rows_returned", result_set.rows.len() as u64);
                if let Some(read_epoch) = result_set.read_epoch {
                    span.record("hydradb.read_epoch", read_epoch);
                }
            }
            Err(err) => {
                self.operation_metrics.record_query_rows_failure(err);
                span.record("error.class", err.class());
                if let Some(operation) = err.limit_operation() {
                    span.record("error.operation", operation);
                }
                span.record("hydradb.sampling.tail_keep", "error");
            }
        }
        result
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_rows_inner(&self, context: QueryContext, query: ParsedRowQuery) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("cypher_rows")?;
        let storage_sequence = context.validated_storage_sequence();
        let read_epoch = self.query_read_epoch(&context).await?;

        let result = if !query.union_arms.is_empty() {
            self.execute_union_opencypher_rows(&context.cell_id, read_epoch, query, context.result_window, &budget)
                .await
        } else {
            self.execute_single_opencypher_rows(&context.cell_id, read_epoch, query, context.result_window, &budget)
                .await
        }?;
        budget.account_result_rows(&result.rows)?;
        let result = result.with_read_epoch(read_epoch);
        Ok(match storage_sequence {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        })
    }

    #[cfg(feature = "opencypher")]
    async fn execute_union_opencypher_rows(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        mut query: ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        budget.check("cypher_union")?;
        let union_all = query.union_all;
        let mut arms = std::mem::take(&mut query.union_arms);
        query.union_all = false;
        let columns = query.columns.clone();

        let first_window = query.window;
        let mut rows = self
            .execute_single_opencypher_rows(cell_id, read_epoch, query, first_window, budget)
            .await?
            .rows;
        for arm in arms.drain(..) {
            budget.check("cypher_union_arm")?;
            if arm.columns != columns {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Union,
                    dialect: "OpenCypher",
                    feature: "UNION arms must project the same column names".to_string(),
                });
            }
            let arm_window = arm.window;
            rows.extend(
                self.execute_single_opencypher_rows(cell_id, read_epoch, arm, arm_window, budget)
                    .await?
                    .rows,
            );
        }

        if !union_all {
            let mut seen = BTreeSet::new();
            rows.retain(|row| seen.insert(row.values.clone()));
        }

        let projected = rows
            .into_iter()
            .map(|row| ProjectedQueryRow {
                row,
                sort_keys: Vec::new(),
            })
            .collect();
        self.finish_projected_rows(columns, projected, &[], false, window, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn execute_single_opencypher_rows(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        if let Some(result) = self
            .try_execute_graph_kernel_row_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_source_relationship_id_rows_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_relationship_count_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_ordered_relationship_property_rows_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_execute_relationship_rows_query(cell_id, read_epoch, &query, window, budget)
            .await?
        {
            return Ok(result);
        }
        if let Some(result) =
            Box::pin(self.try_execute_ordered_string_vertex_rows_query(cell_id, read_epoch, &query, window, budget)).await?
        {
            return Ok(result);
        }

        let bindings = if query.pattern_groups.is_empty() {
            let group = RowMatchGroup {
                patterns: query.patterns.clone(),
                predicate: query.predicate.clone(),
                optional: false,
            };
            let seeds = self
                .node_equality_predicate_seed_rows(cell_id, &group, read_epoch, budget, &BindingRow::default())
                .await?
                .unwrap_or_else(|| vec![BindingRow::default()]);
            let mut bindings = self
                .match_row_patterns_from_rows(cell_id, &query.patterns, read_epoch, budget, seeds)
                .await?;
            if let Some(predicate) = &query.predicate {
                let mut filtered = Vec::with_capacity(bindings.len());
                for row in bindings {
                    budget.check("cypher_where")?;
                    if row_predicate_matches(&row, predicate)? {
                        filtered.push(row);
                    }
                }
                bindings = filtered;
            }
            bindings
        } else {
            self.match_row_pattern_groups(cell_id, &query.pattern_groups, read_epoch, budget)
                .await?
        };

        if row_projections_have_aggregates(&query.projections) {
            let projected = aggregate_projected_rows(bindings, &query.projections, &query.columns, &query.order_by, budget)?;
            return self.finish_projected_rows(query.columns, projected, &query.order_by, query.distinct, window, budget);
        }

        let mut projected = Vec::with_capacity(bindings.len());
        for binding in &bindings {
            budget.check("cypher_project")?;
            let row = project_binding_row(binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(binding, &row, &query.columns, &query.order_by)?;
            push_projected_query_row(&mut projected, row, sort_keys);
        }
        self.finish_projected_rows(query.columns, projected, &query.order_by, query.distinct, window, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_ordered_string_vertex_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(spec) = ordered_string_vertex_index_spec(query, window) else {
            return Ok(None);
        };
        let OrderedStringVertexIndexSpec {
            node,
            binding,
            property,
            predicate,
            ascending,
            limit,
        } = spec;
        if limit == 0 {
            return Ok(Some(QueryResultSet::new(query.columns.clone(), Vec::new())));
        }

        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let required = skip.checked_add(limit).ok_or(GraphError::AdmissionRejected {
            operation: "query_result_window",
            actual: u64::MAX,
            limit: usize::MAX as u64,
        })?;
        // A complete equality union can filter the ordered index before any
        // metadata I/O. Retained IDs obey the existing index-candidate cap;
        // metadata rows still obey the independent intermediate-row cap.
        let group = RowMatchGroup {
            patterns: query.patterns.clone(),
            predicate: query.predicate.clone(),
            optional: false,
        };
        let hydrate_seed = |vertices: Vec<VertexId>| async move {
            let mut projected = OrderedRowWindow::new(required, &query.order_by);
            for batch in vertices.chunks(QUERY_METADATA_HYDRATION_CONCURRENCY) {
                for (vertex, metadata) in self.vertex_metadata_batch_at(cell_id, batch, read_epoch, budget).await? {
                    budget.check("cypher_ordered_vertex_equality_project")?;
                    let Some(mut row) = BindingRow::from_node(node, vertex) else {
                        continue;
                    };
                    row.metadata.insert(binding.to_string(), metadata);
                    if !row_matches_node(&row, node)? || !row_predicate_matches(&row, predicate)? {
                        continue;
                    }
                    let result = project_binding_row(&row, &query.projections)?;
                    let sort_keys = sort_keys_for_row(&row, &result, &query.columns, &query.order_by)?;
                    projected.push(result, sort_keys);
                    self.ensure_query_intermediate_rows("cypher_ordered_vertex_window", projected.len())?;
                }
            }
            self.finish_projected_rows(query.columns.clone(), projected.into_rows(), &query.order_by, false, window, budget)
        };
        // The equality probe is speculative: a broad cold prefix must not
        // delay an ordered page that can already produce the exact result.
        let equality = async {
            let Some((_, vertices)) = self
                .node_equality_predicate_seed_vertices(
                    cell_id,
                    &group,
                    read_epoch,
                    budget,
                    &BindingRow::default(),
                    NodeEqualityProbe::Ordered(256),
                )
                .await?
            else {
                return Ok::<_, GraphError>(None);
            };
            Ok(Some(hydrate_seed(vertices).await?))
        };
        let ordered = async {
            let mut seed: Option<Vec<VertexId>> = None;
            let mut seed_probed = false;
            let mut examined = 0_usize;
            let mut rejected = 0_usize;

            let prefix = format!("{}s", self.vertex_property_index_prefix_at(cell_id, property, budget).await?);
            let order = if ascending {
                slatedb::IterationOrder::Ascending
            } else {
                slatedb::IterationOrder::Descending
            };
            let ordered_scan_options = remote_scan_options().with_order(order);
            // Keyset pagination must seek to its lower bound instead of replaying
            // the complete property index on every page. The prefix already fixes
            // the string type marker (`s`), so the SlateDB subrange starts at the
            // encoded string payload that follows it.
            let start_suffix = ascending
                .then(|| string_property_lower_bound(predicate, binding, property))
                .flatten()
                .map(|bound| {
                    encode_vertex_property_value_key(&VertexPropertyValue::String(bound))
                        .strip_prefix('s')
                        .unwrap_or_default()
                        .as_bytes()
                        .to_vec()
                });
            // Admit only a bounded leading page into the shared block cache. The
            // actual scan stays non-admitting: predicates and primary-key ties can
            // make a small LIMIT walk a large index. A short warm-up avoids paying
            // the same first-block S3 reads on every ordered page without admitting
            // the complete long scan or changing its result/scan-limit semantics.
            if self.storage.has_block_cache() {
                let warm_options = remote_scan_options().with_order(order).with_cache_blocks(true);
                let mut warm = budget
                    .read_only_io(
                        "cypher_ordered_vertex_index_warm",
                        self.storage
                            .scan_prefix_with_options(prefix.as_bytes(), start_suffix.clone(), &warm_options),
                    )
                    .await?;
                for _ in 0..required.saturating_add(1).min(256) {
                    if budget
                        .read_only_io("cypher_ordered_vertex_index_warm_next", async {
                            warm.next().await.map_err(GraphError::from)
                        })
                        .await?
                        .is_none()
                    {
                        break;
                    }
                }
                drop(warm);
            }
            let mut iter = budget
                .read_only_io(
                    "cypher_ordered_vertex_index_seek",
                    self.storage
                        .scan_prefix_with_options(prefix.as_bytes(), start_suffix.clone(), &ordered_scan_options),
                )
                .await?;
            let mut projected = OrderedRowWindow::new(required, &query.order_by);
            let mut scanned = 0_usize;
            let mut boundary = None::<String>;
            let mut exhausted = false;
            let mut seed_fallback = false;
            while !exhausted {
                let batch_capacity = (if boundary.is_some() {
                    QUERY_METADATA_HYDRATION_CONCURRENCY
                } else {
                    required
                        .saturating_sub(projected.len())
                        .clamp(1, QUERY_METADATA_HYDRATION_CONCURRENCY)
                })
                .min(self.limits.max_query_index_candidates.saturating_sub(scanned).max(1));
                let mut candidates = Vec::with_capacity(batch_capacity);
                while candidates.len() < batch_capacity {
                    let Some(kv) = budget
                        .read_only_io("cypher_ordered_vertex_index_next", async {
                            iter.next().await.map_err(GraphError::from)
                        })
                        .await?
                    else {
                        exhausted = true;
                        break;
                    };
                    budget.check("cypher_ordered_vertex_property_index")?;
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let (_cell, indexed_property, encoded, vertex_id) = parse_vertex_property_index_key(&key)?;
                    if indexed_property != *property {
                        continue;
                    }
                    if projected.len() >= required && boundary.as_deref() != Some(encoded.as_str()) {
                        exhausted = true;
                        break;
                    }
                    scanned = scanned.saturating_add(1);
                    if scanned > self.limits.max_query_index_candidates && seed.is_some() {
                        seed_fallback = true;
                        exhausted = true;
                        break;
                    }
                    self.ensure_query_index_candidates("cypher_ordered_vertex_property_candidates", scanned)?;
                    if seed.as_ref().is_some_and(|vertices| vertices.binary_search(&vertex_id).is_err()) {
                        continue;
                    }
                    candidates.push((encoded, vertex_id));
                }
                if seed_fallback {
                    break;
                }
                let vertex_ids = candidates.iter().map(|(_, vertex_id)| *vertex_id).collect::<Vec<_>>();
                let metadata = self.vertex_metadata_batch_at(cell_id, &vertex_ids, read_epoch, budget).await?;
                for ((encoded, vertex_id), (_, metadata)) in candidates.into_iter().zip(metadata) {
                    if projected.len() >= required && boundary.as_deref() != Some(encoded.as_str()) {
                        exhausted = true;
                        break;
                    }
                    examined += 1;
                    if metadata
                        .properties
                        .get(property)
                        .is_none_or(|value| encode_vertex_property_value_key(value) != encoded)
                    {
                        rejected += 1;
                        continue;
                    }
                    let Some(mut row) = BindingRow::from_node(node, vertex_id) else {
                        rejected += 1;
                        continue;
                    };
                    row.metadata.insert(binding.to_string(), metadata);
                    if !row_matches_node(&row, node)? || !row_predicate_matches(&row, predicate)? {
                        rejected += 1;
                        continue;
                    }
                    let result_row = project_binding_row(&row, &query.projections)?;
                    let sort_keys = sort_keys_for_row(&row, &result_row, &query.columns, &query.order_by)?;
                    projected.push(result_row, sort_keys);
                    // Consume the complete primary-key tie for secondary ordering,
                    // but retain only the best window, not the whole tie population.
                    self.ensure_query_intermediate_rows("cypher_ordered_vertex_property_rows", projected.len())?;
                    if projected.len() == required {
                        boundary = Some(encoded);
                    }
                }
                // Dense pages should finish directly, without first enumerating a
                // huge type union. Switch once when a bounded sample shows poor
                // selectivity, or when the ordered access reaches its scan bound.
                if !seed_probed
                    && !exhausted
                    && ((rejected >= QUERY_METADATA_HYDRATION_CONCURRENCY && rejected.saturating_mul(2) > examined)
                        || scanned >= self.limits.max_query_index_candidates)
                {
                    seed_probed = true;
                    seed = self
                        .node_equality_predicate_seed_vertices(
                            cell_id,
                            &group,
                            read_epoch,
                            budget,
                            &BindingRow::default(),
                            NodeEqualityProbe::Ordered(self.limits.max_query_index_candidates),
                        )
                        .await?
                        .map(|(_, vertices)| vertices);
                    if seed.as_ref().is_some_and(Vec::is_empty) {
                        return Ok(Some(QueryResultSet::new(query.columns.clone(), Vec::new())));
                    }
                    if seed.is_some() {
                        // Restart against the same pinned snapshot. Discard the
                        // sample's projections so it cannot duplicate output rows.
                        projected = OrderedRowWindow::new(required, &query.order_by);
                        boundary = None;
                        scanned = 0;
                        iter = budget
                            .read_only_io(
                                "cypher_ordered_vertex_index_seek",
                                self.storage
                                    .scan_prefix_with_options(prefix.as_bytes(), start_suffix.clone(), &ordered_scan_options),
                            )
                            .await?;
                    }
                }
            }

            // The ordered scan may encounter too many unrelated index entries.
            // A complete seed remains an exact bounded fallback, never a partial
            // result from the interrupted ordered pass. Release its iterator first.
            drop(iter);
            if seed_fallback {
                if let Some(vertices) = seed {
                    return Ok(Some(hydrate_seed(vertices).await?));
                }
            }

            Ok(Some(self.finish_projected_rows(
                query.columns.clone(),
                projected.into_rows(),
                &query.order_by,
                false,
                window,
                budget,
            )?))
        };
        tokio::pin!(equality, ordered);
        // Polled in order, the cheap equality probe first: when SlateDB serves
        // both from memory, each can finish in its first poll, and an unbiased
        // select would pick the ordered page's wider hydration at random. A
        // pending probe still never holds the ordered page back.
        tokio::select! {
            biased;
            seed = &mut equality => match seed? {
                Some(result) => Ok(Some(result)),
                None => ordered.await,
            },
            result = &mut ordered => match result {
                Ok(result) => Ok(result),
                Err(error) => match equality.await? {
                    Some(result) => Ok(Some(result)),
                    None => Err(error),
                },
            },
        }
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_graph_kernel_row_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(request) = graph_kernel_row_query_request(query) else {
            return Ok(None);
        };
        if request.projection == GraphKernelProjection::CountAll && request.edge.dst.id.is_none() {
            let (count, edge_visits) = self
                .reachable_count_in_hop_range_at(cell_id, &request.edge.edge_type, request.src, request.hop_range, read_epoch, budget)
                .await?;
            self.ensure_query_scan_edges("cypher_graph_kernel_count_edge_visits", edge_visits)?;
            let row = QueryRow::new(vec![QueryValue::Count(count)]);
            let rows = vec![row];
            return Ok(Some(QueryResultSet::new(
                query.columns.clone(),
                self.apply_query_row_window(rows, window)?,
            )));
        }
        if request.projection == GraphKernelProjection::NodeId && request.edge.dst.id.is_none() && !window.is_default() && !query.distinct {
            let (vertices, edge_visits) = self
                .reachable_vertices_window_in_hop_range_at(ReachableWindowRequest {
                    cell_id,
                    edge_type: &request.edge.edge_type,
                    src: request.src,
                    hop_range: request.hop_range,
                    read_epoch,
                    window,
                    ascending: request.ascending,
                    budget,
                })
                .await?;
            self.ensure_query_scan_edges("cypher_graph_kernel_window_edge_visits", edge_visits)?;
            self.ensure_query_intermediate_rows("cypher_graph_kernel_window_rows", vertices.len())?;
            let rows = graph_kernel_node_id_rows(vertices, budget)?;
            return Ok(Some(QueryResultSet::new(query.columns.clone(), rows)));
        }

        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(cell_id, &request.edge.edge_type, request.src, request.hop_range, read_epoch, budget)
            .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_edge_visits", edge_visits)?;
        if let Some(dst) = request.edge.dst.id {
            vertices.retain(|vertex| *vertex == dst);
        }
        graph_kernel_order_vertices(&mut vertices, request.ascending);
        self.ensure_query_intermediate_rows("cypher_graph_kernel_rows", vertices.len())?;

        let projected = match request.projection {
            GraphKernelProjection::NodeId => graph_kernel_node_id_rows(vertices, budget)?
                .into_iter()
                .map(|row| ProjectedQueryRow {
                    row,
                    sort_keys: Vec::new(),
                })
                .collect(),
            GraphKernelProjection::CountAll => {
                let row = QueryRow::new(vec![QueryValue::Count(vertices.len() as u64)]);
                vec![ProjectedQueryRow {
                    row,
                    sort_keys: Vec::new(),
                }]
            }
        };
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &[],
            query.distinct,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_relationship_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = relationship_rows_query_edge(query) else {
            return Ok(None);
        };
        if !relationship_rows_projection_supported(edge, &query.projections)
            || !relationship_rows_order_supported(edge, &query.projections, &query.columns, &query.order_by)
        {
            return Ok(None);
        }
        let (Some(src), Some(dst)) = (edge.src.id, edge.dst.id) else {
            return Ok(None);
        };
        let relationships = if let Some((property, value)) = first_indexable_property(&edge.properties) {
            self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                cell_id,
                edge_type: &edge.edge_type,
                src,
                dst,
                property,
                value,
                read_epoch,
                budget,
            })
            .await?
        } else {
            self.relationships_for_edge_at(cell_id, &edge.edge_type, src, dst, read_epoch, budget)
                .await?
        };
        let mut projected = Vec::with_capacity(relationships.len());
        for (relationship, metadata) in relationships {
            budget.check("cypher_relationship_rows_fast_path")?;
            if !relationship_metadata_matches(&metadata, edge) {
                continue;
            }
            let row = project_relationship_row(edge, &relationship, &metadata, &query.projections)?;
            let sort_keys = sort_keys_for_relationship_row(edge, &relationship, &metadata, &row, query)?;
            push_projected_query_row(&mut projected, row, sort_keys);
            self.ensure_query_intermediate_rows("cypher_relationship_rows_fast_path", projected.len())?;
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &query.order_by,
            query.distinct,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_ordered_relationship_property_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(spec) = ordered_relationship_property_index_spec(query, window) else {
            return Ok(None);
        };
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }
        let OrderedRelationshipPropertyIndexSpec {
            edge,
            property,
            predicate,
            ascending,
            limit,
        } = spec;
        if limit == 0 {
            return Ok(Some(QueryResultSet::new(query.columns.clone(), Vec::new())));
        }
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let required = skip.checked_add(limit).ok_or(GraphError::AdmissionRejected {
            operation: "query_result_window",
            actual: u64::MAX,
            limit: usize::MAX as u64,
        })?;
        let order = if ascending {
            slatedb::IterationOrder::Ascending
        } else {
            slatedb::IterationOrder::Descending
        };
        let edge_prefix = keys::edge_property_index_property_prefix(cell_id, &edge.edge_type, property);
        let relationship_prefix = keys::relationship_property_index_property_prefix(cell_id, &edge.edge_type, property);
        let mut edge_iters = Vec::with_capacity(ORDERED_PROPERTY_INDEX_TYPE_PREFIXES.len());
        let mut relationship_iters = Vec::with_capacity(ORDERED_PROPERTY_INDEX_TYPE_PREFIXES.len());
        for value_type in ORDERED_PROPERTY_INDEX_TYPE_PREFIXES {
            edge_iters.push(
                self.storage
                    .scan_prefix_with_options(
                        format!("{edge_prefix}{value_type}").as_bytes(),
                        None,
                        &remote_scan_options().with_order(order),
                    )
                    .await?,
            );
            relationship_iters.push(
                self.storage
                    .scan_prefix_with_options(
                        format!("{relationship_prefix}{value_type}").as_bytes(),
                        None,
                        &remote_scan_options().with_order(order),
                    )
                    .await?,
            );
        }
        let mut next_edges = Vec::with_capacity(edge_iters.len());
        for iter in &mut edge_iters {
            next_edges.push(match iter.next().await? {
                Some(kv) => Some(ordered_edge_property_candidate(&kv.key)?),
                None => None,
            });
        }
        let mut next_relationships = Vec::with_capacity(relationship_iters.len());
        for iter in &mut relationship_iters {
            next_relationships.push(match iter.next().await? {
                Some(kv) => Some(ordered_relationship_property_candidate(&kv.key)?),
                None => None,
            });
        }

        let snapshot = self.snapshot(cell_id).await?;
        let mut metadata_cache = BTreeMap::new();
        let mut pair_memo = IndexedRelationshipPairMemo::default();
        let mut top = OrderedRowWindow::new(required, &query.order_by);
        let mut scanned = 0_usize;
        let mut done = false;
        while !done {
            if top.len() == required && query.order_by.len() == 1 {
                break;
            }
            let open = required.saturating_sub(top.len());
            let batch_size = if open == 0 {
                ORDERED_RELATIONSHIP_FETCH_BATCH
            } else {
                open.min(ORDERED_RELATIONSHIP_FETCH_BATCH)
            };
            let mut candidates = Vec::with_capacity(batch_size);
            while candidates.len() < batch_size {
                let Some(source) = ordered_relationship_property_candidate_source(&next_edges, &next_relationships, ascending)? else {
                    done = true;
                    break;
                };
                let candidate = match source {
                    OrderedRelationshipPropertyCandidateSource::Edge(index) => {
                        let candidate = next_edges[index].take().expect("edge head");
                        next_edges[index] = edge_iters[index]
                            .next()
                            .await?
                            .map(|kv| ordered_edge_property_candidate(&kv.key))
                            .transpose()?;
                        candidate
                    }
                    OrderedRelationshipPropertyCandidateSource::Relationship(index) => {
                        let candidate = next_relationships[index].take().expect("relationship head");
                        next_relationships[index] = relationship_iters[index]
                            .next()
                            .await?
                            .map(|kv| ordered_relationship_property_candidate(&kv.key))
                            .transpose()?;
                        candidate
                    }
                };
                budget.check("cypher_ordered_relationship_property_index")?;
                scanned += 1;
                self.ensure_query_index_candidates("cypher_ordered_relationship_property_candidates", scanned)?;
                if top.len() == required {
                    let value = QueryValue::Property(decode_vertex_property_value_key(&candidate.encoded)?);
                    let boundary = &top.rows.peek().expect("full window").projected.sort_keys[0];
                    let cmp = compare_query_values(&value, boundary);
                    if (if ascending { cmp } else { cmp.reverse() }).is_gt() {
                        done = true;
                        break;
                    }
                }
                candidates.push(candidate);
            }
            if candidates.is_empty() {
                break;
            }
            let identified = candidates
                .iter()
                .filter_map(|c| c.relationship_id.map(|id| (c.src, c.dst, id, c.encoded.clone())))
                .collect::<Vec<_>>();
            let identified_metadata = if identified.is_empty() {
                Vec::new()
            } else {
                self.indexed_relationship_metadata_batch_at(IndexedRelationshipBatch {
                    cell_id,
                    edge_type: &edge.edge_type,
                    property,
                    candidates: &identified,
                    read_epoch,
                    inherit_structural: false,
                    budget,
                    memo: &mut pair_memo,
                })
                .await?
            };
            let structural = stream::iter(candidates.iter().filter(|c| c.relationship_id.is_none()).map(|c| {
                let snapshot = &snapshot;
                async move {
                    let (exists, metadata) = futures::try_join!(
                        budget.read_only_io(
                            "cypher_ordered_structural_exists",
                            snapshot.edge_exists(&edge.edge_type, c.src, c.dst)
                        ),
                        self.edge_metadata_at(cell_id, &edge.edge_type, c.src, c.dst, read_epoch, budget),
                    )?;
                    Ok::<_, GraphError>(exists.then_some(metadata))
                }
            }))
            .buffered(ORDERED_RELATIONSHIP_FETCH_CONCURRENCY)
            .boxed()
            .try_collect::<Vec<_>>()
            .await?;
            let mut identified_metadata = identified_metadata.into_iter();
            let mut structural = structural.into_iter();
            let mut bindings = Vec::with_capacity(candidates.len());
            for c in candidates {
                let metadata = if c.relationship_id.is_some() {
                    identified_metadata.next().expect("identified result")
                } else {
                    structural.next().expect("structural result")
                };
                let Some(metadata) = metadata else {
                    continue;
                };
                if metadata
                    .properties
                    .get(property)
                    .is_none_or(|value| encode_vertex_property_value_key(value) != c.encoded)
                {
                    continue;
                }
                let relationship = BoundRelationship {
                    edge_type: edge.edge_type.clone(),
                    src: c.src,
                    dst: c.dst,
                    relationship_id: c.relationship_id,
                };
                if let Some(binding) = BindingRow::from_relationship(edge, relationship, metadata) {
                    bindings.push(binding);
                }
            }
            let wanted = bindings.iter().flat_map(|row| row.values.values().copied()).collect::<Vec<_>>();
            self.hydrate_vertex_metadata_cache_at(cell_id, &wanted, read_epoch, &mut metadata_cache, budget)
                .await?;
            for mut row in bindings {
                self.hydrate_binding_metadata(cell_id, read_epoch, &mut row, &mut metadata_cache, budget)
                    .await?;
                if !row_matches_edge_pattern(&row, edge)? {
                    continue;
                }
                let mut matching = vec![row];
                retain_binding_rows_matching_predicate(&mut matching, predicate, budget)?;
                let Some(row) = matching.pop() else {
                    continue;
                };
                let projected = project_binding_row(&row, &query.projections)?;
                let keys = sort_keys_for_row(&row, &projected, &query.columns, &query.order_by)?;
                top.push(projected, keys);
                self.ensure_query_intermediate_rows("cypher_ordered_relationship_window", top.len())?;
            }
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            top.into_rows(),
            &query.order_by,
            false,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_source_relationship_id_rows_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = source_relationship_id_rows_query_edge(query) else {
            return Ok(None);
        };
        let Some(src) = edge.src.id else {
            return Ok(None);
        };

        let mut bindings = self
            .source_relationship_id_bindings_at(cell_id, edge, src, read_epoch, budget)
            .await?;
        self.ensure_query_intermediate_rows("cypher_source_relationship_id_rows", bindings.len())?;
        let mut projected = Vec::with_capacity(bindings.len());
        for binding in bindings.drain(..) {
            budget.check("cypher_source_relationship_id_project")?;
            let row = project_binding_row(&binding, &query.projections)?;
            let sort_keys = sort_keys_for_row(&binding, &row, &query.columns, &query.order_by)?;
            push_projected_query_row(&mut projected, row, sort_keys);
        }
        Ok(Some(self.finish_projected_rows(
            query.columns.clone(),
            projected,
            &query.order_by,
            query.distinct,
            window,
            budget,
        )?))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_relationship_count_query(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        query: &ParsedRowQuery,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<Option<QueryResultSet>> {
        let Some(edge) = relationship_count_query_edge(query) else {
            return Ok(None);
        };
        let (Some(src), Some(dst)) = (edge.src.id, edge.dst.id) else {
            return Ok(None);
        };
        let count = self
            .relationship_count_for_edge_at(cell_id, &edge.edge_type, src, dst, read_epoch, budget)
            .await?;
        let row = QueryRow::new(vec![QueryValue::Count(count)]);
        let rows = vec![row];
        Ok(Some(QueryResultSet::new(
            query.columns.clone(),
            self.apply_query_row_window(rows, window)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_source_relationship_id_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(edge) = source_relationship_id_rows_query_edge(query) else {
            return Ok(None);
        };
        let Some(ascending) = source_relationship_id_page_order(edge, query) else {
            return Ok(None);
        };
        if query.distinct {
            return Ok(None);
        }
        let Some(src) = edge.src.id else {
            return Ok(None);
        };
        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let window = page_context.result_window;
        if window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(query.columns.clone(), Vec::new(), None)));
        }
        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_source_relationship_page")?;

        let cached = self
            .source_relationship_dsts_at(&context.cell_id, edge, src, read_epoch, &budget)
            .await?;
        let mut dsts = cached.as_ref().clone();
        dsts.sort_unstable();
        if !ascending {
            dsts.reverse();
        }
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let probe_limit = window.limit.unwrap_or(page_size.saturating_add(1));
        let page_dsts = dsts.into_iter().skip(skip).take(probe_limit).collect::<Vec<_>>();
        let has_next = page_dsts.len() > page_size;
        let page_dsts = &page_dsts[..page_dsts.len().min(page_size)];
        let bindings = source_relationship_id_bindings_from_dsts(edge, src, page_dsts, self, &budget)?;
        let mut rows = Vec::with_capacity(bindings.len());
        for binding in bindings {
            budget.check("cypher_source_relationship_page_project")?;
            let row = project_binding_row(&binding, &query.projections)?;
            rows.push(row);
        }
        Ok(Some(QueryResultPage::new(
            query.columns.clone(),
            rows,
            query_next_cursor(cursor_offset, page_size, has_next)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_mutation_with_receipt(
        &self,
        context: QueryContext,
        source_query: &str,
        query: ParsedMutationQuery,
    ) -> Result<QueryMutationResult> {
        #[cfg(feature = "datafusion-cypher-engine")]
        if !context.idempotency_key.is_empty() {
            let lookup = crate::shard::datafusion_cypher::statement_receipt_lookup(&context, source_query)?;
            let result = crate::shard::datafusion_cypher::scope_legacy_statement_receipt(
                lookup.clone(),
                self.execute_parsed_opencypher_mutation(context, query),
            )
            .await?;
            // Receipt persistence advances storage, not graph topology. Preserve
            // the actual mutation's topology signal (including None for metadata
            // updates and segment-backed relationship deletes).
            // Execution already succeeded, so its graph writes have happened.
            // Persisting the completion receipt is part of finishing that write,
            // not another cancellable admission phase. In particular, the
            // multi-batch effects shield ends before reaching this wrapper.
            QueryCancellationToken::shielded(crate::shard::datafusion_cypher::complete_legacy_statement_receipt(
                self, lookup, &result,
            ))
            .await?;
            return Ok(result);
        }
        #[cfg(not(feature = "datafusion-cypher-engine"))]
        let _ = source_query;
        self.execute_parsed_opencypher_mutation(context, query).await
    }

    #[cfg(feature = "opencypher")]
    async fn execute_parsed_opencypher_mutation(&self, context: QueryContext, query: ParsedMutationQuery) -> Result<QueryMutationResult> {
        self.ensure_managed_cypher_layout().await?;
        validate_component("cell_id", &context.cell_id)?;
        if context.read_epoch.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "mutation queries cannot run at a historical read epoch".to_string(),
            });
        }
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_mutation")?;

        if query.patterns.is_empty() {
            return self.execute_patternless_mutation(&context, &query.actions, &budget).await;
        }

        // Keep the actual storage snapshot, not just its sequence: another
        // write may commit between choosing the sequence and expanding an
        // edge. Every matching read must use the same immutable view. Scope
        // only matching; the write paths below must observe current storage.
        // A statement computes its updates from a snapshot taken before its
        // write transactions. If a record it updates changed in between,
        // nothing is written and the whole statement re-runs on a new
        // snapshot, as if it had started after the other write.
        for _attempt in 0..STATEMENT_RACE_ATTEMPTS {
            let snapshot = self.storage.snapshot().await?;
            let read_epoch = snapshot.seq();
            // Match through the same planner as reads, so a `WHERE` equality,
            // prefix or relationship-property predicate becomes an index seek
            // instead of a filter over every vertex the label matches. `WITH ...
            // LIMIT n` applies after the predicate, which is where Cypher puts it;
            // the matcher stops collecting once n filtered rows exist.
            let group = RowMatchGroup {
                patterns: query.patterns.clone(),
                predicate: query.predicate.clone(),
                optional: false,
            };
            let mut bindings = SlateGraphStore::scope_snapshot(snapshot, async {
                self.ensure_cell_readable(&context.cell_id, "cypher_mutation").await?;
                self.match_row_pattern_groups_limited(&context.cell_id, std::slice::from_ref(&group), read_epoch, &budget, query.row_limit)
                    .await
            })
            .await?;
            if let Some(limit) = query.row_limit {
                bindings.truncate(limit);
            }

            let returned_rows = query.returning.as_ref().map(|returning| mutation_return_rows(returning, &bindings));

            let mut result = QueryMutationResult {
                matched_rows: bindings.len() as u64,
                returned_rows,
                ..QueryMutationResult::default()
            };
            let mut effects = StatementEffects::default();
            self.collect_statement_effects(&context.cell_id, read_epoch, &query.actions, &bindings, &budget, &mut effects)
                .await?;
            effects.drop_noops(&mut result);

            // Every check against the deadline happens above: matching and effect
            // collection are read-only, so a statement that runs out of time there
            // has written nothing. From here each kind of effect is one batch
            // transaction with one durable WAL commit, and a commit, once started,
            // is never raced against cancellation.
            budget.check("cypher_mutation_commit")?;
            match self.apply_statement_effects(&context, effects, &mut result).await? {
                StatementApply::Applied => return Ok(result),
                StatementApply::LostRace => continue,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "cypher mutation",
            attempts: STATEMENT_RACE_ATTEMPTS,
        })
    }

    /// `UNWIND $rows AS row MATCH ... SET/REMOVE` as one statement. Every row
    /// is matched against the same snapshot, every row's updates are gathered
    /// (a later row building on an earlier row's update of the same record),
    /// and the whole batch is applied as one transaction per kind of effect
    /// with one durable WAL commit. A batch either applies every row's update
    /// or none of them, and costs one WAL write rather than one per row.
    #[cfg(all(feature = "opencypher", feature = "client-api"))]
    pub(crate) async fn execute_parsed_opencypher_mutation_rows(
        &self,
        context: QueryContext,
        rows: Vec<ParsedMutationQuery>,
    ) -> Result<QueryMutationResult> {
        self.ensure_managed_cypher_layout().await?;
        validate_component("cell_id", &context.cell_id)?;
        if context.read_epoch.is_some() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "OpenCypher",
                feature: "mutation queries cannot run at a historical read epoch".to_string(),
            });
        }
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_unwind_mutation")?;
        if rows
            .iter()
            .any(|row| row.patterns.is_empty() || row.row_limit.is_some() || row.returning.is_some())
        {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Unwind,
                dialect: "OpenCypher",
                feature: "UNWIND MATCH SET requires MATCH and no WITH or RETURN".to_string(),
            });
        }

        // The batch re-runs on a new snapshot when a record it updates
        // changed after its snapshot, as one statement does.
        for _attempt in 0..STATEMENT_RACE_ATTEMPTS {
            let snapshot = self.storage.snapshot().await?;
            let read_epoch = snapshot.seq();
            let mut result = QueryMutationResult::default();
            let mut effects = StatementEffects::default();
            SlateGraphStore::scope_snapshot(snapshot, async {
                self.ensure_cell_readable(&context.cell_id, "cypher_unwind_mutation").await?;
                for row in &rows {
                    budget.check("cypher_unwind_mutation_row")?;
                    let group = RowMatchGroup {
                        patterns: row.patterns.clone(),
                        predicate: row.predicate.clone(),
                        optional: false,
                    };
                    let bindings = self
                        .match_row_pattern_groups(&context.cell_id, std::slice::from_ref(&group), read_epoch, &budget)
                        .await?;
                    result.matched_rows = result.matched_rows.saturating_add(bindings.len() as u64);
                    self.collect_statement_effects(&context.cell_id, read_epoch, &row.actions, &bindings, &budget, &mut effects)
                        .await?;
                }
                Ok::<_, GraphError>(())
            })
            .await?;
            effects.drop_noops(&mut result);

            budget.check("cypher_unwind_mutation_commit")?;
            match self.apply_statement_effects(&context, effects, &mut result).await? {
                StatementApply::Applied => return Ok(result),
                StatementApply::LostRace => continue,
            }
        }
        Err(GraphError::RetryExhausted {
            operation: "cypher unwind mutation",
            attempts: STATEMENT_RACE_ATTEMPTS,
        })
    }

    /// Add the writes `actions` make on every matched row to `effects`,
    /// before any of them is applied. A statement calls this once; an `UNWIND`
    /// batch calls it once per input row, so later rows build on earlier ones.
    #[cfg(feature = "opencypher")]
    async fn collect_statement_effects(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        actions: &[RowMutationAction],
        bindings: &[BindingRow],
        budget: &QueryBudget,
        effects: &mut StatementEffects,
    ) -> Result<()> {
        for action in actions {
            budget.check("cypher_mutation_action")?;
            match action {
                RowMutationAction::DeleteBinding { binding, detach } => {
                    for row in bindings {
                        if let Some(relationship) = row.relationships.get(binding) {
                            effects.deleted_relationships.insert(relationship.clone());
                        } else {
                            let deletes = effects.deleted_vertices.entry(row.get(binding)?).or_insert(false);
                            *deletes |= *detach;
                        }
                    }
                }
                RowMutationAction::DeleteRelationship { binding, .. } => {
                    for row in bindings {
                        let Some(relationship) = row.relationships.get(binding) else {
                            return Err(GraphError::UnsupportedQuery {
                                reason: QueryFailureReason::Mutation,
                                dialect: "OpenCypher",
                                feature: format!("DELETE references unbound relationship {binding}"),
                            });
                        };
                        effects.deleted_relationships.insert(relationship.clone());
                    }
                }
                RowMutationAction::SetProperty { .. }
                | RowMutationAction::SetLabels { .. }
                | RowMutationAction::RemoveProperty { .. }
                | RowMutationAction::RemoveLabels { .. } => {
                    let mut state = VertexMutationApplyState {
                        cell_id,
                        read_epoch,
                        pending_metadata: &mut effects.vertex_metadata,
                        original_metadata: &mut effects.original_metadata,
                        pending_edge_metadata: &mut effects.relationship_metadata,
                        original_edge_metadata: &mut effects.original_edge_metadata,
                        budget,
                    };
                    for row in bindings {
                        self.apply_vertex_mutation_action(row, action, &mut state).await?;
                    }
                }
                RowMutationAction::CreateVertex { .. } | RowMutationAction::CreateEdge { .. } | RowMutationAction::MergeEdge { .. } => {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "OpenCypher",
                        feature: "CREATE and MERGE are executable only as standalone clauses".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Apply a statement's effects as one batch transaction per kind of
    /// effect: property updates (vertices, then relationships) and deletes
    /// (relationships, then vertices), in the order `apply_statement_effect_batches`
    /// gives. A statement that makes one kind of
    /// change -- every `MATCH ... DELETE`, `DETACH DELETE`, `SET` or `REMOVE`
    /// -- commits all of it or none of it, with one durable WAL commit.
    #[cfg(feature = "opencypher")]
    async fn apply_statement_effects(
        &self,
        context: &QueryContext,
        effects: StatementEffects,
        result: &mut QueryMutationResult,
    ) -> Result<StatementApply> {
        // One transaction keeps the write pipeline's cancellation: a cancelled
        // client stops waiting for the WAL upload, and the owned commit task
        // still makes it durable. Several transactions must not stop between
        // them, so once the deadline check has passed they run unshielded by
        // cancellation; the cluster also runs them on their own task, so a
        // dropped caller cannot stop them either.
        // The statement's deadline was checked once its effects were known;
        // the batches carry no budget, so none can stop after another commits.
        if effects.batch_transactions() > 1 {
            QueryCancellationToken::shielded(self.apply_statement_effect_batches(context, effects, result)).await
        } else {
            self.apply_statement_effect_batches(context, effects, result).await
        }
    }

    #[cfg(feature = "opencypher")]
    async fn apply_statement_effect_batches(
        &self,
        context: &QueryContext,
        effects: StatementEffects,
        result: &mut QueryMutationResult,
    ) -> Result<StatementApply> {
        let StatementEffects {
            deleted_relationships,
            deleted_vertices,
            vertex_metadata,
            relationship_metadata,
            original_metadata,
            original_edge_metadata,
        } = effects;
        let updates = StatementUpdates {
            vertex_metadata,
            relationship_metadata,
            original_metadata,
            original_edge_metadata,
        };
        // A plain `DELETE` of a connected vertex is refused, and a refused
        // statement must leave nothing written: its deletes go first, as
        // they always did, and a lost race after them is returned, since a
        // re-run would not find what they deleted. Every other statement
        // updates first, so a lost race is found before anything is written
        // and the statement re-runs. Updates of anything this statement
        // deletes were dropped already.
        if deleted_vertices.values().any(|detach| !detach) {
            let written = self
                .apply_statement_deletes(context, deleted_relationships, &deleted_vertices, result)
                .await?;
            self.apply_statement_updates(context, updates, written, result).await
        } else {
            if let StatementApply::LostRace = self.apply_statement_updates(context, updates, false, result).await? {
                return Ok(StatementApply::LostRace);
            }
            self.apply_statement_deletes(context, deleted_relationships, &deleted_vertices, result)
                .await?;
            Ok(StatementApply::Applied)
        }
    }

    /// Apply the property updates, each only onto the record the statement
    /// read. A record changed since the snapshot is a lost race the statement
    /// can re-run from, until anything has been written (`written`, or the
    /// vertex updates committing): a re-run would then apply that twice
    /// (`SET n.c = n.c + 1`), so the conflict goes to the client.
    #[cfg(feature = "opencypher")]
    async fn apply_statement_updates(
        &self,
        context: &QueryContext,
        updates: StatementUpdates,
        mut written: bool,
        result: &mut QueryMutationResult,
    ) -> Result<StatementApply> {
        let StatementUpdates {
            vertex_metadata,
            relationship_metadata,
            original_metadata,
            original_edge_metadata,
        } = updates;
        if !vertex_metadata.is_empty() {
            let requested = vertex_metadata.len() as u64;
            let swaps = vertex_metadata
                .into_iter()
                .map(|(vertex_id, metadata)| {
                    let read = original_metadata.get(&vertex_id).cloned().unwrap_or_default();
                    (vertex_id, (read, metadata))
                })
                .collect();
            let changed = match self.swap_vertex_metadata_batch(&context.cell_id, swaps).await {
                Err(GraphError::ConditionalWriteConflict { .. }) if !written => return Ok(StatementApply::LostRace),
                changed => changed? as u64,
            };
            written |= changed > 0;
            result.updated_vertices = result.updated_vertices.saturating_add(changed);
            result.noops = result.noops.saturating_add(requested.saturating_sub(changed));
        }

        if !relationship_metadata.is_empty() {
            let requested = relationship_metadata.len() as u64;
            let updates = relationship_metadata
                .into_iter()
                .map(|(relationship, metadata)| RelationshipMetadataUpdate {
                    read: Some(original_edge_metadata.get(&relationship).cloned().unwrap_or_default()),
                    edge_type: relationship.edge_type,
                    src: relationship.src,
                    dst: relationship.dst,
                    relationship_id: relationship.relationship_id,
                    metadata,
                })
                .collect();
            let changed = match self.set_relationship_metadata_batch(&context.cell_id, updates).await {
                Err(GraphError::ConditionalWriteConflict { .. }) if !written => return Ok(StatementApply::LostRace),
                changed => changed? as u64,
            };
            result.updated_relationships = result.updated_relationships.saturating_add(changed);
            result.noops = result.noops.saturating_add(requested.saturating_sub(changed));
        }
        Ok(StatementApply::Applied)
    }

    /// Apply the relationship and vertex deletes; true when any wrote.
    #[cfg(feature = "opencypher")]
    async fn apply_statement_deletes(
        &self,
        context: &QueryContext,
        deleted_relationships: BTreeSet<BoundRelationship>,
        deleted_vertices: &BTreeMap<VertexId, bool>,
        result: &mut QueryMutationResult,
    ) -> Result<bool> {
        let unbounded = QueryBudget::new(None, None);
        let mut written = false;
        if !deleted_relationships.is_empty() {
            let deleted = self
                .delete_bound_relationships_batch(context, deleted_relationships, Vec::new(), &unbounded)
                .await?;
            written |= deleted.deleted > 0;
            result.deleted_edges = result.deleted_edges.saturating_add(deleted.deleted);
            result.noops = result.noops.saturating_add(deleted.already_deleted);
            result.topology_sequence = result.topology_sequence.max(deleted.topology_sequence);
        }

        // One batch per DETACH mode: `DELETE n` must still refuse a connected
        // vertex while `DETACH DELETE m` in the same statement removes its edges.
        for detach in [true, false] {
            let deletions = deleted_vertices
                .iter()
                .filter(|(_, vertex_detach)| **vertex_detach == detach)
                .map(|(vertex_id, _)| {
                    let operation = if detach { "detach-delete-node" } else { "delete-node" };
                    (*vertex_id, format!("{}.{operation}.{vertex_id}", context.idempotency_key))
                })
                .collect::<Vec<_>>();
            if deletions.is_empty() {
                continue;
            }
            for delete in self.delete_vertex_mutations_batch(&context.cell_id, deletions, detach).await? {
                written |= delete.vertex_deleted || delete.incident_edges_deleted > 0 || delete.relationships_deleted > 0;
                result.deleted_edges = result.deleted_edges.saturating_add(delete.incident_edges_deleted);
                if delete.incident_edges_deleted > 0 {
                    result.topology_sequence = result.topology_sequence.max(Some(delete.epoch));
                }
                if delete.vertex_deleted {
                    result.updated_vertices = result.updated_vertices.saturating_add(1);
                } else if delete.incident_edges_deleted == 0 && delete.relationships_deleted == 0 {
                    result.noops = result.noops.saturating_add(1);
                }
            }
        }
        Ok(written)
    }

    #[cfg(feature = "opencypher")]
    async fn delete_bound_relationships_batch(
        &self,
        context: &QueryContext,
        relationships: BTreeSet<BoundRelationship>,
        structural_delete_guards: Vec<EdgeMutation>,
        budget: &QueryBudget,
    ) -> Result<RelationshipDeleteBatchResult> {
        let mut structural = Vec::new();
        let mut identified = Vec::new();
        for relationship in relationships {
            budget.check("cypher_delete_relationship_collect")?;
            let mutation = bound_relationship_delete_mutation(context, &relationship);
            match relationship.relationship_id {
                Some(relationship_id) => identified.push((mutation, relationship_id)),
                None => structural.push(mutation),
            }
        }

        let mut deleted = 0_u64;
        let mut noops = 0_u64;
        let mut topology_sequence = None;
        if !structural.is_empty() {
            budget.check("cypher_delete_edge_batch")?;
            let batch = self.delete_edge_mutations_batch(&context.cell_id, structural).await?;
            deleted = deleted.saturating_add(batch.deleted);
            noops = noops.saturating_add(batch.already_deleted);
            if batch.deleted > 0 {
                topology_sequence = Some(batch.end_epoch);
            }
        }
        if !identified.is_empty() {
            budget.check("cypher_delete_relationship_batch")?;
            let batch = self
                .delete_relationship_mutations_batch_with_guards(&context.cell_id, identified, structural_delete_guards)
                .await?;
            deleted = deleted.saturating_add(batch.deleted);
            noops = noops.saturating_add(batch.already_deleted);
            topology_sequence = topology_sequence.max(batch.topology_sequence);
        }
        Ok(RelationshipDeleteBatchResult {
            deleted,
            already_deleted: noops,
            topology_sequence,
        })
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn delete_relationships_by_property_values_batch(
        &self,
        context: &QueryContext,
        edge_type: &str,
        property: &str,
        values: Vec<VertexPropertyValue>,
    ) -> Result<RelationshipDeleteBatchResult> {
        validate_component("cell_id", &context.cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        let read_epoch = self.current_epoch(&context.cell_id).await?;
        let native_values = values
            .iter()
            .filter(|value| !value.is_scalar_indexable())
            .cloned()
            .collect::<Vec<_>>();
        let scans = values
            .into_iter()
            .flat_map(|value| {
                equivalent_property_index_keys(&value)
                    .into_iter()
                    .map(move |encoded| (value.clone(), encoded))
            })
            .collect::<Vec<_>>();
        let mut scan_results = stream::iter(scans)
            .map(|(value, encoded)| {
                let budget = budget.clone();
                async move {
                    budget.check("cypher_delete_relationship_property_batch")?;
                    let (concrete, structural) = tokio::try_join!(
                        self.scan_relationship_property_index_current(&context.cell_id, edge_type, property, &encoded, &budget,),
                        self.scan_edge_property_index_current(&context.cell_id, edge_type, property, &encoded, &budget,),
                    )?;
                    Ok::<_, GraphError>((value, concrete, structural))
                }
            })
            .buffer_unordered(QUERY_METADATA_HYDRATION_CONCURRENCY);

        let mut concrete_candidates = BTreeMap::<BoundRelationship, Vec<VertexPropertyValue>>::new();
        let mut structural_candidates = BTreeMap::<(VertexId, VertexId), Vec<VertexPropertyValue>>::new();
        while let Some((value, concrete, structural)) = scan_results.try_next().await? {
            for (src, dst, relationship_id) in concrete {
                concrete_candidates
                    .entry(BoundRelationship {
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                        relationship_id: Some(relationship_id),
                    })
                    .or_default()
                    .push(value.clone());
            }
            for edge in structural {
                structural_candidates.entry(edge).or_default().push(value.clone());
            }
            self.ensure_query_index_candidates(
                "cypher_delete_relationship_property_candidates",
                concrete_candidates.len().saturating_add(structural_candidates.len()),
            )?;
        }

        let concrete = stream::iter(concrete_candidates)
            .map(|(relationship, values)| {
                let budget = budget.clone();
                async move {
                    let metadata = self
                        .relationship_metadata_at(&context.cell_id, &relationship, read_epoch, &budget)
                        .await?;
                    Ok::<_, GraphError>(
                        metadata
                            .properties
                            .get(property)
                            .is_some_and(|existing| values.iter().any(|value| vertex_property_values_equal(existing, value)))
                            .then_some(relationship),
                    )
                }
            })
            .buffered(QUERY_METADATA_HYDRATION_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let structural = stream::iter(structural_candidates)
            .map(|((src, dst), values)| {
                let budget = budget.clone();
                async move {
                    if self
                        .relationship_count_for_edge_at(&context.cell_id, edge_type, src, dst, read_epoch, &budget)
                        .await?
                        > 0
                    {
                        return Ok::<_, GraphError>(None);
                    }
                    let metadata = self
                        .edge_metadata_at(&context.cell_id, edge_type, src, dst, read_epoch, &budget)
                        .await?;
                    Ok(metadata
                        .properties
                        .get(property)
                        .is_some_and(|existing| values.iter().any(|value| vertex_property_values_equal(existing, value)))
                        .then_some(BoundRelationship {
                            edge_type: edge_type.to_string(),
                            src,
                            dst,
                            relationship_id: None,
                        }))
                }
            })
            .buffered(QUERY_METADATA_HYDRATION_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut relationships = concrete.into_iter().chain(structural).flatten().collect::<BTreeSet<_>>();
        if !native_values.is_empty() {
            let edges = self
                .edges_at_with_budget(&context.cell_id, edge_type, read_epoch, Some(&budget))
                .await?;
            for edge in edges {
                budget.check("cypher_delete_relationship_native_property_scan")?;
                let rows = self
                    .relationships_for_edge_at(&context.cell_id, edge_type, edge.src, edge.dst, read_epoch, &budget)
                    .await?;
                let has_identified = rows.iter().any(|(relationship, _)| relationship.relationship_id.is_some());
                for (relationship, metadata) in rows {
                    if relationship.relationship_id.is_none() && has_identified {
                        continue;
                    }
                    if metadata
                        .properties
                        .get(property)
                        .is_some_and(|existing| native_values.iter().any(|value| vertex_property_values_equal(existing, value)))
                    {
                        relationships.insert(relationship);
                        self.ensure_query_index_candidates("cypher_delete_relationship_native_property_candidates", relationships.len())?;
                    }
                }
            }
        }
        let structural_delete_guards = relationships
            .iter()
            .filter(|relationship| relationship.relationship_id.is_some())
            .map(|relationship| {
                let mut structural = relationship.clone();
                structural.relationship_id = None;
                (
                    (structural.edge_type.clone(), structural.src, structural.dst),
                    bound_relationship_delete_mutation(context, &structural),
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.delete_bound_relationships_batch(context, relationships, structural_delete_guards.into_values().collect(), &budget)
            .await
    }

    #[cfg(feature = "opencypher")]
    async fn execute_patternless_mutation(
        &self,
        context: &QueryContext,
        actions: &[RowMutationAction],
        budget: &QueryBudget,
    ) -> Result<QueryMutationResult> {
        if actions
            .iter()
            .any(|action| matches!(action, RowMutationAction::CreateVertex { .. }))
        {
            // One standalone node per statement: several would need a batch
            // writer to stay atomic, as multi-pattern CREATE with metadata does.
            let [RowMutationAction::CreateVertex { vertex, metadata }] = actions else {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "OpenCypher",
                    feature: "CREATE of a standalone node must be the only pattern in its statement".to_string(),
                });
            };
            let created = self
                .create_vertex(
                    &context.cell_id,
                    *vertex,
                    metadata.clone(),
                    &format!("{}.create-vertex.{vertex}", context.idempotency_key),
                )
                .await?;
            // A new vertex reports its commit epoch, so the coordinator wakes
            // the graph indexer as it does for other writes; a replayed retry
            // changed nothing and reports none.
            return Ok(QueryMutationResult {
                created_vertices: u64::from(!created.already_created),
                noops: u64::from(created.already_created),
                topology_sequence: (!created.already_created).then_some(created.epoch),
                ..QueryMutationResult::default()
            });
        }
        if actions.iter().any(|action| matches!(action, RowMutationAction::CreateEdge { .. })) {
            if !actions.iter().all(|action| matches!(action, RowMutationAction::CreateEdge { .. })) {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "OpenCypher",
                    feature: "CREATE batches cannot mix mutation action types".to_string(),
                });
            }
            if actions.len() > 1
                && actions.iter().any(|action| match action {
                    RowMutationAction::CreateEdge {
                        src_metadata,
                        dst_metadata,
                        edge_metadata,
                        ..
                    } => {
                        !src_metadata.labels.is_empty()
                            || !src_metadata.properties.is_empty()
                            || !dst_metadata.labels.is_empty()
                            || !dst_metadata.properties.is_empty()
                            || !edge_metadata.properties.is_empty()
                    }
                    _ => false,
                })
            {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "OpenCypher",
                    feature: "multi-pattern CREATE with labels or properties requires the metadata batch writer".to_string(),
                });
            }

            if actions.len() == 1 {
                let RowMutationAction::CreateEdge {
                    edge_type,
                    src,
                    dst,
                    src_metadata,
                    dst_metadata,
                    edge_metadata,
                } = &actions[0]
                else {
                    unreachable!("CREATE batch was validated above")
                };
                let mutation = EdgeMutation {
                    cell_id: context.cell_id.clone(),
                    edge_type: edge_type.clone(),
                    src: *src,
                    dst: *dst,
                    idempotency_key: format!(
                        "{}.create.{}.{}.{}.00000000000000000000",
                        context.idempotency_key, edge_type, src, dst
                    ),
                };
                let created = if src_metadata.labels.is_empty()
                    && src_metadata.properties.is_empty()
                    && dst_metadata.labels.is_empty()
                    && dst_metadata.properties.is_empty()
                    && edge_metadata.properties.is_empty()
                {
                    self.create_relationship(mutation, EdgeMetadata::default()).await?
                } else if edge_metadata.properties.is_empty() {
                    self.create_relationship_with_vertex_metadata(mutation, src_metadata.clone(), dst_metadata.clone())
                        .await?
                } else {
                    self.create_relationship_with_full_metadata(mutation, src_metadata.clone(), dst_metadata.clone(), edge_metadata.clone())
                        .await?
                };
                return Ok(QueryMutationResult {
                    created_edges: u64::from(created.structural_edge_inserted),
                    created_relationships: u64::from(!created.already_created),
                    noops: u64::from(created.already_created),
                    topology_sequence: created.structural_edge_inserted.then_some(created.epoch),
                    ..QueryMutationResult::default()
                });
            }

            let mutations = actions
                .iter()
                .enumerate()
                .map(|(index, action)| {
                    let RowMutationAction::CreateEdge { edge_type, src, dst, .. } = action else {
                        unreachable!("CREATE batch was validated above")
                    };
                    EdgeMutation {
                        cell_id: context.cell_id.clone(),
                        edge_type: edge_type.clone(),
                        src: *src,
                        dst: *dst,
                        idempotency_key: format!("{}.create.{}.{}.{}.{index:020}", context.idempotency_key, edge_type, src, dst),
                    }
                })
                .collect::<Vec<_>>();
            let batch = self.write_edge_mutations_batch(&context.cell_id, mutations).await?;
            return Ok(QueryMutationResult {
                created_edges: batch.inserted,
                noops: batch.already_existed,
                topology_sequence: (batch.inserted > 0).then_some(batch.end_epoch),
                ..QueryMutationResult::default()
            });
        }

        let mut result = QueryMutationResult::default();
        for action in actions {
            budget.check("cypher_patternless_mutation")?;
            let RowMutationAction::MergeEdge {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
                edge_metadata,
            } = action
            else {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Mutation,
                    dialect: "OpenCypher",
                    feature: "patternless mutation supports only standalone MERGE".to_string(),
                });
            };
            let mutation = EdgeMutation {
                cell_id: context.cell_id.clone(),
                edge_type: edge_type.clone(),
                src: *src,
                dst: *dst,
                idempotency_key: format!("{}.merge.{}.{}.{}", context.idempotency_key, edge_type, src, dst),
            };
            let commit = if src_metadata.labels.is_empty()
                && src_metadata.properties.is_empty()
                && dst_metadata.labels.is_empty()
                && dst_metadata.properties.is_empty()
                && edge_metadata.properties.is_empty()
            {
                self.write_edge(mutation).await?
            } else if edge_metadata.properties.is_empty() {
                self.write_edge_with_vertex_metadata(mutation, src_metadata.clone(), dst_metadata.clone())
                    .await?
            } else {
                self.write_edge_with_full_metadata(mutation, src_metadata.clone(), dst_metadata.clone(), edge_metadata.clone())
                    .await?
            };
            if commit.already_existed {
                result.noops = result.noops.saturating_add(1);
            } else {
                result.created_edges = result.created_edges.saturating_add(1);
                result.topology_sequence = result.topology_sequence.max(Some(commit.epoch));
            }
        }
        Ok(result)
    }

    #[cfg(feature = "opencypher")]
    async fn apply_vertex_mutation_action(
        &self,
        row: &BindingRow,
        action: &RowMutationAction,
        state: &mut VertexMutationApplyState<'_>,
    ) -> Result<()> {
        let binding = match action {
            RowMutationAction::SetProperty { binding, .. }
            | RowMutationAction::SetLabels { binding, .. }
            | RowMutationAction::RemoveProperty { binding, .. }
            | RowMutationAction::RemoveLabels { binding, .. } => binding,
            _ => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "OpenCypher",
                    feature: "non-vertex mutation action cannot update metadata".to_string(),
                });
            }
        };
        if let Some(relationship) = row.relationships.get(binding) {
            match action {
                RowMutationAction::SetProperty { property, value, .. } => {
                    if state.pending_edge_metadata.get(relationship).is_none() {
                        state.budget.check("cypher_load_relationship_metadata")?;
                        let metadata = match row.relationship_metadata.get(relationship) {
                            Some(metadata) => metadata.clone(),
                            None => {
                                self.edge_metadata_at(
                                    state.cell_id,
                                    &relationship.edge_type,
                                    relationship.src,
                                    relationship.dst,
                                    state.read_epoch,
                                    state.budget,
                                )
                                .await?
                            }
                        };
                        state.original_edge_metadata.insert(relationship.clone(), metadata.clone());
                        state.pending_edge_metadata.insert(relationship.clone(), metadata);
                    }
                    let metadata = state
                        .pending_edge_metadata
                        .get_mut(relationship)
                        .ok_or_else(|| GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Other,
                            dialect: "OpenCypher",
                            feature: format!("metadata for relationship {binding} was not loaded"),
                        })?;
                    metadata.properties.insert(property.clone(), value.clone());
                    return Ok(());
                }
                RowMutationAction::RemoveProperty { property, .. } => {
                    if state.pending_edge_metadata.get(relationship).is_none() {
                        state.budget.check("cypher_load_relationship_metadata")?;
                        let metadata = match row.relationship_metadata.get(relationship) {
                            Some(metadata) => metadata.clone(),
                            None => {
                                self.edge_metadata_at(
                                    state.cell_id,
                                    &relationship.edge_type,
                                    relationship.src,
                                    relationship.dst,
                                    state.read_epoch,
                                    state.budget,
                                )
                                .await?
                            }
                        };
                        state.original_edge_metadata.insert(relationship.clone(), metadata.clone());
                        state.pending_edge_metadata.insert(relationship.clone(), metadata);
                    }
                    let metadata = state
                        .pending_edge_metadata
                        .get_mut(relationship)
                        .ok_or_else(|| GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::Other,
                            dialect: "OpenCypher",
                            feature: format!("metadata for relationship {binding} was not loaded"),
                        })?;
                    metadata.properties.remove(property);
                    return Ok(());
                }
                RowMutationAction::SetLabels { .. } | RowMutationAction::RemoveLabels { .. } => {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Mutation,
                        dialect: "OpenCypher",
                        feature: "relationship labels are not executable in Query engine".to_string(),
                    });
                }
                _ => {}
            }
        }
        let vertex_id = row.get(binding)?;
        if state.pending_metadata.get(&vertex_id).is_none() {
            state.budget.check("cypher_load_mutation_metadata")?;
            let metadata = match row.metadata.get(binding) {
                Some(metadata) => metadata.clone(),
                None => {
                    self.vertex_metadata_at(state.cell_id, vertex_id, state.read_epoch, state.budget)
                        .await?
                }
            };
            state.original_metadata.insert(vertex_id, metadata.clone());
            state.pending_metadata.insert(vertex_id, metadata);
        }
        let metadata = state
            .pending_metadata
            .get_mut(&vertex_id)
            .ok_or_else(|| GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("metadata for {binding} was not loaded"),
            })?;
        match action {
            RowMutationAction::SetProperty { property, value, .. } => {
                metadata.properties.insert(property.clone(), value.clone());
            }
            RowMutationAction::SetLabels { labels, .. } => {
                metadata.labels.extend(labels.iter().cloned());
            }
            RowMutationAction::RemoveProperty { property, .. } => {
                metadata.properties.remove(property);
            }
            RowMutationAction::RemoveLabels { labels, .. } => {
                for label in labels {
                    metadata.labels.remove(label);
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn execute_query_plan(&self, plan: QueryPlan) -> Result<QueryOutput> {
        self.validate_executable_query_plan(&plan).await?;
        let budget = QueryBudget::new(plan.max_runtime_ms.or(self.limits.max_query_runtime_ms), None);
        budget.check("query_plan")?;
        match plan.physical {
            PhysicalQueryPlan::WriteEdge { edge_type, src, dst } => {
                let result = self
                    .create_relationship(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        EdgeMetadata::default(),
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::WriteEdgeWithMetadata {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
            } => {
                let result = self
                    .create_relationship_with_vertex_metadata(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        src_metadata,
                        dst_metadata,
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::WriteEdgeWithFullMetadata {
                edge_type,
                src,
                dst,
                src_metadata,
                dst_metadata,
                edge_metadata,
            } => {
                let result = self
                    .create_relationship_with_full_metadata(
                        EdgeMutation {
                            cell_id: plan.cell_id,
                            edge_type,
                            src,
                            dst,
                            idempotency_key: plan.idempotency_key,
                        },
                        src_metadata,
                        dst_metadata,
                        edge_metadata,
                    )
                    .await?;
                Ok(QueryOutput::Write(CommitResult {
                    epoch: result.epoch,
                    already_existed: result.already_created,
                }))
            }
            PhysicalQueryPlan::OutDegreeCounter { edge_type, src } => {
                let count = if let Some(read_epoch) = plan.read_epoch {
                    self.out_degree_at(&plan.cell_id, &edge_type, src, read_epoch).await?
                } else {
                    self.out_degree(&plan.cell_id, &edge_type, src).await?
                };
                Ok(QueryOutput::Count(count))
            }
            PhysicalQueryPlan::OutNeighbors { edge_type, src } => {
                let read_epoch = match plan.read_epoch {
                    Some(read_epoch) => read_epoch,
                    None => self.current_epoch(&plan.cell_id).await?,
                };
                let vertices = self
                    .out_neighbors_window_at(&plan.cell_id, &edge_type, src, read_epoch, plan.result_window, Some(&budget))
                    .await?;
                budget.check("query_out_neighbors")?;
                Ok(QueryOutput::Vertices(vertices))
            }
            PhysicalQueryPlan::EdgeExistsToCount { edge_type, src, dst } => {
                let exists = self.query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch).await?;
                Ok(QueryOutput::Count(u64::from(exists)))
            }
            PhysicalQueryPlan::EdgeExistsToVertices { edge_type, src, dst } => {
                let exists = self.query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch).await?;
                if exists {
                    Ok(QueryOutput::Vertices(self.apply_query_window(vec![dst], plan.result_window)?))
                } else {
                    Ok(QueryOutput::Vertices(Vec::new()))
                }
            }
            PhysicalQueryPlan::EdgeExistsToBool { edge_type, src, dst } => {
                let exists = self.query_edge_exists(&plan.cell_id, &edge_type, src, dst, plan.read_epoch).await?;
                Ok(QueryOutput::Bool(exists))
            }
            PhysicalQueryPlan::ReachableVertices {
                edge_type,
                src,
                min_hops,
                max_hops,
                return_count,
            } => {
                let read_epoch = match plan.read_epoch {
                    Some(read_epoch) => read_epoch,
                    None => self.current_epoch(&plan.cell_id).await?,
                };
                let vertices = self
                    .reachable_vertices_in_hop_range_at(&plan.cell_id, &edge_type, src, (min_hops, max_hops), read_epoch, &budget)
                    .await?
                    .0;
                budget.check("query_reachable")?;
                if return_count {
                    Ok(QueryOutput::Count(vertices.len() as u64))
                } else {
                    Ok(QueryOutput::Vertices(self.apply_query_window(vertices, plan.result_window)?))
                }
            }
        }
    }

    async fn validate_executable_query_plan(&self, plan: &QueryPlan) -> Result<()> {
        plan.validate_for_execution()?;
        if !plan.is_write() {
            if let Some(read_epoch) = plan.read_epoch {
                let current_epoch = self.current_epoch(&plan.cell_id).await?;
                if read_epoch > current_epoch {
                    return Err(GraphError::SnapshotAhead {
                        cell_id: plan.cell_id.clone(),
                        read_epoch,
                        current_epoch,
                    });
                }
                if read_epoch != current_epoch {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Other,
                        dialect: "GraphQueryPlan",
                        feature: "stale query plans are not pinned SlateDB snapshots".to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn query_read_epoch(&self, context: &QueryContext) -> Result<StorageSequence> {
        if let Some(read_epoch) = context.validated_read_epoch() {
            return Ok(read_epoch);
        }
        self.current_epoch(&context.cell_id).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_pattern_groups(
        &self,
        cell_id: &str,
        groups: &[RowMatchGroup],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        self.match_row_pattern_groups_limited(cell_id, groups, read_epoch, budget, None)
            .await
    }

    /// [`Self::match_row_pattern_groups`] that stops once the last group has
    /// produced `row_limit` rows. Rows from one seed are counted after the
    /// group's predicate, so the bound is on filtered rows; the caller still
    /// truncates, since one seed may contribute several rows past the bound.
    #[cfg(feature = "opencypher")]
    async fn match_row_pattern_groups_limited(
        &self,
        cell_id: &str,
        groups: &[RowMatchGroup],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        row_limit: Option<usize>,
    ) -> Result<Vec<BindingRow>> {
        if groups.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "MATCH requires at least one executable pattern".to_string(),
            });
        }

        let groups = budget
            .read_only_io(
                "cypher_plan_groups",
                self.optimize_row_match_groups_with_stats(cell_id, groups, read_epoch),
            )
            .await?;
        let mut rows = vec![BindingRow::default()];
        let last_group = groups.len() - 1;
        for (group_index, group) in groups.iter().enumerate() {
            let group_limit = row_limit.filter(|_| group_index == last_group);
            budget.check("cypher_match_group")?;
            if group.patterns.is_empty() {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Pattern,
                    dialect: "OpenCypher",
                    feature: "MATCH requires at least one executable pattern".to_string(),
                });
            }

            let mut group_rows = Vec::new();
            for row in rows {
                if group_limit.is_some_and(|limit| group_rows.len() >= limit) {
                    break;
                }
                let seeded = if let Some(seed_rows) = self.node_id_predicate_seed_rows(group, budget, &row)? {
                    Some(seed_rows)
                } else if let Some(seed_rows) = self
                    .relationship_predicate_seed_rows(cell_id, group, read_epoch, budget, &row)
                    .await?
                {
                    Some(seed_rows)
                } else if let Some(seed_rows) = self
                    .node_equality_predicate_seed_rows(cell_id, group, read_epoch, budget, &row)
                    .await?
                {
                    Some(seed_rows)
                } else {
                    self.prefix_predicate_seed_rows(cell_id, group, read_epoch, budget, &row).await?
                };
                let limited_scan = match (&seeded, group_limit) {
                    (None, Some(_)) => self.limited_node_scan_candidates(cell_id, group, read_epoch, budget, &row).await?,
                    _ => None,
                };
                let wanted = group_limit.map(|limit| limit.saturating_sub(group_rows.len()));
                let mut matches = Vec::new();
                if let Some((node, candidates)) = limited_scan {
                    // An unanchored node under a bound: the index supplied ids
                    // alone, so read their records a batch at a time and stop
                    // once enough pass. A batch is at least
                    // `LIMITED_NODE_SCAN_BATCH` so a selective predicate does
                    // not degrade into one round trip per candidate.
                    let batch = wanted.unwrap_or(1).max(LIMITED_NODE_SCAN_BATCH);
                    for chunk in candidates.chunks(batch) {
                        let found = self
                            .match_node_row_candidates(cell_id, node, read_epoch, budget, chunk.to_vec())
                            .await?;
                        let joined = found.iter().filter_map(|matched| row.join(matched)).collect();
                        extend_with_predicate_matches(group.predicate.as_ref(), joined, &mut matches, budget)?;
                        if wanted.is_some_and(|wanted| matches.len() >= wanted) {
                            break;
                        }
                    }
                } else if let (Some(wanted), Some(edge)) = (wanted, bounded_edge_group_pattern(group)) {
                    // A single hop under a bound: each seed expands through a
                    // traversal that joins, filters and counts the rows itself,
                    // so `WITH r LIMIT n` stops after about n relationships
                    // even when one vertex has more than the intermediate-row
                    // limit.
                    for seed in seeded.unwrap_or_else(|| vec![row.clone()]) {
                        let Some(RowPattern::Edge(bound_edge)) = constrain_row_pattern(edge, &seed)? else {
                            continue;
                        };
                        let bound = EdgeRowBound {
                            input: &seed,
                            predicate: group.predicate.as_ref(),
                            wanted: wanted - matches.len(),
                        };
                        let expanded = self
                            .match_edge_row_pattern(cell_id, &bound_edge, read_epoch, budget, Some(bound))
                            .await?;
                        matches.extend(expanded);
                        if matches.len() >= wanted {
                            break;
                        }
                    }
                } else {
                    // Without a bound the seeds expand together. With one, they
                    // expand a bound's worth at a time and stop once enough rows
                    // pass the predicate, so `LIMIT n` costs about n matches,
                    // not every match of the pattern.
                    let initial_rows = seeded.unwrap_or_else(|| vec![row.clone()]);
                    let seed_chunk = wanted.map_or(initial_rows.len().max(1), |wanted| wanted.max(1));
                    let mut initial_rows = initial_rows.into_iter().peekable();
                    while initial_rows.peek().is_some() {
                        let seeds = initial_rows.by_ref().take(seed_chunk).collect::<Vec<_>>();
                        let expanded = self
                            .match_row_patterns_from_rows(cell_id, &group.patterns, read_epoch, budget, seeds)
                            .await?;
                        extend_with_predicate_matches(group.predicate.as_ref(), expanded, &mut matches, budget)?;
                        if wanted.is_some_and(|wanted| matches.len() >= wanted) {
                            break;
                        }
                    }
                }

                if matches.is_empty() && group.optional {
                    let mut optional_row = row;
                    optional_row.mark_optional_group_nulls(group);
                    self.push_binding_row(&mut group_rows, optional_row, "cypher_optional_match_rows")?;
                } else {
                    for matched in matches {
                        self.push_binding_row(&mut group_rows, matched, "cypher_match_group_rows")?;
                    }
                }
            }

            rows = group_rows;
            self.ensure_query_intermediate_rows("cypher_match_group_pipeline_rows", rows.len())?;
            if rows.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    /// [`Self::relationships_for_edge_at`] as `(relationship id, metadata)`,
    /// before the merge [`Self::native_path_relationships_at`] applies: each
    /// identified relationship carries only its own properties, and the
    /// structural edge (id `None`) appears alone, or beside identified
    /// relationships when it has properties of its own. The ordered
    /// relationship walk needs to know which record a property came from --
    /// which is also its only caller, so it is gated to that walk's feature
    /// rather than to `opencypher`: an `opencypher`-only build would compile
    /// it with nothing calling it and fail `-D dead-code`.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) async fn unmerged_relationships_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(Option<RelationshipId>, EdgeMetadata)>> {
        Ok(self
            .relationships_for_edge_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?
            .into_iter()
            .map(|(relationship, metadata)| (relationship.relationship_id, metadata))
            .collect())
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn native_path_relationships_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(Option<RelationshipId>, EdgeMetadata)>> {
        let relationships = self
            .relationships_for_edge_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?;
        Ok(native_path_rows(relationships))
    }

    /// [`Self::native_path_relationships_at`] for a pair an expansion listed
    /// from the adjacency of the snapshot being read, at `read_epoch`: the
    /// listing already proved the structural edge, so it is not checked again.
    #[cfg(feature = "opencypher")]
    #[allow(dead_code)] // Used only when `experimental-cypher-engine` is also enabled.
    pub(crate) async fn native_path_relationships_of_listed_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(Option<RelationshipId>, EdgeMetadata)>> {
        let relationships = self
            .relationships_for_edge_with_presence_at(cell_id, edge_type, src, dst, read_epoch, budget, EdgePresence::Listed)
            .await?;
        Ok(native_path_rows(relationships))
    }

    /// Consecutive incoming neighbours have distinct sources. Seek their canonical
    /// records in storage-key order using one iterator per namespace. The bounds,
    /// seeks and single lookahead avoid walking unrelated sources between probes.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) async fn listed_incoming_relationship_rows(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell: &str,
        edge_type: &str,
        dst: VertexId,
        sources: &[VertexId],
        budget: &QueryBudget,
    ) -> Result<Vec<NativePathRows>> {
        debug_assert!(!sources.is_empty() && sources.windows(2).all(|ids| ids[0] < ids[1]));
        validate_component("cell_id", cell)?;
        validate_component("edge_type", edge_type)?;
        let record_keys: Vec<_> = sources
            .iter()
            .map(|&src| keys::relationship_edge_prefix(cell, edge_type, src, dst))
            .collect();
        let metadata_keys: Vec<_> = sources.iter().map(|&src| keys::edge_metadata(cell, edge_type, src, dst)).collect();
        let reads = async {
            futures::try_join!(
                self.seek_listed_keys(snapshot, &record_keys, true, budget, decode_relationship_record),
                self.seek_listed_keys(snapshot, &metadata_keys, false, budget, decode_edge_metadata),
            )
        };
        let (records, metadata) = budget.read_only_io("cypher_incoming_relationship_batch", Box::pin(reads)).await?;
        let mut rows = Vec::with_capacity(sources.len());
        for ((&src, records), mut meta) in sources.iter().zip(records).zip(metadata) {
            budget.check("cypher_incoming_relationship_rows")?;
            rows.push(native_path_rows(relationship_rows_for_edge(
                edge_type,
                src,
                dst,
                records,
                meta.pop().unwrap_or_default(),
            )));
        }
        Ok(rows)
    }

    #[cfg(feature = "experimental-cypher-engine")]
    async fn seek_listed_keys<T>(
        &self,
        snapshot: &GraphStorageSnapshot,
        keys: &[String],
        prefixes: bool,
        budget: &QueryBudget,
        decode: impl Fn(&str, &[u8]) -> Result<T>,
    ) -> Result<Vec<Vec<T>>> {
        use std::ops::Bound;
        let first = keys[0].as_bytes();
        let last = keys.last().expect("batch is nonempty").as_bytes();
        let shared = first.iter().zip(last).take_while(|(a, b)| a == b).count();
        let mut end = last[shared..].to_vec();
        end.push(0xff);
        // A sparse seek needs at most one block in flight. SlateDB rounds the
        // positive byte target up to a whole block.
        let options = remote_scan_options_for_expected_items(1)
            .with_read_ahead_bytes(1)
            .with_max_fetch_tasks(1);
        let mut iter = snapshot
            .scan_prefix_with_options(
                &first[..shared],
                (Bound::Included(first[shared..].to_vec()), Bound::Excluded(end)),
                &options,
            )
            .await?;
        let mut next = iter.next().await?;
        let mut output = Vec::with_capacity(keys.len());
        let mut retained = 0;
        for key in keys {
            budget.check("cypher_incoming_relationship_seek")?;
            if next.as_ref().is_some_and(|kv| kv.key.as_ref() < key.as_bytes()) {
                iter.seek(key.as_bytes()).await?;
                next = iter.next().await?;
            }
            let mut matches = Vec::new();
            while let Some(kv) = next.as_ref().filter(|kv| {
                if prefixes {
                    kv.key.starts_with(key.as_bytes())
                } else {
                    kv.key.as_ref() == key.as_bytes()
                }
            }) {
                budget.check("cypher_incoming_relationship_records")?;
                matches.push(decode(&String::from_utf8_lossy(&kv.key), &kv.value)?);
                retained += 1;
                if prefixes {
                    self.ensure_query_index_candidates("cypher_relationship_edge_records", matches.len())?;
                }
                self.ensure_query_intermediate_rows("cypher_incoming_relationship_records", retained)?;
                next = iter.next().await?;
            }
            output.push(matches);
        }
        Ok(output)
    }

    /// [`Self::native_path_relationships_at`] for every listed out-neighbour
    /// of one source at once: one prefix scan of the source's relationship
    /// records and one of its edge metadata, instead of an existence check, a
    /// record scan and a metadata read per edge. On a hub that is two scans
    /// for thousands of edges rather than three round trips each.
    ///
    /// `dsts` must be the source's out-neighbours over `edge_type`, listed
    /// from the snapshot this runs under at `read_epoch`. The listing applies
    /// the rule `edge_exists_in_storage_snapshot` applies -- an `out` edge key,
    /// or a visible segment destination -- so every listed pair already
    /// passed the per-edge existence check. Records under the source for
    /// destinations not in `dsts` (left by a removed edge) are skipped, as the
    /// per-edge path skips them by not reading them.
    ///
    /// Answers each destination's rows in `dsts` order. `None` when
    /// `read_epoch` is not the latest epoch: historical reads keep the
    /// per-edge path, which reads record metadata as of that epoch.
    #[cfg(feature = "opencypher")]
    #[allow(dead_code)] // Used only when `experimental-cypher-engine` is also enabled.
    pub(crate) async fn native_path_relationships_from_source_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dsts: &[VertexId],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<Vec<NativePathRows>>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }
        let wanted = dsts.iter().copied().collect::<BTreeSet<_>>();
        // Edge metadata holds one key per live edge (removing an edge removes
        // it), so the listed destinations bound that scan and it is cached
        // like the listing that produced them. Relationship records do not:
        // a pair can carry any number of parallel relationships, so their
        // count says nothing about how many blocks the scan would admit, and
        // one multi-edge-heavy source could sweep the shared cache. Records
        // are read ahead in large requests and not admitted.
        let metadata_scan_options = || {
            let options = remote_scan_options_for_expected_items(wanted.len() as u64);
            if options.cache_blocks {
                options
            } else {
                remote_bulk_scan_options()
            }
        };
        let records = async {
            let prefix = keys::relationship_source_prefix(cell_id, edge_type, src);
            let mut iter = self
                .storage
                .scan_prefix_with_options(prefix.as_bytes(), None, &remote_bulk_scan_options())
                .await?;
            let mut by_dst: BTreeMap<VertexId, Vec<RelationshipRecord>> = BTreeMap::new();
            let mut kept = 0_usize;
            while let Some(kv) = iter.next().await? {
                budget.check("cypher_relationship_source_records")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let record = decode_relationship_record(&key, &kv.value)?;
                if !wanted.contains(&record.dst) {
                    continue;
                }
                let records = by_dst.entry(record.dst).or_default();
                records.push(record);
                // The per-edge path bounds each pair's records the same way,
                // and the source as a whole is bounded as the expansion's rows
                // are, so a hub stops here rather than after it is all held.
                self.ensure_query_index_candidates("cypher_relationship_edge_records", records.len())?;
                kept += 1;
                self.ensure_query_intermediate_rows("cypher_relationship_source_records", kept)?;
            }
            Ok::<_, GraphError>(by_dst)
        };
        let structural_metadata = async {
            let prefix = keys::edge_metadata_source_prefix(cell_id, edge_type, src);
            let mut iter = self
                .storage
                .scan_prefix_with_options(prefix.as_bytes(), None, &metadata_scan_options())
                .await?;
            let mut by_dst = BTreeMap::new();
            while let Some(kv) = iter.next().await? {
                budget.check("cypher_edge_metadata_source")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let dst = key
                    .rsplit('/')
                    .next()
                    .and_then(|dst| dst.parse::<VertexId>().ok())
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: key.clone(),
                        reason: "edge metadata key does not end in a destination vertex id".to_string(),
                    })?;
                if wanted.contains(&dst) {
                    by_dst.insert(dst, decode_edge_metadata(&key, &kv.value)?);
                }
            }
            Ok::<_, GraphError>(by_dst)
        };
        let (records, structural_metadata) = budget
            .read_only_io(
                "cypher_relationship_source_records",
                Box::pin(async { futures::try_join!(records, structural_metadata) }),
            )
            .await?;
        // Each destination's rows are built once and handed to every listed
        // occurrence of it: a repeated type (`[:KNOWS|KNOWS]`) lists a pair
        // twice, and both must read the same relationships.
        let mut rows_by_dst: BTreeMap<VertexId, NativePathRows> = BTreeMap::new();
        let mut answered = Vec::with_capacity(dsts.len());
        for &dst in dsts {
            budget.check("cypher_relationship_source_rows")?;
            let rows = rows_by_dst.entry(dst).or_insert_with(|| {
                native_path_rows(relationship_rows_for_edge(
                    edge_type,
                    src,
                    dst,
                    records.get(&dst).cloned().unwrap_or_default(),
                    structural_metadata.get(&dst).cloned().unwrap_or_default(),
                ))
            });
            answered.push(rows.clone());
        }
        Ok(Some(answered))
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_predicate_seed_rows(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }

        // An indexed relationship equality may be written either in WHERE
        // (`WHERE r.chunk_id = $id`) or directly in the pattern
        // (`[r:RELATES {chunk_id: $id}]`). Both are the same logical anchor.
        // Treating only the WHERE form as an index seed made the latter begin
        // from every matching endpoint node, which is catastrophic for the
        // production chunk-retrieval UNION on large tenant graphs.
        let predicate_constraints = group
            .predicate
            .as_ref()
            .map(row_predicate_property_equality_constraints)
            .unwrap_or_default();
        let mut seeds = Vec::<(&RowEdgePattern, RowPredicatePropertyEqualityConstraint, u64)>::new();
        for pattern in &group.patterns {
            let RowPattern::Edge(edge) = pattern else {
                continue;
            };
            let Some(binding) = edge.binding.as_ref() else {
                continue;
            };
            // This seed binds the stored source/destination directly. An
            // undirected pattern must go through the two-orientation matcher.
            if edge.direction == EdgeDirection::Bidirectional || edge.hop_range.is_some() || input.relationships.contains_key(binding) {
                continue;
            }

            let mut constraints = predicate_constraints
                .iter()
                .filter(|constraint| constraint.binding == *binding)
                .cloned()
                .collect::<Vec<_>>();
            constraints.extend(
                edge.properties
                    .iter()
                    .map(|(property, value)| RowPredicatePropertyEqualityConstraint {
                        binding: binding.clone(),
                        property: property.clone(),
                        values: vec![value.clone()],
                    }),
            );

            // Inline properties live in a BTreeMap, but lexical property order
            // is unrelated to selectivity: rank every valid equality the same
            // way the optimizer does, by how many values it accepts.
            for constraint in constraints {
                if !constraint.values.iter().all(VertexPropertyValue::is_scalar_indexable) {
                    continue;
                }
                let estimate = crate::shard::query_optimizer::edge_property_equality_estimate(&constraint.values);
                seeds.push((edge, constraint, estimate));
            }
        }
        // `sort_by_key` is stable, so absent/equal statistics retain parser
        // order, but execution is not committed to that guess: an over-budget
        // probe falls through to the next property instead of failing.
        seeds.sort_by_key(|(_, _, estimate)| *estimate);
        if seeds.is_empty() {
            return Ok(None);
        }

        // Race bounded probes instead of scanning them serially. With stale or
        // absent statistics, a selective index can finish without waiting for
        // every earlier broad index to consume a full candidate allowance and
        // the shared wall-clock budget. Dropping the race cancels unfinished
        // probes as soon as one usable seed completes.
        let mut probes = stream::FuturesUnordered::new();
        for (edge, constraint, _estimate) in seeds {
            probes.push(async move {
                let pairs = self
                    .try_scan_edge_property_values_at(
                        cell_id,
                        &edge.edge_type,
                        &constraint.property,
                        &constraint.values,
                        budget,
                        self.limits.max_query_index_candidates,
                    )
                    .await?;
                Ok::<_, GraphError>((edge, pairs))
            });
        }

        let mut selected = None;
        while let Some(probe) = probes.next().await {
            let (edge, pairs) = probe?;
            if let Some(pairs) = pairs {
                selected = Some((edge, pairs));
                break;
            }
        }
        let Some((edge, pairs)) = selected else {
            self.ensure_query_index_candidates(
                "cypher_edge_predicate_index_candidates",
                self.limits.max_query_index_candidates.saturating_add(1),
            )?;
            return Ok(None);
        };

        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let candidates = {
            let mut state = EdgeRowMatchState::new(cell_id, read_epoch, edge, &mut metadata_cache, &mut edge_metadata_cache, budget);
            for (src, dst) in pairs {
                budget.check("cypher_edge_predicate_index_rows")?;
                self.push_matching_edge_row(edge, src, dst, &mut state).await?;
            }
            self.finish_edge_rows(state).await?
        };

        let mut rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            budget.check("cypher_edge_predicate_index_join")?;
            if let Some(joined) = input.join(&candidate) {
                self.push_binding_row(&mut rows, joined, "cypher_edge_predicate_index_seed_rows")?;
            }
        }
        Ok(Some(rows))
    }

    /// `MATCH (n) WHERE n.id IN $ids`: bind the listed vertices directly.
    ///
    /// Ahead of the property and prefix seeds because an id is the tightest
    /// anchor there is — it names the row instead of narrowing to it — and it
    /// costs no index read to produce. Each seeded row then reaches the node
    /// matcher as a plain id seek, the same plan an inline `(n {id: ...})`
    /// already gets, and the group predicate filters what comes back.
    ///
    /// An id in the list that names no stored vertex binds the same way an
    /// inline `(n {id: <unstored>})` does today. That is deliberate: a vertex
    /// here has no existence separate from what constrains it — an id can be an
    /// edge endpoint with no metadata record of its own — so a bare `(n)` with
    /// no labels and no properties has nothing to reject the binding with, by
    /// either spelling. Changing that is a change to node-pattern semantics,
    /// not to this seed.
    #[cfg(feature = "opencypher")]
    fn node_id_predicate_seed_rows(
        &self,
        group: &RowMatchGroup,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        let Some(predicate) = group.predicate.as_ref() else {
            return Ok(None);
        };
        let Some((binding, ids)) = row_predicate_node_id_values(predicate) else {
            return Ok(None);
        };
        if input.values.contains_key(&binding) || input.null_values.contains(&binding) {
            return Ok(None);
        }
        // The binding has to name a node this group matches, and one whose id
        // the pattern does not already pin: an inline `{id: ...}` is the
        // stricter anchor and `constrain_row_pattern` applies it anyway.
        let seedable_node = |node: &RowNodePattern| node.binding.as_deref() == Some(binding.as_str()) && node.id.is_none();
        let seedable = group.patterns.iter().any(|pattern| match pattern {
            RowPattern::Node(node) => seedable_node(node),
            RowPattern::Edge(edge) => seedable_node(&edge.src) || seedable_node(&edge.dst),
        });
        if !seedable {
            return Ok(None);
        }
        self.ensure_query_index_candidates("cypher_node_id_seed_candidates", ids.len())?;
        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            budget.check("cypher_node_id_seed_bind")?;
            let mut row = input.clone();
            if row.bind(Some(&binding), id) {
                self.push_binding_row(&mut rows, row, "cypher_node_id_seed_rows")?;
            }
        }
        Ok(Some(rows))
    }

    /// Candidate ids for a bounded, unanchored single-node group, from the
    /// node's label or property index and without reading any vertex record.
    /// [`Self::match_row_pattern_groups_limited`] reads their records a batch
    /// at a time, applies the pattern and the group predicate, and stops once
    /// enough rows pass. Without this, `MATCH (n:L) WITH n LIMIT 10` read every
    /// `L` record before keeping ten. `None` when another seed or an all-vertex
    /// scan has to match the group instead.
    #[cfg(feature = "opencypher")]
    async fn limited_node_scan_candidates<'group>(
        &self,
        cell_id: &str,
        group: &'group RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<(&'group RowNodePattern, Vec<VertexId>)>> {
        let [RowPattern::Node(node)] = group.patterns.as_slice() else {
            return Ok(None);
        };
        let Some(binding) = node.binding.as_deref() else {
            return Ok(None);
        };
        if node.id.is_some() || input.values.contains_key(binding) || input.null_values.contains(binding) {
            return Ok(None);
        }
        Ok(self
            .candidate_vertex_ids(cell_id, node, read_epoch, budget)
            .await?
            .map(|candidates| (node, candidates)))
    }

    #[cfg(feature = "opencypher")]
    async fn node_equality_predicate_seed_rows(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        let Some((binding, vertices)) = self
            .node_equality_predicate_seed_vertices(
                cell_id,
                group,
                read_epoch,
                budget,
                input,
                NodeEqualityProbe::PredicateEqualities(256.min(self.limits.max_query_intermediate_rows)),
            )
            .await?
        else {
            return Ok(None);
        };
        let mut rows = Vec::with_capacity(vertices.len());
        for vertex in vertices {
            budget.check("cypher_node_equality_seed_bind")?;
            let mut row = input.clone();
            if row.bind(Some(&binding), vertex) {
                self.push_binding_row(&mut rows, row, "cypher_node_equality_seed_rows")?;
            }
        }
        Ok(Some(rows))
    }

    #[cfg(feature = "opencypher")]
    async fn node_equality_predicate_seed_vertices(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
        probe: NodeEqualityProbe,
    ) -> Result<Option<(String, Vec<VertexId>)>> {
        let (max_probe_entries, ordered) = match probe {
            NodeEqualityProbe::Ordered(max) => (max, true),
            NodeEqualityProbe::PredicateEqualities(max) => (max, false),
        };
        let mut constraints = group
            .predicate
            .as_ref()
            .map(row_predicate_property_equality_constraints)
            .unwrap_or_default();
        let eligible = |constraint: &RowPredicatePropertyEqualityConstraint| {
            !constraint.values.is_empty()
                && constraint.values.iter().all(VertexPropertyValue::is_scalar_indexable)
                && !input.values.contains_key(&constraint.binding)
                && !input.null_values.contains(&constraint.binding)
                && group.patterns.iter().any(|pattern| {
                    let eligible_node = |node: &RowNodePattern| node.binding.as_ref() == Some(&constraint.binding) && node.id.is_none();
                    match pattern {
                        RowPattern::Node(node) => eligible_node(node),
                        RowPattern::Edge(edge) => eligible_node(&edge.src) || eligible_node(&edge.dst),
                    }
                })
        };
        constraints.retain(&eligible);
        // Keep the existing predicate probe allowance intact. Broad inline
        // tenant constraints must not dilute a previously usable WHERE seed.
        let use_inline = ordered && constraints.is_empty();
        for pattern in &group.patterns {
            if !use_inline {
                break;
            }
            let mut add_node = |node: &RowNodePattern| {
                let Some(binding) = node.binding.as_ref().filter(|_| node.id.is_none()) else {
                    return;
                };
                for (property, value) in &node.properties {
                    let constraint = RowPredicatePropertyEqualityConstraint {
                        binding: binding.clone(),
                        property: property.clone(),
                        values: vec![value.clone()],
                    };
                    if !constraints.contains(&constraint) {
                        constraints.push(constraint);
                    }
                }
            };
            match pattern {
                RowPattern::Node(node) => add_node(node),
                RowPattern::Edge(edge) => {
                    add_node(&edge.src);
                    add_node(&edge.dst);
                }
            }
        }
        let constraints = constraints
            .iter()
            .filter(|constraint| eligible(constraint))
            .take(QUERY_PROPERTY_PROBE_CANDIDATES)
            .collect::<Vec<_>>();
        if constraints.is_empty() || read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }

        // Runtime probes must also consider single equalities. Divide the
        // candidate allowance across constraints, while bounding equality
        // alternatives separately. Prefix opens and the terminal poll do not
        // produce candidates: charging them to the candidate allowance made a
        // mostly-absent batched OR exhaust its probe before reaching a later
        // selective value. An incomplete probe must still fall back, never seed
        // a partial result set. Ordered scans additionally admit inline values.
        let candidates = constraints
            .iter()
            .map(|constraint| {
                (
                    constraint.property.clone(),
                    constraint
                        .values
                        .iter()
                        .flat_map(equivalent_property_index_keys)
                        // The probe rejects a candidate that reaches its own
                        // alternatives cap, so truncating here cannot turn an
                        // incomplete probe into a complete one.
                        .take(QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let Some((index, vertices)) = self
            .probe_property_index_candidates(cell_id, &candidates, max_probe_entries, budget)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((constraints[index].binding.clone(), vertices)))
    }

    /// Rank equality candidates by bounded index walks and return the vertices
    /// of the first one that proves selective.
    ///
    /// Each candidate gets a slice of one fixed candidate allowance. A
    /// candidate that exhausts its slice is too broad to anchor on and is
    /// discarded; one that finishes inside it has both proven itself selective
    /// and produced its exact vertex set. The walks race, so an answer costs
    /// about what the narrowest index takes to read rather than the sum.
    ///
    /// At most [`QUERY_PROPERTY_PROBE_CANDIDATES`] are ranked, which is also
    /// the bound on concurrent walks; anything past that is ignored.
    ///
    /// A candidate's keys must cover every value it stands for. One with no
    /// keys is declined rather than answered with an empty vertex set, because
    /// the caller cannot tell those apart and would drop every matching row.
    ///
    /// This is what lets a query plan well with no statistics at all: nothing
    /// here consults a stored cardinality, it measures the index instead.
    ///
    /// `candidates` pairs a property with the already-encoded index keys that
    /// satisfy it, so this never interprets a value. Callers holding a
    /// `VertexPropertyValue` expand it with `equivalent_property_index_keys`.
    /// The returned index selects the winning candidate, because the caller,
    /// not this walk, knows what a property was standing in for.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn probe_property_index_candidates(
        &self,
        cell_id: &str,
        candidates: &[(String, Vec<String>)],
        max_probe_entries: usize,
        budget: &QueryBudget,
    ) -> Result<Option<(usize, Vec<VertexId>)>> {
        // Enforced here rather than trusted from the caller: this is where the
        // concurrent walks are started, and the experimental engine offers
        // every equality conjunct it found rather than a pre-trimmed list.
        let candidates = &candidates[..candidates.len().min(QUERY_PROPERTY_PROBE_CANDIDATES)];
        if candidates.is_empty() {
            return Ok(None);
        }
        let allowance = self.limits.max_query_index_candidates.min(max_probe_entries) / candidates.len();
        let alternative_allowance = QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES / candidates.len();
        let concurrency = candidates.len();
        let mut probes = stream::iter(candidates.iter().enumerate())
            .map(|(index, (property, encoded_keys))| async move {
                // Nothing to walk is not the same as nothing to find. A
                // candidate with no index keys proves nothing about the
                // vertices it matches, and a completed empty walk would look
                // exactly like a property that is selective to zero rows.
                if encoded_keys.is_empty() {
                    return Ok::<_, GraphError>(None);
                }
                let mut remaining = allowance;
                let mut vertices = BTreeSet::new();
                let mut encodings = BTreeSet::new();
                let mut complete = true;
                let property_prefix = self.vertex_property_index_prefix_at(cell_id, property, budget).await?;
                'keys: for encoded in encoded_keys {
                    if encodings.contains(encoded) {
                        continue;
                    }
                    if encodings.len() >= alternative_allowance {
                        complete = false;
                        break 'keys;
                    }
                    encodings.insert(encoded.clone());
                    budget.check("cypher_node_equality_seed_seek")?;
                    let prefix = keys::vertex_property_index_value_prefix(&property_prefix, encoded);
                    let mut iter = budget
                        .read_only_io(
                            "cypher_node_equality_seed_seek",
                            self.storage.scan_prefix_with_options(
                                prefix.as_bytes(),
                                None,
                                &remote_scan_options_for_expected_items(allowance as u64),
                            ),
                        )
                        .await?;
                    loop {
                        budget.check("cypher_node_equality_seed_probe")?;
                        let Some(kv) = budget
                            .read_only_io("cypher_node_equality_seed_probe", async {
                                iter.next().await.map_err(GraphError::from)
                            })
                            .await?
                        else {
                            break;
                        };
                        let Some(next) = remaining.checked_sub(1) else {
                            complete = false;
                            break 'keys;
                        };
                        remaining = next;
                        let key = String::from_utf8_lossy(&kv.key);
                        vertices.insert(decode_u64(&key, &kv.value)?);
                    }
                }
                if !complete {
                    return Ok::<_, GraphError>(None);
                }
                Ok(Some((index, vertices.into_iter().collect::<Vec<_>>())))
            })
            .buffer_unordered(concurrency)
            .boxed();
        while let Some(probe) = probes.try_next().await? {
            if let Some((index, vertices)) = probe {
                tracing::Span::current().record("hydradb.query.runtime_property_index", candidates[index].0.as_str());
                return Ok(Some((index, vertices)));
            }
        }
        Ok(None)
    }

    #[cfg(feature = "opencypher")]
    async fn prefix_predicate_seed_rows(
        &self,
        cell_id: &str,
        group: &RowMatchGroup,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        input: &BindingRow,
    ) -> Result<Option<Vec<BindingRow>>> {
        let Some(RowPredicate::StartsWith {
            expression: RowExpression::Property { binding, property },
            prefix,
        }) = group.predicate.as_ref()
        else {
            return Ok(None);
        };
        if input.values.contains_key(binding) || read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }
        let binding_is_present = group.patterns.iter().any(|pattern| match pattern {
            RowPattern::Node(node) => node.binding.as_deref() == Some(binding.as_str()),
            RowPattern::Edge(edge) => {
                edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
            }
        });
        if !binding_is_present {
            return Ok(None);
        }

        let encoded_prefix = encode_vertex_property_value_key(&VertexPropertyValue::String(prefix.clone()));
        let index_prefix = format!(
            "{}{}",
            self.vertex_property_index_prefix_at(cell_id, property, budget).await?,
            encoded_prefix
        );
        let mut iter = self.scan_remote_prefix(&index_prefix).await?;
        let mut vertices = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_property_prefix_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, indexed_property, _encoded, vertex_id) = parse_vertex_property_index_key(&key)?;
            if indexed_property != *property {
                continue;
            }
            vertices.insert(vertex_id);
            self.ensure_query_index_candidates("cypher_vertex_property_prefix_candidates", vertices.len())?;
        }

        let mut rows = Vec::with_capacity(vertices.len());
        for vertex_id in vertices {
            budget.check("cypher_vertex_property_prefix_bind")?;
            let mut row = input.clone();
            if row.bind(Some(binding), vertex_id) {
                rows.push(row);
            }
        }
        Ok(Some(rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_patterns_from_rows(
        &self,
        cell_id: &str,
        patterns: &[RowPattern],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        mut rows: Vec<BindingRow>,
    ) -> Result<Vec<BindingRow>> {
        let initial_bindings = common_binding_row_bound_names(&rows);
        let patterns = budget
            .read_only_io(
                "cypher_plan_patterns",
                self.optimize_row_patterns_with_stats(cell_id, patterns, read_epoch, &initial_bindings),
            )
            .await?;
        for pattern in &patterns {
            budget.check("cypher_match_pipeline")?;
            if let Some(next_rows) = self
                .match_expand_into_pattern_from_rows(cell_id, pattern, read_epoch, budget, &rows)
                .await?
            {
                rows = next_rows;
                self.ensure_query_intermediate_rows("cypher_expand_into_rows", rows.len())?;
                if rows.is_empty() {
                    break;
                }
                continue;
            }
            if let Some(next_rows) = self
                .match_hash_join_pattern_from_rows(cell_id, pattern, read_epoch, budget, &rows)
                .await?
            {
                rows = next_rows;
                self.ensure_query_intermediate_rows("cypher_hash_join_rows", rows.len())?;
                if rows.is_empty() {
                    break;
                }
                continue;
            }
            let mut next_rows = Vec::new();
            for row in rows {
                let Some(bound_pattern) = constrain_row_pattern(pattern, &row)? else {
                    continue;
                };
                let matches = self.match_row_pattern(cell_id, &bound_pattern, read_epoch, budget).await?;
                for matched in matches {
                    budget.check("cypher_match_join")?;
                    if let Some(joined) = row.join(&matched) {
                        self.push_binding_row(&mut next_rows, joined, "cypher_match_join_rows")?;
                    }
                }
            }
            rows = next_rows;
            self.ensure_query_intermediate_rows("cypher_match_pipeline_rows", rows.len())?;
            if rows.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_expand_into_pattern_from_rows(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        rows: &[BindingRow],
    ) -> Result<Option<Vec<BindingRow>>> {
        let RowPattern::Edge(edge) = pattern else {
            return Ok(None);
        };
        if edge.direction == EdgeDirection::Bidirectional || edge.hop_range.is_some() || rows.is_empty() {
            return Ok(None);
        }

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let mut next_rows = Vec::new();
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();

        let mut groups: Vec<(usize, RowEdgePattern, Vec<BindingRow>)> = Vec::new();
        for (input_index, row) in rows.iter().enumerate() {
            budget.check("cypher_expand_into")?;
            let Some(RowPattern::Edge(bound_edge)) = constrain_row_pattern(pattern, row)? else {
                continue;
            };
            let (Some(src), Some(dst)) = (bound_edge.src.id, bound_edge.dst.id) else {
                return Ok(None);
            };
            let exists = if latest_snapshot {
                self.edge_exists(cell_id, &bound_edge.edge_type, src, dst).await?
            } else {
                self.edge_exists_at(cell_id, &bound_edge.edge_type, src, dst, read_epoch).await?
            };
            if !exists {
                continue;
            }
            let relationships = if let Some((property, value)) = first_indexable_property(&bound_edge.properties) {
                self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                    cell_id,
                    edge_type: &bound_edge.edge_type,
                    src,
                    dst,
                    property,
                    value,
                    read_epoch,
                    budget,
                })
                .await?
            } else {
                self.relationships_for_edge_at(cell_id, &bound_edge.edge_type, src, dst, read_epoch, budget)
                    .await?
            };
            let mut matches = Vec::new();
            for (relationship, relationship_metadata) in relationships {
                let Some(matched) = BindingRow::from_relationship(&bound_edge, relationship, relationship_metadata) else {
                    continue;
                };
                matches.push(matched);
            }
            if !matches.is_empty() {
                groups.push((input_index, bound_edge, matches));
            }
        }

        let wanted = groups
            .iter()
            .flat_map(|(_, _, matches)| matches.iter())
            .flat_map(|matched| matched.values.values().copied())
            .collect::<Vec<_>>();
        self.hydrate_vertex_metadata_cache_at(cell_id, &wanted, read_epoch, &mut metadata_cache, budget)
            .await?;

        for (input_index, bound_edge, matches) in groups {
            let row = &rows[input_index];
            for mut matched in matches {
                self.hydrate_binding_metadata(cell_id, read_epoch, &mut matched, &mut metadata_cache, budget)
                    .await?;
                self.hydrate_row_relationship_metadata(cell_id, read_epoch, &mut matched, &bound_edge, &mut edge_metadata_cache, budget)
                    .await?;
                if row_matches_edge_pattern(&matched, &bound_edge)? {
                    if let Some(joined) = row.join(&matched) {
                        self.push_binding_row(&mut next_rows, joined, "cypher_expand_into_rows")?;
                    }
                }
            }
        }
        Ok(Some(next_rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_hash_join_pattern_from_rows(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        rows: &[BindingRow],
    ) -> Result<Option<Vec<BindingRow>>> {
        if rows.len() <= 1 {
            return Ok(None);
        }
        let row_bindings = binding_rows_bound_names_union(rows);
        let pattern_bindings = row_pattern_bound_names(pattern);
        let join_bindings: Vec<_> = row_bindings.intersection(&pattern_bindings).cloned().collect();

        if join_bindings.is_empty() {
            let matches = self.match_row_pattern(cell_id, pattern, read_epoch, budget).await?;
            let mut next_rows = Vec::new();
            for row in rows {
                for matched in &matches {
                    budget.check("cypher_precomputed_cross_join")?;
                    if let Some(joined) = row.join(matched) {
                        self.push_binding_row(&mut next_rows, joined, "cypher_precomputed_cross_join_rows")?;
                    }
                }
            }
            return Ok(Some(next_rows));
        }

        if !hash_joinable_pattern(pattern) {
            return Ok(None);
        }
        let matches = if let RowPattern::Node(node) = pattern {
            // The incoming rows already identify the join vertices. Hydrating
            // the entire node pattern first discards selective upstream work.
            let binding = node.binding.as_ref().expect("node has a join binding");
            let vertices: Vec<_> = rows
                .iter()
                .filter_map(|row| row.values.get(binding).copied())
                .filter(|vertex| node.id.is_none_or(|id| id == *vertex))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if vertices.len() <= self.limits.max_query_index_candidates.min(256) {
                self.match_node_row_candidates(cell_id, node, read_epoch, budget, vertices).await?
            } else {
                self.match_row_pattern(cell_id, pattern, read_epoch, budget).await?
            }
        } else {
            self.match_row_pattern(cell_id, pattern, read_epoch, budget).await?
        };
        let mut matches_by_key = BTreeMap::<Vec<VertexId>, Vec<BindingRow>>::new();
        for matched in matches {
            if let Some(key) = binding_row_join_key(&matched, &join_bindings) {
                matches_by_key.entry(key).or_default().push(matched);
            }
        }

        let mut next_rows = Vec::new();
        for row in rows {
            budget.check("cypher_hash_join")?;
            let Some(key) = binding_row_join_key(row, &join_bindings) else {
                continue;
            };
            let Some(matches) = matches_by_key.get(&key) else {
                continue;
            };
            for matched in matches {
                if let Some(joined) = row.join(matched) {
                    self.push_binding_row(&mut next_rows, joined, "cypher_hash_join_rows")?;
                }
            }
        }
        Ok(Some(next_rows))
    }

    #[cfg(feature = "opencypher")]
    async fn match_row_pattern(
        &self,
        cell_id: &str,
        pattern: &RowPattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        validate_component("cell_id", cell_id)?;
        budget.check("cypher_match")?;
        match pattern {
            RowPattern::Node(node) => self.match_node_row_pattern(cell_id, node, read_epoch, budget).await,
            RowPattern::Edge(edge) => self.match_edge_row_pattern(cell_id, edge, read_epoch, budget, None).await,
        }
    }

    #[cfg(feature = "opencypher")]
    async fn match_node_row_pattern(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(vertices) = self.candidate_vertex_ids(cell_id, node, read_epoch, budget).await? else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "node-only MATCH requires an id, label, or property predicate".to_string(),
            });
        };
        self.match_node_row_candidates(cell_id, node, read_epoch, budget, vertices).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_node_row_candidates(
        &self,
        cell_id: &str,
        node: &RowNodePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        vertices: Vec<VertexId>,
    ) -> Result<Vec<BindingRow>> {
        self.ensure_query_index_candidates("cypher_node_candidates", vertices.len())?;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();

        // Each row keeps its candidate id: an anonymous node binds no variable,
        // so the row alone does not say which vertex it stands for.
        let mut pending = Vec::with_capacity(vertices.len());
        for vertex_id in vertices {
            budget.check("cypher_node_rows")?;
            if let Some(row) = BindingRow::from_node(node, vertex_id) {
                pending.push((vertex_id, row));
            }
        }

        // Every route that hands this matcher candidate ids (an inline id, a
        // `WHERE n.id = ...` seed, an index or a scan) meets here. An id names
        // a candidate; only its canonical record makes it a vertex, so the
        // records read here both filter the candidates and fill the cache.
        let wanted = pending
            .iter()
            .map(|(vertex_id, _)| *vertex_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for (vertex_id, metadata) in self.existing_vertex_metadata_batch_at(cell_id, &wanted, read_epoch, budget).await? {
            metadata_cache.insert(vertex_id, metadata);
        }

        for (vertex_id, mut row) in pending {
            if !metadata_cache.contains_key(&vertex_id) {
                continue;
            }
            self.hydrate_binding_metadata(cell_id, read_epoch, &mut row, &mut metadata_cache, budget)
                .await?;
            if row_matches_node(&row, node)? {
                self.push_binding_row(&mut rows, row, "cypher_node_rows")?;
            }
        }
        Ok(rows)
    }

    /// `(a)-[:R]-(b)`: match the hop in both orientations and merge.
    ///
    /// The fan-out lives here, inside pattern matching, and NOT at the query
    /// level as union arms. `execute_union_opencypher_rows` finishes each arm
    /// independently and never reapplies the query's `ORDER BY` across them, so
    /// expanding an undirected hop into arms would silently unsort the result
    /// and let each arm keep its own `LIMIT`. Merging here means the combined
    /// rows go through one projection pipeline, so `ORDER BY`, `DISTINCT` and
    /// `LIMIT` each apply once.
    ///
    /// `reversed()` returns an `Outbound` pattern, so the oriented matcher
    /// below cannot fan out a second time.
    ///
    /// With a `bound`, rows come back joined to its input and filtered by its
    /// predicate, and the traversal stops once it has the rows wanted; the
    /// reverse orientation runs only for what the forward one left short.
    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        bound: Option<EdgeRowBound<'_>>,
    ) -> Result<Vec<BindingRow>> {
        if edge.direction != EdgeDirection::Bidirectional {
            return self
                .match_edge_row_pattern_oriented(cell_id, edge, read_epoch, budget, bound, false)
                .await;
        }
        let mut rows = self
            .match_edge_row_pattern_oriented(cell_id, edge, read_epoch, budget, bound, false)
            .await?;
        let reverse_bound = match bound {
            Some(bound) if rows.len() >= bound.wanted => return Ok(rows),
            Some(bound) => Some(EdgeRowBound {
                wanted: bound.wanted - rows.len(),
                ..bound
            }),
            None => None,
        };
        let reversed = edge.reversed();
        // A single-hop row carries one physical relationship. Only self-loops
        // duplicate the forward orientation, so the reverse traversal drops
        // them. A non-loop has two matches even when neither endpoint has a
        // binding, so row-value dedup is unsound.
        let reverse_rows = self
            .match_edge_row_pattern_oriented(cell_id, &reversed, read_epoch, budget, reverse_bound, true)
            .await?;
        for row in reverse_rows {
            self.push_binding_row(&mut rows, row, "cypher_undirected_edge_rows")?;
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_oriented(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        bound: Option<EdgeRowBound<'_>>,
        skip_self_loops: bool,
    ) -> Result<Vec<BindingRow>> {
        validate_component("edge_type", &edge.edge_type)?;
        if let Some((min_hops, max_hops)) = edge.hop_range {
            return self
                .match_reachable_row_pattern(cell_id, edge, min_hops, max_hops, read_epoch, budget)
                .await;
        }

        let access = budget
            .read_only_io(
                "cypher_plan_edge_access",
                self.best_row_edge_access_with_stats(cell_id, edge, read_epoch, &BTreeSet::new()),
            )
            .await?;
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();
        let mut state = EdgeRowMatchState::new(cell_id, read_epoch, edge, &mut metadata_cache, &mut edge_metadata_cache, budget);
        state.bound = bound;
        state.skip_self_loops = skip_self_loops;
        match access {
            RowQueryAccess::ExpandInto { .. } => self.match_edge_row_pattern_expand_into(state).await,
            RowQueryAccess::BoundOutExpand { .. } => {
                let Some(sources) = self.candidate_vertex_ids(cell_id, &edge.src, read_epoch, budget).await? else {
                    return self.match_edge_row_pattern_full_scan(state).await;
                };
                self.match_edge_row_pattern_from_sources(state, sources).await
            }
            RowQueryAccess::BoundInExpand { .. } => {
                let Some(destinations) = self.candidate_vertex_ids(cell_id, &edge.dst, read_epoch, budget).await? else {
                    return self.match_edge_row_pattern_full_scan(state).await;
                };
                self.match_edge_row_pattern_from_destinations(state, destinations).await
            }
            RowQueryAccess::EdgePropertyIndex { .. } => self.match_edge_row_pattern_from_property_index(state).await,
            RowQueryAccess::FullEdgeScan { .. } => self.match_edge_row_pattern_full_scan(state).await,
            RowQueryAccess::VariableLengthExpand { .. } => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/edge-access/{}", edge.edge_type),
                reason: "optimizer sent variable-length edge pattern to single-edge matcher".to_string(),
            }),
            RowQueryAccess::VertexIdSeek
            | RowQueryAccess::VertexPropertyIndex { .. }
            | RowQueryAccess::VertexLabelScan { .. }
            | RowQueryAccess::AllVertexScan => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/edge-access/{}", edge.edge_type),
                reason: "optimizer selected node access for edge pattern".to_string(),
            }),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_expand_into(&self, mut state: EdgeRowMatchState<'_>) -> Result<Vec<BindingRow>> {
        let (cell_id, edge, read_epoch, budget) = (state.cell_id, state.edge, state.read_epoch, state.budget);
        let Some(sources) = self.candidate_vertex_ids(cell_id, &edge.src, read_epoch, budget).await? else {
            return self.match_edge_row_pattern_full_scan(state).await;
        };
        let Some(destinations) = self.candidate_vertex_ids(cell_id, &edge.dst, read_epoch, budget).await? else {
            return self.match_edge_row_pattern_full_scan(state).await;
        };
        self.ensure_query_index_candidates("cypher_edge_expand_into_sources", sources.len())?;
        self.ensure_query_index_candidates("cypher_edge_expand_into_destinations", destinations.len())?;
        let candidate_pairs = sources
            .len()
            .checked_mul(destinations.len())
            .ok_or_else(|| GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/expand-into/{}", edge.edge_type),
                reason: "expand-into candidate pair count overflow".to_string(),
            })?;
        self.ensure_query_index_candidates("cypher_edge_expand_into_pairs", candidate_pairs)?;

        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        'sources: for src in sources {
            for dst in &destinations {
                budget.check("cypher_edge_expand_into")?;
                let exists = if latest_snapshot {
                    self.edge_exists(cell_id, &edge.edge_type, src, *dst).await?
                } else {
                    self.edge_exists_at(cell_id, &edge.edge_type, src, *dst, read_epoch).await?
                };
                if exists {
                    self.push_matching_edge_row(edge, src, *dst, &mut state).await?;
                    if state.is_full() {
                        break 'sources;
                    }
                }
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_property_index(&self, mut state: EdgeRowMatchState<'_>) -> Result<Vec<BindingRow>> {
        let (cell_id, edge, read_epoch, budget) = (state.cell_id, state.edge, state.read_epoch, state.budget);
        // The optimizer only picks this access for an indexable property, but
        // never trust the map's first entry: a list or date is never an index
        // key, so looking one up finds nothing.
        let Some((property, value)) = first_indexable_property(&edge.properties) else {
            return self.match_edge_row_pattern_full_scan(state).await;
        };
        let pairs = self
            .scan_edge_property_index_at(cell_id, &edge.edge_type, property, value, read_epoch, budget)
            .await?;
        self.ensure_query_index_candidates("cypher_edge_property_candidates", pairs.len())?;
        for (src, dst) in pairs {
            budget.check("cypher_edge_property_rows")?;
            self.push_matching_edge_row(edge, src, dst, &mut state).await?;
            if state.is_full() {
                break;
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_sources(
        &self,
        mut state: EdgeRowMatchState<'_>,
        sources: Vec<VertexId>,
    ) -> Result<Vec<BindingRow>> {
        let (cell_id, edge, read_epoch, budget) = (state.cell_id, state.edge, state.read_epoch, state.budget);
        self.ensure_query_index_candidates("cypher_edge_source_candidates", sources.len())?;
        // The pairs an edge property equality names, read once, for sources
        // whose degree makes checking every neighbor the dearer way. A probe
        // that overflowed only shows the index is broader than that source:
        // a source with at least twice its cap probes again
        // (anchored_property_probe_due).
        let mut property_pairs: Option<BTreeSet<(VertexId, VertexId)>> = None;
        let mut probe_overflowed_at = 0usize;
        'sources: for src in sources {
            budget.check("cypher_edge_sources")?;
            let neighbors = self
                .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
                .await?;
            let scanned_edges = budget.add_scanned_edges(neighbors.len() as u64);
            self.ensure_query_scan_edges("cypher_edge_neighbor_scan", scanned_edges)?;
            if neighbors.len() > ANCHORED_EXPAND_PROPERTY_PROBE_MIN_DEGREE {
                if property_pairs.is_none() && anchored_property_probe_due(neighbors.len(), probe_overflowed_at) {
                    property_pairs = self
                        .try_edge_property_pairs_bounded(cell_id, edge, read_epoch, budget, neighbors.len())
                        .await?;
                    if property_pairs.is_none() {
                        probe_overflowed_at = neighbors.len();
                    }
                }
                if let Some(pairs) = &property_pairs {
                    for &(_, dst) in pairs.range((src, VertexId::MIN)..=(src, VertexId::MAX)) {
                        self.push_matching_edge_row(edge, src, dst, &mut state).await?;
                        if state.is_full() {
                            break 'sources;
                        }
                    }
                    continue;
                }
            }
            for dst in neighbors {
                self.push_matching_edge_row(edge, src, dst, &mut state).await?;
                if state.is_full() {
                    break 'sources;
                }
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_from_destinations(
        &self,
        mut state: EdgeRowMatchState<'_>,
        destinations: Vec<VertexId>,
    ) -> Result<Vec<BindingRow>> {
        let (cell_id, edge, read_epoch, budget) = (state.cell_id, state.edge, state.read_epoch, state.budget);
        self.ensure_query_index_candidates("cypher_edge_destination_candidates", destinations.len())?;
        'destinations: for dst in destinations {
            budget.check("cypher_edge_destinations")?;
            let neighbors = self
                .in_neighbors_at_for_query(cell_id, &edge.edge_type, dst, read_epoch, budget)
                .await?;
            let scanned_edges = budget.add_scanned_edges(neighbors.len() as u64);
            self.ensure_query_scan_edges("cypher_edge_reverse_neighbor_scan", scanned_edges)?;
            for src in neighbors {
                self.push_matching_edge_row(edge, src, dst, &mut state).await?;
                if state.is_full() {
                    break 'destinations;
                }
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn match_edge_row_pattern_full_scan(&self, mut state: EdgeRowMatchState<'_>) -> Result<Vec<BindingRow>> {
        let (cell_id, edge, read_epoch, budget) = (state.cell_id, state.edge, state.read_epoch, state.budget);
        let records = self
            .edges_at_with_budget(cell_id, &edge.edge_type, read_epoch, Some(budget))
            .await?;
        let scanned_edges = budget.add_scanned_edges(records.len() as u64);
        self.ensure_query_scan_edges("cypher_edge_full_scan", scanned_edges)?;
        for record in records {
            budget.check("cypher_edge_full_scan")?;
            self.push_matching_edge_row(edge, record.src, record.dst, &mut state).await?;
            if state.is_full() {
                break;
            }
        }
        self.finish_edge_rows(state).await
    }

    #[cfg(feature = "opencypher")]
    async fn push_matching_edge_row(
        &self,
        edge: &RowEdgePattern,
        src: VertexId,
        dst: VertexId,
        state: &mut EdgeRowMatchState<'_>,
    ) -> Result<()> {
        state.budget.check("cypher_edge_rows")?;
        if matches!(edge.src.id, Some(fixed_src) if fixed_src != src) || matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
            return Ok(());
        }
        let relationships = if let Some((property, value)) = first_indexable_property(&edge.properties) {
            self.relationships_for_edge_property_at(RelationshipPropertyLookup {
                cell_id: state.cell_id,
                edge_type: &edge.edge_type,
                src,
                dst,
                property,
                value,
                read_epoch: state.read_epoch,
                budget: state.budget,
            })
            .await?
        } else {
            self.relationships_for_edge_at(state.cell_id, &edge.edge_type, src, dst, state.read_epoch, state.budget)
                .await?
        };
        for (relationship, metadata) in relationships {
            let Some(row) = BindingRow::from_relationship(edge, relationship, metadata) else {
                continue;
            };
            state.pending.push(row);
            // Checked per row, not per pair. One `(src, dst)` usually carries a
            // single relationship, but a multigraph pair can carry thousands,
            // and checking after the loop would buffer every one of them before
            // asking -- which is not the bound `EDGE_ROW_HYDRATION_BATCH`
            // claims. Every caller still flushes after its traversal; this only
            // decides how often that happens, never whether it does.
            if state.pending.len() >= EDGE_ROW_HYDRATION_BATCH {
                self.flush_pending_edge_rows(state).await?;
                if state.is_full() {
                    break;
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn flush_pending_edge_rows(&self, state: &mut EdgeRowMatchState<'_>) -> Result<()> {
        if state.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut state.pending);

        let wanted = pending.iter().flat_map(|row| row.values.values().copied()).collect::<Vec<_>>();
        self.hydrate_vertex_metadata_cache_at(state.cell_id, &wanted, state.read_epoch, state.metadata_cache, state.budget)
            .await?;

        for mut row in pending {
            // A bounded traversal keeps no more than it wants. The rest of
            // the batch was hydrated for nothing, which is the price of
            // hydrating in batches at all; the traversal stops after this.
            if state.is_full() {
                break;
            }
            self.hydrate_binding_metadata(state.cell_id, state.read_epoch, &mut row, state.metadata_cache, state.budget)
                .await?;
            self.hydrate_row_relationship_metadata(
                state.cell_id,
                state.read_epoch,
                &mut row,
                state.edge,
                state.edge_metadata_cache,
                state.budget,
            )
            .await?;
            if !row_matches_edge_pattern(&row, state.edge)? {
                continue;
            }
            if state.skip_self_loops
                && row
                    .relationship_metadata
                    .keys()
                    .all(|relationship| relationship.src == relationship.dst)
            {
                continue;
            }
            let row = match state.bound {
                None => row,
                Some(bound) => {
                    let Some(joined) = bound.input.join(&row) else {
                        continue;
                    };
                    if let Some(predicate) = bound.predicate {
                        state.budget.check("cypher_group_where")?;
                        if !row_predicate_matches(&joined, predicate)? {
                            continue;
                        }
                    }
                    joined
                }
            };
            self.push_binding_row(&mut state.rows, row, "cypher_edge_rows")?;
        }
        Ok(())
    }

    /// Drain the last batch and hand back the traversal's rows.
    ///
    /// Every traversal ends here. Rows accumulate un-hydrated while the pattern
    /// is walked, so a caller that returned its rows directly would drop the
    /// last partial batch and report fewer matches than it found, with no error
    /// anywhere -- a failure mode the suite did not catch when I introduced it
    /// deliberately.
    ///
    /// Taking `state` by value drops the buffer with it, which is what makes an
    /// error raised mid-traversal safe: the query failed, the buffered rows are
    /// discarded, and nothing has to distinguish that from a forgotten flush.
    /// An earlier attempt asserted on a non-empty buffer in `Drop` and turned
    /// every mid-scan admission failure into a panic.
    #[cfg(feature = "opencypher")]
    async fn finish_edge_rows(&self, mut state: EdgeRowMatchState<'_>) -> Result<Vec<BindingRow>> {
        self.flush_pending_edge_rows(&mut state).await?;
        Ok(state.rows)
    }

    #[cfg(feature = "opencypher")]
    async fn match_reachable_row_pattern(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        min_hops: u8,
        max_hops: u8,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let Some(src) = edge.src.id else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "variable-length MATCH requires a fixed source id".to_string(),
            });
        };
        // A source id without a canonical record names no vertex, so nothing
        // is reachable from it, not even itself at zero hops.
        if self.vertex_metadata_if_exists_at(cell_id, src, read_epoch, budget).await?.is_none() {
            return Ok(Vec::new());
        }
        let (vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(cell_id, &edge.edge_type, src, (min_hops, max_hops), read_epoch, budget)
            .await?;
        budget.check("cypher_reachable")?;
        self.ensure_query_scan_edges("cypher_reachable_edge_visits", edge_visits)?;
        self.ensure_query_intermediate_rows("cypher_reachable_rows", vertices.len())?;
        let mut rows = Vec::with_capacity(vertices.len());
        let mut metadata_cache = BTreeMap::new();
        let mut edge_metadata_cache = BTreeMap::new();

        let mut pending = Vec::with_capacity(vertices.len());
        for dst in vertices {
            budget.check("cypher_reachable_rows")?;
            if matches!(edge.dst.id, Some(fixed_dst) if fixed_dst != dst) {
                continue;
            }
            if let Some(row) = BindingRow::from_edge(edge, src, dst) {
                pending.push(row);
            }
        }

        let wanted = pending.iter().flat_map(|row| row.values.values().copied()).collect::<Vec<_>>();
        self.hydrate_vertex_metadata_cache_at(cell_id, &wanted, read_epoch, &mut metadata_cache, budget)
            .await?;

        for mut row in pending {
            self.hydrate_binding_metadata(cell_id, read_epoch, &mut row, &mut metadata_cache, budget)
                .await?;
            self.hydrate_row_relationship_metadata(cell_id, read_epoch, &mut row, edge, &mut edge_metadata_cache, budget)
                .await?;
            if row_matches_edge_pattern(&row, edge)? {
                self.push_binding_row(&mut rows, row, "cypher_reachable_rows")?;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn candidate_vertex_ids(
        &self,
        cell_id: &str,
        pattern: &RowNodePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<Vec<VertexId>>> {
        budget.check("cypher_candidate_vertices")?;
        let access = budget
            .read_only_io(
                "cypher_plan_node_access",
                self.best_row_node_access_with_stats(pattern, &BTreeSet::new()),
            )
            .await?;
        match access {
            // The id is only a candidate: `match_node_row_candidates` drops it
            // when the vertex has no canonical record.
            RowQueryAccess::VertexIdSeek => Ok(pattern.id.map(|id| vec![id])),
            RowQueryAccess::VertexPropertyIndex { property } => {
                let Some(value) = pattern.properties.get(&property) else {
                    return Err(GraphError::CorruptValue {
                        key: format!("cell/{cell_id}/query/node-access/{property}"),
                        reason: "optimizer selected missing vertex property".to_string(),
                    });
                };
                if pattern.properties.len() > 1 {
                    // Statistics can lag a hot scope. Probe every inline
                    // equality within one shared allowance and
                    // use the first complete index, so a stale estimate for a
                    // tenant-wide property cannot hide a selective identity
                    // property and turn a valid lookup into an admission
                    // failure. If every probe is broad, preserve the selected
                    // plan's full allowance rather than multiplying a 250k
                    // scan by the number of properties.
                    let mut properties = Vec::with_capacity(pattern.properties.len());
                    properties.push((property.as_str(), value));
                    properties.extend(
                        pattern
                            .properties
                            .iter()
                            .filter(|(candidate, _)| candidate.as_str() != property)
                            .map(|(candidate, value)| (candidate.as_str(), value)),
                    );
                    // Give each property a fixed slice of one aggregate probe
                    // budget. A cached broad prefix can otherwise consume a
                    // shared atomic allowance before a selective property gets
                    // polled, making the result depend on cache timing.
                    let probe_budget = self.limits.max_query_index_candidates.min(256);
                    let allowance = probe_budget / properties.len();
                    let alternative_allowance = QUERY_NODE_EQUALITY_PROBE_ALTERNATIVES / properties.len();
                    if allowance > 0 && alternative_allowance > 0 {
                        let mut probes = stream::FuturesUnordered::new();
                        for (property, value) in properties {
                            probes.push(async move {
                                let vertices = self
                                    .try_scan_vertex_property_value_at(cell_id, property, value, budget, allowance, alternative_allowance)
                                    .await?;
                                Ok::<_, GraphError>((property, vertices))
                            });
                        }
                        while let Some(probe) = probes.next().await {
                            let (winning_property, vertices) = probe?;
                            if let Some(vertices) = vertices {
                                tracing::Span::current().record("hydradb.query.runtime_property_index", winning_property);
                                return Ok(Some(vertices));
                            }
                        }
                    }
                }
                Ok(Some(
                    self.scan_vertex_property_index_at(cell_id, &property, value, read_epoch, budget)
                        .await?,
                ))
            }
            RowQueryAccess::VertexLabelScan { label } => {
                Ok(Some(self.scan_vertex_label_index_at(cell_id, &label, read_epoch, budget).await?))
            }
            RowQueryAccess::AllVertexScan => Ok(None),
            RowQueryAccess::BoundOutExpand { .. }
            | RowQueryAccess::BoundInExpand { .. }
            | RowQueryAccess::ExpandInto { .. }
            | RowQueryAccess::EdgePropertyIndex { .. }
            | RowQueryAccess::FullEdgeScan { .. }
            | RowQueryAccess::VariableLengthExpand { .. } => Err(GraphError::CorruptValue {
                key: format!("cell/{cell_id}/query/node-access"),
                reason: "optimizer selected edge access for node pattern".to_string(),
            }),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn vertex_metadata_at(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<VertexMetadata> {
        validate_component("cell_id", cell_id)?;
        let key = keys::vertex(cell_id, vertex_id);
        match budget.read_only_io("cypher_vertex_metadata", self.read_remote(&key)).await? {
            Some(value) => {
                let mut metadata = decode_vertex_metadata(&key, &value)?;
                self.refuse_pending_record_properties(cell_id, &metadata, budget).await?;
                self.overlay_write_back_properties(cell_id, vertex_id, &mut metadata, budget)
                    .await?;
                Ok(metadata)
            }
            None => Ok(VertexMetadata::default()),
        }
    }

    /// Lay the vertex's write-back values over its decoded record, the
    /// value key winning. The write-back never touches the record, so a
    /// property it wrote lives only here, and a copy the record still holds
    /// from before the property moved is older by construction. Applied only
    /// to a record that exists, which is what keeps a value written for a
    /// vertex deleted in the meantime out of every read. One short prefix
    /// scan per vertex read, on top of the record's point get, is the price
    /// of a write-back that reads nothing.
    #[cfg(feature = "opencypher")]
    async fn overlay_write_back_properties(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        metadata: &mut VertexMetadata,
        budget: &QueryBudget,
    ) -> Result<()> {
        metadata
            .properties
            .extend(self.write_back_properties(cell_id, vertex_id, budget).await?);
        Ok(())
    }

    /// Check the actual value namespace so older stores and values with no
    /// catalog entry are covered. The caller pins the snapshot for the probe
    /// and all vertex reads that reuse its answer.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) async fn cell_has_write_back_values(&self, cell_id: &str, budget: &QueryBudget) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        budget
            .read_only_io("cypher_write_back_presence", async {
                let prefix = keys::vertex_write_back_cell_prefix(cell_id);
                Ok(self.scan_remote_point_prefix(&prefix).await?.next().await?.is_some())
            })
            .await
    }

    /// The write-back values stored for `vertex_id`'s properties, which take
    /// precedence over the vertex record's own.
    #[cfg(feature = "opencypher")]
    async fn write_back_properties(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        budget: &QueryBudget,
    ) -> Result<BTreeMap<String, VertexPropertyValue>> {
        let mut properties = BTreeMap::new();
        let prefix = keys::vertex_write_back_property_prefix(cell_id, vertex_id);
        let mut iter = budget
            .read_only_io("cypher_vertex_write_back", self.scan_remote_point_prefix(&prefix))
            .await?;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let Some(property) = key.strip_prefix(&prefix) else {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "expected a write-back value key".to_string(),
                });
            };
            let value = std::str::from_utf8(&kv.value).map_err(|err| GraphError::CorruptValue {
                key: key.clone(),
                reason: err.to_string(),
            })?;
            let value = decode_vertex_property_value_record(&key, value)?;
            self.refuse_pending_write_back(cell_id, property, budget).await?;
            properties.insert(property.to_string(), value);
        }
        Ok(properties)
    }

    /// Refuse a read of `property` while a write-back of it is pending
    /// ([`keys::vertex_write_back_pending`]): its value keys and its index
    /// disagree until a run completes. Per query, through `budget`'s
    /// [`WriteBackPendingReads`]: one seek of the cell's markers, and one get
    /// per property only when the cell has any.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn refuse_pending_write_back(&self, cell_id: &str, property: &str, budget: &QueryBudget) -> Result<()> {
        let any_pending = *budget
            .write_back_pending
            .cell(cell_id)
            .get_or_try_init(|| async {
                let prefix = keys::vertex_write_back_pending_prefix(cell_id);
                budget
                    .read_only_io("cypher_vertex_write_back_pending", async {
                        Ok(self.scan_remote_point_prefix(&prefix).await?.next().await?.is_some())
                    })
                    .await
            })
            .await?;
        if !any_pending {
            return Ok(());
        }
        let slot = budget.write_back_pending.slot(cell_id, property);
        let pending = slot
            .get_or_try_init(|| async {
                let key = keys::vertex_write_back_pending(cell_id, property);
                budget
                    .read_only_io("cypher_vertex_write_back_pending", self.read_remote(&key))
                    .await?
                    .map(|value| decode_u64(&key, &value))
                    .transpose()
            })
            .await?;
        match pending {
            Some(generation) => Err(hydradb_graph_slate::write::write_back_in_progress(property, *generation)),
            None => Ok(()),
        }
    }

    /// [`Self::refuse_pending_write_back`] for every property a vertex's
    /// record holds. A run that has not reached the vertex yet left no value
    /// key to lay over the record, so a copy the record holds from before
    /// the property moved, or from before its first run, would be read as
    /// the property's value. Checked whether or not the read lays write-back
    /// values over the record, since a cell's first run has none to lay
    /// until its first chunk commits.
    #[cfg(feature = "opencypher")]
    async fn refuse_pending_record_properties(&self, cell_id: &str, record: &VertexMetadata, budget: &QueryBudget) -> Result<()> {
        for property in record.properties.keys() {
            self.refuse_pending_write_back(cell_id, property, budget).await?;
        }
        Ok(())
    }

    /// Where `property`'s index entries live: the current generation of the
    /// write-back index when the catalog names one, the classic index
    /// otherwise. Every seek and walk over a vertex property index takes its
    /// prefix from here, so a property that moved to the write-back layout
    /// is found by the same seeds. One point get of the catalog per query
    /// seed; the catalog is not cached on the shard, so a switch committed
    /// by a write-back is seen by the next read with no invalidation step.
    /// A property whose write-back is pending is refused first
    /// ([`Self::refuse_pending_write_back`]), so no index read of it runs.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn vertex_property_index_prefix_at(&self, cell_id: &str, property: &str, budget: &QueryBudget) -> Result<String> {
        self.refuse_pending_write_back(cell_id, property, budget).await?;
        let key = keys::vertex_write_back_generation(cell_id, property);
        match budget
            .read_only_io("cypher_vertex_property_index_generation", self.read_remote(&key))
            .await?
        {
            Some(value) => Ok(keys::vertex_write_back_index_generation_prefix(
                cell_id,
                property,
                decode_u64(&key, &value)?,
            )),
            None => Ok(keys::vertex_property_index_property_prefix(cell_id, property)),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn vertex_metadata_batch_at(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexMetadata)>> {
        self.vertex_metadata_batch_at_concurrently(cell_id, vertex_ids, read_epoch, budget, QUERY_METADATA_HYDRATION_CONCURRENCY)
            .await
    }

    /// [`Self::vertex_metadata_batch_at`] with `concurrency` point reads in
    /// flight. Each read of a cold vertex is an object-store round trip, so
    /// the batch's latency is about `len / concurrency` of them.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn vertex_metadata_batch_at_concurrently(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        concurrency: usize,
    ) -> Result<Vec<(VertexId, VertexMetadata)>> {
        stream::iter(vertex_ids.iter().copied().map(|vertex_id| async move {
            budget.check("cypher_metadata_hydration")?;
            let started = Instant::now();
            let metadata = self
                .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                .instrument(tracing::info_span!(
                    "query.property_fetch",
                    hydradb.cell_id = %cell_id,
                    hydradb.property_owner = "vertex",
                ))
                .await?;
            self.operation_metrics.record_property_fetch(started.elapsed().as_micros() as u64);
            Ok::<_, GraphError>((vertex_id, metadata))
        }))
        .buffered(concurrency)
        .boxed()
        .try_collect()
        .await
    }

    /// The vertex's canonical record, or `None` when it has none. A record,
    /// even an empty one, is the vertex's membership; an absent record means
    /// the vertex does not exist.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn vertex_metadata_if_exists_at(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<VertexMetadata>> {
        self.read_existing_vertex(cell_id, vertex_id, budget, true).await
    }

    #[cfg(feature = "opencypher")]
    async fn read_existing_vertex(
        &self,
        cell_id: &str,
        vertex_id: VertexId,
        budget: &QueryBudget,
        has_overlays: bool,
    ) -> Result<Option<VertexMetadata>> {
        validate_component("cell_id", cell_id)?;
        let key = keys::vertex(cell_id, vertex_id);
        if !has_overlays {
            let record = budget
                .read_only_io("cypher_vertex_metadata", async {
                    self.read_remote(&key)
                        .await?
                        .map(|bytes| decode_vertex_metadata(&key, &bytes))
                        .transpose()
                })
                .await?;
            if let Some(record) = &record {
                self.refuse_pending_record_properties(cell_id, record, budget).await?;
            }
            return Ok(record);
        }
        // The overlay's prefix needs only the id, so it is read beside the
        // vertex rather than after it: one round trip deep instead of two. The
        // vertex decides: when it has no record the overlay is dropped
        // unfinished and its result, errors included, never consulted, as when
        // it was read only after the vertex. The deadline bounds the whole.
        let vertex = Box::pin(self.read_remote(&key));
        let overlay = Box::pin(self.write_back_properties(cell_id, vertex_id, budget));
        let read = async {
            let (value, overlay) = match futures::future::select(vertex, overlay).await {
                futures::future::Either::Left((value, overlay)) => match value? {
                    Some(value) => (value, overlay.await),
                    None => return Ok(None),
                },
                futures::future::Either::Right((overlay, vertex)) => match vertex.await? {
                    Some(value) => (value, overlay),
                    None => return Ok(None),
                },
            };
            let mut metadata = decode_vertex_metadata(&key, &value)?;
            let overlay = overlay?;
            self.refuse_pending_record_properties(cell_id, &metadata, budget).await?;
            metadata.properties.extend(overlay);
            Ok(Some(metadata))
        };
        budget.read_only_io("cypher_vertex_metadata", read).await
    }

    /// The records of the vertices among `vertex_ids` that exist, in input
    /// order. Absent ids are dropped rather than given empty metadata, so
    /// callers must match results by id, never by position.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn existing_vertex_metadata_batch_at(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<impl Iterator<Item = (VertexId, VertexMetadata)>> {
        self.read_existing_vertices(cell_id, vertex_ids, budget, true).await
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn read_existing_vertices(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        budget: &QueryBudget,
        has_overlays: bool,
    ) -> Result<impl Iterator<Item = (VertexId, VertexMetadata)>> {
        let records = stream::iter(vertex_ids.iter().copied().map(|vertex_id| async move {
            budget.check("cypher_metadata_hydration")?;
            let started = Instant::now();
            let metadata = self
                .read_existing_vertex(cell_id, vertex_id, budget, has_overlays)
                .instrument(tracing::info_span!(
                    "query.property_fetch",
                    hydradb.cell_id = %cell_id,
                    hydradb.property_owner = "vertex",
                ))
                .await?;
            self.operation_metrics.record_property_fetch(started.elapsed().as_micros() as u64);
            Ok::<_, GraphError>(metadata.map(|metadata| (vertex_id, metadata)))
        }))
        .buffered(QUERY_METADATA_HYDRATION_CONCURRENCY)
        .boxed()
        .try_collect::<Vec<_>>()
        .await?;
        Ok(records.into_iter().flatten())
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn hydrate_vertex_metadata_cache_at(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        read_epoch: StorageSequence,
        cache: &mut BTreeMap<VertexId, VertexMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        self.hydrate_vertex_metadata_cache_at_concurrently(
            cell_id,
            vertex_ids,
            read_epoch,
            cache,
            budget,
            QUERY_METADATA_HYDRATION_CONCURRENCY,
        )
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn hydrate_vertex_metadata_cache_at_concurrently(
        &self,
        cell_id: &str,
        vertex_ids: &[VertexId],
        read_epoch: StorageSequence,
        cache: &mut BTreeMap<VertexId, VertexMetadata>,
        budget: &QueryBudget,
        concurrency: usize,
    ) -> Result<()> {
        let mut seen = BTreeSet::new();
        let pending = vertex_ids
            .iter()
            .copied()
            .filter(|vertex_id| !cache.contains_key(vertex_id) && seen.insert(*vertex_id))
            .collect::<Vec<_>>();
        for (vertex_id, metadata) in self
            .vertex_metadata_batch_at_concurrently(cell_id, &pending, read_epoch, budget, concurrency)
            .await?
        {
            cache.insert(vertex_id, metadata);
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn edge_metadata_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<EdgeMetadata> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let key = keys::edge_metadata(cell_id, edge_type, src, dst);
        match budget.read_only_io("cypher_edge_metadata", self.read_remote(&key)).await? {
            Some(value) => decode_edge_metadata(&key, &value),
            None => Ok(EdgeMetadata::default()),
        }
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_metadata_at(
        &self,
        cell_id: &str,
        relationship: &BoundRelationship,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<EdgeMetadata> {
        let Some(relationship_id) = relationship.relationship_id else {
            return self
                .edge_metadata_at(
                    cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                    read_epoch,
                    budget,
                )
                .await;
        };
        budget.check("cypher_relationship_record")?;
        let key = keys::relationship(
            cell_id,
            &relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship_id,
        );
        let Some(value) = budget.read_only_io("cypher_relationship_record", self.read_remote(&key)).await? else {
            return Ok(EdgeMetadata::default());
        };
        let record = decode_relationship_record(&key, &value)?;
        Ok(record.metadata)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn indexed_relationship_metadata_batch_at(
        &self,
        request: IndexedRelationshipBatch<'_>,
    ) -> Result<Vec<Option<EdgeMetadata>>> {
        let IndexedRelationshipBatch {
            cell_id,
            edge_type,
            property,
            candidates,
            read_epoch,
            inherit_structural,
            budget,
            memo,
        } = request;
        // All reads remain inside the caller's pinned storage snapshot. The
        // historical graph-epoch route retains its existing visibility logic.
        if read_epoch != self.current_epoch(cell_id).await? {
            let mut result = Vec::with_capacity(candidates.len());
            for (src, dst, id, encoded) in candidates {
                let records = self
                    .relationships_for_edge_at(cell_id, edge_type, *src, *dst, read_epoch, budget)
                    .await?;
                let own = records.iter().find(|(candidate, metadata)| {
                    candidate.relationship_id == Some(*id)
                        && metadata
                            .properties
                            .get(property)
                            .is_some_and(|value| encode_vertex_property_value_key(value) == *encoded)
                });
                result.push(own.map(|(_, own)| {
                    let mut metadata = if inherit_structural {
                        records
                            .iter()
                            .find_map(|(relationship, metadata)| relationship.relationship_id.is_none().then_some(metadata.clone()))
                            .unwrap_or_default()
                    } else {
                        EdgeMetadata::default()
                    };
                    metadata.properties.extend(own.properties.clone());
                    metadata
                }));
            }
            return Ok(result);
        }
        let snapshot = self.snapshot(cell_id).await?;
        let scope = (cell_id.to_string(), edge_type.to_string(), read_epoch, inherit_structural);
        if memo.scope.as_ref() != Some(&scope) {
            memo.pairs.clear();
            memo.scope = Some(scope);
        }
        let pairs = candidates.iter().map(|(src, dst, _, _)| (*src, *dst)).collect::<BTreeSet<_>>();
        let cached = pairs
            .iter()
            .filter_map(|pair| {
                memo.pairs
                    .get(pair)
                    .map(|exists| (*pair, exists.then_some(EdgeMetadata::default())))
            })
            .collect::<BTreeMap<_, _>>();
        let missing = pairs.into_iter().filter(|pair| !cached.contains_key(pair));
        let mut pairs = stream::iter(missing.map(|(src, dst)| {
            let snapshot = &snapshot;
            async move {
                budget.check("cypher_indexed_relationship_pair")?;
                let exists = budget.read_only_io("cypher_indexed_relationship_exists", snapshot.edge_exists(edge_type, src, dst));
                let metadata = async {
                    if inherit_structural {
                        let key = keys::edge_metadata(cell_id, edge_type, src, dst);
                        budget
                            .read_only_io("cypher_indexed_structural_metadata", self.read_remote(&key))
                            .await
                    } else {
                        Ok(None)
                    }
                };
                let (exists, metadata) = futures::join!(exists, metadata);
                let exists = exists?;
                budget.check("cypher_indexed_relationship_pair")?;
                if !exists {
                    return Ok::<_, GraphError>(((src, dst), None));
                }
                let key = keys::edge_metadata(cell_id, edge_type, src, dst);
                let metadata = match metadata? {
                    Some(value) => decode_edge_metadata(&key, &value)?,
                    None => EdgeMetadata::default(),
                };
                Ok::<_, GraphError>(((src, dst), Some(metadata)))
            }
        }))
        .buffered(ORDERED_RELATIONSHIP_FETCH_CONCURRENCY)
        .boxed()
        .try_collect::<BTreeMap<_, _>>()
        .await?;
        for (pair, metadata) in &pairs {
            if metadata.as_ref().is_none_or(|value| value.properties.is_empty()) {
                if memo.pairs.len() >= 4096 {
                    memo.pairs.clear();
                }
                memo.pairs.insert(*pair, metadata.is_some());
            }
        }
        pairs.extend(cached);
        // A point lookup of the ID the index already supplies replaces the
        // pair-wide prefix scan. Buffered concurrency preserves candidate order.
        stream::iter(candidates.iter().map(|(src, dst, id, encoded)| {
            let pairs = &pairs;
            async move {
                budget.check("cypher_indexed_relationship_record")?;
                let Some(Some(structural)) = pairs.get(&(*src, *dst)) else {
                    return Ok(None);
                };
                let key = keys::relationship(cell_id, edge_type, *src, *dst, *id);
                let Some(value) = budget
                    .read_only_io("cypher_indexed_relationship_record", self.read_remote(&key))
                    .await?
                else {
                    return Ok(None);
                };
                let record = decode_relationship_record(&key, &value)?;
                // Validate the relationship's own indexed value before property
                // inheritance: a stale own-property entry cannot borrow it.
                if record
                    .metadata
                    .properties
                    .get(property)
                    .is_none_or(|value| encode_vertex_property_value_key(value) != *encoded)
                {
                    return Ok(None);
                }
                let metadata = if structural.properties.is_empty() {
                    record.metadata
                } else {
                    let mut metadata = structural.clone();
                    metadata.properties.extend(record.metadata.properties);
                    metadata
                };
                Ok::<_, GraphError>(Some(metadata))
            }
        }))
        .buffered(ORDERED_RELATIONSHIP_FETCH_CONCURRENCY)
        .boxed()
        .try_collect()
        .await
    }

    /// [`relationship_rows_for_edge`] at a historical epoch: each record's
    /// metadata is read as of `read_epoch` rather than taken from the record.
    #[cfg(feature = "opencypher")]
    #[allow(clippy::too_many_arguments)]
    async fn historical_relationship_rows(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        records: Vec<RelationshipRecord>,
        structural_metadata: EdgeMetadata,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let mut rows = relationship_rows_for_edge(edge_type, src, dst, records, structural_metadata);
        for (relationship, metadata) in &mut rows {
            if relationship.relationship_id.is_some() {
                *metadata = self.relationship_metadata_at(cell_id, relationship, read_epoch, budget).await?;
            }
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        self.relationships_for_edge_with_presence_at(cell_id, edge_type, src, dst, read_epoch, budget, EdgePresence::Unverified)
            .await
    }

    #[cfg(feature = "opencypher")]
    #[allow(clippy::too_many_arguments)]
    async fn relationships_for_edge_with_presence_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        presence: EdgePresence,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        // The structural edge, its relationship records and its own metadata
        // live in three unrelated key ranges. On a cold scope each is an
        // object-store round trip, and an incoming hop's records sit under a
        // different source per edge, so reading them one after another made a
        // hub's hydration three round trips deep per edge. Read them together;
        // the edge-existence check still decides what is returned.
        let structural_exists = async {
            match presence {
                EdgePresence::Listed => Ok(true),
                EdgePresence::Unverified if latest_snapshot => self.edge_exists(cell_id, edge_type, src, dst).await,
                EdgePresence::Unverified => self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch).await,
            }
        };
        let relationship_records = async {
            let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
            let mut iter = self.scan_remote_point_prefix(&prefix).await?;
            let mut records = Vec::new();
            while let Some(kv) = iter.next().await? {
                budget.check("cypher_relationship_edge_records")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                records.push(decode_relationship_record(&key, &kv.value)?);
                self.ensure_query_index_candidates("cypher_relationship_edge_records", records.len())?;
            }
            Ok::<_, GraphError>(records)
        };
        let structural_metadata = self.edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget);
        // The existence check decides first: for an absent edge the record and
        // metadata reads are irrelevant, so their ordinary errors (an
        // undecodable leftover record, the index-candidate limit) are held back
        // and must not fail a lookup that answers "no relationships". A failed
        // existence check, a timeout or a cancellation fails the lookup either
        // way, so it ends the join at once and drops the other reads; the
        // budget bounds the whole join so a read stalled on a cold GET cannot
        // outlive the query's deadline or cancellation. Boxed so the three
        // reads' state does not grow every caller's future on the stack.
        let (structural_exists, relationship_records, structural_metadata) = budget
            .read_only_io(
                "cypher_relationship_edge_records",
                Box::pin(async {
                    futures::try_join!(
                        structural_exists,
                        defer_unless_query_timeout(relationship_records),
                        defer_unless_query_timeout(structural_metadata),
                    )
                }),
            )
            .await?;
        if !structural_exists {
            return Ok(Vec::new());
        }
        let relationship_records = relationship_records?;
        let structural_metadata = structural_metadata?;

        let rows = if latest_snapshot {
            // Shared with the source-batched read so the two cannot disagree.
            relationship_rows_for_edge(edge_type, src, dst, relationship_records, structural_metadata)
        } else {
            self.historical_relationship_rows(
                cell_id,
                edge_type,
                src,
                dst,
                relationship_records,
                structural_metadata,
                read_epoch,
                budget,
            )
            .await?
        };
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn source_relationship_id_bindings_at(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<BindingRow>> {
        let cached = self.source_relationship_dsts_at(cell_id, edge, src, read_epoch, budget).await?;
        source_relationship_id_bindings_from_dsts(edge, src, cached.as_slice(), self, budget)
    }

    #[cfg(feature = "opencypher")]
    async fn source_relationship_dsts_at(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Arc<Vec<VertexId>>> {
        let cache_key = SourceRelationshipRowsCacheKey::new(cell_id, &edge.edge_type, src, read_epoch);
        if let Some(cached) = self.source_relationship_rows_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::RelationshipRows);
            return Ok(cached);
        }
        self.cache_metrics.record_miss(GraphCacheKind::RelationshipRows);

        let prefix = keys::relationship_source_prefix(cell_id, &edge.edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut dsts = Vec::new();
        let mut relationship_dsts = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_source_relationship_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            relationship_dsts.insert(record.dst);
            dsts.push(record.dst);
            self.ensure_query_intermediate_rows("cypher_source_relationship_records", dsts.len())?;
        }

        let structural_neighbors = self
            .out_neighbors_at_for_query(cell_id, &edge.edge_type, src, read_epoch, budget)
            .await?;
        for dst in structural_neighbors {
            budget.check("cypher_source_relationship_structural_fallback")?;
            if relationship_dsts.contains(&dst) {
                continue;
            }
            dsts.push(dst);
            self.ensure_query_intermediate_rows("cypher_source_relationship_structural_fallback", dsts.len())?;
        }
        let cached = Arc::new(dsts);
        let resident_bytes = source_relationship_rows_resident_bytes(&cache_key, &cached);
        self.source_relationship_rows_cache.lock().await.insert_sized(
            cache_key,
            Arc::clone(&cached),
            cell_id.to_string(),
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok(cached)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_property_at(
        &self,
        lookup: RelationshipPropertyLookup<'_>,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let RelationshipPropertyLookup {
            cell_id,
            edge_type,
            src,
            dst,
            property,
            value,
            read_epoch,
            budget,
        } = lookup;
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let mut rows = Vec::new();
        let mut seen_relationship_ids = BTreeSet::new();
        let current_epoch = self.current_epoch(cell_id).await?;
        let latest_snapshot = read_epoch == current_epoch;
        let structural_exists = if latest_snapshot {
            budget
                .read_only_io(
                    "cypher_relationship_property_edge_exists",
                    self.edge_exists(cell_id, edge_type, src, dst),
                )
                .await?
        } else {
            budget
                .read_only_io(
                    "cypher_relationship_property_edge_exists",
                    self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch),
                )
                .await?
        };
        if !structural_exists {
            return Ok(Vec::new());
        }
        for encoded in equivalent_property_index_keys(value) {
            let encoded_rows = self
                .relationships_for_edge_encoded_property_at(EncodedRelationshipPropertyLookup {
                    cell_id,
                    edge_type,
                    src,
                    dst,
                    property,
                    encoded: &encoded,
                    read_epoch,
                    latest_snapshot,
                    budget,
                })
                .await?;
            for (relationship, metadata) in encoded_rows {
                let Some(relationship_id) = relationship.relationship_id else {
                    continue;
                };
                if !seen_relationship_ids.insert(relationship_id) {
                    continue;
                }
                rows.push((relationship, metadata));
                self.ensure_query_index_candidates("cypher_relationship_property_edge_candidates", rows.len())?;
            }
        }

        let structural_metadata = self.edge_metadata_at(cell_id, edge_type, src, dst, read_epoch, budget).await?;
        if structural_metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
        {
            rows.push((
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: None,
                },
                structural_metadata,
            ));
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn relationships_for_edge_encoded_property_at(
        &self,
        lookup: EncodedRelationshipPropertyLookup<'_>,
    ) -> Result<Vec<(BoundRelationship, EdgeMetadata)>> {
        let EncodedRelationshipPropertyLookup {
            cell_id,
            edge_type,
            src,
            dst,
            property,
            encoded,
            read_epoch,
            latest_snapshot,
            budget,
        } = lookup;
        let cache_key = RelationshipPropertyRowsCacheKey::new(cell_id, edge_type, src, dst, property, encoded, read_epoch);
        if let Some(cached) = self.relationship_property_rows_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::RelationshipPropertyRows);
            return Ok(relationship_rows_from_cache_value(edge_type, src, dst, cached));
        }
        self.cache_metrics.record_miss(GraphCacheKind::RelationshipPropertyRows);

        let relationship_ids = self
            .scan_relationship_property_index_current_for_edge(RelationshipPropertyEdgeIndexLookup {
                cell_id,
                edge_type,
                property,
                encoded,
                src,
                dst,
                budget,
            })
            .await?;

        let mut rows = Vec::with_capacity(relationship_ids.len());
        for relationship_id in relationship_ids {
            budget.check("cypher_relationship_property_edge_candidates")?;
            let relationship = BoundRelationship {
                edge_type: edge_type.to_string(),
                src,
                dst,
                relationship_id: Some(relationship_id),
            };
            let key = keys::relationship(cell_id, edge_type, src, dst, relationship_id);
            let Some(value) = budget
                .read_only_io("cypher_relationship_property_record", self.read_remote(&key))
                .await?
            else {
                continue;
            };
            let record = decode_relationship_record(&key, &value)?;
            let metadata = if latest_snapshot {
                record.metadata
            } else {
                self.relationship_metadata_at(cell_id, &relationship, read_epoch, budget).await?
            };
            rows.push((relationship, metadata));
            self.ensure_query_index_candidates("cypher_relationship_property_edge_candidates", rows.len())?;
        }
        let cache_value = relationship_rows_cache_value(&rows);
        let resident_bytes = cache_key
            .estimated_resident_bytes()
            .saturating_add(cache_value.estimated_resident_bytes());
        self.relationship_property_rows_cache.lock().await.insert_sized(
            cache_key,
            cache_value,
            cell_id.to_string(),
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_count_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let current_epoch = self.current_epoch(cell_id).await?;
        if read_epoch == current_epoch {
            let count = self.read_counter(&keys::relationship_count(cell_id, edge_type, src, dst)).await?;
            if count > 0 {
                return Ok(count);
            }
        }

        let relationship_records = self
            .relationship_record_count_for_edge_at(cell_id, edge_type, src, dst, read_epoch, budget)
            .await?;
        if relationship_records > 0 {
            return Ok(relationship_records);
        }

        let structural_exists = if read_epoch == current_epoch {
            self.edge_exists(cell_id, edge_type, src, dst).await?
        } else {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch).await?
        };
        Ok(u64::from(structural_exists))
    }

    #[cfg(feature = "opencypher")]
    async fn relationship_record_count_for_edge_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<u64> {
        let prefix = keys::relationship_edge_prefix(cell_id, edge_type, src, dst);
        let mut iter = self.scan_remote_point_prefix(&prefix).await?;
        let mut count = 0_u64;
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_relationship_count_records")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            decode_relationship_record(&key, &kv.value)?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_edge_property_index_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let mut edges = BTreeSet::<(VertexId, VertexId)>::new();
        for encoded in equivalent_property_index_keys(value) {
            for edge in self
                .scan_edge_property_index_current(cell_id, edge_type, property, &encoded, budget)
                .await?
            {
                edges.insert(edge);
                self.ensure_query_index_candidates("cypher_edge_property_index_candidates", edges.len())?;
            }
            for (src, dst, _relationship_id) in self
                .scan_relationship_property_index_current(cell_id, edge_type, property, &encoded, budget)
                .await?
            {
                edges.insert((src, dst));
                self.ensure_query_index_candidates("cypher_edge_property_index_candidates", edges.len())?;
            }
        }
        Ok(edges.into_iter().collect())
    }

    /// The entries an equality on `edge_type`.`property` names, split by kind:
    /// the endpoint pairs of the structural (unidentified) index, and each
    /// relationship-index entry with the id and encoded value it carries. The
    /// candidate bound counts distinct endpoint pairs, as
    /// [`Self::scan_edge_property_index_at`] does.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) async fn scan_relationship_property_entries_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        value: &VertexPropertyValue,
        budget: &QueryBudget,
    ) -> Result<RelationshipPropertyEntries> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let mut entries = RelationshipPropertyEntries::default();
        let mut pairs = BTreeSet::<(VertexId, VertexId)>::new();
        for encoded in equivalent_property_index_keys(value) {
            for edge in self
                .scan_edge_property_index_current(cell_id, edge_type, property, &encoded, budget)
                .await?
            {
                entries.structural_pairs.insert(edge);
                pairs.insert(edge);
                self.ensure_query_index_candidates("cypher_edge_property_index_candidates", pairs.len())?;
            }
            for (src, dst, relationship_id) in self
                .scan_relationship_property_index_current(cell_id, edge_type, property, &encoded, budget)
                .await?
            {
                entries.identified.push((src, dst, relationship_id, encoded.clone()));
                pairs.insert((src, dst));
                self.ensure_query_index_candidates("cypher_edge_property_index_candidates", pairs.len())?;
            }
        }
        Ok(entries)
    }

    /// A bounded speculative range probe. An overfull index is not an error:
    /// the physical plan falls back to its ordinary expansion. Only current
    /// epochs may use the current-property indexes.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) async fn try_scan_edge_property_lower_bound_at(
        &self,
        probe: RelationshipLowerBoundProbe<'_>,
    ) -> Result<Option<Vec<(VertexId, VertexId)>>> {
        let RelationshipLowerBoundProbe {
            cell_id,
            edge_type,
            property,
            lower,
            read_epoch,
            budget,
            max_entries,
        } = probe;
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }

        let max_entries = max_entries.min(self.limits.max_query_index_candidates);
        if max_entries == 0 {
            return Ok(None);
        }
        let mut candidates = EdgeCandidateBitmap::default();
        let mut scanned = 0_usize;
        for (base, identified) in [
            (keys::edge_property_index_property_prefix(cell_id, edge_type, property), false),
            (
                keys::relationship_property_index_property_prefix(cell_id, edge_type, property),
                true,
            ),
        ] {
            // Cypher orders strings above numbers. A numeric >= bound can
            // therefore also match a string-valued property. Float values
            // need a full type-prefix walk to avoid precision loss when an
            // i128 bound cannot be represented exactly as f64.
            for marker in ['i', 'j', 'n', 's'] {
                let start_suffix = match marker {
                    'i' if lower > u64::MAX as i128 => continue,
                    'i' if lower >= 0 => Some(format!("{:020}", lower as u64).into_bytes()),
                    'j' if lower > i64::MAX as i128 => continue,
                    'j' if lower >= i64::MIN as i128 => Some(format!("{:016x}", (lower as i64 as u64) ^ (1_u64 << 63)).into_bytes()),
                    _ => None,
                };
                let prefix = format!("{base}{marker}");
                let mut iter = budget
                    .read_only_io(
                        "cypher_relationship_range_probe_open",
                        self.storage
                            .scan_prefix_with_options(prefix.as_bytes(), start_suffix, &remote_scan_options()),
                    )
                    .await?;
                while let Some(kv) = budget
                    .read_only_io("cypher_relationship_range_probe_read", async {
                        iter.next().await.map_err(GraphError::from)
                    })
                    .await?
                {
                    scanned = scanned.saturating_add(1);
                    if scanned > max_entries {
                        return Ok(None);
                    }
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let (source, target) = if identified {
                        let (_, _, _, _, source, target, _) = parse_relationship_property_index_key(&key)?;
                        (source, target)
                    } else {
                        let (_, _, _, _, source, target) = parse_edge_property_index_key(&key)?;
                        (source, target)
                    };
                    candidates.insert(source, target);
                }
            }
        }
        Ok(Some(candidates.into_pairs()))
    }

    /// Enumerate one relationship type's live edges, or decline.
    ///
    /// `Some` is a complete answer for `read_epoch`: every live edge of the
    /// type, deduplicated, with segment tombstones applied. `None` means the
    /// type needed more than `max_entries` index entries to enumerate, so the
    /// caller keeps its label-anchored plan. Declining is always safe;
    /// returning a partial set never is, which is why every early return here
    /// is `None` rather than the pairs found so far.
    ///
    /// This is the same three representations `canonical_adjacency_at` unions —
    /// point out-edges, batched segments, segment tombstones — with an entry
    /// budget threaded through so a dense type is abandoned instead of read.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) async fn try_scan_edge_type_at(&self, probe: RelationshipTypeProbe<'_>) -> Result<Option<Vec<(VertexId, VertexId)>>> {
        let RelationshipTypeProbe {
            cell_id,
            edge_type,
            read_epoch,
            budget,
            max_entries,
        } = probe;
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        // The point out-edge keys carry no epoch of their own, so they answer
        // for the snapshot rather than for an arbitrary past sequence. Decline
        // a historical read instead of reasoning about when that coincides,
        // exactly as the range probe declines at `query.rs` above.
        if read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }
        let max_entries = max_entries.min(self.limits.max_query_index_candidates);
        if max_entries == 0 {
            return Ok(None);
        }

        let mut scanned = 0_usize;
        let mut pairs = BTreeSet::<(VertexId, VertexId)>::new();

        let prefix = keys::out_edge_type_prefix(cell_id, edge_type);
        let mut iter = budget
            .read_only_io("cypher_relationship_type_probe_open", self.scan_remote_prefix(&prefix))
            .await?;
        while let Some(kv) = budget
            .read_only_io("cypher_relationship_type_probe_read", async {
                iter.next().await.map_err(GraphError::from)
            })
            .await?
        {
            scanned = scanned.saturating_add(1);
            if scanned > max_entries {
                return Ok(None);
            }
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            pairs.insert((record.src, record.dst));
        }

        // Tombstones are read whole and charged against the same budget: a type
        // with more deletions than the budget allows is one this probe has no
        // business reading, and a partial tombstone map would resurrect deleted
        // edges rather than merely cost time.
        let prefix = keys::out_segment_tombstone_edge_type_prefix(cell_id, edge_type);
        let mut iter = budget
            .read_only_io("cypher_relationship_type_probe_tombstone_open", self.scan_remote_prefix(&prefix))
            .await?;
        let mut tombstones = BTreeMap::<(VertexId, VertexId), StorageSequence>::new();
        while let Some(kv) = budget
            .read_only_io("cypher_relationship_type_probe_tombstone_read", async {
                iter.next().await.map_err(GraphError::from)
            })
            .await?
        {
            scanned = scanned.saturating_add(1);
            if scanned > max_entries {
                return Ok(None);
            }
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, src, dst) = parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert((src, dst), epoch);
            }
        }

        let prefix = keys::out_segment_edge_type_prefix(cell_id, edge_type);
        let mut iter = budget
            .read_only_io("cypher_relationship_type_probe_segment_open", self.scan_remote_prefix(&prefix))
            .await?;
        while let Some(kv) = budget
            .read_only_io("cypher_relationship_type_probe_segment_read", async {
                iter.next().await.map_err(GraphError::from)
            })
            .await?
        {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            // The sequence is in the key, so a segment newer than the read can
            // be skipped without decoding it at all.
            let (_, _, segment_src, storage_sequence) = parse_out_edge_segment_key(&key)?;
            if storage_sequence > read_epoch {
                continue;
            }
            // Charged per destination, not per segment key: one segment can hold
            // an entire dense type, and a budget counting keys would wave it
            // through. Read the declared count first and decline before
            // decoding: `decode_out_edge_segment` sizes its `Vec` from that
            // count, so a dense type would otherwise be materialized in full and
            // then thrown away. Declining here is the same decision the
            // per-destination tally reached, since that tally counts every
            // destination whether or not a tombstone hides it.
            let declared = out_edge_segment_destination_count(&key, &kv.value)?;
            if scanned.saturating_add(declared) > max_entries {
                return Ok(None);
            }
            scanned = scanned.saturating_add(declared);
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            for dst in segment.destinations.iter().copied() {
                if segment_edge_visible(storage_sequence, tombstones.get(&(segment_src, dst)).copied()) {
                    pairs.insert((segment_src, dst));
                }
            }
        }

        Ok(Some(pairs.into_iter().collect()))
    }

    #[cfg(feature = "opencypher")]
    async fn try_scan_edge_property_values_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        values: &[VertexPropertyValue],
        budget: &QueryBudget,
        candidate_limit: usize,
    ) -> Result<Option<Vec<(VertexId, VertexId)>>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;

        let mut edges = BTreeSet::<(VertexId, VertexId)>::new();
        let mut scanned_entries = 0_usize;
        for value in values {
            for encoded in equivalent_property_index_keys(value) {
                let mut edge_iter = self
                    .scan_remote_prefix(&keys::edge_property_index_prefix(cell_id, edge_type, property, &encoded))
                    .await?;
                while let Some(kv) = edge_iter.next().await? {
                    budget.check("cypher_edge_property_seed_probe")?;
                    scanned_entries = scanned_entries.saturating_add(1);
                    if scanned_entries > candidate_limit {
                        return Ok(None);
                    }
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let (_cell_id, _edge_type, _property, _encoded, src, dst) = parse_edge_property_index_key(&key)?;
                    edges.insert((src, dst));
                    if edges.len() > candidate_limit {
                        return Ok(None);
                    }
                }

                let mut relationship_iter = self
                    .scan_remote_prefix(&keys::relationship_property_index_prefix(cell_id, edge_type, property, &encoded))
                    .await?;
                while let Some(kv) = relationship_iter.next().await? {
                    budget.check("cypher_relationship_property_seed_probe")?;
                    scanned_entries = scanned_entries.saturating_add(1);
                    if scanned_entries > candidate_limit {
                        return Ok(None);
                    }
                    let key = String::from_utf8_lossy(&kv.key).into_owned();
                    let (_cell_id, _edge_type, _property, _encoded, src, dst, _relationship_id) =
                        parse_relationship_property_index_key(&key)?;
                    edges.insert((src, dst));
                    if edges.len() > candidate_limit {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(edges.into_iter().collect()))
    }

    #[cfg(feature = "opencypher")]
    async fn scan_edge_property_index_current(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId)>> {
        let mut iter = budget
            .read_only_io(
                "cypher_edge_property_index_open",
                self.scan_remote_prefix(&keys::edge_property_index_prefix(cell_id, edge_type, property, encoded)),
            )
            .await?;
        let mut edges = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_edge_property_index_read", async { Ok(iter.next().await?) })
            .await?
        {
            budget.check("cypher_edge_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst) = parse_edge_property_index_key(&key)?;
            edges.push((src, dst));
            self.ensure_query_index_candidates("cypher_edge_property_index_candidates", edges.len())?;
        }
        Ok(edges)
    }

    #[cfg(feature = "opencypher")]
    async fn scan_relationship_property_index_current(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<(VertexId, VertexId, RelationshipId)>> {
        let mut iter = budget
            .read_only_io(
                "cypher_relationship_property_index_open",
                self.scan_remote_prefix(&keys::relationship_property_index_prefix(cell_id, edge_type, property, encoded)),
            )
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_relationship_property_index_read", async { Ok(iter.next().await?) })
            .await?
        {
            budget.check("cypher_relationship_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, src, dst, relationship_id) = parse_relationship_property_index_key(&key)?;
            relationships.push((src, dst, relationship_id));
            self.ensure_query_index_candidates("cypher_relationship_property_index_candidates", relationships.len())?;
        }
        Ok(relationships)
    }

    /// The (src, dst) pairs an edge property equality of `edge` names, read
    /// from the edge and relationship property indexes, or `None` when the
    /// pattern has no indexable equality, the read is not at the current
    /// epoch (the current-property indexes are), or more than
    /// `max_entries` index entries are read (entries, not pairs: parallel
    /// relationships each have one, and the probe must stay cheaper than the
    /// expansion it replaces). A source expansion uses it in place of checking each of
    /// a high-degree source's neighbors, which reads the relationship index
    /// once per neighbor: on a hub entity with thousands of edges, a
    /// single-edge write timed out at the query runtime limit.
    #[cfg(feature = "opencypher")]
    async fn try_edge_property_pairs_bounded(
        &self,
        cell_id: &str,
        edge: &RowEdgePattern,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        max_entries: usize,
    ) -> Result<Option<BTreeSet<(VertexId, VertexId)>>> {
        let Some((property, value)) = edge.properties.iter().next() else {
            return Ok(None);
        };
        if !value.is_scalar_indexable() || read_epoch != self.current_epoch(cell_id).await? {
            return Ok(None);
        }
        let max_entries = max_entries.min(self.limits.max_query_index_candidates);
        let mut pairs = BTreeSet::new();
        let mut entries_read = 0usize;
        for encoded in equivalent_property_index_keys(value) {
            let mut iter = budget
                .read_only_io(
                    "cypher_edge_property_index_open",
                    self.scan_remote_prefix(&keys::edge_property_index_prefix(cell_id, &edge.edge_type, property, &encoded)),
                )
                .await?;
            while let Some(kv) = budget
                .read_only_io("cypher_edge_property_index_read", async { Ok(iter.next().await?) })
                .await?
            {
                budget.check("cypher_edge_property_index")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let (_cell_id, _edge_type, _property, _encoded, src, dst) = parse_edge_property_index_key(&key)?;
                entries_read += 1;
                if entries_read > max_entries {
                    return Ok(None);
                }
                pairs.insert((src, dst));
            }
            let mut iter = budget
                .read_only_io(
                    "cypher_relationship_property_index_open",
                    self.scan_remote_prefix(&keys::relationship_property_index_prefix(
                        cell_id,
                        &edge.edge_type,
                        property,
                        &encoded,
                    )),
                )
                .await?;
            while let Some(kv) = budget
                .read_only_io("cypher_relationship_property_index_read", async { Ok(iter.next().await?) })
                .await?
            {
                budget.check("cypher_relationship_property_index")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let (_cell_id, _edge_type, _property, _encoded, src, dst, _relationship_id) = parse_relationship_property_index_key(&key)?;
                entries_read += 1;
                if entries_read > max_entries {
                    return Ok(None);
                }
                pairs.insert((src, dst));
            }
        }
        Ok(Some(pairs))
    }

    #[cfg(feature = "opencypher")]
    async fn scan_relationship_property_index_current_for_edge(
        &self,
        lookup: RelationshipPropertyEdgeIndexLookup<'_>,
    ) -> Result<Vec<RelationshipId>> {
        let RelationshipPropertyEdgeIndexLookup {
            cell_id,
            edge_type,
            property,
            encoded,
            src,
            dst,
            budget,
        } = lookup;
        let mut iter = budget
            .read_only_io(
                "cypher_relationship_property_index_edge_open",
                self.scan_remote_prefix(&keys::relationship_property_index_edge_prefix(
                    cell_id, edge_type, property, encoded, src, dst,
                )),
            )
            .await?;
        let mut relationships = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_relationship_property_index_edge_read", async { Ok(iter.next().await?) })
            .await?
        {
            budget.check("cypher_relationship_property_index_edge")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, _property, _encoded, parsed_src, parsed_dst, relationship_id) =
                parse_relationship_property_index_key(&key)?;
            if parsed_src != src || parsed_dst != dst {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "relationship property index edge prefix returned a different endpoint".to_string(),
                });
            }
            relationships.push(relationship_id);
            self.ensure_query_index_candidates("cypher_relationship_property_index_edge_candidates", relationships.len())?;
        }
        Ok(relationships)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_vertex_label_index_at(
        &self,
        cell_id: &str,
        label: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        self.scan_vertex_label_index_at_limited(cell_id, label, _read_epoch, budget, None)
            .await
    }

    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) async fn scan_vertex_ids_at(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        let prefix = keys::vertex_prefix(cell_id);
        let mut iter = snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &remote_scan_options())
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let vertex_id = key
                .strip_prefix(&prefix)
                .and_then(|value| value.parse::<VertexId>().ok())
                .ok_or_else(|| GraphError::CorruptValue {
                    key: key.clone(),
                    reason: "vertex key has an invalid id suffix".to_string(),
                })?;
            vertices.push(vertex_id);
            self.ensure_query_index_candidates("cypher_vertex_scan_candidates", vertices.len())?;
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_vertex_label_index_at_limited(
        &self,
        cell_id: &str,
        label: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
        max_results: Option<usize>,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("label", label)?;
        self.scan_vertex_label_index_current(cell_id, label, budget, max_results).await
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_label_index_current(
        &self,
        cell_id: &str,
        label: &str,
        budget: &QueryBudget,
        max_results: Option<usize>,
    ) -> Result<Vec<VertexId>> {
        let mut iter = budget
            .read_only_io(
                "cypher_vertex_label_index_open",
                self.scan_remote_prefix(&keys::vertex_label_prefix(cell_id, label)),
            )
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_vertex_label_index_read", async { Ok(iter.next().await?) })
            .await?
        {
            budget.check("cypher_vertex_label_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
            self.ensure_query_index_candidates("cypher_vertex_label_index_candidates", vertices.len())?;
            if max_results.is_some_and(|limit| vertices.len() >= limit) {
                break;
            }
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn scan_vertex_property_index_at(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        let mut vertices = BTreeSet::<VertexId>::new();
        for encoded in equivalent_property_index_keys(value) {
            for vertex_id in self.scan_vertex_property_index_current(cell_id, property, &encoded, budget).await? {
                vertices.insert(vertex_id);
                self.ensure_query_index_candidates("cypher_vertex_property_index_candidates", vertices.len())?;
            }
        }
        Ok(vertices.into_iter().collect())
    }

    #[cfg(feature = "opencypher")]
    pub(crate) async fn try_scan_vertex_property_value_at(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        budget: &QueryBudget,
        probe_allowance: usize,
        alternative_allowance: usize,
    ) -> Result<Option<Vec<VertexId>>> {
        let probe_allowance = std::sync::atomic::AtomicUsize::new(probe_allowance);
        let refused = std::sync::atomic::AtomicUsize::new(0);
        self.try_scan_vertex_property_value_within(cell_id, property, value, budget, &probe_allowance, &refused, alternative_allowance)
            .await
    }

    /// [`Self::try_scan_vertex_property_value_at`] drawing on an allowance
    /// several concurrent seeks may share: each entry read takes one, and the
    /// seek that finds none left declines. Seeks sharing it therefore read at
    /// most the allowance between them, however many there are, plus one
    /// refused entry each, which `refused` counts so a caller can report
    /// every entry its seeks really read.
    #[cfg(feature = "opencypher")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn try_scan_vertex_property_value_within(
        &self,
        cell_id: &str,
        property: &str,
        value: &VertexPropertyValue,
        budget: &QueryBudget,
        probe_allowance: &std::sync::atomic::AtomicUsize,
        refused: &std::sync::atomic::AtomicUsize,
        alternative_allowance: usize,
    ) -> Result<Option<Vec<VertexId>>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;

        let mut vertices = BTreeSet::new();
        let mut encodings = BTreeSet::new();
        let property_prefix = self.vertex_property_index_prefix_at(cell_id, property, budget).await?;
        for encoded in equivalent_property_index_keys(value) {
            if !encodings.insert(encoded.clone()) {
                continue;
            }
            if encodings.len() > alternative_allowance {
                return Ok(None);
            }
            let mut iter = budget
                .read_only_io(
                    "cypher_vertex_property_seed_probe_open",
                    self.scan_remote_prefix(&keys::vertex_property_index_value_prefix(&property_prefix, &encoded)),
                )
                .await?;
            loop {
                let Some(kv) = budget
                    .read_only_io("cypher_vertex_property_seed_probe_read", async { Ok(iter.next().await?) })
                    .await?
                else {
                    break;
                };
                if !take_one(probe_allowance) {
                    refused.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(None);
                }
                budget.check("cypher_vertex_property_seed_probe")?;
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                vertices.insert(decode_u64(&key, &kv.value)?);
            }
        }
        Ok(Some(vertices.into_iter().collect()))
    }

    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) async fn scan_vertex_property_values_at(
        &self,
        cell_id: &str,
        property: &str,
        _read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("property", property)?;
        let property_prefix = self.vertex_property_index_prefix_at(cell_id, property, budget).await?;
        let mut iter = self.scan_remote_prefix(&property_prefix).await?;
        let mut vertices = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            budget.check("cypher_vertex_property_values")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, indexed_property, _encoded, vertex_id) = parse_vertex_property_index_key(&key)?;
            if indexed_property != property {
                continue;
            }
            vertices.insert(vertex_id);
            self.ensure_query_index_candidates("cypher_vertex_property_value_candidates", vertices.len())?;
        }
        Ok(vertices.into_iter().collect())
    }

    #[cfg(feature = "opencypher")]
    async fn scan_vertex_property_index_current(
        &self,
        cell_id: &str,
        property: &str,
        encoded: &str,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        let property_prefix = self.vertex_property_index_prefix_at(cell_id, property, budget).await?;
        let mut iter = budget
            .read_only_io(
                "cypher_vertex_property_index_open",
                self.scan_remote_prefix(&keys::vertex_property_index_value_prefix(&property_prefix, encoded)),
            )
            .await?;
        let mut vertices = Vec::new();
        while let Some(kv) = budget
            .read_only_io("cypher_vertex_property_index_read", async { Ok(iter.next().await?) })
            .await?
        {
            budget.check("cypher_vertex_property_index")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            vertices.push(decode_u64(&key, &kv.value)?);
            self.ensure_query_index_candidates("cypher_vertex_property_index_candidates", vertices.len())?;
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    async fn hydrate_binding_metadata(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        row: &mut BindingRow,
        cache: &mut BTreeMap<VertexId, VertexMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        let bindings: Vec<_> = row.values.iter().map(|(name, id)| (name.clone(), *id)).collect();
        for (binding, vertex_id) in bindings {
            budget.check("cypher_metadata_hydration")?;
            let metadata = match cache.get(&vertex_id) {
                Some(metadata) => metadata.clone(),
                None => {
                    let started = Instant::now();
                    // Spanned on the miss for the same reason it is timed there:
                    // the span count is then the number of storage reads, not the
                    // number of rows. A thousand rows over twenty distinct
                    // vertices emits twenty spans, and the trace shows the fetches
                    // rather than the iteration.
                    let metadata = self
                        .vertex_metadata_at(cell_id, vertex_id, read_epoch, budget)
                        .instrument(tracing::info_span!(
                            "query.property_fetch",
                            hydradb.cell_id = %cell_id,
                            hydradb.property_owner = "vertex",
                        ))
                        .await?;
                    self.operation_metrics.record_property_fetch(started.elapsed().as_micros() as u64);
                    cache.insert(vertex_id, metadata.clone());
                    metadata
                }
            };
            row.metadata.insert(binding, metadata);
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    async fn hydrate_row_relationship_metadata(
        &self,
        cell_id: &str,
        read_epoch: StorageSequence,
        row: &mut BindingRow,
        pattern: &RowEdgePattern,
        cache: &mut BTreeMap<BoundRelationship, EdgeMetadata>,
        budget: &QueryBudget,
    ) -> Result<()> {
        if pattern.binding.is_none() && pattern.properties.is_empty() {
            return Ok(());
        }
        let relationship = relationship_identity_for_pattern(row, pattern)?;
        budget.check("cypher_relationship_metadata_hydration")?;
        let metadata = match cache.get(&relationship) {
            Some(metadata) => metadata.clone(),
            None => {
                let metadata = match row.relationship_metadata.get(&relationship) {
                    Some(metadata) => metadata.clone(),
                    None => {
                        let started = Instant::now();
                        let metadata = self
                            .relationship_metadata_at(cell_id, &relationship, read_epoch, budget)
                            .instrument(tracing::info_span!(
                                "query.property_fetch",
                                hydradb.cell_id = %cell_id,
                                hydradb.property_owner = "relationship",
                            ))
                            .await?;
                        self.operation_metrics.record_property_fetch(started.elapsed().as_micros() as u64);
                        metadata
                    }
                };
                cache.insert(relationship.clone(), metadata.clone());
                metadata
            }
        };
        row.relationship_metadata.insert(relationship, metadata);
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    fn finish_projected_rows(
        &self,
        columns: Vec<QueryColumn>,
        mut projected: Vec<ProjectedQueryRow>,
        order_by: &[RowSort],
        distinct: bool,
        window: QueryWindow,
        budget: &QueryBudget,
    ) -> Result<QueryResultSet> {
        budget.check("cypher_finish_rows")?;
        if distinct {
            let mut deduped = Vec::with_capacity(projected.len());
            let mut seen = BTreeSet::new();
            for row in projected {
                budget.check("cypher_distinct_rows")?;
                if seen.insert(row.row.values.iter().cloned().map(SemanticQueryValue).collect::<Vec<_>>()) {
                    deduped.push(row);
                }
            }
            projected = deduped;
            self.ensure_query_intermediate_rows("cypher_distinct_rows", projected.len())?;
        }
        if !order_by.is_empty() {
            projected.sort_by(|left, right| compare_projected_rows(left, right, order_by));
        }
        budget.check("cypher_sort_rows")?;

        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let max = self.limits.max_query_result_vertices;
        let mut rows: Vec<_> = projected.into_iter().skip(skip).map(|projected| projected.row).collect();
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            rows.truncate(limit);
        } else {
            ensure_limit("query_result_rows", rows.len() as u64, max as u64)?;
        }
        Ok(QueryResultSet::new(columns, rows))
    }

    async fn out_neighbors_window_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
        window: QueryWindow,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<VertexId>> {
        check_optional_query_budget(budget, "query_out_neighbors_window")?;
        let vertices = match budget {
            Some(budget) => self.out_neighbors_at_for_query(cell_id, edge_type, src, read_epoch, budget).await?,
            None => self.out_neighbors_at(cell_id, edge_type, src, read_epoch).await?,
        };
        self.apply_query_window(vertices, window)
    }

    fn apply_query_window(&self, vertices: Vec<VertexId>, window: QueryWindow) -> Result<Vec<VertexId>> {
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let windowed: Vec<_> = vertices.into_iter().skip(skip).collect();
        self.apply_query_window_fetch_result(windowed, window)
    }

    fn apply_query_window_fetch_result(&self, mut vertices: Vec<VertexId>, window: QueryWindow) -> Result<Vec<VertexId>> {
        let max = self.limits.max_query_result_vertices;
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            vertices.truncate(limit);
        } else {
            ensure_limit("query_result_vertices", vertices.len() as u64, max as u64)?;
        }
        Ok(vertices)
    }

    #[cfg(feature = "opencypher")]
    fn apply_query_row_window(&self, rows: Vec<QueryRow>, window: QueryWindow) -> Result<Vec<QueryRow>> {
        let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
            operation: "query_result_skip",
            actual: window.skip,
            limit: usize::MAX as u64,
        })?;
        let mut rows: Vec<_> = rows.into_iter().skip(skip).collect();
        let max = self.limits.max_query_result_vertices;
        if let Some(limit) = window.limit {
            ensure_limit("query_result_limit", limit as u64, max as u64)?;
            rows.truncate(limit);
        } else {
            ensure_limit("query_result_rows", rows.len() as u64, max as u64)?;
        }
        Ok(rows)
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_graph_kernel_opencypher_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(request) = graph_kernel_row_query_request(query) else {
            return Ok(None);
        };
        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let page_window = page_context.result_window;
        if page_window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(query.columns.clone(), Vec::new(), None)));
        }

        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_graph_kernel_page")?;
        if request.edge.dst.id.is_none() {
            match request.projection {
                GraphKernelProjection::NodeId => {
                    let (mut vertices, edge_visits) = Box::pin(self.reachable_vertices_window_in_hop_range_at(ReachableWindowRequest {
                        cell_id: &context.cell_id,
                        edge_type: &request.edge.edge_type,
                        src: request.src,
                        hop_range: request.hop_range,
                        read_epoch,
                        window: page_window,
                        ascending: request.ascending,
                        budget: &budget,
                    }))
                    .await?;
                    self.ensure_query_scan_edges("cypher_graph_kernel_page_window_edge_visits", edge_visits)?;
                    let has_next = vertices.len() > page_size;
                    vertices.truncate(page_size);
                    let rows = graph_kernel_node_id_rows(vertices, &budget)?;
                    return Ok(Some(QueryResultPage::new(
                        query.columns.clone(),
                        rows,
                        query_next_cursor(cursor_offset, page_size, has_next)?,
                    )));
                }
                GraphKernelProjection::CountAll => {
                    let (count, edge_visits) = self
                        .reachable_count_in_hop_range_at(
                            &context.cell_id,
                            &request.edge.edge_type,
                            request.src,
                            request.hop_range,
                            read_epoch,
                            &budget,
                        )
                        .await?;
                    self.ensure_query_scan_edges("cypher_graph_kernel_page_count_edge_visits", edge_visits)?;
                    let rows = vec![QueryRow::new(vec![QueryValue::Count(count)])];
                    let mut rows = self.apply_query_row_window(rows, page_window)?;
                    let has_next = rows.len() > page_size;
                    rows.truncate(page_size);
                    return Ok(Some(QueryResultPage::new(
                        query.columns.clone(),
                        rows,
                        query_next_cursor(cursor_offset, page_size, has_next)?,
                    )));
                }
            }
        }

        let (mut vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(
                &context.cell_id,
                &request.edge.edge_type,
                request.src,
                request.hop_range,
                read_epoch,
                &budget,
            )
            .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_page_edge_visits", edge_visits)?;
        if let Some(dst) = request.edge.dst.id {
            vertices.retain(|vertex| *vertex == dst);
        }
        graph_kernel_order_vertices(&mut vertices, request.ascending);

        let rows = match request.projection {
            GraphKernelProjection::NodeId => {
                let mut vertices = self.apply_query_window(vertices, page_window)?;
                let has_next = vertices.len() > page_size;
                vertices.truncate(page_size);
                let mut rows = Vec::with_capacity(vertices.len());
                for vertex in vertices {
                    budget.check("cypher_graph_kernel_page_project")?;
                    rows.push(QueryRow::new(vec![QueryValue::VertexId(vertex)]));
                }
                return Ok(Some(QueryResultPage::new(
                    query.columns.clone(),
                    rows,
                    query_next_cursor(cursor_offset, page_size, has_next)?,
                )));
            }
            GraphKernelProjection::CountAll => {
                let rows = vec![QueryRow::new(vec![QueryValue::Count(vertices.len() as u64)])];
                self.apply_query_row_window(rows, page_window)?
            }
        };
        let has_next = rows.len() > page_size;
        let mut rows = rows;
        rows.truncate(page_size);
        Ok(Some(QueryResultPage::new(
            query.columns.clone(),
            rows,
            query_next_cursor(cursor_offset, page_size, has_next)?,
        )))
    }

    #[cfg(feature = "opencypher")]
    async fn try_execute_streaming_opencypher_rows_page(
        &self,
        context: &QueryContext,
        query: &ParsedRowQuery,
        cursor_offset: u64,
        page_size: usize,
    ) -> Result<Option<QueryResultPage>> {
        let Some(edge) = streaming_neighbor_page_edge(query) else {
            return Ok(None);
        };
        if !streaming_neighbor_order_supported(edge, &query.projections, &query.columns, &query.order_by) {
            return Ok(None);
        }

        let page_context = self.query_page_context(context.clone(), cursor_offset, page_size)?;
        let page_window = page_context.result_window;
        if page_window.limit == Some(0) {
            return Ok(Some(QueryResultPage::new(query.columns.clone(), Vec::new(), None)));
        }

        let read_epoch = self.query_read_epoch(context).await?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        );
        budget.check("cypher_rows_page_stream")?;
        let Some(src) = edge.src.id else {
            return Ok(None);
        };
        let mut vertices = self
            .out_neighbors_window_at(&context.cell_id, &edge.edge_type, src, read_epoch, page_window, Some(&budget))
            .await?;
        let has_next = vertices.len() > page_size;
        vertices.truncate(page_size);

        let mut rows = Vec::with_capacity(vertices.len());
        for dst in vertices {
            budget.check("cypher_rows_page_stream_project")?;
            rows.push(QueryRow::new(streaming_neighbor_projection_values(
                edge,
                src,
                dst,
                &query.projections,
            )?));
        }
        let next_cursor = if has_next {
            query_next_cursor(cursor_offset, page_size, true)?
        } else {
            None
        };
        Ok(Some(QueryResultPage::new(query.columns.clone(), rows, next_cursor)))
    }

    #[cfg(feature = "opencypher")]
    fn record_streaming_query_rows_success(&self, row_count: usize, started: std::time::Instant) {
        self.operation_metrics.query_rows_started.fetch_add(1, Ordering::Relaxed);
        self.operation_metrics.query_rows_completed.fetch_add(1, Ordering::Relaxed);
        self.operation_metrics
            .query_rows_returned
            .fetch_add(row_count as u64, Ordering::Relaxed);
        self.operation_metrics.query_rows_latency.record(started.elapsed());
    }

    #[cfg(feature = "opencypher")]
    fn record_streaming_query_rows_failure(&self, started: std::time::Instant, error: &GraphError) {
        self.operation_metrics.query_rows_started.fetch_add(1, Ordering::Relaxed);
        self.operation_metrics.record_query_rows_failure(error);
        self.operation_metrics.query_rows_latency.record(started.elapsed());
    }

    #[cfg(feature = "opencypher")]
    fn query_page_context(&self, context: QueryContext, cursor_offset: u64, page_size: usize) -> Result<QueryContext> {
        let max = self.limits.max_query_result_vertices;
        if page_size == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "query_page_size",
                actual: 0,
                limit: max as u64,
            });
        }
        let max_page_size = max.saturating_sub(1);
        ensure_limit("query_page_size", page_size as u64, max_page_size as u64)?;

        let base_window = context.result_window;
        let skip = base_window.skip.checked_add(cursor_offset).ok_or(GraphError::AdmissionRejected {
            operation: "query_cursor_offset",
            actual: u64::MAX,
            limit: u64::MAX - 1,
        })?;
        let probe_limit = match base_window.limit {
            Some(limit) => {
                let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
                if cursor_offset >= limit_u64 {
                    0
                } else {
                    let remaining = limit_u64 - cursor_offset;
                    usize::try_from(remaining).unwrap_or(usize::MAX).min(page_size.saturating_add(1))
                }
            }
            None => page_size.saturating_add(1),
        };
        Ok(context.with_result_window(skip, Some(probe_limit)))
    }

    #[cfg(feature = "opencypher")]
    pub(super) fn ensure_query_intermediate_rows(&self, operation: &'static str, rows: usize) -> Result<()> {
        ensure_limit(operation, rows as u64, self.limits.max_query_intermediate_rows as u64)
    }

    #[cfg(feature = "opencypher")]
    pub(super) fn ensure_query_index_candidates(&self, operation: &'static str, candidates: usize) -> Result<()> {
        ensure_limit(operation, candidates as u64, self.limits.max_query_index_candidates as u64)
    }

    /// Charge index entries to the query's shared tally and fail once the whole
    /// query has read more of an index than `max_query_index_candidates`
    /// allows.
    ///
    /// The count above is per call, which is the same thing wherever one scan
    /// answers one query. A paged walk asks once per page, so its accounting
    /// has to live on the budget its pages share or it bounds nothing.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(super) fn charge_query_index_candidates(&self, operation: &'static str, budget: &QueryBudget, entries: u64) -> Result<()> {
        ensure_limit(
            operation,
            budget.add_scanned_index_candidates(entries),
            self.limits.max_query_index_candidates as u64,
        )
    }

    #[cfg(feature = "opencypher")]
    pub(super) fn ensure_query_scan_edges(&self, operation: &'static str, edges: u64) -> Result<()> {
        ensure_limit(operation, edges, self.limits.max_query_scan_edges)
    }

    #[cfg(feature = "opencypher")]
    fn push_binding_row(&self, rows: &mut Vec<BindingRow>, row: BindingRow, operation: &'static str) -> Result<()> {
        let next_len = rows.len().checked_add(1).ok_or_else(|| GraphError::AdmissionRejected {
            operation,
            actual: u64::MAX,
            limit: self.limits.max_query_intermediate_rows as u64,
        })?;
        self.ensure_query_intermediate_rows(operation, next_len)?;
        rows.push(row);
        Ok(())
    }

    async fn query_edge_exists(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: Option<StorageSequence>,
    ) -> Result<bool> {
        if let Some(read_epoch) = read_epoch {
            self.edge_exists_at(cell_id, edge_type, src, dst, read_epoch).await
        } else {
            self.edge_exists(cell_id, edge_type, src, dst).await
        }
    }

    fn validate_reachable_hop_request(
        &self,
        operation: &'static str,
        cell_id: &str,
        edge_type: &str,
        hop_range: (u8, u8),
    ) -> Result<(u8, u8)> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let (min_hops, max_hops) = hop_range;
        if min_hops > max_hops {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Pattern,
                dialect: "OpenCypher",
                feature: "invalid variable-length hop range".to_string(),
            });
        }
        ensure_limit(operation, u64::from(max_hops), u64::from(self.limits.max_traversal_hops))?;
        Ok((min_hops, max_hops))
    }

    async fn reachable_vertices_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<(Vec<VertexId>, u64)> {
        budget.check("cypher_match_reachable")?;
        let (min_hops, max_hops) = self.validate_reachable_hop_request("cypher_match_reachable", cell_id, edge_type, hop_range)?;
        if let Some(result) = self
            .reachable_vertices_with_compiled_graph_kernel(cell_id, edge_type, src, hop_range, read_epoch, budget)
            .await?
        {
            return Ok(result);
        }

        let traversal = self
            .reachable_from_storage_frontier(cell_id, edge_type, src, (min_hops, max_hops), read_epoch, budget)
            .await?;
        self.operation_metrics
            .query_rust_sparse_fallbacks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok((traversal.vertices, traversal.edge_visits))
    }

    pub(crate) async fn reachable_from_storage_frontier(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<crate::sparse_kernel::SparseTraversal> {
        let (min_hops, max_hops) = hop_range;
        let snapshot = self.storage.snapshot().await?;
        let mut frontier = BTreeSet::from([src]);
        let mut reachable = BTreeSet::new();
        let mut edge_visits = 0_u64;
        for hop in 1..=max_hops {
            if frontier.is_empty() {
                break;
            }
            let mut next = BTreeSet::new();
            for source in frontier {
                budget.check("graph_storage_frontier_source")?;
                let neighbors = self
                    .out_neighbors_in_storage_snapshot(&snapshot, cell_id, edge_type, source, read_epoch)
                    .await?;
                edge_visits = edge_visits.saturating_add(neighbors.len() as u64);
                ensure_limit("graph_storage_frontier_edges", edge_visits, self.limits.max_query_scan_edges)?;
                next.extend(neighbors);
            }
            ensure_limit(
                "graph_storage_frontier_vertices",
                next.len() as u64,
                self.limits.max_query_intermediate_rows as u64,
            )?;
            if hop >= min_hops {
                reachable.extend(next.iter().copied());
                ensure_limit(
                    "graph_storage_reachable_vertices",
                    reachable.len() as u64,
                    self.limits.max_query_result_vertices as u64,
                )?;
            }
            frontier = next;
        }
        Ok(crate::sparse_kernel::SparseTraversal {
            vertices: reachable.into_iter().collect(),
            edge_visits,
            backend: SparseKernelBackend::Adjacency,
        })
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_count_in_hop_range_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<(u64, u64)> {
        budget.check("cypher_match_reachable_count")?;
        let hop_range = self.validate_reachable_hop_request("cypher_match_reachable_count", cell_id, edge_type, hop_range)?;
        if let Some(result) = self
            .reachable_count_with_compiled_graph_kernel(cell_id, edge_type, src, hop_range, read_epoch, budget)
            .await?
        {
            return Ok(result);
        }
        let (vertices, edge_visits) = self
            .reachable_vertices_in_hop_range_at(cell_id, edge_type, src, hop_range, read_epoch, budget)
            .await?;
        Ok((vertices.len() as u64, edge_visits))
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_vertices_window_in_hop_range_at(&self, request: ReachableWindowRequest<'_>) -> Result<(Vec<VertexId>, u64)> {
        validate_query_result_window(request.window, self.limits.max_query_result_vertices)?;
        self.validate_reachable_hop_request(
            "cypher_match_reachable_window",
            request.cell_id,
            request.edge_type,
            request.hop_range,
        )?;
        let cache_key = SourceRelationshipRowsCacheKey::reachable(
            request.cell_id,
            request.edge_type,
            request.src,
            request.hop_range,
            request.read_epoch,
        );
        if let Some(cached) = self.source_relationship_rows_cache.lock().await.get(&cache_key) {
            self.cache_metrics.record_hit(GraphCacheKind::RelationshipRows);
            return Ok((
                graph_kernel_window_sorted_vertices(cached.as_slice(), request.window, request.ascending)?,
                0,
            ));
        }
        self.cache_metrics.record_miss(GraphCacheKind::RelationshipRows);

        let (mut vertices, edge_visits) = Box::pin(self.reachable_vertices_in_hop_range_at(
            request.cell_id,
            request.edge_type,
            request.src,
            request.hop_range,
            request.read_epoch,
            request.budget,
        ))
        .await?;
        self.ensure_query_scan_edges("cypher_graph_kernel_page_window_edge_visits", edge_visits)?;
        graph_kernel_order_vertices(&mut vertices, true);
        let cached = Arc::new(vertices);
        let resident_bytes = source_relationship_rows_resident_bytes(&cache_key, &cached);
        self.source_relationship_rows_cache.lock().await.insert_sized(
            cache_key,
            Arc::clone(&cached),
            request.cell_id,
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok((
            graph_kernel_window_sorted_vertices(cached.as_slice(), request.window, request.ascending)?,
            edge_visits,
        ))
    }

    async fn reachable_vertices_with_compiled_graph_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<(Vec<VertexId>, u64)>> {
        let _ = (cell_id, edge_type, src, hop_range, read_epoch, budget);
        if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy) == SparseKernelBackend::Adjacency {
            return Ok(None);
        }
        {
            let (min_hops, max_hops) = hop_range;
            budget.check("cypher_graphblas_artifact_lookup")?;
            let Some(artifact) = self.traced_latest_matrix_artifact(cell_id, edge_type, read_epoch).await? else {
                return Ok(None);
            };
            budget.check("cypher_graphblas_compiled_matrix")?;
            let Some((compiled, overlay, rebuilt)) = self
                .compiled_graphblas_query_snapshot(cell_id, edge_type, artifact.base_epoch, read_epoch, budget)
                .await?
            else {
                return Ok(None);
            };
            self.record_graphblas_snapshot(rebuilt);
            let traversal = run_graph_compute(Arc::clone(&self.operation_metrics), "graphblas_expand_range", move || {
                if let Some(overlay) = overlay {
                    expand_range_with_overlay(&compiled, &overlay, &[src], min_hops, max_hops)
                } else {
                    let empty_adjacency = BTreeMap::new();
                    crate::sparse_kernel::expand_range_compiled_graphblas(&compiled, &empty_adjacency, &[src], min_hops, max_hops)
                }
            })
            .await?;
            Ok(Some((traversal.vertices, traversal.edge_visits)))
        }
    }

    #[cfg(feature = "opencypher")]
    async fn reachable_count_with_compiled_graph_kernel(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        hop_range: (u8, u8),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<(u64, u64)>> {
        let _ = (cell_id, edge_type, src, hop_range, read_epoch, budget);
        if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy) == SparseKernelBackend::Adjacency {
            return Ok(None);
        }
        {
            let (min_hops, max_hops) = hop_range;
            budget.check("cypher_graphblas_count_artifact_lookup")?;
            let artifact_started = std::time::Instant::now();
            let Some(artifact) = self.traced_latest_matrix_artifact(cell_id, edge_type, read_epoch).await? else {
                return Ok(None);
            };
            self.operation_metrics.query_artifact_lookup_us.fetch_add(
                artifact_started.elapsed().as_micros().try_into().unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
            budget.check("cypher_graphblas_count_compiled_matrix")?;
            let cache_started = std::time::Instant::now();
            let Some((compiled, overlay, rebuilt)) = self
                .compiled_graphblas_query_snapshot(cell_id, edge_type, artifact.base_epoch, read_epoch, budget)
                .await?
            else {
                return Ok(None);
            };
            self.record_graphblas_snapshot(rebuilt);
            self.operation_metrics.query_graphblas_cache_us.fetch_add(
                cache_started.elapsed().as_micros().try_into().unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
            let run_inline = overlay.is_some() || compiled.prefer_inline_count(max_hops);
            let compute = move || {
                if let Some(overlay) = overlay {
                    let traversal = expand_range_with_overlay(&compiled, &overlay, &[src], min_hops, max_hops)?;
                    Ok(crate::sparse_kernel::SparseTraversalCount {
                        vertices: traversal.vertices.len() as u64,
                        edge_visits: traversal.edge_visits,
                        backend: traversal.backend,
                    })
                } else {
                    let empty_adjacency = BTreeMap::new();
                    crate::sparse_kernel::expand_range_count_compiled_graphblas(&compiled, &empty_adjacency, &[src], min_hops, max_hops)
                }
            };
            let traversal = if run_inline {
                run_graph_compute_inline(Arc::clone(&self.operation_metrics), compute)?
            } else {
                run_graph_compute(Arc::clone(&self.operation_metrics), "graphblas_expand_range_count", compute).await?
            };
            Ok(Some((traversal.vertices, traversal.edge_visits)))
        }
    }

    /// `latest_matrix_artifact` with the `artifact.lookup` span around it.
    ///
    /// The generation this resolves to is the join key that BFG-006, BFG-013
    /// and BFG-014 are all really about: the question in every one of them is
    /// whether the write, the index cycle and the read agreed on a generation.
    /// The indexer records the same `cell_id` and `base_sequence` on the span
    /// that produced the artifact, so the join is a backend query rather than
    /// two log files and a clock.
    pub(crate) async fn traced_latest_matrix_artifact(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
    ) -> Result<Option<MatrixArtifact>> {
        let span = tracing::info_span!(
            "artifact.lookup",
            hydradb.cell_id = %cell_id,
            hydradb.edge_type = %edge_type,
            hydradb.read_epoch = read_epoch,
            hydradb.base_sequence = tracing::field::Empty,
            hydradb.outcome = tracing::field::Empty,
            error.class = tracing::field::Empty,
            hydradb.sampling.tail_keep = tracing::field::Empty,
        );
        let artifact = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .instrument(span.clone())
            .await;
        match &artifact {
            Ok(Some(artifact)) => {
                span.record("hydradb.outcome", "hit");
                span.record("hydradb.base_sequence", artifact.base_epoch);
            }
            // Absence is the interesting case: it means the read fell off the
            // compiled path and back onto a scan.
            Ok(None) => {
                span.record("hydradb.outcome", "miss");
            }
            Err(err) => {
                span.record("error.class", err.class());
                span.record("hydradb.sampling.tail_keep", "error");
            }
        }
        artifact
    }

    pub(crate) async fn compiled_graphblas_query_snapshot(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: StorageSequence,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<
        Option<(
            Arc<crate::sparse_kernel::CompiledGraphBlasMatrix>,
            Option<GraphTopologyOverlay>,
            bool,
        )>,
    > {
        if let Some(generation) = self.graph_index_generation_at(cell_id, edge_type, base_epoch).await? {
            let storage_snapshot = self.storage.snapshot().await?;
            if storage_snapshot.seq() != read_epoch {
                return Ok(None);
            }
            let mut generation = generation;
            for refresh_attempt in 0..=1 {
                let Some(compiled) = self.cached_graphblas_matrix(cell_id, edge_type, generation.base_sequence).await? else {
                    if refresh_attempt == 0 {
                        if let Some(latest) = self.discover_graph_index(cell_id, edge_type).await? {
                            if latest.base_sequence > generation.base_sequence && latest.base_sequence <= read_epoch {
                                generation = latest;
                                continue;
                            }
                        }
                    }
                    return Ok(None);
                };
                if generation.base_sequence >= read_epoch {
                    return Ok(Some((compiled, None, false)));
                }
                // The gap between the compiled generation and the read epoch,
                // replayed from the WAL. BFG-011 was a hole in exactly this
                // span's window, so both ends of it are recorded: what the
                // artifact was built from and what the read needs to see.
                let tail_span = tracing::info_span!(
                    "storage.wal_tail",
                    hydradb.cell_id = %cell_id,
                    hydradb.edge_type = %edge_type,
                    hydradb.read_epoch = read_epoch,
                    hydradb.base_sequence = generation.base_sequence,
                    hydradb.outcome = tracing::field::Empty,
                    error.class = tracing::field::Empty,
                    hydradb.sampling.tail_keep = tracing::field::Empty,
                );
                let tail = self
                    .topology_tail_since(&generation, storage_snapshot.as_ref(), read_epoch, budget)
                    .instrument(tail_span.clone())
                    .await;
                match &tail {
                    Ok(GraphTopologyTail::Complete(_)) => {
                        tail_span.record("hydradb.outcome", "complete");
                    }
                    Ok(GraphTopologyTail::Unavailable) => {
                        tail_span.record("hydradb.outcome", "unavailable");
                    }
                    Err(err) => {
                        tail_span.record("error.class", err.class());
                        tail_span.record("hydradb.sampling.tail_keep", "error");
                    }
                }
                match tail {
                    Ok(GraphTopologyTail::Complete(overlay)) => {
                        return Ok(Some((compiled, Some(overlay), false)));
                    }
                    Ok(GraphTopologyTail::Unavailable) => return Ok(None),
                    Err(err)
                        if refresh_attempt == 0
                            && matches!(
                                &err,
                                GraphError::AdmissionRejected {
                                    operation: "graph_index_wal_affected_edges",
                                    ..
                                }
                            ) =>
                    {
                        let Some(latest) = self.discover_graph_index(cell_id, edge_type).await? else {
                            return Err(err);
                        };
                        if latest.base_sequence <= generation.base_sequence || latest.base_sequence > read_epoch {
                            return Err(err);
                        }
                        generation = latest;
                    }
                    Err(err) => return Err(err),
                }
            }
            unreachable!("graph index tail refresh loop returns on its second attempt");
        }
        if base_epoch < read_epoch {
            budget.check("cypher_graphblas_adjacency_generation")?;
            let adjacency_generation = self.read_counter(&keys::adjacency_generation(cell_id, edge_type)).await?;
            if adjacency_generation == 0 || adjacency_generation > base_epoch {
                return Ok(None);
            }
        }
        Ok(self
            .cached_graphblas_matrix(cell_id, edge_type, base_epoch)
            .await?
            .map(|compiled| (compiled, None, false)))
    }

    /// The compiled matrix of the newest index generation published at or
    /// before `read_epoch`, with that generation, and none of the WAL tail
    /// since. `None` until the indexer has published one, or when the
    /// configured kernel has no compiled form.
    #[cfg(feature = "opencypher")]
    pub(super) async fn published_generation_matrix(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Option<(GraphIndexGeneration, Arc<crate::sparse_kernel::CompiledGraphBlasMatrix>)>> {
        // The published pointer is read on every call, not the cached
        // artifact: a cached older generation would
        // keep answering after the indexer moved on. An older generation is
        // still used when the newest one is past the read epoch.
        let published = budget
            .read_only_io("gds_generation_lookup", self.discover_graph_index(cell_id, edge_type))
            .await?
            .filter(|generation| generation.base_sequence <= read_epoch);
        let generation = match published {
            Some(generation) => generation,
            None => {
                let Some(artifact) = budget
                    .read_only_io(
                        "gds_artifact_lookup",
                        self.traced_latest_matrix_artifact(cell_id, edge_type, read_epoch),
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let Some(generation) = self.graph_index_generation_at(cell_id, edge_type, artifact.base_epoch).await? else {
                    return Ok(None);
                };
                generation
            }
        };
        Ok(self
            .cached_graphblas_matrix(cell_id, edge_type, generation.base_sequence)
            .await?
            .map(|matrix| (generation, matrix)))
    }

    pub(crate) fn record_graphblas_snapshot(&self, rebuilt: bool) {
        let counter = if rebuilt {
            &self.operation_metrics.query_graphblas_rebuilt_snapshots
        } else {
            &self.operation_metrics.query_graphblas_artifact_snapshots
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn edge_exists(&self, cell_id: &str, edge_type: &str, src: VertexId, dst: VertexId) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "edge_exists").await?;
        self.snapshot(cell_id).await?.edge_exists(edge_type, src, dst).await
    }

    pub async fn out_neighbors(&self, cell_id: &str, edge_type: &str, src: VertexId) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "out_neighbors").await?;
        self.snapshot(cell_id).await?.out_neighbors(edge_type, src).await
    }

    pub async fn out_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        let read_epoch = self.current_epoch(cell_id).await?;
        self.out_neighbors_batch_at(cell_id, edge_type, sources, read_epoch).await
    }

    pub async fn out_neighbors_batch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
    ) -> Result<Vec<NeighborBatchEntry>> {
        self.out_neighbors_batch_at_with_cancellation(cell_id, edge_type, sources, read_epoch, None)
            .await
    }

    pub(crate) async fn out_neighbors_batch_at_with_cancellation(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
        cancellation_token: Option<QueryCancellationToken>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "out_neighbors_batch").await?;
        let sources: Vec<_> = sources.into_iter().collect();
        ensure_limit(
            "out_neighbors_batch_sources",
            sources.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        if sources.is_empty() {
            return Ok(Vec::new());
        }

        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, cancellation_token);
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let requested: BTreeSet<_> = sources.iter().copied().collect();
        let mut by_source = requested
            .iter()
            .copied()
            .map(|source| (source, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let mut scanned_edges = 0_u64;

        for source in requested.iter().copied() {
            budget.check("out_neighbors_batch_snapshot_source")?;
            let neighbors = snapshot.out_neighbors(edge_type, source).await?;
            scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
            ensure_limit("out_neighbors_batch_scanned_edges", scanned_edges, self.limits.max_query_scan_edges)?;
            by_source.entry(source).or_default().extend(neighbors);
        }
        Ok(sources
            .into_iter()
            .map(|vertex| NeighborBatchEntry {
                vertex,
                neighbors: by_source
                    .get(&vertex)
                    .map(|neighbors| neighbors.iter().copied().collect())
                    .unwrap_or_default(),
            })
            .collect())
    }

    pub async fn in_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        let read_epoch = self.current_epoch(cell_id).await?;
        self.in_neighbors_batch_at(cell_id, edge_type, destinations, read_epoch).await
    }

    pub async fn in_neighbors_batch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
    ) -> Result<Vec<NeighborBatchEntry>> {
        self.in_neighbors_batch_at_with_cancellation(cell_id, edge_type, destinations, read_epoch, None)
            .await
    }

    pub(crate) async fn in_neighbors_batch_at_with_cancellation(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
        read_epoch: StorageSequence,
        cancellation_token: Option<QueryCancellationToken>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "in_neighbors_batch").await?;
        let destinations: Vec<_> = destinations.into_iter().collect();
        ensure_limit(
            "in_neighbors_batch_destinations",
            destinations.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        if destinations.is_empty() {
            return Ok(Vec::new());
        }

        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, cancellation_token);
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let requested: BTreeSet<_> = destinations.iter().copied().collect();
        let mut by_destination = requested
            .iter()
            .copied()
            .map(|destination| (destination, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let mut scanned_edges = 0_u64;

        if self.writes_reverse_index() {
            for destination in requested.iter().copied() {
                budget.check("in_neighbors_batch_snapshot_destination")?;
                let neighbors = snapshot.in_neighbors(edge_type, destination).await?;
                scanned_edges = scanned_edges.saturating_add(neighbors.len() as u64);
                ensure_limit("in_neighbors_batch_scanned_edges", scanned_edges, self.limits.max_query_scan_edges)?;
                by_destination.entry(destination).or_default().extend(neighbors);
            }
        } else {
            let adjacency = self.canonical_adjacency_at(cell_id, edge_type, read_epoch).await?;
            for (src, destinations) in adjacency {
                budget.check("in_neighbors_batch_snapshot_adjacency")?;
                for destination in destinations {
                    if let Some(neighbors) = by_destination.get_mut(&destination) {
                        scanned_edges = scanned_edges.saturating_add(1);
                        ensure_limit("in_neighbors_batch_scanned_edges", scanned_edges, self.limits.max_query_scan_edges)?;
                        neighbors.insert(src);
                    }
                }
            }
        }
        Ok(destinations
            .into_iter()
            .map(|vertex| NeighborBatchEntry {
                vertex,
                neighbors: by_destination
                    .get(&vertex)
                    .map(|neighbors| neighbors.iter().copied().collect())
                    .unwrap_or_default(),
            })
            .collect())
    }

    pub async fn edge_exists_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
    ) -> Result<Vec<EdgeExistenceBatchEntry>> {
        let read_epoch = self.current_epoch(cell_id).await?;
        self.edge_exists_batch_at(cell_id, edge_type, edges, read_epoch).await
    }

    pub async fn edge_exists_batch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        read_epoch: StorageSequence,
    ) -> Result<Vec<EdgeExistenceBatchEntry>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "edge_exists_batch").await?;
        let edges: Vec<_> = edges.into_iter().collect();
        ensure_limit(
            "edge_exists_batch_edges",
            edges.len() as u64,
            self.limits.max_query_intermediate_rows as u64,
        )?;
        if edges.is_empty() {
            return Ok(Vec::new());
        }
        let budget = QueryBudget::new(self.limits.max_query_runtime_ms, None);
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let mut live = BTreeSet::new();
        for (src, dst) in edges.iter().copied().collect::<BTreeSet<_>>() {
            budget.check("edge_exists_batch_snapshot_point")?;
            if snapshot.edge_exists(edge_type, src, dst).await? {
                live.insert((src, dst));
            }
        }
        Ok(edges
            .into_iter()
            .map(|(src, dst)| EdgeExistenceBatchEntry {
                src,
                dst,
                exists: live.contains(&(src, dst)),
            })
            .collect())
    }

    async fn out_neighbors_at_for_query(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let mut neighbors = snapshot.out_neighbors(edge_type, src).await?;
        for _ in &neighbors {
            budget.check("query_out_neighbors_scan")?;
        }
        neighbors.sort_unstable();
        Ok(neighbors)
    }

    pub(crate) async fn out_segment_edge_record_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<Option<(StorageSequence, EdgeRecord)>> {
        let tombstone_epoch = self
            .out_segment_tombstone_epoch_at(cell_id, edge_type, src, dst, read_epoch)
            .await?;
        let mut latest = None;
        for (sequence, edge) in self.scan_out_segments_for_src_at(cell_id, edge_type, src, read_epoch, None).await? {
            if edge.dst == dst && segment_edge_visible(sequence, tombstone_epoch) {
                latest = Some((sequence, edge));
            }
        }
        Ok(latest)
    }

    async fn scan_out_segments_for_src_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        read_epoch: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<(StorageSequence, EdgeRecord)>> {
        let prefix = keys::out_segment_src_prefix(cell_id, edge_type, src);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut edges = Vec::new();
        while let Some(kv) = iter.next().await? {
            check_optional_query_budget(budget, "query_out_segment_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > read_epoch {
                break;
            }
            for dst in segment.destinations.iter().copied() {
                check_optional_query_budget(budget, "query_out_segment_edge_scan")?;
                edges.push((
                    segment.storage_sequence,
                    EdgeRecord {
                        cell_id: segment.cell_id.clone(),
                        edge_type: segment.edge_type.clone(),
                        src: segment.src,
                        dst,
                    },
                ));
                if budget.is_some() {
                    ensure_limit("query_out_segment_records", edges.len() as u64, self.limits.max_query_scan_edges)?;
                }
            }
        }
        Ok(edges)
    }

    async fn out_segment_tombstone_epoch_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: VertexId,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<Option<StorageSequence>> {
        let key = keys::out_segment_tombstone(cell_id, edge_type, src, dst);
        let Some(value) = self.read_remote(&key).await? else {
            return Ok(None);
        };
        let epoch = decode_u64(&key, &value)?;
        Ok((epoch <= read_epoch).then_some(epoch))
    }

    async fn out_segment_tombstones_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<BTreeMap<(VertexId, VertexId), StorageSequence>> {
        let prefix = keys::out_segment_tombstone_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let mut tombstones = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            check_optional_query_budget(budget, "query_out_segment_tombstone_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (key_cell_id, key_edge_type, src, dst) = parse_out_edge_segment_tombstone_key(&key)?;
            if key_cell_id != cell_id || key_edge_type != edge_type {
                return Err(GraphError::CorruptValue {
                    key,
                    reason: "segment tombstone identity does not match scan prefix".to_string(),
                });
            }
            let epoch = decode_u64(&key, &kv.value)?;
            if epoch <= read_epoch {
                tombstones.insert((src, dst), epoch);
            }
        }
        Ok(tombstones)
    }

    pub(crate) async fn out_segment_edge_pairs_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
    ) -> Result<BTreeSet<(VertexId, VertexId)>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let prefix = keys::out_segment_edge_type_prefix(cell_id, edge_type);
        let mut iter = self.scan_remote_prefix(&prefix).await?;
        let tombstones = self.out_segment_tombstones_at(cell_id, edge_type, read_epoch, None).await?;
        let mut pairs = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let segment = decode_out_edge_segment(&key, &kv.value)?;
            if segment.storage_sequence > read_epoch {
                continue;
            }
            for dst in segment.destinations.iter().copied() {
                let tombstone_epoch = tombstones.get(&(segment.src, dst)).copied();
                if segment_edge_visible(segment.storage_sequence, tombstone_epoch) {
                    pairs.insert((segment.src, dst));
                }
            }
        }
        Ok(pairs)
    }

    pub async fn in_neighbors(&self, cell_id: &str, edge_type: &str, dst: VertexId) -> Result<Vec<VertexId>> {
        let snapshot = self.snapshot(cell_id).await?;
        if self.writes_reverse_index() {
            return snapshot.in_neighbors(edge_type, dst).await;
        }
        let read_epoch = snapshot.read_epoch();
        SlateGraphStore::scope_snapshot(Arc::clone(&snapshot.storage_snapshot), async move {
            let mut neighbors: Vec<_> = self
                .edges_at(cell_id, edge_type, read_epoch)
                .await?
                .into_iter()
                .filter_map(|edge| (edge.dst == dst).then_some(edge.src))
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
            Ok(neighbors)
        })
        .await
    }

    pub async fn in_neighbors_at(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
        read_epoch: StorageSequence,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        if self.writes_reverse_index() {
            return snapshot.in_neighbors(edge_type, dst).await;
        }
        SlateGraphStore::scope_snapshot(Arc::clone(&snapshot.storage_snapshot), async move {
            let mut neighbors: Vec<_> = self
                .edges_at(cell_id, edge_type, read_epoch)
                .await?
                .into_iter()
                .filter_map(|edge| (edge.dst == dst).then_some(edge.src))
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
            Ok(neighbors)
        })
        .await
    }

    #[cfg(feature = "opencypher")]
    pub(super) async fn in_neighbors_at_for_query(
        &self,
        cell_id: &str,
        edge_type: &str,
        dst: VertexId,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<VertexId>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if !self.writes_reverse_index() {
            let mut neighbors = Vec::new();
            for edge in self.edges_at_with_budget(cell_id, edge_type, read_epoch, Some(budget)).await? {
                budget.check("query_in_neighbors_full_scan")?;
                if edge.dst == dst {
                    neighbors.push(edge.src);
                }
            }
            neighbors.sort_unstable();
            neighbors.dedup();
            return Ok(neighbors);
        }

        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        let scan_options = remote_scan_options_for_expected_items(
            self.degree_for_cache_admission(&snapshot.storage_snapshot, &keys::degree_in(cell_id, edge_type, dst))
                .await?,
        );
        let prefix = keys::in_prefix(cell_id, edge_type, dst);
        let mut iter = snapshot
            .storage_snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &scan_options)
            .await?;
        let mut neighbors = Vec::new();
        while let Some(kv) = iter.next().await? {
            budget.check("query_in_neighbors_reverse_scan")?;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_edge_record(&key, &kv.value)?;
            neighbors.push(record.src);
        }
        neighbors.sort_unstable();
        neighbors.dedup();
        Ok(neighbors)
    }

    pub async fn out_degree(&self, cell_id: &str, edge_type: &str, src: VertexId) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "out_degree").await?;
        self.snapshot(cell_id).await?.out_degree(edge_type, src).await
    }

    pub async fn current_epoch(&self, cell_id: &str) -> Result<StorageSequence> {
        validate_component("cell_id", cell_id)?;
        self.ensure_cell_readable(cell_id, "current_epoch").await?;
        Ok(self.storage.snapshot().await?.seq())
    }

    pub(crate) async fn ensure_cell_readable(&self, cell_id: &str, operation: &'static str) -> Result<()> {
        validate_component("cell_id", cell_id)?;
        // Both marker reads use the pinned query snapshot. Rechecking that
        // same immutable view adds I/O but cannot discover a later drop.
        if SlateGraphStore::snapshot_cell_is_readable(cell_id) {
            return Ok(());
        }
        if self.read_remote(&keys::cell_drop_marker(cell_id)).await?.is_some()
            || self.read_remote(&keys::cell_drop_pending_marker(cell_id)).await?.is_some()
        {
            return Err(GraphError::CellDropped {
                operation,
                cell_id: cell_id.to_string(),
            });
        }
        SlateGraphStore::remember_snapshot_readable_cell(cell_id);
        Ok(())
    }

    pub async fn edges_at(&self, cell_id: &str, edge_type: &str, read_epoch: StorageSequence) -> Result<Vec<EdgeRecord>> {
        self.edges_at_with_budget(cell_id, edge_type, read_epoch, None).await
    }

    async fn edges_at_with_budget(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<EdgeRecord>> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        self.ensure_cell_readable(cell_id, "edges_at").await?;
        check_optional_query_budget(budget, "query_edges_at")?;
        let snapshot = self.snapshot_at(cell_id, read_epoch).await?;
        self.edges_in_snapshot_at_topology(&snapshot, edge_type, read_epoch, budget).await
    }

    pub(crate) async fn edges_in_current_snapshot_at_topology(
        &self,
        snapshot: &GraphSnapshot<'_>,
        edge_type: &str,
        topology_sequence: StorageSequence,
    ) -> Result<Vec<EdgeRecord>> {
        validate_component("edge_type", edge_type)?;
        self.edges_in_snapshot_at_topology(snapshot, edge_type, topology_sequence, None)
            .await
    }

    async fn edges_in_snapshot_at_topology(
        &self,
        snapshot: &GraphSnapshot<'_>,
        edge_type: &str,
        topology_sequence: StorageSequence,
        budget: Option<&QueryBudget>,
    ) -> Result<Vec<EdgeRecord>> {
        let cell_id = snapshot.cell_id();
        SlateGraphStore::scope_snapshot(Arc::clone(&snapshot.storage_snapshot), async move {
            let adjacency = self.canonical_adjacency_at(cell_id, edge_type, topology_sequence).await?;
            let mut edges = Vec::new();
            for (src, destinations) in adjacency {
                check_optional_query_budget(budget, "query_edges_at_adjacency")?;
                for dst in destinations {
                    check_optional_query_budget(budget, "query_edges_at_adjacency_edge")?;
                    edges.push(EdgeRecord {
                        cell_id: cell_id.to_string(),
                        edge_type: edge_type.to_string(),
                        src,
                        dst,
                    });
                    if budget.is_some() {
                        ensure_limit("query_edges_at_canonical", edges.len() as u64, self.limits.max_query_scan_edges)?;
                    }
                }
            }
            Ok(edges)
        })
        .await
    }

    pub async fn validate_cell_edge_type(&self, cell_id: &str, edge_type: &str) -> Result<GraphRepairReport> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let read_epoch = self.current_epoch(cell_id).await?;
        let edges = self.edges_at(cell_id, edge_type, read_epoch).await?;
        let mut out_counts = BTreeMap::<VertexId, u64>::new();
        let mut in_counts = BTreeMap::<VertexId, u64>::new();
        for edge in &edges {
            *out_counts.entry(edge.src).or_default() += 1;
            *in_counts.entry(edge.dst).or_default() += 1;
        }
        let mut degree_mismatches = Vec::new();
        for (src, expected) in out_counts {
            let actual = self.out_degree(cell_id, edge_type, src).await?;
            if actual != expected {
                degree_mismatches.push(format!("out:{src}:expected={expected}:actual={actual}"));
            }
        }
        if self.writes_reverse_index() {
            for (dst, expected) in in_counts {
                let actual = self.read_counter(&keys::degree_in(cell_id, edge_type, dst)).await?;
                if actual != expected {
                    degree_mismatches.push(format!("in:{dst}:expected={expected}:actual={actual}"));
                }
            }
        } else {
            let mut iter = self.scan_remote_prefix(&keys::degree_in_prefix(cell_id, edge_type)).await?;
            while let Some(kv) = iter.next().await? {
                let key = String::from_utf8_lossy(&kv.key);
                degree_mismatches.push(format!("in:{key}:unexpected-under-outbound-only"));
            }
        }
        Ok(GraphRepairReport {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            read_epoch,
            live_edges: edges.len() as u64,
            degree_mismatches,
        })
    }
}

/// Take one unit from `allowance`, or report that none is left. Never wraps.
#[cfg(feature = "opencypher")]
fn take_one(allowance: &std::sync::atomic::AtomicUsize) -> bool {
    // A compare-exchange loop rather than `fetch_update`, for the same
    // minimum-Rust reason as `QueryBudget::account_result_row`.
    let mut current = allowance.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let Some(next) = current.checked_sub(1) else {
            return false;
        };
        match allowance.compare_exchange_weak(
            current,
            next,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

/// Hold back a read's error unless it is a timeout or cancellation, which fails
/// the query whatever the other reads it is joined with return.
#[cfg(feature = "opencypher")]
async fn defer_unless_query_timeout<T>(read: impl std::future::Future<Output = Result<T>>) -> Result<Result<T>> {
    match read.await {
        Err(error @ GraphError::QueryTimeout { .. }) => Err(error),
        result => Ok(result),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueryBudget {
    started_at: std::time::Instant,
    max_runtime_ms: Option<u64>,
    cancellation_token: Option<QueryCancellationToken>,
    #[cfg(feature = "opencypher")]
    max_result_bytes: Option<u64>,
    #[cfg(feature = "opencypher")]
    result_bytes: Arc<AtomicU64>,
    /// Edges visited by every edge match in this query, shared across clones.
    /// An undirected hop runs `match_edge_row_pattern_oriented` twice, so a
    /// tally local to one oriented call would let a query spend
    /// `max_query_scan_edges` once per orientation.
    #[cfg(feature = "opencypher")]
    scanned_edges: Arc<AtomicU64>,
    /// Index entries read by every ordered walk in this query, shared across
    /// clones, for the same reason as `scanned_edges`. An ordered walk is paged
    /// and each page is a separate call, so a tally local to one page would
    /// start at zero on every page: `max_query_index_candidates` would bound one
    /// page and nothing at all about a walk, which is to say the query could
    /// read the whole index and never reach the limit.
    #[cfg(feature = "experimental-cypher-engine")]
    scanned_index_candidates: Arc<AtomicU64>,
    /// The write-back pending markers this query has read, shared across
    /// clones, so the query reads each property's once however many
    /// vertices or seeds touch it.
    #[cfg(feature = "opencypher")]
    write_back_pending: Arc<WriteBackPendingReads>,
}

/// The pending markers ([`keys::vertex_write_back_pending`]) one query has
/// read, by cell and property. Held per query rather than on the shard, as
/// [`WriteBackGenerations`] holds the catalog per transaction, so a marker
/// put or cleared between two queries is seen by the second without an
/// invalidation step. Each slot is initialised once: concurrent reads of one
/// property in a query wait for the first one's get rather than each make
/// their own. A cell's slot holds whether it has any marker at all, one seek
/// of the marker prefix, which a property's get waits behind: a query on a
/// cell with no write-back in flight reads that one seek and nothing per
/// property.
#[cfg(feature = "opencypher")]
#[derive(Debug, Default)]
pub(crate) struct WriteBackPendingReads {
    cells: std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::OnceCell<bool>>>>,
    slots: std::sync::Mutex<std::collections::HashMap<(String, String), PendingMarkerSlot>>,
}

#[cfg(feature = "opencypher")]
type PendingMarkerSlot = Arc<tokio::sync::OnceCell<Option<StorageSequence>>>;

#[cfg(feature = "opencypher")]
impl WriteBackPendingReads {
    fn cell(&self, cell_id: &str) -> Arc<tokio::sync::OnceCell<bool>> {
        let mut cells = self.cells.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(cells.entry(cell_id.to_string()).or_default())
    }

    fn slot(&self, cell_id: &str, property: &str) -> PendingMarkerSlot {
        let mut slots = self.slots.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(slots.entry((cell_id.to_string(), property.to_string())).or_default())
    }
}

impl QueryBudget {
    /// Add to the query's edge-visit tally and return the new total. Shared
    /// across clones, so both orientations of an undirected hop draw on one
    /// allowance.
    #[cfg(feature = "opencypher")]
    pub(crate) fn add_scanned_edges(&self, edges: u64) -> u64 {
        self.scanned_edges.fetch_add(edges, Ordering::Relaxed).saturating_add(edges)
    }

    /// Add to the query's index-entry tally and return the new total. Shared
    /// across clones, so every page of an ordered walk draws on one allowance.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) fn add_scanned_index_candidates(&self, entries: u64) -> u64 {
        self.scanned_index_candidates
            .fetch_add(entries, Ordering::Relaxed)
            .saturating_add(entries)
    }

    /// Only for cancellation-safe read I/O, never commits or native compute
    /// whose resource ownership must outlive cancellation of the caller.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn read_only_io<T>(&self, operation: &'static str, read: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        self.check(operation)?;
        // A cache hit can be immediately ready for every iteration of a large
        // scan. `await` alone then never yields the worker to other requests.
        tokio::task::consume_budget().await;
        self.until_deadline(operation, read).await?
    }

    /// Wait for `wait`, or fail at the query's deadline or on its
    /// cancellation, whichever comes first. Only for waits that are safe to
    /// abandon: a read, or an acquisition (a permit, a lock) that holds
    /// nothing until it completes. A commit or native compute must never be
    /// raced against the deadline, since dropping it does not undo it.
    #[cfg(feature = "opencypher")]
    pub(crate) async fn until_deadline<T>(&self, operation: &'static str, wait: impl std::future::Future<Output = T>) -> Result<T> {
        self.check(operation)?;
        let cancelled = async {
            match &self.cancellation_token {
                Some(token) => token.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let deadline = async {
            match self.max_runtime_ms {
                Some(limit) => tokio::time::sleep(std::time::Duration::from_millis(limit).saturating_sub(self.started_at.elapsed())).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            _ = cancelled => Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                elapsed_ms: self.started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                limit_ms: 0,
            }),
            _ = deadline => Err(GraphError::QueryTimeout {
                operation,
                elapsed_ms: self.started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                limit_ms: self.max_runtime_ms.unwrap_or(0),
            }),
            result = wait => Ok(result),
        }
    }

    pub(crate) fn new(max_runtime_ms: Option<u64>, cancellation_token: Option<QueryCancellationToken>) -> Self {
        Self {
            started_at: std::time::Instant::now(),
            max_runtime_ms,
            cancellation_token,
            #[cfg(feature = "opencypher")]
            max_result_bytes: None,
            #[cfg(feature = "opencypher")]
            result_bytes: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "opencypher")]
            scanned_edges: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "experimental-cypher-engine")]
            scanned_index_candidates: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "opencypher")]
            write_back_pending: Arc::default(),
        }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn with_max_result_bytes(mut self, max_result_bytes: Option<u64>) -> Self {
        self.max_result_bytes = max_result_bytes;
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn account_result_row(&self, row: &QueryRow) -> Result<()> {
        let Some(limit) = self.max_result_bytes else {
            return Ok(());
        };
        let bytes = row.estimated_resident_bytes();
        // The closure always returns `Some`, so the update cannot fail and
        // both arms carry the value before this row was added.
        let previous = self
            .result_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_add(bytes)))
            .unwrap_or_else(|previous| previous);
        let actual = previous.saturating_add(bytes);
        if actual > limit {
            return Err(GraphError::AdmissionRejected {
                operation: "client_cursor_buffer_bytes",
                actual,
                limit,
            });
        }
        Ok(())
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn account_result_rows(&self, rows: &[QueryRow]) -> Result<()> {
        for row in rows {
            self.account_result_row(row)?;
        }
        Ok(())
    }

    pub(crate) fn check(&self, operation: &'static str) -> Result<()> {
        if let Some(token) = &self.cancellation_token {
            if !token.is_cancelled() {
                return self.check_runtime_limit(operation);
            }
            return Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                elapsed_ms: self.started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                limit_ms: 0,
            });
        }
        self.check_runtime_limit(operation)
    }

    fn check_runtime_limit(&self, operation: &'static str) -> Result<()> {
        let Some(limit_ms) = self.max_runtime_ms else {
            return Ok(());
        };
        let elapsed_ms = self.started_at.elapsed().as_millis();
        if elapsed_ms >= u128::from(limit_ms) {
            return Err(GraphError::QueryTimeout {
                operation,
                elapsed_ms: elapsed_ms.min(u128::from(u64::MAX)) as u64,
                limit_ms,
            });
        }
        Ok(())
    }
}

fn check_optional_query_budget(budget: Option<&QueryBudget>, operation: &'static str) -> Result<()> {
    if let Some(budget) = budget {
        budget.check(operation)?;
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
struct EdgeRowMatchState<'a> {
    cell_id: &'a str,
    read_epoch: StorageSequence,
    /// The pattern every row in this traversal is matched against. Held here
    /// rather than passed to the flush separately so the pattern a row was
    /// built from and the one it is filtered by cannot diverge.
    edge: &'a RowEdgePattern,
    /// Matched rows, hydrated and filtered.
    ///
    /// Owned rather than borrowed so a traversal's results arrive through
    /// [`GraphShard::finish_edge_rows`], which drains [`Self::pending`] first.
    /// With a borrowed `&mut Vec` the natural way to end a traversal was to
    /// return the caller's own vector, which returned any un-hydrated tail as
    /// though it had never matched -- and removing a flush passed the whole
    /// suite when I tried it.
    ///
    /// This is a convention, not an invariant the compiler holds: field privacy
    /// is module-scoped and this module is one file, so `Ok(state.rows)` still
    /// compiles. What it buys is that the correct spelling is also the obvious
    /// one, and that a bypass has to be written deliberately.
    rows: Vec<BindingRow>,
    /// Rows built but not yet hydrated, drained by
    /// [`GraphShard::flush_pending_edge_rows`].
    pending: Vec<BindingRow>,
    metadata_cache: &'a mut BTreeMap<VertexId, VertexMetadata>,
    edge_metadata_cache: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    budget: &'a QueryBudget,
    /// Set when only some rows are wanted: [`Self::rows`] then holds rows
    /// joined to the bound's input and filtered by its predicate, and the
    /// traversal stops once [`Self::is_full`].
    bound: Option<EdgeRowBound<'a>>,
    /// Drop self-loops, for the reverse orientation of an undirected hop,
    /// whose forward orientation already matched them.
    skip_self_loops: bool,
}

#[cfg(feature = "opencypher")]
impl<'a> EdgeRowMatchState<'a> {
    fn new(
        cell_id: &'a str,
        read_epoch: StorageSequence,
        edge: &'a RowEdgePattern,
        metadata_cache: &'a mut BTreeMap<VertexId, VertexMetadata>,
        edge_metadata_cache: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
        budget: &'a QueryBudget,
    ) -> Self {
        Self {
            cell_id,
            read_epoch,
            edge,
            rows: Vec::new(),
            pending: Vec::new(),
            metadata_cache,
            edge_metadata_cache,
            budget,
            bound: None,
            skip_self_loops: false,
        }
    }

    /// Whether a bounded traversal has the rows it wants. Counted on flushed
    /// rows, so the traversal may read up to one [`EDGE_ROW_HYDRATION_BATCH`]
    /// past its bound, but never keeps more than [`EdgeRowBound::wanted`].
    fn is_full(&self) -> bool {
        self.bound.is_some_and(|bound| self.rows.len() >= bound.wanted)
    }
}

/// The rows a bounded group still needs from one hop: the row being extended,
/// the group's predicate, and how many joined rows that pass it are wanted.
/// Applying both inside the traversal is what lets it stop early: a hop whose
/// vertex has more relationships than the intermediate-row limit still yields
/// its first `wanted` rows instead of being refused.
#[cfg(feature = "opencypher")]
#[derive(Clone, Copy)]
struct EdgeRowBound<'a> {
    input: &'a BindingRow,
    predicate: Option<&'a RowPredicate>,
    wanted: usize,
}

/// How many rows accumulate before a traversal hydrates them.
///
/// Batching trades a bounded amount of held memory for round trips saved, and
/// this is the bound: a traversal never holds more than this many un-hydrated
/// rows however large its candidate set. Comfortably above
/// [`QUERY_METADATA_HYDRATION_CONCURRENCY`] so a flush has enough work to keep
/// the fan-out saturated, and far below the intermediate-row admission limit
/// that `push_binding_row` enforces on the rows actually returned.
#[cfg(feature = "opencypher")]
const EDGE_ROW_HYDRATION_BATCH: usize = 1_024;

/// A source expansion with an edge property equality reads that property's
/// index once, instead of checking each neighbor, past this many neighbors.
#[cfg(feature = "opencypher")]
const ANCHORED_EXPAND_PROPERTY_PROBE_MIN_DEGREE: usize = 64;

/// Whether a source with `degree` neighbors probes the property index, after
/// a probe overflowed at `overflowed_at` entries (0: none has). Only a source
/// with at least twice that degree retries, so on sources of slowly rising
/// degree the failed probes read at most about twice the largest degree in
/// all, while a much larger hub still gets its probe.
#[cfg(feature = "opencypher")]
fn anchored_property_probe_due(degree: usize, overflowed_at: usize) -> bool {
    overflowed_at == 0 || degree >= overflowed_at.saturating_mul(2)
}

#[cfg(feature = "opencypher")]
struct VertexMutationApplyState<'a> {
    cell_id: &'a str,
    read_epoch: StorageSequence,
    pending_metadata: &'a mut BTreeMap<VertexId, VertexMetadata>,
    original_metadata: &'a mut BTreeMap<VertexId, VertexMetadata>,
    pending_edge_metadata: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    original_edge_metadata: &'a mut BTreeMap<BoundRelationship, EdgeMetadata>,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_page_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty()
        || query.predicate.is_some()
        || row_projections_have_aggregates(&query.projections)
        || query.pattern_groups.len() != 1
    {
        return None;
    }
    let group = query.pattern_groups.first()?;
    if group.optional || group.predicate.is_some() || group.patterns.len() != 1 {
        return None;
    }
    let RowPattern::Edge(edge) = group.patterns.first()? else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || !edge.properties.is_empty()
        || edge.src.id.is_none()
        || edge.dst.id.is_some()
        || !edge.src.labels.is_empty()
        || !row_node_has_only_id_property(&edge.src)
        || !edge.dst.labels.is_empty()
        || !edge.dst.properties.is_empty()
    {
        return None;
    }
    if !query
        .projections
        .iter()
        .all(|projection| streaming_neighbor_projection_supported(edge, projection))
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn row_node_has_only_id_property(node: &RowNodePattern) -> bool {
    node.properties.keys().all(|property| property == "id")
}

#[cfg(feature = "opencypher")]
struct GraphKernelRowQueryRequest<'a> {
    edge: &'a RowEdgePattern,
    src: VertexId,
    hop_range: (u8, u8),
    projection: GraphKernelProjection,
    ascending: bool,
}

#[cfg(feature = "opencypher")]
struct ReachableWindowRequest<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    hop_range: (u8, u8),
    read_epoch: StorageSequence,
    window: QueryWindow,
    ascending: bool,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphKernelProjection {
    NodeId,
    CountAll,
}

#[cfg(feature = "opencypher")]
fn graph_kernel_node_id_rows(vertices: Vec<VertexId>, budget: &QueryBudget) -> Result<Vec<QueryRow>> {
    let mut rows = Vec::with_capacity(vertices.len());
    for vertex in vertices {
        budget.check("cypher_graph_kernel_project")?;
        let row = QueryRow::new(vec![QueryValue::VertexId(vertex)]);
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_order_vertices(vertices: &mut Vec<VertexId>, ascending: bool) {
    vertices.sort_unstable();
    vertices.dedup();
    if !ascending {
        vertices.reverse();
    }
}

#[cfg(feature = "opencypher")]
fn graph_kernel_window_sorted_vertices(vertices: &[VertexId], window: QueryWindow, ascending: bool) -> Result<Vec<VertexId>> {
    let skip = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
        operation: "query_result_skip",
        actual: window.skip,
        limit: usize::MAX as u64,
    })?;
    let limit = window.limit.unwrap_or(usize::MAX);
    if ascending {
        Ok(vertices.iter().copied().skip(skip).take(limit).collect())
    } else {
        Ok(vertices.iter().rev().copied().skip(skip).take(limit).collect())
    }
}

#[cfg(feature = "opencypher")]
fn validate_query_result_window(window: QueryWindow, max_rows: usize) -> Result<()> {
    let _ = usize::try_from(window.skip).map_err(|_| GraphError::AdmissionRejected {
        operation: "query_result_skip",
        actual: window.skip,
        limit: usize::MAX as u64,
    })?;
    if let Some(limit) = window.limit {
        ensure_limit("query_result_limit", limit as u64, max_rows as u64)?;
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
fn graph_kernel_row_query_request(query: &ParsedRowQuery) -> Option<GraphKernelRowQueryRequest<'_>> {
    if !query.union_arms.is_empty() || query.predicate.is_some() {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    let src = edge.src.id?;
    let hop_range = edge.hop_range?;
    if edge.direction == EdgeDirection::Bidirectional
        || edge.binding.is_some()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    let projection = graph_kernel_projection(edge, &query.projections)?;
    let ascending = graph_kernel_order_supported(edge, projection, &query.projections, &query.columns, &query.order_by)?;
    Some(GraphKernelRowQueryRequest {
        edge,
        src,
        hop_range,
        projection,
        ascending,
    })
}

#[cfg(feature = "opencypher")]
fn relationship_count_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty() || query.predicate.is_some() || !query.order_by.is_empty() {
        return None;
    }
    if query.projections.as_slice() != [RowProjection::CountAll] {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_none()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_rows_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty() || query.predicate.is_some() || row_projections_have_aggregates(&query.projections) {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_some()
        || edge.binding.is_some()
        || !edge.properties.is_empty()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
        || !source_relationship_id_projection_supported(edge, &query.projections)
        || !source_relationship_id_order_supported(edge, &query.projections, &query.columns, &query.order_by)
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_projection_supported(edge: &RowEdgePattern, projections: &[RowProjection]) -> bool {
    !projections.is_empty()
        && projections.iter().all(|projection| match projection {
            RowProjection::NodeId { binding } => {
                edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
            }
            RowProjection::Property { .. } | RowProjection::CountAll | RowProjection::Literal(_) | RowProjection::Aggregate { .. } => false,
        })
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    order_by.iter().all(|sort| match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Column { name } => columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
            .is_some_and(|projection| match projection {
                RowProjection::NodeId { binding } => {
                    edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
                }
                RowProjection::Property { .. } | RowProjection::CountAll | RowProjection::Literal(_) | RowProjection::Aggregate { .. } => {
                    false
                }
            }),
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    })
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_page_order(edge: &RowEdgePattern, query: &ParsedRowQuery) -> Option<bool> {
    let [sort] = query.order_by.as_slice() else {
        return None;
    };
    let orders_destination = match &sort.expression {
        RowSortExpression::NodeId { binding } => edge.dst.binding.as_deref() == Some(binding.as_str()),
        RowSortExpression::Column { name } => query
            .columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|index| query.projections.get(index))
            .is_some_and(|projection| {
                matches!(
                    projection,
                    RowProjection::NodeId { binding }
                        if edge.dst.binding.as_deref() == Some(binding.as_str())
                )
            }),
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    };
    orders_destination.then_some(sort.ascending)
}

#[cfg(feature = "opencypher")]
fn source_relationship_id_bindings_from_dsts(
    edge: &RowEdgePattern,
    src: VertexId,
    dsts: &[VertexId],
    shard: &GraphShard,
    budget: &QueryBudget,
) -> Result<Vec<BindingRow>> {
    let mut rows = Vec::with_capacity(dsts.len());
    for dst in dsts {
        budget.check("cypher_source_relationship_id_cached_rows")?;
        if let Some(row) = BindingRow::from_edge(edge, src, *dst) {
            rows.push(row);
            shard.ensure_query_intermediate_rows("cypher_source_relationship_id_cached_rows", rows.len())?;
        }
    }
    Ok(rows)
}

#[cfg(feature = "opencypher")]
fn relationship_rows_query_edge(query: &ParsedRowQuery) -> Option<&RowEdgePattern> {
    if !query.union_arms.is_empty() || query.predicate.is_some() || row_projections_have_aggregates(&query.projections) {
        return None;
    }
    let patterns = graph_kernel_query_patterns(query)?;
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional
        || edge.hop_range.is_some()
        || edge.src.id.is_none()
        || edge.dst.id.is_none()
        || edge.binding.is_none()
        || !graph_kernel_node_pattern(&edge.src)
        || !graph_kernel_node_pattern(&edge.dst)
    {
        return None;
    }
    Some(edge)
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_index_spec(
    query: &ParsedRowQuery,
    window: QueryWindow,
) -> Option<OrderedRelationshipPropertyIndexSpec<'_>> {
    if !query.union_arms.is_empty() || query.distinct || row_projections_have_aggregates(&query.projections) {
        return None;
    }
    let (patterns, predicate) = if query.pattern_groups.is_empty() {
        (query.patterns.as_slice(), query.predicate.as_ref()?)
    } else {
        let [group] = query.pattern_groups.as_slice() else {
            return None;
        };
        if group.optional {
            return None;
        }
        let predicate = match (query.predicate.as_ref(), group.predicate.as_ref()) {
            (Some(query_predicate), Some(group_predicate)) if query_predicate == group_predicate => query_predicate,
            (Some(predicate), None) | (None, Some(predicate)) => predicate,
            (Some(_), Some(_)) | (None, None) => return None,
        };
        (group.patterns.as_slice(), predicate)
    };
    let [pattern] = patterns else {
        return None;
    };
    let RowPattern::Edge(edge) = pattern else {
        return None;
    };
    if edge.direction == EdgeDirection::Bidirectional || edge.hop_range.is_some() || !edge.properties.is_empty() || edge.binding.is_none() {
        return None;
    }
    let sort = query.order_by.first()?;
    let property = match &sort.expression {
        RowSortExpression::Property { binding, property } if edge.binding.as_deref() == Some(binding.as_str()) && property != "id" => {
            property.as_str()
        }
        RowSortExpression::Column { name } => {
            let index = query.columns.iter().position(|column| column.name == *name)?;
            match query.projections.get(index)? {
                RowProjection::Property { binding, property } if edge.binding.as_deref() == Some(binding.as_str()) && property != "id" => {
                    property.as_str()
                }
                RowProjection::NodeId { .. }
                | RowProjection::Property { .. }
                | RowProjection::CountAll
                | RowProjection::Literal(_)
                | RowProjection::Aggregate { .. } => return None,
            }
        }
        RowSortExpression::NodeId { .. } | RowSortExpression::Property { .. } | RowSortExpression::CountAll => return None,
    };
    if !row_predicate_requires_scalar_indexed_property(predicate, edge.binding.as_deref()?, property) {
        return None;
    }
    Some(OrderedRelationshipPropertyIndexSpec {
        edge,
        property,
        predicate,
        ascending: sort.ascending,
        limit: window.limit?,
    })
}

#[cfg(feature = "opencypher")]
fn relationship_rows_projection_supported(edge: &RowEdgePattern, projections: &[RowProjection]) -> bool {
    projections.iter().all(|projection| match projection {
        RowProjection::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowProjection::Property { binding, property } => edge.binding.as_deref() == Some(binding.as_str()) && property != "id",
        RowProjection::CountAll | RowProjection::Literal(_) | RowProjection::Aggregate { .. } => false,
    })
}

#[cfg(feature = "opencypher")]
fn relationship_rows_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    order_by.iter().all(|sort| match &sort.expression {
        RowSortExpression::NodeId { binding } => {
            edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
        }
        RowSortExpression::Property { binding, property } => edge.binding.as_deref() == Some(binding.as_str()) && property != "id",
        RowSortExpression::Column { name } => columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
            .is_some_and(|projection| match projection {
                RowProjection::NodeId { binding } => {
                    edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
                }
                RowProjection::Property { binding, property } => edge.binding.as_deref() == Some(binding.as_str()) && property != "id",
                RowProjection::CountAll | RowProjection::Literal(_) | RowProjection::Aggregate { .. } => false,
            }),
        RowSortExpression::CountAll => false,
    })
}

#[cfg(feature = "opencypher")]
fn graph_kernel_query_patterns(query: &ParsedRowQuery) -> Option<&[RowPattern]> {
    if query.pattern_groups.is_empty() {
        return Some(&query.patterns);
    }
    if query.pattern_groups.len() != 1 {
        return None;
    }
    let group = query.pattern_groups.first()?;
    if group.optional || group.predicate.is_some() {
        return None;
    }
    Some(&group.patterns)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_node_pattern(node: &RowNodePattern) -> bool {
    node.labels.is_empty() && row_node_has_only_id_property(node)
}

#[cfg(feature = "opencypher")]
fn graph_kernel_projection(edge: &RowEdgePattern, projections: &[RowProjection]) -> Option<GraphKernelProjection> {
    let [projection] = projections else {
        return None;
    };
    match projection {
        RowProjection::NodeId { binding } => {
            if edge.dst.binding.as_deref() == Some(binding.as_str()) {
                Some(GraphKernelProjection::NodeId)
            } else {
                None
            }
        }
        RowProjection::CountAll => Some(GraphKernelProjection::CountAll),
        RowProjection::Property { .. } | RowProjection::Literal(_) | RowProjection::Aggregate { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
fn graph_kernel_order_supported(
    edge: &RowEdgePattern,
    projection: GraphKernelProjection,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> Option<bool> {
    if order_by.is_empty() {
        return Some(true);
    }
    let [sort] = order_by else {
        return None;
    };
    match projection {
        GraphKernelProjection::NodeId => match &sort.expression {
            RowSortExpression::NodeId { binding } => {
                if edge.dst.binding.as_deref() == Some(binding.as_str()) {
                    Some(sort.ascending)
                } else {
                    None
                }
            }
            RowSortExpression::Column { name } => {
                let projection_idx = columns.iter().position(|column| column.name == *name)?;
                match projections.get(projection_idx) {
                    Some(RowProjection::NodeId { binding }) if edge.dst.binding.as_deref() == Some(binding.as_str()) => {
                        Some(sort.ascending)
                    }
                    _ => None,
                }
            }
            RowSortExpression::Property { .. } | RowSortExpression::CountAll => None,
        },
        GraphKernelProjection::CountAll => match &sort.expression {
            RowSortExpression::CountAll => Some(true),
            RowSortExpression::Column { name } => {
                if matches!(columns.first(), Some(column) if column.name == *name) {
                    Some(true)
                } else {
                    None
                }
            }
            RowSortExpression::NodeId { .. } | RowSortExpression::Property { .. } => None,
        },
    }
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_projection_supported(edge: &RowEdgePattern, projection: &RowProjection) -> bool {
    let RowProjection::NodeId { binding } = projection else {
        return false;
    };
    edge.src.binding.as_deref() == Some(binding.as_str()) || edge.dst.binding.as_deref() == Some(binding.as_str())
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_order_supported(
    edge: &RowEdgePattern,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
) -> bool {
    if order_by.is_empty() {
        return true;
    }
    let [sort] = order_by else {
        return false;
    };
    if !sort.ascending {
        return false;
    }
    match &sort.expression {
        RowSortExpression::NodeId { binding } => edge.dst.binding.as_deref() == Some(binding.as_str()),
        RowSortExpression::Column { name } => match columns
            .iter()
            .position(|column| column.name == *name)
            .and_then(|idx| projections.get(idx))
        {
            Some(projection) => {
                matches!(
                    projection,
                    RowProjection::NodeId { binding }
                        if edge.dst.binding.as_deref() == Some(binding.as_str())
                )
            }
            None => false,
        },
        RowSortExpression::Property { .. } | RowSortExpression::CountAll => false,
    }
}

#[cfg(feature = "opencypher")]
fn streaming_neighbor_projection_values(
    edge: &RowEdgePattern,
    src: VertexId,
    dst: VertexId,
    projections: &[RowProjection],
) -> Result<Vec<QueryValue>> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        let RowProjection::NodeId { binding } = projection else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Return,
                dialect: "OpenCypher",
                feature: "streaming neighbor page supports only node-id projections".to_string(),
            });
        };
        if edge.src.binding.as_deref() == Some(binding.as_str()) {
            values.push(QueryValue::VertexId(src));
        } else if edge.dst.binding.as_deref() == Some(binding.as_str()) {
            values.push(QueryValue::VertexId(dst));
        } else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("streaming neighbor page cannot project unbound {binding}"),
            });
        }
    }
    Ok(values)
}

#[cfg(feature = "opencypher")]
fn query_result_set_to_output(parsed: &ParsedRowQuery, result_set: QueryResultSet) -> QueryOutput {
    match parsed.projections.as_slice() {
        [RowProjection::NodeId { .. }] => {
            let vertices: Option<Vec<VertexId>> = result_set
                .rows
                .iter()
                .map(|row| match row.values.as_slice() {
                    [QueryValue::VertexId(vertex)] => Some(*vertex),
                    _ => None,
                })
                .collect();
            vertices.map_or(QueryOutput::Rows(result_set), QueryOutput::Vertices)
        }
        [RowProjection::CountAll]
        | [RowProjection::Aggregate {
            function: RowAggregateFunction::Count,
            ..
        }] => match result_set.rows.as_slice() {
            [row] => match row.values.as_slice() {
                [QueryValue::Count(count)] => QueryOutput::Count(*count),
                _ => QueryOutput::Rows(result_set),
            },
            _ => QueryOutput::Rows(result_set),
        },
        _ => QueryOutput::Rows(result_set),
    }
}

#[cfg(feature = "opencypher")]
fn merge_opencypher_window(context: QueryContext, window: QueryWindow) -> Result<QueryContext> {
    if window.is_default() {
        return Ok(context);
    }
    if !context.result_window.is_default() && context.result_window != window {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::InvalidRequest,
            dialect: "OpenCypher",
            feature: "query SKIP/LIMIT conflicts with QueryContext result window".to_string(),
        });
    }
    Ok(context.with_result_window(window.skip, window.limit))
}

#[cfg(feature = "opencypher")]
fn opencypher_outer_window(query: &ParsedRowQuery) -> QueryWindow {
    if query.union_arms.is_empty() {
        query.window
    } else {
        QueryWindow::default()
    }
}

#[cfg(feature = "opencypher")]
fn query_next_cursor(cursor_offset: u64, page_size: usize, has_next: bool) -> Result<Option<QueryCursorToken>> {
    if !has_next {
        return Ok(None);
    }
    Ok(Some(QueryCursorToken::new(
        cursor_offset
            .checked_add(u64::try_from(page_size).unwrap_or(u64::MAX))
            .ok_or(GraphError::AdmissionRejected {
                operation: "query_cursor_offset",
                actual: u64::MAX,
                limit: u64::MAX - 1,
            })?,
    )))
}

/// The `(cell, property, encoded value, vertex id)` of a vertex property
/// index entry, from either layout: the classic index or one generation of
/// the write-back index, whose generation segment the readers never need.
#[cfg(feature = "opencypher")]
pub(super) fn parse_vertex_property_index_key(key: &str) -> Result<(String, String, String, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "vprop_idx", property, encoded, vertex_id] | ["cell", cell_id, "vprop_gidx", property, _, encoded, vertex_id] => {
            Ok((
                (*cell_id).to_string(),
                (*property).to_string(),
                (*encoded).to_string(),
                parse_u64(key, vertex_id, "vertex_id")?,
            ))
        }
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected vertex property index key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
pub(super) fn parse_edge_property_index_key(key: &str) -> Result<(String, String, String, String, VertexId, VertexId)> {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["cell", cell_id, "eprop_idx", edge_type, property, encoded, src, dst] => Ok((
            (*cell_id).to_string(),
            (*edge_type).to_string(),
            (*property).to_string(),
            (*encoded).to_string(),
            parse_u64(key, src, "src")?,
            parse_u64(key, dst, "dst")?,
        )),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "expected edge property index key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct BindingRow {
    values: BTreeMap<String, VertexId>,
    null_values: BTreeSet<String>,
    relationships: BTreeMap<String, BoundRelationship>,
    null_relationships: BTreeSet<String>,
    relationship_metadata: BTreeMap<BoundRelationship, EdgeMetadata>,
    metadata: BTreeMap<String, VertexMetadata>,
}

/// How a statement's effects went: applied, or refused before anything was
/// written because a record it updates changed after its snapshot.
#[cfg(feature = "opencypher")]
#[must_use]
enum StatementApply {
    Applied,
    LostRace,
}

/// How many times a statement that lost a race to a concurrent write re-runs
/// on a fresh snapshot before it gives up: as many as a write transaction
/// retries a storage conflict, so a hot vertex is no likelier to fail.
#[cfg(feature = "opencypher")]
const STATEMENT_RACE_ATTEMPTS: usize = crate::GRAPH_TXN_MAX_RETRIES;

/// A statement's property updates, with the records each was computed from.
#[cfg(feature = "opencypher")]
struct StatementUpdates {
    vertex_metadata: BTreeMap<VertexId, VertexMetadata>,
    relationship_metadata: BTreeMap<BoundRelationship, EdgeMetadata>,
    original_metadata: BTreeMap<VertexId, VertexMetadata>,
    original_edge_metadata: BTreeMap<BoundRelationship, EdgeMetadata>,
}

/// What one mutation statement writes, collected from every matched row
/// before anything is applied.
#[cfg(feature = "opencypher")]
#[derive(Debug, Default)]
struct StatementEffects {
    deleted_relationships: BTreeSet<BoundRelationship>,
    /// Each deleted vertex, and whether any `DELETE` of it was `DETACH`.
    deleted_vertices: BTreeMap<VertexId, bool>,
    vertex_metadata: BTreeMap<VertexId, VertexMetadata>,
    relationship_metadata: BTreeMap<BoundRelationship, EdgeMetadata>,
    /// The record each update started from, to tell a change from a no-op.
    original_metadata: BTreeMap<VertexId, VertexMetadata>,
    original_edge_metadata: BTreeMap<BoundRelationship, EdgeMetadata>,
}

#[cfg(feature = "opencypher")]
impl StatementEffects {
    /// How many batch transactions applying these effects takes: identified
    /// and structural relationship deletes, `DETACH` and plain vertex deletes,
    /// vertex updates and relationship updates each commit separately.
    fn batch_transactions(&self) -> usize {
        let relationship_deletes = |identified: bool| {
            self.deleted_relationships
                .iter()
                .any(|relationship| relationship.relationship_id.is_some() == identified)
        };
        let vertex_deletes = |detach: bool| self.deleted_vertices.values().any(|vertex_detach| *vertex_detach == detach);
        [
            relationship_deletes(true),
            relationship_deletes(false),
            vertex_deletes(true),
            vertex_deletes(false),
            !self.vertex_metadata.is_empty(),
            !self.relationship_metadata.is_empty(),
        ]
        .into_iter()
        .filter(|applies| *applies)
        .count()
    }

    /// Drop, and count as no-ops, the updates that change nothing and the
    /// property updates on something the same statement deletes: applying one
    /// after the delete would recreate the record.
    fn drop_noops(&mut self, result: &mut QueryMutationResult) {
        let deleted_vertices = &self.deleted_vertices;
        let original_metadata = &self.original_metadata;
        self.vertex_metadata.retain(|vertex_id, metadata| {
            let keep = original_metadata.get(vertex_id) != Some(metadata) && !deleted_vertices.contains_key(vertex_id);
            if !keep {
                result.noops = result.noops.saturating_add(1);
            }
            keep
        });
        let deleted_relationships = &self.deleted_relationships;
        let original_edge_metadata = &self.original_edge_metadata;
        self.relationship_metadata.retain(|relationship, metadata| {
            let keep = original_edge_metadata.get(relationship) != Some(metadata)
                && !deleted_relationships.contains(relationship)
                && !deleted_vertices.contains_key(&relationship.src)
                && !deleted_vertices.contains_key(&relationship.dst);
            if !keep {
                result.noops = result.noops.saturating_add(1);
            }
            keep
        });
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BoundRelationship {
    edge_type: String,
    src: VertexId,
    dst: VertexId,
    relationship_id: Option<RelationshipId>,
}

/// Append the rows that pass the group's `WHERE`, or all of them without one.
/// The one pattern of a group a bound can be pushed into an edge traversal
/// for: a single fixed-length hop. Longer paths and variable-length hops
/// expand through [`GraphShard::match_row_patterns_from_rows`] instead.
#[cfg(feature = "opencypher")]
fn bounded_edge_group_pattern(group: &RowMatchGroup) -> Option<&RowPattern> {
    match group.patterns.as_slice() {
        [pattern @ RowPattern::Edge(edge)] if edge.hop_range.is_none() => Some(pattern),
        _ => None,
    }
}

#[cfg(feature = "opencypher")]
fn extend_with_predicate_matches(
    predicate: Option<&RowPredicate>,
    rows: Vec<BindingRow>,
    matches: &mut Vec<BindingRow>,
    budget: &QueryBudget,
) -> Result<()> {
    let Some(predicate) = predicate else {
        matches.extend(rows);
        return Ok(());
    };
    for row in rows {
        budget.check("cypher_group_where")?;
        if row_predicate_matches(&row, predicate)? {
            matches.push(row);
        }
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
fn mutation_return_rows(returning: &MutationReturn, bindings: &[BindingRow]) -> QueryResultSet {
    // Cypher's `count(x)` counts rows in which `x` is not null. It is not a
    // distinct count and not a tally of successful storage deletes: two rows
    // binding the same relationship count twice, and a row whose relationship
    // a concurrent writer already removed still counts. Deriving it from the
    // row set rather than from the delete result keeps that true, because the
    // delete result dedupes by relationship identity.
    let counted = bindings.iter().filter(|row| row.is_bound(&returning.binding)).count() as u64;
    QueryResultSet::new(
        vec![returning.column.clone()],
        vec![QueryRow::new(vec![QueryValue::Count(counted)])],
    )
}

#[cfg(feature = "opencypher")]
fn bound_relationship_delete_mutation(context: &QueryContext, relationship: &BoundRelationship) -> EdgeMutation {
    EdgeMutation {
        cell_id: context.cell_id.clone(),
        edge_type: relationship.edge_type.clone(),
        src: relationship.src,
        dst: relationship.dst,
        idempotency_key: format!(
            "{}.delete.{}.{}.{}.{}",
            context.idempotency_key,
            relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship.relationship_id.map_or_else(|| "edge".to_string(), |id| id.to_string())
        ),
    }
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    value: &'a VertexPropertyValue,
    read_epoch: StorageSequence,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct EncodedRelationshipPropertyLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    src: VertexId,
    dst: VertexId,
    property: &'a str,
    encoded: &'a str,
    read_epoch: StorageSequence,
    latest_snapshot: bool,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct RelationshipPropertyEdgeIndexLookup<'a> {
    cell_id: &'a str,
    edge_type: &'a str,
    property: &'a str,
    encoded: &'a str,
    src: VertexId,
    dst: VertexId,
    budget: &'a QueryBudget,
}

#[cfg(feature = "opencypher")]
struct OrderedRelationshipPropertyCandidate {
    encoded: String,
    src: VertexId,
    dst: VertexId,
    relationship_id: Option<RelationshipId>,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Copy)]
enum OrderedRelationshipPropertyCandidateSource {
    Edge(usize),
    Relationship(usize),
}

#[cfg(feature = "opencypher")]
fn ordered_edge_property_candidate(key: &[u8]) -> Result<OrderedRelationshipPropertyCandidate> {
    let key = String::from_utf8_lossy(key).into_owned();
    let (_, _, _, encoded, src, dst) = parse_edge_property_index_key(&key)?;
    Ok(OrderedRelationshipPropertyCandidate {
        encoded,
        src,
        dst,
        relationship_id: None,
    })
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_candidate(key: &[u8]) -> Result<OrderedRelationshipPropertyCandidate> {
    let key = String::from_utf8_lossy(key).into_owned();
    let (_, _, _, encoded, src, dst, relationship_id) = parse_relationship_property_index_key(&key)?;
    Ok(OrderedRelationshipPropertyCandidate {
        encoded,
        src,
        dst,
        relationship_id: Some(relationship_id),
    })
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_source_candidate<'a>(
    source: OrderedRelationshipPropertyCandidateSource,
    edges: &'a [Option<OrderedRelationshipPropertyCandidate>],
    relationships: &'a [Option<OrderedRelationshipPropertyCandidate>],
) -> &'a OrderedRelationshipPropertyCandidate {
    match source {
        OrderedRelationshipPropertyCandidateSource::Edge(index) => edges[index].as_ref().expect("edge candidate source is populated"),
        OrderedRelationshipPropertyCandidateSource::Relationship(index) => {
            relationships[index].as_ref().expect("relationship candidate source is populated")
        }
    }
}

#[cfg(feature = "opencypher")]
fn ordered_relationship_property_candidate_source(
    edges: &[Option<OrderedRelationshipPropertyCandidate>],
    relationships: &[Option<OrderedRelationshipPropertyCandidate>],
    ascending: bool,
) -> Result<Option<OrderedRelationshipPropertyCandidateSource>> {
    let mut selected = None;
    for source in (0..edges.len())
        .map(OrderedRelationshipPropertyCandidateSource::Edge)
        .chain((0..relationships.len()).map(OrderedRelationshipPropertyCandidateSource::Relationship))
    {
        let candidate = match source {
            OrderedRelationshipPropertyCandidateSource::Edge(index) => edges[index].as_ref(),
            OrderedRelationshipPropertyCandidateSource::Relationship(index) => relationships[index].as_ref(),
        };
        let Some(candidate) = candidate else {
            continue;
        };
        let Some(current_source) = selected else {
            selected = Some(source);
            continue;
        };
        let current = ordered_relationship_property_source_candidate(current_source, edges, relationships);
        let ordering = compare_encoded_property_order(&candidate.encoded, &current.encoded)?;
        if (ascending && ordering == std::cmp::Ordering::Less) || (!ascending && ordering == std::cmp::Ordering::Greater) {
            selected = Some(source);
        }
    }
    Ok(selected)
}

#[cfg(feature = "opencypher")]
fn compare_encoded_property_order(left: &str, right: &str) -> Result<std::cmp::Ordering> {
    let left_type = left.as_bytes().first().copied();
    let right_type = right.as_bytes().first().copied();
    if left_type == right_type {
        return Ok(left.cmp(right));
    }
    let left_rank = encoded_property_value_rank(left)?;
    let right_rank = encoded_property_value_rank(right)?;
    if left_rank != right_rank {
        return Ok(left_rank.cmp(&right_rank));
    }
    let left = decode_numeric_property_index_key(left)?;
    let right = decode_numeric_property_index_key(right)?;
    numeric_property_order(&left, &right).ok_or_else(|| GraphError::CorruptValue {
        key: "relationship property index".to_string(),
        reason: "numeric property key decoded to a non-numeric value".to_string(),
    })
}

#[cfg(feature = "opencypher")]
fn encoded_property_value_rank(encoded: &str) -> Result<u8> {
    match encoded.as_bytes().first().copied() {
        Some(b'b') => Ok(0),
        Some(b'i' | b'j' | b'n') => Ok(1),
        Some(b's') => Ok(2),
        _ => Err(GraphError::CorruptValue {
            key: encoded.to_string(),
            reason: "unknown property index value type".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn decode_numeric_property_index_key(encoded: &str) -> Result<VertexPropertyValue> {
    let Some((kind, payload)) = encoded.split_at_checked(1) else {
        return Err(GraphError::CorruptValue {
            key: encoded.to_string(),
            reason: "empty numeric property index value".to_string(),
        });
    };
    let corrupt = |reason: String| GraphError::CorruptValue {
        key: encoded.to_string(),
        reason,
    };
    match kind {
        "i" => payload
            .parse::<u64>()
            .map(VertexPropertyValue::Integer)
            .map_err(|error| corrupt(format!("invalid unsigned property index value: {error}"))),
        "j" => u64::from_str_radix(payload, 16)
            .map(|value| VertexPropertyValue::SignedInteger((value ^ (1_u64 << 63)) as i64))
            .map_err(|error| corrupt(format!("invalid signed property index value: {error}"))),
        "n" => u64::from_str_radix(payload, 16)
            .map(|sortable| {
                let bits = if sortable & (1_u64 << 63) == 0 {
                    !sortable
                } else {
                    sortable ^ (1_u64 << 63)
                };
                VertexPropertyValue::Float(QueryFloat(f64::from_bits(bits)))
            })
            .map_err(|error| corrupt(format!("invalid float property index value: {error}"))),
        _ => Err(corrupt("property index value is not numeric".to_string())),
    }
}

#[cfg(feature = "opencypher")]
struct OrderedRelationshipPropertyIndexSpec<'a> {
    edge: &'a RowEdgePattern,
    property: &'a str,
    predicate: &'a RowPredicate,
    ascending: bool,
    limit: usize,
}

#[cfg(feature = "opencypher")]
impl BindingRow {
    /// Whether `binding` names a non-null vertex or relationship in this row.
    ///
    /// An OPTIONAL MATCH that found nothing records the name in
    /// `null_values`/`null_relationships` rather than omitting it, so a
    /// presence check has to consult the bound maps, not the null sets.
    fn is_bound(&self, binding: &str) -> bool {
        self.values.contains_key(binding) || self.relationships.contains_key(binding)
    }

    fn from_node(pattern: &RowNodePattern, vertex_id: VertexId) -> Option<Self> {
        let mut row = Self::default();
        if !row.bind(pattern.binding.as_deref(), vertex_id) {
            return None;
        }
        Some(row)
    }

    fn from_edge(pattern: &RowEdgePattern, src: VertexId, dst: VertexId) -> Option<Self> {
        let relationship = BoundRelationship {
            edge_type: pattern.edge_type.clone(),
            src,
            dst,
            relationship_id: None,
        };
        Self::from_relationship(pattern, relationship, EdgeMetadata::default())
    }

    fn from_relationship(pattern: &RowEdgePattern, relationship: BoundRelationship, metadata: EdgeMetadata) -> Option<Self> {
        let mut row = Self::default();
        if !row.bind(pattern.src.binding.as_deref(), relationship.src) {
            return None;
        }
        if !row.bind(pattern.dst.binding.as_deref(), relationship.dst) {
            return None;
        }
        if !row.bind_relationship(pattern.binding.as_deref(), relationship.clone()) {
            return None;
        }
        row.relationship_metadata.insert(relationship, metadata);
        Some(row)
    }

    fn bind(&mut self, binding: Option<&str>, value: VertexId) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        if self.null_values.contains(binding) {
            return false;
        }
        match self.values.get(binding) {
            Some(existing) => *existing == value,
            None => {
                self.values.insert(binding.to_string(), value);
                true
            }
        }
    }

    fn bind_relationship(&mut self, binding: Option<&str>, value: BoundRelationship) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        if self.null_relationships.contains(binding) {
            return false;
        }
        match self.relationships.get(binding) {
            Some(existing) => *existing == value,
            None => {
                self.relationships.insert(binding.to_string(), value);
                true
            }
        }
    }

    fn get(&self, binding: &str) -> Result<VertexId> {
        if self.null_values.contains(binding) {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("variable {binding} is null"),
            });
        }
        self.values.get(binding).copied().ok_or_else(|| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: format!("unbound variable {binding}"),
        })
    }

    fn get_optional(&self, binding: &str) -> Result<Option<VertexId>> {
        if self.null_values.contains(binding) {
            return Ok(None);
        }
        self.values
            .get(binding)
            .copied()
            .map(Some)
            .ok_or_else(|| GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("unbound variable {binding}"),
            })
    }

    fn mark_optional_group_nulls(&mut self, group: &RowMatchGroup) {
        for pattern in &group.patterns {
            self.mark_optional_pattern_nulls(pattern);
        }
    }

    fn mark_optional_pattern_nulls(&mut self, pattern: &RowPattern) {
        match pattern {
            RowPattern::Node(node) => self.mark_optional_node_nulls(node),
            RowPattern::Edge(edge) => {
                if let Some(binding) = &edge.binding {
                    if !self.relationships.contains_key(binding) {
                        self.null_relationships.insert(binding.clone());
                    }
                }
                self.mark_optional_node_nulls(&edge.src);
                self.mark_optional_node_nulls(&edge.dst);
            }
        }
    }

    fn mark_optional_node_nulls(&mut self, node: &RowNodePattern) {
        if let Some(binding) = &node.binding {
            if !self.values.contains_key(binding) {
                self.null_values.insert(binding.clone());
            }
        }
    }

    fn join(&self, other: &Self) -> Option<Self> {
        let mut joined = self.clone();
        for (binding, value) in &other.values {
            if !joined.bind(Some(binding), *value) {
                return None;
            }
        }
        for binding in &other.null_values {
            if joined.values.contains_key(binding) {
                return None;
            }
            joined.null_values.insert(binding.clone());
        }
        for (binding, metadata) in &other.metadata {
            match joined.metadata.get(binding) {
                Some(existing) if existing != metadata => return None,
                Some(_) => {}
                None => {
                    joined.metadata.insert(binding.clone(), metadata.clone());
                }
            }
        }
        for (binding, relationship) in &other.relationships {
            match joined.relationships.get(binding) {
                Some(existing) if existing != relationship => return None,
                Some(_) => {}
                None => {
                    joined.relationships.insert(binding.clone(), relationship.clone());
                }
            }
        }
        for binding in &other.null_relationships {
            if joined.relationships.contains_key(binding) {
                return None;
            }
            joined.null_relationships.insert(binding.clone());
        }
        for (relationship, metadata) in &other.relationship_metadata {
            match joined.relationship_metadata.get(relationship) {
                Some(existing) if existing.properties.is_empty() => {
                    joined.relationship_metadata.insert(relationship.clone(), metadata.clone());
                }
                Some(existing) if metadata.properties.is_empty() || existing == metadata => {}
                Some(_) => return None,
                None => {
                    joined.relationship_metadata.insert(relationship.clone(), metadata.clone());
                }
            }
        }
        Some(joined)
    }
}

#[cfg(feature = "opencypher")]
fn common_binding_row_bound_names(rows: &[BindingRow]) -> BTreeSet<String> {
    let Some(first) = rows.first() else {
        return BTreeSet::new();
    };
    let mut common = binding_row_bound_names(first);
    for row in &rows[1..] {
        let bound = binding_row_bound_names(row);
        common.retain(|binding| bound.contains(binding));
    }
    common
}

#[cfg(feature = "opencypher")]
fn binding_rows_bound_names_union(rows: &[BindingRow]) -> BTreeSet<String> {
    rows.iter().flat_map(binding_row_bound_names).collect()
}

#[cfg(feature = "opencypher")]
fn binding_row_bound_names(row: &BindingRow) -> BTreeSet<String> {
    row.values.keys().chain(row.relationships.keys()).cloned().collect()
}

#[cfg(feature = "opencypher")]
fn row_pattern_bound_names(pattern: &RowPattern) -> BTreeSet<String> {
    match pattern {
        RowPattern::Node(node) => node.binding.iter().cloned().collect(),
        RowPattern::Edge(edge) => {
            let mut names = BTreeSet::new();
            if let Some(binding) = &edge.src.binding {
                names.insert(binding.clone());
            }
            if let Some(binding) = &edge.dst.binding {
                names.insert(binding.clone());
            }
            names
        }
    }
}

#[cfg(feature = "opencypher")]
fn binding_row_join_key(row: &BindingRow, bindings: &[String]) -> Option<Vec<VertexId>> {
    let mut key = Vec::with_capacity(bindings.len());
    for binding in bindings {
        key.push(*row.values.get(binding)?);
    }
    Some(key)
}

/// The first pattern property that a property index can look up.
///
/// Lists and dates are never index keys (`is_scalar_indexable`), so an index
/// lookup on one finds nothing. Relationship matching uses this to choose its
/// lookup property; with none, it loads the pair's relationships and compares
/// every pattern property directly, as it does for a pattern without
/// properties.
#[cfg(feature = "opencypher")]
fn first_indexable_property(properties: &BTreeMap<String, VertexPropertyValue>) -> Option<(&String, &VertexPropertyValue)> {
    properties.iter().find(|(_, value)| value.is_scalar_indexable())
}

#[cfg(feature = "opencypher")]
fn hash_joinable_pattern(pattern: &RowPattern) -> bool {
    match pattern {
        RowPattern::Node(node) => node.id.is_none() && (!node.labels.is_empty() || node_has_metadata_constraints(node)),
        RowPattern::Edge(edge) => edge.hop_range.is_none() && !edge.properties.is_empty(),
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectedQueryRow {
    row: QueryRow,
    sort_keys: Vec<QueryValue>,
}

#[cfg(feature = "opencypher")]
struct OrderedWindowRow<'a> {
    projected: ProjectedQueryRow,
    ordinal: usize,
    order_by: &'a [RowSort],
}

#[cfg(feature = "opencypher")]
impl Ord for OrderedWindowRow<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_projected_rows(&self.projected, &other.projected, self.order_by).then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

#[cfg(feature = "opencypher")]
impl PartialOrd for OrderedWindowRow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "opencypher")]
impl PartialEq for OrderedWindowRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

#[cfg(feature = "opencypher")]
impl Eq for OrderedWindowRow<'_> {}

// A max-heap retains exactly the best window. Re-sorting K rows after each
// 16-row hydration batch makes large candidate lookups unnecessarily quadratic.
// The input ordinal preserves the stable-sort behavior for equal sort keys.
#[cfg(feature = "opencypher")]
struct OrderedRowWindow<'a> {
    rows: std::collections::BinaryHeap<OrderedWindowRow<'a>>,
    limit: usize,
    next_ordinal: usize,
    order_by: &'a [RowSort],
}

#[cfg(feature = "opencypher")]
impl<'a> OrderedRowWindow<'a> {
    fn new(limit: usize, order_by: &'a [RowSort]) -> Self {
        Self {
            rows: std::collections::BinaryHeap::new(),
            limit,
            next_ordinal: 0,
            order_by,
        }
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn push(&mut self, row: QueryRow, sort_keys: Vec<QueryValue>) {
        let entry = OrderedWindowRow {
            projected: ProjectedQueryRow { row, sort_keys },
            ordinal: self.next_ordinal,
            order_by: self.order_by,
        };
        self.next_ordinal += 1;
        if self.rows.len() < self.limit {
            self.rows.push(entry);
        } else if let Some(mut worst) = self.rows.peek_mut() {
            if entry < *worst {
                *worst = entry;
            }
        }
    }

    fn into_rows(self) -> Vec<ProjectedQueryRow> {
        self.rows.into_sorted_vec().into_iter().map(|entry| entry.projected).collect()
    }
}

#[cfg(feature = "opencypher")]
fn push_projected_query_row(projected: &mut Vec<ProjectedQueryRow>, row: QueryRow, sort_keys: Vec<QueryValue>) {
    projected.push(ProjectedQueryRow { row, sort_keys });
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum RowScalarValue {
    Value(VertexPropertyValue),
    Missing,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum AggregateAccumulator {
    CountAll(u64),
    CountExpression(u64),
    Sum(u128),
    Avg { sum: u128, count: u64 },
    Collect(Vec<QueryValue>),
}

#[cfg(feature = "opencypher")]
fn constrain_row_pattern(pattern: &RowPattern, row: &BindingRow) -> Result<Option<RowPattern>> {
    Ok(Some(match pattern {
        RowPattern::Node(node) => {
            let Some(node) = constrain_row_node_pattern(node, row)? else {
                return Ok(None);
            };
            RowPattern::Node(node)
        }
        RowPattern::Edge(edge) => {
            if let Some(binding) = &edge.binding {
                if row.null_relationships.contains(binding) {
                    return Ok(None);
                }
            }
            let Some(src) = constrain_row_node_pattern(&edge.src, row)? else {
                return Ok(None);
            };
            let Some(dst) = constrain_row_node_pattern(&edge.dst, row)? else {
                return Ok(None);
            };
            RowPattern::Edge(RowEdgePattern {
                binding: edge.binding.clone(),
                edge_type: edge.edge_type.clone(),
                src,
                dst,
                properties: edge.properties.clone(),
                hop_range: edge.hop_range,
                direction: edge.direction,
            })
        }
    }))
}

#[cfg(feature = "opencypher")]
fn constrain_row_node_pattern(node: &RowNodePattern, row: &BindingRow) -> Result<Option<RowNodePattern>> {
    let Some(binding) = &node.binding else {
        return Ok(Some(node.clone()));
    };
    if row.null_values.contains(binding) {
        return Ok(None);
    }
    let Some(bound_id) = row.values.get(binding).copied() else {
        return Ok(Some(node.clone()));
    };
    if matches!(node.id, Some(pattern_id) if pattern_id != bound_id) {
        return Ok(None);
    }
    let mut constrained = node.clone();
    constrained.id = Some(bound_id);
    if let Some(metadata) = row.metadata.get(binding) {
        if !vertex_metadata_matches(metadata, &constrained) {
            return Ok(None);
        }
    }
    Ok(Some(constrained))
}

#[cfg(feature = "opencypher")]
fn row_matches_edge_pattern(row: &BindingRow, pattern: &RowEdgePattern) -> Result<bool> {
    if !(row_matches_node(row, &pattern.src)? && row_matches_node(row, &pattern.dst)?) {
        return Ok(false);
    }
    if pattern.properties.is_empty() {
        return Ok(true);
    }
    let relationship = relationship_identity_for_pattern(row, pattern)?;
    let Some(metadata) = row.relationship_metadata.get(&relationship) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: "relationship metadata was not hydrated".to_string(),
        });
    };
    Ok(pattern.properties.iter().all(|(property, value)| {
        metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
    }))
}

#[cfg(feature = "opencypher")]
fn relationship_metadata_matches(metadata: &EdgeMetadata, pattern: &RowEdgePattern) -> bool {
    pattern.properties.iter().all(|(property, value)| {
        metadata
            .properties
            .get(property)
            .is_some_and(|existing| vertex_property_values_equal(existing, value))
    })
}

#[cfg(feature = "opencypher")]
fn relationship_identity_for_pattern(row: &BindingRow, pattern: &RowEdgePattern) -> Result<BoundRelationship> {
    if let Some(binding) = &pattern.binding {
        return row.relationships.get(binding).cloned().ok_or_else(|| GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: format!("unbound relationship {binding}"),
        });
    }
    if let (Some(src_binding), Some(dst_binding)) = (pattern.src.binding.as_deref(), pattern.dst.binding.as_deref()) {
        return Ok(BoundRelationship {
            edge_type: pattern.edge_type.clone(),
            src: row.get(src_binding)?,
            dst: row.get(dst_binding)?,
            relationship_id: None,
        });
    }
    let mut matches = row
        .relationship_metadata
        .keys()
        .filter(|relationship| relationship.edge_type == pattern.edge_type);
    let Some(relationship) = matches.next().cloned() else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: "relationship pattern has no bound identity".to_string(),
        });
    };
    if matches.next().is_some() {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Pattern,
            dialect: "OpenCypher",
            feature: "anonymous relationship property pattern is ambiguous".to_string(),
        });
    }
    Ok(relationship)
}

#[cfg(feature = "opencypher")]
fn row_matches_node(row: &BindingRow, node: &RowNodePattern) -> Result<bool> {
    let Some(binding) = &node.binding else {
        return Ok(true);
    };
    if row.null_values.contains(binding) {
        return Ok(false);
    }
    if matches!(node.id, Some(id) if row.get(binding)? != id) {
        return Ok(false);
    }
    if !node_has_metadata_constraints(node) {
        return Ok(true);
    }
    let Some(metadata) = row.metadata.get(binding) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: format!("metadata for bound variable {binding} was not hydrated"),
        });
    };
    Ok(vertex_metadata_matches(metadata, node))
}

#[cfg(feature = "opencypher")]
fn node_has_metadata_constraints(node: &RowNodePattern) -> bool {
    !node.labels.is_empty() || node.properties.keys().any(|property| property != "id")
}

#[cfg(feature = "opencypher")]
fn vertex_metadata_matches(metadata: &VertexMetadata, node: &RowNodePattern) -> bool {
    node.labels.iter().all(|label| metadata.labels.contains(label))
        && node
            .properties
            .iter()
            .filter(|(property, _)| property.as_str() != "id")
            .all(|(property, value)| {
                metadata
                    .properties
                    .get(property)
                    .is_some_and(|existing| vertex_property_values_equal(existing, value))
            })
}

#[cfg(feature = "opencypher")]
fn row_predicate_matches(row: &BindingRow, predicate: &RowPredicate) -> Result<bool> {
    Ok(eval_row_predicate(row, predicate)? == Some(true))
}

/// None is Cypher's unknown truth value. Collapse it only at WHERE, never
/// inside a boolean expression: NOT unknown must remain unknown.
#[cfg(feature = "opencypher")]
fn eval_row_predicate(row: &BindingRow, predicate: &RowPredicate) -> Result<Option<bool>> {
    Ok(match predicate {
        RowPredicate::Compare { left, op, right } => {
            compare_row_values(eval_row_expression(row, left)?, *op, eval_row_expression(row, right)?)?
        }
        RowPredicate::StartsWith { expression, prefix } => match eval_row_expression(row, expression)? {
            RowScalarValue::Value(VertexPropertyValue::String(value)) => Some(value.starts_with(prefix)),
            RowScalarValue::Value(_) => Some(false),
            RowScalarValue::Missing => None,
        },
        RowPredicate::In {
            list: RowInList::Values(values),
            ..
        } if values.is_empty() => Some(false),
        RowPredicate::In {
            expression,
            list: RowInList::Values(values),
        } => match eval_row_expression(row, expression)? {
            RowScalarValue::Value(value) => Some(values.iter().any(|candidate| vertex_property_values_equal(candidate, &value))),
            RowScalarValue::Missing => None,
        },
        RowPredicate::In {
            expression,
            list: RowInList::Expression(list),
        } => match eval_row_expression(row, list)? {
            RowScalarValue::Value(VertexPropertyValue::List(values)) if values.is_empty() => Some(false),
            RowScalarValue::Value(VertexPropertyValue::List(values)) => match eval_row_expression(row, expression)? {
                RowScalarValue::Value(value) => Some(property_list_contains(&values, &value)),
                RowScalarValue::Missing => None,
            },
            RowScalarValue::Missing => None,
            RowScalarValue::Value(_) => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Evaluation,
                    dialect: "OpenCypher",
                    feature: "the right side of IN must evaluate to a list".to_string(),
                });
            }
        },
        RowPredicate::And(left, right) => match eval_row_predicate(row, left)? {
            Some(false) => Some(false),
            Some(true) => eval_row_predicate(row, right)?,
            None => match eval_row_predicate(row, right)? {
                Some(false) => Some(false),
                _ => None,
            },
        },
        RowPredicate::Or(left, right) => match eval_row_predicate(row, left)? {
            Some(true) => Some(true),
            Some(false) => eval_row_predicate(row, right)?,
            None => match eval_row_predicate(row, right)? {
                Some(true) => Some(true),
                _ => None,
            },
        },
        RowPredicate::Not(inner) => eval_row_predicate(row, inner)?.map(|value| !value),
        // The test for unknown is itself never unknown: a missing property is
        // the only null a row holds.
        RowPredicate::IsNull { expression, negated } => {
            Some(matches!(eval_row_expression(row, expression)?, RowScalarValue::Missing) != *negated)
        }
        // The binding itself: null exactly when nothing is bound to it, which
        // an OPTIONAL MATCH that did not match records explicitly. A name the
        // row never bound is null too -- there is nothing there either.
        RowPredicate::BindingIsNull { binding, negated } => {
            let bound = !row.null_values.contains(binding)
                && !row.null_relationships.contains(binding)
                && (row.values.contains_key(binding) || row.relationships.contains_key(binding));
            Some(!bound != *negated)
        }
    })
}

#[cfg(feature = "opencypher")]
fn predicate_guarantees_string_property(predicate: &RowPredicate, binding: &str, property: &str) -> bool {
    match predicate {
        RowPredicate::StartsWith {
            expression:
                RowExpression::Property {
                    binding: candidate_binding,
                    property: candidate_property,
                },
            ..
        } => candidate_binding == binding && candidate_property == property,
        RowPredicate::StartsWith { .. } => false,
        RowPredicate::And(left, right) => {
            predicate_guarantees_string_property(left, binding, property) || predicate_guarantees_string_property(right, binding, property)
        }
        RowPredicate::Or(left, right) => {
            predicate_guarantees_string_property(left, binding, property) && predicate_guarantees_string_property(right, binding, property)
        }
        RowPredicate::Compare { .. }
        | RowPredicate::In { .. }
        | RowPredicate::Not(_)
        | RowPredicate::IsNull { .. }
        | RowPredicate::BindingIsNull { .. } => false,
    }
}

#[cfg(feature = "opencypher")]
fn string_property_lower_bound(predicate: &RowPredicate, binding: &str, property: &str) -> Option<String> {
    fn direct_bound(left: &RowExpression, op: RowComparisonOp, right: &RowExpression, binding: &str, property: &str) -> Option<String> {
        let is_property = |expression: &RowExpression| {
            matches!(
                expression,
                RowExpression::Property {
                    binding: candidate_binding,
                    property: candidate_property,
                } if candidate_binding == binding && candidate_property == property
            )
        };
        let string_literal = |expression: &RowExpression| match expression {
            RowExpression::Literal(VertexPropertyValue::String(value)) => Some(value.clone()),
            _ => None,
        };

        if is_property(left) && matches!(op, RowComparisonOp::Eq | RowComparisonOp::Gt | RowComparisonOp::Gte) {
            return string_literal(right);
        }
        if is_property(right) && matches!(op, RowComparisonOp::Eq | RowComparisonOp::Lt | RowComparisonOp::Lte) {
            return string_literal(left);
        }
        None
    }

    match predicate {
        RowPredicate::Compare { left, op, right } => direct_bound(left, *op, right, binding, property),
        RowPredicate::StartsWith { expression, prefix } => matches!(
            expression,
            RowExpression::Property {
                binding: candidate_binding,
                property: candidate_property,
            } if candidate_binding == binding && candidate_property == property
        )
        .then(|| prefix.clone()),
        // AND may use either proven lower bound, and the stronger bound is
        // safe. OR needs a bound from both branches; the weaker one is the
        // earliest value that either branch can admit.
        RowPredicate::And(left, right) => {
            match (
                string_property_lower_bound(left, binding, property),
                string_property_lower_bound(right, binding, property),
            ) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (left @ Some(_), None) | (None, left @ Some(_)) => left,
                (None, None) => None,
            }
        }
        RowPredicate::Or(left, right) => {
            let left = string_property_lower_bound(left, binding, property)?;
            let right = string_property_lower_bound(right, binding, property)?;
            Some(left.min(right))
        }
        // `IN` deliberately yields no lower bound. The smallest value in the
        // list would be a sound one, but an ordered scan from it is strictly
        // worse than what `row_predicate_property_equality_constraint` already
        // gives this predicate: a multi-value seek that visits only the listed
        // values. Offering a bound here would let the planner pick the weaker
        // access path.
        RowPredicate::In { .. } => None,
        RowPredicate::Not(_) | RowPredicate::IsNull { .. } | RowPredicate::BindingIsNull { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
pub(super) struct OrderedStringVertexIndexSpec<'a> {
    pub(super) node: &'a RowNodePattern,
    pub(super) binding: &'a str,
    pub(super) property: &'a str,
    pub(super) predicate: &'a RowPredicate,
    pub(super) ascending: bool,
    pub(super) limit: usize,
}

#[cfg(feature = "opencypher")]
pub(super) fn ordered_string_vertex_index_spec<'a>(
    query: &'a ParsedRowQuery,
    window: QueryWindow,
) -> Option<OrderedStringVertexIndexSpec<'a>> {
    let [RowPattern::Node(node)] = query.patterns.as_slice() else {
        return None;
    };
    let binding = node.binding.as_deref()?;
    let first_sort = query.order_by.first()?;
    let RowSortExpression::Property {
        binding: sort_binding,
        property,
    } = &first_sort.expression
    else {
        return None;
    };
    let limit = window.limit?;
    let predicate = query.predicate.as_ref()?;
    let simple_match_group = query.pattern_groups.is_empty()
        || matches!(
            query.pattern_groups.as_slice(),
            [group]
                if !group.optional
                    && group.patterns == query.patterns
                    && group.predicate.as_ref() == query.predicate.as_ref()
        );
    if query.distinct
        || !simple_match_group
        || !query.union_arms.is_empty()
        || row_projections_have_aggregates(&query.projections)
        || sort_binding != binding
        || !predicate_guarantees_string_property(predicate, binding, property)
    {
        return None;
    }
    Some(OrderedStringVertexIndexSpec {
        node,
        binding,
        property,
        predicate,
        ascending: first_sort.ascending,
        limit,
    })
}

/// One binding's property pinned to a set of literal values by a WHERE clause.
///
/// The binding may name a relationship or a node — the extractor below reads
/// the predicate, not the pattern, so nothing here is edge-specific. Two
/// callers use it against different pattern kinds: the relationship index plan
/// in `query_optimizer.rs` and the vertex equality pushdown beside it.
#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RowPredicatePropertyEqualityConstraint {
    pub(super) binding: String,
    pub(super) property: String,
    pub(super) values: Vec<VertexPropertyValue>,
}

#[cfg(feature = "opencypher")]
enum NodeEqualityProbe {
    PredicateEqualities(usize),
    Ordered(usize),
}

/// Every equality constraint a WHERE predicate proves, not just the first.
///
/// [`row_predicate_property_equality_constraint`] stops at the first thing it
/// can prove, which is the right answer for a caller that can use exactly one
/// — the edge property index is keyed on a single property. It is the wrong
/// answer for a caller that wants the *best* one, because `AND` is
/// commutative and the extractor is not: `WHERE s.tier = 'common' AND
/// s.app_external_id = 'ext-3'` yielded the tier, and writing the same two
/// terms the other way round yielded the id. Worse, a leading term the
/// extractor can prove but a caller cannot *use* — an `OR` folded to two
/// values — masked an indexable term to its right entirely, and the query
/// went back to a label scan.
///
/// Descending only `And` is what keeps this sound. Both sides of an `And`
/// hold, so every constraint either side proves is a constraint on the row.
/// An `Or` proves only what the singular extractor already folds into one
/// multi-value constraint, and splitting it would claim each branch
/// separately, which is exactly the row-dropping mistake the singular
/// extractor refuses to make.
#[cfg(feature = "opencypher")]
pub(super) fn row_predicate_property_equality_constraints(predicate: &RowPredicate) -> Vec<RowPredicatePropertyEqualityConstraint> {
    match predicate {
        RowPredicate::And(left, right) => {
            let mut constraints = row_predicate_property_equality_constraints(left);
            constraints.extend(row_predicate_property_equality_constraints(right));
            constraints
        }
        other => row_predicate_property_equality_constraint(other).into_iter().collect(),
    }
}

/// Pull an equality constraint out of a WHERE predicate, if one is there.
///
/// Accepts `binding.property = literal` in either operand order, descends AND
/// chains taking the first constraint it can prove, and folds an OR of the
/// same binding and property into one multi-value constraint. Everything else
/// — NOT, ordered comparisons, STARTS WITH, an OR that spans two properties —
/// returns `None`, because the index seek it would authorise could drop rows
/// the predicate keeps.
#[cfg(feature = "opencypher")]
pub(super) fn row_predicate_property_equality_constraint(predicate: &RowPredicate) -> Option<RowPredicatePropertyEqualityConstraint> {
    match predicate {
        RowPredicate::Compare {
            left,
            op: RowComparisonOp::Eq,
            right,
        } => row_property_literal_equality(left, right).or_else(|| row_property_literal_equality(right, left)),
        RowPredicate::And(left, right) => {
            row_predicate_property_equality_constraint(left).or_else(|| row_predicate_property_equality_constraint(right))
        }
        RowPredicate::Or(left, right) => {
            let mut left = row_predicate_property_equality_constraint(left)?;
            let right = row_predicate_property_equality_constraint(right)?;
            if left.binding != right.binding || left.property != right.property {
                return None;
            }
            for value in right.values {
                if !left.values.contains(&value) {
                    left.values.push(value);
                }
            }
            Some(left)
        }
        // The same multi-value seek the OR-fold above produces. An empty list
        // yields no constraint on purpose: a zero-value seek is not a narrower
        // plan, and letting it through would authorise an index lookup that
        // returns nothing while the predicate still has to reject every row.
        RowPredicate::In {
            expression: RowExpression::Property { binding, property },
            list: RowInList::Values(values),
        } if !values.is_empty() => Some(RowPredicatePropertyEqualityConstraint {
            binding: binding.clone(),
            property: property.clone(),
            values: values.clone(),
        }),
        RowPredicate::Compare { .. }
        | RowPredicate::StartsWith { .. }
        | RowPredicate::In { .. }
        | RowPredicate::Not(_)
        | RowPredicate::IsNull { .. }
        | RowPredicate::BindingIsNull { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
fn row_property_literal_equality(
    property_expression: &RowExpression,
    literal_expression: &RowExpression,
) -> Option<RowPredicatePropertyEqualityConstraint> {
    let RowExpression::Property { binding, property } = property_expression else {
        return None;
    };
    let RowExpression::Literal(value) = literal_expression else {
        return None;
    };
    Some(RowPredicatePropertyEqualityConstraint {
        binding: binding.clone(),
        property: property.clone(),
        values: vec![value.clone()],
    })
}

/// Vertex ids a WHERE predicate pins for one binding: `n.id = 7`,
/// `n.id IN [1, 2]`, `n.id IN $ids`.
///
/// `n.id` is not a property. `node_id_expression_binding` routes every `x.id`
/// to `RowExpression::NodeId`, so the property-equality extractor above never
/// sees it, and an unlabelled `MATCH (n) WHERE n.id IN [1, 2]` reached the node
/// matcher with no candidate source at all and failed as an unsupported query.
/// These ids are a candidate set, never a decision: the group predicate still
/// runs over the matched rows afterwards.
#[cfg(feature = "opencypher")]
fn row_predicate_node_id_values(predicate: &RowPredicate) -> Option<(String, Vec<VertexId>)> {
    match predicate {
        RowPredicate::Compare {
            left,
            op: RowComparisonOp::Eq,
            right,
        } => node_id_literal_equality(left, right).or_else(|| node_id_literal_equality(right, left)),
        RowPredicate::In {
            expression: RowExpression::NodeId { binding },
            list: RowInList::Values(values),
        } => {
            let ids = vertex_id_literals(values)?;
            Some((binding.clone(), ids))
        }
        // Both sides of an `And` hold, so either side's ids already contain
        // every row the whole predicate keeps. Seeking one side and filtering
        // with the rest is the same bargain the property extractor makes.
        RowPredicate::And(left, right) => row_predicate_node_id_values(left).or_else(|| row_predicate_node_id_values(right)),
        // Only one side of an `Or` need hold, so one side's ids are not a
        // superset and seeding from them would drop the other side's rows. A
        // union over the same binding is a superset; anything else falls back
        // to the ordinary plan.
        RowPredicate::Or(left, right) => {
            let (binding, mut ids) = row_predicate_node_id_values(left)?;
            let (right_binding, right_ids) = row_predicate_node_id_values(right)?;
            if binding != right_binding {
                return None;
            }
            for id in right_ids {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
            Some((binding, ids))
        }
        RowPredicate::Compare { .. }
        | RowPredicate::StartsWith { .. }
        | RowPredicate::In { .. }
        | RowPredicate::Not(_)
        | RowPredicate::IsNull { .. }
        | RowPredicate::BindingIsNull { .. } => None,
    }
}

#[cfg(feature = "opencypher")]
fn node_id_literal_equality(id_expression: &RowExpression, literal_expression: &RowExpression) -> Option<(String, Vec<VertexId>)> {
    let RowExpression::NodeId { binding } = id_expression else {
        return None;
    };
    let RowExpression::Literal(value) = literal_expression else {
        return None;
    };
    Some((binding.clone(), vec![vertex_id_literal(value)?]))
}

/// Drops the entries that are not vertex ids rather than refusing the whole
/// list: `n.id IN [1, 'x']` still seeks vertex 1, and `'x'` names no vertex, so
/// the shorter list is still a superset of the rows the predicate keeps.
/// `None` when nothing usable is left, including for an empty list, so the
/// caller plans the query the way it did before instead of authorising a seek
/// that reads nothing.
#[cfg(feature = "opencypher")]
fn vertex_id_literals(values: &[VertexPropertyValue]) -> Option<Vec<VertexId>> {
    let mut ids = Vec::with_capacity(values.len());
    for value in values {
        if let Some(id) = vertex_id_literal(value) {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
    }
    (!ids.is_empty()).then_some(ids)
}

#[cfg(feature = "opencypher")]
fn vertex_id_literal(value: &VertexPropertyValue) -> Option<VertexId> {
    match value {
        VertexPropertyValue::Integer(value) => Some(*value),
        VertexPropertyValue::SignedInteger(value) => VertexId::try_from(*value).ok(),
        VertexPropertyValue::Float(_)
        | VertexPropertyValue::Bool(_)
        | VertexPropertyValue::String(_)
        | VertexPropertyValue::Date(_)
        | VertexPropertyValue::List(_) => None,
    }
}

#[cfg(feature = "opencypher")]
fn row_predicate_requires_scalar_indexed_property(predicate: &RowPredicate, binding: &str, property: &str) -> bool {
    let expression_matches = |expression: &RowExpression| {
        matches!(
            expression,
            RowExpression::Property {
                binding: candidate_binding,
                property: candidate_property,
            } if candidate_binding == binding && candidate_property == property
        )
    };
    let scalar_literal = |expression: &RowExpression| matches!(expression, RowExpression::Literal(value) if value.is_scalar_indexable());
    match predicate {
        RowPredicate::Compare {
            left,
            op: RowComparisonOp::Eq | RowComparisonOp::Lt | RowComparisonOp::Gt | RowComparisonOp::Lte | RowComparisonOp::Gte,
            right,
        } => (expression_matches(left) && scalar_literal(right)) || (expression_matches(right) && scalar_literal(left)),
        RowPredicate::Compare { .. } => false,
        RowPredicate::StartsWith { expression, .. } => expression_matches(expression),
        RowPredicate::In {
            expression,
            list: RowInList::Values(values),
        } => expression_matches(expression) && values.iter().all(VertexPropertyValue::is_scalar_indexable),
        RowPredicate::In {
            list: RowInList::Expression(_),
            ..
        } => false,
        RowPredicate::And(left, right) => {
            row_predicate_requires_scalar_indexed_property(left, binding, property)
                || row_predicate_requires_scalar_indexed_property(right, binding, property)
        }
        RowPredicate::Or(left, right) => {
            row_predicate_requires_scalar_indexed_property(left, binding, property)
                && row_predicate_requires_scalar_indexed_property(right, binding, property)
        }
        RowPredicate::Not(_) => false,
        RowPredicate::IsNull { .. } => false,
        RowPredicate::BindingIsNull { .. } => false,
    }
}

#[cfg(feature = "opencypher")]
fn retain_binding_rows_matching_predicate(rows: &mut Vec<BindingRow>, predicate: &RowPredicate, budget: &QueryBudget) -> Result<()> {
    let mut matching = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        budget.check("cypher_ordered_relationship_property_predicate")?;
        if row_predicate_matches(&row, predicate)? {
            matching.push(row);
        }
    }
    *rows = matching;
    Ok(())
}

#[cfg(feature = "opencypher")]
fn eval_row_expression(row: &BindingRow, expression: &RowExpression) -> Result<RowScalarValue> {
    match expression {
        RowExpression::NodeId { binding } => match row.get_optional(binding)? {
            Some(value) => Ok(RowScalarValue::Value(VertexPropertyValue::Integer(value))),
            None => Ok(RowScalarValue::Missing),
        },
        RowExpression::Property { binding, property } => Ok(match binding_property(row, binding, property)? {
            Some(value) => RowScalarValue::Value(value),
            None => RowScalarValue::Missing,
        }),
        RowExpression::Literal(value) => Ok(RowScalarValue::Value(value.clone())),
        RowExpression::Null => Ok(RowScalarValue::Missing),
    }
}

#[cfg(feature = "opencypher")]
fn compare_row_values(left: RowScalarValue, op: RowComparisonOp, right: RowScalarValue) -> Result<Option<bool>> {
    let (RowScalarValue::Value(left), RowScalarValue::Value(right)) = (left, right) else {
        return Ok(None);
    };
    compare_vertex_property_values(&left, op, &right).map(Some)
}

#[cfg(feature = "opencypher")]
fn compare_vertex_property_values(left: &VertexPropertyValue, op: RowComparisonOp, right: &VertexPropertyValue) -> Result<bool> {
    Ok(match op {
        RowComparisonOp::Eq => semantic_property_order(left, right)
            .map(|ordering| compare_ordering(ordering, op))
            .unwrap_or(left == right),
        RowComparisonOp::Ne => semantic_property_order(left, right)
            .map(|ordering| compare_ordering(ordering, op))
            .unwrap_or(left != right),
        RowComparisonOp::Lt | RowComparisonOp::Gt | RowComparisonOp::Lte | RowComparisonOp::Gte => {
            match semantic_property_order(left, right) {
                Some(ordering) => compare_ordering(ordering, op),
                None => {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Evaluation,
                        dialect: "OpenCypher",
                        feature: "ordered comparisons require numeric or matching string, date, or list values".to_string(),
                    });
                }
            }
        }
    })
}

#[cfg(feature = "opencypher")]
fn semantic_property_order(left: &VertexPropertyValue, right: &VertexPropertyValue) -> Option<std::cmp::Ordering> {
    numeric_property_order(left, right).or_else(|| match (left, right) {
        (VertexPropertyValue::String(left), VertexPropertyValue::String(right)) => Some(left.cmp(right)),
        (VertexPropertyValue::Date(left), VertexPropertyValue::Date(right)) => Some(left.cmp(right)),
        (VertexPropertyValue::List(left), VertexPropertyValue::List(right)) => Some(compare_property_lists(left, right)),
        _ => None,
    })
}

#[cfg(feature = "opencypher")]
pub(super) fn vertex_property_values_equal(left: &VertexPropertyValue, right: &VertexPropertyValue) -> bool {
    compare_vertex_property_values(left, RowComparisonOp::Eq, right).unwrap_or(false)
}

#[cfg(feature = "opencypher")]
fn property_list_contains(list: &VertexPropertyList, value: &VertexPropertyValue) -> bool {
    match list {
        VertexPropertyList::Empty => false,
        VertexPropertyList::Integer(values) => values.iter().any(|candidate| {
            let candidate = match candidate {
                VertexPropertyInteger::Unsigned(value) => VertexPropertyValue::Integer(*value),
                VertexPropertyInteger::Signed(value) => VertexPropertyValue::SignedInteger(*value),
            };
            vertex_property_values_equal(&candidate, value)
        }),
        VertexPropertyList::Bool(values) => matches!(value, VertexPropertyValue::Bool(value) if values.contains(value)),
        VertexPropertyList::Float(values) => values
            .iter()
            .any(|candidate| vertex_property_values_equal(&VertexPropertyValue::Float(*candidate), value)),
        VertexPropertyList::String(values) => matches!(value, VertexPropertyValue::String(value) if values.contains(value)),
        VertexPropertyList::Date(values) => matches!(value, VertexPropertyValue::Date(value) if values.contains(value)),
    }
}

#[cfg(feature = "opencypher")]
fn compare_ordering(ordering: std::cmp::Ordering, op: RowComparisonOp) -> bool {
    match op {
        RowComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
        RowComparisonOp::Ne => ordering != std::cmp::Ordering::Equal,
        RowComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        RowComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        RowComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        RowComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

#[cfg(feature = "opencypher")]
fn binding_property(row: &BindingRow, binding: &str, property: &str) -> Result<Option<VertexPropertyValue>> {
    if row.null_values.contains(binding) || row.null_relationships.contains(binding) {
        return Ok(None);
    }
    if row.values.contains_key(binding) && property == "id" {
        return Ok(Some(VertexPropertyValue::Integer(row.get(binding)?)));
    }
    if let Some(relationship) = row.relationships.get(binding) {
        if property == "id" {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "relationship id properties are not executable in Query engine".to_string(),
            });
        }
        let Some(metadata) = row.relationship_metadata.get(relationship) else {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: format!("metadata for relationship {binding} was not hydrated"),
            });
        };
        return Ok(metadata.properties.get(property).cloned());
    }
    let Some(metadata) = row.metadata.get(binding) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: format!("metadata for bound variable {binding} was not hydrated"),
        });
    };
    Ok(metadata.properties.get(property).cloned())
}

#[cfg(feature = "opencypher")]
fn project_binding_row(row: &BindingRow, projections: &[RowProjection]) -> Result<QueryRow> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        match projection {
            RowProjection::NodeId { binding } => {
                values.push(match row.get_optional(binding)? {
                    Some(value) => QueryValue::VertexId(value),
                    None => QueryValue::Null,
                });
            }
            RowProjection::Property { binding, property } => {
                values.push(match binding_property(row, binding, property)? {
                    Some(value) => QueryValue::Property(value),
                    None => QueryValue::Null,
                });
            }
            RowProjection::Literal(literal) => {
                values.push(literal.clone().map_or(QueryValue::Null, QueryValue::Property));
            }
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "OpenCypher",
                    feature: "aggregate projection must be planned as an aggregate".to_string(),
                });
            }
        }
    }
    Ok(QueryRow::new(values))
}

#[cfg(feature = "opencypher")]
fn project_relationship_row(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    metadata: &EdgeMetadata,
    projections: &[RowProjection],
) -> Result<QueryRow> {
    let mut values = Vec::with_capacity(projections.len());
    for projection in projections {
        values.push(match projection {
            RowProjection::NodeId { binding } => QueryValue::VertexId(relationship_endpoint_id(edge, relationship, binding)?),
            RowProjection::Property { binding, property } => {
                if edge.binding.as_deref() != Some(binding.as_str()) {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Return,
                        dialect: "OpenCypher",
                        feature: format!("relationship fast path cannot project property {binding}.{property}"),
                    });
                }
                if property == "id" {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::Return,
                        dialect: "OpenCypher",
                        feature: "relationship id properties are not executable in Query engine".to_string(),
                    });
                }
                metadata
                    .properties
                    .get(property)
                    .cloned()
                    .map(QueryValue::Property)
                    .unwrap_or(QueryValue::Null)
            }
            // Unreachable while the guards above reject literals on this
            // path, but the fast path can project a constant perfectly well.
            RowProjection::Literal(literal) => literal.clone().map_or(QueryValue::Null, QueryValue::Property),
            RowProjection::CountAll | RowProjection::Aggregate { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Other,
                    dialect: "OpenCypher",
                    feature: "aggregate projection must be planned as an aggregate".to_string(),
                });
            }
        });
    }
    Ok(QueryRow::new(values))
}

#[cfg(feature = "opencypher")]
fn relationship_rows_cache_value(rows: &[(BoundRelationship, EdgeMetadata)]) -> RelationshipRowsCacheValue {
    RelationshipRowsCacheValue::new(
        rows.iter()
            .map(|(relationship, metadata)| RelationshipRowsCacheEntry {
                relationship_id: relationship.relationship_id,
                metadata: metadata.clone(),
            })
            .collect(),
    )
}

#[cfg(feature = "opencypher")]
fn relationship_rows_from_cache_value(
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    cached: RelationshipRowsCacheValue,
) -> Vec<(BoundRelationship, EdgeMetadata)> {
    cached
        .rows
        .iter()
        .map(|entry| {
            (
                BoundRelationship {
                    edge_type: edge_type.to_string(),
                    src,
                    dst,
                    relationship_id: entry.relationship_id,
                },
                entry.metadata.clone(),
            )
        })
        .collect()
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_relationship_row(
    edge: &RowEdgePattern,
    relationship: &BoundRelationship,
    metadata: &EdgeMetadata,
    row: &QueryRow,
    query: &ParsedRowQuery,
) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(query.order_by.len());
    for sort in &query.order_by {
        keys.push(match &sort.expression {
            RowSortExpression::NodeId { binding } => QueryValue::VertexId(relationship_endpoint_id(edge, relationship, binding)?),
            RowSortExpression::Property { binding, property } => {
                if edge.binding.as_deref() != Some(binding.as_str()) {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::OrderWindow,
                        dialect: "OpenCypher",
                        feature: format!("relationship fast path cannot sort by property {binding}.{property}"),
                    });
                }
                metadata
                    .properties
                    .get(property)
                    .cloned()
                    .map(QueryValue::Property)
                    .unwrap_or(QueryValue::Null)
            }
            RowSortExpression::Column { name } => projected_column_value(row, &query.columns, name)?,
            RowSortExpression::CountAll => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "count(*) ORDER BY is only valid for aggregate rows".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn relationship_endpoint_id(edge: &RowEdgePattern, relationship: &BoundRelationship, binding: &str) -> Result<VertexId> {
    if edge.src.binding.as_deref() == Some(binding) {
        return Ok(relationship.src);
    }
    if edge.dst.binding.as_deref() == Some(binding) {
        return Ok(relationship.dst);
    }
    Err(GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Other,
        dialect: "OpenCypher",
        feature: format!("relationship fast path cannot project unbound node {binding}"),
    })
}

#[cfg(feature = "opencypher")]
fn row_projections_have_aggregates(projections: &[RowProjection]) -> bool {
    projections.iter().any(is_aggregate_projection)
}

#[cfg(feature = "opencypher")]
fn is_aggregate_projection(projection: &RowProjection) -> bool {
    matches!(projection, RowProjection::CountAll | RowProjection::Aggregate { .. })
}

#[cfg(feature = "opencypher")]
fn aggregate_projected_rows(
    bindings: Vec<BindingRow>,
    projections: &[RowProjection],
    columns: &[QueryColumn],
    order_by: &[RowSort],
    budget: &QueryBudget,
) -> Result<Vec<ProjectedQueryRow>> {
    let group_projection_indexes: Vec<_> = projections
        .iter()
        .enumerate()
        .filter_map(|(idx, projection)| (!is_aggregate_projection(projection)).then_some(idx))
        .collect();
    let aggregate_projection_indexes: Vec<_> = projections
        .iter()
        .enumerate()
        .filter_map(|(idx, projection)| is_aggregate_projection(projection).then_some(idx))
        .collect();

    let mut groups = BTreeMap::<Vec<SemanticQueryValue>, (Vec<QueryValue>, Vec<AggregateAccumulator>)>::new();
    if bindings.is_empty() && group_projection_indexes.is_empty() {
        groups.insert(
            Vec::new(),
            (
                Vec::new(),
                aggregate_projection_indexes
                    .iter()
                    .map(|idx| new_aggregate_accumulator(&projections[*idx]))
                    .collect::<Result<_>>()?,
            ),
        );
    }

    for binding in bindings {
        budget.check("cypher_aggregate_group")?;
        let mut group_key = Vec::with_capacity(group_projection_indexes.len());
        for idx in &group_projection_indexes {
            group_key.push(project_single_binding_value(&binding, &projections[*idx])?);
        }
        let semantic_group_key = group_key.iter().cloned().map(SemanticQueryValue).collect();
        let states = match groups.entry(semantic_group_key) {
            std::collections::btree_map::Entry::Occupied(entry) => &mut entry.into_mut().1,
            std::collections::btree_map::Entry::Vacant(entry) => {
                &mut entry
                    .insert((
                        group_key,
                        aggregate_projection_indexes
                            .iter()
                            .map(|idx| new_aggregate_accumulator(&projections[*idx]))
                            .collect::<Result<_>>()?,
                    ))
                    .1
            }
        };
        for (state_idx, projection_idx) in aggregate_projection_indexes.iter().enumerate() {
            apply_aggregate_projection(&mut states[state_idx], &projections[*projection_idx], &binding)?;
        }
    }

    let mut projected = Vec::with_capacity(groups.len());
    for (_, (group_key, states)) in groups {
        budget.check("cypher_aggregate_project")?;
        let mut group_idx = 0;
        let mut aggregate_idx = 0;
        let mut values = Vec::with_capacity(projections.len());
        for projection in projections {
            if is_aggregate_projection(projection) {
                values.push(finalize_aggregate(&states[aggregate_idx])?);
                aggregate_idx += 1;
            } else {
                values.push(group_key[group_idx].clone());
                group_idx += 1;
            }
        }
        let row = QueryRow::new(values);
        let sort_keys = sort_keys_for_projected_only(&row, columns, order_by)?;
        push_projected_query_row(&mut projected, row, sort_keys);
    }
    Ok(projected)
}

#[cfg(feature = "opencypher")]
fn new_aggregate_accumulator(projection: &RowProjection) -> Result<AggregateAccumulator> {
    Ok(match projection {
        RowProjection::CountAll => AggregateAccumulator::CountAll(0),
        RowProjection::Aggregate { function, .. } => match function {
            RowAggregateFunction::Count => AggregateAccumulator::CountExpression(0),
            RowAggregateFunction::Sum => AggregateAccumulator::Sum(0),
            RowAggregateFunction::Avg => AggregateAccumulator::Avg { sum: 0, count: 0 },
            RowAggregateFunction::Collect => AggregateAccumulator::Collect(Vec::new()),
        },
        _ => {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "non-aggregate projection cannot create aggregate state".to_string(),
            });
        }
    })
}

#[cfg(feature = "opencypher")]
fn apply_aggregate_projection(state: &mut AggregateAccumulator, projection: &RowProjection, row: &BindingRow) -> Result<()> {
    match (state, projection) {
        (AggregateAccumulator::CountAll(count), RowProjection::CountAll) => {
            *count = count.saturating_add(1);
        }
        (
            AggregateAccumulator::CountExpression(count),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Count,
                expression,
            },
        ) => {
            if expression_query_value(row, expression)?.is_some() {
                *count = count.saturating_add(1);
            }
        }
        (
            AggregateAccumulator::Sum(sum),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Sum,
                expression,
            },
        ) => {
            if let Some(value) = aggregate_integer_value(row, expression, "sum")? {
                *sum = sum.checked_add(u128::from(value)).ok_or_else(|| GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Evaluation,
                    dialect: "OpenCypher",
                    feature: "sum aggregate overflowed".to_string(),
                })?;
            }
        }
        (
            AggregateAccumulator::Avg { sum, count },
            RowProjection::Aggregate {
                function: RowAggregateFunction::Avg,
                expression,
            },
        ) => {
            if let Some(value) = aggregate_integer_value(row, expression, "avg")? {
                *sum = sum.checked_add(u128::from(value)).ok_or_else(|| GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::Evaluation,
                    dialect: "OpenCypher",
                    feature: "avg aggregate overflowed".to_string(),
                })?;
                *count = count.saturating_add(1);
            }
        }
        (
            AggregateAccumulator::Collect(values),
            RowProjection::Aggregate {
                function: RowAggregateFunction::Collect,
                expression,
            },
        ) => {
            if let Some(value) = expression_query_value(row, expression)? {
                values.push(value);
            }
        }
        _ => {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "OpenCypher",
                feature: "aggregate projection state mismatch".to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(feature = "opencypher")]
fn aggregate_integer_value(row: &BindingRow, expression: &RowExpression, function: &str) -> Result<Option<u64>> {
    match eval_row_expression(row, expression)? {
        RowScalarValue::Missing => Ok(None),
        RowScalarValue::Value(VertexPropertyValue::Integer(value)) => Ok(Some(value)),
        RowScalarValue::Value(_) => Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Evaluation,
            dialect: "OpenCypher",
            feature: format!("{function} aggregate requires integer values"),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn finalize_aggregate(state: &AggregateAccumulator) -> Result<QueryValue> {
    Ok(match state {
        AggregateAccumulator::CountAll(count) | AggregateAccumulator::CountExpression(count) => QueryValue::Count(*count),
        AggregateAccumulator::Sum(sum) => QueryValue::Property(VertexPropertyValue::Integer((*sum).try_into().map_err(|_| {
            GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Evaluation,
                dialect: "OpenCypher",
                feature: "sum aggregate exceeds u64 result range".to_string(),
            }
        })?)),
        AggregateAccumulator::Avg { sum: _, count: 0 } => QueryValue::Null,
        AggregateAccumulator::Avg { sum, count } => QueryValue::Float(QueryFloat(*sum as f64 / *count as f64)),
        AggregateAccumulator::Collect(values) => QueryValue::List(values.clone()),
    })
}

#[cfg(feature = "opencypher")]
fn project_single_binding_value(row: &BindingRow, projection: &RowProjection) -> Result<QueryValue> {
    match projection {
        RowProjection::NodeId { binding } => Ok(match row.get_optional(binding)? {
            Some(value) => QueryValue::VertexId(value),
            None => QueryValue::Null,
        }),
        RowProjection::Property { binding, property } => Ok(match binding_property(row, binding, property)? {
            Some(value) => QueryValue::Property(value),
            None => QueryValue::Null,
        }),
        // A constant is a legitimate group key: every row carries the same
        // value, so it partitions nothing and drops out of the grouping.
        RowProjection::Literal(literal) => Ok(literal.clone().map_or(QueryValue::Null, QueryValue::Property)),
        RowProjection::CountAll | RowProjection::Aggregate { .. } => Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::Other,
            dialect: "OpenCypher",
            feature: "aggregate projection cannot be used as a group key".to_string(),
        }),
    }
}

#[cfg(feature = "opencypher")]
fn expression_query_value(row: &BindingRow, expression: &RowExpression) -> Result<Option<QueryValue>> {
    Ok(match expression {
        RowExpression::NodeId { binding } => row.get_optional(binding)?.map(QueryValue::VertexId),
        RowExpression::Property { binding, property } => binding_property(row, binding, property)?.map(QueryValue::Property),
        RowExpression::Literal(value) => Some(QueryValue::Property(value.clone())),
        RowExpression::Null => None,
    })
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_row(binding_row: &BindingRow, row: &QueryRow, columns: &[QueryColumn], order_by: &[RowSort]) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(order_by.len());
    for sort in order_by {
        keys.push(match &sort.expression {
            RowSortExpression::NodeId { binding } => match binding_row.get_optional(binding)? {
                Some(value) => QueryValue::VertexId(value),
                None => QueryValue::Null,
            },
            RowSortExpression::Property { binding, property } => match binding_property(binding_row, binding, property)? {
                Some(value) => QueryValue::Property(value),
                None => QueryValue::Null,
            },
            RowSortExpression::Column { name } => projected_column_value(row, columns, name)?,
            RowSortExpression::CountAll => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "count(*) ORDER BY is only valid for aggregate rows".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn sort_keys_for_projected_only(row: &QueryRow, columns: &[QueryColumn], order_by: &[RowSort]) -> Result<Vec<QueryValue>> {
    let mut keys = Vec::with_capacity(order_by.len());
    for sort in order_by {
        keys.push(match &sort.expression {
            RowSortExpression::Column { name } => projected_column_value(row, columns, name)?,
            RowSortExpression::CountAll => {
                let Some(value) = row.values.iter().find(|value| matches!(value, QueryValue::Count(_))) else {
                    return Err(GraphError::UnsupportedQuery {
                        reason: QueryFailureReason::OrderWindow,
                        dialect: "OpenCypher",
                        feature: "count(*) ORDER BY requires an aggregate row".to_string(),
                    });
                };
                value.clone()
            }
            RowSortExpression::NodeId { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "aggregate ORDER BY cannot reference row variables".to_string(),
                });
            }
            RowSortExpression::Property { .. } => {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::OrderWindow,
                    dialect: "OpenCypher",
                    feature: "aggregate ORDER BY cannot reference row properties".to_string(),
                });
            }
        });
    }
    Ok(keys)
}

#[cfg(feature = "opencypher")]
fn projected_column_value(row: &QueryRow, columns: &[QueryColumn], name: &str) -> Result<QueryValue> {
    let Some(index) = columns.iter().position(|column| column.name == name) else {
        return Err(GraphError::UnsupportedQuery {
            reason: QueryFailureReason::OrderWindow,
            dialect: "OpenCypher",
            feature: format!("ORDER BY references unknown projection {name}"),
        });
    };
    row.values.get(index).cloned().ok_or_else(|| GraphError::UnsupportedQuery {
        reason: QueryFailureReason::OrderWindow,
        dialect: "OpenCypher",
        feature: format!("ORDER BY projection {name} has no row value"),
    })
}

#[cfg(feature = "opencypher")]
fn compare_projected_rows(left: &ProjectedQueryRow, right: &ProjectedQueryRow, order_by: &[RowSort]) -> std::cmp::Ordering {
    for (idx, sort) in order_by.iter().enumerate() {
        let ordering = compare_query_values(&left.sort_keys[idx], &right.sort_keys[idx]);
        let ordering = if sort.ascending { ordering } else { ordering.reverse() };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug)]
struct SemanticQueryValue(QueryValue);

#[cfg(feature = "opencypher")]
impl PartialEq for SemanticQueryValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

#[cfg(feature = "opencypher")]
impl Eq for SemanticQueryValue {}

#[cfg(feature = "opencypher")]
impl PartialOrd for SemanticQueryValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "opencypher")]
impl Ord for SemanticQueryValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_query_values(&self.0, &other.0)
    }
}

#[cfg(feature = "opencypher")]
fn compare_query_values(left: &QueryValue, right: &QueryValue) -> std::cmp::Ordering {
    match (left, right) {
        (QueryValue::Null, QueryValue::Null) => std::cmp::Ordering::Equal,
        (QueryValue::VertexId(left), QueryValue::VertexId(right))
        | (QueryValue::VertexId(left), QueryValue::Count(right))
        | (QueryValue::Count(left), QueryValue::VertexId(right))
        | (QueryValue::Count(left), QueryValue::Count(right)) => left.cmp(right),
        (QueryValue::Float(left), QueryValue::Float(right)) => left.cmp(right),
        (QueryValue::Property(left), QueryValue::Property(right)) => compare_vertex_property_order(left, right),
        (QueryValue::List(left), QueryValue::List(right)) => {
            for (left, right) in left.iter().zip(right.iter()) {
                let ordering = compare_query_values(left, right);
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        }
        (QueryValue::Path(left), QueryValue::Path(right)) => left.cmp(right),
        (QueryValue::Bool(left), QueryValue::Bool(right)) => left.cmp(right),
        (QueryValue::GraphValue(left), QueryValue::GraphValue(right)) => left.cmp(right),
        _ => query_value_rank(left).cmp(&query_value_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn compare_vertex_property_order(left: &VertexPropertyValue, right: &VertexPropertyValue) -> std::cmp::Ordering {
    if let Some(ordering) = semantic_property_order(left, right) {
        return ordering;
    }
    match (left, right) {
        (VertexPropertyValue::Bool(left), VertexPropertyValue::Bool(right)) => left.cmp(right),
        _ => vertex_property_rank(left).cmp(&vertex_property_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn compare_property_lists(left: &crate::VertexPropertyList, right: &crate::VertexPropertyList) -> std::cmp::Ordering {
    use crate::VertexPropertyList as List;

    match (left.is_empty(), right.is_empty()) {
        (true, true) => return std::cmp::Ordering::Equal,
        (true, false) => return std::cmp::Ordering::Less,
        (false, true) => return std::cmp::Ordering::Greater,
        (false, false) => {}
    }

    match (left, right) {
        (List::Empty, List::Empty) => std::cmp::Ordering::Equal,
        (List::Empty, _) => std::cmp::Ordering::Less,
        (_, List::Empty) => std::cmp::Ordering::Greater,
        (List::Integer(left), List::Integer(right)) => compare_slices(left, right, compare_property_integers),
        (List::Bool(left), List::Bool(right)) => compare_slices(left, right, bool::cmp),
        (List::Float(left), List::Float(right)) => compare_slices(left, right, |left, right| compare_f64_numeric(left.0, right.0)),
        (List::String(left), List::String(right)) => compare_slices(left, right, String::cmp),
        (List::Date(left), List::Date(right)) => compare_slices(left, right, i32::cmp),
        (List::Integer(left), List::Float(right)) => compare_numeric_list_slices(left, right),
        (List::Float(left), List::Integer(right)) => compare_numeric_list_slices(right, left).reverse(),
        _ => property_list_rank(left).cmp(&property_list_rank(right)),
    }
}

#[cfg(feature = "opencypher")]
fn compare_slices<T, U>(left: &[T], right: &[U], compare: impl Fn(&T, &U) -> std::cmp::Ordering) -> std::cmp::Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = compare(left, right);
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

#[cfg(feature = "opencypher")]
fn compare_property_integers(left: &crate::VertexPropertyInteger, right: &crate::VertexPropertyInteger) -> std::cmp::Ordering {
    use crate::VertexPropertyInteger as Integer;

    match (left, right) {
        (Integer::Unsigned(left), Integer::Unsigned(right)) => left.cmp(right),
        (Integer::Signed(left), Integer::Signed(right)) => left.cmp(right),
        (Integer::Unsigned(_), Integer::Signed(right)) if *right < 0 => std::cmp::Ordering::Greater,
        (Integer::Unsigned(left), Integer::Signed(right)) => left.cmp(&(*right as u64)),
        (Integer::Signed(left), Integer::Unsigned(_)) if *left < 0 => std::cmp::Ordering::Less,
        (Integer::Signed(left), Integer::Unsigned(right)) => (*left as u64).cmp(right),
    }
}

#[cfg(feature = "opencypher")]
fn compare_numeric_list_slices(left: &[crate::VertexPropertyInteger], right: &[QueryFloat]) -> std::cmp::Ordering {
    compare_slices(left, right, |left, right| {
        let left = match left {
            crate::VertexPropertyInteger::Unsigned(value) => VertexPropertyValue::Integer(*value),
            crate::VertexPropertyInteger::Signed(value) => VertexPropertyValue::SignedInteger(*value),
        };
        numeric_property_order(&left, &VertexPropertyValue::Float(*right)).expect("integer and float are numeric")
    })
}

#[cfg(feature = "opencypher")]
fn property_list_rank(value: &crate::VertexPropertyList) -> u8 {
    match value {
        crate::VertexPropertyList::Empty => 0,
        crate::VertexPropertyList::Bool(_) => 1,
        crate::VertexPropertyList::Integer(_) | crate::VertexPropertyList::Float(_) => 2,
        crate::VertexPropertyList::String(_) => 3,
        crate::VertexPropertyList::Date(_) => 4,
    }
}

#[cfg(feature = "opencypher")]
fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::Bool(_) => 1,
        QueryValue::VertexId(_) | QueryValue::Count(_) => 2,
        QueryValue::Float(_) => 3,
        QueryValue::Property(value) => 4 + vertex_property_rank(value),
        QueryValue::List(_) => 8,
        QueryValue::Path(_) => 9,
        QueryValue::Record(_) => 10,
        QueryValue::Temporal(_) => 11,
        QueryValue::Decimal(_) => 12,
        QueryValue::Bytes(_) => 13,
        QueryValue::GraphValue(_) => 14,
        QueryValue::Null => u8::MAX,
    }
}

#[cfg(feature = "opencypher")]
fn vertex_property_rank(value: &VertexPropertyValue) -> u8 {
    match value {
        VertexPropertyValue::Bool(_) => 0,
        VertexPropertyValue::Integer(_) | VertexPropertyValue::SignedInteger(_) | VertexPropertyValue::Float(_) => 1,
        VertexPropertyValue::String(_) => 2,
        VertexPropertyValue::Date(_) => 3,
        VertexPropertyValue::List(_) => 4,
    }
}

#[cfg(feature = "opencypher")]
pub(crate) fn equivalent_property_index_keys(value: &VertexPropertyValue) -> Vec<String> {
    if !value.is_scalar_indexable() {
        return Vec::new();
    }
    let mut keys = BTreeSet::new();
    keys.insert(encode_vertex_property_value_key(value));
    match value {
        VertexPropertyValue::Integer(value) => {
            if let Ok(signed) = i64::try_from(*value) {
                keys.insert(encode_vertex_property_value_key(&VertexPropertyValue::SignedInteger(signed)));
            }
            if let Some(float) = VertexPropertyValue::exact_f64_from_u64(*value) {
                insert_float_property_index_key(&mut keys, float);
                if *value == 0 {
                    insert_float_property_index_key(&mut keys, -0.0);
                }
            }
        }
        VertexPropertyValue::SignedInteger(value) => {
            if let Ok(unsigned) = u64::try_from(*value) {
                keys.insert(encode_vertex_property_value_key(&VertexPropertyValue::Integer(unsigned)));
            }
            if let Some(float) = VertexPropertyValue::exact_f64_from_i64(*value) {
                insert_float_property_index_key(&mut keys, float);
                if *value == 0 {
                    insert_float_property_index_key(&mut keys, -0.0);
                }
            }
        }
        VertexPropertyValue::Float(value) => {
            if value.0 == 0.0 {
                insert_float_property_index_key(&mut keys, 0.0);
                insert_float_property_index_key(&mut keys, -0.0);
            }
            if let Some(integer) = VertexPropertyValue::exact_u64_from_f64(value.0) {
                keys.insert(encode_vertex_property_value_key(&VertexPropertyValue::Integer(integer)));
            }
            if let Some(integer) = VertexPropertyValue::exact_i64_from_f64(value.0) {
                keys.insert(encode_vertex_property_value_key(&VertexPropertyValue::SignedInteger(integer)));
            }
        }
        VertexPropertyValue::Bool(_) | VertexPropertyValue::String(_) | VertexPropertyValue::Date(_) | VertexPropertyValue::List(_) => {}
    }
    keys.into_iter().collect()
}

#[cfg(feature = "opencypher")]
fn insert_float_property_index_key(keys: &mut BTreeSet<String>, value: f64) {
    keys.insert(encode_vertex_property_value_key(&VertexPropertyValue::Float(QueryFloat(value))));
}

#[cfg(feature = "opencypher")]
fn numeric_property_order(left: &VertexPropertyValue, right: &VertexPropertyValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Integer(right)) => Some(left.cmp(right)),
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::SignedInteger(right)) => Some(left.cmp(right)),
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::Integer(right)) => Some(compare_i64_u64(*left, *right)),
        (VertexPropertyValue::Integer(left), VertexPropertyValue::SignedInteger(right)) => Some(compare_i64_u64(*right, *left).reverse()),
        (VertexPropertyValue::Float(left), VertexPropertyValue::Float(right)) => Some(compare_f64_numeric(left.0, right.0)),
        (VertexPropertyValue::Integer(left), VertexPropertyValue::Float(right)) => Some(compare_u64_f64(*left, right.0)),
        (VertexPropertyValue::Float(left), VertexPropertyValue::Integer(right)) => Some(compare_u64_f64(*right, left.0).reverse()),
        (VertexPropertyValue::SignedInteger(left), VertexPropertyValue::Float(right)) => Some(compare_i64_f64(*left, right.0)),
        (VertexPropertyValue::Float(left), VertexPropertyValue::SignedInteger(right)) => Some(compare_i64_f64(*right, left.0).reverse()),
        _ => None,
    }
}

#[cfg(feature = "opencypher")]
fn compare_i64_u64(left: i64, right: u64) -> std::cmp::Ordering {
    match u64::try_from(left) {
        Ok(left) => left.cmp(&right),
        Err(_) => std::cmp::Ordering::Less,
    }
}

#[cfg(feature = "opencypher")]
fn compare_i64_f64(left: i64, right: f64) -> std::cmp::Ordering {
    const I64_INCLUSIVE_LOWER: f64 = -9223372036854775808.0;
    const I64_EXCLUSIVE_UPPER: f64 = 9223372036854775808.0;
    if right.is_nan() {
        return (left as f64).total_cmp(&right);
    }
    if right < I64_INCLUSIVE_LOWER {
        return std::cmp::Ordering::Greater;
    }
    if right >= I64_EXCLUSIVE_UPPER {
        return std::cmp::Ordering::Less;
    }
    let floor = right.floor();
    let floor_integer = floor as i64;
    match left.cmp(&floor_integer) {
        std::cmp::Ordering::Equal if floor == right => std::cmp::Ordering::Equal,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
        ordering => ordering,
    }
}

#[cfg(feature = "opencypher")]
fn compare_f64_numeric(left: f64, right: f64) -> std::cmp::Ordering {
    if left == right {
        std::cmp::Ordering::Equal
    } else {
        left.total_cmp(&right)
    }
}

#[cfg(feature = "opencypher")]
fn compare_u64_f64(left: u64, right: f64) -> std::cmp::Ordering {
    const U64_EXCLUSIVE_UPPER: f64 = 18446744073709551616.0;
    if right.is_nan() {
        return (left as f64).total_cmp(&right);
    }
    if right < 0.0 {
        return std::cmp::Ordering::Greater;
    }
    if right >= U64_EXCLUSIVE_UPPER {
        return std::cmp::Ordering::Less;
    }
    let floor = right.floor();
    let floor_integer = floor as u64;
    match left.cmp(&floor_integer) {
        std::cmp::Ordering::Equal if floor == right => std::cmp::Ordering::Equal,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
        ordering => ordering,
    }
}

/// One pair's relationship rows as native paths read them.
#[cfg(feature = "opencypher")]
pub(crate) type NativePathRows = Vec<(Option<RelationshipId>, EdgeMetadata)>;

/// What a relationship read knows about its endpoint pair's structural edge.
#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EdgePresence {
    /// Nothing: the read checks that the edge exists before answering.
    Unverified,
    /// Listed from the adjacency of the snapshot being read, at the epoch
    /// read, so it exists there.
    Listed,
}

/// A pair's relationships as native paths see them: a structural edge that
/// carries properties lends them to every record of the pair, and the pair's
/// rows are ordered by relationship id.
#[cfg(feature = "opencypher")]
fn native_path_rows(mut relationships: Vec<(BoundRelationship, EdgeMetadata)>) -> NativePathRows {
    let structural_metadata = relationships
        .iter()
        .find_map(|(relationship, metadata)| relationship.relationship_id.is_none().then(|| metadata.clone()));
    if let Some(structural_metadata) = structural_metadata {
        if relationships.iter().any(|(relationship, _)| relationship.relationship_id.is_some()) {
            relationships = relationships
                .into_iter()
                .filter_map(|(relationship, metadata)| {
                    relationship.relationship_id.map(|_| {
                        let mut merged = structural_metadata.clone();
                        merged.properties.extend(metadata.properties);
                        (relationship, merged)
                    })
                })
                .collect();
        }
    }
    relationships.sort_by_key(|(relationship, _)| relationship.relationship_id);
    relationships
        .into_iter()
        .map(|(relationship, metadata)| (relationship.relationship_id, metadata))
        .collect()
}

/// The rows `relationships_for_edge_at` builds for an existing pair at the
/// latest epoch, from its live records and its structural metadata: one row
/// per record, plus the structural edge itself when it has no records or
/// carries properties of its own.
#[cfg(feature = "opencypher")]
fn relationship_rows_for_edge(
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    records: Vec<RelationshipRecord>,
    structural_metadata: EdgeMetadata,
) -> Vec<(BoundRelationship, EdgeMetadata)> {
    let saw_live_relationship_record = !records.is_empty();
    let mut rows = records
        .into_iter()
        .map(|record| {
            (
                BoundRelationship {
                    edge_type: record.edge_type,
                    src: record.src,
                    dst: record.dst,
                    relationship_id: Some(record.relationship_id),
                },
                record.metadata,
            )
        })
        .collect::<Vec<_>>();
    if !saw_live_relationship_record || !structural_metadata.properties.is_empty() {
        rows.push((
            BoundRelationship {
                edge_type: edge_type.to_string(),
                src,
                dst,
                relationship_id: None,
            },
            structural_metadata,
        ));
    }
    rows
}
