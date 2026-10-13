use super::*;
use futures::StreamExt as _;
use slatedb::ValueDeletable;

#[derive(Clone, Debug, Default)]
pub(crate) struct GraphTopologyOverlay {
    states: BTreeMap<VertexId, BTreeMap<VertexId, bool>>,
}

pub(crate) enum GraphTopologyTail {
    Complete(GraphTopologyOverlay),
    Unavailable,
}

impl GraphTopologyOverlay {
    pub(crate) fn set(&mut self, src: VertexId, dst: VertexId, exists: bool) {
        self.states.entry(src).or_default().insert(dst, exists);
    }

    /// Flat iteration over every `(src, dst, exists)` delta in the overlay.
    ///
    /// The read path consumes the overlay through per-source lookups
    /// (`expand_range_with_overlay` below); the incremental index builder in
    /// `src/engine/index_store.rs` instead needs to walk every delta once to
    /// patch a decoded CSC adjacency, which is what this exposes.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (VertexId, VertexId, bool)> + '_ {
        self.states.iter().flat_map(|(src, destinations)| {
            destinations
                .iter()
                .map(move |(dst, exists)| (*src, *dst, *exists))
        })
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn apply_in_neighbors(
        &self,
        dst: VertexId,
        neighbors: &mut BTreeSet<VertexId>,
    ) -> u64 {
        for (src, destinations) in &self.states {
            let Some(exists) = destinations.get(&dst) else {
                continue;
            };
            if *exists {
                neighbors.insert(*src);
            } else {
                neighbors.remove(src);
            }
        }
        u64::try_from(self.states.len()).unwrap_or(u64::MAX)
    }

    pub(crate) fn apply_out_neighbors(
        &self,
        src: VertexId,
        neighbors: &mut BTreeSet<VertexId>,
    ) -> u64 {
        let Some(destinations) = self.states.get(&src) else {
            return 0;
        };
        for (dst, exists) in destinations {
            if *exists {
                neighbors.insert(*dst);
            } else {
                neighbors.remove(dst);
            }
        }
        u64::try_from(destinations.len()).unwrap_or(u64::MAX)
    }

    /// Test-only introspection. The WAL-tail overlay is otherwise only
    /// observable through a compiled GraphBLAS traversal, which needs the
    /// SuiteSparse C kernel; these let a repro assert on the overlay directly.
    #[cfg(test)]
    pub(crate) fn test_state(&self, src: VertexId, dst: VertexId) -> Option<bool> {
        self.states
            .get(&src)
            .and_then(|destinations| destinations.get(&dst))
            .copied()
    }

    #[cfg(test)]
    pub(crate) fn test_len(&self) -> usize {
        self.states.values().map(BTreeMap::len).sum()
    }
}

/// One topology-affecting WAL record, parsed once per file.
///
/// Parsed for *every* cell and edge type in the file, not just the caller's:
/// all edge types share one database, so a per-caller parse would re-download
/// and re-parse the same file once per edge type. Callers filter by
/// `cell_id`/`edge_type`/`seq` at use time. Corruption is retained as an entry
/// owned by the record's cell and edge type, then raised only if that owner is
/// requested. A malformed REPLIES record must not break a CHAIN build that
/// happens to share its WAL file.
#[derive(Clone, Debug)]
pub(crate) struct WalTopologyEntry {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) seq: u64,
    decoded: WalTopologyEntryDecoded,
}

#[derive(Clone, Debug)]
enum WalTopologyEntryDecoded {
    Edge { src: VertexId, dst: VertexId },
    Corrupt { key: String, reason: String },
}

impl WalTopologyEntry {
    fn affected_edge_for(
        &self,
        generation: &crate::GraphIndexGeneration,
        read_sequence: StorageSequence,
    ) -> Result<Option<(VertexId, VertexId)>> {
        if self.seq <= generation.base_sequence
            || self.seq > read_sequence
            || self.cell_id != generation.cell_id
            || self.edge_type != generation.edge_type
        {
            return Ok(None);
        }
        match &self.decoded {
            WalTopologyEntryDecoded::Edge { src, dst } => Ok(Some((*src, *dst))),
            WalTopologyEntryDecoded::Corrupt { key, reason } => Err(GraphError::CorruptValue {
                key: key.clone(),
                reason: reason.clone(),
            }),
        }
    }
}

/// Per-shard cache of parsed WAL files, keyed by WAL id.
///
/// A WAL id names an immutable object, so a cached parse never goes stale —
/// eviction exists only to bound memory. Eviction drops the *smallest* id
/// first: tail walks always move forward, so the oldest file is the least
/// likely to be asked for again.
#[derive(Default)]
pub(crate) struct WalTailFileCache {
    files: BTreeMap<u64, Arc<Vec<WalTopologyEntry>>>,
    total_entries: usize,
}

/// Memory bound for [`WalTailFileCache`], in parsed entries (an entry is two
/// small strings and three integers — the cap is single-digit MiB per shard).
const WAL_TAIL_CACHE_MAX_ENTRIES: usize = 65_536;

/// Concurrent object-store fetches per tail walk. Each WAL file is a tiny
/// object whose cost is dominated by request latency, so the walk is
/// round-trip-bound and parallel fetches divide wall clock almost linearly.
pub(crate) const WAL_TAIL_FETCH_CONCURRENCY: usize = 16;

/// `GRAPH_WAL_TAIL_FETCH_CONCURRENCY` overrides the default, for tuning and
/// for A/B benchmarks (`1` reproduces the serial walk this replaced).
/// Resolved once, on first use, so `std::env::var` stays off the tail path —
/// the same shape as `env_default_kernel` in `sparse_kernel/mod.rs`.
fn wal_tail_fetch_concurrency() -> usize {
    static RESOLVED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| {
        std::env::var("GRAPH_WAL_TAIL_FETCH_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value >= 1)
            .unwrap_or(WAL_TAIL_FETCH_CONCURRENCY)
    })
}

impl WalTailFileCache {
    fn get(&self, wal_id: u64) -> Option<Arc<Vec<WalTopologyEntry>>> {
        self.files.get(&wal_id).cloned()
    }

    fn insert(&mut self, wal_id: u64, entries: Arc<Vec<WalTopologyEntry>>) {
        // A single file larger than the whole budget would evict everything
        // and then exceed the cap anyway; serve it uncached instead.
        if entries.len() > WAL_TAIL_CACHE_MAX_ENTRIES {
            return;
        }
        if let Some(previous) = self.files.insert(wal_id, entries) {
            self.total_entries = self.total_entries.saturating_sub(previous.len());
        }
        self.total_entries = self.total_entries.saturating_add(self.files[&wal_id].len());
        while self.total_entries > WAL_TAIL_CACHE_MAX_ENTRIES {
            let Some((_, evicted)) = self.files.pop_first() else {
                break;
            };
            self.total_entries = self.total_entries.saturating_sub(evicted.len());
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_file_count(&self) -> usize {
        self.files.len()
    }
}

/// A fetched file is either parsed or gone; a hard error is neither and
/// propagates through `Result` instead.
enum WalFileFetch {
    Parsed(Vec<WalTopologyEntry>),
    Unavailable,
}

impl GraphShard {
    pub(crate) async fn topology_tail_since(
        &self,
        generation: &crate::GraphIndexGeneration,
        snapshot: &GraphStorageSnapshot,
        read_sequence: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<GraphTopologyTail> {
        if snapshot.seq() != read_sequence {
            return Ok(GraphTopologyTail::Unavailable);
        }
        if generation.base_sequence >= read_sequence {
            return Ok(GraphTopologyTail::Complete(GraphTopologyOverlay::default()));
        }
        let last_wal_id = self.db.last_durable_wal_id().await?;
        if generation.last_wal_id >= last_wal_id {
            return Ok(GraphTopologyTail::Complete(GraphTopologyOverlay::default()));
        }

        // The cost gate. The span counts object-store round trips, and it
        // scales with write activity since the generation — not with the
        // size of the delta, which may be a handful of edges. Past the cap,
        // declining is cheaper than walking: the index build falls back to
        // a full rebuild and the read path to snapshot adjacency.
        let span = last_wal_id.saturating_sub(generation.last_wal_id);
        if span > self.limits.max_wal_tail_files {
            tracing::debug!(
                span,
                limit = self.limits.max_wal_tail_files,
                base_wal_id = generation.last_wal_id,
                last_wal_id,
                "graph index WAL tail span exceeds the file cap; declining"
            );
            return Ok(GraphTopologyTail::Unavailable);
        }

        let wal_ids =
            (generation.last_wal_id.saturating_add(1)..=last_wal_id).collect::<Vec<u64>>();
        let mut files = BTreeMap::new();
        let mut missing = Vec::new();
        {
            let cache = self.wal_tail_file_cache.lock().await;
            for wal_id in &wal_ids {
                match cache.get(*wal_id) {
                    Some(parsed) => {
                        files.insert(*wal_id, parsed);
                    }
                    None => missing.push(*wal_id),
                }
            }
        }

        {
            let fetches = futures::stream::iter(missing.iter().copied())
                .map(|wal_id| {
                    let wal_reader = self.db.wal_reader();
                    async move {
                        budget.check("graph_index_wal_file")?;
                        let mut entries = match wal_reader.get(wal_id).iterator().await {
                            Ok(entries) => entries,
                            Err(error) => {
                                tracing::debug!(
                                    wal_id,
                                    error = %error,
                                    "graph index WAL tail is unavailable; using snapshot adjacency"
                                );
                                return Ok((wal_id, WalFileFetch::Unavailable));
                            }
                        };
                        let mut parsed = Vec::new();
                        while let Some(entry) = entries.next().await? {
                            collect_wal_topology_entry(
                                &entry.key,
                                &entry.value,
                                entry.seq,
                                &mut parsed,
                            );
                        }
                        Ok::<_, crate::GraphError>((wal_id, WalFileFetch::Parsed(parsed)))
                    }
                })
                .buffered(wal_tail_fetch_concurrency());
            let mut fetches = std::pin::pin!(fetches);
            while let Some(fetched) = fetches.next().await {
                let (wal_id, fetch) = fetched?;
                match fetch {
                    WalFileFetch::Unavailable => return Ok(GraphTopologyTail::Unavailable),
                    WalFileFetch::Parsed(parsed) => {
                        files.insert(wal_id, Arc::new(parsed));
                    }
                }
            }
        }

        {
            let mut cache = self.wal_tail_file_cache.lock().await;
            for wal_id in &missing {
                if let Some(parsed) = files.get(wal_id) {
                    cache.insert(*wal_id, Arc::clone(parsed));
                }
            }
        }

        let mut affected = BTreeSet::new();
        for parsed in files.values() {
            for entry in parsed.iter() {
                budget.check("graph_index_wal_entry")?;
                let Some(edge) = entry.affected_edge_for(generation, read_sequence)? else {
                    continue;
                };
                affected.insert(edge);
                ensure_limit(
                    "graph_index_wal_affected_edges",
                    affected.len() as u64,
                    self.limits.max_query_scan_edges,
                )?;
            }
        }

        // Resolving each affected pair is a point read against the snapshot —
        // a cold block is an object-store round trip, so this loop is
        // request-bound exactly like the file walk above and parallelized the
        // same way. Reads against a pinned snapshot commute; the overlay is
        // keyed by (src, dst), so arrival order cannot change the result.
        let mut overlay = GraphTopologyOverlay::default();
        {
            let resolves = futures::stream::iter(affected.iter().copied())
                .map(|(src, dst)| async move {
                    budget.check("graph_index_wal_resolve_edge")?;
                    let exists = self
                        .edge_exists_in_storage_snapshot(
                            snapshot,
                            &generation.cell_id,
                            &generation.edge_type,
                            src,
                            dst,
                            read_sequence,
                        )
                        .await?;
                    Ok::<_, crate::GraphError>((src, dst, exists))
                })
                .buffered(wal_tail_fetch_concurrency());
            let mut resolves = std::pin::pin!(resolves);
            while let Some(resolved) = resolves.next().await {
                let (src, dst, exists) = resolved?;
                overlay.set(src, dst, exists);
            }
        }
        Ok(GraphTopologyTail::Complete(overlay))
    }

    #[cfg(test)]
    pub(crate) async fn test_wal_tail_cached_file_count(&self) -> usize {
        self.wal_tail_file_cache.lock().await.cached_file_count()
    }
}

fn collect_wal_topology_entry(
    key: &[u8],
    value: &ValueDeletable,
    seq: u64,
    parsed: &mut Vec<WalTopologyEntry>,
) {
    let Ok(key) = std::str::from_utf8(key) else {
        return;
    };
    let fields = key.split('/').collect::<Vec<_>>();
    match fields.as_slice() {
        ["cell", cell, "e", "out", edge_type, src, dst] => {
            push_wal_topology_edge(parsed, cell, edge_type, key, src, dst, seq);
        }
        ["cell", cell, "seg", "tomb", "out", edge_type, src, dst] => {
            push_wal_topology_edge(parsed, cell, edge_type, key, src, dst, seq);
        }
        ["cell", cell, "seg", "out", edge_type, _, _, _] => {
            if let Some(value) = value.as_bytes() {
                match decode_out_edge_segment(key, &value) {
                    Ok(segment) => parsed.extend(segment.destinations.into_iter().map(|dst| {
                        WalTopologyEntry {
                            cell_id: (*cell).to_string(),
                            edge_type: (*edge_type).to_string(),
                            seq,
                            decoded: WalTopologyEntryDecoded::Edge {
                                src: segment.src,
                                dst,
                            },
                        }
                    })),
                    Err(error) => {
                        parsed.push(corrupt_wal_topology_entry(cell, edge_type, key, seq, error))
                    }
                }
            }
        }
        _ => {}
    }
}

fn push_wal_topology_edge(
    parsed: &mut Vec<WalTopologyEntry>,
    cell_id: &str,
    edge_type: &str,
    key: &str,
    src: &str,
    dst: &str,
    seq: u64,
) {
    let decoded =
        parse_u64(key, src, "src").and_then(|src| parse_u64(key, dst, "dst").map(|dst| (src, dst)));
    parsed.push(match decoded {
        Ok((src, dst)) => WalTopologyEntry {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            seq,
            decoded: WalTopologyEntryDecoded::Edge { src, dst },
        },
        Err(error) => corrupt_wal_topology_entry(cell_id, edge_type, key, seq, error),
    });
}

fn corrupt_wal_topology_entry(
    cell_id: &str,
    edge_type: &str,
    fallback_key: &str,
    seq: u64,
    error: GraphError,
) -> WalTopologyEntry {
    let (key, reason) = match error {
        GraphError::CorruptValue { key, reason } => (key, reason),
        error => (fallback_key.to_string(), error.to_string()),
    };
    WalTopologyEntry {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        seq,
        decoded: WalTopologyEntryDecoded::Corrupt { key, reason },
    }
}

pub(crate) fn expand_range_with_overlay(
    compiled: &crate::sparse_kernel::CompiledGraphBlasMatrix,
    overlay: &GraphTopologyOverlay,
    starts: &[VertexId],
    min_hops: u8,
    max_hops: u8,
) -> Result<crate::sparse_kernel::SparseTraversal> {
    let mut frontier = starts.iter().copied().collect::<BTreeSet<_>>();
    let mut reachable = BTreeSet::new();
    let mut edge_visits = 0_u64;

    for hop in 1..=max_hops {
        if frontier.is_empty() {
            break;
        }
        // Expand each frontier vertex on its own and union the results, as
        // `expand_range_compiled_graphblas` does. A single kernel call over the
        // whole frontier treats the frontier as already seen, which drops any
        // edge whose endpoints are both frontier members (#69).
        let mut next = BTreeSet::new();
        for src in &frontier {
            let mut neighbors =
                crate::sparse_kernel::compiled_graphblas_out_neighbors(compiled, *src)
                    .into_iter()
                    .collect::<BTreeSet<_>>();
            edge_visits = edge_visits.saturating_add(neighbors.len() as u64);
            edge_visits =
                edge_visits.saturating_add(overlay.apply_out_neighbors(*src, &mut neighbors));
            next.extend(neighbors);
        }

        if hop >= min_hops {
            reachable.extend(next.iter().copied());
        }
        frontier = next;
    }

    Ok(crate::sparse_kernel::SparseTraversal {
        vertices: reachable.into_iter().collect(),
        edge_visits,
        // The overlay walk runs on whichever compiled kernel the matrix baked in.
        backend: crate::sparse_kernel::compiled_graphblas_kernel(compiled),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "opencypher")]
    #[test]
    fn neighbor_application_reports_overlay_scan_work() {
        let mut overlay = GraphTopologyOverlay::default();
        overlay.set(1, 10, true);
        overlay.set(1, 11, false);
        overlay.set(2, 10, true);
        overlay.set(3, 99, true);

        let mut outgoing = BTreeSet::from([11]);
        assert_eq!(overlay.apply_out_neighbors(1, &mut outgoing), 2);
        assert_eq!(outgoing, BTreeSet::from([10]));

        let mut incoming = BTreeSet::new();
        assert_eq!(overlay.apply_in_neighbors(10, &mut incoming), 3);
        assert_eq!(incoming, BTreeSet::from([1, 2]));
    }

    /// Issue #69: an edge whose endpoints are both in the current frontier
    /// must still be followed. Graph: 0->2, 0->3, 3->2, 2->9. After hop 1 the
    /// frontier is {2, 3}; 2 is reachable in exactly 2 hops only via 3->2.
    #[test]
    fn an_edge_between_two_frontier_members_is_not_lost() {
        use crate::sparse_kernel::{
            compile_graphblas_matrix, expand_range_compiled_graphblas, Adjacency,
            SparseKernelBackend,
        };
        let adjacency: Adjacency = BTreeMap::from([
            (0, BTreeSet::from([2, 3])),
            (2, BTreeSet::from([9])),
            (3, BTreeSet::from([2])),
        ]);
        // A non-empty overlay touching an unrelated edge: the normal state
        // between indexer cycles, which routes queries to the overlay path.
        let mut overlay = GraphTopologyOverlay::default();
        overlay.set(100, 101, true);

        for backend in [
            SparseKernelBackend::CompactCsc,
            SparseKernelBackend::SuiteSparse,
        ] {
            let compiled = compile_graphblas_matrix(&adjacency, backend).expect("compile");
            for (min, max) in [(2, 2), (3, 3), (1, 3)] {
                let without =
                    expand_range_compiled_graphblas(&compiled, &adjacency, &[0], min, max)
                        .expect("compiled range")
                        .vertices
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                let with = expand_range_with_overlay(&compiled, &overlay, &[0], min, max)
                    .expect("overlay range")
                    .vertices
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                assert_eq!(with, without, "backend {backend:?}, *{min}..{max}");
            }
        }
    }

    /// Index + overlay must answer exactly like an index compiled from the
    /// graph with the overlay already applied, for inserts and deletes alike.
    #[test]
    fn overlay_range_matches_compiled_range_on_random_graphs() {
        use crate::sparse_kernel::{
            compile_graphblas_matrix, expand_range_compiled_graphblas, Adjacency,
            SparseKernelBackend,
        };
        // Small deterministic LCG so the corpus is reproducible without a dev-dependency.
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move |bound: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (state >> 33) % bound
        };

        for case in 0..200 {
            let vertices = 2 + next(10);
            let mut base: Adjacency = BTreeMap::new();
            for _ in 0..next(vertices * 3) {
                base.entry(next(vertices))
                    .or_default()
                    .insert(next(vertices));
            }
            let mut overlay = GraphTopologyOverlay::default();
            let mut merged = base.clone();
            for _ in 0..next(6) {
                let (src, dst, exists) = (next(vertices), next(vertices), next(2) == 0);
                overlay.set(src, dst, exists);
                if exists {
                    merged.entry(src).or_default().insert(dst);
                } else if let Some(out) = merged.get_mut(&src) {
                    out.remove(&dst);
                }
            }
            merged.retain(|_, out| !out.is_empty());

            for backend in [
                SparseKernelBackend::CompactCsc,
                SparseKernelBackend::SuiteSparse,
            ] {
                let base_index = compile_graphblas_matrix(&base, backend).expect("compile base");
                let merged_index =
                    compile_graphblas_matrix(&merged, backend).expect("compile merged");
                let start = next(vertices);
                // min_hops >= 1: zero-hop ranges are out of scope for #69.
                let min = 1 + u8::try_from(next(3)).expect("small");
                let max = min + u8::try_from(next(3)).expect("small");
                let expected =
                    expand_range_compiled_graphblas(&merged_index, &merged, &[start], min, max)
                        .expect("compiled range")
                        .vertices
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                let actual = expand_range_with_overlay(&base_index, &overlay, &[start], min, max)
                    .expect("overlay range")
                    .vertices
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                assert_eq!(
                    actual, expected,
                    "case {case}, backend {backend:?}, start {start}, *{min}..{max}, base {base:?}, overlay {:?}",
                    overlay.states
                );
            }
        }
    }

    #[test]
    fn malformed_wal_topology_isolated_to_its_cell_and_edge_type() {
        let mut parsed = Vec::new();
        collect_wal_topology_entry(
            b"cell/cell-a/e/out/CHAIN/1/2",
            &ValueDeletable::Tombstone,
            11,
            &mut parsed,
        );
        collect_wal_topology_entry(
            b"cell/cell-a/e/out/REPLIES/not-a-vertex/9",
            &ValueDeletable::Tombstone,
            12,
            &mut parsed,
        );
        collect_wal_topology_entry(
            b"cell/cell-b/e/out/CHAIN/also-bad/8",
            &ValueDeletable::Tombstone,
            13,
            &mut parsed,
        );

        let generation = crate::GraphIndexGeneration {
            cell_id: "cell-a".to_string(),
            edge_type: "CHAIN".to_string(),
            base_sequence: 10,
            last_wal_id: 0,
            edge_count: 0,
            checksum: 0,
            generation: "fault-isolation".to_string(),
        };
        let affected = parsed
            .iter()
            .map(|entry| entry.affected_edge_for(&generation, 13))
            .collect::<Result<Vec<_>>>()
            .expect("unrelated malformed records must not fail CHAIN in cell-a");
        assert_eq!(
            affected.into_iter().flatten().collect::<Vec<_>>(),
            vec![(1, 2)]
        );

        let malformed_generation = crate::GraphIndexGeneration {
            edge_type: "REPLIES".to_string(),
            ..generation
        };
        let error = parsed
            .iter()
            .map(|entry| entry.affected_edge_for(&malformed_generation, 13))
            .collect::<Result<Vec<_>>>()
            .expect_err("the malformed record must still fail its owning edge type");
        assert!(matches!(error, GraphError::CorruptValue { .. }));
    }
}
