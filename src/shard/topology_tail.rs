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
    fn set(&mut self, src: VertexId, dst: VertexId, exists: bool) {
        self.states.entry(src).or_default().insert(dst, exists);
    }

    fn state(&self, src: VertexId, dst: VertexId) -> Option<bool> {
        self.states
            .get(&src)
            .and_then(|destinations| destinations.get(&dst))
            .copied()
    }

    /// Flat iteration over every `(src, dst, exists)` delta in the overlay.
    ///
    /// The read path consumes the overlay through point lookups
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
    pub(crate) fn apply_in_neighbors(&self, dst: VertexId, neighbors: &mut BTreeSet<VertexId>) {
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
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn apply_out_neighbors(&self, src: VertexId, neighbors: &mut BTreeSet<VertexId>) {
        let Some(destinations) = self.states.get(&src) else {
            return;
        };
        for (dst, exists) in destinations {
            if *exists {
                neighbors.insert(*dst);
            } else {
                neighbors.remove(dst);
            }
        }
    }

    /// Test-only introspection. The WAL-tail overlay is otherwise only
    /// observable through a compiled GraphBLAS traversal, which needs the
    /// SuiteSparse C kernel; these let a repro assert on the overlay directly.
    #[cfg(test)]
    pub(crate) fn test_state(&self, src: VertexId, dst: VertexId) -> Option<bool> {
        self.state(src, dst)
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
/// `cell_id`/`edge_type`/`seq` at use time.
#[derive(Clone, Debug)]
pub(crate) struct WalTopologyEntry {
    pub(crate) cell_id: String,
    pub(crate) edge_type: String,
    pub(crate) src: VertexId,
    pub(crate) dst: VertexId,
    pub(crate) seq: u64,
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
                            )?;
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
                if entry.seq <= generation.base_sequence || entry.seq > read_sequence {
                    continue;
                }
                if entry.cell_id != generation.cell_id || entry.edge_type != generation.edge_type {
                    continue;
                }
                affected.insert((entry.src, entry.dst));
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
) -> Result<()> {
    let Ok(key) = std::str::from_utf8(key) else {
        return Ok(());
    };
    let fields = key.split('/').collect::<Vec<_>>();
    match fields.as_slice() {
        ["cell", cell, "e", "out", edge_type, src, dst] => {
            parsed.push(WalTopologyEntry {
                cell_id: (*cell).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                seq,
            });
        }
        ["cell", cell, "seg", "tomb", "out", edge_type, src, dst] => {
            parsed.push(WalTopologyEntry {
                cell_id: (*cell).to_string(),
                edge_type: (*edge_type).to_string(),
                src: parse_u64(key, src, "src")?,
                dst: parse_u64(key, dst, "dst")?,
                seq,
            });
        }
        ["cell", cell, "seg", "out", edge_type, _, _, _] => {
            if let Some(value) = value.as_bytes() {
                let segment = decode_out_edge_segment(key, &value)?;
                parsed.extend(
                    segment
                        .destinations
                        .into_iter()
                        .map(|dst| WalTopologyEntry {
                            cell_id: (*cell).to_string(),
                            edge_type: (*edge_type).to_string(),
                            src: segment.src,
                            dst,
                            seq,
                        }),
                );
            }
        }
        _ => {}
    }
    Ok(())
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
    let empty = BTreeMap::new();

    for hop in 1..=max_hops {
        if frontier.is_empty() {
            break;
        }
        let frontier_values = frontier.iter().copied().collect::<Vec<_>>();
        let base =
            crate::sparse_kernel::expand_compiled_graphblas(compiled, &empty, &frontier_values, 1)?;
        edge_visits = edge_visits.saturating_add(base.edge_visits);
        let mut next = base.vertices.into_iter().collect::<BTreeSet<_>>();

        for src in &frontier {
            let Some(changes) = overlay.states.get(src) else {
                continue;
            };
            edge_visits = edge_visits.saturating_add(changes.len() as u64);
            for (dst, exists) in changes {
                if *exists {
                    next.insert(*dst);
                    continue;
                }
                if next.contains(dst)
                    && !frontier.iter().any(|candidate| {
                        overlay.state(*candidate, *dst).unwrap_or_else(|| {
                            crate::sparse_kernel::compiled_graphblas_contains_edge(
                                compiled, *candidate, *dst,
                            )
                        })
                    })
                {
                    next.remove(dst);
                }
            }
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
