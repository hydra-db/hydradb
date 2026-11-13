#[cfg(feature = "opencypher")]
use crate::query::opencypher::ParsedRowQuery;
use crate::{
    engine, sparse_kernel, BoundedGraphCache, GraphCacheMetrics, GraphCachePolicy, GraphIndexPolicy, GraphLimits, GraphOperationalMetrics,
    MatrixAdjacency, MatrixCacheKey,
};
#[cfg(feature = "opencypher")]
use crate::{
    GdsProjectedGraph, GdsProjectedGraphCacheKey, GraphScope, NativePathResultCacheKey, NativePathResultCacheValue, ParsedRowQueryCacheKey,
    QueryColumn, QueryRow, RelationshipPropertyRowsCacheKey, RelationshipRowsCacheValue, SourceRelationshipRowsCacheKey, VertexId,
    VertexPropertyValue,
};
pub use hydradb_graph_slate::core::state::*;
#[cfg(feature = "opencypher")]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
#[cfg(feature = "datafusion-cypher-engine")]
use std::sync::{OnceLock, Weak};
#[cfg(feature = "opencypher")]
use std::time::Duration;
#[cfg(feature = "opencypher")]
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore};

#[cfg(feature = "opencypher")]
pub(crate) struct NativePathPageCursor {
    pub(crate) scope: GraphScope,
    pub(crate) cell_id: String,
    pub(crate) query: String,
    pub(crate) parameters: std::collections::BTreeMap<String, VertexPropertyValue>,
    pub(crate) columns: Vec<QueryColumn>,
    pub(crate) rows: std::collections::VecDeque<QueryRow>,
    pub(crate) expires_at: Instant,
    pub(crate) resident_bytes: u64,
}

#[cfg(feature = "opencypher")]
pub(crate) struct NativePathPageCursorStore {
    pub(crate) next_id: u64,
    pub(crate) resident_bytes: u64,
    pub(crate) cursors: std::collections::BTreeMap<u64, NativePathPageCursor>,
}

#[cfg(feature = "opencypher")]
impl Default for NativePathPageCursorStore {
    fn default() -> Self {
        Self {
            next_id: 1,
            resident_bytes: 0,
            cursors: std::collections::BTreeMap::new(),
        }
    }
}

/// What one causal-consistency bookmark wait actually did.
///
/// The wait is the read-your-writes cost, and until change 4 of
/// the cell-affine read routing design nothing measured it: the
/// `query.bookmark_wait` span timed it for a trace, and no counter, log or
/// metric could answer "did this read land on the cell's owner, and did it pay
/// the epoch wait?" without re-running a week-long
/// investigation.
///
/// Both facts are known only inside
/// [`GraphShard::wait_for_storage_sequence_observed`] and both are needed one
/// layer up, at the client, where the request they belong to is: the counters
/// live on `ClientQueryMetricsSnapshot` beside the other per-request client
/// counters, and the span fields go on `query.bookmark_wait`. Carrying them out
/// as a plain `Copy` struct is what avoids either a second metrics home in the
/// shard or a guess at the client.
///
/// Deliberately **not** the bookmark. The token is an opaque caller-held value
/// on the never-recorded list (`crates/telemetry/src/semconv.rs`, "What is
/// never recorded"); only its epoch may be recorded, and that is already a
/// field of the request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BookmarkWait {
    /// Whether the shard that served the wait holds the cell's writer.
    ///
    /// `true` is the free path — [`SlateGraphStore::durable_sequence_observed`]
    /// answered from the writer's own commit status. `false` means the answer
    /// came from a `DbReader`, which is the routing outcome cell-affine reads
    /// exist to stop producing.
    pub served_by_cell_writer: bool,
    /// Manifest refreshes the poll loop performed.
    ///
    /// Zero means the fast exit fired and no object-store round trip was made;
    /// anything else is one `refresh_durable_reader` per unit, at 10ms
    /// intervals. It is a count rather than a bool because "polled once during
    /// a handoff" and "polled two hundred times into the deadline" are the two
    /// outcomes an operator has to tell apart, and the histogram of the wait's
    /// duration cannot say which of the two a given request was.
    pub refreshes: u32,
}

impl BookmarkWait {
    /// Whether the wait fell through to the poll loop at all.
    ///
    /// The alert condition. After cell-affine read routing this is ~0 in steady
    /// state and non-zero only around handoffs, which is exactly what makes a
    /// regression visible without re-deriving the analysis.
    pub fn entered_poll_loop(&self) -> bool {
        self.refreshes > 0
    }
}

pub struct GraphShard {
    pub(crate) storage: SlateGraphStore,
    /// Immutable for a shard's lifetime; offline replacement requires closing it.
    pub(crate) cypher_id_properties: tokio::sync::OnceCell<bool>,
    #[cfg(feature = "datafusion-cypher-engine")]
    pub(crate) datafusion_cypher_engine: OnceLock<std::result::Result<hydradb_cypher_df::CypherDfEngine, String>>,
    #[cfg(feature = "datafusion-cypher-engine")]
    pub(crate) datafusion_self: OnceLock<Weak<GraphShard>>,
    pub(crate) limits: GraphLimits,
    pub(crate) cache_policy: GraphCachePolicy,
    pub(crate) cache_metrics: Arc<GraphCacheMetrics>,
    pub(crate) operation_metrics: Arc<GraphOperationalMetrics>,
    pub(crate) hydration_gate: Arc<Semaphore>,
    pub(crate) matrix_compilation_gate: Arc<Semaphore>,
    pub(crate) graph_write_gate: Arc<Semaphore>,
    pub(crate) graph_write_permit_capacity: usize,
    pub(crate) artifact_build_gate: Arc<Semaphore>,
    pub(crate) gc_gate: Arc<Semaphore>,
    pub(crate) index_policy: GraphIndexPolicy,
    pub(crate) await_durable_writes: bool,
    pub(crate) write_authority: GraphWriteAuthority,
    pub(crate) local_write_guard: Arc<Mutex<()>>,
    pub(crate) local_artifact_guard: Arc<Mutex<()>>,
    pub(crate) writer_lanes: Vec<Mutex<()>>,
    pub(crate) matrix_artifact_cache: Mutex<BoundedGraphCache<MatrixCacheKey, engine::MatrixArtifact>>,
    pub(crate) graph_index_generations: Mutex<std::collections::BTreeMap<MatrixCacheKey, engine::GraphIndexGeneration>>,
    pub(crate) matrix_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<MatrixAdjacency>>>,
    pub(crate) graphblas_cache: Mutex<BoundedGraphCache<MatrixCacheKey, Arc<sparse_kernel::CompiledGraphBlasMatrix>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) parsed_row_query_cache: Mutex<BoundedGraphCache<ParsedRowQueryCacheKey, ParsedRowQuery>>,
    #[cfg(feature = "opencypher")]
    pub(crate) source_relationship_rows_cache: Mutex<BoundedGraphCache<SourceRelationshipRowsCacheKey, Arc<Vec<VertexId>>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) relationship_property_rows_cache: Mutex<BoundedGraphCache<RelationshipPropertyRowsCacheKey, RelationshipRowsCacheValue>>,
    #[cfg(feature = "opencypher")]
    pub(crate) native_path_result_cache: Mutex<BoundedGraphCache<NativePathResultCacheKey, NativePathResultCacheValue>>,
    /// Assembled gds graphs, filled by `gds.graph.project` and read by the
    /// algorithm calls on it.
    #[cfg(feature = "opencypher")]
    pub(crate) gds_projected_graph_cache: Mutex<BoundedGraphCache<GdsProjectedGraphCacheKey, Arc<GdsProjectedGraph>>>,
    #[cfg(feature = "opencypher")]
    pub(crate) native_path_page_cursors: Mutex<NativePathPageCursorStore>,
    /// `GRAPH_PLANNER_STATISTICS`: whether the experimental planner loads the
    /// indexer-published statistics object, and for how long it reuses one.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) planner_statistics: crate::PlannerStatisticsConfig,
    /// The loaded statistics object per cell. Empty while the flag is off.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) planner_statistics_cache: crate::shard::PlannerStatisticsCache,
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) planner_statistics_hints: crate::shard::PlannerStatisticsHints,
    pub(crate) wal_tail_file_cache: Mutex<crate::shard::topology_tail::WalTailFileCache>,
    /// `(cell_id, edge_type)` pairs whose xlog low-water key has been observed
    /// present, so the write path can skip the per-transaction floor check.
    /// Only populated after a read confirms the key exists — a pending put is
    /// never cached, so a rolled-back transaction cannot strand the floor.
    pub(crate) xlog_floor_ensured: Arc<StdRwLock<std::collections::HashSet<String>>>,
    /// The shard's own `Arc`, bound by [`GraphShard::into_shared`]: the owned
    /// handle a borrowed call gives to work that outlives it. Unset on a
    /// shard that was never shared, which no task can outlive.
    #[cfg(feature = "opencypher")]
    pub(crate) self_handle: std::sync::OnceLock<std::sync::Weak<GraphShard>>,
    /// Sweeps of superseded write-back index generations running on tasks
    /// of their own. Finished entries are dropped as new ones are added, and
    /// `close` aborts and drains the rest.
    #[cfg(feature = "opencypher")]
    pub(crate) write_back_sweeps: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// The detached sweep of each `(cell_id, property)` that has not yet
    /// taken the generation it sweeps to, with the newest generation
    /// published for it so far. At most one such sweep exists per property:
    /// a write-back that finds one raises its target instead of spawning
    /// another.
    #[cfg(feature = "opencypher")]
    pub(crate) write_back_sweep_targets: std::sync::Mutex<std::collections::HashMap<(String, String), crate::StorageSequence>>,
    /// Write-backs in flight on this shard, which the detached sweeps wait
    /// out so a sweep never shares the writer with a write-back.
    #[cfg(feature = "opencypher")]
    pub(crate) write_back_activity: Arc<WriteBackActivity>,
}

/// Write-backs in flight on a shard, as the sweeps of superseded write-back
/// index generations see them. A sweep and a write-back that share the
/// writer both lose: the sweep's tombstones and the write-back's puts meet
/// in the same memtables, and the writer's backpressure stalls them
/// together. So a detached sweep starts only once no write-back has run for
/// a grace period, and parks between its batches while one is running.
#[cfg(feature = "opencypher")]
#[derive(Default)]
pub(crate) struct WriteBackActivity {
    in_flight: AtomicUsize,
    /// Every write-back ever begun. A waiter compares it across its grace
    /// to see a write-back that began and ended inside the grace, which
    /// `in_flight` alone would not show.
    started: AtomicU64,
    idle: tokio::sync::Notify,
}

#[cfg(feature = "opencypher")]
impl WriteBackActivity {
    /// Count a write-back as in flight until the guard drops.
    pub(crate) fn begin(self: &Arc<Self>) -> WriteBackInFlight {
        self.started.fetch_add(1, Ordering::AcqRel);
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        WriteBackInFlight(Arc::clone(self))
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub(crate) fn started(&self) -> u64 {
        self.started.load(Ordering::Acquire)
    }

    /// Resolve once no write-back is in flight. The wait registers for the
    /// signal before it reads the count, so a write-back that ends between
    /// the read and the wait is not missed.
    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.in_flight() == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// One write-back in flight; dropping it ends the write-back's count and,
/// when it was the last, wakes every waiting sweep.
#[cfg(feature = "opencypher")]
pub(crate) struct WriteBackInFlight(Arc<WriteBackActivity>);

#[cfg(feature = "opencypher")]
impl Drop for WriteBackInFlight {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// What one sweep of superseded write-back index generations did: the
/// entries it deleted, the batches that took, the time it spent waiting
/// for write-backs to finish, before it started and between batches, the
/// generation it opened its scan at, and the keys that scan yielded, the
/// one at or past the current generation that ended it included.
#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriteBackSweepOutcome {
    pub(crate) deleted: u64,
    pub(crate) batches: u64,
    pub(crate) waited: Duration,
    pub(crate) floor: crate::StorageSequence,
    pub(crate) scanned: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheEntryCounts {
    pub matrix_artifacts: usize,
    pub matrix_adjacencies: usize,
    pub graphblas_matrices: usize,
    #[cfg(feature = "opencypher")]
    pub parsed_row_queries: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_row_sets: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_property_row_sets: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheResidentBytes {
    pub matrix_adjacencies: usize,
    pub graphblas_matrices: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_rows: usize,
    #[cfg(feature = "opencypher")]
    pub source_relationship_rows: usize,
    #[cfg(feature = "opencypher")]
    pub relationship_property_rows: usize,
    #[cfg(feature = "opencypher")]
    pub gds_projected_graphs: usize,
}

impl GraphCacheResidentBytes {
    pub fn total(&self) -> usize {
        self.matrix_adjacencies.saturating_add(self.graphblas_matrices).saturating_add({
            #[cfg(feature = "opencypher")]
            {
                self.relationship_rows
                    .saturating_add(self.source_relationship_rows)
                    .saturating_add(self.relationship_property_rows)
                    .saturating_add(self.gds_projected_graphs)
            }
            #[cfg(not(feature = "opencypher"))]
            {
                0
            }
        })
    }
}
