use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
mod tests;

use crate::engine;
use crate::{AtomicDurationHistogram, DurationHistogramSnapshot, SparseKernelBackend};

/// Enumerate a metrics snapshot's fields, keyed by the **Rust identifier**.
///
/// Generates `counter_fields()` and `histogram_fields()` on `$ty`. The key is
/// the identifier and nothing else: a Prometheus `graph_*` name and an OTel
/// `db.*`/`turbolay.*` name are exposition vocabulary, and neither may appear
/// in this crate. The binaries hold the name tables, which is also where the
/// test that the two exports cannot disagree about a name belongs.
///
/// The destructuring pattern is deliberately **exhaustive** — no `..` arm. Add
/// a field to the snapshot struct and both accessors stop compiling until it is
/// classified as a counter or as a histogram, which is what turns "adding a
/// counter must not silently reach one export and not the other" from a review
/// comment into a build failure. The binary's
/// `every_histogram_field_reaches_both_exports` picks up where this leaves off:
/// this macro proves the field is *enumerated*, that test proves it is *named*.
macro_rules! snapshot_fields {
    (
        $ty:ident {
            counters { $($counter:ident),* $(,)? }
            histograms { $($histogram:ident),* $(,)? }
        }
    ) => {
        impl $ty {
            /// Every scalar counter on this snapshot, keyed by its Rust
            /// identifier, in declaration order.
            pub fn counter_fields(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
                // Exhaustive on purpose; see the macro's documentation.
                let Self {
                    $($counter,)*
                    $($histogram: _,)*
                } = self;
                [$((stringify!($counter), *$counter),)*].into_iter()
            }

            /// Every duration histogram on this snapshot, keyed by its Rust
            /// identifier, in declaration order.
            pub fn histogram_fields(
                &self,
            ) -> impl Iterator<Item = (&'static str, &$crate::DurationHistogramSnapshot)> + '_
            {
                // Exhaustive on purpose; see the macro's documentation.
                let Self {
                    $($counter: _,)*
                    $($histogram,)*
                } = self;
                [$((stringify!($histogram), $histogram),)*].into_iter()
            }
        }
    };
}

// Both out-of-module users sit behind `query-transport` -- `ClientQueryMetrics`
// behind `client-api`, which implies it, and `QueryTransportMetrics` directly --
// while `GraphOperationalMetricsSnapshot` below invokes the macro by name and
// does not go through the re-export. Under default features the re-export is
// therefore genuinely unused, and cfg-ing it is more honest than an `allow`.
#[cfg(feature = "query-transport")]
pub(crate) use snapshot_fields;

/// Non-exhaustive; see [`crate::GraphOpenOptions`] for the construction pattern.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct GraphCachePolicy {
    pub max_matrix_artifacts: usize,
    pub max_matrix_adjacencies: usize,
    pub max_graphblas_matrices: usize,
    /// Which rung of the sparse-kernel ladder this shard traverses on. Read
    /// once, at matrix-compile time, and baked into the compiled artifact.
    pub sparse_kernel: SparseKernelBackend,
    #[cfg(feature = "opencypher")]
    pub max_parsed_row_queries: usize,
    #[cfg(feature = "opencypher")]
    pub max_relationship_row_sets: usize,
    #[cfg(feature = "opencypher")]
    pub max_relationship_property_row_sets: usize,
    pub max_entries_per_cell: Option<usize>,
    pub pin_matrix_min_edges: u64,
    pub max_concurrent_hydrations: usize,
}

impl Default for GraphCachePolicy {
    fn default() -> Self {
        Self {
            max_matrix_artifacts: 1_024,
            max_matrix_adjacencies: 0,
            max_graphblas_matrices: 64,
            sparse_kernel: crate::sparse_kernel::env_default_kernel(),
            #[cfg(feature = "opencypher")]
            max_parsed_row_queries: 4_096,
            #[cfg(feature = "opencypher")]
            max_relationship_row_sets: 1_024,
            #[cfg(feature = "opencypher")]
            max_relationship_property_row_sets: 4_096,
            max_entries_per_cell: Some(8_192),
            pin_matrix_min_edges: 1_000_000,
            max_concurrent_hydrations: 16,
        }
    }
}

impl GraphCachePolicy {
    pub(crate) fn hydration_permits(&self) -> usize {
        self.max_concurrent_hydrations.max(1)
    }

    pub(crate) fn pin_matrix_artifact(&self, artifact: &engine::MatrixArtifact) -> bool {
        artifact.edge_count >= self.pin_matrix_min_edges
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphCacheKind {
    MatrixArtifact,
    MatrixAdjacency,
    GraphBlas,
    ParsedRowQuery,
    RelationshipRows,
    RelationshipPropertyRows,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheMetricsSnapshot {
    pub matrix_artifact_hits: u64,
    pub matrix_artifact_misses: u64,
    pub matrix_adjacency_hits: u64,
    pub matrix_adjacency_misses: u64,
    pub graphblas_hits: u64,
    pub graphblas_misses: u64,
    pub parsed_row_query_hits: u64,
    pub parsed_row_query_misses: u64,
    pub relationship_rows_hits: u64,
    pub relationship_rows_misses: u64,
    pub relationship_property_rows_hits: u64,
    pub relationship_property_rows_misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub pinned_insertions: u64,
    pub tenant_quota_rejections: u64,
    pub hydration_started: u64,
    pub hydration_waited: u64,
    pub hydration_completed: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphOperationalMetricsSnapshot {
    pub write_attempts: u64,
    pub write_commits: u64,
    pub write_retries: u64,
    pub bulk_import_batches_profiled: u64,
    pub bulk_import_preflight_us: u64,
    pub bulk_import_batch_build_us: u64,
    pub bulk_import_counter_read_us: u64,
    pub bulk_import_commit_us: u64,
    pub artifact_builds_started: u64,
    pub artifact_builds_completed: u64,
    pub artifact_build_duration_us: u64,
    pub artifact_publish_batches: u64,
    pub artifact_records_published: u64,
    pub artifact_publish_duration_us: u64,
    pub gc_jobs_started: u64,
    pub gc_jobs_completed: u64,
    pub gc_keys_deleted: u64,
    pub gc_duration_us: u64,
    pub verifier_runs: u64,
    pub verifier_failures: u64,
    pub verifier_duration_us: u64,
    pub query_rows_started: u64,
    pub query_rows_completed: u64,
    pub query_rows_failed: u64,
    pub query_rows_returned: u64,
    /// Total microseconds spent in the row-query path.
    ///
    /// Retained under its old name and type so nothing that read the sum has to
    /// change, but derived from [`Self::query_rows_latency`] rather than stored:
    /// one `fetch_add` on one quantity, so the sum and the distribution cannot
    /// drift apart.
    pub query_rows_duration_us: u64,
    /// The same measurement as a distribution. Every row query -- one-shot and
    /// streaming, success and failure -- lands here.
    pub query_rows_latency: DurationHistogramSnapshot,
    pub query_artifact_lookup_us: u64,
    pub query_graphblas_cache_us: u64,
    pub query_graphblas_artifact_snapshots: u64,
    pub query_graphblas_rebuilt_snapshots: u64,
    pub query_rust_sparse_fallbacks: u64,
    pub graph_compute_tasks: u64,
    pub graph_compute_queue_us: u64,
    pub graph_compute_duration_us: u64,
    pub backpressure_waits: u64,
}

snapshot_fields!(GraphOperationalMetricsSnapshot {
    counters {
        write_attempts,
        write_commits,
        write_retries,
        bulk_import_batches_profiled,
        bulk_import_preflight_us,
        bulk_import_batch_build_us,
        bulk_import_counter_read_us,
        bulk_import_commit_us,
        artifact_builds_started,
        artifact_builds_completed,
        artifact_build_duration_us,
        artifact_publish_batches,
        artifact_records_published,
        artifact_publish_duration_us,
        gc_jobs_started,
        gc_jobs_completed,
        gc_keys_deleted,
        gc_duration_us,
        verifier_runs,
        verifier_failures,
        verifier_duration_us,
        query_rows_started,
        query_rows_completed,
        query_rows_failed,
        query_rows_returned,
        query_rows_duration_us,
        query_artifact_lookup_us,
        query_graphblas_cache_us,
        query_graphblas_artifact_snapshots,
        query_graphblas_rebuilt_snapshots,
        query_rust_sparse_fallbacks,
        graph_compute_tasks,
        graph_compute_queue_us,
        graph_compute_duration_us,
        backpressure_waits,
    }
    histograms {
        query_rows_latency,
    }
});

#[derive(Default)]
pub(crate) struct GraphOperationalMetrics {
    pub(crate) write_attempts: AtomicU64,
    pub(crate) write_commits: AtomicU64,
    pub(crate) write_retries: AtomicU64,
    pub(crate) bulk_import_batches_profiled: AtomicU64,
    pub(crate) bulk_import_preflight_us: AtomicU64,
    pub(crate) bulk_import_batch_build_us: AtomicU64,
    pub(crate) bulk_import_counter_read_us: AtomicU64,
    pub(crate) bulk_import_commit_us: AtomicU64,
    pub(crate) artifact_builds_started: AtomicU64,
    pub(crate) artifact_builds_completed: AtomicU64,
    pub(crate) artifact_build_duration_us: AtomicU64,
    pub(crate) artifact_publish_batches: AtomicU64,
    pub(crate) artifact_records_published: AtomicU64,
    pub(crate) artifact_publish_duration_us: AtomicU64,
    pub(crate) gc_jobs_started: AtomicU64,
    pub(crate) gc_jobs_completed: AtomicU64,
    pub(crate) gc_keys_deleted: AtomicU64,
    pub(crate) gc_duration_us: AtomicU64,
    pub(crate) verifier_runs: AtomicU64,
    pub(crate) verifier_failures: AtomicU64,
    pub(crate) verifier_duration_us: AtomicU64,
    pub(crate) query_rows_started: AtomicU64,
    pub(crate) query_rows_completed: AtomicU64,
    pub(crate) query_rows_failed: AtomicU64,
    pub(crate) query_rows_returned: AtomicU64,
    // Replaces the `query_rows_duration_us` sum. The snapshot field of that
    // name survives, derived from this histogram's `sum_us`, so the only thing
    // that changed for a reader is that the distribution is now there too.
    pub(crate) query_rows_latency: AtomicDurationHistogram,
    pub(crate) query_artifact_lookup_us: AtomicU64,
    pub(crate) query_graphblas_cache_us: AtomicU64,
    pub(crate) query_graphblas_artifact_snapshots: AtomicU64,
    pub(crate) query_graphblas_rebuilt_snapshots: AtomicU64,
    pub(crate) query_rust_sparse_fallbacks: AtomicU64,
    pub(crate) graph_compute_tasks: AtomicU64,
    pub(crate) graph_compute_queue_us: AtomicU64,
    pub(crate) graph_compute_duration_us: AtomicU64,
    pub(crate) backpressure_waits: AtomicU64,
}

impl GraphOperationalMetrics {
    pub(crate) fn snapshot(&self) -> GraphOperationalMetricsSnapshot {
        let query_rows_latency = self.query_rows_latency.snapshot();
        GraphOperationalMetricsSnapshot {
            write_attempts: self.write_attempts.load(Ordering::Relaxed),
            write_commits: self.write_commits.load(Ordering::Relaxed),
            write_retries: self.write_retries.load(Ordering::Relaxed),
            bulk_import_batches_profiled: self.bulk_import_batches_profiled.load(Ordering::Relaxed),
            bulk_import_preflight_us: self.bulk_import_preflight_us.load(Ordering::Relaxed),
            bulk_import_batch_build_us: self.bulk_import_batch_build_us.load(Ordering::Relaxed),
            bulk_import_counter_read_us: self.bulk_import_counter_read_us.load(Ordering::Relaxed),
            bulk_import_commit_us: self.bulk_import_commit_us.load(Ordering::Relaxed),
            artifact_builds_started: self.artifact_builds_started.load(Ordering::Relaxed),
            artifact_builds_completed: self.artifact_builds_completed.load(Ordering::Relaxed),
            artifact_build_duration_us: self.artifact_build_duration_us.load(Ordering::Relaxed),
            artifact_publish_batches: self.artifact_publish_batches.load(Ordering::Relaxed),
            artifact_records_published: self.artifact_records_published.load(Ordering::Relaxed),
            artifact_publish_duration_us: self.artifact_publish_duration_us.load(Ordering::Relaxed),
            gc_jobs_started: self.gc_jobs_started.load(Ordering::Relaxed),
            gc_jobs_completed: self.gc_jobs_completed.load(Ordering::Relaxed),
            gc_keys_deleted: self.gc_keys_deleted.load(Ordering::Relaxed),
            gc_duration_us: self.gc_duration_us.load(Ordering::Relaxed),
            verifier_runs: self.verifier_runs.load(Ordering::Relaxed),
            verifier_failures: self.verifier_failures.load(Ordering::Relaxed),
            verifier_duration_us: self.verifier_duration_us.load(Ordering::Relaxed),
            query_rows_started: self.query_rows_started.load(Ordering::Relaxed),
            query_rows_completed: self.query_rows_completed.load(Ordering::Relaxed),
            query_rows_failed: self.query_rows_failed.load(Ordering::Relaxed),
            query_rows_returned: self.query_rows_returned.load(Ordering::Relaxed),
            query_rows_duration_us: query_rows_latency.sum_us,
            query_rows_latency,
            query_artifact_lookup_us: self.query_artifact_lookup_us.load(Ordering::Relaxed),
            query_graphblas_cache_us: self.query_graphblas_cache_us.load(Ordering::Relaxed),
            query_graphblas_artifact_snapshots: self
                .query_graphblas_artifact_snapshots
                .load(Ordering::Relaxed),
            query_graphblas_rebuilt_snapshots: self
                .query_graphblas_rebuilt_snapshots
                .load(Ordering::Relaxed),
            query_rust_sparse_fallbacks: self.query_rust_sparse_fallbacks.load(Ordering::Relaxed),
            graph_compute_tasks: self.graph_compute_tasks.load(Ordering::Relaxed),
            graph_compute_queue_us: self.graph_compute_queue_us.load(Ordering::Relaxed),
            graph_compute_duration_us: self.graph_compute_duration_us.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
pub(crate) struct GraphCacheMetrics {
    pub(crate) matrix_artifact_hits: AtomicU64,
    pub(crate) matrix_artifact_misses: AtomicU64,
    pub(crate) matrix_adjacency_hits: AtomicU64,
    pub(crate) matrix_adjacency_misses: AtomicU64,
    pub(crate) graphblas_hits: AtomicU64,
    pub(crate) graphblas_misses: AtomicU64,
    pub(crate) parsed_row_query_hits: AtomicU64,
    pub(crate) parsed_row_query_misses: AtomicU64,
    pub(crate) relationship_rows_hits: AtomicU64,
    pub(crate) relationship_rows_misses: AtomicU64,
    pub(crate) relationship_property_rows_hits: AtomicU64,
    pub(crate) relationship_property_rows_misses: AtomicU64,
    pub(crate) insertions: AtomicU64,
    pub(crate) evictions: AtomicU64,
    pub(crate) pinned_insertions: AtomicU64,
    pub(crate) tenant_quota_rejections: AtomicU64,
    pub(crate) hydration_started: AtomicU64,
    pub(crate) hydration_waited: AtomicU64,
    pub(crate) hydration_completed: AtomicU64,
}

impl GraphCacheMetrics {
    pub(crate) fn record_hit(&self, kind: GraphCacheKind) {
        self.counter(kind, true).fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_miss(&self, kind: GraphCacheKind) {
        self.counter(kind, false).fetch_add(1, Ordering::Relaxed);
    }

    fn counter(&self, kind: GraphCacheKind, hit: bool) -> &AtomicU64 {
        match (kind, hit) {
            (GraphCacheKind::MatrixArtifact, true) => &self.matrix_artifact_hits,
            (GraphCacheKind::MatrixArtifact, false) => &self.matrix_artifact_misses,
            (GraphCacheKind::MatrixAdjacency, true) => &self.matrix_adjacency_hits,
            (GraphCacheKind::MatrixAdjacency, false) => &self.matrix_adjacency_misses,
            (GraphCacheKind::GraphBlas, true) => &self.graphblas_hits,
            (GraphCacheKind::GraphBlas, false) => &self.graphblas_misses,
            (GraphCacheKind::ParsedRowQuery, true) => &self.parsed_row_query_hits,
            (GraphCacheKind::ParsedRowQuery, false) => &self.parsed_row_query_misses,
            (GraphCacheKind::RelationshipRows, true) => &self.relationship_rows_hits,
            (GraphCacheKind::RelationshipRows, false) => &self.relationship_rows_misses,
            (GraphCacheKind::RelationshipPropertyRows, true) => {
                &self.relationship_property_rows_hits
            }
            (GraphCacheKind::RelationshipPropertyRows, false) => {
                &self.relationship_property_rows_misses
            }
        }
    }

    pub(crate) fn snapshot(&self) -> GraphCacheMetricsSnapshot {
        GraphCacheMetricsSnapshot {
            matrix_artifact_hits: self.matrix_artifact_hits.load(Ordering::Relaxed),
            matrix_artifact_misses: self.matrix_artifact_misses.load(Ordering::Relaxed),
            matrix_adjacency_hits: self.matrix_adjacency_hits.load(Ordering::Relaxed),
            matrix_adjacency_misses: self.matrix_adjacency_misses.load(Ordering::Relaxed),
            graphblas_hits: self.graphblas_hits.load(Ordering::Relaxed),
            graphblas_misses: self.graphblas_misses.load(Ordering::Relaxed),
            parsed_row_query_hits: self.parsed_row_query_hits.load(Ordering::Relaxed),
            parsed_row_query_misses: self.parsed_row_query_misses.load(Ordering::Relaxed),
            relationship_rows_hits: self.relationship_rows_hits.load(Ordering::Relaxed),
            relationship_rows_misses: self.relationship_rows_misses.load(Ordering::Relaxed),
            relationship_property_rows_hits: self
                .relationship_property_rows_hits
                .load(Ordering::Relaxed),
            relationship_property_rows_misses: self
                .relationship_property_rows_misses
                .load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            pinned_insertions: self.pinned_insertions.load(Ordering::Relaxed),
            tenant_quota_rejections: self.tenant_quota_rejections.load(Ordering::Relaxed),
            hydration_started: self.hydration_started.load(Ordering::Relaxed),
            hydration_waited: self.hydration_waited.load(Ordering::Relaxed),
            hydration_completed: self.hydration_completed.load(Ordering::Relaxed),
        }
    }
}
