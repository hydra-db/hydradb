use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphCachePolicy {
    pub max_matrix_artifacts: usize,
    pub max_matrix_adjacencies: usize,
    pub max_graphblas_matrices: usize,
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
    pub query_rows_duration_us: u64,
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
    pub(crate) query_rows_duration_us: AtomicU64,
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
            query_rows_duration_us: self.query_rows_duration_us.load(Ordering::Relaxed),
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
