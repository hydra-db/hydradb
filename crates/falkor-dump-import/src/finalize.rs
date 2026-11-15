//! The steps that turn a published offline image into a store a graph-node
//! opens and an indexer treats as current: seal, one index generation per
//! edge type, verification, planner statistics and scope registration.
//! Every image builder runs them through [`finalize_image`], so they cannot
//! drift apart.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::try_join_all;
use hydradb::{
    GraphCorrectnessReport, GraphIndexGeneration, GraphLimits, GraphOpenOptions, GraphScope, GraphShard, ObjectStoreGraphScopeDirectory,
    PlannerStatisticsBuildLimits, PlannerStatisticsPublish, SparseKernelBackend,
};
use slatedb::object_store::ObjectStore;

/// Traversal depth and roots `verify_current_graph` checks per edge type.
const VERIFY_MAX_HOPS: u8 = 3;
const VERIFY_TRAVERSAL_ROOTS: usize = 32;

/// What [`finalize_image`] runs over one published image.
pub struct ImageFinalize<'a> {
    pub object_store: Arc<dyn ObjectStore>,
    pub shard_path: String,
    pub cell_id: &'a str,
    pub scope: &'a GraphScope,
    /// How the image's logs name the scope.
    pub scope_label: String,
    pub directory: &'a ObjectStoreGraphScopeDirectory,
    /// The image's edges by type, each type's generation built from exactly
    /// these.
    pub edges: &'a BTreeMap<String, Vec<(u64, u64)>>,
    pub limits: GraphLimits,
    pub max_concurrent_artifact_builds: usize,
    pub statistics_limits: PlannerStatisticsBuildLimits,
    /// Run `verify_current_graph` on every edge type. Without it the image is
    /// still sealed, indexed, counted and registered.
    pub verify: bool,
    /// Prefix of the key an unclean verification reports, naming the image.
    pub error_key: String,
}

/// One edge type's verification: the type, the image's edge count of it and
/// the report.
pub type VerifiedEdgeType = (String, usize, GraphCorrectnessReport);

#[derive(Debug)]
pub struct FinalizedImage {
    pub generations: BTreeMap<String, GraphIndexGeneration>,
    /// `None` when verification was not asked for.
    pub reports: Option<Vec<VerifiedEdgeType>>,
    /// See [`publish_planner_statistics`].
    pub planner_statistics_published: bool,
}

/// Seal the image, build each edge type's generation from its edges, verify
/// every type (unless deferred), let the caller `accept` the clean reports,
/// count and publish planner statistics, close, and register the scope.
///
/// Fails on any unclean report, and on what `accept` refuses, before the
/// statistics are counted or the scope is registered. A scope is registered
/// only once its shard closed cleanly.
pub async fn finalize_image(
    finalize: ImageFinalize<'_>,
    accept: impl FnOnce(&[VerifiedEdgeType]) -> hydradb::Result<()>,
) -> hydradb::Result<FinalizedImage> {
    let mut options = GraphOpenOptions {
        limits: finalize.limits.clone(),
        ..Default::default()
    };
    options.backpressure_policy.max_concurrent_artifact_builds = finalize.max_concurrent_artifact_builds;
    // When deep verification is enabled it must be deterministic on offline
    // builders even when SuiteSparse cannot start its optional JIT compiler.
    // CompactCsc consumes the same published CSC generation without depending
    // on host compiler discovery.
    options.cache_policy.sparse_kernel = SparseKernelBackend::CompactCsc;
    let shard =
        GraphShard::open_standalone_writer_with_options(finalize.shard_path.clone(), Arc::clone(&finalize.object_store), options).await?;
    // The image's rows are all at storage sequence 0, which the edge writers
    // read as an empty cell; sealed, the first online write reads them.
    let result = async {
        shard.seal_offline_image(finalize.cell_id).await?;
        let generations = index_and_verify(&finalize, &shard, accept).await?;
        let published = publish_planner_statistics(finalize.cell_id, &finalize.scope_label, &shard, &finalize.statistics_limits).await?;
        Ok::<_, hydradb::GraphError>((generations, published))
    }
    .await;
    let close = shard.close().await;
    let ((generations, reports), planner_statistics_published) = result?;
    close?;
    finalize.directory.register(finalize.scope).await?;
    Ok(FinalizedImage {
        generations,
        reports,
        planner_statistics_published,
    })
}

async fn index_and_verify(
    finalize: &ImageFinalize<'_>,
    shard: &GraphShard,
    accept: impl FnOnce(&[VerifiedEdgeType]) -> hydradb::Result<()>,
) -> hydradb::Result<(BTreeMap<String, GraphIndexGeneration>, Option<Vec<VerifiedEdgeType>>)> {
    let cell_id = finalize.cell_id;
    let generations = try_join_all(finalize.edges.iter().map(|(edge_type, edges)| async move {
        let generation = shard
            .build_graph_index_from_edges(cell_id, edge_type, edges.iter().copied())
            .await?;
        Ok::<_, hydradb::GraphError>((edge_type.clone(), generation))
    }))
    .await?
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    if !finalize.verify {
        return Ok((generations, None));
    }
    let reports = try_join_all(finalize.edges.iter().map(|(edge_type, edges)| async move {
        let report = shard
            .verify_current_graph(cell_id, edge_type, VERIFY_MAX_HOPS, VERIFY_TRAVERSAL_ROOTS)
            .await?;
        Ok::<_, hydradb::GraphError>((edge_type.clone(), edges.len(), report))
    }))
    .await?;
    for (edge_type, _, report) in &reports {
        if !report.is_clean() {
            return Err(hydradb::GraphError::CorruptValue {
                key: format!("{}/{edge_type}", finalize.error_key),
                reason: format!(
                    "post-import verification found {} mismatches: {:?}",
                    report.mismatch_count, report.mismatch_samples
                ),
            });
        }
    }
    accept(&reports)?;
    Ok((generations, Some(reports)))
}

/// Limits for opening an image's shard to finalize it. The normal limits
/// protect an online query from scanning an unexpectedly broad index. This is
/// an offline build bounded by the image's own counts: refreshing the
/// indexes and statistics must be allowed to visit every record the image
/// holds. The online defaults would fail a valid large image halfway through
/// publication (a label with 250,001 vertices hits
/// `max_query_index_candidates`).
pub fn offline_import_limits(vertices: usize, edges: usize, batch_size: usize) -> Result<GraphLimits, std::num::TryFromIntError> {
    let records = vertices.max(edges).max(1);
    let edges = u64::try_from(edges)?;
    Ok(GraphLimits {
        max_bulk_import_edges: batch_size.max(4_096),
        max_artifact_build_edges: edges.max(1),
        max_query_result_vertices: vertices.max(1),
        max_query_intermediate_rows: records,
        max_query_index_candidates: records,
        max_query_scan_edges: edges.max(1),
        max_query_runtime_ms: None,
        ..GraphLimits::default()
    })
}

/// Count the published image the way the indexer does and publish the result,
/// so a scope plans with statistics from its first query instead of waiting
/// for an indexer build.
///
/// The object is derived data and a scope without one still answers
/// correctly, so three outcomes report it unpublished instead of failing the
/// import: a store that cannot do the conditional update publication needs (a
/// local directory without `SingleWriterLocalStore`), a claim taken over
/// mid-publish, and a claim an indexer
/// already holds for the cell (a resumed scope is registered, so the indexer
/// may be building it, and will publish). Any other error, a transient S3
/// failure included, fails the scope before its completion receipt is
/// written, so `--resume` retries it.
pub async fn publish_planner_statistics(
    cell_id: &str,
    scope: &str,
    shard: &GraphShard,
    limits: &PlannerStatisticsBuildLimits,
) -> hydradb::Result<bool> {
    let statistics = shard.build_planner_statistics_with(cell_id, limits).await?;
    match shard.publish_planner_statistics(&statistics).await {
        // Superseded: a publication at least this new is already current.
        Ok(PlannerStatisticsPublish::Published | PlannerStatisticsPublish::Superseded { .. }) => Ok(true),
        Ok(PlannerStatisticsPublish::Fenced) => {
            eprintln!("planner_statistics_unpublished scope={scope} reason=fenced");
            Ok(false)
        }
        Err(hydradb::GraphError::ConditionalWriteConflict {
            operation: "planner_statistics_publish",
            ..
        }) => {
            eprintln!("planner_statistics_unpublished scope={scope} reason=claimed_elsewhere");
            Ok(false)
        }
        Err(hydradb::GraphError::ObjectStore(error @ slatedb::object_store::Error::NotImplemented { .. })) => {
            eprintln!("planner_statistics_unpublished scope={scope} reason=no_conditional_update error={error}");
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
