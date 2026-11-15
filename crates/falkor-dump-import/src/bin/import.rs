use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use falkor_dump_import::{
    build_snapshot_spill_with_source_layout, finalize_image, load_spilled_scope, offline_import_limits, DedicatedLayout, ExcludedGraph,
    ExternalKvSorter, GraphSelection, ImageFinalize, OrphanEdge, RelationshipAnomaly, ResolvedScope, SingleWriterLocalStore, SnapshotGraph,
    SourceLayout, SpilledUnscoped, VerifiedEdgeType,
};
use falkordb_rdb::{Properties, PropertyValue};
use hydradb::{
    encode_scope_namespace_id, object_store_from_env, EdgeMetadata, GraphId, GraphScope, NamespacePath, ObjectStoreGraphScopeDirectory,
    OfflineGraphImageEncoder, PlannerStatisticsBuildLimits, QueryFloat, RelationshipMutation, VertexMetadata, VertexPropertyValue,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode};

// v2 switched offline scopes to the canonical client namespace encoding. Keep
// its receipts separate so --resume can never accept an image written at the
// obsolete raw-identifier path.
// v3: identities follow Turbolay's writers and non-finite floats are refused, so
// a v2 receipt, attempt marker or image is never resumed.
// v4: every image is sealed past storage sequence 0 (GraphShard::seal_offline_image),
// so a v3 image, which the first edge write would damage, is never resumed.
const COMPLETION_MAGIC: &str = "hydradb-falkor-direct-import-v4";
// Older binaries ignore unknown JSON fields. A distinct run marker prevents
// them from treating an allowlisted image as a complete-snapshot import.
const INCLUDED_RUN_MAGIC: &str = "hydradb-falkor-direct-import-included-v1";

#[derive(Debug)]
struct Config {
    snapshot_root: PathBuf,
    base_path: String,
    root_namespace: NamespacePath,
    graph_id: GraphId,
    cell_id: String,
    workers: usize,
    scope_concurrency: usize,
    batch_size: usize,
    resume: bool,
    defer_deep_verification: bool,
    env_file: Option<String>,
    quarantine_dir: Option<PathBuf>,
    scratch_dir: Option<PathBuf>,
    selection: GraphSelection,
    dedicated_layout: Option<DedicatedLayout>,
    byog: bool,
}

#[derive(Debug, Serialize)]
struct ScopeImportSummary {
    scope_sha256: String,
    scope_content_sha256: String,
    vertices: usize,
    relationships: usize,
    edge_types: usize,
    batches: usize,
    resumed: bool,
    deep_verified: bool,
    /// Whether this run published the scope's planner statistics object. A
    /// resumed scope publishes nothing, and neither does a store that cannot
    /// do the conditional writes publication needs (a local directory can,
    /// through `SingleWriterLocalStore`).
    planner_statistics_published: bool,
    elapsed_milliseconds: u128,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ScopeCompletion {
    magic: String,
    snapshot_id: String,
    manifest_sha256: String,
    scope_sha256: String,
    scope_content_sha256: String,
    vertices: usize,
    relationships: usize,
    edge_types: usize,
    batches: usize,
    #[serde(default = "legacy_completion_was_deep_verified")]
    deep_verified: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    included_graphs: Vec<String>,
}

const fn legacy_completion_was_deep_verified() -> bool {
    true
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ImportRunMarker {
    magic: String,
    snapshot_id: String,
    manifest_sha256: String,
    object_count: usize,
    dump_bytes: u64,
    base_path: String,
    root_namespace: String,
    graph_id: String,
    cell_id: String,
    #[serde(default)]
    excluded_graphs: Vec<ExcludedGraph>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    included_graphs: Vec<SnapshotGraph>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dedicated_layout: Option<DedicatedLayout>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    byog: bool,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ScopeAttempt {
    magic: String,
    manifest_sha256: String,
    scope_sha256: String,
    scope_content_sha256: String,
    vertices: usize,
    relationships: usize,
}

#[derive(Serialize)]
struct QuarantineReport<'a> {
    orphan_edge_records: &'a [OrphanEdge],
    relationship_anomalies: &'a [RelationshipAnomaly],
}

// The import allocates and frees a scope's records stage by stage. The system
// allocator kept the freed memory too fragmented to reuse: on a production
// graph of 8.5 million records a 20 GiB heap peak cost a 38 GiB RSS, which
// mimalloc alone brought to 22 GiB.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("falkor-dump-import: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::parse()?);
    let planning = Instant::now();
    let scratch = if let Some(parent) = &config.scratch_dir {
        std::fs::create_dir_all(parent)?;
        tempfile::Builder::new().prefix("falkor-direct-import-").tempdir_in(parent)?
    } else {
        tempfile::Builder::new().prefix("falkor-direct-import-").tempdir()?
    };
    let plan = build_snapshot_spill_with_source_layout(
        &config.snapshot_root,
        config.workers,
        scratch.path(),
        &config.selection,
        if config.byog {
            SourceLayout::Byog
        } else {
            config
                .dedicated_layout
                .as_ref()
                .map_or(SourceLayout::Shared, SourceLayout::Dedicated)
        },
    )?;
    let unscoped_vertices = plan.unscoped_vertices.count;
    if (plan.orphan_edges > 0 || plan.relationship_anomalies > 0 || unscoped_vertices > 0) && config.quarantine_dir.is_none() {
        return Err(format!(
            "snapshot contains {} unreachable edge records, {} relationship anomalies and {} vertices no scope can hold; pass --quarantine-dir to preserve them before importing",
            plan.orphan_edges, plan.relationship_anomalies, unscoped_vertices
        )
        .into());
    }
    if let (Some(directory), false) = (&config.quarantine_dir, plan.unscoped_vertices.is_empty()) {
        write_unscoped(directory, &plan.unscoped_vertices)?;
    }
    eprintln!(
        "direct_import_plan snapshot={} manifest_sha256={} objects={} selected_objects={} selected_dump_bytes={} included_graphs={} excluded_graphs={} scopes={} raw_scope_fragments={} raw_vertices={} raw_relationships={} orphan_edges={} relationship_anomalies={} dump_bytes={} elapsed_ms={}",
        plan.snapshot_id,
        plan.manifest_sha256,
        plan.object_count,
        plan.selected_object_count,
        plan.selected_dump_bytes,
        serde_json::to_string(&plan.included_graphs)?,
        serde_json::to_string(&plan.excluded_graphs)?,
        plan.scopes.len(),
        plan.raw_scopes,
        plan.raw_vertices,
        plan.raw_relationships,
        plan.orphan_edges,
        plan.relationship_anomalies,
        plan.dump_bytes,
        planning.elapsed().as_millis()
    );

    let object_store = object_store_from_env(config.env_file.clone())?;
    // A local directory cannot do the conditional updates that publish each
    // scope's planner statistics; this process is its only writer, so it does
    // them itself. The statistics then travel with the image when the
    // directory is copied to S3.
    let object_store: Arc<dyn ObjectStore> = if std::env::var("CLOUD_PROVIDER").as_deref() == Ok("local") {
        Arc::new(SingleWriterLocalStore::new(object_store))
    } else {
        object_store
    };
    let directory = Arc::new(ObjectStoreGraphScopeDirectory::new(
        config.base_path.clone(),
        config.root_namespace.clone(),
        config.graph_id.clone(),
        Arc::clone(&object_store),
    ));
    ensure_run_marker(&object_store, &config, &plan).await?;
    let mut tasks = tokio::task::JoinSet::new();
    let mut summaries = Vec::new();
    let snapshot_id = plan.snapshot_id.clone();
    let manifest_sha256 = plan.manifest_sha256.clone();
    for spilled in plan.scopes {
        if tasks.len() >= config.scope_concurrency {
            let result = tasks.join_next().await.expect("a full scope worker set is not empty");
            summaries.push(result.map_err(|error| format!("scope worker panicked: {error}"))??);
        }
        let scope = tokio::task::spawn_blocking(move || load_spilled_scope(&spilled)).await??;
        if needs_quarantine(&scope) {
            let quarantine = config
                .quarantine_dir
                .as_ref()
                .expect("source anomaly count was checked before import");
            write_quarantine(quarantine, std::slice::from_ref(&scope))?;
        }
        let config = Arc::clone(&config);
        let object_store = Arc::clone(&object_store);
        let directory = Arc::clone(&directory);
        let snapshot_id = snapshot_id.clone();
        let manifest_sha256 = manifest_sha256.clone();
        tasks.spawn(async move {
            import_scope(&config, &snapshot_id, &manifest_sha256, scope, object_store, directory)
                .await
                .map_err(|error| error.to_string())
        });
    }
    while let Some(result) = tasks.join_next().await {
        summaries.push(result.map_err(|error| format!("scope worker panicked: {error}"))??);
    }
    summaries.sort_by(|left, right| left.scope_sha256.cmp(&right.scope_sha256));
    let total_vertices: usize = summaries.iter().map(|summary| summary.vertices).sum();
    let total_relationships: usize = summaries.iter().map(|summary| summary.relationships).sum();
    let resumed = summaries.iter().filter(|summary| summary.resumed).count();
    let deep_verified = summaries.iter().filter(|summary| summary.deep_verified).count();
    eprintln!(
        "direct_import_complete scopes={} resumed={} deep_verified={} verification_deferred={} vertices={} relationships={} elapsed_ms={}",
        summaries.len(),
        resumed,
        deep_verified,
        summaries.len().saturating_sub(deep_verified),
        total_vertices,
        total_relationships,
        planning.elapsed().as_millis()
    );
    println!("{}", serde_json::to_string_pretty(&summaries)?);
    Ok(())
}

async fn import_scope(
    config: &Config,
    snapshot_id: &str,
    manifest_sha256: &str,
    scope: ResolvedScope,
    object_store: Arc<dyn ObjectStore>,
    directory: Arc<ObjectStoreGraphScopeDirectory>,
) -> Result<ScopeImportSummary, Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let scope_sha256 = scope_hash(&scope);
    let scope_content_sha256 = scope_content_hash(&scope)?;
    let completion_path = completion_path(config, manifest_sha256, &scope_sha256);
    let mut promoting_deferred_completion = false;
    if config.resume {
        if let Some(completion) = read_json::<ScopeCompletion>(&object_store, &completion_path).await? {
            if completion.included_graphs != config.selection.included_names() {
                return Err(format!("completion receipt graph selection does not match snapshot scope {scope_sha256}").into());
            }
            validate_completion(
                &completion,
                snapshot_id,
                manifest_sha256,
                &scope_sha256,
                &scope_content_sha256,
                &scope,
                false,
            )?;
            if config.defer_deep_verification || completion.deep_verified {
                return Ok(ScopeImportSummary {
                    scope_sha256,
                    scope_content_sha256,
                    vertices: completion.vertices,
                    relationships: completion.relationships,
                    edge_types: completion.edge_types,
                    batches: completion.batches,
                    resumed: true,
                    deep_verified: completion.deep_verified,
                    planner_statistics_published: false,
                    elapsed_milliseconds: started.elapsed().as_millis(),
                });
            }
            promoting_deferred_completion = true;
        }
    }
    let graph_scope = storage_scope(config, &scope)?;
    let shard_path = format!("{}/{}", graph_scope.scoped_store_path(&config.base_path), config.cell_id);
    let destination_has_data = prefix_has_data(&object_store, &shard_path).await?;
    ensure_scope_attempt(
        &object_store,
        config,
        manifest_sha256,
        &scope_sha256,
        &scope_content_sha256,
        &scope,
        destination_has_data,
    )
    .await?;
    let manifest_path = format!("{shard_path}/manifest");
    let image_exists = prefix_has_data(&object_store, &manifest_path).await?;
    if image_exists {
        if !config.resume {
            return Err(format!("destination scope already has a SlateDB manifest: {}", scope.scope).into());
        }
        eprintln!(
            "direct_image_resume scope={} elapsed_ms={}",
            scope.scope,
            started.elapsed().as_millis()
        );
    } else {
        let image = build_direct_image(config, &scope, &shard_path, Arc::clone(&object_store)).await?;
        eprintln!(
            "direct_image_published scope={} entries={} ssts={} elapsed_ms={}",
            scope.scope,
            image.entries,
            image.ssts,
            started.elapsed().as_millis()
        );
    }
    let mut by_type = BTreeMap::<String, Vec<(u64, u64)>>::new();
    for relationship in scope.relationships.values() {
        by_type
            .entry(relationship.edge_type.clone())
            .or_default()
            .push((relationship.src, relationship.dst));
    }
    let mut batches = scope.vertices.len().div_ceil(config.batch_size);
    for relationships in by_type.values() {
        batches += relationships.len().div_ceil(config.batch_size);
    }
    let finalized = finalize_image(
        ImageFinalize {
            object_store: Arc::clone(&object_store),
            shard_path,
            cell_id: &config.cell_id,
            scope: &graph_scope,
            scope_label: scope.scope.to_string(),
            directory: &directory,
            edges: &by_type,
            limits: offline_import_limits(scope.vertices.len(), scope.relationships.len(), config.batch_size)?,
            max_concurrent_artifact_builds: config.workers.clamp(1, 4),
            statistics_limits: import_statistics_limits(),
            verify: !config.defer_deep_verification,
            error_key: format!("direct-import/{scope_sha256}"),
        },
        |reports| check_merge_indexes(&scope, &scope_sha256, reports),
    )
    .await?;
    let summary = (by_type.len(), batches, finalized.reports.is_some());
    let planner_statistics_published = finalized.planner_statistics_published;
    let completion = ScopeCompletion {
        magic: COMPLETION_MAGIC.to_string(),
        snapshot_id: snapshot_id.to_string(),
        manifest_sha256: manifest_sha256.to_string(),
        scope_sha256: scope_sha256.clone(),
        scope_content_sha256: scope_content_sha256.clone(),
        vertices: scope.vertices.len(),
        relationships: scope.relationships.len(),
        edge_types: summary.0,
        batches: summary.1,
        deep_verified: summary.2,
        included_graphs: config.selection.included_names(),
    };
    put_completion(&object_store, &completion_path, &completion).await?;
    eprintln!(
        "direct_scope_complete scope={} deep_verified={} elapsed_ms={}",
        scope.scope,
        completion.deep_verified,
        started.elapsed().as_millis()
    );
    Ok(ScopeImportSummary {
        scope_sha256,
        scope_content_sha256,
        vertices: completion.vertices,
        relationships: completion.relationships,
        edge_types: completion.edge_types,
        batches: completion.batches,
        resumed: promoting_deferred_completion,
        deep_verified: completion.deep_verified,
        planner_statistics_published,
        elapsed_milliseconds: started.elapsed().as_millis(),
    })
}

/// The import counts every index entry: the image is the scope's only data,
/// and the online entry budget, which keeps an indexer from stalling on one
/// scope, would only refuse a large import here.
fn import_statistics_limits() -> PlannerStatisticsBuildLimits {
    PlannerStatisticsBuildLimits {
        max_entries: u64::MAX,
        ..PlannerStatisticsBuildLimits::default()
    }
}

async fn build_direct_image(
    config: &Config,
    scope: &ResolvedScope,
    shard_path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<slatedb::OfflineImageResult, Box<dyn std::error::Error + Send + Sync>> {
    let scratch = if let Some(parent) = &config.scratch_dir {
        std::fs::create_dir_all(parent)?;
        tempfile::Builder::new().prefix("falkor-image-kv-").tempdir_in(parent)?
    } else {
        tempfile::Builder::new().prefix("falkor-image-kv-").tempdir()?
    };
    // Each thread encodes and sorts the vertices and the relationships whose
    // source vertex falls in its partition, so every structural edge's
    // aggregates are counted by one encoder (absorb_disjoint_sources).
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    let (partitions, sort_buffer_bytes) = image_build_shape(cores, config.scope_concurrency);
    let parts = std::thread::scope(|threads| {
        let workers = (0..partitions)
            .map(|partition| {
                let scratch = scratch.path().join(format!("partition-{partition:03}"));
                threads.spawn(move || encode_partition(config, scope, partition, partitions, scratch, sort_buffer_bytes))
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().map_err(|_| "image encoder thread panicked")?)
            .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()
    })?;
    let mut parts = parts.into_iter();
    let (mut sorter, mut encoder) = parts.next().expect("at least one partition");
    for (rows, partition) in parts {
        sorter.absorb(rows)?;
        encoder.absorb_disjoint_sources(partition)?;
    }
    sorter.extend(encoder.finish_rows())?;
    if scope.byog {
        sorter.extend([OfflineGraphImageEncoder::byog_id_semantics_row()])?;
    }
    Ok(sorter
        .publish(ObjectPath::from(shard_path), object_store, hydradb::GRAPH_SST_TARGET_BYTES)
        .await?)
}

/// Sort-buffer memory for every image building at once: 256 MiB for each of
/// the default eight concurrent scopes.
const SORT_BUDGET_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// A scope's image-build partitions and each one's sort buffer. The scopes
/// building at once share the cores and the sort budget, so a host with more
/// cores gets more, smaller sort runs rather than more memory.
fn image_build_shape(cores: usize, scope_concurrency: usize) -> (u64, usize) {
    let partitions = (cores / scope_concurrency).max(1);
    let sort_buffer_bytes = (SORT_BUDGET_BYTES / scope_concurrency / partitions).max(1);
    (partitions as u64, sort_buffer_bytes)
}

/// The sorted rows and the encoder of one partition of a scope: the vertices
/// with `id % partitions == partition`, and the relationships whose source does.
fn encode_partition(
    config: &Config,
    scope: &ResolvedScope,
    partition: u64,
    partitions: u64,
    scratch: PathBuf,
    sort_buffer_bytes: usize,
) -> Result<(ExternalKvSorter, OfflineGraphImageEncoder), Box<dyn std::error::Error + Send + Sync>> {
    let mut sorter = ExternalKvSorter::new(scratch, sort_buffer_bytes)?;
    let mut encoder = OfflineGraphImageEncoder::new(&config.cell_id, true)?;
    for vertex in scope.vertices.values().filter(|vertex| vertex.id % partitions == partition) {
        let metadata = VertexMetadata {
            labels: vertex.labels.clone(),
            properties: property_map(&vertex.properties)?,
        };
        sorter.extend(encoder.vertex_rows(vertex.id, &metadata)?)?;
    }
    for relationship in scope
        .relationships
        .values()
        .filter(|relationship| relationship.src % partitions == partition)
    {
        let mutation = RelationshipMutation {
            cell_id: config.cell_id.clone(),
            edge_type: relationship.edge_type.clone(),
            src: relationship.src,
            dst: relationship.dst,
            relationship_id: relationship.id,
            metadata: EdgeMetadata {
                properties: property_map(&relationship.properties)?,
            },
        };
        sorter.extend(if scope.byog {
            encoder.relationship_rows_preserving_properties(&mutation)?
        } else {
            encoder.relationship_rows(&mutation)?
        })?;
    }
    Ok((sorter, encoder))
}

/// Every FalkorDB relationship carries its stable `id`, so a verified scope
/// holds one identity index per relationship. A BYOG scope indexes only the
/// user ids that are unique per endpoint pair.
fn check_merge_indexes(scope: &ResolvedScope, scope_hash: &str, reports: &[VerifiedEdgeType]) -> hydradb::Result<()> {
    for (edge_type, relationship_count, report) in reports {
        let expected_relationships = u64::try_from(*relationship_count).map_err(|_| hydradb::GraphError::CorruptValue {
            key: format!("direct-import/{scope_hash}/{edge_type}"),
            reason: "relationship count does not fit u64".to_string(),
        })?;
        let expected_merge_indexes = if scope.byog {
            byog_merge_index_count(scope, edge_type)?
        } else {
            expected_relationships
        };
        if report.relationship_records != expected_relationships || report.relationship_merge_indexes != expected_merge_indexes {
            return Err(hydradb::GraphError::CorruptValue {
                key: format!("direct-import/{scope_hash}/{edge_type}/merge-index"),
                reason: format!(
                    "verified {} relationship records / {} identity indexes; expected {expected_relationships} records / {expected_merge_indexes} indexes",
                    report.relationship_records, report.relationship_merge_indexes
                ),
            });
        }
    }
    Ok(())
}

fn byog_merge_index_count(scope: &ResolvedScope, edge_type: &str) -> hydradb::Result<u64> {
    let mut counts = BTreeMap::new();
    for edge in scope.relationships.values().filter(|edge| edge.edge_type == edge_type) {
        if let Some(value) = property_map(&edge.properties)?
            .get("id")
            .filter(|value| value.is_scalar_indexable())
        {
            *counts.entry((edge.src, edge.dst, value.clone())).or_insert(0usize) += 1;
        }
    }
    Ok(counts.values().filter(|&&count| count == 1).count() as u64)
}

fn property_map(properties: &Properties) -> hydradb::Result<BTreeMap<String, VertexPropertyValue>> {
    properties
        .iter()
        .filter_map(|(name, value)| property_value(name, value).map(|result| result.map(|value| (name.to_string(), value))))
        .collect()
}

fn property_value(name: &str, value: &PropertyValue) -> Option<hydradb::Result<VertexPropertyValue>> {
    Some(Ok(match value {
        // A Cypher null is an absent property. Never manufacture a visible
        // string value for it.
        PropertyValue::Null => return None,
        PropertyValue::Bool(value) => VertexPropertyValue::Bool(*value),
        PropertyValue::Integer(value) => VertexPropertyValue::from_i64(*value),
        // Turbolay stores a NaN or an infinity, but its query engine refuses every
        // row that returns one, so the property would be unreadable after cutover.
        PropertyValue::Float(value) if !value.is_finite() => {
            return Some(Err(hydradb::GraphError::CorruptValue {
                key: format!("property/{name}"),
                reason: format!("non-finite FalkorDB float {value} cannot be returned by Turbolay queries"),
            }))
        }
        PropertyValue::Float(value) => VertexPropertyValue::Float(QueryFloat(*value)),
        PropertyValue::String(value) => VertexPropertyValue::String(value.clone()),
        PropertyValue::Array(_) | PropertyValue::Point { .. } | PropertyValue::VectorF32(_) | PropertyValue::Map(_) => {
            return Some(Err(hydradb::GraphError::CorruptValue {
                key: format!("property/{name}"),
                reason: "FalkorDB composite value has no lossless Turbolay property representation".into(),
            }))
        }
        PropertyValue::DateTime(_) | PropertyValue::Date(_) | PropertyValue::Time(_) | PropertyValue::Duration(_) => {
            return Some(Err(hydradb::GraphError::CorruptValue {
                key: format!("property/{name}"),
                reason: "temporal FalkorDB values have no lossless Turbolay property representation".into(),
            }))
        }
    }))
}

fn storage_scope(config: &Config, scope: &ResolvedScope) -> hydradb::Result<GraphScope> {
    let mut namespace = config
        .root_namespace
        .child(encode_scope_namespace_id("tenant_id", &scope.scope.tenant_id)?)?;
    if !scope.scope.sub_tenant_id.is_empty() {
        namespace = namespace.child(encode_scope_namespace_id("sub_tenant_id", &scope.scope.sub_tenant_id)?)?;
    }
    Ok(GraphScope::new(namespace, config.graph_id.clone()))
}

async fn prefix_has_data(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<bool, slatedb::object_store::Error> {
    use futures::StreamExt as _;
    Ok(store.list(Some(&ObjectPath::from(prefix))).next().await.transpose()?.is_some())
}

fn import_run_prefix(config: &Config, manifest_sha256: &str) -> String {
    format!("{}/_offline_imports/v2/{manifest_sha256}", config.base_path.trim_end_matches('/'),)
}

fn completion_path(config: &Config, manifest_sha256: &str, scope_sha256: &str) -> ObjectPath {
    format!("{}/complete/{scope_sha256}.json", import_run_prefix(config, manifest_sha256)).into()
}

async fn read_json<T: DeserializeOwned>(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>> {
    let result = match store.get(path).await {
        Ok(result) => result,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let payload = result.bytes().await?;
    Ok(Some(
        serde_json::from_slice(&payload).map_err(|error| format!("invalid JSON object at {path}: {error}"))?,
    ))
}

async fn put_json_create_exact<T>(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    value: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: DeserializeOwned + PartialEq + Serialize,
{
    let payload = Bytes::from(serde_json::to_vec(value)?);
    match store.put_opts(path, payload.into(), PutMode::Create.into()).await {
        Ok(_) => Ok(()),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing: T = read_json(store, path)
                .await?
                .ok_or_else(|| format!("object disappeared at {path}"))?;
            if existing == *value {
                Ok(())
            } else {
                Err(format!("conflicting object at {path}").into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

async fn put_completion(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    value: &ScopeCompletion,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = Bytes::from(serde_json::to_vec(value)?);
    match store.put_opts(path, payload.clone().into(), PutMode::Create.into()).await {
        Ok(_) => Ok(()),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing: ScopeCompletion = read_json(store, path)
                .await?
                .ok_or_else(|| format!("object disappeared at {path}"))?;
            if existing == *value {
                return Ok(());
            }

            let mut unverified_value = value.clone();
            unverified_value.deep_verified = false;
            if !existing.deep_verified && value.deep_verified && existing == unverified_value {
                store.put_opts(path, payload.into(), PutMode::Overwrite.into()).await?;
                return Ok(());
            }
            Err(format!("conflicting object at {path}").into())
        }
        Err(error) => Err(error.into()),
    }
}

fn validate_completion(
    completion: &ScopeCompletion,
    snapshot_id: &str,
    manifest_sha256: &str,
    scope_sha256: &str,
    scope_content_sha256: &str,
    scope: &ResolvedScope,
    require_deep_verification: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let edge_types = scope
        .relationships
        .values()
        .map(|relationship| relationship.edge_type.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    if completion.magic != COMPLETION_MAGIC
        || completion.snapshot_id != snapshot_id
        || completion.manifest_sha256 != manifest_sha256
        || completion.scope_sha256 != scope_sha256
        || completion.scope_content_sha256 != scope_content_sha256
        || completion.vertices != scope.vertices.len()
        || completion.relationships != scope.relationships.len()
        || completion.edge_types != edge_types
        || (require_deep_verification && !completion.deep_verified)
    {
        return Err(format!("completion receipt does not match snapshot scope {scope_sha256}").into());
    }
    Ok(())
}

async fn ensure_run_marker(
    store: &Arc<dyn ObjectStore>,
    config: &Config,
    plan: &falkor_dump_import::SnapshotSpill,
) -> Result<(), Box<dyn std::error::Error>> {
    let path: ObjectPath = format!("{}/run.json", import_run_prefix(config, &plan.manifest_sha256)).into();
    let expected = ImportRunMarker {
        magic: if config.byog {
            "hydradb-falkor-direct-import-byog-v2"
        } else if config.dedicated_layout.is_some() {
            "hydradb-falkor-direct-import-dedicated-v1"
        } else if matches!(config.selection, GraphSelection::Include(_)) {
            INCLUDED_RUN_MAGIC
        } else {
            COMPLETION_MAGIC
        }
        .to_string(),
        snapshot_id: plan.snapshot_id.clone(),
        manifest_sha256: plan.manifest_sha256.clone(),
        object_count: plan.object_count,
        dump_bytes: plan.dump_bytes,
        base_path: config.base_path.clone(),
        root_namespace: config.root_namespace.to_string(),
        graph_id: config.graph_id.to_string(),
        cell_id: config.cell_id.clone(),
        excluded_graphs: plan.excluded_graphs.clone(),
        included_graphs: plan.included_graphs.clone(),
        dedicated_layout: config.dedicated_layout.clone(),
        byog: config.byog,
    };
    let existing: Option<ImportRunMarker> = read_json(store, &path).await.map_err(|error| error.to_string())?;
    match (config.resume, existing) {
        (false, Some(_)) => Err(format!("import run already exists at {path}; use --resume for this exact snapshot").into()),
        (true, None) => Err(format!("cannot resume without the import run marker at {path}").into()),
        (_, Some(existing)) if existing != expected => Err(format!("import run marker does not match at {path}").into()),
        (true, Some(_)) => Ok(()),
        (false, None) => put_json_create_exact(store, &path, &expected)
            .await
            .map_err(|error| error.to_string().into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn ensure_scope_attempt(
    store: &Arc<dyn ObjectStore>,
    config: &Config,
    manifest_sha256: &str,
    scope_sha256: &str,
    scope_content_sha256: &str,
    scope: &ResolvedScope,
    destination_has_data: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path: ObjectPath = format!("{}/attempt/{scope_sha256}.json", import_run_prefix(config, manifest_sha256)).into();
    let expected = ScopeAttempt {
        magic: COMPLETION_MAGIC.to_string(),
        manifest_sha256: manifest_sha256.to_string(),
        scope_sha256: scope_sha256.to_string(),
        scope_content_sha256: scope_content_sha256.to_string(),
        vertices: scope.vertices.len(),
        relationships: scope.relationships.len(),
    };
    let existing: Option<ScopeAttempt> = read_json(store, &path).await?;
    if let Some(existing) = existing {
        if existing != expected {
            return Err(format!("scope attempt marker does not match at {path}").into());
        }
        if !config.resume {
            return Err(format!("scope import already started at {path}; use --resume").into());
        }
        return Ok(());
    }
    if destination_has_data {
        return Err(format!("destination scope has data without a matching import marker: {}", scope.scope).into());
    }
    put_json_create_exact(store, &path, &expected).await
}

/// The SHA-256 of the scope's named MessagePack encoding, hashed as it is
/// encoded: a large scope encodes to gigabytes, which a buffer would hold on
/// top of the scope itself.
fn scope_content_hash(scope: &ResolvedScope) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut hasher = Sha256::new();
    rmp_serde::encode::write_named(&mut hasher, scope)?;
    Ok(format!("{:x}", hasher.finalize()))
}

fn scope_hash(scope: &ResolvedScope) -> String {
    format!("{:x}", Sha256::digest(scope.scope.to_string().as_bytes()))
}
/// Write the snapshot's vertices no scope can hold, which belong to no
/// scope's report, as `{"unscoped_vertices": [...]}`. Streamed one graph's
/// spill file at a time, so the report never holds every vertex at once.
fn write_unscoped(directory: &Path, unscoped: &SpilledUnscoped) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    std::fs::create_dir_all(directory)?;
    let path = directory.join("unscoped-vertices.json");
    let temporary = path.with_extension("json.tmp");
    let mut report = std::io::BufWriter::new(std::fs::File::create(&temporary)?);
    report.write_all(b"{\"unscoped_vertices\":[")?;
    let mut first = true;
    for fragment in &unscoped.fragments {
        for vertex in SpilledUnscoped::load(fragment)? {
            if !first {
                report.write_all(b",")?;
            }
            first = false;
            serde_json::to_writer(&mut report, &vertex)?;
        }
    }
    report.write_all(b"]}")?;
    report.into_inner().map_err(|error| error.into_error())?.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
fn needs_quarantine(scope: &ResolvedScope) -> bool {
    !scope.orphan_edges.is_empty() || !scope.relationship_anomalies.is_empty()
}
fn write_quarantine(directory: &Path, scopes: &[ResolvedScope]) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(directory)?;
    for scope in scopes.iter().filter(|scope| needs_quarantine(scope)) {
        let path = directory.join(format!("{}.json", scope_hash(scope)));
        let temporary = path.with_extension("json.tmp");
        let report = QuarantineReport {
            orphan_edge_records: &scope.orphan_edges,
            relationship_anomalies: &scope.relationship_anomalies,
        };
        std::fs::write(&temporary, serde_json::to_vec_pretty(&report)?)?;
        std::fs::rename(temporary, path)?;
    }
    Ok(())
}

impl Config {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        Self::parse_args(std::env::args().skip(1))
    }

    fn parse_args(arguments: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut args = arguments.into_iter();
        let mut values = BTreeMap::new();
        let mut flags = BTreeSet::new();
        let mut selection = GraphSelection::All;
        while let Some(argument) = args.next() {
            if argument == "--resume" || argument == "--defer-deep-verification" || argument == "--byog" {
                flags.insert(argument);
                continue;
            }
            if argument == "--exclude-graph" || argument == "--include-graph" {
                let graph = args.next().ok_or_else(|| format!("{argument} requires a value"))?;
                selection.add_graph(&argument, graph)?;
                continue;
            }
            if !argument.starts_with("--") {
                return Err(format!("unexpected argument {argument}").into());
            }
            let value = args.next().ok_or_else(|| format!("{argument} requires a value"))?;
            values.insert(argument, value);
        }
        let snapshot_root = PathBuf::from(required(&mut values, "--snapshot-root")?);
        let base_path = values.remove("--base-path").unwrap_or_else(|| "graph/data".into());
        let root = values.remove("--root-namespace").unwrap_or_else(|| "default".into());
        let root_namespace = NamespacePath::parse(&root)?;
        let graph_id = GraphId::new(values.remove("--graph-id").unwrap_or_else(|| "default".into()))?;
        let cell_id = values.remove("--cell-id").unwrap_or_else(|| "cell-0".into());
        let workers = parse_number(
            values.remove("--workers"),
            std::thread::available_parallelism().map_or(1, usize::from),
            "workers",
        )?;
        let scope_concurrency = parse_number(values.remove("--scope-concurrency"), 8, "scope concurrency")?;
        let batch_size = parse_number(values.remove("--batch-size"), 8_192, "batch size")?;
        let env_file = values.remove("--env-file");
        let quarantine_dir = values.remove("--quarantine-dir").map(PathBuf::from);
        let scratch_dir = values.remove("--scratch-dir").map(PathBuf::from);
        let dedicated_layout = values
            .remove("--dedicated-layout")
            .map(|path| DedicatedLayout::read(Path::new(&path)))
            .transpose()?;
        let byog = flags.contains("--byog");
        if byog && dedicated_layout.is_some() {
            return Err("--byog and --dedicated-layout are mutually exclusive".into());
        }
        if !values.is_empty() {
            return Err(format!("unknown options: {}", values.keys().cloned().collect::<Vec<_>>().join(", ")).into());
        }
        if workers == 0 || scope_concurrency == 0 || batch_size == 0 {
            return Err("workers, scope concurrency, and batch size must be positive".into());
        }
        Ok(Self {
            snapshot_root,
            base_path,
            root_namespace,
            graph_id,
            cell_id,
            workers,
            scope_concurrency,
            batch_size,
            resume: flags.contains("--resume"),
            defer_deep_verification: flags.contains("--defer-deep-verification"),
            env_file,
            quarantine_dir,
            scratch_dir,
            selection,
            dedicated_layout,
            byog,
        })
    }
}
fn required(values: &mut BTreeMap<String, String>, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    values.remove(key).ok_or_else(|| format!("missing required {key}").into())
}
fn parse_number(value: Option<String>, default: usize, name: &str) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(value
        .map(|value| value.parse())
        .transpose()
        .map_err(|error| format!("invalid {name}: {error}"))?
        .unwrap_or(default))
}
