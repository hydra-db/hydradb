//! The Graph Data Science calls: the projection catalog and the two
//! centrality algorithms.
//!
//! A projection is a durable record under the cell, not graph state in one
//! node's memory, because a client's calls land on whichever node routing
//! picks. `gds.graph.project` writes it on the cell's writer, `gds.graph.drop`
//! deletes it, and the algorithms read it at their snapshot anywhere.
//!
//! The algorithms read topology from the published index generations only:
//! each projected relationship type's compiled matrix, without the WAL tail
//! written since. A result is therefore as fresh as the indexer, which the
//! `hydradb.gds.index_base_epoch` span field records, and a type with no
//! generation yet is refused rather than scanned from storage.
//!
//! The graph assembled from those generations is kept in a bounded shard
//! cache, `gds_projected_graph_cache`: every project call assembles and
//! fills it, and the algorithm calls on the projection take it from there,
//! so the assembly is paid once per project rather than once per call. The
//! key is the projection's name and content, the generations, the tail's
//! deletions and, for a label projection, the cell's label membership
//! version, all read at the call's snapshot: a node with the graph cached
//! and one assembling it agree, and a label change on the cell is a new key
//! everywhere while a write-back, which changes no label, keeps hitting. A
//! dropped projection's graph is not evicted; it ages out.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher as _, Hasher as _};
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::Semaphore;

use super::degree_procedure::DegreeRead;
use super::path_procedure::native_path_result_resident_bytes;
use super::query::run_graph_compute;
use super::topology_tail::GraphTopologyTail;
use super::*;
use crate::query::algebra::{QueryTemporal, QueryTemporalKind};
use crate::query::gds_procedure::{
    GdsCall, GdsColumn, GdsField, GdsNodeProjection, GdsOrientation, GdsProcedure, GdsProjection, GdsRelationshipProjection, GDS_VERSION,
};
use crate::sparse_kernel::{OrdinalEdges, SparseKernelBackend};
use crate::{QueryFloat, QueryResultSet, QueryRow, QueryValue};

/// One whole-graph algorithm at a time per process. Each holds the full
/// projected edge list and a GraphBLAS working set at once, and GraphBLAS
/// already spreads one call across every core.
static GDS_COMPUTE_GATE: Semaphore = Semaphore::const_new(1);

/// What a projection selected, with every `'*'` relationship type resolved
/// to the types the cell had when it was projected.
#[derive(Clone, Debug, PartialEq)]
struct ProjectionRecord {
    nodes: GdsNodeProjection,
    relationships: Vec<(String, GdsOrientation)>,
    node_count: u64,
    relationship_count: u64,
    created_at_millis: i64,
}

impl ProjectionRecord {
    fn encode(&self) -> Vec<u8> {
        let labels = match &self.nodes {
            GdsNodeProjection::All => json!("*"),
            GdsNodeProjection::Labels(labels) => json!(labels),
        };
        let relationships = self
            .relationships
            .iter()
            .map(|(edge_type, orientation)| json!({"type": edge_type, "orientation": orientation.as_str()}))
            .collect::<Vec<_>>();
        json!({
            "labels": labels,
            "relationships": relationships,
            "nodeCount": self.node_count,
            "relationshipCount": self.relationship_count,
            "createdAtMillis": self.created_at_millis,
        })
        .to_string()
        .into_bytes()
    }

    fn decode(key: &str, value: &[u8]) -> Result<Self> {
        let corrupt = |reason: &str| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("gds projection record {reason}"),
        };
        let record: Value = serde_json::from_slice(value).map_err(|_| corrupt("is not JSON"))?;
        let nodes = match &record["labels"] {
            Value::String(star) if star == "*" => GdsNodeProjection::All,
            Value::Array(labels) => GdsNodeProjection::Labels(
                labels
                    .iter()
                    .map(|label| label.as_str().map(str::to_string).ok_or_else(|| corrupt("has a non-string label")))
                    .collect::<Result<_>>()?,
            ),
            _ => return Err(corrupt("has no labels")),
        };
        let relationships = record["relationships"]
            .as_array()
            .ok_or_else(|| corrupt("has no relationships"))?
            .iter()
            .map(|relationship| {
                let edge_type = relationship["type"]
                    .as_str()
                    .ok_or_else(|| corrupt("has a relationship without a type"))?;
                let orientation = relationship["orientation"]
                    .as_str()
                    .and_then(GdsOrientation::parse)
                    .ok_or_else(|| corrupt("has a relationship without an orientation"))?;
                Ok((edge_type.to_string(), orientation))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            nodes,
            relationships,
            node_count: record["nodeCount"].as_u64().ok_or_else(|| corrupt("has no nodeCount"))?,
            relationship_count: record["relationshipCount"]
                .as_u64()
                .ok_or_else(|| corrupt("has no relationshipCount"))?,
            created_at_millis: record["createdAtMillis"]
                .as_i64()
                .ok_or_else(|| corrupt("has no createdAtMillis"))?,
        })
    }

    /// GDS's normalized node projection: one entry per label, `__ALL__` for
    /// every node.
    fn node_projection(&self) -> QueryValue {
        let labels = match &self.nodes {
            GdsNodeProjection::All => vec![("__ALL__".to_string(), "*".to_string())],
            GdsNodeProjection::Labels(labels) => labels.iter().map(|label| (label.clone(), label.clone())).collect(),
        };
        record(
            labels
                .into_iter()
                .map(|(key, label)| (key, record(vec![("label", string(label)), ("properties", empty_map())])))
                .collect(),
        )
    }

    /// GDS's normalized relationship projection, keyed by type; a type
    /// projected in two orientations is keyed by both.
    fn relationship_projection(&self) -> QueryValue {
        record(
            self.relationship_keys()
                .map(|(key, edge_type, orientation)| {
                    (
                        key,
                        record(vec![
                            ("aggregation", string("DEFAULT")),
                            ("indexInverse", QueryValue::Bool(false)),
                            ("orientation", string(orientation.as_str())),
                            ("properties", empty_map()),
                            ("type", string(edge_type)),
                        ]),
                    )
                })
                .collect(),
        )
    }

    fn schema_with_orientation(&self) -> QueryValue {
        let nodes = match &self.nodes {
            GdsNodeProjection::All => vec![("__ALL__".to_string(), empty_map())],
            GdsNodeProjection::Labels(labels) => labels.iter().map(|label| (label.clone(), empty_map())).collect(),
        };
        let relationships = self
            .relationship_keys()
            .map(|(key, _, orientation)| {
                let direction = if orientation == GdsOrientation::Undirected {
                    "UNDIRECTED"
                } else {
                    "DIRECTED"
                };
                (key, record(vec![("direction", string(direction)), ("properties", empty_map())]))
            })
            .collect();
        record(vec![
            ("graphProperties", empty_map()),
            ("nodes", record(nodes)),
            ("relationships", record(relationships)),
        ])
    }

    fn relationship_keys(&self) -> impl Iterator<Item = (String, &str, GdsOrientation)> {
        self.relationships.iter().map(|(edge_type, orientation)| {
            let repeated = self.relationships.iter().filter(|(other, _)| other == edge_type).count() > 1;
            let key = if repeated {
                format!("{edge_type}_{}", orientation.as_str())
            } else {
                edge_type.clone()
            };
            (key, edge_type.as_str(), *orientation)
        })
    }

    fn created_at(&self) -> QueryValue {
        let seconds = self.created_at_millis.div_euclid(1_000);
        let nanoseconds = (self.created_at_millis.rem_euclid(1_000) * 1_000_000) as u32;
        let text = chrono::DateTime::from_timestamp(seconds, nanoseconds)
            .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .unwrap_or_default();
        QueryValue::Temporal(QueryTemporal {
            kind: QueryTemporalKind::ZonedDatetime {
                seconds,
                nanoseconds,
                offset_seconds: 0,
            },
            text,
        })
    }
}

/// A map value, its fields ordered by name as `QueryValue::Record` requires.
fn record(fields: Vec<(impl Into<String>, QueryValue)>) -> QueryValue {
    let mut fields = fields.into_iter().map(|(name, value)| (name.into(), value)).collect::<Vec<_>>();
    fields.sort_by(|left, right| left.0.cmp(&right.0));
    QueryValue::Record(fields)
}

fn empty_map() -> QueryValue {
    QueryValue::Record(Vec::new())
}

fn string(value: impl Into<String>) -> QueryValue {
    QueryValue::Property(VertexPropertyValue::String(value.into()))
}

/// A matrix vertex no projected ordinal has been assigned to.
const UNPROJECTED: u64 = u64::MAX;

/// Each projected type's compiled matrix from its newest published index
/// generation, and which generation that was.
struct GenerationMatrices {
    matrices: Vec<GenerationMatrix>,
    generations: Vec<(String, StorageSequence)>,
}

/// One type's generation matrix, and the pairs deleted between its base
/// sequence and the read epoch. A deleted pair the matrix does not hold, one
/// added and deleted within the tail, is never looked up. `deleted` is empty
/// until [`GraphShard::gds_collect_deletions`] fills it under the compute
/// gate.
struct GenerationMatrix {
    generation: crate::GraphIndexGeneration,
    matrix: Arc<crate::sparse_kernel::CompiledGraphBlasMatrix>,
    orientation: GdsOrientation,
    deleted: HashSet<(VertexId, VertexId)>,
}

impl GenerationMatrices {
    /// The oldest generation read: the result is at least that fresh.
    fn index_base_epoch(&self) -> StorageSequence {
        self.generations.iter().map(|(_, base)| *base).min().unwrap_or_default()
    }
}

/// The compute gate, waited for within the query's deadline and
/// cancellation. It is held from before a graph is assembled, so a call
/// waiting for it holds no graph.
async fn gds_compute_permit(budget: &QueryBudget) -> Result<tokio::sync::SemaphorePermit<'static>> {
    budget
        .read_only_io("gds_compute_gate", async {
            GDS_COMPUTE_GATE.acquire().await.map_err(|error| GraphError::CorruptValue {
                key: "query/compute/gds".to_string(),
                reason: format!("gds compute gate closed: {error}"),
            })
        })
        .await
}

fn gds_refusal(feature: impl Into<String>) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Procedure,
        dialect: "OpenCypher",
        feature: feature.into(),
    }
}

/// One row: each requested column's value, in the requested order.
fn gds_row(columns: &[GdsColumn], value: impl Fn(GdsField) -> QueryValue) -> QueryRow {
    QueryRow::new(columns.iter().map(|column| value(column.field)).collect())
}

impl GraphShard {
    pub(super) async fn execute_gds_rows_inner(&self, context: QueryContext, procedure: GdsProcedure) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("gds_start")?;
        let read_epoch = context
            .validated_read_epoch()
            .unwrap_or(self.current_epoch(&context.cell_id).await?);
        let snapshot = self.storage.snapshot().await?;
        if snapshot.seq() != read_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: context.cell_id,
                read_epoch,
                current_epoch: snapshot.seq(),
            });
        }
        let result_columns = procedure.columns();
        let GdsProcedure { call, columns } = procedure.clone();
        let rows = match call {
            GdsCall::Version => vec![gds_row(&columns, |_| string(GDS_VERSION))],
            GdsCall::DebugArrow => vec![gds_row(&columns, |field| match field {
                GdsField::ListenAddress => string(""),
                GdsField::Versions => QueryValue::List(Vec::new()),
                _ => QueryValue::Bool(false),
            })],
            GdsCall::Exists { graph_name } => {
                let exists = self
                    .gds_projection_record(snapshot.as_ref(), &context.cell_id, &graph_name)
                    .await?
                    .is_some();
                vec![gds_row(&columns, |field| match field {
                    GdsField::GraphName => string(graph_name.clone()),
                    _ => QueryValue::Bool(exists),
                })]
            }
            GdsCall::Project(projection) => {
                let read = DegreeRead {
                    snapshot: snapshot.as_ref(),
                    cell_id: &context.cell_id,
                    read_epoch,
                    budget: &budget,
                };
                vec![self.gds_project(&context.scope, read, projection, &columns).await?]
            }
            GdsCall::Drop {
                graph_names,
                fail_if_missing,
            } => self.gds_drop(&context, &graph_names, fail_if_missing, &columns).await?,
            GdsCall::PageRank { graph_name, .. } | GdsCall::Betweenness { graph_name, .. } => {
                return self
                    .gds_algorithm_rows(context, snapshot, read_epoch, &budget, &graph_name, procedure)
                    .await;
            }
        };
        let result = QueryResultSet::new(result_columns, rows).with_read_epoch(read_epoch);
        Ok(match context.validated_storage_sequence() {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        })
    }

    async fn gds_projection_record(
        &self,
        snapshot: &GraphStorageSnapshot,
        cell_id: &str,
        graph_name: &str,
    ) -> Result<Option<ProjectionRecord>> {
        let key = keys::gds_projection(cell_id, graph_name);
        match snapshot.get_with_options(key.as_bytes(), &remote_read_options()).await? {
            Some(value) => ProjectionRecord::decode(&key, &value).map(Some),
            None => Ok(None),
        }
    }

    async fn gds_project(
        &self,
        scope: &GraphScope,
        read: DegreeRead<'_>,
        projection: GdsProjection,
        columns: &[GdsColumn],
    ) -> Result<QueryRow> {
        self.ensure_write_authority(read.cell_id, "gds_graph_project")?;
        let started = std::time::Instant::now();
        let exists = || gds_refusal(format!("A graph with name '{}' already exists.", projection.graph_name));
        // Refused before the projection is assembled; the transaction below
        // checks again, against a project that commits in between.
        if self
            .gds_projection_record(read.snapshot, read.cell_id, &projection.graph_name)
            .await?
            .is_some()
        {
            return Err(exists());
        }
        let relationships = self.gds_resolve_relationship_types(read, &projection.relationships).await?;
        let mut generations = self.gds_generation_matrices(read, &relationships).await?;
        // The counts need the whole graph assembled, as an algorithm does;
        // the algorithm calls that follow find it cached.
        let gate = gds_compute_permit(read.budget).await?;
        self.gds_collect_deletions(read, &mut generations).await?;
        let key = self
            .gds_projected_graph_cache_key(scope, read, &projection.graph_name, &projection.nodes, &relationships, &generations)
            .await?;
        let (graph, gate) = self
            .gds_assemble_and_cache_projected_graph(key, read, &projection.nodes, generations, gate)
            .await?;
        let (relationship_count, gate) = self.gds_relationship_count(&graph, gate).await?;
        let record = ProjectionRecord {
            nodes: projection.nodes,
            relationships,
            node_count: graph.vertices.len() as u64,
            relationship_count,
            created_at_millis: chrono::Utc::now().timestamp_millis(),
        };
        drop((graph, gate));
        let key = keys::gds_projection(read.cell_id, &projection.graph_name);
        let value = record.encode();
        let cell_id = read.cell_id;
        self.run_write_pipeline(async {
            let _permit = self.acquire_graph_write_permit("gds_graph_project").await?;
            let _writer = self.writer_lane(cell_id).lock().await;
            let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
            self.validate_write_fence_txn(&txn, cell_id, "gds_graph_project").await?;
            txn.mark_read([key.as_bytes()])?;
            if txn.get_with_options(key.as_bytes(), &remote_read_options()).await?.is_some() {
                return Err(exists());
            }
            txn.put(key.as_bytes(), value.as_slice())?;
            commit_txn_strict(txn, self.await_durable_writes).await?;
            self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .await?;
        let project_millis = started.elapsed().as_millis() as u64;
        Ok(gds_row(columns, |field| match field {
            GdsField::GraphName => string(projection.graph_name.clone()),
            GdsField::NodeProjection => record.node_projection(),
            GdsField::RelationshipProjection => record.relationship_projection(),
            GdsField::NodeCount => QueryValue::Count(record.node_count),
            GdsField::RelationshipCount => QueryValue::Count(record.relationship_count),
            _ => QueryValue::Count(project_millis),
        }))
    }

    async fn gds_drop(
        &self,
        context: &QueryContext,
        graph_names: &[String],
        fail_if_missing: bool,
        columns: &[GdsColumn],
    ) -> Result<Vec<QueryRow>> {
        self.ensure_write_authority(&context.cell_id, "gds_graph_drop")?;
        let cell_id = context.cell_id.as_str();
        // Each record is read inside the transaction that deletes it, so two
        // drops of one graph cannot both succeed, and a drop cannot delete a
        // replacement projected after it read the old record.
        let dropped = self
            .run_write_pipeline(async {
                let _permit = self.acquire_graph_write_permit("gds_graph_drop").await?;
                let _writer = self.writer_lane(cell_id).lock().await;
                let txn = self.storage.writer()?.begin(IsolationLevel::SerializableSnapshot)?;
                self.validate_write_fence_txn(&txn, cell_id, "gds_graph_drop").await?;
                let mut dropped = Vec::new();
                for graph_name in graph_names {
                    let key = keys::gds_projection(cell_id, graph_name);
                    txn.mark_read([key.as_bytes()])?;
                    match txn.get_with_options(key.as_bytes(), &remote_read_options()).await? {
                        Some(value) => {
                            dropped.push((graph_name, ProjectionRecord::decode(&key, &value)?));
                            txn.delete(key.as_bytes())?;
                        }
                        None if fail_if_missing => {
                            return Err(gds_refusal(format!(
                            "Failed to invoke procedure `gds.graph.drop`: Graph with name `{graph_name}` does not exist on database `{}`.",
                            context.scope.graph_id
                        )))
                        }
                        None => {}
                    }
                }
                if !dropped.is_empty() {
                    commit_txn_strict(txn, self.await_durable_writes).await?;
                    self.operation_metrics.write_commits.fetch_add(1, Ordering::Relaxed);
                }
                Ok(dropped)
            })
            .await?;
        let database = context.scope.graph_id.to_string();
        Ok(dropped
            .iter()
            .map(|(graph_name, record)| {
                let nodes = record.node_count as f64;
                let density = if record.node_count > 1 {
                    record.relationship_count as f64 / (nodes * (nodes - 1.0))
                } else {
                    0.0
                };
                gds_row(columns, |field| match field {
                    GdsField::GraphName => string(graph_name.as_str()),
                    GdsField::Database => string(database.clone()),
                    GdsField::DatabaseLocation => string("local"),
                    GdsField::Configuration => record_configuration(record),
                    // GDS reports a projection's heap footprint here. The
                    // assembled graph is a shard cache entry, bounded and
                    // evictable on its own, not memory the projection owns.
                    GdsField::MemoryUsage => QueryValue::Null,
                    GdsField::SizeInBytes => QueryValue::Property(VertexPropertyValue::SignedInteger(-1)),
                    GdsField::NodeCount => QueryValue::Count(record.node_count),
                    GdsField::RelationshipCount => QueryValue::Count(record.relationship_count),
                    GdsField::CreationTime | GdsField::ModificationTime => record.created_at(),
                    GdsField::SchemaWithOrientation => record.schema_with_orientation(),
                    _ => QueryValue::Float(QueryFloat(density)),
                })
            })
            .collect())
    }

    async fn gds_algorithm_rows(
        &self,
        context: QueryContext,
        snapshot: Arc<GraphStorageSnapshot>,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        graph_name: &str,
        procedure: GdsProcedure,
    ) -> Result<QueryResultSet> {
        let (snapshot, read_epoch, record) = match self.gds_projection_record(snapshot.as_ref(), &context.cell_id, graph_name).await? {
            Some(record) => {
                return self
                    .gds_algorithm_rows_at(context, snapshot, read_epoch, budget, procedure, record)
                    .await
            }
            None => {
                // A replica's reader can trail the writer the project call
                // committed on; one refresh covers a call that follows its
                // project straight onto another node.
                let (_, on_writer) = self.storage.durable_sequence_observed().await?;
                let refreshed = if on_writer {
                    None
                } else {
                    self.storage.refresh_durable_reader().await?;
                    let snapshot = self.storage.reader_snapshot().await?;
                    let record = self.gds_projection_record(snapshot.as_ref(), &context.cell_id, graph_name).await?;
                    record.map(|record| (Arc::clone(&snapshot), snapshot.seq(), record))
                };
                refreshed.ok_or_else(|| {
                    gds_refusal(format!(
                        "Graph with name `{graph_name}` does not exist on database `{}`. It might exist on another database.",
                        context.scope.graph_id
                    ))
                })?
            }
        };
        // The caller scoped the snapshot it was handed, and a storage read
        // that resolves its own snapshot (the WAL boundary of a type's tail)
        // would still see that one. The rest of the call reads the refreshed
        // snapshot, so it runs under it: otherwise an edge deleted between
        // the two would stay in the graph the scores are computed on.
        SlateGraphStore::scope_snapshot(
            Arc::clone(&snapshot),
            self.gds_algorithm_rows_at(context, snapshot, read_epoch, budget, procedure, record),
        )
        .await
    }

    /// [`Self::gds_algorithm_rows`] once the projection's record is read, at
    /// the snapshot it was read from, which the caller has scoped.
    async fn gds_algorithm_rows_at(
        &self,
        context: QueryContext,
        snapshot: Arc<GraphStorageSnapshot>,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
        procedure: GdsProcedure,
        record: ProjectionRecord,
    ) -> Result<QueryResultSet> {
        let max_rows = self.limits.max_query_result_vertices as u64;
        let graph_name = match &procedure.call {
            GdsCall::PageRank { graph_name, .. } | GdsCall::Betweenness { graph_name, .. } => graph_name.clone(),
            _ => unreachable!("only the algorithms reach gds_algorithm_rows"),
        };
        let read = DegreeRead {
            snapshot: snapshot.as_ref(),
            cell_id: &context.cell_id,
            read_epoch,
            budget,
        };
        // A write-back returns one row, so only streaming is capped by rows,
        // and only streamed rows are worth caching.
        let write_property = procedure.call.write_property().map(str::to_string);
        if write_property.is_some() {
            self.ensure_write_authority(&context.cell_id, "gds_write")?;
        } else {
            // The record's count is the projection's size when it was made;
            // the assembled graph is checked again below.
            ensure_limit("gds_result_vertices", record.node_count, max_rows)?;
        }
        let mut generations = self.gds_generation_matrices(read, &record.relationships).await?;

        // Keyed on every type's generation as well as the read epoch: an
        // indexer can publish a generation at or below an epoch already
        // answered, and the deletions subtracted depend on the epoch.
        let result_cache_key = NativePathResultCacheKey {
            scope: context.scope.clone(),
            cell_id: context.cell_id.clone(),
            procedure: format!("{procedure:?} at generations {:?}", generations.generations),
            read_epoch,
            max_result_bytes: context.max_result_bytes,
        };
        if write_property.is_none() {
            if let Some(cached) = self.native_path_result_cache.lock().await.get(&result_cache_key) {
                tracing::Span::current().record("hydradb.query.result_cache", "hit");
                let mut result = (*cached).clone();
                result.storage_sequence = context.validated_storage_sequence();
                return Ok(result);
            }
            tracing::Span::current().record("hydradb.query.result_cache", "miss");
        }

        // Moved into the blocking worker below and handed back with the
        // scores: a call dropped on RESET or a disconnect must not free the
        // gate while its worker still computes. A write-back keeps it until
        // its scores are written, so a write waiting on storage is the only
        // call holding a full score vector.
        let gate = gds_compute_permit(budget).await?;
        self.gds_collect_deletions(read, &mut generations).await?;
        let assembly_started = std::time::Instant::now();
        let index_base_epoch = generations.index_base_epoch();
        let (graph, gate) = self
            .gds_cached_projected_graph(&context.scope, read, &graph_name, &record, generations, gate)
            .await?;
        let deleted_pairs = graph.deleted_pairs_dropped;
        if write_property.is_none() {
            ensure_limit("gds_result_vertices", graph.vertices.len() as u64, max_rows)?;
        }
        // Counted here so the span's count is ready without counting on this
        // worker, and inside pre-processing, which is graph preparation.
        let (_, gate) = self.gds_relationship_count(&graph, gate).await?;
        let pre_processing_millis = assembly_started.elapsed().as_millis() as u64;
        let compute_started = std::time::Instant::now();
        let compute_budget = budget.clone();
        let mut ran_iterations = 0;
        let mut sampling = (0, None);
        let (scores, gate) = match &procedure.call {
            GdsCall::PageRank { config, .. } => {
                let config = config.clone();
                let graph = Arc::clone(&graph);
                run_graph_compute(Arc::clone(&self.operation_metrics), "gds_page_rank", move || {
                    // One library call: the iteration limit bounds it, not
                    // the deadline, which is checked only before it starts.
                    compute_budget.check("gds_page_rank")?;
                    let Some((scores, iterations)) = crate::sparse_kernel::pagerank(
                        &graph.edges,
                        config.damping_factor as f32,
                        config.tolerance as f32,
                        config.max_iterations,
                    )?
                    else {
                        // GDS would return these scores with didConverge
                        // false; LAGraph frees them, so there is nothing to
                        // return but the reason.
                        return Err(GraphError::UnsupportedQuery {
                            reason: QueryFailureReason::NotConverged,
                            dialect: "OpenCypher",
                            feature: format!(
                                "pagerank did not converge in {} iterations at tolerance {:e}; raise maxIterations or tolerance",
                                config.max_iterations, config.tolerance
                            ),
                        });
                    };
                    Ok((scores.into_iter().map(f64::from).collect::<Vec<_>>(), iterations, gate))
                })
                .await
                .map(|(scores, iterations, gate)| {
                    ran_iterations = iterations;
                    (scores, gate)
                })?
            }
            GdsCall::Betweenness { config, .. } => {
                let (size, seed) = config.effective_sampling(graph.edges.vertices);
                sampling = (size, seed);
                let span = tracing::Span::current();
                span.record("hydradb.gds.sampling_size", size);
                if let Some(seed) = seed {
                    span.record("hydradb.gds.sampling_seed", seed);
                }
                let sources = betweenness_sources(graph.edges.vertices, size, seed);
                let graph = Arc::clone(&graph);
                run_graph_compute(Arc::clone(&self.operation_metrics), "gds_betweenness", move || {
                    let batch = betweenness_batch(graph.edges.vertices, graph.edges.len() as u64)?;
                    crate::sparse_kernel::betweenness(&graph.edges, sources, batch, || compute_budget.check("gds_betweenness"))
                        .map(|scores| (scores, gate))
                })
                .await?
            }
            _ => unreachable!("only the algorithms reach gds_algorithm_rows"),
        };
        let compute_millis = compute_started.elapsed().as_millis() as u64;
        record_gds_run(
            &tracing::Span::current(),
            GdsRunShape::of(&procedure.call, &graph, ran_iterations, pre_processing_millis, compute_millis),
        );

        if let Some(property) = write_property {
            let write_started = std::time::Instant::now();
            let WriteBackOutcome {
                written,
                skipped,
                ordered,
                carried,
                carry_scanned,
                carry_millis,
            } = self
                .set_property_on_existing_vertices(&context.cell_id, &property, &graph.vertices, scores, budget, Some(gate))
                .await?;
            let write_millis = write_started.elapsed().as_millis() as u64;
            let distribution = centrality_distribution(&ordered);
            let span = tracing::Span::current();
            span.record("hydradb.gds.properties_written", written);
            span.record("hydradb.gds.skipped_missing_vertices", skipped);
            span.record("hydradb.gds.write_millis", write_millis);
            span.record("hydradb.gds.carried_forward", carried);
            span.record("hydradb.gds.carry_forward_entries", carry_scanned);
            span.record("hydradb.gds.carry_forward_millis", carry_millis);
            let mut configuration = vec![
                ("writeProperty", string(property)),
                ("skippedMissingVertices", QueryValue::Count(skipped)),
                ("indexBaseEpoch", QueryValue::Count(index_base_epoch)),
                ("readEpoch", QueryValue::Count(read_epoch)),
                ("deletedPairsDropped", QueryValue::Count(deleted_pairs)),
            ];
            match &procedure.call {
                GdsCall::PageRank { config, .. } => configuration.extend([
                    ("maxIterations", QueryValue::Count(u64::from(config.max_iterations))),
                    ("dampingFactor", QueryValue::Float(QueryFloat(config.damping_factor))),
                    ("tolerance", QueryValue::Float(QueryFloat(config.tolerance))),
                ]),
                _ => configuration.extend([
                    ("samplingSize", QueryValue::Count(sampling.0)),
                    (
                        "samplingSeed",
                        sampling.1.map_or(QueryValue::Null, |seed| {
                            QueryValue::Property(VertexPropertyValue::SignedInteger(seed))
                        }),
                    ),
                ]),
            }
            let configuration = self::record(configuration);
            let row = gds_row(&procedure.columns, |field| match field {
                GdsField::RanIterations => QueryValue::Count(u64::from(ran_iterations)),
                // A run that does not converge is refused before it writes.
                GdsField::DidConverge => QueryValue::Bool(true),
                GdsField::CentralityDistribution => distribution.clone(),
                GdsField::PreProcessingMillis => QueryValue::Count(pre_processing_millis),
                GdsField::ComputeMillis => QueryValue::Count(compute_millis),
                GdsField::WriteMillis => QueryValue::Count(write_millis),
                GdsField::NodePropertiesWritten => QueryValue::Count(written),
                GdsField::Configuration => configuration.clone(),
                _ => QueryValue::Count(0),
            });
            return Ok(QueryResultSet::new(procedure.columns(), vec![row]).with_read_epoch(read_epoch));
        }
        drop(gate);

        let mut scored = graph.vertices.iter().copied().zip(scores).collect::<Vec<_>>();
        scored.sort_unstable_by_key(|(vertex, _)| *vertex);
        let mut rows = Vec::with_capacity(scored.len());
        for (vertex, score) in scored {
            let row = gds_row(&procedure.columns, |field| match field {
                GdsField::NodeId => QueryValue::VertexId(vertex),
                _ => QueryValue::Float(QueryFloat(score)),
            });
            budget.account_result_row(&row)?;
            rows.push(row);
        }

        let result = QueryResultSet::new(procedure.columns(), rows).with_read_epoch(read_epoch);
        let result = match context.validated_storage_sequence() {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        };
        let resident_bytes = result_cache_key
            .estimated_resident_bytes()
            .saturating_add(native_path_result_resident_bytes(&result));
        self.native_path_result_cache.lock().await.insert_sized(
            result_cache_key,
            Arc::new(result.clone()),
            context.cell_id,
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok(result)
    }

    /// `'*'` becomes every relationship type the cell has written, each in
    /// the orientation it was asked for.
    async fn gds_resolve_relationship_types(
        &self,
        read: DegreeRead<'_>,
        projections: &[GdsRelationshipProjection],
    ) -> Result<Vec<(String, GdsOrientation)>> {
        let mut resolved = BTreeSet::new();
        let mut all_types = None;
        for projection in projections {
            match &projection.edge_type {
                Some(edge_type) => {
                    resolved.insert((edge_type.clone(), projection.orientation));
                }
                None => {
                    if all_types.is_none() {
                        all_types = Some(self.gds_cell_edge_types(read).await?);
                    }
                    for edge_type in all_types.iter().flatten() {
                        resolved.insert((edge_type.clone(), projection.orientation));
                    }
                }
            }
        }
        if resolved.is_empty() {
            return Err(gds_refusal("the cell has no relationship types to project"));
        }
        Ok(resolved.into_iter().collect())
    }

    /// Every relationship type written to the cell: each has a dirty marker
    /// from its first write on, which is how the indexer finds it.
    async fn gds_cell_edge_types(&self, read: DegreeRead<'_>) -> Result<Vec<String>> {
        let prefix = keys::matrix_dirty_prefix(read.cell_id);
        let mut iter = read
            .budget
            .read_only_io("gds_edge_types", async {
                Ok::<_, GraphError>(
                    read.snapshot
                        .scan_prefix_with_options(prefix.as_bytes(), .., &remote_bulk_scan_options())
                        .await?,
                )
            })
            .await?;
        let mut edge_types = Vec::new();
        while let Some(kv) = read
            .budget
            .read_only_io("gds_edge_types", async { Ok::<_, GraphError>(iter.next().await?) })
            .await?
        {
            let key = String::from_utf8_lossy(&kv.key);
            if let Some(edge_type) = key.strip_prefix(&prefix).filter(|edge_type| !edge_type.is_empty()) {
                edge_types.push(edge_type.to_string());
            }
        }
        Ok(edge_types)
    }

    /// Each projected type's published generation, refused for a type the
    /// indexer has not published yet. The pairs deleted since are left for
    /// [`Self::gds_collect_deletions`], which a call runs once it holds the
    /// compute gate: a tail can hold up to `max_query_scan_edges` pairs per
    /// type, and every call queued for the gate would otherwise keep its own
    /// copy while one algorithm runs. The generations alone are what a
    /// result cache key needs, so a cached call never waits for the gate.
    async fn gds_generation_matrices(
        &self,
        read: DegreeRead<'_>,
        relationships: &[(String, GdsOrientation)],
    ) -> Result<GenerationMatrices> {
        if crate::sparse_kernel::default_matrix_kernel(&self.cache_policy) == SparseKernelBackend::Adjacency {
            return Err(gds_refusal(
                "gds procedures read compiled index generations, which GRAPH_SPARSE_KERNEL=adjacency does not keep",
            ));
        }
        let mut generations = GenerationMatrices {
            matrices: Vec::with_capacity(relationships.len()),
            generations: Vec::with_capacity(relationships.len()),
        };
        for (edge_type, orientation) in relationships {
            read.budget.check("gds_generation_lookup")?;
            let Some((generation, matrix)) = self
                .published_generation_matrix(read.cell_id, edge_type, read.read_epoch, read.budget)
                .await?
            else {
                return Err(gds_refusal(format!("no index generation for type {edge_type} yet")));
            };
            generations.generations.push((edge_type.clone(), generation.base_sequence));
            generations.matrices.push(GenerationMatrix {
                generation,
                matrix,
                orientation: *orientation,
                deleted: HashSet::new(),
            });
        }
        let span = tracing::Span::current();
        span.record("hydradb.gds.index_base_epoch", generations.index_base_epoch());
        Ok(generations)
    }

    /// The pairs deleted from each type's generation by the read epoch.
    ///
    /// The generation is read without the edges added after it, so a result
    /// can miss recent additions, but every pair deleted by the read epoch is
    /// removed, so it never holds an edge the snapshot does not. When the WAL
    /// tail that says so cannot be read, the call is refused rather than run
    /// on an unchecked generation.
    async fn gds_collect_deletions(&self, read: DegreeRead<'_>, generations: &mut GenerationMatrices) -> Result<()> {
        for typed in &mut generations.matrices {
            let generation = &typed.generation;
            let index_behind = || GraphError::UnsupportedQuery {
                reason: QueryFailureReason::IndexBehind,
                dialect: "OpenCypher",
                feature: format!(
                    "index_behind: the WAL tail of type {} from its generation at {} to the read epoch {} cannot be read; \
                     retry once the indexer publishes a newer generation",
                    generation.edge_type, generation.base_sequence, read.read_epoch
                ),
            };
            let overlay = match self
                .topology_tail_since(generation, read.snapshot, read.read_epoch, read.budget)
                .await
            {
                Ok(GraphTopologyTail::Complete(overlay)) => overlay,
                Ok(GraphTopologyTail::Unavailable)
                | Err(GraphError::AdmissionRejected {
                    operation: "graph_index_wal_affected_edges",
                    ..
                }) => return Err(index_behind()),
                Err(error) => return Err(error),
            };
            // Collected without a matrix lookup per pair: the walk that
            // assembles the graph skips each one as it meets it.
            typed.deleted = overlay
                .entries()
                .filter(|(_, _, exists)| !exists)
                .map(|(src, dst, _)| (src, dst))
                .collect();
        }
        Ok(())
    }

    /// The cache key of `graph_name`'s graph at this call's snapshot. Reads
    /// the cell's label membership version for a label projection: one
    /// point get per call, which is what lets a cached graph and a fresh
    /// assembly agree on the members without scanning them.
    async fn gds_projected_graph_cache_key(
        &self,
        scope: &GraphScope,
        read: DegreeRead<'_>,
        graph_name: &str,
        nodes: &GdsNodeProjection,
        relationships: &[(String, GdsOrientation)],
        generations: &GenerationMatrices,
    ) -> Result<GdsProjectedGraphCacheKey> {
        let label_version = match nodes {
            GdsNodeProjection::Labels(_) => {
                let key = keys::vertex_label_version(read.cell_id);
                read.snapshot
                    .get_with_options(key.as_bytes(), &remote_read_options())
                    .await?
                    .map(|value| decode_u64(&key, &value))
                    .transpose()?
            }
            GdsNodeProjection::All => None,
        };
        Ok(GdsProjectedGraphCacheKey {
            scope: scope.clone(),
            cell_id: read.cell_id.to_string(),
            graph_name: graph_name.to_string(),
            projection: format!("{nodes:?} over {relationships:?}"),
            label_version,
            generations: generations.generations.clone(),
            deleted: generations
                .matrices
                .iter()
                .map(|generation| {
                    let mut pairs = generation.deleted.iter().copied().collect::<Vec<_>>();
                    pairs.sort_unstable();
                    pairs
                })
                .collect(),
        })
    }

    /// The projection's graph at these generations for an algorithm call:
    /// the one its project call cached, or assembled now and cached. Called
    /// with the compute gate held, as the assembly is, and hands it back.
    /// `record` is the projection the call read, for its nodes and resolved
    /// relationship types.
    async fn gds_cached_projected_graph(
        &self,
        scope: &GraphScope,
        read: DegreeRead<'_>,
        graph_name: &str,
        record: &ProjectionRecord,
        generations: GenerationMatrices,
        gate: tokio::sync::SemaphorePermit<'static>,
    ) -> Result<(Arc<GdsProjectedGraph>, tokio::sync::SemaphorePermit<'static>)> {
        let ProjectionRecord { nodes, relationships, .. } = record;
        let key = self
            .gds_projected_graph_cache_key(scope, read, graph_name, nodes, relationships, &generations)
            .await?;
        let span = tracing::Span::current();
        if let Some(graph) = self.gds_projected_graph_cache.lock().await.get(&key) {
            span.record("hydradb.gds.graph_cache", "hit");
            span.record("hydradb.gds.deleted_pairs", graph.deleted_pairs_dropped);
            return Ok((graph, gate));
        }
        span.record("hydradb.gds.graph_cache", "miss");
        self.gds_assemble_and_cache_projected_graph(key, read, nodes, generations, gate)
            .await
    }

    /// Assemble the graph and make it the cache's entry, replacing one of
    /// the same key. The project call always takes this path, so the counts
    /// its record reports come from an assembly at its own snapshot. The key
    /// covers everything the assembly reads, the label members by their
    /// version, so an entry this replaces holds the same graph, and a cached
    /// graph and a fresh assembly at one snapshot never differ.
    /// The graph's distinct relationship count, counted on the blocking pool the
    /// first time and cached on the graph after. The first count sorts a copy of
    /// every pair, which on a large graph would hold an async worker, and the
    /// requests queued behind it, for as long as the sort takes.
    ///
    /// The compute gate moves into the worker and comes back with the count, as
    /// it does for the algorithm workers: a call dropped on RESET or a
    /// disconnect must not free the gate while its sort still runs, or another
    /// call could start a graph-sized allocation of its own beside it.
    async fn gds_relationship_count(
        &self,
        graph: &Arc<GdsProjectedGraph>,
        gate: tokio::sync::SemaphorePermit<'static>,
    ) -> Result<(u64, tokio::sync::SemaphorePermit<'static>)> {
        if let Some(count) = graph.counted_relationships() {
            return Ok((count, gate));
        }
        let graph = Arc::clone(graph);
        run_graph_compute(Arc::clone(&self.operation_metrics), "gds_relationship_count", move || {
            Ok((graph.relationship_count(), gate))
        })
        .await
    }

    async fn gds_assemble_and_cache_projected_graph(
        &self,
        key: GdsProjectedGraphCacheKey,
        read: DegreeRead<'_>,
        nodes: &GdsNodeProjection,
        generations: GenerationMatrices,
        gate: tokio::sync::SemaphorePermit<'static>,
    ) -> Result<(Arc<GdsProjectedGraph>, tokio::sync::SemaphorePermit<'static>)> {
        let (graph, gate) = self.gds_projected_graph(read, nodes, generations, gate).await?;
        let graph = Arc::new(graph);
        self.operation_metrics.gds_assemblies.fetch_add(1, Ordering::Relaxed);
        tracing::Span::current().record("hydradb.gds.deleted_pairs", graph.deleted_pairs_dropped);
        let resident_bytes = key.estimated_resident_bytes().saturating_add(graph.estimated_resident_bytes());
        self.gds_projected_graph_cache.lock().await.insert_sized(
            key,
            Arc::clone(&graph),
            read.cell_id,
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok((graph, gate))
    }

    /// The projection's vertices and edges, with the compute gate handed
    /// back. The label scan reads storage, so it runs here; the edge walk
    /// is CPU work in proportion to the graph, so it runs on a blocking
    /// worker that holds `gate` and drops `generations`, and the async
    /// worker serves other requests meanwhile. A call dropped on RESET or a
    /// disconnect does not free the gate while the walk still runs.
    async fn gds_projected_graph(
        &self,
        read: DegreeRead<'_>,
        nodes: &GdsNodeProjection,
        generations: GenerationMatrices,
        gate: tokio::sync::SemaphorePermit<'static>,
    ) -> Result<(GdsProjectedGraph, tokio::sync::SemaphorePermit<'static>)> {
        let mut members = None;
        if let GdsNodeProjection::Labels(labels) = nodes {
            let mut vertices = Vec::new();
            let mut scanned = 0;
            for label in labels {
                self.for_each_native_degree_label_member(read, label, &mut scanned, |vertex| vertices.push(vertex))
                    .await?;
            }
            // One label's scan is already ascending; several interleave and
            // can share a vertex.
            if labels.len() > 1 {
                vertices.sort_unstable();
                vertices.dedup();
            }
            members = Some(vertices);
        }
        let budget = read.budget.clone();
        run_graph_compute(Arc::clone(&self.operation_metrics), "gds_projection", move || {
            let graph = assemble_projected_graph(members, &generations, &budget)?;
            Ok((graph, gate))
        })
        .await
    }
}

/// The edge walk of [`GraphShard::gds_projected_graph`]. With labels,
/// `members` are the vertices, in ascending id order, isolated ones
/// included, and an edge is kept only when both ends are members. With
/// `'*'`, a vertex is an endpoint of a kept edge, at the ordinal of its
/// first encounter.
///
/// Endpoints are remapped through one table per generation matrix, from
/// its own ordinal to the projected one, never through a map keyed by
/// id: a label's members and a matrix's vertices are both sorted, so the
/// table is one merge of the two, and `'*'` fills it as edges arrive.
/// Transient while assembling: 8 bytes per matrix vertex for the table,
/// and with `'*'` over more than one type, 16 bytes per projected vertex
/// to sort what earlier matrices assigned. Resident after: 8 bytes per
/// vertex and 16 per edge, as `GdsProjectedGraph` says.
fn assemble_projected_graph(
    members: Option<Vec<VertexId>>,
    generations: &GenerationMatrices,
    budget: &QueryBudget,
) -> Result<GdsProjectedGraph> {
    let labelled = members.is_some();
    let mut vertices = members.unwrap_or_default();
    // With `'*'` every edge but a deleted pair is kept, so the
    // generation's edge count is the size. With labels only edges
    // between members are, which can be a sliver of a large type, so
    // the reservation assumes the members carry the generation's
    // average degree and is never more than its edge count; the vectors
    // grow past it when the members are denser than average. A
    // reservation of the whole type would request memory the result
    // never uses.
    let capacity = generations
        .matrices
        .iter()
        .map(|generation| {
            let per_edge = if generation.orientation == GdsOrientation::Undirected {
                2
            } else {
                1
            };
            let edges = generation.matrix.edge_count();
            let expected = if labelled {
                let matrix_vertices = generation.matrix.vertex_ids().len().max(1) as u128;
                let at_average_degree = vertices.len() as u128 * edges as u128 / matrix_vertices;
                edges.min(usize::try_from(at_average_degree).unwrap_or(usize::MAX))
            } else {
                edges
            };
            expected.saturating_mul(per_edge)
        })
        .fold(0_usize, usize::saturating_add);
    // Sized once every endpoint has an ordinal.
    let mut edges = OrdinalEdges::new(0);
    edges.sources.reserve_exact(capacity);
    edges.targets.reserve_exact(capacity);
    let mut deleted_pairs_dropped = 0_u64;
    for (index, generation) in generations.matrices.iter().enumerate() {
        let GenerationMatrix {
            matrix,
            orientation,
            deleted,
            ..
        } = generation;
        let ids = matrix.vertex_ids();
        let mut table = if labelled {
            ordinal_table(ids, vertices.iter().enumerate().map(|(ordinal, vertex)| (*vertex, ordinal as u64)))
        } else if index == 0 {
            vertices.reserve_exact(ids.len());
            vec![UNPROJECTED; ids.len()]
        } else {
            let mut assigned = vertices
                .iter()
                .enumerate()
                .map(|(ordinal, vertex)| (*vertex, ordinal as u64))
                .collect::<Vec<_>>();
            assigned.sort_unstable();
            ordinal_table(ids, assigned.into_iter())
        };
        let mut checked = 0_u32;
        let mut check = || {
            checked = checked.wrapping_add(1);
            if checked.is_multiple_of(4_096) {
                budget.check("gds_projection")?;
            }
            Ok(())
        };
        matrix.for_each_edge_ordinals(
            |src, dst| {
                // Checked before an ordinal is assigned, so an endpoint
                // whose every edge was deleted is not a vertex of `'*'`.
                if !deleted.is_empty() && deleted.contains(&(ids[src], ids[dst])) {
                    deleted_pairs_dropped += 1;
                    return;
                }
                let mut ordinal = |matrix_ordinal: usize| {
                    let slot = &mut table[matrix_ordinal];
                    if *slot == UNPROJECTED {
                        if labelled {
                            return None;
                        }
                        vertices.push(ids[matrix_ordinal]);
                        *slot = vertices.len() as u64 - 1;
                    }
                    Some(*slot)
                };
                let (Some(src), Some(dst)) = (ordinal(src), ordinal(dst)) else {
                    return;
                };
                match orientation {
                    GdsOrientation::Natural => edges.push(src, dst),
                    GdsOrientation::Reverse => edges.push(dst, src),
                    GdsOrientation::Undirected => {
                        edges.push(src, dst);
                        edges.push(dst, src);
                    }
                }
            },
            &mut check,
        )?;
    }
    edges.vertices = vertices.len() as u64;
    // Cached at its resident size, not the bound it was reserved at.
    vertices.shrink_to_fit();
    edges.sources.shrink_to_fit();
    edges.targets.shrink_to_fit();
    Ok(GdsProjectedGraph {
        vertices,
        edges,
        deleted_pairs_dropped,
        distinct_pairs: std::sync::OnceLock::new(),
    })
}

/// Each matrix ordinal's projected ordinal, from the projected vertices in
/// ascending id order with their ordinals, in one pass over both. A matrix
/// vertex that is not projected stays `UNPROJECTED`.
fn ordinal_table(matrix_ids: &[VertexId], mut projected: impl Iterator<Item = (VertexId, u64)>) -> Vec<u64> {
    let mut table = vec![UNPROJECTED; matrix_ids.len()];
    let mut next = projected.next();
    for (matrix_ordinal, id) in matrix_ids.iter().enumerate() {
        while next.is_some_and(|(vertex, _)| vertex < *id) {
            next = projected.next();
        }
        if let Some((vertex, ordinal)) = next {
            if vertex == *id {
                table[matrix_ordinal] = ordinal;
            }
        }
    }
    table
}

/// The `configuration` GDS reports for a projection: the normalized
/// projections it was made with.
fn record_configuration(record: &ProjectionRecord) -> QueryValue {
    self::record(vec![
        ("nodeProjection", record.node_projection()),
        ("relationshipProjection", record.relationship_projection()),
    ])
}

/// What one algorithm call ran over and where its time went: the timings the
/// call also returns to the client, and the projected graph's size.
#[derive(Debug, Eq, PartialEq)]
struct GdsRunShape {
    pre_processing_millis: u64,
    compute_millis: u64,
    /// `None` for an algorithm that does not iterate.
    iterations: Option<u32>,
    node_count: u64,
    /// Distinct edges as the kernels compute on them, the projection's
    /// `relationshipCount`.
    relationship_count: u64,
}

impl GdsRunShape {
    fn of(call: &GdsCall, graph: &GdsProjectedGraph, ran_iterations: u32, pre_processing_millis: u64, compute_millis: u64) -> Self {
        Self {
            pre_processing_millis,
            compute_millis,
            // Betweenness has no iterations; its `ranIterations` column is a
            // placeholder zero, which the span leaves out.
            iterations: matches!(call, GdsCall::PageRank { .. }).then_some(ran_iterations),
            node_count: graph.vertices.len() as u64,
            relationship_count: graph.relationship_count(),
        }
    }
}

/// Record `shape` on a `query.execute` span, so a trace answers how big the
/// graph was and where the call's time went without the client's result row.
fn record_gds_run(span: &tracing::Span, shape: GdsRunShape) {
    span.record("hydradb.gds.pre_processing_millis", shape.pre_processing_millis);
    span.record("hydradb.gds.compute_millis", shape.compute_millis);
    span.record("hydradb.gds.node_count", shape.node_count);
    span.record("hydradb.gds.relationship_count", shape.relationship_count);
    if let Some(iterations) = shape.iterations {
        span.record("hydradb.gds.iterations", u64::from(iterations));
    }
}

/// GDS's `centralityDistribution`: the extremes, the mean and the upper
/// percentiles of the scores, each the nearest-rank value.
fn centrality_distribution(scores: &crate::engine::IndexOrderedFloats) -> QueryValue {
    let sorted = scores.as_slice();
    let percentile = |p: f64| -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
        sorted[rank.clamp(1, sorted.len()) - 1]
    };
    let mean = if sorted.is_empty() {
        0.0
    } else {
        sorted.iter().sum::<f64>() / sorted.len() as f64
    };
    let float = |value: f64| QueryValue::Float(QueryFloat(value));
    record(vec![
        ("min", float(sorted.first().copied().unwrap_or_default())),
        ("max", float(sorted.last().copied().unwrap_or_default())),
        ("mean", float(mean)),
        ("p50", float(percentile(50.0))),
        ("p90", float(percentile(90.0))),
        ("p99", float(percentile(99.0))),
        ("p999", float(percentile(99.9))),
    ])
}

/// Sources per betweenness kernel call, sized to the memory free now; a
/// graph one source does not fit is refused with the reason logged, so an
/// operator sees what it needed.
fn betweenness_batch(vertices: u64, edges: u64) -> Result<usize> {
    let available = crate::core::available_memory::available_memory_bytes();
    if available.is_none() {
        static UNBOUNDED: std::sync::Once = std::sync::Once::new();
        UNBOUNDED.call_once(|| {
            tracing::warn!("free memory is not readable here; betweenness runs its largest batch of sources without a memory bound");
        });
    }
    crate::sparse_kernel::betweenness_batch(vertices, edges, available).inspect_err(|error| {
        if let GraphError::AdmissionRejected { operation, actual, limit } = error {
            tracing::warn!(
                vertices,
                edges,
                operation,
                needed_bytes = actual,
                budget_bytes = limit,
                available_bytes = available,
                "betweenness refused: the graph and one source need more than three quarters of the memory available"
            );
        }
    })
}

/// Source ordinals for betweenness: every vertex when `size` reaches the
/// vertex count, otherwise `size` distinct ones drawn with `seed`, or with a
/// fresh seed when none is given, as GDS draws them.
///
/// The sources are yielded rather than listed: every vertex is a range, and
/// a sample is shuffled in whichever of [`dense_sample`] and
/// [`sparse_sample`] allocates less, which draw the same sources.
fn betweenness_sources(vertices: u64, size: u64, seed: Option<i64>) -> Box<dyn Iterator<Item = u64> + Send> {
    if size >= vertices {
        return Box::new(0..vertices);
    }
    let state = seed.map_or_else(
        || std::collections::hash_map::RandomState::new().build_hasher().finish(),
        |seed| seed as u64,
    );
    let sources = if dense_sample_is_smaller(vertices, size) {
        dense_sample(vertices, size, state)
    } else {
        sparse_sample(vertices, size, state)
    };
    Box::new(sources.into_iter())
}

/// The swaps of a partial Fisher-Yates shuffle of `0..vertices` over
/// splitmix64 from `state`: step `index` swaps position `index` with the
/// second position, drawn from `index..vertices`.
fn fisher_yates_swaps(vertices: u64, size: u64, mut state: u64) -> impl Iterator<Item = (u64, u64)> {
    (0..size).map(move |index| {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^= mixed >> 31;
        (index, index + mixed % (vertices - index))
    })
}

/// The shuffle over every ordinal listed: 8 bytes a vertex.
fn dense_sample(vertices: u64, size: u64, state: u64) -> Vec<u64> {
    let mut sources = (0..vertices).collect::<Vec<_>>();
    for (index, swap) in fisher_yates_swaps(vertices, size, state) {
        sources.swap(index as usize, swap as usize);
    }
    sources.truncate(size as usize);
    sources.sort_unstable();
    sources
}

/// The shuffle with `0..vertices` held sparsely: `displaced` maps each
/// position a swap has written to the ordinal it now holds, and every other
/// position holds its own index. Position `index` is never read again once
/// its step has drawn it, so the map never holds more than `size` entries.
fn sparse_sample(vertices: u64, size: u64, state: u64) -> Vec<u64> {
    let mut displaced = HashMap::<u64, u64>::with_capacity(size as usize);
    let mut sources = Vec::with_capacity(size as usize);
    for (index, swap) in fisher_yates_swaps(vertices, size, state) {
        sources.push(displaced.get(&swap).copied().unwrap_or(swap));
        let current = displaced.remove(&index).unwrap_or(index);
        if swap != index {
            displaced.insert(swap, current);
        }
    }
    sources.sort_unstable();
    sources
}

/// Whether [`dense_sample`]'s 8 bytes a vertex are no more than
/// [`sparse_sample`]'s 8 bytes a source plus its map. The map is sized for
/// `size` entries: std's hash table rounds that up to a power-of-two bucket
/// count at most 7/8 full (4 or 8 buckets below 8 entries), and each bucket
/// holds a 16-byte entry and a control byte. The crossover falls between
/// about a sixth and a third of the vertices, depending on the rounding.
fn dense_sample_is_smaller(vertices: u64, size: u64) -> bool {
    let buckets = match size {
        0..4 => 4,
        4..8 => 8,
        _ => (size.saturating_mul(8) / 7).next_power_of_two(),
    };
    vertices.saturating_mul(8) <= buckets.saturating_mul(17).saturating_add(size.saturating_mul(8))
}
