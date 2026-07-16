use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

use crate::{
    validate_component, CommitResult, EdgeMetadata, GraphError, GraphScope, QueryFloat,
    RelationshipId, Result, TopologySequence, VertexId, VertexMetadata, VertexPropertyValue,
};

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryParameterValue {
    Scalar(VertexPropertyValue),
    List(Vec<QueryParameterValue>),
    Map(BTreeMap<String, QueryParameterValue>),
}

impl From<VertexPropertyValue> for QueryParameterValue {
    fn from(value: VertexPropertyValue) -> Self {
        Self::Scalar(value)
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryBatchEdge {
    pub src: VertexId,
    pub dst: VertexId,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchVertex {
    pub vertex: VertexId,
    pub metadata: VertexMetadata,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchRelationship {
    pub src: VertexId,
    pub dst: VertexId,
    pub metadata: EdgeMetadata,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchRelationshipMerge {
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
    pub metadata: EdgeMetadata,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryBatchOperation {
    OutNeighbors {
        edge_type: String,
        sources: Vec<VertexId>,
        source_column: QueryColumn,
        destination_column: QueryColumn,
    },
    CreateEdges {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
    },
    DeleteEdges {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
    },
    DeleteVertices {
        vertices: Vec<VertexId>,
        detach: bool,
    },
    DeleteRelationshipsByProperty {
        edge_type: String,
        property: String,
        values: Vec<VertexPropertyValue>,
    },
    CreateEdgesBetweenLabeledVertices {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
        source_label: String,
        destination_label: String,
    },
    UpsertVertices {
        vertices: Vec<QueryBatchVertex>,
    },
    CreateRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationship>,
        source_label: String,
        destination_label: String,
    },
    MergeRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationshipMerge>,
        source_label: String,
        destination_label: String,
    },
}

impl QueryBatchOperation {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Self::CreateEdges { .. }
                | Self::CreateEdgesBetweenLabeledVertices { .. }
                | Self::DeleteEdges { .. }
                | Self::DeleteVertices { .. }
                | Self::DeleteRelationshipsByProperty { .. }
                | Self::UpsertVertices { .. }
                | Self::CreateRelationshipsBetweenLabeledVertices { .. }
                | Self::MergeRelationshipsBetweenLabeledVertices { .. }
        )
    }

    pub fn len(&self) -> usize {
        match self {
            Self::OutNeighbors { sources, .. } => sources.len(),
            Self::CreateEdges { edges, .. }
            | Self::CreateEdgesBetweenLabeledVertices { edges, .. }
            | Self::DeleteEdges { edges, .. } => edges.len(),
            Self::DeleteVertices { vertices, .. } => vertices.len(),
            Self::DeleteRelationshipsByProperty { values, .. } => values.len(),
            Self::UpsertVertices { vertices } => vertices.len(),
            Self::CreateRelationshipsBetweenLabeledVertices { relationships, .. } => {
                relationships.len()
            }
            Self::MergeRelationshipsBetweenLabeledVertices { relationships, .. } => {
                relationships.len()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContext {
    pub scope: GraphScope,
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<TopologySequence>,
    pub result_window: QueryWindow,
    pub parameters: BTreeMap<String, VertexPropertyValue>,
    pub max_runtime_ms: Option<u64>,
    #[cfg_attr(feature = "query-transport", serde(skip, default))]
    pub cancellation_token: Option<QueryCancellationToken>,
    #[cfg(feature = "opencypher")]
    #[cfg_attr(feature = "query-transport", serde(skip, default))]
    validated_read: Option<ValidatedQueryRead>,
}

impl QueryContext {
    pub fn new(cell_id: impl Into<String>, idempotency_key: impl Into<String>) -> Self {
        Self {
            scope: GraphScope::default(),
            cell_id: cell_id.into(),
            idempotency_key: idempotency_key.into(),
            read_epoch: None,
            result_window: QueryWindow::default(),
            parameters: BTreeMap::new(),
            max_runtime_ms: None,
            cancellation_token: None,
            #[cfg(feature = "opencypher")]
            validated_read: None,
        }
    }

    pub fn in_scope(mut self, scope: GraphScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn at_epoch(mut self, read_epoch: TopologySequence) -> Self {
        self.read_epoch = Some(read_epoch);
        self
    }

    pub fn with_result_window(mut self, skip: u64, limit: Option<usize>) -> Self {
        self.result_window = QueryWindow { skip, limit };
        self
    }

    pub fn with_parameter(mut self, name: impl Into<String>, value: VertexPropertyValue) -> Self {
        self.parameters.insert(name.into(), value);
        self
    }

    pub fn with_parameters(
        mut self,
        parameters: impl IntoIterator<Item = (String, VertexPropertyValue)>,
    ) -> Self {
        self.parameters.extend(parameters);
        self
    }

    pub fn with_timeout_ms(mut self, max_runtime_ms: u64) -> Self {
        self.max_runtime_ms = Some(max_runtime_ms);
        self
    }

    pub fn with_cancellation_token(mut self, token: QueryCancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn with_validated_storage_read_epoch(
        mut self,
        read_epoch: TopologySequence,
        storage_sequence: crate::StorageSequence,
    ) -> Self {
        self.read_epoch = Some(read_epoch);
        self.validated_read = Some(ValidatedQueryRead {
            cell_id: self.cell_id.clone(),
            read_epoch,
            retention: ValidatedQueryReadRetention::StorageSnapshot(storage_sequence),
        });
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn validated_read_epoch(&self) -> Option<TopologySequence> {
        self.validated_read.as_ref().and_then(|validated| {
            let _retention = &validated.retention;
            (validated.cell_id == self.cell_id && self.read_epoch == Some(validated.read_epoch))
                .then_some(validated.read_epoch)
        })
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedQueryRead {
    cell_id: String,
    read_epoch: TopologySequence,
    retention: ValidatedQueryReadRetention,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum ValidatedQueryReadRetention {
    StorageSnapshot(crate::StorageSequence),
}

#[derive(Clone, Debug, Default)]
pub struct QueryCancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl QueryCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

impl PartialEq for QueryCancellationToken {
    fn eq(&self, other: &Self) -> bool {
        self.is_cancelled() == other.is_cancelled()
    }
}

impl Eq for QueryCancellationToken {}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryWindow {
    pub skip: u64,
    pub limit: Option<usize>,
}

impl QueryWindow {
    pub fn is_default(self) -> bool {
        self.skip == 0 && self.limit.is_none()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCardinalityStatsKind {
    EdgeType {
        edge_type: String,
    },
    VertexLabel {
        label: String,
    },
    VertexProperty {
        property: String,
        value: VertexPropertyValue,
    },
    EdgeProperty {
        edge_type: String,
        property: String,
        value: VertexPropertyValue,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCardinalityStatsRefresh {
    pub cell_id: String,
    pub read_epoch: TopologySequence,
    pub kind: QueryCardinalityStatsKind,
    pub count: u64,
    pub stats: QueryStatsRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatsRecord {
    pub count: u64,
    pub read_epoch: TopologySequence,
    pub refreshed_at_ms: u64,
    pub distinct_values: u64,
    pub total_values: u64,
    pub most_common_count: u64,
}

impl QueryStatsRecord {
    pub fn point_count(count: u64, read_epoch: TopologySequence, refreshed_at_ms: u64) -> Self {
        Self {
            count,
            read_epoch,
            refreshed_at_ms,
            distinct_values: 1,
            total_values: count,
            most_common_count: count,
        }
    }

    pub fn histogram(
        count: u64,
        read_epoch: TopologySequence,
        refreshed_at_ms: u64,
        distinct_values: u64,
        most_common_count: u64,
    ) -> Self {
        Self {
            count,
            read_epoch,
            refreshed_at_ms,
            distinct_values,
            total_values: count,
            most_common_count,
        }
    }

    pub fn is_stale_at(&self, current_epoch: TopologySequence, now_ms: u64) -> bool {
        self.read_epoch < current_epoch || self.refreshed_at_ms > now_ms
    }

    pub fn equality_estimate(&self) -> u64 {
        if self.distinct_values == 0 {
            return self.count.max(1);
        }
        self.count
            .checked_div(self.distinct_values)
            .unwrap_or(self.count)
            .max(1)
            .min(self.most_common_count.max(1))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatsHistogramRefresh {
    pub cell_id: String,
    pub read_epoch: TopologySequence,
    pub property: String,
    pub edge_type: Option<String>,
    pub stats: QueryStatsRecord,
    pub buckets: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatsRefreshSpec {
    pub cell_id: String,
    pub kind: QueryStatsRefreshKind,
}

impl QueryStatsRefreshSpec {
    pub fn new(cell_id: impl Into<String>, kind: impl Into<QueryStatsRefreshKind>) -> Self {
        Self {
            cell_id: cell_id.into(),
            kind: kind.into(),
        }
    }

    pub fn vertex_property_histogram(
        cell_id: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        Self {
            cell_id: cell_id.into(),
            kind: QueryStatsRefreshKind::VertexPropertyHistogram {
                property: property.into(),
            },
        }
    }

    pub fn edge_property_histogram(
        cell_id: impl Into<String>,
        edge_type: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        Self {
            cell_id: cell_id.into(),
            kind: QueryStatsRefreshKind::EdgePropertyHistogram {
                edge_type: edge_type.into(),
                property: property.into(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatsRefreshKind {
    Cardinality(QueryCardinalityStatsKind),
    VertexPropertyHistogram { property: String },
    EdgePropertyHistogram { edge_type: String, property: String },
}

impl From<QueryCardinalityStatsKind> for QueryStatsRefreshKind {
    fn from(value: QueryCardinalityStatsKind) -> Self {
        Self::Cardinality(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatsRefreshResult {
    Cardinality(QueryCardinalityStatsRefresh),
    Histogram(QueryStatsHistogramRefresh),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutput {
    Write(CommitResult),
    Mutation(QueryMutationResult),
    Rows(QueryResultSet),
    Vertices(Vec<VertexId>),
    Count(u64),
    Bool(bool),
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    pub name: String,
}

impl QueryColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryValue {
    Null,
    VertexId(VertexId),
    Count(u64),
    Bool(bool),
    Float(QueryFloat),
    Property(VertexPropertyValue),
    List(Vec<QueryValue>),
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

impl QueryRow {
    pub fn new(values: Vec<QueryValue>) -> Self {
        Self { values }
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResultSet {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
}

impl QueryResultSet {
    pub fn new(columns: Vec<QueryColumn>, rows: Vec<QueryRow>) -> Self {
        Self { columns, rows }
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryCursorToken {
    pub offset: u64,
}

impl QueryCursorToken {
    pub fn new(offset: u64) -> Self {
        Self { offset }
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResultPage {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
    pub next_cursor: Option<QueryCursorToken>,
}

impl QueryResultPage {
    pub fn new(
        columns: Vec<QueryColumn>,
        rows: Vec<QueryRow>,
        next_cursor: Option<QueryCursorToken>,
    ) -> Self {
        Self {
            columns,
            rows,
            next_cursor,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryMutationResult {
    pub matched_rows: u64,
    pub created_edges: u64,
    pub created_relationships: u64,
    pub deleted_edges: u64,
    pub updated_vertices: u64,
    pub updated_relationships: u64,
    pub noops: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatement {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    CreateEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    CreateEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    MatchOut {
        edge_type: String,
        src: VertexId,
        return_count: bool,
    },
    MatchOutFiltered {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchReachable {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

impl QueryStatement {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            QueryStatement::CreateEdge { .. }
                | QueryStatement::CreateEdgeWithMetadata { .. }
                | QueryStatement::CreateEdgeWithFullMetadata { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlan {
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<TopologySequence>,
    pub result_window: QueryWindow,
    pub max_runtime_ms: Option<u64>,
    pub logical: LogicalQueryPlan,
    pub physical: PhysicalQueryPlan,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlan {
    pub cell_id: String,
    pub read_epoch: TopologySequence,
    pub columns: Vec<QueryColumn>,
    pub groups: Vec<RowQueryPlanGroup>,
    pub union_all: bool,
    pub union_arms: Vec<RowQueryPlan>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlanGroup {
    pub optional: bool,
    pub has_predicate: bool,
    pub estimated_cardinality: u64,
    pub patterns: Vec<RowQueryPlanPattern>,
    pub optimizer_passes: Vec<RowQueryOptimizerPass>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlanPattern {
    pub original_index: usize,
    pub access: RowQueryAccess,
    pub estimated_cardinality: u64,
    pub bindings: Vec<String>,
    pub optimizer_passes: Vec<RowQueryOptimizerPass>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowQueryAccess {
    VertexIdSeek,
    VertexPropertyIndex {
        property: String,
    },
    VertexLabelScan {
        label: String,
    },
    AllVertexScan,
    BoundOutExpand {
        edge_type: String,
    },
    BoundInExpand {
        edge_type: String,
    },
    ExpandInto {
        edge_type: String,
    },
    EdgePropertyIndex {
        edge_type: String,
        property: String,
    },
    FullEdgeScan {
        edge_type: String,
    },
    VariableLengthExpand {
        edge_type: String,
        min_hops: u8,
        max_hops: u8,
    },
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowQueryOptimizerPass {
    UtilizeVertexIndex,
    UtilizeEdgeIndex,
    CostBasedLabelScan,
    ConnectivityOrder,
    JoinOrder,
    ExpandInto,
    GraphKernel,
    ReverseExpand,
    FullScanFallback,
    PreserveOptionalBoundary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalQueryPlan {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    CreateEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    CreateEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    MatchOut {
        edge_type: String,
        src: VertexId,
        return_count: bool,
    },
    MatchOutFiltered {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchReachable {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalQueryPlan {
    WriteEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    WriteEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    WriteEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    OutDegreeCounter {
        edge_type: String,
        src: VertexId,
    },
    OutNeighbors {
        edge_type: String,
        src: VertexId,
    },
    EdgeExistsToCount {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    EdgeExistsToVertices {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    EdgeExistsToBool {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    ReachableVertices {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

impl QueryPlan {
    pub fn is_write(&self) -> bool {
        matches!(
            self.physical,
            PhysicalQueryPlan::WriteEdge { .. }
                | PhysicalQueryPlan::WriteEdgeWithMetadata { .. }
                | PhysicalQueryPlan::WriteEdgeWithFullMetadata { .. }
        )
    }

    pub fn returns_vertices(&self) -> bool {
        matches!(
            self.physical,
            PhysicalQueryPlan::OutNeighbors { .. }
                | PhysicalQueryPlan::EdgeExistsToVertices { .. }
                | PhysicalQueryPlan::ReachableVertices {
                    return_count: false,
                    ..
                }
        )
    }

    pub fn validate_for_execution(&self) -> Result<()> {
        validate_component("cell_id", &self.cell_id)?;
        validate_logical_plan(&self.logical)?;

        let expected_physical = physical_plan(&self.logical);
        if self.physical != expected_physical {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphQuery",
                feature: "physical query plan does not match logical query plan".to_string(),
            });
        }

        if self.is_write() {
            validate_component("idempotency_key", &self.idempotency_key)?;
            if self.read_epoch.is_some() {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "GraphQuery",
                    feature: "snapshot epochs are only valid for read queries".to_string(),
                });
            }
        }
        if !self.result_window.is_default() && !self.returns_vertices() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "GraphQuery",
                feature: "result windows are only valid for vertex-returning read queries"
                    .to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(context: &QueryContext, statement: &QueryStatement) -> Result<QueryPlan> {
        validate_component("cell_id", &context.cell_id)?;

        let logical = logical_plan(statement)?;
        validate_logical_plan(&logical)?;
        let physical = physical_plan(&logical);
        let plan = QueryPlan {
            cell_id: context.cell_id.clone(),
            idempotency_key: context.idempotency_key.clone(),
            read_epoch: context.read_epoch,
            result_window: context.result_window,
            max_runtime_ms: context.max_runtime_ms,
            logical,
            physical,
        };
        plan.validate_for_execution()?;
        Ok(plan)
    }
}

fn logical_plan(statement: &QueryStatement) -> Result<LogicalQueryPlan> {
    Ok(match statement {
        QueryStatement::CreateEdge {
            edge_type,
            src,
            dst,
        } => LogicalQueryPlan::CreateEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        QueryStatement::CreateEdgeWithMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
        } => LogicalQueryPlan::CreateEdgeWithMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
        },
        QueryStatement::CreateEdgeWithFullMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
            edge_metadata,
        } => LogicalQueryPlan::CreateEdgeWithFullMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
            edge_metadata: edge_metadata.clone(),
        },
        QueryStatement::MatchOut {
            edge_type,
            src,
            return_count,
        } => LogicalQueryPlan::MatchOut {
            edge_type: edge_type.clone(),
            src: *src,
            return_count: *return_count,
        },
        QueryStatement::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count,
        } => LogicalQueryPlan::MatchOutFiltered {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            return_count: *return_count,
        },
        QueryStatement::MatchEdge {
            edge_type,
            src,
            dst,
            return_count,
        } => LogicalQueryPlan::MatchEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            return_count: *return_count,
        },
        QueryStatement::MatchReachable {
            edge_type,
            src,
            min_hops,
            max_hops,
            return_count,
        } => LogicalQueryPlan::MatchReachable {
            edge_type: edge_type.clone(),
            src: *src,
            min_hops: *min_hops,
            max_hops: *max_hops,
            return_count: *return_count,
        },
    })
}

fn validate_logical_plan(plan: &LogicalQueryPlan) -> Result<()> {
    match plan {
        LogicalQueryPlan::CreateEdge { edge_type, .. }
        | LogicalQueryPlan::CreateEdgeWithMetadata { edge_type, .. }
        | LogicalQueryPlan::CreateEdgeWithFullMetadata { edge_type, .. }
        | LogicalQueryPlan::MatchOut { edge_type, .. }
        | LogicalQueryPlan::MatchOutFiltered { edge_type, .. }
        | LogicalQueryPlan::MatchEdge { edge_type, .. }
        | LogicalQueryPlan::MatchReachable { edge_type, .. } => {
            validate_component("edge_type", edge_type)?;
        }
    }
    if let LogicalQueryPlan::CreateEdgeWithMetadata {
        src_metadata,
        dst_metadata,
        ..
    }
    | LogicalQueryPlan::CreateEdgeWithFullMetadata {
        src_metadata,
        dst_metadata,
        ..
    } = plan
    {
        validate_query_vertex_metadata(src_metadata)?;
        validate_query_vertex_metadata(dst_metadata)?;
    }
    if let LogicalQueryPlan::CreateEdgeWithFullMetadata { edge_metadata, .. } = plan {
        validate_query_edge_metadata(edge_metadata)?;
    }
    Ok(())
}

fn physical_plan(logical: &LogicalQueryPlan) -> PhysicalQueryPlan {
    match logical {
        LogicalQueryPlan::CreateEdge {
            edge_type,
            src,
            dst,
        } => PhysicalQueryPlan::WriteEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::CreateEdgeWithMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
        } => PhysicalQueryPlan::WriteEdgeWithMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
        },
        LogicalQueryPlan::CreateEdgeWithFullMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
            edge_metadata,
        } => PhysicalQueryPlan::WriteEdgeWithFullMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
            edge_metadata: edge_metadata.clone(),
        },
        LogicalQueryPlan::MatchOut {
            edge_type,
            src,
            return_count: true,
        } => PhysicalQueryPlan::OutDegreeCounter {
            edge_type: edge_type.clone(),
            src: *src,
        },
        LogicalQueryPlan::MatchOut {
            edge_type,
            src,
            return_count: false,
        } => PhysicalQueryPlan::OutNeighbors {
            edge_type: edge_type.clone(),
            src: *src,
        },
        LogicalQueryPlan::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count: true,
        }
        | LogicalQueryPlan::MatchEdge {
            edge_type,
            src,
            dst,
            return_count: true,
        } => PhysicalQueryPlan::EdgeExistsToCount {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count: false,
        } => PhysicalQueryPlan::EdgeExistsToVertices {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchEdge {
            edge_type,
            src,
            dst,
            return_count: false,
        } => PhysicalQueryPlan::EdgeExistsToBool {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchReachable {
            edge_type,
            src,
            min_hops,
            max_hops,
            return_count,
        } => PhysicalQueryPlan::ReachableVertices {
            edge_type: edge_type.clone(),
            src: *src,
            min_hops: *min_hops,
            max_hops: *max_hops,
            return_count: *return_count,
        },
    }
}

fn validate_query_vertex_metadata(metadata: &VertexMetadata) -> Result<()> {
    for label in &metadata.labels {
        validate_component("label", label)?;
    }
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}

fn validate_query_edge_metadata(metadata: &EdgeMetadata) -> Result<()> {
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}
