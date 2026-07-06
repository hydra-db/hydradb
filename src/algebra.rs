use std::collections::BTreeMap;

use crate::{
    validate_component, CommitResult, GraphEpoch, GraphError, Result, VertexId, VertexMetadata,
    VertexPropertyValue,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContext {
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<GraphEpoch>,
    pub result_window: QueryWindow,
    pub parameters: BTreeMap<String, VertexPropertyValue>,
    pub max_runtime_ms: Option<u64>,
}

impl QueryContext {
    pub fn new(cell_id: impl Into<String>, idempotency_key: impl Into<String>) -> Self {
        Self {
            cell_id: cell_id.into(),
            idempotency_key: idempotency_key.into(),
            read_epoch: None,
            result_window: QueryWindow::default(),
            parameters: BTreeMap::new(),
            max_runtime_ms: None,
        }
    }

    pub fn at_epoch(mut self, read_epoch: GraphEpoch) -> Self {
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
}

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
pub enum QueryOutput {
    Write(CommitResult),
    Mutation(QueryMutationResult),
    Vertices(Vec<VertexId>),
    Count(u64),
    Bool(bool),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    pub name: String,
}

impl QueryColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct QueryFloat(pub f64);

impl PartialEq for QueryFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for QueryFloat {}

impl PartialOrd for QueryFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueryFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

impl QueryRow {
    pub fn new(values: Vec<QueryValue>) -> Self {
        Self { values }
    }
}

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryMutationResult {
    pub matched_rows: u64,
    pub created_edges: u64,
    pub deleted_edges: u64,
    pub updated_vertices: u64,
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
            QueryStatement::CreateEdge { .. } | QueryStatement::CreateEdgeWithMetadata { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlan {
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<GraphEpoch>,
    pub result_window: QueryWindow,
    pub max_runtime_ms: Option<u64>,
    pub logical: LogicalQueryPlan,
    pub physical: PhysicalQueryPlan,
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
            PhysicalQueryPlan::WriteEdge { .. } | PhysicalQueryPlan::WriteEdgeWithMetadata { .. }
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
    } = plan
    {
        validate_query_vertex_metadata(src_metadata)?;
        validate_query_vertex_metadata(dst_metadata)?;
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
