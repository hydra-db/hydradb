use crate::{validate_component, CommitResult, GraphEpoch, GraphError, Result, VertexId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContext {
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<GraphEpoch>,
}

impl QueryContext {
    pub fn new(cell_id: impl Into<String>, idempotency_key: impl Into<String>) -> Self {
        Self {
            cell_id: cell_id.into(),
            idempotency_key: idempotency_key.into(),
            read_epoch: None,
        }
    }

    pub fn at_epoch(mut self, read_epoch: GraphEpoch) -> Self {
        self.read_epoch = Some(read_epoch);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutput {
    Write(CommitResult),
    Vertices(Vec<VertexId>),
    Count(u64),
    Bool(bool),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatement {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
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
        matches!(self, QueryStatement::CreateEdge { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlan {
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<GraphEpoch>,
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
        matches!(self.physical, PhysicalQueryPlan::WriteEdge { .. })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(context: &QueryContext, statement: &QueryStatement) -> Result<QueryPlan> {
        validate_component("cell_id", &context.cell_id)?;
        if statement.is_write() {
            validate_component("idempotency_key", &context.idempotency_key)?;
            if context.read_epoch.is_some() {
                return Err(GraphError::UnsupportedQuery {
                    dialect: "GraphQuery",
                    feature: "snapshot epochs are only valid for read queries".to_string(),
                });
            }
        }

        let logical = logical_plan(statement)?;
        validate_logical_plan(&logical)?;
        let physical = physical_plan(&logical);
        Ok(QueryPlan {
            cell_id: context.cell_id.clone(),
            idempotency_key: context.idempotency_key.clone(),
            read_epoch: context.read_epoch,
            logical,
            physical,
        })
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
        | LogicalQueryPlan::MatchOut { edge_type, .. }
        | LogicalQueryPlan::MatchOutFiltered { edge_type, .. }
        | LogicalQueryPlan::MatchEdge { edge_type, .. }
        | LogicalQueryPlan::MatchReachable { edge_type, .. } => {
            validate_component("edge_type", edge_type)?;
        }
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
