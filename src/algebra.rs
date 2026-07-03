use crate::{CommitResult, VertexId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContext {
    pub cell_id: String,
    pub idempotency_key: String,
}

impl QueryContext {
    pub fn new(cell_id: impl Into<String>, idempotency_key: impl Into<String>) -> Self {
        Self {
            cell_id: cell_id.into(),
            idempotency_key: idempotency_key.into(),
        }
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
