use datafusion::error::DataFusionError;
use hydradb_graph_plan::GraphPlanError;

pub type DfResult<T> = Result<T, DfError>;

#[derive(Debug, thiserror::Error)]
pub enum DfError {
    #[error("unsupported Cypher form: {0}")]
    Unsupported(String),
    #[error("invalid Cypher query: {0}")]
    Invalid(String),
    #[error(transparent)]
    GraphPlan(#[from] GraphPlanError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("missing query parameter `${0}`")]
    MissingParameter(String),
}

pub(crate) fn unsupported<T>(message: impl Into<String>) -> DfResult<T> {
    Err(DfError::Unsupported(message.into()))
}
