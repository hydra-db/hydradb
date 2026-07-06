use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::join_all;

use super::{RoutedPhase0Cluster, ShardPlacement};
use crate::{
    validate_component, GraphError, QueryContext, QueryCursorToken, QueryResultPage,
    QueryResultSet, Result,
};

#[async_trait]
pub trait QueryCellClient: Send + Sync {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet>;

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryPageRequest {
    pub context: QueryContext,
    pub cursor: Option<QueryCursorToken>,
}

impl DistributedQueryPageRequest {
    pub fn new(context: QueryContext, cursor: Option<QueryCursorToken>) -> Self {
        Self { context, cursor }
    }
}

pub struct DistributedQueryCoordinator {
    placement: ShardPlacement,
    clients: BTreeMap<String, Arc<dyn QueryCellClient>>,
}

impl DistributedQueryCoordinator {
    pub fn new(placement: ShardPlacement) -> Self {
        Self {
            placement,
            clients: BTreeMap::new(),
        }
    }

    pub fn register_client(
        &mut self,
        node_id: impl Into<String>,
        client: Arc<dyn QueryCellClient>,
    ) -> Result<()> {
        let node_id = node_id.into();
        validate_component("node_id", &node_id)?;
        self.clients.insert(node_id, client);
        Ok(())
    }

    pub fn with_client(
        mut self,
        node_id: impl Into<String>,
        client: Arc<dyn QueryCellClient>,
    ) -> Result<Self> {
        self.register_client(node_id, client)?;
        Ok(self)
    }

    pub async fn execute_cypher_rows_many(
        &self,
        contexts: impl IntoIterator<Item = QueryContext>,
        query: &str,
    ) -> Result<BTreeMap<String, QueryResultSet>> {
        let query = Arc::new(query.to_string());
        let mut seen = BTreeSet::new();
        let mut jobs = Vec::new();
        for context in contexts {
            let cell_id = checked_unique_cell(&mut seen, &context.cell_id)?;
            let client = self.client_for_cell(&cell_id)?;
            let query = Arc::clone(&query);
            jobs.push(async move {
                let result = client.execute_cypher_rows(context, query.as_str()).await;
                (cell_id, result)
            });
        }

        let mut result_sets = BTreeMap::new();
        for (cell_id, result) in join_all(jobs).await {
            result_sets.insert(cell_id, result?);
        }
        Ok(result_sets)
    }

    pub async fn execute_cypher_rows_pages(
        &self,
        requests: impl IntoIterator<Item = DistributedQueryPageRequest>,
        query: &str,
        page_size: usize,
    ) -> Result<BTreeMap<String, QueryResultPage>> {
        let query = Arc::new(query.to_string());
        let mut seen = BTreeSet::new();
        let mut jobs = Vec::new();
        for request in requests {
            let cell_id = checked_unique_cell(&mut seen, &request.context.cell_id)?;
            let client = self.client_for_cell(&cell_id)?;
            let query = Arc::clone(&query);
            jobs.push(async move {
                let result = client
                    .execute_cypher_rows_page(
                        request.context,
                        query.as_str(),
                        request.cursor,
                        page_size,
                    )
                    .await;
                (cell_id, result)
            });
        }

        let mut pages = BTreeMap::new();
        for (cell_id, result) in join_all(jobs).await {
            pages.insert(cell_id, result?);
        }
        Ok(pages)
    }

    fn client_for_cell(&self, cell_id: &str) -> Result<Arc<dyn QueryCellClient>> {
        let owner = self.placement.owner(cell_id)?;
        self.clients
            .get(owner)
            .cloned()
            .ok_or_else(|| GraphError::CorruptValue {
                key: format!("query/node/{owner}"),
                reason: format!("missing query client for owner node {owner}"),
            })
    }
}

#[async_trait]
impl QueryCellClient for RoutedPhase0Cluster {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        RoutedPhase0Cluster::execute_cypher_rows(self, context, query).await
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        RoutedPhase0Cluster::execute_cypher_rows_page(self, context, query, cursor, page_size).await
    }
}

fn checked_unique_cell(seen: &mut BTreeSet<String>, cell_id: &str) -> Result<String> {
    validate_component("cell_id", cell_id)?;
    if !seen.insert(cell_id.to_string()) {
        return Err(GraphError::CorruptValue {
            key: format!("query/cell/{cell_id}"),
            reason: "duplicate cell in distributed query request".to_string(),
        });
    }
    Ok(cell_id.to_string())
}
