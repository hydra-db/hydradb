use std::sync::Arc;

use async_trait::async_trait;
use slatedb_graph_kernel::{
    BoltRoutingServer, BoltServerConfig, ClientBoltServer, ClientQueryService,
    ClientQueryServiceConfig, ClientQueryTarget, GraphScope, QueryCellClient, QueryColumn,
    QueryContext, QueryCursorToken, QueryResultPage, QueryResultSet, QueryRow,
    QueryTransportScopeGrant, QueryValue, Result, StaticClientDatabaseResolver,
    StaticQueryTransportScopeAuthorizer,
};

struct CompatibilityQueryClient;

#[async_trait]
impl QueryCellClient for CompatibilityQueryClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("answer")],
            vec![QueryRow::new(vec![QueryValue::Count(42)])],
        ))
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        _cursor: Option<QueryCursorToken>,
        _page_size: usize,
    ) -> Result<QueryResultPage> {
        let rows = self.execute_cypher_rows(context, query).await?;
        Ok(QueryResultPage::new(rows.columns, rows.rows, None))
    }

    async fn current_graph_epoch(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<u64>> {
        Ok(Some(42))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("BOLT_COMPAT_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:17687".to_string())
        .parse::<std::net::SocketAddr>()
        .map_err(|err| slatedb_graph_kernel::GraphError::CorruptValue {
            key: "BOLT_COMPAT_ADDR".to_string(),
            reason: err.to_string(),
        })?;
    let scope = GraphScope::default();
    let authorizer = StaticQueryTransportScopeAuthorizer::new().with_bearer_grant(
        "bolt-secret",
        QueryTransportScopeGrant::read_graph(scope.clone()),
    )?;
    let service = ClientQueryService::new(
        Arc::new(CompatibilityQueryClient),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("bolt-secret")
            .with_scope_authorizer(Arc::new(authorizer)),
    )?;
    let resolver = StaticClientDatabaseResolver::single(
        "default",
        ClientQueryTarget::new(scope, "compat-cell")?,
    )?;
    let advertised = addr.to_string();
    let mut config = BoltServerConfig::new(Arc::new(resolver)).insecure_allow_plaintext();
    for role in ["ROUTE", "READ", "WRITE"] {
        config = config.with_routing_server(BoltRoutingServer::new(role, [advertised.clone()])?);
    }
    let server = ClientBoltServer::bind(addr, service, config).await?;
    println!("BOLT_READY={}", server.local_addr());
    std::future::pending::<()>().await;
    #[allow(unreachable_code)]
    server.stop().await
}
