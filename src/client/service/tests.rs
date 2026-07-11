use super::*;
use crate::{
    QueryColumn, QueryRow, QueryTransportScopeGrant, QueryValue,
    StaticQueryTransportScopeAuthorizer,
};
use async_trait::async_trait;
use std::sync::atomic::AtomicBool;

struct RevocableScopeAuthorizer {
    allowed: Arc<AtomicBool>,
}

impl QueryTransportScopeAuthorizer for RevocableScopeAuthorizer {
    fn authorize(
        &self,
        _principal: &QueryTransportPrincipal,
        _scope: &GraphScope,
        action: QueryTransportAction,
    ) -> bool {
        self.allowed.load(Ordering::SeqCst) && action == QueryTransportAction::Read
    }
}

struct TestClient {
    epoch: AtomicU64,
}

#[async_trait]
impl QueryCellClient for TestClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        if let Some(token) = context.cancellation_token {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
                _ = token.cancelled() => return Err(client_query_cancelled()),
            }
        }
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("value")],
            vec![QueryRow::new(vec![QueryValue::Count(1)])],
        ))
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        _cursor: Option<QueryCursorToken>,
        _page_size: usize,
    ) -> Result<QueryResultPage> {
        let result = self.execute_cypher_rows(context, query).await?;
        Ok(QueryResultPage::new(result.columns, result.rows, None))
    }

    async fn current_graph_epoch(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<GraphEpoch>> {
        Ok(Some(self.epoch.load(Ordering::Relaxed)))
    }
}

fn target() -> ClientQueryTarget {
    ClientQueryTarget::new(GraphScope::default(), "cell-a").unwrap()
}

fn service() -> ClientQueryService {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    ClientQueryService::new(
        Arc::new(TestClient {
            epoch: AtomicU64::new(7),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer)),
    )
    .unwrap()
}

#[test]
fn bookmarks_round_trip_without_scope_ambiguity() {
    let scope = GraphScope::new(
        NamespacePath::new([
            NamespaceId::new("tenant").unwrap(),
            NamespaceId::new("subtenant").unwrap(),
        ])
        .unwrap(),
        GraphId::new("social").unwrap(),
    );
    let bookmark = ClientBookmark::new(ClientQueryTarget::new(scope, "cell-a").unwrap(), 42);
    assert_eq!(ClientBookmark::parse(&bookmark.encode()).unwrap(), bookmark);
}

#[tokio::test]
async fn service_authenticates_authorizes_and_returns_epoch_bookmark() {
    let service = service();
    let session = service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .unwrap();
    let response = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(target(), "query-1", "MATCH (n {id: 1}) RETURN n.id"),
        )
        .await
        .unwrap();
    assert_eq!(response.result.rows.len(), 1);
    assert_eq!(response.bookmark.unwrap().epoch, 7);
}

#[tokio::test]
async fn prepared_pages_reauthorize_after_scope_grant_revocation() {
    let allowed = Arc::new(AtomicBool::new(true));
    let service = ClientQueryService::new(
        Arc::new(TestClient {
            epoch: AtomicU64::new(7),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(RevocableScopeAuthorizer {
                allowed: Arc::clone(&allowed),
            })),
    )
    .unwrap();
    let session = service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .unwrap();
    let prepared = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-revoked-cursor",
                "MATCH (n {id: 1}) RETURN n.id",
            ),
            1,
        )
        .await
        .unwrap();

    allowed.store(false, Ordering::SeqCst);
    let error = service
        .execute_prepared_page(&session, prepared, Some(QueryCursorToken::new(1)), 1)
        .await
        .unwrap_err();
    assert!(matches!(error, GraphError::GraphScopeAccessDenied { .. }));
    assert_eq!(service.active_query_count().await, 0);
}

#[tokio::test]
async fn service_rejects_cross_target_and_future_bookmarks() {
    let service = service();
    let session = service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .unwrap();
    let future = ClientBookmark::new(target(), 8);
    let error = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(target(), "query-2", "MATCH (n {id: 1}) RETURN n.id")
                .after_bookmark(future),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, GraphError::SnapshotAhead { .. }));
}

#[tokio::test]
async fn service_cancels_only_an_authorized_active_query() {
    let service = service();
    let session = service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .unwrap();
    let query = {
        let service = service.clone();
        let session = session.clone();
        tokio::spawn(async move {
            service
                .execute_rows(
                    &session,
                    ClientQueryRequest::new(
                        target(),
                        "query-cancel",
                        "MATCH (n {id: 1}) RETURN n.id",
                    ),
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while service.active_query_count().await == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    service
        .cancel(&session, &GraphScope::default(), "query-cancel")
        .await
        .unwrap();
    assert!(query
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("client_query_cancelled"));
}

#[tokio::test]
async fn dropping_execution_future_cleans_active_query_lifecycle() {
    let service = service();
    let session = service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .unwrap();
    let query = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .execute_rows(
                    &session,
                    ClientQueryRequest::new(
                        target(),
                        "query-drop",
                        "MATCH (n {id: 1}) RETURN n.id",
                    ),
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while service.active_query_count().await == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    query.abort();
    let _ = query.await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while service.active_query_count().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn service_enforces_server_runtime_ceiling_and_cleans_timed_out_queries() {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(TestClient {
            epoch: AtomicU64::new(7),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_query_runtime_ms(5),
    )
    .unwrap();
    let session = service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .unwrap();

    let oversized = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-oversized-timeout",
                "MATCH (n {id: 1}) RETURN n.id",
            )
            .with_timeout_ms(6),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        oversized,
        GraphError::AdmissionRejected {
            operation: "client_query_runtime_ms",
            actual: 6,
            limit: 5,
        }
    ));

    let timed_out = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-server-timeout",
                "MATCH (n {id: 1}) RETURN n.id",
            ),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        timed_out,
        GraphError::QueryTimeout {
            operation: "client_query_runtime",
            limit_ms: 5,
            ..
        }
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.active_query_count().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

struct CancellationIgnoringClient;

#[async_trait]
impl QueryCellClient for CancellationIgnoringClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok(QueryResultSet::new(Vec::new(), Vec::new()))
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        query: &str,
        _cursor: Option<QueryCursorToken>,
        _page_size: usize,
    ) -> Result<QueryResultPage> {
        let result = self.execute_cypher_rows(context, query).await?;
        Ok(QueryResultPage::new(result.columns, result.rows, None))
    }

    async fn current_graph_epoch(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<GraphEpoch>> {
        Ok(Some(7))
    }
}

#[tokio::test]
async fn timeout_keeps_admission_owned_until_non_cooperative_work_stops() {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(CancellationIgnoringClient),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_concurrent_queries(1)
            .with_max_query_runtime_ms(5),
    )
    .unwrap();
    let session = service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
        )
        .unwrap();

    let started = std::time::Instant::now();
    let error = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-non-cooperative-timeout",
                "MATCH (n {id: 1}) RETURN n.id",
            ),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::QueryTimeout {
            operation: "client_query_runtime",
            ..
        }
    ));
    assert!(started.elapsed() >= Duration::from_millis(25));
    assert_eq!(service.active_query_count().await, 0);
}
