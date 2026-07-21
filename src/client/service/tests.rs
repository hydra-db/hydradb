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

struct CursorTestClient {
    executions: Arc<AtomicU64>,
}

#[test]
fn hierarchical_database_resolver_maps_each_collection_to_a_native_scope() {
    let root = GraphScope::new(
        NamespacePath::root(NamespaceId::new("hydradb").unwrap()),
        GraphId::new("knowledge").unwrap(),
    );
    let resolver = HierarchicalClientDatabaseResolver::new(
        "default",
        ClientQueryTarget::new(root.clone(), "cell-0").unwrap(),
    )
    .unwrap();
    let database = resolver
        .scoped_database_name("tenant-a", Some("collection-b"))
        .unwrap();
    assert_eq!(database, "default.scope1.dGVuYW50LWE.Y29sbGVjdGlvbi1i");
    let target = resolver.resolve_database(Some(&database)).unwrap();
    assert_eq!(
        target.scope.namespace.to_string(),
        "hydradb/dGVuYW50LWE/Y29sbGVjdGlvbi1i"
    );
    assert_eq!(target.scope.graph_id.as_str(), "knowledge");
    assert_eq!(target.cell_id, "cell-0");
    assert_eq!(resolver.resolve_database(None).unwrap().scope, root);
}

#[test]
fn hierarchical_database_resolver_rejects_malformed_or_unsafe_scopes() {
    let resolver = HierarchicalClientDatabaseResolver::new(
        "default",
        ClientQueryTarget::new(GraphScope::default(), "cell-0").unwrap(),
    )
    .unwrap();
    assert!(resolver
        .resolve_database(Some("default.scope1.not+base64._"))
        .is_err());
    let escaped = resolver
        .scoped_database_name("tenant/escape", Some("collection:name"))
        .unwrap();
    assert_eq!(
        resolver
            .resolve_database(Some(&escaped))
            .unwrap()
            .scope
            .namespace
            .to_string(),
        "default/dGVuYW50L2VzY2FwZQ/Y29sbGVjdGlvbjpuYW1l"
    );
    assert!(resolver
        .scoped_database_name("", Some("collection"))
        .is_err());
    assert!(resolver
        .resolve_database(Some("another.scope1.dGVuYW50._"))
        .is_err());
}

struct SnapshotEpochClient;

struct ConsistencyTestClient {
    refreshes: Arc<AtomicU64>,
}

#[async_trait]
impl QueryCellClient for ConsistencyTestClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("refreshes")],
            vec![QueryRow::new(vec![QueryValue::Count(
                self.refreshes.load(Ordering::SeqCst),
            )])],
        )
        .with_read_epoch(7)
        .with_storage_sequence(7))
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

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
        Ok(Some(7))
    }

    async fn refresh_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(Some(7))
    }
}

#[async_trait]
impl QueryCellClient for SnapshotEpochClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("value")],
            vec![QueryRow::new(vec![QueryValue::Count(1)])],
        )
        .with_read_epoch(9)
        .with_storage_sequence(13))
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

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
        Ok(Some(7))
    }
}

#[async_trait]
impl QueryCellClient for CursorTestClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        self.executions.fetch_add(1, Ordering::Relaxed);
        let result = QueryResultSet::new(
            vec![QueryColumn::new("value")],
            (1..=4)
                .map(|value| QueryRow::new(vec![QueryValue::Count(value)]))
                .collect(),
        )
        .with_read_epoch(7)
        .with_storage_sequence(7);
        if let Some(limit) = context.max_result_bytes {
            crate::codec::ensure_limit(
                "client_cursor_buffer_bytes",
                result.estimated_resident_bytes(),
                limit,
            )?;
        }
        Ok(result)
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

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
        Ok(Some(7))
    }
}

#[async_trait]
impl QueryCellClient for TestClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        let read_epoch = context
            .read_epoch
            .unwrap_or_else(|| self.epoch.load(Ordering::Relaxed));
        if let Some(token) = context.cancellation_token {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
                _ = token.cancelled() => return Err(client_query_cancelled()),
            }
        }
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("value")],
            vec![QueryRow::new(vec![QueryValue::Count(1)])],
        )
        .with_read_epoch(read_epoch)
        .with_storage_sequence(read_epoch))
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

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
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

fn cursor_service(
    executions: Arc<AtomicU64>,
    max_cursors: usize,
    max_buffer_bytes: u64,
    ttl_ms: u64,
) -> ClientQueryService {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    ClientQueryService::new(
        Arc::new(CursorTestClient { executions }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_server_cursor_limits(max_cursors, max_buffer_bytes, ttl_ms),
    )
    .unwrap()
}

fn authenticated_session(service: &ClientQueryService) -> ClientQuerySession {
    service
        .authenticate(
            &ClientQueryCredentials::Bearer("secret".to_string()),
            &QueryTransportConnectionIdentity::default(),
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
async fn service_bookmark_uses_the_slatedb_sequence_of_the_snapshot_read() {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(SnapshotEpochClient),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer)),
    )
    .unwrap();
    let session = authenticated_session(&service);
    let response = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(target(), "query-snapshot", "MATCH (n {id: 1}) RETURN n.id"),
        )
        .await
        .unwrap();

    assert_eq!(response.read_epoch, Some(9));
    assert_eq!(response.bookmark.unwrap().epoch, 13);
}

#[tokio::test]
async fn strong_reads_refresh_storage_while_causal_reads_stay_cache_local() {
    let refreshes = Arc::new(AtomicU64::new(0));
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(ConsistencyTestClient {
            refreshes: Arc::clone(&refreshes),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer)),
    )
    .unwrap();
    let session = authenticated_session(&service);

    let causal = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-causal-consistency",
                "MATCH (n {id: 1}) RETURN n.id",
            ),
        )
        .await
        .unwrap();
    assert_eq!(causal.result.rows[0].values, vec![QueryValue::Count(0)]);

    let strong = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-strong-consistency",
                "MATCH (n {id: 1}) RETURN n.id",
            )
            .strong(),
        )
        .await
        .unwrap();
    assert_eq!(strong.result.rows[0].values, vec![QueryValue::Count(1)]);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
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
async fn server_cursor_executes_once_and_release_invalidates_it() {
    let executions = Arc::new(AtomicU64::new(0));
    let service = cursor_service(Arc::clone(&executions), 4, 1 << 20, 1_000);
    let session = authenticated_session(&service);
    let request = ClientQueryRequest::new(
        target(),
        "query-server-cursor",
        "MATCH (n {id: 1}) RETURN n.id AS value",
    );

    let first = service
        .execute_page(&session, request.clone(), None, 1)
        .await
        .unwrap();
    let cursor = first.page.next_cursor.expect("remaining rows use a cursor");
    assert_eq!(executions.load(Ordering::Relaxed), 1);

    let second = service
        .execute_page(&session, request.clone(), Some(cursor), 1)
        .await
        .unwrap();
    assert_eq!(second.page.rows.len(), 1);
    assert_eq!(executions.load(Ordering::Relaxed), 1);
    assert!(
        service
            .release_server_cursor(&session, &request, cursor)
            .await
    );

    let error = service
        .execute_page(&session, request, Some(cursor), 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknown or expired"));
    assert_eq!(executions.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn server_cursor_enforces_count_and_buffer_admission() {
    let executions = Arc::new(AtomicU64::new(0));
    let service = cursor_service(Arc::clone(&executions), 1, 1 << 20, 1_000);
    let session = authenticated_session(&service);
    service
        .execute_page(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-cursor-first",
                "MATCH (n {id: 1}) RETURN n.id AS value",
            ),
            None,
            1,
        )
        .await
        .unwrap();
    let count_error = service
        .execute_page(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-cursor-second",
                "MATCH (n {id: 1}) RETURN n.id AS value",
            ),
            None,
            1,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        count_error,
        GraphError::AdmissionRejected {
            operation: "client_server_cursors",
            ..
        }
    ));

    let tiny_service = cursor_service(Arc::new(AtomicU64::new(0)), 1, 1, 1_000);
    let tiny_session = authenticated_session(&tiny_service);
    let buffer_error = tiny_service
        .execute_page(
            &tiny_session,
            ClientQueryRequest::new(
                target(),
                "query-cursor-buffer",
                "MATCH (n {id: 1}) RETURN n.id AS value",
            ),
            None,
            1,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        buffer_error,
        GraphError::AdmissionRejected {
            operation: "client_cursor_buffer_bytes",
            ..
        }
    ));

    let first_page_error = tiny_service
        .execute_page(
            &tiny_session,
            ClientQueryRequest::new(
                target(),
                "query-cursor-buffer-first-page",
                "MATCH (n {id: 1}) RETURN n.id AS value",
            ),
            None,
            4,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        first_page_error,
        GraphError::AdmissionRejected {
            operation: "client_cursor_buffer_bytes",
            ..
        }
    ));
}

#[tokio::test]
async fn expired_server_cursor_releases_its_buffer() {
    let service = cursor_service(Arc::new(AtomicU64::new(0)), 1, 1 << 20, 1);
    let session = authenticated_session(&service);
    let request = ClientQueryRequest::new(
        target(),
        "query-cursor-expiry",
        "MATCH (n {id: 1}) RETURN n.id AS value",
    );
    let first = service
        .execute_page(&session, request.clone(), None, 1)
        .await
        .unwrap();
    let cursor = first.page.next_cursor.expect("remaining rows use a cursor");
    tokio::time::sleep(Duration::from_millis(5)).await;

    let error = service
        .execute_page(&session, request, Some(cursor), 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknown or expired"));
    assert_eq!(service.inner.cursor_buffer_bytes.load(Ordering::Relaxed), 0);
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

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
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
