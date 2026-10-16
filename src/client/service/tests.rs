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

#[cfg(feature = "experimental-cypher-engine")]
struct ExperimentalModeClient {
    observed: Arc<Mutex<Option<CypherEngineMode>>>,
}

#[cfg(feature = "experimental-cypher-engine")]
struct ExperimentalMutationClient {
    executions: Arc<AtomicU64>,
}

#[cfg(feature = "experimental-cypher-engine")]
struct ExperimentalDispatchClient {
    batch_executions: Arc<AtomicU64>,
    cypher_queries: Arc<Mutex<Vec<String>>>,
}

#[cfg(feature = "experimental-cypher-engine")]
#[async_trait]
impl QueryCellClient for ExperimentalModeClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        *self.observed.lock().await = Some(context.cypher_engine);
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
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
}

#[cfg(feature = "experimental-cypher-engine")]
#[async_trait]
impl QueryCellClient for ExperimentalMutationClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        self.executions.fetch_add(1, Ordering::Relaxed);
        Ok(QueryResultSet::new(Vec::new(), Vec::new()))
    }

    async fn execute_cypher_rows_page(
        &self,
        _context: QueryContext,
        _query: &str,
        _cursor: Option<QueryCursorToken>,
        _page_size: usize,
    ) -> Result<QueryResultPage> {
        self.executions.fetch_add(1, Ordering::Relaxed);
        Ok(QueryResultPage::new(Vec::new(), Vec::new(), None))
    }

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
        Ok(Some(11))
    }
}

#[cfg(feature = "experimental-cypher-engine")]
#[async_trait]
impl QueryCellClient for ExperimentalDispatchClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        query: &str,
    ) -> Result<QueryResultSet> {
        self.cypher_queries.lock().await.push(query.to_string());
        Ok(
            QueryResultSet::new(vec![QueryColumn::new("path")], Vec::new())
                .with_read_epoch(17)
                .with_storage_sequence(17),
        )
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

    async fn execute_batch(
        &self,
        _context: QueryContext,
        _operation: QueryBatchOperation,
    ) -> Result<QueryResultSet> {
        self.batch_executions.fetch_add(1, Ordering::Relaxed);
        Ok(QueryResultSet::new(Vec::new(), Vec::new()).with_storage_sequence(18))
    }

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<StorageSequence>> {
        Ok(Some(18))
    }
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

/// The tenancy read back from a scope must be the tenancy that went in, for the
/// exact scope shape the resolver writes — including the sub-tenant that
/// decodes to something a human wrote, which is the case the log warehouse
/// exists to make searchable.
#[test]
fn scope_tenancy_reads_back_what_the_resolver_encoded() {
    let resolver = HierarchicalClientDatabaseResolver::new(
        "hydradb",
        ClientQueryTarget::new(
            GraphScope::new(
                NamespacePath::root(NamespaceId::new("staging").unwrap()),
                GraphId::new("hydradb").unwrap(),
            ),
            "cell-0",
        )
        .unwrap(),
    )
    .unwrap();
    let database = resolver
        .scoped_database_name("l3c4v6lu2w", Some("[Gmail]/All Mail"))
        .unwrap();
    let scope = resolver.resolve_database(Some(&database)).unwrap().scope;
    assert_eq!(
        scope.to_string(),
        "staging/bDNjNHY2bHUydw/W0dtYWlsXS9BbGwgTWFpbA/graphs/hydradb"
    );

    let tenancy = ScopeTenancy::from_scope(&scope);
    let tenant = tenancy.tenant.expect("the tenant segment is present");
    assert_eq!(tenant.id, "l3c4v6lu2w");
    assert_eq!(tenant.scope_id, "bDNjNHY2bHUydw");
    let sub_tenant = tenancy
        .sub_tenant
        .expect("the sub-tenant segment is present");
    assert_eq!(sub_tenant.id, "[Gmail]/All Mail");
    assert_eq!(sub_tenant.scope_id, "W0dtYWlsXS9BbGwgTWFpbA");
}

/// A tenant-level database has no third segment, and the default scope has
/// neither. Both must report absence rather than a blank identity, which is
/// what keeps an empty `tenant_id` out of the warehouse column.
#[test]
fn scope_tenancy_reports_absence_rather_than_a_blank_identity() {
    let resolver = HierarchicalClientDatabaseResolver::new(
        "default",
        ClientQueryTarget::new(GraphScope::default(), "cell-0").unwrap(),
    )
    .unwrap();
    let database = resolver.scoped_database_name("l3c4v6lu2w", None).unwrap();
    let scope = resolver.resolve_database(Some(&database)).unwrap().scope;
    let tenancy = ScopeTenancy::from_scope(&scope);
    assert_eq!(
        tenancy.tenant.map(|tenant| tenant.id),
        Some("l3c4v6lu2w".to_string())
    );
    assert!(tenancy.sub_tenant.is_none());

    let root = ScopeTenancy::from_scope(&GraphScope::default());
    assert_eq!(root, ScopeTenancy::default());
}

/// A scope built by hand — a static resolver's target, a test, the HTTP
/// `x-graph-namespace` header used literally — carries plain names, not base64.
/// Those are already the identity and must pass through unchanged.
///
/// `acme` is the case that makes this more than a formality: it is canonical
/// URL-safe base64 for two well-formed UTF-8 code points, so a round-trip check
/// alone would report the tenant as `iʮ`. See `decode_scope_id`.
#[test]
fn a_literal_namespace_segment_is_its_own_identity() {
    for segment in ["acme", "search", "tenant-with-a-longer-name"] {
        let scope = GraphScope::new(
            NamespacePath::new([
                NamespaceId::new("staging").unwrap(),
                NamespaceId::new(segment).unwrap(),
            ])
            .unwrap(),
            GraphId::new("social").unwrap(),
        );
        let tenant = ScopeTenancy::from_scope(&scope)
            .tenant
            .expect("the tenant segment is present");
        assert_eq!(tenant.id, segment, "{segment} was decoded rather than kept");
        assert_eq!(tenant.scope_id, segment);
    }
}

struct SnapshotEpochClient;

struct ConsistencyTestClient {
    refreshes: Arc<AtomicU64>,
}

struct BlockingRefreshClient;

#[async_trait]
impl QueryCellClient for BlockingRefreshClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        panic!("query execution must not start before the strong refresh finishes")
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
        std::future::pending().await
    }
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

fn session(token: &str) -> ClientQuerySession {
    ClientQuerySession {
        principal: QueryTransportPrincipal::from_bearer_token(token).unwrap(),
    }
}

#[test]
fn query_context_separates_mutation_identity_from_the_query_handle() {
    let request = ClientQueryRequest::new(
        target(),
        "bolt-2-query-459",
        "UNWIND $rows AS row RETURN row",
    )
    .with_server_generated_mutation_idempotency_key("bolt-mutation-v1-01K00000000000000000000000");

    let context = query_context(
        &session("caller-a"),
        &request,
        BTreeMap::new(),
        BTreeMap::new(),
        QueryCancellationToken::new(),
    );

    assert_eq!(request.query_id, "bolt-2-query-459");
    assert_eq!(
        context.idempotency_key,
        "bolt-mutation-v1-01K00000000000000000000000"
    );
}

#[test]
fn query_context_keeps_the_legacy_fallback_for_non_bolt_callers() {
    let request = ClientQueryRequest::new(target(), "transport-request-7", "RETURN 1");
    let context = query_context(
        &session("caller-a"),
        &request,
        BTreeMap::new(),
        BTreeMap::new(),
        QueryCancellationToken::new(),
    );

    assert_eq!(context.idempotency_key, "transport-request-7");
}

#[test]
fn caller_mutation_identity_is_stable_within_one_principal() {
    let request = ClientQueryRequest::new(target(), "query-1", "RETURN 1")
        .with_mutation_idempotency_key("bolt-caller-v1-retry-42");

    let first = query_context(
        &session("caller-a"),
        &request,
        BTreeMap::new(),
        BTreeMap::new(),
        QueryCancellationToken::new(),
    );
    let second = query_context(
        &session("caller-a"),
        &request,
        BTreeMap::new(),
        BTreeMap::new(),
        QueryCancellationToken::new(),
    );

    assert_eq!(first.idempotency_key, second.idempotency_key);
    assert!(first.idempotency_key.ends_with("-bolt-caller-v1-retry-42"));
}

#[test]
fn identical_caller_mutation_ids_are_isolated_between_principals() {
    let request = ClientQueryRequest::new(target(), "query-1", "RETURN 1")
        .with_mutation_idempotency_key("bolt-caller-v1-retry-42");

    let first = query_context(
        &session("caller-a"),
        &request,
        BTreeMap::new(),
        BTreeMap::new(),
        QueryCancellationToken::new(),
    );
    let second = query_context(
        &session("caller-b"),
        &request,
        BTreeMap::new(),
        BTreeMap::new(),
        QueryCancellationToken::new(),
    );

    assert_ne!(first.idempotency_key, second.idempotency_key);
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
async fn strong_read_timeout_does_not_wait_for_a_blocked_storage_refresh() {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(BlockingRefreshClient),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_query_runtime_ms(10),
    )
    .unwrap();
    let session = authenticated_session(&service);

    let started = std::time::Instant::now();
    let error = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-blocked-strong-refresh",
                "MATCH (n {id: 1}) RETURN n.id",
            )
            .strong(),
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
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(service.active_query_count().await, 0);
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
async fn prepares_native_path_procedure_columns_for_bolt_execution() {
    let service = service();
    let session = authenticated_session(&service);
    let prepared = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "query-native-mspaths",
                "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', \
                 sourceValues: ['alpha', 'beta'], targetValues: ['alpha', 'beta'], \
                 pairwise: true, relTypes: ['RELATES'], maxLen: 3, \
                 relDirection: 'both', pathCount: 5, \
                 fairRelationshipVariants: true, resultLimit: 100}) \
                 YIELD path RETURN path",
            ),
            256,
        )
        .await
        .unwrap();

    assert_eq!(prepared.action, QueryTransportAction::Read);
    assert_eq!(prepared.columns, vec![QueryColumn::new("path")]);
    assert!(prepared.batch_operation.is_none());
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_mode_prepares_native_path_procedures_without_generic_parsing() {
    let service = ClientQueryService::new(
        Arc::new(TestClient {
            epoch: AtomicU64::new(7),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let session = authenticated_session(&service);
    let prepared = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-native-mspaths",
                "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', \
                 sourceValues: ['alpha', 'beta'], targetValues: ['alpha', 'beta'], \
                 pairwise: true, relTypes: ['RELATES'], maxLen: 3, \
                 relDirection: 'both', pathCount: 5, \
                 fairRelationshipVariants: true, resultLimit: 100}) \
                 YIELD path RETURN path",
            ),
            256,
        )
        .await
        .expect("prepare native path call in experimental mode");

    assert_eq!(prepared.action, QueryTransportAction::Read);
    assert_eq!(prepared.columns, vec![QueryColumn::new("path")]);
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_mode_prepares_bolt_columns_with_the_new_parser_and_reaches_the_client() {
    let observed = Arc::new(Mutex::new(None));
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(ExperimentalModeClient {
            observed: Arc::clone(&observed),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let session = authenticated_session(&service);
    let query = "MATCH (e:Entity) RETURN e.entity_id AS id, e.rank AS rank \
                 ORDER BY rank DESC SKIP 1 LIMIT 2";
    let prepared = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(target(), "experimental-bolt-prepare", query),
            128,
        )
        .await
        .expect("prepare Bolt RUN with the experimental parser");
    assert_eq!(
        prepared.columns,
        vec![QueryColumn::new("id"), QueryColumn::new("rank")]
    );

    service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-client-mode",
                "MATCH (e:Entity) RETURN e",
            ),
        )
        .await
        .expect("execute experimental client query");
    assert_eq!(*observed.lock().await, Some(CypherEngineMode::Experimental));

    let explain = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-bolt-explain",
                "EXPLAIN MATCH (e:Entity) RETURN e",
            ),
            128,
        )
        .await
        .expect("prepare experimental EXPLAIN over Bolt");
    assert_eq!(explain.columns, vec![QueryColumn::new("plan")]);
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_mutation_uses_write_authorization_without_a_read_watermark() {
    let executions = Arc::new(AtomicU64::new(0));
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::graph(GraphScope::default(), [QueryTransportAction::Write]),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(ExperimentalMutationClient {
            executions: Arc::clone(&executions),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let session = authenticated_session(&service);

    let response = service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-mutation",
                "CREATE (n:Entity {id: 1})",
            ),
        )
        .await
        .expect("execute experimental mutation through write authorization");

    assert_eq!(executions.load(Ordering::Relaxed), 1);
    assert_eq!(response.read_epoch, None);
    assert_eq!(response.bookmark.expect("write bookmark").epoch, 11);
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_access_classification_enforces_read_and_write_grants() {
    let executions = Arc::new(AtomicU64::new(0));
    let read_authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let read_service = ClientQueryService::new(
        Arc::new(ExperimentalMutationClient {
            executions: Arc::clone(&executions),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(read_authorizer))
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let read_session = authenticated_session(&read_service);
    let mutation_error = read_service
        .execute_rows(
            &read_session,
            ClientQueryRequest::new(
                target(),
                "experimental-read-only-mutation",
                "CREATE (n:Entity {id: 1})",
            ),
        )
        .await
        .expect_err("read-only grant must reject mutations");
    assert!(matches!(
        mutation_error,
        GraphError::GraphScopeAccessDenied { .. }
    ));
    assert_eq!(executions.load(Ordering::Relaxed), 0);

    let write_authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::graph(GraphScope::default(), [QueryTransportAction::Write]),
        )
        .unwrap();
    let write_service = ClientQueryService::new(
        Arc::new(ExperimentalMutationClient {
            executions: Arc::clone(&executions),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(write_authorizer))
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let write_session = authenticated_session(&write_service);
    let read_error = write_service
        .prepare_page_request(
            &write_session,
            ClientQueryRequest::new(
                target(),
                "experimental-write-only-native-read",
                "CALL algo.SSpaths({sourceNode: 1, relTypes: ['ROUTE'], maxLen: 1}) \
                 YIELD path RETURN path",
            ),
            128,
        )
        .await
        .expect_err("write-only grant must reject native reads");
    assert!(matches!(
        read_error,
        GraphError::GraphScopeAccessDenied { .. }
    ));
    assert_eq!(executions.load(Ordering::Relaxed), 0);
}

/// Bolt sends field names in the RUN response, before any record, so a
/// mutation that ends in `RETURN count(r) AS deleted` has to declare that
/// column at preparation — long before the delete runs. A mutation without a
/// RETURN still prepares no columns, which is the shape every existing write
/// keeps.
///
/// Both engine modes are asserted because the experimental route dispatches
/// mutations to the same lowerer: the column a client sees must not depend on
/// which route is configured. The experimental mode needs its engine compiled
/// in even though this query never reaches it, because preparation rejects the
/// mode itself without the feature.
#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn prepares_the_returned_column_of_a_bounded_delete_on_both_engines() {
    for engine in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
        let authorizer = StaticQueryTransportScopeAuthorizer::new()
            .with_bearer_grant(
                "secret",
                QueryTransportScopeGrant::graph(
                    GraphScope::default(),
                    [QueryTransportAction::Write],
                ),
            )
            .unwrap();
        let service = ClientQueryService::new(
            Arc::new(TestClient {
                epoch: AtomicU64::new(7),
            }),
            ClientQueryServiceConfig::default()
                .with_required_bearer_token("secret")
                .with_scope_authorizer(Arc::new(authorizer))
                .with_cypher_engine(engine),
        )
        .unwrap();
        let session = authenticated_session(&service);

        let scope_parameters = [
            (
                "source_id".to_string(),
                QueryParameterValue::Scalar(VertexPropertyValue::String("src-1".to_string())),
            ),
            (
                "tenant_id".to_string(),
                QueryParameterValue::Scalar(VertexPropertyValue::String("tenant-1".to_string())),
            ),
            (
                "sub_tenant_id".to_string(),
                QueryParameterValue::Scalar(VertexPropertyValue::String("sub-1".to_string())),
            ),
        ];

        let prepared = service
            .prepare_page_request(
                &session,
                ClientQueryRequest::new(
                    target(),
                    "bounded-delete-columns",
                    "MATCH (a:Actor)-[r:ACTED_ON]->(s:Source {source_id: $source_id}) \
                     WHERE s.tenant_id = $tenant_id \
                         AND s.sub_tenant_id = $sub_tenant_id \
                     WITH r LIMIT 1000 \
                     DELETE r \
                     RETURN count(r) AS deleted",
                )
                .with_query_parameters(scope_parameters.clone()),
                128,
            )
            .await
            .expect("bounded delete must prepare");

        assert_eq!(prepared.action, QueryTransportAction::Write, "{engine:?}");
        assert_eq!(
            prepared.columns,
            vec![QueryColumn::new("deleted")],
            "{engine:?} must declare the returned column"
        );

        let silent = service
            .prepare_page_request(
                &session,
                ClientQueryRequest::new(
                    target(),
                    "bounded-delete-no-columns",
                    "MATCH (a:Actor)-[r:ACTED_ON]->(s:Source {source_id: $source_id}) \
                     WITH r LIMIT 1000 DELETE r",
                )
                .with_query_parameters(scope_parameters),
                128,
            )
            .await
            .expect("a delete without RETURN must still prepare");
        assert!(
            silent.columns.is_empty(),
            "{engine:?} must declare no columns without a RETURN"
        );
    }
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_mode_prepares_specialized_unwind_batches_before_generic_parsing() {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::graph(GraphScope::default(), [QueryTransportAction::Write]),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(TestClient {
            epoch: AtomicU64::new(7),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let session = authenticated_session(&service);
    let row = QueryParameterValue::Map(BTreeMap::from([(
        "vertex".to_string(),
        QueryParameterValue::Scalar(VertexPropertyValue::Integer(42)),
    )]));
    let prepared = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-specialized-delete",
                "UNWIND $vertices AS row MATCH (n {id: row.vertex}) DETACH DELETE n",
            )
            .with_query_parameters([(
                "vertices".to_string(),
                QueryParameterValue::List(vec![row]),
            )]),
            128,
        )
        .await
        .expect("prepare specialized batch in experimental mode");

    assert_eq!(prepared.action, QueryTransportAction::Write);
    assert!(prepared.columns.is_empty());
    assert!(matches!(
        prepared.batch_operation,
        Some(QueryBatchOperation::DeleteVertices {
            vertices,
            detach: true,
        }) if vertices == vec![42]
    ));
}

/// A read UNWIND batch carries its rows as a list of maps, which is an
/// UNWIND input rather than an `IN` list. The experimental engine used to
/// reject the parameter while classifying access, before the batch path that
/// consumes it was reached, so a query the legacy engine ran happily failed
/// with "composite parameter $rows is only supported as an UNWIND input".
#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_mode_prepares_a_read_unwind_batch_carrying_rows() {
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
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let session = authenticated_session(&service);
    let row = QueryParameterValue::Map(BTreeMap::from([(
        "src".to_string(),
        QueryParameterValue::Scalar(VertexPropertyValue::Integer(42)),
    )]));
    let prepared = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-read-unwind-batch",
                "UNWIND $rows AS row MATCH (s {id: row.src})-[:RELATES]->(d) \
                 RETURN row.src AS src, d.id AS dst",
            )
            .with_query_parameters([("rows".to_string(), QueryParameterValue::List(vec![row]))]),
            128,
        )
        .await
        .expect("prepare a read UNWIND batch in experimental mode");

    assert_eq!(prepared.action, QueryTransportAction::Read);
    assert!(matches!(
        prepared.batch_operation,
        Some(QueryBatchOperation::OutNeighbors { ref sources, .. }) if sources == &vec![42]
    ));

    // The rejection still stands for a query that is not an UNWIND batch:
    // nothing downstream would consume the rows.
    let error = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-composite-parameter",
                "MATCH (n:Entity) WHERE n.id = $rows RETURN n.id AS id",
            )
            .with_query_parameters([(
                "rows".to_string(),
                QueryParameterValue::List(vec![QueryParameterValue::Map(BTreeMap::from([(
                    "src".to_string(),
                    QueryParameterValue::Scalar(VertexPropertyValue::Integer(42)),
                )]))]),
            )]),
            128,
        )
        .await
        .expect_err("a composite parameter outside an UNWIND batch has no consumer");
    assert!(
        error
            .to_string()
            .contains("composite parameter $rows is only supported as an UNWIND input"),
        "{error}"
    );

    // And a read this engine genuinely cannot plan still fails: only a
    // recognized batch is allowed past the planner.
    let error = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-unplannable-read",
                "MATCH (n:Entity) WHERE n.name CONTAINS 'a' RETURN n.id AS id",
            ),
            128,
        )
        .await
        .expect_err("the experimental planner does not lower CONTAINS");
    assert!(error.to_string().contains("not supported yet"), "{error}");
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_mode_prepares_an_empty_read_unwind_batch() {
    let query = "UNWIND $rows AS row MATCH (s {id: row.src})-[:RELATES]->(d) \
                 RETURN row.src AS src, d.id AS dst";

    for engine in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
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
                .with_cypher_engine(engine),
        )
        .unwrap();
        let session = authenticated_session(&service);
        let prepared = service
            .prepare_page_request(
                &session,
                ClientQueryRequest::new(target(), "empty-read-unwind-batch", query)
                    .with_query_parameters([(
                        "rows".to_string(),
                        QueryParameterValue::List(vec![]),
                    )]),
                128,
            )
            .await
            .unwrap_or_else(|error| panic!("{engine:?} should prepare an empty batch: {error}"));

        assert_eq!(prepared.action, QueryTransportAction::Read);
        assert!(matches!(
            prepared.batch_operation,
            Some(QueryBatchOperation::OutNeighbors { ref sources, .. }) if sources.is_empty()
        ));
    }
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn experimental_mode_executes_specialized_batches_and_native_paths_on_their_routes() {
    let batch_executions = Arc::new(AtomicU64::new(0));
    let cypher_queries = Arc::new(Mutex::new(Vec::new()));
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::graph(
                GraphScope::default(),
                [QueryTransportAction::Read, QueryTransportAction::Write],
            ),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(ExperimentalDispatchClient {
            batch_executions: Arc::clone(&batch_executions),
            cypher_queries: Arc::clone(&cypher_queries),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let session = authenticated_session(&service);
    let batch_row = QueryParameterValue::Map(BTreeMap::from([
        (
            "src".to_string(),
            QueryParameterValue::Scalar(VertexPropertyValue::Integer(1)),
        ),
        (
            "dst".to_string(),
            QueryParameterValue::Scalar(VertexPropertyValue::Integer(2)),
        ),
    ]));
    service
        .execute_rows(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-execute-batch",
                "UNWIND $rows AS row CREATE (s {id: row.src})-[:RELATES]->(d {id: row.dst})",
            )
            .with_query_parameters([(
                "rows".to_string(),
                QueryParameterValue::List(vec![batch_row]),
            )])
            .with_server_generated_mutation_idempotency_key(
                "experimental-execute-batch-01K00000000000000000000000",
            ),
        )
        .await
        .expect("execute specialized batch route");

    let native_query = "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', \
                        sourceValues: ['alpha'], targetValues: ['beta'], pairwise: true, \
                        relTypes: ['RELATES'], maxLen: 3, relDirection: 'both', pathCount: 1}) \
                        YIELD path RETURN path";
    service
        .execute_rows(
            &session,
            ClientQueryRequest::new(target(), "experimental-execute-native-path", native_query),
        )
        .await
        .expect("execute native path route");

    assert_eq!(batch_executions.load(Ordering::Relaxed), 1);
    assert_eq!(cypher_queries.lock().await.as_slice(), [native_query]);
}

#[tokio::test]
async fn server_cursor_continuation_does_not_estimate_rows() {
    let service = cursor_service(Arc::new(AtomicU64::new(0)), 4, 1 << 20, 60_000);
    let session = authenticated_session(&service);
    let request = ClientQueryRequest::new(
        target(),
        "query-cursor-no-rescan",
        "MATCH (n {id: 1}) RETURN n.id AS value",
    );
    let first = service
        .execute_page(&session, request.clone(), None, 1)
        .await
        .unwrap();
    let mut token = first.page.next_cursor;
    let before = CURSOR_ROW_ESTIMATES.with(|count| count.get());
    while let Some(cursor) = token {
        token = service
            .execute_page(&session, request.clone(), Some(cursor), 1)
            .await
            .unwrap()
            .page
            .next_cursor;
    }
    let after = CURSOR_ROW_ESTIMATES.with(|count| count.get());
    assert_eq!(
        after - before,
        0,
        "pagination must use cached sizes, not walk row payloads"
    );
    assert_eq!(service.inner.cursor_buffer_bytes.load(Ordering::Relaxed), 0);
}

fn cursor_payload_rows(count: usize) -> Vec<QueryRow> {
    (0..count)
        .map(|index| {
            QueryRow::new(vec![
                QueryValue::Count(index as u64),
                QueryValue::Property(VertexPropertyValue::String("text".repeat(index % 23 + 1))),
                QueryValue::List(vec![
                    QueryValue::Null,
                    QueryValue::List(vec![
                        QueryValue::Bool(true),
                        QueryValue::Property(VertexPropertyValue::String(
                            "nested".repeat(index % 7 + 1),
                        )),
                    ]),
                ]),
                QueryValue::Path(Box::new(crate::QueryPath {
                    nodes: vec![
                        crate::QueryPathNode {
                            id: index as u64,
                            labels: vec!["Entity".into()],
                            properties: BTreeMap::from([(
                                "name".into(),
                                VertexPropertyValue::String("node".repeat(index % 5 + 1)),
                            )]),
                        },
                        crate::QueryPathNode {
                            id: index as u64 + 1,
                            labels: vec![],
                            properties: BTreeMap::new(),
                        },
                    ],
                    relationships: vec![crate::QueryPathRelationship {
                        id: None,
                        edge_type: "LINK".into(),
                        src: index as u64,
                        dst: index as u64 + 1,
                        properties: BTreeMap::from([(
                            "description".into(),
                            VertexPropertyValue::String("relationship".into()),
                        )]),
                    }],
                })),
            ])
        })
        .collect()
}

fn expected_cursor_payload_bytes(rows: &[QueryRow]) -> u64 {
    rows.iter().fold(0_u64, |total, row| {
        total
            .saturating_add(row.estimated_resident_bytes())
            .saturating_add(std::mem::size_of::<u64>() as u64)
    })
}

#[tokio::test]
async fn server_cursor_sizes_payloads_before_locking_registry() {
    let service = cursor_service(Arc::new(AtomicU64::new(0)), 4, 1 << 20, 60_000);
    let session = authenticated_session(&service);
    let request = ClientQueryRequest::new(target(), "cursor-before-lock", "RETURN 1");
    let rows = cursor_payload_rows(12);
    let count = rows.len() as u64;
    let registry = service.inner.cursors.lock().await;
    let before = CURSOR_ROW_ESTIMATES.with(|count| count.get());
    let mut start = std::pin::pin!(service.start_server_cursor(
        &session,
        &request,
        QueryResultSet::new(vec![QueryColumn::new("value")], rows),
        Some(7),
        None,
        1,
    ));
    assert!(futures::poll!(start.as_mut()).is_pending());
    assert_eq!(
        CURSOR_ROW_ESTIMATES.with(|count| count.get()) - before,
        count,
        "all payload sizing must finish before waiting for the registry lock"
    );
    drop(registry);
    let page = start.await.unwrap();
    assert!(page.page.next_cursor.is_some());
}

#[tokio::test]
async fn server_cursor_cached_accounting_preserves_rows_and_metadata() {
    let service = cursor_service(Arc::new(AtomicU64::new(0)), 4, 8 << 20, 60_000);
    let session = authenticated_session(&service);
    let request = ClientQueryRequest::new(target(), "cursor-rich-rows", "RETURN 1");
    let rows = cursor_payload_rows(257);
    let columns = vec![
        QueryColumn::new("count"),
        QueryColumn::new("text"),
        QueryColumn::new("list"),
        QueryColumn::new("path"),
    ];
    let bookmark = Some(ClientBookmark::new(target(), 7));
    let first = service
        .start_server_cursor(
            &session,
            &request,
            QueryResultSet::new(columns.clone(), rows.clone()),
            Some(7),
            bookmark.clone(),
            1,
        )
        .await
        .unwrap();
    let mut seen = first.page.rows;
    let mut token = first.page.next_cursor;
    let before = CURSOR_ROW_ESTIMATES.with(|count| count.get());
    let mut page_index = 0;
    while let Some(cursor) = token {
        let expected = expected_cursor_payload_bytes(&rows[seen.len()..]);
        assert_eq!(
            service.inner.cursor_buffer_bytes.load(Ordering::Relaxed),
            expected
        );
        assert_eq!(
            service
                .inner
                .cursors
                .lock()
                .await
                .get(&cursor.offset)
                .unwrap()
                .resident_bytes,
            expected
        );
        let page = service
            .continue_server_cursor(&session, &request, cursor, [1, 7, 32, 3][page_index % 4])
            .await
            .unwrap();
        assert_eq!(page.page.columns, columns);
        assert_eq!(page.read_epoch, Some(7));
        assert_eq!(page.bookmark, bookmark);
        seen.extend(page.page.rows);
        token = page.page.next_cursor;
        page_index += 1;
    }
    assert_eq!(seen, rows);
    assert_eq!(CURSOR_ROW_ESTIMATES.with(|count| count.get()), before);
    assert!(service.inner.cursors.lock().await.is_empty());
    assert_eq!(service.inner.cursor_buffer_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn server_cursor_cached_accounting_isolated_across_concurrent_cursors() {
    let service = cursor_service(Arc::new(AtomicU64::new(0)), 4, 8 << 20, 60_000);
    let session = authenticated_session(&service);
    let first_request = ClientQueryRequest::new(target(), "cursor-first", "RETURN 1");
    let second_request = ClientQueryRequest::new(target(), "cursor-second", "RETURN 1");
    let first_rows = cursor_payload_rows(21);
    let second_rows = cursor_payload_rows(13);
    let first = service
        .start_server_cursor(
            &session,
            &first_request,
            QueryResultSet::new(vec![], first_rows.clone()),
            Some(7),
            None,
            1,
        )
        .await
        .unwrap();
    let second = service
        .start_server_cursor(
            &session,
            &second_request,
            QueryResultSet::new(vec![], second_rows.clone()),
            Some(7),
            None,
            1,
        )
        .await
        .unwrap();
    let first_token = first.page.next_cursor.unwrap();
    let second_token = second.page.next_cursor.unwrap();
    let (first_page, second_page) = tokio::join!(
        service.continue_server_cursor(&session, &first_request, first_token, 3),
        service.continue_server_cursor(&session, &second_request, second_token, 5),
    );
    assert_eq!(first_page.unwrap().page.rows, first_rows[1..4]);
    assert_eq!(second_page.unwrap().page.rows, second_rows[1..6]);
    assert_eq!(
        service.inner.cursor_buffer_bytes.load(Ordering::Relaxed),
        expected_cursor_payload_bytes(&first_rows[4..])
            + expected_cursor_payload_bytes(&second_rows[6..])
    );
    let guard_is_live = service
        .inner
        .cursors
        .lock()
        .await
        .get(&first_token.offset)
        .unwrap()
        .memory_diagnostic
        .probe();
    assert!(
        service
            .release_server_cursor(&session, &first_request, first_token)
            .await
    );
    assert!(
        !guard_is_live(),
        "release must drop the diagnostic charge too"
    );
    assert!(
        !service
            .release_server_cursor(&session, &first_request, first_token)
            .await
    );
    assert_eq!(
        service.inner.cursor_buffer_bytes.load(Ordering::Relaxed),
        expected_cursor_payload_bytes(&second_rows[6..])
    );
    service
        .inner
        .cursors
        .lock()
        .await
        .get_mut(&second_token.offset)
        .unwrap()
        .expires_at = Instant::now() - Duration::from_secs(1);
    let error = service
        .continue_server_cursor(&session, &second_request, second_token, 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknown or expired"));
    assert_eq!(service.inner.cursor_buffer_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn server_cursor_rejected_continuation_keeps_cached_charge() {
    let service = cursor_service(Arc::new(AtomicU64::new(0)), 4, 1 << 20, 60_000);
    let owner = authenticated_session(&service);
    let request = ClientQueryRequest::new(target(), "cursor-owner", "RETURN 1");
    let first = service
        .start_server_cursor(
            &owner,
            &request,
            QueryResultSet::new(vec![], cursor_payload_rows(9)),
            Some(7),
            None,
            1,
        )
        .await
        .unwrap();
    let token = first.page.next_cursor.unwrap();
    let bytes = service.inner.cursor_buffer_bytes.load(Ordering::Relaxed);
    let wrong_principal = session("another-principal");
    assert!(service
        .continue_server_cursor(&wrong_principal, &request, token, 2)
        .await
        .is_err());
    assert!(
        !service
            .release_server_cursor(&wrong_principal, &request, token)
            .await
    );
    for altered in [
        ClientQueryRequest::new(target(), "another-query-id", "RETURN 1"),
        ClientQueryRequest::new(target(), "cursor-owner", "RETURN 2"),
        ClientQueryRequest::new(
            ClientQueryTarget::new(GraphScope::default(), "another-cell").unwrap(),
            "cursor-owner",
            "RETURN 1",
        ),
        ClientQueryRequest::new(target(), "cursor-owner", "RETURN 1").with_parameters(
            BTreeMap::from([("x".into(), VertexPropertyValue::Integer(1))]),
        ),
    ] {
        assert!(service
            .continue_server_cursor(&owner, &altered, token, 2)
            .await
            .is_err());
        assert_eq!(
            service.inner.cursor_buffer_bytes.load(Ordering::Relaxed),
            bytes
        );
    }
    assert!(service.release_server_cursor(&owner, &request, token).await);
    assert_eq!(service.inner.cursor_buffer_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn server_cursor_admission_charges_cached_size_storage() {
    let rows = cursor_payload_rows(4);
    let payload_bytes = rows
        .iter()
        .map(QueryRow::estimated_resident_bytes)
        .sum::<u64>();
    let service = cursor_service(Arc::new(AtomicU64::new(0)), 4, payload_bytes, 60_000);
    let session = authenticated_session(&service);
    let request = ClientQueryRequest::new(target(), "cursor-cache-budget", "RETURN 1");
    let error = service
        .start_server_cursor(
            &session,
            &request,
            QueryResultSet::new(vec![], rows),
            Some(7),
            None,
            1,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::AdmissionRejected {
            operation: "client_cursor_buffer_bytes",
            ..
        }
    ));
    assert!(service.inner.cursors.lock().await.is_empty());
    assert_eq!(service.inner.cursor_buffer_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn server_cursor_complete_first_page_does_not_charge_cached_sizes() {
    for count in [0, 4] {
        let rows = cursor_payload_rows(count);
        let limit = rows
            .iter()
            .map(QueryRow::estimated_resident_bytes)
            .sum::<u64>()
            .max(1);
        let service = cursor_service(Arc::new(AtomicU64::new(0)), 1, limit, 60_000);
        let session = authenticated_session(&service);
        let request = ClientQueryRequest::new(target(), "cursor-complete-page", "RETURN 1");
        let page = service
            .start_server_cursor(
                &session,
                &request,
                QueryResultSet::new(vec![], rows.clone()),
                Some(7),
                None,
                4,
            )
            .await
            .unwrap();
        assert_eq!(page.page.rows, rows);
        assert!(page.page.next_cursor.is_none());
        assert!(service.inner.cursors.lock().await.is_empty());
        assert_eq!(service.inner.cursor_buffer_bytes.load(Ordering::Relaxed), 0);
    }
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

/// `execution_duration_us` stopped being stored and is now the sum of the two
/// histograms, so the thing worth pinning is that its *value* did not move.
/// The inputs deliberately straddle the ladder: below the 100 µs floor, exactly
/// on a bound (`le` is inclusive), mid-bucket, and past the 30 s overflow.
#[test]
fn derived_execution_duration_is_bit_exact_over_the_recorded_durations() {
    const READS_US: [u64; 4] = [37, 100, 4_321, 40_000_000];
    const WRITES_US: [u64; 3] = [250, 999_999, 12_345_678];

    let metrics = ClientQueryMetrics::default();
    // Split across the engines as well, so the sum is proven total over all
    // four histograms and not just over a matching pair.
    for (index, micros) in READS_US.into_iter().enumerate() {
        metrics.record_execution(
            QueryTransportAction::Read,
            Duration::from_micros(micros),
            engine(index),
        );
    }
    for (index, micros) in WRITES_US.into_iter().enumerate() {
        metrics.record_execution(
            QueryTransportAction::Write,
            Duration::from_micros(micros),
            engine(index),
        );
    }

    let snapshot = metrics.snapshot();
    let expected: u64 = READS_US.iter().chain(WRITES_US.iter()).sum();
    assert_eq!(snapshot.execution_duration_us, expected);
    // The same equality stated the other way round, so a regression that
    // reintroduced a separate counter fails here rather than drifting quietly.
    assert_eq!(
        snapshot.read_latency_legacy.sum_us
            + snapshot.read_latency_experimental.sum_us
            + snapshot.write_latency_legacy.sum_us
            + snapshot.write_latency_experimental.sum_us,
        snapshot.execution_duration_us
    );
    assert_eq!(
        snapshot.read_latency_legacy.count() + snapshot.read_latency_experimental.count(),
        READS_US.len() as u64
    );
    assert_eq!(
        snapshot.write_latency_legacy.count() + snapshot.write_latency_experimental.count(),
        WRITES_US.len() as u64
    );
}

/// Alternating engines, so no test above depends on which one a given index
/// lands on — only on every observation reaching exactly one histogram.
fn engine(index: usize) -> CypherEngineMode {
    if index.is_multiple_of(2) {
        CypherEngineMode::Legacy
    } else {
        CypherEngineMode::Experimental
    }
}

/// The kill switch can flip a node mid-life, so one process holds both
/// populations. An observation belongs to the engine that produced it, whatever
/// the node is set to afterwards — otherwise the engine comparison these
/// families exist for reads a legacy number as an experimental one.
#[test]
fn an_execution_lands_in_the_histogram_for_the_engine_that_ran_it() {
    let metrics = ClientQueryMetrics::default();
    metrics.record_execution(
        QueryTransportAction::Read,
        Duration::from_micros(1_000),
        CypherEngineMode::Experimental,
    );
    metrics.record_execution(
        QueryTransportAction::Read,
        Duration::from_micros(2_000),
        CypherEngineMode::Legacy,
    );
    metrics.record_execution(
        QueryTransportAction::Write,
        Duration::from_micros(3_000),
        CypherEngineMode::Experimental,
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.read_latency_experimental.count(), 1);
    assert_eq!(snapshot.read_latency_experimental.sum_us, 1_000);
    assert_eq!(snapshot.read_latency_legacy.count(), 1);
    assert_eq!(snapshot.read_latency_legacy.sum_us, 2_000);
    assert_eq!(snapshot.write_latency_experimental.count(), 1);
    assert_eq!(snapshot.write_latency_experimental.sum_us, 3_000);
    // A write on the engine that ran no write stays empty: the four are
    // populations, not a pair of totals split two ways.
    assert_eq!(snapshot.write_latency_legacy.count(), 0);
    assert_eq!(snapshot.write_latency_legacy.sum_us, 0);
}

/// The point of the split: a mutation's commit path and a read must not share
/// one distribution. Under the old single counter both counts would read 2.
#[test]
fn a_read_and_a_write_land_in_different_histograms() {
    let metrics = ClientQueryMetrics::default();
    metrics.record_execution(
        QueryTransportAction::Read,
        Duration::from_micros(1_000),
        CypherEngineMode::Legacy,
    );
    metrics.record_execution(
        QueryTransportAction::Write,
        Duration::from_micros(5_000_000),
        CypherEngineMode::Legacy,
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.read_latency_legacy.count(), 1);
    assert_eq!(snapshot.read_latency_legacy.sum_us, 1_000);
    assert_eq!(snapshot.write_latency_legacy.count(), 1);
    assert_eq!(snapshot.write_latency_legacy.sum_us, 5_000_000);

    // Neither observation appears in the other's buckets, which is the claim
    // the counts alone only imply.
    let read_bucket = snapshot
        .read_latency_legacy
        .bucket_counts
        .iter()
        .position(|count| *count > 0)
        .unwrap();
    let write_bucket = snapshot
        .write_latency_legacy
        .bucket_counts
        .iter()
        .position(|count| *count > 0)
        .unwrap();
    assert_ne!(read_bucket, write_bucket);
    assert_eq!(snapshot.write_latency_legacy.bucket_counts[read_bucket], 0);
    assert_eq!(snapshot.read_latency_legacy.bucket_counts[write_bucket], 0);
}

/// Each of the ten classes lands in its own slot on the client's counter too,
/// and in no other. The mapping is `GraphError::class_index` in both places, so
/// this and the shard's copy of it are checking the same taxonomy from the two
/// structs that carry it.
#[test]
fn client_query_failures_land_in_their_own_class_slot() {
    for (index, error) in GraphError::one_per_class().into_iter().enumerate() {
        let metrics = ClientQueryMetrics::default();
        metrics.record_result(0, Some(&error));
        let snapshot = metrics.snapshot();

        let expected: [u64; GraphError::CLASS_COUNT] =
            std::array::from_fn(|slot| u64::from(slot == index));
        assert_eq!(
            snapshot.queries_failed_by_class,
            expected,
            "{} landed outside its own slot",
            error.class()
        );
        assert_eq!(snapshot.queries_failed, 1);
    }
}

/// A success dimensions nothing: it must not reach the per-class array at all.
/// The array counts failures, and a completion counted as a class would put a
/// non-error into an error-rate panel.
#[test]
fn a_completed_query_touches_no_class_slot() {
    let metrics = ClientQueryMetrics::default();
    metrics.record_result(7, None);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.queries_completed, 1);
    assert_eq!(snapshot.rows_returned, 7);
    assert_eq!(snapshot.queries_failed, 0);
    assert_eq!(
        snapshot.queries_failed_by_class,
        [0; GraphError::CLASS_COUNT]
    );
}

/// The per-class rows the export layer will read, and the total they must add
/// up to. `class_counter_fields` is the only enumeration of this field; an
/// export that reached for the array by offset instead is the failure mode the
/// flattened rows exist to prevent.
#[test]
fn client_class_counter_rows_sum_to_the_undimensioned_total() {
    let metrics = ClientQueryMetrics::default();
    let errors = GraphError::one_per_class();
    metrics.record_result(0, Some(&errors[GraphError::CLASS_FENCING]));
    metrics.record_result(0, Some(&errors[GraphError::CLASS_FENCING]));
    metrics.record_result(0, Some(&errors[GraphError::CLASS_STORAGE]));

    let snapshot = metrics.snapshot();
    let rows: Vec<(&'static str, &'static str, u64)> = snapshot.class_counter_fields().collect();

    assert_eq!(rows.len(), GraphError::CLASS_COUNT);
    let nonzero: Vec<(&'static str, u64)> = rows
        .iter()
        .filter(|(_, _, count)| *count > 0)
        .map(|(_, class, count)| (*class, *count))
        .collect();
    assert_eq!(nonzero, vec![("fencing", 2), ("storage", 1)]);
    assert_eq!(
        rows.iter().map(|(_, _, count)| count).sum::<u64>(),
        snapshot.queries_failed
    );
}

/// A list parameter used to be refused here, before the query text was even
/// looked at, whichever engine was selected. That rejection is what made
/// `WHERE x IN $ids` unreachable from a client.
#[test]
fn a_list_of_scalars_is_split_out_rather_than_refused() {
    let parameters = BTreeMap::from([
        (
            "tenant_id".to_string(),
            QueryParameterValue::Scalar(VertexPropertyValue::String("tenant-1".to_string())),
        ),
        (
            "chunk_ids".to_string(),
            QueryParameterValue::List(vec![
                QueryParameterValue::Scalar(VertexPropertyValue::String("chunk-a".to_string())),
                QueryParameterValue::Scalar(VertexPropertyValue::String("chunk-b".to_string())),
            ]),
        ),
    ]);

    let (scalars, lists) = split_query_parameters(&parameters).expect("lists are accepted");

    assert_eq!(
        scalars.keys().collect::<Vec<_>>(),
        vec!["tenant_id"],
        "a list must not land in the scalar map"
    );
    assert_eq!(
        lists.get("chunk_ids").map(Vec::len),
        Some(2),
        "the list must reach the sidecar intact"
    );
}

/// A list of rows is an UNWIND input, not an `IN` list, and still gets the
/// message it always had. Widening that would send batch payloads somewhere
/// they cannot be interpreted.
#[test]
fn a_list_of_rows_is_still_an_unwind_only_input() {
    let parameters = BTreeMap::from([(
        "rows".to_string(),
        QueryParameterValue::List(vec![QueryParameterValue::Map(BTreeMap::from([(
            "id".to_string(),
            QueryParameterValue::Scalar(VertexPropertyValue::Integer(1)),
        )]))]),
    )]);

    let error = split_query_parameters(&parameters).expect_err("a list of maps is refused");

    assert!(
        error
            .to_string()
            .contains("only supported as an UNWIND input"),
        "unexpected error: {error}"
    );
}

/// A map parameter is refused for the same reason, unchanged.
#[test]
fn a_map_parameter_is_still_refused() {
    let parameters = BTreeMap::from([(
        "filter".to_string(),
        QueryParameterValue::Map(BTreeMap::from([(
            "id".to_string(),
            QueryParameterValue::Scalar(VertexPropertyValue::Integer(1)),
        )])),
    )]);

    assert!(split_query_parameters(&parameters).is_err());
}

#[tokio::test]
async fn memory_diagnostics_prepared_copies_have_independent_lifetimes() {
    let service = service();
    let session = session("secret");
    let prepared = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(target(), "memory-prepared", "MATCH (n {id: 1}) RETURN n.id"),
            1,
        )
        .await
        .unwrap();
    let original_live = prepared.memory_diagnostic.probe();
    let clone = prepared.clone();
    let clone_live = clone.memory_diagnostic.probe();
    drop(prepared);
    assert!(!original_live());
    assert!(clone_live());
    drop(clone);
    assert!(!clone_live());
}

fn failure_count(snapshot: &ClientQueryMetricsSnapshot, stage: &str, reason: &str) -> u64 {
    snapshot
        .failure_counter_fields()
        .filter(|(_, row_stage, row_reason, _)| (*row_stage, *row_reason) == (stage, reason))
        .map(|(.., count)| count)
        .sum()
}

fn failure_total(snapshot: &ClientQueryMetricsSnapshot) -> u64 {
    snapshot
        .failure_counter_fields()
        .map(|(.., count)| count)
        .sum()
}

/// A query failure lands in exactly one row, under its reason; a failure of
/// any other class -- a timeout, admission, storage -- is not counted at all,
/// and neither is a remote failure its owner did not classify.
#[test]
fn only_query_failures_are_counted_each_under_its_reason() {
    for error in GraphError::one_per_class() {
        let metrics = ClientQueryMetrics::default();
        metrics
            .queries_failed_by_reason_legacy
            .record(QueryFailureStage::Execute, &error);
        let snapshot = metrics.snapshot();
        match error.failure_reason() {
            Some(reason) => {
                assert_eq!(failure_total(&snapshot), 1, "{error:?}");
                assert_eq!(
                    failure_count(&snapshot, "execute", reason.as_str()),
                    1,
                    "{error:?}"
                );
            }
            None => assert_eq!(failure_total(&snapshot), 0, "{error:?}"),
        }
    }
    let metrics = ClientQueryMetrics::default();
    metrics.queries_failed_by_reason_legacy.record(
        QueryFailureStage::Execute,
        &GraphError::UnclassifiedQuery {
            dialect: "QueryTransport",
            feature: "query/transport/rows: exceeded query timeout".to_string(),
        },
    );
    assert_eq!(failure_total(&metrics.snapshot()), 0);
    // Two engines x two stages x every reason: the enumeration is the
    // product, and `failure_count` sums the engines because which one served
    // a failure is not what these assertions are about.
    assert_eq!(
        ClientQueryMetricsSnapshot::default()
            .failure_counter_fields()
            .count(),
        2 * 2 * QueryFailureReason::COUNT
    );
}

/// The engine a failure is counted against is fixed when it is recorded, so a
/// kill-switch flip afterwards cannot move it. Without the split, a scrape
/// after a rollback would report every experimental failure as legacy.
#[test]
fn a_failure_is_counted_against_the_engine_that_produced_it() {
    let metrics = ClientQueryMetrics::default();
    let error = GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Where,
        dialect: "Cypher25",
        feature: "a predicate no engine supports".to_string(),
    };
    metrics
        .queries_failed_by_reason_experimental
        .record(QueryFailureStage::Prepare, &error);

    let snapshot = metrics.snapshot();
    let rows = |counts: &crate::QueryFailureCountsSnapshot| -> u64 {
        counts.rows().map(|(.., count)| count).sum()
    };
    assert_eq!(rows(&snapshot.queries_failed_by_reason_experimental), 1);
    assert_eq!(rows(&snapshot.queries_failed_by_reason_legacy), 0);
    // And the enumeration keeps both populations apart, which is what the
    // export reads the label from.
    assert_eq!(
        snapshot
            .failure_counter_fields()
            .filter(|(field, ..)| *field == "queries_failed_by_reason_experimental")
            .map(|(.., count)| count)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        snapshot
            .failure_counter_fields()
            .filter(|(field, ..)| *field == "queries_failed_by_reason_legacy")
            .map(|(.., count)| count)
            .sum::<u64>(),
        0
    );
}

/// The production gap this family closes: a query the engine cannot plan
/// fails in prepare, goes straight back to a Bolt client, and was counted by
/// nothing -- `queries_failed` still reads zero afterwards.
#[tokio::test]
async fn an_unsupported_query_is_counted_at_prepare_under_its_reason() {
    let service = service();
    let session = authenticated_session(&service);
    let error = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "legacy-unsupported-where",
                "MATCH (n:Entity) WHERE n.name CONTAINS 'a' RETURN n.name AS name",
            ),
            128,
        )
        .await
        .expect_err("legacy WHERE does not lower CONTAINS");
    assert_eq!(error.failure_reason(), Some(QueryFailureReason::Where));

    let snapshot = service.metrics();
    assert_eq!(failure_count(&snapshot, "prepare", "unsupported_where"), 1);
    assert_eq!(failure_total(&snapshot), 1);
    assert_eq!(snapshot.queries_failed, 0);
}

/// The comparison the dashboard is for: the same unsupported predicate on an
/// experimental node lands in the same bucket as on a legacy one, rather than
/// in `untyped_statement`, because the Cypher 25 lowering reports the clause
/// it stopped in.
#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn both_engines_file_the_same_unsupported_predicate_under_the_same_reason() {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let service = ClientQueryService::new(
        Arc::new(ExperimentalModeClient {
            observed: Arc::new(Mutex::new(None)),
        }),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_cypher_engine(CypherEngineMode::Experimental),
    )
    .unwrap();
    let session = authenticated_session(&service);
    let error = service
        .prepare_page_request(
            &session,
            ClientQueryRequest::new(
                target(),
                "experimental-unsupported-where",
                "MATCH (n:Entity) WHERE n.name CONTAINS 'a' RETURN n.name AS name",
            ),
            128,
        )
        .await
        .expect_err("Cypher 25 lowering does not type CONTAINS");
    assert!(
        error
            .to_string()
            .contains("expected exactly one typed query statement"),
        "{error}"
    );
    assert_eq!(error.failure_reason(), Some(QueryFailureReason::Where));
    assert_eq!(
        failure_count(&service.metrics(), "prepare", "unsupported_where"),
        1
    );
}

/// `execute_page` rejects a historical epoch before preparing, which neither
/// the prepare nor the execute wrapper sees.
#[tokio::test]
async fn a_historical_epoch_page_request_is_counted_at_prepare() {
    let service = service();
    let session = authenticated_session(&service);
    let mut request = ClientQueryRequest::new(
        target(),
        "historical-epoch",
        "MATCH (n {id: 1}) RETURN n.id",
    );
    request.read_epoch = Some(7);
    service
        .execute_page(&session, request, None, 16)
        .await
        .expect_err("a first page cannot pin a historical epoch");
    let snapshot = service.metrics();
    assert_eq!(failure_count(&snapshot, "prepare", "invalid_request"), 1);
    assert_eq!(failure_total(&snapshot), 1);
}
