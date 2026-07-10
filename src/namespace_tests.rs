use std::sync::Arc;

use slatedb::object_store::{memory::InMemory, ObjectStore};

use crate::*;

fn namespace(name: &str) -> NamespacePath {
    NamespacePath::root(NamespaceId::new(name).unwrap())
}

fn subnamespace(tenant: &str, child: &str) -> NamespacePath {
    namespace(tenant)
        .child(NamespaceId::new(child).unwrap())
        .unwrap()
}

fn scope(tenant: &str, child: &str, graph: &str) -> GraphScope {
    GraphScope::new(subnamespace(tenant, child), GraphId::new(graph).unwrap())
}

#[test]
fn namespace_paths_are_validated_hierarchical_and_collision_free() {
    let tenant = namespace("acme");
    let search = tenant.child(NamespaceId::new("search").unwrap()).unwrap();
    let flat = namespace("acme-search");

    assert_eq!(search.tenant_id().as_str(), "acme");
    assert_eq!(search.leaf().as_str(), "search");
    assert_eq!(search.depth(), 2);
    assert!(search.is_descendant_of(&tenant));
    assert!(!tenant.is_descendant_of(&search));

    let nested_scope = GraphScope::new(search, GraphId::new("social").unwrap());
    let flat_scope = GraphScope::new(flat, GraphId::new("social").unwrap());
    assert_eq!(
        nested_scope.scoped_store_path("graph-data"),
        "graph-data/namespaces/acme/subnamespaces/search/graphs/social"
    );
    assert_eq!(
        flat_scope.scoped_store_path("graph-data"),
        "graph-data/namespaces/acme-search/graphs/social"
    );
    assert_ne!(
        nested_scope.scoped_store_path("graph-data"),
        flat_scope.scoped_store_path("graph-data")
    );
    assert_eq!(
        nested_scope.scoped_store_path(""),
        "namespaces/acme/subnamespaces/search/graphs/social"
    );

    assert!(NamespaceId::new("invalid/name").is_err());
    assert!(GraphId::new("invalid/name").is_err());
    assert!(NamespacePath::new(Vec::<NamespaceId>::new()).is_err());
    let too_deep =
        (0..=MAX_NAMESPACE_DEPTH).map(|index| NamespaceId::new(format!("n{index}")).unwrap());
    assert!(matches!(
        NamespacePath::new(too_deep),
        Err(GraphError::AdmissionRejected {
            operation: "namespace_depth",
            ..
        })
    ));
}

#[cfg(feature = "query-transport")]
#[test]
fn query_context_scope_round_trips_and_legacy_frames_default_safely() {
    let scoped = QueryContext::new("cell-a", "query-a").in_scope(scope("acme", "search", "social"));
    let encoded = serde_json::to_value(&scoped).unwrap();
    assert_eq!(
        serde_json::from_value::<QueryContext>(encoded).unwrap(),
        scoped
    );

    let legacy = serde_json::json!({
        "cell_id": "cell-a",
        "idempotency_key": "legacy-query",
        "read_epoch": null,
        "result_window": { "skip": 0, "limit": null },
        "parameters": {},
        "max_runtime_ms": null
    });
    let legacy = serde_json::from_value::<QueryContext>(legacy).unwrap();
    assert_eq!(legacy.scope, GraphScope::default());

    let invalid = serde_json::json!({
        "scope": {
            "namespace": ["acme", "invalid/name"],
            "graph_id": "social"
        },
        "cell_id": "cell-a",
        "idempotency_key": "invalid-scope",
        "read_epoch": null,
        "result_window": { "skip": 0, "limit": null },
        "parameters": {},
        "max_runtime_ms": null
    });
    assert!(serde_json::from_value::<QueryContext>(invalid).is_err());
}

#[tokio::test]
async fn scoped_graph_clusters_isolate_identical_graph_keys() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first_scope = scope("acme", "search", "social");
    let second_scope = scope("acme", "billing", "social");

    let first = GraphCluster::open_cells_standalone_writers_scoped(
        "tenant-graphs",
        first_scope.clone(),
        ["cell-a"],
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    let second = GraphCluster::open_cells_standalone_writers_scoped(
        "tenant-graphs",
        second_scope.clone(),
        ["cell-a"],
        Arc::clone(&object_store),
    )
    .await
    .unwrap();

    first
        .shard("cell-a")
        .unwrap()
        .write_edge(EdgeMutation {
            cell_id: "cell-a".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 10,
            idempotency_key: "same-request-id".to_string(),
        })
        .await
        .unwrap();
    second
        .shard("cell-a")
        .unwrap()
        .write_edge(EdgeMutation {
            cell_id: "cell-a".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 20,
            idempotency_key: "same-request-id".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(
        first
            .shard("cell-a")
            .unwrap()
            .out_neighbors("cell-a", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![10]
    );
    assert_eq!(
        second
            .shard("cell-a")
            .unwrap()
            .out_neighbors("cell-a", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![20]
    );
    first.close().await.unwrap();
    second.close().await.unwrap();

    let reopened = GraphCluster::open_cells_scoped(
        "tenant-graphs",
        first_scope,
        ["cell-a"],
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .shard("cell-a")
            .unwrap()
            .out_neighbors("cell-a", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![10]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn default_scope_reuses_the_legacy_storage_path() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let scoped = GraphCluster::open_cells_standalone_writers_scoped(
        "legacy-compatible",
        GraphScope::default(),
        ["cell-a"],
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    scoped
        .shard("cell-a")
        .unwrap()
        .write_edge(EdgeMutation {
            cell_id: "cell-a".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "default-scope-write".to_string(),
        })
        .await
        .unwrap();
    scoped.close().await.unwrap();

    let legacy =
        GraphCluster::open_cells("legacy-compatible", ["cell-a"], Arc::clone(&object_store))
            .await
            .unwrap();
    assert_eq!(
        legacy
            .shard("cell-a")
            .unwrap()
            .out_neighbors("cell-a", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![2]
    );
    legacy.close().await.unwrap();

    let scoped_control = GraphControlPlane::open_scoped(
        "legacy-control",
        Arc::clone(&object_store),
        GraphScope::default(),
    )
    .await
    .unwrap();
    scoped_control
        .publish_placement(&ShardPlacement::fixed([("cell-a", "node-a")]).unwrap())
        .await
        .unwrap();
    scoped_control.close().await.unwrap();
    let legacy_control = GraphControlPlane::open("legacy-control", object_store)
        .await
        .unwrap();
    assert_eq!(
        legacy_control
            .load_placement()
            .await
            .unwrap()
            .owner("cell-a")
            .unwrap(),
        "node-a"
    );
    legacy_control.close().await.unwrap();
}

#[tokio::test]
async fn scoped_control_planes_isolate_placement_lease_and_catalog_state() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first_scope = scope("acme", "search", "social");
    let second_scope = scope("acme", "billing", "social");
    let first =
        GraphControlPlane::open_scoped("control", Arc::clone(&object_store), first_scope.clone())
            .await
            .unwrap();
    let second =
        GraphControlPlane::open_scoped("control", Arc::clone(&object_store), second_scope.clone())
            .await
            .unwrap();

    first
        .publish_placement(&ShardPlacement::fixed([("cell-a", "node-search")]).unwrap())
        .await
        .unwrap();
    second
        .publish_placement(&ShardPlacement::fixed([("cell-a", "node-billing")]).unwrap())
        .await
        .unwrap();
    assert_eq!(
        first
            .load_placement()
            .await
            .unwrap()
            .owner("cell-a")
            .unwrap(),
        "node-search"
    );
    assert_eq!(
        second
            .load_placement()
            .await
            .unwrap()
            .owner("cell-a")
            .unwrap(),
        "node-billing"
    );

    let first_lease = first
        .acquire_lease("cell-a", "node-search", std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let second_lease = second
        .acquire_lease("cell-a", "node-billing", std::time::Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(first_lease.owner_node_id, "node-search");
    assert_eq!(second_lease.owner_node_id, "node-billing");

    first
        .publish_scoped_placement_with_catalog(
            &ShardPlacement::fixed([("cell-a", "node-search")]).unwrap(),
            7,
        )
        .await
        .unwrap();
    assert_eq!(
        first
            .current_shard_metadata("cell-a")
            .await
            .unwrap()
            .unwrap()
            .graph_id
            .as_deref(),
        Some(first_scope.graph_id.as_str())
    );
    let mismatch = first
        .publish_placement_with_catalog(
            &ShardPlacement::fixed([("cell-a", "node-search")]).unwrap(),
            "another-graph",
            8,
        )
        .await
        .unwrap_err();
    assert!(matches!(mismatch, GraphError::GraphScopeMismatch { .. }));

    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn routed_queries_reject_a_context_from_another_graph_scope() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let expected_scope = scope("acme", "search", "social");
    let actual_scope = scope("acme", "billing", "social");
    let cluster = RoutedGraphCluster::open_owned_scoped(
        "routed-scope",
        expected_scope.clone(),
        "node-a",
        ShardPlacement::fixed([("cell-a", "node-a")]).unwrap(),
        object_store,
    )
    .await
    .unwrap();

    let error = cluster
        .execute_query_statement(
            QueryContext::new("cell-a", "cross-scope-query").in_scope(actual_scope.clone()),
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: false,
            },
        )
        .await
        .unwrap_err();
    match error {
        GraphError::GraphScopeMismatch { expected, actual } => {
            assert_eq!(expected, expected_scope.to_string());
            assert_eq!(actual, actual_scope.to_string());
        }
        other => panic!("expected graph scope mismatch, received {other}"),
    }
    cluster.close().await.unwrap();
}

#[cfg(feature = "query-transport")]
struct StaticNamespaceQueryClient;

#[cfg(feature = "query-transport")]
#[async_trait::async_trait]
impl QueryCellClient for StaticNamespaceQueryClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("scope")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::String(context.scope.to_string()),
            )])],
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
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn transport_tokens_are_confined_to_granted_namespace_and_graph_scopes() {
    let tenant = namespace("acme");
    let search = scope("acme", "search", "social");
    let billing = scope("acme", "billing", "ledger");
    let billing_other_graph = scope("acme", "billing", "analytics");
    let other_tenant = scope("other", "search", "social");
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "tenant-token",
            QueryTransportScopeGrant::read_namespace(tenant, true),
        )
        .unwrap()
        .with_bearer_grant(
            "billing-token",
            QueryTransportScopeGrant::read_graph(billing.clone()),
        )
        .unwrap();
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticNamespaceQueryClient),
        QueryTransportServerConfig::default()
            .with_required_bearer_tokens(["tenant-token".to_string(), "billing-token".to_string()])
            .with_scope_authorizer(Arc::new(authorizer))
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();

    let tenant_client = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("tenant-token")
        .insecure_allow_plaintext();
    let result = tenant_client
        .execute_cypher_rows(
            QueryContext::new("cell-a", "tenant-child-read").in_scope(search.clone()),
            "RETURN 1",
        )
        .await
        .unwrap();
    assert_eq!(
        result.rows[0].values[0],
        QueryValue::Property(VertexPropertyValue::String(search.to_string()))
    );
    let denied = tenant_client
        .execute_cypher_rows(
            QueryContext::new("cell-a", "tenant-cross-read").in_scope(other_tenant),
            "RETURN 1",
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, GraphError::UnsupportedQuery { .. }));
    assert!(denied.to_string().contains("not authorized"));

    let billing_client = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("billing-token")
        .insecure_allow_plaintext();
    billing_client
        .execute_cypher_rows(
            QueryContext::new("cell-a", "billing-exact-read").in_scope(billing),
            "RETURN 1",
        )
        .await
        .unwrap();
    let denied = billing_client
        .execute_cypher_rows(
            QueryContext::new("cell-a", "billing-other-graph").in_scope(billing_other_graph),
            "RETURN 1",
        )
        .await
        .unwrap_err();
    assert!(denied.to_string().contains("not authorized"));

    let metrics = server.metrics();
    assert_eq!(metrics.auth_failures, 0);
    assert_eq!(metrics.namespace_access_denials, 2);
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
struct NamespaceConcurrencyQueryClient {
    active: std::sync::atomic::AtomicUsize,
    max_active: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "query-transport")]
#[async_trait::async_trait]
impl QueryCellClient for NamespaceConcurrencyQueryClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        let active = self
            .active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.max_active
            .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        self.active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        Ok(QueryResultSet::new(Vec::new(), Vec::new()))
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
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn parent_namespace_quota_limits_queries_across_subtenants() {
    let tenant = namespace("acme");
    let search = scope("acme", "search", "social");
    let billing = scope("acme", "billing", "ledger");
    let query_client = Arc::new(NamespaceConcurrencyQueryClient {
        active: std::sync::atomic::AtomicUsize::new(0),
        max_active: std::sync::atomic::AtomicUsize::new(0),
    });
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "tenant-token",
            QueryTransportScopeGrant::read_namespace(tenant.clone(), true),
        )
        .unwrap();
    let quotas = QueryTransportNamespaceQuotas::new().with_query_limit(tenant, 1);
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        query_client.clone(),
        QueryTransportServerConfig::default()
            .with_required_bearer_token("tenant-token")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_namespace_quotas(quotas)
            .with_max_concurrent_requests(8)
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for (query_id, graph_scope) in [("search-query", search), ("billing-query", billing)] {
        let barrier = Arc::clone(&barrier);
        let client = TcpQueryCellClient::new(server.local_addr())
            .with_bearer_token("tenant-token")
            .insecure_allow_plaintext();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .execute_cypher_rows(
                    QueryContext::new("cell-a", query_id).in_scope(graph_scope),
                    "RETURN 1",
                )
                .await
        }));
    }
    barrier.wait().await;
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(
        query_client
            .max_active
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(server.metrics().namespace_quota_waits >= 1);
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
struct ScopeCancellationQueryClient {
    started: std::sync::atomic::AtomicUsize,
    started_notify: tokio::sync::Notify,
}

#[cfg(feature = "query-transport")]
#[async_trait::async_trait]
impl QueryCellClient for ScopeCancellationQueryClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        if self
            .started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
            >= 2
        {
            self.started_notify.notify_waiters();
        }
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        Ok(QueryResultSet::new(Vec::new(), Vec::new()))
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
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn query_cancellation_is_isolated_by_graph_scope() {
    let tenant = namespace("acme");
    let search = scope("acme", "search", "social");
    let billing = scope("acme", "billing", "ledger");
    let query_client = Arc::new(ScopeCancellationQueryClient {
        started: std::sync::atomic::AtomicUsize::new(0),
        started_notify: tokio::sync::Notify::new(),
    });
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "tenant-token",
            QueryTransportScopeGrant::read_namespace(tenant, true),
        )
        .unwrap();
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        query_client.clone(),
        QueryTransportServerConfig::default()
            .with_required_bearer_token("tenant-token")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_concurrent_requests(4)
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let client = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("tenant-token")
        .insecure_allow_plaintext();

    let search_task = {
        let client = client.clone();
        let search = search.clone();
        tokio::spawn(async move {
            client
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "shared-query-id").in_scope(search),
                    "RETURN 1",
                )
                .await
        })
    };
    let billing_task = {
        let client = client.clone();
        let billing = billing.clone();
        tokio::spawn(async move {
            client
                .execute_cypher_rows(
                    QueryContext::new("cell-a", "shared-query-id").in_scope(billing),
                    "RETURN 1",
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if query_client
                .started
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 2
            {
                break;
            }
            query_client.started_notify.notified().await;
        }
    })
    .await
    .unwrap();

    client
        .cancel_query_in_scope(search, "shared-query-id")
        .await
        .unwrap();
    let search_error = search_task.await.unwrap().unwrap_err();
    assert!(search_error
        .to_string()
        .contains("query_transport_cancelled"));
    billing_task.await.unwrap().unwrap();
    assert_eq!(server.metrics().cancellations, 1);
    server.stop().await.unwrap();
}
