use super::*;
use crate::{
    QueryCellClient, QueryColumn, QueryContext, QueryResultPage, QueryResultSet, QueryRow,
    QueryTransportScopeGrant, StaticQueryTransportScopeAuthorizer,
    StaticQueryTransportTlsServerConfigProvider,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

struct HttpTestClient {
    observed_epochs: Mutex<Vec<Option<u64>>>,
    refreshes: AtomicU64,
}

#[async_trait]
impl QueryCellClient for HttpTestClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        self.observed_epochs.lock().await.push(context.read_epoch);
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("n")],
            (1..=3)
                .map(|value| QueryRow::new(vec![QueryValue::Count(value)]))
                .collect(),
        )
        .with_read_epoch(11)
        .with_storage_sequence(11))
    }

    async fn execute_cypher_rows_page(
        &self,
        context: QueryContext,
        _query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        self.observed_epochs.lock().await.push(context.read_epoch);
        let offset = cursor.map_or(0, |cursor| cursor.offset as usize);
        let all_rows: Vec<_> = (1..=3)
            .map(|value| QueryRow::new(vec![QueryValue::Count(value)]))
            .collect();
        let end = (offset + page_size).min(all_rows.len());
        let next_cursor = (end < all_rows.len()).then(|| QueryCursorToken::new(end as u64));
        Ok(QueryResultPage::new(
            vec![QueryColumn::new("n")],
            all_rows[offset..end].to_vec(),
            next_cursor,
        ))
    }

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<u64>> {
        Ok(Some(11))
    }

    async fn refresh_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<u64>> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(Some(11))
    }
}

fn http_scope() -> GraphScope {
    GraphScope::new(
        NamespacePath::new([NamespaceId::new("acme").unwrap()]).unwrap(),
        GraphId::new("social").unwrap(),
    )
}

fn http_service(client: Arc<HttpTestClient>) -> ClientQueryService {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "http-secret",
            QueryTransportScopeGrant::read_graph(http_scope()),
        )
        .unwrap();
    ClientQueryService::new(
        client,
        crate::ClientQueryServiceConfig::default()
            .with_required_bearer_token("http-secret")
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_page_size(8),
    )
    .unwrap()
}

#[test]
fn http_parameters_preserve_integer_sign_and_precision() {
    assert_eq!(
        http_parameter_to_property("large", &serde_json::json!(9007199254740993_u64)).ok(),
        Some(VertexPropertyValue::Integer(9007199254740993))
    );
    assert_eq!(
        http_parameter_to_property("negative", &serde_json::json!(-1)).ok(),
        Some(VertexPropertyValue::SignedInteger(-1))
    );
    assert_eq!(
        http_parameter_to_property("fraction", &serde_json::json!(-1.5)).ok(),
        Some(VertexPropertyValue::Float(QueryFloat(-1.5)))
    );
}

#[tokio::test]
async fn http_api_enforces_auth_scope_and_returns_typed_json() {
    let backend = Arc::new(HttpTestClient {
        observed_epochs: Mutex::new(Vec::new()),
        refreshes: AtomicU64::new(0),
    });
    let server = ClientHttpServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        http_service(backend),
        HttpQueryServerConfig::default()
            .with_default_page_size(2)
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let url = format!("http://{}/v1/graphs/social/query", server.local_addr());
    let client = reqwest::Client::new();
    let unauthorized = client
        .post(&url)
        .header(GRAPH_NAMESPACE_HEADER, "acme")
        .json(&serde_json::json!({
            "cell_id": "cell-a",
            "query": "MATCH (n {id: 1}) RETURN n.id"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let response = client
        .post(&url)
        .bearer_auth("http-secret")
        .header(GRAPH_NAMESPACE_HEADER, "acme")
        .json(&serde_json::json!({
            "cell_id": "cell-a",
            "query_id": "http-json-query",
            "query": "MATCH (n {id: 1}) RETURN n.id",
            "page_size": 2
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["query_id"], "http-json-query");
    assert_eq!(body["read_epoch"], 11);
    assert!(body["next_cursor"].as_u64().is_some());
    assert_eq!(body["rows"][0][0]["type"], "integer");
    assert_eq!(body["rows"][0][0]["value"], 1);
    assert!(body["bookmark"].as_str().unwrap().starts_with("sgk:1:"));
    server.stop().await.unwrap();
}

#[tokio::test]
async fn http_strong_consistency_refreshes_the_slatedb_reader() {
    let backend = Arc::new(HttpTestClient {
        observed_epochs: Mutex::new(Vec::new()),
        refreshes: AtomicU64::new(0),
    });
    let server = ClientHttpServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        http_service(Arc::clone(&backend)),
        HttpQueryServerConfig::default()
            .with_default_page_size(2)
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/graphs/social/query",
            server.local_addr()
        ))
        .bearer_auth("http-secret")
        .header(GRAPH_NAMESPACE_HEADER, "acme")
        .json(&serde_json::json!({
            "cell_id": "cell-a",
            "query_id": "http-strong-query",
            "query": "MATCH (n {id: 1}) RETURN n.id",
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(backend.refreshes.load(Ordering::SeqCst), 1);
    server.stop().await.unwrap();
}

#[tokio::test]
async fn ndjson_stream_uses_one_snapshot_backed_server_cursor() {
    let backend = Arc::new(HttpTestClient {
        observed_epochs: Mutex::new(Vec::new()),
        refreshes: AtomicU64::new(0),
    });
    let server = ClientHttpServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        http_service(Arc::clone(&backend)),
        HttpQueryServerConfig::default()
            .with_default_page_size(1)
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/graphs/social/query",
            server.local_addr()
        ))
        .bearer_auth("http-secret")
        .header(GRAPH_NAMESPACE_HEADER, "acme")
        .header(ACCEPT.as_str(), NDJSON_CONTENT_TYPE)
        .json(&serde_json::json!({
            "cell_id": "cell-a",
            "query_id": "http-stream-query",
            "query": "MATCH (n {id: 1}) RETURN n.id",
            "page_size": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with(NDJSON_CONTENT_TYPE));
    let lines: Vec<serde_json::Value> = response
        .text()
        .await
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines[0]["type"], "header");
    assert_eq!(lines.iter().filter(|line| line["type"] == "row").count(), 3);
    assert_eq!(lines.last().unwrap()["type"], "summary");
    assert_eq!(backend.observed_epochs.lock().await.as_slice(), &[None]);
    server.stop().await.unwrap();
}

#[tokio::test]
async fn http_api_serves_authenticated_queries_over_https() {
    let tls = crate::client::client_test_tls_bundle();
    let server = ClientHttpServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        http_service(Arc::new(HttpTestClient {
            observed_epochs: Mutex::new(Vec::new()),
            refreshes: AtomicU64::new(0),
        })),
        HttpQueryServerConfig::default()
            .with_default_page_size(2)
            .with_tls_provider(Arc::new(StaticQueryTransportTlsServerConfigProvider::new(
                tls.server,
            ))),
    )
    .await
    .unwrap();
    let certificate = reqwest::Certificate::from_der(&tls.certificate_der).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(certificate)
        .build()
        .unwrap();
    let response = client
        .post(format!(
            "https://localhost:{}/v1/graphs/social/query",
            server.local_addr().port()
        ))
        .bearer_auth("http-secret")
        .header(GRAPH_NAMESPACE_HEADER, "acme")
        .json(&serde_json::json!({
            "cell_id": "cell-a",
            "query": "MATCH (n {id: 1}) RETURN n.id",
            "page_size": 2
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    drop(response);
    drop(client);
    server.stop().await.unwrap();
}
