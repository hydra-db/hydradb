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

/// Touch point (c) for HTTP: 421 Misdirected Request, with the owner in the
/// body because an HTTP client has no routing table to discard and re-fetch.
/// The hint is *absent* rather than null when this node has shed its view —
/// there is nothing truthful to point at, and an absent field says that where a
/// null would invite a client to route to "unknown".
#[test]
fn a_write_to_a_non_owner_is_a_421_naming_the_owner() {
    let hinted = HttpApiError::from_graph(GraphError::NotCellWriter {
        cell_id: "cell-a".to_string(),
        owner: Some("node-b".to_string()),
    });
    assert_eq!(hinted.status, StatusCode::MISDIRECTED_REQUEST);
    assert_eq!(hinted.code, "not_cell_writer");
    assert_eq!(hinted.owner.as_deref(), Some("node-b"));
    assert!(hinted.message.contains("node-b"));
    let body = serde_json::to_value(HttpErrorEnvelope {
        error: HttpErrorBody {
            code: hinted.code,
            message: hinted.message,
            owner: hinted.owner,
        },
    })
    .unwrap();
    assert_eq!(body["error"]["owner"], serde_json::json!("node-b"));

    let shed = HttpApiError::from_graph(GraphError::NotCellWriter {
        cell_id: "cell-a".to_string(),
        owner: None,
    });
    assert_eq!(shed.status, StatusCode::MISDIRECTED_REQUEST);
    assert_eq!(shed.owner, None);
    let shed_body = serde_json::to_value(HttpErrorEnvelope {
        error: HttpErrorBody {
            code: shed.code,
            message: shed.message,
            owner: shed.owner,
        },
    })
    .unwrap();
    assert!(shed_body["error"].get("owner").is_none());
}

/// A routing refusal is *this node* being unable to serve, not a bad request.
/// 503 rather than the catch-all's 500, so a client and any proxy in front of it
/// read it as retryable — the HTTP analogue of the `TransientError` class the
/// Bolt mapping uses, minus the router list an HTTP client does not have.
#[test]
fn a_routing_refusal_is_a_503_and_not_an_internal_error() {
    let unavailable = HttpApiError::from_graph(GraphError::RoutingUnavailable {
        reason: "no live node owns this cell".to_string(),
    });
    assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable.code, "routing_unavailable");
    assert_eq!(unavailable.owner, None);
    assert!(
        unavailable.message.contains("no live node owns this cell"),
        "the reason must survive to the client: {}",
        unavailable.message
    );
}

fn namespace_headers(values: &[&[u8]]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for value in values {
        headers.append(
            GRAPH_NAMESPACE_HEADER,
            HeaderValue::from_bytes(value).expect("valid header bytes"),
        );
    }
    headers
}

/// An absent namespace header and an unreadable one are different requests: the
/// first has opted into the server's default, the second is naming a namespace
/// and failing. Collapsing the second into the first sends the query to
/// `default` — a namespace the client never asked for — whenever
/// `allow_default_namespace` is on, which is the one configuration where the
/// mistake is silent rather than a 400. `0xff` is a legal header byte on the
/// wire and an illegal one for `HeaderValue::to_str`.
#[test]
fn an_unreadable_namespace_header_is_rejected_rather_than_defaulted() {
    let headers = namespace_headers(&[&[0xff]]);

    let error = http_graph_scope(&headers, "g".to_string(), true)
        .expect_err("a present but unreadable namespace header must not fall back to the default");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "invalid_namespace");
    assert!(
        error.message.contains(GRAPH_NAMESPACE_HEADER),
        "the client must be told which header it got wrong: {}",
        error.message
    );
}

/// With the default `allow_default_namespace = false` an unreadable header was
/// already refused, but as `missing_namespace` — the wrong diagnosis, because
/// the client did send the header. The status was right by accident; this keeps
/// the two answers distinguishable so a client can tell "you sent nothing" from
/// "what you sent is unreadable".
#[test]
fn an_unreadable_namespace_header_is_not_reported_as_a_missing_one() {
    let headers = namespace_headers(&[&[0xff]]);

    let error = http_graph_scope(&headers, "g".to_string(), false)
        .expect_err("an unreadable namespace header is refused however defaulting is configured");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "invalid_namespace");
}

/// `HeaderMap::get` returns the first value of a repeated header, so a request
/// naming two namespaces would have silently been served from whichever the
/// client happened to send first. Tenancy is not a field to guess at: refuse.
#[test]
fn a_repeated_namespace_header_is_rejected_rather_than_resolved_to_the_first() {
    let headers = namespace_headers(&[b"tenant-a", b"tenant-b"]);

    let error = http_graph_scope(&headers, "g".to_string(), true)
        .expect_err("two conflicting namespace headers must not resolve to the first");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "invalid_namespace");
}

/// The paths that were already correct, pinned so that refusing the unreadable
/// and the repeated header does not cost the readable, the absent, or the
/// readable-but-invalid one their existing answers.
#[test]
fn readable_absent_and_invalid_namespace_headers_keep_their_answers() {
    // `HttpApiError` is deliberately not `Debug` — it is a response, not a
    // diagnostic — so the success arms unwrap by hand rather than with
    // `expect`, and report the message the client would have seen.
    let headers = namespace_headers(&[b"tenant-a/team-b"]);
    let named = http_graph_scope(&headers, "g".to_string(), true)
        .unwrap_or_else(|error| panic!("a readable namespace must resolve: {}", error.message));
    assert_eq!(
        named.namespace,
        NamespacePath::new([
            NamespaceId::new("tenant-a").unwrap(),
            NamespaceId::new("team-b").unwrap(),
        ])
        .unwrap()
    );
    assert_eq!(named.graph_id, GraphId::new("g").unwrap());

    let defaulted = http_graph_scope(&HeaderMap::new(), "g".to_string(), true)
        .unwrap_or_else(|error| panic!("an absent header must still default: {}", error.message));
    assert_eq!(defaulted.namespace, NamespacePath::default());

    let missing = http_graph_scope(&HeaderMap::new(), "g".to_string(), false)
        .expect_err("an absent header is still required when defaulting is off");
    assert_eq!(missing.status, StatusCode::BAD_REQUEST);
    assert_eq!(missing.code, "missing_namespace");

    // Readable but not a legal component: rejected by `NamespaceId::new`, which
    // is a path the unreadable case never reached.
    let invalid = http_graph_scope(&namespace_headers(&[b"tenant a"]), "g".to_string(), true)
        .expect_err("a readable namespace is still validated");
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
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
