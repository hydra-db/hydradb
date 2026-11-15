use super::*;
use crate::{
    GraphId, NamespaceId, NamespacePath, QueryCellClient, QueryColumn, QueryContext, QueryResultPage, QueryResultSet, QueryRow,
    QueryTransportScopeGrant, StaticQueryTransportScopeAuthorizer, StaticQueryTransportTlsServerConfigProvider,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

#[test]
fn byte_values_keep_their_uppercase_hex_json_shape() {
    for (bytes, expected) in [(vec![], ""), (vec![0, 0xA5, 0xFF], "00A5FF")] {
        let value = http_query_value(&QueryValue::Bytes(bytes)).unwrap();
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({"type": "bytes", "value": expected})
        );
    }
}

struct HttpTestClient {
    observed_epochs: Mutex<Vec<Option<u64>>>,
    refreshes: AtomicU64,
}

#[async_trait]
impl QueryCellClient for HttpTestClient {
    async fn execute_cypher_rows(&self, context: QueryContext, _query: &str) -> Result<QueryResultSet> {
        self.observed_epochs.lock().await.push(context.read_epoch);
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("n")],
            (1..=3).map(|value| QueryRow::new(vec![QueryValue::Count(value)])).collect(),
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
        let all_rows: Vec<_> = (1..=3).map(|value| QueryRow::new(vec![QueryValue::Count(value)])).collect();
        let end = (offset + page_size).min(all_rows.len());
        let next_cursor = (end < all_rows.len()).then(|| QueryCursorToken::new(end as u64));
        Ok(QueryResultPage::new(
            vec![QueryColumn::new("n")],
            all_rows[offset..end].to_vec(),
            next_cursor,
        ))
    }

    async fn current_storage_sequence(&self, _scope: &GraphScope, _cell_id: &str) -> Result<Option<u64>> {
        Ok(Some(11))
    }

    async fn refresh_storage_sequence(&self, _scope: &GraphScope, _cell_id: &str) -> Result<Option<u64>> {
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
        .with_bearer_grant("http-secret", QueryTransportScopeGrant::read_graph(http_scope()))
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

#[test]
fn typed_http_parameters_preserve_native_dates_and_lists() {
    let body: HttpQueryRequestBody = serde_json::from_value(serde_json::json!({
        "cell_id": "cell-0",
        "query": "RETURN $joined, $tags",
        "typed_parameters": {
            "joined": {"type": "date", "value": "2026-10-27"},
            "tags": {"type": "list", "value": [
                {"type": "string", "value": "engineer"},
                {"type": "string", "value": "database"}
            ]}
        }
    }))
    .unwrap();
    assert_eq!(
        http_typed_parameter_to_property("joined", &body.typed_parameters["joined"])
            .ok()
            .unwrap(),
        VertexPropertyValue::Date(20_753)
    );
    assert_eq!(
        http_typed_parameter_to_property("tags", &body.typed_parameters["tags"])
            .ok()
            .unwrap(),
        VertexPropertyValue::List(crate::VertexPropertyList::String(vec!["engineer".into(), "database".into()]))
    );
    assert_eq!(
        http_typed_parameter_to_query_value("tags", &body.typed_parameters["tags"])
            .ok()
            .unwrap(),
        QueryParameterValue::List(vec![
            QueryParameterValue::Scalar(VertexPropertyValue::String("engineer".into())),
            QueryParameterValue::Scalar(VertexPropertyValue::String("database".into())),
        ])
    );
}

#[test]
fn typed_http_lists_reject_mixed_nested_and_invalid_values() {
    for value in [
        serde_json::json!({"type": "list", "value": [
            {"type": "string", "value": "engineer"},
            {"type": "integer", "value": 1}
        ]}),
        serde_json::json!({"type": "list", "value": [
            {"type": "list", "value": []}
        ]}),
        serde_json::json!({"type": "date", "value": "2026-02-29"}),
    ] {
        assert!(http_typed_parameter_to_property("invalid", &value).is_err());
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
#[test]
fn canonical_http_dates_keep_typed_result_shapes() {
    let date = hydradb_graph_types::DateValue::parse_iso("2026-10-27").unwrap();
    assert_eq!(
        http_graph_value(hydradb_graph_types::CypherValue::Date(date)).unwrap(),
        HttpQueryValue::Date("2026-10-27".into())
    );
    assert_eq!(
        http_graph_value(hydradb_graph_types::CypherValue::List(vec![
            hydradb_graph_types::CypherValue::Date(date)
        ]))
        .unwrap(),
        HttpQueryValue::List(vec![HttpQueryValue::Date("2026-10-27".into())])
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
            gql: None,
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
            gql: None,
        },
    })
    .unwrap();
    assert!(shed_body["error"].get("owner").is_none());
}

/// The turbolay proxy retries a 421 from the response head, so the owner is
/// in `x-graph-owner` too, and absent when there is no hint.
#[test]
fn a_421_names_the_owner_in_a_header_the_proxy_reads() {
    let hinted = HttpApiError::from_graph(GraphError::NotCellWriter {
        cell_id: "cell-a".to_string(),
        owner: Some("node-b".to_string()),
    })
    .into_response();
    assert_eq!(hinted.status(), StatusCode::MISDIRECTED_REQUEST);
    assert_eq!(
        hinted
            .headers()
            .get(hydradb_protocol::http::GRAPH_OWNER_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("node-b")
    );
    let shed = HttpApiError::from_graph(GraphError::NotCellWriter {
        cell_id: "cell-a".to_string(),
        owner: None,
    })
    .into_response();
    assert!(shed.headers().get(hydradb_protocol::http::GRAPH_OWNER_HEADER).is_none());
}

/// The proxy retries a refused Bolt write at the owner it parses from the
/// message, so every node id graph-node accepts at startup has to survive that
/// parse. Checked one character at a time, which catches the parser's copy of
/// the key alphabet falling behind placement's.
#[test]
fn every_valid_node_id_round_trips_through_the_proxy_owner_parser() {
    let message = |owner: Option<&str>| {
        GraphError::NotCellWriter {
            cell_id: "cell-a".to_string(),
            owner: owner.map(str::to_string),
        }
        .to_string()
    };
    for byte in 0_u8..=255 {
        let owner = format!("node{}", char::from(byte));
        if hydradb_placement::heartbeat::validate_node_id(&owner).is_ok() {
            assert_eq!(
                hydradb_protocol::owner::parse_owner_hint(&message(Some(&owner))),
                Some(owner.as_str())
            );
        }
    }
    assert_eq!(hydradb_protocol::owner::parse_owner_hint(&message(None)), None);
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

/// Active queries are keyed by id across the service, so a node with a public
/// and an internal listener must not issue `http-query-1` on both.
#[tokio::test]
async fn two_http_listeners_never_issue_the_same_query_id() {
    let service = http_service(Arc::new(HttpTestClient {
        observed_epochs: Mutex::new(Vec::new()),
        refreshes: AtomicU64::new(0),
    }));
    let config = HttpQueryServerConfig::default()
        .with_default_page_size(8)
        .insecure_allow_plaintext();
    let first = ClientHttpServer::bind("127.0.0.1:0".parse().unwrap(), service.clone(), config.clone())
        .await
        .unwrap();
    let second = ClientHttpServer::bind("127.0.0.1:0".parse().unwrap(), service, config)
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let mut ids = Vec::new();
    for server in [&first, &second, &first, &second] {
        let body: serde_json::Value = client
            .post(format!("http://{}/v1/graphs/social/query", server.local_addr()))
            .bearer_auth("http-secret")
            .header(GRAPH_NAMESPACE_HEADER, "acme")
            .json(&serde_json::json!({
                "cell_id": "cell-a",
                "query": "MATCH (n {id: 1}) RETURN n.id"
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let id = body["query_id"].as_str().expect("a generated query id").to_string();
        assert!(id.starts_with("http-query-"), "{id}");
        ids.push(id);
    }
    let distinct: std::collections::BTreeSet<_> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len(), "{ids:?}");
    first.stop().await.unwrap();
    second.stop().await.unwrap();
}

/// HTTP rejects `read_epoch` before the query service sees the request, so it
/// has to count the failure itself or the dashboard never shows it.
#[tokio::test]
async fn an_http_read_epoch_rejection_is_counted_as_a_prepare_failure() {
    let backend = Arc::new(HttpTestClient {
        observed_epochs: Mutex::new(Vec::new()),
        refreshes: AtomicU64::new(0),
    });
    let service = http_service(backend);
    let server = ClientHttpServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        HttpQueryServerConfig::default()
            .with_default_page_size(2)
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/graphs/social/query", server.local_addr()))
        .bearer_auth("http-secret")
        .header(GRAPH_NAMESPACE_HEADER, "acme")
        .json(&serde_json::json!({
            "cell_id": "cell-a",
            "query": "MATCH (n {id: 1}) RETURN n.id",
            "read_epoch": 7
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_client_error(), "{}", response.status());
    let counted: u64 = service
        .metrics()
        .failure_counter_fields()
        .filter(|(_, stage, reason, _)| (*stage, *reason) == ("prepare", "invalid_request"))
        .map(|(.., count)| count)
        .sum();
    assert_eq!(counted, 1);
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
        .post(format!("http://{}/v1/graphs/social/query", server.local_addr()))
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
        .post(format!("http://{}/v1/graphs/social/query", server.local_addr()))
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
            .with_tls_provider(Arc::new(StaticQueryTransportTlsServerConfigProvider::new(tls.server))),
    )
    .await
    .unwrap();
    let certificate = reqwest::Certificate::from_der(&tls.certificate_der).unwrap();
    let client = reqwest::Client::builder().add_root_certificate(certificate).build().unwrap();
    let response = client
        .post(format!("https://localhost:{}/v1/graphs/social/query", server.local_addr().port()))
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

/// A write-back in progress is retryable, not a bad request: 503 with its
/// own code, the HTTP side of the Bolt mapping's `TransientError`.
#[test]
fn a_write_back_in_progress_refusal_is_a_503_and_not_a_bad_request() {
    let error = hydradb_graph_slate::write::write_back_in_progress("rank", 7);
    let expected = error.to_string();
    let response = HttpApiError::from_graph(error);
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.code, "write_back_in_progress");
    assert_eq!(response.message, expected);
}

/// An index generation behind the WAL is retryable, not a bad request: 503
/// with its own code, the HTTP side of the Bolt mapping's `TransientError`.
#[test]
fn an_index_behind_refusal_is_a_503_and_not_a_bad_request() {
    let error = GraphError::UnsupportedQuery {
        dialect: "OpenCypher",
        feature: "index_behind: the WAL tail of type LINKS from its generation at 4 to the read epoch 9 cannot be read".into(),
        reason: crate::QueryFailureReason::IndexBehind,
    };
    let expected = error.to_string();
    let response = HttpApiError::from_graph(error);
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.code, "index_behind");
    assert_eq!(response.message, expected);
}

#[test]
fn unavailable_snapshot_reaches_http_as_a_snapshot_conflict() {
    let error = GraphError::SnapshotUnavailable {
        cell_id: "cell-a".into(),
        read_epoch: 7,
        current_epoch: 9,
    };
    let expected = error.to_string();
    let response = HttpApiError::from_graph(error);
    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(response.code, "snapshot_unavailable");
    assert_eq!(response.message, expected);
}

#[cfg(feature = "datafusion-cypher-engine")]
#[test]
fn invalid_provider_values_reach_http_as_bad_requests() {
    let response = HttpApiError::from_graph(GraphError::DataFusionProvider(
        hydradb_graph_provider::GraphProviderError::InvalidRequest("property lists must contain one element type".into()),
    ));

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(response.code, "invalid_request");
    assert!(response.message.contains("property lists must contain one element type"));
}

/// An engine failure's HTTP error body carries its GQL-status object beside
/// `code` and `message`; other failures' bodies are as they were.
#[test]
fn an_engine_failure_carries_its_gql_status_in_the_http_error_body() {
    let error = HttpApiError::from_graph(GraphError::GqlQuery {
        dialect: "Cypher25",
        feature: "RETURN references out-of-scope binding m".to_string(),
        reason: crate::QueryFailureReason::Return,
        status: Box::new(crate::GqlStatusReport {
            gql_status: "42002".to_string(),
            description: "error: syntax error or access rule violation - invalid reference".to_string(),
            diagnostic: BTreeMap::from([("INVALID_REFERENCE".to_string(), "m".to_string())]),
        }),
    });
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    let body = serde_json::to_value(HttpErrorEnvelope {
        error: HttpErrorBody {
            code: error.code,
            message: error.message,
            owner: error.owner,
            gql: error.gql.as_deref().map(HttpGqlStatus::from),
        },
    })
    .unwrap();
    assert_eq!(body["error"]["code"], serde_json::json!("invalid_request"));
    assert_eq!(body["error"]["gql_status"], serde_json::json!("42002"));
    assert_eq!(
        body["error"]["description"],
        serde_json::json!("error: syntax error or access rule violation - invalid reference")
    );
    assert_eq!(body["error"]["diagnostic_record"]["INVALID_REFERENCE"], serde_json::json!("m"));

    let plain = HttpApiError::from_graph(GraphError::QueryParse {
        dialect: "OpenCypher",
        reason: "bad".to_string(),
    });
    assert!(plain.gql.is_none());
}

#[cfg(feature = "datafusion-cypher-engine")]
#[test]
fn canonical_graph_values_use_typed_http_scalars() {
    let encoded = hydradb_graph_types::encode_value(&hydradb_graph_types::CypherValue::Integer(7)).unwrap();
    let value = http_query_value(&QueryValue::GraphValue(encoded)).unwrap();
    let json = serde_json::to_value(value).unwrap();
    assert_eq!(json, serde_json::json!({"type": "integer", "value": 7}));

    for (value, expected) in [
        (
            hydradb_graph_types::CypherValue::Decimal("1.50".to_owned()),
            serde_json::json!({"type": "decimal", "value": "1.50"}),
        ),
        (
            hydradb_graph_types::CypherValue::Bytes(vec![0, 1, 0xff]),
            serde_json::json!({"type": "bytes", "value": "0001FF"}),
        ),
    ] {
        let encoded = hydradb_graph_types::encode_value(&value).unwrap();
        let value = http_query_value(&QueryValue::GraphValue(encoded)).unwrap();
        assert_eq!(serde_json::to_value(value).unwrap(), expected);
    }
}

#[cfg(feature = "datafusion-cypher-engine")]
#[test]
fn nested_record_and_temporal_http_json_survive_canonical_replay() {
    use hydradb_graph_types::{CypherTemporal, CypherValue};

    let before = QueryValue::Record(vec![(
        "nested".to_owned(),
        QueryValue::Record(vec![
            (
                "date".to_owned(),
                QueryValue::Temporal(crate::query::algebra::QueryTemporal {
                    kind: crate::query::algebra::QueryTemporalKind::Date { days: 20_000 },
                    text: "2024-10-04".to_owned(),
                }),
            ),
            (
                "timestamp".to_owned(),
                QueryValue::Temporal(crate::query::algebra::QueryTemporal {
                    kind: crate::query::algebra::QueryTemporalKind::ZonedDatetime {
                        seconds: 1_728_000_000,
                        nanoseconds: 123_000_000,
                        offset_seconds: 3_600,
                    },
                    text: "2024-10-04T10:40:00.123+01:00".to_owned(),
                }),
            ),
        ]),
    )]);
    let canonical = CypherValue::Record(BTreeMap::from([(
        "nested".to_owned(),
        CypherValue::Record(BTreeMap::from([
            (
                "date".to_owned(),
                CypherValue::Temporal(CypherTemporal::Date {
                    days: 20_000,
                    text: "2024-10-04".to_owned(),
                }),
            ),
            (
                "timestamp".to_owned(),
                CypherValue::Temporal(CypherTemporal::ZonedDateTime {
                    seconds: 1_728_000_000,
                    nanoseconds: 123_000_000,
                    offset_seconds: 3_600,
                    text: "2024-10-04T10:40:00.123+01:00".to_owned(),
                }),
            ),
        ])),
    )]));
    let encoded = hydradb_graph_types::encode_value(&canonical).unwrap();
    let replayed = QueryValue::GraphValue(encoded);

    let before_json = serde_json::to_value(http_query_value(&before).unwrap()).unwrap();
    let replayed_json = serde_json::to_value(http_query_value(&replayed).unwrap()).unwrap();
    assert_eq!(replayed_json, before_json);
}

#[cfg(feature = "datafusion-cypher-engine")]
#[test]
fn canonical_entities_keep_labels_and_properties_in_http_values() {
    let entity = hydradb_graph_types::CypherValue::NodeEntity(hydradb_graph_types::NodeEntity {
        id: hydradb_graph_types::NodeId(7),
        labels: vec!["Person".to_string()],
        properties: [("name".to_string(), hydradb_graph_types::CypherValue::String("Ada".to_string()))]
            .into_iter()
            .collect(),
    });
    let encoded = hydradb_graph_types::encode_value(&entity).unwrap();
    let value = http_query_value(&QueryValue::GraphValue(encoded)).unwrap();
    assert_eq!(
        serde_json::to_value(value).unwrap(),
        serde_json::json!({
            "type": "node",
            "value": {
                "id": 7,
                "labels": ["Person"],
                "properties": {"name": {"type": "string", "value": "Ada"}}
            }
        })
    );

    let entity = hydradb_graph_types::CypherValue::RelationshipEntity(hydradb_graph_types::RelationshipEntity {
        identity: hydradb_graph_types::RelationshipRef::identified(
            hydradb_graph_types::RelationshipId(9),
            hydradb_graph_types::NodeId(7),
            hydradb_graph_types::NodeId(8),
            "KNOWS",
        ),
        properties: [("since".to_string(), hydradb_graph_types::CypherValue::Integer(2010))]
            .into_iter()
            .collect(),
    });
    let encoded = hydradb_graph_types::encode_value(&entity).unwrap();
    let value = http_query_value(&QueryValue::GraphValue(encoded)).unwrap();
    assert_eq!(
        serde_json::to_value(value).unwrap(),
        serde_json::json!({
            "type": "relationship",
            "value": {
                "id": 9,
                "start_node_id": 7,
                "end_node_id": 8,
                "relationship_type": "KNOWS",
                "properties": {"since": {"type": "integer", "value": 2010}}
            }
        })
    );
}
