use super::values::bolt_parameter_to_property;
use super::wire::strict_decode_client_message;
use super::*;
use crate::{
    ClientQueryServiceConfig, ObjectStoreNodeDirectory, QueryCellClient, QueryColumn, QueryContext,
    QueryCursorToken, QueryFloat, QueryParameterValue, QueryResultPage, QueryResultSet, QueryRow,
    QueryTransportAction, QueryTransportScopeGrant, QueryValue, RoutedGraphCluster,
    StaticClientDatabaseResolver, StaticQueryTransportScopeAuthorizer,
    StaticQueryTransportTlsServerConfigProvider, VertexPropertyValue,
};
use boltr::chunk::{ChunkReader, ChunkWriter};
use boltr::client::BoltSession;
use boltr::message::encode::encode_client_message;
use boltr::message::sig;
use boltr::packstream::marker;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use std::collections::HashMap;
use tokio::io::{duplex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

struct BoltTestClient;

#[test]
fn bolt_consistency_metadata_accepts_only_causal_or_strong() {
    assert_eq!(
        bolt_read_consistency(&BoltDict::from([(
            "consistency".to_string(),
            BoltValue::String("strong".to_string()),
        )]))
        .unwrap(),
        Some(ClientReadConsistency::Strong)
    );
    assert_eq!(
        bolt_read_consistency(&BoltDict::from([(
            "tx_metadata".to_string(),
            BoltValue::Dict(BoltDict::from([(
                "turbolay.consistency".to_string(),
                BoltValue::String("causal".to_string()),
            )])),
        )]))
        .unwrap(),
        Some(ClientReadConsistency::Causal)
    );
    assert!(bolt_read_consistency(&BoltDict::from([(
        "consistency".to_string(),
        BoltValue::String("eventual".to_string()),
    )]))
    .is_err());
}

#[async_trait::async_trait]
impl QueryCellClient for BoltTestClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("answer")],
            vec![QueryRow::new(vec![QueryValue::Count(42)])],
        )
        .with_read_epoch(9)
        .with_storage_sequence(9))
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
    ) -> Result<Option<u64>> {
        Ok(Some(9))
    }
}

struct PagedBoltTestClient;

#[async_trait::async_trait]
impl QueryCellClient for PagedBoltTestClient {
    async fn execute_cypher_rows(
        &self,
        _context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        Ok(QueryResultSet::new(
            vec![QueryColumn::new("answer")],
            (1..=5)
                .map(|value| QueryRow::new(vec![QueryValue::Count(value)]))
                .collect(),
        )
        .with_read_epoch(9)
        .with_storage_sequence(9))
    }

    async fn execute_cypher_rows_page(
        &self,
        _context: QueryContext,
        _query: &str,
        cursor: Option<QueryCursorToken>,
        page_size: usize,
    ) -> Result<QueryResultPage> {
        let rows: Vec<_> = (1..=5)
            .map(|value| QueryRow::new(vec![QueryValue::Count(value)]))
            .collect();
        let offset = cursor.map_or(0, |cursor| cursor.offset as usize);
        let end = (offset + page_size).min(rows.len());
        Ok(QueryResultPage::new(
            vec![QueryColumn::new("answer")],
            rows[offset..end].to_vec(),
            (end < rows.len()).then(|| QueryCursorToken::new(end as u64)),
        ))
    }

    async fn current_storage_sequence(
        &self,
        _scope: &GraphScope,
        _cell_id: &str,
    ) -> Result<Option<u64>> {
        Ok(Some(9))
    }
}

struct BlockingBoltTestClient {
    started: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl QueryCellClient for BlockingBoltTestClient {
    async fn execute_cypher_rows(
        &self,
        context: QueryContext,
        _query: &str,
    ) -> Result<QueryResultSet> {
        let token = context
            .cancellation_token
            .ok_or_else(|| GraphError::CorruptValue {
                key: "bolt/test/cancellation".to_string(),
                reason: "missing cancellation token".to_string(),
            })?;
        self.started
            .store(true, std::sync::atomic::Ordering::SeqCst);
        token.cancelled().await;
        Err(GraphError::QueryTimeout {
            operation: "bolt_test_cancelled",
            elapsed_ms: 0,
            limit_ms: 0,
        })
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
    ) -> Result<Option<u64>> {
        Ok(Some(9))
    }
}

fn bolt_test_service() -> ClientQueryService {
    bolt_service_for(Arc::new(BoltTestClient))
}

fn bolt_service_for(client: Arc<dyn QueryCellClient>) -> ClientQueryService {
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "bolt-secret",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    ClientQueryService::new(
        client,
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("bolt-secret")
            .with_scope_authorizer(Arc::new(authorizer)),
    )
    .unwrap()
}

fn bolt_test_config() -> BoltServerConfig {
    let resolver = StaticClientDatabaseResolver::single(
        "default",
        ClientQueryTarget::new(GraphScope::default(), "cell-a").unwrap(),
    )
    .unwrap();
    BoltServerConfig::new(Arc::new(resolver)).insecure_allow_plaintext()
}

#[tokio::test]
async fn classic_handshake_selects_highest_supported_version_in_client_order() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { bolt_server_handshake(&mut server).await });
    client.write_all(&BOLT_MAGIC).await.unwrap();
    client
        .write_all(&[
            0, 2, 6, 5, // 5.6 through 5.4
            0, 0, 2, 5, // 5.2
            0, 0, 0, 0, 0, 0, 0, 0,
        ])
        .await
        .unwrap();
    let mut selected = [0_u8; 4];
    client.read_exact(&mut selected).await.unwrap();
    assert_eq!(selected, [0, 0, 4, 5]);
    assert_eq!(server_task.await.unwrap().unwrap(), (5, 4));
}

#[tokio::test]
async fn manifest_handshake_offers_and_validates_bolt_5_range() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { bolt_server_handshake(&mut server).await });
    client.write_all(&BOLT_MAGIC).await.unwrap();
    client
        .write_all(&[
            0, 0, 1, 0xff, // Manifest v1
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ])
        .await
        .unwrap();
    let mut response = [0_u8; 10];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0, 0, 1, 0xff, 1, 0, 3, 4, 5, 0]);
    client.write_all(&[0, 0, 4, 5, 0]).await.unwrap();
    assert_eq!(server_task.await.unwrap().unwrap(), (5, 4));
}

#[test]
fn bolt_value_conversion_rejects_unsigned_overflow() {
    assert!(query_value_to_bolt(&QueryValue::Count(u64::MAX)).is_err());
    assert_eq!(
        query_value_to_bolt(&QueryValue::Property(VertexPropertyValue::Float(
            QueryFloat(42.5)
        )))
        .unwrap(),
        BoltValue::Float(42.5)
    );
    assert_eq!(
        bolt_parameter_to_property("offset", &BoltValue::Integer(-1)).unwrap(),
        VertexPropertyValue::SignedInteger(-1)
    );
    assert_eq!(
        query_value_to_bolt(&QueryValue::Property(VertexPropertyValue::SignedInteger(
            -1
        )))
        .unwrap(),
        BoltValue::Integer(-1)
    );
}

#[test]
fn bolt_parameters_decode_nested_unwind_rows_without_changing_scalar_types() {
    let value = BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
        ("src".to_string(), BoltValue::Integer(7)),
        ("active".to_string(), BoltValue::Boolean(true)),
    ]))]);
    assert_eq!(
        bolt_parameter_to_query_value("rows", &value).unwrap(),
        QueryParameterValue::List(vec![QueryParameterValue::Map(BTreeMap::from([
            (
                "active".to_string(),
                QueryParameterValue::Scalar(VertexPropertyValue::Bool(true)),
            ),
            (
                "src".to_string(),
                QueryParameterValue::Scalar(VertexPropertyValue::Integer(7)),
            ),
        ]))])
    );
}

#[test]
fn strict_decoder_rejects_surplus_fields_and_excessive_nesting() {
    let mut hello = BytesMut::new();
    encode_client_message(
        &mut hello,
        &ClientMessage::Hello {
            extra: BoltDict::new(),
        },
    );
    hello.extend_from_slice(&[marker::NULL]);
    assert!(strict_decode_client_message(&hello)
        .unwrap_err()
        .to_string()
        .contains("trailing"));

    assert!(strict_decode_client_message(&[0xb0, sig::HELLO])
        .unwrap_err()
        .to_string()
        .contains("expected 1"));

    let mut nested = vec![0xb1, sig::HELLO];
    nested.extend(std::iter::repeat_n(0x91, BOLT_MAX_PACKSTREAM_DEPTH + 1));
    nested.push(marker::NULL);
    assert!(strict_decode_client_message(&nested)
        .unwrap_err()
        .to_string()
        .contains("nesting exceeds"));
}

#[tokio::test]
async fn bolt_server_runs_autocommit_queries_and_rejects_fake_transactions() {
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        bolt_test_service(),
        bolt_test_config(),
    )
    .await
    .unwrap();
    let mut session = BoltSession::connect_basic(server.local_addr(), "neo4j", "bolt-secret")
        .await
        .unwrap();
    assert_eq!(session.version(), (5, 4));
    let result = session
        .run("MATCH (n {id: 1}) RETURN n.id AS answer")
        .await
        .unwrap();
    assert_eq!(result.columns, vec!["answer"]);
    assert_eq!(result.records, vec![vec![BoltValue::Integer(42)]]);
    assert!(result
        .summary
        .get("bookmark")
        .and_then(BoltValue::as_str)
        .unwrap()
        .starts_with("sgk:1:"));

    let transaction_error = session.begin().await.unwrap_err();
    assert!(transaction_error
        .to_string()
        .contains("explicit transactions are not supported"));
    session.reset().await.unwrap();
    assert_eq!(
        session
            .run("MATCH (n {id: 1}) RETURN n.id AS answer")
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    session.close().await.unwrap();
    server.stop().await.unwrap();
}

#[tokio::test]
async fn bolt_server_executes_autocommit_create_on_routed_cluster() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let placement = ObjectStoreNodeDirectory::new(["cell-a"], ["node-a"]).unwrap();
    let cluster = Arc::new(
        RoutedGraphCluster::open_promotable("bolt-create/data", "node-a", placement, object_store)
            .await
            .unwrap(),
    );
    let scope = GraphScope::default();
    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "bolt-secret",
            QueryTransportScopeGrant::graph(
                scope.clone(),
                [
                    QueryTransportAction::Read,
                    QueryTransportAction::Write,
                    QueryTransportAction::Cancel,
                ],
            ),
        )
        .unwrap();
    let service = ClientQueryService::new(
        cluster.clone(),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token("bolt-secret")
            .with_scope_authorizer(Arc::new(authorizer)),
    )
    .unwrap();
    let resolver = StaticClientDatabaseResolver::single(
        "default",
        ClientQueryTarget::new(scope, "cell-a").unwrap(),
    )
    .unwrap();
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        service,
        BoltServerConfig::new(Arc::new(resolver)).insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let mut session = BoltSession::connect_basic(server.local_addr(), "neo4j", "bolt-secret")
        .await
        .unwrap();
    let result = session
        .run("CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})")
        .await
        .unwrap();
    assert!(result.columns.is_empty());
    assert!(result.records.is_empty());
    assert!(cluster
        .shard("cell-a")
        .unwrap()
        .edge_exists("cell-a", "FOLLOWS", 1, 2)
        .await
        .unwrap());

    let _ = session
        .run(
            "CREATE ({id: 1})-[:FOLLOWS]->({id: 3}), \
             ({id: 1})-[:FOLLOWS]->({id: 4})",
        )
        .await
        .unwrap();
    assert_eq!(
        cluster
            .shard("cell-a")
            .unwrap()
            .out_neighbors_batch("cell-a", "FOLLOWS", [1])
            .await
            .unwrap()[0]
            .neighbors,
        vec![2, 3, 4]
    );

    let _ = session
        .run("MATCH ({id: 1})-[r:FOLLOWS]->() DELETE r")
        .await
        .unwrap();
    assert!(cluster
        .shard("cell-a")
        .unwrap()
        .out_neighbors_batch("cell-a", "FOLLOWS", [1])
        .await
        .unwrap()[0]
        .neighbors
        .is_empty());

    let edge_rows = BoltValue::List(vec![
        BoltValue::Dict(BoltDict::from([
            ("src".to_string(), BoltValue::Integer(10)),
            ("dst".to_string(), BoltValue::Integer(20)),
        ])),
        BoltValue::Dict(BoltDict::from([
            ("src".to_string(), BoltValue::Integer(10)),
            ("dst".to_string(), BoltValue::Integer(21)),
        ])),
        BoltValue::Dict(BoltDict::from([
            ("src".to_string(), BoltValue::Integer(11)),
            ("dst".to_string(), BoltValue::Integer(22)),
        ])),
    ]);
    let _ = session
        .run_with_params(
            "UNWIND $edges AS edge \
             CREATE ({id: edge.src})-[:FOLLOWS]->({id: edge.dst})",
            BoltDict::from([("edges".to_string(), edge_rows.clone())]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let vertex_rows = BoltValue::List(vec![
        BoltValue::Dict(BoltDict::from([
            ("vertex".to_string(), BoltValue::Integer(70)),
            (
                "entity_id".to_string(),
                BoltValue::String("entity-a".to_string()),
            ),
            ("active".to_string(), BoltValue::Boolean(true)),
            (
                "tenant_id".to_string(),
                BoltValue::String("tenant-a".to_string()),
            ),
            (
                "sub_tenant_id".to_string(),
                BoltValue::String("sub-a".to_string()),
            ),
        ])),
        BoltValue::Dict(BoltDict::from([
            ("vertex".to_string(), BoltValue::Integer(71)),
            (
                "entity_id".to_string(),
                BoltValue::String("entity-b".to_string()),
            ),
            ("active".to_string(), BoltValue::Boolean(true)),
            (
                "tenant_id".to_string(),
                BoltValue::String("tenant-a".to_string()),
            ),
            (
                "sub_tenant_id".to_string(),
                BoltValue::String("sub-a".to_string()),
            ),
        ])),
    ]);
    let _ = session
        .run_with_params(
            "UNWIND $rows AS row MERGE (n {id: row.vertex}) \
             SET n:Entity, n.entity_id = row.entity_id, n.active = row.active, \
                 n.tenant_id = row.tenant_id, n.sub_tenant_id = row.sub_tenant_id",
            BoltDict::from([("rows".to_string(), vertex_rows)]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let upserted = session
        .run("MATCH (n:Entity {entity_id: 'entity-a'}) RETURN n.id AS id, n.active AS active")
        .await
        .unwrap();
    assert_eq!(
        upserted.records,
        vec![vec![BoltValue::Integer(70), BoltValue::Boolean(true)]]
    );
    let _ = session
        .run_with_params(
            "UNWIND $rows AS row MERGE (n {id: row.vertex}) \
             SET n:Entity, n.name = row.name",
            BoltDict::from([(
                "rows".to_string(),
                BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
                    ("vertex".to_string(), BoltValue::Integer(70)),
                    (
                        "name".to_string(),
                        BoltValue::String("Entity A".to_string()),
                    ),
                ]))]),
            )]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let merged = session
        .run(
            "MATCH (n:Entity {entity_id: 'entity-a'}) \
             RETURN n.active AS active, n.name AS name",
        )
        .await
        .unwrap();
    assert_eq!(
        merged.records,
        vec![vec![
            BoltValue::Boolean(true),
            BoltValue::String("Entity A".to_string()),
        ]]
    );

    let relationship_rows = BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
        ("source_vertex".to_string(), BoltValue::Integer(70)),
        ("destination_vertex".to_string(), BoltValue::Integer(71)),
        ("relationship_vertex".to_string(), BoltValue::Integer(900)),
        (
            "relationship_id".to_string(),
            BoltValue::String("relationship-a".to_string()),
        ),
        (
            "chunk_id".to_string(),
            BoltValue::String("chunk-a".to_string()),
        ),
    ]))]);
    let relationship_query = "UNWIND $rows AS row \
         MATCH (s:Entity {id: row.source_vertex}), \
               (d:Entity {id: row.destination_vertex}) \
         CREATE (s)-[:RELATES {id: row.relationship_vertex, \
                 relationship_id: row.relationship_id, chunk_id: row.chunk_id}]->(d)";
    for _ in 0..2 {
        let _ = session
            .run_with_params(
                relationship_query,
                BoltDict::from([("rows".to_string(), relationship_rows.clone())]),
                BoltDict::new(),
            )
            .await
            .unwrap();
    }
    let updated_relationship_rows = BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
        ("source_vertex".to_string(), BoltValue::Integer(70)),
        ("destination_vertex".to_string(), BoltValue::Integer(71)),
        ("relationship_vertex".to_string(), BoltValue::Integer(900)),
        (
            "relationship_id".to_string(),
            BoltValue::String("relationship-a".to_string()),
        ),
        (
            "chunk_id".to_string(),
            BoltValue::String("chunk-b".to_string()),
        ),
    ]))]);
    let _ = session
        .run_with_params(
            relationship_query,
            BoltDict::from([("rows".to_string(), updated_relationship_rows)]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let relationship = session
        .run(
            "MATCH ({id: 70})-[r:RELATES {relationship_id: 'relationship-a'}]->({id: 71}) \
             RETURN r.chunk_id AS chunk_id ORDER BY chunk_id",
        )
        .await
        .unwrap();
    assert_eq!(
        relationship.records,
        vec![
            vec![BoltValue::String("chunk-a".to_string())],
            vec![BoltValue::String("chunk-a".to_string())],
            vec![BoltValue::String("chunk-b".to_string())],
        ]
    );
    let merge_created_relationship_rows = BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
        ("source_vertex".to_string(), BoltValue::Integer(70)),
        ("destination_vertex".to_string(), BoltValue::Integer(71)),
        ("relationship_vertex".to_string(), BoltValue::Integer(900)),
        ("replayed".to_string(), BoltValue::Boolean(true)),
    ]))]);
    let _ = session
        .run_with_params(
            "UNWIND $rows AS row \
             MATCH (s:Entity {id: row.source_vertex}), \
                   (d:Entity {id: row.destination_vertex}) \
             MERGE (s)-[r:RELATES {id: row.relationship_vertex}]->(d) \
             SET r.replayed = row.replayed",
            BoltDict::from([("rows".to_string(), merge_created_relationship_rows)]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let merged_created_relationships = session
        .run(
            "MATCH ({id: 70})-[r:RELATES {id: 900, replayed: true}]->({id: 71}) \
             RETURN count(*) AS count",
        )
        .await
        .unwrap();
    assert_eq!(
        merged_created_relationships.records,
        vec![vec![BoltValue::Integer(3)]]
    );
    let merge_relationship_query = "UNWIND $rows AS row \
         MATCH (s:Entity {id: row.source_vertex}), \
               (d:Entity {id: row.destination_vertex}) \
         MERGE (s)-[r:RELATES {id: row.relationship_vertex}]->(d) \
         SET r.relationship_id = row.relationship_id, r.chunk_id = row.chunk_id";
    let merge_relationship_rows = BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
        ("source_vertex".to_string(), BoltValue::Integer(70)),
        ("destination_vertex".to_string(), BoltValue::Integer(71)),
        ("relationship_vertex".to_string(), BoltValue::Integer(901)),
        (
            "relationship_id".to_string(),
            BoltValue::String("relationship-merge".to_string()),
        ),
        (
            "chunk_id".to_string(),
            BoltValue::String("chunk-a".to_string()),
        ),
    ]))]);
    let _ = session
        .run_with_params(
            merge_relationship_query,
            BoltDict::from([("rows".to_string(), merge_relationship_rows)]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let merge_update_rows = BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
        ("source_vertex".to_string(), BoltValue::Integer(70)),
        ("destination_vertex".to_string(), BoltValue::Integer(71)),
        ("relationship_vertex".to_string(), BoltValue::Integer(901)),
        (
            "chunk_id".to_string(),
            BoltValue::String("chunk-b".to_string()),
        ),
    ]))]);
    let _ = session
        .run_with_params(
            "UNWIND $rows AS row \
             MATCH (s:Entity {id: row.source_vertex}), \
                   (d:Entity {id: row.destination_vertex}) \
             MERGE (s)-[r:RELATES {id: row.relationship_vertex}]->(d) \
             SET r.chunk_id = row.chunk_id",
            BoltDict::from([("rows".to_string(), merge_update_rows)]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let merged_relationship = session
        .run(
            "MATCH ({id: 70})-[r:RELATES {relationship_id: 'relationship-merge'}]->({id: 71}) \
             RETURN r.chunk_id AS chunk_id",
        )
        .await
        .unwrap();
    assert_eq!(
        merged_relationship.records,
        vec![vec![BoltValue::String("chunk-b".to_string())]]
    );
    let _ = session
        .run(
            "MATCH (scope:Entity {tenant_id: 'tenant-a', sub_tenant_id: 'sub-a'}) \
                   -[r:RELATES {relationship_id: 'relationship-a'}]->() \
             SET r.superseded_by = 'relationship-b', r.valid_to = 42.5",
        )
        .await
        .unwrap();
    let updated_relationship = session
        .run(
            "MATCH ({id: 70})-[r:RELATES {relationship_id: 'relationship-a'}]->({id: 71}) \
             WHERE r.superseded_by = 'relationship-b' AND r.valid_to = 42.5 \
             RETURN count(*) AS count",
        )
        .await
        .unwrap();
    assert_eq!(
        updated_relationship.records,
        vec![vec![BoltValue::Integer(3)]]
    );
    let _ = session
        .run(
            "MATCH (scope:Entity {tenant_id: 'tenant-a', sub_tenant_id: 'sub-a'}) \
                   -[r:RELATES]->() \
             WHERE r.chunk_id = 'chunk-a' OR r.chunk_id = 'chunk-b' DELETE r",
        )
        .await
        .unwrap();
    let deleted_relationship = session
        .run(
            "MATCH ({id: 70})-[r:RELATES {relationship_id: 'relationship-a'}]->({id: 71}) \
             RETURN count(*) AS count",
        )
        .await
        .unwrap();
    assert_eq!(
        deleted_relationship.records,
        vec![vec![BoltValue::Integer(0)]]
    );
    let _ = session
        .run("CREATE (:Source {id: 60})-[:SEED]->(:Source {id: 61})")
        .await
        .unwrap();
    let shard = cluster.shard("cell-a").unwrap();
    shard
        .write_edge_mutations_batch_between_labeled_vertices(
            "cell-a",
            [crate::EdgeMutation {
                cell_id: "cell-a".to_string(),
                edge_type: "FORCEFUL_RELATION".to_string(),
                src: 60,
                dst: 61,
                idempotency_key: "bolt-matched-create-direct-check".to_string(),
            }],
            "Source",
            "Source",
        )
        .await
        .unwrap();
    shard
        .delete_edge(crate::EdgeMutation {
            cell_id: "cell-a".to_string(),
            edge_type: "FORCEFUL_RELATION".to_string(),
            src: 60,
            dst: 61,
            idempotency_key: "bolt-matched-create-direct-cleanup".to_string(),
        })
        .await
        .unwrap();
    let matched_edge_rows = BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
        ("source_vertex".to_string(), BoltValue::Integer(60)),
        ("related_vertex".to_string(), BoltValue::Integer(61)),
    ]))]);
    let _ = session
        .run_with_params(
            "UNWIND $rows AS row \
             MATCH (s:Source {id: row.source_vertex}), \
                   (r:Source {id: row.related_vertex}) \
             CREATE (s)-[:FORCEFUL_RELATION]->(r)",
            BoltDict::from([("rows".to_string(), matched_edge_rows)]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    assert!(cluster
        .shard("cell-a")
        .unwrap()
        .edge_exists("cell-a", "FORCEFUL_RELATION", 60, 61)
        .await
        .unwrap());

    let missing_endpoint = session
        .run_with_params(
            "UNWIND $rows AS row \
             MATCH (s:Source {id: row.source_vertex}), \
                   (r:Source {id: row.related_vertex}) \
             CREATE (s)-[:FORCEFUL_RELATION]->(r)",
            BoltDict::from([(
                "rows".to_string(),
                BoltValue::List(vec![BoltValue::Dict(BoltDict::from([
                    ("source_vertex".to_string(), BoltValue::Integer(60)),
                    ("related_vertex".to_string(), BoltValue::Integer(62)),
                ]))]),
            )]),
            BoltDict::new(),
        )
        .await
        .unwrap_err();
    assert!(missing_endpoint.to_string().contains("does not exist"));
    session.reset().await.unwrap();
    assert!(!cluster
        .shard("cell-a")
        .unwrap()
        .edge_exists("cell-a", "FORCEFUL_RELATION", 60, 62)
        .await
        .unwrap());
    let rows = session
        .run_with_params(
            "UNWIND $sources AS row \
             MATCH ({id: row.src})-[:FOLLOWS]->(v) \
             RETURN row.src AS src, v.id AS dst",
            BoltDict::from([(
                "sources".to_string(),
                BoltValue::List(vec![
                    BoltValue::Dict(BoltDict::from([(
                        "src".to_string(),
                        BoltValue::Integer(10),
                    )])),
                    BoltValue::Dict(BoltDict::from([(
                        "src".to_string(),
                        BoltValue::Integer(11),
                    )])),
                ]),
            )]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["src", "dst"]);
    assert_eq!(
        rows.records,
        vec![
            vec![BoltValue::Integer(10), BoltValue::Integer(20)],
            vec![BoltValue::Integer(10), BoltValue::Integer(21)],
            vec![BoltValue::Integer(11), BoltValue::Integer(22)],
        ]
    );
    let _ = session
        .run_with_params(
            "UNWIND $edges AS edge \
             MATCH ({id: edge.src})-[r:FOLLOWS]->({id: edge.dst}) DELETE r",
            BoltDict::from([("edges".to_string(), edge_rows)]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    assert!(cluster
        .shard("cell-a")
        .unwrap()
        .edge_exists_batch("cell-a", "FOLLOWS", [(10, 20), (10, 21), (11, 22)])
        .await
        .unwrap()
        .iter()
        .all(|entry| !entry.exists));

    let _ = session
        .run("CREATE ({id: 30})-[:FOLLOWS]->({id: 31})")
        .await
        .unwrap();
    let _ = session
        .run_with_params(
            "UNWIND $vertices AS row MATCH (n {id: row.vertex}) DETACH DELETE n",
            BoltDict::from([(
                "vertices".to_string(),
                BoltValue::List(vec![BoltValue::Dict(BoltDict::from([(
                    "vertex".to_string(),
                    BoltValue::Integer(30),
                )]))]),
            )]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    assert!(!cluster
        .shard("cell-a")
        .unwrap()
        .edge_exists("cell-a", "FOLLOWS", 30, 31)
        .await
        .unwrap());

    let _ = session
        .run("CREATE ({id: 40})-[:RELATES {chunk_id: 'chunk-a'}]->({id: 41})")
        .await
        .unwrap();
    let _ = session
        .run("CREATE ({id: 40})-[:RELATES {chunk_id: 'chunk-b'}]->({id: 41})")
        .await
        .unwrap();
    let _ = session
        .run_with_params(
            "UNWIND $rows AS row \
             MATCH ()-[r:RELATES {chunk_id: row.chunk_id}]->() DELETE r",
            BoltDict::from([(
                "rows".to_string(),
                BoltValue::List(vec![BoltValue::Dict(BoltDict::from([(
                    "chunk_id".to_string(),
                    BoltValue::String("chunk-a".to_string()),
                )]))]),
            )]),
            BoltDict::new(),
        )
        .await
        .unwrap();
    assert!(cluster
        .shard("cell-a")
        .unwrap()
        .edge_exists("cell-a", "RELATES", 40, 41)
        .await
        .unwrap());
    let chunk_a = session
        .run(
            "MATCH ({id: 40})-[r:RELATES {chunk_id: 'chunk-a'}]->({id: 41}) \
             RETURN count(*) AS count",
        )
        .await
        .unwrap();
    let chunk_b = session
        .run(
            "MATCH ({id: 40})-[r:RELATES {chunk_id: 'chunk-b'}]->({id: 41}) \
             RETURN count(*) AS count",
        )
        .await
        .unwrap();
    assert_eq!(chunk_a.records, vec![vec![BoltValue::Integer(0)]]);
    assert_eq!(chunk_b.records, vec![vec![BoltValue::Integer(1)]]);

    let malformed = session
        .run_with_params(
            "UNWIND $edges AS edge \
             CREATE ({id: edge.src})-[:FOLLOWS]->({id: edge.dst})",
            BoltDict::from([(
                "edges".to_string(),
                BoltValue::List(vec![BoltValue::Dict(BoltDict::from([(
                    "src".to_string(),
                    BoltValue::Integer(30),
                )]))]),
            )]),
            BoltDict::new(),
        )
        .await
        .unwrap_err();
    assert!(malformed.to_string().contains("missing field dst"));
    assert!(cluster
        .shard("cell-a")
        .unwrap()
        .out_neighbors("cell-a", "FOLLOWS", 30)
        .await
        .unwrap()
        .is_empty());

    session.close().await.unwrap();
    server.stop().await.unwrap();
    cluster.close().await.unwrap();
}

#[tokio::test]
async fn bolt_pull_uses_snapshot_backed_server_cursor_pages() {
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        bolt_service_for(Arc::new(PagedBoltTestClient)),
        bolt_test_config().with_prefetch_rows(2),
    )
    .await
    .unwrap();
    let mut session = BoltSession::connect_basic(server.local_addr(), "neo4j", "bolt-secret")
        .await
        .unwrap();
    session
        .connection()
        .run(
            "MATCH (n {id: 1}) RETURN n.id AS answer",
            HashMap::new(),
            BoltDict::new(),
        )
        .await
        .unwrap();
    let (first, first_summary) = session.connection().pull_n(1).await.unwrap();
    assert_eq!(first, vec![vec![BoltValue::Integer(1)]]);
    assert_eq!(
        first_summary.get("has_more"),
        Some(&BoltValue::Boolean(true))
    );
    let (remaining, summary) = session.connection().pull_all().await.unwrap();
    assert_eq!(
        remaining,
        vec![
            vec![BoltValue::Integer(2)],
            vec![BoltValue::Integer(3)],
            vec![BoltValue::Integer(4)],
            vec![BoltValue::Integer(5)],
        ]
    );
    assert_eq!(summary.get("has_more"), Some(&BoltValue::Boolean(false)));

    session
        .connection()
        .run(
            "MATCH (n {id: 1}) RETURN n.id AS answer",
            HashMap::new(),
            BoltDict::new(),
        )
        .await
        .unwrap();
    session
        .connection()
        .send(&ClientMessage::Discard {
            extra: BoltDict::from([
                ("n".to_string(), BoltValue::Integer(2)),
                ("qid".to_string(), BoltValue::Integer(-1)),
            ]),
        })
        .await
        .unwrap();
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Success { ref metadata }
            if metadata.get("has_more") == Some(&BoltValue::Boolean(true))
    ));
    let (after_discard, _) = session.connection().pull_all().await.unwrap();
    assert_eq!(
        after_discard,
        vec![
            vec![BoltValue::Integer(3)],
            vec![BoltValue::Integer(4)],
            vec![BoltValue::Integer(5)],
        ]
    );

    session
        .connection()
        .run(
            "MATCH (n {id: 1}) RETURN n.id AS answer",
            HashMap::new(),
            BoltDict::new(),
        )
        .await
        .unwrap();
    session
        .connection()
        .send(&ClientMessage::Discard {
            extra: BoltDict::from([
                ("n".to_string(), BoltValue::Integer(-1)),
                ("qid".to_string(), BoltValue::Integer(-1)),
            ]),
        })
        .await
        .unwrap();
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Success { ref metadata }
            if metadata.get("has_more") == Some(&BoltValue::Boolean(false))
                && metadata.get("bookmark").and_then(BoltValue::as_str).is_some()
    ));
    session.close().await.unwrap();
    server.stop().await.unwrap();
}

#[tokio::test]
async fn bolt_reset_interrupts_active_query_and_returns_connection_to_ready() {
    let backend = Arc::new(BlockingBoltTestClient {
        started: std::sync::atomic::AtomicBool::new(false),
    });
    let service = bolt_service_for(backend.clone());
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        bolt_test_config(),
    )
    .await
    .unwrap();
    let mut session = BoltSession::connect_basic(server.local_addr(), "neo4j", "bolt-secret")
        .await
        .unwrap();
    session
        .connection()
        .send(&ClientMessage::Run {
            query: "MATCH (n {id: 1}) RETURN n.id".to_string(),
            parameters: BoltDict::new(),
            extra: BoltDict::new(),
        })
        .await
        .unwrap();
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Success { .. }
    ));
    session
        .connection()
        .send(&ClientMessage::pull_all())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backend.started.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    session
        .connection()
        .send(&ClientMessage::pull_all())
        .await
        .unwrap();
    session
        .connection()
        .send(&ClientMessage::Reset)
        .await
        .unwrap();
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Ignored
    ));
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Ignored
    ));
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Success { .. }
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.active_query_count().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    session.close().await.unwrap();
    server.stop().await.unwrap();
}

#[tokio::test]
async fn bolt_reset_after_run_does_not_start_backend_execution() {
    let backend = Arc::new(BlockingBoltTestClient {
        started: std::sync::atomic::AtomicBool::new(false),
    });
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        bolt_service_for(backend.clone()),
        bolt_test_config(),
    )
    .await
    .unwrap();
    let mut session = BoltSession::connect_basic(server.local_addr(), "neo4j", "bolt-secret")
        .await
        .unwrap();
    session
        .connection()
        .send(&ClientMessage::Run {
            query: "MATCH (n {id: 1}) RETURN n.id".to_string(),
            parameters: BoltDict::new(),
            extra: BoltDict::new(),
        })
        .await
        .unwrap();
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Success { .. }
    ));
    session
        .connection()
        .send(&ClientMessage::Reset)
        .await
        .unwrap();
    assert!(matches!(
        session.connection().recv().await.unwrap(),
        ServerMessage::Success { .. }
    ));
    assert!(!backend.started.load(std::sync::atomic::Ordering::SeqCst));
    session.close().await.unwrap();
    server.stop().await.unwrap();
}

#[tokio::test]
async fn bolt_server_accepts_authenticated_queries_over_tls() {
    let tls = crate::client::client_test_tls_bundle();
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        bolt_test_service(),
        bolt_test_config().with_tls_provider(Arc::new(
            StaticQueryTransportTlsServerConfigProvider::new(tls.server),
        )),
    )
    .await
    .unwrap();
    let tcp = TcpStream::connect(server.local_addr()).await.unwrap();
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost")
        .unwrap()
        .to_owned();
    let mut stream = tokio_rustls::TlsConnector::from(tls.client)
        .connect(server_name, tcp)
        .await
        .unwrap();
    stream.write_all(&BOLT_MAGIC).await.unwrap();
    stream
        .write_all(&[0, 3, 4, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut selected = [0_u8; 4];
    stream.read_exact(&mut selected).await.unwrap();
    assert_eq!(selected, [0, 0, 4, 5]);
    let (read_half, write_half) = tokio::io::split(stream);
    let mut reader = ChunkReader::new(read_half);
    let mut writer = ChunkWriter::new(write_half);

    send_test_bolt_client_message(
        &mut writer,
        &ClientMessage::Hello {
            extra: BoltDict::from([(
                "user_agent".to_string(),
                BoltValue::String("sgk-tls-test".to_string()),
            )]),
        },
    )
    .await;
    assert!(matches!(
        decode_test_bolt_server_message(&mut reader).await,
        ServerMessage::Success { .. }
    ));
    send_test_bolt_client_message(
        &mut writer,
        &ClientMessage::Logon {
            auth: BoltDict::from([
                ("scheme".to_string(), BoltValue::String("basic".to_string())),
                (
                    "principal".to_string(),
                    BoltValue::String("neo4j".to_string()),
                ),
                (
                    "credentials".to_string(),
                    BoltValue::String("bolt-secret".to_string()),
                ),
            ]),
        },
    )
    .await;
    assert!(matches!(
        decode_test_bolt_server_message(&mut reader).await,
        ServerMessage::Success { .. }
    ));
    send_test_bolt_client_message(
        &mut writer,
        &ClientMessage::Run {
            query: "MATCH (n {id: 1}) RETURN n.id AS answer".to_string(),
            parameters: BoltDict::new(),
            extra: BoltDict::new(),
        },
    )
    .await;
    assert!(matches!(
        decode_test_bolt_server_message(&mut reader).await,
        ServerMessage::Success { .. }
    ));
    send_test_bolt_client_message(&mut writer, &ClientMessage::pull_all()).await;
    assert_eq!(
        decode_test_bolt_server_message(&mut reader).await,
        ServerMessage::Record {
            data: vec![BoltValue::Integer(42)]
        }
    );
    assert!(matches!(
        decode_test_bolt_server_message(&mut reader).await,
        ServerMessage::Success { .. }
    ));
    drop(reader);
    drop(writer);
    server.stop().await.unwrap();
}

#[tokio::test]
async fn bolt_server_closes_idle_post_handshake_connections() {
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        bolt_test_service(),
        bolt_test_config().with_idle_timeout(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
    stream.write_all(&BOLT_MAGIC).await.unwrap();
    stream
        .write_all(&[0, 3, 4, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut selected = [0_u8; 4];
    stream.read_exact(&mut selected).await.unwrap();
    assert_eq!(selected, [0, 0, 4, 5]);

    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read, 0);
    server.stop().await.unwrap();
}

#[tokio::test]
async fn object_store_routing_advertises_all_readers_and_one_soft_affinity_writer() {
    let preferred_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let preferred_address = preferred_listener.local_addr().unwrap().to_string();
    let fallback_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_address = fallback_listener.local_addr().unwrap().to_string();
    let unready_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unready_address = unready_listener.local_addr().unwrap().to_string();
    let (preferred_readiness, preferred_readiness_task) = start_test_readiness_endpoint(true).await;
    let (fallback_readiness, fallback_readiness_task) = start_test_readiness_endpoint(true).await;
    let (unready_readiness, unready_readiness_task) = start_test_readiness_endpoint(false).await;
    let provider = ObjectStoreBoltRoutingTableProvider::new(
        [
            ("node-a".to_string(), preferred_address.clone()),
            ("node-b".to_string(), fallback_address.clone()),
            ("node-c".to_string(), unready_address),
        ],
        30,
    )
    .unwrap()
    .with_readiness_addresses([
        ("node-a".to_string(), preferred_readiness),
        ("node-b".to_string(), fallback_readiness),
        ("node-c".to_string(), unready_readiness),
    ])
    .unwrap()
    .with_health_probe_timeout(Duration::from_secs(1))
    .unwrap();
    let table = provider
        .routing_table(
            "default",
            &ClientQueryTarget::new(GraphScope::default(), "cell-a").unwrap(),
        )
        .await
        .unwrap();
    assert!(table.ttl_secs > 0 && table.ttl_secs <= 30);
    let write = table
        .servers
        .iter()
        .find(|server| server.role == "WRITE")
        .unwrap();
    assert_eq!(write.addresses, vec![preferred_address.clone()]);
    let read = table
        .servers
        .iter()
        .find(|server| server.role == "READ")
        .unwrap();
    assert_eq!(
        read.addresses,
        vec![preferred_address.clone(), fallback_address.clone()]
    );

    let preferred = provider
        .clone()
        .with_preferred_writer_node("node-b")
        .unwrap()
        .routing_table(
            "default",
            &ClientQueryTarget::new(GraphScope::default(), "cell-a").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        preferred
            .servers
            .iter()
            .find(|server| server.role == "WRITE")
            .unwrap()
            .addresses,
        vec![fallback_address.clone()]
    );

    drop(preferred_listener);
    preferred_readiness_task.abort();
    let _ = preferred_readiness_task.await;
    let failed_over = provider
        .routing_table(
            "default",
            &ClientQueryTarget::new(GraphScope::default(), "cell-a").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        failed_over
            .servers
            .iter()
            .find(|server| server.role == "WRITE")
            .unwrap()
            .addresses,
        vec![fallback_address.clone()]
    );
    assert_eq!(
        failed_over
            .servers
            .iter()
            .find(|server| server.role == "READ")
            .unwrap()
            .addresses,
        vec![fallback_address]
    );

    fallback_readiness_task.abort();
    unready_readiness_task.abort();
}

async fn start_test_readiness_endpoint(ready: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut request = [0_u8; 256];
            let _ = stream.read(&mut request).await;
            let status = if ready {
                "HTTP/1.1 200 OK"
            } else {
                "HTTP/1.1 503 Service Unavailable"
            };
            let response = format!("{status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    (address, task)
}

async fn send_test_bolt_client_message<W>(writer: &mut ChunkWriter<W>, message: &ClientMessage)
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = BytesMut::new();
    encode_client_message(&mut bytes, message);
    writer.write_message(&bytes).await.unwrap();
    writer.flush().await.unwrap();
}

async fn decode_test_bolt_server_message<R>(reader: &mut ChunkReader<R>) -> ServerMessage
where
    R: AsyncRead + Unpin,
{
    boltr::message::decode::decode_server_message(&reader.read_message().await.unwrap()).unwrap()
}
