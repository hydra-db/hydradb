use super::wire::strict_decode_client_message;
use super::*;
use crate::{
    ClientQueryServiceConfig, GraphControlPlane, GraphNodeHealthState, QueryCellClient,
    QueryColumn, QueryContext, QueryCursorToken, QueryFloat, QueryResultPage, QueryResultSet,
    QueryRow, QueryTransportScopeGrant, QueryValue, ShardPlacement, StaticClientDatabaseResolver,
    StaticQueryTransportScopeAuthorizer, StaticQueryTransportTlsServerConfigProvider,
    VertexPropertyValue,
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
        ))
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

    async fn current_graph_epoch(
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

    async fn current_graph_epoch(
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
async fn legacy_handshake_selects_highest_supported_version_in_client_order() {
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
async fn bolt_pull_streams_backend_cursor_pages_without_full_materialization() {
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
async fn controller_routing_uses_live_nodes_and_current_lease_owner() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open("client/bolt/controller-routing", object_store)
            .await
            .unwrap(),
    );
    control
        .publish_placement(&ShardPlacement::fixed([("cell-a", "node-a")]).unwrap())
        .await
        .unwrap();
    control
        .publish_node_heartbeat("node-a", GraphNodeHealthState::Active)
        .await
        .unwrap();
    control
        .publish_node_heartbeat("node-b", GraphNodeHealthState::Active)
        .await
        .unwrap();
    control
        .acquire_lease("cell-a", "node-a", Duration::from_secs(60))
        .await
        .unwrap();
    let provider = ControllerBoltRoutingTableProvider::new(
        Arc::clone(&control),
        [
            ("node-a".to_string(), "node-a.example:7687".to_string()),
            ("node-b".to_string(), "node-b.example:7687".to_string()),
        ],
        Duration::from_secs(60),
        30,
    )
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
    assert_eq!(write.addresses, vec!["node-a.example:7687"]);
    let read = table
        .servers
        .iter()
        .find(|server| server.role == "READ")
        .unwrap();
    assert_eq!(
        read.addresses,
        vec!["node-a.example:7687", "node-b.example:7687"]
    );
    control.close().await.unwrap();
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
