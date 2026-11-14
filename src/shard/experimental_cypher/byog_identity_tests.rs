use super::*;
use crate::{EdgeMetadata, OfflineGraphImageEncoder, QueryRow, RelationshipMutation, VertexMetadata};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::{OfflineImageBuilder, OfflineImageOptions};

async fn image_store(path: &str, byog: bool) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut encoder = OfflineGraphImageEncoder::new("cell-a", true).unwrap();
    let mut rows = Vec::new();
    if byog {
        rows.push(OfflineGraphImageEncoder::byog_id_semantics_row());
    }
    for (id, property) in [
        (0, Some(VertexPropertyValue::String("user-id".into()))),
        (1, Some(VertexPropertyValue::Integer(900))),
        (2, None),
    ] {
        let mut metadata = VertexMetadata::default().with_label("Item");
        if let Some(value) = property {
            metadata.properties.insert("id".into(), value);
        }
        rows.extend(encoder.vertex_rows(id, &metadata).unwrap());
    }
    for id in 0..2 {
        let mut metadata = EdgeMetadata::default();
        if id == 0 {
            metadata
                .properties
                .insert("id".into(), VertexPropertyValue::String("edge-user".into()));
        }
        rows.extend(
            encoder
                .relationship_rows_preserving_properties(&RelationshipMutation {
                    cell_id: "cell-a".into(),
                    edge_type: "LINK".into(),
                    src: 0,
                    dst: 1,
                    relationship_id: id,
                    metadata,
                })
                .unwrap(),
        );
    }
    rows.extend(encoder.finish_rows());
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    let mut builder = OfflineImageBuilder::create(path, Arc::clone(&store), OfflineImageOptions::default())
        .await
        .unwrap();
    for row in rows {
        builder.add(row.key, row.value).await.unwrap();
    }
    builder.finish().await.unwrap();
    store
}

async fn image(path: &str, byog: bool) -> GraphShard {
    GraphShard::open_standalone_writer(path, image_store(path, byog).await)
        .await
        .unwrap()
}
fn context() -> QueryContext {
    QueryContext::new("cell-a", "byog-identity").with_cypher_engine(CypherEngineMode::Experimental)
}
fn integer(v: u64) -> QueryValue {
    QueryValue::Property(VertexPropertyValue::Integer(v))
}
fn string(v: &str) -> QueryValue {
    QueryValue::Property(VertexPropertyValue::String(v.into()))
}

#[tokio::test]
async fn byog_identity_offline_marker_drives_queries_and_separates_cached_templates() {
    let byog = image("byog-id", true).await;
    let managed = image("managed-id", false).await;
    let query = "MATCH (n:Item) RETURN n.id AS property, id(n) AS internal ORDER BY internal";
    for shard in [&managed, &byog, &managed, &byog] {
        let result = shard.execute_cypher_rows(context(), query).await.unwrap();
        let expected = if std::ptr::eq(shard, &byog) {
            vec![
                QueryRow::new(vec![string("user-id"), integer(0)]),
                QueryRow::new(vec![integer(900), integer(1)]),
                QueryRow::new(vec![QueryValue::Null, integer(2)]),
            ]
        } else {
            (0..3)
                .map(|id| QueryRow::new(vec![QueryValue::VertexId(id), integer(id)]))
                .collect()
        };
        assert_eq!(result.rows, expected);
    }
    for query in [
        "MATCH (n:Item {id:'user-id'}) RETURN id(n)",
        "MATCH (n:Item) WHERE n.id='user-id' RETURN id(n)",
    ] {
        assert_eq!(
            byog.execute_cypher_rows(context(), query).await.unwrap().rows,
            vec![QueryRow::new(vec![integer(0)])]
        );
    }
    let rows = byog
        .execute_cypher_rows(context(), "MATCH (a)-[r:LINK]->(b) RETURN id(r), r.id, id(a), id(b) ORDER BY id(r)")
        .await
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            QueryRow::new(vec![integer(0), string("edge-user"), integer(0), integer(1)]),
            QueryRow::new(vec![integer(1), QueryValue::Null, integer(0), integer(1)])
        ]
    );
    let rows = byog
        .execute_cypher_rows(
            context(),
            "MATCH (n:Item) WITH {node:n} AS x RETURN id(x.node), x.node.id ORDER BY id(x.node)",
        )
        .await
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            QueryRow::new(vec![integer(0), string("user-id")]),
            QueryRow::new(vec![integer(1), integer(900)]),
            QueryRow::new(vec![integer(2), QueryValue::Null])
        ]
    );
    byog.close().await.unwrap();
    managed.close().await.unwrap();
}

#[tokio::test]
async fn byog_identity_rejects_managed_mutation_fallback_and_legacy_reads() {
    let shard = image("byog-mutation-guard", true).await;
    let legacy = QueryContext::new("cell-a", "legacy");
    let error = shard.execute_cypher_rows(legacy, "MATCH (n:Item) RETURN n.id").await.unwrap_err();
    assert!(error.to_string().contains("require the experimental"));
    let error = shard
        .execute_cypher_rows(context(), "MATCH (n:Item {id:0}) SET n.name='wrong'")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("managed Cypher fallback"), "{error}");
    assert_eq!(
        shard
            .execute_cypher_rows(context(), "MATCH (n:Item) WHERE n.name='wrong' RETURN id(n)")
            .await
            .unwrap()
            .rows
            .len(),
        0
    );
    shard.close().await.unwrap();
}

/// Exercise the transport's UNWIND optimizer, rather than calling the shard's
/// Cypher entrypoint (which already rejects these writes).
#[cfg(feature = "client-api")]
#[tokio::test]
async fn byog_identity_client_batches_reject_before_touching_imported_data() {
    use crate::{
        ClientQueryCredentials, ClientQueryRequest, ClientQueryService, ClientQueryServiceConfig, ClientQueryTarget, GraphScope,
        ObjectStoreNodeDirectory, PlacementConfig, PlacementView, QueryParameterValue, QueryTransportAction,
        QueryTransportConnectionIdentity, QueryTransportScopeGrant, RoutedGraphCluster, StaticQueryTransportScopeAuthorizer,
    };

    let base = "byog-client-batch-guard";
    let path = format!("{}/cell-a", GraphScope::default().scoped_store_path(base));
    let store = image_store(&path, true).await;
    let cluster = Arc::new(
        RoutedGraphCluster::open_promotable(
            base,
            "node-a",
            ObjectStoreNodeDirectory::new(["cell-a"], ["node-a"]).unwrap(),
            PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
            store,
        )
        .await
        .unwrap(),
    );
    let shard = cluster.shard("cell-a").unwrap();
    let vertices_query = "MATCH (n) RETURN id(n), n ORDER BY id(n)";
    let edges_query = "MATCH (a)-[r:LINK]->(b) RETURN id(a), id(r), id(b), r ORDER BY id(r)";
    let before_vertices = shard.execute_cypher_rows(context(), vertices_query).await.unwrap().rows;
    let before_edges = shard.execute_cypher_rows(context(), edges_query).await.unwrap().rows;
    let engines = [
        CypherEngineMode::Legacy,
        CypherEngineMode::Experimental,
        #[cfg(feature = "datafusion-cypher-engine")]
        CypherEngineMode::DataFusion,
    ];
    for engine in engines {
        let authorizer = StaticQueryTransportScopeAuthorizer::new()
            .with_bearer_grant(
                "secret",
                QueryTransportScopeGrant::graph(GraphScope::default(), [QueryTransportAction::Read, QueryTransportAction::Write]),
            )
            .unwrap();
        let service = ClientQueryService::new(
            cluster.clone(),
            ClientQueryServiceConfig::default()
                .with_required_bearer_token("secret")
                .with_scope_authorizer(Arc::new(authorizer))
                .with_cypher_engine(engine),
        )
        .unwrap();
        let session = service
            .authenticate(
                &ClientQueryCredentials::Bearer("secret".into()),
                &QueryTransportConnectionIdentity::default(),
            )
            .unwrap();
        #[cfg(feature = "datafusion-cypher-engine")]
        if engine == CypherEngineMode::DataFusion {
            // DataFusion does not accept list-of-map parameters in this release.
            // Exercise its actual service route with supported scalar parameters.
            for (index, query) in [
                "MATCH (n {id:$id}) RETURN n.id",
                "MATCH (n {id:$id}) SET n.name='wrong'",
                "MATCH (n {id:$id}) DELETE n",
                "CREATE (n {id:$id})",
            ]
            .into_iter()
            .enumerate()
            {
                for paged in [false, true] {
                    let request = ClientQueryRequest::new(
                        ClientQueryTarget::new(GraphScope::default(), "cell-a").unwrap(),
                        format!("datafusion-{index}-{paged}"),
                        query,
                    )
                    .with_query_parameters([("id".into(), QueryParameterValue::Scalar(VertexPropertyValue::Integer(1)))]);
                    let error = if paged {
                        service.execute_page(&session, request, None, 10).await.unwrap_err()
                    } else {
                        service.execute_rows(&session, request).await.unwrap_err()
                    };
                    assert!(error.to_string().contains("managed Cypher fallback"), "{query}: {error}");
                    assert_eq!(
                        shard.execute_cypher_rows(context(), vertices_query).await.unwrap().rows,
                        before_vertices
                    );
                    assert_eq!(shard.execute_cypher_rows(context(), edges_query).await.unwrap().rows, before_edges);
                }
            }
            continue;
        }
        for (index, query) in [
            "UNWIND $rows AS row MATCH (n {id: row.id}) DETACH DELETE n",
            "UNWIND $rows AS row MATCH (n {id: row.id}) DELETE n",
            "UNWIND $rows AS row MERGE (n {id: row.id}) SET n:Item, n.name = row.name",
            "UNWIND $rows AS row MATCH (n {id: row.id}) SET n.name = row.name",
            "UNWIND $rows AS row MATCH (s {id: row.id})-[:LINK]->(d) RETURN row.id AS src, d.id AS dst",
        ]
        .into_iter()
        .enumerate()
        {
            assert!(crate::query::opencypher::parse_opencypher_unwind_batch(query).unwrap().is_some());
            for paged in [false, true] {
                let request = ClientQueryRequest::new(
                    ClientQueryTarget::new(GraphScope::default(), "cell-a").unwrap(),
                    format!("{engine:?}-{index}-{paged}"),
                    query,
                )
                .with_query_parameters([(
                    "rows".into(),
                    QueryParameterValue::List(vec![QueryParameterValue::Map(BTreeMap::from([
                        ("id".into(), QueryParameterValue::Scalar(VertexPropertyValue::Integer(1))),
                        (
                            "name".into(),
                            QueryParameterValue::Scalar(VertexPropertyValue::String("wrong".into())),
                        ),
                    ]))]),
                )]);
                let error = if paged {
                    service.execute_page(&session, request, None, 10).await.unwrap_err()
                } else {
                    service.execute_rows(&session, request).await.unwrap_err()
                };
                assert!(
                    error.to_string().contains("managed Cypher fallback"),
                    "{engine:?}: {query}: {error}"
                );
                assert_eq!(
                    shard.execute_cypher_rows(context(), vertices_query).await.unwrap().rows,
                    before_vertices
                );
                assert_eq!(shard.execute_cypher_rows(context(), edges_query).await.unwrap().rows, before_edges);
            }
        }
    }
    // The query guard must not disable the separately exposed storage API,
    // whose arguments explicitly refer to internal IDs.
    cluster.ensure_local_writer("cell-a").await.unwrap();
    cluster
        .shard("cell-a")
        .unwrap()
        .delete_vertex_mutations_batch("cell-a", vec![(2, "explicit-internal-delete".into())], false)
        .await
        .unwrap();
    assert_eq!(
        cluster
            .shard("cell-a")
            .unwrap()
            .execute_cypher_rows(context(), vertices_query)
            .await
            .unwrap()
            .rows
            .len(),
        2
    );
    cluster.close().await.unwrap();
}

#[cfg(feature = "datafusion-cypher-engine")]
#[tokio::test]
async fn byog_identity_datafusion_routes_reject_before_writer_promotion() {
    use crate::{GraphScope, ObjectStoreNodeDirectory, PlacementConfig, PlacementView, QueryCellClient, RoutedGraphCluster};

    let base = "byog-datafusion-routing-guard";
    let path = format!("{}/cell-a", GraphScope::default().scoped_store_path(base));
    let cluster = RoutedGraphCluster::open_promotable(
        base,
        "node-a",
        ObjectStoreNodeDirectory::new(["cell-a"], ["node-a"]).unwrap(),
        PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
        image_store(&path, true).await,
    )
    .await
    .unwrap();
    let shard = cluster.shard_arc("cell-a").unwrap();
    let vertices = "MATCH (n) RETURN id(n), n ORDER BY id(n)";
    let edges = "MATCH (a)-[r:LINK]->(b) RETURN id(a), id(r), id(b), r ORDER BY id(r)";
    let before_vertices = shard.execute_cypher_rows(context(), vertices).await.unwrap().rows;
    let before_edges = shard.execute_cypher_rows(context(), edges).await.unwrap().rows;
    assert!(shard.storage.writer().is_err());
    for query in [
        "MATCH (n {id:1}) RETURN n.id",
        "MATCH (n {id:1}) SET n.name='wrong' RETURN n",
        "MATCH (n {id:1}) DELETE n",
        "CREATE (n {id:42})",
    ] {
        let ctx = || QueryContext::new("cell-a", "byog-datafusion-rejected").with_cypher_engine(CypherEngineMode::DataFusion);
        let errors = [
            cluster.execute_cypher(ctx(), query).await.unwrap_err(),
            cluster.execute_cypher_rows(ctx(), query).await.unwrap_err(),
            cluster.execute_cypher_rows_page(ctx(), query, None, 10).await.unwrap_err(),
            QueryCellClient::prepare_datafusion_cypher(&cluster, ctx(), query)
                .await
                .unwrap_err(),
            QueryCellClient::execute_cypher_rows(&cluster, ctx(), query).await.unwrap_err(),
            QueryCellClient::execute_cypher_rows_page(&cluster, ctx(), query, None, 10)
                .await
                .unwrap_err(),
            QueryCellClient::resolve_datafusion_statement_receipt(&cluster, ctx(), query)
                .await
                .unwrap_err(),
            shard.execute_datafusion_cypher(ctx(), query).await.unwrap_err(),
            shard.execute_datafusion_cypher_rows(ctx(), query).await.unwrap_err(),
            shard.execute_datafusion_cypher_rows_page(ctx(), query, None, 10).await.unwrap_err(),
        ];
        for error in errors {
            assert!(error.to_string().contains("managed Cypher fallback"), "{query}: {error}");
        }
        assert!(shard.storage.writer().is_err(), "rejected query promoted a writer: {query}");
        assert_eq!(shard.execute_cypher_rows(context(), vertices).await.unwrap().rows, before_vertices);
        assert_eq!(shard.execute_cypher_rows(context(), edges).await.unwrap().rows, before_edges);
    }
    cluster.close().await.unwrap();
}
