use super::*;
use slatedb::object_store::memory::InMemory;

async fn connected_match_filtered_frontier_with_small_limit(property_filter: bool) {
    let shard = GraphShard::open_standalone_writer_with_limits(
        format!("graph/connected-match-filtered-frontier-{property_filter}"),
        Arc::new(InMemory::new()),
        GraphLimits {
            max_query_intermediate_rows: 6,
            ..GraphLimits::default()
        },
    )
    .await
    .unwrap();
    let mut vertices = (1..=2)
        .map(|id| {
            (
                id,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("entity_id", VertexPropertyValue::String(format!("entity-{id}"))),
            )
        })
        .collect::<Vec<_>>();
    for entity in 1..=2 {
        for offset in 1..=4 {
            vertices.push((
                entity * 100 + offset,
                VertexMetadata::default()
                    .with_label(if property_filter { "Chunk" } else { "WrongLabel" })
                    .with_property("scope", VertexPropertyValue::Bool(false)),
            ));
        }
    }
    shard.set_vertex_metadata_batch("cell-a", vertices).await.unwrap();
    for entity in 1..=2 {
        for offset in 1..=4 {
            let target = entity * 100 + offset;
            shard
                .write_edge(EdgeMutation {
                    cell_id: "cell-a".into(),
                    edge_type: "PRESENT_IN".into(),
                    src: entity,
                    dst: target,
                    idempotency_key: format!("present-{entity}-{target}"),
                })
                .await
                .unwrap();
        }
    }
    let target = if property_filter { "c:Chunk {scope: true}" } else { "c:Chunk" };
    let query = format!(
        "MATCH (e:Entity) WHERE e.entity_id IN $ids \
         MATCH (e)-[:PRESENT_IN]->({target}) RETURN e, c ORDER BY e, c"
    );
    let baseline = query.replace("MATCH (e)-", "WITH e MATCH (e)-");
    for accepted in [false, true] {
        if accepted {
            shard
                .set_vertex_metadata_batch(
                    "cell-a",
                    [101, 201].map(|id| {
                        (
                            id,
                            VertexMetadata::default()
                                .with_label("Chunk")
                                .with_property("scope", VertexPropertyValue::Bool(true)),
                        )
                    }),
                )
                .await
                .unwrap();
        }
        let scalar = shard
            .execute_cypher_rows(context(CypherEngineMode::Experimental), &baseline)
            .await
            .expect("scalar expansion fits the six-row budget before target filtering");
        let expected = if accepted {
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(101)]),
                QueryRow::new(vec![QueryValue::VertexId(2), QueryValue::VertexId(201)]),
            ]
        } else {
            Vec::new()
        };
        assert_eq!(scalar.rows, expected);
        let batched = shard
            .execute_cypher_rows(context(CypherEngineMode::Experimental), &query)
            .await
            .expect("batching must not combine rejected target frontiers against the row budget");
        assert_eq!(batched.rows, scalar.rows);
    }
    shard.close().await.unwrap();
}

#[tokio::test]
async fn experimental_connected_match_batching_filters_target_labels_before_combined_budget() {
    connected_match_filtered_frontier_with_small_limit(false).await;
}

#[tokio::test]
async fn experimental_connected_match_batching_filters_target_properties_before_combined_budget() {
    connected_match_filtered_frontier_with_small_limit(true).await;
}

#[tokio::test]
async fn experimental_connected_match_batching_preserves_structural_relationship_ids() {
    let shard = GraphShard::open_standalone_writer("graph/connected-match-structural-ids", Arc::new(InMemory::new()))
        .await
        .unwrap();
    shard
        .set_vertex_metadata_batch(
            "cell-a",
            [
                (1, "Entity"),
                (2, "Entity"),
                (100, "Chunk"),
                (200, "Chunk"),
                (301, "Source"),
                (302, "Source"),
                (401, "Source"),
                (402, "Source"),
            ]
            .map(|(id, label)| {
                (
                    id,
                    VertexMetadata::default()
                        .with_label(label)
                        .with_property("entity_id", VertexPropertyValue::String(format!("entity-{id}"))),
                )
            }),
        )
        .await
        .unwrap();
    for (edge_type, src, dst) in [
        ("PRESENT_IN", 1, 200),
        ("PRESENT_IN", 2, 100),
        ("HAS_CHUNK", 200, 301),
        ("HAS_CHUNK", 302, 200),
        ("HAS_CHUNK", 100, 401),
        ("HAS_CHUNK", 402, 100),
    ] {
        shard
            .write_edge(EdgeMutation {
                cell_id: "cell-a".into(),
                edge_type: edge_type.into(),
                src,
                dst,
                idempotency_key: format!("{edge_type}-{src}-{dst}"),
            })
            .await
            .unwrap();
    }
    let query = "MATCH (e:Entity) WHERE e.entity_id IN $ids \
                 MATCH (e)-[:PRESENT_IN]->(c:Chunk) \
                 MATCH (c)-[h:HAS_CHUNK]-(s:Source) \
                 RETURN e, c, s, h ORDER BY e, c, s";
    let baseline = query
        .replace("MATCH (e)-", "WITH e MATCH (e)-")
        .replace("MATCH (c)-", "WITH e, c MATCH (c)-");
    let scalar = shard
        .execute_cypher_rows(context(CypherEngineMode::Experimental), &baseline)
        .await
        .unwrap();
    let expected = [(1, 200, 301), (1, 200, 302), (2, 100, 401), (2, 100, 402)]
        .into_iter()
        .enumerate()
        .map(|(index, (entity, chunk, source))| {
            QueryRow::new(vec![
                QueryValue::VertexId(entity),
                QueryValue::VertexId(chunk),
                QueryValue::VertexId(source),
                QueryValue::Property(VertexPropertyValue::Integer(u64::MAX - 2 - index as u64)),
            ])
        })
        .collect::<Vec<_>>();
    assert_eq!(scalar.rows, expected);
    let batched = shard
        .execute_cypher_rows(context(CypherEngineMode::Experimental), query)
        .await
        .unwrap();
    assert_eq!(batched.rows, scalar.rows);
    shard.close().await.unwrap();
}

fn context(engine: CypherEngineMode) -> QueryContext {
    QueryContext::new("cell-a", "connected-match-batching")
        .with_cypher_engine(engine)
        .with_parameter("tenant", VertexPropertyValue::String("tenant-a".into()))
        .with_parameter("collection", VertexPropertyValue::String("collection-a".into()))
        .with_parameter("cap", VertexPropertyValue::Integer(5001))
        .with_list_parameters([(
            "ids".to_string(),
            (1..=8).map(|id| VertexPropertyValue::String(format!("entity-{id}"))).collect(),
        )])
}

fn expansion_work(shard: &GraphShard) -> (u64, u64) {
    shard
        .graph_operational_metrics()
        .experimental_operators
        .iter()
        .filter(|operator| operator.operator == "ExpandExec")
        .fold((0, 0), |(invocations, scanned), operator| {
            (invocations + operator.invocations, scanned + operator.scanned_relationships)
        })
}

#[tokio::test]
async fn experimental_connected_match_batching_preserves_scope_and_reduces_expansion_work() {
    let shard = GraphShard::open_standalone_writer("graph/connected-match-batching", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let mut vertices = (1..=8)
        .map(|id| {
            (
                id,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("entity_id", VertexPropertyValue::String(format!("entity-{id}")))
                    .with_property("tenant_id", VertexPropertyValue::String("tenant-a".into()))
                    .with_property("sub_tenant_id", VertexPropertyValue::String("collection-a".into())),
            )
        })
        .collect::<Vec<_>>();
    for (id, label, tenant, collection) in [
        (101, "Chunk", "tenant-a", "collection-a"),
        (102, "Chunk", "tenant-a", "collection-a"),
        (103, "Chunk", "tenant-a", "collection-b"),
        (201, "Source", "tenant-a", "collection-a"),
        (202, "Source", "tenant-a", "collection-a"),
        (203, "Source", "tenant-b", "collection-a"),
    ] {
        vertices.push((
            id,
            VertexMetadata::default()
                .with_label(label)
                .with_property("source_id", VertexPropertyValue::String(format!("source-{id}")))
                .with_property("tenant_id", VertexPropertyValue::String(tenant.into()))
                .with_property("sub_tenant_id", VertexPropertyValue::String(collection.into())),
        ));
    }
    shard.set_vertex_metadata_batch("cell-a", vertices).await.unwrap();
    let mut edges = (1..=8)
        .flat_map(|entity| (101..=103).map(move |chunk| ("PRESENT_IN", entity, chunk)))
        .collect::<Vec<_>>();
    edges.extend([
        ("HAS_CHUNK", 201, 101),
        ("HAS_CHUNK", 101, 202),
        ("HAS_CHUNK", 201, 102),
        ("HAS_CHUNK", 102, 202),
        ("HAS_CHUNK", 203, 101),
        ("HAS_CHUNK", 103, 201),
    ]);
    for (edge_type, src, dst) in edges {
        shard
            .write_edge(EdgeMutation {
                cell_id: "cell-a".into(),
                edge_type: edge_type.into(),
                src,
                dst,
                idempotency_key: format!("{edge_type}-{src}-{dst}"),
            })
            .await
            .unwrap();
    }
    let query = "MATCH (e:Entity {tenant_id: $tenant, sub_tenant_id: $collection}) \
                 WHERE e.entity_id IN $ids \
                 MATCH (e)-[:PRESENT_IN]->(c:Chunk {tenant_id: $tenant, sub_tenant_id: $collection}) \
                 MATCH (c)-[:HAS_CHUNK]-(s:Source {tenant_id: $tenant, sub_tenant_id: $collection}) \
                 RETURN DISTINCT e.entity_id AS entity, s.source_id AS source \
                 ORDER BY entity, source LIMIT $cap";
    let baseline = query
        .replace("MATCH (e)-", "WITH e MATCH (e)-")
        .replace("MATCH (c)-", "WITH e, c MATCH (c)-");
    let before = expansion_work(&shard);
    let batched = shard
        .execute_cypher_rows(context(CypherEngineMode::Experimental), query)
        .await
        .unwrap();
    let after_batch = expansion_work(&shard);
    let scalar = shard
        .execute_cypher_rows(context(CypherEngineMode::Experimental), &baseline)
        .await
        .unwrap();
    let after_scalar = expansion_work(&shard);
    let expected = (1..=8)
        .flat_map(|id| {
            [201, 202].map(|source| {
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::String(format!("entity-{id}"))),
                    QueryValue::Property(VertexPropertyValue::String(format!("source-{source}"))),
                ])
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(batched.rows, expected);
    assert_eq!(batched.rows, scalar.rows);
    assert_eq!(after_batch.0 - before.0, 10);
    assert_eq!(after_scalar.0 - after_batch.0, 24);
    assert!(after_batch.1 - before.1 < after_scalar.1 - after_batch.1);
    println!(
        "GraphShard: rows={} batched_expand_invocations={} scalar_expand_invocations={} batched_scanned_relationships={} scalar_scanned_relationships={}",
        batched.rows.len(),
        after_batch.0 - before.0,
        after_scalar.0 - after_batch.0,
        after_batch.1 - before.1,
        after_scalar.1 - after_batch.1,
    );
    let legacy = shard.execute_cypher_rows(context(CypherEngineMode::Legacy), query).await.unwrap();
    assert_eq!(batched.rows, legacy.rows);
    shard.close().await.unwrap();
}
