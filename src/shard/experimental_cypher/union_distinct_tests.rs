use std::sync::Arc;

use slatedb::object_store::memory::InMemory;

use crate::{
    CypherEngineMode, GraphShard, QueryColumn, QueryContext, QueryResultSet, QueryRow, QueryValue,
    VertexMetadata, VertexPropertyValue,
};

async fn shard(path: &str) -> GraphShard {
    GraphShard::open_standalone_writer(path, Arc::new(InMemory::new()))
        .await
        .expect("open graph shard")
}

async fn seeded_items(path: &str) -> GraphShard {
    let shard = shard(path).await;
    for id in [1_u64, 2] {
        shard
            .set_vertex_metadata(
                "cell-a",
                id,
                VertexMetadata::default()
                    .with_label("item")
                    .with_property("id", VertexPropertyValue::Integer(id))
                    .with_property("category", VertexPropertyValue::String("a".to_string())),
            )
            .await
            .expect("write vertex metadata");
    }
    shard
}

fn expected() -> QueryResultSet {
    QueryResultSet::new(
        vec![QueryColumn::new("category")],
        vec![QueryRow::new(vec![QueryValue::Property(
            VertexPropertyValue::String("a".to_string()),
        )])],
    )
}

async fn categories(shard: &GraphShard, engine: CypherEngineMode, query: &str) -> QueryResultSet {
    shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "union-distinct").with_cypher_engine(engine),
            query,
        )
        .await
        .unwrap_or_else(|error| panic!("{engine:?} rejected {query}: {error}"))
}

#[tokio::test]
async fn union_distinct_dedupes_rows_from_the_first_arm() {
    let shard = seeded_items("graph/union-distinct-first-arm").await;
    let query = "MATCH (n:item) RETURN n.category AS category \
                 UNION MATCH (m:item {id: 1}) RETURN m.category AS category";
    let experimental = categories(&shard, CypherEngineMode::Experimental, query).await;
    let legacy = categories(&shard, CypherEngineMode::Legacy, query).await;
    assert_eq!(experimental, expected());
    assert_eq!(experimental, legacy);
    shard.close().await.expect("close graph shard");
}

#[tokio::test]
async fn union_distinct_does_not_depend_on_arm_order() {
    let shard = seeded_items("graph/union-distinct-arm-order").await;
    let forward = "MATCH (n:item) RETURN n.category AS category \
                   UNION MATCH (m:item {id: 1}) RETURN m.category AS category";
    let reversed = "MATCH (m:item {id: 1}) RETURN m.category AS category \
                    UNION MATCH (n:item) RETURN n.category AS category";
    let expected = expected();
    for query in [forward, reversed] {
        let experimental = categories(&shard, CypherEngineMode::Experimental, query).await;
        let legacy = categories(&shard, CypherEngineMode::Legacy, query).await;
        assert_eq!(experimental, expected);
        assert_eq!(experimental, legacy);
    }
    shard.close().await.expect("close graph shard");
}
