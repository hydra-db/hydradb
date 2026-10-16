#![cfg(feature = "opencypher")]

use hydradb::{CypherEngineMode, GraphShard, QueryContext, VertexMetadata};
use slatedb::object_store::memory::InMemory;
use std::sync::Arc;

async fn connected_fixture() -> GraphShard {
    let shard =
        GraphShard::open_standalone_writer("delete-statement-atomicity", Arc::new(InMemory::new()))
            .await
            .unwrap();
    for (index, query) in [
        "CREATE (a:Person {id: 1})-[:TEMP]->(d:Person {id: 4})",
        "CREATE (b:Person {id: 2})-[:KNOWS]->(c:Person {id: 3})",
        "MATCH (a:Person {id: 1})-[r:TEMP]->(d:Person {id: 4}) DELETE r",
    ]
    .iter()
    .enumerate()
    {
        shard
            .execute_cypher(QueryContext::new("review", format!("setup-{index}")), query)
            .await
            .unwrap();
    }
    shard
}

async fn person_exists(shard: &GraphShard, id: u64) -> bool {
    !shard
        .execute_cypher_rows(
            QueryContext::new("review", format!("read-{id}")),
            &format!("MATCH (n:Person {{id: {id}}}) RETURN n.id"),
        )
        .await
        .unwrap()
        .rows
        .is_empty()
}

#[tokio::test]
async fn failed_multi_vertex_delete_preserves_graph_state() {
    for (mode, suffix) in [
        (CypherEngineMode::Legacy, "legacy"),
        (CypherEngineMode::Experimental, "experimental"),
    ] {
        let shard = connected_fixture().await;
        assert!(person_exists(&shard, 1).await);
        assert!(person_exists(&shard, 2).await);

        let result = shard
            .execute_cypher(
                QueryContext::new("review", format!("delete-{suffix}")).with_cypher_engine(mode),
                "MATCH (n:Person) WHERE n.id = 1 OR n.id = 2 DELETE n",
            )
            .await;

        assert!(result.is_err(), "connected vertex requires DETACH DELETE");
        assert!(
            format!("{:?}", result.unwrap_err()).contains("requires DETACH"),
            "the query should fail because vertex 2 is connected"
        );
        assert!(
            person_exists(&shard, 1).await,
            "isolated vertex was deleted"
        );
        assert!(
            person_exists(&shard, 2).await,
            "connected vertex was deleted"
        );
        assert!(shard.edge_exists("review", "KNOWS", 2, 3).await.unwrap());
        shard.close().await.unwrap();
    }
}

#[tokio::test]
async fn successful_multi_vertex_delete_still_deletes_all_isolated_vertices() {
    let shard = GraphShard::open_standalone_writer(
        "delete-statement-atomicity-isolated",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    for id in 1..=2 {
        shard
            .set_vertex_metadata("review", id, VertexMetadata::default().with_label("Person"))
            .await
            .unwrap();
    }

    shard
        .execute_cypher(
            QueryContext::new("review", "delete-isolated"),
            "MATCH (n:Person) WHERE n.id = 1 OR n.id = 2 DELETE n",
        )
        .await
        .unwrap();

    assert!(!person_exists(&shard, 1).await);
    assert!(!person_exists(&shard, 2).await);
    shard.close().await.unwrap();
}
