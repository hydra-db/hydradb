#![cfg(feature = "opencypher")]

use hydradb::{
    CypherEngineMode, EdgeMetadata, EdgeMutation, GraphShard, QueryContext, QueryOutput, QueryRow,
    QueryValue, VertexMetadata, VertexPropertyValue,
};
use slatedb::object_store::memory::InMemory;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);

fn context(mode: CypherEngineMode) -> QueryContext {
    QueryContext::new(
        "test",
        format!("query-{}", NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)),
    )
    .with_cypher_engine(mode)
}

fn engines() -> Vec<CypherEngineMode> {
    let mut modes = vec![CypherEngineMode::Legacy];
    if cfg!(feature = "experimental-cypher-engine") {
        modes.push(CypherEngineMode::Experimental);
    }
    modes
}

async fn fixture() -> GraphShard {
    let shard =
        GraphShard::open_standalone_writer("set-delete-metadata", Arc::new(InMemory::new()))
            .await
            .unwrap();
    for id in 1..=4 {
        shard
            .set_vertex_metadata(
                "test",
                id,
                VertexMetadata::default()
                    .with_label("Person")
                    .with_property("name", VertexPropertyValue::String("Before".into())),
            )
            .await
            .unwrap();
    }
    shard
}

async fn execute(shard: &GraphShard, mode: CypherEngineMode, query: &str) -> QueryOutput {
    shard
        .execute_cypher(context(mode), query)
        .await
        .unwrap_or_else(|error| panic!("{mode:?}: {query}: {error:?}"))
}

async fn rows(shard: &GraphShard, mode: CypherEngineMode, query: &str) -> Vec<QueryRow> {
    shard
        .execute_cypher_rows(context(mode), query)
        .await
        .unwrap()
        .rows
}

async fn assert_deleted(shard: &GraphShard, mode: CypherEngineMode, id: u64) {
    for query in [
        format!("MATCH (n:Person {{id: {id}}}) RETURN n.id"),
        format!("MATCH (n:Extra {{id: {id}}}) RETURN n.id"),
        format!("MATCH (n {{id: {id}}}) WHERE n.name = 'Before' OR n.name = 'Alice' RETURN n.name"),
    ] {
        assert!(
            rows(shard, mode, &query).await.is_empty(),
            "{mode:?}: {query}"
        );
    }
}

#[tokio::test]
async fn deleted_vertices_do_not_flush_buffered_properties_or_labels() {
    for mode in engines() {
        for query in [
            "MATCH (n:Person {id: 1}) SET n.name = 'Alice' DELETE n",
            "MATCH (n:Person {id: 1}) SET n:Extra DELETE n",
            "MATCH (n:Person {id: 1}) REMOVE n.name DELETE n",
            "MATCH (n:Person {id: 1}) REMOVE n:Person DELETE n",
            "MATCH (n:Person {id: 1}) SET n.name = 'Alice' SET n:Extra DELETE n",
            "MATCH (a:Person {id: 1}), (b:Person {id: 1}) SET a.name = 'Alice' DELETE b",
            "MATCH (n:Person {id: 1}) DELETE n",
        ] {
            let shard = fixture().await;
            execute(&shard, mode, query).await;
            assert_deleted(&shard, mode, 1).await;
            shard.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn buffered_updates_are_discarded_for_every_deleted_vertex() {
    for mode in engines() {
        for query in [
            "MATCH (n:Person) WHERE n.id = 1 OR n.id = 2 SET n.name = 'Alice' DELETE n",
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) SET a.name = 'Alice', b.name = 'Alice' DELETE a, b",
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) SET a.name = 'Alice', b.name = 'Alice' DELETE a, a, b",
        ] {
            let shard = fixture().await;
            execute(&shard, mode, query).await;
            assert_deleted(&shard, mode, 1).await;
            assert_deleted(&shard, mode, 2).await;
            assert_eq!(rows(&shard, mode, "MATCH (n:Person) RETURN n.id").await.len(), 2);
            shard.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn deleting_one_vertex_keeps_another_vertices_buffered_update() {
    for mode in engines() {
        let shard = fixture().await;
        execute(&shard, mode, "MATCH (a:Person {id: 1}), (b:Person {id: 2}) SET a.name = 'Alice', b.name = 'Changed' DELETE a").await;
        assert_deleted(&shard, mode, 1).await;
        assert_eq!(
            rows(&shard, mode, "MATCH (n:Person {id: 2}) RETURN n.name").await,
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::String("Changed".into())
            )])]
        );
        shard.close().await.unwrap();
    }
}

#[tokio::test]
async fn relationship_delete_discards_buffered_updates_in_both_storage_forms() {
    for mode in engines() {
        for structural in [false, true] {
            for query in [
                "MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person {id: 2}) SET r.weight = 7 DELETE r",
                "MATCH (a:Person {id: 2})<-[r:KNOWS]-(b:Person {id: 1}) SET r.weight = 7 DELETE r",
                "MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person {id: 2}) REMOVE r.weight DELETE r",
                "MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person {id: 2}) SET r.weight = 7 DELETE r, r",
            ] {
                let shard = fixture().await;
                if structural {
                    shard.write_edge(EdgeMutation { cell_id: "test".into(), edge_type: "KNOWS".into(), src: 1, dst: 2, idempotency_key: "setup".into() }).await.unwrap();
                    shard.set_edge_metadata("test", "KNOWS", 1, 2, EdgeMetadata::default().with_property("weight", VertexPropertyValue::Integer(1))).await.unwrap();
                } else {
                    execute(&shard, mode, "CREATE (a:Person {id: 1})-[:KNOWS {weight: 1}]->(b:Person {id: 2})").await;
                }
                let output = execute(&shard, mode, query).await;
                let QueryOutput::Mutation(result) = output else { panic!("expected mutation"); };
                assert_eq!(result.updated_relationships, 0);
                assert!(rows(&shard, mode, "MATCH (a)-[r:KNOWS]->(b) RETURN r.weight").await.is_empty());
                assert!(!rows(&shard, mode, "MATCH (n:Person {id: 1}) RETURN n.id").await.is_empty());
                assert!(!rows(&shard, mode, "MATCH (n:Person {id: 2}) RETURN n.id").await.is_empty());
                shard.close().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn relationship_aliases_share_the_deleted_identity() {
    for mode in engines() {
        let shard = fixture().await;
        execute(
            &shard,
            mode,
            "CREATE (a:Person {id: 1})-[:KNOWS {weight: 1}]->(b:Person {id: 2})",
        )
        .await;
        execute(&shard, mode, "MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person {id: 2}), (a)-[s:KNOWS]->(b) SET s.weight = 7 DELETE r").await;
        assert!(
            rows(&shard, mode, "MATCH (a)-[r:KNOWS]->(b) RETURN r.weight")
                .await
                .is_empty()
        );
        shard.close().await.unwrap();
    }
}

#[tokio::test]
async fn unmatched_delete_does_not_change_any_vertex() {
    for mode in engines() {
        let shard = fixture().await;
        let output = execute(
            &shard,
            mode,
            "MATCH (n:Person {id: 99}) SET n.name = 'Alice' DELETE n",
        )
        .await;
        let QueryOutput::Mutation(result) = output else {
            panic!("expected mutation");
        };
        assert_eq!(result.matched_rows, 0);
        assert_eq!(result.updated_vertices, 0);
        assert_eq!(
            rows(
                &shard,
                mode,
                "MATCH (n:Person) WHERE n.name = 'Before' RETURN n.id"
            )
            .await
            .len(),
            4
        );
        shard.close().await.unwrap();
    }
}

#[tokio::test]
async fn detach_delete_discards_incident_relationship_updates_in_either_direction() {
    for mode in engines() {
        for setup in [
            "CREATE (a:Person {id: 1})-[:KNOWS {weight: 1}]->(b:Person {id: 2})",
            "CREATE (b:Person {id: 2})-[:KNOWS {weight: 1}]->(a:Person {id: 1})",
            "CREATE (a:Person {id: 1})-[:KNOWS {weight: 1}]->(a:Person {id: 1})",
        ] {
            let shard = fixture().await;
            execute(&shard, mode, setup).await;
            execute(&shard, mode, "MATCH (a:Person {id: 1})-[r:KNOWS]-(b) SET a.name = 'Alice', r.weight = 7 DETACH DELETE a").await;
            assert_deleted(&shard, mode, 1).await;
            assert!(
                rows(&shard, mode, "MATCH (a)-[r:KNOWS]->(b) RETURN r.weight")
                    .await
                    .is_empty()
            );
            shard.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn deleting_one_parallel_relationship_keeps_the_other_update() {
    for mode in engines() {
        let shard = fixture().await;
        for tag in ["remove", "keep"] {
            execute(&shard, mode, &format!("CREATE (a:Person {{id: 1}})-[:KNOWS {{tag: '{tag}', weight: 1}}]->(b:Person {{id: 2}})")).await;
        }
        execute(&shard, mode, "MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person {id: 2}), (a)-[s:KNOWS]->(b) WHERE r.tag = 'remove' AND s.tag = 'keep' SET r.weight = 7, s.weight = 8 DELETE r").await;
        assert_eq!(
            rows(
                &shard,
                mode,
                "MATCH (a)-[r:KNOWS]->(b) RETURN r.tag, r.weight"
            )
            .await,
            vec![QueryRow::new(vec![
                QueryValue::Property(VertexPropertyValue::String("keep".into())),
                QueryValue::Property(VertexPropertyValue::Integer(8))
            ])]
        );
        shard.close().await.unwrap();
    }
}

#[tokio::test]
async fn detach_delete_keeps_unrelated_relationship_updates() {
    for mode in engines() {
        let shard = fixture().await;
        execute(
            &shard,
            mode,
            "CREATE (a:Person {id: 1})-[:KNOWS {weight: 1}]->(b:Person {id: 2})",
        )
        .await;
        execute(
            &shard,
            mode,
            "CREATE (c:Person {id: 3})-[:KNOWS {weight: 1}]->(d:Person {id: 4})",
        )
        .await;
        execute(&shard, mode, "MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person {id: 2}), (c:Person {id: 3})-[s:KNOWS]->(d:Person {id: 4}) SET r.weight = 7, s.weight = 8 DETACH DELETE a").await;
        assert_deleted(&shard, mode, 1).await;
        assert_eq!(
            rows(
                &shard,
                mode,
                "MATCH (c:Person {id: 3})-[s:KNOWS]->(d:Person {id: 4}) RETURN s.weight"
            )
            .await,
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::Integer(8)
            )])]
        );
        shard.close().await.unwrap();
    }
}

#[tokio::test]
async fn connected_vertex_rejection_does_not_flush_its_update() {
    for mode in engines() {
        let shard = fixture().await;
        execute(
            &shard,
            mode,
            "CREATE (a:Person {id: 1, name: 'Before'})-[:KNOWS]->(b:Person {id: 2})",
        )
        .await;
        let result = shard
            .execute_cypher(
                QueryContext::new("test", "rejected-delete").with_cypher_engine(mode),
                "MATCH (n:Person {id: 1}) SET n.name = 'Alice' DELETE n",
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            rows(&shard, mode, "MATCH (n:Person {id: 1}) RETURN n.name").await,
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::String("Before".into())
            )])]
        );
        assert!(shard.edge_exists("test", "KNOWS", 1, 2).await.unwrap());
        shard.close().await.unwrap();
    }
}
