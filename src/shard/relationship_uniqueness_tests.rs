//! Relationship uniqueness across the parts of one `MATCH` (#290).
//!
//! Cypher binds each relationship at most once per match: in
//! `MATCH (a)-[r]->(b), (a)-[s]->(b)` the variables `r` and `s` must be two
//! different relationships, even though they sit in different comma-separated
//! pattern parts. With a single relationship between `a` and `b` the pattern
//! matches nothing, so a `DETACH DELETE b` over it must leave the graph alone.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;

use crate::{CypherEngineMode, EdgeMutation, GraphShard, QueryContext, QueryValue, VertexMetadata};

const CELL: &str = "cell-a";

async fn shard(path: &str) -> GraphShard {
    GraphShard::open_standalone_writer(path, Arc::new(InMemory::new()))
        .await
        .expect("open graph shard")
}

fn context(engine: CypherEngineMode, key: &str) -> QueryContext {
    QueryContext::new(CELL, key).with_cypher_engine(engine)
}

async fn mutate(shard: &GraphShard, engine: CypherEngineMode, key: &str, query: &str) {
    shard
        .execute_cypher(context(engine, key), query)
        .await
        .unwrap_or_else(|error| panic!("{engine:?} rejected {query}: {error}"));
}

async fn rows(shard: &GraphShard, engine: CypherEngineMode, query: &str) -> Vec<Vec<QueryValue>> {
    shard
        .execute_cypher_rows(context(engine, "read"), query)
        .await
        .unwrap_or_else(|error| panic!("{engine:?} rejected {query}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn check<T: PartialEq + std::fmt::Debug>(failures: &mut Vec<String>, actual: T, expected: T, what: String) {
    if actual != expected {
        failures.push(format!("{what}: got {actual:?}, expected {expected:?}"));
    }
}

fn engines() -> Vec<CypherEngineMode> {
    vec![
        CypherEngineMode::Legacy,
        #[cfg(feature = "experimental-cypher-engine")]
        CypherEngineMode::Experimental,
        #[cfg(feature = "datafusion-cypher-engine")]
        CypherEngineMode::DataFusion,
    ]
}

const TWO_PARTS: &str = "MATCH (a:Person {id: 1})-[r:R]->(b), (a)-[s:R]->(b)";

/// One relationship cannot fill both `r` and `s`, so the read matches nothing.
#[tokio::test]
async fn one_relationship_cannot_bind_two_variables_of_one_match() {
    let mut failures = Vec::new();
    for engine in engines() {
        let shard = shard(&format!("graph/uniqueness-read-{}", engine.as_str())).await;
        mutate(&shard, engine, "seed", "CREATE (a:Person {id: 1})-[:R]->(b:Person {id: 2})").await;

        check(
            &mut failures,
            rows(&shard, engine, &format!("{TWO_PARTS} RETURN b.id AS id")).await,
            Vec::<Vec<QueryValue>>::new(),
            format!("{engine:?}: a single relationship matched both r and s"),
        );
        shard.close().await.expect("close graph shard");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The write path must agree with the read path: a pattern that matches
/// nothing deletes nothing.
#[tokio::test]
async fn detach_delete_over_an_unmatched_pattern_keeps_the_node() {
    let mut failures = Vec::new();
    for engine in engines() {
        let shard = shard(&format!("graph/uniqueness-delete-{}", engine.as_str())).await;
        mutate(&shard, engine, "seed", "CREATE (a:Person {id: 1})-[:R]->(b:Person {id: 2})").await;
        mutate(&shard, engine, "delete", &format!("{TWO_PARTS} DETACH DELETE b")).await;

        check(
            &mut failures,
            rows(&shard, engine, "MATCH (b:Person {id: 2}) RETURN b.id AS id").await,
            vec![vec![QueryValue::VertexId(2)]],
            format!("{engine:?}: DETACH DELETE removed a node the pattern does not match"),
        );
        shard.close().await.expect("close graph shard");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Control: two parallel relationships do satisfy the pattern, once per
/// ordered `(r, s)` pair, and the delete then removes `b`.
#[tokio::test]
async fn two_parallel_relationships_still_match() {
    let mut failures = Vec::new();
    for engine in engines() {
        let shard = shard(&format!("graph/uniqueness-parallel-{}", engine.as_str())).await;
        // Two relationships between the same pair. The shard API, because
        // the legacy engine has no `MATCH ... CREATE`.
        for key in ["first", "second"] {
            shard
                .create_relationship_with_vertex_metadata(
                    EdgeMutation {
                        cell_id: CELL.to_string(),
                        edge_type: "R".to_string(),
                        src: 1,
                        dst: 2,
                        idempotency_key: key.to_string(),
                    },
                    VertexMetadata::default().with_label("Person"),
                    VertexMetadata::default().with_label("Person"),
                )
                .await
                .expect("create relationship");
        }

        check(
            &mut failures,
            rows(&shard, engine, &format!("{TWO_PARTS} RETURN b.id AS id")).await.len(),
            2,
            format!("{engine:?}: two parallel relationships give two (r, s) orderings"),
        );

        mutate(&shard, engine, "delete", &format!("{TWO_PARTS} DETACH DELETE b")).await;
        check(
            &mut failures,
            rows(&shard, engine, "MATCH (b:Person {id: 2}) RETURN b.id AS id").await,
            Vec::<Vec<QueryValue>>::new(),
            format!("{engine:?}: the matched node was not deleted"),
        );
        shard.close().await.expect("close graph shard");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// One relationship per read query, against the single-relationship graph.
async fn single_relationship_rows(engine: CypherEngineMode, path: &str, query: &str) -> Vec<Vec<QueryValue>> {
    let shard = shard(&format!("graph/{path}-{}", engine.as_str())).await;
    mutate(&shard, engine, "seed", "CREATE (a:Person {id: 1})-[:R]->(b:Person {id: 2})").await;
    let result = rows(&shard, engine, query).await;
    shard.close().await.expect("close graph shard");
    result
}

/// Uniqueness is per MATCH clause, not per query: a second MATCH may bind the
/// relationship the first one bound.
#[tokio::test]
async fn a_later_match_clause_may_rebind_the_same_relationship() {
    let mut failures = Vec::new();
    for engine in engines() {
        check(
            &mut failures,
            single_relationship_rows(
                engine,
                "uniqueness-two-clauses",
                "MATCH (a:Person {id: 1})-[r:R]->(b) MATCH (a)-[s:R]->(b) RETURN b.id AS id",
            )
            .await,
            vec![vec![QueryValue::VertexId(2)]],
            format!("{engine:?}: a second MATCH could not rebind r's relationship"),
        );

        let shard = shard(&format!("graph/uniqueness-two-clause-delete-{}", engine.as_str())).await;
        mutate(&shard, engine, "seed", "CREATE (a:Person {id: 1})-[:R]->(b:Person {id: 2})").await;
        mutate(
            &shard,
            engine,
            "delete",
            "MATCH (a:Person {id: 1})-[r:R]->(b) MATCH (a)-[s:R]->(b) DETACH DELETE b",
        )
        .await;
        check(
            &mut failures,
            rows(&shard, engine, "MATCH (b:Person {id: 2}) RETURN b.id AS id").await,
            Vec::<Vec<QueryValue>>::new(),
            format!("{engine:?}: a two-clause DETACH DELETE kept a node both clauses match"),
        );
        shard.close().await.expect("close graph shard");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The rule holds along one path too, not only across comma-separated parts.
#[tokio::test]
async fn one_relationship_cannot_close_a_path_back_on_itself() {
    let mut failures = Vec::new();
    for engine in engines() {
        check(
            &mut failures,
            single_relationship_rows(
                engine,
                "uniqueness-path",
                "MATCH (a:Person {id: 1})-[r:R]->(b)<-[s:R]-(a) RETURN b.id AS id",
            )
            .await,
            Vec::<Vec<QueryValue>>::new(),
            format!("{engine:?}: a path reused one relationship"),
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Writing one name twice asks for one relationship, so it is not a conflict.
#[tokio::test]
async fn one_name_written_twice_is_one_relationship() {
    let mut failures = Vec::new();
    for engine in engines() {
        check(
            &mut failures,
            single_relationship_rows(
                engine,
                "uniqueness-same-name",
                "MATCH (a:Person {id: 1})-[r:R]->(b), (a)-[r:R]->(b) RETURN b.id AS id",
            )
            .await,
            vec![vec![QueryValue::VertexId(2)]],
            format!("{engine:?}: a repeated name was treated as two relationships"),
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
