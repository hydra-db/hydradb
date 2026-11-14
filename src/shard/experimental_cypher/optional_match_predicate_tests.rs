//! Parity for a `WHERE` that belongs to an `OPTIONAL MATCH`.
//!
//! openCypher attaches the predicate to the optional pattern, so a row that
//! fails it is not dropped: the optional side contributes nothing and the
//! left row survives with the optional bindings null. Applying the predicate
//! after the join instead drops those rows, which turns every
//! "rows whose optional side does not match X" query into "rows with no
//! optional side at all".

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;

use crate::{CypherEngineMode, EdgeMutation, GraphShard, QueryContext, QueryValue, VertexMetadata, VertexPropertyValue};

async fn shard(path: &str) -> GraphShard {
    GraphShard::open_standalone_writer(path, Arc::new(InMemory::new()))
        .await
        .expect("open graph shard")
}

/// Two entities, one with an outgoing relationship to a `beta` node.
async fn seeded_shard(path: &str) -> GraphShard {
    let shard = shard(path).await;
    for (id, name) in [(1, "alpha"), (2, "orphan"), (3, "beta")] {
        shard
            .set_vertex_metadata(
                "cell-a",
                id,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("name", VertexPropertyValue::String(name.to_string())),
            )
            .await
            .expect("write vertex metadata");
    }
    shard
        .write_edge(EdgeMutation {
            cell_id: "cell-a".to_string(),
            edge_type: "RELATES".to_string(),
            src: 1,
            dst: 3,
            idempotency_key: "optional-where-edge".to_string(),
        })
        .await
        .expect("write relationship");
    shard
}

async fn ids(shard: &GraphShard, engine: CypherEngineMode, query: &str) -> Vec<u64> {
    let result = shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "optional-match-predicate").with_cypher_engine(engine),
            query,
        )
        .await
        .unwrap_or_else(|error| panic!("{engine:?} rejected {query}: {error}"));
    result
        .rows
        .iter()
        .map(|row| match row.values[0] {
            QueryValue::VertexId(id) => id,
            ref other => panic!("unexpected id: {other:?}"),
        })
        .collect()
}

/// A predicate on the optional side keeps every left row: the one whose
/// optional match fails it comes back with the optional bindings null.
#[tokio::test]
async fn an_optional_match_predicate_does_not_drop_left_rows() {
    let shard = seeded_shard("graph/optional-match-predicate-parity").await;
    let query = "MATCH (a:Entity) OPTIONAL MATCH (a)-[r:RELATES]->(b:Entity) \
                 WHERE b.name = 'nothing' RETURN a.id AS id ORDER BY id";

    let legacy = ids(&shard, CypherEngineMode::Legacy, query).await;
    let experimental = ids(&shard, CypherEngineMode::Experimental, query).await;
    assert_eq!(legacy, vec![1, 2, 3], "legacy keeps every left row");
    assert_eq!(experimental, legacy, "the engines disagree");

    shard.close().await.expect("close graph shard");
}

/// The same, for a predicate the optional match does satisfy: only the rows
/// whose optional side matched carry its bindings, and the rest still appear.
#[tokio::test]
async fn an_optional_match_predicate_that_matches_keeps_every_row_too() {
    let shard = seeded_shard("graph/optional-match-predicate-parity-match").await;
    let query = "MATCH (a:Entity) OPTIONAL MATCH (a)-[r:RELATES]->(b:Entity) \
                 WHERE b.name = 'beta' RETURN a.id AS id ORDER BY id";

    let legacy = ids(&shard, CypherEngineMode::Legacy, query).await;
    let experimental = ids(&shard, CypherEngineMode::Experimental, query).await;
    assert_eq!(legacy, vec![1, 2, 3]);
    assert_eq!(experimental, legacy, "the engines disagree");

    shard.close().await.expect("close graph shard");
}

/// The predicate may name a binding from an earlier clause. The optional side
/// is executed once per left row, so filtering it there still sees `a`.
#[tokio::test]
async fn an_optional_match_predicate_may_reference_an_outer_binding() {
    let shard = seeded_shard("graph/optional-match-predicate-correlated").await;
    let query = "MATCH (a:Entity) OPTIONAL MATCH (a)-[r:RELATES]->(b:Entity) \
                 WHERE b.name = a.name RETURN a.id AS id ORDER BY id";

    let legacy = ids(&shard, CypherEngineMode::Legacy, query).await;
    let experimental = ids(&shard, CypherEngineMode::Experimental, query).await;
    // No relationship joins two entities of the same name, so every row keeps
    // its place with the optional side null.
    assert_eq!(legacy, vec![1, 2, 3]);
    assert_eq!(experimental, legacy, "the engines disagree");

    shard.close().await.expect("close graph shard");
}

/// An ordinary MATCH is an inner join: its predicate still drops rows.
#[tokio::test]
async fn a_required_match_predicate_still_drops_rows() {
    let shard = seeded_shard("graph/required-match-predicate").await;
    let query = "MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE b.name = 'nothing' \
                 RETURN a.id AS id ORDER BY id";

    let legacy = ids(&shard, CypherEngineMode::Legacy, query).await;
    let experimental = ids(&shard, CypherEngineMode::Experimental, query).await;
    assert!(legacy.is_empty());
    assert_eq!(experimental, legacy, "the engines disagree");

    shard.close().await.expect("close graph shard");
}
