//! `WHERE` written inside a node or relationship pattern.
//!
//! `(n WHERE p)` and `-[r WHERE p]->` mean what a clause `WHERE p` would for
//! the same pattern. The matrix rows `compat_node_local_where` and
//! `compat_relationship_local_where` hold the same answers to Neo4j.

mod common;

use common::compat::{refusal, run, table};

#[tokio::test]
async fn a_node_predicate_filters_the_node_it_is_written_on() {
    let result = run("MATCH (i:Item WHERE i.score > 2) RETURN i.name AS name ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"apple\""], ["null"]]);
}

#[tokio::test]
async fn a_relationship_predicate_filters_the_relationship() {
    let result = run("MATCH (h:Hub)-[r:HAS WHERE r.weight > 1]->(i:Item) RETURN i.name AS name ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"apple\""], ["\"carrot\""]]);
}

#[tokio::test]
async fn an_anonymous_relationship_may_carry_a_predicate_on_outer_names() {
    let result = run("MATCH (h:Hub)-[:HAS WHERE h.name = 'hub']->(i:Item WHERE i.score < 2) RETURN i.name AS name").await;
    assert_eq!(table(&result), rows![["\"banana\""]]);
}

#[tokio::test]
async fn a_target_predicate_may_read_the_source() {
    // The source is bound by the time the target is, so this filters the hop.
    let result = run("MATCH (a:Node {name: 'a'})-[:R]->(b WHERE b.name > a.name) RETURN b.name AS name ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"b\""], ["\"c\""]]);
}

#[tokio::test]
async fn a_source_predicate_may_read_the_target_and_waits_for_it() {
    // `a` is matched first, so its WHERE cannot run until `b` is bound.
    let result = run("MATCH (a:Node WHERE a.name < b.name)-[:R]->(b:Node) RETURN a.name AS a, b.name AS b ORDER BY a ASC, b ASC").await;
    assert_eq!(
        table(&result),
        rows![["\"a\"", "\"b\""], ["\"a\"", "\"c\""], ["\"b\"", "\"d\""], ["\"c\"", "\"d\""]]
    );
}

#[tokio::test]
async fn element_and_clause_predicates_combine() {
    let result = run("MATCH (i:Item WHERE i.kind = 'fruit') WHERE i.score >= 1 RETURN i.name AS name ORDER BY name ASC").await;
    assert_eq!(table(&result), rows![["\"apple\""], ["\"banana\""]]);
}

#[tokio::test]
async fn an_inline_map_and_a_where_both_apply() {
    let result = run("MATCH (i:Item {kind: 'fruit'} WHERE i.score IS NULL) RETURN i.name AS name").await;
    assert_eq!(table(&result), rows![["\"Durian\""]]);
}

#[tokio::test]
async fn an_optional_match_keeps_its_row_when_the_element_predicate_fails() {
    // The predicate belongs to the optional pattern, as a clause WHERE on an
    // OPTIONAL MATCH does: a hub whose edges all fail it keeps its row.
    let result = run("MATCH (h:Hub) OPTIONAL MATCH (h)-[r:HAS WHERE r.weight > 10]->(i) RETURN h.name AS hub, i.name AS item").await;
    assert_eq!(table(&result), rows![["\"hub\"", "null"]]);
}

#[tokio::test]
async fn a_predicate_on_an_unbound_name_is_refused() {
    for predicate in ["missing.score > 2", "missing IS NULL", "missing IS NOT NULL"] {
        for query in [
            format!("MATCH (i:Item WHERE {predicate}) RETURN i.name AS name"),
            format!("MATCH (i:Item) WHERE {predicate} RETURN i.name AS name"),
        ] {
            assert!(refusal(&query).await.contains("unbound variable missing"), "{query}");
        }
    }
}

#[tokio::test]
async fn a_variable_length_relationship_predicate_is_refused() {
    assert!(refusal("MATCH (a:Node)-[:R*1..2 WHERE a.name = 'a']->(b) RETURN b.name AS name")
        .await
        .contains("variable-length relationship is not supported"));
}

#[tokio::test]
async fn local_predicate_waits_for_later_comma_pattern() {
    assert_eq!(
        table(&run("MATCH (a:Item WHERE a.score < b.score), (b:Item) RETURN count(*) AS n").await),
        rows![["10"]]
    );
}

#[tokio::test]
async fn optional_cross_pattern_predicate_keeps_the_left_row_on_miss() {
    assert_eq!(
        table(
            &run("MATCH (h:Hub) OPTIONAL MATCH (a:Item WHERE a.score > b.score + 100), (b:Item) RETURN h.name AS hub, a.name AS a").await
        ),
        rows![["\"hub\"", "null"]]
    );
}

#[tokio::test]
async fn bound_target_predicate_preserves_reverse_anchoring() {
    use hydradb_cypher_engine::{CypherEngine, ParameterMap};
    let graph = common::compat::graph();
    let requests = graph.expand_requests.clone();
    let engine = CypherEngine::new(graph);
    let prepared = engine
        .prepare(
            "MATCH (h:Hub) MATCH (i:Item)<-[:HAS]-(h WHERE h.name = 'hub') RETURN count(*) AS n",
            &ParameterMap::new(),
        )
        .unwrap();
    assert_eq!(table(&engine.execute_prepared(&prepared).await.unwrap()), rows![["3"]]);
    // The fixture HAS edges point outward from h; the reversed incoming walk
    // should still expand from h rather than scanning every Item.
    assert!(requests.lock().unwrap().iter().all(|request| request.input_vertex_ids == [1010]));
}
