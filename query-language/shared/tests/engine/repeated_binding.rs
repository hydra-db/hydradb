//! A pattern naming one node at both ends of a hop: `(n)-[:R]->(n)`, and a
//! later pattern naming a relationship an earlier one bound:
//! `MATCH (h)-[r]->(i) MATCH (h)-[r]->(x)`.
//!
//! The matrix row `compat_self_loop` holds the shared answer to Neo4j.
//!
//! A reused relationship names its own endpoints, so the hop over it reads no
//! adjacency: `BoundRelationshipExec` checks it between them. Expanding it
//! from its source instead walked the whole of a hub's edges to find the one
//! the row already held, and failed the query's edge budget on a hub.

mod common;

use common::compat::{graph, run, table};
use common::MemoryStorage;
use hydradb_cypher_engine::{CypherEngine, ParameterMap, RelationshipRecord, VertexRecord};

#[tokio::test]
async fn a_repeated_adjacent_node_matches_self_loops() {
    let result = run("MATCH (n)-[:R]->(n) RETURN n.name AS name").await;
    assert_eq!(table(&result), rows![["\"loop\""]]);
}

#[tokio::test]
async fn labels_properties_and_a_relationship_name_still_apply() {
    let result = run("MATCH (n:Node {name: 'loop'})-[r:R]->(n) RETURN n.name AS name, r IS NULL AS missing").await;
    assert_eq!(table(&result), rows![["\"loop\"", "false"]]);
    let none = run("MATCH (n:Item)-[:R]->(n) RETURN n.name AS name").await;
    assert!(table(&none).is_empty());
}

#[tokio::test]
async fn the_path_continues_from_the_repeated_node() {
    // a's only incoming R is from d, which has no self-loop.
    let result = run("MATCH (n:Node {name: 'a'})<-[:R]-(m)-[:R]->(m) RETURN m.name AS name").await;
    assert!(table(&result).is_empty());
}

#[tokio::test]
async fn a_node_predicate_on_the_repeated_node_reads_it() {
    let result = run("MATCH (n)-[:R]->(n WHERE n.name = 'loop') RETURN n.name AS name").await;
    assert_eq!(table(&result), rows![["\"loop\""]]);
}

#[tokio::test]
async fn repeated_variable_endpoint_matches_cycle_trails() {
    assert_eq!(
        table(&run("MATCH (n:Node {name:'a'})-[:R*1..3]->(n) RETURN count(*) AS n").await),
        rows![["2"]]
    );
    assert!(table(&run("MATCH (n:Item)-[:R*1..3]->(n) RETURN n").await).is_empty());
}

#[tokio::test]
async fn repeated_variable_endpoint_zero_hop_preserves_node_constraints() {
    assert_eq!(
        table(&run("MATCH (n:Node {name:'a'})-[:R*0..0]->(n {name:'a'}) RETURN count(*) AS n").await),
        rows![["1"]]
    );
    assert!(table(&run("MATCH (n:Node {name:'a'})-[:R*0..0]->(n {name:'b'}) RETURN n").await).is_empty());
}

/// Binds `r` to the Hub's HAS edge to banana and its endpoints to `h`, `i`.
const BANANA: &str = "MATCH (h:Hub)-[r:HAS]->(i:Item {name: 'banana'}) ";

/// `query`'s rows, and the expansions and label scans it issued beyond those
/// of `anchor` run alone: what the rest of the query read.
async fn rows_and_extra_reads(storage: &MemoryStorage, anchor: &str, query: &str) -> (Vec<Vec<String>>, usize, usize) {
    let engine = CypherEngine::new(storage.clone());
    let reads = || {
        (
            storage.expand_requests.lock().unwrap().len(),
            storage.label_scan_limits.lock().unwrap().len(),
        )
    };
    let before = reads();
    let prepared = engine
        .prepare(&format!("{anchor}RETURN 1 AS one"), &ParameterMap::new())
        .expect("prepare anchor");
    engine.execute_prepared(&prepared).await.expect("execute anchor");
    let anchored = reads();
    let prepared = engine
        .prepare(query, &ParameterMap::new())
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let result = engine
        .execute_prepared(&prepared)
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let after = reads();
    let anchor_reads = (anchored.0 - before.0, anchored.1 - before.1);
    (
        table(&result),
        (after.0 - anchored.0).saturating_sub(anchor_reads.0),
        (after.1 - anchored.1).saturating_sub(anchor_reads.1),
    )
}

fn explain(query: &str) -> String {
    CypherEngine::new(graph())
        .prepare(query, &ParameterMap::new())
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .explain()
}

#[tokio::test]
async fn a_reused_relationship_is_checked_between_its_endpoints() {
    let query = format!("{BANANA}MATCH (h)-[r:HAS]->(x) RETURN x.name AS x");
    let plan = explain(&query);
    assert!(
        plan.contains("BoundRelationshipExec(relationship=r, from=h, source_labels=[], types=[HAS], direction=outgoing, to=x,"),
        "{plan}"
    );
    assert_eq!(
        plan.matches("ExpandExec(").count(),
        1,
        "only the anchor expands; r is not expanded again: {plan}"
    );
    let storage = graph();
    let (rows, expansions, scans) = rows_and_extra_reads(&storage, BANANA, &query).await;
    assert_eq!(rows, rows![["\"banana\""]]);
    assert_eq!((expansions, scans), (0, 0), "the reuse reads no adjacency and scans nothing");
}

#[tokio::test]
async fn a_reused_relationship_keeps_its_type_and_direction() {
    for (tail, expected) in [
        // Reversed endpoints, another type, and an incoming spelling of the
        // same edge.
        ("MATCH (i)-[r:HAS]->(h) RETURN i.name AS x", vec![]),
        ("MATCH (h)-[r:R]->(i) RETURN i.name AS x", vec![]),
        ("MATCH ()-[r:R]->() RETURN i.name AS x", vec![]),
        ("MATCH (i)<-[r:HAS]-(h) RETURN i.name AS x", vec![vec!["\"banana\"".to_string()]]),
        ("MATCH (x)<-[r]-(y:Hub) RETURN x.name AS x", vec![vec!["\"banana\"".to_string()]]),
        // An OPTIONAL reuse that does not fit keeps the row, its names
        // still bound.
        (
            "OPTIONAL MATCH (i)-[r:HAS]->(h) RETURN i.name AS x",
            vec![vec!["\"banana\"".to_string()]],
        ),
        ("OPTIONAL MATCH (x)-[r:R]->(y) RETURN x.name AS x", vec![vec!["null".to_string()]]),
    ] {
        let query = format!("{BANANA}{tail}");
        assert!(explain(&query).contains("BoundRelationshipExec(relationship=r"), "{query}");
        let storage = graph();
        let (rows, expansions, scans) = rows_and_extra_reads(&storage, BANANA, &query).await;
        assert_eq!(rows, expected, "{tail}");
        assert_eq!((expansions, scans), (0, 0), "{tail}");
    }
}

#[tokio::test]
async fn a_reused_relationship_binds_the_endpoints_its_pattern_left_free() {
    for (tail, expected) in [
        // Neither end bound: the relationship binds both, with no scan of the
        // first node's label or of the inventory.
        (
            "WITH r MATCH (x)-[r]->(y) RETURN x.name AS x, y.name AS y",
            rows![["\"hub\"", "\"banana\""]],
        ),
        (
            "WITH r MATCH (x:Hub)-[r]->(y:Item) RETURN x.name AS x, y.name AS y",
            rows![["\"hub\"", "\"banana\""]],
        ),
        ("WITH r MATCH (x:Item)-[r]->(y) RETURN x.name AS x, y.name AS y", vec![]),
        ("WITH r MATCH (x)-[r]->(y:Hub) RETURN x.name AS x, y.name AS y", vec![]),
        // Undirected: either way round, so both rows when neither end is
        // bound, and only the one that fits a bound end.
        (
            "WITH r MATCH (x)-[r]-(y) RETURN x.name AS x, y.name AS y ORDER BY x",
            rows![["\"banana\"", "\"hub\""], ["\"hub\"", "\"banana\""]],
        ),
        (
            "MATCH (i)-[r]-(y) RETURN i.name AS x, y.name AS y",
            rows![["\"banana\"", "\"hub\""]],
        ),
        // The path continues from the end the relationship bound.
        (
            "WITH r MATCH (x)-[r]->(y) MATCH (y)<-[s:HAS]-(z) RETURN z.name AS x, y.name AS y",
            rows![["\"hub\"", "\"banana\""]],
        ),
    ] {
        let query = format!("{BANANA}{tail}");
        let plan = explain(&query);
        assert!(plan.contains("BoundRelationshipExec(relationship=r"), "{query}: {plan}");
        let storage = graph();
        let (rows, _, scans) = rows_and_extra_reads(&storage, BANANA, &query).await;
        assert_eq!(rows, expected, "{tail}");
        assert_eq!(scans, 0, "{tail}: the relationship, not a scan, binds the first node");
    }
}

#[tokio::test]
async fn an_undirected_reuse_of_a_self_loop_yields_one_row() {
    let anchor = "MATCH (n:Node {name: 'loop'})-[r:R]->() ";
    let storage = graph();
    let (rows, expansions, _) = rows_and_extra_reads(
        &storage,
        anchor,
        &format!("{anchor}WITH r MATCH (x)-[r]-(y) RETURN x.name AS x, y.name AS y"),
    )
    .await;
    assert_eq!(rows, rows![["\"loop\"", "\"loop\""]]);
    assert_eq!(expansions, 0);
    let (rows, _, _) = rows_and_extra_reads(&storage, anchor, &format!("{anchor}MATCH (n)-[r]->(n) RETURN n.name AS x")).await;
    assert_eq!(rows, rows![["\"loop\""]]);
}

/// A hub's degree does not enter into the reuse: the hop reads none of its
/// sixty relationships, where an expansion read all of them.
#[tokio::test]
async fn reusing_a_hub_relationship_reads_none_of_the_hubs_edges() {
    let mut vertices = vec![VertexRecord::new(1).with_label("Entity")];
    vertices.extend((100..160).map(|id| VertexRecord::new(id).with_label("Entity")));
    let storage = MemoryStorage::with_graph(vertices, (0..60).map(|k| RelationshipRecord::new(1000 + k, 1, 100 + k, "RELATES")));
    let anchor = "MATCH (a:Entity)-[r:RELATES]->(b) WHERE id(b) = 105 ";
    let engine = CypherEngine::new(storage.clone());
    let records = || *storage.expanded_relationship_records.lock().unwrap();
    let query = |tail: &str| format!("{anchor}{tail}");
    let run = |query: String| {
        let engine = &engine;
        async move {
            let prepared = engine
                .prepare(&query, &ParameterMap::new())
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            table(
                &engine
                    .execute_prepared(&prepared)
                    .await
                    .unwrap_or_else(|error| panic!("{query}: {error}")),
            )
        }
    };
    let before = records();
    run(query("RETURN id(b) AS c")).await;
    let anchor_records = records() - before;
    for (tail, expected) in [
        ("MATCH (a)-[r:RELATES]->(c) RETURN id(c) AS c", rows![["105"]]),
        ("MATCH (a)-[r:RELATES]-(c) RETURN id(c) AS c", rows![["105"]]),
        ("MATCH (c)-[r:RELATES]->(a) RETURN id(c) AS c", vec![]),
    ] {
        let before = records();
        assert_eq!(run(query(tail)).await, expected, "{tail}");
        assert_eq!(
            records() - before,
            anchor_records,
            "{tail}: the reuse reads no relationship of the hub"
        );
    }
}

/// The first node's map and WHERE move after a reused relationship only when
/// they cannot fail: one that can keeps the scan, so it still runs, and fails
/// the query, for a row the relationship would reject.
#[tokio::test]
async fn a_fallible_first_node_filter_keeps_its_scan_and_its_error() {
    for tail in [
        "MATCH (x:Hub {score: 1 / (size(x.name) - 3)})-[r:R]->(y) RETURN x.name AS x",
        "MATCH (x:Hub WHERE 1 / (size(x.name) - 3) = 1)-[r:R]->(y) RETURN x.name AS x",
    ] {
        let query = format!("{BANANA}{tail}");
        let plan = explain(&query);
        assert!(plan.contains("binding=x, labels=[Hub]"), "the Hub scan stays: {plan}");
        assert!(
            plan.contains("BoundRelationshipExec(relationship=r, from=x, source_labels=[]"),
            "{plan}"
        );
        let engine = CypherEngine::new(graph());
        let prepared = engine.prepare(&query, &ParameterMap::new()).expect("prepare");
        let error = engine
            .execute_prepared(&prepared)
            .await
            .expect_err("the written order divides by zero before the hop rejects r");
        assert!(error.to_string().to_lowercase().contains("zero"), "{tail}: {error}");
    }
}
