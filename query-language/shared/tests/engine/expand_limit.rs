//! A `LIMIT` over an expansion -- directly, through Filters, through a
//! one-to-one projection, or on the right of a join -- stops the hop once the
//! window is full instead of reading every relationship first. The rows must
//! be exactly the leading rows of the same query without the window, in the
//! same order.

mod common;

use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryRow, QueryValue, RelationshipRecord, ScalarValue, VertexRecord};

const HUB_DEGREE: u64 = 1_000;

/// A hub (1) with `HUB_DEGREE` outgoing `KNOWS` relationships whose targets
/// alternate between `Person` and `Robot`, written in descending target order
/// so storage order is not output order. Every seventh target gets a second,
/// parallel relationship, and the hub has a self-loop and a reciprocal edge
/// back from target 1001. Hubs 2 and 3 have a few edges of their own.
/// Every relationship stores its identity as `id`, so `r.id` names it.
fn graph() -> MemoryStorage {
    let mut vertices = vec![
        entity(1, "Entity", "hub"),
        entity(2, "Entity", "second"),
        entity(3, "Entity", "third"),
    ];
    let mut relationships = Vec::new();
    let mut next_id = 1_000_000_u64;
    let mut relationship = |source: VertexId, target: VertexId, weight: i128| {
        next_id -= 1;
        RelationshipRecord::new(next_id, source, target, "KNOWS")
            .with_property("id", ScalarValue::Integer(i128::from(next_id)))
            .with_property("weight", ScalarValue::Integer(weight))
    };
    for offset in (0..HUB_DEGREE).rev() {
        let target = 1_000 + offset;
        let label = if offset % 2 == 0 { "Person" } else { "Robot" };
        vertices.push(
            VertexRecord::new(target)
                .with_label(label)
                .with_property("n", ScalarValue::Integer(i128::from(offset % 5))),
        );
        relationships.push(relationship(1, target, i128::from(offset)));
        if offset % 7 == 0 {
            relationships.push(relationship(1, target, i128::from(offset) + 1));
        }
    }
    relationships.push(relationship(1, 1, -1));
    relationships.push(relationship(1_001, 1, -2));
    for target in [1_003, 1_002, 1_500] {
        relationships.push(relationship(2, target, 7));
        relationships.push(relationship(3, target, 8));
    }
    MemoryStorage::with_graph(vertices, relationships)
}

type VertexId = u64;

async fn run(storage: &MemoryStorage, query: &str) -> Vec<QueryRow> {
    let engine = CypherEngine::new(storage.clone());
    engine
        .execute(query, &ParameterMap::new())
        .await
        .unwrap_or_else(|error| panic!("{query:?} failed: {error}"))
        .rows
}

/// Every row of `pattern` names its relationship by a non-null `r` column,
/// and no two rows name the same one -- so a window that swapped or dropped a
/// parallel edge cannot compare equal to the unwindowed rows.
async fn assert_distinct_relationship_ids(storage: &MemoryStorage, pattern: &str) {
    let result = CypherEngine::new(storage.clone())
        .execute(pattern, &ParameterMap::new())
        .await
        .unwrap_or_else(|error| panic!("{pattern:?} failed: {error}"));
    let column = result
        .columns
        .iter()
        .position(|column| column.name == "r")
        .unwrap_or_else(|| panic!("{pattern}: no `r` column"));
    let mut seen = std::collections::HashSet::new();
    for row in &result.rows {
        let id = &row.values[column];
        assert!(
            !matches!(id, QueryValue::Null | QueryValue::Scalar(ScalarValue::Null)),
            "{pattern}: null r.id in {row:?}"
        );
        assert!(seen.insert(format!("{id:?}")), "{pattern}: r.id {id:?} on more than one row");
    }
}

fn records(storage: &MemoryStorage) -> usize {
    *storage.expanded_relationship_records.lock().unwrap()
}

/// The windowed query returns the leading `limit` rows of the unwindowed
/// one, in order, and reads at most `max_records` relationship records to
/// do it. Also holds a backend that cannot list neighbours to the same rows.
async fn assert_window(pattern: &str, limit: usize, max_records: usize) {
    let full_storage = graph();
    let full = run(&full_storage, pattern).await;
    let windowed_storage = graph();
    let query = format!("{pattern} LIMIT {limit}");
    let windowed = run(&windowed_storage, &query).await;
    assert_eq!(windowed, full[..limit.min(full.len())], "{query}");
    let read = records(&windowed_storage);
    assert!(
        read <= max_records,
        "{query}: read {read} relationship records for {} rows (unwindowed read {})",
        windowed.len(),
        records(&full_storage)
    );

    let eager_storage = MemoryStorage {
        without_neighbor_listing: true,
        ..graph()
    };
    assert_eq!(run(&eager_storage, &query).await, windowed, "{query} without neighbour listing");
}

#[tokio::test]
async fn a_limit_over_an_expansion_reads_only_the_window() {
    let pattern = "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) RETURN b.id AS b, r.id AS r, r.weight AS w";
    assert_distinct_relationship_ids(&graph(), pattern).await;
    let full_storage = graph();
    let full = run(&full_storage, pattern).await;
    assert!(full.len() > HUB_DEGREE as usize, "{}", full.len());
    assert!(records(&full_storage) > HUB_DEGREE as usize);
    for limit in [1, 3, 10, 50] {
        // One record per target read, plus every seventh target's parallel
        // one: the window, not the degree.
        assert_window(pattern, limit, limit + limit / 7 + 2).await;
    }
    assert_window(pattern, 5_000, 2 * HUB_DEGREE as usize).await;
}

#[tokio::test]
async fn target_labels_and_filters_count_only_surviving_rows() {
    for pattern in [
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b:Person) RETURN b.id AS b, r.id AS r",
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) WHERE r.weight % 10 = 3 RETURN b.id AS b, r.id AS r",
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b:Robot) WHERE b.n = 3 AND r.weight > 100 RETURN b.id AS b, r.weight AS w",
    ] {
        if pattern.contains("r.id AS r") {
            assert_distinct_relationship_ids(&graph(), pattern).await;
        }
        for limit in [1, 4, 25] {
            // Rejection doubles the batch each round, so the reads stay a
            // small multiple of what the survivors need -- far below the
            // hub's degree.
            assert_window(pattern, limit, HUB_DEGREE as usize / 2).await;
        }
    }
}

#[tokio::test]
async fn a_stacked_filter_still_shields_its_outer_predicate() {
    // The inline map keeps `b.n = 0` out of the outer, fallible division:
    // folded into the hop, the outer predicate must still never see a row
    // the inner one rejected, or it would divide by zero.
    let pattern = "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b {n: 2}) WHERE 10 / (b.n - 2 + 1) > 5 RETURN b.id AS b";
    let engine = CypherEngine::new(graph());
    let explain = engine.prepare(pattern, &ParameterMap::new()).expect("prepare").explain();
    assert!(
        explain.contains("> 5))\n    FilterExec(predicate=(b.n = 2))\n      ExpandExec"),
        "the predicates must stay stacked:\n{explain}"
    );
    assert_window(pattern, 3, HUB_DEGREE as usize / 2).await;
    let pattern = "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) WHERE b.n <> 4 RETURN b.id AS b, b.n AS n";
    assert_window(pattern, 30, HUB_DEGREE as usize / 2).await;
}

#[tokio::test]
async fn undirected_hops_and_many_inputs_keep_the_unwindowed_order() {
    let undirected = "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]-(b) RETURN b.id AS b, r.id AS r";
    assert_distinct_relationship_ids(&graph(), undirected).await;
    assert_window(undirected, 12, 64).await;
    // Three inputs: rows run input by input, so the window can end inside
    // the first and never read the others.
    let pattern = "MATCH (a:Entity)-[r:KNOWS]->(b) RETURN a.id AS a, b.id AS b, r.id AS r";
    assert_distinct_relationship_ids(&graph(), pattern).await;
    for limit in [2, 9, 40] {
        assert_window(pattern, limit, limit + 8).await;
    }
}

#[tokio::test]
async fn the_window_reaches_through_a_projection_and_into_a_join() {
    assert_window(
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) WITH b, r.weight AS w RETURN b.id AS b, w",
        6,
        10,
    )
    .await;
    assert_window(
        "MATCH (a:Entity {entity_id: 'hub'}) OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) RETURN a.id AS a, b.id AS b",
        6,
        // Half the targets are robots, so a second, doubled round runs.
        24,
    )
    .await;
    assert_window(
        "MATCH (a:Entity) OPTIONAL MATCH (a)-[r:KNOWS]->(b:Robot {n: 1}) RETURN a.id AS a, b.id AS b",
        4,
        HUB_DEGREE as usize / 2,
    )
    .await;
}

#[tokio::test]
async fn shapes_that_change_the_row_count_still_read_everything() {
    // DISTINCT, ORDER BY and aggregation need every row before the first is
    // known, so the window must not reach the hop.
    for query in [
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) RETURN DISTINCT b.n AS n LIMIT 2",
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) RETURN b.id AS b ORDER BY r.weight DESC LIMIT 2",
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) RETURN b.n AS n, count(*) AS c LIMIT 2",
    ] {
        let storage = graph();
        let rows = run(&storage, query).await;
        assert_eq!(rows.len(), 2, "{query}");
        assert!(records(&storage) > HUB_DEGREE as usize, "{query}");
    }
}

#[tokio::test]
async fn a_window_fits_a_row_budget_the_whole_hop_would_exceed() {
    let pattern = "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) RETURN b.id AS b";
    let budgeted = || MemoryStorage {
        max_intermediate_rows: Some(100),
        ..graph()
    };
    let engine = CypherEngine::new(budgeted());
    let error = engine
        .execute(pattern, &ParameterMap::new())
        .await
        .expect_err("the whole hop exceeds the row budget");
    assert!(error.to_string().contains("intermediate row limit"), "{error}");

    let storage = budgeted();
    let rows = run(&storage, &format!("{pattern} LIMIT 20")).await;
    assert_eq!(rows.len(), 20);
    assert!(records(&storage) <= 30, "{}", records(&storage));
}

#[tokio::test]
async fn the_profile_still_reports_the_folded_operators() {
    let storage = graph();
    let (result, profile, _) = common::profiling::profile_query(
        storage,
        "MATCH (a:Entity {entity_id: 'hub'})-[r:KNOWS]->(b) WHERE r.weight > 10 RETURN b.id AS b LIMIT 5",
        &ParameterMap::new(),
    )
    .await;
    assert_eq!(result.rows.len(), 5);
    let expand = common::profiling::operator(&profile, "ExpandExec");
    assert_eq!(expand.invocations, 1);
    assert_eq!(expand.rows_out, 5);
    // The first eleven targets fail the predicate, so the batch doubles
    // from 5 to 20 before five rows survive: tens of records, not a thousand.
    assert!(expand.scanned_relationships <= 64, "{expand:?}");
}

/// The leading `limit` rows of `pattern`, in order, against `storage`'s graph,
/// and the relationship records both runs read: `(limited, unlimited)`.
async fn window_against(storage: impl Fn() -> MemoryStorage, pattern: &str, limit: usize) -> (usize, usize) {
    let full_storage = storage();
    let full = run(&full_storage, pattern).await;
    let windowed_storage = storage();
    let query = format!("{pattern} LIMIT {limit}");
    let windowed = run(&windowed_storage, &query).await;
    assert!(full.len() > limit, "{pattern}: only {} rows", full.len());
    assert_eq!(windowed, full[..limit], "{query}");
    (records(&windowed_storage), records(&full_storage))
}

fn probing_graph() -> MemoryStorage {
    let mut storage = graph();
    // A rare type the probe reads whole: hub 2 and hub 3 each mentor a few.
    for (index, (source, target)) in [(2, 1_004), (3, 1_002), (2, 1_000), (3, 1_006), (2, 1_008)].into_iter().enumerate() {
        storage.relationships.push(
            RelationshipRecord::new(10 + index as u64, source, target, "MENTORS")
                .with_property("id", ScalarValue::Integer(10 + index as i128)),
        );
    }
    MemoryStorage {
        answers_relationship_probes: true,
        ..storage
    }
}

#[tokio::test]
async fn a_window_reaches_the_type_probe_and_its_fallback() {
    // `a:Entity` is a bare scan, so both hops plan the type probe.
    let rare = "MATCH (a:Entity)-[r:MENTORS]->(b:Person) WHERE b.n <> 4 RETURN a.id AS a, r.id AS r, b.id AS b";
    let broad = "MATCH (a:Entity)-[r:KNOWS]->(b) WHERE r.weight % 10 = 3 RETURN a.id AS a, r.id AS r, b.id AS b";
    let engine = CypherEngine::new(probing_graph());
    for pattern in [rare, broad] {
        assert_distinct_relationship_ids(&probing_graph(), pattern).await;
        let explain = engine.prepare(pattern, &ParameterMap::new()).expect("prepare").explain();
        assert!(explain.contains("RelationshipTypeProbe("), "{explain}");
    }

    // The rare type answers from the probe: nothing is expanded either way,
    // and the window takes the leading rows of the probe's answer.
    let storage = probing_graph();
    let probes = std::sync::Arc::clone(&storage.relationship_probe_requests);
    let (limited, unlimited) = window_against(probing_graph, rare, 2).await;
    assert_eq!((limited, unlimited), (0, 0));
    run(&storage, &format!("{rare} LIMIT 2")).await;
    assert_eq!(*probes.lock().unwrap(), vec!["type:MENTORS".to_string()]);

    // KNOWS is too broad for the probe, which declines to its label-anchored
    // expansion; the window and the predicate follow it there.
    let (limited, unlimited) = window_against(probing_graph, broad, 5).await;
    assert!(unlimited > HUB_DEGREE as usize, "{unlimited}");
    assert!(limited <= 100, "read {limited} relationship records, unwindowed {unlimited}");
}

/// A root with `HUB_DEGREE` `LINKS` children, each with one child of its own,
/// so a two-hop walk visits every child and every grandchild.
fn tree() -> MemoryStorage {
    let mut vertices = vec![entity(1, "Entity", "root")];
    let mut relationships = Vec::new();
    for child in 0..HUB_DEGREE {
        let (middle, leaf) = (10_000 + child, 20_000 + child);
        for id in [middle, leaf] {
            vertices.push(
                VertexRecord::new(id)
                    .with_label("Node")
                    .with_property("n", ScalarValue::Integer(i128::from(child % 5))),
            );
        }
        relationships.push(RelationshipRecord::new(100_000 + child, 1, middle, "LINKS"));
        relationships.push(RelationshipRecord::new(200_000 + child, middle, leaf, "LINKS"));
    }
    MemoryStorage::with_graph(vertices, relationships)
}

#[tokio::test]
async fn a_window_stops_a_variable_length_walk_inside_a_depth() {
    // Every leaf is two hops out: the root's thousand children are read whole
    // (one input, ordered by relationship identity), and then only the
    // children whose grandchildren the window still needs.
    let (limited, unlimited) = window_against(tree, "MATCH (a:Entity {entity_id: 'root'})-[:LINKS*2..2]->(c) RETURN c.id AS c", 5).await;
    assert!(unlimited >= 3 * HUB_DEGREE as usize, "{unlimited}");
    assert!(limited <= 2 * HUB_DEGREE as usize + 200, "read {limited}, unwindowed {unlimited}");

    // A predicate over the walk's rows is pushed into it: depth one already
    // has enough survivors, so depth two is never read.
    let (limited, unlimited) = window_against(
        tree,
        "MATCH (a:Entity {entity_id: 'root'})-[rs:LINKS*1..2]->(c) WHERE c.n = 3 RETURN c.id AS c, size(rs) AS hops",
        4,
    )
    .await;
    assert!(limited <= 2 * HUB_DEGREE as usize + 2, "read {limited}, unwindowed {unlimited}");
    assert!(limited < unlimited, "read {limited}, unwindowed {unlimited}");

    // Many walks at once: rows run walk by walk, so the window ends early in
    // the frontier and the rest of it is never expanded.
    let (limited, unlimited) = window_against(
        tree,
        "MATCH (m:Node) WHERE m.n = 1 MATCH (m)-[:LINKS*1..1]->(c) RETURN m.id AS m, c.id AS c",
        3,
    )
    .await;
    assert!(limited * 2 < unlimited, "read {limited}, unwindowed {unlimited}");
}
