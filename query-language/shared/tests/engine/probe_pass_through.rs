//! A plan that expands is executed through the hydration identity map, which
//! wraps the backend. Every probe the backend answers must reach it through
//! the map: a probe the map swallowed would decline on every wrapped plan
//! and quietly fall back to the label-wide expansion it exists to avoid.

mod common;

use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{CypherEngine, ParameterMap, RelationshipRecord, ScalarValue};

/// Five people, a rare `MENTORS` type with two relationships, and `KNOWS`
/// relationships timestamped 1..=8 so a range probe has something to bound.
fn graph() -> MemoryStorage {
    let people = (1..=5).map(|id| entity(id, "Person", &format!("p{id}")));
    let mut relationships = vec![
        RelationshipRecord::new(100, 1, 2, "MENTORS"),
        RelationshipRecord::new(101, 3, 4, "MENTORS"),
    ];
    for (index, (source, target)) in [(1, 2), (2, 3), (3, 4), (4, 5), (5, 1), (2, 4), (3, 5), (4, 1)]
        .into_iter()
        .enumerate()
    {
        relationships.push(
            RelationshipRecord::new(200 + index as u64, source, target, "KNOWS")
                .with_property("ts", ScalarValue::Integer(index as i128 + 1)),
        );
    }
    MemoryStorage {
        answers_relationship_probes: true,
        ..MemoryStorage::with_graph(people, relationships)
    }
}

async fn probes_and_rows(query: &str) -> (Vec<String>, String, usize) {
    let storage = graph();
    let probes = std::sync::Arc::clone(&storage.relationship_probe_requests);
    let engine = CypherEngine::new(storage);
    let prepared = engine.prepare(query, &ParameterMap::new()).expect("prepare");
    let rows = engine.execute_prepared(&prepared).await.expect("execute").rows.len();
    let probes = probes.lock().unwrap().clone();
    (probes, prepared.explain(), rows)
}

#[tokio::test]
async fn a_type_probe_under_a_second_hop_reaches_the_backend() {
    // The second hop is what makes the plan reuse hydration, and so be
    // wrapped in the identity map.
    let (probes, explain, rows) =
        probes_and_rows("MATCH (a:Person)-[m:MENTORS]->(b:Person)-[k:KNOWS]->(c:Person) RETURN a.id, b.id, c.id").await;
    assert!(explain.contains("RelationshipTypeProbe("), "{explain}");
    assert!(explain.contains("ExpandExec("), "{explain}");
    assert_eq!(probes, vec!["type:MENTORS".to_string()], "{explain}");
    // 1->2 then 2->{3,4}; 3->4 then 4->{5,1}.
    assert_eq!(rows, 4);
}

#[tokio::test]
async fn a_range_probe_under_a_second_hop_reaches_the_backend() {
    let (probes, explain, rows) = probes_and_rows(
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) WHERE k.ts >= 7 \
         MATCH (b)-[:KNOWS]->(c:Person) RETURN a.id, b.id, c.id",
    )
    .await;
    assert!(explain.contains("RelationshipPropertyRangeProbe("), "{explain}");
    assert!(probes.contains(&"range:KNOWS.ts".to_string()), "{probes:?}\n{explain}");
    // ts 7 is 3->5, ts 8 is 4->1; 5 knows 1, 1 knows 2.
    assert_eq!(rows, 2);
}

/// A probe that answers and one that declines to its expansion return the
/// same rows in the same order. Arrival order is what a sort keeps for a tie
/// between rows binding two relationships, so a probe answering in another
/// order would reorder those ties the moment the map let it through.
#[tokio::test]
async fn an_answered_probe_keeps_the_expansions_order() {
    for query in [
        "MATCH (a:Person)-[m:MENTORS]->(b:Person)-[k:KNOWS]->(c:Person) RETURN a.id, m.id, k.id",
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) WHERE k.ts >= 2 MATCH (b)-[j:KNOWS]->(c:Person) RETURN k.id, j.id",
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) WHERE k.ts >= 2 OPTIONAL MATCH (b)-[m:MENTORS]->(c:Person) \
         RETURN k.id, m.id ORDER BY k.ts > 0",
    ] {
        let mut answered = Vec::new();
        for answers in [true, false] {
            let storage = MemoryStorage {
                answers_relationship_probes: answers,
                ..graph()
            };
            let probes = std::sync::Arc::clone(&storage.relationship_probe_requests);
            let rows = CypherEngine::new(storage)
                .execute(query, &ParameterMap::new())
                .await
                .expect("execute")
                .rows;
            assert_eq!(!probes.lock().unwrap().is_empty(), answers, "{query}");
            answered.push(rows);
        }
        assert_eq!(answered[0], answered[1], "{query}");
    }
}
