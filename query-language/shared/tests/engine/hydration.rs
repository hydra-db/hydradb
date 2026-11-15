//! A request hydrates each vertex at most once, however many operators and
//! rows reach it. Proven with the hydrated-vertices counter and the
//! backend's own request log.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::profiling::profile_query;
use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{ParameterMap, RelationshipRecord};

/// Eight people. Everyone knows the three "hubs" (1, 2, 3), and the hubs know
/// each other, so join and expand shapes reach the same few vertices from
/// many rows.
fn graph() -> MemoryStorage {
    let people = (1..=8).map(|id| entity(id, "Person", &format!("p{id}")));
    let mut edges = Vec::new();
    for source in 1..=8_u64 {
        for hub in 1..=3_u64 {
            if source != hub {
                edges.push((source, hub));
            }
        }
    }
    let knows = edges.into_iter().enumerate().map(|(index, (source, target))| RelationshipRecord {
        id: 100 + index as u64,
        source,
        target,
        relationship_type: "KNOWS".to_string(),
        properties: BTreeMap::new(),
    });
    MemoryStorage::with_graph(people, knows)
}

struct Hydration {
    rows: usize,
    counted: u64,
    requested: Vec<u64>,
}

async fn hydration(query: &str) -> Hydration {
    let storage = graph();
    let requests = Arc::clone(&storage.hydration_requests);
    let (result, profile, _) = profile_query(storage, query, &ParameterMap::new()).await;
    let counted = profile.operators.iter().map(|operator| operator.hydrated_vertices).sum();
    let requested = requests.lock().unwrap().iter().flatten().copied().collect();
    Hydration {
        rows: result.rows.len(),
        counted,
        requested,
    }
}

#[tokio::test]
async fn join_and_expand_shapes_hydrate_each_vertex_once_and_fewer_than_their_rows() {
    let shapes = [
        // Cartesian join: the right input runs once per left row and used to
        // hydrate all eight people each time (8 + 8 * 8 = 72 hydrations).
        (
            "join",
            "MATCH (a:Person) MATCH (b:Person) RETURN a.entity_id AS a, b.entity_id AS b",
            64,
        ),
        // Correlated expand: every left row reaches the same three hubs.
        (
            "correlated expand",
            "MATCH (a:Person) MATCH (a)-[:KNOWS]->(b) RETURN a.entity_id AS a, b.entity_id AS b",
            21,
        ),
        // Two hops: the second hop's targets are the first hop's targets.
        (
            "two-hop expand",
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.entity_id AS a, c.entity_id AS c",
            42,
        ),
        // Variable length: every depth re-reaches the hubs.
        (
            "variable-length expand",
            "MATCH (a:Person)-[:KNOWS*1..3]->(c) RETURN a.entity_id AS a, c.entity_id AS c",
            0,
        ),
    ];
    for (name, query, expected_rows) in shapes {
        let hydration = hydration(query).await;
        if expected_rows > 0 {
            assert_eq!(hydration.rows, expected_rows, "{name}");
        }
        let mut distinct = hydration.requested.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            hydration.requested.len(),
            "{name}: a vertex was fetched twice: {:?}",
            hydration.requested
        );
        assert!(distinct.len() <= 8, "{name}");
        assert_eq!(hydration.counted, distinct.len() as u64, "{name}");
        assert!(
            (hydration.counted as usize) < hydration.rows,
            "{name}: {} hydrations for {} rows",
            hydration.counted,
            hydration.rows
        );
    }
}
