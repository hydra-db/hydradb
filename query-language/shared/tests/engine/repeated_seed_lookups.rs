//! Production degree-query shape: incoming/outgoing UNION arms repeat each
//! seed's equality restrictions within one snapshot.
mod common;

use std::collections::BTreeMap;

use common::MemoryStorage;
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryValue, RelationshipRecord, ScalarValue, VertexRecord};

fn fixture(probe: bool) -> MemoryStorage {
    let vertices = (0..11).map(|id| {
        VertexRecord::new(id)
            .with_label("Entity")
            .with_property("name", ScalarValue::String(format!("n{id}").into()))
            .with_property("tenant_id", ScalarValue::String("tenant".into()))
            .with_property("sub_tenant_id", ScalarValue::String("scope".into()))
    });
    let mut edges = (0..10).map(|id| RelationshipRecord::new(id, id, 10, "RELATES")).collect::<Vec<_>>();
    // Parallel relationships and self loops retain their original degree
    // semantics: the loop contributes to both outgoing and incoming counts.
    edges.push(RelationshipRecord::new(20, 0, 10, "RELATES"));
    edges.push(RelationshipRecord::new(21, 0, 0, "RELATES"));
    let mut storage = MemoryStorage::with_graph(vertices, edges);
    if probe {
        storage.selective_properties = vec!["name".into()];
    }
    storage
}

fn query() -> String {
    (0..10)
        .flat_map(|id| {
            ["(e)-[:RELATES]->(other:Entity)", "(e)<-[:RELATES]-(other:Entity)"].map(|hop| {
                format!(
                    "MATCH (e:Entity {{name: 'n{id}', tenant_id: 'tenant', sub_tenant_id: 'scope'}}) \
                     MATCH {hop} WHERE other.tenant_id = 'tenant' AND other.sub_tenant_id = 'scope' \
                     RETURN e.name AS seed_name, count(*) AS degree"
                )
            })
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

async fn assert_degrees(storage: &MemoryStorage) {
    let result = CypherEngine::new(storage.clone())
        .execute(&query(), &ParameterMap::new())
        .await
        .unwrap();
    let mut degrees = BTreeMap::<String, u64>::new();
    for row in result.rows {
        let [QueryValue::Scalar(ScalarValue::String(name)), QueryValue::Count(degree)] = row.values.as_slice() else {
            panic!("unexpected degree row: {row:?}");
        };
        *degrees.entry(name.to_string()).or_default() += degree;
    }
    assert_eq!(degrees.len(), 10);
    assert_eq!(degrees["n0"], 4);
    for id in 1..10 {
        assert_eq!(degrees[&format!("n{id}")], 1);
    }
}

#[tokio::test]
async fn degree_union_reuses_each_selective_probe_once_per_snapshot() {
    let storage = fixture(true);
    assert_degrees(&storage).await;
    assert_eq!(
        storage.probe_requests.lock().unwrap().len(),
        10,
        "twenty arms need only ten seed probes"
    );
    assert!(storage.seek_values.lock().unwrap().is_empty());
    // A new read must not reuse results from the previous snapshot.
    assert_degrees(&storage).await;
    assert_eq!(storage.probe_requests.lock().unwrap().len(), 20);
}

#[tokio::test]
async fn degree_union_reuses_seeks_when_backend_has_no_probe() {
    let storage = fixture(false);
    assert_degrees(&storage).await;
    assert_eq!(storage.seek_values.lock().unwrap().len(), 10);
    assert_eq!(storage.probe_requests.lock().unwrap().len(), 10);
}

#[tokio::test]
async fn cached_empty_probe_stays_empty_without_falling_back_to_a_seek() {
    let mut storage = fixture(true);
    storage.probe_answers_empty = true;
    let result = CypherEngine::new(storage.clone())
        .execute(&query(), &ParameterMap::new())
        .await
        .unwrap();
    assert!(result.rows.is_empty());
    assert_eq!(storage.probe_requests.lock().unwrap().len(), 10);
    assert!(storage.seek_values.lock().unwrap().is_empty());
}
