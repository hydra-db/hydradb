use async_trait::async_trait;
use hydradb_cypher_engine::{
    scalar_values_equal, CypherEngine, ExpandDirection, ExpandRequest, GraphRead, GraphStorage,
    OrderedPropertyScanPage, OrderedPropertyScanRequest, ParameterMap, QueryValue, ReadRequest,
    RelationshipRecord, ScalarValue, StorageResult, VertexId, VertexRecord,
};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

#[derive(Clone)]
struct MemoryGraph;

fn vertices() -> Vec<VertexRecord> {
    vec![
        VertexRecord::new(1)
            .with_label("Person")
            .with_property("name", ScalarValue::String("Alice".into())),
        VertexRecord::new(2)
            .with_label("Person")
            .with_property("name", ScalarValue::String("Bob".into())),
    ]
}

#[async_trait]
impl GraphStorage for MemoryGraph {
    async fn begin_read(&self, _: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>> {
        Ok(Box::new(self.clone()))
    }
}

#[async_trait]
impl GraphRead for MemoryGraph {
    async fn seek_vertices_by_property(
        &mut self,
        key: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<VertexId>> {
        Ok(vertices()
            .iter()
            .filter(|v| {
                v.properties
                    .get(key)
                    .is_some_and(|x| scalar_values_equal(x, value))
            })
            .map(|v| v.id)
            .collect())
    }
    async fn scan_vertices_by_property(&mut self, key: &str) -> StorageResult<Vec<VertexId>> {
        Ok(vertices()
            .iter()
            .filter(|v| v.properties.contains_key(key))
            .map(|v| v.id)
            .collect())
    }
    async fn scan_vertices_by_property_ordered(
        &mut self,
        _: &OrderedPropertyScanRequest,
    ) -> StorageResult<OrderedPropertyScanPage> {
        panic!("not used")
    }
    async fn scan_vertices_by_label(
        &mut self,
        label: &str,
        max: Option<usize>,
    ) -> StorageResult<Vec<VertexId>> {
        Ok(vertices()
            .iter()
            .filter(|v| v.labels.contains(label))
            .take(max.unwrap_or(usize::MAX))
            .map(|v| v.id)
            .collect())
    }
    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>> {
        Ok(vec![1, 2])
    }
    async fn seek_relationships_by_property(
        &mut self,
        _: &str,
        _: &str,
        _: &ScalarValue,
    ) -> StorageResult<Vec<RelationshipRecord>> {
        Ok(vec![])
    }
    async fn expand_relationships(
        &mut self,
        request: &ExpandRequest,
    ) -> StorageResult<Vec<RelationshipRecord>> {
        // Vertex and relationship IDs may overlap; their records stay distinct.
        let edge = RelationshipRecord::new(1, 1, 2, "KNOWS")
            .with_property("name", ScalarValue::String("friend".into()));
        let input = match request.direction {
            ExpandDirection::Outgoing => edge.source,
            ExpandDirection::Incoming => edge.target,
        };
        Ok(
            if request.input_vertex_ids.contains(&input)
                && (request.relationship_types.is_empty()
                    || request.relationship_types.iter().any(|s| s == "KNOWS"))
            {
                vec![edge]
            } else {
                vec![]
            },
        )
    }
    async fn hydrate_vertices(&mut self, ids: &[VertexId]) -> StorageResult<Vec<VertexRecord>> {
        Ok(vertices()
            .into_iter()
            .filter(|v| ids.contains(&v.id))
            .collect())
    }
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}
fn run<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn check(query: &str, expected: Vec<QueryValue>) {
    let result = run(CypherEngine::new(MemoryGraph).execute(query, &ParameterMap::new()));
    let result = result.unwrap_or_else(|error| panic!("{query}: {error}"));
    let actual = result
        .rows
        .into_iter()
        .map(|row| row.values[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}
fn alice_bob() -> Vec<QueryValue> {
    ["Alice", "Bob"]
        .into_iter()
        .map(|s| QueryValue::Scalar(ScalarValue::String(s.into())))
        .collect()
}

#[test]
fn direct_alias_control() {
    check(
        "MATCH (n:Person) WITH n AS person RETURN person.name ORDER BY person.name",
        alice_bob(),
    );
}
#[test]
fn coalesce_vertex_properties() {
    check(
        "MATCH (n:Person) WITH coalesce(n, null) AS person RETURN person.name ORDER BY person.name",
        alice_bob(),
    );
}
#[test]
fn case_vertex_properties() {
    check("MATCH (n:Person) WITH CASE WHEN true THEN n ELSE null END AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn collect_unwind_vertex_properties() {
    check("MATCH (n:Person) WITH collect(n) AS people UNWIND people AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn literal_list_unwind_vertex_properties() {
    check("MATCH (n:Person) WITH [n] AS people UNWIND people AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn minimum_vertex_properties() {
    check(
        "MATCH (n:Person) WITH min(n) AS person RETURN person.name",
        vec![alice_bob().remove(0)],
    );
}

#[test]
fn direct_alias_identity_control() {
    check(
        "MATCH (n:Person) WITH n AS person RETURN person.id ORDER BY person.id",
        vec![QueryValue::VertexId(1), QueryValue::VertexId(2)],
    );
}
#[test]
fn coalesce_vertex_identity() {
    check(
        "MATCH (n:Person) WITH coalesce(n, null) AS person RETURN person.id ORDER BY person.id",
        vec![QueryValue::VertexId(1), QueryValue::VertexId(2)],
    );
}
#[test]
fn coalesce_property_filter() {
    check("MATCH (n:Person) WITH coalesce(n, null) AS person WHERE person.name = 'Alice' RETURN person.name", vec![alice_bob().remove(0)]);
}
#[test]
fn coalesce_missing_property_filter() {
    check("MATCH (n:Person) WITH coalesce(n, null) AS person WHERE person.name IS NULL RETURN 1 AS wrong", vec![]);
}
#[test]
fn coalesce_null_first() {
    check(
        "MATCH (n:Person) WITH coalesce(null, n) AS person RETURN person.name ORDER BY person.name",
        alice_bob(),
    );
}
#[test]
fn case_simple_vertex() {
    check("MATCH (n:Person) WITH CASE 1 WHEN 1 THEN n ELSE null END AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn coalesce_distinct_vertex() {
    check("MATCH (n:Person) WITH DISTINCT coalesce(n, null) AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn computed_vertex_two_scope_boundaries() {
    check("MATCH (n:Person) WITH coalesce(n, null) AS first WITH first AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn coalesce_scalar_control() {
    check(
        "MATCH (n:Person) WITH coalesce(n.name, 'missing') AS name RETURN name ORDER BY name",
        alice_bob(),
    );
}
#[test]
fn unwind_scalar_list_control() {
    check("MATCH (n:Person) WITH collect(n.name) AS names UNWIND names AS name RETURN name ORDER BY name", alice_bob());
}
#[test]
fn coalesce_null_control() {
    check(
        "WITH coalesce(null, null) AS person RETURN person.name",
        vec![QueryValue::Null],
    );
}
#[test]
fn direct_projection_vertex_control() {
    check(
        "MATCH (n:Person) WITH coalesce(n, null) AS person RETURN person ORDER BY person",
        vec![QueryValue::VertexId(1), QueryValue::VertexId(2)],
    );
}

#[test]
fn coalesce_relationship_control() {
    check(
        "MATCH (a:Person)-[r:KNOWS]->(b) WITH coalesce(r, null) AS edge RETURN edge.name",
        vec![QueryValue::Scalar(ScalarValue::String("friend".into()))],
    );
}
#[test]
fn collect_unwind_relationship_control() {
    check("MATCH (a:Person)-[r:KNOWS]->(b) WITH collect(r) AS edges UNWIND edges AS edge RETURN edge.name", vec![QueryValue::Scalar(ScalarValue::String("friend".into()))]);
}
#[test]
fn coalesce_vertex_with_original_still_in_scope() {
    check("MATCH (n:Person) WITH n, coalesce(n, null) AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn maximum_vertex_properties() {
    check(
        "MATCH (n:Person) WITH max(n) AS person RETURN person.name",
        vec![alice_bob().remove(1)],
    );
}
#[test]
fn collect_distinct_unwind_vertex_properties() {
    check("MATCH (n:Person) WITH collect(DISTINCT n) AS people UNWIND people AS person RETURN person.name ORDER BY person.name", alice_bob());
}
#[test]
fn computed_null_property_control() {
    check(
        "MATCH (n:Person) WITH CASE WHEN false THEN n ELSE null END AS person RETURN person.name",
        vec![QueryValue::Null, QueryValue::Null],
    );
}

#[test]
fn computed_vertex_count_identity() {
    check(
        "MATCH (n:Person) WITH coalesce(n, null) AS person RETURN count(person.id)",
        vec![QueryValue::Count(2)],
    );
}
#[test]
fn computed_vertex_count_property() {
    check(
        "MATCH (n:Person) WITH coalesce(n, null) AS person RETURN count(person.name)",
        vec![QueryValue::Count(2)],
    );
}
#[test]
fn empty_collect_unwind_control() {
    check(
        "MATCH (n:Missing) WITH collect(n) AS people UNWIND people AS person RETURN person.name",
        vec![],
    );
}
#[test]
fn null_fallback_to_vertex_from_optional() {
    check("MATCH (n:Person) OPTIONAL MATCH (n)-[:ABSENT]->(missing) WITH coalesce(missing, n) AS person RETURN person.name ORDER BY person.name", alice_bob());
}

#[test]
fn nested_collected_nodes_survive_aliases_and_join() {
    check(
        "MATCH (n:Person) WITH [collect(n)] AS groups WITH groups AS renamed MATCH (other:Person) UNWIND renamed AS people UNWIND people AS person RETURN person.name ORDER BY person.name",
        vec![alice_bob()[0].clone(), alice_bob()[0].clone(), alice_bob()[1].clone(), alice_bob()[1].clone()],
    );
}

#[test]
fn mixed_node_relationship_and_null_list() {
    check(
        "MATCH (a:Person)-[r:KNOWS]->(b) WITH [a, r, b, null] AS entities UNWIND entities AS entity RETURN entity.name",
        vec![alice_bob()[0].clone(), QueryValue::Scalar(ScalarValue::String("friend".into())), alice_bob()[1].clone(), QueryValue::Null],
    );
}

#[test]
fn computed_node_pattern_rebinding_remains_explicitly_unsupported() {
    let result = run(CypherEngine::new(MemoryGraph).execute(
        "MATCH (n:Person) WITH coalesce(n, null) AS person MATCH (person)-[:KNOWS]->(friend) RETURN friend.name",
        &ParameterMap::new(),
    ));
    assert!(result.unwrap_err().to_string().contains(
        "person holds a value projected by WITH and cannot also bind a node or relationship"
    ));
}

#[test]
fn repeated_nodes_keep_distinct_identity() {
    check(
        "MATCH (n:Person) WITH [n, n] AS people UNWIND people AS person WITH DISTINCT coalesce(person, null) AS selected WITH collect(selected) AS people UNWIND people AS person RETURN person.name ORDER BY person.name",
        alice_bob(),
    );
}

#[test]
fn computed_node_grouping_key_keeps_its_record() {
    check(
        "MATCH (n:Person) WITH coalesce(n, null) AS person, count(*) AS total RETURN person.name ORDER BY person.name",
        alice_bob(),
    );
}

#[test]
fn scalar_id_does_not_pick_up_a_node_record() {
    check(
        "MATCH (n:Person) WITH n, 1 AS person RETURN person.name",
        vec![QueryValue::Null, QueryValue::Null],
    );
}

#[test]
fn computed_nested_list_alias_keeps_node_records() {
    check(
        "MATCH (n:Person) WITH [collect(n)] AS groups WITH CASE WHEN true THEN groups ELSE [] END AS selected UNWIND selected AS people UNWIND people AS person RETURN person.name ORDER BY person.name",
        alice_bob(),
    );
}
