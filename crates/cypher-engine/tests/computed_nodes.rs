mod common;

use common::{query, text};
use hydradb_cypher_engine::QueryValue;

#[test]
fn computed_node_aliases_keep_properties_and_identity() {
    for expression in [
        "n",
        "coalesce(n, null)",
        "CASE WHEN n.name = 'Alice' THEN n ELSE n END",
        "CASE n.name WHEN 'Alice' THEN n ELSE n END",
    ] {
        let source = format!(
            "MATCH (n:Person) WITH {expression} AS person \
            RETURN person.id, person.name ORDER BY person.name"
        );
        assert_eq!(
            query(&source),
            vec![
                vec![QueryValue::VertexId(1), text("Alice")],
                vec![QueryValue::VertexId(2), text("Bob")],
            ],
            "{expression}"
        );
    }
}

#[test]
fn computed_nodes_work_in_filters_and_counts() {
    assert_eq!(
        query(
            "MATCH (n:Person) WITH coalesce(n, null) AS person \
        WHERE person.name = 'Alice' RETURN person.name"
        ),
        vec![vec![text("Alice")]]
    );
    assert_eq!(
        query(
            "MATCH (n:Person) WITH coalesce(n, null) AS person \
        RETURN count(person.name)"
        ),
        vec![vec![QueryValue::Count(2)]]
    );
}

#[test]
fn aggregate_nodes_keep_the_selected_record() {
    for (aggregate, name, id) in [("min", "Alice", 1), ("max", "Bob", 2)] {
        assert_eq!(
            query(&format!(
                "MATCH (n:Person) WITH {aggregate}(n) AS person \
            WITH person AS renamed RETURN renamed.name, renamed.id"
            )),
            vec![vec![text(name), QueryValue::VertexId(id)]]
        );
    }
}

#[test]
fn collected_nodes_survive_aliases_and_nested_unwind() {
    for pipeline in [
        "WITH collect(n) AS people UNWIND people AS person",
        "WITH collect(DISTINCT n) AS people WITH people AS renamed UNWIND renamed AS person",
        "WITH [collect(n)] AS groups UNWIND groups AS people UNWIND people AS person",
        "WITH [n, n] AS people WITH DISTINCT people AS renamed UNWIND renamed AS person",
    ] {
        let rows = query(&format!(
            "MATCH (n:Person) {pipeline} \
            RETURN DISTINCT person.name ORDER BY person.name"
        ));
        assert_eq!(
            rows,
            vec![vec![text("Alice")], vec![text("Bob")]],
            "{pipeline}"
        );
    }
}

#[test]
fn null_and_scalar_values_do_not_become_nodes() {
    assert_eq!(
        query(
            "MATCH (n:Person) WITH CASE WHEN n.name = 'Alice' THEN n ELSE null END AS person \
        RETURN person.name ORDER BY person.name"
        ),
        vec![vec![text("Alice")], vec![QueryValue::Null]]
    );
    assert_eq!(
        query("MATCH (n:Person) WITH coalesce(n.name, null) AS name RETURN name ORDER BY name"),
        vec![vec![text("Alice")], vec![text("Bob")]]
    );
}

#[test]
fn node_and_relationship_lists_retain_records_independently() {
    assert_eq!(
        query(
            "MATCH (a:Person)-[r:KNOWS]->(b) WITH [a, r, b] AS entities \
        UNWIND entities AS entity RETURN entity.name"
        ),
        vec![vec![text("Alice")], vec![text("friend")], vec![text("Bob")]]
    );
}

#[test]
fn collected_node_records_survive_a_join() {
    assert_eq!(
        query(
            "MATCH (n:Person) WITH collect(n) AS people MATCH (other:Person) \
            UNWIND people AS person RETURN person.name, other.name \
            ORDER BY person.name, other.name"
        ),
        vec![
            vec![text("Alice"), text("Alice")],
            vec![text("Alice"), text("Bob")],
            vec![text("Bob"), text("Alice")],
            vec![text("Bob"), text("Bob")],
        ]
    );
}

#[test]
fn computed_nodes_keep_distinct_identity_and_can_be_collected_again() {
    assert_eq!(
        query(
            "MATCH (n:Person) WITH [n, n] AS people UNWIND people AS person \
            WITH DISTINCT coalesce(person, null) AS selected \
            WITH collect(selected) AS people UNWIND people AS person \
            RETURN person.name ORDER BY person.name"
        ),
        vec![vec![text("Alice")], vec![text("Bob")]]
    );
}

#[test]
fn computed_list_aliases_keep_nodes_and_null_elements() {
    assert_eq!(
        query(
            "MATCH (n:Person) WITH collect(n) AS people \
            WITH CASE WHEN true THEN people ELSE [] END AS selected \
            UNWIND selected AS person RETURN person.name ORDER BY person.name"
        ),
        vec![vec![text("Alice")], vec![text("Bob")]]
    );
    assert_eq!(
        query(
            "MATCH (n:Person) WITH [n, null] AS people UNWIND people AS person \
            RETURN person.name ORDER BY person.name"
        ),
        vec![
            vec![text("Alice")],
            vec![text("Bob")],
            vec![QueryValue::Null],
            vec![QueryValue::Null]
        ]
    );
}
