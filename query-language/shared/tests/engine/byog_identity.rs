//! User properties and internal identities must stay distinct across every access form.
mod common;
use common::{compat::table, MemoryStorage};
use hydradb_cypher_engine::{
    CypherEngine, ListParameterMap, LoweredTemplate, ParameterMap, RelationshipRecord, ScalarValue, UnknownStatistics, VertexRecord,
};

fn graph() -> MemoryStorage {
    MemoryStorage::with_graph(
        [
            VertexRecord::new(0)
                .with_label("Item")
                .with_property("id", ScalarValue::String("user-a".into())),
            VertexRecord::new(1)
                .with_label("Item")
                .with_property("id", ScalarValue::Integer(900)),
            VertexRecord::new(2).with_label("Item"),
        ],
        [
            RelationshipRecord::new(0, 0, 1, "LINK").with_property("id", ScalarValue::String("edge-user".into())),
            RelationshipRecord::new(1, 0, 1, "LINK"),
            RelationshipRecord::new(2, 2, 2, "LINK").with_property("id", ScalarValue::Integer(700)),
        ],
    )
}
async fn run(query: &str, byog: bool) -> Vec<Vec<String>> {
    let (template, _) = LoweredTemplate::lower_timed_with_id_properties(query, byog).unwrap_or_else(|e| panic!("{query}: {e}"));
    let prepared = template
        .bind(&ParameterMap::new(), &ListParameterMap::new())
        .plan_with_statistics(&UnknownStatistics)
        .unwrap();
    let result = CypherEngine::new(graph())
        .execute_prepared(&prepared)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    table(&result)
}
#[tokio::test]
async fn byog_identity_nodes_and_relationships_are_not_user_properties() {
    assert_eq!(
        run("MATCH (n:Item) RETURN id(n), n.id ORDER BY id(n)", true).await,
        rows![["0", "\"user-a\""], ["1", "900"], ["2", "null"]]
    );
    assert_eq!(
        run("MATCH (a)-[r:LINK]->(b) RETURN id(r), r.id, id(a), id(b) ORDER BY id(r)", true).await,
        rows![["0", "\"edge-user\"", "0", "1"], ["1", "null", "0", "1"], ["2", "700", "2", "2"]]
    );
}
#[tokio::test]
async fn byog_identity_filters_and_pattern_properties_use_stored_values() {
    for query in [
        "MATCH (n:Item {id:'user-a'}) RETURN id(n)",
        "MATCH (n:Item) WHERE n.id = 'user-a' RETURN id(n)",
        "MATCH (n:Item) WHERE n.id IN ['user-a'] RETURN id(n)",
    ] {
        assert_eq!(run(query, true).await, rows![["0"]]);
    }
    assert_eq!(run("MATCH (n:Item {id:900}) RETURN id(n)", true).await, rows![["1"]]);
    assert!(run("MATCH (n:Item {id:0}) RETURN id(n)", true).await.is_empty());
    assert_eq!(run("MATCH (n:Item) WHERE n.id IS NULL RETURN id(n)", true).await, rows![["2"]]);
    assert_eq!(run("MATCH ()-[r:LINK {id:'edge-user'}]->() RETURN id(r)", true).await, rows![["0"]]);
}
#[tokio::test]
async fn byog_identity_survives_aliases_records_lists_and_subqueries() {
    for query in [
        "MATCH (n:Item) WITH n AS x RETURN id(x), x.id ORDER BY id(x)",
        "MATCH (n:Item) WITH {node:n} AS x RETURN id(x.node), x.node.id ORDER BY id(x.node)",
        "MATCH (n:Item) WITH collect(n) AS ns UNWIND ns AS x RETURN id(x), x.id ORDER BY id(x)",
        "MATCH (n:Item) WHERE EXISTS { MATCH (n) WHERE n.id = 'user-a' } RETURN id(n), n.id",
    ] {
        let expected = if query.contains("EXISTS") {
            rows![["0", "\"user-a\""]]
        } else {
            rows![["0", "\"user-a\""], ["1", "900"], ["2", "null"]]
        };
        assert_eq!(run(query, true).await, expected, "{query}");
    }
    assert_eq!(run("RETURN {id:123}.id, id(null)", true).await, rows![["123", "null"]]);
}
#[tokio::test]
async fn byog_identity_keeps_managed_semantics_and_validates_function_arguments() {
    assert_eq!(
        run("MATCH (n:Item) RETURN n.id ORDER BY n.id", false).await,
        rows![["#0"], ["#1"], ["#2"]]
    );
    assert_eq!(
        run("MATCH ()-[r:LINK]->() RETURN id(r), r.id ORDER BY id(r)", false).await,
        rows![["0", "\"edge-user\""], ["1", "null"], ["2", "700"]]
    );
    for query in ["RETURN id()", "RETURN id(1,2)"] {
        assert!(LoweredTemplate::lower(query).is_err());
    }
    let engine = CypherEngine::new(graph());
    for query in ["RETURN id(42)", "RETURN id('x')", "RETURN id({id:0})"] {
        assert!(engine.execute(query, &ParameterMap::new()).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn byog_identity_optional_grouped_and_computed_properties_preserve_nulls() {
    assert_eq!(
        run("MATCH (n:Item) RETURN PROPERTY_EXISTS(n, id) AS p, id(n) AS i ORDER BY i", true).await,
        rows![["true", "0"], ["true", "1"], ["false", "2"]]
    );
    assert_eq!(
        run("OPTIONAL MATCH (n:Missing) RETURN id(n), n.id, PROPERTY_EXISTS(n,id)", true).await,
        rows![["null", "null", "null"]]
    );
    assert_eq!(
        run("MATCH (n:Item) RETURN collect(n)[0].id, count(*)", true).await,
        rows![["\"user-a\"", "3"]]
    );
    assert_eq!(
        run("MATCH (n:Item) RETURN id([n][0]), [n][0].id ORDER BY id([n][0])", true).await,
        rows![["0", "\"user-a\""], ["1", "900"], ["2", "null"]]
    );
    assert_eq!(
        run(
            "MATCH (n:Item {id:'user-a'}) RETURN n.id AS p UNION ALL MATCH (n:Item {id:900}) RETURN n.id AS p",
            true
        )
        .await,
        rows![["\"user-a\""], ["900"]]
    );
}

#[test]
fn byog_identity_preserves_transport_column_names() {
    for query in [
        "MATCH (n) RETURN id(n), n.id",
        "MATCH (n) WITH {node:n} AS x RETURN id(x.node), x.node.id",
        "MATCH (n) RETURN collect(n)[0].id, count(*)",
        "MATCH (n) RETURN PROPERTY_EXISTS(n,id)",
        "MATCH (n) RETURN coalesce([n][0].id,'none')",
    ] {
        let columns = |byog| {
            LoweredTemplate::lower_timed_with_id_properties(query, byog)
                .unwrap()
                .0
                .bind(&ParameterMap::new(), &ListParameterMap::new())
                .plan_with_statistics(&UnknownStatistics)
                .unwrap()
                .column_names()
                .unwrap()
        };
        assert_eq!(columns(true), columns(false), "{query}");
    }
}
