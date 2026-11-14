//! `r.id` on a relationship reads the relationship's `id` property -- the
//! identity the client wrote with `MERGE (s)-[r:T {id: $h}]->(d)` -- not the
//! internal relationship counter. A vertex's `n.id` is still its identity,
//! because a vertex's identity is the client's `id`.
//!
//! The fixture gives each relationship an internal identity and a different
//! client `id`, so every assertion can tell the two apart.

mod common;

use common::MemoryStorage;
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryValue, RelationshipRecord, ScalarValue, VertexRecord};

/// Internal relationship identities 7 and 8; client ids 9001 and 9002;
/// weights 10 and 20.
fn storage() -> MemoryStorage {
    MemoryStorage::with_graph(
        [
            VertexRecord::new(1)
                .with_label("Entity")
                .with_property("entity_id", ScalarValue::String("a".into())),
            VertexRecord::new(2)
                .with_label("Entity")
                .with_property("entity_id", ScalarValue::String("b".into())),
            VertexRecord::new(3)
                .with_label("Entity")
                .with_property("entity_id", ScalarValue::String("c".into())),
        ],
        [
            RelationshipRecord::new(7, 1, 2, "RELATES")
                .with_property("id", ScalarValue::Integer(9001))
                .with_property("weight", ScalarValue::Integer(10)),
            RelationshipRecord::new(8, 1, 3, "RELATES")
                .with_property("id", ScalarValue::Integer(9002))
                .with_property("weight", ScalarValue::Integer(20)),
        ],
    )
}

async fn values(query: &str, parameters: &ParameterMap) -> Vec<Vec<QueryValue>> {
    CypherEngine::new(storage())
        .execute(query, parameters)
        .await
        .unwrap_or_else(|error| panic!("execute {query}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn client_id() -> ParameterMap {
    ParameterMap::from([("h".to_string(), ScalarValue::Integer(9001))])
}

fn integer(value: i128) -> QueryValue {
    QueryValue::Scalar(ScalarValue::Integer(value))
}

#[tokio::test]
async fn an_inline_relationship_id_matches_the_client_id() {
    for query in [
        // Anchored start: filter on the expanded relationship.
        "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES {id: $h}]->(o) RETURN o.entity_id AS o",
        // Bare start: the relationship-property seek.
        "MATCH (s)-[r:RELATES {id: $h}]->(o) RETURN o.entity_id AS o",
        // No variable: the lowering names the relationship itself.
        "MATCH ()-[:RELATES {id: $h}]->(o) RETURN o.entity_id AS o",
    ] {
        assert_eq!(
            values(query, &client_id()).await,
            vec![vec![QueryValue::Scalar(ScalarValue::String("b".into()))]],
            "{query}"
        );
    }
    // The internal identity is not what `{id: ...}` means.
    let internal = ParameterMap::from([("h".to_string(), ScalarValue::Integer(7))]);
    assert!(values("MATCH (s)-[r:RELATES {id: $h}]->(o) RETURN o.entity_id AS o", &internal)
        .await
        .is_empty());
}

#[tokio::test]
async fn a_where_on_relationship_id_compares_the_client_id() {
    assert_eq!(
        values(
            "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WHERE r.id = $h RETURN count(*) AS n",
            &client_id()
        )
        .await,
        vec![vec![QueryValue::Count(1)]]
    );
    // An element predicate is the same predicate.
    assert_eq!(
        values(
            "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES WHERE r.id = $h]->(o) RETURN o.entity_id AS o",
            &client_id()
        )
        .await,
        vec![vec![QueryValue::Scalar(ScalarValue::String("b".into()))]]
    );
}

#[tokio::test]
async fn returning_relationship_id_returns_the_client_id() {
    assert_eq!(
        values(
            "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) RETURN r.id AS id ORDER BY id",
            &ParameterMap::new()
        )
        .await,
        vec![vec![integer(9001)], vec![integer(9002)]]
    );
    // Carried through WITH, renamed or not, it is still the relationship.
    assert_eq!(
        values(
            "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WITH r AS rel, o \
             WHERE rel.id > 9001 RETURN rel.id AS id, o.entity_id AS o",
            &ParameterMap::new()
        )
        .await,
        vec![vec![integer(9002), QueryValue::Scalar(ScalarValue::String("c".into()))]]
    );
    // A vertex's `id` is still its identity, and the relationship itself
    // still renders as its internal identity.
    assert_eq!(
        values(
            "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES {id: $h}]->(o) RETURN o.id AS o, r AS r",
            &client_id()
        )
        .await,
        vec![vec![QueryValue::VertexId(2), QueryValue::RelationshipId(7)]]
    );
}

/// The shape clients use to find one relationship by its client id, with
/// nothing known about its endpoints, plans a relationship-property seek on
/// `id` rather than expanding every relationship of the type.
#[test]
fn a_bare_start_relationship_id_plans_a_relationship_property_seek() {
    let engine = CypherEngine::new(storage());
    for query in [
        "MATCH ()-[r:RELATES {id: $h}]->() RETURN count(r) AS n",
        "MATCH (s)-[r:RELATES]->(d) WHERE r.id = $h RETURN s.id AS s, d.id AS d",
    ] {
        let explain = engine
            .prepare(query, &client_id())
            .unwrap_or_else(|error| panic!("prepare {query}: {error}"))
            .explain();
        assert!(
            explain.contains("RelationshipPropertySeek(") && explain.contains("property=id"),
            "{query}\n{explain}"
        );
        assert!(!explain.contains("ExpandExec("), "{query}\n{explain}");
    }
}

/// Every way a relationship reaches a new name other than a bare `WITH r AS
/// rel`, each producing one row per relationship of `a` bound to `rel`.
const RELATIONSHIP_ALIASES: [&str; 8] = [
    "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) UNWIND [r] AS rel",
    "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WITH collect(r) AS rs UNWIND rs AS rel",
    "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WITH [r] AS rs UNWIND rs AS rel",
    "MATCH (e:Entity {entity_id: 'a'})-[rs:RELATES*1..1]->(o) UNWIND rs AS rel",
    "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WITH coalesce(r, null) AS rel",
    "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WITH CASE WHEN r IS NULL THEN null ELSE r END AS rel",
    "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WITH o, min(r) AS rel",
    "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES]->(o) WITH o, max(r) AS rel",
];

/// However a relationship got its name, `.id` reads the client id and every
/// other property reads the relationship record, exactly as `r.id` and
/// `r.weight` do on the name the pattern bound.
#[tokio::test]
async fn an_aliased_relationship_reads_the_client_id_and_its_properties() {
    let mut cases = Vec::new();
    for alias in RELATIONSHIP_ALIASES {
        cases.push((
            format!("{alias} RETURN rel.id AS id, rel.weight AS weight ORDER BY id"),
            vec![vec![integer(9001), integer(10)], vec![integer(9002), integer(20)]],
        ));
        for (predicate, expected) in [("rel.id = 9001", 1), ("rel.id = 7", 0), ("rel.weight = 20", 1)] {
            cases.push((
                format!("{alias} WITH rel WHERE {predicate} RETURN count(*) AS n"),
                vec![vec![QueryValue::Count(expected)]],
            ));
        }
        // Projected under yet another name, the id is a value, not the
        // relationship.
        cases.push((
            format!("{alias} WITH rel.id AS id RETURN id ORDER BY id"),
            vec![vec![integer(9001)], vec![integer(9002)]],
        ));
    }
    let mut wrong = Vec::new();
    for (query, expected) in cases {
        let actual = values(&query, &ParameterMap::new()).await;
        if actual != expected {
            wrong.push(format!("{query}\n  got {actual:?}\n  want {expected:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The pattern's own name and the alias answer the same `id`.
#[tokio::test]
async fn an_unwound_relationship_and_its_source_agree_on_id() {
    assert_eq!(
        values(
            "MATCH (e:Entity {entity_id: 'a'})-[r:RELATES {id: $h}]->(o) UNWIND [r] AS rel RETURN r.id AS r, rel.id AS rel",
            &client_id()
        )
        .await,
        vec![vec![integer(9001), integer(9001)]]
    );
}
