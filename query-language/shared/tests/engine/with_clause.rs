//! `WITH` as a scope and projection boundary.
//!
//! These are positive correctness tests, not
//! differential ones, and that distinction is the point: legacy supports
//! exactly one form of `WITH` - bare identifiers covering every in-scope
//! binding - and treats even that as a no-op contributing nothing to the plan.
//! Every shape below except the pass-through one is refused by legacy, so
//! there is no legacy behaviour to be identical to. The ticket's definition of
//! done allows exactly this, when the new engine intentionally adds support
//! and carries a positive correctness test for it.

mod common;

use common::{entity, MemoryStorage};
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryResult, QueryValue, ScalarValue};

fn engine() -> CypherEngine<MemoryStorage> {
    CypherEngine::new(MemoryStorage::with_vertices([
        entity(1, "Entity", "alpha"),
        entity(2, "Entity", "beta"),
        entity(3, "Entity", "gamma"),
    ]))
}

async fn run(query: &str) -> QueryResult {
    engine()
        .execute(query, &ParameterMap::new())
        .await
        .unwrap_or_else(|error| panic!("{query:?} failed: {error}"))
}

fn column(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            QueryValue::Scalar(ScalarValue::String(value)) => value.to_string(),
            QueryValue::Scalar(ScalarValue::Integer(value)) => value.to_string(),
            QueryValue::Count(value) => value.to_string(),
            QueryValue::Null => "null".to_string(),
            other => panic!("unexpected value {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn a_pass_through_with_still_returns_what_it_always_did() {
    let result = run("MATCH (e:Entity) WITH e RETURN e.entity_id AS id ORDER BY id ASC").await;
    assert_eq!(column(&result), ["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn with_aliases_a_computed_value_the_rest_of_the_query_can_read() {
    let result = run("MATCH (e:Entity) WITH e.entity_id AS id RETURN id ORDER BY id ASC").await;
    assert_eq!(column(&result), ["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn with_renames_an_entity_without_losing_its_properties() {
    let result = run("MATCH (e:Entity) WITH e AS renamed RETURN renamed.entity_id AS id ORDER BY id ASC").await;
    assert_eq!(column(&result), ["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn where_after_with_filters_the_projected_scope() {
    let result = run("MATCH (e:Entity) WITH e.entity_id AS id WHERE id = 'beta' RETURN id").await;
    assert_eq!(column(&result), ["beta"]);
}

#[tokio::test]
async fn order_by_and_limit_after_with_window_the_projected_scope() {
    let result = run("MATCH (e:Entity) WITH e.entity_id AS id ORDER BY id DESC LIMIT 2 RETURN id").await;
    assert_eq!(column(&result), ["gamma", "beta"]);
}

#[tokio::test]
async fn skip_after_with_starts_the_window_where_it_says() {
    let result = run("MATCH (e:Entity) WITH e.entity_id AS id ORDER BY id ASC SKIP 1 RETURN id").await;
    assert_eq!(column(&result), ["beta", "gamma"]);
}

#[tokio::test]
async fn distinct_after_with_collapses_duplicates_in_the_new_scope() {
    // Every row projects the same label, so DISTINCT leaves one.
    let result = run("MATCH (e:Entity) WITH DISTINCT 'same' AS tag RETURN tag").await;
    assert_eq!(column(&result), ["same"]);
}

#[tokio::test]
async fn with_chains_so_a_later_segment_sees_only_the_earlier_projection() {
    let result = run("MATCH (e:Entity) WITH e.entity_id AS id WITH id AS carried ORDER BY carried ASC \
         RETURN carried")
    .await;
    assert_eq!(column(&result), ["alpha", "beta", "gamma"]);
}

/// A binding a `WITH` dropped is gone, and saying so beats returning nulls.
#[tokio::test]
async fn referencing_a_dropped_binding_is_refused_by_name() {
    let error = engine()
        .execute(
            "MATCH (e:Entity) WITH e.entity_id AS id RETURN e.entity_id AS gone",
            &ParameterMap::new(),
        )
        .await
        .expect_err("a dropped binding is out of scope");
    assert!(error.to_string().contains("out-of-scope binding e"), "unexpected refusal: {error}");
}

#[tokio::test]
async fn the_forms_that_remain_unsupported_still_name_themselves() {
    let cases = [(
        "MATCH (e:Entity) WITH e.entity_id RETURN e.entity_id AS id",
        "must be given a name with AS",
    )];
    for (query, expected) in cases {
        let error = engine()
            .execute(query, &ParameterMap::new())
            .await
            .expect_err(&format!("{query:?} is not supported"));
        assert!(
            error.to_string().contains(expected),
            "{query:?} was refused as {error}, which does not mention {expected:?}"
        );
    }
}
