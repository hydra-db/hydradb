//! GQL's explicit GROUP BY, as Neo4j accepts it under CYPHER 25.
//!
//! Each expectation below was probed on Neo4j 2026.09; the matrix row
//! `compat_group_by` holds the shared answer to Neo4j.

mod common;

use common::compat::{columns, refusal, run, table};

#[tokio::test]
async fn group_by_an_alias_forms_the_implicit_groups() {
    let result = run("MATCH (i:Item) RETURN i.kind AS kind, count(*) AS total GROUP BY kind ORDER BY kind ASC").await;
    assert_eq!(table(&result), rows![["\"fruit\"", "4"], ["\"veg\"", "2"]]);
}

#[tokio::test]
async fn group_by_the_expression_itself_is_the_same_grouping() {
    let result = run("MATCH (i:Item) RETURN i.kind AS kind, count(*) AS total GROUP BY i.kind ORDER BY kind ASC").await;
    assert_eq!(table(&result), rows![["\"fruit\"", "4"], ["\"veg\"", "2"]]);
}

#[tokio::test]
async fn a_key_need_not_be_returned() {
    let result = run("UNWIND [1, 1, 2] AS x RETURN count(*) AS c GROUP BY x ORDER BY c DESC").await;
    assert_eq!(columns(&result), ["c"]);
    assert_eq!(table(&result), rows![["2"], ["1"]]);
}

#[tokio::test]
async fn an_item_computed_from_a_key_is_allowed() {
    let result = run("UNWIND [1, 1, 2] AS x RETURN x AS k, x * 2 AS d, count(*) AS c GROUP BY k ORDER BY k ASC").await;
    assert_eq!(table(&result), rows![["1", "2", "2"], ["2", "4", "1"]]);
}

#[tokio::test]
async fn grouping_without_an_aggregate_deduplicates() {
    let result = run("UNWIND [1, 1, 2] AS x RETURN x AS k GROUP BY k ORDER BY k ASC").await;
    assert_eq!(table(&result), rows![["1"], ["2"]]);
}

#[tokio::test]
async fn a_hidden_key_without_an_aggregate_still_groups_by_it() {
    // Groups by x, not by the parity it returns: 1 and 3 stay two rows.
    let result = run("UNWIND [1, 3, 3] AS x RETURN x % 2 AS parity GROUP BY x").await;
    assert_eq!(table(&result), rows![["1"], ["1"]]);
}

#[tokio::test]
async fn with_may_group_by_too() {
    let result = run("UNWIND [1, 1, 2] AS x WITH x AS k, count(*) AS c GROUP BY k RETURN k, c ORDER BY k ASC").await;
    assert_eq!(table(&result), rows![["1", "2"], ["2", "1"]]);
}

#[tokio::test]
async fn an_item_not_derivable_from_the_keys_is_refused() {
    assert!(refusal("UNWIND [1, 1, 2] AS x RETURN x AS k, count(*) AS c GROUP BY x + 1")
        .await
        .contains("neither a GROUP BY key nor computed from one"));
}

#[tokio::test]
async fn hidden_group_key_survives_until_ordering() {
    assert_eq!(
        table(&run("UNWIND [2,2,1] AS x RETURN count(*) AS c GROUP BY x ORDER BY x").await),
        rows![["1"], ["2"]]
    );
    assert_eq!(
        table(&run("UNWIND [2,2,1] AS x WITH count(*) AS c GROUP BY x ORDER BY x RETURN c").await),
        rows![["1"], ["2"]]
    );
}

#[tokio::test]
async fn generated_group_names_do_not_collide_with_user_names() {
    assert_eq!(table(&run("UNWIND [1,1,2] AS __hydradb_group_0 RETURN count(*) AS __hydradb_group_1 GROUP BY __hydradb_group_0 ORDER BY __hydradb_group_0").await), rows![["2"], ["1"]]);
}

#[tokio::test]
async fn constant_explicit_group_has_no_group_on_empty_input() {
    assert!(table(&run("MATCH (n:Missing) RETURN count(*) AS n GROUP BY 42").await).is_empty());
    assert!(table(&run("MATCH (n:Missing) RETURN 42 AS k, count(*) AS n GROUP BY k").await).is_empty());
    assert_eq!(
        table(&run("UNWIND [1,2] AS x RETURN count(*) AS n GROUP BY 42").await),
        rows![["2"]]
    );
}
