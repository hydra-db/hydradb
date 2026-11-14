//! `toUpper`, `toLower`, `size`, `toString` and `toInteger`.
//!
//! Expectations follow Neo4j 2026.09 under CYPHER 25; the matrix rows
//! `compat_string_functions` and `compat_conversion_functions` hold the same
//! answers to Neo4j through `just neo4j-oracle`.

mod common;

use common::compat::{refusal, run, table};

#[tokio::test]
async fn case_mapping_and_length() {
    let result = run("MATCH (i:Item) WHERE i.name IS NOT NULL \
         RETURN toUpper(i.name) AS upper, toLower(i.name) AS lower, size(i.name) AS length \
         ORDER BY upper ASC, length ASC")
    .await;
    assert_eq!(
        table(&result),
        rows![
            ["\"APPLE\"", "\"apple\"", "5"],
            ["\"APPLE\"", "\"apple\"", "5"],
            ["\"BANANA\"", "\"banana\"", "6"],
            ["\"CARROT\"", "\"carrot\"", "6"],
            ["\"DURIAN\"", "\"durian\"", "6"],
        ]
    );
}

#[tokio::test]
async fn unicode_case_mapping_and_character_counting() {
    // Full case mapping (ß upper-cases to SS) and characters rather than
    // bytes or UTF-16 units: what Neo4j returns for the same literals.
    let result =
        run("MATCH (h:Hub) RETURN toUpper('straße') AS upper, toLower('ÀB') AS lower, size('a😀') AS length, size('') AS empty").await;
    assert_eq!(table(&result), rows![["\"STRASSE\"", "\"àb\"", "2", "0"]]);
}

#[tokio::test]
async fn every_function_of_null_is_null() {
    let result = run("MATCH (i:Item {name: 'Durian'}) \
         RETURN toUpper(i.missing) AS a, toLower(i.missing) AS b, size(i.missing) AS c, toString(i.score) AS d, toInteger(i.score) AS e")
    .await;
    assert_eq!(table(&result), rows![["null", "null", "null", "null", "null"]]);
}

#[tokio::test]
async fn to_string_and_back() {
    let result =
        run("MATCH (i:Item) RETURN i.score AS score, toString(i.score) AS text, toInteger(toString(i.score)) AS back ORDER BY score ASC")
            .await;
    assert_eq!(
        table(&result),
        rows![
            ["0", "\"0\"", "0"],
            ["1", "\"1\"", "1"],
            ["2", "\"2\"", "2"],
            ["3", "\"3\"", "3"],
            ["5", "\"5\"", "5"],
            ["null", "null", "null"],
        ]
    );
}

#[tokio::test]
async fn to_string_spells_booleans_and_floats_as_cypher_does() {
    let result = run("MATCH (h:Hub) RETURN toString(true) AS t, toString(1.0) AS one, toString(0.1) AS tenth, toString(100000000.0) AS big, toString('s') AS s").await;
    assert_eq!(table(&result), rows![["\"true\"", "\"1.0\"", "\"0.1\"", "\"1.0E8\"", "\"s\""]]);
}

#[tokio::test]
async fn to_integer_parses_trims_truncates_and_gives_up_with_null() {
    let result = run(
        "MATCH (h:Hub) RETURN toInteger('3.7') AS a, toInteger('x') AS b, toInteger(true) AS c, \
         toInteger(2.9) AS d, toInteger(' 4 ') AS e, toInteger('-12') AS f",
    )
    .await;
    assert_eq!(table(&result), rows![["3", "null", "1", "2", "4", "-12"]]);
}

#[tokio::test]
async fn string_functions_refuse_a_number() {
    assert!(refusal("MATCH (i:Item) RETURN toUpper(i.score) AS x")
        .await
        .contains("toUpper requires a string argument"));
    assert!(refusal("MATCH (i:Item) RETURN size(i.score) AS x")
        .await
        .contains("size requires a string argument"));
}

#[tokio::test]
async fn single_argument_functions_check_their_arity() {
    assert!(refusal("MATCH (i:Item) RETURN toUpper(i.name, i.kind) AS x")
        .await
        .contains("does not take 2 arguments"));
}

#[tokio::test]
async fn functions_filter_and_order() {
    let result =
        run("MATCH (i:Item) WHERE size(coalesce(i.name, '')) = 6 RETURN toLower(i.name) AS name ORDER BY toUpper(i.name) DESC").await;
    assert_eq!(table(&result), rows![["\"durian\""], ["\"carrot\""], ["\"banana\""]]);
}

#[tokio::test]
async fn integer_string_overflow_is_a_type_error() {
    // Neo4j 2026.09 / CYPHER 25 raises TypeError (22003), rather than null,
    // for a syntactically valid integer outside the signed 64-bit range.
    assert!(refusal("MATCH (h:Hub) RETURN toInteger('9223372036854775808') AS x")
        .await
        .contains("out of range"));
}
