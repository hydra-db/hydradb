//! DISTINCT, UNION and grouping key rows through an insertion-ordered map
//! instead of rescanning every kept row. Results keep first-occurrence order and the
//! Cypher value equality, with mixed numeric representations treated alike.

mod common;

use common::profiling::profile_query;
use common::MemoryStorage;
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryRow, QueryValue, ScalarValue, VertexRecord};

fn scalar(value: ScalarValue) -> QueryValue {
    QueryValue::Scalar(value)
}

fn integer(value: i128) -> QueryValue {
    scalar(ScalarValue::Integer(value))
}

fn float(value: &str) -> QueryValue {
    scalar(ScalarValue::Float(value.into()))
}

fn string(value: &str) -> QueryValue {
    scalar(ScalarValue::String(value.into()))
}

fn rows(rows: &[QueryRow]) -> Vec<Vec<QueryValue>> {
    rows.iter().map(|row| row.values.clone()).collect()
}

/// `population` rows over `population / 4` distinct kinds, each kind three
/// times plus once more in a second label, so every dedup step sees
/// duplicates spread across the whole input.
fn scaled(population: u64) -> MemoryStorage {
    let kinds = population / 4;
    MemoryStorage::with_vertices((1..=population).map(|id| {
        VertexRecord::new(id)
            .with_label(if id.is_multiple_of(4) { "Other" } else { "Entity" })
            .with_label("Row")
            .with_property("kind", ScalarValue::Integer(i128::from(id % kinds)))
    }))
}

/// At 1x and 10x the fixture, DISTINCT, UNION (whose first arm is
/// deduplicated too) and grouping each return one row per distinct key, over
/// an input whose duplicates are spread across the whole of it.
///
/// This asserts correctness, not scaling, and that is deliberate. An earlier
/// revision hand-rolled the index -- buckets of positions chained per hash
/// beside the caller's `Vec` -- and counted the whole-key comparisons it
/// made, so this test could hold them to one per duplicate row and to a
/// tenfold rise at tenfold input. What those assertions were really
/// protecting was the bespoke structure: that its chaining narrowed lookups
/// to same-hash keys rather than quietly walking everything. `IndexMap` is
/// hashbrown, whose lookup cost is its own guarantee and not this suite's to
/// re-prove -- and it exposes no comparison count to re-prove it with. The
/// structure and its assertions went together.
///
/// These checks protect cardinality and first representatives; they do not
/// claim measured latency or re-prove the hash table's complexity.
#[tokio::test]
async fn distinct_union_and_grouping_dedup_at_ten_times_the_fixture() {
    const FIXTURE: u64 = 400;
    let queries = [
        ("distinct", "MATCH (n:Row) RETURN DISTINCT n.kind AS kind"),
        (
            "union",
            "MATCH (n:Entity) RETURN n.kind AS kind UNION MATCH (n:Row) RETURN n.kind AS kind",
        ),
        ("grouping", "MATCH (n:Row) RETURN n.kind AS kind, count(*) AS total"),
    ];
    for (name, query) in queries {
        for population in [FIXTURE, FIXTURE * 10] {
            let (result, _, _) = profile_query(scaled(population), query, &ParameterMap::new()).await;
            assert_eq!(result.rows.len(), (population / 4) as usize, "{name} at {population}");
        }
    }
}

fn values_fixture() -> MemoryStorage {
    let scores = [
        Some(ScalarValue::Integer(1)),
        Some(ScalarValue::Float("1.0".into())),
        Some(ScalarValue::Integer(1)),
        Some(ScalarValue::Float("0.0".into())),
        Some(ScalarValue::Float("-0.0".into())),
        Some(ScalarValue::Integer(0)),
        Some(ScalarValue::Float("NaN".into())),
        None,
        Some(ScalarValue::Float("NaN".into())),
        Some(ScalarValue::String("1".into())),
        Some(ScalarValue::Boolean(true)),
        None,
        Some(ScalarValue::Float("1.0".into())),
    ];
    MemoryStorage::with_vertices(scores.into_iter().enumerate().map(|(index, score)| {
        let id = index as u64 + 1;
        let mut vertex = VertexRecord::new(id)
            .with_label("Entity")
            .with_label(if id.is_multiple_of(2) { "Even" } else { "Odd" })
            .with_property("kind", ScalarValue::String(["a", "b", "c"][index % 3].into()));
        if let Some(score) = score {
            vertex = vertex.with_property("score", score);
        }
        vertex
    }))
}

/// Neo4j 2026.09 / CYPHER 25 groups mixed numeric representations and signed
/// zeros, keeps each NaN occurrence, and keeps one group for missing scores.
/// First-occurrence result representations remain intact.
fn distinct_scores() -> Vec<Vec<QueryValue>> {
    vec![
        vec![integer(1)],
        vec![float("0.0")],
        vec![float("NaN")],
        vec![QueryValue::Null],
        vec![float("NaN")],
        vec![string("1")],
        vec![scalar(ScalarValue::Boolean(true))],
    ]
}

#[tokio::test]
async fn distinct_keeps_first_occurrences_under_cypher_equality() {
    let engine = CypherEngine::new(values_fixture());
    let result = engine
        .execute("MATCH (n:Entity) RETURN DISTINCT n.score AS score", &ParameterMap::new())
        .await
        .expect("DISTINCT executes");
    assert_eq!(rows(&result.rows), distinct_scores());
}

#[tokio::test]
async fn union_deduplicates_every_arm_including_the_first() {
    let engine = CypherEngine::new(values_fixture());
    let parameters = ParameterMap::new();
    let union = engine
        .execute(
            "MATCH (n:Odd) RETURN n.score AS score \
             UNION MATCH (n:Entity) RETURN n.score AS score \
             UNION MATCH (n:Missing) RETURN n.score AS score",
            &parameters,
        )
        .await
        .expect("UNION executes");
    // The odd rows first (ids 1, 3, ..., 13, which repeat `1` and NaN inside
    // the first arm), then what the second arm adds, each value once.
    assert_eq!(
        rows(&union.rows),
        vec![
            vec![integer(1)],
            vec![float("-0.0")],
            vec![float("NaN")],
            vec![float("NaN")],
            vec![scalar(ScalarValue::Boolean(true))],
            vec![float("NaN")],
            vec![QueryValue::Null],
            vec![float("NaN")],
            vec![string("1")],
        ]
    );

    // A first arm with duplicates and nothing after it is still deduplicated.
    let first_only = engine
        .execute(
            "MATCH (n:Entity) RETURN n.kind AS kind UNION MATCH (n:Missing) RETURN n.kind AS kind",
            &parameters,
        )
        .await
        .expect("UNION with an empty second arm executes");
    assert_eq!(
        rows(&first_only.rows),
        vec![vec![string("a")], vec![string("b")], vec![string("c")]]
    );

    // UNION ALL keeps every row of every arm.
    let all = engine
        .execute(
            "MATCH (n:Entity) RETURN n.kind AS kind UNION ALL MATCH (n:Odd) RETURN n.kind AS kind",
            &parameters,
        )
        .await
        .expect("UNION ALL executes");
    assert_eq!(all.rows.len(), 13 + 7);
}

#[tokio::test]
async fn grouping_keeps_first_seen_group_order_and_one_null_group() {
    let engine = CypherEngine::new(values_fixture());
    let result = engine
        .execute("MATCH (n:Entity) RETURN n.score AS score, count(*) AS total", &ParameterMap::new())
        .await
        .expect("grouping executes");
    let totals = [4, 3, 1, 2, 1, 1, 1];
    assert_eq!(
        rows(&result.rows),
        distinct_scores()
            .into_iter()
            .zip(totals)
            .map(|(mut row, total)| {
                row.push(QueryValue::Count(total));
                row
            })
            .collect::<Vec<_>>()
    );
}

/// List values compare element by element, in order, with the same scalar
/// equality: numeric representations merge recursively; reordered lists differ.
#[tokio::test]
async fn list_values_deduplicate_element_by_element() {
    let engine = CypherEngine::new(MemoryStorage::with_vertices([
        VertexRecord::new(1).with_label("A").with_property("v", ScalarValue::Integer(1)),
        VertexRecord::new(2).with_label("A").with_property("v", ScalarValue::Integer(2)),
        VertexRecord::new(3).with_label("B").with_property("v", ScalarValue::Integer(1)),
        VertexRecord::new(4).with_label("B").with_property("v", ScalarValue::Integer(2)),
        VertexRecord::new(5).with_label("C").with_property("v", ScalarValue::Integer(2)),
        VertexRecord::new(6).with_label("C").with_property("v", ScalarValue::Integer(1)),
        VertexRecord::new(7)
            .with_label("D")
            .with_property("v", ScalarValue::Float("1.0".into())),
        VertexRecord::new(8).with_label("D").with_property("v", ScalarValue::Integer(2)),
    ]));
    let result = engine
        .execute(
            "MATCH (n:A) RETURN collect(n.v) AS values \
             UNION MATCH (n:B) RETURN collect(n.v) AS values \
             UNION MATCH (n:C) RETURN collect(n.v) AS values \
             UNION MATCH (n:D) RETURN collect(n.v) AS values",
            &ParameterMap::new(),
        )
        .await
        .expect("UNION of lists executes");
    assert_eq!(
        rows(&result.rows),
        vec![
            vec![QueryValue::List(vec![integer(1), integer(2)])],
            vec![QueryValue::List(vec![integer(2), integer(1)])],
        ]
    );
}

/// SKIP and LIMIT count distinct rows. Previously the window was
/// applied to the binding rows beneath DISTINCT, so a window full of
/// duplicates returned too few rows.
#[tokio::test]
async fn a_distinct_window_counts_distinct_rows() {
    let parameters = ParameterMap::new();
    let (result, _, plan) = profile_query(
        values_fixture(),
        "MATCH (n:Entity) RETURN DISTINCT n.kind AS kind ORDER BY kind SKIP 1 LIMIT 2",
        &parameters,
    )
    .await;
    assert_eq!(rows(&result.rows), vec![vec![string("b")], vec![string("c")]]);
    let explain = hydradb_cypher_engine::explain_physical_plan(&plan);
    assert!(
        explain.contains("ProjectExec(distinct=true, items=[n.kind AS kind], skip=1, limit=2)"),
        "{explain}"
    );
    assert!(!explain.contains("LimitExec"), "{explain}");
    assert!(!explain.contains("SkipExec"), "{explain}");

    let (result, _, _) = profile_query(
        values_fixture(),
        "MATCH (n:Entity) RETURN DISTINCT n.kind AS kind LIMIT 2",
        &parameters,
    )
    .await;
    assert_eq!(rows(&result.rows), vec![vec![string("a")], vec![string("b")]]);

    let (result, _, _) = profile_query(
        values_fixture(),
        "MATCH (n:Entity) RETURN DISTINCT n.kind AS kind ORDER BY kind DESC LIMIT 0",
        &parameters,
    )
    .await;
    assert!(result.rows.is_empty());
}

#[tokio::test]
async fn aggregate_distinct_keeps_each_nan_occurrence_like_neo4j() {
    let engine = CypherEngine::new(values_fixture());
    let result = engine
        .execute("MATCH (n:Entity) RETURN count(DISTINCT n.score) AS n", &ParameterMap::new())
        .await
        .unwrap();
    assert_eq!(rows(&result.rows), vec![vec![QueryValue::Count(6)]]);
}
