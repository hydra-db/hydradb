//! `ORDER BY ... [SKIP s] LIMIT l` over an input the index cannot order keeps
//! a `Sort`. Under that window the sort ranks with a heap of at most `s + l`
//! rows and returns exactly the rows a full stable sort truncated to the window
//! would -- the rows it hands its parent, measured by `rows_out`, are bounded by
//! the window.
//!
//! What is *not* bounded is the sort's peak retention: its child hands it a
//! materialized input, so the peak-retained-rows counter reads the
//! whole input with or without a window. The assertions below hold it to that
//! truth rather than to the window, so a future streaming child shows up here
//! as a failure to update instead of passing silently.

mod common;

use common::profiling::{operator, profile_query};
use common::MemoryStorage;
use hydradb_cypher_engine::{ParameterMap, QueryRow, QueryValue, ScalarValue, VertexRecord};

const POPULATION: u64 = 60;

/// Heavy duplicate keys so every window boundary lands inside a tie, a
/// missing key (null) every seventh row, and every ninth key of another type.
fn fixture() -> MemoryStorage {
    MemoryStorage::with_vertices((1..=POPULATION).map(|id| {
        let mut vertex = VertexRecord::new(id).with_label("Entity");
        if id % 7 != 0 {
            let score = match id % 9 {
                0 => ScalarValue::String(format!("s{}", id % 4).into()),
                4 => ScalarValue::Boolean(id % 2 == 0),
                5 => ScalarValue::Float(format!("{}.5", id % 3).into()),
                // `1.0` ties the integer `1`: equal keys of different types.
                6 => ScalarValue::Float("1.0".into()),
                _ => ScalarValue::Integer(i128::from(id % 5) - 2),
            };
            vertex = vertex.with_property("score", score);
        }
        if id % 3 != 0 {
            vertex = vertex.with_property("kind", ScalarValue::String(format!("k{}", id % 2).into()));
        }
        vertex.with_label(if id % 2 == 0 { "Even" } else { "Odd" })
    }))
}

fn ids(rows: &[QueryRow]) -> Vec<u64> {
    rows.iter()
        .map(|row| match row.values.as_slice() {
            [QueryValue::VertexId(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

const ORDERS: [&str; 6] = [
    "n.score",
    "n.score DESC",
    "n.score, n.id DESC",
    "n.kind DESC, n.score",
    "n.kind, n.score DESC, n.id",
    // No tie-break at all: equal keys keep input order, exactly as the full
    // stable sort leaves them.
    "n.kind",
];

const WINDOWS: [(usize, usize); 9] = [(0, 1), (0, 4), (2, 3), (5, 9), (0, 17), (58, 5), (0, 60), (0, 200), (100, 3)];

#[tokio::test]
async fn a_windowed_sort_returns_only_the_window_and_matches_the_full_sort() {
    let parameters = ParameterMap::new();
    for order in ORDERS {
        let reference_query = format!("MATCH (n:Entity) RETURN n.id AS id ORDER BY {order}");
        let (reference, reference_profile, _) = profile_query(fixture(), &reference_query, &parameters).await;
        let reference = ids(&reference.rows);
        assert_eq!(reference.len() as u64, POPULATION);
        // Without a window the sort holds its whole input.
        assert_eq!(operator(&reference_profile, "SortExec").peak_retained_rows, POPULATION);

        for (skip, limit) in WINDOWS {
            let window = if skip == 0 {
                format!("LIMIT {limit}")
            } else {
                format!("SKIP {skip} LIMIT {limit}")
            };
            let query = format!("{reference_query} {window}");
            let (result, profile, _) = profile_query(fixture(), &query, &parameters).await;
            let expected = reference.iter().copied().skip(skip).take(limit).collect::<Vec<_>>();
            assert_eq!(ids(&result.rows), expected, "{query}");

            let scan = operator(&profile, "VertexScan");
            assert_eq!(scan.access, Some("label-prefix"));
            assert_eq!(scan.rows_out, POPULATION, "{query}: the input is complete");
            let sort = operator(&profile, "SortExec");
            let window_rows = (skip + limit) as u64;
            // What the window bounds: the rows the sort ranks into and hands
            // on. `SKIP` is applied above the sort, so the window it works to
            // is `SKIP + LIMIT`.
            assert_eq!(
                sort.rows_out,
                window_rows.min(POPULATION),
                "{query}: sort returned {} rows for a window of {window_rows}",
                sort.rows_out
            );
            // What it does not bound: the input the child materialized, which
            // the sort holds in full before ranking any of it.
            assert_eq!(
                sort.peak_retained_rows, POPULATION,
                "{query}: a windowed sort still holds its whole input"
            );
        }
    }
}

#[tokio::test]
async fn limit_zero_and_empty_input_retain_nothing() {
    let parameters = ParameterMap::new();
    let (result, profile, _) = profile_query(
        fixture(),
        "MATCH (n:Entity) RETURN n.id AS id ORDER BY n.score LIMIT 0",
        &parameters,
    )
    .await;
    assert!(result.rows.is_empty());
    // LIMIT 0 never runs the sort's input.
    assert_eq!(operator(&profile, "VertexScan").invocations, 0);
    assert_eq!(operator(&profile, "SortExec").peak_retained_rows, 0);

    for query in [
        "MATCH (n:Missing) RETURN n.id AS id ORDER BY n.score LIMIT 3",
        "MATCH (n:Missing) RETURN n.id AS id ORDER BY n.score DESC SKIP 2 LIMIT 3",
    ] {
        let (result, profile, _) = profile_query(fixture(), query, &parameters).await;
        assert!(result.rows.is_empty(), "{query}");
        assert_eq!(operator(&profile, "SortExec").peak_retained_rows, 0, "{query}");
    }
}

/// Multi-label input and a filter below the sort: the sort sees a complete,
/// reduced input, holds all of it, and returns only its window.
#[tokio::test]
async fn a_filtered_multi_label_input_bounds_what_the_sort_returns() {
    let parameters = ParameterMap::new();
    let base = "MATCH (n:Entity:Even) WHERE n.kind <> 'k9' RETURN n.id AS id ORDER BY n.kind DESC, n.score";
    let (reference, _, _) = profile_query(fixture(), base, &parameters).await;
    let reference = ids(&reference.rows);
    assert!(reference.len() > 10);
    let (result, profile, _) = profile_query(fixture(), &format!("{base} SKIP 3 LIMIT 4"), &parameters).await;
    assert_eq!(ids(&result.rows), reference[3..7].to_vec());
    let sort = operator(&profile, "SortExec");
    assert_eq!(sort.rows_out, 7, "the window bounds what the sort returns");
    // The filter below the sort reduced the input; the sort holds all of what
    // survived, not just its window.
    assert_eq!(
        sort.peak_retained_rows,
        reference.len() as u64,
        "a windowed sort still holds its whole input"
    );
}
