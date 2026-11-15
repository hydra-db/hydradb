//! Aggregation at a WITH boundary.
//!
//! `WITH` groups by its non-aggregate items exactly as RETURN does, and what
//! follows reads the grouped rows. The matrix rows `combo_with_aggregate`,
//! `compat_with_grouped_aggregate_filter` and `compat_with_aggregate_then_match`
//! hold the same answers to Neo4j.

mod common;

use common::compat::{run, table};

#[tokio::test]
async fn with_counts_then_return_reads_the_count() {
    let result = run("MATCH (i:Item) WITH count(i) AS n RETURN n").await;
    assert_eq!(table(&result), rows![["6"]]);
}

#[tokio::test]
async fn with_groups_and_its_where_filters_the_groups() {
    let result = run("MATCH (i:Item) WITH i.kind AS kind, count(*) AS total WHERE total > 2 RETURN kind, total").await;
    assert_eq!(table(&result), rows![["\"fruit\"", "4"]]);
}

#[tokio::test]
async fn an_aggregate_from_with_correlates_a_later_match() {
    let result = run("MATCH (i:Item) WITH max(i.score) AS top MATCH (j:Item) WHERE j.score = top RETURN j.kind AS kind").await;
    assert_eq!(table(&result), rows![["\"veg\""]]);
}

#[tokio::test]
async fn a_grouping_key_that_is_a_node_stays_a_node() {
    // Grouped by the hub, the hub is still readable -- and expandable -- after.
    let result = run("MATCH (h:Hub)-[:HAS]->(i:Item) WITH h, count(i) AS items \
         MATCH (h)-[:HAS]->(j:Item {name: 'carrot'}) RETURN h.name AS hub, items, j.score AS score")
    .await;
    assert_eq!(table(&result), rows![["\"hub\"", "3", "2"]]);
}

#[tokio::test]
async fn with_orders_and_windows_its_groups() {
    let result = run("MATCH (i:Item) WITH i.kind AS kind, sum(i.score) AS total ORDER BY total DESC LIMIT 1 RETURN kind, total").await;
    assert_eq!(table(&result), rows![["\"veg\"", "7"]]);
}

#[tokio::test]
async fn aggregating_nothing_at_with_still_yields_one_row() {
    let result = run("MATCH (i:Item) WHERE i.score > 100 WITH count(*) AS n, collect(i.name) AS names RETURN n, names").await;
    assert_eq!(table(&result), rows![["0", "[]"]]);
}

#[tokio::test]
async fn a_collected_list_unwinds_again() {
    let result = run("MATCH (i:Item) WITH collect(i.score) AS scores UNWIND scores AS score RETURN sum(score) AS total").await;
    assert_eq!(table(&result), rows![["11"]]);
}

#[tokio::test]
async fn two_aggregating_withs_chain() {
    let result =
        run("MATCH (i:Item) WITH i.kind AS kind, count(*) AS n WITH sum(n) AS total, count(kind) AS kinds RETURN total, kinds").await;
    assert_eq!(table(&result), rows![["6", "2"]]);
}

#[tokio::test]
async fn unwinding_collected_nodes_reads_their_records() {
    // The collected list holds the nodes by identity; the row keeps their
    // records, so each unwinds as the node, not as its id.
    let mut names: Vec<String> = table(&run("MATCH (i:Item) WITH collect(i) AS items UNWIND items AS item RETURN item.name AS name").await)
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    names.sort();
    assert_eq!(names, ["\"Durian\"", "\"apple\"", "\"apple\"", "\"banana\"", "\"carrot\"", "null"]);
}

#[tokio::test]
async fn membership_recursively_evaluates_aggregate_operands() {
    assert_eq!(
        table(&run("MATCH (i:Item) WITH count(*) IN [6] AS has_six RETURN has_six").await),
        rows![["true"]]
    );
    assert_eq!(
        table(&run("MATCH (i:Item) WITH (count(*) + 1) IN [7] AS has_seven RETURN has_seven").await),
        rows![["true"]]
    );
    assert_eq!(
        table(&run("MATCH (i:Missing) WITH count(*) IN [1, null] AS answer RETURN answer").await),
        rows![["null"]]
    );
}
