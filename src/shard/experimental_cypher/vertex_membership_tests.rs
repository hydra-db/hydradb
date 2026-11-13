//! Vertex membership on the experimental route.
//!
//! A canonical vertex record is the node: a valid record, even with empty
//! metadata, means the node exists, and a missing record means it does not.
//! These tests pin the write side of that contract. Clearing a node's labels
//! and properties keeps its record, explicit deletion removes it, and every
//! writer that inserts an edge gives both endpoints a record in the same
//! transaction. The experimental all-node scan enumerates records, so it is
//! the query-level witness that a node exists.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;

use crate::{
    keys, CypherEngineMode, EdgeMetadata, EdgeMutation, GraphIndexPolicy, GraphOpenOptions, GraphShard, QueryContext, QueryValue,
    RelationshipMutation, VertexId, VertexMetadata, VertexPropertyValue,
};

const CELL: &str = "cell-a";

async fn shard(path: &str) -> GraphShard {
    GraphShard::open_standalone_writer(path, Arc::new(InMemory::new()))
        .await
        .expect("open graph shard")
}

fn edge(src: VertexId, dst: VertexId, idempotency_key: &str) -> EdgeMutation {
    EdgeMutation {
        cell_id: CELL.to_string(),
        edge_type: "LINK".to_string(),
        src,
        dst,
        idempotency_key: idempotency_key.to_string(),
    }
}

/// The stored canonical record, decoded. `None` means the node is absent.
async fn record(shard: &GraphShard, vertex: VertexId) -> Option<VertexMetadata> {
    let key = keys::vertex(CELL, vertex);
    shard
        .read_remote(&key)
        .await
        .expect("read vertex record")
        .map(|value| crate::codec::decode_vertex_metadata(&key, &value).expect("decode vertex record"))
}

/// Vertex ids from the first column, in result order.
async fn ids(shard: &GraphShard, query: &str) -> Vec<VertexId> {
    let result = shard
        .execute_cypher_rows(
            QueryContext::new(CELL, "vertex-membership").with_cypher_engine(CypherEngineMode::Experimental),
            query,
        )
        .await
        .unwrap_or_else(|error| panic!("experimental route rejected {query}: {error}"));
    result
        .rows
        .iter()
        .map(|row| match row.values[0] {
            QueryValue::VertexId(id) => id,
            ref other => panic!("unexpected id: {other:?}"),
        })
        .collect()
}

async fn all_nodes(shard: &GraphShard) -> Vec<VertexId> {
    ids(shard, "MATCH (n) RETURN n.id AS id ORDER BY id").await
}

async fn mutate(shard: &GraphShard, query: &str) {
    // These are distinct statements, not retries. The DataFusion feature also
    // enables durable statement receipts for the legacy mutation fallback.
    static NEXT_WRITE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let key = format!(
        "vertex-membership-write-{}",
        NEXT_WRITE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    shard
        .execute_cypher(
            QueryContext::new(CELL, key).with_cypher_engine(CypherEngineMode::Experimental),
            query,
        )
        .await
        .unwrap_or_else(|error| panic!("experimental route rejected {query}: {error}"));
}

/// Ticket example 3: removing a node's last label leaves a node with no
/// labels, not a deleted one.
#[tokio::test]
async fn clearing_the_last_label_keeps_the_node() {
    let shard = shard("graph/membership-clear").await;
    shard
        .set_vertex_metadata(CELL, 777003, VertexMetadata::default().with_label("Probe"))
        .await
        .expect("label the node");
    shard
        .set_vertex_metadata(CELL, 777003, VertexMetadata::default())
        .await
        .expect("clear the node");

    assert_eq!(record(&shard, 777003).await, Some(VertexMetadata::default()));
    assert_eq!(all_nodes(&shard).await, vec![777003]);
    shard.close().await.expect("close graph shard");
}

/// Clearing through Cypher `REMOVE` is the same operation.
#[tokio::test]
async fn removing_every_label_and_property_through_cypher_keeps_the_node() {
    let shard = shard("graph/membership-cypher-remove").await;
    shard
        .set_vertex_metadata(
            CELL,
            7,
            VertexMetadata::default()
                .with_label("Probe")
                .with_property("name", VertexPropertyValue::String("seven".into())),
        )
        .await
        .expect("seed the node");
    mutate(&shard, "MATCH (n:Probe {id: 7}) REMOVE n:Probe, n.name").await;

    assert_eq!(record(&shard, 7).await, Some(VertexMetadata::default()));
    assert_eq!(all_nodes(&shard).await, vec![7]);
    shard.close().await.expect("close graph shard");
}

/// Setting empty metadata on an id with no record creates an empty node.
#[tokio::test]
async fn empty_metadata_on_a_new_id_creates_the_node() {
    let shard = shard("graph/membership-empty-create").await;
    shard
        .set_vertex_metadata(CELL, 41, VertexMetadata::default())
        .await
        .expect("create the empty node");
    shard
        .set_vertex_metadata_batch(CELL, [(42, VertexMetadata::default())])
        .await
        .expect("create the empty node in a batch");
    shard
        .merge_vertex_metadata_batch(CELL, [(43, VertexMetadata::default())], None)
        .await
        .expect("create the empty node by merge");

    for vertex in [41, 42, 43] {
        assert_eq!(record(&shard, vertex).await, Some(VertexMetadata::default()), "vertex {vertex}");
    }
    assert_eq!(all_nodes(&shard).await, vec![41, 42, 43]);
    shard.close().await.expect("close graph shard");
}

/// Explicit deletion removes an empty node and reports it; deleting the now
/// absent node again reports nothing deleted.
#[tokio::test]
async fn deleting_an_empty_node_removes_it_and_reports_the_deletion() {
    let shard = shard("graph/membership-delete-empty").await;
    shard
        .set_vertex_metadata(CELL, 777003, VertexMetadata::default().with_label("Probe"))
        .await
        .expect("label the node");
    shard
        .set_vertex_metadata(CELL, 777003, VertexMetadata::default())
        .await
        .expect("clear the node");

    let deleted = shard.delete_vertex(CELL, 777003, "delete-empty").await.expect("delete");
    assert!(deleted.vertex_deleted, "the empty node existed");
    assert_eq!(record(&shard, 777003).await, None);
    assert!(all_nodes(&shard).await.is_empty());

    let again = shard.delete_vertex(CELL, 777003, "delete-empty-again").await.expect("delete again");
    assert!(!again.vertex_deleted, "nothing left to delete");
    shard.close().await.expect("close graph shard");
}

/// Ticket example 4: a native edge write establishes both endpoints, and
/// removing the edge keeps them.
#[tokio::test]
async fn a_native_edge_write_establishes_both_endpoints() {
    let shard = shard("graph/membership-write-edge").await;
    shard.write_edge(edge(10, 20, "edge-create-1")).await.expect("write edge");

    assert_eq!(record(&shard, 10).await, Some(VertexMetadata::default()));
    assert_eq!(record(&shard, 20).await, Some(VertexMetadata::default()));
    assert_eq!(all_nodes(&shard).await, vec![10, 20]);
    assert_eq!(ids(&shard, "MATCH (a {id: 10})-[:LINK]->(b) RETURN b.id AS id").await, vec![20]);

    shard.delete_edge(edge(10, 20, "edge-delete-1")).await.expect("delete edge");
    assert_eq!(all_nodes(&shard).await, vec![10, 20], "removing the last edge keeps both nodes");
    shard.close().await.expect("close graph shard");
}

/// Establishing an endpoint never overwrites the metadata it already has.
#[tokio::test]
async fn an_edge_write_keeps_existing_endpoint_metadata() {
    let shard = shard("graph/membership-keep-metadata").await;
    let labelled = VertexMetadata::default()
        .with_label("Person")
        .with_property("name", VertexPropertyValue::String("ten".into()));
    shard
        .set_vertex_metadata(CELL, 10, labelled.clone())
        .await
        .expect("seed the source");
    shard.write_edge(edge(10, 20, "edge-keep")).await.expect("write edge");

    assert_eq!(record(&shard, 10).await, Some(labelled));
    assert_eq!(record(&shard, 20).await, Some(VertexMetadata::default()));
    shard.close().await.expect("close graph shard");
}

/// Ticket example 5: replaying an edge request after its endpoint was deleted
/// returns the recorded response and does not bring the endpoint back. A new
/// request with a fresh key may recreate it.
#[tokio::test]
async fn retrying_an_old_edge_request_does_not_resurrect_a_deleted_endpoint() {
    let shard = shard("graph/membership-replay").await;
    let first = shard.write_edge(edge(10, 20, "edge-create-1")).await.expect("write edge");
    let deleted = shard.detach_delete_vertex(CELL, 20, "delete-20").await.expect("detach delete");
    assert!(deleted.vertex_deleted);
    assert_eq!(deleted.incident_edges_deleted, 1);

    let replay = shard.write_edge(edge(10, 20, "edge-create-1")).await.expect("replay");
    assert_eq!(replay, first, "a replay returns the recorded response");
    assert_eq!(record(&shard, 20).await, None, "the replay did not recreate 20");
    assert_eq!(all_nodes(&shard).await, vec![10]);
    assert!(ids(&shard, "MATCH (a {id: 10})-[:LINK]->(b) RETURN b.id AS id").await.is_empty());

    shard.write_edge(edge(10, 20, "edge-create-2")).await.expect("fresh create");
    assert_eq!(all_nodes(&shard).await, vec![10, 20]);
    shard.close().await.expect("close graph shard");
}

/// Every other native edge writer establishes its endpoints too.
#[tokio::test]
async fn every_native_edge_writer_establishes_its_endpoints() {
    let shard = shard("graph/membership-writers").await;
    shard
        .write_edge_mutations_batch(CELL, [edge(1, 2, "batch-1"), edge(1, 3, "batch-2")])
        .await
        .expect("edge mutation batch");
    shard
        .create_relationship(edge(4, 5, "relationship-1"), EdgeMetadata::default())
        .await
        .expect("create relationship");
    shard
        .import_relationships_batch(
            CELL,
            "LINK",
            [RelationshipMutation {
                cell_id: CELL.to_string(),
                edge_type: "LINK".to_string(),
                src: 6,
                dst: 7,
                relationship_id: 9_001,
                metadata: EdgeMetadata::default(),
            }],
            "relationship-import-1",
        )
        .await
        .expect("relationship import");
    shard
        .bulk_import_edges(CELL, "LINK", [(8, 9), (8, 10)], "bulk-import-1")
        .await
        .expect("bulk import");

    let expected = (1..=10).collect::<Vec<VertexId>>();
    for vertex in &expected {
        assert_eq!(record(&shard, *vertex).await, Some(VertexMetadata::default()), "vertex {vertex}");
    }
    assert_eq!(all_nodes(&shard).await, expected);
    shard.close().await.expect("close graph shard");
}

/// The trusted segment append, which only an outbound-only store accepts,
/// establishes its source and every inserted destination.
#[tokio::test]
async fn a_segment_append_establishes_its_endpoints() {
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/membership-segment",
        Arc::new(InMemory::new()),
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .expect("open outbound-only graph shard");
    shard
        .bulk_append_out_adjacency_segment_trusted(CELL, "LINK", 11, [12, 13], "segment-1")
        .await
        .expect("segment append");

    for vertex in [11, 12, 13] {
        assert_eq!(record(&shard, vertex).await, Some(VertexMetadata::default()), "vertex {vertex}");
    }
    shard.close().await.expect("close graph shard");
}

/// Single-statement Cypher edge writes without endpoint labels establish their
/// endpoints. The `UNWIND $rows ... CREATE` batch form is routed to
/// `write_edge_mutations_batch`, covered above.
#[tokio::test]
async fn cypher_edge_writes_establish_unlabelled_endpoints() {
    let shard = shard("graph/membership-cypher-writes").await;
    mutate(&shard, "CREATE (a {id: 1})-[:LINK]->(b {id: 2})").await;
    mutate(&shard, "MERGE (a {id: 3})-[:LINK]->(b {id: 4})").await;

    assert_eq!(all_nodes(&shard).await, vec![1, 2, 3, 4]);
    shard.close().await.expect("close graph shard");
}

/// A failed edge batch publishes neither its edges nor their new endpoints.
#[tokio::test]
async fn a_failed_edge_batch_publishes_no_endpoints() {
    let shard = shard("graph/membership-atomic").await;
    shard
        .set_vertex_metadata(CELL, 1, VertexMetadata::default().with_label("Person"))
        .await
        .expect("seed the source");
    // The second mutation reuses the first one's idempotency key, so the whole
    // batch is rejected before its transaction commits.
    let error = shard
        .write_edge_mutations_batch(CELL, [edge(1, 2, "duplicate"), edge(1, 3, "duplicate")])
        .await;
    assert!(error.is_err(), "the batch is rejected");

    assert_eq!(record(&shard, 2).await, None);
    assert_eq!(record(&shard, 3).await, None);
    assert_eq!(all_nodes(&shard).await, vec![1]);
    shard.close().await.expect("close graph shard");
}

/// Seeds the cleanup fixtures: 20 is a `Person` and 21 is empty, both without
/// edges; 22 is a `Location` without edges.
async fn seed_cleanup_candidates(shard: &GraphShard) {
    shard
        .set_vertex_metadata(CELL, 20, VertexMetadata::default().with_label("Person"))
        .await
        .expect("seed person");
    shard
        .set_vertex_metadata(CELL, 21, VertexMetadata::default())
        .await
        .expect("seed empty vertex");
    shard
        .set_vertex_metadata(CELL, 22, VertexMetadata::default().with_label("Location"))
        .await
        .expect("seed location");
}

/// The isolated-vertex cleanup (`MATCH (n:Location {id: row.vertex}) WHERE
/// NOT (n)--() DELETE n`) deletes only candidates its pattern matches. A
/// labelled or empty vertex the pattern does not match is left in place and
/// not counted.
#[tokio::test]
async fn the_isolated_vertex_cleanup_skips_candidates_its_pattern_does_not_match() {
    let shard = shard("graph/membership-isolated-mismatch").await;
    seed_cleanup_candidates(&shard).await;
    let location = VertexMetadata::default().with_label("Location");

    let results = shard
        .delete_isolated_vertex_mutations_batch(
            CELL,
            [20, 21, 22]
                .into_iter()
                .map(|vertex| (vertex, format!("isolated-mismatch-{vertex}"), location.clone()))
                .collect(),
        )
        .await
        .expect("isolated cleanup");
    let deleted = results.iter().map(|result| result.vertex_deleted).collect::<Vec<_>>();
    assert_eq!(deleted, vec![false, false, true]);
    assert_eq!(record(&shard, 20).await, Some(VertexMetadata::default().with_label("Person")));
    assert_eq!(record(&shard, 21).await, Some(VertexMetadata::default()));
    assert_eq!(all_nodes(&shard).await, vec![20, 21]);
    shard.close().await.expect("close graph shard");
}

/// The mixed cleanup (detach rows plus isolated candidates) applies the same
/// rule to its isolated candidates and still deletes its detach rows.
#[tokio::test]
async fn the_mixed_cleanup_skips_isolated_candidates_its_pattern_does_not_match() {
    let shard = shard("graph/membership-mixed-mismatch").await;
    seed_cleanup_candidates(&shard).await;
    shard.write_edge(edge(30, 31, "detach-target")).await.expect("seed detach target");
    let location = VertexMetadata::default().with_label("Location");

    let results = shard
        .delete_vertices_and_isolated_candidates_batch(
            CELL,
            vec![
                (30, "mixed-detach-30".to_string(), true, VertexMetadata::default()),
                (20, "mixed-isolated-20".to_string(), false, location.clone()),
                (21, "mixed-isolated-21".to_string(), false, location.clone()),
                (22, "mixed-isolated-22".to_string(), false, location),
            ],
        )
        .await
        .expect("mixed cleanup");
    let deleted = results.iter().map(|result| result.vertex_deleted).collect::<Vec<_>>();
    assert_eq!(deleted, vec![true, false, false, true]);
    assert_eq!(all_nodes(&shard).await, vec![20, 21, 31]);
    shard.close().await.expect("close graph shard");
}

// ---- Read side: absent ids select no node -----------------------------------

/// The single count a query returns. A rejected query or any other result
/// shape is an error for the calling test to propagate.
async fn count(shard: &GraphShard, query: &str) -> Result<u64, String> {
    let result = shard
        .execute_cypher_rows(
            QueryContext::new(CELL, "vertex-membership-count").with_cypher_engine(CypherEngineMode::Experimental),
            query,
        )
        .await
        .map_err(|error| format!("experimental route rejected {query}: {error}"))?;
    match result.rows.as_slice() {
        [row] => match row.values.first() {
            Some(QueryValue::Count(value)) => Ok(*value),
            other => Err(format!("{query} returned {other:?}, not a count")),
        },
        rows => Err(format!("{query} returned {} rows, a count returns one: {rows:?}", rows.len())),
    }
}

/// Ticket example 1: an id that was never created selects nothing, with or
/// without a label, and counts as zero.
#[tokio::test]
async fn an_id_that_was_never_created_matches_nothing() -> Result<(), String> {
    let shard = shard("graph/membership-never-created").await;

    assert!(ids(&shard, "MATCH (n {id: 777001}) RETURN n.id AS id").await.is_empty());
    assert!(ids(&shard, "MATCH (n:Probe {id: 777001}) RETURN n.id AS id").await.is_empty());
    assert_eq!(count(&shard, "MATCH (n {id: 777001}) RETURN count(n.id) AS total").await?, 0);
    assert_eq!(count(&shard, "MATCH (n {id: 777001}) RETURN count(*) AS total").await?, 0);
    shard.close().await.expect("close graph shard");
    Ok(())
}

/// Ticket example 2: a deleted node is gone from id lookups, and the other
/// endpoint of its removed edge remains.
#[tokio::test]
async fn a_deleted_node_is_absent_from_id_lookups() -> Result<(), String> {
    let shard = shard("graph/membership-deleted").await;
    mutate(&shard, "CREATE (n:Probe {id: 777001})-[:LINK]->(m:Probe {id: 777002})").await;
    mutate(&shard, "MATCH (n:Probe {id: 777001}) DETACH DELETE n").await;

    assert!(ids(&shard, "MATCH (n {id: 777001}) RETURN n.id AS id").await.is_empty());
    assert_eq!(count(&shard, "MATCH (n {id: 777001}) RETURN count(n.id) AS total").await?, 0);
    assert_eq!(ids(&shard, "MATCH (n {id: 777002}) RETURN n.id AS id").await, vec![777002]);
    shard.close().await.expect("close graph shard");
    Ok(())
}

/// Ticket example 3, read side: an empty node is found by its id until it is
/// explicitly deleted.
#[tokio::test]
async fn an_empty_node_is_found_until_deleted() {
    let shard = shard("graph/membership-empty-lookup").await;
    shard
        .set_vertex_metadata(CELL, 777003, VertexMetadata::default().with_label("Probe"))
        .await
        .expect("label the node");
    shard
        .set_vertex_metadata(CELL, 777003, VertexMetadata::default())
        .await
        .expect("clear the node");
    assert_eq!(ids(&shard, "MATCH (n {id: 777003}) RETURN n.id AS id").await, vec![777003]);

    shard.delete_vertex(CELL, 777003, "delete-777003").await.expect("delete");
    assert!(ids(&shard, "MATCH (n {id: 777003}) RETURN n.id AS id").await.is_empty());
    shard.close().await.expect("close graph shard");
}

/// Shapes a node with nothing would otherwise satisfy: a negative pattern,
/// an optional match, a null check, a repeated match and a zero-length hop.
#[tokio::test]
async fn an_absent_id_satisfies_no_pattern_that_accepts_an_empty_node() {
    let shard = shard("graph/membership-empty-accepting").await;
    shard.write_edge(edge(10, 20, "edge-1")).await.expect("write edge");

    for query in [
        "MATCH (n {id: 99}) OPTIONAL MATCH (n)-[:LINK]->(m) RETURN n.id AS id",
        "MATCH (n {id: 99}) WHERE n.name IS NULL RETURN n.id AS id",
        "MATCH (a {id: 10}) MATCH (b {id: 99}) RETURN b.id AS id",
        "MATCH (n {id: 99})-[:LINK*0..1]->(m) RETURN m.id AS id",
    ] {
        assert!(ids(&shard, query).await.is_empty(), "{query}");
    }
    // The same shapes still find a node that exists.
    assert_eq!(
        ids(&shard, "MATCH (n {id: 20}) OPTIONAL MATCH (n)-[:LINK]->(m) RETURN n.id AS id").await,
        vec![20]
    );
    assert_eq!(
        ids(&shard, "MATCH (n {id: 10})-[:LINK*0..1]->(m) RETURN m.id AS id ORDER BY id").await,
        vec![10, 20]
    );
    shard.close().await.expect("close graph shard");
}

/// An absent candidate does not take a LIMIT slot from a node that exists.
#[tokio::test]
async fn an_absent_candidate_takes_no_limit_slot() -> Result<(), String> {
    let shard = shard("graph/membership-limit").await;
    shard
        .set_vertex_metadata(CELL, 2, VertexMetadata::default())
        .await
        .expect("create node 2");

    assert_eq!(
        ids(&shard, "UNWIND [1, 2] AS i MATCH (n {id: i}) RETURN n.id AS id ORDER BY id LIMIT 1").await,
        vec![2]
    );
    assert_eq!(
        ids(&shard, "MATCH (n) WHERE n.id = 1 OR n.id = 2 RETURN n.id AS id ORDER BY id LIMIT 1").await,
        vec![2]
    );
    assert_eq!(
        count(&shard, "MATCH (n) WHERE n.id = 1 OR n.id = 2 OR n.id = 3 RETURN count(n) AS total").await?,
        1
    );
    shard.close().await.expect("close graph shard");
    Ok(())
}

/// A MATCH-driven write against an absent id writes nothing: it neither
/// creates the node nor reports a deletion.
#[tokio::test]
async fn a_match_driven_write_on_an_absent_id_does_nothing() {
    let shard = shard("graph/membership-match-write").await;
    mutate(&shard, "MATCH (n {id: 99}) SET n.name = 'ghost'").await;
    assert_eq!(record(&shard, 99).await, None, "SET did not create the node");

    mutate(&shard, "MATCH (n {id: 99}) DETACH DELETE n").await;
    assert!(all_nodes(&shard).await.is_empty());

    shard.close().await.expect("close graph shard");
}

/// The ingestion clients' isolated-vertex cleanup,
/// `UNWIND $rows AS row MATCH (n {id: row.vertex}) WHERE NOT (n)--() DELETE n
/// RETURN count(n)`, is executed as this batch and counts `vertex_deleted`.
/// An absent id counts nothing; an empty vertex counts as deleted.
#[tokio::test]
async fn the_isolated_vertex_cleanup_counts_only_vertices_that_existed() {
    let shard = shard("graph/membership-isolated-cleanup").await;
    shard
        .set_vertex_metadata(CELL, 7, VertexMetadata::default().with_label("Entity"))
        .await
        .expect("seed an isolated vertex");
    shard
        .set_vertex_metadata(CELL, 8, VertexMetadata::default())
        .await
        .expect("seed an empty isolated vertex");

    let results = shard
        .delete_isolated_vertex_mutations_batch(
            CELL,
            [7, 8, 99]
                .into_iter()
                .map(|vertex| (vertex, format!("isolated-{vertex}"), VertexMetadata::default()))
                .collect(),
        )
        .await
        .expect("isolated cleanup");
    let deleted = results.iter().map(|result| result.vertex_deleted).collect::<Vec<_>>();
    assert_eq!(deleted, vec![true, true, false]);
    assert!(all_nodes(&shard).await.is_empty());
    shard.close().await.expect("close graph shard");
}

// ---- Corruption, snapshots, ordered windows and failed commits --------------

/// Writes raw bytes under `key`, bypassing every graph-level writer.
async fn put_raw(shard: &GraphShard, key: &str, value: &[u8]) {
    let txn = shard
        .storage
        .writer()
        .expect("writer")
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .expect("begin raw transaction");
    txn.put(key.as_bytes(), value).expect("stage raw write");
    crate::codec::commit_txn_strict(txn, true).await.expect("commit raw write");
}

/// A record that does not decode is an error, never an absent vertex.
#[tokio::test]
async fn a_corrupt_record_is_an_error_not_an_absent_vertex() {
    let shard = shard("graph/membership-corrupt").await;
    put_raw(&shard, &keys::vertex(CELL, 5), b"not a vertex record").await;

    let error = shard
        .execute_cypher_rows(
            QueryContext::new(CELL, "vertex-membership-corrupt").with_cypher_engine(CypherEngineMode::Experimental),
            "MATCH (n {id: 5}) RETURN n.id AS id",
        )
        .await
        .expect_err("a corrupt record fails the read");
    assert!(error.to_string().contains("vertex"), "unexpected error: {error}");
    shard.close().await.expect("close graph shard");
}

/// The vertex record a retained storage snapshot holds.
async fn record_in(snapshot: &crate::GraphSnapshot<'_>, vertex: VertexId) -> Option<VertexMetadata> {
    let key = keys::vertex(CELL, vertex);
    snapshot
        .storage_snapshot
        .get_with_options(key.as_bytes(), &slatedb::config::ReadOptions::default())
        .await
        .expect("read vertex record in snapshot")
        .map(|value| crate::codec::decode_vertex_metadata(&key, &value).expect("decode vertex record"))
}

/// The records query hydration returns for `vertex` inside `snapshot`'s scope.
async fn hydrated_in(shard: &GraphShard, snapshot: &crate::GraphSnapshot<'_>, vertex: VertexId) -> Vec<(VertexId, VertexMetadata)> {
    let budget = crate::shard::QueryBudget::new(None, None);
    crate::SlateGraphStore::scope_snapshot(
        Arc::clone(&snapshot.storage_snapshot),
        shard.existing_vertex_metadata_batch_at(CELL, &[vertex], snapshot.read_epoch, &budget),
    )
    .await
    .expect("hydrate inside the snapshot")
    .collect()
}

/// Experimental queries execute against one retained storage snapshot. A
/// snapshot keeps the membership it was taken with through every later
/// lifecycle step: create, clear, delete and recreate.
#[tokio::test]
async fn a_retained_snapshot_keeps_its_membership() {
    let shard = shard("graph/membership-snapshots").await;
    let labelled = VertexMetadata::default().with_label("Probe");

    shard.set_vertex_metadata(CELL, 30, labelled.clone()).await.expect("create");
    let created = shard.snapshot(CELL).await.expect("snapshot after create");
    shard.set_vertex_metadata(CELL, 30, VertexMetadata::default()).await.expect("clear");
    let cleared = shard.snapshot(CELL).await.expect("snapshot after clear");
    shard.delete_vertex(CELL, 30, "delete-30").await.expect("delete");
    let deleted = shard.snapshot(CELL).await.expect("snapshot after delete");
    shard.write_edge(edge(30, 31, "recreate-30")).await.expect("recreate");
    let recreated = shard.snapshot(CELL).await.expect("snapshot after recreate");

    assert_eq!(record_in(&created, 30).await, Some(labelled.clone()));
    assert_eq!(
        record_in(&cleared, 30).await,
        Some(VertexMetadata::default()),
        "a cleared vertex still exists"
    );
    assert_eq!(record_in(&deleted, 30).await, None);
    assert_eq!(record_in(&recreated, 30).await, Some(VertexMetadata::default()));

    // Query hydration, scoped to each snapshot the way the experimental
    // adapter scopes it, sees the same membership rather than current state.
    assert_eq!(hydrated_in(&shard, &created, 30).await, vec![(30, labelled)]);
    assert_eq!(hydrated_in(&shard, &cleared, 30).await, vec![(30, VertexMetadata::default())]);
    assert_eq!(
        hydrated_in(&shard, &deleted, 30).await,
        Vec::new(),
        "the deleted snapshot does not see the recreated vertex"
    );
    assert_eq!(hydrated_in(&shard, &recreated, 30).await, vec![(30, VertexMetadata::default())]);
    drop((created, cleared, deleted, recreated));
    shard.close().await.expect("close graph shard");
}

/// An ordered property window skips a candidate whose vertex is absent (an
/// index entry without a record) and keeps every other vertex's own metadata.
#[tokio::test]
async fn an_ordered_window_skips_an_absent_candidate() {
    let shard = shard("graph/membership-ordered-window").await;
    for (id, name) in [(1, "b"), (2, "c"), (3, "d")] {
        shard
            .set_vertex_metadata(
                CELL,
                id,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("name", VertexPropertyValue::String(name.into())),
            )
            .await
            .expect("seed entity");
    }
    // An index entry for vertex 9 that sorts first but has no record.
    let stale = VertexPropertyValue::String("a".into());
    put_raw(
        &shard,
        &keys::vertex_property_index(CELL, "name", &crate::codec::encode_vertex_property_value_key(&stale), 9),
        &crate::codec::encode_u64(9),
    )
    .await;

    let result = shard
        .execute_cypher_rows(
            QueryContext::new(CELL, "vertex-membership-ordered").with_cypher_engine(CypherEngineMode::Experimental),
            "MATCH (n:Entity) RETURN n.id AS id, n.name AS name ORDER BY n.name LIMIT 2",
        )
        .await
        .expect("ordered window");
    let rows = result
        .rows
        .iter()
        .map(|row| (row.values[0].clone(), row.values[1].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            (
                QueryValue::VertexId(1),
                QueryValue::Property(VertexPropertyValue::String("b".into()))
            ),
            (
                QueryValue::VertexId(2),
                QueryValue::Property(VertexPropertyValue::String("c".into()))
            ),
        ]
    );
    shard.close().await.expect("close graph shard");
}

/// An edge write whose commit fails publishes neither the edge nor the
/// endpoint records it staged. A second writer fences the first, so the
/// first writer's transaction cannot commit.
#[tokio::test]
async fn a_failed_commit_publishes_no_endpoints() {
    let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/membership-fenced";
    let first = GraphShard::open_standalone_writer(path, Arc::clone(&store))
        .await
        .expect("open first writer");
    first.write_edge(edge(1, 2, "first-1")).await.expect("first write");

    let second = GraphShard::open_standalone_writer(path, store).await.expect("open second writer");
    first
        .write_edge(edge(5, 6, "fenced-1"))
        .await
        .expect_err("a fenced writer cannot commit");

    assert_eq!(record(&second, 5).await, None);
    assert_eq!(record(&second, 6).await, None);
    assert_eq!(all_nodes(&second).await, vec![1, 2]);
    second.close().await.expect("close graph shard");
}

/// A node seeded by `WHERE n.id = X` instead of an inline `{id: X}` goes
/// through the same membership check in the row matcher. The mutation route
/// rejects a bare `MATCH (n) WHERE n.id = X` write, so the legacy read route
/// is where this seed is reachable.
#[tokio::test]
async fn a_where_seeded_id_matches_only_an_existing_node_in_the_row_matcher() {
    let shard = shard("graph/membership-where-seed").await;
    shard.write_edge(edge(10, 20, "edge-1")).await.expect("write edge");

    let legacy = |query: &'static str| {
        let shard = &shard;
        async move {
            shard
                .execute_cypher_rows(
                    QueryContext::new(CELL, "vertex-membership-where").with_cypher_engine(CypherEngineMode::Legacy),
                    query,
                )
                .await
                .unwrap_or_else(|error| panic!("legacy route rejected {query}: {error}"))
                .rows
        }
    };
    assert!(legacy("MATCH (n) WHERE n.id = 99 RETURN n.id AS id").await.is_empty());
    assert_eq!(legacy("MATCH (n) WHERE n.id = 20 RETURN n.id AS id").await.len(), 1);
    shard.close().await.expect("close graph shard");
}

/// A zero-length variable hop from an absent id binds nothing for a write.
#[tokio::test]
async fn a_zero_hop_write_from_an_absent_id_does_nothing() {
    let shard = shard("graph/membership-zero-hop-write").await;
    shard.write_edge(edge(10, 20, "edge-1")).await.expect("write edge");

    mutate(&shard, "MATCH (a {id: 99})-[:LINK*0..1]->(b) SET b.name = 'ghost'").await;
    assert_eq!(record(&shard, 99).await, None);
    mutate(&shard, "MATCH (a {id: 10})-[:LINK*0..1]->(b) SET b.seen = true").await;
    assert_eq!(
        record(&shard, 20).await,
        Some(VertexMetadata::default().with_property("seen", VertexPropertyValue::Bool(true))),
        "an existing source still reaches its neighbour"
    );
    shard.close().await.expect("close graph shard");
}

/// An anonymous id-only node binds no variable, so its row carries no id to
/// check. It must still match only an existing vertex: an absent one empties
/// the whole match, for reads and for the writes it drives.
#[tokio::test]
async fn an_anonymous_absent_node_empties_the_match() {
    let shard = shard("graph/membership-anonymous").await;
    shard
        .set_vertex_metadata(CELL, 20, VertexMetadata::default().with_label("Probe"))
        .await
        .expect("seed node 20");

    let read = shard
        .execute_cypher_rows(
            QueryContext::new(CELL, "vertex-membership-anonymous").with_cypher_engine(CypherEngineMode::Legacy),
            "MATCH ({id: 99}), (n {id: 20}) RETURN n.id AS id",
        )
        .await
        .expect("anonymous read");
    assert!(read.rows.is_empty(), "absent anonymous node matched: {:?}", read.rows);
    // An existing anonymous node with the wrong label does not match either.
    let mislabelled = shard
        .execute_cypher_rows(
            QueryContext::new(CELL, "vertex-membership-anonymous-label").with_cypher_engine(CypherEngineMode::Legacy),
            "MATCH (:Other {id: 20}), (n {id: 20}) RETURN n.id AS id",
        )
        .await
        .expect("anonymous labelled read");
    assert!(
        mislabelled.rows.is_empty(),
        "mislabelled anonymous node matched: {:?}",
        mislabelled.rows
    );

    mutate(&shard, "MATCH ({id: 99}), (n {id: 20}) SET n.name = 'changed'").await;
    assert_eq!(record(&shard, 20).await, Some(VertexMetadata::default().with_label("Probe")));

    // An existing anonymous node still lets the match through.
    mutate(&shard, "MATCH ({id: 20}), (n {id: 20}) SET n.name = 'kept'").await;
    assert_eq!(
        record(&shard, 20).await,
        Some(
            VertexMetadata::default()
                .with_label("Probe")
                .with_property("name", VertexPropertyValue::String("kept".into()))
        )
    );
    shard.close().await.expect("close graph shard");
}
