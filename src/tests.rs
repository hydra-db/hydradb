use super::*;
use futures::StreamExt;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;

async fn open_test_shard(path: &str, object_store: Arc<dyn ObjectStore>) -> GraphShard {
    GraphShard::open_standalone_writer(path, object_store)
        .await
        .unwrap()
}

/// A placement view over a hand-picked fleet, for tests whose subject is not
/// placement.
///
/// No store and no heartbeats are involved: before its first refresh a view is
/// `Grace(members)` — decision 8's assume-the-configured-fleet posture — so
/// ownership is simply the rendezvous winner over exactly `members`.
fn placement_over(local_node_id: &str, members: &[&str]) -> PlacementView {
    PlacementView::new(
        local_node_id,
        members.iter().copied(),
        PlacementConfig::default(),
    )
    .expect("a valid test fleet")
}

/// The view a node holds when it believes it is the whole fleet: it owns every
/// cell and promotes on demand, which is what every routed cluster did before
/// ownership was checked at all.
///
/// Two clusters built this way are two nodes with *skewed* views, each certain
/// it is alone — which is exactly the situation SlateDB's writer epoch has to
/// survive, and the reason the fencing tests below use it rather than a shared
/// fleet in which rendezvous would let only one of them promote.
fn sole_writer_placement(local_node_id: &str) -> PlacementView {
    placement_over(local_node_id, &[local_node_id])
}

/// Open options whose fenced-writer wait is measured in milliseconds.
///
/// The production value is 5s (decision 5), and it is a real wait: touch point
/// (d) paces a fenced writer rather than letting it re-open at once. A test that
/// drives a fence through the promotion gate therefore serves that wait for
/// real, and at the default it would spend five seconds asleep to observe a
/// state machine that takes microseconds to decide. The pacing is still
/// exercised in full; only its magnitude changes.
fn fast_fence_options() -> GraphOpenOptions {
    GraphOpenOptions {
        fence_backoff_interval: std::time::Duration::from_millis(20),
        ..GraphOpenOptions::default()
    }
}

#[tokio::test]
async fn durable_reader_refreshes_to_writer_sequence_without_a_controller() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/slatedb-native-reader-refresh";
    let writer = open_test_shard(path, Arc::clone(&object_store)).await;
    writer
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "reader-base"))
        .await
        .unwrap();

    let reader = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    reader.refresh_storage_sequence("cell-a").await.unwrap();
    assert!(reader.edge_exists("cell-a", "CHAIN", 1, 2).await.unwrap());

    let committed = writer
        .write_edge(typed_mutation("cell-a", "CHAIN", 2, 3, "reader-tail"))
        .await
        .unwrap();
    let refreshed = reader.refresh_storage_sequence("cell-a").await.unwrap();
    assert!(refreshed >= committed.epoch);
    assert!(reader.edge_exists("cell-a", "CHAIN", 2, 3).await.unwrap());

    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn refreshed_reader_query_ignores_a_stale_local_writer_view() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/strong-reader-after-writer-handoff";
    let first_writer = open_test_shard(path, Arc::clone(&object_store)).await;
    first_writer
        .write_edge(typed_mutation(
            "cell-a",
            "CHAIN",
            1,
            2,
            "writer-handoff-base",
        ))
        .await
        .unwrap();

    let second_writer = open_test_shard(path, object_store).await;
    second_writer
        .write_edge(typed_mutation(
            "cell-a",
            "CHAIN",
            2,
            3,
            "writer-handoff-tail",
        ))
        .await
        .unwrap();

    first_writer
        .refresh_storage_sequence("cell-a")
        .await
        .unwrap();
    let rows = first_writer
        .execute_cypher_rows(
            QueryContext::new("cell-a", "strong-reader-after-writer-handoff")
                .with_refreshed_reader(),
            "MATCH (u {id: 2})-[:CHAIN]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(3)])],
        )
    );

    second_writer.close().await.unwrap();
    let _ = first_writer.close().await;
}

#[tokio::test]
async fn graph_index_generations_are_isolated_by_cell() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/index-cell-isolation", object_store).await;
    shard
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "cell-a-edge"))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation("cell-b", "CHAIN", 10, 20, "cell-b-edge"))
        .await
        .unwrap();

    let cell_a = shard.build_graph_index("cell-a", "CHAIN").await.unwrap();
    let cell_b = shard.build_graph_index("cell-b", "CHAIN").await.unwrap();
    assert_ne!(cell_a.generation, cell_b.generation);
    assert_eq!(
        shard.current_graph_index("cell-a", "CHAIN").await.unwrap(),
        Some(cell_a)
    );
    assert_eq!(
        shard.current_graph_index("cell-b", "CHAIN").await.unwrap(),
        Some(cell_b)
    );
}

#[tokio::test]
async fn graph_index_gc_keeps_current_and_bounded_previous_generations() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/index-generation-gc";
    let shard = open_test_shard(path, Arc::clone(&object_store)).await;
    for (index, (src, dst)) in [(1, 2), (2, 3), (3, 4)].into_iter().enumerate() {
        shard
            .write_edge(typed_mutation(
                "cell-a",
                "CHAIN",
                src,
                dst,
                &format!("index-gc-{index}"),
            ))
            .await
            .unwrap();
        shard.build_graph_index("cell-a", "CHAIN").await.unwrap();
    }

    assert_eq!(
        shard
            .gc_graph_index_generations("cell-a", "CHAIN", 1)
            .await
            .unwrap(),
        1
    );
    let prefix: slatedb::object_store::path::Path =
        format!("{path}/_graph_index/cell-a/CHAIN/generations").into();
    let generations = object_store
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(generations.len(), 2);
}

#[tokio::test]
async fn graph_index_query_recovers_when_gc_removes_its_selected_generation() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/index-generation-gc-query-race";
    let shard = open_test_shard(path, object_store).await;
    shard
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "gc-race-base"))
        .await
        .unwrap();
    let selected = shard.build_graph_index("cell-a", "CHAIN").await.unwrap();

    shard
        .write_edge(typed_mutation("cell-a", "CHAIN", 2, 3, "gc-race-next"))
        .await
        .unwrap();
    let current = shard.build_graph_index("cell-a", "CHAIN").await.unwrap();
    assert!(current.base_sequence > selected.base_sequence);
    assert_eq!(
        shard
            .gc_graph_index_generations("cell-a", "CHAIN", 0)
            .await
            .unwrap(),
        1
    );
    assert!(shard.graph_index_csc(&selected).await.unwrap().is_none());

    let snapshot = shard.snapshot("cell-a").await.unwrap();
    let read_sequence = snapshot.read_epoch();
    let compiled = crate::GraphStore::scope_snapshot(
        Arc::clone(&snapshot.storage_snapshot),
        shard.compiled_graphblas_query_snapshot(
            "cell-a",
            "CHAIN",
            selected.base_sequence,
            read_sequence,
            &crate::shard::QueryBudget::new(None, None),
        ),
    )
    .await
    .unwrap();
    assert!(compiled.is_some());

    let traversal = snapshot
        .matrix_reachable_with_kernel("CHAIN", &[1], 2, SparseKernelBackend::SuiteSparse)
        .await
        .unwrap();
    assert_eq!(traversal.vertices, vec![2, 3]);
}

#[tokio::test]
async fn concurrent_indexers_publish_and_gc_without_regressing_current_generation() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/concurrent-index-publish-gc";
    let writer = open_test_shard(path, Arc::clone(&object_store)).await;
    writer
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "index-base"))
        .await
        .unwrap();

    let indexer_a = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    let indexer_b = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    let indexer_c = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();

    let mut last_sequence = 0;
    for round in 0..4_u64 {
        if round > 0 {
            writer
                .write_edge(typed_mutation(
                    "cell-a",
                    "CHAIN",
                    round + 1,
                    round + 2,
                    &format!("index-round-{round}"),
                ))
                .await
                .unwrap();
        }
        for indexer in [&indexer_a, &indexer_b, &indexer_c] {
            indexer.refresh_storage_sequence("cell-a").await.unwrap();
        }

        let (a, b, c) = tokio::join!(
            indexer_a.build_graph_index("cell-a", "CHAIN"),
            indexer_b.build_graph_index("cell-a", "CHAIN"),
            indexer_c.build_graph_index("cell-a", "CHAIN"),
        );
        let generations = [a.unwrap(), b.unwrap(), c.unwrap()];
        let current = indexer_a
            .current_graph_index("cell-a", "CHAIN")
            .await
            .unwrap()
            .unwrap();
        assert!(current.base_sequence > last_sequence);
        assert!(generations
            .iter()
            .all(|generation| generation.base_sequence == current.base_sequence));
        last_sequence = current.base_sequence;

        let (a, b, c) = tokio::join!(
            indexer_a.gc_graph_index_generations("cell-a", "CHAIN", 1),
            indexer_b.gc_graph_index_generations("cell-a", "CHAIN", 1),
            indexer_c.gc_graph_index_generations("cell-a", "CHAIN", 1),
        );
        a.unwrap();
        b.unwrap();
        c.unwrap();
        assert_eq!(
            indexer_b
                .current_graph_index("cell-a", "CHAIN")
                .await
                .unwrap(),
            Some(current)
        );
    }

    let prefix: slatedb::object_store::path::Path =
        format!("{path}/_graph_index/cell-a/CHAIN/generations").into();
    let generations = object_store
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert!(!generations.is_empty());
    assert!(generations.len() <= 2);

    indexer_a.close().await.unwrap();
    indexer_b.close().await.unwrap();
    indexer_c.close().await.unwrap();
    writer.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn graph_index_wal_tail_fails_at_its_bound_and_recovers_after_reindexing() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/index-wal-tail-bound";
    let limits = GraphLimits {
        max_query_scan_edges: 2,
        ..GraphLimits::default()
    };
    let writer = GraphShard::open_standalone_writer_with_limits(
        path,
        Arc::clone(&object_store),
        limits.clone(),
    )
    .await
    .unwrap();
    writer
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "tail-base"))
        .await
        .unwrap();

    let indexer = GraphShard::open_with_options(
        path,
        Arc::clone(&object_store),
        GraphOpenOptions {
            limits,
            ..GraphOpenOptions::default()
        },
    )
    .await
    .unwrap();
    indexer.refresh_storage_sequence("cell-a").await.unwrap();
    indexer.build_graph_index("cell-a", "CHAIN").await.unwrap();

    for (src, dst, key) in [(10, 11, "tail-1"), (20, 21, "tail-2")] {
        writer
            .write_edge(typed_mutation("cell-a", "CHAIN", src, dst, key))
            .await
            .unwrap();
    }
    indexer.refresh_storage_sequence("cell-a").await.unwrap();
    let within_limit = indexer
        .execute_cypher_rows(
            QueryContext::new("cell-a", "tail-within-limit"),
            "MATCH ({id: 1})-[:CHAIN*1..1]->(v) RETURN count(*) AS total",
        )
        .await
        .unwrap();
    assert_eq!(within_limit.rows[0].values, vec![QueryValue::Count(1)]);

    writer
        .write_edge(typed_mutation("cell-a", "CHAIN", 30, 31, "tail-overflow"))
        .await
        .unwrap();
    indexer.refresh_storage_sequence("cell-a").await.unwrap();
    let overflow = indexer
        .execute_cypher_rows(
            QueryContext::new("cell-a", "tail-overflow"),
            "MATCH ({id: 1})-[:CHAIN*1..1]->(v) RETURN count(*) AS total",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        overflow,
        GraphError::AdmissionRejected {
            operation: "graph_index_wal_affected_edges",
            actual: 3,
            limit: 2,
        }
    ));

    indexer.build_graph_index("cell-a", "CHAIN").await.unwrap();
    let recovered = indexer
        .execute_cypher_rows(
            QueryContext::new("cell-a", "tail-reindexed"),
            "MATCH ({id: 1})-[:CHAIN*1..1]->(v) RETURN count(*) AS total",
        )
        .await
        .unwrap();
    assert_eq!(recovered.rows[0].values, vec![QueryValue::Count(1)]);

    indexer.close().await.unwrap();
    writer.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_graphblas_applies_wal_tail_after_edge_changes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/cypher-graphblas-snapshot-rebuild";
    let writer = open_test_shard(path, Arc::clone(&object_store)).await;

    for (index, (src, dst)) in [(1, 2), (2, 3), (1, 4)].into_iter().enumerate() {
        writer
            .write_edge(typed_mutation(
                "reddit-home",
                "CHAIN",
                src,
                dst,
                &format!("snapshot-base-{index}"),
            ))
            .await
            .unwrap();
    }
    let base_epoch = writer.current_epoch("reddit-home").await.unwrap();
    let indexer = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    indexer
        .refresh_storage_sequence("reddit-home")
        .await
        .unwrap();
    let index = indexer
        .build_graph_index("reddit-home", "CHAIN")
        .await
        .unwrap();
    assert_eq!(index.base_sequence, base_epoch);
    writer
        .write_edge(typed_mutation(
            "reddit-home",
            "CHAIN",
            3,
            5,
            "snapshot-plus",
        ))
        .await
        .unwrap();
    writer
        .delete_edge(typed_mutation(
            "reddit-home",
            "CHAIN",
            1,
            4,
            "snapshot-minus",
        ))
        .await
        .unwrap();

    let reader = GraphShard::open(path, object_store).await.unwrap();
    reader
        .refresh_storage_sequence("reddit-home")
        .await
        .unwrap();
    let rows = reader
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "snapshot-rebuild-read"),
            "MATCH (u {id: 1})-[:CHAIN*1..3]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(3)]),
                QueryRow::new(vec![QueryValue::VertexId(5)]),
            ],
        )
    );
    let metrics = reader.graph_operational_metrics();
    assert_eq!(metrics.query_graphblas_artifact_snapshots, 1);
    assert_eq!(metrics.query_graphblas_rebuilt_snapshots, 0);
    assert_eq!(metrics.query_rust_sparse_fallbacks, 0);

    let cached_rows = reader
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "snapshot-rebuild-cached-read"),
            "MATCH (u {id: 1})-[:CHAIN*1..3]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(cached_rows, rows);
    let cached_metrics = reader.graph_operational_metrics();
    assert_eq!(cached_metrics.query_graphblas_rebuilt_snapshots, 0);
    assert_eq!(cached_metrics.query_graphblas_artifact_snapshots, 2);

    let writer_rows = writer
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "snapshot-writer-local-read"),
            "MATCH (u {id: 1})-[:CHAIN*1..3]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(writer_rows, rows);
    let writer_metrics = writer.graph_operational_metrics();
    assert_eq!(writer_metrics.query_graphblas_artifact_snapshots, 1);
    assert_eq!(writer_metrics.query_graphblas_rebuilt_snapshots, 0);
    assert_eq!(writer_metrics.query_rust_sparse_fallbacks, 0);

    reader.close().await.unwrap();
    indexer.close().await.unwrap();
    writer.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn native_sp_paths_uses_one_pinned_graphblas_snapshot_with_wal_tail() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/native-sp-paths-pinned-tail";
    let writer = open_test_shard(path, Arc::clone(&object_store)).await;
    for (index, (src, dst)) in [(1, 2), (2, 3), (1, 4)].into_iter().enumerate() {
        writer
            .write_edge(typed_mutation(
                "cell-a",
                "CHAIN",
                src,
                dst,
                &format!("native-path-base-{index}"),
            ))
            .await
            .unwrap();
    }
    let indexer = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    indexer.refresh_storage_sequence("cell-a").await.unwrap();
    indexer.build_graph_index("cell-a", "CHAIN").await.unwrap();

    writer
        .write_edge(typed_mutation(
            "cell-a",
            "CHAIN",
            3,
            5,
            "native-path-tail-add",
        ))
        .await
        .unwrap();
    writer
        .delete_edge(typed_mutation(
            "cell-a",
            "CHAIN",
            1,
            4,
            "native-path-tail-delete",
        ))
        .await
        .unwrap();

    let reader = GraphShard::open(path, object_store).await.unwrap();
    reader.refresh_storage_sequence("cell-a").await.unwrap();
    let result = reader
        .execute_cypher_rows(
            QueryContext::new("cell-a", "native-sp-paths"),
            "CALL algo.SPpaths({sourceNode: 5, targetNode: 1, relTypes: ['CHAIN'], \
             maxLen: 4, relDirection: 'both', pathCount: 1}) \
             YIELD path, pathWeight, pathCost RETURN path, pathWeight, pathCost",
        )
        .await
        .unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.rows.len(), 1);
    let QueryValue::Path(path) = &result.rows[0].values[0] else {
        panic!("native procedure must return a Bolt-compatible path value");
    };
    assert_eq!(
        path.nodes.iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![5, 3, 2, 1]
    );
    assert_eq!(
        path.relationships
            .iter()
            .map(|relationship| (relationship.src, relationship.dst))
            .collect::<Vec<_>>(),
        vec![(3, 5), (2, 3), (1, 2)]
    );
    assert_eq!(result.rows[0].values[1], QueryValue::Count(3));
    assert_eq!(result.rows[0].values[2], QueryValue::Count(0));
    let metrics = reader.graph_operational_metrics();
    assert_eq!(metrics.query_graphblas_artifact_snapshots, 1);
    assert_eq!(metrics.query_rust_sparse_fallbacks, 0);

    reader.close().await.unwrap();
    indexer.close().await.unwrap();
    writer.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn native_sp_paths_honors_weight_cost_and_parameterized_limits() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/native-sp-paths-weighted", object_store).await;
    for (index, query) in [
        "CREATE (a {id: 1})-[:ROUTE {weight: 1, cost: 5}]->(b {id: 2})",
        "CREATE (a {id: 2})-[:ROUTE {weight: 1, cost: 5}]->(b {id: 4})",
        "CREATE (a {id: 1})-[:ROUTE {weight: 2, cost: 1}]->(b {id: 3})",
        "CREATE (a {id: 3})-[:ROUTE {weight: 2, cost: 1}]->(b {id: 4})",
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .execute_cypher(
                QueryContext::new("cell-a", format!("native-weighted-create-{index}")),
                query,
            )
            .await
            .unwrap();
    }
    shard.build_graph_index("cell-a", "ROUTE").await.unwrap();

    let result = shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "native-weighted-read").with_parameters(BTreeMap::from([
                ("source".to_string(), VertexPropertyValue::Integer(1)),
                ("target".to_string(), VertexPropertyValue::Integer(4)),
                ("max_len".to_string(), VertexPropertyValue::Integer(3)),
                (
                    "weight_property".to_string(),
                    VertexPropertyValue::String("weight".to_string()),
                ),
                ("max_cost".to_string(), VertexPropertyValue::Integer(5)),
                ("path_count".to_string(), VertexPropertyValue::Integer(1)),
            ])),
            "CALL algo.SPpaths({sourceNode: $source, targetNode: $target, \
             relTypes: ['ROUTE'], maxLen: $max_len, weightProp: $weight_property, \
             costProp: 'cost', maxCost: $max_cost, pathCount: $path_count}) \
             YIELD path, pathWeight, pathCost RETURN path, pathWeight, pathCost",
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    let QueryValue::Path(path) = &result.rows[0].values[0] else {
        panic!("weighted procedure must return a path");
    };
    assert_eq!(
        path.nodes.iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![1, 3, 4]
    );
    assert_eq!(result.rows[0].values[1], QueryValue::Count(4));
    assert_eq!(result.rows[0].values[2], QueryValue::Count(2));

    let single_source = shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "native-single-source-read"),
            "CALL algo.SSpaths({sourceNode: 1, relTypes: ['ROUTE'], maxLen: 1, \
             relDirection: 'outgoing', pathCount: 10}) \
             YIELD path RETURN path",
        )
        .await
        .unwrap();
    assert_eq!(single_source.rows.len(), 2);
    assert_eq!(
        single_source
            .rows
            .iter()
            .map(|row| match &row.values[0] {
                QueryValue::Path(path) => path.nodes[1].id,
                value => panic!("single-source procedure returned {value:?}"),
            })
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn native_ms_paths_resolves_indexed_sources_in_one_pinned_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/native-ms-paths", object_store).await;
    for (index, query) in [
        "CREATE (a:Entity {id: 1, name: 'alpha', entity_id: 'a'})-[:RELATES {relationship_id: 'ab'}]->(b:Entity {id: 2, name: 'beta', entity_id: 'b'})",
        "CREATE (a:Entity {id: 1, name: 'alpha', entity_id: 'a'})-[:RELATES {relationship_id: 'ab-2'}]->(b:Entity {id: 2, name: 'beta', entity_id: 'b'})",
        "CREATE (a:Entity {id: 2, name: 'beta', entity_id: 'b'})-[:RELATES {relationship_id: 'bc'}]->(b:Entity {id: 3, name: 'gamma', entity_id: 'c'})",
        "CREATE (a:Other {id: 4, name: 'alpha', entity_id: 'wrong-label'})-[:IGNORED]->(b:Entity {id: 1, name: 'alpha', entity_id: 'a'})",
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .execute_cypher(
                QueryContext::new("cell-a", format!("native-ms-create-{index}")),
                query,
            )
            .await
            .unwrap();
    }
    shard.build_graph_index("cell-a", "RELATES").await.unwrap();
    let pinned_snapshot = shard.db.snapshot().await.unwrap();
    let before = shard.graph_operational_metrics();

    let result = shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "native-ms-read"),
            "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', \
             sourceValues: ['alpha', 'beta', 'gamma'], targetValues: ['alpha', 'beta', 'gamma'], \
             pairwise: true, relTypes: ['RELATES'], maxLen: 2, relDirection: 'both', \
             pathCount: 1, fairRelationshipVariants: true, resultLimit: 10}) \
             YIELD path RETURN path",
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 5);
    let endpoints = result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            QueryValue::Path(path) => {
                let first = path.nodes.first().expect("path source").id;
                let last = path.nodes.last().expect("path target").id;
                (first.min(last), first.max(last))
            }
            value => panic!("multi-source procedure returned {value:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(endpoints, BTreeSet::from([(1, 2), (1, 3), (2, 3)]));
    let direct_relationships = result
        .rows
        .iter()
        .filter_map(|row| match &row.values[0] {
            QueryValue::Path(path)
                if path.nodes.first().map(|node| node.id) == Some(1)
                    && path.nodes.last().map(|node| node.id) == Some(2) =>
            {
                path.relationships[0].properties.get("relationship_id")
            }
            _ => None,
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        direct_relationships,
        BTreeSet::from([
            VertexPropertyValue::String("ab".to_string()),
            VertexPropertyValue::String("ab-2".to_string()),
        ]),
        "MSpaths must preserve relationship-distinct paths"
    );
    assert!(result.rows.iter().all(|row| match &row.values[0] {
        QueryValue::Path(path) => path.nodes.iter().all(|node| node.id != 4),
        _ => false,
    }));
    let after = shard.graph_operational_metrics();
    assert_eq!(
        after
            .query_graphblas_artifact_snapshots
            .saturating_sub(before.query_graphblas_artifact_snapshots),
        1,
        "one batched procedure should load one compiled topology snapshot"
    );

    shard
        .set_vertex_metadata(
            "cell-a",
            1,
            VertexMetadata::default()
                .with_label("Entity")
                .with_property("id", VertexPropertyValue::Integer(1))
                .with_property(
                    "name",
                    VertexPropertyValue::String("renamed-alpha".to_string()),
                )
                .with_property("entity_id", VertexPropertyValue::String("a".to_string())),
        )
        .await
        .unwrap();
    let historical = crate::GraphStore::scope_snapshot(
        pinned_snapshot,
        shard.execute_cypher_rows(
            QueryContext::new("cell-a", "native-ms-historical-selector"),
            "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', \
             sourceValues: ['alpha'], targetValues: ['beta'], pairwise: false, \
             relTypes: ['RELATES'], maxLen: 1, relDirection: 'both', pathCount: 1, \
             resultLimit: 10}) YIELD path RETURN path",
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        historical.rows.len(),
        1,
        "selector discovery must use the same pinned snapshot as traversal"
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn native_ms_paths_fairly_budgets_parallel_variants_across_structures() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/native-ms-paths-fair-variants", object_store).await;
    for (index, query) in [
        "CREATE (a:Entity {id: 1, name: 'alpha', entity_id: 'a'})-[:RELATES {relationship_id: 'ab-1'}]->(b:Entity {id: 2, name: 'beta', entity_id: 'b'})",
        "CREATE (a:Entity {id: 1, name: 'alpha', entity_id: 'a'})-[:RELATES {relationship_id: 'ab-2'}]->(b:Entity {id: 2, name: 'beta', entity_id: 'b'})",
        "CREATE (a:Entity {id: 1, name: 'alpha', entity_id: 'a'})-[:RELATES {relationship_id: 'ab-3'}]->(b:Entity {id: 2, name: 'beta', entity_id: 'b'})",
        "CREATE (a:Entity {id: 1, name: 'alpha', entity_id: 'a'})-[:RELATES {relationship_id: 'ab-4'}]->(b:Entity {id: 2, name: 'beta', entity_id: 'b'})",
        "CREATE (a:Entity {id: 2, name: 'beta', entity_id: 'b'})-[:RELATES {relationship_id: 'bc'}]->(b:Entity {id: 3, name: 'gamma', entity_id: 'c'})",
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .execute_cypher(
                QueryContext::new("cell-a", format!("native-ms-fair-create-{index}")),
                query,
            )
            .await
            .unwrap();
    }
    shard.build_graph_index("cell-a", "RELATES").await.unwrap();

    let result = shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "native-ms-fair-read"),
            "CALL algo.MSpaths({sourceLabel: 'Entity', sourceProperty: 'name', \
             sourceValues: ['alpha', 'beta', 'gamma'], targetValues: ['alpha', 'beta', 'gamma'], \
             pairwise: true, relTypes: ['RELATES'], maxLen: 1, relDirection: 'both', \
             pathCount: 1, fairRelationshipVariants: true, resultLimit: 3}) \
             YIELD path RETURN path",
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    let endpoints = result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            QueryValue::Path(path) => {
                let first = path.nodes.first().expect("path source").id;
                let last = path.nodes.last().expect("path target").id;
                (first.min(last), first.max(last))
            }
            value => panic!("multi-source procedure returned {value:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        endpoints,
        BTreeSet::from([(1, 2), (2, 3)]),
        "one dense pair must not consume the complete native result budget"
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn native_sp_paths_preserves_parallel_relationship_identity_and_scores() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/native-sp-paths-parallel", object_store).await;
    for (index, query) in [
        "CREATE (a {id: 1})-[:ROUTE {weight: 9}]->(b {id: 2})",
        "CREATE (a {id: 1})-[:ROUTE {weight: 1, cost: 10}]->(b {id: 2})",
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .execute_cypher(
                QueryContext::new("cell-a", format!("native-parallel-create-{index}")),
                query,
            )
            .await
            .unwrap();
    }
    shard
        .set_edge_metadata(
            "cell-a",
            "ROUTE",
            1,
            2,
            EdgeMetadata::default()
                .with_property("weight", VertexPropertyValue::Integer(100))
                .with_property("cost", VertexPropertyValue::Integer(90))
                .with_property(
                    "shared",
                    VertexPropertyValue::String("structural".to_string()),
                ),
        )
        .await
        .unwrap();
    shard.build_graph_index("cell-a", "ROUTE").await.unwrap();

    let result = shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "native-parallel-read"),
            "CALL algo.SPpaths({sourceNode: 1, targetNode: 2, relTypes: ['ROUTE'], \
             maxLen: 1, weightProp: 'weight', costProp: 'cost', pathCount: 3}) \
             YIELD path, pathWeight, pathCost RETURN path, pathWeight, pathCost",
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    let relationship_ids = result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            QueryValue::Path(path) => path.relationships[0]
                .id
                .expect("parallel relationship path must retain its identity"),
            value => panic!("native procedure returned {value:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(relationship_ids.len(), 2);
    for row in &result.rows {
        let QueryValue::Path(path) = &row.values[0] else {
            panic!("native procedure must return paths");
        };
        assert_eq!(
            path.relationships[0].properties.get("shared"),
            Some(&VertexPropertyValue::String("structural".to_string()))
        );
    }
    assert_eq!(result.rows[0].values[1], QueryValue::Count(1));
    assert_eq!(result.rows[0].values[2], QueryValue::Count(10));
    assert_eq!(result.rows[1].values[1], QueryValue::Count(9));
    assert_eq!(result.rows[1].values[2], QueryValue::Count(90));

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn native_path_pages_remain_on_the_first_page_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/native-path-page-snapshot", object_store).await;
    for (index, dst) in [2, 3, 4].into_iter().enumerate() {
        shard
            .write_edge(typed_mutation(
                "cell-a",
                "ROUTE",
                1,
                dst,
                &format!("native-page-seed-{index}"),
            ))
            .await
            .unwrap();
    }
    let query = "CALL algo.SSpaths({sourceNode: 1, relTypes: ['ROUTE'], maxLen: 1, \
                 pathCount: 10}) YIELD path RETURN path";
    let first = shard
        .execute_cypher_rows_page(
            QueryContext::new("cell-a", "native-page-first"),
            query,
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!(native_path_target(&first.rows[0]), 2);
    let cursor = first.next_cursor.expect("three rows require another page");

    shard
        .delete_edge(typed_mutation(
            "cell-a",
            "ROUTE",
            1,
            3,
            "native-page-delete",
        ))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation(
            "cell-a",
            "ROUTE",
            1,
            0,
            "native-page-insert",
        ))
        .await
        .unwrap();

    let second = shard
        .execute_cypher_rows_page(
            QueryContext::new("cell-a", "native-page-second"),
            query,
            Some(cursor),
            1,
        )
        .await
        .unwrap();
    assert_eq!(native_path_target(&second.rows[0]), 3);
    let cursor = second.next_cursor.expect("one original row remains");
    let third = shard
        .execute_cypher_rows_page(
            QueryContext::new("cell-a", "native-page-third"),
            query,
            Some(cursor),
            1,
        )
        .await
        .unwrap();
    assert_eq!(native_path_target(&third.rows[0]), 4);
    assert!(third.next_cursor.is_none());

    let current = shard
        .execute_cypher_rows(QueryContext::new("cell-a", "native-page-current"), query)
        .await
        .unwrap();
    assert_eq!(
        current
            .rows
            .iter()
            .map(native_path_target)
            .collect::<Vec<_>>(),
        vec![0, 2, 4]
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
fn native_path_target(row: &QueryRow) -> VertexId {
    match &row.values[0] {
        QueryValue::Path(path) => path.nodes.last().expect("path has a target").id,
        value => panic!("native procedure returned {value:?}"),
    }
}

#[tokio::test]
async fn graphblas_wal_tail_resolves_edges_at_the_pinned_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/index-pinned-tail", object_store).await;
    shard
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "pinned-base"))
        .await
        .unwrap();
    shard.build_graph_index("cell-a", "CHAIN").await.unwrap();
    shard
        .delete_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "pinned-delete"))
        .await
        .unwrap();
    let pinned = shard.snapshot("cell-a").await.unwrap();

    shard
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "pinned-readd"))
        .await
        .unwrap();

    let traversal = pinned
        .matrix_reachable_with_kernel("CHAIN", &[1], 1, SparseKernelBackend::SuiteSparse)
        .await
        .unwrap();
    assert!(traversal.vertices.is_empty());
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_cold_graphblas_snapshot_does_not_reacquire_compilation_gate() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/cypher-cold-graphblas-snapshot";
    let writer = open_test_shard(path, Arc::clone(&object_store)).await;
    writer
        .write_edge(typed_mutation("reddit-home", "CHAIN", 1, 2, "base-1"))
        .await
        .unwrap();
    writer
        .write_edge(typed_mutation("reddit-home", "CHAIN", 2, 3, "base-2"))
        .await
        .unwrap();
    let base_epoch = writer.current_epoch("reddit-home").await.unwrap();
    let index = writer
        .build_graph_index("reddit-home", "CHAIN")
        .await
        .unwrap();
    assert_eq!(index.base_sequence, base_epoch);

    // Advance the topology sequence without changing this edge type. After a
    // reopen, the query must hydrate the cold base matrix and compile the
    // current snapshot without recursively acquiring the compilation gate.
    writer
        .set_vertex_metadata(
            "reddit-home",
            99,
            VertexMetadata::default().with_label("Marker"),
        )
        .await
        .unwrap();
    assert!(writer.current_epoch("reddit-home").await.unwrap() > base_epoch);
    writer.close().await.unwrap();

    let reopened = GraphShard::open(path, object_store).await.unwrap();
    assert_eq!(reopened.graphblas_cache.lock().await.len(), 0);
    let rows = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        reopened.execute_cypher_rows(
            QueryContext::new("reddit-home", "cold-snapshot-read"),
            "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) RETURN count(*) AS total",
        ),
    )
    .await
    .expect("cold GraphBLAS snapshot compilation must not deadlock")
    .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("total")],
            vec![QueryRow::new(vec![QueryValue::Count(1)])],
        )
    );
    let metrics = reopened.graph_operational_metrics();
    assert_eq!(metrics.query_graphblas_artifact_snapshots, 1);
    assert_eq!(metrics.query_graphblas_rebuilt_snapshots, 0);
    assert_eq!(reopened.graphblas_cache.lock().await.len(), 1);
    reopened.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_graphblas_matrix_survives_interleaved_unrelated_writes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-graphblas-adjacency-generation", object_store).await;
    shard
        .write_edge(typed_mutation("reddit-home", "CHAIN", 1, 2, "base-1"))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation("reddit-home", "CHAIN", 2, 3, "base-2"))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    let index = shard
        .build_graph_index("reddit-home", "CHAIN")
        .await
        .unwrap();
    assert_eq!(index.base_sequence, base_epoch);

    for marker in 0..3 {
        shard
            .set_vertex_metadata(
                "reddit-home",
                100 + marker,
                VertexMetadata::default().with_label("Marker"),
            )
            .await
            .unwrap();
        let rows = shard
            .execute_cypher_rows(
                QueryContext::new("reddit-home", format!("unrelated-write-{marker}")),
                "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) RETURN count(*) AS total",
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            QueryResultSet::new(
                vec![QueryColumn::new("total")],
                vec![QueryRow::new(vec![QueryValue::Count(1)])],
            )
        );
    }

    let generation_key = keys::adjacency_generation("reddit-home", "CHAIN");
    assert_eq!(
        shard.read_counter(&generation_key).await.unwrap(),
        base_epoch
    );
    let metrics = shard.graph_operational_metrics();
    assert_eq!(metrics.query_graphblas_artifact_snapshots, 3);
    assert_eq!(metrics.query_graphblas_rebuilt_snapshots, 0);
    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
}

fn mutation(src: VertexId, dst: VertexId, idempotency_key: &str) -> EdgeMutation {
    EdgeMutation {
        cell_id: "reddit-home".to_string(),
        edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
        src,
        dst,
        idempotency_key: idempotency_key.to_string(),
    }
}

fn typed_mutation(
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dst: VertexId,
    idempotency_key: &str,
) -> EdgeMutation {
    EdgeMutation {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        src,
        dst,
        idempotency_key: idempotency_key.to_string(),
    }
}

#[cfg(feature = "opencypher")]
async fn read_query_stats_record_for_test(shard: &GraphShard, key: &str) -> QueryStatsRecord {
    let record_key = keys::query_stats_record_key(key);
    let value = shard
        .read_remote(&record_key)
        .await
        .unwrap_or_else(|err| panic!("read stats record {record_key} failed: {err}"))
        .unwrap_or_else(|| panic!("missing stats record {record_key}"));
    decode_query_stats_record(&record_key, &value)
        .unwrap_or_else(|err| panic!("decode stats record {record_key} failed: {err}"))
}

async fn segment_append_txn_retry_for_test(
    shard: Arc<GraphShard>,
    cell_id: &str,
    edge_type: &str,
    src: VertexId,
    dsts: Vec<VertexId>,
    idempotency_key: &str,
) -> Result<BulkImportResult> {
    let edges: Vec<_> = dsts.iter().copied().map(|dst| (src, dst)).collect();
    let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match shard
            .bulk_append_out_adjacency_segment_trusted_txn(
                cell_id,
                edge_type,
                src,
                &dsts,
                idempotency_key,
                fingerprint,
            )
            .await
        {
            Err(err)
                if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    unreachable!("transaction retry loop always returns on final attempt")
}

#[tokio::test]
async fn batch_reads_share_one_snapshot_and_preserve_input_order() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/batch-reads", object_store).await;
    shard
        .write_edge_mutations_batch(
            "reddit-home",
            [
                typed_mutation("reddit-home", "FOLLOWS", 1, 10, "batch-read-1"),
                typed_mutation("reddit-home", "FOLLOWS", 1, 11, "batch-read-2"),
                typed_mutation("reddit-home", "FOLLOWS", 2, 11, "batch-read-3"),
                typed_mutation("reddit-home", "FOLLOWS", 3, 12, "batch-read-4"),
            ],
        )
        .await
        .unwrap();
    let snapshot = shard.snapshot("reddit-home").await.unwrap();

    assert_eq!(
        shard
            .out_neighbors_batch("reddit-home", "FOLLOWS", [2, 1, 404, 1])
            .await
            .unwrap(),
        vec![
            NeighborBatchEntry {
                vertex: 2,
                neighbors: vec![11],
            },
            NeighborBatchEntry {
                vertex: 1,
                neighbors: vec![10, 11],
            },
            NeighborBatchEntry {
                vertex: 404,
                neighbors: Vec::new(),
            },
            NeighborBatchEntry {
                vertex: 1,
                neighbors: vec![10, 11],
            },
        ]
    );
    assert_eq!(
        shard
            .in_neighbors_batch("reddit-home", "FOLLOWS", [11, 12, 404])
            .await
            .unwrap(),
        vec![
            NeighborBatchEntry {
                vertex: 11,
                neighbors: vec![1, 2],
            },
            NeighborBatchEntry {
                vertex: 12,
                neighbors: vec![3],
            },
            NeighborBatchEntry {
                vertex: 404,
                neighbors: Vec::new(),
            },
        ]
    );
    assert_eq!(
        shard
            .edge_exists_batch(
                "reddit-home",
                "FOLLOWS",
                [(1, 10), (2, 10), (3, 12), (1, 10)],
            )
            .await
            .unwrap(),
        vec![
            EdgeExistenceBatchEntry {
                src: 1,
                dst: 10,
                exists: true,
            },
            EdgeExistenceBatchEntry {
                src: 2,
                dst: 10,
                exists: false,
            },
            EdgeExistenceBatchEntry {
                src: 3,
                dst: 12,
                exists: true,
            },
            EdgeExistenceBatchEntry {
                src: 1,
                dst: 10,
                exists: true,
            },
        ]
    );

    shard
        .delete_edge(typed_mutation(
            "reddit-home",
            "FOLLOWS",
            1,
            10,
            "batch-read-delete",
        ))
        .await
        .unwrap();
    assert!(snapshot.edge_exists("FOLLOWS", 1, 10).await.unwrap());
    assert!(
        !shard
            .edge_exists_batch("reddit-home", "FOLLOWS", [(1, 10)])
            .await
            .unwrap()[0]
            .exists
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_batches_multi_pattern_create_and_multi_row_delete() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-batch-mutations", object_store).await;
    let query = "CREATE ({id: 1})-[:FOLLOWS]->({id: 10}), \
                 ({id: 1})-[:FOLLOWS]->({id: 11}), \
                 ({id: 2})-[:FOLLOWS]->({id: 11})";
    assert_eq!(
        shard
            .execute_cypher(
                QueryContext::new("reddit-home", "cypher-batch-create"),
                query,
            )
            .await
            .unwrap(),
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 3,
            ..QueryMutationResult::default()
        })
    );
    assert_eq!(
        shard
            .execute_cypher(
                QueryContext::new("reddit-home", "cypher-batch-create"),
                query,
            )
            .await
            .unwrap(),
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 3,
            ..QueryMutationResult::default()
        })
    );

    assert_eq!(
        shard
            .execute_cypher(
                QueryContext::new("reddit-home", "cypher-batch-delete"),
                "MATCH (u {id: 1})-[r:FOLLOWS]->(v) DELETE r",
            )
            .await
            .unwrap(),
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 2,
            deleted_edges: 2,
            ..QueryMutationResult::default()
        })
    );
    assert_eq!(
        shard
            .out_neighbors_batch("reddit-home", "FOLLOWS", [1, 2])
            .await
            .unwrap(),
        vec![
            NeighborBatchEntry {
                vertex: 1,
                neighbors: Vec::new(),
            },
            NeighborBatchEntry {
                vertex: 2,
                neighbors: vec![11],
            },
        ]
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_rejects_metadata_multi_create_before_writing_any_edge() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-batch-metadata-reject", object_store).await;
    let error = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-batch-metadata-reject"),
            "CREATE (:User {id: 1, name: 'alice'})-[:FOLLOWS]->({id: 2}), \
             ({id: 3})-[:FOLLOWS]->({id: 4})",
        )
        .await
        .unwrap_err();
    assert!(matches!(error, GraphError::UnsupportedQuery { .. }));
    assert!(shard
        .edges_at(
            "reddit-home",
            "FOLLOWS",
            shard.current_epoch("reddit-home").await.unwrap(),
        )
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn batch_reads_enforce_configured_request_limit_before_scanning() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let limits = GraphLimits {
        max_query_intermediate_rows: 2,
        ..GraphLimits::default()
    };
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/batch-read-limits",
        object_store,
        limits,
    )
    .await
    .unwrap();
    let error = shard
        .out_neighbors_batch("reddit-home", "FOLLOWS", [1, 2, 3])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::AdmissionRejected {
            operation: "out_neighbors_batch_sources",
            actual: 3,
            limit: 2,
        }
    ));
}

#[tokio::test]
async fn batch_neighbor_reads_honor_cancellation_before_storage_scans() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/batch-read-cancel", object_store).await;
    let token = QueryCancellationToken::new();
    token.cancel();

    let error = shard
        .out_neighbors_batch_at_with_cancellation("reddit-home", "FOLLOWS", [1], 0, Some(token))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::QueryTimeout {
            operation: "query_cancelled",
            limit_ms: 0,
            ..
        }
    ));

    let token = QueryCancellationToken::new();
    token.cancel();
    let error = shard
        .in_neighbors_batch_at_with_cancellation("reddit-home", "FOLLOWS", [1], 0, Some(token))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::QueryTimeout {
            operation: "query_cancelled",
            limit_ms: 0,
            ..
        }
    ));
}

#[tokio::test]
async fn batch_reads_scope_work_to_requested_vertices() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/batch-read-scoped-vertices",
        object_store,
        GraphLimits {
            max_query_scan_edges: 2,
            ..GraphLimits::default()
        },
    )
    .await
    .unwrap();
    let mut mutations = vec![typed_mutation(
        "reddit-home",
        "FOLLOWS",
        1,
        10,
        "scoped-requested",
    )];
    mutations.extend((0..32).map(|index| {
        typed_mutation(
            "reddit-home",
            "FOLLOWS",
            100 + index,
            1_000 + index,
            &format!("scoped-unrelated-{index}"),
        )
    }));
    shard
        .write_edge_mutations_batch("reddit-home", mutations)
        .await
        .unwrap();
    let read_epoch = shard.current_epoch("reddit-home").await.unwrap();

    assert_eq!(
        shard
            .out_neighbors_batch_at("reddit-home", "FOLLOWS", [1], read_epoch)
            .await
            .unwrap()[0]
            .neighbors,
        vec![10]
    );
    assert_eq!(
        shard
            .in_neighbors_batch_at("reddit-home", "FOLLOWS", [10], read_epoch)
            .await
            .unwrap()[0]
            .neighbors,
        vec![1]
    );
    assert!(
        shard
            .edge_exists_batch_at("reddit-home", "FOLLOWS", [(1, 10)], read_epoch)
            .await
            .unwrap()[0]
            .exists
    );
}

async fn bulk_import_txn_retry_for_test(
    shard: Arc<GraphShard>,
    cell_id: &str,
    edge_type: &str,
    edges: Vec<(VertexId, VertexId)>,
    idempotency_key: &str,
    options: BulkImportOptions,
) -> Result<BulkImportResult> {
    let fingerprint = bulk_import_fingerprint(cell_id, edge_type, &edges);
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match shard
            .bulk_import_edges_txn(
                cell_id,
                edge_type,
                &edges,
                idempotency_key,
                fingerprint,
                options,
            )
            .await
        {
            Err(err)
                if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    unreachable!("transaction retry loop always returns on final attempt")
}

async fn edge_mutation_batch_txn_retry_for_test(
    shard: Arc<GraphShard>,
    cell_id: &str,
    mutations: Vec<EdgeMutation>,
    operation: &'static str,
) -> Result<EdgeMutationBatchResult> {
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match shard
            .write_edge_mutations_batch_txn(cell_id, &mutations, operation, None)
            .await
        {
            Err(err)
                if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    unreachable!("transaction retry loop always returns on final attempt")
}

async fn write_edge_txn_retry_for_test(
    shard: Arc<GraphShard>,
    mutation: EdgeMutation,
) -> Result<CommitResult> {
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match shard.write_edge_txn(&mutation).await {
            Err(err)
                if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    unreachable!("transaction retry loop always returns on final attempt")
}

async fn delete_edge_txn_retry_for_test(
    shard: Arc<GraphShard>,
    mutation: EdgeMutation,
) -> Result<DeleteResult> {
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match shard.delete_edge_txn(&mutation).await {
            Err(err)
                if is_retryable_write_conflict(&err) && attempt + 1 < GRAPH_TXN_MAX_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    unreachable!("transaction retry loop always returns on final attempt")
}

#[tokio::test]
async fn raw_graph_shard_open_is_read_only() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = open_test_shard("graph/read-only-open", Arc::clone(&object_store)).await;
    writer
        .write_edge(mutation(1, 2, "seed-reader"))
        .await
        .unwrap();
    let shard = GraphShard::open("graph/read-only-open", object_store)
        .await
        .unwrap();
    let err = shard
        .write_edge(mutation(1, 2, "read-only"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::WriteRequiresWriter {
            operation: "write_edge",
            cell_id
        } if cell_id == "reddit-home"
    ));
    assert!(shard
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
        .await
        .unwrap());
    writer
        .write_edge(mutation(1, 3, "writer-after-reader-open"))
        .await
        .unwrap();
    shard.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn read_only_shard_treats_uninitialized_store_as_empty_until_first_write() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/uninitialized-reader";
    let reader = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();

    assert!(!reader.edge_exists("cell-a", "FOLLOWS", 1, 2).await.unwrap());
    assert!(object_store.list(None).collect::<Vec<_>>().await.is_empty());

    let writer = open_test_shard(path, Arc::clone(&object_store)).await;
    let committed = writer
        .write_edge(typed_mutation("cell-a", "FOLLOWS", 1, 2, "first-write"))
        .await
        .unwrap();
    assert!(
        reader.edge_exists("cell-a", "FOLLOWS", 1, 2).await.unwrap(),
        "an ordinary read must discover initialization immediately after the durable write"
    );
    assert!(
        reader
            .wait_for_storage_sequence("cell-a", committed.epoch)
            .await
            .unwrap()
            >= committed.epoch
    );
    assert!(reader.edge_exists("cell-a", "FOLLOWS", 1, 2).await.unwrap());

    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
#[tokio::test]
async fn write_authoritative_open_rejects_relaxed_durability() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = GraphOpenOptions {
        durability: GraphDurabilityConfig::default().with_await_durable_writes(false),
        ..Default::default()
    };

    let seed = open_test_shard("graph/relaxed-durability-reader", Arc::clone(&object_store)).await;
    seed.close().await.unwrap();
    let reader = GraphShard::open_with_options(
        "graph/relaxed-durability-reader",
        Arc::clone(&object_store),
        options.clone(),
    )
    .await
    .unwrap();
    reader.close().await.unwrap();

    let err = match GraphShard::open_standalone_writer_with_options(
        "graph/relaxed-durability-writer",
        object_store,
        options,
    )
    .await
    {
        Ok(_) => panic!("write-authoritative shard accepted relaxed durability"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        GraphError::UnsafeDurabilityConfig {
            operation: "open_write_authoritative_shard",
            ref reason
        } if reason.contains("remotely durable SlateDB sequences")
    ));
}

#[tokio::test]
async fn graph_open_options_wire_slatedb_disk_cache() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache_dir = tempfile::tempdir().unwrap();
    let options = GraphOpenOptions {
        cache: GraphCacheConfig::disk_cache(cache_dir.path(), 64 * 1024 * 1024),
        ..Default::default()
    };
    {
        let shard = GraphShard::open_standalone_writer_with_options(
            "graph/cache-config",
            Arc::clone(&object_store),
            options.clone(),
        )
        .await
        .unwrap();
        shard.write_edge(mutation(1, 2, "cache-1")).await.unwrap();
        shard.close().await.unwrap();
    }

    let reader = GraphShard::open_with_options("graph/cache-config", object_store, options)
        .await
        .unwrap();
    assert!(reader
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
        .await
        .unwrap());
    reader.close().await.unwrap();
}

#[tokio::test]
async fn graph_cache_policy_bounds_entries_and_reports_hits_misses() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = GraphOpenOptions {
        cache_policy: GraphCachePolicy {
            max_matrix_adjacencies: 1,
            max_graphblas_matrices: 1,
            max_entries_per_cell: Some(8),
            ..Default::default()
        },
        ..Default::default()
    };
    let path = "graph/cache-policy";
    {
        let writer = GraphShard::open_standalone_writer_with_options(
            path,
            Arc::clone(&object_store),
            options.clone(),
        )
        .await
        .unwrap();
        writer.write_edge(mutation(1, 2, "cache-a")).await.unwrap();
        writer
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "OTHER_EDGE".to_string(),
                src: 10,
                dst: 20,
                idempotency_key: "cache-b".to_string(),
            })
            .await
            .unwrap();
        writer
            .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2, 2)
            .await
            .unwrap();
        writer
            .build_matrix_tiles("reddit-home", "OTHER_EDGE", 2, 2)
            .await
            .unwrap();
        // Artifact publication no longer hydrates a second adjacency cache.
        assert_eq!(writer.matrix_cache.lock().await.len(), 0);
        writer.close().await.unwrap();
    }

    let reader = GraphShard::open_with_options(path, object_store, options)
        .await
        .unwrap();
    let kernel = SparseKernelBackend::SuiteSparse;
    for _ in 0..2 {
        reader
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1],
                1,
                2,
                kernel,
            )
            .await
            .unwrap();
    }
    reader
        .matrix_reachable_with_kernel("reddit-home", "OTHER_EDGE", &[10], 1, 2, kernel)
        .await
        .unwrap();
    assert_eq!(reader.graphblas_cache.lock().await.len(), 1);
    let metrics = reader.graph_cache_metrics();
    assert!(metrics.matrix_artifact_misses >= 1);
    assert!(metrics.matrix_artifact_hits >= 1);
    assert!(metrics.evictions >= 1);
    assert!(metrics.graphblas_misses >= 1);
    assert!(metrics.graphblas_hits >= 1);
    assert_eq!(metrics.matrix_adjacency_misses, 0);
    assert_eq!(metrics.matrix_adjacency_hits, 0);
    assert!(metrics.hydration_started >= 2);
    assert!(metrics.hydration_completed >= 2);
    reader.close().await.unwrap();
}

#[tokio::test]
async fn write_edge_commits_canonical_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/write-edge", object_store).await;

    let first = shard.write_edge(mutation(1, 2, "req-1")).await.unwrap();
    assert_eq!(
        first,
        CommitResult {
            epoch: 1,
            already_existed: false
        }
    );
    assert!(shard
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
        .await
        .unwrap());
    assert_eq!(
        shard
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2]
    );
    assert_eq!(
        shard
            .in_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2)
            .await
            .unwrap(),
        vec![1]
    );
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        1
    );

    let retry = shard.write_edge(mutation(1, 2, "req-1")).await.unwrap();
    assert_eq!(retry, first);
}

#[test]
fn canonical_graph_records_derive_identity_from_keys() {
    let edge_key = keys::out_edge("reddit-home", "USER_FOLLOWS_USER", 1, 2);
    let canonical_edge = decode_edge_record(&edge_key, b"graph-edge\n").unwrap();
    assert_eq!(canonical_edge.src, 1);
    assert_eq!(canonical_edge.dst, 2);
    assert!(decode_edge_record(
        &edge_key,
        b"edge1\t7\treddit-home\tUSER_FOLLOWS_USER\t1\t2\n"
    )
    .is_err());
    assert!(decode_edge_record(&edge_key, b"edge2\t8\n").is_err());
}
#[tokio::test]
async fn duplicate_edge_with_new_request_does_not_increment_degree() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/duplicate-edge", object_store).await;

    let first = shard.write_edge(mutation(7, 8, "req-1")).await.unwrap();
    let duplicate = shard.write_edge(mutation(7, 8, "req-2")).await.unwrap();

    assert_eq!(first.epoch, duplicate.epoch);
    assert!(!first.already_existed);
    assert!(duplicate.already_existed);
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn concurrent_writes_allocate_unique_epochs_through_slate_transactions() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(open_test_shard("graph/concurrent-unique", object_store).await);

    let mut handles = Vec::new();
    for idx in 0..16_u64 {
        let shard = Arc::clone(&shard);
        handles.push(tokio::spawn(async move {
            shard
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                    src: 1,
                    dst: 1_000 + idx,
                    idempotency_key: format!("concurrent-{idx}"),
                })
                .await
        }));
    }

    let mut epochs = Vec::new();
    for handle in handles {
        epochs.push(handle.await.unwrap().unwrap().epoch);
    }
    epochs.sort_unstable();
    assert_eq!(epochs, (1..=16).collect::<Vec<_>>());
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        16
    );
}

#[tokio::test]
async fn write_edge_transactions_retry_without_epoch_overlap() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(open_test_shard("graph/write-edge-transaction-race", object_store).await);
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let left = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            write_edge_txn_retry_for_test(shard, mutation(1, 2, "single-race-a")).await
        })
    };
    let right = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            write_edge_txn_retry_for_test(shard, mutation(1, 3, "single-race-b")).await
        })
    };

    let mut results = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
    results.sort_by_key(|result| result.epoch);
    assert_eq!(
        results,
        vec![
            CommitResult {
                epoch: 1,
                already_existed: false,
            },
            CommitResult {
                epoch: 2,
                already_existed: false,
            },
        ]
    );
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 2);
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 2);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3]
    );
    let report = shard
        .verify_current_graph(cell_id, edge_type, 2, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn delete_edge_transactions_retry_without_epoch_overlap() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(open_test_shard("graph/delete-edge-transaction-race", object_store).await);
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    shard
        .write_edge(mutation(1, 2, "delete-race-base-a"))
        .await
        .unwrap();
    shard
        .write_edge(mutation(1, 3, "delete-race-base-b"))
        .await
        .unwrap();

    let left = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            delete_edge_txn_retry_for_test(shard, mutation(1, 2, "delete-race-a")).await
        })
    };
    let right = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            delete_edge_txn_retry_for_test(shard, mutation(1, 3, "delete-race-b")).await
        })
    };

    let mut results = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
    results.sort_by_key(|result| result.epoch);
    assert_eq!(
        results,
        vec![
            DeleteResult {
                epoch: 3,
                deleted: true,
            },
            DeleteResult {
                epoch: 4,
                deleted: true,
            },
        ]
    );
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 4);
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 0);
    assert!(shard
        .out_neighbors(cell_id, edge_type, 1)
        .await
        .unwrap()
        .is_empty());
    let report = shard
        .verify_current_graph(cell_id, edge_type, 2, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn bulk_import_transactions_retry_without_epoch_overlap() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(open_test_shard("graph/bulk-import-transaction-race", object_store).await);
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let left = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            bulk_import_txn_retry_for_test(
                shard,
                cell_id,
                edge_type,
                vec![(1, 2), (1, 3)],
                "bulk-race-a",
                BulkImportOptions::default(),
            )
            .await
        })
    };
    let right = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            bulk_import_txn_retry_for_test(
                shard,
                cell_id,
                edge_type,
                vec![(1, 4), (1, 5)],
                "bulk-race-b",
                BulkImportOptions::default(),
            )
            .await
        })
    };

    let mut ranges = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
    ranges.sort_by_key(|result| result.start_epoch);
    assert_eq!(
        ranges,
        vec![
            BulkImportResult {
                start_epoch: 1,
                end_epoch: 1,
                inserted: 2,
                already_existed: 0,
            },
            BulkImportResult {
                start_epoch: 2,
                end_epoch: 2,
                inserted: 2,
                already_existed: 0,
            },
        ]
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4, 5]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 4);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn concurrent_duplicate_edge_writes_converge_to_one_record() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(open_test_shard("graph/concurrent-duplicate", object_store).await);

    let mut handles = Vec::new();
    for idx in 0..12_u64 {
        let shard = Arc::clone(&shard);
        handles.push(tokio::spawn(async move {
            shard
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                    src: 7,
                    dst: 8,
                    idempotency_key: format!("same-edge-{idx}"),
                })
                .await
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap().unwrap());
    }
    assert_eq!(
        results
            .iter()
            .filter(|result| !result.already_existed)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .find(|result| !result.already_existed)
            .unwrap()
            .epoch,
        1
    );
    assert!(results.iter().all(|result| result.epoch >= 1));
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn bulk_import_edges_writes_normal_indexes_and_idempotency() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/bulk-import", object_store).await;

    let result = shard
        .bulk_import_edges(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 2), (1, 3), (1, 2)],
            "bulk-1",
        )
        .await
        .unwrap();
    let retry = shard
        .bulk_import_edges(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 3), (1, 2)],
            "bulk-1",
        )
        .await
        .unwrap();

    assert_eq!(
        result,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 1,
            inserted: 2,
            already_existed: 0
        }
    );
    assert_eq!(retry, result);
    assert_eq!(
        shard
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        2
    );

    let conflict = shard
        .bulk_import_edges(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 4)],
            "bulk-1",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        GraphError::IdempotencyConflict {
            operation: "bulk-import",
            ref idempotency_key,
            reason: "this key already stored a result for a different payload"
        } if idempotency_key == "bulk-1"
    ));

    let second = shard
        .bulk_import_edges(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 2), (2, 4)],
            "bulk-2",
        )
        .await
        .unwrap();
    assert_eq!(
        second,
        BulkImportResult {
            start_epoch: 2,
            end_epoch: 2,
            inserted: 1,
            already_existed: 1
        }
    );
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 2);
}

#[tokio::test]
async fn chunked_bulk_import_respects_batch_limits_and_keeps_idempotency() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/bulk-import-chunked",
        object_store,
        GraphLimits {
            max_bulk_import_edges: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let too_large = shard
        .bulk_import_edges(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 2), (1, 3), (1, 4)],
            "bulk-too-large",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        too_large,
        GraphError::AdmissionRejected {
            operation: "bulk_import_edges",
            actual: 3,
            limit: 2
        }
    ));

    let result = shard
        .bulk_import_edges_chunked(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 2), (1, 3), (1, 4), (1, 5), (1, 6)],
            "bulk-chunked",
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 3,
            inserted: 5,
            already_existed: 0
        }
    );

    let retry = shard
        .bulk_import_edges_chunked(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 6), (1, 5), (1, 4), (1, 3), (1, 2)],
            "bulk-chunked",
            2,
        )
        .await
        .unwrap();
    assert_eq!(retry, result);
    assert_eq!(
        shard
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2, 3, 4, 5, 6]
    );
}

#[tokio::test]
async fn trusted_chunked_bulk_append_uses_bounded_canonical_batches() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/trusted-bulk-import-chunked",
        object_store,
        GraphLimits {
            max_bulk_import_edges: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let zero_chunk = shard
        .bulk_append_edges_trusted_bounded(cell_id, edge_type, [(1, 2)], "trusted-zero-chunk", 0)
        .await
        .unwrap_err();
    assert!(matches!(
        zero_chunk,
        GraphError::CorruptValue { ref key, .. } if key == "trusted_append_chunk_size"
    ));

    let result = shard
        .bulk_append_edges_trusted_bounded(
            cell_id,
            edge_type,
            [(1, 2), (1, 3), (1, 4), (1, 5), (1, 6)],
            "trusted-chunked",
            2,
        )
        .await
        .unwrap();
    let retry = shard
        .bulk_append_edges_trusted_chunked(
            cell_id,
            edge_type,
            [(1, 6), (1, 5), (1, 4), (1, 3), (1, 2)],
            "trusted-chunked",
            2,
        )
        .await
        .unwrap();

    assert_eq!(
        result,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 3,
            inserted: 5,
            already_existed: 0
        }
    );
    assert_eq!(retry, result);
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 3);

    let overlap = shard
        .bulk_append_edges_trusted_bounded(
            cell_id,
            edge_type,
            [(1, 6), (1, 7)],
            "trusted-overlap-new-job",
            10,
        )
        .await
        .unwrap();
    assert_eq!(
        overlap,
        BulkImportResult {
            start_epoch: 4,
            end_epoch: 4,
            inserted: 1,
            already_existed: 1
        }
    );
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 4);
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 6);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4, 5, 6, 7]
    );

    let chunked_overlap = shard
        .bulk_append_edges_trusted_chunked(
            cell_id,
            edge_type,
            [(1, 7), (1, 8)],
            "trusted-chunked-overlap-new-job",
            2,
        )
        .await
        .unwrap();
    assert_eq!(chunked_overlap.inserted, 1);
    assert_eq!(chunked_overlap.already_existed, 1);
    assert_eq!(chunked_overlap.end_epoch, 5);
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 5);
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 7);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4, 5, 6, 7, 8]
    );
}

#[tokio::test]
async fn segmented_adjacency_delete_then_reinsert_clears_the_old_tombstone() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-delete-reinsert",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let inserted = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "segment-insert")
        .await
        .unwrap();
    assert_eq!(inserted.end_epoch, 1);

    let deleted = shard
        .delete_edge(typed_mutation(cell_id, edge_type, 1, 2, "segment-delete"))
        .await
        .unwrap();
    assert_eq!(deleted.epoch, 2);
    assert!(deleted.deleted);
    assert!(!shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap());

    let reinserted = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "segment-reinsert")
        .await
        .unwrap();
    assert_eq!(reinserted.end_epoch, 3);
    assert_eq!(reinserted.inserted, 1);
    assert!(shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap());
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 1);
}

#[tokio::test]
async fn segment_append_transactions_retry_without_epoch_overlap() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = GraphOpenOptions {
        index_policy: GraphIndexPolicy::OutboundOnly,
        ..Default::default()
    };
    let shard = Arc::new(
        GraphShard::open_standalone_writer_with_options(
            "graph/segment-transaction-race",
            object_store,
            options,
        )
        .await
        .unwrap(),
    );
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let left = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            segment_append_txn_retry_for_test(
                shard,
                cell_id,
                edge_type,
                1,
                vec![2, 3],
                "segment-race-a",
            )
            .await
        })
    };
    let right = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            segment_append_txn_retry_for_test(
                shard,
                cell_id,
                edge_type,
                1,
                vec![4, 5],
                "segment-race-b",
            )
            .await
        })
    };

    let mut ranges = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
    ranges.sort_by_key(|result| result.start_epoch);
    assert_eq!(
        ranges,
        vec![
            BulkImportResult {
                start_epoch: 1,
                end_epoch: 1,
                inserted: 2,
                already_existed: 0,
            },
            BulkImportResult {
                start_epoch: 2,
                end_epoch: 2,
                inserted: 2,
                already_existed: 0,
            },
        ]
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4, 5]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 4);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[test]
fn writer_lanes_partition_different_cells() {
    assert_ne!(writer_lane_index("cell-a"), writer_lane_index("cell-b"));
    assert_ne!(
        writer_lane_index("reddit-home"),
        writer_lane_index("other-cell")
    );
}

#[tokio::test]
async fn write_edges_batch_uses_one_atomic_idempotent_transaction() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/write-edges-batch", object_store).await;

    let result = shard
        .write_edges_batch(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(7, 10), (7, 11), (7, 10), (8, 12)],
            "batch-create-1",
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 1,
            inserted: 3,
            already_existed: 0
        }
    );
    assert_eq!(
        shard
            .write_edges_batch(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                [(8, 12), (7, 11), (7, 10)],
                "batch-create-1",
            )
            .await
            .unwrap(),
        result
    );

    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 1);
}

#[tokio::test]
async fn write_edge_mutations_batch_keeps_per_edge_idempotency_and_indexes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/write-edge-mutations-batch", object_store).await;

    let result = shard
        .write_edge_mutations_batch(
            "reddit-home",
            [
                mutation(7, 10, "edge-batch-1"),
                mutation(7, 11, "edge-batch-2"),
                mutation(7, 10, "edge-batch-3"),
            ],
        )
        .await
        .unwrap();

    assert_eq!(result.start_epoch, 1);
    assert_eq!(result.end_epoch, 1);
    assert_eq!(result.inserted, 2);
    assert_eq!(result.already_existed, 1);
    assert_eq!(
        result.results,
        vec![
            CommitResult {
                epoch: 1,
                already_existed: false
            },
            CommitResult {
                epoch: 1,
                already_existed: false
            },
            CommitResult {
                epoch: 1,
                already_existed: true
            }
        ]
    );
    assert_eq!(
        shard
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
            .await
            .unwrap(),
        vec![10, 11]
    );
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
            .await
            .unwrap(),
        2
    );

    let retry = shard
        .write_edge_mutations_batch(
            "reddit-home",
            [
                mutation(7, 10, "edge-batch-1"),
                mutation(7, 11, "edge-batch-2"),
                mutation(7, 10, "edge-batch-3"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(retry, result);
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 1);
}

#[tokio::test]
async fn edge_mutation_batch_transactions_retry_without_epoch_overlap() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard =
        Arc::new(open_test_shard("graph/write-edge-mutations-batch-race", object_store).await);
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let left = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            edge_mutation_batch_txn_retry_for_test(
                shard,
                cell_id,
                vec![
                    mutation(7, 10, "edge-batch-race-a-1"),
                    mutation(7, 11, "edge-batch-race-a-2"),
                ],
                "write_edge_mutations_batch",
            )
            .await
        })
    };
    let right = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            edge_mutation_batch_txn_retry_for_test(
                shard,
                cell_id,
                vec![
                    mutation(7, 12, "edge-batch-race-b-1"),
                    mutation(7, 13, "edge-batch-race-b-2"),
                ],
                "write_edge_mutations_batch",
            )
            .await
        })
    };

    let mut ranges = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
    ranges.sort_by_key(|result| result.start_epoch);
    assert_eq!(
        ranges,
        vec![
            EdgeMutationBatchResult {
                start_epoch: 1,
                end_epoch: 1,
                inserted: 2,
                already_existed: 0,
                results: vec![
                    CommitResult {
                        epoch: 1,
                        already_existed: false,
                    },
                    CommitResult {
                        epoch: 1,
                        already_existed: false,
                    },
                ],
            },
            EdgeMutationBatchResult {
                start_epoch: 2,
                end_epoch: 2,
                inserted: 2,
                already_existed: 0,
                results: vec![
                    CommitResult {
                        epoch: 2,
                        already_existed: false,
                    },
                    CommitResult {
                        epoch: 2,
                        already_existed: false,
                    },
                ],
            },
        ]
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 7).await.unwrap(),
        vec![10, 11, 12, 13]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 7).await.unwrap(), 4);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn write_edge_mutations_batch_rejects_idempotency_reuse_for_different_edge() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/write-edge-mutations-batch-conflict", object_store).await;

    shard
        .write_edge_mutations_batch(
            "reddit-home",
            [
                mutation(7, 10, "edge-batch-conflict"),
                mutation(7, 11, "edge-batch-ok"),
            ],
        )
        .await
        .unwrap();

    let conflict = shard
        .write_edge_mutations_batch("reddit-home", [mutation(7, 12, "edge-batch-conflict")])
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        GraphError::IdempotencyConflict {
            operation: "create",
            ref idempotency_key,
            reason: "this key already stored a result for a different edge"
        } if idempotency_key == "edge-batch-conflict"
    ));

    let duplicate_in_batch = shard
        .write_edge_mutations_batch(
            "reddit-home",
            [
                mutation(8, 10, "edge-batch-duplicate"),
                mutation(8, 11, "edge-batch-duplicate"),
            ],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate_in_batch,
        GraphError::IdempotencyConflict {
            operation: "create",
            ref idempotency_key,
            reason: "the same key appears twice in one batch"
        } if idempotency_key == "edge-batch-duplicate"
    ));
}

#[tokio::test]
async fn ingest_edge_mutations_chunks_and_replays_idempotently() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/ingest-edge-mutations-chunked",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_bulk_import_edges: 2,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mutations =
        (0..5).map(|index| mutation(9, 100 + index, &format!("edge-ingest-chunked-{index}")));
    let result = shard
        .ingest_edge_mutations(
            "reddit-home",
            mutations,
            EdgeIngestOptions { batch_size: 10 },
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        EdgeIngestResult {
            start_epoch: 1,
            end_epoch: 3,
            inserted: 5,
            already_existed: 0,
            batches: 3,
            mutations: 5,
        }
    );
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 9)
            .await
            .unwrap(),
        5
    );

    let replay = shard
        .ingest_edge_mutations(
            "reddit-home",
            (0..5).map(|index| mutation(9, 100 + index, &format!("edge-ingest-chunked-{index}"))),
            EdgeIngestOptions { batch_size: 10 },
        )
        .await
        .unwrap();
    assert_eq!(replay, result);
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 3);

    let duplicate = shard
        .ingest_edge_mutations(
            "reddit-home",
            [mutation(9, 100, "edge-ingest-existing-edge")],
            EdgeIngestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.inserted, 0);
    assert_eq!(duplicate.already_existed, 1);
    assert_eq!(duplicate.end_epoch, 3);
}

#[tokio::test]
async fn ingest_edge_mutations_rejects_zero_batch_size() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/ingest-edge-mutations-zero-batch", object_store).await;

    let err = shard
        .ingest_edge_mutations(
            "reddit-home",
            [mutation(9, 10, "edge-ingest-zero-batch")],
            EdgeIngestOptions { batch_size: 0 },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CorruptValue { ref key, .. } if key == "edge_ingest_batch_size"
    ));
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 0);
}

#[tokio::test]
async fn idempotency_keys_are_bound_to_the_original_edge() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/idempotency-conflict", object_store).await;

    shard
        .write_edge(mutation(7, 8, "create-conflict"))
        .await
        .unwrap();
    let create_err = shard
        .write_edge(mutation(7, 9, "create-conflict"))
        .await
        .unwrap_err();
    assert!(matches!(
        create_err,
        GraphError::IdempotencyConflict {
            operation: "create",
            ref idempotency_key,
            reason: "this key already stored a result for a different edge"
        } if idempotency_key == "create-conflict"
    ));

    shard
        .delete_edge(mutation(7, 8, "delete-conflict"))
        .await
        .unwrap();
    let delete_err = shard
        .delete_edge(mutation(7, 9, "delete-conflict"))
        .await
        .unwrap_err();
    assert!(matches!(
        delete_err,
        GraphError::IdempotencyConflict {
            operation: "delete",
            ref idempotency_key,
            reason: "this key already stored a result for a different edge"
        } if idempotency_key == "delete-conflict"
    ));
}

#[tokio::test]
async fn delete_edge_updates_canonical_snapshot_idempotently() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/delete-edge", object_store).await;

    let first = shard.write_edge(mutation(1, 2, "create-1")).await.unwrap();
    let second = shard.write_edge(mutation(1, 3, "create-2")).await.unwrap();
    let delete = shard.delete_edge(mutation(1, 2, "delete-1")).await.unwrap();
    let retry = shard.delete_edge(mutation(1, 2, "delete-1")).await.unwrap();
    let absent = shard
        .delete_edge(mutation(1, 99, "delete-absent"))
        .await
        .unwrap();

    assert_eq!(first.epoch, 1);
    assert_eq!(second.epoch, 2);
    assert_eq!(
        delete,
        DeleteResult {
            epoch: 3,
            deleted: true
        }
    );
    assert_eq!(retry, delete);
    assert_eq!(
        absent,
        DeleteResult {
            epoch: 3,
            deleted: false
        }
    );

    assert!(!shard
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
        .await
        .unwrap());
    assert_eq!(
        shard
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![3]
    );
    assert_eq!(
        shard
            .out_neighbors_at(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                1,
                shard.current_epoch("reddit-home").await.unwrap(),
            )
            .await
            .unwrap(),
        vec![3]
    );
}

#[tokio::test]
async fn delete_edges_batch_updates_canonical_state_and_replays_idempotently() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/delete-edges-batch", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    shard
        .bulk_import_edges(
            cell_id,
            edge_type,
            [(1, 2), (1, 3), (1, 4)],
            "delete-batch-seed",
        )
        .await
        .unwrap();

    let result = shard
        .delete_edges_batch(
            cell_id,
            edge_type,
            [(1, 2), (1, 4), (1, 2), (1, 99)],
            "delete-batch-1",
        )
        .await
        .unwrap();
    assert_eq!(result.start_epoch, 2);
    assert_eq!(result.end_epoch, 2);
    assert_eq!(result.deleted, 2);
    assert_eq!(result.already_deleted, 1);
    assert_eq!(
        result.results,
        vec![
            DeleteResult {
                epoch: 2,
                deleted: true,
            },
            DeleteResult {
                epoch: 2,
                deleted: true,
            },
            DeleteResult {
                epoch: 1,
                deleted: false,
            },
        ]
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![3]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 1);
    assert!(!shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap());
    assert!(shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
    assert!(!shard.edge_exists(cell_id, edge_type, 1, 4).await.unwrap());
    let retry = shard
        .delete_edges_batch(
            cell_id,
            edge_type,
            [(1, 99), (1, 4), (1, 2)],
            "delete-batch-1",
        )
        .await
        .unwrap();
    assert_eq!(retry, result);
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 2);

    shard
        .bulk_import_edges(
            cell_id,
            edge_type,
            [(2, 10), (2, 11), (2, 12)],
            "delete-batch-chunked-seed",
        )
        .await
        .unwrap();
    let chunked = shard
        .delete_edges_batch_chunked(
            cell_id,
            edge_type,
            [(2, 10), (2, 11), (2, 12), (2, 99)],
            "delete-batch-chunked-1",
            2,
        )
        .await
        .unwrap();
    assert_eq!(chunked.deleted, 3);
    assert_eq!(chunked.already_deleted, 1);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 2).await.unwrap(),
        Vec::<VertexId>::new()
    );
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn delete_edge_mutations_batch_rejects_duplicate_edge_identities() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/delete-edge-mutations-batch-duplicates", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    shard
        .write_edge(mutation(1, 2, "delete-batch-duplicate-seed"))
        .await
        .unwrap();
    let err = shard
        .delete_edge_mutations_batch(
            cell_id,
            [
                mutation(1, 2, "delete-batch-duplicate-a"),
                mutation(1, 2, "delete-batch-duplicate-b"),
            ],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::IdempotencyConflict {
            operation: "delete",
            idempotency_key,
            reason: "two keys in one batch delete the same edge"
        } if idempotency_key.contains("delete-batch-duplicate-a")
            && idempotency_key.contains("delete-batch-duplicate-b")
    ));
    assert!(shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap());
    assert!(shard
        .read_remote(&keys::idempotency(
            cell_id,
            "delete",
            "delete-batch-duplicate-a"
        ))
        .await
        .unwrap()
        .is_none());
    assert!(shard
        .read_remote(&keys::idempotency(
            cell_id,
            "delete",
            "delete-batch-duplicate-b"
        ))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn snapshot_at_rejects_future_epochs() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/snapshot-future", object_store).await;

    shard
        .write_edge(mutation(10, 20, "create-1"))
        .await
        .unwrap();
    let err = match shard.snapshot_at("reddit-home", 2).await {
        Ok(_) => panic!("future snapshot should be rejected"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        GraphError::SnapshotAhead {
            ref cell_id,
            read_epoch: 2,
            current_epoch: 1,
        } if cell_id == "reddit-home"
    ));
}

#[tokio::test]
async fn current_snapshot_uses_one_slatedb_storage_sequence() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/slatedb-snapshot-isolation", object_store).await;

    shard
        .write_edge(mutation(10, 20, "snapshot-storage-1"))
        .await
        .unwrap();
    let snapshot = shard.snapshot("reddit-home").await.unwrap();
    let storage_sequence = snapshot
        .storage_sequence()
        .expect("writer snapshots expose the SlateDB sequence");
    assert!(storage_sequence > 0);

    shard
        .write_edge(mutation(10, 30, "snapshot-storage-2"))
        .await
        .unwrap();

    assert_eq!(
        snapshot
            .out_neighbors("USER_SUBSCRIBED_TO_SUBREDDIT", 10)
            .await
            .unwrap(),
        vec![20]
    );
    assert!(snapshot
        .edge_exists("USER_SUBSCRIBED_TO_SUBREDDIT", 10, 20)
        .await
        .unwrap());
    assert!(!snapshot
        .edge_exists("USER_SUBSCRIBED_TO_SUBREDDIT", 10, 30)
        .await
        .unwrap());
    assert_eq!(
        shard
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 10)
            .await
            .unwrap(),
        vec![20, 30]
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn current_snapshot_preserves_point_edge_across_delete() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/slatedb-snapshot-point-delete", object_store).await;

    shard
        .write_edge(mutation(10, 20, "snapshot-point-create"))
        .await
        .unwrap();
    let snapshot = shard.snapshot("reddit-home").await.unwrap();
    shard
        .delete_edge(mutation(10, 20, "snapshot-point-delete"))
        .await
        .unwrap();

    assert!(snapshot
        .edge_exists("USER_SUBSCRIBED_TO_SUBREDDIT", 10, 20)
        .await
        .unwrap());
    assert!(!shard
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 10, 20)
        .await
        .unwrap());
    shard.close().await.unwrap();
}

#[tokio::test]
async fn reopened_reader_sees_data_from_object_store() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/reopen";

    {
        let shard = open_test_shard(path, Arc::clone(&object_store)).await;
        shard.write_edge(mutation(100, 200, "req-1")).await.unwrap();
        shard.close().await.unwrap();
    }

    let reopened = open_test_shard(path, object_store).await;
    assert!(reopened
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 100, 200)
        .await
        .unwrap());
    assert_eq!(
        reopened
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 100)
            .await
            .unwrap(),
        vec![200]
    );
    assert_eq!(
        reopened
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 100)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn local_object_store_reopen_reads_from_remote_ground_truth() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = "graph/local-empty-cache";

    {
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tempdir.path()).unwrap());
        let shard = open_test_shard(path, object_store).await;
        shard.write_edge(mutation(500, 600, "req-1")).await.unwrap();
        shard.close().await.unwrap();
    }

    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tempdir.path()).unwrap());
    let reopened = open_test_shard(path, object_store).await;
    assert!(reopened
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 500, 600)
        .await
        .unwrap());
    assert_eq!(
        reopened
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 500)
            .await
            .unwrap(),
        vec![600]
    );
}

#[tokio::test]
async fn second_writer_open_fences_first_writer_instance() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/writer-fence";

    let first = open_test_shard(path, Arc::clone(&object_store)).await;
    first.write_edge(mutation(1, 2, "first-1")).await.unwrap();

    let second = open_test_shard(path, object_store).await;
    second.write_edge(mutation(1, 3, "second-1")).await.unwrap();

    let err = first
        .write_edge(mutation(1, 4, "first-fenced"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::Slate(ref slate_err) if matches!(slate_err.kind(), ErrorKind::Closed(_))
    ));
    second.close().await.unwrap();
}

#[tokio::test]
async fn fenced_writer_falls_back_to_reader_for_reads_and_index_discovery() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/fenced-writer-read-fallback";

    let first = open_test_shard(path, Arc::clone(&object_store)).await;
    let first_commit = first
        .write_edge(typed_mutation("cell-a", "CHAIN", 1, 2, "first"))
        .await
        .unwrap();
    assert!(
        first.refresh_storage_sequence("cell-a").await.unwrap() >= first_commit.epoch,
        "the pre-fence reader should be cached at the first writer sequence"
    );

    let replacement = open_test_shard(path, object_store).await;
    let replacement_commit = replacement
        .write_edge(typed_mutation("cell-a", "CHAIN", 2, 3, "replacement"))
        .await
        .unwrap();

    let stale_writer = first.db.writer().unwrap();
    let error = stale_writer.refresh_manifest().await.unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Closed(slatedb::CloseReason::Fenced)
    ));

    let dirty = first.dirty_graph_index_edge_types("cell-a").await.unwrap();
    assert!(dirty.iter().any(|(edge_type, _)| edge_type == "CHAIN"));
    assert!(
        first.current_storage_sequence("cell-a").await.unwrap() >= replacement_commit.epoch,
        "writer demotion must refresh an already-cached reader"
    );
    assert!(first.edge_exists("cell-a", "CHAIN", 2, 3).await.unwrap());

    let later_commit = replacement
        .write_edge(typed_mutation("cell-a", "CHAIN", 3, 4, "replacement-later"))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if first.edge_exists("cell-a", "CHAIN", 3, 4).await.unwrap() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("managed reader should discover later replacement writes");
    assert!(
        first.current_storage_sequence("cell-a").await.unwrap() >= later_commit.epoch,
        "ordinary sequence reads must advance with the managed reader"
    );

    first.close().await.unwrap();
    replacement.close().await.unwrap();
}
#[test]
fn db_reader_child_process_entry() {
    if std::env::var("SLATEDB_GRAPH_READER_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let object_root = std::env::var("SLATEDB_GRAPH_READER_OBJECT_ROOT").unwrap();
        let ready_path =
            std::path::PathBuf::from(std::env::var("SLATEDB_GRAPH_READER_READY_FILE").unwrap());
        let stop_path =
            std::path::PathBuf::from(std::env::var("SLATEDB_GRAPH_READER_STOP_FILE").unwrap());
        let object_store = local_object_store(object_root).unwrap();
        let reader = GraphShard::open("graph/process-reader", object_store)
            .await
            .unwrap();
        assert!(reader
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2,)
            .await
            .unwrap());
        std::fs::write(&ready_path, b"ready").unwrap();
        while !stop_path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        reader.close().await.unwrap();
    });
}

#[tokio::test]
async fn separate_process_db_reader_does_not_fence_active_writer() {
    let object_root = tempfile::tempdir().unwrap();
    let object_store = local_object_store(object_root.path()).unwrap();
    let writer =
        GraphShard::open_standalone_writer("graph/process-reader", Arc::clone(&object_store))
            .await
            .unwrap();
    writer
        .write_edge(mutation(1, 2, "reader-child-seed"))
        .await
        .unwrap();

    let ready_file = object_root.path().join("reader.ready");
    let stop_file = object_root.path().join("reader.stop");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("tests::db_reader_child_process_entry")
        .arg("--nocapture")
        .env("SLATEDB_GRAPH_READER_CHILD", "1")
        .env("SLATEDB_GRAPH_READER_OBJECT_ROOT", object_root.path())
        .env("SLATEDB_GRAPH_READER_READY_FILE", &ready_file)
        .env("SLATEDB_GRAPH_READER_STOP_FILE", &stop_file)
        .spawn()
        .unwrap();

    for _ in 0..200 {
        if ready_file.exists() {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("reader child exited before ready: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(ready_file.exists(), "reader child did not become ready");
    writer
        .write_edge(mutation(1, 3, "writer-after-child-reader"))
        .await
        .unwrap();

    std::fs::write(&stop_file, b"stop").unwrap();
    for _ in 0..200 {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "reader child failed: {status}");
            writer.close().await.unwrap();
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    child.kill().unwrap();
    panic!("reader child did not stop");
}

#[tokio::test]
async fn graph_cluster_runs_multiple_local_shards_on_one_object_store() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cluster = GraphCluster::open_cells_standalone_writers(
        "graph-cluster",
        ["cell-a".to_string(), "cell-b".to_string()],
        object_store,
    )
    .await
    .unwrap();
    assert_eq!(cluster.shard_count(), 2);

    for (cell_id, src, dst) in [("cell-a", 1, 10), ("cell-b", 1, 20)] {
        cluster
            .shard(cell_id)
            .unwrap()
            .write_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("req-{cell_id}"),
            })
            .await
            .unwrap();
    }

    assert_eq!(
        cluster
            .shard("cell-a")
            .unwrap()
            .out_neighbors("cell-a", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![10]
    );
    assert_eq!(
        cluster
            .shard("cell-b")
            .unwrap()
            .out_neighbors("cell-b", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![20]
    );
    cluster.close().await.unwrap();
}

#[tokio::test]
async fn graph_cluster_open_cleans_previously_opened_shards_after_later_validation_error() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seed = open_test_shard(
        &format!(
            "{}/cell-a",
            GraphScope::default().scoped_store_path("graph-cluster-partial-open-cleanup")
        ),
        Arc::clone(&object_store),
    )
    .await;
    seed.close().await.unwrap();
    let err = match GraphCluster::open_cells(
        "graph-cluster-partial-open-cleanup",
        ["cell-a", "bad/cell"],
        Arc::clone(&object_store),
    )
    .await
    {
        Ok(_) => panic!("partial reader open unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        GraphError::InvalidKeyComponent {
            component: "cell_id",
            ..
        }
    ));

    let reopened = GraphCluster::open_cells(
        "graph-cluster-partial-open-cleanup",
        ["cell-a"],
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    assert_eq!(reopened.shard_count(), 1);
    reopened.close().await.unwrap();

    let err = match GraphCluster::open_cells_standalone_writers(
        "graph-cluster-partial-open-cleanup-writer",
        ["cell-a", "bad/cell"],
        Arc::clone(&object_store),
    )
    .await
    {
        Ok(_) => panic!("partial writer open unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        GraphError::InvalidKeyComponent {
            component: "cell_id",
            ..
        }
    ));
    let reopened = GraphCluster::open_cells_standalone_writers(
        "graph-cluster-partial-open-cleanup-writer",
        ["cell-a"],
        object_store,
    )
    .await
    .unwrap();
    assert_eq!(reopened.shard_count(), 1);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_readers_open_every_configured_cell() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seed = open_test_shard(
        &format!(
            "{}/reddit-home",
            GraphScope::default().scoped_store_path("graph-routed-cluster")
        ),
        Arc::clone(&object_store),
    )
    .await;
    seed.close().await.unwrap();
    let seed = open_test_shard(
        &format!(
            "{}/reddit-search",
            GraphScope::default().scoped_store_path("graph-routed-cluster")
        ),
        Arc::clone(&object_store),
    )
    .await;
    seed.close().await.unwrap();
    let placement =
        ObjectStoreNodeDirectory::new(["reddit-home", "reddit-search"], ["node-a", "node-b"])
            .unwrap();
    let cluster = RoutedGraphCluster::open_readers(
        "graph-routed-cluster",
        "node-a",
        placement,
        placement_over("node-a", &["node-a", "node-b"]),
        object_store,
    )
    .await
    .unwrap();
    assert_eq!(cluster.local_cells(), vec!["reddit-home", "reddit-search"]);

    let read_only_error = cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "owned-write".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        read_only_error,
        GraphError::WriteRequiresWriter {
            operation: "routed_write",
            ref cell_id
        } if cell_id == "reddit-home"
    ));

    let second_read_only_error = cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-search".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "second-read-only".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        second_read_only_error,
        GraphError::WriteRequiresWriter {
            operation: "routed_write",
            ref cell_id
        } if cell_id == "reddit-search"
    ));
    cluster.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_uses_slatedb_writer_fencing() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let placement = ObjectStoreNodeDirectory::new(["cell-a"], ["node-a", "node-b"]).unwrap();
    // Two nodes, each holding a view in which it is alone — the view skew that
    // ownership cannot resolve and SlateDB's epoch must. A shared fleet here
    // would let only the rendezvous winner promote, and this test would stop
    // exercising the fence at all.
    let first = RoutedGraphCluster::open_promotable_scoped_with_memory_options(
        "graph-slate-writer-fencing",
        GraphScope::default(),
        "node-b",
        placement.clone(),
        sole_writer_placement("node-b"),
        Arc::clone(&object_store),
        fast_fence_options(),
        GraphMemoryConfig::default(),
    )
    .await
    .unwrap();
    first
        .write_edge(typed_mutation("cell-a", "FOLLOWS", 1, 2, "first"))
        .await
        .unwrap();

    let replacement = RoutedGraphCluster::open_promotable_scoped_with_memory_options(
        "graph-slate-writer-fencing",
        GraphScope::default(),
        "node-a",
        placement,
        sole_writer_placement("node-a"),
        object_store,
        fast_fence_options(),
        GraphMemoryConfig::default(),
    )
    .await
    .unwrap();
    replacement
        .write_edge(typed_mutation("cell-a", "FOLLOWS", 2, 3, "replacement"))
        .await
        .unwrap();

    let stale = first
        .write_edge(typed_mutation("cell-a", "FOLLOWS", 3, 4, "stale"))
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        GraphError::Slate(ref error)
            if matches!(
                error.kind(),
                slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
            )
    ));

    first
        .write_edge(typed_mutation("cell-a", "FOLLOWS", 3, 4, "recovered"))
        .await
        .unwrap();
    let replacement_stale = replacement
        .write_edge(typed_mutation(
            "cell-a",
            "FOLLOWS",
            4,
            5,
            "replacement-stale",
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        replacement_stale,
        GraphError::Slate(ref error)
            if matches!(
                error.kind(),
                slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
            )
    ));

    let _ = replacement.close().await;
    let _ = first.close().await;
}

/// The prod incident in `cell-writer-fencing-pingpong`, reproduced: three
/// graph-nodes, one cell, and writes arriving at all three of them at once.
///
/// Before rendezvous placement every node opened the SlateDB writer on demand.
/// Whoever opened it last fenced the incumbent, the fenced node's next write
/// took the epoch straight back, and the three traded one cell's `writer_epoch`
/// for as long as traffic kept arriving. Nothing was wrong with the fencing —
/// SlateDB did exactly what it promises — the bug was that nothing had decided
/// which node *should* be holding the writer, so every node was equally
/// entitled to steal it.
///
/// # Why this view is shared, where the fencing test's is not
///
/// `routed_cluster_uses_slatedb_writer_fencing` above gives each node
/// `sole_writer_placement`, a view in which it believes it is the whole fleet,
/// precisely so the two *do* fence each other and the epoch is exercised. This
/// test is the opposite arrangement, and the agreement is its entire subject:
/// all three nodes hold the same fleet view, so rendezvous names exactly one
/// owner and the other two are refused at the gate instead of taking the epoch.
///
/// The winner is computed here from `turbolay_placement::hash::owner` rather
/// than read back off whichever node happened to promote. A test that observed
/// the winner would still pass if ownership were settled by a coin flip, and a
/// coin flip is what the incident was.
///
/// # The epoch assertion is a bound, not a count
///
/// Decision 6 rule 3 retries a fenced writer *without* re-checking ownership,
/// which accepts that a converging node may re-fence the winner once more
/// before the two views agree. An assertion of "exactly one promotion" would
/// therefore fail correctly-behaving code the first time that happened. The
/// invariant that does hold is convergence within a bound: the incident's
/// signature is an epoch that climbs with traffic and never settles, and no
/// ceiling at all survives that.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn three_nodes_writing_one_cell_at_once_leave_one_writer_and_a_bounded_epoch() {
    const BASE: &str = "graph-writer-epoch-pingpong";
    const CELL: &str = "cell-a";
    const FLEET: [&str; 3] = ["node-a", "node-b", "node-c"];
    // Rounds of concurrent writes, because a duel is only visible over repeated
    // traffic: the incident's epoch climbed per write, not per process. Four is
    // enough that unbounded growth overshoots the ceiling several times over
    // while the test still costs milliseconds.
    const ROUNDS: u64 = 4;
    // One promotion by the rendezvous owner, plus one spare for decision 6
    // rule 3's blind retry after a fence. Twelve writes fit inside this; the
    // pre-fix behaviour of every node promoting on demand is already at three
    // after the first round, so the bound discriminates rather than merely
    // being generous.
    const MAX_WRITER_EPOCH: u64 = 2;

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let scope = GraphScope::default();
    let expected_owner = turbolay_placement::hash::owner(&scope.to_string(), CELL, &FLEET)
        .expect("a non-empty fleet has an owner");
    let directory = ObjectStoreNodeDirectory::new([CELL], FLEET).unwrap();

    let mut clusters = Vec::with_capacity(FLEET.len());
    for node_id in FLEET {
        clusters.push(
            RoutedGraphCluster::open_promotable_scoped_with_memory_options(
                BASE,
                scope.clone(),
                node_id,
                directory.clone(),
                // The shared fleet view. Every node computes ownership from the
                // same candidate list, which is the whole precondition
                // rendezvous needs to end the duel.
                placement_over(node_id, &FLEET),
                Arc::clone(&object_store),
                fast_fence_options(),
                GraphMemoryConfig::default(),
            )
            .await
            .unwrap(),
        );
    }

    let mut promotions = 0_u64;
    let mut refusals = 0_u64;
    for round in 0..ROUNDS {
        let (from_a, from_b, from_c) = tokio::join!(
            clusters[0].write_edge(typed_mutation(
                CELL,
                "FOLLOWS",
                1,
                10 + round,
                &format!("{}-round-{round}", FLEET[0]),
            )),
            clusters[1].write_edge(typed_mutation(
                CELL,
                "FOLLOWS",
                2,
                20 + round,
                &format!("{}-round-{round}", FLEET[1]),
            )),
            clusters[2].write_edge(typed_mutation(
                CELL,
                "FOLLOWS",
                3,
                30 + round,
                &format!("{}-round-{round}", FLEET[2]),
            )),
        );
        for (node_id, result) in FLEET.iter().zip([from_a, from_b, from_c]) {
            match result {
                Ok(_) => {
                    assert_eq!(
                        *node_id, expected_owner,
                        "round {round}: only the rendezvous owner may promote"
                    );
                    promotions += 1;
                }
                Err(GraphError::NotCellWriter {
                    ref cell_id,
                    owner: Some(ref hint),
                }) => {
                    assert_eq!(cell_id, CELL);
                    assert_eq!(
                        hint, expected_owner,
                        "round {round}: {node_id} must name the rendezvous winner so the \
                         driver re-routes instead of retrying into the same wrong node"
                    );
                    refusals += 1;
                }
                other => panic!(
                    "round {round}: {node_id} should have promoted or refused with a hint, \
                     got {other:?}"
                ),
            }
        }
    }
    assert_eq!(promotions, ROUNDS, "the owner's write lands every round");
    assert_eq!(refusals, 2 * ROUNDS, "the other two refuse every round");

    // A refusal is only worth anything if the refusing node left the writer
    // alone, so check the handles rather than trusting the error type.
    for (node_id, cluster) in FLEET.iter().zip(&clusters) {
        let holds_the_writer = cluster.shard(CELL).unwrap().db.writer().is_ok();
        assert_eq!(
            holds_the_writer,
            *node_id == expected_owner,
            "{node_id} holds_the_writer={holds_the_writer}, but the owner is {expected_owner}"
        );
    }

    for cluster in clusters {
        let _ = cluster.close().await;
    }

    // The epoch off the manifest SlateDB actually wrote. Decision 3 records that
    // this is the only authority on who holds the writer, and it is the number
    // the incident was watched on.
    let shard_path = format!("{}/{CELL}", scope.scoped_store_path(BASE));
    let manifest = slatedb::admin::Admin::builder(shard_path.as_str(), object_store)
        .build()
        .read_manifest(None)
        .await
        .expect("the shard's manifest is readable")
        .expect("a promotion wrote a manifest");
    let writer_epoch = manifest.writer_epoch();
    assert!(
        writer_epoch >= 1,
        "somebody must have promoted, or this test proves nothing; epoch {writer_epoch}"
    );
    assert!(
        writer_epoch <= MAX_WRITER_EPOCH,
        "the writer epoch must converge rather than climb with traffic: {writer_epoch} \
         after {} writes across {} nodes",
        ROUNDS * FLEET.len() as u64,
        FLEET.len()
    );
}

#[cfg(feature = "query-transport")]
#[test]
fn scoped_routed_cluster_returns_empty_rows_without_registering_an_unwritten_scope() {
    std::thread::Builder::new()
        .name("empty-scoped-query".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(
                    scoped_routed_cluster_returns_empty_rows_without_registering_an_unwritten_scope_inner(),
                );
        })
        .unwrap()
        .join()
        .unwrap();
}

#[cfg(feature = "query-transport")]
async fn scoped_routed_cluster_returns_empty_rows_without_registering_an_unwritten_scope_inner() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root_namespace = NamespacePath::root(NamespaceId::new("production").unwrap());
    let graph_id = GraphId::new("hydradb").unwrap();
    let scope = GraphScope::new(
        root_namespace
            .child(NamespaceId::new("tenant-empty").unwrap())
            .unwrap()
            .child(NamespaceId::new("collection-empty").unwrap())
            .unwrap(),
        graph_id.clone(),
    );
    let runtime = ScopedRoutedGraphCluster::new(
        "graph/empty-native-scope",
        root_namespace.clone(),
        graph_id.clone(),
        "node-a",
        ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
        sole_writer_placement("node-a"),
        Arc::clone(&object_store),
        fast_fence_options(),
        GraphMemoryConfig::default(),
        4,
    )
    .unwrap();

    let result = QueryCellClient::execute_cypher_rows(
        &runtime,
        QueryContext::new("cell-0", "empty-scope-read").in_scope(scope),
        "MATCH (n:Source) RETURN count(*) AS total",
    )
    .await
    .unwrap();
    assert_eq!(result.rows[0].values, vec![QueryValue::Count(0)]);

    let scope_directory = ObjectStoreGraphScopeDirectory::new(
        "graph/empty-native-scope",
        root_namespace,
        graph_id,
        object_store,
    );
    assert!(scope_directory.list().await.unwrap().is_empty());
    runtime.close().await.unwrap();
}
#[cfg(feature = "query-transport")]
#[tokio::test]
async fn scoped_routed_cluster_isolates_collection_writers_and_registers_scopes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root_namespace = NamespacePath::root(NamespaceId::new("production").unwrap());
    let graph_id = GraphId::new("hydradb").unwrap();
    let directory = ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap();
    let runtime = ScopedRoutedGraphCluster::new(
        "graph/native-scopes",
        root_namespace.clone(),
        graph_id.clone(),
        "node-a",
        directory,
        sole_writer_placement("node-a"),
        Arc::clone(&object_store),
        fast_fence_options(),
        GraphMemoryConfig::default(),
        4,
    )
    .unwrap();
    let collection_a = GraphScope::new(
        root_namespace
            .child(NamespaceId::new("tenant-a").unwrap())
            .unwrap()
            .child(NamespaceId::new("collection-a").unwrap())
            .unwrap(),
        graph_id.clone(),
    );
    let collection_b = GraphScope::new(
        root_namespace
            .child(NamespaceId::new("tenant-a").unwrap())
            .unwrap()
            .child(NamespaceId::new("collection-b").unwrap())
            .unwrap(),
        graph_id.clone(),
    );

    let empty_cluster = runtime.cluster_for_scope(&collection_a).await.unwrap();
    assert!(empty_cluster
        .shard("cell-0")
        .unwrap()
        .dirty_graph_index_edge_types("cell-0")
        .await
        .unwrap()
        .is_empty());
    let scope_directory = ObjectStoreGraphScopeDirectory::new(
        "graph/native-scopes",
        root_namespace.clone(),
        graph_id.clone(),
        Arc::clone(&object_store),
    );
    assert!(scope_directory.list().await.unwrap().is_empty());
    drop(empty_cluster);

    let cluster_a = runtime
        .cluster_for_scope_write(&collection_a, "cell-0")
        .await
        .unwrap();
    let cluster_b = runtime
        .cluster_for_scope_write(&collection_b, "cell-0")
        .await
        .unwrap();
    let (write_a, write_b) = tokio::join!(
        cluster_a.write_edge(typed_mutation("cell-0", "FOLLOWS", 1, 2, "scope-a")),
        cluster_b.write_edge(typed_mutation("cell-0", "FOLLOWS", 1, 3, "scope-b")),
    );
    write_a.unwrap();
    write_b.unwrap();

    assert!(cluster_a
        .shard("cell-0")
        .unwrap()
        .edge_exists("cell-0", "FOLLOWS", 1, 2)
        .await
        .unwrap());
    assert!(!cluster_a
        .shard("cell-0")
        .unwrap()
        .edge_exists("cell-0", "FOLLOWS", 1, 3)
        .await
        .unwrap());
    assert!(cluster_b
        .shard("cell-0")
        .unwrap()
        .edge_exists("cell-0", "FOLLOWS", 1, 3)
        .await
        .unwrap());

    assert_eq!(
        scope_directory.list().await.unwrap(),
        vec![collection_a, collection_b]
    );

    drop(cluster_a);
    drop(cluster_b);
    runtime.close().await.unwrap();
}

#[tokio::test]
async fn routed_reader_catches_up_to_a_remote_writer_storage_sequence() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let placement = ObjectStoreNodeDirectory::new(["cell-a"], ["node-a", "node-b"]).unwrap();
    let writer = RoutedGraphCluster::open_promotable_scoped_with_memory_options(
        "graph-slate-multi-reader",
        GraphScope::default(),
        "node-a",
        placement.clone(),
        sole_writer_placement("node-a"),
        Arc::clone(&object_store),
        fast_fence_options(),
        GraphMemoryConfig::default(),
    )
    .await
    .unwrap();
    writer
        .write_edge(typed_mutation("cell-a", "FOLLOWS", 1, 2, "seed"))
        .await
        .unwrap();

    let reader = RoutedGraphCluster::open_readers(
        "graph-slate-multi-reader",
        "node-b",
        placement,
        placement_over("node-b", &["node-a", "node-b"]),
        object_store,
    )
    .await
    .unwrap();
    assert!(reader
        .shard("cell-a")
        .unwrap()
        .snapshot("cell-a")
        .await
        .unwrap()
        .edge_exists("FOLLOWS", 1, 2)
        .await
        .unwrap());

    writer
        .write_edge(typed_mutation("cell-a", "FOLLOWS", 2, 3, "later"))
        .await
        .unwrap();
    let sequence = writer
        .shard("cell-a")
        .unwrap()
        .current_storage_sequence("cell-a")
        .await
        .unwrap();
    let reader_shard = reader.shard("cell-a").unwrap();
    assert!(
        reader_shard
            .wait_for_storage_sequence("cell-a", sequence)
            .await
            .unwrap()
            >= sequence
    );
    assert!(reader_shard
        .snapshot("cell-a")
        .await
        .unwrap()
        .edge_exists("FOLLOWS", 2, 3)
        .await
        .unwrap());

    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn matrix_snapshot_abort_cleanup_removes_unpublished_chunks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/matrix-snapshot-abort-cleanup", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "ABORT_CLEANUP_EDGE";

    shard
        .write_edge(typed_mutation(cell_id, edge_type, 1, 2, "cleanup-seed"))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    let epoch = format!("{base_epoch:020}");
    let keys = vec![
        format!("cell/{cell_id}/artifact/matrix/{edge_type}/{epoch}/out/00000000000000000000/00000000000000000000"),
        format!("cell/{cell_id}/artifact/matrix/{edge_type}/{epoch}/in/00000000000000000000/00000000000000000000"),
        format!("cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{epoch}/vertices/00000000000000000000"),
        format!("cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{epoch}/pointers/00000000000000000000"),
        format!("cell/{cell_id}/artifact/graphblas_csc_chunk/{edge_type}/{epoch}/indices/00000000000000000000"),
    ];

    let mut batch = GraphWriteBatch::new();
    for key in &keys {
        batch.put(key.as_bytes(), b"unpublished");
    }
    shard
        .write_graph_batch_strict(cell_id, "test_seed_unpublished_matrix_artifacts", batch)
        .await
        .unwrap();

    let cleanup = engine::cleanup_unpublished_matrix_artifact_epoch(
        &shard,
        cell_id,
        edge_type,
        base_epoch,
        "test_cleanup_unpublished_matrix_artifacts",
    )
    .await;
    assert_eq!(cleanup.deleted_keys, keys.len() as u64);
    assert_eq!(cleanup.cleanup_errors, 0);
    assert!(!cleanup.skipped_published_manifest);
    for key in keys {
        assert!(
            shard.read_remote(&key).await.unwrap().is_none(),
            "expected unpublished artifact key to be deleted: {key}"
        );
    }
}

#[tokio::test]
async fn matrix_snapshot_abort_cleanup_preserves_published_manifest_epoch() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/matrix-snapshot-abort-published-guard", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "PUBLISHED_GUARD_EDGE";

    shard
        .write_edge(typed_mutation(
            cell_id,
            edge_type,
            1,
            2,
            "published-guard-seed",
        ))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    let epoch = format!("{base_epoch:020}");
    let manifest_key = format!("cell/{cell_id}/artifact/matrix_manifest/{edge_type}/{epoch}");
    let chunk_key = format!(
        "cell/{cell_id}/artifact/matrix/{edge_type}/{epoch}/out/00000000000000000000/00000000000000000000"
    );

    let mut batch = GraphWriteBatch::new();
    batch.put(manifest_key.as_bytes(), b"published");
    batch.put(chunk_key.as_bytes(), b"published-chunk");
    shard
        .write_graph_batch_strict(cell_id, "test_seed_published_matrix_artifacts", batch)
        .await
        .unwrap();

    let cleanup = engine::cleanup_unpublished_matrix_artifact_epoch(
        &shard,
        cell_id,
        edge_type,
        base_epoch,
        "test_cleanup_published_matrix_artifacts",
    )
    .await;
    assert_eq!(cleanup.deleted_keys, 0);
    assert_eq!(cleanup.cleanup_errors, 0);
    assert!(cleanup.skipped_published_manifest);
    assert!(shard.read_remote(&manifest_key).await.unwrap().is_some());
    assert!(shard.read_remote(&chunk_key).await.unwrap().is_some());
}

#[tokio::test]
async fn matrix_snapshot_abort_cleanup_respects_inflight_artifact_builder_lock() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/matrix-snapshot-abort-cleanup-builder-lock",
        object_store,
    )
    .await;
    let cell_id = "reddit-home";
    let edge_type = "ABORT_CLEANUP_LOCKED_EDGE";

    shard
        .write_edge(typed_mutation(
            cell_id,
            edge_type,
            1,
            2,
            "cleanup-builder-lock-seed",
        ))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    let epoch = format!("{base_epoch:020}");
    let chunk_key = format!(
        "cell/{cell_id}/artifact/matrix/{edge_type}/{epoch}/out/00000000000000000000/00000000000000000000"
    );

    let mut batch = GraphWriteBatch::new();
    batch.put(chunk_key.as_bytes(), b"inflight-builder-chunk");
    shard
        .write_graph_batch_strict(cell_id, "test_seed_inflight_matrix_artifact", batch)
        .await
        .unwrap();

    let builder_lock = shard
        .acquire_local_artifact_guard(cell_id, edge_type, base_epoch, "held-by-test-builder")
        .await
        .unwrap();
    let cleanup = engine::cleanup_unpublished_matrix_artifact_epoch(
        &shard,
        cell_id,
        edge_type,
        base_epoch,
        "test_cleanup_contended_matrix_artifact",
    )
    .await;
    assert_eq!(cleanup.deleted_keys, 0);
    assert!(cleanup.cleanup_errors > 0);
    assert!(!cleanup.skipped_published_manifest);
    assert!(
        shard.read_remote(&chunk_key).await.unwrap().is_some(),
        "cleanup must not delete chunks while the same artifact epoch is locked by a builder"
    );
    builder_lock.release().await.unwrap();
}

#[tokio::test]
async fn matrix_publish_is_fenced_by_cleanup_generation_and_reclaims_stale_marker() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/matrix-cleanup-generation", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "MATRIX_CLEANUP_GENERATION_EDGE";
    shard
        .write_edge(typed_mutation(
            cell_id,
            edge_type,
            1,
            2,
            "matrix-cleanup-generation-seed",
        ))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    let cleanup_marker = engine::matrix_cleanup_marker_key(cell_id, edge_type, base_epoch);
    let manifest_key =
        format!("cell/{cell_id}/artifact/matrix_manifest/{edge_type}/{base_epoch:020}");

    let mut marker_batch = GraphWriteBatch::new();
    marker_batch.put(&cleanup_marker, b"cleanup-owner");
    shard
        .write_graph_batch_strict(cell_id, "test_claim_matrix_cleanup", marker_batch)
        .await
        .unwrap();

    let mut unsafe_publish = GraphWriteBatch::new();
    unsafe_publish.put(&manifest_key, b"incomplete-manifest");
    let err = shard
        .write_graph_batch_strict_guarded(
            cell_id,
            "test_publish_matrix_manifest",
            vec![GraphWriteGuard::absent(&cleanup_marker)],
            unsafe_publish,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::ConditionalWriteConflict { ref key, .. } if key == &cleanup_marker
    ));
    assert!(shard.read_remote(&manifest_key).await.unwrap().is_none());

    let artifact = shard
        .build_matrix_tiles(cell_id, edge_type, base_epoch, 64)
        .await
        .unwrap();
    assert_eq!(artifact.base_epoch, base_epoch);
    assert!(shard.read_remote(&cleanup_marker).await.unwrap().is_none());
    assert!(shard.read_remote(&manifest_key).await.unwrap().is_some());
    shard.close().await.unwrap();
}

#[tokio::test]
async fn graph_limits_reject_unbounded_bulk_artifact_and_traversal_work() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/limits",
        object_store,
        GraphLimits {
            max_bulk_import_edges: 2,
            max_artifact_source_epochs: 2,
            max_traversal_hops: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bulk_err = shard
        .bulk_import_edges(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 2), (1, 3), (1, 4)],
            "too-large",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        bulk_err,
        GraphError::AdmissionRejected {
            operation: "bulk_import_edges",
            actual: 3,
            limit: 2
        }
    ));

    shard.write_edge(mutation(1, 2, "limit-1")).await.unwrap();
    shard.write_edge(mutation(2, 3, "limit-2")).await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2, 2)
        .await
        .unwrap();
    shard.write_edge(mutation(3, 4, "limit-3")).await.unwrap();
    let artifact_err = shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 3, 2)
        .await
        .unwrap_err();
    assert!(matches!(
        artifact_err,
        GraphError::AdmissionRejected {
            operation: "build_matrix_tiles",
            actual: 3,
            limit: 2
        }
    ));

    let traversal_err = shard
        .matrix_reachable("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", &[1], 2, 2)
        .await
        .unwrap_err();
    assert!(matches!(
        traversal_err,
        GraphError::AdmissionRejected {
            operation: "matrix_reachable",
            actual: 2,
            limit: 1
        }
    ));
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn current_one_shot_query_uses_slatedb_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-slatedb-snapshot", object_store).await;
    shard
        .write_edge(mutation(1, 2, "query-storage-snapshot-seed"))
        .await
        .unwrap();

    let result = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-storage-snapshot"),
            "MATCH ({id: 1})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(v) RETURN v.id",
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn artifact_build_edge_limit_rejects_loaded_builds() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/artifact-edge-limit",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_artifact_build_edges: 2,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    shard
        .write_edge(mutation(1, 2, "edge-limit-1"))
        .await
        .unwrap();
    shard
        .write_edge(mutation(2, 3, "edge-limit-2"))
        .await
        .unwrap();
    shard
        .write_edge(mutation(3, 4, "edge-limit-3"))
        .await
        .unwrap();
    let err = shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 3, 2)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::AdmissionRejected {
            operation: "build_matrix_tiles_edges",
            actual: 3,
            limit: 2
        }
    ));
    shard.close().await.unwrap();
}

#[tokio::test]
async fn repair_report_validates_canonical_edges_and_degrees() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/repair-report", object_store).await;
    shard.write_edge(mutation(1, 2, "repair-1")).await.unwrap();
    shard.write_edge(mutation(1, 3, "repair-2")).await.unwrap();
    let report = shard
        .validate_cell_edge_type("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT")
        .await
        .unwrap();
    assert_eq!(report.live_edges, 2);
    assert!(report.degree_mismatches.is_empty());
    shard.close().await.unwrap();
}

#[tokio::test]
async fn current_graph_verifier_detects_index_corruption() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    let shard = open_test_shard("graph/current-verifier-corrupt", object_store).await;
    shard
        .write_edge(mutation(1, 2, "verify-corrupt"))
        .await
        .unwrap();

    let mut batch = WriteBatch::new();
    batch.delete(keys::out_edge(cell_id, edge_type, 1, 2).as_bytes());
    shard.write_strict_for_test(batch).await.unwrap();

    let report = shard
        .verify_current_graph(cell_id, edge_type, 1, 4)
        .await
        .unwrap();
    assert!(!report.is_clean());
    assert!(
        report
            .mismatch_samples
            .iter()
            .any(|sample| sample.contains("in_index:extra")),
        "{:?}",
        report.mismatch_samples
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn current_graph_verifier_detects_relationship_index_corruption() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cell_id = "reddit-home";
    let edge_type = "FOLLOWS";
    let shard = open_test_shard("graph/current-verifier-rel-corrupt", object_store).await;
    let relationship = shard
        .create_relationship(
            typed_mutation(cell_id, edge_type, 1, 2, "verify-rel-corrupt"),
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(7)),
        )
        .await
        .unwrap();

    let clean = shard
        .verify_current_graph(cell_id, edge_type, 1, 4)
        .await
        .unwrap();
    assert!(clean.is_clean(), "{:?}", clean.mismatch_samples);
    assert_eq!(clean.relationship_records, 1);
    assert_eq!(clean.relationship_count_counters, 1);
    assert_eq!(clean.relationship_property_indexes, 1);

    let mut batch = WriteBatch::new();
    batch.put(
        keys::relationship_count(cell_id, edge_type, 1, 2).as_bytes(),
        encode_u64(99),
    );
    batch.delete(
        keys::relationship_property_index(
            cell_id,
            edge_type,
            "rank",
            &encode_vertex_property_value_key(&VertexPropertyValue::Integer(7)),
            1,
            2,
            relationship.relationship_id,
        )
        .as_bytes(),
    );
    shard.write_strict_for_test(batch).await.unwrap();

    let corrupt = shard
        .verify_current_graph(cell_id, edge_type, 1, 4)
        .await
        .unwrap();
    assert!(!corrupt.is_clean());
    assert!(
        corrupt
            .mismatch_samples
            .iter()
            .any(|sample| sample.contains("relationship_count:mismatch")),
        "{:?}",
        corrupt.mismatch_samples
    );
    assert!(
        corrupt
            .mismatch_samples
            .iter()
            .any(|sample| sample.contains("relationship_property_index:missing")),
        "{:?}",
        corrupt.mismatch_samples
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn env_object_store_loader_supports_graph_harness() {
    let tempdir = tempfile::tempdir().unwrap();
    let env_path = tempdir.path().join("memory.env");
    std::fs::write(&env_path, "CLOUD_PROVIDER=memory\n").unwrap();

    let object_store =
        object_store_from_env(Some(env_path.to_string_lossy().into_owned())).unwrap();
    let shard = open_test_shard("graph/env-loader", object_store).await;
    shard.write_edge(mutation(700, 701, "req-1")).await.unwrap();
    assert!(shard
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 700, 701)
        .await
        .unwrap());
}

#[tokio::test]
async fn query_planner_selects_physical_operators() {
    let context = QueryContext::new("reddit-home", "query-plan-read");
    let count_plan = QueryPlanner::plan(
        &context,
        &QueryStatement::MatchOut {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            return_count: true,
        },
    )
    .unwrap();
    assert_eq!(
        count_plan.physical,
        PhysicalQueryPlan::OutDegreeCounter {
            edge_type: "FOLLOWS".to_string(),
            src: 1
        }
    );
    assert!(!count_plan.is_write());

    let write_plan = QueryPlanner::plan(
        &QueryContext::new("reddit-home", "query-plan-write"),
        &QueryStatement::CreateEdge {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
        },
    )
    .unwrap();
    assert_eq!(
        write_plan.physical,
        PhysicalQueryPlan::WriteEdge {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2
        }
    );
    assert!(write_plan.is_write());

    let metadata_plan = QueryPlanner::plan(
        &QueryContext::new("reddit-home", "query-plan-write-metadata"),
        &QueryStatement::CreateEdgeWithMetadata {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            src_metadata: VertexMetadata::default().with_label("User"),
            dst_metadata: VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("alice".to_string())),
        },
    )
    .unwrap();
    assert_eq!(
        metadata_plan.physical,
        PhysicalQueryPlan::WriteEdgeWithMetadata {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            src_metadata: VertexMetadata::default().with_label("User"),
            dst_metadata: VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("alice".to_string())),
        }
    );
    assert!(metadata_plan.is_write());

    assert!(matches!(
        QueryPlanner::plan(
            &QueryContext::new("reddit-home", "query-plan-write-snapshot").at_epoch(1),
            &QueryStatement::CreateEdge {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
            },
        ),
        Err(GraphError::UnsupportedQuery { .. })
    ));

    assert!(matches!(
        QueryPlanner::plan(
            &QueryContext::new("reddit-home", "query-plan-count-window")
                .with_result_window(0, Some(1)),
            &QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: true,
            },
        ),
        Err(GraphError::UnsupportedQuery { .. })
    ));
}

#[tokio::test]
async fn stale_query_plans_require_a_live_pinned_slatedb_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-plan-snapshot", object_store).await;
    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "query-snapshot-1".to_string(),
        })
        .await
        .unwrap();
    let read_epoch = shard.current_epoch("reddit-home").await.unwrap();
    let plan = shard
        .plan_query_statement(
            QueryContext::new("reddit-home", "query-snapshot-read").at_epoch(read_epoch),
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: false,
            },
        )
        .unwrap();
    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 3,
            idempotency_key: "query-snapshot-2".to_string(),
        })
        .await
        .unwrap();

    assert!(matches!(
        shard.execute_query_plan(plan).await.unwrap_err(),
        GraphError::UnsupportedQuery { .. }
    ));
    assert!(matches!(
        shard
            .execute_query_statement(
                QueryContext::new("reddit-home", "query-snapshot-count").at_epoch(read_epoch),
                QueryStatement::MatchOut {
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    return_count: true,
                },
            )
            .await
            .unwrap_err(),
        GraphError::UnsupportedQuery { .. }
    ));
    assert!(matches!(
        shard
            .execute_query_statement(
                QueryContext::new("reddit-home", "query-snapshot-exists").at_epoch(read_epoch),
                QueryStatement::MatchEdge {
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    dst: 3,
                    return_count: false,
                },
            )
            .await
            .unwrap_err(),
        GraphError::UnsupportedQuery { .. }
    ));
    shard.close().await.unwrap();
}

#[tokio::test]
async fn query_plan_rejects_future_snapshot_epoch() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-plan-future-snapshot", object_store).await;
    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "query-future-seed".to_string(),
        })
        .await
        .unwrap();
    let current_epoch = shard.current_epoch("reddit-home").await.unwrap();
    let err = shard
        .execute_query_statement(
            QueryContext::new("reddit-home", "query-future-read").at_epoch(current_epoch + 1),
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: true,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::SnapshotAhead {
            ref cell_id,
            read_epoch,
            current_epoch: observed_current,
        } if cell_id == "reddit-home"
            && read_epoch == current_epoch + 1
            && observed_current == current_epoch
    ));
    shard.close().await.unwrap();
}

#[tokio::test]
async fn query_timeout_rejects_expired_public_plans() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-timeout-public-plan", object_store).await;
    shard
        .write_edge(mutation(1, 2, "query-timeout-seed"))
        .await
        .unwrap();

    let err = shard
        .execute_query_statement(
            QueryContext::new("reddit-home", "query-timeout-read").with_timeout_ms(0),
            QueryStatement::MatchOut {
                edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                src: 1,
                return_count: false,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::QueryTimeout {
            operation: "query_plan",
            limit_ms: 0,
            ..
        }
    ));
    shard.close().await.unwrap();
}

#[tokio::test]
async fn execute_query_plan_revalidates_public_plans() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-plan-public-validation", object_store).await;

    let write_with_snapshot = QueryPlan {
        cell_id: "reddit-home".to_string(),
        idempotency_key: "query-public-write".to_string(),
        read_epoch: Some(0),
        result_window: QueryWindow::default(),
        max_runtime_ms: None,
        logical: LogicalQueryPlan::CreateEdge {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
        },
        physical: PhysicalQueryPlan::WriteEdge {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
        },
    };
    assert!(matches!(
        shard.execute_query_plan(write_with_snapshot).await,
        Err(GraphError::UnsupportedQuery { .. })
    ));

    let mismatched_plan = QueryPlan {
        cell_id: "reddit-home".to_string(),
        idempotency_key: "query-public-mismatch".to_string(),
        read_epoch: None,
        result_window: QueryWindow::default(),
        max_runtime_ms: None,
        logical: LogicalQueryPlan::MatchOut {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            return_count: true,
        },
        physical: PhysicalQueryPlan::OutNeighbors {
            edge_type: "FOLLOWS".to_string(),
            src: 1,
        },
    };
    assert!(matches!(
        shard.execute_query_plan(mismatched_plan).await,
        Err(GraphError::UnsupportedQuery { .. })
    ));

    let invalid_edge_type_plan = QueryPlan {
        cell_id: "reddit-home".to_string(),
        idempotency_key: "query-public-invalid-edge-type".to_string(),
        read_epoch: None,
        result_window: QueryWindow::default(),
        max_runtime_ms: None,
        logical: LogicalQueryPlan::MatchOut {
            edge_type: "BAD/TYPE".to_string(),
            src: 1,
            return_count: false,
        },
        physical: PhysicalQueryPlan::OutNeighbors {
            edge_type: "BAD/TYPE".to_string(),
            src: 1,
        },
    };
    assert!(matches!(
        shard.execute_query_plan(invalid_edge_type_plan).await,
        Err(GraphError::InvalidKeyComponent {
            component: "edge_type",
            ..
        })
    ));

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_timeout_rejects_expired_queries() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-timeout", object_store).await;
    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 10,
            idempotency_key: "cypher-row-timeout-seed".to_string(),
        })
        .await
        .unwrap();

    let err = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-timeout-read").with_timeout_ms(0),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::QueryTimeout {
            operation: "cypher_rows",
            limit_ms: 0,
            ..
        }
    ));
}

#[tokio::test]
async fn query_windows_bound_neighbor_results() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/query-window",
        object_store,
        GraphLimits {
            max_query_result_vertices: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, dst) in [10, 11, 12].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("query-window-{idx}"),
            })
            .await
            .unwrap();
    }

    let bounded = shard
        .execute_query_statement(
            QueryContext::new("reddit-home", "query-window-read").with_result_window(1, Some(1)),
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(bounded, QueryOutput::Vertices(vec![11]));

    let zero_limit = shard
        .execute_query_statement(
            QueryContext::new("reddit-home", "query-window-zero").with_result_window(0, Some(0)),
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(zero_limit, QueryOutput::Vertices(Vec::new()));

    let too_many = shard
        .execute_query_statement(
            QueryContext::new("reddit-home", "query-window-unbounded"),
            QueryStatement::MatchOut {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                return_count: false,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        too_many,
        GraphError::AdmissionRejected {
            operation: "query_result_vertices",
            actual: 3,
            limit: 2,
        }
    ));
    shard.close().await.unwrap();
}

#[cfg(feature = "query-transport")]
async fn test_read_transport_json(
    reader: &mut tokio::io::BufReader<tokio::net::TcpStream>,
) -> serde_json::Value {
    use tokio::io::AsyncBufReadExt;

    let mut line = String::new();
    let read = reader.read_line(&mut line).await.unwrap();
    assert!(read > 0, "transport peer closed before sending a frame");
    serde_json::from_str(&line).unwrap()
}

#[cfg(feature = "query-transport")]
async fn test_write_transport_json(
    reader: &mut tokio::io::BufReader<tokio::net::TcpStream>,
    value: &serde_json::Value,
) {
    use tokio::io::AsyncWriteExt;

    let mut frame = serde_json::to_vec(value).unwrap();
    frame.push(b'\n');
    reader.get_mut().write_all(&frame).await.unwrap();
}

#[cfg(feature = "query-transport")]
fn test_transport_rows_response() -> serde_json::Value {
    serde_json::json!({
        "kind": "rows",
        "result": QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
        ),
    })
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_guarded_batch_uses_a_distinct_wire_operation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let version_one_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = tokio::io::BufReader::new(stream);
        let request = test_read_transport_json(&mut reader).await;
        assert_eq!(request["version"].as_u64(), Some(1));
        assert_eq!(
            request["operation"]["GuardedUpsertVertices"]["merge_policy"]["update_if_newer_by"]
                .as_str(),
            Some("updated_at")
        );
        test_write_transport_json(
            &mut reader,
            &serde_json::json!({
                "response": {
                    "kind": "error",
                    "message": "unknown batch operation GuardedUpsertVertices",
                },
                "close_connection": true,
            }),
        )
        .await;
    });

    let client = TcpQueryCellClient::new(addr).with_timeout(std::time::Duration::from_secs(2));
    let result = client
        .execute_batch(
            QueryContext::new("reddit-home", "strict-version-client"),
            QueryBatchOperation::GuardedUpsertVertices {
                vertices: Vec::new(),
                merge_policy: QueryBatchMergePolicy {
                    update_if_newer_by: "updated_at".to_string(),
                    create_only_properties: std::collections::BTreeSet::new(),
                },
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(result, GraphError::UnsupportedQuery { .. }));
    assert_eq!(client.metrics().connections_created, 1);
    assert_eq!(client.metrics().client_retries, 0);
    version_one_server.await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_server_rejects_obsolete_client_version() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("v.id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default().insecure_allow_unauthenticated(),
    )
    .await
    .unwrap();
    let stream = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    let mut reader = tokio::io::BufReader::new(stream);
    let request = serde_json::json!({
        "kind": "rows",
        "version": 2,
        "auth": { "bearer_token": null },
        "context": QueryContext::new("reddit-home", "obsolete-version-client"),
        "query": "MATCH (u {id: 1}) RETURN u.id",
    });
    test_write_transport_json(&mut reader, &request).await;
    let response = test_read_transport_json(&mut reader).await;
    assert_eq!(response["response"]["kind"], "error");
    assert!(response["response"]["message"]
        .as_str()
        .unwrap()
        .contains("expected 1"));
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_never_replays_after_execution_loses_response() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let raw_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = tokio::io::BufReader::new(stream);
        let warm = test_read_transport_json(&mut first).await;
        assert_eq!(warm["context"]["idempotency_key"], "at-most-once-warm");
        test_write_transport_json(
            &mut first,
            &serde_json::json!({
                "response": test_transport_rows_response(),
                "close_connection": false,
            }),
        )
        .await;

        let ambiguous = test_read_transport_json(&mut first).await;
        assert_eq!(
            ambiguous["context"]["idempotency_key"],
            "at-most-once-ambiguous"
        );
        drop(first);

        let (stream, _) = listener.accept().await.unwrap();
        let mut recovery = tokio::io::BufReader::new(stream);
        let request = test_read_transport_json(&mut recovery).await;
        assert_eq!(
            request["context"]["idempotency_key"], "at-most-once-explicit-retry",
            "the ambiguous request was automatically replayed"
        );
        test_write_transport_json(
            &mut recovery,
            &serde_json::json!({
                "response": test_transport_rows_response(),
                "close_connection": true,
            }),
        )
        .await;
    });

    let client = TcpQueryCellClient::new(addr)
        .with_max_retries(3)
        .with_timeout(std::time::Duration::from_secs(2));
    client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "at-most-once-warm"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "at-most-once-ambiguous"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap_err();
    let recovered = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "at-most-once-explicit-retry"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(recovered.rows.len(), 1);
    assert_eq!(client.metrics().client_retries, 0);
    raw_server.await.unwrap();
}

#[cfg(feature = "query-transport-tls")]
struct TestMtlsBundle {
    server: Arc<tokio_rustls::rustls::ServerConfig>,
    clients: Vec<(Arc<tokio_rustls::rustls::ClientConfig>, String)>,
    anonymous_client: Arc<tokio_rustls::rustls::ClientConfig>,
    intermediate_fingerprint: String,
}

#[cfg(feature = "query-transport-tls")]
fn test_mtls_bundle(expired: bool) -> TestMtlsBundle {
    use rcgen::{
        date_time_ymd, BasicConstraints, CertificateParams, DistinguishedName, DnType,
        ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    };
    use sha2::Digest;
    use tokio_rustls::rustls::{
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
        server::WebPkiClientVerifier,
        ClientConfig, RootCertStore, ServerConfig,
    };

    fn private_key(key: &KeyPair) -> PrivateKeyDer<'static> {
        PrivatePkcs8KeyDer::from(key.serialize_der()).into()
    }

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::default();
    let mut ca_name = DistinguishedName::new();
    ca_name.push(DnType::CommonName, "query-transport-test-ca");
    ca_params.distinguished_name = ca_name;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let intermediate_key = KeyPair::generate().unwrap();
    let mut intermediate_params = CertificateParams::default();
    let mut intermediate_name = DistinguishedName::new();
    intermediate_name.push(DnType::CommonName, "query-transport-test-intermediate");
    intermediate_params.distinguished_name = intermediate_name;
    intermediate_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    intermediate_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let intermediate_cert = intermediate_params
        .signed_by(&intermediate_key, &ca_cert, &ca_key)
        .unwrap();
    let intermediate_digest = sha2::Sha256::digest(intermediate_cert.der().as_ref());
    let intermediate_fingerprint = format!(
        "sha256:{}",
        intermediate_digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    if expired {
        server_params.not_before = date_time_ymd(2000, 1, 1);
        server_params.not_after = date_time_ymd(2001, 1, 1);
    }
    let server_cert = server_params
        .signed_by(&server_key, &intermediate_cert, &intermediate_key)
        .unwrap();

    let mut client_roots = RootCertStore::empty();
    client_roots.add(ca_cert.der().clone()).unwrap();
    let anonymous_client = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(client_roots.clone())
            .with_no_client_auth(),
    );

    let mut clients = Vec::new();
    let mut server_client_roots = RootCertStore::empty();
    server_client_roots.add(ca_cert.der().clone()).unwrap();
    for index in 0..2 {
        let client_key = KeyPair::generate().unwrap();
        let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        let mut client_name = DistinguishedName::new();
        client_name.push(DnType::CommonName, format!("query-client-{index}"));
        client_params.distinguished_name = client_name;
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        if expired {
            client_params.not_before = date_time_ymd(2000, 1, 1);
            client_params.not_after = date_time_ymd(2001, 1, 1);
        }
        let client_cert = client_params
            .signed_by(&client_key, &intermediate_cert, &intermediate_key)
            .unwrap();
        let client_config = ClientConfig::builder()
            .with_root_certificates(client_roots.clone())
            .with_client_auth_cert(
                vec![client_cert.der().clone(), intermediate_cert.der().clone()],
                private_key(&client_key),
            )
            .unwrap();
        let digest = sha2::Sha256::digest(client_cert.der().as_ref());
        let fingerprint = format!(
            "sha256:{}",
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        clients.push((Arc::new(client_config), fingerprint));
    }

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(server_client_roots))
        .build()
        .unwrap();
    let server = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(
            vec![server_cert.der().clone(), intermediate_cert.der().clone()],
            private_key(&server_key),
        )
        .unwrap();
    TestMtlsBundle {
        server: Arc::new(server),
        clients,
        anonymous_client,
        intermediate_fingerprint,
    }
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_default_bind_rejects_unauthenticated_requests() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("v.id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let server = TcpQueryServer::bind("127.0.0.1:0".parse().unwrap(), Arc::new(StaticQueryClient))
        .await
        .unwrap();
    let err = TcpQueryCellClient::new(server.local_addr())
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-default-deny"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unauthorized"));
    assert!(server.metrics().auth_failures >= 1);
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_batches_page_rows_and_enforce_write_grants() {
    struct StaticBatchClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticBatchClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            unreachable!("batch transport test does not execute Cypher rows")
        }

        async fn execute_cypher_rows_page(
            &self,
            _context: QueryContext,
            _query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            unreachable!("batch transport test does not execute Cypher pages")
        }

        async fn execute_batch(
            &self,
            _context: QueryContext,
            operation: QueryBatchOperation,
        ) -> Result<QueryResultSet> {
            let QueryBatchOperation::OutNeighbors {
                sources,
                source_column,
                destination_column,
                ..
            } = operation
            else {
                return Ok(QueryResultSet::new(Vec::new(), Vec::new()));
            };
            Ok(QueryResultSet::new(
                vec![source_column, destination_column],
                sources
                    .into_iter()
                    .map(|source| {
                        QueryRow::new(vec![
                            QueryValue::VertexId(source),
                            QueryValue::VertexId(source + 100),
                        ])
                    })
                    .collect(),
            ))
        }
    }

    let authorizer = StaticQueryTransportScopeAuthorizer::new()
        .with_bearer_grant(
            "batch-reader",
            QueryTransportScopeGrant::read_graph(GraphScope::default()),
        )
        .unwrap();
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticBatchClient),
        QueryTransportServerConfig::default()
            .with_required_bearer_token("batch-reader")
            .with_scope_authorizer(Arc::new(authorizer))
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let client = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("batch-reader")
        .insecure_allow_plaintext();
    let operation = QueryBatchOperation::OutNeighbors {
        edge_type: "FOLLOWS".to_string(),
        sources: vec![1, 2, 3],
        source_column: QueryColumn::new("src"),
        destination_column: QueryColumn::new("dst"),
    };
    let first = client
        .execute_batch_page(
            QueryContext::new("cell-a", "tcp-batch-page-1"),
            operation.clone(),
            None,
            2,
        )
        .await
        .unwrap();
    assert_eq!(first.rows.len(), 2);
    assert_eq!(first.next_cursor, Some(QueryCursorToken::new(2)));
    let second = client
        .execute_batch_page(
            QueryContext::new("cell-a", "tcp-batch-page-2"),
            operation,
            first.next_cursor,
            2,
        )
        .await
        .unwrap();
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.next_cursor, None);

    let denied = client
        .execute_batch(
            QueryContext::new("cell-a", "tcp-batch-write-denied"),
            QueryBatchOperation::CreateEdges {
                edge_type: "FOLLOWS".to_string(),
                edges: vec![QueryBatchEdge { src: 1, dst: 2 }],
            },
        )
        .await
        .unwrap_err();
    assert!(denied.to_string().contains("not authorized to write"));
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_blank_bearer_token_fails_closed() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("v.id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    assert!(QueryTransportSecret::try_new("").is_err());
    assert!(QueryTransportSecret::try_new("   ").is_err());

    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default().with_required_bearer_token("   "),
    )
    .await
    .unwrap();
    let err = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("   ")
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-blank-deny"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unauthorized"));
    assert!(server.metrics().auth_failures >= 1);
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_requires_explicit_plaintext_for_authenticated_servers() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            _context: QueryContext,
            _query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            Ok(QueryResultPage::new(Vec::new(), Vec::new(), None))
        }
    }

    let result = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default().with_required_bearer_token("secret"),
    )
    .await;
    assert!(matches!(
        result,
        Err(GraphError::UnsafeDurabilityConfig {
            operation: "query_transport_config",
            ..
        })
    ));

    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default()
            .with_required_bearer_token("secret")
            .insecure_allow_plaintext(),
    )
    .await
    .unwrap();
    let client_err = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("secret")
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "plaintext-client-denied"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        client_err,
        GraphError::UnsafeDurabilityConfig {
            operation: "query_transport_config",
            ..
        }
    ));
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_does_not_replay_a_stale_pooled_request() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("v.id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default()
            .with_required_bearer_token("secret")
            .insecure_allow_plaintext()
            .with_idle_timeout(std::time::Duration::from_millis(40))
            .with_max_requests_per_connection(8),
    )
    .await
    .unwrap();
    let client = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("secret")
        .insecure_allow_plaintext()
        .with_max_idle_connections(1);
    for index in 0..3 {
        let result = client
            .execute_cypher_rows(
                QueryContext::new("reddit-home", format!("connection-reuse-{index}")),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }
    assert_eq!(client.idle_connection_count().await, 1);
    assert_eq!(client.metrics().connections_created, 1);
    assert_eq!(client.metrics().connections_reused, 2);
    assert_eq!(server.metrics().connections_accepted, 1);

    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    let ambiguous = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "connection-reuse-after-idle-close"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(ambiguous.to_string().contains("query/transport"));
    assert_eq!(client.metrics().connections_created, 1);

    let recovered = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "connection-reuse-explicit-retry"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(recovered.rows.len(), 1);
    assert_eq!(client.metrics().connections_created, 2);
    assert_eq!(client.metrics().client_retries, 0);
    assert!(server.metrics().idle_timeouts >= 1);
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_cleans_lifecycle_after_executor_panic() {
    struct PanicOnceQueryClient {
        should_panic: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl QueryCellClient for PanicOnceQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            if self
                .should_panic
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                panic!("intentional query transport test panic");
            }
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(PanicOnceQueryClient {
            should_panic: std::sync::atomic::AtomicBool::new(true),
        }),
        QueryTransportServerConfig::default().insecure_allow_unauthenticated(),
    )
    .await
    .unwrap();
    let client = TcpQueryCellClient::new(server.local_addr());
    let context = QueryContext::new("reddit-home", "panic-lifecycle-reuse");
    let first = client
        .execute_cypher_rows(context.clone(), "MATCH (u {id: 1}) RETURN u.id")
        .await
        .unwrap_err();
    assert!(first.to_string().contains("internal query execution error"));
    client
        .execute_cypher_rows(context, "MATCH (u {id: 1}) RETURN u.id")
        .await
        .unwrap();
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_limits_idle_and_concurrent_connections() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            _context: QueryContext,
            _query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            Ok(QueryResultPage::new(Vec::new(), Vec::new(), None))
        }
    }

    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default()
            .with_max_connections(1)
            .with_reserved_control_connections(1)
            .with_idle_timeout(std::time::Duration::from_millis(50)),
    )
    .await
    .unwrap();
    let first = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    for _ in 0..50 {
        if server.metrics().connections_active == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let second = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    for _ in 0..50 {
        if server.metrics().connections_active == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let third = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(server.metrics().connections_accepted, 2);
    assert!(server.metrics().connections_rejected >= 1);
    drop(third);
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(server.metrics().idle_timeouts >= 1);
    assert_eq!(server.metrics().connections_active, 0);
    drop(second);
    drop(first);
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_graceful_shutdown_drains_active_query() {
    struct ControlledQueryClient {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl QueryCellClient for ControlledQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("v.id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(ControlledQueryClient {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        }),
        QueryTransportServerConfig::default()
            .with_required_bearer_token("secret")
            .insecure_allow_plaintext()
            .with_graceful_shutdown_timeout(std::time::Duration::from_secs(2)),
    )
    .await
    .unwrap();
    let client = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("secret")
        .insecure_allow_plaintext();
    let query = tokio::spawn(async move {
        client
            .execute_cypher_rows(
                QueryContext::new("reddit-home", "graceful-drain"),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
    });
    started.notified().await;
    let stop = tokio::spawn(server.stop());
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!stop.is_finished());
    release.notify_one();
    assert_eq!(query.await.unwrap().unwrap().rows.len(), 1);
    stop.await.unwrap().unwrap();
}

#[cfg(feature = "query-transport-tls")]
#[tokio::test]
async fn tcp_query_transport_mtls_authenticates_certificates_and_rejects_invalid_peers() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("v.id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let bundle = test_mtls_bundle(false);
    let allowed_fingerprint = bundle.clients[0].1.to_ascii_uppercase();
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default()
            .with_tls(Arc::clone(&bundle.server))
            .with_required_mtls_fingerprints([allowed_fingerprint])
            .with_handshake_timeout(std::time::Duration::from_millis(40)),
    )
    .await
    .unwrap();

    let valid = TcpQueryCellClient::new(server.local_addr())
        .with_tls("localhost", Arc::clone(&bundle.clients[0].0));
    assert_eq!(
        valid
            .execute_cypher_rows(
                QueryContext::new("reddit-home", "mtls-valid"),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
            .unwrap()
            .rows
            .len(),
        1
    );

    let wrong_identity = TcpQueryCellClient::new(server.local_addr())
        .with_tls("localhost", Arc::clone(&bundle.clients[1].0));
    let wrong_identity_err = wrong_identity
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-wrong-identity"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(wrong_identity_err.to_string().contains("unauthorized"));

    let anonymous = TcpQueryCellClient::new(server.local_addr())
        .with_tls("localhost", Arc::clone(&bundle.anonymous_client));
    assert!(anonymous
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-anonymous"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .is_err());

    let wrong_hostname = TcpQueryCellClient::new(server.local_addr())
        .with_tls("not-localhost", Arc::clone(&bundle.clients[0].0));
    assert!(wrong_hostname
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-wrong-hostname"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .is_err());
    let slow_handshake = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(server.metrics().handshake_failures >= 1);
    drop(slow_handshake);
    server.stop().await.unwrap();

    let expired = test_mtls_bundle(true);
    let expired_server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default()
            .with_tls(Arc::clone(&expired.server))
            .with_required_mtls_fingerprints([expired.clients[0].1.clone()]),
    )
    .await
    .unwrap();
    let expired_client = TcpQueryCellClient::new(expired_server.local_addr())
        .with_tls("localhost", Arc::clone(&expired.clients[0].0));
    assert!(expired_client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-expired"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .is_err());
    expired_server.stop().await.unwrap();
}

#[cfg(feature = "query-transport-tls")]
#[tokio::test]
async fn tcp_query_transport_mtls_accepts_an_allowed_presented_chain_fingerprint() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            _context: QueryContext,
            _query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            Ok(QueryResultPage::new(Vec::new(), Vec::new(), None))
        }
    }

    let bundle = test_mtls_bundle(false);
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default()
            .with_tls(Arc::clone(&bundle.server))
            .with_required_mtls_fingerprints([bundle.intermediate_fingerprint.clone()]),
    )
    .await
    .unwrap();
    TcpQueryCellClient::new(server.local_addr())
        .with_tls("localhost", Arc::clone(&bundle.clients[0].0))
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-intermediate-fingerprint"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport-tls")]
#[tokio::test]
async fn tcp_query_transport_rotates_server_and_client_certificates() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            _context: QueryContext,
            _query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            Ok(QueryResultPage::new(Vec::new(), Vec::new(), None))
        }
    }

    let first = test_mtls_bundle(false);
    let second = test_mtls_bundle(false);
    let server_provider = Arc::new(ReloadableQueryTransportTlsServerConfigProvider::new(
        Arc::clone(&first.server),
    ));
    let client_provider = Arc::new(ReloadableQueryTransportTlsClientConfigProvider::new(
        Arc::clone(&first.clients[0].0),
    ));
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(StaticQueryClient),
        QueryTransportServerConfig::default()
            .with_tls_provider(server_provider.clone())
            .with_required_mtls_fingerprints([
                first.clients[0].1.clone(),
                second.clients[0].1.clone(),
            ])
            .with_max_requests_per_connection(1),
    )
    .await
    .unwrap();
    let client = TcpQueryCellClient::new(server.local_addr())
        .with_tls_provider("localhost", client_provider.clone());
    client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-before-rotation"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();

    server_provider.rotate(Arc::clone(&second.server)).unwrap();
    client_provider
        .rotate(Arc::clone(&second.clients[0].0))
        .unwrap();
    client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-after-rotation"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();

    let stale_client = TcpQueryCellClient::new(server.local_addr())
        .with_tls("localhost", Arc::clone(&first.clients[0].0));
    assert!(stale_client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "mtls-stale-after-rotation"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .is_err());
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport-tls")]
#[tokio::test]
async fn tcp_query_transport_cancellation_is_scoped_to_mtls_identity() {
    struct ControlledQueryClient {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl QueryCellClient for ControlledQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let bundle = test_mtls_bundle(false);
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(ControlledQueryClient {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        }),
        QueryTransportServerConfig::default()
            .with_tls(Arc::clone(&bundle.server))
            .with_required_mtls_fingerprints([bundle.intermediate_fingerprint.clone()]),
    )
    .await
    .unwrap();
    let owner = TcpQueryCellClient::new(server.local_addr())
        .with_tls("localhost", Arc::clone(&bundle.clients[0].0));
    let other = TcpQueryCellClient::new(server.local_addr())
        .with_tls("localhost", Arc::clone(&bundle.clients[1].0));
    let owner_query = owner.clone();
    let query = tokio::spawn(async move {
        owner_query
            .execute_cypher_rows(
                QueryContext::new("reddit-home", "identity-scoped-cancel"),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
    });
    started.notified().await;
    let unscoped_err = server
        .cancel_query("identity-scoped-cancel")
        .await
        .unwrap_err();
    assert!(unscoped_err.to_string().contains("no active query"));
    let wrong_server_principal =
        QueryTransportCancellationPrincipal::mtls_peer_fingerprint(bundle.clients[1].1.clone())
            .unwrap();
    let wrong_server_err = server
        .cancel_query_for_principal(wrong_server_principal, "identity-scoped-cancel")
        .await
        .unwrap_err();
    assert!(wrong_server_err.to_string().contains("no active query"));
    let other_err = other
        .cancel_query("identity-scoped-cancel")
        .await
        .unwrap_err();
    assert!(other_err.to_string().contains("no active query"));
    owner.cancel_query("identity-scoped-cancel").await.unwrap();
    release.notify_one();
    assert!(query
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("query_transport_cancelled"));
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_stop_aborts_idle_connections() {
    struct StaticQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let server = TcpQueryServer::bind("127.0.0.1:0".parse().unwrap(), Arc::new(StaticQueryClient))
        .await
        .unwrap();
    let _idle_connection = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), server.stop())
        .await
        .expect("query transport stop should abort idle connection tasks")
        .unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_applies_server_backpressure_under_load() {
    struct SlowQueryClient;

    #[async_trait::async_trait]
    impl QueryCellClient for SlowQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("v.id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(SlowQueryClient),
        QueryTransportServerConfig::default()
            .with_required_bearer_token("secret")
            .insecure_allow_plaintext()
            .with_max_concurrent_requests(1),
    )
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for idx in 0..6 {
        let client = TcpQueryCellClient::new(server.local_addr())
            .with_bearer_token("secret")
            .insecure_allow_plaintext();
        tasks.push(tokio::spawn(async move {
            client
                .execute_cypher_rows(
                    QueryContext::new("reddit-home", format!("query-transport-load-{idx}")),
                    "MATCH (u {id: 1}) RETURN u.id",
                )
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        let result = task.await.unwrap();
        assert_eq!(result.rows.len(), 1);
    }
    assert!(server.metrics().backpressure_waits >= 1);

    let client = TcpQueryCellClient::new(server.local_addr())
        .with_bearer_token("secret")
        .insecure_allow_plaintext();
    let blocker_client = client.clone();
    let blocker = tokio::spawn(async move {
        blocker_client
            .execute_cypher_rows(
                QueryContext::new("reddit-home", "query-transport-queue-blocker"),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let queued_client = client.clone();
    let queued = tokio::spawn(async move {
        queued_client
            .execute_cypher_rows(
                QueryContext::new("reddit-home", "query-transport-cancel-queued"),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    client
        .cancel_query("query-transport-cancel-queued")
        .await
        .unwrap();
    let retry_after_queued_cancel = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-cancel-queued"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(retry_after_queued_cancel.rows.len(), 1);
    let queued_cancelled = queued.await.unwrap().unwrap_err();
    assert!(queued_cancelled
        .to_string()
        .contains("query_transport_cancelled"));
    assert_eq!(blocker.await.unwrap().unwrap().rows.len(), 1);

    let active_client = client.clone();
    let active = tokio::spawn(async move {
        active_client
            .execute_cypher_rows(
                QueryContext::new("reddit-home", "query-transport-cancel-active"),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    client
        .cancel_query("query-transport-cancel-active")
        .await
        .unwrap();
    let cancelled = active.await.unwrap().unwrap_err();
    assert!(cancelled.to_string().contains("query_transport_cancelled"));

    let reused = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-cancel-active"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(reused.rows.len(), 1);

    let finished = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-late-cancel"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(finished.rows.len(), 1);
    let late_cancel = client
        .cancel_query("query-transport-late-cancel")
        .await
        .unwrap_err();
    assert!(late_cancel
        .to_string()
        .contains("no active query with id query-transport-late-cancel was cancelled"));
    let reused_after_late_cancel = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-late-cancel"),
            "MATCH (u {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(reused_after_late_cancel.rows.len(), 1);
    let metrics = server.metrics();
    assert!(metrics.cancellations >= 1);
    assert!(metrics.cancelled_rejections >= 1);
    server.stop().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_cancellation_bypasses_saturated_query_connections() {
    struct ControlledQueryClient {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl QueryCellClient for ControlledQueryClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(QueryResultSet::new(Vec::new(), Vec::new()))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(ControlledQueryClient {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        }),
        QueryTransportServerConfig::default()
            .insecure_allow_unauthenticated()
            .with_max_connections(1)
            .with_reserved_control_connections(1)
            .with_max_concurrent_requests(1),
    )
    .await
    .unwrap();
    let client = TcpQueryCellClient::new(server.local_addr())
        .with_max_connections(1)
        .with_max_control_connections(1)
        .with_timeout(std::time::Duration::from_secs(2));
    let query_client = client.clone();
    let query = tokio::spawn(async move {
        query_client
            .execute_cypher_rows(
                QueryContext::new("reddit-home", "cancel-saturated-query"),
                "MATCH (u {id: 1}) RETURN u.id",
            )
            .await
    });
    started.notified().await;

    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        client.cancel_query("cancel-saturated-query"),
    )
    .await
    .expect("cancellation waited behind the saturated query connection")
    .unwrap();
    release.notify_one();
    assert!(query
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("query_transport_cancelled"));
    assert!(server.metrics().connections_accepted >= 2);
    server.stop().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn opencypher_local_executor_honors_cancellation_token() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-local-cancel", object_store).await;
    shard
        .write_edge(mutation(1, 2, "query-local-cancel-edge"))
        .await
        .unwrap();

    let token = QueryCancellationToken::new();
    token.cancel();
    let err = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-local-cancel").with_cancellation_token(token),
            "MATCH (u {id: 1})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(v) RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("query_cancelled"));
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn query_cardinality_stats_refresh_persists_edge_counts() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-cardinality-stats", object_store).await;
    for idx in 0..3 {
        shard
            .write_edge(mutation(
                1,
                10 + idx,
                &format!("query-cardinality-stats-edge-{idx}"),
            ))
            .await
            .unwrap();
    }
    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true)),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            10,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true)),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            11,
            VertexMetadata::default()
                .with_label("Subreddit")
                .with_property("active", VertexPropertyValue::Bool(false)),
        )
        .await
        .unwrap();
    for (idx, dst) in [10, 11].into_iter().enumerate() {
        shard
            .set_edge_metadata(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                1,
                dst,
                EdgeMetadata::default().with_property("weight", VertexPropertyValue::Integer(7)),
            )
            .await
            .unwrap_or_else(|err| panic!("edge metadata {idx} failed: {err}"));
    }

    let refresh = shard
        .refresh_edge_type_query_stats("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT")
        .await
        .unwrap();
    assert_eq!(refresh.count, 3);
    assert_eq!(refresh.stats.count, 3);
    assert_eq!(refresh.stats.read_epoch, refresh.read_epoch);
    assert!(refresh.stats.refreshed_at_ms > 0);
    assert_eq!(refresh.cell_id, "reddit-home");
    let key = keys::query_stats_edge_type("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT");
    let stored = shard.read_counter(&key).await.unwrap();
    assert_eq!(stored, 3);
    let record = read_query_stats_record_for_test(&shard, &key).await;
    assert_eq!(record, refresh.stats);

    let label_refresh = shard
        .refresh_vertex_label_query_stats("reddit-home", "User")
        .await
        .unwrap();
    assert_eq!(label_refresh.count, 2);
    assert_eq!(label_refresh.stats.count, 2);
    let label_key = keys::query_stats_vertex_label("reddit-home", "User");
    assert_eq!(shard.read_counter(&label_key).await.unwrap(), 2);
    let label_record = read_query_stats_record_for_test(&shard, &label_key).await;
    assert_eq!(label_record, label_refresh.stats);

    let active = VertexPropertyValue::Bool(true);
    let vertex_property_refresh = shard
        .refresh_vertex_property_query_stats("reddit-home", "active", &active)
        .await
        .unwrap();
    assert_eq!(vertex_property_refresh.count, 2);
    assert_eq!(vertex_property_refresh.stats.count, 2);
    assert_eq!(vertex_property_refresh.stats.distinct_values, 2);
    assert_eq!(vertex_property_refresh.stats.total_values, 3);
    assert_eq!(vertex_property_refresh.stats.most_common_count, 2);
    let active_key = encode_vertex_property_value_key(&active);
    let vertex_property_key =
        keys::query_stats_vertex_property("reddit-home", "active", &active_key);
    assert_eq!(shard.read_counter(&vertex_property_key).await.unwrap(), 2);
    let vertex_property_record =
        read_query_stats_record_for_test(&shard, &vertex_property_key).await;
    assert_eq!(vertex_property_record, vertex_property_refresh.stats);

    let weight = VertexPropertyValue::Integer(7);
    let edge_property_refresh = shard
        .refresh_edge_property_query_stats(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            "weight",
            &weight,
        )
        .await
        .unwrap();
    assert_eq!(edge_property_refresh.count, 2);
    assert_eq!(edge_property_refresh.stats.count, 2);
    assert_eq!(edge_property_refresh.stats.distinct_values, 1);
    assert_eq!(edge_property_refresh.stats.total_values, 2);
    assert_eq!(edge_property_refresh.stats.most_common_count, 2);
    let weight_key = encode_vertex_property_value_key(&weight);
    let edge_property_key = keys::query_stats_edge_property(
        "reddit-home",
        "USER_SUBSCRIBED_TO_SUBREDDIT",
        "weight",
        &weight_key,
    );
    assert_eq!(shard.read_counter(&edge_property_key).await.unwrap(), 2);
    let edge_property_record = read_query_stats_record_for_test(&shard, &edge_property_key).await;
    assert_eq!(edge_property_record, edge_property_refresh.stats);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn query_property_histogram_stats_refresh_persists_selectivity_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/query-histogram-stats", object_store).await;

    for (vertex, tier) in [(1, "common"), (2, "common"), (3, "rare")] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex,
                VertexMetadata::default()
                    .with_label("User")
                    .with_property("tier", VertexPropertyValue::String(tier.to_string())),
            )
            .await
            .unwrap();
    }
    for (idx, (src, dst, weight)) in [(1, 10, 7), (2, 20, 7), (3, 30, 9)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("query-histogram-stats-edge-{idx}"),
            })
            .await
            .unwrap();
        shard
            .set_edge_metadata(
                "reddit-home",
                "FOLLOWS",
                src,
                dst,
                EdgeMetadata::default()
                    .with_property("weight", VertexPropertyValue::Integer(weight)),
            )
            .await
            .unwrap();
    }

    let vertex_histogram = shard
        .refresh_vertex_property_histogram_query_stats("reddit-home", "tier")
        .await
        .unwrap();
    assert_eq!(vertex_histogram.stats.count, 3);
    assert_eq!(vertex_histogram.stats.distinct_values, 2);
    assert_eq!(vertex_histogram.stats.most_common_count, 2);
    let common =
        encode_vertex_property_value_key(&VertexPropertyValue::String("common".to_string()));
    let rare = encode_vertex_property_value_key(&VertexPropertyValue::String("rare".to_string()));
    assert_eq!(vertex_histogram.buckets.get(&common), Some(&2));
    assert_eq!(vertex_histogram.buckets.get(&rare), Some(&1));
    let vertex_histogram_key = keys::query_stats_vertex_property_histogram("reddit-home", "tier");
    let vertex_histogram_record =
        read_query_stats_record_for_test(&shard, &vertex_histogram_key).await;
    assert_eq!(vertex_histogram_record, vertex_histogram.stats);
    let rare_key = keys::query_stats_vertex_property("reddit-home", "tier", &rare);
    let rare_record = read_query_stats_record_for_test(&shard, &rare_key).await;
    assert_eq!(rare_record.count, 1);
    assert_eq!(rare_record.distinct_values, 2);

    let edge_histogram = shard
        .refresh_edge_property_histogram_query_stats("reddit-home", "FOLLOWS", "weight")
        .await
        .unwrap();
    assert_eq!(edge_histogram.stats.count, 3);
    assert_eq!(edge_histogram.stats.distinct_values, 2);
    assert_eq!(edge_histogram.stats.most_common_count, 2);
    let weight_7 = encode_vertex_property_value_key(&VertexPropertyValue::Integer(7));
    let weight_9 = encode_vertex_property_value_key(&VertexPropertyValue::Integer(9));
    assert_eq!(edge_histogram.buckets.get(&weight_7), Some(&2));
    assert_eq!(edge_histogram.buckets.get(&weight_9), Some(&1));
    let edge_histogram_key =
        keys::query_stats_edge_property_histogram("reddit-home", "FOLLOWS", "weight");
    let edge_histogram_record = read_query_stats_record_for_test(&shard, &edge_histogram_key).await;
    assert_eq!(edge_histogram_record, edge_histogram.stats);
    let weight_9_key =
        keys::query_stats_edge_property("reddit-home", "FOLLOWS", "weight", &weight_9);
    let weight_9_record = read_query_stats_record_for_test(&shard, &weight_9_key).await;
    assert_eq!(weight_9_record.count, 1);
    assert_eq!(weight_9_record.distinct_values, 2);

    let mut stale_batch = WriteBatch::new();
    stale_batch.put(
        keys::vertex_property_index("reddit-home", "tier", &common, 3),
        encode_u64(3),
    );
    stale_batch.put(
        keys::edge_property_index("reddit-home", "FOLLOWS", "weight", &weight_9, 1, 10),
        encode_u64(10),
    );
    shard.write_strict_for_test(stale_batch).await.unwrap();

    let filtered_vertex_histogram = shard
        .refresh_vertex_property_histogram_query_stats("reddit-home", "tier")
        .await
        .unwrap();
    assert_eq!(filtered_vertex_histogram.stats.count, 3);
    assert_eq!(filtered_vertex_histogram.stats.distinct_values, 2);
    assert_eq!(filtered_vertex_histogram.buckets.get(&common), Some(&2));
    assert_eq!(filtered_vertex_histogram.buckets.get(&rare), Some(&1));

    let filtered_edge_histogram = shard
        .refresh_edge_property_histogram_query_stats("reddit-home", "FOLLOWS", "weight")
        .await
        .unwrap();
    assert_eq!(filtered_edge_histogram.stats.count, 3);
    assert_eq!(filtered_edge_histogram.stats.distinct_values, 2);
    assert_eq!(filtered_edge_histogram.buckets.get(&weight_7), Some(&2));
    assert_eq!(filtered_edge_histogram.buckets.get(&weight_9), Some(&1));

    shard
        .set_vertex_metadata(
            "reddit-home",
            3,
            VertexMetadata::default()
                .with_label("User")
                .with_property("tier", VertexPropertyValue::String("common".to_string())),
        )
        .await
        .unwrap();
    shard
        .set_edge_metadata(
            "reddit-home",
            "FOLLOWS",
            3,
            30,
            EdgeMetadata::default().with_property("weight", VertexPropertyValue::Integer(7)),
        )
        .await
        .unwrap();

    let zeroed_vertex_histogram = shard
        .refresh_vertex_property_histogram_query_stats("reddit-home", "tier")
        .await
        .unwrap();
    assert_eq!(zeroed_vertex_histogram.stats.count, 3);
    assert_eq!(zeroed_vertex_histogram.stats.distinct_values, 1);
    assert_eq!(zeroed_vertex_histogram.buckets.get(&common), Some(&3));
    assert_eq!(zeroed_vertex_histogram.buckets.get(&rare), None);
    assert!(shard.read_remote(&rare_key).await.unwrap().is_none());
    assert!(shard
        .read_remote(&keys::query_stats_record_key(&rare_key))
        .await
        .unwrap()
        .is_none());

    let zeroed_edge_histogram = shard
        .refresh_edge_property_histogram_query_stats("reddit-home", "FOLLOWS", "weight")
        .await
        .unwrap();
    assert_eq!(zeroed_edge_histogram.stats.count, 3);
    assert_eq!(zeroed_edge_histogram.stats.distinct_values, 1);
    assert_eq!(zeroed_edge_histogram.buckets.get(&weight_7), Some(&3));
    assert_eq!(zeroed_edge_histogram.buckets.get(&weight_9), None);
    assert!(shard.read_remote(&weight_9_key).await.unwrap().is_none());
    assert!(shard
        .read_remote(&keys::query_stats_record_key(&weight_9_key))
        .await
        .unwrap()
        .is_none());

    let plan = shard
        .explain_opencypher_rows(
            QueryContext::new("reddit-home", "query-histogram-stats-explain"),
            "MATCH (u:User {tier: 'common'}) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(
        plan.groups[0].patterns[0].access,
        RowQueryAccess::VertexPropertyIndex {
            property: "tier".to_string(),
        }
    );
    assert_eq!(plan.groups[0].patterns[0].estimated_cardinality, 3);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[test]
fn cypher_float_properties_roundtrip_index_compare_and_order() {
    std::thread::Builder::new()
        .name("cypher-float-properties".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(cypher_float_properties_roundtrip_index_compare_and_order_inner());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[cfg(feature = "opencypher")]
async fn cypher_float_properties_roundtrip_index_compare_and_order_inner() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-float-properties", object_store).await;

    shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-float-create"),
            "CREATE (a:Event {id: 1, timestamp: 1783112914.47055})-[:NEXT]->\
             (b:Event {id: 2, timestamp: 1783112915.25})",
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            3,
            VertexMetadata::default()
                .with_label("Event")
                .with_property("timestamp", VertexPropertyValue::Float(QueryFloat(100.5))),
        )
        .await
        .unwrap();
    shard
        .set_edge_metadata(
            "reddit-home",
            "NEXT",
            1,
            2,
            EdgeMetadata::default()
                .with_property("confidence", VertexPropertyValue::Float(QueryFloat(0.75))),
        )
        .await
        .unwrap();
    for (vertex_id, score) in [
        (10, VertexPropertyValue::Integer(42)),
        (11, VertexPropertyValue::Float(QueryFloat(42.0))),
        (12, VertexPropertyValue::Integer(999)),
        (13, VertexPropertyValue::Float(QueryFloat(1.0))),
        (14, VertexPropertyValue::SignedInteger(-2)),
        (15, VertexPropertyValue::Float(QueryFloat(-2.0))),
        (16, VertexPropertyValue::Float(QueryFloat(-1.5))),
    ] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex_id,
                VertexMetadata::default()
                    .with_label("Score")
                    .with_property("score", score),
            )
            .await
            .unwrap();
    }
    for (vertex_id, score) in [
        (20, VertexPropertyValue::Float(QueryFloat(0.0))),
        (21, VertexPropertyValue::Float(QueryFloat(-0.0))),
        (22, VertexPropertyValue::Integer(0)),
    ] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex_id,
                VertexMetadata::default()
                    .with_label("ZeroScore")
                    .with_property("score", score),
            )
            .await
            .unwrap();
    }

    let exact = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-float-exact"),
            "MATCH (e:Event {timestamp: 1783112914.47055}) RETURN e.id",
        )
        .await
        .unwrap();
    assert_eq!(exact, QueryOutput::Vertices(vec![1]));

    let range_order = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-float-range"),
            "MATCH (e:Event) WHERE e.timestamp > 1783112914.47055 \
             RETURN e.id AS event, e.timestamp AS ts ORDER BY ts",
        )
        .await
        .unwrap();
    assert_eq!(
        range_order,
        QueryResultSet::new(
            vec![QueryColumn::new("event"), QueryColumn::new("ts")],
            vec![QueryRow::new(vec![
                QueryValue::VertexId(2),
                QueryValue::Property(VertexPropertyValue::Float(QueryFloat(1783112915.25))),
            ])],
        )
    );

    let edge_property = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-float-edge-property"),
            "MATCH (a)-[r:NEXT {confidence: 0.75}]->(b) RETURN b.id",
        )
        .await
        .unwrap();
    assert_eq!(edge_property, QueryOutput::Vertices(vec![2]));

    let integer_literal_exact = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-integer-exact"),
            "MATCH (s:Score {score: 42}) RETURN s.id AS score_id ORDER BY score_id",
        )
        .await
        .unwrap();
    assert_eq!(
        integer_literal_exact,
        QueryResultSet::new(
            vec![QueryColumn::new("score_id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(11)]),
            ],
        )
    );

    let float_literal_exact = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-float-exact"),
            "MATCH (s:Score {score: 42.0}) RETURN s.id AS score_id ORDER BY score_id",
        )
        .await
        .unwrap();
    assert_eq!(float_literal_exact, integer_literal_exact);

    let signed_literal_exact = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-signed-exact"),
            "MATCH (s:Score {score: -2}) RETURN s.id AS score_id ORDER BY score_id",
        )
        .await
        .unwrap();
    let signed_parameter_exact = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-signed-parameter")
                .with_parameter("score", VertexPropertyValue::SignedInteger(-2)),
            "MATCH (s:Score {score: $score}) RETURN s.id AS score_id ORDER BY score_id",
        )
        .await
        .unwrap();
    let expected_signed_exact = QueryResultSet::new(
        vec![QueryColumn::new("score_id")],
        vec![
            QueryRow::new(vec![QueryValue::VertexId(14)]),
            QueryRow::new(vec![QueryValue::VertexId(15)]),
        ],
    );
    assert_eq!(signed_literal_exact, expected_signed_exact);
    assert_eq!(signed_parameter_exact, expected_signed_exact);

    let signed_range = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-signed-range"),
            "MATCH (s:Score) WHERE s.score < 0.0 RETURN s.id AS score_id ORDER BY score_id",
        )
        .await
        .unwrap();
    assert_eq!(
        signed_range,
        QueryResultSet::new(
            vec![QueryColumn::new("score_id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(14)]),
                QueryRow::new(vec![QueryValue::VertexId(15)]),
                QueryRow::new(vec![QueryValue::VertexId(16)]),
            ],
        )
    );

    let float_range = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-float-range"),
            "MATCH (s:Score) WHERE s.score > 3.0 RETURN s.id AS score_id ORDER BY score_id",
        )
        .await
        .unwrap();
    let integer_range = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-integer-range"),
            "MATCH (s:Score) WHERE s.score > 3 RETURN s.id AS score_id ORDER BY score_id",
        )
        .await
        .unwrap();
    let expected_range = QueryResultSet::new(
        vec![QueryColumn::new("score_id")],
        vec![
            QueryRow::new(vec![QueryValue::VertexId(10)]),
            QueryRow::new(vec![QueryValue::VertexId(11)]),
            QueryRow::new(vec![QueryValue::VertexId(12)]),
        ],
    );
    assert_eq!(float_range, expected_range);
    assert_eq!(integer_range, expected_range);

    let expected_zero_exact = QueryResultSet::new(
        vec![QueryColumn::new("zero_id")],
        vec![
            QueryRow::new(vec![QueryValue::VertexId(20)]),
            QueryRow::new(vec![QueryValue::VertexId(21)]),
            QueryRow::new(vec![QueryValue::VertexId(22)]),
        ],
    );
    for (request_id, query, context) in [
        (
            "cypher-mixed-numeric-integer-zero-exact",
            "MATCH (z:ZeroScore {score: 0}) RETURN z.id AS zero_id ORDER BY zero_id",
            QueryContext::new("reddit-home", "cypher-mixed-numeric-integer-zero-exact"),
        ),
        (
            "cypher-mixed-numeric-positive-zero-exact",
            "MATCH (z:ZeroScore {score: 0.0}) RETURN z.id AS zero_id ORDER BY zero_id",
            QueryContext::new("reddit-home", "cypher-mixed-numeric-positive-zero-exact"),
        ),
        (
            "cypher-mixed-numeric-negative-zero-exact",
            "MATCH (z:ZeroScore {score: -0.0}) RETURN z.id AS zero_id ORDER BY zero_id",
            QueryContext::new("reddit-home", "cypher-mixed-numeric-negative-zero-exact"),
        ),
        (
            "cypher-mixed-numeric-negative-zero-parameter-exact",
            "MATCH (z:ZeroScore {score: $score}) RETURN z.id AS zero_id ORDER BY zero_id",
            QueryContext::new(
                "reddit-home",
                "cypher-mixed-numeric-negative-zero-parameter-exact",
            )
            .with_parameter("score", VertexPropertyValue::Float(QueryFloat(-0.0))),
        ),
    ] {
        let exact_zero = shard.execute_cypher_rows(context, query).await.unwrap();
        assert_eq!(exact_zero, expected_zero_exact, "{request_id}");
    }

    let numeric_order = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mixed-numeric-order"),
            "MATCH (s:Score) RETURN s.id AS score_id, s.score AS score ORDER BY score, score_id",
        )
        .await
        .unwrap();
    assert_eq!(
        numeric_order,
        QueryResultSet::new(
            vec![QueryColumn::new("score_id"), QueryColumn::new("score")],
            vec![
                QueryRow::new(vec![
                    QueryValue::VertexId(14),
                    QueryValue::Property(VertexPropertyValue::SignedInteger(-2)),
                ]),
                QueryRow::new(vec![
                    QueryValue::VertexId(15),
                    QueryValue::Property(VertexPropertyValue::Float(QueryFloat(-2.0))),
                ]),
                QueryRow::new(vec![
                    QueryValue::VertexId(16),
                    QueryValue::Property(VertexPropertyValue::Float(QueryFloat(-1.5))),
                ]),
                QueryRow::new(vec![
                    QueryValue::VertexId(13),
                    QueryValue::Property(VertexPropertyValue::Float(QueryFloat(1.0))),
                ]),
                QueryRow::new(vec![
                    QueryValue::VertexId(10),
                    QueryValue::Property(VertexPropertyValue::Integer(42)),
                ]),
                QueryRow::new(vec![
                    QueryValue::VertexId(11),
                    QueryValue::Property(VertexPropertyValue::Float(QueryFloat(42.0))),
                ]),
                QueryRow::new(vec![
                    QueryValue::VertexId(12),
                    QueryValue::Property(VertexPropertyValue::Integer(999)),
                ]),
            ],
        )
    );

    let low_key = encode_vertex_property_value_key(&VertexPropertyValue::Float(QueryFloat(1.5)));
    let high_key = encode_vertex_property_value_key(&VertexPropertyValue::Float(QueryFloat(2.5)));
    assert!(low_key < high_key);

    shard.close().await.unwrap();
}

#[cfg(feature = "json-properties")]
#[test]
fn json_property_values_preserve_integer_and_float_number_shapes() {
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!(42)),
        VertexPropertyValue::Integer(42)
    );
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!(-42)),
        VertexPropertyValue::SignedInteger(-42)
    );
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!(42.0)),
        VertexPropertyValue::Integer(42)
    );
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!(42.5)),
        VertexPropertyValue::Float(QueryFloat(42.5))
    );
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!(1783112914.47055)),
        VertexPropertyValue::Float(QueryFloat(1783112914.47055))
    );
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!(true)),
        VertexPropertyValue::Bool(true)
    );
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!("source")),
        VertexPropertyValue::String("source".to_string())
    );
    assert_eq!(
        VertexPropertyValue::from_json_value(&serde_json::json!({"raw": ["value"]})),
        VertexPropertyValue::String("{\"raw\":[\"value\"]}".to_string())
    );
}

#[test]
fn signed_integer_property_records_round_trip() {
    let metadata =
        VertexMetadata::default().with_property("offset", VertexPropertyValue::SignedInteger(-42));
    let encoded = encode_vertex_metadata(&metadata);
    assert_eq!(
        decode_vertex_metadata("vertex/signed", &encoded).unwrap(),
        metadata
    );
    assert_ne!(
        encode_vertex_property_value_key(&VertexPropertyValue::SignedInteger(-1)),
        encode_vertex_property_value_key(&VertexPropertyValue::Integer(1))
    );
}

#[tokio::test]
async fn batch_metadata_writes_are_idempotent_and_validate_edges() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/batch-metadata-writes", object_store).await;

    let alice = VertexMetadata::default()
        .with_label("User")
        .with_property("name", VertexPropertyValue::String("alice".to_string()));
    let bob = VertexMetadata::default()
        .with_label("User")
        .with_property("score", VertexPropertyValue::Float(QueryFloat(42.5)));
    assert_eq!(
        shard
            .set_vertex_metadata_batch("reddit-home", [(1, alice.clone()), (2, bob.clone())])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        shard
            .set_vertex_metadata_batch("reddit-home", [(1, alice.clone()), (2, bob)])
            .await
            .unwrap(),
        0
    );

    let alice_key = keys::vertex("reddit-home", 1);
    let stored_alice = decode_vertex_metadata(
        &alice_key,
        &shard.read_remote(&alice_key).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(stored_alice, alice);

    shard
        .bulk_import_edges(
            "reddit-home",
            "FOLLOWS",
            [(1, 2), (1, 3)],
            "batch-metadata-edges",
        )
        .await
        .unwrap();
    let since = EdgeMetadata::default()
        .with_property("since", VertexPropertyValue::Integer(2026))
        .with_property(
            "timestamp",
            VertexPropertyValue::Float(QueryFloat(1783112914.47055)),
        );
    let confidence = EdgeMetadata::default()
        .with_property("confidence", VertexPropertyValue::Float(QueryFloat(0.9)));
    assert_eq!(
        shard
            .set_edge_metadata_batch(
                "reddit-home",
                "FOLLOWS",
                [(1, 2, since.clone()), (1, 3, confidence.clone())],
            )
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        shard
            .set_edge_metadata_batch(
                "reddit-home",
                "FOLLOWS",
                [(1, 2, since.clone()), (1, 3, confidence)],
            )
            .await
            .unwrap(),
        0
    );

    let edge_key = keys::edge_metadata("reddit-home", "FOLLOWS", 1, 2);
    let stored_edge = decode_edge_metadata(
        &edge_key,
        &shard.read_remote(&edge_key).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(stored_edge, since);

    let missing = shard
        .set_edge_metadata_batch("reddit-home", "FOLLOWS", [(9, 9, EdgeMetadata::default())])
        .await
        .unwrap_err();
    assert!(missing
        .to_string()
        .contains("cannot set metadata for missing edge"));

    let conflicting = shard
        .set_edge_metadata_batch(
            "reddit-home",
            "FOLLOWS",
            [
                (
                    1,
                    2,
                    EdgeMetadata::default()
                        .with_property("weight", VertexPropertyValue::Integer(1)),
                ),
                (
                    1,
                    2,
                    EdgeMetadata::default()
                        .with_property("weight", VertexPropertyValue::Integer(2)),
                ),
            ],
        )
        .await
        .unwrap_err();
    assert!(conflicting
        .to_string()
        .contains("conflicting metadata values"));

    shard.close().await.unwrap();
}

#[tokio::test]
async fn import_vertex_metadata_batch_is_bounded_and_rejects_conflicts() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/import-vertex-metadata-batch", object_store).await;

    let alice = VertexMetadata::default()
        .with_label("User")
        .with_property("_fid", VertexPropertyValue::Integer(1))
        .with_property("name", VertexPropertyValue::String("alice".to_string()));
    assert_eq!(
        shard
            .import_vertex_metadata_batch("reddit-home", [(1, alice.clone())])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        shard
            .import_vertex_metadata_batch("reddit-home", [(1, alice.clone())])
            .await
            .unwrap(),
        0
    );

    let conflicting = VertexMetadata::default()
        .with_label("User")
        .with_property("_fid", VertexPropertyValue::Integer(1))
        .with_property(
            "name",
            VertexPropertyValue::String("alice-updated".to_string()),
        );
    let err = shard
        .import_vertex_metadata_batch("reddit-home", [(1, conflicting)])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("different metadata during import"));

    let key = keys::vertex("reddit-home", 1);
    let stored =
        decode_vertex_metadata(&key, &shard.read_remote(&key).await.unwrap().unwrap()).unwrap();
    assert_eq!(stored, alice);

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn relationship_property_delete_retry_does_not_expand_to_recreated_structural_edge() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/relationship-property-delete-stable-scope",
        object_store,
    )
    .await;
    let metadata = EdgeMetadata::default().with_property(
        "chunk_id",
        VertexPropertyValue::String("chunk-a".to_string()),
    );
    shard
        .import_relationships_batch(
            "reddit-home",
            "RELATES",
            [RelationshipMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "RELATES".to_string(),
                src: 40,
                dst: 41,
                relationship_id: 100,
                metadata: metadata.clone(),
            }],
            "relationship-property-delete-stable-scope-import",
        )
        .await
        .unwrap();
    shard
        .set_edge_metadata("reddit-home", "RELATES", 40, 41, metadata.clone())
        .await
        .unwrap();

    let context = QueryContext::new("reddit-home", "relationship-property-delete-stable-scope");
    assert_eq!(
        shard
            .delete_relationships_by_property_values_batch(
                &context,
                "RELATES",
                "chunk_id",
                vec![VertexPropertyValue::String("chunk-a".to_string())],
            )
            .await
            .unwrap(),
        (1, 0)
    );

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "RELATES".to_string(),
            src: 40,
            dst: 41,
            idempotency_key: "relationship-property-delete-recreated-edge".to_string(),
        })
        .await
        .unwrap();
    shard
        .set_edge_metadata("reddit-home", "RELATES", 40, 41, metadata)
        .await
        .unwrap();

    assert_eq!(
        shard
            .delete_relationships_by_property_values_batch(
                &context,
                "RELATES",
                "chunk_id",
                vec![VertexPropertyValue::String("chunk-a".to_string())],
            )
            .await
            .unwrap(),
        (0, 0)
    );
    let structural_retry = shard
        .delete_edge_mutations_batch(
            "reddit-home",
            [EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "RELATES".to_string(),
                src: 40,
                dst: 41,
                idempotency_key:
                    "relationship-property-delete-stable-scope.delete.RELATES.40.41.edge"
                        .to_string(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(structural_retry.deleted, 0);
    assert_eq!(structural_retry.already_deleted, 1);
    assert!(shard
        .edge_exists("reddit-home", "RELATES", 40, 41)
        .await
        .unwrap());
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn multigraph_relationship_import_preserves_parallel_relationship_rows() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/multigraph-relationship-import", object_store).await;

    let result = shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [
                RelationshipMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    dst: 2,
                    relationship_id: 100,
                    metadata: EdgeMetadata::default()
                        .with_property("_fid", VertexPropertyValue::Integer(100))
                        .with_property("weight", VertexPropertyValue::Integer(7)),
                },
                RelationshipMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    dst: 2,
                    relationship_id: 101,
                    metadata: EdgeMetadata::default()
                        .with_property("_fid", VertexPropertyValue::Integer(101))
                        .with_property("weight", VertexPropertyValue::Integer(9)),
                },
            ],
            "multigraph-relationship-import",
        )
        .await
        .unwrap();
    assert_eq!(result.relationships_inserted, 2);
    assert_eq!(result.structural_edges_inserted, 1);
    assert_eq!(
        shard.out_degree("reddit-home", "FOLLOWS", 1).await.unwrap(),
        1
    );

    let replay = shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [
                RelationshipMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    dst: 2,
                    relationship_id: 100,
                    metadata: EdgeMetadata::default()
                        .with_property("_fid", VertexPropertyValue::Integer(100))
                        .with_property("weight", VertexPropertyValue::Integer(7)),
                },
                RelationshipMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    dst: 2,
                    relationship_id: 101,
                    metadata: EdgeMetadata::default()
                        .with_property("_fid", VertexPropertyValue::Integer(101))
                        .with_property("weight", VertexPropertyValue::Integer(9)),
                },
            ],
            "multigraph-relationship-import",
        )
        .await
        .unwrap();
    assert_eq!(replay, result);

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "multigraph-relationship-rows"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) \
             RETURN r._fid AS fid, r.weight AS weight ORDER BY fid",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("fid"), QueryColumn::new("weight")],
            vec![
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::Integer(100)),
                    QueryValue::Property(VertexPropertyValue::Integer(7)),
                ]),
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::Integer(101)),
                    QueryValue::Property(VertexPropertyValue::Integer(9)),
                ]),
            ],
        )
    );
    let cache_metrics_before = shard.graph_cache_metrics();
    let cached_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "multigraph-relationship-rows-cached"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) \
             RETURN r._fid AS fid, r.weight AS weight ORDER BY fid",
        )
        .await
        .unwrap();
    assert_eq!(cached_rows, rows);
    let cache_metrics_after = shard.graph_cache_metrics();
    assert!(
        cache_metrics_after.relationship_rows_hits > cache_metrics_before.relationship_rows_hits
    );
    assert!(shard.graph_cache_entry_counts().await.relationship_row_sets >= 1);

    let indexed = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "multigraph-relationship-indexed"),
            "MATCH (u)-[r:FOLLOWS {weight: 9}]->(v) RETURN r._fid AS fid",
        )
        .await
        .unwrap();
    assert_eq!(
        indexed,
        QueryResultSet::new(
            vec![QueryColumn::new("fid")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::Integer(101)
            )])],
        )
    );
    let property_cache_metrics_before = shard.graph_cache_metrics();
    let exact_indexed = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "multigraph-relationship-indexed-exact"),
            "MATCH (u {id: 1})-[r:FOLLOWS {weight: 9}]->(v {id: 2}) RETURN r._fid AS fid",
        )
        .await
        .unwrap();
    assert_eq!(exact_indexed, indexed);
    let exact_indexed_cached = shard
        .execute_cypher_rows(
            QueryContext::new(
                "reddit-home",
                "multigraph-relationship-indexed-exact-cached",
            ),
            "MATCH (u {id: 1})-[r:FOLLOWS {weight: 9}]->(v {id: 2}) RETURN r._fid AS fid",
        )
        .await
        .unwrap();
    assert_eq!(exact_indexed_cached, indexed);
    let property_cache_metrics_after = shard.graph_cache_metrics();
    assert!(
        property_cache_metrics_after.relationship_property_rows_hits
            > property_cache_metrics_before.relationship_property_rows_hits
    );
    assert!(
        shard
            .graph_cache_entry_counts()
            .await
            .relationship_property_row_sets
            >= 1
    );

    let conflict = shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [RelationshipMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 3,
                relationship_id: 100,
                metadata: EdgeMetadata::default()
                    .with_property("_fid", VertexPropertyValue::Integer(100)),
            }],
            "multigraph-relationship-conflict",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(conflict, GraphError::IdempotencyConflict { .. }),
        "{conflict:?}"
    );

    let generated = shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "multigraph-generated-after-import".to_string(),
            },
            EdgeMetadata::default()
                .with_property("_fid", VertexPropertyValue::Integer(102))
                .with_property("weight", VertexPropertyValue::Integer(11)),
        )
        .await
        .unwrap();
    assert_eq!(generated.relationship_id, 102);
    assert!(!generated.structural_edge_inserted);
    assert_eq!(
        shard.out_degree("reddit-home", "FOLLOWS", 1).await.unwrap(),
        1
    );
    let rows_after_create = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "multigraph-relationship-rows-after-create"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) \
             RETURN r._fid AS fid, r.weight AS weight ORDER BY fid",
        )
        .await
        .unwrap();
    assert_eq!(
        rows_after_create,
        QueryResultSet::new(
            vec![QueryColumn::new("fid"), QueryColumn::new("weight")],
            vec![
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::Integer(100)),
                    QueryValue::Property(VertexPropertyValue::Integer(7)),
                ]),
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::Integer(101)),
                    QueryValue::Property(VertexPropertyValue::Integer(9)),
                ]),
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::Integer(102)),
                    QueryValue::Property(VertexPropertyValue::Integer(11)),
                ]),
            ],
        )
    );

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 4,
            idempotency_key: "multigraph-structural-only-edge".to_string(),
        })
        .await
        .unwrap();
    let anonymous_fanout = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "multigraph-anonymous-source-fanout"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id AS dst ORDER BY dst",
        )
        .await
        .unwrap();
    assert_eq!(
        anonymous_fanout,
        QueryResultSet::new(
            vec![QueryColumn::new("dst")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(4)]),
            ],
        )
    );
    let source_cache_metrics_before = shard.graph_cache_metrics();
    let cached_anonymous_fanout = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "multigraph-anonymous-source-fanout-cached"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id AS dst ORDER BY dst",
        )
        .await
        .unwrap();
    assert_eq!(cached_anonymous_fanout, anonymous_fanout);
    let source_cache_metrics_after = shard.graph_cache_metrics();
    assert!(
        source_cache_metrics_after.relationship_rows_hits
            > source_cache_metrics_before.relationship_rows_hits
    );

    let resident_bytes = shard.graph_cache_resident_bytes().await;
    let memory_limits = GraphMemoryConfig::default();
    assert!(resident_bytes.relationship_rows > 0);
    assert!(resident_bytes.source_relationship_rows > 0);
    assert!(resident_bytes.relationship_property_rows > 0);
    assert!(resident_bytes.relationship_rows <= memory_limits.max_relationship_rows_bytes);
    assert!(
        resident_bytes.source_relationship_rows <= memory_limits.max_source_relationship_rows_bytes
    );
    assert!(
        resident_bytes.relationship_property_rows
            <= memory_limits.max_relationship_property_rows_bytes
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn oversized_relationship_results_are_not_retained() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let memory = GraphMemoryConfig {
        max_relationship_rows_bytes: 1,
        max_source_relationship_rows_bytes: 1,
        max_relationship_property_rows_bytes: 1,
        ..GraphMemoryConfig::default()
    };
    let shard = GraphShard::open_standalone_writer_with_memory_options(
        "graph/relationship-cache-byte-limits",
        object_store,
        GraphOpenOptions::default(),
        memory,
    )
    .await
    .unwrap();

    shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [RelationshipMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                relationship_id: 100,
                metadata: EdgeMetadata::default()
                    .with_property("_fid", VertexPropertyValue::Integer(100))
                    .with_property("weight", VertexPropertyValue::Integer(9))
                    .with_property(
                        "description",
                        VertexPropertyValue::String("large relationship metadata".repeat(32)),
                    ),
            }],
            "relationship-cache-byte-limits",
        )
        .await
        .unwrap();

    let exact = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "relationship-cache-exact"),
            "MATCH ({id: 1})-[r:FOLLOWS]->({id: 2}) RETURN r._fid AS fid",
        )
        .await
        .unwrap();
    assert_eq!(exact.rows.len(), 1);

    let source = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "relationship-cache-source"),
            "MATCH ({id: 1})-[:FOLLOWS]->(v) RETURN v.id AS dst",
        )
        .await
        .unwrap();
    assert_eq!(source.rows.len(), 1);

    let property = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "relationship-cache-property"),
            "MATCH ()-[r:FOLLOWS {weight: 9}]->() RETURN r._fid AS fid",
        )
        .await
        .unwrap();
    assert_eq!(property.rows.len(), 1);

    let counts = shard.graph_cache_entry_counts().await;
    let resident = shard.graph_cache_resident_bytes().await;
    assert_eq!(counts.relationship_row_sets, 0);
    assert_eq!(counts.relationship_property_row_sets, 0);
    assert_eq!(resident.relationship_rows, 0);
    assert_eq!(resident.source_relationship_rows, 0);
    assert_eq!(resident.relationship_property_rows, 0);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn high_degree_source_results_respect_cache_byte_limit_without_truncation() {
    const DEGREE: u64 = 1_000;
    const SOURCE_CACHE_BYTES: usize = 4 * 1024;

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let memory = GraphMemoryConfig {
        max_source_relationship_rows_bytes: SOURCE_CACHE_BYTES,
        ..GraphMemoryConfig::default()
    };
    let shard = GraphShard::open_standalone_writer_with_memory_options(
        "graph/high-degree-source-cache-byte-limit",
        object_store,
        GraphOpenOptions::default(),
        memory,
    )
    .await
    .unwrap();
    let relationships = (0..DEGREE).map(|offset| RelationshipMutation {
        cell_id: "reddit-home".to_string(),
        edge_type: "FOLLOWS".to_string(),
        src: 1,
        dst: 10_000 + offset,
        relationship_id: 20_000 + offset,
        metadata: EdgeMetadata::default(),
    });
    shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            relationships,
            "high-degree-source-cache-byte-limit",
        )
        .await
        .unwrap();

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "high-degree-source-cache-query"),
            "MATCH ({id: 1})-[:FOLLOWS]->(v) RETURN v.id AS dst ORDER BY dst",
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), DEGREE as usize);
    assert_eq!(
        rows.rows.first().unwrap().values,
        vec![QueryValue::VertexId(10_000)]
    );
    assert_eq!(
        rows.rows.last().unwrap().values,
        vec![QueryValue::VertexId(10_999)]
    );

    let resident = shard.graph_cache_resident_bytes().await;
    assert_eq!(resident.source_relationship_rows, 0);
    assert!(resident.source_relationship_rows <= SOURCE_CACHE_BYTES);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn normal_relationship_create_generates_multigraph_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/multigraph-normal-create", object_store).await;

    let first = shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "normal-rel-create-1".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(1)),
        )
        .await
        .unwrap();
    let replay = shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "normal-rel-create-1".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(1)),
        )
        .await
        .unwrap();
    let second = shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "normal-rel-create-2".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(2)),
        )
        .await
        .unwrap();

    assert_eq!(replay, first);
    assert_eq!(first.relationship_id, 1);
    assert_eq!(second.relationship_id, 2);
    assert!(first.structural_edge_inserted);
    assert!(!second.structural_edge_inserted);
    assert_eq!(
        shard.out_degree("reddit-home", "FOLLOWS", 1).await.unwrap(),
        1
    );

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "normal-rel-create-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN r.rank AS rank ORDER BY rank",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("rank")],
            vec![
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::Integer(1))]),
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::Integer(2))]),
            ],
        )
    );

    let missing_edge_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "normal-rel-create-missing-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 99}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert!(missing_edge_rows.rows.is_empty());

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn relationship_rows_require_live_structural_edge() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/multigraph-structural-delete-hides-rows",
        object_store,
    )
    .await;

    shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "structural-delete-rel-1".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(1)),
        )
        .await
        .unwrap();
    shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "structural-delete-rel-2".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(2)),
        )
        .await
        .unwrap();

    let deleted = shard
        .delete_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "structural-delete-edge".to_string(),
        })
        .await
        .unwrap();
    assert!(deleted.deleted);
    assert!(!shard
        .edge_exists("reddit-home", "FOLLOWS", 1, 2)
        .await
        .unwrap());

    let endpoint_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "structural-delete-endpoint-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN r.rank AS rank ORDER BY rank",
        )
        .await
        .unwrap();
    assert!(endpoint_rows.rows.is_empty());

    let property_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "structural-delete-property-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS {rank: 2}]->(v {id: 2}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert!(property_rows.rows.is_empty());

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn structural_edge_delete_removes_relationships_before_recreate() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/multigraph-structural-delete-recreate", object_store).await;

    shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "structural-recreate-rel-1".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(1)),
        )
        .await
        .unwrap();
    shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "structural-recreate-rel-2".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(2)),
        )
        .await
        .unwrap();

    let deleted = shard
        .delete_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "structural-recreate-delete".to_string(),
        })
        .await
        .unwrap();
    assert!(deleted.deleted);

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "structural-recreate-edge".to_string(),
        })
        .await
        .unwrap();

    let resurrected_property_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "structural-recreate-property-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS {rank: 2}]->(v {id: 2}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert!(resurrected_property_rows.rows.is_empty());

    let endpoint_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "structural-recreate-endpoint-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN v.id AS dst",
        )
        .await
        .unwrap();
    assert_eq!(
        endpoint_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("dst")],
            vec![QueryRow::new(vec![QueryValue::VertexId(2)])],
        )
    );

    let source_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "structural-recreate-source-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id AS dst",
        )
        .await
        .unwrap();
    assert_eq!(
        source_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("dst")],
            vec![QueryRow::new(vec![QueryValue::VertexId(2)])],
        )
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn batch_structural_edge_delete_removes_relationships_before_recreate() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/multigraph-batch-structural-delete-recreate",
        object_store,
    )
    .await;

    shard
        .create_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "LIKES".to_string(),
                src: 5,
                dst: 6,
                idempotency_key: "batch-structural-recreate-rel".to_string(),
            },
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(9)),
        )
        .await
        .unwrap();
    let deleted = shard
        .delete_edges_batch(
            "reddit-home",
            "LIKES",
            [(5, 6)],
            "batch-structural-recreate-delete",
        )
        .await
        .unwrap();
    assert_eq!(deleted.deleted, 1);

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "LIKES".to_string(),
            src: 5,
            dst: 6,
            idempotency_key: "batch-structural-recreate-edge".to_string(),
        })
        .await
        .unwrap();

    let property_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "batch-structural-recreate-property-read"),
            "MATCH (u {id: 5})-[r:LIKES {rank: 9}]->(v {id: 6}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert!(property_rows.rows.is_empty());

    let endpoint_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "batch-structural-recreate-endpoint-read"),
            "MATCH (u {id: 5})-[r:LIKES]->(v {id: 6}) RETURN v.id AS dst",
        )
        .await
        .unwrap();
    assert_eq!(
        endpoint_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("dst")],
            vec![QueryRow::new(vec![QueryValue::VertexId(6)])],
        )
    );

    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_vertex_requires_detach_and_detach_cascades_incident_edges() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/delete-vertex-detach", object_store).await;

    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("alice".to_string())),
        )
        .await
        .unwrap();
    shard
        .write_edge(mutation(1, 2, "vertex-delete-edge-out"))
        .await
        .unwrap();

    let err = shard
        .delete_vertex("reddit-home", 1, "vertex-delete-with-edge")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::UnsupportedQuery {
            dialect: "Graph",
            feature
        } if feature.contains("requires DETACH")
    ));
    assert!(shard
        .read_remote(&keys::vertex("reddit-home", 1))
        .await
        .unwrap()
        .is_some());

    shard
        .write_edge(typed_mutation(
            "reddit-home",
            "LIKES",
            3,
            1,
            "vertex-delete-edge-in",
        ))
        .await
        .unwrap();
    shard
        .create_relationship(
            typed_mutation("reddit-home", "MENTIONS", 1, 4, "vertex-delete-rel"),
            EdgeMetadata::default().with_property("rank", VertexPropertyValue::Integer(1)),
        )
        .await
        .unwrap();

    let deleted = shard
        .detach_delete_vertex("reddit-home", 1, "vertex-detach-delete")
        .await
        .unwrap();
    assert!(deleted.vertex_deleted);
    assert_eq!(deleted.incident_edges_deleted, 3);
    assert_eq!(deleted.relationships_deleted, 1);
    assert!(!shard
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
        .await
        .unwrap());
    assert!(!shard
        .edge_exists("reddit-home", "LIKES", 3, 1)
        .await
        .unwrap());
    assert!(!shard
        .edge_exists("reddit-home", "MENTIONS", 1, 4)
        .await
        .unwrap());
    assert!(shard
        .read_remote(&keys::vertex("reddit-home", 1))
        .await
        .unwrap()
        .is_none());

    let retry = shard
        .detach_delete_vertex("reddit-home", 1, "vertex-detach-delete")
        .await
        .unwrap();
    assert_eq!(retry, deleted);

    let tombstone_only = shard
        .delete_vertex("reddit-home", 4, "vertex-delete-tombstone-only")
        .await
        .unwrap();
    assert!(!tombstone_only.vertex_deleted);
    assert_eq!(tombstone_only.incident_edges_deleted, 0);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn drop_cell_purges_namespace_and_blocks_future_writes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/drop-cell", object_store).await;

    shard
        .write_edge(mutation(1, 2, "drop-cell-edge"))
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default().with_label("User"),
        )
        .await
        .unwrap();

    let dropped = shard.drop_cell("reddit-home", "drop-cell-1").await.unwrap();
    assert!(!dropped.already_dropped);
    assert!(dropped.deleted_keys > 0);
    assert!(dropped.batches > 0);
    assert!(shard
        .read_remote(&keys::cell_drop_marker("reddit-home"))
        .await
        .unwrap()
        .is_some());

    let mut iter = shard
        .scan_remote_prefix(&keys::cell_prefix("reddit-home"))
        .await
        .unwrap();
    assert!(iter.next().await.unwrap().is_none());

    let retry = shard.drop_cell("reddit-home", "drop-cell-1").await.unwrap();
    assert_eq!(retry, dropped);
    let err = shard
        .write_edge(mutation(1, 3, "drop-cell-write-after"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellDropped {
            operation: "write_edge",
            cell_id
        } if cell_id == "reddit-home"
    ));
    shard.close().await.unwrap();
}

#[tokio::test]
async fn pending_drop_marker_blocks_writes_and_drop_cell_finalizes_cleanup() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/drop-cell-pending", object_store).await;

    shard
        .write_edge(mutation(1, 2, "pending-drop-seed"))
        .await
        .unwrap();
    let mut batch = GraphWriteBatch::new();
    batch.put(
        keys::cell_drop_pending_marker("reddit-home").as_bytes(),
        encode_u64(77),
    );
    shard
        .write_graph_batch_strict("reddit-home", "drop_cell", batch)
        .await
        .unwrap();

    let err = shard
        .write_edge(mutation(1, 3, "pending-drop-write-after"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellDropped {
            operation: "write_edge",
            cell_id
        } if cell_id == "reddit-home"
    ));
    let err = shard
        .edge_exists("reddit-home", "FOLLOWS", 1, 2)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellDropped {
            operation: "edge_exists",
            cell_id
        } if cell_id == "reddit-home"
    ));
    let err = shard
        .out_neighbors("reddit-home", "FOLLOWS", 1)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellDropped {
            operation: "out_neighbors",
            cell_id
        } if cell_id == "reddit-home"
    ));
    let err = shard
        .out_degree("reddit-home", "FOLLOWS", 1)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellDropped {
            operation: "out_degree",
            cell_id
        } if cell_id == "reddit-home"
    ));
    let err = shard.current_epoch("reddit-home").await.unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellDropped {
            operation: "current_epoch",
            cell_id
        } if cell_id == "reddit-home"
    ));

    let dropped = shard
        .drop_cell("reddit-home", "pending-drop-finalize")
        .await
        .unwrap();
    assert_eq!(dropped.marker_epoch, 77);
    assert!(!dropped.already_dropped);
    assert!(dropped.deleted_keys > 0);
    assert!(shard
        .read_remote(&keys::cell_drop_pending_marker("reddit-home"))
        .await
        .unwrap()
        .is_none());
    assert!(shard
        .read_remote(&keys::cell_drop_marker("reddit-home"))
        .await
        .unwrap()
        .is_some());
    let mut iter = shard
        .scan_remote_prefix(&keys::cell_prefix("reddit-home"))
        .await
        .unwrap();
    assert!(iter.next().await.unwrap().is_none());
    shard.close().await.unwrap();
}

#[tokio::test]
async fn resumed_drop_cell_preserves_pending_marker_until_final_commit() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/resumed-drop-keeps-pending-marker", object_store).await;
    let pending_epoch = 42;
    let pending_key = keys::cell_drop_pending_marker("reddit-home");
    let mut batch = GraphWriteBatch::new();
    batch.put(pending_key.as_bytes(), encode_u64(pending_epoch));
    shard
        .write_graph_batch_strict("reddit-home", "drop_cell", batch)
        .await
        .unwrap();

    let result = shard
        .drop_cell("reddit-home", "resume-empty-pending-drop")
        .await
        .unwrap();
    assert_eq!(result.marker_epoch, pending_epoch);
    assert_eq!(result.deleted_keys, 0);
    assert!(!result.already_dropped);
    assert!(shard.read_remote(&pending_key).await.unwrap().is_none());
    assert_eq!(
        shard
            .read_counter(&keys::cell_drop_marker("reddit-home"))
            .await
            .unwrap(),
        pending_epoch
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn drop_cell_preserves_an_open_slatedb_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/drop-cell-open-snapshot", object_store).await;

    shard
        .write_edge(mutation(1, 2, "drop-active-reader-seed"))
        .await
        .unwrap();
    let snapshot = shard.snapshot("reddit-home").await.unwrap();
    assert_eq!(
        snapshot
            .out_neighbors("USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2]
    );
    let dropped = shard
        .drop_cell("reddit-home", "drop-active-reader")
        .await
        .unwrap();
    assert_eq!(
        snapshot
            .out_neighbors("USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2],
        "the SlateDB snapshot remains readable after current keys are dropped"
    );
    assert_eq!(dropped.marker_epoch, 2);
    assert!(!dropped.already_dropped);
    assert!(dropped.deleted_keys > 0);
    let mut iter = shard
        .scan_remote_prefix(&keys::cell_prefix("reddit-home"))
        .await
        .unwrap();
    assert!(iter.next().await.unwrap().is_none());
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn deleted_relationship_id_pointer_does_not_block_reimport() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/multigraph-reimport-deleted-id", object_store).await;

    let imported = shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [RelationshipMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                relationship_id: 200,
                metadata: EdgeMetadata::default()
                    .with_property("_fid", VertexPropertyValue::Integer(200))
                    .with_property("rank", VertexPropertyValue::Integer(1)),
            }],
            "relationship-reimport-seed",
        )
        .await
        .unwrap();
    assert_eq!(imported.relationships_inserted, 1);

    let deleted = shard
        .delete_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "relationship-reimport-delete".to_string(),
            },
            200,
        )
        .await
        .unwrap();
    assert!(deleted.deleted);

    let reimported = shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [RelationshipMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 3,
                relationship_id: 200,
                metadata: EdgeMetadata::default()
                    .with_property("_fid", VertexPropertyValue::Integer(200))
                    .with_property("rank", VertexPropertyValue::Integer(2)),
            }],
            "relationship-reimport-same-id",
        )
        .await
        .unwrap();
    assert_eq!(reimported.relationships_inserted, 1);

    let old_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "relationship-reimport-old-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert!(old_rows.rows.is_empty());

    let new_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "relationship-reimport-new-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 3}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert_eq!(
        new_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("rank")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::Integer(2)
            )])],
        )
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn reimported_relationship_id_same_edge_reuses_deleted_id() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/multigraph-reimport-same-edge", object_store).await;

    let imported = shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [RelationshipMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 10,
                dst: 20,
                relationship_id: 201,
                metadata: EdgeMetadata::default()
                    .with_property("_fid", VertexPropertyValue::Integer(201))
                    .with_property("rank", VertexPropertyValue::Integer(1)),
            }],
            "relationship-reimport-same-edge-seed",
        )
        .await
        .unwrap();
    assert_eq!(imported.relationships_inserted, 1);

    let deleted = shard
        .delete_relationship(
            EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 10,
                dst: 20,
                idempotency_key: "relationship-reimport-same-edge-delete".to_string(),
            },
            201,
        )
        .await
        .unwrap();
    assert!(deleted.deleted);

    let reimported = shard
        .import_relationships_batch(
            "reddit-home",
            "FOLLOWS",
            [RelationshipMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 10,
                dst: 20,
                relationship_id: 201,
                metadata: EdgeMetadata::default()
                    .with_property("_fid", VertexPropertyValue::Integer(201))
                    .with_property("rank", VertexPropertyValue::Integer(3)),
            }],
            "relationship-reimport-same-edge-new",
        )
        .await
        .unwrap();
    assert_eq!(reimported.relationships_inserted, 1);

    let endpoint_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "relationship-reimport-same-edge-read"),
            "MATCH (u {id: 10})-[r:FOLLOWS]->(v {id: 20}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert_eq!(
        endpoint_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("rank")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::Integer(3)
            )])],
        )
    );

    let property_rows = shard
        .execute_cypher_rows(
            QueryContext::new(
                "reddit-home",
                "relationship-reimport-same-edge-property-read",
            ),
            "MATCH (u {id: 10})-[r:FOLLOWS {rank: 3}]->(v {id: 20}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert_eq!(
        property_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("rank")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::Integer(3)
            )])],
        )
    );

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_create_set_and_delete_target_individual_relationships() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-multigraph-create", object_store).await;

    let first = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-multi-create-1"),
            "CREATE (u {id: 1})-[r:FOLLOWS {rank: 7}]->(v {id: 2})",
        )
        .await
        .unwrap();
    let second = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-multi-create-2"),
            "CREATE (u {id: 1})-[r:FOLLOWS {rank: 9}]->(v {id: 2})",
        )
        .await
        .unwrap();
    assert!(matches!(
        first,
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 1,
            created_relationships: 1,
            ..
        })
    ));
    assert!(matches!(
        second,
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 0,
            created_relationships: 1,
            ..
        })
    ));
    assert_eq!(
        shard.out_degree("reddit-home", "FOLLOWS", 1).await.unwrap(),
        1
    );

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-multi-create-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN r.rank AS rank ORDER BY rank",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("rank")],
            vec![
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::Integer(7))]),
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::Integer(9))]),
            ],
        )
    );

    let updated = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-multi-set"),
            "MATCH (u {id: 1})-[r:FOLLOWS {rank: 7}]->(v {id: 2}) SET r.rank = 11",
        )
        .await
        .unwrap();
    assert_eq!(
        updated,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            updated_relationships: 1,
            ..QueryMutationResult::default()
        })
    );

    let after_update = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-multi-set-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN r.rank AS rank ORDER BY rank",
        )
        .await
        .unwrap();
    assert_eq!(
        after_update,
        QueryResultSet::new(
            vec![QueryColumn::new("rank")],
            vec![
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::Integer(9))]),
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::Integer(11))]),
            ],
        )
    );

    let deleted_one = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-multi-delete-one"),
            "MATCH (u {id: 1})-[r:FOLLOWS {rank: 9}]->(v {id: 2}) DELETE r",
        )
        .await
        .unwrap();
    assert_eq!(
        deleted_one,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            deleted_edges: 1,
            ..QueryMutationResult::default()
        })
    );
    assert_eq!(
        shard.out_degree("reddit-home", "FOLLOWS", 1).await.unwrap(),
        1
    );

    let remaining = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-multi-delete-one-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert_eq!(
        remaining,
        QueryResultSet::new(
            vec![QueryColumn::new("rank")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::Integer(11)
            )])],
        )
    );

    let deleted_last = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-multi-delete-last"),
            "MATCH (u {id: 1})-[r:FOLLOWS {rank: 11}]->(v {id: 2}) DELETE r",
        )
        .await
        .unwrap();
    assert_eq!(
        deleted_last,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            deleted_edges: 1,
            ..QueryMutationResult::default()
        })
    );
    assert_eq!(
        shard.out_degree("reddit-home", "FOLLOWS", 1).await.unwrap(),
        0
    );

    let empty = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-multi-delete-last-read"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) RETURN r.rank AS rank",
        )
        .await
        .unwrap();
    assert!(empty.rows.is_empty());

    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn query_stats_background_refresh_job_publishes_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard =
        Arc::new(open_test_shard("graph/query-stats-background-refresh", object_store).await);
    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("tier", VertexPropertyValue::String("rare".to_string())),
        )
        .await
        .unwrap();
    let key = keys::query_stats_vertex_label("reddit-home", "User");
    let histogram_key = keys::query_stats_vertex_property_histogram("reddit-home", "tier");
    let handle = Arc::clone(&shard)
        .start_query_stats_refresh_job(
            vec![
                QueryStatsRefreshSpec::new(
                    "reddit-home",
                    QueryCardinalityStatsKind::VertexLabel {
                        label: "User".to_string(),
                    },
                ),
                QueryStatsRefreshSpec::vertex_property_histogram("reddit-home", "tier"),
            ],
            std::time::Duration::from_millis(10),
        )
        .unwrap();
    for _ in 0..100 {
        if shard.read_counter(&key).await.unwrap() == 1
            && shard.read_counter(&histogram_key).await.unwrap() == 1
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    handle.stop().await.unwrap();
    assert_eq!(shard.read_counter(&key).await.unwrap(), 1);
    let record = read_query_stats_record_for_test(&shard, &key).await;
    assert_eq!(record.count, 1);
    assert!(record.refreshed_at_ms > 0);
    assert_eq!(shard.read_counter(&histogram_key).await.unwrap(), 1);
    let histogram_record = read_query_stats_record_for_test(&shard, &histogram_key).await;
    assert_eq!(histogram_record.count, 1);
    assert_eq!(histogram_record.distinct_values, 1);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn query_stats_background_refresh_job_stops_during_long_interval() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(open_test_shard("graph/query-stats-background-stop", object_store).await);
    let handle = Arc::clone(&shard)
        .start_query_stats_refresh_job(
            vec![QueryStatsRefreshSpec::new(
                "reddit-home",
                QueryCardinalityStatsKind::VertexLabel {
                    label: "User".to_string(),
                },
            )],
            std::time::Duration::from_secs(60),
        )
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), handle.stop())
        .await
        .expect("query stats refresh stop should not wait for the full interval")
        .unwrap();
    shard.close().await.unwrap();
}

#[cfg(feature = "query-service-discovery")]
#[test]
fn query_service_discovery_parses_kubernetes_consul_and_etcd() {
    let k8s = serde_json::json!({
        "items": [{
            "ports": [{"name": "cypher", "port": 7777}],
            "endpoints": [{
                "hostname": "node-a",
                "addresses": ["127.0.0.1"],
                "conditions": {"ready": true}
            }, {
                "hostname": "node-b",
                "addresses": ["127.0.0.2"],
                "conditions": {"ready": false}
            }]
        }]
    });
    let directory = KubernetesQueryServiceDiscovery::directory_from_endpointslices_json(
        &k8s,
        Some("cypher"),
        QueryTransportClientConfig::default(),
    )
    .unwrap();
    assert_eq!(directory.endpoint("node-a").unwrap().addr.port(), 7777);
    assert!(directory.endpoint("node-b").is_none());

    let consul = serde_json::json!([{
        "Node": {"Address": "127.0.0.3"},
        "Service": {"ID": "node-c", "Service": "graph-query", "Address": "", "Port": 8888}
    }]);
    let directory = ConsulQueryServiceDiscovery::directory_from_health_service_json(
        &consul,
        QueryTransportClientConfig::default(),
    )
    .unwrap();
    assert_eq!(directory.endpoint("node-c").unwrap().addr.port(), 8888);

    use base64::Engine;
    let record = serde_json::json!({
        "node_id": "node-d",
        "address": "127.0.0.4",
        "port": 9999
    })
    .to_string();
    let etcd = serde_json::json!({
        "kvs": [{
            "value": base64::engine::general_purpose::STANDARD.encode(record.as_bytes())
        }]
    });
    let directory = EtcdQueryServiceDiscovery::directory_from_range_json(
        &etcd,
        QueryTransportClientConfig::default(),
    )
    .unwrap();
    assert_eq!(directory.endpoint("node-d").unwrap().addr.port(), 9999);
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn distributed_query_plan_orders_legs_by_cost_estimate() {
    let plan = DistributedQueryPlan::inner_join(
        vec![
            DistributedQueryLeg::new(
                "large",
                QueryContext::new("reddit-large", "distributed-cost-large"),
                "MATCH (u:User) RETURN u.id AS id",
            )
            .unwrap()
            .with_estimated_rows(10_000),
            DistributedQueryLeg::new(
                "small",
                QueryContext::new("reddit-small", "distributed-cost-small"),
                "MATCH (u:User {id: 1}) RETURN u.id AS id",
            )
            .unwrap()
            .with_estimated_rows(1),
            DistributedQueryLeg::new(
                "unknown",
                QueryContext::new("reddit-unknown", "distributed-cost-unknown"),
                "MATCH (u) RETURN u.id AS id",
            )
            .unwrap(),
        ],
        DistributedQueryJoin::inner("small", "id", "large", "id"),
    )
    .optimized_for_costs();
    let names: Vec<_> = plan.legs.iter().map(|leg| leg.name.as_str()).collect();
    assert_eq!(names, vec!["small", "large", "unknown"]);
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn distributed_union_all_preserves_declared_leg_order() {
    struct StaticRowsClient;

    #[async_trait::async_trait]
    impl QueryCellClient for StaticRowsClient {
        async fn execute_cypher_rows(
            &self,
            context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            let value = match context.cell_id.as_str() {
                "cell-z" => 2,
                "cell-a" => 1,
                other => panic!("unexpected test cell {other}"),
            };
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(value)])],
            ))
        }

        async fn execute_cypher_rows_page(
            &self,
            context: QueryContext,
            query: &str,
            _cursor: Option<QueryCursorToken>,
            _page_size: usize,
        ) -> Result<QueryResultPage> {
            let rows = self.execute_cypher_rows(context, query).await?;
            Ok(QueryResultPage::new(rows.columns, rows.rows, None))
        }
    }

    let placement =
        ObjectStoreNodeDirectory::new(["cell-z", "cell-a"], ["node-z", "node-a"]).unwrap();
    let coordinator = DistributedQueryCoordinator::new(placement)
        .with_client("node-z", Arc::new(StaticRowsClient))
        .unwrap()
        .with_client("node-a", Arc::new(StaticRowsClient))
        .unwrap();
    let plan = DistributedQueryPlan::union_all(vec![
        DistributedQueryLeg::new(
            "z_first",
            QueryContext::new("cell-z", "distributed-union-z"),
            "MATCH (u) RETURN u.id AS id",
        )
        .unwrap()
        .with_estimated_rows(10_000),
        DistributedQueryLeg::new(
            "a_second",
            QueryContext::new("cell-a", "distributed-union-a"),
            "MATCH (u) RETURN u.id AS id",
        )
        .unwrap()
        .with_estimated_rows(1),
    ])
    .optimized_for_costs();

    let result = coordinator
        .execute_distributed_query_plan(plan)
        .await
        .unwrap();
    assert_eq!(
        result.merged,
        QueryResultSet::new(
            vec![QueryColumn::new("id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(1)]),
            ],
        )
    );
}

#[cfg(feature = "query-transport")]
#[test]
fn query_transport_child_process_entry() {
    if std::env::var("SLATEDB_GRAPH_QUERY_CHILD").ok().as_deref() != Some("1") {
        return;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let object_root = std::env::var("SLATEDB_GRAPH_QUERY_OBJECT_ROOT").unwrap();
        let addr: std::net::SocketAddr = std::env::var("SLATEDB_GRAPH_QUERY_ADDR")
            .unwrap()
            .parse()
            .unwrap();
        let ready_path =
            std::path::PathBuf::from(std::env::var("SLATEDB_GRAPH_QUERY_READY_FILE").unwrap());
        let stop_path =
            std::path::PathBuf::from(std::env::var("SLATEDB_GRAPH_QUERY_STOP_FILE").unwrap());
        let token = std::env::var("SLATEDB_GRAPH_QUERY_TOKEN").unwrap();

        let object_store = local_object_store(&object_root).unwrap();
        let placement = ObjectStoreNodeDirectory::new(["reddit-home"], ["node-child"]).unwrap();
        let cluster = Arc::new(
            RoutedGraphCluster::open_promotable(
                "query-child-process",
                "node-child",
                placement,
                sole_writer_placement("node-child"),
                object_store,
            )
            .await
            .unwrap(),
        );
        for (idx, dst) in [71, 72].into_iter().enumerate() {
            cluster
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src: 7,
                    dst,
                    idempotency_key: format!("query-child-process-edge-{idx}"),
                })
                .await
                .unwrap();
        }

        let client: Arc<dyn QueryCellClient> = cluster.clone();
        let server = TcpQueryServer::bind_with_config(
            addr,
            client,
            QueryTransportServerConfig::default()
                .with_required_bearer_token(token)
                .insecure_allow_plaintext(),
        )
        .await
        .unwrap();
        std::fs::write(&ready_path, b"ready").unwrap();
        while !stop_path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        server.stop().await.unwrap();
        cluster.close().await.unwrap();
    });
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_runs_against_separate_child_process_and_local_object_store() {
    let object_root = tempfile::tempdir().unwrap();
    let control_socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = control_socket.local_addr().unwrap();
    drop(control_socket);

    let ready_file = object_root.path().join("child.ready");
    let stop_file = object_root.path().join("child.stop");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("tests::query_transport_child_process_entry")
        .arg("--nocapture")
        .env("SLATEDB_GRAPH_QUERY_CHILD", "1")
        .env("SLATEDB_GRAPH_QUERY_OBJECT_ROOT", object_root.path())
        .env("SLATEDB_GRAPH_QUERY_ADDR", addr.to_string())
        .env("SLATEDB_GRAPH_QUERY_READY_FILE", &ready_file)
        .env("SLATEDB_GRAPH_QUERY_STOP_FILE", &stop_file)
        .env("SLATEDB_GRAPH_QUERY_TOKEN", "child-secret")
        .spawn()
        .unwrap();

    for _ in 0..200 {
        if ready_file.exists() {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("query child exited before ready: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(ready_file.exists(), "query child did not become ready");

    let client = TcpQueryCellClient::new(addr)
        .with_bearer_token("child-secret")
        .insecure_allow_plaintext();
    let rows = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-child-process-read"),
            "MATCH (u {id: 7})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(71)]),
                QueryRow::new(vec![QueryValue::VertexId(72)]),
            ],
        )
    );

    std::fs::write(&stop_file, b"stop").unwrap();
    for _ in 0..200 {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "query child failed: {status}");
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    child.kill().unwrap();
    panic!("query child did not stop");
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_create_and_match_use_storage_kernel() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-create-match", object_store).await;

    let write = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-req-1"),
            "CREATE (u {id: 10})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s {id: 20})",
        )
        .await
        .unwrap();
    assert!(matches!(
        write,
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 1,
            created_relationships: 1,
            ..
        })
    ));

    let neighbors = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "read-req"),
            "MATCH (u {id: 10})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s) RETURN s.id",
        )
        .await
        .unwrap();
    assert_eq!(neighbors, QueryOutput::Vertices(vec![20]));

    let count = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "read-req"),
            "MATCH (u {id: 10})-[:USER_SUBSCRIBED_TO_SUBREDDIT]->(s {id: 20}) RETURN count(*)",
        )
        .await
        .unwrap();
    assert_eq!(count, QueryOutput::Count(1));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_create_labels_properties_are_atomic_and_idempotent() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-create-metadata", object_store).await;

    let query = "CREATE (u:User {id: 10, name: 'alice', active: true})-[:FOLLOWS]->\
                 (v:User {id: 20, name: 'bob', age: 42})";
    let write = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-create-metadata"),
            query,
        )
        .await
        .unwrap();
    assert!(matches!(
        write,
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 1,
            created_relationships: 1,
            ..
        })
    ));

    let rows_at_commit = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-create-metadata-read"),
            "MATCH (u:User {active: true})-[:FOLLOWS]->(v:User) \
             RETURN u.name AS src, v.name AS dst, v.age AS age",
        )
        .await
        .unwrap();
    assert_eq!(
        rows_at_commit,
        QueryResultSet::new(
            vec![
                QueryColumn::new("src"),
                QueryColumn::new("dst"),
                QueryColumn::new("age")
            ],
            vec![QueryRow::new(vec![
                QueryValue::Property(VertexPropertyValue::String("alice".to_string())),
                QueryValue::Property(VertexPropertyValue::String("bob".to_string())),
                QueryValue::Property(VertexPropertyValue::Integer(42)),
            ])],
        )
    );

    let replay = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-create-metadata"),
            query,
        )
        .await
        .unwrap();
    assert_eq!(replay, write);
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 1);
    assert_eq!(
        shard
            .out_degree("reddit-home", "FOLLOWS", 10)
            .await
            .unwrap(),
        1
    );

    let metadata_merge = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-create-metadata-merge"),
            "CREATE (u:Moderator {id: 10})-[:FOLLOWS]->\
             (v:User {id: 20, active: true})",
        )
        .await
        .unwrap();
    assert!(matches!(
        metadata_merge,
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 0,
            created_relationships: 1,
            ..
        })
    ));
    let merged_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-create-metadata-merged-read"),
            "MATCH (u:Moderator {name: 'alice'})-[:FOLLOWS]->(v:User {active: true}) \
             RETURN count(*) AS rels",
        )
        .await
        .unwrap();
    assert_eq!(
        merged_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("rels")],
            vec![QueryRow::new(vec![QueryValue::Count(2)])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_parameters_bind_create_match_where_and_window() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-parameters", object_store).await;

    let create_context = QueryContext::new("reddit-home", "cypher-params-create")
        .with_parameter("src", VertexPropertyValue::Integer(10))
        .with_parameter("dst", VertexPropertyValue::Integer(20))
        .with_parameter("src_name", VertexPropertyValue::String("alice".to_string()))
        .with_parameter("dst_name", VertexPropertyValue::String("bob".to_string()))
        .with_parameter("active", VertexPropertyValue::Bool(true))
        .with_parameter("age", VertexPropertyValue::Integer(42));
    let write = shard
        .execute_cypher(
            create_context,
            "CREATE (u:User {id: $src, name: $src_name, active: $active})-[:FOLLOWS]->\
             (v:User {id: $dst, name: $dst_name, age: $age})",
        )
        .await
        .unwrap();
    assert!(matches!(
        write,
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 1,
            created_relationships: 1,
            ..
        })
    ));

    let read_context = QueryContext::new("reddit-home", "cypher-params-read")
        .with_parameter("active", VertexPropertyValue::Bool(true))
        .with_parameter("age", VertexPropertyValue::Integer(42))
        .with_parameter("name", VertexPropertyValue::String("bob".to_string()))
        .with_parameter("skip", VertexPropertyValue::Integer(0))
        .with_parameter("limit", VertexPropertyValue::Integer(1));
    let rows = shard
        .execute_cypher_rows(
            read_context,
            "MATCH (u:User {active: $active})-[:FOLLOWS]->(v:User {age: $age}) \
             WHERE v.name = $name \
             RETURN u.name AS src, v.name AS dst SKIP $skip LIMIT $limit",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("src"), QueryColumn::new("dst")],
            vec![QueryRow::new(vec![
                QueryValue::Property(VertexPropertyValue::String("alice".to_string())),
                QueryValue::Property(VertexPropertyValue::String("bob".to_string())),
            ])],
        )
    );

    let missing = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-params-missing"),
            "MATCH (u {id: $missing})-[:FOLLOWS]->(v) RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        missing,
        GraphError::MissingQueryParameter {
            dialect: "OpenCypher",
            ref name
        } if name == "missing"
    ));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_limit_skip_uses_query_window() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-limit-skip", object_store).await;

    for (idx, dst) in [10, 11, 12, 13].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("cypher-limit-{idx}"),
            })
            .await
            .unwrap();
    }

    let windowed = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-window-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id SKIP 2 - 1 LIMIT 1 + 1",
        )
        .await
        .unwrap();
    assert_eq!(windowed, QueryOutput::Vertices(vec![11, 12]));

    let count_window = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-window-count"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN count(*) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(count_window, QueryOutput::Count(4));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_rows_return_columns_and_typed_values() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-rows", object_store).await;

    for (idx, dst) in [10, 11, 12, 13].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("cypher-row-{idx}"),
            })
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id SKIP 1 LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(11)]),
                QueryRow::new(vec![QueryValue::VertexId(12)]),
            ],
        )
    );

    let aliased_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-alias-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id AS vertex_id LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        aliased_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("vertex_id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(10)])],
        )
    );

    let count = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-count"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN count(*)",
        )
        .await
        .unwrap();
    assert_eq!(
        count,
        QueryResultSet::new(
            vec![QueryColumn::new("count(*)")],
            vec![QueryRow::new(vec![QueryValue::Count(4)])],
        )
    );

    let aliased_count = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-count-alias"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN count(*) AS total",
        )
        .await
        .unwrap();
    assert_eq!(
        aliased_count,
        QueryResultSet::new(
            vec![QueryColumn::new("total")],
            vec![QueryRow::new(vec![QueryValue::Count(4)])],
        )
    );

    let write = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-write"),
            "CREATE (u {id: 1})-[:FOLLOWS]->(v {id: 99})",
        )
        .await
        .unwrap_err();
    assert!(matches!(write, GraphError::UnsupportedQuery { .. }));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_rows_distinct_deduplicates_before_windowing() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-distinct", object_store).await;

    for (idx, dst) in [10, 11, 12].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("cypher-row-distinct-{idx}"),
            })
            .await
            .unwrap();
    }

    let distinct = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-distinct-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) \
             RETURN DISTINCT u.id AS src ORDER BY src",
        )
        .await
        .unwrap();
    assert_eq!(
        distinct,
        QueryResultSet::new(
            vec![QueryColumn::new("src")],
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
        )
    );

    let windowed = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-distinct-window"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) \
             RETURN DISTINCT u.id AS src ORDER BY src SKIP 1 LIMIT 10",
        )
        .await
        .unwrap();
    assert_eq!(
        windowed,
        QueryResultSet::new(vec![QueryColumn::new("src")], Vec::new())
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_rows_page_returns_bounded_cursor_pages() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-pages", object_store).await;

    for (idx, dst) in [10, 11, 12, 13].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("cypher-row-page-{idx}"),
            })
            .await
            .unwrap();
    }

    let first = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-row-page-first"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
            None,
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        first,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(11)]),
            ],
            Some(QueryCursorToken::new(2)),
        )
    );

    let second = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-row-page-second"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
            first.next_cursor,
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        second,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(12)]),
                QueryRow::new(vec![QueryValue::VertexId(13)]),
            ],
            None,
        )
    );

    let limited_first = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-row-page-limited-first"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id SKIP 1 LIMIT 2",
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        limited_first,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(11)])],
            Some(QueryCursorToken::new(1)),
        )
    );

    let limited_second = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-row-page-limited-second"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id SKIP 1 LIMIT 2",
            limited_first.next_cursor,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        limited_second,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(12)])],
            None,
        )
    );

    let zero_page = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-row-page-zero"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
            None,
            0,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        zero_page,
        GraphError::AdmissionRejected {
            operation: "query_page_size",
            ..
        }
    ));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_supports_bindings_where_order_and_windows() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-engine", object_store).await;

    for (idx, (src, dst)) in [(1, 10), (1, 11), (1, 12), (1, 13), (2, 12), (2, 14)]
        .into_iter()
        .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-engine-{idx}"),
            })
            .await
            .unwrap();
    }

    let filtered = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-engine-filtered"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) \
             WHERE v.id >= 10 AND v.id <> 12 \
             RETURN u.id AS src, v.id AS dst ORDER BY dst DESC SKIP 1 LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        filtered,
        QueryResultSet::new(
            vec![QueryColumn::new("src"), QueryColumn::new("dst")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(11)]),
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(10)]),
            ],
        )
    );

    let scanned = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-engine-scan"),
            "MATCH (u)-[:FOLLOWS]->(v {id: 12}) \
             RETURN u.id AS src, v.id AS dst ORDER BY src",
        )
        .await
        .unwrap();
    assert_eq!(
        scanned,
        QueryResultSet::new(
            vec![QueryColumn::new("src"), QueryColumn::new("dst")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(12)]),
                QueryRow::new(vec![QueryValue::VertexId(2), QueryValue::VertexId(12)]),
            ],
        )
    );

    let disjunction = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-engine-or"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) \
             WHERE v.id = 10 OR v.id = 13 RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        disjunction,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(13)]),
            ],
        )
    );

    let count_window = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-engine-count-window"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN count(*) AS total SKIP 1",
        )
        .await
        .unwrap();
    assert_eq!(
        count_window,
        QueryResultSet::new(vec![QueryColumn::new("total")], Vec::new())
    );

    for (idx, (src, dst)) in [(1, 2), (2, 3), (2, 4)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "CHAIN".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-engine-chain-{idx}"),
            })
            .await
            .unwrap();
    }
    let variable_hop_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-engine-varhop"),
            "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) \
             RETURN u.id AS src, v.id AS dst ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        variable_hop_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("src"), QueryColumn::new("dst")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(3)]),
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(4)]),
            ],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_variable_hops_use_the_configured_graph_kernel_backend() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-varhop-matrix-artifact", object_store).await;

    for (idx, (src, dst)) in [(1, 2), (2, 1), (2, 3)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "CHAIN".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-varhop-matrix-artifact-{idx}"),
            })
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", "CHAIN", base_epoch, 4)
        .await
        .unwrap();
    let plan = shard
        .explain_opencypher_rows(
            QueryContext::new("reddit-home", "cypher-varhop-matrix-artifact-plan"),
            "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert!(plan.groups[0].patterns[0]
        .optimizer_passes
        .contains(&RowQueryOptimizerPass::GraphKernel));

    let before_metrics = shard.graph_cache_metrics();
    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-varhop-matrix-artifact-read"),
            "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1)]),
                QueryRow::new(vec![QueryValue::VertexId(3)]),
            ],
        )
    );
    let parsed_metrics = shard.graph_cache_metrics();
    assert!(parsed_metrics.parsed_row_query_misses >= 1);
    assert!(parsed_metrics.parsed_row_query_hits >= 1);

    let first_page = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-varhop-matrix-artifact-page-1"),
            "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) RETURN v.id ORDER BY v.id",
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        first_page,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
            Some(QueryCursorToken::new(1)),
        )
    );
    let second_page = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-varhop-matrix-artifact-page-2"),
            "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) RETURN v.id ORDER BY v.id",
            first_page.next_cursor,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        second_page,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(3)])],
            None,
        )
    );

    let count_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-varhop-matrix-artifact-count"),
            "MATCH (u {id: 1})-[:CHAIN*2..2]->(v) RETURN count(*) AS total",
        )
        .await
        .unwrap();
    assert_eq!(
        count_rows,
        QueryResultSet::new(
            vec![QueryColumn::new("total")],
            vec![QueryRow::new(vec![QueryValue::Count(2)])],
        )
    );
    let first_metrics = {
        let metrics = shard.graph_cache_metrics();
        assert!(metrics.graphblas_hits > before_metrics.graphblas_hits);
        metrics
    };
    shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-varhop-matrix-artifact-hot"),
            "MATCH (u {id: 1})-[:CHAIN*1..2]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert!(shard.graph_cache_metrics().graphblas_hits > first_metrics.graphblas_hits);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_graph_kernel_descending_order_is_global_before_window() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-varhop-desc-order", object_store).await;

    for (idx, (src, dst)) in [(1, 50), (1, 10), (50, 2), (10, 99), (2, 7)]
        .into_iter()
        .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "DESC_CHAIN".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-varhop-desc-order-{idx}"),
            })
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", "DESC_CHAIN", base_epoch, 4)
        .await
        .unwrap();

    let query = "MATCH (u {id: 1})-[:DESC_CHAIN*1..3]->(v) \
                 RETURN v.id ORDER BY v.id DESC LIMIT 4";
    let plan = shard
        .explain_opencypher_rows(
            QueryContext::new("reddit-home", "cypher-varhop-desc-order-plan"),
            query,
        )
        .await
        .unwrap();
    assert!(plan.groups[0].patterns[0]
        .optimizer_passes
        .contains(&RowQueryOptimizerPass::GraphKernel));

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-varhop-desc-order-read"),
            query,
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(99)]),
                QueryRow::new(vec![QueryValue::VertexId(50)]),
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(7)]),
            ],
        )
    );

    let first_page = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-varhop-desc-order-page-1"),
            query,
            None,
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        first_page,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(99)]),
                QueryRow::new(vec![QueryValue::VertexId(50)]),
            ],
            Some(QueryCursorToken::new(2)),
        )
    );
    let second_page = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-varhop-desc-order-page-2"),
            query,
            first_page.next_cursor,
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        second_page,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(7)]),
            ],
            None,
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_executes_multi_match_join_pipeline() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-multi-match", object_store).await;

    for (idx, (edge_type, src, dst)) in [
        ("FOLLOWS", 1, 10),
        ("FOLLOWS", 1, 11),
        ("FOLLOWS", 2, 12),
        ("POSTED", 10, 100),
        ("POSTED", 11, 101),
        ("POSTED", 12, 102),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-multi-match-edge-{idx}"),
            })
            .await
            .unwrap();
    }
    for (post, score) in [(100, 5), (101, 20), (102, 30)] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                post,
                VertexMetadata::default()
                    .with_label("Post")
                    .with_property("score", VertexPropertyValue::Integer(score)),
            )
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-multi-match-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) \
             MATCH (v)-[:POSTED]->(p:Post) \
             WHERE p.score >= 10 \
             RETURN u.id AS user, v.id AS followed, p.id AS post ORDER BY post",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![
                QueryColumn::new("user"),
                QueryColumn::new("followed"),
                QueryColumn::new("post"),
            ],
            vec![QueryRow::new(vec![
                QueryValue::VertexId(1),
                QueryValue::VertexId(11),
                QueryValue::VertexId(101),
            ])],
        )
    );

    let with_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-multi-match-with-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) WITH u, v \
             MATCH (v)-[:POSTED]->(p:Post) \
             WHERE p.score >= 10 \
             RETURN u.id AS user, v.id AS followed, p.id AS post ORDER BY post",
        )
        .await
        .unwrap();
    assert_eq!(with_rows, rows);
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_executes_multi_edge_path_pipeline() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-multi-edge-path", object_store).await;

    for (idx, (edge_type, src, dst)) in [
        ("FOLLOWS", 1, 10),
        ("FOLLOWS", 1, 11),
        ("POSTED", 10, 100),
        ("POSTED", 11, 101),
        ("POSTED", 99, 999),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-multi-edge-path-{idx}"),
            })
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-multi-edge-path-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v)-[:POSTED]->(p) \
             RETURN v.id AS followed, p.id AS post ORDER BY followed",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("followed"), QueryColumn::new("post")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10), QueryValue::VertexId(100)]),
                QueryRow::new(vec![QueryValue::VertexId(11), QueryValue::VertexId(101)]),
            ],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_executes_grouped_aggregates() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-aggregates", object_store).await;

    for (idx, (edge_type, src, dst)) in [
        ("FOLLOWS", 1, 10),
        ("FOLLOWS", 1, 11),
        ("FOLLOWS", 2, 12),
        ("POSTED", 10, 100),
        ("POSTED", 11, 101),
        ("POSTED", 12, 102),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-aggregates-edge-{idx}"),
            })
            .await
            .unwrap();
    }
    for (post, score) in [(100, 5), (101, 20), (102, 30)] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                post,
                VertexMetadata::default()
                    .with_label("Post")
                    .with_property("score", VertexPropertyValue::Integer(score)),
            )
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-aggregates-read"),
            "MATCH (u)-[:FOLLOWS]->(v)-[:POSTED]->(p:Post) \
             RETURN u.id AS user, count(*) AS posts, sum(p.score) AS score, \
             avg(p.score) AS avg_score, collect(p.id) AS post_ids ORDER BY user",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![
                QueryColumn::new("user"),
                QueryColumn::new("posts"),
                QueryColumn::new("score"),
                QueryColumn::new("avg_score"),
                QueryColumn::new("post_ids"),
            ],
            vec![
                QueryRow::new(vec![
                    QueryValue::VertexId(1),
                    QueryValue::Count(2),
                    QueryValue::Property(VertexPropertyValue::Integer(25)),
                    QueryValue::Float(QueryFloat(12.5)),
                    QueryValue::List(vec![QueryValue::VertexId(100), QueryValue::VertexId(101)]),
                ]),
                QueryRow::new(vec![
                    QueryValue::VertexId(2),
                    QueryValue::Count(1),
                    QueryValue::Property(VertexPropertyValue::Integer(30)),
                    QueryValue::Float(QueryFloat(30.0)),
                    QueryValue::List(vec![QueryValue::VertexId(102)]),
                ]),
            ],
        )
    );

    let empty = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-aggregates-empty"),
            "MATCH (u {id: 404})-[:FOLLOWS]->(v) \
             RETURN count(*) AS total, sum(v.id) AS sum_ids, avg(v.id) AS avg_id, \
             collect(v.id) AS ids",
        )
        .await
        .unwrap();
    assert_eq!(
        empty,
        QueryResultSet::new(
            vec![
                QueryColumn::new("total"),
                QueryColumn::new("sum_ids"),
                QueryColumn::new("avg_id"),
                QueryColumn::new("ids"),
            ],
            vec![QueryRow::new(vec![
                QueryValue::Count(0),
                QueryValue::Property(VertexPropertyValue::Integer(0)),
                QueryValue::Null,
                QueryValue::List(Vec::new()),
            ])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_executes_set_remove_delete_and_merge_mutations() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-mutations", object_store).await;

    shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-mutation-merge"),
            "MERGE (u:User {id: 1, name: 'alice'})-[:FOLLOWS]->\
             (v:User {id: 2, name: 'bob'})",
        )
        .await
        .unwrap();
    assert!(shard
        .edge_exists("reddit-home", "FOLLOWS", 1, 2)
        .await
        .unwrap());

    let set = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-mutation-set"),
            "MATCH (u {id: 1}) SET u.active = true, u:Moderator",
        )
        .await
        .unwrap();
    assert_eq!(
        set,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            updated_vertices: 1,
            ..QueryMutationResult::default()
        })
    );

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mutation-read-set"),
            "MATCH (u:Moderator {active: true}) RETURN u.name",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("u.name")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::String("alice".to_string())
            )])]
        )
    );

    let remove = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-mutation-remove"),
            "MATCH (u {id: 1}) REMOVE u.active, u:Moderator",
        )
        .await
        .unwrap();
    assert_eq!(
        remove,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            updated_vertices: 1,
            ..QueryMutationResult::default()
        })
    );

    let removed_rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-mutation-read-remove"),
            "MATCH (u:Moderator {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert!(removed_rows.rows.is_empty());

    let delete = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-mutation-delete"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) DELETE r",
        )
        .await
        .unwrap();
    assert_eq!(
        delete,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            deleted_edges: 1,
            ..QueryMutationResult::default()
        })
    );
    assert!(!shard
        .edge_exists("reddit-home", "FOLLOWS", 1, 2)
        .await
        .unwrap());

    let replay = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-mutation-delete"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) DELETE r",
        )
        .await
        .unwrap();
    assert_eq!(
        replay,
        QueryOutput::Mutation(QueryMutationResult::default())
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_detach_delete_node_cascades_edges_and_metadata() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-detach-delete-node", object_store).await;

    shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-detach-delete-seed"),
            "MERGE (u:User {id: 1, name: 'alice'})-[:FOLLOWS]->(v:User {id: 2})",
        )
        .await
        .unwrap();
    shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-detach-delete-seed-2"),
            "MERGE (v:User {id: 3})-[:LIKES]->(u:User {id: 1})",
        )
        .await
        .unwrap();

    let deleted = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-detach-delete"),
            "MATCH (u:User {id: 1}) DETACH DELETE u",
        )
        .await
        .unwrap();
    assert_eq!(
        deleted,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            deleted_edges: 2,
            updated_vertices: 1,
            ..QueryMutationResult::default()
        })
    );
    assert!(!shard
        .edge_exists("reddit-home", "FOLLOWS", 1, 2)
        .await
        .unwrap());
    assert!(!shard
        .edge_exists("reddit-home", "LIKES", 3, 1)
        .await
        .unwrap());
    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-detach-delete-read"),
            "MATCH (u:User {id: 1}) RETURN u.id",
        )
        .await
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[cfg(feature = "opencypher")]
#[test]
fn cypher_relationship_properties_are_indexed_mutable_and_snapshot_safe() {
    std::thread::Builder::new()
        .name("cypher-relationship-properties".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(cypher_relationship_properties_case());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[cfg(feature = "opencypher")]
async fn cypher_relationship_properties_case() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-relationship-properties",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let create = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-rel-props-create"),
            "CREATE (u {id: 1})-[r:FOLLOWS {since: 2020, close: true}]->(v {id: 2})",
        )
        .await
        .unwrap();
    assert!(matches!(
        create,
        QueryOutput::Mutation(QueryMutationResult {
            created_edges: 1,
            created_relationships: 1,
            ..
        })
    ));
    let created_epoch = shard.current_epoch("reddit-home").await.unwrap();

    for (idx, (src, dst)) in [(3, 4), (5, 6)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-rel-props-extra-{idx}"),
            })
            .await
            .unwrap();
    }

    let indexed = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-rel-props-indexed"),
            "MATCH (u)-[r:FOLLOWS {since: 2020}]->(v) \
             RETURN u.id AS src, v.id AS dst, r.close AS close",
        )
        .await
        .unwrap();
    assert_eq!(
        indexed,
        QueryResultSet::new(
            vec![
                QueryColumn::new("src"),
                QueryColumn::new("dst"),
                QueryColumn::new("close")
            ],
            vec![QueryRow::new(vec![
                QueryValue::VertexId(1),
                QueryValue::VertexId(2),
                QueryValue::Property(VertexPropertyValue::Bool(true)),
            ])],
        )
    );

    let update = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-rel-props-set"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) SET r.since = 2021, r.weight = 7",
        )
        .await
        .unwrap();
    assert_eq!(
        update,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            updated_relationships: 1,
            ..QueryMutationResult::default()
        })
    );

    let old_index_latest = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-rel-props-old-latest"),
            "MATCH (u)-[r:FOLLOWS {since: 2020}]->(v) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(
        old_index_latest,
        QueryResultSet::new(vec![QueryColumn::new("u.id")], Vec::new())
    );

    let old_index_snapshot = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-rel-props-old-snapshot")
                .at_epoch(created_epoch),
            "MATCH (u)-[r:FOLLOWS {since: 2020}]->(v) RETURN r.since AS since",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        old_index_snapshot,
        GraphError::UnsupportedQuery { .. }
    ));

    let updated = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-rel-props-updated"),
            "MATCH (u)-[r:FOLLOWS {since: 2021}]->(v) \
             RETURN u.id AS src, v.id AS dst, r.since AS since, r.weight AS weight",
        )
        .await
        .unwrap();
    assert_eq!(
        updated,
        QueryResultSet::new(
            vec![
                QueryColumn::new("src"),
                QueryColumn::new("dst"),
                QueryColumn::new("since"),
                QueryColumn::new("weight")
            ],
            vec![QueryRow::new(vec![
                QueryValue::VertexId(1),
                QueryValue::VertexId(2),
                QueryValue::Property(VertexPropertyValue::Integer(2021)),
                QueryValue::Property(VertexPropertyValue::Integer(7)),
            ])],
        )
    );

    let remove = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-rel-props-remove"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) REMOVE r.close",
        )
        .await
        .unwrap();
    assert_eq!(
        remove,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            updated_relationships: 1,
            ..QueryMutationResult::default()
        })
    );

    let removed_index = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-rel-props-removed-index"),
            "MATCH (u)-[r:FOLLOWS {close: true}]->(v) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(
        removed_index,
        QueryResultSet::new(vec![QueryColumn::new("u.id")], Vec::new())
    );

    let delete = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-rel-props-delete"),
            "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) DELETE r",
        )
        .await
        .unwrap();
    assert_eq!(
        delete,
        QueryOutput::Mutation(QueryMutationResult {
            matched_rows: 1,
            deleted_edges: 1,
            ..QueryMutationResult::default()
        })
    );

    let deleted_index = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-rel-props-deleted-index"),
            "MATCH (u)-[r:FOLLOWS {since: 2021}]->(v) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(
        deleted_index,
        QueryResultSet::new(vec![QueryColumn::new("u.id")], Vec::new())
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_optional_match_preserves_required_rows_with_null_bindings() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-optional-match", object_store).await;

    for user in [1, 3] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                user,
                VertexMetadata::default().with_label("User"),
            )
            .await
            .unwrap();
    }
    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "cypher-optional-follows".to_string(),
        })
        .await
        .unwrap();
    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "POSTED".to_string(),
            src: 2,
            dst: 99,
            idempotency_key: "cypher-optional-posted".to_string(),
        })
        .await
        .unwrap();

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-optional-read"),
            "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v) \
             RETURN u.id AS user, v.id AS followed ORDER BY user",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("user"), QueryColumn::new("followed")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(3), QueryValue::Null]),
            ],
        )
    );

    let filtered_optional = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-optional-where"),
            "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v) WHERE v.id = 99 \
             RETURN u.id AS user, v.id AS followed ORDER BY user",
        )
        .await
        .unwrap();
    assert_eq!(
        filtered_optional,
        QueryResultSet::new(
            vec![QueryColumn::new("user"), QueryColumn::new("followed")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::Null]),
                QueryRow::new(vec![QueryValue::VertexId(3), QueryValue::Null]),
            ],
        )
    );

    let nullable_order = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-optional-null-order"),
            "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v) \
             RETURN v.id AS followed ORDER BY followed",
        )
        .await
        .unwrap();
    assert_eq!(
        nullable_order,
        QueryResultSet::new(
            vec![QueryColumn::new("followed")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::Null]),
            ],
        )
    );

    let required_after_optional = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-optional-required-after"),
            "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v) \
             MATCH (v)-[:POSTED]->(p) RETURN u.id AS user, p.id AS post",
        )
        .await
        .unwrap();
    assert_eq!(
        required_after_optional,
        QueryResultSet::new(
            vec![QueryColumn::new("user"), QueryColumn::new("post")],
            vec![QueryRow::new(vec![
                QueryValue::VertexId(1),
                QueryValue::VertexId(99),
            ])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn execute_cypher_uses_row_engine_for_general_read_queries() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-public-row-engine", object_store).await;

    for user in [1, 3] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                user,
                VertexMetadata::default().with_label("User"),
            )
            .await
            .unwrap();
    }
    shard
        .set_vertex_metadata(
            "reddit-home",
            99,
            VertexMetadata::default().with_label("Post"),
        )
        .await
        .unwrap();
    for (idx, (edge_type, src, dst)) in [("FOLLOWS", 1, 2), ("POSTED", 2, 99), ("FOLLOWS", 2, 3)]
        .into_iter()
        .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-public-row-engine-{idx}"),
            })
            .await
            .unwrap();
    }

    let optional_rows = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "public-optional-read"),
            "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v) \
             RETURN u.id AS user, v.id AS followed ORDER BY user",
        )
        .await
        .unwrap();
    assert_eq!(
        optional_rows,
        QueryOutput::Rows(QueryResultSet::new(
            vec![QueryColumn::new("user"), QueryColumn::new("followed")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(3), QueryValue::Null]),
            ],
        ))
    );

    let multi_edge_path = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "public-multi-edge-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v)-[:POSTED]->(p:Post) \
             RETURN p.id AS post ORDER BY post",
        )
        .await
        .unwrap();
    assert_eq!(multi_edge_path, QueryOutput::Vertices(vec![99]));

    let aggregate = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "public-aggregate-read"),
            "MATCH (u {id: 1})-[:FOLLOWS*1..2]->(v) RETURN count(*) AS total",
        )
        .await
        .unwrap();
    assert_eq!(aggregate, QueryOutput::Count(2));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_union_merges_row_query_arms_with_distinct_or_all_semantics() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-union", object_store).await;

    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default().with_label("User"),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            2,
            VertexMetadata::default()
                .with_label("User")
                .with_label("Moderator"),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            3,
            VertexMetadata::default().with_label("Moderator"),
        )
        .await
        .unwrap();

    let distinct = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-union-distinct"),
            "MATCH (u:User) RETURN u.id AS id \
             UNION MATCH (m:Moderator) RETURN m.id AS id",
        )
        .await
        .unwrap();
    assert_eq!(
        distinct,
        QueryResultSet::new(
            vec![QueryColumn::new("id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1)]),
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(3)]),
            ],
        )
    );

    let all = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-union-all"),
            "MATCH (u:User) RETURN u.id AS id \
             UNION ALL MATCH (m:Moderator) RETURN m.id AS id",
        )
        .await
        .unwrap();
    assert_eq!(
        all,
        QueryResultSet::new(
            vec![QueryColumn::new("id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1)]),
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(3)]),
            ],
        )
    );

    let windowed = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-union-windowed-arms"),
            "MATCH (u:User) RETURN u.id AS id ORDER BY id DESC LIMIT 1 \
			 UNION ALL MATCH (m:Moderator) RETURN m.id AS id ORDER BY id ASC LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        windowed,
        QueryResultSet::new(
            vec![QueryColumn::new("id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(2)]),
            ],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_starts_with_uses_current_index_and_rejects_graph_epoch_replay() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-prefix-index", object_store).await;

    for (vertex, value) in [(1, "thread-alpha"), (2, "thread-beta")] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex,
                VertexMetadata::default()
                    .with_label("Source")
                    .with_property("thread_id", VertexPropertyValue::String(value.to_string())),
            )
            .await
            .unwrap();
    }
    let historical_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            2,
            VertexMetadata::default()
                .with_label("Source")
                .with_property(
                    "thread_id",
                    VertexPropertyValue::String("other".to_string()),
                ),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            3,
            VertexMetadata::default()
                .with_label("Source")
                .with_property(
                    "thread_id",
                    VertexPropertyValue::String("thread-current".to_string()),
                ),
        )
        .await
        .unwrap();

    let query = "MATCH (s:Source) WHERE s.thread_id STARTS WITH $prefix \
                 RETURN s.id ORDER BY s.id";
    let current = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "prefix-current")
                .with_parameter("prefix", VertexPropertyValue::String("thread-".to_string())),
            query,
        )
        .await
        .unwrap();
    assert_eq!(
        current.rows,
        vec![
            QueryRow::new(vec![QueryValue::VertexId(1)]),
            QueryRow::new(vec![QueryValue::VertexId(3)]),
        ]
    );

    let historical = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "prefix-historical")
                .at_epoch(historical_epoch)
                .with_parameter("prefix", VertexPropertyValue::String("thread-".to_string())),
            query,
        )
        .await
        .unwrap_err();
    assert!(matches!(historical, GraphError::UnsupportedQuery { .. }));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_tck_style_row_corpus_covers_supported_clause_semantics() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-tck-style-corpus", object_store).await;

    for (vertex, active, name) in [(1, true, "alice"), (2, true, "bob"), (3, false, "carol")] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex,
                VertexMetadata::default()
                    .with_label("User")
                    .with_property("active", VertexPropertyValue::Bool(active))
                    .with_property("name", VertexPropertyValue::String(name.to_string())),
            )
            .await
            .unwrap();
    }
    shard
        .set_vertex_metadata(
            "reddit-home",
            2,
            VertexMetadata::default()
                .with_label("User")
                .with_label("Moderator")
                .with_property("active", VertexPropertyValue::Bool(true))
                .with_property("name", VertexPropertyValue::String("bob".to_string())),
        )
        .await
        .unwrap();
    for (idx, (edge_type, src, dst)) in [
        ("FOLLOWS", 1, 2),
        ("FOLLOWS", 1, 3),
        ("FOLLOWS", 2, 3),
        ("POSTED", 2, 100),
        ("POSTED", 2, 101),
        ("POSTED", 3, 102),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-tck-style-corpus-edge-{idx}"),
            })
            .await
            .unwrap();
    }
    for (post, score) in [(100, 5), (101, 7), (102, 11)] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                post,
                VertexMetadata::default()
                    .with_label("Post")
                    .with_property("score", VertexPropertyValue::Integer(score)),
            )
            .await
            .unwrap();
    }
    shard
        .set_edge_metadata(
            "reddit-home",
            "FOLLOWS",
            1,
            2,
            EdgeMetadata::default()
                .with_property("since", VertexPropertyValue::Integer(2020))
                .with_property("close", VertexPropertyValue::Bool(true)),
        )
        .await
        .unwrap();
    shard
        .set_edge_metadata(
            "reddit-home",
            "FOLLOWS",
            1,
            3,
            EdgeMetadata::default().with_property("since", VertexPropertyValue::Integer(2021)),
        )
        .await
        .unwrap();

    let corpus = parse_opencypher_tck_corpus(
        r#"
Feature: Supported row-query corpus

  Scenario: label-property-filter-order
    When executing query:
      """
      MATCH (u:User {active: true}) RETURN u.name AS name ORDER BY name
      """
    Then the result should be, in order:
      | name |
      | 'alice' |
      | 'bob' |

  Scenario: optional-match-null-extension
    When executing query:
      """
      MATCH (u:User) OPTIONAL MATCH (u)-[:POSTED]->(p)
      RETURN u.id AS user, p.id AS post ORDER BY user, post
      """
    Then the result should be, in order:
      | user | post |
      | 1 | null |
      | 2 | 100 |
      | 2 | 101 |
      | 3 | 102 |

  Scenario: relationship-property-filter-project
    When executing query:
      """
      MATCH (u)-[r:FOLLOWS {since: 2020}]->(v)
      RETURN u.id AS user, v.id AS followed, r.close AS close
      """
    Then the result should be, in order:
      | user | followed | close |
      | 1 | 2 | true |

  Scenario: grouped-aggregate
    When executing query:
      """
      MATCH (u)-[:POSTED]->(p:Post)
      RETURN u.id AS user, count(*) AS total, sum(p.score) AS score ORDER BY user
      """
    Then the result should be, in order:
      | user | total | score |
      | 2 | 2 | 12 |
      | 3 | 1 | 11 |

  Scenario: union-distinct
    When executing query:
      """
      MATCH (u:User) RETURN u.id AS id
      UNION MATCH (m:Moderator) RETURN m.id AS id
      """
    Then the result should be, in order:
      | id |
      | 1 |
      | 2 |
      | 3 |

  Scenario: skipped-side-effect
    When executing query:
      """
      CREATE (u {id: 99})-[:FOLLOWS]->(v {id: 100})
      """
    Then the side effects should be:
      | +relationships | 1 |

  Scenario: skipped-result-without-table
    When executing query:
      """
      CALL db.labels()
      """
    Then the result should be empty
"#,
    )
    .unwrap();
    assert_eq!(corpus.cases.len(), 5);
    assert_eq!(corpus.skipped.len(), 2);
    let report = corpus.compatibility_report();
    assert_eq!(report.total_scenarios, 7);
    assert_eq!(report.runnable_scenarios, 5);
    assert_eq!(report.skipped_scenarios, 2);
    assert!(report
        .skipped
        .iter()
        .any(|reason| reason.contains("side-effect assertions")));
    assert!(report
        .skipped
        .iter()
        .any(|reason| reason.contains("result assertions without inline tables")));

    for case in corpus.cases {
        let rows = shard
            .execute_cypher_rows(
                QueryContext::new("reddit-home", format!("cypher-tck-style-{}", case.name)),
                &case.query,
            )
            .await
            .unwrap();
        assert_eq!(rows, case.expected, "{}", case.name);
    }
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_records_operational_metrics() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-query-metrics", object_store).await;

    for (idx, dst) in [10, 11].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("cypher-row-query-metrics-edge-{idx}"),
            })
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-query-metrics-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 2);

    let err = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-query-metrics-fail"),
            "MATCH (u) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, GraphError::UnsupportedQuery { .. }));

    let metrics = shard.graph_operational_metrics();
    assert_eq!(metrics.query_rows_started, 2);
    assert_eq!(metrics.query_rows_completed, 1);
    assert_eq!(metrics.query_rows_failed, 1);
    assert_eq!(metrics.query_rows_returned, 2);
    assert!(metrics.query_rows_duration_us > 0);
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_property_fuzz_matches_direct_model() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-property-fuzz", object_store).await;
    let mut expected = BTreeMap::<u64, BTreeSet<(VertexId, VertexId)>>::new();

    for idx in 0_u64..48 {
        let src = 1 + ((idx * 17 + 3) % 11);
        let dst = 100 + idx;
        let score = (idx * 7 + 5) % 9;
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "POSTED".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-property-fuzz-edge-{idx}"),
            })
            .await
            .unwrap();
        shard
            .set_vertex_metadata(
                "reddit-home",
                dst,
                VertexMetadata::default()
                    .with_label("Post")
                    .with_property("score", VertexPropertyValue::Integer(score)),
            )
            .await
            .unwrap();
        expected.entry(score).or_default().insert((src, dst));
    }

    for score in 0_u64..9 {
        let rows = shard
            .execute_cypher_rows(
                QueryContext::new("reddit-home", format!("cypher-row-property-fuzz-{score}"))
                    .with_parameter("score", VertexPropertyValue::Integer(score)),
                "MATCH (u)-[:POSTED]->(p:Post {score: $score}) \
                 RETURN u.id AS user, p.id AS post ORDER BY user, post",
            )
            .await
            .unwrap();
        let actual: BTreeSet<_> = rows
            .rows
            .iter()
            .map(|row| match row.values.as_slice() {
                [QueryValue::VertexId(src), QueryValue::VertexId(dst)] => (*src, *dst),
                values => panic!("unexpected row values: {values:?}"),
            })
            .collect();
        assert_eq!(
            actual,
            expected.get(&score).cloned().unwrap_or_default(),
            "score {score}"
        );
    }
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_rejects_excess_intermediate_rows() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-row-intermediate-limit",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_intermediate_rows: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, dst) in [10, 11].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("cypher-row-intermediate-limit-{idx}"),
            })
            .await
            .unwrap();
    }

    let err = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-intermediate-limit-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::AdmissionRejected {
            operation: "cypher_source_relationship_structural_fallback",
            actual: 2,
            limit: 1
        }
    ));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_rejects_large_index_candidate_scans() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-row-index-limit",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_index_candidates: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for vertex_id in [1, 2] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex_id,
                VertexMetadata::default().with_label("User"),
            )
            .await
            .unwrap();
    }

    let err = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-index-limit-read"),
            "MATCH (u:User) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::AdmissionRejected {
            operation: "cypher_vertex_label_index_candidates",
            actual: 2,
            limit: 1
        }
    ));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_engine_rejects_large_full_edge_scans() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-row-full-scan-limit",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, (src, dst)) in [(1, 10), (2, 20)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-full-scan-limit-{idx}"),
            })
            .await
            .unwrap();
    }

    let err = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-full-scan-limit-read"),
            "MATCH (u)-[:FOLLOWS]->(v) RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            GraphError::AdmissionRejected {
                operation: "query_edges_at_canonical",
                actual: 2,
                limit: 1
            }
        ),
        "{err:?}"
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_planner_runs_selective_match_group_before_full_scan() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-row-planner-selective-group",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, (src, dst)) in [(1, 10), (2, 20), (3, 30)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-planner-selective-group-{idx}"),
            })
            .await
            .unwrap();
    }
    shard
        .set_vertex_metadata(
            "reddit-home",
            20,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true)),
        )
        .await
        .unwrap();

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-planner-selective-group-read"),
            "MATCH (u)-[:FOLLOWS]->(v) \
             MATCH (v:User {active: true}) \
             RETURN u.id AS user, v.id AS followed",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("user"), QueryColumn::new("followed")],
            vec![QueryRow::new(vec![
                QueryValue::VertexId(2),
                QueryValue::VertexId(20),
            ])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_explain_shows_connectivity_aware_reverse_expansion() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-explain-optimizer", object_store).await;

    let plan = shard
        .explain_opencypher_rows(
            QueryContext::new("reddit-home", "cypher-row-explain-optimizer"),
            "MATCH (u)-[:FOLLOWS]->(v)-[:POSTED]->(p:Post {score: 20}) \
             RETURN p.id",
        )
        .await
        .unwrap();

    assert_eq!(plan.groups.len(), 1);
    let patterns = &plan.groups[0].patterns;
    assert_eq!(patterns.len(), 2);
    assert_eq!(patterns[0].original_index, 1);
    assert_eq!(
        patterns[0].access,
        RowQueryAccess::BoundInExpand {
            edge_type: "POSTED".to_string(),
        }
    );
    assert!(patterns[0]
        .optimizer_passes
        .contains(&RowQueryOptimizerPass::ReverseExpand));
    assert_eq!(patterns[1].original_index, 0);
    assert_eq!(
        patterns[1].access,
        RowQueryAccess::BoundInExpand {
            edge_type: "FOLLOWS".to_string(),
        }
    );
    assert!(patterns[1]
        .optimizer_passes
        .contains(&RowQueryOptimizerPass::ConnectivityOrder));
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_row_explain_uses_persisted_stats_for_index_choice() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-explain-stats", object_store).await;

    for vertex in 1..=5 {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex,
                VertexMetadata::default()
                    .with_label("User")
                    .with_property("active", VertexPropertyValue::Bool(true)),
            )
            .await
            .unwrap();
    }
    shard
        .set_vertex_metadata(
            "reddit-home",
            99,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(false))
                .with_property("name", VertexPropertyValue::String("neo".to_string())),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            100,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(false)),
        )
        .await
        .unwrap();
    shard
        .refresh_vertex_label_query_stats("reddit-home", "User")
        .await
        .unwrap();
    shard
        .refresh_vertex_property_query_stats(
            "reddit-home",
            "active",
            &VertexPropertyValue::Bool(false),
        )
        .await
        .unwrap();
    shard
        .refresh_vertex_property_query_stats(
            "reddit-home",
            "name",
            &VertexPropertyValue::String("neo".to_string()),
        )
        .await
        .unwrap();

    let plan = shard
        .explain_opencypher_rows(
            QueryContext::new("reddit-home", "cypher-row-explain-stats"),
            "MATCH (u:User {active: false, name: 'neo'}) RETURN u.id",
        )
        .await
        .unwrap();

    assert_eq!(plan.groups.len(), 1);
    assert_eq!(plan.groups[0].patterns.len(), 1);
    assert_eq!(
        plan.groups[0].patterns[0].access,
        RowQueryAccess::VertexPropertyIndex {
            property: "name".to_string(),
        }
    );
    assert_eq!(plan.groups[0].patterns[0].estimated_cardinality, 1);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_expand_into_checks_bound_edge_without_neighbor_scan() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-expand-into-physical",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, dst) in [10, 11].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("cypher-expand-into-physical-{idx}"),
            })
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-expand-into-physical-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v {id: 10}) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(10)])],
        )
    );
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_hash_join_uses_edge_property_index_without_per_row_neighbor_scans() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-hash-join-physical",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, (src, dst, weight)) in [(1, 10, 7), (2, 20, 9), (3, 30, 7)].into_iter().enumerate() {
        shard
            .set_vertex_metadata(
                "reddit-home",
                src,
                VertexMetadata::default().with_label("User"),
            )
            .await
            .unwrap();
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-hash-join-physical-edge-{idx}"),
            })
            .await
            .unwrap();
        shard
            .set_edge_metadata(
                "reddit-home",
                "FOLLOWS",
                src,
                dst,
                EdgeMetadata::default()
                    .with_property("weight", VertexPropertyValue::Integer(weight)),
            )
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-hash-join-physical-read"),
            "MATCH (u:User) MATCH (u)-[e:FOLLOWS {weight: 7}]->(v) \
             RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(30)]),
            ],
        )
    );
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_edge_match_uses_destination_id_reverse_index_without_full_scan() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-dst-id-index",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, (src, dst)) in [(1, 10), (2, 20), (3, 30)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-dst-id-index-{idx}"),
            })
            .await
            .unwrap();
    }

    let pinned_snapshot = shard.db.snapshot().await.unwrap();
    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 4,
            dst: 40,
            idempotency_key: "cypher-dst-id-index-newer-write".to_string(),
        })
        .await
        .unwrap();

    let rows = crate::GraphStore::scope_snapshot(
        pinned_snapshot,
        shard.execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-dst-id-index-read"),
            "MATCH (u)-[:FOLLOWS]->(v {id: 20}) RETURN u.id",
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("u.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(2)])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_edge_match_uses_edge_property_index_before_endpoint_expansion() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-edge-property-index-plan",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, (src, dst)) in [(1, 10), (2, 20), (3, 30)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-edge-property-index-plan-{idx}"),
            })
            .await
            .unwrap();
        shard
            .set_vertex_metadata(
                "reddit-home",
                src,
                VertexMetadata::default().with_label("User"),
            )
            .await
            .unwrap();
    }
    shard
        .set_edge_metadata(
            "reddit-home",
            "FOLLOWS",
            2,
            20,
            EdgeMetadata::default().with_property("weight", VertexPropertyValue::Integer(7)),
        )
        .await
        .unwrap();

    let plan = shard
        .explain_opencypher_rows(
            QueryContext::new("reddit-home", "cypher-edge-property-index-plan-explain"),
            "MATCH (u:User)-[e:FOLLOWS {weight: 7}]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        plan.groups[0].patterns[0].access,
        RowQueryAccess::EdgePropertyIndex {
            edge_type: "FOLLOWS".to_string(),
            property: "weight".to_string(),
        }
    );

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-edge-property-index-plan-read"),
            "MATCH (u:User)-[e:FOLLOWS {weight: 7}]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(20)])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_relationship_where_disjunction_uses_edge_property_index() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-edge-predicate-index-plan",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, (src, dst, chunk_id, superseded_by)) in [
        (1, 10, "chunk-a", ""),
        (2, 20, "chunk-z", ""),
        (3, 30, "chunk-b", "newer"),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "resume".to_string(),
                edge_type: "RELATES".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-edge-predicate-index-plan-{idx}"),
            })
            .await
            .unwrap();
        shard
            .set_edge_metadata(
                "resume",
                "RELATES",
                src,
                dst,
                EdgeMetadata::default()
                    .with_property(
                        "chunk_id",
                        VertexPropertyValue::String(chunk_id.to_string()),
                    )
                    .with_property(
                        "superseded_by",
                        VertexPropertyValue::String(superseded_by.to_string()),
                    )
                    .with_property(
                        "relationship_id",
                        VertexPropertyValue::String(format!("relationship-{idx}")),
                    ),
            )
            .await
            .unwrap();
    }

    let query = "MATCH (s)-[r:RELATES]->(o) \
                 WHERE (r.chunk_id = $chunk_id_0 OR r.chunk_id = $chunk_id_1 \
                        OR r.chunk_id = $chunk_id_2 OR r.chunk_id = $chunk_id_3 \
                        OR r.chunk_id = $chunk_id_4 OR r.chunk_id = $chunk_id_5) \
                   AND r.superseded_by = $current_marker \
                 RETURN r.relationship_id AS rid";
    let context = QueryContext::new("resume", "cypher-edge-predicate-index-plan-read")
        .with_parameter(
            "chunk_id_0",
            VertexPropertyValue::String("chunk-a".to_string()),
        )
        .with_parameter(
            "chunk_id_1",
            VertexPropertyValue::String("chunk-b".to_string()),
        )
        .with_parameter(
            "chunk_id_2",
            VertexPropertyValue::String("chunk-c".to_string()),
        )
        .with_parameter(
            "chunk_id_3",
            VertexPropertyValue::String("chunk-d".to_string()),
        )
        .with_parameter(
            "chunk_id_4",
            VertexPropertyValue::String("chunk-e".to_string()),
        )
        .with_parameter(
            "chunk_id_5",
            VertexPropertyValue::String("chunk-f".to_string()),
        )
        .with_parameter("current_marker", VertexPropertyValue::String(String::new()));

    let plan = shard
        .explain_opencypher_rows(context.clone(), query)
        .await
        .unwrap();
    assert_eq!(
        plan.groups[0].patterns[0].access,
        RowQueryAccess::EdgePropertyIndex {
            edge_type: "RELATES".to_string(),
            property: "chunk_id".to_string(),
        }
    );
    assert!(!plan.groups[0].patterns[0]
        .optimizer_passes
        .contains(&RowQueryOptimizerPass::FullScanFallback));

    let rows = shard.execute_cypher_rows(context, query).await.unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("rid")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::String("relationship-0".to_string()),
            )])],
        )
    );
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_edge_property_queries_reject_graph_epoch_replay() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-edge-property-index-snapshot", object_store).await;

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 10,
            idempotency_key: "cypher-edge-property-index-snapshot-edge".to_string(),
        })
        .await
        .unwrap();
    let pinned_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .set_edge_metadata(
            "reddit-home",
            "FOLLOWS",
            1,
            10,
            EdgeMetadata::default().with_property("weight", VertexPropertyValue::Integer(7)),
        )
        .await
        .unwrap();

    let plan = shard
        .explain_opencypher_rows(
            QueryContext::new("reddit-home", "cypher-edge-property-index-snapshot-explain")
                .at_epoch(pinned_epoch),
            "MATCH ()-[e:FOLLOWS {weight: 7}]->() RETURN count(*)",
        )
        .await
        .unwrap_err();
    assert!(matches!(plan, GraphError::UnsupportedQuery { .. }));

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-edge-property-index-snapshot-read")
                .at_epoch(pinned_epoch),
            "MATCH ()-[e:FOLLOWS {weight: 7}]->() RETURN count(*)",
        )
        .await
        .unwrap_err();
    assert!(matches!(rows, GraphError::UnsupportedQuery { .. }));
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_edge_match_uses_destination_metadata_index_with_reverse_expansion() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-dst-metadata-index",
        object_store,
        GraphOpenOptions {
            limits: GraphLimits {
                max_query_scan_edges: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for (idx, (src, dst)) in [(1, 10), (2, 20), (3, 30)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-dst-metadata-index-{idx}"),
            })
            .await
            .unwrap();
    }
    shard
        .set_vertex_metadata(
            "reddit-home",
            20,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true)),
        )
        .await
        .unwrap();

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-dst-metadata-index-read"),
            "MATCH (u)-[:FOLLOWS]->(v:User {active: true}) RETURN u.id, v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("u.id"), QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![
                QueryValue::VertexId(2),
                QueryValue::VertexId(20),
            ])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn destination_reverse_expansion_rejects_graph_epoch_replay() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer("graph/cypher-dst-snapshot", object_store)
        .await
        .unwrap();

    let write = shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 20,
            idempotency_key: "cypher-dst-snapshot-write".to_string(),
        })
        .await
        .unwrap();
    shard
        .delete_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 20,
            idempotency_key: "cypher-dst-snapshot-delete".to_string(),
        })
        .await
        .unwrap();

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-dst-snapshot-read").at_epoch(write.epoch),
            "MATCH (u)-[:FOLLOWS]->(v {id: 20}) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(rows, GraphError::UnsupportedQuery { .. }));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_rows_filter_project_and_sort_vertex_metadata() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-metadata", object_store).await;

    for (idx, (src, dst)) in [(1, 10), (1, 11), (2, 12), (3, 13)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-row-metadata-edge-{idx}"),
            })
            .await
            .unwrap();
    }

    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true))
                .with_property("name", VertexPropertyValue::String("alice".to_string())),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            2,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true))
                .with_property("name", VertexPropertyValue::String("bob".to_string())),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            3,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(false))
                .with_property("name", VertexPropertyValue::String("carol".to_string())),
        )
        .await
        .unwrap();
    for (vertex, name, age) in [
        (10, "dan", 20),
        (11, "erin", 17),
        (12, "frank", 42),
        (13, "zoe", 60),
    ] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex,
                VertexMetadata::default()
                    .with_label("User")
                    .with_property("name", VertexPropertyValue::String(name.to_string()))
                    .with_property("age", VertexPropertyValue::Integer(age)),
            )
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-row-metadata-read"),
            "MATCH (u:User {active: true})-[:FOLLOWS]->(v:User) \
             WHERE v.age >= 18 AND v.name <> 'zoe' \
             RETURN u.name AS src, v.name AS dst, v.age AS age ORDER BY age DESC",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![
                QueryColumn::new("src"),
                QueryColumn::new("dst"),
                QueryColumn::new("age")
            ],
            vec![
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::String("bob".to_string())),
                    QueryValue::Property(VertexPropertyValue::String("frank".to_string())),
                    QueryValue::Property(VertexPropertyValue::Integer(42)),
                ]),
                QueryRow::new(vec![
                    QueryValue::Property(VertexPropertyValue::String("alice".to_string())),
                    QueryValue::Property(VertexPropertyValue::String("dan".to_string())),
                    QueryValue::Property(VertexPropertyValue::Integer(20)),
                ]),
            ],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn vertex_metadata_indexes_are_replaced_on_update() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/vertex-metadata-index-update", object_store).await;

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 10,
            idempotency_key: "metadata-index-edge".to_string(),
        })
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("old".to_string())),
        )
        .await
        .unwrap();

    let old = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "metadata-index-old"),
            "MATCH (u:User {name: 'old'})-[:FOLLOWS]->(v) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(
        old,
        QueryResultSet::new(
            vec![QueryColumn::new("u.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
        )
    );

    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("new".to_string())),
        )
        .await
        .unwrap();

    let stale = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "metadata-index-stale"),
            "MATCH (u:User {name: 'old'})-[:FOLLOWS]->(v) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(
        stale,
        QueryResultSet::new(vec![QueryColumn::new("u.id")], Vec::new())
    );

    let updated = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "metadata-index-new"),
            "MATCH (u:User {name: 'new'})-[:FOLLOWS]->(v) RETURN u.id",
        )
        .await
        .unwrap();
    assert_eq!(
        updated,
        QueryResultSet::new(
            vec![QueryColumn::new("u.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_vertex_metadata_uses_current_storage_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-row-metadata-snapshot", object_store).await;

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 10,
            idempotency_key: "metadata-snapshot-edge".to_string(),
        })
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true))
                .with_property("name", VertexPropertyValue::String("alice".to_string())),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            10,
            VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("erin".to_string()))
                .with_property("age", VertexPropertyValue::Integer(17)),
        )
        .await
        .unwrap();
    let pinned_epoch = shard.current_epoch("reddit-home").await.unwrap();

    shard
        .set_vertex_metadata(
            "reddit-home",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(false))
                .with_property("name", VertexPropertyValue::String("alice".to_string())),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata(
            "reddit-home",
            10,
            VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("erin".to_string()))
                .with_property("age", VertexPropertyValue::Integer(18)),
        )
        .await
        .unwrap();

    let old_snapshot = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "metadata-snapshot-old").at_epoch(pinned_epoch),
            "MATCH (u:User {active: true})-[:FOLLOWS]->(v:User) \
             WHERE v.age >= 18 RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(old_snapshot, GraphError::UnsupportedQuery { .. }));

    let old_property = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "metadata-snapshot-old-property")
                .at_epoch(pinned_epoch),
            "MATCH (u:User {active: true})-[:FOLLOWS]->(v:User) \
             WHERE v.age = 17 RETURN u.name AS src, v.age AS age",
        )
        .await
        .unwrap_err();
    assert!(matches!(old_property, GraphError::UnsupportedQuery { .. }));

    let latest = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "metadata-snapshot-latest"),
            "MATCH (u:User {active: false})-[:FOLLOWS]->(v:User) \
             WHERE v.age >= 18 RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        latest,
        QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(10)])],
        )
    );
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_node_only_rows_use_vertex_metadata_indexes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-node-only-metadata", object_store).await;

    for (vertex, active, name) in [(1, true, "alice"), (2, true, "bob"), (3, false, "carol")] {
        shard
            .set_vertex_metadata(
                "reddit-home",
                vertex,
                VertexMetadata::default()
                    .with_label("User")
                    .with_property("active", VertexPropertyValue::Bool(active))
                    .with_property("name", VertexPropertyValue::String(name.to_string())),
            )
            .await
            .unwrap();
    }

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "node-only-active-users"),
            "MATCH (u:User {active: true}) RETURN u.name AS name ORDER BY u.name",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("name")],
            vec![
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::String(
                    "alice".to_string()
                ))]),
                QueryRow::new(vec![QueryValue::Property(VertexPropertyValue::String(
                    "bob".to_string()
                ))]),
            ],
        )
    );

    let count = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "node-only-count"),
            "MATCH (u:User {active: true}) RETURN count(*) AS total",
        )
        .await
        .unwrap();
    assert_eq!(
        count,
        QueryResultSet::new(
            vec![QueryColumn::new("total")],
            vec![QueryRow::new(vec![QueryValue::Count(2)])],
        )
    );

    let unbounded = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "node-only-unbounded"),
            "MATCH (u) RETURN u.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(unbounded, GraphError::UnsupportedQuery { .. }));
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn cypher_where_and_variable_hops_use_storage_kernel() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-where-varhop", object_store).await;

    for (idx, (src, dst)) in [(1, 2), (2, 3), (3, 4), (1, 3), (1, 9)]
        .into_iter()
        .enumerate()
    {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-hop-{idx}"),
            })
            .await
            .unwrap();
    }

    let filtered = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "read-req"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) WHERE v.id = 9 RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(filtered, QueryOutput::Vertices(vec![9]));

    let reachable = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "read-req"),
            "MATCH (u {id: 1})-[:FOLLOWS*2..3]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(reachable, QueryOutput::Vertices(vec![3, 4]));

    let exact_two_hop = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "read-req"),
            "MATCH (u {id: 1})-[:FOLLOWS*2..2]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(exact_two_hop, QueryOutput::Vertices(vec![3, 4]));

    for (idx, (src, dst)) in [(1, 2), (2, 3), (1, 3)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "MASKED_BY_SHORTER_PATH".to_string(),
                src,
                dst,
                idempotency_key: format!("cypher-shortest-mask-{idx}"),
            })
            .await
            .unwrap();
    }
    let shortest_mask_regression = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "read-req"),
            "MATCH (u {id: 1})-[:MASKED_BY_SHORTER_PATH*2..2]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(shortest_mask_regression, QueryOutput::Vertices(vec![3]));
}

#[tokio::test]
async fn edge_writes_advance_canonical_adjacency_generation() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/adjacency-generation", object_store).await;

    shard.write_edge(mutation(1, 10, "req-1")).await.unwrap();
    shard.write_edge(mutation(1, 11, "req-2")).await.unwrap();

    let dirty_key = keys::matrix_dirty("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT");
    let dirty_epoch = shard
        .read_remote(&dirty_key)
        .await
        .unwrap()
        .map(|value| decode_u64(&dirty_key, &value).unwrap());
    assert_eq!(dirty_epoch, Some(2));

    let generation_key = keys::adjacency_generation("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT");
    assert_eq!(shard.read_counter(&generation_key).await.unwrap(), 2);
}

#[tokio::test]
async fn reopened_reader_sees_canonical_edge_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/reopen-canonical-records";

    {
        let shard = open_test_shard(path, Arc::clone(&object_store)).await;
        shard.write_edge(mutation(42, 84, "req-1")).await.unwrap();
        shard.close().await.unwrap();
    }

    let reopened = open_test_shard(path, object_store).await;
    assert!(reopened
        .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 42, 84)
        .await
        .unwrap());
}

#[tokio::test]
async fn graphblas_matrix_kernel_matches_rust_kernel_after_canonical_snapshot_rebuild() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/matrix-graphblas", object_store).await;

    for (idx, (src, dst)) in [
        (1, 2),
        (1, 3),
        (2, 4),
        (3, 4),
        (4, 5),
        (42, 100),
        (42, 101),
        (42, 102),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(mutation(src, dst, &format!("graphblas-base-{idx}")))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
        .await
        .unwrap();

    shard
        .write_edge(mutation(4, 6, "graphblas-snapshot-plus"))
        .await
        .unwrap();
    shard
        .delete_edge(mutation(3, 4, "graphblas-snapshot-minus"))
        .await
        .unwrap();
    let read_epoch = shard.current_epoch("reddit-home").await.unwrap();

    let rust = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            read_epoch,
            SparseKernelBackend::Adjacency,
        )
        .await
        .unwrap();
    let graphblas = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            read_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();

    assert_eq!(graphblas.sparse_kernel, SparseKernelBackend::SuiteSparse);
    assert_eq!(graphblas.vertices, rust.vertices);
    assert_eq!(graphblas.edge_visits, rust.edge_visits);
}

#[tokio::test]
async fn graphblas_matrix_kernel_reuses_compiled_base_matrix_cache() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/matrix-graphblas-cache", object_store).await;

    for (idx, (src, dst)) in [
        (1, 2),
        (1, 3),
        (2, 4),
        (3, 4),
        (4, 5),
        (42, 100),
        (42, 101),
        (42, 102),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(mutation(src, dst, &format!("graphblas-cache-base-{idx}")))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
        .await
        .unwrap();

    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
    let cache_counts = shard.graph_cache_entry_counts().await;
    assert_eq!(cache_counts.graphblas_matrices, 1);
    let resident_bytes = shard.graph_cache_resident_bytes().await;
    assert!(resident_bytes.graphblas_matrices > 0);
    assert!(resident_bytes.graphblas_matrices <= GraphMemoryConfig::default().max_graphblas_bytes);
    let first = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();
    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
    let second = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();
    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
    assert_eq!(second.vertices, first.vertices);

    shard
        .write_edge(mutation(4, 6, "graphblas-cache-snapshot-plus"))
        .await
        .unwrap();
    let read_epoch = shard.current_epoch("reddit-home").await.unwrap();
    let rebuilt = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            read_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();
    assert!(rebuilt.vertices.contains(&6));
    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
}

#[tokio::test]
async fn compact_csc_policy_runs_and_reports_the_compact_kernel() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = GraphOpenOptions {
        cache_policy: GraphCachePolicy {
            sparse_kernel: SparseKernelBackend::CompactCsc,
            ..Default::default()
        },
        ..Default::default()
    };
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/kernel-policy-compact",
        object_store,
        options,
    )
    .await
    .unwrap();

    for (idx, (src, dst)) in [
        (1, 2),
        (1, 3),
        (2, 4),
        (3, 4),
        (4, 5),
        (42, 100),
        (42, 101),
        (101, 102),
    ]
    .into_iter()
    .enumerate()
    {
        shard
            .write_edge(mutation(src, dst, &format!("compact-policy-{idx}")))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
        .await
        .unwrap();

    // The argument asks for kernel 3; the policy is authoritative for which
    // compiled rung the artifact was built as, so kernel 2 is what runs.
    let compact = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();
    assert_eq!(compact.sparse_kernel, SparseKernelBackend::CompactCsc);
    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);

    let adjacency = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::Adjacency,
        )
        .await
        .unwrap();
    assert_eq!(adjacency.sparse_kernel, SparseKernelBackend::Adjacency);
    assert_eq!(compact.vertices, adjacency.vertices);
    assert_eq!(compact.edge_visits, adjacency.edge_visits);
}

#[tokio::test]
async fn adjacency_policy_compiles_no_matrix_at_all() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = GraphOpenOptions {
        cache_policy: GraphCachePolicy {
            sparse_kernel: SparseKernelBackend::Adjacency,
            ..Default::default()
        },
        ..Default::default()
    };
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/kernel-policy-adjacency",
        object_store,
        options,
    )
    .await
    .unwrap();

    for (idx, (src, dst)) in [(1, 2), (1, 3), (2, 4), (3, 4), (4, 5), (42, 100)]
        .into_iter()
        .enumerate()
    {
        shard
            .write_edge(mutation(src, dst, &format!("adjacency-policy-{idx}")))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
        .await
        .unwrap();

    // The policy is a ceiling: asking for kernel 3 must not compile one.
    let traversal = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();
    assert_eq!(traversal.sparse_kernel, SparseKernelBackend::Adjacency);
    assert_eq!(shard.graphblas_cache.lock().await.len(), 0);
    assert!(traversal.vertices.contains(&5));
}

#[tokio::test]
async fn graphblas_empty_cache_reader_uses_persisted_csc_artifact() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = open_test_shard(
        "graph/matrix-graphblas-persisted-csc",
        Arc::clone(&object_store),
    )
    .await;

    for (idx, (src, dst)) in [
        (1, 2),
        (1, 3),
        (2, 4),
        (3, 4),
        (4, 5),
        (4, 6),
        (42, 100),
        (42, 101),
        (101, 102),
    ]
    .into_iter()
    .enumerate()
    {
        writer
            .write_edge(mutation(src, dst, &format!("graphblas-csc-base-{idx}")))
            .await
            .unwrap();
    }
    let base_epoch = writer.current_epoch("reddit-home").await.unwrap();
    writer
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
        .await
        .unwrap();
    let expected = writer
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();
    writer.close().await.unwrap();

    let reader = open_test_shard("graph/matrix-graphblas-persisted-csc", object_store).await;
    assert_eq!(reader.graphblas_cache.lock().await.len(), 0);
    assert_eq!(reader.matrix_cache.lock().await.len(), 0);

    let actual = reader
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::SuiteSparse,
        )
        .await
        .unwrap();
    assert_eq!(actual.vertices, expected.vertices);
    assert_eq!(actual.edge_visits, expected.edge_visits);
    assert_eq!(reader.graphblas_cache.lock().await.len(), 1);
    assert_eq!(reader.matrix_cache.lock().await.len(), 0);
}

#[tokio::test]
async fn epoch_scoped_read_is_stable_after_segment_reinsert_clears_the_tombstone() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/epoch-read-stability-reinsert",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let inserted = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "stability-insert")
        .await
        .unwrap();
    assert_eq!(inserted.end_epoch, 1);

    let deleted = shard
        .delete_edge(typed_mutation(cell_id, edge_type, 1, 2, "stability-delete"))
        .await
        .unwrap();
    assert!(deleted.deleted);
    let deleted_epoch = deleted.epoch;

    let deleted_snapshot = shard.snapshot_at(cell_id, deleted_epoch).await.unwrap();
    let before_reinsert = deleted_snapshot.edge_exists(edge_type, 1, 2).await.unwrap();
    assert!(
        !before_reinsert,
        "edge must be absent at its acknowledged delete epoch {deleted_epoch}"
    );

    let reinserted = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "stability-reinsert")
        .await
        .unwrap();
    assert_eq!(reinserted.inserted, 1);

    let after_reinsert = deleted_snapshot.edge_exists(edge_type, 1, 2).await.unwrap();
    assert!(
        !after_reinsert,
        "read at epoch {deleted_epoch} changed its answer after an unrelated later \
         re-insert: the acknowledged delete at epoch {deleted_epoch} has been \
         retroactively erased"
    );
    assert!(matches!(
        shard
            .edge_exists_at(cell_id, edge_type, 1, 2, deleted_epoch)
            .await,
        Err(GraphError::UnsupportedQuery { .. })
    ));
}

#[tokio::test]
async fn epoch_scoped_read_excludes_edges_committed_after_the_requested_epoch() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/epoch-read-excludes-future",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let first = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "future-seed")
        .await
        .unwrap();
    assert_eq!(first.end_epoch, 1);
    let first_snapshot = shard.snapshot_at(cell_id, first.end_epoch).await.unwrap();

    let second = shard
        .write_edge(typed_mutation(cell_id, edge_type, 1, 3, "future-point"))
        .await
        .unwrap();
    assert_eq!(second.epoch, 2);

    let at_first_epoch = first_snapshot.out_neighbors(edge_type, 1).await.unwrap();
    assert_eq!(
        at_first_epoch,
        vec![2],
        "read at epoch {} returned an edge committed later at epoch {}",
        first.end_epoch,
        second.epoch
    );
    assert!(matches!(
        shard
            .out_neighbors_at(cell_id, edge_type, 1, first.end_epoch)
            .await,
        Err(GraphError::UnsupportedQuery { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_epoch_reads_match_acknowledged_history_under_concurrent_reinserts() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(
        GraphShard::open_standalone_writer_with_options(
            "graph/current-epoch-read-race",
            object_store,
            GraphOpenOptions {
                index_policy: GraphIndexPolicy::OutboundOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let seed = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "race-seed")
        .await
        .unwrap();
    assert_eq!(seed.end_epoch, 1);

    // Acknowledged history: epoch -> edge exists after that epoch's operation.
    let history = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::from([(
        seed.end_epoch,
        true,
    )])));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let shard = Arc::clone(&shard);
        let history = Arc::clone(&history);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut cycle = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                let deleted = shard
                    .delete_edge(typed_mutation(
                        cell_id,
                        edge_type,
                        1,
                        2,
                        &format!("race-delete-{cycle}"),
                    ))
                    .await
                    .unwrap();
                assert!(deleted.deleted, "cycle {cycle} delete must apply");
                assert_eq!(
                    shard.current_epoch(cell_id).await.unwrap(),
                    deleted.epoch,
                    "cycle {cycle} delete result must name its committed storage sequence"
                );
                history.lock().unwrap().insert(deleted.epoch, false);
                let reinserted = shard
                    .write_edge(typed_mutation(
                        cell_id,
                        edge_type,
                        1,
                        2,
                        &format!("race-reinsert-{cycle}"),
                    ))
                    .await
                    .unwrap();
                assert!(
                    !reinserted.already_existed,
                    "cycle {cycle} reinsert must apply"
                );
                assert_eq!(
                    shard.current_epoch(cell_id).await.unwrap(),
                    reinserted.epoch,
                    "cycle {cycle} reinsert result must name its committed storage sequence"
                );
                history.lock().unwrap().insert(reinserted.epoch, true);
                cycle += 1;
            }
        })
    };

    let mut anomalies = Vec::new();
    for _ in 0..4_000 {
        let snapshot = shard.snapshot(cell_id).await.unwrap();
        let read_epoch = snapshot.read_epoch();
        let observed = snapshot.edge_exists(edge_type, 1, 2).await.unwrap();
        let (covered, expected) = {
            let history = history.lock().unwrap();
            (
                history.contains_key(&read_epoch),
                history
                    .range(..=read_epoch)
                    .next_back()
                    .map(|(_, exists)| *exists),
            )
        };
        // Only judge when the acknowledged history already covers read_epoch:
        // the op that committed read_epoch must itself be recorded.
        if covered {
            if let Some(expected) = expected {
                if observed != expected {
                    anomalies.push((read_epoch, expected, observed));
                }
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();

    assert!(
        anomalies.is_empty(),
        "{} current-epoch reads contradicted acknowledged history; first: \
         (read_epoch, expected, observed) = {:?}",
        anomalies.len(),
        anomalies.first()
    );
}

// ---------------------------------------------------------------------------
// Ranked data-loss suspects: reproduction attempts.
//
// Each test below encodes the intended contract for one suspect from the
// static analysis. A test that fails has reproduced its bug; a test that
// passes has disproven it through the currently reachable callers and stays
// in the suite as a regression guard. Nothing here applies a fix.
// ---------------------------------------------------------------------------

/// A fresh idempotency key represents a new append intent, even when the
/// requested content matches an earlier import. It must therefore restore an
/// edge deleted after that earlier operation.
#[tokio::test]
async fn trusted_segment_reimport_under_a_fresh_key_restores_an_acknowledged_delete() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/trusted-reimport-resurrection",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    // Create: the original import lands edge 1->2.
    let imported = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "import-run-1")
        .await
        .unwrap();
    assert_eq!(imported.inserted, 1);

    // Delete: acknowledged removal of that edge.
    let deleted = shard
        .delete_edge(typed_mutation(cell_id, edge_type, 1, 2, "reimport-delete"))
        .await
        .unwrap();
    assert!(deleted.deleted, "delete must be acknowledged");
    assert!(
        !shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap(),
        "edge must be absent after its acknowledged delete"
    );

    // A different key is a distinct append operation, so it re-establishes
    // the requested adjacency.
    let reimported = shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "import-run-2")
        .await
        .unwrap();

    assert_eq!(reimported.inserted, 1);
    assert_eq!(reimported.already_existed, 0);
    assert!(shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap());
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 1);
}

/// An identical retry uses the original operation identity and must remain a
/// no-op even if the imported edge was subsequently deleted.
#[tokio::test]
async fn trusted_segment_reimport_under_the_same_key_is_a_noop_after_a_delete() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/trusted-reimport-same-key",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "same-key-import")
        .await
        .unwrap();
    let deleted = shard
        .delete_edge(typed_mutation(cell_id, edge_type, 1, 2, "same-key-delete"))
        .await
        .unwrap();
    assert!(deleted.deleted);

    shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2], "same-key-import")
        .await
        .unwrap();

    assert!(
        !shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap(),
        "an identical retry must replay its idempotency record, not re-insert"
    );
}

/// Suspect 3 — three uncoordinated deleters, no survivor invariant.
///
/// Segment compaction requires a matrix artifact at exactly its
/// `compacted_through_epoch` (`src/shard/maintenance.rs:126-143`); artifact GC
/// deletes artifacts below a caller-trusted `keep_epoch` with no check that
/// anything newer survives (`src/engine/artifact_gc.rs:28`); index-generation
/// GC deletes `_graph_index` files with no guard at all
/// (`src/engine/index_store.rs:226-232`). Each checks a different survivor in a
/// different store. This sequences all three through the public API and asks
/// the only question that matters: does a committed edge stay readable?
#[tokio::test]
async fn committed_edges_stay_readable_after_compaction_then_artifact_gc() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/gc-survivor-sequencing",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    // Create, then modify: two segment appends and one acknowledged delete, so
    // the surviving set is a proper subset of everything ever written.
    shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [2, 3], "gc-seed-a")
        .await
        .unwrap();
    shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 1, [4], "gc-seed-b")
        .await
        .unwrap();
    let deleted = shard
        .delete_edge(typed_mutation(cell_id, edge_type, 1, 3, "gc-delete"))
        .await
        .unwrap();
    assert!(deleted.deleted);

    let expected = vec![2_u64, 4];
    let before = shard.out_neighbors(cell_id, edge_type, 1).await.unwrap();
    assert_eq!(before, expected, "baseline before any GC runs");

    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .build_matrix_tiles(cell_id, edge_type, base_epoch, 4)
        .await
        .unwrap();

    // Deleter 1: compaction collapses the raw segments through base_epoch.
    shard
        .compact_out_adjacency_segments(cell_id, edge_type, 1, base_epoch, "gc-compact")
        .await
        .unwrap();
    let after_compaction = shard.out_neighbors(cell_id, edge_type, 1).await.unwrap();
    assert_eq!(
        after_compaction, expected,
        "compaction must preserve the live edge set"
    );

    // Deleter 2: artifact GC with a caller-trusted keep_epoch above the only
    // artifact that exists. Nothing checks that a newer artifact survives.
    let gc = shard
        .delete_graph_artifacts_before(cell_id, edge_type, base_epoch + 1)
        .await
        .unwrap();
    assert!(
        gc.deleted_keys > 0,
        "the artifact at base_epoch should have been collected"
    );

    let after_artifact_gc = shard.out_neighbors(cell_id, edge_type, 1).await;
    assert_eq!(
        after_artifact_gc.unwrap(),
        expected,
        "committed edges must stay readable after compaction and artifact GC \
         have both run; if this errors or truncates, the three deleters have \
         destroyed the last readable copy"
    );
}

/// Suspect 1 — compiled-index generation ahead of the pinned read snapshot.
///
/// `topology_tail_since` returns an empty overlay when
/// `generation.base_sequence >= read_sequence` (`src/shard/topology_tail.rs:38`).
/// Equality is sound; `>` would merge a CSC built at a newer sequence than the
/// snapshot the read is pinned to. Guard test: assert no reachable caller can
/// pair an old read epoch with a newer generation. `graph_index_generation_at`
/// filters `base_sequence == base_epoch` (`src/engine/index_store.rs:200-207`)
/// and the refresh branch filters `latest.base_sequence <= read_epoch`
/// (`src/shard/query.rs:5281`), so this is expected to pass; it exists to fail
/// loudly if either filter is ever relaxed.
#[tokio::test]
async fn compiled_graph_index_generation_never_exceeds_the_read_epoch() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/generation-ahead-guard",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    for (idx, (src, dst)) in [(1_u64, 2_u64), (2, 3)].into_iter().enumerate() {
        shard
            .bulk_append_out_adjacency_segment_trusted(
                cell_id,
                edge_type,
                src,
                [dst],
                &format!("guard-seed-{idx}"),
            )
            .await
            .unwrap();
    }

    let pinned_read_epoch = shard.current_epoch(cell_id).await.unwrap();

    // Advance the store, then publish a generation strictly ahead of the epoch
    // a reader might still be holding.
    shard
        .bulk_append_out_adjacency_segment_trusted(cell_id, edge_type, 3, [4], "guard-advance")
        .await
        .unwrap();
    let generation = shard.build_graph_index(cell_id, edge_type).await.unwrap();
    assert!(
        generation.base_sequence > pinned_read_epoch,
        "test setup must publish a generation ahead of the pinned read epoch"
    );

    let selected = shard
        .graph_index_generation_at(cell_id, edge_type, pinned_read_epoch)
        .await
        .unwrap();
    assert!(
        selected.is_none_or(|selected| selected.base_sequence <= pinned_read_epoch),
        "generation selection handed back an index built ahead of the read epoch"
    );
}

/// Suspect 2 — WAL-file boundary hole in the base + WAL-tail merge.
///
/// `topology_tail_since` starts its scan at `generation.last_wal_id + 1`
/// (`src/shard/topology_tail.rs:48`) and returns an empty overlay outright when
/// `generation.last_wal_id >= last_durable_wal_id` (`:42-44`). The generation's
/// `last_wal_id` comes from `snapshot.last_wal_id().unwrap_or(0)`
/// (`src/engine/index_store.rs:114`), and on a writer node the durable frontier
/// falls back to `last_flushed_wal_id()` (`src/core/state.rs:361-364`). Neither
/// tracks `snapshot.seq()`. Any commit that lands in the generation's own WAL
/// file — or that is acknowledged from the memtable before the WAL flushes —
/// falls in a hole: not in the compiled base, not in the tail.
///
/// This drives the create → modify → delete interleaving through the public
/// cypher kernel path and compares against the storage ground truth.
#[tokio::test]
async fn compiled_traversal_reflects_writes_committed_after_the_graph_index_generation() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/wal-tail-visibility-hole", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    // Create: 1 -> 2 -> 3, so the two-hop answer from 1 is [3].
    for (idx, (src, dst)) in [(1_u64, 2_u64), (2, 3)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("wal-tail-seed-{idx}"),
            })
            .await
            .unwrap();
    }

    // Build the generation manifest exactly as `build_graph_index` computes it
    // (`src/engine/index_store.rs:113-114`): base_sequence from the snapshot's
    // sequence, last_wal_id from `snapshot.last_wal_id().unwrap_or(0)`. Only
    // the manifest window matters here — the compiled CSC payload plays no part
    // in whether the tail covers the commit window.
    shard.db.refresh_durable_reader().await.unwrap();
    let build_snapshot = shard.db.snapshot().await.unwrap();
    let generation = crate::GraphIndexGeneration {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        base_sequence: build_snapshot.seq(),
        last_wal_id: build_snapshot.last_wal_id().unwrap_or(0),
        edge_count: 2,
        checksum: 0,
        generation: "wal-tail-repro".to_string(),
    };
    drop(build_snapshot);

    // Modify, then delete — both committed strictly after the generation was
    // built, and both landing in the generation's own WAL file.
    shard
        .write_edge(EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src: 2,
            dst: 4,
            idempotency_key: "wal-tail-add".to_string(),
        })
        .await
        .unwrap();
    let deleted = shard
        .delete_edge(typed_mutation(cell_id, edge_type, 2, 3, "wal-tail-delete"))
        .await
        .unwrap();
    assert!(deleted.deleted, "delete must be acknowledged");

    let read_epoch = shard.current_epoch(cell_id).await.unwrap();
    assert!(
        read_epoch > generation.base_sequence,
        "test setup must commit after the generation: read_epoch={read_epoch} \
         base_sequence={}",
        generation.base_sequence
    );

    // Ground truth from live storage: 1 -> 2 -> 4 only.
    let hop_one = shard.out_neighbors(cell_id, edge_type, 1).await.unwrap();
    assert_eq!(hop_one, vec![2]);
    let hop_two = shard.out_neighbors(cell_id, edge_type, 2).await.unwrap();
    assert_eq!(
        hop_two,
        vec![4],
        "storage ground truth after the delete and the append"
    );

    // The overlay is what the compiled traversal merges onto the base CSC. It
    // must carry every edge whose state changed in (base_sequence, read_epoch]:
    // 2->3 as absent, 2->4 as present. Anything less and the compiled read
    // silently answers from the base alone.
    let storage_snapshot = shard.db.snapshot().await.unwrap();
    assert_eq!(
        storage_snapshot.seq(),
        read_epoch,
        "the pinned snapshot must match the read epoch for the tail to be usable"
    );
    use crate::shard::topology_tail::GraphTopologyTail;
    let budget = crate::shard::QueryBudget::new(None, None);
    let tail = shard
        .topology_tail_since(&generation, storage_snapshot.as_ref(), read_epoch, &budget)
        .await
        .unwrap();

    let overlay = match tail {
        GraphTopologyTail::Complete(overlay) => overlay,
        GraphTopologyTail::Unavailable => panic!(
            "tail reported Unavailable, so the compiled path would fall back; \
             this repro needs the Complete branch"
        ),
    };

    assert_eq!(
        overlay.test_state(2, 4),
        Some(true),
        "WAL-tail overlay lost an edge committed in (base_sequence={}, \
         read_epoch={read_epoch}]: 2->4 is missing, so a compiled traversal \
         answers from the stale base. overlay_len={} generation.last_wal_id={}",
        generation.base_sequence,
        overlay.test_len(),
        generation.last_wal_id
    );
    assert_eq!(
        overlay.test_state(2, 3),
        Some(false),
        "WAL-tail overlay lost an acknowledged delete committed in \
         (base_sequence={}, read_epoch={read_epoch}]: 2->3 is missing, so a \
         compiled traversal resurrects it from the base. overlay_len={} \
         generation.last_wal_id={}",
        generation.base_sequence,
        overlay.test_len(),
        generation.last_wal_id
    );
}

/// Suspect 2, the reachable shape. A writer-mode snapshot yields
/// `last_wal_id() == None`, so `unwrap_or(0)` makes the tail scan start at file
/// 1 and sweep everything — the hole is closed by accident. The indexer opens
/// as a *reader* (`src/bin/graph-indexer.rs:138`), and a reader snapshot yields
/// `Some(L)` (`src/core/state.rs:102-105`). The tail then starts at `L + 1`
/// (`src/shard/topology_tail.rs:48`), so any commit that lands in WAL file `L`
/// itself is in neither the compiled base nor the tail.
#[tokio::test]
async fn reader_built_generation_tail_covers_commits_in_its_own_wal_file() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let placement = ObjectStoreNodeDirectory::new(["cell-a"], ["node-a", "node-b"]).unwrap();
    let writer = RoutedGraphCluster::open_promotable_scoped_with_memory_options(
        "graph-wal-tail-reader-hole",
        GraphScope::default(),
        "node-a",
        placement.clone(),
        sole_writer_placement("node-a"),
        Arc::clone(&object_store),
        fast_fence_options(),
        GraphMemoryConfig::default(),
    )
    .await
    .unwrap();
    let cell_id = "cell-a";
    let edge_type = "CHAIN";

    // Create: 1 -> 2 -> 3.
    for (idx, (src, dst)) in [(1_u64, 2_u64), (2, 3)].into_iter().enumerate() {
        writer
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                src,
                dst,
                &format!("reader-hole-seed-{idx}"),
            ))
            .await
            .unwrap();
    }

    // The indexer: a reader-mode shard building a generation off its snapshot.
    let indexer = RoutedGraphCluster::open_readers(
        "graph-wal-tail-reader-hole",
        "node-b",
        placement,
        placement_over("node-b", &["node-a", "node-b"]),
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    let indexer_shard = indexer.shard(cell_id).unwrap();

    indexer_shard.db.refresh_durable_reader().await.unwrap();
    let build_snapshot = indexer_shard.db.snapshot().await.unwrap();
    let generation = crate::GraphIndexGeneration {
        cell_id: cell_id.to_string(),
        edge_type: edge_type.to_string(),
        base_sequence: build_snapshot.seq(),
        last_wal_id: build_snapshot.last_wal_id().unwrap_or(0),
        edge_count: 2,
        checksum: 0,
        generation: "reader-hole-repro".to_string(),
    };
    drop(build_snapshot);
    assert!(
        generation.last_wal_id > 0,
        "this repro needs a reader snapshot that reports a WAL id; got 0, which \
         means the writer-mode unwrap_or(0) path was taken instead"
    );

    // Modify, then delete — committed after the generation, landing in the WAL
    // file the generation already claims to have covered.
    writer
        .write_edge(typed_mutation(cell_id, edge_type, 2, 4, "reader-hole-add"))
        .await
        .unwrap();
    let deleted = writer
        .delete_edge(typed_mutation(
            cell_id,
            edge_type,
            2,
            3,
            "reader-hole-delete",
        ))
        .await
        .unwrap();
    assert!(deleted.deleted, "delete must be acknowledged");

    indexer_shard.db.refresh_durable_reader().await.unwrap();
    let read_snapshot = indexer_shard.db.snapshot().await.unwrap();
    let read_epoch = read_snapshot.seq();
    assert!(
        read_epoch > generation.base_sequence,
        "the reader must have caught up past the generation: read_epoch={read_epoch} \
         base_sequence={}",
        generation.base_sequence
    );

    use crate::shard::topology_tail::GraphTopologyTail;
    let budget = crate::shard::QueryBudget::new(None, None);
    let tail = indexer_shard
        .topology_tail_since(&generation, read_snapshot.as_ref(), read_epoch, &budget)
        .await
        .unwrap();
    let overlay = match tail {
        GraphTopologyTail::Complete(overlay) => overlay,
        GraphTopologyTail::Unavailable => {
            panic!("tail reported Unavailable; this repro needs the Complete branch")
        }
    };

    assert_eq!(
        (overlay.test_state(2, 4), overlay.test_state(2, 3)),
        (Some(true), Some(false)),
        "reader-built generation's WAL tail missed commits in \
         (base_sequence={}, read_epoch={read_epoch}]: the scan starts at \
         last_wal_id + 1 = {}, so entries in file {} are in neither the base nor \
         the tail. overlay_len={}",
        generation.base_sequence,
        generation.last_wal_id + 1,
        generation.last_wal_id,
        overlay.test_len()
    );
}

/// Suspect 4 — the async indexer's generation GC is unfenced.
///
/// The indexer opens as a *reader* (`src/bin/graph-indexer.rs:138`) and mutates
/// `_graph_index` purely through the object store, so SlateDB writer fencing
/// never applies to it. `gc_graph_index_generations`
/// (`src/engine/index_store.rs:210-237`) takes no lease and asks no reader
/// whether it is mid-fetch: a SIGSTOP'd indexer that wakes up long after its
/// view of the world went stale still gets to call `delete`.
///
/// This test drives that exact interleaving: a live reader resolves the
/// `current` manifest, and only *then* does an unfenced zombie replica run GC
/// with `retain_previous = 0` — the most aggressive retention the code accepts,
/// stronger than the `1` the binary defaults to. The invariant under test is
/// that the generation object named by the manifest a reader just resolved is
/// still fetchable afterwards. Also runs the publish and the GC concurrently,
/// so a delete can race a publish, to check no manifest is ever left dangling.
#[tokio::test]
async fn unfenced_index_gc_never_deletes_the_generation_the_current_manifest_names() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/unfenced-index-gc-fence";
    let cell_id = "cell-a";
    let edge_type = "CHAIN";
    use slatedb::object_store::ObjectStoreExt;
    let writer = open_test_shard(path, Arc::clone(&object_store)).await;

    // Three reader-mode shards standing in for indexer replicas. None of them
    // holds a SlateDB writer fence; every one of them can publish and delete
    // `_graph_index` state through the object store alone.
    let indexer = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    let zombie = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    let reader = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();

    let generation_object = |generation: &crate::GraphIndexGeneration| {
        slatedb::object_store::path::Path::from(format!(
            "{path}/_graph_index/{cell_id}/{edge_type}/generations/{:020}-{}.csc",
            generation.base_sequence, generation.generation
        ))
    };

    let mut total_deleted = 0_u64;
    for round in 0..6_u64 {
        writer
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                round + 1,
                round + 2,
                &format!("unfenced-gc-{round}"),
            ))
            .await
            .unwrap();
        indexer.refresh_storage_sequence(cell_id).await.unwrap();
        indexer.build_graph_index(cell_id, edge_type).await.unwrap();

        // A live reader resolves the manifest and is now "mid-fetch": it holds
        // the generation identity but has not read the object yet.
        let resolved = reader
            .current_graph_index(cell_id, edge_type)
            .await
            .unwrap()
            .expect("a generation was just published");

        // The paused zombie wakes up here, with no fence and no reader lease.
        total_deleted = total_deleted.saturating_add(
            zombie
                .gc_graph_index_generations(cell_id, edge_type, 0)
                .await
                .unwrap(),
        );

        // The reader completes its fetch. If GC were unfenced with respect to
        // the *current* generation this is where the object would be gone.
        let location = generation_object(&resolved);
        let payload = object_store
            .get(&location)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "round {round}: unfenced GC deleted the generation a live reader \
                     had just resolved from the manifest ({location}): {err}"
                )
            })
            .bytes()
            .await
            .unwrap();
        assert!(
            !payload.is_empty(),
            "round {round}: generation object {location} is present but empty"
        );
    }

    assert!(
        total_deleted > 0,
        "the invariant above is vacuous unless the zombie GC actually deleted \
         generation objects; it deleted none"
    );

    // Now let a publish and two aggressive GCs race, so a delete can be issued
    // against a listing taken before the publish landed.
    for round in 6..10_u64 {
        writer
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                round + 1,
                round + 2,
                &format!("unfenced-gc-race-{round}"),
            ))
            .await
            .unwrap();
        indexer.refresh_storage_sequence(cell_id).await.unwrap();
        let (built, gc_a, gc_b) = tokio::join!(
            indexer.build_graph_index(cell_id, edge_type),
            zombie.gc_graph_index_generations(cell_id, edge_type, 0),
            reader.gc_graph_index_generations(cell_id, edge_type, 0),
        );
        built.unwrap();
        gc_a.unwrap();
        gc_b.unwrap();

        let current = reader
            .current_graph_index(cell_id, edge_type)
            .await
            .unwrap()
            .expect("a generation is published");
        let location = generation_object(&current);
        object_store.head(&location).await.unwrap_or_else(|err| {
            panic!(
                "round {round}: the published manifest points at a generation object \
                 a racing unfenced GC had already deleted ({location}): {err}"
            )
        });
    }

    reader.close().await.unwrap();
    zombie.close().await.unwrap();
    indexer.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Suspect 4, the reader-observable half. A generation that has been superseded
/// *can* legitimately be deleted underneath a reader still holding it (that is
/// what `retain_previous` bounds). The question that decides data-loss versus
/// availability is what such a reader observes.
///
/// `graph_index_csc` (`src/engine/index_store.rs:148-180`) maps `NotFound` to
/// `Ok(None)` at both the `get` and the `bytes` step, and
/// `cached_graphblas_matrix` (`src/engine/matrix_cache.rs:84-87`) turns that
/// into `forget_graph_index_generation` plus `Ok(None)`, which
/// `compiled_graphblas_query_snapshot` and `matrix_reachable_with_kernel`
/// (`src/engine/traversal.rs:69-94`) treat as "no compiled index, read from
/// storage". So the reader must see a clean miss and correct data — never a
/// truncated or silently-empty answer. This asserts exactly that, without
/// touching the compiled SuiteSparse kernel.
#[tokio::test]
async fn reader_holding_a_gc_deleted_index_generation_sees_a_clean_miss_not_lost_edges() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/unfenced-index-gc-reader-miss";
    let cell_id = "cell-a";
    let edge_type = "CHAIN";
    let writer = open_test_shard(path, Arc::clone(&object_store)).await;
    let indexer = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();
    let reader = GraphShard::open(path, Arc::clone(&object_store))
        .await
        .unwrap();

    writer
        .write_edge(typed_mutation(cell_id, edge_type, 1, 2, "reader-miss-seed"))
        .await
        .unwrap();
    indexer.refresh_storage_sequence(cell_id).await.unwrap();
    indexer.build_graph_index(cell_id, edge_type).await.unwrap();

    // The reader resolves and caches generation G1 while it is still current.
    let held = reader
        .discover_graph_index(cell_id, edge_type)
        .await
        .unwrap()
        .expect("G1 is published");
    assert!(
        reader.graph_index_csc(&held).await.unwrap().is_some(),
        "the reader must be able to fetch G1 while it is current"
    );

    // Two newer generations land, which is what makes G1 collectable at all.
    for (round, (src, dst)) in [(2_u64, 3_u64), (3, 4)].into_iter().enumerate() {
        writer
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                src,
                dst,
                &format!("reader-miss-advance-{round}"),
            ))
            .await
            .unwrap();
        indexer.refresh_storage_sequence(cell_id).await.unwrap();
        indexer.build_graph_index(cell_id, edge_type).await.unwrap();
    }

    // The unfenced zombie GC runs and takes G1 out from under the reader.
    assert!(
        indexer
            .gc_graph_index_generations(cell_id, edge_type, 0)
            .await
            .unwrap()
            >= 1,
        "the test needs GC to actually delete the generation the reader holds"
    );

    // What the reader observes: a clean miss, not a corrupt-value error and not
    // a partial CSC.
    assert!(
        reader.graph_index_csc(&held).await.unwrap().is_none(),
        "a deleted generation must read back as a clean miss"
    );
    assert!(
        reader
            .cached_graphblas_matrix(cell_id, edge_type, held.base_sequence)
            .await
            .unwrap()
            .is_none(),
        "the compiled-matrix layer must report the deleted generation as absent \
         so its callers fall back to storage"
    );

    // And the data itself is untouched: the fallback path answers in full.
    reader.refresh_storage_sequence(cell_id).await.unwrap();
    let read_epoch = reader.current_epoch(cell_id).await.unwrap();
    assert_eq!(
        reader.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2],
        "edges must survive an unfenced index GC"
    );
    assert_eq!(
        reader
            .direct_snapshot_reachable(cell_id, edge_type, &[1], 3, read_epoch)
            .await
            .unwrap()
            .vertices,
        vec![2, 3, 4],
        "the storage fallback must answer in full after the index generation was \
         deleted underneath the reader"
    );

    reader.close().await.unwrap();
    indexer.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Equivalence check for `GraphShard::build_graph_index_incremental`.
///
/// The oracle is content addressing: `generation = sha256(payload)`
/// (`src/engine/index_store.rs`), and the payload deterministically encodes
/// `base_sequence`, `last_wal_id`, the checksum, and the CSC. Two builds taken
/// at the same durable sequence therefore agree on every byte of the published
/// index state if and only if their payload hashes are equal — no invariant
/// has to be argued, only compared. The same property is what makes the
/// main-vs-branch state-equivalence check in the guide test-independent.
///
/// The comparison must be made against the payload *objects*, not the
/// returned manifests: `publish_graph_index` returns the already-current
/// manifest to any same-`(base_sequence, last_wal_id)` proposal, so whichever
/// build runs second has its own generation id laundered into the first's and
/// the manifest assertions become vacuously true. The final assertion — one
/// `.csc` object at the shared base sequence — is the part that can actually
/// fail when the builds disagree.
///
#[tokio::test]
async fn incremental_graph_index_matches_full_rebuild() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/incremental-index-equivalence",
        Arc::clone(&object_store),
    )
    .await;
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    // Seed 1 -> 2 -> 3 plus an isolated 7 -> 8, and publish a real full
    // generation as the baseline the incremental build will patch. The
    // isolated edge exists to be deleted: removing it empties source 7
    // entirely, which is the case that catches an incremental build leaving
    // an empty source key behind — the CSC vertex dictionary is sources ∪
    // destinations (`graphblas_vertices_from_adjacency`), so a stale `7: {}`
    // changes the encoded bytes even though no edge differs.
    for (idx, (src, dst)) in [(1_u64, 2_u64), (2, 3), (7, 8)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("incr-seed-{idx}"),
            })
            .await
            .unwrap();
    }
    let baseline = shard.build_graph_index(cell_id, edge_type).await.unwrap();

    // Mutate strictly after the baseline: add 2 -> 4 and 3 -> 5, delete
    // 2 -> 3. All three land in WAL files beyond `baseline.last_wal_id`, so
    // the tail overlay must carry exactly these deltas.
    for (idx, (src, dst)) in [(2_u64, 4_u64), (3, 5)].into_iter().enumerate() {
        shard
            .write_edge(EdgeMutation {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src,
                dst,
                idempotency_key: format!("incr-add-{idx}"),
            })
            .await
            .unwrap();
    }
    let deleted = shard
        .delete_edge(typed_mutation(cell_id, edge_type, 2, 3, "incr-delete"))
        .await
        .unwrap();
    assert!(deleted.deleted, "the delete must be acknowledged");
    let deleted = shard
        .delete_edge(typed_mutation(
            cell_id,
            edge_type,
            7,
            8,
            "incr-delete-isolated",
        ))
        .await
        .unwrap();
    assert!(deleted.deleted, "the isolated delete must be acknowledged");

    // Incremental first (it patches `baseline`), then a full rebuild at the
    // same durable sequence. Publishing the same content twice is fine — the
    // concurrent-indexer test above already relies on that.
    let (incremental, path) = shard
        .build_graph_index_incremental(cell_id, edge_type)
        .await
        .unwrap()
        .expect("baseline exists and the WAL tail is available in-memory");
    let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();

    match path {
        crate::GraphIndexBuildPath::Incremental { delta_edges } => {
            // 2->4 added, 3->5 added, 2->3 deleted, 7->8 deleted.
            assert_eq!(
                delta_edges, 4,
                "the overlay must carry exactly the changed edges"
            );
        }
        other => panic!("expected an incremental build, got {other:?}"),
    }
    assert!(
        incremental.base_sequence > baseline.base_sequence,
        "the incremental build must advance past the baseline"
    );
    assert_eq!(
        incremental.base_sequence, full.base_sequence,
        "both builds must observe the same durable sequence for the oracle to apply"
    );
    // The oracle: byte-identical payloads, therefore identical content ids.
    assert_eq!(
        incremental.checksum, full.checksum,
        "CSC checksums must match"
    );
    assert_eq!(incremental.edge_count, full.edge_count);
    assert_eq!(
        incremental.generation, full.generation,
        "content-addressed generation ids must be byte-for-byte equal"
    );

    // The manifest comparison above is necessary but NOT sufficient: whichever
    // build publishes second proposes the same `(base_sequence, last_wal_id)`
    // pair as the first, so `publish_graph_index`'s monotonicity guard returns
    // the already-current manifest and the second build's own generation id is
    // never seen — two disagreeing builds would still compare equal. The
    // payload objects are immune to that laundering: each build content-
    // addresses its payload *before* manifest arbitration, so disagreeing
    // builds leave two `.csc` objects at the same base sequence. Exactly one
    // may exist.
    let generations_prefix = slatedb::object_store::path::Path::from(format!(
        "graph/incremental-index-equivalence/_graph_index/{cell_id}/{edge_type}/generations"
    ));
    let sequence_prefix = format!("{:020}-", incremental.base_sequence);
    let mut listing = object_store.list(Some(&generations_prefix));
    let mut at_sequence = Vec::new();
    while let Some(meta) = listing.next().await {
        let location = meta.unwrap().location;
        if location
            .filename()
            .is_some_and(|name| name.starts_with(&sequence_prefix))
        {
            at_sequence.push(location);
        }
    }
    assert_eq!(
        at_sequence.len(),
        1,
        "both builds must produce one identical payload; two objects at the \
         same base sequence mean the builds disagreed and the manifest \
         comparison was laundered: {at_sequence:?}"
    );
}

/// A delta larger than `max_query_scan_edges` used to force the WAL-tail
/// incremental build to decline (`AdmissionRejected` from
/// `topology_tail_since`), because the tail was priced per WAL round trip.
/// The xlog derivation is priced per delta byte, so the same limit no longer
/// applies to the build at all: an oversized delta is absorbed incrementally,
/// the auto path stays incremental, and only `max_artifact_build_edges` —
/// which governs the full rebuild identically — bounds the result.
#[tokio::test]
async fn oversized_delta_no_longer_declines_the_incremental_build() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let limits = GraphLimits {
        max_query_scan_edges: 2,
        ..GraphLimits::default()
    };
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/incremental-index-oversized-tail",
        object_store,
        limits,
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    shard
        .write_edge(typed_mutation(cell_id, edge_type, 1, 2, "oversized-seed"))
        .await
        .unwrap();
    shard.build_graph_index(cell_id, edge_type).await.unwrap();

    // Three affected edges since the baseline, against a read-path scan
    // limit of two — the shape that used to trip the cost gate.
    for (idx, (src, dst)) in [(2_u64, 3_u64), (3, 4), (4, 5)].into_iter().enumerate() {
        shard
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                src,
                dst,
                &format!("oversized-add-{idx}"),
            ))
            .await
            .unwrap();
    }

    let (generation, path) = shard
        .build_graph_index_auto(cell_id, edge_type)
        .await
        .unwrap();
    match path {
        crate::GraphIndexBuildPath::Incremental { delta_edges } => {
            assert_eq!(
                delta_edges, 3,
                "the xlog delta must carry exactly the three added edges"
            );
        }
        other => panic!("auto must have stayed incremental, got {other:?}"),
    }
    assert_eq!(generation.edge_count, 4);

    // And the result must be byte-identical to a full rebuild at the same
    // snapshot — absorbing the delta is only a win if it is also correct.
    let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();
    assert_eq!(generation.generation, full.generation);
}

/// A writer snapshot carries no WAL position of its own (`state.rs`), and
/// before the pre-pin capture in `build_graph_index` a writer-built
/// generation published `last_wal_id = 0`. The zero silently turned every
/// later WAL tail into a walk of the entire WAL history: correct while every
/// file still exists, but SlateDB's WAL GC (60s retention past the compacted
/// boundary) eventually punches holes in that range, after which both the
/// incremental build and the read-path overlay decline to full scans
/// forever. Found by the 5M-edge MinIO stress run, where every incremental
/// cycle fell back for exactly this reason.
#[tokio::test]
async fn writer_built_generations_record_the_wal_position() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer("graph/writer-wal-position", object_store)
        .await
        .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    shard
        .write_edge(typed_mutation(
            cell_id,
            edge_type,
            1,
            2,
            "wal-position-seed",
        ))
        .await
        .unwrap();
    let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();
    assert!(
        full.last_wal_id > 0,
        "a durable write precedes this build, so the generation must record a \
         real WAL position, got {}",
        full.last_wal_id
    );

    // The incremental path stamps the same field, and a correct position on
    // the previous generation is exactly what lets it read a short tail here
    // instead of the whole history.
    shard
        .write_edge(typed_mutation(cell_id, edge_type, 2, 3, "wal-position-add"))
        .await
        .unwrap();
    let (incremental, path) = shard
        .build_graph_index_incremental(cell_id, edge_type)
        .await
        .unwrap()
        .expect("a one-edge tail on a fresh store is buildable");
    assert!(
        matches!(
            path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "expected a one-edge incremental build, got {path:?}"
    );
    assert!(
        incremental.last_wal_id >= full.last_wal_id,
        "the WAL position must never move backwards: {} < {}",
        incremental.last_wal_id,
        full.last_wal_id
    );
}

/// The WAL-tail cost gate (`GraphLimits::max_wal_tail_files`) is a read-path
/// concern only: the builder derives its delta from the xlog and never walks
/// WAL files, so even a cap of zero — under which the old tail-based build
/// declined unconditionally — must leave the incremental build untouched.
/// This is the regression's fix stated as a test: build cost no longer
/// depends on WAL file count at all.
#[tokio::test]
async fn incremental_build_ignores_the_wal_span_cap() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_limits(
        "graph/tail-span-cap",
        object_store,
        GraphLimits {
            max_wal_tail_files: 0,
            ..GraphLimits::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    shard
        .write_edge(typed_mutation(cell_id, edge_type, 1, 2, "cap-seed"))
        .await
        .unwrap();
    let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();

    shard
        .write_edge(typed_mutation(cell_id, edge_type, 2, 3, "cap-add"))
        .await
        .unwrap();
    let (generation, path) = shard
        .build_graph_index_auto(cell_id, edge_type)
        .await
        .unwrap();
    assert!(
        matches!(
            path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "the WAL span cap must not reach the xlog build, got {path:?}"
    );
    assert_eq!(generation.edge_count, 2);
    assert!(
        generation.base_sequence > full.base_sequence,
        "the incremental build must publish the newer snapshot: {} <= {}",
        generation.base_sequence,
        full.base_sequence
    );
}

/// All edge types share one database, and the WAL-tail walk used to make the
/// builder re-download that shared span once per edge type — the request
/// amplification behind the staging regression. The xlog keys each edge
/// type's changes under its own prefix, so each incremental build scans only
/// its own delta and the builder touches **no WAL files at all**: the
/// per-shard WAL file cache (still used by the query-time read path) must
/// stay empty across both builds.
#[tokio::test]
async fn incremental_builds_scan_per_type_deltas_without_walking_wal() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer("graph/tail-shared-parse", object_store)
        .await
        .unwrap();
    let cell_id = "reddit-home";

    shard
        .write_edge(typed_mutation(cell_id, "CHAIN", 1, 2, "shared-seed-a"))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation(cell_id, "REPLIES", 1, 9, "shared-seed-b"))
        .await
        .unwrap();
    shard.build_graph_index(cell_id, "CHAIN").await.unwrap();
    shard.build_graph_index(cell_id, "REPLIES").await.unwrap();

    shard
        .write_edge(typed_mutation(cell_id, "CHAIN", 2, 3, "shared-add-a"))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation(cell_id, "REPLIES", 9, 10, "shared-add-b"))
        .await
        .unwrap();

    let (chain, chain_path) = shard
        .build_graph_index_incremental(cell_id, "CHAIN")
        .await
        .unwrap()
        .expect("a short tail on a fresh store is buildable");
    assert!(
        matches!(
            chain_path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "CHAIN sees only its own delta, got {chain_path:?}"
    );
    assert_eq!(
        shard.test_wal_tail_cached_file_count().await,
        0,
        "the xlog build must not walk WAL files at all"
    );

    let (replies, replies_path) = shard
        .build_graph_index_incremental(cell_id, "REPLIES")
        .await
        .unwrap()
        .expect("the second edge type's delta is scannable from its own prefix");
    assert!(
        matches!(
            replies_path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "REPLIES sees only its own delta, got {replies_path:?}"
    );
    assert_eq!(
        shard.test_wal_tail_cached_file_count().await,
        0,
        "neither build may touch the WAL file cache"
    );

    assert_eq!(chain.edge_count, 2, "CHAIN patched to two edges");
    assert_eq!(replies.edge_count, 2, "REPLIES patched to two edges");
}

/// The missed-site detector: randomized bursts over the public write APIs —
/// single inserts, deletes of existing and absent edges, overlapping bulk
/// imports, trusted segment appends, and a topology-preserving segment
/// compaction — asserting after every burst that the xlog-driven incremental
/// build produces a byte-identical payload (same content-addressed generation
/// id) to a full rebuild at the same snapshot, across two edge types sharing
/// one database, with a GC pass interleaved to prove retention never breaks
/// coverage. Any mutation path that changes topology without logging its
/// delta through `mark_topology_change_txn` breaks the hash here.
#[tokio::test]
async fn xlog_incremental_matches_full_over_random_mutation_mix() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // Outbound-only policy so the mix can include trusted segment appends,
    // which are refused under a reverse-indexing policy.
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/xlog-property-equivalence",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_types = ["CHAIN", "REPLIES"];
    // The segment-append source: trusted appends bypass canonical edge keys
    // entirely, so their xlog entries are the only trace the builder sees.
    let segment_src = 77_u64;

    // Deterministic xorshift so a failure names a reproducible seed.
    let mut rng_state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut rng = move || {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state
    };

    let mut models: std::collections::BTreeMap<&str, BTreeSet<(u64, u64)>> =
        edge_types.iter().map(|ty| (*ty, BTreeSet::new())).collect();

    // Seed both types and publish full baselines the increments will patch.
    for edge_type in edge_types {
        let seed: Vec<(u64, u64)> = (0..24).map(|_| (1 + rng() % 20, 1 + rng() % 40)).collect();
        for (src, dst) in &seed {
            models.get_mut(edge_type).unwrap().insert((*src, *dst));
        }
        shard
            .bulk_import_edges(cell_id, edge_type, seed, &format!("xlog-seed-{edge_type}"))
            .await
            .unwrap();
        shard.build_graph_index(cell_id, edge_type).await.unwrap();
    }

    for round in 0..6 {
        for edge_type in edge_types {
            let model = models.get_mut(edge_type).unwrap();
            // Single inserts, some duplicating existing edges (idempotent
            // re-inserts log nothing new logically and must stay harmless).
            for op in 0..8 {
                let (src, dst) = (1 + rng() % 20, 1 + rng() % 40);
                shard
                    .write_edge(typed_mutation(
                        cell_id,
                        edge_type,
                        src,
                        dst,
                        &format!("xlog-mix-{edge_type}-{round}-w{op}"),
                    ))
                    .await
                    .unwrap();
                model.insert((src, dst));
            }
            // Deletes: half aimed at live edges, half at arbitrary pairs
            // that may not exist (an unacknowledged delete logs nothing).
            for op in 0..4 {
                let (src, dst) = if op % 2 == 0 && !model.is_empty() {
                    let pick = rng() as usize % model.len();
                    *model.iter().nth(pick).unwrap()
                } else {
                    (1 + rng() % 20, 1 + rng() % 40)
                };
                let deleted = shard
                    .delete_edge(typed_mutation(
                        cell_id,
                        edge_type,
                        src,
                        dst,
                        &format!("xlog-mix-{edge_type}-{round}-d{op}"),
                    ))
                    .await
                    .unwrap();
                if deleted.deleted {
                    model.remove(&(src, dst));
                }
            }
            // An overlapping bulk import every round.
            let bulk: Vec<(u64, u64)> = (0..6).map(|_| (1 + rng() % 20, 1 + rng() % 40)).collect();
            for (src, dst) in &bulk {
                model.insert((*src, *dst));
            }
            shard
                .bulk_import_edges(
                    cell_id,
                    edge_type,
                    bulk,
                    &format!("xlog-mix-{edge_type}-{round}-bulk"),
                )
                .await
                .unwrap();
            // Trusted segment appends on alternating rounds: these edges
            // never touch canonical keys, so only their xlog entries and the
            // segment values themselves carry them.
            if round % 2 == 0 {
                let dsts: Vec<u64> = (0..3).map(|_| 200 + rng() % 25).collect();
                for dst in &dsts {
                    model.insert((segment_src, *dst));
                }
                shard
                    .bulk_append_out_adjacency_segment_trusted(
                        cell_id,
                        edge_type,
                        segment_src,
                        dsts,
                        &format!("xlog-mix-{edge_type}-{round}-seg"),
                    )
                    .await
                    .unwrap();
            }

            let (incremental, path) = shard
                .build_graph_index_incremental(cell_id, edge_type)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("round {round} {edge_type}: must stay incremental"));
            assert!(
                matches!(path, crate::GraphIndexBuildPath::Incremental { .. }),
                "round {round} {edge_type}: expected incremental, got {path:?}"
            );
            let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();
            assert_eq!(
                incremental.base_sequence, full.base_sequence,
                "round {round} {edge_type}: both builds must pin the same sequence"
            );
            assert_eq!(
                incremental.generation, full.generation,
                "round {round} {edge_type}: a diverging content id means a \
                 mutation path changed topology without logging its delta"
            );
            assert_eq!(
                incremental.edge_count,
                model.len() as u64,
                "round {round} {edge_type}: the index must match the model"
            );
        }

        // Interleave retention mid-run: GC must reclaim consumed entries
        // without ever breaking the next round's coverage.
        if round == 2 {
            for edge_type in edge_types {
                let deleted = shard
                    .gc_topology_changelog(cell_id, edge_type)
                    .await
                    .unwrap();
                assert!(
                    deleted > 0,
                    "{edge_type}: three rounds of mutations must leave dead entries"
                );
            }
        }
    }

    // Topology-preserving segment compaction logs nothing — the published
    // index must be indifferent to it. Requires a matrix artifact at the
    // compacted-through epoch, exactly like the production maintenance path.
    let edge_type = edge_types[0];
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .build_matrix_tiles(cell_id, edge_type, base_epoch, 4)
        .await
        .unwrap();
    shard
        .compact_out_adjacency_segments(cell_id, edge_type, segment_src, base_epoch, "xlog-compact")
        .await
        .unwrap();
    let (incremental, _) = shard
        .build_graph_index_incremental(cell_id, edge_type)
        .await
        .unwrap()
        .expect("post-compaction build must stay incremental");
    let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();
    assert_eq!(
        incremental.generation, full.generation,
        "segment compaction changed the published index despite preserving topology"
    );
    assert_eq!(
        incremental.edge_count,
        models[edge_type].len() as u64,
        "compaction must not change the edge set"
    );
}

/// Multi-tenancy: two cells share one shard, mutate concurrently, and each
/// cell's incremental build must see exactly its own delta — never the
/// neighbor's — with byte-identical-to-full results per cell, and one cell's
/// xlog GC must not disturb the other's coverage. The cell id is part of the
/// xlog key's address, so cross-cell reads are impossible by key-range
/// construction; this test pins that as behavior rather than argument.
#[tokio::test]
async fn xlog_is_isolated_by_cell() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/xlog-cell-isolation", object_store).await;
    let edge_type = "CHAIN";

    for cell in ["tenant-a", "tenant-b"] {
        shard
            .write_edge(typed_mutation(cell, edge_type, 1, 2, "iso-seed"))
            .await
            .unwrap();
        shard.build_graph_index(cell, edge_type).await.unwrap();
    }

    // Interleaved mutations: tenant-a gains two edges and loses one,
    // tenant-b gains one — deliberately overlapping vertex ids, so only the
    // cell in the key can tell the entries apart.
    shard
        .write_edge(typed_mutation("tenant-a", edge_type, 2, 3, "iso-a1"))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation("tenant-b", edge_type, 2, 3, "iso-b1"))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation("tenant-a", edge_type, 3, 4, "iso-a2"))
        .await
        .unwrap();
    let deleted = shard
        .delete_edge(typed_mutation("tenant-a", edge_type, 1, 2, "iso-a3"))
        .await
        .unwrap();
    assert!(deleted.deleted);

    let (a, a_path) = shard
        .build_graph_index_incremental("tenant-a", edge_type)
        .await
        .unwrap()
        .expect("tenant-a stays incremental");
    assert!(
        matches!(
            a_path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 3 }
        ),
        "tenant-a must see exactly its own three changes, got {a_path:?}"
    );
    // The byte-identical oracle per cell, at the same durable sequence as
    // the incremental build it checks (no writes in between).
    let a_full = shard
        .build_graph_index("tenant-a", edge_type)
        .await
        .unwrap();
    assert_eq!(a.generation, a_full.generation);
    let (b, b_path) = shard
        .build_graph_index_incremental("tenant-b", edge_type)
        .await
        .unwrap()
        .expect("tenant-b stays incremental");
    assert!(
        matches!(
            b_path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "tenant-b must see exactly its own single change, got {b_path:?}"
    );
    let b_full = shard
        .build_graph_index("tenant-b", edge_type)
        .await
        .unwrap();
    assert_eq!(b.generation, b_full.generation);
    assert_eq!(a.edge_count, 2, "tenant-a: 1→2 deleted, 2→3 and 3→4 added");
    assert_eq!(b.edge_count, 2, "tenant-b: 1→2 kept, 2→3 added");

    // One cell's GC must not break the other's coverage: reclaim tenant-a,
    // then tenant-b must still build incrementally with an exact delta.
    let reclaimed = shard
        .gc_topology_changelog("tenant-a", edge_type)
        .await
        .unwrap();
    assert!(reclaimed > 0, "tenant-a has consumed entries to reclaim");
    shard
        .write_edge(typed_mutation("tenant-b", edge_type, 3, 4, "iso-b2"))
        .await
        .unwrap();
    let (b_after, b_after_path) = shard
        .build_graph_index_incremental("tenant-b", edge_type)
        .await
        .unwrap()
        .expect("tenant-b coverage survives tenant-a's GC");
    assert!(
        matches!(
            b_after_path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "got {b_after_path:?}"
    );

    let b_after_full = shard
        .build_graph_index("tenant-b", edge_type)
        .await
        .unwrap();
    assert_eq!(b_after.generation, b_after_full.generation);
    assert_ne!(a.generation, b_after.generation, "cells stay distinct");
}

/// Retention lifecycle: GC reclaims exactly the consumed entries once,
/// reports zero on an immediately repeated pass, and later builds remain
/// incremental and byte-identical — the low-water mark never breaks coverage
/// for a range a future build still needs.
#[tokio::test]
async fn xlog_gc_reclaims_consumed_entries_and_preserves_coverage() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/xlog-gc-lifecycle", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    shard
        .write_edge(typed_mutation(cell_id, edge_type, 1, 2, "gc-seed"))
        .await
        .unwrap();
    shard.build_graph_index(cell_id, edge_type).await.unwrap();
    shard
        .write_edge(typed_mutation(cell_id, edge_type, 2, 3, "gc-add"))
        .await
        .unwrap();
    let (generation, _) = shard
        .build_graph_index_incremental(cell_id, edge_type)
        .await
        .unwrap()
        .expect("incremental after baseline");

    let deleted = shard
        .gc_topology_changelog(cell_id, edge_type)
        .await
        .unwrap();
    assert!(
        deleted > 0,
        "entries at or below base {} must be reclaimed",
        generation.base_sequence
    );
    assert_eq!(
        shard
            .gc_topology_changelog(cell_id, edge_type)
            .await
            .unwrap(),
        0,
        "an immediately repeated pass has nothing left to reclaim"
    );

    shard
        .write_edge(typed_mutation(cell_id, edge_type, 3, 4, "gc-post"))
        .await
        .unwrap();
    let (incremental, path) = shard
        .build_graph_index_incremental(cell_id, edge_type)
        .await
        .unwrap()
        .expect("coverage must survive GC");
    assert!(
        matches!(
            path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "got {path:?}"
    );
    let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();
    assert_eq!(incremental.generation, full.generation);
}

/// A purged low-water mark (manual intervention, or a pre-xlog generation) is
/// the `Uninitialized` bootstrap: the incremental build declines exactly
/// once, the full rebuild re-establishes coverage through the next logged
/// mutation, and the cycle after is incremental again.
#[tokio::test]
async fn xlog_purge_forces_one_bootstrap_then_recovers() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/xlog-purge-bootstrap", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "CHAIN";

    shard
        .write_edge(typed_mutation(cell_id, edge_type, 1, 2, "purge-seed"))
        .await
        .unwrap();
    shard.build_graph_index(cell_id, edge_type).await.unwrap();
    shard
        .write_edge(typed_mutation(cell_id, edge_type, 2, 3, "purge-add"))
        .await
        .unwrap();

    // Simulate a manual purge: the coverage floor disappears out from under
    // the builder.
    let mut batch = WriteBatch::new();
    batch.delete(keys::xlog_low_water(cell_id, edge_type).as_bytes());
    shard.write_strict_for_test(batch).await.unwrap();

    assert!(
        shard
            .build_graph_index_incremental(cell_id, edge_type)
            .await
            .unwrap()
            .is_none(),
        "a missing floor must bootstrap, never guess at coverage"
    );
    let full = shard.build_graph_index(cell_id, edge_type).await.unwrap();
    assert_eq!(full.edge_count, 2, "the bootstrap full build is correct");

    // The write path's floor cache still believes the floor exists — only
    // the GC pass, which reads storage under writer authority, notices the
    // purge and repairs the floor at the current epoch. This is the same
    // call the indexer's cleanup step makes every cycle.
    assert_eq!(
        shard
            .gc_topology_changelog(cell_id, edge_type)
            .await
            .unwrap(),
        0,
        "the repair pass reclaims nothing; it only restores the floor"
    );

    // With the floor repaired, the next mutation is covered and the build
    // after it is incremental again.
    shard
        .write_edge(typed_mutation(cell_id, edge_type, 3, 4, "purge-recover"))
        .await
        .unwrap();
    let (incremental, path) = shard
        .build_graph_index_incremental(cell_id, edge_type)
        .await
        .unwrap()
        .expect("coverage re-established after one bootstrap");
    assert!(
        matches!(
            path,
            crate::GraphIndexBuildPath::Incremental { delta_edges: 1 }
        ),
        "got {path:?}"
    );
    let check = shard.build_graph_index(cell_id, edge_type).await.unwrap();
    assert_eq!(incremental.generation, check.generation);
}
