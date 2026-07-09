use super::*;
use slatedb::bytes::Bytes;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::{PrefixExtractor, PrefixTarget};

async fn open_test_shard(path: &str, object_store: Arc<dyn ObjectStore>) -> GraphShard {
    GraphShard::open_standalone_writer(path, object_store)
        .await
        .unwrap()
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
fn adjacency_from_records_for_test(edges: &[EdgeRecord]) -> MatrixAdjacency {
    let mut adjacency = MatrixAdjacency::new();
    for edge in edges {
        adjacency.entry(edge.src).or_default().insert(edge.dst);
    }
    adjacency
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
            .bulk_append_supernode_segment_trusted_txn(
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

async fn append_edge_mutation_log_txn_retry_for_test(
    shard: Arc<GraphShard>,
    cell_id: &str,
    batch_id: &str,
    mutations: Vec<EdgeMutation>,
) -> Result<EdgeMutationLogAppendResult> {
    let fingerprint = edge_mutation_log_fingerprint(cell_id, batch_id, &mutations);
    for attempt in 0..GRAPH_TXN_MAX_RETRIES {
        match shard
            .append_edge_mutation_log_txn(cell_id, batch_id, &mutations, fingerprint)
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

fn assert_stale_node_a(err: GraphError) {
    assert!(matches!(
        err,
        GraphError::StaleShardLease {
            ref cell_id,
            ref node_id,
            lease_token: 1
        } if cell_id == "reddit-home" && node_id == "node-a"
    ));
}

#[tokio::test]
async fn raw_graph_shard_open_is_read_only() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open("graph/read-only-open", object_store)
        .await
        .unwrap();
    let err = shard
        .write_edge(mutation(1, 2, "read-only"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::WriteRequiresLease {
            operation: "write_edge",
            cell_id
        } if cell_id == "reddit-home"
    ));
}

#[tokio::test]
async fn write_authoritative_open_rejects_relaxed_durability() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = GraphOpenOptions {
        durability: GraphDurabilityConfig::default().with_await_durable_writes(false),
        ..Default::default()
    };

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
        } if reason.contains("remote-visible metadata")
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
        assert_eq!(writer.matrix_cache.lock().await.len(), 1);
        assert!(writer.graph_cache_metrics().evictions >= 1);
        writer.close().await.unwrap();
    }

    let reader = GraphShard::open_with_options(path, object_store, options)
        .await
        .unwrap();
    for _ in 0..2 {
        reader
            .matrix_reachable_with_kernel(
                "reddit-home",
                "USER_SUBSCRIBED_TO_SUBREDDIT",
                &[1],
                1,
                2,
                SparseKernelBackend::RustSparse,
            )
            .await
            .unwrap();
    }
    let metrics = reader.graph_cache_metrics();
    assert!(metrics.matrix_artifact_misses >= 1);
    assert!(metrics.matrix_artifact_hits >= 1);
    assert!(metrics.matrix_adjacency_misses >= 1);
    assert!(metrics.matrix_adjacency_hits >= 1);
    assert!(metrics.hydration_started >= 2);
    assert!(metrics.hydration_completed >= 2);
    reader.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn graph_layer_caches_match_reopened_object_store_truth() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/cache-accuracy-reopen";
    let cell_id = "reddit-home";
    let edge_type = "CACHE_EDGE";
    let edges = [
        (1, 2),
        (1, 3),
        (2, 4),
        (3, 5),
        (4, 6),
        (5, 7),
        (100, 200),
        (100, 201),
        (100, 202),
        (100, 203),
        (100, 204),
        (100, 205),
        (100, 206),
        (100, 207),
    ];
    let options = GraphOpenOptions {
        cache_policy: GraphCachePolicy {
            prefetch_supernode_chunks: 2,
            ..Default::default()
        },
        ..Default::default()
    };

    let base_epoch = {
        let writer = GraphShard::open_standalone_writer_with_options(
            path,
            Arc::clone(&object_store),
            options.clone(),
        )
        .await
        .unwrap();
        for (idx, (src, dst)) in edges.into_iter().enumerate() {
            writer
                .write_edge(typed_mutation(
                    cell_id,
                    edge_type,
                    src,
                    dst,
                    &format!("cache-accuracy-edge-{idx}"),
                ))
                .await
                .unwrap();
        }
        let base_epoch = writer.current_epoch(cell_id).await.unwrap();
        writer
            .build_matrix_tiles(cell_id, edge_type, base_epoch, 4)
            .await
            .unwrap();
        writer
            .build_supernode_groups_for_directions(
                cell_id,
                edge_type,
                base_epoch,
                4,
                3,
                &[ArtifactDirection::Out],
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        base_epoch
    };

    let reader = GraphShard::open_with_options(path, Arc::clone(&object_store), options)
        .await
        .unwrap();
    assert_eq!(
        reader.graph_cache_entry_counts().await,
        GraphCacheEntryCounts::default()
    );

    let raw_edges = reader
        .edges_at(cell_id, edge_type, base_epoch)
        .await
        .unwrap();
    let expected_adjacency = adjacency_from_records_for_test(&raw_edges);
    assert_eq!(
        expected_adjacency
            .get(&100)
            .map(|neighbors| neighbors.iter().copied().collect::<Vec<_>>()),
        Some((200..208).collect::<Vec<_>>())
    );

    let posting_truth = reader
        .posting_reachable(cell_id, edge_type, &[1], 3, base_epoch)
        .await
        .unwrap();
    let matrix_rust = reader
        .matrix_reachable_with_kernel(
            cell_id,
            edge_type,
            &[1],
            3,
            base_epoch,
            SparseKernelBackend::RustSparse,
        )
        .await
        .unwrap();
    assert_eq!(matrix_rust.vertices, posting_truth.vertices);
    assert_eq!(matrix_rust.delta_records_applied, 0);

    let matrix_key = MatrixCacheKey::new(cell_id, edge_type, base_epoch);
    let cached_adjacency = reader
        .matrix_cache
        .lock()
        .await
        .get(&matrix_key)
        .expect("matrix adjacency should be cached after RustSparse traversal");
    assert_eq!(cached_adjacency.as_ref(), &expected_adjacency);
    let cached_artifact = reader
        .matrix_artifact_cache
        .lock()
        .await
        .get(&matrix_key)
        .expect("matrix artifact manifest should be cached after traversal");
    assert_eq!(cached_artifact.base_epoch, base_epoch);
    assert_eq!(cached_artifact.edge_count, raw_edges.len() as u64);

    #[cfg(feature = "graphblas")]
    {
        let graphblas = reader
            .matrix_reachable_with_kernel(
                cell_id,
                edge_type,
                &[1],
                3,
                base_epoch,
                SparseKernelBackend::SuiteSparseGraphBlas,
            )
            .await
            .unwrap();
        assert_eq!(graphblas.vertices, posting_truth.vertices);
        assert!(reader
            .graphblas_cache
            .lock()
            .await
            .get(&matrix_key)
            .is_some());
    }

    let rows = reader
        .execute_cypher_rows(
            QueryContext::new(cell_id, "cache-accuracy-varhop").at_epoch(base_epoch),
            "MATCH (u {id: 1})-[:CACHE_EDGE*1..3]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    let row_vertices = rows
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [QueryValue::VertexId(vertex)] => *vertex,
            values => panic!("unexpected row values: {values:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(row_vertices, posting_truth.vertices);
    let reachability_key = ReachabilityCacheKey::new(cell_id, edge_type, 1, (1, 3), base_epoch);
    let cached_reachability = reader
        .reachability_cache
        .lock()
        .await
        .get(&reachability_key)
        .expect("variable-hop query should cache reachability output");
    assert_eq!(
        cached_reachability
            .vertices()
            .expect("reachability cache should store vertices")
            .as_ref(),
        &posting_truth.vertices
    );
    assert_eq!(
        cached_reachability.count(),
        posting_truth.vertices.len() as u64
    );

    let page = reader
        .supernode_page(
            cell_id,
            edge_type,
            ArtifactDirection::Out,
            100,
            base_epoch,
            0,
        )
        .await
        .unwrap()
        .expect("supernode page should exist");
    assert_eq!(page.vertices, vec![200, 201, 202]);
    assert!(page.has_next);

    let supernode_truth = reader
        .matrix_reachable_with_kernel(
            cell_id,
            edge_type,
            &[100],
            1,
            base_epoch,
            SparseKernelBackend::RustSparse,
        )
        .await
        .unwrap();
    assert_eq!(supernode_truth.vertices, (200..208).collect::<Vec<_>>());

    let supernode_key =
        SupernodeCacheKey::new(cell_id, edge_type, ArtifactDirection::Out, 100, base_epoch);
    let cached_group = reader
        .supernode_group_cache
        .lock()
        .await
        .get(&supernode_key)
        .expect("supernode group should be cached");
    assert_eq!(cached_group.degree, 8);
    assert_eq!(cached_group.chunk_count, 3);
    let cached_chunk = reader
        .posting_chunk_cache
        .lock()
        .await
        .get(&PostingChunkCacheKey::new(&cached_group, 0))
        .expect("first posting chunk should be cached");
    assert_eq!(cached_chunk.vertices, vec![200, 201, 202]);
    let materialized = reader
        .materialized_supernode_cache
        .lock()
        .await
        .get(&supernode_key)
        .expect("full one-hop supernode read should cache materialized vertices");
    assert_eq!(materialized.as_ref(), &(200..208).collect::<Vec<_>>());

    reader.close().await.unwrap();
}

#[tokio::test]
async fn supernode_lookup_prefetches_and_caches_posting_chunks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/supernode-prefetch";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    {
        let writer = open_test_shard(path, Arc::clone(&object_store)).await;
        for dst in 10..18 {
            writer
                .write_edge(EdgeMutation {
                    cell_id: "reddit-home".to_string(),
                    edge_type: edge_type.to_string(),
                    src: 1,
                    dst,
                    idempotency_key: format!("prefetch-{dst}"),
                })
                .await
                .unwrap();
        }
        let base_epoch = writer.current_epoch("reddit-home").await.unwrap();
        writer
            .build_supernode_groups("reddit-home", edge_type, base_epoch, 4, 2)
            .await
            .unwrap();
        writer.close().await.unwrap();
    }

    let reader = GraphShard::open_with_options(
        path,
        object_store,
        GraphOpenOptions {
            cache_policy: GraphCachePolicy {
                prefetch_supernode_chunks: 2,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let read_epoch = reader.current_epoch("reddit-home").await.unwrap();
    let page = reader
        .supernode_page(
            "reddit-home",
            edge_type,
            ArtifactDirection::Out,
            1,
            read_epoch,
            0,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.vertices, vec![10, 11]);
    assert!(reader.posting_chunk_cache.lock().await.len() >= 1);
    let metrics = reader.graph_cache_metrics();
    assert!(metrics.supernode_group_misses >= 1);
    assert!(metrics.prefetch_requests >= 1);
    assert!(metrics.posting_chunk_misses >= 1);
    assert!(metrics.posting_chunk_hits >= 1);
    reader.close().await.unwrap();
}

#[tokio::test]
async fn write_edge_commits_canonical_records_and_outbox() {
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

    let outbox = shard.outbox_since("reddit-home", 0).await.unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].kind, DeltaKind::Plus);
    assert_eq!(outbox[0].edge.src, 1);
    assert_eq!(outbox[0].edge.dst, 2);
}

#[test]
fn compact_v2_values_decode_alongside_legacy_v1_values() {
    let edge_key = keys::out_edge("reddit-home", "USER_FOLLOWS_USER", 1, 2);
    let v1_edge = b"edge1\t7\treddit-home\tUSER_FOLLOWS_USER\t1\t2\n";
    let decoded_v1 = decode_edge_record(&edge_key, v1_edge).unwrap();
    assert_eq!(decoded_v1.epoch, 7);
    assert_eq!(decoded_v1.src, 1);
    assert_eq!(decoded_v1.dst, 2);

    let v2_edge = decode_edge_record(&edge_key, b"edge2\t8\n").unwrap();
    assert_eq!(v2_edge.epoch, 8);
    assert_eq!(v2_edge.edge_type, "USER_FOLLOWS_USER");
    assert_eq!(v2_edge.src, 1);
    assert_eq!(v2_edge.dst, 2);

    let v3_edge = decode_edge_record(&edge_key, &encode_edge_epoch(9)).unwrap();
    assert_eq!(v3_edge.epoch, 9);
    assert_eq!(v3_edge.edge_type, "USER_FOLLOWS_USER");
    assert_eq!(v3_edge.src, 1);
    assert_eq!(v3_edge.dst, 2);

    let outbox_key = keys::outbox("reddit-home", 9, DeltaKind::Plus, "USER_FOLLOWS_USER", 1, 2);
    let delta_v2 = decode_delta_record(&outbox_key, b"delta2\n").unwrap();
    assert_eq!(delta_v2.kind, DeltaKind::Plus);
    assert_eq!(delta_v2.edge.epoch, 9);
    assert_eq!(delta_v2.edge.src, 1);
    assert_eq!(delta_v2.edge.dst, 2);

    let outbox_batch_key = keys::outbox_batch(
        "reddit-home",
        11,
        10,
        DeltaKind::Plus,
        "USER_FOLLOWS_USER",
        "b1",
    );
    let outbox_batch_v2 = encode_outbox_delta_batch(
        "reddit-home",
        "USER_FOLLOWS_USER",
        DeltaKind::Plus,
        10,
        11,
        &[(1, 2), (3, 4)],
    );
    assert!(outbox_batch_v2.starts_with(b"outbox_batch2\n"));
    let decoded_batch_v2 = decode_outbox_delta_batch(&outbox_batch_key, &outbox_batch_v2).unwrap();
    assert_eq!(decoded_batch_v2.edges, vec![(1, 2), (3, 4)]);
    assert_eq!(decoded_batch_v2.start_epoch, 10);
    assert_eq!(decoded_batch_v2.end_epoch, 11);

    let outbox_batch_v3 = encode_outbox_delta_batch(
        "reddit-home",
        "USER_FOLLOWS_USER",
        DeltaKind::Plus,
        10,
        11,
        &[(9, 2), (9, 4)],
    );
    assert!(outbox_batch_v3.starts_with(b"outbox_batch3\n"));
    assert!(outbox_batch_v3.len() < outbox_batch_v2.len());
    let decoded_batch_v3 = decode_outbox_delta_batch(&outbox_batch_key, &outbox_batch_v3).unwrap();
    assert_eq!(decoded_batch_v3.edges, vec![(9, 2), (9, 4)]);
    assert_eq!(decoded_batch_v3.start_epoch, 10);
    assert_eq!(decoded_batch_v3.end_epoch, 11);

    let outbox_batch_v1 =
        b"outbox_batch1\treddit-home\tUSER_FOLLOWS_USER\t10\t11\tplus\t2\n1\t2\n3\t4\n";
    let decoded_batch_v1 = decode_outbox_delta_batch(&outbox_batch_key, outbox_batch_v1).unwrap();
    assert_eq!(decoded_batch_v1.edges, decoded_batch_v2.edges);

    let owner_key = keys::owner_delta(
        "reddit-home",
        DeltaKind::Minus,
        "USER_FOLLOWS_USER",
        "in",
        2,
        10,
        1,
    );
    let owner_delta = decode_delta_record(&owner_key, b"delta2\n").unwrap();
    assert_eq!(owner_delta.kind, DeltaKind::Minus);
    assert_eq!(owner_delta.edge.epoch, 10);
    assert_eq!(owner_delta.edge.src, 1);
    assert_eq!(owner_delta.edge.dst, 2);

    let legacy_delta = b"delta1\t+\t11\treddit-home\tUSER_FOLLOWS_USER\t3\t4\n";
    let decoded_legacy = decode_delta_record(&outbox_key, legacy_delta).unwrap();
    assert_eq!(decoded_legacy.kind, DeltaKind::Plus);
    assert_eq!(decoded_legacy.edge.epoch, 11);
    assert_eq!(decoded_legacy.edge.src, 3);
    assert_eq!(decoded_legacy.edge.dst, 4);
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
    assert_eq!(shard.outbox_since("reddit-home", 0).await.unwrap().len(), 1);
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
                end_epoch: 2,
                inserted: 2,
                already_existed: 0,
            },
            BulkImportResult {
                start_epoch: 3,
                end_epoch: 4,
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
    assert!(results.iter().all(|result| result.epoch == 1));
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 7)
            .await
            .unwrap(),
        1
    );
    assert_eq!(shard.outbox_since("reddit-home", 0).await.unwrap().len(), 1);
}

#[tokio::test]
async fn bulk_import_edges_writes_normal_indexes_deltas_and_idempotency() {
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
            end_epoch: 2,
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
    assert_eq!(
        shard
            .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
            .await
            .unwrap()
            .iter()
            .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
            .collect::<Vec<_>>(),
        vec![(DeltaKind::Plus, 1, 2, 1), (DeltaKind::Plus, 1, 3, 2)]
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
            ref idempotency_key
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
            start_epoch: 3,
            end_epoch: 3,
            inserted: 1,
            already_existed: 1
        }
    );
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 3);
}

#[tokio::test]
async fn trusted_bulk_append_uses_batch_delta_log_and_survives_rollup_gc() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/trusted-bulk-append", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    let result = shard
        .bulk_append_edges_trusted(
            cell_id,
            edge_type,
            [(1, 2), (1, 3), (2, 4), (1, 2)],
            "trusted-bulk-1",
        )
        .await
        .unwrap();
    let retry = shard
        .bulk_append_edges_trusted(
            cell_id,
            edge_type,
            [(1, 2), (1, 3), (2, 4)],
            "trusted-bulk-1",
        )
        .await
        .unwrap();

    assert_eq!(
        result,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 3,
            inserted: 3,
            already_existed: 0,
        }
    );
    assert_eq!(retry, result);

    let mut per_edge_outbox = shard
        .scan_remote_prefix(&keys::outbox_prefix(cell_id))
        .await
        .unwrap();
    assert!(per_edge_outbox.next().await.unwrap().is_none());

    let mut batch_outbox = shard
        .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
        .await
        .unwrap();
    assert!(batch_outbox.next().await.unwrap().is_some());
    assert!(batch_outbox.next().await.unwrap().is_none());

    assert_eq!(
        shard
            .deltas_since(cell_id, edge_type, 0)
            .await
            .unwrap()
            .iter()
            .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
            .collect::<Vec<_>>(),
        vec![
            (DeltaKind::Plus, 1, 2, 1),
            (DeltaKind::Plus, 1, 3, 2),
            (DeltaKind::Plus, 2, 4, 3),
        ]
    );
    assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 3);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        shard.edges_at(cell_id, edge_type, 2).await.unwrap(),
        vec![
            EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: 1,
                dst: 2,
                epoch: 1,
            },
            EdgeRecord {
                cell_id: cell_id.to_string(),
                edge_type: edge_type.to_string(),
                src: 1,
                dst: 3,
                epoch: 2,
            },
        ]
    );

    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);

    shard
        .rollup_artifacts(cell_id, edge_type, result.end_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    let gc = shard
        .delete_deltas_through_rollup(cell_id, edge_type, result.end_epoch)
        .await
        .unwrap();
    assert_eq!(gc.deleted_delta_keys, 1);
    assert!(matches!(
        shard.deltas_since(cell_id, edge_type, 0).await.unwrap_err(),
        GraphError::SnapshotExpired { min_epoch: 3, .. }
    ));
    assert!(shard.outbox_since(cell_id, 0).await.unwrap().is_empty());
    let mut batch_outbox = shard
        .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
        .await
        .unwrap();
    assert!(batch_outbox.next().await.unwrap().is_none());

    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn outbound_only_index_policy_skips_reverse_rows_with_read_fallback() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/outbound-only-index",
        object_store,
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = shard
        .bulk_import_edges(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            [(1, 2), (1, 3), (4, 2)],
            "bulk-outbound-only",
        )
        .await
        .unwrap();

    assert_eq!(result.inserted, 3);
    assert_eq!(shard.graph_index_policy(), GraphIndexPolicy::OutboundOnly);
    assert_eq!(
        shard
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        shard
            .in_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2)
            .await
            .unwrap(),
        vec![1, 4]
    );
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        2
    );

    let mut reverse_edges = shard
        .scan_remote_prefix(&keys::in_edge_type_prefix(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
        ))
        .await
        .unwrap();
    assert!(reverse_edges.next().await.unwrap().is_none());

    let mut reverse_degrees = shard
        .scan_remote_prefix(&keys::degree_in_prefix(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
        ))
        .await
        .unwrap();
    assert!(reverse_degrees.next().await.unwrap().is_none());

    let report = shard
        .verify_current_graph("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 2, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    assert_eq!(report.in_index_edges, 0);
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
            end_epoch: 5,
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
async fn trusted_chunked_bulk_append_uses_bounded_batch_delta_logs() {
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
            end_epoch: 5,
            inserted: 5,
            already_existed: 0
        }
    );
    assert_eq!(retry, result);
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 5);
    assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 5);

    let mut per_edge_outbox = shard
        .scan_remote_prefix(&keys::outbox_prefix(cell_id))
        .await
        .unwrap();
    assert!(per_edge_outbox.next().await.unwrap().is_none());

    let mut batch_outbox = shard
        .scan_remote_prefix(&keys::outbox_batch_prefix(cell_id))
        .await
        .unwrap();
    let mut batch_records = 0;
    while batch_outbox.next().await.unwrap().is_some() {
        batch_records += 1;
    }
    assert_eq!(batch_records, 3);

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
            start_epoch: 6,
            end_epoch: 6,
            inserted: 1,
            already_existed: 1
        }
    );
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 6);
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
    assert_eq!(chunked_overlap.end_epoch, 7);
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 7);
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 7);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4, 5, 6, 7, 8]
    );
}

#[tokio::test]
async fn trusted_supernode_segment_append_skips_canonical_rows_and_survives_rollup_gc() {
    let full_index_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let full_index = open_test_shard("graph/segment-full-index-reject", full_index_store).await;
    let rejected = full_index
        .bulk_append_supernode_segment_trusted(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            1,
            [2],
            "segment-reject",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        rejected,
        GraphError::UnsupportedQuery {
            dialect: "GraphWrite",
            ..
        }
    ));

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/trusted-supernode-segment",
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

    let result = shard
        .bulk_append_supernode_segment_trusted(
            cell_id,
            edge_type,
            1,
            [4, 2, 3, 2],
            "trusted-segment-1",
        )
        .await
        .unwrap();
    let retry = shard
        .bulk_append_supernode_segment_trusted(
            cell_id,
            edge_type,
            1,
            [2, 3, 4],
            "trusted-segment-1",
        )
        .await
        .unwrap();

    assert_eq!(
        result,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 3,
            inserted: 3,
            already_existed: 0
        }
    );
    assert_eq!(retry, result);
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 3);
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
    assert!(shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4]
    );
    assert_eq!(
        shard.in_neighbors(cell_id, edge_type, 2).await.unwrap(),
        vec![1]
    );
    assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 3);
    assert_eq!(
        shard
            .deltas_since(cell_id, edge_type, 0)
            .await
            .unwrap()
            .iter()
            .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
            .collect::<Vec<_>>(),
        vec![
            (DeltaKind::Plus, 1, 2, 1),
            (DeltaKind::Plus, 1, 3, 2),
            (DeltaKind::Plus, 1, 4, 3),
        ]
    );

    let mut canonical = shard
        .scan_remote_prefix(&keys::out_edge_type_prefix(cell_id, edge_type))
        .await
        .unwrap();
    assert!(canonical.next().await.unwrap().is_none());
    let mut segments = shard
        .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
        .await
        .unwrap();
    assert!(segments.next().await.unwrap().is_some());
    assert!(segments.next().await.unwrap().is_none());

    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    assert_eq!(report.canonical_edges, 0);
    assert_eq!(report.out_index_edges, 3);

    let delete = shard
        .delete_edge(EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src: 1,
            dst: 3,
            idempotency_key: "trusted-segment-delete-1".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(
        delete,
        DeleteResult {
            epoch: 4,
            deleted: true
        }
    );
    let delete_retry = shard
        .delete_edge(EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src: 1,
            dst: 3,
            idempotency_key: "trusted-segment-delete-1".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(delete_retry, delete);
    assert!(!shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 4]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 2);
    assert_eq!(
        shard
            .out_neighbors_at(cell_id, edge_type, 1, result.end_epoch)
            .await
            .unwrap(),
        vec![2, 3, 4]
    );
    assert_eq!(shard.outbox_since(cell_id, 0).await.unwrap().len(), 4);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    assert_eq!(report.canonical_edges, 0);
    assert_eq!(report.out_index_edges, 2);

    shard
        .rollup_artifacts(cell_id, edge_type, delete.epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    let gc = shard
        .delete_deltas_through_rollup(cell_id, edge_type, delete.epoch)
        .await
        .unwrap();
    assert_eq!(gc.deleted_delta_keys, 2);
    assert!(shard.outbox_since(cell_id, 0).await.unwrap().is_empty());
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 4]
    );

    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn segment_compaction_merges_segments_and_gcs_tombstones_after_rollup() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-compaction",
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
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [2, 3], "compact-segment-1")
        .await
        .unwrap();
    let second = shard
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [4, 5], "compact-segment-2")
        .await
        .unwrap();
    assert_eq!(first.end_epoch, 2);
    assert_eq!(second.end_epoch, 4);

    let delete = shard
        .delete_edge(EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            src: 1,
            dst: 3,
            idempotency_key: "compact-delete-3".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(delete.epoch, 5);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 4, 5]
    );

    shard
        .rollup_artifacts(cell_id, edge_type, delete.epoch, 2, 2, 1, 2)
        .await
        .unwrap();

    let mut segments = shard
        .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
        .await
        .unwrap();
    let mut segment_count = 0;
    while segments.next().await.unwrap().is_some() {
        segment_count += 1;
    }
    assert_eq!(segment_count, 2);
    let mut tombstones = shard
        .scan_remote_prefix(&keys::out_segment_tombstone_src_prefix(
            cell_id, edge_type, 1,
        ))
        .await
        .unwrap();
    assert!(tombstones.next().await.unwrap().is_some());
    assert!(tombstones.next().await.unwrap().is_none());

    let compact = shard
        .compact_supernode_segments(cell_id, edge_type, 1, delete.epoch, "compact-1")
        .await
        .unwrap();
    assert_eq!(
        compact,
        SegmentCompactionResult {
            compacted_through_epoch: 5,
            source_segments: 2,
            deleted_segment_keys: 2,
            deleted_tombstone_keys: 1,
            input_edges: 4,
            output_edges: 3,
        }
    );
    let retry = shard
        .compact_supernode_segments(cell_id, edge_type, 1, delete.epoch, "compact-1")
        .await
        .unwrap();
    assert_eq!(retry, compact);

    let mut segments = shard
        .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
        .await
        .unwrap();
    let mut compacted_values = Vec::new();
    while let Some(kv) = segments.next().await.unwrap() {
        compacted_values.push(kv.value.to_vec());
    }
    assert_eq!(compacted_values.len(), 1);
    assert!(compacted_values[0].starts_with(b"out_segment2\n"));
    let mut tombstones = shard
        .scan_remote_prefix(&keys::out_segment_tombstone_src_prefix(
            cell_id, edge_type, 1,
        ))
        .await
        .unwrap();
    assert!(tombstones.next().await.unwrap().is_none());

    assert!(!shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 4, 5]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);

    shard
        .delete_deltas_through_rollup(cell_id, edge_type, delete.epoch)
        .await
        .unwrap();
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn segment_compaction_respects_cell_write_lock_before_scanning() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-compaction-cell-lock",
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

    let lock = shard
        .acquire_cell_write_lock(cell_id, "held-by-test-writer")
        .await
        .unwrap();
    let err = shard
        .compact_supernode_segments(cell_id, edge_type, 1, 0, "compact-lock-held")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellWriteConflict {
            operation: "compact_supernode_segments",
            ref cell_id
        } if cell_id == "reddit-home"
    ));
    lock.release().await.unwrap();
}

#[tokio::test]
async fn stale_legacy_cell_write_lock_can_be_reclaimed() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/stale-legacy-cell-write-lock",
        Arc::clone(&object_store),
    )
    .await;
    let cell_id = "reddit-home";
    let path = shard.cell_write_lock_path(cell_id);
    let stale_created_ms = graph_now_millis()
        .saturating_sub(GRAPH_CELL_WRITE_LOCK_TTL_MS)
        .saturating_sub(1);
    let stale_payload = Bytes::from(format!(
            "graph-cell-write-lock-v1\ncell={cell_id}\noperation=crashed-writer\ncreated_ms={stale_created_ms}\n"
        ));
    object_store
        .put_opts(&path, stale_payload.into(), PutMode::Create.into())
        .await
        .unwrap();

    let lock = shard
        .acquire_cell_write_lock(cell_id, "new-writer")
        .await
        .unwrap();
    let current = object_store.get(&path).await.unwrap();
    let current_value = current.bytes().await.unwrap();
    let current_record = decode_cell_write_lock_record(path.as_ref(), &current_value).unwrap();
    assert_eq!(current_record.cell_id, cell_id);
    assert_eq!(current_record.operation, "new-writer");
    assert_eq!(current_record.owner_token, lock.owner_token);
    assert_eq!(current_record.state, CellWriteLockState::Active);
    assert!(current_record.expires_at_ms > graph_now_millis());

    let released_token = lock.owner_token.clone();
    lock.release().await.unwrap();
    let released = object_store.get(&path).await.unwrap();
    let released_value = released.bytes().await.unwrap();
    let released_record = decode_cell_write_lock_record(path.as_ref(), &released_value).unwrap();
    assert_eq!(released_record.owner_token, released_token);
    assert_eq!(released_record.state, CellWriteLockState::Released);
    assert_eq!(released_record.expires_at_ms, 0);

    let next = shard
        .acquire_cell_write_lock(cell_id, "next-writer")
        .await
        .unwrap();
    assert_ne!(next.owner_token, released_token);
    next.release().await.unwrap();
}

#[tokio::test]
async fn stale_owner_release_does_not_remove_reclaimed_cell_write_lock() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/stale-owner-release-cell-write-lock",
        Arc::clone(&object_store),
    )
    .await;
    let cell_id = "reddit-home";
    let path = shard.cell_write_lock_path(cell_id);
    let stale_owner = shard
        .acquire_cell_write_lock(cell_id, "slow-writer")
        .await
        .unwrap();
    let stale_created_ms = graph_now_millis()
        .saturating_sub(GRAPH_CELL_WRITE_LOCK_TTL_MS)
        .saturating_sub(1);
    let stale_payload = encode_cell_write_lock_record(
        cell_id,
        "slow-writer",
        &stale_owner.owner_token,
        stale_created_ms,
        stale_created_ms,
        CellWriteLockState::Active,
    );
    object_store
        .put_opts(&path, stale_payload.into(), PutMode::Overwrite.into())
        .await
        .unwrap();

    let reclaimed = shard
        .acquire_cell_write_lock(cell_id, "new-writer")
        .await
        .unwrap();
    assert_ne!(reclaimed.owner_token, stale_owner.owner_token);
    stale_owner.release().await.unwrap();

    let current = object_store.get(&path).await.unwrap();
    let current_value = current.bytes().await.unwrap();
    let current_record = decode_cell_write_lock_record(path.as_ref(), &current_value).unwrap();
    assert_eq!(current_record.owner_token, reclaimed.owner_token);
    assert_eq!(current_record.state, CellWriteLockState::Active);
    reclaimed.release().await.unwrap();
}

#[tokio::test]
async fn cell_write_lock_renew_extends_owner_expiry() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/cell-write-lock-renew-extends-expiry",
        Arc::clone(&object_store),
    )
    .await;
    let cell_id = "reddit-home";
    let path = shard.cell_write_lock_path(cell_id);
    let lock = shard
        .acquire_cell_write_lock(cell_id, "long-maintenance")
        .await
        .unwrap();
    let stale_created_ms = graph_now_millis()
        .saturating_sub(GRAPH_CELL_WRITE_LOCK_TTL_MS)
        .saturating_sub(1);
    let stale_payload = encode_cell_write_lock_record(
        cell_id,
        "long-maintenance",
        &lock.owner_token,
        stale_created_ms,
        stale_created_ms,
        CellWriteLockState::Active,
    );
    object_store
        .put_opts(&path, stale_payload.into(), PutMode::Overwrite.into())
        .await
        .unwrap();

    lock.renew().await.unwrap();
    let current = object_store.get(&path).await.unwrap();
    let current_value = current.bytes().await.unwrap();
    let current_record = decode_cell_write_lock_record(path.as_ref(), &current_value).unwrap();
    assert_eq!(current_record.owner_token, lock.owner_token);
    assert_eq!(current_record.state, CellWriteLockState::Active);
    assert!(current_record.expires_at_ms > graph_now_millis());

    let err = match shard
        .acquire_cell_write_lock(cell_id, "contending-writer")
        .await
    {
        Ok(_) => panic!("contending writer acquired renewed cell write lock"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        GraphError::CellWriteConflict {
            operation: "contending-writer",
            ref cell_id
        } if cell_id == "reddit-home"
    ));
    lock.release().await.unwrap();
}

#[tokio::test]
async fn stale_owner_cannot_renew_reclaimed_cell_write_lock() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/stale-owner-renew-cell-write-lock",
        Arc::clone(&object_store),
    )
    .await;
    let cell_id = "reddit-home";
    let path = shard.cell_write_lock_path(cell_id);
    let stale_owner = shard
        .acquire_cell_write_lock(cell_id, "slow-writer")
        .await
        .unwrap();
    let replacement_payload = encode_cell_write_lock_record(
        cell_id,
        "new-writer",
        "replacement-owner-token",
        graph_now_millis(),
        graph_now_millis().saturating_add(GRAPH_CELL_WRITE_LOCK_TTL_MS),
        CellWriteLockState::Active,
    );
    object_store
        .put_opts(&path, replacement_payload.into(), PutMode::Overwrite.into())
        .await
        .unwrap();

    let err = stale_owner.renew().await.unwrap_err();
    assert!(matches!(
        err,
        GraphError::CellWriteConflict {
            operation: "renew_cell_write_lock",
            ref cell_id
        } if cell_id == "reddit-home"
    ));
    stale_owner.release().await.unwrap();
    let current = object_store.get(&path).await.unwrap();
    let current_value = current.bytes().await.unwrap();
    let current_record = decode_cell_write_lock_record(path.as_ref(), &current_value).unwrap();
    assert_eq!(current_record.owner_token, "replacement-owner-token");
    assert_eq!(current_record.state, CellWriteLockState::Active);
}

#[tokio::test]
async fn matrix_artifact_write_lock_is_epoch_scoped_and_cell_lock_independent() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/matrix-artifact-write-lock-scope",
        Arc::clone(&object_store),
    )
    .await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    let base_epoch = 7;

    let artifact_lock = shard
        .acquire_matrix_artifact_write_lock(cell_id, edge_type, base_epoch, "build_matrix_tiles")
        .await
        .unwrap();
    let same_epoch_err = match shard
        .acquire_matrix_artifact_write_lock(cell_id, edge_type, base_epoch, "contending-builder")
        .await
    {
        Ok(_) => panic!("contending builder acquired matrix artifact write lock"),
        Err(err) => err,
    };
    assert!(matches!(
        same_epoch_err,
        GraphError::CellWriteConflict {
            operation: "contending-builder",
            ref cell_id
        } if cell_id == "reddit-home"
    ));

    let next_epoch_lock = shard
        .acquire_matrix_artifact_write_lock(
            cell_id,
            edge_type,
            base_epoch + 1,
            "different-epoch-builder",
        )
        .await
        .unwrap();
    let different_edge_type_lock = shard
        .acquire_matrix_artifact_write_lock(
            cell_id,
            "OTHER_EDGE_TYPE",
            base_epoch,
            "different-edge-builder",
        )
        .await
        .unwrap();
    let cell_lock = shard
        .acquire_cell_write_lock(cell_id, "ordinary-cell-writer")
        .await
        .unwrap();

    cell_lock.release().await.unwrap();
    different_edge_type_lock.release().await.unwrap();
    next_epoch_lock.release().await.unwrap();
    artifact_lock.release().await.unwrap();
}

#[tokio::test]
async fn posting_artifact_write_lock_is_epoch_scoped_and_matrix_independent() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard(
        "graph/posting-artifact-write-lock-scope",
        Arc::clone(&object_store),
    )
    .await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    let base_epoch = 9;

    let posting_lock = shard
        .acquire_posting_artifact_write_lock(cell_id, edge_type, base_epoch, "build_posting_chunks")
        .await
        .unwrap();
    let same_epoch_err = match shard
        .acquire_posting_artifact_write_lock(
            cell_id,
            edge_type,
            base_epoch,
            "contending-posting-builder",
        )
        .await
    {
        Ok(_) => panic!("contending builder acquired posting artifact write lock"),
        Err(err) => err,
    };
    assert!(matches!(
        same_epoch_err,
        GraphError::CellWriteConflict {
            operation: "contending-posting-builder",
            ref cell_id
        } if cell_id == "reddit-home"
    ));

    let matrix_lock = shard
        .acquire_matrix_artifact_write_lock(cell_id, edge_type, base_epoch, "matrix-builder")
        .await
        .unwrap();
    let next_epoch_lock = shard
        .acquire_posting_artifact_write_lock(
            cell_id,
            edge_type,
            base_epoch + 1,
            "different-posting-epoch",
        )
        .await
        .unwrap();

    next_epoch_lock.release().await.unwrap();
    matrix_lock.release().await.unwrap();
    posting_lock.release().await.unwrap();
}

#[tokio::test]
async fn trusted_segment_append_replay_with_new_job_id_does_not_double_count_degree() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-replay-fingerprint",
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
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [2, 3, 4], "segment-job-a")
        .await
        .unwrap();
    let replay = shard
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [2, 3, 4], "segment-job-b")
        .await
        .unwrap();

    assert_eq!(replay, first);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
    let mut segments = shard
        .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
        .await
        .unwrap();
    let mut segment_count = 0;
    while segments.next().await.unwrap().is_some() {
        segment_count += 1;
    }
    assert_eq!(segment_count, 1);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn trusted_segment_append_filters_partial_overlap_without_degree_drift() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-partial-overlap",
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
        .bulk_append_supernode_segment_trusted(
            cell_id,
            edge_type,
            1,
            [2, 3, 4],
            "segment-overlap-a",
        )
        .await
        .unwrap();
    let second = shard
        .bulk_append_supernode_segment_trusted(
            cell_id,
            edge_type,
            1,
            [3, 4, 5],
            "segment-overlap-b",
        )
        .await
        .unwrap();

    assert_eq!(
        first,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 3,
            inserted: 3,
            already_existed: 0,
        }
    );
    assert_eq!(
        second,
        BulkImportResult {
            start_epoch: 4,
            end_epoch: 4,
            inserted: 1,
            already_existed: 2,
        }
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
async fn bulk_import_treats_segment_edges_as_existing_without_degree_drift() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-then-bulk-overlap",
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

    let segment = shard
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [2, 3], "segment-seed")
        .await
        .unwrap();
    assert_eq!(
        segment,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 2,
            inserted: 2,
            already_existed: 0,
        }
    );

    let bulk = shard
        .bulk_import_edges(cell_id, edge_type, [(1, 2), (1, 4)], "bulk-overlap-segment")
        .await
        .unwrap();
    assert_eq!(
        bulk,
        BulkImportResult {
            start_epoch: 3,
            end_epoch: 3,
            inserted: 1,
            already_existed: 1,
        }
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn write_edge_treats_segment_edges_as_existing_without_degree_drift() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-then-single-write-overlap",
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

    let segment = shard
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [2, 3], "segment-seed")
        .await
        .unwrap();
    assert_eq!(
        segment,
        BulkImportResult {
            start_epoch: 1,
            end_epoch: 2,
            inserted: 2,
            already_existed: 0,
        }
    );

    let duplicate = shard
        .write_edge(mutation(1, 2, "single-overlap-segment"))
        .await
        .unwrap();
    assert_eq!(
        duplicate,
        CommitResult {
            epoch: 1,
            already_existed: true,
        }
    );
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 2);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 2);

    let fresh = shard
        .write_edge(mutation(1, 4, "single-new-after-segment"))
        .await
        .unwrap();
    assert_eq!(
        fresh,
        CommitResult {
            epoch: 3,
            already_existed: false,
        }
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn write_edge_reinserts_deleted_segment_edge_without_stale_duplicate_check() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-delete-then-single-write",
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
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [2], "segment-seed")
        .await
        .unwrap();
    let deleted = shard
        .delete_edge(mutation(1, 2, "delete-segment-before-reinsert"))
        .await
        .unwrap();
    assert_eq!(
        deleted,
        DeleteResult {
            epoch: 2,
            deleted: true,
        }
    );

    let reinserted = shard
        .write_edge(mutation(1, 2, "reinsert-after-segment-delete"))
        .await
        .unwrap();
    assert_eq!(
        reinserted,
        CommitResult {
            epoch: 3,
            already_existed: false,
        }
    );
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 3);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 1);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 2, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
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
                end_epoch: 2,
                inserted: 2,
                already_existed: 0,
            },
            BulkImportResult {
                start_epoch: 3,
                end_epoch: 4,
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
async fn segment_compaction_preserves_segments_after_compacted_epoch() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-compaction-boundary",
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
        .bulk_append_supernode_segment_trusted(
            cell_id,
            edge_type,
            1,
            [2, 3],
            "boundary-segment-old",
        )
        .await
        .unwrap();
    let rollup_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, rollup_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    shard
        .bulk_append_supernode_segment_trusted(
            cell_id,
            edge_type,
            1,
            [4, 5],
            "boundary-segment-new",
        )
        .await
        .unwrap();

    let compact = shard
        .compact_supernode_segments(cell_id, edge_type, 1, rollup_epoch, "boundary-compact")
        .await
        .unwrap();
    assert_eq!(
        compact,
        SegmentCompactionResult {
            compacted_through_epoch: rollup_epoch,
            source_segments: 1,
            deleted_segment_keys: 1,
            deleted_tombstone_keys: 0,
            input_edges: 2,
            output_edges: 2,
        }
    );
    assert_eq!(
        shard
            .out_neighbors_at(cell_id, edge_type, 1, rollup_epoch)
            .await
            .unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4, 5]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 4);

    let mut segments = shard
        .scan_remote_prefix(&keys::out_segment_src_prefix(cell_id, edge_type, 1))
        .await
        .unwrap();
    let mut segment_count = 0;
    while segments.next().await.unwrap().is_some() {
        segment_count += 1;
    }
    assert_eq!(segment_count, 2);
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
async fn write_edges_batch_uses_one_batch_idempotency_and_logs_batch_boundary() {
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
            end_epoch: 3,
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

    let mut iter = shard
        .scan_remote_prefix("cell/reddit-home/mutation_batch/")
        .await
        .unwrap();
    let mut logs = Vec::new();
    while let Some(kv) = iter.next().await.unwrap() {
        logs.push((
            String::from_utf8_lossy(&kv.key).into_owned(),
            String::from_utf8_lossy(&kv.value).into_owned(),
        ));
    }
    assert_eq!(logs.len(), 1);
    assert!(logs[0].0.ends_with("/batch-create-1"));
    assert!(logs[0]
        .1
        .starts_with("mutation_batch1\tUSER_SUBSCRIBED_TO_SUBREDDIT\t1\t3\t3\t0\t"));
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
    assert_eq!(result.end_epoch, 2);
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
                epoch: 2,
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
    assert_eq!(
        shard
            .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
            .await
            .unwrap()
            .iter()
            .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
            .collect::<Vec<_>>(),
        vec![(DeltaKind::Plus, 7, 10, 1), (DeltaKind::Plus, 7, 11, 2)]
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
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 2);
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
                end_epoch: 2,
                inserted: 2,
                already_existed: 0,
                results: vec![
                    CommitResult {
                        epoch: 1,
                        already_existed: false,
                    },
                    CommitResult {
                        epoch: 2,
                        already_existed: false,
                    },
                ],
            },
            EdgeMutationBatchResult {
                start_epoch: 3,
                end_epoch: 4,
                inserted: 2,
                already_existed: 0,
                results: vec![
                    CommitResult {
                        epoch: 3,
                        already_existed: false,
                    },
                    CommitResult {
                        epoch: 4,
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
            ref idempotency_key
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
            ref idempotency_key
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
            end_epoch: 5,
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
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 5);

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
    assert_eq!(duplicate.end_epoch, 5);
}

#[tokio::test]
async fn ingest_edge_mutations_treats_segment_edges_as_existing_without_degree_drift() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/segment-then-ingest-overlap",
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
        .bulk_append_supernode_segment_trusted(cell_id, edge_type, 1, [2, 3], "segment-ingest-seed")
        .await
        .unwrap();

    let ingest = shard
        .ingest_edge_mutations(
            cell_id,
            [
                mutation(1, 2, "ingest-overlap-segment"),
                mutation(1, 4, "ingest-new-after-segment"),
            ],
            EdgeIngestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        ingest,
        EdgeIngestResult {
            start_epoch: 3,
            end_epoch: 3,
            inserted: 1,
            already_existed: 1,
            batches: 1,
            mutations: 2,
        }
    );
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![2, 3, 4]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 3);
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
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
async fn mutation_log_append_is_durable_and_replayed_after_reopen() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/mutation-log-reopen";
    {
        let writer = open_test_shard(path, Arc::clone(&object_store)).await;
        let result = writer
            .append_edge_mutation_log(
                "reddit-home",
                "log-batch-1",
                [
                    mutation(20, 30, "log-edge-1"),
                    mutation(20, 31, "log-edge-2"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            EdgeMutationLogAppendResult {
                log_epoch: 1,
                mutations: 2,
                already_appended: false
            }
        );
        assert_eq!(writer.current_epoch("reddit-home").await.unwrap(), 0);
        assert!(!writer
            .edge_exists("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 20, 30)
            .await
            .unwrap());
        writer.close().await.unwrap();
    }

    let reopened = open_test_shard(path, Arc::clone(&object_store)).await;
    let materialized = reopened
        .materialize_edge_mutation_log("reddit-home", 16)
        .await
        .unwrap();
    assert_eq!(materialized.scanned_batches, 1);
    assert_eq!(materialized.materialized_batches, 1);
    assert_eq!(materialized.mutations, 2);
    assert_eq!(materialized.inserted, 2);
    assert_eq!(materialized.materialized_log_epoch, 1);
    assert_eq!(materialized.current_epoch, 2);
    assert_eq!(
        reopened
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 20)
            .await
            .unwrap(),
        vec![30, 31]
    );
}

#[tokio::test]
async fn graph_mutation_log_and_outbox_payloads_match_replayed_graph() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/mutation-log-payload-accuracy";
    let cell_id = "reddit-home";
    let edge_type = "WAL_EDGE";
    let first_batch = vec![
        typed_mutation(cell_id, edge_type, 1, 10, "wal-payload-1"),
        typed_mutation(cell_id, edge_type, 1, 11, "wal-payload-2"),
    ];
    let second_batch = vec![
        typed_mutation(cell_id, edge_type, 2, 20, "wal-payload-3"),
        typed_mutation(cell_id, edge_type, 2, 21, "wal-payload-4"),
    ];

    {
        let writer = open_test_shard(path, Arc::clone(&object_store)).await;
        assert_eq!(
            writer
                .append_edge_mutation_log(cell_id, "wal-batch-1", first_batch.clone())
                .await
                .unwrap(),
            EdgeMutationLogAppendResult {
                log_epoch: 1,
                mutations: 2,
                already_appended: false,
            }
        );
        assert_eq!(
            writer
                .append_edge_mutation_log(cell_id, "wal-batch-2", second_batch.clone())
                .await
                .unwrap(),
            EdgeMutationLogAppendResult {
                log_epoch: 2,
                mutations: 2,
                already_appended: false,
            }
        );

        let mut iter = writer
            .scan_remote_prefix(&keys::mutation_log_prefix(cell_id))
            .await
            .unwrap();
        let mut decoded = Vec::new();
        while let Some(kv) = iter.next().await.unwrap() {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            decoded.push((
                parse_mutation_log_epoch(&key).unwrap(),
                decode_edge_mutation_log_batch(&key, &kv.value).unwrap(),
            ));
        }
        decoded.sort_by_key(|(log_epoch, _)| *log_epoch);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, 1);
        assert_eq!(decoded[0].1.cell_id, cell_id);
        assert_eq!(decoded[0].1.batch_id, "wal-batch-1");
        assert_eq!(
            decoded[0].1.fingerprint,
            edge_mutation_log_fingerprint(cell_id, "wal-batch-1", &first_batch)
        );
        assert_eq!(decoded[0].1.mutations, first_batch);
        assert_eq!(decoded[1].0, 2);
        assert_eq!(decoded[1].1.cell_id, cell_id);
        assert_eq!(decoded[1].1.batch_id, "wal-batch-2");
        assert_eq!(
            decoded[1].1.fingerprint,
            edge_mutation_log_fingerprint(cell_id, "wal-batch-2", &second_batch)
        );
        assert_eq!(decoded[1].1.mutations, second_batch);
        assert_eq!(
            writer
                .read_counter(&keys::mutation_log_epoch(cell_id))
                .await
                .unwrap(),
            2
        );
        assert_eq!(writer.current_epoch(cell_id).await.unwrap(), 0);
        assert!(writer.outbox_since(cell_id, 0).await.unwrap().is_empty());
        writer.close().await.unwrap();
    }

    let reopened = open_test_shard(path, Arc::clone(&object_store)).await;
    let replay = reopened
        .materialize_edge_mutation_log(cell_id, 16)
        .await
        .unwrap();
    assert_eq!(
        replay,
        EdgeMutationLogMaterializeResult {
            scanned_batches: 2,
            materialized_batches: 2,
            mutations: 4,
            inserted: 4,
            already_existed: 0,
            last_log_epoch: 2,
            materialized_log_epoch: 2,
            current_epoch: 4,
        }
    );

    let expected_pairs = [(1, 10), (1, 11), (2, 20), (2, 21)];
    let outbox = reopened.outbox_since(cell_id, 0).await.unwrap();
    assert_eq!(outbox.len(), expected_pairs.len());
    for (idx, record) in outbox.iter().enumerate() {
        assert_eq!(record.kind, DeltaKind::Plus);
        assert_eq!(record.edge.cell_id, cell_id);
        assert_eq!(record.edge.edge_type, edge_type);
        assert_eq!(record.edge.epoch, (idx + 1) as u64);
        assert_eq!((record.edge.src, record.edge.dst), expected_pairs[idx]);
    }
    assert_eq!(
        reopened
            .read_counter(&keys::mutation_log_materialized_epoch(cell_id))
            .await
            .unwrap(),
        2
    );

    let live_edges = reopened.edges_at(cell_id, edge_type, 4).await.unwrap();
    assert_eq!(
        live_edges
            .iter()
            .map(|edge| (edge.epoch, edge.src, edge.dst))
            .collect::<Vec<_>>(),
        vec![(1, 1, 10), (2, 1, 11), (3, 2, 20), (4, 2, 21)]
    );
    assert_eq!(
        reopened.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![10, 11]
    );
    assert_eq!(
        reopened.out_neighbors(cell_id, edge_type, 2).await.unwrap(),
        vec![20, 21]
    );
    assert_eq!(reopened.out_degree(cell_id, edge_type, 1).await.unwrap(), 2);
    assert_eq!(reopened.out_degree(cell_id, edge_type, 2).await.unwrap(), 2);

    let replay_again = reopened
        .materialize_edge_mutation_log(cell_id, 16)
        .await
        .unwrap();
    assert_eq!(replay_again.scanned_batches, 0);
    assert_eq!(replay_again.materialized_batches, 0);
    assert_eq!(replay_again.current_epoch, 4);
}

#[tokio::test]
async fn mutation_log_append_is_batch_idempotent_and_detects_conflict() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/mutation-log-idempotency", object_store).await;

    let first = shard
        .append_edge_mutation_log(
            "reddit-home",
            "log-batch-idem",
            [
                mutation(30, 40, "log-idem-1"),
                mutation(30, 41, "log-idem-2"),
            ],
        )
        .await
        .unwrap();
    let retry = shard
        .append_edge_mutation_log(
            "reddit-home",
            "log-batch-idem",
            [
                mutation(30, 40, "log-idem-1"),
                mutation(30, 41, "log-idem-2"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        retry,
        EdgeMutationLogAppendResult {
            already_appended: true,
            ..first
        }
    );

    let conflict = shard
        .append_edge_mutation_log(
            "reddit-home",
            "log-batch-idem",
            [mutation(30, 42, "log-idem-different")],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        GraphError::IdempotencyConflict {
            operation: "mutation-log",
            ref idempotency_key
        } if idempotency_key == "log-batch-idem"
    ));

    let mut iter = shard
        .scan_remote_prefix("cell/reddit-home/mutation_log/")
        .await
        .unwrap();
    let mut logs = 0;
    while iter.next().await.unwrap().is_some() {
        logs += 1;
    }
    assert_eq!(logs, 1);
}

#[tokio::test]
async fn mutation_log_appends_retry_without_log_epoch_overlap() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard =
        Arc::new(open_test_shard("graph/mutation-log-transaction-race", object_store).await);
    let cell_id = "reddit-home";

    let left = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            append_edge_mutation_log_txn_retry_for_test(
                shard,
                cell_id,
                "log-race-a",
                vec![mutation(60, 70, "log-race-edge-a")],
            )
            .await
        })
    };
    let right = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            append_edge_mutation_log_txn_retry_for_test(
                shard,
                cell_id,
                "log-race-b",
                vec![mutation(61, 71, "log-race-edge-b")],
            )
            .await
        })
    };

    let mut results = vec![left.await.unwrap().unwrap(), right.await.unwrap().unwrap()];
    results.sort_by_key(|result| result.log_epoch);
    assert_eq!(
        results,
        vec![
            EdgeMutationLogAppendResult {
                log_epoch: 1,
                mutations: 1,
                already_appended: false,
            },
            EdgeMutationLogAppendResult {
                log_epoch: 2,
                mutations: 1,
                already_appended: false,
            },
        ]
    );
    assert_eq!(
        shard
            .read_counter(&keys::mutation_log_epoch(cell_id))
            .await
            .unwrap(),
        2
    );

    let materialized = shard
        .materialize_edge_mutation_log(cell_id, 16)
        .await
        .unwrap();
    assert_eq!(materialized.scanned_batches, 2);
    assert_eq!(materialized.materialized_batches, 2);
    assert_eq!(materialized.mutations, 2);
    assert_eq!(materialized.materialized_log_epoch, 2);
    assert_eq!(materialized.current_epoch, 2);
    assert!(shard
        .edge_exists(cell_id, "USER_SUBSCRIBED_TO_SUBREDDIT", 60, 70)
        .await
        .unwrap());
    assert!(shard
        .edge_exists(cell_id, "USER_SUBSCRIBED_TO_SUBREDDIT", 61, 71)
        .await
        .unwrap());
}

#[tokio::test]
async fn mutation_log_materializer_replay_is_idempotent_if_watermark_is_lost() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/mutation-log-replay-idempotent", object_store).await;

    shard
        .append_edge_mutation_log(
            "reddit-home",
            "log-batch-watermark",
            [
                mutation(40, 50, "log-watermark-1"),
                mutation(40, 51, "log-watermark-2"),
            ],
        )
        .await
        .unwrap();
    let first = shard
        .materialize_edge_mutation_log("reddit-home", 16)
        .await
        .unwrap();
    assert_eq!(first.inserted, 2);
    assert_eq!(first.current_epoch, 2);

    let mut batch = WriteBatch::new();
    batch.put(
        keys::mutation_log_materialized_epoch("reddit-home"),
        encode_u64(0),
    );
    shard.write_strict_for_test(batch).await.unwrap();

    let replay = shard
        .materialize_edge_mutation_log("reddit-home", 16)
        .await
        .unwrap();
    assert_eq!(replay.materialized_batches, 1);
    assert_eq!(replay.current_epoch, 2);
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 2);
    assert_eq!(
        shard
            .out_degree("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 40)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn mutation_log_materializer_uses_bounded_microdrains() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/mutation-log-bounded-drain", object_store).await;

    for batch in 0..2_u64 {
        let mutations = (0..400_u64)
            .map(|index| {
                mutation(
                    50 + batch,
                    10_000 + (batch * 1_000) + index,
                    &format!("log-bounded-{batch}-{index}"),
                )
            })
            .collect::<Vec<_>>();
        shard
            .append_edge_mutation_log(
                "reddit-home",
                &format!("log-bounded-batch-{batch}"),
                mutations,
            )
            .await
            .unwrap();
    }

    let first = shard
        .materialize_edge_mutation_log("reddit-home", 2)
        .await
        .unwrap();
    assert_eq!(first.scanned_batches, 2);
    assert_eq!(first.materialized_batches, 2);
    assert_eq!(first.mutations, 800);
    assert_eq!(first.materialized_log_epoch, 2);
    assert_eq!(first.last_log_epoch, 2);
    assert_eq!(first.current_epoch, 800);

    let second = shard
        .materialize_edge_mutation_log("reddit-home", 2)
        .await
        .unwrap();
    assert_eq!(second.scanned_batches, 0);
    assert_eq!(second.materialized_batches, 0);
    assert_eq!(second.mutations, 0);
    assert_eq!(second.materialized_log_epoch, 2);
    assert_eq!(second.current_epoch, 800);
    assert_eq!(shard.current_epoch("reddit-home").await.unwrap(), 800);
}

#[test]
fn bulk_import_chunk_order_spreads_layered_supernode_edges() {
    let mut edges: Vec<_> = (0..1_000_u64)
        .flat_map(|index| {
            (1..=12_u64).scan(1_u64, move |src, hop| {
                let dst = hop * 1_000_000 + index + 1;
                let edge = (*src, dst);
                *src = dst;
                Some(edge)
            })
        })
        .collect();
    edges.sort_unstable_by_key(|(src, dst)| (bulk_import_chunk_order(*src, *dst), *src, *dst));

    let root_edges_in_first_chunk = edges
        .iter()
        .take(1_000)
        .filter(|(src, _)| *src == 1)
        .count();
    assert!(
        root_edges_in_first_chunk < 250,
        "deterministic chunk order concentrated {root_edges_in_first_chunk} root edges"
    );
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
            ref idempotency_key
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
            ref idempotency_key
        } if idempotency_key == "delete-conflict"
    ));
}

#[tokio::test]
async fn delete_edge_publishes_delta_minus_and_snapshot_reads_stay_correct() {
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
            .out_neighbors_at("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 2)
            .await
            .unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        shard
            .out_neighbors_at("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1, 3)
            .await
            .unwrap(),
        vec![3]
    );

    let deltas = shard
        .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
        .await
        .unwrap();
    assert_eq!(
        deltas.iter().map(|delta| delta.kind).collect::<Vec<_>>(),
        vec![DeltaKind::Plus, DeltaKind::Plus, DeltaKind::Minus]
    );
    assert_eq!(
        shard
            .outbox_since("reddit-home", 0)
            .await
            .unwrap()
            .iter()
            .map(|delta| delta.kind)
            .collect::<Vec<_>>(),
        vec![DeltaKind::Plus, DeltaKind::Plus, DeltaKind::Minus]
    );
}

#[tokio::test]
async fn delete_edges_batch_publishes_delta_minus_and_replays_idempotently() {
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
    assert_eq!(result.start_epoch, 4);
    assert_eq!(result.end_epoch, 5);
    assert_eq!(result.deleted, 2);
    assert_eq!(result.already_deleted, 1);
    assert_eq!(
        result.results,
        vec![
            DeleteResult {
                epoch: 4,
                deleted: true,
            },
            DeleteResult {
                epoch: 5,
                deleted: true,
            },
            DeleteResult {
                epoch: 3,
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
    assert_eq!(
        shard
            .out_neighbors_at(cell_id, edge_type, 1, 3)
            .await
            .unwrap(),
        vec![2, 3, 4]
    );

    let deltas = shard
        .deltas_since(cell_id, edge_type, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
        .collect::<Vec<_>>();
    assert_eq!(
        deltas,
        vec![
            (DeltaKind::Plus, 1, 2, 1),
            (DeltaKind::Plus, 1, 3, 2),
            (DeltaKind::Plus, 1, 4, 3),
            (DeltaKind::Minus, 1, 2, 4),
            (DeltaKind::Minus, 1, 4, 5),
        ]
    );
    assert_eq!(
        shard
            .outbox_since(cell_id, 3)
            .await
            .unwrap()
            .into_iter()
            .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
            .collect::<Vec<_>>(),
        vec![(DeltaKind::Minus, 1, 2, 4), (DeltaKind::Minus, 1, 4, 5),]
    );
    let mut legacy_iter = shard
        .scan_remote_prefix(&keys::outbox_prefix(cell_id))
        .await
        .unwrap();
    let mut legacy_minus = Vec::new();
    while let Some(kv) = legacy_iter.next().await.unwrap() {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let delta = decode_delta_record(&key, &kv.value).unwrap();
        if delta.kind == DeltaKind::Minus && delta.edge.epoch > 3 {
            legacy_minus.push((delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch));
        }
    }
    assert_eq!(
        legacy_minus,
        vec![(DeltaKind::Minus, 1, 2, 4), (DeltaKind::Minus, 1, 4, 5)]
    );

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
    assert_eq!(shard.current_epoch(cell_id).await.unwrap(), 5);

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
            idempotency_key
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
async fn delete_edges_batch_tombstones_segment_edges_without_degree_drift() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/delete-segment-edges-batch",
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
        .bulk_append_supernode_segment_trusted(
            cell_id,
            edge_type,
            1,
            [2, 3, 4],
            "segment-delete-batch-seed",
        )
        .await
        .unwrap();
    let result = shard
        .delete_edges_batch(
            cell_id,
            edge_type,
            [(1, 2), (1, 4), (1, 8)],
            "segment-delete-batch-1",
        )
        .await
        .unwrap();

    assert_eq!(result.start_epoch, 4);
    assert_eq!(result.end_epoch, 5);
    assert_eq!(result.deleted, 2);
    assert_eq!(result.already_deleted, 1);
    assert_eq!(
        shard.out_neighbors(cell_id, edge_type, 1).await.unwrap(),
        vec![3]
    );
    assert_eq!(shard.out_degree(cell_id, edge_type, 1).await.unwrap(), 1);
    assert!(!shard.edge_exists(cell_id, edge_type, 1, 2).await.unwrap());
    assert!(shard.edge_exists(cell_id, edge_type, 1, 3).await.unwrap());
    assert!(!shard.edge_exists(cell_id, edge_type, 1, 4).await.unwrap());
    assert_eq!(
        shard
            .out_neighbors_at(cell_id, edge_type, 1, 3)
            .await
            .unwrap(),
        vec![2, 3, 4]
    );
    assert_eq!(
        shard
            .outbox_since(cell_id, 3)
            .await
            .unwrap()
            .into_iter()
            .map(|delta| (delta.kind, delta.edge.src, delta.edge.dst, delta.edge.epoch))
            .collect::<Vec<_>>(),
        vec![(DeltaKind::Minus, 1, 2, 4), (DeltaKind::Minus, 1, 4, 5),]
    );
    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
}

#[tokio::test]
async fn snapshot_api_pins_epoch_across_deletes_and_artifact_rebuilds() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/snapshot-api", object_store).await;
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    shard.write_edge(mutation(1, 2, "create-1")).await.unwrap();
    let epoch_one = shard.snapshot("reddit-home").await.unwrap();

    shard.write_edge(mutation(1, 3, "create-2")).await.unwrap();
    let epoch_two = shard.snapshot("reddit-home").await.unwrap();
    shard
        .build_matrix_tiles("reddit-home", edge_type, epoch_two.read_epoch(), 2)
        .await
        .unwrap();
    shard
        .build_supernode_groups("reddit-home", edge_type, epoch_two.read_epoch(), 2, 2)
        .await
        .unwrap();

    shard.delete_edge(mutation(1, 2, "delete-1")).await.unwrap();
    let latest = shard.snapshot("reddit-home").await.unwrap();

    assert_eq!(epoch_one.read_epoch(), 1);
    assert_eq!(
        epoch_one.out_neighbors(edge_type, 1).await.unwrap(),
        vec![2]
    );
    assert!(epoch_one.edge_exists(edge_type, 1, 2).await.unwrap());
    assert_eq!(
        epoch_one
            .matrix_reachable(edge_type, &[1], 1)
            .await
            .unwrap()
            .vertices,
        vec![2]
    );

    assert_eq!(
        epoch_two.out_neighbors(edge_type, 1).await.unwrap(),
        vec![2, 3]
    );
    assert_eq!(epoch_two.supernode_degree(edge_type, 1).await.unwrap(), 2);
    assert!(epoch_two
        .supernode_edge_exists(edge_type, 1, 2)
        .await
        .unwrap());

    assert_eq!(latest.read_epoch(), 3);
    assert_eq!(latest.out_neighbors(edge_type, 1).await.unwrap(), vec![3]);
    assert!(!latest.edge_exists(edge_type, 1, 2).await.unwrap());
    assert_eq!(latest.supernode_degree(edge_type, 1).await.unwrap(), 1);
    assert_eq!(
        latest
            .matrix_reachable(edge_type, &[1], 1)
            .await
            .unwrap()
            .vertices,
        vec![3]
    );
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
    assert_eq!(
        reopened.outbox_since("reddit-home", 0).await.unwrap().len(),
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
async fn leased_writer_requires_installed_data_write_fence() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let lease = ShardLease {
        cell_id: "reddit-home".to_string(),
        owner_node_id: "node-a".to_string(),
        lease_token: 1,
        expires_at_ms: graph_now_millis() + 60_000,
    };
    let leases = Arc::new(RwLock::new(BTreeMap::from([(
        lease.cell_id.clone(),
        lease.clone(),
    )])));
    let shard = GraphShard::open_leased_writer(
        "graph/leased-fence-required",
        Arc::clone(&object_store),
        GraphOpenOptions::default(),
        "node-a".to_string(),
        Arc::clone(&leases),
    )
    .await
    .unwrap();

    let err = shard
        .write_edge(mutation(1, 2, "missing-data-fence"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::WriteRequiresLease {
            operation: "write_edge",
            ref cell_id
        } if cell_id == "reddit-home"
    ));

    shard
        .install_write_fence("reddit-home", &lease)
        .await
        .unwrap();
    shard
        .write_edge(mutation(1, 2, "after-data-fence"))
        .await
        .unwrap();
    shard.close().await.unwrap();
}

#[tokio::test]
async fn newer_data_write_fence_rejects_all_stale_write_classes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    let lease = ShardLease {
        cell_id: cell_id.to_string(),
        owner_node_id: "node-a".to_string(),
        lease_token: 1,
        expires_at_ms: graph_now_millis() + 60_000,
    };
    let leases = Arc::new(RwLock::new(BTreeMap::from([(
        lease.cell_id.clone(),
        lease.clone(),
    )])));
    let shard = GraphShard::open_leased_writer(
        "graph/stale-data-fence",
        Arc::clone(&object_store),
        GraphOpenOptions::default(),
        "node-a".to_string(),
        Arc::clone(&leases),
    )
    .await
    .unwrap();
    shard.install_write_fence(cell_id, &lease).await.unwrap();
    shard.write_edge(mutation(1, 2, "base-1")).await.unwrap();
    let epoch_two = shard.write_edge(mutation(1, 3, "base-2")).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, epoch_two.epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    let epoch_three = shard.write_edge(mutation(1, 4, "base-3")).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, epoch_three.epoch, 2, 2, 1, 2)
        .await
        .unwrap();

    let newer = ShardLease {
        cell_id: cell_id.to_string(),
        owner_node_id: "node-b".to_string(),
        lease_token: 2,
        expires_at_ms: graph_now_millis() + 60_000,
    };
    let mut batch = WriteBatch::new();
    batch.put(
        keys::write_fence(cell_id),
        encode_write_fence(&GraphWriteFence::from(&newer)),
    );
    shard.write_strict_for_test(batch).await.unwrap();

    assert_stale_node_a(
        shard
            .write_edge(mutation(1, 5, "stale-edge"))
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .delete_edge(mutation(1, 2, "stale-delete"))
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .bulk_import_edges(cell_id, edge_type, [(2, 20), (2, 21)], "stale-bulk")
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .build_posting_chunks(cell_id, edge_type, epoch_three.epoch, 2)
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .build_matrix_tiles(cell_id, edge_type, epoch_three.epoch, 2)
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .build_supernode_groups(cell_id, edge_type, epoch_three.epoch, 1, 2)
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .rollup_artifacts(cell_id, edge_type, epoch_three.epoch, 2, 2, 1, 2)
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .delete_graph_artifacts_before(cell_id, edge_type, epoch_three.epoch)
            .await
            .unwrap_err(),
    );
    assert_stale_node_a(
        shard
            .delete_deltas_through_rollup(cell_id, edge_type, epoch_three.epoch)
            .await
            .unwrap_err(),
    );

    assert!(!shard.edge_exists(cell_id, edge_type, 1, 5).await.unwrap());
    assert_eq!(
        shard.delta_gc_watermark(cell_id, edge_type).await.unwrap(),
        0
    );
    shard.close().await.unwrap();
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
async fn routed_cluster_rejects_writes_for_non_owned_cells() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let placement =
        ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-search", "node-b")]).unwrap();
    let cluster =
        RoutedGraphCluster::open_owned("graph-routed-cluster", "node-a", placement, object_store)
            .await
            .unwrap();
    assert_eq!(cluster.local_cells(), vec!["reddit-home"]);

    let unleased = cluster
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
        unleased,
        GraphError::WriteRequiresLease {
            operation: "routed_write",
            ref cell_id
        } if cell_id == "reddit-home"
    ));

    let err = cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-search".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "wrong-owner".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::ShardNotOwned {
            ref cell_id,
            ref owner_node_id,
            ref local_node_id
        } if cell_id == "reddit-search"
            && owner_node_id == "node-b"
            && local_node_id == "node-a"
    ));
    cluster.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_persists_placement_and_enforces_active_leases() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/leases", Arc::clone(&object_store))
        .await
        .unwrap();
    let placement =
        ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-search", "node-b")]).unwrap();
    control.publish_placement(&placement).await.unwrap();

    let mut cluster = RoutedGraphCluster::open_owned_with_control(
        "graph-control-cluster",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    let first_token = cluster.lease("reddit-home").unwrap().lease_token;
    cluster
        .renew_leases(&control, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        cluster.lease("reddit-home").unwrap().lease_token,
        first_token
    );
    cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "leased-write".to_string(),
        })
        .await
        .unwrap();

    let failover = ShardPlacement::fixed([("reddit-home", "node-b")]).unwrap();
    control.publish_placement(&failover).await.unwrap();
    let stale_renewal = cluster
        .renew_leases(&control, std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert!(matches!(
        stale_renewal,
        GraphError::StaleShardLease {
            ref cell_id,
            ref node_id,
            lease_token
        } if cell_id == "reddit-home" && node_id == "node-a" && lease_token == first_token
    ));
    let held = control
        .acquire_lease("reddit-home", "node-b", std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert!(matches!(
        held,
        GraphError::ShardLeaseHeld {
            ref cell_id,
            ref owner_node_id,
            ..
        } if cell_id == "reddit-home" && owner_node_id == "node-a"
    ));
    cluster.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn lease_renewer_extends_owned_shard_leases_in_background() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open("graph-control/renewer", Arc::clone(&object_store))
            .await
            .unwrap(),
    );
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    control.publish_placement(&placement).await.unwrap();
    let cluster = RoutedGraphCluster::open_owned_with_control(
        "graph-renewer-cluster",
        "node-a",
        &control,
        object_store,
        std::time::Duration::from_secs(2),
    )
    .await
    .unwrap();
    let first_expiry = cluster.lease("reddit-home").unwrap().expires_at_ms;
    let handle = cluster
        .start_lease_renewer(
            Arc::clone(&control),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_millis(25),
        )
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(750);
    let mut renewed_expiry = first_expiry;
    while std::time::Instant::now() < deadline {
        renewed_expiry = cluster.lease("reddit-home").unwrap().expires_at_ms;
        if renewed_expiry > first_expiry {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(renewed_expiry > first_expiry);
    handle.stop().await.unwrap();
    cluster.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_metrics_count_lease_renewal_failures() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/renew-metrics", object_store)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let lease = control
        .acquire_lease("reddit-home", "node-a", std::time::Duration::from_millis(5))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let err = control
        .renew_lease(&lease, std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::StaleShardLease {
            ref cell_id,
            ref node_id,
            lease_token: 1
        } if cell_id == "reddit-home" && node_id == "node-a"
    ));

    let metrics = control.graph_control_metrics();
    assert_eq!(metrics.lease_acquire_attempts, 1);
    assert_eq!(metrics.lease_acquire_successes, 1);
    assert_eq!(metrics.lease_renew_attempts, 1);
    assert_eq!(metrics.lease_renew_successes, 0);
    assert_eq!(metrics.lease_renew_failures, 1);
    assert_eq!(metrics.lease_renew_lost, 1);
    control.close().await.unwrap();
}

#[tokio::test]
async fn graph_node_starts_lease_renewal_automatically() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open("graph-control/node-renewer", Arc::clone(&object_store))
            .await
            .unwrap(),
    );
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();

    let node = GraphNode::open(
        "graph-node",
        "node-a",
        Arc::clone(&control),
        object_store,
        std::time::Duration::from_secs(2),
        std::time::Duration::from_millis(25),
    )
    .await
    .unwrap();
    assert_eq!(
        control
            .node_heartbeat("node-a")
            .await
            .unwrap()
            .unwrap()
            .state,
        GraphNodeHealthState::Active
    );
    let draining = node
        .set_health_state(GraphNodeHealthState::Draining)
        .await
        .unwrap();
    assert_eq!(draining.state, GraphNodeHealthState::Draining);
    assert_eq!(
        control
            .node_heartbeat("node-a")
            .await
            .unwrap()
            .unwrap()
            .state,
        GraphNodeHealthState::Draining
    );
    let first_expiry = node.cluster().lease("reddit-home").unwrap().expires_at_ms;
    node.cluster()
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "node-write".to_string(),
        })
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(750);
    let mut renewed_expiry = first_expiry;
    while std::time::Instant::now() < deadline {
        renewed_expiry = node.cluster().lease("reddit-home").unwrap().expires_at_ms;
        if renewed_expiry > first_expiry {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(renewed_expiry > first_expiry);
    node.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn graph_node_close_publishes_draining_heartbeat() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open("graph-control/node-close-drain", Arc::clone(&object_store))
            .await
            .unwrap(),
    );
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();

    let node = GraphNode::open(
        "graph-node-close-drain",
        "node-a",
        Arc::clone(&control),
        object_store,
        std::time::Duration::from_secs(2),
        std::time::Duration::from_millis(25),
    )
    .await
    .unwrap();
    assert_eq!(
        control
            .node_heartbeat("node-a")
            .await
            .unwrap()
            .unwrap()
            .state,
        GraphNodeHealthState::Active
    );

    node.close().await.unwrap();
    let heartbeat = control.node_heartbeat("node-a").await.unwrap().unwrap();
    assert_eq!(heartbeat.state, GraphNodeHealthState::Draining);
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    control
        .publish_node_heartbeat("node-b", GraphNodeHealthState::Active)
        .await
        .unwrap();
    let report = control
        .reconcile_cluster(
            &GraphClusterControllerConfig::discover_existing(
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(report.failed_over_leases.len(), 1);
    assert_eq!(report.failed_over_leases[0].owner_node_id, "node-b");
    assert_eq!(
        control
            .current_lease("reddit-home")
            .await
            .unwrap()
            .unwrap()
            .owner_node_id,
        "node-b"
    );
    control.close().await.unwrap();
}

#[tokio::test]
async fn graph_node_rejects_bad_runtime_config_before_acquiring_lease() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open(
            "graph-control/node-open-validation",
            Arc::clone(&object_store),
        )
        .await
        .unwrap(),
    );
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();

    let err = match GraphNode::open(
        "graph-node-open-validation",
        "node-a",
        Arc::clone(&control),
        Arc::clone(&object_store),
        std::time::Duration::from_secs(2),
        std::time::Duration::ZERO,
    )
    .await
    {
        Ok(_) => panic!("graph node accepted zero lease renewal interval"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        GraphError::CorruptValue {
            ref key,
            ..
        } if key == "control/lease_renew_interval"
    ));
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control.node_heartbeat("node-a").await.unwrap().is_none());

    let err = match GraphNode::open_managed(
        "graph-managed-open-validation",
        "node-a",
        Arc::clone(&control),
        Arc::clone(&object_store),
        std::time::Duration::from_secs(2),
        std::time::Duration::from_millis(25),
        std::time::Duration::ZERO,
    )
    .await
    {
        Ok(_) => panic!("managed graph node accepted zero shard refresh interval"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        GraphError::CorruptValue {
            ref key,
            ..
        } if key == "control/shard_refresh_interval"
    ));
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control.node_heartbeat("node-a").await.unwrap().is_none());
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_can_fail_over_after_lease_expiry() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/failover", object_store)
        .await
        .unwrap();
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    control.publish_placement(&placement).await.unwrap();
    control
        .acquire_lease("reddit-home", "node-a", std::time::Duration::from_millis(5))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let lease = control
        .failover_expired_cell("reddit-home", "node-b", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(lease.owner_node_id, "node-b");
    assert_eq!(
        control
            .load_placement()
            .await
            .unwrap()
            .owner("reddit-home")
            .unwrap(),
        "node-b"
    );
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_release_lease_requires_matching_owner_and_token() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/release-lease", object_store)
        .await
        .unwrap();
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    control.publish_placement(&placement).await.unwrap();
    let lease = control
        .acquire_lease("reddit-home", "node-a", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    let stale = ShardLease {
        lease_token: lease.lease_token + 1,
        ..lease.clone()
    };

    let err = control.release_lease(&stale).await.unwrap_err();
    assert!(matches!(
        err,
        GraphError::StaleShardLease {
            ref cell_id,
            ref node_id,
            lease_token
        } if cell_id == "reddit-home"
            && node_id == "node-a"
            && lease_token == stale.lease_token
    ));
    assert_eq!(
        control.current_lease("reddit-home").await.unwrap().unwrap(),
        lease
    );

    assert!(control.release_lease(&lease).await.unwrap());
    assert!(!control.release_lease(&lease).await.unwrap());
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    let metrics = control.graph_control_metrics();
    assert_eq!(metrics.lease_release_attempts, 3);
    assert_eq!(metrics.lease_release_successes, 1);
    assert_eq!(metrics.lease_release_failures, 1);
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_drop_cell_control_state_removes_discovery_and_watermarks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/drop-cell-state", object_store)
        .await
        .unwrap();
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    control
        .publish_placement_with_catalog(&placement, "reddit", 1)
        .await
        .unwrap();
    let lease = control
        .acquire_lease("reddit-home", "node-a", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    control
        .advance_watermark(
            GraphControlWatermark {
                cell_id: "reddit-home".to_string(),
                durable_epoch: 10,
                safe_read_epoch: 10,
                outbox_epoch: 9,
                artifact_epoch: 8,
                generation: 0,
            },
            None,
        )
        .await
        .unwrap();
    control
        .advance_edge_watermark(
            GraphControlEdgeWatermark {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                durable_epoch: 10,
                safe_read_epoch: 10,
                outbox_epoch: 9,
                artifact_epoch: 8,
                generation: 0,
            },
            None,
        )
        .await
        .unwrap();
    control
        .commit_control_idempotency("reddit-home", "drop-test", "idem-1", b"ok")
        .await
        .unwrap();

    let report = control
        .drop_cell_control_state("reddit-home", Some(&lease))
        .await
        .unwrap();
    assert_eq!(report.cell_id, "reddit-home");
    assert!(report.deleted_control_keys >= 7);
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control
        .current_watermark("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control
        .current_edge_watermark("reddit-home", "FOLLOWS")
        .await
        .unwrap()
        .is_none());
    assert!(control
        .control_idempotency_result("reddit-home", "drop-test", "idem-1")
        .await
        .unwrap()
        .is_none());
    let controller_report = control
        .reconcile_cluster(
            &GraphClusterControllerConfig::discover_existing(
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(controller_report.controlled_cells.is_empty());
    control.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_open_cleans_up_leases_after_partial_start_failure() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/open-cleanup", Arc::clone(&object_store))
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("b-blocked", "node-b")]).unwrap())
        .await
        .unwrap();
    let blocked = control
        .acquire_lease("b-blocked", "node-b", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    control
        .publish_placement(
            &ShardPlacement::fixed([("a-ok", "node-a"), ("b-blocked", "node-a")]).unwrap(),
        )
        .await
        .unwrap();

    let err = match RoutedGraphCluster::open_owned_with_control(
        "graph-open-cleanup",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    {
        Ok(_) => panic!("routed cluster opened despite held second lease"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        GraphError::ShardLeaseHeld {
            ref cell_id,
            ref owner_node_id,
            ..
        } if cell_id == "b-blocked" && owner_node_id == "node-b"
    ));
    assert!(control.current_lease("a-ok").await.unwrap().is_none());
    assert_eq!(
        control.current_lease("b-blocked").await.unwrap().unwrap(),
        blocked
    );
    control.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_refreshes_owned_shards_after_failover() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/refresh-owned", Arc::clone(&object_store))
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let mut cluster_a = RoutedGraphCluster::open_owned_with_control(
        "graph-refresh-owned",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    let mut cluster_b = RoutedGraphCluster::open_owned_with_control(
        "graph-refresh-owned",
        "node-b",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert_eq!(cluster_a.local_cells(), vec!["reddit-home"]);
    assert!(cluster_b.local_cells().is_empty());
    cluster_a
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "refresh-before".to_string(),
        })
        .await
        .unwrap();

    let expired_at = cluster_a.lease("reddit-home").unwrap().expires_at_ms + 1;
    control
        .failover_expired_cell_at(
            "reddit-home",
            "node-b",
            std::time::Duration::from_secs(60),
            expired_at,
        )
        .await
        .unwrap();
    let opened = cluster_b
        .refresh_owned_shards(&control, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(opened.opened_cells, vec!["reddit-home"]);
    assert!(opened.closed_cells.is_empty());
    let closed = cluster_a
        .refresh_owned_shards(&control, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(closed.closed_cells, vec!["reddit-home"]);
    assert!(closed.opened_cells.is_empty());

    cluster_b
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 2,
            dst: 3,
            idempotency_key: "refresh-after".to_string(),
        })
        .await
        .unwrap();
    let err = cluster_a
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 3,
            dst: 4,
            idempotency_key: "refresh-stale".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::ShardNotOwned {
            ref cell_id,
            ref owner_node_id,
            ref local_node_id
        } if cell_id == "reddit-home"
            && owner_node_id == "node-b"
            && local_node_id == "node-a"
    ));
    cluster_a.close().await.unwrap();
    cluster_b.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_refresh_releases_closed_shard_lease_for_handoff() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control =
        GraphControlPlane::open("graph-control/refresh-release", Arc::clone(&object_store))
            .await
            .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let mut cluster_a = RoutedGraphCluster::open_owned_with_control(
        "graph-refresh-release",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_some());

    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-b")]).unwrap())
        .await
        .unwrap();
    let report_a = cluster_a
        .refresh_owned_shards(&control, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(report_a.closed_cells, vec!["reddit-home"]);
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());

    let cluster_b = RoutedGraphCluster::open_owned_with_control(
        "graph-refresh-release",
        "node-b",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert_eq!(
        control
            .current_lease("reddit-home")
            .await
            .unwrap()
            .unwrap()
            .owner_node_id,
        "node-b"
    );
    cluster_a.close().await.unwrap();
    cluster_b.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_refresh_restores_lease_after_retained_fence_failure() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/refresh-retained-fence-failure",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let mut cluster = RoutedGraphCluster::open_owned_with_control(
        "graph-refresh-retained-fence-failure",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    let original = cluster.lease("reddit-home").unwrap();

    let newer = ShardLease {
        cell_id: "reddit-home".to_string(),
        owner_node_id: "node-z".to_string(),
        lease_token: 999,
        expires_at_ms: graph_now_millis() + 60_000,
    };
    let mut batch = WriteBatch::new();
    batch.put(
        keys::write_fence("reddit-home"),
        encode_write_fence(&GraphWriteFence::from(&newer)),
    );
    cluster
        .shard("reddit-home")
        .unwrap()
        .write_strict_for_test(batch)
        .await
        .unwrap();
    control.release_lease(&original).await.unwrap();
    cluster
        .set_local_lease_expiry_for_test("reddit-home", 0)
        .unwrap();

    let err = cluster
        .refresh_owned_shards(&control, std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::StaleShardLease {
            cell_id,
            node_id,
            lease_token: 2,
        } if cell_id == "reddit-home" && node_id == "node-a"
    ));
    assert_eq!(
        cluster.lease("reddit-home").unwrap().lease_token,
        original.lease_token
    );
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    cluster.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn managed_graph_nodes_refresh_shards_in_background_after_failover() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open("graph-control/managed-refresh", Arc::clone(&object_store))
            .await
            .unwrap(),
    );
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let node_a = GraphNode::open_managed(
        "graph-managed-refresh",
        "node-a",
        Arc::clone(&control),
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(20),
    )
    .await
    .unwrap();
    let node_b = GraphNode::open_managed(
        "graph-managed-refresh",
        "node-b",
        Arc::clone(&control),
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(20),
    )
    .await
    .unwrap();
    assert_eq!(node_a.local_cells().await.unwrap(), vec!["reddit-home"]);
    assert!(node_b.local_cells().await.unwrap().is_empty());
    node_a
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "managed-before".to_string(),
        })
        .await
        .unwrap();

    let lease_expires_at = node_a
        .lease("reddit-home")
        .await
        .unwrap()
        .unwrap()
        .expires_at_ms;
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-b")]).unwrap())
        .await
        .unwrap();
    control
        .failover_expired_cell_at(
            "reddit-home",
            "node-b",
            std::time::Duration::from_secs(60),
            lease_expires_at + 1,
        )
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let node_a_cells = node_a.local_cells().await.unwrap();
        let node_b_cells = node_b.local_cells().await.unwrap();
        if node_a_cells.is_empty() && node_b_cells == vec!["reddit-home"] {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "managed nodes did not refresh ownership: node-a={node_a_cells:?} node-b={node_b_cells:?}"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_eq!(
        node_b
            .out_neighbors("reddit-home", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![2]
    );
    node_b
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 2,
            dst: 3,
            idempotency_key: "managed-after".to_string(),
        })
        .await
        .unwrap();
    let err = node_a
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 3,
            dst: 4,
            idempotency_key: "managed-stale".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::ShardNotOwned {
            ref cell_id,
            ref owner_node_id,
            ref local_node_id
        } if cell_id == "reddit-home"
            && owner_node_id == "node-b"
            && local_node_id == "node-a"
    ));
    let metrics_a = node_a.maintenance_metrics();
    let metrics_b = node_b.maintenance_metrics();
    assert!(metrics_a.shard_refresh_closed_cells >= 1);
    assert!(metrics_b.shard_refresh_opened_cells >= 1);
    assert!(metrics_a.shard_refresh_attempts >= metrics_a.shard_refresh_successes);
    assert!(metrics_b.shard_refresh_attempts >= metrics_b.shard_refresh_successes);

    node_a.close().await.unwrap();
    node_b.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn managed_graph_node_allows_concurrent_routed_writes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open(
            "graph-control/managed-concurrent",
            Arc::clone(&object_store),
        )
        .await
        .unwrap(),
    );
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let node = Arc::new(
        GraphNode::open_managed(
            "graph-managed-concurrent",
            "node-a",
            Arc::clone(&control),
            Arc::clone(&object_store),
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(30),
            std::time::Duration::from_millis(50),
        )
        .await
        .unwrap(),
    );

    let left = {
        let node = Arc::clone(&node);
        tokio::spawn(async move {
            node.write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 2,
                idempotency_key: "managed-concurrent-left".to_string(),
            })
            .await
        })
    };
    let right = {
        let node = Arc::clone(&node);
        tokio::spawn(async move {
            node.write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 3,
                idempotency_key: "managed-concurrent-right".to_string(),
            })
            .await
        })
    };
    left.await.unwrap().unwrap();
    right.await.unwrap().unwrap();
    assert_eq!(
        node.out_neighbors("reddit-home", "FOLLOWS", 1)
            .await
            .unwrap(),
        vec![2, 3]
    );

    let node = Arc::try_unwrap(node).unwrap_or_else(|_| panic!("managed node still shared"));
    node.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn managed_graph_node_drop_cell_unregisters_control_state() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open("graph-control/managed-drop-cell", Arc::clone(&object_store))
            .await
            .unwrap(),
    );
    control
        .publish_placement_with_catalog(
            &ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap(),
            "reddit",
            1,
        )
        .await
        .unwrap();
    let node = GraphNode::open_managed(
        "graph-managed-drop-cell",
        "node-a",
        Arc::clone(&control),
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(50),
    )
    .await
    .unwrap();
    node.write_edge(EdgeMutation {
        cell_id: "reddit-home".to_string(),
        edge_type: "FOLLOWS".to_string(),
        src: 1,
        dst: 2,
        idempotency_key: "managed-drop-before".to_string(),
    })
    .await
    .unwrap();

    let dropped = node
        .drop_cell("reddit-home", "managed-drop-cell")
        .await
        .unwrap();
    assert!(!dropped.already_dropped);
    assert!(node.local_cells().await.unwrap().is_empty());
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .is_none());
    control
        .publish_node_heartbeat("node-b", GraphNodeHealthState::Active)
        .await
        .unwrap();
    let report = control
        .reconcile_cluster(
            &GraphClusterControllerConfig::discover_existing(
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(report.controlled_cells.is_empty());
    assert!(report.failed_over_leases.is_empty());
    let err = node
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 2,
            dst: 3,
            idempotency_key: "managed-drop-after".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::UnknownShard { ref cell_id } if cell_id == "reddit-home"
    ));
    node.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_open_cleans_control_state_for_final_dropped_cell() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/final-dropped-cell-recovery",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    control
        .publish_placement_with_catalog(
            &ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap(),
            "reddit",
            1,
        )
        .await
        .unwrap();
    let cluster = RoutedGraphCluster::open_owned_with_control(
        "graph-final-dropped-cell-recovery",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "final-drop-seed".to_string(),
        })
        .await
        .unwrap();
    let original_lease = cluster.lease("reddit-home").unwrap();
    let dropped = cluster
        .drop_cell("reddit-home", "final-drop-data-only")
        .await
        .unwrap();
    assert!(!dropped.already_dropped);
    assert!(control.release_lease(&original_lease).await.unwrap());
    cluster.close().await.unwrap();

    let reopened = RoutedGraphCluster::open_owned_with_control(
        "graph-final-dropped-cell-recovery",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert!(reopened.local_cells().is_empty());
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .is_none());
    reopened.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn routed_cluster_refresh_cleans_control_state_for_retained_final_dropped_cell() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/final-dropped-cell-refresh",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    control
        .publish_placement_with_catalog(
            &ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap(),
            "reddit",
            1,
        )
        .await
        .unwrap();
    let mut cluster = RoutedGraphCluster::open_owned_with_control(
        "graph-final-dropped-cell-refresh",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "final-drop-refresh-seed".to_string(),
        })
        .await
        .unwrap();
    cluster
        .drop_cell("reddit-home", "final-drop-refresh-data-only")
        .await
        .unwrap();

    let report = cluster
        .refresh_owned_shards(&control, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(report.closed_cells, vec!["reddit-home".to_string()]);
    assert!(cluster.local_cells().is_empty());
    assert!(control
        .current_lease("reddit-home")
        .await
        .unwrap()
        .is_none());
    assert!(control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .is_none());
    cluster.close().await.unwrap();
    control.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn managed_graph_node_executes_cypher_rows() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = Arc::new(
        GraphControlPlane::open("graph-control/managed-cypher", Arc::clone(&object_store))
            .await
            .unwrap(),
    );
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let node = GraphNode::open_managed(
        "graph-managed-cypher",
        "node-a",
        Arc::clone(&control),
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(50),
    )
    .await
    .unwrap();
    node.write_edge(EdgeMutation {
        cell_id: "reddit-home".to_string(),
        edge_type: "FOLLOWS".to_string(),
        src: 1,
        dst: 2,
        idempotency_key: "managed-cypher-seed".to_string(),
    })
    .await
    .unwrap();

    let rows = node
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "managed-cypher-read"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0].values, vec![QueryValue::VertexId(2)]);

    node.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_records_node_heartbeats_and_metrics() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/node-heartbeat", object_store)
        .await
        .unwrap();
    let first = control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    let second = control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Draining, 1_500)
        .await
        .unwrap();

    assert_eq!(first.started_at_ms, 1_000);
    assert_eq!(first.generation, 1);
    assert_eq!(second.started_at_ms, 1_000);
    assert_eq!(second.last_seen_ms, 1_500);
    assert_eq!(second.generation, 2);
    assert_eq!(second.state, GraphNodeHealthState::Draining);

    let loaded = control.node_heartbeat("node-a").await.unwrap().unwrap();
    assert_eq!(loaded, second);
    assert_eq!(control.load_node_heartbeats().await.unwrap(), vec![second]);
    assert_eq!(control.graph_control_metrics().node_heartbeat_writes, 2);
    control.close().await.unwrap();
}

#[tokio::test]
async fn cluster_controller_assigns_unplaced_cells_to_live_nodes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/controller-bootstrap", object_store)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-b", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    let config = GraphClusterControllerConfig::new(
        ["reddit-home", "reddit-search"],
        std::time::Duration::from_millis(1_000),
        std::time::Duration::from_secs(60),
    )
    .unwrap();

    let report = control.reconcile_cluster_at(&config, 1_100).await.unwrap();
    assert_eq!(report.active_nodes, vec!["node-a", "node-b"]);
    assert_eq!(report.reassignments.len(), 2);
    assert_eq!(report.failed_over_leases.len(), 2);
    assert!(report.pending_failovers.is_empty());
    assert!(report.unassigned_cells.is_empty());

    let placement = control.load_placement().await.unwrap();
    for cell_id in ["reddit-home", "reddit-search"] {
        let owner = placement.owner(cell_id).unwrap();
        assert!(owner == "node-a" || owner == "node-b");
        let lease = control.current_lease(cell_id).await.unwrap().unwrap();
        assert_eq!(lease.owner_node_id, owner);
        assert_eq!(lease.expires_at_ms, 61_100);
    }
    let metrics = control.graph_control_metrics();
    assert_eq!(metrics.controller_runs, 1);
    assert_eq!(metrics.controller_reassignments, 2);
    assert_eq!(metrics.controller_failovers, 2);
    control.close().await.unwrap();
}

fn rendezvous_prefers_node_b_cell_for_test() -> String {
    for idx in 0..10_000 {
        let cell_id = format!("rebalance-{idx}");
        let placement = ShardPlacement::rendezvous([cell_id.as_str()], ["node-a", "node-b"])
            .unwrap_or_else(|err| panic!("rendezvous placement failed for {cell_id}: {err}"));
        if placement.owner(&cell_id).unwrap() == "node-b" {
            return cell_id;
        }
    }
    panic!("could not find test cell preferring node-b");
}

#[tokio::test]
async fn cluster_controller_stability_first_keeps_live_owner() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/controller-stable", object_store)
        .await
        .unwrap();
    let cell_id = rendezvous_prefers_node_b_cell_for_test();
    control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-b", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([(cell_id.as_str(), "node-a")]).unwrap())
        .await
        .unwrap();
    let config = GraphClusterControllerConfig::new(
        [cell_id.as_str()],
        std::time::Duration::from_millis(5_000),
        std::time::Duration::from_secs(60),
    )
    .unwrap();

    let report = control.reconcile_cluster_at(&config, 1_100).await.unwrap();
    assert!(report.reassignments.is_empty());
    assert!(report.pending_failovers.is_empty());
    assert_eq!(
        control
            .load_placement()
            .await
            .unwrap()
            .owner(&cell_id)
            .unwrap(),
        "node-a"
    );
    control.close().await.unwrap();
}

#[tokio::test]
async fn cluster_controller_rendezvous_rebalance_waits_for_old_lease_expiry() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/controller-rebalance", object_store)
        .await
        .unwrap();
    let cell_id = rendezvous_prefers_node_b_cell_for_test();
    control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-b", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([(cell_id.as_str(), "node-a")]).unwrap())
        .await
        .unwrap();
    control
        .acquire_lease_at(
            &cell_id,
            "node-a",
            std::time::Duration::from_millis(1_000),
            1_000,
        )
        .await
        .unwrap();
    let config = GraphClusterControllerConfig::new(
        [cell_id.as_str()],
        std::time::Duration::from_millis(5_000),
        std::time::Duration::from_secs(60),
    )
    .unwrap()
    .with_rebalance_mode(GraphClusterRebalanceMode::Rendezvous);

    let pending = control.reconcile_cluster_at(&config, 1_100).await.unwrap();
    assert_eq!(pending.reassignments.len(), 1);
    assert_eq!(
        pending.reassignments[0].previous_owner_node_id.as_deref(),
        Some("node-a")
    );
    assert_eq!(pending.reassignments[0].new_owner_node_id, "node-b");
    assert!(pending.failed_over_leases.is_empty());
    assert_eq!(
        pending.pending_failovers,
        vec![GraphPendingFailover {
            cell_id: cell_id.clone(),
            current_owner_node_id: "node-a".to_string(),
            target_owner_node_id: "node-b".to_string(),
            lease_expires_at_ms: 2_000,
        }]
    );
    assert_eq!(
        control
            .load_placement()
            .await
            .unwrap()
            .owner(&cell_id)
            .unwrap(),
        "node-b"
    );
    assert_eq!(
        control
            .current_lease(&cell_id)
            .await
            .unwrap()
            .unwrap()
            .owner_node_id,
        "node-a"
    );

    let failed_over = control.reconcile_cluster_at(&config, 2_001).await.unwrap();
    assert!(failed_over.pending_failovers.is_empty());
    assert_eq!(failed_over.failed_over_leases.len(), 1);
    assert_eq!(failed_over.failed_over_leases[0].owner_node_id, "node-b");
    control.close().await.unwrap();
}

#[tokio::test]
async fn cluster_controller_moves_draining_cells_after_lease_expiry() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/controller-drain", object_store)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-b", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    control
        .acquire_lease_at(
            "reddit-home",
            "node-a",
            std::time::Duration::from_millis(1_000),
            1_000,
        )
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Draining, 1_100)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-b", GraphNodeHealthState::Active, 1_100)
        .await
        .unwrap();
    let config = GraphClusterControllerConfig::new(
        ["reddit-home"],
        std::time::Duration::from_millis(1_000),
        std::time::Duration::from_secs(60),
    )
    .unwrap();

    let pending = control.reconcile_cluster_at(&config, 1_200).await.unwrap();
    assert_eq!(pending.draining_nodes, vec!["node-a"]);
    assert_eq!(pending.reassignments.len(), 1);
    assert_eq!(pending.reassignments[0].new_owner_node_id, "node-b");
    assert_eq!(pending.failed_over_leases, Vec::<ShardLease>::new());
    assert_eq!(
        pending.pending_failovers,
        vec![GraphPendingFailover {
            cell_id: "reddit-home".to_string(),
            current_owner_node_id: "node-a".to_string(),
            target_owner_node_id: "node-b".to_string(),
            lease_expires_at_ms: 2_000,
        }]
    );
    assert_eq!(
        control
            .current_lease("reddit-home")
            .await
            .unwrap()
            .unwrap()
            .owner_node_id,
        "node-a"
    );

    let failed_over = control.reconcile_cluster_at(&config, 2_001).await.unwrap();
    assert!(failed_over.pending_failovers.is_empty());
    assert_eq!(failed_over.failed_over_leases.len(), 1);
    assert_eq!(failed_over.failed_over_leases[0].owner_node_id, "node-b");
    assert_eq!(
        control
            .current_lease("reddit-home")
            .await
            .unwrap()
            .unwrap()
            .owner_node_id,
        "node-b"
    );
    control.close().await.unwrap();
}

#[tokio::test]
async fn cluster_controller_fails_closed_without_active_nodes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/controller-no-live", object_store)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-a", GraphNodeHealthState::Draining, 1_000)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let config = GraphClusterControllerConfig::new(
        ["reddit-home"],
        std::time::Duration::from_millis(100),
        std::time::Duration::from_secs(60),
    )
    .unwrap();

    let report = control.reconcile_cluster_at(&config, 1_050).await.unwrap();
    assert_eq!(report.draining_nodes, vec!["node-a"]);
    assert_eq!(report.unassigned_cells, vec!["reddit-home"]);
    assert!(report.reassignments.is_empty());
    assert!(report.failed_over_leases.is_empty());
    assert_eq!(
        control
            .load_placement()
            .await
            .unwrap()
            .owner("reddit-home")
            .unwrap(),
        "node-a"
    );

    let expired = control.reconcile_cluster_at(&config, 1_101).await.unwrap();
    assert_eq!(expired.expired_nodes, vec!["node-a"]);
    assert_eq!(expired.unassigned_cells, vec!["reddit-home"]);
    assert!(expired.reassignments.is_empty());
    control.close().await.unwrap();
}

#[tokio::test]
async fn cluster_controller_discovers_cells_from_control_state() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/controller-discovery", object_store)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-b", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("placement-only", "node-a")]).unwrap())
        .await
        .unwrap();
    control
        .compare_and_publish_shard_metadata(
            GraphShardCatalogEntry {
                graph_id: Some("reddit".to_string()),
                cell_id: "catalog-only".to_string(),
                owner_node_id: "node-a".to_string(),
                lease_token: 0,
                schema_epoch: Some(1),
                graph_epoch: Some(0),
                generation: 0,
            },
            None,
        )
        .await
        .unwrap();
    let config = GraphClusterControllerConfig::discover_existing(
        std::time::Duration::from_millis(1_000),
        std::time::Duration::from_secs(60),
    )
    .unwrap();

    let report = control.reconcile_cluster_at(&config, 1_100).await.unwrap();
    assert_eq!(
        report.controlled_cells,
        vec!["catalog-only", "placement-only"]
    );
    assert_eq!(report.reassignments.len(), 2);
    assert_eq!(report.failed_over_leases.len(), 2);
    assert!(report.unassigned_cells.is_empty());
    assert_eq!(
        control
            .load_placement()
            .await
            .unwrap()
            .cells()
            .collect::<Vec<_>>(),
        vec!["catalog-only", "placement-only"]
    );
    for cell_id in ["catalog-only", "placement-only"] {
        let lease = control.current_lease(cell_id).await.unwrap().unwrap();
        assert_eq!(lease.owner_node_id, "node-b");
    }
    let catalog = control
        .current_shard_metadata("catalog-only")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog.graph_id.as_deref(), Some("reddit"));
    assert_eq!(catalog.owner_node_id, "node-b");
    control.close().await.unwrap();
}

#[tokio::test]
async fn cluster_controller_can_disable_existing_cell_discovery() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/controller-static-only", object_store)
        .await
        .unwrap();
    control
        .publish_node_heartbeat_at("node-b", GraphNodeHealthState::Active, 1_000)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("existing-cell", "node-a")]).unwrap())
        .await
        .unwrap();
    let config = GraphClusterControllerConfig::new(
        ["configured-cell"],
        std::time::Duration::from_millis(1_000),
        std::time::Duration::from_secs(60),
    )
    .unwrap()
    .with_existing_cell_discovery(false);

    let report = control.reconcile_cluster_at(&config, 1_100).await.unwrap();
    assert_eq!(report.controlled_cells, vec!["configured-cell"]);
    assert!(control
        .current_lease("existing-cell")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        control
            .current_lease("configured-cell")
            .await
            .unwrap()
            .unwrap()
            .owner_node_id,
        "node-b"
    );
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_catalog_uses_generation_cas() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/catalog-cas", object_store)
        .await
        .unwrap();
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    let entries = control
        .publish_placement_with_catalog(&placement, "reddit", 7)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].generation, 1);
    assert_eq!(
        control
            .load_placement()
            .await
            .unwrap()
            .owner("reddit-home")
            .unwrap(),
        "node-a"
    );

    let mut update = entries[0].clone();
    update.graph_epoch = Some(10);
    let published = control
        .compare_and_publish_shard_metadata(update.clone(), Some(1))
        .await
        .unwrap();
    assert_eq!(published.generation, 2);
    assert_eq!(published.graph_epoch, Some(10));

    update.graph_epoch = Some(11);
    let err = control
        .compare_and_publish_shard_metadata(update, Some(1))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::ControlMetadataConflict {
            ref key,
            expected_generation: Some(1),
            actual_generation: Some(2),
        } if key == "control/catalog/reddit-home"
    ));
    let metrics = control.graph_control_metrics();
    assert_eq!(metrics.metadata_cas_attempts, 2);
    assert!(metrics.metadata_cas_successes >= 2);
    assert_eq!(metrics.metadata_cas_conflicts, 1);
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_catalog_tracks_lease_token_generation() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/catalog-lease", object_store)
        .await
        .unwrap();
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    control
        .publish_placement_with_catalog(&placement, "reddit", 1)
        .await
        .unwrap();
    let lease_a = control
        .acquire_lease("reddit-home", "node-a", std::time::Duration::from_millis(5))
        .await
        .unwrap();
    let catalog_a = control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog_a.owner_node_id, "node-a");
    assert_eq!(catalog_a.lease_token, lease_a.lease_token);
    assert_eq!(catalog_a.generation, 2);

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let lease_b = control
        .failover_expired_cell("reddit-home", "node-b", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    let catalog_b = control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog_b.owner_node_id, "node-b");
    assert_eq!(catalog_b.lease_token, lease_b.lease_token);
    assert_eq!(catalog_b.generation, 3);
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_legacy_placement_creates_catalog_on_lease() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/catalog-legacy", object_store)
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    assert!(control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .is_none());

    let lease = control
        .acquire_lease("reddit-home", "node-a", std::time::Duration::from_millis(5))
        .await
        .unwrap();
    let catalog = control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog.graph_id, None);
    assert_eq!(catalog.schema_epoch, None);
    assert_eq!(catalog.graph_epoch, None);
    assert!(!catalog.has_graph_metadata());
    assert_eq!(catalog.owner_node_id, "node-a");
    assert_eq!(catalog.lease_token, lease.lease_token);
    assert_eq!(catalog.generation, 1);

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let lease = control
        .failover_expired_cell("reddit-home", "node-b", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    let catalog = control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog.owner_node_id, "node-b");
    assert_eq!(catalog.lease_token, lease.lease_token);
    assert_eq!(catalog.generation, 2);
    assert_eq!(catalog.graph_id, None);
    assert_eq!(catalog.schema_epoch, None);
    assert_eq!(catalog.graph_epoch, None);

    control
        .publish_placement_with_catalog(
            &ShardPlacement::fixed([("reddit-home", "node-b")]).unwrap(),
            "reddit",
            9,
        )
        .await
        .unwrap();
    let catalog = control
        .current_shard_metadata("reddit-home")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog.graph_id.as_deref(), Some("reddit"));
    assert_eq!(catalog.schema_epoch, Some(9));
    assert_eq!(catalog.graph_epoch, Some(0));
    assert!(catalog.has_graph_metadata());
    assert_eq!(catalog.owner_node_id, "node-b");
    assert_eq!(catalog.lease_token, lease.lease_token);
    assert_eq!(catalog.generation, 3);
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_watermarks_are_monotonic_and_generation_checked() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/watermark-cas", object_store)
        .await
        .unwrap();

    let first = control
        .advance_watermark(
            GraphControlWatermark {
                cell_id: "reddit-home".to_string(),
                durable_epoch: 5,
                safe_read_epoch: 4,
                outbox_epoch: 3,
                artifact_epoch: 2,
                generation: 0,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.generation, 1);

    let same = control
        .advance_watermark(
            GraphControlWatermark {
                generation: 99,
                ..first.clone()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(same, first);

    let stale = control
        .advance_watermark(
            GraphControlWatermark {
                cell_id: "reddit-home".to_string(),
                durable_epoch: 6,
                safe_read_epoch: 5,
                outbox_epoch: 4,
                artifact_epoch: 2,
                generation: 0,
            },
            Some(0),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        GraphError::ControlMetadataConflict {
            ref key,
            expected_generation: Some(0),
            actual_generation: Some(1)
        } if key == "control/watermark/reddit-home"
    ));

    let regression = control
        .advance_watermark(
            GraphControlWatermark {
                cell_id: "reddit-home".to_string(),
                durable_epoch: 6,
                safe_read_epoch: 3,
                outbox_epoch: 4,
                artifact_epoch: 2,
                generation: 0,
            },
            Some(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        regression,
        GraphError::ControlWatermarkRegression {
            ref cell_id,
            field: "safe_read_epoch",
            requested_epoch: 3,
            current_epoch: 4
        } if cell_id == "reddit-home"
    ));

    let advanced = control
        .advance_watermark(
            GraphControlWatermark {
                cell_id: "reddit-home".to_string(),
                durable_epoch: 6,
                safe_read_epoch: 5,
                outbox_epoch: 4,
                artifact_epoch: 2,
                generation: 0,
            },
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(advanced.generation, 2);
    assert_eq!(
        control
            .current_watermark("reddit-home")
            .await
            .unwrap()
            .unwrap(),
        advanced
    );
    let metrics = control.graph_control_metrics();
    assert_eq!(metrics.watermark_advances, 2);
    assert_eq!(metrics.watermark_rejects, 1);
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_idempotency_replays_and_rejects_conflicts() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/idempotency", object_store)
        .await
        .unwrap();
    let first = control
        .commit_control_idempotency("reddit-home", "repair", "req-1", b"ok")
        .await
        .unwrap();
    assert_eq!(first.result, b"ok");
    let replay = control
        .commit_control_idempotency("reddit-home", "repair", "req-1", b"ok")
        .await
        .unwrap();
    assert_eq!(replay, first);
    let conflict = control
        .commit_control_idempotency("reddit-home", "repair", "req-1", b"different")
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        GraphError::IdempotencyConflict {
            operation: "control",
            ref idempotency_key
        } if idempotency_key == "req-1"
    ));
    assert_eq!(
        control
            .control_idempotency_result("reddit-home", "repair", "req-1")
            .await
            .unwrap()
            .unwrap(),
        first
    );
    let metrics = control.graph_control_metrics();
    assert_eq!(metrics.control_idempotency_commits, 1);
    assert_eq!(metrics.control_idempotency_replays, 1);
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_repair_rebuilds_watermark_from_verified_graph_state() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/repair", Arc::clone(&object_store))
        .await
        .unwrap();
    let shard = open_test_shard("graph/control-repair", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    shard
        .write_edge(mutation(1, 2, "repair-cp-1"))
        .await
        .unwrap();
    shard
        .write_edge(mutation(1, 3, "repair-cp-2"))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
        .await
        .unwrap();

    let report = control
        .repair_cell_control_state(&shard, cell_id, edge_type)
        .await
        .unwrap();
    assert_eq!(report.read_epoch, base_epoch);
    assert_eq!(report.live_edges, 2);
    assert_eq!(report.delta_records, 2);
    assert_eq!(report.mismatch_count, 0);
    assert!(report.repaired_watermark);
    assert_eq!(report.watermark.durable_epoch, base_epoch);
    assert_eq!(report.watermark.safe_read_epoch, base_epoch);
    assert_eq!(report.watermark.outbox_epoch, base_epoch);
    assert_eq!(report.watermark.artifact_epoch, base_epoch);
    assert_eq!(report.watermark.edge_type, edge_type);
    assert!(control.current_watermark(cell_id).await.unwrap().is_none());
    assert_eq!(
        control
            .current_edge_watermark(cell_id, edge_type)
            .await
            .unwrap()
            .unwrap(),
        report.watermark
    );

    let replay = control
        .repair_cell_control_state(&shard, cell_id, edge_type)
        .await
        .unwrap();
    assert!(!replay.repaired_watermark);
    assert_eq!(replay.watermark, report.watermark);

    shard
        .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
        .await
        .unwrap();
    let after_delta_gc = control
        .repair_cell_control_state(&shard, cell_id, edge_type)
        .await
        .unwrap();
    assert!(!after_delta_gc.repaired_watermark);
    assert_eq!(after_delta_gc.watermark, report.watermark);

    let metrics = control.graph_control_metrics();
    assert_eq!(metrics.repair_runs, 3);
    assert_eq!(metrics.repair_actions, 1);
    shard.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_empty_compute_node_replacement_reads_object_store_state() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control_path = "graph-control/empty-compute-replacement";
    let control = GraphControlPlane::open(control_path, Arc::clone(&object_store))
        .await
        .unwrap();
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    control
        .publish_placement_with_catalog(&placement, "reddit", 1)
        .await
        .unwrap();
    let cluster_a = RoutedGraphCluster::open_owned_with_control(
        "phase1-empty-compute",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    let cluster_a_lease_expires_at = cluster_a.lease("reddit-home").unwrap().expires_at_ms;
    cluster_a
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "empty-compute-a".to_string(),
        })
        .await
        .unwrap();
    cluster_a.close().await.unwrap();
    control.close().await.unwrap();

    let control = GraphControlPlane::open(control_path, Arc::clone(&object_store))
        .await
        .unwrap();
    control
        .failover_expired_cell_at(
            "reddit-home",
            "node-b",
            std::time::Duration::from_secs(60),
            cluster_a_lease_expires_at + 1,
        )
        .await
        .unwrap();
    let cluster_b = RoutedGraphCluster::open_owned_with_control(
        "phase1-empty-compute",
        "node-b",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    let shard_b = cluster_b.shard("reddit-home").unwrap();
    assert_eq!(
        shard_b
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2]
    );
    cluster_b
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
            src: 1,
            dst: 3,
            idempotency_key: "empty-compute-b".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(
        shard_b
            .out_neighbors("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
            .await
            .unwrap(),
        vec![2, 3]
    );
    cluster_b.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_repair_does_not_advance_cell_watermark_for_other_edge_types() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control =
        GraphControlPlane::open("graph-control/repair-edge-scope", Arc::clone(&object_store))
            .await
            .unwrap();
    let shard = open_test_shard("graph/control-repair-edge-scope", object_store).await;
    let cell_id = "reddit-home";
    let repaired_edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    let dirty_edge_type = "USER_BLOCKED_USER";
    shard
        .write_edge(EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: repaired_edge_type.to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "repair-scope-clean".to_string(),
        })
        .await
        .unwrap();
    shard
        .write_edge(EdgeMutation {
            cell_id: cell_id.to_string(),
            edge_type: dirty_edge_type.to_string(),
            src: 7,
            dst: 8,
            idempotency_key: "repair-scope-dirty".to_string(),
        })
        .await
        .unwrap();
    let read_epoch = shard.current_epoch(cell_id).await.unwrap();

    let mut batch = WriteBatch::new();
    batch.delete(keys::out_edge(cell_id, dirty_edge_type, 7, 8).as_bytes());
    shard.write_strict_for_test(batch).await.unwrap();

    let report = control
        .repair_cell_control_state(&shard, cell_id, repaired_edge_type)
        .await
        .unwrap();
    assert_eq!(report.read_epoch, read_epoch);
    assert_eq!(report.watermark.edge_type, repaired_edge_type);
    assert_eq!(report.watermark.safe_read_epoch, read_epoch);
    assert!(control.current_watermark(cell_id).await.unwrap().is_none());
    assert!(control
        .current_edge_watermark(cell_id, dirty_edge_type)
        .await
        .unwrap()
        .is_none());
    assert!(!shard
        .verify_current_graph(cell_id, dirty_edge_type, 1, 4)
        .await
        .unwrap()
        .is_clean());

    shard.close().await.unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
async fn control_plane_repair_refuses_to_advance_on_corrupt_graph_state() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control =
        GraphControlPlane::open("graph-control/repair-corrupt", Arc::clone(&object_store))
            .await
            .unwrap();
    let shard = open_test_shard("graph/control-repair-corrupt", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    shard
        .write_edge(mutation(1, 2, "repair-cp-corrupt"))
        .await
        .unwrap();
    let mut batch = WriteBatch::new();
    batch.delete(keys::out_edge(cell_id, edge_type, 1, 2).as_bytes());
    shard.write_strict_for_test(batch).await.unwrap();

    let err = control
        .repair_cell_control_state(&shard, cell_id, edge_type)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::CorruptValue {
            ref key,
            ref reason
        } if key == "control/watermark_edge/reddit-home/USER_SUBSCRIBED_TO_SUBREDDIT"
            && reason.contains("cannot repair control state")
    ));
    assert!(control.current_watermark(cell_id).await.unwrap().is_none());
    shard.close().await.unwrap();
    control.close().await.unwrap();
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
        .acquire_matrix_artifact_write_lock(cell_id, edge_type, base_epoch, "held-by-test-builder")
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
async fn posting_chunks_ignore_unpublished_orphan_chunks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/posting-orphan-chunk-hidden", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "ORPHAN_POSTING_EDGE";
    let base_epoch = 7;
    let chunk_key = format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/out/{:020}/{base_epoch:020}/{:020}",
        1, 0
    );
    let chunk_value = format!("posting1\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t0\t2,3\n");

    let mut batch = GraphWriteBatch::new();
    batch.put(chunk_key.as_bytes(), chunk_value.as_bytes());
    shard
        .write_graph_batch_strict(cell_id, "test_seed_orphan_posting_chunk", batch)
        .await
        .unwrap();

    assert!(shard
        .posting_chunks(cell_id, edge_type, ArtifactDirection::Out, 1, base_epoch)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn posting_chunks_ignore_owner_manifest_without_epoch_manifest() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/posting-owner-manifest-hidden", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "PARTIAL_POSTING_MANIFEST_EDGE";
    let base_epoch = 11;
    let chunk_key = format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/out/{:020}/{base_epoch:020}/{:020}",
        1, 0
    );
    let manifest_key = format!(
        "cell/{cell_id}/artifact/posting_manifest/{edge_type}/out/{:020}/{base_epoch:020}",
        1
    );
    let chunk_value = format!("posting1\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t0\t2,3\n");
    let manifest_value =
        format!("posting_manifest1\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t1\t2\t0\n");

    let mut batch = GraphWriteBatch::new();
    batch.put(chunk_key.as_bytes(), chunk_value.as_bytes());
    batch.put(manifest_key.as_bytes(), manifest_value.as_bytes());
    shard
        .write_graph_batch_strict(cell_id, "test_seed_partial_posting_manifest", batch)
        .await
        .unwrap();

    assert!(shard
        .posting_chunks(cell_id, edge_type, ArtifactDirection::Out, 1, base_epoch)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn posting_abort_cleanup_removes_unpublished_chunks_and_owner_manifests() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/posting-abort-cleanup", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "POSTING_ABORT_CLEANUP_EDGE";
    let base_epoch = 13;
    let chunk_key = format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/out/{:020}/{base_epoch:020}/{:020}",
        1, 0
    );
    let manifest_key = format!(
        "cell/{cell_id}/artifact/posting_manifest/{edge_type}/out/{:020}/{base_epoch:020}",
        1
    );
    let chunk_value = format!("posting1\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t0\t2,3\n");
    let manifest_value =
        format!("posting_manifest1\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t1\t2\t0\n");

    let mut batch = GraphWriteBatch::new();
    batch.put(chunk_key.as_bytes(), chunk_value.as_bytes());
    batch.put(manifest_key.as_bytes(), manifest_value.as_bytes());
    shard
        .write_graph_batch_strict(cell_id, "test_seed_unpublished_posting_artifacts", batch)
        .await
        .unwrap();

    let cleanup = engine::cleanup_unpublished_posting_artifact_epoch(
        &shard,
        cell_id,
        edge_type,
        base_epoch,
        "test_cleanup_unpublished_posting_artifacts",
    )
    .await;
    assert_eq!(cleanup.deleted_keys, 2);
    assert_eq!(cleanup.cleanup_errors, 0);
    assert!(!cleanup.skipped_published_manifest);
    assert!(shard.read_remote(&chunk_key).await.unwrap().is_none());
    assert!(shard.read_remote(&manifest_key).await.unwrap().is_none());
}

#[tokio::test]
async fn posting_abort_cleanup_preserves_published_epoch_manifest() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/posting-abort-cleanup-published", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "POSTING_ABORT_PUBLISHED_EDGE";

    shard
        .write_edge(typed_mutation(
            cell_id,
            edge_type,
            1,
            2,
            "posting-published-seed",
        ))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .build_posting_chunks(cell_id, edge_type, base_epoch, 2)
        .await
        .unwrap();
    let epoch_manifest_key =
        format!("cell/{cell_id}/artifact/posting_epoch_manifest/{edge_type}/{base_epoch:020}");
    assert!(shard
        .read_remote(&epoch_manifest_key)
        .await
        .unwrap()
        .is_some());

    let cleanup = engine::cleanup_unpublished_posting_artifact_epoch(
        &shard,
        cell_id,
        edge_type,
        base_epoch,
        "test_cleanup_published_posting_artifacts",
    )
    .await;
    assert_eq!(cleanup.deleted_keys, 0);
    assert_eq!(cleanup.cleanup_errors, 0);
    assert!(cleanup.skipped_published_manifest);
    assert!(shard
        .read_remote(&epoch_manifest_key)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn supernode_groups_ignore_unpublished_orphan_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/supernode-orphan-hidden", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "SUPER_ORPHAN_EDGE";
    let base_epoch = 13;
    let chunk_key = format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/out/{:020}/{base_epoch:020}/{:020}",
        1, 0
    );
    let group_key = format!(
        "cell/{cell_id}/artifact/supernode/{edge_type}/out/{:020}/{base_epoch:020}",
        1
    );
    let chunk_value = format!("posting1\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t0\t2,3\n");
    let group_value =
        format!("supernode3\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t2\t1\t2\t0:2:3\n");

    let mut batch = GraphWriteBatch::new();
    batch.put(chunk_key.as_bytes(), chunk_value.as_bytes());
    batch.put(group_key.as_bytes(), group_value.as_bytes());
    shard
        .write_graph_batch_strict(cell_id, "test_seed_unpublished_supernode_artifacts", batch)
        .await
        .unwrap();

    assert!(shard
        .supernode_group(cell_id, edge_type, ArtifactDirection::Out, 1, base_epoch)
        .await
        .unwrap()
        .is_none());

    let cleanup = engine::cleanup_unpublished_supernode_artifact_epoch(
        &shard,
        cell_id,
        edge_type,
        base_epoch,
        "test_cleanup_unpublished_supernode_artifacts",
    )
    .await;
    assert_eq!(cleanup.deleted_keys, 2);
    assert_eq!(cleanup.cleanup_errors, 0);
    assert!(!cleanup.skipped_published_manifest);
    assert!(shard.read_remote(&chunk_key).await.unwrap().is_none());
    assert!(shard.read_remote(&group_key).await.unwrap().is_none());
}

#[tokio::test]
async fn supernode_build_publishes_epoch_manifest() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/supernode-publishes-manifest", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "SUPER_MANIFEST_EDGE";
    for dst in 10..14 {
        shard
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                1,
                dst,
                &format!("super-manifest-{dst}"),
            ))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .build_supernode_groups(cell_id, edge_type, base_epoch, 2, 2)
        .await
        .unwrap();
    let manifest_key =
        format!("cell/{cell_id}/artifact/supernode_epoch_manifest/{edge_type}/{base_epoch:020}");
    assert!(shard.read_remote(&manifest_key).await.unwrap().is_some());
    assert_eq!(
        shard
            .supernode_page(cell_id, edge_type, ArtifactDirection::Out, 1, base_epoch, 0)
            .await
            .unwrap()
            .unwrap()
            .vertices,
        vec![10, 11]
    );
}

#[tokio::test]
async fn supernode_abort_cleanup_preserves_published_epoch_manifest() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/supernode-cleanup-published", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "SUPER_CLEANUP_PUBLISHED_EDGE";
    for dst in 20..24 {
        shard
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                1,
                dst,
                &format!("super-cleanup-published-{dst}"),
            ))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .build_supernode_groups(cell_id, edge_type, base_epoch, 2, 2)
        .await
        .unwrap();
    let manifest_key =
        format!("cell/{cell_id}/artifact/supernode_epoch_manifest/{edge_type}/{base_epoch:020}");
    assert!(shard.read_remote(&manifest_key).await.unwrap().is_some());

    let cleanup = engine::cleanup_unpublished_supernode_artifact_epoch(
        &shard,
        cell_id,
        edge_type,
        base_epoch,
        "test_cleanup_published_supernode_artifacts",
    )
    .await;
    assert_eq!(cleanup.deleted_keys, 0);
    assert_eq!(cleanup.cleanup_errors, 0);
    assert!(cleanup.skipped_published_manifest);
    assert!(shard.read_remote(&manifest_key).await.unwrap().is_some());
}

#[tokio::test]
async fn posting_chunks_require_all_manifest_chunks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/posting-manifest-missing-chunk", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "MISSING_POSTING_CHUNK_EDGE";

    for (idx, dst) in [2, 3, 4].into_iter().enumerate() {
        shard
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                1,
                dst,
                &format!("posting-missing-seed-{idx}"),
            ))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    assert_eq!(
        shard
            .build_posting_chunks(cell_id, edge_type, base_epoch, 2)
            .await
            .unwrap()
            .len(),
        5
    );
    let missing_key = format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/out/{:020}/{base_epoch:020}/{:020}",
        1, 1
    );
    let mut batch = GraphWriteBatch::new();
    batch.delete(missing_key.as_bytes());
    shard
        .write_graph_batch_strict(cell_id, "test_delete_manifest_chunk", batch)
        .await
        .unwrap();

    let err = shard
        .posting_chunks(cell_id, edge_type, ArtifactDirection::Out, 1, base_epoch)
        .await
        .unwrap_err();
    assert!(matches!(err, GraphError::CorruptValue { .. }));
}

#[tokio::test]
async fn posting_chunks_validate_manifest_checksums() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/posting-manifest-checksum", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "CHECKSUM_POSTING_EDGE";

    for (idx, dst) in [2, 3, 4].into_iter().enumerate() {
        shard
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                1,
                dst,
                &format!("posting-checksum-seed-{idx}"),
            ))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .build_posting_chunks(cell_id, edge_type, base_epoch, 2)
        .await
        .unwrap();
    let chunk_key = format!(
        "cell/{cell_id}/artifact/posting/{edge_type}/out/{:020}/{base_epoch:020}/{:020}",
        1, 0
    );
    let bad_chunk = format!("posting1\t{cell_id}\t{edge_type}\tout\t1\t{base_epoch}\t0\t999\n");
    let mut batch = GraphWriteBatch::new();
    batch.put(chunk_key.as_bytes(), bad_chunk.as_bytes());
    shard
        .write_graph_batch_strict(cell_id, "test_corrupt_manifest_chunk", batch)
        .await
        .unwrap();

    let err = shard
        .posting_chunks(cell_id, edge_type, ArtifactDirection::Out, 1, base_epoch)
        .await
        .unwrap_err();
    assert!(matches!(err, GraphError::CorruptValue { .. }));
}

#[tokio::test]
async fn build_posting_chunks_rejects_incompatible_republish() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/posting-incompatible-republish", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "REBUILD_POSTING_EDGE";

    for (idx, dst) in [2, 3, 4].into_iter().enumerate() {
        shard
            .write_edge(typed_mutation(
                cell_id,
                edge_type,
                1,
                dst,
                &format!("posting-republish-seed-{idx}"),
            ))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .build_posting_chunks(cell_id, edge_type, base_epoch, 2)
        .await
        .unwrap();
    let epoch_manifest_key =
        format!("cell/{cell_id}/artifact/posting_epoch_manifest/{edge_type}/{base_epoch:020}");
    assert!(shard
        .read_remote(&epoch_manifest_key)
        .await
        .unwrap()
        .is_some());

    let err = shard
        .build_posting_chunks(cell_id, edge_type, base_epoch, 3)
        .await
        .unwrap_err();
    assert!(matches!(err, GraphError::CorruptValue { .. }));

    let chunks = shard
        .posting_chunks(cell_id, edge_type, ArtifactDirection::Out, 1, base_epoch)
        .await
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].vertices, vec![2, 3]);
    assert_eq!(chunks[1].vertices, vec![4]);
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

#[tokio::test]
async fn read_leases_block_delta_and_artifact_gc_until_ttl() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/read-lease-retention",
        object_store,
        GraphOpenOptions {
            retention_policy: GraphRetentionPolicy {
                read_lease_ttl_ms: 60_000,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    shard.write_edge(mutation(1, 2, "lease-1")).await.unwrap();
    shard.write_edge(mutation(2, 3, "lease-2")).await.unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    let _snapshot = shard.snapshot(cell_id).await.unwrap();

    let delta_err = shard
        .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
        .await
        .unwrap_err();
    assert!(matches!(
        delta_err,
        GraphError::RetentionViolation {
            operation: "delete_deltas_through_rollup",
            ref cell_id,
            requested_epoch,
            safe_epoch: 0,
        } if cell_id == "reddit-home" && requested_epoch == base_epoch
    ));

    let artifact_err = shard
        .delete_graph_artifacts_before(cell_id, edge_type, base_epoch + 1)
        .await
        .unwrap_err();
    assert!(matches!(
        artifact_err,
        GraphError::RetentionViolation {
            operation: "delete_graph_artifacts_before",
            ref cell_id,
            requested_epoch,
            safe_epoch: 1,
        } if cell_id == "reddit-home" && requested_epoch == base_epoch + 1
    ));

    let metrics = shard.graph_operational_metrics();
    assert!(metrics.read_leases_created >= 1);
    assert!(metrics.retention_rejects >= 2);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn expired_read_leases_are_pruned_and_gc_can_continue() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/read-lease-expiry",
        object_store,
        GraphOpenOptions {
            retention_policy: GraphRetentionPolicy {
                read_lease_ttl_ms: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    shard.write_edge(mutation(1, 2, "expiry-1")).await.unwrap();
    shard.write_edge(mutation(2, 3, "expiry-2")).await.unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    let _snapshot = shard.snapshot(cell_id).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let delta_gc = shard
        .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
        .await
        .unwrap();
    assert!(delta_gc.deleted_delta_keys > 0);
    assert_eq!(
        shard.delta_gc_watermark(cell_id, edge_type).await.unwrap(),
        base_epoch
    );
    let metrics = shard.graph_operational_metrics();
    assert!(metrics.gc_jobs_completed >= 1);
    assert!(metrics.gc_keys_deleted >= delta_gc.deleted_delta_keys);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn min_retained_epochs_blocks_delta_gc() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/min-retained-epochs",
        object_store,
        GraphOpenOptions {
            retention_policy: GraphRetentionPolicy {
                min_retained_epochs: 10,
                read_lease_ttl_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    shard
        .write_edge(mutation(1, 2, "retained-1"))
        .await
        .unwrap();
    shard
        .write_edge(mutation(2, 3, "retained-2"))
        .await
        .unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    let err = shard
        .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::RetentionViolation {
            operation: "delete_deltas_through_rollup",
            requested_epoch: 2,
            safe_epoch: 0,
            ..
        }
    ));
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
async fn operational_metrics_track_writes_artifacts_gc_and_verifier() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/operational-metrics",
        object_store,
        GraphOpenOptions {
            retention_policy: GraphRetentionPolicy {
                read_lease_ttl_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    shard.write_edge(mutation(1, 2, "metrics-1")).await.unwrap();
    shard.write_edge(mutation(2, 3, "metrics-2")).await.unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    shard
        .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
        .await
        .unwrap();
    let report = shard
        .verify_current_graph(cell_id, edge_type, 2, 8)
        .await
        .unwrap();
    assert_eq!(report.mismatch_count, 0);

    let metrics = shard.graph_operational_metrics();
    assert!(metrics.write_attempts >= 2);
    assert!(metrics.write_commits >= 2);
    assert!(metrics.artifact_builds_started >= 2);
    assert!(metrics.artifact_builds_completed >= 2);
    assert!(metrics.artifact_build_duration_us > 0);
    assert!(metrics.artifact_publish_batches > 0);
    assert!(metrics.artifact_records_published > 0);
    assert!(metrics.artifact_publish_duration_us > 0);
    assert!(metrics.gc_jobs_started >= 1);
    assert!(metrics.gc_jobs_completed >= 1);
    assert!(metrics.gc_keys_deleted > 0);
    assert!(metrics.gc_duration_us > 0);
    assert!(metrics.verifier_runs >= 1);
    assert_eq!(metrics.verifier_failures, 0);
    assert!(metrics.verifier_duration_us > 0);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn repair_report_validates_degrees_and_delta_counts() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/repair-report", object_store).await;
    shard.write_edge(mutation(1, 2, "repair-1")).await.unwrap();
    shard.write_edge(mutation(1, 3, "repair-2")).await.unwrap();
    let report = shard
        .validate_cell_edge_type("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT")
        .await
        .unwrap();
    assert_eq!(report.live_edges, 2);
    assert_eq!(report.delta_records, 2);
    assert!(report.degree_mismatches.is_empty());
    shard.close().await.unwrap();
}

#[tokio::test]
async fn current_graph_verifier_survives_rollup_and_delta_gc() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";
    let shard = open_test_shard("graph/current-verifier-gc", object_store).await;
    shard.write_edge(mutation(1, 2, "verify-1")).await.unwrap();
    shard.write_edge(mutation(1, 3, "verify-2")).await.unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    shard
        .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
        .await
        .unwrap();
    shard.write_edge(mutation(1, 4, "verify-3")).await.unwrap();
    shard
        .delete_edge(mutation(1, 2, "verify-delete-1"))
        .await
        .unwrap();

    let report = shard
        .verify_current_graph(cell_id, edge_type, 3, 8)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.mismatch_samples);
    assert_eq!(report.digest.live_edges, 2);
    assert!(report.delta_gc_watermark >= base_epoch);
    assert!(report.matrix_edges_checked >= 2);
    assert!(report.traversal_roots_checked > 0);
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
            .any(|sample| sample.contains("out_index:missing")),
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
async fn rollup_artifact_gc_keeps_latest_artifacts_and_retains_snapshot_deltas() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/rollup-artifact-gc";
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    {
        let shard = open_test_shard(path, Arc::clone(&object_store)).await;
        shard
            .write_edge(mutation(1, 2, "rollup-base-1"))
            .await
            .unwrap();
        let epoch_one = shard.current_epoch(cell_id).await.unwrap();
        let first = shard
            .rollup_artifacts(cell_id, edge_type, epoch_one, 2, 2, 1, 2)
            .await
            .unwrap();
        assert_eq!(first.base_epoch, epoch_one);
        let epoch_one_posting_manifest =
            format!("cell/{cell_id}/artifact/posting_epoch_manifest/{edge_type}/{epoch_one:020}");
        assert!(shard
            .read_remote(&epoch_one_posting_manifest)
            .await
            .unwrap()
            .is_some());

        shard
            .write_edge(mutation(1, 3, "rollup-base-2"))
            .await
            .unwrap();
        let epoch_two = shard.current_epoch(cell_id).await.unwrap();
        let second = shard
            .rollup_artifacts(cell_id, edge_type, epoch_two, 2, 2, 1, 2)
            .await
            .unwrap();
        assert_eq!(second.base_epoch, epoch_two);

        let gc = shard
            .delete_graph_artifacts_before(cell_id, edge_type, epoch_two)
            .await
            .unwrap();
        assert!(gc.deleted_keys > 0);
        assert!(gc.retained_keys > 0);
        shard.close().await.unwrap();
    }

    let reopened = open_test_shard(path, object_store).await;
    assert!(reopened
        .latest_matrix_artifact(cell_id, edge_type, 1)
        .await
        .unwrap()
        .is_none());
    let epoch_one_posting_manifest = format!(
        "cell/{cell_id}/artifact/posting_epoch_manifest/{edge_type}/{:020}",
        1
    );
    assert!(reopened
        .read_remote(&epoch_one_posting_manifest)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        reopened
            .latest_rollup(cell_id, edge_type, 2)
            .await
            .unwrap()
            .unwrap()
            .base_epoch,
        2
    );
    assert_eq!(
        reopened
            .out_neighbors_at(cell_id, edge_type, 1, 1)
            .await
            .unwrap(),
        vec![2]
    );
    assert_eq!(
        reopened
            .matrix_reachable(cell_id, edge_type, &[1], 1, 2)
            .await
            .unwrap()
            .vertices,
        vec![2, 3]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn delta_gc_requires_rollup_and_preserves_reads_after_watermark() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/delta-gc-rollup", object_store).await;
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    shard.write_edge(mutation(1, 2, "gc-base-1")).await.unwrap();
    shard.write_edge(mutation(1, 3, "gc-base-2")).await.unwrap();
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();
    shard
        .rollup_artifacts(cell_id, edge_type, base_epoch, 2, 2, 1, 2)
        .await
        .unwrap();
    shard
        .write_edge(mutation(1, 4, "gc-after-rollup"))
        .await
        .unwrap();
    let read_epoch = shard.current_epoch(cell_id).await.unwrap();

    let gc = shard
        .delete_deltas_through_rollup(cell_id, edge_type, base_epoch)
        .await
        .unwrap();
    assert_eq!(gc.compacted_through_epoch, base_epoch);
    assert_eq!(gc.deleted_delta_keys, 2);
    assert_eq!(
        shard
            .out_neighbors_at(cell_id, edge_type, 1, read_epoch)
            .await
            .unwrap(),
        vec![2, 3, 4]
    );

    let old_snapshot = shard
        .out_neighbors_at(cell_id, edge_type, 1, base_epoch - 1)
        .await
        .unwrap_err();
    assert!(matches!(
        old_snapshot,
        GraphError::SnapshotExpired {
            ref cell_id,
            ref edge_type,
            min_epoch,
            ..
        } if cell_id == "reddit-home"
            && edge_type == "USER_SUBSCRIBED_TO_SUBREDDIT"
            && min_epoch == base_epoch
    ));
    let raw_deltas = shard.deltas_since(cell_id, edge_type, 0).await.unwrap_err();
    assert!(matches!(raw_deltas, GraphError::SnapshotExpired { .. }));
    shard.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_rollup_artifact_builds_publish_one_coherent_epoch() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(open_test_shard("graph/concurrent-rollup", object_store).await);
    let cell_id = "reddit-home";
    let edge_type = "USER_SUBSCRIBED_TO_SUBREDDIT";

    for idx in 0..64_u64 {
        shard
            .write_edge(mutation(
                1,
                10_000 + idx,
                &format!("rollup-concurrent-{idx}"),
            ))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch(cell_id).await.unwrap();

    let first = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            shard
                .rollup_artifacts(cell_id, edge_type, base_epoch, 8, 16, 16, 8)
                .await
        })
    };
    let second = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            shard
                .rollup_artifacts(cell_id, edge_type, base_epoch, 8, 16, 16, 8)
                .await
        })
    };

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first, second);
    assert_eq!(
        shard
            .latest_rollup(cell_id, edge_type, base_epoch)
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        shard
            .matrix_reachable(cell_id, edge_type, &[1], 1, base_epoch)
            .await
            .unwrap()
            .vertices
            .len(),
        64
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn graph_cluster_reopens_many_shards_from_local_object_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let cells = ["cell-a", "cell-b", "cell-c", "cell-d"];
    let edge_type = "FOLLOWS";

    {
        let object_store = local_object_store(tempdir.path()).unwrap();
        let cluster =
            GraphCluster::open_cells_standalone_writers("graph-local-cluster", cells, object_store)
                .await
                .unwrap();
        for (idx, cell_id) in cells.iter().enumerate() {
            let shard = cluster.shard(cell_id).unwrap();
            let src = 10 + idx as u64;
            for step in 0..4 {
                shard
                    .write_edge(EdgeMutation {
                        cell_id: (*cell_id).to_string(),
                        edge_type: edge_type.to_string(),
                        src: src + step,
                        dst: src + step + 1,
                        idempotency_key: format!("{cell_id}-chain-{step}"),
                    })
                    .await
                    .unwrap();
            }
            for dst in 100..106 {
                shard
                    .write_edge(EdgeMutation {
                        cell_id: (*cell_id).to_string(),
                        edge_type: edge_type.to_string(),
                        src: 1000 + idx as u64,
                        dst,
                        idempotency_key: format!("{cell_id}-super-{dst}"),
                    })
                    .await
                    .unwrap();
            }
            let base_epoch = shard.current_epoch(cell_id).await.unwrap();
            shard
                .build_posting_chunks(cell_id, edge_type, base_epoch, 2)
                .await
                .unwrap();
            shard
                .build_matrix_tiles(cell_id, edge_type, base_epoch, 4)
                .await
                .unwrap();
            shard
                .build_supernode_groups(cell_id, edge_type, base_epoch, 4, 2)
                .await
                .unwrap();
        }
        cluster.close().await.unwrap();
    }

    let object_store = local_object_store(tempdir.path()).unwrap();
    let reopened = GraphCluster::open_cells("graph-local-cluster", cells, object_store)
        .await
        .unwrap();
    assert_eq!(reopened.shard_count(), cells.len());
    for (idx, cell_id) in cells.iter().enumerate() {
        let shard = reopened.shard(cell_id).unwrap();
        let read_epoch = shard.current_epoch(cell_id).await.unwrap();
        let src = 10 + idx as u64;
        let traversal = shard
            .matrix_reachable(cell_id, edge_type, &[src], 4, read_epoch)
            .await
            .unwrap();
        assert_eq!(traversal.vertices, vec![src + 1, src + 2, src + 3, src + 4]);
        assert_eq!(
            shard
                .supernode_degree(cell_id, edge_type, 1000 + idx as u64, read_epoch)
                .await
                .unwrap(),
            6
        );
        assert!(shard
            .supernode_edge_exists(cell_id, edge_type, 1000 + idx as u64, 105, read_epoch)
            .await
            .unwrap());
    }
    reopened.close().await.unwrap();
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

#[test]
fn locality_cell_extractor_covers_graph_keyspace() {
    let keys = vec![
            keys::last_epoch("reddit-home"),
            keys::idempotency("reddit-home", "create", "req-1"),
            keys::edge("reddit-home", "USER_FOLLOWS_USER", 1, 2),
            keys::out_edge("reddit-home", "USER_FOLLOWS_USER", 1, 2),
            keys::in_edge("reddit-home", "USER_FOLLOWS_USER", 2, 1),
            keys::degree_out("reddit-home", "USER_FOLLOWS_USER", 1),
            keys::degree_in("reddit-home", "USER_FOLLOWS_USER", 2),
            keys::outbox("reddit-home", 1, DeltaKind::Plus, "USER_FOLLOWS_USER", 1, 2),
            keys::delta_plus("reddit-home", "USER_FOLLOWS_USER", 1, 1, 2),
            keys::delta_minus("reddit-home", "USER_FOLLOWS_USER", 2, 1, 2),
            "cell/reddit-home/artifact/posting/USER_FOLLOWS_USER/out/00000000000000000001/00000000000000000002/00000000000000000000".to_string(),
            "cell/reddit-home/artifact/matrix_manifest/USER_FOLLOWS_USER/00000000000000000002".to_string(),
            "cell/reddit-home/artifact/matrix/USER_FOLLOWS_USER/00000000000000000002/out/00000000000000000000/00000000000000000000".to_string(),
            "cell/reddit-home/artifact/supernode/USER_FOLLOWS_USER/out/00000000000000000001/00000000000000000002".to_string(),
            keys::edge("subreddit-programming", "POSTED_IN", 10, 20),
        ];

    let experiment = compare_locality_layouts(keys.iter().map(String::as_str));
    assert!(experiment.segment_extractor_safe());
    assert_eq!(experiment.total_keys, keys.len());
    assert_eq!(experiment.cells["reddit-home"], 14);
    assert_eq!(experiment.cells["subreddit-programming"], 1);
    assert_eq!(
        experiment.recommended_layout,
        StorageLayout::OneDbPerLocalityCell
    );

    let extractor = LocalityCellExtractor::new();
    let expected = locality_cell_prefix("reddit-home");
    let edge_key = keys::out_edge("reddit-home", "USER_FOLLOWS_USER", 1, 2);
    assert_eq!(
        extractor.prefix(edge_key.as_bytes()),
        Some(expected.as_ref())
    );
    assert_eq!(
        extractor.prefix_len(&PrefixTarget::Point(Bytes::from(edge_key))),
        Some(expected.len())
    );
    assert_eq!(
        extractor.prefix_len(&PrefixTarget::Prefix(Bytes::from_static(
            b"cell/reddit-home/e/out/"
        ))),
        Some(expected.len())
    );
    assert_eq!(
        extractor.prefix_len(&PrefixTarget::Prefix(Bytes::from_static(b"cell/reddit"))),
        None
    );
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
async fn query_plan_snapshot_reads_pin_epoch() {
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

    assert_eq!(
        shard.execute_query_plan(plan).await.unwrap(),
        QueryOutput::Vertices(vec![2])
    );
    assert_eq!(
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
            .unwrap(),
        QueryOutput::Count(1)
    );
    assert_eq!(
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
            .unwrap(),
        QueryOutput::Bool(false)
    );
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

#[tokio::test]
async fn routed_cluster_executes_read_plans_without_write_lease_and_rejects_writes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open("graph-control/query-routed", Arc::clone(&object_store))
        .await
        .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let mut cluster = RoutedGraphCluster::open_owned_with_control(
        "query-routed",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();
    cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "query-routed-seed".to_string(),
        })
        .await
        .unwrap();

    #[cfg(feature = "opencypher")]
    {
        let merge = cluster
            .execute_cypher(
                QueryContext::new("reddit-home", "query-routed-cypher-merge"),
                "MERGE (u {id: 1})-[:FOLLOWS]->(v {id: 4})",
            )
            .await
            .unwrap();
        assert_eq!(
            merge,
            QueryOutput::Mutation(QueryMutationResult {
                created_edges: 1,
                ..QueryMutationResult::default()
            })
        );
    }

    cluster
        .renew_leases(&control, std::time::Duration::from_millis(25))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    #[cfg(feature = "opencypher")]
    let expected_vertices = vec![2, 4];
    #[cfg(not(feature = "opencypher"))]
    let expected_vertices = vec![2];
    assert_eq!(
        cluster
            .execute_query_statement(
                QueryContext::new("reddit-home", "query-routed-read"),
                QueryStatement::MatchOut {
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    return_count: false,
                },
            )
            .await
            .unwrap(),
        QueryOutput::Vertices(expected_vertices)
    );

    let err = cluster
        .execute_query_statement(
            QueryContext::new("reddit-home", "query-routed-write"),
            QueryStatement::CreateEdge {
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst: 3,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GraphError::StaleShardLease {
            ref cell_id,
            ref node_id,
            ..
        } if cell_id == "reddit-home" && node_id == "node-a"
    ));
    #[cfg(feature = "opencypher")]
    {
        let err = cluster
            .execute_cypher(
                QueryContext::new("reddit-home", "query-routed-cypher-delete"),
                "MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) DELETE r",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            GraphError::StaleShardLease {
                ref cell_id,
                ref node_id,
                ..
            } if cell_id == "reddit-home" && node_id == "node-a"
        ));
    }
    cluster.close().await.unwrap();
    control.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn routed_cluster_execute_cypher_uses_row_engine_for_public_reads() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/query-routed-row-engine",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    control
        .publish_placement(&ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap())
        .await
        .unwrap();
    let cluster = RoutedGraphCluster::open_owned_with_control(
        "query-routed-row-engine",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();

    for user in [1, 3] {
        cluster
            .set_vertex_metadata(
                "reddit-home",
                user,
                VertexMetadata::default().with_label("User"),
            )
            .await
            .unwrap();
    }
    cluster
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "query-routed-row-engine-edge".to_string(),
        })
        .await
        .unwrap();

    let output = cluster
        .execute_cypher(
            QueryContext::new("reddit-home", "query-routed-row-engine-read"),
            "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v) \
             RETURN u.id AS user, v.id AS followed ORDER BY user",
        )
        .await
        .unwrap();
    assert_eq!(
        output,
        QueryOutput::Rows(QueryResultSet::new(
            vec![QueryColumn::new("user"), QueryColumn::new("followed")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(1), QueryValue::VertexId(2)]),
                QueryRow::new(vec![QueryValue::VertexId(3), QueryValue::Null]),
            ],
        ))
    );

    cluster.close().await.unwrap();
    control.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn routed_cluster_executes_row_queries_across_local_cells() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/query-routed-multi-cell",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    control
        .publish_placement(
            &ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-popular", "node-a")])
                .unwrap(),
        )
        .await
        .unwrap();
    let cluster = RoutedGraphCluster::open_owned_with_control(
        "query-routed-multi-cell",
        "node-a",
        &control,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap();

    for (cell_id, dsts) in [("reddit-home", vec![10, 11]), ("reddit-popular", vec![20])] {
        for (idx, dst) in dsts.into_iter().enumerate() {
            cluster
                .write_edge(EdgeMutation {
                    cell_id: cell_id.to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    dst,
                    idempotency_key: format!("query-routed-multi-cell-{cell_id}-{idx}"),
                })
                .await
                .unwrap();
        }
    }

    let result_sets = cluster
        .execute_cypher_rows_many(
            [
                QueryContext::new("reddit-home", "query-routed-multi-cell-home"),
                QueryContext::new("reddit-popular", "query-routed-multi-cell-popular"),
            ],
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        result_sets.get("reddit-home").unwrap(),
        &QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(11)]),
            ],
        )
    );
    assert_eq!(
        result_sets.get("reddit-popular").unwrap(),
        &QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(20)])],
        )
    );

    let page = cluster
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "query-routed-multi-cell-page"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        page,
        QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(10)])],
            Some(QueryCursorToken::new(1)),
        )
    );

    let duplicate = cluster
        .execute_cypher_rows_many(
            [
                QueryContext::new("reddit-home", "query-routed-duplicate-a"),
                QueryContext::new("reddit-home", "query-routed-duplicate-b"),
            ],
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(duplicate, GraphError::CorruptValue { .. }));

    cluster.close().await.unwrap();
    control.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn distributed_query_coordinator_routes_to_remote_cell_clients() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/query-distributed-coordinator",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    let placement =
        ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-popular", "node-b")]).unwrap();
    control.publish_placement(&placement).await.unwrap();

    let cluster_a = Arc::new(
        RoutedGraphCluster::open_owned_with_control(
            "query-distributed-coordinator",
            "node-a",
            &control,
            Arc::clone(&object_store),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );
    let cluster_b = Arc::new(
        RoutedGraphCluster::open_owned_with_control(
            "query-distributed-coordinator",
            "node-b",
            &control,
            Arc::clone(&object_store),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );

    cluster_a
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 10,
            idempotency_key: "query-distributed-home".to_string(),
        })
        .await
        .unwrap();
    cluster_b
        .write_edge(EdgeMutation {
            cell_id: "reddit-popular".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 20,
            idempotency_key: "query-distributed-popular".to_string(),
        })
        .await
        .unwrap();

    let client_a: Arc<dyn QueryCellClient> = cluster_a.clone();
    let client_b: Arc<dyn QueryCellClient> = cluster_b.clone();
    let coordinator = DistributedQueryCoordinator::new(placement)
        .with_client("node-a", client_a)
        .unwrap()
        .with_client("node-b", client_b)
        .unwrap();

    let rows = coordinator
        .execute_cypher_rows_many(
            [
                QueryContext::new("reddit-home", "query-distributed-home-read"),
                QueryContext::new("reddit-popular", "query-distributed-popular-read"),
            ],
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.get("reddit-home").unwrap(),
        &QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(10)])],
        )
    );
    assert_eq!(
        rows.get("reddit-popular").unwrap(),
        &QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(20)])],
        )
    );

    let pages = coordinator
        .execute_cypher_rows_pages(
            [
                DistributedQueryPageRequest::new(
                    QueryContext::new("reddit-home", "query-distributed-home-page"),
                    None,
                ),
                DistributedQueryPageRequest::new(
                    QueryContext::new("reddit-popular", "query-distributed-popular-page"),
                    None,
                ),
            ],
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        pages.get("reddit-home").unwrap(),
        &QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(10)])],
            None,
        )
    );
    assert_eq!(
        pages.get("reddit-popular").unwrap(),
        &QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(20)])],
            None,
        )
    );

    cluster_a.close().await.unwrap();
    cluster_b.close().await.unwrap();
    control.close().await.unwrap();
}

#[cfg(feature = "query-transport")]
#[tokio::test]
async fn tcp_query_transport_routes_distributed_cypher_pages() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/query-tcp-transport",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    let placement =
        ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-popular", "node-b")]).unwrap();
    control.publish_placement(&placement).await.unwrap();

    let cluster_a = Arc::new(
        RoutedGraphCluster::open_owned_with_control(
            "query-tcp-transport",
            "node-a",
            &control,
            Arc::clone(&object_store),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );
    let cluster_b = Arc::new(
        RoutedGraphCluster::open_owned_with_control(
            "query-tcp-transport",
            "node-b",
            &control,
            Arc::clone(&object_store),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );

    for (cluster, cell_id, dsts) in [
        (Arc::clone(&cluster_a), "reddit-home", vec![10, 11]),
        (Arc::clone(&cluster_b), "reddit-popular", vec![20]),
    ] {
        for (idx, dst) in dsts.into_iter().enumerate() {
            cluster
                .write_edge(EdgeMutation {
                    cell_id: cell_id.to_string(),
                    edge_type: "FOLLOWS".to_string(),
                    src: 1,
                    dst,
                    idempotency_key: format!("query-tcp-transport-{cell_id}-{idx}"),
                })
                .await
                .unwrap();
        }
    }

    let client_a: Arc<dyn QueryCellClient> = cluster_a.clone();
    let client_b: Arc<dyn QueryCellClient> = cluster_b.clone();
    let server_a = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        client_a,
        QueryTransportServerConfig::default().with_required_bearer_token("secret-a"),
    )
    .await
    .unwrap();
    let server_b = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        client_b,
        QueryTransportServerConfig::default().with_required_bearer_token("secret-b"),
    )
    .await
    .unwrap();
    let coordinator = DistributedQueryCoordinator::new(placement)
        .with_client(
            "node-a",
            Arc::new(TcpQueryCellClient::new(server_a.local_addr()).with_bearer_token("secret-a")),
        )
        .unwrap()
        .with_client(
            "node-b",
            Arc::new(TcpQueryCellClient::new(server_b.local_addr()).with_bearer_token("secret-b")),
        )
        .unwrap();

    let rows = coordinator
        .execute_cypher_rows_many(
            [
                QueryContext::new("reddit-home", "query-tcp-home-rows"),
                QueryContext::new("reddit-popular", "query-tcp-popular-rows"),
            ],
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.get("reddit-home").unwrap(),
        &QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![
                QueryRow::new(vec![QueryValue::VertexId(10)]),
                QueryRow::new(vec![QueryValue::VertexId(11)]),
            ],
        )
    );
    assert_eq!(
        rows.get("reddit-popular").unwrap(),
        &QueryResultSet::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(20)])],
        )
    );

    let pages = coordinator
        .execute_cypher_rows_pages(
            [
                DistributedQueryPageRequest::new(
                    QueryContext::new("reddit-home", "query-tcp-home-page"),
                    None,
                ),
                DistributedQueryPageRequest::new(
                    QueryContext::new("reddit-popular", "query-tcp-popular-page"),
                    None,
                ),
            ],
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        pages.get("reddit-home").unwrap(),
        &QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(10)])],
            Some(QueryCursorToken::new(1)),
        )
    );
    assert_eq!(
        pages.get("reddit-popular").unwrap(),
        &QueryResultPage::new(
            vec![QueryColumn::new("v.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(20)])],
            None,
        )
    );

    server_a.stop().await.unwrap();
    server_b.stop().await.unwrap();
    cluster_a.close().await.unwrap();
    cluster_b.close().await.unwrap();
    control.close().await.unwrap();
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
async fn tcp_query_transport_enforces_auth_cancellation_streaming_metrics_and_discovery() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control = GraphControlPlane::open(
        "graph-control/query-transport-hardening",
        Arc::clone(&object_store),
    )
    .await
    .unwrap();
    let placement = ShardPlacement::fixed([("reddit-home", "node-a")]).unwrap();
    control.publish_placement(&placement).await.unwrap();

    let cluster = Arc::new(
        RoutedGraphCluster::open_owned_with_control(
            "query-transport-hardening",
            "node-a",
            &control,
            Arc::clone(&object_store),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );
    for (idx, dst) in [10, 11, 12].into_iter().enumerate() {
        cluster
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "FOLLOWS".to_string(),
                src: 1,
                dst,
                idempotency_key: format!("query-transport-hardening-edge-{idx}"),
            })
            .await
            .unwrap();
    }

    let cluster_client: Arc<dyn QueryCellClient> = cluster.clone();
    let server = TcpQueryServer::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        cluster_client,
        QueryTransportServerConfig::default()
            .with_required_bearer_token("secret")
            .with_max_concurrent_requests(1)
            .with_slow_query_log_threshold(Some(std::time::Duration::ZERO)),
    )
    .await
    .unwrap();

    let unauthorized = TcpQueryCellClient::new(server.local_addr());
    let unauthorized_err = unauthorized
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-unauthorized"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
        )
        .await
        .unwrap_err();
    assert!(unauthorized_err.to_string().contains("unauthorized"));

    let mut directory = QueryServiceDirectory::new();
    directory
        .insert(
            QueryServiceEndpoint::new("node-a", server.local_addr()).with_client_config(
                QueryTransportClientConfig::default().with_bearer_token("secret"),
            ),
        )
        .unwrap();
    let coordinator =
        DistributedQueryCoordinator::from_service_directory(placement.clone(), &directory).unwrap();
    let rows = coordinator
        .execute_cypher_rows_many(
            [QueryContext::new(
                "reddit-home",
                "query-transport-discovery",
            )],
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(rows["reddit-home"].rows.len(), 3);

    let client = TcpQueryCellClient::new(server.local_addr()).with_bearer_token("secret");
    let mut stream = client.stream_cypher_rows(
        QueryContext::new("reddit-home", "query-transport-stream"),
        "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        2,
    );
    assert_eq!(
        stream.next_row().await.unwrap(),
        Some(QueryRow::new(vec![QueryValue::VertexId(10)]))
    );
    assert_eq!(
        stream.next_row().await.unwrap(),
        Some(QueryRow::new(vec![QueryValue::VertexId(11)]))
    );
    assert_eq!(
        stream.next_row().await.unwrap(),
        Some(QueryRow::new(vec![QueryValue::VertexId(12)]))
    );
    assert_eq!(stream.next_row().await.unwrap(), None);
    assert_eq!(stream.columns().unwrap(), &[QueryColumn::new("v.id")]);

    let inactive_cancel = client
        .cancel_query("query-transport-cancelled")
        .await
        .unwrap_err();
    assert!(inactive_cancel
        .to_string()
        .contains("no active query with id query-transport-cancelled was cancelled"));
    let reused_after_precancel = client
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "query-transport-cancelled"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_eq!(reused_after_precancel.rows.len(), 3);

    let server_metrics = server.metrics();
    assert!(server_metrics.auth_failures >= 1);
    assert_eq!(server_metrics.cancelled_rejections, 0);
    assert!(server_metrics.slow_queries >= 1);
    assert!(server_metrics.bytes_received > 0);
    assert!(server_metrics.bytes_sent > 0);
    let client_metrics = client.metrics();
    assert!(client_metrics.bytes_received > 0);
    assert!(client_metrics.bytes_sent > 0);

    server.stop().await.unwrap();
    cluster.close().await.unwrap();
    control.close().await.unwrap();
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
            .with_max_concurrent_requests(1),
    )
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for idx in 0..6 {
        let client = TcpQueryCellClient::new(server.local_addr()).with_bearer_token("secret");
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

    let client = TcpQueryCellClient::new(server.local_addr()).with_bearer_token("secret");
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
#[tokio::test]
async fn cypher_float_properties_roundtrip_index_compare_and_order() {
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
async fn structural_edge_delete_tombstones_relationships_before_recreate() {
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
async fn batch_structural_edge_delete_tombstones_relationships_before_recreate() {
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
async fn leased_drop_cell_replays_after_write_fence_is_deleted() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let lease = ShardLease {
        cell_id: "reddit-home".to_string(),
        owner_node_id: "node-a".to_string(),
        lease_token: 1,
        expires_at_ms: graph_now_millis() + 60_000,
    };
    let leases = Arc::new(RwLock::new(BTreeMap::from([(
        lease.cell_id.clone(),
        lease.clone(),
    )])));
    let shard = GraphShard::open_leased_writer(
        "graph/drop-cell-leased-replay",
        Arc::clone(&object_store),
        GraphOpenOptions::default(),
        "node-a".to_string(),
        Arc::clone(&leases),
    )
    .await
    .unwrap();
    shard
        .install_write_fence("reddit-home", &lease)
        .await
        .unwrap();
    shard
        .write_edge(mutation(1, 2, "leased-drop-seed"))
        .await
        .unwrap();

    let dropped = shard.drop_cell("reddit-home", "leased-drop").await.unwrap();
    assert!(!dropped.already_dropped);
    assert!(shard
        .read_remote(&keys::write_fence("reddit-home"))
        .await
        .unwrap()
        .is_none());

    let replay = shard.drop_cell("reddit-home", "leased-drop").await.unwrap();
    assert_eq!(replay, dropped);

    let already = shard
        .drop_cell("reddit-home", "leased-drop-again")
        .await
        .unwrap();
    assert_eq!(already.marker_epoch, dropped.marker_epoch);
    assert_eq!(already.deleted_keys, 0);
    assert!(already.already_dropped);
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
async fn drop_cell_waits_for_active_read_leases_before_deleting_data() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/drop-cell-active-reader",
        object_store,
        GraphOpenOptions {
            retention_policy: GraphRetentionPolicy {
                read_lease_ttl_ms: 25,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

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
    drop(snapshot);
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
async fn reimported_relationship_id_same_edge_supersedes_tombstone() {
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
    assert_eq!(
        first,
        QueryOutput::Write(CommitResult {
            epoch: 1,
            already_existed: false
        })
    );
    assert_eq!(
        second,
        QueryOutput::Write(CommitResult {
            epoch: 2,
            already_existed: false
        })
    );
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
    struct StaticRowsClient {
        value: VertexId,
    }

    #[async_trait::async_trait]
    impl QueryCellClient for StaticRowsClient {
        async fn execute_cypher_rows(
            &self,
            _context: QueryContext,
            _query: &str,
        ) -> Result<QueryResultSet> {
            Ok(QueryResultSet::new(
                vec![QueryColumn::new("id")],
                vec![QueryRow::new(vec![QueryValue::VertexId(self.value)])],
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

    let placement = ShardPlacement::fixed([("cell-z", "node-z"), ("cell-a", "node-a")]).unwrap();
    let coordinator = DistributedQueryCoordinator::new(placement)
        .with_client("node-z", Arc::new(StaticRowsClient { value: 2 }))
        .unwrap()
        .with_client("node-a", Arc::new(StaticRowsClient { value: 1 }))
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

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn distributed_query_plan_joins_results_across_cells() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control =
        GraphControlPlane::open("graph-control/query-distributed-join", object_store.clone())
            .await
            .unwrap();
    let placement =
        ShardPlacement::fixed([("reddit-users", "node-a"), ("reddit-posts", "node-b")]).unwrap();
    control.publish_placement(&placement).await.unwrap();

    let cluster_a = Arc::new(
        RoutedGraphCluster::open_owned_with_control(
            "query-distributed-join",
            "node-a",
            &control,
            object_store.clone(),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );
    let cluster_b = Arc::new(
        RoutedGraphCluster::open_owned_with_control(
            "query-distributed-join",
            "node-b",
            &control,
            object_store,
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );

    for (vertex, name) in [(1, "alice"), (2, "bob")] {
        cluster_a
            .set_vertex_metadata(
                "reddit-users",
                vertex,
                VertexMetadata::default()
                    .with_label("User")
                    .with_property("uid", VertexPropertyValue::Integer(vertex))
                    .with_property("name", VertexPropertyValue::String(name.to_string())),
            )
            .await
            .unwrap();
    }
    for (post, author) in [(100, 1), (101, 1), (102, 2)] {
        cluster_b
            .set_vertex_metadata(
                "reddit-posts",
                post,
                VertexMetadata::default()
                    .with_label("Post")
                    .with_property("author_id", VertexPropertyValue::Integer(author)),
            )
            .await
            .unwrap();
    }

    let client_a: Arc<dyn QueryCellClient> = cluster_a.clone();
    let client_b: Arc<dyn QueryCellClient> = cluster_b.clone();
    let coordinator = DistributedQueryCoordinator::new(placement)
        .with_client("node-a", client_a)
        .unwrap()
        .with_client("node-b", client_b)
        .unwrap();
    let plan = DistributedQueryPlan::inner_join(
        vec![
            DistributedQueryLeg::new(
                "users",
                QueryContext::new("reddit-users", "distributed-join-users"),
                "MATCH (u:User) RETURN u.uid AS user, u.name AS name ORDER BY user",
            )
            .unwrap()
            .with_estimated_rows(2),
            DistributedQueryLeg::new(
                "posts",
                QueryContext::new("reddit-posts", "distributed-join-posts"),
                "MATCH (p:Post) RETURN p.author_id AS user, p.id AS post ORDER BY post",
            )
            .unwrap()
            .with_estimated_rows(3),
        ],
        DistributedQueryJoin::inner("users", "user", "posts", "user"),
    );
    let result = coordinator
        .execute_distributed_query_plan(plan)
        .await
        .unwrap();
    assert_eq!(
        result.merged.columns,
        vec![
            QueryColumn::new("users.user"),
            QueryColumn::new("users.name"),
            QueryColumn::new("posts.user"),
            QueryColumn::new("posts.post"),
        ]
    );
    assert_eq!(result.merged.rows.len(), 3);
    assert!(result.merged.rows.iter().any(|row| {
        row.values
            == vec![
                QueryValue::Property(VertexPropertyValue::Integer(1)),
                QueryValue::Property(VertexPropertyValue::String("alice".to_string())),
                QueryValue::Property(VertexPropertyValue::Integer(1)),
                QueryValue::VertexId(100),
            ]
    }));

    cluster_a.close().await.unwrap();
    cluster_b.close().await.unwrap();
    control.close().await.unwrap();
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
        let control =
            GraphControlPlane::open("graph-control/query-child-process", object_store.clone())
                .await
                .unwrap();
        let placement = ShardPlacement::fixed([("reddit-home", "node-child")]).unwrap();
        control.publish_placement(&placement).await.unwrap();
        let cluster = Arc::new(
            RoutedGraphCluster::open_owned_with_control(
                "query-child-process",
                "node-child",
                &control,
                object_store,
                std::time::Duration::from_secs(60),
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
            QueryTransportServerConfig::default().with_required_bearer_token(token),
        )
        .await
        .unwrap();
        std::fs::write(&ready_path, b"ready").unwrap();
        while !stop_path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        server.stop().await.unwrap();
        cluster.close().await.unwrap();
        control.close().await.unwrap();
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

    let client = TcpQueryCellClient::new(addr).with_bearer_token("child-secret");
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
async fn cypher_explain_uses_query_planner() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/cypher-explain-plan", object_store).await;
    let plan = shard
        .explain_cypher(
            QueryContext::new("reddit-home", "cypher-explain").at_epoch(7),
            "MATCH (u {id: 10})-[:FOLLOWS]->(v) RETURN count(*)",
        )
        .unwrap();
    assert_eq!(plan.read_epoch, Some(7));
    assert_eq!(
        plan.logical,
        LogicalQueryPlan::MatchOut {
            edge_type: "FOLLOWS".to_string(),
            src: 10,
            return_count: true,
        }
    );
    assert_eq!(
        plan.physical,
        PhysicalQueryPlan::OutDegreeCounter {
            edge_type: "FOLLOWS".to_string(),
            src: 10,
        }
    );
    assert!(!plan.is_write());
    shard.close().await.unwrap();
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
    assert_eq!(
        write,
        QueryOutput::Write(CommitResult {
            epoch: 1,
            already_existed: false
        })
    );

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

    let plan = shard
        .explain_cypher(
            QueryContext::new("reddit-home", "cypher-create-metadata-plan"),
            "CREATE (u:User {id: 10, name: 'alice', active: true})-[:FOLLOWS]->\
             (v:User {id: 20, name: 'bob', age: 42})",
        )
        .unwrap();
    assert_eq!(
        plan.physical,
        PhysicalQueryPlan::WriteEdgeWithMetadata {
            edge_type: "FOLLOWS".to_string(),
            src: 10,
            dst: 20,
            src_metadata: VertexMetadata::default()
                .with_label("User")
                .with_property("active", VertexPropertyValue::Bool(true))
                .with_property("name", VertexPropertyValue::String("alice".to_string())),
            dst_metadata: VertexMetadata::default()
                .with_label("User")
                .with_property("age", VertexPropertyValue::Integer(42))
                .with_property("name", VertexPropertyValue::String("bob".to_string())),
        }
    );

    let query = "CREATE (u:User {id: 10, name: 'alice', active: true})-[:FOLLOWS]->\
                 (v:User {id: 20, name: 'bob', age: 42})";
    let write = shard
        .execute_cypher(
            QueryContext::new("reddit-home", "cypher-create-metadata"),
            query,
        )
        .await
        .unwrap();
    assert_eq!(
        write,
        QueryOutput::Write(CommitResult {
            epoch: 1,
            already_existed: false
        })
    );

    let rows_at_commit = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-create-metadata-read").at_epoch(1),
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
    assert_eq!(
        metadata_merge,
        QueryOutput::Write(CommitResult {
            epoch: 2,
            already_existed: false
        })
    );
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
    assert_eq!(
        write,
        QueryOutput::Write(CommitResult {
            epoch: 1,
            already_existed: false
        })
    );

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

    let plan = shard
        .explain_cypher(
            QueryContext::new("reddit-home", "cypher-window-plan"),
            "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id SKIP 2 - 1 LIMIT 1 + 1",
        )
        .unwrap();
    assert_eq!(
        plan.result_window,
        QueryWindow {
            skip: 1,
            limit: Some(2),
        }
    );

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
async fn cypher_rows_page_streams_bound_neighbor_windows_without_row_materialization() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = GraphShard::open_standalone_writer_with_options(
        "graph/cypher-row-page-streaming",
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

    for dst in 10..16 {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "USER_FOLLOWS_USER".to_string(),
                src: 100,
                dst,
                idempotency_key: format!("cypher-row-page-streaming-{dst}"),
            })
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    shard
        .build_supernode_groups("reddit-home", "USER_FOLLOWS_USER", base_epoch, 4, 2)
        .await
        .unwrap();

    let first = shard
        .execute_cypher_rows_page(
            QueryContext::new("reddit-home", "cypher-row-page-streaming-first"),
            "MATCH (u {id: 100})-[:USER_FOLLOWS_USER]->(v) \
             RETURN v.id ORDER BY v.id",
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
            QueryContext::new("reddit-home", "cypher-row-page-streaming-second"),
            "MATCH (u {id: 100})-[:USER_FOLLOWS_USER]->(v) \
             RETURN v.id ORDER BY v.id",
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
            Some(QueryCursorToken::new(4)),
        )
    );

    let metrics = shard.graph_operational_metrics();
    assert_eq!(metrics.query_rows_started, 2);
    assert_eq!(metrics.query_rows_completed, 2);
    assert_eq!(metrics.query_rows_returned, 4);
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
async fn cypher_variable_hops_use_matrix_artifact_adjacency() {
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
    assert!(parsed_metrics.reachability_result_misses >= 1);

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
    let first_metrics = shard.graph_cache_metrics();
    assert!(first_metrics.reachability_result_hits > parsed_metrics.reachability_result_hits);
    #[cfg(feature = "graphblas")]
    assert!(first_metrics.graphblas_hits > before_metrics.graphblas_hits);
    #[cfg(not(feature = "graphblas"))]
    assert!(
        first_metrics.matrix_adjacency_hits > before_metrics.matrix_adjacency_hits
            || first_metrics.matrix_adjacency_misses > before_metrics.matrix_adjacency_misses
    );

    shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-varhop-matrix-artifact-hot"),
            "MATCH (u {id: 1})-[:CHAIN*1..2]->(v) RETURN v.id",
        )
        .await
        .unwrap();
    let hot_metrics = shard.graph_cache_metrics();
    #[cfg(feature = "graphblas")]
    assert!(hot_metrics.graphblas_hits > first_metrics.graphblas_hits);
    #[cfg(not(feature = "graphblas"))]
    assert!(hot_metrics.matrix_adjacency_hits > first_metrics.matrix_adjacency_hits);

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
#[tokio::test]
async fn cypher_relationship_properties_are_indexed_mutable_and_snapshot_safe() {
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
        QueryOutput::Write(CommitResult {
            already_existed: false,
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
        .unwrap();
    assert_eq!(
        old_index_snapshot,
        QueryResultSet::new(
            vec![QueryColumn::new("since")],
            vec![QueryRow::new(vec![QueryValue::Property(
                VertexPropertyValue::Integer(2020)
            )])],
        )
    );

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
    assert!(matches!(
        err,
        GraphError::AdmissionRejected {
            operation: "cypher_edge_full_scan" | "query_outbox_delta_records",
            actual: 2,
            limit: 1
        }
    ));
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

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-dst-id-index-read"),
            "MATCH (u)-[:FOLLOWS]->(v {id: 20}) RETURN u.id",
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
async fn cypher_edge_property_index_is_not_planned_for_historical_snapshots() {
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
        .unwrap();
    assert_eq!(
        plan.groups[0].patterns[0].access,
        RowQueryAccess::FullEdgeScan {
            edge_type: "FOLLOWS".to_string(),
        }
    );

    let rows = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "cypher-edge-property-index-snapshot-read")
                .at_epoch(pinned_epoch),
            "MATCH ()-[e:FOLLOWS {weight: 7}]->() RETURN count(*)",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("count(*)")],
            vec![QueryRow::new(vec![QueryValue::Count(0)])],
        )
    );
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
async fn destination_reverse_expansion_preserves_historical_snapshot_reads() {
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
        .unwrap();
    assert_eq!(
        rows,
        QueryResultSet::new(
            vec![QueryColumn::new("u.id")],
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])],
        )
    );
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
async fn cypher_vertex_metadata_reads_are_snapshot_consistent() {
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
        .unwrap();
    assert_eq!(
        old_snapshot,
        QueryResultSet::new(vec![QueryColumn::new("v.id")], Vec::new())
    );

    let old_property = shard
        .execute_cypher_rows(
            QueryContext::new("reddit-home", "metadata-snapshot-old-property")
                .at_epoch(pinned_epoch),
            "MATCH (u:User {active: true})-[:FOLLOWS]->(v:User) \
             WHERE v.age = 17 RETURN u.name AS src, v.age AS age",
        )
        .await
        .unwrap();
    assert_eq!(
        old_property,
        QueryResultSet::new(
            vec![QueryColumn::new("src"), QueryColumn::new("age")],
            vec![QueryRow::new(vec![
                QueryValue::Property(VertexPropertyValue::String("alice".to_string())),
                QueryValue::Property(VertexPropertyValue::Integer(17)),
            ])],
        )
    );

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
async fn edge_writes_publish_delta_plus_records_for_builders() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/delta-plus", object_store).await;

    shard.write_edge(mutation(1, 10, "req-1")).await.unwrap();
    shard.write_edge(mutation(1, 11, "req-2")).await.unwrap();

    let deltas = shard
        .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
        .await
        .unwrap();
    assert_eq!(
        deltas,
        vec![
            DeltaRecord {
                kind: DeltaKind::Plus,
                edge: EdgeRecord {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                    src: 1,
                    dst: 10,
                    epoch: 1
                }
            },
            DeltaRecord {
                kind: DeltaKind::Plus,
                edge: EdgeRecord {
                    cell_id: "reddit-home".to_string(),
                    edge_type: "USER_SUBSCRIBED_TO_SUBREDDIT".to_string(),
                    src: 1,
                    dst: 11,
                    epoch: 2
                }
            }
        ]
    );

    let after_first = shard
        .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 1)
        .await
        .unwrap();
    assert_eq!(after_first.len(), 1);
    assert_eq!(after_first[0].edge.dst, 11);
}

#[tokio::test]
async fn reopened_reader_sees_delta_plus_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "graph/reopen-deltas";

    {
        let shard = open_test_shard(path, Arc::clone(&object_store)).await;
        shard.write_edge(mutation(42, 84, "req-1")).await.unwrap();
        shard.close().await.unwrap();
    }

    let reopened = open_test_shard(path, object_store).await;
    let deltas = reopened
        .deltas_since("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", 0)
        .await
        .unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].kind, DeltaKind::Plus);
    assert_eq!(deltas[0].edge.src, 42);
    assert_eq!(deltas[0].edge.dst, 84);
}

#[tokio::test]
async fn posting_and_matrix_artifacts_apply_delta_overlay_for_hot_hops() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/matrix-overlay", object_store).await;

    for (idx, (src, dst)) in [(1, 2), (1, 3), (2, 4), (3, 4), (4, 5)]
        .into_iter()
        .enumerate()
    {
        shard
            .write_edge(mutation(src, dst, &format!("base-{idx}")))
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();

    let chunks = shard
        .build_posting_chunks("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
        .await
        .unwrap();
    assert!(chunks.iter().any(|chunk| {
        chunk.direction == ArtifactDirection::Out
            && chunk.owner == 1
            && chunk.vertices == vec![2, 3]
    }));

    let artifact = shard
        .build_matrix_tiles("reddit-home", "USER_SUBSCRIBED_TO_SUBREDDIT", base_epoch, 2)
        .await
        .unwrap();
    assert_eq!(artifact.edge_count, 5);
    assert!(artifact.out_tiles > 0);
    assert!(artifact.transpose_tiles > 0);

    shard
        .write_edge(mutation(4, 6, "delta-plus"))
        .await
        .unwrap();
    shard
        .delete_edge(mutation(3, 4, "delta-minus"))
        .await
        .unwrap();
    let read_epoch = shard.current_epoch("reddit-home").await.unwrap();

    let posting = shard
        .posting_reachable(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1],
            3,
            read_epoch,
        )
        .await
        .unwrap();
    let matrix = shard
        .matrix_reachable(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1],
            3,
            read_epoch,
        )
        .await
        .unwrap();
    assert_eq!(matrix.base_epoch, base_epoch);
    assert_eq!(matrix.delta_records_applied, 2);
    assert_eq!(matrix.vertices, vec![2, 3, 4, 5, 6]);
    assert_eq!(posting.vertices, matrix.vertices);

    let bench = shard
        .benchmark_hot_hops(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1],
            3,
            read_epoch,
        )
        .await
        .unwrap();
    assert!(bench.matrix_wins);
    assert_eq!(bench.matrix.vertices, bench.posting.vertices);
    assert!(bench.matrix.delta_records_applied < bench.posting.delta_records_applied);
}

#[cfg(feature = "graphblas")]
#[tokio::test]
async fn graphblas_matrix_kernel_matches_rust_kernel_after_delta_overlay() {
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
        .write_edge(mutation(4, 6, "graphblas-delta-plus"))
        .await
        .unwrap();
    shard
        .delete_edge(mutation(3, 4, "graphblas-delta-minus"))
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
            SparseKernelBackend::RustSparse,
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
            SparseKernelBackend::SuiteSparseGraphBlas,
        )
        .await
        .unwrap();

    assert_eq!(
        graphblas.sparse_kernel,
        SparseKernelBackend::SuiteSparseGraphBlas
    );
    assert_eq!(graphblas.vertices, rust.vertices);
    assert_eq!(graphblas.edge_visits, rust.edge_visits);
    assert_eq!(graphblas.delta_records_applied, rust.delta_records_applied);
}

#[cfg(feature = "graphblas")]
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
    let first = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            base_epoch,
            SparseKernelBackend::SuiteSparseGraphBlas,
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
            SparseKernelBackend::SuiteSparseGraphBlas,
        )
        .await
        .unwrap();
    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
    assert_eq!(second.vertices, first.vertices);
    assert_eq!(second.delta_records_applied, 0);

    shard
        .write_edge(mutation(4, 6, "graphblas-cache-delta-plus"))
        .await
        .unwrap();
    let read_epoch = shard.current_epoch("reddit-home").await.unwrap();
    let with_delta = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_SUBSCRIBED_TO_SUBREDDIT",
            &[1, 42],
            3,
            read_epoch,
            SparseKernelBackend::SuiteSparseGraphBlas,
        )
        .await
        .unwrap();
    assert_eq!(with_delta.delta_records_applied, 1);
    assert_eq!(shard.graphblas_cache.lock().await.len(), 1);
}

#[cfg(feature = "graphblas")]
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
            SparseKernelBackend::SuiteSparseGraphBlas,
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
            SparseKernelBackend::SuiteSparseGraphBlas,
        )
        .await
        .unwrap();
    assert_eq!(actual.vertices, expected.vertices);
    assert_eq!(actual.edge_visits, expected.edge_visits);
    assert_eq!(actual.delta_records_applied, 0);
    assert_eq!(reader.graphblas_cache.lock().await.len(), 1);
    assert_eq!(reader.matrix_cache.lock().await.len(), 0);
}

#[tokio::test]
async fn supernode_groups_count_exists_intersect_and_page_without_full_scan() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = open_test_shard("graph/supernode", object_store).await;

    for dst in 10..16 {
        shard
            .write_edge(EdgeMutation {
                cell_id: "reddit-home".to_string(),
                edge_type: "USER_FOLLOWS_USER".to_string(),
                src: 100,
                dst,
                idempotency_key: format!("follow-{dst}"),
            })
            .await
            .unwrap();
    }
    let base_epoch = shard.current_epoch("reddit-home").await.unwrap();
    let groups = shard
        .build_supernode_groups("reddit-home", "USER_FOLLOWS_USER", base_epoch, 4, 2)
        .await
        .unwrap();
    let group = groups
        .iter()
        .find(|group| group.direction == ArtifactDirection::Out && group.vertex_id == 100)
        .unwrap();
    assert_eq!(group.degree, 6);
    assert_eq!(group.chunk_count, 3);
    assert_eq!(group.page_size, 2);
    assert_eq!(
        group
            .chunk_bounds
            .iter()
            .map(|bound| (bound.chunk_id, bound.first, bound.last))
            .collect::<Vec<_>>(),
        vec![(0, 10, 11), (1, 12, 13), (2, 14, 15)]
    );

    assert_eq!(
        shard
            .supernode_degree("reddit-home", "USER_FOLLOWS_USER", 100, base_epoch)
            .await
            .unwrap(),
        6
    );
    assert!(shard
        .supernode_edge_exists("reddit-home", "USER_FOLLOWS_USER", 100, 14, base_epoch)
        .await
        .unwrap());
    let one_hop = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_FOLLOWS_USER",
            &[100],
            1,
            base_epoch,
            SparseKernelBackend::SuiteSparseGraphBlas,
        )
        .await
        .unwrap();
    assert_eq!(one_hop.vertices, vec![10, 11, 12, 13, 14, 15]);
    assert_eq!(one_hop.edge_visits, 6);
    assert_eq!(
        shard
            .graph_cache_entry_counts()
            .await
            .materialized_supernodes,
        1
    );
    assert!(shard.graph_cache_metrics().materialized_supernode_misses >= 1);

    let one_hop_cached = shard
        .matrix_reachable_with_kernel(
            "reddit-home",
            "USER_FOLLOWS_USER",
            &[100],
            1,
            base_epoch,
            SparseKernelBackend::SuiteSparseGraphBlas,
        )
        .await
        .unwrap();
    assert_eq!(one_hop_cached.vertices, one_hop.vertices);
    assert!(shard.graph_cache_metrics().materialized_supernode_hits >= 1);
    assert_eq!(
        shard
            .supernode_intersection(
                "reddit-home",
                "USER_FOLLOWS_USER",
                100,
                &[11, 14, 99],
                base_epoch,
            )
            .await
            .unwrap(),
        vec![11, 14]
    );

    let page = shard
        .supernode_page(
            "reddit-home",
            "USER_FOLLOWS_USER",
            ArtifactDirection::Out,
            100,
            base_epoch,
            0,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.vertices, vec![10, 11]);
    assert!(page.has_next);
    assert_eq!(
        shard
            .execute_query_statement(
                QueryContext::new("reddit-home", "supernode-query-window-base")
                    .at_epoch(base_epoch)
                    .with_result_window(2, Some(2)),
                QueryStatement::MatchOut {
                    edge_type: "USER_FOLLOWS_USER".to_string(),
                    src: 100,
                    return_count: false,
                },
            )
            .await
            .unwrap(),
        QueryOutput::Vertices(vec![12, 13])
    );

    shard
        .write_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "USER_FOLLOWS_USER".to_string(),
            src: 100,
            dst: 16,
            idempotency_key: "follow-16".to_string(),
        })
        .await
        .unwrap();
    shard
        .delete_edge(EdgeMutation {
            cell_id: "reddit-home".to_string(),
            edge_type: "USER_FOLLOWS_USER".to_string(),
            src: 100,
            dst: 11,
            idempotency_key: "unfollow-11".to_string(),
        })
        .await
        .unwrap();
    let read_epoch = shard.current_epoch("reddit-home").await.unwrap();

    assert_eq!(
        shard
            .supernode_degree("reddit-home", "USER_FOLLOWS_USER", 100, read_epoch)
            .await
            .unwrap(),
        6
    );
    assert!(!shard
        .supernode_edge_exists("reddit-home", "USER_FOLLOWS_USER", 100, 11, read_epoch)
        .await
        .unwrap());
    assert!(shard
        .supernode_edge_exists("reddit-home", "USER_FOLLOWS_USER", 100, 16, read_epoch)
        .await
        .unwrap());
    assert_eq!(
        shard
            .supernode_intersection(
                "reddit-home",
                "USER_FOLLOWS_USER",
                100,
                &[11, 14, 16],
                read_epoch,
            )
            .await
            .unwrap(),
        vec![14, 16]
    );

    let current_page_0 = shard
        .supernode_page(
            "reddit-home",
            "USER_FOLLOWS_USER",
            ArtifactDirection::Out,
            100,
            read_epoch,
            0,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current_page_0.vertices, vec![10, 12]);
    assert!(current_page_0.has_next);
    assert_eq!(
        shard
            .execute_query_statement(
                QueryContext::new("reddit-home", "supernode-query-window-overlay")
                    .at_epoch(read_epoch)
                    .with_result_window(1, Some(3)),
                QueryStatement::MatchOut {
                    edge_type: "USER_FOLLOWS_USER".to_string(),
                    src: 100,
                    return_count: false,
                },
            )
            .await
            .unwrap(),
        QueryOutput::Vertices(vec![12, 13, 14])
    );

    let current_page_2 = shard
        .supernode_page(
            "reddit-home",
            "USER_FOLLOWS_USER",
            ArtifactDirection::Out,
            100,
            read_epoch,
            2,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current_page_2.vertices, vec![15, 16]);
    assert!(!current_page_2.has_next);
}
