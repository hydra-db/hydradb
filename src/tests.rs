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
async fn phase0_cluster_runs_multiple_local_shards_on_one_object_store() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cluster = Phase0Cluster::open_cells_standalone_writers(
        "phase0-cluster",
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
async fn routed_cluster_rejects_writes_for_non_owned_cells() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let placement =
        ShardPlacement::fixed([("reddit-home", "node-a"), ("reddit-search", "node-b")]).unwrap();
    let cluster =
        RoutedPhase0Cluster::open_owned("phase0-routed-cluster", "node-a", placement, object_store)
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

    let mut cluster = RoutedPhase0Cluster::open_owned_with_control(
        "phase0-control-cluster",
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
    let cluster = RoutedPhase0Cluster::open_owned_with_control(
        "phase0-renewer-cluster",
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
        "phase0-graph-node",
        "node-a",
        Arc::clone(&control),
        object_store,
        std::time::Duration::from_secs(2),
        std::time::Duration::from_millis(25),
    )
    .await
    .unwrap();
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
async fn phase0_cluster_reopens_many_shards_from_local_object_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let cells = ["cell-a", "cell-b", "cell-c", "cell-d"];
    let edge_type = "FOLLOWS";

    {
        let object_store = local_object_store(tempdir.path()).unwrap();
        let cluster = Phase0Cluster::open_cells_standalone_writers(
            "phase0-local-cluster",
            cells,
            object_store,
        )
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
    let reopened = Phase0Cluster::open_cells("phase0-local-cluster", cells, object_store)
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
async fn env_object_store_loader_supports_phase0_harness() {
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
fn locality_cell_extractor_covers_phase0_keyspace() {
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
        experiment.recommended_phase0_layout,
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
