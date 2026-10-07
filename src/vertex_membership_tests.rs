use super::*;

fn membership_options() -> GraphOpenOptions {
    GraphOpenOptions {
        unstable_canonical_vertex_membership: true,
        ..Default::default()
    }
}

async fn canonical_shard(path: &str) -> GraphShard {
    GraphShard::open_standalone_writer_with_options(
        path,
        Arc::new(InMemory::new()),
        membership_options(),
    )
    .await
    .unwrap()
}

async fn activate(shard: &GraphShard, cell: &str) {
    assert!(
        shard
            .unstable_backfill_vertex_membership(cell, VertexMembershipBackfillOptions::default())
            .await
            .unwrap()
            .complete
    );
}

async fn present(shard: &GraphShard, cell: &str, id: VertexId) -> bool {
    shard
        .vertex_metadata_if_exists(cell, id)
        .await
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn empty_metadata_is_presence_and_only_explicit_deletion_removes_it() {
    let shard = canonical_shard("membership/empty").await;
    activate(&shard, "a").await;
    assert!(!present(&shard, "a", 1).await);
    shard
        .set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .unwrap();
    assert!(present(&shard, "a", 1).await);
    let sequence = shard.current_epoch("a").await.unwrap();
    shard
        .set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .unwrap();
    assert_eq!(shard.current_epoch("a").await.unwrap(), sequence);
    shard
        .set_vertex_metadata(
            "a",
            1,
            VertexMetadata::default()
                .with_label("User")
                .with_property("name", VertexPropertyValue::String("one".into())),
        )
        .await
        .unwrap();
    shard
        .set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .unwrap();
    assert!(present(&shard, "a", 1).await);
    assert!(!present(&shard, "b", 1).await);
    let deleted = shard.delete_vertex("a", 1, "delete").await.unwrap();
    assert!(deleted.vertex_deleted);
    assert!(!present(&shard, "a", 1).await);
    assert_eq!(
        shard.delete_vertex("a", 1, "delete").await.unwrap(),
        deleted
    );
    assert!(
        !shard
            .delete_vertex("a", 1, "delete-again")
            .await
            .unwrap()
            .vertex_deleted
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn empty_set_merge_and_import_batches_create_records_and_remain_idempotent() {
    let shard = canonical_shard("membership/batches").await;
    assert_eq!(
        shard
            .set_vertex_metadata_batch("a", [(1, VertexMetadata::default())])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        shard
            .set_vertex_metadata_batch("a", [(1, VertexMetadata::default())])
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        shard
            .merge_vertex_metadata_batch("a", [(2, VertexMetadata::default())], None)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        shard
            .merge_vertex_metadata_batch("a", [(2, VertexMetadata::default())], None)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        shard
            .import_vertex_metadata_batch("a", [(3, VertexMetadata::default())])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        shard
            .import_vertex_metadata_batch("a", [(3, VertexMetadata::default())])
            .await
            .unwrap(),
        0
    );
    for id in 1..=3 {
        assert!(present(&shard, "a", id).await);
    }
    shard.close().await.unwrap();
}

#[tokio::test]
async fn native_edges_keep_endpoints_after_last_edge_and_detach_is_atomic() {
    let shard = canonical_shard("membership/edges").await;
    activate(&shard, "a").await;
    let before = shard.db.snapshot().await.unwrap();
    let edge = typed_mutation("a", "R", 10, 20, "edge");
    shard.write_edge(edge.clone()).await.unwrap();
    for id in [10, 20] {
        assert!(present(&shard, "a", id).await);
    }
    GraphStore::scope_snapshot(before.clone(), async {
        assert!(!present(&shard, "a", 10).await);
        assert!(!shard.edge_exists("a", "R", 10, 20).await.unwrap());
    })
    .await;
    assert!(shard.delete_vertex("a", 10, "connected").await.is_err());
    shard
        .delete_edge(typed_mutation("a", "R", 10, 20, "remove-edge"))
        .await
        .unwrap();
    for id in [10, 20] {
        assert!(present(&shard, "a", id).await);
    }
    shard
        .write_edge(typed_mutation("a", "R", 10, 20, "recreate"))
        .await
        .unwrap();
    shard
        .write_edge(typed_mutation("a", "S", 10, 10, "loop"))
        .await
        .unwrap();
    let live = shard.db.snapshot().await.unwrap();
    let graph_snapshot = shard.snapshot("a").await.unwrap();
    let deletion = shard.detach_delete_vertex("a", 10, "detach").await.unwrap();
    assert!(deletion.vertex_deleted);
    assert_eq!(deletion.incident_edges_deleted, 2);
    assert!(!present(&shard, "a", 10).await);
    assert!(graph_snapshot
        .vertex_metadata_if_exists(10)
        .await
        .unwrap()
        .is_some());
    assert!(present(&shard, "a", 20).await);
    GraphStore::scope_snapshot(live.clone(), async {
        assert!(present(&shard, "a", 10).await);
        assert!(shard.edge_exists("a", "R", 10, 20).await.unwrap());
    })
    .await;
    // Replaying a successful old edge write cannot recreate deleted endpoints.
    shard.write_edge(edge).await.unwrap();
    assert!(!present(&shard, "a", 10).await);
    drop(before);
    drop(live);
    drop(graph_snapshot);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn bulk_relationship_and_segment_writers_establish_empty_endpoints() {
    for (case, policy) in [GraphIndexPolicy::Full, GraphIndexPolicy::OutboundOnly]
        .into_iter()
        .enumerate()
    {
        let shard = GraphShard::open_standalone_writer_with_options(
            format!("membership/import-{case}"),
            Arc::new(InMemory::new()),
            GraphOpenOptions {
                index_policy: policy,
                ..membership_options()
            },
        )
        .await
        .unwrap();
        shard
            .bulk_import_edges("a", "R", [(1, 2), (2, 3)], "bulk")
            .await
            .unwrap();
        shard
            .bulk_append_edges_trusted("a", "S", [(4, 5), (4, 6)], "trusted")
            .await
            .unwrap();
        shard
            .write_edge_mutations_batch("a", [typed_mutation("a", "T", 7, 8, "batch")])
            .await
            .unwrap();
        shard
            .create_relationship(
                typed_mutation("a", "U", 9, 10, "rel"),
                EdgeMetadata::default(),
            )
            .await
            .unwrap();
        shard
            .import_relationships_batch(
                "a",
                "V",
                [RelationshipMutation {
                    cell_id: "a".into(),
                    edge_type: "V".into(),
                    src: 11,
                    dst: 12,
                    relationship_id: 90,
                    metadata: EdgeMetadata::default(),
                }],
                "rels",
            )
            .await
            .unwrap();
        if policy == GraphIndexPolicy::OutboundOnly {
            shard
                .bulk_append_out_adjacency_segment_trusted("a", "W", 13, [14, 15], "segment")
                .await
                .unwrap();
            for id in 13..=15 {
                assert!(present(&shard, "a", id).await);
            }
        }
        for id in 1..=12 {
            assert!(present(&shard, "a", id).await, "missing endpoint {id}");
        }
        shard.close().await.unwrap();
    }
}

#[tokio::test]
async fn late_endpoint_corruption_rolls_back_edge_and_other_endpoint() {
    let shard = canonical_shard("membership/rollback").await;
    let mut corrupt = WriteBatch::new();
    corrupt.put(keys::vertex("a", 2).as_bytes(), b"bad".as_slice());
    shard.write_strict_for_test(corrupt).await.unwrap();
    assert!(matches!(
        shard
            .write_edge(typed_mutation("a", "R", 1, 2, "edge"))
            .await,
        Err(GraphError::CorruptValue { .. })
    ));
    assert!(!present(&shard, "a", 1).await);
    assert!(!shard.edge_exists("a", "R", 1, 2).await.unwrap());
    assert!(matches!(
        shard.vertex_metadata_if_exists("a", 2).await,
        Err(GraphError::CorruptValue { .. })
    ));
    shard.close().await.unwrap();
}

#[tokio::test]
async fn backfill_resumes_across_reopen_and_resolves_segment_tombstones() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "membership/migration";
    let legacy = GraphShard::open_standalone_writer_with_options(
        path,
        store.clone(),
        GraphOpenOptions {
            index_policy: GraphIndexPolicy::OutboundOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    legacy
        .write_edge(typed_mutation("a", "R", 1, 2, "edge"))
        .await
        .unwrap();
    legacy
        .write_edge(typed_mutation("a", "S", 3, 3, "loop"))
        .await
        .unwrap();
    legacy
        .bulk_append_out_adjacency_segment_trusted("a", "T", 10, 11..=19, "segment")
        .await
        .unwrap();
    for id in 11..=12 {
        legacy
            .delete_edge(typed_mutation("a", "T", 10, id, &format!("delete-{id}")))
            .await
            .unwrap();
    }
    legacy
        .bulk_append_out_adjacency_segment_trusted("a", "Gone", 50, [51], "gone")
        .await
        .unwrap();
    legacy
        .delete_edge(typed_mutation("a", "Gone", 50, 51, "delete-gone"))
        .await
        .unwrap();
    legacy
        .set_vertex_metadata("a", 100, VertexMetadata::default().with_label("Keep"))
        .await
        .unwrap();
    legacy.close().await.unwrap();
    let opts = GraphOpenOptions {
        index_policy: GraphIndexPolicy::OutboundOnly,
        ..membership_options()
    };
    let shard = GraphShard::open_standalone_writer_with_options(path, store.clone(), opts.clone())
        .await
        .unwrap();
    let old = shard.db.snapshot().await.unwrap();
    let chunk = VertexMembershipBackfillOptions {
        max_records_per_commit: 2,
        max_commits: 1,
        restart: false,
        ..Default::default()
    };
    let first = shard
        .unstable_backfill_vertex_membership("a", chunk.clone())
        .await
        .unwrap();
    assert!(!first.complete);
    assert!(!shard
        .unstable_vertex_membership_complete("a")
        .await
        .unwrap());
    assert!(shard
        .set_vertex_metadata("a", 900, VertexMetadata::default())
        .await
        .is_err());
    GraphStore::scope_snapshot(old.clone(), async {
        assert!(!shard
            .unstable_vertex_membership_complete("a")
            .await
            .unwrap());
    })
    .await;
    drop(old);
    shard.close().await.unwrap();
    let shard = GraphShard::open_standalone_writer_with_options(path, store.clone(), opts)
        .await
        .unwrap();
    let mut report = first;
    for _ in 0..100 {
        report = shard
            .unstable_backfill_vertex_membership("a", chunk.clone())
            .await
            .unwrap();
        if report.complete {
            break;
        }
    }
    assert!(
        report.complete,
        "bounded migration must eventually complete"
    );
    assert_eq!(report.vertices_created, 11);
    for id in [1, 2, 3, 10, 13, 14, 15, 16, 17, 18, 19, 100] {
        assert!(present(&shard, "a", id).await);
    }
    for id in [11, 12, 50, 51, 900] {
        assert!(!present(&shard, "a", id).await);
    }
    assert_eq!(
        shard
            .vertex_metadata_if_exists("a", 100)
            .await
            .unwrap()
            .unwrap(),
        VertexMetadata::default().with_label("Keep")
    );
    assert!(
        shard
            .unstable_backfill_vertex_membership("a", chunk)
            .await
            .unwrap()
            .complete
    );
    shard.close().await.unwrap();
    let disabled = GraphShard::open_standalone_writer(path, store)
        .await
        .unwrap();
    assert!(disabled.out_neighbors("a", "R", 1).await.is_err());
    assert!(disabled.snapshot("a").await.is_err());
    assert!(disabled
        .set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .is_err());
    disabled.close().await.unwrap();
}

#[tokio::test]
async fn interrupted_backfill_detects_other_cell_writes_and_requires_restart() {
    let shard = canonical_shard("membership/restart").await;
    let chunk = VertexMembershipBackfillOptions {
        max_records_per_commit: 1,
        max_commits: 1,
        restart: false,
        ..Default::default()
    };
    assert!(
        !shard
            .unstable_backfill_vertex_membership("a", chunk.clone())
            .await
            .unwrap()
            .complete
    );
    shard
        .set_vertex_metadata("b", 1, VertexMetadata::default())
        .await
        .unwrap();
    assert!(matches!(
        shard
            .unstable_backfill_vertex_membership("a", chunk.clone())
            .await,
        Err(GraphError::ConditionalWriteConflict { .. })
    ));
    let restarted = VertexMembershipBackfillOptions {
        restart: true,
        ..Default::default()
    };
    assert!(
        shard
            .unstable_backfill_vertex_membership("a", restarted)
            .await
            .unwrap()
            .complete
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_and_corruption_never_activate() {
    let shard = canonical_shard("membership/failure").await;
    let token = QueryCancellationToken::new();
    token.cancel();
    assert!(token
        .scope(
            shard.unstable_backfill_vertex_membership(
                "a",
                VertexMembershipBackfillOptions::default()
            )
        )
        .await
        .is_err());
    assert!(!shard
        .unstable_vertex_membership_complete("a")
        .await
        .unwrap());
    let mut bad = WriteBatch::new();
    bad.put(keys::vertex("a", 1).as_bytes(), b"".as_slice());
    shard.write_strict_for_test(bad).await.unwrap();
    assert!(matches!(
        shard
            .unstable_backfill_vertex_membership("a", VertexMembershipBackfillOptions::default())
            .await,
        Err(GraphError::CorruptValue { .. })
    ));
    assert!(!shard
        .unstable_vertex_membership_complete("a")
        .await
        .unwrap());
    shard.close().await.unwrap();
}

#[tokio::test]
async fn verification_rejects_missing_endpoint_without_publishing_complete() {
    let shard = canonical_shard("membership/verify").await;
    let sequence = shard.current_epoch("a").await.unwrap() + 1;
    let edge = EdgeRecord {
        cell_id: "a".into(),
        edge_type: "R".into(),
        src: 1,
        dst: 2,
    };
    let key = keys::vertex_membership("a");
    let mut fixture = WriteBatch::new();
    fixture.put(
        keys::out_edge("a", "R", 1, 2).as_bytes(),
        encode_edge_record(&edge),
    );
    // Model a damaged store at the beginning of outbound verification.
    fixture.put(
        key.as_bytes(),
        format!("vertex-membership-v1\nbackfilling\n5\n\n0\n{sequence}\n0\n0\n").as_bytes(),
    );
    shard.write_strict_for_test(fixture).await.unwrap();
    let before = shard.read_remote(&key).await.unwrap();
    assert!(matches!(
        shard
            .unstable_backfill_vertex_membership("a", VertexMembershipBackfillOptions::default())
            .await,
        Err(GraphError::CorruptValue { .. })
    ));
    assert_eq!(shard.read_remote(&key).await.unwrap(), before);
    assert!(!shard
        .unstable_vertex_membership_complete("a")
        .await
        .unwrap());
    assert!(!present(&shard, "a", 1).await);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_schema_and_record_byte_limits_fail_closed() {
    let shard = canonical_shard("membership/checkpoint").await;
    for checkpoint in [
        "vertex-membership-v2\ncomplete\n",
        "vertex-membership-v1\nbackfilling\n6\n\n1\n0\n0\n0\n",
        "vertex-membership-v1\ncomplete\nextra",
        "vertex-membership-v1\nbackfilling\n8\n\n0\n0\n0\n0\n",
    ] {
        let mut fixture = WriteBatch::new();
        fixture.put(
            keys::vertex_membership("a").as_bytes(),
            checkpoint.as_bytes(),
        );
        shard.write_strict_for_test(fixture).await.unwrap();
        assert!(matches!(
            shard.unstable_vertex_membership_complete("a").await,
            Err(GraphError::CorruptValue { .. })
        ));
        assert!(matches!(
            shard
                .unstable_backfill_vertex_membership(
                    "a",
                    VertexMembershipBackfillOptions::default()
                )
                .await,
            Err(GraphError::CorruptValue { .. })
        ));
    }
    shard
        .set_vertex_metadata(
            "b",
            1,
            VertexMetadata::default()
                .with_property("large", VertexPropertyValue::String("x".repeat(2048))),
        )
        .await
        .unwrap();
    let small = VertexMembershipBackfillOptions {
        max_bytes_per_commit: 1024,
        ..Default::default()
    };
    assert!(matches!(
        shard.unstable_backfill_vertex_membership("b", small).await,
        Err(GraphError::AdmissionRejected { .. })
    ));
    assert!(!shard
        .unstable_vertex_membership_complete("b")
        .await
        .unwrap());
    activate(&shard, "b").await;
    shard.close().await.unwrap();
}

#[tokio::test]
async fn migration_rejects_fenced_writer_and_unsafe_durability() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = GraphShard::open_standalone_writer_with_options(
        "membership/fence",
        store.clone(),
        membership_options(),
    )
    .await
    .unwrap();
    first
        .unstable_backfill_vertex_membership(
            "a",
            VertexMembershipBackfillOptions {
                max_commits: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let second = GraphShard::open_standalone_writer_with_options(
        "membership/fence",
        store,
        membership_options(),
    )
    .await
    .unwrap();
    assert!(first
        .unstable_backfill_vertex_membership("a", VertexMembershipBackfillOptions::default())
        .await
        .is_err());
    activate(&second, "a").await;
    second.close().await.unwrap();
    let unsafe_open = GraphShard::open_standalone_writer_with_options(
        "membership/unsafe",
        Arc::new(InMemory::new()),
        GraphOpenOptions {
            durability: GraphDurabilityConfig::default().with_await_durable_writes(false),
            ..membership_options()
        },
    )
    .await;
    assert!(matches!(
        unsafe_open,
        Err(GraphError::UnsafeDurabilityConfig { .. })
    ));
}

#[tokio::test]
async fn membership_is_confined_to_graph_store_and_cell() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = GraphShard::open_standalone_writer_with_options(
        "scope/one",
        store.clone(),
        membership_options(),
    )
    .await
    .unwrap();
    let b =
        GraphShard::open_standalone_writer_with_options("scope/two", store, membership_options())
            .await
            .unwrap();
    a.set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .unwrap();
    activate(&a, "a").await;
    assert!(!present(&b, "a", 1).await);
    assert!(!present(&a, "b", 1).await);
    assert!(!b.unstable_vertex_membership_complete("a").await.unwrap());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn readiness_uses_pinned_snapshot_and_matched_writes_do_not_resurrect() {
    let shard = canonical_shard("membership/readiness-snapshot").await;
    let old = shard.db.snapshot().await.unwrap();
    activate(&shard, "a").await;
    let query = "MATCH (n {id: 99}) RETURN n.id";
    assert!(shard
        .execute_cypher_rows(QueryContext::new("a", "new"), query)
        .await
        .unwrap()
        .rows
        .is_empty());
    let legacy = GraphStore::scope_snapshot(
        old.clone(),
        shard.execute_cypher_rows(QueryContext::new("a", "old"), query),
    )
    .await
    .unwrap();
    assert_eq!(
        legacy.rows,
        vec![QueryRow::new(vec![QueryValue::VertexId(99)])]
    );
    shard
        .set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .unwrap();
    shard.delete_vertex("a", 1, "delete").await.unwrap();
    assert!(matches!(
        shard
            .set_matched_vertex_metadata("a", 1, VertexMetadata::default().with_label("Ghost"))
            .await,
        Err(GraphError::ConditionalWriteConflict { .. })
    ));
    assert!(!present(&shard, "a", 1).await);
    drop(old);
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn graph_plan_hydration_discards_absent_index_candidates() {
    use hydradb_graph_plan::{GraphPhysicalPlan, PhysicalExpression, PhysicalProjection};
    let shard = canonical_shard("membership/physical").await;
    activate(&shard, "a").await;
    let mut fixture = WriteBatch::new();
    fixture.put(
        keys::vertex_label("a", "Ghost", 99).as_bytes(),
        encode_u64(99),
    );
    shard.write_strict_for_test(fixture).await.unwrap();
    let plan = GraphPhysicalPlan::Project {
        input: Box::new(GraphPhysicalPlan::VertexLabelScan {
            binding: "n".into(),
            label: "Ghost".into(),
        }),
        items: vec![PhysicalProjection {
            expression: PhysicalExpression::Binding("n".into()),
            alias: None,
        }],
    };
    assert!(shard
        .execute_graph_physical_plan(QueryContext::new("a", "physical"), plan)
        .await
        .unwrap()
        .rows
        .is_empty());
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn ordered_hydration_preserves_ids_when_an_index_candidate_is_absent() {
    let shard = canonical_shard("membership/ordered").await;
    activate(&shard, "a").await;
    for id in [100, 101] {
        shard
            .set_vertex_metadata(
                "a",
                id,
                VertexMetadata::default()
                    .with_label("Entity")
                    .with_property("created_at", VertexPropertyValue::String("same".into())),
            )
            .await
            .unwrap();
    }
    let encoded = encode_vertex_property_value_key(&VertexPropertyValue::String("same".into()));
    let mut fixture = WriteBatch::new();
    fixture.put(
        keys::vertex_property_index("a", "created_at", &encoded, 99).as_bytes(),
        encode_u64(99),
    );
    fixture.put(
        keys::vertex_label("a", "Entity", 99).as_bytes(),
        encode_u64(99),
    );
    shard.write_strict_for_test(fixture).await.unwrap();
    for mode in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
        if mode == CypherEngineMode::Experimental && !cfg!(feature = "experimental-cypher-engine") {
            continue;
        }
        let query = "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id ORDER BY n.created_at, n.id LIMIT 2";
        let result = shard
            .execute_cypher_rows(
                QueryContext::new("a", "ordered").with_cypher_engine(mode),
                query,
            )
            .await
            .unwrap();
        assert_eq!(
            result.rows,
            vec![
                QueryRow::new(vec![QueryValue::VertexId(100)]),
                QueryRow::new(vec![QueryValue::VertexId(101)])
            ],
            "{mode:?}"
        );
        let page = shard.execute_cypher_rows(QueryContext::new("a", "skip").with_cypher_engine(mode), "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' RETURN n.id ORDER BY n.created_at, n.id SKIP 1 LIMIT 1").await.unwrap();
        assert_eq!(
            page.rows,
            vec![QueryRow::new(vec![QueryValue::VertexId(101)])],
            "{mode:?}"
        );
    }
    shard.close().await.unwrap();
}

#[tokio::test]
async fn flag_defaults_to_legacy_empty_metadata_behavior() {
    let shard = open_test_shard("membership/legacy", Arc::new(InMemory::new())).await;
    assert!(!GraphOpenOptions::default().unstable_canonical_vertex_membership);
    shard
        .set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .unwrap();
    assert!(!present(&shard, "a", 1).await);
    shard
        .set_vertex_metadata("a", 1, VertexMetadata::default().with_label("User"))
        .await
        .unwrap();
    shard
        .set_vertex_metadata("a", 1, VertexMetadata::default())
        .await
        .unwrap();
    assert!(!present(&shard, "a", 1).await);
    assert!(shard
        .unstable_backfill_vertex_membership("a", VertexMembershipBackfillOptions::default())
        .await
        .is_err());
    shard.close().await.unwrap();
}

#[cfg(feature = "opencypher")]
#[tokio::test]
async fn entity_binding_count_optional_zero_hop_and_mutations_respect_presence() {
    for mode in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
        if mode == CypherEngineMode::Experimental && !cfg!(feature = "experimental-cypher-engine") {
            continue;
        }
        let shard = GraphShard::open_standalone_writer_with_options(
            format!("membership/query-{mode:?}"),
            Arc::new(InMemory::new()),
            GraphOpenOptions {
                ..membership_options()
            },
        )
        .await
        .unwrap();
        activate(&shard, "a").await;
        shard
            .set_vertex_metadata("a", 1, VertexMetadata::default())
            .await
            .unwrap();
        for query in [
            "MATCH (n {id: 99}) RETURN n.id",
            "MATCH (n {id: 99})-[:R*0..1]->(m) RETURN m.id",
        ] {
            let result = shard
                .execute_cypher_rows(
                    QueryContext::new("a", "absent").with_cypher_engine(mode),
                    query,
                )
                .await
                .unwrap_or_else(|err| panic!("{mode:?} {query}: {err:?}"));
            assert!(
                result.rows.is_empty(),
                "{mode:?} {query}: {:?}",
                result.rows
            );
        }
        shard
            .execute_cypher(
                QueryContext::new("a", "absent-mutation").with_cypher_engine(mode),
                "MATCH (n {id: 99}) SET n.name = 'ghost'",
            )
            .await
            .unwrap();
        assert!(!present(&shard, "a", 99).await);
        let count = shard
            .execute_cypher_rows(
                QueryContext::new("a", "count").with_cypher_engine(mode),
                "MATCH (n {id: 99}) RETURN count(*)",
            )
            .await
            .unwrap();
        assert_eq!(count.rows, vec![QueryRow::new(vec![QueryValue::Count(0)])]);
        let empty = shard
            .execute_cypher_rows(
                QueryContext::new("a", "existing").with_cypher_engine(mode),
                "MATCH (n {id: 1}) RETURN n.id",
            )
            .await
            .unwrap();
        assert_eq!(
            empty.rows,
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])]
        );
        let zero = shard
            .execute_cypher_rows(
                QueryContext::new("a", "zero").with_cypher_engine(mode),
                "MATCH (n {id: 1})-[:R*0..1]->(m) RETURN m.id",
            )
            .await
            .unwrap();
        assert_eq!(
            zero.rows,
            vec![QueryRow::new(vec![QueryValue::VertexId(1)])]
        );
        let optional = shard
            .execute_cypher_rows(
                QueryContext::new("a", "optional").with_cypher_engine(mode),
                "MATCH (n {id: 1}) OPTIONAL MATCH (m {id: 99}) RETURN n.id, m.id",
            )
            .await
            .unwrap();
        assert_eq!(
            optional.rows,
            vec![QueryRow::new(vec![
                QueryValue::VertexId(1),
                QueryValue::Null
            ])]
        );
        shard
            .execute_cypher(
                QueryContext::new("a", "set").with_cypher_engine(mode),
                "MATCH (n {id: 1}) SET n.name = 'one'",
            )
            .await
            .unwrap();
        assert_eq!(
            shard
                .vertex_metadata_if_exists("a", 1)
                .await
                .unwrap()
                .unwrap()
                .properties
                .get("name"),
            Some(&VertexPropertyValue::String("one".into()))
        );
        shard
            .execute_cypher(
                QueryContext::new("a", "remove").with_cypher_engine(mode),
                "MATCH (n {id: 1}) REMOVE n.name",
            )
            .await
            .unwrap();
        assert!(present(&shard, "a", 1).await);
        if mode == CypherEngineMode::Experimental {
            let scan = shard
                .execute_cypher_rows(
                    QueryContext::new("a", "scan").with_cypher_engine(mode),
                    "MATCH (n) RETURN n.id",
                )
                .await
                .unwrap();
            assert_eq!(
                scan.rows,
                vec![QueryRow::new(vec![QueryValue::VertexId(1)])]
            );
        }
        let deleted = shard
            .execute_cypher(
                QueryContext::new("a", "delete-empty").with_cypher_engine(mode),
                "MATCH (n {id: 1}) DELETE n",
            )
            .await
            .unwrap();
        let QueryOutput::Mutation(deleted) = deleted else {
            panic!("expected vertex deletion statistics");
        };
        assert_eq!(deleted.updated_vertices, 1);
        assert!(!present(&shard, "a", 1).await);
        let repeated = shard
            .execute_cypher(
                QueryContext::new("a", "delete-absent").with_cypher_engine(mode),
                "MATCH (n {id: 1}) DELETE n",
            )
            .await
            .unwrap();
        let QueryOutput::Mutation(repeated) = repeated else {
            panic!("expected repeated deletion statistics");
        };
        assert_eq!(repeated.updated_vertices, 0);

        for (query, ids, key) in [
            (
                "CREATE (a {id: 2})-[:R]->(b {id: 3})",
                [2, 3],
                "create-empty-endpoints",
            ),
            (
                "MERGE (a {id: 4})-[:S]->(b {id: 5})",
                [4, 5],
                "merge-empty-endpoints",
            ),
        ] {
            shard
                .execute_cypher(QueryContext::new("a", key).with_cypher_engine(mode), query)
                .await
                .unwrap();
            for id in ids {
                assert!(present(&shard, "a", id).await);
            }
        }
        let detached = shard
            .execute_cypher(
                QueryContext::new("a", "detach-empty").with_cypher_engine(mode),
                "MATCH (n {id: 2}) DETACH DELETE n",
            )
            .await
            .unwrap();
        let QueryOutput::Mutation(detached) = detached else {
            panic!("expected detach deletion statistics");
        };
        assert_eq!(detached.updated_vertices, 1);
        assert_eq!(detached.deleted_edges, 1);
        assert!(!present(&shard, "a", 2).await);
        assert!(present(&shard, "a", 3).await);
        shard.close().await.unwrap();
    }
}
