use super::*;
use crate::codec::encode_relationship_record;
use crate::shard::query::{IndexedRelationshipBatch, IndexedRelationshipPairMemo};
use slatedb::object_store::memory::InMemory;
use slatedb::WriteBatch;

#[tokio::test]
async fn batched_relationship_windows_match_full_expansion_with_stale_indexes() {
    let shard = GraphShard::open_standalone_writer("graph/batched-relationship-window", Arc::new(InMemory::new()))
        .await
        .unwrap();
    shard
        .set_vertex_metadata_batch(
            "cell-a",
            (1..=4)
                .map(|id| {
                    (
                        id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property("name", VertexPropertyValue::String(format!("node-{id}"))),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    shard
        .write_edge(EdgeMutation {
            cell_id: "cell-a".into(),
            edge_type: "RELATES".into(),
            src: 1,
            dst: 2,
            idempotency_key: "pair".into(),
        })
        .await
        .unwrap();
    shard
        .set_edge_metadata(
            "cell-a",
            "RELATES",
            1,
            2,
            EdgeMetadata::default()
                .with_property("score", VertexPropertyValue::Integer(10_000))
                .with_property("kind", VertexPropertyValue::String("inherited".into())),
        )
        .await
        .unwrap();
    let records = (0..256)
        .map(|i| {
            let mut metadata = EdgeMetadata::default()
                .with_property("id", VertexPropertyValue::Integer(1000 + i))
                .with_property("seq", VertexPropertyValue::Integer(i));
            if i != 0 {
                metadata = metadata.with_property("score", VertexPropertyValue::Integer(if i < 200 { 10 } else { 9 }));
            }
            if i % 3 == 0 {
                metadata = metadata.with_property("kind", VertexPropertyValue::String("override".into()));
            }
            RelationshipMutation {
                cell_id: "cell-a".into(),
                edge_type: "RELATES".into(),
                src: 1,
                dst: 2,
                relationship_id: 1000 + i,
                metadata,
            }
        })
        .collect::<Vec<_>>();
    shard
        .import_relationships_batch("cell-a", "RELATES", records, "many-parallel-records")
        .await
        .unwrap();

    let encoded = encode_vertex_property_value_key(&VertexPropertyValue::Integer(10_000));
    let mut stale = WriteBatch::new();
    // Own score missing, own score changed, missing record: none may borrow
    // the structural score to validate its own-property index entry.
    for id in [1000, 1128, 70_000] {
        stale.put(
            keys::relationship_property_index("cell-a", "RELATES", "score", &encoded, 1, 2, id),
            [],
        );
    }
    // A live-looking record/index without a structural edge must be rejected.
    let ghost = crate::RelationshipRecord {
        cell_id: "cell-a".into(),
        edge_type: "RELATES".into(),
        src: 3,
        dst: 4,
        relationship_id: 70_001,
        metadata: EdgeMetadata::default()
            .with_property("score", VertexPropertyValue::Integer(10_000))
            .with_property("seq", VertexPropertyValue::Integer(999)),
    };
    stale.put(
        keys::relationship("cell-a", "RELATES", 3, 4, 70_001),
        encode_relationship_record(&ghost),
    );
    stale.put(
        keys::relationship_property_index("cell-a", "RELATES", "score", &encoded, 3, 4, 70_001),
        [],
    );
    stale.put(keys::edge_metadata("cell-a", "RELATES", 3, 4), b"malformed orphan metadata");
    shard.write_strict_for_test(stale).await.unwrap();

    let base = "MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE r.score >= 0 \
                RETURN r.seq AS seq, r.score AS score, r.kind AS kind, a.name AS source \
                ORDER BY r.score DESC, r.seq ASC";
    for mode in [CypherEngineMode::Legacy, CypherEngineMode::Experimental] {
        // No LIMIT: the reference takes the full matching/expansion and sort
        // route rather than the new bounded storage path.
        let reference = shard
            .execute_cypher_rows(QueryContext::new("cell-a", "full-reference").with_cypher_engine(mode), base)
            .await
            .unwrap();
        assert_eq!(reference.rows.len(), 256);
        if matches!(mode, CypherEngineMode::Experimental) {
            assert_eq!(reference.rows[0].values[0], QueryValue::Property(VertexPropertyValue::Integer(0)));
            assert_eq!(
                reference.rows[0].values[2],
                QueryValue::Property(VertexPropertyValue::String("override".into()))
            );
            assert_eq!(
                reference.rows[1].values[2],
                QueryValue::Property(VertexPropertyValue::String("inherited".into()))
            );
        }
        for (skip, limit) in [(0, 1), (0, 7), (3, 129), (0, 300)] {
            let query = format!("{base} SKIP {skip} LIMIT {limit}");
            let result = shard
                .execute_cypher_rows(QueryContext::new("cell-a", "bounded-window").with_cypher_engine(mode), &query)
                .await
                .unwrap();
            let expected = reference.rows.iter().skip(skip).take(limit).cloned().collect::<Vec<_>>();
            assert_eq!(result.columns, reference.columns);
            assert_eq!(result.rows, expected, "mode={mode:?}, skip={skip}, limit={limit}");
        }
    }
    shard.close().await.unwrap();
}

#[tokio::test]
async fn direct_relationship_batch_keeps_the_callers_snapshot_during_updates() {
    let shard = GraphShard::open_standalone_writer("graph/direct-relationship-snapshot", Arc::new(InMemory::new()))
        .await
        .unwrap();
    shard
        .import_relationships_batch(
            "cell-a",
            "RELATES",
            [RelationshipMutation {
                cell_id: "cell-a".into(),
                edge_type: "RELATES".into(),
                src: 1,
                dst: 2,
                relationship_id: 10,
                metadata: EdgeMetadata::default().with_property("score", VertexPropertyValue::Integer(1)),
            }],
            "original",
        )
        .await
        .unwrap();
    let snapshot = shard.storage.snapshot().await.unwrap();
    let epoch = snapshot.seq();
    let candidates = [(1, 2, 10, encode_vertex_property_value_key(&VertexPropertyValue::Integer(1)))];
    shard
        .set_relationship_metadata(
            "cell-a",
            "RELATES",
            1,
            2,
            10,
            EdgeMetadata::default().with_property("score", VertexPropertyValue::Integer(2)),
        )
        .await
        .unwrap();
    let budget = QueryBudget::new(None, None);
    let mut memo = IndexedRelationshipPairMemo::default();
    let old = SlateGraphStore::scope_snapshot(
        Arc::clone(&snapshot),
        shard.indexed_relationship_metadata_batch_at(IndexedRelationshipBatch {
            cell_id: "cell-a",
            edge_type: "RELATES",
            property: "score",
            candidates: &candidates,
            read_epoch: epoch,
            inherit_structural: true,
            budget: &budget,
            memo: &mut memo,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        old[0].as_ref().unwrap().properties.get("score"),
        Some(&VertexPropertyValue::Integer(1))
    );
    let current = shard.current_epoch("cell-a").await.unwrap();
    let newer = shard
        .indexed_relationship_metadata_batch_at(IndexedRelationshipBatch {
            cell_id: "cell-a",
            edge_type: "RELATES",
            property: "score",
            candidates: &candidates,
            read_epoch: current,
            inherit_structural: true,
            budget: &budget,
            memo: &mut memo,
        })
        .await
        .unwrap();
    assert!(newer[0].is_none(), "obsolete indexed value must not match the new snapshot");
    drop(snapshot);
    shard.close().await.unwrap();
}

/// A relationship property equality seek reads each relationship-index entry
/// by its id instead of every relationship on its pair, and must still keep
/// exactly what reading every pair kept: values inherited from the pair's
/// structural properties, parallel relationships with other values, stale
/// index entries and records whose structural edge is gone.
#[tokio::test]
async fn relationship_equality_seek_matches_the_unindexed_expansion() {
    let shard = GraphShard::open_standalone_writer("graph/relationship-equality-seek", Arc::new(InMemory::new()))
        .await
        .unwrap();
    shard
        .set_vertex_metadata_batch(
            "cell-a",
            (1..=6)
                .map(|id| {
                    (
                        id,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property("name", VertexPropertyValue::String(format!("node-{id}"))),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    // (1,2): the pair itself carries chunk "c", so every relationship on it
    // inherits the value unless it overrides it.
    shard
        .write_edge(EdgeMutation {
            cell_id: "cell-a".into(),
            edge_type: "RELATES".into(),
            src: 1,
            dst: 2,
            idempotency_key: "inherited-pair".into(),
        })
        .await
        .unwrap();
    shard
        .set_edge_metadata(
            "cell-a",
            "RELATES",
            1,
            2,
            EdgeMetadata::default().with_property("chunk_id", VertexPropertyValue::String("c".into())),
        )
        .await
        .unwrap();
    let chunk = |value: &str| VertexPropertyValue::String(value.into());
    let relationship = |src, dst, id, chunk_id: Option<&str>| RelationshipMutation {
        cell_id: "cell-a".into(),
        edge_type: "RELATES".into(),
        src,
        dst,
        relationship_id: id,
        metadata: match chunk_id {
            Some(value) => EdgeMetadata::default()
                .with_property("seq", VertexPropertyValue::Integer(id))
                .with_property("chunk_id", chunk(value)),
            None => EdgeMetadata::default().with_property("seq", VertexPropertyValue::Integer(id)),
        },
    };
    let mut records = vec![
        relationship(1, 2, 100, None),      // inherits "c"
        relationship(1, 2, 101, Some("d")), // overrides with "d"
        relationship(1, 2, 102, Some("c")),
    ];
    // (1,3) and (4,5): only relationship-index entries, several parallel
    // relationships each, some with the wanted values.
    records.extend((0..6).map(|i| relationship(1, 3, 200 + i, Some(if i % 2 == 0 { "c" } else { "d" }))));
    records.extend((0..4).map(|i| relationship(4, 5, 300 + i, Some(if i < 2 { "e" } else { "d" }))));
    shard
        .import_relationships_batch("cell-a", "RELATES", records, "equality-seek")
        .await
        .unwrap();

    let encoded = encode_vertex_property_value_key(&chunk("c"));
    let mut stale = WriteBatch::new();
    // A stale entry: relationship 201 holds "d" now; a missing record.
    for (src, dst, id) in [(1, 3, 201), (1, 3, 70_000)] {
        stale.put(
            keys::relationship_property_index("cell-a", "RELATES", "chunk_id", &encoded, src, dst, id),
            [],
        );
    }
    // A record and entry without a structural edge.
    let ghost = crate::RelationshipRecord {
        cell_id: "cell-a".into(),
        edge_type: "RELATES".into(),
        src: 5,
        dst: 6,
        relationship_id: 70_001,
        metadata: EdgeMetadata::default()
            .with_property("chunk_id", chunk("c"))
            .with_property("seq", VertexPropertyValue::Integer(999)),
    };
    stale.put(
        keys::relationship("cell-a", "RELATES", 5, 6, 70_001),
        encode_relationship_record(&ghost),
    );
    stale.put(
        keys::relationship_property_index("cell-a", "RELATES", "chunk_id", &encoded, 5, 6, 70_001),
        [],
    );
    shard.write_strict_for_test(stale).await.unwrap();

    let returns = "RETURN a.name AS source, b.name AS target, r.seq AS seq, r.chunk_id AS chunk ORDER BY source, target, seq";
    for (operator, seek, reference) in [
        (
            "RelationshipPropertySeek(",
            format!("MATCH (a:Entity)-[r:RELATES {{chunk_id: 'c'}}]->(b:Entity) {returns}"),
            format!("MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE toString(r.chunk_id) = 'c' {returns}"),
        ),
        (
            "RelationshipPropertyMultiSeek(",
            format!("MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE r.chunk_id = 'c' OR r.chunk_id = 'e' {returns}"),
            format!("MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE toString(r.chunk_id) = 'c' OR toString(r.chunk_id) = 'e' {returns}"),
        ),
        // Relationship 201 holds "d" and also has a stale "c" entry: it must
        // still be found through its live entry.
        (
            "RelationshipPropertyMultiSeek(",
            format!("MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE r.chunk_id = 'c' OR r.chunk_id = 'd' {returns}"),
            format!("MATCH (a:Entity)-[r:RELATES]->(b:Entity) WHERE toString(r.chunk_id) = 'c' OR toString(r.chunk_id) = 'd' {returns}"),
        ),
    ] {
        let context = || QueryContext::new("cell-a", "equality-seek").with_cypher_engine(CypherEngineMode::Experimental);
        // The seek takes the index path under test; the reference cannot.
        for (query, indexed) in [(&seek, true), (&reference, false)] {
            let explain = shard.execute_cypher_rows(context(), &format!("EXPLAIN {query}")).await.unwrap();
            let QueryValue::Property(VertexPropertyValue::String(plan)) = &explain.rows[0].values[0] else {
                panic!("EXPLAIN returns plan text");
            };
            assert_eq!(plan.contains(operator), indexed, "{plan}");
        }
        let expected = shard.execute_cypher_rows(context(), &reference).await.unwrap();
        let actual = shard.execute_cypher_rows(context(), &seek).await.unwrap();
        assert!(!expected.rows.is_empty(), "{reference}");
        assert_eq!(actual.columns, expected.columns);
        assert_eq!(actual.rows, expected.rows, "{seek}");
    }
    shard.close().await.unwrap();
}

/// A write committed after a query pinned its snapshot does not change what
/// the batch returns inside that snapshot: the live relationships, and nothing
/// for a record whose structural edge does not exist. Inside the scope the
/// current epoch is the pinned one, so the batch keeps its point reads.
#[tokio::test]
async fn relationship_batch_inside_a_pinned_snapshot_ignores_later_writes_and_orphans() {
    let shard = GraphShard::open_standalone_writer("graph/historical-relationship-batch", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let score = |value| EdgeMetadata::default().with_property("score", VertexPropertyValue::Integer(value));
    shard
        .import_relationships_batch(
            "cell-a",
            "RELATES",
            [10, 11].map(|id| RelationshipMutation {
                cell_id: "cell-a".into(),
                edge_type: "RELATES".into(),
                src: 1,
                dst: 2,
                relationship_id: id,
                metadata: score(1),
            }),
            "live",
        )
        .await
        .unwrap();
    let encoded = encode_vertex_property_value_key(&VertexPropertyValue::Integer(1));
    let orphan = crate::RelationshipRecord {
        cell_id: "cell-a".into(),
        edge_type: "RELATES".into(),
        src: 5,
        dst: 6,
        relationship_id: 70_001,
        metadata: score(1),
    };
    let mut batch = WriteBatch::new();
    batch.put(
        keys::relationship("cell-a", "RELATES", 5, 6, 70_001),
        encode_relationship_record(&orphan),
    );
    batch.put(
        keys::relationship_property_index("cell-a", "RELATES", "score", &encoded, 5, 6, 70_001),
        [],
    );
    shard.write_strict_for_test(batch).await.unwrap();

    let snapshot = shard.storage.snapshot().await.unwrap();
    let epoch = snapshot.seq();
    // An unrelated write moves the current epoch past the pinned one.
    shard
        .import_relationships_batch(
            "cell-a",
            "RELATES",
            [RelationshipMutation {
                cell_id: "cell-a".into(),
                edge_type: "RELATES".into(),
                src: 3,
                dst: 4,
                relationship_id: 20,
                metadata: score(2),
            }],
            "later",
        )
        .await
        .unwrap();
    assert_ne!(shard.current_epoch("cell-a").await.unwrap(), epoch);

    let candidates = [
        (1, 2, 10, encoded.clone()),
        (1, 2, 11, encoded.clone()),
        (5, 6, 70_001, encoded.clone()),
    ];
    let budget = QueryBudget::new(None, None);
    let mut memo = IndexedRelationshipPairMemo::default();
    let result = SlateGraphStore::scope_snapshot(
        Arc::clone(&snapshot),
        shard.indexed_relationship_metadata_batch_at(IndexedRelationshipBatch {
            cell_id: "cell-a",
            edge_type: "RELATES",
            property: "score",
            candidates: &candidates,
            read_epoch: epoch,
            inherit_structural: true,
            budget: &budget,
            memo: &mut memo,
        }),
    )
    .await
    .unwrap();
    assert!(result[0].is_some() && result[1].is_some(), "{result:?}");
    assert!(result[2].is_none(), "an orphan record must not match: {result:?}");
    drop(snapshot);
    shard.close().await.unwrap();
}
