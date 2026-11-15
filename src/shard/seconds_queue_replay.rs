use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "controlled nine-second storage stall; not staging client percentiles"]
async fn seconds_cleanup_queue_replay() {
    for trial in 0..3 {
        let store = crate::tests::ReadCountingObjectStore::new();
        let shard = Arc::new(
            GraphShard::open_standalone_writer_with_options(
                format!("seconds-cleanup-{trial}"),
                store.clone(),
                GraphOpenOptions {
                    cache: GraphCacheConfig {
                        slatedb_cache_bytes: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        shard
            .set_vertex_metadata_batch("cell-0", (1..=18).map(|id| (id, VertexMetadata::default().with_label("Vertex"))))
            .await
            .unwrap();
        shard
            .write_edges_batch("cell-0", "HAS_CHUNK", [(2, 1), (3, 1), (4, 1)], "chunks")
            .await
            .unwrap();
        shard
            .write_edges_batch("cell-0", "PRESENT_IN", (5..=18).map(|id| (id, 2 + id % 3)), "entities")
            .await
            .unwrap();
        shard
            .storage
            .writer()
            .unwrap()
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();
        let (started, release) = store.pause_next_get();
        let deleting = {
            let shard = Arc::clone(&shard);
            tokio::spawn(async move {
                let start = std::time::Instant::now();
                let result = shard
                    .delete_vertices_and_isolated_candidates_batch(
                        "cell-0",
                        (1..=18)
                            .map(|id| (id, format!("cleanup-{id}"), id <= 4, VertexMetadata::default()))
                            .collect(),
                    )
                    .await
                    .unwrap();
                (start.elapsed(), result)
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), started)
            .await
            .unwrap()
            .unwrap();
        let releasing = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(9271)).await;
            let _ = release.send(());
        });
        let writers = (0..8)
            .map(|id| {
                let shard = Arc::clone(&shard);
                tokio::spawn(async move {
                    let start = std::time::Instant::now();
                    shard
                        .merge_vertex_metadata_batch("cell-0", [(1000 + id, VertexMetadata::default().with_label("Concurrent"))], None)
                        .await
                        .unwrap();
                    start.elapsed().as_micros()
                })
            })
            .collect::<Vec<_>>();
        let mut samples = Vec::new();
        for writer in writers {
            samples.push(writer.await.unwrap());
        }
        let (cleanup, deleted) = deleting.await.unwrap();
        releasing.await.unwrap();
        assert_eq!(deleted.len(), 18);
        assert!(deleted.iter().all(|result| result.vertex_deleted));
        samples.sort_unstable();
        eprintln!(
            "seconds-queue trial={trial} injected_get_stall_ms=9271 cleanup_us={} changing_write_us={samples:?}",
            cleanup.as_micros()
        );
        shard.close().await.unwrap();
    }
}
