//! Opt-in, read-only profiling against a frozen object inventory from a real scope.
use super::*;
use futures::stream::BoxStream;
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use std::sync::atomic::AtomicU64;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct CapturedObject {
    key: String,
    size: u64,
    modified: String,
    etag: Option<String>,
    version: Option<String>,
}

impl CapturedObject {
    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(self.key.clone()),
            size: self.size,
            last_modified: self.modified.parse().expect("captured object timestamp"),
            e_tag: self.etag.clone(),
            version: self.version.clone(),
        }
    }
}

#[derive(Debug)]
struct FrozenStore {
    remote: Arc<dyn ObjectStore>,
    objects: BTreeMap<Path, ObjectMeta>,
    gets: AtomicU64,
    bytes: AtomicU64,
}

impl std::fmt::Display for FrozenStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrozenReadOnlyReplay")
    }
}

fn denied() -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "FrozenReadOnlyReplay",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "replay store forbids all mutations",
        )),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FrozenStore {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> slatedb::object_store::Result<PutResult> {
        Err(denied())
    }
    async fn put_multipart_opts(&self, _: &Path, _: PutMultipartOptions) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        Err(denied())
    }
    async fn get_opts(&self, path: &Path, mut opts: GetOptions) -> slatedb::object_store::Result<GetResult> {
        let Some(meta) = self.objects.get(path) else {
            return Err(slatedb::object_store::Error::NotFound {
                path: path.to_string(),
                source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
            });
        };
        opts.if_match = meta.e_tag.clone();
        self.gets.fetch_add(1, Ordering::Relaxed);
        let result = self.remote.get_opts(path, opts).await?;
        self.bytes.fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        Ok(result)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        let objects = self
            .objects
            .values()
            .filter(|meta| prefix.is_none_or(|prefix| meta.location.prefix_matches(prefix)))
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>();
        stream::iter(objects).boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> slatedb::object_store::Result<ListResult> {
        let root = prefix.map(|p| format!("{p}/")).unwrap_or_default();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
            extensions: Default::default(),
        };
        let mut common = BTreeSet::new();
        for meta in self.objects.values() {
            let Some(suffix) = meta.location.as_ref().strip_prefix(&root) else {
                continue;
            };
            if let Some((part, _)) = suffix.split_once('/') {
                common.insert(Path::from(format!("{root}{part}")));
            } else {
                result.objects.push(meta.clone());
            }
        }
        result.common_prefixes = common.into_iter().collect();
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
        paths.map(|_| Err(denied())).boxed()
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> slatedb::object_store::Result<()> {
        Err(denied())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Fixture {
    path: String,
    epoch: u64,
    vertices: BTreeSet<VertexId>,
    detach: BTreeSet<VertexId>,
}

async fn choose_fixture(shard: &GraphShard, path: &str, cell: &str) -> Fixture {
    let epoch = shard.current_epoch(cell).await.unwrap();
    let snapshot = shard.snapshot_at(cell, epoch).await.unwrap();
    let options = ScanOptions::default();
    let prefix = keys::vertex_label_prefix(cell, "Source");
    let mut sources = snapshot
        .storage_snapshot
        .scan_prefix_with_options(prefix.as_bytes(), .., &options)
        .await
        .unwrap();
    let mut detach = BTreeSet::new();
    for _ in 0..50 {
        let Some(source) = sources.next().await.unwrap() else {
            break;
        };
        let id = decode_u64("source label", &source.value).unwrap();
        let prefix = keys::in_prefix(cell, "HAS_CHUNK", id);
        let mut chunks = snapshot
            .storage_snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &options)
            .await
            .unwrap();
        let mut candidate = BTreeSet::from([id]);
        while candidate.len() < 4 {
            let Some(kv) = chunks.next().await.unwrap() else {
                break;
            };
            let edge = decode_edge_record(&String::from_utf8_lossy(&kv.key), &kv.value).unwrap();
            candidate.insert(edge.src);
        }
        if candidate.len() > 1 {
            detach = candidate;
            break;
        }
    }
    assert!(detach.len() > 1, "no source/chunk fixture found in first 50 sources");
    let mut vertices = detach.clone();
    for vertex in &detach {
        let prefix = keys::in_prefix(cell, "PRESENT_IN", *vertex);
        let mut entities = snapshot
            .storage_snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &options)
            .await
            .unwrap();
        while vertices.len() < 18 {
            let Some(kv) = entities.next().await.unwrap() else {
                break;
            };
            let edge = decode_edge_record(&String::from_utf8_lossy(&kv.key), &kv.value).unwrap();
            vertices.insert(edge.src);
        }
    }
    Fixture {
        path: path.into(),
        epoch,
        vertices,
        detach,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit read-only S3 replay environment and frozen inventory"]
async fn real_scope_incident_replay() {
    let path = std::env::var("HYDRADB_REPLAY_PATH").expect("explicit source cell path");
    let inventory = std::env::var("HYDRADB_REPLAY_INVENTORY").expect("inventory filename");
    let fixture_file = std::env::var("HYDRADB_REPLAY_FIXTURE").expect("fixture filename");
    let remote = crate::object_store_from_env(None).unwrap();
    let objects: Vec<CapturedObject> = if std::path::Path::new(&inventory).exists() {
        serde_json::from_slice(&std::fs::read(&inventory).unwrap()).unwrap()
    } else {
        let mut objects = Vec::new();
        // Capture the manifest first and WAL last. Never discover future WALs
        // on subsequent runs, and fail rather than replace a GC'd/changed object.
        for folder in ["manifest", "compacted", "wal"] {
            let prefix = Path::from(format!("{path}/{folder}"));
            let mut listing = remote.list(Some(&prefix));
            while let Some(meta) = listing.try_next().await.unwrap() {
                assert!(objects.len() < 100_000, "replay inventory bound exceeded");
                objects.push(CapturedObject {
                    key: meta.location.to_string(),
                    size: meta.size,
                    modified: meta.last_modified.to_rfc3339(),
                    etag: meta.e_tag,
                    version: meta.version,
                });
            }
        }
        std::fs::write(&inventory, serde_json::to_vec(&objects).unwrap()).unwrap();
        objects
    };
    assert!(objects.iter().all(|o| o.key.starts_with(&format!("{path}/"))));
    let options = GraphOpenOptions {
        reader_mode: crate::GraphReaderMode::FollowLatest,
        reader_manifest_poll_interval: std::time::Duration::from_secs(86400),
        cache: GraphCacheConfig {
            slatedb_cache_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
        ..Default::default()
    };
    let make_store = || {
        Arc::new(FrozenStore {
            remote: Arc::clone(&remote),
            objects: objects.iter().map(|o| (Path::from(o.key.clone()), o.meta())).collect(),
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    };
    let fixture: Fixture = if std::path::Path::new(&fixture_file).exists() {
        serde_json::from_slice(&std::fs::read(&fixture_file).unwrap()).unwrap()
    } else {
        eprintln!("real-scope-fixture opening reader");
        let shard = tokio::time::timeout(
            std::time::Duration::from_secs(180),
            GraphShard::open_with_options(path.clone(), make_store(), options.clone()),
        )
        .await
        .expect("fixture reader open exceeded 180 seconds")
        .unwrap();
        let fixture = tokio::time::timeout(std::time::Duration::from_secs(180), choose_fixture(&shard, &path, "cell-0"))
            .await
            .expect("fixture selection exceeded 180 seconds");
        tokio::time::timeout(std::time::Duration::from_secs(30), shard.close())
            .await
            .expect("fixture reader close exceeded 30 seconds")
            .unwrap();
        std::fs::write(&fixture_file, serde_json::to_vec(&fixture).unwrap()).unwrap();
        fixture
    };
    assert_eq!(fixture.path, path);
    let guard_shard = GraphShard::open_standalone_writer("replay-guard", Arc::new(slatedb::object_store::memory::InMemory::new()))
        .await
        .unwrap();
    let guard = guard_shard.acquire_local_write_guard("cell-0", "readonly_replay").await.unwrap();
    for trial in 0..1 {
        let store = make_store();
        let opened = std::time::Instant::now();
        eprintln!("real-scope-open trial={trial} starting");
        let shard = tokio::time::timeout(
            std::time::Duration::from_secs(180),
            GraphShard::open_with_options(path.clone(), store.clone(), options.clone()),
        )
        .await
        .expect("frozen reader open exceeded 180 seconds")
        .unwrap();
        eprintln!(
            "real-scope-open trial={trial} elapsed_ms={} gets={}",
            opened.elapsed().as_millis(),
            store.gets.load(Ordering::Relaxed)
        );
        assert_eq!(
            shard.current_epoch("cell-0").await.unwrap(),
            fixture.epoch,
            "frozen replay sequence changed"
        );
        let snapshot = shard.snapshot_at("cell-0", fixture.epoch).await.unwrap();
        let mut expected = None;
        for (temperature, admit) in [
            ("non-admitting-cold", false),
            ("non-admitting-repeat", false),
            ("admitting-first", true),
            ("admitting-repeat", true),
        ] {
            eprintln!("real-scope-discovery trial={trial} temperature={temperature} starting");
            let gets = store.gets.load(Ordering::Relaxed);
            let bytes = store.bytes.load(Ordering::Relaxed);
            let started = std::time::Instant::now();
            let edges = tokio::time::timeout(
                std::time::Duration::from_secs(180),
                shard.indexed_vertex_delete_discovery(
                    &snapshot,
                    &fixture.vertices,
                    Some(&fixture.detach),
                    Some(&guard),
                    &ScanOptions::default().with_cache_blocks(admit),
                ),
            )
            .await
            .unwrap()
            .unwrap()
            .edges;
            if let Some(expected) = &expected {
                assert_eq!(&edges, expected, "cache admission changed incident edges");
            } else {
                expected = Some(edges.clone());
            }
            let digest = edges
                .iter()
                .fold(0_u64, |sum, edge| sum.wrapping_add(edge.src).wrapping_add(edge.dst));
            eprintln!("real-scope-discovery trial={trial} temperature={temperature} epoch={} vertices={} detach={} elapsed_ms={} edges={} digest={digest} s3_gets={} s3_bytes={}", fixture.epoch, fixture.vertices.len(), fixture.detach.len(), started.elapsed().as_millis(), edges.len(), store.gets.load(Ordering::Relaxed)-gets, store.bytes.load(Ordering::Relaxed)-bytes);
        }
        drop(snapshot);
        eprintln!("real-scope-close trial={trial} starting");
        tokio::time::timeout(std::time::Duration::from_secs(30), shard.close())
            .await
            .expect("replay reader close exceeded 30 seconds")
            .unwrap();
    }
    guard.release().await.unwrap();
    guard_shard.close().await.unwrap();
}

#[tokio::test]
async fn incident_discovery_reuses_immutable_blocks_within_cache_budget() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = GraphShard::open_standalone_writer_with_options(
        "incident-block-reuse",
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
    .unwrap();
    for index in 0..8 {
        shard
            .write_edges_batch("cell-0", &format!("TYPE{index}"), [(1, 2), (3, 4)], &format!("seed-{index}"))
            .await
            .unwrap();
    }
    shard
        .storage
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
    drop(shard);
    let shard = GraphShard::open_standalone_writer_with_options(
        "incident-block-reuse",
        store.clone(),
        GraphOpenOptions {
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let guard = shard.acquire_local_write_guard("cell-0", "cache-reuse-test").await.unwrap();
    let epoch = shard.current_epoch("cell-0").await.unwrap();
    let vertices = BTreeSet::from([1, 2, 3, 4]);
    let before = store.reads();
    let first = shard
        .indexed_vertex_delete_incidents_at("cell-0", &vertices, None, epoch, &guard)
        .await
        .unwrap();
    let cold_reads = store.reads() - before;
    let before = store.reads();
    let second = shard
        .indexed_vertex_delete_incidents_at("cell-0", &vertices, None, epoch, &guard)
        .await
        .unwrap();
    let repeat_reads = store.reads() - before;
    assert_eq!(first, second);
    assert_eq!(first.len(), 16);
    guard.release().await.unwrap();
    shard.close().await.unwrap();
    assert!(cold_reads > 0, "fixture must exercise storage reads");
    assert_eq!(
        repeat_reads, 0,
        "repeated discovery reread {repeat_reads} immutable objects after {cold_reads} cold reads"
    );
}

#[tokio::test]
async fn prepared_small_delete_warms_apply_reads_before_writer_admission() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let path = "prepared-delete-apply-cache";
    let shard = GraphShard::open_standalone_writer_with_options(
        path,
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    shard
        .set_vertex_metadata_batch("cell-0", (1..=3).map(|id| (id, VertexMetadata::default().with_label("Entity"))))
        .await
        .unwrap();
    shard
        .write_edges_batch("cell-0", "EDGE", [(2, 1), (3, 1)], "incoming")
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
    shard.close().await.unwrap();
    drop(shard);
    let shard = GraphShard::open_standalone_writer_with_options(
        path,
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let deletions = vec![(1, "cleanup".into(), true, VertexMetadata::default())];
    let mode = VertexDeleteBatchMode::Detach;
    let before = store.compacted_reads();
    let prepared = shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap();
    assert!(prepared.is_some());
    assert!(store.compacted_reads() > before, "preparation must read the cold SST");
    // Background GC/manifest polling is independent of these writer guards.
    // Count immutable SST reads to verify the warmed apply working set.
    let before = store.compacted_reads();
    let lock = shard.acquire_local_write_guard_no_fence("cell-0", mode.operation()).await.unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked("cell-0", &deletions, mode, mode.operation(), &lock, prepared)
        .await
        .unwrap();
    let guarded_reads = store.compacted_reads() - before;
    lock.release().await.unwrap();
    assert!(result[0].vertex_deleted);
    assert_eq!(result[0].incident_edges_deleted, 2);
    assert_eq!(guarded_reads, 0, "cold apply reads still held the writer guard");
    assert!(shard.read_remote(&keys::vertex("cell-0", 1)).await.unwrap().is_none());
    shard.close().await.unwrap();
}

async fn open_cached_delete_writer(path: &str, store: Arc<crate::tests::ReadCountingObjectStore>, cache_bytes: usize) -> GraphShard {
    GraphShard::open_standalone_writer_with_options(
        path,
        store,
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: cache_bytes,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

fn relationship_mutation(src: VertexId, dst: VertexId, idempotency_key: &str) -> EdgeMutation {
    EdgeMutation {
        cell_id: "cell-0".into(),
        edge_type: "PRESENT_IN".into(),
        src,
        dst,
        idempotency_key: idempotency_key.into(),
    }
}

async fn relationship_records_between(shard: &GraphShard, src: VertexId, dst: VertexId) -> usize {
    let snapshot = shard.snapshot("cell-0").await.unwrap();
    let prefix = keys::relationship_edge_prefix("cell-0", "PRESENT_IN", src, dst);
    let mut iter = snapshot
        .storage_snapshot
        .scan_prefix_with_options(prefix.as_bytes(), .., &slatedb::config::ScanOptions::default())
        .await
        .unwrap();
    let mut records = 0;
    while iter.next().await.unwrap().is_some() {
        records += 1;
    }
    records
}

/// The apply-read warm above covers point keys only. Edges that carry
/// relationship records are also scanned by prefix inside the writer guard;
/// that scan must find its blocks cached, not go to object storage.
#[tokio::test]
async fn prepared_small_delete_warms_relationship_scans_before_writer_admission() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let path = "prepared-delete-relationship-cache";
    let shard = open_cached_delete_writer(path, store.clone(), 0).await;
    shard
        .set_vertex_metadata_batch("cell-0", (1..=3).map(|id| (id, VertexMetadata::default().with_label("Entity"))))
        .await
        .unwrap();
    for (src, key) in [(2, "r-2a"), (2, "r-2b"), (3, "r-3a")] {
        shard
            .create_relationship(relationship_mutation(src, 1, key), EdgeMetadata::default())
            .await
            .unwrap();
    }
    // Unrelated relationships whose keys sort on both sides of the targets'
    // `rel/PRESENT_IN/...` range. Without them the whole SST is one block, so
    // warming any point key would also cache the relationship records and the
    // test could not tell a warmed scan from an unwarmed one.
    shard
        .set_vertex_metadata_batch("cell-0", (100..=299).map(|id| (id, VertexMetadata::default().with_label("Entity"))))
        .await
        .unwrap();
    for (edge_type, offset) in [("AAA_PADDING", 100), ("ZZZ_PADDING", 200)] {
        for src in offset..offset + 99 {
            shard
                .create_relationship(
                    EdgeMutation {
                        cell_id: "cell-0".into(),
                        edge_type: edge_type.into(),
                        src,
                        dst: src + 1,
                        idempotency_key: format!("pad-{edge_type}-{src}"),
                    },
                    EdgeMetadata::default(),
                )
                .await
                .unwrap();
        }
    }
    shard
        .storage
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
    drop(shard);

    let shard = open_cached_delete_writer(path, store.clone(), 8 * 1024 * 1024).await;
    let deletions = vec![(1, "cleanup".into(), true, VertexMetadata::default())];
    let mode = VertexDeleteBatchMode::Detach;
    let prepared = shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap();
    assert!(prepared.is_some());
    let before = store.compacted_reads();
    let lock = shard.acquire_local_write_guard_no_fence("cell-0", mode.operation()).await.unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked("cell-0", &deletions, mode, mode.operation(), &lock, prepared)
        .await
        .unwrap();
    let guarded_reads = store.compacted_reads() - before;
    lock.release().await.unwrap();
    assert!(result[0].vertex_deleted);
    assert_eq!(result[0].incident_edges_deleted, 2);
    assert_eq!(result[0].relationships_deleted, 3);
    assert_eq!(guarded_reads, 0, "cold relationship scans still held the writer guard");
    assert_eq!(relationship_records_between(&shard, 2, 1).await, 0);
    assert_eq!(relationship_records_between(&shard, 3, 1).await, 0);
    shard.close().await.unwrap();
}

/// A parallel relationship added to an already-connected pair writes no xlog
/// entry, so preparation validation cannot see it. The warm only admits
/// blocks; the guarded scan must still find and delete the new record.
#[tokio::test]
async fn warmed_relationship_scan_still_deletes_a_relationship_added_after_preparation() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let path = "prepared-delete-relationship-race";
    let shard = open_cached_delete_writer(path, store.clone(), 0).await;
    shard
        .set_vertex_metadata_batch("cell-0", (1..=2).map(|id| (id, VertexMetadata::default().with_label("Entity"))))
        .await
        .unwrap();
    shard
        .create_relationship(relationship_mutation(2, 1, "before"), EdgeMetadata::default())
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
    shard.close().await.unwrap();
    drop(shard);

    let shard = open_cached_delete_writer(path, store.clone(), 8 * 1024 * 1024).await;
    let deletions = vec![(1, "cleanup".into(), true, VertexMetadata::default())];
    let mode = VertexDeleteBatchMode::Detach;
    let prepared = shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap();
    assert!(prepared.is_some());
    let parallel = shard
        .create_relationship(relationship_mutation(2, 1, "after-warm"), EdgeMetadata::default())
        .await
        .unwrap();
    assert!(
        !parallel.structural_edge_inserted,
        "the race needs a parallel relationship, not a new edge"
    );
    assert_eq!(relationship_records_between(&shard, 2, 1).await, 2);

    let lock = shard.acquire_local_write_guard_no_fence("cell-0", mode.operation()).await.unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked("cell-0", &deletions, mode, mode.operation(), &lock, prepared)
        .await
        .unwrap();
    lock.release().await.unwrap();
    assert!(result[0].vertex_deleted);
    assert_eq!(
        result[0].relationships_deleted, 2,
        "the relationship written after the warm was not deleted"
    );
    assert_eq!(relationship_records_between(&shard, 2, 1).await, 0);
    shard.close().await.unwrap();
}

/// Remembers the fields a span carried, both the ones set at creation and the
/// ones filled in later.
///
/// Hand-rolled against the `tracing` facade, like `RecordedFields` in
/// `query_optimizer`: `tracing-subscriber` is deliberately not a dependency of
/// this library.
struct RecordedWarmFields(Arc<std::sync::Mutex<Vec<(String, String)>>>);

impl tracing::field::Visit for RecordedWarmFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .lock()
            .expect("field capture mutex is not poisoned")
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

impl tracing::Subscriber for RecordedWarmFields {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        span.record(&mut RecordedWarmFields(self.0.clone()));
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        values.record(&mut RecordedWarmFields(self.0.clone()));
    }

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Signals when a span with this name opens, and records nothing else.
struct SpanOpened {
    name: &'static str,
    opened: Arc<tokio::sync::Notify>,
}

impl tracing::Subscriber for SpanOpened {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        if span.metadata().name() == self.name {
            // `notify_one` keeps the permit if nobody is waiting yet.
            self.opened.notify_one();
        }
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// The point warm and the relationship warm spend one budget between them, and
/// the relationship warm stops when it is gone.
///
/// The first version of the relationship warm opened a fresh counter at
/// `DELETE_APPLY_WARM_MAX_KEYS`, so a preparation whose point keys had already
/// spent the budget could read that many entries again before admitting the
/// writer — twice the pre-guard work the budget promises, which is the delay
/// this whole path exists to remove.
///
/// The fixture writes more relationship records than the leftover budget allows,
/// so the assertions are on entries actually read: a warm that took the budget
/// and then ignored it would read all 612 and fail here.
#[tokio::test]
async fn relationship_warm_reads_only_the_budget_the_point_warm_left() {
    /// One short of the point warm's admission gate: 1 vertex + 5 * 204 + 2
    /// estimated keys is 1_023 of 1_024.
    const EDGES: usize = 204;
    const RECORDS_PER_EDGE: usize = 3;

    let shard = open_cached_delete_writer(
        "prepared-delete-warm-budget",
        crate::tests::ReadCountingObjectStore::new(),
        8 * 1024 * 1024,
    )
    .await;
    let sources = 2..=(EDGES as VertexId + 1);
    shard
        .set_vertex_metadata_batch(
            "cell-0",
            std::iter::once(1)
                .chain(sources.clone())
                .map(|id| (id, VertexMetadata::default().with_label("Entity"))),
        )
        .await
        .unwrap();
    // Every source keeps one edge to vertex 1 carrying three relationship
    // records, so each warmed prefix has more than one entry to read.
    for src in sources.clone() {
        for nth in 0..RECORDS_PER_EDGE {
            shard
                .create_relationship(relationship_mutation(src, 1, &format!("r-{src}-{nth}")), EdgeMetadata::default())
                .await
                .unwrap();
        }
    }
    let vertices = BTreeSet::from([1]);
    let edges = sources
        .map(|src| IncidentEdge {
            edge_type: "PRESENT_IN".into(),
            src,
            dst: 1,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(edges.len(), EDGES);

    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let snapshot = shard.snapshot("cell-0").await.unwrap();
    {
        use tracing::instrument::WithSubscriber;
        shard
            .warm_vertex_delete_apply_reads(&snapshot, &vertices, &vertices, &edges)
            .with_subscriber(RecordedWarmFields(captured.clone()))
            .await
            .unwrap();
    }
    let fields = captured.lock().unwrap().clone();
    let field = |name: &str| {
        fields
            .iter()
            .find(|(recorded, _)| recorded == name)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| panic!("the relationship warm records {name}, got {fields:?}"))
    };
    let number = |name: &str| field(name).parse::<usize>().expect("the field is recorded as a number");

    // The point warm reads the vertex key, two cell markers, and an out-edge,
    // metadata and out-degree key per edge; the in-degree and xlog low-water
    // keys dedup to one apiece, because every edge shares a destination and a
    // type. What is left of the 1_024 is the relationship warm's to spend.
    let point_keys = 1 + 2 + EDGES * 3 + 1 + 1;
    let budget = DELETE_APPLY_WARM_MAX_KEYS - point_keys;
    assert_eq!(
        number("hydradb.warm.budget"),
        budget,
        "the relationship warm must spend what the point warm left, not a fresh budget"
    );

    let entries = number("hydradb.warm.entries");
    let seeks = number("hydradb.warm.seeks");
    let records = EDGES * RECORDS_PER_EDGE;
    assert!(records > budget, "the fixture must write more records than the budget allows");
    assert_eq!(
        field("hydradb.warm.truncated"),
        "true",
        "the warm read {entries} of {records} entries without truncating"
    );
    assert!(entries > 0, "the warm stopped before reading any record");
    // Every read is charged before it is made, seeks and records alike, so the
    // bound is exact however many prefixes warm concurrently.
    assert!(seeks <= budget, "the warm made {seeks} seeks against a budget of {budget}");
    assert!(
        seeks + entries <= budget,
        "the warm spent {seeks} seeks and {entries} entries against a budget of {budget}"
    );
    shard.close().await.unwrap();
}

/// A prefix seek costs a read even when the prefix holds no relationship
/// records, which is why the budget charges the seek and not only what it
/// returns. Devin on #142: while only records were charged, a detach of plain
/// structural edges opened a prefix per edge however little budget was left,
/// delaying writer admission to warm nothing.
///
/// Distinct edge types are what make the point warm expensive here: nothing
/// dedups, so 204 edges spend 1_023 of the 1_024 keys and leave a budget of one.
#[tokio::test]
async fn relationship_warm_charges_a_seek_that_finds_no_records() {
    const EDGES: usize = 204;

    let shard = open_cached_delete_writer(
        "prepared-delete-warm-empty-prefixes",
        crate::tests::ReadCountingObjectStore::new(),
        8 * 1024 * 1024,
    )
    .await;
    let vertices = BTreeSet::from([1]);
    let edges = (2..=(EDGES as VertexId + 1))
        .map(|src| IncidentEdge {
            edge_type: format!("PRESENT_IN_{src}"),
            src,
            dst: 1,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(edges.len(), EDGES);

    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let snapshot = shard.snapshot("cell-0").await.unwrap();
    {
        use tracing::instrument::WithSubscriber;
        shard
            .warm_vertex_delete_apply_reads(&snapshot, &vertices, &vertices, &edges)
            .with_subscriber(RecordedWarmFields(captured.clone()))
            .await
            .unwrap();
    }
    let fields = captured.lock().unwrap().clone();
    let number = |name: &str| {
        fields
            .iter()
            .find(|(recorded, _)| recorded == name)
            .map(|(_, value)| value.parse::<usize>().expect("the field is recorded as a number"))
            .unwrap_or_else(|| panic!("the relationship warm records {name}, got {fields:?}"))
    };

    // Nothing dedups across distinct types: the vertex key, two cell markers and
    // five keys per edge.
    let budget = DELETE_APPLY_WARM_MAX_KEYS - (1 + 2 + EDGES * 5);
    assert_eq!(
        number("hydradb.warm.budget"),
        budget,
        "the point warm should have spent all but {budget}"
    );
    assert_eq!(
        number("hydradb.warm.prefixes"),
        EDGES,
        "every detached edge contributes a prefix to warm"
    );
    assert_eq!(number("hydradb.warm.entries"), 0, "the fixture writes no relationship records");
    let seeks = number("hydradb.warm.seeks");
    assert!(
        seeks <= budget,
        "the warm made {seeks} seeks on {EDGES} empty prefixes against a budget of {budget}"
    );
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_revalidates_new_edge_types_but_allows_metadata_writes() {
    let shard = GraphShard::open_standalone_writer(
        "prepared-delete-revalidation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    shard
        .set_vertex_metadata("cell-0", 1, VertexMetadata::default().with_label("Entity"))
        .await
        .unwrap();
    let deletions = vec![(1, "cleanup".into(), false, VertexMetadata::default())];
    let mode = VertexDeleteBatchMode::IsolatedOnly;
    let prepared = shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap();
    assert!(prepared.as_ref().unwrap().discovery.edges.is_empty());
    shard
        .write_edges_batch("cell-0", "NEW_TYPE", [(2, 1)], "concurrent-edge")
        .await
        .unwrap();
    let lock = shard.acquire_local_write_guard("cell-0", "test").await.unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked("cell-0", &deletions, mode, mode.operation(), &lock, prepared)
        .await;
    lock.release().await.unwrap();
    assert!(matches!(result, Err(GraphError::ConditionalWriteConflict { .. })));
    let retained = shard
        .delete_vertex_mutation_requests_batch_with_mode("cell-0", deletions.clone(), mode)
        .await
        .unwrap();
    assert!(!retained[0].vertex_deleted);

    shard
        .set_vertex_metadata("cell-0", 3, VertexMetadata::default().with_label("Entity"))
        .await
        .unwrap();
    let deletions = vec![(3, "metadata-race".into(), false, VertexMetadata::default())];
    let prepared = shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap();
    shard
        .set_vertex_metadata("cell-0", 3, VertexMetadata::default().with_label("Changed"))
        .await
        .unwrap();
    let lock = shard.acquire_local_write_guard("cell-0", "test").await.unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked("cell-0", &deletions, mode, mode.operation(), &lock, prepared)
        .await
        .unwrap();
    lock.release().await.unwrap();
    assert!(result[0].vertex_deleted);
    // The transaction must remove the newly written label, not stale metadata.
    let snapshot = shard.snapshot("cell-0").await.unwrap();
    assert!(snapshot
        .storage_snapshot
        .get_with_options(keys::vertex("cell-0", 3).as_bytes(), &slatedb::config::ReadOptions::default())
        .await
        .unwrap()
        .is_none());
    assert!(
        snapshot
            .storage_snapshot
            .get_with_options(
                keys::vertex_label("cell-0", "Changed", 3).as_bytes(),
                &slatedb::config::ReadOptions::default(),
            )
            .await
            .unwrap()
            .is_none(),
        "deletion retained the concurrently written label index"
    );
    drop(snapshot);
    shard.close().await.unwrap();
}

/// Fill the old process-wide preparation limit with fallback requests waiting
/// on one graph's writer, then prove another graph can still prepare and delete.
async fn assert_delete_preparation_fallback_does_not_block_other_graphs(
    shard: &GraphShard,
    deletions: Vec<(VertexId, String, bool, VertexMetadata)>,
) {
    let mode = VertexDeleteBatchMode::Detach;
    assert!(shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap().is_none());
    let unrelated = GraphShard::open_standalone_writer("unrelated-preparation", Arc::new(slatedb::object_store::memory::InMemory::new()))
        .await
        .unwrap();
    unrelated
        .set_vertex_metadata("cell-1", 1, VertexMetadata::default().with_label("Entity"))
        .await
        .unwrap();
    let held = shard.acquire_graph_write_permit("test-held-writer").await.unwrap();
    let waits_before = shard.operation_metrics.backpressure_waits.load(Ordering::Relaxed);
    let blocked =
        futures::future::join_all((0..4).map(|_| shard.delete_vertex_mutation_requests_batch_with_mode("cell-0", deletions.clone(), mode)));
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            _ = blocked => panic!("fallback deletes passed the held writer permit"),
            result = async {
                // A writer wait proves each request has finished preparation;
                // no sleep or global available-permit count is needed.
                while shard.operation_metrics.backpressure_waits.load(Ordering::Relaxed)
                    < waits_before + 4
                {
                    tokio::task::yield_now().await;
                }
                unrelated.detach_delete_vertex("cell-1", 1, "unrelated-cleanup").await
            } => result,
        }
    })
    .await;
    // Dropping the blocked futures cancels them before they can mutate. Release
    // all resources even when running this regression against the broken code.
    drop(held);
    unrelated.close().await.unwrap();
    assert!(
        result
            .expect("fallback deletes monopolized global preparation slots while waiting for a writer")
            .unwrap()
            .vertex_deleted
    );
}

#[tokio::test]
async fn delete_preparation_oversized_batches_do_not_block_other_graphs() {
    let shard = GraphShard::open_standalone_writer(
        "oversized-delete-preparation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    let deletions = (0..=DELETE_PREPARATION_MAX_VERTICES)
        .map(|vertex| (vertex as VertexId, format!("cleanup-{vertex}"), true, VertexMetadata::default()))
        .collect();
    assert_delete_preparation_fallback_does_not_block_other_graphs(&shard, deletions).await;
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_byte_limit_fallback_does_not_block_other_graphs() {
    let shard = GraphShard::open_standalone_writer(
        "byte-limit-delete-preparation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    // Long but valid type names reach the actual 8 MiB retained-edge limit with
    // a small fixture. Seed the incident index directly; requests are cancelled
    // at writer admission, so this fixture never enters a delete transaction.
    let edge_type = "T".repeat(4096);
    let edge_count = DELETE_PREPARATION_BYTES / (edge_type.len() as u64 + 128) + 1;
    let mut batch = WriteBatch::new();
    batch.put(keys::matrix_dirty("cell-0", &edge_type).as_bytes(), encode_u64(1));
    for dst in 2..edge_count + 2 {
        batch.put(keys::out_edge("cell-0", &edge_type, 1, dst).as_bytes(), b"graph-edge\n".as_slice());
    }
    shard.write_strict_for_test(batch).await.unwrap();
    assert_delete_preparation_fallback_does_not_block_other_graphs(&shard, vec![(1, "cleanup".into(), true, VertexMetadata::default())])
        .await;
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_replays_do_not_block_other_graphs() {
    let shard = GraphShard::open_standalone_writer(
        "replayed-delete-preparation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    shard.detach_delete_vertex("cell-0", 1, "cleanup").await.unwrap();
    assert_delete_preparation_fallback_does_not_block_other_graphs(&shard, vec![(1, "cleanup".into(), true, VertexMetadata::default())])
        .await;
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_rechecks_partially_replayed_batches() {
    let shard = GraphShard::open_standalone_writer("prepared-delete-replay", Arc::new(slatedb::object_store::memory::InMemory::new()))
        .await
        .unwrap();
    shard
        .set_vertex_metadata_batch(
            "cell-0",
            [
                (1, VertexMetadata::default().with_label("Source")),
                (2, VertexMetadata::default().with_label("Entity")),
            ],
        )
        .await
        .unwrap();
    let deletions = vec![
        (1, "source".into(), true, VertexMetadata::default()),
        (2, "entity".into(), false, VertexMetadata::default()),
    ];
    let mode = VertexDeleteBatchMode::DetachAndIsolated;
    let prepared = shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap();
    shard
        .delete_vertex_mutation_requests_batch_with_mode("cell-0", vec![deletions[0].clone()], mode)
        .await
        .unwrap();
    let lock = shard.acquire_local_write_guard("cell-0", "test").await.unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked("cell-0", &deletions, mode, mode.operation(), &lock, prepared)
        .await;
    lock.release().await.unwrap();
    assert!(matches!(result, Err(GraphError::ConditionalWriteConflict { .. })));
    shard
        .set_vertex_metadata("cell-0", 1, VertexMetadata::default().with_label("NewSource"))
        .await
        .unwrap();
    shard.write_edges_batch("cell-0", "NEW_EDGE", [(2, 1)], "recreated").await.unwrap();
    let retried = shard
        .delete_vertex_mutation_requests_batch_with_mode("cell-0", deletions, mode)
        .await
        .unwrap();
    assert!(retried[0].vertex_deleted, "source returns its stored replay result");
    assert!(!retried[1].vertex_deleted, "the replayed source must not detach a new relationship");
    assert_eq!(shard.out_degree("cell-0", "NEW_EDGE", 2).await.unwrap(), 1);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_uses_retained_xlog_for_unrelated_topology_changes() {
    let shard = GraphShard::open_standalone_writer("prepared-delete-xlog", Arc::new(slatedb::object_store::memory::InMemory::new()))
        .await
        .unwrap();
    shard
        .set_vertex_metadata_batch("cell-0", (1..=3).map(|id| (id, VertexMetadata::default().with_label("Entity"))))
        .await
        .unwrap();
    shard.write_edges_batch("cell-0", "TYPE", [(900, 901)], "seed").await.unwrap();
    let mode = VertexDeleteBatchMode::IsolatedOnly;
    for vertex in 1..=3 {
        let deletions = vec![(vertex, format!("cleanup-{vertex}"), false, VertexMetadata::default())];
        let prepared = shard.prepare_vertex_delete("cell-0", &deletions, mode).await.unwrap();
        let destination = if vertex == 2 { vertex } else { 910 + vertex };
        shard
            .write_edges_batch("cell-0", "TYPE", [(900, destination)], &format!("race-{vertex}"))
            .await
            .unwrap();
        if vertex == 3 {
            // A collected suffix cannot be used to prove that no target changed.
            shard
                .storage
                .writer()
                .unwrap()
                .put(keys::xlog_low_water("cell-0", "TYPE").as_bytes(), encode_u64(u64::MAX))
                .await
                .unwrap();
        }
        let lock = shard.acquire_local_write_guard("cell-0", "test").await.unwrap();
        let result = shard
            .delete_vertex_mutations_batch_txn_locked("cell-0", &deletions, mode, mode.operation(), &lock, prepared)
            .await;
        lock.release().await.unwrap();
        if vertex == 1 {
            assert!(
                result.unwrap()[0].vertex_deleted,
                "unrelated edge writes should not restart cleanup"
            );
        } else {
            assert!(
                matches!(result, Err(GraphError::ConditionalWriteConflict { .. })),
                "affected topology or a coverage gap must reject preparation"
            );
        }
    }
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_validation_overlaps_types_and_reuses_bounded_xlog_blocks() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = GraphShard::open_standalone_writer_with_options(
        "prepared-validation-block-reuse",
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    shard.set_vertex_metadata("cell-0", 1, VertexMetadata::default()).await.unwrap();
    for i in 0..8 {
        shard
            .write_edges_batch("cell-0", &format!("TYPE{i}"), [(900, 901)], &format!("seed-{i}"))
            .await
            .unwrap();
    }
    let prepared = shard
        .prepare_vertex_delete(
            "cell-0",
            &[(1, "delete-1".into(), false, VertexMetadata::default())],
            VertexDeleteBatchMode::IsolatedOnly,
        )
        .await
        .unwrap()
        .unwrap();
    // Enough unrelated changes to occupy multiple data blocks, while remaining
    // below the validation scan cap. They must not invalidate this deletion.
    for edge_type in 0..8 {
        shard
            .write_edges_batch(
                "cell-0",
                &format!("TYPE{edge_type}"),
                (0..250).map(|i| (10_000 + i, 20_000 + i)),
                &format!("unrelated-{edge_type}"),
            )
            .await
            .unwrap();
    }
    shard
        .storage
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
    drop(shard);
    let mut shard = GraphShard::open_standalone_writer_with_options(
        "prepared-validation-block-reuse",
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let txn = shard.storage.writer().unwrap().begin(IsolationLevel::SerializableSnapshot).unwrap();
    store.delay_reads_and_reset_peak(10);
    let validation_start = std::time::Instant::now();
    let before = store.compacted_reads();
    assert!(shard.vertex_delete_preparation_is_current(&txn, "cell-0", &prepared).await.unwrap());
    let cold_reads = store.compacted_reads() - before;
    let peak_reads = store.peak_reads();
    let cold_validation_elapsed = validation_start.elapsed();
    store.delay_reads_and_reset_peak(0);
    let before = store.compacted_reads();
    assert!(shard.vertex_delete_preparation_is_current(&txn, "cell-0", &prepared).await.unwrap());
    let repeat_reads = store.compacted_reads() - before;
    // Each history has fewer than 1,000 entries, but their shared total is
    // 2,000. The parallel validator must enforce a single allowance.
    shard.limits.max_query_scan_edges = 1_000;
    assert!(!shard.vertex_delete_preparation_is_current(&txn, "cell-0", &prepared).await.unwrap());
    drop(txn);
    assert!(
        peak_reads > 1 && peak_reads <= VERTEX_DELETE_READ_CONCURRENCY as u64,
        "cold history reads did not overlap within their bound: {peak_reads}"
    );
    shard.close().await.unwrap();
    eprintln!("delete validation object reads: cold={cold_reads}, repeated={repeat_reads}, peak={peak_reads}, elapsed={cold_validation_elapsed:?}");
    assert!(cold_reads > 0, "validation must read flushed data blocks");
    assert_eq!(
        repeat_reads, 0,
        "validation repeated {repeat_reads} object reads after {cold_reads} cold reads"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_storage_wait_does_not_hold_writer_gate() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = Arc::new(
        GraphShard::open_standalone_writer_with_options(
            "cleanup-no-writer-blocking",
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
        .set_vertex_metadata("cell-0", 1, VertexMetadata::default().with_label("Source"))
        .await
        .unwrap();
    shard
        .set_vertex_metadata("cell-0", 2, VertexMetadata::default().with_label("Chunk"))
        .await
        .unwrap();
    shard.write_edges_batch("cell-0", "HAS_CHUNK", [(2, 1)], "seed").await.unwrap();
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
            shard
                .delete_vertices_and_isolated_candidates_batch(
                    "cell-0",
                    vec![
                        (1, "cleanup".into(), true, VertexMetadata::default()),
                        (2, "candidate".into(), false, VertexMetadata::default()),
                    ],
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), started)
        .await
        .unwrap()
        .unwrap();
    let before = std::time::Instant::now();
    let changed = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        shard.set_vertex_metadata("cell-0", 9000, VertexMetadata::default().with_label("Concurrent")),
    )
    .await;
    let latency = before.elapsed();
    let _ = release.send(());
    let deleted = deleting.await.unwrap().unwrap();
    assert!(deleted.iter().all(|result| result.vertex_deleted));
    shard.close().await.unwrap();
    changed.expect("cleanup storage I/O held the graph writer gate").unwrap();
    eprintln!("write during paused cleanup completed in {} us", latency.as_micros());
}

fn merge_import_options() -> RelationshipImportOptions<'static> {
    RelationshipImportOptions {
        endpoint_labels: Some(("Entity", "Chunk")),
        create_always: false,
        update_existing_metadata: true,
        merge_policy: None,
        #[cfg(feature = "opencypher")]
        guarded_rows: None,
        operation: "merge_relationships_batch_between_labeled_vertices",
    }
}

fn merge_rows(pairs: impl IntoIterator<Item = (VertexId, VertexId, RelationshipId)>) -> Vec<RelationshipMutation> {
    let mut rows = pairs
        .into_iter()
        .map(|(src, dst, id)| RelationshipMutation {
            cell_id: "cell-0".to_string(),
            edge_type: "PRESENT_IN".to_string(),
            src,
            dst,
            relationship_id: id,
            metadata: EdgeMetadata::default().with_property("id", VertexPropertyValue::Integer(id)),
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.relationship_id, row.src, row.dst, row.edge_type.clone()));
    rows
}

/// Seed labelled endpoints with existing relationships (so their degree
/// counters and identity indexes live in SSTs), plus unrelated relationships
/// so the targets do not share one block with everything else. Flushed and
/// closed, so the next open starts with a cold block cache.
async fn seed_merge_scope(path: &str, store: Arc<crate::tests::ReadCountingObjectStore>) {
    let shard = open_cached_delete_writer(path, store, 0).await;
    let vertices = (1..=400).flat_map(|i| {
        [
            (i, VertexMetadata::default().with_label("Entity")),
            (100_000 + i, VertexMetadata::default().with_label("Chunk")),
        ]
    });
    shard.import_vertex_metadata_batch("cell-0", vertices).await.unwrap();
    for chunk in (1..=400u64).collect::<Vec<_>>().chunks(100) {
        shard
            .merge_relationships_batch_between_labeled_vertices(
                "cell-0",
                "PRESENT_IN",
                merge_rows(chunk.iter().map(|i| (*i, 100_000 + *i, 1_000_000 + *i))),
                &format!("seed-{}", chunk[0]),
                ("Entity", "Chunk"),
                None,
            )
            .await
            .unwrap();
    }
    shard
        .storage
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
}

/// Object reads made by the guarded MERGE transaction for new relationships
/// between existing endpoints, optionally after the pre-permit warm.
async fn guarded_merge_object_reads(path: &str, store: Arc<crate::tests::ReadCountingObjectStore>, warm: bool) -> u64 {
    let shard = open_cached_delete_writer(path, store.clone(), 8 * 1024 * 1024).await;
    // New identities between endpoints spread across the seeded range.
    let rows = merge_rows((0..20u64).map(|i| (1 + i * 19, 100_001 + i * 19, 2_000_000 + i)));
    let options = merge_import_options();
    if warm {
        shard
            .warm_relationship_import_reads("cell-0", "PRESENT_IN", &rows, &format!("probe-{warm}"), options)
            .await
            .unwrap();
    }
    let fingerprint = crate::relationship_import_fingerprint("cell-0", "PRESENT_IN", &rows);
    let before = store.compacted_reads();
    let result = shard
        .import_relationships_batch_txn("cell-0", "PRESENT_IN", &rows, &format!("probe-{warm}"), fingerprint, options)
        .await
        .unwrap();
    let reads = store.compacted_reads() - before;
    assert_eq!(result.relationships_inserted, 20);
    shard.close().await.unwrap();
    reads
}

#[tokio::test]
async fn relationship_merge_warm_moves_object_reads_out_of_the_writer_guard() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_merge_scope("merge-warm-cold", store.clone()).await;
    let cold = guarded_merge_object_reads("merge-warm-cold", store.clone(), false).await;
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_merge_scope("merge-warm-warm", store.clone()).await;
    let warmed = guarded_merge_object_reads("merge-warm-warm", store.clone(), true).await;
    eprintln!("guarded object reads: cold={cold} warmed={warmed}");
    assert!(cold > 0, "the cold guarded MERGE must read SSTs, or this test proves nothing");
    // Exact counts depend on how this small scope packs into blocks (27 cold,
    // 1 warmed when written); the claim is that the warm covers the guarded
    // reads, not a particular layout.
    assert!(
        warmed * 10 <= cold,
        "guarded MERGE still made {warmed} object reads after the warm (cold: {cold})"
    );
}

/// The warm reads "no merge pointer" for a new identity and discards it. If
/// another MERGE creates that identity before this batch takes the permit, the
/// guarded lookup must find it (and count it as existing), not trust anything
/// the warm saw and insert a duplicate.
#[tokio::test]
async fn relationship_merge_warm_does_not_hide_an_identity_created_after_it() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_merge_scope("merge-warm-race", store.clone()).await;
    let shard = open_cached_delete_writer("merge-warm-race", store.clone(), 8 * 1024 * 1024).await;
    let rows = merge_rows([(7, 100_007, 3_000_000)]);
    let options = merge_import_options();
    shard
        .warm_relationship_import_reads("cell-0", "PRESENT_IN", &rows, "late", options)
        .await
        .unwrap();

    let first = shard
        .merge_relationships_batch_between_labeled_vertices("cell-0", "PRESENT_IN", rows.clone(), "early", ("Entity", "Chunk"), None)
        .await
        .unwrap();
    assert_eq!(first.relationships_inserted, 1);

    let fingerprint = crate::relationship_import_fingerprint("cell-0", "PRESENT_IN", &rows);
    let late = shard
        .import_relationships_batch_txn("cell-0", "PRESENT_IN", &rows, "late", fingerprint, options)
        .await
        .unwrap();
    assert_eq!(late.relationships_inserted, 0, "the warm hid an identity created after it");
    assert_eq!(late.relationships_already_existed, 1);
    assert_eq!(
        relationship_records_between_typed(&shard, "PRESENT_IN", 7, 100_007).await,
        2,
        "expected the seeded relationship plus exactly one new one"
    );
    shard.close().await.unwrap();
}

/// Greptile on #142: an idempotent replay warmed the whole batch before the
/// guarded transaction looked at its durable result, then the transaction
/// returned that result without reading a relationship. Every one of those reads
/// was spent for nothing, on the latency path the warm exists to shorten.
#[tokio::test]
async fn relationship_merge_warm_skips_a_replay() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_merge_scope("merge-warm-replay", store.clone()).await;
    let shard = open_cached_delete_writer("merge-warm-replay", store.clone(), 8 * 1024 * 1024).await;
    let rows = merge_rows([(7, 100_007, 3_000_101)]);
    let options = merge_import_options();
    shard
        .merge_relationships_batch_between_labeled_vertices("cell-0", "PRESENT_IN", rows.clone(), "once", ("Entity", "Chunk"), None)
        .await
        .unwrap();

    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        use tracing::instrument::WithSubscriber;
        shard
            .warm_relationship_import_reads("cell-0", "PRESENT_IN", &rows, "once", options)
            .with_subscriber(RecordedWarmFields(captured.clone()))
            .await
            .unwrap();
    }
    let fields = captured.lock().unwrap().clone();
    assert!(
        fields.iter().any(|(name, value)| name == "hydradb.warm.replay" && value == "true"),
        "the warm did not recognise the replay: {fields:?}"
    );
    assert!(
        !fields.iter().any(|(name, _)| name == "hydradb.warm.point_keys"),
        "a replay must stop before warming the batch: {fields:?}"
    );
    shard.close().await.unwrap();
}

/// Greptile on #142: the row gate bounds rows, not reads. One row costs up to
/// two endpoint keys, the direct edge, two degree counters, the relationship
/// counter, a merge pointer and then a record or a bounded scan, so a batch
/// inside the row gate could admit several thousand blocks and evict what the
/// cache was holding. The reads are now bounded too.
#[tokio::test]
async fn relationship_merge_warm_stops_at_its_read_budget() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_merge_scope("merge-warm-read-budget", store.clone()).await;
    let shard = open_cached_delete_writer("merge-warm-read-budget", store.clone(), 8 * 1024 * 1024).await;
    // Distinct endpoints throughout, so nothing dedups: 700 rows are well inside
    // the row gate and well past the read budget.
    let rows = merge_rows((0..700).map(|n| (1_000 + n as VertexId, 200_000 + n as VertexId, 4_000_000 + n as RelationshipId)));
    assert!(
        rows.len() <= RELATIONSHIP_IMPORT_WARM_MAX_ROWS,
        "the fixture must pass the row gate"
    );

    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        use tracing::instrument::WithSubscriber;
        shard
            .warm_relationship_import_reads("cell-0", "PRESENT_IN", &rows, "read-budget", merge_import_options())
            .with_subscriber(RecordedWarmFields(captured.clone()))
            .await
            .unwrap();
    }
    let fields = captured.lock().unwrap().clone();
    let field = |name: &str| {
        fields
            .iter()
            .find(|(recorded, _)| recorded == name)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| panic!("the warm records {name}, got {fields:?}"))
    };
    let reads = field("hydradb.warm.reads").parse::<usize>().expect("reads is a number");
    assert_eq!(
        field("hydradb.warm.budget").parse::<usize>().unwrap(),
        RELATIONSHIP_IMPORT_WARM_MAX_READS
    );
    assert_eq!(
        field("hydradb.warm.truncated"),
        "true",
        "the warm made {reads} reads without exhausting its budget"
    );
    assert!(
        reads <= RELATIONSHIP_IMPORT_WARM_MAX_READS,
        "the warm made {reads} reads against a budget of {RELATIONSHIP_IMPORT_WARM_MAX_READS}"
    );
    shard.close().await.unwrap();
}

async fn relationship_records_between_typed(shard: &GraphShard, edge_type: &str, src: VertexId, dst: VertexId) -> usize {
    let snapshot = shard.snapshot("cell-0").await.unwrap();
    let prefix = keys::relationship_edge_prefix("cell-0", edge_type, src, dst);
    let mut iter = snapshot
        .storage_snapshot
        .scan_prefix_with_options(prefix.as_bytes(), .., &slatedb::config::ScanOptions::default())
        .await
        .unwrap();
    let mut records = 0;
    while iter.next().await.unwrap().is_some() {
        records += 1;
    }
    records
}

/// Seed a relationship-delete scope: one relationship on every edge
/// (`1_000_000 + i`) and a second on every even edge (`1_500_000 + i`), so a
/// delete of the first leaves a survivor on even edges and removes the edge
/// on odd ones. Flushed and closed, so the next open starts cold.
async fn seed_relationship_delete_scope(path: &str, store: Arc<crate::tests::ReadCountingObjectStore>) {
    let shard = open_cached_delete_writer(path, store, 0).await;
    let vertices = (1..=400).flat_map(|i| {
        [
            (i, VertexMetadata::default().with_label("Entity")),
            (100_000 + i, VertexMetadata::default().with_label("Chunk")),
        ]
    });
    shard.import_vertex_metadata_batch("cell-0", vertices).await.unwrap();
    for chunk in (1..=400u64).collect::<Vec<_>>().chunks(100) {
        let rows = chunk.iter().flat_map(|i| {
            let first = (*i, 100_000 + *i, 1_000_000 + *i);
            let second = (i % 2 == 0).then_some((*i, 100_000 + *i, 1_500_000 + *i));
            std::iter::once(first).chain(second)
        });
        shard
            .merge_relationships_batch_between_labeled_vertices(
                "cell-0",
                "PRESENT_IN",
                merge_rows(rows),
                &format!("seed-{}", chunk[0]),
                ("Entity", "Chunk"),
                None,
            )
            .await
            .unwrap();
    }
    shard
        .storage
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
}

/// The stored id of the relationship MERGE created for `identity`: MERGE
/// allocates relationship ids itself and keeps the caller's as the `id`
/// property.
async fn relationship_id_for_identity(shard: &GraphShard, src: VertexId, dst: VertexId, identity: u64) -> RelationshipId {
    let snapshot = shard.snapshot("cell-0").await.unwrap();
    let prefix = keys::relationship_edge_prefix("cell-0", "PRESENT_IN", src, dst);
    let mut iter = snapshot
        .storage_snapshot
        .scan_prefix_with_options(prefix.as_bytes(), .., &slatedb::config::ScanOptions::default())
        .await
        .unwrap();
    while let Some(kv) = iter.next().await.unwrap() {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        let record = decode_relationship_record(&key, &kv.value).unwrap();
        if record.metadata.properties.get("id") == Some(&VertexPropertyValue::Integer(identity)) {
            return record.relationship_id;
        }
    }
    panic!("no relationship {src}->{dst} carries identity {identity}");
}

/// Deletions of the first relationship (identity `1_000_000 + src`) on each
/// seeded edge from `srcs`.
async fn relationship_delete_rows(
    shard: &GraphShard,
    key: &str,
    srcs: impl IntoIterator<Item = VertexId>,
) -> Vec<(EdgeMutation, RelationshipId)> {
    let mut rows = Vec::new();
    for src in srcs {
        let relationship_id = relationship_id_for_identity(shard, src, 100_000 + src, 1_000_000 + src).await;
        rows.push((relationship_mutation(src, 100_000 + src, &format!("{key}-{src}")), relationship_id));
    }
    rows
}

/// 20 edges spread across the seeded range, half of which keep a second
/// relationship.
fn spread_edges() -> impl Iterator<Item = VertexId> {
    (0..20u64).map(|n| 1 + n * 19)
}

/// Object reads made by the guarded relationship-delete transaction, optionally
/// after the pre-permit warm.
async fn guarded_relationship_delete_object_reads(path: &str, store: Arc<crate::tests::ReadCountingObjectStore>, warm: bool) -> u64 {
    // Resolve the ids on a separate open, so the lookup scan does not warm the
    // cache the measured open starts with.
    let rows = {
        let shard = open_cached_delete_writer(path, store.clone(), 0).await;
        let rows = relationship_delete_rows(&shard, "probe", spread_edges()).await;
        shard.close().await.unwrap();
        rows
    };
    let shard = open_cached_delete_writer(path, store.clone(), 8 * 1024 * 1024).await;
    if warm {
        shard.warm_relationship_delete_reads("cell-0", &rows, &[]).await.unwrap();
    }
    let before = store.compacted_reads();
    let result = shard.delete_relationship_mutations_batch_txn("cell-0", &rows, &[]).await.unwrap();
    let reads = store.compacted_reads() - before;
    assert_eq!(result.deleted, 20);
    assert!(result.topology_sequence.is_some(), "the odd edges lost their last relationship");
    for (index, (mutation, _)) in rows.iter().enumerate() {
        let survives = mutation.src % 2 == 0;
        assert_eq!(
            shard.edge_exists("cell-0", "PRESENT_IN", mutation.src, mutation.dst).await.unwrap(),
            survives,
            "row {index}: edge {}->{}",
            mutation.src,
            mutation.dst
        );
        assert_eq!(
            shard
                .read_counter(&keys::relationship_count("cell-0", "PRESENT_IN", mutation.src, mutation.dst))
                .await
                .unwrap(),
            u64::from(survives)
        );
    }
    shard.close().await.unwrap();
    reads
}

#[tokio::test]
async fn relationship_delete_warm_moves_object_reads_out_of_the_writer_guard() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_relationship_delete_scope("reldelete-warm-cold", store.clone()).await;
    let cold = guarded_relationship_delete_object_reads("reldelete-warm-cold", store.clone(), false).await;
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_relationship_delete_scope("reldelete-warm-warm", store.clone()).await;
    let warmed = guarded_relationship_delete_object_reads("reldelete-warm-warm", store.clone(), true).await;
    eprintln!("guarded object reads: cold={cold} warmed={warmed}");
    assert!(cold > 0, "the cold guarded delete must read SSTs, or this test proves nothing");
    assert!(
        warmed * 10 <= cold,
        "guarded relationship delete still made {warmed} object reads after the warm (cold: {cold})"
    );
}

/// The warm sees an edge whose only relationship is being deleted. If another
/// relationship lands on that edge before the delete takes the permit, the
/// guarded survivor check must find it and keep the edge, not trust anything
/// the warm saw.
#[tokio::test]
async fn relationship_delete_warm_does_not_hide_a_relationship_added_after_it() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_relationship_delete_scope("reldelete-warm-race", store.clone()).await;
    let shard = open_cached_delete_writer("reldelete-warm-race", store.clone(), 8 * 1024 * 1024).await;
    let rows = relationship_delete_rows(&shard, "late", [7]).await;
    shard.warm_relationship_delete_reads("cell-0", &rows, &[]).await.unwrap();

    let added = shard
        .merge_relationships_batch_between_labeled_vertices(
            "cell-0",
            "PRESENT_IN",
            merge_rows([(7, 100_007, 3_000_007)]),
            "added",
            ("Entity", "Chunk"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(added.relationships_inserted, 1);

    let result = shard.delete_relationship_mutations_batch_txn("cell-0", &rows, &[]).await.unwrap();
    assert_eq!(result.deleted, 1);
    assert!(
        result.topology_sequence.is_none(),
        "the warm hid a relationship added after it and the edge was removed"
    );
    assert!(shard.edge_exists("cell-0", "PRESENT_IN", 7, 100_007).await.unwrap());
    assert_eq!(relationship_records_between_typed(&shard, "PRESENT_IN", 7, 100_007).await, 1);
    assert_eq!(
        shard
            .read_counter(&keys::relationship_count("cell-0", "PRESENT_IN", 7, 100_007))
            .await
            .unwrap(),
        1
    );
    shard.close().await.unwrap();
}

/// Run the relationship-delete warm and return the fields its span recorded.
async fn relationship_delete_warm_fields(
    shard: &GraphShard,
    rows: &[(EdgeMutation, RelationshipId)],
    guards: &[EdgeMutation],
) -> Vec<(String, String)> {
    use tracing::instrument::WithSubscriber;
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    shard
        .warm_relationship_delete_reads("cell-0", rows, guards)
        .with_subscriber(RecordedWarmFields(captured.clone()))
        .await
        .unwrap();
    let fields = captured.lock().unwrap().clone();
    fields
}

fn warm_field(fields: &[(String, String)], name: &str) -> String {
    fields
        .iter()
        .find(|(recorded, _)| recorded == name)
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| panic!("the warm records {name}, got {fields:?}"))
}

fn warm_count(fields: &[(String, String)], name: &str) -> usize {
    warm_field(fields, name).parse().unwrap_or_else(|_| panic!("{name} is a number"))
}

/// A batch whose reads exceed the budget stops warming at it, each part at its
/// share, and the guarded transaction still deletes everything.
#[tokio::test]
async fn relationship_delete_warm_stops_at_its_read_budget() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_relationship_delete_scope("reldelete-warm-read-budget", store.clone()).await;
    let shard = open_cached_delete_writer("reldelete-warm-read-budget", store.clone(), 8 * 1024 * 1024).await;
    // 400 edges need well over the budget in point keys and in prefixes alike.
    let rows = relationship_delete_rows(&shard, "budget", 1..=400).await;

    let fields = relationship_delete_warm_fields(&shard, &rows, &[]).await;
    let reads = warm_count(&fields, "hydradb.warm.reads");
    assert_eq!(
        warm_field(&fields, "hydradb.warm.truncated"),
        "true",
        "the warm made {reads} reads without exhausting its budget"
    );
    assert!(
        reads <= RELATIONSHIP_DELETE_WARM_MAX_READS,
        "the warm made {reads} reads against a budget of {RELATIONSHIP_DELETE_WARM_MAX_READS}"
    );
    // Both parts overflow, so each spends exactly its half.
    assert_eq!(
        warm_count(&fields, "hydradb.warm.prefix_reads"),
        RELATIONSHIP_DELETE_WARM_MAX_READS / 2
    );
    assert_eq!(
        warm_count(&fields, "hydradb.warm.point_reads"),
        RELATIONSHIP_DELETE_WARM_MAX_READS / 2
    );

    let result = shard.delete_relationship_mutations_batch_txn("cell-0", &rows, &[]).await.unwrap();
    assert_eq!(result.deleted, 400);
    shard.close().await.unwrap();
}

/// Greptile on #146: point reads and prefix scans raced for one allowance, so a
/// batch heavy in point keys could spend it before the survivor scans, the
/// measured cost under the guard, ran at all. Here the point keys alone exceed
/// the budget, and every survivor prefix is still warmed in full.
#[tokio::test]
async fn relationship_delete_warm_keeps_the_survivor_scans_when_point_keys_overflow() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_relationship_delete_scope("reldelete-warm-point-heavy", store.clone()).await;
    let shard = open_cached_delete_writer("reldelete-warm-point-heavy", store.clone(), 8 * 1024 * 1024).await;
    let rows = relationship_delete_rows(&shard, "point-heavy", 1..=100).await;
    // Replay guards are point keys only: enough of them to overflow the budget.
    let guards = (0..400u64)
        .map(|n| relationship_mutation(200 + n, 100_200 + n, &format!("guard-{n}")))
        .collect::<Vec<_>>();
    assert!(
        rows.len() <= RELATIONSHIP_DELETE_WARM_MAX_ROWS,
        "the fixture must pass the row gate"
    );

    let fields = relationship_delete_warm_fields(&shard, &rows, &guards).await;
    let point_keys = warm_count(&fields, "hydradb.warm.point_keys");
    assert!(
        point_keys > RELATIONSHIP_DELETE_WARM_MAX_READS,
        "point keys ({point_keys}) must overflow the budget"
    );
    assert_eq!(warm_field(&fields, "hydradb.warm.truncated"), "true");
    // Each prefix is a seek plus `deleting + 1` records: 100 edges, one
    // deletion each.
    assert_eq!(
        warm_count(&fields, "hydradb.warm.prefix_reads"),
        300,
        "the point keys crowded out the survivor scans: {fields:?}"
    );
    assert_eq!(warm_count(&fields, "hydradb.warm.reads"), RELATIONSHIP_DELETE_WARM_MAX_READS);
    shard.close().await.unwrap();
}

/// Greptile on #146: the warm built every key for the whole batch before the
/// read budget charged anything, so a bulk delete paid to assemble millions of
/// keys it would never read. Above the row gate it now builds none.
#[tokio::test]
async fn relationship_delete_warm_skips_a_batch_above_its_row_gate() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = open_cached_delete_writer("reldelete-warm-row-gate", store, 8 * 1024 * 1024).await;
    let rows = (0..=RELATIONSHIP_DELETE_WARM_MAX_ROWS as u64)
        .map(|n| (relationship_mutation(1 + n, 100_001 + n, &format!("bulk-{n}")), n))
        .collect::<Vec<_>>();
    let fields = relationship_delete_warm_fields(&shard, &rows, &[]).await;
    assert_eq!(warm_field(&fields, "hydradb.warm.skipped"), "true");
    assert!(
        !fields.iter().any(|(name, _)| name == "hydradb.warm.point_keys"),
        "a batch above the row gate must not build its keys: {fields:?}"
    );
    shard.close().await.unwrap();
}

/// Greptile on #146: the row gate counted replay guards, so 513 deletions on
/// distinct edges with a guard each skipped the warm outright and every survivor
/// scan ran cold under the guard. Guards are one point key apiece; they are
/// capped at the budget now, and only deletions gate the batch.
#[tokio::test]
async fn relationship_delete_warm_is_not_skipped_for_its_replay_guards() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = open_cached_delete_writer("reldelete-warm-guarded-batch", store, 8 * 1024 * 1024).await;
    let rows = (0..513u64)
        .map(|n| (relationship_mutation(1 + n, 100_001 + n, &format!("rel-{n}")), n))
        .collect::<Vec<_>>();
    let guards = rows
        .iter()
        .map(|(mutation, _)| relationship_mutation(mutation.src, mutation.dst, &format!("guard-{}", mutation.src)))
        .collect::<Vec<_>>();
    assert!(
        rows.len() + guards.len() > RELATIONSHIP_DELETE_WARM_MAX_ROWS,
        "the fixture must overflow the old gate"
    );

    let fields = relationship_delete_warm_fields(&shard, &rows, &guards).await;
    assert_eq!(warm_field(&fields, "hydradb.warm.skipped"), "false", "{fields:?}");
    // The prefixes need 1,539 reads, so they spend their half of the budget.
    assert_eq!(
        warm_count(&fields, "hydradb.warm.prefix_reads"),
        RELATIONSHIP_DELETE_WARM_MAX_READS / 2
    );
    shard.close().await.unwrap();
}

/// Devin on #146: the prefix warm counted deletions without looking at the
/// records, so on a dense edge whose first record survives it read every
/// deleted record after it, while the guarded check stops at that survivor.
/// It now stops where the check does.
#[tokio::test]
async fn relationship_delete_warm_stops_at_the_first_survivor() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_relationship_delete_scope("reldelete-warm-dense-edge", store.clone()).await;
    let shard = open_cached_delete_writer("reldelete-warm-dense-edge", store.clone(), 8 * 1024 * 1024).await;
    // Edge 7 holds one seeded relationship, the lowest id on it. Put 50 more
    // behind it and delete only those, so the survivor is the first record.
    let added = shard
        .merge_relationships_batch_between_labeled_vertices(
            "cell-0",
            "PRESENT_IN",
            merge_rows((0..50u64).map(|n| (7, 100_007, 5_000_000 + n))),
            "dense",
            ("Entity", "Chunk"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(added.relationships_inserted, 50);
    let mut rows = Vec::new();
    for n in 0..50u64 {
        let relationship_id = relationship_id_for_identity(&shard, 7, 100_007, 5_000_000 + n).await;
        rows.push((relationship_mutation(7, 100_007, &format!("dense-{n}")), relationship_id));
    }
    let survivor = relationship_id_for_identity(&shard, 7, 100_007, 1_000_007).await;
    assert!(
        rows.iter().all(|(_, id)| *id > survivor),
        "the survivor must be the edge's first record"
    );

    let fields = relationship_delete_warm_fields(&shard, &rows, &[]).await;
    assert_eq!(
        warm_count(&fields, "hydradb.warm.prefix_reads"),
        2,
        "the warm must stop at the survivor, as the guarded check does (seek + one record): {fields:?}"
    );

    let result = shard.delete_relationship_mutations_batch_txn("cell-0", &rows, &[]).await.unwrap();
    assert_eq!(result.deleted, 50);
    assert!(result.topology_sequence.is_none(), "the survivor keeps the edge");
    assert_eq!(relationship_records_between_typed(&shard, "PRESENT_IN", 7, 100_007).await, 1);
    shard.close().await.unwrap();
}

/// Greptile on #146: a relationship delete whose query expires while the warm
/// waits on object storage must stop there, before the writer permit, as
/// vertex-delete preparation does, instead of holding its query capacity until
/// the read comes back.
///
/// Every object read is slowed to five seconds rather than pausing "the next
/// GET": the first version paused whichever read came first, which on CI was a
/// background read, so the warm never blocked and the delete finished before
/// the cancel. Here any read the cold warm makes outlasts the deadline, and
/// which task reads first no longer matters.
///
/// The cancel waits for this delete's warm to start (its span opening), not for
/// a fixed sleep. Cancelling earlier would pass without the fix, because the
/// permit acquisition is cancellable too, and the test would prove nothing.
#[tokio::test]
async fn cancelled_relationship_delete_stops_while_the_warm_waits() {
    let store = crate::tests::ReadCountingObjectStore::new();
    seed_relationship_delete_scope("reldelete-warm-cancel", store.clone()).await;
    let rows = {
        let shard = open_cached_delete_writer("reldelete-warm-cancel", store.clone(), 0).await;
        let rows = relationship_delete_rows(&shard, "cancel", [7]).await;
        shard.close().await.unwrap();
        rows
    };
    let shard = Arc::new(open_cached_delete_writer("reldelete-warm-cancel", store.clone(), 8 * 1024 * 1024).await);
    let epoch = shard.current_epoch("cell-0").await.unwrap();

    store.delay_reads_and_reset_peak(5_000);
    let token = crate::QueryCancellationToken::new();
    let warm_started = Arc::new(tokio::sync::Notify::new());
    let deleting = {
        use tracing::instrument::WithSubscriber;
        let (shard, rows, token) = (Arc::clone(&shard), rows.clone(), token.clone());
        let subscriber = SpanOpened {
            name: "write.reldelete_warm",
            opened: Arc::clone(&warm_started),
        };
        tokio::spawn(
            async move {
                token
                    .scope(shard.delete_relationship_mutations_batch_with_guards("cell-0", rows, Vec::new()))
                    .await
            }
            .with_subscriber(subscriber),
        )
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), warm_started.notified())
        .await
        .expect("the delete never started its warm");
    token.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), deleting).await;
    store.delay_reads_and_reset_peak(0);
    let result = result.expect("a cancelled delete kept waiting on its warm").unwrap();
    assert!(
        matches!(
            result,
            Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                ..
            })
        ),
        "a cancelled delete must stop at the warm, got {result:?}"
    );
    assert_eq!(shard.current_epoch("cell-0").await.unwrap(), epoch);
    assert!(shard.edge_exists("cell-0", "PRESENT_IN", 7, 100_007).await.unwrap());

    // Cancellation is scoped to the request, not the shard or its next user.
    let result = shard
        .delete_relationship_mutations_batch_with_guards("cell-0", rows, Vec::new())
        .await
        .unwrap();
    assert_eq!(result.deleted, 1);
    Arc::into_inner(shard)
        .expect("the spawned delete released the shard")
        .close()
        .await
        .unwrap();
}
