//! Cold-scope cost of `CALL algo.MSpaths` on a production-shaped scope.
//!
//! ```text
//! cargo run --release --features opencypher --example mspaths_cold_bench -- \
//!     build <store-dir> [entities=111000] [relationships=126000]
//! cargo run --release --features opencypher --example mspaths_cold_bench -- \
//!     query <store-dir> [get-latency-ms=0] [path-count=801]
//! cargo run --release --features opencypher --example mspaths_cold_bench -- \
//!     degree <store-dir> [get-latency-ms=0]
//! ```
//!
//! `build` writes a local-filesystem store: `entities` Entity vertices with
//! realistic vertex properties, and `relationships` RELATES relationships
//! with realistically sized properties, two of whose endpoints are seeds with 960
//! incident relationships and two with 40. `query` reopens that store as a
//! fresh reader behind an object store that sleeps `get-latency-ms` before
//! every GET and LIST (an S3 stand-in) and runs the application's one-hop
//! seed call: first cold, then on two different seeds once the topology is
//! resident, then the first seeds again with every cache warm. Each line
//! reports the wall time and the object-store GETs and bytes it cost.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use futures::StreamExt as _;
use hydradb::{
    EdgeMetadata, GraphOpenOptions, GraphShard, QueryContext, RelationshipMutation, Result, VertexMetadata, VertexPropertyValue,
};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};

const CELL: &str = "cell-a";
const GRAPH: &str = "bench/mspaths-cold";
const HUB_DEGREE: u64 = 960;
const SMALL_DEGREE: u64 = 40;

#[derive(Debug)]
struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    latency: Duration,
    gets: AtomicU64,
    bytes: AtomicU64,
    lists: AtomicU64,
}

impl fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStore({})", self.inner)
    }
}

impl LatencyStore {
    fn counters(&self) -> (u64, u64, u64) {
        (
            self.gets.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
            self.lists.load(Ordering::Relaxed),
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(&self, path: &Path, payload: PutPayload, opts: PutOptions) -> slatedb::object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(&self, path: &Path, opts: PutMultipartOptions) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> slatedb::object_store::Result<GetResult> {
        tokio::time::sleep(self.latency).await;
        let result = self.inner.get_opts(path, opts).await?;
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        Ok(result)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        let latency = self.latency;
        let inner = self.inner.list(prefix);
        futures::stream::once(async move {
            tokio::time::sleep(latency).await;
            inner
        })
        .flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> slatedb::object_store::Result<ListResult> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.latency).await;
        self.inner.list_with_delimiter(prefix).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn arg<T: std::str::FromStr>(index: usize, default: T) -> T {
    std::env::args().nth(index).and_then(|value| value.parse().ok()).unwrap_or(default)
}

fn entity(id: u64) -> VertexMetadata {
    VertexMetadata::default()
        .with_label("Entity")
        .with_property("name", VertexPropertyValue::String(format!("entity-{id}")))
        .with_property("entity_id", VertexPropertyValue::String(format!("{id:016x}")))
        .with_property("type", VertexPropertyValue::String("CONCEPT".to_string()))
        .with_property("namespace", VertexPropertyValue::String("bench".to_string()))
        .with_property("identifier", VertexPropertyValue::String(format!("entity-{id}")))
}

fn relationship(id: u64, src: u64, dst: u64) -> RelationshipMutation {
    let metadata = EdgeMetadata::default()
        .with_property("relationship_id", VertexPropertyValue::String(format!("{id:016x}")))
        .with_property("chunk_id", VertexPropertyValue::String(format!("chunk-{:08}", id / 4)))
        .with_property("canonical_relation", VertexPropertyValue::String("RELATED_TO".to_string()))
        .with_property(
            "raw_relation",
            VertexPropertyValue::String(format!(
                "entity-{src} is related to entity-{dst} through a sentence of ordinary length"
            )),
        )
        .with_property("timestamp", VertexPropertyValue::Integer(1_700_000_000 + id));
    RelationshipMutation {
        cell_id: CELL.to_string(),
        edge_type: "RELATES".to_string(),
        src,
        dst,
        relationship_id: id,
        metadata,
    }
}

async fn build(store: Arc<dyn ObjectStore>, entities: u64, relationships: u64) -> Result<()> {
    let shard = GraphShard::open_standalone_writer(GRAPH, store).await?;
    let started = Instant::now();
    for start in (1..=entities).step_by(5_000) {
        let end = (start + 5_000).min(entities + 1);
        shard
            .set_vertex_metadata_batch(CELL, (start..end).map(|id| (id, entity(id))))
            .await?;
    }
    // Seeds 1 and 3 are hubs, 2 and 4 small; half of each seed's
    // relationships point in, half out. Everything else is uniform.
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut random = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        5 + state % (entities - 4)
    };
    let mut pending = Vec::new();
    let mut next_id = 1_u64;
    let mut push = |pending: &mut Vec<RelationshipMutation>, src: u64, dst: u64| {
        pending.push(relationship(next_id, src, dst));
        next_id += 1;
    };
    for (seed, degree) in [(1, HUB_DEGREE), (2, SMALL_DEGREE), (3, HUB_DEGREE), (4, SMALL_DEGREE)] {
        for index in 0..degree {
            let other = random();
            if index % 2 == 0 {
                push(&mut pending, seed, other);
            } else {
                push(&mut pending, other, seed);
            }
        }
    }
    while (pending.len() as u64) < relationships {
        let (src, dst) = (random(), random());
        push(&mut pending, src, dst);
    }
    for (batch, chunk) in pending.chunks(5_000).enumerate() {
        shard
            .import_relationships_batch(CELL, "RELATES", chunk.to_vec(), &format!("bench-relates-{batch}"))
            .await?;
    }
    let generation = shard.build_graph_index(CELL, "RELATES").await?;
    shard.close().await?;
    println!(
        "built entities={entities} relationships={} generation_edges={} in {:?}",
        pending.len(),
        generation.edge_count,
        started.elapsed()
    );
    Ok(())
}

fn seed_call(seeds: [u64; 2], path_count: u64) -> String {
    format!(
        "CALL algo.MSpaths({{sourceLabel: 'Entity', sourceProperty: 'name', sourceValues: ['entity-{}', 'entity-{}'], \
         relTypes: ['RELATES'], maxLen: 1, relDirection: 'both', pathCount: {path_count}, resultLimit: 100000}}) \
         YIELD path RETURN path",
        seeds[0], seeds[1]
    )
}

async fn query(local: Arc<dyn ObjectStore>, latency_ms: u64, path_count: u64) -> Result<()> {
    let store = Arc::new(LatencyStore {
        inner: local,
        latency: Duration::from_millis(latency_ms),
        gets: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        lists: AtomicU64::new(0),
    });
    let mut last = (0, 0, 0);
    let mut report = |phase: &str, elapsed: Duration, rows: Option<usize>| {
        let now = store.counters();
        println!(
            "{phase:<28} {:>8.1} ms  rows={:<6} gets={:<5} bytes={:<10} lists={}",
            elapsed.as_secs_f64() * 1_000.0,
            rows.map_or_else(|| "-".to_string(), |rows| rows.to_string()),
            now.0 - last.0,
            now.1 - last.1,
            now.2 - last.2,
        );
        last = now;
    };
    let started = Instant::now();
    let reader = GraphShard::open_with_options(GRAPH, Arc::clone(&store) as Arc<dyn ObjectStore>, GraphOpenOptions::default()).await?;
    report("open reader", started.elapsed(), None);
    for (phase, seeds, result_bytes) in [
        ("cold seeds 1,2", [1, 2], None),
        ("topology warm, seeds 3,4", [3, 4], None),
        ("warm seeds 1,2", [1, 2], Some(u64::MAX - 1)),
    ] {
        let mut context = QueryContext::new(CELL, "mspaths-cold-bench");
        if let Some(bytes) = result_bytes {
            // A different result budget is a different result-cache key.
            context = context.with_max_result_bytes(bytes);
        }
        let started = Instant::now();
        let result = reader.execute_cypher_rows(context, &seed_call(seeds, path_count)).await?;
        report(phase, started.elapsed(), Some(result.rows.len()));
    }
    reader.close().await?;
    Ok(())
}

/// The admin super-node call on a fresh reader: cold, then warm. Reports the
/// wall time and the object-store GETs and bytes each run costs.
async fn degree(local: Arc<dyn ObjectStore>, latency_ms: u64) -> Result<()> {
    let store = Arc::new(LatencyStore {
        inner: local,
        latency: Duration::from_millis(latency_ms),
        gets: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        lists: AtomicU64::new(0),
    });
    let started = Instant::now();
    let reader = GraphShard::open_with_options(GRAPH, Arc::clone(&store) as Arc<dyn ObjectStore>, GraphOpenOptions::default()).await?;
    println!("open reader                  {:>8.1} ms", started.elapsed().as_secs_f64() * 1_000.0);
    let mut last = store.counters();
    for (phase, limit, bytes) in [
        ("cold limit 50", 50, None),
        ("warm limit 50", 50, Some(u64::MAX - 1)),
        ("warm limit 1", 1, Some(u64::MAX - 2)),
    ] {
        let mut context = QueryContext::new(CELL, "degree-cold-bench");
        if let Some(bytes) = bytes {
            context = context.with_max_result_bytes(bytes);
        }
        let call = format!(
            "CALL algo.degree({{label: 'Entity', relTypes: ['RELATES'], relDirection: 'undirected', minDegree: 1, limit: {limit}}}) \
             YIELD node, degree RETURN node.name AS name, degree"
        );
        let started = Instant::now();
        let result = reader.execute_cypher_rows(context, &call).await?;
        let now = store.counters();
        println!(
            "{phase:<28} {:>8.1} ms  rows={:<6} gets={:<5} bytes={:<10} lists={}",
            started.elapsed().as_secs_f64() * 1_000.0,
            result.rows.len(),
            now.0 - last.0,
            now.1 - last.1,
            now.2 - last.2,
        );
        last = now;
    }
    reader.close().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let dir = std::env::args()
        .nth(2)
        .expect("usage: mspaths_cold_bench build|query <store-dir> ...");
    std::fs::create_dir_all(&dir).expect("store directory");
    let local: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).expect("local object store"));
    match mode.as_str() {
        "build" => build(local, arg(3, 111_000), arg(4, 126_000)).await,
        "query" => query(local, arg(3, 0), arg(4, 801)).await,
        "degree" => degree(local, arg(3, 0)).await,
        _ => panic!("usage: mspaths_cold_bench build|query|degree <store-dir> ..."),
    }
}
