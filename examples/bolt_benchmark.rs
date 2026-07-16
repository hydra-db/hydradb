use std::collections::HashMap;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use boltr::chunk::{ChunkReader, ChunkWriter};
use boltr::message::decode::decode_server_message;
use boltr::message::encode::encode_client_message;
use boltr::message::{ClientMessage, ServerMessage};
use boltr::server::handshake::{client_handshake, default_client_proposals};
use boltr::types::{BoltDict, BoltValue};
use bytes::BytesMut;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, BoltServerConfig, BoltServerHandle, ClientBoltServer, ClientQueryService,
    ClientQueryServiceConfig, ClientQueryTarget, GraphBackpressurePolicy, GraphCacheConfig,
    GraphCachePolicy, GraphIndexPolicy, GraphLimits, GraphOpenOptions, GraphScope,
    QueryTransportAction, QueryTransportScopeGrant, RoutedGraphCluster, ShardPlacement,
    StaticClientDatabaseResolver, StaticQueryTransportScopeAuthorizer,
};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ConcurrentTask = tokio::task::JoinHandle<BenchResult<(Vec<Duration>, Duration)>>;

const CELL_ID: &str = "bolt-bench";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";
const TOKEN: &str = "bolt-benchmark-secret";
const DEFAULT_FANOUTS: &[u64] = &[10, 50, 100, 1_000, 5_000, 10_000];
const DEFAULT_HOPS: &[u8] = &[1, 3, 5, 10];

#[tokio::main]
async fn main() -> BenchResult<()> {
    let fanouts = parse_u64_list("GRAPH_BOLT_BENCH_FANOUTS", DEFAULT_FANOUTS);
    let hops = parse_u8_list("GRAPH_BOLT_BENCH_HOPS", DEFAULT_HOPS);
    let max_hop = hops.iter().copied().max().unwrap_or(10);
    let read_warmup = env_usize("GRAPH_BOLT_BENCH_READ_WARMUP", 5);
    let read_latency_iters = env_usize("GRAPH_BOLT_BENCH_READ_LATENCY_ITERS", 30).max(1);
    let read_concurrency = env_usize("GRAPH_BOLT_BENCH_READ_CONCURRENCY", 8).max(1);
    let read_queries_per_worker = env_usize("GRAPH_BOLT_BENCH_READ_QUERIES_PER_WORKER", 20).max(1);
    let write_warmup = env_usize("GRAPH_BOLT_BENCH_WRITE_WARMUP", 2);
    let write_latency_iters = env_usize("GRAPH_BOLT_BENCH_WRITE_LATENCY_ITERS", 10).max(1);
    let write_concurrency = env_usize("GRAPH_BOLT_BENCH_WRITE_CONCURRENCY", 4).max(1);
    let write_queries_per_worker = env_usize("GRAPH_BOLT_BENCH_WRITE_QUERIES_PER_WORKER", 5).max(1);
    let bulk_chunk_size = env_usize("GRAPH_BOLT_BENCH_BULK_CHUNK", 10_000).max(1);
    if let Ok(addr) = std::env::var("GRAPH_BOLT_BENCH_EXTERNAL_ADDR") {
        return run_external_benchmark(ExternalBenchmarkConfig {
            addr: addr.parse()?,
            fanouts,
            hops,
            max_hop,
            read_warmup,
            read_latency_iters,
            read_concurrency,
            read_queries_per_worker,
            write_warmup,
            write_latency_iters,
            write_concurrency,
            write_queries_per_worker,
            build_chunk_size: bulk_chunk_size,
        })
        .await;
    }
    let root = tempfile::tempdir()?;
    let object_root = root.path().join("object-store");
    let cache_root = root.path().join("slatedb-cache");
    std::fs::create_dir_all(&object_root)?;
    std::fs::create_dir_all(&cache_root)?;
    let object_store = local_object_store(&object_root)?;

    eprintln!(
        "Bolt benchmark: fanouts={fanouts:?} hops={hops:?} read_latency_iters={read_latency_iters} read_concurrency={read_concurrency} read_queries_per_worker={read_queries_per_worker} write_latency_iters={write_latency_iters} write_concurrency={write_concurrency} write_queries_per_worker={write_queries_per_worker} backend=local-object-store transport=tcp-loopback kernel={}",
        if cfg!(feature = "graphblas") { "graphblas" } else { "rust" }
    );
    println!(
        "kind,backend,kernel,fanout,hops,latency_samples,p50_us,p95_us,p99_us,mean_us,concurrency,concurrent_operations,concurrent_p50_us,concurrent_p95_us,concurrent_p99_us,qps,operations_per_s,reachable_vertices_per_s,logical_edges_per_s,expected_count"
    );
    io::stdout().flush()?;

    for fanout in fanouts {
        let paths = BenchPaths {
            graph: format!("bolt-benchmark/fanout-{fanout}/graph"),
            cache: cache_root.join(format!("fanout-{fanout}")),
        };
        std::fs::create_dir_all(&paths.cache)?;
        let environment = open_environment(
            fanout,
            max_hop,
            paths,
            Arc::clone(&object_store),
            bulk_chunk_size,
        )
        .await?;
        eprintln!(
            "fanout={fanout} stage=ready addr={} edges={}",
            environment.server.local_addr(),
            fanout.saturating_mul(u64::from(max_hop))
        );

        for &hop in &hops {
            let query = format!(
                "MATCH (u {{id: 1}})-[:{EDGE_TYPE}*{hop}..{hop}]->(v) RETURN count(*) AS total"
            );
            let mut session = connect(environment.server.local_addr()).await?;
            for _ in 0..read_warmup {
                assert_read_count(&session.run(&query, HashMap::new()).await?, fanout)?;
            }
            let client_metrics_before = environment.service.metrics();
            let graph_metrics_before = environment
                .cluster
                .shard(CELL_ID)?
                .graph_operational_metrics();
            let mut latency_samples = Vec::with_capacity(read_latency_iters);
            for _ in 0..read_latency_iters {
                let started = Instant::now();
                let result = session.run(&query, HashMap::new()).await?;
                latency_samples.push(started.elapsed());
                assert_read_count(&result, fanout)?;
            }
            session.close().await?;
            let latency = LatencyStats::from_durations(&latency_samples);
            let concurrent = concurrent_reads(
                environment.server.local_addr(),
                query,
                fanout,
                read_concurrency,
                read_queries_per_worker,
            )
            .await?;
            let qps = concurrent.operations as f64
                / concurrent.elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
            if env_bool("GRAPH_BOLT_BENCH_PROFILE_STAGES", false) {
                let client_metrics_after = environment.service.metrics();
                let graph_metrics_after = environment
                    .cluster
                    .shard(CELL_ID)?
                    .graph_operational_metrics();
                let operations = (read_latency_iters + concurrent.operations).max(1) as f64;
                eprintln!(
                    "bolt_stage_profile fanout={fanout} hops={hop} operations={} prepare_us_per_op={:.3} execute_us_per_op={:.3} artifact_lookup_us_per_op={:.3} graphblas_cache_us_per_op={:.3} compute_queue_us_per_op={:.3} compute_us_per_op={:.3}",
                    operations as u64,
                    client_metrics_after
                        .prepare_duration_us
                        .saturating_sub(client_metrics_before.prepare_duration_us)
                        as f64
                        / operations,
                    client_metrics_after
                        .execution_duration_us
                        .saturating_sub(client_metrics_before.execution_duration_us)
                        as f64
                        / operations,
                    graph_metrics_after
                        .query_artifact_lookup_us
                        .saturating_sub(graph_metrics_before.query_artifact_lookup_us)
                        as f64
                        / operations,
                    graph_metrics_after
                        .query_graphblas_cache_us
                        .saturating_sub(graph_metrics_before.query_graphblas_cache_us)
                        as f64
                        / operations,
                    graph_metrics_after
                        .graph_compute_queue_us
                        .saturating_sub(graph_metrics_before.graph_compute_queue_us)
                        as f64
                        / operations,
                    graph_metrics_after
                        .graph_compute_duration_us
                        .saturating_sub(graph_metrics_before.graph_compute_duration_us)
                        as f64
                        / operations,
                );
            }
            print_record(BenchRecord {
                kind: "read",
                fanout,
                hops: hop,
                latency_samples: read_latency_iters,
                latency,
                concurrency: read_concurrency,
                concurrent: &concurrent,
                qps,
                reachable_vertices_per_s: fanout as f64 * qps,
                logical_edges_per_s: fanout as f64 * f64::from(hop) * qps,
                expected_count: fanout,
            })?;
        }

        let mut session = connect(environment.server.local_addr()).await?;
        let write_query = format!("CREATE (a {{id: $src}})-[:{EDGE_TYPE}]->(b {{id: $dst}})");
        let mut next_dst = 90_000_000_u64.saturating_add(fanout.saturating_mul(10_000));
        for _ in 0..write_warmup {
            run_write(&mut session, &write_query, next_dst).await?;
            next_dst = next_dst.saturating_add(1);
        }
        let mut write_latencies = Vec::with_capacity(write_latency_iters);
        for _ in 0..write_latency_iters {
            let started = Instant::now();
            run_write(&mut session, &write_query, next_dst).await?;
            write_latencies.push(started.elapsed());
            next_dst = next_dst.saturating_add(1);
        }
        session.close().await?;
        let write_latency = LatencyStats::from_durations(&write_latencies);
        let concurrent_writes = concurrent_writes(
            environment.server.local_addr(),
            write_query,
            next_dst,
            write_concurrency,
            write_queries_per_worker,
        )
        .await?;
        let write_qps = concurrent_writes.operations as f64
            / concurrent_writes
                .elapsed
                .as_secs_f64()
                .max(f64::MIN_POSITIVE);
        print_record(BenchRecord {
            kind: "write",
            fanout,
            hops: 0,
            latency_samples: write_latency_iters,
            latency: write_latency,
            concurrency: write_concurrency,
            concurrent: &concurrent_writes,
            qps: write_qps,
            reachable_vertices_per_s: 0.0,
            logical_edges_per_s: 0.0,
            expected_count: 1,
        })?;

        environment.stop().await?;
    }
    Ok(())
}

struct ExternalBenchmarkConfig {
    addr: SocketAddr,
    fanouts: Vec<u64>,
    hops: Vec<u8>,
    max_hop: u8,
    read_warmup: usize,
    read_latency_iters: usize,
    read_concurrency: usize,
    read_queries_per_worker: usize,
    write_warmup: usize,
    write_latency_iters: usize,
    write_concurrency: usize,
    write_queries_per_worker: usize,
    build_chunk_size: usize,
}

async fn run_external_benchmark(config: ExternalBenchmarkConfig) -> BenchResult<()> {
    let backend =
        std::env::var("GRAPH_BOLT_BENCH_BACKEND").unwrap_or_else(|_| "external-bolt".to_string());
    let kernel =
        std::env::var("GRAPH_BOLT_BENCH_KERNEL").unwrap_or_else(|_| "server-default".to_string());
    eprintln!(
        "Bolt benchmark: fanouts={:?} hops={:?} read_latency_iters={} read_concurrency={} read_queries_per_worker={} write_latency_iters={} write_concurrency={} write_queries_per_worker={} backend={backend} transport=tcp-loopback kernel={kernel}",
        config.fanouts,
        config.hops,
        config.read_latency_iters,
        config.read_concurrency,
        config.read_queries_per_worker,
        config.write_latency_iters,
        config.write_concurrency,
        config.write_queries_per_worker,
    );
    println!(
        "kind,backend,kernel,fanout,hops,latency_samples,p50_us,p95_us,p99_us,mean_us,concurrency,concurrent_operations,concurrent_p50_us,concurrent_p95_us,concurrent_p99_us,qps,operations_per_s,reachable_vertices_per_s,logical_edges_per_s,expected_count"
    );
    io::stdout().flush()?;

    let mut admin = connect(config.addr).await?;
    admin
        .run("MATCH (n) DETACH DELETE n", HashMap::new())
        .await?;
    admin
        .run("CREATE INDEX FOR (n:BenchNode) ON (n.id)", HashMap::new())
        .await?;
    admin.close().await?;

    for fanout in config.fanouts {
        let build_started = Instant::now();
        prepare_external_dataset(config.addr, fanout, config.max_hop, config.build_chunk_size)
            .await?;
        eprintln!(
            "fanout={fanout} stage=ready addr={} edges={} build_ms={:.3}",
            config.addr,
            fanout.saturating_mul(u64::from(config.max_hop)),
            build_started.elapsed().as_secs_f64() * 1_000.0,
        );

        for &hop in &config.hops {
            let query = format!(
                "MATCH (u:BenchNode {{id: 1}})-[:{EDGE_TYPE}*{hop}..{hop}]->(v) RETURN count(*) AS total"
            );
            let mut session = connect(config.addr).await?;
            for _ in 0..config.read_warmup {
                assert_read_count(&session.run(&query, HashMap::new()).await?, fanout)?;
            }
            let mut latency_samples = Vec::with_capacity(config.read_latency_iters);
            for _ in 0..config.read_latency_iters {
                let started = Instant::now();
                let result = session.run(&query, HashMap::new()).await?;
                latency_samples.push(started.elapsed());
                assert_read_count(&result, fanout)?;
            }
            session.close().await?;
            let latency = LatencyStats::from_durations(&latency_samples);
            let concurrent = concurrent_reads(
                config.addr,
                query,
                fanout,
                config.read_concurrency,
                config.read_queries_per_worker,
            )
            .await?;
            let qps = concurrent.operations as f64
                / concurrent.elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
            print_record_with_backend(
                BenchRecord {
                    kind: "read",
                    fanout,
                    hops: hop,
                    latency_samples: config.read_latency_iters,
                    latency,
                    concurrency: config.read_concurrency,
                    concurrent: &concurrent,
                    qps,
                    reachable_vertices_per_s: fanout as f64 * qps,
                    logical_edges_per_s: fanout as f64 * f64::from(hop) * qps,
                    expected_count: fanout,
                },
                &backend,
                &kernel,
            )?;
        }

        let mut session = connect(config.addr).await?;
        let write_query = format!("CREATE (a {{id: $src}})-[:{EDGE_TYPE}]->(b {{id: $dst}})");
        let mut next_dst = 90_000_000_u64.saturating_add(fanout.saturating_mul(10_000));
        for _ in 0..config.write_warmup {
            run_write(&mut session, &write_query, next_dst).await?;
            next_dst = next_dst.saturating_add(1);
        }
        let mut write_latencies = Vec::with_capacity(config.write_latency_iters);
        for _ in 0..config.write_latency_iters {
            let started = Instant::now();
            run_write(&mut session, &write_query, next_dst).await?;
            write_latencies.push(started.elapsed());
            next_dst = next_dst.saturating_add(1);
        }
        session.close().await?;
        let write_latency = LatencyStats::from_durations(&write_latencies);
        let concurrent = concurrent_writes(
            config.addr,
            write_query,
            next_dst,
            config.write_concurrency,
            config.write_queries_per_worker,
        )
        .await?;
        let qps =
            concurrent.operations as f64 / concurrent.elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
        print_record_with_backend(
            BenchRecord {
                kind: "write",
                fanout,
                hops: 0,
                latency_samples: config.write_latency_iters,
                latency: write_latency,
                concurrency: config.write_concurrency,
                concurrent: &concurrent,
                qps,
                reachable_vertices_per_s: 0.0,
                logical_edges_per_s: 0.0,
                expected_count: 1,
            },
            &backend,
            &kernel,
        )?;
    }
    Ok(())
}

async fn prepare_external_dataset(
    addr: SocketAddr,
    fanout: u64,
    max_hop: u8,
    build_chunk_size: usize,
) -> BenchResult<()> {
    let mut session = connect(addr).await?;
    session
        .run("MATCH (n) DETACH DELETE n", HashMap::new())
        .await?;
    session
        .run("CREATE (:BenchNode {id: 1})", HashMap::new())
        .await?;
    let mut path = "(root)".to_string();
    for depth in 0..max_hop {
        path.push_str(&format!(
            "-[:{EDGE_TYPE}]->(:BenchNode {{id: 2 + branch * {} + {depth}}})",
            u64::from(max_hop)
        ));
    }
    let query = format!(
        "UNWIND range($start, $end) AS branch MATCH (root:BenchNode {{id: 1}}) CREATE {path}"
    );
    let chunk_size = u64::try_from(build_chunk_size)?.max(1);
    let mut start = 0_u64;
    while start < fanout {
        let end = start.saturating_add(chunk_size).min(fanout) - 1;
        session
            .run(
                &query,
                HashMap::from([
                    (
                        "start".to_string(),
                        BoltValue::Integer(i64::try_from(start)?),
                    ),
                    ("end".to_string(), BoltValue::Integer(i64::try_from(end)?)),
                ]),
            )
            .await?;
        start = end.saturating_add(1);
    }
    session.close().await?;
    Ok(())
}

struct BenchPaths {
    graph: String,
    cache: std::path::PathBuf,
}

struct BenchEnvironment {
    server: BoltServerHandle,
    service: ClientQueryService,
    cluster: Arc<RoutedGraphCluster>,
}

impl BenchEnvironment {
    async fn stop(self) -> BenchResult<()> {
        self.server.stop().await?;
        self.cluster.close().await?;
        Ok(())
    }
}

async fn open_environment(
    fanout: u64,
    max_hop: u8,
    paths: BenchPaths,
    object_store: Arc<dyn ObjectStore>,
    bulk_chunk_size: usize,
) -> BenchResult<BenchEnvironment> {
    let cluster = Arc::new(
        RoutedGraphCluster::open_fenced_owned_with_options(
            paths.graph,
            "benchmark-node",
            ShardPlacement::fixed([(CELL_ID, "benchmark-node")])?,
            object_store,
            graph_options(fanout, max_hop, paths.cache),
        )
        .await?,
    );
    let shard = cluster.shard(CELL_ID)?;
    let build_started = Instant::now();
    shard
        .bulk_append_edges_trusted_chunked(
            CELL_ID,
            EDGE_TYPE,
            layered_edges(fanout, max_hop),
            &format!("bolt-benchmark-seed-{fanout}"),
            bulk_chunk_size,
        )
        .await?;
    let epoch = shard.current_epoch(CELL_ID).await?;
    shard
        .build_matrix_tiles(CELL_ID, EDGE_TYPE, epoch, 4_096)
        .await?;
    shard
        .refresh_edge_type_query_stats(CELL_ID, EDGE_TYPE)
        .await?;
    eprintln!(
        "fanout={fanout} stage=build elapsed_ms={:.3}",
        build_started.elapsed().as_secs_f64() * 1_000.0
    );

    let scope = GraphScope::default();
    let authorizer = StaticQueryTransportScopeAuthorizer::new().with_bearer_grant(
        TOKEN,
        QueryTransportScopeGrant::graph(
            scope.clone(),
            [
                QueryTransportAction::Read,
                QueryTransportAction::Write,
                QueryTransportAction::Cancel,
            ],
        ),
    )?;
    let service = ClientQueryService::new(
        cluster.clone(),
        ClientQueryServiceConfig::default()
            .with_required_bearer_token(TOKEN)
            .with_scope_authorizer(Arc::new(authorizer))
            .with_max_concurrent_queries(256)
            .with_max_query_runtime_ms(120_000),
    )?;
    let resolver =
        StaticClientDatabaseResolver::single("default", ClientQueryTarget::new(scope, CELL_ID)?)?;
    let server = ClientBoltServer::bind(
        "127.0.0.1:0".parse()?,
        service.clone(),
        BoltServerConfig::new(Arc::new(resolver))
            .with_max_connections(256)
            .with_prefetch_rows(64)
            .insecure_allow_plaintext(),
    )
    .await?;
    Ok(BenchEnvironment {
        server,
        service,
        cluster,
    })
}

fn graph_options(fanout: u64, max_hop: u8, cache_dir: std::path::PathBuf) -> GraphOpenOptions {
    let edges = fanout.saturating_mul(u64::from(max_hop));
    GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: usize::try_from(edges.saturating_add(1_000))
                .unwrap_or(usize::MAX),
            max_artifact_source_epochs: u64::MAX,
            max_traversal_hops: max_hop,
            max_artifact_build_edges: edges.saturating_add(1),
            max_query_result_vertices: usize::try_from(fanout.saturating_add(1_024))
                .unwrap_or(usize::MAX),
            max_query_intermediate_rows: usize::try_from(edges.saturating_add(1_024))
                .unwrap_or(usize::MAX),
            max_query_index_candidates: usize::try_from(edges.saturating_add(1_024))
                .unwrap_or(usize::MAX),
            max_query_scan_edges: edges.saturating_mul(u64::from(max_hop)).max(1),
            max_query_runtime_ms: Some(120_000),
        },
        cache: GraphCacheConfig::disk_cache_without_preload(cache_dir, 2 * 1024 * 1024 * 1024),
        cache_policy: GraphCachePolicy {
            max_matrix_adjacencies: 0,
            max_graphblas_matrices: 1,
            max_entries_per_cell: None,
            pin_matrix_min_edges: 0,
            max_concurrent_hydrations: 32,
            ..Default::default()
        },
        backpressure_policy: GraphBackpressurePolicy {
            max_concurrent_graph_writes: 1,
            ..Default::default()
        },
        index_policy: GraphIndexPolicy::Full,
        ..Default::default()
    }
}

struct BenchBoltSession {
    reader: ChunkReader<OwnedReadHalf>,
    writer: ChunkWriter<OwnedWriteHalf>,
}

impl BenchBoltSession {
    async fn connect(addr: SocketAddr) -> BenchResult<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        client_handshake(&mut stream, &default_client_proposals()).await?;
        let (read, write) = stream.into_split();
        let mut session = Self {
            reader: ChunkReader::new(read),
            writer: ChunkWriter::new(write),
        };
        session
            .send(&ClientMessage::Hello {
                extra: BoltDict::from([(
                    "user_agent".to_string(),
                    BoltValue::String("slatedb-graph-benchmark/0.1".to_string()),
                )]),
            })
            .await?;
        session.expect_success("HELLO").await?;
        let auth = if std::env::var("GRAPH_BOLT_BENCH_EMPTY_AUTH").as_deref() == Ok("1") {
            BoltDict::new()
        } else {
            BoltDict::from([
                ("scheme".to_string(), BoltValue::String("basic".to_string())),
                (
                    "principal".to_string(),
                    BoltValue::String(
                        std::env::var("GRAPH_BOLT_BENCH_PRINCIPAL")
                            .unwrap_or_else(|_| "benchmark".to_string()),
                    ),
                ),
                (
                    "credentials".to_string(),
                    BoltValue::String(
                        std::env::var("GRAPH_BOLT_BENCH_CREDENTIALS")
                            .unwrap_or_else(|_| TOKEN.to_string()),
                    ),
                ),
            ])
        };
        session.send(&ClientMessage::Logon { auth }).await?;
        session.expect_success("LOGON").await?;
        Ok(session)
    }

    async fn run(
        &mut self,
        query: &str,
        parameters: HashMap<String, BoltValue>,
    ) -> BenchResult<Vec<Vec<BoltValue>>> {
        self.send(&ClientMessage::Run {
            query: query.to_string(),
            parameters,
            extra: BoltDict::new(),
        })
        .await?;
        self.expect_success("RUN").await?;
        self.send(&ClientMessage::pull_all()).await?;
        let mut records = Vec::new();
        loop {
            match self.recv().await? {
                ServerMessage::Record { data } => records.push(data),
                ServerMessage::Success { .. } => return Ok(records),
                ServerMessage::Failure { metadata } => {
                    return Err(bolt_failure("PULL", &metadata).into());
                }
                message => {
                    return Err(
                        format!("expected RECORD or SUCCESS after PULL, got {message:?}").into(),
                    );
                }
            }
        }
    }

    async fn close(&mut self) -> BenchResult<()> {
        self.send(&ClientMessage::Goodbye).await
    }

    async fn send(&mut self, message: &ClientMessage) -> BenchResult<()> {
        let mut bytes = BytesMut::new();
        encode_client_message(&mut bytes, message);
        self.writer.write_message(&bytes).await?;
        Ok(())
    }

    async fn recv(&mut self) -> BenchResult<ServerMessage> {
        let bytes = self.reader.read_message().await?;
        Ok(decode_server_message(&bytes)?)
    }

    async fn expect_success(&mut self, operation: &str) -> BenchResult<BoltDict> {
        match self.recv().await? {
            ServerMessage::Success { metadata } => Ok(metadata),
            ServerMessage::Failure { metadata } => Err(bolt_failure(operation, &metadata).into()),
            message => Err(format!("expected SUCCESS after {operation}, got {message:?}").into()),
        }
    }
}

fn bolt_failure(operation: &str, metadata: &BoltDict) -> String {
    let code = metadata
        .get("code")
        .and_then(BoltValue::as_str)
        .unwrap_or("unknown");
    let message = metadata
        .get("message")
        .and_then(BoltValue::as_str)
        .unwrap_or("unknown Bolt failure");
    format!("{operation} failed: {code}: {message}")
}

async fn connect(addr: SocketAddr) -> BenchResult<BenchBoltSession> {
    BenchBoltSession::connect(addr).await
}

fn assert_read_count(records: &[Vec<BoltValue>], expected: u64) -> BenchResult<()> {
    let actual = records
        .first()
        .and_then(|row| row.first())
        .and_then(BoltValue::as_int)
        .and_then(|value| u64::try_from(value).ok());
    if actual != Some(expected) {
        return Err(format!("expected Bolt count {expected}, got {actual:?}").into());
    }
    Ok(())
}

async fn run_write(session: &mut BenchBoltSession, query: &str, dst: u64) -> BenchResult<()> {
    let dst = i64::try_from(dst)?;
    let params = HashMap::from([
        ("src".to_string(), BoltValue::Integer(1)),
        ("dst".to_string(), BoltValue::Integer(dst)),
    ]);
    let records = session.run(query, params).await?;
    if !records.is_empty() {
        return Err("write query unexpectedly returned records".into());
    }
    Ok(())
}

async fn concurrent_reads(
    addr: SocketAddr,
    query: String,
    expected: u64,
    concurrency: usize,
    queries_per_worker: usize,
) -> BenchResult<ConcurrentResult> {
    let mut sessions = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        sessions.push(connect(addr).await?);
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);
    for mut session in sessions {
        let barrier = Arc::clone(&barrier);
        let query = query.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let total_started = Instant::now();
            let mut latencies = Vec::with_capacity(queries_per_worker);
            for _ in 0..queries_per_worker {
                let started = Instant::now();
                let result = session.run(&query, HashMap::new()).await?;
                latencies.push(started.elapsed());
                assert_read_count(&result, expected)?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((latencies, total_started.elapsed()))
        }));
    }
    barrier.wait().await;
    collect_concurrent(tasks, concurrency.saturating_mul(queries_per_worker)).await
}

async fn concurrent_writes(
    addr: SocketAddr,
    query: String,
    first_dst: u64,
    concurrency: usize,
    queries_per_worker: usize,
) -> BenchResult<ConcurrentResult> {
    let mut sessions = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        sessions.push(connect(addr).await?);
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);
    for (worker, mut session) in sessions.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let query = query.clone();
        let worker_base =
            first_dst.saturating_add((worker as u64).saturating_mul(queries_per_worker as u64));
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let total_started = Instant::now();
            let mut latencies = Vec::with_capacity(queries_per_worker);
            for offset in 0..queries_per_worker {
                let started = Instant::now();
                run_write(
                    &mut session,
                    &query,
                    worker_base.saturating_add(offset as u64),
                )
                .await?;
                latencies.push(started.elapsed());
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((latencies, total_started.elapsed()))
        }));
    }
    barrier.wait().await;
    collect_concurrent(tasks, concurrency.saturating_mul(queries_per_worker)).await
}

async fn collect_concurrent(
    tasks: Vec<ConcurrentTask>,
    operations: usize,
) -> BenchResult<ConcurrentResult> {
    let mut latencies = Vec::with_capacity(operations);
    let mut elapsed = Duration::ZERO;
    for task in tasks {
        let (worker_latencies, worker_elapsed) = task.await??;
        latencies.extend(worker_latencies);
        elapsed = elapsed.max(worker_elapsed);
    }
    Ok(ConcurrentResult {
        operations,
        elapsed,
        latency: LatencyStats::from_durations(&latencies),
    })
}

struct ConcurrentResult {
    operations: usize,
    elapsed: Duration,
    latency: LatencyStats,
}

struct BenchRecord<'a> {
    kind: &'a str,
    fanout: u64,
    hops: u8,
    latency_samples: usize,
    latency: LatencyStats,
    concurrency: usize,
    concurrent: &'a ConcurrentResult,
    qps: f64,
    reachable_vertices_per_s: f64,
    logical_edges_per_s: f64,
    expected_count: u64,
}

fn print_record(record: BenchRecord<'_>) -> BenchResult<()> {
    print_record_with_backend(
        record,
        "local-object-store",
        if cfg!(feature = "graphblas") {
            "graphblas"
        } else {
            "rust"
        },
    )
}

fn print_record_with_backend(
    record: BenchRecord<'_>,
    backend: &str,
    kernel: &str,
) -> BenchResult<()> {
    println!(
        "{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{},{},{:.3},{:.3},{:.3},{:.2},{:.2},{:.2},{:.2},{}",
        record.kind,
        backend,
        kernel,
        record.fanout,
        record.hops,
        record.latency_samples,
        record.latency.p50_us,
        record.latency.p95_us,
        record.latency.p99_us,
        record.latency.mean_us,
        record.concurrency,
        record.concurrent.operations,
        record.concurrent.latency.p50_us,
        record.concurrent.latency.p95_us,
        record.concurrent.latency.p99_us,
        record.qps,
        record.qps,
        record.reachable_vertices_per_s,
        record.logical_edges_per_s,
        record.expected_count,
    );
    io::stdout().flush()?;
    Ok(())
}

#[derive(Clone, Copy)]
struct LatencyStats {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    mean_us: f64,
}

impl LatencyStats {
    fn from_durations(values: &[Duration]) -> Self {
        let mut micros: Vec<_> = values
            .iter()
            .map(|value| value.as_secs_f64() * 1_000_000.0)
            .collect();
        micros.sort_by(f64::total_cmp);
        let mean_us = micros.iter().sum::<f64>() / micros.len().max(1) as f64;
        Self {
            p50_us: percentile(&micros, 50),
            p95_us: percentile(&micros, 95),
            p99_us: percentile(&micros, 99),
            mean_us,
        }
    }
}

fn percentile(values: &[f64], percentile: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = (values.len() - 1).saturating_mul(percentile) / 100;
    values[index]
}

fn layered_edges(fanout: u64, max_hop: u8) -> impl Iterator<Item = (u64, u64)> {
    (0..fanout).flat_map(move |index| {
        (1..=max_hop).scan(1, move |src, hop| {
            let dst = (u64::from(hop) * 1_000_000) + index + 1;
            let edge = (*src, dst);
            *src = dst;
            Some(edge)
        })
    })
}

fn parse_u64_list(name: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn parse_u8_list(name: &str, default: &[u8]) -> Vec<u8> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}
