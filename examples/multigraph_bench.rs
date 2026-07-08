use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, EdgeMetadata, EdgeMutation, GraphOpenOptions, GraphShard, QueryColumn,
    QueryContext, QueryResultSet, QueryRow, QueryValue, RelationshipMutation, VertexPropertyValue,
};

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "FOLLOWS";
const DEFAULT_REL_COUNTS: &[u64] = &[50, 100, 1_000, 5_000, 10_000];

#[tokio::main]
async fn main() -> BenchResult<()> {
    let relationship_counts =
        parse_u64_list("GRAPH_MULTIGRAPH_BENCH_REL_COUNTS", DEFAULT_REL_COUNTS);
    let hot_iters = env_u32("GRAPH_MULTIGRAPH_BENCH_HOT_ITERS", 11).max(1);
    let cold_iters = env_u32("GRAPH_MULTIGRAPH_BENCH_COLD_ITERS", 3).max(1);
    let create_samples = env_u32("GRAPH_MULTIGRAPH_BENCH_CREATE_SAMPLES", 16).max(1);

    let root = TempBenchRoot::new()?;
    let object_root = root.path().join("object-store");
    let cache_root = root.path().join("slatedb-cache");
    fs::create_dir_all(&object_root)?;
    fs::create_dir_all(&cache_root)?;
    let object_store = local_object_store(&object_root)?;
    let run_id = format!("multigraph-bench-{}", std::process::id());

    eprintln!(
        "multigraph benchmark: relationship_counts={relationship_counts:?} cold_iters={cold_iters} hot_iters={hot_iters} create_samples={create_samples} object_store=local:{}",
        object_root.display()
    );
    println!("section,relationship_count,workload,expected_rows,actual_rows,status,detail");
    io::stdout().flush()?;

    let mut speed_rows = Vec::new();
    for relationship_count in relationship_counts {
        let shard_path = format!("{run_id}/rels-{relationship_count}");
        let writer = GraphShard::open_standalone_writer_with_options(
            shard_path.clone(),
            Arc::clone(&object_store),
            GraphOpenOptions::default(),
        )
        .await?;

        let import_started = Instant::now();
        let import = writer
            .import_relationships_batch(
                CELL_ID,
                EDGE_TYPE,
                relationship_mutations(relationship_count),
                &format!("multigraph-import-{relationship_count}"),
            )
            .await?;
        let import_elapsed = import_started.elapsed();
        if import.relationships_inserted != relationship_count
            || import.structural_edges_inserted != 1
        {
            return Err(format!(
                "bad import result for {relationship_count}: inserted relationships={} structural_edges={}",
                import.relationships_inserted, import.structural_edges_inserted
            )
            .into());
        }
        let degree = writer.out_degree(CELL_ID, EDGE_TYPE, 1).await?;
        if degree != 1 {
            return Err(format!(
                "bad structural degree for {relationship_count}: expected 1 got {degree}"
            )
            .into());
        }
        writer.close().await?;

        let cache_dir = cache_root.join(format!("rels-{relationship_count}"));
        reset_dir(&cache_dir)?;
        let reader = GraphShard::open_with_options(
            shard_path.clone(),
            Arc::clone(&object_store),
            GraphOpenOptions::default(),
        )
        .await?;

        verify_count(&reader, relationship_count).await?;
        verify_index_exact(&reader, relationship_count).await?;
        verify_all_rows(&reader, relationship_count).await?;
        println!(
            "accuracy,{relationship_count},count/index/all_rows,{relationship_count},{relationship_count},ok,degree=1"
        );
        reader.close().await?;

        speed_rows.push(SpeedRow::write(
            relationship_count,
            "import_relationships_batch",
            relationship_count,
            import_elapsed,
        ));

        for workload in multigraph_workloads(relationship_count) {
            speed_rows.push(
                bench_read_workload(
                    &object_store,
                    &shard_path,
                    &cache_root,
                    relationship_count,
                    workload,
                    cold_iters,
                    hot_iters,
                )
                .await?,
            );
        }

        let writer = GraphShard::open_standalone_writer_with_options(
            shard_path,
            Arc::clone(&object_store),
            GraphOpenOptions::default(),
        )
        .await?;
        speed_rows.push(bench_generated_create(&writer, relationship_count, create_samples).await?);
        writer.close().await?;
    }

    println!(
        "section,relationship_count,workload,rows_or_writes,cold_p50_us,cold_p95_us,warm_us,hot_p50_us,hot_p95_us,hot_p99_us,hot_mean_us,qps,total_ms"
    );
    for row in speed_rows {
        row.print();
    }
    Ok(())
}

fn relationship_mutations(count: u64) -> Vec<RelationshipMutation> {
    (0..count)
        .map(|rank| RelationshipMutation {
            cell_id: CELL_ID.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: 1,
            dst: 2,
            relationship_id: rank + 1,
            metadata: EdgeMetadata::default()
                .with_property("rank", VertexPropertyValue::Integer(rank))
                .with_property("bucket", VertexPropertyValue::Integer(rank % 10)),
        })
        .collect()
}

struct Workload {
    name: &'static str,
    query: String,
    expected_rows: usize,
}

fn multigraph_workloads(relationship_count: u64) -> Vec<Workload> {
    let midpoint = relationship_count / 2;
    vec![
        Workload {
            name: "count_all",
            query: format!(
                "MATCH (u {{id: 1}})-[r:{EDGE_TYPE}]->(v {{id: 2}}) RETURN count(*) AS total"
            ),
            expected_rows: 1,
        },
        Workload {
            name: "index_exact",
            query: format!(
                "MATCH (u {{id: 1}})-[r:{EDGE_TYPE} {{rank: {midpoint}}}]->(v {{id: 2}}) RETURN r.rank AS rank"
            ),
            expected_rows: 1,
        },
        Workload {
            name: "all_rows_ordered",
            query: format!(
                "MATCH (u {{id: 1}})-[r:{EDGE_TYPE}]->(v {{id: 2}}) RETURN r.rank AS rank ORDER BY rank"
            ),
            expected_rows: usize::try_from(relationship_count).unwrap_or(usize::MAX),
        },
    ]
}

async fn verify_count(shard: &GraphShard, relationship_count: u64) -> BenchResult<()> {
    let rows = shard
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, format!("accuracy-count-{relationship_count}")),
            &format!("MATCH (u {{id: 1}})-[r:{EDGE_TYPE}]->(v {{id: 2}}) RETURN count(*) AS total"),
        )
        .await?;
    let expected = QueryResultSet::new(
        vec![QueryColumn::new("total")],
        vec![QueryRow::new(vec![QueryValue::Count(relationship_count)])],
    );
    if rows != expected {
        return Err(format!("count accuracy failed for {relationship_count}: {rows:?}").into());
    }
    Ok(())
}

async fn verify_index_exact(shard: &GraphShard, relationship_count: u64) -> BenchResult<()> {
    let midpoint = relationship_count / 2;
    let rows = shard
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, format!("accuracy-index-{relationship_count}")),
            &format!(
                "MATCH (u {{id: 1}})-[r:{EDGE_TYPE} {{rank: {midpoint}}}]->(v {{id: 2}}) RETURN r.rank AS rank"
            ),
        )
        .await?;
    let expected = QueryResultSet::new(
        vec![QueryColumn::new("rank")],
        vec![QueryRow::new(vec![QueryValue::Property(
            VertexPropertyValue::Integer(midpoint),
        )])],
    );
    if rows != expected {
        return Err(format!("index accuracy failed for {relationship_count}: {rows:?}").into());
    }
    Ok(())
}

async fn verify_all_rows(shard: &GraphShard, relationship_count: u64) -> BenchResult<()> {
    let rows = shard
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, format!("accuracy-all-{relationship_count}")),
            &format!(
                "MATCH (u {{id: 1}})-[r:{EDGE_TYPE}]->(v {{id: 2}}) RETURN r.rank AS rank ORDER BY rank"
            ),
        )
        .await?;
    if rows.rows.len() != usize::try_from(relationship_count)? {
        return Err(format!(
            "all-row accuracy failed for {relationship_count}: expected {relationship_count} got {}",
            rows.rows.len()
        )
        .into());
    }
    for (idx, row) in rows.rows.iter().enumerate() {
        let expected = u64::try_from(idx)?;
        match row.values.as_slice() {
            [QueryValue::Property(VertexPropertyValue::Integer(actual))] if *actual == expected => {
            }
            other => {
                return Err(format!(
                    "all-row accuracy failed for {relationship_count} at row {idx}: {other:?}"
                )
                .into())
            }
        }
    }
    Ok(())
}

async fn bench_read_workload(
    object_store: &Arc<dyn ObjectStore>,
    shard_path: &str,
    cache_root: &Path,
    relationship_count: u64,
    workload: Workload,
    cold_iters: u32,
    hot_iters: u32,
) -> BenchResult<SpeedRow> {
    let mut cold_samples = Vec::new();
    for iter in 0..cold_iters {
        let cache_dir = cache_root.join(format!(
            "cold-{relationship_count}-{}-{iter}",
            workload.name
        ));
        reset_dir(&cache_dir)?;
        let started = Instant::now();
        let reader = GraphShard::open_with_options(
            shard_path.to_string(),
            Arc::clone(object_store),
            GraphOpenOptions::default(),
        )
        .await?;
        let rows = reader
            .execute_cypher_rows(
                QueryContext::new(
                    CELL_ID,
                    format!("cold-{relationship_count}-{}-{iter}", workload.name),
                ),
                &workload.query,
            )
            .await?;
        assert_rows(workload.name, workload.expected_rows, &rows)?;
        reader.close().await?;
        cold_samples.push(started.elapsed().as_micros());
    }

    let reader = GraphShard::open_with_options(
        shard_path.to_string(),
        Arc::clone(object_store),
        GraphOpenOptions::default(),
    )
    .await?;
    let warm_started = Instant::now();
    let warm_rows = reader
        .execute_cypher_rows(
            QueryContext::new(
                CELL_ID,
                format!("warm-{relationship_count}-{}", workload.name),
            ),
            &workload.query,
        )
        .await?;
    assert_rows(workload.name, workload.expected_rows, &warm_rows)?;
    let warm_us = warm_started.elapsed().as_micros();

    let mut hot_samples = Vec::new();
    let hot_started = Instant::now();
    for iter in 0..hot_iters {
        let started = Instant::now();
        let rows = reader
            .execute_cypher_rows(
                QueryContext::new(
                    CELL_ID,
                    format!("hot-{relationship_count}-{}-{iter}", workload.name),
                ),
                &workload.query,
            )
            .await?;
        assert_rows(workload.name, workload.expected_rows, &rows)?;
        hot_samples.push(started.elapsed().as_micros());
    }
    let hot_elapsed = hot_started.elapsed();
    reader.close().await?;

    Ok(SpeedRow::read(
        relationship_count,
        workload.name,
        workload.expected_rows as u64,
        cold_samples,
        warm_us,
        hot_samples,
        hot_elapsed,
    ))
}

async fn bench_generated_create(
    writer: &GraphShard,
    relationship_count: u64,
    samples: u32,
) -> BenchResult<SpeedRow> {
    let mut timings = Vec::new();
    for sample in 0..samples {
        let rank = relationship_count + u64::from(sample);
        let started = Instant::now();
        let result = writer
            .create_relationship(
                EdgeMutation {
                    cell_id: CELL_ID.to_string(),
                    edge_type: EDGE_TYPE.to_string(),
                    src: 1,
                    dst: 2,
                    idempotency_key: format!("generated-create-{relationship_count}-{sample}"),
                },
                EdgeMetadata::default()
                    .with_property("rank", VertexPropertyValue::Integer(rank))
                    .with_property("bucket", VertexPropertyValue::Integer(rank % 10)),
            )
            .await?;
        if result.structural_edge_inserted {
            return Err(format!(
                "generated create unexpectedly inserted structural edge for {relationship_count}"
            )
            .into());
        }
        timings.push(started.elapsed().as_micros());
    }
    let total = Duration::from_micros(timings.iter().sum::<u128>() as u64);
    Ok(SpeedRow::timed(
        relationship_count,
        "generated_create",
        u64::from(samples),
        Vec::new(),
        0,
        timings,
        total,
    ))
}

fn assert_rows(workload: &str, expected_rows: usize, rows: &QueryResultSet) -> BenchResult<()> {
    if rows.rows.len() != expected_rows {
        return Err(format!(
            "workload {workload} expected {expected_rows} rows got {}",
            rows.rows.len()
        )
        .into());
    }
    Ok(())
}

struct SpeedRow {
    relationship_count: u64,
    workload: &'static str,
    rows_or_writes: u64,
    cold_samples: Vec<u128>,
    warm_us: u128,
    hot_samples: Vec<u128>,
    total_elapsed: Duration,
}

impl SpeedRow {
    fn write(
        relationship_count: u64,
        workload: &'static str,
        rows_or_writes: u64,
        total_elapsed: Duration,
    ) -> Self {
        Self {
            relationship_count,
            workload,
            rows_or_writes,
            cold_samples: Vec::new(),
            warm_us: 0,
            hot_samples: vec![total_elapsed.as_micros()],
            total_elapsed,
        }
    }

    fn read(
        relationship_count: u64,
        workload: &'static str,
        rows_or_writes: u64,
        cold_samples: Vec<u128>,
        warm_us: u128,
        hot_samples: Vec<u128>,
        total_elapsed: Duration,
    ) -> Self {
        Self::timed(
            relationship_count,
            workload,
            rows_or_writes,
            cold_samples,
            warm_us,
            hot_samples,
            total_elapsed,
        )
    }

    fn timed(
        relationship_count: u64,
        workload: &'static str,
        rows_or_writes: u64,
        cold_samples: Vec<u128>,
        warm_us: u128,
        hot_samples: Vec<u128>,
        total_elapsed: Duration,
    ) -> Self {
        Self {
            relationship_count,
            workload,
            rows_or_writes,
            cold_samples,
            warm_us,
            hot_samples,
            total_elapsed,
        }
    }

    fn print(mut self) {
        self.cold_samples.sort_unstable();
        self.hot_samples.sort_unstable();
        let qps = if self.total_elapsed.is_zero() {
            0.0
        } else {
            self.hot_samples.len() as f64 / self.total_elapsed.as_secs_f64()
        };
        println!(
            "speed,{},{},{},{},{},{},{},{},{},{:.1},{:.1},{}",
            self.relationship_count,
            self.workload,
            self.rows_or_writes,
            percentile(&self.cold_samples, 50),
            percentile(&self.cold_samples, 95),
            self.warm_us,
            percentile(&self.hot_samples, 50),
            percentile(&self.hot_samples, 95),
            percentile(&self.hot_samples, 99),
            mean(&self.hot_samples),
            qps,
            self.total_elapsed.as_millis()
        );
    }
}

fn parse_u64_list(name: &str, default: &[u64]) -> Vec<u64> {
    let Ok(value) = std::env::var(name) else {
        return default.to_vec();
    };
    let parsed: Vec<_> = value
        .split(',')
        .filter_map(|part| part.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .collect();
    if parsed.is_empty() {
        default.to_vec()
    } else {
        parsed
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn percentile(values: &[u128], percentile: u32) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() as u128 - 1) * u128::from(percentile) / 100) as usize;
    values[index]
}

fn mean(values: &[u128]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<u128>() as f64 / values.len() as f64
}

fn reset_dir(path: &Path) -> BenchResult<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

struct TempBenchRoot {
    path: PathBuf,
}

impl TempBenchRoot {
    fn new() -> BenchResult<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path =
            std::env::temp_dir().join(format!("multigraph-bench-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBenchRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
